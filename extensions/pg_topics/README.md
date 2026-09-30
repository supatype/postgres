# pg_topics

`pg_topics` is a PostgreSQL 17 extension that turns a table into a Kafka
topic. A stock Kafka client, such as the Java client, librdkafka or
Confluent JS, can produce and consume on a port that the extension opens.
A SQL user can do the same work with plain functions, inside their own
transaction. An optional worker keeps a normal table in sync with the
topic, so you can read the current state with `SELECT`. There is no broker
to run, back up or patch.

## Who it is for

pg_topics is for a team that already runs PostgreSQL and needs up to tens of
thousands of events a second. You get:

- one system to run, back up and secure, instead of a broker cluster.
- the history of every topic as a table that SQL can query.
- a publish inside your own database transaction, with no outbox table.
- PostgreSQL roles and grants as the only permission system.

One node handles about 10,000 records a second with low latency, and up
to about 44,000 a second at top speed. If you need hundreds of thousands
of records a second, or Kafka transactions, use Kafka.

## Limits to know first

| Limit | Detail |
|---|---|
| A record value is JSON | pg_topics refuses a value that does not parse as JSON. |
| A record key is UTF-8 text of 40 characters or fewer | pg_topics refuses a longer key, and a key that is not valid UTF-8. |
| A topic name is `schema.table_q` | The table name must end in `_q`. |

Warning: a Kafka user must use the full topic name. A producer aimed at a topic
named `orders` gets `TOPIC_AUTHORIZATION_FAILED`. The real topic name is,
for example, `public.orders_q`.

The full list is in [Kafka compatibility](#kafka-compatibility), at the end
of this page.

## Quick start

1. Build and install the extension, and add it to
   `shared_preload_libraries`. See [Install](docs/install.md).
2. Set `pg_topics.databases`, `pg_topics.failover_is_fenced`, a TLS
   certificate and `pg_topics.advertised_host`, then restart PostgreSQL.
3. Run `CREATE EXTENSION pg_topics;` in the database.
4. Make a topic and publish to it:

   ```sql
   SELECT topic.create_topic('public.orders_q', band_count => 4);
   SELECT topic.publish('public.orders_q', '{"order_id": 1}', key => 'customer-42');
   ```

5. Point a Kafka client at the listener with `SASL_SSL`, `PLAIN`, a
   PostgreSQL role and its password. See [Connect a Kafka client](docs/kafka-clients.md).

A band is what Kafka calls a partition.

## Documentation

| Guide | For |
|---|---|
| [Install](docs/install.md) | Build, install, first topic, upgrade and removal |
| [Configure](docs/configuration.md) | Every setting, topics, durability, replication, permissions, the table sync, backups and failover |
| [Connect a Kafka client](docs/kafka-clients.md) | Settings for Java, librdkafka, Confluent JS, kafkajs and franz-go, and the admin tools that work |
| [Use from SQL](docs/sql.md) | Publish, read, keep a position and join a group from SQL |
| [Tune](docs/tuning.md) | Where the limits are, and which settings change the numbers |
| [Operate](docs/operations.md) | Monitoring, alerts, log lines and how to fix common problems |

## Performance

These numbers come from `bench/run_benchmark.sh` with `BENCH_SECONDS=120`,
and from `bench/run_soak.sh` with `SOAK_SECONDS=600`, on 29 September 2026,
at commit `dccda33`.

The machine was an AMD Ryzen 9 5900X with 12 cores and 24 threads, 31 GiB
of RAM, Windows WSL2 with kernel 6.18, PostgreSQL 17.11 and a release build
of pg_topics. The Kafka clients were the Java tools from
`confluentinc/cp-kafka:7.7.1` in Docker Desktop. Each record was about 200
bytes of JSON unless a row says otherwise. The producer was idempotent and
used `acks=all`, unless a row says otherwise. Latency is the time from send
to answer, as `kafka-producer-perf-test` reports it.

Warning: every node ran on this one host, so replication added no network
latency. Docker Desktop added one network hop through Windows to each
request. Measure on your own hardware before you size a deployment.

[Tune](docs/tuning.md) explains what each result means for your settings.

### Kafka produce, one node

| Test | Bands | Rate | p50 | p99 | p99.9 |
|---|---:|---:|---:|---:|---:|
| Fixed 10,000 records/s for 120 s | 4 | 9,997/s | 34 ms | 111 ms | 232 ms |
| Fixed 10,000 records/s for 120 s | 12 | 9,996/s | 31 ms | 71 ms | 205 ms |
| Top speed, 1 producer | 4 | 27,910/s, 5.3 MB/s | 3.2 s | 5.3 s | 5.3 s |
| Top speed, 1 producer, `lz4` | 4 | 32,557/s, 6.2 MB/s | 2.9 s | 5.3 s | 5.4 s |
| Top speed, 1 producer, `acks=1`, not idempotent | 4 | 28,066/s, 5.4 MB/s | 3.4 s | 5.3 s | 5.3 s |
| Top speed, 1 producer, 1 KB records | 4 | 6,348/s, 6.2 MB/s | 4.5 s | 4.9 s | 4.9 s |
| Top speed, 1 producer, 10 KB records | 4 | 447/s, 4.3 MB/s | 4.5 s | 4.9 s | 4.9 s |
| Top speed, 4 producers | 4 | 43,889/s in total | | 3.6 s, worst producer | |

At top speed the producer fills its own send buffer, so its latency grows
to seconds. For low latency, keep a node near 10,000 records a second.

At the fixed rate, the stamper gave every record its offset within 4 ms.
The time from `Produce` to a consumer that waits for the record was 2 ms at
the median and 3 ms at p99, over 10 samples.

### Kafka consume, one node

| Test | Records/s in total | Slowest group |
|---|---:|---:|
| 1 group reads 1,200,000 records, 4 bands | 63,586 | |
| 1 group reads 1,200,000 records, 12 bands | 61,069 | |
| 4 groups read the same 12 band topic at once | 131,292 | 32,669 |
| 8 groups read the same 12 band topic at once | 161,282 | 17,686 |

### Soak, one node

10 minutes of produce at a fixed 10,000 records a second, with one consumer
group reading at the same time. 4 bands, records of about 220 bytes:

| Measure | Result |
|---|---:|
| Records produced and consumed | 6,000,000 |
| Produce rate | 9,999/s |
| Produce p50, p99, p99.9 | 44 ms, 215 ms, 383 ms |
| Consume rate | 10,000/s, the same as the produce rate |
| Largest `backlog_age` | 0.021 s |
| Dead tuples in the write partition at the end | 654,784 |

The dead tuples are the old row versions that the stamper leaves. At the
end they were about 11% of the rows, so autovacuum kept up.

### Three nodes, one cluster

A primary and two synchronous streaming standbys, all on the same host.
The topic uses the `replicated` tier, so a Kafka client sees a replication
factor of 3.

| `synchronous_standby_names` | Copies at the answer | Rate | p50 | p99 | p99.9 | Consume |
|---|---|---:|---:|---:|---:|---:|
| `FIRST 2 (s1, s2)` | 3 of 3 | 9,987/s | 84 ms | 498 ms | 594 ms | 58,007/s |
| `ANY 1 (s1, s2)` | 2 of 3 | 9,993/s | 99 ms | 463 ms | 619 ms | 55,117/s |

### The stamper and the table sync

The stamper is the worker that gives each record its offset. One stamper
serves every topic in a database, and a second one serves its replicated
topics, so a slow standby cannot hold back the other topics.

| Test | Result |
|---|---|
| Stamp a backlog of 1,000,000 rows, no publishers | 99,512 rows/s, 10.0 s |
| SQL publish, 100 rows a transaction, 8 clients, 1 topic, 30 s | 178,023 rows/s published, 70,826 rows/s stamped while publishing, backlog cleared 46 s later |
| The same, 10 topics | 205,083 rows/s published, 77,143 rows/s stamped while publishing, backlog cleared 39 s later |
| The same, 100 topics | 123,046 rows/s published, 82,323 rows/s stamped while publishing, backlog cleared 13 s later |
| Sync 200,000 records for 20,000 keys into a base table | All stamped after 8.2 s, base table current 9 ms later, 24,384 rows/s |

A publish rate above the stamper rate grows a backlog. Publishes fail when
the backlog is older than the topic's `max_backlog_age`, 60 seconds by
default.

### SQL publish

One `topic.publish` in each transaction, from `pgbench`, for 10 seconds.
Each transaction waits for its own commit, so this is much slower than a
Kafka producer, which sends records in batches. The numbers are
transactions a second.

| Setup | 1 client | 4 clients | 8 clients |
|---|---:|---:|---:|
| One node, `relaxed` | 4,490 | 15,124 | 27,087 |
| One node, `durable` | 393 | 1,204 | 2,425 |
| Three nodes, `FIRST 2`, 3 of 3 copies | 91 | 378 | 671 |
| Three nodes, `ANY 1`, 2 of 3 copies | 115 | 352 | 659 |

### Run the benchmarks

```bash
BENCH_SECONDS=120 bash bench/run_benchmark.sh
SOAK_SECONDS=600 bash bench/run_soak.sh
```

Both need Docker, and the environment in `bench/setup_local_pg.sh`.

## Monitoring

Poll `SELECT * FROM topic.health();` and alert on `ok = false`. The
monitoring views and the alert rules are in [Operate](docs/operations.md).

## Known limits

- One primary does all the writes. Publish throughput does not grow with
  more machines. Standbys give failover and SQL history reads.
- One stamper for each database gives out offsets for every topic in it
  that is not replicated, and a second one for the replicated topics,
  at about 70,000 to 100,000 records a second on the test machine.
- The stamper gives offsets to committed records, so offsets follow commit
  order as the stamper sees it. When one stamper pass sees several
  committed transactions, the one that first wrote to the topic gets the
  lower offsets. Take a SQL transaction that publishes, reads records that another
  transaction committed, and publishes again. It can get offsets below
  those records. Each Kafka `Produce` request is its own transaction, so
  Kafka producers get append order.
- Retention is by time only, and it drops whole partitions. A long query
  anywhere in the database can hold retention until the query ends.
- After a `pg_dump` and a restore into another cluster, a restored record
  with no offset can get a higher offset than a new record. Before the
  dump, stamp every record.
- A restart of PostgreSQL makes every consumer group rebalance once.
- A queue table cannot be renamed or moved to another schema.
- If you publish to a topic, you trust its owner. A trigger that the
  owner puts on the queue table runs as the publisher.
- The Kafka wire codec, the `kafka-protocol` crate 0.18.0, is vendored in
  `vendor/kafka-protocol` with a small patch. The patch stops a crash on a
  malformed request and keeps repeated header names.
  `vendor/kafka-protocol/PATCH.md` holds the full patch. Remove the copy
  when upstream has both fixes.

## Kafka compatibility

pg_topics is one Kafka broker, node 0, that speaks a subset of the Kafka
protocol. This section lists every difference from Apache Kafka.

### What works

- Produce, including idempotent producers, and `gzip`, `snappy`, `lz4` and
  `zstd` batches.
- Fetch, ListOffsets and Metadata.
- Consumer groups with the classic protocol: JoinGroup, SyncGroup,
  Heartbeat, LeaveGroup, OffsetCommit and OffsetFetch.
- Admin: CreateTopics, DeleteTopics, DescribeConfigs, AlterConfigs,
  IncrementalAlterConfigs, ListGroups, DescribeGroups, DeleteGroups and
  DescribeCluster.
- SASL PLAIN over TLS.

These clients pass the test suite: the Java client and tools from
`confluentinc/cp-kafka:7.7.1`, librdkafka 2.15.1 and Confluent JS 1.10.1.
The suite also runs kafkajs 2.2.4 and franz-go 1.22.1, but it only reports
their results. A failure of these two does not fail the suite.

### Not supported

| Kafka feature | pg_topics |
|---|---|
| Transactions and exactly once | Not supported. `InitProducerId` with a `transactional.id` gets `TRANSACTIONAL_ID_AUTHORIZATION_FAILED`. The transaction APIs are not offered. |
| Log compaction | Not supported. `cleanup.policy=compact` is refused. The table sync gives a current state table instead. |
| Size based retention and segments | Not supported. `retention.bytes`, `segment.bytes`, `segment.ms` and other configs are refused. |
| Adding partitions | Not supported. There is no `CreatePartitions`. The band count is fixed when you make the topic. |
| `DeleteRecords`, `OffsetDelete`, `OffsetForLeaderEpoch` | Not offered |
| ACL APIs | Not offered. PostgreSQL grants control access. |
| Quotas and throttling | Not offered. `throttle_time_ms` is always 0. |
| SCRAM, OAUTHBEARER, GSSAPI, mutual TLS, plain text | Not offered. Only SASL PLAIN over TLS works. |
| Re-authentication, KIP-368 | Not offered. A second SASL request closes the connection. |
| The new consumer group protocol, KIP-848 | Not offered. Use `group.protocol=classic`, the default. |
| Share groups and streams groups | Not offered |
| Static membership, `group.instance.id` | Ignored. A restarted member joins as a new member. |
| Fetch sessions, KIP-227 | Not offered. Each `Fetch` is a full fetch. |
| Follower fetch and racks | Not offered. The listener serves the primary only. |
| Topic IDs | Not used. Metadata, Produce and Fetch stop below the versions that need topic IDs. |
| Leader epochs | Always -1. A client cannot detect log truncation after a failover. |
| Multiple brokers, reassignment, leader election, log dirs | Not offered. There is one broker. |
| Auto topic creation | Never. A topic must exist before a client uses it. |
| Several named listeners | Not offered. One TLS port for each database, and one advertised host. |
| Broker configs | `DescribeConfigs` returns nothing for a broker resource. |
| Delegation tokens, `UpdateFeatures`, telemetry | Not offered |

### Behaves differently

Records:

- A value must be valid JSON. A batch with any other value fails with
  `INVALID_RECORD`, for the whole partition batch.
- `Fetch` returns the value as PostgreSQL `jsonb` prints it. Field order
  and white space can change, and duplicate JSON keys collapse. A
  signature over the raw bytes does not match.
- A key must be UTF-8 text of 40 characters or fewer.
- A header name must be UTF-8, or the batch fails with `CORRUPT_MESSAGE`.
- The record timestamp is always the server time of the insert, as
  `LogAppendTime`. The producer's own timestamp is stored in
  `producer_timestamp` but is not sent back over Kafka.
  `message.timestamp.type` is read-only.
- `Fetch` sends uncompressed batches, whatever codec the producer used.
- `Fetch` builds new batches. The producer id, the sequence and the batch
  boundaries of the producer are not kept.
- `max.message.bytes` is one server setting,
  `pg_topics.max_message_bytes`, default 1048576. It is read-only on each
  topic. One request can be at most that plus 64 KiB. A larger request
  closes the connection with no error code. Kafka's default request limit
  is 100 MB.

Topics:

- A topic name is `schema.table_q`: a schema of 1 to 63 characters and a
  table of 1 to 47, from `A-Z`, `a-z`, `0-9`, `_` and `-`. A bad name gets
  `INVALID_REQUEST`. Kafka gives `INVALID_TOPIC_EXCEPTION`.
- A topic that does not exist gets `TOPIC_AUTHORIZATION_FAILED`, the same
  answer as a topic that the role may not see. Kafka gives
  `UNKNOWN_TOPIC_OR_PARTITION`. A Java client treats it as an
  authorization error, not as a retriable error.
- `CreateTopics` with `--partitions -1` gives 4 bands. Kafka uses the
  broker default. The band count is 1 to 1024.
- `CreateTopics` ignores a manual partition assignment and `timeout_ms`.
- `CreateTopics`, `AlterConfigs` and `IncrementalAlterConfigs` accept only
  `retention.ms` and `pg_topics.min_durability`. Every other config gets
  `INVALID_CONFIG`. So tools that make compacted topics for their own
  state, such as Kafka Connect in distributed mode, Kafka Streams and
  Schema Registry, are expected to fail. They are not tested.
- `retention.ms` must be from 1 ms to 100 years. `-1`, keep forever, is
  refused.
- Retention drops whole time partitions, 1 day wide for a topic made over
  Kafka. A record stays for at least `retention.ms` and at most one day
  more.
- `Metadata` lists one replica and one in-sync replica for each band, also
  for a topic made with a replication factor above 1. The read-only config
  `pg_topics.replication_factor` gives the real number of copies.
- A replication factor above 1 is accepted only when synchronous standbys
  keep that many copies. See [Replication factor](docs/configuration.md#replication-factor).
- There is no `min.insync.replicas`.
- The cluster id is the PostgreSQL `system_identifier` as a decimal
  string. Kafka uses a base64 UUID.

Producers:

- A record gets its offset from the stamper after the commit. With
  `acks=1` or `acks=all`, `Produce` waits for the offsets. When they do not
  come within the request timeout, the answer is `REQUEST_TIMED_OUT`,
  although the records are stored. An idempotent producer's retry gets
  `DUPLICATE` with the first offsets. A producer that is not idempotent
  writes the records again.
- `acks=0` and `acks=1` ask for no disk flush, but the topic's durability
  floor still applies. With the default `durable` floor, every record is
  flushed to disk.
- When the stamper is too far behind, `Produce` gets
  `UNKNOWN_SERVER_ERROR`, which a client does not retry.
- `InitProducerId` always gives a new producer id with epoch 0. It ignores
  the producer id and epoch in the request.
- The first sequence of a new producer does not have to be 0.
- A producer id that is idle for more than 7 days is removed. The producer
  then gets `UNKNOWN_PRODUCER_ID` and must restart.
- At most 5 `Produce` answers can wait on one connection.
- A `Produce` whose rows a tenant trigger drops gets `base_offset = -1`
  with no error.

Consumers and groups:

- Group names are shared by the whole database. The first role that uses
  a name owns the group.
- `OffsetCommit` checks the generation only. A commit from an unknown
  member with the right generation succeeds. Kafka gives
  `UNKNOWN_MEMBER_ID`.
- `OffsetCommit` with a lower offset than the stored one, in a live
  generation, does nothing and returns no error. Kafka moves the offset
  back. A reset of an empty group with `kafka-consumer-groups` still works.
- Commit metadata is not stored. `OffsetFetch` returns an empty string.
- Committed offsets never expire. Kafka removes them 7 days after a group
  becomes empty.
- `isolation.level` is ignored. There are no aborted transactions, so
  `read_committed` and `read_uncommitted` read the same records.
- `ListOffsets` with the latest timestamp, `-3`, returns the last offset
  but no timestamp.
- `ListOffsets` by time uses the server time of the insert. Offsets follow
  transaction order, not insert time, so a time lookup is close but not
  exact.
- `Fetch` waits at most 30 seconds, and returns at most 2,000 records for
  each band. `fetch.min.bytes` is checked against an estimate of the
  record size.
- The first join window of an empty group is one fixed window,
  `pg_topics.group_initial_rebalance_delay_ms`, default 3000 ms.
- `DeleteGroups` on a group that does not exist gets
  `GROUP_AUTHORIZATION_FAILED`. Kafka gives `GROUP_ID_NOT_FOUND`.
- `ListGroups` also shows the table sync groups, named
  `__pg_topics_sync:<schema>.<table>`.

Connections and security:

- Only SASL PLAIN inside TLS. The user name and password are a PostgreSQL
  role and its password.
- At most 64 connections can wait to log in, for at most 10 seconds each.
  The server accepts at most `pg_topics.max_clients` logged-in clients,
  default 100. An idle connection closes after 600 seconds.
- A request with an API key or a version that pg_topics does not offer
  closes the connection.
- Before the login, only `ApiVersions`, `SaslHandshake` and
  `SaslAuthenticate` are accepted, in frames of at most 64 KiB.
- There are no JMX metrics. Monitoring is in SQL.

librdkafka:

- The librdkafka default partitioner, `consistent_random`, does not agree
  with the partitioner of a keyed SQL publish. Set
  `partitioner=murmur2_random`. The Java, kafkajs 2.x and franz-go
  defaults agree.

### API versions offered

| API | Versions |
|---|---|
| `ApiVersions` | 0 to 3 |
| `SaslHandshake` | 1 |
| `SaslAuthenticate` | 0 to 2 |
| `Metadata` | 0 to 12 |
| `Produce` | 3 to 9 |
| `Fetch` | 4 to 12 |
| `ListOffsets` | 1 to 7 |
| `FindCoordinator` | 0 to 4 |
| `JoinGroup` | 0 to 9 |
| `SyncGroup` | 0 to 5 |
| `Heartbeat` | 0 to 4 |
| `LeaveGroup` | 0 to 5 |
| `OffsetCommit` | 2 to 8 |
| `OffsetFetch` | 1 to 8 |
| `InitProducerId` | 0 to 4 |
| `CreateTopics` | 2 to 7 |
| `DeleteTopics` | 1 to 5 |
| `DescribeConfigs` | 1 to 4 |
| `AlterConfigs` | 0 to 2 |
| `IncrementalAlterConfigs` | 0 to 1 |
| `DeleteGroups` | 0 to 2 |
| `DescribeCluster` | 0 to 1 |
| `ListGroups` | 0 to 4 |
| `DescribeGroups` | 0 to 5 |

`Produce` from version 3 and `Fetch` from version 4 mean that a client
older than Kafka 0.11 cannot connect.
