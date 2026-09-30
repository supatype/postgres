# Tune pg_topics

This guide gives the speed of pg_topics, its limits, and the settings that change the numbers. Every number comes from `bench/run_benchmark.sh` on one machine. The full tables and the machine are in [Performance](../README.md#performance). Measure on your own hardware before you size a deployment.

## The short version

- Keep one node at about 10,000 records a second for low latency. At that rate the median `Produce` answer took 31 to 34 ms and the stamper stayed within 4 ms.
- One producer peaked at about 28,000 records a second of 200 bytes. 4 producers together peaked at about 44,000.
- Bytes limit the speed as much as records do. One producer reaches about 4 to 6 MB a second for any record size.
- The stamper gives out about 70,000 to 90,000 offsets a second for a whole database. A higher publish rate grows a backlog. Publishes fail when the backlog passes `max_backlog_age`.
- A 10 minute soak at 10,000 records a second held a p99 of 215 ms, with no backlog.
- Reads are cheap. One consumer group read about 61,000 to 64,000 records a second. 8 groups at once read about 161,000 records a second in total.
- A SQL publish of one row for each transaction waits for its own disk flush. Put many rows in one transaction.

## Producers

### Keep the default: idempotent, `acks=all`

With the default `durable` floor, the listener flushes every record to disk for any `acks` value. So `acks=1` gains nothing. One producer sent 28,066 records a second with `acks=1` and no idempotence, and 27,910 with the defaults. Keep `enable.idempotence=true`. It makes a retry safe.

### Use compression

`lz4` raised one producer from 27,910 to 32,557 records a second, 17% more. The listener unpacks the batch and stores the record as `jsonb`. So compression saves network time only. `Fetch` sends uncompressed batches.

### Record size

| Record size | Records a second, 1 producer | MB a second |
|---|---:|---:|
| About 200 bytes | 27,910 | 5.3 |
| About 1 KB | 6,348 | 6.2 |
| About 10 KB | 447 | 4.3 |

pg_topics parses each record value as JSON and stores it as `jsonb`, so a larger record costs more work. Plan by bytes a second, not only by records.

### Batch size and the message limit

The benchmarks use the Java defaults for `batch.size` and `linger.ms`. A batch must fit in `pg_topics.max_message_bytes`, default 1 MiB. One `Produce` request must fit in `pg_topics.max_message_bytes` plus 64 KiB, across all its partitions. A larger request closes the connection.

Other batch settings are not measured.

### Latency at top speed

At top speed a producer fills its own send buffer, so the time from send to answer grows to seconds. For low latency, stay below the top speed. At a fixed 10,000 records a second the p99 was 71 to 111 ms.

## Bands

A band is a Kafka partition.

- The band count is the most consumers that one group can use at once. Choose at least the number of consumers that you plan for. The maximum is 1024.
- You cannot change the band count later.
- At 10,000 records a second, 4 bands and 12 bands gave the same median latency, 34 ms and 31 ms.
- A key with few distinct values puts most records on a few bands. The consumers that own those bands then do most of the work.

## The stamper

One stamper worker gives out offsets for every topic in its database, one topic after another. A second stamper does the same for the replicated topics. Their commits wait for the synchronous standbys to apply, so a slow standby holds back only those topics. The stamper limits the publish throughput.

| Load, SQL, 100 rows a transaction, 8 clients | Published | Stamped while publishing | Backlog left, cleared after | Largest `backlog_age` |
|---|---:|---:|---:|---:|
| 1 topic | 178,023 rows/s | 70,826 rows/s | 46 s | 17.6 s |
| 10 topics | 205,083 rows/s | 77,143 rows/s | 39 s | 17.6 s |
| 100 topics | 123,046 rows/s | 82,323 rows/s | 13 s | 7.0 s |

Each run published for 30 seconds. With no publishers, the stamper gave out offsets to a backlog of 1,000,000 rows at 99,512 rows a second.

The number of topics does not lower the stamper rate. 100 topics stamped as fast as 1.

What to do:

- Keep the sustained publish rate of each database below about 70,000 records a second.
- Each database in `pg_topics.databases` has its own stamper. To go higher, spread topics over more databases. Each database costs 4 worker slots and one port.
- `max_backlog_age`, default 60 seconds, sets how long a burst may run above the stamper rate before publishes fail. To allow longer bursts, raise it with `topic.set_backlog_limit`. A larger backlog uses more disk and makes consumers wait longer.

## Consumers

| Readers | Records a second, in total | Slowest group |
|---|---:|---:|
| 1 group | 61,069 to 63,586 | |
| 4 groups at once | 131,292 | 32,669 |
| 8 groups at once | 161,282 | 17,686 |

Each group read a 12 band topic of 1,200,000 records from the start. A `Fetch` returns at most 2,000 records for each band.

- You can move history reads in SQL to a streaming standby with `topic.fetch`. The Kafka listener serves the primary only.

## SQL publish

| Setup | 1 client | 4 clients | 8 clients |
|---|---:|---:|---:|
| One row a transaction, `durable` | 393 | 1,204 | 2,425 |
| One row a transaction, `relaxed` | 4,490 | 15,124 | 27,087 |
| 100 rows a transaction, `durable`, 8 clients | | | 178,023 rows/s |

The numbers are transactions a second for one row a transaction.

- Each `durable` transaction waits for its own disk flush. Many rows in one transaction share one flush.
- `relaxed` does not wait for the flush, so it is much faster. A crash can lose the last records. Use it only for data that you can lose.

## The table sync

The sync worker kept a base table current at the same speed as the stamper. The stamper stamped 200,000 records for 20,000 keys after 8.2 seconds. The base table was current 9 ms later. That is at least 24,000 records a second.

One sync worker serves every synced topic in a database, one after another.

## Replication

A `replicated` topic waits until the required standbys apply each commit.

| `synchronous_standby_names` | Copies at the answer | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|
| `FIRST 2 (s1, s2)` | 3 of 3 | 84 ms | 498 ms | 594 ms |
| `ANY 1 (s1, s2)` | 2 of 3 | 99 ms | 463 ms | 619 ms |

These tests ran at a fixed 10,000 records a second, with all 3 nodes on one host. A real network adds its round trip to each commit.

On this machine the two setups gave about the same latency. Both were 2 to 4 times slower than one node. For 2 copies, use a quorum, `ANY 1 (s1, s2)`. Then one stopped standby does not block commits.

A SQL publish of one row a transaction on 3 nodes gave 91 to 115 transactions a second with 1 client, and 659 to 671 with 8 clients.

## Connections

Each Kafka client holds one PostgreSQL connection and one thread in the listener.

- Set `pg_topics.max_clients` to the most Kafka clients that you expect.
- Keep `max_connections` above `pg_topics.max_clients` plus every other client.
- An idle Kafka connection closes after 600 seconds.

## Storage and retention

- pg_topics splits each topic into time partitions of `partition_interval`, default 1 day. Retention drops whole partitions, so old data costs no `DELETE` and no vacuum.
- The stamper updates each row once to set its offset. That leaves one dead row version for each record in the current partition. Autovacuum cleans it up. `topic.write_partition_dead_tuples` shows the count.
- For a short retention, use a short `partition_interval`, such as `1 hour`. Retention can then drop data closer to the limit. Many small partitions cost more planning time for queries on the whole topic.
- Put the WAL on a fast disk. Every `durable` commit waits for a WAL flush.

## The test machine

The numbers in this guide come from one machine. It has an AMD Ryzen 9 5900X with 12 cores, 31 GiB of RAM, Windows WSL2, PostgreSQL 17.11 and a release build of pg_topics. The Kafka clients were the Java tools from `confluentinc/cp-kafka:7.7.1` in Docker Desktop. Docker Desktop adds one network hop through Windows. Records were about 200 bytes of JSON unless the table says otherwise.
