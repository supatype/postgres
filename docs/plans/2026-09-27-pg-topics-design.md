# pg_topics

Kafka inside Postgres. Draft, September 2026.

`pg_topics` is a Postgres extension that speaks the Kafka wire protocol. A Kafka client connects to
port 9092 and produces and consumes with its own library, unchanged. Every topic is a table you can
query in SQL, and a topic can keep a second table current, one row per thing.

It ships in `Supatype/Postgres/extensions/pg_topics`, alongside `pg_keyspace`, `pg_guard` and
`supatype_mask`. Rust on pgrx 0.12.9, PostgreSQL License, and buildable against anyone's Postgres.

---

## Who it is for

Teams running a three broker Kafka cluster, Kinesis, or DynamoDB Streams to move a few thousand
events a second. They pay for the cluster, the operator, the schema registry and a connector
pipeline.

If you need millions of events a second, stay on Kafka.

The topic is a table, so the history is already in your database. Ordinary SQL queries it, your
backups cover it, and your permissions protect it. There is no Kinesis into Clickpipes into
ClickHouse, and no Kafka into Debezium into a warehouse. A publish can sit inside your own
transaction, which removes the outbox pattern.

## Limits

Three limits are visible before you start.

| Limit | Detail |
|---|---|
| The record value is JSON | Nothing else. A value that will not parse is refused. |
| The record key is UTF-8, 40 characters or fewer | Longer or binary is refused. |
| A topic is named `schema.table_q` | Not what your Kafka topics are called today. |

## What it does not do

- Kafka transactions, and so exactly once. At least once with idempotent handlers is the contract.
- Binary record values or binary keys.
- Avro, Protobuf and any other format that needs a schema registry to read.
- Follower fetching, rack awareness, or any multi broker feature.
- Massive scale.
- Log compaction on the queue table. The base table is the compacted view, and the queue keeps
  everything until retention drops it.
- A connector ecosystem. Kafka Connect talks the protocol, so some connectors may work. None are
  tested.
- A cutover runbook from Kafka. Replay and per group reset are the primitives. Nothing here
  describes a dual publish period, a backfill or a consumer cutover.
- Cross topic transactions across databases. Two topics in one database are already atomic together.
- Server side filtering over the wire.

## What was rejected, and why

| Option | Why not |
|---|---|
| Queue table with `FOR UPDATE SKIP LOCKED` | No order per key across parallel workers. |
| `pg_advisory_xact_lock(topic)` at publish, offset assigned inline | Every publisher waits for the slowest open transaction. |
| Logical decoding instead of the whole design | No `log_offset` column, so no offset query over history. |
| Logical decoding for the ordering step only | Removes `track_commit_timestamp`, the clock rule and the advisory lock, because write ahead log order is already commit order. Costs one replication slot per database, and a stalled apply worker grows write ahead log retention instead of an index. `Fetch` still needs `log_offset` on the row, so the second write stays. Worth a spike before item 4. |
| SQL only, with a client library per language | Kafka clients cannot connect. |
| A bespoke wire protocol on `pg_keyspace`'s Redis port | No standard client speaks it. |
| An existing Rust Kafka protocol crate, instead of writing the codec | Worth trying. Nobody has checked whether one builds under `pgrx` and its allocator. Answer before item 7. |
| `pg_partman` for partitions | Does not do the detach, check, drop order that retention needs. |
| Avro, Protobuf or raw bytes as the record value | Cannot be read without a schema registry, which is the thing being removed. |

---

## Naming

Three objects, one root name.

| Object | Name | Exists when |
|---|---|---|
| Base table | `bottles` | You want rows kept up to date |
| Queue table | `bottles_q` | Always |
| Error table | `bottles_qe` | The base table exists |

The Kafka topic name is the queue table's name, always. A consumer subscribes to `public.bottles_q`.

Warning: this is a rename for anyone arriving from Kafka. A producer aimed at `orders` gets
`UNKNOWN_TOPIC_OR_PARTITION`, because the topic is `public.orders_q`. The client library does not
change. The topic string does.

Warning: a Kafka name with several dots, such as `com.example.orders.created`, cannot be expressed.
The first dot splits schema from table, and neither part may contain another dot.

Two routines create a topic:

1. Attach to a table you already have. Write ordinary `CREATE TABLE` DDL, then call the attach
   function with the table and the key column.
2. Create both together. One call makes the base table from a column list and attaches the queue.

A Kafka client calling `CreateTopics` gets a queue table alone, with no base table and no sync. It
names the topic itself, suffix included.

---

## Prerequisites

Cluster settings, all needing a restart. On the Supatype image they are image config. Elsewhere they
are the operator's job.

| Setting | Value | Why |
|---|---|---|
| `track_commit_timestamp` | `on` | The stamper orders a batch by commit timestamp. |
| System clock | Slew only, never step | A backward step reorders concurrent commits. |
| `pg_topics.port` | 9092 | Per database, set with `ALTER DATABASE`. Kafka's default. Set it to 0 and no listener starts. |
| `pg_topics.advertised_host` | Reachable name | What `Metadata` tells a client to reconnect to. Defaults to `localhost`, which is wrong in a container. |
| `pg_topics.max_clients` | Your ceiling | Each connected Kafka client costs one backend. Set `max_connections` above it. |
| `pg_topics.max_message_bytes` | 1048576 | Matches Kafka. A larger batch is refused. |
| `pg_topics.failover_is_fenced` | `on` | Required before a topic accepts `durable` or `replicated`. |
| `synchronous_standby_names` | Set | Only for the `replicated` tier. |

PostgreSQL 17 is the tested version. The pgrx feature flags cover 14 to 17. Versions 14 to 16 are
untested and unsupported.

A new release needs an instance restart, because the background workers are loaded by the
postmaster.

Warning: a restart takes longer than any realistic `session.timeout.ms`. Every consumer group member
in every database expires at once, and every group rebalances when clients reconnect. Drain
consumers first, or say so in the release notes.

---

## The queue table

Every queue table has the same shape. Only the band count differs.

```sql
CREATE TABLE bottles_q (
    seq                bigint      NOT NULL GENERATED ALWAYS AS IDENTITY,
    log_offset         bigint,
    band               smallint    NOT NULL CHECK (band BETWEEN 0 AND 3),
    key                varchar(40),
    value              jsonb,
    headers            jsonb,
    published_by       name        NOT NULL DEFAULT current_user,
    published_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
    producer_timestamp timestamptz,
    PRIMARY KEY (published_at, seq)
) PARTITION BY RANGE (published_at);
```

| Column | Meaning |
|---|---|
| `value` | The record, as JSON. Refused at publish if it is present and will not parse. SQL NULL is a tombstone. |
| `key` | The record key, as text. Routing only, not an identity. |
| `headers` | Kafka record headers. |
| `published_by` | The Postgres role that wrote it. Kafka records do not carry this. |
| `published_at` | The server's clock when the record arrived. |
| `producer_timestamp` | What the producer claimed. Kept, never trusted. NULL when the record carried Kafka's no-timestamp marker. |
| `log_offset` | The log position, per band, from 0. NULL until the stamper fills it. |
| `seq` | Publish order. Breaks ties inside one commit timestamp. No consumer sees it. |

The `CHECK` on `band` is generated from that topic's `band_count`, which is 4 here. It is not a
fixed range.

There are no per topic columns. The record's fields are pulled out later, when the sync writes down
into the base table. A field with no column to go to still sits in `value`, so it is still there
after you widen the base table.

`seq` comes from one sequence shared by every partition, not one per partition. That was checked on
a live PostgreSQL 17.11 instance. It matters, because the stamper falls back to sorting by `seq`
alone when commit timestamps have been truncated.

`log_offset` avoids `offset`, and `group_name` elsewhere avoids `group`. Both are reserved words in
Postgres.

The primary key is per partition, because a partitioned table's key must include the partition key.

### What the wire allows, and what is refused

A Kafka record carries a key, a value, headers and a timestamp. There is no fifth field, so anything
extra rides in headers or the server works it out. `published_by` is the second kind.

| Refused at publish | Error |
|---|---|
| A value that is present and is not JSON | `INVALID_RECORD` |
| A key that is not valid UTF-8, or longer than 40 characters | `INVALID_RECORD` |
| A batch above `pg_topics.max_message_bytes` | `MESSAGE_TOO_LARGE` |

Warning: a record batch carries a CRC32C over its own bytes, and a Java consumer checks it by
default through `check.crcs`. `Fetch` builds each batch from rows, so the codec computes that CRC
itself. Get it wrong and every fetched batch is rejected as corrupt. Put it in the client test
matrix beside the partitioner's golden file.

Warning: `Fetch` returns re-serialised JSON, because the value is stored as `jsonb`. Field order and
whitespace change. A consumer that parses the value is unaffected. A consumer that hashes the raw
bytes or checks a signature over them is not, and the original bytes are not kept.

`published_at` is always the server's clock, so `message.timestamp.type` reports `LogAppendTime` and
`Fetch` returns `published_at` as the record timestamp. `ListOffsets` by timestamp searches
`published_at`, which prunes partitions instead of scanning. `published_by` is a SQL side column and
is not returned over the wire.

### Bands

`band` is what Kafka calls a partition. Set the count at creation, store it in `topic_config`,
default 4, any value from 1 to 1024. A high count makes a consumer fetch from every band on each
poll.

A key buys ordering and costs even spread. Two records with the same key always land in the same
band and always arrive in order. A random key, or none, spreads the load and promises nothing about
order. Kafka has the same trade. The default is a UUID per record, which chooses spread.

Warning: the broker must never compute `band` from the key. In Kafka the producer picks the
partition and sends the answer in the `Produce` request. A server side hash would disagree and split
order per key, with no error. The `Produce` handler writes whatever band the client chose.

The SQL publish function runs Kafka's own partitioner, so a SQL publisher and a Kafka producer
agree. That is 32 bit MurmurHash2 with seed `0x9747b28c`, taken as `(hash & 0x7fffffff) %
band_count`. `pg_keyspace`'s `core/src/prob.rs` uses 64 bit murmur64a with a different seed, so it
is the wrong function here. Pin it with a golden file of key to band pairs, generated by a real Java
client and by `librdkafka`. Test against that file in CI.

A null key gets no hash. Kafka's default partitioner spreads unkeyed records, so the SQL publish
function keeps a round robin counter per topic and takes the next band.

The create function builds the `CHECK` from a validated integer, never from client text. `UPDATE` on
`topic_config.band_count` is revoked from every role, including the table's owner. A revoke that
leaves the owner out leaves every `SECURITY DEFINER` function it owns able to write the column.

Warning: raising the band count breaks order per key, exactly as adding Kafka partitions does.
Lowering it is not possible. Treat a change as creating a new topic.

Warning: the revoke protects the bookkeeping number, not the rule. The real limit is the `CHECK` on
the queue table, and the tenant owns that table, so they can drop and replace that constraint.
`topic_config.band_count` would still say 4 while the table accepted 8. The event trigger watches
for it and alerts. Nothing prevents it.

### Indexes

| Index | Purpose |
|---|---|
| unique btree `(band, log_offset) WHERE log_offset IS NOT NULL`, on each partition | The `Fetch` read, and a duplicate offset is rejected on write. |
| btree `(seq) WHERE log_offset IS NULL` | Lets the stamper find unstamped rows. |
| BRIN `(log_offset)` | A scan of old history. |

The partition worker declares the first one on each leaf partition, not on the parent. A unique
index on a partitioned parent must include the partition key, and `published_at` is not in this
index. A unique index on a leaf carries no such rule.

The second index stays small while the stamper keeps up, because a row leaves it once stamped. If
the stamper stops, it grows with every publish.

BRIN only helps when offset order and physical row order line up. The stamper rewrites each row when
it stamps it, and nobody has measured where the new version lands. Poor correlation costs scan time,
not correctness.

Retention drops whole partitions, so no index needs a bulk delete.

Add a GIN index on `value` when anyone queries history by field.

---

## Offsets

If every publisher takes its own offset from a sequence, messages go missing. Transaction A takes
offset 100 and transaction B takes 101. B commits first. A consumer reads
`WHERE log_offset > 99 ORDER BY log_offset`, sees 101 and commits 101. Then A commits. Offset 100 is
behind the consumer's position and nobody reads it.

One process assigns every offset on a topic, and only to rows that have already committed. That is
what a Kafka partition leader does.

### The stamper

A publish inserts its row with `log_offset` NULL. The stamper finds committed rows with no offset
and fills in consecutive offsets, per band. An uncommitted row is invisible to it.

One transaction does a whole batch across every band of the topic. It reads each band's
`next_offset`, stamps that band's rows, and writes the counters back. If the stamper dies part way,
the transaction rolls back and the next run stamps them instead. The counters live in a table, not a
sequence, so a crash cannot burn numbers.

It orders each band's rows by `pg_xact_commit_timestamp(xmin)`, then by `seq`. Rows of one
transaction share a commit timestamp, so `seq` puts them back in publish order. It runs at READ
COMMITTED, and each batch takes a fresh snapshot.

One stamper worker runs per database and services every topic in turn. It takes a session level
advisory lock before stamping and releases it before moving on. The key is the two integer form: a
fixed namespace number for `pg_topics`, and `hashtext(schema_name || '.' || topic)`. That is a 32
bit hash, so two topics can collide. A collision costs a little blocking between two stampers during
a rolling restart, and never a safety violation.

The price is stamp latency. A database with 200 topics stamps each far less often than a database
with one. Splitting the stamper per band is the upgrade path for one hot topic, and it is not in v1.

The advisory lock goes when the connection drops, so a stamper must take it again before stamping.
Restart with a backoff and alert on a restart loop.

### The ordering guarantee

Offsets follow the order in which rows became visible. A row does not exist before its commit, so no
reader could have seen another order. A transaction acknowledged before another started is always
visible first, so causal order is safe.

The commit timestamp only breaks ties inside one batch, where every row is already visible. Two rows
in one batch were concurrent and no observer could tell which came first.

Warning: a clock that steps backwards reorders rows inside a batch. Configure the clock to slew and
never step. The write ahead log position of a commit is the only value monotonic by construction,
and Postgres exposes no SQL function for it.

Warning: Postgres truncates commit timestamps against the cluster freeze horizon. The stamper then
gets NULL for the oldest unstamped rows. Stamp those in `seq` order, record the range in
`seq_ordered_to`, and alert. Do not skip them and do not fail.

### What this buys

- A `Fetch` reads `WHERE band = :b AND log_offset IS NOT NULL AND log_offset >= :from`. Nothing
  tracks the oldest open transaction, and no lower offset can appear later.
- Offsets have no holes, so a Kafka client's own gap detection stays quiet.
- A long transaction that publishes holds up nothing but its own rows. The one exception is
  retention's detach of the partition it is writing into.
- There is no in-flight registry to keep or clean up.

### What it costs

A consumer sees an event one stamper cycle after it commits. Run the stamper continuously and that
is a millisecond or two.

The stamping write cannot be a HOT update, because `log_offset` appears in both partial indexes.
Every event costs a second heap tuple, two index changes, and a dead tuple, on top of the insert.
The partition being written has continuous autovacuum work.

`topic_offsets` takes an update per band per commit on a table that never grows. Set a low
fillfactor so those updates stay HOT.

Nobody has measured the stamper's ceiling.

### Backlog limit

A publish fails when `backlog_age` exceeds `max_backlog_age`, both on that topic's config row. The
stamper writes the value each cycle and a publish reads that one row. Do not compute it per publish.
The unstamped index carries `seq`, not `published_at`, so finding the oldest row means a heap fetch.
That one row read is still an extra round trip on every publish, so count it separately when the
publish path is measured.

Without the limit, a stuck stamper grows the unstamped index without bound while retention refuses
to free disk, and publishers carry on.

### Duplicate offsets

The per partition unique index catches a duplicate inside one partition. A scheduled query catches
the rest. Bound it to the newest partition each run and sweep the whole history on a slower cadence.

Warning: an advisory lock is local to one instance. It is not in the write ahead log and a replica
has its own empty set. If a failover promotes a replica while the old primary still takes writes,
two stampers assign the same offsets on two timelines. `pg_topics` cannot detect that, which is why
`pg_topics.failover_is_fenced` must be set before a topic accepts `durable` or `replicated`.

Fencing is not enough on a cluster you do not operate, so the stamper writes its own node identity
into `topic_band_position.stamped_by` on every batch. A split then shows up as a value that changed
under you, which the duplicate check reports.

There is no automatic repair. Stop the stamper for that topic and find the split point from the
commit timestamps of the duplicated rows.

Warning: verify before you touch anything. Compare row counts and a checksum across the two
timelines, and confirm the chosen point is at or below the lowest `committed_offset` any group
holds. Only then renumber one side or truncate back, and reset every group.

---

## Writing down into a table

This is the part Kafka does not do. Publish into `bottles_q`, and `bottles` stays current.

A sync worker reads the queue in offset order and pulls the record's fields out of `value`. It
upserts one row per thing into the base table.

```sql
CREATE TABLE bottles (
    bottle_id  uuid        PRIMARY KEY,
    name       text        NOT NULL,
    abv        numeric(4,2),
    event_at   timestamptz NOT NULL
);
```

`bottles` is yours. You made it, you own it, and you may add columns and indexes to it freely. The
sync only writes the columns that match a field in the record, plus `event_at`.

Sync is opt in. Declare a key column and you get it. Declare none and you get `bottles_q` alone,
which is a plain Kafka topic with a queryable history. `sync_enabled` turns it off without unpicking
anything.

### The key that identifies a row

The sync key is a column on the base table, named in `topic_config.sync_key`, filled from the field
of the same name inside `value`. Here it is `bottle_id`.

It is not the record key. The record key routes to a band and is usually random, so two updates to
one bottle can arrive on different bands in either order.

`event_at` holds the `published_at` of the record that produced the row. Every band's `published_at`
comes from the same server clock, so the comparison holds across bands.

The comparison is part of the write, not a read before it:

```sql
INSERT INTO bottles (bottle_id, name, abv, event_at)
VALUES (:id, :name, :abv, :event_at)
ON CONFLICT (bottle_id) DO UPDATE
   SET name = EXCLUDED.name, abv = EXCLUDED.abv, event_at = EXCLUDED.event_at
 WHERE bottles.event_at < EXCLUDED.event_at;
```

Every writer goes through this one statement, including `retry_errors`. A read then a write leaves a
gap, and `retry_errors` is a second writer that can run at any time. Without the guard, an operator
retrying an old failed record while the sync applies a newer one puts the row back to older data.
No error is raised and no trace is left.

A record with a key and a null value deletes that row, exactly as a Kafka compacted topic does. That
is why `value` allows NULL. The JSON scalar `null` is a different value and stores as
`'null'::jsonb`, so a producer sending it deletes nothing. The record stays in the queue, so the
history still shows the delete.

Warning: reduce a batch to one row per sync key before the upsert. Keep the newest `event_at` and
break a tie on the higher `(band, log_offset)`. Postgres refuses an `INSERT ... ON CONFLICT DO
UPDATE` whose rows collide on the conflict target inside one statement, with `command cannot affect
row a second time`. Two updates to one bottle in one batch is the ordinary case. A bulk upsert
without the reduction throws and stalls forever on the retry. Without the tie-break, a replay of the
same batch can pick a different row.

### Matching a record to the table

| Case | What happens |
|---|---|
| A field with no matching column | Ignored. It stays in `value` in the queue. |
| A missing field, column allows nulls | Written as null. |
| A missing field, column is `NOT NULL` | Goes to `bottles_qe`. |
| A value that will not cast to the column's type | Goes to `bottles_qe`. |

Forgiving about shape, strict about types.

### When a write down fails

Nothing stops. The record is already in `bottles_q` and consumers still read it. A copy goes to
`bottles_qe` with the reason, and the sync carries on with the next record.

```sql
CREATE TABLE bottles_qe (
    band               smallint    NOT NULL,
    log_offset         bigint      NOT NULL,
    seq                bigint      NOT NULL,
    key                varchar(40),
    value              jsonb,
    headers            jsonb,
    published_by       name        NOT NULL,
    published_at       timestamptz NOT NULL,
    producer_timestamp timestamptz,
    failed_at          timestamptz NOT NULL DEFAULT now(),
    error              text        NOT NULL,
    PRIMARY KEY (band, log_offset)
);

CREATE INDEX ON bottles_qe (failed_at);
```

It holds the whole record, not a pointer to it. The queue has retention and this table does not. A
row here still replays a year later, when the record it came from is gone.

Nothing clears it. You look at what failed, fix the cause, and then do one of three things. Widen
`bottles`. Write your own `INSERT ... SELECT` to transform the rows in. Or call `retry_errors` to
run them through the sync again. A row that succeeds on retry is deleted. A row that fails again has
its `error` and `failed_at` updated in place, because `(band, log_offset)` is its primary key.

Warning: with no stop and no expiry, a schema mistake that fails every record fills this table at
the full rate of the topic. Its row count is on the monitoring list for that reason.

### Keeping up with your `ALTER TABLE`

You widen `bottles` with plain DDL, at any time, with no control plane function involved. The next
batch has to see the new column, and the sync cannot re-read the schema per record.

`topic_config.shape_version` is an integer. Two database wide event triggers maintain it.

On `ddl_command_end`, read `pg_event_trigger_ddl_commands()`. If the statement touched a base table
with a queue, increment that topic's `shape_version`. The same trigger watches for a `CHECK`
constraint change on a queue table and alerts, which is the only way band count drift is noticed.

On `sql_drop`, stop that topic's sync, so a dropped base table halts cleanly rather than failing
every record. The same trigger matches a dropped queue table and removes that topic's control rows.
Table ownership carries `DROP TABLE`, and no function can take that away. One statement can drop
both tables, so the trigger stops the sync before it removes the control rows.

Both functions are `SECURITY DEFINER` with `SET search_path = pg_catalog, pg_temp`. They take no
arguments and read only server generated output.

Warning: both wrap their whole body in an exception block that logs and swallows. They fire inside
every DDL statement in the database, including statements from tenants who have never made a topic.
An unhandled error inside one aborts that tenant's `CREATE TABLE`. Missing a `shape_version` bump
costs one stale batch. Breaking DDL for the database costs everybody.

A refresh also checks that `sync_key` still names a column. Drop that one column and the upsert has
no conflict target. The sync then stops the same way a dropped base table stops it.

The sync caches the column list and the version it was built from. It already reads that topic's row
at the start of each batch to get its position, so the version check costs nothing extra. Same
version, use the cache. Different, re-read the columns once. That covers added columns, dropped
columns and changed types alike.

### The sync is a consumer

It reads in offset order and saves a position, so it uses `topic_offsets` under a reserved group
name. `DescribeGroups` reports it, so `kafka-consumer-groups` and any Kafka UI show how far behind
your table is. Resetting it is the offset reset you already have.

It does not join. There is no `JoinGroup`, no heartbeat and no rebalance, because it is the only
member and always owns every band. `DescribeGroups` builds its answer from the saved rows.

Turning sync on for an existing topic starts it at the oldest surviving offset, so the table is as
complete as the log allows. Say how far back that reaches. On a seven day topic it is seven days,
and people assume it is everything.

Warning: `bottles` is not a cache. Once retention has passed, the records that built its older rows
are gone and a full rebuild is impossible. It is real data and your backups must cover it.

---

## Control tables

```sql
CREATE TABLE topic_config (
    schema_name        text     NOT NULL,
    topic              text     NOT NULL,
    band_count         smallint NOT NULL DEFAULT 4 CHECK (band_count BETWEEN 1 AND 1024),
    retention_interval interval NOT NULL,
    min_durability     text     NOT NULL DEFAULT 'durable'
        CHECK (min_durability IN ('relaxed','durable','replicated')),
    max_backlog_age    interval NOT NULL DEFAULT '60 seconds',
    offset_retention   interval,
    min_session_ms     integer  NOT NULL DEFAULT 6000,
    max_session_ms     integer  NOT NULL DEFAULT 1800000,
    sync_table         regclass,
    sync_key           text,
    sync_enabled       boolean  NOT NULL DEFAULT false,
    shape_version      integer  NOT NULL DEFAULT 0,
    backlog_age        interval NOT NULL DEFAULT '0',
    seq_ordered_to     bigint,
    PRIMARY KEY (schema_name, topic),
    CHECK ((sync_table IS NULL) = (sync_key IS NULL)),
    CHECK (NOT sync_enabled OR sync_table IS NOT NULL),
    CHECK (retention_interval > interval '0'),
    CHECK (max_backlog_age > interval '0'),
    CHECK (min_session_ms <= max_session_ms)
);

CREATE TABLE topic_band_position (
    schema_name    text     NOT NULL,
    topic          text     NOT NULL,
    band           smallint NOT NULL,
    next_offset    bigint   NOT NULL DEFAULT 0,
    oldest_offset  bigint   NOT NULL DEFAULT 0,
    stamped_by     text,
    PRIMARY KEY (schema_name, topic, band),
    FOREIGN KEY (schema_name, topic)
        REFERENCES topic_config (schema_name, topic) ON DELETE CASCADE
);

CREATE TABLE topic_groups (
    schema_name       text     NOT NULL,
    topic             text     NOT NULL,
    group_name        text     NOT NULL,
    owner_role        name     NOT NULL,
    generation_id     integer  NOT NULL DEFAULT 0,
    leader_member_id  text,
    protocol_type     text,
    protocol_name     text,
    state             text     NOT NULL DEFAULT 'Empty'
        CHECK (state IN ('Empty','PreparingRebalance','CompletingRebalance','Stable','Dead')),
    PRIMARY KEY (schema_name, topic, group_name),
    FOREIGN KEY (schema_name, topic)
        REFERENCES topic_config (schema_name, topic) ON DELETE CASCADE
);

CREATE TABLE topic_group_members (
    schema_name        text        NOT NULL,
    topic              text        NOT NULL,
    group_name         text        NOT NULL,
    member_id          text        NOT NULL,
    owner_role         name        NOT NULL,
    client_id          text,
    session_timeout_ms integer     NOT NULL,
    rebalance_ms       integer     NOT NULL,
    subscription       bytea,
    assignment         bytea,
    last_heartbeat_at  timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, topic, group_name, member_id),
    FOREIGN KEY (schema_name, topic, group_name)
        REFERENCES topic_groups (schema_name, topic, group_name) ON DELETE CASCADE
);

CREATE TABLE topic_offsets (
    schema_name       text     NOT NULL,
    topic             text     NOT NULL,
    group_name        text     NOT NULL,
    band              smallint NOT NULL,
    owner_role        name     NOT NULL,
    committed_offset  bigint   NOT NULL DEFAULT -1,
    generation_id     integer  NOT NULL DEFAULT 0,
    PRIMARY KEY (schema_name, topic, group_name, band),
    FOREIGN KEY (schema_name, topic, group_name)
        REFERENCES topic_groups (schema_name, topic, group_name) ON DELETE CASCADE,
    FOREIGN KEY (schema_name, topic, band)
        REFERENCES topic_band_position (schema_name, topic, band) ON DELETE CASCADE
);

CREATE TABLE topic_producers (
    schema_name    text        NOT NULL,
    topic          text        NOT NULL,
    producer_id    bigint      NOT NULL,
    producer_epoch smallint    NOT NULL,
    band           smallint    NOT NULL,
    slot           smallint    NOT NULL CHECK (slot BETWEEN 0 AND 4),
    first_sequence integer     NOT NULL,
    last_sequence  integer     NOT NULL,
    base_offset    bigint      NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, topic, producer_id, band, slot),
    FOREIGN KEY (schema_name, topic, band)
        REFERENCES topic_band_position (schema_name, topic, band) ON DELETE CASCADE
);

CREATE INDEX ON topic_group_members (last_heartbeat_at);
```

| Column | Meaning |
|---|---|
| `topic` | The queue table's name, `bottles_q`. |
| `sync_table` | The base table, held as `regclass` so a rename follows it. It resolves within one database, so a partial restore or a copy of `topic_config` into another database does not carry it. |
| `next_offset` | The ceiling for that band. The next offset the stamper will assign. |
| `oldest_offset` | The floor for that band. Raised by retention. |
| `backlog_age` | Age of the oldest unstamped row on the topic. Written each stamper cycle. |
| `seq_ordered_to` | Highest offset stamped in insert order rather than commit order. NULL in normal running. |
| `stamped_by` | The node that last stamped that band. A changed value means a split. |
| `offset_retention` | How long an empty group keeps its saved positions. NULL means forever, and that is the default. |
| `generation_id` | Kafka's generation. It increments on every rebalance. |
| `subscription`, `assignment` | Opaque bytes the client sends and receives. The broker does not parse them. |

All six key on the schema as well as the topic, because two schemas in one database can each hold a
topic called `bottles_q`. Every statement that touches them carries `schema_name`.

Kafka deletes a group's saved positions after it sits empty for seven days. People are caught out.
A group idle over a holiday comes back and reads everything again. Here the default is never. A row per band per group is tiny, and `DeleteGroups` is the deliberate way to remove one.

When `offset_retention` is set, the partition worker reaps on its existing timer. A group in `Empty`
state with no member row, untouched for longer than the interval, is deleted.

The same timer reaps `topic_producers` by `updated_at`, and that one is not optional. Every
`InitProducerId` mints a new producer id. A restarting job or a serverless client mints one on every
start and never clears its five rows. Watch the row count.

`min_session_ms` and `max_session_ms` bound what a client may ask for in `JoinGroup`. The defaults
match Kafka's.

The band columns carry a foreign key to `topic_band_position`, so no band outside the topic's
`band_count` can be written.

Dropping a `topic_config` row cascades to all of them. Drop the queue table in the same transaction.

Warning: every foreign key here says `ON DELETE CASCADE`, including the one from `topic_offsets` to
`topic_band_position`. Leave that one at the default and a topic that any consumer has ever
committed against cannot be dropped at all. The cascade reaches `topic_band_position` while a
`topic_offsets` row still points at it, and the whole delete rolls back on a foreign key violation.

Warning: a configuration change is an `UPDATE`. A delete and insert pair looks like an edit and
takes every consumer group's state with it.

---

## The Kafka protocol surface

`pg_topics` runs its own listener as a background worker, on its own port. It does not use
`pg_keyspace`'s Redis listener, because `pg_topics` must not depend on `pg_keyspace`.

### Connection and identity

The listener is one background worker running a non-blocking event loop, and it runs no SQL on that
loop. For each authenticated client it opens one Postgres backend over the loopback socket, as the
client's own role. Every request runs as the caller, so `Produce` calls the same publish function a
SQL user calls. Row level security and grants apply, and no client facing path bypasses a policy.
The stamper and the sync worker do, by design, and Permissions says how that is contained.

That backend costs a slot in `max_connections`, not `max_worker_processes`. Bound it with
`pg_topics.max_clients` and set `max_connections` above it. This is a load bearing number.

Warning: `max_clients` counts authenticated clients, and a client costs the listener memory before
it authenticates. A caller that opens a socket, sends `ApiVersions` and then stops holds event loop
state and no backend, so it is invisible to that limit. Cap connections waiting to authenticate
separately, and drop one that has not finished within a few seconds. There is one listener per
database, so exhausting it stops produce and consume for every topic in that database.

A blocking `Fetch` waits in the event loop on a latch, not inside a query. The client's backend sits
idle for that wait and issues one read when data arrives or the deadline passes. A long poll costs a
connection and not a busy backend, and never queues behind another client.

Auth is SASL against Postgres's own roles, so there is no second credential store and no user
provisioning.

| Mechanism | When |
|---|---|
| `PLAIN` over TLS | The default, and the one with no privilege cost. |
| `SCRAM-SHA-256` | Optional, and it costs a cluster wide credential grant. |

`PLAIN` hands the listener the role name and the password, so it opens the backend with those
credentials directly. Postgres does the authentication, exactly as it would for `psql`. The listener
needs no privilege, no `SET ROLE` and no membership in anybody's role.

TLS is not optional with it, and that is a check rather than advice. The listener does not advertise
`PLAIN` in `SaslHandshake` on a connection that has not upgraded to TLS, and refuses a `PLAIN`
`SaslAuthenticate` on one. Otherwise a misconfigured client sends a Postgres password in clear.

Warning: `SCRAM-SHA-256` is nicer on the wire and much worse off it. Verifying a SCRAM proof means
reading `rolpassword` from `pg_authid`, and that grant is table wide. Whatever role holds it can
read every password verifier in the cluster, including superuser and replication roles. That is why
`PLAIN` over TLS is the default rather than the fallback.

Warning: with `SCRAM-SHA-256` the listener needs a base role with membership in every enrolled role,
and `SET ROLE` per request. That role never takes a string built from client input. The SASL
exchange resolves the asserted name to an `oid` in `pg_authid` first, and the role is set from that
oid. Restrict it in `pg_hba.conf` to local connections.

TLS follows `pg_keyspace`'s pattern, including the option to borrow the cluster's own certificate.

A Kafka topic name maps to `schema.topic`, which is legal in Kafka's topic naming rules. The first
dot splits the two. Topic creation refuses a schema or table name containing a dot, so the split is
never ambiguous. Postgres allows a dot inside a quoted identifier. Without that rule a schema called
`a.b` and a schema called `a` fight over the same wire name.

`Metadata` lists the topics the connected role can see in the connected database, and reports
`pg_topics.advertised_host` as the broker's address.

### APIs

| API | Purpose |
|---|---|
| `ApiVersions` | Advertises a narrow recent version range per API. Clients negotiate down. |
| `SaslHandshake`, `SaslAuthenticate` | The two APIs a client uses to authenticate. Without them no stock client can connect. |
| `Metadata` | Topics, bands as partitions, and one broker. |
| `Produce` | Writes rows. The client chose the band. |
| `Fetch` | Reads rows and builds a record batch. |
| `ListOffsets` | Earliest and latest per band, from `oldest_offset` and `next_offset`. By timestamp, from `published_at`. |
| `FindCoordinator` | Returns this broker. |
| `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup` | Group membership. |
| `OffsetCommit`, `OffsetFetch` | Positions, in `topic_offsets`. |
| `InitProducerId` | Idempotent producers. |
| `CreateTopics`, `DeleteTopics`, `DescribeConfigs`, `AlterConfigs` | Admin, routed into the control plane functions. |
| `DeleteGroups` | Removes one group without touching the topic. |
| `DescribeCluster`, `ListGroups`, `DescribeGroups` | What Kafka tools read to show you a cluster. |

Record batch v2 only, which is everything since Kafka 0.11.

Two wire framings exist, and the codec implements both. The older APIs use the classic layout. The
newer versions of `Metadata`, `Produce`, `Fetch`, `JoinGroup` and `OffsetCommit` use the flexible
layout, with compact strings, compact arrays and a tagged field section. Some APIs, including
`DescribeCluster`, have no classic form at all.

The framing is not a separate choice. The version fixes it. So `ApiVersions` advertises only
versions the codec truly encodes and decodes. Write the list of those versions per API down before
the codec is built.

Advertise a version whose framing is not implemented and a stock Java client negotiates up to it. It
then sends bytes the parser cannot read. The connection dies on the first real call, not at
`ApiVersions`.

`Produce` accepts batches compressed with none, gzip, snappy, lz4 or zstd, which are Kafka's five
codecs, so no producer configuration is refused. `Fetch` emits uncompressed to begin with, because
every client accepts it. Add `Fetch` compression when a soak test says the bandwidth matters.

`CreateTopics` takes the partition count as `band_count`. It carries a replication factor too, and
anything but 1 is refused with `INVALID_REPLICATION_FACTOR`. Accepting a 3 would tell an operator
they have three copies of every partition when they have none.

`DescribeConfigs` returns only the settings actually honoured. `AlterConfigs` refuses any other key
with `INVALID_CONFIG`, naming it. Quietly accepting `cleanup.policy=compact` would leave an operator
believing compaction is on.

### Idempotent producers

The Java client has had `enable.idempotence` on by default since 3.0, so a modern producer asks for
it without being told to. `InitProducerId` hands out a producer id and epoch. A retried `Produce` is
recognised in `topic_producers` and answered with the original offset, instead of writing a second
row.

Keep five, not one. `max.in.flight.requests.per.connection` defaults to 5, so a producer can have
five batches outstanding. A single last sequence cannot tell a legitimate retry of an earlier
in-flight batch from a real gap.

`slot` is a ring index, not a hash of the sequence. The five rows are the five most recent batches,
oldest evicted first. Indexing by `sequence % 5` would let a later batch overwrite the row a still
outstanding retry needs.

Each row holds both ends of its batch, `first_sequence` and `last_sequence`, and the `base_offset`
the client was told. A retry matches when its base sequence equals a row's `first_sequence`, and the
answer is that row's `base_offset`. One end is not enough. A client retrying a 200 record batch
sends the first sequence, not the last. A row holding only `last_sequence` never matches, and the
whole batch is written twice.

Warning: verify against a real Java 3.x client early. If a client hard fails rather than warning
when the broker refuses `InitProducerId`, this moves from optional to required.

Kafka transactions are not supported. `AddPartitionsToTxn`, `EndTxn`, transaction markers and
`read_committed` isolation are a separate subsystem, and they are how Kafka delivers exactly once.

### Errors

Kafka has a fixed table of numbered error codes and clients branch on them. Map a small set
precisely and return `UNKNOWN_SERVER_ERROR` for everything else, with the Postgres error in the log.

| Kafka error | Raised when |
|---|---|
| `UNKNOWN_TOPIC_OR_PARTITION` | No such topic, or band outside `band_count`. |
| `OFFSET_OUT_OF_RANGE` | The requested offset is below `oldest_offset`. |
| `TOPIC_AUTHORIZATION_FAILED` | The role lacks `EXECUTE` on the publish function, or `SELECT` on the table. |
| `GROUP_AUTHORIZATION_FAILED` | The role is not a member of `owner_role`. |
| `REBALANCE_IN_PROGRESS` | The group state is `PreparingRebalance`. |
| `ILLEGAL_GENERATION` | The request carries a stale `generation_id`. |
| `UNKNOWN_MEMBER_ID` | The member was expired by a missed heartbeat. |
| `POLICY_VIOLATION` | A control plane function refused the request. |
| `OUT_OF_ORDER_SEQUENCE_NUMBER` | An idempotent producer's sequence left a gap. |
| `INVALID_RECORD` | The value is not JSON, or the key is not UTF-8 or is over 40 characters. |
| `MESSAGE_TOO_LARGE` | The batch is above `pg_topics.max_message_bytes`. |
| `INVALID_REPLICATION_FACTOR` | `CreateTopics` asked for anything but 1. |
| `INVALID_CONFIG` | A topic setting that is not implemented. |

`INVALID_PRODUCER_EPOCH` is not in that list. A producer epoch only bumps for a transactional
producer, so nothing here can raise it.

`OFFSET_OUT_OF_RANGE` is how a consumer that fell behind retention finds out, and its own
`auto.offset.reset` handles it. No bespoke reset call is needed.

### Blocking Fetch

Kafka's `Fetch` waits up to `max.wait.ms` for `min.bytes` of data. The stamper and the listener are
both `pg_topics` background workers in the same instance, so the stamper sets the listener's latch
directly when it stamps. A SQL consumer gets a `NOTIFY` instead.

One latch per database means a stamp on any topic wakes every blocked `Fetch` on that database, and
each re-runs its read to find nothing. That cost grows with topic count times blocked consumer
count, and it is a reason to split a busy multi tenant database's topics across databases.

A safety poll every 5 seconds covers a missed wake on both paths. It has to stay well under a
client's `request.timeout.ms`, which defaults to 30 seconds. A missed wake still costs a low latency
consumer far more than its own `max.wait.ms`, which is often 500ms. Treat one as a bug to fix.

### Client compatibility

| Client | Status |
|---|---|
| Java | Must pass. The reference implementation and the strictest. |
| `librdkafka` | Must pass. Backs the Python, Go, Node, Ruby and .NET clients. |
| Confluent's JavaScript and TypeScript SDK | Must pass. It wraps `librdkafka`, so types and API shape are the risk, not the protocol. |
| `kafkajs` | Third. Pure JavaScript and picky about version negotiation. |
| `franz-go` | Fourth. Pure Go, no C dependency. |

The first three gate a release. All five run in the Docker harness.

---

## Publishing

Over the wire, a Kafka producer sends `Produce`. The client picked the band. The listener runs the
publish function as the caller's role.

In SQL, `SELECT topic.publish('public.bottles_q', ...)` commits with your business write or not at
all. The function picks the band with Kafka's partitioner.

Without an idempotent producer, a publish call that fails after its transaction committed writes a
second row on retry. Give events a business key and make consumers idempotent on it.

---

## Consumer groups

Group coordination is Kafka's, and it is client driven. Consumers send `JoinGroup`. The coordinator
picks one member as leader and returns the member list to it. That leader runs its own assignor,
Range, RoundRobin or CooperativeSticky, and returns the assignment through `SyncGroup`. The
coordinator hands each member its share. `Heartbeat` keeps membership.

The coordinator lives in the listener worker. There is no rebalancer background worker, and the
client's own sticky assignor does the minimal movement a bespoke one would have done.

The broker does not parse `subscription` or `assignment`. They are opaque bytes it stores and
forwards, exactly as Kafka does.

A SQL consumer calls functions that perform the same join, sync, heartbeat and commit steps, so one
coordination model governs both paths.

The checkpoint belongs to the band, not the worker. `topic_offsets` is keyed by group and band, as
Kafka's `__consumer_offsets` is keyed by partition.

### The state machine

| From | To | On |
|---|---|---|
| `Empty` | `PreparingRebalance` | The first `JoinGroup`. |
| `Stable` | `PreparingRebalance` | A member joins, leaves, or is expired by a missed heartbeat. |
| `PreparingRebalance` | `CompletingRebalance` | The join window closes. Every member gets its `JoinGroup` response, the leader also gets the member list, and `generation_id` increments. |
| `CompletingRebalance` | `Stable` | The leader's `SyncGroup` arrives. Every member gets its share of the assignment. |

`Dead` is a group whose last member left and whose rows are waiting to be cleaned up.

`JoinGroup` responses are held, not answered as they arrive. The window closes when every member of
the previous generation has rejoined, or when the largest `rebalance_ms` any member asked for has
elapsed. Answering the first `JoinGroup` at once would give the leader a one member assignment, and
three consumers starting together would rebalance three times before settling.

A member is expired when `now() - last_heartbeat_at` exceeds its `session_timeout_ms`. That compares
against a value on the row, not a constant, so the index on `last_heartbeat_at` cannot answer it
alone. The coordinator scans rows older than `now() - max_session_ms`, which the index does bound,
and re-checks each one against its own timeout. Expiring one member moves the group to
`PreparingRebalance`. Every other member's outstanding `Heartbeat` is answered with
`REBALANCE_IN_PROGRESS` at once, rather than left to time out.

### Offset commits are fenced

`OffsetCommit` carries the member's `generation_id`. A stale generation is rejected with
`ILLEGAL_GENERATION`, which stops a paused worker writing over the position of the member that took
its band.

The SQL below writes parameters as `:name` for reading. Postgres uses `$1` and `$2`.

```sql
INSERT INTO topic_offsets (schema_name, topic, group_name, band,
                           owner_role, committed_offset, generation_id)
SELECT :s, :t, :g, :b, g.owner_role, :n, :expected
  FROM topic_groups g
 WHERE g.schema_name = :s AND g.topic = :t AND g.group_name = :g
   AND g.generation_id = :expected
ON CONFLICT (schema_name, topic, group_name, band) DO UPDATE
   SET committed_offset = EXCLUDED.committed_offset,
       generation_id    = EXCLUDED.generation_id
 WHERE topic_offsets.generation_id <= EXCLUDED.generation_id;
```

It has to be an upsert. A group's rows do not exist until its first commit, and a bare `UPDATE`
would affect zero rows every time.

The `SELECT ... FROM topic_groups` fences the insert branch. A `WHERE` on `DO UPDATE` gates only the
update, so a plain `VALUES` insert would write whatever generation the caller claimed. A member
holding a stale generation could plant its number on a band's first commit and fence out the member
that actually won the band.

The handler caps `:n` at that band's `next_offset`, so a client cannot commit past what the server
can show was there.

A commit in the current generation can move the offset back, as in Kafka. A consumer that seeks back
and commits must resume from the lower offset after a crash. The cost is that a late retry of an
older commit can move the offset back too. That gives duplicate delivery, never a skipped record.

Zero rows affected means a stale generation. The handler answers `ILLEGAL_GENERATION`, and the
client rejoins.

A missing group or topic raises a foreign key violation rather than affecting zero rows, and the
handler maps that to `UNKNOWN_TOPIC_OR_PARTITION`.

Each band commits on its own statement, so losing one band part way through does not block the
others.

---

## What a consumer can do

Everything a Kafka consumer can do, plus what a table gives you.

- Join a group, or assign bands itself with no group at all.
- Start from earliest, latest, a given offset, or a timestamp. The timestamp case prunes partitions
  instead of scanning.
- Block in `Fetch` up to `max.wait.ms`.
- Commit offsets on its own schedule, or let `enable.auto.commit` do it.
- Get the batch again after a crash, because delivery is at least once.
- Rely on order per band, which is order per key when the producer keys by the thing it is
  describing. One case breaks it: a stamper down long enough to lose commit timestamps falls back to
  insert order.
- Get `OFFSET_OUT_OF_RANGE` and reset itself when it falls behind retention.
- Replay, by committing an earlier offset or starting a new group.
- Read its own lag, either through `DescribeGroups` or as a SQL query.
- Query the history directly in SQL, with no consumer process at all.
- Read the current state straight out of the base table, with no consumer at all.

### Server side filtering

A Kafka consumer reads the whole partition and discards what it does not want. A SQL consumer can
ask for `value->>'status' = 'confirmed'` and the database does the work. This is a SQL path feature,
because the protocol cannot express it.

The predicate is bound as typed parameters against named fields, never taken as a SQL fragment from
the client.

Two limits. It saves bandwidth, not disk reads, unless there is a GIN index on `value` or an
expression index on the field. And a filtered consumer commits the highest offset it read, not the
highest that matched. Commit the highest match instead and the unmatched rows after it are read
again on every batch.

---

## Where topics live

- A topic belongs to one database, because it is a table. `CREATE EXTENSION pg_topics` installs into
  one database.
- The listener is per database and each one binds its own port. Only one process can bind a port,
  and a Postgres backend cannot change database mid session. Set the port with
  `ALTER DATABASE app SET pg_topics.port = 9092`, or to 0 for no listener at all.
- Two databases left on the same port do not both work. The second listener fails to bind, logs the
  collision, and does not start. That database then has topics nothing serves, so the bind failure
  is a monitored signal rather than a log line.
- Roles are cluster wide. Grants are per database.
- A client connection is pinned to one database when it authenticates, so a Kafka topic name is
  `schema.topic` and never carries a database.
- There is no cross database consumption.

Warning: multi tenancy is by schema, one schema per tenant, and not by row. A row restricting policy
on a topic breaks the Kafka path. Bands are shared, so `ListOffsets` reads a band's true ceiling
from `topic_band_position`, which no policy filters, while a tenant's `Fetch` returns only its own
rows. Its lag never reaches zero and no error explains why. Keep row level security as a second line
of defence on the SQL path, not as the thing that separates tenants.

### Worker slots

A background worker connects to exactly one database. Four need a connection: the stamper, the
partition worker, the sync worker, and the listener. So every database hosting topics costs four
worker slots out of `max_worker_processes`, and one TCP port.

Partition creation and retention are one worker, because both issue DDL against the same parent
table and would otherwise contend for the same lock. Creation runs first on every tick and never
waits behind retention. Each topic's step runs inside its own savepoint, so one topic's failure does
not stop the tick for the rest. The detach step carries a short `lock_timeout`, and a blocked detach
is skipped and retried next tick.

One sync worker serves every synced topic in the database, in turn, the same way the stamper serves
every topic.

On a shared instance with hundreds of databases this is a hard ceiling. Two ways out: nominate which
databases may host topics, or run one launcher servicing many databases in rotation. The second is
the only option for dense multi tenancy and trades worker slots for stamp latency.

A new release migrates the six control tables under the same expand and contract discipline asked of
a customer's base table. Add a column nullable, ship workers that read both the old and the new
shape, backfill, then tighten in the release after. The point of no return is the tightening, so it
never lands in the same release as the column.

---

## Retention

Partitions are by time. Dropping one returns the disk at once, where a `DELETE` would leave dead
tuples, autovacuum work and index bloat.

Warning: step 2 must come after step 1. A check taken while the partition is still attached cannot
see an open transaction's uncommitted row. That transaction can commit in the gap, and the drop then
destroys a row acknowledged as durable that no consumer ever saw. The detach closes the gap because
`DETACH PARTITION ... CONCURRENTLY` waits for every transaction that could still write the
partition. Confirm that on a live PostgreSQL 17 instance before building on it.

Warning: step 3 must read the surviving partitions. A transaction that inserts early and commits
late leaves a high offset in an old partition. That partition's own maximum would push the floor
past offsets still present and unread.

Retention runs four steps:

1. `ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY`. This avoids the long ACCESS EXCLUSIVE lock
   on the parent, so publishes to other partitions keep running. It cannot run inside a transaction
   block.
2. Check the detached table for any row with `log_offset` NULL. If it finds one, reattach and alert.
3. Read the lowest surviving `log_offset` per band.
4. In one transaction, `DROP TABLE` the detached partition and set each band's `oldest_offset` to
   the value from step 3.

An interrupted detach leaves the partition pending, and
`ALTER TABLE ... DETACH PARTITION ... FINALIZE` clears it. A crash between step 1 and step 4 leaves
a fully detached table that nothing owns and nothing drops. The worker scans for detached tables
matching the topic's partition naming on every restart, and resumes at step 2 for each.

A consumer below `oldest_offset` gets `OFFSET_OUT_OF_RANGE` on its next `Fetch`, and its own
`auto.offset.reset` decides what happens next.

Warning: something must create partitions ahead of time, and Postgres will not. An insert with no
matching range raises an error. If the newest partition's range ends before the current time, every
publish on every topic in that database fails at once.

The partition worker runs on a timer, so it cannot cover the moment a topic is created. The create
function makes the current partition and the next one itself, in the same transaction as the
`CREATE TABLE` and the `topic_config` row.

Retention refuses to drop unstamped rows, so a stuck stamper grows the unstamped index and blocks
disk reclaim at the same time. That is what the publish backlog limit is for.

Retention applies to the queue table only. The base table and the error table keep every row until
you remove it.

---

## Durability

| Tier | `synchronous_commit` | An acknowledged publish means |
|---|---|---|
| Relaxed | `off` | In the write ahead log buffer. Survives a clean restart. A small window of loss on a crash. |
| Durable | `on` | Flushed to disk. Survives `kill -9`. |
| Replicated | `remote_apply` | As durable, and a standby has applied it. |

The tier is per topic, in `topic_config.min_durability`, and the default is `durable`.

There is no tier meaning lost on any restart. No `synchronous_commit` value does that, and
delivering one would need an unlogged table, which cannot also serve the other tiers from one
partitioned table.

| `acks` | Tier it asks for |
|---|---|
| `0` | Relaxed. The producer does not wait at all. |
| `1` | Relaxed. Kafka's own `acks=1` means the leader wrote it, not that it flushed. |
| `all` | Durable, or replicated when `synchronous_standby_names` is set. |

A publish runs at whichever is stronger, the topic's floor or what `acks` asked for. So a topic at
`relaxed` still gives an `acks=all` producer a durable write. Kafka forces `acks=all` whenever
idempotence is on, and idempotence is on by default in the Java client since 3.0. A stock modern
producer therefore always gets durability, with no silent downgrade and nothing to refuse.

### The replicated tier

Two things go wrong, and they go wrong in opposite directions.

With `synchronous_standby_names` empty, `remote_apply` does nothing. The commit behaves as `on` and
returns with no error, so the tier is a label. The function that sets `min_durability` to
`replicated` therefore refuses unless that setting is not empty.

With it set to a standby that is not connected, the commit does not degrade. It blocks, with no
timeout, until that standby comes back. Every publisher to that topic then sits in `COMMIT`, each
holding a backend. Enough of them exhaust `pg_topics.max_clients`. The listener can then open no
backend for any topic in the database.

Warning: cancelling that wait is not an escape. Postgres has already committed locally, so a
cancelled backend reports failure for a record that is present and will survive. There is no safe
timeout to add.

Two real mitigations. Use a quorum in `synchronous_standby_names`, such as `ANY 1 (a, b)`, so one
standby restarting does not block anything. And alert on backends whose wait event is `SyncRep`,
which is the only early warning there is.

Leave the default at `durable` until the quorum form and the `SyncRep` alert have both been run in
anger. `replicated` is the one tier whose failure reaches past its own topic.

### The invariant

A publish acknowledged at `durable` or `replicated` is never lost. For those two tiers, a publish
that cannot be queued on the wire path holds the acknowledgement rather than returning it. It fails
after a bounded time. That bound covers the queue, not the commit. A `replicated` commit
waiting on a missing standby is the case above, and it has no bound.

The invariant assumes one primary. A failover that does not fence the old primary breaks it, and no
tier survives that.

---

## Permissions

Topics use the database's permission system instead of a parallel one. Every path runs as a Postgres
role, including the Kafka one.

Five paths must agree: SQL, the Kafka listener, the stamper, the sync worker, and the control plane.

### The queue table

Nobody gets plain `INSERT`. A publisher gets `EXECUTE` on a `SECURITY DEFINER` publish function and
nothing else, and that function rejects a caller supplied `log_offset`. A role with raw `INSERT`
could otherwise write a `log_offset` and put back the concurrency bug the stamper removes.

Row level security applies to every read of the table, through SQL or through `Fetch`, because both
run as the caller.

The publish function ends with `SET LOCAL synchronous_commit` at the topic's `min_durability`.

Warning: that raises the floor, it does not close the hole. Postgres reads `synchronous_commit` at
commit time, not when the function runs, so a client that sets it again before the commit still
wins. Say that to callers rather than imply a guarantee.

### The base table

It is the user's, under their own grants. `pg_topics` never hands out rights on it. The sync worker
writes to it and nothing else does on `pg_topics`'s behalf.

### The control tables

`topic_groups`, `topic_group_members` and `topic_offsets` each carry `owner_role` and each get a row
policy on it, testing membership with `pg_has_role(current_user, owner_role, 'member')` rather than
equality. A team owning a group through a shared role would fail an equality test. The column is
repeated on all three on purpose. A policy joining back to `topic_groups` would pay a subquery per
row and recurse into that table's own policy.

A row policy says which rows a role may touch, not what values it may write. So all six control
tables revoke `INSERT`, `UPDATE` and `DELETE` from every role except the extension's own functions.
Without that, a role that owns a group can set `committed_offset` by hand and skip everything
between.

`topic_config`, `topic_band_position` and `topic_producers` carry no `owner_role`, because one row
serves every publisher into that topic. Protect them with a plain grant and expose what a tenant
needs through functions.

A topic's owner is the owner of its queue table, `pg_class.relowner`, set to whoever called the
create function. Every topic level control plane operation checks it with
`pg_has_role(current_user, relowner, 'member')`.

### The control plane

Seven operations write `topic_config` and issue DDL:

- Create a topic.
- Drop a topic.
- Attach a base table.
- Change its retention.
- Change its sync settings.
- Change its durability floor.
- Turn sync on or off.

Dropping a group is an eighth, and it writes `topic_groups`. Fence all eight on the role that owns
the topic or the group.

`CreateTopics`, `DeleteTopics`, `AlterConfigs` and `DeleteGroups` arriving over the wire call those
same functions as the authenticated role, so the fence applies with no extra work.

Drop is the one to fence hardest. It cascades through the foreign keys and removes every consumer
group's saved position. Its `DROP TABLE` needs an ACCESS EXCLUSIVE lock, so give it a short
`lock_timeout` and retry rather than queue behind a long publisher.

Warning: those functions build DDL from a caller supplied schema and topic name, both plain `text`.
Quote every identifier with `format('%I')`. String concatenation is a DDL injection hole that runs
as the extension's own owner.

Warning: every `SECURITY DEFINER` function pins its own search path with
`SET search_path = pg_catalog, pg_temp`. Without it, a role that can create objects in an earlier
schema plants one with the name the function calls. That object then runs with the function owner's
rights.

### The privileged roles

There are two, and they are the most powerful things in the system.

The stamper writes `log_offset` on every tenant's rows, so it holds `BYPASSRLS`. It does not own the
tables. A table has one owner, and that owner is the tenant, which is what every control plane fence
tests against.

The sync worker writes into every tenant's base table.

Warning: `BYPASSRLS` reaches every table with a row policy in the database, including tables
`pg_topics` never created. That is the real blast radius of either role.

Warning: give each its own role, and give both `NOLOGIN`. No user facing function may reach either
and no `SECURITY DEFINER` function may run as either. Lint function ownership in CI.

---

## Monitoring

One view per concern, plus `pg_topics.health()` returning a row per topic. Anything that scrapes
Postgres then works, and you ship no exporter.

| Signal | Why it matters |
|---|---|
| Age of the oldest row with no offset | A stuck stamper never restarts, so a restart alert stays silent. |
| Age of the oldest open transaction | It holds back the vacuum horizon for everybody. |
| A partition detach that has been waiting | The fix is to end the blocking transaction. |
| Dead tuples on the partition being written | The stamper leaves one per event. |
| Time the newest partition still covers | Alert below a day. |
| Headroom in `max_worker_processes` | All four workers can be starved at once. |
| A listener that failed to bind its port | That database has topics nothing serves. |
| Members expired by missed heartbeat | A rising count means consumers pause longer than their session timeout. |
| Rows in each `_qe` table, and the rate they arrive | Nothing stops and nothing expires, so a schema mistake fills the table at the topic's full rate. |
| Rows in `topic_producers` | A short lived producer identity mints five rows and never returns. |
| Backends whose wait event is `SyncRep` | A `replicated` topic blocked on a missing standby. |
| Sync lag, per topic | How stale the base table is. Read it through `DescribeGroups` like any other group. |
| A duplicate `log_offset`, or a changed `stamped_by` | Split brain. The most severe signal, and the only one with no automatic repair. |

Consumer lag is a Kafka concept and Kafka tools read it through `DescribeGroups`. In SQL it is each
band's `next_offset` minus its `committed_offset`.

Each signal needs a named owner and a runbook. One has a trap: fixing `max_worker_processes` needs a
restart, so the alert fires during an incident and the remedy is an outage.

---

## How it scales

### Consumers scale out

Add workers, the client assignor redistributes bands, throughput goes up. Bands are disjoint, so two
consumers never read the same rows and never contend on the same locks. They still share the
primary's buffer pool, disks and write ahead log.

The band count is the parallel ceiling for a group. Default 4, maximum 1024.

One limit is shared with Kafka. A key with few distinct values puts most traffic on a few bands, so
the members owning them do most of the work. Order per key and even spread are in tension.

### History reads scale out

The topic is an ordinary table under ordinary replication, so a SQL consumer reading the past can
use a replica.

One rule makes that safe. A consumer commits only offsets it actually read and processed from that
replica. It must never take a ceiling from the primary and commit it because the replica returned no
rows. Use `pg_last_wal_replay_lsn()` to show how far behind the replica is.

The Kafka listener serves the primary only. Kafka's own follower fetching is not implemented.

### Publish throughput does not scale out

One primary means one WAL. That is the wall, and your database already has it.

No `pg_topics` code exists, so there are no measurements. The numbers below are `pg_keyspace`'s
cache benchmarks, `bench/run_scaleout.sh` and `bench/run_persist_scaleout.sh`. Treat them as a rough
ceiling on the machinery, not as evidence about this design. A cache write is one insert. A stamped
event is an insert, a second heap tuple, two index changes and a dead tuple.

| Write path | 1 worker | 4 workers | Scaling |
|---|---:|---:|---|
| Shared memory only, no persistence | 568k/s | 2.46M/s | roughly linear |
| Ring drained into a Postgres table, distinct rows | ~107k/s | ~145k/s | 1.35x for 4x the workers |

In the same benchmarks a client waiting for durable acknowledgements gets 23,327 a second on one
connection and 49,989 across eight. A different run, six connections with the persist window widened
to 50ms, reaches about 64,000.

Whether a few thousand events a second leaves headroom is not known. Measure the real publish path
before anybody sizes a deployment.

Writes split across machines is the only thing that makes publish throughput horizontal, and it is
not in v1.

---

## More than one machine

Copies for failover and read load. That is a streaming replica. The topics are intact after a
failover because they are ordinary tables under ordinary replication. This is what most people
asking for more than one broker want.

Writes split across machines. Each machine owns a slice of the bands, which is what Kafka brokers
do. It costs routing across machines, rebalance and the split brain questions. It also costs the
central claim: the topic becomes several tables on several machines, and reading the history as one
table then needs a distributed query layer.

`Metadata` already carries a broker list and a leader per partition, so the protocol has room for it
without a client change.

---

## Packaging

`pg_topics` is its own extension and does not depend on `pg_keyspace`. It works on stock Postgres.
It has its own listener, port, auth and wake path, and shares nothing with `pg_keyspace` but a
repository. `pg_keyspace` stays what it is, a Redis compatible cache.

---

## pg_topics and Kafka compared

| | pg_topics | Kafka |
|---|---|---|
| Things to run | Your existing Postgres | Broker cluster, KRaft or ZooKeeper, connectors |
| Client compatibility | Kafka clients, unchanged | Kafka clients |
| Throughput | Not yet measured | Millions per second |
| Ordering | Per partition, which the producer keys | Per partition |
| Partitions | Chosen at creation, default 4 | Chosen at creation |
| Consumer groups | Kafka's protocol | Kafka's protocol |
| Record value | JSON only | Any bytes |
| History queryable | Yes, it is a table | No, needs a pipeline elsewhere |
| Current state as a table | Yes, kept up to date for you | No, needs ksqlDB or a connector |
| Publish in your app's transaction | Yes, on the SQL path | No, hence the outbox pattern |
| Permissions | Postgres roles and row level security | Separate ACL system |
| Backups | Whatever you already do | Separate concern |
| Exactly once | No | Yes, with transactions |
| Ecosystem and tooling | Kafka's, as far as the protocol reaches | Huge and mature |
| Genuine massive scale | No | Yes |

---

## Order of work

1. Prove the durability invariant under injected failure, so a publish acknowledged as durable is
   never lost without an error. Include an unfenced failover, because split brain is the one case
   the design cannot prevent alone. Confirm the ordering claim under load, because a transaction
   takes its commit timestamp before it becomes visible. Prove
   `DETACH PARTITION ... CONCURRENTLY` in the same pass. Hold an open transaction that has inserted
   into a partition, detach it concurrently from a second session, and confirm the detach waits.
   Retention's whole safety argument rests on that.
2. Measure what the publish path gives a publisher on the target hardware. If it falls short,
   everything after this is the wrong build.
3. The work queue. It shares the queue table shape, the partitioning and the retention order, and it
   is SQL functions only with no wire protocol. It proves the table and the partition worker under
   real load before the offset machinery lands on top. Its own API is specified in a separate
   document.
4. Append only storage and the stamper, with its advisory lock, its crash behaviour, the publish
   backlog limit, and a measurement of what one stamper sustains.
5. Time partitioning: creating partitions ahead of time first, then retention with its detach, check
   and drop order.
6. The sync worker: attach, the shape cache and its event triggers, the upsert, `event_at` ordering,
   the error table and `retry_errors`.
7. The Kafka listener, in two halves. First `ApiVersions`, `Metadata`, `Produce`, `Fetch` and
   `ListOffsets`, so a producer and a plain consumer work. Then `FindCoordinator`, `JoinGroup`,
   `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit` and `OffsetFetch`.
8. `InitProducerId` and idempotent producers.
9. Admin APIs, routed into the control plane functions.
10. Permissions across every path.
11. Failover and replica reads, with the clamp to what the read returned.

Keep consumer groups off for tenants that do not trust each other until item 10 lands. Item 7 builds
the mechanics and item 10 builds the fences.

Everything is tested in a Docker harness running the real image, with spike, soak and load runs. The
durability proof kills Postgres at scripted points, restarts, and diffs what survived against what
was acknowledged.

The declaration syntax and the typed client are not on that list. Shape the v1 SQL functions so they
can be generated from, because that costs nothing today and a rewrite later.

A queue and a log are different primitives. A queue needs to say "deliver this in five minutes", for
a retry backoff or a scheduled send. A log is ordered strictly by arrival and cannot express that.

---

## Later, with Supatype

Supatype's idea is that you describe what you want in TypeScript and the system builds it. You would
declare a topic like anything else: a name, a table shape, a band count, retention, a sync key, and
who may publish and subscribe. You push and get the base table, the queue table and the error table.
You also get the permissions and row level security, the retention schedule, and a typed producer
and consumer in your client.

Kafka, Kinesis and the Postgres queue extensions can all move bytes. None takes one declaration and
gives you the log, the table it keeps current, the permissions and the typed client together.

---

## Open questions

- At what rate does one stamper stop keeping up? The named upgrade path is one stamper per band,
  with a counter per band, which it already has.
- How many topics can one stamper worker service before consumers notice? Stamp delay rises with
  topic count, because one worker takes each topic's lock in turn. The same applies to the sync
  worker.
- A shared instance with hundreds of databases still needs an answer. One worker set per database
  costs four slots each. Dense multi tenancy would need one launcher across many databases.
- Does a modern Java producer hard fail or warn when `InitProducerId` is refused? It decides whether
  idempotent producer support is optional or required.
- Which Kafka protocol versions to advertise per API. A narrow recent range is the plan, and the
  exact floor comes out of the client test matrix.
- Is failover enough, or must more than one machine mean writes split across machines?
