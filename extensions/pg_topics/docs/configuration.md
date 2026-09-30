# Configure pg_topics

This guide is for the person who runs the database. It lists every setting and says when each one takes effect. It also covers topics, durability, permissions, the table sync, consumer groups, backups and failover. [Install pg_topics](install.md) comes first.

## Where settings live

There are three levels:

| Level | Where you set it | Examples |
|---|---|---|
| Server | `postgresql.conf` or `ALTER SYSTEM` | `pg_topics.databases`, `pg_topics.max_clients` |
| Database | `ALTER DATABASE ... SET` | `pg_topics.port` |
| Topic | `topic.*` functions, or a Kafka admin tool | retention, durability, backlog limit |

## Server settings

Only a superuser can change these settings.

| Setting | Default | Takes effect | Meaning |
|---|---|---|---|
| `pg_topics.databases` | empty | Restart | A comma list of the databases that get the 5 workers. |
| `pg_topics.failover_is_fenced` | `off` | Reload | Your promise that the old primary is stopped before a standby takes over. pg_topics does not check it. A topic cannot use the `durable` or `replicated` tier while this is `off`. |
| `pg_topics.port` | `9092` | Listener restart | The TCP port of the Kafka listener. `0` means no listener. Set it per database with `ALTER DATABASE`. |
| `pg_topics.advertised_host` | `localhost` | Listener restart | The host name that `Metadata` gives to Kafka clients. Clients reconnect to this name. |
| `pg_topics.max_clients` | `100` | Listener restart | The most Kafka clients that one listener accepts at once. Allowed range: 1 to 100000. |
| `pg_topics.max_message_bytes` | `1048576` | Listener restart | The largest record batch that `Produce` accepts, in bytes. Allowed range: 1024 to 1073741824. |
| `pg_topics.tls_cert_file` | empty | Listener restart | The PEM certificate of the listener. A relative path starts at the data directory. |
| `pg_topics.tls_key_file` | empty | Listener restart | The PEM private key of the listener. |
| `pg_topics.tls_use_postgres_cert` | `off` | Listener restart | Use `ssl_cert_file` and `ssl_key_file` instead. Works only when the two `tls_*_file` settings are empty and `ssl = on`. |
| `pg_topics.group_min_session_ms` | `6000` | Reload | The shortest session timeout that a consumer group member may ask for. |
| `pg_topics.group_max_session_ms` | `1800000` | Reload | The longest session timeout that a consumer group member may ask for. |
| `pg_topics.group_initial_rebalance_delay_ms` | `3000` | Reload | How long the first join window of a new or empty group stays open. |

To reload, edit the file and run `SELECT pg_reload_conf();`.

### Restart the listener after a change

The listener reads its settings once, when it starts. To apply a change, reload the configuration and stop the listener. PostgreSQL starts the listener again after 5 seconds.

Warning: this closes every Kafka connection to that database. Producers and consumers reconnect, and consumer groups rebalance.

```sql
SELECT pg_reload_conf();
SELECT pg_terminate_backend(pid) FROM topic.listener_status;
```

The listener binds its port on every IPv4 address of the host. Use a firewall to limit who can reach it.

### Settings that PostgreSQL needs

| PostgreSQL setting | What to set |
|---|---|
| `shared_preload_libraries` | Must include `pg_topics`. Restart. |
| `max_worker_processes` | At least 5 for each database in `pg_topics.databases`, plus what the rest of the server uses. |
| `max_connections` | Above `pg_topics.max_clients`, plus your other clients. Each Kafka client holds one connection. |
| `synchronous_standby_names` | Needed only for the `replicated` tier. See [Replication factor](#replication-factor). |
| `pg_hba.conf` | A `local` line for the bootstrap superuser, and a password line for `127.0.0.1/32`. See [Install](install.md#3-let-the-workers-connect). |

## Topics

A topic is a partitioned table named `schema.table_q`. The role that makes the topic owns the table.

### Make a topic

In SQL:

```sql
SELECT topic.create_topic('shop.orders_q',
    band_count         => 12,
    retention          => '3 days',
    min_durability     => 'durable',
    partition_interval => '1 hour');
```

| Parameter | Default | Rule |
|---|---|---|
| `topic` | none | `schema.table`. The schema has 1 to 63 characters and the table 1 to 47. Both use only `A-Z`, `a-z`, `0-9`, `_` and `-`. The table name ends in `_q`. |
| `band_count` | `4` | 1 to 1024. A band is a Kafka partition. You cannot change it later. |
| `retention` | `7 days` | How long a record stays. See [Retention](#retention). |
| `min_durability` | `durable` | `relaxed`, `durable` or `replicated`. See [Durability tiers](#durability-tiers). |
| `partition_interval` | `1 day` | The time width of each storage partition. At least `1 minute`. You cannot change it later. |

The caller needs `CREATE` on the schema. `create_topic` refuses to run in a database that is not in `pg_topics.databases`, unless that setting is empty.

With a Kafka admin tool:

```bash
kafka-topics --bootstrap-server kafka.example.internal:9092 --command-config admin.properties \
  --create --topic shop.orders_q --partitions 12 \
  --config retention.ms=259200000 --config pg_topics.min_durability=durable
```

`CreateTopics` accepts only the configs `retention.ms` and `pg_topics.min_durability`. It always uses a `partition_interval` of 1 day. `--partitions -1` gives 4 bands.

### Change a topic

Only a member of the owner role can change or drop a topic.

| To change | SQL | Kafka tool |
|---|---|---|
| Retention | `SELECT topic.set_retention('shop.orders_q', '14 days');` | `kafka-configs --alter --entity-type topics --entity-name shop.orders_q --add-config retention.ms=1209600000` |
| Durability tier | `SELECT topic.set_durability('shop.orders_q', 'relaxed');` | `--add-config pg_topics.min_durability=relaxed` |
| Backlog limit | `SELECT topic.set_backlog_limit('shop.orders_q', '2 minutes');` | none |
| Sync on or off | `SELECT topic.set_sync_enabled('shop.orders_q', false);` | none |
| Drop the topic | `SELECT topic.drop_topic('shop.orders_q');` | `kafka-topics --delete --topic shop.orders_q` |

You cannot change the band count or the partition interval. To change them, make a new topic and move the producers and consumers to it.

`drop_topic` drops the queue table and every control row of the topic. This includes the consumer group offsets for the topic. A plain `DROP TABLE` on the queue table does the same.

Warning: do not rename a queue table. The topic stops working, and its control row stays behind. Another role with `CREATE` on the schema can then make a new table with the old name.

### Retention

Records are stored in time partitions of `partition_interval` width. The partition worker drops a whole partition when its end is older than `now() - retention`. So a record stays for at least `retention`, and for at most `retention` plus one `partition_interval`.

The worker makes partitions 3 intervals ahead of the current time. It checks every 10 seconds.

Retention never drops a partition that still has a row with no offset. It attaches such a partition again and waits one more `retention` before it tries again. `detach_waiting` and `health()` show this.

A detach must wait for every older snapshot in the database to end. So one long query anywhere in the database stops retention on every topic until the query ends. `topic.oldest_xact` shows the oldest open transaction.

Retention applies only to the queue table. The sync base table and the error table keep every row until you delete it.

### Backlog limit

Each topic has a `max_backlog_age`. The default is 60 seconds and the minimum is 2 seconds. Every new publish to the topic fails in two cases:

- The oldest record without an offset is older than this limit.
- The stamper has not run on the topic for this long.

The error text starts with `topic: <schema>.<table> backlog_age` or `topic: the stamper has not run on`.

This limit stops a stuck stamper from filling the disk. A Kafka producer gets `UNKNOWN_SERVER_ERROR` for these batches.

## Durability tiers

Each topic has a floor, `min_durability`. A publish always gets at least the floor.

| Tier | `synchronous_commit` | An acknowledged record |
|---|---|---|
| `relaxed` | `off` | Is in the WAL buffer. A crash can lose up to 3 times `wal_writer_delay` of records, 600 ms with the default. |
| `durable` | `on` | Is flushed to disk. It survives `kill -9` and a power loss. |
| `replicated` | `remote_apply` | Is on disk, and the required synchronous standbys applied it. |

Kafka `acks` can ask for more than the floor, never for less:

| `acks` | Asks for |
|---|---|
| `0` | `relaxed`. The producer does not wait. |
| `1` | `relaxed` |
| `all` | `durable`, or `replicated` when `synchronous_standby_names` is set |

So with the default floor `durable`, the server flushes every record to disk for every `acks` value.

`durable` and `replicated` need `pg_topics.failover_is_fenced = on`. `replicated` also needs `synchronous_standby_names` to be set.

Warning: a SQL caller can lower `synchronous_commit` in the same transaction after `topic.publish`. This loses the guarantee. The Kafka path cannot do this.

### Replication factor

`CreateTopics` accepts a replication factor N only when PostgreSQL keeps N copies of each commit. The copies are the primary, plus the k synchronous standbys that must confirm each commit. `synchronous_standby_names` sets k:

| `synchronous_standby_names` | k | Largest replication factor |
|---|---|---|
| empty | 0 | 1 |
| `s1` or `s1, s2` | 1 | 2 |
| `2 (s1, s2)` or `FIRST 2 (s1, s2)` | 2 | 3 |
| `ANY 1 (s1, s2)` | 1 | 2 |
| `ANY 2 (s1, s2, s3)` | 2 | 3 |

- A replication factor of 1 or -1 makes a normal topic.
- A replication factor from 2 to 1 + k makes a `replicated` topic. It also needs `pg_topics.failover_is_fenced = on`.
- Any other value gets `INVALID_REPLICATION_FACTOR`. The message gives the number of copies that the standby setup keeps.
- A value above 1 with `pg_topics.min_durability` set to another tier gets `INVALID_CONFIG`.

`Metadata` always lists one replica, node 0, because pg_topics is one broker. The read-only topic config `pg_topics.replication_factor` gives the real number. For a `replicated` topic it is 1 + k from the current `synchronous_standby_names`. For any other topic it is 1.

With a primary and two standbys `s1` and `s2`:

- For 2 copies, use `ANY 1 (s1, s2)`. Commits continue when one standby stops.
- For 3 copies, use `FIRST 2 (s1, s2)` or `ANY 2 (s1, s2)`.

Warning: with 3 copies and two required standbys, one stopped standby blocks every commit in the whole server, not only on topics. The commit waits with no time limit until the standby comes back. Waiting publishers hold their connections, and enough of them use up `pg_topics.max_clients`. Watch `topic.syncrep_waiters`.

`synchronous_standby_names` must name only physical standbys. A logical subscriber confirms WAL positions also for tables that it does not hold.

## Permissions

Every path runs as a PostgreSQL role. A Kafka client logs in with a role name and password, and the listener runs its work as that role. There is no separate user list.

| Action | What the role needs |
|---|---|
| Make a topic | `CREATE` on the schema |
| Publish, SQL or Kafka | `INSERT` on the queue table columns `band, key, value, headers, producer_timestamp` |
| Read, SQL or Kafka | `SELECT` on the queue table |
| Commit a group offset | `SELECT` on the queue table, and membership in the group owner role |
| Change, drop or describe a topic config | Membership in the queue table owner role |
| Read the monitoring views | Membership in `pg_monitor` |

Give publish and read rights with these functions. The caller must be a member of the topic owner role:

```sql
SELECT topic.grant_publish('shop.orders_q', 'orders_service');
SELECT topic.grant_consume('shop.orders_q', 'billing_service');
```

Both functions refuse `public`. To give a right to every role on purpose, use a plain `GRANT`. To take a right away, use a plain `REVOKE`:

```sql
REVOKE INSERT ON shop.orders_q FROM orders_service;
REVOKE SELECT ON shop.orders_q FROM billing_service;
```

A role that cannot see a topic gets the same answer as for a topic that does not exist: `TOPIC_AUTHORIZATION_FAILED` on the Kafka path. So a client cannot find out which topics exist.

### Many tenants

Separate tenants by schema: one schema for each tenant, owned by that tenant's role.

Warning: do not use row level security to separate tenants on a topic. The bands are shared. `ListOffsets` reads the true end of each band, but `Fetch` returns only the rows that the policy allows. The consumer lag of such a tenant never reaches zero, and no error says why.

Warning: when you publish to a topic, you trust its owner. A trigger that the owner puts on the queue table runs as the publisher, the same as any PostgreSQL trigger. This applies to Kafka `Produce` too.

### Consumer group names

The whole database shares group names, as in one Kafka cluster. The role that first uses a group name owns that group. Another role gets `GROUP_AUTHORIZATION_FAILED`, unless it is a member of the owner role. Any role that can join a group can take a free name first. Tell each tenant to start its group names with its schema name, for example `shop.billing`.

The table sync reserves names that start with `__pg_topics_sync:`.

## Keep a table in sync with a topic

The sync worker can keep a normal table current from a topic. The table has one row for each thing, updated from the newest record for that thing.

### Set it up

For a table that you already have:

```sql
CREATE TABLE shop.orders (
    order_id bigint      PRIMARY KEY,
    status   text,
    total    numeric,
    event_at timestamptz NOT NULL
);
SELECT topic.attach('shop.orders'::regclass, sync_key => 'order_id', band_count => 4);
```

Or make the table and the topic together:

```sql
SELECT topic.create_table_topic('shop.orders',
    columns  => '{"order_id": "bigint", "status": "text", "total": "numeric"}',
    sync_key => 'order_id');
```

Both make the topic `shop.orders_q` if it does not exist, and an error table `shop.orders_qe`. The base table must:

- have the `sync_key` column, `NOT NULL`. Make it unique, for example the primary key. `create_table_topic` makes it the primary key.
- have a column `event_at timestamptz NOT NULL`.

The caller must be a member of the base table owner role and have `CREATE` on the schema.

When you attach a topic that already has records, the sync starts at the oldest record that retention kept.

### How a record becomes a row

The sync reads the `value` of each record as a JSON object:

- The field with the name of `sync_key` picks the row.
- Each field with the name of a column fills that column. Other fields are ignored.
- On an insert, a missing field is NULL. On an update, a missing field keeps the current value.
- `event_at` gets the `published_at` of the record. A record that is older than the `event_at` of the row does not overwrite it. So records on different bands can arrive in any order.
- A record with a key and a NULL value, a tombstone, deletes the row. For a tombstone, the record key is the sync key value. The JSON value `null` is not a tombstone.
- A tombstone deletes the row, and the sync keeps no trace of the delete. So a record for the same key with an older `published_at` that arrives after the tombstone makes the row again. This can happen when a long transaction publishes the record before the tombstone and commits after it.

### When a record fails

A record that does not fit goes to the error table `shop.orders_qe`, with the reason in `error`. Two examples are a value that does not cast to the column type and a missing value for a `NOT NULL` column. The sync goes on with the next record. The record stays in the topic.

Only a data error or a constraint error, SQLSTATE class 22 or 23, sends a record to the error table. Any other error stops the sync at that record, and `topic.health()` shows `ok = false`. The sync tries again each second. So a trigger on the base table that refuses a record must raise a class 22 or 23 error, for example `RAISE EXCEPTION 'bad order' USING ERRCODE = 'check_violation'`.

Nothing clears the error table. Fix the cause. Then run the failed records again:

```sql
SELECT * FROM topic.retry_errors('shop.orders_q');
```

It works on the 50 oldest failed rows in each call. It returns `retried`, the number of rows that now succeeded, and `failed`, the number that failed again. A row that succeeds leaves the error table. A row that fails again gets the new error and a new `failed_at`. Call it again until the error table is empty or holds only rows that you do not want.

Warning: a schema mistake that fails every record fills the error table at the full rate of the topic. `topic.error_rows()` shows the count.

### Change or stop the sync

- `ALTER TABLE` on the base table needs no other step. The sync reads the columns again on each batch.
- `SELECT topic.set_sync_enabled('shop.orders_q', false);` pauses the sync. `true` starts it again from where it stopped.
- `SELECT topic.set_sync('shop.orders_q', 'shop.orders_v2'::regclass, 'order_id');` points the sync at another table with the same owner.
- If you drop the base table or the sync key column, the sync stops with a `WARNING`.

`topic.sync_lag` shows how many records the sync is behind, per band.

Warning: the base table is real data, not a cache. After retention drops old records, you cannot build the table again from the topic. Back it up.

## Consumer groups

Kafka consumer groups work as in Kafka. The standard tool works:

```bash
kafka-consumer-groups --bootstrap-server kafka.example.internal:9092 --command-config admin.properties --list
kafka-consumer-groups ... --describe --group shop.billing
kafka-consumer-groups ... --delete --group shop.billing
kafka-consumer-groups ... --reset-offsets --to-earliest --group shop.billing --topic shop.orders_q --execute
```

A reset works only when the group has no members, as in Kafka.

Committed offsets never expire by default. Kafka removes the offsets of an empty group after 7 days. pg_topics keeps them until you delete the group. pg_topics removes an empty group with no offsets after 1 day.

The table sync shows up as a group named `__pg_topics_sync:<schema>.<table>`. You cannot delete it or join it.

## Backups and restore

Queue tables, base tables, error tables and the control tables are normal tables. So `pg_dump` and physical backups cover them. `pg_dump` includes the rows of the control tables.

Warning: stamp every record before a `pg_dump`. Check that this returns 0 for each topic:

```sql
SELECT count(*) FROM shop.orders_q WHERE log_offset IS NULL;
```

After a restore into another cluster, a restored record with no offset can get a higher offset than a new record. A Kafka producer can also get a wrong offset in its answer. If you cannot stamp first, do not publish until the restored records have offsets.

A physical backup or a replica has no such problem.

## Standbys and failover

A streaming standby keeps a full copy of every topic.

- The 5 workers do not run on a standby while it is in recovery. Its listener port stays closed.
- A normal role can read a topic on a standby with `topic.fetch` and `topic.band_offsets`. This moves history reads off the primary.
- `ALTER DATABASE ... SET pg_topics.port` replicates. A standby on the same host as the primary must set its own `pg_topics.port` in its own `postgresql.conf`.

When you promote a standby, its 5 workers start and its listener opens its port. Each band continues from the replicated `next_offset`.

For failover:

1. Stop the old primary, or cut it off from clients. This is the fence that `pg_topics.failover_is_fenced` promises.
2. Promote the standby.
3. Point `pg_topics.advertised_host` at the new primary. The simplest way is a DNS name that you move, so the setting does not change.

Kafka clients reconnect to the advertised host. Consumer groups rebalance.

Warning: if the old primary still takes writes after the promotion, both nodes give out the same offsets to different records. pg_topics cannot repair this. The promoted node logs a `WARNING` that `stamped_by` changed, for example `topic.stamp_topic: shop.orders_q band 0 was stamped by 7412345678901234567/1, now by 7412345678901234567/2`. A normal promotion logs this too. The old primary logs nothing. To find the split point, compare the rows of the two nodes by `published_at`.

## More than one database

Each database in `pg_topics.databases` has its own 5 workers, its own listener and its own port. A topic belongs to one database. A Kafka client connection sees only the topics of the database whose port it uses. Roles are shared by the whole server, and grants are per database.
