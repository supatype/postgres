# Use pg_topics from SQL

A SQL user can do everything a Kafka client can do. The functions are in the
schema `topic`. A SQL publish joins your own transaction, so it commits
with your other writes or not at all. You need no outbox table.

The examples use the topic `shop.orders_q` with 4 bands. A band is what
Kafka calls a partition.

## Publish

```sql
SELECT topic.publish('shop.orders_q', '{"order_id": 1, "status": "new"}', key => 'customer-42');
```

| Parameter | Meaning |
|---|---|
| `topic` | The topic, `schema.table_q` |
| `value` | A `jsonb` value. NULL makes a tombstone. |
| `key` | Optional. Text of 40 characters or fewer. Records with the same key go to the same band, and keep their order. |
| `headers` | Optional. A JSON array of headers, see [Headers](#headers). |

A record with a key goes to the band that Kafka's own partitioner picks for
that key. A Java producer and `topic.publish` put the same key on the same
band. A record with no key goes to the next band in order. Each connection keeps
its own count.

The caller needs publish rights on the topic. See
[Permissions](configuration.md#permissions).

### Publish in your own transaction

```sql
BEGIN;
UPDATE shop.stock SET count = count - 1 WHERE sku = 'A-100';
SELECT topic.publish('shop.orders_q', '{"order_id": 2, "sku": "A-100"}', key => 'customer-42');
COMMIT;
```

Consumers see the record only after the `COMMIT`. A `ROLLBACK` removes the record.

The commit waits for the topic's durability tier. On a `durable` topic it
waits for the disk flush, even if your session sets `synchronous_commit`
lower.

### Many records in one statement

```sql
SELECT topic.publish('shop.orders_q', jsonb_build_object('order_id', i), key => 'customer-' || (i % 100))
FROM generate_series(1, 1000) i;
```

A role with publish rights can also insert into the queue table directly.
You then give the band yourself:

```sql
INSERT INTO shop.orders_q (band, key, value)
SELECT topic.band_for('customer-' || (i % 100), 4), 'customer-' || (i % 100), jsonb_build_object('order_id', i)
FROM generate_series(1, 1000) i;
```

`topic.band_for(key, band_count)` gives the band that Kafka's partitioner
picks. An insert must not set `log_offset`, `published_by`, `xact` or a
future `published_at`. A trigger refuses such a row.

### The order of offsets

A record gets its offset a short time after the commit, from the stamper
worker. The stamper gives offsets only to committed records, so offsets
follow commit order as the stamper sees it. When one stamper pass sees
several committed transactions, the one that first wrote to the topic gets
the lower offsets. The records of one transaction get offsets next to each
other in each band.

Warning: a transaction can publish, then read records that another
transaction committed, then publish again. Its last records can get offsets
below those records. To avoid this, do not publish before a read in the same
transaction.

## Read

```sql
SELECT * FROM topic.fetch('shop.orders_q', band => 0, from_offset => 0, max_rows => 500);
```

It returns `log_offset`, `key`, `value`, `headers` and `published_at`, in
offset order, for records that already have an offset.

| Parameter | Meaning |
|---|---|
| `band` | The band to read |
| `from_offset` | The first offset to return |
| `max_rows` | At most this many rows. Default 500. |
| `filter` | Optional `jsonb`. Returns only records whose value contains it, as with the `@>` operator. |

A `from_offset` below the oldest offset that retention kept raises an error
with SQLSTATE `PT001`. The caller needs read rights on the topic.

### Offsets of each band

```sql
SELECT * FROM topic.band_offsets('shop.orders_q');
```

It returns `band`, `oldest_offset` and `next_offset` for each band.
`next_offset` is the offset that the next record gets.

### Find an offset by time

```sql
SELECT topic.offset_for_time('shop.orders_q', 0, now() - interval '1 hour');
```

It returns the first offset in band 0 published at or after that time, or
NULL.

### Filter on the server

```sql
SELECT * FROM topic.fetch('shop.orders_q', 0, 0, 500, filter => '{"status": "paid"}');
```

This is an equality match on fields. It does not do ranges. For a large
topic, add a GIN index on `value`:

```sql
CREATE INDEX ON shop.orders_q USING gin (value jsonb_path_ops);
```

### Read on a standby

`topic.fetch` and `topic.band_offsets` work on a streaming standby, for a
normal role. Commit only offsets that you read and processed from that
standby. Do not take an end offset from the primary and commit it because
the standby returned no rows.

## Keep a position

A SQL reader can keep its position without a Kafka group join.
Commit with generation `-1`:

```sql
SELECT topic.commit_offset('shop.orders_q', 'shop.shipper', 0, 10, -1);
SELECT topic.fetch_offset('shop.orders_q', 'shop.shipper', 0);
```

The committed offset is the next offset to read, as in Kafka. `commit_offset`
returns `NONE` on success, or a Kafka error name such as
`TOPIC_AUTHORIZATION_FAILED`. `fetch_offset` returns NULL when the group has
no offset for that band.

A commit with generation `-1` works only while the group has no members.
It can also move the offset back.

### Process and commit in one transaction

Put your work and the commit in one transaction. A crash then keeps both
or loses both, so your work handles each record once:

```sql
WITH batch AS (SELECT * FROM topic.fetch('shop.orders_q', 0, 10, 500)),
     work AS (INSERT INTO shop.shipments (order_id)
              SELECT (value->>'order_id')::bigint FROM batch)
SELECT topic.commit_offset('shop.orders_q', 'shop.shipper', 0, max(log_offset) + 1, -1)
FROM batch HAVING count(*) > 0;
```

One statement is one transaction, so the insert and the commit succeed or
fail together. The statement reads the batch once, so the commit matches
the rows that it processed.

This works only when the work writes to the same database.

### Wait for new records

The stamper sends a notification on the channel `pg_topics_stamped` each
time it gives out offsets. The payload is the topic name:

```sql
LISTEN pg_topics_stamped;
```

Wait for a notification with the payload `shop.orders_q`, then call
`topic.fetch` again.

## Join a group with Kafka members

A SQL reader can be a full member of a Kafka consumer group. These
functions do the same work as the Kafka requests with the same names:

| Function | Kafka request |
|---|---|
| `topic.group_join(group_name, member_id, client_id, session_ms, rebalance_ms, protocol_type, protocols)` | `JoinGroup` |
| `topic.group_sync(group_name, member_id, generation, assignments)` | `SyncGroup` |
| `topic.group_heartbeat(group_name, member_id, generation)` | `Heartbeat` |
| `topic.group_leave(group_name, member_id)` | `LeaveGroup` |
| `topic.commit_offset(topic, group_name, band, offset, generation)` | `OffsetCommit` |
| `topic.fetch_offset(topic, group_name, band)` | `OffsetFetch` |
| `topic.delete_group(group_name)` | `DeleteGroups` |

The first `group_join` with an empty `member_id` returns the error
`MEMBER_ID_REQUIRED` and a new `member_id`. Call `group_join` again with
that `member_id`:

```sql
SELECT * FROM topic.group_join('shop.shipper', '', 'client-1', 30000, 5000,
    'consumer', '[{"name": "range"}]');
```

The leader gets the member list and sends each member's bands in
`group_sync`. Each member then calls `group_heartbeat` more often than
`session_ms`.

Warning: a group can mix Kafka and SQL members only when every SQL member
sends the metadata of the Kafka consumer protocol in `protocols`, as a base64
string. Without it, a Kafka leader gives the SQL member no bands.

## Headers

Headers are a JSON array, in record order. A name can occur many
times:

```json
[{"key": "trace", "value": "abc"}, {"key": "bin", "value_base64": "/wAB"}, {"key": "n", "value": null}]
```

- A value that is valid UTF-8 is text in `value`.
- Any other value is standard base64 in `value_base64`.
- A null value is `"value": null`.

Give `publish` headers in the same format. A Kafka consumer then gets the
same bytes.

## Other functions

| Function | Use |
|---|---|
| `topic.create_topic`, `topic.drop_topic` | Create and drop a topic, see [Configure](configuration.md#topics) |
| `topic.set_retention`, `topic.set_durability`, `topic.set_backlog_limit` | Change a topic |
| `topic.grant_publish`, `topic.grant_consume` | Give rights |
| `topic.attach`, `topic.create_table_topic`, `topic.set_sync`, `topic.set_sync_enabled`, `topic.retry_errors` | The table sync, see [Configure](configuration.md#keep-a-table-in-sync-with-a-topic) |
| `topic.describe_configs(topic)` | The Kafka configs of a topic |
| `topic.describe_group(group_name)` | The members of a group |
| `topic.health()` and the monitoring views | See [Operate](operations.md) |
