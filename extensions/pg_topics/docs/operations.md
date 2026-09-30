# Operate pg_topics

This guide shows what to watch, when to alert, and what to do when something goes wrong. It assumes that pg_topics is installed and configured. See [Install](install.md) and [Configure](configuration.md).

## The one query to poll

```sql
SELECT * FROM topic.health();
```

It returns one row for each topic in the database. Poll it from the tool that already watches PostgreSQL. You need no separate exporter. Alert when any row has `ok = false`.

| Column | Meaning |
|---|---|
| `backlog_age` | The age of the oldest record that has no offset yet, when the stamper last measured it |
| `stamped_at` | When the stamper last ran on this topic |
| `partition_headroom` | How far into the future the newest storage partition reaches |
| `listener_bound` | The Kafka listener of this database has its port |
| `duplicate` | The topic has a duplicate offset. This is the most serious signal. |
| `syncrep_waiting` | A connection waits for a synchronous standby |
| `ok` | False when any check below fails |

`ok` is false when any of these is true:

- `now() - stamped_at` is more than half of the topic's `max_backlog_age`.
- `partition_headroom` is less than 1 day, or less than the topic's `partition_interval` when that is shorter.
- The listener is not bound.
- The topic has a duplicate offset.
- Any connection waits on `SyncRep`.
- The queue table is gone.

Only members of `pg_monitor` can run `health()` and read the views below. Give your monitoring role that membership:

```sql
GRANT pg_monitor TO monitoring;
```

## Monitoring views

Every view is in the schema `topic`.

| View or function | Reports | Alert when |
|---|---|---|
| `stamp_backlog` | Each topic's `backlog_age` and `stamped_at` | `backlog_age` grows toward `max_backlog_age` |
| `consumer_lag` | Records behind, for each group, topic and band | Lag grows without limit |
| `sync_lag` | Records behind, for each synced topic and band | Lag grows without limit |
| `error_rows()` | Rows in each sync error table, in total and in the last hour | `recent` is above 0 |
| `partition_headroom` | How long the newest partition still covers | Below 1 day |
| `detach_waiting` | A retention detach that started, and whether it waits on a lock | A row stays for more than a few minutes |
| `oldest_xact` | The age of the oldest open transaction | Older than a few minutes. It stops retention. |
| `write_partition_dead_tuples` | Dead rows in the partition that each topic writes now | Keeps growing |
| `worker_headroom` | Free slots in `max_worker_processes` | Below 1 |
| `listener_status` | What each listener reported at start | The text is not `listening on port N` |
| `group_expiry` | Members of each group that expired because they missed a heartbeat | Grows fast |
| `producer_rows` | Rows kept for idempotent producers, per topic | Keeps growing |
| `syncrep_waiters` | Connections that wait for a synchronous standby | Any row |
| `duplicate_offsets()` | Every duplicate offset in every topic | Any row |

The partition worker also checks the whole history of every topic for a duplicate offset once an hour. It logs a `WARNING` for each one.

## What the workers log

Each worker writes to the PostgreSQL log. Each line starts with a fixed text:

| Start of the line | Worker |
|---|---|
| `pg_topics listener <database>:` | The listener, once at start, with its status |
| `pg_topics listener: <topic>:` | A server error inside a Kafka request |
| `pg_topics partition worker:` | Partition creation, retention and the hourly duplicate check |
| `pg_topics sync worker:` | The table sync |
| `pg_topics stamper:` | An error in the stamper |
| `pg_topics replicated stamper:` | An error in the stamper of the replicated topics |
| `topic.stamp_topic:` | The stamper saw a changed `stamped_by`, for example after a promotion |

Each worker restarts 5 seconds after it stops.

## Fix a problem

### A Kafka client cannot connect

| What the client sees | Cause and fix |
|---|---|
| TLS handshake error | The certificate does not name the host that the client uses, or the client does not trust it. Give the client the certificate as its CA. Check `subjectAltName`. |
| `SASL_AUTHENTICATION_FAILED` with a PostgreSQL message | Wrong role or password, or no `pg_hba.conf` password line for `127.0.0.1`. |
| `SASL_AUTHENTICATION_FAILED`: `the connection to 127.0.0.1 did not use a password method` | The `pg_hba.conf` line for `127.0.0.1` is `trust`, `peer` or another method that is not a password method. Use a password method. |
| `SASL_AUTHENTICATION_FAILED`: `the listener has max_clients authenticated clients` | Raise `pg_topics.max_clients`, then restart the listener. |
| The client connects once, then fails to reach `localhost` or a wrong host | Set `pg_topics.advertised_host` to a name that the client can reach, then restart the listener. |
| Connection refused | Check `topic.listener_status`. See the next section. |

### The listener did not start

`SELECT query FROM topic.listener_status;` gives the reason:

| Status | Fix |
|---|---|
| `no listener: pg_topics.port is 0` | Set a port. |
| `no listener: set pg_topics.tls_cert_file and pg_topics.tls_key_file, ...` | Give the listener a certificate. |
| `no listener: set both pg_topics.tls_cert_file and pg_topics.tls_key_file` | One of the two is empty. |
| `bind failed: Address already in use ...` | Another process, or the listener of another database, has the port. Give each database its own port. |
| No row | The database is not in `pg_topics.databases`, or `pg_topics` is not in `shared_preload_libraries`. |

### Produce fails

| Error | Cause and fix |
|---|---|
| `TOPIC_AUTHORIZATION_FAILED` | The topic name is wrong, or the role has no rights on it. The name is `schema.table_q`. |
| `INVALID_RECORD` | A value is not valid JSON, or a key is longer than 40 characters or not UTF-8. The whole batch fails. |
| `CORRUPT_MESSAGE` | A header name is not valid UTF-8. |
| `MESSAGE_TOO_LARGE` | A batch is larger than `pg_topics.max_message_bytes`. Lower the `batch.size` of the client, or raise the setting. |
| `REQUEST_TIMED_OUT` | The stamper did not give the records offsets in time. The records are stored. A retry from an idempotent producer gets the first offsets back. Check `stamp_backlog`. |
| `UNKNOWN_SERVER_ERROR` | Often the backlog limit, which means the stamper is too far behind. Check `stamp_backlog` and the log. The log has other causes, on a line that starts `pg_topics listener:`. |
| `UNKNOWN_PRODUCER_ID` | The producer was idle for more than 7 days. Restart the producer. |

### The backlog grows

The stamper gives out offsets for every topic in the database, one topic after another. The replicated stamper does the same for the replicated topics. When `backlog_age` grows:

1. Check that the stamper runs:
   `SELECT * FROM pg_stat_activity WHERE backend_type IN ('pg_topics stamper', 'pg_topics replicated stamper');`
   A replicated stamper with `wait_event` `SyncRep` waits for a standby to apply its commit.
2. Check for a lock on the topic's band rows in `pg_locks`.
3. Compare the publish rate with the stamper limits in
   [Tune](tuning.md#the-stamper).

When `backlog_age` passes `max_backlog_age`, new publishes fail. This stops the disk from getting full. Records that were already committed are safe.

### Retention does not drop old partitions

1. Check `topic.oldest_xact`. A detach waits for every older snapshot. End
   the long transaction.
2. Check `topic.detach_waiting`. `worker_waiting = true` means that the
   detach waits on a lock.
3. Check the log for `has rows with no log_offset, so retention attached it
   again`. That topic holds retention for one more `retention` interval.
4. The partition worker connects over the Unix socket as the bootstrap
   superuser. If the log says `cannot connect to <database> over the
   socket`, fix the `local` line in `pg_hba.conf`.

### Publishes hang

Check `topic.syncrep_waiters`. A `replicated` topic waits for the required standbys with no time limit. So does any commit at `synchronous_commit = on` or higher when `synchronous_standby_names` is set.

Warning: do not cancel the connections that wait. PostgreSQL already committed their records locally. A cancelled connection reports failure for a record that is stored and stays stored.

Bring the standby back, or change `synchronous_standby_names` to a quorum such as `ANY 1 (s1, s2)` and reload.

### A consumer's lag never reaches zero

A row level security policy on the queue table hides rows from that consumer, but the band end counts them. Remove the policy and separate tenants by schema.

### The sync error table grows

`SELECT * FROM shop.orders_qe ORDER BY failed_at DESC LIMIT 10;` shows the reasons. Fix the base table or the producer. Then run `SELECT * FROM topic.retry_errors('shop.orders_q');` until it returns 0 failed rows.

### A duplicate offset

A duplicate offset means that two stampers gave out the same offsets. The usual cause is a failover where the old primary still took writes.

1. Stop publishing to the topic.
2. Find the split point. Compare the rows of both nodes by `published_at`.
   You cannot compare `seq` across nodes after a promotion.
3. Check that the split point is at or below the lowest committed offset of
   every consumer group.
4. Keep one side, and reset every consumer group to the split point.

pg_topics does not repair a split for you.

## Restart and upgrade

A restart stops every worker and closes every Kafka connection. Consumer groups rebalance once when their clients come back. The stamper continues with the records that have no offset yet. The restart loses no committed record.

To install a new build, see [Install](install.md#upgrade-or-reinstall).

## Failover

See [Standbys and failover](configuration.md#standbys-and-failover).
