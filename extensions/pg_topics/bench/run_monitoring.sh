#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster
start_listener

psql_as postgres "CREATE ROLE mon_ok LOGIN IN ROLE pg_monitor;
  CREATE ROLE mon_no LOGIN" >/dev/null

state_as() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U "$1" -d postgres -tA -v VERBOSITY=verbose -c "$2" 2>&1 |
    sed -n 's/^ERROR:  \([0-9A-Z]\{5\}\): .*/\1/p'
}

objects="topic.stamp_backlog topic.oldest_xact topic.detach_waiting topic.write_partition_dead_tuples
  topic.partition_headroom topic.worker_headroom topic.listener_status topic.group_expiry
  topic.producer_rows topic.syncrep_waiters topic.sync_lag topic.consumer_lag
  topic.error_rows() topic.duplicate_offsets() topic.health()"
for obj in $objects; do
  chk "pg_monitor reads $obj" "" "$(state_as mon_ok "SELECT count(*) FROM $obj")"
  chk "a role without pg_monitor is refused on $obj" 42501 "$(state_as mon_no "SELECT count(*) FROM $obj")"
done

psql_as postgres "SELECT topic.create_topic('public.mon_backlog_q', 1);
  UPDATE topic.topic_config SET max_backlog_age = '2 seconds' WHERE topic = 'mon_backlog_q'" >/dev/null
before=$(psql_as postgres "SELECT ok FROM topic.health() WHERE topic = 'mon_backlog_q'")
chk "health() reports ok=true for an idle, freshly stamped topic" t "$before"

hold_stamp_lock public.mon_backlog_q
psql_as postgres "SELECT topic.publish('public.mon_backlog_q', '{}')" >/dev/null
sleep 2
out=$(psql_as postgres "SELECT schema_name || ':' || topic || ':' || ok FROM topic.health() WHERE topic = 'mon_backlog_q'")
echo "RED: health() while the stamper cannot take public.mon_backlog_q -- $out"
chk "health() reports ok=false while the stamper is stalled past half its backlog limit" "public:mon_backlog_q:false" "$out"
release_stamp_lock
wait_for "[ \"\$(unstamped public.mon_backlog_q)\" = 0 ]"
chk "health() reports ok=true again once the stamper catches up" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT ok FROM topic.health() WHERE topic = 'mon_backlog_q'\")\" = t ]" && echo yes || echo no)"

psql_as postgres "SELECT topic.create_topic('public.mon_head_q', 1)" >/dev/null
out=$("$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -tA -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
DO $$
DECLARE p text; cur text;
BEGIN
    cur := 'mon_head_q_p' || to_char(date_bin('1 day', now(), timestamptz '2000-01-01 00:00:00+00')
                                      AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS');
    FOR p IN SELECT c.relname FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
             WHERE h.inhparent = 'public.mon_head_q'::regclass AND c.relname <> cur
    LOOP
        EXECUTE format('ALTER TABLE public.mon_head_q DETACH PARTITION public.%I', p);
        EXECUTE format('DROP TABLE public.%I', p);
    END LOOP;
END $$;
SELECT topic || ':' || (partition_headroom < interval '1 day') || ':' || ok
FROM topic.health() WHERE topic = 'mon_head_q';
COMMIT;
SQL
)
out=$(grep '^mon_head_q:' <<<"$out")
echo "RED: health() after dropping every lookahead partition of public.mon_head_q -- $out"
chk "health() reports ok=false when partition headroom drops below its threshold" "mon_head_q:true:false" "$out"

psql_as postgres "SELECT topic.create_topic('public.mon_dup_q', 1, partition_interval => '1 hour');
  SELECT topic.publish('public.mon_dup_q', '{}')" >/dev/null
wait_for "[ \"\$(unstamped public.mon_dup_q)\" = 0 ]"
psql_as postgres "SET session_replication_role = replica;
  INSERT INTO public.mon_dup_q (band, value, published_at)
  VALUES (0, '{}', date_bin('1 hour', now(), '2000-01-01') + interval '1 hour');
  UPDATE public.mon_dup_q SET log_offset = 0 WHERE log_offset IS NULL" >/dev/null
out=$(psql_as postgres "SELECT topic || ':' || ok FROM topic.health() WHERE topic = 'mon_dup_q'")
echo "RED: health() after forcing a duplicate log_offset on public.mon_dup_q -- $out"
chk "health() reports ok=false when duplicate_offsets finds a duplicate" "mon_dup_q:false" "$out"
chk "duplicate_offsets reports the forced duplicate" "public:mon_dup_q:0:0:2" \
  "$(psql_as postgres "SELECT schema_name || ':' || topic || ':' || band || ':' || log_offset || ':' || copies
                       FROM topic.duplicate_offsets() WHERE topic = 'mon_dup_q'")"

psql_as postgres "SELECT topic.create_table_topic('public.mon_bottles', '{\"bottle_id\": \"uuid\", \"name\": \"text\"}', 'bottle_id');
  ALTER TABLE public.mon_bottles ALTER COLUMN name SET NOT NULL;
  SELECT topic.publish('public.mon_bottles_q', '{\"bottle_id\": \"11111111-1111-1111-1111-111111111111\"}')" >/dev/null
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM public.mon_bottles_qe\")\" = 1 ]"
chk "error_rows counts the record that failed to sync" "public:mon_bottles_q:1:1" \
  "$(psql_as postgres "SELECT schema_name || ':' || topic || ':' || rows || ':' || recent
                       FROM topic.error_rows() WHERE topic = 'mon_bottles_q'")"

psql_as postgres "SELECT topic.create_topic('public.mon_gone_q', 1)" >/dev/null
psql_as postgres "SET session_replication_role = replica; ALTER TABLE public.mon_gone_q RENAME TO mon_gone_renamed" >/dev/null
out=$(psql_as postgres "SELECT topic || ':' || ok FROM topic.health() WHERE topic = 'mon_gone_q'" || true)
echo "RED: health() after renaming the queue table of public.mon_gone_q -- $out"
chk "health() reports ok=false for a topic whose queue table was renamed away" "mon_gone_q:false" "$out"
chk "duplicate_offsets skips a topic whose queue table was renamed away, instead of erroring" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.duplicate_offsets() WHERE topic = 'mon_gone_q'" || true)"

psql_as postgres "SELECT topic.create_table_topic('public.mon_errtab', '{\"id\": \"int\"}', 'id');
  DROP TABLE public.mon_errtab_qe" >/dev/null
chk "error_rows skips a topic whose error table is gone, instead of erroring" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.error_rows() WHERE topic = 'mon_errtab_q'" || true)"

psql_as postgres "SELECT topic.create_topic('public.mon_cross_q', 1)" >/dev/null
psql_as postgres "CREATE DATABASE bind_fail_db" >/dev/null
psql_as postgres "ALTER SYSTEM SET pg_topics.databases = 'postgres, bind_fail_db'" >/dev/null
psql_as postgres "ALTER SYSTEM SET max_worker_processes = 16" >/dev/null
psql_as postgres "ALTER DATABASE bind_fail_db SET pg_topics.port = $PORT" >/dev/null
stop_pg >/dev/null
start_pg >/dev/null
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d bind_fail_db -c "CREATE EXTENSION pg_topics" >/dev/null
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d bind_fail_db -c "SELECT topic.create_topic('public.bind_fail_q', 1)" >/dev/null
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'pg_topics listener'\")\" = 2 ]"
out=$("$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d bind_fail_db -tAc \
  "SELECT listener_bound || ':' || ok FROM topic.health() WHERE topic = 'bind_fail_q'")
echo "RED: health() in bind_fail_db, whose own listener could not bind $PORT -- $out"
chk "health() reports listener_bound=false and ok=false for a database whose own listener failed to bind" "false:false" "$out"
chk "health() in postgres still reports its own bound listener as true, unaffected by bind_fail_db" t \
  "$(psql_as postgres "SELECT listener_bound FROM topic.health() WHERE topic = 'mon_cross_q'")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
