#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "SELECT topic.create_table_topic('public.bottles', '{\"bottle_id\": \"int\", \"n\": \"int\"}', 'bottle_id', 4);
                  SELECT topic.create_table_topic('public.casks', '{\"cask_id\": \"int\", \"n\": \"int\"}', 'cask_id', 1)" >/dev/null

caught_up() {
  psql_as postgres "SELECT bool_and(o.committed_offset = p.next_offset) AND sum(p.next_offset) = $2
                    FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
                    WHERE o.group_name = '__pg_topics_sync:public.$1'"
}

for s in 1 2 3 4; do
  (for t in $(seq 1 25); do
    psql_as postgres "SELECT topic.publish('public.bottles_q',
                        jsonb_build_object('bottle_id', 1 + floor(random() * 100)::int, 'n', $s * 100000 + $t * 1000 + i),
                        md5(random()::text))
                      FROM generate_series(1, 100) i" >/dev/null
  done) &
done
wait

chk "the worker keeps up with 10000 updates" yes \
  "$(wait_for "[ \"\$(caught_up bottles_q 10000)\" = t ]" && echo yes || echo no)"
chk "the updates used all 4 bands" 4 "$(psql_as postgres "SELECT count(DISTINCT band) FROM public.bottles_q")"
chk "the base table equals the newest record per key" 0 \
  "$(psql_as postgres "SELECT count(*) FROM public.bottles b
                       FULL JOIN (SELECT DISTINCT ON ((value->>'bottle_id')::int) (value->>'bottle_id')::int AS bottle_id,
                                         (value->>'n')::int AS n, published_at
                                  FROM public.bottles_q ORDER BY 1, published_at DESC, band DESC, log_offset DESC) q
                       USING (bottle_id)
                       WHERE b.n IS DISTINCT FROM q.n OR b.event_at IS DISTINCT FROM q.published_at")"
chk "the base table has one row per key" \
  "$(psql_as postgres "SELECT count(DISTINCT value->>'bottle_id') FROM public.bottles_q")" \
  "$(psql_as postgres "SELECT count(*) FROM public.bottles")"
chk "the error table is empty" 0 "$(psql_as postgres "SELECT count(*) FROM public.bottles_qe")"

xid_before=$(psql_as postgres "SELECT txid_current()")
sleep 3
xids=$(($(psql_as postgres "SELECT txid_current()") - xid_before))
echo "transaction ids used in 3 s by 2 idle synced topics: $xids"
chk "2 idle synced topics use fewer than 20 transaction ids in 3 s" yes "$([ "$xids" -lt 20 ] && echo yes || echo no)"

psql_as postgres "DROP TABLE public.casks_qe;
                  SELECT topic.publish('public.casks_q', '{\"cask_id\": 1, \"n\": \"bad\"}')" >/dev/null
chk "the sync worker stops syncing and logs a WARNING when the error table is gone" yes \
  "$(wait_for "grep -q 'topic.sync_topic: public.casks_q stops syncing, because its base table or the column cask_id is gone, or its error table has another owner or is gone' '$PGDATA/log'" && echo yes || echo no)"
chk "casks_q has sync_enabled turned off" f \
  "$(psql_as postgres "SELECT sync_enabled FROM topic.topic_config WHERE topic = 'casks_q'")"
psql_as postgres "SELECT topic.publish('public.bottles_q', '{\"bottle_id\": 1, \"n\": -1}')" >/dev/null
chk "while one topic fails, the other topic syncs" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT n FROM public.bottles WHERE bottle_id = 1\")\" = -1 ]" && echo yes || echo no)"
chk "the failed topic keeps its position" 0 \
  "$(psql_as postgres "SELECT committed_offset FROM topic.topic_offsets WHERE group_name = '__pg_topics_sync:public.casks_q'")"

psql_as postgres "ALTER TABLE public.bottles_q DROP CONSTRAINT bottles_q_band_check" >/dev/null
chk "a changed band CHECK on a queue table gives a WARNING" yes \
  "$(grep -q 'WARNING:  topic: the CHECK constraints on band of public.bottles_q are <NULL>, which do not match band_count 4' "$PGDATA/log" && echo yes || echo no)"

psql_as postgres "CREATE ROLE tenant_a LOGIN; CREATE SCHEMA tenant_a AUTHORIZATION tenant_a" >/dev/null
psql_as tenant_a "SELECT topic.create_table_topic('tenant_a.kegs', '{\"keg_id\": \"int\", \"n\": \"int\"}', 'keg_id', 1)" >/dev/null
mkfifo "$WORK/hold.fifo"
"$PGBIN/psql" -h /tmp -p "$PORT" -U tenant_a -d postgres -q -f "$WORK/hold.fifo" >"$WORK/hold.out" 2>&1 &
exec 7>"$WORK/hold.fifo"

granted() {
  psql_as postgres "SELECT count(*) FROM pg_locks l JOIN pg_stat_activity a USING (pid)
                    WHERE l.relation = '$1'::regclass AND l.mode = 'AccessExclusiveLock' AND l.granted AND a.usename = 'tenant_a'"
}

synced_on_time() {
  psql_as postgres "SELECT topic.publish('public.bottles_q', jsonb_build_object('bottle_id', 2, 'n', $1))" >/dev/null
  wait_for "[ \"\$(psql_as postgres \"SELECT n FROM public.bottles WHERE bottle_id = 2\")\" = $1 ]" >/dev/null &
  local waiter=$! start
  start=$(date +%s%N)
  wait "$waiter" && [ $((($(date +%s%N) - start) / 1000000)) -le 2000 ] && echo yes || echo no
}

psql_as tenant_a "SELECT topic.publish('tenant_a.kegs_q', '{\"keg_id\": 1, \"n\": 1}')" >/dev/null
echo "BEGIN; ALTER TABLE tenant_a.kegs ADD COLUMN extra int;" >&7
wait_for "[ \"\$(granted tenant_a.kegs)\" = 1 ]"
sleep 1.5
chk "while a tenant holds ALTER TABLE on its base table open, another topic is stamped and synced within 2 s" yes \
  "$(synced_on_time 101)"
echo "ROLLBACK;" >&7
echo "BEGIN; LOCK TABLE tenant_a.kegs_q;" >&7
wait_for "[ \"\$(granted tenant_a.kegs_q)\" = 1 ]"
sleep 1.5
chk "while a tenant holds LOCK TABLE on its queue table open, another topic is stamped and synced within 2 s" yes \
  "$(synced_on_time 102)"
echo "ROLLBACK;" >&7
exec 7>&-
chk "the tenant topic syncs after the tenant releases its locks" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT n FROM tenant_a.kegs WHERE keg_id = 1\")\" = 1 ]" && echo yes || echo no)"

chk "the tenant record did not go to the error table while it waited for the lock" 0 \
  "$(psql_as postgres "SELECT count(*) FROM tenant_a.kegs_qe")"

mkfifo "$WORK/row.fifo"
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -f "$WORK/row.fifo" >"$WORK/row.out" 2>&1 &
exec 8>"$WORK/row.fifo"
echo "BEGIN; SELECT 1 FROM public.bottles WHERE bottle_id = 5 FOR UPDATE;" >&8
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE state = 'idle in transaction' AND usename = 'postgres'\")\" = 1 ]"
psql_as postgres "SELECT topic.publish('public.bottles_q', '{\"bottle_id\": 5, \"n\": -5}')" >/dev/null
chk "the sync worker logs the lock timeout on the held row" yes \
  "$(wait_for "grep -q 'pg_topics sync worker: canceling statement due to lock timeout. The sync worker tries again in 1 s.' '$PGDATA/log'" && echo yes || echo no)"
sleep 1.5
chk "a record that waits for a row lock does not go to the error table" 0 \
  "$(psql_as postgres "SELECT count(*) FROM public.bottles_qe")"
echo "COMMIT;" >&8
exec 8>&-
chk "the record lands after the row lock is released" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT n FROM public.bottles WHERE bottle_id = 5\")\" = -5 ]" && echo yes || echo no)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
