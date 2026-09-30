#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"

: "${PG_CONFIG:?set PG_CONFIG to the pg_config of the PostgreSQL 17 copy}"
PGBIN=$("$PG_CONFIG" --bindir)
PORT=$(free_port)
WORK=$(mktemp -d)
PGDATA="$WORK/data"

cleanup() {
  stop_pg >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

(cd "$HERE/../extension" && cargo pgrx install --pg-config "$PG_CONFIG" >/dev/null)

"$PGBIN/initdb" -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256 >/dev/null
cat >>"$PGDATA/postgresql.conf" <<EOF
shared_preload_libraries = 'pg_topics'
pg_topics.databases = ''
pg_topics.failover_is_fenced = on
EOF
start_pg >/dev/null

psql_as postgres "CREATE EXTENSION pg_topics" >/dev/null

sed -i "s/pg_topics.databases = ''/pg_topics.databases = 'some_other_db'/" "$PGDATA/postgresql.conf"
stop_pg >/dev/null
start_pg >/dev/null
out=$(psql_as postgres "SELECT topic.create_topic('public.orders_q', 1)" || true)
echo "RED: create_topic when pg_topics.databases names a different database -- $out"
chk "create_topic refuses a database that pg_topics.databases does not name" yes \
  "$(grep -q 'is not in pg_topics.databases' <<<"$out" && echo yes || echo no)"
chk "create_topic left no topic behind" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'orders_q'")"

sed -i "s/pg_topics.databases = 'some_other_db'/pg_topics.databases = ''/" "$PGDATA/postgresql.conf"
stop_pg >/dev/null
start_pg >/dev/null
psql_as postgres "SELECT topic.create_topic('public.orders_q', 1)" >/dev/null

stamp() {
  psql_as postgres "SELECT topic.stamp_topic('public', 'orders_q', 1)"
}

offset_of() {
  psql_as postgres "SELECT log_offset FROM public.orders_q WHERE value->>'s' = '$1'"
}

backlog_is_zero() {
  psql_as postgres "SELECT backlog_age = interval '0' FROM topic.topic_config WHERE topic = 'orders_q'"
}

psql_as postgres "SELECT topic.publish('public.orders_q', '{\"s\": \"A\"}')" >/dev/null
psql_as postgres "SELECT topic.publish('public.orders_q', '{\"s\": \"B\"}')" >/dev/null
chk "first stamp takes one row" 1 "$(stamp)"
chk "backlog_age shows the unstamped B" f "$(backlog_is_zero)"
chk "second stamp takes one row" 1 "$(stamp)"
chk "backlog_age is 0 with no unstamped row" t "$(backlog_is_zero)"
chk "A committed first and has offset 0" 0 "$(offset_of A)"
chk "B committed second and has offset 1" 1 "$(offset_of B)"

"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q >/dev/null 2>&1 <<'EOF' &
BEGIN;
SELECT topic.publish('public.orders_q', '{"s": "C"}');
SELECT pg_sleep(4);
COMMIT;
EOF
open_pid=$!
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"

psql_as postgres "SELECT topic.publish('public.orders_q', '{\"s\": \"D\"}')" >/dev/null
chk "stamp skips the open insert and takes D" 1 "$(stamp)"
chk "D has the next offset 2" 2 "$(offset_of D)"
chk "stamp finds nothing while C is open" 0 "$(stamp)"
chk "C has no offset while it is open" "" "$(offset_of C)"

wait "$open_pid"
chk "stamp takes C after its commit" 1 "$(stamp)"
chk "C has the next offset 3" 3 "$(offset_of C)"
chk "offsets have no gap" "0,1,2,3" \
  "$(psql_as postgres "SELECT string_agg(log_offset::text, ',' ORDER BY log_offset) FROM public.orders_q")"
chk "next_offset follows the last offset" 4 \
  "$(psql_as postgres "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'orders_q'")"

"$PGBIN/pg_dump" -h /tmp -p "$PORT" -U postgres -d postgres -f "$WORK/dump.sql"
psql_as postgres "CREATE DATABASE restored" >/dev/null
restore_status=0
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d restored -v ON_ERROR_STOP=1 -q -f "$WORK/dump.sql" \
  >"$WORK/restore.log" 2>&1 || restore_status=$?
chk "restore runs with no error" 0 "$restore_status"

psql_restored() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d restored -tAc "$1" 2>&1
}

chk "restore keeps the topic_config row" 1 \
  "$(psql_restored "SELECT count(*) FROM topic.topic_config WHERE topic = 'orders_q'")"
chk "restore keeps the stamped rows" 4 \
  "$(psql_restored "SELECT count(log_offset) FROM public.orders_q")"
chk "restore keeps next_offset" 4 \
  "$(psql_restored "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'orders_q'")"
psql_restored "SELECT topic.publish('public.orders_q', '{\"s\": \"E\"}')" >/dev/null
chk "stamp after restore takes E" 1 "$(psql_restored "SELECT topic.stamp_topic('public', 'orders_q')")"
chk "E has the next offset 4" 4 \
  "$(psql_restored "SELECT log_offset FROM public.orders_q WHERE value->>'s' = 'E'")"

ranges() {
  psql_as postgres "SELECT string_agg(t || ':' || lo || '-' || hi, ',' ORDER BY lo)
    FROM (SELECT value->>'t' AS t, min(log_offset) AS lo, max(log_offset) AS hi FROM $1 GROUP BY 1) r"
}

psql_as postgres "SELECT topic.create_topic('public.pair_q', 1)" >/dev/null
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q >/dev/null 2>&1 <<'EOF' &
BEGIN;
SELECT topic.publish('public.pair_q', '{"t": "A"}');
SELECT pg_sleep(2);
CREATE TEMP TABLE seen AS SELECT count(*) AS b FROM public.pair_q WHERE value->>'t' = 'B';
SELECT topic.publish('public.pair_q', jsonb_build_object('t', 'A', 'saw_b', (SELECT b FROM seen)));
COMMIT;
EOF
open_pid=$!
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"
psql_as postgres "BEGIN; SELECT topic.publish('public.pair_q', '{\"t\": \"B\"}') FROM generate_series(1, 2); COMMIT" >/dev/null
wait "$open_pid"
chk "two transactions interleave their rows in one band" "A,B,B,A" \
  "$(psql_as postgres "SELECT string_agg(value->>'t', ',' ORDER BY seq) FROM public.pair_q")"
chk "stamp takes all 4 rows" 4 "$(psql_as postgres "SELECT topic.stamp_topic('public', 'pair_q')")"
chk "each transaction gets one contiguous offset range" "A:0-1,B:2-3" "$(ranges public.pair_q)"
chk "offsets follow the first write of each transaction: A read B's 2 committed rows, then wrote offset 1, below B" "2:1" \
  "$(psql_as postgres "SELECT (value->>'saw_b') || ':' || log_offset FROM public.pair_q WHERE value ? 'saw_b'")"

psql_as postgres "SELECT topic.create_topic('public.cut_q', 1)" >/dev/null
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -c "BEGIN" -c "SELECT pg_current_xact_id()" \
  -c "SELECT pg_sleep(2)" -c "COMMIT" >/dev/null 2>&1 &
open_pid=$!
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"
psql_as postgres "SELECT topic.publish('public.cut_q', '{\"t\": \"G\"}') FROM generate_series(1, 3)" >/dev/null
chk "while an older transaction is open, stamp with max_rows = 1 takes the whole 3-row transaction" 3 \
  "$(psql_as postgres "SELECT topic.stamp_topic('public', 'cut_q', 1)")"
wait "$open_pid"
psql_as postgres "SELECT topic.publish('public.cut_q', '{\"t\": \"H\"}') FROM generate_series(1, 3)" >/dev/null
chk "with no older transaction open, stamp with max_rows = 1 takes 1 row" 1 \
  "$(psql_as postgres "SELECT topic.stamp_topic('public', 'cut_q', 1)")"
psql_as postgres "SELECT topic.publish('public.cut_q', '{\"t\": \"I\"}')" >/dev/null
chk "the cut transaction still stamps first, before a newer one" "1 1 1" \
  "$(for i in 1 2 3; do psql_as postgres "SELECT topic.stamp_topic('public', 'cut_q', 1)"; done | tr '\n' ' ' | sed 's/ $//')"
chk "each transaction of the cut topic gets one contiguous offset range" "G:0-2,H:3-5,I:6-6" "$(ranges public.cut_q)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
