#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

for t in a b c; do
  psql_as postgres "SELECT topic.create_topic('public.${t}_q', 2)" >/dev/null
done

stamper_pid() {
  psql_as postgres "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_topics stamper'"
}

all_unstamped() {
  psql_as postgres "SELECT (SELECT count(*) FROM public.a_q WHERE log_offset IS NULL)
                         + (SELECT count(*) FROM public.b_q WHERE log_offset IS NULL)
                         + (SELECT count(*) FROM public.c_q WHERE log_offset IS NULL)"
}

stamped_within_ms() {
  local start
  start=$(date +%s%N)
  until [ "$(all_unstamped)" = 0 ]; do
    [ $((($(date +%s%N) - start) / 1000000)) -gt "$1" ] && echo no && return
    sleep 0.01
  done
  [ $((($(date +%s%N) - start) / 1000000)) -le "$1" ] && echo yes || echo no
}

publish_1000() {
  psql_as postgres "SELECT topic.publish('public.' || (ARRAY['a', 'b', 'c'])[1 + i % 3] || '_q',
                                         jsonb_build_object('i', i))
                    FROM generate_series(1, 1000) i" >/dev/null
}

"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -tA \
  -c "LISTEN pg_topics_stamped" -c "SELECT pg_sleep(3)" >"$WORK/listen.out" 2>&1 &
listen_pid=$!
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"

publish_1000
chk "the stamper gives 1000 rows on 3 topics an offset within 2 s" yes "$(stamped_within_ms 2000)"
chk "the offsets have no gap and no duplicate" t "$(gap_free a_q b_q c_q)"
chk "the statistics show the stamp updates, so autovacuum sees them" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT coalesce(sum(n_tup_upd), 0) > 0 FROM pg_stat_user_tables WHERE relname LIKE 'a\\_q\\_p%'\")\" = t ]" && echo yes || echo no)"
xid_before=$(psql_as postgres "SELECT txid_current()")
sleep 3
xids=$(($(psql_as postgres "SELECT txid_current()") - xid_before))
echo "transaction ids used in 3 s by 3 idle topics: $xids"
chk "3 idle topics use fewer than 20 transaction ids in 3 s" yes "$([ "$xids" -lt 20 ] && echo yes || echo no)"
wait "$listen_pid"
chk "a LISTEN session gets pg_topics_stamped" yes \
  "$(grep -q 'notification "pg_topics_stamped" with payload "public\.[abc]_q"' "$WORK/listen.out" && echo yes || echo no)"

old=$(stamper_pid)
kill -9 "$old"
wait_for "p=\$(stamper_pid); [[ \"\$p\" =~ ^[0-9]+\$ ]] && [ \"\$p\" != $old ]"
new=$(stamper_pid)
chk "the stamper restarts after kill -9" yes "$([[ "$new" =~ ^[0-9]+$ ]] && [ "$new" != "$old" ] && echo yes || echo no)"
publish_1000
chk "the new stamper gives 1000 more rows an offset within 2 s" yes "$(stamped_within_ms 2000)"
chk "the offsets still have no gap and no duplicate" t "$(gap_free a_q b_q c_q)"

hold_stamp_lock public.a_q
psql_as postgres "SELECT topic.publish('public.a_q', '{}')" >/dev/null
sleep 0.5
chk "the stamper skips a topic while another session holds its lock" 1 "$(unstamped public.a_q)"
release_stamp_lock
chk "the stamper takes the topic after the lock is free" yes "$(stamped_within_ms 2000)"

psql_as postgres "UPDATE topic.topic_config SET max_backlog_age = '2 seconds' WHERE topic = 'a_q'" >/dev/null
hold_stamp_lock public.a_q
sleep 3
refused=$(psql_as postgres "SELECT topic.publish('public.a_q', '{}')" || true)
chk "publish is refused while the stamper cannot take the topic" yes \
  "$(grep -q 'the stamper has not run on public.a_q' <<<"$refused" && echo yes || echo no)"
release_stamp_lock
chk "publish works again after the lock is free" yes \
  "$(wait_for "psql_as postgres \"SELECT topic.publish('public.a_q', '{}')\" >/dev/null" && echo yes || echo no)"

psql_as postgres "SELECT topic.create_topic('public.bad_q', 1);
  CREATE FUNCTION public.refuse() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN RAISE EXCEPTION 'refused by the harness'; END \$\$;
  CREATE TRIGGER refuse BEFORE UPDATE ON public.bad_q FOR EACH ROW EXECUTE FUNCTION public.refuse();
  SELECT topic.publish('public.bad_q', '{}');" >/dev/null
chk "the stamper logs the error of a topic" yes \
  "$(wait_for "grep -q 'pg_topics stamper: refused by the harness. The stamper tries again in 1 s.' '$PGDATA/log'" && echo yes || echo no)"
latencies=""
for i in 1 2 3 4 5; do
  psql_as postgres "SELECT topic.publish('public.a_q', '{}')" >/dev/null
  latencies="$latencies$(stamped_within_ms 300) "
done
chk "while one topic fails, the other topics are stamped within 300 ms" "yes yes yes yes yes " "$latencies"
chk "the stamper releases the lock of the failed topic" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT EXISTS (SELECT FROM topic.topic_band_position WHERE schema_name = 'public' AND topic = 'bad_q' AND band = 0 FOR NO KEY UPDATE SKIP LOCKED)\")\" = t ]" && echo yes || echo no)"
psql_as postgres "DROP TRIGGER refuse ON public.bad_q" >/dev/null
wait_for "[ \"\$(unstamped public.bad_q)\" = 0 ]"
chk "the failed topic is stamped after the fault is gone" 0 "$(unstamped public.bad_q)"
chk "every offset has no gap and no duplicate" t "$(gap_free a_q b_q c_q bad_q)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
