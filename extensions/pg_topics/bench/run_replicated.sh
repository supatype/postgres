#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster
P1=$PORT
S1=$(free_port)
S2=$(free_port)
trap 'for s in s1 s2; do PGDATA=$WORK/$s stop_pg >/dev/null 2>&1 || true; done; cleanup' EXIT

on() {
  local port=$1
  shift
  PORT=$port psql_as "$@"
}

on_standbys() {
  echo "$(on "$S1" postgres "$1") $(on "$S2" postgres "$1")"
}

sync_standbys() {
  on "$P1" postgres "SELECT string_agg(application_name || ':' || sync_state, ' ' ORDER BY application_name)
                     FROM pg_stat_replication"
}

set_standbys() {
  on "$P1" postgres "ALTER SYSTEM SET synchronous_standby_names = '$1'" >/dev/null
  on "$P1" postgres "SELECT pg_reload_conf()" >/dev/null
  wait_for "[ \"\$(sync_standbys)\" = '$2' ]" || true
  chk "pg_stat_replication shows $2 under $1" "$2" "$(sync_standbys)"
}

stampers_on_syncrep() {
  on "$P1" postgres "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep' AND backend_type LIKE 'pg_topics %'"
}

rf() {
  kafka_py alice alice-pw describe_config_sources "$1" | awk '$2 == "pg_topics.replication_factor" {print $3}'
}

on "$P1" postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
start_listener
for s in s1 s2; do
  "$PGBIN/pg_basebackup" -h /tmp -p "$P1" -U postgres -D "$WORK/$s" -R -X stream -c fast
  echo "cluster_name = '$s'" >>"$WORK/$s/postgresql.conf"
done
PGDATA=$WORK/s1 PORT=$S1 start_pg >/dev/null
PGDATA=$WORK/s2 PORT=$S2 start_pg >/dev/null

out=$(kafka_py alice alice-pw create_topic alice.none_q 1 2)
echo "RED: replication_factor 2 with no synchronous standby -- $out"
chk "replication_factor 2 is refused while synchronous_standby_names is empty" yes \
  "$(grep -q '^create_topic INVALID_REPLICATION_FACTOR .*keeps 1 copies' <<<"$out" && echo yes || echo no)"

set_standbys 'FIRST 2 (s1, s2)' 's1:sync s2:sync'
out=$(kafka_py alice alice-pw create_topic alice.rf3_q 2 3)
chk "replication_factor 3 is accepted under FIRST 2 (s1, s2)" "create_topic NONE" "$out"
chk "the replication_factor 3 topic uses the replicated tier" replicated \
  "$(on "$P1" postgres "SELECT min_durability FROM topic.topic_config WHERE topic = 'rf3_q'")"
chk "DescribeConfigs reports pg_topics.replication_factor 3" 3 "$(rf alice.rf3_q)"

out=$(kafka_py alice alice-pw produce alice.rf3_q 50)
chk "an acks=all produce of 50 records gets 50 acks" "50 left 0" \
  "$(grep -c '^ok' <<<"$out" || true) $(grep '^left' <<<"$out")"
chk "each acknowledged record is on both standbys when the ack returns" "50 50" \
  "$(on_standbys "SELECT count(*) FROM alice.rf3_q")"

out=$(kafka_py alice alice-pw produce alice.rf3_q 10 json none "" -1 0 1)
chk "an acks=1 produce of 10 records gets 10 acks" "10 left 0" \
  "$(grep -c '^ok' <<<"$out" || true) $(grep '^left' <<<"$out")"
chk "the topic floor holds for acks=1 too: all 60 records are on both standbys" "60 60" \
  "$(on_standbys "SELECT count(*) FROM alice.rf3_q")"

on "$P1" alice "SELECT topic.publish('alice.rf3_q', jsonb_build_object('sql', i)) FROM generate_series(1, 5) i" >/dev/null
chk "a SQL publish that returned is on both standbys" "65 65" "$(on_standbys "SELECT count(*) FROM alice.rf3_q")"

on "$S2" postgres "SELECT pg_wal_replay_pause()" >/dev/null
out=$(kafka_py alice alice-pw produce alice.rf3_q 1 json none "" -1 0 1)
echo "RED: an acks=1 produce while s2 pauses replay -- $(tr '\n' ' ' <<<"$out")"
chk "no acks=1 ack returns while s2 has not applied the record" 0 "$(grep -c '^ok' <<<"$out" || true)"
sql_exit=0
timeout 5 "$PGBIN/psql" -h /tmp -p "$P1" -U alice -d postgres -tAq -c "SET statement_timeout = '3s'" \
  -c "SELECT topic.publish('alice.rf3_q', '{\"sql\": \"paused\"}')" >/dev/null 2>&1 || sql_exit=$?
chk "a SQL publish does not return while s2 pauses replay" 124 "$sql_exit"
chk "both publishers wait on SyncRep while s2 pauses replay" 2 \
  "$(on "$P1" postgres "SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'SyncRep' AND backend_type = 'client backend'")"
on "$S2" postgres "SELECT pg_wal_replay_resume()" >/dev/null
wait_for "[ \"\$(on_standbys 'SELECT count(*) FROM alice.rf3_q')\" = '67 67' ]" || true
chk "the two waiting records commit on both standbys after s2 resumes replay" "67 67" \
  "$(on_standbys "SELECT count(*) FROM alice.rf3_q")"

on "$P1" postgres "SELECT topic.create_topic('public.relaxed_q', 1, min_durability => 'relaxed')" >/dev/null
hold_stamp_lock alice.rf3_q
on "$P1" alice "SELECT topic.publish('alice.rf3_q', jsonb_build_object('held', i)) FROM generate_series(1, 3) i" >/dev/null
on "$S2" postgres "SELECT pg_wal_replay_pause()" >/dev/null
release_stamp_lock
wait_for "[ \"\$(stampers_on_syncrep)\" = 1 ]" || true
chk "a stamper waits on SyncRep for the replicated topic while s2 pauses replay" 1 "$(stampers_on_syncrep)"
on "$P1" postgres "SELECT topic.publish('public.relaxed_q', jsonb_build_object('i', i)) FROM generate_series(1, 5) i" >/dev/null
wait_for "[ \"\$(unstamped public.relaxed_q)\" = 0 ]" || true
echo "RED: a relaxed topic while the replicated stamper waits on s2 -- $(unstamped public.relaxed_q) unstamped"
chk "a relaxed topic is still stamped while the replicated topic waits on s2" 0 "$(unstamped public.relaxed_q)"
on "$S2" postgres "SELECT pg_wal_replay_resume()" >/dev/null
wait_for "[ \"\$(unstamped alice.rf3_q)\" = 0 ]" || true
chk "the replicated topic is stamped after s2 resumes replay" 0 "$(unstamped alice.rf3_q)"

set_standbys 'ANY 1 (s1, s2)' 's1:quorum s2:quorum'
out=$(kafka_py alice alice-pw create_topic alice.rf3b_q 1 3)
echo "RED: replication_factor 3 under ANY 1 (s1, s2) -- $out"
chk "replication_factor 3 is refused under ANY 1 (s1, s2), with the copies it backs" yes \
  "$(grep -q '^create_topic INVALID_REPLICATION_FACTOR .*keeps 2 copies' <<<"$out" && echo yes || echo no)"
chk "the refused topic made no control row" 0 \
  "$(on "$P1" postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'rf3b_q'")"
chk "replication_factor 2 is accepted under ANY 1 (s1, s2)" "create_topic NONE" \
  "$(kafka_py alice alice-pw create_topic alice.rf2_q 1 2)"
chk "DescribeConfigs reports the copies the standbys back now, 2, for the replication_factor 3 topic" 2 \
  "$(rf alice.rf3_q)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
