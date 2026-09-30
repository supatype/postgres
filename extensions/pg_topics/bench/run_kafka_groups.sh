#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster
trap 'docker rm -f $(docker ps -aq --filter "name=pgt_${PORT}_") >/dev/null 2>&1 || true; cleanup' EXIT

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
for t in six_q:6 pause_q:6 coop_q:6 java_q:1 auto_q:3 mix_q:6 slow_q:6 flood_q:1 list_a_q:1 list_b_q:1; do
  psql_as alice "SELECT topic.create_topic('alice.${t%:*}', ${t#*:})" >/dev/null
done
psql_as alice "SELECT topic.publish('alice.java_q', jsonb_build_object('j', i)) FROM generate_series(1, 10) i;
  SELECT topic.publish('alice.auto_q', jsonb_build_object('a', i), 'k' || i) FROM generate_series(1, 30) i;
  SELECT topic.publish('alice.list_a_q', '{}'), topic.publish('alice.list_b_q', '{}') FROM generate_series(1, 3)" >/dev/null
wait_for "[ \"\$(unstamped alice.java_q)\" = 0 ] && [ \"\$(unstamped alice.auto_q)\" = 0 ] && [ \"\$(unstamped alice.list_a_q)\" = 0 ] && [ \"\$(unstamped alice.list_b_q)\" = 0 ]"
start_listener

member() {
  local name=$1 topic=$2 group=$3 cfg=${4:-}
  KAFKA_NAME="pgt_${PORT}_$name" kafka_py alice alice-pw member "$topic" "$group" "$name" "$cfg" >"$WORK/$name.out" &
}

assigned() {
  { grep '^assigned' "$WORK/$1.out" || true; } | tail -n1 | cut -d' ' -f2
}

stop() {
  touch "$WORK/stop-$1"
  wait_for "grep -q '^closed' '$WORK/$1.out'"
}

within() {
  local seconds=$1 end=$((SECONDS + $1))
  while [ "$SECONDS" -lt "$end" ]; do
    eval "$2" && return 0
    sleep 0.2
  done
  return 1
}

group_row() {
  psql_as postgres "SELECT concat_ws('|', state, generation_id, protocol_name, expired_members)
                    FROM topic.topic_groups WHERE group_name = '$1'"
}

bands() {
  echo "$@" | tr ', ' '\n\n' | grep -v '^$' | sort -n | paste -sd,
}

member a alice.six_q three
member b alice.six_q three
member c alice.six_q three
wait_for "[ \"\$(bands \$(assigned a) \$(assigned b) \$(assigned c))\" = 0,1,2,3,4,5 ] && [ \"\$(group_row three | cut -d'|' -f1)\" = Stable ]"
chk "three librdkafka consumers on a 6-band topic each get 2 bands" "2 2 2" \
  "$(for m in a b c; do assigned $m | tr ',' '\n' | grep -c .; done | paste -sd' ')"
before=$(group_row three | cut -d'|' -f2)
started=$(date +%s%N)
touch "$WORK/stop-c"
within 15 "[ \"\$(assigned a | tr ',' '\n' | grep -c .) \$(assigned b | tr ',' '\n' | grep -c .)\" = '3 3' ] && [ \"\$(group_row three | cut -d'|' -f1)\" = Stable ]" || true
took=$((($(date +%s%N) - started) / 1000000))
echo "the rebalance after the close took $took ms"
chk "the closed consumer sent LeaveGroup and the other two get 3 bands each in less than 4 s (session.timeout.ms is 6 s)" yes \
  "$([ "$took" -lt 4000 ] && echo yes || echo no)"
stop c
chk "the two consumers cover every band" 0,1,2,3,4,5 "$(bands "$(assigned a)" "$(assigned b)")"
chk "one rebalance moved the bands of the closed consumer" "$((before + 1))" "$(group_row three | cut -d'|' -f2)"
stop a
stop b

member p alice.pause_q pause
member q alice.pause_q pause
wait_for "[ \"\$(bands \$(assigned p) \$(assigned q))\" = 0,1,2,3,4,5 ] && [ \"\$(group_row pause | cut -d'|' -f1)\" = Stable ]"
docker pause "pgt_${PORT}_q" >/dev/null
chk "a consumer paused longer than session.timeout.ms (6 s) is expired and the other gets all 6 bands" yes \
  "$(within 30 "[ \"\$(assigned p)\" = 0,1,2,3,4,5 ] && [ \"\$(group_row pause | cut -d'|' -f1)\" = Stable ]" && echo yes || echo no)"
chk "the group counts one expired member" 1 "$(group_row pause | cut -d'|' -f4)"
docker unpause "pgt_${PORT}_q" >/dev/null
stop p
stop q

member r alice.coop_q coop partition.assignment.strategy=range
member s alice.coop_q coop partition.assignment.strategy=range
wait_for "[ \"\$(bands \$(assigned r) \$(assigned s))\" = 0,1,2,3,4,5 ] && [ \"\$(group_row coop | cut -d'|' -f1)\" = Stable ]"
chk "the range assignor reaches Stable" "Stable|range" "$(group_row coop | cut -d'|' -f1,3)"
stop r
stop s
member x alice.coop_q sticky partition.assignment.strategy=cooperative-sticky
member y alice.coop_q sticky partition.assignment.strategy=cooperative-sticky
wait_for "[ \"\$(bands \$(assigned x) \$(assigned y))\" = 0,1,2,3,4,5 ] && [ \"\$(group_row sticky | cut -d'|' -f1)\" = Stable ]"
chk "the cooperative-sticky assignor reaches Stable and splits 6 bands 3 and 3" "Stable|cooperative-sticky 3 3" \
  "$(group_row sticky | cut -d'|' -f1,3) $(for m in x y; do assigned $m | tr ',' '\n' | grep -c .; done | paste -sd' ')"
stop x
stop y

member auto alice.auto_q auto enable.auto.commit=true,enable.auto.offset.store=true,auto.commit.interval.ms=500
wait_for "[ \"\$(assigned auto)\" = 0,1,2 ]"
chk "enable.auto.commit=true commits next_offset on every band" t \
  "$(within 30 "[ \"\$(psql_as postgres \"SELECT count(*) = 3 AND bool_and(o.committed_offset = p.next_offset)
       FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
       WHERE o.group_name = 'auto'\")\" = t ]" && echo t || echo f)"
stop auto
chk "the auto-commit consumer read all 30 records" "closed read 30" "$(grep '^closed' "$WORK/auto.out")"

java_config alice alice-pw
java_group() {
  kafka_java kafka-console-consumer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.java_q --group jg \
    --consumer.config /w/alice.properties --consumer-property max.poll.records=1 \
    --consumer-property auto.offset.reset=earliest --max-messages "$1" --timeout-ms 30000
}
chk "Java kafka-console-consumer --group reads the first 4 records" '{"j": 1} {"j": 2} {"j": 3} {"j": 4}' \
  "$(java_group 4 | grep '^{' | paste -sd' ')"
chk "the Java consumer commits offset 4" 4 \
  "$(psql_as postgres "SELECT committed_offset FROM topic.topic_offsets WHERE group_name = 'jg'")"
chk "on restart the Java consumer continues from the committed offset" '{"j": 5} {"j": 6} {"j": 7}' \
  "$(java_group 3 | grep '^{' | paste -sd' ')"

sql_member() {
  local group=$1 client=$2 session=$3 rebalance=$4 name=$5 mode=$6 out="$WORK/$5.out" id r e gen
  local args="'$client', $session, $rebalance, 'consumer', '[{\"name\": \"range\"}]'"
  id=$(psql_as alice "SELECT member_id FROM topic.group_join('$group', '', $args)")
  while [ ! -e "$WORK/stop-$name" ]; do
    r=$(psql_as alice "SELECT concat_ws('|', coalesce(error, 'WAIT'), generation_id, leader_id, members)
                       FROM topic.group_join('$group', '$id', $args)")
    while [ "${r%%|*}" = WAIT ]; do
      sleep 0.1
      r=$(psql_as alice "SELECT concat_ws('|', coalesce(error, 'WAIT'), generation_id, leader_id, members)
                         FROM topic.group_join_poll('$group', '$id')")
    done
    echo "joined $r" >>"$out"
    [ "${r%%|*}" = NONE ] || { sleep 0.5; continue; }
    gen=$(cut -d'|' -f2 <<<"$r")
    if [ "$(cut -d'|' -f3 <<<"$r")" = "$id" ]; then
      r=$(psql_as alice "SELECT coalesce(error, 'WAIT') FROM topic.group_sync('$group', '$id', $gen,
            (SELECT jsonb_object_agg(m->>'member_id', '[0]'::jsonb) FROM jsonb_array_elements('$(cut -d'|' -f4 <<<"$r")') m))")
    else
      r=$(psql_as alice "SELECT coalesce(error, 'WAIT') FROM topic.group_sync('$group', '$id', $gen, '{}')")
    fi
    while [ "$r" = WAIT ]; do
      sleep 0.1
      r=$(psql_as alice "SELECT coalesce(error, 'WAIT') FROM topic.group_sync_poll('$group', '$id', $gen)")
    done
    echo "synced $gen $r" >>"$out"
    [ "$mode" = once ] && { wait_for "[ -e '$WORK/stop-$name' ]" || true; break; }
    e=NONE
    while [ "$e" = NONE ] && [ ! -e "$WORK/stop-$name" ]; do
      sleep 1
      e=$(psql_as alice "SELECT topic.group_heartbeat('$group', '$id', $gen)")
      echo "heartbeat $e" >>"$out"
    done
  done
  echo closed >>"$out"
}

log_lines() {
  wc -l <"$PGDATA/log"
}

closed_since() {
  tail -n +"$1" "$PGDATA/log" | grep 'a client connection closed' | grep -vc '(os error [0-9]*)$' || true
}

members_of() {
  psql_as postgres "SELECT count(*) FROM topic.topic_group_members WHERE group_name = '$1'"
}

member ka alice.mix_q mixa
wait_for "[ \"\$(assigned ka)\" = 0,1,2,3,4,5 ] && [ \"\$(group_row mixa | cut -d'|' -f1)\" = Stable ]"
mark=$(log_lines)
sql_member mixa zz 10000 5000 sa follow &
chk "a librdkafka leader and a SQL member with no metadata in one group both reach Stable" yes \
  "$(within 30 "[ \"\$(group_row mixa | cut -d'|' -f1)\" = Stable ] && [ \"\$(members_of mixa)\" = 2 ] && grep -q '^synced .* NONE' '$WORK/sa.out' && [ \"\$(tail -n1 '$WORK/sa.out')\" = 'heartbeat NONE' ]" && echo yes || echo no)"
chk "the listener kept the librdkafka leader session, and the leader did not crash" "0 0" "$(closed_since "$mark") $(grep -c 'Assertion\|Traceback' "$WORK/ka.out" || true)"
stop ka
touch "$WORK/stop-sa"

mark=$(log_lines)
sql_member mixb aa 10000 5000 sb json &
wait_for "grep -q '^synced' '$WORK/sb.out'"
member kb alice.mix_q mixb
within 30 "[ \"\$(group_row mixb | cut -d'|' -f1)\" = Stable ] && [ \"\$(members_of mixb)\" = 2 ]" || true
settled=$(group_row mixb | cut -d'|' -f1,2)
sleep 4
chk "a SQL leader that writes JSON assignments and a librdkafka follower stay Stable in one generation" \
  "Stable|2 Stable|2" "$settled $(group_row mixb | cut -d'|' -f1,2)"
chk "the listener kept the librdkafka follower session, and the follower did not crash" "0 0" "$(closed_since "$mark") $(grep -c 'Assertion\|Traceback' "$WORK/kb.out" || true)"
chk "the librdkafka follower got no UNKNOWN_SERVER_ERROR on SyncGroup" 0 \
  "$(tail -n +"$mark" "$PGDATA/log" | grep -c 'pg_topics listener: group mixb:' || true)"
chk "the listener logs a WARNING for the assignment that is not a base64 string" yes \
  "$(tail -n +"$mark" "$PGDATA/log" | grep -q 'WARNING: group mixb: the assignment of member .* is not a base64 string' && echo yes || echo no)"
stop kb
touch "$WORK/stop-sb"

sql_member slow aa 30000 8000 sl once &
wait_for "grep -q '^synced' '$WORK/sl.out'"
member ks alice.slow_q slow max.poll.interval.ms=6000,debug=cgrp
chk "a held JoinGroup waits for the largest rebalance timeout in the group, not its own" yes \
  "$(within 30 "[ \"\$(assigned ks)\" = 0,1,2,3,4,5 ] && [ \"\$(group_row slow | cut -d'|' -f1)\" = Stable ]" && echo yes || echo no)"
chk "the member with the shorter rebalance timeout got no REBALANCE_IN_PROGRESS on JoinGroup" 0 \
  "$(grep -ci 'JoinGroup.*rebalance in progress' "$WORK/ks.out" || true)"
stop ks
touch "$WORK/stop-sl"

psql_as postgres "CREATE ROLE flood LOGIN PASSWORD 'flood-pw'; GRANT USAGE ON SCHEMA alice TO flood;
  ALTER ROLE flood SET log_statement = 'all'" >/dev/null
psql_as alice "GRANT SELECT ON alice.flood_q TO flood" >/dev/null
psql_as postgres "DO \$\$ BEGIN FOR i IN 1..3000 LOOP
    PERFORM pg_notify('pg_topics_group', 'flood'); COMMIT; PERFORM pg_sleep(0.002); END LOOP; END \$\$" >/dev/null &
flooder=$!
KAFKA_NAME="pgt_${PORT}_fl" kafka_py flood flood-pw member alice.flood_q flood fl "" >"$WORK/fl.out" &
wait_for "[ \"\$(assigned fl)\" = 0 ] && [ \"\$(group_row flood | cut -d'|' -f1)\" = Stable ]"
wait "$flooder"
polls=$(grep -c 'topic.group_join_poll(' "$PGDATA/log" || true)
echo "a held JoinGroup polled $polls times under a NOTIFY flood"
chk "a NOTIFY flood on pg_topics_group does not make a held JoinGroup poll more than once per 100 ms" yes \
  "$([ "$polls" -lt 60 ] && echo yes || echo no)"
stop fl

psql_as postgres "CREATE ROLE lister LOGIN PASSWORD 'lister-pw'; GRANT USAGE ON SCHEMA alice TO lister" >/dev/null
psql_as alice "GRANT SELECT ON alice.list_a_q, alice.list_b_q TO lister" >/dev/null
chk "a generation -1 commit creates the missing group" "NONE|NONE" \
  "$(psql_as lister "SELECT topic.commit_offset('alice.list_a_q', 'listed', 0, 2, -1) || '|' ||
                            topic.commit_offset('alice.list_b_q', 'listed', 0, 2, -1)")"
psql_as alice "REVOKE SELECT ON alice.list_b_q FROM lister" >/dev/null
chk "OffsetFetch with no topic list skips a topic the caller can no longer read" "offset alice.list_a_q 0 2 NONE" \
  "$(kafka_py lister lister-pw group_offsets listed | grep '^offset' || true)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
