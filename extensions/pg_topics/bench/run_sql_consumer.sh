#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

PROTOCOLS='[{"name": "range", "metadata": "m"}]'

psql_as postgres "CREATE ROLE consumer LOGIN; CREATE SCHEMA consumer AUTHORIZATION consumer" >/dev/null
psql_as consumer "SELECT topic.create_topic('consumer.orders_q', 4);
                  SELECT topic.publish('consumer.orders_q', jsonb_build_object('i', i), 'k' || i)
                  FROM generate_series(1, 40) i" >/dev/null
wait_for "[ \"\$(unstamped consumer.orders_q)\" = 0 ]"

join() {
  psql_as consumer "SELECT concat_ws('|', coalesce(error, 'WAIT'), member_id, generation_id, leader_id)
                    FROM topic.group_join('orders', '$1', 'sql', 10000, 2000, 'consumer', '$PROTOCOLS')"
}

poll_join() {
  local r
  while r=$(psql_as consumer "SELECT concat_ws('|', coalesce(error, 'WAIT'), member_id, generation_id, leader_id)
                              FROM topic.group_join_poll('orders', '$1')"); [ "${r%%|*}" = WAIT ]; do
    sleep 0.1
  done
  echo "$r"
}

sync() {
  psql_as consumer "SELECT concat_ws('|', coalesce(error, 'WAIT'), assignment)
                    FROM topic.group_sync('orders', '$1', $2, '$3')"
}

poll_sync() {
  local r
  while r=$(psql_as consumer "SELECT concat_ws('|', coalesce(error, 'WAIT'), assignment)
                              FROM topic.group_sync_poll('orders', '$1', $2)"); [ "${r%%|*}" = WAIT ]; do
    sleep 0.1
  done
  echo "$r"
}

consume() {
  local member=$1 generation=$2 bands=$3 band from rows total=0
  for band in $bands; do
    from=$(psql_as consumer "SELECT coalesce(topic.fetch_offset('consumer.orders_q', 'orders', $band), 0)")
    rows=$(psql_as consumer "SELECT count(*) || '|' || coalesce(max(log_offset) + 1, $from)
                             FROM topic.fetch('consumer.orders_q', $band, $from)")
    total=$((total + ${rows%%|*}))
    [ "$(psql_as consumer "SELECT topic.commit_offset('consumer.orders_q', 'orders', $band, ${rows##*|}, $generation)")" = NONE ] \
      || echo "commit of band $band by $member failed" >&2
  done
  echo "$total"
}

a=$(join "" | cut -d'|' -f2)
b=$(join "" | cut -d'|' -f2)
chk "an empty member id gets MEMBER_ID_REQUIRED and two different ids" yes \
  "$([ -n "$a" ] && [ -n "$b" ] && [ "$a" != "$b" ] && echo yes || echo no)"
chk "the first join of A waits for the window" WAIT "$(join "$a" | cut -d'|' -f1)"
chk "the first join of B waits for the window" WAIT "$(join "$b" | cut -d'|' -f1)"
ra=$(poll_join "$a")
rb=$(poll_join "$b")
leader=$(echo "$ra" | cut -d'|' -f4)
chk "A and B get generation 1 and the same leader" "NONE|1|$leader NONE|1|$leader" \
  "$(echo "$ra" | cut -d'|' -f1,3,4) $(echo "$rb" | cut -d'|' -f1,3,4)"
follower=$([ "$leader" = "$a" ] && echo "$b" || echo "$a")
chk "the leader sync stores the hand-made assignment" "NONE|[0, 1]" \
  "$(sync "$leader" 1 "{\"$leader\": [0, 1], \"$follower\": [2, 3]}")"
chk "the follower gets its share" "NONE|[2, 3]" "$(poll_sync "$follower" 1)"
read_leader=$(consume "$leader" 1 "0 1")
read_follower=$(consume "$follower" 1 "2 3")
chk "the two consumers read every row once" 40 "$((read_leader + read_follower))"
chk "the committed offsets equal next_offset" t \
  "$(psql_as postgres "SELECT bool_and(o.committed_offset = p.next_offset) AND count(*) = 4
                       FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
                       WHERE o.group_name = 'orders'")"

chk "the follower leaves" NONE "$(psql_as consumer "SELECT topic.group_leave('orders', '$follower')")"
chk "the leader heartbeat sees the rebalance" REBALANCE_IN_PROGRESS \
  "$(psql_as consumer "SELECT topic.group_heartbeat('orders', '$leader', 1)")"
chk "the rejoin closes the window at once with generation 2" "NONE|2|$leader" \
  "$(join "$leader" | cut -d'|' -f1,3,4)"
chk "the remaining consumer gets all bands" "NONE|[0, 1, 2, 3]" \
  "$(sync "$leader" 2 "{\"$leader\": [0, 1, 2, 3]}")"
chk "a commit from the old generation is refused" ILLEGAL_GENERATION \
  "$(psql_as consumer "SELECT topic.commit_offset('consumer.orders_q', 'orders', 2, 1, 1)")"
psql_as consumer "SELECT topic.publish('consumer.orders_q', jsonb_build_object('i', i), 'k' || i)
                  FROM generate_series(41, 48) i" >/dev/null
wait_for "[ \"\$(unstamped consumer.orders_q)\" = 0 ]"
chk "the remaining consumer reads the new rows on all bands" 8 "$(consume "$leader" 2 "0 1 2 3")"

racer() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U consumer -d postgres -qtA 2>&1 <<SQL
DO \$\$
DECLARE
  r record;
  m text := '$2';
BEGIN
  IF m = '' THEN
    SELECT * INTO r FROM topic.group_join('$1', '', 'racer', $4, $3, 'consumer', '$PROTOCOLS');
    m := r.member_id;
  END IF;
  SELECT * INTO r FROM topic.group_join('$1', m, 'racer', $4, $3, 'consumer', '$PROTOCOLS');
  COMMIT;
  WHILE r.error IS NULL LOOP
    SELECT * INTO r FROM topic.group_join_poll('$1', m);
    COMMIT;
  END LOOP;
  RAISE NOTICE 'result % % % %', r.error, r.generation_id, r.leader_id = m, m;
END
\$\$;
SQL
}

results() {
  grep -h "result $1" "$WORK"/"$2"_*.out | wc -l
}

for i in $(seq 1 8); do racer racers "" 3000 10000 >"$WORK/first_$i.out" & done
wait
chk "8 concurrent joins give exactly one generation increment" 1 \
  "$(psql_as postgres "SELECT generation_id FROM topic.topic_groups WHERE group_name = 'racers'")"
chk "all 8 members get generation 1" 8 "$(results "NONE 1 " first)"
chk "exactly one member is the leader" 1 "$(results "NONE 1 t" first)"
chk "the group has 8 members" 8 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_group_members WHERE group_name = 'racers'")"

leader=$(grep -h "result NONE 1 t" "$WORK"/first_*.out | awk '{print $NF}')
chk "the leader sync makes the group Stable" "NONE|Stable" \
  "$(psql_as consumer "SELECT error FROM topic.group_sync('racers', '$leader', 1, '{}')")|$(psql_as postgres "SELECT state FROM topic.topic_groups WHERE group_name = 'racers'")"
late=$(psql_as consumer "SELECT member_id FROM topic.group_join('racers', '', 'late', 10000, 20000, 'consumer', '$PROTOCOLS')")
chk "a new member starts a rebalance of the Stable group" "WAIT|PreparingRebalance" \
  "$(psql_as consumer "SELECT coalesce(error, 'WAIT') FROM topic.group_join('racers', '$late', 'late', 10000, 20000, 'consumer', '$PROTOCOLS')")|$(psql_as postgres "SELECT state FROM topic.topic_groups WHERE group_name = 'racers'")"
start=$(date +%s%N)
i=0
for m in $(grep -h "result NONE 1" "$WORK"/first_*.out | awk '{print $NF}') "$late"; do
  i=$((i + 1))
  racer racers "$m" 20000 10000 >"$WORK/again_$i.out" &
done
wait
elapsed=$((($(date +%s%N) - start) / 1000000))
echo "the rejoin of 9 members took $elapsed ms"
chk "the window closes when all 9 members rejoined, before the 20 s timeout" yes \
  "$([ "$elapsed" -lt 10000 ] && echo yes || echo no)"
chk "the rejoin gives exactly one more generation" 2 \
  "$(psql_as postgres "SELECT generation_id FROM topic.topic_groups WHERE group_name = 'racers'")"
chk "all 9 members get generation 2 and one is the leader" "9|1" "$(results "NONE 2 " again)|$(results "NONE 2 t" again)"

idle=$(racer idle "" 1000 6000 | awk '/result NONE 1 t/ {print $NF}')
chk "a one-member group becomes Stable" NONE \
  "$(psql_as consumer "SELECT error FROM topic.group_sync('idle', '$idle', 1, '{}')")"
chk "the partition worker expires the member of a group that nobody calls" yes \
  "$(wait_for "[ \"\$(psql_as postgres \"SELECT state || '|' || expired_members FROM topic.topic_groups WHERE group_name = 'idle'\")\" = 'Empty|1' ]" && echo yes || echo no)"

hold_and_drop() {
  local topic=$1 seconds=$2 start out
  psql_as consumer "SELECT topic.create_topic('consumer.$topic', 1)" >/dev/null
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -o /dev/null \
    -c "BEGIN" -c "LOCK TABLE consumer.$topic IN ACCESS SHARE MODE" -c "SELECT pg_sleep($seconds)" -c "COMMIT" \
    >/dev/null 2>&1 &
  wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"
  start=$(date +%s%N)
  out=$(psql_as consumer "SELECT topic.drop_topic('consumer.$topic')")
  echo "$(( ($(date +%s%N) - start) / 1000000 ))|$out"
  wait
}

r=$(hold_and_drop held_q 3)
chk "drop_topic retries past a 3 s lock and drops the topic" "yes|" \
  "$([ "${r%%|*}" -ge 2000 ] && [ "${r%%|*}" -lt 6000 ] && echo yes || echo no)|${r#*|}"
chk "the dropped topic has no control rows" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'held_q'")"
r=$(hold_and_drop stuck_q 9)
chk "drop_topic gives up after 3 lock timeouts of 2 s" yes \
  "$([ "${r%%|*}" -ge 6000 ] && echo "${r#*|}" | grep -q 'canceling statement due to lock timeout' && echo yes || echo no)"
chk "the topic that could not be dropped keeps its table and rows" "true|1" \
  "$(psql_as postgres "SELECT to_regclass('consumer.stuck_q') IS NOT NULL || '|' || count(*)
                       FROM topic.topic_config WHERE topic = 'stuck_q'")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
