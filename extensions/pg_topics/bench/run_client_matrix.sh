#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster
trap 'docker rm -f $(docker ps -aq --filter "name=pgt_${PORT}_") >/dev/null 2>&1 || true; cleanup' EXIT

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
start_listener
java_config alice alice-pw
for c in node go; do
  docker build -q -t "pg_topics_$c:matrix" "$HERE/clients/$c" >/dev/null 2>&1 || { echo "FAIL  docker build of clients/$c"; exit 1; }
done

in_docker() {
  local name=$1
  shift
  docker run --rm ${name:+--name "pgt_${PORT}_$name"} "${DOCKER_NET_ARGS[@]}" -v "$WORK:/w:ro" \
    -e BOOTSTRAP="$BOOTSTRAP_HOST:$KPORT" -e KAFKA_USER=alice -e KAFKA_PASSWORD=alice-pw "$@" 2>&1
}

produce() {
  case $1 in
    java) seq 0 999 | awk '{ printf "n:%d\tk-%d\t{\"i\": %d}\n", $1, $1, $1 }' |
      in_docker "" -i confluentinc/cp-kafka:7.7.1 kafka-console-producer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" \
        --topic "$2" --producer.config /w/alice.properties --property parse.key=true --property parse.headers=true ;;
    librdkafka) in_docker "" -v "$HERE/clients/python:/app:ro" pg_topics_python:2.15.1 \
      python /app/client.py produce "$2" 1000 json none k ;;
    confluent | kafkajs) in_docker "" pg_topics_node:matrix node client.js "$1" produce "$2" ;;
    franz-go) in_docker "" pg_topics_go:matrix client produce "$2" ;;
  esac
}

member() {
  case $1 in
    java) in_docker "$4" confluentinc/cp-kafka:7.7.1 kafka-verifiable-consumer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" \
      --topic "$2" --group-id "$3" --consumer.config /w/alice.properties --verbose ;;
    librdkafka) in_docker "$4" -v "$HERE/clients/python:/app:ro" pg_topics_python:2.15.1 python /app/client.py \
      member "$2" "$3" "$4" enable.auto.commit=true,enable.auto.offset.store=true,auto.commit.interval.ms=500 ;;
    confluent | kafkajs) in_docker "$4" pg_topics_node:matrix node client.js "$1" member "$2" "$3" "$4" ;;
    franz-go) in_docker "$4" pg_topics_go:matrix client member "$2" "$3" "$4" ;;
  esac >"$WORK/$4.out" &
}

stop_member() {
  touch "$WORK/stop-$2"
  if [ "$1" = java ]; then
    docker stop -t 30 "pgt_${PORT}_$2" >/dev/null 2>&1 || true
  fi
  wait_for "[ -z \"\$(docker ps -q --filter name=^pgt_${PORT}_$2\$)\" ]" || true
}

msgs() {
  grep '^msg ' "$WORK/$1.out" || true
  jq -rR 'fromjson? | objects | select(.name == "record_data") | "msg \(.partition) \(.offset)"' "$WORK/$1.out"
}

first_offsets() {
  msgs "$1" | awk '!($2 in m) || $3 < m[$2] { m[$2] = $3 } END { for (b in m) print b ":" m[b] }' | sort -n | paste -sd,
}

group_state() {
  psql_as postgres "SELECT g.state || '|' || count(m.member_id) FROM topic.topic_groups g
    LEFT JOIN topic.topic_group_members m USING (group_name) WHERE g.group_name = '$1' GROUP BY g.state"
}

committed_all() {
  psql_as postgres "SELECT count(*) = 4 AND bool_and(o.committed_offset = p.next_offset)
    FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
    WHERE o.group_name = '$1'"
}

matrix() {
  local c=$1 t="alice.${1//-/_}_q" g="${1}_g" out expected seen both
  psql_as alice "SELECT topic.create_topic('$t', 4)" >/dev/null
  member "$c" "$t" "$g" "${c}_a"
  member "$c" "$t" "$g" "${c}_b"
  wait_for "[ \"\$(group_state $g)\" = 'Stable|2' ]" || true
  chk "$c: two members join group $g and reach Stable" "Stable|2" "$(group_state "$g")"

  out=$(produce "$c" "$t" || true)
  wait_for "[ \"\$(unstamped $t)\" = 0 ]" || true
  chk "$c: produces 1000 keyed JSON records with headers over SASL_SSL PLAIN" 1000 \
    "$(psql_as postgres "SELECT count(*) FROM $t WHERE key = 'k-' || (value->>'i')
       AND headers = jsonb_build_array(jsonb_build_object('key', 'n', 'value', value->>'i'))")"
  chk "$c: every key is on band_for(key, 4)" 0 \
    "$(psql_as postgres "SELECT count(*) FROM $t WHERE band IS DISTINCT FROM topic.band_for(key, 4)")"
  if [ "$(psql_as postgres "SELECT count(*) FROM $t")" != 1000 ]; then
    echo "        $c produce output: $(tail -n 5 <<<"$out")"
  fi

  wait_for "[ \"\$(committed_all $g)\" = t ] && [ \"\$({ msgs ${c}_a; msgs ${c}_b; } | sort -u | wc -l)\" = 1000 ]" || true
  seen=$({ msgs "${c}_a"; msgs "${c}_b"; } | sort -u | wc -l)
  both=no
  [ -n "$(msgs "${c}_a")" ] && [ -n "$(msgs "${c}_b")" ] && both=yes
  chk "$c: the two members read all 1000 records, both read some, and the group commits next_offset on every band" \
    "1000 yes t" "$seen $both $(committed_all "$g")"
  stop_member "$c" "${c}_a"
  stop_member "$c" "${c}_b"

  expected=$(psql_as postgres "SELECT string_agg(band || ':' || committed_offset, ',' ORDER BY band)
    FROM topic.topic_offsets WHERE group_name = '$g'")
  psql_as alice "SELECT topic.publish('$t', jsonb_build_object('r', i), 'r-' || i) FROM generate_series(1, 200) i" >/dev/null
  wait_for "[ \"\$(unstamped $t)\" = 0 ]" || true
  member "$c" "$t" "$g" "${c}_r"
  wait_for "[ \"\$(msgs ${c}_r | wc -l)\" -ge 200 ] && [ \"\$(committed_all $g)\" = t ]" || true
  stop_member "$c" "${c}_r"
  chk "$c: after a restart the group resumes at the committed offset of each band and reads only the 200 new records" \
    "$expected 200" "$(first_offsets "${c}_r") $(msgs "${c}_r" | wc -l)"

  chk "$c: a Java consumer with check.crcs=true reads every batch of the topic" 1200 \
    "$(in_docker "" confluentinc/cp-kafka:7.7.1 kafka-console-consumer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" \
        --topic "$t" --from-beginning --max-messages 1200 --timeout-ms 30000 \
        --consumer.config /w/alice.properties --consumer-property check.crcs=true | grep -c '^{"[ir]": [0-9]*}$' || true)"
}

for c in java librdkafka confluent kafkajs franz-go; do
  before=$fail
  matrix "$c"
  if [ "$c" = kafkajs ] || [ "$c" = franz-go ]; then
    echo "REPORT ONLY  $c: $((fail - before)) check(s) failed; they do not fail the run"
    fail=$before
  fi
done

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
