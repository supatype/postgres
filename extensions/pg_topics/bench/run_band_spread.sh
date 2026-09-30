#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
source "$HERE/benchmark/lib.sh"
new_cluster

sql_uuid_spread() {
  local bands=$1
  local topic="public.sql${bands}_q"
  psql_as postgres "SELECT topic.create_topic('$topic', $bands)" >/dev/null
  psql_as postgres "SELECT topic.publish('$topic', jsonb_build_object('i', i), gen_random_uuid()::text)
                    FROM generate_series(1, 100000) i" >/dev/null
  wait_for "[ \"\$(unstamped $topic)\" = 0 ]"
  chk "SQL publish with random uuid keys spreads within 5% of the mean, $bands bands" t "$(band_spread_ok "$topic")"
  chk "SQL publish with random uuid keys gets one offset with no gap per band, $bands bands" t "$(gap_free "sql${bands}_q")"
}

sql_round_robin_spread() {
  local bands=$1
  local topic="public.rr${bands}_q"
  psql_as postgres "SELECT topic.create_topic('$topic', $bands)" >/dev/null
  psql_as postgres "SELECT topic.publish('$topic', jsonb_build_object('i', i)) FROM generate_series(1, 100000) i" >/dev/null
  wait_for "[ \"\$(unstamped $topic)\" = 0 ]"
  chk "NULL-key SQL publish round-robins within 5% of the mean, $bands bands" t "$(band_spread_ok "$topic")"
  chk "NULL-key SQL publish gets one offset with no gap per band, $bands bands" t "$(gap_free "rr${bands}_q")"
}

for bands in 4 12; do
  sql_uuid_spread "$bands"
  sql_round_robin_spread "$bands"
done

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
start_listener
java_config alice alice-pw

kafka_uuid_lines() {
  awk -v n=100000 'BEGIN {
    srand()
    for (i = 0; i < n; i++) {
      key = sprintf("%04x%04x-%04x-4%03x-%x%03x-%04x%04x%04x", rand() * 65536, rand() * 65536, rand() * 65536, \
        rand() * 4096, 8 + rand() * 4, rand() * 4096, rand() * 65536, rand() * 65536, rand() * 65536)
      printf "%s\t{\"i\": %d}\n", key, i
    }
  }'
}

if kafka_reachable; then
  kafka_uuid_spread() {
    local bands=$1
    local topic="public.kb${bands}_q"
    psql_as postgres "SELECT topic.create_topic('$topic', $bands)" >/dev/null
    psql_as postgres "SELECT topic.grant_publish('$topic', 'alice')" >/dev/null
    kafka_uuid_lines | kafka_java kafka-console-producer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" \
      --topic "$topic" --producer.config /w/alice.properties --property parse.key=true >/dev/null
    wait_for "[ \"\$(unstamped $topic)\" = 0 ]"
    chk "Kafka producer with random uuid keys spreads within 5% of the mean, $bands bands" t "$(band_spread_ok "$topic")"
    chk "Kafka producer with random uuid keys gets one offset with no gap per band, $bands bands" t "$(gap_free "kb${bands}_q")"
  }
  for bands in 4 12; do
    kafka_uuid_spread "$bands"
  done
else
  echo "SKIPPED: docker cannot reach the listener (Kafka producer band spread)"
fi

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
