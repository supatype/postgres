#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
source "$HERE/benchmark/lib.sh"
new_cluster
install_release_build

seconds=${SOAK_SECONDS:-600}
rate=10000
records=$((rate * seconds))

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
psql_as alice "SELECT topic.create_topic('alice.soak_q', 4)" >/dev/null
start_listener
java_config alice alice-pw
seq 1 100 | awk '{ printf "{\"i\": %d, \"pad\": \"%0200d\"}\n", $1, 0 }' >"$WORK/payload.json"

: >"$WORK/backlog"
while sleep 1; do
  psql_as postgres "SELECT extract(epoch FROM backlog_age) FROM topic.stamp_backlog WHERE topic = 'soak_q'" >>"$WORK/backlog"
done &
sampler=$!
kafka_java kafka-consumer-perf-test --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.soak_q --group soak \
  --messages "$records" --timeout 60000 --consumer.config /w/alice.properties >"$WORK/consumer.out" &
consumer=$!
produced=$(kafka_java kafka-producer-perf-test --topic alice.soak_q --num-records "$records" --throughput "$rate" \
  --payload-file /w/payload.json --producer.config /w/alice.properties \
  --producer-props "bootstrap.servers=$BOOTSTRAP_HOST:$KPORT" acks=all | grep 'records sent' | tail -n 1)
wait "$consumer"
kill "$sampler"
wait "$sampler" 2>/dev/null || true
consumed=$(grep -A1 '^start.time' "$WORK/consumer.out" | tail -n 1)

echo "soak: $seconds s, $records records of about 220 bytes, target $rate records/s, acks=all, 4 bands"
echo "produce records/s:      $(sed -n 's/^[0-9]* records sent, \([0-9.]*\) records\/sec.*/\1/p' <<<"$produced")"
echo "produce p99 latency ms: $(sed -n 's/.* \([0-9]*\) ms 99th,.*/\1/p' <<<"$produced")"
echo "consume records:        $(cut -d, -f5 <<<"$consumed" | tr -d ' ')"
echo "consume records/s:      $(cut -d, -f6 <<<"$consumed" | tr -d ' ')"
echo "max backlog_age s:      $(sort -g "$WORK/backlog" | tail -n 1)"
echo "backlog_age at end:     $(psql_as postgres "SELECT backlog_age FROM topic.stamp_backlog WHERE topic = 'soak_q'")"
echo "dead tuples at end:     $(psql_as postgres "SELECT n_dead_tup FROM topic.write_partition_dead_tuples WHERE topic = 'soak_q'")"
echo "producer summary:       $produced"
