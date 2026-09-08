#!/usr/bin/env bash
# Shared-nothing scale-out. Each slot worker owns a disjoint partition in
# its own segment and its own port, so there is no cross-worker contention. We
# drive N workers with N parallel redis-benchmark clients and sum throughput.
#
# Caveat: client and server are co-located on 4 cores, so N parallel clients
# compete with the N workers for CPU; scaling is therefore sub-linear here and
# would be cleaner with the load generator on a separate host.
set -u
BIN=/home/user/postgres/extensions/pg_keyspace/core/target/release/pgkeyspaced
OUT=/home/user/postgres/extensions/pg_keyspace/results
BASE=6390
REQS=500000
mkdir -p "$OUT"

pkill -f "pgkeyspaced" 2>/dev/null; sleep 1
"$BIN" --workers 4 --port $BASE --keys-per-worker 300000 --val-bytes 512 >/tmp/pgks_scale.log 2>&1 &
DPID=$!
sleep 1
for _ in $(seq 1 20); do redis-cli -p $BASE ping >/dev/null 2>&1 && break; sleep 0.3; done

run_n() {
  local n=$1 test=$2
  local pids=() tmp=()
  for i in $(seq 0 $((n-1))); do
    local f="/tmp/scale_${test}_${n}_${i}.txt"
    tmp+=("$f")
    redis-benchmark -p $((BASE+i)) -t "$test" -n "$REQS" -c 25 -P 16 -d 512 -r 200000 >"$f" 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait "$p"; done
  local sum=0
  for f in "${tmp[@]}"; do
    local r
    r=$(grep -oE "[0-9.]+ requests per second" "$f" | head -1 | awk '{print $1}')
    sum=$(awk -v a="$sum" -v b="${r:-0}" 'BEGIN{printf "%.0f", a+b}')
  done
  echo "$sum"
}

{
echo "# Shared-nothing scale-out (pipelined -c 25 -P 16, 512B), aggregate rps"
printf "%-8s | %14s %14s\n" "workers" "SET agg rps" "GET agg rps"
for n in 1 2 4; do
  s=$(run_n "$n" set)
  g=$(run_n "$n" get)
  printf "%-8s | %14s %14s\n" "$n" "$s" "$g"
done
} | tee "$OUT/scaleout.txt"

kill $DPID 2>/dev/null
pkill -f "pgkeyspaced" 2>/dev/null
echo "done."
