#!/usr/bin/env bash
# pg_keyspace benchmark harness.
#
# Compares pg_keyspace (RESP served by a Postgres background worker over a PG
# shared-memory segment) against Redis/Valkey, on the same host, same client,
# same parameters. Also measures the in-backend SQL read path and the
# per-durability-tier RESP SET cost.
#
# Assumes: redis on $REDIS_PORT, pg_keyspace RESP on $PGKS_PORT, Postgres on
# $PGPORT with the extension created.
set -u

REDIS_PORT=${REDIS_PORT:-6379}
PGKS_PORT=${PGKS_PORT:-6380}
PGPORT=${PGPORT:-5433}
PGHOST=${PGHOST:-127.0.0.1}
PGUSER=${PGUSER:-supatype_admin}
OUT=${OUT:-/home/user/postgres/extensions/pg_keyspace/results}
DATASIZE=${DATASIZE:-512}
KEYSPACE=${KEYSPACE:-100000}
mkdir -p "$OUT"
PSQL="psql -h $PGHOST -p $PGPORT -U $PGUSER -d postgres -X -q -t -A"

# Extract "avg min p50 p95 p99 max" + throughput from redis-benchmark output.
parse_bench() {
  awk '
    /throughput summary/ { getline; next }
    /requests per second/ { rps=$1 }
    /latency summary/ { inlat=1; getline; getline; avg=$1;min=$2;p50=$3;p95=$4;p99=$5;max=$6; inlat=0 }
    END { printf "%.0f %s %s %s %s %s %s", rps, avg, min, p50, p95, p99, max }
  '
}

run_one() {
  local port=$1 label=$2 test=$3 clients=$4 pipe=$5 reqs=$6
  local raw="$OUT/raw_${label}_${test}_c${clients}_P${pipe}.txt"
  redis-benchmark -h 127.0.0.1 -p "$port" -t "$test" -n "$reqs" -c "$clients" \
      -P "$pipe" -d "$DATASIZE" -r "$KEYSPACE" >"$raw" 2>&1
  # redis-benchmark prints "requests per second" on the completion line and a
  # "latency summary (msec)" block; pull p50/p99/avg from the latter.
  local rps p50 p99 avg
  rps=$(grep -oE "[0-9.]+ requests per second" "$raw" | head -1 | awk '{print $1}')
  # latency summary block
  read -r avg min p50 p95 p99 max < <(awk '/latency summary/{getline;getline;print $1,$2,$3,$4,$5,$6}' "$raw")
  printf "%s\n" "$rps|$avg|$p50|$p99|$max"
}

echo "# pg_keyspace benchmark — $(date -u +%FT%TZ)" | tee "$OUT/summary.txt"
echo "host: $(nproc) cpus, $(uname -sr)" | tee -a "$OUT/summary.txt"
echo "redis: $(redis-cli -p $REDIS_PORT info server 2>/dev/null | grep -oE 'redis_version:[0-9.]+')" | tee -a "$OUT/summary.txt"
echo "datasize=${DATASIZE}B keyspace=${KEYSPACE}" | tee -a "$OUT/summary.txt"
echo | tee -a "$OUT/summary.txt"

# ---------------------------------------------------------------------------
# 1. Closed-loop latency (1 connection, no pipeline) — the client-observed
#    single-request latency the table targets.
# ---------------------------------------------------------------------------
echo "## Closed-loop latency (-c 1 -P 1), 200k requests, ${DATASIZE}B values" | tee -a "$OUT/summary.txt"
printf "%-10s %-10s | %12s %8s %8s %8s\n" "op" "server" "rps" "avg(ms)" "p50" "p99" | tee -a "$OUT/summary.txt"
for test in set get incr; do
  for srv in "redis:$REDIS_PORT" "pgks:$PGKS_PORT"; do
    label=${srv%%:*}; port=${srv##*:}
    IFS='|' read -r rps avg p50 p99 max <<<"$(run_one "$port" "$label" "$test" 1 1 200000)"
    printf "%-10s %-10s | %12s %8s %8s %8s\n" "$test" "$label" "$rps" "$avg" "$p50" "$p99" | tee -a "$OUT/summary.txt"
  done
done
echo | tee -a "$OUT/summary.txt"

# ---------------------------------------------------------------------------
# 2. Pipelined throughput (-c 50 -P 16) — the throughput row.
# ---------------------------------------------------------------------------
echo "## Pipelined throughput (-c 50 -P 16), 1M requests, ${DATASIZE}B values" | tee -a "$OUT/summary.txt"
printf "%-10s %-10s | %12s %8s %8s\n" "op" "server" "rps" "p50(ms)" "p99" | tee -a "$OUT/summary.txt"
for test in set get; do
  for srv in "redis:$REDIS_PORT" "pgks:$PGKS_PORT"; do
    label=${srv%%:*}; port=${srv##*:}
    IFS='|' read -r rps avg p50 p99 max <<<"$(run_one "$port" "$label" "$test" 50 16 1000000)"
    printf "%-10s %-10s | %12s %8s %8s\n" "$test" "$label" "$rps" "$p50" "$p99" | tee -a "$OUT/summary.txt"
  done
done
echo | tee -a "$OUT/summary.txt"

# ---------------------------------------------------------------------------
# 3. In-backend SQL read path: pure shared-memory op latency inside the
#    calling Postgres backend, no client round-trip.
# ---------------------------------------------------------------------------
echo "## In-backend shared-memory op latency, 5M iters, in-process" | tee -a "$OUT/summary.txt"
ITERS=5000000
gns=$($PSQL -c "SELECT supacache.bench_get('benchk', repeat('x',$DATASIZE)::bytea, $ITERS);")
sns=$($PSQL -c "SELECT supacache.bench_set('benchk', repeat('x',$DATASIZE)::bytea, $ITERS);")
ins=$($PSQL -c "SELECT supacache.bench_incr('benchc', $ITERS);")
printf "  get:  %s ns/op\n" "$gns" | tee -a "$OUT/summary.txt"
printf "  set:  %s ns/op\n" "$sns" | tee -a "$OUT/summary.txt"
printf "  incr: %s ns/op\n" "$ins" | tee -a "$OUT/summary.txt"
echo | tee -a "$OUT/summary.txt"

# ---------------------------------------------------------------------------
# 4. Full round-trip of the SQL surface via libpq (what /rpc/get would incur).
# ---------------------------------------------------------------------------
echo "## SQL surface via libpq round-trip (SELECT supacache.get), pgbench" | tee -a "$OUT/summary.txt"
$PSQL -c "SELECT supacache.set('rpckey','hello'::bytea,0);" >/dev/null
cat > /tmp/pgks_rpc.sql <<SQL
SELECT supacache.get('rpckey');
SQL
pgbench -h $PGHOST -p $PGPORT -U $PGUSER -d postgres -n -c 1 -T 5 -f /tmp/pgks_rpc.sql 2>/dev/null \
  | grep -E "latency average|tps" | sed 's/^/  /' | tee -a "$OUT/summary.txt"
echo | tee -a "$OUT/summary.txt"

echo "raw redis-benchmark outputs in $OUT/raw_*.txt" | tee -a "$OUT/summary.txt"
echo "done."
