#!/usr/bin/env bash
# Per-durability-tier RESP SET benchmark (§3.4), all in-PG. For each tier we
# reconfigure pg_keyspace.durability, restart Postgres (the GUC is Postmaster
# context), and measure:
#   * closed-loop SET latency (-c 1 -P 1): what one client waits for a write;
#   * concurrent SET throughput (-c 50 -P 1): shows one fsync amortised across
#     many in-flight writes — the commit-batching lever.
set -u
PGBIN=/usr/lib/postgresql/16/bin
DATA=/tmp/pgks_data
PGKS_PORT=6380
OUT=/home/user/postgres/extensions/pg_keyspace/results
P="psql -h 127.0.0.1 -p 5433 -U supatype_admin -d postgres -X -q -t -A"
mkdir -p "$OUT"

restart_with() {
  local tier=$1
  runuser -u postgres -- $PGBIN/pg_ctl -D "$DATA" -w stop >/dev/null 2>&1
  $P -c "ALTER SYSTEM SET pg_keyspace.durability='$tier';" >/dev/null 2>&1 || true
  # ALTER SYSTEM needs a running server; instead edit conf directly for robustness.
  sed -i "s/^pg_keyspace.durability = .*/pg_keyspace.durability = '$tier'/" "$DATA/postgresql.conf"
  runuser -u postgres -- $PGBIN/pg_ctl -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
  # wait for RESP port
  for _ in $(seq 1 20); do redis-cli -p $PGKS_PORT ping >/dev/null 2>&1 && break; sleep 0.3; done
}

{
echo "# Durability tiers — RESP SET (§3.4), commit_window=500us"
echo "host: $(nproc) cpus; WAL on container fs"
printf "%-12s | %14s %10s | %16s\n" "tier" "closed p50(ms)" "avg(ms)" "concurrent rps(c50)"
for tier in ephemeral relaxed durable replicated; do
  restart_with "$tier"
  raw1="$OUT/dur_${tier}_closed.txt"
  raw2="$OUT/dur_${tier}_c50.txt"
  redis-benchmark -p $PGKS_PORT -t set -n 50000 -c 1  -P 1 -d 512 >"$raw1" 2>&1
  redis-benchmark -p $PGKS_PORT -t set -n 200000 -c 50 -P 1 -d 512 >"$raw2" 2>&1
  read -r avg1 min1 p50c p95 p99 max < <(awk '/latency summary/{getline;getline;print $1,$2,$3,$4,$5,$6}' "$raw1")
  rps2=$(grep -oE "[0-9.]+ requests per second" "$raw2" | head -1 | awk '{print $1}')
  printf "%-12s | %14s %10s | %16s\n" "$tier" "$p50c" "$avg1" "$rps2"
done
} | tee "$OUT/durability.txt"

# restore default
restart_with ephemeral
echo "done."
