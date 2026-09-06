#!/usr/bin/env bash
# Durable-write scale-out: does adding persistence workers raise the sustained
# rate at which RESP writes reach supacache.kv? For W in 1,2,4 we reconfigure
# pg_keyspace.persist_workers (Postmaster GUC), restart, fire a burst that fits
# the rings (no drops), and time how long until the rings fully drain.
set -u
PGBIN=/usr/lib/postgresql/16/bin
DATA=/tmp/pgks_data
OUT=/home/user/postgres/extensions/pg_keyspace/results
P="psql -h 127.0.0.1 -p 5433 -U supatype_admin -d postgres -X -q -t -A"
BURST=${BURST:-300000}
mkdir -p "$OUT"

restart_with_workers() {
  local w=$1
  runuser -u postgres -- $PGBIN/pg_ctl -D "$DATA" -w stop >/dev/null 2>&1
  sed -i "s/^pg_keyspace.persist_workers = .*/pg_keyspace.persist_workers = $w/" "$DATA/postgresql.conf"
  runuser -u postgres -- $PGBIN/pg_ctl -D "$DATA" -l "$DATA/server.log" -w start >/dev/null 2>&1
  for _ in $(seq 1 30); do redis-cli -p 6380 ping >/dev/null 2>&1 && break; sleep 0.3; done
  # wait for the table (persist worker 0 creates it)
  for _ in $(seq 1 30); do [ "$($P -c "SELECT to_regclass('supacache.kv') IS NOT NULL;" 2>/dev/null)" = "t" ] && break; sleep 0.2; done
}

{
echo "# Durable-write scale-out — sustained drain rate vs persistence workers"
echo "# burst=$BURST distinct keys (-r 5000000), 512B, ring_mb=256 each, 4-vCPU"
printf "%-9s | %14s | %8s | %10s\n" "workers" "drain rate/s" "dropped" "table rows"
for w in 1 2 4; do
  restart_with_workers "$w"
  $P -c "TRUNCATE supacache.kv;" >/dev/null 2>&1
  read P0 D0 <<<"$($P -c "SELECT pushed||' '||dropped FROM supacache.ring_stats();")"
  T0=$(date +%s.%N)
  redis-benchmark -p 6380 -t set -n "$BURST" -c 50 -P 16 -d 512 -r 5000000 -q >/dev/null 2>&1
  while true; do B=$($P -c "SELECT backlog_bytes FROM supacache.ring_stats();"); [ "${B:-1}" = "0" ] && break; sleep 0.05; done
  T1=$(date +%s.%N)
  read PB DB <<<"$($P -c "SELECT pushed||' '||dropped FROM supacache.ring_stats();")"
  local_pushed=$((PB-P0)); local_drop=$((DB-D0))
  rate=$(awk -v p=$local_pushed -v a=$T0 -v b=$T1 'BEGIN{printf "%.0f", p/(b-a)}')
  rows=$($P -c "SELECT count(*) FROM supacache.kv;")
  printf "%-9s | %14s | %8s | %10s\n" "$w" "$rate" "$local_drop" "$rows"
done
} | tee "$OUT/persist_scaleout.txt"

# restore single worker
restart_with_workers 1
echo "done."
