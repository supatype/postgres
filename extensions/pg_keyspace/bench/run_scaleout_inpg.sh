#!/usr/bin/env bash
# in-PG multi-worker scale-out. The extension can run N shared-
# nothing RESP slot workers (pg_keyspace.workers), each owning its own shared-
# memory segment and listening on pg_keyspace.port + its index. This proves the
# workers are independent (a key on one is invisible on another) and that
# aggregate RESP throughput scales with N — the horizontal-scaling claim, now
# inside Postgres, not just in the standalone daemon.
#
# Run against a cluster started with pg_keyspace.workers > 1. Auto-detects TLS
# and how many worker ports are live from pg_keyspace.port.
set -u
PGPORT=${PGPORT:-5434}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
BASE=$($P -c "SHOW pg_keyspace.port" 2>/dev/null | tr -d '[:space:]'); BASE=${BASE:-6381}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-50s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-50s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

# TLS autodetect on the base port
if redis-cli -p "$BASE" PING 2>/dev/null | grep -q PONG; then T=""; else T="--tls --insecure"; fi
R(){ redis-cli $T -p "$1" "${@:2}" 2>/dev/null; }
[ "$(timeout 3 redis-cli $T -p "$BASE" PING 2>/dev/null)" = "PONG" ] || { echo "  SKIP  no RESP worker on :$BASE"; exit 0; }

# discover live worker ports (BASE, BASE+1, ...)
ports=(); p=$BASE
while [ "$(timeout 2 redis-cli $T -p "$p" PING 2>/dev/null)" = "PONG" ] && [ ${#ports[@]} -lt 64 ]; do
  ports+=("$p"); p=$((p+1))
done
n=${#ports[@]}
echo "# in-PG scale-out — $n RESP slot worker(s) on ${ports[0]}..${ports[-1]} (tls=${T:-off})"
[ "$n" -ge 2 ] || { echo "  SKIP  only $n worker(s) — set pg_keyspace.workers>1 and restart"; exit 0; }

# 1. shared-nothing: a uniquely-named key written to worker 0 is invisible on
#    every other worker (each has its own segment).
probe="sn_probe_$$_$(date +%s)"
R "${ports[0]}" SET "$probe" hello >/dev/null
chk "worker 0 serves its own key"            "hello" "$(R "${ports[0]}" GET "$probe")"
miss=0
for pp in "${ports[@]:1}"; do [ -z "$(R "$pp" GET "$probe")" ] && miss=$((miss+1)); done
chk "key is invisible on the other $((n-1)) workers" "$((n-1))" "$miss"
R "${ports[0]}" DEL "$probe" >/dev/null

# 2. independent keyspaces: writing a distinct number of NEW keys to each worker
#    moves only that worker's DBSIZE by exactly that count (delta, so leftover
#    keys from any prior run don't matter).
ok=1
for i in "${!ports[@]}"; do
  cnt=$(( (i+1) * 10 ))
  before=$(R "${ports[$i]}" DBSIZE)
  for k in $(seq 1 $cnt); do R "${ports[$i]}" SET "sc_${probe}_w${i}k${k}" v >/dev/null; done
  after=$(R "${ports[$i]}" DBSIZE)
  delta=$(( after - before ))
  [ "$delta" = "$cnt" ] || { ok=0; echo "    worker $i delta=$delta want=$cnt"; }
done
chk "each worker's DBSIZE moves independently (delta)" "1" "$ok"
for i in "${!ports[@]}"; do
  cnt=$(( (i+1) * 10 ))
  for k in $(seq 1 $cnt); do R "${ports[$i]}" DEL "sc_${probe}_w${i}k${k}" >/dev/null; done
done

# 3. throughput scales: single-worker baseline vs all-workers aggregate
rps() { redis-benchmark $T -p "$1" -n 40000 -c 50 -P 8 -q -t SET 2>/dev/null \
        | grep -oE '[0-9]+\.[0-9]+ requests per second' | grep -oE '^[0-9]+'; }
single=$(rps "${ports[0]}")
tmp=$(mktemp -d)
for pp in "${ports[@]}"; do ( rps "$pp" > "$tmp/$pp" ) & done
wait
agg=0
for pp in "${ports[@]}"; do agg=$(( agg + $(cat "$tmp/$pp" 2>/dev/null || echo 0) )); done
rm -rf "$tmp"
echo "  INFO  single worker: ${single} SET/s   |   $n workers aggregate: ${agg} SET/s"
# aggregate should clear ~1.8x a single worker (real horizontal scaling)
chk "aggregate throughput scales past a single worker" "1" \
    "$([ "${agg:-0}" -gt $(( ${single:-1} * 18 / 10 )) ] && echo 1 || echo 0)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
