#!/usr/bin/env bash
# BF.*/CF.* benchmark: the pg_keyspace daemon against a real Redis 8 on the same
# host. Closed-loop p50 and pipelined throughput, median of 3 runs per cell.
set -u

HERE=$(cd -- "$(dirname -- "$0")" && pwd)
DAEMON=${DAEMON:-$HERE/../core/target/release/pgkeyspaced}
PORT=${PORT:-6403}
REDIS_PORT=${REDIS_PORT:-6382}
CONTAINER=${CONTAINER:-pgks-redis8-bench}
REDIS_IMAGE=${REDIS_IMAGE:-redis:8}
REPS=${REPS:-3}
CLOSED_N=${CLOSED_N:-200000}
PIPE_N=${PIPE_N:-1000000}
CLIENTS=${CLIENTS:-50}
PIPELINE=${PIPELINE:-16}
BF_CAP=${BF_CAP:-1000000}
BF10_CAP=${BF10_CAP:-10000000}
CF_CAP=${CF_CAP:-1000000}
KEYS_PER_WORKER=${KEYS_PER_WORKER:-200000}
VAL_BYTES=${VAL_BYTES:-4096}
SHMEM_MB=${SHMEM_MB:-2048}
DATASIZE=${DATASIZE:-512}
KEYSPACE=${KEYSPACE:-100000}
LOG=${LOG:-/tmp/pgks_prob_bench_$(date -u +%Y%m%dT%H%M%SZ).log}

command -v redis-benchmark >/dev/null 2>&1 || { echo "  SKIP  redis-benchmark not installed"; exit 0; }
command -v redis-cli >/dev/null 2>&1 || { echo "  SKIP  redis-cli not installed"; exit 0; }
[ -x "$DAEMON" ] || { echo "  SKIP  daemon not built at $DAEMON"; exit 0; }

DPID=""
STARTED_REDIS=0
cleanup() {
  [ -n "$DPID" ] && kill "$DPID" 2>/dev/null
  [ -n "$DPID" ] && rm -f "/dev/shm/pgks_w0_$DPID"
  [ "$STARTED_REDIS" = 1 ] && docker rm -f "$CONTAINER" >/dev/null 2>&1
  return 0
}
trap cleanup EXIT

if ! redis-cli -p "$REDIS_PORT" PING 2>/dev/null | grep -q PONG; then
  command -v docker >/dev/null 2>&1 || { echo "  SKIP  no Redis on :$REDIS_PORT and no docker"; exit 0; }
  docker rm -f "$CONTAINER" >/dev/null 2>&1
  docker run --rm -d --name "$CONTAINER" -p "$REDIS_PORT":6379 "$REDIS_IMAGE" >/dev/null 2>&1 \
    || { echo "  SKIP  could not start $REDIS_IMAGE"; exit 0; }
  STARTED_REDIS=1
  for _ in $(seq 1 40); do redis-cli -p "$REDIS_PORT" PING 2>/dev/null | grep -q PONG && break; sleep 0.5; done
fi
redis-cli -p "$REDIS_PORT" PING 2>/dev/null | grep -q PONG || { echo "  SKIP  no Redis 8 on :$REDIS_PORT"; exit 0; }
redis-cli -p "$REDIS_PORT" BF.RESERVE probe_support 0.01 100 2>/dev/null | grep -q OK \
  || { echo "  SKIP  Redis on :$REDIS_PORT has no BF.* commands"; exit 0; }
redis-cli -p "$REDIS_PORT" DEL probe_support >/dev/null
HAVE_CTL=0
docker exec "$CONTAINER" test -x /usr/local/bin/redis-benchmark >/dev/null 2>&1 && HAVE_CTL=1

"$DAEMON" --workers 1 --port "$PORT" --keys-per-worker "$KEYS_PER_WORKER" \
  --val-bytes "$VAL_BYTES" --shmem-mb "$SHMEM_MB" >"$LOG.daemon" 2>&1 &
DPID=$!
for _ in $(seq 1 40); do redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG && break; sleep 0.25; done
redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  daemon did not start"; cat "$LOG.daemon"; exit 1; }

both() { for p in "$PORT" "$REDIS_PORT"; do redis-cli -p "$p" "$@" >/dev/null; done; }

med() { printf '%s\n' "$@" | sort -g | awk '{a[NR]=$1} END{print a[int((NR+1)/2)]}'; }

ratio() { awk -v a="$1" -v b="$2" 'BEGIN{ if (b+0==0) print "n/a"; else printf "%.2f", a/b }'; }

p50_of() {
  awk '/latency summary/ {getline; getline; print $3; exit}
       /milliseconds$/ {p=$1; sub("%","",p); if (p+0>=50) {print $3; exit}}'
}

rps_of() {
  awk '/requests per second/ {for (i=1;i<=NF;i++) if ($i=="requests") r=$(i-1)}
       END {printf "%.2f", r+0}'
}

measure() {
  local port=$1 tag=$2; shift 2
  local raw p50 rps
  raw=$(redis-benchmark -p "$port" "$@" 2>&1 | tr '\r' '\n')
  p50=$(printf '%s\n' "$raw" | p50_of)
  rps=$(printf '%s\n' "$raw" | rps_of)
  {
    echo "### $tag  port=$port  args: $*"
    printf '%s\n' "$raw" | grep -E "requests completed in|requests per second"
    echo "p50=${p50:-nan} ms  rps=${rps:-nan}"
    printf '%s\n' "$raw" | grep -E "ERR |WRONGTYPE" | grep -v "failed to fetch CONFIG" | sort -u | head -3
    echo
  } >>"$LOG"
  echo "${p50:-nan}|${rps:-nan}"
}

ROWS=()
row() {
  local label=$1 field=$2 range=$3; shift 3
  local pg=() rd=() i r
  for i in $(seq 1 "$REPS"); do
    r=$(measure "$PORT" "$label pgks rep$i" -r "$range" "$@"); pg+=("$(echo "$r" | cut -d'|' -f"$field")")
    r=$(measure "$REDIS_PORT" "$label redis rep$i" -r "$range" "$@"); rd+=("$(echo "$r" | cut -d'|' -f"$field")")
  done
  local a b
  a=$(med "${pg[@]}"); b=$(med "${rd[@]}")
  MED_PG=$a; MED_RD=$b
  ROWS+=("$label|$a|$b|$(ratio "$a" "$b")")
  printf "  done  %-34s pgks=%-12s redis=%s\n" "$label" "$a" "$b" >&2
}

ctl_measure() {
  local tag=$1; shift
  local raw p50 rps
  raw=$(docker exec "$CONTAINER" redis-benchmark -p 6379 "$@" 2>&1 | tr '\r' '\n')
  p50=$(printf '%s\n' "$raw" | p50_of)
  rps=$(printf '%s\n' "$raw" | rps_of)
  {
    echo "### $tag  in-container  args: $*"
    printf '%s\n' "$raw" | grep -A2 -E "requests completed in|latency summary"
    echo "p50=${p50:-nan} ms  rps=${rps:-nan}"
    echo
  } >>"$LOG"
  echo "${p50:-nan}|${rps:-nan}"
}

CTLROWS=()
ctl_row() {
  local label=$1 field=$2 range=$3; shift 3
  [ "$HAVE_CTL" = 1 ] || return 0
  local v=() i r
  for i in $(seq 1 "$REPS"); do
    r=$(ctl_measure "$label in-container rep$i" -r "$range" "$@"); v+=("$(echo "$r" | cut -d'|' -f"$field")")
  done
  local c; c=$(med "${v[@]}")
  CTLROWS+=("$label|$MED_PG|$MED_RD|$c|$(ratio "$MED_PG" "$c")")
}

closed() {
  local label=$1 range=$2; shift 2
  row "$label p50 (ms)" 1 "$range" -c 1 -n "$CLOSED_N" --precision 3 -e "$@"
  ctl_row "$label p50 (ms)" 1 "$range" -c 1 -n "$CLOSED_N" --precision 3 -e "$@"
}

piped() {
  local label=$1 range=$2; shift 2
  row "$label thrpt (ops/s)" 2 "$range" -c "$CLIENTS" -P "$PIPELINE" -n "$PIPE_N" --precision 3 -e "$@"
  ctl_row "$label thrpt (ops/s)" 2 "$range" -c "$CLIENTS" -P "$PIPELINE" -n "$PIPE_N" --precision 3 -e "$@"
}

echo "# pg_keyspace BF.*/CF.* benchmark — $(date -u +%FT%TZ)"
echo "host: $(nproc) cpus, $(uname -sr)"
echo "redis: $(redis-cli -p "$REDIS_PORT" INFO server 2>/dev/null | tr -d '\r' | grep -oE 'redis_version:[0-9.]+')"
echo "daemon flags: --workers 1 --port $PORT --keys-per-worker $KEYS_PER_WORKER --val-bytes $VAL_BYTES --shmem-mb $SHMEM_MB"
echo "log: $LOG"
echo

both DEL bf bf10 cf
both BF.RESERVE bf 0.01 "$BF_CAP"
both BF.RESERVE bf10 0.01 "$BF10_CAP"
both CF.RESERVE cf "$CF_CAP"
BF_SZ_PG=$(redis-cli -p "$PORT" BF.INFO bf SIZE)
BF_SZ_RD=$(redis-cli -p "$REDIS_PORT" BF.INFO bf SIZE)
BF10_SZ_PG=$(redis-cli -p "$PORT" BF.INFO bf10 SIZE)
BF10_SZ_RD=$(redis-cli -p "$REDIS_PORT" BF.INFO bf10 SIZE)
CF_SZ_PG=$(redis-cli -p "$PORT" CF.INFO cf | sed -n 2p)
CF_SZ_RD=$(redis-cli -p "$REDIS_PORT" CF.INFO cf | sed -n 2p)

closed "BF.ADD 1M" "$BF_CAP" BF.ADD bf item:__rand_int__
piped  "BF.ADD 1M" "$BF_CAP" BF.ADD bf item:__rand_int__
closed "BF.EXISTS 1M" "$BF_CAP" BF.EXISTS bf item:__rand_int__
piped  "BF.EXISTS 1M" "$BF_CAP" BF.EXISTS bf item:__rand_int__

closed "BF.ADD 10M" "$BF10_CAP" BF.ADD bf10 item:__rand_int__
piped  "BF.ADD 10M" "$BF10_CAP" BF.ADD bf10 item:__rand_int__
closed "BF.EXISTS 10M" "$BF10_CAP" BF.EXISTS bf10 item:__rand_int__
piped  "BF.EXISTS 10M" "$BF10_CAP" BF.EXISTS bf10 item:__rand_int__

closed "CF.ADD 1M" "$CF_CAP" CF.ADD cf item:__rand_int__
piped  "CF.ADD 1M" "$CF_CAP" CF.ADD cf item:__rand_int__
closed "CF.EXISTS 1M" "$CF_CAP" CF.EXISTS cf item:__rand_int__
piped  "CF.EXISTS 1M" "$CF_CAP" CF.EXISTS cf item:__rand_int__
closed "CF.DEL 1M" "$CF_CAP" CF.DEL cf item:__rand_int__
piped  "CF.DEL 1M" "$CF_CAP" CF.DEL cf item:__rand_int__

closed "SET (baseline)" "$KEYSPACE" -d "$DATASIZE" -t set
closed "GET (baseline)" "$KEYSPACE" -d "$DATASIZE" -t get

closed "PING" "$KEYSPACE" -t ping

ROWS+=("BF.INFO bf SIZE (bytes)|$BF_SZ_PG|$BF_SZ_RD|$(ratio "$BF_SZ_PG" "$BF_SZ_RD")")
ROWS+=("BF.INFO bf10 SIZE (bytes)|$BF10_SZ_PG|$BF10_SZ_RD|$(ratio "$BF10_SZ_PG" "$BF10_SZ_RD")")
ROWS+=("CF.INFO cf Size (bytes)|$CF_SZ_PG|$CF_SZ_RD|$(ratio "$CF_SZ_PG" "$CF_SZ_RD")")

both DEL bfe cfe
both BF.RESERVE bfe 0.01 "$BF_CAP"
both CF.RESERVE cfe "$CF_CAP"
DIAG=()
for srv in "pg_keyspace:$PORT" "Redis 8:$REDIS_PORT"; do
  lbl=${srv%%:*}; prt=${srv##*:}
  e=$(measure "$prt" "diag BF.EXISTS empty $lbl" -r "$BF_CAP" -c "$CLIENTS" -P "$PIPELINE" -n "$PIPE_N" --precision 3 -e BF.EXISTS bfe item:__rand_int__)
  f=$(measure "$prt" "diag BF.EXISTS full $lbl" -r "$BF_CAP" -c "$CLIENTS" -P "$PIPELINE" -n "$PIPE_N" --precision 3 -e BF.EXISTS bf item:__rand_int__)
  DIAG+=("BF.EXISTS thrpt (ops/s)|$lbl|$(echo "$e" | cut -d'|' -f2)|$(echo "$f" | cut -d'|' -f2)")
  e=$(measure "$prt" "diag CF.EXISTS empty $lbl" -r "$CF_CAP" -c "$CLIENTS" -P "$PIPELINE" -n "$PIPE_N" --precision 3 -e CF.EXISTS cfe item:__rand_int__)
  f=$(measure "$prt" "diag CF.EXISTS full $lbl" -r "$CF_CAP" -c "$CLIENTS" -P "$PIPELINE" -n "$PIPE_N" --precision 3 -e CF.EXISTS cf item:__rand_int__)
  DIAG+=("CF.EXISTS thrpt (ops/s)|$lbl|$(echo "$e" | cut -d'|' -f2)|$(echo "$f" | cut -d'|' -f2)")
done

{
  echo "=== end state"
  for p in "$PORT" "$REDIS_PORT"; do
    echo "-- port $p"
    for k in bf bf10; do echo "BF.INFO $k:"; redis-cli -p "$p" BF.INFO "$k" | paste - -; done
    echo "CF.INFO cf:"; redis-cli -p "$p" CF.INFO cf | paste - -
  done
} >>"$LOG"

echo
echo "| operation | pg_keyspace | Redis 8 | ratio |"
echo "|---|---:|---:|---:|"
printf '%s\n' "${ROWS[@]}" | awk -F'|' '{printf "| %s | %s | %s | %s |\n", $1, $2, $3, $4}'
echo
echo "ratio = pg_keyspace / Redis 8. Lower is better for p50 and size, higher is better for throughput."
echo "closed-loop: -c 1 -n $CLOSED_N --precision 3 -e ; pipelined: -c $CLIENTS -P $PIPELINE -n $PIPE_N"
echo "baseline rows: -d $DATASIZE -r $KEYSPACE ; median of $REPS runs per cell."
if [ "${#CTLROWS[@]}" -gt 0 ]; then
  echo
  echo "Network control: Redis 8 answers on a docker published port, pg_keyspace on the host loopback."
  echo "The last column removes the published-port cost. It runs redis-benchmark inside the container."
  echo
  echo "| operation | pg_keyspace | Redis 8 (published port) | Redis 8 (in container) | ratio |"
  echo "|---|---:|---:|---:|---:|"
  printf '%s\n' "${CTLROWS[@]}" | awk -F'|' '{printf "| %s | %s | %s | %s | %s |\n", $1, $2, $3, $4, $5}'
fi
echo
echo "Diagnostic: the same read on an empty filter and on the filter the rows above filled."
echo "It separates the hash cost from the memory and sub-filter cost. One run per cell."
echo
echo "| operation | server | empty filter | filled filter |"
echo "|---|---|---:|---:|"
printf '%s\n' "${DIAG[@]}" | awk -F'|' '{printf "| %s | %s | %s | %s |\n", $1, $2, $3, $4}'
echo
echo "end-state BF.INFO/CF.INFO for both servers: $LOG"
echo "raw summary lines: $LOG"
