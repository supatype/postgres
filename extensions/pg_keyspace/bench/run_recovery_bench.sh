#!/usr/bin/env bash
# Recovery cost against persisted key count (#42).
#
# What recovery does is load every persisted key for a worker's slot range into
# the segment before the RESP port opens, so its cost is a startup outage and a
# memory spike, not a throughput number. The README extrapolates ~3.5us per key
# from a 1M run and has never been checked past that, and the memory side has
# never been measured at all — which is the half that actually bounds how large
# a keyspace can be restarted safely.
#
# Rows go into supacache.kv with SQL rather than through RESP on purpose. The
# durable tier holds each ack until its record commits, so populating a million
# keys over RESP is hours of sequential round trips; recovery reads the table,
# so filling the table directly measures the same path with the setup cost gone.
#
# Reports, per key count: recovery wall time as the worker itself logged it,
# keys restored, and the worker's peak RSS. A zero-row row at the same sizing is
# measured first, because peak RSS includes the segment pages the load touches
# and only the difference from that baseline is the cost of loading.
set -u
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-recovery-bench}
PORT=${PGKS_PG_PORT:-5466}
RESP=${PGKS_RESP_PORT:-6436}
PROFILE=${PGKS_BUILD_PROFILE:-release}
# Sized so the whole persisted set fits: eviction during load would make the
# timing a measurement of a partial cache rather than of recovery.
VAL_BYTES=${PGKS_BENCH_VAL_BYTES:-64}
KEY_COUNTS=${PGKS_BENCH_KEYS:-"0 100000 1000000"}
SKIP_BUILD=${PGKS_BENCH_SKIP_BUILD:-0}

psql_() { timeout 600 $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 120); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
set_conf() {
  sed -i "s/^$1 = .*/$1 = $2/" $PGDATA/postgresql.conf 2>/dev/null
  grep -q "^$1" $PGDATA/postgresql.conf || echo "$1 = $2" >> $PGDATA/postgresql.conf
}
# Workers outlive a killed postmaster and keep the ports, which makes the next
# cluster look alive while answering nothing. Clear them before starting.
kill_stragglers() {
  ps -eo pid,args | grep -E "pg_keyspace|postgres -D $PGDATA" \
    | grep -v -E "grep|eval|snapshot|claude" | awk '{print $1}' \
    | while read -r p; do kill -9 "$p" 2>/dev/null; done
  sleep 2
}

if [ "$SKIP_BUILD" != "1" ]; then
  echo "=== build + install the extension ==="
  cd "$EXT_DIR"
  REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
  cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/pgks_bench_install.log 2>&1 || {
    echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/pgks_bench_install.log; exit 1; }
  echo "installed ($PROFILE)"
fi

printf '\n%-12s  %-10s  %-14s  %-12s  %s\n' "keys" "recovered" "recovery time" "peak RSS" "RSS over baseline"
printf -- '---------------------------------------------------------------------------------\n'

BASE_RSS=""
for N in $KEY_COUNTS; do
  kill_stragglers
  rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
  su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
  # 20% headroom over the row count so nothing is evicted while loading.
  KEYS=$(( N + (N / 5) + 1024 ))
  {
    echo "shared_preload_libraries = 'pg_keyspace'"
    echo "pg_keyspace.port = $RESP"
    echo "pg_keyspace.require_mask = off"
    echo "pg_keyspace.durability = 'durable'"
    echo "pg_keyspace.keys = $KEYS"
    echo "pg_keyspace.val_bytes = $VAL_BYTES"
    echo "pg_keyspace.workers = 1"
    echo "pg_keyspace.persist_workers = 1"
    echo "maintenance_work_mem = '256MB'"
  } >> $PGDATA/postgresql.conf
  chown -R postgres:postgres $PGDATA
  start_pg; wait_ready || { echo "cluster did not start for N=$N"; continue; }
  psql_ "CREATE EXTENSION pg_keyspace;" >/dev/null
  stop_pg; sleep 1; start_pg; wait_ready || { echo "cluster did not restart for N=$N"; continue; }
  sleep 2

  if [ "$N" -gt 0 ]; then
    # slot is only read by the sharded scan, and this runs one worker, which
    # takes the unfiltered path — so a constant is honest here rather than a
    # CRC16 the benchmark would have to reimplement in SQL.
    psql_ "INSERT INTO supacache.kv (tenant, key, slot, kind, val, expires_at)
           SELECT '', ('k'||g)::bytea, 0, 's', repeat('v', $VAL_BYTES)::bytea, 0
           FROM generate_series(1, $N) g" >/dev/null
    ROWS=$(psql_ "SELECT count(*) FROM supacache.kv")
    [ "$ROWS" = "$N" ] || echo "  (warning: inserted $ROWS rows, wanted $N)"
  fi

  LOG_MARK=$(wc -l < $PGDATA/log)
  stop_pg; sleep 1; start_pg; wait_ready || { echo "cluster did not come back for N=$N"; continue; }
  sleep 3

  NEW=$(tail -n +$((LOG_MARK+1)) $PGDATA/log)
  # "recovered N keys (slots a..b) from supacache.kv in 11.09ms" — the worker
  # times itself, so this is the load alone rather than the whole startup.
  LINE=$(echo "$NEW" | grep -o "recovered [0-9]* keys .* in .*" | tail -1)
  GOT=$(echo "$LINE" | sed -n 's/^recovered \([0-9]*\) keys.*/\1/p')
  TOOK=$(echo "$LINE" | sed -n 's/.* in \(.*\)$/\1/p')
  WPID=$(ps -eo pid,args | grep "pg_keyspace: RESP slot worker 0" | grep -v grep | awk '{print $1}' | head -1)
  RSS_KB=$(awk '/VmHWM/{print $2}' /proc/$WPID/status 2>/dev/null)
  RSS_MB=$(( ${RSS_KB:-0} / 1024 ))
  if [ "$N" -eq 0 ]; then BASE_RSS=$RSS_MB; DELTA="baseline";
  else DELTA="$(( RSS_MB - ${BASE_RSS:-0} )) MB"; fi

  printf '%-12s  %-10s  %-14s  %-12s  %s\n' \
    "$N" "${GOT:-?}" "${TOOK:-?}" "${RSS_MB} MB" "$DELTA"
done

kill_stragglers
rm -rf $PGDATA
echo ""
echo "peak RSS is VmHWM of the RESP worker, which includes the segment pages the"
echo "load touches; the baseline row is the same sizing with an empty table, so"
echo "the last column is what loading the rows actually cost."
