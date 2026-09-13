#!/usr/bin/env bash
# A worker-count change is reported rather than silent (#101, first slice).
#
# Keys map to workers by CRC16 slot in contiguous ranges, and changing
# pg_keyspace.workers is a restart. supacache.kv.slot is stable, so nothing is
# lost -- but most of the persisted keyspace comes back into a DIFFERENT
# worker's segment, which is that much of the warm cache dropped and
# re-recovered. Today that happens with no signal at all: an operator who
# doubles the worker count and then watches the hit rate collapse for an hour
# has nothing connecting the two.
#
# This is step 1 of #101 and deliberately only that: the layout is recorded and
# a change is reported. It does NOT move slots between running workers -- no
# slot map to consult, no migrating/importing states, no ASK redirection. Those
# are the rest of the issue.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-topo-data}
PORT=${PGKS_PG_PORT:-5447}
RESP=${PGKS_RESP_PORT:-6407}
MAX_WORKERS=4
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
set_conf() {
  sed -i "s|^$1 = .*|$1 = $2|" $PGDATA/postgresql.conf 2>/dev/null
  grep -q "^$1 " $PGDATA/postgresql.conf || echo "$1 = $2" >> $PGDATA/postgresql.conf
}
# Wait until `n` slot workers are actually serving RESP.
#
# A persisted worker serves NOTHING until its startup recovery finishes -- the
# log says so itself ("no RESP traffic is served until this finishes"). Sleeping
# a fixed few seconds and then probing measures recovery in progress: the first
# version of this harness did exactly that and reported 99 of 400 keys
# "missing", which was four workers still recovering, not data loss.
wait_workers() {
  local want=$1 i
  for i in $(seq 1 90); do
    [ "$(grep -c 'RESP listening' $PGDATA/log 2>/dev/null)" -ge "$want" ] && \
    [ "$(grep -c 'recovered .* keys' $PGDATA/log 2>/dev/null)" -ge "$want" ] && { echo 1; return; }
    sleep 1
  done
  echo "0 [after 90s: listening=$(grep -c 'RESP listening' $PGDATA/log 2>/dev/null) recovered=$(grep -c 'recovered .* keys' $PGDATA/log 2>/dev/null), wanted $want]"
}
restart_with_workers() {
  stop_pg; sleep 1
  set_conf "pg_keyspace.workers" "$1"
  : > $PGDATA/log; chown postgres:postgres $PGDATA/log
  start_pg; wait_ready
}
# Is a key readable anywhere in the cluster? Asked port by port rather than
# through `redis-cli -c`: the point is whether the data came back, not whether
# this client follows MOVED.
key_reachable() {
  local k=$1 p
  for p in $(seq $RESP $((RESP + MAX_WORKERS - 1))); do
    [ -n "$(redis-cli -p $p GET "$k" 2>/dev/null)" ] && return 0
  done
  return 1
}

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/topo_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/topo_install.log; exit 1; }
echo "installed"

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.workers = 2"
  echo "pg_keyspace.keys = 20000"
  # Required: a persisted tier with workers > 1 redirects clients by address, so
  # the workers REFUSE to start without it and park instead. Without this the
  # whole run measures a cluster whose workers never came up -- which is how the
  # first version of this harness failed, reporting an empty topology row as
  # though the feature did not work.
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
psql_ "CREATE EXTENSION pg_keyspace" >/dev/null
restart_with_workers 2

echo
echo "########## 1. the running layout is recorded ##########"
# Guard first. Everything below reads state the workers write during startup
# recovery, so a cluster whose workers parked would fail every assertion for a
# reason that has nothing to do with topology.
chk "the slot workers actually started" "0" \
    "$(grep -c 'REFUSING to start' $PGDATA/log 2>/dev/null | tr -d '[:space:]')"
chk "and both of them finished recovery and are serving" "1" "$(wait_workers 2)"
chk "the topology table exists" "1" \
    "$(psql_ "SELECT count(*) FROM pg_tables WHERE schemaname='supacache' AND tablename='topology'")"
chk "and records the running worker count" "2" \
    "$(psql_ "SELECT workers FROM supacache.topology WHERE id=1")"
chk "no change is reported when nothing changed" "2|2|0" \
    "$(psql_ "SELECT recorded_workers||'|'||running_workers||'|'||slots_moved FROM supacache.topology_change()" | tr -d '[:space:]')"
chk "and the log says nothing about a worker count change" "0" \
    "$(grep -c 'WORKER COUNT CHANGED' $PGDATA/log 2>/dev/null | tr -d '[:space:]')"

echo
echo "########## 2. a real change is reported, with what it costs ##########"
# Write through the durable tier so there is a persisted keyspace for the
# change to actually move. Without this the section would report a number
# about a cluster holding nothing.
for i in $(seq 1 400); do
  redis-cli -p $RESP SET "k$i" "v$i" >/dev/null 2>&1
done
sleep 2
PERSISTED=$(psql_ "SELECT count(*) FROM supacache.kv")
chk "there is a persisted keyspace for the change to move" "1" \
    "$([ "${PERSISTED:-0}" -gt 100 ] && echo 1 || echo 0)"
DISTINCT_SLOTS=$(psql_ "SELECT count(DISTINCT slot) FROM supacache.kv")
chk "and those keys span many slots, not one" "1" \
    "$([ "${DISTINCT_SLOTS:-0}" -gt 50 ] && echo 1 || echo 0)"

restart_with_workers 4
chk "all four workers finished recovery and are serving" "1" "$(wait_workers 4)"
chk "the change is logged" "1" \
    "$(grep -c 'WORKER COUNT CHANGED 2 -> 4' $PGDATA/log 2>/dev/null | tr -d '[:space:]')"
# 2 -> 4 moves three quarters of the slots: contiguous ranges mean only worker
# 0's first sub-range keeps its owner. The intuitive "half" is wrong, which is
# exactly why reporting the real number is worth doing.
chk "the log names how much moved" "1" \
    "$(grep -c '12288 of 16384 slots (75.0%)' $PGDATA/log 2>/dev/null | tr -d '[:space:]')"
chk "the new layout is now what is recorded" "4" \
    "$(psql_ "SELECT workers FROM supacache.topology WHERE id=1")"
chk "and the change function agrees once settled" "4|4|0" \
    "$(psql_ "SELECT recorded_workers||'|'||running_workers||'|'||slots_moved FROM supacache.topology_change()" | tr -d '[:space:]')"

echo
echo "########## 3. no data was lost, which is the claim the warning makes ##########"
# The log tells the operator "no data is lost". That is only worth saying if it
# is true, so it is asserted rather than asserted-by-comment.
chk "every persisted key is still in the table" "$PERSISTED" \
    "$(psql_ "SELECT count(*) FROM supacache.kv")"
STILL=0
for i in $(seq 1 400); do
  key_reachable "k$i" && STILL=$((STILL+1))
done
chk "and every key still reads back from some worker ($STILL/400)" "400" "$STILL"

echo
echo "########## 4. the SQL routing table agrees with CLUSTER SLOTS ##########"
# The defect was the two surfaces disagreeing about the same function's output
# (#121). CLUSTER SLOTS/SHARDS/NODES all emit `hi - 1` because the Redis
# convention is inclusive; slot_ranges() emitted crc16::slot_range's half-open
# bounds raw, so adjacent workers OVERLAPPED -- worker 0 ending at 4096 and
# worker 1 starting at 4096. A client sharding from that table sent every
# boundary slot to the wrong worker.
#
# Four workers are running here, so there are three interior boundaries to get
# wrong.
OVERLAPS=$(psql_ "SELECT count(*) FROM (SELECT slot_hi, lead(slot_lo) OVER (ORDER BY worker) AS nxt FROM supacache.slot_ranges()) t WHERE nxt IS NOT NULL AND nxt <= slot_hi" | tr -d '[:space:]')
chk "adjacent workers' ranges do not overlap" "0" "$OVERLAPS"
GAPS=$(psql_ "SELECT count(*) FROM (SELECT slot_hi, lead(slot_lo) OVER (ORDER BY worker) AS nxt FROM supacache.slot_ranges()) t WHERE nxt IS NOT NULL AND nxt <> slot_hi + 1" | tr -d '[:space:]')
chk "and leave no gap between them" "0" "$GAPS"
chk "together they cover every slot exactly once" "16384" \
    "$(psql_ "SELECT sum(slot_hi - slot_lo + 1) FROM supacache.slot_ranges()" | tr -d '[:space:]')"
# The agreement itself, which nothing compared before.
SQL_RANGES=$(psql_ "SELECT string_agg(slot_lo||'-'||slot_hi, ',' ORDER BY worker) FROM supacache.slot_ranges()" | tr -d '[:space:]')
# Parsed from CLUSTER NODES, not CLUSTER SLOTS: the latter is a nested array
# that redis-cli flattens into bare integers, so scraping it also picks up the
# port numbers and produces nonsense. NODES is line-oriented --
# "<id> <ip:port@bus> <flags> - 0 0 <worker> connected <lo>-<hi>" -- so the
# range is the last field and the worker index is field 7.
RESP_RANGES=$(redis-cli -p $RESP CLUSTER NODES 2>/dev/null \
  | awk 'NF>=9 {print $7" "$NF}' | sort -n | awk '{printf "%s%s", (NR>1?",":""), $2}')
chk "the SQL table and CLUSTER NODES report identical ranges" "$RESP_RANGES" "$SQL_RANGES"
chk "and the ranges are non-empty, so that comparison means something" "1" \
    "$([ -n "$SQL_RANGES" ] && echo 1 || echo 0)"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
