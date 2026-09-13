#!/bin/bash
# In-Postgres durability proof for the probabilistic filters. A Bloom filter and
# a Cuckoo filter written in the `durable` tier must survive kill -9 and come
# back with their type and their contents. A filter with a TTL must stay gone.
# Own cluster and own ports, so it runs beside run_durability_pg.sh. Run as root:
#   sudo -E env "PATH=$PATH" "PGRX_HOME=$HOME/.pgrx" PGKS_BUILD_PROFILE=debug bash extensions/pg_keyspace/bench/run_prob_durability_pg.sh
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-prob-data}
PORT=${PGKS_PG_PORT:-5434}
RESP=${PGKS_RESP_PORT:-6398}
PROFILE=${PGKS_BUILD_PROFILE:-release}
RCLI_TIMEOUT=${PGKS_RCLI_TIMEOUT:-20}
PGOPTS="-c statement_timeout=120s -c lock_timeout=60s"
N=10000
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { PGOPTIONS="$PGOPTS" $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
rcli() { timeout "$RCLI_TIMEOUT" redis-cli -p $RESP "$@" 2>&1; }
cfi() { rcli CF.INFO "$1" | awk -v f="$2" '$0==f{getline; print; exit}'; }
fill() {
  awk -v c="$1" -v k="$2" -v n="$3" 'BEGIN{
    for (i = 1; i <= n; i++) {
      it = "item:" i
      printf "*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", length(c), c, length(k), k, length(it), it
    }}' | timeout 120 redis-cli -p $RESP --pipe >/dev/null 2>&1
}
items() { seq "$1" "$2" "$3" | sed 's/^/item:/'; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 30); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
# The run leaves the cluster up after the restart check. Stop it and drop the
# data directory on every exit path, so the next CI step finds no postmaster.
cleanup() { stop_pg; rm -rf "$PGDATA"; }
trap cleanup EXIT
wait_resp()  { for _ in $(seq 1 30); do rcli PING | grep -q PONG && return 0; sleep 1; done; return 1; }

echo "=== build + install the extension ==="
cd "$EXT_DIR" || exit 1
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/pgks-prob-install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/pgks-prob-install.log; exit 1; }
echo "installed"

echo "=== initdb ==="
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.keys = 20000"
  echo "pg_keyspace.val_bytes = 512"
  echo "pg_keyspace.ring_mb = 8"
  echo "pg_keyspace.persist_window_ms = 10"
} >> $PGDATA/postgresql.conf

start_pg; wait_ready
psql_ "CREATE EXTENSION pg_keyspace;" >/dev/null
stop_pg; sleep 1
start_pg
wait_ready || { echo "cluster did not start"; tail -20 $PGDATA/log; exit 1; }
wait_resp  || { echo "RESP port did not open"; tail -20 $PGDATA/log; exit 1; }
sleep 2

echo ""
echo "########## A. write the filters into the durable tier ##########"
chk "BF.RESERVE bf"                     "OK"  "$(rcli BF.RESERVE bf 0.01 20000)"
fill BF.ADD bf $N
chk "BF.CARD counts every add"          "$N"  "$(rcli BF.CARD bf)"
chk "CF.RESERVE cf"                     "OK"  "$(rcli CF.RESERVE cf 20000)"
fill CF.ADD cf $N
chk "CF.DEL removes one item"           "1"   "$(rcli CF.DEL cf item:7)"
chk "BF.RESERVE g for growth"           "OK"  "$(rcli BF.RESERVE g 0.01 100)"
fill BF.ADD g 300
chk "growth added a sub-filter"         "2"   "$(rcli BF.INFO g FILTERS)"
chk "SET a string for the type guard"   "OK"  "$(rcli SET s v)"
BF_FILTERS=$(rcli BF.INFO bf FILTERS)
sleep 1
chk "the bloom filter persists as kind b" "b" \
    "$(psql_ "SELECT kind FROM supacache.kv WHERE key='bf'::bytea")"

echo ""
echo "########## B. a filter with a TTL ##########"
chk "BF.RESERVE t"                      "OK"  "$(rcli BF.RESERVE t 0.01 100)"
chk "BF.ADD t x"                        "1"   "$(rcli BF.ADD t x)"
chk "EXPIRE t 2"                        "1"   "$(rcli EXPIRE t 2)"
sleep 1
chk "the TTL filter persists as kind b" "b" \
    "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='t'::bytea")"
sleep 3

echo ""
echo "########## C. kill -9, restart, recover ##########"
kill -9 "$(head -1 $PGDATA/postmaster.pid)" 2>/dev/null
pkill -9 -u postgres 2>/dev/null
for _ in $(seq 1 30); do pgrep -u postgres >/dev/null || break; sleep 1; done
rm -f $PGDATA/postmaster.pid
start_pg
if wait_ready && wait_resp; then
  sleep 3
  echo "  recovery log: $(grep -h "recovered .* keys" $PGDATA/log | tail -1)"
  mapfile -t SPREAD < <(items 100 100 10000)
  mapfile -t GROWN  < <(items 1 1 300)

  chk "TYPE bf is MBbloom--"            "MBbloom--" "$(rcli TYPE bf)"
  chk "BF.CARD survived the crash"      "$N"  "$(rcli BF.CARD bf)"
  chk "BF.EXISTS item:1"                "1"   "$(rcli BF.EXISTS bf item:1)"
  chk "BF.EXISTS item:5000"             "1"   "$(rcli BF.EXISTS bf item:5000)"
  chk "BF.EXISTS item:10000"            "1"   "$(rcli BF.EXISTS bf item:10000)"
  chk "BF.MEXISTS on 100 spread items"  "100" "$(rcli BF.MEXISTS bf "${SPREAD[@]}" | grep -c '^1$')"
  chk "BF.INFO FILTERS unchanged"       "$BF_FILTERS" "$(rcli BF.INFO bf FILTERS)"

  chk "TYPE cf is MBbloomCF"            "MBbloomCF" "$(rcli TYPE cf)"
  chk "the deleted item stayed deleted" "0"   "$(rcli CF.EXISTS cf item:7)"
  chk "its neighbour survived"          "1"   "$(rcli CF.EXISTS cf item:8)"
  chk "CF.COUNT item:1"                 "1"   "$(rcli CF.COUNT cf item:1)"
  chk "CF.INFO counts the live items"   "$((N-1))" "$(cfi cf 'Number of items inserted')"
  chk "CF.INFO items deleted"           "1"   "$(cfi cf 'Number of items deleted')"

  chk "the grown filter kept both sub-filters" "2" "$(rcli BF.INFO g FILTERS)"
  chk "all 300 grown items survived"    "300" "$(rcli BF.MEXISTS g "${GROWN[@]}" | grep -c '^1$')"

  chk "the expired filter is gone"      "0"   "$(rcli EXISTS t)"
  chk "and so is its item"              "0"   "$(rcli BF.EXISTS t x)"
  chk "BF.ADD on a recovered string is WRONGTYPE" "1" "$(rcli BF.ADD s x | grep -c WRONGTYPE)"
else
  echo "FAIL  cluster did not restart after kill -9"; fail=$((fail+1))
  tail -15 $PGDATA/log
fi

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
