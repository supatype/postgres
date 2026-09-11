#!/bin/bash
# In-Postgres durability validation for pg_keyspace.
#
# Unlike the other bench/ harnesses, which drive the standalone pgkeyspaced
# daemon, this one installs the extension into a real PostgreSQL 17, starts a
# cluster with it preloaded and exercises the paths that only exist in-process:
# the persistence tiers, crash recovery, the large-value reference path, slab
# reclamation, and the replicated tier's startup refusal.
#
# Expects to run where cargo, cargo-pgrx, a PGDG PostgreSQL 17 and redis-cli
# are all present, with the extension source mounted at /src. It creates and
# destroys a cluster at $PGDATA, so point that somewhere disposable.
#
# Prints "# result: N passed, M failed" like the other harnesses.
set -uo pipefail
PGBIN=/usr/lib/postgresql/17/bin
PGDATA=/pgdata
PORT=5433
RESP=6399
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
chk_contains() {
  if echo "$3" | grep -qF "$2"; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        wanted: [$2]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
rcli()  { redis-cli -p $RESP "$@" 2>&1; }
# Large values must come from stdin: a megabyte as an argv element exceeds
# ARG_MAX. redis-cli -x appends stdin as the final argument.
rcli_x() { local f="$1"; shift; redis-cli -p $RESP -x "$@" < "$f" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 30); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }

echo "=== build + install the extension ==="
cd /src/extension
cargo pgrx install --release --pg-config $PGBIN/pg_config >/tmp/install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/install.log; exit 1; }
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
start_pg; wait_ready; sleep 2

python3 -c "import sys;sys.stdout.write('x'*(1024*1024))" > /tmp/big.txt
python3 -c "import sys;sys.stdout.write('z'*(6*1024*1024))" > /tmp/mid.txt
python3 -c "import sys;sys.stdout.write('y'*(9*1024*1024))" > /tmp/huge.txt

echo ""
echo "########## A. baseline RESP + SQL share one keyspace ##########"
chk "RESP SET"            "OK"  "$(rcli SET foo bar)"
chk "RESP GET"            "bar" "$(rcli GET foo)"
chk "SQL sees same bytes" "bar" "$(psql_ "SELECT convert_from(supacache.get('foo'),'UTF8')")"

echo ""
echo "########## B. durable tier actually persists (#44) ##########"
rcli SET durable:k1 v1 >/dev/null; sleep 1
chk "row present in supacache.kv" "1"  "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='durable:k1'::bytea")"
chk "value correct in kv"         "v1" "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='durable:k1'::bytea")"

echo ""
echo "########## C. large value by reference (#48) ##########"
chk "RESP SET 1MiB"    "OK"      "$(rcli_x /tmp/big.txt SET big:1m)"
chk "RESP STRLEN 1MiB" "1048576" "$(rcli STRLEN big:1m)"
sleep 2
chk "1MiB persisted via reference path" "1048576" "$(psql_ "SELECT octet_length(val) FROM supacache.kv WHERE key='big:1m'::bytea")"
chk "1MiB bytes intact in kv"           "t"       "$(psql_ "SELECT val = repeat('x',1048576)::bytea FROM supacache.kv WHERE key='big:1m'::bytea")"

echo ""
echo "########## D. the real value ceiling ##########"
O9=$(rcli_x /tmp/huge.txt SET big:9m); echo "  9 MiB SET -> [$O9]"
chk "6 MiB SET accepted" "OK" "$(rcli_x /tmp/mid.txt SET big:6m)"
sleep 3
chk "6 MiB persisted" "6291456" "$(psql_ "SELECT octet_length(val) FROM supacache.kv WHERE key='big:6m'::bytea")"

echo ""
echo "########## F. oversized reclamation (#49) ##########"
U0=$(psql_ "SELECT data_used FROM supacache.stats()")
rcli_x /tmp/big.txt SET recl:1 >/dev/null
U1=$(psql_ "SELECT data_used FROM supacache.stats()")
rcli DEL recl:1 >/dev/null; sleep 2
U2=$(psql_ "SELECT data_used FROM supacache.stats()")
echo "  data_used: before=$U0 peak=$U1 after-del=$U2"
RET=$((U1-U2)); RESID=$((U2-U0))
echo "  returned on delete: $RET bytes; residual $RESID (the key own slab class)"
if [ "$U1" -gt "$U0" ] && [ "$RET" -gt 1000000 ] && [ "$RESID" -lt 1024 ]; then
  echo "PASS  oversized space returned to the bump pointer"; pass=$((pass+1))
else
  echo "FAIL  oversized space not reclaimed"; fail=$((fail+1)); fi

echo ""
echo "########## G. ring stats / no silent drops (#46) ##########"
chk "no dropped records during the run" "0" "$(psql_ "SELECT dropped FROM supacache.ring_stats()")"

echo ""
echo "########## H. replication_status on the durable tier (#47) ##########"
chk "replication_status: durable, no standby, honoured" "durable|false|0|true" \
    "$(psql_ "SELECT tier||'|'||standby_configured||'|'||sync_standbys_connected||'|'||honoured FROM supacache.replication_status()")"

echo ""
echo "########## E. crash recovery: kill -9, restart, values survive ##########"
rcli SET crash:k1 survive-me >/dev/null
rcli_x /tmp/big.txt SET crash:big >/dev/null
sleep 3
# Kill the whole cluster, not just the postmaster: a surviving backend keeps
# the shared memory segment attached and the restart then fails.
kill -9 "$(head -1 $PGDATA/postmaster.pid)" 2>/dev/null
pkill -9 -u postgres 2>/dev/null
for _ in $(seq 1 30); do pgrep -u postgres >/dev/null || break; sleep 1; done
rm -f $PGDATA/postmaster.pid
start_pg
if wait_ready; then
  sleep 3
  chk "small value survived kill -9" "survive-me" "$(rcli GET crash:k1)"
  chk "1MiB value survived kill -9"  "1048576"    "$(rcli STRLEN crash:big)"
  chk "6MiB value survived kill -9"  "6291456"    "$(rcli STRLEN big:6m)"
  chk "pre-crash key recovered"      "bar"        "$(rcli GET foo)"
  echo "  recovery log: $(grep -h "recovered .* keys" $PGDATA/log | tail -1)"
else
  echo "FAIL  cluster did not restart after kill -9"; fail=$((fail+1))
  tail -15 $PGDATA/log
fi

echo ""
echo "########## I. persist worker health ##########"
ERRS=$(grep -c "FAILED to persist" $PGDATA/log || true)
chk "no 'FAILED to persist' in log" "0" "$ERRS"
PANIC=$(grep -ci "panic" $PGDATA/log || true)
chk "no panics in log" "0" "$PANIC"

echo ""
echo "########## J. replicated tier refuses without a standby (#47) ##########"
stop_pg; sleep 1
sed -i "s/pg_keyspace.durability = 'durable'/pg_keyspace.durability = 'replicated'/" $PGDATA/postgresql.conf
start_pg; sleep 6
chk_contains "worker refuses replicated without synchronous_standby_names" "REFUSING to start" "$(cat $PGDATA/log)"
stop_pg; sleep 1

echo ""
echo "########## K. value larger than the whole arena ##########"
sed -i "s/pg_keyspace.durability = 'replicated'/pg_keyspace.durability = 'durable'/" $PGDATA/postgresql.conf
sed -i "s/pg_keyspace.keys = 20000/pg_keyspace.keys = 1024/" $PGDATA/postgresql.conf
sed -i "s/pg_keyspace.val_bytes = 512/pg_keyspace.val_bytes = 1/" $PGDATA/postgresql.conf
start_pg; wait_ready; sleep 2
ARENA=$(psql_ "SELECT data_cap FROM supacache.stats()")
echo "  arena data_cap now: $ARENA bytes; writing 6 MiB into it"
OUT=$(rcli_x /tmp/mid.txt SET toobig)
GOT=$(rcli STRLEN toobig)
echo "  SET -> [$OUT]   STRLEN -> [$GOT]"
case "$OUT" in
  OK) echo "FAIL  client got +OK for a value the store could not hold (STRLEN=$GOT)"; fail=$((fail+1)) ;;
  OOM*) echo "PASS  over-arena write refused with the Redis OOM error"; pass=$((pass+1)) ;;
  *) echo "FAIL  unexpected reply to an over-arena write: [$OUT]"; fail=$((fail+1)) ;;
esac

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
