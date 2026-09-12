#!/bin/bash
# In-Postgres durability validation for pg_keyspace.
#
# Unlike the other bench/ harnesses, which drive the standalone pgkeyspaced
# daemon, this one installs the extension into a real PostgreSQL 17, starts a
# cluster with it preloaded and exercises the paths that only exist in-process:
# the persistence tiers, crash recovery, the large-value reference path, slab
# reclamation, and the replicated tier's startup refusal.
#
# Expects cargo, cargo-pgrx, a PGDG PostgreSQL 17 and redis-cli on PATH. Paths
# are derived from this script's location, so it runs from a checkout or a
# mount. It creates and destroys a cluster at $PGDATA (default under /tmp), so
# point that somewhere disposable. Override PGBIN, PGDATA, PGKS_PG_PORT,
# PGKS_RESP_PORT, PGKS_BUILD_PROFILE and PGKS_RCLI_TIMEOUT as needed.
#
# Prints "# result: N passed, M failed" like the other harnesses.
set -uo pipefail
# Everything is overridable so this runs both in a container and on a CI
# runner. Paths are derived from the script's own location rather than assumed.
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-durability-data}
PORT=${PGKS_PG_PORT:-5433}
RESP=${PGKS_RESP_PORT:-6399}
# Release uses thin LTO with one codegen unit and takes upwards of ten minutes;
# correctness does not need the optimiser, so CI passes debug.
PROFILE=${PGKS_BUILD_PROFILE:-release}
# No client call may block indefinitely. A durable-tier reply is legitimately
# held until its record commits, so an unbounded call hangs the whole suite
# when anything goes wrong: an earlier run sat on one SET for over two hours.
RCLI_TIMEOUT=${PGKS_RCLI_TIMEOUT:-20}
# The same applies to the SQL side, and for a while it did not. A DROP
# TABLESPACE that waits on a procsignal barrier no backend will acknowledge
# waits forever, and an unbounded psql turned that into a suite that sat there
# for thirty-six minutes looking slow rather than broken. Every statement now
# has a ceiling, so a hang surfaces as a failed assertion naming the statement.
# Generous, because some of these are legitimately slow: recovery of a large
# keyspace, a base backup, a tablespace move.
PSQL_TIMEOUT=${PGKS_PSQL_TIMEOUT:-120s}
PSQL_LOCK_TIMEOUT=${PGKS_PSQL_LOCK_TIMEOUT:-60s}
PGOPTS="-c statement_timeout=$PSQL_TIMEOUT -c lock_timeout=$PSQL_LOCK_TIMEOUT"
# Topology under test. The multi-worker section below deliberately uses an
# uneven split (3 does not divide 16384) because the slot-range arithmetic is
# where off-by-one errors live.
MW_WORKERS=${PGKS_MW_WORKERS:-3}
MW_PERSIST=${PGKS_MW_PERSIST:-2}
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
chk_contains() {
  if echo "$3" | grep -qF "$2"; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        wanted: [$2]"; fail=$((fail+1)); fi
}
# grep -c prints "0" and still exits non-zero when it matches nothing, so
# `grep -c ... || echo 0` emits TWO zeros and every comparison against it fails
# with a confusing "expected [0], actual [0 0]". Count through this instead.
countlog() { grep -ci "$1" "$2" 2>/dev/null | head -1 || true; }
psql_() { PGOPTIONS="$PGOPTS" $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
rcli()  { timeout "$RCLI_TIMEOUT" redis-cli -p $RESP "$@" 2>&1; }
# Large values must come from stdin: a megabyte as an argv element exceeds
# ARG_MAX. redis-cli -x appends stdin as the final argument.
rcli_x() { local f="$1"; shift; timeout "$RCLI_TIMEOUT" redis-cli -p $RESP -x "$@" < "$f" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 30); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
# The row cache is only served while its invalidation worker is beating (#39),
# so anything asserting the cache is used has to wait for that rather than sleep
# a guessed amount. Returns 1 if it became coherent, 0 if it never did.
wait_coherent() {
  for _ in $(seq 1 "${1:-30}"); do
    [ "$(psql_ "SELECT coherent FROM supacache.rowcache_coherence()")" = "t" ] && { echo 1; return; }
    sleep 1
  done
  echo 0
}

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/install.log 2>&1 || {
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
if [ "$RESID" -lt 1024 ] && { [ "$U1" -eq "$U0" ] || [ "$RET" -gt 1000000 ]; }; then
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
echo "########## L. persistence failure holds the ack and loses nothing ##########"
# Restore a workable arena after the over-arena section.
stop_pg; sleep 1
sed -i "s/pg_keyspace.keys = 1024/pg_keyspace.keys = 20000/" $PGDATA/postgresql.conf
sed -i "s/pg_keyspace.val_bytes = 1$/pg_keyspace.val_bytes = 512/" $PGDATA/postgresql.conf
start_pg; wait_ready; sleep 2

# Fail exactly the statement bulk_upsert runs. NOT VALID leaves existing rows
# readable, so only new writes fail, and dropping it restores service.
psql_ "ALTER TABLE supacache.kv ADD CONSTRAINT fi_block CHECK (false) NOT VALID" >/dev/null
# In a sync-ack tier the reply is held until the record commits, so this must
# NOT come back with OK while persistence is broken.
OUT=$(timeout 6 redis-cli -p $RESP SET fi:k1 v1 2>&1); RC=$?
echo "  SET during failure -> [$OUT] (rc=$RC)"
if [ "$OUT" = "OK" ]; then
  echo "FAIL  ack released while persistence was failing"; fail=$((fail+1))
else
  echo "PASS  ack held while persistence was failing"; pass=$((pass+1)); fi
chk "nothing written to supacache.kv" "0" "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='fi:k1'::bytea")"
BACKLOG=$(psql_ "SELECT backlog_bytes FROM supacache.ring_stats()")
if [ "$BACKLOG" -gt 0 ]; then
  echo "PASS  record retained in the ring (backlog $BACKLOG bytes)"; pass=$((pass+1))
else
  echo "FAIL  record not retained in the ring (backlog $BACKLOG)"; fail=$((fail+1)); fi
if grep -qE "FAILED to persist|violates check constraint" $PGDATA/log; then
  echo "PASS  failure is visible in the log"; pass=$((pass+1))
else
  echo "FAIL  persistence failure left no trace in the log"; fail=$((fail+1)); fi

# Recovery: the SAME record must commit once persistence works again.
psql_ "ALTER TABLE supacache.kv DROP CONSTRAINT fi_block" >/dev/null
for _ in $(seq 1 20); do
  [ "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='fi:k1'::bytea")" = "1" ] && break
  sleep 1
done
ERRS=$(psql_ "SELECT failed_batches FROM supacache.ring_stats()")
if [ "$ERRS" -gt 0 ]; then
  echo "PASS  the failure is countable, not just loggable (errors=$ERRS)"; pass=$((pass+1))
else
  echo "FAIL  persistence failed but ring_stats().failed_batches stayed at 0"; fail=$((fail+1)); fi
chk "retained record commits after recovery" "1"  "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='fi:k1'::bytea")"
chk "and its value is correct"              "v1" "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='fi:k1'::bytea")"
chk "lag returns to zero once persistence recovers" "0" "$(psql_ "SELECT lag FROM supacache.ring_stats()")"

echo ""
echo "########## M. killing the persist worker loses nothing that was acked ##########"
# A background worker attached to shared memory dying abnormally makes Postgres
# restart the whole cluster, which recreates the shared segment. The ring lives
# there, so an un-acked record in flight is legitimately gone: it was never
# promised to anyone. What must survive is everything already acked, which is
# in supacache.kv. This pins that boundary exactly.
ACKED_BEFORE=$(psql_ "SELECT count(*) FROM supacache.kv")
psql_ "ALTER TABLE supacache.kv ADD CONSTRAINT fi_block CHECK (false) NOT VALID" >/dev/null
timeout 4 redis-cli -p $RESP SET fi:k2 v2 >/dev/null 2>&1   # times out: never acked
sleep 2
PW=""
for _ in $(seq 1 10); do
  PW=$(ps -eo pid,args | grep "[p]ersistence worker" | awk '{print $1}' | head -1)
  [ -n "$PW" ] && break
  sleep 1
done
if [ -n "$PW" ]; then
  echo "  killing persistence worker pid $PW"
  kill -9 "$PW" 2>/dev/null
else
  echo "  persistence worker already down (cycling on the injected failure)"
fi
sleep 8
# The cluster comes back by itself; wait for it rather than assuming.
for _ in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && break; sleep 1; done
psql_ "ALTER TABLE supacache.kv DROP CONSTRAINT fi_block" >/dev/null 2>&1
sleep 3
# fi:k2 was never acked, so BOTH outcomes are correct and which one occurs
# depends on whether the worker was alive to be killed:
#   killed   -> a shmem-attached worker dying restarts the cluster, the shared
#               segment is recreated, and the in-flight record is gone
#   cycling  -> the record stayed in the ring and commits once the injected
#               failure is lifted
# Asserting either one specifically would be flaky, so report it and assert
# only the invariant that must hold in both: nothing acked is lost.
INFLIGHT=$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='fi:k2'::bytea")
echo "  un-acked in-flight record after the kill: present=$INFLIGHT (either is correct)"
ACKED_AFTER=$(psql_ "SELECT count(*) FROM supacache.kv")
echo "  acked rows before=$ACKED_BEFORE after=$ACKED_AFTER"
if [ "$ACKED_AFTER" -ge "$ACKED_BEFORE" ]; then
  echo "PASS  every previously acked write survived the restart"; pass=$((pass+1))
else
  echo "FAIL  acked writes lost across the restart ($ACKED_BEFORE -> $ACKED_AFTER)"; fail=$((fail+1)); fi
chk "earlier durable value still readable" "v1"     "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='durable:k1'::bytea")"

echo "########## N. ring saturation parks, never drops ##########"
stop_pg; sleep 1
sed -i "s/pg_keyspace.ring_mb = 8/pg_keyspace.ring_mb = 1/" $PGDATA/postgresql.conf
start_pg; wait_ready; sleep 2
psql_ "ALTER TABLE supacache.kv ADD CONSTRAINT fi_block CHECK (false) NOT VALID" >/dev/null
# Fill the ring with the persist side wedged. Each client is parked, so run
# them with a timeout and in the background; none should report success.
OKS=0
python3 -c "import sys;sys.stdout.write('s'*40000)" > /tmp/sat.txt
for i in $(seq 1 40); do
  R=$(timeout 3 redis-cli -p $RESP -x SET "sat:$i" < /tmp/sat.txt 2>&1)
  [ "$R" = "OK" ] && OKS=$((OKS+1))
done
DROPPED=$(psql_ "SELECT dropped FROM supacache.ring_stats()")
echo "  writes reporting OK while wedged: $OKS ; ring dropped: $DROPPED"
chk "no write was silently dropped" "0" "$DROPPED"
if [ "$OKS" = "0" ]; then
  echo "PASS  every write parked rather than being acked"; pass=$((pass+1))
else
  echo "FAIL  $OKS write(s) acked while persistence was wedged"; fail=$((fail+1)); fi
psql_ "ALTER TABLE supacache.kv DROP CONSTRAINT fi_block" >/dev/null
sleep 8
chk "no panics after the whole run" "0" "$(grep -ci panic $PGDATA/log || true)"

echo ""
echo "########## O. multi-worker persistence ##########"
# The case the single-worker sections cannot reach. Values above INLINE_MAX are
# staged by reference and resolved out of a keyspace segment, and with several
# workers a key lives only in the segment of the worker owning its slot.
# Resolving against the wrong segment finds either the wrong bytes or nothing,
# and finding nothing is indistinguishable from a legitimately superseded
# write, so the failure would be silent. The only way to catch it is to write a
# large value to a key owned by a worker other than 0 and check it lands.
stop_pg; sleep 1
set_conf() {
  sed -i "s/^$1 = .*/$1 = $2/" $PGDATA/postgresql.conf 2>/dev/null
  grep -q "^$1" $PGDATA/postgresql.conf || echo "$1 = $2" >> $PGDATA/postgresql.conf
}
set_conf "pg_keyspace.workers" "$MW_WORKERS"
set_conf "pg_keyspace.persist_workers" "$MW_PERSIST"
set_conf "pg_keyspace.durability" "'durable'"
set_conf "pg_keyspace.cluster_announce_host" "'127.0.0.1'"
start_pg; wait_ready; sleep 5

UP=0
for w in $(seq 0 $((MW_WORKERS-1))); do
  timeout 5 redis-cli -p $((RESP+w)) PING 2>/dev/null | grep -q PONG && UP=$((UP+1))
done
chk "all $MW_WORKERS workers listening" "$MW_WORKERS" "$UP"
chk "$MW_PERSIST persistence workers running" "$MW_PERSIST" "$(ps -eo args | grep -c '[p]ersistence worker' || true)"

TARGET=$((MW_WORKERS-1))
OWNED=""
for i in $(seq 1 60); do
  R=$(timeout 5 redis-cli -p $((RESP+TARGET)) SET "mw:probe$i" v 2>&1)
  if [ "$R" = "OK" ]; then OWNED="mw:probe$i"; break; fi
done
if [ -z "$OWNED" ]; then
  echo "FAIL  could not find a key owned by worker $TARGET"; fail=$((fail+1))
else
  echo "  worker $TARGET owns $OWNED"
  chk "1MiB SET on worker $TARGET accepted" "OK" "$(timeout 25 redis-cli -p $((RESP+TARGET)) -x SET "$OWNED" < /tmp/big.txt 2>&1)"
  sleep 4
  chk "it persisted byte-identical from worker $TARGET segment" "t"       "$(psql_ "SELECT val = repeat('x',1048576)::bytea FROM supacache.kv WHERE key='$OWNED'::bytea")"
  stop_pg; sleep 1; start_pg; wait_ready; sleep 5
  chk "recovered to the owning worker" "1048576" "$(timeout 10 redis-cli -p $((RESP+TARGET)) STRLEN "$OWNED" 2>&1)"
  OW0=$(timeout 5 redis-cli -p $RESP STRLEN "$OWNED" 2>&1)
  case "$OW0" in
    MOVED*) echo "PASS  worker 0 redirects rather than serving a key it does not own"; pass=$((pass+1)) ;;
    0)      echo "PASS  the key is absent from worker 0 segment"; pass=$((pass+1)) ;;
    *)      echo "FAIL  worker 0 answered [$OW0] for a key owned by worker $TARGET"; fail=$((fail+1)) ;;
  esac
fi
chk "no dropped records across the multi-worker run" "0" "$(psql_ "SELECT dropped FROM supacache.ring_stats()")"
chk "no unresolved references" "0" "$(psql_ "SELECT unresolved FROM supacache.ring_stats()")"

echo "########## Q. cross-worker pub/sub ##########"
# The workers are separate processes. A SUBSCRIBE on one and a PUBLISH on
# another only meet if the routing table and inboxes live in shared memory;
# with the in-process bus the message is dropped and PUBLISH answers 0, which
# is exactly what "nobody is listening" looks like, so neither side can tell.
SUBW=$((MW_WORKERS-1))
rm -f /tmp/sub.out /tmp/psub.out

( timeout 10 redis-cli -p $((RESP+SUBW)) SUBSCRIBE xw:chan > /tmp/sub.out 2>&1 ) &
( timeout 10 redis-cli -p $((RESP+SUBW)) PSUBSCRIBE "xw:*" > /tmp/psub.out 2>&1 ) &
sleep 3

RECV=$(timeout 5 redis-cli -p $RESP PUBLISH xw:chan hello-across 2>&1)
chk "PUBLISH on worker 0 counts the subscribers on worker $SUBW" "2" "$RECV"

# A channel nobody subscribed to must still report nobody, so the count above
# is the routing table working rather than a broadcast to every worker. The
# name deliberately falls outside "xw:*" too: the pattern subscriber above
# matches anything under that prefix, so xw:quiet would have had a real
# receiver and this would have been asserting the wrong thing.
chk "an unsubscribed channel still reports no receivers" "0" \
    "$(timeout 5 redis-cli -p $RESP PUBLISH zz:quiet nobody 2>&1)"

wait 2>/dev/null || true
if grep -q "hello-across" /tmp/sub.out 2>/dev/null; then
  echo "PASS  the channel subscriber on worker $SUBW received it"; pass=$((pass+1))
else
  echo "FAIL  worker $SUBW never received the channel message"; fail=$((fail+1))
fi
if grep -q "hello-across" /tmp/psub.out 2>/dev/null; then
  echo "PASS  the pattern subscriber on worker $SUBW received it"; pass=$((pass+1))
else
  echo "FAIL  worker $SUBW never received the pattern message"; fail=$((fail+1))
fi

chk "no pub/sub messages dropped" "0" "$(psql_ "SELECT dropped FROM supacache.pubsub_stats()")"
chk "no subscriptions refused for table space" "0" "$(psql_ "SELECT route_full FROM supacache.pubsub_stats()")"
chk "no subscriptions refused for name length" "0" "$(psql_ "SELECT name_too_long FROM supacache.pubsub_stats()")"

echo "########## R. Mode B row cache under multiple RESP workers ##########"
# Issue #8 lists the row cache as single-worker alongside persistence. Reading
# the code that looks wrong: the cache lives in its own segment and is reached
# from a Postgres backend through the planner hook and CustomScan, never from a
# RESP worker, so the RESP worker count should not touch it. The cluster is
# already running three of them here, so assert it rather than reason about it.
psql_ "DROP TABLE IF EXISTS public.rc_mw" >/dev/null 2>&1
psql_ "CREATE TABLE public.rc_mw(id bigint primary key, v text)" >/dev/null 2>&1
psql_ "INSERT INTO public.rc_mw VALUES (1,'one'),(2,'two')" >/dev/null 2>&1
chk "register a table while $MW_WORKERS RESP workers run" "t"     "$(psql_ "SELECT supacache.rowcache_register('public.rc_mw', 1)")"
chk "the cache was populated" "t" "$(psql_ "SELECT entries > 0 FROM supacache.rowcache_stats()")"

RC_H0=$(psql_ "SELECT hits FROM supacache.rowcache_stats()")
chk "a cached row reads back correctly"        "one" "$(psql_ "SELECT v FROM public.rc_mw WHERE id = 1")"
chk "a second cached row reads back correctly" "two" "$(psql_ "SELECT v FROM public.rc_mw WHERE id = 2")"
RC_H1=$(psql_ "SELECT hits FROM supacache.rowcache_stats()")
# The values above would also be right if the cache were bypassed entirely and
# the rows came off the heap, so the hit counter is what shows the substitution
# actually happened with several RESP workers running.
if [ "${RC_H1:-0}" -gt "${RC_H0:-0}" ]; then
  echo "PASS  the scan was served from the cache (hits $RC_H0 -> $RC_H1)"; pass=$((pass+1))
else
  echo "FAIL  no cache hit recorded (hits $RC_H0 -> $RC_H1)"; fail=$((fail+1)); fi

stop_pg; sleep 1
set_conf "pg_keyspace.workers" "1"
set_conf "pg_keyspace.persist_workers" "1"
start_pg; wait_ready; sleep 2

echo "########## P. backing table unavailable ##########"
# A different failure class from the constraint case in L: there the statement
# is rejected, here the relation is gone entirely (undefined_table). Both must
# hold the ack and retain the records, and the point of testing two is that the
# handling is not special-cased to one error.
#
# Note on what is NOT injected here: the persistence worker connects as a
# superuser, and superusers bypass table ACLs, so REVOKE INSERT would not fail
# for it. A genuine permission failure needs the worker to run as a
# non-superuser role, which is a change to the extension rather than to this
# harness.
BEFORE_P=$(psql_ "SELECT count(*) FROM supacache.kv")
psql_ "ALTER TABLE supacache.kv RENAME TO kv_hidden" >/dev/null 2>&1
OUT=$(timeout 6 redis-cli -p $RESP SET tbl:k1 v1 2>&1); RC=$?
echo "  SET with the table renamed away -> [$OUT] (rc=$RC)"
if [ "$OUT" = "OK" ]; then
  echo "FAIL  ack released while the backing table was missing"; fail=$((fail+1))
else
  echo "PASS  ack held while the backing table was missing"; pass=$((pass+1)); fi
BACKLOG=$(psql_ "SELECT backlog_bytes FROM supacache.ring_stats()")
if [ "$BACKLOG" -gt 0 ]; then
  echo "PASS  record retained in the ring (backlog $BACKLOG bytes)"; pass=$((pass+1))
else
  echo "FAIL  record not retained (backlog $BACKLOG)"; fail=$((fail+1)); fi

psql_ "ALTER TABLE supacache.kv_hidden RENAME TO kv" >/dev/null 2>&1
for _ in $(seq 1 30); do
  [ "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='tbl:k1'::bytea")" = "1" ] && break
  sleep 1
done
chk "the retained record commits once the table returns" "1"  "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='tbl:k1'::bytea")"
chk "with the right value"                                "v1" "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='tbl:k1'::bytea")"
AFTER_P=$(psql_ "SELECT count(*) FROM supacache.kv")
if [ "$AFTER_P" -ge "$BEFORE_P" ]; then
  echo "PASS  nothing already durable was lost across the outage"; pass=$((pass+1))
else
  echo "FAIL  rows lost across the outage ($BEFORE_P -> $AFTER_P)"; fail=$((fail+1)); fi

echo ""
echo "########## S. a worker terminated out from under the cluster comes back ##########"
# A third failure class, distinct from both above. L rejects the statement and P
# removes the relation; here the worker's own session is terminated.
#
# What Postgres does with that is not what "restart_time = 2s" suggests.
# pg_terminate_backend on a background worker calls TerminateBackgroundWorker,
# which makes the postmaster DEREGISTER it rather than restart it, whatever
# bgw_restart_time says. Persistence would then stop for the life of the
# cluster, and because a durable write holds its ack rather than failing, the
# only outward sign is that writes stop completing.
#
# The watchdog exists for exactly this: every worker beats a heartbeat and every
# worker scans for gaps, so as long as one survives the rest are relaunched.
set_conf "pg_keyspace.watchdog_secs" "10"
stop_pg; sleep 1; start_pg; wait_ready; sleep 4

BEFORE_S=$(psql_ "SELECT count(*) FROM supacache.kv")
rcli SET fi:conn v1 >/dev/null 2>&1
sleep 2
chk "the write before the drop is durable" "v1"     "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='fi:conn'::bytea")"

SPID=$(ps -eo pid,args | grep "[p]ersistence worker" | awk '{print $1}' | head -1)
if [ -z "$SPID" ]; then
  echo "FAIL  no persistence worker to terminate"; fail=$((fail+1))
else
  echo "  terminating the session of persistence worker pid $SPID"
  psql_ "SELECT pg_terminate_backend($SPID)" >/dev/null 2>&1
  sleep 3
  chk "the cluster stayed up (this is not a crash)" "1" "$(psql_ "SELECT 1")"

  # Postgres will not bring it back. The watchdog must, within its window.
  BACK=0; NEWPID=""
  for _ in $(seq 1 60); do
    NEWPID=$(ps -eo pid,args | grep "[p]ersistence worker" | awk '{print $1}' | head -1)
    if [ -n "$NEWPID" ] && [ "$NEWPID" != "$SPID" ]; then BACK=1; break; fi
    sleep 1
  done
  chk "the watchdog relaunched the deregistered worker" "1" "$BACK"
  [ "$BACK" = "1" ] && echo "  came back as pid $NEWPID (was $SPID)"
  chk_contains "the relaunch is in the log, not silent" "watchdog" "$(cat $PGDATA/log 2>/dev/null | tail -80)"

  # The real test: a durable write must complete again with no restart.
  OUT=$(timeout 20 redis-cli -p $RESP SET fi:conn2 v2 2>&1)
  chk "a durable write completes again without restarting the cluster" "OK" "$OUT"
  chk "and it reached the table" "v2"       "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='fi:conn2'::bytea")"
  chk "persistence caught up" "0" "$(psql_ "SELECT lag FROM supacache.ring_stats()")"

  # Exactly one worker per shard: a second one draining the same ring would
  # corrupt it, since the rings are single-consumer.
  chk "exactly one persistence worker is draining the shard" "1"       "$(ps -eo args | grep -c '[p]ersistence worker')"
fi

AFTER_S=$(psql_ "SELECT count(*) FROM supacache.kv")
if [ "${AFTER_S:-0}" -ge "${BEFORE_S:-0}" ]; then
  echo "PASS  nothing durable was lost across the outage"; pass=$((pass+1))
else
  echo "FAIL  rows lost across the terminate ($BEFORE_S -> $AFTER_S)"; fail=$((fail+1)); fi


echo ""
echo "########## T. replicated tier with a standby that goes away ##########"
# J asserts the tier refuses to start with no standby configured. This is the
# other half, and the one that matters in production: a standby that exists,
# and then stops.
#
# The promise of the replicated tier is that a successful reply means the write
# reached a synchronous standby. The only two honest behaviours when the standby
# is gone are to hold the reply or to refuse it. Quietly degrading to local
# commit would keep answering OK while the promise is no longer true, and
# nothing downstream could tell.
SBDATA=${PGKS_STANDBY_DATA:-/tmp/pgks-standby}
SBPORT=$((PORT + 10))
# -k /tmp to match start_pg: without it the standby puts its socket in the
# build default and every psql -h /tmp against it fails, which looks exactly
# like a standby that never started.
sb_start() { su postgres -c "$PGBIN/pg_ctl -D $SBDATA -l $SBDATA/log -o \"-p $SBPORT -k /tmp\" -w start" >/dev/null 2>&1; }
sb_stop()  { su postgres -c "$PGBIN/pg_ctl -D $SBDATA -m fast -w stop" >/dev/null 2>&1; }

stop_pg; sleep 1
set_conf "wal_level" "replica"
set_conf "max_wal_senders" "10"
set_conf "pg_keyspace.durability" "'durable'"
start_pg; wait_ready; sleep 2

rm -rf $SBDATA
timeout 180 su postgres -c "$PGBIN/pg_basebackup -h /tmp -p $PORT -U postgres -D $SBDATA -X stream -c fast -R" >/tmp/basebackup.log 2>&1
if [ ! -f "$SBDATA/postgresql.conf" ]; then
  echo "FAIL  could not take a base backup for the standby"; fail=$((fail+1))
  tail -5 /tmp/basebackup.log
else
  # The standby must not run the extension: its workers would fight for the RESP
  # port and the persistence worker would try to INSERT on a read-only server.
  # This section is about replication, not about running two keyspaces.
  sed -i "/shared_preload_libraries/d" $SBDATA/postgresql.conf
  {
    echo "port = $SBPORT"
    echo "hot_standby = on"
  } >> $SBDATA/postgresql.conf
  # application_name is what synchronous_standby_names matches on, and
  # pg_basebackup -R does not put one in primary_conninfo. Last setting in
  # postgresql.auto.conf wins, so this overrides what -R wrote.
  echo "primary_conninfo = 'host=/tmp port=$PORT user=postgres application_name=standby1'" >> $SBDATA/postgresql.auto.conf
  sb_start
  SB_UP=0
  for _ in $(seq 1 30); do
    if $PGBIN/psql -h /tmp -p $SBPORT -U postgres -d postgres -tAc "SELECT 1" 2>/dev/null | grep -q "^1$"; then
      SB_UP=1; break
    fi
    sleep 1
  done
  chk "the standby is streaming" "1" "$SB_UP"

  # Now switch the primary to the replicated tier, with the standby named.
  stop_pg; sleep 1
  set_conf "synchronous_standby_names" "'standby1'"
  set_conf "pg_keyspace.durability" "'replicated'"
  start_pg; wait_ready; sleep 5

  # J's refusal must NOT fire now: the condition it names is satisfied.
  RECENT=$(tail -60 $PGDATA/log)
  if echo "$RECENT" | grep -q "REFUSING to start"; then
    echo "FAIL  refused the replicated tier despite a configured standby"; fail=$((fail+1))
  else
    echo "PASS  the replicated tier starts once a standby is configured"; pass=$((pass+1))
  fi
  chk "replication_status reports the promise is honoured" "t" \
      "$(psql_ "SELECT honoured FROM supacache.replication_status()")"
  chk "the standby is registered as synchronous" "sync" \
      "$(psql_ "SELECT sync_state FROM pg_stat_replication WHERE application_name='standby1'")"

  # A write with the standby up must complete.
  chk "a replicated write completes with the standby up" "OK" \
      "$(timeout 20 redis-cli -p $RESP SET rep:k1 v1 2>&1)"
  chk "and it reached the table" "v1" \
      "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='rep:k1'::bytea")"

  # Take the standby away. remote_apply cannot be satisfied, so the commit
  # blocks; the ack must be held rather than given.
  sb_stop; sleep 2
  BEFORE_T=$(psql_ "SELECT count(*) FROM supacache.kv")
  OUT=$(timeout 8 redis-cli -p $RESP SET rep:k2 v2 2>&1); RC=$?
  echo "  SET with the standby stopped -> [$OUT] (rc=$RC)"
  if [ "$RC" -ne 0 ] || [ -z "$OUT" ]; then
    echo "PASS  the ack was held rather than degrading to a local commit"; pass=$((pass+1))
  else
    echo "FAIL  acked [$OUT] while no standby could have received it"; fail=$((fail+1)); fi
  chk "nothing reached the table for it" "0" \
      "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='rep:k2'::bytea")"
  chk "the write already durable is still readable" "v1" \
      "$(timeout 10 redis-cli -p $RESP GET rep:k1 2>&1)"

  # Bring it back: the held record must commit rather than having been dropped.
  sb_start
  for _ in $(seq 1 60); do
    [ "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='rep:k2'::bytea")" = "1" ] && break
    sleep 1
  done
  chk "the held record commits once the standby returns" "1" \
      "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='rep:k2'::bytea")"
  chk "with the right value" "v2" \
      "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='rep:k2'::bytea")"
  AFTER_T=$(psql_ "SELECT count(*) FROM supacache.kv")
  if [ "${AFTER_T:-0}" -ge "${BEFORE_T:-0}" ]; then
    echo "PASS  nothing durable was lost while the standby was away"; pass=$((pass+1))
  else
    echo "FAIL  rows lost across the standby outage ($BEFORE_T -> $AFTER_T)"; fail=$((fail+1)); fi

  # The promise can also be withdrawn by a reload rather than by a failure:
  # synchronous_standby_names is sighup context. Clearing it must fail closed,
  # not silently downgrade every subsequent write to a local commit.
  psql_ "ALTER SYSTEM SET synchronous_standby_names = ''" >/dev/null 2>&1
  psql_ "SELECT pg_reload_conf()" >/dev/null 2>&1
  sleep 2
  chk "replication_status reports the promise is no longer honoured" "f" \
      "$(psql_ "SELECT honoured FROM supacache.replication_status()")"
  psql_ "ALTER SYSTEM RESET synchronous_standby_names" >/dev/null 2>&1
  psql_ "SELECT pg_reload_conf()" >/dev/null 2>&1

  sb_stop
fi

stop_pg; sleep 1
set_conf "pg_keyspace.durability" "'durable'"
set_conf "synchronous_standby_names" "''"
start_pg; wait_ready; sleep 2

echo ""
echo "########## U. the durable table's filesystem fills up ##########"
# The one row of the failure matrix that needs a real full filesystem rather
# than a rejected statement: ENOSPC arrives from the storage layer, mid
# transaction, not from the planner. A small tmpfs holding supacache.kv is the
# closest thing to it that can be arranged repeatably.
#
# Mounting one needs CAP_SYS_ADMIN, which a container may not have, so this
# section skips rather than fails when it cannot arrange the conditions. A
# skipped section says so; it does not quietly pass.
SMALL=${PGKS_SMALL_MOUNT:-/tmp/pgks-small}
mkdir -p $SMALL
# Big enough that moving the existing table onto it succeeds, small enough
# that the filler below fills it in a reasonable number of steps.
if ! mount -t tmpfs -o size=32m tmpfs $SMALL 2>/dev/null; then
  echo "  SKIP  cannot mount a tmpfs here (needs CAP_SYS_ADMIN); disk-full not exercised"
else
  chown postgres:postgres $SMALL
  chmod 700 $SMALL
  psql_ "DROP TABLESPACE IF EXISTS pgks_small" >/dev/null 2>&1
  TS=$(psql_ "CREATE TABLESPACE pgks_small LOCATION '$SMALL'" 2>&1)
  # Emptied before the move, not after: the table arrives here carrying every
  # earlier section's data, which both risks not fitting on a deliberately small
  # filesystem and, more importantly, leaves reusable free space behind. See the
  # note below the move for why that free space defeats the whole section.
  psql_ "TRUNCATE supacache.kv" >/dev/null 2>&1
  # supacache.kv is PARTITION BY HASH, so the parent has no storage of its own.
  # ALTER TABLE on it sets the default tablespace for FUTURE partitions and
  # moves nothing, succeeding silently while every row stays where it was. Four
  # earlier versions of this section did exactly that and then "tested" a full
  # filesystem the data was never on.
  psql_ "DO \$\$ DECLARE p regclass; BEGIN FOR p IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = 'supacache.kv'::regclass LOOP EXECUTE format('ALTER TABLE %s SET TABLESPACE pgks_small', p); END LOOP; END \$\$" >/dev/null 2>&1
  MOVED=$(psql_ "ALTER TABLE supacache.kv SET TABLESPACE pgks_small" 2>&1)
  # Assert the precondition instead of assuming it. This is the check whose
  # absence let the section pass while exercising nothing.
  STRAY=$(psql_ "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid LEFT JOIN pg_tablespace t ON t.oid = c.reltablespace WHERE i.inhparent = 'supacache.kv'::regclass AND coalesce(t.spcname,'pg_default') <> 'pgks_small'")
  if [ "${STRAY:-1}" != "0" ]; then
    echo "FAIL  $STRAY partition(s) of supacache.kv are not on the small tablespace;"
    echo "        the section would be filling a filesystem the data is not on"
    fail=$((fail+1))
    psql_ "DROP TABLESPACE IF EXISTS pgks_small" >/dev/null 2>&1
    umount $SMALL 2>/dev/null
  elif echo "$MOVED" | grep -qi "error"; then
    echo "  SKIP  could not move supacache.kv onto the small tablespace [$MOVED]"
    umount $SMALL 2>/dev/null
  else
    echo "PASS  every supacache.kv partition is on the small filesystem"; pass=$((pass+1))
    # Start from an empty table. By the time this section runs, supacache.kv
    # carries the deletes and overwrites of every section before it, and a TOAST
    # table with reusable free space absorbs a megabyte without extending a
    # single file. A full filesystem does not stop a write that never needed to
    # allocate, so with that history in place the section tests nothing: two
    # earlier versions of it were fooled exactly that way, one of them while
    # reporting PASS.
    #
    # TRUNCATE rather than VACUUM FULL, because compacting still leaves the rows
    # occupying the filesystem and can still leave slack. This section needs a
    # relation with nowhere to put anything.
    BEFORE_U=$(psql_ "SELECT count(*) FROM supacache.kv")
    chk "a write still works before it fills" "OK"         "$(timeout 20 redis-cli -p $RESP SET du:ok v1 2>&1)"

    # Fill the filesystem directly rather than by generating traffic. Trying to
    # fill it through the RESP path makes the test depend on arena size, TOAST
    # compression and eviction, none of which is what is under test here; an
    # earlier version of this section quietly failed to fill anything at all.
    # STORAGE EXTERNAL keeps the filler out of line and uncompressed, so a
    # megabyte of payload costs a megabyte of disk.
    psql_ "CREATE TABLE supacache.du_filler(b bytea) TABLESPACE pgks_small" >/dev/null 2>&1
    psql_ "ALTER TABLE supacache.du_filler ALTER COLUMN b SET STORAGE EXTERNAL" >/dev/null 2>&1
    # Two stages, because "full" is not one thing. A filesystem that cannot take
    # another megabyte will still take another 8 kB page, and an earlier version
    # of this stopped at the first stage: the filesystem was full, a 1 MB insert
    # failed, and the durable write under test then succeeded anyway because a
    # fifty-byte row fitted in the space that was left.
    FULL=0
    for i in $(seq 1 80); do
      ERR=$(psql_ "INSERT INTO supacache.du_filler SELECT repeat('x', 1000000)::bytea")
      case "$ERR" in
        *"o space left"*) FULL=1; echo "  no room for another MB after ${i} MB of filler"; break ;;
      esac
    done
    FINE=0
    if [ "$FULL" -eq 1 ]; then
      for j in $(seq 1 400); do
        ERR=$(psql_ "INSERT INTO supacache.du_filler SELECT repeat('y', 7000)::bytea")
        case "$ERR" in
          *"o space left"*) FINE=1; echo "  and no room for another page after $j more"; break ;;
        esac
      done
    fi
    chk "the filesystem is full to the byte" "1" "$FINE"

    if [ "$FINE" -eq 1 ]; then
      # The invariant. A durable write that cannot be stored must not be acked,
      # and nothing already durable may be lost.
      # A megabyte, not a short string: a small value can land in free space
      # inside an existing page and commit even on a full filesystem, which
      # proves nothing. This one has to allocate.
      head -c 700000 /dev/urandom | base64 | head -c 1000000 > /tmp/nospace.txt
      OUT=$(timeout 10 redis-cli -p $RESP -x SET du:nospace < /tmp/nospace.txt 2>&1); RC=$?
      echo "  SET with the filesystem full -> [$OUT] (rc=$RC)"
      if [ "$RC" -ne 0 ] || [ -z "$OUT" ] || [ "$OUT" != "OK" ]; then
        echo "PASS  the write was not acked while it could not be stored"; pass=$((pass+1))
      else
        echo "FAIL  acked [$OUT] with no space to store it"; fail=$((fail+1)); fi
      chk "nothing reached the table for it" "0"           "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='du:nospace'::bytea")"
      chk "the value written before it filled is still durable" "v1"           "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='du:ok'::bytea")"
      chk "no panics from the storage error" "0"           "$(countlog panicked $PGDATA/log)"
      ERRC=$(psql_ "SELECT failed_batches FROM supacache.ring_stats()")
      if [ "${ERRC:-0}" -gt 0 ]; then
        echo "PASS  the storage failure is countable, not just loggable ($ERRC)"; pass=$((pass+1))
      else
        echo "FAIL  the filesystem filled and failed_batches still reads $ERRC"; fail=$((fail+1)); fi

      # Free the space: the retained record must commit on its own, with no
      # restart and no intervention beyond making room.
      psql_ "DROP TABLE supacache.du_filler" >/dev/null 2>&1
      for _ in $(seq 1 40); do
        [ "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='du:nospace'::bytea")" = "1" ] && break
        sleep 1
      done
      chk "the held record commits once space is freed" "1"           "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='du:nospace'::bytea")"
      chk "with the right value" "$(wc -c < /tmp/nospace.txt)"           "$(psql_ "SELECT length(val) FROM supacache.kv WHERE key='du:nospace'::bytea")"
      AFTER_U=$(psql_ "SELECT count(*) FROM supacache.kv")
      if [ "${AFTER_U:-0}" -ge "${BEFORE_U:-0}" ]; then
        echo "PASS  nothing durable was lost across the outage"; pass=$((pass+1))
      else
        echo "FAIL  rows lost while the disk was full ($BEFORE_U -> $AFTER_U)"; fail=$((fail+1)); fi
    fi
    psql_ "DROP TABLE IF EXISTS supacache.du_filler" >/dev/null 2>&1

    # Put it back before anything else runs, and time the drop.
    #
    # This is a regression test for a defect the section found by hanging on it
    # for eleven minutes. DROP TABLESPACE emits a procsignal barrier and waits
    # for every backend to acknowledge it. A backend acknowledges from inside
    # CHECK_FOR_INTERRUPTS, which these workers never reached: the RESP worker
    # sits in a mio poll, the persistence worker in a drain loop, the expiry
    # worker in a sleep. None of them ever answered, so the drop waited forever
    # with no error and nothing in the log naming the culprit.
    #
    # The timeout is what keeps a recurrence a failed assertion rather than a
    # wedged suite.
    psql_ "DELETE FROM supacache.kv WHERE key LIKE 'du:%'::bytea" >/dev/null 2>&1
    psql_ "ALTER TABLE supacache.kv SET TABLESPACE pg_default" >/dev/null 2>&1
    psql_ "DO \$\$ DECLARE p regclass; BEGIN FOR p IN SELECT inhrelid::regclass FROM pg_inherits WHERE inhparent = 'supacache.kv'::regclass LOOP EXECUTE format('ALTER TABLE %s SET TABLESPACE pg_default', p); END LOOP; END \$\$" >/dev/null 2>&1
    T0=$(date +%s)
    PGOPTIONS='-c statement_timeout=45s' $PGBIN/psql -h /tmp -p $PORT -U postgres       -d postgres -tAc "DROP TABLESPACE IF EXISTS pgks_small" >/tmp/dropts.txt 2>&1
    ELAPSED=$(( $(date +%s) - T0 ))
    if grep -qi "timeout\|cancel" /tmp/dropts.txt; then
      echo "FAIL  DROP TABLESPACE never completed (${ELAPSED}s): a worker is not"
      echo "        acknowledging procsignal barriers"
      fail=$((fail+1))
    else
      echo "PASS  DROP TABLESPACE completed in ${ELAPSED}s, so barriers are acknowledged"
      pass=$((pass+1))
    fi
    umount $SMALL 2>/dev/null
  fi
fi


echo ""
echo "########## V. a serialization failure on the persistence transaction ##########"
# The issue filed this as needing a test hook. It does not. The persistence
# worker sets only `SET LOCAL synchronous_commit` and never an isolation level,
# so default_transaction_isolation applies to its transaction, and a genuine
# 40001 can be produced from outside with nothing compiled in.
stop_pg; sleep 1
set_conf "default_transaction_isolation" "'serializable'"
start_pg; wait_ready; sleep 3

psql_ "SELECT 1" >/dev/null 2>&1
BEFORE_V=$(psql_ "SELECT count(*) FROM supacache.kv")
# Contend for the whole table from another session while writes are flowing.
# A serializable reader that then writes creates the read-write dependency SSI
# refuses, and either side can be the one it cancels.
for r in 1 2 3 4 5 6; do
  (
    timeout 60 env PGOPTIONS="$PGOPTS" $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAq >/dev/null 2>&1 <<SQL
BEGIN ISOLATION LEVEL SERIALIZABLE;
-- Read the very row the persistence worker is about to write, then write it.
-- That read-write pair against a concurrent writer of the same row is the
-- dangerous structure SSI refuses; reading the whole table and writing some
-- other key, as an earlier version did, gives it no cycle to find.
SELECT version FROM supacache.kv WHERE key = ('ser:k$r')::bytea;
SELECT pg_sleep(0.5);
INSERT INTO supacache.kv (tenant,key,slot,kind,val,expires_at,version)
VALUES ('', ('ser:k$r')::bytea, 1, 's', 'contended'::bytea, 0, 1)
ON CONFLICT (tenant,key) DO UPDATE SET version = supacache.kv.version + 1;
COMMIT;
SQL
  ) &
  sleep 0.2
  timeout 10 redis-cli -p $RESP SET "ser:k$r" "v$r" >/dev/null 2>&1
  wait
done
sleep 3

SERR=$(grep -ci "could not serialize" $PGDATA/log 2>/dev/null || true)
if [ "${SERR:-0}" -gt 0 ]; then
  echo "PASS  a real serialization failure was produced ($SERR in the log)"; pass=$((pass+1))
else
  echo "FAIL  no serialization failure occurred, so nothing was exercised"; fail=$((fail+1))
fi
# Whichever side SSI cancelled, the invariant is the same: a write that was
# acked is durable, and the worker is still draining afterwards.
ALL_OK=1
for r in 1 2 3 4 5 6; do
  GOT=$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key=('ser:k$r')::bytea")
  [ "$GOT" = "v$r" ] || ALL_OK=0
done
chk "every acked write survived the serialization failures" "1" "$ALL_OK"
chk "no panics from the retry path" "0" "$(countlog panicked $PGDATA/log)"
chk "persistence is still draining afterwards" "OK" "$(timeout 20 redis-cli -p $RESP SET ser:after v 2>&1)"
chk "and it committed" "1" "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='ser:after'::bytea")"
AFTER_V=$(psql_ "SELECT count(*) FROM supacache.kv")
if [ "${AFTER_V:-0}" -ge "${BEFORE_V:-0}" ]; then
  echo "PASS  nothing durable was lost across the serialization failures"; pass=$((pass+1))
else
  echo "FAIL  rows lost ($BEFORE_V -> $AFTER_V)"; fail=$((fail+1)); fi

stop_pg; sleep 1
set_conf "default_transaction_isolation" "'read committed'"
start_pg; wait_ready; sleep 2

echo ""
echo "########## W. a deadlock involving the persistence transaction ##########"
# Also filed as needing a hook, and also not needing one. The persistence batch
# locks the keys it is writing; another session can hold one of them and then
# reach for another the batch already holds, which is a deadlock by
# construction rather than by luck.
#
# It is the least deterministic row in the matrix, because it depends on the
# batch containing both keys and on the order it takes their locks. The loop
# below retries, and the section says plainly whether it managed to produce one
# rather than passing on the strength of having tried.
psql_ "DELETE FROM supacache.kv WHERE key LIKE 'dl:%'::bytea" >/dev/null 2>&1
timeout 20 redis-cli -p $RESP SET dl:a seed >/dev/null 2>&1
timeout 20 redis-cli -p $RESP SET dl:b seed >/dev/null 2>&1
sleep 2
BEFORE_W=$(psql_ "SELECT count(*) FROM supacache.kv")
DEADLOCKED=0
for attempt in 1 2 3 4 5; do
  # Hold dl:a, then reach for dl:b once the batch is in flight holding it.
  (
    timeout 60 env PGOPTIONS="$PGOPTS" $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAq >/dev/null 2>&1 <<SQL
BEGIN;
UPDATE supacache.kv SET version = version + 1 WHERE key = 'dl:a'::bytea;
SELECT pg_sleep(0.6);
UPDATE supacache.kv SET version = version + 1 WHERE key = 'dl:b'::bytea;
COMMIT;
SQL
  ) &
  HOLDER=$!
  sleep 0.2
  # Written b first so the batch takes b's lock before it waits on a.
  timeout 15 redis-cli -p $RESP SET dl:b "w$attempt" >/dev/null 2>&1 &
  timeout 15 redis-cli -p $RESP SET dl:a "w$attempt" >/dev/null 2>&1 &
  wait $HOLDER 2>/dev/null || true
  wait 2>/dev/null || true
  if [ "$(countlog 'deadlock detected' $PGDATA/log)" -gt 0 ]; then
    DEADLOCKED=1; echo "  produced a deadlock on attempt $attempt"; break
  fi
  sleep 1
done

if [ "$DEADLOCKED" -eq 1 ]; then
  echo "PASS  a real deadlock was produced against the persistence transaction"; pass=$((pass+1))
  sleep 4
  chk "no panics from the deadlock" "0" "$(countlog panicked $PGDATA/log)"
  chk "persistence is still draining afterwards" "OK" "$(timeout 20 redis-cli -p $RESP SET dl:after v 2>&1)"
  chk "and it committed" "1" "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key='dl:after'::bytea")"
  AFTER_W=$(psql_ "SELECT count(*) FROM supacache.kv")
  if [ "${AFTER_W:-0}" -ge "${BEFORE_W:-0}" ]; then
    echo "PASS  nothing durable was lost across the deadlock"; pass=$((pass+1))
  else
    echo "FAIL  rows lost across the deadlock ($BEFORE_W -> $AFTER_W)"; fail=$((fail+1)); fi
else
  # Reported, not swallowed. A row that cannot be provoked here is a row this
  # harness does not cover, and saying so is the point.
  echo "  NOT PRODUCED  no deadlock in 5 attempts; this row is not exercised on this run"
fi

echo ""
echo "########## X. the WAL filesystem fills up ##########"
# The row where Postgres takes itself down. A WAL write that cannot complete is
# a PANIC, not an ERROR, because there is no way to continue correctly without
# it. So this is not "does the extension handle an error" but "does anything
# acked survive the server killing itself", which is the same invariant every
# other row asserts, under the harshest way of losing the server.
#
# Needs the same CAP_SYS_ADMIN as the disk-full row, and skips out loud without it.
WALMNT=$PGDATA/pg_wal
WALBAK=${PGKS_WAL_BACKUP:-/tmp/pgks-walbak}
stop_pg; sleep 1
rm -rf $WALBAK; mkdir -p $WALBAK
cp -a $WALMNT/. $WALBAK/ 2>/dev/null
if ! mount -t tmpfs -o size=96m tmpfs $WALMNT 2>/dev/null; then
  echo "  SKIP  cannot mount a tmpfs here (needs CAP_SYS_ADMIN); WAL exhaustion not exercised"
  start_pg; wait_ready; sleep 2
else
  cp -a $WALBAK/. $WALMNT/ 2>/dev/null
  chown -R postgres:postgres $WALMNT
  chmod 700 $WALMNT
  start_pg; wait_ready; sleep 3

  # The write whose survival is the whole point of the section.
  chk "a durable write is acked before the WAL fills" "OK" \
      "$(timeout 20 redis-cli -p $RESP SET wal:keep v1 2>&1)"
  sleep 2
  chk "and it is durable" "v1" \
      "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='wal:keep'::bytea")"

  # Burn WAL until the filesystem holding it cannot take another byte.
  psql_ "CREATE TABLE IF NOT EXISTS supacache.wal_filler(b bytea)" >/dev/null 2>&1
  psql_ "ALTER TABLE supacache.wal_filler ALTER COLUMN b SET STORAGE EXTERNAL" >/dev/null 2>&1
  # Remember where the log is now, so the assertions below read exactly this
  # section's output. A fixed tail cannot: how much Postgres writes after the
  # PANIC (restart attempts, recovery chatter) varies run to run, and when it
  # runs long the line naming the cause falls off the end of the window while
  # the PANIC itself is still there — which reads as "it went down for some
  # other reason" and fails a row that is actually fine.
  LOG_X=$(wc -l < $PGDATA/log)
  DOWN=0
  for i in $(seq 1 150); do
    psql_ "INSERT INTO supacache.wal_filler SELECT repeat('w', 1000000)::bytea" >/dev/null 2>&1
    if ! psql_ "SELECT 1" 2>/dev/null | grep -q "^1$"; then
      DOWN=1; echo "  the server stopped answering after ${i} MB of WAL churn"; break
    fi
  done

  if [ "$DOWN" -eq 0 ]; then
    echo "  NOT PRODUCED  the WAL filesystem did not fill within the budget"
  else
    if grep -qi "PANIC" $PGDATA/log 2>/dev/null; then
      echo "PASS  Postgres PANICked on the WAL write rather than continuing"; pass=$((pass+1))
    else
      echo "FAIL  the server went down without a PANIC in the log"; fail=$((fail+1)); fi
    chk_contains "the cause is recorded as a storage failure" "No space left" \
        "$(tail -n +$((LOG_X+1)) $PGDATA/log)"

    # The realistic recovery: give the WAL filesystem more room. Nothing can be
    # freed from inside, because freeing WAL needs a checkpoint, and a
    # checkpoint needs to write WAL.
    mount -o remount,size=512m $WALMNT 2>/dev/null
    start_pg
    UP=0
    for _ in $(seq 1 90); do
      psql_ "SELECT 1" 2>/dev/null | grep -q "^1$" && { UP=1; break; }
      sleep 1
    done
    chk "the cluster comes back once the WAL filesystem has room" "1" "$UP"
    if [ "$UP" = "1" ]; then
      chk "the write acked before the PANIC survived it" "v1" \
          "$(psql_ "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='wal:keep'::bytea")"
      chk "and is served again from shared memory" "v1" \
          "$(timeout 20 redis-cli -p $RESP GET wal:keep 2>&1)"
      chk "persistence resumed" "OK" "$(timeout 25 redis-cli -p $RESP SET wal:after v2 2>&1)"
    fi
  fi

  # Put the real WAL directory back, or nothing after this point would start.
  psql_ "DROP TABLE IF EXISTS supacache.wal_filler" >/dev/null 2>&1
  stop_pg; sleep 1
  # The WAL the cluster needs now lives on the tmpfs. Unmounting exposes the
  # copies taken before the mount, which are stale by a PANIC, a recovery and
  # everything since, so the cluster would not start on them. Carry the live
  # segments across the unmount instead of stranding them.
  WALNOW=${PGKS_WAL_NOW:-/tmp/pgks-walnow}
  rm -rf $WALNOW; mkdir -p $WALNOW
  cp -a $WALMNT/. $WALNOW/ 2>/dev/null
  umount $WALMNT 2>/dev/null || umount -l $WALMNT 2>/dev/null
  rm -rf $WALMNT/* $WALMNT/.[!.]* 2>/dev/null
  cp -a $WALNOW/. $WALMNT/ 2>/dev/null
  chown -R postgres:postgres $WALMNT 2>/dev/null
  start_pg; wait_ready; sleep 2
  chk "the cluster is healthy on its real WAL directory" "1" "$(psql_ "SELECT 1")"
fi

echo ""
echo "########## Y. an interrupted invalidation apply loses nothing (#66) ##########"
# The Mode B invalidation worker peeks the decode slot, applies what it read, and
# only then advances the slot. It used to consume first and apply second, so
# anything that interrupted the apply lost those invalidations permanently: the
# slot had already moved past them, nothing replayed them, and the row cache went
# on serving the pre-change values. Nothing was logged either, because from the
# worker's own point of view nothing had failed.
#
# What makes that observable — rather than masked by a restart wiping the cache —
# is that a worker which ERRORs exits with code 1, and the postmaster does not
# treat code 1 as a crash: the cluster stays up and shared memory, row cache
# included, survives. So hold ACCESS EXCLUSIVE on the cached table and the
# refill's own SELECT times out, failing the apply with the cache live underneath
# it. Needs wal_level=logical and the keys-only output plugin.
PLUGIN_OK=1
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/pgks_plugin.log 2>&1 || PLUGIN_OK=0
chk "the supacache_keys output plugin builds and installs" "1" "$PLUGIN_OK"

# Some builds gate output plugins behind an allowlist GUC. Setting a parameter
# the server does not know would stop it starting at all, so ask first.
OPL=$(psql_ "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")

if [ "$PLUGIN_OK" = "1" ]; then
  stop_pg; sleep 1
  set_conf "wal_level" "logical"
  set_conf "max_replication_slots" "10"
  set_conf "pg_keyspace.workers" "1"
  set_conf "pg_keyspace.persist_workers" "1"
  set_conf "pg_keyspace.rowcache_decode" "on"
  set_conf "pg_keyspace.rowcache_refill" "on"
  # Long enough that the drain lands inside the window this section controls
  # rather than racing it.
  set_conf "pg_keyspace.rowcache_decode_ms" "4000"
  # lock_timeout, NOT statement_timeout. statement_timeout is armed in
  # start_xact_command(), which is the main query loop; a background worker's SPI
  # never goes through it, so the setting has no effect there and the refill just
  # waits out the lock (which is what the first version of this section did).
  # lock_timeout is armed by the lock manager itself in ProcSleep, so it applies
  # to any lock wait however the query was started. Every psql_ here overrides it
  # per session through PGOPTIONS, so only the worker's refill is affected.
  set_conf "lock_timeout" "2s"
  [ "$OPL" = "1" ] && set_conf "output_plugin_libraries" "'supacache_keys'"
  start_pg; wait_ready; sleep 3

  psql_ "DROP TABLE IF EXISTS public.inv66 CASCADE" >/dev/null
  psql_ "CREATE TABLE public.inv66(id int primary key, v text)" >/dev/null
  psql_ "INSERT INTO public.inv66 SELECT g, 'v1' FROM generate_series(1,200) g" >/dev/null
  psql_ "SELECT supacache.rowcache_register('public.inv66', 1)" >/dev/null
  # Let the worker consume the setup INSERTs' own decode records before caching,
  # or they would invalidate the rows we just cached.
  sleep 6
  psql_ "SELECT supacache.rowcache_put('public.inv66', g) FROM generate_series(1,200) g" >/dev/null
  chk "the row cache is serving cached rows" "1" \
      "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.inv66 WHERE id=1" | grep -c 'pg_keyspace_rowcache')"

  PMT0=$(psql_ "SELECT pg_postmaster_start_time()")
  LSN0=$(psql_ "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='supacache_rowcache'")
  LOG0=$(wc -l < $PGDATA/log)

  # Change every cached row, then make the apply impossible before the worker
  # next wakes. The lock outlives several drain attempts.
  # Decoding does not take a relation lock — a peek returns normally while
  # ACCESS EXCLUSIVE is held — so phase 1 always completes here and it is
  # specifically the apply that fails. That is what makes the next assertion
  # decisive rather than vacuous: the changes really were read before they were
  # lost.
  psql_ "UPDATE public.inv66 SET v='v2'" >/dev/null
  ( psql_ "BEGIN; LOCK TABLE public.inv66 IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(20); COMMIT" >/dev/null 2>&1 ) &
  LOCKER=$!
  sleep 22
  wait $LOCKER 2>/dev/null

  ERRS=$(tail -n +$((LOG0+1)) $PGDATA/log | grep -c "rowcache invalidation worker.*exit code 1" || true)
  PMT1=$(psql_ "SELECT pg_postmaster_start_time()")
  LSN1=$(psql_ "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='supacache_rowcache'")

  chk "the apply failed while the lock was held" "1" "$([ "${ERRS:-0}" -ge 1 ] && echo 1 || echo 0)"
  chk "the cluster stayed up, so the row cache survived the failure" "$PMT0" "$PMT1"
  # The assertion this section exists for. Consuming before applying moved the
  # slot here, and those invalidations were never seen again.
  chk "the slot did not advance past changes that were never applied" "$LSN0" "$LSN1"

  # With the lock gone the same batch is re-read and applied. Both terminal
  # actions are idempotent, so the replay converges rather than double-applying.
  SAMPLE="SELECT v FROM public.inv66 WHERE id=1 UNION ALL SELECT v FROM public.inv66 WHERE id=37 \
UNION ALL SELECT v FROM public.inv66 WHERE id=99 UNION ALL SELECT v FROM public.inv66 WHERE id=150 \
UNION ALL SELECT v FROM public.inv66 WHERE id=200"
  STALE=""
  for _ in $(seq 1 20); do
    STALE=$(psql_ "SELECT count(*) FROM ($SAMPLE) s WHERE v <> 'v2'")
    [ "$STALE" = "0" ] && break
    sleep 2
  done
  chk "the cache is coherent once the apply can run again" "0" "$STALE"

  # And the slot must move once a batch really has been applied, or the fix
  # would trade lost invalidations for unbounded WAL retention.
  LSN2=$(psql_ "SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name='supacache_rowcache'")
  chk "the slot advances once the batch is applied" "1" \
      "$(psql_ "SELECT ('$LSN2'::pg_lsn > '$LSN0'::pg_lsn)::int")"
fi

echo ""
echo "########## Z. recovery says so when the persisted set does not fit (#42) ##########"
# Recovery loads every persisted key for this worker's slot range into the
# segment. When the segment is smaller than that set, CLOCK eviction quietly
# makes room as it loads and the cache comes back partial: every lookup still
# answers, some of them with a miss for a key that is durably stored and was
# never deleted. Nothing said so. pg_keyspace.keys is Postmaster-level, so the
# way in is to persist under one size and recover under a smaller one.
BULK=${PGKS_BULK_FILE:-/tmp/pgks_recov_bulk.txt}
BULK_N=${PGKS_BULK_N:-2000}
ZLOG=${PGKS_Z_LOG:-/tmp/pgks_recov_log.txt}
seq 1 $BULK_N | awk '{print "SET recov:"$1" v"$1}' > $BULK
# One redis-cli for the lot. The durable tier holds each ack until its record
# commits, so this is deliberately a few thousand sequential round trips.
timeout 180 redis-cli -p $RESP < $BULK >/dev/null 2>&1
sleep 3
KV_BEFORE=$(psql_ "SELECT count(*) FROM supacache.kv")
chk "the bulk write persisted" "1" "$([ "${KV_BEFORE:-0}" -ge "$BULK_N" ] && echo 1 || echo 0)"

stop_pg; sleep 1
# 1024 is the floor the extension clamps to, which is ~1142 entries — well under
# what was just persisted.
set_conf "pg_keyspace.keys" "1024"
LOG_Z=$(wc -l < $PGDATA/log)
start_pg; wait_ready; sleep 4
tail -n +$((LOG_Z+1)) $PGDATA/log > $ZLOG

chk "recovery announces itself before it starts" "1" \
    "$([ "$(countlog 'recovering slots' $ZLOG)" -ge 1 ] && echo 1 || echo 0)"
chk "recovery warns that the cache came back partial" "1" \
    "$([ "$(countlog 'recovery evicted' $ZLOG)" -ge 1 ] && echo 1 || echo 0)"
chk "the warning names the setting to raise" "1" \
    "$([ "$(countlog 'pg_keyspace.keys' $ZLOG)" -ge 1 ] && echo 1 || echo 0)"

ENTRIES=$(psql_ "SELECT coalesce(sum(entries),0)::bigint FROM supacache.stats()")
chk "the cache really did come back short" "1" \
    "$([ "${ENTRIES:-0}" -lt "$BULK_N" ] && echo 1 || echo 0)"
# The whole point of warning: the rows are all still there, the cache is not.
KV_AFTER=$(psql_ "SELECT count(*) FROM supacache.kv")
chk "nothing durable was lost, only cached" "$KV_BEFORE" "$KV_AFTER"

# Put the size back, so anything added after this section sees the cluster the
# rest of the suite expects.
stop_pg; sleep 1
set_conf "pg_keyspace.keys" "20000"
start_pg; wait_ready; sleep 2
chk "the cluster is healthy again at the original size" "1" "$(psql_ "SELECT 1")"

echo ""
echo "########## AA. the row cache never crosses a tenant boundary (#39) ##########"
# Two things, neither of which runs in CI today.
#
# The RLS invariant itself. The CustomScan serves the RAW pre-policy row at the
# leaf and relies on the relation's own restriction clauses being re-applied
# above it, so a regression there is a cross-tenant read rather than a wrong
# answer. bench/run_security.sh proves it, but that harness needs a hand-built
# cluster and is wired into no workflow, so nothing checks it automatically.
#
# And the half of the cache key run_security.sh cannot reach. The key is
# (relid, pk_bytes), and its two tables use disjoint pk values — 1,2 against
# 10,11 — so a bug that ignored relid entirely would still pass there. A
# single-column primary key cannot hold one value twice, so the only way to put
# the same pk on two tenants is two tables, which is also how multi-tenant
# schemas are usually shaped.
psql_ "DROP TABLE IF EXISTS public.rls_rows, public.ten_a, public.ten_b, public.ten_c CASCADE" >/dev/null 2>&1
psql_ "CREATE ROLE ten_alpha LOGIN" >/dev/null 2>&1
psql_ "CREATE ROLE ten_beta LOGIN"  >/dev/null 2>&1
# Connect AS the tenant rather than SET ROLE inside the superuser session: psql
# prints the "SET" command tag ahead of the rows, which lands in the captured
# output and makes an empty result compare as "SET" rather than "". It is also
# closer to what a tenant actually does.
as_tenant() { PGOPTIONS="$PGOPTS" $PGBIN/psql -h /tmp -p $PORT -U "$1" -d postgres -tAc "$2" 2>&1; }
psql_ "CREATE TABLE public.rls_rows(id bigint primary key, tenant name, secret text)" >/dev/null
psql_ "INSERT INTO public.rls_rows VALUES (1,'ten_alpha','alpha-secret'),(2,'ten_beta','beta-secret')" >/dev/null
psql_ "ALTER TABLE public.rls_rows ENABLE ROW LEVEL SECURITY" >/dev/null
psql_ "ALTER TABLE public.rls_rows FORCE ROW LEVEL SECURITY" >/dev/null
psql_ "CREATE POLICY rls_own ON public.rls_rows FOR SELECT USING (tenant = current_user)" >/dev/null
psql_ "GRANT SELECT ON public.rls_rows TO ten_alpha, ten_beta" >/dev/null
# Same pk values in two tables; and a text pk whose canonical bytes are the same
# as a bigint pk's, since the canonical form is the type's output text.
psql_ "CREATE TABLE public.ten_a(id bigint primary key, v text)" >/dev/null
psql_ "CREATE TABLE public.ten_b(id bigint primary key, v text)" >/dev/null
psql_ "CREATE TABLE public.ten_c(id text   primary key, v text)" >/dev/null
psql_ "INSERT INTO public.ten_a VALUES (1,'alpha-one'),(2,'alpha-two')" >/dev/null
psql_ "INSERT INTO public.ten_b VALUES (1,'beta-one'),(2,'beta-two')" >/dev/null
psql_ "INSERT INTO public.ten_c VALUES ('1','gamma-one')" >/dev/null
for t in rls_rows ten_a ten_b ten_c; do
  psql_ "SELECT supacache.rowcache_register('public.$t', 1)" >/dev/null
done
# The invalidation worker is live from section Y. Let it consume the decode
# records for the setup writes above before caching anything, or it drops the
# rows these assertions are about and they all fall back to index scans.
sleep 8
psql_ "SELECT supacache.rowcache_put('public.rls_rows', 1)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.rls_rows', 2)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.ten_a', 1)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.ten_a', 2)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.ten_b', 1)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.ten_b', 2)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.ten_c', '1')" >/dev/null

# Everything below is worthless if the cache path is not the one being taken:
# an index scan returns the right answers too, and the section would pass while
# testing nothing.
chk "a cached lookup takes the Custom Scan" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.ten_a WHERE id=1" | grep -c 'pg_keyspace_rowcache')"
chk "so does the other table at the same pk" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.ten_b WHERE id=1" | grep -c 'pg_keyspace_rowcache')"

# The relid half of the key: identical pk bytes, different relations.
chk "pk 1 serves its own table's row"           "alpha-one" "$(psql_ "SELECT v FROM public.ten_a WHERE id=1")"
chk "pk 1 in the other table serves the other"  "beta-one"  "$(psql_ "SELECT v FROM public.ten_b WHERE id=1")"
chk "pk 2 serves its own table's row"           "alpha-two" "$(psql_ "SELECT v FROM public.ten_a WHERE id=2")"
chk "pk 2 in the other table serves the other"  "beta-two"  "$(psql_ "SELECT v FROM public.ten_b WHERE id=2")"
chk "a text pk with the same bytes as an integer pk does not collide" "gamma-one" \
    "$(psql_ "SELECT v FROM public.ten_c WHERE id='1'")"

# RLS re-applied above the cached leaf.
chk "a tenant reads its own cached row" "alpha-secret" \
    "$(as_tenant ten_alpha "SELECT secret FROM public.rls_rows WHERE id=1")"
chk "the other tenant reads its own cached row" "beta-secret" \
    "$(as_tenant ten_beta "SELECT secret FROM public.rls_rows WHERE id=2")"
chk "a tenant is denied the other's cached row" "" \
    "$(as_tenant ten_alpha "SELECT secret FROM public.rls_rows WHERE id=2")"
chk "and the denial is mutual" "" \
    "$(as_tenant ten_beta "SELECT secret FROM public.rls_rows WHERE id=1")"
# The denial has to come from the policy rather than from the cache having
# missed, so prove the denied read still goes through the cache path.
chk "the denied read still went through the cache path" "1" \
    "$(as_tenant ten_alpha "EXPLAIN (COSTS OFF) SELECT secret FROM public.rls_rows WHERE id=2" | grep -c 'pg_keyspace_rowcache')"

echo ""
echo "########## AB. the row cache's coherence window, asserted (#39) ##########"
# The row cache is eventually coherent, not immediately: a committed change is
# visible to the cache only once the invalidation worker drains the decode slot,
# which is pg_keyspace.rowcache_decode_ms away at worst. Nothing has ever
# asserted that bound, so "eventually" has been a claim rather than a measured
# property, and the DELETE case — where the window serves a row that no longer
# exists, which is worse than serving a stale value — was never exercised.
#
# Every assertion below is a CONVERGENCE assertion with a budget, never an
# immediate-coherence one. Asserting that a read straight after COMMIT is stale
# would be asserting a race: the worker is free to have drained already, and the
# test would fail on the runs where the cache did better than its guarantee.
DECODE_MS=$(psql_ "SHOW pg_keyspace.rowcache_decode_ms" | tr -d '[:space:]')
# Defaulted rather than trusted: an empty SHOW would turn the arithmetic below
# into a bash error mid-section rather than a failed assertion.
case "$DECODE_MS" in ''|*[!0-9]*) DECODE_MS=4000 ;; esac
# Generous against a loaded runner, but still several poll intervals, so a
# genuinely broken drain fails rather than hangs.
CONVERGE_BUDGET=${PGKS_CONVERGE_BUDGET:-25}
echo "  (decode interval $DECODE_MS ms, convergence budget ${CONVERGE_BUDGET}s)"

# Wait until `$2` is what the cached read returns, or the budget runs out.
# Echoes how long it took so a regression in the bound is visible in the log
# even when the assertion still passes.
converge() { # converge <sql> <want>
  local waited=0
  while [ "$waited" -lt "$CONVERGE_BUDGET" ]; do
    [ "$(psql_ "$1")" = "$2" ] && { echo "$waited"; return 0; }
    sleep 1; waited=$((waited+1))
  done
  echo "$waited"; return 1
}

psql_ "DROP TABLE IF EXISTS public.cc CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.cc(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.cc VALUES (1,'one'),(2,'two'),(3,'three'),(4,'four')" >/dev/null
psql_ "SELECT supacache.rowcache_register('public.cc', 1)" >/dev/null
# Same reason as section AA: let the worker consume the setup writes before
# caching, or it invalidates the rows these cases are about.
sleep 8
for k in 1 2 3 4; do psql_ "SELECT supacache.rowcache_put('public.cc', $k)" >/dev/null; done
chk "the rows under test are actually served from the cache" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.cc WHERE id=1" | grep -c 'pg_keyspace_rowcache')"

# --- UPDATE then read -------------------------------------------------------
psql_ "UPDATE public.cc SET v='one_v2' WHERE id=1" >/dev/null
T=$(converge "SELECT v FROM public.cc WHERE id=1" "one_v2")
chk "an UPDATE reaches the cache within the budget (took ${T}s)" "1" \
    "$([ "$(psql_ "SELECT v FROM public.cc WHERE id=1")" = "one_v2" ] && echo 1 || echo 0)"

# --- DELETE then read: the window serves a row that no longer exists ---------
psql_ "DELETE FROM public.cc WHERE id=2" >/dev/null
T=$(converge "SELECT count(*) FROM public.cc WHERE id=2" "0")
chk "a DELETE stops the cache serving the removed row (took ${T}s)" "0" \
    "$(psql_ "SELECT count(*) FROM public.cc WHERE id=2")"

# --- ROLLBACK must not change what is served --------------------------------
psql_ "BEGIN; UPDATE public.cc SET v='rolled_back' WHERE id=3; ROLLBACK" >/dev/null
sleep $(( (DECODE_MS / 1000) + 4 ))
chk "a ROLLBACK leaves the cached row alone" "three" \
    "$(psql_ "SELECT v FROM public.cc WHERE id=3")"

# --- two writers: the last commit is the one that survives -------------------
psql_ "UPDATE public.cc SET v='four_a' WHERE id=4" >/dev/null
psql_ "UPDATE public.cc SET v='four_b' WHERE id=4" >/dev/null
T=$(converge "SELECT v FROM public.cc WHERE id=4" "four_b")
chk "back-to-back UPDATEs converge on the last one (took ${T}s)" "four_b" \
    "$(psql_ "SELECT v FROM public.cc WHERE id=4")"

# --- INSERT: a row that was never cached must not be a phantom --------------
psql_ "INSERT INTO public.cc VALUES (5,'five')" >/dev/null
chk "a row inserted after caching reads correctly" "five" \
    "$(psql_ "SELECT v FROM public.cc WHERE id=5")"
chk "and it is served from the heap, not the cache" "0" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.cc WHERE id=5" | grep -c 'pg_keyspace_rowcache')"

# --- the window itself ------------------------------------------------------
# A read taken immediately after COMMIT may legitimately be either value. What
# it must never be is anything else — a torn read, an empty result, or the value
# of a different row.
psql_ "UPDATE public.cc SET v='one_v3' WHERE id=1" >/dev/null
IMM=$(psql_ "SELECT v FROM public.cc WHERE id=1")
if [ "$IMM" = "one_v2" ] || [ "$IMM" = "one_v3" ]; then IMM_OK=1; else IMM_OK=0; fi
chk "a read inside the window returns one of the two committed values (got '$IMM')" "1" "$IMM_OK"
T=$(converge "SELECT v FROM public.cc WHERE id=1" "one_v3")
chk "and the window closes within the budget (took ${T}s)" "one_v3" \
    "$(psql_ "SELECT v FROM public.cc WHERE id=1")"

echo ""
echo "########## AC. persisted-data versioning, both directions (#42) ##########"
# There is no migration script and no version table. `pg_ensure_schema` runs the
# whole DDL as CREATE/ALTER ... IF NOT EXISTS at every worker start, before
# recovery reads anything, so the schema converges on whatever the running
# binary expects. That is the entire mechanism, and nothing asserted the two
# properties it rests on.
#
# DOWNGRADE (new table, old binary) works only if every column the old binary
# does not know is defaulted, because its INSERTs name columns explicitly and
# simply omit them. That is a property of the table, so it needs no old binary
# to check: replay the pre-#25 statements verbatim against today's tables.
#
# UPGRADE (old table, new binary) works only if every column the new binary
# names is either original or carried by an ADD COLUMN IF NOT EXISTS retrofit.
# One column has ever been added after the fact -- kv_ttl.kind, in #25 -- and it
# has its retrofit. Dropping that column reproduces the pre-#25 table exactly,
# so restarting into it is the real upgrade, not an approximation of one.
rcli SET ver:plain v1            >/dev/null
rcli SET ver:ttl v1 EX 3600      >/dev/null
rcli DEL ver:hashttl             >/dev/null
rcli HSET ver:hashttl f1 hv1     >/dev/null
rcli EXPIRE ver:hashttl 3600     >/dev/null
sleep 2
chk "a TTL'd string persists with its kind" "s" "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='ver:ttl'::bytea")"
chk "a TTL'd hash persists with its kind"   "h" "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='ver:hashttl'::bytea")"

# --- downgrade: a binary that predates a column must still be able to write ---
# The generic form of the rule, so a column added tomorrow is caught here rather
# than by an operator rolling back. These five are supplied by every binary that
# has ever written these tables; any other NOT NULL column without a default is
# one an older binary could not satisfy.
chk "no persisted column is NOT NULL without a default" "0" \
    "$(psql_ "SELECT count(*) FROM information_schema.columns \
              WHERE table_schema='supacache' AND table_name IN ('kv','kv_ttl') \
                AND is_nullable='NO' AND column_default IS NULL \
                AND column_name NOT IN ('tenant','key','slot','expires_at','bucket')")"

# The concrete form: the pre-#25 INSERT, column list and all. Slot, expiry and
# bucket come from the row the current binary just wrote, so the target
# partition is known to exist and the arithmetic is not duplicated here.
psql_ "INSERT INTO supacache.kv_ttl (tenant,key,slot,val,expires_at,bucket) \
       SELECT '', 'ver:downgrade'::bytea, slot, val, expires_at, bucket \
       FROM supacache.kv_ttl WHERE key='ver:ttl'::bytea" >/dev/null
chk "an older binary's INSERT still succeeds against today's table" "1" \
    "$(psql_ "SELECT count(*) FROM supacache.kv_ttl WHERE key='ver:downgrade'::bytea")"
chk "and the column it did not know takes its default" "s" \
    "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='ver:downgrade'::bytea")"
# The pre-#25 read: still selectable, so nothing an old binary names has been
# renamed or dropped out from under it.
chk "an older binary's SELECT still resolves every column it names" "1" \
    "$([ "$(psql_ "SELECT count(*) FROM (SELECT key, val, expires_at FROM supacache.kv_ttl) t")" -ge 1 ] && echo 1 || echo 0)"

# What downgrade costs, set up as a controlled pair: ver:lostkind gets the exact
# bytes of the hash at ver:hashttl, through the old column list, so the only
# difference between the two rows is the one column the old binary cannot write.
psql_ "INSERT INTO supacache.kv_ttl (tenant,key,slot,val,expires_at,bucket) \
       SELECT '', 'ver:lostkind'::bytea, slot, val, expires_at, bucket \
       FROM supacache.kv_ttl WHERE key='ver:hashttl'::bytea" >/dev/null
chk "a hash written by an older binary records no type" "s" \
    "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='ver:lostkind'::bytea")"

stop_pg; sleep 1
start_pg; wait_ready; sleep 5

chk "a row an older binary wrote recovers" "v1" "$(rcli GET ver:downgrade)"
chk "a row the current binary wrote keeps its type" "hash" "$(rcli TYPE ver:hashttl)"
chk "and is still readable as one" "hv1" "$(rcli HGET ver:hashttl f1)"
# Same bytes, same expiry, same partition -- only the kind column differs, and
# that is the whole cost of a downgrade: the value survives, what it was does not.
chk "the identical row without a type comes back as a string" "string" \
    "$(rcli TYPE ver:lostkind)"
chk "so hash commands refuse it rather than mis-read the bytes" "1" \
    "$(rcli HGET ver:lostkind f1 | grep -c WRONGTYPE)"

# --- upgrade: the pre-#25 table, met by today's binary -----------------------
psql_ "ALTER TABLE supacache.kv_ttl DROP COLUMN kind" >/dev/null
chk "the table is back in its pre-retrofit shape" "0" \
    "$(psql_ "SELECT count(*) FROM information_schema.columns \
              WHERE table_schema='supacache' AND table_name='kv_ttl' AND column_name='kind'")"
# A row left behind by the old binary, written through the old column list.
psql_ "INSERT INTO supacache.kv_ttl (tenant,key,slot,val,expires_at,bucket) \
       SELECT '', 'ver:preretrofit'::bytea, slot, val, expires_at, bucket \
       FROM supacache.kv_ttl WHERE key='ver:ttl'::bytea" >/dev/null
chk "the old binary's row is there to be upgraded" "1" \
    "$(psql_ "SELECT count(*) FROM supacache.kv_ttl WHERE key='ver:preretrofit'::bytea")"

stop_pg; sleep 1
start_pg; wait_ready; sleep 5

chk "the retrofit put the column back" "1" \
    "$(psql_ "SELECT count(*) FROM information_schema.columns \
              WHERE table_schema='supacache' AND table_name='kv_ttl' AND column_name='kind'")"
chk "and backfilled every pre-existing row with its default" "s" \
    "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='ver:preretrofit'::bytea")"
chk "recovery reads the upgraded table" "v1" "$(rcli GET ver:preretrofit)"
# The cost of this direction, and the reason it is worth documenting: the column
# did not exist when these rows were written, so the retrofit defaults them all
# to 's'. An aggregate persisted by a pre-#25 binary comes back as a string and
# its own commands refuse it until the key is rewritten. No row is lost; the
# type is.
chk "an aggregate persisted before the retrofit comes back untyped" "string" \
    "$(rcli TYPE ver:hashttl)"
chk "and its own commands refuse it until it is rewritten" "1" \
    "$(rcli HGET ver:hashttl f1 | grep -c WRONGTYPE)"
# The retrofitted column is a real column, not just a backfill: a write after
# the upgrade records its type again, which is what makes the rewrite a fix.
rcli DEL ver:afterfix        >/dev/null
rcli HSET ver:afterfix f1 av1 >/dev/null
rcli EXPIRE ver:afterfix 3600 >/dev/null
sleep 2
chk "a write after the upgrade records its type again" "h" \
    "$(psql_ "SELECT kind FROM supacache.kv_ttl WHERE key='ver:afterfix'::bytea")"

echo ""
echo "########## AD. the persist worker's writes reach pg_stat_user_tables (#77) ##########"
# A background worker never runs the backend main loop, which is what normally
# publishes pending table statistics. Without an explicit flush the persist
# worker's row counts sit in process-local memory until the shutdown hook
# empties them, so a running cluster reports zero writes on its own cache tables
# however much it has written -- and autovacuum, whose thresholds are computed
# from those same counters, never sees the churn.
#
# The assertion has to be a DELTA taken without a restart in the middle. Absolute
# counters are no good: the suite has restarted this cluster several times by
# now and each shutdown flushed whatever was pending, so they are already
# non-zero and would pass without the fix.
STAT_BEFORE=$(psql_ "SELECT coalesce(sum(n_tup_ins + n_tup_upd),0)::bigint FROM pg_stat_user_tables WHERE schemaname='supacache'")
KV_STAT_BEFORE=$(psql_ "SELECT count(*) FROM supacache.kv")
seq 1 300 | awk '{print "SET statk:"$1" v"$1}' > /tmp/pgks_stat_writes.txt
timeout 120 redis-cli -p $RESP < /tmp/pgks_stat_writes.txt >/dev/null 2>&1
sleep 3
KV_STAT_AFTER=$(psql_ "SELECT count(*) FROM supacache.kv")
# Proving the writes landed first is the point: without this a zero delta below
# is ambiguous between "statistics are broken" and "nothing was written", and
# the test would pass vacuously the day the write path breaks.
chk "the writes actually persisted" "1" \
    "$([ "$(( ${KV_STAT_AFTER:-0} - ${KV_STAT_BEFORE:-0} ))" -ge 300 ] && echo 1 || echo 0)"
STAT_AFTER=$(psql_ "SELECT coalesce(sum(n_tup_ins + n_tup_upd),0)::bigint FROM pg_stat_user_tables WHERE schemaname='supacache'")
chk "and the statistics views saw them, with no restart in between" "1" \
    "$([ "${STAT_AFTER:-0}" -gt "${STAT_BEFORE:-0}" ] && echo 1 || echo 0)"
echo "  (n_tup_ins + n_tup_upd: $STAT_BEFORE -> $STAT_AFTER over $(( ${KV_STAT_AFTER:-0} - ${KV_STAT_BEFORE:-0} )) new rows)"
# Not asserted here: that a vacuum ran. 300 rows is far below the default
# threshold, so asserting it would be asserting a race. What matters is that
# the number autovacuum reads is no longer frozen at zero, which is what the
# delta above establishes.

echo ""
echo "########## AE. a durable connection pipelines instead of stalling (#78) ##########"
# A sync-ack tier used to stop reading a connection the moment one of its writes
# was waiting to commit, so pipeline depth was fixed at 1 and each connection
# got one durable write per persist window -- about 90/s at the default 10 ms.
# Reading on is safe because replies are appended to wbuf in command order and
# flush holds the whole buffer until every outstanding ack commits.
#
# Three things have to hold, and throughput is the least important of them:
# every acked write must still be durable, replies must still arrive in command
# order, and a read behind a write in the same pipeline must see that write.
PIPE_N=${PGKS_PIPE_N:-2000}
KV_PIPE_BEFORE=$(psql_ "SELECT count(*) FROM supacache.kv")
seq 1 $PIPE_N | awk '{print "SET pipek:"$1" v"$1}' > /tmp/pgks_pipe.txt
T_PIPE0=$(date +%s.%N)
PIPE_OUT=$(timeout 120 redis-cli -p $RESP --pipe < /tmp/pgks_pipe.txt 2>&1)
T_PIPE1=$(date +%s.%N)
PIPE_SECS=$(awk -v a="$T_PIPE0" -v b="$T_PIPE1" 'BEGIN{printf "%.1f", b-a}')
chk "every pipelined write was replied to" "1" \
    "$(echo "$PIPE_OUT" | grep -c "replies: $PIPE_N")"
chk "and none of them errored" "1" \
    "$(echo "$PIPE_OUT" | grep -c 'errors: 0')"
# The whole point: at one write per persist window this would take PIPE_N/90
# seconds. The budget is generous against a loaded runner and still nowhere
# near that, so it fails on a regression rather than on a slow day.
PIPE_BUDGET=${PGKS_PIPE_BUDGET:-10}
chk "the pipeline did not serialise on the persist window (took ${PIPE_SECS}s, budget ${PIPE_BUDGET}s)" "1" \
    "$(awk -v t="$PIPE_SECS" -v b="$PIPE_BUDGET" 'BEGIN{print (t < b) ? 1 : 0}')"
sleep 2
KV_PIPE_AFTER=$(psql_ "SELECT count(*) FROM supacache.kv")
# Throughput is worthless if the acks were lying. Every one of those writes was
# acknowledged on the durable tier, so every one must be in the table.
chk "and every acked write is actually in supacache.kv" "1" \
    "$([ "$(( ${KV_PIPE_AFTER:-0} - ${KV_PIPE_BEFORE:-0} ))" -ge "$PIPE_N" ] && echo 1 || echo 0)"

# Ordering, and read-your-writes within one pipeline. Interleaves SET and GET on
# the same key so the reply stream is only correct if replies come back in
# command order AND each read saw the write immediately before it. A reordering
# bug shows up as a mismatched reply rather than as a slow test.
cat > /tmp/pgks_order.py <<'PYEOF'
import socket, sys
port = int(sys.argv[1])
s = socket.create_connection(('127.0.0.1', port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
N = 500
out = bytearray()
want = []
for i in range(N):
    # The key length is computed, not written by hand: an $N that disagrees with
    # the key is a protocol error, and it fails as "out of order" rather than as
    # the malformed request it actually is.
    k = b'ordk:%03d' % (i % 1000)
    v = b'v%d' % i
    out += b'*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(k), k, len(v), v)
    want.append(b'+OK')
    out += b'*2\r\n$3\r\nGET\r\n$%d\r\n%s\r\n' % (len(k), k)
    want.append(b'$%d\r\n%s' % (len(v), v))
s.sendall(out)
expect = b''.join(w + b'\r\n' for w in want)
buf = b''
s.settimeout(30)
while len(buf) < len(expect):
    try:
        d = s.recv(1 << 16)
    except OSError:
        break
    if not d:
        break
    buf += d
if buf == expect:
    print('OK')
else:
    # Say where it diverged: a reordering shows as a reply appearing early, a
    # protocol error as an "-ERR" in the stream, and a short read as a length.
    n = min(len(buf), len(expect))
    at = next((i for i in range(n) if buf[i] != expect[i]), n)
    print('MISMATCH at byte %d of %d: wanted %r, got %r'
          % (at, len(expect), expect[at:at + 40], buf[at:at + 40]))
PYEOF
chk "replies come back in command order, and reads see the writes ahead of them" "OK" \
    "$(timeout 60 python3 /tmp/pgks_order.py $RESP 2>&1 | tail -1)"

echo ""
echo "########## AF. one connection cannot monopolise the worker (#83) ##########"
# Two failures, found together. process() drained a connection's whole read
# buffer before yielding, so a deep pipeline was served to completion while
# everyone else waited -- an unrelated connection went from 91586 operations in
# five seconds to one operation in eight. And a parked connection was
# re-executing the command it parked on, because `parked` was checked only after
# dispatch: SET is idempotent so that merely desynchronised a pipelining client,
# but INCR or LPUSH would have corrupted the value.
#
# Both need a ring small enough to be full, which is what makes a connection
# park at all, so this section reconfigures and puts the cluster back after.
stop_pg; sleep 1
set_conf "pg_keyspace.ring_mb" "1"
start_pg; wait_ready; sleep 3
chk "the cluster came back on a small ring" "1" "$(psql_ "SELECT 1")"

# --- the reply stream must have exactly one reply per command ---------------
# Counted in bytes rather than by a client-side counter: "+OK" is five bytes, so
# a duplicated reply is arithmetic rather than a judgement call. This is the
# assertion that caught the double-execute; the latency one below would not have.
cat > /tmp/pgks_replycount.py <<'PYEOF'
import socket, sys, time
port = int(sys.argv[1]); N = int(sys.argv[2])
s = socket.create_connection(('127.0.0.1', port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
val = b'v' * 1024
out = bytearray()
for i in range(N):
    k = b'rc:%d' % i
    out += b'*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (len(k), k, len(val), val)
s.sendall(out)
buf = b''
s.settimeout(30)
deadline = time.monotonic() + 180
while time.monotonic() < deadline and len(buf) < 5 * N:
    try:
        d = s.recv(1 << 16)
    except OSError:
        break
    if not d:
        break
    buf += d
# Give any surplus reply a chance to show up rather than declaring victory the
# instant the expected count is reached.
time.sleep(0.5)
s.setblocking(False)
try:
    while True:
        more = s.recv(1 << 16)
        if not more:
            break
        buf += more
except OSError:
    pass
print(len(buf) - 5 * N)
PYEOF
RC_N=${PGKS_RC_N:-50000}
RC_DELTA=$(timeout 300 python3 /tmp/pgks_replycount.py $RESP $RC_N 2>&1 | tail -1)
chk "exactly one reply per command, with the ring full throughout ($RC_N cmds, delta ${RC_DELTA}B)" "0" "$RC_DELTA"

# --- an unrelated connection keeps being served -----------------------------
cat > /tmp/pgks_victim.py <<'PYEOF'
import socket, sys, time
port = int(sys.argv[1]); secs = float(sys.argv[2])
s = socket.create_connection(('127.0.0.1', port))
s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
lat = []; t_end = time.monotonic() + secs; i = 0
s.settimeout(30)
while time.monotonic() < t_end:
    i += 1
    k = b'victim:%d' % i
    t0 = time.monotonic()
    try:
        s.sendall(b'*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$2\r\nvv\r\n' % (len(k), k))
        r = s.recv(64)
    except OSError:
        break
    lat.append((time.monotonic() - t0) * 1000)
    if not r:
        break
lat.sort()
n = len(lat)
print('%d %.1f' % (n, lat[-1] if n else 999999))
PYEOF
# Flood on one connection while a second does ordinary sequential writes.
seq 1 60000 | awk '{print "SET flood:"$1" "sprintf("%01024d", $1)}' > /tmp/pgks_flood.txt
(timeout 180 redis-cli -p $RESP --pipe < /tmp/pgks_flood.txt >/dev/null 2>&1) &
FLOOD_PID=$!
sleep 1
read -r VIC_OPS VIC_MAX <<<"$(timeout 120 python3 /tmp/pgks_victim.py $RESP 8 2>&1 | tail -1)"
wait $FLOOD_PID 2>/dev/null
echo "  (victim managed ${VIC_OPS:-0} ops, worst ${VIC_MAX:-?}ms, while another connection flooded)"
# Budgets are deliberately loose: the point is to catch a connection being
# frozen out, not to pin a latency number on a shared CI runner. Unfixed, this
# was 1 op and 9860ms.
chk "an unrelated connection is still served during a flood (${VIC_OPS:-0} ops)" "1" \
    "$([ "${VIC_OPS:-0}" -ge 20 ] && echo 1 || echo 0)"
chk "and its worst round trip stays bounded (${VIC_MAX:-?}ms)" "1" \
    "$(awk -v m="${VIC_MAX:-999999}" 'BEGIN{print (m < 2000) ? 1 : 0}')"

stop_pg; sleep 1
set_conf "pg_keyspace.ring_mb" "8"
start_pg; wait_ready; sleep 2
chk "the cluster is healthy again at the original ring size" "1" "$(psql_ "SELECT 1")"

echo ""
echo "########## AG. a cached plan never returns fewer rows than the heap has (#85) ##########"
# Whether to use the row cache is decided at PLAN time; the lookup happens at
# EXECUTION time. rc_access treated a miss as end of scan, so anything that
# removed the entry in between turned a correct query into an empty result. With
# a cached plan the window is unbounded, because the plan outlives the entry and
# row-cache activity does not invalidate plans.
#
# Eviction is used to remove the entry rather than an invalidation: it needs no
# decode worker (so this runs wherever the suite does), and it is ordinary
# operation for any working set larger than pg_keyspace.rowcache_mb rather than
# a failure mode.
stop_pg; sleep 1
set_conf "pg_keyspace.rowcache_mb" "1"
start_pg; wait_ready; sleep 2
# Invalidation is on by this point in the suite, and the cache is not served
# until its worker beats, so wait for that instead of assuming two seconds did it.
chk "the invalidation worker is up, so the cache is served at all" "1" "$(wait_coherent 30)"
psql_ "DROP TABLE IF EXISTS public.ev CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.ev(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.ev SELECT g, repeat('x', 900)||g FROM generate_series(1,4000) g" >/dev/null
psql_ "SELECT supacache.rowcache_register('public.ev', 1)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.ev', 2)" >/dev/null
EV_HEAP=$(psql_ "SELECT length(v) FROM public.ev WHERE id=2")
# Without this the section proves nothing: if the row were never cached, the
# plan would be an ordinary index scan and the cached-plan path untested.
chk "the row under test is served from the cache at plan time" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.ev WHERE id=2" | grep -c 'pg_keyspace_rowcache')"

# One session throughout: plan once while the row is cached, evict it, then reuse
# the very same plan. force_generic_plan makes the reuse explicit; a prepared
# statement reaches a generic plan on its own after five executions.
#
# The eviction range must name rows that actually exist: rowcache_put on a
# missing row caches nothing, so a range past the end of the table evicts
# nothing and the section passes without testing anything. The "really was
# evicted" assertion below exists to catch exactly that, and did.
# The last bare line of the session is the second EXECUTE's result.
# Sentinels around the second EXECUTE rather than `tail -1`: when it returns no
# rows psql prints nothing, and the last line is then whatever came before it --
# which reported the failure as the eviction count, an opaque number that looks
# like a value. Between the markers, empty means empty.
EV_RAW=$(timeout 300 $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tA <<'SQL' 2>&1
SET plan_cache_mode = force_generic_plan;
PREPARE p AS SELECT length(v) FROM public.ev WHERE id = 2;
EXECUTE p;
SELECT count(*) FROM (SELECT supacache.rowcache_put('public.ev', g) FROM generate_series(100,3900) g) t;
\echo RCBEGIN
EXECUTE p;
\echo RCEND
SQL
)
EV_AFTER=$(echo "$EV_RAW" | awk '/^RCBEGIN$/{f=1;next} /^RCEND$/{f=0} f' | tr -d '[:space:]')
[ -z "$EV_AFTER" ] && EV_AFTER="NOROWS"
chk "a cached plan still returns the row after its cache entry is evicted (got '${EV_AFTER:-}')" "$EV_HEAP" "$EV_AFTER"
# And the entry really was gone, or the assertion above passed without testing
# anything: a fresh plan for the same row must now take the heap path.
chk "and the entry really was evicted (a fresh plan no longer uses the cache)" "0" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.ev WHERE id=2" | grep -c 'pg_keyspace_rowcache')"
chk "the heap still agrees" "$EV_HEAP" "$(psql_ "SELECT length(v) FROM public.ev WHERE id=2")"

stop_pg; sleep 1
set_conf "pg_keyspace.rowcache_mb" "64"
start_pg; wait_ready; sleep 2
chk "the cluster is healthy again at the original row-cache size" "1" "$(psql_ "SELECT 1")"

echo ""
echo "########## AH. a busy cache does not un-register its own tables (#87) ##########"
# rowcache_register stored the pk attnum as an ordinary entry in the row cache,
# competing with the cached rows for the same arena and the same CLOCK eviction.
# Evicted, rc_pathlist_hook found no registration and stopped substituting the
# CustomScan for that table, so caching silently turned itself off under exactly
# the load it exists to serve. No wrong answers -- reads fall back to the heap --
# but the feature stops working and nothing reports it.
stop_pg; sleep 1
set_conf "pg_keyspace.rowcache_mb" "1"
start_pg; wait_ready; sleep 2
chk "the invalidation worker is up, so the cache is served at all" "1" "$(wait_coherent 30)"
psql_ "DROP TABLE IF EXISTS public.rp CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.rp(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.rp SELECT g, repeat('x',900)||g FROM generate_series(1,4000) g" >/dev/null
psql_ "SELECT supacache.rowcache_register('public.rp', 1)" >/dev/null
psql_ "SELECT supacache.rowcache_put('public.rp', 7)" >/dev/null
# Without this the section proves nothing: if the table were never usable from
# the cache, "still usable after pressure" would hold trivially.
chk "the table is cached before any pressure" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.rp WHERE id=7" | grep -c 'pg_keyspace_rowcache')"
psql_ "SELECT count(*) FROM (SELECT supacache.rowcache_put('public.rp', g) FROM generate_series(100,3900) g) t" >/dev/null
RP_ENTRIES=$(psql_ "SELECT sum(entries)::text FROM supacache.rowcache_stats()")
# The pressure has to be real, or the registration was never at risk.
chk "the cache filled and evicted (holding $RP_ENTRIES of 3801 put)" "1" \
    "$([ "${RP_ENTRIES:-0}" -lt 3801 ] && echo 1 || echo 0)"
# Re-cache the row and ask the planner again. If the registration was evicted,
# rc_pathlist_hook declines and this is 0 however many times the row is put.
psql_ "SELECT supacache.rowcache_put('public.rp', 7)" >/dev/null
chk "the table is still registered after the cache filled" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.rp WHERE id=7" | grep -c 'pg_keyspace_rowcache')"
chk "and the row still reads correctly" "901" "$(psql_ "SELECT length(v) FROM public.rp WHERE id=7")"
# Pinning exempts an entry from eviction; it must not exempt it from an explicit
# unregister, or a table could never be taken back out of the cache.
psql_ "SELECT supacache.rowcache_unregister('public.rp')" >/dev/null
chk "unregistering still works on a pinned registration" "0" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.rp WHERE id=7" | grep -c 'pg_keyspace_rowcache')"

stop_pg; sleep 1
set_conf "pg_keyspace.rowcache_mb" "64"
start_pg; wait_ready; sleep 2
chk "the cluster is healthy again at the original row-cache size" "1" "$(psql_ "SELECT 1")"

echo ""
echo "########## AI. the row cache fails closed when nothing is invalidating it (#39) ##########"
# The row cache holds raw pre-policy rows and is only as correct as the worker
# that invalidates them. If that worker stops, entries go stale with no bound and
# nothing says so -- #39's "serves stale rows indefinitely with no alarm".
#
# Reads now refuse a cache whose invalidation is configured but not running, at
# plan time and again at execution time, because a plan outlives the condition.
# With the heap fallback from #85 in place that costs a fetch rather than an
# answer, which is what makes failing closed safe to do at all.
chk "coherence is reported, and true while the worker is beating" "t" \
    "$(psql_ "SELECT coherent FROM supacache.rowcache_coherence()")"
psql_ "DROP TABLE IF EXISTS public.fc CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.fc(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.fc VALUES (1,'one'),(2,'two')" >/dev/null
psql_ "SELECT supacache.rowcache_register('public.fc', 1)" >/dev/null
sleep 6
psql_ "SELECT supacache.rowcache_put('public.fc', 2)" >/dev/null
# Without this the section proves nothing: a cache that was never used cannot be
# observed to stop being used.
chk "the row is served from the cache while invalidation is healthy" "1" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.fc WHERE id=2" | grep -c 'pg_keyspace_rowcache')"
chk "and reads correctly" "two" "$(psql_ "SELECT v FROM public.fc WHERE id=2")"

# Stop the invalidation worker without touching anything else. SIGKILL rather
# than a clean shutdown: a worker that is killed cannot flush, disable itself, or
# leave any note, which is precisely the case the heartbeat exists for.
INVAL_PID=$(ps -eo pid,args --no-headers | awk '/pg_keyspace: rowcache invalidation worker/ && !/awk/ {print $1}' | head -1)
chk "the invalidation worker was running to begin with" "1" \
    "$([ -n "$INVAL_PID" ] && echo 1 || echo 0)"
[ -n "$INVAL_PID" ] && kill -9 "$INVAL_PID" 2>/dev/null
# Wait out the staleness window rather than guessing: the heartbeat has to age
# past pg_keyspace.watchdog_secs before a reader distrusts it.
STALE_MS=$(psql_ "SELECT stale_after_ms FROM supacache.rowcache_coherence()")
STALE_WAIT=$(( ${STALE_MS:-30000} / 1000 + 15 ))
GONE=0
for _ in $(seq 1 $STALE_WAIT); do
  [ "$(psql_ "SELECT coherent FROM supacache.rowcache_coherence()")" = "f" ] && { GONE=1; break; }
  sleep 1
done
chk "coherence goes false once the worker stops beating (within ${STALE_WAIT}s)" "1" "$GONE"
# The point of all of it: the cache is no longer served, and the answer is still
# right because the read falls through to the heap (#85).
chk "a fresh plan stops using the cache" "0" \
    "$(psql_ "EXPLAIN (COSTS OFF) SELECT v FROM public.fc WHERE id=2" | grep -c 'pg_keyspace_rowcache')"
chk "and the row still reads correctly from the heap" "two" \
    "$(psql_ "SELECT v FROM public.fc WHERE id=2")"
# It must recover on its own: the watchdog relaunches the worker, it beats again,
# and the cache becomes usable without operator action.
chk "coherence returns once the worker is back" "1" "$(wait_coherent 90)"

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
