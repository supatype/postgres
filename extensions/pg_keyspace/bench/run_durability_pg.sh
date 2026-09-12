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
    chk_contains "the cause is recorded as a storage failure" "No space left" "$(tail -200 $PGDATA/log)"

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
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
