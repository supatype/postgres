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
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
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
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
