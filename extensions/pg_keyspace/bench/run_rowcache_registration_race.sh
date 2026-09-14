#!/usr/bin/env bash
# Creating a row-cache catalogue or a decode slot must be race-free (#120, on
# the #130 pattern).
#
# `CREATE TABLE IF NOT EXISTS` is NOT race-free: two sessions can both pass the
# existence check before either inserts its catalogue row, and the loser raises
# `duplicate_table`. `let _ =` was never protection -- a Postgres ERROR inside
# SPI longjmps out and aborts the transaction, which pgrx surfaces as a panic
# that takes the worker with it, so the discarded Result never sees it. Measured
# in #130: four persistence workers, 44 deaths a minute, one per bucket rollover.
#
# This design has the same shape in three places, and unlike #130 the entrants
# are not workers on a timer but PEOPLE and a pool arriving at once:
#
#   * several backends calling rowcache_register in a database that has no
#     `supacache.rowcache_reg` yet -- each of them tries to create the schema and
#     the table;
#   * pool workers probing the same fresh database at the same moment;
#   * `pg_create_logical_replication_slot`, which has the same hazard under its
#     own error (`duplicate_object`) and cannot use the plpgsql EXCEPTION handler
#     the tables use, because it has to run through read-only SPI and a DO block
#     is a utility statement read-only SPI will not run. It takes an advisory
#     lock and re-checks under it instead.
#
# The assertions are the #130 ones: no worker deaths, no duplicate-object errors
# in the log, and every registration recorded exactly once. Plus the one that
# makes them mean something -- that the race really was contended, rather than
# the run having serialised itself by accident.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-regrace}
PORT=${PGKS_PG_PORT:-5475}
RESP=${PGKS_RESP_PORT:-6477}
PROFILE=${PGKS_BUILD_PROFILE:-release}
DBCOUNT=${DBCOUNT:-4}
RACERS=${RACERS:-8}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d "${2:-postgres}" -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
# `pg_ctl -w stop` gives up after its own timeout and returns non-zero with the
# postmaster STILL shutting down. Discarding that status and starting another one
# a second later starts it on top of a live postmaster: that start fails, and
# every readiness poll then reads `FATAL: the database system is shutting down`
# until the loop expires -- surfacing as whichever assertion came next, pointing
# at the feature under test and nothing to do with it (#120). Shutdown length
# tracks how much the persistence worker has to flush, so it bites after a heavy
# section and passes everywhere else. Verify it rather than assume it.
stop_pg() {
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 120 stop -m fast" >/dev/null 2>&1
  for _ in $(seq 1 120); do
    su postgres -c "$PGBIN/pg_ctl -D $PGDATA status" >/dev/null 2>&1 || return 0
    sleep 1
  done
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 60 stop -m immediate" >/dev/null 2>&1
  return 0
}
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }
log_lines() { wc -l < $PGDATA/log 2>/dev/null || echo 0; }
since() { tail -n +"${1:-0}" $PGDATA/log 2>/dev/null; }

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/regrace_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/regrace_install.log; exit 1; }
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/regrace_plugin.log 2>&1 || {
  echo "PLUGIN INSTALL FAILED"; tail -20 /tmp/regrace_plugin.log; exit 1; }

echo "=== cluster ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.rowcache_decode = on"
  # Fast, so the pool churns through databases DURING the registration storm
  # rather than after it. Probing workers are half the entrants in this race.
  echo "pg_keyspace.rowcache_decode_ms = 100"
  echo "pg_keyspace.rowcache_lease_ms = 2000"
  echo "pg_keyspace.rowcache_invalidation_workers = 3"
  echo "wal_level = logical"
  echo "max_replication_slots = 24"
  echo "max_wal_senders = 24"
  echo "max_worker_processes = 16"
  echo "max_connections = 200"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi

Q "CREATE DATABASE tmpl" >/dev/null
Q "CREATE TABLE public.t(id bigint primary key, v text)" tmpl >/dev/null
Q "CREATE TABLE public.u(id bigint primary key, v text)" tmpl >/dev/null
DBS=""
for i in $(seq 1 $DBCOUNT); do
  Q "CREATE DATABASE race_$i TEMPLATE tmpl" >/dev/null
  DBS="$DBS race_$i"
done
restart
# CREATE EXTENSION only. The catalogue is deliberately NOT created here: the
# whole point is that the first registrations arrive into a database that has
# no supacache.rowcache_reg, so every racer tries to create it.
for d in $DBS; do Q "CREATE EXTENSION pg_keyspace" $d >/dev/null; done
restart

echo
echo "########## 0. the precondition: no catalogue yet ##########"
# If the table already existed, every racer below would take the cheap path and
# the run would prove nothing.
MISSING=0
for d in $DBS; do
  [ "$(Q "SELECT to_regclass('supacache.rowcache_reg') IS NULL" $d)" = "t" ] && MISSING=$((MISSING+1))
done
chk "none of the $DBCOUNT databases has a row-cache catalogue yet" "$DBCOUNT" "$MISSING"

echo
echo "########## 1. $RACERS concurrent registrations per database, $DBCOUNT databases ##########"
N0=$(log_lines)
# The invalidation POOL is deliberately excluded: its members exit and are
# relaunched every lease, by design, so their pids changing says nothing. The
# RESP, persistence and expiry workers do not cycle, so a change in THEIR pids
# is a death.
PIDS0=$(Q "SELECT string_agg(pid::text, ',' ORDER BY pid) FROM pg_stat_activity
           WHERE backend_type LIKE '%pg_keyspace%'
             AND backend_type NOT LIKE '%invalidation%'")
for d in $DBS; do
  for r in $(seq 1 $RACERS); do
    ( Q "SELECT supacache.rowcache_register('public.t')" $d >/dev/null 2>&1
      Q "SELECT supacache.rowcache_register('public.u')" $d >/dev/null 2>&1 ) &
  done
done
wait
sleep 5

DUP=$(since $N0 | grep -ciE "duplicate_table|duplicate_object|duplicate_schema|already exists" || true)
chk "no duplicate-object error was raised" "0" "$DUP"
# An assertion that only says a race leaked is an assertion you cannot act on:
# which object raced decides whether the hole is in the catalogue DDL, the slot's
# advisory lock, or somewhere with no guard at all. Print the evidence.
[ "${DUP:-0}" != "0" ] && {
  echo "--- duplicate-object evidence ---"
  since $N0 | grep -iE "duplicate_table|duplicate_object|duplicate_schema|already exists" | head -20
  echo "---"
}
DEAD=$(since $N0 | grep -ciE "pg_keyspace.*(exit code 1|was terminated|FATAL)" || true)
chk "no pg_keyspace worker died" "0" "$DEAD"
chk "no backend segfaulted" "0" "$(since $N0 | grep -c 'signal 11')"
chk "the cluster is still up" "1" "$(Q "SELECT 1")"

echo
echo "########## 2. every registration recorded exactly once ##########"
# ON CONFLICT makes the INSERT idempotent, so the failure this catches is a
# catalogue created twice or a row lost to a rolled-back subtransaction.
for d in $DBS; do
  chk "  $d has exactly 2 registrations" "2" "$(Q "SELECT count(*) FROM supacache.rowcache_reg" $d)"
  chk "  $d has exactly one supacache schema" "1" \
      "$(Q "SELECT count(*) FROM pg_namespace WHERE nspname='supacache'" $d)"
  chk "  $d has exactly one rowcache_reg table" "1" \
      "$(Q "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
            WHERE n.nspname='supacache' AND c.relname='rowcache_reg'" $d)"
done

echo
echo "########## 3. one slot per database, never two ##########"
# `pg_create_logical_replication_slot` has the same hazard under its own error,
# and a pool of three workers probing four fresh databases is a real race for it.
for _ in $(seq 1 90); do
  [ "$(Q "SELECT count(*) FROM pg_replication_slots WHERE plugin='supacache_keys'")" = "$DBCOUNT" ] && break
  sleep 1
done
chk "exactly $DBCOUNT slots exist, one per database" "$DBCOUNT" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE plugin='supacache_keys'")"
chk "no database has more than one" "0" \
    "$(Q "SELECT count(*) FROM (SELECT database FROM pg_replication_slots
          WHERE plugin='supacache_keys' GROUP BY database HAVING count(*) > 1) x")"

echo
echo "########## 4. the workers that raced are the SAME processes ##########"
# The #130 signature was not an error in the log but workers quietly dying and
# being relaunched two seconds later. Comparing pids before and after is what
# catches a race that was survived by restarting rather than by not happening.
PIDS1=$(Q "SELECT string_agg(pid::text, ',' ORDER BY pid) FROM pg_stat_activity
           WHERE backend_type LIKE '%pg_keyspace%'
             AND backend_type NOT LIKE '%invalidation%'")
chk "no non-cycling pg_keyspace worker was replaced during the storm" "$PIDS0" "$PIDS1"
# The pool members do cycle, so they are judged on how they left instead: an
# orderly hand-over logs that it is handing over, a #130-style death does not.
chk "every pool worker that stopped did so deliberately" "0" \
    "$(since $N0 | grep -ciE 'rowcache invalidation worker.*(exit code 1|terminated by signal)' || true)"

echo
echo "########## 5. and the databases actually work afterwards ##########"
# A race "survived" by leaving the catalogue half-built would show up here.
for d in $DBS; do
  Q "INSERT INTO public.t VALUES (1,'x-$d')" $d >/dev/null
done
for _ in $(seq 1 90); do
  OK=1
  for d in $DBS; do
    [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" $d)" = "t" ] || OK=0
  done
  [ "$OK" = "1" ] && break
  sleep 1
done
for d in $DBS; do
  chk "  $d is coherent" "t" "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" $d)"
  Q "SELECT supacache.rowcache_put('public.t', 1)" $d >/dev/null
  chk "  $d serves its row from the cache" "x-$d" "$(Q "SELECT v FROM public.t WHERE id=1" $d)"
done

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
