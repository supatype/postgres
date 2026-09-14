#!/usr/bin/env bash
# The row cache never crosses a database boundary (#117, #118).
#
# Three facts combined into a cross-database data leak:
#
#   * shared_preload_libraries loads the extension per CLUSTER, so the planner
#     hook is installed in every database -- not only those that ran
#     CREATE EXTENSION;
#   * the row-cache segment is cluster-wide shared memory;
#   * row-cache keys were `relid ++ pk`, with no database in them.
#
# Relids are per-database, and `CREATE DATABASE ... TEMPLATE` copies pg_class
# physically -- so two cloned databases have IDENTICAL relids. That made the
# collision certain rather than unlikely in the per-project-database pattern: a
# registration in one database caused queries in another to be served the first
# database's rows, with a correct-looking Custom Scan plan and no error.
#
# RLS does not save you: it is re-applied above the cache, so the second
# database's policies are evaluated against the first database's row.
#
# Keys now carry the database oid and a tag byte. A database that has not
# registered the table finds no registration and plans an ordinary index scan.
#
# Since #120 the row cache serves EVERY database, so section 4's assertion is
# inverted: registering from a second database now succeeds. Sections 1-3 are
# unchanged and matter more than they did, not less -- cross-database isolation
# was a latent hazard when only one database was ever served and is a live one
# now that several are.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-dbscope}
PORT=${PGKS_PG_PORT:-5451}
RESP=${PGKS_RESP_PORT:-6421}
PROFILE=${PGKS_BUILD_PROFILE:-release}
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

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/dbscope_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/dbscope_install.log; exit 1; }
PLUGIN_OK=1
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/dbscope_plugin.log 2>&1 || PLUGIN_OK=0
chk "the supacache_keys output plugin builds and installs" "1" "$PLUGIN_OK"
[ "$PLUGIN_OK" = "1" ] || { tail -20 /tmp/dbscope_plugin.log; exit 1; }

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.rowcache_decode = on"
  echo "pg_keyspace.rowcache_decode_ms = 500"
  # Two databases are served here (section 4), so give each one its own pool
  # worker: cycling is a different property and has its own harness
  # (run_rowcache_multidb.sh section 9).
  echo "pg_keyspace.rowcache_invalidation_workers = 2"
  echo "wal_level = logical"
  # One slot per participating database, plus the legacy slot section 1b
  # creates by hand.
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
  echo "max_worker_processes = 12"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi

# Two "project" databases cloned from one template, so their relids match.
Q "CREATE DATABASE tmpl" >/dev/null
Q "CREATE TABLE public.orders(id bigint primary key, v text)" tmpl >/dev/null
Q "CREATE DATABASE proj_a TEMPLATE tmpl" >/dev/null
Q "CREATE DATABASE proj_b TEMPLATE tmpl" >/dev/null
Q "INSERT INTO public.orders VALUES (1,'SECRET-OF-PROJECT-A')" proj_a >/dev/null
Q "INSERT INTO public.orders VALUES (1,'project-b-own-row')" proj_b >/dev/null
# pg_keyspace.database is still where the RESP keyspace and persistence live;
# since #120 it no longer decides which database the ROW CACHE serves.
echo "pg_keyspace.database = 'proj_a'" >> $PGDATA/postgresql.conf
restart
Q "CREATE EXTENSION pg_keyspace" proj_a >/dev/null
restart

echo
echo "########## 1. the precondition: two databases, one relid ##########"
# If the relids differed, everything below would pass with the bug present.
A_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_a)
B_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_b)
chk "the cloned databases really share a relid ($A_OID / $B_OID)" "same" \
    "$([ "$A_OID" = "$B_OID" ] && echo same || echo different)"

# Databases are picked up LAZILY since #120: one with no registrations gets no
# slot, no worker and no turn, and therefore reads as NOT coherent. That is the
# design, not a fault -- there is nothing to keep coherent -- so registration
# has to come before any assertion about slots or coherence.
chk "with nothing registered, proj_a has no slot and is not served" "f" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)"
chk "registering in proj_a succeeds" "t" \
    "$(Q "SELECT supacache.rowcache_register('public.orders')" proj_a)"
for _ in $(seq 1 90); do [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)" = "t" ] && break; sleep 1; done
chk "and a pool worker then picks it up, so the cache is served at all" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)"

echo
echo "########## 1b. the decode slot is named for the database ##########"
# A logical slot belongs to the database it was created in and only ever decodes
# changes from that database, so the slot has never been able to mean anything
# else -- and naming it for the database is what lets there be more than one
# (#120). The oid, not the name: a slot name is capped at 63 characters, a
# database name can fill that on its own, and two long names sharing a prefix
# would truncate to the SAME slot. Two databases sharing one slot is the leak
# below, one layer down, with the invalidations crossing instead of the rows.
A_DBOID=$(Q "SELECT oid FROM pg_database WHERE datname='proj_a'")
chk "a slot exists named for proj_a's oid" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots \
          WHERE slot_name = 'supacache_rowcache_$A_DBOID' AND plugin = 'supacache_keys'")"
chk "and it belongs to proj_a" "proj_a" \
    "$(Q "SELECT database FROM pg_replication_slots WHERE slot_name='supacache_rowcache_$A_DBOID'")"
chk "no slot is left under the bare configured name" "0" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'supacache_rowcache'")"

# The upgrade path. A cluster that ran an earlier version has a slot named for
# the configuration verbatim, and after the upgrade NOTHING consumes it -- an
# unconsumed slot pins WAL from its restart_lsn forever, which is the worst
# operational failure this subsystem has and would arrive silently, on a cluster
# that had done nothing but upgrade. Simulated here by creating that slot by
# hand and restarting into it.
Q "SELECT pg_create_logical_replication_slot('supacache_rowcache','supacache_keys')" proj_a >/dev/null
chk "(setup) a pre-upgrade slot exists to be cleaned up" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'supacache_rowcache'")"
restart
for _ in $(seq 1 40); do
  [ "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='supacache_rowcache'")" = "0" ] && break
  sleep 1
done
chk "the worker drops the pre-upgrade slot rather than leaving it pinning WAL" "0" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'supacache_rowcache'")"
chk "and says so in the log, naming what it dropped" "1" \
    "$(grep -c "dropped the pre-per-database slot 'supacache_rowcache'" $PGDATA/log || true)"
chk "the per-database slot is untouched" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name = 'supacache_rowcache_$A_DBOID'")"
for _ in $(seq 1 40); do [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)" = "t" ] && break; sleep 1; done
chk "and invalidation is still healthy afterwards" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)"

echo
echo "########## 2. proj_a caches its own row ##########"
chk "the row caches" "t" "$(Q "SELECT supacache.rowcache_put('public.orders', 1)" proj_a)"
chk "proj_a is served from the cache" "1" \
    "$(Q "EXPLAIN SELECT v FROM public.orders WHERE id=1" proj_a | grep -c pg_keyspace_rowcache)"
chk "and reads its own row" "SECRET-OF-PROJECT-A" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_a)"

echo
echo "########## 3. proj_b is NOT served proj_a's row ##########"
# The leak, as a direct assertion. proj_b installed no extension and registered
# nothing; before the fix it got a Custom Scan plan and proj_a's data.
chk "proj_b plans an ordinary index scan, not the cache" "0" \
    "$(Q "EXPLAIN SELECT v FROM public.orders WHERE id=1" proj_b | grep -c pg_keyspace_rowcache)"
chk "and reads ITS OWN row" "project-b-own-row" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_b)"
chk "proj_b never saw proj_a's value at all" "0" \
    "$(Q "SELECT count(*) FROM public.orders WHERE v LIKE 'SECRET%'" proj_b)"

echo
echo "########## 4. registering outside pg_keyspace.database now WORKS ##########"
# This assertion is inverted from what it used to be, and deliberately so.
#
# It used to check that registration here was REFUSED, and the refusal was
# right at the time: there was one invalidation slot and it lived in
# pg_keyspace.database, so a table registered anywhere else would be cached and
# then never invalidated -- stale indefinitely, with coherence still reporting
# healthy because it described the worker rather than your table (#118).
#
# #120 removed the cause rather than the symptom: every database with
# registrations gets its own slot and its own turn in a bounded pool. So the
# refusal goes, and what replaces it is the stronger claim -- proj_b is served
# from the cache AND still cannot see proj_a's rows.
#
# Sections 1-3 above are untouched and matter MORE than they did, not less:
# cross-database isolation was a latent hazard when only one database was ever
# served, and is a live one now that both are.
Q "CREATE EXTENSION pg_keyspace" proj_b >/dev/null 2>&1
chk "registration from a second database succeeds" "t" \
    "$(Q "SELECT supacache.rowcache_register('public.orders')" proj_b)"
for _ in $(seq 1 90); do
  [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_b)" = "t" ] && break
  sleep 1
done
chk "proj_b becomes coherent under its own slot" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_b)"
B_DBOID=$(Q "SELECT oid FROM pg_database WHERE datname='proj_b'")
chk "and that slot is proj_b's own, not proj_a's" "proj_b" \
    "$(Q "SELECT database FROM pg_replication_slots WHERE slot_name='supacache_rowcache_$B_DBOID'")"
chk "proj_b is now served from the cache" "1" \
    "$(Q "SELECT supacache.rowcache_put('public.orders', 1)" proj_b >/dev/null;
        Q "EXPLAIN SELECT v FROM public.orders WHERE id=1" proj_b | grep -c pg_keyspace_rowcache)"
# The #117 assertion, repeated under the regime that makes it live. Identical
# relids, both databases cached, one shared segment -- and still each reads only
# its own row.
chk "and STILL reads its own row, not proj_a's" "project-b-own-row" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_b)"
chk "proj_a is likewise unaffected" "SECRET-OF-PROJECT-A" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_a)"
chk "neither database ever saw the other's value" "0" \
    "$(Q "SELECT count(*) FROM public.orders WHERE v LIKE 'SECRET%'" proj_b)"
# Invalidation is per-database too: a write in proj_b must not disturb proj_a's
# cached row, and must reach proj_b's.
Q "UPDATE public.orders SET v='project-b-updated' WHERE id=1" proj_b >/dev/null
OKB=timeout
for _ in $(seq 1 90); do
  [ "$(Q "SELECT v FROM public.orders WHERE id=1" proj_b)" = "project-b-updated" ] && { OKB=ok; break; }
  sleep 1
done
chk "a write in proj_b is invalidated in proj_b" "ok" "$OKB"
chk "and proj_a's cached row is untouched by it" "SECRET-OF-PROJECT-A" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_a)"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
