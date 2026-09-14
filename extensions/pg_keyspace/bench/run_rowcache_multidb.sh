#!/usr/bin/env bash
# The Mode B row cache works in EVERY database, not just pg_keyspace.database
# (#120, subsuming #118).
#
# What used to be refused was not a limitation of the catalogue or of the keys
# but of INVALIDATION: a logical replication slot belongs to the database it was
# created in and only ever decodes changes from that database, so with one slot
# in one database, a table registered anywhere else would be cached and then
# NEVER invalidated -- stale indefinitely, while coherence still reported healthy,
# because coherence described the worker rather than your table.
#
# Every database now gets its own slot and its own turn in a BOUNDED pool. The
# assertions below are in the order the design has to be true in:
#
#   1. the cloned databases really share a relid, or sections 2-4 would pass
#      with the #117 leak present;
#   2. registration succeeds in a database that is not pg_keyspace.database;
#   3. each database is served its OWN row, which is #117's regression guard and
#      matters MORE now that multi-database is the normal case rather than the
#      refused one;
#   4. a write in a SECONDARY database is actually invalidated. This is the one
#      assertion the whole issue turns on: everything else can be true while the
#      cache quietly serves a row nothing will ever take back out;
#   5. a database with no registrations gets no slot, and one that loses its last
#      registration has its slot dropped -- a slot retains WAL until it is
#      consumed, so a forgotten one pins WAL for the whole cluster;
#   6. the pool is BOUNDED by the GUC. Worker count is something an operator
#      sets, not a function of how many databases exist. With more databases
#      than workers invalidation cycles, and every database must still be
#      invalidated -- a knob, not a wall.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-multidb}
PORT=${PGKS_PG_PORT:-5471}
RESP=${PGKS_RESP_PORT:-6473}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d "${2:-postgres}" -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop -m fast" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }
set_conf() { sed -i "/^$1 /d;/^$1=/d" $PGDATA/postgresql.conf; echo "$1 = $2" >> $PGDATA/postgresql.conf; }

DBS="proj_a proj_b proj_c"

# Wait until a database's row cache is trusted. Reads FAIL CLOSED while it is
# not, so without this every later assertion would be racing the pool.
wait_coherent() { # <db> [timeout_s]
  for _ in $(seq 1 "${2:-90}"); do
    [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" "$1")" = "t" ] && return 0
    sleep 1
  done
  return 1
}
# Wait for a value to reach a cached read. The point of the whole issue: a write
# in this database must be taken back out of the cache by something.
wait_value() { # <db> <query> <expected> [timeout_s]
  for _ in $(seq 1 "${4:-90}"); do
    [ "$(Q "$2" "$1")" = "$3" ] && return 0
    sleep 1
  done
  return 1
}
slot_of() { Q "SELECT 'supacache_rowcache_' || oid FROM pg_database WHERE datname='$1'"; }
live_workers() {
  Q "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE '%rowcache invalidation%'"
}

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/multidb_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/multidb_install.log; exit 1; }
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/multidb_plugin.log 2>&1 || {
  echo "PLUGIN INSTALL FAILED"; tail -20 /tmp/multidb_plugin.log; exit 1; }

echo "=== cluster ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.rowcache_decode = on"
  echo "pg_keyspace.rowcache_decode_ms = 200"
  echo "pg_keyspace.rowcache_readthrough = on"
  echo "pg_keyspace.rowcache_lease_ms = 3000"
  # One worker per participating database to begin with, so sections 1-5 test
  # per-database invalidation WITHOUT cycling. Section 6 turns it down to 1 and
  # asserts that cycling still invalidates everything.
  echo "pg_keyspace.rowcache_invalidation_workers = 3"
  echo "wal_level = logical"
  # One slot per participating database, plus room for the test's own probes.
  echo "max_replication_slots = 16"
  echo "max_wal_senders = 16"
  # The pool comes out of this budget, alongside the RESP, persistence and
  # expiry workers. The default 8 is not enough for a pool of three.
  echo "max_worker_processes = 16"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi

# Four "project" databases cloned from ONE template, so their relids are
# identical -- the shape `CREATE DATABASE ... TEMPLATE` produces, and the reason
# #117's collision was certain rather than unlikely. proj_d registers nothing.
Q "CREATE DATABASE tmpl" >/dev/null
Q "CREATE TABLE public.orders(id bigint primary key, v text)" tmpl >/dev/null
Q "CREATE TABLE public.combo(a int, b text, v text, PRIMARY KEY (a,b))" tmpl >/dev/null
Q "CREATE TABLE public.byuuid(id uuid primary key, v text)" tmpl >/dev/null
for d in $DBS proj_d; do Q "CREATE DATABASE $d TEMPLATE tmpl" >/dev/null; done
for d in $DBS proj_d; do
  Q "INSERT INTO public.orders VALUES (1,'SECRET-OF-$d')" $d >/dev/null
  Q "INSERT INTO public.combo VALUES (7,'k','combo-of-$d')" $d >/dev/null
  Q "INSERT INTO public.byuuid VALUES ('11111111-1111-1111-1111-111111111111','uuid-of-$d')" $d >/dev/null
done
restart
for d in $DBS proj_d; do Q "CREATE EXTENSION pg_keyspace" $d >/dev/null; done
restart

echo
echo "########## 1. the precondition: the databases share a relid ##########"
# Without this, sections 2-4 would pass even with the #117 leak present.
A_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_a)
B_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_b)
C_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_c)
chk "the cloned databases share a relid ($A_OID/$B_OID/$C_OID)" "same" \
    "$([ "$A_OID" = "$B_OID" ] && [ "$B_OID" = "$C_OID" ] && echo same || echo different)"

echo
echo "########## 2. registration succeeds in EVERY database ##########"
# The whole of #118: this used to return false with a warning naming
# pg_keyspace.database, because there was one slot and it was not here.
for d in $DBS; do
  chk "  $d registers public.orders" "t" "$(Q "SELECT supacache.rowcache_register('public.orders')" $d)"
done
for d in $DBS; do
  chk "  $d becomes coherent (a worker took it)" "ok" "$(wait_coherent $d && echo ok || echo timeout)"
done
chk "the directory reports three participating databases" "3" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases WHERE state='participating'")"

echo
echo "########## 3. one slot PER DATABASE, and none for a database that wants none ##########"
for d in $DBS; do
  S=$(slot_of $d)
  chk "  $d has its own slot ($S)" "1" \
      "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$S' AND plugin='supacache_keys'")"
  chk "  and that slot belongs to $d" "$d" \
      "$(Q "SELECT database FROM pg_replication_slots WHERE slot_name='$S'")"
done
# Lazy launch. proj_d has the extension but no registrations, so it must cost
# nothing: no slot, no WAL retained, no turn in the cycle.
chk "proj_d registered nothing, so it has no slot" "0" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$(slot_of proj_d)'")"
chk "and the directory marks it idle rather than participating" "idle" \
    "$(Q "SELECT state FROM supacache.pg_stat_keyspace_rowcache_databases WHERE datname='proj_d'")"

echo
echo "########## 4. each database is served ITS OWN row (#117 regression) ##########"
# RLS does not save you here: it is re-applied ABOVE the cache, so a leak would
# evaluate the querying database's policies against another database's row.
for d in $DBS proj_d; do
  Q "SELECT supacache.rowcache_put('public.orders', 1)" $d >/dev/null
done
for d in $DBS; do
  chk "  $d plans through the cache" "1" \
      "$(Q "EXPLAIN (COSTS OFF) SELECT v FROM public.orders WHERE id=1" $d | grep -c pg_keyspace_rowcache)"
  chk "  $d reads its own row" "SECRET-OF-$d" "$(Q "SELECT v FROM public.orders WHERE id=1" $d)"
done
chk "no database can see another's value" "0" \
    "$(Q "SELECT count(*) FROM public.orders WHERE v <> 'SECRET-OF-proj_b'" proj_b)"

echo
echo "########## 5. a write in a SECONDARY database is INVALIDATED ##########"
# The assertion the whole issue turns on. Before this work a row cached in any
# database but pg_keyspace.database was never invalidated: this UPDATE would
# land in the heap and the cached read would keep returning the old value
# forever, with coherence still reporting healthy.
Q "UPDATE public.orders SET v='UPDATED-IN-proj_b' WHERE id=1" proj_b >/dev/null
chk "UPDATE in proj_b reaches the cached read" "ok" \
    "$(wait_value proj_b "SELECT v FROM public.orders WHERE id=1" "UPDATED-IN-proj_b" && echo ok || echo timeout)"
# ...and only there. An invalidation crossing databases would be the same bug
# wearing the other hat.
chk "and proj_a's cached row is untouched by it" "SECRET-OF-proj_a" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_a)"

Q "DELETE FROM public.orders WHERE id=1" proj_c >/dev/null
chk "DELETE in proj_c reaches the cached read" "ok" \
    "$(wait_value proj_c "SELECT count(*) FROM public.orders WHERE id=1" "0" && echo ok || echo timeout)"

# Read-through in a secondary database: the miss is what populates the cache, so
# this is the path that makes every backend a writer, in a database that never
# had one before.
Q "INSERT INTO public.orders VALUES (2,'warmed-by-read-through')" proj_a >/dev/null
chk "a read-through miss in proj_a caches the row it read" "warmed-by-read-through" \
    "$(Q "SELECT v FROM public.orders WHERE id=2" proj_a)"
Q "UPDATE public.orders SET v='after-readthrough' WHERE id=2" proj_a >/dev/null
chk "and that row is invalidated too" "ok" \
    "$(wait_value proj_a "SELECT v FROM public.orders WHERE id=2" "after-readthrough" && echo ok || echo timeout)"

echo
echo "########## 6. composite and non-integer keys, in a SECONDARY database ##########"
# The planner hook was effectively dead outside pg_keyspace.database until now:
# it ran everywhere, found no registration, and returned early. #128 is the
# cautionary example -- a null baserestrictinfo deref that survived this long
# partly because the hook's real work only ever happened in one database. So the
# key-shape matrix is exercised where it has never run before.
chk "a composite key registers in proj_b" "t" \
    "$(Q "SELECT supacache.rowcache_register('public.combo')" proj_b)"
chk "a uuid key registers in proj_b" "t" \
    "$(Q "SELECT supacache.rowcache_register('public.byuuid')" proj_b)"
Q "SELECT supacache.rowcache_put('public.combo', 7, 'k')" proj_b >/dev/null
Q "SELECT supacache.rowcache_put('public.byuuid', '11111111-1111-1111-1111-111111111111'::uuid)" proj_b >/dev/null
chk "the composite key reads back in proj_b" "combo-of-proj_b" \
    "$(Q "SELECT v FROM public.combo WHERE a=7 AND b='k'" proj_b)"
chk "the uuid key reads back in proj_b" "uuid-of-proj_b" \
    "$(Q "SELECT v FROM public.byuuid WHERE id='11111111-1111-1111-1111-111111111111'" proj_b)"
Q "UPDATE public.combo SET v='combo-updated' WHERE a=7 AND b='k'" proj_b >/dev/null
chk "and the composite key is invalidated in proj_b" "ok" \
    "$(wait_value proj_b "SELECT v FROM public.combo WHERE a=7 AND b='k'" "combo-updated" && echo ok || echo timeout)"
# The unfiltered-query shape from #128, now in a database where the hook does
# real work. A crash here takes the whole cluster down, so it is checked rather
# than assumed.
LOG0=$(wc -l < $PGDATA/log)
Q "SELECT count(*) FROM public.combo" proj_b >/dev/null
chk "a query with no WHERE clause does not crash a secondary database" "0" \
    "$(tail -n +$LOG0 $PGDATA/log | grep -c 'signal 11')"

echo
echo "########## 7. the views are per-database ##########"
# A single row would show one arbitrary database's decode lag and let an
# operator believe it was the cluster's.
chk "the invalidation view has one row per database with a slot" "3" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation")"
chk "and each row names the database its slot belongs to" "3" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation i
          JOIN pg_replication_slots s ON s.slot_name = i.slot_name
          WHERE s.database = i.datname")"
chk "every participating database reads as coherent" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases
          WHERE state='participating' AND NOT coherent")"
chk "the rowcache view names the database it is describing" "proj_a" \
    "$(Q "SELECT datname FROM supacache.pg_stat_keyspace_rowcache" proj_a)"
chk "...and the same view answers for proj_b when asked there" "proj_b" \
    "$(Q "SELECT datname FROM supacache.pg_stat_keyspace_rowcache" proj_b)"

echo
echo "########## 8. losing the last registration retires the database ##########"
# A slot retains WAL until it is consumed. One left behind for a database nobody
# caches any more pins WAL for the WHOLE cluster with nothing to show for it,
# which is the hazard that makes one-slot-per-database worth being careful about.
C_SLOT=$(slot_of proj_c)
chk "(setup) proj_c still has its slot" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$C_SLOT'")"
Q "SELECT supacache.rowcache_unregister('public.orders')" proj_c >/dev/null
for _ in $(seq 1 60); do
  [ "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$C_SLOT'")" = "0" ] && break
  sleep 1
done
chk "unregistering the last table drops proj_c's slot" "0" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$C_SLOT'")"
chk "and proj_c reads as idle, not as a stalled participant" "idle" \
    "$(Q "SELECT state FROM supacache.pg_stat_keyspace_rowcache_databases WHERE datname='proj_c'")"
chk "the other databases are unaffected" "2" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases WHERE state='participating'")"
# Put it back, so section 9 cycles over three databases rather than two.
Q "SELECT supacache.rowcache_register('public.orders')" proj_c >/dev/null
wait_coherent proj_c

echo
echo "########## 9. the pool is BOUNDED, and cycling still invalidates ##########"
# Worker count is something an operator sets, not a function of how many
# databases exist -- that is the difference between this and a worker per
# database, and it is the whole reason the design is a pool. With fewer workers
# than databases, invalidation CYCLES: latency becomes the cycle time rather
# than the decode interval. A knob, not a wall.
set_conf "pg_keyspace.rowcache_invalidation_workers" "1"
restart
for d in $DBS; do wait_coherent $d 120 >/dev/null; done

# Sampled repeatedly rather than once: the bound has to hold at every instant,
# not at a convenient one.
MAXW=0
for _ in $(seq 1 20); do
  W=$(live_workers); W=${W:-0}
  [ "$W" -gt "$MAXW" ] 2>/dev/null && MAXW=$W
  sleep 0.5
done
chk "never more than 1 invalidation worker, with 3 databases to serve (max $MAXW)" "t" \
    "$([ "$MAXW" -le 1 ] && echo t || echo f)"
chk "...and at least one really was running" "t" \
    "$([ "$MAXW" -ge 1 ] && echo t || echo f)"

# The trade, asserted rather than described: the window each database is judged
# against is now the CYCLE time, and it is reported instead of left to be
# derived. Without this the cache would fail closed on every database the moment
# it had to wait its turn.
STALE=$(Q "SELECT DISTINCT stale_after_ms FROM supacache.pg_stat_keyspace_rowcache_databases WHERE state='participating'")
chk "stale_after_ms widened to cover the cycle (${STALE}ms)" "t" \
    "$([ "${STALE:-0}" -gt 3000 ] && echo t || echo f)"

# And the assertion that makes section 9 mean anything: EVERY database is still
# invalidated, just slower. A bounded pool that dropped a database would be the
# original bug with a GUC in front of it.
for d in $DBS; do
  Q "INSERT INTO public.orders VALUES (99,'before-cycle') ON CONFLICT (id) DO UPDATE SET v='before-cycle'" $d >/dev/null
done
for d in $DBS; do Q "SELECT supacache.rowcache_put('public.orders', 99)" $d >/dev/null; done
for d in $DBS; do
  Q "UPDATE public.orders SET v='cycled-$d' WHERE id=99" $d >/dev/null
done
for d in $DBS; do
  chk "  $d is still invalidated under a pool of one" "ok" \
      "$(wait_value $d "SELECT v FROM public.orders WHERE id=99" "cycled-$d" 120 && echo ok || echo timeout)"
done
chk "every database is still coherent after cycling" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases
          WHERE state='participating' AND NOT coherent")"

echo
echo "########## 10. a database that cannot get a slot is INCOHERENT, not healthy ##########"
# Slots are a cluster-wide resource with a low default ceiling, and one is needed
# per participating database. A database that cannot get one is not being
# invalidated -- so it must fail closed, exactly as a stalled one does. Reporting
# it healthy would be the original bug reached by a different road.
# Three participating databases and room for two slots, so one of them cannot
# be served however long it waits. Existing slots are dropped first, or the
# cluster would simply keep the ones it already had.
for d in $DBS; do Q "SELECT pg_drop_replication_slot('$(slot_of $d)')" postgres >/dev/null 2>&1; done
set_conf "max_replication_slots" "2"
restart
sleep 20
SLOTS=$(Q "SELECT count(*) FROM pg_replication_slots WHERE plugin='supacache_keys'")
chk "three databases want a slot and the cluster allows two (got $SLOTS)" "t" \
    "$([ "${SLOTS:-0}" -le 2 ] && echo t || echo f)"
# The decisive assertion. Whichever database missed out is NOT being invalidated,
# so it must NOT be serving cached rows -- `coherent` false is what makes its
# reads fall back to the heap. Reporting it healthy would be the original bug
# reached by a different road: cached, never invalidated, and saying it is fine.
chk "a database without a slot never reads as coherent" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases d
          WHERE d.state='participating' AND d.coherent
            AND NOT EXISTS (SELECT 1 FROM pg_replication_slots s
                             WHERE s.slot_name = d.slot_name)")"
# And the failure is loud rather than silent.
chk "and the log says which database could not get one" "t" \
    "$([ "$(grep -c 'cannot create slot' $PGDATA/log)" -ge 1 ] && echo t || echo f)"
# A starved database must not starve the healthy ones: the pool has to keep
# cycling rather than retrying the one it cannot serve.
chk "the databases that did get slots are still served" "t" \
    "$([ "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases
               WHERE state='participating' AND coherent")" -ge 1 ] && echo t || echo f)"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
