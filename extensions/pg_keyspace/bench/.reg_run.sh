#!/usr/bin/env bash
# Row-cache registrations survive a segment reinitialisation (#103).
#
# A registration is configuration, not cache content, and it used to live only
# in shared memory. Anything that reinitialises that segment -- a watchdog
# relaunch, a crash-restart, pg_terminate_backend on a worker, an ordinary
# restart -- took the pinned entry with it. rowcache_register had already
# returned true, so the application believed it was registered; the table simply
# stopped being cached, with no error, no log line, and rowcache_coherence()
# still reporting healthy, because coherence describes the invalidation worker
# rather than your registration.
#
# supacache.rowcache_reg is now the source of truth and the pinned entry is a
# cache of it, reloaded by the invalidation worker whenever it finds a segment
# with no load marker.
#
# Self-contained: builds the extension, creates and destroys its own cluster.
# Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-reg-data}
PORT=${PGKS_PG_PORT:-5441}
RESP=${PGKS_RESP_PORT:-6401}
PROFILE=${PGKS_BUILD_PROFILE:-release}
DECODE_MS=${PGKS_REG_DECODE_MS:-500}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }
# The worker reloads on its next pass, so give it a couple of decode intervals.
settle() { sleep $(( (DECODE_MS * 3 + 999) / 1000 + 1 )); }
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
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/reg_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/reg_install.log; exit 1; }
echo "installed (rowcache_reg references in source: $(grep -c rowcache_reg src/lib.rs))"
# The keys-only output plugin is a separate C build; cargo pgrx does not cover
# it. Without it the invalidation worker cannot open its slot, dies on a loop,
# and the whole run measures a cluster whose row cache is never served.
PLUGIN_OK=1
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/reg_plugin.log 2>&1 || PLUGIN_OK=0
chk "the supacache_keys output plugin builds and installs" "1" "$PLUGIN_OK"
[ "$PLUGIN_OK" = "1" ] || { echo "cannot continue without the plugin"; tail -20 /tmp/reg_plugin.log; exit 1; }

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.rowcache_decode = on"
  echo "pg_keyspace.rowcache_decode_ms = $DECODE_MS"
  echo "wal_level = logical"
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
} >> $PGDATA/postgresql.conf

chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
psql_ "CREATE EXTENSION pg_keyspace" >/dev/null
# Some builds gate output plugins behind an allowlist GUC. Asked of the running
# server rather than guessed: without it the invalidation worker cannot open its
# slot, dies on a five-second loop, and every row-cache assertion below fails
# for a reason that has nothing to do with registrations.
if [ "$(psql_ "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  grep -q "^output_plugin_libraries" $PGDATA/postgresql.conf ||     echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi
# From here on the allowlist (if this build has one) is in place, so truncate:
# the guard below is about whether the worker crash-loops from now on, and the
# first start before the setting was applied would otherwise count against it.
: > $PGDATA/log; chown postgres:postgres $PGDATA/log
restart
# The pool is up, which is not the same as the cache being SERVED. Databases are
# picked up lazily since #120: one with no registrations gets no slot, no worker
# and no turn, so it correctly reads as not coherent until something is
# registered. Asking for coherence here -- before section 1 registers anything --
# could only ever time out.
chk "an invalidation worker is running" "t" \
    "$([ "$(psql_ "SELECT count(*) FROM supacache.pg_stat_keyspace_activity \
                    WHERE role='invalidation' AND alive")" -ge 1 ] && echo t || echo f)"
chk "and this database is not served yet, having registered nothing" "f" \
    "$(psql_ "SELECT coherent FROM supacache.rowcache_coherence()")"
# Guard, because a dead worker makes everything below meaningless: the section
# that matters asserts a registration survives a restart, and a worker stuck in
# a crash loop would fail it for the wrong reason.
chk "the invalidation worker stays up rather than crash-looping" "0" \
    "$(grep -c 'may not be used as an output plugin' $PGDATA/log 2>/dev/null | tr -d '[:space:]')"
chk "the registration catalogue exists" "1" \
    "$(psql_ "SELECT count(*) FROM pg_tables WHERE schemaname='supacache' AND tablename='rowcache_reg'")"

psql_ "DROP TABLE IF EXISTS public.reg CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.reg(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.reg VALUES (1,'one'),(2,'two')" >/dev/null

echo
echo "########## 1. registering records durably, not just in shared memory ##########"
chk "the table is not registered to begin with" "{}" \
    "$(psql_ "SELECT supacache.rowcache_registration('public.reg')::text" | tr -d '[:space:]')"
chk "registering succeeds" "t" "$(psql_ "SELECT supacache.rowcache_register('public.reg')")"
# NOW the database participates, so a worker takes it and the cache is served.
# Everything after this point depends on that, so it is waited for here rather
# than raced by each assertion in turn.
chk "and the cache becomes coherent once it does" "1" "$(wait_coherent 90)"
chk "and it is readable from shared memory" "{id}" \
    "$(psql_ "SELECT supacache.rowcache_registration('public.reg')::text" | tr -d '[:space:]')"
# The point of the fix: there is now a row on disk, under the table's NAME.
chk "a catalogue row was written" "public.reg|{1}" \
    "$(psql_ "SELECT tbl||'|'||attnums::text FROM supacache.rowcache_reg" | tr -d '[:space:]')"

echo
echo "########## 2. it survives a restart, with no re-registration ##########"
# A restart reinitialises the segment, which is exactly what used to lose it.
# Nothing below re-registers: if the registration is present it came from the
# catalogue.
# Cache a row first. After the restart it must be GONE while the registration
# is PRESENT -- that pair is what distinguishes a registration reloaded from the
# catalogue from a segment that simply survived. Without this the section would
# pass vacuously on any build where the restart did not actually reinitialise
# shared memory, which is precisely the thing being claimed.
psql_ "SELECT supacache.rowcache_put('public.reg', 2)" >/dev/null
chk "a row is cached before the restart" "f" \
    "$(psql_ "SELECT supacache.rowcache_cached_has_external('public.reg', 2::bigint)")"
restart
chk "the worker is coherent again" "1" "$(wait_coherent 30)"
settle
chk "the segment really was reinitialised (the cached row is gone)" "" \
    "$(psql_ "SELECT supacache.rowcache_cached_has_external('public.reg', 2::bigint)")"
chk "the registration is still there, without re-registering" "{id}" \
    "$(psql_ "SELECT supacache.rowcache_registration('public.reg')::text" | tr -d '[:space:]')"
chk "and the table is genuinely cacheable, not just recorded" "t" \
    "$(psql_ "SELECT supacache.rowcache_put('public.reg', 1)")"
chk "the cached row is really there" "f" \
    "$(psql_ "SELECT supacache.rowcache_cached_has_external('public.reg', 1::bigint)")"

echo
echo "########## 3. unregistering is durable too ##########"
# The mirror image, and the bug the fix could easily have introduced: a
# registration that comes back from the dead on the next reload.
chk "unregistering succeeds" "t" "$(psql_ "SELECT supacache.rowcache_unregister('public.reg')")"
chk "the catalogue row is gone" "0" "$(psql_ "SELECT count(*) FROM supacache.rowcache_reg")"
restart; wait_coherent 30 >/dev/null; settle
chk "and it stays unregistered after a restart" "{}" \
    "$(psql_ "SELECT supacache.rowcache_registration('public.reg')::text" | tr -d '[:space:]')"

echo
echo "########## 4. the registration follows the table, not its oid ##########"
# Recording the oid would have been simpler and wrong: a migration that drops
# and recreates a table, or a dump/restore, gives it a new oid. Recorded by
# name, the registration is still correct afterwards.
chk "register it again" "t" "$(psql_ "SELECT supacache.rowcache_register('public.reg')")"
OLD_OID=$(psql_ "SELECT 'public.reg'::regclass::oid")
psql_ "DROP TABLE public.reg CASCADE" >/dev/null
psql_ "CREATE TABLE public.reg(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.reg VALUES (1,'one')" >/dev/null
NEW_OID=$(psql_ "SELECT 'public.reg'::regclass::oid")
chk "the recreated table really has a different oid" "different" \
    "$([ "$OLD_OID" != "$NEW_OID" ] && echo different || echo "same [$OLD_OID]")"
chk "reloading picks it up under the new oid" "1" \
    "$(psql_ "SELECT supacache.rowcache_reload_registrations()")"
chk "and the registration resolves again" "{id}" \
    "$(psql_ "SELECT supacache.rowcache_registration('public.reg')::text" | tr -d '[:space:]')"

echo
echo "########## 5. a dropped table does not break the reload ##########"
# to_regclass returns NULL rather than raising, so one stale row must not stop
# the other registrations loading.
psql_ "DROP TABLE IF EXISTS public.reg2 CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.reg2(id bigint primary key, v text)" >/dev/null
psql_ "SELECT supacache.rowcache_register('public.reg2')" >/dev/null
psql_ "DROP TABLE public.reg2 CASCADE" >/dev/null
chk "the dropped table's row is still in the catalogue" "1" \
    "$(psql_ "SELECT count(*) FROM supacache.rowcache_reg WHERE tbl='public.reg2'")"
chk "the reload skips it and still loads the live one" "1" \
    "$(psql_ "SELECT supacache.rowcache_reload_registrations()")"
chk "the live table is unaffected" "{id}" \
    "$(psql_ "SELECT supacache.rowcache_registration('public.reg')::text" | tr -d '[:space:]')"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
