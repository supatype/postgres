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
# Keys now carry the database oid and a tag byte. A foreign database finds no
# registration, so it plans an ordinary index scan.
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
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
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
  echo "wal_level = logical"
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
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
# The worker serves proj_a, so that is where registration is allowed.
echo "pg_keyspace.database = 'proj_a'" >> $PGDATA/postgresql.conf
restart
Q "CREATE EXTENSION pg_keyspace" proj_a >/dev/null
restart
for _ in $(seq 1 40); do [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)" = "t" ] && break; sleep 1; done

echo
echo "########## 1. the precondition: two databases, one relid ##########"
# If the relids differed, everything below would pass with the bug present.
A_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_a)
B_OID=$(Q "SELECT 'public.orders'::regclass::oid" proj_b)
chk "the cloned databases really share a relid ($A_OID / $B_OID)" "same" \
    "$([ "$A_OID" = "$B_OID" ] && echo same || echo different)"
chk "the invalidation worker is up, so the cache is served at all" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" proj_a)"

echo
echo "########## 2. proj_a caches its own row ##########"
chk "registering in the worker's database succeeds" "t" \
    "$(Q "SELECT supacache.rowcache_register('public.orders')" proj_a)"
chk "and the row caches" "t" "$(Q "SELECT supacache.rowcache_put('public.orders', 1)" proj_a)"
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
echo "########## 4. registering outside the worker's database is refused ##########"
# Previously this failed with `relation "supacache.rowcache_reg" does not exist`,
# which named the symptom rather than the reason (#118).
Q "CREATE EXTENSION pg_keyspace" proj_b >/dev/null 2>&1
REG_B=$(Q "SELECT supacache.rowcache_register('public.orders')" proj_b)
chk "registration from the wrong database returns false, not an error" "f" \
    "$(echo "$REG_B" | grep -o '^[ft]$' | head -1)"
chk "and says why, naming pg_keyspace.database" "1" \
    "$(echo "$REG_B" | grep -c "pg_keyspace.database" || true)"
chk "proj_b still reads its own row afterwards" "project-b-own-row" \
    "$(Q "SELECT v FROM public.orders WHERE id=1" proj_b)"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
