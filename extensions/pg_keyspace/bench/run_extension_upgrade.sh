#!/usr/bin/env bash
# An existing install reaches the current catalogue: 0.1.0 -> 0.2.0 (#136).
#
# Postgres runs an extension's SQL once, at CREATE EXTENSION. A cluster that
# takes a newer pg_keyspace.so keeps whatever catalogue it had when the
# extension was first created, so fixes that live in the binary arrive on their
# own and SQL objects do not. Until 0.2.0 there was no upgrade script at all,
# which left an install from the v17.2.4 or v17.2.5 image seventeen functions
# and ten views short -- and holding a ring_stats() entry promising three
# columns where the new wrapper returns seven.
#
# The assertion that matters is section 3: an upgraded 0.1.0 install and a
# fresh CREATE EXTENSION must end up with the SAME catalogue -- same signatures,
# same argument names, same ACLs, same comments, same view definitions. Anyone
# who later adds a view or changes a signature in lib.rs and forgets the upgrade
# script fails here, which is the only durable defence against the two drifting.
#
# Self-contained: builds the extension, creates and destroys its own cluster.
# Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-upgrade}
PORT=${PGKS_PG_PORT:-5469}
RESP=${PGKS_RESP_PORT:-6469}
PROFILE=${PGKS_BUILD_PROFILE:-release}
OLD_VER=0.1.0
NEW_VER=0.6.0
# The upgrade is a CHAIN now: 0.1.0 -> ... -> 0.6.0, a script per step.
# Postgres walks it on its own, so ALTER EXTENSION ... UPDATE TO the newest
# version is still one statement -- but every step has to be packaged, which is
# what section 0 checks. Add a step here when you add a script.
CHAIN="0.1.0--0.2.0 0.2.0--0.3.0 0.3.0--0.4.0 0.4.0--0.5.0 0.5.0--0.6.0"
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q()  { $PGBIN/psql -h /tmp -p $PORT -U postgres -d "${2:-postgres}" -tAc "$1" 2>&1; }
QM() { $PGBIN/psql -h /tmp -p $PORT -U metrics  -d "${2:-postgres}" -tAc "$1" 2>&1; }
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

# Everything the extension owns, in a form two databases can be compared on.
# Deliberately more than "the objects exist": argument names, volatility,
# strictness, ACLs and comments are all things an upgrade script can silently
# get wrong while still creating something of the right name.
CATALOGUE_SQL=$(cat <<'EOF'
WITH ext AS (SELECT oid FROM pg_extension WHERE extname = 'pg_keyspace'),
     mem AS (SELECT d.classid, d.objid
               FROM pg_depend d, ext
              WHERE d.refclassid = 'pg_extension'::regclass
                AND d.refobjid = ext.oid AND d.deptype = 'e')
SELECT line FROM (
  SELECT format('function|%s.%s(%s)|args=%s|returns=%s|flags=%s%s%s%s%s|symbol=%s|acl=%s|comment=%s',
                p.pronamespace::regnamespace, p.proname,
                pg_get_function_identity_arguments(p.oid),
                pg_get_function_arguments(p.oid),
                pg_get_function_result(p.oid),
                p.prokind, p.provolatile, p.proparallel,
                p.proisstrict::text, p.prosecdef::text,
                coalesce(p.prosrc, '-'),
                coalesce(array_to_string(p.proacl::text[], ' '), '-'),
                coalesce(obj_description(p.oid, 'pg_proc'), '-')) AS line
    FROM pg_proc p JOIN mem ON mem.classid = 'pg_proc'::regclass AND mem.objid = p.oid
  UNION ALL
  SELECT format('relation|%s.%s|kind=%s|acl=%s|comment=%s|def=%s',
                c.relnamespace::regnamespace, c.relname, c.relkind,
                coalesce(array_to_string(c.relacl::text[], ' '), '-'),
                coalesce(obj_description(c.oid, 'pg_class'), '-'),
                coalesce(regexp_replace(pg_get_viewdef(c.oid, true), '\s+', ' ', 'g'), '-'))
    FROM pg_class c JOIN mem ON mem.classid = 'pg_class'::regclass AND mem.objid = c.oid
  UNION ALL
  SELECT format('schema|%s|acl=%s|comment=%s',
                n.nspname,
                coalesce(array_to_string(n.nspacl::text[], ' '), '-'),
                coalesce(obj_description(n.oid, 'pg_namespace'), '-'))
    FROM pg_namespace n JOIN mem ON mem.classid = 'pg_namespace'::regclass AND mem.objid = n.oid
) s ORDER BY line
EOF
)
VIEWS="pg_stat_keyspace pg_stat_keyspace_workers pg_stat_keyspace_activity
       pg_stat_keyspace_persist pg_stat_keyspace_persist_total
       pg_stat_keyspace_tenants pg_stat_keyspace_rowcache
       pg_stat_keyspace_invalidation pg_stat_keyspace_pubsub
       pg_stat_keyspace_topology"

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
# A pg_keyspace--0.1.0.sql left in the extension directory by an install from
# before default_version moved would defeat the "not shipped" check below, and
# this test stages that filename itself. Clear it first so the check means
# something on a machine that has built this extension before.
rm -f "$($PGBIN/pg_config --sharedir)/extension/pg_keyspace--$OLD_VER.sql"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/upgrade_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/upgrade_install.log; exit 1; }
SHAREDIR=$($PGBIN/pg_config --sharedir)/extension

echo
echo "########## 0. the upgrade script is packaged, not just written ##########"
# A correct upgrade script that never leaves the source tree fixes nothing, and
# the mistake is invisible until someone runs ALTER EXTENSION in production.
# cargo pgrx install copies sql/<ext>--<old>--<new>.sql; assert it actually did.
chk "the control file declares $NEW_VER" "$NEW_VER" \
    "$(grep -oP "(?<=^default_version = ')[^']+" $SHAREDIR/pg_keyspace.control)"
chk "the $NEW_VER schema was installed" "1" \
    "$([ -f "$SHAREDIR/pg_keyspace--$NEW_VER.sql" ] && echo 1 || echo 0)"
# Every step of the chain, not just the last one: a missing intermediate script
# breaks the walk for exactly the installs that need it most -- the oldest ones.
for step in $CHAIN; do
  chk "  the $step upgrade script was installed" "1" \
      "$([ -f "$SHAREDIR/pg_keyspace--$step.sql" ] && echo 1 || echo 0)"
  chk "  and it is the one from the source tree, byte for byte" "same" \
      "$(cmp -s "$EXT_DIR/sql/pg_keyspace--$step.sql" \
                "$SHAREDIR/pg_keyspace--$step.sql" && echo same || echo differs)"
done
# Test data, not a shipped artefact: pgrx copies only the --old--new form and
# does not recurse, so the archived release schema must NOT have been installed.
chk "the archived $OLD_VER schema is not shipped by the install" "0" \
    "$([ -f "$SHAREDIR/pg_keyspace--$OLD_VER.sql" ] && echo 1 || echo 0)"

# Staged by hand, only so this test can build a genuine 0.1.0 install to upgrade
# from. Once default_version moves there is no other way to reconstruct one.
cp "$EXT_DIR/sql/testdata/pg_keyspace--$OLD_VER.sql" "$SHAREDIR/pg_keyspace--$OLD_VER.sql"

echo
echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.workers = 2"
  echo "pg_keyspace.persist_workers = 2"
  echo "pg_keyspace.durability = 'durable'"
  echo "wal_level = logical"
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; tail -20 $PGDATA/log; exit 1; }

Q "CREATE DATABASE upgraded" >/dev/null
Q "CREATE DATABASE fresh" >/dev/null
Q "CREATE ROLE metrics LOGIN IN ROLE pg_monitor" >/dev/null

echo
echo "########## 1. an install as the shipped images left it ##########"
# Preconditions. If this section does not describe a real 0.1.0 install then
# everything below measures the wrong thing -- an upgrade from a catalogue that
# already had the views would pass section 3 while proving nothing.
# Every version in the chain is offerable, not just its ends: each upgrade
# script makes its own target installable directly. Derived from CHAIN so that
# adding a step does not need this line edited -- and so a step that failed to
# package shows up here as a missing version rather than as a puzzle in
# section 3.
CHAIN_VERS=$(printf '%s\n' $CHAIN | tr -- '--' '\n' | grep -v '^$' | sort -u | tr '\n' ' ' | sed 's/ $//')
chk "every version in the chain is offered to CREATE EXTENSION" "$CHAIN_VERS" \
    "$(Q "SELECT string_agg(version, ' ' ORDER BY version) FROM pg_available_extension_versions WHERE name='pg_keyspace'")"
Q "CREATE EXTENSION pg_keyspace VERSION '$OLD_VER'" upgraded >/dev/null
chk "the extension installs at $OLD_VER" "$OLD_VER" \
    "$(Q "SELECT extversion FROM pg_extension WHERE extname='pg_keyspace'" upgraded)"
chk "it has the 18 functions the images shipped" "18" \
    "$(Q "SELECT count(*) FROM pg_proc p JOIN pg_depend d ON d.classid='pg_proc'::regclass AND d.objid=p.oid JOIN pg_extension e ON e.oid=d.refobjid WHERE e.extname='pg_keyspace' AND d.deptype='e'" upgraded)"
chk "and none of the views" "0" \
    "$(Q "SELECT count(*) FROM pg_views WHERE schemaname='supacache'" upgraded)"

echo
echo "########## 2. what that install is actually missing ##########"
# The negative control for the whole exercise. These are the symptoms an
# operator sees after taking the new .so without running ALTER EXTENSION; each
# one must be repaired by section 3, and each must be broken here or the upgrade
# script is being credited with fixing something that was never wrong.
# Captured and then matched, never piped into grep: under `set -o pipefail` the
# pipeline reports psql's exit status, so an error that matched would still read
# as a miss and quietly turn these negative controls green.
V_ERR=$(Q "SELECT 1 FROM supacache.pg_stat_keyspace" upgraded)
W_ERR=$(Q "SELECT 1 FROM supacache.worker_stats()" upgraded)
chk "the dashboard rollup view does not exist" "1" \
    "$(case "$V_ERR" in *"does not exist"*) echo 1;; *) echo 0;; esac)"
chk "worker_stats(), which the views read, does not exist" "1" \
    "$(case "$W_ERR" in *"does not exist"*) echo 1;; *) echo 0;; esac)"
chk "the catalogue still describes ring_stats() as three columns" "3" \
    "$(Q "SELECT cardinality(proallargtypes) FROM pg_proc WHERE proname='ring_stats' AND pronamespace='supacache'::regnamespace" upgraded)"
# And the failure mode is the quiet one. Calling it does not raise: the stale
# entry silently serves the first three of the seven columns the wrapper
# returns, so an operator sees ring_stats() working while committed, lag,
# failed_batches and unresolved simply are not there to be read.
RS_BEFORE=$(Q "SELECT * FROM supacache.ring_stats()" upgraded)
chk "calling ring_stats() does not raise -- it looks fine" "0" \
    "$(case "$RS_BEFORE" in *ERROR*|*FATAL*|*"server closed"*) echo 1;; *) echo 0;; esac)"
chk "but it silently serves three columns, not seven" "3" \
    "$(Q "SELECT count(*) FROM (SELECT row_to_json(r) AS j FROM supacache.ring_stats() r) x, json_each(x.j)" upgraded)"
echo "        it reports: $(echo "$RS_BEFORE" | head -1)"

echo
echo "########## 3. the upgrade lands exactly where a fresh install does ##########"
UPD=$(Q "ALTER EXTENSION pg_keyspace UPDATE TO '$NEW_VER'" upgraded)
chk "ALTER EXTENSION ... UPDATE succeeds" "" "$(echo "$UPD" | grep -i error)"
chk "the install now reports $NEW_VER" "$NEW_VER" \
    "$(Q "SELECT extversion FROM pg_extension WHERE extname='pg_keyspace'" upgraded)"
Q "CREATE EXTENSION pg_keyspace" fresh >/dev/null
chk "a fresh CREATE EXTENSION gets $NEW_VER" "$NEW_VER" \
    "$(Q "SELECT extversion FROM pg_extension WHERE extname='pg_keyspace'" fresh)"

Q "$CATALOGUE_SQL" upgraded > /tmp/pgks_cat_upgraded.txt
Q "$CATALOGUE_SQL" fresh    > /tmp/pgks_cat_fresh.txt
# Guard: an empty or error-filled dump would make the diff below pass vacuously.
chk "the upgraded catalogue dumped $((41 + 11 + 1)) objects" "53" \
    "$(grep -c '^\(function\|relation\|schema\)|' /tmp/pgks_cat_upgraded.txt)"
chk "the fresh catalogue dumped the same number" \
    "$(grep -c '^\(function\|relation\|schema\)|' /tmp/pgks_cat_upgraded.txt)" \
    "$(grep -c '^\(function\|relation\|schema\)|' /tmp/pgks_cat_fresh.txt)"
DIFF=$(diff /tmp/pgks_cat_upgraded.txt /tmp/pgks_cat_fresh.txt)
chk "upgraded and fresh catalogues are identical" "" "$DIFF"
[ -n "$DIFF" ] && { echo "--- upgraded vs fresh ---"; echo "$DIFF" | head -40; }

echo
echo "########## 4. and the repaired objects genuinely work ##########"
# Identical catalogues would still be identically broken if the views could not
# be read, so exercise them against the running cluster rather than pg_catalog.
chk "ring_stats() now describes seven columns" "7" \
    "$(Q "SELECT cardinality(proallargtypes) FROM pg_proc WHERE proname='ring_stats' AND pronamespace='supacache'::regnamespace" upgraded)"
chk "and a call really returns all seven" "7" "$(Q "SELECT count(*) FROM (SELECT row_to_json(r) AS j FROM supacache.ring_stats() r) x, json_each(x.j)" upgraded)"
BAD=""
for v in $VIEWS; do
  out=$(Q "SELECT count(*) >= 0 FROM supacache.$v" upgraded)
  [ "$out" = "t" ] || BAD="$BAD $v[$out]"
done
chk "every view is readable after the upgrade" "" "$BAD"
# #111's point: a pg_monitor member needs no grant beyond the role itself. The
# upgrade script carries those grants, so this fails if it dropped them.
BADM=""
for v in $VIEWS; do
  out=$(QM "SELECT count(*) >= 0 FROM supacache.$v" upgraded)
  [ "$out" = "t" ] || BADM="$BADM $v[$(echo "$out" | head -1)]"
done
chk "a pg_monitor member reads all ten, with no further grant" "" "$BADM"

# Calling them, because a function that resolves is not yet a function that
# works. A missing symbol is not the risk here -- Postgres's C-language
# validator looks it up at CREATE FUNCTION time, so an upgrade script naming a
# wrapper that is not in the library fails the whole ALTER EXTENSION with
# `could not find function "..." in file "..."` and never reaches this point
# (checked, by adding exactly such a function to the upgrade script).
#
# What survives CREATE is a row whose DECLARED shape disagrees with what the
# wrapper returns -- the ring_stats() bug this whole PR exists for, where the
# catalogue promised three columns and the wrapper returned seven. That is
# caught by calling, and only by calling.
#
# Semantic errors are fine here and expected: the row cache serves one database
# and these run in another. So this looks only for the loader's complaint, which
# must never appear.
UNRESOLVED=""
probe() {
  out=$(Q "$1" upgraded)
  case "$out" in *"could not find function"*|*"undefined symbol"*)
    UNRESOLVED="$UNRESOLVED ${2:-$1}";; esac
}
# Zero-argument functions come from the catalogue rather than a list, so a
# function added later is probed without anyone remembering to add it here.
ZERO_ARG=$(Q "SELECT p.proname FROM pg_proc p JOIN pg_depend d ON d.classid='pg_proc'::regclass AND d.objid=p.oid JOIN pg_extension e ON e.oid=d.refobjid WHERE e.extname='pg_keyspace' AND d.deptype='e' AND p.pronargs=0 ORDER BY 1" upgraded)
# An exact count, in the same spirit as the 18 above: a sweep that quietly
# stopped finding functions would probe nothing and pass. Bump it deliberately
# when a zero-argument function is added.
chk "the sweep found all 18 zero-argument functions" "18" \
    "$(echo "$ZERO_ARG" | grep -c .)"
for f in $ZERO_ARG; do probe "SELECT * FROM supacache.$f()" "$f()"; done
# The rest take arguments, so they are named with values that are safe to pass:
# reads, a registration of a table made for it, and a reload that is idempotent.
Q "CREATE TABLE public.smoke(id bigint primary key, v text)" upgraded >/dev/null
probe "SELECT supacache.key_worker('smoke')"                                "key_worker(text)"
probe "SELECT supacache.rowcache_registration('public.smoke')"              "rowcache_registration(text)"
probe "SELECT supacache.rowcache_register('public.smoke')"                  "rowcache_register(text)"
probe "SELECT supacache.rowcache_put_pk('public.smoke', ARRAY['1'])"        "rowcache_put_pk(text,text[])"
probe "SELECT supacache.rowcache_cached_pk_has_external('public.smoke', ARRAY['1'])" "rowcache_cached_pk_has_external(text,text[])"
chk "every function is callable against the $NEW_VER library" "" "$UNRESOLVED"

echo
echo "########## 5. the upgraded install is still a clean extension ##########"
# A script that created objects outside the extension would look fine above and
# leave debris behind on DROP -- and would go missing from pg_dump.
chk "every view belongs to the extension" "11" \
    "$(Q "SELECT count(*) FROM pg_class c JOIN pg_depend d ON d.classid='pg_class'::regclass AND d.objid=c.oid JOIN pg_extension e ON e.oid=d.refobjid WHERE e.extname='pg_keyspace' AND d.deptype='e' AND c.relkind='v'" upgraded)"
# `supacache.rowcache_reg` is created at RUNTIME, by rowcache_register, in
# whichever database it is called in -- section 4's probe calls it here. It is
# deliberately NOT an extension member, for the same reason supacache.kv and
# supacache.acl are not: it holds configuration an operator entered, so DROP
# EXTENSION must not silently take it. That does mean a database with
# registrations needs it dropped (or CASCADE) before the extension will go, and
# since #120 that is any database, not just pg_keyspace.database.
#
# Dropped here so the assertions below stay about what this section is for: that
# the UPGRADE SCRIPT created only extension members, and left no debris of its
# own that would go missing from pg_dump.
Q "DROP TABLE IF EXISTS supacache.rowcache_reg" upgraded >/dev/null
DROPX=$(Q "DROP EXTENSION pg_keyspace" upgraded)
chk "the extension drops cleanly once its runtime data is gone" "" \
    "$(echo "$DROPX" | grep -i error)"
chk "dropping it leaves no views behind" "0" \
    "$(Q "SELECT count(*) FROM pg_views WHERE schemaname='supacache'" upgraded)"
chk "and no functions behind" "0" \
    "$(Q "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='supacache'" upgraded)"
# The schema is an extension member too, so it goes with it -- which is why the
# count above joins pg_namespace rather than casting to regnamespace, a cast
# that raises on a schema that is gone.
chk "and takes its schema with it" "0" \
    "$(Q "SELECT count(*) FROM pg_namespace WHERE nspname='supacache'" upgraded)"

stop_pg
rm -f "$SHAREDIR/pg_keyspace--$OLD_VER.sql"
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
