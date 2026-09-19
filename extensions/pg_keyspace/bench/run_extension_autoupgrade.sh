#!/usr/bin/env bash
# The catalogue upgrades itself when the library moves (#136).
#
# run_extension_upgrade.sh proves ALTER EXTENSION lands the right catalogue.
# This proves nobody has to run it.
#
# That distinction is the whole point. Postgres executes an extension's SQL once,
# at CREATE EXTENSION, so a cluster that takes a newer pg_keyspace.so keeps the
# catalogue it was created with. The resulting failure is quiet -- a function
# whose columns changed keeps describing the old shape and returns the old
# columns without raising -- so the operator who most needs the command is the
# one with no reason to suspect there is one.
#
# This lives in the extension rather than in any image's bootstrap because
# pg_keyspace is used standalone: on plain Postgres, an AMI, bare metal, or
# someone else's container. A fix wired into one project's init scripts would
# reach none of them.
#
# Self-contained: builds the extension, creates and destroys its own cluster.
# Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-autoupgrade}
PORT=${PGKS_PG_PORT:-5471}
RESP=${PGKS_RESP_PORT:-6471}
PROFILE=${PGKS_BUILD_PROFILE:-release}
OLD_VER=0.1.0
NEW_VER=0.6.0
# The upgrade is a chain of scripts (#120 added the second step, supacache.publish
# the third). Postgres walks it on its own, so the worker still issues one ALTER
# EXTENSION -- but section 6 needs to know which file to remove to break the walk.
CHAIN="0.1.0--0.2.0 0.2.0--0.3.0 0.3.0--0.4.0 0.4.0--0.5.0 0.5.0--0.6.0"
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
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
# Stop BEFORE truncating, always. Truncating under a running server leaves a
# window in which a worker can append to the file after the truncate, which is
# exactly the race that made the registration harness intermittent (#134).
cycle() {
  stop_pg
  : > $PGDATA/log; chown postgres:postgres $PGDATA/log
  start_pg; wait_ready || { echo "NO START"; tail -20 $PGDATA/log; exit 1; }
  # Worker 0 does this at startup; give it a moment to get there.
  for _ in $(seq 1 30); do
    grep -q 'catalogue' $PGDATA/log && break
    [ "$(Q "SELECT extversion FROM pg_extension WHERE extname='pg_keyspace'")" = "$NEW_VER" ] && break
    sleep 1
  done
  sleep 1
}
extver() { Q "SELECT extversion FROM pg_extension WHERE extname='pg_keyspace'"; }
nfuncs() { Q "SELECT count(*) FROM pg_proc p JOIN pg_depend d ON d.classid='pg_proc'::regclass AND d.objid=p.oid JOIN pg_extension e ON e.oid=d.refobjid WHERE e.extname='pg_keyspace' AND d.deptype='e'"; }
nviews() { Q "SELECT count(*) FROM pg_views WHERE schemaname='supacache'"; }
logcount() { grep -c "$1" $PGDATA/log 2>/dev/null | tr -d '[:space:]'; }
# Anchored on the server's severity prefix. Postgres echoes a failing statement
# into the log as context, and these messages quote the very text being matched,
# so an unanchored grep counts one warning as two.
warncount() { grep -cE "WARNING: +pg_keyspace: $1" $PGDATA/log 2>/dev/null | tr -d '[:space:]'; }
logline()   { grep -cE "LOG: +pg_keyspace worker: $1" $PGDATA/log 2>/dev/null | tr -d '[:space:]'; }
set_guc() {
  grep -v "^pg_keyspace.auto_upgrade" $PGDATA/postgresql.conf > $PGDATA/conf.tmp
  mv $PGDATA/conf.tmp $PGDATA/postgresql.conf
  [ -n "$1" ] && echo "pg_keyspace.auto_upgrade = $1" >> $PGDATA/postgresql.conf
  chown postgres:postgres $PGDATA/postgresql.conf
}
reinstall_old() {
  Q "DROP EXTENSION IF EXISTS pg_keyspace CASCADE" >/dev/null
  Q "CREATE EXTENSION pg_keyspace VERSION '$OLD_VER'" >/dev/null
}

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
rm -f "$($PGBIN/pg_config --sharedir)/extension/pg_keyspace--$OLD_VER.sql"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/autoupgrade_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/autoupgrade_install.log; exit 1; }
SHAREDIR=$($PGBIN/pg_config --sharedir)/extension
# Staged only so this test can build a genuine 0.1.0 install to upgrade FROM.
cp "$EXT_DIR/sql/testdata/pg_keyspace--$OLD_VER.sql" "$SHAREDIR/pg_keyspace--$OLD_VER.sql"

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.database = 'postgres'"
  echo "pg_keyspace.workers = 2"
  echo "pg_keyspace.persist_workers = 2"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; tail -20 $PGDATA/log; exit 1; }

echo
echo "########## 1. a cluster as an older image left it ##########"
# Preconditions. If this is not a real 0.1.0 install then section 2 proves
# nothing -- an upgrade from a catalogue that already had the views would pass
# every assertion below while repairing nothing.
Q "CREATE EXTENSION pg_keyspace VERSION '$OLD_VER'" >/dev/null
chk "the extension is installed at $OLD_VER" "$OLD_VER" "$(extver)"
chk "it has the 18 functions the shipped images carried" "18" "$(nfuncs)"
chk "and none of the views" "0" "$(nviews)"
# The worker was already running when CREATE EXTENSION happened, and it checks
# once at startup. It must NOT reach in and change a running session's catalogue.
chk "a running worker does not upgrade mid-session" "$OLD_VER" "$(extver)"

echo
echo "########## 2. restarting is the whole procedure ##########"
# No ALTER EXTENSION anywhere in this test. If the version moves, the worker
# moved it.
cycle
chk "after a restart the extension reports $NEW_VER" "$NEW_VER" "$(extver)"
chk "it now has all 41 functions" "41" "$(nfuncs)"
chk "and every view" "11" "$(nviews)"
chk "the worker said so in the log, once" "1" \
    "$(logcount "upgraded the extension catalogue $OLD_VER -> $NEW_VER")"
chk "and warned about nothing" "0" "$(warncount 'automatic catalogue upgrade failed')"
# Repaired, not merely renumbered: the seven-column ring_stats() is the object
# whose absence is silent, so read it rather than the version string.
chk "ring_stats() serves all seven columns" "7" \
    "$(Q "SELECT count(*) FROM (SELECT row_to_json(r) AS j FROM supacache.ring_stats() r) x, json_each(x.j)")"
BAD=""
for v in pg_stat_keyspace pg_stat_keyspace_workers pg_stat_keyspace_activity \
         pg_stat_keyspace_persist pg_stat_keyspace_persist_total \
         pg_stat_keyspace_tenants pg_stat_keyspace_rowcache \
         pg_stat_keyspace_invalidation pg_stat_keyspace_pubsub pg_stat_keyspace_topology; do
  out=$(Q "SELECT count(*) >= 0 FROM supacache.$v")
  [ "$out" = "t" ] || BAD="$BAD $v[$(echo "$out" | head -1)]"
done
chk "every view is readable" "" "$BAD"

echo
echo "########## 3. and it is a no-op from then on ##########"
# A check that fired on every start would be a restart-time ALTER EXTENSION on a
# healthy cluster forever: lock churn, log noise, and a needless failure mode.
cycle
chk "the second restart still reports $NEW_VER" "$NEW_VER" "$(extver)"
chk "and does not upgrade again" "0" "$(logcount 'upgraded the extension catalogue')"
chk "nor complain" "0" "$(warncount 'automatic catalogue upgrade failed')"
chk "nor nag about a skew that is gone" "0" "$(logcount 'ALTER EXTENSION pg_keyspace UPDATE')"

echo
echo "########## 4. off means off ##########"
# The negative control. With the GUC off the version must stay put -- otherwise
# section 2 proved only that restarting upgrades, not that this setting is what
# does it -- and the operator must be told, since a silent skew is the very
# failure being prevented.
reinstall_old
chk "back to $OLD_VER for the control" "$OLD_VER" "$(extver)"
set_guc off
cycle
chk "with auto_upgrade off it stays at $OLD_VER" "$OLD_VER" "$(extver)"
chk "and the views are still missing" "0" "$(nviews)"
chk "but the skew is reported, not swallowed" "1" \
    "$(logcount 'ALTER EXTENSION pg_keyspace UPDATE')"
chk "and nothing claims to have upgraded" "0" "$(logcount 'upgraded the extension catalogue')"

echo
echo "########## 5. turning it back on repairs the same cluster ##########"
# Proves section 4 left a genuinely repairable cluster rather than a broken one,
# and that the setting is the only thing that differed.
set_guc on
cycle
chk "it upgrades on the next start" "$NEW_VER" "$(extver)"
chk "with every view back" "11" "$(nviews)"

echo
echo "########## 6. an upgrade that cannot run must not take the worker with it ##########"
# The claim the code makes for itself, tested rather than asserted. ALTER
# EXTENSION raises for reasons that are not emergencies -- here, an install whose
# upgrade script is simply absent -- and a worker that died on one would take the
# keyspace out of service on a five-second relaunch loop, which is exactly the
# shape of #130. The statement runs inside a DO block with an exception handler,
# so the failure has to arrive as a warning and startup has to continue.
reinstall_old
chk "back to $OLD_VER with no upgrade script present" "$OLD_VER" "$(extver)"
# The LAST step of the chain, not a single old--new file: the path is
# 0.1.0 -> 0.2.0 -> 0.3.0 -> 0.4.0, and removing the last hop is what leaves an install
# that can start walking and cannot finish. ALTER EXTENSION then raises "no
# update path", which is the non-emergency failure this section is about.
LAST_STEP=$(printf '%s\n' $CHAIN | tail -1)
mv "$SHAREDIR/pg_keyspace--$LAST_STEP.sql" /tmp/pgks_upgrade_script.bak
cycle
chk "the upgrade fails, and says so once" "1" "$(warncount 'automatic catalogue upgrade failed')"
chk "the extension is left alone at $OLD_VER" "$OLD_VER" "$(extver)"
# Worker 0 reports the unrepaired skew exactly once per start, so a second copy
# of this line IS the relaunch loop -- a direct detector rather than a proxy.
chk "worker 0 started once, so there is no relaunch loop" "1" \
    "$(logline 'the extension catalogue is still')"
chk "the worker is alive and serving its segment" "t" \
    "$(Q "SELECT count(*) > 0 FROM supacache.stats()")"
chk "and the server is serving" "1" "$(Q "SELECT 1")"
mv /tmp/pgks_upgrade_script.bak "$SHAREDIR/pg_keyspace--$LAST_STEP.sql"

stop_pg
rm -f "$SHAREDIR/pg_keyspace--$OLD_VER.sql"
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
