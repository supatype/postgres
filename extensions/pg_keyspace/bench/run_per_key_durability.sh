#!/usr/bin/env bash
# Per-key durability (#164): one instance, a durable prefix and an ephemeral
# cache beside it.
#
# The contract under test is not "the GUC is read". It is that the tier a key
# resolves to decides four separate things, each of which has its own way of
# being wrong:
#
#   what is provisioned  an ephemeral DEFAULT with a durable PREFIX still has
#                        to start rings, persistence workers and a synchronous
#                        commit -- provisioning from pg_keyspace.durability
#                        would leave the durable prefix with nowhere to go
#   what is staged       only the covered keys reach supacache.kv
#   what survives        only those come back after an immediate restart
#   what is refused      a spec that does not parse, and a `replicated` tier
#                        mixed with others, stop the worker rather than being
#                        half-applied
#
# It also covers the case a narrowed map creates: rows under a prefix that is
# no longer persisted are skipped by recovery (not served, cannot resurrect)
# and removed only by an explicit prune, which refuses to empty the table.
#
# Self-contained, like run_extension_autoupgrade.sh and for the same reason:
# pg_keyspace.durability_overrides is postmaster context, so the test has to
# own the cluster it restarts. Override PGBIN, PGDATA, PGKS_PG_PORT,
# PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-perkey}
PORT=${PGKS_PG_PORT:-5472}
RESP=${PGKS_RESP_PORT:-6472}
PROFILE=${PGKS_BUILD_PROFILE:-release}

pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -X -q -A -t -c "$1" 2>&1; }
R() { redis-cli -p $RESP "$@" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
# Same shutdown care as run_extension_autoupgrade.sh: `pg_ctl -w stop` can give
# up with the postmaster still shutting down, and starting on top of that turns
# into a readiness timeout blamed on whatever assertion came next.
stop_pg() {
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 120 stop -m ${1:-fast}" >/dev/null 2>&1
  for _ in $(seq 1 120); do
    su postgres -c "$PGBIN/pg_ctl -D $PGDATA status" >/dev/null 2>&1 || return 0
    sleep 1
  done
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 60 stop -m immediate" >/dev/null 2>&1
  return 0
}
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
# The RESP port opens after recovery, so waiting on Postgres alone races a
# worker still loading -- which reads as data loss in section 3.
wait_resp() { for _ in $(seq 1 60); do [ "$(R PING)" = "PONG" ] && return 0; sleep 1; done; return 1; }
# Staged writes reach supacache.kv a batch later; poll for the count rather
# than sleeping a guessed interval.
wait_rows() {
  for _ in $(seq 1 60); do
    [ "$(Q "SELECT count(*) FROM supacache.kv")" = "$1" ] && return 0
    sleep 1
  done
  return 1
}
set_overrides() {
  grep -v "^pg_keyspace.durability_overrides" $PGDATA/postgresql.conf > $PGDATA/conf.tmp
  mv $PGDATA/conf.tmp $PGDATA/postgresql.conf
  echo "pg_keyspace.durability_overrides = '$1'" >> $PGDATA/postgresql.conf
  chown postgres:postgres $PGDATA/postgresql.conf
}
kv_keys() { Q "SELECT coalesce(string_agg(convert_from(key,'UTF8'), ',' ORDER BY key), '') FROM supacache.kv"; }

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/perkey_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/perkey_install.log; exit 1; }

echo "=== initdb ==="
stop_pg immediate
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.database = 'postgres'"
  echo "pg_keyspace.workers = 1"
  # The self-host shape: the keyspace is a cache, and one prefix is not.
  echo "pg_keyspace.durability = 'ephemeral'"
  echo "pg_keyspace.durability_overrides = 'acme:=durable'"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; tail -20 $PGDATA/log; exit 1; }
Q "CREATE EXTENSION pg_keyspace" >/dev/null
# The operational tables are created by worker 0 at startup and only once the
# extension exists, so this restart is required rather than incidental.
stop_pg; start_pg; wait_ready || { echo "NO RESTART"; tail -20 $PGDATA/log; exit 1; }
wait_resp || { echo "RESP NEVER CAME UP"; tail -20 $PGDATA/log; exit 1; }

echo
echo "########## 1. an ephemeral default with one durable prefix ##########"
# The provisioning question. Reading pg_keyspace.durability here would start no
# rings at all and the durable prefix would have nowhere to go.
chk "persistence is on despite durability = ephemeral" "1" "$(grep -c 'persistence ON' $PGDATA/log)"
chk "and the worker printed the policy it resolved" "1" \
    "$(grep -c 'per-key durability is ON' $PGDATA/log)"
chk "key_durability resolves the covered prefix" "durable" \
    "$(Q "SELECT supacache.key_durability('acme:cert')")"
chk "and everything else to the default" "ephemeral" \
    "$(Q "SELECT supacache.key_durability('cache:page')")"

echo
echo "########## 2. only the covered prefix is staged ##########"
R SET acme:cert certval >/dev/null
R SET acme:acct acctval >/dev/null
R SET cache:page pageval >/dev/null
wait_rows 2 || echo "  (timed out waiting for the persist worker)"
chk "the durable prefix reached supacache.kv" "acme:acct,acme:cert" "$(kv_keys)"
chk "and both keys are readable over RESP meanwhile" "certval|pageval" \
    "$(R GET acme:cert)|$(R GET cache:page)"

echo
echo "########## 2b. a value too large to inline follows the same rule ##########"
# Past INLINE_MAX (8 KiB) the ring record carries the entry's version and the
# persistence worker reads the bytes from shared memory instead. That is a
# different branch of the staging path, and a durable prefix has to survive a
# restart through it -- the unit tests cover which records reach the ring,
# this covers the value actually coming back.
BIG=$(head -c 20000 /dev/zero | tr '\0' 'x')
R SET acme:big "$BIG" >/dev/null
R SET cache:big "$BIG" >/dev/null
wait_rows 3 || echo "  (timed out waiting for the large durable row)"
chk "the large durable value is in supacache.kv at full length" "20000" \
    "$(Q "SELECT length(val) FROM supacache.kv WHERE key = 'acme:big'::bytea")"
chk "and the large ephemeral one is not there at all" "0" \
    "$(Q "SELECT count(*) FROM supacache.kv WHERE key = 'cache:big'::bytea")"

echo
echo "########## 3. a restart keeps one and drops the other ##########"
stop_pg immediate; start_pg; wait_ready || { echo "NO RESTART"; exit 1; }
wait_resp || { echo "RESP NEVER CAME UP"; exit 1; }
chk "the durable key survived kill -9" "certval" "$(R GET acme:cert)"
chk "the ephemeral one did not" "" "$(R GET cache:page)"
# The by-reference path end to end: staged as a version, read back out of the
# segment by the persistence worker, recovered into a fresh segment.
chk "the large durable value came back whole" "20000" "$(R STRLEN acme:big)"
chk "and the large ephemeral one did not come back" "0" "$(R EXISTS cache:big)"
# Put the row count back to what the sections below were written against.
# They assert exact contents of supacache.kv, so a key that outlives its own
# section silently rewrites their expectations -- which is how a harness ends
# up with constants nobody can derive.
R DEL acme:big >/dev/null
wait_rows 2 || echo "  (timed out clearing the large durable row)"

echo
echo "########## 4. a delete of a covered key does not resurrect ##########"
# The tombstone follows the same predicate as the write, so this is the case
# that would break if it did not.
R DEL acme:acct >/dev/null
wait_rows 1 || echo "  (timed out waiting for the tombstone)"
stop_pg immediate; start_pg; wait_ready || exit 1; wait_resp || exit 1
chk "the deleted key stays deleted across a restart" "" "$(R GET acme:acct)"
chk "and its row is gone from supacache.kv" "acme:cert" "$(kv_keys)"

echo
echo "########## 5. narrowing the map strands rows, it does not serve them ##########"
R SET acme:two v2 >/dev/null
wait_rows 2 || echo "  (timed out waiting for the second row)"
stop_pg; set_overrides "other:=durable"; start_pg; wait_ready || exit 1; wait_resp || exit 1
chk "a key under the dropped prefix is not served" "" "$(R GET acme:cert)"
chk "recovery reported what it skipped" "1" "$(grep -c 'recovery skipped 2 row' $PGDATA/log)"
chk "undurable_rows() names the prefix and the count" "acme:|2" \
    "$(Q "SELECT key_prefix||'|'||rows FROM supacache.undurable_rows()")"

echo
echo "########## 6. prune refuses to empty the table, then does its job ##########"
# Every row is stranded at this point, which is indistinguishable from a
# mistyped override -- so the guard should hold.
chk "prune_undurable() refuses to delete everything" "0" "$(Q "SELECT supacache.prune_undurable()" | tail -1)"
chk "and the rows are still there" "2" "$(Q "SELECT count(*) FROM supacache.kv")"
R SET other:keep keepval >/dev/null
wait_rows 3 || echo "  (timed out waiting for the covered row)"
chk "with something covered present, it removes only the stranded" "2" \
    "$(Q "SELECT supacache.prune_undurable()" | tail -1)"
chk "and leaves the covered row alone" "other:keep" "$(kv_keys)"

echo
echo "########## 7. a configuration it cannot honour refuses to start ##########"
stop_pg; set_overrides "acme:=forever"; start_pg; wait_ready || true
for _ in $(seq 1 30); do grep -q 'durability_overrides is invalid' $PGDATA/log && break; sleep 1; done
chk "an unparseable spec stops the worker" "1" "$(grep -c 'durability_overrides is invalid' $PGDATA/log)"
chk "and the message quotes the entry" "1" "$(grep -c 'forever' $PGDATA/log)"

stop_pg immediate; set_overrides "acme:=replicated"; start_pg; wait_ready || true
for _ in $(seq 1 30); do grep -q "names the 'replicated' tier" $PGDATA/log && break; sleep 1; done
chk "a replicated tier mixed with others stops the worker" "1" \
    "$(grep -c "names the 'replicated' tier" $PGDATA/log)"

stop_pg immediate

echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
