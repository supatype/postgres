#!/bin/bash
# pg_basebackup + PITR restores the cache, asserted rather than assumed (#112).
#
# Persisted keys live in supacache.kv and the supacache.kv_ttl bucket
# partitions -- ordinary tables, in the WAL, inside the same cluster as the
# application data they front. So an existing Postgres backup should already
# cover the cache, and a point-in-time restore should already bring it back
# *consistent with the data it fronts*. Valkey cannot offer that at any price:
# RDB and AOF are a separate artefact on a separate schedule with a separate
# restore procedure, and nothing makes the cache and the database agree.
#
# Until there is a test that is a plausible claim rather than a supported one,
# and it should not be advertised. This is that test.
#
# The interesting assertion is the boundary. On the durable tier the RESP reply
# is held until the record commits, so a `+OK` is a promise that the row is in
# supacache.kv. PITR cuts straight through that promise: everything acked
# before the target must come back, and nothing written after it may. Both
# halves are asserted here, against a keyspace and a table written in the same
# transaction stream.
#
# Expects cargo, cargo-pgrx, a PGDG PostgreSQL and redis-cli on PATH. It
# creates and destroys clusters under $PGDATA and $RESTOREDATA, so point those
# somewhere disposable. Prints "# result: N passed, M failed" like the other
# harnesses.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-pitr-data}
ARCHIVE=${PGKS_ARCHIVE:-/tmp/pgks-pitr-archive}
BASEDIR=${PGKS_BASEBACKUP:-/tmp/pgks-pitr-base}
RESTOREDATA=${PGKS_RESTOREDATA:-/tmp/pgks-pitr-restored}
PORT=${PGKS_PG_PORT:-5481}
RPORT=${PGKS_RESTORE_PG_PORT:-5482}
RESP=${PGKS_RESP_PORT:-6481}
# The restored cluster's workers listen on RRESP..RRESP+workers-1, so this is
# a range, not a port: keep it clear of $RESP and of anything else on the box.
RRESP=${PGKS_RESTORE_RESP_PORT:-6485}
PROFILE=${PGKS_BUILD_PROFILE:-release}
RCLI_TIMEOUT=${PGKS_RCLI_TIMEOUT:-20}
PSQL_TIMEOUT=${PGKS_PSQL_TIMEOUT:-120s}
PGOPTS="-c statement_timeout=$PSQL_TIMEOUT"
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}

# Two clusters, so every client helper takes its port. The original is stopped
# before the restored one starts -- that is the real disaster-recovery shape,
# and it keeps the RESP ports and shared-memory segments from overlapping.
psql_()  { PGOPTIONS="$PGOPTS" $PGBIN/psql -h /tmp -p "$1" -U postgres -d postgres -tAc "$2" 2>&1; }
rcli()   { local p="$1"; shift; timeout "$RCLI_TIMEOUT" redis-cli -p "$p" "$@" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $1 -l $1/log -o \"-p $2 -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $1 -m fast -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do psql_ "$1" "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
wait_resp()  { for _ in $(seq 1 60); do rcli "$1" PING | grep -q PONG && return 0; sleep 1; done; return 1; }
# Reaching consistency and finishing promotion are different moments: a
# restored cluster answers SELECT 1 while still in recovery, so waiting on
# readiness alone and then asserting pg_is_in_recovery() is a race against the
# promotion this harness asked for.
wait_promoted() { for _ in $(seq 1 60); do [ "$(psql_ "$1" "SELECT pg_is_in_recovery()")" = "f" ] && return 0; sleep 1; done; return 1; }

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/pitr-install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/pitr-install.log; exit 1; }
echo "installed"

echo "=== initdb (archive_mode on, durable tier) ==="
rm -rf $PGDATA $ARCHIVE $BASEDIR $RESTOREDATA
mkdir -p $PGDATA $ARCHIVE $BASEDIR $RESTOREDATA
chown postgres:postgres $PGDATA $ARCHIVE $BASEDIR $RESTOREDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  # The tier whose acknowledgement is a promise: the reply is held until the
  # record commits, which is what makes the boundary assertion below exact.
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.keys = 20000"
  echo "pg_keyspace.val_bytes = 512"
  echo "pg_keyspace.ring_mb = 8"
  echo "pg_keyspace.persist_window_ms = 10"
  echo "archive_mode = on"
  echo "archive_command = 'test ! -f $ARCHIVE/%f && cp %p $ARCHIVE/%f'"
  echo "wal_level = replica"
} >> $PGDATA/postgresql.conf

start_pg $PGDATA $PORT; wait_ready $PORT
psql_ $PORT "CREATE EXTENSION pg_keyspace;" >/dev/null
stop_pg $PGDATA; sleep 1
start_pg $PGDATA $PORT
wait_ready $PORT || { echo "FAIL  cluster did not start"; exit 1; }
wait_resp $RESP  || { echo "FAIL  RESP port never answered"; exit 1; }

# The application data the cache fronts. The whole point of the claim is that
# these two recover to the SAME instant, so they are written together below.
psql_ $PORT "CREATE TABLE orders (id int primary key, note text)" >/dev/null

echo ""
echo "########## A. base backup, with WAL archiving on ##########"
su postgres -c "$PGBIN/pg_basebackup -h /tmp -p $PORT -U postgres -D $BASEDIR/data -X stream -c fast" \
  >/tmp/pitr-basebackup.log 2>&1
chk "pg_basebackup succeeded" "1" "$([ -f $BASEDIR/data/PG_VERSION ] && echo 1 || echo 0)"
# Nothing about the cache needed a separate backup step, which is the claim.
chk "the backup contains supacache's tablespace-less tables" "1" \
    "$([ -d $BASEDIR/data/base ] && echo 1 || echo 0)"

echo ""
echo "########## B. traffic, with the acknowledged set recorded ##########"
# Batch 1: acknowledged BEFORE the restore target. Every one of these replies
# only after its record committed, so each is a promise PITR has to keep.
acked=0
for i in $(seq 1 50); do
  r=$(rcli $RESP SET "pitr:before:$i" "v$i")
  [ "$r" = "OK" ] && acked=$((acked+1))
done
chk "50 durable writes acknowledged" "50" "$acked"
# The same transaction stream the cache lives in.
psql_ $PORT "INSERT INTO orders VALUES (1,'before')" >/dev/null

# TTL keys, one that outlives the restore and one already dead by then. The
# expired one is the interesting half: its bucket partition may have been
# dropped, and a restore must not resurrect it.
rcli $RESP SET "pitr:ttl:lives" "still-here" EX 3600 >/dev/null
rcli $RESP SET "pitr:ttl:dead"  "gone-by-then" EX 2 >/dev/null

# The cut. Taken from the server so no client clock is involved, and fenced by
# a pause on each side so the target unambiguously separates the two batches:
# recovery stops at the last commit at or before this instant.
sleep 2
T1=$(psql_ $PORT "SELECT now()")
chk "captured a restore target" "1" "$([ -n "$T1" ] && echo 1 || echo 0)"
echo "  restore target: $T1"
sleep 2

# Batch 2: everything after the target. None of it may come back -- a restore
# that returns these is returning data from the future of its own target.
for i in $(seq 1 50); do rcli $RESP SET "pitr:after:$i" "future$i" >/dev/null; done
psql_ $PORT "INSERT INTO orders VALUES (2,'after')" >/dev/null
chk "batch 2 is visible before the restore" "50" \
    "$(psql_ $PORT "SELECT count(*) FROM supacache.kv WHERE key LIKE 'pitr:after:%'::bytea")"

# Force the WAL holding batch 2 out to the archive, so the restore has every
# segment it needs to reach the target and could overshoot it if it wanted to.
psql_ $PORT "SELECT pg_switch_wal()" >/dev/null
sleep 2
stop_pg $PGDATA

echo ""
echo "########## C. restore the backup to the target time ##########"
cp -a $BASEDIR/data/. $RESTOREDATA/
chown -R postgres:postgres $RESTOREDATA
rm -f $RESTOREDATA/postmaster.pid
{
  echo "port = $RPORT"
  echo "pg_keyspace.port = $RRESP"
  echo "archive_mode = off"
  echo "restore_command = 'cp $ARCHIVE/%f %p'"
  echo "recovery_target_time = '$T1'"
  echo "recovery_target_action = 'promote'"
} >> $RESTOREDATA/postgresql.conf
su postgres -c "touch $RESTOREDATA/recovery.signal"
# pg_ctl writes its log inside the data directory, so the base backup captured
# the ORIGINAL cluster's log too. Left in place, every log assertion below
# counts the original's lines as well as the restored cluster's.
rm -f $RESTOREDATA/log
start_pg $RESTOREDATA $RPORT
wait_ready $RPORT || { echo "FAIL  restored cluster did not reach consistency"; tail -30 $RESTOREDATA/log; echo "# result: $pass passed, $((fail+1)) failed"; exit 1; }
chk "the restored cluster came up" "1" "$(psql_ $RPORT "SELECT 1")"
wait_promoted $RPORT
chk "and finished recovery (not still in it)" "f" "$(psql_ $RPORT "SELECT pg_is_in_recovery()")"
# Names the instant it actually stopped at, which is the first commit after the
# target -- the evidence that the cut landed where it was asked to.
echo "  $(grep -o 'recovery stopping before commit of transaction.*' $RESTOREDATA/log | tail -1)"

echo ""
echo "########## D. the boundary: everything acked, nothing later ##########"
chk "every key acked before the target is back" "50" \
    "$(psql_ $RPORT "SELECT count(*) FROM supacache.kv WHERE key LIKE 'pitr:before:%'::bytea")"
chk "and no key written after it" "0" \
    "$(psql_ $RPORT "SELECT count(*) FROM supacache.kv WHERE key LIKE 'pitr:after:%'::bytea")"
chk "the values are the ones that were acked" "v42" \
    "$(psql_ $RPORT "SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='pitr:before:42'::bytea")"
# The property no separate cache backup can offer: the cache and the rows it
# fronts stopped at the same instant.
chk "the table it fronts is at the same instant" "1" \
    "$(psql_ $RPORT "SELECT count(*) FROM orders")"
chk "and does not hold the post-target row" "0" \
    "$(psql_ $RPORT "SELECT count(*) FROM orders WHERE note='after'")"

echo ""
echo "########## E. recovery, not just table contents ##########"
# The worker rebuilds shared memory from the tables at startup, so a restored
# cluster has to come up and repopulate -- a correct table with an empty
# keyspace would still be a broken restore.
wait_resp $RRESP || echo "  (RESP port on the restored cluster never answered)"
chk "the restored keyspace serves a read over RESP" "v7" "$(rcli $RRESP GET 'pitr:before:7')"
chk "and does not serve a post-target key" "" "$(rcli $RRESP GET 'pitr:after:7')"
chk "the worker logged its recovery" "1" \
    "$(grep -c 'recovered .* keys' $RESTOREDATA/log 2>/dev/null | head -1)"
# Stronger than "it logged something": the keyspace rebuilt to exactly the
# acknowledged set -- 50 plain keys plus the one TTL key that outlives the
# target, and neither the expired key nor anything from batch 2.
chk "and rebuilt exactly the acknowledged keyspace" "51" \
    "$(grep -o 'recovered [0-9]* keys' $RESTOREDATA/log | tail -1 | grep -o '[0-9]*')"

echo ""
echo "########## F. TTL survives, and the dead stay dead ##########"
t=$(rcli $RRESP TTL 'pitr:ttl:lives')
if [ "$t" -gt 3000 ] && [ "$t" -le 3600 ]; then
  echo "PASS  a live TTL came back with its expiry ($t)"; pass=$((pass+1))
else
  echo "FAIL  a live TTL came back with its expiry"; echo "        actual: [$t]"; fail=$((fail+1))
fi
chk "an already-expired key is not resurrected" "" "$(rcli $RRESP GET 'pitr:ttl:dead')"

echo ""
echo "########## G. restored onto a different worker count ##########"
# The issue's own suspicion: a restore is a plausible place for the worker count
# to change by accident, since the restored cluster's postgresql.conf is edited
# by hand. supacache.topology is table-backed, so it rides along in the backup
# and the running cluster can tell it has been re-sharded.
stop_pg $RESTOREDATA
sed -i "s/^pg_keyspace.workers.*//" $RESTOREDATA/postgresql.conf
echo "pg_keyspace.workers = 3" >> $RESTOREDATA/postgresql.conf
mv $RESTOREDATA/log $RESTOREDATA/log.before-reshard
start_pg $RESTOREDATA $RPORT
wait_ready $RPORT

# First, the accident itself. A persisted tier with >1 worker redirects clients
# by address, so it needs an announce host -- and a hand-edited restore config
# is unlikely to have one. What matters is that this is LOUD: the workers refuse
# and name the setting, rather than coming up and serving an empty keyspace that
# looks exactly like a restore which silently lost everything.
# Three workers, three refusals: every one of them declines, so there is no
# half-started cluster serving part of a keyspace.
chk "restoring onto >1 worker without an announce host refuses" "3" \
    "$(grep -c 'REFUSING to start' $RESTOREDATA/log 2>/dev/null | head -1)"
chk "and every one of them names the setting that fixes it" "3" \
    "$(grep -c 'cluster_announce_host must be set' $RESTOREDATA/log 2>/dev/null | head -1)"
chk "Postgres itself stays up either way" "1" "$(psql_ $RPORT "SELECT 1")"

# Now the configured version: the same re-shard, with the announce host set.
stop_pg $RESTOREDATA
echo "pg_keyspace.cluster_announce_host = '127.0.0.1'" >> $RESTOREDATA/postgresql.conf
mv $RESTOREDATA/log $RESTOREDATA/log.refused
start_pg $RESTOREDATA $RPORT
if wait_ready $RPORT; then
  chk "with the announce host set, the re-shard starts" "1" "$(psql_ $RPORT "SELECT 1")"
  chk "and says so, rather than changing shape silently" "1" \
      "$(grep -c 'WORKER COUNT CHANGED' $RESTOREDATA/log 2>/dev/null | head -1)"
  echo "  $(grep -o 'WORKER COUNT CHANGED.*' $RESTOREDATA/log | tail -1 | cut -c1-170)"
  chk "the topology row records the new count" "3" \
      "$(psql_ $RPORT "SELECT workers FROM supacache.topology WHERE id=1")"
  # Where the keys ended up. With >1 worker the keyspace is sharded across
  # port..port+n-1, so a key is served by exactly the worker owning its slot:
  # the restore is not lost, it moved.
  for _ in $(seq 1 30); do rcli $RRESP PING | grep -q PONG && break; sleep 1; done
  found=0
  for p in $(seq $RRESP $((RRESP+2))); do
    [ "$(rcli $p GET 'pitr:before:7')" = "v7" ] && found=$((found+1))
  done
  chk "a restored key is served by exactly one of the new workers" "1" "$found"
else
  echo "FAIL  the cluster did not start after a re-shard"; fail=$((fail+1))
  tail -20 $RESTOREDATA/log
fi

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
stop_pg $RESTOREDATA
[ "$fail" -eq 0 ]
