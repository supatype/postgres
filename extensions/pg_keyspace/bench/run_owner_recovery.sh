#!/usr/bin/env bash
# Owner-addressed recovery (#164): a persisted multi-worker cluster a plain,
# non-cluster client can actually use.
#
# Slot addressing makes the worker that ACCEPTS a write the one that must
# recover it, which is why a persisted tier with workers > 1 issues MOVED and
# needs both cluster_announce_host and a cluster-aware client. Owner
# addressing stamps the accepting worker into supacache.kv instead, so a key
# comes back where it was written and nothing has to redirect.
#
# Section 5 is the one that matters most, and it is not what the original
# prototype claimed. "Graceful scaling" holds for the STORAGE at any worker
# count -- owner % workers partitions the owner space, so no key is ever
# orphaned. It does not hold for where a CLIENT will look. Recovery places a
# key at `owner % workers` and a client shards `hash(key) % workers`; those
# agree for every key only when the new count divides the old one. So 4 -> 2
# is safe and 2 -> 4, 4 -> 3 and every growth are not: the key is present, on
# a worker nobody asks, and the rewrite that follows is collapsed by the
# single supacache.kv row at the NEXT restart. That is a loss landing one
# restart after the resize that caused it, so startup refuses it -- and this
# harness asserts the refusal rather than asserting the old claim.
#
# Self-contained: builds the extension, owns its cluster, restarts it at
# several worker counts (pg_keyspace.workers is postmaster context).
# Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-owner}
PORT=${PGKS_PG_PORT:-5473}
RESP=${PGKS_RESP_PORT:-6473}
PROFILE=${PGKS_BUILD_PROFILE:-release}
PER_WORKER=${PGKS_KEYS_PER_WORKER:-25}

pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -X -q -A -t -c "$1" 2>&1; }
# Bounded, like the other harnesses: a redis-cli left waiting on a worker that
# refused to start turns a clear assertion failure into a hung job.
R() { local p=$1; shift; timeout 5 redis-cli -p "$p" "$@" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
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
# A worker recovers BEFORE it listens, and each does so independently, so
# waiting on the ports alone races a worker still loading and reads an empty
# segment as data loss. Wait for every worker to have recovered AND opened its
# port. (The prototype harness learned this the expensive way: a verify ran
# mid-recovery and reported a convincing false failure.)
wait_workers() {
  local n=$1
  for _ in $(seq 1 90); do
    local rec lis
    rec=$(grep -c 'recovered [0-9]* keys' $PGDATA/log 2>/dev/null)
    lis=$(grep -c 'RESP listening on 0.0.0.0:' $PGDATA/log 2>/dev/null)
    [ "${rec:-0}" -ge "$n" ] && [ "${lis:-0}" -ge "$n" ] && return 0
    sleep 1
  done
  return 1
}
set_guc() { # set_guc <name> <value>
  grep -v "^$1 " $PGDATA/postgresql.conf > $PGDATA/conf.tmp
  mv $PGDATA/conf.tmp $PGDATA/postgresql.conf
  echo "$1 = $2" >> $PGDATA/postgresql.conf
  chown postgres:postgres $PGDATA/postgresql.conf
}
# Restart at a worker count, truncating the log first so the assertions below
# read only this boot. Stop before truncating, always: a worker appending
# after the truncate is how the registration harness became intermittent.
boot() {
  stop_pg
  : > $PGDATA/log; chown postgres:postgres $PGDATA/log
  set_guc "pg_keyspace.workers" "$1"
  start_pg
}

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/owner_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/owner_install.log; exit 1; }

echo "=== initdb ==="
stop_pg immediate
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.database = 'postgres'"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.recovery_addressing = 'owner'"
  echo "pg_keyspace.workers = 4"
  # Small on purpose. This is a placement test, not a capacity one, and the
  # default sizing reserves a segment and a ring set PER WORKER -- about a
  # gigabyte of shared memory across four, which is a lot to map and unmap
  # five times in a harness that restarts the cluster at four worker counts.
  echo "pg_keyspace.keys = 10000"
  echo "pg_keyspace.val_bytes = 1024"
  echo "pg_keyspace.ring_mb = 1"
  echo "pg_keyspace.rowcache_mb = 1"
  # Deliberately NO pg_keyspace.cluster_announce_host: slot addressing refuses
  # to start a persisted multi-worker tier without one, and not needing it is
  # half the point of owner addressing.
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; tail -20 $PGDATA/log; exit 1; }
Q "CREATE EXTENSION pg_keyspace" >/dev/null
boot 4; wait_ready || { echo "NO RESTART"; tail -20 $PGDATA/log; exit 1; }
wait_workers 4 || { echo "WORKERS NEVER CAME UP"; tail -30 $PGDATA/log; exit 1; }

echo
echo "########## 1. a persisted cluster with no announce host ##########"
chk "four workers are persisted" "4" "$(grep -c 'persistence ON' $PGDATA/log)"
chk "none of them announced a slot map" "0" "$(grep -c 'answer MOVED' $PGDATA/log)"
chk "each says it recovers what it wrote" "4" "$(grep -c "recovery addressing is 'owner'" $PGDATA/log)"

echo
echo "########## 2. a plain client is not redirected ##########"
# The control for this is section 7: under slot addressing the very first SET
# comes back MOVED, which is the reason owner addressing exists.
moved=0
for i in 0 1 2 3; do
  for j in $(seq 1 $PER_WORKER); do
    out=$(R $((RESP + i)) SET "own:w${i}:k${j}" "by-worker-${i}")
    [ "$out" = "OK" ] || moved=$((moved + 1))
  done
done
chk "every SET to every worker was accepted" "0" "$moved"

echo
echo "########## 3. a restart at the same count brings each key back ##########"
boot 4; wait_ready || exit 1; wait_workers 4 || exit 1
missing=0
for i in 0 1 2 3; do
  for j in $(seq 1 $PER_WORKER); do
    [ "$(R $((RESP + i)) GET "own:w${i}:k${j}")" = "by-worker-${i}" ] || missing=$((missing + 1))
  done
done
chk "every key is back on the worker that wrote it" "0" "$missing"

echo
echo "########## 4. a shrink to a divisor keeps them findable ##########"
# 4 -> 2 is the safe shape: (h % 4) % 2 == h % 2, so a client sharding the key
# asks the worker recovery used.
boot 2; wait_ready || exit 1; wait_workers 2 || exit 1
chk "the cluster started" "2" "$(grep -c 'persistence ON' $PGDATA/log)"
missing=0
for i in 0 1 2 3; do
  for j in $(seq 1 $PER_WORKER); do
    [ "$(R $((RESP + i % 2)) GET "own:w${i}:k${j}")" = "by-worker-${i}" ] || missing=$((missing + 1))
  done
done
chk "every key is at owner % 2" "0" "$missing"

echo
echo "########## 5. an unsafe resize is refused, not silently served ##########"
# 2 -> 3 does not divide. Recovery would place keys where no client asks, and
# the rewrite that follows is lost at the restart after this one.
boot 3; wait_ready || true
for _ in $(seq 1 30); do grep -q 'REFUSING to start' $PGDATA/log && break; sleep 1; done
chk "startup refused the resize" "1" \
    "$([ "$(grep -c 'REFUSING to start' $PGDATA/log)" -ge 1 ] && echo 1 || echo 0)"
chk "and named the way through" "1" \
    "$([ "$(grep -c 'supacache.rehome(3)' $PGDATA/log)" -ge 1 ] && echo 1 || echo 0)"
chk "no worker served RESP" "0" "$(grep -c 'RESP listening on 0.0.0.0:' $PGDATA/log)"

echo
echo "########## 6. rehome makes it safe ##########"
# Back to a count that starts, so the rehome can be run.
boot 2; wait_ready || exit 1; wait_workers 2 || exit 1
rehomed=$(Q "SELECT supacache.rehome(3)" | tail -1)
chk "rehome rewrote every row it had to" "1" "$([ "${rehomed:-0}" -gt 0 ] && echo 1 || echo 0)"
boot 3; wait_ready || exit 1
wait_workers 3 || { echo "STILL REFUSING after rehome"; grep 'REFUSING' $PGDATA/log | head -2; }
chk "the cluster now starts at 3" "3" "$(grep -c 'persistence ON' $PGDATA/log)"
# After a rehome, placement is what supacache.key_worker() reports -- the only
# placement a client can compute for itself.
missing=0
for i in 0 1 2 3; do
  for j in $(seq 1 $PER_WORKER); do
    k="own:w${i}:k${j}"
    w=$(Q "SELECT supacache.key_worker('$k')")
    [ "$(R $((RESP + w)) GET "$k")" = "by-worker-${i}" ] || missing=$((missing + 1))
  done
done
chk "every key is where key_worker() says" "0" "$missing"
chk "topology_change() reports the mode it is in" "owner" \
    "$(Q "SELECT addressing FROM supacache.topology_change()")"
chk "and does not report a slot number it is not using" "" \
    "$(Q "SELECT slots_moved FROM supacache.topology_change()")"

echo
echo "########## 7. the control: slot addressing refuses a plain client ##########"
# Expected to fail a plain client at the first write. This is what makes
# section 2 mean something rather than being a tautology.
stop_pg
: > $PGDATA/log; chown postgres:postgres $PGDATA/log
set_guc "pg_keyspace.recovery_addressing" "'slot'"
set_guc "pg_keyspace.workers" "4"
set_guc "pg_keyspace.cluster_announce_host" "'127.0.0.1'"
start_pg; wait_ready || exit 1; wait_workers 4 || true
out=$(R $((RESP)) SET "own:w1:k1" x)
chk "a misrouted SET is answered MOVED" "1" \
    "$(echo "$out" | grep -c '^MOVED' )"

stop_pg immediate

echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
