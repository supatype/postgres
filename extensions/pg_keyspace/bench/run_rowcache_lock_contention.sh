#!/usr/bin/env bash
# The row-cache writer lock, partitioned (#120, on top of #127).
#
# #127 gave the row-cache segment a writer lock because it had no owning
# process: its writers are ordinary BACKENDS -- rowcache_put, registration, and
# above all read-through in rc_access, which makes a writer of every backend
# that misses. Two at once corrupted the arena and segfaulted a backend, which
# Postgres answers by crash-restarting the whole cluster.
#
# ONE lock for the whole segment was right while the segment served ONE
# database. #120 makes every database read-through into it, and a single lock is
# then a CLUSTER-WIDE serialisation point for row-cache writes: one database
# with a cold cache and heavy read-through stalls caching for every other one.
#
# So the lock follows the segment's own partitioning -- Store::partition_of
# names the partition, and each partition has its own lock.
#
# Two things are being checked, and only one of them is an assertion:
#
#   * ASSERTED: partitioning does not break what #127 fixed. Both
#     configurations must survive the same concurrent read-through race, serve
#     correct rows, and keep the cache coherent. This is the one that matters:
#     if partition_of disagreed with where get/set actually route, writers would
#     hold different locks while mutating the SAME arena -- the #127 crash, with
#     a lock in front of it saying it could not happen. Corruption shows up here
#     as a segfault or a malformed row, not as a slow run.
#
#   * ASSERTED: raising the partition count must not multiply the shared memory
#     an operator asked for. `rowcache_mb` has always meant the size of the
#     whole segment; partitions divide it.
#
#   * MEASURED, not asserted: throughput and observed waits on the row-cache
#     lock, at 1 partition vs N. Printed as a table. Timing on a shared CI
#     runner is not a thing to gate a merge on, and the issue asked for this to
#     be evidenced rather than assumed.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-rclock}
PORT=${PGKS_PG_PORT:-5469}
RESP=${PGKS_RESP_PORT:-6471}
PROFILE=${PGKS_BUILD_PROFILE:-release}
CLIENTS=${CLIENTS:-8}
SECS=${SECS:-20}
ROWS=${ROWS:-50000}
PARTS=${PARTS:-8}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop -m fast" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
log_lines() { wc -l < $PGDATA/log 2>/dev/null || echo 0; }
crashes_since() { tail -n +"${1:-0}" $PGDATA/log 2>/dev/null | grep -c "signal 11"; }

# Rewrite pg_keyspace.rowcache_partitions and restart. It is Postmaster
# context -- the segment geometry and the lock tranche are both sized at
# postmaster start -- so there is no reloading it.
set_partitions() {
  sed -i "/^pg_keyspace.rowcache_partitions/d" $PGDATA/postgresql.conf
  echo "pg_keyspace.rowcache_partitions = $1" >> $PGDATA/postgresql.conf
  stop_pg; sleep 1; start_pg; wait_ready || return 1
  return 0
}

# Coherence is waited for AFTER registering, not before. A database with no
# registrations gets no slot and no worker (#120), so it never becomes coherent
# and a wait placed ahead of registration can only ever time out -- which is
# what it did, taking every round with it.
wait_coherent() {
  for _ in $(seq 1 "${1:-90}"); do
    [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()")" = "t" ] && return 0
    sleep 1
  done
  return 1
}

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/rclock_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/rclock_install.log; exit 1; }
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/rclock_plugin.log 2>&1 || {
  echo "PLUGIN INSTALL FAILED"; tail -20 /tmp/rclock_plugin.log; exit 1; }

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
  # Read-through ON is the whole point: it is what makes a backend a writer,
  # and therefore what contends for the lock being measured.
  echo "pg_keyspace.rowcache_readthrough = on"
  # Small, so eviction runs during the test. ensure_alloc's eviction path is
  # the longest critical section a writer holds, so it is where contention
  # actually shows up -- and where an arena/lock mismatch would corrupt.
  echo "pg_keyspace.rowcache_mb = 16"
  echo "wal_level = logical"
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
  echo "max_connections = 100"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi
Q "CREATE EXTENSION pg_keyspace" >/dev/null
stop_pg; sleep 1; start_pg; wait_ready || { echo "NO START"; exit 1; }

Q "CREATE TABLE profiles(id bigint primary key, tenant text NOT NULL, payload text NOT NULL)" >/dev/null
Q "INSERT INTO profiles SELECT g, 't'||(g%3), md5(g::text)||repeat('p',200) FROM generate_series(1,$ROWS) g" >/dev/null

cat > /tmp/rclock_read.sql <<PG
\\set id random(1, $ROWS)
SELECT payload FROM profiles WHERE id = :id;
PG
cat > /tmp/rclock_write.sql <<PG
\\set id random(1, $ROWS)
UPDATE profiles SET payload = md5(random()::text) || repeat('p',200) WHERE id = :id;
PG

# One measured run at a given partition count. Prints "tps waits data_cap".
run_round() {
  local parts=$1
  printf "0 0 0\n" > /tmp/rclock_round_$parts   # so a failed round cannot leave §3 reading an unset variable
  set_partitions "$parts" || { chk "  the cluster restarts at $parts partition(s)" "t" "f"; return 1; }
  chk "  the table registers at $parts partition(s)" "t" \
      "$(Q "SELECT supacache.rowcache_register('public.profiles')")"
  chk "  and the cache becomes coherent at $parts partition(s)" "t" \
      "$(wait_coherent 120 && echo t || echo f)"
  # Warm, so the run is steady-state rather than mostly cold-start.
  Q "SELECT supacache.rowcache_put('public.profiles', g) FROM generate_series(1,2000) g" >/dev/null
  local cap; cap=$(Q "SELECT data_cap FROM supacache.rowcache_stats()")
  local n0; n0=$(log_lines)

  # Sample pg_stat_activity for backends parked on OUR lock tranche while the
  # load runs. The tranche is named, so the wait event is named too, which is
  # what makes this specific rather than "something was waiting".
  ( waits=0
    for _ in $(seq 1 200); do
      w=$($PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc \
            "SELECT count(*) FROM pg_stat_activity
              WHERE wait_event_type = 'LWLock'
                AND wait_event = 'pg_keyspace_rowcache_write'" 2>/dev/null)
      waits=$(( waits + ${w:-0} ))
      sleep 0.1
    done
    echo "$waits" > /tmp/rclock_waits_$parts ) &
  local sampler=$!

  local out tps
  out=$($PGBIN/pgbench -h /tmp -p $PORT -U postgres -n -c $CLIENTS -j 4 -T $SECS \
          -f /tmp/rclock_read.sql@8 -f /tmp/rclock_write.sql@2 postgres 2>&1)
  wait $sampler 2>/dev/null
  tps=$(grep -oP '^tps = \K[0-9]+' <<<"$out" | head -1)
  ABORTED=$(grep -c "aborted in command" <<<"$out")
  CRASH=$(crashes_since "$n0")

  echo
  echo "----- $parts partition(s) -----"
  chk "  no backend segfaulted at $parts partition(s)" "0" "$CRASH"
  chk "  no client was aborted at $parts partition(s)" "0" "$ABORTED"
  chk "  the cluster is still up at $parts partition(s)" "1" "$(Q "SELECT 1")"
  # A torn read does not segfault: it splices the head of one tuple onto the
  # tail of another and deforms into a plausible-looking wrong row. THIS is the
  # assertion that catches a lock guarding the wrong arena.
  local mismatch
  mismatch=$(Q "SELECT count(*) FROM profiles p
                WHERE p.payload IS NULL OR length(p.payload) <> 232
                   OR p.payload !~ '^[0-9a-f]{32}p+\$'")
  chk "  every row reads back well-formed at $parts partition(s)" "0" "$mismatch"
  chk "  ...over all $ROWS rows" "$ROWS" "$(Q "SELECT count(*) FROM profiles")"
  chk "  the cache served reads at $parts partition(s)" "t" \
      "$(Q "SELECT hits > 0 FROM supacache.rowcache_stats()")"
  chk "  the cache is still coherent at $parts partition(s)" "t" \
      "$(Q "SELECT coherent FROM supacache.rowcache_coherence()")"
  # A run that did nothing could not have corrupted anything either.
  chk "  the run did enough work to be meaningful (${tps:-0} tps)" "t" \
      "$([ "${tps:-0}" -gt 200 ] && echo t || echo f)"

  printf "%s %s %s\n" "${tps:-0}" "$(cat /tmp/rclock_waits_$parts 2>/dev/null || echo 0)" "$cap" \
    > /tmp/rclock_round_$parts
}

echo
echo "########## 1. one lock for the whole segment (the #127 shape) ##########"
run_round 1
read -r TPS1 WAIT1 CAP1 < /tmp/rclock_round_1 || true
TPS1=${TPS1:-0}; WAIT1=${WAIT1:-0}; CAP1=${CAP1:-0}

echo
echo "########## 2. one lock per partition (#120) ##########"
run_round $PARTS
read -r TPSN WAITN CAPN < /tmp/rclock_round_$PARTS || true
TPSN=${TPSN:-0}; WAITN=${WAITN:-0}; CAPN=${CAPN:-0}

echo
echo "########## 3. partitioning must not multiply shared memory ##########"
# `rowcache_mb` means the size of the WHOLE segment and always has. If raising
# the partition count grew the arena instead of dividing it, an operator who
# set rowcache_partitions would silently get N times the shared memory they
# asked for -- and on a machine sized for the old request, a postmaster that
# will not start.
chk "the arena is the same size at 1 and $PARTS partitions" "t" \
    "$([ -n "$CAP1" ] && [ -n "$CAPN" ] && [ "$CAP1" -gt 0 ] \
        && [ "$(( CAPN * 100 / CAP1 ))" -ge 90 ] && [ "$(( CAPN * 100 / CAP1 ))" -le 110 ] \
        && echo t || echo f)"

echo
echo "########## 4. measurement (not a gate) ##########"
printf "%-14s %12s %16s %14s\n" "partitions" "tps" "lock-wait obs" "arena bytes"
printf "%-14s %12s %16s %14s\n" "1" "${TPS1:-0}" "${WAIT1:-0}" "${CAP1:-0}"
printf "%-14s %12s %16s %14s\n" "$PARTS" "${TPSN:-0}" "${WAITN:-0}" "${CAPN:-0}"
echo
echo "# 'lock-wait obs' is the summed count of backends observed parked on the"
echo "# pg_keyspace_rowcache_write tranche across 200 samples during the run."
echo "# Lower is less contention. Not asserted: a shared CI runner is not a"
echo "# bench rig, and a timing gate there is a flaky gate."

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
