#!/usr/bin/env bash
# TTL deadlines survive a wall-clock step (#110).
#
# Expiry used to compare against CLOCK_REALTIME, so a step moved every key's
# deadline at once. Forward: everything with a deadline inside the jump expired
# together -- from the application's side, a cache that emptied itself for no
# reason. Backward: keys outlived their TTL by the size of the jump, which for a
# TTL used as a lock lease or a rate-limit window is a correctness problem. Both
# silent, neither diagnosable afterwards.
#
# This sets the SYSTEM CLOCK and checks what happens to live keys. The unit
# tests cover the arithmetic; only this covers the syscall, the shared anchor
# across processes, and Postgres itself surviving the step.
#
# REQUIRES A SETTABLE CLOCK (CAP_SYS_TIME). It skips rather than fails where the
# clock cannot be set, because a test that silently does nothing is worse than
# one that says it did nothing.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-clockstep}
PORT=${PGKS_PG_PORT:-5455}
RESP=${PGKS_RESP_PORT:-6425}
PROFILE=${PGKS_BUILD_PROFILE:-release}
STEP=${PGKS_CLOCK_STEP_SECS:-3600}
pass=0; fail=0; skipped=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
R() { redis-cli -p $RESP "$@" 2>/dev/null; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }

# Restore the clock however we leave, or the whole container is left skewed.
ORIG_EPOCH=""
restore_clock() {
  [ -n "$ORIG_EPOCH" ] || return 0
  # Put back the real time plus however long we have been running.
  date -s "@$(( ORIG_EPOCH + SECONDS ))" >/dev/null 2>&1
}
trap 'restore_clock' EXIT

echo "=== can the clock be set? ==="
ORIG_EPOCH=$(date -u +%s)
if ! date -s "@$ORIG_EPOCH" >/dev/null 2>&1; then
  echo "SKIP  the system clock is not settable here (needs CAP_SYS_TIME);"
  echo "      the arithmetic is covered by the store::tests::*clock* unit tests."
  exit 0
fi
echo "clock is settable"

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/clockstep_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/clockstep_install.log; exit 1; }

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.durability = 'ephemeral'"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }

echo
echo "########## 1. keys with a TTL well beyond the step ##########"
# Deadlines an hour and a half out. A forward step of one hour must not reach
# them; before #110 it would have expired every one.
R SET survives:1 v EX 5400 >/dev/null
R SET survives:2 v EX 5400 >/dev/null
R SET forever v >/dev/null
# A TTL SHORTER than the step, which is what makes the existence checks
# discriminating rather than decorative. With a 5400s TTL against a 3600s step
# the keys survive even with the bug present -- only the TTL number collapses.
# This one would be GONE under the old CLOCK_REALTIME behaviour, since the step
# carries the clock straight past its deadline.
R SET vanishes v EX 1800 >/dev/null
chk "the long-TTL keys are present to begin with" "2" "$(R EXISTS survives:1 survives:2)"
chk "and so is the key whose TTL is shorter than the step" "1" "$(R EXISTS vanishes)"
TTL_BEFORE=$(R TTL survives:1)
chk "and report a sane TTL ($TTL_BEFORE)" "1" \
    "$([ "${TTL_BEFORE:-0}" -gt 5000 ] && [ "${TTL_BEFORE:-0}" -le 5400 ] && echo 1 || echo 0)"

echo
echo "########## 2. step the wall clock forward ##########"
BEFORE=$(date -u +%s)
date -s "@$(( BEFORE + STEP ))" >/dev/null 2>&1
AFTER=$(date -u +%s)
chk "the system clock really moved (${STEP}s)" "1" \
    "$([ $(( AFTER - BEFORE )) -ge $(( STEP - 5 )) ] && echo 1 || echo 0)"
# Guard: if the step did not take, every assertion below passes for free.
chk "and Postgres survived it" "1" "$(Q 'SELECT 1')"
sleep 2

echo
echo "########## 3. nothing expired that should not have ##########"
chk "both long-TTL keys are still there after the step" "2" "$(R EXISTS survives:1 survives:2)"
# The assertion that fails loudest without the fix: this key's deadline is
# inside the jump, so a CLOCK_REALTIME-based expiry deletes it outright.
chk "and the key whose TTL is INSIDE the jump did not expire" "1" "$(R EXISTS vanishes)"
chk "with its value intact" "v" "$(R GET vanishes)"
chk "and still hold their value" "v" "$(R GET survives:1)"
chk "the key with no TTL is untouched" "v" "$(R GET forever)"
TTL_AFTER=$(R TTL survives:1)
chk "the remaining TTL did not collapse (was $TTL_BEFORE, now $TTL_AFTER)" "1" \
    "$([ "${TTL_AFTER:-0}" -gt 5000 ] && echo 1 || echo 0)"

echo
echo "########## 4. TTLs still work afterwards ##########"
# The clock must be usable, not merely frozen: a short TTL set after the step
# has to expire on time.
R SET shortlived v EX 2 >/dev/null
chk "a key set after the step is present" "1" "$(R EXISTS shortlived)"
sleep 4
chk "and expires on schedule" "0" "$(R EXISTS shortlived)"
chk "while the long-TTL keys are still alive" "2" "$(R EXISTS survives:1 survives:2)"

echo
echo "########## 5. a backward step does not resurrect or extend ##########"
date -s "@$(( $(date -u +%s) - STEP ))" >/dev/null 2>&1
sleep 2
chk "Postgres survived the step back" "1" "$(Q 'SELECT 1')"
chk "the long-TTL keys are still there" "2" "$(R EXISTS survives:1 survives:2)"
R SET shortlived2 v EX 2 >/dev/null
sleep 4
chk "and a short TTL set afterwards still expires" "0" "$(R EXISTS shortlived2)"

stop_pg
restore_clock
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
