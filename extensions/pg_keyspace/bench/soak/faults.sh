#!/usr/bin/env bash
# Inject faults WHILE traffic is running (#113).
#
# run_durability_pg.sh already kills workers, fills disks, exhausts WAL and
# drops standbys. It does each one in isolation, against a quiet cluster. That
# is the right way to test that recovery works and it says nothing about what
# happens when recovery runs against a write flood -- which is the only time it
# ever runs in production.
#
# So this is not a second copy of the durability suite. It is a scheduler that
# fires a small, survivable set of faults at intervals during a soak, leaving
# the checksum verification in mixed.js and the drift thresholds in drift.sh to
# say whether the cluster came back correct as well as alive.
#
# Every fault here is one the system is DESIGNED to survive: the watchdog
# relaunches a terminated worker, a reload re-reads config, a dropped slot is
# recreated. A fault the design does not claim to survive belongs in the
# durability suite as a dedicated test with its own assertions, not scattered
# through a soak where the outcome would be ambiguous.
#
# Usage: faults.sh <seconds-between-faults> <logfile>
# Runs until killed. Env: PGBIN PGPORT PGDATA RESP_PORT SKIP_FAULTS
set -uo pipefail
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGPORT=${PGPORT:-5432}
PGHOST=${PGHOST:-/tmp}
PGDB=${PGDB:-postgres}
EVERY=${1:-60}
LOG=${2:-/tmp/soak_faults.log}
SKIP_FAULTS=${SKIP_FAULTS:-}

Q() { $PGBIN/psql -h "$PGHOST" -p "$PGPORT" -U postgres -d "$PGDB" -tAc "$1" 2>&1; }
say() { echo "[$(date -u +%H:%M:%S)] $*" | tee -a "$LOG"; }

# Terminate a RESP worker and let the watchdog bring it back. The interesting
# part is not that it restarts -- that has its own test -- but that it restarts
# while clients are mid-flight against it, and that the keys it owned come back
# readable and CORRECT (mixed.js checks the second part).
fault_kill_resp() {
  local pid
  pid=$(Q "SELECT pid FROM supacache.pg_stat_keyspace_activity
           WHERE role='resp' AND alive AND pid IS NOT NULL ORDER BY random() LIMIT 1")
  [[ "$pid" =~ ^[0-9]+$ ]] || { say "kill_resp: no live RESP worker to kill"; return 1; }
  say "kill_resp: terminating RESP worker pid=$pid"
  Q "SELECT pg_terminate_backend($pid)" >/dev/null
}

# Same for a persist worker, which is the one with a queue behind it: the ring
# keeps filling while it is gone, so this exercises backlog drain on relaunch
# rather than a cold start.
fault_kill_persist() {
  local pid
  pid=$(Q "SELECT pid FROM supacache.pg_stat_keyspace_activity
           WHERE role='persist' AND alive AND pid IS NOT NULL ORDER BY random() LIMIT 1")
  [[ "$pid" =~ ^[0-9]+$ ]] || { say "kill_persist: no live persist worker"; return 1; }
  local backlog
  backlog=$(Q "SELECT backlog_bytes FROM supacache.pg_stat_keyspace_persist_total")
  say "kill_persist: terminating persist worker pid=$pid (backlog ${backlog}B)"
  Q "SELECT pg_terminate_backend($pid)" >/dev/null
}

# The invalidation worker is the one whose absence is silent by design: the row
# cache fails closed, so killing it under load should show up as coherent=false
# in the drift samples and then recover -- not as stale rows being served.
fault_kill_invalidation() {
  local pid
  pid=$(Q "SELECT pid FROM supacache.pg_stat_keyspace_activity
           WHERE role='invalidation' AND alive AND pid IS NOT NULL")
  [[ "$pid" =~ ^[0-9]+$ ]] || { say "kill_invalidation: not running"; return 1; }
  say "kill_invalidation: terminating decoder pid=$pid"
  Q "SELECT pg_terminate_backend($pid)" >/dev/null
}

# A config reload mid-flight. Cheap, and it touches the paths that re-read
# credentials and TLS material while connections are open -- the place a
# reload-under-load bug would live.
fault_reload() {
  say "reload: pg_reload_conf()"
  Q "SELECT pg_reload_conf()" >/dev/null
}

# A checkpoint under write load, which is where persistence contends with
# Postgres's own IO rather than having it to itself.
fault_checkpoint() {
  say "checkpoint: forcing one under load"
  Q "CHECKPOINT" >/dev/null
}

# Long-running exclusive lock on a backing partition, so the persist worker
# meets a real conflict instead of an idle table.
fault_lock_partition() {
  say "lock_partition: 3s ACCESS EXCLUSIVE on supacache.kv_p0"
  ( Q "BEGIN; LOCK TABLE supacache.kv_p0 IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(3); COMMIT;" >/dev/null ) &
}

FAULTS=(fault_kill_resp fault_kill_persist fault_kill_invalidation
        fault_reload fault_checkpoint fault_lock_partition)

: > "$LOG"
say "fault injection every ${EVERY}s: ${FAULTS[*]}"
[ -n "$SKIP_FAULTS" ] && say "SKIPPING: $SKIP_FAULTS"

i=0
while :; do
  sleep "$EVERY"
  f=${FAULTS[$(( i % ${#FAULTS[@]} ))]}
  i=$((i+1))
  case " $SKIP_FAULTS " in *" $f "*) say "skip $f"; continue;; esac
  $f || true
  # Give the watchdog a moment, then record whether everything came back. This
  # is a log line, not a verdict: the verdict belongs to drift.sh and to the
  # checksum thresholds, which is what stops a fault from being "injected" into
  # a cluster that quietly never recovered.
  sleep 10
  alive=$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE alive")
  total=$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity")
  say "  after $f: $alive/$total workers alive"
done
