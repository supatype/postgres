#!/usr/bin/env bash
# Regression test for the "orphan restarts the server" bug.
#
# Run directly under Docker with no init, postgres is PID 1 and adopts every orphan in the
# container. A health check that Docker times out leaves an orphaned pg_isready (exit 2), and
# Postgres 17 treats an unknown child exiting non-0/1 as a crashed backend and restarts the whole
# server. This starts the image, produces orphans two ways, and fails if the server restarts.
# It then checks that `docker stop` shuts down cleanly and that the next start does no crash
# recovery.
#
# Usage: scripts/test-orphan-restart.sh <image> [container-name-prefix]
set -u

IMAGE="${1:?usage: $0 <image> [name-prefix]}"
PREFIX="${2:-pid1-test}"
NAME="${PREFIX}-$$"
FAIL=0

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT

fail() { echo "FAIL: $*"; FAIL=1; }

start() {
  docker run -d --name "$NAME" \
    -e POSTGRES_PASSWORD=testpassword \
    -e SUPATYPE_SQL_EDITOR_PASSWORD=editorpassword \
    "$@" "$IMAGE" >/dev/null
  wait_ready
}

wait_ready() {
  for _ in $(seq 1 60); do
    if docker exec "$NAME" pg_isready -h 127.0.0.1 -U supatype_admin -d postgres -q 2>/dev/null; then
      # Past the temporary init server: the real one answers on TCP and stays up.
      sleep 3
      docker exec "$NAME" pg_isready -h 127.0.0.1 -U supatype_admin -d postgres -q 2>/dev/null && return 0
    fi
    sleep 2
  done
  echo "server never became ready"; docker logs "$NAME" 2>&1 | tail -30; exit 1
}

# Logs written after the last mark_logs call, so each step is judged on its own output.
MARK=0
mark_logs() { MARK="$(docker logs "$NAME" 2>&1 | wc -l)"; }
new_logs() { docker logs "$NAME" 2>&1 | tail -n +"$((MARK + 1))"; }

# Lines that mean the postmaster restarted itself.
restart_lines() {
  new_logs | grep -E "terminating any other active server processes|server process \(PID [0-9]+\) exited with exit code|all server processes terminated; reinitializing" || true
}

assert_no_restart() {
  local label="$1" found
  found="$(restart_lines)"
  if [ -n "$found" ]; then
    fail "$label: the server restarted"
    echo "$found"
  else
    echo "ok: $label: no restart"
  fi
}

echo "image: $IMAGE"
start

echo "--- repro 1: orphan that exits 2 after the exec returns"
mark_logs
docker exec "$NAME" sh -c '(sleep 2; exit 2) & exit 0'
sleep 6
assert_no_restart "orphan exit 2"

echo "--- repro 2: freeze PID 1 past the health check timeout"
mark_logs
docker kill -s SIGSTOP "$NAME" >/dev/null
sleep 12
docker kill -s SIGCONT "$NAME" >/dev/null
sleep 10
assert_no_restart "SIGSTOP 12s"

echo "--- clean stop"
mark_logs
START_MS="$(date +%s)"
docker stop -t 30 "$NAME" >/dev/null
ELAPSED=$(( $(date +%s) - START_MS ))
CODE="$(docker inspect -f '{{.State.ExitCode}}' "$NAME")"
LOGS="$(new_logs)"
echo "$LOGS" | grep -q "database system is shut down" && echo "ok: shut down line present" || fail "no 'database system is shut down' line"
[ "$CODE" = "0" ] && echo "ok: exit code 0" || fail "exit code $CODE"
[ "$ELAPSED" -lt 30 ] && echo "ok: stopped in ${ELAPSED}s" || fail "stop took ${ELAPSED}s"

echo "--- restart after stop does no crash recovery"
mark_logs
docker start "$NAME" >/dev/null
wait_ready
if new_logs | grep -q "database system was not properly shut down"; then
  fail "crash recovery ran after a clean stop"
else
  echo "ok: no crash recovery"
fi

exit "$FAIL"
