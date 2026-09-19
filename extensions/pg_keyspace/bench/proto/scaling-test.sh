#!/bin/sh
# Graceful scaling under pg_keyspace.recovery_addressing = 'owner'.
#
# Writes a known key to every worker with a plain (non-cluster) client, restarts
# the cluster at a different worker count, and requires that every key is still
# readable somewhere, at the worker `owner % workers` predicts. Run inside a
# container sharing the database container's network namespace.
#
#   $1  number of workers currently running
#   $2  "write" | "verify"
#   $3  base port (default 6379)
set -e
N="$1"; MODE="$2"; BASE="${3:-6379}"
KEYS_PER_WORKER=25

r() { port="$1"; shift; redis-cli -h 127.0.0.1 -p "$port" --no-auth-warning "$@"; }

if [ "$MODE" = "write" ]; then
  i=0
  while [ "$i" -lt "$N" ]; do
    p=$((BASE + i)); j=0
    while [ "$j" -lt "$KEYS_PER_WORKER" ]; do
      # The value records which worker accepted it, so a misplacement after the
      # restart is visible in the data rather than inferred from a count.
      out=$(r "$p" SET "scale:w${i}:k${j}" "written-by-worker-${i}")
      [ "$out" = "OK" ] || { echo "FAIL: SET to worker $i returned '$out'"; exit 1; }
      j=$((j+1))
    done
    echo "worker $i (port $p): wrote $KEYS_PER_WORKER keys"
    i=$((i+1))
  done
  # Durable tier holds the +OK until commit, so returning means it is persisted.
  echo "wrote $((N * KEYS_PER_WORKER)) keys across $N worker(s)"
  exit 0
fi

# verify: every key written by any original worker must come back, and it must
# come back at owner % N.
ORIG="$4"   # worker count the keys were written under
missing=0; misplaced=0; found=0
i=0
while [ "$i" -lt "$ORIG" ]; do
  expect=$((i % N)); p=$((BASE + expect)); j=0
  while [ "$j" -lt "$KEYS_PER_WORKER" ]; do
    v=$(r "$p" GET "scale:w${i}:k${j}" 2>/dev/null || true)
    if [ "$v" = "written-by-worker-${i}" ]; then
      found=$((found+1))
    elif [ -z "$v" ]; then
      # Not where the rule says. Look everywhere before calling it lost, so the
      # failure distinguishes "misplaced" from "gone".
      k=0; seen=""
      while [ "$k" -lt "$N" ]; do
        v2=$(r $((BASE + k)) GET "scale:w${i}:k${j}" 2>/dev/null || true)
        [ -n "$v2" ] && seen="$k"
        k=$((k+1))
      done
      if [ -n "$seen" ]; then
        misplaced=$((misplaced+1))
        [ "$misplaced" -le 3 ] && echo "  MISPLACED scale:w${i}:k${j}: expected worker $expect, found on $seen"
      else
        missing=$((missing+1))
        [ "$missing" -le 3 ] && echo "  MISSING   scale:w${i}:k${j} (expected worker $expect)"
      fi
    fi
    j=$((j+1))
  done
  echo "owner $i -> worker $expect"
  i=$((i+1))
done
total=$((ORIG * KEYS_PER_WORKER))
echo
echo "found $found / $total   misplaced $misplaced   missing $missing"
[ "$missing" -eq 0 ] && [ "$misplaced" -eq 0 ]
