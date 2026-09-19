#!/bin/sh
# Same scaling property, but for TTL'd keys, which persist to supacache.kv_ttl
# rather than supacache.kv and take a different INSERT path.
set -e
N="$1"; MODE="$2"; ORIG="$4"; BASE=6379; PER=15
r() { p="$1"; shift; redis-cli -h 127.0.0.1 -p "$p" --no-auth-warning "$@"; }
if [ "$MODE" = "write" ]; then
  i=0; while [ "$i" -lt "$N" ]; do
    j=0; while [ "$j" -lt "$PER" ]; do
      out=$(r $((BASE+i)) SET "ttl:w${i}:k${j}" "ttl-by-worker-${i}" EX 3600)
      [ "$out" = "OK" ] || { echo "FAIL: SET EX to worker $i -> '$out'"; exit 1; }
      j=$((j+1)); done
    i=$((i+1)); done
  echo "wrote $((N*PER)) TTL'd keys across $N worker(s)"
  exit 0
fi
missing=0; found=0; nottl=0
i=0; while [ "$i" -lt "$ORIG" ]; do
  expect=$((i % N)); j=0
  while [ "$j" -lt "$PER" ]; do
    v=$(r $((BASE+expect)) GET "ttl:w${i}:k${j}" 2>/dev/null || true)
    if [ "$v" = "ttl-by-worker-${i}" ]; then
      found=$((found+1))
      t=$(r $((BASE+expect)) TTL "ttl:w${i}:k${j}")
      # A TTL that came back as -1 would mean the key recovered but its expiry
      # did not, which is a silent leak rather than a loss.
      [ "$t" -gt 0 ] 2>/dev/null || { nottl=$((nottl+1)); [ "$nottl" -le 3 ] && echo "  NO TTL ttl:w${i}:k${j} -> $t"; }
    else
      missing=$((missing+1)); [ "$missing" -le 3 ] && echo "  MISSING ttl:w${i}:k${j} at worker $expect (got '$v')"
    fi
    j=$((j+1)); done
  i=$((i+1)); done
echo "found $found / $((ORIG*PER))   missing $missing   lost-ttl $nottl"
[ "$missing" -eq 0 ] && [ "$nottl" -eq 0 ]
