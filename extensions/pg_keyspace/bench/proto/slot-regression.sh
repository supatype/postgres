#!/bin/sh
# Default path regression: slot addressing with a CLUSTER-AWARE client
# (redis-cli -c follows MOVED). Must behave exactly as it did before the
# owner-addressing change, which for this scheme means every key is readable
# from any entry port because the client follows the redirect.
set -e
MODE="$1"; N=40
c() { redis-cli -c -h 127.0.0.1 -p 6379 --no-auth-warning "$@"; }
if [ "$MODE" = "write" ]; then
  i=0; while [ "$i" -lt "$N" ]; do
    out=$(c SET "slotreg:k${i}" "v${i}")
    [ "$out" = "OK" ] || { echo "FAIL: SET slotreg:k${i} -> '$out'"; exit 1; }
    i=$((i+1)); done
  echo "wrote $N keys through a cluster-aware client"
  exit 0
fi
found=0; missing=0
i=0; while [ "$i" -lt "$N" ]; do
  v=$(c GET "slotreg:k${i}" 2>/dev/null || true)
  if [ "$v" = "v${i}" ]; then found=$((found+1)); else
    missing=$((missing+1)); [ "$missing" -le 3 ] && echo "  MISSING slotreg:k${i} (got '$v')"; fi
  i=$((i+1)); done
echo "found $found / $N   missing $missing"
[ "$missing" -eq 0 ]
