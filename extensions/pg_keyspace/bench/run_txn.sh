#!/usr/bin/env bash
# Transactions: MULTI / EXEC / DISCARD / WATCH / UNWATCH. redis-cli run against
# a pipe executes every line on ONE connection, which is how we drive a
# multi-command transaction. Assumes a daemon on $RESP.
set -u
RESP=${RESP:-6381}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  K="redis-cli -p $RESP"
else
  K="redis-cli --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-50s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-50s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# transactions"
$K DEL tx tx2 k k2 >/dev/null 2>&1

# MULTI/EXEC applies the whole queue atomically
out=$(printf 'MULTI\nSET tx 1\nINCR tx\nEXEC\n' | $K)
chk "MULTI/EXEC queued the commands"  "2"   "$(printf '%s\n' "$out" | grep -c QUEUED)"
chk "MULTI/EXEC applied the writes"   "2"   "$($K GET tx)"

# DISCARD throws the queue away
printf 'MULTI\nSET tx2 99\nDISCARD\n' | $K >/dev/null
chk "DISCARD did not apply the writes" "0"  "$($K EXISTS tx2)"

# EXEC without MULTI is an error
chk "EXEC without MULTI -> error"     "1"   "$($K EXEC 2>&1 | grep -ci 'without MULTI')"
chk "DISCARD without MULTI -> error"  "1"   "$($K DISCARD 2>&1 | grep -ci 'without MULTI')"

# WATCH with no intervening change: the transaction runs
$K SET k2 5 >/dev/null
printf 'WATCH k2\nMULTI\nINCR k2\nEXEC\n' | $K >/dev/null
chk "WATCH (no change) -> EXEC applied" "6" "$($K GET k2)"

# WATCH with an intervening change from another connection: EXEC aborts, so the
# queued INCR never runs and the key keeps the interleaved value.
$K SET k 100 >/dev/null
( echo "WATCH k"; echo "MULTI"; echo "INCR k"; sleep 0.4; \
  $K SET k 999 >/dev/null; echo "EXEC" ) | $K >/dev/null
chk "WATCH abort: queued INCR did not run" "999" "$($K GET k)"

# WATCH inside MULTI is rejected
chk "WATCH inside MULTI -> error" "1" "$(printf 'MULTI\nWATCH k\n' | $K 2>&1 | grep -ci 'not allowed')"
$K UNWATCH >/dev/null 2>&1

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
