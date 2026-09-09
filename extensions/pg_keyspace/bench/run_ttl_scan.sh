#!/usr/bin/env bash
# TTL / EXPIRE family + SCAN / KEYS command coverage. (RESP3 / HELLO
# negotiation lives in run_resp3.sh.)
# Assumes a daemon is already listening on $RESP (started by the harness);
# works whether that port is plaintext or TLS.
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
inrange() { # desc lo hi val
  if [ "$4" -ge "$2" ] && [ "$4" -le "$3" ] 2>/dev/null; then
    printf "  PASS  %-50s (%s)\n" "$1" "$4"; pass=$((pass+1));
  else printf "  FAIL  %-50s got=[%s] want[%s,%s]\n" "$1" "$4" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# TTL / EXPIRE / SCAN / KEYS"
$K DEL k k2 tmp nope >/dev/null 2>&1

chk "TTL of a missing key -> -2"         "-2" "$($K TTL k)"
$K SET k v >/dev/null
chk "TTL of a no-expiry key -> -1"       "-1" "$($K TTL k)"

$K SET k v EX 100 >/dev/null
inrange "TTL after SET EX 100"           90 100 "$($K TTL k)"
inrange "PTTL after SET EX 100"          1 100000 "$(($($K PTTL k) / 1000))"

chk "PERSIST removes the TTL -> 1"       "1"  "$($K PERSIST k)"
chk "TTL after PERSIST -> -1"            "-1" "$($K TTL k)"
chk "PERSIST with no TTL -> 0"           "0"  "$($K PERSIST k)"

chk "EXPIRE on existing key -> 1"        "1"  "$($K EXPIRE k 50)"
inrange "TTL after EXPIRE 50"            45 50 "$($K TTL k)"
chk "EXPIRE on a missing key -> 0"       "0"  "$($K EXPIRE nope 50)"
chk "PEXPIRE on existing key -> 1"       "1"  "$($K PEXPIRE k 60000)"

$K SET tmp v >/dev/null
chk "EXPIREAT in the past -> 1"          "1"  "$($K EXPIREAT tmp 1)"
chk "key gone after past EXPIREAT"       "0"  "$($K EXISTS tmp)"
$K SET k2 v >/dev/null
chk "EXPIREAT far future -> 1"           "1"  "$($K EXPIREAT k2 9999999999)"
inrange "TTL after EXPIREAT future"      1 9999999999 "$($K TTL k2)"

# ---- SCAN / KEYS ----
for i in 1 2 3; do $K SET "scan:test:$i" v >/dev/null; done
cur=0; found=""
while :; do
  out=$($K SCAN "$cur" MATCH 'scan:test:*' COUNT 100)
  cur=$(printf '%s\n' "$out" | head -1)
  found="$found $(printf '%s\n' "$out" | tail -n +2)"
  [ "$cur" = "0" ] && break
done
chk "SCAN MATCH finds all 3 keys"        "3"  "$(printf '%s\n' $found | grep -c '^scan:test:')"
chk "KEYS pattern finds all 3 keys"      "3"  "$($K KEYS 'scan:test:*' | grep -c '^scan:test:')"

# (HELLO / RESP3 negotiation is covered by run_resp3.sh — HELLO is now a real
# command that negotiates RESP2/RESP3, not an unknown-command fallback.)

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
