#!/usr/bin/env bash
# RESP3: HELLO 2/3 negotiation and typed replies (map/set/double), driven by a
# real RESP3 client (redis-cli -3) — if the server emitted malformed RESP3 the
# client would fail to parse and these would error. Assumes a daemon on $RESP.
set -u
RESP=${RESP:-6381}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  R2="redis-cli -p $RESP"; R3="redis-cli -3 -p $RESP"
else
  R2="redis-cli --tls --insecure -p $RESP"; R3="redis-cli -3 --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-48s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-48s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# RESP3"
# The -3 client PINGing at all proves the HELLO 3 handshake map parsed cleanly.
chk "RESP3 client connects (HELLO 3 ok)" "PONG" "$($R3 PING)"
chk "HELLO 3 reports proto 3" "3" "$($R3 HELLO 3 | grep -A1 -iE '^proto$' | tail -1)"
chk "HELLO 2 reports proto 2" "2" "$($R2 HELLO 2 | grep -A1 -iE '^proto$' | tail -1)"
chk "HELLO with a bad version errors" "1" "$($R2 HELLO 4 2>&1 | grep -ci 'NOPROTO')"

$R3 DEL h s z >/dev/null 2>&1

# map (%): HGETALL — values must round-trip through the RESP3 map decode
$R3 HSET h f1 v1 f2 v2 >/dev/null
chk "HGETALL map round-trips (RESP3)"  "f1,f2,v1,v2" "$($R3 HGETALL h | tr -d '"' | sort | paste -sd,)"
chk "HGETALL flat array (RESP2)"       "f1,f2,v1,v2" "$($R2 HGETALL h | sort | paste -sd,)"

# set (~): SMEMBERS / SINTER
$R3 SADD s a b c >/dev/null
chk "SMEMBERS set round-trips (RESP3)" "a,b,c" "$($R3 SMEMBERS s | tr -d '"' | sort | paste -sd,)"

# double (,): ZSCORE / ZMSCORE / ZINCRBY
$R3 ZADD z 1.5 m >/dev/null
chk "ZSCORE double (RESP3)"   "1.5" "$($R3 ZSCORE z m | tr -d '"()a-z ' )"
chk "ZSCORE bulk (RESP2)"     "1.5" "$($R2 ZSCORE z m)"
chk "ZINCRBY double (RESP3)"  "2.5" "$($R3 ZINCRBY z 1 m | tr -d '"()a-z ')"

# null: a miss under RESP3 is still a nil to the client
chk "GET missing -> nil (RESP3)" "" "$($R3 GET nope)"

# ---- client-side caching (CLIENT TRACKING) ----
chk "CLIENT ID returns a number"      "1" "$([ "$($R3 CLIENT ID)" -gt 0 ] 2>/dev/null && echo 1 || echo 0)"
chk "CLIENT TRACKING ON (RESP3) -> OK" "OK" "$($R3 CLIENT TRACKING ON)"
chk "CLIENT TRACKING ON needs RESP3"   "1"  "$($R2 CLIENT TRACKING ON 2>&1 | grep -ci 'RESP3')"

# End-to-end invalidation. Drive one RESP3 connection over a raw socket (so we
# read the exact bytes, including the out-of-band `invalidate` push, without
# depending on redis-cli's push rendering): HELLO 3, enable tracking, read tk;
# then another connection writes tk; the server must push an invalidation naming
# tk on the first connection. Only over a plaintext port (bash has no TLS).
if [ "$R2" = "redis-cli -p $RESP" ]; then
  $R2 SET tk v1 >/dev/null
  inv=""
  if exec 3<>"/dev/tcp/127.0.0.1/$RESP" 2>/dev/null; then
    printf 'HELLO 3\r\nCLIENT TRACKING ON\r\nGET tk\r\n' >&3
    sleep 0.3
    $R2 SET tk v2 >/dev/null   # a different connection changes the tracked key
    inv=$(timeout 1 cat <&3 2>/dev/null || true)
    exec 3<&- 2>/dev/null || true
  fi
  chk "tracking client gets an invalidation" "1" "$(printf '%s' "$inv" | grep -ci invalidate)"
  chk "invalidation names the tracked key"   "1" "$(printf '%s' "$inv" | grep -c 'tk')"
else
  echo "  SKIP  tracking invalidation (TLS port; raw socket unavailable)"
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
