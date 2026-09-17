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
# redis-cli renders a RESP3 map as one "key value" line (so proto + its value
# share a line) but a RESP2 array as one token per line (value on the next).
chk "HELLO 3 reports proto 3" "3" "$($R3 HELLO 3 | grep -i proto | tr -dc '0-9')"
chk "HELLO 2 reports proto 2" "2" "$($R2 HELLO 2 | grep -A1 -iE '^proto$' | tail -1)"
chk "HELLO with a bad version errors" "1" "$($R2 HELLO 4 2>&1 | grep -ci 'NOPROTO')"

$R3 DEL h s z >/dev/null 2>&1

# map (%): HGETALL — values must round-trip through the RESP3 map decode
$R3 HSET h f1 v1 f2 v2 >/dev/null
# redis-cli prints a RESP3 map as "f1 v1" per line; split on space+newline so
# the same check works whether the reply is a map (pairs) or a flat array.
chk "HGETALL map round-trips (RESP3)"  "f1,f2,v1,v2" "$($R3 HGETALL h | tr -d '"' | tr ' ' '\n' | grep . | sort | paste -sd,)"
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

  # WITHSCORES pair shape (RESP3): ZRANGE … WITHSCORES is an array of
  # [member, double] pairs. Read the raw bytes to confirm the , double type and
  # the nested framing, which redis-cli's rendering would otherwise hide.
  $R2 DEL zz >/dev/null; $R2 ZADD zz 1 a 2 b >/dev/null
  raw=""
  if exec 3<>"/dev/tcp/127.0.0.1/$RESP" 2>/dev/null; then
    printf 'HELLO 3\r\nZRANGE zz 0 -1 WITHSCORES\r\n' >&3
    sleep 0.3
    raw=$(timeout 1 cat <&3 2>/dev/null || true)
    exec 3<&- 2>/dev/null || true
  fi
  chk "ZRANGE WITHSCORES emits RESP3 doubles" "2" "$(printf '%s' "$raw" | grep -c '^,')"
  # one top-level *2 (two pairs) + two per-pair *2 = three; HELLO's map is %7.
  chk "ZRANGE WITHSCORES nests member+score"  "3" "$(printf '%s' "$raw" | grep -c '^\*2')"

  # BCAST: prefix-driven invalidation with no prior read of the key.
  $R2 DEL bc:1 >/dev/null
  inv=""
  if exec 3<>"/dev/tcp/127.0.0.1/$RESP" 2>/dev/null; then
    printf 'HELLO 3\r\nCLIENT TRACKING ON BCAST PREFIX bc:\r\n' >&3
    sleep 0.3
    $R2 SET bc:1 v >/dev/null   # matches the bc: prefix; never read on conn 3
    inv=$(timeout 1 cat <&3 2>/dev/null || true)
    exec 3<&- 2>/dev/null || true
  fi
  chk "BCAST invalidation on prefix write" "1" "$(printf '%s' "$inv" | grep -ci invalidate)"
  chk "BCAST invalidation names the key"   "1" "$(printf '%s' "$inv" | grep -c 'bc:1')"

  # OPTIN: only reads that follow CLIENT CACHING YES are tracked.
  $R2 DEL oi:1 oi:2 >/dev/null; $R2 SET oi:1 a >/dev/null; $R2 SET oi:2 b >/dev/null
  out=""
  if exec 3<>"/dev/tcp/127.0.0.1/$RESP" 2>/dev/null; then
    printf 'HELLO 3\r\nCLIENT TRACKING ON OPTIN\r\nGET oi:1\r\nCLIENT CACHING YES\r\nGET oi:2\r\n' >&3
    sleep 0.3
    $R2 SET oi:1 a2 >/dev/null   # not opted in -> no invalidation
    $R2 SET oi:2 b2 >/dev/null   # opted in    -> invalidation for oi:2
    out=$(timeout 1 cat <&3 2>/dev/null || true)
    exec 3<&- 2>/dev/null || true
  fi
  chk "OPTIN skips the un-opted read"  "0" "$(printf '%s' "$out" | grep -c 'oi:1')"
  chk "OPTIN tracks the opted-in read" "1" "$(printf '%s' "$out" | grep -c 'oi:2')"
else
  echo "  SKIP  tracking invalidation (TLS port; raw socket unavailable)"
fi

# ---- connection state: RESET and CLIENT SETNAME/GETNAME ----------------------
# One connection across several commands, so redis-cli is no use here (it opens
# a new one per invocation) -- these go down a raw socket.
#
# Every expectation below was measured against redis 7.0.15 rather than recalled.
# RESET used to reply +OK and do nothing at all, which mattered most in exactly
# the place it looks harmless: RESET is on the short allowlist of commands
# accepted in RESP2 subscribe mode, because it is the documented way OUT of it.
raw() { # reads commands on stdin, returns the multiplexed replies
  if echo "$R2" | grep -q tls; then
    { cat; sleep 0.5; } | timeout 5 openssl s_client -quiet -connect 127.0.0.1:$RESP 2>/dev/null
  else
    { cat; sleep 0.5; } | timeout 5 bash -c "exec 3<>/dev/tcp/127.0.0.1/$RESP; cat >&3; cat <&3"
  fi
}

out="$(printf 'CLIENT GETNAME\r\nCLIENT SETNAME worker-7\r\nCLIENT GETNAME\r\n' | raw)"
chk "CLIENT GETNAME unset is a null, not an empty string" "1" "$(echo "$out" | grep -c '^\$-1')"
chk "CLIENT SETNAME then GETNAME round-trips" "1" "$(echo "$out" | grep -c 'worker-7')"

out="$(printf 'HELLO 3\r\nCLIENT GETNAME\r\n' | raw)"
chk "CLIENT GETNAME unset is RESP3 null under HELLO 3" "1" "$(echo "$out" | grep -c '^_')"

# Multibulk, not inline: the inline protocol splits on whitespace, so
# `CLIENT SETNAME a b` arrives as the perfectly legal name "a" and proves
# nothing. A name with a space or a newline in it can only be sent as a frame.
out="$(printf '*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$3\r\na b\r\n' | raw)"
chk "CLIENT SETNAME refuses a name with a space" "1" \
    "$(echo "$out" | grep -c 'cannot contain spaces')"
out="$(printf '*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$3\r\na\nb\r\n' | raw)"
chk "CLIENT SETNAME refuses a name with a newline" "1" \
    "$(echo "$out" | grep -c 'cannot contain spaces')"
# An empty name is legal, and means "unset".
out="$(printf '*3\r\n$6\r\nCLIENT\r\n$7\r\nSETNAME\r\n$0\r\n\r\nCLIENT GETNAME\r\n' | raw)"
chk "CLIENT SETNAME '' is accepted and unsets" "1" "$(echo "$out" | grep -c '^\$-1')"

out="$(printf 'RESET\r\n' | raw)"
chk "RESET replies +RESET, not +OK" "1" "$(echo "$out" | grep -c '^+RESET')"

# The one that matters: subscribe mode is a state you must be able to leave.
out="$(printf 'SUBSCRIBE ch\r\nGET nokey\r\nRESET\r\nGET nokey\r\n' | raw)"
chk "a keyed command is refused while subscribed" "1" \
    "$(echo "$out" | grep -c "allowed in this context")"
chk "RESET exits subscribe mode" "1" "$(echo "$out" | grep -c '^\$-1')"

out="$(printf 'MULTI\r\nSET k v\r\nRESET\r\nEXEC\r\n' | raw)"
chk "RESET inside MULTI runs rather than queueing" "1" "$(echo "$out" | grep -c '^+RESET')"
chk "and discards the transaction" "1" "$(echo "$out" | grep -c 'EXEC without MULTI')"

out="$(printf 'CLIENT SETNAME gone\r\nRESET\r\nCLIENT GETNAME\r\n' | raw)"
chk "RESET clears the connection name" "1" "$(echo "$out" | grep -c '^\$-1')"

# RESET undoes HELLO 3, so a miss must come back as $-1 and not _.
out="$(printf 'HELLO 3\r\nGET nokey\r\nRESET\r\nGET nokey\r\n' | raw)"
chk "RESET drops RESP3 back to RESP2" "1" "$(echo "$out" | grep -c '^\$-1')"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
