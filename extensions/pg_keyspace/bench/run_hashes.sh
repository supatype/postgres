#!/usr/bin/env bash
# hash type: command coverage, WRONGTYPE semantics, redis parity, and a
# throughput sample. Works whether the RESP port is plaintext or TLS.
set -u
RESP=${RESP:-6381}
CERT=${CERT:-/tmp/pgks_tls/cert.pem}
# auto-detect TLS: if a plaintext PING fails but a TLS one works, use --tls
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  K="redis-cli -p $RESP"
else
  K="redis-cli --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-46s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-46s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# hashes"
$K DEL h str >/dev/null 2>&1

chk "HSET returns new-field count"     "3"  "$($K HSET h a 1 b 2 c 3)"
chk "HSET counts only new fields"      "1"  "$($K HSET h a 10 d 4)"
chk "HGET existing"                    "10" "$($K HGET h a)"
chk "HGET missing field -> nil"        ""   "$($K HGET h zzz)"
chk "HLEN"                             "4"  "$($K HLEN h)"
chk "HEXISTS present"                  "1"  "$($K HEXISTS h b)"
chk "HEXISTS absent"                   "0"  "$($K HEXISTS h zzz)"
chk "HMGET a zzz c"                    "10,,3" "$($K HMGET h a zzz c | paste -sd,)"
chk "HSETNX on existing -> 0"          "0"  "$($K HSETNX h a 99)"
chk "HSETNX on new -> 1"               "1"  "$($K HSETNX h e 5)"
chk "HINCRBY new value"               "7"  "$($K HINCRBY h b 5)"
chk "HSTRLEN"                          "2"  "$($K HINCRBY h b 3 >/dev/null; $K HSTRLEN h b)"  # b=10 -> len 2
chk "HDEL removes present+absent -> 1" "1"  "$($K HDEL h c zzz)"
chk "HKEYS (sorted)"                   "a,b,d,e" "$($K HKEYS h | sort | paste -sd,)"
chk "TYPE hash"                        "hash" "$($K TYPE h)"
chk "TYPE none"                        "none" "$($K TYPE nope)"

# empty hash is dropped (Redis semantics)
$K DEL e1 >/dev/null 2>&1; $K HSET e1 only 1 >/dev/null
chk "emptying a hash deletes the key"  "0"   "$($K HDEL e1 only >/dev/null; $K EXISTS e1)"

# WRONGTYPE both directions
$K SET str hello >/dev/null
chk "HGET on a string -> WRONGTYPE"    "1"  "$($K HGET str f 2>&1 | grep -c WRONGTYPE)"
chk "GET on a hash -> WRONGTYPE"       "1"  "$($K GET h 2>&1 | grep -c WRONGTYPE)"
chk "INCR on a hash -> WRONGTYPE"      "1"  "$($K INCR h 2>&1 | grep -c WRONGTYPE)"

# binary-safe fields/values
$K DEL hb >/dev/null 2>&1
$K HSET hb $'\x01\x02' $'\xaa\xbb' >/dev/null
chk "binary field/value round-trips"   "1"  "$($K HEXISTS hb $'\x01\x02')"

# parity spot-check against a real redis on :6379 if present
if redis-cli -p 6379 PING 2>/dev/null | grep -q PONG; then
  redis-cli -p 6379 DEL hp >/dev/null 2>&1; $K DEL hp >/dev/null 2>&1
  redis-cli -p 6379 HSET hp x 1 y 2 z 3 >/dev/null; redis-cli -p 6379 HINCRBY hp x 9 >/dev/null; redis-cli -p 6379 HDEL hp y >/dev/null
  $K HSET hp x 1 y 2 z 3 >/dev/null; $K HINCRBY hp x 9 >/dev/null; $K HDEL hp y >/dev/null
  chk "HGETALL matches real redis" \
      "$(redis-cli -p 6379 HGETALL hp | paste -sd,)" "$($K HGETALL hp | paste -sd,)"
fi

echo
# throughput sample (pipelined) via redis-benchmark if available
if command -v redis-benchmark >/dev/null 2>&1; then
  TLSFLAGS=""; echo "$K" | grep -q tls && TLSFLAGS="--tls --insecure"
  echo "# throughput (redis-benchmark, 100k ops, pipeline 16):"
  redis-benchmark $TLSFLAGS -p $RESP -n 100000 -P 16 -q -t hset 2>/dev/null | sed 's/^/  /'
  redis-benchmark $TLSFLAGS -p $RESP -n 100000 -P 16 -q -r 10000 hget h a 2>/dev/null | sed 's/^/  /' || true
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
