#!/usr/bin/env bash
# §5 — sorted-set type: command coverage, WRONGTYPE semantics, redis parity,
# and a throughput sample. Works whether the RESP port is plaintext or TLS.
# Scores here are integers/exactly-representable, so formatting matches redis.
set -u
RESP=${RESP:-6381}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  K="redis-cli -p $RESP"
else
  K="redis-cli --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-46s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-46s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# sorted sets (§5)"
$K DEL z str >/dev/null 2>&1

chk "ZADD 4 members"               "4"          "$($K ZADD z 1 a 2 b 3 c 2 d)"
chk "ZADD existing -> 0 added"     "0"          "$($K ZADD z 9 a)"
chk "ZADD CH counts changes"       "1"          "$($K ZADD z CH 5 a)"
chk "ZADD NX returns 0 (existing skipped)"  "0"  "$($K ZADD z NX 100 a)"
chk "ZSCORE after NX still 5"      "5"          "$($K ZSCORE z a)"
chk "ZADD XX returns 0 (new skipped)"       "0"  "$($K ZADD z XX 1 newbie)"
chk "XX did not add the new member -> nil"  ""   "$($K ZSCORE z newbie)"
chk "ZCARD"                        "4"          "$($K ZCARD z)"
chk "ZSCORE c"                     "3"          "$($K ZSCORE z c)"
chk "ZSCORE missing -> nil"        ""           "$($K ZSCORE z zzz)"
chk "ZRANGE 0 -1"                  "b,d,c,a"    "$($K ZRANGE z 0 -1 | paste -sd,)"
chk "ZRANGE WITHSCORES"            "b,2,d,2,c,3,a,5" "$($K ZRANGE z 0 -1 WITHSCORES | paste -sd,)"
chk "ZREVRANGE 0 1"               "a,c"        "$($K ZREVRANGE z 0 1 | paste -sd,)"
chk "ZRANK b / d (tie by member)"  "0,1"        "$($K ZRANK z b),$($K ZRANK z d)"
chk "ZREVRANK a"                   "0"          "$($K ZREVRANK z a)"
chk "ZRANGEBYSCORE 2 3"            "b,d,c"      "$($K ZRANGEBYSCORE z 2 3 | paste -sd,)"
chk "ZRANGEBYSCORE (2 +inf"        "c,a"        "$($K ZRANGEBYSCORE z '(2' +inf | paste -sd,)"
chk "ZRANGEBYSCORE LIMIT 1 2"      "d,c"        "$($K ZRANGEBYSCORE z 2 5 LIMIT 1 2 | paste -sd,)"
chk "ZREVRANGEBYSCORE +inf 2"      "a,c,d,b"    "$($K ZREVRANGEBYSCORE z +inf 2 | paste -sd,)"
chk "ZCOUNT 2 5"                   "4"          "$($K ZCOUNT z 2 5)"
chk "ZCOUNT (2 5"                  "2"          "$($K ZCOUNT z '(2' 5)"
chk "ZINCRBY 10 d -> 12"           "12"         "$($K ZINCRBY z 10 d)"
chk "ZMSCORE a d zzz"              "5,12,"      "$($K ZMSCORE z a d zzz | paste -sd,)"
chk "ZREM a b -> 2"                "2"          "$($K ZREM z a b)"
chk "TYPE zset"                    "zset"       "$($K TYPE z)"
$K DEL z1 >/dev/null 2>&1; $K ZADD z1 1 only >/dev/null
chk "emptied zset deletes key"     "0"          "$($K ZREM z1 only >/dev/null; $K EXISTS z1)"

# WRONGTYPE both directions
$K SET str hi >/dev/null
chk "ZADD on a string -> WRONGTYPE"   "1"  "$($K ZADD str 1 a 2>&1 | grep -c WRONGTYPE)"
$K DEL zt >/dev/null 2>&1; $K ZADD zt 1 a >/dev/null
chk "GET on a zset -> WRONGTYPE"      "1"  "$($K GET zt 2>&1 | grep -c WRONGTYPE)"

# parity spot-check against a real redis on :6379 if present
if redis-cli -p 6379 PING 2>/dev/null | grep -q PONG; then
  redis-cli -p 6379 DEL zp >/dev/null 2>&1; $K DEL zp >/dev/null 2>&1
  for c in "ZADD zp 1 a 2 b 3 c 2 d" "ZADD zp CH 5 a" "ZINCRBY zp 4 c" "ZREM zp b"; do
    redis-cli -p 6379 $c >/dev/null 2>&1; $K $c >/dev/null 2>&1
  done
  chk "ZRANGE WITHSCORES matches real redis" \
      "$(redis-cli -p 6379 ZRANGE zp 0 -1 WITHSCORES | paste -sd,)" \
      "$($K ZRANGE zp 0 -1 WITHSCORES | paste -sd,)"
fi

echo
if command -v redis-benchmark >/dev/null 2>&1; then
  TLSFLAGS=""; echo "$K" | grep -q tls && TLSFLAGS="--tls --insecure"
  echo "# throughput (redis-benchmark, 100k ZADD across 100k small zsets, pipeline 16):"
  redis-benchmark $TLSFLAGS -p $RESP -n 100000 -P 16 -q -r 100000 ZADD "z:__rand_int__" 1 m 2>/dev/null | sed 's/^/  /'
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
