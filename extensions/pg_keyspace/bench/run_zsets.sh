#!/usr/bin/env bash
# sorted-set type: command coverage, WRONGTYPE semantics, redis parity,
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

echo "# sorted sets"
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
# ---- ZADD GT / LT / INCR ----------------------------------------------------
# Every expectation measured against redis 7.0.15. These three were not
# implemented at all: `ZADD k INCR 5 m` answered "ERR syntax error".
$K DEL fg fl fi fx fp >/dev/null 2>&1

# Incompatible combinations, and the two DIFFERENT messages redis uses for them.
chk "NX+XX is refused" "1" \
    "$($K ZADD fg NX XX 1 m 2>&1 | grep -c 'XX and NX options at the same time')"
chk "GT+NX is refused" "1" \
    "$($K ZADD fg GT NX 1 m 2>&1 | grep -c 'GT, LT, and/or NX options')"
chk "LT+NX likewise"   "1" \
    "$($K ZADD fg LT NX 1 m 2>&1 | grep -c 'GT, LT, and/or NX options')"
chk "GT+LT likewise"   "1" \
    "$($K ZADD fg GT LT 1 m 2>&1 | grep -c 'GT, LT, and/or NX options')"
# GT/LT with XX is legal -- XX is not part of that incompatibility.
chk "GT+XX is accepted"  "0"  "$($K ZADD fg GT XX 1 m)"

# GT raises only. The return stays the ADDED count, so an update answers 0.
$K ZADD fg 5 m >/dev/null
chk "GT with a lower score does nothing" "0" "$($K ZADD fg GT 3 m)"
chk "and the score is unchanged"         "5" "$($K ZSCORE fg m)"
chk "GT with a higher score updates"     "0" "$($K ZADD fg GT 7 m)"
chk "and the score moved"                "7" "$($K ZSCORE fg m)"
chk "GT CH reports the change"           "1" "$($K ZADD fg GT CH 9 m)"
chk "GT CH reports a no-op as 0"         "0" "$($K ZADD fg GT CH 1 m)"

# LT lowers only.
$K ZADD fl 5 m >/dev/null
chk "LT with a higher score does nothing" "0" "$($K ZADD fl LT 9 m)"
chk "and the score is unchanged"          "5" "$($K ZSCORE fl m)"
chk "LT with a lower score updates"       "0" "$($K ZADD fl LT 3 m)"
chk "and the score moved"                 "3" "$($K ZSCORE fl m)"

# A member that is not there yet is ADDED by GT/LT -- there is no old score to
# lose the comparison against.
chk "GT adds a missing member"            "1" "$($K ZADD fx GT 5 new)"
chk "with the given score"                "5" "$($K ZSCORE fx new)"
chk "LT adds a missing member too"        "1" "$($K ZADD fx LT 5 new2)"

# INCR answers the resulting score rather than a count.
chk "INCR on a new member"                "5" "$($K ZADD fi INCR 5 m)"
chk "INCR accumulates"                  "7.5" "$($K ZADD fi INCR 2.5 m)"
chk "and the score is stored"           "7.5" "$($K ZSCORE fi m)"
chk "INCR takes only one pair"            "1" \
    "$($K ZADD fi INCR 1 a 2 b 2>&1 | grep -c 'single increment-element pair')"
# Flags may interleave, in any order.
chk "CH INCR parses"                     "10" "$($K ZADD fi CH INCR 2.5 m)"
chk "INCR NX on a new member"             "3" "$($K ZADD fp INCR NX 3 m)"
# A blocked INCR answers NIL and writes nothing.
chk "NX INCR on an existing member -> nil" ""  "$($K ZADD fp NX INCR 5 m)"
chk "and the score did not move"          "3" "$($K ZSCORE fp m)"
chk "XX INCR on a missing member -> nil"  ""  "$($K ZADD fp XX INCR 5 nope)"
# GT/LT gate the RESULT against the old score.
chk "GT INCR that would lower -> nil"     ""  "$($K ZADD fp GT INCR -1 m)"
chk "GT INCR that raises"                 "8" "$($K ZADD fp GT INCR 5 m)"
chk "LT INCR that would raise -> nil"     ""  "$($K ZADD fp LT INCR 1 m)"
chk "LT INCR that lowers"                 "3" "$($K ZADD fp LT INCR -5 m)"
# ...but on a missing member they add it, INCR or not.
chk "GT INCR adds a missing member"       "5" "$($K ZADD fp GT INCR 5 fresh)"
# An increment to NaN is refused, and the signed zero of the fix above survives
# this path too.
$K ZADD fn inf m >/dev/null
chk "INCR to NaN is refused"              "1" \
    "$($K ZADD fn INCR -inf m 2>&1 | grep -c 'not a number')"
chk "INCR -0 on a new member -> -0"      "-0" "$($K ZADD fn2 GT INCR -0 fresh)"
# A blocked write must not bring the key into existence.
chk "XX on a missing key creates nothing" "0" "$($K ZADD nokey XX 5 m >/dev/null; $K EXISTS nokey)"
chk "XX INCR likewise"                    "0" "$($K ZADD nokey2 XX INCR 5 m >/dev/null; $K EXISTS nokey2)"
$K DEL fg fl fi fx fp fn fn2 nokey nokey2 >/dev/null 2>&1

# ---- signed zero ------------------------------------------------------------
# Every expectation here was measured against redis 7.0.15, not reasoned about.
# The rule is narrower than it looks: redis's d2string prints "-0" for a
# negative zero, but a STORED score never is one — so the sign shows up in the
# ZINCRBY reply and nowhere else.
#
# Two halves, and either alone is a divergence:
#   - format a computed negative zero as "-0"        (d2string)
#   - normalise a stored one, so no READ surfaces it (ZSCORE and friends)
# Teaching the formatter about the sign without normalising on store would fix
# the ZINCRBY reply and break ZSCORE, ZRANGE WITHSCORES, ZMSCORE and ZPOPMIN.
$K DEL nz0 nz1 nz2 nz3 nz4 >/dev/null 2>&1

# A new member takes the increment AS its score: `0.0 + -0.0` is `+0.0` in
# IEEE, so adding to an implicit zero would erase the sign.
chk "ZINCRBY -0 on a new member -> -0"  "-0"  "$($K ZINCRBY nz0 -0 m)"
chk "ZINCRBY -0.0 likewise"             "-0"  "$($K ZINCRBY nz1 -0.0 m)"
# ...but the stored score is normalised, so every read says 0.
chk "ZSCORE of it -> 0, not -0"          "0"  "$($K ZSCORE nz0 m)"
chk "ZRANGE WITHSCORES -> 0"           "m,0"  "$($K ZRANGE nz0 0 -1 WITHSCORES | paste -sd,)"
chk "ZMSCORE -> 0"                       "0"  "$($K ZMSCORE nz0 m)"
chk "ZPOPMIN -> 0"                     "m,0"  "$($K ZPOPMIN nz0 | paste -sd,)"
# A second increment adds to a stored +0, so the sign does not come back.
chk "ZINCRBY -0 again -> 0"              "0"  "$($K ZINCRBY nz1 -0 m)"
# ZADD stores directly, so it never reports the sign in the first place.
chk "ZADD -0 then ZSCORE -> 0"           "0"  "$($K ZADD nz2 -0 m >/dev/null; $K ZSCORE nz2 m)"
# And a zero reached by arithmetic is a positive zero, as it is in redis.
chk "ZINCRBY back to zero -> 0"          "0"  "$($K ZADD nz3 1 m >/dev/null; $K ZINCRBY nz3 -1 m)"
chk "from a negative score -> 0"         "0"  "$($K ZADD nz4 -1 m >/dev/null; $K ZINCRBY nz4 1 m)"
# Ordinary scores are untouched by any of this.
chk "a negative score still prints"   "-1.5"  "$($K ZADD nzf -1.5 m >/dev/null; $K ZSCORE nzf m)"
$K DEL nz0 nz1 nz2 nz3 nz4 nzf >/dev/null 2>&1

if command -v redis-benchmark >/dev/null 2>&1; then
  TLSFLAGS=""; echo "$K" | grep -q tls && TLSFLAGS="--tls --insecure"
  echo "# throughput (redis-benchmark, 100k ZADD across 100k small zsets, pipeline 16):"
  redis-benchmark $TLSFLAGS -p $RESP -n 100000 -P 16 -q -r 100000 ZADD "z:__rand_int__" 1 m 2>/dev/null | sed 's/^/  /'
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
