#!/usr/bin/env bash
# Sorted-set set operations: ZUNION/ZINTER/ZDIFF (+STORE, WEIGHTS/AGGREGATE),
# ZMPOP, ZLEXCOUNT, ZRANGEBYLEX/ZREVRANGEBYLEX, ZRANGESTORE. Daemon on $RESP.
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
csv() { $K "$@" | paste -sd,; }

echo "# zset set-ops"
$K DEL z1 z2 dst zl zm >/dev/null 2>&1
$K ZADD z1 1 a 2 b 3 c >/dev/null
$K ZADD z2 4 b 5 c 6 d >/dev/null

# UNION / INTER / DIFF (non-store)
chk "ZUNION members (score order)" "a,b,d,c" "$(csv ZUNION 2 z1 z2)"
chk "ZINTER members"               "b,c"     "$(csv ZINTER 2 z1 z2)"
chk "ZDIFF members"                "a"       "$(csv ZDIFF 2 z1 z2)"
chk "ZUNION WITHSCORES"            "a,1,b,6,d,6,c,8" "$(csv ZUNION 2 z1 z2 WITHSCORES)"

# STORE variants + SUM/WEIGHTS/AGGREGATE
chk "ZUNIONSTORE -> count"         "4"       "$($K ZUNIONSTORE dst 2 z1 z2)"
chk "ZUNIONSTORE SUM b=6"          "6"       "$($K ZSCORE dst b)"
chk "ZINTERSTORE -> count"         "2"       "$($K ZINTERSTORE dst 2 z1 z2)"
chk "ZINTERSTORE SUM c=8"          "8"       "$($K ZSCORE dst c)"
chk "ZDIFFSTORE -> count"          "1"       "$($K ZDIFFSTORE dst 2 z1 z2)"
chk "ZDIFFSTORE kept a"            "a"       "$(csv ZRANGE dst 0 -1)"
$K ZUNIONSTORE dst 2 z1 z2 WEIGHTS 2 3 >/dev/null
chk "ZUNIONSTORE WEIGHTS 2 3 b=16" "16"      "$($K ZSCORE dst b)"
$K ZUNIONSTORE dst 2 z1 z2 AGGREGATE MAX >/dev/null
chk "ZUNIONSTORE AGGREGATE MAX c=5" "5"      "$($K ZSCORE dst c)"

# ZMPOP (check side effects; the reply is a nested array)
$K DEL zm >/dev/null; $K ZADD zm 1 a 2 b 3 c >/dev/null
$K ZMPOP 1 zm MIN >/dev/null                 # pops the min (a)
chk "ZMPOP MIN removed the min"    ""        "$($K ZSCORE zm a)"
chk "ZMPOP MIN left 2"             "2"       "$($K ZCARD zm)"
$K ZMPOP 1 zm MAX COUNT 2 >/dev/null         # pops c and b
chk "ZMPOP MAX COUNT 2 emptied it" "0"       "$($K ZCARD zm)"
chk "ZMPOP on all-empty -> nil"    ""        "$($K ZMPOP 1 zm MIN)"

# lexicographic (equal scores)
$K DEL zl >/dev/null; $K ZADD zl 0 a 0 b 0 c 0 d >/dev/null
chk "ZLEXCOUNT - +"                "4"       "$($K ZLEXCOUNT zl - +)"
chk "ZLEXCOUNT [b [c"              "2"       "$($K ZLEXCOUNT zl '[b' '[c')"
chk "ZLEXCOUNT (a +"               "3"       "$($K ZLEXCOUNT zl '(a' +)"
chk "ZRANGEBYLEX - +"              "a,b,c,d" "$(csv ZRANGEBYLEX zl - +)"
chk "ZRANGEBYLEX [b (d"            "b,c"     "$(csv ZRANGEBYLEX zl '[b' '(d')"
chk "ZREVRANGEBYLEX + -"           "d,c,b,a" "$(csv ZREVRANGEBYLEX zl + -)"
chk "ZRANGEBYLEX LIMIT 1 2"        "b,c"     "$(csv ZRANGEBYLEX zl - + LIMIT 1 2)"

# ZRANGESTORE
$K DEL dst >/dev/null
chk "ZRANGESTORE whole -> count"   "3"       "$($K ZRANGESTORE dst z1 0 -1)"
chk "ZRANGESTORE copied a,b,c"     "a,b,c"   "$(csv ZRANGE dst 0 -1)"
chk "ZRANGESTORE 0 1 -> count"     "2"       "$($K ZRANGESTORE dst z1 0 1)"
chk "ZRANGESTORE BYSCORE 1 2"      "2"       "$($K ZRANGESTORE dst z1 1 2 BYSCORE)"
chk "ZRANGESTORE empty deletes dst" "0"      "$($K ZRANGESTORE dst z1 5 10 BYSCORE)"
chk "dst gone after empty store"   "0"       "$($K EXISTS dst)"

# WRONGTYPE
$K SET str v >/dev/null
chk "ZUNIONSTORE over a string -> WRONGTYPE" "1" "$($K ZUNIONSTORE dst 1 str 2>&1 | grep -c WRONGTYPE)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
