#!/usr/bin/env bash
# Aggregate command gaps: HINCRBYFLOAT/HRANDFIELD, LINSERT/LREM/LPOS/LMOVE/
# RPOPLPUSH, ZPOPMIN/ZPOPMAX/ZRANDMEMBER. Assumes a daemon on $RESP.
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

echo "# aggregate gaps"
$K DEL h l l2 z src dst >/dev/null 2>&1

# HINCRBYFLOAT / HRANDFIELD
chk "HINCRBYFLOAT from missing field" "10.5"  "$($K HINCRBYFLOAT h f 10.5)"
chk "HINCRBYFLOAT add"                "10.6"  "$($K HINCRBYFLOAT h f 0.1)"
$K HSET h a 1 b 2 c 3 >/dev/null
rf=$($K HRANDFIELD h)
chk "HRANDFIELD returns a real field" "1"     "$($K HEXISTS h "$rf")"
chk "HRANDFIELD count 3 distinct"     "3"     "$($K HRANDFIELD h 3 | sort -u | grep -cE '^(a|b|c|f)$')"
chk "HRANDFIELD negative count len"   "5"     "$($K HRANDFIELD h -5 | grep -c .)"
chk "HRANDFIELD WITHVALUES pairs"     "4"     "$($K HRANDFIELD h 2 WITHVALUES | grep -c .)"

# LINSERT / LREM / LPOS
$K DEL l >/dev/null
$K RPUSH l a b c b d b >/dev/null
chk "LINSERT BEFORE returns new len"  "7"     "$($K LINSERT l BEFORE c X)"
chk "LINSERT put X before c"          "X"     "$($K LINDEX l 2)"
chk "LINSERT missing pivot -> -1"     "-1"    "$($K LINSERT l BEFORE zzz Y)"
chk "LPOS first b"                    "1"     "$($K LPOS l b)"
chk "LPOS RANK -1 (last b)"           "6"     "$($K LPOS l b RANK -1)"
chk "LPOS COUNT 0 finds all b"        "3"     "$($K LPOS l b COUNT 0 | grep -c .)"
chk "LREM +2 from head"               "2"     "$($K LREM l 2 b)"
chk "LREM count reduced"              "1"     "$($K LPOS l b COUNT 0 | grep -c .)"

# LMOVE / RPOPLPUSH
$K DEL src dst >/dev/null
$K RPUSH src 1 2 3 >/dev/null
chk "RPOPLPUSH moves the tail"        "3"     "$($K RPOPLPUSH src dst)"
chk "RPOPLPUSH dst head"             "3"     "$($K LINDEX dst 0)"
chk "LMOVE LEFT RIGHT"               "1"     "$($K LMOVE src dst LEFT RIGHT)"
chk "LMOVE dst tail"                "1"     "$($K LINDEX dst -1)"
$K DEL rot >/dev/null; $K RPUSH rot a b c >/dev/null
chk "LMOVE onto itself (rotate)"    "c"     "$($K LMOVE rot rot RIGHT LEFT)"
chk "rotate head is now c"          "c"     "$($K LINDEX rot 0)"

# ZPOPMIN / ZPOPMAX / ZRANDMEMBER
$K DEL z >/dev/null
$K ZADD z 1 a 2 b 3 c 4 d >/dev/null
chk "ZPOPMIN member"                "a"     "$($K ZPOPMIN z | head -1)"
chk "ZPOPMAX member"                "d"     "$($K ZPOPMAX z | head -1)"
chk "ZPOPMIN count 2 -> 4 lines"    "4"     "$($K ZPOPMIN z 2 | grep -c .)"
$K ZADD z 1 a 2 b 3 c >/dev/null
zm=$($K ZRANDMEMBER z)
chk "ZRANDMEMBER real member"       "1"     "$([ -n "$($K ZSCORE z "$zm")" ] && echo 1 || echo 0)"
chk "ZRANDMEMBER count 3 distinct"  "3"     "$($K ZRANDMEMBER z 3 | sort -u | grep -cE '^(a|b|c)$')"
chk "ZRANDMEMBER WITHSCORES pairs"  "4"     "$($K ZRANDMEMBER z 2 WITHSCORES | grep -c .)"

# WRONGTYPE guard
$K SET str v >/dev/null
chk "LINSERT on a string -> WRONGTYPE" "1"   "$($K LINSERT str BEFORE a b 2>&1 | grep -c WRONGTYPE)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
