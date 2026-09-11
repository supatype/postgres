#!/usr/bin/env bash
# String command coverage: MGET/MSET/MSETNX, SETEX/PSETEX, GETEX, GETDEL,
# APPEND, STRLEN, GETRANGE, SETRANGE, INCRBYFLOAT. Assumes a daemon on $RESP.
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

echo "# strings"
$K DEL a b c h m1 m2 m3 s r f >/dev/null 2>&1

# MSET / MGET
chk "MSET -> OK"                    "OK"      "$($K MSET m1 x m2 y m3 z)"
chk "MGET m1 m2 m3"                 "x,y,z"   "$($K MGET m1 m2 m3 | paste -sd,)"
chk "MGET with a missing key"      "x,,z"    "$($K MGET m1 nope m3 | paste -sd,)"
$K HSET h f 1 >/dev/null
chk "MGET on a hash key -> nil (no WRONGTYPE)" "x," "$($K MGET m1 h | paste -sd,)"

# MSETNX all-or-nothing
chk "MSETNX all new -> 1"          "1"       "$($K MSETNX n1 a n2 b)"
chk "MSETNX with one existing -> 0" "0"      "$($K MSETNX n1 z n3 c)"
chk "MSETNX did not set n3"         "0"       "$($K EXISTS n3)"

# SET options. These are flags, not pairs — an option parser that stepped two
# arguments at a time never saw them, so KEEPTTL silently dropped the TTL and
# NX/XX silently did nothing while still replying OK.
$K DEL so >/dev/null 2>&1
$K SET so v1 EX 300 >/dev/null
chk "SET EX sets a TTL"                        "300"  "$($K TTL so)"
chk "plain SET clears the TTL"                 "-1"   "$($K SET so v2 >/dev/null; $K TTL so)"
$K SET so v3 EX 300 >/dev/null
chk "SET KEEPTTL retains the TTL"              "300"  "$($K SET so v4 KEEPTTL >/dev/null; $K TTL so)"
chk "SET KEEPTTL still wrote the value"        "v4"   "$($K GET so)"
chk "SET EX overrides KEEPTTL"                 "100"  "$($K SET so v5 KEEPTTL EX 100 >/dev/null; $K TTL so)"
chk "SET NX on an existing key -> nil"         ""     "$($K SET so v6 NX)"
chk "SET NX did not overwrite"                 "v5"   "$($K GET so)"
$K DEL so2 >/dev/null 2>&1
chk "SET NX on a missing key -> OK"            "OK"   "$($K SET so2 fresh NX)"
chk "SET XX on a missing key -> nil"           ""     "$($K SET so3 nope XX)"
chk "SET XX did not create the key"            "0"    "$($K EXISTS so3)"
chk "SET XX on an existing key -> OK"          "OK"   "$($K SET so2 replaced XX)"
chk "SET GET returns the previous value"       "replaced" "$($K SET so2 next GET)"
chk "SET GET on a missing key -> nil"          ""     "$($K SET so4 v GET)"
chk "SET NX GET reports old, does not write"   "next" "$($K SET so2 blocked NX GET)"
chk "SET NX GET left the value alone"          "next" "$($K GET so2)"
chk "SET EXAT sets an absolute deadline"       "ok"   "$(t=$($K SET so2 v EXAT $(( $(date +%s) + 200 )) >/dev/null; $K TTL so2); [ "$t" -gt 150 ] && [ "$t" -le 200 ] && echo ok || echo "got $t")"
chk "SET with an unknown option -> error"      "ERR"  "$($K SET so2 v BOGUS 2>&1 | grep -o ERR | head -1)"
chk "SET NX XX together -> error"              "ERR"  "$($K SET so2 v NX XX 2>&1 | grep -o ERR | head -1)"
$K DEL so so2 so3 so4 >/dev/null 2>&1

# SETEX / PSETEX + TTL
chk "SETEX -> OK"                  "OK"      "$($K SETEX s 100 hi)"
t=$($K TTL s); [ "$t" -ge 90 ] && [ "$t" -le 100 ] && { echo "  PASS  SETEX sets a ~100s TTL ($t)"; pass=$((pass+1)); } || { echo "  FAIL  SETEX TTL got=$t"; fail=$((fail+1)); }
chk "SETEX with 0 seconds -> error" "1"      "$($K SETEX s 0 hi 2>&1 | grep -ci 'invalid expire')"
chk "PSETEX -> OK"                 "OK"      "$($K PSETEX s 60000 hi)"

# GETDEL
$K SET a hello >/dev/null
chk "GETDEL returns the value"     "hello"   "$($K GETDEL a)"
chk "GETDEL removed the key"       "0"       "$($K EXISTS a)"
chk "GETDEL on missing -> nil"     ""        "$($K GETDEL a)"

# GETEX (value + TTL side effects)
$K SET b world >/dev/null
chk "GETEX returns the value"      "world"   "$($K GETEX b EX 50)"
t=$($K TTL b); [ "$t" -ge 45 ] && [ "$t" -le 50 ] && { echo "  PASS  GETEX EX set a TTL ($t)"; pass=$((pass+1)); } || { echo "  FAIL  GETEX TTL got=$t"; fail=$((fail+1)); }
chk "GETEX PERSIST clears the TTL" "world"   "$($K GETEX b PERSIST)"
chk "TTL after GETEX PERSIST -> -1" "-1"     "$($K TTL b)"

# APPEND / STRLEN
$K DEL c >/dev/null
chk "APPEND to new key -> len"     "5"       "$($K APPEND c hello)"
chk "APPEND extends -> len"        "11"      "$($K APPEND c ' world')"
chk "STRLEN"                       "11"      "$($K STRLEN c)"
chk "STRLEN missing -> 0"          "0"       "$($K STRLEN nope)"
chk "APPEND on a hash -> WRONGTYPE" "1"      "$($K APPEND h x 2>&1 | grep -c WRONGTYPE)"

# GETRANGE (inclusive, negative indices)
$K SET r "Hello World" >/dev/null
chk "GETRANGE 0 4"                 "Hello"   "$($K GETRANGE r 0 4)"
chk "GETRANGE -5 -1"               "World"   "$($K GETRANGE r -5 -1)"
chk "GETRANGE 0 -1 (whole)"        "Hello World" "$($K GETRANGE r 0 -1)"

# SETRANGE (overwrite + zero-pad)
$K SET r "Hello World" >/dev/null
chk "SETRANGE overwrite -> len"    "11"      "$($K SETRANGE r 6 Redis)"
chk "SETRANGE result"              "Hello Redis" "$($K GET r)"

# INCRBYFLOAT
$K DEL f >/dev/null
chk "INCRBYFLOAT from missing"     "10.5"    "$($K INCRBYFLOAT f 10.5)"
chk "INCRBYFLOAT add"              "10.6"    "$($K INCRBYFLOAT f 0.1)"
chk "INCRBYFLOAT negative"         "5.6"     "$($K INCRBYFLOAT f -5)"
$K SET f notanumber >/dev/null
chk "INCRBYFLOAT on non-float -> error" "1"  "$($K INCRBYFLOAT f 1 2>&1 | grep -ci 'not a valid float')"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
