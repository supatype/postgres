#!/usr/bin/env bash
# Container scans: HSCAN (incl. NOVALUES), SSCAN, ZSCAN, with MATCH filtering.
# Each returns the whole collection with next cursor 0. Assumes a daemon on $RESP.
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
# data lines of a SCAN reply (everything after the cursor line)
data() { tail -n +2 | grep -c .; }

echo "# scans"
$K DEL h s z >/dev/null 2>&1

# HSCAN
$K HSET h f1 a f2 b g3 c >/dev/null
chk "HSCAN cursor is 0"           "0"       "$($K HSCAN h 0 | head -1)"
chk "HSCAN returns field+value pairs" "6"   "$($K HSCAN h 0 | data)"
chk "HSCAN NOVALUES -> fields only"   "3"   "$($K HSCAN h 0 NOVALUES | data)"
chk "HSCAN MATCH f* -> 2 fields"  "2"       "$($K HSCAN h 0 MATCH 'f*' NOVALUES | data)"
chk "HSCAN MATCH f* value present" "a"      "$($K HSCAN h 0 MATCH f1 | tail -n +2 | tail -1)"

# SSCAN
$K SADD s apple apricot banana >/dev/null
chk "SSCAN cursor is 0"           "0"       "$($K SSCAN s 0 | head -1)"
chk "SSCAN returns all members"   "3"       "$($K SSCAN s 0 | data)"
chk "SSCAN MATCH ap* -> 2"        "2"       "$($K SSCAN s 0 MATCH 'ap*' | data)"

# ZSCAN
$K ZADD z 1 a 2 b 3 c >/dev/null
chk "ZSCAN cursor is 0"           "0"       "$($K ZSCAN z 0 | head -1)"
chk "ZSCAN returns member+score pairs" "6"  "$($K ZSCAN z 0 | data)"
chk "ZSCAN MATCH a -> member+score" "2"     "$($K ZSCAN z 0 MATCH a | data)"

# WRONGTYPE
$K SET str v >/dev/null
chk "HSCAN on a string -> WRONGTYPE" "1"    "$($K HSCAN str 0 2>&1 | grep -c WRONGTYPE)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
