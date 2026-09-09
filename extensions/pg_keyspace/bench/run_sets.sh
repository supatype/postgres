#!/usr/bin/env bash
# Set type: SADD/SREM/SCARD/SISMEMBER/SMISMEMBER/SMEMBERS/SPOP/SRANDMEMBER/
# SMOVE and the SUNION/SINTER/SDIFF (+STORE) family. Assumes a daemon on $RESP.
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
# sorted, comma-joined members (SMEMBERS/S*STORE order is unspecified)
sj() { $K "$@" | sort | paste -sd,; }

echo "# sets"
$K DEL s a b c d dst str >/dev/null 2>&1

# SADD / SCARD / SISMEMBER
chk "SADD 3 new -> 3"              "3"       "$($K SADD s x y z)"
chk "SADD with a dup -> 1"        "1"       "$($K SADD s x w)"
chk "SCARD"                       "4"       "$($K SCARD s)"
chk "SISMEMBER present -> 1"      "1"       "$($K SISMEMBER s x)"
chk "SISMEMBER absent -> 0"       "0"       "$($K SISMEMBER s nope)"
chk "SMISMEMBER x nope z"         "1,0,1"   "$($K SMISMEMBER s x nope z | paste -sd,)"
chk "SMEMBERS"                    "w,x,y,z" "$(sj SMEMBERS s)"

# SREM
chk "SREM present+absent -> 1"    "1"       "$($K SREM s x nope)"
chk "SCARD after SREM"            "3"       "$($K SCARD s)"

# SPOP / SRANDMEMBER
$K DEL s >/dev/null; $K SADD s a b c d e >/dev/null
sp=$($K SPOP s)
chk "SPOP returns a former member" "0"      "$($K SISMEMBER s "$sp")"
chk "SPOP shrank the set -> 4"    "4"       "$($K SCARD s)"
chk "SPOP count 2 -> 2 lines"     "2"       "$($K SPOP s 2 | grep -c .)"
chk "SPOP left 2"                 "2"       "$($K SCARD s)"
sr=$($K SRANDMEMBER s)
chk "SRANDMEMBER does not remove"  "1"      "$($K SISMEMBER s "$sr")"
chk "SRANDMEMBER -5 allows repeats" "5"     "$($K SRANDMEMBER s -5 | grep -c .)"

# SMOVE
$K DEL a b >/dev/null; $K SADD a 1 2 3 >/dev/null; $K SADD b 9 >/dev/null
chk "SMOVE existing member -> 1"  "1"       "$($K SMOVE a b 2)"
chk "SMOVE removed from source"   "0"       "$($K SISMEMBER a 2)"
chk "SMOVE added to destination"  "1"       "$($K SISMEMBER b 2)"
chk "SMOVE missing member -> 0"   "0"       "$($K SMOVE a b 42)"

# SUNION / SINTER / SDIFF
$K DEL a b >/dev/null; $K SADD a 1 2 3 4 >/dev/null; $K SADD b 3 4 5 6 >/dev/null
chk "SUNION"                      "1,2,3,4,5,6" "$(sj SUNION a b)"
chk "SINTER"                      "3,4"     "$(sj SINTER a b)"
chk "SDIFF a\\b"                  "1,2"     "$(sj SDIFF a b)"

# SUNIONSTORE / SINTERSTORE / SDIFFSTORE (the MAU aggregation shape)
chk "SUNIONSTORE -> count"        "6"       "$($K SUNIONSTORE dst a b)"
chk "SUNIONSTORE stored the union" "1,2,3,4,5,6" "$(sj SMEMBERS dst)"
chk "SINTERSTORE -> count"        "2"       "$($K SINTERSTORE dst a b)"
chk "SINTERSTORE stored value"    "3,4"     "$(sj SMEMBERS dst)"
chk "SDIFFSTORE -> count"         "2"       "$($K SDIFFSTORE dst a b)"
chk "SDIFFSTORE stored value"     "1,2"     "$(sj SMEMBERS dst)"
$K DEL a b >/dev/null; $K SADD a 1 >/dev/null; $K SADD b 2 >/dev/null
chk "empty SINTERSTORE deletes dst" "0"     "$($K SINTERSTORE dst a b)"
chk "dst gone after empty store"  "0"       "$($K EXISTS dst)"

# SINTERCARD
$K DEL a b >/dev/null; $K SADD a 1 2 3 4 >/dev/null; $K SADD b 3 4 5 6 >/dev/null
chk "SINTERCARD full"             "2"       "$($K SINTERCARD 2 a b)"
chk "SINTERCARD LIMIT 1"          "1"       "$($K SINTERCARD 2 a b LIMIT 1)"
chk "SINTERCARD LIMIT 0 = no cap" "2"       "$($K SINTERCARD 2 a b LIMIT 0)"

# TYPE + WRONGTYPE
$K DEL s >/dev/null; $K SADD s a >/dev/null
chk "TYPE reports set"            "set"     "$($K TYPE s)"
chk "OBJECT ENCODING set"         "hashtable" "$($K OBJECT ENCODING s)"
$K SET str v >/dev/null
chk "SADD on a string -> WRONGTYPE" "1"     "$($K SADD str x 2>&1 | grep -c WRONGTYPE)"
chk "GET on a set -> WRONGTYPE"    "1"      "$($K GET s 2>&1 | grep -c WRONGTYPE)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
