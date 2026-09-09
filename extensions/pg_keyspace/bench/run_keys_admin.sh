#!/usr/bin/env bash
# Key-space management (RENAME/RENAMENX/COPY/TOUCH/RANDOMKEY/EXPIRETIME/OBJECT)
# and server/admin commands (ECHO/TIME/INFO/MEMORY/DEBUG/FLUSHDB). Assumes a
# daemon on $RESP.
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

echo "# keys + admin"
$K FLUSHDB >/dev/null 2>&1
$K DEL a b c src dst h >/dev/null 2>&1

# RENAME / RENAMENX
$K SET src hello >/dev/null
chk "RENAME -> OK"                  "OK"      "$($K RENAME src dst)"
chk "RENAME moved the value"        "hello"   "$($K GET dst)"
chk "RENAME removed the source"     "0"       "$($K EXISTS src)"
chk "RENAME missing key -> error"   "1"       "$($K RENAME nope x 2>&1 | grep -ci 'no such key')"
$K SET a 1 >/dev/null; $K SET b 2 >/dev/null
chk "RENAMENX onto existing -> 0"   "0"       "$($K RENAMENX a b)"
$K DEL b >/dev/null
chk "RENAMENX onto free -> 1"       "1"       "$($K RENAMENX a b)"
chk "RENAMENX moved the value"      "1"       "$($K GET b)"

# COPY
$K SET c orig >/dev/null; $K DEL dst2 >/dev/null
chk "COPY to new key -> 1"          "1"       "$($K COPY c dst2)"
chk "COPY duplicated the value"     "orig"    "$($K GET dst2)"
chk "COPY onto existing -> 0"       "0"       "$($K COPY c dst2)"
chk "COPY REPLACE onto existing -> 1" "1"     "$($K COPY c dst2 REPLACE)"
chk "COPY source still present"     "orig"    "$($K GET c)"

# TOUCH
chk "TOUCH counts existing keys"    "2"       "$($K TOUCH c dst2 nope)"

# TTL preserved by RENAME
$K SET t v EX 100 >/dev/null
$K RENAME t t2 >/dev/null
tt=$($K TTL t2); [ "$tt" -ge 90 ] && [ "$tt" -le 100 ] && { echo "  PASS  RENAME preserves TTL ($tt)"; pass=$((pass+1)); } || { echo "  FAIL  RENAME TTL got=$tt"; fail=$((fail+1)); }

# EXPIRETIME / PEXPIRETIME
$K SET e v >/dev/null
chk "EXPIRETIME no expiry -> -1"    "-1"      "$($K EXPIRETIME e)"
chk "EXPIRETIME missing -> -2"      "-2"      "$($K EXPIRETIME nope)"
$K EXPIREAT e 9999999999 >/dev/null
chk "EXPIRETIME returns the stamp"  "9999999999" "$($K EXPIRETIME e)"

# OBJECT ENCODING
$K SET i 12345 >/dev/null
chk "OBJECT ENCODING int"           "int"     "$($K OBJECT ENCODING i)"
$K SET sv "not a number here" >/dev/null
chk "OBJECT ENCODING embstr"        "embstr"  "$($K OBJECT ENCODING sv)"
$K DEL hh >/dev/null; $K HSET hh f 1 >/dev/null
chk "OBJECT ENCODING hashtable"     "hashtable" "$($K OBJECT ENCODING hh)"
chk "OBJECT ENCODING missing -> err" "1"       "$($K OBJECT ENCODING nope 2>&1 | grep -ci 'no such key')"
chk "OBJECT REFCOUNT"               "1"       "$($K OBJECT REFCOUNT i)"

# RANDOMKEY returns one of the live keys
rk=$($K RANDOMKEY)
chk "RANDOMKEY returns a live key"  "1"       "$($K EXISTS "$rk")"

# ---- admin ----
chk "ECHO"                          "hi there" "$($K ECHO 'hi there')"
chk "TIME returns two fields"       "2"       "$($K TIME | wc -l | tr -d ' ')"
chk "INFO has redis_version"        "1"       "$($K INFO | grep -ci '^redis_version:')"
chk "INFO has a keyspace line"      "1"       "$($K INFO | grep -ci '^db0:keys=')"
chk "MEMORY USAGE returns bytes"    "1"       "$([ "$($K MEMORY USAGE i)" -gt 0 ] && echo 1 || echo 0)"
chk "MEMORY USAGE missing -> nil"   ""        "$($K MEMORY USAGE nope)"
chk "DEBUG OBJECT"                  "1"       "$($K DEBUG OBJECT i | grep -ci 'encoding')"
chk "DEBUG SLEEP -> OK (no stall)"  "OK"      "$($K DEBUG SLEEP 0)"

# FLUSHDB clears the keyspace
chk "FLUSHDB -> OK"                 "OK"      "$($K FLUSHDB)"
chk "FLUSHDB emptied the keyspace"  "0"       "$($K DBSIZE)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
