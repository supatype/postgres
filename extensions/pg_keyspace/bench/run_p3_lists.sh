#!/usr/bin/env bash
# P3 §5 — list type: command coverage, WRONGTYPE semantics, redis parity, and a
# throughput sample. Works whether the RESP port is plaintext or TLS.
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

echo "# P3 lists (§5)"
$K DEL l str >/dev/null 2>&1

chk "RPUSH a b c -> 3"            "3"        "$($K RPUSH l a b c)"
chk "LPUSH z -> 4"               "4"        "$($K LPUSH l z)"
chk "LRANGE 0 -1"                "z,a,b,c"  "$($K LRANGE l 0 -1 | paste -sd,)"
chk "LRANGE 1 2"                 "a,b"      "$($K LRANGE l 1 2 | paste -sd,)"
chk "LRANGE -2 -1"              "b,c"      "$($K LRANGE l -2 -1 | paste -sd,)"
chk "LINDEX 0 / -1"             "z,c"      "$($K LINDEX l 0),$($K LINDEX l -1)"
chk "LINDEX out of range -> nil" ""         "$($K LINDEX l 99)"
chk "LLEN"                       "4"        "$($K LLEN l)"
chk "LPOP"                       "z"        "$($K LPOP l)"
chk "RPOP"                       "c"        "$($K RPOP l)"
chk "LRANGE after pops"          "a,b"      "$($K LRANGE l 0 -1 | paste -sd,)"
chk "LSET 0 X"                   "OK"       "$($K LSET l 0 X)"
chk "LSET out of range -> err"   "1"        "$($K LSET l 9 Y 2>&1 | grep -c 'out of range')"
chk "LTRIM 0 0"                  "OK"       "$($K LTRIM l 0 0)"
chk "LRANGE after trim"          "X"        "$($K LRANGE l 0 -1 | paste -sd,)"
chk "LPOP count 5 drains"        "X"        "$($K LPOP l 5 | paste -sd,)"
chk "emptied list deletes key"   "0"        "$($K EXISTS l)"
chk "LPUSHX on missing -> 0"     "0"        "$($K LPUSHX gone v)"
chk "RPUSHX on missing -> 0"     "0"        "$($K RPUSHX gone v)"
chk "TYPE list"                  "list"     "$($K RPUSH lt a >/dev/null; $K TYPE lt)"

# WRONGTYPE both directions
$K SET str hi >/dev/null
chk "RPUSH on a string -> WRONGTYPE"   "1"  "$($K RPUSH str x 2>&1 | grep -c WRONGTYPE)"
chk "LRANGE on a string -> WRONGTYPE"  "1"  "$($K LRANGE str 0 -1 2>&1 | grep -c WRONGTYPE)"
$K DEL lh >/dev/null 2>&1; $K RPUSH lh a >/dev/null
chk "GET on a list -> WRONGTYPE"       "1"  "$($K GET lh 2>&1 | grep -c WRONGTYPE)"

# parity spot-check against a real redis on :6379 if present
if redis-cli -p 6379 PING 2>/dev/null | grep -q PONG; then
  redis-cli -p 6379 DEL lp >/dev/null 2>&1; $K DEL lp >/dev/null 2>&1
  for c in "RPUSH lp 1 2 3 4 5" "LPUSH lp 0" "LPOP lp" "RPOP lp" "LTRIM lp 0 1" "LSET lp 1 9"; do
    redis-cli -p 6379 $c >/dev/null 2>&1; $K $c >/dev/null 2>&1
  done
  chk "LRANGE matches real redis" \
      "$(redis-cli -p 6379 LRANGE lp 0 -1 | paste -sd,)" "$($K LRANGE lp 0 -1 | paste -sd,)"
fi

echo
if command -v redis-benchmark >/dev/null 2>&1; then
  TLSFLAGS=""; echo "$K" | grep -q tls && TLSFLAGS="--tls --insecure"
  # Spread pushes across many DISTINCT small lists (-r random key), which is the
  # cache-representative case. NB: hammering ONE list to 100k elements is the O(n)
  # blob-rewrite worst case by design — small collections are the intended use.
  echo "# throughput (redis-benchmark, 100k RPUSH across 100k small lists, pipeline 16):"
  redis-benchmark $TLSFLAGS -p $RESP -n 100000 -P 16 -q -r 100000 RPUSH "list:__rand_int__" v 2>/dev/null | sed 's/^/  /'
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
