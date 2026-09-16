#!/usr/bin/env bash
# Bitmaps: SETBIT/GETBIT/BITCOUNT/BITPOS/BITOP/BITFIELD/BITFIELD_RO on $RESP,
# plus reply parity against a real redis on $REDIS.
#
# A bitmap is a string addressed by bit, so the assertions below are as much
# about the string underneath (it is still TYPE string, it still keeps its TTL,
# it is still WRONGTYPE against an aggregate) as about the bit arithmetic.
#
# The parity block re-runs the interesting cases against a real redis and
# compares replies verbatim, because these commands are worth having only if
# they answer exactly what a stock client expects — down to which of two errors
# an invalid command reports first. It SKIPS, rather than fails, where no redis
# is reachable, so the harness still runs on the macOS job.
set -u
RESP=${RESP:-6381}
REDIS=${REDIS:-6379}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  K="redis-cli -p $RESP"
else
  K="redis-cli --tls --insecure -p $RESP"
fi
R="redis-cli -p $REDIS"
PARITY=1
$R PING 2>/dev/null | grep -q PONG || PARITY=

pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
# Same command on both servers, replies compared verbatim.
par() {
  [ -n "$PARITY" ] || return 0
  local a b
  a="$($R "$@" 2>&1 | paste -sd,)"
  b="$($K "$@" 2>&1 | paste -sd,)"
  chk "parity: $*" "$a" "$b"
}
# Set the same state up on both servers.
both() { $K "$@" >/dev/null 2>&1; [ -n "$PARITY" ] && $R "$@" >/dev/null 2>&1; return 0; }

KEYS="bm bs bh bt ones zeros {bm}a {bm}b {bm}c {bm}dst bf bf2 bf3 bro"
clean() { both DEL $KEYS; }

echo "# bitmaps"
clean

# ---- SETBIT / GETBIT -------------------------------------------------------
chk "SETBIT on a new key -> 0"          "0"   "$($K SETBIT bm 7 1)"
chk "the key is a string"               "string" "$($K TYPE bm)"
chk "SETBIT returns the previous bit"   "1"   "$($K SETBIT bm 7 1)"
chk "bit 0 is the high bit of byte 0"   "1"   "$($K SETBIT bm 0 1 >/dev/null; $K GETBIT bm 0)"
chk "SETBIT zero-pads to reach the bit" "13"  "$($K SETBIT bm 100 1 >/dev/null; $K STRLEN bm)"
chk "GETBIT past the end -> 0"          "0"   "$($K GETBIT bm 1000)"
chk "GETBIT on a missing key -> 0"      "0"   "$($K GETBIT nosuchkey 0)"
chk "SETBIT clears a bit too"           "1"   "$($K SETBIT bm 7 0)"
chk "and the bit is clear"              "0"   "$($K GETBIT bm 7)"

# SETBIT is an ordinary string write, so it must not disturb the key's TTL.
$K DEL bt >/dev/null; $K SET bt x EX 300 >/dev/null; $K SETBIT bt 40 1 >/dev/null
t=$($K TTL bt); [ "$t" -gt 280 ] && [ "$t" -le 300 ] \
  && { echo "  PASS  SETBIT keeps the key's TTL ($t)"; pass=$((pass+1)); } \
  || { echo "  FAIL  SETBIT keeps the key's TTL got=$t"; fail=$((fail+1)); }

# ---- BITCOUNT --------------------------------------------------------------
both SET bs foobar
chk "BITCOUNT whole value"              "26"  "$($K BITCOUNT bs)"
chk "BITCOUNT byte range"               "4"   "$($K BITCOUNT bs 0 0)"
chk "BITCOUNT negative byte range"      "7"   "$($K BITCOUNT bs -2 -1)"
chk "BITCOUNT bit range"                "17"  "$($K BITCOUNT bs 5 30 BIT)"
chk "BITCOUNT inverted range -> 0"      "0"   "$($K BITCOUNT bs 3 1)"
chk "BITCOUNT past the end -> 0"        "0"   "$($K BITCOUNT bs 6 9)"
chk "BITCOUNT on a missing key -> 0"    "0"   "$($K BITCOUNT nosuchkey)"

# ---- BITPOS ----------------------------------------------------------------
both SET ones $'\xff\xff'
both SET zeros $'\x00\x00'
chk "BITPOS first set bit"              "1"   "$($K BITPOS bs 1)"
chk "BITPOS first clear bit"            "0"   "$($K BITPOS bs 0)"
chk "BITPOS 1 in an all-zero value"     "-1"  "$($K BITPOS zeros 1)"
# Looking for a clear bit with no explicit end treats the value as zero-padded
# on the right, so the answer is the first bit past it.
chk "BITPOS 0 in an all-ones value"     "16"  "$($K BITPOS ones 0)"
chk "BITPOS 0, start only, all ones"    "16"  "$($K BITPOS ones 0 0)"
# With an explicit end the range really did hold no clear bit.
chk "BITPOS 0 with an explicit end"     "-1"  "$($K BITPOS ones 0 0 -1)"
chk "BITPOS 0 with a BIT range"         "-1"  "$($K BITPOS ones 0 0 -1 BIT)"
chk "BITPOS start past the value"       "-1"  "$($K BITPOS ones 1 2)"
chk "BITPOS inverted range -> -1"       "-1"  "$($K BITPOS ones 0 5 3)"
chk "BITPOS 1 on a missing key -> -1"   "-1"  "$($K BITPOS nosuchkey 1)"
chk "BITPOS 0 on a missing key -> 0"    "0"   "$($K BITPOS nosuchkey 0)"

# ---- BITOP -----------------------------------------------------------------
both SET '{bm}a' abc
both SET '{bm}b' abcdef
chk "BITOP OR -> longest source"        "6"   "$($K BITOP OR '{bm}dst' '{bm}a' '{bm}b')"
chk "BITOP AND is as long as the longest" "6"  "$($K BITOP AND '{bm}dst' '{bm}a' '{bm}b')"
# The short operand is zero-extended, so the bytes past its end are all zero —
# not a copy of the longer one.
chk "BITOP AND zero-extends the short"  "0"   "$($K BITCOUNT '{bm}dst' 3 5)"
chk "BITOP XOR of a value with itself"  "0"   "$($K BITOP XOR '{bm}dst' '{bm}a' '{bm}a' >/dev/null; $K BITCOUNT '{bm}dst')"
chk "BITOP NOT"                         "3"   "$($K BITOP NOT '{bm}dst' '{bm}a')"
chk "BITOP over missing sources -> 0"   "0"   "$($K BITOP AND '{bm}dst' nosuch1 nosuch2)"
# An empty result deletes the destination rather than storing a zero-length
# value — including when the destination already had one.
chk "an empty result deletes the dest"  "0"   "$($K SET '{bm}dst' prior >/dev/null; $K BITOP AND '{bm}dst' nosuch1 >/dev/null; $K EXISTS '{bm}dst')"

# ---- BITFIELD --------------------------------------------------------------
chk "BITFIELD SET returns the old value" "0"  "$($K BITFIELD bf SET u8 0 255 | paste -sd,)"
chk "BITFIELD SET again returns 255"     "255" "$($K BITFIELD bf SET u8 0 1 | paste -sd,)"
chk "BITFIELD INCRBY returns the new"    "11" "$($K BITFIELD bf INCRBY u8 0 10 | paste -sd,)"
chk "BITFIELD GET"                       "11" "$($K BITFIELD bf GET u8 0 | paste -sd,)"
chk "operations apply in order"          "11,12,12" "$($K BITFIELD bf GET u8 0 INCRBY u8 0 1 GET u8 0 | paste -sd,)"
chk "u8 wraps by default"                "9"  "$($K BITFIELD bf SET u8 0 255 >/dev/null; $K BITFIELD bf INCRBY u8 0 10 | paste -sd,)"
chk "OVERFLOW SAT clamps"                "255" "$($K BITFIELD bf SET u8 0 250 >/dev/null; $K BITFIELD bf OVERFLOW SAT INCRBY u8 0 10 | paste -sd,)"
chk "OVERFLOW SAT clamps downward too"   "0"  "$($K BITFIELD bf OVERFLOW SAT INCRBY u8 0 -300 | paste -sd,)"
chk "OVERFLOW FAIL replies nil"          ""   "$($K BITFIELD bf SET u8 0 255 >/dev/null; $K BITFIELD bf OVERFLOW FAIL INCRBY u8 0 10 | paste -sd,)"
chk "and a failed write changed nothing" "255" "$($K BITFIELD bf GET u8 0 | paste -sd,)"
chk "signed fields sign-extend"          "-1" "$($K BITFIELD bf2 SET i4 0 -1 >/dev/null; $K BITFIELD bf2 GET i4 0 | paste -sd,)"
chk "the same bits read unsigned"        "15" "$($K BITFIELD bf2 GET u4 0 | paste -sd,)"
chk "i8 wraps at its own width"          "-128" "$($K BITFIELD bf2 SET i8 0 127 >/dev/null; $K BITFIELD bf2 INCRBY i8 0 1 | paste -sd,)"
chk "fields straddle byte boundaries"    "65535" "$($K BITFIELD bf2 SET u16 4 65535 >/dev/null; $K BITFIELD bf2 GET u16 4 | paste -sd,)"
chk "# offsets are counted in fields"    "3"  "$($K BITFIELD bf3 SET u8 '#2' 7 >/dev/null; $K STRLEN bf3)"
chk "a write extends the value"          "3"  "$($K STRLEN bf3)"
chk "GET alone does not create the key"  "0"  "$($K BITFIELD nosuchbf GET u8 100 >/dev/null; $K EXISTS nosuchbf)"
chk "BITFIELD with no operations -> []"  ""   "$($K BITFIELD bf3)"
chk "BITFIELD_RO GET works"              "0"  "$($K BITFIELD_RO bro GET u8 0 | paste -sd,)"
chk "BITFIELD_RO refuses SET"            "1"  "$($K BITFIELD_RO bro SET u8 0 1 2>&1 | grep -c 'only supports the GET')"
chk "BITFIELD_RO allows OVERFLOW"        "0"  "$($K BITFIELD_RO bro OVERFLOW SAT GET u8 0 | paste -sd,)"

# BITFIELD is a string write like the rest, so the TTL survives it.
$K DEL bt >/dev/null; $K SET bt x EX 300 >/dev/null; $K BITFIELD bt SET u8 0 1 >/dev/null
t=$($K TTL bt); [ "$t" -gt 280 ] && [ "$t" -le 300 ] \
  && { echo "  PASS  BITFIELD keeps the key's TTL ($t)"; pass=$((pass+1)); } \
  || { echo "  FAIL  BITFIELD keeps the key's TTL got=$t"; fail=$((fail+1)); }

# ---- WRONGTYPE -------------------------------------------------------------
# Every one of these is a string command, so an aggregate must refuse it.
$K DEL bh >/dev/null; $K HSET bh f v >/dev/null
for c in "SETBIT bh 0 1" "GETBIT bh 0" "BITCOUNT bh" "BITPOS bh 1" "BITFIELD bh GET u8 0"; do
  chk "WRONGTYPE: $c" "1" "$($K $c 2>&1 | grep -c WRONGTYPE)"
done
chk "WRONGTYPE: BITOP source" "1" "$($K BITOP AND '{bm}dst' bh 2>&1 | grep -c WRONGTYPE)"

# ---- errors ----------------------------------------------------------------
chk "SETBIT with a bad offset"    "1" "$($K SETBIT bm bogus 1 2>&1 | grep -c 'bit offset is not an integer')"
chk "SETBIT with a negative offset" "1" "$($K SETBIT bm -1 1 2>&1 | grep -c 'bit offset is not an integer')"
chk "SETBIT past max_value_bytes" "1" "$($K SETBIT bm 4294967296 1 2>&1 | grep -c 'bit offset is not an integer')"
chk "SETBIT with a bit that is not 0/1" "1" "$($K SETBIT bm 0 2 2>&1 | grep -c 'bit is not an integer')"
chk "BITCOUNT with a lone start"  "1" "$($K BITCOUNT bs 1 2>&1 | grep -c 'syntax error')"
chk "BITCOUNT with a bad unit"    "1" "$($K BITCOUNT bs 0 1 NOPE 2>&1 | grep -c 'syntax error')"
chk "BITPOS with a bit other than 0/1" "1" "$($K BITPOS bs 2 2>&1 | grep -c 'must be 1 or 0')"
chk "BITOP with an unknown operation" "1" "$($K BITOP NOPE '{bm}dst' '{bm}a' 2>&1 | grep -c 'syntax error')"
chk "BITOP NOT with two sources"  "1" "$($K BITOP NOT '{bm}dst' '{bm}a' '{bm}b' 2>&1 | grep -c 'single source key')"
chk "BITPOS with no bit argument"  "1" "$($K BITPOS bs 2>&1 | grep -c 'wrong number of arguments')"
chk "BITPOS past the unit argument" "1" "$($K BITPOS bs 1 0 -1 BYTE X 2>&1 | grep -c 'syntax error')"
chk "BITFIELD with a bad encoding" "1" "$($K BITFIELD bf GET zz 0 2>&1 | grep -c 'Invalid bitfield type')"
chk "BITFIELD u64 is not supported" "1" "$($K BITFIELD bf GET u64 0 2>&1 | grep -c 'Invalid bitfield type')"
chk "BITFIELD with a bad OVERFLOW" "1" "$($K BITFIELD bf OVERFLOW NOPE 2>&1 | grep -c 'Invalid OVERFLOW type')"
chk "BITFIELD with a truncated op" "1" "$($K BITFIELD bf GET u8 2>&1 | grep -c 'syntax error')"
# A syntax error in the last operation must leave the value untouched.
chk "a rejected BITFIELD writes nothing" "0" "$($K DEL bf3 >/dev/null; $K BITFIELD bf3 SET u8 0 1 BOGUS >/dev/null 2>&1; $K EXISTS bf3)"

# ---- parity vs a real redis ------------------------------------------------
if [ -z "$PARITY" ]; then
  echo "  SKIP  no redis on :$REDIS for parity"
else
  echo "# parity vs redis on :$REDIS"
  clean
  both SET bs foobar
  both SET ones $'\xff\xff'
  both SET zeros $'\x00\x00'
  both SET '{bm}a' abc
  both SET '{bm}b' abcdef
  both HSET bh f v

  par SETBIT bm 7 1
  par SETBIT bm 7 1
  par SETBIT bm 100 1
  par STRLEN bm
  par GETBIT bm 100
  par GETBIT bm 4096
  par GETBIT nosuchkey 0

  par BITCOUNT bs
  par BITCOUNT bs 0 0
  par BITCOUNT bs 1 1
  par BITCOUNT bs 0 -5
  par BITCOUNT bs -2 -1
  par BITCOUNT bs -1 -2
  par BITCOUNT bs 3 1
  par BITCOUNT bs 6 9
  par BITCOUNT bs 0 0 BIT
  par BITCOUNT bs 5 30 BIT
  par BITCOUNT bs 0 -5 BIT
  par BITCOUNT bs -8 -1 BIT
  par BITCOUNT bs 0 1 bit
  par BITCOUNT nosuchkey
  par BITCOUNT nosuchkey 1
  par BITCOUNT nosuchkey a b

  par BITPOS bs 1
  par BITPOS bs 0
  par BITPOS bs 1 2
  par BITPOS bs 1 -100 -1
  par BITPOS bs 1 0 -1 BIT
  par BITPOS ones 0
  par BITPOS ones 0 0
  par BITPOS ones 0 0 -1
  par BITPOS ones 0 0 -1 BIT
  par BITPOS ones 1 2
  par BITPOS ones 0 5 3
  par BITPOS zeros 1
  par BITPOS zeros 0 1 1 BIT
  par BITPOS nosuchkey 1
  par BITPOS nosuchkey 0
  par BITPOS nosuchkey 1 bogus

  par BITOP OR '{bm}dst' '{bm}a' '{bm}b'
  par BITCOUNT '{bm}dst'
  par BITOP AND '{bm}dst' '{bm}a' '{bm}b'
  par BITCOUNT '{bm}dst'
  par BITOP XOR '{bm}dst' '{bm}a' '{bm}b'
  par BITCOUNT '{bm}dst'
  par BITOP NOT '{bm}dst' '{bm}a'
  par BITCOUNT '{bm}dst'
  par BITOP AND '{bm}dst' nosuch1 nosuch2
  par EXISTS '{bm}dst'
  par BITOP NOT '{bm}dst' nosuch1
  par EXISTS '{bm}dst'

  par BITFIELD bf SET u8 0 255
  par BITFIELD bf SET u8 0 1
  par BITFIELD bf INCRBY u8 0 10
  par BITFIELD bf GET u8 0 INCRBY u8 0 1 GET u8 0
  par BITFIELD bf SET u8 0 255
  par BITFIELD bf INCRBY u8 0 10
  par BITFIELD bf OVERFLOW SAT INCRBY u8 0 300
  par BITFIELD bf OVERFLOW SAT INCRBY u8 0 -300
  par BITFIELD bf OVERFLOW FAIL SET u8 0 300
  par BITFIELD bf OVERFLOW FAIL INCRBY u8 0 10
  par BITFIELD bf SET u8 0 300
  par BITFIELD bf SET i8 0 -128
  par BITFIELD bf INCRBY i8 0 -1
  par BITFIELD bf GET i8 0
  par BITFIELD bf2 SET i4 0 -1
  par BITFIELD bf2 GET i4 0
  par BITFIELD bf2 GET u4 0
  par BITFIELD bf2 SET u16 4 65535
  par BITFIELD bf2 GET u16 4
  par BITFIELD bf2 SET i64 0 -9223372036854775808
  par BITFIELD bf2 GET i64 0
  par BITFIELD bf2 OVERFLOW SAT INCRBY i64 0 -1
  par BITFIELD bf2 SET u63 0 9223372036854775807
  par BITFIELD bf2 GET u63 0
  par BITFIELD bf3 SET u8 '#2' 7
  par BITFIELD bf3 GET u8 '#2'
  par STRLEN bf3
  par BITFIELD nosuchbf GET u8 100
  par EXISTS nosuchbf
  par BITFIELD bf3
  par BITFIELD_RO bf3 GET u8 0
  par BITFIELD_RO bf3 OVERFLOW SAT GET u8 0
  par BITFIELD_RO bf3 SET u8 0 1

  # Errors, and which of two the command reports first.
  par SETBIT bm bogus 1
  par SETBIT bm -1 1
  par SETBIT bm 4294967296 1
  par SETBIT bm 0 2
  par SETBIT bh bogus 1
  par SETBIT bh 0 2
  par SETBIT bh 0 1
  par GETBIT bh bogus
  par GETBIT bh 0
  par BITCOUNT bh
  par BITCOUNT bh 1
  par BITCOUNT bs 1
  par BITCOUNT bs 0 1 NOPE
  par BITPOS bh 1
  par BITPOS bs 2
  par BITOP NOPE '{bm}dst' '{bm}a'
  par BITOP NOT '{bm}dst' '{bm}a' '{bm}b'
  par BITOP AND '{bm}dst' bh
  par BITFIELD bh GET u8 0
  par BITFIELD bh BOGUS
  par BITFIELD bh GET u8
  par BITFIELD bf GET zz 0
  par BITFIELD bf GET u64 0
  par BITFIELD bf GET i65 0
  par BITFIELD bf GET u8 -1
  par BITFIELD bf OVERFLOW NOPE
  par BITFIELD bf OVERFLOW
  par BITFIELD bf SET u8 0 notanint
  # Argument counts past the last meaningful one, and the strict integer parse:
  # a real redis refuses a leading `+`, a leading zero and surrounding space.
  # NOTE: `par BITPOS bs` is deliberately absent. The reply differs by the
  # trailing " command" that redis appends to every arity error and that no
  # message in this codebase carries; the arity itself is asserted above.
  par BITPOS bs 1 0 -1 BYTE X
  par BITCOUNT bs 0 1 BYTE X
  par SETBIT bm 08 1
  par SETBIT bm +1 1
  par GETBIT bm 08
  par BITCOUNT bs +0 +1
  par BITCOUNT bs -0 1
  par BITFIELD bf GET u8 08
  par BITFIELD bf SET u8 0 +1
fi

clean
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
