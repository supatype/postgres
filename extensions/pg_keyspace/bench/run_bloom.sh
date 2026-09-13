#!/usr/bin/env bash
# Bloom filters: BF.RESERVE/ADD/MADD/INSERT/EXISTS/MEXISTS/INFO/CARD/SCANDUMP/
# LOADCHUNK on $RESP, plus reply parity against a real Redis 8 on $REDIS8.
set -u
RESP=${RESP:-6381}
REDIS8=${REDIS8:-6380}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  K="redis-cli -p $RESP"; K3="redis-cli -3 -p $RESP"
else
  K="redis-cli --tls --insecure -p $RESP"; K3="redis-cli -3 --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
yn() { if "$@"; then echo yes; else echo no; fi; }
DUMP=/tmp/pgks_bloom_dump.bin
CHUNK=/tmp/pgks_bloom_chunk.bin
trap 'rm -f "$DUMP" "$CHUNK"' EXIT

echo "# bloom filters"
$K DEL b auto ins ins2 ins3 ns exp4 str cp rn dsrc ddst tk mq dbf grow lc r3 >/dev/null 2>&1

# BF.RESERVE / BF.ADD / BF.EXISTS / BF.CARD
chk "BF.RESERVE -> OK"                  "OK"  "$($K BF.RESERVE b 0.01 100)"
chk "BF.ADD new item -> 1"              "1"   "$($K BF.ADD b alpha)"
chk "BF.ADD the same item -> 0"         "0"   "$($K BF.ADD b alpha)"
chk "BF.EXISTS added -> 1"              "1"   "$($K BF.EXISTS b alpha)"
chk "BF.EXISTS never added -> 0"        "0"   "$($K BF.EXISTS b nope)"
chk "BF.EXISTS on a missing key -> 0"   "0"   "$($K BF.EXISTS gone alpha)"
chk "BF.CARD counts the items"          "1"   "$($K BF.CARD b)"
chk "BF.CARD on a missing key -> 0"     "0"   "$($K BF.CARD gone)"

# BF.MADD / BF.MEXISTS
chk "BF.MADD dup + new -> 0,1"          "0,1"     "$($K BF.MADD b alpha beta | paste -sd,)"
chk "BF.MEXISTS mixed -> 1,1,0"         "1,1,0"   "$($K BF.MEXISTS b alpha beta nope | paste -sd,)"
chk "BF.CARD after BF.MADD"             "2"       "$($K BF.CARD b)"

# BF.ADD creates the key with the RedisBloom defaults
chk "BF.ADD creates a filter"           "1"       "$($K BF.ADD auto x)"
chk "default capacity is 100"           "100"     "$($K BF.INFO auto CAPACITY)"
chk "default expansion is 2"            "2"       "$($K BF.INFO auto EXPANSION)"
$K DEL auto >/dev/null

# BF.INFO, whole reply and every field
chk "BF.INFO has five fields"           "5"       "$($K BF.INFO b | grep -c '^[A-Z]')"
chk "BF.INFO field names"               "Capacity,Size,Number of filters,Number of items inserted,Expansion rate" \
                                        "$($K BF.INFO b | grep '^[A-Z]' | paste -sd,)"
chk "BF.INFO CAPACITY"                  "100"     "$($K BF.INFO b CAPACITY)"
chk "BF.INFO FILTERS"                   "1"       "$($K BF.INFO b FILTERS)"
chk "BF.INFO ITEMS"                     "2"       "$($K BF.INFO b ITEMS)"
chk "BF.INFO EXPANSION"                 "2"       "$($K BF.INFO b EXPANSION)"
chk "BF.INFO SIZE is positive"          "yes"     "$(yn [ "$($K BF.INFO b SIZE)" -gt 0 ])"

# BF.RESERVE options
chk "BF.RESERVE EXPANSION 4"            "OK"      "$($K BF.RESERVE exp4 0.01 100 EXPANSION 4)"
chk "EXPANSION 4 is reported"           "4"       "$($K BF.INFO exp4 EXPANSION)"
chk "BF.RESERVE NONSCALING"             "OK"      "$($K BF.RESERVE ns 0.01 3 NONSCALING)"
chk "NONSCALING reports no expansion"   ""        "$($K BF.INFO ns EXPANSION)"

# BF.INSERT and its options
chk "BF.INSERT ITEMS -> 1,1,1"          "1,1,1"   "$($K BF.INSERT ins ITEMS a b c | paste -sd,)"
chk "BF.INSERT dup + new -> 0,1"        "0,1"     "$($K BF.INSERT ins ITEMS a d | paste -sd,)"
chk "BF.INSERT NOCREATE on a filter"    "1"       "$($K BF.INSERT ins NOCREATE ITEMS e)"
chk "BF.INSERT CAPACITY + ERROR"        "1"       "$($K BF.INSERT ins2 CAPACITY 1000 ERROR 0.001 EXPANSION 3 ITEMS z)"
chk "BF.INSERT set the capacity"        "1000"    "$($K BF.INFO ins2 CAPACITY)"
chk "BF.INSERT set the expansion"       "3"       "$($K BF.INFO ins2 EXPANSION)"
chk "BF.INSERT NONSCALING"              "1"       "$($K BF.INSERT ins3 CAPACITY 2 NONSCALING ITEMS z)"
chk "BF.INSERT NONSCALING has no rate"  ""        "$($K BF.INFO ins3 EXPANSION)"
$K DEL ins2 ins3 >/dev/null

# error texts
chk "BF.RESERVE on an existing key"     "ERR item exists" "$($K BF.RESERVE b 0.01 100 2>&1)"
chk "BF.RESERVE bad error rate"         "ERR bad error rate" "$($K BF.RESERVE q abc 100 2>&1)"
chk "BF.RESERVE error rate out of range" "ERR error rate must be in the range (0.000000, 1.000000)" \
                                        "$($K BF.RESERVE q 1.0 100 2>&1)"
chk "BF.RESERVE bad capacity"           "ERR bad capacity" "$($K BF.RESERVE q 0.01 abc 2>&1)"
chk "BF.RESERVE capacity out of range"  "ERR capacity must be in the range [1, 1073741824]" \
                                        "$($K BF.RESERVE q 0.01 0 2>&1)"
chk "BF.RESERVE EXPANSION + NONSCALING" "Nonscaling filters cannot expand" \
                                        "$($K BF.RESERVE q 0.01 100 EXPANSION 2 NONSCALING 2>&1)"
chk "BF.INFO on a missing key"          "ERR not found" "$($K BF.INFO gone 2>&1)"
chk "BF.INFO with a bad field"          "Invalid information value" "$($K BF.INFO b BOGUS 2>&1)"
chk "BF.INSERT NOCREATE on a missing key" "ERR not found" "$($K BF.INSERT gone NOCREATE ITEMS a 2>&1)"
chk "BF.INSERT bad capacity"            "Bad capacity" "$($K BF.INSERT q CAPACITY 0 ITEMS a 2>&1)"
chk "BF.INSERT bad error rate"          "Bad error rate" "$($K BF.INSERT q ERROR 2 ITEMS a 2>&1)"
chk "BF.INSERT bad expansion"           "Bad expansion" "$($K BF.INSERT q EXPANSION x ITEMS a 2>&1)"
chk "BF.INSERT unknown argument"        "Unknown argument received" "$($K BF.INSERT q BOGUS ITEMS a 2>&1)"
chk "BF.RESERVE bad expansion"          "ERR bad expansion" "$($K BF.RESERVE q 0.01 100 EXPANSION abc 2>&1)"
chk "BF.LOADCHUNK of a foreign chunk"   "ERR received bad data" "$($K BF.LOADCHUNK lc 1 not-a-filter 2>&1)"
chk "BF.LOADCHUNK iterator 0"           "ERR not found" "$($K BF.LOADCHUNK lc 0 not-a-filter 2>&1)"
chk "BF.SCANDUMP iterator not numeric"  "Second argument must be numeric" "$($K BF.SCANDUMP b abc 2>&1)"
chk "BF.SCANDUMP on a missing key"      "ERR not found" "$($K BF.SCANDUMP gone 0 2>&1)"
chk "BF.RESERVE a subnormal error rate"  "ERR could not create filter" "$($K BF.RESERVE sub 5e-324 1 2>&1)"
chk "BF.RESERVE a negative error rate"   "ERR error rate must be in the range (0.000000, 1.000000)" \
                                        "$($K BF.RESERVE sub2 -1 100 2>&1)"
chk "BF.INSERT a negative error rate"    "Bad error rate" "$($K BF.INSERT sub3 ERROR -1 ITEMS a 2>&1)"
chk "BF.INSERT a zero error rate"        "Bad error rate" "$($K BF.INSERT sub4 ERROR 0 ITEMS a 2>&1)"
chk "BF.RESERVE EXPANSION with no value" "ERR no expansion" "$($K BF.RESERVE sub5 0.01 100 X EXPANSION 2>&1)"
chk "BF.INSERT CAPACITY with no value"   "ERR wrong number of arguments for 'bf.insert' command" \
                                        "$($K BF.INSERT sub6 CAPACITY 2>&1)"
chk "BF.LOADCHUNK of an empty chunk"     "ERR received bad data" "$($K BF.LOADCHUNK ez 1 "" 2>&1)"
chk "an empty chunk created no key"      "0" "$($K EXISTS ez)"
# option-token parity cases, pinned here and compared to Redis 8 below
$K DEL o1 o2 o3 o4 o5 o6 o7 >/dev/null 2>&1
chk "BF.RESERVE an unknown token"        "OK" "$($K BF.RESERVE o1 0.01 100 BOGUS 2>&1)"
chk "BF.RESERVE two unknown tokens"      "OK" "$($K BF.RESERVE o2 0.01 100 BOGUS BOGUS 2>&1)"
chk "BF.RESERVE NONSCALING twice"        "OK" "$($K BF.RESERVE o3 0.01 100 NONSCALING NONSCALING 2>&1)"
chk "BF.RESERVE EXPANSION twice"         "ERR wrong number of arguments for 'bf.reserve' command" \
                                        "$($K BF.RESERVE o4 0.01 100 EXPANSION 2 EXPANSION 3 2>&1)"
chk "BF.RESERVE a trailing EXPANSION"    "OK" "$($K BF.RESERVE o5 0.01 100 EXPANSION 2 EXPANSION 2>&1)"
chk "the first EXPANSION won"            "2" "$($K BF.INFO o5 EXPANSION)"
chk "BF.INSERT CAPACITY twice"           "1" "$($K BF.INSERT o6 CAPACITY 200 CAPACITY 300 ITEMS x 2>&1)"
chk "the last CAPACITY won"              "300" "$($K BF.INFO o6 CAPACITY)"
chk "BF.INSERT EXPANSION twice"          "1" "$($K BF.INSERT o7 EXPANSION 2 EXPANSION 3 ITEMS x 2>&1)"
chk "the last EXPANSION won"             "3" "$($K BF.INFO o7 EXPANSION)"
$K DEL o1 o2 o3 o4 o5 o6 o7 ez sub2 sub3 sub4 sub5 sub6 >/dev/null 2>&1
# a chain that keeps scaling is not capped at 32 sub-filters
$K DEL mfx >/dev/null; $K BF.RESERVE mfx 0.01 1 EXPANSION 1 >/dev/null
awk 'BEGIN{for(i=0;i<60;i++) printf "BF.ADD mfx f-%d\n", i}' | $K >/dev/null 2>&1
chk "a scaling chain passes 32 filters"  "yes" "$(yn [ "$($K BF.INFO mfx FILTERS)" -gt 32 ])"
chk "and answers no error at 60 adds"    "0" "$($K BF.ADD mfx f-0 2>&1 | grep -c ERR)"
$K DEL mfx >/dev/null
chk "BF.INSERT a subnormal error rate"   "ERR could not create filter" "$($K BF.INSERT sub ERROR 5e-324 ITEMS x 2>&1)"
chk "BF.LOADCHUNK iterator not numeric" "ERR Second argument must be numeric" "$($K BF.LOADCHUNK b abc data 2>&1)"
chk "BF.ADD wrong arity"                "ERR wrong number of arguments for 'bf.add' command" "$($K BF.ADD b 2>&1)"
chk "BF.RESERVE wrong arity"            "ERR wrong number of arguments for 'bf.reserve' command" "$($K BF.RESERVE b 2>&1)"

# a NONSCALING filter fills up and refuses more
$K BF.ADD ns n1 >/dev/null; $K BF.ADD ns n2 >/dev/null; $K BF.ADD ns n3 >/dev/null
chk "a full NONSCALING filter refuses"  "ERR non scaling filter is full" "$($K BF.ADD ns over 2>&1)"
# BF.MADD and BF.INSERT answer the array built so far, then the error as its
# last element, and stop. Redis 8 does the same.
$K DEL mfull >/dev/null; $K BF.RESERVE mfull 0.01 2 NONSCALING >/dev/null
chk "BF.MADD fills then errors in the array" "1,1,ERR non scaling filter is full" \
                                        "$($K BF.MADD mfull p q r s 2>&1 | grep . | paste -sd,)"
chk "BF.MADD on a full filter -> 1 element" "ERR non scaling filter is full" \
                                        "$($K BF.MADD mfull z 2>&1 | grep . | paste -sd,)"
$K DEL mfull2 >/dev/null; $K BF.RESERVE mfull2 0.01 2 NONSCALING >/dev/null
chk "BF.INSERT fills then errors in the array" "1,1,ERR non scaling filter is full" \
                                        "$($K BF.INSERT mfull2 ITEMS p q r s 2>&1 | grep . | paste -sd,)"
$K DEL mfull3 >/dev/null; $K BF.RESERVE mfull3 0.01 2 NONSCALING >/dev/null
chk "BF.MADD keeps the duplicate result"  "1,0,1,ERR non scaling filter is full" \
                                        "$($K BF.MADD mfull3 p p q r 2>&1 | grep . | paste -sd,)"
$K DEL mfull mfull2 mfull3 >/dev/null

# WRONGTYPE both ways, TYPE and OBJECT ENCODING
$K SET str v >/dev/null
chk "BF.ADD on a string -> WRONGTYPE"   "1" "$($K BF.ADD str x 2>&1 | grep -c WRONGTYPE)"
chk "BF.INFO on a string -> WRONGTYPE"  "1" "$($K BF.INFO str 2>&1 | grep -c WRONGTYPE)"
chk "BF.CARD on a string -> WRONGTYPE"  "1" "$($K BF.CARD str 2>&1 | grep -c WRONGTYPE)"
chk "BF.SCANDUMP on a string -> WRONGTYPE"  "1" "$($K BF.SCANDUMP str 0 2>&1 | grep -c WRONGTYPE)"
chk "BF.LOADCHUNK on a string -> WRONGTYPE" "1" "$($K BF.LOADCHUNK str 1 zz 2>&1 | grep -c WRONGTYPE)"
chk "GET on a filter -> WRONGTYPE"      "1" "$($K GET b 2>&1 | grep -c WRONGTYPE)"
chk "SADD on a filter -> WRONGTYPE"     "1" "$($K SADD b x 2>&1 | grep -c WRONGTYPE)"
chk "TYPE reports MBbloom--"            "MBbloom--" "$($K TYPE b)"
chk "OBJECT ENCODING reports raw"       "raw" "$($K OBJECT ENCODING b)"

# EXPIRE / TTL, kept by an add and by growth
$K BF.RESERVE tk 0.01 2 >/dev/null
$K EXPIRE tk 100 >/dev/null
chk "EXPIRE on a filter -> TTL"         "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
$K BF.ADD tk t1 >/dev/null
chk "an add keeps the TTL"              "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
$K BF.ADD tk t2 >/dev/null; $K BF.ADD tk t3 >/dev/null
chk "growth keeps the TTL"              "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
chk "the grown filter kept its items"   "1,1,1" "$($K BF.MEXISTS tk t1 t2 t3 | paste -sd,)"
$K PERSIST tk >/dev/null
chk "PERSIST clears the TTL"            "-1" "$($K TTL tk)"

# COPY and RENAME keep the type and the contents
$K DEL cp rn >/dev/null
chk "COPY a filter -> 1"                "1" "$($K COPY b cp)"
chk "the copy is a filter"              "MBbloom--" "$($K TYPE cp)"
chk "the copy answers BF.EXISTS"        "1" "$($K BF.EXISTS cp alpha)"
chk "RENAME a filter -> OK"             "OK" "$($K RENAME cp rn)"
chk "the renamed key is a filter"       "MBbloom--" "$($K TYPE rn)"
chk "the renamed key answers BF.EXISTS" "1" "$($K BF.EXISTS rn alpha)"
$K DEL rn >/dev/null

# BF.ADD inside MULTI/EXEC
chk "BF.ADD queues and runs in MULTI"   "OK,QUEUED,QUEUED,1,1" \
    "$(printf 'MULTI\nBF.ADD mq v1\nBF.EXISTS mq v1\nEXEC\n' | $K | paste -sd,)"

# SCANDUMP / LOADCHUNK round trip, driven the way a RedisBloom client drives it:
# the iterator SCANDUMP returns is the iterator LOADCHUNK is given.
$K BF.RESERVE dsrc 0.01 200 >/dev/null
for i in 1 2 3 4 5 6 7 8; do $K BF.ADD dsrc "dump-$i" >/dev/null; done
it=0; loaded=""; chunks=0
while :; do
  $K BF.SCANDUMP dsrc "$it" > "$DUMP" 2>&1
  it=$(head -1 "$DUMP")
  case "$it" in ''|*[!0-9]*) loaded="$it"; break;; esac
  [ "$it" -eq 0 ] && break
  tot=$(wc -c < "$DUMP")
  tail -c +$(( ${#it} + 2 )) "$DUMP" | head -c $(( tot - ${#it} - 2 )) > "$CHUNK"
  loaded=$($K -x BF.LOADCHUNK ddst "$it" < "$CHUNK" 2>&1)
  [ "$loaded" = "OK" ] || break
  chunks=$((chunks+1))
  [ "$chunks" -gt 8 ] && break
done
chk "BF.LOADCHUNK accepts a SCANDUMP chunk" "OK" "$loaded"
chk "the restored filter is a filter"   "MBbloom--" "$($K TYPE ddst)"
chk "the restored filter has the items" "1,1,1,0" "$($K BF.MEXISTS ddst dump-1 dump-5 dump-8 dump-none | paste -sd,)"
chk "the restored filter has the count" "$($K BF.CARD dsrc)" "$($K BF.CARD ddst)"

# one filter is one key, before and after growth
before=$($K DBSIZE)
$K BF.RESERVE dbf 0.01 10 >/dev/null
for i in $(seq 1 40); do $K BF.ADD dbf "k$i" >/dev/null; done
chk "a filter is one key (DBSIZE +1)"   "1" "$(( $($K DBSIZE) - before ))"
chk "DEL removes the whole filter"      "1" "$($K DEL dbf)"

# growth: 300 items into a capacity-100 filter
$K BF.RESERVE grow 0.01 100 >/dev/null
awk 'BEGIN{for(i=0;i<300;i++) printf "BF.ADD grow grow-%d\n", i}' | $K >/dev/null
chk "growth added sub-filters"          "2" "$($K BF.INFO grow FILTERS)"
chk "growth raised the capacity"        "300" "$($K BF.INFO grow CAPACITY)"
chk "growth is still one key"           "MBbloom--" "$($K TYPE grow)"
hit=$(awk 'BEGIN{line="BF.MEXISTS grow"; for(i=0;i<300;i++) line=line " grow-" i; print line}' | $K | grep -c '^1$')
chk "all 300 items survive growth"      "300" "$hit"

# RESP3 reply shapes (redis-cli -3 parses them, so a malformed frame errors)
$K DEL r3 >/dev/null
chk "RESP3 BF.ADD is a boolean"         "true"  "$($K3 BF.ADD r3 z | tr -d '()')"
chk "RESP3 BF.ADD dup is a boolean"     "false" "$($K3 BF.ADD r3 z | tr -d '()')"
chk "RESP3 BF.EXISTS is a boolean"      "true"  "$($K3 BF.EXISTS r3 z | tr -d '()')"
chk "RESP3 BF.MEXISTS is an array"      "true,false" "$($K3 BF.MEXISTS r3 z q | tr -d '()' | paste -sd,)"
chk "RESP3 BF.INFO is a map"            "Capacity 100" "$($K3 BF.INFO r3 | head -1)"
chk "RESP3 BF.INFO CAPACITY is a map"   "Capacity 100" "$($K3 BF.INFO r3 CAPACITY)"
chk "RESP3 BF.CARD is an integer"       "1" "$($K3 BF.CARD r3)"

$K DEL b auto ins ns exp4 str dsrc ddst tk mq grow lc r3 >/dev/null 2>&1

# ---- parity against Redis 8 -------------------------------------------------
R8="redis-cli -p $REDIS8"
if ! $R8 PING 2>/dev/null | grep -q PONG; then
  echo "  SKIP  no redis 8 on :$REDIS8 for parity"
elif $R8 BF.RESERVE pingf 0.01 100 2>&1 | grep -qi 'unknown command'; then
  echo "  SKIP  no bloom filters on :$REDIS8 for parity"
else
  echo "# parity vs redis 8 on :$REDIS8"
  for R in "$K" "$R8"; do $R DEL p pm pg pn fp >/dev/null 2>&1; done

  a=$($R8 BF.RESERVE p 0.01 100); b=$($K BF.RESERVE p 0.01 100)
  chk "BF.RESERVE parity"               "$a" "$b"
  adds() { awk 'BEGIN{for(i=0;i<100;i++) printf "BF.ADD p p-%d\n", i}'; }
  a=$(adds | $R8 | paste -sd,); b=$(adds | $K | paste -sd,)
  chk "100 BF.ADD replies parity"       "$a" "$b"
  hits() { awk -v p="$1" 'BEGIN{line="BF.MEXISTS p"; for(i=0;i<100;i++) line=line " " p "-" i; print line}'; }
  a=$(hits p | $R8 | paste -sd,); b=$(hits p | $K | paste -sd,)
  chk "BF.MEXISTS on 100 members parity" "$a" "$b"
  ar=$(hits miss | $R8 | grep -c '^1$'); br=$(hits miss | $K | grep -c '^1$')
  chk "redis 8 false positives in 100 misses within budget" "yes" "$(yn [ "$ar" -le 3 ])"
  chk "daemon false positives in 100 misses within budget"  "yes" "$(yn [ "$br" -le 3 ])"
  chk "BF.CARD parity"                  "$($R8 BF.CARD p)"            "$($K BF.CARD p)"
  chk "BF.INFO CAPACITY parity"         "$($R8 BF.INFO p CAPACITY)"   "$($K BF.INFO p CAPACITY)"
  chk "BF.INFO ITEMS parity"            "$($R8 BF.INFO p ITEMS)"      "$($K BF.INFO p ITEMS)"
  chk "BF.INFO EXPANSION parity"        "$($R8 BF.INFO p EXPANSION)"  "$($K BF.INFO p EXPANSION)"
  chk "BF.MADD parity"                  "$($R8 BF.MADD pm m1 m2 m1 | paste -sd,)" "$($K BF.MADD pm m1 m2 m1 | paste -sd,)"
  chk "BF.MEXISTS parity"               "$($R8 BF.MEXISTS pm m1 m2 zz | paste -sd,)" "$($K BF.MEXISTS pm m1 m2 zz | paste -sd,)"

  # growth: the same 300 items into the same capacity-100 filter on both
  grows() { awk 'BEGIN{print "BF.RESERVE pg 0.01 100"; for(i=0;i<300;i++) printf "BF.ADD pg g-%d\n", i}'; }
  grows | $R8 >/dev/null; grows | $K >/dev/null
  chk "BF.INFO FILTERS parity after growth" "$($R8 BF.INFO pg FILTERS)"  "$($K BF.INFO pg FILTERS)"
  chk "BF.INFO CAPACITY parity after growth" "$($R8 BF.INFO pg CAPACITY)" "$($K BF.INFO pg CAPACITY)"

  # error texts, reply for reply
  errs() {
    $1 BF.RESERVE p 0.01 100 2>&1
    $1 BF.RESERVE pe abc 100 2>&1
    $1 BF.RESERVE pe 1.0 100 2>&1
    $1 BF.RESERVE pe 0.01 abc 2>&1
    $1 BF.RESERVE pe 0.01 0 2>&1
    $1 BF.RESERVE pe 0.01 100 EXPANSION 2 NONSCALING 2>&1
    $1 BF.INSERT pe NOCREATE ITEMS a 2>&1
    $1 BF.INSERT pe CAPACITY 0 ITEMS a 2>&1
    $1 BF.INSERT pe ERROR 2 ITEMS a 2>&1
    $1 BF.INSERT pe EXPANSION x ITEMS a 2>&1
    $1 BF.INSERT pe BOGUS ITEMS a 2>&1
    $1 BF.INFO pe 2>&1
    $1 BF.INFO p BOGUS 2>&1
    $1 BF.SCANDUMP p abc 2>&1
    $1 BF.LOADCHUNK p abc d 2>&1
    $1 BF.ADD p 2>&1
    $1 BF.RESERVE p 2>&1
    $1 BF.RESERVE pn 0.01 2 NONSCALING 2>&1
    $1 BF.ADD pn n1 2>&1; $1 BF.ADD pn n2 2>&1; $1 BF.ADD pn n3 2>&1
    $1 SET pstr v 2>&1
    $1 BF.ADD pstr x 2>&1
    $1 BF.SCANDUMP pstr 0 2>&1
    $1 BF.LOADCHUNK pstr 1 zz 2>&1
    $1 BF.EXISTS pstr x 2>&1
    $1 BF.MEXISTS pstr x y 2>&1
    $1 BF.RESERVE psub 5e-324 1 2>&1
    $1 BF.INSERT psub ERROR 5e-324 ITEMS x 2>&1
    $1 BF.RESERVE psub -1 100 2>&1
    $1 BF.INSERT psub ERROR -1 ITEMS a 2>&1
    $1 BF.INSERT psub ERROR 0 ITEMS a 2>&1
    $1 BF.INSERT psub ERROR 2 ITEMS a 2>&1
    $1 BF.RESERVE psub 0.01 100 X EXPANSION 2>&1
    $1 BF.INSERT psub CAPACITY 2>&1
    $1 BF.LOADCHUNK pez 1 "" 2>&1
    $1 EXISTS pez 2>&1
    $1 BF.RESERVE po1 0.01 100 BOGUS 2>&1
    $1 BF.RESERVE po2 0.01 100 BOGUS BOGUS 2>&1
    $1 BF.RESERVE po3 0.01 100 NONSCALING NONSCALING 2>&1
    $1 BF.RESERVE po4 0.01 100 EXPANSION 2 EXPANSION 3 2>&1
    $1 BF.RESERVE po5 0.01 100 EXPANSION 2 EXPANSION 2>&1
    $1 BF.INFO po5 EXPANSION 2>&1
    $1 BF.RESERVE po6 0.01 100 X EXPANSION 3 2>&1
    $1 BF.INFO po6 EXPANSION 2>&1
    $1 BF.RESERVE po7 0.01 100 EXPANSION 3 X 2>&1
    $1 BF.INFO po7 EXPANSION 2>&1
    $1 BF.INSERT po8 CAPACITY 200 CAPACITY 300 ITEMS x 2>&1
    $1 BF.INFO po8 CAPACITY 2>&1
    $1 BF.INSERT po9 EXPANSION 2 EXPANSION 3 ITEMS x 2>&1
    $1 BF.INFO po9 EXPANSION 2>&1
  }
  a=$(errs "$R8" | grep . | paste -sd'|'); b=$(errs "$K" | grep . | paste -sd'|')
  chk "error text parity"               "$a" "$b"
  mfull() {
    $1 DEL pmf >/dev/null 2>&1
    $1 BF.RESERVE pmf 0.01 2 NONSCALING >/dev/null 2>&1
    $1 BF.MADD pmf p q r s 2>&1 | grep . | paste -sd,
    $1 BF.MADD pmf z 2>&1 | grep . | paste -sd,
    $1 DEL pmf2 >/dev/null 2>&1
    $1 BF.RESERVE pmf2 0.01 2 NONSCALING >/dev/null 2>&1
    $1 BF.INSERT pmf2 ITEMS p q r s 2>&1 | grep . | paste -sd,
    $1 DEL pmf3 >/dev/null 2>&1
    $1 BF.RESERVE pmf3 0.01 2 NONSCALING >/dev/null 2>&1
    $1 BF.MADD pmf3 p p q r 2>&1 | grep . | paste -sd,
  }
  chk "a full NONSCALING array parity"  "$(mfull "$R8" | paste -sd'|')" "$(mfull "$K" | paste -sd'|')"
  for R in "$K" "$R8"; do $R DEL pmf pmf2 pmf3 psub >/dev/null 2>&1; done
  expl() { $1 BF.RESERVE px 0.01 100 EXPANSION -1 2>&1; $1 BF.RESERVE px 0.01 100 EXPANSION 99999 2>&1; }
  chk "BF.RESERVE expansion range parity" "$(expl "$R8" | grep . | paste -sd'|')" "$(expl "$K" | grep . | paste -sd'|')"
  for R in "$K" "$R8"; do $R DEL px >/dev/null 2>&1; done
  for R in "$K" "$R8"; do
    $R DEL pe pn pstr pez po1 po2 po3 po4 po5 po6 po7 po8 po9 >/dev/null 2>&1
  done

  # measured false-positive rate: 100k inserts, 100k misses, on each server
  N=100000
  fill() { awk -v n="$N" 'BEGIN{for(i=0;i<n;i++){k="item-" i; printf "*3\r\n$6\r\nBF.ADD\r\n$2\r\nfp\r\n$%d\r\n%s\r\n", length(k), k}}'; }
  probe() { awk -v n="$N" 'BEGIN{for(b=0;b<20;b++){line="BF.MEXISTS fp"; for(i=0;i<n/20;i++) line=line " absent-" (b*n/20+i); print line}}'; }
  $R8 BF.RESERVE fp 0.01 "$N" >/dev/null; $K BF.RESERVE fp 0.01 "$N" >/dev/null
  fill | $R8 --pipe >/dev/null 2>&1; fill | $K --pipe >/dev/null 2>&1
  chk "redis 8 holds $N items in one filter" "1" "$($R8 BF.INFO fp FILTERS)"
  chk "the daemon holds $N items in one filter" "1" "$($K BF.INFO fp FILTERS)"
  ar=$(probe | $R8 | grep -c '^1$'); br=$(probe | $K | grep -c '^1$')
  chk "redis 8 false positives $ar/$N under 2%"  "yes" "$(yn [ "$ar" -lt 2000 ])"
  chk "daemon false positives $br/$N under 2%"   "yes" "$(yn [ "$br" -lt 2000 ])"
  within=no
  if [ "$br" -le "$((2*ar+1))" ] && [ "$ar" -le "$((2*br+1))" ]; then within=yes; fi
  chk "the two rates are within 2x"              "yes" "$within"
  for R in "$K" "$R8"; do $R DEL p pm pg fp pingf >/dev/null 2>&1; done
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
