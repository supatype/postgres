#!/usr/bin/env bash
# Cuckoo filters: CF.RESERVE/ADD/ADDNX/INSERT/INSERTNX/EXISTS/MEXISTS/DEL/COUNT/
# INFO/SCANDUMP/LOADCHUNK on $RESP, plus reply parity against a real Redis 8 on $REDIS8.
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
# CF.INFO has no per-field form: read the value on the line after the field name.
cfi() { $1 CF.INFO "$2" 2>&1 | awk -v f="$3" '$0==f{getline; print; exit}'; }
DUMP=/tmp/pgks_cuckoo_dump.bin
CHUNK=/tmp/pgks_cuckoo_chunk.bin
trap 'rm -f "$DUMP" "$CHUNK"' EXIT

echo "# cuckoo filters"
$K DEL c auto ins nx bs mi ex str cp rn dsrc ddst tk mq dbf grow lc r3 nf >/dev/null 2>&1

# CF.RESERVE / CF.ADD / CF.EXISTS / CF.COUNT
chk "CF.RESERVE -> OK"                  "OK"  "$($K CF.RESERVE c 100)"
chk "CF.ADD new item -> 1"              "1"   "$($K CF.ADD c alpha)"
chk "CF.ADD the same item -> 1"         "1"   "$($K CF.ADD c alpha)"
chk "CF.COUNT counts both copies"       "2"   "$($K CF.COUNT c alpha)"
chk "CF.EXISTS added -> 1"              "1"   "$($K CF.EXISTS c alpha)"
chk "CF.EXISTS never added -> 0"        "0"   "$($K CF.EXISTS c nope)"
chk "CF.EXISTS on a missing key -> 0"   "0"   "$($K CF.EXISTS gone alpha)"
chk "CF.COUNT on a missing key -> 0"    "0"   "$($K CF.COUNT gone alpha)"
chk "CF.MEXISTS mixed -> 1,0"           "1,0" "$($K CF.MEXISTS c alpha nope | paste -sd,)"

# CF.ADDNX
chk "CF.ADDNX a new item -> 1"          "1"   "$($K CF.ADDNX c beta)"
chk "CF.ADDNX a present item -> 0"      "0"   "$($K CF.ADDNX c beta)"
chk "CF.ADDNX did not add a copy"       "1"   "$($K CF.COUNT c beta)"

# CF.DEL
chk "CF.DEL removes one copy -> 1"      "1"   "$($K CF.DEL c alpha)"
chk "CF.COUNT dropped by one"           "1"   "$($K CF.COUNT c alpha)"
chk "CF.EXISTS still finds the copy"    "1"   "$($K CF.EXISTS c alpha)"
chk "CF.DEL removes the last copy"      "1"   "$($K CF.DEL c alpha)"
chk "CF.EXISTS is 0 after the last DEL" "0"   "$($K CF.EXISTS c alpha)"
chk "CF.DEL of an absent item -> 0"     "0"   "$($K CF.DEL c alpha)"
chk "CF.DEL on a missing key"           "Not found" "$($K CF.DEL gone alpha 2>&1)"

# CF.ADD creates the key with the RedisBloom defaults
chk "CF.ADD creates a filter"           "1"   "$($K CF.ADD auto x)"
chk "default bucket size is 2"          "2"   "$(cfi "$K" auto 'Bucket size')"
chk "default expansion is 1"            "1"   "$(cfi "$K" auto 'Expansion rate')"
chk "default max iterations is 20"      "20"  "$(cfi "$K" auto 'Max iterations')"
chk "default capacity is 1024 buckets"  "512" "$(cfi "$K" auto 'Number of buckets')"
$K DEL auto >/dev/null

# CF.INFO field names and order
chk "CF.INFO has eight fields"          "8" "$($K CF.INFO c | grep -c '^[A-Z]')"
chk "CF.INFO field names"               "Size,Number of buckets,Number of filters,Number of items inserted,Number of items deleted,Bucket size,Expansion rate,Max iterations" \
                                        "$($K CF.INFO c | grep '^[A-Z]' | paste -sd,)"
chk "CF.INFO counts the live items"     "1" "$(cfi "$K" c 'Number of items inserted')"
chk "CF.INFO counts the deletes"        "2" "$(cfi "$K" c 'Number of items deleted')"
chk "CF.INFO SIZE is positive"          "yes" "$(yn [ "$(cfi "$K" c 'Size')" -gt 0 ])"

# capacity rounds up to a power of two of buckets
for pair in "100 64" "1000 512" "1024 512" "1500 1024" "4 2" "5 2"; do
  set -- $pair
  $K DEL cap >/dev/null
  $K CF.RESERVE cap "$1" >/dev/null
  chk "capacity $1 -> $2 buckets"       "$2" "$(cfi "$K" cap 'Number of buckets')"
done
$K DEL cap >/dev/null

# CF.RESERVE options
chk "CF.RESERVE BUCKETSIZE 4"           "OK" "$($K CF.RESERVE bs 100 BUCKETSIZE 4)"
chk "BUCKETSIZE 4 is reported"          "4"  "$(cfi "$K" bs 'Bucket size')"
chk "BUCKETSIZE 4 halves the buckets"   "32" "$(cfi "$K" bs 'Number of buckets')"
chk "CF.RESERVE MAXITERATIONS 7"        "OK" "$($K CF.RESERVE mi 100 MAXITERATIONS 7)"
chk "MAXITERATIONS 7 is reported"       "7"  "$(cfi "$K" mi 'Max iterations')"
chk "CF.RESERVE EXPANSION 4"            "OK" "$($K CF.RESERVE ex 100 EXPANSION 4)"
chk "EXPANSION 4 is reported"           "4"  "$(cfi "$K" ex 'Expansion rate')"

# CF.INSERT / CF.INSERTNX and their options
chk "CF.INSERT ITEMS -> 1,1,1"          "1,1,1" "$($K CF.INSERT ins ITEMS a b a | paste -sd,)"
chk "CF.INSERT added both copies"       "2"     "$($K CF.COUNT ins a)"
chk "CF.INSERTNX dup + new -> 0,1"      "0,1"   "$($K CF.INSERTNX ins ITEMS a c | paste -sd,)"
chk "CF.INSERT NOCREATE on a filter"    "1"     "$($K CF.INSERT ins NOCREATE ITEMS e)"
chk "CF.INSERT CAPACITY creates"        "1"     "$($K CF.INSERT nx CAPACITY 2000 ITEMS z)"
chk "CF.INSERT CAPACITY was applied"    "1024"  "$(cfi "$K" nx 'Number of buckets')"
$K DEL ins nx >/dev/null

# error texts
chk "CF.RESERVE on an existing key"     "ERR item exists" "$($K CF.RESERVE c 100 2>&1)"
chk "CF.RESERVE bad capacity"           "Bad capacity" "$($K CF.RESERVE q abc 2>&1)"
chk "CF.RESERVE capacity out of range"  "Capacity must be in the range [2 * BUCKETSIZE, 1073741824]" \
                                        "$($K CF.RESERVE q 0 2>&1)"
chk "CF.RESERVE capacity below 2*bs"    "Capacity must be in the range [2 * BUCKETSIZE, 1073741824]" \
                                        "$($K CF.RESERVE q 4 BUCKETSIZE 8 2>&1)"
chk "CF.RESERVE bad bucket size"        "Couldn't parse BUCKETSIZE" "$($K CF.RESERVE q 100 BUCKETSIZE abc 2>&1)"
chk "CF.RESERVE bucket size range"      "BUCKETSIZE: value must be in the range [1, 255]" \
                                        "$($K CF.RESERVE q 100 BUCKETSIZE 0 2>&1)"
chk "CF.RESERVE bad max iterations"     "Couldn't parse MAXITERATIONS" "$($K CF.RESERVE q 100 MAXITERATIONS abc 2>&1)"
chk "CF.RESERVE max iterations range"   "MAXITERATIONS: value must be in the range [1, 65535]" \
                                        "$($K CF.RESERVE q 100 MAXITERATIONS 0 2>&1)"
chk "CF.RESERVE bad expansion"          "Couldn't parse EXPANSION" "$($K CF.RESERVE q 100 EXPANSION abc 2>&1)"
chk "CF.RESERVE expansion range"        "EXPANSION: value must be in the range [0, 32768]" \
                                        "$($K CF.RESERVE q 100 EXPANSION -1 2>&1)"
chk "CF.RESERVE a dangling BUCKETSIZE"  "ERR wrong number of arguments for 'cf.reserve' command" \
                                        "$($K CF.RESERVE q 100 BUCKETSIZE 2>&1)"
chk "CF.INFO on a missing key"          "ERR not found" "$($K CF.INFO gone 2>&1)"
chk "CF.INSERT NOCREATE on a missing key" "ERR not found" "$($K CF.INSERT gone NOCREATE ITEMS a 2>&1)"
chk "CF.INSERTNX NOCREATE on a missing key" "ERR not found" "$($K CF.INSERTNX gone NOCREATE ITEMS a 2>&1)"
chk "CF.INSERT bad capacity"            "Bad capacity" "$($K CF.INSERT q CAPACITY abc ITEMS a 2>&1)"
chk "CF.INSERT capacity out of range"   "Capacity must be in the range [cf-bucket-size * 2, 1073741824]" \
                                        "$($K CF.INSERT q CAPACITY 0 ITEMS a 2>&1)"
chk "CF.INSERT unknown argument"        "Unknown argument received" "$($K CF.INSERT q BOGUS ITEMS a 2>&1)"
chk "CF.INSERT rejects BUCKETSIZE"      "Unknown argument received" "$($K CF.INSERT q BUCKETSIZE 4 ITEMS a 2>&1)"
chk "CF.INSERT with no ITEMS"           "ERR wrong number of arguments for 'cf.insert' command" \
                                        "$($K CF.INSERT q CAPACITY 100 2>&1)"
chk "CF.SCANDUMP bad iterator"          "Invalid position" "$($K CF.SCANDUMP c abc 2>&1)"
chk "CF.SCANDUMP negative iterator"     "Invalid position" "$($K CF.SCANDUMP c -5 2>&1)"
chk "CF.SCANDUMP on a missing key"      "ERR not found" "$($K CF.SCANDUMP gone 0 2>&1)"
chk "CF.LOADCHUNK bad iterator"         "Invalid position" "$($K CF.LOADCHUNK lc abc d 2>&1)"
chk "CF.LOADCHUNK iterator 0"           "Invalid position" "$($K CF.LOADCHUNK lc 0 d 2>&1)"
chk "CF.LOADCHUNK of a foreign chunk"   "Invalid header" "$($K CF.LOADCHUNK lc 1 not-a-filter 2>&1)"
chk "CF.ADD wrong arity"                "ERR wrong number of arguments for 'cf.add' command" "$($K CF.ADD c 2>&1)"
chk "CF.ADDNX wrong arity"              "ERR wrong number of arguments for 'cf.addnx' command" "$($K CF.ADDNX c 2>&1)"
chk "CF.DEL wrong arity"                "ERR wrong number of arguments for 'cf.del' command" "$($K CF.DEL c 2>&1)"
chk "CF.COUNT wrong arity"              "ERR wrong number of arguments for 'cf.count' command" "$($K CF.COUNT c 2>&1)"
chk "CF.INFO wrong arity"               "ERR wrong number of arguments for 'cf.info' command" "$($K CF.INFO c BOGUS 2>&1)"
chk "CF.RESERVE wrong arity"            "ERR wrong number of arguments for 'cf.reserve' command" "$($K CF.RESERVE c 2>&1)"

# a filter with EXPANSION 0 fills up and refuses more
$K CF.RESERVE nf 128 EXPANSION 0 >/dev/null
full=$(awk 'BEGIN{for(i=0;i<400;i++) printf "CF.ADD nf z-%d\n", i}' | $K 2>&1 | grep -c 'Filter is full')
chk "a full EXPANSION 0 filter refuses" "yes" "$(yn [ "$full" -gt 0 ])"
chk "CF.INSERT on a full filter -> -1"  "-1" "$($K CF.INSERT nf ITEMS over-and-out)"
chk "CF.INSERTNX on a full filter -> -1" "-1" "$($K CF.INSERTNX nf ITEMS over-and-out)"
chk "the full filter stayed one filter" "1" "$(cfi "$K" nf 'Number of filters')"

# WRONGTYPE both ways, TYPE and OBJECT ENCODING
$K SET str v >/dev/null
chk "CF.ADD on a string -> WRONGTYPE"   "1" "$($K CF.ADD str x 2>&1 | grep -c WRONGTYPE)"
chk "CF.ADDNX on a string -> WRONGTYPE" "1" "$($K CF.ADDNX str x 2>&1 | grep -c WRONGTYPE)"
chk "CF.INSERT on a string -> WRONGTYPE" "1" "$($K CF.INSERT str ITEMS x 2>&1 | grep -c WRONGTYPE)"
chk "CF.INFO on a string -> WRONGTYPE"  "1" "$($K CF.INFO str 2>&1 | grep -c WRONGTYPE)"
chk "CF.RESERVE on a string -> WRONGTYPE" "1" "$($K CF.RESERVE str 100 2>&1 | grep -c WRONGTYPE)"
chk "CF.EXISTS on a string -> 0"        "0" "$($K CF.EXISTS str x 2>&1)"
chk "CF.COUNT on a string -> 0"         "0" "$($K CF.COUNT str x 2>&1)"
chk "CF.DEL on a string -> Not found"   "Not found" "$($K CF.DEL str x 2>&1)"
chk "GET on a filter -> WRONGTYPE"      "1" "$($K GET c 2>&1 | grep -c WRONGTYPE)"
chk "SADD on a filter -> WRONGTYPE"     "1" "$($K SADD c x 2>&1 | grep -c WRONGTYPE)"
chk "BF.ADD on a cuckoo filter"         "1" "$($K BF.ADD c x 2>&1 | grep -c WRONGTYPE)"
chk "TYPE reports MBbloomCF"            "MBbloomCF" "$($K TYPE c)"
chk "OBJECT ENCODING reports raw"       "raw" "$($K OBJECT ENCODING c)"

# EXPIRE / TTL, kept by an add, by a delete and by growth
$K CF.RESERVE tk 4 >/dev/null
$K EXPIRE tk 100 >/dev/null
chk "EXPIRE on a filter -> TTL"         "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
$K CF.ADD tk t1 >/dev/null
chk "an add keeps the TTL"              "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
$K CF.DEL tk t1 >/dev/null; $K CF.ADD tk t1 >/dev/null
chk "a delete keeps the TTL"            "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
for i in 2 3 4 5 6 7 8 9 10 11 12; do $K CF.ADD tk "t$i" >/dev/null; done
chk "growth keeps the TTL"              "yes" "$(yn [ "$($K TTL tk)" -ge 90 ])"
chk "growth added sub-filters"          "yes" "$(yn [ "$(cfi "$K" tk 'Number of filters')" -gt 1 ])"
chk "the grown filter kept its items"   "1,1,1" "$($K CF.MEXISTS tk t1 t6 t12 | paste -sd,)"
$K PERSIST tk >/dev/null
chk "PERSIST clears the TTL"            "-1" "$($K TTL tk)"

# COPY and RENAME keep the type and the contents
$K DEL cp rn >/dev/null
chk "COPY a filter -> 1"                "1" "$($K COPY c cp)"
chk "the copy is a filter"              "MBbloomCF" "$($K TYPE cp)"
chk "the copy answers CF.EXISTS"        "1" "$($K CF.EXISTS cp beta)"
chk "RENAME a filter -> OK"             "OK" "$($K RENAME cp rn)"
chk "the renamed key is a filter"       "MBbloomCF" "$($K TYPE rn)"
chk "the renamed key answers CF.EXISTS" "1" "$($K CF.EXISTS rn beta)"
$K DEL rn >/dev/null

# CF.ADD inside MULTI/EXEC
chk "CF.ADD queues and runs in MULTI"   "OK,QUEUED,QUEUED,QUEUED,1,1,1" \
    "$(printf 'MULTI\nCF.ADD mq v1\nCF.EXISTS mq v1\nCF.DEL mq v1\nEXEC\n' | $K | paste -sd,)"

# SCANDUMP / LOADCHUNK round trip, driven the way a RedisBloom client drives it:
# the iterator SCANDUMP returns is the iterator LOADCHUNK is given.
$K CF.RESERVE dsrc 200 >/dev/null
for i in 1 2 3 4 5 6 7 8; do $K CF.ADD dsrc "dump-$i" >/dev/null; done
it=0; loaded=""; chunks=0
while :; do
  $K CF.SCANDUMP dsrc "$it" > "$DUMP" 2>&1
  it=$(head -1 "$DUMP")
  case "$it" in ''|*[!0-9]*) loaded="$it"; break;; esac
  [ "$it" -eq 0 ] && break
  tot=$(wc -c < "$DUMP")
  tail -c +$(( ${#it} + 2 )) "$DUMP" | head -c $(( tot - ${#it} - 2 )) > "$CHUNK"
  loaded=$($K -x CF.LOADCHUNK ddst "$it" < "$CHUNK" 2>&1)
  [ "$loaded" = "OK" ] || break
  chunks=$((chunks+1))
  [ "$chunks" -gt 8 ] && break
done
chk "CF.LOADCHUNK accepts a SCANDUMP chunk" "OK" "$loaded"
chk "the restored filter is a filter"   "MBbloomCF" "$($K TYPE ddst)"
chk "the restored filter has the items" "1,1,1,0" "$($K CF.MEXISTS ddst dump-1 dump-5 dump-8 dump-none | paste -sd,)"
chk "the restored filter has the count" "$(cfi "$K" dsrc 'Number of items inserted')" \
                                        "$(cfi "$K" ddst 'Number of items inserted')"
chk "CF.LOADCHUNK onto a live filter"   "ERR item exists" "$($K CF.LOADCHUNK ddst 1 anything 2>&1)"

# one filter is one key, before and after growth
before=$($K DBSIZE)
$K CF.RESERVE dbf 8 >/dev/null
for i in $(seq 1 40); do $K CF.ADD dbf "k$i" >/dev/null; done
chk "a filter is one key (DBSIZE +1)"   "1" "$(( $($K DBSIZE) - before ))"
chk "DEL removes the whole filter"      "1" "$($K DEL dbf)"

# growth: 300 items into a capacity-100 filter, every item survives
$K CF.RESERVE grow 100 >/dev/null
awk 'BEGIN{for(i=0;i<300;i++) printf "CF.ADD grow grow-%d\n", i}' | $K >/dev/null
chk "growth added sub-filters"          "yes" "$(yn [ "$(cfi "$K" grow 'Number of filters')" -gt 1 ])"
chk "growth counted every item"         "300" "$(cfi "$K" grow 'Number of items inserted')"
chk "growth is still one key"           "MBbloomCF" "$($K TYPE grow)"
hit=$(awk 'BEGIN{line="CF.MEXISTS grow"; for(i=0;i<300;i++) line=line " grow-" i; print line}' | $K | grep -c '^1$')
chk "all 300 items survive growth"      "300" "$hit"

# CF.DEL on a sparse filter removes exactly the items it names
$K DEL dgrow >/dev/null; $K CF.RESERVE dgrow 100000 >/dev/null
awk 'BEGIN{for(i=0;i<300;i++) printf "CF.ADD dgrow d-%d\n", i}' | $K >/dev/null
gone=$(awk 'BEGIN{for(i=0;i<150;i++) printf "CF.DEL dgrow d-%d\n", i}' | $K | grep -c '^1$')
chk "CF.DEL removes 150 of them"        "150" "$gone"
back=$(awk 'BEGIN{line="CF.MEXISTS dgrow"; for(i=0;i<150;i++) line=line " d-" i; print line}' | $K | grep -c '^1$')
chk "no deleted item reads back"        "0" "$back"
kept=$(awk 'BEGIN{line="CF.MEXISTS dgrow"; for(i=150;i<300;i++) line=line " d-" i; print line}' | $K | grep -c '^1$')
chk "the other 150 are untouched"       "150" "$kept"
chk "CF.INFO counts 150 deletes"        "150" "$(cfi "$K" dgrow 'Number of items deleted')"
chk "CF.INFO counts 150 live items"     "150" "$(cfi "$K" dgrow 'Number of items inserted')"
$K DEL dgrow >/dev/null

# RESP3 reply shapes (redis-cli -3 parses them, so a malformed frame errors)
$K DEL r3 >/dev/null
chk "RESP3 CF.ADD is a boolean"         "true"  "$($K3 CF.ADD r3 z | tr -d '()')"
chk "RESP3 CF.ADD again is a boolean"   "true"  "$($K3 CF.ADD r3 z | tr -d '()')"
chk "RESP3 CF.ADDNX is a boolean"       "false" "$($K3 CF.ADDNX r3 z | tr -d '()')"
chk "RESP3 CF.EXISTS is a boolean"      "true"  "$($K3 CF.EXISTS r3 z | tr -d '()')"
chk "RESP3 CF.MEXISTS is an array"      "true,false" "$($K3 CF.MEXISTS r3 z q | tr -d '()' | paste -sd,)"
chk "RESP3 CF.COUNT is an integer"      "2" "$($K3 CF.COUNT r3 z)"
chk "RESP3 CF.DEL is a boolean"         "true"  "$($K3 CF.DEL r3 z | tr -d '()')"
chk "RESP3 CF.INSERT is an array"       "true" "$($K3 CF.INSERT r3 ITEMS w | tr -d '()')"
chk "RESP3 CF.INSERTNX is an integer"   "0" "$($K3 CF.INSERTNX r3 ITEMS w)"
chk "RESP3 CF.INFO is a map"            "Size" "$($K3 CF.INFO r3 | head -1 | awk '{print $1}')"

$K DEL c auto ins nx bs mi ex str dsrc ddst tk mq grow lc r3 nf >/dev/null 2>&1

# ---- parity against Redis 8 -------------------------------------------------
R8="redis-cli -p $REDIS8"
if ! $R8 PING 2>/dev/null | grep -q PONG; then
  echo "  SKIP  no redis 8 on :$REDIS8 for parity"
elif $R8 CF.RESERVE pingf 100 2>&1 | grep -qi 'unknown command'; then
  echo "  SKIP  no cuckoo filters on :$REDIS8 for parity"
else
  echo "# parity vs redis 8 on :$REDIS8"
  for R in "$K" "$R8"; do $R DEL p pm pd pn pe pstr fp pingf >/dev/null 2>&1; done

  a=$($R8 CF.RESERVE p 1000); b=$($K CF.RESERVE p 1000)
  chk "CF.RESERVE parity"               "$a" "$b"
  adds() { awk 'BEGIN{for(i=0;i<200;i++) printf "CF.ADD p p-%d\n", i}'; }
  a=$(adds | $R8 | paste -sd,); b=$(adds | $K | paste -sd,)
  chk "200 CF.ADD replies parity"       "$a" "$b"
  hits() { awk -v p="$1" 'BEGIN{line="CF.MEXISTS p"; for(i=0;i<200;i++) line=line " " p "-" i; print line}'; }
  a=$(hits p | $R8 | paste -sd,); b=$(hits p | $K | paste -sd,)
  chk "CF.MEXISTS on 200 members parity" "$a" "$b"
  counts() { awk 'BEGIN{for(i=0;i<200;i++) printf "CF.COUNT p p-%d\n", i}'; }
  a=$(counts | $R8 | paste -sd,); b=$(counts | $K | paste -sd,)
  chk "CF.COUNT on 200 members parity"  "$a" "$b"
  dels() { awk 'BEGIN{for(i=0;i<200;i++) printf "CF.DEL p p-%d\n", i}'; }
  a=$(dels | $R8 | paste -sd,); b=$(dels | $K | paste -sd,)
  chk "CF.DEL on 200 members parity"    "$a" "$b"
  a=$(hits p | $R8 | paste -sd,); b=$(hits p | $K | paste -sd,)
  chk "CF.MEXISTS after CF.DEL parity"  "$a" "$b"
  chk "CF.DEL of an absent item parity" "$($R8 CF.DEL p never 2>&1)" "$($K CF.DEL p never 2>&1)"

  for f in 'Bucket size' 'Expansion rate' 'Max iterations' 'Number of items inserted' \
           'Number of items deleted' 'Number of buckets'; do
    chk "CF.INFO $f parity"             "$(cfi "$R8" p "$f")" "$(cfi "$K" p "$f")"
  done
  for cap in 100 1000 1024 1500; do
    for R in "$K" "$R8"; do $R DEL pc >/dev/null 2>&1; $R CF.RESERVE pc "$cap" >/dev/null 2>&1; done
    chk "capacity $cap rounds the same"  "$(cfi "$R8" pc 'Number of buckets')" "$(cfi "$K" pc 'Number of buckets')"
  done
  for R in "$K" "$R8"; do $R DEL pc >/dev/null 2>&1; done

  mix() { $1 CF.INSERT pm ITEMS m1 m2 m1 2>&1 | paste -sd,; $1 CF.INSERTNX pm ITEMS m1 m3 2>&1 | paste -sd,; }
  chk "CF.INSERT + CF.INSERTNX parity"  "$(mix "$R8")" "$(mix "$K")"
  chk "CF.ADDNX parity"                 "$($R8 CF.ADDNX pd d1 2>&1),$($R8 CF.ADDNX pd d1 2>&1)" \
                                        "$($K CF.ADDNX pd d1 2>&1),$($K CF.ADDNX pd d1 2>&1)"
  chk "TYPE parity"                     "$($R8 TYPE p)" "$($K TYPE p)"
  chk "OBJECT ENCODING parity"          "$($R8 OBJECT ENCODING p)" "$($K OBJECT ENCODING p)"

  # error texts, reply for reply
  errs() {
    $1 CF.RESERVE p 100 2>&1
    $1 CF.RESERVE pe abc 2>&1
    $1 CF.RESERVE pe 0 2>&1
    $1 CF.RESERVE pe 4 BUCKETSIZE 8 2>&1
    $1 CF.RESERVE pe 1073741825 2>&1
    $1 CF.RESERVE pe 100 BUCKETSIZE abc 2>&1
    $1 CF.RESERVE pe 100 BUCKETSIZE 0 2>&1
    $1 CF.RESERVE pe 100 BUCKETSIZE 256 2>&1
    $1 CF.RESERVE pe 100 MAXITERATIONS abc 2>&1
    $1 CF.RESERVE pe 100 MAXITERATIONS 0 2>&1
    $1 CF.RESERVE pe 100 MAXITERATIONS 65536 2>&1
    $1 CF.RESERVE pe 100 EXPANSION abc 2>&1
    $1 CF.RESERVE pe 100 EXPANSION -1 2>&1
    $1 CF.RESERVE pe 100 EXPANSION 32769 2>&1
    $1 CF.RESERVE pe 100 BUCKETSIZE 2>&1
    $1 CF.RESERVE pe 100 MAXITERATIONS 2>&1
    $1 CF.RESERVE pe 100 EXPANSION 2>&1
    $1 CF.INSERT pe NOCREATE ITEMS a 2>&1
    $1 CF.INSERTNX pe NOCREATE ITEMS a 2>&1
    $1 CF.INSERT pe CAPACITY abc ITEMS a 2>&1
    $1 CF.INSERT pe CAPACITY 0 ITEMS a 2>&1
    $1 CF.INSERT pe BOGUS ITEMS a 2>&1
    $1 CF.INSERT pe BUCKETSIZE 4 ITEMS a 2>&1
    $1 CF.INSERT pe CAPACITY 100 2>&1
    $1 CF.INSERT pe ITEMS 2>&1
    $1 CF.INFO pe 2>&1
    $1 CF.INFO p BOGUS 2>&1
    $1 CF.SCANDUMP p abc 2>&1
    $1 CF.SCANDUMP p -5 2>&1
    $1 CF.SCANDUMP pe 0 2>&1
    $1 CF.LOADCHUNK pe abc d 2>&1
    $1 CF.LOADCHUNK pe 0 d 2>&1
    $1 CF.LOADCHUNK pe 1 not-a-filter 2>&1
    $1 CF.LOADCHUNK p 1 not-a-filter 2>&1
    $1 CF.ADD p 2>&1
    $1 CF.ADDNX p 2>&1
    $1 CF.DEL p 2>&1
    $1 CF.COUNT p 2>&1
    $1 CF.EXISTS p 2>&1
    $1 CF.MEXISTS p 2>&1
    $1 CF.INFO 2>&1
    $1 CF.RESERVE p 2>&1
    $1 CF.INSERT p 2>&1
    $1 CF.INSERTNX p 2>&1
    $1 CF.SCANDUMP p 2>&1
    $1 CF.LOADCHUNK p 1 2>&1
    $1 CF.DEL pe absent 2>&1
    $1 SET pstr v 2>&1
    $1 CF.ADD pstr x 2>&1
    $1 CF.ADDNX pstr x 2>&1
    $1 CF.INSERT pstr ITEMS x 2>&1
    $1 CF.INFO pstr 2>&1
    $1 CF.RESERVE pstr 100 2>&1
    $1 CF.SCANDUMP pstr 0 2>&1
    $1 CF.EXISTS pstr x 2>&1
    $1 CF.COUNT pstr x 2>&1
    $1 CF.DEL pstr x 2>&1
  }
  a=$(errs "$R8" | grep . | paste -sd'|'); b=$(errs "$K" | grep . | paste -sd'|')
  chk "error text parity"               "$a" "$b"
  for R in "$K" "$R8"; do $R DEL pe pstr >/dev/null 2>&1; done

  # a full EXPANSION 0 filter answers the same way on both
  fulls() {
    $1 DEL pn >/dev/null 2>&1
    $1 CF.RESERVE pn 128 EXPANSION 0 >/dev/null 2>&1
    awk 'BEGIN{for(i=0;i<400;i++) printf "CF.ADD pn f-%d\n", i}' | $1 2>&1 | sort | uniq -c | awk '{print $1, $2, $3, $4}'
    $1 CF.INSERT pn ITEMS over 2>&1
    $1 CF.INSERTNX pn ITEMS over 2>&1
  }
  chk "a full filter answers the same"  "$(fulls "$R8" | tail -3)" "$(fulls "$K" | tail -3)"
  for R in "$K" "$R8"; do $R DEL pn >/dev/null 2>&1; done

  # measured false-positive rate: 100k inserts, 100k misses, on each server
  N=100000
  fill() { awk -v n="$N" 'BEGIN{for(i=0;i<n;i++){k="item-" i; printf "*3\r\n$6\r\nCF.ADD\r\n$2\r\nfp\r\n$%d\r\n%s\r\n", length(k), k}}'; }
  probe() { awk -v n="$N" 'BEGIN{for(b=0;b<20;b++){line="CF.MEXISTS fp"; for(i=0;i<n/20;i++) line=line " absent-" (b*n/20+i); print line}}'; }
  $R8 CF.RESERVE fp "$N" >/dev/null; $K CF.RESERVE fp "$N" >/dev/null
  fill | $R8 --pipe >/dev/null 2>&1; fill | $K --pipe >/dev/null 2>&1
  chk "redis 8 took all $N items"       "$N" "$(cfi "$R8" fp 'Number of items inserted')"
  chk "the daemon took all $N items"    "$N" "$(cfi "$K" fp 'Number of items inserted')"
  ar=$(probe | $R8 | grep -c '^1$'); br=$(probe | $K | grep -c '^1$')
  chk "redis 8 false positives $ar/$N under 3%" "yes" "$(yn [ "$ar" -lt 3000 ])"
  chk "daemon false positives $br/$N under 3%"  "yes" "$(yn [ "$br" -lt 3000 ])"
  within=no
  if [ "$br" -le "$((2*ar+1))" ] && [ "$ar" -le "$((2*br+1))" ]; then within=yes; fi
  chk "the two rates are within 2x"     "yes" "$within"
  for R in "$K" "$R8"; do $R DEL p pm pd fp pingf >/dev/null 2>&1; done
fi

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
