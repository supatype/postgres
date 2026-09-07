#!/usr/bin/env bash
# P3 §5 — native large-sorted-set structure. Past a threshold a zset is stored
# as a member->score bucket table (O(1) ZSCORE) plus a pre-sorted offset array
# (O(log n) ZRANK/ZRANGEBYSCORE, O(k) ZRANGE) instead of re-sorting on every op.
# Proves a 10k-member zset (with score ties, to exercise (score,member) order) is
# byte-for-byte Redis-compatible over the read surface.
set -u
DAEMON=${DAEMON:-/home/user/postgres/extensions/pg_keyspace/poc/target/release/pgkeyspaced}
PORT=${PORT:-6398}
REDIS=${REDIS:-6379}
N=${N:-10000}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-50s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-50s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

[ -x "$DAEMON" ] || { echo "  SKIP  daemon not built at $DAEMON"; exit 0; }
redis-cli -p "$REDIS" PING 2>/dev/null | grep -q PONG || { echo "  SKIP  no redis on :$REDIS"; exit 0; }

"$DAEMON" --workers 1 --port "$PORT" --keys-per-worker 100000 --shmem-mb 128 >/tmp/pgks_bigzset.log 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null' EXIT
for _ in $(seq 1 20); do redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done
redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  daemon did not start"; exit 1; }

echo "# P3 native large-zset (§5) — $N members with ties, parity vs redis :$REDIS"
redis-cli -p "$REDIS" DEL bz >/dev/null; redis-cli -p "$PORT" DEL bz >/dev/null
# score = i % 50 -> heavy ties, so (score, member) tie-breaking is exercised
build() { awk -v n="$N" 'BEGIN{for(i=0;i<n;i++) printf "ZADD bz %d m%06d\n", i%50, i}'; }
build | redis-cli -p "$REDIS" >/dev/null
build | redis-cli -p "$PORT"  >/dev/null

chk "ZCARD matches redis"        "$(redis-cli -p $REDIS ZCARD bz)" "$(redis-cli -p $PORT ZCARD bz)"
chk "ZCARD == N"                 "$N" "$(redis-cli -p $PORT ZCARD bz)"
chk "TYPE is zset"               "zset" "$(redis-cli -p $PORT TYPE bz)"

mism=0
for i in 0 49 50 2500 9999; do
  m="m$(printf '%06d' $i)"
  a="$(redis-cli -p $REDIS ZSCORE bz $m)"; b="$(redis-cli -p $PORT ZSCORE bz $m)"
  [ "$a" = "$b" ] || { mism=$((mism+1)); echo "    ZSCORE $m: r=$a d=$b"; }
  a="$(redis-cli -p $REDIS ZRANK bz $m)"; b="$(redis-cli -p $PORT ZRANK bz $m)"
  [ "$a" = "$b" ] || { mism=$((mism+1)); echo "    ZRANK $m: r=$a d=$b"; }
done
chk "ZSCORE + ZRANK agree with redis (spread)" "0" "$mism"
chk "ZSCORE absent -> nil" "$(redis-cli -p $REDIS ZSCORE bz nope)" "$(redis-cli -p $PORT ZSCORE bz nope)"
chk "ZRANK absent -> nil"  "$(redis-cli -p $REDIS ZRANK bz nope)"  "$(redis-cli -p $PORT ZRANK bz nope)"

chk "ZRANGE 0 -1 full order matches (md5)" \
    "$(redis-cli -p $REDIS ZRANGE bz 0 -1 | md5sum)" "$(redis-cli -p $PORT ZRANGE bz 0 -1 | md5sum)"
chk "ZRANGE WITHSCORES head matches (md5)" \
    "$(redis-cli -p $REDIS ZRANGE bz 0 20 WITHSCORES | md5sum)" "$(redis-cli -p $PORT ZRANGE bz 0 20 WITHSCORES | md5sum)"
chk "ZREVRANGE head matches (md5)" \
    "$(redis-cli -p $REDIS ZREVRANGE bz 0 20 | md5sum)" "$(redis-cli -p $PORT ZREVRANGE bz 0 20 | md5sum)"
chk "ZRANGEBYSCORE 10 12 matches (md5)" \
    "$(redis-cli -p $REDIS ZRANGEBYSCORE bz 10 12 | md5sum)" "$(redis-cli -p $PORT ZRANGEBYSCORE bz 10 12 | md5sum)"
chk "ZRANGEBYSCORE exclusive (10 (12 matches (md5)" \
    "$(redis-cli -p $REDIS ZRANGEBYSCORE bz '(10' '(12' | md5sum)" "$(redis-cli -p $PORT ZRANGEBYSCORE bz '(10' '(12' | md5sum)"
chk "ZRANGEBYSCORE LIMIT matches (md5)" \
    "$(redis-cli -p $REDIS ZRANGEBYSCORE bz 5 45 LIMIT 100 50 | md5sum)" "$(redis-cli -p $PORT ZRANGEBYSCORE bz 5 45 LIMIT 100 50 | md5sum)"
chk "ZCOUNT 10 20 matches" \
    "$(redis-cli -p $REDIS ZCOUNT bz 10 20)" "$(redis-cli -p $PORT ZCOUNT bz 10 20)"

# writes on a promoted zset stay correct
chk "ZADD update existing score -> 0 added" "0" "$(redis-cli -p $PORT ZADD bz 999 m000000)"
chk "ZSCORE reflects the update"            "999" "$(redis-cli -p $PORT ZSCORE bz m000000)"
chk "ZREM existing -> 1"                     "1" "$(redis-cli -p $PORT ZREM bz m000001)"
chk "ZCARD after ZREM == N-1"               "$((N-1))" "$(redis-cli -p $PORT ZCARD bz)"
chk "still ONE key"                          "1" "$(redis-cli -p $PORT DBSIZE)"

redis-cli -p "$REDIS" DEL bz >/dev/null
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
