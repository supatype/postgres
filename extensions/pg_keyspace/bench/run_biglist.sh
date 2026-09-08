#!/usr/bin/env bash
# native large-list structure. Past a threshold a list is stored with an
# explicit offset table so LINDEX/LRANGE/LLEN are O(1)/O(k) instead of walking
# every length-prefix from the head. This proves a 10k-element list is
# byte-for-byte Redis-compatible over the read surface (parity vs a real
# redis-server on :6379) and stays a single key.
set -u
DAEMON=${DAEMON:-/home/user/postgres/extensions/pg_keyspace/core/target/release/pgkeyspaced}
PORT=${PORT:-6396}
REDIS=${REDIS:-6379}
N=${N:-10000}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-50s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-50s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

[ -x "$DAEMON" ] || { echo "  SKIP  daemon not built at $DAEMON"; exit 0; }
redis-cli -p "$REDIS" PING 2>/dev/null | grep -q PONG || { echo "  SKIP  no redis on :$REDIS"; exit 0; }

"$DAEMON" --workers 1 --port "$PORT" --keys-per-worker 100000 --shmem-mb 128 >/tmp/pgks_biglist.log 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null' EXIT
for _ in $(seq 1 20); do redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done
redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  daemon did not start"; exit 1; }

echo "# native large-list — $N elements, parity vs redis :$REDIS"
redis-cli -p "$REDIS" DEL bl >/dev/null; redis-cli -p "$PORT" DEL bl >/dev/null
build() { awk -v n="$N" 'BEGIN{for(i=0;i<n;i++) printf "RPUSH bl e%d\n", i}'; }
build | redis-cli -p "$REDIS" >/dev/null
build | redis-cli -p "$PORT"  >/dev/null

chk "LLEN matches redis"        "$(redis-cli -p $REDIS LLEN bl)" "$(redis-cli -p $PORT LLEN bl)"
chk "LLEN == N"                 "$N" "$(redis-cli -p $PORT LLEN bl)"
chk "TYPE is list"              "list" "$(redis-cli -p $PORT TYPE bl)"

mism=0
for i in 0 1 127 128 4999 5000 9999 -1 -2 -5000; do
  a="$(redis-cli -p $REDIS LINDEX bl $i)"; b="$(redis-cli -p $PORT LINDEX bl $i)"
  [ "$a" = "$b" ] || { mism=$((mism+1)); echo "    LINDEX $i: redis=$a daemon=$b"; }
done
chk "LINDEX agrees with redis (spread + negative idx)" "0" "$mism"
chk "LINDEX out of range -> nil" "$(redis-cli -p $REDIS LINDEX bl 999999)" "$(redis-cli -p $PORT LINDEX bl 999999)"

chk "LRANGE tail matches redis" \
    "$(redis-cli -p $REDIS LRANGE bl 9995 -1 | paste -sd,)" "$(redis-cli -p $PORT LRANGE bl 9995 -1 | paste -sd,)"
chk "LRANGE middle matches redis" \
    "$(redis-cli -p $REDIS LRANGE bl 4990 5010 | md5sum)" "$(redis-cli -p $PORT LRANGE bl 4990 5010 | md5sum)"
chk "LRANGE 0 -1 whole list matches (md5)" \
    "$(redis-cli -p $REDIS LRANGE bl 0 -1 | md5sum)" "$(redis-cli -p $PORT LRANGE bl 0 -1 | md5sum)"

# writes on a promoted list stay correct (head/tail push/pop re-encode the blob)
chk "LPOP returns the head"     "e0" "$(redis-cli -p $PORT LPOP bl)"
chk "RPOP returns the tail"     "e$((N-1))" "$(redis-cli -p $PORT RPOP bl)"
chk "LLEN after pops == N-2"    "$((N-2))" "$(redis-cli -p $PORT LLEN bl)"
chk "LINDEX 0 now the 2nd elem" "e1" "$(redis-cli -p $PORT LINDEX bl 0)"

chk "still ONE key (no index-side pollution)" "1" "$(redis-cli -p $PORT DBSIZE)"
redis-cli -p "$REDIS" DEL bl >/dev/null
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
