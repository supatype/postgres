#!/usr/bin/env bash
# §5 — native large-collection structure (indexed hashes). Past a threshold a
# hash is stored in an in-value bucket table so point reads are O(1) average
# instead of scanning every field. This test proves the promoted (10k-field)
# hash behaves EXACTLY like Redis for the read/write surface, and that the
# indexed blob is still one atomically-managed keyspace value. Parity is checked
# against a real redis-server on :6379.
set -u
DAEMON=${DAEMON:-/home/user/postgres/extensions/pg_keyspace/core/target/release/pgkeyspaced}
PORT=${PORT:-6392}
REDIS=${REDIS:-6379}
N=${N:-10000}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-50s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-50s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

[ -x "$DAEMON" ] || { echo "  SKIP  daemon not built at $DAEMON"; exit 0; }
redis-cli -p "$REDIS" PING 2>/dev/null | grep -q PONG || { echo "  SKIP  no redis on :$REDIS for parity"; exit 0; }

"$DAEMON" --workers 1 --port "$PORT" --keys-per-worker 100000 --val-bytes 64 --shmem-mb 128 >/tmp/pgks_bighash.log 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null' EXIT
for _ in $(seq 1 20); do redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done
redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  daemon did not start"; cat /tmp/pgks_bighash.log; exit 1; }

echo "# native large-collection hash (§5) — $N fields, parity vs redis :$REDIS"

# Build the same big hash on both servers, one pipelined connection each.
redis-cli -p "$REDIS" DEL bh >/dev/null
redis-cli -p "$PORT"  DEL bh >/dev/null
build() { awk -v n="$N" 'BEGIN{for(i=0;i<n;i++) printf "HSET bh field-%08d value-%d\n", i, i}'; }
build | redis-cli -p "$REDIS" >/dev/null
build | redis-cli -p "$PORT"  >/dev/null

chk "HLEN matches redis"                 "$(redis-cli -p $REDIS HLEN bh)"            "$(redis-cli -p $PORT HLEN bh)"
chk "HLEN == N"                          "$N"                                        "$(redis-cli -p $PORT HLEN bh)"
chk "TYPE is hash"                       "hash"                                      "$(redis-cli -p $PORT TYPE bh)"

# point reads across the whole hash agree with redis
mism=0
for i in 0 1 42 128 129 500 999 5000 9999; do
  f="field-$(printf '%08d' $i)"
  a="$(redis-cli -p $REDIS HGET bh "$f")"; b="$(redis-cli -p $PORT HGET bh "$f")"
  [ "$a" = "$b" ] || { mism=$((mism+1)); echo "    HGET $f: redis=$a daemon=$b"; }
done
chk "HGET agrees with redis (9 spread keys)" "0" "$mism"

chk "HGET missing field -> nil"          "$(redis-cli -p $REDIS HGET bh nope)"       "$(redis-cli -p $PORT HGET bh nope)"
chk "HEXISTS present -> 1"               "1"                                         "$(redis-cli -p $PORT HEXISTS bh field-00005000)"
chk "HEXISTS absent -> 0"               "0"                                         "$(redis-cli -p $PORT HEXISTS bh field-99999999)"
chk "HSTRLEN matches redis"             "$(redis-cli -p $REDIS HSTRLEN bh field-00000042)" "$(redis-cli -p $PORT HSTRLEN bh field-00000042)"

# HMGET across the threshold (inline<->indexed boundary at 128)
a="$(redis-cli -p $REDIS HMGET bh field-00000001 field-00000200 nope | paste -sd,)"
b="$(redis-cli -p $PORT  HMGET bh field-00000001 field-00000200 nope | paste -sd,)"
chk "HMGET matches redis"               "$a" "$b"

# HGETALL: same field/value set (order differs for hashtable encoding, so sort)
a="$(redis-cli -p $REDIS HGETALL bh | sort | md5sum | cut -d' ' -f1)"
b="$(redis-cli -p $PORT  HGETALL bh | sort | md5sum | cut -d' ' -f1)"
chk "HGETALL content matches redis (sorted)" "$a" "$b"

# write path still correct on a promoted hash: overwrite + add + delete
chk "HSET overwrite existing -> 0 added"  "0" "$(redis-cli -p $PORT HSET bh field-00000001 changed)"
chk "HGET sees overwrite"                 "changed" "$(redis-cli -p $PORT HGET bh field-00000001)"
chk "HSET brand-new field -> 1 added"     "1" "$(redis-cli -p $PORT HSET bh field-new hi)"
chk "HLEN grew by one"                    "$((N+1))" "$(redis-cli -p $PORT HLEN bh)"
chk "HDEL existing -> 1"                  "1" "$(redis-cli -p $PORT HDEL bh field-00000002)"
chk "HGET deleted -> nil"                 "" "$(redis-cli -p $PORT HGET bh field-00000002)"
chk "HLEN back to N"                      "$N" "$(redis-cli -p $PORT HLEN bh)"

# still ONE key: the indexed table lives inside the value, not as extra keys
chk "DBSIZE == 1 (no chunk-key pollution)" "1" "$(redis-cli -p $PORT DBSIZE)"
chk "DEL removes the whole hash -> 1"      "1" "$(redis-cli -p $PORT DEL bh)"
chk "DBSIZE == 0 after DEL"                "0" "$(redis-cli -p $PORT DBSIZE)"

redis-cli -p "$REDIS" DEL bh >/dev/null
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
