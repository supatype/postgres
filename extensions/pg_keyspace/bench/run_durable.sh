#!/usr/bin/env bash
# durable aggregates: hashes/lists/sorted-sets persist to
# supacache.kv with their type tag and recover, with the right type, after a
# crash. Requires a durable tier (pg_keyspace.durability != ephemeral) and the
# ability to restart the cluster. Set PGDATA + PGCTL + PG_USER for the restart.
set -u
RESP=${RESP:-6381}
PGPORT=${PGPORT:-5434}
PGDATA=${PGDATA:-/tmp/pgks17}
PGCTL=${PGCTL:-/usr/local/pgsql17/bin/pg_ctl}
PG_USER=${PG_USER:-postgres}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then K="redis-cli -p $RESP";
else K="redis-cli --tls --insecure -p $RESP"; fi
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-46s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-46s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# durable aggregates"
if [ "$($P -c "SHOW pg_keyspace.durability")" = "ephemeral" ]; then
  echo "  SKIP  cluster is ephemeral; set pg_keyspace.durability=relaxed"; exit 0
fi

$K DEL dh dl dz ds >/dev/null 2>&1
$K HSET dh f1 v1 f2 v2 >/dev/null
$K RPUSH dl a b c >/dev/null
$K ZADD dz 1 x 2 y >/dev/null
$K SET ds strval >/dev/null
sleep 2   # let the persistence worker drain the ring into supacache.kv

# each type persisted with the right kind tag
chk "hash persisted with kind='h'"  "h" "$($P -c "SELECT kind FROM supacache.kv WHERE key='dh'::bytea")"
chk "list persisted with kind='l'"  "l" "$($P -c "SELECT kind FROM supacache.kv WHERE key='dl'::bytea")"
chk "zset persisted with kind='z'"  "z" "$($P -c "SELECT kind FROM supacache.kv WHERE key='dz'::bytea")"
chk "string persisted with kind='s'" "s" "$($P -c "SELECT kind FROM supacache.kv WHERE key='ds'::bytea")"

# simulate a crash: immediate restart drops shmem, which is rebuilt from kv
su "$PG_USER" -c "$PGCTL -D $PGDATA -w -t 30 -m immediate restart -l $PGDATA/server.log" >/dev/null 2>&1
sleep 2

chk "types recovered after crash" "hash,list,zset,string" \
    "$($K TYPE dh),$($K TYPE dl),$($K TYPE dz),$($K TYPE ds)"
chk "hash values recovered"  "f1,v1,f2,v2" "$($K HGETALL dh | paste -sd,)"
chk "list values recovered"  "a,b,c"       "$($K LRANGE dl 0 -1 | paste -sd,)"
chk "zset recovered w/scores" "x,1,y,2"     "$($K ZRANGE dz 0 -1 WITHSCORES | paste -sd,)"
chk "string value recovered" "strval"      "$($K GET ds)"

# a subsequent aggregate mutation still persists (kind stays on UPDATE)
$K HSET dh f3 v3 >/dev/null; sleep 2
su "$PG_USER" -c "$PGCTL -D $PGDATA -w -t 30 -m immediate restart -l $PGDATA/server.log" >/dev/null 2>&1
sleep 2
chk "hash mutation survives a 2nd crash" "f1,v1,f2,v2,f3,v3" "$($K HGETALL dh | paste -sd,)"

# ---- durable TTL: EXPIRE / PERSIST changes, and a TTL'd aggregate's type,
# must survive recovery (not revert to the TTL/type the last SET persisted) ----
$K DEL de dp dhz >/dev/null 2>&1
$K SET de v >/dev/null;         $K EXPIRE de 100000 >/dev/null    # TTL added after a SET
$K SET dp v EX 1000 >/dev/null; $K PERSIST dp >/dev/null          # TTL removed after SET EX
$K HSET dhz f1 v1 >/dev/null;   $K EXPIRE dhz 100000 >/dev/null   # a hash that gained a TTL
sleep 2
su "$PG_USER" -c "$PGCTL -D $PGDATA -w -t 30 -m immediate restart -l $PGDATA/server.log" >/dev/null 2>&1
sleep 2
t=$($K TTL de); { [ "$t" -gt 0 ] && { echo "  PASS  EXPIRE survived recovery (ttl=$t)"; pass=$((pass+1)); }; } \
                || { echo "  FAIL  EXPIRE lost after recovery (ttl=$t)"; fail=$((fail+1)); }
chk "PERSIST survived recovery (-1)"     "-1"    "$($K TTL dp)"
chk "TTL'd hash recovered as a hash"     "hash"  "$($K TYPE dhz)"
chk "TTL'd hash kept its value"          "v1"    "$($K HGET dhz f1)"
t=$($K TTL dhz); { [ "$t" -gt 0 ] && { echo "  PASS  TTL'd hash kept its TTL (ttl=$t)"; pass=$((pass+1)); }; } \
                 || { echo "  FAIL  TTL'd hash lost its TTL (ttl=$t)"; fail=$((fail+1)); }

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
