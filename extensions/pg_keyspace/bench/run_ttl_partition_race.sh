#!/usr/bin/env bash
# Concurrent persistence workers must not kill each other creating a TTL
# partition (#130).
#
# TTL'd keys persist into supacache.kv_ttl, RANGE-partitioned by expiry bucket,
# and the partitions are created on demand by whichever persistence worker first
# sees a write land in a new bucket. With persist_workers > 1 that makes every
# bucket rollover a race, and `CREATE TABLE IF NOT EXISTS` does not settle it:
# two sessions can both pass the existence check before either inserts its
# catalogue row, and the loser raises `duplicate_table`.
#
# The loser then DIES -- a Postgres ERROR inside SPI longjmps out and pgrx
# surfaces it as a panic. The watchdog brings it back in ~2s and nothing is
# lost, but the shard stops draining for that gap and clients see errors.
#
# What this asserts is therefore the absence of a log line and of worker exits,
# not a query result. It is deliberately sized to lose on an unfixed build
# quickly: a short bucket width and enough TTL write concurrency that every
# rollover is contested.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-ttlrace}
PORT=${PGKS_PG_PORT:-5459}
RESP=${PGKS_RESP_PORT:-6461}
PROFILE=${PGKS_BUILD_PROFILE:-release}
SHARDS=${PGKS_PERSIST_WORKERS:-4}
WORKERS=${PGKS_WORKERS:-2}
BUCKET=${BUCKET_SECS:-2}
SECS=${SECS:-60}
WRITERS=${WRITERS:-8}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop -m fast" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }
log_lines() { wc -l < $PGDATA/log 2>/dev/null || echo 0; }
since() { tail -n +"${1:-0}" $PGDATA/log 2>/dev/null; }

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/ttlrace_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/ttlrace_install.log; exit 1; }

echo "=== cluster ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.workers = $WORKERS"
  # More than one persistence worker is the whole precondition: with one, there
  # is nobody to race.
  echo "pg_keyspace.persist_workers = $SHARDS"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
  # A short bucket makes rollovers frequent, so a 60s run crosses ~30 of them
  # instead of 6.
  echo "pg_keyspace.ttl_bucket_secs = $BUCKET"
  echo "max_connections = 100"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
Q "CREATE EXTENSION pg_keyspace" >/dev/null
restart
for _ in $(seq 1 60); do
  [ "$(Q "SELECT count(*) FROM pg_stat_activity WHERE backend_type='pg_keyspace: persistence worker')")" = "$SHARDS" ] && break
  sleep 1
done

echo
echo "########## 0. preconditions ##########"
chk "$SHARDS persistence workers are running" "$SHARDS" \
    "$(Q "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'pg_keyspace: persistence worker%'")"
chk "the durable tier is configured, so TTL writes really persist" "durable" \
    "$(Q "SELECT tier FROM supacache.replication_status()")"
chk "the TTL bucket is short enough to roll over often ($BUCKET s)" "$BUCKET" \
    "$(Q "SHOW pg_keyspace.ttl_bucket_secs" | tr -d 's ')"
chk "the RESP port answers" "PONG" "$(redis-cli -p $RESP PING 2>&1)"

echo
echo "########## 1. sustained TTL writes across many bucket rollovers ##########"
N0=$(log_lines)
BUCKETS0=$(Q "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhparent WHERE c.relname='kv_ttl'")
END=$(( $(date +%s) + SECS ))
for w in $(seq 1 $WRITERS); do
  ( while [ "$(date +%s)" -lt "$END" ]; do
      for i in $(seq 1 40); do
        redis-cli -c -p $RESP SET "eph:$w:$RANDOM" "v" EX 30 >/dev/null 2>&1
      done
    done ) &
done
wait
sleep 3

CRASHES=$(since $N0 | grep -c "persistence worker.*exited with exit code")
DUPES=$(since $N0 | grep -c 'relation "kv_ttl_b[0-9]*" already exists')
BUCKETS1=$(Q "SELECT count(*) FROM pg_inherits i JOIN pg_class c ON c.oid=i.inhparent WHERE c.relname='kv_ttl'")
ROLLOVERS=$(( BUCKETS1 - BUCKETS0 ))

# Guard first: with too few rollovers there was nothing to race over, and a
# clean result below would mean nothing at all.
chk "the run crossed enough bucket rollovers to race ($ROLLOVERS)" "t" \
    "$([ "$ROLLOVERS" -ge 5 ] && echo t || echo f)"
chk "no persistence worker died" "0" "$CRASHES"
chk "no worker lost a partition-creation race" "0" "$DUPES"
chk "the cluster is still up" "1" "$(Q "SELECT 1")"

echo
echo "########## 2. the TTL data path still works ##########"
# A fix that stopped creating partitions at all would pass section 1 perfectly.
chk "TTL rows were persisted" "t" \
    "$(Q "SELECT count(*) > 100 FROM supacache.kv_ttl")"
chk "they landed in more than one bucket partition" "t" \
    "$(Q "SELECT count(DISTINCT bucket) > 1 FROM supacache.kv_ttl")"
chk "a TTL'd key reads back over RESP" "v" \
    "$(redis-cli -c -p $RESP SET ttlprobe v EX 60 >/dev/null 2>&1; redis-cli -c -p $RESP GET ttlprobe 2>&1)"
chk "every persistence worker is still the one that started" "$SHARDS" \
    "$(Q "SELECT count(*) FROM pg_stat_activity WHERE backend_type LIKE 'pg_keyspace: persistence worker%'")"

echo
echo "=== $pass passed, $fail failed ==="
stop_pg
[ "$fail" -eq 0 ]
