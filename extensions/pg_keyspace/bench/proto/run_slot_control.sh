#!/bin/bash
# The control for run_owner_recovery.sh, and it is EXPECTED TO FAIL.
#
# Same plain (non-cluster) client, same writes, against the default `slot`
# addressing. It fails at the first SET with a MOVED redirect, which is the
# whole point: without owner addressing a non-cluster client cannot use a
# persisted multi-worker cluster at all. All three Supatype clients
# (valkey-go, iovalkey, go-redis) use standalone constructors.
#
# It then runs the same scenario WITH a cluster-aware client (redis-cli -c) and
# requires that to pass, which is the regression check that owner addressing
# did not disturb the default path.
set -uo pipefail
IMAGE="${IMAGE:-pgks-proto:owner}"
HERE="$(cd "$(dirname "$0")" && pwd)"
NAME=pgks-slot-control
VOL=pgks-slot-control-data

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; docker volume rm "$VOL" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

start() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" -e POSTGRES_PASSWORD=p -e POSTGRES_DB=supatype \
    -v "$VOL":/var/lib/postgresql/data "$IMAGE" \
    postgres -c config_file=/etc/postgresql/postgresql.conf \
    -c shared_preload_libraries='pg_stat_statements, pg_cron, pg_net, plan_filter, safeupdate, pg_keyspace, supatype_mask' \
    -c pg_keyspace.database=supatype -c pg_keyspace.durability=durable \
    -c pg_keyspace.cluster_announce_host=127.0.0.1 -c pg_keyspace.workers=4 \
    -c pg_keyspace.keys=10000 -c pg_keyspace.val_bytes=1024 \
    -c pg_keyspace.ring_mb=1 -c pg_keyspace.rowcache_mb=1 >/dev/null
}
ready() {
  until [ "$(docker logs "$NAME" 2>&1 | grep -c 'recovered [0-9]* keys')" -ge 4 ] \
     && [ "$(docker logs "$NAME" 2>&1 | grep -c 'RESP listening on 0.0.0.0:')" -ge 4 ]; do sleep 2; done
}
probe() { docker run --rm --network "container:$NAME" -v "$HERE:/probe" redis:7-alpine "$@"; }

start; until docker exec "$NAME" pg_isready -h 127.0.0.1 -q 2>/dev/null; do sleep 2; done
docker exec -i "$NAME" psql -h 127.0.0.1 -U supatype_admin -d supatype -tAc \
  "create extension if not exists pg_keyspace;" >/dev/null
start; ready

echo "== plain client against slot addressing (must be refused) =="
if probe sh /probe/scaling-test.sh 4 write; then
  echo "UNEXPECTED: a plain client wrote to a persisted multi-worker cluster"
  exit 1
fi
echo "   refused, as expected"

echo "== cluster-aware client against slot addressing (must pass) =="
probe sh /probe/slot-regression.sh write || exit 1
start; ready
probe sh /probe/slot-regression.sh verify || exit 1
echo "PASS: default slot path unchanged"
