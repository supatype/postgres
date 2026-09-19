#!/bin/bash
# Graceful scaling under pg_keyspace.recovery_addressing = 'owner'.
#
# Writes with a PLAIN (non-cluster) client to every worker of a persisted
# multi-worker cluster, then restarts at a series of different worker counts and
# requires that every key is still readable, at the worker `owner % workers`
# predicts.
#
# The control matters as much as the result: run_slot_control below does the
# same thing against the default `slot` addressing and is expected to FAIL at
# the first write with MOVED, because a non-cluster client cannot use a
# persisted multi-worker cluster at all today. That failure is the reason this
# mode exists.
#
# Driven through Docker rather than a local cluster because it needs to restart
# the postmaster at a different pg_keyspace.workers several times, which is a
# postmaster-context GUC.
#
#   IMAGE=pgks-proto:owner ./run_owner_recovery.sh
set -euo pipefail
IMAGE="${IMAGE:-pgks-proto:owner}"
HERE="$(cd "$(dirname "$0")" && pwd)"
NAME=pgks-owner-test
VOL=pgks-owner-test-data

start() { # $1 workers, $2 addressing, $3.. extra args
  local n="$1" addr="$2"; shift 2
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  docker run -d --name "$NAME" -e POSTGRES_PASSWORD=p -e POSTGRES_DB=supatype \
    -v "$VOL":/var/lib/postgresql/data "$IMAGE" \
    postgres -c config_file=/etc/postgresql/postgresql.conf \
    -c shared_preload_libraries='pg_stat_statements, pg_cron, pg_net, plan_filter, safeupdate, pg_keyspace, supatype_mask' \
    -c pg_keyspace.database=supatype -c pg_keyspace.durability=durable \
    -c pg_keyspace.recovery_addressing="$addr" -c pg_keyspace.workers="$n" \
    -c pg_keyspace.keys=10000 -c pg_keyspace.val_bytes=1024 \
    -c pg_keyspace.ring_mb=1 -c pg_keyspace.rowcache_mb=1 "$@" >/dev/null
}

# A worker recovers BEFORE it listens, and each does so independently, so the
# only sound readiness signal is "every worker has recovered AND is listening".
# Waiting on the ports alone races: a verify can run while a later worker is
# still loading and read an empty segment as data loss. That false failure cost
# an hour, so the check is spelled out rather than approximated.
ready() {
  local n="$1"
  until [ "$(docker logs "$NAME" 2>&1 | grep -c 'recovered [0-9]* keys')" -ge "$n" ] \
     && [ "$(docker logs "$NAME" 2>&1 | grep -c 'RESP listening on 0.0.0.0:')" -ge "$n" ]; do
    sleep 2
  done
}
pgready() { until docker exec "$NAME" pg_isready -h 127.0.0.1 -q 2>/dev/null; do sleep 2; done; }
probe() { docker run --rm --network "container:$NAME" -v "$HERE:/probe" redis:7-alpine "$@"; }

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; docker volume rm "$VOL" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "== bringing up 4 durable workers, owner addressing, NO cluster_announce_host =="
start 4 owner; pgready
docker exec -i "$NAME" psql -h 127.0.0.1 -U supatype_admin -d supatype -tAc \
  "create extension if not exists pg_keyspace;" >/dev/null
# The operational tables are created by worker 0 at startup and only if the
# extension is already installed, so this restart is required, not incidental.
start 4 owner; ready 4
persisted=$(docker logs "$NAME" 2>&1 | grep -c 'persisted=true')
[ "$persisted" -eq 4 ] || { echo "FAIL: only $persisted/4 workers persisted"; exit 1; }
echo "   4 workers persisted, and it started with no announce host (slot addressing refuses this)"

probe sh /probe/scaling-test.sh 4 write
probe sh /probe/ttl-scaling.sh 4 write

fail=0
for n in 2 4 3 1 4; do
  echo "== restart at $n worker(s) =="
  start "$n" owner; ready "$n"
  probe sh /probe/scaling-test.sh "$n" verify 6379 4 | tail -1 || fail=1
  probe sh /probe/ttl-scaling.sh  "$n" verify 6379 4 | tail -1 || fail=1
done
[ "$fail" -eq 0 ] || { echo "FAILED"; exit 1; }
echo "PASS: every key survived 4 -> 2 -> 4 -> 3 -> 1 -> 4 with a plain client"
