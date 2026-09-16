#!/bin/bash
# supacache.publish(): a SQL backend reaching RESP subscribers.
#
# A backend could already read and write the keyspace through supacache.*, but
# had no way to reach a subscriber -- a trigger that wanted to tell a RESP
# client something had to go out through the application and back in over the
# wire.
#
# The mechanism is worth stating because it looks like it should be impossible.
# The bus rings are per (from, to) worker pair and lock-free SPSC, so a backend
# cannot borrow a worker's lane without putting two producers on one ring. But
# the DIAGONAL rings (w, w) are allocated and initialised like every other ring
# and nothing uses them: publish filters them out with `w != from`, invalidate
# and drain skip them. So each worker has an idle inbox no worker writes to, and
# an outside publisher owns it as sole producer -- no extra rings, no larger
# segment, and no lock anywhere near the RESP hot path.
#
# Asserts delivery across THREE separate worker processes (the case that would
# silently deliver nothing if the lane were wrong), that an unsubscribed channel
# reaches nobody, that nothing is dropped, and that the superuser restriction is
# enforced by the function rather than by a GRANT.
#
# Expects cargo, cargo-pgrx, a PGDG PostgreSQL and redis-cli on PATH.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-sqlpub-data}
PORT=${PGKS_PG_PORT:-5487}
RESP=${PGKS_RESP_PORT:-6487}
WORKERS=${PGKS_WORKERS:-3}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/sqlpub-install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/sqlpub-install.log; exit 1; }

echo "=== cluster ($WORKERS workers, so the lane is exercised across processes) ==="
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA; chmod 700 $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  # Pub/sub needs no persistence, and >1 worker forces the ephemeral tier anyway.
  echo "pg_keyspace.durability = 'ephemeral'"
  echo "pg_keyspace.workers = $WORKERS"
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
  echo "pg_keyspace.keys = 10000"
} >> $PGDATA/postgresql.conf
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o '-p $PORT -k /tmp' -w start" >/dev/null 2>&1
for i in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && break; sleep 1; done
psql_ "CREATE EXTENSION pg_keyspace" >/dev/null
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -m fast -w stop" >/dev/null 2>&1
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o '-p $PORT -k /tmp' -w start" >/dev/null 2>&1
for i in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && break; sleep 1; done
# Every worker's port, not just worker 0's: they come up seconds apart, and a
# subscriber that connects before its worker is listening proves nothing.
up=0
for w in $(seq 0 $((WORKERS-1))); do
  for i in $(seq 1 40); do redis-cli -p $((RESP+w)) PING 2>/dev/null | grep -q PONG && { up=$((up+1)); break; }; sleep 1; done
done
chk "all $WORKERS workers are listening" "$WORKERS" "$up"

echo ""
echo "########## A. SQL reaches subscribers on every worker ##########"
OUT=/tmp/pgks-sqlpub
rm -f $OUT.*.out
for w in $(seq 0 $((WORKERS-1))); do
  ( timeout 15 redis-cli -p $((RESP+w)) SUBSCRIBE demo > $OUT.$w.out 2>&1 ) &
done
# Wait for the subscriptions to register rather than sleeping and hoping.
for i in $(seq 1 30); do
  n=$(grep -l subscribe $OUT.*.out 2>/dev/null | wc -l)
  [ "$n" = "$WORKERS" ] && break; sleep 0.5
done
chk "every subscriber registered" "$WORKERS" "$(grep -l subscribe $OUT.*.out 2>/dev/null | wc -l | tr -d ' ')"

chk "supacache.publish() counts every subscriber" "$WORKERS" \
    "$(psql_ "SELECT supacache.publish('demo','hello-from-sql'::bytea)")"
# The same count a RESP PUBLISH reports, which is the contract being matched.
chk "and agrees with what RESP PUBLISH reports" "$WORKERS" \
    "$(redis-cli -p $RESP PUBLISH demo from-resp 2>&1)"
sleep 1
wait 2>/dev/null
got=0
for w in $(seq 0 $((WORKERS-1))); do
  grep -q "hello-from-sql" $OUT.$w.out 2>/dev/null && got=$((got+1))
done
chk "every worker's subscriber actually received it" "$WORKERS" "$got"

echo ""
echo "########## B. it reaches nobody it should not ##########"
chk "an unsubscribed channel reports no receivers" "0" \
    "$(psql_ "SELECT supacache.publish('quiet','x'::bytea)")"
chk "nothing was dropped on the way" "0" "$(psql_ "SELECT dropped FROM supacache.pubsub_stats()")"
chk "no subscription was refused for table space" "0" \
    "$(psql_ "SELECT route_full FROM supacache.pubsub_stats()")"

echo ""
echo "########## C. the restriction is in the function, not a GRANT ##########"
# RESP channels are force-scoped {tenant}: per credential; a Postgres role is
# not a credential, so this function cannot scope a caller's channel name and
# must not publish one unscoped. The check has to survive an operator granting
# every privilege there is -- that is the whole point of putting it in the code.
psql_ "CREATE ROLE tenant_a LOGIN" >/dev/null 2>&1
psql_ "GRANT USAGE ON SCHEMA supacache TO tenant_a" >/dev/null 2>&1
psql_ "GRANT EXECUTE ON FUNCTION supacache.publish(text,bytea) TO tenant_a" >/dev/null 2>&1
DENIED=$($PGBIN/psql -h /tmp -p $PORT -U tenant_a -d postgres -tAc \
  "SELECT supacache.publish('demo','sneaky'::bytea)" 2>&1 | head -1)
chk "a granted non-superuser is still refused" "1" "$(echo "$DENIED" | grep -c 'superuser-only')"
# And refused by THIS check, not by the schema: a "permission denied for schema"
# would mean the grants above were incomplete and the guard never ran.
chk "and refused by the function, not the schema" "0" \
    "$(echo "$DENIED" | grep -c 'permission denied for schema')"

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -m fast -w stop" >/dev/null 2>&1
[ "$fail" -eq 0 ]
