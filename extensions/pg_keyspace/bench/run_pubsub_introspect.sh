#!/bin/bash
# PUBSUB introspection on a real multi-worker, authenticated cluster.
#
# Two things only this shape can show, and neither is reachable from the
# conformance harness (one worker, no auth):
#
# 1. The answer is INSTANCE-WIDE. Workers are separate processes with separate
#    connection tables, so a worker asked about channels knows only its own --
#    unless it reads the shared routing table, which is what PUBSUB does here.
#    Redis Cluster answers per node and this deliberately does not: a Redis
#    Cluster node is a peer the client chose, while a pg_keyspace worker is an
#    implementation detail of one cache and which one a connection landed on is
#    not something the client picked. Asserted by subscribing on workers 1 and 2
#    and asking worker 0.
#
# 2. It does not leak across tenants. Channel names are force-scoped `{tenant}:`
#    per credential, and enumeration is exactly the leak that scoping exists to
#    stop: a tenant that can list another's channels learns what it is doing
#    without receiving a single message. NUMSUB and NUMPAT leak the same way in
#    numbers rather than names, so all three are asserted.
#
# Sections C and D cover sharded pub/sub, which only exists on a cluster: a
# shard channel hashes to a slot like a key, so it has one owning worker and a
# client asking anywhere else is redirected there. That is what makes SPUBLISH
# free of bus traffic, and it is asserted rather than assumed -- including that
# two tenants using the same client-facing name are routed by their own scoped
# names and cannot reach each other.
#
# Expects cargo, cargo-pgrx, a PGDG PostgreSQL and redis-cli on PATH.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-psintro-data}
PORT=${PGKS_PG_PORT:-5489}
RESP=${PGKS_RESP_PORT:-6491}
WORKERS=${PGKS_WORKERS:-3}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
restart_pg() {
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -m fast -w stop" >/dev/null 2>&1
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o '-p $PORT -k /tmp' -w start" >/dev/null 2>&1
  for i in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && break; sleep 1; done
}
wait_workers() {
  local up=0 w i
  for w in $(seq 0 $((WORKERS-1))); do
    for i in $(seq 1 40); do redis-cli -p $((RESP+w)) PING 2>/dev/null | grep -q PONG && { up=$((up+1)); break; }; sleep 1; done
  done
  echo $up
}

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/psintro-install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/psintro-install.log; exit 1; }

echo "=== cluster ($WORKERS workers) ==="
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA; chmod 700 $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  # Durable, not ephemeral, and that is load-bearing for sections C and D:
  # slot routing is configured only where persistence is on (the
  # set_slot_routing call lives inside that branch), so an EPHEMERAL
  # multi-worker cluster never redirects and every worker accepts every shard
  # channel locally. Sharded pub/sub has something to route only here.
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.workers = $WORKERS"
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
  echo "pg_keyspace.keys = 10000"
} >> $PGDATA/postgresql.conf
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o '-p $PORT -k /tmp' -w start" >/dev/null 2>&1
for i in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && break; sleep 1; done
psql_ "CREATE EXTENSION pg_keyspace" >/dev/null
restart_pg
chk "all $WORKERS workers are listening" "$WORKERS" "$(wait_workers)"

OUT=/tmp/pgks-psintro
rm -f $OUT.*.out

echo ""
echo "########## A. the answer covers the instance, not one worker ##########"
# Deliberately NOTHING on worker 0, which is the worker being asked. Every
# channel it reports is therefore one it learned from the routing table rather
# than from its own connection table.
( timeout 20 redis-cli -p $((RESP+1)) SUBSCRIBE alpha beta > $OUT.w1.out 2>&1 ) &
( timeout 20 redis-cli -p $((RESP+2)) SUBSCRIBE alpha      > $OUT.w2.out 2>&1 ) &
( timeout 20 redis-cli -p $((RESP+2)) PSUBSCRIBE "al*"     > $OUT.w2p.out 2>&1 ) &
for i in $(seq 1 30); do
  [ "$(grep -l subscribe $OUT.w1.out $OUT.w2.out $OUT.w2p.out 2>/dev/null | wc -l)" = 3 ] && break; sleep 0.5
done
chk "three subscribers registered, none on worker 0" "3" \
    "$(grep -l subscribe $OUT.w1.out $OUT.w2.out $OUT.w2p.out 2>/dev/null | wc -l | tr -d ' ')"
chk "worker 0 has no subscribers of its own" "0" \
    "$(redis-cli -p $RESP CLIENT LIST 2>/dev/null | grep -c 'cmd=subscribe')"

chk "worker 0 lists channels held on workers 1 and 2" "alpha,beta" \
    "$(redis-cli -p $RESP PUBSUB CHANNELS 2>&1 | sort | paste -sd,)"
chk "and counts subscribers across both of them" "alpha 2" \
    "$(redis-cli -p $RESP PUBSUB NUMSUB alpha 2>&1 | paste -sd' ')"
chk "beta, held on one worker only" "beta 1" \
    "$(redis-cli -p $RESP PUBSUB NUMSUB beta 2>&1 | paste -sd' ')"
chk "and the pattern on worker 2" "1" "$(redis-cli -p $RESP PUBSUB NUMPAT 2>&1)"
# Every worker must give the same answer, or "instance-wide" is only true of
# whichever one you happened to ask.
same=1
for w in $(seq 0 $((WORKERS-1))); do
  [ "$(redis-cli -p $((RESP+w)) PUBSUB CHANNELS 2>&1 | sort | paste -sd,)" = "alpha,beta" ] || same=0
done
chk "every worker gives the same answer" "1" "$same"
wait 2>/dev/null
chk "channels are gone once the subscribers exit" "" \
    "$(redis-cli -p $RESP PUBSUB CHANNELS 2>&1 | paste -sd,)"

echo ""
echo "########## B. it does not leak across tenants ##########"
psql_ "INSERT INTO supacache.resp_credential(username,secret,role_name,tenant) VALUES
         ('ua','pw1','role_a','ta'),('ub','pw2','role_b','tb')
       ON CONFLICT(username) DO UPDATE SET secret=EXCLUDED.secret,
         role_name=EXCLUDED.role_name, tenant=EXCLUDED.tenant" >/dev/null
psql_ "INSERT INTO supacache.acl(role_name,prefix,can_read,can_write) VALUES
         ('role_a','',true,true),('role_b','',true,true)
       ON CONFLICT DO NOTHING" >/dev/null
restart_pg
chk "workers are back with credentials loaded" "$WORKERS" "$(wait_workers)"
# Bounded, like every other subscribe used as a question here. This one matters
# most: it exists to catch credentials that did not load, and an unbounded
# SUBSCRIBE in exactly that case succeeds and blocks -- so the assertion would
# hang on the condition it was written to detect, and report as a job timeout
# rather than as a failure naming the cause.
chk "AUTH is required, so the credentials really loaded" "1" \
    "$(timeout 3 redis-cli -p $RESP --no-auth-warning SUBSCRIBE nope 2>&1 | grep -c NOAUTH)"

rm -f $OUT.ta.out $OUT.tb.out $OUT.tbp.out
# Both tenants subscribe to the SAME client-facing name. Server-side they are
# ta:shared and tb:shared, which is the case a prefix bug would collapse.
( timeout 20 redis-cli -p $RESP       --user ua -a pw1 --no-auth-warning SUBSCRIBE shared    > $OUT.ta.out 2>&1 ) &
( timeout 20 redis-cli -p $((RESP+1)) --user ub -a pw2 --no-auth-warning SUBSCRIBE shared secret > $OUT.tb.out 2>&1 ) &
( timeout 20 redis-cli -p $((RESP+2)) --user ub -a pw2 --no-auth-warning PSUBSCRIBE "sec*"  > $OUT.tbp.out 2>&1 ) &
for i in $(seq 1 30); do
  [ "$(grep -l subscribe $OUT.ta.out $OUT.tb.out $OUT.tbp.out 2>/dev/null | wc -l)" = 3 ] && break; sleep 0.5
done
chk "both tenants' subscribers registered" "3" \
    "$(grep -l subscribe $OUT.ta.out $OUT.tb.out $OUT.tbp.out 2>/dev/null | wc -l | tr -d ' ')"

A="redis-cli -p $RESP --user ua -a pw1 --no-auth-warning"
B="redis-cli -p $RESP --user ub -a pw2 --no-auth-warning"
# Tenant A holds only 'shared'. It must not see 'secret', which is tenant B's.
chk "tenant A sees only its own channel" "shared" "$($A PUBSUB CHANNELS 2>&1 | sort | paste -sd,)"
chk "tenant B sees both of its own" "secret,shared" "$($B PUBSUB CHANNELS 2>&1 | sort | paste -sd,)"
# Names come back unprefixed: the client gets what it subscribed with.
chk "names are reported unprefixed, not ta:shared" "0" \
    "$($A PUBSUB CHANNELS 2>&1 | grep -c ':')"
# The count is the tenant's own, not both tenants' subscribers on that name.
chk "NUMSUB counts only the asking tenant" "shared 1" "$($A PUBSUB NUMSUB shared 2>&1 | paste -sd' ')"
chk "and for the other tenant, separately" "shared 1" "$($B PUBSUB NUMSUB shared 2>&1 | paste -sd' ')"
# Asking for a channel only the other tenant holds must report nothing, not 1:
# a non-zero count here is the enumeration leak in numeric form.
chk "asking for the other tenant's channel reports nothing" "secret 0" \
    "$($A PUBSUB NUMSUB secret 2>&1 | paste -sd' ')"
chk "NUMPAT excludes the other tenant's pattern" "0" "$($A PUBSUB NUMPAT 2>&1)"
chk "and includes its own" "1" "$($B PUBSUB NUMPAT 2>&1)"
# A glob is not a way around the prefix filter.
chk "a wildcard does not escape the tenant scope" "shared" \
    "$($A PUBSUB CHANNELS '*' 2>&1 | sort | paste -sd,)"
wait 2>/dev/null

echo ""
echo "########## C. shard channels are routed by slot ##########"
# A shard channel hashes to a slot exactly as a key does, so it has ONE owning
# worker and a client that asks anywhere else is redirected. That is what makes
# SPUBLISH cost nothing across the bus: every subscriber to a channel a worker
# owns is on that worker, so there is nobody else to tell.
#
# Find a channel worker 0 does not own by asking it, which is also the redirect
# under test rather than a re-implementation of the hash in bash.
target=""; owner_ep=""
# Probe with SPUBLISH, not SSUBSCRIBE. Both route identically, but SPUBLISH
# ANSWERS -- a count or a MOVED -- while a SSUBSCRIBE that is not redirected
# succeeds and holds a live subscription, which would both hang the probe and
# add a phantom subscriber to every count taken afterwards.
for cand in sh1 sh2 sh3 sh4 sh5 sh6 sh7 sh8 sh9 sh10 sh11 sh12; do
  r=$(timeout 3 redis-cli -p $RESP SPUBLISH $cand probe 2>&1 | head -1)
  case "$r" in
    MOVED*) target=$cand; owner_ep=$(echo "$r" | awk '{print $3}'); break;;
  esac
done
chk "a shard channel owned by another worker exists" "1" "$([ -n "$target" ] && echo 1 || echo 0)"

if [ -n "$target" ]; then
  owner_port=${owner_ep##*:}
  chk "SSUBSCRIBE on the wrong worker is redirected" "1" \
      "$(timeout 3 redis-cli -p $RESP SSUBSCRIBE $target 2>&1 | grep -c '^MOVED')"
  chk "SPUBLISH on the wrong worker is redirected too" "1" \
      "$(redis-cli -p $RESP SPUBLISH $target x 2>&1 | grep -c '^MOVED')"
  chk "the redirect names a worker of this cluster" "1" \
      "$([ "$owner_port" -ge "$RESP" ] && [ "$owner_port" -lt "$((RESP+WORKERS))" ] && echo 1 || echo 0)"
  # And the redirect is honest: following it works.
  rm -f $OUT.sh.out
  ( timeout 15 redis-cli -p $owner_port SSUBSCRIBE $target > $OUT.sh.out 2>&1 ) &
  for i in $(seq 1 30); do grep -q ssubscribe $OUT.sh.out 2>/dev/null && break; sleep 0.5; done
  chk "SSUBSCRIBE on the owner is accepted" "1" "$(grep -c ssubscribe $OUT.sh.out 2>/dev/null)"
  chk "SPUBLISH on the owner reaches it" "1" \
      "$(redis-cli -p $owner_port SPUBLISH $target hello-shard 2>&1)"
  sleep 1
  chk "and the payload arrived as an smessage" "1" \
      "$(grep -c 'hello-shard' $OUT.sh.out 2>/dev/null)"
  # The point of the whole design: no bus traffic. A classic PUBLISH fans out to
  # every worker's inbox; a shard one must touch none of them.
  chk "nothing was queued to another worker" "0" \
      "$(psql_ "SELECT dropped FROM supacache.pubsub_stats()")"
  chk "and the shard channel is not in the classic listing" "0" \
      "$(redis-cli -p $owner_port PUBSUB CHANNELS 2>&1 | grep -c "^$target$")"
  chk "but is in the shard listing, on its owner" "1" \
      "$(redis-cli -p $owner_port PUBSUB SHARDCHANNELS 2>&1 | grep -c "^$target$")"
  wait 2>/dev/null
fi

echo ""
echo "########## D. shard channels are scoped per tenant too ##########"
# Two tenants using the SAME client-facing shard channel name. Server-side they
# are ta:<name> and tb:<name>, which hash differently -- so the tenants can be
# redirected to DIFFERENT workers for what looks to them like one channel, and
# neither may see the other's.
# SPUBLISH again, for the same reason: it answers where the channel lives
# without becoming a subscriber to it. The two tenants scope to ta:tshared and
# tb:tshared, which hash differently, so they may well be told different ports
# for what looks to them like one channel.
ta_ep=$(timeout 3 redis-cli -p $RESP --user ua -a pw1 --no-auth-warning SPUBLISH tshared probe 2>&1 | head -1)
tb_ep=$(timeout 3 redis-cli -p $RESP --user ub -a pw2 --no-auth-warning SPUBLISH tshared probe 2>&1 | head -1)
ta_port=$RESP; tb_port=$RESP
case "$ta_ep" in MOVED*) ta_port=$(echo "$ta_ep" | awk '{print $3}'); ta_port=${ta_port##*:};; esac
case "$tb_ep" in MOVED*) tb_port=$(echo "$tb_ep" | awk '{print $3}'); tb_port=${tb_port##*:};; esac
rm -f $OUT.sta.out $OUT.stb.out
( timeout 15 redis-cli -p $ta_port --user ua -a pw1 --no-auth-warning SSUBSCRIBE tshared > $OUT.sta.out 2>&1 ) &
( timeout 15 redis-cli -p $tb_port --user ub -a pw2 --no-auth-warning SSUBSCRIBE tshared > $OUT.stb.out 2>&1 ) &
for i in $(seq 1 30); do
  [ "$(grep -l ssubscribe $OUT.sta.out $OUT.stb.out 2>/dev/null | wc -l)" = 2 ] && break; sleep 0.5
done
chk "both tenants subscribed to the same shard name" "2" \
    "$(grep -l ssubscribe $OUT.sta.out $OUT.stb.out 2>/dev/null | wc -l | tr -d ' ')"
chk "tenant A's SPUBLISH reaches exactly one subscriber" "1" \
    "$(redis-cli -p $ta_port --user ua -a pw1 --no-auth-warning SPUBLISH tshared from-ta 2>&1)"
sleep 1
chk "and it was tenant A's" "1" "$(grep -c 'from-ta' $OUT.sta.out 2>/dev/null)"
chk "tenant B never saw it" "0" "$(grep -c 'from-ta' $OUT.stb.out 2>/dev/null)"
chk "SHARDNUMSUB counts only the asking tenant" "tshared 1" \
    "$(redis-cli -p $ta_port --user ua -a pw1 --no-auth-warning PUBSUB SHARDNUMSUB tshared 2>&1 | paste -sd' ')"
wait 2>/dev/null

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -m fast -w stop" >/dev/null 2>&1
[ "$fail" -eq 0 ]
