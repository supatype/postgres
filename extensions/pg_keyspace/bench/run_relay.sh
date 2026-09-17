#!/bin/bash
# Cross-instance relay: a message published on instance A reaching a RESP
# subscriber on instance B.
#
# The relay is a RESP CLIENT, not a bus participant, and that is the design
# rather than an implementation detail. Giving it its own inbox would have meant
# sizing the pub/sub bus for nworkers+1 -- 2n+1 extra rings to hand one
# participant a mailbox, quadratic, paid by every deployment whether or not it
# relays. As a subscriber it needs no shared memory: the fan-out, the tenant
# scoping and the wakeups already exist.
#
# Delivery is AT-MOST-ONCE, matching valkey. A message published while a peer is
# unreachable is lost, and there is no catch-up -- asserted here, so the
# contract is recorded rather than assumed.
#
# Expects cargo, cargo-pgrx, a PGDG PostgreSQL, dblink and redis-cli.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
ADATA=${PGKS_A_DATA:-/tmp/pgks-relay-a}; BDATA=${PGKS_B_DATA:-/tmp/pgks-relay-b}
APORT=${PGKS_A_PG:-5477}; BPORT=${PGKS_B_PG:-5478}
ARESP=${PGKS_A_RESP:-6477}; BRESP=${PGKS_B_RESP:-6478}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
qa() { $PGBIN/psql -h /tmp -p $APORT -U postgres -d postgres -tAc "$1" 2>&1; }
qb() { $PGBIN/psql -h /tmp -p $BPORT -U postgres -d postgres -tAc "$1" 2>&1; }

echo "=== build + install ==="
cd "$EXT_DIR"
REL=""; [ "$PROFILE" = "release" ] && REL="--release"
cargo pgrx install $REL --pg-config $PGBIN/pg_config >/tmp/relay-install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/relay-install.log; exit 1; }

start() { # dir pgport resp relay_channels
  rm -rf $1; mkdir -p $1; chown postgres:postgres $1; chmod 700 $1
  su postgres -c "$PGBIN/initdb -D $1 -U postgres" >/dev/null 2>&1
  { echo "shared_preload_libraries = 'pg_keyspace'"; echo "pg_keyspace.port = $3"
    echo "pg_keyspace.require_mask = off"; echo "pg_keyspace.durability = 'ephemeral'"
    echo "pg_keyspace.keys = 10000"
    [ -n "$4" ] && echo "pg_keyspace.relay_channels = '$4'"; } >> $1/postgresql.conf
  su postgres -c "$PGBIN/pg_ctl -D $1 -l $1/log -o '-p $2 -k /tmp' -w start" >/dev/null 2>&1
  for i in $(seq 1 40); do $PGBIN/psql -h /tmp -p $2 -U postgres -d postgres -tAc "SELECT 1" 2>/dev/null | grep -q 1 && break; sleep 1; done
}
restart() {
  su postgres -c "$PGBIN/pg_ctl -D $1 -m fast -w restart -o '-p $2 -k /tmp'" >/dev/null 2>&1
  for i in $(seq 1 40); do $PGBIN/psql -h /tmp -p $2 -U postgres -d postgres -tAc "SELECT 1" 2>/dev/null | grep -q 1 && break; sleep 1; done
}
waitresp() { for i in $(seq 1 30); do redis-cli -p $1 PING 2>/dev/null | grep -q PONG && return; sleep 1; done; }

echo "=== two independent instances (A relays, B receives) ==="
start $ADATA $APORT $ARESP "inval:*"
start $BDATA $BPORT $BRESP ""
qa "CREATE EXTENSION pg_keyspace" >/dev/null; qa "CREATE EXTENSION IF NOT EXISTS dblink" >/dev/null
qb "CREATE EXTENSION pg_keyspace" >/dev/null
restart $ADATA $APORT; restart $BDATA $BPORT
waitresp $ARESP; waitresp $BRESP
chk "instance A serves RESP" "PONG" "$(redis-cli -p $ARESP PING 2>&1)"
chk "instance B serves RESP" "PONG" "$(redis-cli -p $BRESP PING 2>&1)"
chk "they are separate postmasters" "1" \
    "$([ "$(head -1 $ADATA/postmaster.pid)" != "$(head -1 $BDATA/postmaster.pid)" ] && echo 1 || echo 0)"
chk "A started its relay worker" "1" "$(grep -c 'relay: forwarding' $ADATA/log)"
chk "B started none (relay_channels empty)" "0" "$(grep -c 'relay: forwarding' $BDATA/log)"

echo ""
echo "########## A. a relayed channel crosses to the peer ##########"
qa "INSERT INTO supacache.peer(name, conninfo) VALUES
      ('b', 'host=/tmp port=$BPORT user=postgres dbname=postgres')
    ON CONFLICT (name) DO UPDATE SET conninfo=EXCLUDED.conninfo, enabled=true" >/dev/null
chk "A has one enabled peer" "1" "$(qa "SELECT count(*) FROM supacache.peer WHERE enabled")"

OUT=/tmp/pgks-relay; rm -f $OUT.*.out
( timeout 20 redis-cli -p $BRESP SUBSCRIBE "inval:users" > $OUT.b.out 2>&1 ) &
( timeout 20 redis-cli -p $ARESP SUBSCRIBE "chat:room" > $OUT.a2.out 2>&1 ) &
for i in $(seq 1 30); do [ "$(grep -l subscribe $OUT.b.out $OUT.a2.out 2>/dev/null | wc -l)" = 2 ] && break; sleep 0.5; done
chk "subscribers registered on both instances" "2" \
    "$(grep -l subscribe $OUT.b.out $OUT.a2.out 2>/dev/null | wc -l | tr -d ' ')"

# Published on A. Nobody on A subscribes to it; the only subscriber is on B.
redis-cli -p $ARESP PUBLISH "inval:users" crossed >/dev/null 2>&1
sleep 3
chk "B's subscriber received a message published on A" "1" "$(grep -c crossed $OUT.b.out 2>/dev/null)"

echo ""
echo "########## B. only the opted-in patterns cross ##########"
# chat:room does not match inval:*, and has a subscriber on A only.
redis-cli -p $ARESP PUBLISH "chat:room" local-only >/dev/null 2>&1
sleep 2
chk "an unlisted channel still reaches A's own subscriber" "1" "$(grep -c local-only $OUT.a2.out 2>/dev/null)"
chk "and did not cross to B" "0" "$(grep -c local-only $OUT.b.out 2>/dev/null)"
wait 2>/dev/null

echo ""
echo "########## C. a relayed message is not relayed onward ##########"
# B has no peers and no relay worker, so nothing can bounce back. The structural
# guarantee is publish_relayed(): it never forwards, whatever B is configured to
# do, which is why A -> B -> A cannot form.
chk "B has no peers configured" "0" "$(qb "SELECT count(*) FROM supacache.peer")"
chk "B ran no relay worker at all" "0" "$(grep -c 'relay: forwarding' $BDATA/log)"

echo ""
echo "########## D. a dead peer does not wedge the relay ##########"
qa "INSERT INTO supacache.peer(name, conninfo) VALUES
      ('dead', 'host=/tmp port=59999 user=postgres dbname=postgres')
    ON CONFLICT (name) DO UPDATE SET conninfo=EXCLUDED.conninfo, enabled=true" >/dev/null
t0=$(date +%s)
redis-cli -p $ARESP PUBLISH "inval:users" with-dead-peer >/dev/null 2>&1
chk "A's RESP port stays responsive while a peer is unreachable" "PONG" "$(redis-cli -p $ARESP PING 2>&1)"
t1=$(date +%s)
chk "and answered promptly (<5s)" "1" "$([ $((t1-t0)) -lt 5 ] && echo 1 || echo 0)"
sleep 4
chk "A's relay worker is still alive" "1" \
    "$(ps -eo args | grep -c 'pg_keyspace: cross-instance rela[y]')"

echo ""
echo "########## E. at-most-once: a message sent while B is down is lost ##########"
# valkey: "if the subscriber is unable to handle the message ... the message is
# forever lost". There is no catch-up, deliberately.
su postgres -c "$PGBIN/pg_ctl -D $BDATA -m fast -w stop" >/dev/null 2>&1
redis-cli -p $ARESP PUBLISH "inval:users" while-b-was-down >/dev/null 2>&1
sleep 2
su postgres -c "$PGBIN/pg_ctl -D $BDATA -l $BDATA/log -o '-p $BPORT -k /tmp' -w start" >/dev/null 2>&1
for i in $(seq 1 40); do qb "SELECT 1" | grep -q 1 && break; sleep 1; done
waitresp $BRESP
rm -f $OUT.b2.out
( timeout 12 redis-cli -p $BRESP SUBSCRIBE "inval:users" > $OUT.b2.out 2>&1 ) &
for i in $(seq 1 20); do grep -q subscribe $OUT.b2.out 2>/dev/null && break; sleep 0.5; done
chk "nothing is replayed to a peer that was down" "0" "$(grep -c while-b-was-down $OUT.b2.out 2>/dev/null)"
# But the link recovers by itself for what comes next.
redis-cli -p $ARESP PUBLISH "inval:users" after-b-returned >/dev/null 2>&1
sleep 3
chk "and the relay resumes once the peer is back" "1" "$(grep -c after-b-returned $OUT.b2.out 2>/dev/null)"
wait 2>/dev/null

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
su postgres -c "$PGBIN/pg_ctl -D $ADATA -m fast -w stop" >/dev/null 2>&1
su postgres -c "$PGBIN/pg_ctl -D $BDATA -m fast -w stop" >/dev/null 2>&1
[ "$fail" -eq 0 ]
