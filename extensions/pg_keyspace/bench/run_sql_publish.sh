#!/bin/bash
# supacache.publish(): a SQL backend reaching RESP subscribers, as a tenant.
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
# The other half is who may publish, and under what name. RESP channels are
# force-scoped `{tenant}:` per credential, and a Postgres role is not a
# credential, so the caller's tenant comes from `pg_keyspace.tenant` -- a SUSET
# GUC an operator sets cluster-wide or per role, which a role can neither set
# nor reset for itself. Sections C-E are that boundary: every way a caller might
# try to assert its own tenant, and the two ways an operator may grant one.
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
# As an arbitrary role, which is the whole point of most of what follows.
as() { local r=$1; shift; $PGBIN/psql -h /tmp -p $PORT -U "$r" -d postgres -tAc "$1" 2>&1; }
restart_pg() {
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -m fast -w stop" >/dev/null 2>&1
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o '-p $PORT -k /tmp' -w start" >/dev/null 2>&1
  for i in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && break; sleep 1; done
}
# Every worker's port, not just worker 0's: they come up seconds apart, and a
# subscriber that connects before its worker is listening proves nothing.
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
restart_pg
chk "all $WORKERS workers are listening" "$WORKERS" "$(wait_workers)"

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
echo "########## C. a caller cannot assert its own tenant ##########"
# Granted every privilege there is, so that whatever refuses below is the check
# in the function and not a missing GRANT. That distinction is the point: a
# boundary a well-meaning grant can remove is not a boundary.
for r in tenant_a tenant_b tenant_c; do
  psql_ "CREATE ROLE $r LOGIN" >/dev/null 2>&1
  psql_ "GRANT USAGE ON SCHEMA supacache TO $r" >/dev/null 2>&1
  psql_ "GRANT EXECUTE ON FUNCTION supacache.publish(text,bytea) TO $r" >/dev/null 2>&1
done

chk "pg_keyspace.tenant is SUSET, so a role cannot set it" "superuser" \
    "$(psql_ "SELECT context FROM pg_settings WHERE name='pg_keyspace.tenant'")"
chk "and a role that tries is refused by Postgres" "1" \
    "$(as tenant_a "SET pg_keyspace.tenant='ta'" | grep -c 'permission denied to set parameter')"

DENIED=$(as tenant_a "SELECT supacache.publish('demo','sneaky'::bytea)" | head -1)
chk "with no tenant configured, a non-superuser is refused" "1" \
    "$(echo "$DENIED" | grep -c 'superuser-only until pg_keyspace.tenant is set')"
# Refused by THIS check, not by the schema: a "permission denied for schema"
# would mean the grants above were incomplete and the guard never ran.
chk "and refused by the function, not the schema" "0" \
    "$(echo "$DENIED" | grep -c 'permission denied for schema')"

# PG15+ lets an operator hand over the right to SET a SUSET parameter. That is
# the one way a caller can put a value there itself, so the value's *source* is
# checked rather than just its presence.
psql_ "GRANT SET ON PARAMETER pg_keyspace.tenant TO tenant_b" >/dev/null 2>&1
chk "a granted role really can SET it now" "session" \
    "$(as tenant_b "SET pg_keyspace.tenant='tb'; SELECT source FROM pg_settings WHERE name='pg_keyspace.tenant'" | tail -1)"
chk "but a session-set tenant is refused as self-asserted" "1" \
    "$(as tenant_b "SET pg_keyspace.tenant='tb'; SELECT supacache.publish('demo','x'::bytea)" | grep -c 'set in this session rather than by the operator')"
# The same attempt aimed at somebody else's tenant, which is what the check is
# actually for.
chk "including when it names another tenant" "1" \
    "$(as tenant_b "SET pg_keyspace.tenant='ta'; SELECT supacache.publish('demo','x'::bytea)" | grep -c 'set in this session rather than by the operator')"

echo ""
echo "########## D. an operator-granted tenant scopes the channel ##########"
# ALTER ROLE ... SET on a SUSET GUC applies at that role's login and cannot be
# overridden or RESET by the role: per-role tenancy with no table of our own,
# held in Postgres's pg_db_role_setting.
psql_ "ALTER ROLE tenant_a SET pg_keyspace.tenant = 'ta'" >/dev/null
psql_ "ALTER ROLE tenant_b SET pg_keyspace.tenant = 'tb'" >/dev/null
chk "the per-role value arrives at login, from the operator" "ta|user" \
    "$(as tenant_a "SELECT current_setting('pg_keyspace.tenant') || '|' || source FROM pg_settings WHERE name='pg_keyspace.tenant'")"
chk "and the role still cannot reset it" "1" \
    "$(as tenant_a "RESET pg_keyspace.tenant" | grep -c 'permission denied to set parameter')"

rm -f $OUT.ta.out $OUT.tb.out
( timeout 15 redis-cli -p $RESP        SUBSCRIBE ta:demo > $OUT.ta.out 2>&1 ) &
( timeout 15 redis-cli -p $((RESP+1))  SUBSCRIBE tb:demo > $OUT.tb.out 2>&1 ) &
for i in $(seq 1 30); do
  [ "$(grep -l subscribe $OUT.ta.out $OUT.tb.out 2>/dev/null | wc -l)" = 2 ] && break; sleep 0.5
done
chk "both tenant subscribers registered" "2" \
    "$(grep -l subscribe $OUT.ta.out $OUT.tb.out 2>/dev/null | wc -l | tr -d ' ')"
# tenant_a publishes the bare name; only the ta: subscriber may see it, and the
# count must be 1 -- a count of 2 would mean the prefix was not applied.
chk "tenant_a publishing 'demo' reaches exactly one subscriber" "1" \
    "$(as tenant_a "SELECT supacache.publish('demo','from-ta'::bytea)")"
# Twice in one session, because the second call answers from the per-backend
# memo of where the value came from rather than looking it up again. The memo is
# keyed on the value, so a hit must give the same scope the lookup gave.
chk "a second publish in the same session is scoped the same" "1|1" \
    "$(as tenant_a "SELECT supacache.publish('demo','memo-a'::bytea) || '|' || supacache.publish('demo','memo-b'::bytea)")"
sleep 1
wait 2>/dev/null
chk "and it was the ta: one" "1" "$(grep -c 'from-ta' $OUT.ta.out 2>/dev/null)"
chk "the memo-path messages arrived there too" "2" \
    "$(grep -cE 'memo-a|memo-b' $OUT.ta.out 2>/dev/null)"
chk "tenant_b's subscriber saw none of them" "0" \
    "$(grep -cE 'from-ta|memo-a|memo-b' $OUT.tb.out 2>/dev/null)"

echo ""
echo "########## E. cluster-wide tenancy, and an authenticated subscriber ##########"
# A single-tenant cluster configures the tenant once in postgresql.conf instead
# of per role. tenant_c has no ALTER ROLE setting, so it inherits this one.
echo "pg_keyspace.tenant = 'tc'" >> $PGDATA/postgresql.conf
restart_pg
chk "workers are back" "$WORKERS" "$(wait_workers)"
chk "tenant_c inherits the cluster-wide value" "tc|configuration file" \
    "$(as tenant_c "SELECT current_setting('pg_keyspace.tenant') || '|' || source FROM pg_settings WHERE name='pg_keyspace.tenant'")"
# A per-role value still wins over it, which is what makes one mechanism serve
# both a single-tenant and a multi-tenant cluster.
chk "and a per-role value still overrides it" "ta" \
    "$(as tenant_a "SELECT current_setting('pg_keyspace.tenant')")"

rm -f $OUT.tc.out
( timeout 15 redis-cli -p $RESP SUBSCRIBE tc:demo > $OUT.tc.out 2>&1 ) &
for i in $(seq 1 30); do grep -q subscribe $OUT.tc.out 2>/dev/null && break; sleep 0.5; done
chk "tenant_c's publish is scoped by the config-file value" "1" \
    "$(as tenant_c "SELECT supacache.publish('demo','from-tc'::bytea)")"
sleep 1; wait 2>/dev/null
chk "and arrived on tc:demo" "1" "$(grep -c 'from-tc' $OUT.tc.out 2>/dev/null)"

# Now the end-to-end claim: a RESP client authenticated as a tenant subscribes
# to the bare name, because its credential forces the same prefix -- so a SQL
# publish by the matching role lands exactly where that client is listening.
psql_ "INSERT INTO supacache.resp_credential(username,secret,role_name,tenant) VALUES
         ('sub_a','pw1','role_a','ta'),('sub_b','pw2','role_b','tb')
       ON CONFLICT(username) DO UPDATE SET secret=EXCLUDED.secret,
         role_name=EXCLUDED.role_name, tenant=EXCLUDED.tenant" >/dev/null
psql_ "INSERT INTO supacache.acl(role_name,prefix,can_read,can_write) VALUES
         ('role_a','',true,true),('role_b','',true,true)
       ON CONFLICT DO NOTHING" >/dev/null
restart_pg
chk "workers are back with credentials loaded" "$WORKERS" "$(wait_workers)"
# AUTH replies +OK even when no credentials loaded, so prove it the other way.
chk "AUTH is now required, so the credentials really loaded" "1" \
    "$(redis-cli -p $RESP --no-auth-warning SUBSCRIBE nope 2>&1 | grep -c NOAUTH)"

rm -f $OUT.auth_a.out $OUT.auth_b.out
( timeout 15 redis-cli -p $RESP       --user sub_a -a pw1 --no-auth-warning SUBSCRIBE demo > $OUT.auth_a.out 2>&1 ) &
( timeout 15 redis-cli -p $((RESP+1)) --user sub_b -a pw2 --no-auth-warning SUBSCRIBE demo > $OUT.auth_b.out 2>&1 ) &
for i in $(seq 1 30); do
  [ "$(grep -l subscribe $OUT.auth_a.out $OUT.auth_b.out 2>/dev/null | wc -l)" = 2 ] && break; sleep 0.5
done
chk "both authenticated subscribers registered" "2" \
    "$(grep -l subscribe $OUT.auth_a.out $OUT.auth_b.out 2>/dev/null | wc -l | tr -d ' ')"
chk "tenant_a's SQL publish reaches the authenticated ta subscriber" "1" \
    "$(as tenant_a "SELECT supacache.publish('demo','sql-to-authed'::bytea)")"
# A superuser bypasses scoping and publishes the name as given -- so naming the
# scoped channel outright reaches the same subscriber. Were the cluster-wide
# 'tc' applied to superusers too, this would land on tc:ta:demo and reach nobody.
chk "a superuser publishing 'ta:demo' reaches it too, unscoped" "1" \
    "$(psql_ "SELECT supacache.publish('ta:demo','su-to-authed'::bytea)")"
sleep 1; wait 2>/dev/null
chk "the authenticated subscriber received the tenant's message" "1" \
    "$(grep -c 'sql-to-authed' $OUT.auth_a.out 2>/dev/null)"
chk "and the superuser's" "1" "$(grep -c 'su-to-authed' $OUT.auth_a.out 2>/dev/null)"
chk "the other tenant's subscriber received neither" "0" \
    "$(grep -cE 'sql-to-authed|su-to-authed' $OUT.auth_b.out 2>/dev/null)"
chk "still nothing dropped" "0" "$(psql_ "SELECT dropped FROM supacache.pubsub_stats()")"

echo ""
echo "########## F. from a trigger, which is why this exists ##########"
# The motivating case, and the one that exercises publish() from inside another
# SPI context rather than at the top of a statement -- the tenant lookup reads
# pg_settings through SPI itself, so nesting is worth asserting rather than
# assuming.
psql_ "CREATE TABLE public.orders(id int)" >/dev/null
psql_ "GRANT INSERT ON public.orders TO tenant_a" >/dev/null
psql_ "CREATE FUNCTION public.notify_order() RETURNS trigger LANGUAGE plpgsql AS \
       \$\$ BEGIN PERFORM supacache.publish('orders', NEW.id::text::bytea); RETURN NEW; END \$\$" >/dev/null
psql_ "CREATE TRIGGER orders_pub AFTER INSERT ON public.orders \
       FOR EACH ROW EXECUTE FUNCTION public.notify_order()" >/dev/null
psql_ "GRANT EXECUTE ON FUNCTION public.notify_order() TO tenant_a" >/dev/null

rm -f $OUT.trig.out
( timeout 15 redis-cli -p $RESP --user sub_a -a pw1 --no-auth-warning SUBSCRIBE orders > $OUT.trig.out 2>&1 ) &
for i in $(seq 1 30); do grep -q subscribe $OUT.trig.out 2>/dev/null && break; sleep 0.5; done
chk "the trigger's subscriber registered" "1" "$(grep -c subscribe $OUT.trig.out 2>/dev/null)"
# Three rows in one statement: three trigger firings, three publishes, all from
# inside one SPI nest.
chk "an INSERT by the tenant runs the trigger" "INSERT 0 3" \
    "$($PGBIN/psql -h /tmp -p $PORT -U tenant_a -d postgres -c "INSERT INTO public.orders VALUES (1),(2),(3)" 2>&1 | tail -1)"
sleep 1; wait 2>/dev/null
chk "and the RESP subscriber got all three" "3" \
    "$(grep -c '^message$' $OUT.trig.out 2>/dev/null)"
chk "with the payloads the trigger sent" "1,2,3," \
    "$(sed -n '4,$p' $OUT.trig.out 2>/dev/null | awk 'NR%3==0' | tr '\n' ',')"
# The client is told the name it subscribed to, not the scoped one the bus
# routed on -- the same unwrapping a RESP publish gets.
chk "reported under the name it subscribed to, not ta:orders" "orders,orders,orders," \
    "$(sed -n '4,$p' $OUT.trig.out 2>/dev/null | awk 'NR%3==2' | tr '\n' ',')"

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
su postgres -c "$PGBIN/pg_ctl -D $PGDATA -m fast -w stop" >/dev/null 2>&1
[ "$fail" -eq 0 ]
