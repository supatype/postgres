#!/usr/bin/env bash
# Per-tenant share of the persistence ring (#43).
#
# Keys are force-scoped to `{tenant}:` for non-exempt roles, and that scoping is
# enforced server-side rather than by convention -- but the prefix is an
# addressing and ACL device and carries no accounting. One shared SPSC ring per
# persist shard, FIFO, no per-tenant share, means a tenant writing hard enough to
# keep the ring full starves every other tenant on the same shard: their writes
# meet a full ring and park.
#
# This measures that directly. One tenant floods with pipelined writes over
# several connections; a second tenant does ordinary sequential writes on one
# connection and is not flooding anything. The same load runs twice, once with
# pg_keyspace.tenant_ring_share off (first come, first served -- the behaviour
# before #43) and once with it on, and the two are printed side by side.
#
# The victim's numbers are the point. The flood's are printed too, because a
# fairness policy that fixes the victim by wrecking everyone is not a fix.
#
# Self-contained: builds the extension, creates and destroys its own cluster.
# Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-fairness-data}
PORT=${PGKS_PG_PORT:-5437}
RESP=${PGKS_RESP_PORT:-6397}
PROFILE=${PGKS_BUILD_PROFILE:-release}
# The ring has to be small enough to actually fill, or there is no contention to
# be fair about and both runs measure the same thing.
RING_MB=${PGKS_FAIRNESS_RING_MB:-1}
SECONDS_PER_RUN=${PGKS_FAIRNESS_SECONDS:-10}
FLOOD_CONNS=${PGKS_FAIRNESS_FLOOD_CONNS:-8}
VALSIZE=${PGKS_FAIRNESS_VALSIZE:-1024}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
set_conf() {
  sed -i "s/^$1 = .*/$1 = $2/" $PGDATA/postgresql.conf 2>/dev/null
  grep -q "^$1" $PGDATA/postgresql.conf || echo "$1 = $2" >> $PGDATA/postgresql.conf
}

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/fairness_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/fairness_install.log; exit 1; }
# Say what was built, so a run against a stale .so cannot be mistaken for a
# result. An earlier harness in this tree measured a binary it had not built.
echo "installed (tenant-share call sites in source: $(grep -c would_exceed ../core/src/server.rs))"

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  # Durable, because that is where a full ring is felt: the reply is held until
  # the record commits, so a tenant shut out of the ring is a tenant whose
  # writes do not complete.
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.keys = 200000"
  echo "pg_keyspace.val_bytes = 2048"
  echo "pg_keyspace.ring_mb = $RING_MB"
  echo "pg_keyspace.persist_window_ms = 10"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
psql_ "CREATE EXTENSION pg_keyspace" >/dev/null
# The worker creates supacache.resp_credential and supacache.acl itself, and only
# once CREATE EXTENSION has run -- so the credentials cannot be inserted until
# after a restart. Inserting them before it silently does nothing, and the whole
# run then measures a cluster with no auth, no tenants and nothing to be fair
# about. That is how the first version of this harness produced two sets of
# plausible numbers that meant nothing.
stop_pg; sleep 1; start_pg; wait_ready; sleep 2
chk "the worker created its credential table" "1" \
    "$(psql_ "SELECT count(*) FROM pg_tables WHERE schemaname='supacache' AND tablename='resp_credential'")"
# Two tenants on one ring, each allowed to write its own namespace. The `{tenant}:`
# prefix is added server-side, so both write the same client-facing key names.
psql_ "INSERT INTO supacache.resp_credential(username,secret,role_name,tenant) VALUES
         ('flood','pw1','tenant_a','ta'),('victim','pw2','tenant_b','tb')
       ON CONFLICT(username) DO UPDATE SET secret=EXCLUDED.secret,
         role_name=EXCLUDED.role_name, tenant=EXCLUDED.tenant" >/dev/null
psql_ "INSERT INTO supacache.acl(role_name,prefix,can_read,can_write) VALUES
         ('tenant_a','',true,true),('tenant_b','',true,true)
       ON CONFLICT DO NOTHING" >/dev/null
chk "two credentials are configured" "2" "$(psql_ "SELECT count(*) FROM supacache.resp_credential")"
stop_pg; sleep 1; start_pg; wait_ready; sleep 2

# Guards, because everything below is meaningless without them. AUTH replies +OK
# even when the worker loaded no credentials at all, so a successful AUTH proves
# nothing -- and a tenant with no scope is not accounted, which makes the share
# inert and the comparison a comparison of noise.
chk "AUTH is required, so credentials really loaded" "1" \
    "$(redis-cli -p $RESP --no-auth-warning SET noauth v 2>&1 | grep -c NOAUTH)"
chk "both tenants can write" "OK" \
    "$(redis-cli -p $RESP --user victim -a pw2 --no-auth-warning SET probe v 2>/dev/null)"
sleep 1
# The stored key must carry the tenant prefix. Without it there is no tenant to
# charge ring bytes to, and the share never engages.
chk "writes are tenant-scoped server-side (tb:probe)" "1" \
    "$(psql_ "SELECT count(*) FROM supacache.kv WHERE key = 'tb:probe'::bytea")"

LOADER=/tmp/pgks_fairness_loader.py
cat > $LOADER <<'PYEOF'
"""Two tenants on one ring: one floods, one does ordinary sequential writes.

The victim is deliberately a single connection issuing one write at a time and
waiting for the reply, because that is the shape of traffic a fairness problem
actually hurts -- a pipelining victim would hide behind its own queue depth.
"""
import socket, sys, threading, time

args = dict(a.split('=', 1) for a in sys.argv[1:])
port     = int(args['port'])
secs     = float(args['secs'])
conns    = int(args['conns'])
valsize  = int(args['valsize'])

val = b'x' * valsize
stop_at = [0.0]


def resp(*parts):
    out = b'*%d\r\n' % len(parts)
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        out += b'$%d\r\n%s\r\n' % (len(p), p)
    return out


def auth(s, user, pw):
    s.sendall(resp('AUTH', user, pw))
    buf = b''
    while b'\r\n' not in buf:
        buf += s.recv(4096)
    if not buf.startswith(b'+OK'):
        raise SystemExit('auth failed: %r' % buf[:80])


flood = {'ok': 0}


def flood_conn(idx):
    """Pipelined writes, replies drained so the socket never stalls the sender."""
    s = socket.create_connection(('127.0.0.1', port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    auth(s, 'flood', 'pw1')
    got = [0]
    running = [True]

    def reader():
        while running[0]:
            try:
                d = s.recv(1 << 16)
            except OSError:
                return
            if not d:
                return
            got[0] += d.count(b'+OK\r\n')

    t = threading.Thread(target=reader, daemon=True)
    t.start()
    n = 0
    try:
        while time.time() < stop_at[0]:
            batch = b''.join(resp('SET', 'k%d:%d' % (idx, n + i), val) for i in range(32))
            n += 32
            s.sendall(batch)
    except OSError:
        pass
    time.sleep(0.5)
    running[0] = False
    try:
        s.close()
    except OSError:
        pass
    flood['ok'] += got[0]


victim = {'ok': 0, 'lat': [], 'first': None}
vlock = threading.Lock()


def victim_conn(idx):
    """One write at a time per connection, waiting for each reply.

    Several connections rather than one, because a single synchronous durable
    writer completes so few writes in the window that scheduling noise dominates
    the effect being measured -- an earlier version of this used one and produced
    counts between 0 and 38 that sometimes ranked the two configurations the
    wrong way round.
    """
    s = socket.create_connection(('127.0.0.1', port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    s.settimeout(30)
    auth(s, 'victim', 'pw2')
    n = 0
    while time.time() < stop_at[0]:
        t0 = time.time()
        try:
            s.sendall(resp('SET', 'v%d_%d' % (idx, n), val))
            buf = b''
            while b'\r\n' not in buf:
                d = s.recv(4096)
                if not d:
                    return
                buf += d
        except OSError:
            return
        done_at = time.time()
        dt = (done_at - t0) * 1000.0
        # A write still in flight when the window closes completes only once the
        # flood stops and the backlog drains, so counting it reports a ten-second
        # latency that is an artefact of the run ending rather than of anything
        # the victim experienced. Only writes that finished inside the window
        # count.
        if done_at > stop_at[0]:
            break
        if buf.startswith(b'+OK'):
            with vlock:
                victim['ok'] += 1
                victim['lat'].append(dt)
                if victim['first'] is None:
                    victim['first'] = (done_at - started) * 1000.0
        n += 1
    s.close()


started = time.time()
stop_at[0] = started + secs
victims = int(args.get('victims', '4'))
threads = [threading.Thread(target=flood_conn, args=(i,)) for i in range(conns)]
threads += [threading.Thread(target=victim_conn, args=(i,)) for i in range(victims)]
for t in threads:
    t.start()
for t in threads:
    t.join()

lat = sorted(victim['lat'])


def pct(p):
    if not lat:
        return 0.0
    return lat[min(len(lat) - 1, int(len(lat) * p))]


# When the victim's first write completed says whether a long tail is a startup
# stall or an ongoing one -- the difference between "shut out until the share
# engages" and "still shut out".
print('victim_ops=%d victim_first_ms=%.0f victim_p50=%.1f victim_p99=%.1f victim_max=%.1f flood_ops=%d'
      % (victim['ok'], victim['first'] if victim['first'] is not None else -1,
         pct(0.50), pct(0.99), max(lat) if lat else 0.0, flood['ok']))
PYEOF

# One round is a sample, not a result: the victim's count is small enough that
# scheduling noise can rank two configurations the wrong way round. Several
# rounds are summed, and the per-round values are printed so a reader can see
# the spread rather than take the total on trust.
ROUNDS=${PGKS_FAIRNESS_ROUNDS:-3}
VICTIM_CONNS=${PGKS_FAIRNESS_VICTIM_CONNS:-4}
run_load() {
  local tv=0 tf=0 tp50=0 per=""
  local r out v f p
  for r in $(seq 1 $ROUNDS); do
    out=$(python3 $LOADER port=$RESP secs=$SECONDS_PER_RUN conns=$FLOOD_CONNS \
          valsize=$VALSIZE victims=$VICTIM_CONNS 2>&1 | tail -1)
    v=$(echo "$out" | tr ' ' '\n' | grep '^victim_ops=' | cut -d= -f2)
    f=$(echo "$out" | tr ' ' '\n' | grep '^flood_ops=' | cut -d= -f2)
    p=$(echo "$out" | tr ' ' '\n' | grep '^victim_p50=' | cut -d= -f2)
    tv=$((tv + ${v:-0})); tf=$((tf + ${f:-0})); per="$per ${v:-0}"
    tp50=$p
  done
  echo "victim_ops=$tv victim_p50=$tp50 flood_ops=$tf victim_rounds=$per"
}
field() { echo "$1" | tr ' ' '\n' | grep "^$2=" | cut -d= -f2; }

echo ""
echo "########## first come, first served (tenant_ring_share = off) ##########"
stop_pg; sleep 1
set_conf "pg_keyspace.tenant_ring_share" "off"
start_pg; wait_ready; sleep 2
chk "the share is off for this run" "off" "$(psql_ "SHOW pg_keyspace.tenant_ring_share")"
OFF=$(run_load)
echo "  $OFF"
echo "  ($ROUNDS rounds of ${SECONDS_PER_RUN}s, $VICTIM_CONNS victim connections)"

echo ""
echo "########## per-tenant share (tenant_ring_share = on) ##########"
stop_pg; sleep 1
set_conf "pg_keyspace.tenant_ring_share" "on"
start_pg; wait_ready; sleep 2
chk "the share is on for this run" "on" "$(psql_ "SHOW pg_keyspace.tenant_ring_share")"
ON=$(run_load)
echo "  $ON"

OFF_V=$(field "$OFF" victim_ops); ON_V=$(field "$ON" victim_ops)
OFF_F=$(field "$OFF" flood_ops);  ON_F=$(field "$ON" flood_ops)
OFF_P50=$(field "$OFF" victim_p50); ON_P50=$(field "$ON" victim_p50)

echo ""
echo "================================================"
printf "%-22s %12s %12s\n" "" "share off" "share on"
printf "%-22s %12s %12s\n" "victim writes" "${OFF_V:-0}" "${ON_V:-0}"
printf "%-22s %12s %12s\n" "victim p50 (ms)" "${OFF_P50:-0}" "${ON_P50:-0}"
printf "%-22s %12s %12s\n" "flood writes" "${OFF_F:-0}" "${ON_F:-0}"
echo "================================================"

# The load has to have been a load, or the comparison is between two idle runs.
chk "the flood actually loaded the ring" "1" "$([ "${OFF_F:-0}" -gt 1000 ] && echo 1 || echo 0)"
# The point of the change.
chk "the share gets the victim more writes through (${OFF_V:-0} -> ${ON_V:-0})" "1" \
    "$([ "${ON_V:-0}" -gt "${OFF_V:-0}" ] && echo 1 || echo 0)"
# And does not do it by starving the tenant it is policing: the flood is capped,
# not stopped. A tenth of its unpoliced rate would be a different bug.
chk "and the flood is capped rather than stopped (${OFF_F:-0} -> ${ON_F:-0})" "1" \
    "$([ "${ON_F:-0}" -gt $(( ${OFF_F:-0} / 4 )) ] && echo 1 || echo 0)"
# Usage is visible, not just enforced: INFO reports what a tenant has in flight.
INFO_SELF=$(redis-cli -p $RESP --user victim -a pw2 --no-auth-warning INFO 2>/dev/null | grep -c '^tenant_')
chk "a tenant can see its own ring usage through INFO" "1" \
    "$([ "${INFO_SELF:-0}" -ge 1 ] && echo 1 || echo 0)"
# And cannot see anyone else's: the namespace is an isolation boundary, so the
# accounting for it must not become a way around it.
chk "and cannot see another tenant's" "0" \
    "$(redis-cli -p $RESP --user victim -a pw2 --no-auth-warning INFO 2>/dev/null | grep -c '^tenant_ta')"

echo ""
echo "########## cache memory: a cold flood must not evict another tenant ##########"
# The other axis #43 names: "a cold-key flood from one tenant simply evicts
# everyone else. CLOCK eviction has no admission control, so cold keys are
# admitted unconditionally and evict hot ones."
#
# The victim writes a small working set once and then stops. The flood writes
# cold keys until the arena has turned over many times. The measurement is how
# much of the victim's set is still there afterwards -- run once with
# pg_keyspace.tenant_scoped_eviction off and once with it on.
#
# Ephemeral tier and a small keyspace, so this measures the cache arena rather
# than the persistence ring the sections above measure.
VICTIM_KEYS=${PGKS_FAIRNESS_VICTIM_KEYS:-200}
FLOOD_KEYS=${PGKS_FAIRNESS_FLOOD_KEYS:-40000}

evict_run() {
  local scoped=$1
  stop_pg; sleep 1
  set_conf "pg_keyspace.tenant_scoped_eviction" "$scoped"
  set_conf "pg_keyspace.durability" "'ephemeral'"
  set_conf "pg_keyspace.keys" "2000"
  set_conf "pg_keyspace.val_bytes" "256"
  start_pg; wait_ready || { echo "NO START"; return 1; }
  local i
  for i in $(seq 1 $VICTIM_KEYS); do
    redis-cli -p $RESP --user victim -a pw2 --no-auth-warning SET "w$i" "$(printf 'v%.0s' $(seq 1 64))" >/dev/null 2>&1
  done
  # One pipelined stream of cold keys from the other tenant.
  { for i in $(seq 1 $FLOOD_KEYS); do echo "SET c$i vvvvvvvvvvvvvvvvvvvvvvvvvvvvvvvv"; done; } \
    | redis-cli -p $RESP --user flood -a pw1 --no-auth-warning --pipe >/dev/null 2>&1
  local survived=0
  for i in $(seq 1 $VICTIM_KEYS); do
    [ -n "$(redis-cli -p $RESP --user victim -a pw2 --no-auth-warning GET "w$i" 2>/dev/null)" ] \
      && survived=$((survived+1))
  done
  echo "$survived"
}

EV_OFF=$(evict_run off)
echo "  scoped eviction off: $EV_OFF/$VICTIM_KEYS of the victim's keys survived"
EV_ON=$(evict_run on)
echo "  scoped eviction on:  $EV_ON/$VICTIM_KEYS of the victim's keys survived"

# Guard first: if the flood did not actually cause eviction there is nothing to
# be fair about and both numbers are meaningless.
chk "the flood evicted from the cache" "1" \
    "$([ "$(psql_ "SELECT sum(evictions) > 0 FROM supacache.stats()")" = "t" ] && echo 1 || echo 0)"
chk "scoped eviction protects the victim tenant (${EV_OFF:-0} -> ${EV_ON:-0} of $VICTIM_KEYS)" "1" \
    "$([ "${EV_ON:-0}" -gt "${EV_OFF:-0}" ] && echo 1 || echo 0)"
# And it must stay a preference rather than becoming a budget: the flooding
# tenant's own recent keys are still there, so it was not simply shut out.
chk "the flooding tenant still holds its own recent keys" "1" \
    "$([ -n "$(redis-cli -p $RESP --user flood -a pw1 --no-auth-warning GET "c$FLOOD_KEYS" 2>/dev/null)" ] && echo 1 || echo 0)"

stop_pg; rm -rf $PGDATA
echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
