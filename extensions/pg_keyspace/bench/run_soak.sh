#!/usr/bin/env bash
# Sustained, multi-tenant, fault-injected soak with a drift verdict (#113).
#
# WHAT THIS CAN AND CANNOT ESTABLISH
#
# #113 is gated on hardware, not on code. Running the generator on the same box
# as the server makes every throughput comparison meaningless -- measured on a
# 4-vCPU container, pg_keyspace and redis 7.0.15 tracked within 0.6% and BOTH
# kneed at 20k ops/s, which redis does not do on real hardware. So this script
# does not report a headline rate as a result, and SPLIT=1 exists so the
# generator can live somewhere else (see "Split-host" below).
#
# What it does establish, on any hardware:
#
#   * CORRECTNESS under concurrency. Every read verifies a checksum derived
#     from its own key, and after the run every persisted row is checked
#     against the same function. A cache confidently returning the wrong bytes
#     fails; a soak that only checks for nulls cannot see that.
#   * DRIFT. RSS, arena occupancy, eviction rate, persist lag, WAL growth and
#     decode lag are sampled throughout and judged as trends. These are the
#     questions a two-minute run structurally cannot answer, and they are why
#     this waited on #111's views.
#   * RECOVERY UNDER LOAD. Workers are terminated while traffic runs, which is
#     the only condition under which recovery ever actually happens.
#
# Duration is a parameter and the default is short enough to run in CI. A real
# answer to #113 needs DURATION=4h or more, on a separate generator host.
#
# Split-host:
#   on the server:    bench/soak/drift.sh sample drift.csv
#   on the generator: HOST=<server> PORT=6380 k6 run bench/k6/mixed.js
#   afterwards:       bench/soak/drift.sh judge drift.csv
#
# Env: DURATION PEAK TENANTS WORKERS FAULT_EVERY SAMPLE_SECS PGBIN NO_FAULTS
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
export PGDATA=${PGDATA:-/tmp/pgks-soak}
PORT=${PGKS_PG_PORT:-5455}
RESP=${PGKS_RESP_PORT:-6441}
PROFILE=${PGKS_BUILD_PROFILE:-release}
DURATION=${DURATION:-5m}
PEAK=${PEAK:-24}
TENANTS=${TENANTS:-3}
WORKERS=${WORKERS:-2}
SHARDS=${PGKS_PERSIST_WORKERS:-2}
# The invalidation POOL (#120). One variable, because the conf below and three
# separate worker-count assertions have to agree on it, and when they did not
# the soak waited out a 60s readiness loop that could never succeed and then
# failed its own end-of-run check.
POOL=${POOL:-2}
FAULT_EVERY=${FAULT_EVERY:-45}
export SAMPLE_SECS=${SAMPLE_SECS:-5}
SECRET=${SECRET:-soakpw}
OUT=${OUT:-/tmp/pgks-soak-out}
K6=${K6:-$(command -v k6 || echo "$HOME/go/bin/k6")}
NO_FAULTS=${NO_FAULTS:-}
# Extra row-cache databases beyond `postgres` (#120). 0 keeps the original
# single-database shape. Above 0, each gets its own registered table, its own
# slot and its own turn in the invalidation pool -- which is the drift this
# design actually puts at risk: slot growth, retained WAL, and cycle-time
# invalidation latency.
SOAK_DATABASES=${SOAK_DATABASES:-0}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
# `pg_ctl -w stop` gives up after its own timeout and returns non-zero with the
# postmaster STILL shutting down. Discarding that status and starting another one
# a second later starts it on top of a live postmaster: that start fails, and
# every readiness poll then reads `FATAL: the database system is shutting down`
# until the loop expires -- surfacing as whichever assertion came next, pointing
# at the feature under test and nothing to do with it (#120). Shutdown length
# tracks how much the persistence worker has to flush, so it bites after a heavy
# section and passes everywhere else. Verify it rather than assume it.
stop_pg() {
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 120 stop -m fast" >/dev/null 2>&1
  for _ in $(seq 1 120); do
    su postgres -c "$PGBIN/pg_ctl -D $PGDATA status" >/dev/null 2>&1 || return 0
    sleep 1
  done
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 60 stop -m immediate" >/dev/null 2>&1
  return 0
}
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }

mkdir -p "$OUT"
[ -x "$K6" ] || { echo "k6 not found (set K6=, or: go install go.k6.io/k6@latest)"; exit 2; }

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/soak_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/soak_install.log; exit 1; }
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/soak_plugin.log 2>&1 || {
  echo "PLUGIN INSTALL FAILED"; tail -20 /tmp/soak_plugin.log; exit 1; }

echo "=== cluster ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.workers = $WORKERS"
  echo "pg_keyspace.persist_workers = $SHARDS"
  # Durable: the tier with a queue behind it, which is the one drift can expose.
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
  echo "pg_keyspace.rowcache_decode = on"
  echo "pg_keyspace.rowcache_readthrough = on"
  echo "pg_keyspace.rowcache_decode_ms = 200"
  # Deliberately SMALLER than the number of databases when SOAK_DATABASES is
  # set, so the run exercises CYCLING rather than a worker per database. A pool
  # large enough to avoid cycling would soak the easy case.
  echo "pg_keyspace.rowcache_invalidation_workers = $POOL"
  echo "pg_keyspace.rowcache_lease_ms = 3000"
  # Deliberately small, so the cold set does not fit and eviction runs for the
  # whole soak. A cache that never evicts is not being soaked -- at 120000 the
  # cache was still filling when a 4-minute run ended -- entries reached 34k of
  # a 50k ceiling with only 1,254 evictions -- and the drift check correctly
  # reported it as not yet at steady state. A multi-hour run should raise this
  # to a realistic size; it is small here so a short run still reaches its
  # ceiling and exercises eviction, which is the point of soaking a cache.
  echo "pg_keyspace.keys = ${PGKS_KEYS:-8000}"
  echo "pg_keyspace.val_bytes = 320"
  echo "pg_keyspace.tenant_arena_pct = 60"
  echo "pg_keyspace.ttl_bucket_secs = 10"
  echo "wal_level = logical"
  # One slot per participating database (#120), plus headroom.
  echo "max_replication_slots = 24"
  # The invalidation pool comes out of this budget alongside the RESP,
  # persistence and expiry workers; the default 8 leaves no room.
  echo "max_worker_processes = 16"
  # THE mitigation for one-slot-per-database: a slot retains WAL until it is
  # consumed, so without a bound one stalled database pins WAL for the whole
  # cluster. Bounded, the server invalidates the slot instead and pg_keyspace
  # marks that database incoherent and rebuilds it -- which the drift judge
  # asserts never had to happen.
  echo "max_slot_wal_keep_size = 2GB"
  echo "max_wal_senders = 8"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi
Q "CREATE EXTENSION pg_keyspace" >/dev/null
restart
for _ in $(seq 1 60); do
  [ "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE alive")" = "$((WORKERS+SHARDS+1+POOL))" ] && break
  sleep 1
done

echo "=== tenants + row cache ==="
for i in $(seq 0 $((TENANTS-1))); do
  Q "SELECT supacache.set_credential('t$i','$SECRET','app','t$i')" >/dev/null
done
# A credential with no ACL row gets NOPERM on every key. The empty prefix is
# "everything this role's tenant scope covers", which is the whole of a tenant's
# keyspace -- the server force-scopes the keys, so this does not cross tenants.
# Without it the first run of this harness did 0 iterations while section 3's
# "no checksum mismatch" passed vacuously; that is what the section 2 guards
# below exist to catch.
Q "INSERT INTO supacache.acl(role_name,prefix,can_read,can_write)
     VALUES ('app','',true,true) ON CONFLICT DO NOTHING" >/dev/null
Q "SELECT pg_reload_conf()" >/dev/null
sleep 2
# The row cache is a SQL path, so pgbench drives it -- which is also how it is
# really used (a PostgREST-shaped pk lookup), rather than through RESP.
Q "CREATE TABLE profiles(id bigint primary key, tenant text NOT NULL, payload text NOT NULL)" >/dev/null
Q "INSERT INTO profiles SELECT g, 't'||(g%$TENANTS), md5(g::text)||repeat('p',200) FROM generate_series(1,50000) g" >/dev/null
chk "the row cache registers the table" "t" "$(Q "SELECT supacache.rowcache_register('public.profiles')")"
for _ in $(seq 1 60); do [ "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache")" = "t" ] && break; sleep 1; done
chk "the row cache is coherent before load" "t" "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache")"

# Extra databases, each a tenant with its own row cache. This is what turns the
# soak into a test of #120 rather than of the single-database path: N slots, N
# turns in the cycle, and N chances for one database's invalidation to stall and
# pin WAL for the whole cluster.
SOAK_DBS=""
if [ "${SOAK_DATABASES:-0}" -gt 0 ]; then
  for i in $(seq 1 "$SOAK_DATABASES"); do
    d="soak_$i"
    Q "CREATE DATABASE $d" >/dev/null
    QD() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d "$d" -tAc "$1" 2>&1; }
    QD "CREATE EXTENSION pg_keyspace" >/dev/null
    QD "CREATE TABLE profiles(id bigint primary key, payload text NOT NULL)" >/dev/null
    QD "INSERT INTO profiles SELECT g, md5(g::text)||repeat('p',200) FROM generate_series(1,20000) g" >/dev/null
    chk "  $d registers its table" "t" "$(QD "SELECT supacache.rowcache_register('public.profiles')")"
    SOAK_DBS="$SOAK_DBS $d"
  done
  for d in $SOAK_DBS; do
    for _ in $(seq 1 120); do
      [ "$($PGBIN/psql -h /tmp -p $PORT -U postgres -d "$d" -tAc \
            "SELECT coherent FROM supacache.rowcache_coherence()" 2>&1)" = "t" ] && break
      sleep 1
    done
    chk "  $d is coherent before load" "t" \
        "$($PGBIN/psql -h /tmp -p $PORT -U postgres -d "$d" -tAc \
           "SELECT coherent FROM supacache.rowcache_coherence()" 2>&1)"
  done
  chk "every soak database is participating" "$SOAK_DATABASES" \
      "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases
            WHERE state='participating' AND datname LIKE 'soak\\_%'")"
  cat > "$OUT/rowcache_db.sql" <<'PG'
\set id random(1, 20000)
SELECT payload FROM profiles WHERE id = :id;
PG
  cat > "$OUT/rowwrite_db.sql" <<'PG'
\set id random(1, 20000)
UPDATE profiles SET payload = md5(random()::text) || repeat('p',200) WHERE id = :id;
PG
fi

cat > "$OUT/rowcache.sql" <<'PG'
\set id random(1, 50000)
SELECT payload FROM profiles WHERE id = :id;
PG
cat > "$OUT/rowwrite.sql" <<'PG'
\set id random(1, 50000)
UPDATE profiles SET payload = md5(random()::text) || repeat('p',200) WHERE id = :id;
PG

echo
echo "########## 0. preconditions ##########"
# RESP + persist + expiry + the invalidation POOL (#120), which is sized by its
# own GUC rather than being a single worker.
SOAK_POOL=$(Q "SHOW pg_keyspace.rowcache_invalidation_workers"); SOAK_POOL=${SOAK_POOL:-1}
chk "the server runs the pool size this soak asked for ($POOL)" "$POOL" "$SOAK_POOL"
chk "$WORKERS RESP workers and $SHARDS persist shards are up" "$((WORKERS+SHARDS+1+POOL))" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE alive")"
chk "the durable tier is actually configured" "durable" "$(Q "SELECT tier FROM supacache.replication_status()")"
# A tenant must be able to AUTH, or mixed.js measures nothing but auth failures.
chk "a tenant can AUTH over RESP" "PONG" \
    "$(redis-cli -p $RESP --user t0 --pass "$SECRET" --no-auth-warning PING 2>&1)"
# ...and must be force-scoped, or "multi-tenant" is a label on one keyspace and
# every fairness path below is being measured on a single tenant's traffic.
#
# `-c` because the keyspace is sharded across workers and a bare redis-cli does
# not follow MOVED. The scope lands in the KEY (`t0:probe`), not in the `tenant`
# column -- checking the column instead is how the first version of this
# assertion failed against a correctly-scoped cluster.
redis-cli -c -p $RESP --user t0 --pass "$SECRET" --no-auth-warning SET probe v1 >/dev/null 2>&1
redis-cli -c -p $RESP --user t1 --pass "$SECRET" --no-auth-warning SET probe v2 >/dev/null 2>&1
for _ in $(seq 1 20); do
  [ "$(Q "SELECT count(*) FROM supacache.kv WHERE convert_from(key,'UTF8') IN ('t0:probe','t1:probe')")" = "2" ] && break
  sleep 1
done
chk "a tenant's keys are force-scoped server-side (t0:probe, t1:probe)" "2" \
    "$(Q "SELECT count(*) FROM supacache.kv WHERE convert_from(key,'UTF8') IN ('t0:probe','t1:probe')")"
# Two tenants wrote the SAME key name and must not have collided -- that is what
# the scoping is for, and a count of 2 above with one value here would mean the
# prefix is cosmetic.
chk "...so two tenants writing one key name do not collide" "v1" \
    "$(redis-cli -c -p $RESP --user t0 --pass "$SECRET" --no-auth-warning GET probe 2>&1)"

echo
echo "########## 1. load ##########"
echo "duration=$DURATION peak=$PEAK tenants=$TENANTS faults=${NO_FAULTS:+off}${NO_FAULTS:-every ${FAULT_EVERY}s}"
SECS=$(awk -v d="$DURATION" 'BEGIN{ if (d ~ /h$/) print substr(d,1,length(d)-1)*3600;
                                    else if (d ~ /m$/) print substr(d,1,length(d)-1)*60;
                                    else print substr(d,1,length(d)-1) }')
PGPORT=$PORT PGBIN=$PGBIN bash "$SCRIPT_DIR/soak/drift.sh" sample "$OUT/drift.csv" &
DRIFT_PID=$!
if [ -z "$NO_FAULTS" ]; then
  PGPORT=$PORT PGBIN=$PGBIN bash "$SCRIPT_DIR/soak/faults.sh" "$FAULT_EVERY" "$OUT/faults.log" &
  FAULTS_PID=$!
fi
# pgbench drives the row cache (reads) and its invalidation (writes) for the
# whole run, concurrently with the RESP load. -T takes seconds.
$PGBIN/pgbench -h /tmp -p $PORT -U postgres -n -c 4 -j 2 -T "$SECS" \
  -f "$OUT/rowcache.sql"@8 -f "$OUT/rowwrite.sql"@2 postgres \
  > "$OUT/pgbench.log" 2>&1 &
PGBENCH_PID=$!
# One generator per extra database, so every participating database is producing
# invalidations for the whole run. A database that is registered but idle would
# never stall its slot, and the WAL-retention hazard would go untested.
DB_PIDS=""
for d in $SOAK_DBS; do
  $PGBIN/pgbench -h /tmp -p $PORT -U postgres -n -c 2 -j 1 -T "$SECS" \
    -f "$OUT/rowcache_db.sql"@8 -f "$OUT/rowwrite_db.sql"@2 "$d" \
    > "$OUT/pgbench_$d.log" 2>&1 &
  DB_PIDS="$DB_PIDS $!"
done

# Each worker kill drops every connection that worker was holding, so a run with
# fault injection has a floor of real errors that is a property of the faults,
# not of the server. Budget it from the schedule rather than from a number that
# happened to pass: peak VUs, times the kills that will fire, times slack for
# the reconnect storm behind each one. With faults off the budget is 0.
if [ -n "$NO_FAULTS" ]; then
  MAX_ERRORS=0
else
  MAX_ERRORS=$(( PEAK * ((SECS / FAULT_EVERY) + 1) * 100 ))
fi
HOST=127.0.0.1 PORT=$RESP WORKERS=$WORKERS TENANTS=$TENANTS SECRET=$SECRET PEAK=$PEAK DURATION=$DURATION \
MAX_ERRORS=$MAX_ERRORS \
LABEL="soak-${DURATION}" SUMMARY_JSON="$OUT/k6.json" \
  "$K6" run --quiet "$SCRIPT_DIR/k6/mixed.js" 2>&1 | tee "$OUT/k6.log"
K6_RC=${PIPESTATUS[0]}

wait $PGBENCH_PID 2>/dev/null
for pid in $DB_PIDS; do wait "$pid" 2>/dev/null; done
# Stop injecting first, then keep SAMPLING through a quiet drain window. The
# last drift sample is then taken against an idle cluster, which is the only
# condition under which "batches still uncommitted" means "never committed"
# rather than "in flight" -- the persistence counter is a gauge, not a tally.
[ -n "${FAULTS_PID:-}" ] && kill $FAULTS_PID 2>/dev/null
sleep "${DRAIN_SECS:-20}"
kill $DRIFT_PID 2>/dev/null
sleep 2

echo
echo "########## 2. the load actually ran ##########"
# Everything below judges a run; these judge that there WAS one. A soak that
# fell over in its first seconds and then reported clean drift is the exact
# failure mode this whole file exists to avoid.
K6_ITERS=$(grep -oP 'iterations: \K[0-9]+' "$OUT/k6.log" | head -1)
VERIFIED=$(grep -oP 'checksums verified: \K[0-9]+' "$OUT/k6.log" | head -1)
MISMATCH=$(grep -oP 'MISMATCHES: \K[0-9]+' "$OUT/k6.log" | head -1)
K6_ERRS=$(grep -oP '^errors: \K[0-9]+' "$OUT/k6.log" | head -1)
chk "k6 ran a meaningful number of iterations (${K6_ITERS:-0})" "t" \
    "$([ "${K6_ITERS:-0}" -gt 10000 ] && echo t || echo f)"
chk "checksums were actually verified (${VERIFIED:-0})" "t" \
    "$([ "${VERIFIED:-0}" -gt 1000 ] && echo t || echo f)"
PGB_TX=$(grep -oP 'number of transactions actually processed: \K[0-9]+' "$OUT/pgbench.log" | head -1)
chk "pgbench drove the row cache (${PGB_TX:-0} tx)" "t" \
    "$([ "${PGB_TX:-0}" -gt 1000 ] && echo t || echo f)"
if [ -z "$NO_FAULTS" ]; then
  # grep -c prints 0 and ALSO exits non-zero, so `|| echo 0` appends a second
  # line and the count becomes "0\n0", which fails an integer test with a shell
  # error rather than a verdict.
  NFAULTS=$(grep -c "after fault_" "$OUT/faults.log" 2>/dev/null); NFAULTS=${NFAULTS:-0}
  chk "faults were injected during the run (${NFAULTS})" "t" \
      "$([ "${NFAULTS:-0}" -ge 1 ] && echo t || echo f)"
fi

echo
echo "########## 3. correctness under concurrency ##########"
# The one invariant that never relaxes, faults or no faults: the cache may miss,
# may drop, may be restarted underneath a client -- it may never hand back bytes
# that are not the ones written.
chk "no checksum mismatch during the run" "0" "${MISMATCH:-none}"
# Errors, unlike mismatches, have a floor when workers are being killed on
# purpose: every kill drops the connections that worker held. Same budget k6
# thresholded on, so the two cannot disagree.
chk "operation errors are within the fault budget (${K6_ERRS:-?} of ${MAX_ERRORS})" "t" \
    "$([ "${K6_ERRS:-999999999}" -le "$MAX_ERRORS" ] && echo t || echo f)"
# With faults off there is no excuse for any of them, and that is the run that
# says whether the budget above is covering up a real problem.
[ -n "$NO_FAULTS" ] && chk "...and with faults off there are none at all" "0" "${K6_ERRS:-none}"
# The in-flight check covers what the cache SERVED. This covers what it
# PERSISTED, which no reader getting a cache hit would ever notice was wrong.
#
# Not a well-formedness proxy: FNV-1a is re-derived here, in SQL, from the key,
# and compared against the tag the client wrote. That closes the loop the whole
# way -- RESP client -> keyspace -> persist ring -> Postgres table -> SQL read --
# which is what #113 means by "write-then-read-back verification with
# checksums".
#
# The stored key is the SCOPED one (`t0:cold:7`); the client hashed the
# unscoped key it sent (`cold:7`), so the prefix is stripped before hashing.
Q "CREATE OR REPLACE FUNCTION public.soak_fnv1a32(s text) RETURNS bigint AS \$\$
   DECLARE h bigint := 2166136261; i int;
   BEGIN
     FOR i IN 1..length(s) LOOP
       h := h # ascii(substr(s,i,1));
       -- The client's shift-and-add is exactly a multiply by the FNV prime:
       -- 1 + 2 + 16 + 128 + 256 + 16777216 = 16777619.
       h := (h * 16777619) & 4294967295;
     END LOOP;
     RETURN h;
   END \$\$ LANGUAGE plpgsql IMMUTABLE" >/dev/null
# Not pg_temp: every Q() opens its own psql session, so a temp function is gone
# by the next statement -- the first version failed with `schema "pg_temp" does
# not exist` on the very query it was created for.
CHECKED=$(Q "SELECT count(*) FROM supacache.kv
             WHERE kind='s' AND val IS NOT NULL
               AND convert_from(key,'UTF8') ~ '^t[0-9]+:(cold|hot):'")
chk "there were persisted rows to verify (${CHECKED})" "t" \
    "$([ "${CHECKED:-0}" -gt 100 ] && echo t || echo f)"
BAD=$(Q "SELECT count(*) FROM supacache.kv
         WHERE kind='s' AND val IS NOT NULL
           AND convert_from(key,'UTF8') ~ '^t[0-9]+:(cold|hot):'
           AND lpad(to_hex(public.soak_fnv1a32(
                 regexp_replace(convert_from(key,'UTF8'), '^t[0-9]+:', ''))), 8, '0')
               <> left(convert_from(val,'UTF8'), 8)")
chk "every persisted row's checksum matches its key" "0" "$BAD"
# The checksum function must be able to FAIL, or the line above is decoration.
chk "...and the check would catch a corrupted one" "1" \
    "$(Q "SELECT count(*) FROM (SELECT 'cold:1'::text k, 'deadbeef'::text v) t
          WHERE lpad(to_hex(public.soak_fnv1a32(t.k)),8,'0') <> left(t.v,8)")"

echo
echo "########## 4. the cluster survived ##########"
# RESP + persist + expiry + the pool. The old "+2" assumed a single
# invalidation worker, which #120 replaced with a pool sized by its GUC -- so a
# multi-database soak, the one shape this file exists to exercise, reported
# 7 alive against an expected 6 and failed while nothing was wrong.
chk "every worker is alive at the end" "$((WORKERS+SHARDS+1+POOL))" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE alive")"
chk "the row cache is coherent at the end" "t" \
    "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache")"
chk "the registration survived the faults" "1" \
    "$(Q "SELECT registrations FROM supacache.pg_stat_keyspace_rowcache")"
chk "eviction ran, so the cache was genuinely under pressure" "t" \
    "$(Q "SELECT evictions > 0 FROM supacache.pg_stat_keyspace")"
chk "TTL partitions were cycled, not just accumulated" "t" \
    "$(Q "SELECT count(*) BETWEEN 1 AND 200 FROM pg_inherits i
          JOIN pg_class c ON c.oid=i.inhparent WHERE c.relname='kv_ttl'")"

echo
echo "########## 5. drift ##########"
# fault_lock_partition takes ACCESS EXCLUSIVE on a backing partition on purpose,
# so a persistence batch failing is a designed consequence rather than a
# surprise. Those records are retained and retried -- which is why `dropped`
# stays the assertion that nothing was LOST, and why the checksum sweep above is
# the one that would notice if that promise were broken. With faults off, zero.
# One ACCESS EXCLUSIVE lock fault stalls the persist worker for its whole hold,
# and every batch it attempts in that window fails and is retried -- so the
# allowance scales with how many times that fault fires, not a flat number that
# a slightly longer run would trip.
if [ -n "$NO_FAULTS" ]; then
  FAILED_ALLOWED=0
else
  # faults.sh rotates through 6 fault types, so the lock fault fires roughly
  # once every 6 intervals; 25 retried batches per hold is generous.
  FAILED_ALLOWED=${FAILED_ALLOWED:-$(( ((SECS / FAULT_EVERY) / 6 + 1) * 25 + 25 ))}
fi
MAX_PERSIST_FAILED=$FAILED_ALLOWED \
PGPORT=$PORT PGBIN=$PGBIN bash "$SCRIPT_DIR/soak/drift.sh" judge "$OUT/drift.csv"
DRIFT_RC=$?
chk "drift thresholds hold over the run" "0" "$DRIFT_RC"

echo
echo "=== $pass passed, $fail failed  (k6 thresholds rc=$K6_RC) ==="
echo "artifacts: $OUT/{drift.csv,k6.log,k6.json,pgbench.log,faults.log}"
stop_pg
[ "$fail" -eq 0 ] && [ "$K6_RC" -eq 0 ]
