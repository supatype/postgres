#!/usr/bin/env bash
# Operational metrics as pg_stat_*-shaped views (#111).
#
# The gap this closes is not "no numbers" -- supacache.stats(), ring_stats() and
# the RESP INFO text all existed. It is that collecting them meant writing
# something. A Redis-style exporter is the usual answer, and it is a second
# artefact to version, deploy and watch.
#
# Postgres already has the ecosystem: postgres_exporter, pgwatch, Datadog and
# pganalyze all collect from pg_stat_*-shaped views, and all of them are already
# pointed at a role that is a member of pg_monitor. So the whole of the setup
# is CREATE EXTENSION.
#
# That claim has four parts, and this asserts each one against a live cluster
# rather than describing it:
#
#   1. a pg_monitor member reads every view with NO other grant, and a role
#      without pg_monitor is refused;
#   2. the view -- not the function -- is the privilege boundary, so revoking
#      EXECUTE from PUBLIC on the underlying functions leaves the collector
#      working and still unable to call them directly;
#   3. the counters are cumulative and reading does not reset them, which is
#      what makes a collector's rate() mean anything;
#   4. the views are extension members, so pg_dump and DROP EXTENSION handle
#      them -- a runtime CREATE VIEW would look identical until a restore.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-statviews}
PORT=${PGKS_PG_PORT:-5453}
RESP=${PGKS_RESP_PORT:-6431}
PROFILE=${PGKS_BUILD_PROFILE:-release}
WORKERS=${PGKS_WORKERS:-2}
SHARDS=${PGKS_PERSIST_WORKERS:-2}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
chk_ne() {
  if [ "$2" != "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        must not be: [$2]"; fail=$((fail+1)); fi
}
Q()  { $PGBIN/psql -h /tmp -p $PORT -U postgres -d "${2:-postgres}" -tAc "$1" 2>&1; }
QM() { $PGBIN/psql -h /tmp -p $PORT -U metrics  -d "${2:-postgres}" -tAc "$1" 2>&1; }
QP() { $PGBIN/psql -h /tmp -p $PORT -U plainuser -d "${2:-postgres}" -tAc "$1" 2>&1; }
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

VIEWS="pg_stat_keyspace pg_stat_keyspace_workers pg_stat_keyspace_activity \
pg_stat_keyspace_persist pg_stat_keyspace_persist_total pg_stat_keyspace_tenants \
pg_stat_keyspace_rowcache pg_stat_keyspace_rowcache_databases \
pg_stat_keyspace_invalidation pg_stat_keyspace_pubsub \
pg_stat_keyspace_topology"

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/statviews_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/statviews_install.log; exit 1; }
PLUGIN_OK=1
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/statviews_plugin.log 2>&1 || PLUGIN_OK=0
chk "the supacache_keys output plugin builds and installs" "1" "$PLUGIN_OK"
[ "$PLUGIN_OK" = "1" ] || { tail -20 /tmp/statviews_plugin.log; exit 1; }

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.workers = $WORKERS"
  echo "pg_keyspace.persist_workers = $SHARDS"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.cluster_announce_host = '127.0.0.1'"
  echo "pg_keyspace.rowcache_decode = on"
  echo "pg_keyspace.rowcache_decode_ms = 500"
  echo "wal_level = logical"
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi
Q "CREATE EXTENSION pg_keyspace" >/dev/null
restart
# The invalidation POOL is sized by its own GUC (#120), so the expected worker
# count is RESP + persist + expiry + pool rather than a fixed +2.
POOL=$(Q "SHOW pg_keyspace.rowcache_invalidation_workers")
POOL=${POOL:-1}
# A persisted worker serves nothing until startup recovery finishes, and the
# decoder has to claim its slot; wait for both rather than sleeping and hoping.
for _ in $(seq 1 60); do
  [ "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE alive")" = "$((WORKERS+SHARDS+1+POOL))" ] && break
  sleep 1
done

echo
echo "########## 0. preconditions ##########"
# Everything below is read through these, so a broken cluster must fail here and
# not as a subtle wrong number three sections down.
chk "the extension is installed" "1" "$(Q "SELECT count(*) FROM pg_extension WHERE extname='pg_keyspace'")"
chk "$WORKERS slot workers are running" "$WORKERS" "$(Q "SELECT count(DISTINCT worker) FROM supacache.worker_stats()")"
chk "the RESP port answers" "PONG" "$(redis-cli -p $RESP PING 2>&1)"

echo
echo "########## 1. every view exists, and is a view ##########"
for v in $VIEWS; do
  chk "supacache.$v is a view" "v" \
      "$(Q "SELECT relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='supacache' AND c.relname='$v'")"
done

echo
echo "########## 2. the views are EXTENSION MEMBERS ##########"
# The alternative -- creating them from the background worker at startup -- looks
# identical in psql and is broken where it matters: an unowned view is not in
# pg_dump's extension output, so a restore silently loses every one of them, and
# DROP EXTENSION leaves them behind pointing at functions that no longer exist.
MEMBERS=$(Q "SELECT count(*) FROM pg_depend d
             JOIN pg_extension e ON e.oid = d.refobjid AND e.extname='pg_keyspace'
             JOIN pg_class c ON c.oid = d.objid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE d.deptype='e' AND n.nspname='supacache' AND c.relkind='v'
               AND c.relname LIKE 'pg\_stat\_keyspace%'")
chk "all 11 views are owned by the extension" "11" "$MEMBERS"

echo
echo "########## 3. a pg_monitor member scrapes with NO other grant ##########"
Q "CREATE ROLE metrics LOGIN" >/dev/null
Q "GRANT pg_monitor TO metrics" >/dev/null
Q "CREATE ROLE plainuser LOGIN" >/dev/null
MON_OK=0; MON_BAD=""
for v in $VIEWS; do
  out=$(QM "SELECT count(*) >= 0 FROM supacache.$v")
  if [ "$out" = "t" ]; then MON_OK=$((MON_OK+1)); else MON_BAD="$MON_BAD $v:[$out]"; fi
done
chk "pg_monitor reads all 11 views${MON_BAD:+ -- failed:$MON_BAD}" "11" "$MON_OK"

# The negative control. If this passed, the grants above would be proving
# nothing: everything would be world-readable and "monitoring role" would be a
# label rather than a boundary.
# NB: capture, then match. `psql | grep -q` under `set -o pipefail` returns
# psql's non-zero exit even when grep matched, so the first version of this
# counted zero denials and reported the negative control as broken -- which is
# the only reason it was noticed.
DENIED=0; ALLOWED=""
for v in $VIEWS; do
  out=$(QP "SELECT count(*) FROM supacache.$v")
  case "$out" in *"permission denied"*) DENIED=$((DENIED+1));; *) ALLOWED="$ALLOWED $v";; esac
done
chk "a role WITHOUT pg_monitor is refused all 11 views${ALLOWED:+ -- allowed:$ALLOWED}" "11" "$DENIED"

echo
echo "########## 4. hardening the install does not break the collector ##########"
# Several functions in this schema write (set, rowcache_put, pg_recover), so an
# operator revoking PUBLIC EXECUTE is doing a reasonable thing. The grants to
# pg_monitor are explicit and survive it.
#
# This section is here because the first version of the design assumed a view
# executes its body as its OWNER and granted SELECT on the views alone. That is
# true of the view's TABLE references and NOT of a set-returning function in its
# FROM clause: the executor checks EXECUTE against GetUserId(), the caller. The
# collector got "permission denied for function worker_stats" on eight of ten
# views. Asserted here so the assumption cannot come back.
Q "REVOKE EXECUTE ON FUNCTION supacache.worker_stats() FROM PUBLIC" >/dev/null
Q "REVOKE EXECUTE ON FUNCTION supacache.persist_shard_stats() FROM PUBLIC" >/dev/null
Q "REVOKE EXECUTE ON FUNCTION supacache.tenant_stats() FROM PUBLIC" >/dev/null
chk "the collector still reads the view after PUBLIC loses EXECUTE" "t" \
    "$(QM "SELECT count(*) > 0 FROM supacache.pg_stat_keyspace_workers")"
# ...and the revoke really took, or the line above proves nothing.
chk "a role without pg_monitor cannot call the function directly" "denied" \
    "$(case "$(QP "SELECT count(*) FROM supacache.worker_stats()")" in
         *"permission denied"*) echo denied;; *) echo allowed;; esac)"
Q "GRANT EXECUTE ON FUNCTION supacache.worker_stats() TO PUBLIC" >/dev/null
Q "GRANT EXECUTE ON FUNCTION supacache.persist_shard_stats() TO PUBLIC" >/dev/null
Q "GRANT EXECUTE ON FUNCTION supacache.tenant_stats() TO PUBLIC" >/dev/null

echo
echo "########## 5. counters are cumulative, and reading does not reset them ##########"
redis-cli -p $RESP FLUSHALL >/dev/null 2>&1
H0=$(Q "SELECT hits FROM supacache.pg_stat_keyspace")
M0=$(Q "SELECT misses FROM supacache.pg_stat_keyspace")
S0=$(Q "SELECT sets FROM supacache.pg_stat_keyspace")
for i in $(seq 1 200); do redis-cli -c -p $RESP SET "ctr:$i" "v$i" >/dev/null 2>&1; done
for i in $(seq 1 200); do redis-cli -c -p $RESP GET "ctr:$i" >/dev/null 2>&1; done
for i in $(seq 1 50);  do redis-cli -c -p $RESP GET "absent:$i" >/dev/null 2>&1; done
H1=$(Q "SELECT hits FROM supacache.pg_stat_keyspace")
M1=$(Q "SELECT misses FROM supacache.pg_stat_keyspace")
S1=$(Q "SELECT sets FROM supacache.pg_stat_keyspace")
chk "200 SETs moved the sets counter by at least 200" "t" "$([ $((S1-S0)) -ge 200 ] && echo t || echo f)"
chk "200 GETs of present keys moved hits by at least 200" "t" "$([ $((H1-H0)) -ge 200 ] && echo t || echo f)"
chk "50 GETs of absent keys moved misses by at least 50" "t" "$([ $((M1-M0)) -ge 50 ] && echo t || echo f)"
# Cumulative means a second read with no traffic in between returns the same
# numbers. A surface that reset on read would give a collector a rate of
# "everything since you last looked", which is the same shape and wrong.
H2=$(Q "SELECT hits FROM supacache.pg_stat_keyspace")
S2=$(Q "SELECT sets FROM supacache.pg_stat_keyspace")
chk "reading twice does not reset hits" "$H1" "$H2"
chk "reading twice does not reset sets" "$S1" "$S2"

echo
echo "########## 6. the rollup agrees with the per-worker rows ##########"
# The rollup is a sum over the same function, so a mismatch means the view is
# summing something other than what it reports per row.
chk "rollup entries = sum of worker rows" \
    "$(Q "SELECT sum(entries) FROM supacache.pg_stat_keyspace_workers")" \
    "$(Q "SELECT entries FROM supacache.pg_stat_keyspace")"
chk "rollup hits = sum of worker rows" \
    "$(Q "SELECT sum(hits) FROM supacache.pg_stat_keyspace_workers")" \
    "$(Q "SELECT hits FROM supacache.pg_stat_keyspace")"
# stats() fuses (worker, partition) into one index; worker_stats() splits it.
# Both must be the same data, or one of them is lying.
chk "worker_stats() and stats() report the same row count" \
    "$(Q "SELECT count(*) FROM supacache.stats()")" \
    "$(Q "SELECT count(*) FROM supacache.worker_stats()")"
chk "worker_stats() and stats() report the same total entries" \
    "$(Q "SELECT sum(entries) FROM supacache.stats()")" \
    "$(Q "SELECT sum(entries) FROM supacache.worker_stats()")"
# The join key is the point of the new column: a collector cannot undo the fused
# index without knowing the partition count.
chk "each worker row carries the slot range it serves" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_workers WHERE slot_lo IS NULL OR slot_hi IS NULL")"
chk "the worker column really distinguishes workers" "$WORKERS" \
    "$(Q "SELECT count(DISTINCT worker) FROM supacache.pg_stat_keyspace_workers")"

echo
echo "########## 7. persistence is reported PER RING, not only summed ##########"
chk "one row per (worker, shard)" "$((WORKERS*SHARDS))" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_persist")"
chk "per-ring pushed sums to ring_stats()" \
    "$(Q "SELECT pushed FROM supacache.ring_stats()")" \
    "$(Q "SELECT sum(pushed) FROM supacache.pg_stat_keyspace_persist")"
chk "per-ring committed sums to ring_stats()" \
    "$(Q "SELECT committed FROM supacache.ring_stats()")" \
    "$(Q "SELECT sum(committed) FROM supacache.pg_stat_keyspace_persist")"
# The durable tier ran 200 SETs above, so persistence must have seen traffic --
# otherwise every equality above is 0 = 0 and proves nothing.
chk "persistence actually saw the durable writes" "t" \
    "$(Q "SELECT sum(pushed) > 0 FROM supacache.pg_stat_keyspace_persist")"
chk "the total view exposes the deepest single ring" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_persist_total WHERE worst_ring_backlog_bytes IS NULL")"
# uncommitted_batches is a GAUGE of batches in flight, not a failure count: the
# ring increments before attempting and decrements after committing. Asserting
# it is >= 0 and present is all that is meaningful; asserting it is 0 would fail
# on any run that happens to sample mid-batch, which is most of them.
chk "uncommitted_batches is reported and non-negative" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_persist
          WHERE uncommitted_batches IS NULL OR uncommitted_batches < 0")"

echo
echo "########## 8. per-tenant arena occupancy ##########"
for i in $(seq 1 300); do redis-cli -c -p $RESP SET "acme:k$i" "0123456789abcdef" >/dev/null 2>&1; done
for i in $(seq 1 40);  do redis-cli -c -p $RESP SET "tiny:k$i" "x" >/dev/null 2>&1; done
chk "the big tenant appears" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_tenants WHERE tenant='acme'")"
chk "the small tenant appears too" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_tenants WHERE tenant='tiny'")"
chk "the tenant name has no trailing colon" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_tenants WHERE tenant LIKE '%:'")"
chk "acme holds more entries than tiny" "t" \
    "$(Q "SELECT (SELECT entries FROM supacache.pg_stat_keyspace_tenants WHERE tenant='acme')
              > (SELECT entries FROM supacache.pg_stat_keyspace_tenants WHERE tenant='tiny')")"
chk "acme's entry count matches what was written" "300" \
    "$(Q "SELECT entries FROM supacache.pg_stat_keyspace_tenants WHERE tenant='acme'")"

echo
echo "########## 9. every background worker is visible and beating ##########"
chk "one activity row per tracked worker" "$((WORKERS+SHARDS+1+POOL))" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity")"
chk "$WORKERS resp rows" "$WORKERS" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE role='resp'")"
chk "$SHARDS persist rows" "$SHARDS" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE role='persist'")"
chk "one expiry row" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE role='expiry'")"
# One per POOL MEMBER (#120), which is what pg_keyspace.rowcache_invalidation_
# workers sets. Not one per database: a pool worker's identity is its slot in
# the pool, and which database it is draining changes over its life.
chk "$POOL invalidation row(s), one per pool member" "$POOL" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE role='invalidation'")"
chk "each pool member is numbered" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity
          WHERE role='invalidation' AND worker IS NULL")"
chk "every worker is alive" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE NOT alive")"
chk "every live worker reports a pid" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity WHERE alive AND pid IS NULL")"
# A pid that is not a running process would make this view decorative.
LIVE_PIDS=$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_activity a
                WHERE a.alive AND EXISTS (SELECT 1 FROM pg_stat_activity p WHERE p.pid = a.pid)")
chk "every reported pid is a real backend" "$((WORKERS+SHARDS+1+POOL))" "$LIVE_PIDS"

echo
echo "########## 10. WAL decode lag ##########"
# A database gets a decode slot only once it has registrations (#120): they are
# picked up LAZILY, so one that caches nothing costs no slot, no worker and no
# WAL. Section 11 registers a table and checks occupancy; the slot has to exist
# before that to be reported here, so register the first one now.
Q "CREATE TABLE IF NOT EXISTS slotmaker(id bigint primary key, v text)" >/dev/null
chk "(setup) a registration exists, so this database has a slot at all" "t" \
    "$(Q "SELECT supacache.rowcache_register('public.slotmaker')")"
for _ in $(seq 1 90); do
  [ "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation")" = "1" ] && break
  sleep 1
done
chk "the invalidation view has a row while decoding is on" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation")"
chk "it names the slot the decoder uses" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation i
          JOIN pg_replication_slots s ON s.slot_name = i.slot_name")"
# The slot is named for the DATABASE, not for the configuration (#120):
# `pg_keyspace.rowcache_slot` is the stem and the database oid is the suffix.
# A logical slot only ever decodes changes from the database it belongs to, so
# a view that resolved the configured name verbatim would name a slot that does
# not exist -- and would go empty, which is the signal for "decoding is off".
chk "the slot it names belongs to THIS database" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation i
          JOIN pg_replication_slots s ON s.slot_name = i.slot_name
          WHERE s.database = current_database()")"
chk "and carries this database's oid in its name" "1" \
    "$(Q "SELECT (slot_name = current_setting('pg_keyspace.rowcache_slot')
                               || '_' || (SELECT oid FROM pg_database
                                           WHERE datname = current_database()))::int
          FROM supacache.pg_stat_keyspace_invalidation")"
chk "decode lag is reported, not null" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation WHERE decode_lag_bytes IS NULL")"
chk "retained WAL is reported, not null" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation WHERE retained_bytes IS NULL")"
# Keyed by database (#120). There is one slot per participating database, so a
# view that returned a single row would show one arbitrary database's decode lag
# and an operator watching it would believe it was the cluster's.
chk "every row names the database its slot belongs to" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation WHERE datname IS NULL")"
chk "and the datid matches that database" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation i
          WHERE i.datid <> (SELECT oid FROM pg_database d WHERE d.datname = i.datname)")"
chk "wal_status is reported, so a lost slot is visible" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation WHERE wal_status IS NULL")"
# The row cache holds one slot per database and a slot retains WAL until it is
# consumed, so whether that is bounded is load-bearing operational state, not
# trivia. Surfacing it here is what lets it be alerted on rather than remembered.
chk "the WAL retention bound is reported" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation
          WHERE max_slot_wal_keep_size IS NULL")"
# Generating WAL the decoder has not consumed yet must show up as lag. Without
# this, a view returning a constant zero would pass everything above.
Q "CREATE TABLE lagmaker(id int primary key, pad text)" >/dev/null
Q "INSERT INTO lagmaker SELECT g, repeat('x', 2000) FROM generate_series(1,20000) g" >/dev/null
LAG=$(Q "SELECT decode_lag_bytes FROM supacache.pg_stat_keyspace_invalidation")
chk "bulk WAL shows up as decode lag (got ${LAG}B)" "t" \
    "$([ "${LAG:-0}" -gt 0 ] && echo t || echo f)"

echo
echo "########## 11. row cache occupancy, coherence and registrations ##########"
Q "CREATE TABLE things(id bigint primary key, v text)" >/dev/null
Q "INSERT INTO things VALUES (1,'one'),(2,'two')" >/dev/null
# One-arg form: the whole primary key. Asserted rather than sent to /dev/null --
# the first version passed 'id' as the attnum, which ERRORed, and the section
# then reported "0 registrations" as though the view were wrong.
chk "the table registers" "t" "$(Q "SELECT supacache.rowcache_register('public.things')")"
# TWO: `slotmaker` from section 10, which had to register something to give this
# database a slot at all (databases are picked up lazily since #120), and
# `things` just now. Counted against the catalogue rather than hardcoded, so
# this does not have to be re-edited every time an earlier section registers
# something -- and so it still fails if the view and the catalogue disagree,
# which is the thing being tested.
chk "the registration is counted" \
    "$(Q "SELECT count(*) FROM supacache.rowcache_reg")" \
    "$(Q "SELECT registrations FROM supacache.pg_stat_keyspace_rowcache")"
chk "and there are the two this run registered" "2" \
    "$(Q "SELECT count(*) FROM supacache.rowcache_reg")"
chk "registrations are resident in the segment" "t" \
    "$(Q "SELECT registrations_loaded FROM supacache.pg_stat_keyspace_rowcache")"
for _ in $(seq 1 40); do [ "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache")" = "t" ] && break; sleep 1; done
chk "the row cache reports itself coherent" "t" \
    "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache")"
chk "decode is reported as enabled" "t" \
    "$(Q "SELECT decode_enabled FROM supacache.pg_stat_keyspace_rowcache")"
Q "SELECT supacache.rowcache_put('things', 1::bigint)" >/dev/null
chk "a cached row shows up in the row-cache entry count" "t" \
    "$(Q "SELECT entries > 0 FROM supacache.pg_stat_keyspace_rowcache")"

echo
echo "########## 11b. coherence is per-database ##########"
# The cache FAILS CLOSED on incoherence, so which database is incoherent decides
# which queries are served from the cache at all -- load-bearing for
# correctness, not only for dashboards.
chk "the rowcache view names the database it describes" "postgres" \
    "$(Q "SELECT datname FROM supacache.pg_stat_keyspace_rowcache")"
chk "the per-database view has a row for it" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases
          WHERE datname = current_database()")"
chk "it reads as participating, having a registration" "participating" \
    "$(Q "SELECT state FROM supacache.pg_stat_keyspace_rowcache_databases
          WHERE datname = current_database()")"
chk "and as coherent" "t" \
    "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache_databases
          WHERE datname = current_database()")"
chk "with no lost slot" "f" \
    "$(Q "SELECT slot_lost FROM supacache.pg_stat_keyspace_rowcache_databases
          WHERE datname = current_database()")"
# The window each database is judged against is reported rather than left to be
# derived, because it GROWS with the number of participating databases once they
# outnumber the pool -- which is the headline trade this design makes.
chk "the staleness window is reported and positive" "t" \
    "$([ "$(Q "SELECT stale_after_ms FROM supacache.pg_stat_keyspace_rowcache_databases
               WHERE datname = current_database()")" -gt 0 ] && echo t || echo f)"
chk "the slot it names is the one that exists" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases d
          JOIN pg_replication_slots s ON s.slot_name = d.slot_name
          WHERE d.datname = current_database()")"
chk "no participating database is incoherent" "0" \
    "$(Q "SELECT incoherent_databases FROM supacache.pg_stat_keyspace_rowcache")"

echo
echo "########## 12. topology ##########"
chk "running_workers matches the GUC" "$WORKERS" \
    "$(Q "SELECT running_workers FROM supacache.pg_stat_keyspace_topology")"
chk "nothing has moved on an unchanged layout" "0" \
    "$(Q "SELECT slots_moved FROM supacache.pg_stat_keyspace_topology")"

echo
echo "########## 13. an OFF feature reports NO ROWS, not a row of zeroes ##########"
# This is the distinction an alert depends on. "decode_lag_bytes = 0" means the
# decoder is current; no row means there is no decoder. Collapsing the two makes
# "invalidation is switched off in production" indistinguishable from "perfectly
# healthy", which is the failure mode worth spending a restart to rule out.
sed -i "s/^pg_keyspace.rowcache_decode = on/pg_keyspace.rowcache_decode = off/" $PGDATA/postgresql.conf
restart
chk "decoding off => the invalidation view is empty" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_invalidation")"
chk "...and the row cache view says so rather than vanishing" "f" \
    "$(Q "SELECT decode_enabled FROM supacache.pg_stat_keyspace_rowcache")"
# The per-database view follows the same rule, and for the same reason: with no
# decoder there is no pool, so no database is being invalidated and none is
# listed. An empty list is "nothing is being kept coherent", which is what an
# alert needs to be able to see.
chk "decoding off => no database is listed as served" "0" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache_databases")"
# But the cache is still SERVED, because with decoding off coherence is the
# operator's own business -- they warm it by hand and keep it current by hand.
# Failing closed here would break a deliberate configuration rather than protect
# anyone.
chk "...and the cache is still trusted, because coherence is manual then" "t" \
    "$(Q "SELECT coherent FROM supacache.pg_stat_keyspace_rowcache")"
sed -i "s/^pg_keyspace.rowcache_decode = off/pg_keyspace.rowcache_decode = on/" $PGDATA/postgresql.conf
restart

echo
echo "########## 14. a scrape before the worker's tables exist must not ERROR ##########"
# supacache.topology and supacache.rowcache_reg are created by the background
# worker, not by CREATE EXTENSION, so there is a window at startup -- and right
# here, after a DROP/CREATE with no restart -- where the views reference tables
# that do not exist. Selecting from them directly makes the whole view ERROR,
# and a collector reads an erroring scrape as the database being down. Both
# functions guard with to_regclass.
Q "DROP EXTENSION pg_keyspace CASCADE" >/dev/null
Q "CREATE EXTENSION pg_keyspace" >/dev/null
chk "the worker's tables really are absent (or this proves nothing)" "0" \
    "$(Q "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='supacache' AND c.relname IN ('topology','rowcache_reg')")"
chk "the rowcache view returns a row instead of erroring" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_rowcache")"
chk "...reporting zero registrations" "0" \
    "$(Q "SELECT registrations FROM supacache.pg_stat_keyspace_rowcache")"
chk "the topology view returns a row instead of erroring" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_topology")"
chk "...with recorded_workers NULL rather than a made-up number" "1" \
    "$(Q "SELECT count(*) FROM supacache.pg_stat_keyspace_topology WHERE recorded_workers IS NULL")"
restart

echo
echo "########## 15. the views survive a restart and drop with the extension ##########"
chk "all 11 views are still present after a restart" "11" \
    "$(Q "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='supacache' AND c.relkind='v' AND c.relname LIKE 'pg\_stat\_keyspace%'")"
# pg_dump reproduces an extension as CREATE EXTENSION, so what is dumped is
# exactly what is a member. Check the dump rather than inferring from pg_depend
# twice.
DUMPED=$($PGBIN/pg_dump -h /tmp -p $PORT -U postgres -d postgres 2>/dev/null | grep -c "CREATE VIEW supacache.pg_stat_keyspace")
chk "pg_dump emits no view DDL (they come back with the extension)" "0" "$DUMPED"
Q "DROP EXTENSION pg_keyspace CASCADE" >/dev/null
chk "DROP EXTENSION takes every view with it" "0" \
    "$(Q "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='supacache' AND c.relkind='v' AND c.relname LIKE 'pg\_stat\_keyspace%'")"

echo
echo "=== $pass passed, $fail failed ==="
stop_pg
[ "$fail" -eq 0 ]
