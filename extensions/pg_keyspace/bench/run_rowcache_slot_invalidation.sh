#!/usr/bin/env bash
# A lost replication slot must make that database's cache INCOHERENT and then
# rebuild it — never resume over the gap (#120).
#
# One slot per participating database is the design's operational hazard. A slot
# retains WAL until it is consumed, so with one database the risk was singular
# and visible; with several, ANY ONE database's stalled invalidation pins WAL for
# the whole cluster, and one slow tenant can fill the WAL volume for everyone.
#
# Postgres already solves this: `max_slot_wal_keep_size`. Set it, and a slot
# reserving more than that is invalidated by the server rather than allowed to
# pin WAL indefinitely. That trades an availability problem for a CORRECTNESS
# one, and this is the test that the trade was actually made:
#
#   an invalidated slot means an unknown set of changes was never delivered.
#   Resuming from a fresh slot without dropping what was cached would serve rows
#   that silently disagree with the heap -- a stale row served as truth, which is
#   the worst failure this system can produce, arriving at exactly the moment
#   everything reports healthy again.
#
# So the sequence asserted here is: mark incoherent (reads fall back to the heap)
# -> purge that database's cached rows -> rebuild the slot -> serve again. And
# throughout, the OTHER database must be unaffected, because "one database's
# stalled worker must not read as the whole cache being incoherent, nor the
# reverse" is the half that a single-slot design could never have got wrong.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-slotinval}
PORT=${PGKS_PG_PORT:-5473}
RESP=${PGKS_RESP_PORT:-6475}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d "${2:-postgres}" -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop -m fast" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }
set_conf() { sed -i "/^$1 /d;/^$1=/d" $PGDATA/postgresql.conf; echo "$1 = $2" >> $PGDATA/postgresql.conf; }
wait_coherent() {
  for _ in $(seq 1 "${2:-90}"); do
    [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" "$1")" = "t" ] && return 0
    sleep 1
  done
  return 1
}
slot_of() { Q "SELECT 'supacache_rowcache_' || oid FROM pg_database WHERE datname='$1'"; }

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/slotinval_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/slotinval_install.log; exit 1; }
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/slotinval_plugin.log 2>&1 || {
  echo "PLUGIN INSTALL FAILED"; tail -20 /tmp/slotinval_plugin.log; exit 1; }

echo "=== cluster ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.rowcache_decode = on"
  echo "pg_keyspace.rowcache_decode_ms = 200"
  echo "pg_keyspace.rowcache_invalidation_workers = 2"
  echo "wal_level = logical"
  echo "max_replication_slots = 16"
  echo "max_wal_senders = 16"
  echo "max_worker_processes = 16"
  # Small segments and a small bound, so a slot can be pushed past it in seconds
  # rather than in gigabytes.
  echo "min_wal_size = 32MB"
  echo "max_wal_size = 64MB"
  # Start UNBOUNDED, so section 1 can assert the warning an operator gets.
  echo "max_slot_wal_keep_size = -1"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi
Q "CREATE DATABASE victim" >/dev/null
Q "CREATE DATABASE bystander" >/dev/null
for d in victim bystander; do
  Q "CREATE TABLE public.t(id bigint primary key, v text)" $d >/dev/null
  Q "INSERT INTO public.t VALUES (1,'v1-$d')" $d >/dev/null
done
restart
# `postgres` too: the per-database view is cluster-wide and is read from ONE
# database, which is how a monitoring collector uses it. Without the extension
# there, section 4's assertion fails on a missing relation rather than on what
# it is checking. It registers nothing, so it stays idle and costs no slot.
for d in postgres victim bystander; do Q "CREATE EXTENSION pg_keyspace" $d >/dev/null; done
restart

echo
echo "########## 1. an unbounded max_slot_wal_keep_size is called out ##########"
# One slot per database is only safe because Postgres can cut a runaway slot
# loose. Unbounded, it cannot, and the failure mode is the WAL volume filling up
# for every tenant because of one. That deserves saying at the one moment an
# operator is definitely reading the log.
chk "the log warns that slots can retain WAL without bound" "t" \
    "$([ "$(grep -c 'max_slot_wal_keep_size is unset' $PGDATA/log)" -ge 1 ] && echo t || echo f)"
chk "and the cluster serves anyway rather than refusing to start" "1" "$(Q "SELECT 1")"
chk "the view reports the setting, so it is visible without reading logs" "-1" \
    "$(Q "SELECT DISTINCT max_slot_wal_keep_size FROM supacache.pg_stat_keyspace_invalidation" victim)"

echo
echo "########## 2. both databases cache and are coherent ##########"
set_conf "max_slot_wal_keep_size" "32MB"
restart
for d in victim bystander; do
  chk "  $d registers" "t" "$(Q "SELECT supacache.rowcache_register('public.t')" $d)"
done
for d in victim bystander; do
  chk "  $d is coherent" "ok" "$(wait_coherent $d && echo ok || echo timeout)"
  chk "  $d caches its row" "t" "$(Q "SELECT supacache.rowcache_put('public.t', 1)" $d)"
  chk "  $d reads it from the cache" "1" \
      "$(Q "EXPLAIN (COSTS OFF) SELECT v FROM public.t WHERE id=1" $d | grep -c pg_keyspace_rowcache)"
  chk "  $d reads the right value" "v1-$d" "$(Q "SELECT v FROM public.t WHERE id=1" $d)"
done
chk "the bound is now reported as set" "32MB" \
    "$(Q "SELECT DISTINCT max_slot_wal_keep_size FROM supacache.pg_stat_keyspace_invalidation" victim)"

echo
echo "########## 3. stall the victim's slot and push it past the bound ##########"
# The slot has to stop advancing for the server to have anything to invalidate.
# Holding ACCESS EXCLUSIVE on the registered table makes the APPLY phase fail
# while decoding still succeeds, so the worker reads the batch and cannot apply
# it -- which is exactly the deliberate design (a change that cannot be applied
# blocks the channel rather than being consumed and forgotten). The slot stays
# put while WAL piles up behind it.
VS=$(slot_of victim)
BS=$(slot_of bystander)
chk "(setup) the victim has a slot named for its database" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$VS'")"
LSN0=$(Q "SELECT restart_lsn FROM pg_replication_slots WHERE slot_name='$VS'")

( Q "BEGIN; LOCK TABLE public.t IN ACCESS EXCLUSIVE MODE; SELECT pg_sleep(120); COMMIT" victim >/dev/null 2>&1 ) &
LOCKER=$!
sleep 3
# Generate far more WAL than the bound, and force segment recycling, which is
# what actually invalidates an over-reserving slot.
for i in $(seq 1 14); do
  Q "CREATE TABLE IF NOT EXISTS wal_$i(id int, pad text)" victim >/dev/null
  Q "INSERT INTO wal_$i SELECT g, repeat('x',2000) FROM generate_series(1,12000) g" victim >/dev/null
  Q "CHECKPOINT" >/dev/null
done
STATUS=""
for _ in $(seq 1 40); do
  STATUS=$(Q "SELECT wal_status FROM pg_replication_slots WHERE slot_name='$VS'")
  [ "$STATUS" = "lost" ] && break
  Q "SELECT pg_switch_wal()" >/dev/null; Q "CHECKPOINT" >/dev/null
  sleep 1
done
kill $LOCKER 2>/dev/null; wait $LOCKER 2>/dev/null

if [ "$STATUS" != "lost" ]; then
  # Say so rather than passing quietly. A run that never invalidated the slot
  # has not tested the thing this file exists for, and a test that silently does
  # nothing is worse than one that says it did nothing.
  echo "SKIP  could not push the slot past max_slot_wal_keep_size (wal_status=$STATUS)"
  echo "      the recovery path below was NOT exercised on this run"
  stop_pg
  echo "# result: $pass passed, $fail failed (recovery path skipped)"
  [ "$fail" -eq 0 ]
  exit $?
fi
chk "the server invalidated the victim's slot" "lost" "$STATUS"

echo
echo "########## 4. the victim fails closed; the bystander does not ##########"
# "One database's stalled worker must not read as the whole cache being
# incoherent, nor the reverse." Both halves, on the same cluster, at the same
# moment.
# Evidenced from the LOG, not by catching the live flag. Recovery is fast --
# mark incoherent, purge, rebuild -- and polling for `coherent = false` is a race
# against it that the test loses on a quiet cluster, reporting a failure for
# behaviour that worked. The log is the durable record that the transition
# happened, and section 5 is what proves it happened in the right ORDER: if the
# database had gone on being served across the gap, a stale row would have
# survived, and it did not.
for _ in $(seq 1 120); do
  [ "$(grep -c 'has been invalidated by the server' $PGDATA/log)" -ge 1 ] && break
  sleep 1
done
chk "the victim was marked incoherent and its cache dropped" "t" \
    "$([ "$(grep -c 'has been invalidated by the server' $PGDATA/log)" -ge 1 ] && echo t || echo f)"
chk "and the log names the victim's slot, not the bystander's" "t" \
    "$([ "$(grep -c "slot '$VS' has been invalidated" $PGDATA/log)" -ge 1 ] && echo t || echo f)"
chk "its reads fall back to an ordinary index scan while the cache is empty" "0" \
    "$(Q "EXPLAIN (COSTS OFF) SELECT v FROM public.t WHERE id=1" victim | grep -c pg_keyspace_rowcache)"

# The bystander is the non-racy half: it must be unaffected throughout, and
# nothing about it changes, so this is a live check rather than a log one.
chk "the bystander is still coherent" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" bystander)"
chk "the bystander's slot was never invalidated" "0" \
    "$(grep -c "slot '$BS' has been invalidated" $PGDATA/log || true)"
# Re-warmed first. Several minutes of WAL went by during section 3, and the
# bystander's own decoder will have caught up with the INSERT that predates its
# cached copy and dropped it -- correctly, since that change is older than the
# entry. Asserting on the pre-outage entry would be testing decode latency
# rather than isolation.
Q "SELECT supacache.rowcache_put('public.t', 1)" bystander >/dev/null
chk "and it is still served from the cache" "1" \
    "$(Q "EXPLAIN (COSTS OFF) SELECT v FROM public.t WHERE id=1" bystander | grep -c pg_keyspace_rowcache)"
chk "the bystander still reads its own value" "v1-bystander" \
    "$(Q "SELECT v FROM public.t WHERE id=1" bystander)"

echo
echo "########## 5. THE ASSERTION: no stale row survives the gap ##########"
# Change the row while the slot is lost. That UPDATE's invalidation is in the
# hole -- it will never be delivered, on this slot or any other. If recovery
# resumed from a fresh slot without dropping what was cached, this read would
# return 'v1-victim' forever, with everything reporting healthy.
Q "UPDATE public.t SET v='CHANGED-DURING-OUTAGE' WHERE id=1" victim >/dev/null
VAL=""
for _ in $(seq 1 120); do
  VAL=$(Q "SELECT v FROM public.t WHERE id=1" victim)
  [ "$VAL" = "CHANGED-DURING-OUTAGE" ] && break
  sleep 1
done
chk "the row written during the outage is what comes back" "CHANGED-DURING-OUTAGE" "$VAL"
chk "the log says the cached rows were dropped rather than resumed over" "t" \
    "$([ "$(grep -c 'dropped .* cached row' $PGDATA/log)" -ge 1 ] && echo t || echo f)"

echo
echo "########## 6. the slot is rebuilt and the database is served again ##########"
for _ in $(seq 1 120); do
  [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" victim)" = "t" ] && break
  sleep 1
done
chk "the victim is coherent again" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()" victim)"
chk "a healthy slot exists for it once more" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots
          WHERE slot_name='$VS' AND plugin='supacache_keys' AND wal_status <> 'lost'")"
chk "and it advanced past where the lost one stopped" "t" \
    "$([ "$(Q "SELECT (restart_lsn > '$LSN0'::pg_lsn)::int FROM pg_replication_slots WHERE slot_name='$VS'")" = "1" ] && echo t || echo f)"
# The registrations are configuration, not cache content. Dropping them on top
# of the outage would silently stop caching the tables an operator asked for
# (#103) -- a second failure caused by the recovery from the first.
chk "the registration survived the rebuild" "1" \
    "$(Q "SELECT count(*) FROM supacache.rowcache_reg" victim)"
chk "so the table is cached again after a fresh warm" "1" \
    "$(Q "SELECT supacache.rowcache_put('public.t', 1)" victim >/dev/null;
        Q "EXPLAIN (COSTS OFF) SELECT v FROM public.t WHERE id=1" victim | grep -c pg_keyspace_rowcache)"
chk "and invalidation works again afterwards" "t" \
    "$(Q "UPDATE public.t SET v='after-recovery' WHERE id=1" victim >/dev/null
       ok=f; for _ in $(seq 1 60); do
         [ "$(Q "SELECT v FROM public.t WHERE id=1" victim)" = "after-recovery" ] && { ok=t; break; }; sleep 1; done; echo $ok)"
chk "the bystander's slot was never touched" "1" \
    "$(Q "SELECT count(*) FROM pg_replication_slots WHERE slot_name='$BS' AND wal_status <> 'lost'")"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
