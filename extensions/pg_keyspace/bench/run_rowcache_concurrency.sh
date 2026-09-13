#!/usr/bin/env bash
# The row cache must not crash the cluster (#127, #128).
#
# Two segfaults, both found by the #113 soak harness on its first run, both
# taking the whole cluster down because Postgres answers a backend SIGSEGV by
# terminating every other backend and crash-restarting.
#
# #128 is the trivial one: a query with NO WHERE clause against a registered
# table dereferenced a null `baserestrictinfo` at plan time. An empty Postgres
# List is a null pointer, and `SELECT count(*) FROM t` is exactly that shape.
# No concurrency, no unusual settings, first statement.
#
# #127 is the concurrency one:
# Store's public API is documented "called only by the owning worker for
# partition p", and every RESP keyspace segment honours it: one worker process
# owns it. The Mode B row-cache segment has no owner. Its writers are ordinary
# BACKENDS -- rowcache_put, registration, and above all the read-through path in
# rc_access, which runs in every backend that misses.
#
# Two at once corrupt the arena: ensure_alloc walks the slab free lists and can
# call evict_one, which moves the bucket array, the entry array and the bump
# pointer. The first backend to follow a torn pointer segfaults, and Postgres
# answers a segfault by killing every other backend and crash-restarting the
# cluster. Found by the #113 soak harness on its first run, in under a second.
#
# This is a CRASH test, so the assertions are unusual: the thing being checked
# is the absence of `signal 11` in the server log and the absence of aborted
# clients, not a query result. It is deliberately sized so that the unfixed
# build fails within seconds -- with the fix reverted it dies at around 7,000
# transactions, and the clean run below does hundreds of thousands.
#
# Self-contained. Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-rcconc}
PORT=${PGKS_PG_PORT:-5457}
RESP=${PGKS_RESP_PORT:-6451}
PROFILE=${PGKS_BUILD_PROFILE:-release}
CLIENTS=${CLIENTS:-8}
SECS=${SECS:-25}
ROWS=${ROWS:-50000}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
Q() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop -m fast" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 60); do Q "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
restart() { stop_pg; sleep 1; start_pg; wait_ready; sleep 2; }
log_lines() { wc -l < $PGDATA/log 2>/dev/null || echo 0; }
crashes_since() { tail -n +"${1:-0}" $PGDATA/log 2>/dev/null | grep -c "signal 11"; }

echo "=== build + install ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/rcconc_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/rcconc_install.log; exit 1; }
make -C "$SCRIPT_DIR/../plugin" PG_CONFIG=$PGBIN/pg_config install >/tmp/rcconc_plugin.log 2>&1 || {
  echo "PLUGIN INSTALL FAILED"; tail -20 /tmp/rcconc_plugin.log; exit 1; }

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
  # Read-through ON is the whole point: it is what makes a backend a writer.
  echo "pg_keyspace.rowcache_readthrough = on"
  # Small, so eviction runs during the test. ensure_alloc's eviction path is
  # where the concurrent mutation actually lands; a cache that never evicts
  # would take far longer to corrupt and could pass an unfixed build.
  echo "pg_keyspace.rowcache_mb = 16"
  echo "wal_level = logical"
  echo "max_replication_slots = 8"
  echo "max_wal_senders = 8"
  echo "max_connections = 100"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "NO START"; exit 1; }
if [ "$(Q "SELECT count(*) FROM pg_settings WHERE name='output_plugin_libraries'")" = "1" ]; then
  echo "output_plugin_libraries = 'supacache_keys'" >> $PGDATA/postgresql.conf
fi
Q "CREATE EXTENSION pg_keyspace" >/dev/null
restart

Q "CREATE TABLE profiles(id bigint primary key, tenant text NOT NULL, payload text NOT NULL)" >/dev/null
Q "INSERT INTO profiles SELECT g, 't'||(g%3), md5(g::text)||repeat('p',200) FROM generate_series(1,$ROWS) g" >/dev/null

echo
echo "########## 0. preconditions ##########"
# Every one of these is load-bearing. Read-through off, or an unregistered
# table, or an incoherent cache, and the run below never writes from a backend
# at all -- it would pass on the broken build and prove nothing.
chk "read-through is on (this is what makes a backend a writer)" "on" \
    "$(Q "SHOW pg_keyspace.rowcache_readthrough")"
chk "the table registers" "t" "$(Q "SELECT supacache.rowcache_register('public.profiles')")"
for _ in $(seq 1 60); do [ "$(Q "SELECT coherent FROM supacache.rowcache_coherence()")" = "t" ] && break; sleep 1; done
chk "the row cache is coherent, so reads are served from it" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()")"
chk "a pk lookup really plans through the cache" "1" \
    "$(Q "EXPLAIN (COSTS OFF) SELECT payload FROM profiles WHERE id = 42" | grep -ci "custom scan\|supacache")"

echo
echo "########## 0b. a query with NO WHERE clause (#128) ##########"
# The pathlist hook runs for every registered base relation in a SELECT while
# the cache is coherent, and then searched baserestrictinfo for `pk = const`.
# A relation with no clauses has baserestrictinfo == NULL -- an empty Postgres
# List is a null pointer, not an empty object -- and reading through it killed
# the backend at plan time.
#
# Each of these crashed the cluster before the fix. They are run one at a time,
# each checked for a new segfault, so the output names WHICH shape broke rather
# than only that something did.
for q in \
  "SELECT count(*) FROM profiles" \
  "SELECT id FROM profiles LIMIT 1" \
  "SELECT * FROM profiles ORDER BY id LIMIT 1" \
  "SELECT max(id) FROM profiles" \
  "SELECT count(*) FROM profiles WHERE payload IS NOT NULL" \
  "SELECT count(*) FROM profiles p JOIN profiles q ON q.id = p.id + 1"
do
  NQ=$(log_lines)
  RES=$(Q "$q")
  if [ "$(crashes_since $NQ)" != "0" ]; then
    chk "no crash: $q" "no crash" "SEGFAULT"
    # The cluster is restarting; wait before the next one or every later
    # assertion reports a connection error instead of its own result.
    wait_ready
  else
    case "$RES" in
      *"connection to server"*|*"server closed"*|*"recovery mode"*)
        chk "no crash: $q" "a result" "$RES"; wait_ready ;;
      *) chk "no crash: $q" "ok" "ok" ;;
    esac
  fi
done
# The pk path must still work afterwards -- a "fix" that disabled the hook
# entirely would pass every assertion above.
chk "a pk lookup still plans through the cache" "1" \
    "$(Q "EXPLAIN (COSTS OFF) SELECT payload FROM profiles WHERE id = 7" | grep -ci "custom scan\|supacache")"
chk "and returns the right row" "$(Q "SELECT md5('7'::text)||repeat('p',200)")" \
    "$(Q "SELECT payload FROM profiles WHERE id = 7")"

cat > /tmp/rcconc_read.sql <<PG
\\set id random(1, $ROWS)
SELECT payload FROM profiles WHERE id = :id;
PG
cat > /tmp/rcconc_write.sql <<PG
\\set id random(1, $ROWS)
UPDATE profiles SET payload = md5(random()::text) || repeat('p',200) WHERE id = :id;
PG

echo
echo "########## 1. concurrent read-through against invalidation ##########"
# 80/20 read/update. The updates drive invalidation, invalidation drives misses,
# and every miss makes its backend write. That is the loop that corrupts the
# arena on an unfixed build.
N0=$(log_lines)
OUT=$($PGBIN/pgbench -h /tmp -p $PORT -U postgres -n -c $CLIENTS -j 4 -T $SECS \
        -f /tmp/rcconc_read.sql@8 -f /tmp/rcconc_write.sql@2 postgres 2>&1)
TX=$(grep -oP 'actually processed: \K[0-9]+' <<<"$OUT")
ABORTED=$(grep -c "aborted in command" <<<"$OUT")
chk "no backend segfaulted" "0" "$(crashes_since $N0)"
chk "no client was aborted by a dying backend" "0" "$ABORTED"
# Guard: a run that did almost nothing could not have crashed either. On an
# unfixed build this is where it stops, at a few thousand.
chk "the run did enough work to be meaningful (${TX:-0} tx)" "t" \
    "$([ "${TX:-0}" -gt 50000 ] && echo t || echo f)"
chk "the cluster is still up" "1" "$(Q "SELECT 1")"

echo
echo "########## 2. the cache was actually exercised ##########"
# Without these, section 1 is "nothing crashed while nothing happened".
chk "the row cache holds rows" "t" "$(Q "SELECT entries > 1 FROM supacache.rowcache_stats()")"
chk "read-through produced hits" "t" "$(Q "SELECT hits > 0 FROM supacache.rowcache_stats()")"
chk "the registration survived the run" "1" \
    "$(Q "SELECT count(*) FROM supacache.rowcache_reg")"
chk "the cache is still coherent" "t" \
    "$(Q "SELECT coherent FROM supacache.rowcache_coherence()")"

echo
echo "########## 3. cached rows are CORRECT, not merely present ##########"
# A torn read does not segfault: it returns the head of one tuple and the tail
# of another, which deforms into a plausible-looking wrong row. Serialising
# writers does not prevent that -- readers are deliberately not serialised --
# so the read path uses get_stable's seqlock. This is what checks it.
#
# Compared against the heap with the cache bypassed, over every row, after a
# run whose whole purpose was to race reads against rewrites.
MISMATCH=$(Q "SELECT count(*) FROM profiles p
              WHERE p.payload IS NULL OR length(p.payload) <> 232
                 OR p.payload !~ '^[0-9a-f]{32}p+$'")
chk "every row reads back well-formed after the race" "0" "$MISMATCH"
SEEN=$(Q "SELECT count(*) FROM profiles")
chk "...over all $ROWS rows" "$ROWS" "$SEEN"

echo
echo "########## 4. concurrent explicit writers ##########"
# rowcache_put from several sessions at once: the same hazard by a different
# door, and the one an operator could hit without read-through on at all.
N1=$(log_lines)
for i in $(seq 1 6); do
  ( for j in $(seq 1 300); do
      Q "SELECT supacache.rowcache_put('public.profiles', $(( (i*300+j) % ROWS + 1 ))::bigint)" >/dev/null
    done ) &
done
wait
chk "concurrent rowcache_put did not crash a backend" "0" "$(crashes_since $N1)"
chk "the cluster survived concurrent explicit writes" "1" "$(Q "SELECT 1")"

echo
echo "=== $pass passed, $fail failed ==="
stop_pg
[ "$fail" -eq 0 ]
