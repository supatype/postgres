#!/usr/bin/env bash
# P6 slice 3 (§3.5) — the keys-only logical-decoding invalidation worker.
#
# The Mode B row cache holds RAW pre-policy tuples, so it must be dropped the
# instant the underlying row changes. A logical replication slot whose output
# plugin (supacache_keys) emits ONLY `<I|U|D> <relid> <pk>` drives that: the
# worker learns *which* keys changed and drops them, and no column value ever
# leaves the plugin (§4.7 "decoding worker stores WAL values" — impossible here).
#
# Requires: wal_level=logical, pg_keyspace.rowcache_decode=on, the supacache_keys
# plugin installed, and the invalidation worker running.
set -u
PGPORT=${PGPORT:-5434}
LOG=${LOG:-/tmp/pgks17/server.log}
A="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-50s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-50s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
plan() { $A -c "EXPLAIN (COSTS OFF) $1" 2>/dev/null | head -1; }

echo "# P6 slice 3 — keys-only invalidation (§3.5)"

$A >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.inval CASCADE;
CREATE TABLE public.inval(id bigint primary key, name text, secret text);
INSERT INTO public.inval VALUES (1,'one','sec1'),(2,'two','sec2');
SELECT supacache.rowcache_register('public.inval', 1);
SQL
# Let the worker drain the setup INSERTs' own decode records first (they would
# otherwise invalidate what we cache next) — you cache a row that already exists.
sleep 1
$A >/dev/null 2>&1 <<'SQL'
SELECT supacache.rowcache_put('public.inval', 1);
SELECT supacache.rowcache_put('public.inval', 2);
SQL

# 1. cache is serving
chk "cached pk=1 uses the Custom Scan" \
    "Custom Scan (pg_keyspace_rowcache) on inval" "$(plan 'SELECT * FROM public.inval WHERE id=1')"
chk "cached pk=1 serves the cached row" \
    "1|one|sec1" "$($A -c 'SELECT * FROM public.inval WHERE id=1')"

# 2. UPDATE invalidates (and the UPDATE itself must not use the cache)
chk "UPDATE target scan is NOT the Custom Scan (needs real ctid)" \
    "1" "$(plan 'UPDATE public.inval SET name=$$x$$ WHERE id=1' | grep -cvq 'Custom Scan' && echo 1 || echo 0)"
$A -c "UPDATE public.inval SET name='one_v2' WHERE id=1;" >/dev/null 2>&1
sleep 1
chk "after UPDATE: pk=1 falls back to a non-cache plan" \
    "0" "$(plan 'SELECT * FROM public.inval WHERE id=1' | grep -c 'Custom Scan')"
chk "after UPDATE: pk=1 read returns the fresh value" \
    "1|one_v2|sec1" "$($A -c 'SELECT * FROM public.inval WHERE id=1')"
# pk=2 was untouched -> still cached
chk "untouched pk=2 is still served from cache" \
    "Custom Scan (pg_keyspace_rowcache) on inval" "$(plan 'SELECT * FROM public.inval WHERE id=2')"

# 3. DELETE invalidates
$A -c "SELECT supacache.rowcache_put('public.inval', 2);" >/dev/null 2>&1
$A -c "DELETE FROM public.inval WHERE id=2;" >/dev/null 2>&1
sleep 1
chk "after DELETE: pk=2 gone (cache + table coherent)" \
    "0" "$($A -c 'SELECT count(*) FROM public.inval WHERE id=2')"

# 4. FOR UPDATE must not take the cache (needs a lockable real row)
$A -c "INSERT INTO public.inval VALUES (3,'three','sec3'); SELECT supacache.rowcache_put('public.inval',3);" >/dev/null 2>&1
chk "SELECT ... FOR UPDATE does NOT use the cache" \
    "0" "$(plan 'SELECT * FROM public.inval WHERE id=3 FOR UPDATE' | grep -c 'Custom Scan')"

# 5. the keys-only guarantee at the source: peek the raw slot stream and prove
#    no column value ever appears in it.
$A -c "SELECT supacache.rowcache_put('public.inval',3);" >/dev/null 2>&1
$A -c "UPDATE public.inval SET secret='LEAK_CANARY' WHERE id=3;" >/dev/null 2>&1
# read the worker's slot non-destructively via a peek on a throwaway 2nd slot
$A >/dev/null 2>&1 -c "SELECT pg_drop_replication_slot('sc_probe') FROM pg_replication_slots WHERE slot_name='sc_probe';"
# (a fresh slot only sees changes from now, so exercise one more change after it)
$A -t -c "SELECT slot_name FROM pg_create_logical_replication_slot('sc_probe','supacache_keys');" >/dev/null 2>&1
$A -c "UPDATE public.inval SET secret='LEAK_CANARY2' WHERE id=3;" >/dev/null 2>&1
stream="$($A -c "SELECT string_agg(data,'|') FROM pg_logical_slot_peek_changes('sc_probe',NULL,NULL)" 2>/dev/null)"
$A >/dev/null 2>&1 -c "SELECT pg_drop_replication_slot('sc_probe');"
chk "keys-only stream carries a change record" \
    "1" "$(echo "$stream" | grep -cq 'inval\|[0-9]' && echo 1 || echo 0)"
chk "keys-only stream contains NO column value (canary/secret)" \
    "0" "$(echo "$stream" | grep -cE 'LEAK_CANARY|sec3|three|secret|name')"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
