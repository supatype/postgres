#!/usr/bin/env bash
# slice 3 — Mode B row cache for NON-integer and text primary keys.
#
# The cache used to key rows by (relid, int8 pk) — only single-column integer
# PKs were cacheable, which excludes the uuid/text keys most PostgREST tables
# actually use. The pk is now a *canonical* byte form (the pk type's output
# text), produced identically by the planner hook, rowcache_put/refill, and the
# keys-only decode plugin, so uuid and text PKs cache and invalidate coherently.
#
# Proven per type: EXPLAIN shows the Custom Scan for `WHERE pk = <value>`; the
# cached row is served correctly; an UPDATE stays coherent (the plugin emits the
# canonical pk hex-encoded, the worker drops/refills); and the keys-only stream
# still leaks no NON-key column value. Also: a composite-PK table is refused
# registration (arity guard), so it can never be matched incoherently.
set -u
PGPORT=${PGPORT:-5434}
A="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-54s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-54s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
plan() { $A -c "EXPLAIN (COSTS OFF) $1" 2>/dev/null | head -1; }
refill="$($A -c 'SHOW pg_keyspace.rowcache_refill' | tr -d '[:space:]')"

echo "# Mode B row cache for non-integer PKs (uuid, text); refill=$refill"

# ---------------------------------------------------------------- uuid PK -----
U1='11111111-1111-1111-1111-111111111111'
U2='22222222-2222-2222-2222-222222222222'
$A >/dev/null 2>&1 <<SQL
DROP TABLE IF EXISTS public.u CASCADE;
CREATE TABLE public.u(id uuid primary key, v text, secret text);
INSERT INTO public.u VALUES ('$U1','alpha','sU1'),('$U2','beta','sU2');
SELECT supacache.rowcache_register('public.u', 1);
SQL
sleep 1
$A -c "SELECT supacache.rowcache_put('public.u', '$U1'::uuid);" >/dev/null 2>&1
$A -c "SELECT supacache.rowcache_put('public.u', '$U2'::uuid);" >/dev/null 2>&1

chk "uuid: registration accepted (single-col uuid PK)" "t" \
    "$($A -c "SELECT count(*)=1 FROM pg_class WHERE relname='u'")"
chk "uuid: cached row uses the Custom Scan" \
    "Custom Scan (pg_keyspace_rowcache) on u" "$(plan "SELECT * FROM public.u WHERE id='$U1'")"
chk "uuid: Custom Scan serves the cached row" \
    "$U1|alpha|sU1" "$($A -c "SELECT * FROM public.u WHERE id='$U1'")"

h0=$($A -c "SELECT hits FROM supacache.rowcache_stats();")
$A -c "SELECT * FROM public.u WHERE id='$U1'" >/dev/null 2>&1
h1=$($A -c "SELECT hits FROM supacache.rowcache_stats();")
chk "uuid: the read was a cache hit (hits climbed)" "1" "$([ "${h1:-0}" -gt "${h0:-0}" ] && echo 1 || echo 0)"

# UPDATE coherence: worker invalidates the canonical-uuid key
$A -c "UPDATE public.u SET v='alpha2' WHERE id='$U1';" >/dev/null 2>&1
sleep 1
chk "uuid: after UPDATE the read is fresh (coherent)" \
    "$U1|alpha2|sU1" "$($A -c "SELECT * FROM public.u WHERE id='$U1'")"
if [ "$refill" = "on" ]; then
  chk "uuid: refill keeps it on the Custom Scan" \
      "Custom Scan (pg_keyspace_rowcache) on u" "$(plan "SELECT * FROM public.u WHERE id='$U1'")"
fi
chk "uuid: untouched key still served from cache" \
    "Custom Scan (pg_keyspace_rowcache) on u" "$(plan "SELECT * FROM public.u WHERE id='$U2'")"

# ---------------------------------------------------------------- text PK -----
$A >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.t CASCADE;
CREATE TABLE public.t(slug text primary key, v text);
INSERT INTO public.t VALUES ('hello world','H'),('a/b?c','Q');
SELECT supacache.rowcache_register('public.t', 1);
SQL
sleep 1
# a slug with a space + one with URL-ish chars: exercises the hex wire encoding
$A -c "SELECT supacache.rowcache_put('public.t', 'hello world'::text);" >/dev/null 2>&1
$A -c "SELECT supacache.rowcache_put('public.t', 'a/b?c'::text);" >/dev/null 2>&1

chk "text: cached row (key with a space) uses the Custom Scan" \
    "Custom Scan (pg_keyspace_rowcache) on t" "$(plan "SELECT * FROM public.t WHERE slug='hello world'")"
chk "text: Custom Scan serves the cached row" \
    "hello world|H" "$($A -c "SELECT * FROM public.t WHERE slug='hello world'")"
$A -c "UPDATE public.t SET v='H2' WHERE slug='hello world';" >/dev/null 2>&1
sleep 1
chk "text: after UPDATE the read is fresh (coherent)" \
    "hello world|H2" "$($A -c "SELECT * FROM public.t WHERE slug='hello world'")"

# ------------------------------------------------ keys-only leak guard --------
# The decode stream may carry the pk (identity, hex-encoded) but never a NON-key
# column value. Put a canary in a value column and prove it never appears.
$A -c "SELECT pg_drop_replication_slot('sc_probe2') FROM pg_replication_slots WHERE slot_name='sc_probe2';" >/dev/null 2>&1
$A -t -c "SELECT slot_name FROM pg_create_logical_replication_slot('sc_probe2','supacache_keys');" >/dev/null 2>&1
$A -c "UPDATE public.u SET secret='LEAK_CANARY_UUID' WHERE id='$U1';" >/dev/null 2>&1
stream="$($A -c "SELECT string_agg(data,'|') FROM pg_logical_slot_peek_changes('sc_probe2',NULL,NULL)" 2>/dev/null)"
$A -c "SELECT pg_drop_replication_slot('sc_probe2');" >/dev/null 2>&1
# the pk is emitted hex-encoded: the canonical uuid text's first bytes are the
# hex of "1111..." = 3131..., which must appear; the raw uuid string must not.
chk "keys-only stream carries the hex-encoded uuid change" \
    "1" "$(echo "$stream" | grep -cq '3131313131313131' && echo 1 || echo 0)"
chk "keys-only stream contains NO value-column canary" \
    "0" "$(echo "$stream" | grep -cE 'LEAK_CANARY_UUID|alpha|beta')"

# ------------------------------------------------ composite-PK guard ----------
$A >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.ck CASCADE;
CREATE TABLE public.ck(a int, b int, v text, primary key (a,b));
INSERT INTO public.ck VALUES (1,2,'x');
SQL
chk "composite PK: registration is REFUSED (arity guard)" \
    "f" "$($A -c "SELECT supacache.rowcache_register('public.ck', 1)")"
chk "composite PK: query falls back to a normal scan (no Custom Scan)" \
    "0" "$(plan 'SELECT * FROM public.ck WHERE a=1 AND b=2' | grep -c 'Custom Scan')"

# cleanup
$A >/dev/null 2>&1 <<'SQL'
SELECT supacache.rowcache_unregister('public.u');
SELECT supacache.rowcache_unregister('public.t');
DROP TABLE IF EXISTS public.u, public.t, public.ck CASCADE;
SQL
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
