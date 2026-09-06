#!/usr/bin/env bash
# P6 — Mode B transparent row-cache LATENCY. What does substituting the cached
# row at the leaf save versus the normal index-scan + heap-fetch path, for a
# single-row pk lookup — the PostgREST hot path (§7.1)? And how does it behave
# on a MASKED table, where the mask CASE runs above the scan either way (§4.3c)?
#
# We compare two IDENTICAL tables: one registered+cached, one not. EXPLAIN
# ANALYZE separates planner-hook overhead (Planning Time) from executor cost
# (Execution Time); we report the min of N runs of each.
set -u
PGPORT=${PGPORT:-5434}
N=${N:-9}
ADMIN="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"

$ADMIN >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.rc_hot, public.rc_cold, public.rc_hotm, public.rc_coldm CASCADE;
-- a realistic-ish narrow row (a dozen columns) x 200k rows
CREATE TABLE public.rc_cold(
  id bigint primary key,
  a text, b text, c text, d text, e text,
  f int, g int, h timestamptz default now(), i bool, j numeric);
INSERT INTO public.rc_cold
  SELECT g, 'a'||g,'b'||g,'c'||g,'d'||g,'e'||g, g, g*2, now(), (g%2=0), g::numeric/3
  FROM generate_series(1,200000) g;
CREATE TABLE public.rc_hot   (LIKE public.rc_cold INCLUDING ALL);
INSERT INTO public.rc_hot SELECT * FROM public.rc_cold;

-- masked twins: mask a,b,c (3 predicate calls/row) for non-exempt roles
CREATE TABLE public.rc_coldm (LIKE public.rc_cold INCLUDING ALL);
INSERT INTO public.rc_coldm SELECT * FROM public.rc_cold;
CREATE TABLE public.rc_hotm  (LIKE public.rc_cold INCLUDING ALL);
INSERT INTO public.rc_hotm  SELECT * FROM public.rc_cold;
-- one predicate per table (supatype_mask resolves the predicate by the
-- column's own composite type; a mismatched type masks unconditionally)
CREATE OR REPLACE FUNCTION public.rc_mask_c(r public.rc_coldm) RETURNS bool
  LANGUAGE sql STABLE AS $$ SELECT current_user='service_role' $$;
CREATE OR REPLACE FUNCTION public.rc_mask_h(r public.rc_hotm) RETURNS bool
  LANGUAGE sql STABLE AS $$ SELECT current_user='service_role' $$;

DROP ROLE IF EXISTS rc_bench; CREATE ROLE rc_bench LOGIN;
GRANT SELECT ON public.rc_hot, public.rc_cold, public.rc_hotm, public.rc_coldm TO rc_bench;
ANALYZE public.rc_hot; ANALYZE public.rc_cold;
ANALYZE public.rc_hotm; ANALYZE public.rc_coldm;

-- register + cache the SAME pk (=100000) in the "hot" twins
SELECT supacache.rowcache_register('public.rc_hot', 1);
SELECT supacache.rowcache_register('public.rc_hotm', 1);
SELECT supacache.rowcache_put('public.rc_hot', 100000);
SELECT supacache.rowcache_put('public.rc_hotm', 100000);
SQL

# apply masks AFTER caching so the raw (unmasked) row is what's cached
$ADMIN >/dev/null 2>&1 <<'SQL'
SECURITY LABEL FOR supatype ON COLUMN public.rc_coldm.a IS 'MASK READ public.rc_mask_c WRITE public.rc_mask_c';
SECURITY LABEL FOR supatype ON COLUMN public.rc_coldm.b IS 'MASK READ public.rc_mask_c WRITE public.rc_mask_c';
SECURITY LABEL FOR supatype ON COLUMN public.rc_coldm.c IS 'MASK READ public.rc_mask_c WRITE public.rc_mask_c';
SECURITY LABEL FOR supatype ON COLUMN public.rc_hotm.a  IS 'MASK READ public.rc_mask_h WRITE public.rc_mask_h';
SECURITY LABEL FOR supatype ON COLUMN public.rc_hotm.b  IS 'MASK READ public.rc_mask_h WRITE public.rc_mask_h';
SECURITY LABEL FOR supatype ON COLUMN public.rc_hotm.c  IS 'MASK READ public.rc_mask_h WRITE public.rc_mask_h';
SQL

# min-of-N Planning/Execution time (ms) for one query, as role rc_bench
timeit() { # $1 label  $2 query
  local pbest=999999 ebest=999999 p e
  for _ in $(seq 1 "$N"); do
    read -r p e < <(psql -h 127.0.0.1 -p $PGPORT -U rc_bench -d postgres -X -q -A -t \
      -c "EXPLAIN (ANALYZE, TIMING ON, COSTS OFF) $2" 2>/dev/null \
      | awk -F': ' '/Planning Time/{p=$2} /Execution Time/{e=$2} END{gsub(/ ms/,"",p);gsub(/ ms/,"",e);print p, e}')
    awk -v a="$p" -v b="$pbest" 'BEGIN{exit !(a<b)}' && pbest=$p
    awk -v a="$e" -v b="$ebest" 'BEGIN{exit !(a<b)}' && ebest=$e
  done
  printf "  %-40s plan %8s ms   exec %8s ms\n" "$1" "$pbest" "$ebest"
}
node() { psql -h 127.0.0.1 -p $PGPORT -U rc_bench -d postgres -X -q -A -t -c "EXPLAIN (COSTS OFF) $1" 2>/dev/null | head -1; }

Q='SELECT * FROM public.%s WHERE id = 100000'
echo "# P6 Mode B row-cache latency (single-row pk=100000, min of $N, role rc_bench)"
echo "# plain twins:"
echo "    cold plan: $(node "$(printf "$Q" rc_cold)")   |   hot plan: $(node "$(printf "$Q" rc_hot)")"
timeit "cold: index scan + heap fetch"        "$(printf "$Q" rc_cold)"
timeit "hot : Mode B Custom Scan (shmem)"     "$(printf "$Q" rc_hot)"
echo "# masked twins (mask CASE on a,b,c applies above the scan either way):"
echo "    coldm plan: $(node "$(printf "$Q" rc_coldm)")  |  hotm plan: $(node "$(printf "$Q" rc_hotm)")"
timeit "coldm: masked, index scan + heap"     "$(printf "$Q" rc_coldm)"
timeit "hotm : masked, Mode B Custom Scan"    "$(printf "$Q" rc_hotm)"
