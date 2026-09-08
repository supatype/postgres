#!/usr/bin/env bash
# the cost of reading a MASKED table, and how supacache.get
# (the SQL surface) accelerates it. Run against the PG17 base with
# supatype_mask + pg_keyspace loaded. Measures a full scan of a 100k-row table
# that references 3 masked columns (so supatype_mask wraps each in
# CASE WHEN <predicate>(row) THEN col ELSE NULL END -> 3 predicate calls/row),
# as a NON-EXEMPT role, under three predicate implementations.
set -u
PGPORT=${PGPORT:-5434}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q"

$P >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.bench_plain, public.bench_masked, public.bench_perms CASCADE;
CREATE TABLE public.bench_plain(id int primary key, c1 text, c2 text, c3 text, filler text);
INSERT INTO public.bench_plain SELECT g,'a'||g,'b'||g,'c'||g,repeat('x',200) FROM generate_series(1,100000) g;
CREATE TABLE public.bench_masked (LIKE public.bench_plain INCLUDING ALL);
INSERT INTO public.bench_masked SELECT * FROM public.bench_plain;
CREATE TABLE public.bench_perms(role text primary key, allowed bool);
INSERT INTO public.bench_perms VALUES ('bench_user', true) ON CONFLICT (role) DO UPDATE SET allowed=true;
-- trivial (inlinable), realistic table-lookup, and shmem predicates
CREATE OR REPLACE FUNCTION public.can_read_true (r public.bench_masked) RETURNS bool LANGUAGE sql STABLE AS $$ SELECT true $$;
CREATE OR REPLACE FUNCTION public.can_read_join (r public.bench_masked) RETURNS bool LANGUAGE sql STABLE AS
  $$ SELECT coalesce((SELECT allowed FROM public.bench_perms WHERE role = current_user), false) $$;
CREATE OR REPLACE FUNCTION public.can_read_shmem(r public.bench_masked) RETURNS bool LANGUAGE sql STABLE AS
  $$ SELECT supacache.get('perm:'||current_user) IS NOT NULL $$;
-- row-INDEPENDENT overload: zero args, SAME body as can_read_join, but supatype_mask
-- emits it as (SELECT can_read_norow()) -> planner InitPlan -> evaluated ONCE per scan.
CREATE OR REPLACE FUNCTION public.can_read_norow() RETURNS bool LANGUAGE sql STABLE AS
  $$ SELECT coalesce((SELECT allowed FROM public.bench_perms WHERE role = current_user), false) $$;
SELECT supacache.set('perm:bench_user','1'::bytea);
DROP ROLE IF EXISTS bench_user; CREATE ROLE bench_user LOGIN;
GRANT SELECT ON public.bench_plain, public.bench_masked, public.bench_perms TO bench_user;
GRANT USAGE ON SCHEMA supacache TO bench_user; GRANT EXECUTE ON FUNCTION supacache.get(text) TO bench_user;
ANALYZE public.bench_plain; ANALYZE public.bench_masked;
SQL

swap() { $P >/dev/null 2>&1 -c "
SECURITY LABEL FOR supatype ON COLUMN public.bench_masked.c1 IS 'MASK READ public.$1 WRITE public.$1';
SECURITY LABEL FOR supatype ON COLUMN public.bench_masked.c2 IS 'MASK READ public.$1 WRITE public.$1';
SECURITY LABEL FOR supatype ON COLUMN public.bench_masked.c3 IS 'MASK READ public.$1 WRITE public.$1';"; }
run() { local best=999999; for i in 1 2 3 4 5; do
  t=$(psql -h 127.0.0.1 -p $PGPORT -U bench_user -d postgres -X -q -A -t -c "\timing on" -c "$2" 2>/dev/null | grep -oE 'Time: [0-9.]+' | grep -oE '[0-9.]+')
  awk -v a="$t" -v b="$best" 'BEGIN{exit !(a<b)}' && best=$t; done
  printf "  %-46s %10s ms\n" "$1" "$best"; }

Q="SELECT count(length(c1)+length(c2)+length(c3)) FROM public.bench_masked"
echo "# masked-read cost (100k rows, 3 masked columns, min of 5 runs)"
run "plain, no mask" "SELECT count(length(c1)+length(c2)+length(c3)) FROM public.bench_plain"
swap can_read_true;  run "masked, predicate = trivial (inlined)"     "$Q"
swap can_read_join;  run "masked, predicate = table lookup per row"  "$Q"
swap can_read_shmem; run "masked, predicate = supacache.get"    "$Q"
swap can_read_norow; run "masked, predicate = row-indep. InitPlan"   "$Q"
