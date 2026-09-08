#!/usr/bin/env bash
# §4.6 / §4.7 — Mode B row-cache SECURITY regression suite.
#
# The transparent row cache substitutes a CustomScan at the LEAF for a
# `pk = Const` lookup on a registered, currently-cached relation, serving the
# RAW cached tuple. The whole safety argument (§4.6) is that RLS quals and the
# supatype_mask CASE are re-applied ABOVE the leaf (in the scan's qual and
# targetlist), so the cache is never a policy bypass. This suite proves it by
# comparing the cached path against ground truth on the same rows.
#
# Run against the PG17 base with pg_keyspace + supatype_mask + pg_guard loaded.
set -u
PGPORT=${PGPORT:-5434}
ADMIN="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { # chk "label" "expected" "actual"
  if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
  else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi
}
asuser() { psql -h 127.0.0.1 -p $PGPORT -U "$1" -d postgres -X -q -A -t -c "$2" 2>&1; }

echo "# Mode B security regression (§4.6 leaf-only substitution)"

# ---- fixtures -------------------------------------------------------------
$ADMIN >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.rc_rls CASCADE;
DROP TABLE IF EXISTS public.rc_mask CASCADE;
DROP ROLE IF EXISTS rc_user;
DROP ROLE IF EXISTS rc_owner;
CREATE ROLE rc_user  LOGIN;
CREATE ROLE rc_owner LOGIN;
-- exempt role (named in supatype_mask.exempt_roles) for the unmasked case
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='service_role') THEN
    CREATE ROLE service_role LOGIN;
  END IF;
END $$;

-- RLS table: policy restricts rows to owner = current_user
CREATE TABLE public.rc_rls(id bigint primary key, owner name, data text);
INSERT INTO public.rc_rls VALUES (1,'rc_owner','owned-by-rc_owner'),
                                 (2,'rc_user','owned-by-rc_user');
ALTER TABLE public.rc_rls ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.rc_rls FORCE ROW LEVEL SECURITY;
CREATE POLICY rc_rls_sel ON public.rc_rls FOR SELECT USING (owner = current_user);
GRANT SELECT ON public.rc_rls TO rc_user, rc_owner;

-- Masked table: c_secret is masked; visible only if perms say so
CREATE TABLE public.rc_mask(id bigint primary key, label text, c_secret text);
INSERT INTO public.rc_mask VALUES (10,'row-ten','TOP-SECRET-10'),
                                  (11,'row-eleven','TOP-SECRET-11');
GRANT SELECT ON public.rc_mask TO rc_user, service_role;
CREATE OR REPLACE FUNCTION public.rc_can_read(r public.rc_mask) RETURNS bool
  LANGUAGE sql STABLE AS $$ SELECT current_user = 'service_role' $$;
SECURITY LABEL FOR supatype ON COLUMN public.rc_mask.c_secret
  IS 'MASK READ public.rc_can_read WRITE public.rc_can_read';

-- register + cache one row from each table
SELECT supacache.rowcache_register('public.rc_rls', 1);
SELECT supacache.rowcache_register('public.rc_mask', 1);
SELECT supacache.rowcache_put('public.rc_rls', 1);   -- rc_owner's row
SELECT supacache.rowcache_put('public.rc_rls', 2);   -- rc_user's row
SELECT supacache.rowcache_put('public.rc_mask', 10);
SQL

# ---- 0. the cache path is actually taken -----------------------------------
plan=$(asuser rc_user "EXPLAIN (COSTS OFF) SELECT * FROM public.rc_rls WHERE id=2" | head -1)
chk "cached pk lookup uses the Custom Scan node" \
    "Custom Scan (pg_keyspace_rowcache) on rc_rls" "$plan"

# ---- 1. RLS is re-applied above the cached leaf ----------------------------
# rc_user querying id=2 (their own row, cached) -> visible
chk "RLS: owner sees own cached row (id=2)" \
    "2|rc_user|owned-by-rc_user" "$(asuser rc_user 'SELECT * FROM public.rc_rls WHERE id=2')"
# rc_user querying id=1 (rc_owner's row, cached) -> RLS must hide it (0 rows)
chk "RLS: non-owner is DENIED a cached foreign row (id=1)" \
    "" "$(asuser rc_user 'SELECT * FROM public.rc_rls WHERE id=1')"
# rc_owner querying id=1 (their own, cached) -> visible
chk "RLS: owner sees own cached row (id=1)" \
    "1|rc_owner|owned-by-rc_owner" "$(asuser rc_owner 'SELECT * FROM public.rc_rls WHERE id=1')"
# ground truth: same denial WITHOUT the cache (uncached id has no cache entry)
chk "RLS: denial matches non-cached ground truth" \
    "$(asuser rc_user 'SELECT * FROM public.rc_rls WHERE id=1')" \
    "$(asuser rc_user 'SELECT id,owner,data FROM public.rc_rls WHERE id=1 AND id+0=1')"

# ---- 2. supatype_mask CASE is re-applied above the cached leaf -------------
# non-exempt rc_user: c_secret must be masked (NULL) even though raw bytes cached
chk "MASK: non-exempt role gets NULL for masked cached col" \
    "10|row-ten|" "$(asuser rc_user 'SELECT id,label,c_secret FROM public.rc_mask WHERE id=10')"
# exempt service_role: sees the real cached value
chk "MASK: exempt role sees real masked value (cached)" \
    "10|row-ten|TOP-SECRET-10" "$(asuser service_role 'SELECT id,label,c_secret FROM public.rc_mask WHERE id=10')"
# the cache genuinely holds the raw secret (superuser bypasses mask) -> proves
# masking is NOT done by storing NULL, but re-applied on read
chk "MASK: raw secret IS in the cache (superuser/plan uses Custom Scan)" \
    "10|row-ten|TOP-SECRET-10" "$($ADMIN -c 'SELECT id,label,c_secret FROM public.rc_mask WHERE id=10')"
plan2=$($ADMIN -c "EXPLAIN (COSTS OFF) SELECT * FROM public.rc_mask WHERE id=10" | head -1)
chk "MASK: that superuser read went through the Custom Scan" \
    "Custom Scan (pg_keyspace_rowcache) on rc_mask" "$plan2"

# ---- 3. parameterized / generic plans never take the const-only cache path -
# The hook only fires for pk = Const at plan time; a parameter ($1) must fall
# back to the normal plan (§4.3b: no baking one caller's row into a generic plan)
gplan=$($ADMIN <<'SQL' 2>&1 | grep -m1 -E 'Custom Scan|Index Scan|Seq Scan'
SET plan_cache_mode = force_generic_plan;
PREPARE q(bigint) AS SELECT * FROM public.rc_mask WHERE id=$1;
EXPLAIN (COSTS OFF) EXECUTE q(10);
SQL
)
chk "GENERIC: parameterized lookup does NOT use the cache" \
    "1" "$(echo "$gplan" | grep -cq 'Index Scan\|Seq Scan' && echo 1 || echo 0)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
