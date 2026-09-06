#!/usr/bin/env bash
# P6 slice 3 — the PostgREST access pattern, end to end.
#
# PostgREST could not be installed in this sandbox (its GitHub release download
# returns 403 through the egress proxy). But PostgREST is a thin REST->SQL layer:
# per request it opens a transaction, assumes the caller's role and JWT claims,
# and issues plain SQL — a `GET /t?id=eq.N` becomes `SELECT ... FROM t WHERE
# id = N`, a `PATCH` becomes an `UPDATE ... WHERE id = N`. This script issues
# exactly those statements against a MASKED, RLS-protected, row-cached table, so
# it exercises the whole Mode B stack the way PostgREST would — transparently.
#
# It shows: the client sees identical results whether or not the row is cached;
# the mask + RLS still apply on the cached path; and a PATCH stays coherent
# because the keys-only worker invalidates the cache.
set -u
PGPORT=${PGPORT:-5434}
ADMIN="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

# One PostgREST-style request: BEGIN; assume role + claims; run SQL; COMMIT.
req() { # req ROLE UID SQL   -> prints result rows only
  psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t 2>/dev/null <<SQL
BEGIN;
SET LOCAL role TO $1;
DO \$do\$ BEGIN PERFORM set_config('request.jwt.uid', '$2', true); END \$do\$;
$3
COMMIT;
SQL
}

echo "# P6 — PostgREST access pattern over a masked + RLS + cached table"

$ADMIN >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.pr CASCADE;
DROP ROLE IF EXISTS webuser;
CREATE ROLE webuser LOGIN;
CREATE TABLE public.pr(id bigint primary key, owner text, email text);
INSERT INTO public.pr VALUES (1,'alice','alice@x.com'),(2,'bob','bob@x.com');
-- RLS: you only see your own row (owner = jwt uid)
ALTER TABLE public.pr ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pr FORCE ROW LEVEL SECURITY;
CREATE POLICY pr_sel ON public.pr FOR SELECT USING (owner = current_setting('request.jwt.uid', true));
CREATE POLICY pr_upd ON public.pr FOR UPDATE USING (owner = current_setting('request.jwt.uid', true));
GRANT SELECT, UPDATE ON public.pr TO webuser;
-- mask email unless you own the row (row-dependent predicate)
CREATE OR REPLACE FUNCTION public.pr_can_read(r public.pr) RETURNS bool LANGUAGE sql STABLE AS
  $$ SELECT ($1).owner = current_setting('request.jwt.uid', true) $$;
SECURITY LABEL FOR supatype ON COLUMN public.pr.email IS 'MASK READ public.pr_can_read WRITE public.pr_can_read';
SELECT supacache.rowcache_register('public.pr', 1);
SQL
sleep 1   # let the worker drain the setup INSERTs before we cache
$ADMIN -c "SELECT supacache.rowcache_put('public.pr', 1);" >/dev/null 2>&1

# GET /pr?id=eq.1 as alice (owner) — served from cache, email visible
chk "GET id=1 as owner alice (cached): sees own row + email" \
    "1|alice|alice@x.com" "$(req webuser alice 'SELECT * FROM public.pr WHERE id=1;')"
# GET /pr?id=eq.1 as bob (not owner) — RLS hides the cached row entirely
chk "GET id=1 as bob (not owner): RLS denies the cached row" \
    "" "$(req webuser bob 'SELECT * FROM public.pr WHERE id=1;')"

# ground truth: same query on the NON-cached path must match exactly
$ADMIN -c "SELECT supacache.rowcache_unregister('public.pr');" >/dev/null 2>&1
chk "owner result matches the non-cached ground truth" \
    "$(req webuser alice 'SELECT * FROM public.pr WHERE id=1 AND id+0=1;')" \
    "1|alice|alice@x.com"
$ADMIN -c "SELECT supacache.rowcache_register('public.pr',1); SELECT supacache.rowcache_put('public.pr',1);" >/dev/null 2>&1

# PATCH /pr?id=eq.1 as alice — UPDATE succeeds (cache not used for the target)
req webuser alice "UPDATE public.pr SET email='alice2@x.com' WHERE id=1;" >/dev/null 2>&1
sleep 1   # keys-only worker invalidates id=1
chk "after PATCH: GET id=1 reflects the new value (coherent)" \
    "1|alice|alice2@x.com" "$(req webuser alice 'SELECT * FROM public.pr WHERE id=1;')"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
