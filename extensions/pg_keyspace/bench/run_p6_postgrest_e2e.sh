#!/usr/bin/env bash
# P6 slice 3 — REAL PostgREST end-to-end over the Mode B row cache.
#
# This runs the actual PostgREST binary (v12.2.3) in front of the cluster: real
# HTTP requests with signed JWTs -> PostgREST opens a txn, SET ROLE from the
# `role` claim, exposes the JWT as `request.jwt.claims`, and issues the SELECT/
# UPDATE. The table is MASKED (supatype_mask), RLS-protected, and registered in
# the pg_keyspace row cache, so each GET is transparently served by the Mode B
# CustomScan — proven by the cache's own hit counter climbing per request.
#
# It shows, over real HTTP: (1) a GET is served from the cache (hits++);
# (2) the client sees exactly the direct-SQL ground truth; (3) the mask + RLS
# still apply on the cached path (owner sees email, a non-owner sees nothing);
# (4) a PATCH stays coherent because the keys-only worker invalidates the row.
set -u
PGPORT=${PGPORT:-5434}
HTTP=${HTTP:-3333}
SECRET=${SECRET:-pgkeyspace-e2e-hs256-secret-32bytes-min!}
BIN=${BIN:-/tmp/postgrest_bin}
CONF=/tmp/pgks_postgrest.conf
ADMIN="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-54s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-54s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

# Obtain the real PostgREST binary if it is not already present.
if [ ! -x "$BIN" ]; then
  TAG=v12.2.3; F=postgrest-$TAG-linux-static-x64.tar.xz
  if curl -fsSL -o /tmp/$F "https://github.com/PostgREST/postgrest/releases/download/$TAG/$F" 2>/dev/null; then
    tar xf /tmp/$F -C /tmp && mv /tmp/postgrest "$BIN" && chmod +x "$BIN"
  fi
fi
[ -x "$BIN" ] || { echo "  SKIP  PostgREST binary unavailable ($BIN)"; exit 0; }
$ADMIN -c "SELECT 1" >/dev/null 2>&1 || { echo "  SKIP  no cluster on :$PGPORT"; exit 0; }

# HS256 JWT signer: mkjwt ROLE UID  -> a token with {role, uid}
mkjwt() { python3 - "$SECRET" "$1" "$2" <<'PY'
import sys, json, hmac, hashlib, base64
secret, role, uid = sys.argv[1], sys.argv[2], sys.argv[3]
b64 = lambda b: base64.urlsafe_b64encode(b).rstrip(b'=')
seg = lambda o: b64(json.dumps(o, separators=(',', ':')).encode())
h = seg({"alg": "HS256", "typ": "JWT"})
p = seg({"role": role, "uid": uid})
sig = b64(hmac.new(secret.encode(), h + b'.' + p, hashlib.sha256).digest())
print((h + b'.' + p + b'.' + sig).decode())
PY
}

echo "# P6 — REAL PostgREST e2e over masked + RLS + row-cached table ($($BIN --version))"

# --- schema: roles, table, claims-based RLS + mask, row cache -----------------
$ADMIN >/dev/null 2>&1 <<'SQL'
DROP TABLE IF EXISTS public.pr CASCADE;
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='anon') THEN CREATE ROLE anon NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='webuser') THEN CREATE ROLE webuser NOLOGIN; END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='authenticator') THEN CREATE ROLE authenticator LOGIN NOINHERIT; END IF;
END $$;
GRANT anon TO authenticator;
GRANT webuser TO authenticator;
GRANT USAGE ON SCHEMA public TO anon, webuser, authenticator;

CREATE TABLE public.pr(id bigint primary key, owner text, email text);
INSERT INTO public.pr VALUES (1,'alice','alice@x.com'),(2,'bob','bob@x.com');
GRANT SELECT, UPDATE ON public.pr TO webuser;

-- uid comes from the PostgREST-set JWT claims JSON (v12: request.jwt.claims)
CREATE OR REPLACE FUNCTION public.jwt_uid() RETURNS text LANGUAGE sql STABLE AS
  $$ SELECT nullif(current_setting('request.jwt.claims', true),'')::json ->> 'uid' $$;

ALTER TABLE public.pr ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.pr FORCE ROW LEVEL SECURITY;
CREATE POLICY pr_sel ON public.pr FOR SELECT USING (owner = public.jwt_uid());
CREATE POLICY pr_upd ON public.pr FOR UPDATE USING (owner = public.jwt_uid());

-- mask email unless you own the row (row-dependent predicate)
CREATE OR REPLACE FUNCTION public.pr_can_read(r public.pr) RETURNS bool LANGUAGE sql STABLE AS
  $$ SELECT ($1).owner = public.jwt_uid() $$;
SECURITY LABEL FOR supatype ON COLUMN public.pr.email IS 'MASK READ public.pr_can_read WRITE public.pr_can_read';

SELECT supacache.rowcache_register('public.pr', 1);
SQL
sleep 1
$ADMIN -c "SELECT supacache.rowcache_put('public.pr', 1);" >/dev/null 2>&1  # warm id=1

# --- start real PostgREST ----------------------------------------------------
cat > "$CONF" <<CONF
db-uri = "postgres://authenticator@127.0.0.1:$PGPORT/postgres"
db-schemas = "public"
db-anon-role = "anon"
jwt-secret = "$SECRET"
server-port = $HTTP
server-host = "127.0.0.1"
CONF
"$BIN" "$CONF" >/tmp/pgks_postgrest.log 2>&1 &
PID=$!
trap 'kill $PID 2>/dev/null' EXIT
ready=0
for _ in $(seq 1 40); do
  curl -fsS "http://127.0.0.1:$HTTP/" >/dev/null 2>&1 && { ready=1; break; }
  sleep 0.25
done
[ "$ready" = 1 ] || { echo "  FAIL  PostgREST did not become ready"; tail -20 /tmp/pgks_postgrest.log; exit 1; }

ALICE=$(mkjwt webuser alice); BOB=$(mkjwt webuser bob)
GET() { curl -fsS -H "Authorization: Bearer $1" "http://127.0.0.1:$HTTP/pr?id=eq.1"; }

# (1) served from the cache: the hit counter must climb across a GET
h0=$($ADMIN -c "SELECT hits FROM supacache.rowcache_stats();")
body="$(GET "$ALICE")"
h1=$($ADMIN -c "SELECT hits FROM supacache.rowcache_stats();")
chk "GET as alice returns exactly one row"      "1" "$(echo "$body" | jq 'length')"
chk "row cache served the GET (hits climbed)"   "1" "$([ "${h1:-0}" -gt "${h0:-0}" ] && echo 1 || echo 0)"

# (2) mask + RLS on the cached path: owner sees id/owner/email
chk "owner alice sees id"                       "1"           "$(echo "$body" | jq -r '.[0].id')"
chk "owner alice sees owner"                    "alice"       "$(echo "$body" | jq -r '.[0].owner')"
chk "owner alice sees UNMASKED email"           "alice@x.com" "$(echo "$body" | jq -r '.[0].email')"

# (3) non-owner bob: RLS hides the (cached) row entirely -> empty array
chk "non-owner bob sees no rows (RLS on cache)" "0" "$(GET "$BOB" | jq 'length')"

# ground truth: identical to the direct-SQL non-cached path
gt="$(psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -A -t 2>/dev/null <<SQL
BEGIN; SET LOCAL role webuser;
SELECT set_config('request.jwt.claims', '{"uid":"alice"}', true);
SELECT email FROM public.pr WHERE id=1; COMMIT;
SQL
)"
gt="$(echo "$gt" | grep -x 'alice@x.com')"
chk "PostgREST email == direct-SQL ground truth" "alice@x.com" "$gt"

# (4) PATCH coherence: UPDATE via HTTP, then GET reflects it (worker invalidated)
curl -fsS -X PATCH -H "Authorization: Bearer $ALICE" -H "Content-Type: application/json" \
     -d '{"email":"alice2@x.com"}' "http://127.0.0.1:$HTTP/pr?id=eq.1" >/dev/null 2>&1
sleep 1  # keys-only worker invalidates id=1
chk "after PATCH: GET reflects new email (coherent)" \
    "alice2@x.com" "$(GET "$ALICE" | jq -r '.[0].email')"

# cleanup
$ADMIN -c "SELECT supacache.rowcache_unregister('public.pr');" >/dev/null 2>&1
$ADMIN -c "UPDATE public.pr SET email='alice@x.com' WHERE id=1;" >/dev/null 2>&1
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
