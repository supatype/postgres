#!/bin/sh
# Asserts that auth.uid(), auth.role(), auth.email(), auth.jwt(), auth.claim(), auth.claim_text() and
# auth.roles() read the claims PostgREST sets.
#
# PostgREST (v10 and later) puts the verified token in one JSON setting, `request.jwt.claims`, and
# no longer sets the per-claim `request.jwt.claim.sub`, `.role` and `.email` the image's functions
# read. On a fresh database auth.uid() was therefore NULL for every real request, and a row
# security rule written on it matched nothing. The functions now read `request.jwt.claims` and fall
# back to the per-claim settings when no claims are set, with the same NULL behaviour as the
# helpers the Supatype schema engine installs:
#
#   auth.uid()    NULL when there is no subject
#   auth.role()   app_metadata.role, else the top-level role claim, else 'anon'
#   auth.email()  the email claim, else the empty string
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in the environment
#   when the container's pg_hba asks for one over the socket.
set -eu

container=$1
db=${2:-supatype_admin}
failed=0

CLAIMS='request.jwt.claims'
SUB=11111111-1111-1111-1111-111111111111
OLD_SUB=22222222-2222-2222-2222-222222222222

# run <setup sql> <expression>: the expression's value with the settings applied in one transaction.
# The setup's own output goes to /dev/null so only the expression is read back.
run() {
	printf '%s\n' 'BEGIN;' '\o /dev/null' "$1;" '\o' "SELECT $2;" 'ROLLBACK;' |
		docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" -i "$container" \
			psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U supatype_admin -d "$db" -tAq
}

# expect <label> <setup sql> <expression> <expected output>
expect() {
	if got=$(run "$2" "$3" 2>&1) && [ "$got" = "$4" ]; then
		echo "ok      $1"
	else
		echo "FAIL    $1 (expected '$4', got '$got')"
		failed=$((failed + 1))
	fi
}

claims() { echo "SELECT set_config('$CLAIMS', '$1', true)"; }
legacy() { echo "SELECT set_config('request.jwt.claim.$1', '$2', true)"; }

# What PostgREST does on a request today: one JSON setting.
expect "claims: uid is the sub claim" \
	"$(claims '{"sub":"'$SUB'","role":"authenticated","email":"a@b.dev"}')" "auth.uid()::text" "$SUB"
expect "claims: role is the top-level role claim" \
	"$(claims '{"sub":"'$SUB'","role":"authenticated"}')" "auth.role()" "authenticated"
expect "claims: app_metadata.role wins over the top-level role" \
	"$(claims '{"role":"authenticated","app_metadata":{"role":"admin"}}')" "auth.role()" "admin"
expect "claims: email is the email claim" \
	"$(claims '{"email":"a@b.dev"}')" "auth.email()" "a@b.dev"

# NULL semantics shared with the engine's helpers.
expect "claims without a sub: uid is NULL" \
	"$(claims '{"role":"anon"}')" "coalesce(auth.uid()::text, 'null')" "null"
expect "claims with an empty sub: uid is NULL" \
	"$(claims '{"sub":""}')" "coalesce(auth.uid()::text, 'null')" "null"
expect "no settings at all: uid is NULL" \
	"SELECT 1" "coalesce(auth.uid()::text, 'null')" "null"
expect "no settings at all: role is anon" "SELECT 1" "auth.role()" "anon"
expect "no settings at all: email is the empty string" "SELECT 1" "'[' || auth.email() || ']'" "[]"
expect "an empty claims setting reads as anonymous, not a JSON error" \
	"$(claims '')" "coalesce(auth.uid()::text, 'null') || auth.role()" "nullanon"

# Older PostgREST, and any caller that sets the per-claim settings by hand.
expect "legacy settings: uid" "$(legacy sub $OLD_SUB)" "auth.uid()::text" "$OLD_SUB"
expect "legacy settings: role" "$(legacy role service_role)" "auth.role()" "service_role"
expect "legacy settings: email" "$(legacy email old@b.dev)" "auth.email()" "old@b.dev"
expect "claims win over the legacy settings when both are set" \
	"$(claims '{"sub":"'$SUB'"}'); $(legacy sub $OLD_SUB)" "auth.uid()::text" "$SUB"
expect "claims without a sub do not borrow a stale legacy sub" \
	"$(claims '{"role":"anon"}'); $(legacy sub $OLD_SUB)" "coalesce(auth.uid()::text, 'null')" "null"

# auth.jwt(), auth.claim(path), auth.claim_text(path) and auth.roles() ship with the image too. Row security written
# Supabase-style calls them, and on a project whose owner cannot create objects in schema auth
# the engine's push cannot install them. Same sources and the same NULL behaviour as the engine's
# definitions of them.
TIER='{"sub":"'$SUB'","role":"authenticated","app_metadata":{"tier":"pro","sites":["a","b"]},"roles":["editor","admin"]}'

expect "auth.jwt, auth.claim, auth.claim_text and auth.roles exist with the engine's signatures" "SELECT 1" \
	"(SELECT string_agg(p.proname || '(' || pg_get_function_identity_arguments(p.oid) || ')' || ' ' || pg_get_function_result(p.oid) || ' ' || p.provolatile::text, ',' ORDER BY p.proname)
	    FROM pg_proc p WHERE p.pronamespace = 'auth'::regnamespace AND p.proname IN ('jwt', 'claim', 'claim_text', 'roles'))" \
	"claim(claim_path text) jsonb s,claim_text(claim_path text) text s,jwt() jsonb s,roles() text[] s"

expect "claims: jwt returns the claims" \
	"$(claims "$TIER")" "auth.jwt() ->> 'sub'" "$SUB"
expect "claims: claim reads a nested path" \
	"$(claims "$TIER")" "auth.claim('app_metadata.tier') #>> '{}'" "pro"
expect "claims: claim keeps an array a jsonb array" \
	"$(claims "$TIER")" "auth.claim('app_metadata.sites')::text" '["a", "b"]'
expect "claims: a missing path is NULL" \
	"$(claims "$TIER")" "coalesce(auth.claim('app_metadata.nope')::text, 'null')" "null"
expect "claims: roles is the roles claim as a text array" \
	"$(claims "$TIER")" "auth.roles()::text" "{editor,admin}"

expect "legacy settings: jwt carries sub, role and email" \
	"$(legacy sub $OLD_SUB); $(legacy role service_role); $(legacy email old@b.dev)" \
	"(auth.jwt() ->> 'sub') || ' ' || (auth.jwt() ->> 'role') || ' ' || (auth.jwt() ->> 'email')" \
	"$OLD_SUB service_role old@b.dev"
expect "legacy settings: claim reads them by name" \
	"$(legacy role service_role)" "auth.claim('role') #>> '{}'" "service_role"
expect "claims win over the legacy settings for jwt" \
	"$(claims '{"sub":"'$SUB'"}'); $(legacy sub $OLD_SUB); $(legacy role service_role)" \
	"(auth.jwt() ->> 'sub') || ' ' || coalesce(auth.jwt() ->> 'role', 'null')" "$SUB null"

expect "no settings at all: jwt is an empty object" "SELECT 1" "auth.jwt()::text" "{}"
expect "no settings at all: claim is NULL" "SELECT 1" "coalesce(auth.claim('sub')::text, 'null')" "null"
expect "no settings at all: roles is the empty array" "SELECT 1" "auth.roles()::text" "{}"
expect "an empty claims setting reads as anonymous for jwt, claim and roles" \
	"$(claims '')" "auth.jwt()::text || coalesce(auth.claim('sub')::text, 'null') || auth.roles()::text" "{}null{}"

expect "claims: claim_text reads a nested path as text" \
	"$(claims "$TIER")" "auth.claim_text('app_metadata.tier')" "pro"
expect "claims: claim_text of an array is its json text" \
	"$(claims "$TIER")" "auth.claim_text('app_metadata.sites')" '["a", "b"]'
expect "claims: claim_text of a missing path is NULL" \
	"$(claims "$TIER")" "coalesce(auth.claim_text('app_metadata.nope'), 'null')" "null"
expect "legacy settings: claim_text reads them by name" \
	"$(legacy role service_role)" "auth.claim_text('role')" "service_role"
expect "no settings at all: claim_text is NULL" "SELECT 1" "coalesce(auth.claim_text('sub'), 'null')" "null"
expect "an empty claims setting reads as NULL for claim_text" "$(claims '')" "coalesce(auth.claim_text('sub'), 'null')" "null"

# The API roles run every query, so they have to be able to call all seven.
for role in anon authenticated; do
	expect "$role can call all seven helpers" \
		"$(claims "$TIER"); SET LOCAL ROLE $role" \
		"(auth.uid() IS NOT NULL) AND auth.role() = 'authenticated' AND auth.email() = ''
		 AND auth.jwt() ? 'sub' AND auth.claim('app_metadata.tier') IS NOT NULL AND auth.claim_text('app_metadata.tier') = 'pro' AND auth.roles() = '{editor,admin}'" "t"
done

# One owner for all seven (D14): the image owns the auth helpers.
expect "all seven helpers exist with one owner and one set of grants" "SELECT 1" \
	"(SELECT count(DISTINCT proowner) || ' owner, ' || count(DISTINCT proacl::text) || ' acl, ' || count(*) || ' functions' FROM pg_proc
	   WHERE pronamespace = 'auth'::regnamespace AND proname IN ('uid', 'role', 'email', 'jwt', 'claim', 'claim_text', 'roles'))" "1 owner, 1 acl, 7 functions"

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all auth helper checks passed"
