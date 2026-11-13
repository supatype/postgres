#!/bin/sh
# Asserts that auth.uid(), auth.role() and auth.email() read the claims PostgREST sets.
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

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all auth helper checks passed"
