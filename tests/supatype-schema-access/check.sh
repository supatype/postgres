#!/bin/sh
# Asserts that the image creates the schema engine's internal schema, _supatype, and lets the API
# roles use it.
#
# Without USAGE on _supatype an engine-generated rule (a field rule, a Studio policy) fails for
# anon and authenticated with "permission denied for schema _supatype". The engine grants it itself
# only when it owns the schema, which it does not on a host where it pushes as a non-owner role.
# Only schema USAGE is granted: nothing on the tables or functions in it, and no CREATE.
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in the environment
#   when the container's pg_hba asks for one over the socket.
set -eu

container=$1
db=${2:-supatype_admin}
failed=0

# query <sql>: the value of one SQL expression, as supatype_admin.
query() {
	docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" "$container" \
		psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U supatype_admin -d "$db" -tAq -c "SELECT $1"
}

# expect <label> <expression> <expected output>
expect() {
	if got=$(query "$2" 2>&1) && [ "$got" = "$3" ]; then
		echo "ok      $1"
	else
		echo "FAIL    $1 (expected '$3', got '$got')"
		failed=$((failed + 1))
	fi
}

expect "_supatype exists" "to_regnamespace('_supatype') IS NOT NULL" "t"
expect "_supatype is owned by the image's admin role" "nspowner::regrole::text FROM pg_namespace WHERE nspname = '_supatype'" "supatype_admin"

for role in anon authenticated service_role; do
	expect "$role has USAGE on _supatype" "has_schema_privilege('$role', '_supatype', 'USAGE')" "t"
	expect "$role has no CREATE on _supatype" "has_schema_privilege('$role', '_supatype', 'CREATE')" "f"
done

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all _supatype schema access checks passed"
