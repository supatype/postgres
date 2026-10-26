#!/bin/sh
# Asserts that the Studio SQL editor role (SEC-SQL / SQ1a) inherits only the project owner's
# ownership rights and nothing a superuser or a predefined role would carry. The editor used to
# be a member of the demoted `postgres` role, which is a member of pg_read_all_data (reads
# pg_authid), pg_monitor and pg_signal_backend; a dedicated owner with no predefined-role
# memberships closes that.
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in the environment
#   when the container's pg_hba asks for one over the socket.
set -eu

container=$1
db=${2:-supatype_admin}
failed=0

psql_in() {
	docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" -i "$container" \
		psql -v ON_ERROR_STOP=1 -U supatype_admin -d "$db" -tAc "$1"
}

# expect <label> <sql returning t/f> <wanted>
expect() {
	got=$(psql_in "$2")
	if [ "$got" = "$3" ]; then
		echo "ok      $1 ($got)"
	else
		echo "FAIL    $1 (expected $3, got $got)"
		failed=$((failed + 1))
	fi
}

expect "editor is not a superuser" \
	"SELECT rolsuper FROM pg_roles WHERE rolname = 'supatype_sql_editor'" f
expect "editor cannot read pg_authid" \
	"SELECT has_table_privilege('supatype_sql_editor', 'pg_authid', 'SELECT')" f
expect "editor is not in pg_read_all_data" \
	"SELECT pg_has_role('supatype_sql_editor', 'pg_read_all_data', 'USAGE')" f
expect "editor is not in pg_read_all_stats" \
	"SELECT pg_has_role('supatype_sql_editor', 'pg_read_all_stats', 'USAGE')" f
expect "editor is not in pg_monitor" \
	"SELECT pg_has_role('supatype_sql_editor', 'pg_monitor', 'USAGE')" f
expect "editor is not in pg_signal_backend" \
	"SELECT pg_has_role('supatype_sql_editor', 'pg_signal_backend', 'USAGE')" f

# The only predefined role it may be in is pg_database_owner, which is how the owner holds
# rights on public. Any other pg_* membership is a leak.
expect "editor is in no predefined role except pg_database_owner" \
	"SELECT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname LIKE 'pg\\_%' AND r.rolname <> 'pg_database_owner' AND pg_has_role('supatype_sql_editor', r.oid, 'USAGE'))" f

# It is a member of the project owner, and that owner is itself a plain non-superuser.
expect "editor is a member of supatype_owner" \
	"SELECT pg_has_role('supatype_sql_editor', 'supatype_owner', 'USAGE')" t
expect "supatype_owner is not a superuser" \
	"SELECT rolsuper FROM pg_roles WHERE rolname = 'supatype_owner'" f
expect "supatype_owner is in no predefined role except pg_database_owner" \
	"SELECT EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname LIKE 'pg\\_%' AND r.rolname <> 'pg_database_owner' AND pg_has_role('supatype_owner', r.oid, 'USAGE'))" f

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all editor privilege checks passed"
