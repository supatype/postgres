#!/bin/sh
# Asserts that a table made by hand in `public` is private to service_role.
#
# The image used to grant every privilege on new tables in `public` to anon and
# authenticated by default, so any table nobody had written grants for was
# readable and writable with the anon key. Tables now belong to service_role
# (and their owner) until something grants more: the schema engine writes a
# model table's grants from its access rules.
#
# Sequences keep their defaults on purpose, so a serial column on a hand-made
# table, or a model table pushed by an engine that predates its own sequence
# grants, still works for the API roles once its table is granted.
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in
#   the environment when the container's pg_hba asks for one over the socket.
set -eu

container=$1
db=${2:-postgres}
failed=0

psql_in() {
	docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" -i "$container" \
		psql -v ON_ERROR_STOP=1 -U supatype_admin -d "$db" -tAc "$1"
}

# expect <description> <expected t|f> <sql returning one boolean>
expect() {
	got=$(psql_in "$3")
	if [ "$got" = "$2" ]; then
		echo "ok      $1"
	else
		echo "FAIL    $1 (expected $2, got $got)"
		failed=$((failed + 1))
	fi
}

# One table per role that creates tables in `public` in practice: supatype_admin
# (the image's own superuser, which owns a stack's database), and postgres (a
# developer at psql), where it may create there: in the `postgres` database, not
# in one the entrypoint created for POSTGRES_DB.
tables=cw3_by_admin
psql_in "DROP TABLE IF EXISTS public.cw3_by_admin;
         CREATE TABLE public.cw3_by_admin (id serial PRIMARY KEY);" > /dev/null
if [ "$(psql_in "SELECT has_schema_privilege('postgres', 'public', 'CREATE')")" = t ]; then
	tables="$tables cw3_by_postgres"
	psql_in "SET ROLE postgres; DROP TABLE IF EXISTS public.cw3_by_postgres;
	         CREATE TABLE public.cw3_by_postgres (id serial PRIMARY KEY);" > /dev/null
fi

for t in $tables; do
	for role in anon authenticated; do
		for priv in SELECT INSERT UPDATE DELETE; do
			expect "$role has no $priv on $t" f \
				"SELECT has_table_privilege('$role', 'public.$t', '$priv')"
		done
	done
	for priv in SELECT INSERT UPDATE DELETE; do
		expect "service_role has $priv on $t" t \
			"SELECT has_table_privilege('service_role', 'public.$t', '$priv')"
	done
	for role in anon authenticated service_role; do
		expect "$role keeps USAGE on ${t}_id_seq" t \
			"SELECT has_sequence_privilege('$role', 'public.${t}_id_seq', 'USAGE')"
	done
done

expect "no default table privileges in public name anon or authenticated" t \
	"SELECT NOT EXISTS (
	   SELECT 1 FROM pg_default_acl d, aclexplode(d.defaclacl) a
	    WHERE d.defaclnamespace = 'public'::regnamespace
	      AND d.defaclobjtype = 'r'
	      AND a.grantee IN ('anon'::regrole, 'authenticated'::regrole))"

psql_in "DROP TABLE IF EXISTS public.cw3_by_postgres, public.cw3_by_admin;" > /dev/null

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all default-privilege checks passed"
