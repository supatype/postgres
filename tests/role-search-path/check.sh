#!/bin/sh
# Asserts that the image's roles keep the search path they were given, `$user` included.
#
# The initial setup wrote `ALTER ROLE ... SET search_path TO "\$user", ...` in a file psql runs with
# `-f`, where nothing unescapes the backslash, so the stored path named a schema called `\$user`
# rather than the role's own. A migration corrects a cluster made that way.
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in the environment
#   when the container's pg_hba asks for one over the socket.
set -eu

container=$1
db=${2:-postgres}
failed=0

psql_in() {
	docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" -i "$container" \
		psql -v ON_ERROR_STOP=1 -U supatype_admin -d "$db" -tAc "$1"
}

# path_of <role>: the search_path a role is given in every database, or nothing.
path_of() {
	psql_in "SELECT substr(c, length('search_path=') + 1)
	           FROM pg_db_role_setting s, unnest(s.setconfig) AS c
	          WHERE s.setrole = '$1'::regrole AND s.setdatabase = 0 AND c LIKE 'search_path=%'"
}

# expect_path <role> <path>
expect_path() {
	got=$(path_of "$1")
	if [ "$got" = "$2" ]; then
		echo "ok      $1 search_path is $2"
	else
		echo "FAIL    $1 search_path (expected $2, got $got)"
		failed=$((failed + 1))
	fi
}

expect_path supatype_admin '"$user", public, auth, extensions'
expect_path postgres '"$user", public, extensions'

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all role search path checks passed"
