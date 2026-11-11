#!/bin/sh
set -eu

# supatype-summary: create pg_keyspace's database and extension

#######################################
# Puts the pg_keyspace extension where the worker looks for it, while the temporary server is up
# and before the real one starts:
#
#   1. the extension in POSTGRES_DB, as the image always has, so its SQL surface is there;
#   2. the keyspace database, SUPATYPE_KEYSPACE_DATABASE, when it names another one: created if
#      missing, with the extension in it too. The worker connects to that database
#      (pg_keyspace.database) and keeps its credentials and backing tables there, so an extension
#      that existed only in POSTGRES_DB left it serving without persistence.
#
# The RESP password is not set here. The image points pg_keyspace.password_file at a file holding
# it, and the extension reads that file itself, so the password never passes through psql.
#
# The image runs this on the first boot, from the bootstrap, and on every later start, from the
# entrypoint, so a database that has gone missing comes back. Every step is idempotent, and each
# database gets one psql session.
#
# Env vars:
#   SUPATYPE_KEYSPACE_DATABASE  the database the worker uses (default POSTGRES_DB).
#   SUPATYPE_MIGRATION_ROLE     role to connect as (default supatype_admin). It must be able to
#                               create databases and extensions, which in the image means the
#                               bootstrap superuser.
#   POSTGRES_DB                 the stack's database (defaults to POSTGRES_USER, then postgres).
#   PGHOST, PGPORT              as psql reads them. Unset, the Unix socket.
#
# Exit code:
#   0 when every step succeeded; 1 when any failed, saying what that leaves behind.
#######################################

PROG="${SUPATYPE_CMD:-$(basename "$0")}"
role="${SUPATYPE_MIGRATION_ROLE:-supatype_admin}"
main_db="${POSTGRES_DB:-${POSTGRES_USER:-postgres}}"
ks_db="${SUPATYPE_KEYSPACE_DATABASE:-$main_db}"
rc=0

note() { echo "$PROG: $*"; }
warn() {
	echo "$PROG: $*" >&2
	rc=1
}

run_psql() {
	psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -q -U "$role" "$@"
}

# POSTGRES_DB: the extension and, when the keyspace has a database of its own, that database.
# CREATE DATABASE cannot run in a transaction or take IF NOT EXISTS, so the statement is built only
# when the database is missing and run by \gexec. The name goes through %I, so any name works.
setup_main_database() {
	run_psql -d "$main_db" -v "ks_db=$ks_db" <<'SQL'
CREATE EXTENSION IF NOT EXISTS pg_keyspace;
SELECT format('CREATE DATABASE %I', :'ks_db')
 WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'ks_db') \gexec
SQL
}

if setup_main_database; then
	note "pg_keyspace extension present in '$main_db'."
else
	warn "could not create pg_keyspace in '$main_db' or the database '$ks_db'; RESP will serve without persistence."
fi

if [ "$ks_db" != "$main_db" ]; then
	if run_psql -d "$ks_db" -c 'CREATE EXTENSION IF NOT EXISTS pg_keyspace'; then
		note "pg_keyspace extension present in its own database '$ks_db'."
	else
		warn "could not create pg_keyspace in '$ks_db'; RESP will serve without persistence."
	fi
fi

exit "$rc"
