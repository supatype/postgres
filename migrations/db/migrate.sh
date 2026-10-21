#!/bin/sh
set -eu

#######################################
# Used by both ami and docker builds to initialise database schema.
# Env vars:
#   POSTGRES_DB        defaults to postgres
#   POSTGRES_HOST      if set, psql uses TCP to this host; if unset, use Unix socket
#                      (required for official postgres image first-boot init)
#   POSTGRES_PORT      defaults to 5432
#   POSTGRES_PASSWORD  the password supatype_admin will carry. Unset under an operator such
#                      as CloudNativePG, which made that role before this script ran and
#                      keeps its password itself: the role is then left as found.
#   AUTHENTICATOR_PASSWORD  the password PostgREST connects with. Falls back to
#                      POSTGRES_PASSWORD; with neither set the script refuses to start.
#   USE_DBMATE         defaults to ""
# Exit code:
#   0 if migration succeeds, non-zero on error.
#######################################

export PGDATABASE="${POSTGRES_DB:-postgres}"
export PGPORT="${POSTGRES_PORT:-5432}"
if [ -n "${POSTGRES_HOST:-}" ]; then
	export PGHOST="$POSTGRES_HOST"
fi
export PGPASSWORD="${POSTGRES_PASSWORD:-}"

# if args are supplied, simply forward to dbmate
_connect_host="${PGHOST:-localhost}"
connect="$PGPASSWORD@$_connect_host:$PGPORT/$PGDATABASE?sslmode=disable"
if [ "$#" -ne 0 ]; then
    export DATABASE_URL="${DATABASE_URL:-postgres://supatype_admin:$connect}"
    exec dbmate "$@"
    exit 0
fi

# Which passwords this run will leave behind, settled before anything touches the database:
# the init scripts below are not re-runnable, so a bootstrap that stops half-way cannot
# simply be run again.
#
# supatype_admin: POSTGRES_PASSWORD, when given. Under an operator such as CloudNativePG the
# role was made before this script ran, with a password the operator keeps in its own Secret,
# and the variable is unset. Writing '' then would not be a no-op: Postgres stores it as no
# password at all, and the operator does not put its own back until its Secret next changes.
if [ -z "$PGPASSWORD" ]; then
    echo "$0: note: POSTGRES_PASSWORD is unset; leaving supatype_admin's password as it is." >&2
fi

# authenticator: PostgREST connects as it, so it needs a password, and '' would again be none.
# `AUTHENTICATOR_PASSWORD`, not `POSTGRES_PASSWORD`: the latter is the operator's, for direct
# SQL access, and rotating it must not take the REST API down. Falls back to it only so a
# hand-rolled stack that never set the new variable still boots -- with a warning, because a
# shared credential is not the intended posture.
_auth_pw="${AUTHENTICATOR_PASSWORD:-$PGPASSWORD}"
if [ -z "$_auth_pw" ]; then
    echo "$0: ERROR: neither AUTHENTICATOR_PASSWORD nor POSTGRES_PASSWORD is set, so the" >&2
    echo "$0:        authenticator role would have no password and PostgREST could never" >&2
    echo "$0:        connect. Set AUTHENTICATOR_PASSWORD. This script has changed nothing," >&2
    echo "$0:        but under the image's own entrypoint initdb has already run: remove the volume" >&2
    echo "$0:        before starting again, or the next start adopts it as already migrated." >&2
    exit 1
fi
if [ -z "${AUTHENTICATOR_PASSWORD:-}" ]; then
    echo "$0: WARNING: AUTHENTICATOR_PASSWORD is unset; falling back to POSTGRES_PASSWORD." >&2
    echo "$0:          Set AUTHENTICATOR_PASSWORD so rotating the database password cannot" >&2
    echo "$0:          break PostgREST." >&2
fi

# supatype_sql_editor: the role Studio's SQL editor connects as (SEC-SQL), from its own
# variable and nothing else. No fallback to `POSTGRES_PASSWORD`: the editor must not share
# the operator's direct-SQL credential -- that is the whole point of giving it its own
# non-superuser login. Unset, the role is left without a password and cannot log in, which is
# the safe default: the host that runs the editor (the CLI's reconcile, or the Cloud
# provisioner) sets it.
_editor_pw="${SUPATYPE_SQL_EDITOR_PASSWORD:-}"
if [ -z "$_editor_pw" ]; then
    echo "$0: note: SUPATYPE_SQL_EDITOR_PASSWORD is unset; the Studio SQL editor role has no" >&2
    echo "$0:       password and cannot log in until the host sets one." >&2
fi

# supatype_admin's password, only when one was given (see above). Before the migrations, as
# postgres: the demote migration takes postgres's superuser away, and with it the right to
# alter supatype_admin. In the dbmate branch it is also what lets dbmate connect at all.
#
# Every password reaches SQL through set_role_password: psql reads it from the environment
# (\getenv, nothing on the command line for `ps` to see) and quotes it itself (:'pw', so a
# quote in a password cannot break the statement, print it, and stop a bootstrap that cannot
# be run again). Each such session first turns statement logging off, in the server log and
# in pg_stat_statements: the image logs DDL verbatim, and ALTER USER ... PASSWORD is DDL.
quiet="SET log_statement = 'none'; SET log_min_error_statement = 'panic'; SET pg_stat_statements.track = 'none';"

# set_role_password <connect as> <role> <name of the exported variable holding the password>
set_role_password() {
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U "$1" <<EOSQL
$quiet
\\getenv pw $3
ALTER USER $2 WITH PASSWORD :'pw';
EOSQL
}

set_admin_password() {
    if [ -n "$PGPASSWORD" ]; then
        set_role_password postgres supatype_admin PGPASSWORD
    fi
}

db=$( cd -- "$( dirname -- "$0" )" > /dev/null 2>&1 && pwd )

# Applying migrations is apply-migrations.sh's job, here and at start-up, so that
# both paths write the same ledger and agree on what "already applied" means.
#
# In the Docker image it is reached as `supatype migrate`: this script lives in
# /docker-entrypoint-initdb.d/ and cannot have a sibling there, because the stock
# entrypoint executes everything in that directory. The AMI and native builds copy
# migrations/db/ wholesale, so there it is simply next door.
if [ -x "$db/apply-migrations.sh" ]; then
    run_migrate() { "$db/apply-migrations.sh" "$@"; }
elif command -v supatype > /dev/null 2>&1; then
    run_migrate() { supatype migrate "$@"; }
else
    echo "$0: cannot find apply-migrations.sh, and no supatype command on PATH" >&2
    exit 1
fi

if [ -z "${USE_DBMATE:-}" ]; then
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin <<EOSQL
$quiet
-- postgres role is pre-created during AMI build, and by an operator's initdb
SELECT NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres') AS no_postgres \\gset
\\if :no_postgres
  BEGIN;
  \\getenv pw PGPASSWORD
  CREATE ROLE postgres SUPERUSER LOGIN PASSWORD :'pw';
  ALTER DATABASE postgres OWNER TO postgres;
  COMMIT;
\\endif
EOSQL
    # run init scripts as postgres user
    for sql in "$db"/init-scripts/*.sql; do
        echo "$0: running $sql"
        psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U postgres -f "$sql"
    done
    set_admin_password
    # run migrations as super user. The demote-postgres migration demotes postgres, unless
    # it is the bootstrap superuser (an operator-made cluster), which it leaves alone.
    #
    # `bootstrap`, not `sync`: this only ever runs against a database being
    # created, so an absent ledger means nothing has run yet rather than nothing
    # was recorded. Getting that distinction wrong in the other direction is the
    # whole of #138.
    SUPATYPE_MIGRATIONS_DIR="$db/migrations" run_migrate bootstrap
else
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin <<EOSQL
$quiet
\\getenv pw PGPASSWORD
CREATE ROLE postgres SUPERUSER LOGIN PASSWORD :'pw';
ALTER DATABASE postgres OWNER TO postgres;
EOSQL
    # run init scripts as postgres user
    DBMATE_MIGRATIONS_DIR="$db/init-scripts" DATABASE_URL="postgres://postgres:$connect" dbmate --no-dump-schema migrate
    set_admin_password
    # run migrations as super user. The demote-postgres migration demotes postgres; this
    # branch created that role just above, so it is never the bootstrap superuser here.
    #
    # dbmate keeps its own schema_migrations table, so this branch does not write
    # the ledger apply-migrations.sh uses. A cluster bootstrapped this way is
    # adopted by the start-up sync the same way any pre-ledger cluster is.
    DBMATE_MIGRATIONS_DIR="$db/migrations" DATABASE_URL="postgres://supatype_admin:$connect" dbmate --no-dump-schema migrate
fi

# The other two passwords settled above. authenticator and the SQL editor are created
# passwordless by the init scripts and the editor migration, so this is what makes them
# usable at all. Set after the migrations rather than in an init script because only this
# script sees the environment, and after `20221103090837_revoke_admin.sql` has taken
# `supatype_admin` back off authenticator -- a login credential should not be handed out
# while it still inherits superuser.
export _auth_pw _editor_pw
set_role_password supatype_admin authenticator _auth_pw
if [ -n "$_editor_pw" ]; then
    set_role_password supatype_admin supatype_sql_editor _editor_pw
fi

# pg_cron, in this database.
#
# The extension is preloaded by the image and was created by nobody, so the cluster had a
# scheduler it could never use. `cron.database_name` is written from POSTGRES_DB by the entrypoint
# before any server starts, so by the time this runs the current database is the one pg_cron will
# accept.
#
# Non-fatal: a cluster with pg_cron pointed elsewhere is a degraded stack, not a broken one, and
# anything that schedules work says so when it finds no scheduler.
psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin \
    -c 'CREATE EXTENSION IF NOT EXISTS pg_cron' \
  || echo "$0: WARNING: could not create pg_cron in $PGDATABASE; scheduled jobs will not run on a timer." >&2

# Same shape for pg_keyspace, and for the same reason one step further on: the extension is what
# gives the RESP worker somewhere to persist to. Without it the worker still serves, but only from
# shared memory, and it says so once in the log and nowhere else -- a stack configured for durable
# keys would be silently ephemeral.
#
# This runs against the temporary server the entrypoint starts for initialisation; the real
# postmaster starts afterwards and finds the extension already there, which is the restart the
# worker's message asks for.
#
# Only when the keyspace was actually enabled: SUPATYPE_KEYSPACE_ENABLED is what put pg_keyspace
# into shared_preload_libraries, and creating the extension without the library loaded is an error
# rather than a no-op.
case "${SUPATYPE_KEYSPACE_ENABLED:-}" in
  1|true|TRUE|on|ON|yes|YES)
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin \
        -c 'CREATE EXTENSION IF NOT EXISTS pg_keyspace' \
      || echo "$0: WARNING: could not create pg_keyspace in $PGDATABASE; RESP will serve without persistence." >&2
    ;;
esac

# run any post migration script to update role passwords
postinit="/etc/postgresql.schema.sql"
if [ -e "$postinit" ]; then
    echo "$0: running $postinit"
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin -f "$postinit"
fi

# once done with everything, reset stats from init
psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin -c 'SELECT extensions.pg_stat_statements_reset(); SELECT pg_stat_reset();' || true
