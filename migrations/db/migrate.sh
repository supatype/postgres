#!/bin/sh
set -eu

#######################################
# Used by both ami and docker builds to initialise database schema.
# Env vars:
#   POSTGRES_DB        defaults to postgres
#   POSTGRES_HOST      if set, psql uses TCP to this host; if unset, use Unix socket
#                      (required for official postgres image first-boot init)
#   POSTGRES_PORT      defaults to 5432
#   POSTGRES_PASSWORD  defaults to ""
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

db=$( cd -- "$( dirname -- "$0" )" > /dev/null 2>&1 && pwd )
if [ -z "${USE_DBMATE:-}" ]; then
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin <<EOSQL
do \$\$
begin
  -- postgres role is pre-created during AMI build
  if not exists (select from pg_roles where rolname = 'postgres') then
    create role postgres superuser login password '$PGPASSWORD';
    alter database postgres owner to postgres;
  end if;
end \$\$
EOSQL
    # run init scripts as postgres user
    for sql in "$db"/init-scripts/*.sql; do
        echo "$0: running $sql"
        psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U postgres -f "$sql"
    done
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U postgres -c "ALTER USER supatype_admin WITH PASSWORD '$PGPASSWORD'"
    # run migrations as super user - postgres user demoted in post-setup
    for sql in "$db"/migrations/*.sql; do
        echo "$0: running $sql"
        psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin -f "$sql"
    done
else
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin <<EOSQL
  create role postgres superuser login password '$PGPASSWORD';
  alter database postgres owner to postgres;
EOSQL
    # run init scripts as postgres user
    DBMATE_MIGRATIONS_DIR="$db/init-scripts" DATABASE_URL="postgres://postgres:$connect" dbmate --no-dump-schema migrate
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U postgres -c "ALTER USER supatype_admin WITH PASSWORD '$PGPASSWORD'"
    # run migrations as super user - postgres user demoted in post-setup
    DBMATE_MIGRATIONS_DIR="$db/migrations" DATABASE_URL="postgres://supatype_admin:$connect" dbmate --no-dump-schema migrate
fi

# PostgREST connects as `authenticator`, so it needs a password: pg_hba uses
# scram-sha-256 for every non-loopback host, which is every containerised path.
#
# Created passwordless by the init scripts, so this is what makes it usable at all. Set after
# the migrations rather than in an init script because only this script sees the environment,
# and after `20221103090837_revoke_admin.sql` has taken `supatype_admin` back off the role --
# a login credential should not be handed out while it still inherits superuser.
#
# `AUTHENTICATOR_PASSWORD`, not `POSTGRES_PASSWORD`: the latter is the operator's, for direct
# SQL access, and rotating it must not take the REST API down. Falls back to it only so a
# hand-rolled stack that never set the new variable still boots -- with a warning, because a
# shared credential is not the intended posture.
_auth_pw="${AUTHENTICATOR_PASSWORD:-}"
if [ -z "$_auth_pw" ]; then
    echo "$0: WARNING: AUTHENTICATOR_PASSWORD is unset; falling back to POSTGRES_PASSWORD." >&2
    echo "$0:          Set AUTHENTICATOR_PASSWORD so rotating the database password cannot" >&2
    echo "$0:          break PostgREST." >&2
    _auth_pw="$PGPASSWORD"
fi
psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin \
    -c "ALTER USER authenticator WITH PASSWORD '$_auth_pw'"

# run any post migration script to update role passwords
postinit="/etc/postgresql.schema.sql"
if [ -e "$postinit" ]; then
    echo "$0: running $postinit"
    psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin -f "$postinit"
fi

# once done with everything, reset stats from init
psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U supatype_admin -c 'SELECT extensions.pg_stat_statements_reset(); SELECT pg_stat_reset();' || true
