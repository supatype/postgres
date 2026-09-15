#!/usr/bin/env bash
#
# The image's ENTRYPOINT. Starting Postgres is still entirely the stock
# entrypoint's job; this wrapper exists for the one thing the stock entrypoint
# deliberately does not do.
#
# `docker_process_init_files /docker-entrypoint-initdb.d/*` runs only when the
# data directory is empty. The bootstrap script is installed there, so on any cluster that
# already exists it never runs, and every migration added since that volume was
# created is silently absent -- including the one that keeps pg_guard in
# `authenticator`'s session_preload_libraries (#138).
#
# So before handing over: if the data directory already holds a cluster, start
# the same socket-only temporary server the stock entrypoint uses for first-boot
# init, and apply whatever the migration ledger does not record. Doing it here
# rather than in the background after start-up means a failed migration stops the
# container, instead of letting clients connect to a half-migrated database.

# Deliberately not -u: the stock entrypoint's own functions are not unset-safe
# yet (there is a TODO to that effect at the top of it), and we call several.
set -Eeo pipefail

# Sourcing gives us docker_setup_env, docker_temp_server_start/stop and friends
# without running the entrypoint: the `_is_sourced` guard at the foot of that
# file is there for exactly this.
# shellcheck source=/dev/null
source /usr/local/bin/docker-entrypoint.sh

readonly SUPATYPE_SELF="${BASH_SOURCE[0]}"
readonly SUPATYPE_MIGRATE_HBA=/etc/postgresql/pg_hba.migrate.conf

# Resolved here rather than left to the runner's own default so that the
# reachability probes below and the migrations themselves are certain to be
# talking about the same role.
SUPATYPE_MIGRATION_ROLE="${SUPATYPE_MIGRATION_ROLE:-supatype_admin}"
export SUPATYPE_MIGRATION_ROLE

supatype_note() { echo "supatype-entrypoint: $*"; }
supatype_warn() { echo "supatype-entrypoint: $*" >&2; }

# Named explicitly, by the same rule the runner uses. Without -d, psql would
# connect to a database named after the role: identical in the default image
# (POSTGRES_DB defaults to POSTGRES_USER) and wrong the moment an operator sets
# POSTGRES_DB, which would make both probes below fail and silently skip every
# migration.
supatype_psql() {
	psql -U "$SUPATYPE_MIGRATION_ROLE" \
		-d "${POSTGRES_DB:-${POSTGRES_USER:-postgres}}" \
		--no-password --no-psqlrc -tAc "$1"
}

# True when we can reach the cluster as the role that owns the migrations. A
# volume from some other image will not have that role, and that is not a
# failure -- it is simply not a database this image has migrations for.
supatype_can_connect() {
	supatype_psql 'SELECT 1' > /dev/null 2>&1
}

# True when this data directory is a standby. Replaying the primary's WAL is how
# a standby gets its schema; migrating one directly is both impossible (it is
# read-only) and wrong.
supatype_in_recovery() {
	[ "$(supatype_psql 'SELECT pg_is_in_recovery()' 2>/dev/null)" = 't' ]
}

supatype_migrate_existing_cluster() {
	if [ -n "${SUPATYPE_SKIP_MIGRATIONS:-}" ]; then
		supatype_warn "SUPATYPE_SKIP_MIGRATIONS is set; not checking for unapplied migrations."
		return 0
	fi

	supatype_note "existing data directory; checking for unapplied migrations."

	# The temporary server listens on "${PGPORT:-5432}" and nothing else, so pin
	# the runner's port to the same value: POSTGRES_PORT is what it reads.
	local port="${PGPORT:-5432}"

	# Socket-only (docker_temp_server_start forces listen_addresses='') and with
	# an hba file that allows nothing but peer-mapped local connections, so
	# nothing outside this container's filesystem can reach the server during
	# the migration window.
	#
	# The shipped pg_hba.conf requires scram-sha-256 for supatype_admin even over
	# the socket, and a restart carries no POSTGRES_PASSWORD -- upstream requires
	# it only to initialise. Peer is what makes this work without one.
	docker_temp_server_start "$@" -c hba_file="$SUPATYPE_MIGRATE_HBA"

	local rc=0
	if ! supatype_can_connect; then
		supatype_warn "cannot connect as '$SUPATYPE_MIGRATION_ROLE'; leaving this cluster alone."
	elif supatype_in_recovery; then
		supatype_note "cluster is in recovery (standby); its schema comes from the primary."
	else
		# The socket, always: a PGHOST inherited from the environment would send
		# the runner somewhere that is not the server we just started.
		(
			unset PGHOST POSTGRES_HOST
			export POSTGRES_PORT="$port"
			supatype migrate sync
		) || rc=$?
	fi

	docker_temp_server_stop

	if [ "$rc" -ne 0 ]; then
		supatype_warn "migration failed (exit $rc); refusing to start."
		supatype_warn "fix the migration, or set SUPATYPE_SKIP_MIGRATIONS=1 to start anyway."
		exit "$rc"
	fi
}

# Mirror _main's argument handling so that we and it agree on what counts as
# "starting the server"; anything else (psql, a shell, --help) we just pass on.
if [ -n "${1-}" ] && [ "${1:0:1}" = '-' ]; then
	set -- postgres "$@"
fi

if [ "${1-}" = 'postgres' ] && ! _pg_want_help "$@"; then
	docker_setup_env
	docker_create_db_directories

	if [ "$(id -u)" = '0' ]; then
		# Same re-exec as the stock entrypoint: the temporary server and psql
		# below have to run as the user that owns the data directory.
		exec gosu postgres "$SUPATYPE_SELF" "$@"
	fi

	if [ -n "${DATABASE_ALREADY_EXISTS:-}" ]; then
		supatype_migrate_existing_cluster "$@"
	fi
fi

exec /usr/local/bin/docker-entrypoint.sh "$@"
