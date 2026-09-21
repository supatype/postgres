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

readonly SUPATYPE_CRON_CONF=/etc/postgresql-custom/pg_cron.conf

# Point pg_cron at this stack's database.
#
# pg_cron runs its background worker against exactly one database per cluster, named by
# `cron.database_name`, and it refuses `CREATE EXTENSION pg_cron` in any other:
#
#   ERROR: can only create extension in database postgres
#   HINT:  Add cron.database_name = 'supatype' in postgresql.conf to use the current database.
#
# That name was hardcoded to 'postgres' in the image config while a stack's database is whatever
# POSTGRES_DB says, so on every deployment that names it anything else the extension could be
# created nowhere at all. Anything scheduled recorded its intent and nothing ever ran it, and the
# only hint was a NOTICE during schema push saying the scheduler was absent.
#
# It happens here rather than in a bootstrap script because the setting is PGC_POSTMASTER. Those
# scripts run against a temporary server that has already read its configuration, so the extension
# would still be refused on the first boot, which is the only boot that runs them.
supatype_write_cron_database() {
	local db="${POSTGRES_DB:-postgres}"
	# A single quote in a database name would end the literal and leave a file Postgres refuses to
	# start on. Doubling is the escape it expects.
	local line="cron.database_name = '${db//\'/\'\'}'"

	# The second pass after the gosu re-exec below runs as `postgres`, which cannot write here and
	# has nothing to do: the first pass already wrote it.
	if [ -f "$SUPATYPE_CRON_CONF" ] && grep -qxF "$line" "$SUPATYPE_CRON_CONF"; then
		return 0
	fi

	if ! printf '%s\n%s\n' \
		"# Written at container start. The value follows POSTGRES_DB; edits do not survive." \
		"$line" > "$SUPATYPE_CRON_CONF" 2>/dev/null
	then
		supatype_warn "could not write $SUPATYPE_CRON_CONF; pg_cron keeps whatever database it is"
		supatype_warn "already pointed at, and scheduled jobs in '$db' will not run."
	fi
}

readonly SUPATYPE_KEYSPACE_CONF=/etc/postgresql-custom/pg_keyspace.conf

# Turn pg_keyspace on from the environment, or leave the image exactly as it was.
#
# pg_keyspace registers background workers and requests shared memory at postmaster start, so it
# has to be in shared_preload_libraries -- there is no runtime toggle, and `CREATE EXTENSION`
# alone does nothing. That makes enabling it a property of the configuration a server reads before
# it starts, which is why this happens here rather than in a bootstrap script: those run against a
# temporary server that has already read its configuration.
#
# The whole preload list is restated because shared_preload_libraries is a single string, and this
# include is read *after* the one in postgresql.conf, so the last assignment wins. Order matters:
# pg_keyspace goes BEFORE supatype_mask so the mask stays outermost and pg_keyspace.require_mask
# is satisfied rather than refusing to serve.
#
# With SUPATYPE_KEYSPACE_ENABLED unset the file is left alone, and the image behaves exactly as it
# did before this existed: the extension ships built but not loaded.
supatype_write_keyspace_conf() {
	case "${SUPATYPE_KEYSPACE_ENABLED:-}" in
		1|true|TRUE|on|ON|yes|YES) ;;
		*) return 0 ;;
	esac

	# Defaults are the measured floor for a small stack rather than the extension's own, which
	# reserve ~689 MiB per worker before rings and row cache -- larger than some deployments'
	# whole memory limit. Every one of these is shared memory taken at postmaster start.
	local db="${SUPATYPE_KEYSPACE_DATABASE:-${POSTGRES_DB:-postgres}}"
	local durability="${SUPATYPE_KEYSPACE_DURABILITY:-ephemeral}"
	local overrides="${SUPATYPE_KEYSPACE_DURABILITY_OVERRIDES:-}"
	local port="${SUPATYPE_KEYSPACE_PORT:-6379}"
	local keys="${SUPATYPE_KEYSPACE_KEYS:-200000}"
	local val_bytes="${SUPATYPE_KEYSPACE_VAL_BYTES:-512}"
	local ring_mb="${SUPATYPE_KEYSPACE_RING_MB:-16}"
	local rowcache_mb="${SUPATYPE_KEYSPACE_ROWCACHE_MB:-1}"
	local require_mask="${SUPATYPE_KEYSPACE_REQUIRE_MASK:-on}"
	local preload="${SUPATYPE_SHARED_PRELOAD_LIBRARIES:-pg_stat_statements, pg_cron, pg_net, plan_filter, safeupdate, pg_keyspace, supatype_mask}"

	# A single quote would end the literal and leave a file Postgres refuses to start on.
	local db_lit="${db//\'/\'\'}"
	local overrides_lit="${overrides//\'/\'\'}"

	{
		echo "# Written at container start from SUPATYPE_KEYSPACE_*; edits do not survive."
		echo "# Unset SUPATYPE_KEYSPACE_ENABLED to go back to the shipped, inert configuration."
		echo "shared_preload_libraries = '${preload}'"
		echo "pg_keyspace.require_mask = ${require_mask}"
		echo "pg_keyspace.port = ${port}"
		echo "pg_keyspace.database = '${db_lit}'"
		echo "pg_keyspace.durability = '${durability}'"
		# An if rather than `[ … ] && echo`: this is not the last line of the block, so under a
		# future `set -e` a false test would abort the group and leave a truncated file, which is
		# a cluster that refuses to start rather than one missing a setting.
		if [ -n "$overrides" ]; then
			echo "pg_keyspace.durability_overrides = '${overrides_lit}'"
		fi
		echo "pg_keyspace.keys = ${keys}"
		echo "pg_keyspace.val_bytes = ${val_bytes}"
		echo "pg_keyspace.ring_mb = ${ring_mb}"
		# rowcache_mb is a request, not a reservation: the segment carries a fixed directory for
		# 200k entries on top, about 15.8 MiB, so even 1 costs ~17 MiB and 0 is not accepted.
		echo "pg_keyspace.rowcache_mb = ${rowcache_mb}"
	} > "$SUPATYPE_KEYSPACE_CONF" 2>/dev/null || {
		supatype_warn "could not write $SUPATYPE_KEYSPACE_CONF; pg_keyspace stays disabled."
		return 0
	}

	supatype_note "pg_keyspace enabled: RESP on :${port}, durability=${durability}${overrides:+ (${overrides})}, database=${db}"
}

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

# Create the extension while the temporary server is up, so the real one starts with it.
#
# Without it the worker serves RESP but refuses to persist, saying so once in the log and
# nowhere else:
#
#   pg_keyspace worker: the pg_keyspace extension is not installed in database '...'; run
#   CREATE EXTENSION pg_keyspace and restart to enable persistence. Serving RESP in ephemeral
#   mode until then.
#
# "and restart" is the part that matters: creating it against the running server would not take
# effect until the next boot, so a stack configured for durability would silently be ephemeral
# for its whole first life. Doing it here means the restart is the one that was going to happen
# anyway -- this temporary server stops, and the real postmaster starts with the extension
# already present.
#
# Non-fatal: a cluster serving RESP without persistence is degraded, not broken, and the worker
# already says so.
supatype_create_keyspace_extension() {
	case "${SUPATYPE_KEYSPACE_ENABLED:-}" in
		1|true|TRUE|on|ON|yes|YES) ;;
		*) return 0 ;;
	esac
	if supatype_psql 'CREATE EXTENSION IF NOT EXISTS pg_keyspace' > /dev/null 2>&1; then
		supatype_note "pg_keyspace extension present; persistence is available on the next start."
	else
		supatype_warn "could not create the pg_keyspace extension; RESP will serve without persistence."
	fi
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
		supatype_create_keyspace_extension
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

	# Before anything starts a server, including the temporary one the migration path below uses:
	# cron.database_name is read at postmaster start and never re-read, and pg_keyspace cannot be
	# loaded at all without being in shared_preload_libraries first.
	supatype_write_cron_database
	supatype_write_keyspace_conf

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
