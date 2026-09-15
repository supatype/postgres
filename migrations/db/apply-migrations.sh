#!/bin/sh
set -eu

#######################################
# Applies the migrations this cluster has not already run, and records what it
# ran so that the next start can tell the difference.
#
# Until this existed the image kept no migration state at all: migrate.sh ran
# every file in migrations/ unconditionally, which is only safe because the
# stock postgres entrypoint runs /docker-entrypoint-initdb.d/* exactly once,
# against a data directory it just created. The other half of that arrangement
# is the bug this fixes (#138): start the image over a volume that already holds
# a cluster and the entrypoint skips init entirely, so nothing applies the
# migrations added since that volume was created -- silently, with no error and
# no way to ask what is missing.
#
# The ledger is keyed by filename rather than by a parsed version number: these
# migrations are dbmate-shaped (`-- migrate:up`) but not all of them are
# dbmate-parseable (`00-stat-extension.sql` has no `<version>_<name>` split), so
# the filename is the only identifier every file actually has.
#
# Env vars:
#   SUPATYPE_MIGRATIONS_DIR   directory of *.sql migrations to apply
#                             (default /docker-entrypoint-initdb.d/migrations)
#   SUPATYPE_MIGRATION_ROLE   role to apply them as (default supatype_admin)
#   POSTGRES_DB               database to apply them to (defaults to
#                             POSTGRES_USER, then postgres)
#   POSTGRES_HOST             if set, psql uses TCP to this host; if unset, the
#                             Unix socket
#   POSTGRES_PORT             defaults to 5432
#   POSTGRES_PASSWORD         defaults to ""
#
# Commands:
#   bootstrap   apply every migration and record it. For a database being
#               created: an empty ledger means nothing has run yet.
#   sync        apply only what the ledger does not record. If the ledger is
#               absent on an existing database, create it and mark every
#               shipped migration as applied *without running it* -- see the
#               warning in backfill_ledger() for why, and what it costs.
#   status      print the ledger, newest first.
#   replay <file>...  re-run named migrations and re-record them. The operator
#               escape hatch for a cluster whose ledger was backfilled.
#
# Exit code:
#   0 if every migration that needed to run did, non-zero on error.
#######################################

# Deterministic ordering for the glob below, and for psql's own output, whatever
# the host locale happens to be.
LC_ALL=C
export LC_ALL

PROG=$(basename "$0")

MIGRATIONS_DIR="${SUPATYPE_MIGRATIONS_DIR:-/docker-entrypoint-initdb.d/migrations}"
MIGRATION_ROLE="${SUPATYPE_MIGRATION_ROLE:-supatype_admin}"

# Same rule the stock entrypoint uses (POSTGRES_DB defaults to POSTGRES_USER), so
# that `docker exec <container> supatype-migrate status` lands in the database the
# migrations were applied to. POSTGRES_DB is only ever set inside the entrypoint's
# own process; a later `docker exec` sees just the image's POSTGRES_USER.
export PGDATABASE="${POSTGRES_DB:-${POSTGRES_USER:-postgres}}"
export PGPORT="${POSTGRES_PORT:-5432}"
if [ -n "${POSTGRES_HOST:-}" ]; then
	export PGHOST="$POSTGRES_HOST"
fi
export PGPASSWORD="${POSTGRES_PASSWORD:-}"

# Temporary files are cleaned up on any exit, not just the happy one: a failing
# migration aborts the script through `set -e`, part-way through a run.
TMP_FILES=''
cleanup_tmp() {
	[ -n "$TMP_FILES" ] && rm -f $TMP_FILES
	return 0
}
trap cleanup_tmp EXIT HUP INT TERM

# Sets NEW_TMP rather than echoing the path: called as `x=$(new_tmp)` the
# registration would happen in the command substitution's subshell and never
# reach TMP_FILES here, leaving the file behind.
new_tmp() {
	NEW_TMP=$(mktemp "${TMPDIR:-/tmp}/supatype-migrate.XXXXXX")
	TMP_FILES="$TMP_FILES $NEW_TMP"
}

psql_admin() {
	psql -v ON_ERROR_STOP=1 --no-password --no-psqlrc -U "$MIGRATION_ROLE" "$@"
}

# Scalar query, unaligned and untitled.
psql_value() {
	psql_admin -tAc "$1"
}

# Single-quote a value for interpolation into SQL. The filenames are ours, but
# the ledger is the one place a stray quote would turn a bookkeeping insert into
# something else.
sql_lit() {
	printf '%s' "$1" | sed "s/'/''/g"
}

checksum() {
	if command -v sha256sum > /dev/null 2>&1; then
		sha256sum "$1" | cut -d' ' -f1
	else
		echo ''
	fi
}

ledger_exists() {
	[ "$(psql_value "SELECT to_regclass('supatype_migrations.applied') IS NOT NULL")" = 't' ]
}

create_ledger() {
	psql_admin --single-transaction > /dev/null <<-'SQL'
		CREATE SCHEMA IF NOT EXISTS supatype_migrations;
		REVOKE ALL ON SCHEMA supatype_migrations FROM PUBLIC;

		CREATE TABLE IF NOT EXISTS supatype_migrations.applied (
		    filename    text PRIMARY KEY,
		    checksum    text,
		    applied_at  timestamptz NOT NULL DEFAULT now(),
		    backfilled  boolean NOT NULL DEFAULT false
		);
		REVOKE ALL ON supatype_migrations.applied FROM PUBLIC;

		COMMENT ON SCHEMA supatype_migrations IS
		    'Bookkeeping for the image''s own migrations. Not user schema.';
		COMMENT ON TABLE supatype_migrations.applied IS
		    'One row per migration this database has run. Written by supatype-migrate.';
		COMMENT ON COLUMN supatype_migrations.applied.checksum IS
		    'sha256 of the file as applied; a mismatch means the migration was edited after the fact.';
		COMMENT ON COLUMN supatype_migrations.applied.backfilled IS
		    'true when the row was written to adopt a pre-existing cluster, i.e. the migration was assumed applied rather than run.';
	SQL
}

# Adopting a cluster that predates the ledger.
#
# There is no way to ask an existing database which of these files it has run, so
# the choice is to assume all of them (and leave a cluster that is genuinely
# behind still behind) or to replay all of them (and hope every one is idempotent
# against a populated database). This takes the first: it never replays DDL
# against live data, and it is honest about what it did -- every row it writes is
# flagged `backfilled`, so `status` shows exactly which migrations were assumed
# rather than observed, and `replay` can run any of them on demand.
backfill_ledger() {
	new_tmp; tmpsql=$NEW_TMP
	count=0

	{
		echo "INSERT INTO supatype_migrations.applied (filename, checksum, backfilled) VALUES"
		sep=' '
		for f in "$MIGRATIONS_DIR"/*.sql; do
			[ -e "$f" ] || continue
			printf "%s ('%s', '%s', true)\n" \
				"$sep" "$(sql_lit "$(basename "$f")")" "$(sql_lit "$(checksum "$f")")"
			sep=','
			count=$((count + 1))
		done
		echo "ON CONFLICT (filename) DO NOTHING;"
	} > "$tmpsql"

	if [ "$count" -eq 0 ]; then
		return 0
	fi

	psql_admin --single-transaction -f "$tmpsql" > /dev/null

	echo "$PROG: ------------------------------------------------------------------" >&2
	echo "$PROG: WARNING: '$PGDATABASE' had no migration ledger, so all $count shipped" >&2
	echo "$PROG:          migrations have been marked applied WITHOUT being run." >&2
	echo "$PROG:" >&2
	echo "$PROG:          Correct if this cluster was already up to date. If it was" >&2
	echo "$PROG:          BEHIND, it stays behind: the migrations it never ran are now" >&2
	echo "$PROG:          recorded as applied and will not run on their own. Nothing on" >&2
	echo "$PROG:          disk distinguishes the two cases, so check for yourself." >&2
	echo "$PROG:" >&2
	echo "$PROG:          This affects THIS START ONLY. New migrations from here on are" >&2
	echo "$PROG:          applied normally." >&2
	echo "$PROG:" >&2
	echo "$PROG:          Most important to verify -- if pg_guard is missing here, it is" >&2
	echo "$PROG:          not loading for PostgREST sessions and privilege enforcement is" >&2
	echo "$PROG:          off across the API surface:" >&2
	echo "$PROG:" >&2
	echo "$PROG:            SELECT rolconfig FROM pg_roles WHERE rolname = 'authenticator';" >&2
	echo "$PROG:" >&2
	echo "$PROG:          Then: supatype-migrate status   (what was assumed)" >&2
	echo "$PROG:                supatype-migrate replay <filename>" >&2
	echo "$PROG:          See the Migrations and upgrades section of the README for the" >&2
	echo "$PROG:          full check." >&2
	echo "$PROG: ------------------------------------------------------------------" >&2
}

# Apply one migration and record it in the same transaction, so a ledger row can
# only exist if the migration it names committed. psql runs -f and -c in the
# order given, and --single-transaction wraps the pair in one BEGIN/COMMIT.
#
# None of the shipped migrations contain CREATE INDEX CONCURRENTLY, ALTER SYSTEM,
# VACUUM or an explicit BEGIN/COMMIT, so all of them are transaction-safe; a
# future one that is not would need its own handling here.
apply_one() {
	file=$1
	base=$(basename "$file")

	# Full path, matching the "running <file>" lines migrate.sh still emits for
	# the init scripts, so anything parsing the boot log sees one shape.
	echo "$PROG: applying $file"
	psql_admin --single-transaction -f "$file" \
		-c "INSERT INTO supatype_migrations.applied (filename, checksum, backfilled)
		    VALUES ('$(sql_lit "$base")', '$(sql_lit "$(checksum "$file")")', false)
		    ON CONFLICT (filename) DO UPDATE
		      SET checksum   = EXCLUDED.checksum,
		          applied_at = now(),
		          backfilled = false" > /dev/null
}

# Applies everything the ledger does not already record. Shared by bootstrap and
# sync; they differ only in what an absent ledger means, which is settled before
# this runs.
apply_pending() {
	new_tmp; applied=$NEW_TMP
	psql_value "SELECT filename FROM supatype_migrations.applied" > "$applied"

	pending=0
	for f in "$MIGRATIONS_DIR"/*.sql; do
		[ -e "$f" ] || continue
		if grep -Fxq "$(basename "$f")" "$applied"; then
			continue
		fi
		apply_one "$f"
		pending=$((pending + 1))
	done

	if [ "$pending" -eq 0 ]; then
		echo "$PROG: no pending migrations."
	else
		echo "$PROG: applied $pending migration(s)."
	fi
}

cmd_status() {
	if ! ledger_exists; then
		echo "$PROG: no migration ledger in '$PGDATABASE'." >&2
		return 1
	fi
	psql_admin -c "SELECT filename,
	                      applied_at,
	                      backfilled AS assumed_not_run
	                 FROM supatype_migrations.applied
	                ORDER BY filename DESC"
}

cmd_replay() {
	if [ "$#" -eq 0 ]; then
		echo "$PROG: replay needs at least one migration filename." >&2
		return 2
	fi
	if ! ledger_exists; then
		create_ledger
	fi
	for name in "$@"; do
		file="$MIGRATIONS_DIR/$(basename "$name")"
		if [ ! -e "$file" ]; then
			echo "$PROG: no such migration: $file" >&2
			return 1
		fi
		apply_one "$file"
	done
}

main() {
	cmd="${1:-sync}"
	[ "$#" -gt 0 ] && shift || true

	if [ ! -d "$MIGRATIONS_DIR" ]; then
		echo "$PROG: migrations directory not found: $MIGRATIONS_DIR" >&2
		return 1
	fi

	case "$cmd" in
		bootstrap)
			# A database being created: an absent ledger means nothing has run.
			ledger_exists || create_ledger
			apply_pending
			;;
		sync)
			# A database that already exists: an absent ledger means no record,
			# which is not the same as nothing having run.
			if ! ledger_exists; then
				create_ledger
				backfill_ledger
			fi
			apply_pending
			;;
		status)
			cmd_status
			;;
		replay)
			cmd_replay "$@"
			;;
		*)
			echo "$PROG: unknown command '$cmd'" >&2
			echo "usage: $PROG {bootstrap|sync|status|replay <filename>...}" >&2
			return 2
			;;
	esac
}

main "$@"
