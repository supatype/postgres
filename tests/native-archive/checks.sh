# shellcheck shell=bash
# checks.sh: the SQL checks every native archive has to pass, sourced by the per-platform runtime
# scripts once they have a server running.
#
# The caller defines `pg <program> [args]` (one of the archive's programs, run the way that
# platform needs) and sets `port`, then calls `run_sql_checks`. Each check prints `ok` or `FAIL`;
# `failed` counts the failures, and the caller decides what to do with it.

failed=0

step() {
	local label=$1
	shift
	if out=$("$@" 2>&1); then
		echo "ok      $label"
	else
		echo "FAIL    $label (exit $?)"
		echo "$out" | tail -5 | sed 's/^/        /'
		failed=$((failed + 1))
		return 1
	fi
}

# shellcheck disable=SC2154 # port is set by the script that sources this one
psql_run() {
	pg psql -h 127.0.0.1 -p "$port" -U postgres -d postgres -v ON_ERROR_STOP=1 -tAc "$1"
}

# expect_true <label> <sql>: the query must answer `t`.
expect_true() {
	local got
	if got=$(psql_run "$2" 2>&1) && [ "$got" = "t" ]; then
		echo "ok      $1"
	else
		echo "FAIL    $1: $(echo "$got" | tail -1)"
		failed=$((failed + 1))
	fi
}

# dev_server_options <shared_preload_libraries>: the server settings every bundled library needs,
# which native dev has to start the server with (config/postgresql.conf gives the image the same).
# wal_level=logical for realtime's wal2json slot. pg_cron schedules from one database, and with
# background workers rather than libpq connections it needs no password or TCP auth to run jobs.
# The worker pool has room for pg_cron, pg_net, pg_keyspace and parallel query.
dev_server_options() {
	echo "-c wal_level=logical -c shared_preload_libraries=$1 -c cron.database_name=postgres" \
		"-c cron.use_background_workers=on -c max_worker_processes=16"
}

# Realtime streams database changes through a wal2json logical replication slot. wal2json is an
# output plugin, not an extension, so nothing else here would notice it missing: the archive
# would start, every extension would create, and `supatype dev` would report "database changes
# are off". The server must be running with wal_level=logical.
check_wal2json() {
	local slot=native_archive_wal2json
	expect_true "wal_level = logical" "SELECT current_setting('wal_level') = 'logical'"
	step "create a wal2json replication slot" \
		psql_run "SELECT pg_create_logical_replication_slot('$slot', 'wal2json')" || return 0
	step "write a row" psql_run "CREATE TABLE wal2json_probe (id int PRIMARY KEY, v text); INSERT INTO wal2json_probe VALUES (1, 'streamed')" || true
	expect_true "wal2json decodes the insert" \
		"SELECT count(*) = 1 FROM pg_logical_slot_get_changes('$slot', NULL, NULL) WHERE data LIKE '%\"wal2json_probe\"%' AND data LIKE '%\"streamed\"%'"
	step "drop the slot" psql_run "SELECT pg_drop_replication_slot('$slot')" || true
}

# expect_error <label> <sql> <pattern>: the query must fail with an error matching the pattern.
expect_error() {
	local got
	if got=$(psql_run "$2" 2>&1); then
		echo "FAIL    $1: succeeded"
		failed=$((failed + 1))
	elif echo "$got" | grep -q "$3"; then
		echo "ok      $1"
	else
		echo "FAIL    $1: $(echo "$got" | tail -1)"
		failed=$((failed + 1))
	fi
}

# probe <extension-or-library>: show that it works, not only that it loads.
probe() {
	case "$1" in
	pgcrypto) expect_true "pgcrypto digest()" "SELECT encode(digest('a', 'sha256'), 'hex') LIKE 'ca978112%'" ;;
	unaccent) expect_true "unaccent()" "SELECT unaccent('H' || chr(244) || 'tel') = 'Hotel'" ;;
	vector) expect_true "vector distance" "SELECT '[1,2]'::vector <-> '[1,3]'::vector = 1" ;;
	pg_cron)
		expect_true "pg_cron launcher running" "SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type = 'pg_cron launcher'"
		expect_true "pg_cron schedules a job" "SELECT cron.schedule('native_archive_probe', '* * * * *', 'SELECT 1') > 0"
		;;
	pg_net)
		expect_true "pg_net worker running" "SELECT count(*) = 1 FROM pg_stat_activity WHERE backend_type LIKE 'pg_net%'"
		expect_true "pg_net queues a request" "SELECT net.http_get('http://127.0.0.1:9') > 0"
		;;
	postgis)
		expect_true "PostGIS geometry" "SELECT ST_AsText(ST_Point(1, 2)) = 'POINT(1 2)'"
		# Reads PROJ's database, so this fails when share/proj is missing or cannot be found.
		expect_true "PostGIS ST_Transform (PROJ data)" \
			"SELECT round(ST_X(ST_Transform(ST_SetSRID(ST_Point(1, 0), 4326), 3857))) = 111319"
		;;
	safeupdate)
		step "write a row for pg_safeupdate" psql_run "CREATE TABLE safeupdate_probe (id int); INSERT INTO safeupdate_probe VALUES (1)" || true
		expect_error "pg_safeupdate refuses DELETE without WHERE" "DELETE FROM safeupdate_probe" "requires a WHERE clause"
		;;
	plan_filter) expect_true "pg_plan_filter loaded" "SELECT current_setting('plan_filter.statement_cost_limit') IS NOT NULL" ;;
	esac
}

# run_sql_checks <extension>... [-- <preloaded-library>...]: create each extension and probe it,
# probe each library that is preloaded but has no extension, then check the rest of what native
# dev relies on.
run_sql_checks() {
	local ext libraries=()
	step "connect" psql_run "SELECT 1" || return 1
	while [ $# -gt 0 ] && [ "$1" != "--" ]; do
		step "CREATE EXTENSION $1" psql_run "CREATE EXTENSION IF NOT EXISTS \"$1\"" && probe "$1"
		shift
	done
	[ $# -eq 0 ] || { shift; libraries=("$@"); }
	for ext in "${libraries[@]}"; do
		probe "$ext"
	done
	expect_true "gen_random_uuid()" "SELECT gen_random_uuid() IS NOT NULL"
	expect_true "ICU collation" "SELECT 'a' < 'b' COLLATE \"und-x-icu\""
	check_wal2json
}
