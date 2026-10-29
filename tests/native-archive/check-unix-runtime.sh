#!/usr/bin/env bash
# Starts a staged or unpacked Linux/macOS archive the way native `supatype dev` does, and checks
# everything native dev relies on: the extensions it creates, the libraries it preloads, and
# wal2json logical decoding for realtime.
#
# The server is started with every library the image preloads (config/postgresql.conf), plus
# pg_keyspace with the CLI's dev settings, which is what native dev has to start it with for each
# of them to work. A library left outside the server's $libdir is not a missing
# feature there: the postmaster refuses to start at all, so a published archive with every
# extension library in lib/ instead of lib/postgresql/ took native dev down with it.
#
# Must not run as root (initdb refuses). Usage: check-unix-runtime.sh <archive-dir> [port]
set -euo pipefail

root=$(cd "$1" && pwd)
port=${2:-55432}
keyspace_port=$((port + 1))
data=$(mktemp -d)

# shellcheck source=tests/native-archive/checks.sh
source "$(dirname "${BASH_SOURCE[0]}")/checks.sh"

# The CLI puts the archive's lib/ on the loader path, for libpq and the other shared libraries the
# programs link; the server's own modules are found through $libdir, not through this.
export LD_LIBRARY_PATH="$root/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
export DYLD_LIBRARY_PATH="$root/lib${DYLD_LIBRARY_PATH:+:$DYLD_LIBRARY_PATH}"

extensions=(plpgsql pgcrypto unaccent vector supatype_mask pgjwt pg_stat_statements pg_cron pg_net pg_keyspace)
# PostGIS is in the Linux archive only; the macOS one does not bundle GEOS and PROJ yet.
if [ "$(uname -s)" = Linux ]; then
	extensions+=(postgis postgis_topology)
fi
# pg_keyspace first and supatype_mask last, the order the CLI uses, so the mask stays outermost.
preload=pg_keyspace,pg_stat_statements,pg_cron,pg_net,plan_filter,safeupdate,supatype_mask

# pg <program> [args]: one of the archive's programs.
pg() {
	local program=$1
	shift
	"$root/bin/$program" "$@"
}

stop() {
	pg pg_ctl -D "$data" -m immediate stop >/dev/null 2>&1 || true
	rm -rf "$data"
}
trap stop EXIT

step "initdb" pg initdb -D "$data" -U postgres --auth=trust --encoding=UTF8 --no-locale || exit 1

# pg_keyspace's settings are the ones pgServerOptions in the CLI's postgres-ctl.ts passes.
options="-p $port -c listen_addresses=127.0.0.1 $(dev_server_options "$preload")"
options+=" -c pg_keyspace.port=$keyspace_port -c pg_keyspace.durability=ephemeral"
options+=" -c pg_keyspace.keys=50000 -c pg_keyspace.ring_mb=8 -c pg_keyspace.rowcache_mb=1"

# Not through `step`: the server inherits pg_ctl's output handles, so capturing them would wait for
# the server to exit.
if pg pg_ctl -D "$data" -o "$options" -l "$data/server.log" -w -t 60 start >"$data.ctl" 2>&1; then
	echo "ok      pg_ctl start with $preload preloaded"
	rm -f "$data.ctl"
else
	echo "FAIL    pg_ctl start with $preload preloaded"
	cat "$data.ctl" "$data/server.log" 2>/dev/null | tail -20 | sed 's/^/        /'
	rm -f "$data.ctl"
	exit 1
fi

run_sql_checks "${extensions[@]}" -- plan_filter safeupdate || exit 1

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed for $root"
	exit 1
fi
echo "the archive at $root starts, creates every extension native dev uses and decodes with wal2json"
