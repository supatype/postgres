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
# The image's preload list with pg_keyspace in front, the order the CLI uses: pg_keyspace first and
# supatype_mask last, so the mask stays outermost.
# shellcheck disable=SC2119 # every library the image preloads, none dropped
image=$(image_preload)
preload="pg_keyspace,${image}"

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
start_server "$preload" -c pg_keyspace.port="$keyspace_port" -c pg_keyspace.durability=ephemeral \
	-c pg_keyspace.keys=50000 -c pg_keyspace.ring_mb=8 -c pg_keyspace.rowcache_mb=1 || exit 1

run_sql_checks "${extensions[@]}" -- plan_filter safeupdate || exit 1

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed for $root"
	exit 1
fi
echo "the archive at $root starts, creates every extension native dev uses and decodes with wal2json"
