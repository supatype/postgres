#!/usr/bin/env bash
# Starts an unpacked Windows archive on a machine where it is the only Postgres on PATH, and
# creates every extension native `supatype dev` relies on.
#
# PATH holds only the Windows system directories, so a DLL the archive forgot cannot be found in a
# toolchain directory (MSYS2, Git for Windows) and hide the gap. That is how an archive missing
# ICU, libxml2 and every contrib module was published: on the build machine they were all there.
#
# Usage: check-windows-runtime.sh <unpacked-archive-dir> [port]
set -euo pipefail

root=$(cd "$1" && pwd)
port=${2:-55432}
data=$(mktemp -d)

# shellcheck source=tests/native-archive/checks.sh
source "$(dirname "${BASH_SOURCE[0]}")/checks.sh"

clean_path="/c/Windows/System32:/c/Windows"
export PGPASSWORD=smoke-test-password

# Extensions the schema engine creates (pgcrypto, unaccent, vector), the field masking extension
# the CLI creates when the archive carries it, and the rest the image carries that build on
# Windows. pg_net does not (it has no Windows event loop) and pg_keyspace does not (no pgrx target).
extensions=(plpgsql pgcrypto unaccent vector supatype_mask pgjwt pg_stat_statements pg_cron postgis postgis_topology)
# Preloaded the way config/postgresql.conf preloads them for the image.
preload=pg_stat_statements,pg_cron,plan_filter,safeupdate,supatype_mask

# pg <program> [args]: one of the archive's programs, with only Windows on PATH.
pg() {
	local program=$1
	shift
	PATH="$clean_path" "$root/bin/$program.exe" "$@"
}

stop() {
	pg pg_ctl -D "$data" -m immediate stop >/dev/null 2>&1 || true
	rm -rf "$data"
}
trap stop EXIT

for program in postgres initdb pg_ctl psql; do
	step "$program --version" pg "$program" --version || true
done

pwfile="$data.pw"
printf '%s\n' "$PGPASSWORD" >"$pwfile"
step "initdb" pg initdb -D "$data" -U postgres --pwfile="$pwfile" \
	--auth=scram-sha-256 --encoding=UTF8 --no-locale || { rm -f "$pwfile"; exit 1; }
rm -f "$pwfile"

# Not through `step`: the server inherits pg_ctl's output handles, so capturing them would wait for
# the server to exit. pg_ctl's own output goes to a file instead.
if pg pg_ctl -D "$data" -o "-p $port -c listen_addresses=127.0.0.1 $(dev_server_options "$preload")" \
	-l "$data/server.log" -w -t 60 start >"$data.ctl" 2>&1; then
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
