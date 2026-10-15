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
failed=0

clean_path="/c/Windows/System32:/c/Windows"
export PGPASSWORD=smoke-test-password

# Extensions the schema engine creates (pgcrypto, unaccent, vector), the field masking extension
# the CLI creates when the archive carries it, and pgjwt.
extensions=(plpgsql pgcrypto unaccent vector supatype_mask pgjwt)

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
if pg pg_ctl -D "$data" -o "-p $port -c listen_addresses=127.0.0.1" -l "$data/server.log" \
	-w -t 60 start >"$data.ctl" 2>&1; then
	echo "ok      pg_ctl start"
	rm -f "$data.ctl"
else
	echo "FAIL    pg_ctl start"
	cat "$data.ctl" "$data/server.log" 2>/dev/null | tail -20 | sed 's/^/        /'
	rm -f "$data.ctl"
	exit 1
fi

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

step "connect with a password" psql_run "SELECT 1" || exit 1
for ext in "${extensions[@]}"; do
	step "CREATE EXTENSION $ext" psql_run "CREATE EXTENSION IF NOT EXISTS \"$ext\"" || true
done
expect_true "gen_random_uuid()" "SELECT gen_random_uuid() IS NOT NULL"
expect_true "unaccent()" "SELECT unaccent('H' || chr(244) || 'tel') = 'Hotel'"
expect_true "vector distance" "SELECT '[1,2]'::vector <-> '[1,3]'::vector = 1"
expect_true "ICU collation" "SELECT 'a' < 'b' COLLATE \"und-x-icu\""

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed for $root"
	exit 1
fi
echo "the archive at $root starts and creates every extension native dev uses"
