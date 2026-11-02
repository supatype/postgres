# shellcheck shell=bash
# lib.sh: helpers the native build scripts share (build-native.sh, build-native-extensions.sh and
# build-native-windows.sh). Sourced, not run.

die() { echo "ERROR: $*" >&2; exit 1; }

# build_pgxs <source-dir> [make-variable=value...]: build an extension through PGXS against
# $PG_CONFIG with $JOBS parallel jobs, and install it there.
#
# PGXS takes every install directory from pg_config, which is relocatable and so reports the
# staged tree: the library lands in the server's $libdir (lib/postgresql/), the .control and .sql
# beside the other extensions. `clean` first, so a library left in the source tree is never
# mistaken for a fresh build. PG_CONFIG goes to `clean` as well, because a PGXS makefile runs
# `$(PG_CONFIG) --pgxs` for every target and fails wherever no system PostgreSQL is installed.
build_pgxs() {
	local dir=$1
	shift
	make -C "${dir}" clean PG_CONFIG="${PG_CONFIG}" "$@" >/dev/null
	make -C "${dir}" -j"${JOBS}" PG_CONFIG="${PG_CONFIG}" "$@"
	make -C "${dir}" install PG_CONFIG="${PG_CONFIG}" "$@"
}
