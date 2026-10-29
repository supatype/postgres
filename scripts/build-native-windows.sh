#!/usr/bin/env bash
# build-native-windows.sh: build the Windows (MINGW64) native archive's contents from source.
#
# Run inside an MSYS2 MINGW64 shell. Used by the build-windows job in
# .github/workflows/native-archives.yml and by scripts/build-native-windows.ps1, so CI and a local
# build produce the same tree.
#
# PostgreSQL is built from the same pinned source tarball as the Linux and macOS archives, against
# the libraries MSYS2 currently ships. The archive used to be MSYS2's prebuilt PostgreSQL package
# instead, pinned by URL, with its libraries installed from the rolling MSYS2 repository. The two
# drifted apart: the package needed ICU 75, libxml2-2.dll and a libintl that exports DllMain, the
# repository had moved on to ICU 78, libxml2-16.dll and a newer libintl, and the published archive
# could not start (0xC0000135). Then MSYS2 deleted the pinned package, so the build could not run
# at all. Built from source, PostgreSQL links against the libraries that are installed, and the
# DLLs bundled below are worked out from the binaries, so the two cannot drift.
#
# Usage: build-native-windows.sh <staging-dir>
#
# Environment overrides:
#   PG_VERSION        PostgreSQL version (default 17.2)
#   PG_SHA256         SHA-256 of the source tarball
#   BUILD_DIR         scratch space (default: a new temporary directory)
#   JOBS              parallel make jobs (default: nproc)
# The upstream extension pins live in scripts/build-native-extensions.sh, and its own overrides
# (PGVECTOR_VERSION and the rest) pass through.
set -euo pipefail

PG_VERSION="${PG_VERSION:-17.2}"
PG_SHA256="${PG_SHA256:-51d8cdd6a5220fa8c0a3b12f2d0eeb50fcf5e0bdb7b37904a9cdff5cf1e61c36}"
JOBS="${JOBS:-$(nproc)}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

info() { echo "[build-native-windows] $*"; }
# shellcheck source=scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"

[ $# -eq 1 ] || { echo "Usage: $(basename "$0") <staging-dir>" >&2; exit 2; }
[ "${MSYSTEM:-}" = "MINGW64" ] || die "run this in an MSYS2 MINGW64 shell."

mkdir -p "$1"
STAGE="$(cd "$1" && pwd)"
[ -z "$(ls -A "${STAGE}")" ] || die "${STAGE} is not empty."
BUILD_DIR="${BUILD_DIR:-$(mktemp -d)}"
mkdir -p "${BUILD_DIR}"

# PostgreSQL is installed here, then copied to the staging directory. It drops the `postgresql`
# level from lib/ and share/ when the prefix already names "postgres" or "pgsql", which a CI
# checkout path does, so the prefix names neither and the layout matches the other archives.
PREFIX="${BUILD_DIR}/supatype-pg"
case "${PREFIX}" in *postgres*|*pgsql*) die "BUILD_DIR must not contain postgres or pgsql: ${BUILD_DIR}" ;; esac
PG_CONFIG="${PREFIX}/bin/pg_config"

build_postgres() {
	local tarball="${BUILD_DIR}/postgresql-${PG_VERSION}.tar.gz"
	info "PostgreSQL ${PG_VERSION}: download and verify"
	curl -fsSL "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz" -o "${tarball}"
	echo "${PG_SHA256}  ${tarball}" | sha256sum --check
	tar -xzf "${tarball}" -C "${BUILD_DIR}"

	# The same features as the Linux and macOS archives, less uuid-ossp, which has no e2fs
	# library on Windows. `world-bin` is the server, the client programs and contrib (pgcrypto,
	# unaccent and the rest), without the documentation. GCC 15 compiles C23 by default, where
	# `bool` is a keyword and PostgreSQL 17.2's `typedef unsigned char bool` is an error, so the
	# dialect is pinned; PGXS hands the same flags to every extension built against it.
	info "PostgreSQL ${PG_VERSION}: configure, build, install (logs in ${BUILD_DIR})"
	(
		cd "${BUILD_DIR}/postgresql-${PG_VERSION}"
		./configure --prefix="${PREFIX}" --with-openssl --with-icu --with-zstd \
			CFLAGS="-O2 -std=gnu17" >"${BUILD_DIR}/configure.log"
		make -j"${JOBS}" world-bin >"${BUILD_DIR}/make.log"
		make install-world-bin >"${BUILD_DIR}/install.log"
	)
}

build_extensions() {
	info "pg_guard"
	build_pgxs "${REPO_ROOT}/extensions/pg_guard"
	info "supatype_mask"
	build_pgxs "${REPO_ROOT}/extensions/supatype_mask"

	# pg_keyspace is not built: it is a pgrx extension and pgrx has no Windows target. `supatype
	# dev` on Windows finds no library and starts the Valkey sidecar instead.

	# pgvector, wal2json, pg_cron, pg_safeupdate, pg_plan_filter, pgjwt and PostGIS, pinned in
	# the script every native build shares. bundle_dlls below picks up the DLLs PostGIS needs.
	BUILD_DIR="${BUILD_DIR}" JOBS="${JOBS}" \
		bash "${REPO_ROOT}/scripts/build-native-extensions.sh" "${PG_CONFIG}" windows
}

# imported_dlls <pe>...: the DLL names the files import, lower-case, one per line.
imported_dlls() {
	objdump -p "$@" | awk '/^\tDLL Name: / { print tolower($3) }' | sort -u
}

# Copy into bin/ every MINGW64 DLL the programs and libraries need, and the DLLs those need, until
# nothing new turns up. Worked out from the binaries rather than from `ldd`, which reports a
# missing DLL as "not found" on a line the old filter silently skipped.
bundle_dlls() {
	info "bundling runtime DLLs"
	local added=1 dll found
	while [ "${added}" -eq 1 ]; do
		added=0
		for dll in $(imported_dlls "${PREFIX}"/bin/*.exe "${PREFIX}"/bin/*.dll "${PREFIX}"/lib/postgresql/*.dll); do
			[ -e "${PREFIX}/bin/${dll}" ] && continue
			found=$(find /mingw64/bin -maxdepth 1 -iname "${dll}" -print -quit)
			[ -n "${found}" ] || continue
			cp "${found}" "${PREFIX}/bin/"
			echo "  ${dll}"
			added=1
		done
	done
}

# The archive carries what runs: programs, DLLs, server modules and share data. Headers, import
# libraries and PGXS are for building against it and stay behind.
stage() {
	mkdir -p "${STAGE}/lib/postgresql"
	cp -a "${PREFIX}/bin" "${PREFIX}/share" "${STAGE}/"
	cp -a "${PREFIX}"/lib/postgresql/*.dll "${STAGE}/lib/postgresql/"
}

build_postgres
build_extensions
bundle_dlls
stage

info "checking every program and library can load"
bash "${REPO_ROOT}/tests/native-archive/check-windows-deps.sh" "${STAGE}"
info "done: ${STAGE}"
