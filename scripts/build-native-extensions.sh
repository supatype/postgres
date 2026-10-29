#!/usr/bin/env bash
# build-native-extensions.sh: build the upstream extensions every native archive carries, and
# install them into a staged PostgreSQL.
#
# One list for every build path: the build job in .github/workflows/native-archives.yml (Linux and
# macOS), scripts/build-native.sh and scripts/build-native-windows.sh all run this script, so the
# pins below are the only place a native extension version is set. They match what the Dockerfile
# installs, which is what keeps native dev and the image behaving the same.
#
# Every extension installs through PGXS, which takes its directories from the staged pg_config
# (it is relocatable), so libraries land in the server's $libdir and SQL beside the other
# extensions. The in-repo extensions (pg_guard, supatype_mask, pg_keyspace) are built by the
# callers.
#
# What each platform gets:
#   all       pgvector, wal2json, pg_cron, pg_safeupdate, pg_plan_filter, pgjwt
#   linux     + pg_net, PostGIS
#   darwin    + pg_net
#   windows   + PostGIS
# pg_net has no Windows event loop (epoll on Linux, kqueue on macOS, nothing else). PostGIS on
# macOS needs GEOS, PROJ and json-c bundled and relinked into the archive, which is not done yet.
#
# Usage: build-native-extensions.sh <pg_config> <linux|darwin|windows>
# Environment: BUILD_DIR (scratch, default a new temporary directory), JOBS (default nproc).
set -euo pipefail

PGVECTOR_VERSION="${PGVECTOR_VERSION:-0.8.0}"
WAL2JSON_VERSION="${WAL2JSON_VERSION:-2.7}"
PG_CRON_VERSION="${PG_CRON_VERSION:-1.6.8}"
PG_NET_VERSION="${PG_NET_VERSION:-0.14.0}"
PG_SAFEUPDATE_VERSION="${PG_SAFEUPDATE_VERSION:-1.7}"
# pg_plan_filter has never been tagged; this is the commit the Dockerfile's clone of HEAD gets.
PG_PLAN_FILTER_COMMIT="${PG_PLAN_FILTER_COMMIT:-c1afc73dacae8688456bdeef72ed6a3030c500ba}"
PGJWT_COMMIT="${PGJWT_COMMIT:-f3d82fd30151e754e19ce5d6a06c71c20689ce3d}"
POSTGIS_VERSION="${POSTGIS_VERSION:-3.6.4}"
POSTGIS_SHA256="${POSTGIS_SHA256:-ed8dc6679f1e06f7b113592b04cde2a7e00f1b1e681294c8ca2204058990cec6}"
# Linux only, for pg_net: see build_linux_libcurl.
CURL_VERSION="${CURL_VERSION:-8.23.0}"
CURL_SHA256="${CURL_SHA256:-e3de77cfa6367083c776a26647fc9f633b538c06c3baa73d931a252ac8f72b49}"

info() { echo "[build-native-extensions] $*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

[ $# -eq 2 ] || { echo "Usage: $(basename "$0") <pg_config> <linux|darwin|windows>" >&2; exit 2; }
PG_CONFIG=$1
PLATFORM=$2
case "${PLATFORM}" in linux|darwin|windows) ;; *) die "unknown platform ${PLATFORM}" ;; esac
[ -x "${PG_CONFIG}" ] || die "pg_config not found at ${PG_CONFIG}"

if [ -z "${JOBS:-}" ]; then
	JOBS=$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)
fi
BUILD_DIR="${BUILD_DIR:-$(mktemp -d)}"
SRC="${BUILD_DIR}/native-extensions"
mkdir -p "${SRC}"

# fetch <git-url> <tag-or-commit> <name>: a shallow checkout of exactly that ref.
fetch() {
	local dir="${SRC}/$3"
	[ ! -e "${dir}" ] || die "${dir} already exists; use a fresh BUILD_DIR"
	git init -q "${dir}"
	git -C "${dir}" fetch -q --depth 1 "$1" "$2"
	git -C "${dir}" checkout -q FETCH_HEAD
}

# build_pgxs <name> [make-variable=value...]: build and install a fetched extension.
build_pgxs() {
	local dir="${SRC}/$1"
	shift
	make -C "${dir}" -j"${JOBS}" PG_CONFIG="${PG_CONFIG}" "$@"
	make -C "${dir}" install PG_CONFIG="${PG_CONFIG}" "$@"
}

# OPTFLAGS empty, as in the Dockerfile: pgvector otherwise compiles with -march=native, so the
# library carries whatever instructions the build machine's CPU has (AVX-512 on a CI runner) and
# `CREATE EXTENSION vector` crashes the backend with an illegal instruction on a CPU without them.
# The AVX-512 paths pgvector picks at run time are unaffected.
build_pgvector() {
	info "pgvector ${PGVECTOR_VERSION}"
	fetch https://github.com/pgvector/pgvector.git "v${PGVECTOR_VERSION}" pgvector
	build_pgxs pgvector OPTFLAGS=
}

# Realtime streams database changes through a wal2json logical replication slot. It is an output
# plugin, not an extension, so without it everything else works and `supatype dev` reports
# "database changes are off: the wal2json output plugin is not installed".
build_wal2json() {
	info "wal2json ${WAL2JSON_VERSION}"
	fetch https://github.com/eulerto/wal2json.git "wal2json_${WAL2JSON_VERSION//./_}" wal2json
	build_pgxs wal2json
}

build_small_extensions() {
	info "pg_cron ${PG_CRON_VERSION}"
	fetch https://github.com/citusdata/pg_cron.git "v${PG_CRON_VERSION}" pg_cron
	build_pgxs pg_cron

	info "pg_safeupdate ${PG_SAFEUPDATE_VERSION}"
	fetch https://github.com/eradman/pg-safeupdate.git "${PG_SAFEUPDATE_VERSION}" pg_safeupdate
	build_pgxs pg_safeupdate

	info "pg_plan_filter ${PG_PLAN_FILTER_COMMIT}"
	fetch https://github.com/pgexperts/pg_plan_filter.git "${PG_PLAN_FILTER_COMMIT}" pg_plan_filter
	# Its OBJS names $(WIN32RES), a version resource only PostgreSQL's own tree can build, so a
	# PGXS build on Windows stops at "No rule to make target 'win32ver.rc'". Empty elsewhere anyway.
	build_pgxs pg_plan_filter WIN32RES=

	info "pgjwt (SQL only)"
	fetch https://github.com/michelp/pgjwt.git "${PGJWT_COMMIT}" pgjwt
	cp "${SRC}/pgjwt/pgjwt.control" "${SRC}"/pgjwt/pgjwt--*.sql "$("${PG_CONFIG}" --sharedir)/extension/"
}

build_pg_net() {
	info "pg_net ${PG_NET_VERSION}"
	fetch https://github.com/supabase/pg_net.git "v${PG_NET_VERSION}" pg_net
	build_pgxs pg_net "$@"
}

# pg_net needs libcurl 7.83 or later and the Linux archive is built on Ubuntu 22.04, which ships
# 7.81, so Linux builds a pinned libcurl into the archive's lib/ and pg_net against it. Only what
# HTTP needs is enabled, and certificates are verified against OpenSSL's own default store rather
# than a bundle path from the build machine.
build_linux_libcurl() {
	local tarball="${SRC}/curl-${CURL_VERSION}.tar.gz" prefix="${SRC}/curl-install"
	info "libcurl ${CURL_VERSION}"
	curl -fsSL "https://curl.se/download/curl-${CURL_VERSION}.tar.gz" -o "${tarball}"
	echo "${CURL_SHA256}  ${tarball}" | sha256sum --check
	tar -xzf "${tarball}" -C "${SRC}"
	(
		cd "${SRC}/curl-${CURL_VERSION}"
		./configure --prefix="${prefix}" --with-openssl --without-ca-bundle --without-ca-path \
			--with-ca-fallback --disable-ldap --disable-ldaps --disable-docs --disable-manual \
			--without-libpsl --without-libidn2 --without-nghttp2 --without-brotli --without-zstd \
			--without-librtmp --without-libssh2 --disable-static >"${SRC}/curl-configure.log"
		make -j"${JOBS}" >"${SRC}/curl-make.log"
		make install >"${SRC}/curl-install.log"
	)
	cp -P "${prefix}"/lib/libcurl.so* "$("${PG_CONFIG}" --libdir)/"
	CURL_PREFIX="${prefix}"
}

# PostGIS without raster (GDAL) or protobuf (ST_AsMVT): the two dependencies too large to carry
# for a development database. GEOS, PROJ, libxml2 and json-c come from the build machine and are
# copied into the archive afterwards (scripts/bundle-linux-libraries.sh on Linux, the DLL scan in
# build-native-windows.sh on Windows).
build_postgis() {
	local tarball="${SRC}/postgis-${POSTGIS_VERSION}.tar.gz"
	local dir="${SRC}/postgis-${POSTGIS_VERSION}" root
	root=$(cd "$("${PG_CONFIG}" --bindir)/.." && pwd)
	info "PostGIS ${POSTGIS_VERSION}"
	curl -fsSL "https://download.osgeo.org/postgis/source/postgis-${POSTGIS_VERSION}.tar.gz" -o "${tarball}"
	echo "${POSTGIS_SHA256}  ${tarball}" | sha256sum --check
	tar -xzf "${tarball}" -C "${SRC}"
	(
		cd "${dir}"
		# --prefix so the loader programs (shp2pgsql, pgtopo_import, ...) land in the archive's
		# bin/ beside the server rather than in /usr/local/bin; the extension itself installs
		# through PGXS either way.
		./configure --prefix="${root}" --with-pgconfig="${PG_CONFIG}" --without-raster \
			--without-protobuf --without-interrupt-tests >"${SRC}/postgis-configure.log"
		# Serial: in parallel, two makes generate raster/rt_pg/rtpostgis.sql at once (the SQL is
		# built even without raster) and one deletes the other's temporary file.
		make >"${SRC}/postgis-make.log" 2>&1 || { tail -40 "${SRC}/postgis-make.log"; exit 1; }
		make install >"${SRC}/postgis-install.log"
	)
	bundle_proj_data "${root}"
}

# ST_Transform reads PROJ's coordinate system database. PROJ looks for it in share/proj one
# level above the directory its own library is in, which in the archive is share/proj at the
# root, beside the server's own share directory. bundle_proj_data <archive-root>.
bundle_proj_data() {
	local root=$1 candidate data=""
	for candidate in /usr/share/proj /usr/local/share/proj /mingw64/share/proj; do
		if [ -f "${candidate}/proj.db" ]; then
			data=${candidate}
			break
		fi
	done
	[ -n "${data}" ] || die "proj.db not found; install PROJ's data files"
	mkdir -p "${root}/share/proj"
	cp -R "${data}"/. "${root}/share/proj/"
}

build_pgvector
build_wal2json
build_small_extensions
case "${PLATFORM}" in
	linux)
		build_linux_libcurl
		# CPPFLAGS through the environment, not the make command line: pg_net folds the
		# environment's CPPFLAGS into its own flags, while a command-line CPPFLAGS replaces
		# PostgreSQL's (and with them _GNU_SOURCE). CURL_DISABLE_TYPECHECK because curl 8's
		# compile-time option checks reject pg_net 0.14.0 under its -Werror; the image builds
		# the same source against Debian's curl 7.88, whose checks it passes.
		CPPFLAGS="-I${CURL_PREFIX}/include -DCURL_DISABLE_TYPECHECK" \
			build_pg_net PG_LDFLAGS="-L${CURL_PREFIX}/lib"
		build_postgis
		;;
	darwin)
		build_pg_net
		;;
	windows)
		build_postgis
		;;
esac
info "done"
