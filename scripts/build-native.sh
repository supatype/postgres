#!/usr/bin/env bash
# build-native.sh: build PostgreSQL 17 from source for a given target platform.
# Mirrors the steps in .github/workflows/native-archives.yml so the build can be
# reproduced locally without CI. (CI uses a native Linux ARM64 runner for
# linux-arm64 so pg_guard + pgvector compile; this script still supports
# cross-compiling Postgres from x86_64 Linux without those extensions.)
#
# Usage:
#   ./scripts/build-native.sh --target <target> [--pg-guard-dir <path>]
#
# Targets:
#   linux-amd64      Native Linux x86-64
#   linux-arm64      Linux aarch64: a native build on arm64 hosts; from x86_64 Linux, Postgres cross-builds but the C and Rust extensions are skipped (install aarch64-linux-gnu-*)
#   darwin-arm64     Native macOS Apple Silicon
#   darwin-amd64    Native macOS Intel
#
# Environment overrides:
#   PG_VERSION       Defaults to 17.2
#   INSTALL_PREFIX   Defaults to /usr/local/supatype-pg
#   BUILD_DIR        Defaults to ./pg-build (scratch space)
#   JOBS             Parallel make jobs (defaults to nproc / hw.logicalcpu)

set -euo pipefail

# --------------------------------------------------------------------------- #
# Defaults
# --------------------------------------------------------------------------- #
PG_VERSION="${PG_VERSION:-17.2}"
PG_SHA256="${PG_SHA256:-51d8cdd6a5220fa8c0a3b12f2d0eeb50fcf5e0bdb7b37904a9cdff5cf1e61c36}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local/supatype-pg}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/pg-build}"

TARGET=""
PG_GUARD_DIR="${REPO_ROOT}/extensions/pg_guard"
MASK_DIR="${REPO_ROOT}/extensions/supatype_mask"
KEYSPACE_DIR="${REPO_ROOT}/extensions/pg_keyspace"

# The pgrx version pg_keyspace is written against. It is not a floor: cargo-pgrx
# refuses to build a crate pinned to a different pgrx, so this tracks the
# `pgrx = "=x.y.z"` in extensions/pg_keyspace/extension/Cargo.toml exactly.
PGRX_VERSION="${PGRX_VERSION:-0.12.9}"

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
usage() {
  cat <<EOF
Usage: $(basename "$0") --target TARGET [--help]

Build PostgreSQL ${PG_VERSION} from source and produce a self-contained archive.
The in-repo extensions are bundled automatically: pg_guard, supatype_mask and
pg_keyspace, plus the upstream extensions scripts/build-native-extensions.sh
pins. pg_keyspace additionally needs a Rust toolchain; everything else needs
a C compiler and the libraries Step 1 installs.

Options:
  --target TARGET        Build target. One of:
                           linux-amd64      Native Linux x86-64
                           linux-arm64      Cross-compiled Linux aarch64
                           darwin-arm64     Native macOS Apple Silicon
                           darwin-amd64    Native macOS Intel
  --help                 Show this help message and exit.

Environment overrides:
  PG_VERSION             PostgreSQL version to build  (default: 17.2)
  INSTALL_PREFIX         Installation prefix inside archive (default: /usr/local/supatype-pg)
  BUILD_DIR              Scratch directory for source/build (default: ./pg-build)
  JOBS                   Parallel make jobs (default: auto-detected)
  PGRX_VERSION           cargo-pgrx version for pg_keyspace (default: 0.12.9)
  (The upstream extension pins and their overrides are in build-native-extensions.sh.)
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
info() { echo "[build-native] $*"; }
warn() { echo "WARNING: $*" >&2; }

# build_pgxs <source-dir>: build an in-repo extension against the staged
# PostgreSQL and install it there. PGXS takes every install directory from pg_config, which is
# relocatable and so reports the staged tree, so the library lands in the server's $libdir
# (lib/postgresql/) and the .control and .sql beside the other extensions. `clean` first, so a
# library left in the source tree is never mistaken for a fresh build.
build_pgxs() {
  make -C "$1" clean PG_CONFIG="${PG_CONFIG_BIN}" >/dev/null
  make -C "$1" -j"${JOBS}" PG_CONFIG="${PG_CONFIG_BIN}"
  make -C "$1" install PG_CONFIG="${PG_CONFIG_BIN}"
}

# --------------------------------------------------------------------------- #
# Parse arguments
# --------------------------------------------------------------------------- #
while [[ $# -gt 0 ]]; do
  case "$1" in
    --target)
      TARGET="$2"; shift 2 ;;
    --help|-h)
      usage; exit 0 ;;
    *)
      die "Unknown argument: $1. Run with --help for usage." ;;
  esac
done

[[ -n "${TARGET}" ]] || { usage; die "--target is required."; }

# Validate target
case "${TARGET}" in
  linux-amd64|linux-arm64|darwin-arm64|darwin-amd64) ;;
  *) die "Invalid target '${TARGET}'. Must be one of: linux-amd64, linux-arm64, darwin-arm64, darwin-amd64" ;;
esac

# Detect host OS
HOST_OS="$(uname -s)"
case "${HOST_OS}" in
  Linux)  HOST_PLATFORM="linux" ;;
  Darwin) HOST_PLATFORM="darwin" ;;
  MINGW*|CYGWIN*|MSYS*)
    die "Windows host detected. Use the PowerShell script instead:
  powershell.exe -ExecutionPolicy Bypass -File scripts/build-native-windows.ps1" ;;
  *) die "Unsupported host OS: ${HOST_OS}" ;;
esac

# linux-arm64: cross-compile Postgres only from x86_64 Linux. On aarch64 Linux
# (e.g. GitHub ubuntu-24.04-arm) or macOS, build natively so the extensions build.
IS_CROSS=false
if [[ "${TARGET}" == "linux-arm64" && "${HOST_PLATFORM}" == "linux" ]]; then
  host_arch="$(uname -m)"
  if [[ "${host_arch}" == "x86_64" ]]; then
    IS_CROSS=true
  fi
fi

# Auto-detect job count
if [[ -z "${JOBS:-}" ]]; then
  if [[ "${HOST_PLATFORM}" == "linux" ]]; then
    JOBS="$(nproc)"
  else
    JOBS="$(sysctl -n hw.logicalcpu)"
  fi
fi

PG_TARBALL="postgresql-${PG_VERSION}.tar.gz"
PG_URL="https://ftp.postgresql.org/pub/source/v${PG_VERSION}/${PG_TARBALL}"
PG_SRC_DIR="${BUILD_DIR}/postgresql-${PG_VERSION}"
STAGE_DIR="${BUILD_DIR}/pg-install"
ARCHIVE_NAME="${REPO_ROOT}/supatype-pg-17-${TARGET}.tar.gz"
PG_CONFIG_BIN="${STAGE_DIR}${INSTALL_PREFIX}/bin/pg_config"

info "Target:       ${TARGET}"
info "PG version:   ${PG_VERSION}"
info "Prefix:       ${INSTALL_PREFIX}"
info "Build dir:    ${BUILD_DIR}"
info "Jobs:         ${JOBS}"
info "Cross-build:  ${IS_CROSS}"
info "pg_guard dir: ${PG_GUARD_DIR}"
info "mask dir:     ${MASK_DIR}"
if [[ -d "${KEYSPACE_DIR}" ]]; then
  info "keyspace dir: ${KEYSPACE_DIR}"
else
  warn "pg_keyspace dir not found, so the archive will have no keyspace."
fi
echo ""

mkdir -p "${BUILD_DIR}"

# --------------------------------------------------------------------------- #
# Step 1: Install build dependencies
# --------------------------------------------------------------------------- #
info "=== Step 1: Install build dependencies ==="

if [[ "${HOST_PLATFORM}" == "linux" ]]; then
  # clang and libclang for pgrx's bindgen (pg_keyspace), as the CI job installs them.
  # bison and flex because PostgreSQL 17's configure refuses to run without them, and
  # git and curl because this script fetches the sources; a CI runner image has all
  # four, a fresh machine has none. libcurl for pg_net; GEOS, PROJ, libxml2 and
  # json-c for PostGIS.
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends \
    build-essential \
    bison \
    flex \
    git \
    curl \
    ca-certificates \
    pkg-config \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev \
    libicu-dev \
    libzstd-dev \
    uuid-dev \
    clang \
    libclang-dev \
    patchelf \
    libcurl4-openssl-dev \
    libgeos-dev \
    libproj-dev \
    libxml2-dev \
    libjson-c-dev

  if [[ "${IS_CROSS}" == "true" ]]; then
    info "Installing aarch64 cross-compilation toolchain..."
    sudo dpkg --add-architecture arm64
    sudo apt-get update -qq
    sudo apt-get install -y --no-install-recommends \
      gcc-aarch64-linux-gnu \
      binutils-aarch64-linux-gnu \
      libssl-dev:arm64 \
      libreadline-dev:arm64 \
      zlib1g-dev:arm64 \
      libicu-dev:arm64 \
      libzstd-dev:arm64 \
      uuid-dev:arm64
  fi

elif [[ "${HOST_PLATFORM}" == "darwin" ]]; then
  if ! command -v brew &>/dev/null; then
    die "Homebrew is required on macOS. Install it from https://brew.sh"
  fi
  brew install openssl readline icu4c zstd
fi

# --------------------------------------------------------------------------- #
# Step 2: Download & verify source tarball
# --------------------------------------------------------------------------- #
info "=== Step 2: Download PostgreSQL ${PG_VERSION} source ==="

if [[ ! -f "${BUILD_DIR}/${PG_TARBALL}" ]]; then
  curl -fsSL "${PG_URL}" -o "${BUILD_DIR}/${PG_TARBALL}"
else
  info "Tarball already present, skipping download."
fi

info "Verifying SHA256 checksum..."
if [[ "${HOST_PLATFORM}" == "linux" ]]; then
  echo "${PG_SHA256}  ${BUILD_DIR}/${PG_TARBALL}" | sha256sum --check
else
  echo "${PG_SHA256}  ${BUILD_DIR}/${PG_TARBALL}" | shasum -a 256 --check
fi
info "Checksum OK."

# --------------------------------------------------------------------------- #
# Step 3: Extract source
# --------------------------------------------------------------------------- #
info "=== Step 3: Extract source ==="

if [[ ! -d "${PG_SRC_DIR}" ]]; then
  tar -xzf "${BUILD_DIR}/${PG_TARBALL}" -C "${BUILD_DIR}"
else
  info "Source directory already exists, skipping extraction."
fi

# --------------------------------------------------------------------------- #
# Step 4: Configure
# --------------------------------------------------------------------------- #
info "=== Step 4: Configure ==="

pushd "${PG_SRC_DIR}" > /dev/null

CONFIGURE_FLAGS=(
  "--prefix=${INSTALL_PREFIX}"
  "--with-openssl"
  "--with-uuid=e2fs"
  "--with-icu"
  "--with-zstd"
  "--disable-rpath"
)

if [[ "${IS_CROSS}" == "true" ]]; then
  export CC=aarch64-linux-gnu-gcc
  export CXX=aarch64-linux-gnu-g++
  export AR=aarch64-linux-gnu-ar
  export STRIP=aarch64-linux-gnu-strip
  CONFIGURE_FLAGS+=(
    "--host=aarch64-linux-gnu"
    "--build=x86_64-linux-gnu"
  )
elif [[ "${HOST_PLATFORM}" == "darwin" ]]; then
  if [[ "$(uname -m)" == "x86_64" && -z "${MACOSX_DEPLOYMENT_TARGET:-}" ]]; then
    # Xcode 16.4's SDK exposes strchrnul as macOS 15.4+, and PG17 builds
    # with -Werror=unguarded-availability-new on Intel macOS.
    export MACOSX_DEPLOYMENT_TARGET=15.4
  fi
  OPENSSL_PREFIX="$(brew --prefix openssl)"
  ICU4C_PREFIX="$(brew --prefix icu4c)"
  READLINE_PREFIX="$(brew --prefix readline)"
  ZSTD_PREFIX="$(brew --prefix zstd)"
  export PKG_CONFIG_PATH="${ICU4C_PREFIX}/lib/pkgconfig:${OPENSSL_PREFIX}/lib/pkgconfig:${READLINE_PREFIX}/lib/pkgconfig:${ZSTD_PREFIX}/lib/pkgconfig${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
  CONFIGURE_FLAGS+=(
    "--with-libraries=${OPENSSL_PREFIX}/lib:${ICU4C_PREFIX}/lib:${READLINE_PREFIX}/lib:${ZSTD_PREFIX}/lib"
    "--with-includes=${OPENSSL_PREFIX}/include:${ICU4C_PREFIX}/include:${READLINE_PREFIX}/include:${ZSTD_PREFIX}/include"
  )
fi

./configure "${CONFIGURE_FLAGS[@]}"

# --------------------------------------------------------------------------- #
# Step 5: Build
#
# `world-bin`, not the default target: the server, the client programs and
# contrib, without the documentation. The default target leaves contrib out,
# and with it pgcrypto, unaccent and every other module the schema engine
# creates, so `CREATE EXTENSION pgcrypto` failed on every Linux and macOS
# archive. The Windows archive has always been built this way.
# --------------------------------------------------------------------------- #
info "=== Step 5: Build (${JOBS} jobs) ==="
make -j"${JOBS}" world-bin

# --------------------------------------------------------------------------- #
# Step 6: Staged install
# --------------------------------------------------------------------------- #
info "=== Step 6: Staged install ==="
rm -rf "${STAGE_DIR}"
make install-world-bin DESTDIR="${STAGE_DIR}"

popd > /dev/null

# --------------------------------------------------------------------------- #
# Step 7: C extensions (native targets only)
#
# Each is installed through PGXS into the server's $libdir, which is
# lib/postgresql/ and not lib/. Copied into lib/, as they were, the server could
# not find any of them: `CREATE EXTENSION vector` failed with "could not access
# file", and naming supatype_mask in shared_preload_libraries, which the CLI
# does whenever the library is in the archive, stopped the server starting.
# --------------------------------------------------------------------------- #
info "=== Step 7: C extensions ==="

if [[ "${IS_CROSS}" == "true" ]]; then
  # A native archive without the masking library is a database whose field rules do
  # not hold, so this must be loud rather than a silent skip.
  warn "Extensions cannot be cross-compiled, so pg_guard, supatype_mask and every upstream"
  warn "extension are skipped. The archive will NOT enforce field-level masking and realtime"
  warn "cannot stream database changes. Do not ship it."
else
  [[ -x "${PG_CONFIG_BIN}" ]] || die "pg_config not found at ${PG_CONFIG_BIN}"
  [[ -d "${PG_GUARD_DIR}" ]] || die "pg_guard directory not found at ${PG_GUARD_DIR}; repo may be incomplete."
  [[ -d "${MASK_DIR}" ]] || die "supatype_mask directory not found at ${MASK_DIR}; repo may be incomplete."

  info "pg_guard"
  build_pgxs "${PG_GUARD_DIR}"

  # Ships SQL as well as a library (the rejection function lives in an extension),
  # which PGXS installs beside it.
  info "supatype_mask"
  build_pgxs "${MASK_DIR}"

  # pgvector, wal2json, pg_cron, pg_net, pg_safeupdate, pg_plan_filter, pgjwt and
  # (Linux) PostGIS, pinned in the script every native build shares.
  BUILD_DIR="${BUILD_DIR}" JOBS="${JOBS}" \
    bash "${SCRIPT_DIR}/build-native-extensions.sh" "${PG_CONFIG_BIN}" "${HOST_PLATFORM}"
fi

# --------------------------------------------------------------------------- #
# Step 7d: Build and bundle pg_keyspace (Rust/pgrx, native targets only)
# --------------------------------------------------------------------------- #
info "=== Step 7d: pg_keyspace ==="

if [[ "${IS_CROSS}" == "true" ]]; then
  # Unlike the mask, an archive without this one is not unsafe, only smaller:
  # `supatype dev` probes for the library and falls back to the Valkey sidecar
  # when it is missing. Still worth saying, because the fallback is a second
  # process the developer did not ask for.
  warn "pg_keyspace cross-compilation not supported, so it is skipped. \`supatype dev\` will"
  warn "fall back to the Valkey sidecar with this archive."
elif [[ ! -d "${KEYSPACE_DIR}" ]]; then
  die "pg_keyspace directory not found at ${KEYSPACE_DIR}; repo may be incomplete."
else
  [[ -x "${PG_CONFIG_BIN}" ]] || die "pg_config not found at ${PG_CONFIG_BIN}"

  command -v cargo >/dev/null 2>&1 \
    || die "cargo not found. Install Rust (https://rustup.rs); pg_keyspace is a pgrx extension."
  if ! command -v cargo-pgrx >/dev/null 2>&1; then
    info "Installing cargo-pgrx ${PGRX_VERSION}..."
    cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
  fi

  # The major version drives both the pgrx feature and the package directory's
  # name, and PG_VERSION is an override here rather than a constant, so read it
  # from the Postgres that was actually built rather than assuming 17.
  PG_MAJOR="$("${PG_CONFIG_BIN}" --version | awk '{print $2}' | cut -d. -f1)"

  # `cargo pgrx package` builds against --pg-config directly, but it still
  # requires PGRX_HOME to exist ("$PGRX_HOME does not exist" otherwise).
  # It must NOT be created with `cargo pgrx init`: init runs initdb, which fails
  # against a staged (DESTDIR) Postgres whose binaries resolve libpq at an
  # absolute install prefix that is not installed yet. So write a minimal
  # PGRX_HOME that registers this pg_config, with no initdb involved.
  #
  # Under BUILD_DIR rather than ~/.pgrx: a developer running this script may
  # already have a pgrx home pointing at their own Postgres, and overwriting it
  # would be a surprising thing for a build script to do.
  export PGRX_HOME="${BUILD_DIR}/pgrx-home"
  mkdir -p "${PGRX_HOME}"
  printf '[configs]\npg%s = "%s"\n' "${PG_MAJOR}" "${PG_CONFIG_BIN}" > "${PGRX_HOME}/config.toml"

  info "Building pg_keyspace from ${KEYSPACE_DIR} against PG${PG_MAJOR}..."
  ( cd "${KEYSPACE_DIR}/extension" && cargo pgrx package --pg-config "${PG_CONFIG_BIN}" )

  # pgrx lays the package tree under target/release/pg_keyspace-pg{major} at the
  # paths pg_config REPORTS. PostgreSQL's pg_config is relocatable, so for a
  # staged build those are the ${STAGE_DIR}/... paths, not ${INSTALL_PREFIX}, so
  # derive them from pg_config rather than assuming the prefix. The same two
  # directories are where the library and its SQL go in the archive.
  PKGLIB_DIR="$("${PG_CONFIG_BIN}" --pkglibdir)"
  EXT_DIR="$("${PG_CONFIG_BIN}" --sharedir)/extension"
  PKGROOT="${KEYSPACE_DIR}/extension/target/release/pg_keyspace-pg${PG_MAJOR}"
  LIBSRC="${PKGROOT}${PKGLIB_DIR}"
  SHARESRC="${PKGROOT}${EXT_DIR}"

  find "${LIBSRC}" \( -name "pg_keyspace.so" -o -name "pg_keyspace.dylib" \) \
    -exec cp {} "${PKGLIB_DIR}/" \;
  cp "${SHARESRC}/pg_keyspace.control" "${EXT_DIR}/"
  cp "${SHARESRC}"/pg_keyspace--*.sql "${EXT_DIR}/"

  # Keys-only logical-decode plugin (Mode B row cache), beside the extension.
  info "supacache_keys"
  build_pgxs "${KEYSPACE_DIR}/plugin"

  info "pg_keyspace bundled into the server's \$libdir and extension directory."
fi

# --------------------------------------------------------------------------- #
# Step 7e: Make the archive self-contained and start it the way native dev does
# (native Linux only, as in CI). Bundling copies every non-glibc library the
# archive loads into lib/ and points every ELF file at it, so it starts on a
# distribution other than the build machine's.
# --------------------------------------------------------------------------- #
if [[ "${IS_CROSS}" == "false" && "${HOST_PLATFORM}" == "linux" ]]; then
  info "=== Step 7e: Bundle shared libraries ==="
  bash "${SCRIPT_DIR}/bundle-linux-libraries.sh" "${STAGE_DIR}${INSTALL_PREFIX}"
  info "=== Step 7f: Runtime check ==="
  bash "${REPO_ROOT}/tests/native-archive/check-unix-runtime.sh" "${STAGE_DIR}${INSTALL_PREFIX}"
fi

# --------------------------------------------------------------------------- #
# Step 8: Package archive
# --------------------------------------------------------------------------- #
info "=== Step 8: Package archive ==="

tar -czf "${ARCHIVE_NAME}" -C "${STAGE_DIR}${INSTALL_PREFIX}" .
info "Archive created: ${ARCHIVE_NAME}"

# --------------------------------------------------------------------------- #
# Step 9: Checksum
# --------------------------------------------------------------------------- #
info "=== Step 9: Generate checksum ==="

if [[ "${HOST_PLATFORM}" == "linux" ]]; then
  sha256sum "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256"
else
  shasum -a 256 "${ARCHIVE_NAME}" > "${ARCHIVE_NAME}.sha256"
fi

info "Checksum written: ${ARCHIVE_NAME}.sha256"
info ""
info "Done! Output files:"
info "  ${ARCHIVE_NAME}"
info "  ${ARCHIVE_NAME}.sha256"
