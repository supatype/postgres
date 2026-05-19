#!/usr/bin/env bash
# build-native.sh — Build PostgreSQL 17 from source for a given target platform.
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
#   linux-arm64      Linux aarch64 — native build on arm64 hosts; from x86_64 Linux, Postgres cross-builds but pg_guard/pgvector are skipped (install aarch64-linux-gnu-*)
#   darwin-arm64     Native macOS Apple Silicon
#   darwin-x86_64    Native macOS Intel
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

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
usage() {
  cat <<EOF
Usage: $(basename "$0") --target TARGET [--help]

Build PostgreSQL ${PG_VERSION} from source and produce a self-contained archive.
pg_guard is bundled automatically from extensions/pg_guard/ in this repo.

Options:
  --target TARGET        Build target. One of:
                           linux-amd64      Native Linux x86-64
                           linux-arm64      Cross-compiled Linux aarch64
                           darwin-arm64     Native macOS Apple Silicon
                           darwin-x86_64    Native macOS Intel
  --help                 Show this help message and exit.

Environment overrides:
  PG_VERSION             PostgreSQL version to build  (default: 17.2)
  INSTALL_PREFIX         Installation prefix inside archive (default: /usr/local/supatype-pg)
  BUILD_DIR              Scratch directory for source/build (default: ./pg-build)
  JOBS                   Parallel make jobs (default: auto-detected)
EOF
}

die() { echo "ERROR: $*" >&2; exit 1; }
info() { echo "[build-native] $*"; }
warn() { echo "WARNING: $*" >&2; }

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
  linux-amd64|linux-arm64|darwin-arm64|darwin-x86_64) ;;
  *) die "Invalid target '${TARGET}'. Must be one of: linux-amd64, linux-arm64, darwin-arm64, darwin-x86_64" ;;
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
# (e.g. GitHub ubuntu-24.04-arm) or macOS, build natively so pg_guard/pgvector run.
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

info "Target:       ${TARGET}"
info "PG version:   ${PG_VERSION}"
info "Prefix:       ${INSTALL_PREFIX}"
info "Build dir:    ${BUILD_DIR}"
info "Jobs:         ${JOBS}"
info "Cross-build:  ${IS_CROSS}"
[[ -n "${PG_GUARD_DIR}" ]] && info "pg_guard dir: ${PG_GUARD_DIR}" || warn "pg_guard dir not set — pg_guard will be skipped."
echo ""

mkdir -p "${BUILD_DIR}"

# --------------------------------------------------------------------------- #
# Step 1 — Install build dependencies
# --------------------------------------------------------------------------- #
info "=== Step 1: Install build dependencies ==="

if [[ "${HOST_PLATFORM}" == "linux" ]]; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends \
    build-essential \
    libssl-dev \
    libreadline-dev \
    zlib1g-dev \
    libicu-dev \
    libzstd-dev

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
      libzstd-dev:arm64
  fi

elif [[ "${HOST_PLATFORM}" == "darwin" ]]; then
  if ! command -v brew &>/dev/null; then
    die "Homebrew is required on macOS. Install it from https://brew.sh"
  fi
  brew install openssl readline icu4c zstd
fi

# --------------------------------------------------------------------------- #
# Step 2 — Download & verify source tarball
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
# Step 3 — Extract source
# --------------------------------------------------------------------------- #
info "=== Step 3: Extract source ==="

if [[ ! -d "${PG_SRC_DIR}" ]]; then
  tar -xzf "${BUILD_DIR}/${PG_TARBALL}" -C "${BUILD_DIR}"
else
  info "Source directory already exists, skipping extraction."
fi

# --------------------------------------------------------------------------- #
# Step 4 — Configure
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
  OPENSSL_PREFIX="$(brew --prefix openssl)"
  ICU4C_PREFIX="$(brew --prefix icu4c)"
  READLINE_PREFIX="$(brew --prefix readline)"
  ZSTD_PREFIX="$(brew --prefix zstd)"
  CONFIGURE_FLAGS+=(
    "--with-libraries=${OPENSSL_PREFIX}/lib:${ICU4C_PREFIX}/lib:${READLINE_PREFIX}/lib:${ZSTD_PREFIX}/lib"
    "--with-includes=${OPENSSL_PREFIX}/include:${ICU4C_PREFIX}/include:${READLINE_PREFIX}/include:${ZSTD_PREFIX}/include"
  )
fi

./configure "${CONFIGURE_FLAGS[@]}"

# --------------------------------------------------------------------------- #
# Step 5 — Build
# --------------------------------------------------------------------------- #
info "=== Step 5: Build (${JOBS} jobs) ==="
make -j"${JOBS}"

# --------------------------------------------------------------------------- #
# Step 6 — Staged install
# --------------------------------------------------------------------------- #
info "=== Step 6: Staged install ==="
rm -rf "${STAGE_DIR}"
make install DESTDIR="${STAGE_DIR}"

popd > /dev/null

# --------------------------------------------------------------------------- #
# Step 7 — Build and bundle pg_guard (best-effort)
# --------------------------------------------------------------------------- #
info "=== Step 7: pg_guard ==="

if [[ "${IS_CROSS}" == "true" ]]; then
  warn "pg_guard cross-compilation not supported — skipping."
elif [[ ! -d "${PG_GUARD_DIR}" ]]; then
  die "pg_guard directory not found at ${PG_GUARD_DIR} — repo may be incomplete."
else
  info "Building pg_guard from ${PG_GUARD_DIR}..."
  PG_CONFIG_BIN="${STAGE_DIR}${INSTALL_PREFIX}/bin/pg_config"
  if [[ ! -x "${PG_CONFIG_BIN}" ]]; then
    die "pg_config not found at ${PG_CONFIG_BIN}"
  fi
  PG_CONFIG="${PG_CONFIG_BIN}" make -C "${PG_GUARD_DIR}"
  LIB_DIR="${STAGE_DIR}${INSTALL_PREFIX}/lib"
  mkdir -p "${LIB_DIR}"
  find "${PG_GUARD_DIR}" \( -name "pg_guard.so" -o -name "pg_guard.dylib" \) \
    -exec cp {} "${LIB_DIR}/" \; -print
  info "pg_guard bundled into ${LIB_DIR}."
fi

# --------------------------------------------------------------------------- #
# Step 7b — pgvector (C extension — native targets only)
# --------------------------------------------------------------------------- #
info "=== Step 7b: pgvector ==="

PGVECTOR_VERSION="${PGVECTOR_VERSION:-0.8.0}"

if [[ "${IS_CROSS}" == "true" ]]; then
  warn "pgvector cross-compilation not supported — skipping."
else
  PG_CONFIG_BIN="${STAGE_DIR}${INSTALL_PREFIX}/bin/pg_config"
  LIB_DIR="${STAGE_DIR}${INSTALL_PREFIX}/lib"
  SHARE_EXT_DIR="${STAGE_DIR}${INSTALL_PREFIX}/share/postgresql/extension"
  mkdir -p "${SHARE_EXT_DIR}"

  # Clone the tagged release; if it fails to compile against this PG version,
  # retry with HEAD (which tracks the latest PG release).
  git clone --depth 1 --branch "v${PGVECTOR_VERSION}" \
    https://github.com/pgvector/pgvector.git "${BUILD_DIR}/pgvector"
  if ! PG_CONFIG="${PG_CONFIG_BIN}" make -C "${BUILD_DIR}/pgvector" -j"${JOBS}"; then
    warn "pgvector ${PGVECTOR_VERSION} failed to compile — retrying with HEAD..."
    rm -rf "${BUILD_DIR}/pgvector"
    git clone --depth 1 https://github.com/pgvector/pgvector.git "${BUILD_DIR}/pgvector"
    PG_CONFIG="${PG_CONFIG_BIN}" make -C "${BUILD_DIR}/pgvector" -j"${JOBS}"
  fi

  find "${BUILD_DIR}/pgvector" \( -name "vector.so" -o -name "vector.dylib" \) \
    -exec cp {} "${LIB_DIR}/" \;
  cp "${BUILD_DIR}/pgvector/vector.control" "${SHARE_EXT_DIR}/"
  find "${BUILD_DIR}/pgvector" -name "vector--*.sql" \
    -exec cp {} "${SHARE_EXT_DIR}/" \;
  info "pgvector bundled."
fi

# --------------------------------------------------------------------------- #
# Step 7c — pgjwt (SQL-only — works for all targets including cross-compile)
# --------------------------------------------------------------------------- #
info "=== Step 7c: pgjwt ==="

PGJWT_COMMIT="${PGJWT_COMMIT:-f3d82fd30151e754e19ce5d6a06c71c20689ce3d}"
git clone https://github.com/michelp/pgjwt.git "${BUILD_DIR}/pgjwt"
git -C "${BUILD_DIR}/pgjwt" checkout "${PGJWT_COMMIT}"
SHARE_EXT_DIR="${STAGE_DIR}${INSTALL_PREFIX}/share/postgresql/extension"
mkdir -p "${SHARE_EXT_DIR}"
cp "${BUILD_DIR}/pgjwt/pgjwt.control" "${SHARE_EXT_DIR}/"
find "${BUILD_DIR}/pgjwt" -name "pgjwt--*.sql" \
  -exec cp {} "${SHARE_EXT_DIR}/" \;
info "pgjwt bundled."

# --------------------------------------------------------------------------- #
# Step 8 — Package archive
# --------------------------------------------------------------------------- #
info "=== Step 8: Package archive ==="

tar -czf "${ARCHIVE_NAME}" -C "${STAGE_DIR}${INSTALL_PREFIX}" .
info "Archive created: ${ARCHIVE_NAME}"

# --------------------------------------------------------------------------- #
# Step 9 — Checksum
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
