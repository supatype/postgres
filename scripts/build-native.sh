#!/usr/bin/env bash
# build-native.sh: build PostgreSQL 17 and every extension the native archives carry, from
# source, for one Linux or macOS target, and package the result.
#
# The build job in .github/workflows/native-archives.yml runs this script for every Linux and
# macOS target, the way the build-windows job runs build-native-windows.sh, so a local build and
# a CI build go through the same steps. CI builds linux-arm64 on a native arm64 runner so the
# extensions compile; from x86_64 Linux this script cross-compiles Postgres for that target
# without them.
#
# Usage: ./scripts/build-native.sh --target <target> [--ccache] [--stage-only | --package-only]
# Run with --help for the targets, the options and the environment overrides.

set -euo pipefail

PG_VERSION="${PG_VERSION:-17.2}"
PG_SHA256="${PG_SHA256:-51d8cdd6a5220fa8c0a3b12f2d0eeb50fcf5e0bdb7b37904a9cdff5cf1e61c36}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local/supatype-pg}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
BUILD_DIR="${BUILD_DIR:-${REPO_ROOT}/pg-build}"
KEYSPACE_DIR="${REPO_ROOT}/extensions/pg_keyspace"

# The pgrx version pg_keyspace is written against. It is not a floor: cargo-pgrx
# refuses to build a crate pinned to a different pgrx, so this tracks the
# `pgrx = "=x.y.z"` in extensions/pg_keyspace/extension/Cargo.toml exactly.
PGRX_VERSION="${PGRX_VERSION:-0.12.9}"

# Every Debian/Ubuntu package a Linux build needs, here and nowhere else (CI runs this script).
# bison and flex because PostgreSQL 17's configure refuses to run without them; git and curl
# because the sources are fetched; clang and libclang for pgrx's bindgen (pg_keyspace); patchelf
# to point the archive at the libraries bundled into it; libcurl for pg_net; GEOS, PROJ, libxml2
# and json-c for PostGIS.
LINUX_PACKAGES=(
  build-essential bison flex git curl ca-certificates pkg-config
  libssl-dev libreadline-dev zlib1g-dev libicu-dev libzstd-dev uuid-dev
  clang libclang-dev patchelf
  libcurl4-openssl-dev libgeos-dev libproj-dev libxml2-dev libjson-c-dev
)
HOMEBREW_PACKAGES=(openssl readline icu4c zstd)

# shellcheck source=scripts/lib.sh
source "${SCRIPT_DIR}/lib.sh"

TARGET=""
USE_CCACHE=false
PHASE=all

usage() {
  cat <<EOF
Usage: $(basename "$0") --target TARGET [--ccache] [--stage-only | --package-only]

Build PostgreSQL ${PG_VERSION} from source and produce a self-contained archive,
supatype-pg-17-TARGET.tar.gz and its .sha256, in the repository root. The in-repo
extensions (pg_guard, supatype_mask, pg_keyspace) are bundled, and so are the
upstream extensions scripts/build-native-extensions.sh pins. pg_keyspace needs a
Rust toolchain; everything else needs what Step 1 installs (apt or Homebrew).

Options:
  --target TARGET   linux-amd64    Native Linux x86-64
                    linux-arm64    Linux aarch64: native on an arm64 host; from
                                   x86_64 Postgres alone is cross-compiled
                    darwin-arm64   Native macOS Apple Silicon
                    darwin-amd64   Native macOS Intel
  --ccache          Compile through ccache (installed in Step 1).
  --stage-only      Stop once the staged tree is built and checked, so it can be
                    signed before packaging.
  --package-only    Package a tree an earlier --stage-only run left.
  --help            Show this help message and exit.

Environment overrides:
  PG_VERSION        PostgreSQL version to build (default: 17.2)
  PG_SHA256         SHA-256 of its source tarball
  INSTALL_PREFIX    Installation prefix inside archive (default: /usr/local/supatype-pg)
  BUILD_DIR         Scratch directory for source and build (default: ./pg-build)
  JOBS              Parallel make jobs (default: auto-detected)
  PGRX_VERSION      cargo-pgrx version for pg_keyspace (default: 0.12.9)
  (The upstream extension pins and their overrides are in build-native-extensions.sh.)
EOF
}

info() { echo "[build-native] $*"; }
warn() { echo "WARNING: $*" >&2; }

set_phase() {
  [[ "${PHASE}" == all ]] || die "--stage-only and --package-only cannot be combined."
  PHASE=$1
}

parse_args() {
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --target) [[ $# -ge 2 ]] || die "--target needs a value."; TARGET="$2"; shift 2 ;;
      --ccache) USE_CCACHE=true; shift ;;
      --stage-only) set_phase stage; shift ;;
      --package-only) set_phase package; shift ;;
      --help|-h) usage; exit 0 ;;
      *) die "Unknown argument: $1. Run with --help for usage." ;;
    esac
  done
  [[ -n "${TARGET}" ]] || { usage; die "--target is required."; }
  case "${TARGET}" in
    linux-amd64|linux-arm64|darwin-arm64|darwin-amd64) ;;
    *) die "Invalid target '${TARGET}'. Must be one of: linux-amd64, linux-arm64, darwin-arm64, darwin-amd64" ;;
  esac
}

# Sets HOST_PLATFORM, IS_CROSS and JOBS. linux-arm64 is cross-compiled only from x86_64 Linux;
# on an arm64 host (GitHub's ubuntu-24.04-arm) it is a native build, extensions included.
detect_host() {
  case "$(uname -s)" in
    Linux) HOST_PLATFORM=linux ;;
    Darwin) HOST_PLATFORM=darwin ;;
    MINGW*|CYGWIN*|MSYS*) die "Windows host detected. Use scripts/build-native-windows.ps1 instead." ;;
    *) die "Unsupported host OS: $(uname -s)" ;;
  esac
  [[ "${TARGET%%-*}" == "${HOST_PLATFORM}" ]] || die "a ${TARGET} archive cannot be built on ${HOST_PLATFORM}."
  IS_CROSS=false
  if [[ "${TARGET}" == linux-arm64 && "$(uname -m)" == x86_64 ]]; then
    IS_CROSS=true
  fi
  JOBS="${JOBS:-$(nproc 2>/dev/null || sysctl -n hw.logicalcpu)}"
}

set_paths() {
  PG_TARBALL="${BUILD_DIR}/postgresql-${PG_VERSION}.tar.gz"
  PG_SRC_DIR="${BUILD_DIR}/postgresql-${PG_VERSION}"
  STAGE_DIR="${BUILD_DIR}/pg-install"
  STAGE_ROOT="${STAGE_DIR}${INSTALL_PREFIX}"
  PG_CONFIG="${STAGE_ROOT}/bin/pg_config"
  ARCHIVE_NAME="supatype-pg-17-${TARGET}.tar.gz"
}

print_summary() {
  info "Target:       ${TARGET}"
  info "PG version:   ${PG_VERSION}"
  info "Prefix:       ${INSTALL_PREFIX}"
  info "Build dir:    ${BUILD_DIR}"
  info "Jobs:         ${JOBS}"
  info "Cross-build:  ${IS_CROSS}"
  info "Phase:        ${PHASE}"
  echo ""
}

# sha256 <args>: sha256sum where there is one, else macOS's shasum, with the same arguments.
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum "$@"; else shasum -a 256 "$@"; fi
}

install_linux_deps() {
  local packages=("${LINUX_PACKAGES[@]}")
  if [[ "${USE_CCACHE}" == true ]]; then packages+=(ccache); fi
  sudo apt-get update -qq
  sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends "${packages[@]}"
  if [[ "${IS_CROSS}" == true ]]; then
    info "Installing the aarch64 cross-compilation toolchain..."
    sudo dpkg --add-architecture arm64
    sudo apt-get update -qq
    sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
      gcc-aarch64-linux-gnu binutils-aarch64-linux-gnu libssl-dev:arm64 libreadline-dev:arm64 \
      zlib1g-dev:arm64 libicu-dev:arm64 libzstd-dev:arm64 uuid-dev:arm64
  fi
}

install_deps() {
  info "=== Step 1: Install build dependencies ==="
  if [[ "${HOST_PLATFORM}" == linux ]]; then
    install_linux_deps
    return
  fi
  command -v brew >/dev/null 2>&1 || die "Homebrew is required on macOS. Install it from https://brew.sh"
  local packages=("${HOMEBREW_PACKAGES[@]}")
  if [[ "${USE_CCACHE}" == true ]]; then packages+=(ccache); fi
  brew install "${packages[@]}"
}

# Puts ccache's compiler wrappers first on PATH, so PostgreSQL, every PGXS extension (which
# compiles with the CC configure recorded, `gcc` or `clang`), libcurl, PostGIS and pgrx's C shim
# all compile through it.
use_ccache() {
  [[ "${USE_CCACHE}" == true ]] || return 0
  local wrappers=/usr/lib/ccache
  if [[ "${HOST_PLATFORM}" == darwin ]]; then wrappers="$(brew --prefix)/opt/ccache/libexec"; fi
  [[ -d "${wrappers}" ]] || die "ccache's compiler wrappers are not at ${wrappers}"
  export PATH="${wrappers}:${PATH}"
  info "Compiling through ccache (${wrappers})"
}

fetch_source() {
  info "=== Step 2: Download, verify and extract PostgreSQL ${PG_VERSION} ==="
  mkdir -p "${BUILD_DIR}"
  if [[ ! -f "${PG_TARBALL}" ]]; then
    curl -fsSL "https://ftp.postgresql.org/pub/source/v${PG_VERSION}/postgresql-${PG_VERSION}.tar.gz" -o "${PG_TARBALL}"
  else
    info "Tarball already present, skipping download."
  fi
  echo "${PG_SHA256}  ${PG_TARBALL}" | sha256 --check
  if [[ ! -d "${PG_SRC_DIR}" ]]; then
    tar -xzf "${PG_TARBALL}" -C "${BUILD_DIR}"
  else
    info "Source directory already exists, skipping extraction."
  fi
}

# Homebrew's openssl, icu4c, readline and zstd are keg-only, so neither the compiler nor
# pkg-config finds them unaided. Appends to CONFIGURE_FLAGS.
add_homebrew_flags() {
  local formula prefix libs="" includes="" pkgconfig=""
  for formula in "${HOMEBREW_PACKAGES[@]}"; do
    prefix="$(brew --prefix "${formula}")"
    libs+="${libs:+:}${prefix}/lib"
    includes+="${includes:+:}${prefix}/include"
    pkgconfig+="${pkgconfig:+:}${prefix}/lib/pkgconfig"
  done
  export PKG_CONFIG_PATH="${pkgconfig}${PKG_CONFIG_PATH:+:${PKG_CONFIG_PATH}}"
  CONFIGURE_FLAGS+=("--with-libraries=${libs}" "--with-includes=${includes}")
}

configure_postgres() {
  info "=== Step 3: Configure ==="
  CONFIGURE_FLAGS=("--prefix=${INSTALL_PREFIX}" --with-openssl --with-uuid=e2fs --with-icu --with-zstd --disable-rpath)
  if [[ "${IS_CROSS}" == true ]]; then
    export CC=aarch64-linux-gnu-gcc CXX=aarch64-linux-gnu-g++ AR=aarch64-linux-gnu-ar STRIP=aarch64-linux-gnu-strip
    CONFIGURE_FLAGS+=(--host=aarch64-linux-gnu --build=x86_64-linux-gnu)
  elif [[ "${HOST_PLATFORM}" == darwin ]]; then
    if [[ "$(uname -m)" == x86_64 && -z "${MACOSX_DEPLOYMENT_TARGET:-}" ]]; then
      # Xcode 16.4's SDK exposes strchrnul as macOS 15.4+, and PG17 builds
      # with -Werror=unguarded-availability-new on Intel macOS.
      export MACOSX_DEPLOYMENT_TARGET=15.4
    fi
    add_homebrew_flags
  fi
  ( cd "${PG_SRC_DIR}" && ./configure "${CONFIGURE_FLAGS[@]}" )
}

# `world-bin`, not the default target: the server, the client programs and contrib, without the
# documentation. The default target leaves contrib out, and with it pgcrypto, unaccent and every
# other module the schema engine creates. The Windows archive is built the same way.
build_postgres() {
  info "=== Step 4: Build (${JOBS} jobs) ==="
  make -C "${PG_SRC_DIR}" -j"${JOBS}" world-bin
  info "=== Step 5: Staged install ==="
  rm -rf "${STAGE_DIR}"
  make -C "${PG_SRC_DIR}" install-world-bin DESTDIR="${STAGE_DIR}"
}

# Every extension installs through PGXS into the server's $libdir (lib/postgresql/), not lib/,
# where the server never looks: copied there, no bundled extension could be created, and
# preloading supatype_mask, which the CLI does whenever the library is in the archive, stopped
# the server starting.
build_extensions() {
  if [[ "${IS_CROSS}" == true ]]; then
    # A native archive without the masking library is a database whose field rules do not
    # hold, so this must be loud rather than a silent skip. Without pg_keyspace it is only
    # smaller: `supatype dev` falls back to the Valkey sidecar.
    warn "Extensions cannot be cross-compiled, so pg_guard, supatype_mask, pg_keyspace and every"
    warn "upstream extension are skipped. The archive will NOT enforce field-level masking and"
    warn "realtime cannot stream database changes. Do not ship it."
    return 0
  fi
  [[ -x "${PG_CONFIG}" ]] || die "pg_config not found at ${PG_CONFIG}"
  local name
  info "=== Step 6: In-repo C extensions ==="
  for name in pg_guard supatype_mask; do
    [[ -d "${REPO_ROOT}/extensions/${name}" ]] || die "extensions/${name} not found; repo may be incomplete."
    info "${name}"
    build_pgxs "${REPO_ROOT}/extensions/${name}"
  done
  info "=== Step 7: Upstream extensions ==="
  # A fresh source directory, as the script requires, like the staged tree Step 5 replaced.
  rm -rf "${BUILD_DIR}/native-extensions"
  BUILD_DIR="${BUILD_DIR}" JOBS="${JOBS}" \
    bash "${SCRIPT_DIR}/build-native-extensions.sh" "${PG_CONFIG}" "${HOST_PLATFORM}"
  build_keyspace
}

# `cargo pgrx package` builds against --pg-config directly, but still requires PGRX_HOME to
# exist. It must NOT be created with `cargo pgrx init`: init runs initdb, which fails against a
# staged (DESTDIR) Postgres whose binaries resolve libpq at an install prefix that does not exist
# yet. So write a minimal PGRX_HOME that registers this pg_config, under BUILD_DIR rather than
# ~/.pgrx, which may already point at a developer's own Postgres. write_pgrx_home <pg-major>.
write_pgrx_home() {
  export PGRX_HOME="${BUILD_DIR}/pgrx-home"
  mkdir -p "${PGRX_HOME}"
  printf '[configs]\npg%s = "%s"\n' "$1" "${PG_CONFIG}" >"${PGRX_HOME}/config.toml"
}

# pgrx lays the package tree under target/release/pg_keyspace-pg<major> at the paths pg_config
# reports, which for a staged build are the staged paths (pg_config is relocatable), and those
# are where the library and its SQL go. install_keyspace_package <pg-major>.
install_keyspace_package() {
  local pkglib ext root
  pkglib="$("${PG_CONFIG}" --pkglibdir)"
  ext="$("${PG_CONFIG}" --sharedir)/extension"
  root="${KEYSPACE_DIR}/extension/target/release/pg_keyspace-pg$1"
  find "${root}${pkglib}" \( -name pg_keyspace.so -o -name pg_keyspace.dylib \) -exec cp {} "${pkglib}/" \;
  cp "${root}${ext}/pg_keyspace.control" "${root}${ext}"/pg_keyspace--*.sql "${ext}/"
}

# pg_keyspace (Rust/pgrx) and its keys-only logical decoding plugin, supacache_keys.
build_keyspace() {
  info "=== Step 8: pg_keyspace ==="
  [[ -d "${KEYSPACE_DIR}" ]] || die "extensions/pg_keyspace not found; repo may be incomplete."
  command -v cargo >/dev/null 2>&1 \
    || die "cargo not found. Install Rust (https://rustup.rs); pg_keyspace is a pgrx extension."
  if ! command -v cargo-pgrx >/dev/null 2>&1; then
    info "Installing cargo-pgrx ${PGRX_VERSION}..."
    cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
  fi
  # Read from the Postgres actually built, since PG_VERSION is an override, not a constant.
  local pg_major
  pg_major="$("${PG_CONFIG}" --version | awk '{print $2}' | cut -d. -f1)"
  write_pgrx_home "${pg_major}"
  ( cd "${KEYSPACE_DIR}/extension" && cargo pgrx package --pg-config "${PG_CONFIG}" )
  install_keyspace_package "${pg_major}"
  info "supacache_keys"
  build_pgxs "${KEYSPACE_DIR}/plugin"
}

# Native builds only. On Linux, bundling copies every non-glibc library the archive loads into
# lib/ and points every ELF file at it, so it starts on a distribution other than the build
# machine's. On Linux and macOS alike, the check then starts the tree the way native
# `supatype dev` does.
bundle_and_check() {
  [[ "${IS_CROSS}" == false ]] || return 0
  if [[ "${HOST_PLATFORM}" == linux ]]; then
    info "=== Step 9: Bundle shared libraries ==="
    bash "${SCRIPT_DIR}/bundle-linux-libraries.sh" "${STAGE_ROOT}"
  fi
  info "=== Step 10: Runtime check ==="
  bash "${REPO_ROOT}/tests/native-archive/check-unix-runtime.sh" "${STAGE_ROOT}"
}

# The checksum file names the archive, not a path to it, so `sha256sum --check` works beside
# the downloaded archive.
package_archive() {
  info "=== Step 11: Package archive and checksum ==="
  [[ -x "${STAGE_ROOT}/bin/postgres" ]] || die "no staged tree at ${STAGE_ROOT}; build it first."
  tar -czf "${REPO_ROOT}/${ARCHIVE_NAME}" -C "${STAGE_ROOT}" .
  ( cd "${REPO_ROOT}" && sha256 "${ARCHIVE_NAME}" >"${ARCHIVE_NAME}.sha256" )
  info "Done. Output files:"
  info "  ${REPO_ROOT}/${ARCHIVE_NAME}"
  info "  ${REPO_ROOT}/${ARCHIVE_NAME}.sha256"
}

build_stage() {
  install_deps
  use_ccache
  fetch_source
  configure_postgres
  build_postgres
  build_extensions
  bundle_and_check
  if [[ "${USE_CCACHE}" == true ]]; then ccache --show-stats; fi
  info "Staged tree: ${STAGE_ROOT}"
}

main() {
  parse_args "$@"
  detect_host
  set_paths
  print_summary
  if [[ "${PHASE}" != package ]]; then build_stage; fi
  if [[ "${PHASE}" != stage ]]; then package_archive; fi
}

main "$@"
