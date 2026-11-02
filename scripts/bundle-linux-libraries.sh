#!/usr/bin/env bash
# bundle-linux-libraries.sh: make a staged Linux archive self-contained.
#
# Every shared library a program or module in the archive loads, other than the C and C++
# runtimes every glibc system has, is copied into the archive's lib/, and every ELF file in the
# archive gets a RUNPATH that finds lib/ relative to itself ($ORIGIN). Without this the archive
# loads whatever ICU, readline, OpenSSL, GEOS or PROJ the build machine had, by soname: it needed
# Ubuntu 22.04's libicu70 and libreadline8 and did not start on Fedora 40.
#
# The glibc floor is the build machine's (Ubuntu 22.04, glibc 2.35): glibc itself, libgcc_s and
# libstdc++ are left to the host, which has them at that version or later.
#
# Run it after everything is installed into the tree. It fails when any file still resolves a
# non-baseline library from outside the archive.
#
# Needs patchelf. Usage: bundle-linux-libraries.sh <archive-root>
set -euo pipefail

root=$(cd "$1" && pwd)
lib="${root}/lib"
baseline='^(linux-vdso|ld-linux[^ ]*|libc|libm|libdl|libpthread|librt|libresolv|libutil|libanl|libgcc_s|libstdc\+\+)\.so'

info() { echo "[bundle-linux-libraries] $*"; }
# shellcheck source=scripts/lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
command -v patchelf >/dev/null || die "patchelf is required"

# elf_files: every dynamically linked ELF file in the archive, one per line.
elf_files() {
	local f
	while IFS= read -r -d '' f; do
		# Captured, not piped into `grep -q`: under pipefail an early grep exit fails the pipe.
		[[ $(readelf -d "$f" 2>/dev/null) == *"(NEEDED)"* ]] && printf '%s\n' "$f"
	done < <(find "${root}" -type f -print0)
}

# outside_dependencies: "path" for every library some file in the archive resolves from outside
# it that is not baseline. ldd output is captured before it is filtered, so pipefail never hides
# a line.
outside_dependencies() {
	local resolved
	mapfile -t files < <(elf_files)
	resolved=$(ldd "${files[@]}" 2>/dev/null || true)
	awk -v root="${root}/" -v baseline="${baseline}" '
		$2 == "=>" && $3 ~ /^\// && index($3, root) != 1 {
			n = split($3, parts, "/"); if (parts[n] !~ baseline) print $3
		}
		/not found/ { print "MISSING " $1 }
	' <<<"${resolved}" | sort -u
}

info "copying the libraries the archive loads from the build machine into lib/"
# Repeat until nothing new turns up: a copied library can need another.
while :; do
	added=0
	while IFS= read -r dep; do
		case "${dep}" in MISSING*) die "unresolved dependency on the build machine: ${dep#MISSING }" ;; esac
		[ -e "${lib}/${dep##*/}" ] && continue
		cp -L "${dep}" "${lib}/"
		chmod u+w "${lib}/${dep##*/}"
		echo "  ${dep##*/}"
		added=1
	done < <(LD_LIBRARY_PATH="${lib}" outside_dependencies)
	[ "${added}" -eq 1 ] || break
done

info "setting RUNPATH on every ELF file"
while IFS= read -r f; do
	rel=$(realpath --relative-to="$(dirname "$f")" "${lib}")
	patchelf --set-rpath "\$ORIGIN/${rel}" "$f"
done < <(elf_files)

# The proof: with no LD_LIBRARY_PATH at all, nothing non-baseline may resolve from outside.
leaks=$(env -u LD_LIBRARY_PATH bash -c "$(declare -f elf_files outside_dependencies); root='${root}' baseline='${baseline}' outside_dependencies")
[ -z "${leaks}" ] || die "still loaded from outside the archive: ${leaks}"
info "every program and module loads only baseline libraries from the host"

# Last, because it is a script rather than ELF: bin/postgres becomes scripts/linux-postgres-wrapper.sh,
# which points the archive's OpenSSL at the host's CA certificates, and the server moves to
# bin/postgres.bin beside it. Run again on the same tree, it only refreshes the wrapper.
info "wrapping bin/postgres so pg_net's HTTPS finds the host's CA certificates"
[ -e "${root}/bin/postgres.bin" ] || mv "${root}/bin/postgres" "${root}/bin/postgres.bin"
install -m 755 "$(dirname "${BASH_SOURCE[0]}")/linux-postgres-wrapper.sh" "${root}/bin/postgres"
