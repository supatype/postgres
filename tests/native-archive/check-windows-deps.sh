#!/usr/bin/env bash
# Asserts that every program and library in an unpacked Windows archive can be loaded on a machine
# with nothing but Windows installed.
#
# For each PE file it lists the DLLs it imports. A DLL Windows itself provides is fine; any other
# one must sit in the archive's `bin/` directory (where Windows looks first, beside the program),
# and must export every name imported from it. The first rule catches a missing DLL, which Windows
# reports as 0xC0000135; the second catches a DLL from a different build than the program, which
# Windows reports as 0xC0000139 ("entry point not found").
#
# Language bridges whose runtime the archive does not ship (Perl, Python, Tcl) are not checked:
# they need that language installed on the machine.
#
# Usage: check-windows-deps.sh <unpacked-archive-dir> [system-dll-dir]
#   Needs `objdump` (binutils). The system DLL directory defaults to C:/Windows/System32.
set -euo pipefail

root=${1%/}
system_dir=${2:-/c/Windows/System32}
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT

language_bridge='^(plperl|plpython3|pltcl|bool_plperl|hstore_plperl|hstore_plpython3|jsonb_plperl|jsonb_plpython3|ltree_plpython3)\.dll$'

[ -d "$root/bin" ] || { echo "FAIL    $root has no bin/ directory"; exit 1; }

# The DLLs Windows provides, by lower-case name.
ls "$system_dir" >"$work/system"

# Every name each bundled program or DLL exports, as "file<TAB>name". Extensions import from
# postgres.exe, so programs count as providers too.
for f in "$root"/bin/*.dll "$root"/bin/*.exe; do
	[ -f "$f" ] || continue
	base=${f##*/}
	objdump -p "$f" | awk -v dll="${base,,}" '
		/^\[Ordinal\/Name Pointer\] Table/ { on = 1; next }
		on && /^\t\[ *[0-9]+\] / { print dll "\t" $NF; next }
		on && !/^\t/ { on = 0 }
	'
	printf '%s\t\n' "${base,,}"
done >"$work/exports"

# Every import of every file checked, as "file<TAB>dll<TAB>name" (name empty for the DLL itself).
for pe in "$root"/bin/*.exe "$root"/bin/*.dll "$root"/lib/postgresql/*.dll; do
	[ -f "$pe" ] || continue
	base=${pe##*/}
	[[ ${base,,} =~ $language_bridge ]] && continue
	objdump -p "$pe" | awk -v pe="${pe#"$root"/}" '
		/^\tDLL Name: / { dll = tolower($3); print pe "\t" dll "\t"; next }
		/^\t[0-9a-f]+\t +[0-9]+ +[^ ]+$/ && dll != "" { print pe "\t" dll "\t" $3 }
	'
done >"$work/imports"

awk -F'\t' '
	FILENAME == ARGV[1] { system_dll[tolower($1)] = 1; next }
	FILENAME == ARGV[2] { bundled[$1] = 1; if ($2 != "") exported[$1 "\t" $2] = 1; next }
	{
		pe = $1; dll = $2; name = $3
		if (!(pe in seen)) { seen[pe] = 1; order[++n] = pe }
		if (dll in bundled) {
			if (name != "" && !((dll "\t" name) in exported)) bad[pe] = bad[pe] " " dll "!" name
		} else if (!(dll in system_dll) && dll !~ /^(api|ext)-ms-win-/) {
			if (index(missing[pe] " ", " " dll " ") == 0) missing[pe] = missing[pe] " " dll
		}
	}
	END {
		failed = 0
		for (i = 1; i <= n; i++) {
			pe = order[i]
			if (pe in missing) print "FAIL    " pe ": missing DLL(s):" missing[pe]
			if (pe in bad) print "FAIL    " pe ": missing export(s):" bad[pe]
			if ((pe in missing) || (pe in bad)) failed++
			else print "ok      " pe
		}
		if (failed) { print failed " file(s) cannot load"; exit 1 }
		print "every program and library can load"
	}
' "$work/system" "$work/exports" "$work/imports"
