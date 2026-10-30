# build-native-windows.ps1
# Build the supatype-postgres Windows archive locally, with MSYS2.
#
# Runs scripts/build-native-windows.sh, the script the build-windows job in
# .github/workflows/native-archives.yml runs, then zips the tree the same way CI does.
#
# Prerequisites:
#   - MSYS2 installed at C:\msys64 (https://www.msys2.org/), or pass -Msys2Root.
#
# Usage (PowerShell):
#   .\scripts\build-native-windows.ps1 [-OutDir .\dist] [-Msys2Root C:\msys64]
#
# The result is <OutDir>\supatype-pg-17-windows-amd64.zip and the unpacked tree beside it in
# <OutDir>\supatype-pg. To use the tree with `supatype dev`, set
# overrides: { postgres_dir: "<OutDir>\\supatype-pg" } in the project's supatype.config.ts.
# Nothing is written to the CLI's download cache. (This script used to copy the zip into
# ~/.supatype/cache/postgres/17, a directory the CLI never reads.)

[CmdletBinding()]
param(
    [string]$Msys2Root = "C:\msys64",
    [string]$OutDir = (Join-Path (Get-Location) "dist")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$ArchiveName = "supatype-pg-17-windows-amd64.zip"
$RepoRoot = Split-Path -Parent $PSScriptRoot

$Bash = Join-Path $Msys2Root "usr\bin\bash.exe"
if (-not (Test-Path $Bash)) {
    throw "MSYS2 not found at $Msys2Root. Install it from https://www.msys2.org/ or pass -Msys2Root."
}

# Windows paths to MSYS2 paths (C:\a\b -> /c/a/b).
function ConvertTo-MsysPath([string]$WindowsPath) {
    $p = $WindowsPath -replace '\\', '/'
    if ($p -match '^([A-Za-z]):(.*)') { return '/' + $Matches[1].ToLower() + $Matches[2] }
    return $p
}

$Stage = Join-Path $OutDir "supatype-pg"
if (Test-Path $Stage) {
    throw "$Stage already exists. Remove it, or pass a different -OutDir."
}
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

# The same packages the CI job installs.
$Packages = @(
    "mingw-w64-x86_64-gcc", "mingw-w64-x86_64-binutils", "mingw-w64-x86_64-pkgconf",
    "mingw-w64-x86_64-icu", "mingw-w64-x86_64-openssl", "mingw-w64-x86_64-zstd",
    "mingw-w64-x86_64-zlib", "mingw-w64-x86_64-readline",
    "mingw-w64-x86_64-geos", "mingw-w64-x86_64-proj", "mingw-w64-x86_64-json-c",
    "mingw-w64-x86_64-libxml2",
    "make", "bison", "flex", "diffutils", "perl", "git", "curl", "tar"
) -join " "

$env:MSYSTEM = "MINGW64"
$env:CHERE_INVOKING = "1"
Push-Location $RepoRoot
# The two bash calls below are judged by their exit code. Under "Stop", Windows PowerShell 5.1
# turns any line they write to stderr into a terminating error once output is redirected
# (`*> build.log`), so pacman's "is up to date -- skipping" warning ended the build.
$ErrorActionPreference = "Continue"
try {
    Write-Host "[supatype-postgres] Installing the MSYS2 build packages..."
    & $Bash -lc "pacman -S --noconfirm --needed $Packages"
    if ($LASTEXITCODE -ne 0) { throw "pacman failed (exit $LASTEXITCODE)" }

    Write-Host "[supatype-postgres] Building (this takes a while)..."
    & $Bash -lc "bash scripts/build-native-windows.sh '$(ConvertTo-MsysPath $Stage)'"
    if ($LASTEXITCODE -ne 0) { throw "build-native-windows.sh failed (exit $LASTEXITCODE)" }
} finally {
    Pop-Location
    $ErrorActionPreference = "Stop"
}

$Zip = Join-Path $OutDir $ArchiveName
Compress-Archive -Path "$Stage\*" -DestinationPath $Zip -Force

Write-Host ""
Write-Host "[supatype-postgres] Done."
Write-Host "  Archive: $Zip"
Write-Host "  Tree:    $Stage"
Write-Host 'Use the tree with supatype dev through overrides.postgres_dir in supatype.config.ts.'
