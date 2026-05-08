# build-native-windows.ps1
# Build the supatype-postgres Windows archive for local development using MSYS2.
# Mirrors the build-windows job in .github/workflows/native-archives.yml.
#
# Prerequisites:
#   - MSYS2 installed at C:\msys64  (https://www.msys2.org/)
#   - Run once per machine before the CDN has live releases.
#
# Usage (PowerShell):
#   .\scripts\build-native-windows.ps1
#
# Usage (Git Bash):
#   powershell.exe -ExecutionPolicy Bypass -File scripts/build-native-windows.ps1

[CmdletBinding()]
param(
    [string]$Msys2Root = "C:\msys64"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$PG_VERSION   = "17"
$ARCHIVE_NAME = "supatype-pg-$PG_VERSION-windows-amd64.zip"

$CacheDir  = Join-Path $env:USERPROFILE ".supatype\cache\postgres\$PG_VERSION"
$CacheDest = Join-Path $CacheDir $ARCHIVE_NAME

# ── Sanity checks ─────────────────────────────────────────────────────────────

if (Test-Path $CacheDest) {
    Write-Host "[supatype-postgres] Already cached: $CacheDest"
    Write-Host "  Delete it and re-run to force a fresh build."
    exit 0
}

$Bash = Join-Path $Msys2Root "usr\bin\bash.exe"
if (-not (Test-Path $Bash)) {
    Write-Error (
        "MSYS2 not found at $Msys2Root.`n" +
        "Install MSYS2 from https://www.msys2.org/ then re-run this script.`n" +
        "Or specify a custom path: -Msys2Root 'D:\msys64'"
    )
    exit 1
}

# Resolve the repo root (two dirs above this script).
$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
# When called without PSScriptRoot (e.g. from Git Bash), fall back to cwd.
if (-not $RepoRoot -or -not (Test-Path (Join-Path $RepoRoot "extensions"))) {
    $RepoRoot = (Get-Location).Path
}

$TmpDir = Join-Path $env:TEMP "supatype-pg-build-$(New-Guid)"
New-Item -ItemType Directory -Path $TmpDir | Out-Null

# Convert Windows paths to MSYS2 Unix paths.
function ToMsysPath([string]$wp) {
    $wp = $wp -replace '\\', '/'
    if ($wp -match '^([A-Za-z]):(.*)') {
        return '/' + $Matches[1].ToLower() + $Matches[2]
    }
    return $wp
}

$MsysTmp  = ToMsysPath $TmpDir
$MsysRepo = ToMsysPath $RepoRoot

# ── Helper: run a command inside MSYS2 MINGW64 ───────────────────────────────
function Invoke-Msys2([string]$Script) {
    $wrapped = "export MSYSTEM=MINGW64; source /etc/profile; set -euo pipefail; $Script"
    & $Bash --login -c $wrapped
    if ($LASTEXITCODE -ne 0) {
        throw "MSYS2 command failed (exit $LASTEXITCODE)"
    }
}

try {
    # ── Step 1: Install build dependencies ───────────────────────────────────
    Write-Host "[supatype-postgres] Installing MSYS2 packages (this may take a minute on first run)..."
    Invoke-Msys2 "pacman -S --noconfirm --needed mingw-w64-x86_64-postgresql mingw-w64-x86_64-gcc make base-devel git 2>&1 | tail -5"

    # ── Step 2: Build pg_guard ────────────────────────────────────────────────
    Write-Host "[supatype-postgres] Building pg_guard..."
    Invoke-Msys2 "make -C '$MsysRepo/extensions/pg_guard' PG_CONFIG=/mingw64/bin/pg_config"

    # ── Step 2b: Install pgvector ─────────────────────────────────────────────
    # Strategy: try the MSYS2 package first (always ABI-compatible with the
    # installed Postgres version). Fall back to building from HEAD if the
    # package isn't available yet (e.g. new PG major release).
    Write-Host "[supatype-postgres] Installing pgvector..."
    $PgvectorDist = "$MsysTmp/pgvector-dist"
    Invoke-Msys2 "mkdir -p '$PgvectorDist/lib' '$PgvectorDist/share'"

    try { Invoke-Msys2 "pacman -S --noconfirm --needed mingw-w64-x86_64-pgvector" } catch {}

    $hasPackage = (& $Bash --login -c "export MSYSTEM=MINGW64; source /etc/profile; [ -f /mingw64/lib/postgresql/vector.dll ] && echo yes || echo no" 2>$null).Trim() -eq "yes"

    if ($hasPackage) {
        Write-Host "[supatype-postgres] pgvector: using MSYS2 package"
        Invoke-Msys2 "cp /mingw64/lib/postgresql/vector.dll '$PgvectorDist/lib/'"
        Invoke-Msys2 "cp /mingw64/share/postgresql/extension/vector.control '$PgvectorDist/share/'"
        Invoke-Msys2 "find /mingw64/share/postgresql/extension -name 'vector--*.sql' -exec cp {} '$PgvectorDist/share/' \;"
    } else {
        Write-Host "[supatype-postgres] pgvector: building from HEAD (MSYS2 package unavailable)"
        $PgvectorSrc = "$MsysTmp/pgvector-src"
        Invoke-Msys2 "git clone --depth 1 https://github.com/pgvector/pgvector.git '$PgvectorSrc'"
        Invoke-Msys2 "make -C '$PgvectorSrc' PG_CONFIG=/mingw64/bin/pg_config -j4"
        Invoke-Msys2 "find '$PgvectorSrc' -name 'vector.dll' -exec cp {} '$PgvectorDist/lib/' \;"
        Invoke-Msys2 "cp '$PgvectorSrc/vector.control' '$PgvectorDist/share/'"
        Invoke-Msys2 "find '$PgvectorSrc' -name 'vector--*.sql' -exec cp {} '$PgvectorDist/share/' \;"
    }

    # ── Step 2c: Clone pgjwt (SQL-only, no compilation needed) ───────────────
    Write-Host "[supatype-postgres] Cloning pgjwt..."
    $PgjwtCommit = "f3d82fd30151e754e19ce5d6a06c71c20689ce3d"
    Invoke-Msys2 "git clone https://github.com/michelp/pgjwt.git '$MsysTmp/pgjwt'"
    Invoke-Msys2 "git -C '$MsysTmp/pgjwt' checkout $PgjwtCommit"

    # ── Step 3: Stage archive contents ───────────────────────────────────────
    Write-Host "[supatype-postgres] Staging archive..."
    $StageScript = @"
STAGING='$MsysTmp/staging'
mkdir -p "`$STAGING/bin" "`$STAGING/lib/postgresql" "`$STAGING/share/postgresql"

# PostgreSQL executables
for exe in postgres pg_ctl initdb psql pg_dump pg_restore \
           createdb dropdb createuser dropuser pg_isready \
           pg_basebackup ecpg; do
    [ -f "/mingw64/bin/`${exe}.exe" ] && cp "/mingw64/bin/`${exe}.exe" "`$STAGING/bin/"
done

# Runtime DLLs (MSYS2/MINGW64 only -- skip Windows system DLLs)
for exe in "`$STAGING/bin/"*.exe; do
    ldd "`$exe" 2>/dev/null \
      | awk '/\/mingw64\/bin\// { print `$3 }' \
      | while read dll; do
          [ -f "`$dll" ] && cp -n "`$dll" "`$STAGING/bin/"
        done
done

# PostgreSQL extension DLLs (encoding conversions, etc.)
cp -r /mingw64/lib/postgresql/. "`$STAGING/lib/postgresql/"

# pg_guard extension DLL (overwrites any stale copy)
find '$MsysRepo/extensions/pg_guard' -name 'pg_guard.dll' \
    -exec cp {} "`$STAGING/lib/postgresql/" \;

# Share data (timezone tables etc. -- required by initdb)
cp -r /mingw64/share/postgresql/. "`$STAGING/share/postgresql/"

# pgvector extension DLL + SQL files (from dist dir populated in step 2b)
if [ -f '$MsysTmp/pgvector-dist/lib/vector.dll' ]; then
    cp '$MsysTmp/pgvector-dist/lib/vector.dll' "`$STAGING/lib/postgresql/"
    mkdir -p "`$STAGING/share/postgresql/extension"
    cp '$MsysTmp/pgvector-dist/share/vector.control' "`$STAGING/share/postgresql/extension/"
    find '$MsysTmp/pgvector-dist/share' -name 'vector--*.sql' \
        -exec cp {} "`$STAGING/share/postgresql/extension/" \;
fi

# pgjwt SQL + control files (SQL-only, no DLL)
cp '$MsysTmp/pgjwt/pgjwt.control' "`$STAGING/share/postgresql/extension/"
find '$MsysTmp/pgjwt' -name 'pgjwt--*.sql' \
    -exec cp {} "`$STAGING/share/postgresql/extension/" \;
"@
    Invoke-Msys2 $StageScript

    # ── Step 4: Create zip archive ────────────────────────────────────────────
    Write-Host "[supatype-postgres] Creating $ARCHIVE_NAME..."
    $StagingDir = Join-Path $TmpDir "staging"
    $OutZip     = Join-Path $TmpDir $ARCHIVE_NAME
    Compress-Archive -Path "$StagingDir\*" -DestinationPath $OutZip

    # ── Step 5: Place in supatype cache ───────────────────────────────────────
    New-Item -ItemType Directory -Path $CacheDir -Force | Out-Null
    Copy-Item -Path $OutZip -Destination $CacheDest

    Write-Host ""
    Write-Host "[supatype-postgres] Done. Archive placed in supatype cache:"
    Write-Host "  $CacheDest"
    Write-Host ""
    Write-Host "Run 'supatype dev' from your project (provider = `"native`" in config)."
} finally {
    Remove-Item -Path $TmpDir -Recurse -Force -ErrorAction SilentlyContinue
}
