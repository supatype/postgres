#!/bin/bash
# Build, install and run the regression suite against a throwaway PG17 cluster.
# Used by CI and by the Docker one-liner in the README.
set -e

PG_BIN=${PG_BIN:-/usr/lib/postgresql/17/bin}
# Not ${PGDATA:-...}: the postgres image already exports a PGDATA that points at a
# cluster this script must not touch.
export PGDATA=${MASK_TEST_PGDATA:-/tmp/pgdata}
export PGPORT=${PGPORT:-5555}

# The server cannot run as root, so a containerised run installs as root and then
# re-enters here unprivileged.
[ -n "$SKIP_INSTALL" ] || make -s install

[ -d "$PGDATA" ] || "$PG_BIN/initdb" -D "$PGDATA" -U postgres -A trust >/dev/null

# Idempotent, so the suite can be run twice in one container.
"$PG_BIN/pg_ctl" -D "$PGDATA" status >/dev/null 2>&1 ||
  "$PG_BIN/pg_ctl" -D "$PGDATA" -o "-p $PGPORT" -l /tmp/pg.log -w start >/dev/null

# Debian ships pg_regress under pgxs rather than in bindir.
PG_REGRESS=${PG_REGRESS:-$(dirname "$("$PG_BIN/pg_config" --pgxs)")/../../src/test/regress/pg_regress}

"$PG_REGRESS" \
  --bindir="$PG_BIN" \
  --inputdir=test --outputdir="${OUTDIR:-/tmp/out}" \
  --port="$PGPORT" --user=postgres \
  --dbname=masktest --load-extension=supatype_mask \
  "$@"
