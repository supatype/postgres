#!/bin/bash

set -eEuo pipefail

export PGPASSWORD=postgres
export PGUSER=supatype_admin
export PGHOST=localhost
export PGDATABASE=postgres

ARTIFACTS_BUCKET_NAME=${1:-}
if [ -z "$ARTIFACTS_BUCKET_NAME" ]; then
  echo "Usage: $0 <ARTIFACTS_BUCKET_NAME> [INITIAL_PG_VERSION]"
  exit 1
fi

INITIAL_PG_VERSION=${2:-15.1.1.60}
LATEST_PG_VERSION=$(sed -e 's/postgres-version = "\(.*\)"/\1/g' ../../common.vars.pkr.hcl)

LATEST_VERSION_SCRIPTS="scripts/pg_upgrade_scripts-${LATEST_PG_VERSION}.tar.gz"
LATEST_VERSION_BIN="scripts/pg_upgrade_bin-${LATEST_PG_VERSION}.tar.gz"

if [ ! -f "$LATEST_VERSION_SCRIPTS" ]; then
  aws s3 cp "s3://${ARTIFACTS_BUCKET_NAME}/upgrades/postgres/supatype-postgres-${LATEST_PG_VERSION}/pg_upgrade_scripts.tar.gz" "$LATEST_VERSION_SCRIPTS"
fi

if [ ! -f "$LATEST_VERSION_BIN" ]; then
  aws s3 cp "s3://${ARTIFACTS_BUCKET_NAME}/upgrades/postgres/supatype-postgres-${LATEST_PG_VERSION}/24.04.tar.gz" "$LATEST_VERSION_BIN"
fi

rm -rf scripts/pg_upgrade_scripts
cp "$LATEST_VERSION_SCRIPTS" scripts/pg_upgrade_scripts.tar.gz
cp "$LATEST_VERSION_BIN" scripts/pg_upgrade_bin.tar.gz

docker rm -f pg_upgrade_test || true

docker run -t --name pg_upgrade_test --env-file .env \
   -v "$(pwd)/scripts:/tmp/upgrade" \
   --entrypoint /tmp/upgrade/entrypoint.sh -d \
   -p 5432:5432 \
   "supatype/postgres:${INITIAL_PG_VERSION}"

sleep 3
while ! docker exec -it pg_upgrade_test bash -c "pg_isready"; do
  echo "Waiting for postgres to start..."
  sleep 1
done

echo "Running migrations"
docker cp ../../migrations/db/migrations "pg_upgrade_test:/docker-entrypoint-initdb.d/"

# The image installs migrate.sh as 99-supatype-migrate.sh, so that the stock
# entrypoint runs it last. Older published images shipped it under its own name,
# and INITIAL_PG_VERSION decides which one is in this container -- so find it
# rather than assuming either. Hard-coding the bare name meant this step failed
# against every image this repository has ever built.
# `if !` rather than a trailing `$?` test: this script runs under `set -e`, so a
# failing `docker exec` aborts it before any such test is reached, and the log
# that would say why is never printed.
if ! docker exec -i pg_upgrade_test bash -c '
  set -eu
  for f in /docker-entrypoint-initdb.d/99-supatype-migrate.sh \
           /docker-entrypoint-initdb.d/migrate.sh; do
    if [ -x "$f" ]; then
      echo "Using $f"
      "$f" > /tmp/migrate.log 2>&1
      exit $?
    fi
  done
  echo "No migration script in /docker-entrypoint-initdb.d/ (looked for" \
       "99-supatype-migrate.sh and migrate.sh):" >&2
  ls -la /docker-entrypoint-initdb.d/ >&2
  exit 1
'; then
  echo "Running migrations failed. Exiting."
  docker exec -i pg_upgrade_test bash -c 'cat /tmp/migrate.log' || true
  exit 1
fi

echo "Running tests"
pg_prove "../../migrations/tests/test.sql"
psql -f "./tests/97-enable-extensions.sql"
psql -f "./tests/98-data-fixtures.sql"
psql -f "./tests/99-fixtures.sql"

echo "Initiating pg_upgrade"
docker exec -it pg_upgrade_test bash -c '/tmp/upgrade/pg_upgrade_scripts/initiate.sh "$PG_MAJOR_VERSION"; exit $?'
if [ $? -ne 0 ]; then
  echo "Initiating pg_upgrade failed. Exiting."
  exit 1
fi

sleep 3
echo "Completing pg_upgrade"
docker exec -it pg_upgrade_test bash -c 'rm -f /tmp/pg-upgrade-status; /tmp/upgrade/pg_upgrade_scripts/complete.sh; exit $?'
if [ $? -ne 0 ]; then
  echo "Completing pg_upgrade failed. Exiting."
  exit 1
fi

pg_prove tests/01-schema.sql
pg_prove tests/02-data.sql
pg_prove tests/03-settings.sql

