#!/bin/bash
# A cluster in recovery says so, and still comes up on promotion.
#
# Every pg_keyspace background worker asks for SPI, and pgrx's
# `enable_spi_access()` registers such a worker with
# `BgWorkerStartTime::RecoveryFinished` -- SPI is not reachable before recovery
# ends. On a streaming standby recovery never ends, so Postgres never launches
# any of them. Nothing errors, because nothing failed: they are queued for a
# state that will not arrive. The whole symptom is a RESP port that never
# answers, with not one line in the log to say why.
#
# This asserts the notice is there, that it is accurate (the port really does
# not answer on the standby), and -- the half that matters most -- that the
# deferral is only a deferral: promote the standby and the workers start on
# their own and serve the keyspace they inherited.
#
# Expects cargo, cargo-pgrx, a PGDG PostgreSQL and redis-cli on PATH. Creates
# and destroys clusters under $PGDATA and $SBDATA, so point those somewhere
# disposable.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-standby-primary}
SBDATA=${PGKS_SBDATA:-/tmp/pgks-standby-replica}
PORT=${PGKS_PG_PORT:-5483}
SPORT=${PGKS_SB_PG_PORT:-5484}
RESP=${PGKS_RESP_PORT:-6483}
SRESP=${PGKS_SB_RESP_PORT:-6484}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0

chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p "$1" -U postgres -d postgres -tAc "$2" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $1 -l $1/log -o \"-p $2 -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $1 -m fast -w stop" >/dev/null 2>&1; }
wait_sql() { for _ in $(seq 1 60); do psql_ "$1" "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
# Deliberately bounded rather than open-ended: on the standby the expected
# answer is "never", so this doubles as the measurement.
wait_resp() { for _ in $(seq 1 "${2:-25}"); do redis-cli -p "$1" PING 2>/dev/null | grep -q PONG && return 0; sleep 1; done; return 1; }

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/standby-install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/standby-install.log; exit 1; }

echo "=== primary ==="
rm -rf $PGDATA $SBDATA; mkdir -p $PGDATA $SBDATA
chown postgres:postgres $PGDATA $SBDATA; chmod 700 $PGDATA $SBDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.keys = 10000"
  echo "wal_level = replica"
  echo "max_wal_senders = 4"
  echo "hot_standby = on"
} >> $PGDATA/postgresql.conf
echo "host replication all 127.0.0.1/32 trust" >> $PGDATA/pg_hba.conf
start_pg $PGDATA $PORT; wait_sql $PORT
psql_ $PORT "CREATE EXTENSION pg_keyspace" >/dev/null
stop_pg $PGDATA; start_pg $PGDATA $PORT; wait_sql $PORT
wait_resp $RESP 40 || { echo "FAIL  primary RESP never answered"; exit 1; }

chk "the primary serves RESP"          "PONG" "$(redis-cli -p $RESP PING 2>&1)"
chk "and says nothing about recovery"  "0"    "$(grep -c 'starting in recovery' $PGDATA/log 2>/dev/null | head -1)"
for i in $(seq 1 10); do redis-cli -p $RESP SET "sb:k$i" "v$i" >/dev/null 2>&1; done
sleep 2
chk "10 keys persisted before the backup" "10" \
    "$(psql_ $PORT "SELECT count(*) FROM supacache.kv WHERE key LIKE 'sb:%'::bytea")"

echo ""
echo "########## A. a streaming standby says why it will not serve ##########"
su postgres -c "$PGBIN/pg_basebackup -h /tmp -p $PORT -U postgres -D $SBDATA -X stream -c fast -R" \
  >/tmp/standby-basebackup.log 2>&1
# pg_ctl writes its log inside the data directory, so the backup brought the
# primary's along; every log assertion below would otherwise count its lines.
rm -f $SBDATA/log
{ echo "port = $SPORT"; echo "pg_keyspace.port = $SRESP"; echo "hot_standby = on"; } >> $SBDATA/postgresql.conf
chown -R postgres:postgres $SBDATA; chmod 700 $SBDATA
start_pg $SBDATA $SPORT
wait_sql $SPORT || { echo "FAIL  standby did not accept connections"; exit 1; }

chk "the standby is in recovery"        "t" "$(psql_ $SPORT "SELECT pg_is_in_recovery()")"
chk "it replicated the keyspace rows"   "10" \
    "$(psql_ $SPORT "SELECT count(*) FROM supacache.kv WHERE key LIKE 'sb:%'::bytea")"
chk "and it says the workers are deferred" "1" \
    "$(grep -c 'starting in recovery' $SBDATA/log 2>/dev/null | head -1)"
chk "naming the signal file that put it there" "1" \
    "$(grep -c 'standby.signal present' $SBDATA/log 2>/dev/null | head -1)"
# The notice has to be true, not merely present.
if wait_resp $SRESP 20; then
  echo "FAIL  the standby answered RESP, so the notice is wrong"; fail=$((fail+1))
else
  echo "PASS  the standby's RESP port does not answer, as the notice says"; pass=$((pass+1))
fi
chk "and no worker claimed to be listening" "0" \
    "$(grep -c 'RESP listening' $SBDATA/log 2>/dev/null | head -1)"

echo ""
echo "########## B. the deferral is only a deferral ##########"
# The point of the notice is that nothing is broken and no operator action is
# needed. Promote, and the workers must start by themselves.
stop_pg $PGDATA
su postgres -c "$PGBIN/pg_ctl -D $SBDATA promote -w" >/dev/null 2>&1
for _ in $(seq 1 60); do [ "$(psql_ $SPORT "SELECT pg_is_in_recovery()")" = "f" ] && break; sleep 1; done
chk "the promoted cluster is out of recovery" "f" "$(psql_ $SPORT "SELECT pg_is_in_recovery()")"
if wait_resp $SRESP 60; then
  echo "PASS  the workers started on their own after promotion"; pass=$((pass+1))
else
  echo "FAIL  the workers never started after promotion"; fail=$((fail+1))
  tail -20 $SBDATA/log
fi
chk "and it serves the keyspace it inherited" "v7" "$(redis-cli -p $SRESP GET sb:k7 2>&1)"
chk "the worker logged its recovery"          "1" \
    "$(grep -c 'recovered .* keys' $SBDATA/log 2>/dev/null | head -1)"

echo ""
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
stop_pg $SBDATA
[ "$fail" -eq 0 ]
