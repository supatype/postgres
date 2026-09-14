#!/usr/bin/env bash
# RESP TLS from the cluster's own certificate (#12).
#
# TLS on the RESP port was bring-your-own-cert: pg_keyspace.tls_cert_file and
# tls_key_file, supplied and rotated by hand, separately from the certificate
# the operator already supplies and rotates for libpq. pg_keyspace.tls_use_
# postgres_cert lets the RESP port inherit that one instead, so there is no
# second cert to manage.
#
# Four things have to hold, and each is checked against the wire rather than
# against a log line:
#
#   1. the default is unchanged -- plaintext, because switching a port to TLS
#      implicitly would break every plaintext client already on it;
#   2. opting in on a cluster with ssl = off fails closed, because there is no
#      certificate to inherit and quietly serving plaintext is the wrong
#      reading of "encrypt this";
#   3. opting in on a cluster with ssl = on serves TLS, and serves *the same
#      certificate Postgres serves* -- compared by fingerprint, on both ports;
#   4. an explicit pg_keyspace cert still wins, so inheriting cannot silently
#      take over a deployment that configured its own.
#
# Self-contained: builds the extension, creates and destroys its own cluster.
# Override PGBIN, PGDATA, PGKS_PG_PORT, PGKS_RESP_PORT, PGKS_BUILD_PROFILE.
set -uo pipefail
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-tls-data}
PORT=${PGKS_PG_PORT:-5439}
RESP=${PGKS_RESP_PORT:-6399}
PROFILE=${PGKS_BUILD_PROFILE:-release}
pass=0; fail=0
chk() {
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1"; echo "        expected: [$2]"; echo "        actual:   [$3]"; fail=$((fail+1)); fi
}
psql_() { $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
# `pg_ctl -w stop` gives up after its own timeout and returns non-zero with the
# postmaster STILL shutting down. Discarding that status and starting another one
# a second later starts it on top of a live postmaster: that start fails, and
# every readiness poll then reads `FATAL: the database system is shutting down`
# until the loop expires -- surfacing as whichever assertion came next, pointing
# at the feature under test and nothing to do with it (#120). Shutdown length
# tracks how much the persistence worker has to flush, so it bites after a heavy
# section and passes everywhere else. Verify it rather than assume it.
stop_pg() {
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 120 stop -m fast" >/dev/null 2>&1
  for _ in $(seq 1 120); do
    su postgres -c "$PGBIN/pg_ctl -D $PGDATA status" >/dev/null 2>&1 || return 0
    sleep 1
  done
  su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w -t 60 stop -m immediate" >/dev/null 2>&1
  return 0
}
wait_ready() { for _ in $(seq 1 60); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
set_conf() {
  sed -i "s|^$1 = .*|$1 = $2|" $PGDATA/postgresql.conf 2>/dev/null
  grep -q "^$1 " $PGDATA/postgresql.conf || echo "$1 = $2" >> $PGDATA/postgresql.conf
}
del_conf() { sed -i "/^$1 /d" $PGDATA/postgresql.conf; }

# Does a plaintext RESP client get a reply? Plaintext against a TLS listener
# does not fail fast on every stack, so bound the wait.
resp_plain() { timeout 5 redis-cli -h 127.0.0.1 -p $RESP PING 2>&1 | tr -d '[:space:]'; }
# ... and a TLS one? Spoken straight down an openssl tunnel rather than through
# `redis-cli --tls`, because redis-tools is not built with TLS everywhere and a
# harness that cannot run on the CI runner is a harness that rots. RESP PING is
# four bytes and a CRLF, so there is nothing a client library adds here. No cert
# validation: these are self-signed, and this asks whether the port speaks TLS
# at all. *Which* certificate it speaks it with is checked separately, by
# fingerprint, which is the stronger question anyway.
resp_tls() {
  printf 'PING\r\n' | timeout 5 openssl s_client -connect 127.0.0.1:$RESP -quiet 2>/dev/null \
    | tr -d '[:space:]' | sed 's/^+//'
}
# SHA-256 fingerprint of the certificate a port actually serves.
fp_served() { timeout 5 openssl s_client -connect 127.0.0.1:$1 ${2:-} </dev/null 2>/dev/null \
  | openssl x509 -noout -fingerprint -sha256 2>/dev/null | sed 's/.*=//'; }
fp_file() { openssl x509 -in "$1" -noout -fingerprint -sha256 2>/dev/null | sed 's/.*=//'; }
# The worker parks on a refusal instead of exiting, so the evidence is the log.
worker_log() { grep -c "$1" $PGDATA/log 2>/dev/null | tr -d '[:space:]'; }
restart() { stop_pg; sleep 1; : > $PGDATA/log; chown postgres:postgres $PGDATA/log; start_pg; wait_ready; sleep 2; }

echo "=== build + install the extension ==="
cd "$EXT_DIR"
REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/tls_install.log 2>&1 || {
  echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/tls_install.log; exit 1; }
# Say what was built. A harness earlier in this tree measured a .so it had not
# built and reported the old behaviour as the new one.
echo "installed (tls_use_postgres_cert in source: $(grep -c tls_use_postgres_cert src/lib.rs))"

echo "=== initdb ==="
stop_pg
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "listen_addresses = '*'"
} >> $PGDATA/postgresql.conf

# Two distinct self-signed certs. Distinct on purpose: "which cert is being
# served" is the whole question in sections 3 and 4, and two copies of the same
# cert could not tell them apart.
echo "=== certificates ==="
for pair in "pg:the cluster" "resp:pg_keyspace"; do
  n=${pair%%:*}
  openssl req -new -x509 -days 2 -nodes -newkey rsa:2048 \
    -keyout $PGDATA/$n.key -out $PGDATA/$n.crt -subj "/CN=${n}-cert" >/dev/null 2>&1
  chmod 600 $PGDATA/$n.key
done
chown -R postgres:postgres $PGDATA
PG_FP=$(fp_file $PGDATA/pg.crt)
RESP_FP=$(fp_file $PGDATA/resp.crt)
chk "two different certificates were generated" "different" \
    "$([ -n "$PG_FP" ] && [ -n "$RESP_FP" ] && [ "$PG_FP" != "$RESP_FP" ] && echo different || echo "same-or-missing [$PG_FP / $RESP_FP]")"

start_pg; wait_ready || { echo "NO START"; exit 1; }

echo
echo "########## 1. the default is plaintext ##########"
# Nothing TLS-related is set. This is the case every existing deployment is in,
# and the one that must not change.
chk "a plaintext client is served" "PONG" "$(resp_plain)"
chk "and a TLS client is not" "no" \
    "$([ "$(resp_tls)" = "PONG" ] && echo yes || echo no)"

echo
echo "########## 2. inheriting with ssl = off fails closed ##########"
# The operator asked for the cluster's certificate on a cluster that has none.
# ssl_cert_file has a non-empty default ('server.crt') even here, so the value
# of that setting cannot be the test -- `ssl` is.
set_conf "pg_keyspace.tls_use_postgres_cert" "on"
set_conf "ssl" "off"
restart
chk "postgres itself is up (so a refusal is the worker's, not the cluster's)" "1" "$(psql_ 'SELECT 1')"
chk "the worker refuses to serve at all" "no" \
    "$([ "$(resp_plain)" = "PONG" ] && echo yes || echo no)"
chk "and not over TLS either" "no" \
    "$([ "$(resp_tls)" = "PONG" ] && echo yes || echo no)"
chk "the log says why, naming ssl = off" "1" "$(worker_log 'ssl = off')"

echo
echo "########## 3. inheriting the cluster's certificate ##########"
# Relative paths on purpose: ssl_cert_file is resolved against the data
# directory by the server, and inheriting it has to resolve it the same way.
set_conf "ssl" "on"
set_conf "ssl_cert_file" "'pg.crt'"
set_conf "ssl_key_file" "'pg.key'"
# First with the flag OFF. Without this the section proves nothing: a RESP port
# that spoke TLS merely because the cluster does would pass every check below
# with the feature removed.
set_conf "pg_keyspace.tls_use_postgres_cert" "off"
restart
chk "with the flag off, a TLS cluster still leaves RESP plaintext" "PONG" "$(resp_plain)"
chk "and RESP is not speaking TLS yet" "no" \
    "$([ "$(resp_tls)" = "PONG" ] && echo yes || echo no)"
set_conf "pg_keyspace.tls_use_postgres_cert" "on"
restart
chk "postgres is serving TLS" "on" "$(psql_ 'SHOW ssl')"
chk "no pg_keyspace cert is configured" "" "$(psql_ 'SHOW pg_keyspace.tls_cert_file')"
chk "the RESP port now speaks TLS" "PONG" "$(resp_tls)"
chk "and plaintext no longer gets through" "no" \
    "$([ "$(resp_plain)" = "PONG" ] && echo yes || echo no)"
SERVED=$(fp_served $RESP)
chk "the served certificate is the cluster's" "$PG_FP" "$SERVED"
chk "it is not the pg_keyspace-specific one" "no" \
    "$([ "$SERVED" = "$RESP_FP" ] && echo yes || echo no)"
# The point of the feature: one certificate, two ports. Compared on the wire,
# so a rotation that reached only one of them would show up here.
chk "postgres and RESP serve the same certificate" "$(fp_served $PORT '-starttls postgres')" "$SERVED"

echo
echo "########## 4. an explicit pg_keyspace certificate still wins ##########"
# Inheriting must not be able to take over a deployment that configured its own
# cert, even with the inherit flag left on.
set_conf "pg_keyspace.tls_cert_file" "'$PGDATA/resp.crt'"
set_conf "pg_keyspace.tls_key_file" "'$PGDATA/resp.key'"
restart
chk "the inherit flag is still on" "on" "$(psql_ 'SHOW pg_keyspace.tls_use_postgres_cert')"
chk "the RESP port still speaks TLS" "PONG" "$(resp_tls)"
SERVED=$(fp_served $RESP)
chk "the served certificate is the explicit one" "$RESP_FP" "$SERVED"
chk "and not the cluster's" "no" \
    "$([ "$SERVED" = "$PG_FP" ] && echo yes || echo no)"

stop_pg
echo
echo "================================================"
echo "# result: $pass passed, $fail failed"
echo "================================================"
[ "$fail" -eq 0 ]
