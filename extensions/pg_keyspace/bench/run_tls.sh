#!/usr/bin/env bash
# hardening — native TLS on the RESP wire. The AUTH password and all
# values are otherwise sent in clear; with pg_keyspace.tls_cert_file +
# tls_key_file set, every RESP connection is wrapped in a rustls TLS session.
#
# Requires the cluster started with TLS configured (see the cert/key GUCs) and a
# TLS-capable redis-cli. CERT points at the server cert for CA verification.
set -u
RESP=${RESP:-6381}
PGPORT=${PGPORT:-5434}
CERT=${CERT:-/tmp/pgks_tls/cert.pem}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
TLS="redis-cli --tls --cacert $CERT --sni localhost -p $RESP"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# TLS on the RESP wire"

# 1. plaintext is refused on the TLS port (handshake fails -> connection closed)
chk "plaintext client is rejected on the TLS port" \
    "1" "$(timeout 5 redis-cli -p $RESP PING 2>&1 | grep -ciE 'closed|error|protocol|reset')"

# 2. TLS handshake + basic commands
chk "TLS PING -> PONG" "PONG" "$(timeout 5 $TLS PING 2>&1)"
chk "TLS SET/GET round-trips" "v1" "$(timeout 5 $TLS SET tk v1 >/dev/null 2>&1; timeout 5 $TLS GET tk 2>&1)"

# 3. CA verification actually validates the server cert (no --insecure)
chk "server cert validates against the CA (verified handshake)" \
    "PONG" "$(timeout 5 redis-cli --tls --cacert $CERT --sni localhost -p $RESP PING 2>&1)"

# 4. AUTH over TLS — the password now travels encrypted
$P -c "SELECT supacache.set_credential('tlsuser','tlspw','service_role','');" >/dev/null 2>&1
$P -c "SELECT pg_reload_conf();" >/dev/null 2>&1; sleep 1
chk "unauth keyed command over TLS -> NOAUTH" \
    "1" "$(timeout 5 $TLS GET foo 2>&1 | grep -ciE 'NOAUTH|auth')"
authset="$({ echo 'AUTH tlsuser tlspw'; echo 'SET ak av'; echo 'GET ak'; } | timeout 5 $TLS 2>&1 | tail -1)"
chk "AUTH over TLS then SET/GET" "av" "$authset"
chk "wrong password over TLS -> WRONGPASS" \
    "1" "$(timeout 5 $TLS AUTH tlsuser nope 2>&1 | grep -ci WRONGPASS)"

# cleanup creds
$P -c "TRUNCATE supacache.resp_credential; SELECT pg_reload_conf();" >/dev/null 2>&1

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
