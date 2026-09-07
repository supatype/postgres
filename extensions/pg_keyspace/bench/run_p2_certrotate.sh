#!/usr/bin/env bash
# P2 §4.5 — TLS cert rotation: swapping the cert/key files in place and sending
# SIGHUP (pg_reload_conf) makes new connections use the new cert with no restart,
# and a broken cert on reload keeps the current cert (never drops TLS mid-flight).
set -u
RESP=${RESP:-6381}
PGPORT=${PGPORT:-5434}
CERTDIR=${CERTDIR:-/tmp/pgks_tls}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
LOG=${LOG:-/tmp/pgks17/server.log}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-46s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-46s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
fp() { echo | timeout 4 openssl s_client -connect 127.0.0.1:$RESP 2>/dev/null | openssl x509 -noout -fingerprint -sha256 2>/dev/null; }
reload() { $P -c "SELECT pg_reload_conf();" >/dev/null 2>&1; sleep 1; }

echo "# P2 TLS cert rotation (§4.5)"
before="$(fp)"
[ -z "$before" ] && { echo "  SKIP  TLS not enabled on :$RESP"; exit 0; }

# rotate: new cert/key at the SAME paths (the cert-manager model), then SIGHUP
cp "$CERTDIR/cert.pem" "$CERTDIR/cert.prev" 2>/dev/null
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CERTDIR/key.pem" -out "$CERTDIR/cert.pem" \
  -subj "/CN=rotated-$(date +%s)" -days 365 >/dev/null 2>&1
chown postgres:postgres "$CERTDIR"/*.pem 2>/dev/null; chmod 600 "$CERTDIR/key.pem" 2>/dev/null
reload
after="$(fp)"
chk "SIGHUP swapped the served cert (no restart)" "1" "$([ -n "$after" ] && [ "$before" != "$after" ] && echo 1 || echo 0)"
chk "worker logged a TLS cert reload"             "1" "$(grep -c 'reloaded auth.*TLS cert' "$LOG" 2>/dev/null | tail -1 | grep -q '[1-9]' && echo 1 || echo 0)"
chk "TLS still serves after rotation"             "PONG" "$(timeout 4 redis-cli --tls --insecure -p $RESP PING 2>&1)"

# failure case: point the key file at garbage, SIGHUP -> keep the current cert
good="$after"
echo "not a key" > "$CERTDIR/key.pem"; chown postgres:postgres "$CERTDIR/key.pem" 2>/dev/null
reload
chk "broken cert reload keeps serving"            "PONG" "$(timeout 4 redis-cli --tls --insecure -p $RESP PING 2>&1)"
chk "broken cert reload kept the old cert"        "1" "$([ "$(fp)" = "$good" ] && echo 1 || echo 0)"
chk "worker logged the reload failure"            "1" "$(grep -c 'TLS cert reload FAILED' "$LOG" 2>/dev/null | tail -1 | grep -q '[1-9]' && echo 1 || echo 0)"

# restore a good cert so later runs verify cleanly
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$CERTDIR/key.pem" -out "$CERTDIR/cert.pem" \
  -subj "/CN=localhost" -days 365 >/dev/null 2>&1
chown postgres:postgres "$CERTDIR"/*.pem 2>/dev/null; chmod 600 "$CERTDIR/key.pem" 2>/dev/null
reload
rm -f "$CERTDIR/cert.prev"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
