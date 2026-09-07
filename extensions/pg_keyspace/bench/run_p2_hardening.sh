#!/usr/bin/env bash
# P2 hardening — RESP AUTH secrets are hashed at rest (salted SHA-256, constant-
# time verify) and credentials hot-reload on SIGHUP with no worker restart.
set -u
PGPORT=${PGPORT:-5434}
RESP=${RESP:-6381}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
R="redis-cli -p $RESP"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
reload() { $P -c "SELECT pg_reload_conf();" >/dev/null 2>&1; sleep 1; }

echo "# P2 hardening — hashed AUTH secrets + hot reload"

# clean slate: remove any creds, reload -> no-auth mode
$P -c "TRUNCATE supacache.resp_credential;" >/dev/null 2>&1; reload

# 1. register a credential via the helper — plaintext must NOT be stored
$P -c "SELECT supacache.set_credential('alice','s3cret-alice','service_role','');" >/dev/null 2>&1
stored="$($P -c "SELECT secret FROM supacache.resp_credential WHERE username='alice';")"
chk "secret stored as salted SHA-256, not plaintext" \
    "1" "$(echo "$stored" | grep -Eq '^sha256\$[0-9a-f]+\$[0-9a-f]{64}$' && echo 1 || echo 0)"
chk "plaintext secret does NOT appear in the table" \
    "0" "$(echo "$stored" | grep -c 's3cret-alice')"

# 2. hot reload: creds were 0 at worker start; reload makes AUTH enforced now
reload
chk "before AUTH: keyed command is refused (NOAUTH)" \
    "1" "$($R GET foo 2>&1 | grep -ci 'NOAUTH\|auth')"
chk "AUTH with correct password succeeds" \
    "OK" "$($R AUTH alice 's3cret-alice' 2>&1)"
chk "AUTH with wrong password is refused (WRONGPASS)" \
    "1" "$($R AUTH alice 'wrong' 2>&1 | grep -ci 'WRONGPASS')"
# authed session can use the keyspace (AUTH + commands on one connection;
# redis-cli -a does single-arg AUTH as user "default", so drive it via stdin)
getset="$({ echo "AUTH alice s3cret-alice"; echo "SET hk bar"; echo "GET hk"; } | $R 2>&1 | tail -1)"
chk "authed client can SET/GET" "bar" "$getset"

# 3. hot reload a NEW credential without restarting the worker
$P -c "SELECT supacache.set_credential('bob','s3cret-bob','service_role','');" >/dev/null 2>&1
chk "new cred not usable before reload" \
    "1" "$($R AUTH bob 's3cret-bob' 2>&1 | grep -ci 'WRONGPASS')"
reload
chk "new cred works after pg_reload_conf (no restart)" \
    "OK" "$($R AUTH bob 's3cret-bob' 2>&1)"

# 4. hot-remove a credential
$P -c "DELETE FROM supacache.resp_credential WHERE username='bob';" >/dev/null 2>&1; reload
chk "removed cred rejected after reload" \
    "1" "$($R AUTH bob 's3cret-bob' 2>&1 | grep -ci 'WRONGPASS')"
chk "surviving cred still works" \
    "OK" "$($R AUTH alice 's3cret-alice' 2>&1)"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
