#!/usr/bin/env bash
# security tests (§4.7) against the real base: PG17 + supatype_mask + pg_guard
# + pg_keyspace. Assumes the cluster is up on $PGPORT with RESP on $RESP_PORT and
# credentials seeded (see the "seed" section). The load-order and seclabel
# refusal cases require a restart with a changed config and are documented at the
# bottom rather than run here.
set -u
PGPORT=${PGPORT:-5434}
RESP_PORT=${RESP_PORT:-6381}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q"
A() { redis-cli -p "$RESP_PORT" --user "$1" -a "$2" "${@:3}" 2>/dev/null; }
R="redis-cli -p $RESP_PORT"
pass=0; fail=0
check() { # desc, got, want
  if [ "$2" = "$3" ]; then echo "PASS  $1"; pass=$((pass+1));
  else echo "FAIL  $1  (got '[$2]' want '[$3]')"; fail=$((fail+1)); fi
}

# --- seed credentials + ACL (idempotent) ---
$P -c "
INSERT INTO supacache.resp_credential(username,secret,role_name,tenant) VALUES
  ('alice','pw1','tenant_a','ta'),('bob','pw2','tenant_b','tb'),('svc','pw3','service_role','')
ON CONFLICT(username) DO UPDATE SET secret=EXCLUDED.secret,role_name=EXCLUDED.role_name,tenant=EXCLUDED.tenant;
INSERT INTO supacache.acl(role_name,prefix,can_read,can_write) VALUES
  ('tenant_a','session:',true,true),('tenant_b','session:',true,true),('tenant_a','ro:',true,false)
ON CONFLICT(role_name,prefix) DO UPDATE SET can_read=EXCLUDED.can_read,can_write=EXCLUDED.can_write;
SELECT pg_reload_conf();" >/dev/null
# Hot reload (hardening): the worker picks up credential/ACL changes on SIGHUP,
# so pg_reload_conf() applies them with no restart. (secret shown here is stored
# plaintext to exercise the legacy verify path; supacache.set_credential hashes.)
sleep 1
echo

check "unauth data command -> NOAUTH"        "$($R get session:zzz 2>&1 | head -1)" "NOAUTH Authentication required."
check "wrong password -> not authenticated"  "$(redis-cli -p $RESP_PORT --user alice -a wrong get session:zzz 2>/dev/null | head -1)" "NOAUTH Authentication required."
A alice pw1 set session:iso A >/dev/null
check "authed SET+GET own key"               "$(A alice pw1 get session:iso)" "A"
check "tenant isolation: bob cannot see it"  "$(A bob pw2 get session:iso)" ""
check "ACL: read unlisted prefix -> nil"     "$(A alice pw1 get other:1)" ""
check "ACL: write unlisted prefix -> NOPERM" "$(A alice pw1 set other:1 x)" "NOPERM this user has no permissions to access one of the keys used as arguments"
check "ACL: write read-only prefix -> NOPERM" "$(A alice pw1 set ro:1 x)" "NOPERM this user has no permissions to access one of the keys used as arguments"
check "exempt svc sees raw scoped key"        "$(A svc pw3 get ta:session:iso)" "A"

echo
echo "pg_guard reserved-membership (run as a non-superuser role):"
$P -c "CREATE ROLE supacache_admin NOLOGIN;" >/dev/null 2>&1 || true
$P -c "DROP ROLE IF EXISTS attacker; CREATE ROLE attacker LOGIN CREATEROLE;" >/dev/null 2>&1
got=$(psql -h 127.0.0.1 -p $PGPORT -U attacker -d postgres -X -q -c "GRANT supacache_admin TO attacker;" 2>&1 | head -1)
case "$got" in *"reserved"*) echo "PASS  tenant self-grant supacache_admin blocked by pg_guard"; pass=$((pass+1));;
  *) echo "FAIL  pg_guard did not block ($got)"; fail=$((fail+1));; esac

echo
echo "== $pass passed, $fail failed =="
echo
cat <<'DOC'
Restart-based cases (change config, then `pg_ctl restart`):
  §4.1 load order:  set shared_preload_libraries='supatype_mask, pg_keyspace'
                    -> worker logs "REFUSING ... must load AFTER pg_keyspace"; RESP down.
  §4.5 seclabel:    SECURITY LABEL FOR supatype ON COLUMN supacache.kv.val IS 'MASK ...'
                    -> worker logs "REFUSING ... supatype security label ... (§4.5)"; RESP down.
                    Remove the label (IS NULL) and restart -> RESP recovers.
DOC
