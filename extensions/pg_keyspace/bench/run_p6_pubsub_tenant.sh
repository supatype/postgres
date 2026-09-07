#!/usr/bin/env bash
# P3/P2 — tenant-scoped pub/sub. Channels are namespaced by the authenticated
# connection's tenant (the same `{tenant}:` prefix that isolates keys), so one
# tenant's SUBSCRIBE/PUBLISH cannot reach another's — transparently: every frame
# echoes the client's own unscoped channel name. Exempt (service_role)
# connections use the raw namespace and can address a tenant channel explicitly.
#
# Requires the in-PG RESP worker with AUTH configured; auto-detects TLS.
set -u
RESP=${RESP:-6381}
PGPORT=${PGPORT:-5434}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

# credentials: two tenants + one exempt service role
$P >/dev/null 2>&1 <<'SQL'
INSERT INTO supacache.resp_credential(username,secret,role_name,tenant) VALUES
  ('alice','pw1','tenant_a','ta'),('bob','pw2','tenant_b','tb'),('svc','pw3','service_role','')
ON CONFLICT(username) DO UPDATE SET secret=EXCLUDED.secret,role_name=EXCLUDED.role_name,tenant=EXCLUDED.tenant;
SELECT pg_reload_conf();
SQL
sleep 1

# auto-detect TLS on the RESP port
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then TLS=""; else TLS="--tls --insecure"; fi
if [ -n "$TLS" ] && ! timeout 3 redis-cli $TLS -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  echo "  SKIP  RESP worker not reachable on :$RESP"; exit 0
fi
A() { redis-cli $TLS -p "$RESP" --user "$1" -a "$2" --no-auth-warning "${@:3}" 2>/dev/null; }

echo "# P3/P2 tenant-scoped pub/sub (RESP :$RESP, tls=${TLS:-off})"

# alice (tenant_a) subscribes to "news"; capture her stream for 5s
sub=$(mktemp)
timeout 5 redis-cli $TLS -p "$RESP" --user alice -a pw1 --no-auth-warning SUBSCRIBE news > "$sub" 2>&1 &
sleep 1

# receiver counts prove routing: bob (other tenant) reaches nobody; alice's own
# publish reaches her 1 subscriber.
chk "bob PUBLISH news -> 0 (isolated from tenant_a)" "0" "$(A bob pw2 PUBLISH news fromB)"
chk "alice PUBLISH news -> 1 (same tenant)"          "1" "$(A alice pw1 PUBLISH news fromA)"
# exempt service role publishes in the RAW namespace: plain "news" misses alice's
# scoped channel, but it can target "ta:news" explicitly and reach her.
chk "svc PUBLISH news -> 0 (raw namespace, misses ta)"  "0" "$(A svc pw3 PUBLISH news fromSvc)"
chk "svc PUBLISH ta:news -> 1 (explicit tenant target)" "1" "$(A svc pw3 PUBLISH ta:news cross)"
sleep 1
wait 2>/dev/null

# alice's stream: she got her own + the explicit svc message, NOT bob's/global svc
got="$(grep -A2 '^message$' "$sub" | grep -vE '^message$|^--')"
chk "alice received her own tenant message"      "1" "$(echo "$got" | grep -cx 'fromA')"
chk "alice received the explicit-target message" "1" "$(echo "$got" | grep -cx 'cross')"
chk "alice did NOT receive bob's message"        "0" "$(echo "$got" | grep -cx 'fromB')"
chk "alice did NOT receive the raw-svc message"  "0" "$(echo "$got" | grep -cx 'fromSvc')"
# transparency: every frame shows the UNSCOPED name "news" (1 subscribe ack +
# 2 message frames), and the scoped "ta:news" never reaches the client.
chk "frames show unscoped 'news' (ack + 2 messages)" "3" "$(grep -cx 'news' "$sub")"
chk "no scoped 'ta:news' leaked to the client"    "0" "$(grep -cx 'ta:news' "$sub")"

# pattern isolation: alice PSUBSCRIBE news.* only matches her own tenant
psub=$(mktemp)
timeout 4 redis-cli $TLS -p "$RESP" --user alice -a pw1 --no-auth-warning PSUBSCRIBE 'news.*' > "$psub" 2>&1 &
sleep 1
chk "alice PUBLISH news.tech -> 1 (own pattern)"  "1" "$(A alice pw1 PUBLISH news.tech deep)"
chk "bob PUBLISH news.tech -> 0 (isolated)"       "0" "$(A bob pw2 PUBLISH news.tech nope)"
sleep 1
wait 2>/dev/null
pgot="$(grep -A3 '^pmessage$' "$psub" | grep -vE '^pmessage$|^--')"
chk "alice pmessage: unscoped pattern + channel + payload" \
    "news.*|news.tech|deep" "$(echo "$pgot" | paste -sd'|')"

# cleanup
$P >/dev/null 2>&1 <<'SQL'
DELETE FROM supacache.resp_credential WHERE username IN ('alice','bob','svc');
SELECT pg_reload_conf();
SQL
rm -f "$sub" "$psub"
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
