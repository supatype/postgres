#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice" >/dev/null
psql_as alice "SELECT topic.create_topic('alice.r_q', 1)" >/dev/null
start_listener

fds=()
for i in $(seq 1 64); do
  exec {fd}<>"/dev/tcp/127.0.0.1/$KPORT"
  fds+=("$fd")
done
for fd in "${fds[@]}"; do printf '\x16\x03\x01\x3f\xff' >&"$fd"; done
(for i in 1 2 3 4; do
  sleep 4
  for fd in "${fds[@]}"; do printf '\x00' >&"$fd" 2>/dev/null || true; done
done) &
trickle=$!
sleep 12
closed=0
for fd in "${fds[@]}"; do
  rc=0
  read -r -t 0.1 -u "$fd" _ || rc=$?
  [ "$rc" = 1 ] && closed=$((closed + 1))
done
chk "64 sockets that trickle 1 byte every 4 s are closed by 12 s" 64 "$closed"
wait "$trickle" || true
for fd in "${fds[@]}"; do exec {fd}<&-; done
chk "a real client connects after the trickle sockets are gone" "ok 0" \
  "$(kafka_py alice alice-pw produce alice.r_q 1 | grep -o '^ok 0' || true)"

stamper() {
  psql_as postgres "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_topics stamper'"
}
old=$(listener_pid)
old_stamper=$(stamper)
psql_as postgres "SELECT pg_terminate_backend($old)" >/dev/null
wait_for "[ \"\$(listener_pid)\" != '$old' ] && [ \"\$(listener_status)\" = 'listening on port $KPORT' ]"
chk "SIGTERM restarts the listener worker alone (its pid changes, the stamper pid does not)" yes \
  "$([ "$(listener_pid)" != "$old" ] && [ "$(stamper)" = "$old_stamper" ] && echo yes || echo no)"

kafka_py alice alice-pw traffic alice.r_q 20 >"$WORK/traffic.out" &
traffic=$!
wait_for "[ \"\$(psql_as postgres 'SELECT count(*) > 100 FROM alice.r_q')\" = t ]"
old=$(listener_pid)
kill -9 "$old"
wait_for "p=\$(listener_pid); [[ \"\$p\" =~ ^[0-9]+\$ ]] && [ \"\$p\" != '$old' ] && [ \"\$(listener_status)\" = 'listening on port $KPORT' ]"
chk "the cluster recovers after kill -9 of the listener worker" "listening on port $KPORT" "$(listener_status)"
wait "$traffic"
grep -v "^cb " "$WORK/traffic.out" || true

read -r _ sent _ delivered _ failed _ consumed <<<"$(grep '^sent' "$WORK/traffic.out")"
chk "the producer reconnects and every record is delivered" "$sent 0" "$delivered $failed"
chk "the consumer reconnects and reads every delivered record" "$delivered" "$consumed"
chk "every delivered record is in the table" "$sent" \
  "$(psql_as postgres "SELECT count(DISTINCT value) FROM alice.r_q")"
chk "the clients saw the listener go away" yes \
  "$(grep -q '^cb ' "$WORK/traffic.out" && echo yes || echo no)"

cp "$WORK/server.crt" "$WORK/server.key" "$PGDATA/"
chmod 600 "$PGDATA/server.key"
psql_as postgres "ALTER SYSTEM SET ssl = on" >/dev/null
psql_as postgres "ALTER SYSTEM RESET pg_topics.tls_cert_file" >/dev/null
psql_as postgres "ALTER SYSTEM RESET pg_topics.tls_key_file" >/dev/null
psql_as postgres "ALTER SYSTEM SET pg_topics.tls_use_postgres_cert = on" >/dev/null
psql_as postgres "SELECT pg_reload_conf()" >/dev/null
restart_listener
chk "with pg_topics.tls_use_postgres_cert, the listener serves the relative ssl_cert_file" "ok 0" \
  "$(kafka_py alice alice-pw produce alice.r_q 1 | grep -o '^ok 0' || true)"

psql_as postgres "ALTER SYSTEM SET pg_topics.max_clients = 1" >/dev/null
psql_as postgres "SELECT pg_reload_conf()" >/dev/null
restart_listener
kafka_py alice alice-pw consume alice.r_q 0 0 1000000 >/dev/null &
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'pg_topics listener'\")\" = 1 ]"
out=$(kafka_py alice alice-pw produce alice.r_q 1)
chk "above max_clients, the listener refuses the next client after SaslAuthenticate" yes \
  "$(grep -q 'the listener has max_clients authenticated clients' <<<"$out" && echo yes || echo no)"
wait

old=$(listener_pid)
psql_as postgres "ALTER DATABASE postgres SET pg_topics.port = 0" >/dev/null
psql_as postgres "SELECT pg_terminate_backend($old)" >/dev/null
wait_for "[ \"\$(listener_pid)\" != '$old' ] && [ -n \"\$(listener_status)\" ]"
chk "port 0 means no listener" "no listener: pg_topics.port is 0" "$(listener_status)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
