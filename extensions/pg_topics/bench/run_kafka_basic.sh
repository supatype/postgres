#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice;
  CREATE ROLE bob LOGIN PASSWORD 'bob-pw'; GRANT USAGE ON SCHEMA alice TO bob;
  CREATE ROLE carol LOGIN PASSWORD 'carol-pw'; GRANT USAGE ON SCHEMA alice TO carol;
  CREATE ROLE dave LOGIN PASSWORD 'dave-pw'; GRANT USAGE ON SCHEMA alice TO dave;
  CREATE ROLE slow LOGIN PASSWORD 'slow-pw'; CREATE SCHEMA slow AUTHORIZATION slow;
  ALTER ROLE slow SET statement_timeout = '3s'; ALTER ROLE slow SET log_statement = 'all'" >/dev/null
for t in java_q:1 keyed_q:4 codec_q:1 bad_q:1 wait_q:1 old_q:1 err_q:1 time_q:1 acks_q:1 big_q:1 mem_q:1 hdr_q:1 \
  offs_q:4 joffs_q:4 pub_q:4 late_q:1 drop_q:1 fast_q:1 slowc_q:3 stall_q:1; do
  psql_as alice "SELECT topic.create_topic('alice.${t%:*}', ${t#*:})" >/dev/null
done
psql_as alice "SELECT topic.grant_publish('alice.pub_q', 'dave');
  GRANT INSERT (band, key, value, headers, producer_timestamp) ON alice.keyed_q TO bob;
  GRANT SELECT ON alice.keyed_q TO carol;
  CREATE FUNCTION alice.refuse() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN RAISE EXCEPTION 'refused by a tenant trigger'; END \$\$;
  CREATE TRIGGER refuse BEFORE INSERT ON alice.err_q FOR EACH ROW EXECUTE FUNCTION alice.refuse();
  CREATE FUNCTION alice.drop_row() RETURNS trigger LANGUAGE plpgsql AS \$\$ BEGIN RETURN NULL; END \$\$;
  CREATE TRIGGER drop_row BEFORE INSERT ON alice.drop_q FOR EACH ROW EXECUTE FUNCTION alice.drop_row();
  CREATE FUNCTION alice.slow_commit() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN PERFORM pg_sleep(1); RETURN NULL; END \$\$;
  CREATE CONSTRAINT TRIGGER slow_commit AFTER INSERT ON alice.slowc_q DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW EXECUTE FUNCTION alice.slow_commit();
  SELECT topic.set_backlog_limit('alice.stall_q', '2 seconds')" >/dev/null
psql_as slow "SELECT topic.create_topic('slow.s_q', 1)" >/dev/null

start_listener
chk "the listener binds the port that ALTER DATABASE postgres SET pg_topics.port gives" \
  "listening on port $KPORT" "$(listener_status)"
raw=$(kafka_py alice alice-pw raw 0000000b0012006300000007000000 | grep '^raw ' | cut -d' ' -f2)
api_count=$((16#${raw:20:8}))
expected_len=$(printf '%08x%08x%04x' $((10 + 6 * api_count)) 7 35)
chk "an ApiVersions version that is not known gets the v0 response with UNSUPPORTED_VERSION, and the frame length matches the advertised API count" \
  "$expected_len" "${raw:0:20}"
chk "a Metadata request before authentication closes the connection with no response" "raw closed" \
  "$(kafka_py alice alice-pw raw 0000000e00030000000000010000000000000000 | grep '^raw' || true)"
chk "before authentication, a frame above 64 KiB closes the connection at once" "raw closed" \
  "$(kafka_py alice alice-pw raw 000111700012000000000007000000 | grep '^raw' || true)"

java_config alice alice-pw
seq 1 100 | sed 's/.*/{"j": &}/' | kafka_java kafka-console-producer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" \
  --topic alice.java_q --producer.config /w/alice.properties --producer-property enable.idempotence=false >/dev/null
chk "a Java producer over SASL_SSL PLAIN writes 100 records" 100 "$(psql_as postgres "SELECT count(*) FROM alice.java_q")"
wait_for "[ \"\$(unstamped alice.java_q)\" = 0 ]"
consumed=$(kafka_java kafka-console-consumer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.java_q \
  --partition 0 --offset 0 --max-messages 100 --timeout-ms 20000 \
  --consumer.config /w/alice.properties --consumer-property check.crcs=true)
chk "a Java consumer with check.crcs=true reads the 100 records back" 100 "$(grep -c '^{"j": [0-9]*}$' <<<"$consumed" || true)"

out=$(kafka_py alice alice-pw produce alice.keyed_q 100 json none key)
chk "librdkafka produces 100 keyed records" 100 "$(grep -c '^ok ' <<<"$out" || true)"
chk "every key lands on band_for(key, 4), as with murmur2_random" 0 \
  "$(psql_as postgres "SELECT count(*) FROM alice.keyed_q WHERE band <> topic.band_for(key, 4)")"
chk "the keys spread over all 4 bands" 4 "$(psql_as postgres "SELECT count(DISTINCT band) FROM alice.keyed_q")"
chk "the headers are stored as a JSON array of key and value" '[{"key": "n", "value": "7"}]' \
  "$(psql_as postgres "SELECT headers FROM alice.keyed_q WHERE key = 'key-7'")"
chk "librdkafka produces a record with repeated, binary and null headers, and gets offset 0" 'ok 0 0 {"h": 1}' \
  "$(kafka_py alice alice-pw produce_headers alice.hdr_q | grep '^ok' || true)"
chk "the headers are stored in order, and a value that is not UTF-8 is stored as value_base64" \
  '[{"key": "a", "value": "1"}, {"key": "a", "value": "2"}, {"key": "bin", "value_base64": "/wAB"}, {"key": "n", "value": null}]' \
  "$(psql_as postgres "SELECT headers FROM alice.hdr_q")"
wait_for "[ \"\$(unstamped alice.hdr_q)\" = 0 ]"
chk "a librdkafka consumer reads back the same headers, in order" \
  "headers [('a', b'1'), ('a', b'2'), ('bin', b'\\xff\\x00\\x01'), ('n', None)]" \
  "$(kafka_py alice alice-pw headers alice.hdr_q | grep '^headers' || true)"
chk "a Java consumer with print.headers=true reads back the same header bytes, in order" \
  "613a312c613a322c62696e3aff00012c6e3a6e756c6c" \
  "$(kafka_java kafka-console-consumer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.hdr_q \
     --partition 0 --offset 0 --max-messages 1 --timeout-ms 20000 --consumer.config /w/alice.properties \
     --property print.headers=true --property print.value=false | grep -a '^a:' | head -c -1 | od -An -tx1 | tr -d ' \n')"

for c in none gzip snappy lz4 zstd; do
  out=$(kafka_py alice alice-pw produce alice.codec_q 10 json "$c")
  chk "librdkafka produces 10 records with compression $c" 10 "$(grep -c '^ok ' <<<"$out" || true)"
done
chk "the codec topic has 50 JSON rows" 50 \
  "$(psql_as postgres "SELECT count(*) FROM alice.codec_q WHERE value ? 'i'")"

out=$(kafka_py alice alice-pw produce alice.offs_q 3000 json none offs)
chk "librdkafka gets 3000 delivery reports" 3000 "$(grep -c '^ok ' <<<"$out" || true)"
chk "librdkafka reports the true offset of every record" 0 "$(wrong_offsets alice.offs_q "$(py_offsets "$out")")"
batches() {
  psql_as postgres "SELECT (SELECT max(n) > 1 FROM (SELECT count(*) AS n FROM $1 GROUP BY xact, band) b)
                           || ' ' || EXISTS (SELECT FROM $1 GROUP BY xact HAVING count(DISTINCT band) > 1)"
}
chk "the librdkafka requests carried batches of many records" true "$(batches alice.offs_q | cut -d' ' -f1)"
cp "$WORK/alice.properties" "$WORK/batch.properties"
printf 'enable.idempotence=false\nacks=all\nlinger.ms=50\nbatch.size=1024\n' >>"$WORK/batch.properties"
out=$(kafka_java kafka-verifiable-producer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.joffs_q \
  --max-messages 3000 --producer.config /w/batch.properties)
chk "the Java producer gets 3000 acks" 3000 "$(java_offsets "$out" | grep -c . || true)"
chk "the Java producer reports the true offset of every record" 0 "$(wrong_offsets alice.joffs_q "$(java_offsets "$out")")"
chk "the Java requests carried batches of many records, and several partitions in one request" "true true" \
  "$(batches alice.joffs_q)"
out=$(kafka_py dave dave-pw produce alice.pub_q 200)
chk "a role with only grant_publish gets the true offset of every record" 0 \
  "$(wrong_offsets alice.pub_q "$(py_offsets "$out")")"
hold_stamp_lock alice.late_q
out=$(kafka_py alice alice-pw produce alice.late_q 1 json none "" -1 0 all 2000)
release_stamp_lock
chk "a record that the stamper does not stamp within the request timeout gets REQUEST_TIMED_OUT" \
  "err REQUEST_TIMED_OUT" "$(grep '^err' <<<"$out" || true)"
chk "the timed-out record is committed, and the stamper gives it offset 0 later" 0 \
  "$(wait_for "[ \"\$(unstamped alice.late_q)\" = 0 ]"; psql_as postgres "SELECT log_offset FROM alice.late_q")"
healthy() {
  psql_as postgres "SELECT ok FROM topic.health() WHERE topic = 'stall_q'"
}
wait_for "[ \"\$(healthy)\" = t ]" || true
hold_stamp_lock alice.stall_q
chk "while the stamper cannot stamp a topic, health() reports ok = false for it" f \
  "$(wait_for "[ \"\$(healthy)\" = f ]" || true; healthy)"
release_stamp_lock
chk "when the stamper runs again, health() reports ok = true" t "$(wait_for "[ \"\$(healthy)\" = t ]" || true; healthy)"
out=$(kafka_py alice alice-pw produce alice.drop_q 3)
chk "when a tenant trigger drops the rows, librdkafka still gets 3 acks and no error" "3 0" \
  "$(grep -c '^ok ' <<<"$out" || true) $(grep -c '^err' <<<"$out" || true)"
out=$(kafka_py alice alice-pw idempotent alice.drop_q 3)
chk "when a tenant trigger drops the rows, an idempotent producer still gets 3 acks and no error" "acked 3 failed 0" \
  "$(grep '^acked' <<<"$out" || true)"
PGAPPNAME=hold "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -o /dev/null -c "BEGIN" \
  -c "SELECT FROM topic.topic_band_position WHERE topic = 'fast_q' FOR NO KEY UPDATE" -c "SELECT pg_sleep(3600)" >/dev/null 2>&1 &
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'hold' AND wait_event = 'PgSleep'\")\" = 1 ]"
(until [ "$(psql_as postgres "SELECT count(*) FROM alice.fast_q")" = 1 ]; do sleep 0.05; done
 sleep 0.2
 psql_as postgres "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name = 'hold'" >/dev/null) &
release=$!
out=$(kafka_py alice alice-pw pipeline alice.fast_q alice.slowc_q)
wait "$release"
chk "the first request is answered first, and each of the 3 slow requests is answered" "fast 3" \
  "$(grep '^pipeline' <<<"$out" | awk '{ n = 0; for (i = 3; i <= NF; i++) if ($i ~ /^slow/) n++; print $2, n }')"
fast=$(grep '^fast' <<<"$out" || true)
echo "fast record answered: $fast"
chk "the first answer has its true offset and waits for at most one slow later request (under 2 s)" "fast 0 yes" \
  "$([[ "$fast" =~ ^fast\ ([^ ]+)\ ([0-9]+)$ ]] && echo "fast ${BASH_REMATCH[1]} $([ "${BASH_REMATCH[2]}" -lt 2000 ] && echo yes || echo no)")"

chk "a value that is not JSON gets INVALID_RECORD" "err INVALID_RECORD" \
  "$(kafka_py alice alice-pw produce alice.bad_q 1 text | grep '^err' || true)"
chk "a key of 41 characters gets INVALID_RECORD" "err INVALID_RECORD" \
  "$(kafka_py alice alice-pw produce alice.bad_q 1 json none long | grep '^err' || true)"
chk "a batch above max_message_bytes gets MESSAGE_TOO_LARGE" "err MSG_SIZE_TOO_LARGE" \
  "$(kafka_py alice alice-pw produce alice.bad_q 1 big | grep '^err' || true)"
chk "a compression bomb that expands past the request budget gets MESSAGE_TOO_LARGE" "err MSG_SIZE_TOO_LARGE" \
  "$(kafka_py alice alice-pw produce alice.bad_q 1 bomb zstd | grep '^err' || true)"
chk "the refused batches wrote no row" 0 "$(psql_as postgres "SELECT count(*) FROM alice.bad_q")"

out=$(kafka_py alice alice-pw produce alice.acks_q 3 json none "" -1 0 0)
chk "acks=0 gets no error from the producer" 0 "$(grep -c '^err' <<<"$out" || true)"
wait_for "[ \"\$(psql_as postgres 'SELECT count(*) >= 3 FROM alice.acks_q')\" = t ]"
chk "acks=0 still writes the rows" t "$(psql_as postgres "SELECT count(*) >= 3 FROM alice.acks_q")"

out=$(kafka_py alice wrong-pw produce alice.bad_q 1)
chk "a wrong password fails SASL authentication" yes \
  "$(grep -q 'authentication failed: password authentication failed for user "alice"' <<<"$out" && echo yes || echo no)"
chk "the local trust line in pg_hba.conf has no effect on the listener" yes \
  "$(grep -q '^local *all *all *trust' "$PGDATA/pg_hba.conf" && grep -q '^err' <<<"$out" && echo yes || echo no)"
chk "a role without SELECT gets TOPIC_AUTHORIZATION_FAILED on Fetch" "err TOPIC_AUTHORIZATION_FAILED" \
  "$(kafka_py bob bob-pw consume alice.keyed_q 0 0 1 | grep -o '^err [A-Z_]*' || true)"

chk "a role without INSERT gets TOPIC_AUTHORIZATION_FAILED on Produce" "err TOPIC_AUTHORIZATION_FAILED" \
  "$(kafka_py carol carol-pw produce alice.keyed_q 1 | grep '^err' || true)"
chk "any other Postgres error gets UNKNOWN_SERVER_ERROR" "err UNKNOWN" \
  "$(kafka_py alice alice-pw produce alice.err_q 1 | grep '^err' || true)"
chk "the listener logs that Postgres error" yes \
  "$(grep -q 'pg_topics listener: alice.err_q: refused by a tenant trigger' "$PGDATA/log" && echo yes || echo no)"
chk "a record timestamp of -1 gives a NULL producer_timestamp" "ok 0" \
  "$(kafka_py alice alice-pw produce alice.time_q 1 json none "" -1 -1 | grep -o '^ok 0' || true)"
chk "the row has a NULL producer_timestamp" t \
  "$(psql_as postgres "SELECT bool_and(producer_timestamp IS NULL) FROM alice.time_q")"

cp "$PGDATA/pg_hba.conf" "$WORK/pg_hba.conf"
{ echo "host all all 127.0.0.1/32 trust"; cat "$WORK/pg_hba.conf"; } >"$PGDATA/pg_hba.conf"
psql_as postgres "SELECT pg_reload_conf()" >/dev/null
out=$(kafka_py alice wrong-pw produce alice.bad_q 1)
chk "a trust line for 127.0.0.1 makes PLAIN auth fail (the system_user check)" yes \
  "$(grep -q 'did not use a password method (system_user is None)' <<<"$out" && echo yes || echo no)"
chk "the trust connection wrote no row" 0 "$(psql_as postgres "SELECT count(*) FROM alice.bad_q")"
cp "$WORK/pg_hba.conf" "$PGDATA/pg_hba.conf"
psql_as postgres "SELECT pg_reload_conf()" >/dev/null

out=$(kafka_py slow slow-pw produce slow.s_q 5)
chk "a role with statement_timeout = 3s produces with acks=all" 5 "$(grep -c '^ok ' <<<"$out" || true)"
before_commit=$(awk '/LOG:  (statement|execute [^:]*): / {
    pid = $4; sub(/.*LOG:  (statement|execute [^:]*): /, "")
    if ($0 == "COMMIT") print last[pid]; last[pid] = $0 }' "$PGDATA/log" | sort -u)
chk "SET LOCAL statement_timeout = 0 is the last statement before each COMMIT of that role" \
  "SET LOCAL statement_timeout = 0" "$before_commit"

latency=$(kafka_py alice alice-pw latency alice.wait_q | grep '^latency' || true)
echo "blocking fetch: $latency ms"
chk "a blocking Fetch with fetch.wait.max.ms=5000 returns within 200 ms after a publish" yes \
  "$([[ "$latency" =~ ^latency\ ([0-9]+)$ ]] && [ "${BASH_REMATCH[1]}" -lt 200 ] && echo yes || echo no)"

psql_as alice "SELECT topic.publish('alice.old_q', jsonb_build_object('i', i)), pg_sleep(0.01)
               FROM generate_series(0, 9) i" >/dev/null
wait_for "[ \"\$(unstamped alice.old_q)\" = 0 ]"
psql_as postgres "UPDATE topic.topic_band_position SET oldest_offset = 5 WHERE topic = 'old_q'" >/dev/null
out=$(kafka_py alice alice-pw consume alice.old_q 0 2 1 error)
chk "a consumer below oldest_offset gets OFFSET_OUT_OF_RANGE" yes \
  "$(grep -q '^err _AUTO_OFFSET_RESET .*Offset out of range' <<<"$out" && echo yes || echo no)"
chk "auto.offset.reset=earliest then reads from oldest_offset" "msg 0 5" \
  "$(kafka_py alice alice-pw consume alice.old_q 0 2 1 earliest | grep -o '^msg 0 [0-9]*' || true)"
ts=$(psql_as postgres "SELECT floor(extract(epoch FROM published_at) * 1000)::int8 FROM alice.old_q WHERE log_offset = 7")
chk "ListOffsets earliest, latest and by time give 5, 10 and 7" "watermarks 5 10
time 7" "$(kafka_py alice alice-pw offsets alice.old_q 0 "$ts" | grep '^watermarks\|^time' || true)"
chk "ListOffsets by max timestamp (-3) gives the last offset" "alice.old_q:0:9" \
  "$(kafka_java kafka-get-offsets --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.old_q --time -3 --command-config /w/alice.properties | tr -d '[:space:]')"

kafka_py alice alice-pw produce alice.big_q 5 mid >/dev/null
wait_for "[ \"\$(unstamped alice.big_q)\" = 0 ]"
chk "a tiny max.partition.fetch.bytes still returns every record (progress is guaranteed)" "count 5" \
  "$(kafka_py alice alice-pw consume alice.big_q 0 0 5 earliest "max.partition.fetch.bytes=1" | grep '^count' || true)"
chk "a tiny fetch.max.bytes still returns every record" "count 5" \
  "$(kafka_py alice alice-pw consume alice.big_q 0 0 5 earliest "fetch.max.bytes=1000,message.max.bytes=1000" | grep '^count' || true)"
next=$(psql_as postgres "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'big_q' AND band = 0")
chk "a high fetch.min.bytes on an empty tail waits and returns nothing" "count 0" \
  "$(kafka_py alice alice-pw consume alice.big_q 0 "$next" 1 latest "fetch.min.bytes=1000000,fetch.wait.max.ms=1500" | grep '^count' || true)"

chk "a nonexistent topic gets TOPIC_AUTHORIZATION_FAILED on Produce, like an unseen one" \
  "err TOPIC_AUTHORIZATION_FAILED" "$(kafka_py alice alice-pw produce alice.ghost_q 1 | grep '^err' || true)"
chk "Metadata gives the same answer for a nonexistent topic and an unseen one" \
  "metadata TOPIC_AUTHORIZATION_FAILED
metadata TOPIC_AUTHORIZATION_FAILED" \
  "$(kafka_py alice alice-pw metadata alice.ghost_q | grep '^metadata'; kafka_py alice alice-pw metadata slow.s_q | grep '^metadata')"
chk "band_offsets refuses a nonexistent topic and an unseen one with the same error" \
  "ERROR:  topic.band_offsets: role bob may not read topic alice.ghost_q
ERROR:  topic.band_offsets: role bob may not read topic slow.s_q" \
  "$(psql_as bob "SELECT * FROM topic.band_offsets('alice.ghost_q')" | grep '^ERROR'
     psql_as bob "SELECT * FROM topic.band_offsets('slow.s_q')" | grep '^ERROR')"

psql_as alice "SELECT topic.publish('alice.mem_q', ('\"' || repeat('x', 200000) || '\"')::jsonb)
               FROM generate_series(1, 200)" >/dev/null
wait_for "[ \"\$(unstamped alice.mem_q)\" = 0 ]"
restart_listener
lpid=$(listener_pid)
base_hwm=$(awk '/VmHWM/{print $2}' "/proc/$lpid/status")
kafka_py alice alice-pw consume alice.mem_q 0 0 3 earliest "max.partition.fetch.bytes=500000" >/dev/null
peak_hwm=$(awk '/VmHWM/{print $2}' "/proc/$lpid/status")
grew=$(((peak_hwm - base_hwm) / 1024))
echo "listener VmHWM grew ${grew} MiB while it fetched 3 of 200 large rows"
chk "the listener streams a fetch and does not load every row of a large topic" yes \
  "$([ "$grew" -lt 20 ] && echo yes || echo no)"

fds=()
for i in $(seq 1 70); do
  exec {fd}<>"/dev/tcp/127.0.0.1/$KPORT"
  fds+=("$fd")
done
sleep 1
closed() {
  local n=0 fd rc
  for fd in "${fds[@]}"; do
    rc=0
    read -r -t 0.1 -u "$fd" _ || rc=$?
    [ "$rc" = 1 ] && n=$((n + 1))
  done
  echo "$n"
}
chk "of 70 idle connections from one host that never authenticate, the 17th and later are closed" 54 "$(closed)"
sleep 10
chk "the listener closes the other 16 when 10 s pass with no SASL" 70 "$(closed)"
for fd in "${fds[@]}"; do exec {fd}<&-; done
chk "a real client still connects" "ok 0" "$(kafka_py alice alice-pw produce alice.wait_q 1 | grep -o '^ok 0' || true)"

psql_as postgres "ALTER SYSTEM SET pg_topics.databases = 'postgres, other'" >/dev/null
psql_as postgres "ALTER SYSTEM SET max_worker_processes = 16" >/dev/null
psql_as postgres "CREATE DATABASE other" >/dev/null
psql_as postgres "ALTER DATABASE other SET pg_topics.port = $KPORT" >/dev/null
stop_pg >/dev/null
start_pg >/dev/null
wait_for "[ \"\$(listener_status | grep -c .)\" = 2 ]"
chk "with two databases on one port, one listener binds and the other reports bind failed" \
  "bind failed: Address already in use (os error 98)
listening on port $KPORT" "$(listener_status | sort)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
