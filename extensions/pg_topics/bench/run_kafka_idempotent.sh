#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice;
  ALTER ROLE alice SET default_transaction_isolation = 'repeatable read'; CREATE ROLE stranger LOGIN" >/dev/null
for t in java_q:4 java_kill_q:4 py_kill_q:4 epoch_q:1; do
  psql_as alice "SELECT topic.create_topic('alice.${t%:*}', ${t#*:})" >/dev/null
done
psql_as alice "CREATE FUNCTION alice.slow_commit() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN PERFORM pg_sleep(15); RETURN NULL; END \$\$;
  CREATE CONSTRAINT TRIGGER slow_commit AFTER INSERT ON alice.java_kill_q DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN (NEW.value::text = '3000') EXECUTE FUNCTION alice.slow_commit();
  CREATE CONSTRAINT TRIGGER slow_commit AFTER INSERT ON alice.py_kill_q DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW WHEN (NEW.value::text = '3000') EXECUTE FUNCTION alice.slow_commit()" >/dev/null
start_listener
java_config alice alice-pw
printf 'enable.idempotence=true\nacks=all\nretries=2147483647\n' >>"$WORK/alice.properties"

rows() {
  psql_as postgres "SELECT count(*), count(DISTINCT value) FROM alice.$1"
}
acked() {
  grep -o '"name":"tool_data","sent":[0-9]*,"acked":[0-9]*' <<<"$1" | grep -o '[0-9]*$' || true
}
retry_waits_on_row_lock() {
  psql_as postgres "SELECT count(*) > 0 AND EXISTS (SELECT FROM pg_stat_activity
      WHERE application_name = 'pg_topics listener' AND wait_event = 'PgSleep')
    FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
    WHERE NOT l.granted AND l.locktype IN ('transactionid', 'tuple') AND a.application_name = 'pg_topics listener'"
}
drop_listener_in_commit() {
  wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity
    WHERE application_name = 'pg_topics listener' AND wait_event = 'PgSleep'\")\" = 1 ]"
  restart_listener
  wait_for "[ \"\$(retry_waits_on_row_lock)\" = t ]" || true
  chk "$1: while the first commit is open, the retry waits on its row lock" t "$(retry_waits_on_row_lock)"
}

PGAPPNAME=stranger "$PGBIN/psql" -h /tmp -p "$PORT" -U stranger -d postgres -q -o /dev/null \
  -c "SELECT pg_advisory_lock(1885828208, hashint8(i)) FROM generate_series(1, 100) i" -c "SELECT pg_sleep(3600)" >/dev/null 2>&1 &
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE application_name = 'stranger' AND wait_event = 'PgSleep'\")\" = 1 ]"
out=$(kafka_java kafka-verifiable-producer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.java_q \
  --max-messages 10000 --producer.config /w/alice.properties)
chk "while a stranger holds advisory locks, a Java producer with enable.idempotence=true and acks=all gets 10000 acks" \
  10000 "$(acked "$out")"
chk "the table has exactly 10000 rows, all distinct" "10000|10000" "$(rows java_q)"
chk "the idempotent Java producer reports the true offset of every record" 0 \
  "$(wrong_offsets alice.java_q "$(java_offsets "$out")")"
chk "the Java producer got one producer id from InitProducerId" 1 \
  "$(psql_as postgres "SELECT count(DISTINCT producer_id) FROM topic.topic_producers WHERE topic = 'java_q'")"

kafka_java kafka-verifiable-producer --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.java_kill_q \
  --max-messages 10000 --producer.config /w/alice.properties >"$WORK/java_kill.out" &
java=$!
drop_listener_in_commit Java
wait "$java"
n=$(acked "$(cat "$WORK/java_kill.out")")
chk "the listener restart hit a commit in flight, and the Java producer still gets 10000 acks" 10000 "$n"
chk "the Java retries wrote no duplicate: the row count equals the acknowledged count" "$n|$n" "$(rows java_kill_q)"
chk "the DUPLICATE answer to the Java retry gives the original offsets, as every other ack does" 0 \
  "$(wrong_offsets alice.java_kill_q "$(java_offsets "$(cat "$WORK/java_kill.out")")")"

kafka_py alice alice-pw idempotent alice.py_kill_q 10000 >"$WORK/py_kill.out" &
py=$!
drop_listener_in_commit librdkafka
wait "$py"
chk "librdkafka with enable.idempotence=true gets 10000 acks across the listener restart" "acked 10000 failed 0" \
  "$(grep '^acked' "$WORK/py_kill.out" || true)"
chk "the librdkafka retries wrote no duplicate" "10000|10000" "$(rows py_kill_q)"
chk "the DUPLICATE answer to the librdkafka retry gives the original offsets, as every other ack does" 0 \
  "$(wrong_offsets alice.py_kill_q "$(py_offsets "$(cat "$WORK/py_kill.out")")")"

out=$( (for v in 1 2 3 notjson 4 5 6 7 8; do echo "$v"; sleep 1; done) | kafka_java kafka-console-producer \
  --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --topic alice.epoch_q --producer.config /w/alice.properties --producer-property linger.ms=0 || true)
chk "only the record that is not JSON fails" 1 "$(grep -c 'Error when sending message' <<<"$out" || true)"
chk "after INVALID_RECORD the Java producer bumps its epoch, keeps working, and every acked record is in the table" \
  "1 2 3 4 5 6 7 8" "$(psql_as postgres "SELECT string_agg(value::text, ' ' ORDER BY value::text) FROM alice.epoch_q")"
chk "the producer wrote the records after the failure with epoch 1" 1 \
  "$(psql_as postgres "SELECT max(producer_epoch) FROM topic.topic_producers WHERE topic = 'epoch_q'")"

chk "a transactional producer gets TRANSACTIONAL_ID_AUTHORIZATION_FAILED from InitProducerId" \
  "txn TRANSACTIONAL_ID_AUTHORIZATION_FAILED" "$(kafka_py alice alice-pw transactional | grep '^txn' || true)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
