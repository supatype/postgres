#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster
trap 'docker rm -f $(docker ps -aq --filter "name=pgt_${PORT}_") >/dev/null 2>&1 || true; cleanup' EXIT

psql_as postgres "CREATE ROLE alice LOGIN PASSWORD 'alice-pw'; CREATE SCHEMA alice AUTHORIZATION alice;
  GRANT CREATE ON SCHEMA public TO alice;
  CREATE ROLE bob LOGIN PASSWORD 'bob-pw'; CREATE SCHEMA bob AUTHORIZATION bob" >/dev/null
start_listener
java_config alice alice-pw
java_config bob bob-pw

system_id=$(psql_as postgres "SELECT system_identifier FROM pg_control_system()")
out=$(kafka_java kafka-cluster cluster-id --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --config /w/alice.properties)
chk "kafka-cluster cluster-id (DescribeCluster) reports the system_identifier" "Cluster ID: $system_id" "$out"
chk "Metadata's cluster_id equals DescribeCluster's" "cluster_id $system_id" \
  "$(kafka_py alice alice-pw cluster_id | grep '^cluster_id ')"

member() {
  local name=$1 topic=$2 group=$3 cfg=${4:-}
  KAFKA_NAME="pgt_${PORT}_$name" kafka_py alice alice-pw member "$topic" "$group" "$name" "$cfg" >"$WORK/$name.out" &
}
assigned() {
  { grep '^assigned' "$WORK/$1.out" || true; } | tail -n1 | cut -d' ' -f2
}
stop() {
  touch "$WORK/stop-$1"
  wait_for "grep -q '^closed' '$WORK/$1.out'"
}
group_state() {
  psql_as postgres "SELECT state FROM topic.topic_groups WHERE group_name = '$1'"
}

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --create --topic public.orders_q --partitions 6 --replication-factor 1)
echo "$out"
chk "kafka-topics --create makes a 6-band topic with replication-factor 1" yes \
  "$(grep -q 'Created topic public.orders_q' <<<"$out" && echo yes || echo no)"
chk "the topic has 6 bands in topic_config" 6 \
  "$(psql_as postgres "SELECT band_count FROM topic.topic_config WHERE schema_name = 'public' AND topic = 'orders_q'")"

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --create --topic public.rf3_q --partitions 1 --replication-factor 3 || true)
echo "RED: --replication-factor 3 is refused -- $out"
chk "--replication-factor 3 fails with INVALID_REPLICATION_FACTOR" yes \
  "$(grep -qi 'InvalidReplicationFactorException' <<<"$out" && echo yes || echo no)"
chk "the refused topic made no control row" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'rf3_q'")"

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --create --topic alice.forever_q --partitions 1 --replication-factor 1 --config retention.ms=-1 || true)
echo "RED: retention.ms=-1 is refused -- $out"
chk "retention.ms=-1 (Kafka's keep forever) is refused with a message that needs a finite retention" yes \
  "$(grep -qi 'needs a finite retention' <<<"$out" && echo yes || echo no)"
chk "the refused topic made no control row" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'forever_q'")"

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --create --topic alice.p2000_q --partitions 2000 --replication-factor 1 || true)
echo "RED: partitions=2000 is refused -- $out"
chk "partitions=2000 fails with InvalidPartitionsException" yes \
  "$(grep -qi 'InvalidPartitionsException' <<<"$out" && echo yes || echo no)"
chk "the refused topic made no control row" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'p2000_q'")"

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --create --topic orders --partitions 1 --replication-factor 1 || true)
echo "RED: --topic orders (no schema.table_q) is refused -- $out"
chk "--topic orders names the schema.table_q rule" yes \
  "$(grep -qi 'schema.table' <<<"$out" && echo yes || echo no)"

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --describe --topic public.orders_q)
chk "--describe shows 6 partitions" yes "$(grep -q 'PartitionCount: 6' <<<"$out" && echo yes || echo no)"

class() {
  grep -o 'org\.apache\.kafka\.common\.errors\.[A-Za-z]*Exception' <<<"$1" | head -n1
}
missing=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/bob.properties \
  --create --topic ghostschema.x_q --partitions 1 --replication-factor 1 || true)
forbidden=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/bob.properties \
  --create --topic alice.x_q --partitions 1 --replication-factor 1 || true)
echo "RED: bob creates a topic in a schema that does not exist -- $missing"
echo "RED: bob creates a topic in alice's schema, without CREATE -- $forbidden"
chk "CreateTopics gives the same error class for a schema that does not exist and one bob has no CREATE on" yes \
  "$([ -n "$(class "$missing")" ] && [ "$(class "$missing")" = "$(class "$forbidden")" ] && echo yes || echo no)"
chk "neither attempt made a control row" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE topic = 'x_q'")"

out=$(kafka_py alice alice-pw create_topic_validate_only public.validate_q 3)
chk "CreateTopics validate_only reports success" "validate_only NONE" "$out"
chk "CreateTopics validate_only rolled back and made no topic" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE schema_name = 'public' AND topic = 'validate_q'")"

out=$(kafka_java kafka-configs --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --alter --entity-type topics --entity-name public.orders_q --add-config retention.ms=3600000)
chk "kafka-configs --alter --add-config retention.ms=3600000 works" yes \
  "$(grep -qi 'Completed updating config' <<<"$out" && echo yes || echo no)"
chk "retention_interval is now 1 hour" "01:00:00" \
  "$(psql_as postgres "SELECT retention_interval::text FROM topic.topic_config WHERE schema_name = 'public' AND topic = 'orders_q'")"

out=$(kafka_java kafka-configs --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --alter --entity-type topics --entity-name public.orders_q --add-config cleanup.policy=compact || true)
echo "RED: cleanup.policy=compact is refused -- $out"
chk "cleanup.policy=compact fails with INVALID_CONFIG" yes \
  "$(grep -qi 'InvalidConfigurationException' <<<"$out" && echo yes || echo no)"
chk "the refused alter did not change the topic" delete \
  "$(psql_as postgres "SELECT (SELECT count(*) FROM topic.topic_config
     WHERE schema_name = 'public' AND topic = 'orders_q'
       AND min_durability = 'durable') > 0" | sed 's/^t$/delete/')"
chk "retention_interval is still 01:00:00 after the refused alter" "01:00:00" \
  "$(psql_as postgres "SELECT retention_interval::text FROM topic.topic_config WHERE schema_name = 'public' AND topic = 'orders_q'")"

out=$(kafka_java kafka-configs --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --describe --entity-type topics --entity-name public.orders_q)
chk "kafka-configs --describe shows the one config that changed from its default (retention.ms)" 1 \
  "$(grep -cE 'retention\.ms=' <<<"$out" || true)"
chk "kafka-configs --describe does not show min_durability, which is still its default" 0 \
  "$(grep -cE 'pg_topics\.min_durability=' <<<"$out" || true)"

out=$(kafka_py alice alice-pw describe_config_sources public.orders_q)
echo "$out"
chk "retention.ms (changed from 7 days) reports config_source 1 (TOPIC_CONFIG)" 1 \
  "$(awk '$2 == "retention.ms" {print $4}' <<<"$out")"
chk "pg_topics.min_durability (still the default) reports config_source 5 (DEFAULT_CONFIG)" 5 \
  "$(awk '$2 == "pg_topics.min_durability" {print $4}' <<<"$out")"
chk "cleanup.policy, message.timestamp.type and max.message.bytes report config_source 5 (DEFAULT_CONFIG)" "5
5
5" "$(awk '$2 == "cleanup.policy" || $2 == "message.timestamp.type" || $2 == "max.message.bytes" {print $4}' <<<"$out" | sort)"

psql_as alice "SELECT topic.create_topic('alice.legacy_q', 1, interval '2 hours', 'relaxed')" >/dev/null
kafka_py alice alice-pw legacy_alter alice.legacy_q retention.ms=3600000
chk "legacy AlterConfigs sets the key it was given" "01:00:00" \
  "$(psql_as postgres "SELECT retention_interval::text FROM topic.topic_config WHERE schema_name = 'alice' AND topic = 'legacy_q'")"
chk "legacy AlterConfigs resets the key it was not given back to its default" durable \
  "$(psql_as postgres "SELECT min_durability FROM topic.topic_config WHERE schema_name = 'alice' AND topic = 'legacy_q'")"
out=$(kafka_py alice alice-pw legacy_alter alice.legacy_q cleanup.policy=delete,retention.ms=7200000 | grep '^legacy_alter ')
chk "legacy AlterConfigs accepts a read-only key sent back with its current value" "legacy_alter NONE" "$out"
chk "the accepted read-only key changed nothing but the key that was really sent" "02:00:00" \
  "$(psql_as postgres "SELECT retention_interval::text FROM topic.topic_config WHERE schema_name = 'alice' AND topic = 'legacy_q'")"
out=$(kafka_py alice alice-pw legacy_alter alice.legacy_q cleanup.policy=compact | grep '^legacy_alter ')
echo "RED: legacy AlterConfigs refuses a read-only key sent with a different value -- $out"
chk "legacy AlterConfigs refuses a read-only key sent with a different value" "legacy_alter INVALID_CONFIG" "$out"
chk "the refused legacy alter left retention_interval untouched" "02:00:00" \
  "$(psql_as postgres "SELECT retention_interval::text FROM topic.topic_config WHERE schema_name = 'alice' AND topic = 'legacy_q'")"

psql_as alice "SELECT topic.create_topic('alice.grp_q', 2)" >/dev/null
psql_as alice "SELECT topic.publish('alice.grp_q', jsonb_build_object('i', i)) FROM generate_series(1, 20) i" >/dev/null
wait_for "[ \"\$(unstamped alice.grp_q)\" = 0 ]"
member g1 alice.grp_q g "enable.auto.commit=true,enable.auto.offset.store=true,auto.commit.interval.ms=500"
wait_for "[ \"\$(assigned g1)\" = 0,1 ]"
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) = 2 AND bool_and(o.committed_offset = p.next_offset)
    FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
    WHERE o.group_name = 'g'\")\" = t ]"

out=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties --list)
chk "kafka-consumer-groups --list shows g" yes "$(grep -qx 'g' <<<"$out" && echo yes || echo no)"

out=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --describe --group g)
echo "$out"
chk "--describe --group g lists both partitions with a member" 2 "$(grep -c 'alice\.grp_q' <<<"$out" || true)"
chk "--describe --group g shows lag 0 on both partitions" "0" \
  "$(awk '$2 == "alice.grp_q" {print $6}' <<<"$out" | sort -u)"

stop g1
wait_for "[ \"\$(group_state g)\" = Empty ]"
out=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --reset-offsets --group g --topic alice.grp_q --to-earliest --execute)
chk "--reset-offsets --to-earliest --execute works on an Empty group" yes \
  "$(grep -q 'alice.grp_q' <<<"$out" && echo yes || echo no)"
chk "both bands reset to offset 0" "0
0" "$(psql_as postgres "SELECT committed_offset FROM topic.topic_offsets WHERE group_name = 'g' ORDER BY band")"

out=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --delete --group g)
chk "kafka-consumer-groups --delete --group g works" yes \
  "$(grep -qi 'successful' <<<"$out" && echo yes || echo no)"
chk "the group row is gone" 0 "$(psql_as postgres "SELECT count(*) FROM topic.topic_groups WHERE group_name = 'g'")"

out=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --reset-offsets --group fresh --topic alice.grp_q --to-latest --execute)
echo "$out"
chk "--reset-offsets --execute makes the offsets of a group that does not exist yet" "2|true" \
  "$(psql_as postgres "SELECT count(*) || '|' || bool_and(o.committed_offset = p.next_offset)
     FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
     WHERE o.group_name = 'fresh'")"

psql_as alice "SELECT topic.create_table_topic('alice.things', '{\"thing_id\": \"int\", \"n\": \"int\"}', 'thing_id', 2)" >/dev/null
psql_as alice "SELECT topic.publish('alice.things_q', jsonb_build_object('thing_id', i, 'n', i)) FROM generate_series(1, 10) i" >/dev/null
wait_for "[ \"\$(psql_as postgres \"SELECT bool_and(o.committed_offset = p.next_offset)
    FROM topic.topic_offsets o JOIN topic.topic_band_position p USING (schema_name, topic, band)
    WHERE o.group_name = '__pg_topics_sync:alice.things_q'\")\" = t ]"
chk "the sync group's state is Empty" Empty \
  "$(psql_as postgres "SELECT state FROM topic.topic_groups WHERE group_name = '__pg_topics_sync:alice.things_q'")"
out=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --describe --group '__pg_topics_sync:alice.things_q')
echo "$out"
chk "the sync group's lag is visible and 0 on both bands" "0" \
  "$(awk '$2 == "alice.things_q" {print $6}' <<<"$out" | sort -u)"

out=$(kafka_java kafka-topics --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties \
  --delete --topic public.orders_q)
chk "kafka-topics --delete removes the topic" yes \
  "$(psql_as postgres "SELECT to_regclass('public.orders_q') IS NULL" | grep -q t && echo yes || echo no)"
chk "every control row for the deleted topic is gone" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE schema_name = 'public' AND topic = 'orders_q'")"

out=$(kafka_java kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties)
echo "$out"
chk "kafka-broker-api-versions lists exactly 24 APIs" 24 \
  "$(grep -c '\[usable: [0-9]*\]' <<<"$out" || true)"
for entry in 'CreateTopics(19): 2 to 7' 'DeleteTopics(20): 1 to 5' 'DescribeConfigs(32): 1 to 4' \
             'AlterConfigs(33): 0 to 2' 'IncrementalAlterConfigs(44): 0 to 1' 'DeleteGroups(42): 0 to 2' \
             'DescribeCluster(60): 0 to 1' 'ListGroups(16): 0 to 4' 'DescribeGroups(15): 0 to 5'; do
  chk "kafka-broker-api-versions advertises $entry" yes "$(grep -q "$entry" <<<"$out" && echo yes || echo no)"
done

psql_as alice "SELECT topic.create_topic('alice.secret_q', 1)" >/dev/null
psql_as alice "SELECT topic.group_join('alice_priv', '', 'c', 6000, 5000, 'consumer', '[{\"name\": \"range\"}]')" >/dev/null

missing=$(kafka_py bob bob-pw delete_topic alice.ghost_q)
forbidden=$(kafka_py bob bob-pw delete_topic alice.secret_q)
echo "RED: bob deletes a missing topic through the AdminClient -- $missing"
echo "RED: bob deletes alice's topic through the AdminClient -- $forbidden"
chk "DeleteTopics gives the same answer for a nonexistent topic and one bob cannot see" yes \
  "$([ "$missing" = "$forbidden" ] && [ "$missing" != "delete_topic NONE" ] && echo yes || echo no)"
chk "bob's forbidden delete left alice's topic in place" 1 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_config WHERE schema_name = 'alice' AND topic = 'secret_q'")"

missing=$(kafka_py bob bob-pw incremental_alter alice.ghost_q retention.ms set 1000)
forbidden=$(kafka_py bob bob-pw incremental_alter alice.secret_q retention.ms set 1000)
echo "RED: bob incrementally alters a missing topic through the AdminClient -- $missing"
echo "RED: bob incrementally alters alice's topic through the AdminClient -- $forbidden"
chk "IncrementalAlterConfigs gives the same answer for a nonexistent topic and one bob cannot see" yes \
  "$([ "$missing" = "$forbidden" ] && [ "$missing" != "incremental_alter NONE" ] && echo yes || echo no)"

missing=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/bob.properties \
  --describe --group ghost_group 2>&1 || true)
forbidden=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/bob.properties \
  --describe --group alice_priv 2>&1 || true)
chk "DescribeGroups gives the same answer for a nonexistent group and one bob cannot see" yes \
  "$(grep -q 'does not exist' <<<"$missing" && grep -q 'does not exist' <<<"$forbidden" && echo yes || echo no)"

missing=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/bob.properties \
  --delete --group ghost_group || true)
forbidden=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/bob.properties \
  --delete --group alice_priv || true)
chk "DeleteGroups gives the same error class for a nonexistent group and one bob cannot see" yes \
  "$([ -n "$(class "$missing")" ] && [ "$(class "$missing")" = "$(class "$forbidden")" ] && echo yes || echo no)"
chk "bob's forbidden delete left alice's group in place" 1 \
  "$(psql_as postgres "SELECT count(*) FROM topic.topic_groups WHERE group_name = 'alice_priv'")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
