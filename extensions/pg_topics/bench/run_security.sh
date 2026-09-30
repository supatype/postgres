#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

chk "pg_topics makes no role of its own" "" \
  "$(psql_as postgres "SELECT string_agg(rolname, ',') FROM pg_roles WHERE rolname !~ '^pg_' AND rolname <> 'postgres'")"

psql_as postgres "CREATE ROLE tenant_a LOGIN PASSWORD 'a-pw'; CREATE SCHEMA tenant_a AUTHORIZATION tenant_a;
  CREATE ROLE tenant_b LOGIN PASSWORD 'b-pw'; CREATE SCHEMA tenant_b AUTHORIZATION tenant_b;
  CREATE ROLE b_member LOGIN PASSWORD 'm-pw' IN ROLE tenant_b;
  CREATE ROLE pub LOGIN PASSWORD 'pub-pw'; CREATE ROLE con LOGIN PASSWORD 'con-pw';
  GRANT USAGE ON SCHEMA tenant_b TO pub, con, tenant_a;
  CREATE ROLE authenticator LOGIN NOINHERIT; GRANT tenant_a TO authenticator;
  CREATE TABLE public.x (i int); ALTER TABLE public.x OWNER TO tenant_a" >/dev/null

same() {
  [ -n "$1" ] && [ "$1" = "$2" ] && echo yes || echo no
}
join_sql() {
  echo "SELECT (topic.group_join('$1', m.member_id, 'c', 1800000, 5000, 'consumer', '[{\"name\": \"range\"}]')).error
        FROM topic.group_join('$1', '', 'c', 1800000, 5000, 'consumer', '[{\"name\": \"range\"}]') m"
}
psql_as tenant_b "SELECT topic.create_topic('tenant_b.orders_q', 2);
  SELECT topic.publish('tenant_b.orders_q', jsonb_build_object('i', i)) FROM generate_series(1, 4) i" >/dev/null
psql_as tenant_a "SELECT topic.create_topic('tenant_a.mine_q', 1);
  SELECT topic.publish('tenant_a.mine_q', '{}') FROM generate_series(1, 2)" >/dev/null
wait_for "[ \"\$(unstamped tenant_b.orders_q)\" = 0 ] && [ \"\$(unstamped tenant_a.mine_q)\" = 0 ]"
psql_as tenant_b "SELECT topic.commit_offset('tenant_b.orders_q', 'b_group', 0, 1, -1); $(join_sql b_join)" >/dev/null
psql_as tenant_a "SELECT topic.commit_offset('tenant_a.mine_q', 'a_group', 0, 1, -1); $(join_sql a_join)" >/dev/null

rls="SELECT concat_ws('|', (SELECT string_agg(group_name, ',' ORDER BY group_name) FROM topic.topic_groups),
                           (SELECT string_agg(DISTINCT group_name, ',') FROM topic.topic_group_members),
                           (SELECT string_agg(DISTINCT group_name, ',') FROM topic.topic_offsets))"
chk "the superuser sees the rows of both tenants" "a_group,a_join,b_group,b_join|a_join,b_join|a_group,b_group" \
  "$(psql_as postgres "$rls")"
chk "tenant_a reads only its own rows in topic_groups, topic_group_members and topic_offsets" \
  "a_group,a_join|a_join|a_group" "$(psql_as tenant_a "$rls")"
chk "a member of tenant_b reads only tenant_b's rows" "b_group,b_join|b_join|b_group" "$(psql_as b_member "$rls")"

state_as() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U "$1" -d postgres -tA -v VERBOSITY=verbose -c "$2" 2>&1 |
    sed -n 's/^ERROR:  \([0-9A-Z]\{5\}\): .*/\1/p'
}
for sql in "SELECT topic.publish('tenant_b.orders_q', '{}')" \
           "INSERT INTO tenant_b.orders_q (band) VALUES (0)" \
           "SELECT count(*) FROM tenant_b.orders_q" \
           "SELECT count(*) FROM topic.fetch('tenant_b.orders_q', 0, 0)" \
           "SELECT topic.set_retention('tenant_b.orders_q', '1 hour')" \
           "SELECT topic.set_durability('tenant_b.orders_q', 'relaxed')" \
           "SELECT topic.drop_topic('tenant_b.orders_q')" \
           "SELECT topic.grant_publish('tenant_b.orders_q', 'tenant_a')" \
           "SELECT topic.grant_consume('tenant_b.orders_q', 'tenant_a')" \
           "SELECT topic.fetch_offset('tenant_b.orders_q', 'b_group', 0)" \
           "SELECT topic.delete_group('b_group')"; do
  echo "RED: tenant_a runs $sql -- $(psql_as tenant_a "$sql" || true)"
  chk "SQL: tenant_a is refused with 42501: $sql" 42501 "$(state_as tenant_a "$sql")"
done
chk "SQL: commit_offset on tenant_b's topic gives TOPIC_AUTHORIZATION_FAILED, as on a missing topic" \
  "TOPIC_AUTHORIZATION_FAILED TOPIC_AUTHORIZATION_FAILED" \
  "$(psql_as tenant_a "SELECT topic.commit_offset('tenant_b.orders_q', 'a_group', 0, 3, -1)") $(psql_as tenant_a "SELECT topic.commit_offset('tenant_b.ghost_q', 'a_group', 0, 3, -1)")"
chk "SQL: commit_offset and group_join on tenant_b's group give GROUP_AUTHORIZATION_FAILED" \
  "GROUP_AUTHORIZATION_FAILED GROUP_AUTHORIZATION_FAILED" \
  "$(psql_as tenant_a "SELECT topic.commit_offset('tenant_a.mine_q', 'b_group', 0, 1, -1)") $(psql_as tenant_a "SELECT (topic.group_join('b_group', '', 'c', 6000, 5000, 'consumer', '[{\"name\": \"range\"}]')).error")"
forbidden=$({ psql_as tenant_a "SELECT count(*) FROM topic.fetch('tenant_b.orders_q', 0, 0)" || true; } | sed -n 's/tenant_b\.orders_q/<topic>/g;1p')
missing=$({ psql_as tenant_a "SELECT count(*) FROM topic.fetch('tenant_b.ghost_q', 0, 0)" || true; } | sed -n 's/tenant_b\.ghost_q/<topic>/g;1p')
chk "SQL: fetch answers a forbidden topic like a missing one" yes "$(same "$forbidden" "$missing")"
forbidden=$({ psql_as tenant_a "SELECT topic.fetch_offset('tenant_b.orders_q', 'b_group', 0)" || true; } | sed -n 's/tenant_b\.orders_q/<topic>/g;1p')
missing=$({ psql_as tenant_a "SELECT topic.fetch_offset('tenant_b.ghost_q', 'b_group', 0)" || true; } | sed -n 's/tenant_b\.ghost_q/<topic>/g;1p')
chk "SQL: fetch_offset answers a forbidden topic like a missing one" yes "$(same "$forbidden" "$missing")"

start_listener
java_config tenant_a a-pw
forbidden=$(kafka_py tenant_a a-pw produce tenant_b.orders_q 1 | grep '^err' || true)
missing=$(kafka_py tenant_a a-pw produce tenant_b.ghost_q 1 | grep '^err' || true)
chk "Kafka: tenant_a cannot produce to tenant_b's topic" "err TOPIC_AUTHORIZATION_FAILED" "$forbidden"
chk "Kafka: Produce answers a forbidden topic like a missing one" yes "$(same "$forbidden" "$missing")"
forbidden=$(kafka_py tenant_a a-pw consume tenant_b.orders_q 0 0 1 | grep '^count\|^err' || true)
missing=$(kafka_py tenant_a a-pw consume tenant_b.ghost_q 0 0 1 | grep '^count\|^err' || true)
echo "RED: tenant_a consumes tenant_b's topic -- $forbidden"
chk "Kafka: tenant_a reads no record of tenant_b's topic" "count 0" "$forbidden"
chk "Kafka: the owner reads the same band in the same run" "count 1" \
  "$(kafka_py tenant_b b-pw consume tenant_b.orders_q 0 0 1 | grep '^count' || true)"
chk "Kafka: Metadata gives TOPIC_AUTHORIZATION_FAILED for tenant_b's topic and for a missing one" \
  "metadata TOPIC_AUTHORIZATION_FAILED metadata TOPIC_AUTHORIZATION_FAILED" \
  "$(kafka_py tenant_a a-pw metadata tenant_b.orders_q | grep "^metadata") $(kafka_py tenant_a a-pw metadata tenant_b.ghost_q | grep "^metadata")"
chk "Kafka: a consumer gets the same answer for a forbidden topic and a missing one" yes "$(same "$forbidden" "$missing")"
chk "Kafka: Produce to a band outside band_count gives TOPIC_AUTHORIZATION_FAILED (29) on a forbidden topic, as on a missing one" \
  "produce_partition 29 produce_partition 29" \
  "$(kafka_py tenant_a a-pw produce_partition tenant_b.orders_q 99 | grep '^produce_partition' || true) $(kafka_py tenant_a a-pw produce_partition tenant_b.ghost_q 99 | grep '^produce_partition' || true)"
chk "Kafka: the owner gets UNKNOWN_TOPIC_OR_PARTITION (3) for a band outside band_count" "produce_partition 3" \
  "$(kafka_py tenant_b b-pw produce_partition tenant_b.orders_q 99 | grep '^produce_partition' || true)"
forbidden=$(kafka_py tenant_a a-pw incremental_alter tenant_b.orders_q retention.ms set 1000 | grep '^incremental_alter ' || true)
missing=$(kafka_py tenant_a a-pw incremental_alter tenant_b.ghost_q retention.ms set 1000 | grep '^incremental_alter ' || true)
chk "Kafka: tenant_a cannot alter tenant_b's topic" "incremental_alter TOPIC_AUTHORIZATION_FAILED" "$forbidden"
chk "Kafka: IncrementalAlterConfigs answers a forbidden topic like a missing one" yes "$(same "$forbidden" "$missing")"
forbidden=$(kafka_py tenant_a a-pw delete_topic tenant_b.orders_q | grep '^delete_topic ' || true)
missing=$(kafka_py tenant_a a-pw delete_topic tenant_b.ghost_q | grep '^delete_topic ' || true)
chk "Kafka: tenant_a cannot delete tenant_b's topic" "delete_topic TOPIC_AUTHORIZATION_FAILED" "$forbidden"
chk "Kafka: DeleteTopics answers a forbidden topic like a missing one" yes "$(same "$forbidden" "$missing")"
forbidden=$(kafka_py tenant_a a-pw commit tenant_b.orders_q 0 3 a_group | grep '^commit ' || true)
missing=$(kafka_py tenant_a a-pw commit tenant_b.ghost_q 0 3 a_group | grep '^commit ' || true)
chk "Kafka: tenant_a cannot commit on tenant_b's topic" "commit TOPIC_AUTHORIZATION_FAILED" "$forbidden"
chk "Kafka: OffsetCommit answers a forbidden topic like a missing one" yes "$(same "$forbidden" "$missing")"
chk "Kafka: tenant_a cannot commit on tenant_b's group" "commit GROUP_AUTHORIZATION_FAILED" \
  "$(kafka_py tenant_a a-pw commit tenant_a.mine_q 0 1 b_group | grep '^commit ' || true)"
chk "Kafka: tenant_a cannot read the offsets of tenant_b's group" "committed GROUP_AUTHORIZATION_FAILED" \
  "$(kafka_py tenant_a a-pw committed tenant_a.mine_q 0 b_group | grep '^committed ' || true)"
class() {
  grep -o 'org\.apache\.kafka\.common\.errors\.[A-Za-z]*Exception' <<<"$1" | head -n1
}
forbidden=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/tenant_a.properties \
  --delete --group b_group || true)
missing=$(kafka_java kafka-consumer-groups --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/tenant_a.properties \
  --delete --group ghost_group || true)
chk "Kafka (Java CLI): tenant_a cannot delete tenant_b's group" org.apache.kafka.common.errors.GroupAuthorizationException \
  "$(class "$forbidden")"
chk "Kafka (Java CLI): DeleteGroups answers a forbidden group like a missing one" yes "$(same "$(class "$forbidden")" "$(class "$missing")")"

chk "tenant_b's topic keeps its retention, its rows and its group offset" "7 days|4|1|0" \
  "$(psql_as postgres "SELECT concat_ws('|', c.retention_interval, (SELECT count(*) FROM tenant_b.orders_q),
       (SELECT committed_offset FROM topic.topic_offsets WHERE group_name = 'b_group' AND band = 0),
       (SELECT count(*) FROM topic.topic_offsets WHERE group_name = 'a_group' AND schema_name = 'tenant_b'))
     FROM topic.topic_config c WHERE c.schema_name = 'tenant_b' AND c.topic = 'orders_q'")"
chk "tenant_a's refused grant_publish and grant_consume gave it no privilege" "false|false" \
  "$(psql_as postgres "SELECT has_any_column_privilege('tenant_a', 'tenant_b.orders_q', 'INSERT') || '|' ||
                             has_table_privilege('tenant_a', 'tenant_b.orders_q', 'SELECT')")"

psql_as tenant_b "SELECT topic.grant_publish('tenant_b.orders_q', 'pub')" >/dev/null
chk "a member of the owner role passes the fence of grant_consume" "" \
  "$(psql_as b_member "SELECT topic.grant_consume('tenant_b.orders_q', 'con')")"
chk "grant_publish gives INSERT on band, key, value, headers and producer_timestamp only, and no SELECT" \
  "band,headers,key,producer_timestamp,value|false" \
  "$(psql_as postgres "SELECT string_agg(attname, ',' ORDER BY attname) || '|' || has_table_privilege('pub', 'tenant_b.orders_q', 'SELECT')
     FROM pg_attribute WHERE attrelid = 'tenant_b.orders_q'::regclass AND attnum > 0 AND NOT attisdropped
       AND has_column_privilege('pub', attrelid, attnum, 'INSERT')")"
chk "grant_consume gives SELECT and no INSERT" "true|false" \
  "$(psql_as postgres "SELECT has_table_privilege('con', 'tenant_b.orders_q', 'SELECT') || '|' ||
                             has_any_column_privilege('con', 'tenant_b.orders_q', 'INSERT')")"

chk "SQL: a role with only grant_publish publishes" "" "$(psql_as pub "SELECT topic.publish('tenant_b.orders_q', '{\"by\": \"pub\"}')")"
chk "Kafka: a role with only grant_publish produces" 2 "$(kafka_py pub pub-pw produce tenant_b.orders_q 2 | grep -c '^ok ' || true)"
chk "the three published rows carry published_by = pub" 3 \
  "$(psql_as postgres "SELECT count(*) FROM tenant_b.orders_q WHERE published_by = 'pub'")"
for col in "log_offset|7" "published_by|'tenant_b'" "published_at|now() - interval '1 minute'" \
           "published_at|now() + interval '1 minute'"; do
  chk "a role with only grant_publish cannot set ${col%%|*} to ${col#*|}" "ERROR:  permission denied for table orders_q" \
    "$(psql_as pub "INSERT INTO tenant_b.orders_q (band, ${col%%|*}) VALUES (0, ${col#*|})")"
done
chk "a role with only grant_publish cannot set seq" "ERROR:  permission denied for table orders_q" \
  "$(psql_as pub "INSERT INTO tenant_b.orders_q (band, seq) OVERRIDING SYSTEM VALUE VALUES (0, 1)")"
chk "SQL: a role with only grant_publish cannot read" "ERROR:  permission denied for table orders_q" \
  "$(psql_as pub "SELECT count(*) FROM tenant_b.orders_q")"
chk "Kafka: a role with only grant_publish cannot fetch" "err TOPIC_AUTHORIZATION_FAILED" \
  "$(kafka_py pub pub-pw consume tenant_b.orders_q 0 0 1 | grep -o '^err [A-Z_]*' || true)"

wait_for "[ \"\$(unstamped tenant_b.orders_q)\" = 0 ]"
chk "SQL: a role with only grant_consume fetches" \
  "$(psql_as postgres "SELECT count(*) FROM tenant_b.orders_q WHERE band = 0")" \
  "$(psql_as con "SELECT count(*) FROM topic.fetch('tenant_b.orders_q', 0, 0)")"
chk "Kafka: a role with only grant_consume fetches" "count 1" \
  "$(kafka_py con con-pw consume tenant_b.orders_q 0 0 1 | grep '^count' || true)"
chk "SQL: a role with only grant_consume cannot publish" 42501 \
  "$(state_as con "SELECT topic.publish('tenant_b.orders_q', '{}')")"
chk "Kafka: a role with only grant_consume cannot produce" "err TOPIC_AUTHORIZATION_FAILED" \
  "$(kafka_py con con-pw produce tenant_b.orders_q 1 | grep '^err' || true)"

chk "a member of the owner role passes the control plane, commit and offset fences" "NONE|2" \
  "$(psql_as b_member "SELECT concat_ws('|', topic.set_retention('tenant_b.orders_q', '2 days'),
       topic.commit_offset('tenant_b.orders_q', 'b_group', 0, 2, -1),
       topic.fetch_offset('tenant_b.orders_q', 'b_group', 0))")"
chk "Kafka: a member of the owner role commits on the owner's group" "commit NONE" \
  "$(kafka_py b_member m-pw commit tenant_b.orders_q 0 3 b_group | grep '^commit ' || true)"

out=$("$PGBIN/psql" -h /tmp -p "$PORT" -U authenticator -d postgres -qtA -c "BEGIN" -c "SET LOCAL ROLE tenant_a" \
  -c "SELECT topic.caller()" -c "SELECT topic.set_retention('tenant_a.mine_q', '3 days')" \
  -c "SELECT topic.commit_offset('tenant_a.mine_q', 'b_group', 0, 1, -1)" \
  -c "SELECT topic.drop_topic('tenant_b.orders_q')" -c "ROLLBACK" 2>&1 || true)
echo "$out"
chk "PostgREST style (authenticator, SET LOCAL ROLE tenant_a): caller() is tenant_a" tenant_a "$(sed -n 1p <<<"$out")"
chk "PostgREST style: tenant_a's own topic passes the fence" "" "$(sed -n 2p <<<"$out")"
chk "PostgREST style: tenant_b's group is refused" GROUP_AUTHORIZATION_FAILED "$(sed -n 3p <<<"$out")"
chk "PostgREST style: tenant_b's topic is refused" "ERROR:  topic: role tenant_a is not a member of tenant_b, the owner of topic tenant_b.orders_q" \
  "$(sed -n 4p <<<"$out")"

psql_as tenant_b "CREATE TABLE tenant_b.seen (at text, who name, super boolean);
  CREATE FUNCTION tenant_b.record() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN
      INSERT INTO tenant_b.seen SELECT TG_ARGV[0], current_user, rolsuper FROM pg_roles WHERE rolname = current_user;
      RETURN NEW;
  END \$\$;
  CREATE FUNCTION tenant_b.reset_role() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN
      RESET ROLE;
      INSERT INTO tenant_b.seen SELECT TG_ARGV[0], current_user, rolsuper FROM pg_roles WHERE rolname = current_user;
      RETURN NEW;
  END \$\$;
  CREATE FUNCTION tenant_b.set_role() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN
      SET ROLE postgres;
      INSERT INTO tenant_b.seen SELECT TG_ARGV[0], current_user, rolsuper FROM pg_roles WHERE rolname = current_user;
      RETURN NEW;
  END \$\$;
  CREATE FUNCTION tenant_b.squat() RETURNS trigger LANGUAGE plpgsql AS \$\$
  BEGIN PERFORM topic.drop_topic('tenant_a.mine_q'); RETURN NEW; END \$\$;
  CREATE FUNCTION tenant_b.check_escape(o bigint) RETURNS boolean LANGUAGE plpgsql AS \$\$
  BEGIN
      IF o IS NOT NULL THEN
          RESET ROLE;
          INSERT INTO tenant_b.seen SELECT 'check', current_user, rolsuper FROM pg_roles WHERE rolname = current_user;
      END IF;
      RETURN true;
  END \$\$;
  CREATE FUNCTION tenant_b.index_escape(o bigint) RETURNS bigint IMMUTABLE LANGUAGE sql
  AS \$\$ SELECT CASE WHEN o IS NULL THEN 0 ELSE length(set_config('role', 'postgres', false)) END \$\$;
  SELECT topic.create_table_topic('tenant_b.watched', '{\"w_id\": \"int\", \"n\": \"int\"}', 'w_id', 1);
  CREATE TRIGGER record BEFORE UPDATE ON tenant_b.watched_q FOR EACH ROW EXECUTE FUNCTION tenant_b.record('stamp');
  CREATE TRIGGER record BEFORE INSERT OR UPDATE ON tenant_b.watched FOR EACH ROW EXECUTE FUNCTION tenant_b.record('sync');
  SELECT topic.create_topic('tenant_b.qreset_q', 1);
  CREATE TRIGGER escape BEFORE UPDATE ON tenant_b.qreset_q FOR EACH ROW EXECUTE FUNCTION tenant_b.reset_role('stamp');
  SELECT topic.create_topic('tenant_b.qsetrole_q', 1);
  CREATE TRIGGER escape BEFORE UPDATE ON tenant_b.qsetrole_q FOR EACH ROW EXECUTE FUNCTION tenant_b.set_role('stamp');
  SELECT topic.create_topic('tenant_b.qcaller_q', 1);
  CREATE TRIGGER escape BEFORE UPDATE ON tenant_b.qcaller_q FOR EACH ROW EXECUTE FUNCTION tenant_b.squat();
  SELECT topic.create_topic('tenant_b.qcheck_q', 1);
  ALTER TABLE tenant_b.qcheck_q ADD CONSTRAINT escape CHECK (tenant_b.check_escape(log_offset));
  SELECT topic.create_topic('tenant_b.qindex_q', 1);
  CREATE INDEX ON tenant_b.qindex_q (tenant_b.index_escape(log_offset));
  SELECT topic.create_table_topic('tenant_b.breset', '{\"w_id\": \"int\"}', 'w_id', 1);
  CREATE TRIGGER escape BEFORE INSERT ON tenant_b.breset FOR EACH ROW EXECUTE FUNCTION tenant_b.reset_role('sync');
  SELECT topic.create_table_topic('tenant_b.bsetrole', '{\"w_id\": \"int\"}', 'w_id', 1);
  CREATE TRIGGER escape BEFORE INSERT ON tenant_b.bsetrole FOR EACH ROW EXECUTE FUNCTION tenant_b.set_role('sync')" >/dev/null
for t in qreset_q qsetrole_q qcaller_q qcheck_q qindex_q breset_q bsetrole_q; do
  psql_as tenant_b "SELECT topic.publish('tenant_b.$t', '{\"w_id\": 1}')" >/dev/null
done
synced() {
  wait_for "[ \"\$(psql_as postgres \"SELECT n FROM tenant_b.watched WHERE w_id = 1\")\" = $1 ]" && echo yes || echo no
}
psql_as tenant_b "SELECT topic.publish('tenant_b.watched_q', '{\"w_id\": 1, \"n\": 1}')" >/dev/null
chk "a clean topic is stamped and synced while the escape topics fail" yes "$(synced 1)"
psql_as tenant_b "SELECT topic.publish('tenant_b.watched_q', '{\"w_id\": 1, \"n\": 2}')" >/dev/null
chk "a clean topic is still stamped and synced on a later pass" yes "$(synced 2)"
chk "a tenant trigger on its queue table (stamp) and on its base table (sync) runs as the tenant, never as a superuser" \
  "stamp:tenant_b:false,sync:tenant_b:false" \
  "$(psql_as postgres "SELECT string_agg(DISTINCT at || ':' || who || ':' || super, ',') FROM tenant_b.seen")"

restricted='cannot set parameter "role" within security-definer function'
for t in qreset_q qsetrole_q qcheck_q qindex_q; do
  chk "the stamper leaves tenant_b.$t unstamped" 1 "$(unstamped tenant_b.$t)"
  out=$(psql_as postgres "SELECT topic.stamp_topic('tenant_b', '$t')" || true)
  echo "RED: stamp of tenant_b.$t -- $out"
  chk "the escape on tenant_b.$t fails during the stamp" yes "$(grep -qF "$restricted" <<<"$out" && echo yes || echo no)"
done
chk "the stamper leaves tenant_b.qcaller_q unstamped" 1 "$(unstamped tenant_b.qcaller_q)"
out=$(psql_as postgres "SELECT topic.stamp_topic('tenant_b', 'qcaller_q')" || true)
echo "RED: stamp of tenant_b.qcaller_q -- $out"
chk "caller() refuses in a restricted context, so a tenant trigger cannot use the superuser identity" yes \
  "$(grep -qF 'topic.caller: refused inside a security-restricted operation' <<<"$out" && echo yes || echo no)"
chk "tenant_a's topic survives the attempt to drop it from tenant_b's trigger" t \
  "$(psql_as postgres "SELECT to_regclass('tenant_a.mine_q') IS NOT NULL")"
for t in breset_q bsetrole_q; do
  wait_for "[ \"\$(unstamped tenant_b.$t)\" = 0 ]"
  out=$(psql_as postgres "SELECT topic.sync_topic('tenant_b', '$t')" || true)
  echo "RED: sync of tenant_b.$t -- $out"
  chk "the escape on the base table of tenant_b.$t fails during the sync" yes \
    "$(grep -qF "$restricted" <<<"$out" && echo yes || echo no)"
  chk "the failed sync of tenant_b.$t keeps its position, writes no base row and no error row" "0|0|0" \
    "$(psql_as postgres "SELECT concat_ws('|', (SELECT committed_offset FROM topic.topic_offsets
                                                  WHERE group_name = '__pg_topics_sync:tenant_b.$t'),
                                                 (SELECT count(*) FROM tenant_b.${t%_q}),
                                                 (SELECT count(*) FROM tenant_b.${t}e))")"
done
chk "the stamper logs the refused escape and tries again" yes \
  "$([ "$(grep -cF "pg_topics stamper: $restricted. The stamper tries again in 1 s." "$PGDATA/log")" -ge 2 ] && echo yes || echo no)"
chk "the sync worker logs the refused escape and tries again" yes \
  "$([ "$(grep -cF "pg_topics sync worker: $restricted. The sync worker tries again in 1 s." "$PGDATA/log")" -ge 2 ] && echo yes || echo no)"
chk "no tenant statement ran as a superuser" 0 "$(psql_as postgres "SELECT count(*) FROM tenant_b.seen WHERE super")"

hold_as() {
  local role=$1
  shift
  "$PGBIN/psql" -h /tmp -p "$PORT" -U "$role" -d postgres -q -o /dev/null -c "BEGIN" "$@" -c "SELECT pg_sleep(3600)" >/dev/null 2>&1 &
  wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND usename = '$role'\")\" = 1 ]"
}
release_holds() {
  psql_as postgres "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'PgSleep' AND usename <> 'postgres'" >/dev/null
  wait_for "psql_as tenant_b \"SELECT topic.publish('tenant_b.orders_q', '{}')\" >/dev/null"
}
served() {
  psql_as tenant_b "SELECT topic.publish('tenant_b.orders_q', '{}'), topic.publish('tenant_b.watched_q', '{\"w_id\": 1, \"n\": $1}')" >/dev/null
  wait_for "[ \"\$(unstamped tenant_b.orders_q)\" = 0 ]" && [ "$(synced "$1")" = yes ] && sleep 3 &&
    psql_as tenant_b "SELECT topic.publish('tenant_b.orders_q', '{}')" >/dev/null && echo yes || echo no
}
psql_as tenant_b "SELECT topic.set_backlog_limit('tenant_b.orders_q', '2 seconds')" >/dev/null
hold_as tenant_a -c "SELECT pg_advisory_lock(1885828211, hashtext('tenant_b.orders_q')),
                            pg_advisory_lock(1885827961, hashtext('tenant_b.watched_q'))"
chk "while tenant_a holds the old stamper and sync advisory keys of tenant_b's topics, tenant_b is stamped, synced and can publish" \
  yes "$(served 3)"
release_holds
hold_as tenant_a -c "SELECT topic.group_heartbeat('__pg_topics_sync:tenant_b.watched_q', 'x', 0)" \
  -c "SELECT topic.group_heartbeat('b_group', 'x', 0)"
chk "while tenant_a holds open group calls on tenant_b's sync group and group, tenant_b is stamped, synced and can publish" \
  yes "$(served 4)"
chk "while tenant_a holds an open group call on tenant_b's group, tenant_b commits on it" NONE \
  "$(PGOPTIONS='-c lock_timeout=2s' psql_as tenant_b "SELECT topic.commit_offset('tenant_b.orders_q', 'b_group', 0, 4, -1)")"
release_holds
hold_as con -c "SELECT topic.commit_offset('tenant_b.orders_q', 'con_g', 0, 0, -1)"
chk "while a consumer holds an open offset commit on tenant_b's topic, tenant_b is stamped, synced and can publish" \
  yes "$(served 5)"
release_holds

out=$(psql_as tenant_a "SELECT topic.create_topic('public.\"a\"\"; DROP TABLE public.x; --_q\"')" || true)
echo "RED: DDL injection through create_topic -- $out"
chk "DDL injection through the create_topic name fails on the name rule" yes \
  "$(grep -q 'is not a valid topic name' <<<"$out" && echo yes || echo no)"
out=$(psql_as tenant_a "SELECT topic.create_table_topic('tenant_a.inj', '{\"id\": \"int; DROP TABLE public.x\"}', 'id')" || true)
echo "RED: DDL injection through a create_table_topic column type -- $out"
chk "DDL injection through a create_table_topic column type fails with a syntax error (42601)" 42601 \
  "$(state_as tenant_a "SELECT topic.create_table_topic('tenant_a.inj', '{\"id\": \"int; DROP TABLE public.x\"}', 'id')")"
chk "the injections dropped nothing and made nothing" "true|true" \
  "$(psql_as postgres "SELECT (to_regclass('public.x') IS NOT NULL) || '|' || (to_regclass('tenant_a.inj') IS NULL)")"

out=$(psql_as b_member "UPDATE topic.topic_offsets SET committed_offset = 999" || true)
chk "a member of the group owner cannot UPDATE topic.topic_offsets directly" "ERROR:  permission denied for table topic_offsets" "$out"
for t in topic_config topic_band_position topic_groups topic_group_members topic_offsets topic_producers producer_ids; do
  chk "tenant_b cannot DELETE from topic.$t directly" "ERROR:  permission denied for table $t" \
    "$(psql_as tenant_b "DELETE FROM topic.$t")"
done
chk "no role but the extension owner may INSERT, UPDATE, DELETE or TRUNCATE a control table" "" \
  "$(psql_as postgres "SELECT string_agg(r.rolname || ':' || t::text, ',') FROM pg_roles r,
     unnest('{topic.topic_config,topic.topic_band_position,topic.topic_groups,topic.topic_group_members,
              topic.topic_offsets,topic.topic_producers,topic.producer_ids}'::regclass[]) t
     WHERE r.rolname !~ '^pg_' AND r.oid <> (SELECT extowner FROM pg_extension WHERE extname = 'pg_topics')
       AND (has_table_privilege(r.oid, t, 'INSERT, UPDATE, DELETE, TRUNCATE')
            OR has_any_column_privilege(r.oid, t, 'INSERT, UPDATE'))")"

chk "lint: every function in schema topic has search_path=pg_catalog, pg_temp" "" \
  "$(psql_as postgres "SELECT string_agg(oid::regprocedure::text, ',') FROM pg_proc
     WHERE pronamespace = 'topic'::regnamespace
       AND NOT coalesce('search_path=pg_catalog, pg_temp' = ANY (proconfig), false)")"
chk "lint: every function in schema topic is owned by the extension owner" "" \
  "$(psql_as postgres "SELECT string_agg(p.oid::regprocedure::text, ',') FROM pg_proc p, pg_extension e
     WHERE e.extname = 'pg_topics' AND p.pronamespace = 'topic'::regnamespace AND p.proowner <> e.extowner")"
chk "lint: no role has BYPASSRLS but the bootstrap superuser" postgres \
  "$(psql_as postgres "SELECT string_agg(rolname, ',') FROM pg_roles WHERE rolbypassrls")"
chk "lint: no SECURITY DEFINER function is owned by a NOLOGIN role" "" \
  "$(psql_as postgres "SELECT string_agg(p.oid::regprocedure::text, ',') FROM pg_proc p
     JOIN pg_roles r ON r.oid = p.proowner WHERE p.prosecdef AND NOT r.rolcanlogin")"
chk "lint: the five workers connect as the bootstrap superuser" "5|postgres" \
  "$(psql_as postgres "SELECT count(*) || '|' || string_agg(DISTINCT usename, ',') FROM pg_stat_activity
     WHERE backend_type LIKE 'pg_topics %'")"

psql_as postgres "CREATE ROLE hijacker LOGIN" >/dev/null
psql_as postgres "CREATE DATABASE hijack_db" >/dev/null
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d hijack_db -c "CREATE SCHEMA topic AUTHORIZATION hijacker" >/dev/null
out=$("$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d hijack_db -c "CREATE EXTENSION pg_topics" 2>&1 || true)
echo "RED: CREATE EXTENSION with schema topic pre-owned by a non-superuser -- $out"
chk "CREATE EXTENSION refuses a topic schema that a non-superuser owns" yes \
  "$(grep -q 'schema topic already exists' <<<"$out" && echo yes || echo no)"
chk "the refused CREATE EXTENSION installs nothing in hijack_db" f \
  "$("$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d hijack_db -tAc "SELECT EXISTS (SELECT FROM pg_extension WHERE extname = 'pg_topics')")"

psql_as postgres "CREATE DATABASE reinstall_db" >/dev/null
"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d reinstall_db -c "CREATE EXTENSION pg_topics" -c "DROP EXTENSION pg_topics" >/dev/null
out=$("$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d reinstall_db -c "CREATE EXTENSION pg_topics" 2>&1 || true)
echo "CREATE EXTENSION after DROP EXTENSION -- $out"
chk "CREATE EXTENSION works again after DROP EXTENSION" yes \
  "$(grep -qx 'CREATE EXTENSION' <<<"$out" && echo yes || echo no)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
