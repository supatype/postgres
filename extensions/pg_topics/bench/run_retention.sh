#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "ALTER SYSTEM SET track_functions = 'all'" >/dev/null
psql_as postgres "SELECT pg_reload_conf()" >/dev/null
psql_as postgres "CREATE ROLE tenant_a LOGIN; CREATE SCHEMA tenant_a AUTHORIZATION tenant_a;
                  CREATE ROLE tenant_b LOGIN; CREATE SCHEMA tenant_b AUTHORIZATION tenant_b;" >/dev/null

LO=$(psql_as postgres "SELECT date_bin('1 minute', now(), '2000-01-01')")

worker_pid() {
  psql_as postgres "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_topics partition'"
}

ticks() {
  psql_as postgres "SELECT coalesce((SELECT calls FROM pg_stat_user_functions WHERE schemaname = 'topic' AND funcname = 'reap'), 0)"
}

part() {
  psql_as postgres "SELECT '${1#*.}_p' || to_char(('$LO'::timestamptz - $2 * interval '1 minute') AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS')"
}

at() {
  echo "'$LO'::timestamptz - ($1 - 0.5) * interval '1 minute'"
}

old_partitions() {
  echo "DO \$\$ DECLARE p text; BEGIN
    FOR i IN 1..$2 LOOP
      p := '${1#*.}_p' || to_char(('$LO'::timestamptz - i * interval '1 minute') AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS');
      EXECUTE format('CREATE TABLE ${1%%.*}.%I PARTITION OF $1 FOR VALUES FROM (%L) TO (%L)', p,
                     '$LO'::timestamptz - i * interval '1 minute', '$LO'::timestamptz - (i - 1) * interval '1 minute');
      EXECUTE format('CREATE UNIQUE INDEX ON ${1%%.*}.%I (band, log_offset) WHERE log_offset IS NOT NULL', p);
    END LOOP; END \$\$;"
}

topic_sql() {
  echo "SELECT topic.create_topic('$1', $2, retention => '${3:-2 minutes}', partition_interval => '1 minute');"
}

set_retention() {
  psql_as postgres "UPDATE topic.topic_config SET retention_interval = '2 minutes' WHERE topic = '$1'" >/dev/null
}

exists() {
  psql_as postgres "SELECT to_regclass('${1%%.*}.$2') IS NOT NULL"
}

attached() {
  psql_as postgres "SELECT coalesce((SELECT NOT inhdetachpending FROM pg_inherits
                                     WHERE inhrelid = to_regclass('${1%%.*}.$2') AND inhparent = '$1'::regclass)::text, 'no')"
}

future() {
  psql_as postgres "SELECT count(*) FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
                    WHERE h.inhparent = '$1'::regclass AND right(c.relname, 14)
                    > to_char(date_bin('1 minute', now(), '2000-01-01') AT TIME ZONE 'UTC', 'YYYYMMDDHH24MISS')"
}

floor_ok() {
  psql_as postgres "SELECT bool_and(p.oldest_offset = coalesce(q.low, p.next_offset))
    FROM topic.topic_band_position p
    LEFT JOIN (SELECT band, min(log_offset) AS low FROM $1 GROUP BY band) q ON q.band = p.band
    WHERE p.schema_name = '${1%%.*}' AND p.topic = '${1#*.}'"
}

logged() {
  wait_for "grep -q -- \"$1\" '$PGDATA/log'" && echo yes || echo no
}

hide() {
  echo "ALTER TABLE $1.$2 ENABLE ROW LEVEL SECURITY; ALTER TABLE $1.$2 FORCE ROW LEVEL SECURITY;
        CREATE POLICY hide ON $1.$2 USING (false);"
}

detaching_is() {
  psql_as postgres "SELECT coalesce(detaching::text, 'NULL') FROM topic.topic_config WHERE topic = '$1'"
}

held() {
  psql_as postgres "SELECT retention_hold_until > clock_timestamp() FROM topic.topic_config WHERE topic = '$1'"
}

utc() {
  PGOPTIONS='-c TimeZone=UTC' psql_as postgres "$1"
}

fifo_session() {
  mkfifo "$WORK/$1.fifo"
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -f "$WORK/$1.fifo" >"$WORK/$1.out" 2>&1 &
}

"$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -v ON_ERROR_STOP=1 >/dev/null 2>&1 <<SQL
BEGIN;
$(topic_sql public.ret_q 2)
$(old_partitions public.ret_q 4)
INSERT INTO public.ret_q (band, value, published_at)
SELECT b, '{}', t FROM (VALUES (0, $(at 4)), (0, $(at 4)), (1, $(at 4)), (0, $(at 3)), (0, $(at 3)),
                               (1, $(at 3)), (0, $(at 1)), (0, $(at 1))) v(b, t);
SELECT topic.stamp_topic('public', 'ret_q');

$(topic_sql public.wait_q 1 '1 hour')
$(old_partitions public.wait_q 3)
INSERT INTO public.wait_q (band, value, published_at) VALUES (0, '{}', $(at 3)), (0, '{}', $(at 1));
SELECT topic.stamp_topic('public', 'wait_q');

$(topic_sql public.hand_q 1)
$(old_partitions public.hand_q 3)
INSERT INTO public.hand_q (band, value, published_at) VALUES (0, '{}', $(at 3));
SELECT topic.stamp_topic('public', 'hand_q');
ALTER TABLE public.hand_q DETACH PARTITION public.$(part public.hand_q 3);

$(topic_sql public.user_q 1 '1 hour')
$(old_partitions public.user_q 3)
INSERT INTO public.user_q (band, value, published_at) VALUES (0, '{}', $(at 3));
SELECT topic.stamp_topic('public', 'user_q');

$(topic_sql public.odd_q 1)
CREATE TABLE public.odd_q_old PARTITION OF public.odd_q
    FOR VALUES FROM ('$LO'::timestamptz - interval '3 minutes') TO ('$LO'::timestamptz - interval '2 minutes');
CREATE UNIQUE INDEX ON public.odd_q_old (band, log_offset) WHERE log_offset IS NOT NULL;
INSERT INTO public.odd_q (band, value, published_at) VALUES (0, '{}', $(at 3));

SET LOCAL ROLE tenant_b;
$(topic_sql tenant_b.dup_q 1 '1 hour')
SELECT topic.publish('tenant_b.dup_q', '{}') FROM generate_series(1, 3);
RESET ROLE;
SELECT topic.stamp_topic('tenant_b', 'dup_q');

$(topic_sql public.crash_q 1 '1 hour')
$(old_partitions public.crash_q 3)
INSERT INTO public.crash_q (band, value, published_at) VALUES (0, '{}', $(at 3)), (0, '{}', $(at 1));
SELECT topic.stamp_topic('public', 'crash_q');

SET LOCAL ROLE tenant_b;
$(topic_sql tenant_b.rls_q 1)
$(old_partitions tenant_b.rls_q 4)
INSERT INTO tenant_b.rls_q (band, value, published_at) VALUES (0, '{}', $(at 4)), (0, '{}', $(at 3));
RESET ROLE;
SELECT topic.stamp_topic('tenant_b', 'rls_q');
SET LOCAL ROLE tenant_b;
$(hide tenant_b "$(part tenant_b.rls_q 4)")
$(hide tenant_b "$(part tenant_b.rls_q 3)")

SET LOCAL ROLE tenant_a;
$(topic_sql tenant_a.probe_q 1)
CREATE FUNCTION tenant_a.probe(tag text, b smallint) RETURNS boolean LANGUAGE plpgsql IMMUTABLE AS \$\$
BEGIN
  RAISE WARNING 'pgt_probe % % %', tag, current_user, (SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user);
  RETURN b IS NOT NULL;
END \$\$;
CREATE FUNCTION tenant_a.probe_trigger() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN PERFORM tenant_a.probe('trigger', NEW.band); RETURN NEW; END \$\$;
ALTER TABLE tenant_a.probe_q ADD CONSTRAINT probe_check CHECK (tenant_a.probe('check', band));
CREATE INDEX probe_index ON tenant_a.probe_q (tenant_a.probe('index', band));
CREATE TRIGGER probe BEFORE INSERT OR UPDATE ON tenant_a.probe_q FOR EACH ROW EXECUTE FUNCTION tenant_a.probe_trigger();
$(old_partitions tenant_a.probe_q 4)
INSERT INTO tenant_a.probe_q (band, value, published_at) VALUES (0, '{}', $(at 4));
RESET ROLE;
SELECT topic.stamp_topic('tenant_a', 'probe_q');
SET LOCAL ROLE tenant_a;
INSERT INTO tenant_a.probe_q (band, value, published_at) VALUES (0, '{"unstamped": 1}', $(at 3));

$(topic_sql tenant_a.evil_q 1)

$(topic_sql tenant_a.late_q 1)
ALTER TABLE tenant_a.late_q ADD CONSTRAINT probe_check CHECK (tenant_a.probe('check', band));
CREATE INDEX late_index ON tenant_a.late_q (tenant_a.probe('index', band));
CREATE TRIGGER probe BEFORE INSERT OR UPDATE ON tenant_a.late_q FOR EACH ROW EXECUTE FUNCTION tenant_a.probe_trigger();
$(old_partitions tenant_a.late_q 3)
INSERT INTO tenant_a.late_q (band, value, published_at) VALUES (0, '{}', $(at 3));
$(hide tenant_a "$(part tenant_a.late_q 3)")
RESET ROLE;
CREATE FUNCTION public.hold() RETURNS trigger LANGUAGE plpgsql AS \$\$
BEGIN RAISE EXCEPTION 'the harness holds the stamper off'; END \$\$;
CREATE TRIGGER a_hold BEFORE UPDATE ON tenant_a.probe_q FOR EACH ROW EXECUTE FUNCTION public.hold();
CREATE TRIGGER a_hold BEFORE UPDATE ON tenant_a.late_q FOR EACH ROW EXECUTE FUNCTION public.hold();
CREATE TRIGGER a_hold BEFORE UPDATE ON public.odd_q FOR EACH ROW EXECUTE FUNCTION public.hold();
COMMIT;
SQL
chk "the setup commits" 11 "$(psql_as postgres "SELECT count(*) FROM topic.topic_config")"
probes=$(grep -c pgt_probe "$PGDATA/log")

fifo_session a
exec 3>"$WORK/a.fifo"
echo "BEGIN; INSERT INTO public.wait_q (band, value, published_at) VALUES (0, '{\"a\": 1}', $(at 3));" >&3
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_locks WHERE relation = 'public.wait_q'::regclass AND mode = 'RowExclusiveLock'\")\" = 1 ]"
set_retention wait_q
WAIT_P=$(part public.wait_q 3)

chk "the partition worker runs on one thread" 1 "$(ls "/proc/$(worker_pid)/task" | wc -l)"

chk "the worker makes 3 future partitions" yes \
  "$(wait_for "[ \"\$(future public.ret_q)\" = 3 ]" && echo yes || echo no)"
chk "the worker drops the oldest expired partition" yes \
  "$(wait_for "[ \"\$(exists public.ret_q "$(part public.ret_q 4)")\" = f ]" && echo yes || echo no)"
chk "the worker drops the next expired partition" yes \
  "$(wait_for "[ \"\$(exists public.ret_q "$(part public.ret_q 3)")\" = f ]" && echo yes || echo no)"
chk "oldest_offset equals the lowest surviving offset per band" t "$(floor_ok public.ret_q)"
chk "oldest_offset of band 0 went up" t \
  "$(psql_as postgres "SELECT oldest_offset > 0 FROM topic.topic_band_position WHERE topic = 'ret_q' AND band = 0")"
chk "a band with no surviving stamped rows gets oldest_offset = next_offset" "true:true" \
  "$(psql_as postgres "SELECT (next_offset > 0) || ':' || (oldest_offset = next_offset)
                       FROM topic.topic_band_position WHERE topic = 'ret_q' AND band = 1")"
chk "a read below oldest_offset returns zero rows" 0 \
  "$(psql_as postgres "SELECT count(*) FROM public.ret_q q JOIN topic.topic_band_position p
                       ON p.topic = 'ret_q' AND p.band = q.band WHERE q.log_offset < p.oldest_offset")"
chk "detaching is NULL after the drop" t \
  "$(psql_as postgres "SELECT detaching IS NULL FROM topic.topic_config WHERE topic = 'ret_q'")"

chk "the detach of the partition that A writes starts" yes \
  "$(logged "retention detaches public.$WAIT_P from public.wait_q")"
chk "the detach waits for A and times out, so the worker tries again" yes \
  "$(wait_for "[ \"\$(grep -c 'public.wait_q retention: canceling statement due to lock timeout' '$PGDATA/log')\" -ge 2 ]" && echo yes || echo no)"
chk "the detach does not complete while A is open" t \
  "$(psql_as postgres "SELECT inhdetachpending FROM pg_inherits WHERE inhrelid = to_regclass('public.$WAIT_P')")"
publishes=""
for i in 1 2 3; do
  publishes="$publishes$(PGOPTIONS='-c statement_timeout=2s' psql_as postgres "SELECT topic.publish('public.wait_q', '{\"b\": $i}')" >/dev/null && echo ok || echo fail) "
done
chk "publishes to other partitions work while A is open" "ok ok ok " "$publishes"
newest() {
  psql_as postgres "SELECT max(right(c.relname, 14)) FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
                    WHERE h.inhparent = 'public.wait_q'::regclass"
}
top=$(newest)
(while [ ! -f "$WORK/stop_pub" ]; do
  PGOPTIONS='-c lock_timeout=200ms' psql_as postgres "SELECT topic.publish('public.wait_q', '{}')" >"$WORK/pub.out" \
    || head -1 "$WORK/pub.out" >>"$WORK/blocked"
  sleep 0.2
done) &
pub_pid=$!
chk "the worker makes a new partition while A holds its insert open" yes \
  "$( (wait_for "[ \"\$(newest)\" \> $top ]" || wait_for "[ \"\$(newest)\" \> $top ]") && echo yes || echo no)"
touch "$WORK/stop_pub"
wait "$pub_pid"
chk "no publish waits for a lock while the worker makes partitions" "" "$(sort -u "$WORK/blocked" 2>/dev/null)"
chk "partition creation never times out behind A" 0 "$(grep -c 'public.wait_q partition creation' "$PGDATA/log" || true)"
echo "COMMIT;" >&3
exec 3>&-
chk "after A commits, retention finds A's unstamped row and attaches the partition again" yes \
  "$(logged "public.$WAIT_P has rows with no log_offset, so retention attached it again to public.wait_q")"
chk "A's row gets an offset before the drop" yes \
  "$(wait_for "[ -n \"\$(psql_as postgres \"SELECT log_offset FROM public.wait_q WHERE value->>'a' = '1'\")\" ]" && echo yes || echo no)"
chk "retention holds the topic after it attaches a partition again" t "$(held wait_q)"
start=$(ticks)
wait_for "[ \"\$(ticks)\" -ge $((start + 2)) ]"
chk "the attached partition survives the next 2 ticks" "true:t" "$(attached public.wait_q "$WAIT_P"):$(held wait_q)"
psql_as postgres "UPDATE topic.topic_config SET retention_hold_until = NULL WHERE topic = 'wait_q'" >/dev/null
chk "the partition is dropped after the hold ends" yes \
  "$(wait_for "[ \"\$(exists public.wait_q "$WAIT_P")\" = f ]" && echo yes || echo no)"
chk "oldest_offset of wait_q equals the lowest surviving offset" t "$(floor_ok public.wait_q)"

PROBE_OLD=$(part tenant_a.probe_q 4)
PROBE_P=$(part tenant_a.probe_q 3)
chk "the worker drops the expired tenant partition with only stamped rows" yes \
  "$(wait_for "[ \"\$(exists tenant_a.probe_q "$PROBE_OLD")\" = f ]" && echo yes || echo no)"
chk "a partition with an unstamped row is attached again, with a WARNING" yes \
  "$(logged "WARNING:  topic: tenant_a.$PROBE_P has rows with no log_offset, so retention attached it again to tenant_a.probe_q")"
chk "the partition with the unstamped row stays attached and keeps the row" "true:1" \
  "$(wait_for "[ \"\$(attached tenant_a.probe_q "$PROBE_P")\" = t ]" >/dev/null; echo "$(attached tenant_a.probe_q "$PROBE_P"):$(psql_as postgres "SELECT count(*) FROM tenant_a.probe_q WHERE log_offset IS NULL")")"
chk "retention holds the tenant topic after the attach" t "$(held probe_q)"
chk "the worker makes the future partitions of the tenant topic" yes \
  "$(wait_for "[ \"\$(future tenant_a.probe_q)\" = 3 ]" && echo yes || echo no)"
chk "the tenant functions ran" yes "$(grep -q 'pgt_probe trigger tenant_a f' "$PGDATA/log" && echo yes || echo no)"
chk "CREATE, DETACH, ATTACH and DROP run no tenant function as a superuser" 0 \
  "$(grep -c 'pgt_probe .* t$' "$PGDATA/log" || true)"
chk "CREATE, DETACH, ATTACH and DROP run no tenant function at all" "$probes" "$(grep -c pgt_probe "$PGDATA/log")"

RLS_P=$(part tenant_b.rls_q 4)
chk "retention_check raises an error on a partition with forced row level security" yes \
  "$(logged "tenant_b.rls_q retention: query would be affected by row-level security policy for table")"
chk "the partition with forced row level security is not dropped" "t:t" \
  "$(echo "$(exists tenant_b.rls_q "$RLS_P"):$(psql_as postgres "SELECT detaching = 'tenant_b.$RLS_P'::regclass FROM topic.topic_config WHERE topic = 'rls_q'")")"

psql_as tenant_b "ALTER TABLE tenant_b.$RLS_P RENAME TO kept_p4" >/dev/null
chk "retention leaves a detached table that the tenant renamed, with a WARNING" yes \
  "$(logged "WARNING:  topic: tenant_b.kept_p4 was renamed or attached to another table after retention detached it from tenant_b.rls_q")"
RLS_P3=$(part tenant_b.rls_q 3)
chk "retention goes on to the next partition" yes \
  "$(logged "rls_q retention: query would be affected by row-level security policy for table .$RLS_P3.")"
chk "the renamed table is kept" t "$(exists tenant_b.rls_q kept_p4)"
psql_as tenant_b "CREATE TABLE tenant_b.other (LIKE tenant_b.rls_q INCLUDING DEFAULTS INCLUDING CONSTRAINTS) PARTITION BY RANGE (published_at);
                  ALTER TABLE tenant_b.other ATTACH PARTITION tenant_b.$RLS_P3 FOR VALUES FROM (MINVALUE) TO (MAXVALUE)" >/dev/null
chk "retention leaves a detached table that the tenant attached to another table, with a WARNING" yes \
  "$(logged "WARNING:  topic: tenant_b.$RLS_P3 was renamed or attached to another table after retention detached it from tenant_b.rls_q")"
chk "the table attached to another table is kept there" "tenant_b.other" \
  "$(psql_as postgres "SELECT inhparent::regclass FROM pg_inherits WHERE inhrelid = 'tenant_b.$RLS_P3'::regclass")"

HAND_P=$(part public.hand_q 3)
start=$(ticks)
chk "the worker ticks" yes "$(wait_for "[ \"\$(ticks)\" -ge $((start + 3)) ]" && echo yes || echo no)"
chk "a partition that the user detached is still present after 3 ticks" "t:t" \
  "$(echo "$(exists public.hand_q "$HAND_P"):$(psql_as postgres "SELECT detaching IS NULL FROM topic.topic_config WHERE topic = 'hand_q'")")"

USER_P=$(part public.user_q 3)
fifo_session u
exec 6>"$WORK/u.fifo"
echo "BEGIN; LOCK TABLE public.$USER_P IN SHARE UPDATE EXCLUSIVE MODE;" >&6
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_locks WHERE relation = 'public.$USER_P'::regclass AND granted\")\" = 1 ]"
set_retention user_q
chk "the worker's DETACH of the partition fails while the user holds a lock" yes \
  "$(logged "public.user_q retention: canceling statement due to lock timeout")"
chk "detaching is NULL after the failed DETACH, because the partition is still attached" NULL \
  "$(wait_for "[ \"\$(detaching_is user_q)\" = NULL ]"; detaching_is user_q)"
echo "ALTER TABLE public.user_q DETACH PARTITION public.$USER_P; COMMIT;" >&6
exec 6>&-
wait_for "[ \"\$(attached public.user_q "$USER_P")\" = no ]"
start=$(ticks)
wait_for "[ \"\$(ticks)\" -ge $((start + 2)) ]"
chk "a partition that the user detached after a failed DETACH is still present" "t:no" \
  "$(exists public.user_q "$USER_P"):$(attached public.user_q "$USER_P")"

LATE_P=$(part tenant_a.late_q 3)
chk "the late tenant partition is detached and held by its row level security" yes \
  "$(logged "tenant_a.late_q retention: query would be affected by row-level security policy")"
psql_as tenant_a "CREATE INDEX late_index2 ON tenant_a.late_q (tenant_a.probe('index2', band));
                  ALTER TABLE tenant_a.late_q ADD CONSTRAINT late_check2 CHECK (tenant_a.probe('check2', band));
                  ALTER TABLE tenant_a.$LATE_P NO FORCE ROW LEVEL SECURITY;
                  ALTER TABLE tenant_a.$LATE_P DISABLE ROW LEVEL SECURITY" >/dev/null
chk "the attach of a table that lacks a new parent constraint fails with no tenant code" yes \
  "$(logged "tenant_a.late_q retention: child table is missing constraint .late_check2.")"
chk "the table stays detached and named in detaching" "tenant_a.$LATE_P" "$(detaching_is late_q)"
psql_as tenant_a "ALTER TABLE tenant_a.late_q DROP CONSTRAINT late_check2" >/dev/null
chk "after the tenant drops the constraint, retention attaches the table again" yes \
  "$(logged "WARNING:  topic: tenant_a.$LATE_P has rows with no log_offset, so retention attached it again to tenant_a.late_q")"
chk "the attach builds the new tenant index as the owner" yes \
  "$(grep -q 'pgt_probe index2 tenant_a f' "$PGDATA/log" && echo yes || echo no)"

chk "retention attaches a partition with a free-form name again, with its own bounds" yes \
  "$(logged "WARNING:  topic: public.odd_q_old has rows with no log_offset, so retention attached it again to public.odd_q")"
chk "the free-form partition keeps its range" \
  "$(utc "SELECT format('FOR VALUES FROM (%L) TO (%L)', '$LO'::timestamptz - interval '3 minutes', '$LO'::timestamptz - interval '2 minutes')")" \
  "$(utc "SELECT pg_get_expr(relpartbound, oid) FROM pg_class WHERE oid = 'public.odd_q_old'::regclass")"

chk "check_duplicates returns zero rows on a clean topic" 0 \
  "$(psql_as postgres "SELECT count(*) FROM topic.check_duplicates('tenant_b', 'dup_q', full => true)")"
psql_as postgres "SET session_replication_role = replica;
                  INSERT INTO tenant_b.dup_q (band, value, published_at)
                  VALUES (0, '{}', date_bin('1 minute', now(), '2000-01-01') + interval '2 minutes');
                  UPDATE tenant_b.dup_q SET log_offset = 0 WHERE log_offset IS NULL" >/dev/null
chk "check_duplicates returns one row after a duplicate in a second partition" "0|0|2" \
  "$(psql_as postgres "SELECT band, log_offset, copies FROM topic.check_duplicates('tenant_b', 'dup_q', full => true)")"
refused=$(psql_as tenant_a "ALTER TABLE tenant_a.evil_q RENAME TO gone" 2>&1 || true)
echo "RED: the tenant renames its queue table -- $refused"
chk "a tenant cannot rename its queue table" yes \
  "$(grep -q 'a queue table must keep its name and schema' <<<"$refused" && echo yes || echo no)"
psql_as postgres "SET session_replication_role = replica; ALTER TABLE tenant_a.evil_q RENAME TO gone" >/dev/null
psql_as tenant_a "DROP TABLE tenant_a.gone;
                  CREATE VIEW tenant_a.evil_q AS
                  SELECT 0::smallint AS band, 0::bigint AS log_offset, now() AS published_at, 0::bigint AS seq
                  WHERE tenant_a.probe('view', 0::smallint)" >/dev/null

CRASH_P=$(part public.crash_q 3)
fifo_session b
exec 4>"$WORK/b.fifo"
echo "BEGIN; INSERT INTO public.crash_q (band, value, published_at) VALUES (0, '{}', $(at 3));" >&4
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_locks WHERE relation = 'public.crash_q'::regclass AND mode = 'RowExclusiveLock'\")\" = 1 ]"
set_retention crash_q
chk "the crash test sees the detach line" yes "$(logged "retention detaches public.$CRASH_P from public.crash_q")"
chk "detaching names the partition when the worker is killed" t \
  "$(psql_as postgres "SELECT detaching = 'public.$CRASH_P'::regclass FROM topic.topic_config WHERE topic = 'crash_q'")"
kill -9 "$(worker_pid)"
exec 4>&-
wait_for "psql_as postgres 'SELECT 1' >/dev/null"
chk "after the restart, the table named in detaching is checked and dropped" yes \
  "$(wait_for "[ \"\$(exists public.crash_q "$CRASH_P")\" = f ]" && echo yes || echo no)"
chk "detaching is NULL after the resumed drop" t \
  "$(psql_as postgres "SELECT detaching IS NULL FROM topic.topic_config WHERE topic = 'crash_q'")"
chk "oldest_offset of crash_q is correct after the resumed drop" t "$(floor_ok public.crash_q)"
chk "the first tick after the restart refuses the view in place of a queue" yes \
  "$(logged "tenant_a.evil_q duplicate check: topic.check_duplicates: tenant_a.evil_q is not a partitioned table")"
chk "the first tick after the restart logs the duplicate of a later topic" yes \
  "$(logged "pg_topics partition worker: duplicate offset: tenant_b.dup_q band 0 has 2 rows with log_offset 0")"
chk "the view ran no tenant function as a superuser" 0 "$(grep -c 'pgt_probe view .* t$' "$PGDATA/log" || true)"
chk "no tenant function ran as a superuser in the whole run" 0 "$(grep -c 'pgt_probe .* t$' "$PGDATA/log" || true)"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
