#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster
P1=$PORT
D1=$PGDATA
P2=$(free_port)
D2=$WORK/s
K1=$(free_port)
K2=$(free_port)
trap 'PGDATA=$D2 stop_pg >/dev/null 2>&1 || true; cleanup' EXIT

on() {
  local port=$1
  shift
  PORT=$port psql_as "$@"
}

workers() {
  on "$1" postgres "SELECT coalesce(string_agg(backend_type, ',' ORDER BY backend_type), '')
                    FROM pg_stat_activity WHERE backend_type LIKE 'pg_topics %'"
}

port_open() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && echo open || echo closed
}

offsets() {
  on "$1" postgres "SELECT string_agg(band || ':' || next_offset, ' ' ORDER BY band)
                    FROM topic.topic_band_position WHERE topic = 'fo_q'"
}

caught_up() {
  local lsn
  lsn=$(on "$P1" postgres "SELECT pg_current_wal_lsn()")
  wait_for "[ \"\$(on $P2 postgres \"SELECT pg_last_wal_replay_lsn() >= '$lsn'\")\" = t ]"
}

make_cert "$WORK" >/dev/null
stop_pg >/dev/null
cat >>"$D1/postgresql.conf" <<CONF
pg_topics.tls_cert_file = '$WORK/server.crt'
pg_topics.tls_key_file = '$WORK/server.key'
pg_topics.advertised_host = '$BOOTSTRAP_HOST'
pg_topics.port = $K1
CONF
start_pg >/dev/null
wait_for "[ \"\$(listener_status)\" = 'listening on port $K1' ]"

on "$P1" postgres "CREATE ROLE fo_reader LOGIN; CREATE ROLE fo_mon LOGIN IN ROLE pg_monitor;
  SELECT topic.create_topic('public.fo_q', 4); SELECT topic.grant_consume('public.fo_q', 'fo_reader')" >/dev/null

"$PGBIN/pg_basebackup" -h /tmp -p "$P1" -U postgres -D "$D2" -R -X stream -c fast
echo "pg_topics.port = $K2" >>"$D2/postgresql.conf"
PGDATA=$D2 PORT=$P2 start_pg >/dev/null
chk "the standby is in recovery" t "$(on "$P2" postgres "SELECT pg_is_in_recovery()")"
chk "the standby keeps pg_topics in shared_preload_libraries" pg_topics \
  "$(on "$P2" postgres "SHOW shared_preload_libraries")"
chk "the standby keeps pg_topics.databases" postgres "$(on "$P2" postgres "SHOW pg_topics.databases")"
chk "the standby has its own listener port" "$K2" "$(on "$P2" postgres "SHOW pg_topics.port")"

on "$P1" postgres "SELECT topic.publish('public.fo_q', jsonb_build_object('i', i), 'k' || i)
                   FROM generate_series(1, 1000) i" >/dev/null
PORT=$P1 wait_for "[ \"\$(unstamped public.fo_q)\" = 0 ]"
caught_up
echo "standby pg_last_wal_replay_lsn(): $(on "$P2" postgres "SELECT pg_last_wal_replay_lsn()")"

chk "no pg_topics worker runs on the standby during recovery" "" "$(workers "$P2")"
chk "the standby listener port is closed during recovery" closed "$(port_open "$K2")"
chk "a normal role reads all 1000 stamped rows with topic.fetch on the standby" 1000 \
  "$(on "$P2" fo_reader "SELECT sum((SELECT count(*) FROM topic.fetch('public.fo_q', b, 0, 1000)))
                         FROM generate_series(0, 3) b")"
chk "a normal role reads topic.band_offsets on the standby" "$(offsets "$P1")" \
  "$(on "$P2" fo_reader "SELECT string_agg(band || ':' || next_offset, ' ' ORDER BY band)
                         FROM topic.band_offsets('public.fo_q')")"
health=$(on "$P2" fo_mon "SELECT topic || ':' || duplicate || ':' || listener_bound || ':' || ok FROM topic.health()")
echo "standby topic.health() as a pg_monitor role: $health"
chk "topic.health() on the standby gives a row and no error" "fo_q:false:false:false" "$health"

fork=$(offsets "$P2")
echo "next_offset per band at the fork: $fork"
"$PGBIN/pg_ctl" -D "$D2" -w promote >/dev/null
chk "the promoted standby left recovery" f "$(on "$P2" postgres "SELECT pg_is_in_recovery()")"
wait_for "[ \"\$(workers $P2)\" = 'pg_topics listener,pg_topics partition,pg_topics replicated stamper,pg_topics stamper,pg_topics sync' ]" || true
chk "the five workers start on the new primary after promotion" \
  "pg_topics listener,pg_topics partition,pg_topics replicated stamper,pg_topics stamper,pg_topics sync" "$(workers "$P2")"
PORT=$P2 wait_for "[ \"\$(listener_status)\" = 'listening on port $K2' ]" || true
chk "the new primary listener binds its own port after promotion" "listening on port $K2" "$(PORT=$P2 listener_status)"
chk "the old primary listener still holds its port" "listening on port $K1" "$(PORT=$P1 listener_status)"

on "$P2" postgres "SELECT topic.publish('public.fo_q', jsonb_build_object('side', 'new', 'i', i), 'k' || i)
                   FROM generate_series(1, 100) i" >/dev/null
on "$P1" postgres "SELECT topic.publish('public.fo_q', jsonb_build_object('side', 'old', 'i', i), 'k' || i)
                   FROM generate_series(1, 150) i" >/dev/null
PORT=$P2 wait_for "[ \"\$(unstamped public.fo_q)\" = 0 ]"
PORT=$P1 wait_for "[ \"\$(unstamped public.fo_q)\" = 0 ]"

chk "the new primary stamps its rows with no gap and no duplicate" t "$(PORT=$P2 gap_free fo_q)"
chk "the new primary continues each band at the replicated next_offset" "$fork" \
  "$(on "$P2" postgres "SELECT string_agg(band || ':' || lo, ' ' ORDER BY band) FROM (
                          SELECT band, min(log_offset) AS lo FROM public.fo_q WHERE value->>'side' = 'new' GROUP BY band) s")"
sysid=$(on "$P2" postgres "SELECT system_identifier FROM pg_control_system()")
chk "the new primary logs the changed stamped_by WARNING for the new timeline" 4 \
  "$(grep -c "WARNING:  topic.stamp_topic: public.fo_q band [0-3] was stamped by $sysid/1, now by $sysid/2" "$D2/log" || true)"
chk "the old primary keeps timeline 1, so its stamper logs no WARNING" 0 \
  "$(grep -c 'was stamped by' "$D1/log" || true)"
chk "stamped_by on the new primary names timeline 2" "$sysid/2" \
  "$(on "$P2" postgres "SELECT string_agg(DISTINCT stamped_by, ' ') FROM topic.topic_band_position WHERE topic = 'fo_q'")"
chk "stamped_by on the old primary still names timeline 1" "$sysid/1" \
  "$(on "$P1" postgres "SELECT string_agg(DISTINCT stamped_by, ' ') FROM topic.topic_band_position WHERE topic = 'fo_q'")"
chk "the old primary stamps its rows with no gap and no duplicate" t "$(PORT=$P1 gap_free fo_q)"
chk "health() on each side finds no duplicate, because the split is across two nodes" "f f" \
  "$(on "$P1" postgres "SELECT duplicate FROM topic.health()") $(on "$P2" postgres "SELECT duplicate FROM topic.health()")"

split=$(on "$P1" postgres "COPY (SELECT band, log_offset, published_at, seq, value FROM public.fo_q) TO STDOUT" |
  "$PGBIN/psql" -h /tmp -p "$P2" -U postgres -d postgres -tA -v ON_ERROR_STOP=1 \
    -c "CREATE TEMP TABLE o (band smallint, log_offset bigint, published_at timestamptz, seq bigint, value jsonb)" \
    -c "COPY o FROM STDIN" \
    -c "CREATE TEMP VIEW d AS SELECT n.band, n.log_offset, n.published_at AS n_at, n.seq AS n_seq,
                                     o.published_at AS o_at, o.seq AS o_seq
                              FROM public.fo_q n JOIN o USING (band, log_offset)
                              WHERE (n.published_at, n.seq, n.value) IS DISTINCT FROM (o.published_at, o.seq, o.value)" \
    -c "SELECT 'split ' || count(*) FROM d" \
    -c "SELECT DISTINCT ON (band) 'band ' || band || ' offset ' || log_offset || ' new ' || n_at || ' seq ' || n_seq
               || ' old ' || o_at || ' seq ' || o_seq FROM d ORDER BY band, log_offset" \
    -c "SELECT 'first ' || string_agg(band || ':' || lo, ' ' ORDER BY band)
        FROM (SELECT band, min(log_offset) AS lo FROM d GROUP BY band) s")
echo "split point per band (published_at and seq on each side):"
sed -n 's/^band /  band /p' <<<"$split"
chk "both timelines assigned the same offsets to the 100 rows of the new primary" "split 100" \
  "$(grep '^split ' <<<"$split")"
chk "the first duplicated offset per band is the fork point" "first $fork" "$(grep '^first ' <<<"$split")"

old0=$(on "$P1" postgres "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'fo_q' AND band = 0")
new0=$(on "$P2" postgres "SELECT next_offset FROM topic.topic_band_position WHERE topic = 'fo_q' AND band = 0")
echo "band 0 next_offset: old primary $old0, new primary $new0"
chk "the old primary is ahead of the new primary on band 0" t "$([ "$old0" -gt "$new0" ] && echo t || echo f)"
chk "commit_offset on the new primary accepts the old primary position" NONE \
  "$(on "$P2" fo_reader "SELECT topic.commit_offset('public.fo_q', 'fo_group', 0, $old0, -1)")"
chk "commit_offset on the new primary stores no offset above its next_offset" "$new0" \
  "$(on "$P2" fo_reader "SELECT topic.fetch_offset('public.fo_q', 'fo_group', 0)")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
