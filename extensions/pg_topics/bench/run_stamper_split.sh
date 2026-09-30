#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "SELECT topic.create_topic('public.split_q', 1)" >/dev/null
psql_as postgres "UPDATE topic.topic_band_position SET stamped_by = 'other/1' WHERE topic = 'split_q'" >/dev/null
psql_as postgres "SELECT topic.publish('public.split_q', '{}')" >/dev/null
wait_for "[ \"\$(unstamped public.split_q)\" = 0 ]"

chk "the stamper stamps the row" 0 "$(unstamped public.split_q)"
chk "the stamper log has the WARNING for a changed node identity" 1 \
  "$(grep -c 'WARNING:  topic.stamp_topic: public.split_q band 0 was stamped by other/1, now by ' "$PGDATA/log")"
chk "stamped_by now names this node" "$(psql_as postgres "SELECT s.system_identifier || '/' || ('x' || left(pg_walfile_name(pg_current_wal_insert_lsn()), 8))::bit(32)::int
                                         FROM pg_control_system() s")" \
  "$(psql_as postgres "SELECT stamped_by FROM topic.topic_band_position WHERE topic = 'split_q'")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
