#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

psql_as postgres "SELECT topic.create_topic('public.dur_q', 2, min_durability => 'durable')" >/dev/null

publisher() {
  local id=0
  while [ ! -f "$WORK/stop" ]; do
    id=$((id + 1))
    if PGOPTIONS='-c synchronous_commit=off' psql_as postgres "SELECT topic.publish('public.dur_q', '{\"id\": $id}')" >/dev/null; then
      echo "$id" >>"$WORK/acked"
    else
      sleep 0.05
    fi
  done
}

publisher &
publisher_pid=$!
for kill in 1 2 3 4 5; do
  sleep "$((RANDOM % 2)).$((RANDOM % 10))"
  kill9_pg
  wait_for "start_pg >/dev/null 2>&1"
done
sleep 1
touch "$WORK/stop"
wait "$publisher_pid"

chk "the server did crash recovery 5 times" 5 \
  "$(grep -c 'database system was not properly shut down; automatic recovery in progress' "$PGDATA/log")"
chk "the publisher got acknowledgements" yes "$([ "$(wc -l <"$WORK/acked")" -gt 0 ] && echo yes || echo no)"
wait_for "[ \"\$(unstamped public.dur_q)\" = 0 ]"
chk "every row gets an offset after the restart" 0 "$(unstamped public.dur_q)"
psql_as postgres "SELECT value->>'id' FROM public.dur_q GROUP BY 1 HAVING count(*) = 1" | sort >"$WORK/once"
chk "every acknowledged id is present exactly once" "" "$(sort "$WORK/acked" | comm -23 - "$WORK/once")"
chk "the offsets have no gap and no duplicate" t "$(gap_free dur_q)"
echo "acknowledged ids: $(wc -l <"$WORK/acked")  rows: $(psql_as postgres "SELECT count(*) FROM public.dur_q")"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
