#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

publishers=16
per_key=250
psql_as postgres "SELECT topic.create_topic('public.ord_q', 4)" >/dev/null

pids=()
for k in $(seq 1 "$publishers"); do
  for n in $(seq 1 "$per_key"); do
    echo "SELECT topic.publish('public.ord_q', jsonb_build_object('n', $n, 't', now()), 'key-$k');"
  done >"$WORK/key-$k.sql"
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -o /dev/null \
    -v ON_ERROR_STOP=1 -f "$WORK/key-$k.sql" &
  pids+=("$!")
done
failed=0
for pid in "${pids[@]}"; do
  wait "$pid" || failed=$((failed + 1))
done
chk "all 16 publishers finish with no error" 0 "$failed"

wait_for "[ \"\$(unstamped public.ord_q)\" = 0 ]"
chk "every row is stamped" "$((publishers * per_key)):0" \
  "$(psql_as postgres "SELECT count(*) || ':' || count(*) FILTER (WHERE log_offset IS NULL) FROM public.ord_q")"
chk "per key, log_offset order equals counter order" 0 \
  "$(psql_as postgres "SELECT count(*) FROM (
       SELECT (value->>'n')::int AS n,
              lag((value->>'n')::int) OVER (PARTITION BY key ORDER BY log_offset) AS prev,
              row_number() OVER (PARTITION BY key ORDER BY log_offset) AS pos
       FROM public.ord_q) r
     WHERE n <> pos OR (prev IS NOT NULL AND n <> prev + 1)")"

causal=$(psql_as postgres "WITH r AS (
    SELECT key, band, log_offset, (value->>'t')::timestamptz AS started,
           lead((value->>'t')::timestamptz) OVER (PARTITION BY key ORDER BY (value->>'n')::int) AS acked_by
    FROM public.ord_q)
  SELECT count(*) FILTER (WHERE a.key <> b.key) > 0, count(*) FILTER (WHERE a.log_offset > b.log_offset)
  FROM r a JOIN r b ON a.band = b.band AND a.acked_by < b.started")
chk "the causal check covers rows of different keys" t "${causal%%|*}"
chk "a row acknowledged before another row started has the lower offset" 0 "${causal##*|}"

echo "passed: $pass  failed: $fail"
[ "$fail" -eq 0 ]
