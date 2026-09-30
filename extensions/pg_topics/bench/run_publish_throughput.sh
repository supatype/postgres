#!/bin/bash
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
source "$HERE/lib.sh"
new_cluster

seconds=10
stamped() {
  psql_as postgres "SELECT count(log_offset) FROM public.$1"
}

printf '%-10s %-8s %-10s %-10s %s\n' durability clients events/s stamped/s max_backlog_age_s
for durability in relaxed durable; do
  psql_as postgres "SELECT topic.create_topic('public.${durability}_q', 4, min_durability => '$durability')" >/dev/null
  echo "SELECT topic.publish('public.${durability}_q', '{\"v\": 1}');" >"$WORK/$durability.sql"
  commit=on
  [ "$durability" = relaxed ] && commit=off
  for clients in 1 4 8; do
    : >"$WORK/backlog"
    while sleep 0.5; do
      psql_as postgres "SELECT extract(epoch FROM backlog_age) FROM topic.topic_config WHERE topic = '${durability}_q'" >>"$WORK/backlog"
    done &
    sampler=$!
    before=$(stamped "${durability}_q")
    tps=$(PGOPTIONS="-c synchronous_commit=$commit" "$PGBIN/pgbench" -h /tmp -p "$PORT" -U postgres -n \
      -c "$clients" -j "$clients" -T "$seconds" -f "$WORK/$durability.sql" postgres 2>/dev/null |
      sed -n 's/^tps = \([0-9.]*\) .*/\1/p')
    after=$(stamped "${durability}_q")
    kill "$sampler"
    wait "$sampler" 2>/dev/null || true
    printf '%-10s %-8s %-10.0f %-10s %s\n' "$durability" "$clients" "$tps" "$(((after - before) / seconds))" \
      "$(sort -g "$WORK/backlog" | tail -1)"
  done
done

psql_as postgres "SELECT topic.create_topic('public.catch_q', 4)" >/dev/null
hold_stamp_lock public.catch_q
psql_as postgres "INSERT INTO public.catch_q (band, value) SELECT i % 4, '{}' FROM generate_series(1, 1000000) i" >/dev/null
start=$(date +%s%N)
release_stamp_lock
wait_for "[ \"\$(psql_as postgres \"SELECT count(*) = 0 FROM public.catch_q WHERE log_offset IS NULL\")\" = t ]"
wait_for "[ \"\$(psql_as postgres \"SELECT backlog_age = interval '0' FROM topic.topic_config WHERE topic = 'catch_q'\")\" = t ]"
echo "catch-up of 1000000 rows after a stalled stamper: $((($(date +%s%N) - start) / 1000000)) ms"
