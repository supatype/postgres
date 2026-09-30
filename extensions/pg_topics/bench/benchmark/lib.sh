write_payload_json() {
  local file=${1:-payload.json} pad=${2:-180}
  seq 1 100 | awk -v pad="$pad" '{ printf "{\"i\": %d, \"pad\": \"%0*d\"}\n", $1, pad, 0 }' >"$WORK/$file"
}

kafka_reachable() {
  timeout 15 docker run --rm "${DOCKER_NET_ARGS[@]}" -v "$WORK:/w:ro" confluentinc/cp-kafka:7.7.1 \
    kafka-broker-api-versions --bootstrap-server "$BOOTSTRAP_HOST:$KPORT" --command-config /w/alice.properties >/dev/null 2>&1
}

producer_rate()  { sed -n 's/^[0-9]* records sent, \([0-9.]*\) records\/sec.*/\1/p' <<<"$1"; }
producer_p50()   { sed -n 's/.* \([0-9]*\) ms 50th,.*/\1/p' <<<"$1"; }
producer_p99()   { sed -n 's/.* \([0-9]*\) ms 99th,.*/\1/p' <<<"$1"; }
producer_mb()    { sed -n 's/.* records\/sec (\([0-9.]*\) MB\/sec).*/\1/p' <<<"$1"; }
producer_p999()  { sed -n 's/.* \([0-9.]*\) ms 99\.9th\..*/\1/p' <<<"$1"; }

consumer_row()     { grep -A1 '^start.time' <<<"$1" | tail -n 1; }
consumer_rate()    { cut -d, -f6 <<<"$1" | tr -d ' '; }
consumer_mb_rate() { cut -d, -f4 <<<"$1" | tr -d ' '; }

percentile() {
  local p=$1 file=$2 n rank
  n=$(wc -l <"$file")
  [ "$n" -eq 0 ] && return
  rank=$(((p * n + 99) / 100))
  [ "$rank" -lt 1 ] && rank=1
  sort -g "$file" | sed -n "${rank}p"
}

e2e_latency_samples() {
  local topic=$1 count=$2 i out
  for i in $(seq 1 "$count"); do
    if out=$(kafka_py alice alice-pw latency "$topic" 2>&1); then
      sed -n 's/^latency \([0-9]*\)$/\1/p' <<<"$out"
    fi
  done
}

band_spread_ok() {
  psql_as postgres "WITH c AS (SELECT band, count(*) AS n FROM $1 GROUP BY band)
    SELECT bool_and(abs(n - avg_n) <= avg_n * 0.05) FROM c, (SELECT avg(n) AS avg_n FROM c) a"
}

install_release_build() {
  stop_pg
  (cd "$HERE/../extension" && cargo pgrx install --release --pg-config "$PG_CONFIG" >/dev/null)
  start_pg >/dev/null
}

machine_facts() {
  local build_profile=$1
  echo "CPU: $(sed -n 's/^model name\s*: //p' /proc/cpuinfo | head -1)"
  echo "cores: $(nproc)"
  echo "RAM: $(free -h | awk '/^Mem:/ {print $2}')"
  local root_dev
  root_dev=$(findmnt -no SOURCE / | sed -E 's/p?[0-9]+$//')
  echo "disk rota flag on $root_dev (WSL2 virtual disks do not report real media type): $(lsblk -dno rota "$root_dev" 2>/dev/null || echo unknown)"
  echo "kernel: $(uname -r)"
  echo "PostgreSQL: $("$PG_CONFIG" --version)"
  echo "pg_topics commit: $(git -C "$HERE" rev-parse HEAD)"
  echo "pg_topics build profile: $build_profile"
}
