pass=0
fail=0

if docker info --format '{{.OperatingSystem}}' 2>/dev/null | grep -qx 'Docker Desktop'; then
  BOOTSTRAP_HOST=host.docker.internal
  DOCKER_NET_ARGS=()
else
  BOOTSTRAP_HOST=127.0.0.1
  DOCKER_NET_ARGS=(--network host)
fi

chk() {
  if [ "$2" = "$3" ]; then
    echo "PASS  $1"
    pass=$((pass + 1))
  else
    echo "FAIL  $1"
    echo "        expected: [$2]"
    echo "        actual:   [$3]"
    fail=$((fail + 1))
  fi
}

start_pg() {
  "$PGBIN/pg_ctl" -D "$PGDATA" -l "$PGDATA/log" -o "-p $PORT -k /tmp" -w start
}

stop_pg() {
  "$PGBIN/pg_ctl" -D "$PGDATA" -m fast -w stop
}

kill9_pg() {
  local pid
  pid=$(head -1 "$PGDATA/postmaster.pid")
  kill -9 "$pid" $(pgrep -P "$pid")
}

free_port() {
  local port
  while port=$((20000 + RANDOM % 20000)); [ -e "/tmp/.s.PGSQL.$port" ] || (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; do
    :
  done
  echo "$port"
}

psql_as() {
  local role=$1 sql=$2
  "$PGBIN/psql" -h /tmp -p "$PORT" -U "$role" -d postgres -tAc "$sql" 2>&1
}

make_cert() {
  local dir=$1
  openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
    -keyout "$dir/server.key" -out "$dir/server.crt" \
    -subj /CN=localhost -addext subjectAltName=DNS:localhost,DNS:host.docker.internal,IP:127.0.0.1 2>&1
}

wait_for() {
  local i
  for i in $(seq 1 60); do
    eval "$1" && return 0
    sleep 1
  done
  return 1
}

cleanup() {
  local rc=$?
  kill $(jobs -p) 2>/dev/null || true
  stop_pg >/dev/null 2>&1 || true
  if [ "$rc" -ne 0 ] && [ -f "$PGDATA/log" ]; then
    echo "--- last 200 lines of the server log ---"
    tail -n 200 "$PGDATA/log"
  fi
  rm -rf "$WORK"
}

new_cluster() {
  : "${PG_CONFIG:?set PG_CONFIG to the pg_config of the PostgreSQL 17 copy}"
  PGBIN=$("$PG_CONFIG" --bindir)
  PORT=$(free_port)
  WORK=$(mktemp -d)
  chmod 755 "$WORK"
  PGDATA="$WORK/data"
  trap cleanup EXIT
  (cd "$HERE/../extension" && cargo pgrx install --pg-config "$PG_CONFIG" >/dev/null)
  "$PGBIN/initdb" -D "$PGDATA" -U postgres --auth-local=trust --auth-host=scram-sha-256 >/dev/null
  cat >>"$PGDATA/postgresql.conf" <<CONF
shared_preload_libraries = 'pg_topics'
pg_topics.databases = 'postgres'
pg_topics.failover_is_fenced = on
CONF
  start_pg >/dev/null
  psql_as postgres "CREATE EXTENSION pg_topics" >/dev/null
}

unstamped() {
  psql_as postgres "SELECT count(*) FROM $1 WHERE log_offset IS NULL"
}

wrong_offsets() {
  wait_for "[ \"\$(unstamped $1)\" = 0 ]" || true
  diff <(psql_as postgres "SELECT band || ' ' || log_offset || ' ' || value::text FROM $1" | LC_ALL=C sort) \
    <(LC_ALL=C sort <<<"$2") | grep -c '^[<>]' || true
}

py_offsets() {
  grep '^ok ' <<<"$1" | cut -d' ' -f2- || true
}

java_offsets() {
  jq -rR 'fromjson? | objects | select(.name == "producer_send_success") | "\(.partition) \(.offset) \(.value)"' <<<"$1"
}

hold_stamp_lock() {
  "$PGBIN/psql" -h /tmp -p "$PORT" -U postgres -d postgres -q -o /dev/null -c "BEGIN" \
    -c "SELECT FROM topic.topic_band_position WHERE schema_name || '.' || topic = '$1' AND band = 0 FOR NO KEY UPDATE" \
    -c "SELECT pg_sleep(3600)" >/dev/null 2>&1 &
  wait_for "[ \"\$(psql_as postgres \"SELECT count(*) FROM pg_stat_activity WHERE wait_event = 'PgSleep'\")\" = 1 ]"
}

release_stamp_lock() {
  psql_as postgres "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE wait_event = 'PgSleep'" >/dev/null
}

gap_free() {
  local t rows="" names=""
  for t in "$@"; do
    rows="$rows${rows:+ UNION ALL }SELECT '$t' AS t, band, log_offset FROM public.$t"
    names="$names${names:+, }'$t'"
  done
  psql_as postgres "SELECT bool_and(coalesce(s.n, 0) = p.next_offset AND coalesce(s.d, 0) = p.next_offset
                                    AND coalesce(s.hi, -1) = p.next_offset - 1)
    FROM topic.topic_band_position p
    LEFT JOIN (SELECT t, band, count(*) AS n, count(DISTINCT log_offset) AS d, max(log_offset) AS hi
               FROM ($rows) r GROUP BY t, band) s ON s.t = p.topic AND s.band = p.band
    WHERE p.schema_name = 'public' AND p.topic IN ($names)"
}

listener_status() {
  psql_as postgres "SELECT query FROM pg_stat_activity WHERE backend_type = 'pg_topics listener' ORDER BY pid"
}

listener_pid() {
  psql_as postgres "SELECT pid FROM pg_stat_activity WHERE backend_type = 'pg_topics listener'"
}

restart_listener() {
  local old
  old=$(listener_pid)
  psql_as postgres "SELECT pg_terminate_backend($old)" >/dev/null
  wait_for "[ \"\$(listener_pid)\" != '$old' ] && [ \"\$(listener_status)\" = 'listening on port $KPORT' ]"
}

warm_up_docker_net() {
  local i
  for i in $(seq 1 50); do
    docker run --rm pg_topics_python:2.15.1 \
      python3 -c "import socket; socket.create_connection(('$BOOTSTRAP_HOST', $KPORT), timeout=1)" >/dev/null 2>&1 && return 0
    sleep 0.1
  done
}

start_listener() {
  KPORT=$(free_port)
  make_cert "$WORK" >/dev/null
  docker build -q -t pg_topics_python:2.15.1 "$HERE/clients/python" >/dev/null 2>&1 || { echo "FAIL  docker build of clients/python"; exit 1; }
  psql_as postgres "ALTER SYSTEM SET pg_topics.tls_cert_file = '$WORK/server.crt'" >/dev/null
  psql_as postgres "ALTER SYSTEM SET pg_topics.tls_key_file = '$WORK/server.key'" >/dev/null
  psql_as postgres "ALTER SYSTEM SET pg_topics.advertised_host = '$BOOTSTRAP_HOST'" >/dev/null
  psql_as postgres "SELECT pg_reload_conf()" >/dev/null
  psql_as postgres "ALTER DATABASE postgres SET pg_topics.port = $KPORT" >/dev/null
  restart_listener
  if [ ${#DOCKER_NET_ARGS[@]} -eq 0 ]; then
    warm_up_docker_net
  fi
}

kafka_py() {
  local user=$1 password=$2
  shift 2
  docker run --rm ${KAFKA_NAME:+--name "$KAFKA_NAME"} "${DOCKER_NET_ARGS[@]}" -v "$WORK:/w:ro" -v "$HERE/clients/python:/app:ro" \
    -e BOOTSTRAP="$BOOTSTRAP_HOST:$KPORT" -e KAFKA_USER="$user" -e KAFKA_PASSWORD="$password" \
    pg_topics_python:2.15.1 python /app/client.py "$@" 2>&1
}

java_config() {
  cat >"$WORK/$1.properties" <<PROPS
security.protocol=SASL_SSL
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="$1" password="$2";
ssl.truststore.type=PEM
ssl.truststore.location=/w/server.crt
PROPS
}

kafka_java() {
  docker run --rm -i "${DOCKER_NET_ARGS[@]}" -v "$WORK:/w:ro" confluentinc/cp-kafka:7.7.1 "$@" 2>&1
}
