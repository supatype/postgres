#!/usr/bin/env bash
# WAL and storage cost of a durable cache write (#41).
#
# Throughput is published as operations per second, which tells an operator
# nothing about what the durable tiers cost in WAL, table churn or vacuum work.
# This measures the thing that actually sizes a durable deployment: WAL bytes
# per logical cache write, and how far that is from WAL bytes per row.
#
# Two effects make a naive number meaningless, so both are measured rather than
# assumed away:
#
#   Batching deduplicates. The persist worker collapses a window's writes to the
#   last one per key before the statement runs, so a hot key rewritten a
#   thousand times in one window costs one row version, not a thousand. Skew,
#   not rate, is what decides how much of that happens -- which is why the
#   sweep is over skew and the dedup factor is reported next to every row.
#
#   full_page_writes dominates the first touch of each page after a checkpoint.
#   Every configuration is therefore run twice: once immediately after an
#   explicit CHECKPOINT (cold pages, full-page images in the WAL) and once
#   straight after that (warm). Publishing one number without saying which of
#   those it is would be off by several times.
#
# Reports per run: WAL bytes total and per logical write, rows actually
# inserted/updated, the dedup factor, dead tuples left behind, and the heap and
# index growth. The final section repeats one configuration with the Mode B row
# cache and concurrent SQL traffic active, which is the interaction an operator
# hits in practice and the one the review asked about.
#
# Not measured here, and deliberately: replica lag and fsync latency. Both need
# hardware this harness does not have (a second host, a device it can saturate);
# a number from a container would be a number about the container.
set -u
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)
EXT_DIR=${PGKS_EXT_DIR:-$SCRIPT_DIR/../extension}
PGBIN=${PGBIN:-/usr/lib/postgresql/17/bin}
PGDATA=${PGDATA:-/tmp/pgks-wal-bench}
PORT=${PGKS_PG_PORT:-5467}
RESP=${PGKS_RESP_PORT:-6437}
PROFILE=${PGKS_BUILD_PROFILE:-release}
SKIP_BUILD=${PGKS_BENCH_SKIP_BUILD:-0}

# Writes per run. Large enough that a checkpoint's worth of full-page images is
# amortised over a meaningful number of rows, small enough to finish.
WRITES=${PGKS_WAL_WRITES:-200000}
VAL_BYTES=${PGKS_WAL_VAL_BYTES:-128}
# The keyspace the skewed generators draw from. Smaller than the write count on
# purpose: with 200k writes over 20k keys every key is touched ~10 times, so
# there is dedup to find.
KEYSPACE=${PGKS_WAL_KEYSPACE:-20000}
SKEWS=${PGKS_WAL_SKEWS:-"uniform zipf hot"}
# Offered rates, in writes per second. 0 means "as fast as the pipeline goes".
RATES=${PGKS_WAL_RATES:-"10000 50000 0"}
# Segment sized for the whole keyspace several times over: eviction during the
# run would turn a WAL measurement into a measurement of eviction.
KEYS=$(( KEYSPACE * 4 + 4096 ))

psql_() { timeout 600 $PGBIN/psql -h /tmp -p $PORT -U postgres -d postgres -tAc "$1" 2>&1; }
start_pg() { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -l $PGDATA/log -o \"-p $PORT -k /tmp\" -w start" >/dev/null 2>&1; }
stop_pg()  { su postgres -c "$PGBIN/pg_ctl -D $PGDATA -w stop" >/dev/null 2>&1; }
wait_ready() { for _ in $(seq 1 120); do psql_ "SELECT 1" | grep -q "^1$" && return 0; sleep 1; done; return 1; }
# Workers outlive a killed postmaster and keep the ports, which makes the next
# cluster look alive while answering nothing. Clear them before starting.
# Matched on the process name rather than a substring of the whole command line:
# this script's own path contains "pg_keyspace", so a looser pattern makes the
# harness kill itself the moment it is invoked by its full path.
kill_stragglers() {
  ps -eo pid,args --no-headers | awk -v pgdata="$PGDATA" '
    { pid = $1; $1 = ""; cmd = substr($0, 2) }
    cmd ~ /^postgres: pg_keyspace/            { print pid; next }
    cmd ~ /(^|\/)postgres / && index(cmd, pgdata) { print pid }
  ' | while read -r p; do [ "$p" != "$$" ] && kill -9 "$p" 2>/dev/null; done
  sleep 2
}

# ---------------------------------------------------------------------------
# The load generator. redis-benchmark cannot do a skewed key distribution or an
# offered rate, and the durable tier holds every ack until its record commits,
# so the load has to be pipelined or it measures the persist window instead of
# the storage. This writes RESP on a raw socket, paces itself against a
# monotonic clock, and drains replies on a second thread so a full receive
# buffer never becomes back-pressure that looks like a slow server.
# ---------------------------------------------------------------------------
LOADER=/tmp/pgks_wal_loader.py
cat > $LOADER <<'PYEOF'
import bisect, random, socket, sys, threading, time

args = dict(a.split('=', 1) for a in sys.argv[1:])
port     = int(args['port'])
writes   = int(args['writes'])
keyspace = int(args['keyspace'])
skew     = args['skew']
valsize  = int(args['valsize'])
rate     = int(args.get('rate', '0'))
conns    = int(args.get('conns', '8'))
prefix   = args.get('prefix', 'w')

val = b'v' * valsize

cursor = 0
cursor_lock = threading.Lock()


def keygen(n):
    """n key indices, drawn according to the skew under test."""
    global cursor
    if skew == 'seq':
        # Priming only: walk the keyspace once so every row exists before
        # anything is measured, and the measured passes are overwrites rather
        # than a mix of insert and overwrite. Under lock, so N connections
        # between them cover the keyspace once rather than N times over.
        with cursor_lock:
            start = cursor
            cursor += n
        return [(start + i) % keyspace for i in range(n)]
    if skew == 'hot':
        return [0] * n
    if skew == 'uniform':
        return [random.randrange(keyspace) for _ in range(n)]
    # Zipf, s = 1.0, over the keyspace. Built once as a cumulative harmonic
    # table and sampled by bisect, which is fast enough not to be the bottleneck.
    return [bisect.bisect(CUM, random.random() * CUM[-1]) for _ in range(n)]

CUM = []
if skew == 'zipf':
    acc = 0.0
    for i in range(1, keyspace + 1):
        acc += 1.0 / i
        CUM.append(acc)

# One connection is not how load arrives, and on a single worker it is also not
# enough to keep the persist ring busy: the durable tier acks a batch at a time,
# so a lone pipelined socket spends most of its life waiting on a window it
# alone has to fill. Several connections, each pipelined, is both more realistic
# and what the other persist benches drive (redis-benchmark -c 50 -P 16).
state = {'ok': 0, 'sent': 0, 'err': None, 'done': False}
lock = threading.Lock()


def connection(idx, quota, my_rate):
    s = socket.create_connection(('127.0.0.1', port))
    s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    got = [0]

    # Replies are drained, not ignored: an undrained socket stalls the sender,
    # and an error reply would otherwise pass as success.
    def reader():
        buf = b''
        while got[0] < quota:
            try:
                d = s.recv(1 << 16)
            except OSError:
                return
            if not d:
                return
            buf += d
            got[0] += buf.count(b'+OK\r\n')
            if b'-' in d and state['err'] is None:
                state['err'] = d[:120].decode('latin1', 'replace')
            # Keep four bytes, not five: a whole "+OK\r\n" cannot hide in the
            # retained tail, so a reply split across two reads is counted once
            # and none is counted twice. Trimming matters -- an untrimmed buffer
            # is recounted from the start on every read, which overshoots the
            # target and returns before the writes have been acknowledged.
            buf = buf[-4:]

    r = threading.Thread(target=reader, daemon=True)
    r.start()

    BATCH = 500
    t0 = time.monotonic()
    sent = 0
    while sent < quota:
        n = min(BATCH, quota - sent)
        out = bytearray()
        for k in keygen(n):
            key = ('%s:%d' % (prefix, k)).encode()
            out += b'*3\r\n$3\r\nSET\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n' % (
                len(key), key, len(val), val)
        s.sendall(out)
        sent += n
        if my_rate > 0:
            due = t0 + sent / my_rate
            now = time.monotonic()
            if due > now:
                time.sleep(due - now)

    # Every write must be acknowledged before the caller reads the WAL position,
    # or the measurement straddles a window the server has not committed yet.
    deadline = time.monotonic() + 600
    while got[0] < quota and time.monotonic() < deadline:
        time.sleep(0.05)
    s.close()
    with lock:
        state['ok'] += got[0]
        state['sent'] += sent


t0 = time.monotonic()
threads = []
for i in range(conns):
    quota = writes // conns + (1 if i < writes % conns else 0)
    if quota == 0:
        continue
    t = threading.Thread(target=connection, args=(i, quota, rate / conns if rate else 0))
    t.start()
    threads.append(t)
for t in threads:
    t.join()
el = time.monotonic() - t0
print('sent=%d acked=%d elapsed=%.3f rate=%.0f err=%s'
      % (state['sent'], state['ok'], el,
         state['sent'] / el if el > 0 else 0, state['err']))
PYEOF

# ---------------------------------------------------------------------------
if [ "$SKIP_BUILD" != "1" ]; then
  echo "=== build + install the extension ==="
  cd "$EXT_DIR"
  REL_FLAG=""; [ "$PROFILE" = "release" ] && REL_FLAG="--release"
  cargo pgrx install $REL_FLAG --pg-config $PGBIN/pg_config >/tmp/pgks_wal_install.log 2>&1 || {
    echo "EXTENSION INSTALL FAILED"; tail -30 /tmp/pgks_wal_install.log; exit 1; }
  echo "installed ($PROFILE)"
fi

kill_stragglers
rm -rf $PGDATA; mkdir -p $PGDATA; chown postgres:postgres $PGDATA
su postgres -c "$PGBIN/initdb -D $PGDATA -U postgres" >/dev/null 2>&1
{
  echo "shared_preload_libraries = 'pg_keyspace'"
  echo "pg_keyspace.port = $RESP"
  echo "pg_keyspace.require_mask = off"
  echo "pg_keyspace.durability = 'durable'"
  echo "pg_keyspace.keys = $KEYS"
  echo "pg_keyspace.val_bytes = $(( VAL_BYTES + 64 ))"
  echo "pg_keyspace.workers = 1"
  echo "pg_keyspace.persist_workers = 1"
  # Left at their defaults on purpose. A benchmark that widens the persist
  # window or turns off full_page_writes to make the number look better is
  # measuring a cluster nobody runs.
  echo "max_wal_size = '4GB'"
  echo "maintenance_work_mem = '256MB'"
} >> $PGDATA/postgresql.conf
chown -R postgres:postgres $PGDATA
start_pg; wait_ready || { echo "cluster did not start"; exit 1; }
psql_ "CREATE EXTENSION pg_keyspace;" >/dev/null
stop_pg; sleep 1; start_pg; wait_ready || { echo "cluster did not restart"; exit 1; }
sleep 2

echo ""
echo "writes/run $WRITES, value $VAL_BYTES B, keyspace $KEYSPACE, segment $KEYS keys"
echo "full_page_writes=$(psql_ "SHOW full_page_writes"), wal_level=$(psql_ "SHOW wal_level"), persist window $(psql_ "SHOW pg_keyspace.persist_window_ms") ms"

# How many row versions the writes actually produced, read out of the WAL
# itself with pg_waldump rather than out of pg_stat_user_tables.
#
# That is not a stylistic preference. The persist worker is a background worker
# that never runs the main query loop, so it never flushes its pending stats:
# every counter in pg_stat_user_tables for the supacache tables reads zero
# while the worker is up, no matter how many rows it has written. Anything
# built on those counters -- a dedup factor, a dead-tuple figure, an operator's
# monitoring -- would silently be a column of zeros. The WAL cannot lie about
# what was written, so the benchmark reads that instead, and the closing
# section reports the blind spot rather than hiding it.
#
# Sets three globals: WS_BYTES (combined WAL size), WS_FPI (of which full-page
# images) and WS_ROWS (heap row versions written).
wal_stats() { # wal_stats <lsn0> <lsn1>
  local out
  out=$($PGBIN/pg_waldump -p $PGDATA/pg_wal --start="$1" --end="$2" --stats=record 2>/dev/null)
  # Each row is "Type  N (pct)  record (pct)  FPI (pct)  combined (pct)", and
  # the percentages are sometimes " ( 1.23)" and sometimes "(100.00)" -- two
  # fields or one, depending on the value. Strip every parenthesised group
  # first and the columns are fixed, which they are not if you index into the
  # raw line. The Total line is skipped or every sum would be doubled.
  read -r WS_BYTES WS_FPI WS_ROWS <<<"$(echo "$out" | awk '
    /^Total/ { next }
    /^[A-Za-z0-9_]+\// {
      line = $0
      gsub(/\([^)]*\)/, "", line)
      sub(/^[ \t]+/, "", line)
      n = split(line, f, /[ \t]+/)
      if (n < 5) next
      if (f[1] == "Heap/INSERT" || f[1] == "Heap/UPDATE" || f[1] == "Heap/HOT_UPDATE") rows += f[2]
      fpi += f[4]; comb += f[5]
    }
    END { printf "%d %d %d", comb+0, fpi+0, rows+0 }')"
}
# After every write is acknowledged the data is already committed -- the durable
# tier holds each ack until then -- so this is a quiescence check rather than a
# drain: wait for the WAL to stop moving before reading its end position, so a
# trailing autovacuum or checkpoint record does not land inside the window.
wait_quiet() {
  local last="" now stable=0
  for _ in $(seq 1 120); do
    now=$(psql_ "SELECT pg_current_wal_lsn()")
    if [ "$now" = "$last" ]; then
      stable=$((stable+1)); [ "$stable" -ge 2 ] && return 0
    else
      stable=0
    fi
    last=$now; sleep 1
  done
}
schema_bytes() {
  psql_ "SELECT coalesce(sum(pg_total_relation_size(c.oid)),0)::bigint FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='supacache' AND c.relkind IN ('r','p','i')"
}

printf '\n%-8s %-7s %-6s %-12s %-11s %-9s %-8s %-10s %-9s %s\n' \
  "skew" "rate" "pages" "achieved/s" "WAL total" "B/write" "B/row" "rows" "dedup" "dead"
printf -- '-------------------------------------------------------------------------------------------------------\n'

# One configuration, measured twice: cold is the pass straight after a
# CHECKPOINT and carries the full-page images, warm is the pass straight after
# that against pages already dirtied in this checkpoint cycle.
run_one() { # run_one <skew> <rate> <prefix>
  local skew=$1 rate=$2 prefix=$3 pass lsn0 lsn1 sz1 out sz_start=""
  # Prime first, unmeasured: walk the whole keyspace once so every row exists.
  # Without it the cold pass would be inserting rows and the warm pass
  # overwriting them, and the difference between the two would be that rather
  # than the full-page images it is meant to isolate.
  python3 $LOADER port=$RESP writes=$KEYSPACE keyspace=$KEYSPACE \
      skew=seq valsize=$VAL_BYTES rate=0 prefix=$prefix >/dev/null 2>&1
  wait_quiet
  for pass in cold warm; do
    if [ "$pass" = "cold" ]; then psql_ "CHECKPOINT" >/dev/null; sleep 1; fi
    lsn0=$(psql_ "SELECT pg_current_wal_lsn()")
    [ "$pass" = "cold" ] && sz_start=$(schema_bytes)
    out=$(python3 $LOADER port=$RESP writes=$WRITES keyspace=$KEYSPACE \
          skew=$skew valsize=$VAL_BYTES rate=$rate prefix=$prefix 2>&1)
    wait_quiet
    lsn1=$(psql_ "SELECT pg_current_wal_lsn()")
    sz1=$(schema_bytes)
    local acked err achieved
    acked=$(echo "$out" | sed -n 's/.*acked=\([0-9]*\).*/\1/p')
    err=$(echo "$out" | sed -n 's/.*err=\(.*\)$/\1/p')
    achieved=$(echo "$out" | sed -n 's/.*rate=\([0-9.]*\).*/\1/p')
    if [ "${acked:-0}" != "$WRITES" ]; then
      printf '%-8s %-7s %-6s  !! only %s of %s writes acknowledged (%s)\n' \
        "$skew" "$rate" "$pass" "${acked:-0}" "$WRITES" "${err:-no error}"
      continue
    fi
    local wal
    wal=$(psql_ "SELECT pg_wal_lsn_diff('$lsn1','$lsn0')::bigint")
    wal_stats "$lsn0" "$lsn1"
    printf '%-8s %-7s %-6s %-11s %-10s %-9s %-7s %-10s %-8s %s\n' \
      "$skew" "$([ "$rate" = 0 ] && echo max || echo "$rate")" "$pass" \
      "$(awk -v r="${achieved:-0}" 'BEGIN{printf "%.0f", r}')" \
      "$(awk -v w="$wal" 'BEGIN{printf "%.1f MB", w/1048576}')" \
      "$(awk -v w="$wal" -v n="$WRITES" 'BEGIN{printf "%.0f", w/n}')" \
      "$(awk -v f="${WS_FPI:-0}" -v c="${WS_BYTES:-0}" 'BEGIN{ if (c>0) printf "%.0f%%", 100*f/c; else print "-" }')" \
      "${WS_ROWS:-0}" \
      "$(awk -v w="$wal" -v r="${WS_ROWS:-0}" 'BEGIN{ if (r>0) printf "%.0f", w/r; else print "-" }')" \
      "$(awk -v n="$WRITES" -v r="${WS_ROWS:-0}" 'BEGIN{ if (r>0) printf "%.1fx", n/r; else print "-" }')"
    # Growth is reported once, across both measured passes: these are pure
    # overwrites of rows that already existed, so whatever the schema gains is
    # dead versions waiting on autovacuum rather than new data.
    if [ "$pass" = "warm" ]; then
      printf '%-8s %-7s %-6s growth across both passes (pure overwrites): %s\n' "" "" "" \
        "$(awk -v a="${sz_start:-0}" -v b="$sz1" 'BEGIN{printf "%+.1f MB", (b-a)/1048576}')"
    fi
  done
}

# --- skew sweep at one rate ------------------------------------------------
# Skew moves the answer further than rate does, so it is swept first and on its
# own: same write count, same value size, only the distribution changes.
i=0
for skew in $SKEWS; do
  i=$((i+1))
  run_one "$skew" 0 "s$i"
done

# --- rate sweep at one skew ------------------------------------------------
# Rate matters only through the persist window: a slower offered rate puts fewer
# writes in each window, so less of the hot-key rewriting collapses.
i=0
for rate in $RATES; do
  i=$((i+1))
  run_one zipf "$rate" "r$i"
done

# --- the same write, alongside normal SQL and the row cache ----------------
# The configuration an operator actually runs: cache writes are not the only
# thing on the device, and the row cache's decode slot is another WAL consumer.
echo ""
echo "=== with the row cache and concurrent SQL traffic ==="
psql_ "DROP TABLE IF EXISTS public.wal_rows CASCADE" >/dev/null 2>&1
psql_ "CREATE TABLE public.wal_rows(id bigint primary key, v text)" >/dev/null
psql_ "INSERT INTO public.wal_rows SELECT g, 'row'||g FROM generate_series(1,10000) g" >/dev/null
psql_ "SELECT supacache.rowcache_register('public.wal_rows', 1)" >/dev/null 2>&1
sleep 6
for k in $(seq 1 200); do psql_ "SELECT supacache.rowcache_put('public.wal_rows', $k)" >/dev/null; done

PGB=$(command -v pgbench || echo "$PGBIN/pgbench")
SQL_SUMMARY="pgbench not available; SQL-side latency not measured"
if [ -x "$PGB" ]; then
  cat > /tmp/pgks_wal_probe.sql <<'SQLEOF'
\set id random(1, 10000)
SELECT v FROM public.wal_rows WHERE id = :id;
SQLEOF
  # Long -T, stopped when the cache load finishes, so the probe covers exactly
  # the window under measurement rather than a fixed slice that might end before
  # the run does or idle after it. The per-interval progress lines are the
  # output that matters, which is just as well: pgbench does not reliably stop
  # on SIGINT here, so it is terminated and the final summary is never printed.
  "$PGB" -h /tmp -p $PORT -U postgres -d postgres -n -f /tmp/pgks_wal_probe.sql \
    -c 4 -T 3600 -P 5 > /tmp/pgks_wal_pgbench.log 2>&1 &
  PGB_PID=$!
else
  PGB_PID=""
fi
run_one zipf 0 "mx"
if [ -n "$PGB_PID" ]; then
  kill -TERM $PGB_PID 2>/dev/null
  wait $PGB_PID 2>/dev/null
  # Summarised from the progress lines: mean tps across intervals, and the
  # worst interval, which is the one an operator would notice.
  SQL_SUMMARY=$(awk '/^progress:/ {
      for (i = 1; i <= NF; i++) {
        if ($i == "tps,")  { t = $(i-1); n++; sum += t; if (mx == "" || t < mx) mx = t }
        if ($i == "lat")   { l = $(i+1); lsum += l; if (l > lmax) lmax = l }
      }
    } END {
      if (n > 0) printf "%d intervals, mean %.0f tps (worst %.0f), mean latency %.2f ms (worst %.2f)", n, sum/n, mx, lsum/n, lmax;
      else print "no progress lines"
    }' /tmp/pgks_wal_pgbench.log)
fi
echo "concurrent SQL: $SQL_SUMMARY"

echo ""
echo "=== after the run ==="
# pg_stat_checkpointer is PG17; older servers keep the counters on
# pg_stat_bgwriter. psql prints the error rather than failing, so the fallback
# has to look at the text.
CKPT=$(psql_ "SELECT num_timed||' timed, '||num_requested||' requested' FROM pg_stat_checkpointer")
case "$CKPT" in *ERROR*) CKPT=$(psql_ "SELECT checkpoints_timed||' timed, '||checkpoints_req||' requested' FROM pg_stat_bgwriter") ;; esac
echo "checkpoints: $CKPT"
echo "supacache on disk: $(psql_ "SELECT pg_size_pretty(sum(pg_total_relation_size(c.oid))) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='supacache' AND c.relkind IN ('r','p','i')")"
echo "rows in supacache.kv: $(psql_ "SELECT count(*) FROM supacache.kv")"

# What an operator's monitoring would see, next to what actually happened. The
# persist worker is a background worker: it never runs the main query loop, so
# nothing ever flushes its pending statistics while it is alive. Every counter
# below reads zero regardless of how many row versions the run just wrote, so
# anything downstream of them -- dashboards, and autovacuum's own thresholds,
# which are computed from exactly these numbers -- is working from zero.
echo ""
echo "=== what pg_stat_user_tables says about all of that ==="
psql_ "SELECT relname||': '||n_tup_ins||' ins, '||n_tup_upd||' upd, '||n_dead_tup||' dead, '||autovacuum_count||' autovacuums' FROM pg_stat_user_tables WHERE schemaname='supacache' AND relname LIKE 'kv_p%' ORDER BY relname"
STAT_SUM=$(psql_ "SELECT coalesce(sum(n_tup_ins+n_tup_upd),0)::bigint FROM pg_stat_user_tables WHERE schemaname='supacache'")
if [ "${STAT_SUM:-0}" = "0" ]; then
  echo "-> zero rows written, according to the statistics views. The WAL above says otherwise."
  echo "   The counters are pending inside the persist worker and are flushed only when it exits,"
  echo "   so they are unusable for monitoring a running cluster and unusable as an autovacuum"
  echo "   trigger. Overwrites still leave dead tuples; nothing is counting them."
fi

stop_pg
kill_stragglers
rm -rf $PGDATA
echo ""
echo "B/write is WAL bytes per logical cache write -- what the client issued."
echo "B/row is WAL bytes per row version the persist worker actually wrote,"
echo "counted out of the WAL with pg_waldump; the two differ by the dedup"
echo "factor, which is what key skew buys. FPI is the share of the WAL that is"
echo "full-page images: high on the cold pass, near zero on the warm one, so a"
echo "deployment's real figure sits between them and moves with checkpoint"
echo "frequency rather than being a single constant."
