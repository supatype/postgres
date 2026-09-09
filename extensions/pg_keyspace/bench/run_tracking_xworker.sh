#!/usr/bin/env bash
# CROSS-worker client-side-caching invalidation. The scale-out daemon runs N
# shared-nothing slot workers as threads of one process. CLIENT TRACKING tables
# are per-worker, so when tracking is active anywhere a write broadcasts the
# changed key over the same in-process Bus that carries pub/sub; each worker then
# invalidates its own trackers of that key. This test proves a tracker on worker
# B is invalidated by a write issued on worker A (default mode, BCAST, and the
# whole-keyspace FLUSHALL signal). The RESP3 client is a raw socket so we read the
# exact out-of-band `invalidate` push bytes without depending on redis-cli.
set -u
DAEMON=${DAEMON:-/home/user/postgres/extensions/pg_keyspace/core/target/release/pgkeyspaced}
BASE=${BASE:-6392}          # worker 0 = BASE, worker 1 = BASE+1
W0=$BASE; W1=$((BASE+1))
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

[ -x "$DAEMON" ] || { echo "  SKIP  daemon not built at $DAEMON"; exit 0; }

"$DAEMON" --workers 2 --port "$BASE" --keys-per-worker 10000 --val-bytes 64 >/tmp/pgks_track_xworker.log 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null' EXIT
for _ in $(seq 1 20); do
  redis-cli -p "$W0" PING 2>/dev/null | grep -q PONG && redis-cli -p "$W1" PING 2>/dev/null | grep -q PONG && break
  sleep 0.2
done
redis-cli -p "$W1" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  daemon did not start"; cat /tmp/pgks_track_xworker.log; exit 1; }

echo "# cross-worker CLIENT TRACKING — worker0=:$W0 worker1=:$W1"

# --- default mode: tracker on w1, writer on w0 -----------------------------
# The tracker GETs xk on w1 (registering it); a write to xk on w0 must reach w1
# over the Bus and invalidate the tracker there.
redis-cli -p "$W1" SET xk v0 >/dev/null
inv=""
if exec 3<>"/dev/tcp/127.0.0.1/$W1" 2>/dev/null; then
  printf 'HELLO 3\r\nCLIENT TRACKING ON\r\nGET xk\r\n' >&3
  sleep 0.3
  redis-cli -p "$W0" SET xk v9 >/dev/null   # different worker, different segment
  inv=$(timeout 1 cat <&3 2>/dev/null || true)
  exec 3<&- 2>/dev/null || true
fi
chk "default: w0 write invalidates w1 tracker" "1" "$(printf '%s' "$inv" | grep -ci invalidate)"
chk "default: invalidation names the key"      "1" "$(printf '%s' "$inv" | grep -c 'xk')"

# --- BCAST: prefix tracker on w1, writer on w0 -----------------------------
inv=""
if exec 3<>"/dev/tcp/127.0.0.1/$W1" 2>/dev/null; then
  printf 'HELLO 3\r\nCLIENT TRACKING ON BCAST PREFIX bx:\r\n' >&3
  sleep 0.3
  redis-cli -p "$W0" SET bx:1 v >/dev/null  # matches prefix; never read on w1
  inv=$(timeout 1 cat <&3 2>/dev/null || true)
  exec 3<&- 2>/dev/null || true
fi
chk "BCAST: w0 prefix write invalidates w1"    "1" "$(printf '%s' "$inv" | grep -ci invalidate)"
chk "BCAST: invalidation names the key"        "1" "$(printf '%s' "$inv" | grep -c 'bx:1')"

# --- FLUSHALL on w0 sends the whole-keyspace signal to a w1 tracker --------
redis-cli -p "$W1" SET fk v0 >/dev/null
inv=""
if exec 3<>"/dev/tcp/127.0.0.1/$W1" 2>/dev/null; then
  printf 'HELLO 3\r\nCLIENT TRACKING ON\r\nGET fk\r\n' >&3
  sleep 0.3
  redis-cli -p "$W0" FLUSHALL >/dev/null
  inv=$(timeout 1 cat <&3 2>/dev/null || true)
  exec 3<&- 2>/dev/null || true
fi
chk "FLUSHALL on w0 signals w1 tracker"        "1" "$(printf '%s' "$inv" | grep -ci invalidate)"
# the whole-keyspace signal is a null (_) after invalidate, carrying no key.
chk "FLUSHALL signal is a null invalidate"     "1" "$(printf '%s' "$inv" | grep -c '^_')"

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
