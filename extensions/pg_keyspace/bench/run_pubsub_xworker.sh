#!/usr/bin/env bash
# CROSS-worker pub/sub. The scale-out daemon runs N shared-nothing slot
# workers as threads of one process; pub/sub channels are not sharded, so a
# PUBLISH on one worker must reach subscribers on ANY worker. This is delivered
# by an in-process Bus: a routing table (channel/pattern -> which workers hold
# subscribers) plus a per-worker inbox woken by an eventfd on that worker's
# epoll loop. This test proves a SUBSCRIBE on worker B receives a PUBLISH issued
# on worker A (and the reverse), across direct channels and glob patterns.
set -u
DAEMON=${DAEMON:-/home/user/postgres/extensions/pg_keyspace/core/target/release/pgkeyspaced}
BASE=${BASE:-6390}          # worker 0 = BASE, worker 1 = BASE+1
W0=$BASE; W1=$((BASE+1))
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

[ -x "$DAEMON" ] || { echo "  SKIP  daemon not built at $DAEMON"; exit 0; }

# start a 2-worker daemon (ephemeral tier — pub/sub needs no persistence)
"$DAEMON" --workers 2 --port "$BASE" --keys-per-worker 10000 --val-bytes 64 >/tmp/pgks_xworker.log 2>&1 &
DPID=$!
trap 'kill $DPID 2>/dev/null' EXIT
for _ in $(seq 1 20); do
  redis-cli -p "$W0" PING 2>/dev/null | grep -q PONG && redis-cli -p "$W1" PING 2>/dev/null | grep -q PONG && break
  sleep 0.2
done
redis-cli -p "$W0" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  daemon did not start"; cat /tmp/pgks_xworker.log; exit 1; }

echo "# cross-worker pub/sub — worker0=:$W0 worker1=:$W1"

# --- direct channel, A publishes -> B receives -----------------------------
subA=$(mktemp); subB=$(mktemp)
timeout 4 redis-cli -p "$W1" SUBSCRIBE news > "$subB" 2>&1 &   # subscriber on worker 1
sleep 1
chk "PUBLISH on w0 reaches 1 subscriber on w1"     "1" "$(redis-cli -p "$W0" PUBLISH news hello)"
sleep 1
gotB="$(grep -A2 '^message$' "$subB" | grep -vE '^message$|^--' | paste -sd,)"
chk "w1 subscriber received the w0 payload"        "news,hello" "$gotB"

# --- reverse direction: B publishes -> A receives --------------------------
timeout 4 redis-cli -p "$W0" SUBSCRIBE sports > "$subA" 2>&1 &  # subscriber on worker 0
sleep 1
chk "PUBLISH on w1 reaches 1 subscriber on w0"     "1" "$(redis-cli -p "$W1" PUBLISH sports goal)"
sleep 1
gotA="$(grep -A2 '^message$' "$subA" | grep -vE '^message$|^--' | paste -sd,)"
chk "w0 subscriber received the w1 payload"        "sports,goal" "$gotA"

# --- pattern subscription routes across workers ----------------------------
subP=$(mktemp)
timeout 4 redis-cli -p "$W1" PSUBSCRIBE 'news.*' > "$subP" 2>&1 &  # pattern sub on worker 1
sleep 1
chk "PUBLISH news.tech on w0 matches w1 pattern"   "1" "$(redis-cli -p "$W0" PUBLISH news.tech deep)"
sleep 1
gotP="$(grep -A3 '^pmessage$' "$subP" | grep -vE '^pmessage$|^--' | paste -sd,)"
chk "w1 pattern sub received the pmessage"         "news.*,news.tech,deep" "$gotP"

# --- local + remote receiver count sums correctly --------------------------
# one subscriber on each worker for the same channel; PUBLISH on w0 should count
# both (1 local + 1 remote).
subL=$(mktemp); subR=$(mktemp)
timeout 4 redis-cli -p "$W0" SUBSCRIBE room > "$subL" 2>&1 &   # local to publisher
timeout 4 redis-cli -p "$W1" SUBSCRIBE room > "$subR" 2>&1 &   # remote
sleep 1
chk "PUBLISH counts local+remote subscribers (2)"  "2" "$(redis-cli -p "$W0" PUBLISH room hi)"
sleep 1
chk "local subscriber got it"                      "room,hi" "$(grep -A2 '^message$' "$subL" | grep -vE '^message$|^--' | paste -sd,)"
chk "remote subscriber got it"                     "room,hi" "$(grep -A2 '^message$' "$subR" | grep -vE '^message$|^--' | paste -sd,)"

# --- no subscribers anywhere -> 0 ------------------------------------------
chk "PUBLISH with no subscribers -> 0"             "0" "$(redis-cli -p "$W0" PUBLISH ghost x)"

rm -f "$subA" "$subB" "$subP" "$subL" "$subR"
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
