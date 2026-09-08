#!/usr/bin/env bash
# REAL synchronous replication (replicated tier). A standalone
# pgks-replica standby process receives streamed WAL batches over a socket,
# appends+fsyncs them to its own WAL, and acks; the primary's replicated SET
# returns only once the standby has acked. This proves the write is durable on a
# SECOND node — not a sleep. Also contrasts replicated vs durable latency (the
# real socket round-trip + second fsync).
set -u
DAEMON=${DAEMON:-/home/user/postgres/extensions/pg_keyspace/core/target/release/pgkeyspaced}
REPLICA=${REPLICA:-/home/user/postgres/extensions/pg_keyspace/core/target/release/pgks-replica}
PORT=${PORT:-6500}
RPORT=${RPORT:-7500}
DIR=${DIR:-/tmp/pgks_repl_$$}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

[ -x "$DAEMON" ] && [ -x "$REPLICA" ] || { echo "  SKIP  binaries not built"; exit 0; }
mkdir -p "$DIR/pri" "$DIR/rep"

# 1. start the standby, then the primary pointed at it
"$REPLICA" --host 127.0.0.1 --port "$RPORT" --workers 1 --wal-dir "$DIR/rep" >/tmp/pgks_replica.log 2>&1 &
RPID=$!
sleep 0.5
"$DAEMON" --workers 1 --port "$PORT" --tier replicated --wal-dir "$DIR/pri" \
  --replica-addr 127.0.0.1 --replica-port "$RPORT" --commit-window-us 200 --shmem-mb 64 \
  >/tmp/pgks_repl_pri.log 2>&1 &
DPID=$!
trap 'kill $DPID $RPID 2>/dev/null' EXIT
for _ in $(seq 1 30); do redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG && break; sleep 0.2; done
redis-cli -p "$PORT" PING 2>/dev/null | grep -q PONG || { echo "  FAIL  primary did not start"; cat /tmp/pgks_repl_pri.log; exit 1; }

echo "# real synchronous replication — primary :$PORT, standby :$RPORT"

RWAL="$DIR/rep/pgks_replica_w0.wal"
PWAL="$(ls "$DIR"/pri/*.wal 2>/dev/null | head -1)"

# 2. a replicated SET returns only after the standby acked -> the value must
#    ALREADY be on the standby's WAL the instant the reply comes back.
chk "replicated SET -> OK" "OK" "$(redis-cli -p $PORT SET k1 REPLTOKEN_ALPHA)"
chk "standby WAL has the value immediately after the ack" \
    "1" "$(grep -c REPLTOKEN_ALPHA "$RWAL" 2>/dev/null | grep -q '[1-9]' && echo 1 || echo 0)"
chk "primary WAL also has it (durable locally)" \
    "1" "$(grep -c REPLTOKEN_ALPHA "$PWAL" 2>/dev/null | grep -q '[1-9]' && echo 1 || echo 0)"
chk "GET reads it back from the primary" "REPLTOKEN_ALPHA" "$(redis-cli -p $PORT GET k1)"

# 3. many writes all reach the standby (real streaming, not a one-off)
for i in $(seq 1 200); do redis-cli -p "$PORT" SET "rk$i" "RVAL$i" >/dev/null; done
sleep 0.5
present=0
for i in 1 50 123 200; do grep -q "RVAL$i\b" "$RWAL" 2>/dev/null && present=$((present+1)); done
chk "a spread of 200 writes are all on the standby" "4" "$present"
chk "standby WAL size > 0" "1" "$([ -s "$RWAL" ] && echo 1 || echo 0)"

# 4. latency: 300 replicated SETs carry a real socket round-trip + a second
#    fsync on the standby (reported as evidence the path is real, not free).
t0=$(date +%s.%N)
for i in $(seq 1 300); do redis-cli -p "$PORT" SET "lk$i" v >/dev/null; done
t1=$(date +%s.%N)
echo "  INFO  300 replicated SETs in $(awk "BEGIN{printf \"%.2f\", $t1-$t0}")s (incl. standby ack per commit)"

# 5. isolation of the mechanism: with the standby DOWN, a replicated write blocks
#    (synchronous rep really waits for the ack). Kill the replica; a SET must not
#    return OK within a short timeout.
kill $RPID 2>/dev/null; sleep 0.5
got="$(timeout 2 redis-cli -p "$PORT" SET k2 SHOULD_BLOCK 2>&1)"
chk "with standby down, a replicated SET does NOT ack" "1" "$([ "$got" != "OK" ] && echo 1 || echo 0)"

echo
echo "# result: $pass passed, $fail failed"
rm -rf "$DIR"
[ "$fail" -eq 0 ]
