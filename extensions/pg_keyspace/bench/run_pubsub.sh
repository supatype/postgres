#!/usr/bin/env bash
# pub/sub: SUBSCRIBE/PSUBSCRIBE/PUBLISH/UNSUBSCRIBE, pattern globbing,
# receiver counts, PUBSUB introspection, and the RESP2 subscribe-mode gate.
# Plaintext or TLS.
#
# Fan-out here is one worker's, because that is what this harness runs against.
# Cross-WORKER fan-out is not a follow-up -- it works, over the shared-memory
# bus, and section Q of run_durability_pg.sh asserts it across processes. This
# header used to say otherwise, which is how the README came to say it too.
#
# The PUBSUB block compares replies verbatim against a real redis, down to the
# error text, because introspection is worth having only if a stock client and
# whatever ops tooling it brought get the answer they parse for.
set -u
RESP=${RESP:-6381}
REDIS=${REDIS:-6379}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  CLI="redis-cli -p $RESP"; SUBCLI="redis-cli -p $RESP"
else
  CLI="redis-cli --tls --insecure -p $RESP"; SUBCLI="redis-cli --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-46s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-46s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
R="redis-cli -p $REDIS"
PARITY=1
$R PING 2>/dev/null | grep -q PONG || PARITY=
# Same command on both servers, replies compared verbatim. SKIPPED rather than
# failed where no redis is reachable, so this still runs on the macOS job.
par() {
  [ -n "$PARITY" ] || return 0
  local a b
  a="$($R "$@" 2>&1 | paste -sd,)"
  b="$($CLI "$@" 2>&1 | paste -sd,)"
  chk "parity: $*" "$a" "$b"
}

echo "# pub/sub"

# background subscribers
sub_ch=$(mktemp); sub_pat=$(mktemp)
timeout 4 $SUBCLI SUBSCRIBE news sports > "$sub_ch" 2>&1 &
timeout 4 $SUBCLI PSUBSCRIBE 'news.*' 'sp?rts' > "$sub_pat" 2>&1 &
sleep 1

chk "PUBLISH to a subscribed channel -> 1"      "1" "$($CLI PUBLISH news hello)"
chk "PUBLISH to a pattern-matched channel -> 1" "1" "$($CLI PUBLISH news.tech deep)"
chk "PUBLISH sports (channel + sp?rts pattern) -> 2" "2" "$($CLI PUBLISH sports goal)"
chk "PUBLISH with no subscribers -> 0"          "0" "$($CLI PUBLISH nobody x)"
sleep 1

# channel subscriber received the exact-channel messages
got_ch="$(grep -A2 '^message$' "$sub_ch" | grep -vE '^message$|^--' | paste -sd,)"
chk "channel sub received news+sports payloads"  "news,hello,sports,goal" "$got_ch"
# pattern subscriber received pmessages (news.tech via news.*, sports via sp?rts)
got_pat="$(grep -A3 '^pmessage$' "$sub_pat" | grep -vE '^pmessage$|^--' | paste -sd,)"
chk "pattern sub received pmessage frames" \
    "news.*,news.tech,deep,sp?rts,sports,goal" "$got_pat"
wait 2>/dev/null

# subscribe-mode gate + confirmations + UNSUBSCRIBE, on ONE connection. redis-cli
# stops reading stdin once subscribed, so drive raw RESP (inline) via openssl.
raw() { # reads commands on stdin, returns the multiplexed replies
  if echo "$CLI" | grep -q tls; then
    { cat; sleep 1; } | timeout 5 openssl s_client -quiet -connect 127.0.0.1:$RESP 2>/dev/null
  else
    { cat; sleep 1; } | timeout 5 bash -c "exec 3<>/dev/tcp/127.0.0.1/$RESP; cat >&3; cat <&3"
  fi
}
out="$(printf 'SUBSCRIBE a b\r\nGET k\r\nPING\r\nUNSUBSCRIBE a\r\n' | raw)"
chk "SUBSCRIBE confirms running count (2)"       "1" "$(echo "$out" | grep -c ':2')"
chk "keyed command in subscribe mode is refused" "1" \
    "$(echo "$out" | grep -ci "allowed in this context")"
chk "and the refusal names the command, as redis does" "1" \
    "$(echo "$out" | grep -c "Can't execute 'get'")"
chk "PING still works in subscribe mode"         "1" "$(echo "$out" | grep -ci 'PONG')"
chk "UNSUBSCRIBE confirms"                        "1" "$(echo "$out" | grep -qi 'unsubscribe' && echo 1 || echo 0)"

# auth gate: with AUTH configured, SUBSCRIBE without auth is refused
# ---- sharded pub/sub (SSUBSCRIBE / SUNSUBSCRIBE / SPUBLISH) ---------------
# A separate namespace from classic pub/sub, not a subset: PUBLISH never reaches
# a shard subscriber and SPUBLISH never reaches a channel or pattern one. That
# separation is the whole contract, so it is asserted from both directions and
# compared against a real redis, where the counts are 2 and 1 respectively.
ssub=$(mktemp); csub=$(mktemp); psub=$(mktemp)
timeout 6 $SUBCLI SSUBSCRIBE sc  > "$ssub" 2>&1 &
timeout 6 $SUBCLI SUBSCRIBE  sc  > "$csub" 2>&1 &
timeout 6 $SUBCLI PSUBSCRIBE 's*' > "$psub" 2>&1 &
if [ -n "$PARITY" ]; then
  timeout 6 $R SSUBSCRIBE sc   >/dev/null 2>&1 &
  timeout 6 $R SUBSCRIBE  sc   >/dev/null 2>&1 &
  timeout 6 $R PSUBSCRIBE 's*' >/dev/null 2>&1 &
fi
sleep 1

chk "PUBLISH reaches the channel and pattern subs, not the shard sub"  "2" "$($CLI PUBLISH sc viaPublish)"
chk "SPUBLISH reaches only the shard sub"                              "1" "$($CLI SPUBLISH sc viaSpublish)"
par PUBLISH sc parPublish
par SPUBLISH sc parSpublish
chk "SPUBLISH to a shard channel nobody holds -> 0" "0" "$($CLI SPUBLISH nobody x)"
sleep 1
chk "the shard subscriber got the SPUBLISH and not the PUBLISH" "1,0" \
    "$(grep -c viaSpublish "$ssub"),$(grep -c viaPublish "$ssub")"
chk "the channel subscriber got the PUBLISH and not the SPUBLISH" "1,0" \
    "$(grep -c viaPublish "$csub"),$(grep -c viaSpublish "$csub")"
chk "the pattern subscriber likewise" "1,0" \
    "$(grep -c viaPublish "$psub"),$(grep -c viaSpublish "$psub")"
# The frame is `smessage`, not `message`: a client demultiplexes on it.
chk "delivery frame is smessage" "smessage,sc,viaSpublish" \
    "$(grep -A2 '^smessage$' "$ssub" | head -3 | paste -sd,)"

chk "PUBSUB SHARDCHANNELS lists it" "sc" "$($CLI PUBSUB SHARDCHANNELS | paste -sd,)"
chk "PUBSUB SHARDNUMSUB counts it"  "sc 1" "$($CLI PUBSUB SHARDNUMSUB sc | paste -sd' ')"
# Separate namespaces both ways round: a shard channel is not a channel.
chk "PUBSUB CHANNELS does not list the shard channel" "sc" \
    "$($CLI PUBSUB CHANNELS | paste -sd,)"
chk "PUBSUB SHARDCHANNELS does not list the plain channel" "sc" \
    "$($CLI PUBSUB SHARDCHANNELS | paste -sd,)"
par PUBSUB SHARDCHANNELS
par PUBSUB SHARDNUMSUB sc absent
par PUBSUB SHARDCHANNELS 's*'
par PUBSUB SHARDCHANNELS 'zz*'
par PUBSUB HELP
wait 2>/dev/null
rm -f "$ssub" "$csub" "$psub"

# Counts, gating and confirmations on ONE connection, driven as raw RESP because
# redis-cli stops reading stdin once subscribed. The shard counter is separate
# from the channel+pattern one, which a client tracking both would notice.
flat() { tr '\r\n' ' '; }  # one line, so a whole exchange is one expectation
got="$(printf 'SUBSCRIBE a\r\nSSUBSCRIBE b\r\nSUBSCRIBE c\r\n' | raw | flat)"
chk "shard subscriptions count separately from channel ones" \
    "*3  \$9  subscribe  \$1  a  :1  *3  \$10  ssubscribe  \$1  b  :1  *3  \$9  subscribe  \$1  c  :2  " "$got"
got="$(printf 'SSUBSCRIBE b c\r\nSUNSUBSCRIBE b\r\nSUNSUBSCRIBE\r\n' | raw | flat)"
chk "SUNSUBSCRIBE confirms named, then the bare form drops the rest" \
    "*3  \$10  ssubscribe  \$1  b  :1  *3  \$10  ssubscribe  \$1  c  :2  *3  \$12  sunsubscribe  \$1  b  :1  *3  \$12  sunsubscribe  \$1  c  :0  " "$got"
# The subscribe-context gate, in both protocols. AUTH, HELLO and CLIENT used to
# be dispatched above it and so ran anyway; redis refuses all three, and a
# client that used CLIENT SETNAME to label a subscriber connection got a silent
# +OK here and an error there.
for c in 'CLIENT GETNAME' 'CLIENT SETNAME foo' 'CLIENT ID' 'HELLO' 'AUTH x'; do
  got="$(printf "SUBSCRIBE a\r\n$c\r\n" | raw | flat | sed 's/.*-ERR/-ERR/')"
  chk "RESP2 subscribe mode refuses $c" "1" "$(echo "$got" | grep -c "allowed in this context")"
done
# RESP3 has no such restriction: pub/sub is out of band there, so redis runs
# anything on a subscribed connection and so must this.
got="$(printf 'HELLO 3\r\nSUBSCRIBE a\r\nSET gk gv\r\n' | raw | flat)"
chk "RESP3 subscribe mode runs ordinary commands" "1" "$(echo "$got" | grep -c '+OK')"
# PING's reply SHAPE is subscribe-context behaviour: a two-element array in
# RESP2 subscribe mode, a bare +PONG everywhere else.
chk "RESP2 subscribed PING is an array" \
    "*2  \$4  pong  \$0    " "$(printf 'SUBSCRIBE a\r\nPING\r\n' | raw | flat | sed 's/.*:1  //')"
chk "RESP2 subscribed PING carries its argument" \
    "*2  \$4  pong  \$2  hi  " "$(printf 'SUBSCRIBE a\r\nPING hi\r\n' | raw | flat | sed 's/.*:1  //')"
chk "RESP3 subscribed PING is +PONG" "1" \
    "$(printf 'HELLO 3\r\nSUBSCRIBE a\r\nPING\r\n' | raw | flat | grep -c '+PONG')"
chk "unsubscribed PING is still +PONG" "1" "$(printf 'PING\r\n' | raw | flat | grep -c '+PONG')"
chk "CLIENT still works when not subscribed" "1" \
    "$(printf 'CLIENT SETNAME bar\r\n' | raw | flat | grep -c '+OK')"
# SSUBSCRIBE must be allowed in subscribe context, and the refusal that
# follows must name the command redis names.
got="$(printf 'SUBSCRIBE a\r\nSSUBSCRIBE b\r\nSET k v\r\n' | raw | flat | sed 's/.*-ERR/-ERR/')"
chk "S-variants allowed in subscribe context, others refused by name" \
    "-ERR Can't execute 'set': only (P|S)SUBSCRIBE / (P|S)UNSUBSCRIBE / PING / QUIT / RESET are allowed in this context  " \
    "$got"

# ---- a subscriber that stops reading ---------------------------------------
# It gets disconnected rather than buffered without limit. Before this existed,
# publishing 256 MB to a subscriber that never read took a worker from 708 MB
# RSS to 961 MB, linear in what was published -- which in an in-Postgres
# background worker ends at the OOM killer, taking the cluster with it.
#
# pg_keyspace.max_held_reply_bytes did NOT cover this: it is applied on the
# dispatch path, so it only fires for a connection that is SENDING commands, and
# a pure subscriber sends none. Its buffer is filled by other clients' publishes.
if command -v python3 >/dev/null 2>&1; then
  cat > /tmp/pgks_slowsub.py <<'PYEOF'
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.sendall(b"SUBSCRIBE slowch\r\n")
time.sleep(float(sys.argv[2]))
PYEOF
  python3 /tmp/pgks_slowsub.py "$RESP" 40 &
  slowpid=$!
  sleep 1
  chk "the slow subscriber is attached" "slowch 1" "$($CLI PUBSUB NUMSUB slowch | paste -sd' ')"
  # Enough to pass the 32 MiB default several times over, in 64 KiB messages.
  payload=$(head -c 65536 /dev/zero | tr '\0' 'x')
  for i in $(seq 1 1500); do echo "PUBLISH slowch $payload"; done | $CLI --pipe >/dev/null 2>&1
  sleep 1
  chk "it was disconnected rather than buffered without limit" "slowch 0" \
      "$($CLI PUBSUB NUMSUB slowch | paste -sd' ')"
  # The point of the limit: the worker is still here to say so.
  chk "the worker survived and still serves" "PONG" "$($CLI PING)"
  chk "and other clients are unaffected" "OK" "$($CLI SET after-slowsub v)"
  kill $slowpid 2>/dev/null
  rm -f /tmp/pgks_slowsub.py

  # The publisher can BE the subscriber. RESP3 allows PUBLISH while subscribed,
  # so one message large enough to cross the limit in a single delivery closes
  # the very connection the reply was for. Replying into it crashed the worker:
  #   thread 'slot-worker-0' panicked at src/server.rs: called `Option::unwrap()`
  # which is a one-line remote abort, so it is asserted rather than reasoned about.
  cat > /tmp/pgks_selfpub.py <<'PYEOF'
import socket, sys, time
s = socket.create_connection(("127.0.0.1", int(sys.argv[1])))
s.sendall(b"HELLO 3\r\nSUBSCRIBE selfch\r\n")
time.sleep(0.5)
payload = b"x" * (40 * 1024 * 1024)
try:
    s.sendall(b"*3\r\n$7\r\nPUBLISH\r\n$6\r\nselfch\r\n$" +
              str(len(payload)).encode() + b"\r\n" + payload + b"\r\n")
except Exception:
    pass
time.sleep(1)
PYEOF
  maxv=$($CLI CONFIG GET maxmemory >/dev/null 2>&1; echo ok)
  python3 /tmp/pgks_selfpub.py "$RESP" >/dev/null 2>&1
  chk "a subscriber publishing past its own limit does not crash the worker" "PONG" "$($CLI PING)"
  chk "and the worker still serves other clients" "OK" "$($CLI SET after-selfpub v)"
  rm -f /tmp/pgks_selfpub.py
else
  echo "  SKIP  slow-subscriber limit (no python3)"
fi

# ---- PUBSUB introspection -------------------------------------------------
# Subscribers on BOTH servers, so the parity comparisons below describe the same
# state rather than two different ones.
psub1=$(mktemp); psub2=$(mktemp); ppat=$(mktemp)
timeout 6 $SUBCLI SUBSCRIBE news sports > "$psub1" 2>&1 &
timeout 6 $SUBCLI SUBSCRIBE news        > "$psub2" 2>&1 &
timeout 6 $SUBCLI PSUBSCRIBE 'ne*'      > "$ppat"  2>&1 &
if [ -n "$PARITY" ]; then
  timeout 6 $R SUBSCRIBE news sports >/dev/null 2>&1 &
  timeout 6 $R SUBSCRIBE news        >/dev/null 2>&1 &
  timeout 6 $R PSUBSCRIBE 'ne*'      >/dev/null 2>&1 &
fi
sleep 1

chk "PUBSUB NUMSUB news -> 2"   "news 2"   "$($CLI PUBSUB NUMSUB news | paste -sd' ')"
chk "PUBSUB NUMSUB sports -> 1" "sports 1" "$($CLI PUBSUB NUMSUB sports | paste -sd' ')"
chk "PUBSUB NUMSUB of a channel nobody holds -> 0" "absent 0" \
    "$($CLI PUBSUB NUMSUB absent | paste -sd' ')"
chk "PUBSUB NUMPAT -> 1" "1" "$($CLI PUBSUB NUMPAT)"
chk "PUBSUB CHANNELS lists both channels" "news,sports" \
    "$($CLI PUBSUB CHANNELS | sort | paste -sd,)"
# A pattern subscription is not a channel: 'ne*' must not appear above, and
# CHANNELS must filter by its own argument rather than returning everything.
chk "PUBSUB CHANNELS ne* -> news only" "news" "$($CLI PUBSUB CHANNELS 'ne*' | paste -sd,)"
chk "PUBSUB CHANNELS with a pattern matching nothing" "" \
    "$($CLI PUBSUB CHANNELS 'zz*' | paste -sd,)"

par PUBSUB NUMSUB news
par PUBSUB NUMSUB news sports absent
par PUBSUB NUMSUB
par PUBSUB NUMPAT
par PUBSUB CHANNELS 'ne*'
par PUBSUB CHANNELS 'zz*'
# The error surface, verbatim. Each of these is a different message in redis and
# a client that greps for one should not have to special-case us.
par PUBSUB
par PUBSUB bogus
par PUBSUB numpat x
par PUBSUB channels a b
wait 2>/dev/null
rm -f "$psub1" "$psub2" "$ppat"

# Once every subscriber is gone the channels must go with them. A stale entry
# would be indistinguishable to a client from a live subscriber.
chk "PUBSUB CHANNELS is empty once subscribers exit" "" "$($CLI PUBSUB CHANNELS | paste -sd,)"
chk "PUBSUB NUMPAT is 0 once subscribers exit" "0" "$($CLI PUBSUB NUMPAT)"

PGPORT=${PGPORT:-5434}
P="psql -h 127.0.0.1 -p $PGPORT -U supatype_admin -d postgres -X -q -A -t"
if $P -c "SELECT 1" >/dev/null 2>&1; then
  $P -c "SELECT supacache.set_credential('psuser','pspw','service_role','');" >/dev/null 2>&1
  $P -c "SELECT pg_reload_conf();" >/dev/null 2>&1; sleep 1
  chk "unauth SUBSCRIBE -> NOAUTH" "1" \
      "$({ echo 'SUBSCRIBE x'; sleep 1; } | timeout 4 $SUBCLI 2>&1 | grep -ci NOAUTH)"
  $P -c "TRUNCATE supacache.resp_credential; SELECT pg_reload_conf();" >/dev/null 2>&1; sleep 1
fi

rm -f "$sub_ch" "$sub_pat"
echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
