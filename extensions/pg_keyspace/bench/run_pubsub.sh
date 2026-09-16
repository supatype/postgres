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
chk "keyed command in subscribe mode is refused" "1" "$(echo "$out" | grep -ci 'subscribe context')"
chk "PING still works in subscribe mode"         "1" "$(echo "$out" | grep -ci 'PONG')"
chk "UNSUBSCRIBE confirms"                        "1" "$(echo "$out" | grep -qi 'unsubscribe' && echo 1 || echo 0)"

# auth gate: with AUTH configured, SUBSCRIBE without auth is refused
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
