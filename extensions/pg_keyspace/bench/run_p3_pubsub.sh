#!/usr/bin/env bash
# P3 §5 — pub/sub: SUBSCRIBE/PSUBSCRIBE/PUBLISH/UNSUBSCRIBE, pattern globbing,
# receiver counts, and the RESP2 subscribe-mode gate. Local single-worker
# fan-out (cross-worker pub/sub is a documented follow-up). Plaintext or TLS.
set -u
RESP=${RESP:-6381}
if redis-cli -p "$RESP" PING 2>/dev/null | grep -q PONG; then
  CLI="redis-cli -p $RESP"; SUBCLI="redis-cli -p $RESP"
else
  CLI="redis-cli --tls --insecure -p $RESP"; SUBCLI="redis-cli --tls --insecure -p $RESP"
fi
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-46s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-46s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }

echo "# P3 pub/sub (§5)"

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
