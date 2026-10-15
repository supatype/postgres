#!/usr/bin/env bash
# Expiry arguments: what is accepted, what is refused, and the TTL that results,
# for every command that takes one. The expected column is Valkey 8's reply,
# captured from valkey/valkey:8 (8.1) and kept here verbatim, so this runs with
# no reference server and the same table can be pointed at a real Valkey to
# re-derive it:  HOST=<valkey host> RESP=6379 bash bench/run_expire_parity.sh
#
# The bug this exists to stop coming back: deadlines were computed with
# saturating or unchecked arithmetic in microseconds, so a value whose
# millisecond deadline overflows int64 was accepted and stored as nonsense.
# EXPIREAT k 99999999999999999 answered 1 and then reported a TTL of about
# 9221579948775; EXPIREAT k -2^62 was accepted; EXPIREAT k 0 cleared the TTL
# instead of deleting the key. Valkey checks for overflow when converting
# seconds to milliseconds and again when adding the base time of a relative
# form, and refuses either with "invalid expire time in '<command>' command".
#
# One divergence is structural and deliberate. pg_keyspace stores deadlines as
# int64 MICROseconds, so the latest deadline it can hold is 9223372036854775 ms
# (the year 294247). Valkey stores milliseconds and accepts deadlines up to
# 2^63-1 ms. A deadline past what pg_keyspace can hold is ACCEPTED exactly as
# Valkey accepts it and clamped to that latest deadline, so those rows assert
# the reply and "TTL at least 9221000000000000 ms" rather than the exact TTL.
set -u
RESP=${RESP:-6381}
HOST=${HOST:-127.0.0.1}
TOL=${TOL:-3000} # ms of slack for "TTL is about N"
K="redis-cli -h $HOST -p $RESP"
if ! $K PING 2>/dev/null | grep -q PONG; then
  echo "# expire parity: nothing answering on $HOST:$RESP"; exit 1
fi
pass=0; fail=0
ok()  { printf "  PASS  %-58s\n" "$1"; pass=$((pass+1)); }
bad() { printf "  FAIL  %-58s %s\n" "$1" "$2"; fail=$((fail+1)); }
chk() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "exp=[$2] got=[$3]"; fi; }
now_ms() { date +%s%3N; }

# Does `pttl` satisfy the TTL expectation `want`?
#   -2 / -1    exact (gone / no expiry)
#   rel:N      a relative TTL of N ms, give or take TOL
#   at:D       an absolute deadline of D unix ms, give or take TOL
#   ge:N       at least N ms (the clamped region described above)
ttl_ok() { # ttl_ok <want> <pttl>
  local want=$1 got=$2 n
  case "$got" in ''|*[!0-9-]*) return 1 ;; esac
  case "$want" in
    -1|-2) [ "$got" = "$want" ] ;;
    rel:*) n=${want#rel:}; [ "$got" -le "$n" ] && [ "$got" -ge $((n - TOL)) ] ;;
    at:*)  n=$(( ${want#at:} - $(now_ms) ))
           [ "$got" -le $((n + TOL)) ] && [ "$got" -ge $((n - TOL)) ] ;;
    ge:*)  [ "$got" -ge "${want#ge:}" ] ;;
    *) return 1 ;;
  esac
}

# row <seed> <want reply> <want ttl> <command...>
#   seed  plain   k holds "v" with no TTL
#         ttl1000 k holds "v" with a 1000 s TTL
# The TTL checked is that of the command's key, argument 2.
row() {
  local seed=$1 want=$2 wttl=$3; shift 3
  $K DEL k nokey >/dev/null
  case "$seed" in
    plain)   $K SET k v >/dev/null ;;
    ttl1000) $K SET k v EX 1000 >/dev/null ;;
  esac
  local got pttl
  # $(...) drops the blank line redis-cli prints after an error; any line
  # breaks left are a multi-line reply, joined so the row stays on one line.
  got=$($K "$@" 2>&1); got=${got//$'\n'/,}
  pttl=$($K PTTL "$2")
  if [ "$got" != "$want" ]; then
    bad "[$seed] $*" "reply exp=[$want] got=[$got]"
  elif ! ttl_ok "$wttl" "$pttl"; then
    bad "[$seed] $*" "pttl exp=[$wttl] got=[$pttl]"
  else
    ok "[$seed] $*"
  fi
}

E_EXPIRE="ERR invalid expire time in 'expire' command"
E_PEXPIRE="ERR invalid expire time in 'pexpire' command"
E_EXPIREAT="ERR invalid expire time in 'expireat' command"
E_SET="ERR invalid expire time in 'set' command"
E_GETEX="ERR invalid expire time in 'getex' command"
E_SETEX="ERR invalid expire time in 'setex' command"
E_PSETEX="ERR invalid expire time in 'psetex' command"
E_INT="ERR value is not an integer or out of range"
E_SYN="ERR syntax error"
I64_MAX=9223372036854775807
I64_MIN=-9223372036854775808
MAX_S=9223372036854775   # i64::MAX / 1000: the largest seconds value that converts
HUGE=ge:9221000000000000 # a TTL in the clamped region (see the header)
Y2100_S=4102444800       # 2100-01-01T00:00:00Z

echo "# expiry arguments vs Valkey 8, on $HOST:$RESP"

echo "  -- EXPIRE (relative seconds)"
row plain   "1"         rel:100000 EXPIRE k 100
row plain   "1"         -2         EXPIRE k 0
row plain   "1"         -2         EXPIRE k -1
row plain   "1"         -2         EXPIRE k -$MAX_S
row plain   "$E_EXPIRE" -1         EXPIRE k -9223372036854776
row plain   "$E_EXPIRE" -1         EXPIRE k -4611686018427387904
row plain   "$E_EXPIRE" -1         EXPIRE k $I64_MIN
row plain   "1"         $HUGE      EXPIRE k 9223370000000000
row plain   "$E_EXPIRE" -1         EXPIRE k $MAX_S
row plain   "$E_EXPIRE" -1         EXPIRE k 9223372036854776
row plain   "$E_EXPIRE" -1         EXPIRE k 99999999999999999
row plain   "$E_EXPIRE" -1         EXPIRE k $I64_MAX
row plain   "$E_INT"    -1         EXPIRE k 9223372036854775808
row plain   "$E_INT"    -1         EXPIRE k abc
row plain   "0"         -2         EXPIRE nokey 100
row plain   "0"         -2         EXPIRE nokey -1
row plain   "$E_EXPIRE" -2         EXPIRE nokey $I64_MAX

echo "  -- PEXPIRE (relative milliseconds)"
row plain   "1"          rel:100000 PEXPIRE k 100000
row plain   "1"          -2         PEXPIRE k 0
row plain   "1"          -2         PEXPIRE k $I64_MIN
row plain   "1"          $HUGE      PEXPIRE k 99999999999999999
row plain   "1"          $HUGE      PEXPIRE k 9223370000000000000
row plain   "$E_PEXPIRE" -1         PEXPIRE k 9223372036854775806
row plain   "$E_PEXPIRE" -1         PEXPIRE k $I64_MAX

echo "  -- EXPIREAT (absolute seconds)"
row plain   "1"           -2              EXPIREAT k 0
row plain   "1"           -2              EXPIREAT k 1
row plain   "1"           -2              EXPIREAT k -1
row plain   "1"           -2              EXPIREAT k -$MAX_S
row plain   "1"           at:${Y2100_S}000 EXPIREAT k $Y2100_S
row plain   "1"           $HUGE           EXPIREAT k $MAX_S
row plain   "$E_EXPIREAT" -1              EXPIREAT k 9223372036854776
row plain   "$E_EXPIREAT" -1              EXPIREAT k 99999999999999999
row plain   "$E_EXPIREAT" -1              EXPIREAT k $I64_MAX
row plain   "$E_EXPIREAT" -1              EXPIREAT k -9223372036854776
row plain   "$E_EXPIREAT" -1              EXPIREAT k -4611686018427387904
row plain   "$E_EXPIREAT" -1              EXPIREAT k $I64_MIN

echo "  -- PEXPIREAT (absolute milliseconds)"
row plain   "1" -2               PEXPIREAT k 0
row plain   "1" -2               PEXPIREAT k $I64_MIN
row plain   "1" at:${Y2100_S}000 PEXPIREAT k ${Y2100_S}000
row plain   "1" at:$MAX_S        PEXPIREAT k $MAX_S
row plain   "1" $HUGE            PEXPIREAT k 99999999999999999
row plain   "1" $HUGE            PEXPIREAT k $I64_MAX

echo "  -- EXPIRE family options (NX / XX / GT / LT)"
row plain   "1" rel:10000   EXPIRE k 10 NX
row plain   "0" -1          EXPIRE k 10 XX
row plain   "0" -1          EXPIRE k 10 GT
row plain   "1" rel:10000   EXPIRE k 10 LT
row plain   "1" rel:10000   EXPIRE k 10 nx
row ttl1000 "0" rel:1000000 EXPIRE k 10 NX
row ttl1000 "1" rel:10000   EXPIRE k 10 XX
row ttl1000 "0" rel:1000000 EXPIRE k 10 GT
row ttl1000 "1" rel:2000000 EXPIRE k 2000 GT
row ttl1000 "1" rel:10000   EXPIRE k 10 LT
row ttl1000 "0" rel:1000000 EXPIRE k 2000 LT
row ttl1000 "1" rel:10000   PEXPIRE k 10000 XX
row ttl1000 "1" rel:2000000 PEXPIREAT k $(( $(date +%s) * 1000 + 2000000 )) GT
row ttl1000 "0" rel:1000000 EXPIRE k -1 GT
row ttl1000 "1" -2          EXPIRE k -1 LT
row ttl1000 "1" rel:10000   EXPIRE k 10 XX LT
row plain   "ERR NX and XX, GT or LT options at the same time are not compatible" -1 EXPIRE k 10 NX XX
row plain   "ERR NX and XX, GT or LT options at the same time are not compatible" -1 EXPIRE k 10 NX GT
row plain   "ERR GT and LT options at the same time are not compatible"           -1 EXPIRE k 10 GT LT
row plain   "ERR Unsupported option FOO" -1 EXPIRE k 10 FOO
row plain   "ERR Unsupported option FOO" -1 EXPIRE k abc FOO
row plain   "$E_EXPIRE" -1 EXPIRE k $I64_MAX NX
row plain   "$E_INT"    -1 EXPIRE k abc NX

echo "  -- SET EX / PX / EXAT / PXAT"
row plain   "OK"     rel:100000      SET k v2 EX 100
row plain   "$E_SET" -1              SET k v2 EX 0
row plain   "$E_SET" -1              SET k v2 EX -1
row plain   "OK"     $HUGE           SET k v2 EX 9223370000000000
row plain   "$E_SET" -1              SET k v2 EX $MAX_S
row plain   "$E_SET" -1              SET k v2 EX 9223372036854776
row plain   "$E_SET" -1              SET k v2 EX $I64_MAX
row plain   "$E_INT" -1              SET k v2 EX abc
row plain   "OK"     rel:100000      SET k v2 PX 100000
row plain   "$E_SET" -1              SET k v2 PX 0
row plain   "$E_SET" -1              SET k v2 PX -1
row plain   "OK"     $HUGE           SET k v2 PX 9223370000000000000
row plain   "$E_SET" -1              SET k v2 PX $I64_MAX
row plain   "OK"     -2              SET k v2 EXAT 1
row plain   "$E_SET" -1              SET k v2 EXAT 0
row plain   "$E_SET" -1              SET k v2 EXAT -1
row plain   "OK"     at:${Y2100_S}000 SET k v2 EXAT $Y2100_S
row plain   "OK"     $HUGE           SET k v2 EXAT $MAX_S
row plain   "$E_SET" -1              SET k v2 EXAT 9223372036854776
row plain   "$E_SET" -1              SET k v2 EXAT 99999999999999999
row plain   "OK"     -2              SET k v2 PXAT 1
row plain   "$E_SET" -1              SET k v2 PXAT 0
row plain   "$E_SET" -1              SET k v2 PXAT -4611686018427387904
row plain   "OK"     at:$MAX_S       SET k v2 PXAT $MAX_S
row plain   "OK"     $HUGE           SET k v2 PXAT $I64_MAX
row plain   "v"      -2              SET k v2 EXAT 1 GET
row plain   "$E_SET" -1              SET k v2 EX 0 NX
row plain   "OK"     rel:20000       SET k v2 EX 10 EX 20
row plain   "$E_SYN" -1              SET k v2 EX 10 PX 10
row plain   "$E_SYN" -1              SET k v2 KEEPTTL EX 10
row plain   "$E_SYN" -1              SET k v2 EX 10 KEEPTTL
row plain   "$E_SYN" -1              SET k v2 EXAT 10 PXAT 10
row plain   "$E_SYN" -1              SET k v2 EX abc PX 10
row plain   "$E_SYN" -1              SET k v2 EX

echo "  -- GETEX"
row plain   "v"        rel:100000 GETEX k EX 100
row plain   "$E_GETEX" -1         GETEX k EX 0
row plain   "$E_GETEX" -1         GETEX k EX $MAX_S
row plain   "$E_GETEX" -1         GETEX k PX 0
row plain   "$E_GETEX" -1         GETEX k PX $I64_MAX
row plain   "v"        $HUGE      GETEX k PX 9223370000000000000
row plain   "v"        -2         GETEX k EXAT 1
row plain   "$E_GETEX" -1         GETEX k EXAT 0
row plain   "$E_GETEX" -1         GETEX k EXAT 9223372036854776
row plain   "v"        $HUGE      GETEX k EXAT $MAX_S
row plain   "$E_GETEX" -1         GETEX k PXAT -1
row plain   "v"        $HUGE      GETEX k PXAT $I64_MAX
row ttl1000 "v"        -1         GETEX k PERSIST
row plain   "v"        rel:20000  GETEX k EX 10 EX 20
row plain   "$E_INT"   -1         GETEX k EX abc
row plain   ""         -2         GETEX nokey EX 0
row plain   ""         -2         GETEX nokey EX abc
row plain   "$E_SYN"   -2         GETEX nokey FOO
row plain   "$E_SYN"   -1         GETEX k FOO
row plain   "$E_SYN"   -1         GETEX k EX
row plain   "$E_SYN"   -1         GETEX k KEEPTTL
row plain   "$E_SYN"   -1         GETEX k PERSIST EX 10
row plain   "$E_SYN"   -1         GETEX k EX 10 PX 10

echo "  -- SETEX / PSETEX"
row plain   "OK"        rel:100000 SETEX k 100 v3
row plain   "$E_SETEX"  -1         SETEX k 0 v3
row plain   "$E_SETEX"  -1         SETEX k -1 v3
row plain   "$E_SETEX"  -1         SETEX k $MAX_S v3
row plain   "$E_SETEX"  -1         SETEX k $I64_MAX v3
row plain   "OK"        $HUGE      SETEX k 9223370000000000 v3
row plain   "$E_INT"    -1         SETEX k abc v3
row plain   "OK"        rel:100000 PSETEX k 100000 v3
row plain   "$E_PSETEX" -1         PSETEX k 0 v3
row plain   "$E_PSETEX" -1         PSETEX k -1 v3
row plain   "$E_PSETEX" -1         PSETEX k $I64_MAX v3
row plain   "OK"        $HUGE      PSETEX k 9223370000000000000 v3

echo "  -- EXPIRETIME rounds to the nearest second, as TTL does"
row plain "1" at:99999999999500 PEXPIREAT k 99999999999500
chk "EXPIRETIME after PEXPIREAT 99999999999500"  "100000000000"   "$($K EXPIRETIME k)"
chk "PEXPIRETIME after PEXPIREAT 99999999999500" "99999999999500" "$($K PEXPIRETIME k)"
$K DEL k >/dev/null

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
