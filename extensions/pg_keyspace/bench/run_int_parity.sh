#!/usr/bin/env bash
# Strict integer-argument parsing (#150), compared verbatim against a real redis.
#
# Redis parses integer arguments with `string2ll`, which refuses a leading `+`,
# leading zeros and `-0`. pg_keyspace used Rust's `str::parse`, which accepts all
# three, so a command a real Redis refuses was executed here instead —
# `INCRBY n 05` moved the counter by 5 and said nothing.
#
# Two things this harness exists to stop coming back, because neither is
# obvious from the diff:
#
#  1. Swapping the parser is NOT the fix on its own. Nearly every call site read
#     `...parse().ok().unwrap_or(0)`, so a rejected argument became a silent
#     DEFAULT rather than an error. Every "refused" assertion below would pass
#     with a strict parser and a defaulting call site — they are here because
#     they failed exactly that way first.
#  2. Not every integer-looking argument is strict. FLOAT arguments go through
#     `strtod`, which accepts `+1`, and the SCAN CURSOR goes through `strtoull`,
#     which accepts `+1` and `00`. Tightening either would be a new divergence.
#     The "still accepted" section is the guard on that.
#
# Needs a real redis-server to compare against; skipped without one.
set -u
RESP=${RESP:-6381}
REDIS=${REDIS:-6379}
pass=0; fail=0
chk() { if [ "$2" = "$3" ]; then printf "  PASS  %-52s\n" "$1"; pass=$((pass+1));
        else printf "  FAIL  %-52s exp=[%s] got=[%s]\n" "$1" "$2" "$3"; fail=$((fail+1)); fi; }
K="redis-cli -p $RESP"
R="redis-cli -p $REDIS"
if ! $R PING 2>/dev/null | grep -q PONG; then
  echo "# integer parity: SKIPPED (no redis on :$REDIS)"; exit 0
fi
echo "# strict integer arguments, vs redis on :$REDIS"

seed() { # seed <cli>
  $1 FLUSHALL >/dev/null
  $1 SET k Hello >/dev/null; $1 SET n 5 >/dev/null
  $1 RPUSH l a b c >/dev/null; $1 HSET h f 1 >/dev/null
  $1 ZADD z 1 a 2 b >/dev/null; $1 SADD st a b c >/dev/null
  $1 SETBIT bb 7 1 >/dev/null
}
# Same command both sides, replies compared verbatim. Re-seeds first so an
# earlier case that mutated state cannot leak into a later comparison.
par() {
  seed "$R"; local a; a="$($R "$@" 2>&1 | paste -sd,)"
  seed "$K"; local b; b="$($K "$@" 2>&1 | paste -sd,)"
  chk "$*" "$a" "$b"
}
# Same, but comparing only "refused, and with which message" rather than the
# reply itself. For commands whose successful reply is a RANDOM member
# (SPOP/SRANDMEMBER/ZRANDMEMBER/HRANDFIELD) an exact comparison tests the RNG,
# not the parser; the refusals are what this harness is about and those are
# deterministic.
parc() {
  seed "$R"; local a; a="$($R "$@" 2>&1 | head -1)"
  seed "$K"; local b; b="$($K "$@" 2>&1 | head -1)"
  case "$a" in ERR*|WRONGTYPE*|NOPROTO*) : ;; *) a="(accepted)" ;; esac
  case "$b" in ERR*|WRONGTYPE*|NOPROTO*) : ;; *) b="(accepted)" ;; esac
  chk "$*" "$a" "$b"
}

for BAD in "+1" "01" "-0"; do
  echo "  -- argument '$BAD'"
  par GETRANGE k "$BAD" 4;            par GETRANGE k 0 "$BAD"
  par SETRANGE k "$BAD" x
  par EXPIRE n "$BAD";                par PEXPIRE n "$BAD"
  par EXPIREAT n "$BAD";              par PEXPIREAT n "$BAD"
  par INCRBY n "$BAD";                par DECRBY n "$BAD"
  par SETEX s2 "$BAD" v;              par PSETEX s2 "$BAD" v
  par GETEX k EX "$BAD"
  par SET k2 v EX "$BAD";             par SET k2 v PX "$BAD"
  par SET k2 v EXAT "$BAD";           par SET k2 v PXAT "$BAD"
  par LINDEX l "$BAD";                par LSET l "$BAD" x
  par LRANGE l "$BAD" -1;             par LTRIM l "$BAD" -1
  par LPOP l "$BAD";                  par RPOP l "$BAD"
  par LREM l "$BAD" a
  par LPOS l a RANK "$BAD";           par LPOS l a COUNT "$BAD"
  par ZRANGE z "$BAD" -1
  par ZRANGEBYSCORE z -inf +inf LIMIT "$BAD" 1
  par ZRANGEBYSCORE z -inf +inf LIMIT 0 "$BAD"
  par ZRANGEBYLEX z - + LIMIT "$BAD" 1
  par ZRANGESTORE dst z "$BAD" -1
  par ZPOPMIN z "$BAD";               par ZPOPMAX z "$BAD"
  parc ZRANDMEMBER z "$BAD";          parc HRANDFIELD h "$BAD"
  parc SPOP st "$BAD";                parc SRANDMEMBER st "$BAD"
  par HINCRBY h f "$BAD"
  par SINTERCARD "$BAD" st;           par SINTERCARD 1 st LIMIT "$BAD"
  par ZMPOP "$BAD" z MIN;             par ZUNIONSTORE d "$BAD" z
  par SCAN 0 COUNT "$BAD";            par HELLO "$BAD"
  par SETBIT bb "$BAD" 1;             par GETBIT bb "$BAD"
  par BITCOUNT k "$BAD" 1

  # Still ACCEPTED: a float argument is strtod, which takes a leading '+'.
  parc ZADD zf "$BAD" m;              parc ZINCRBY zf "$BAD" m
  parc INCRBYFLOAT nf "$BAD"
  # Still ACCEPTED: a SCAN cursor is strtoull. Compared as "not an error",
  # because cursor values and iteration order are implementation-defined.
  seed "$K"
  chk "SCAN $BAD is not refused" "0" "$($K SCAN "$BAD" 2>&1 | grep -c '^ERR')"
done

echo "  -- still accepted (the negative controls)"
par LREM l -1 a;                      par LPOS l a RANK -1
par LRANGE l 0 -1;                    par GETRANGE k 0 -1
par ZRANGEBYSCORE z -inf +inf LIMIT -1 1
par ZRANGEBYSCORE z -inf +inf LIMIT 0 -1
par ZRANGEBYLEX z - + LIMIT -1 1
par SETRANGE k -1 x
par ZADD zf +1.5 m2;                  par INCRBYFLOAT nf +2.5
par EXPIRE n 100;                     par SINTERCARD 1 st
par ZMPOP 1 z MIN;                    par ZUNIONSTORE d 1 z
par LPOS l a COUNT 0;                 par ZPOPMIN z 1
parc SPOP st 1

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
