#!/usr/bin/env bash
# Arity errors: every command this server implements, sent with too few
# arguments (and, where Valkey's arity is exact, one too many), must be refused
# with Valkey 8's exact reply:
#
#   ERR wrong number of arguments for '<name>' command
#
# where <name> is the command's registered name in lower case, whatever case it
# was typed in, and `container|subcommand` for a subcommand. The rows are
# generated from Valkey 8's own COMMAND INFO arities (valkey/valkey:8, 8.1) and
# kept here verbatim, so this runs with no reference server and the same table
# can be pointed at a real Valkey to re-derive it:
#   HOST=<valkey host> RESP=6379 bash bench/run_arity_parity.sh
#
# What this exists to stop: the arity check lived in each handler, so about
# seventy of them dropped the trailing " command" from the text, a handful said
# only "ERR wrong number of arguments", and others (SELECT, CONFIG, DBSIZE with
# an argument, a short CLIENT subcommand) were accepted and answered OK. Valkey
# checks arity once, before the command runs, and so does pg_keyspace now.
#
# BF.* and CF.* are not here: Valkey 8 has no probabilistic types, and their
# arity replies are compared against Redis 8 by run_bloom.sh and run_cuckoo.sh.
set -u
RESP=${RESP:-6381}
HOST=${HOST:-127.0.0.1}
K="redis-cli -h $HOST -p $RESP"
if ! $K PING 2>/dev/null | grep -q PONG; then
  echo "# arity parity: nothing answering on $HOST:$RESP"; exit 1
fi
pass=0; fail=0
ok()  { printf "  PASS  %-58s\n" "$1"; pass=$((pass+1)); }
bad() { printf "  FAIL  %-58s %s\n" "$1" "$2"; fail=$((fail+1)); }
chk() { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "exp=[$2] got=[$3]"; fi; }

# row <name> <command...>: the reply must be the arity error naming <name>.
# Each row is its own connection, so a command that wrongly ran (MULTI, RESET,
# a subscribe) cannot leak state into the next. `timeout` keeps a row that
# wrongly entered subscribe mode from hanging the run.
row() {
  local name=$1; shift
  local got
  got=$(timeout 5 $K "$@" 2>&1); got=${got//$'\n'/,}
  chk "$*" "ERR wrong number of arguments for '$name' command" "$got"
}

echo "# arity errors vs Valkey 8, on $HOST:$RESP"
row 'append' APPEND
row 'append' APPEND a1
row 'append' APPEND a1 a2 a3
row 'auth' AUTH
row 'bitcount' BITCOUNT
row 'bitfield' BITFIELD
row 'bitfield_ro' BITFIELD_RO
row 'bitop' BITOP
row 'bitop' BITOP a1 a2
row 'bitpos' BITPOS
row 'bitpos' BITPOS a1
row 'client' CLIENT
row 'client|caching' CLIENT CACHING
row 'client|caching' CLIENT CACHING a1 a2
row 'client|getname' CLIENT GETNAME a1
row 'client|id' CLIENT ID a1
row 'client|no-evict' CLIENT NO-EVICT
row 'client|no-evict' CLIENT NO-EVICT a1 a2
row 'client|no-touch' CLIENT NO-TOUCH
row 'client|no-touch' CLIENT NO-TOUCH a1 a2
row 'client|setinfo' CLIENT SETINFO
row 'client|setinfo' CLIENT SETINFO a1
row 'client|setinfo' CLIENT SETINFO a1 a2 a3
row 'client|setname' CLIENT SETNAME
row 'client|setname' CLIENT SETNAME a1 a2
row 'client|tracking' CLIENT TRACKING
row 'client|unpause' CLIENT UNPAUSE a1
row 'cluster' CLUSTER
row 'cluster|countkeysinslot' CLUSTER COUNTKEYSINSLOT
row 'cluster|countkeysinslot' CLUSTER COUNTKEYSINSLOT a1 a2
row 'cluster|info' CLUSTER INFO a1
row 'cluster|keyslot' CLUSTER KEYSLOT
row 'cluster|keyslot' CLUSTER KEYSLOT a1 a2
row 'cluster|myid' CLUSTER MYID a1
row 'cluster|nodes' CLUSTER NODES a1
row 'cluster|shards' CLUSTER SHARDS a1
row 'cluster|slots' CLUSTER SLOTS a1
row 'command|count' COMMAND COUNT a1
row 'command|getkeys' COMMAND GETKEYS
row 'config' CONFIG
row 'config|get' CONFIG GET
row 'config|resetstat' CONFIG RESETSTAT a1
row 'config|rewrite' CONFIG REWRITE a1
row 'config|set' CONFIG SET
row 'config|set' CONFIG SET a1
row 'copy' COPY
row 'copy' COPY a1
row 'dbsize' DBSIZE a1
row 'decr' DECR
row 'decr' DECR a1 a2
row 'decrby' DECRBY
row 'decrby' DECRBY a1
row 'decrby' DECRBY a1 a2 a3
row 'del' DEL
row 'discard' DISCARD a1
row 'echo' ECHO
row 'echo' ECHO a1 a2
# EXEC is the exception: Valkey answers any refused EXEC through its abort path,
# so the arity error arrives as an EXECABORT carrying the usual text.
chk "EXEC a1" "EXECABORT Transaction discarded because of: wrong number of arguments for 'exec' command" \
  "$(timeout 5 $K EXEC a1 2>&1)"
row 'exists' EXISTS
row 'expire' EXPIRE
row 'expire' EXPIRE a1
row 'expireat' EXPIREAT
row 'expireat' EXPIREAT a1
row 'expiretime' EXPIRETIME
row 'expiretime' EXPIRETIME a1 a2
row 'get' GET
row 'get' GET a1 a2
row 'getbit' GETBIT
row 'getbit' GETBIT a1
row 'getbit' GETBIT a1 a2 a3
row 'getdel' GETDEL
row 'getdel' GETDEL a1 a2
row 'getex' GETEX
row 'getrange' GETRANGE
row 'getrange' GETRANGE a1 a2
row 'getrange' GETRANGE a1 a2 a3 a4
row 'getset' GETSET
row 'getset' GETSET a1
row 'getset' GETSET a1 a2 a3
row 'hdel' HDEL
row 'hdel' HDEL a1
row 'hexists' HEXISTS
row 'hexists' HEXISTS a1
row 'hexists' HEXISTS a1 a2 a3
row 'hget' HGET
row 'hget' HGET a1
row 'hget' HGET a1 a2 a3
row 'hgetall' HGETALL
row 'hgetall' HGETALL a1 a2
row 'hincrby' HINCRBY
row 'hincrby' HINCRBY a1 a2
row 'hincrby' HINCRBY a1 a2 a3 a4
row 'hincrbyfloat' HINCRBYFLOAT
row 'hincrbyfloat' HINCRBYFLOAT a1 a2
row 'hincrbyfloat' HINCRBYFLOAT a1 a2 a3 a4
row 'hkeys' HKEYS
row 'hkeys' HKEYS a1 a2
row 'hlen' HLEN
row 'hlen' HLEN a1 a2
row 'hmget' HMGET
row 'hmget' HMGET a1
row 'hmset' HMSET
row 'hmset' HMSET a1 a2
row 'hrandfield' HRANDFIELD
row 'hscan' HSCAN
row 'hscan' HSCAN a1
row 'hset' HSET
row 'hset' HSET a1 a2
row 'hsetnx' HSETNX
row 'hsetnx' HSETNX a1 a2
row 'hsetnx' HSETNX a1 a2 a3 a4
row 'hstrlen' HSTRLEN
row 'hstrlen' HSTRLEN a1
row 'hstrlen' HSTRLEN a1 a2 a3
row 'hvals' HVALS
row 'hvals' HVALS a1 a2
row 'incr' INCR
row 'incr' INCR a1 a2
row 'incrby' INCRBY
row 'incrby' INCRBY a1
row 'incrby' INCRBY a1 a2 a3
row 'incrbyfloat' INCRBYFLOAT
row 'incrbyfloat' INCRBYFLOAT a1
row 'incrbyfloat' INCRBYFLOAT a1 a2 a3
row 'keys' KEYS
row 'keys' KEYS a1 a2
row 'lindex' LINDEX
row 'lindex' LINDEX a1
row 'lindex' LINDEX a1 a2 a3
row 'linsert' LINSERT
row 'linsert' LINSERT a1 a2 a3
row 'linsert' LINSERT a1 a2 a3 a4 a5
row 'llen' LLEN
row 'llen' LLEN a1 a2
row 'lmove' LMOVE
row 'lmove' LMOVE a1 a2 a3
row 'lmove' LMOVE a1 a2 a3 a4 a5
row 'lpop' LPOP
row 'lpos' LPOS
row 'lpos' LPOS a1
row 'lpush' LPUSH
row 'lpush' LPUSH a1
row 'lpushx' LPUSHX
row 'lpushx' LPUSHX a1
row 'lrange' LRANGE
row 'lrange' LRANGE a1 a2
row 'lrange' LRANGE a1 a2 a3 a4
row 'lrem' LREM
row 'lrem' LREM a1 a2
row 'lrem' LREM a1 a2 a3 a4
row 'lset' LSET
row 'lset' LSET a1 a2
row 'lset' LSET a1 a2 a3 a4
row 'ltrim' LTRIM
row 'ltrim' LTRIM a1 a2
row 'ltrim' LTRIM a1 a2 a3 a4
row 'memory' MEMORY
row 'memory|doctor' MEMORY DOCTOR a1
row 'memory|usage' MEMORY USAGE
row 'mget' MGET
row 'mset' MSET
row 'mset' MSET a1
row 'msetnx' MSETNX
row 'msetnx' MSETNX a1
row 'multi' MULTI a1
row 'object' OBJECT
row 'object|encoding' OBJECT ENCODING
row 'object|encoding' OBJECT ENCODING a1 a2
row 'object|freq' OBJECT FREQ
row 'object|freq' OBJECT FREQ a1 a2
row 'object|help' OBJECT HELP a1
row 'object|idletime' OBJECT IDLETIME
row 'object|idletime' OBJECT IDLETIME a1 a2
row 'object|refcount' OBJECT REFCOUNT
row 'object|refcount' OBJECT REFCOUNT a1 a2
row 'persist' PERSIST
row 'persist' PERSIST a1 a2
row 'pexpire' PEXPIRE
row 'pexpire' PEXPIRE a1
row 'pexpireat' PEXPIREAT
row 'pexpireat' PEXPIREAT a1
row 'pexpiretime' PEXPIRETIME
row 'pexpiretime' PEXPIRETIME a1 a2
row 'psetex' PSETEX
row 'psetex' PSETEX a1 a2
row 'psetex' PSETEX a1 a2 a3 a4
row 'psubscribe' PSUBSCRIBE
row 'pttl' PTTL
row 'pttl' PTTL a1 a2
row 'publish' PUBLISH
row 'publish' PUBLISH a1
row 'publish' PUBLISH a1 a2 a3
row 'pubsub' PUBSUB
row 'pubsub|help' PUBSUB HELP a1
row 'pubsub|numpat' PUBSUB NUMPAT a1
row 'randomkey' RANDOMKEY a1
row 'rename' RENAME
row 'rename' RENAME a1
row 'rename' RENAME a1 a2 a3
row 'renamenx' RENAMENX
row 'renamenx' RENAMENX a1
row 'renamenx' RENAMENX a1 a2 a3
row 'reset' RESET a1
row 'rpop' RPOP
row 'rpoplpush' RPOPLPUSH
row 'rpoplpush' RPOPLPUSH a1
row 'rpoplpush' RPOPLPUSH a1 a2 a3
row 'rpush' RPUSH
row 'rpush' RPUSH a1
row 'rpushx' RPUSHX
row 'rpushx' RPUSHX a1
row 'sadd' SADD
row 'sadd' SADD a1
row 'scan' SCAN
row 'scard' SCARD
row 'scard' SCARD a1 a2
row 'sdiff' SDIFF
row 'sdiffstore' SDIFFSTORE
row 'sdiffstore' SDIFFSTORE a1
row 'select' SELECT
row 'select' SELECT a1 a2
row 'set' SET
row 'set' SET a1
row 'setbit' SETBIT
row 'setbit' SETBIT a1 a2
row 'setbit' SETBIT a1 a2 a3 a4
row 'setex' SETEX
row 'setex' SETEX a1 a2
row 'setex' SETEX a1 a2 a3 a4
row 'setnx' SETNX
row 'setnx' SETNX a1
row 'setnx' SETNX a1 a2 a3
row 'setrange' SETRANGE
row 'setrange' SETRANGE a1 a2
row 'setrange' SETRANGE a1 a2 a3 a4
row 'sinter' SINTER
row 'sintercard' SINTERCARD
row 'sintercard' SINTERCARD a1
row 'sinterstore' SINTERSTORE
row 'sinterstore' SINTERSTORE a1
row 'sismember' SISMEMBER
row 'sismember' SISMEMBER a1
row 'sismember' SISMEMBER a1 a2 a3
row 'smembers' SMEMBERS
row 'smembers' SMEMBERS a1 a2
row 'smismember' SMISMEMBER
row 'smismember' SMISMEMBER a1
row 'smove' SMOVE
row 'smove' SMOVE a1 a2
row 'smove' SMOVE a1 a2 a3 a4
row 'spop' SPOP
row 'spublish' SPUBLISH
row 'spublish' SPUBLISH a1
row 'spublish' SPUBLISH a1 a2 a3
row 'srandmember' SRANDMEMBER
row 'srem' SREM
row 'srem' SREM a1
row 'sscan' SSCAN
row 'sscan' SSCAN a1
row 'ssubscribe' SSUBSCRIBE
row 'strlen' STRLEN
row 'strlen' STRLEN a1 a2
row 'subscribe' SUBSCRIBE
row 'sunion' SUNION
row 'sunionstore' SUNIONSTORE
row 'sunionstore' SUNIONSTORE a1
row 'time' TIME a1
row 'touch' TOUCH
row 'ttl' TTL
row 'ttl' TTL a1 a2
row 'type' TYPE
row 'type' TYPE a1 a2
row 'unlink' UNLINK
row 'unwatch' UNWATCH a1
row 'watch' WATCH
row 'zadd' ZADD
row 'zadd' ZADD a1 a2
row 'zcard' ZCARD
row 'zcard' ZCARD a1 a2
row 'zcount' ZCOUNT
row 'zcount' ZCOUNT a1 a2
row 'zcount' ZCOUNT a1 a2 a3 a4
row 'zdiff' ZDIFF
row 'zdiff' ZDIFF a1
row 'zdiffstore' ZDIFFSTORE
row 'zdiffstore' ZDIFFSTORE a1 a2
row 'zincrby' ZINCRBY
row 'zincrby' ZINCRBY a1 a2
row 'zincrby' ZINCRBY a1 a2 a3 a4
row 'zinter' ZINTER
row 'zinter' ZINTER a1
row 'zinterstore' ZINTERSTORE
row 'zinterstore' ZINTERSTORE a1 a2
row 'zlexcount' ZLEXCOUNT
row 'zlexcount' ZLEXCOUNT a1 a2
row 'zlexcount' ZLEXCOUNT a1 a2 a3 a4
row 'zmpop' ZMPOP
row 'zmpop' ZMPOP a1 a2
row 'zmscore' ZMSCORE
row 'zmscore' ZMSCORE a1
row 'zpopmax' ZPOPMAX
row 'zpopmin' ZPOPMIN
row 'zrandmember' ZRANDMEMBER
row 'zrange' ZRANGE
row 'zrange' ZRANGE a1 a2
row 'zrangebylex' ZRANGEBYLEX
row 'zrangebylex' ZRANGEBYLEX a1 a2
row 'zrangebyscore' ZRANGEBYSCORE
row 'zrangebyscore' ZRANGEBYSCORE a1 a2
row 'zrangestore' ZRANGESTORE
row 'zrangestore' ZRANGESTORE a1 a2 a3
row 'zrank' ZRANK
row 'zrank' ZRANK a1
row 'zrem' ZREM
row 'zrem' ZREM a1
row 'zrevrange' ZREVRANGE
row 'zrevrange' ZREVRANGE a1 a2
row 'zrevrangebylex' ZREVRANGEBYLEX
row 'zrevrangebylex' ZREVRANGEBYLEX a1 a2
row 'zrevrangebyscore' ZREVRANGEBYSCORE
row 'zrevrangebyscore' ZREVRANGEBYSCORE a1 a2
row 'zrevrank' ZREVRANK
row 'zrevrank' ZREVRANK a1
row 'zscan' ZSCAN
row 'zscan' ZSCAN a1
row 'zscore' ZSCORE
row 'zscore' ZSCORE a1
row 'zscore' ZSCORE a1 a2 a3
row 'zunion' ZUNION
row 'zunion' ZUNION a1
row 'zunionstore' ZUNIONSTORE
row 'zunionstore' ZUNIONSTORE a1 a2

echo "  -- the name is the registered one, whatever case it was typed in"
row 'get' get
row 'get' gEt
row 'hset' hSet k f
row 'client|setname' client setname
row 'pubsub|numpat' PubSub NumPat x
# PING's arity is -1 in COMMAND INFO; the cap of one argument is its own check.
row 'ping' PING a1 a2

echo "  -- inside MULTI: refused at once, and EXEC then discards the transaction"
$K DEL arity_k >/dev/null
got=$(printf 'MULTI\nGET\nSET arity_k v\nEXEC\n' | timeout 5 $K 2>&1 | grep -v '^$' | paste -sd, -)
chk "MULTI / GET / SET / EXEC" \
  "OK,ERR wrong number of arguments for 'get' command,QUEUED,EXECABORT Transaction discarded because of previous errors." \
  "$got"
chk "the queued SET did not run" "0" "$($K EXISTS arity_k)"
got=$(printf 'MULTI\nSET arity_k v\nEXEC\n' | timeout 5 $K 2>&1 | grep -v '^$' | paste -sd, -)
chk "a clean transaction after it still runs" "OK,QUEUED,OK" "$got"
chk "and its SET landed" "1" "$($K EXISTS arity_k)"
$K DEL arity_k >/dev/null

echo
echo "# result: $pass passed, $fail failed"
[ "$fail" -eq 0 ]
