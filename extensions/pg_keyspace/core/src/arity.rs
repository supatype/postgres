//! Valkey's arity check, made once before a command runs.
//!
//! Valkey refuses a command with the wrong number of arguments before it looks
//! at anything else (authentication, subscribe mode, the arguments themselves),
//! and always in the same words:
//!
//! ```text
//! ERR wrong number of arguments for '<name>' command
//! ```
//!
//! where `<name>` is the registered name in lower case, whatever case it was
//! typed in, and `container|subcommand` for a subcommand. This crate used to
//! check arity in each handler, so the text drifted (most handlers dropped the
//! trailing " command" and some named no command at all), some commands were
//! answered OK when Valkey refuses them, and a bare `INCR` or `HLEN` indexed past the end of its
//! arguments and took the whole server down.
//!
//! [`ARITY`] is Valkey 8's own `COMMAND INFO` arity for every command and
//! subcommand this server implements, and `bench/run_arity_parity.sh` sends
//! each one short (and, where the arity is exact, one long) and compares the
//! reply with Valkey's. BF.* and CF.* are absent because Valkey has no
//! probabilistic types; their handlers check arity against Redis 8 themselves.

/// `(NAME, arity)`, sorted by name for a binary search. A positive arity is the
/// exact argument count, the command name included; a negative one is the
/// minimum. Subcommands are `CONTAINER|SUB`.
const ARITY: &[(&str, i32)] = &[
    ("APPEND", 3),
    ("AUTH", -2),
    ("BITCOUNT", -2),
    ("BITFIELD", -2),
    ("BITFIELD_RO", -2),
    ("BITOP", -4),
    ("BITPOS", -3),
    ("CLIENT", -2),
    ("CLIENT|CACHING", 3),
    ("CLIENT|GETNAME", 2),
    ("CLIENT|ID", 2),
    ("CLIENT|NO-EVICT", 3),
    ("CLIENT|NO-TOUCH", 3),
    ("CLIENT|SETINFO", 4),
    ("CLIENT|SETNAME", 3),
    ("CLIENT|TRACKING", -3),
    ("CLIENT|UNPAUSE", 2),
    ("CLUSTER", -2),
    ("CLUSTER|COUNTKEYSINSLOT", 3),
    ("CLUSTER|INFO", 2),
    ("CLUSTER|KEYSLOT", 3),
    ("CLUSTER|MYID", 2),
    ("CLUSTER|NODES", 2),
    ("CLUSTER|SHARDS", 2),
    ("CLUSTER|SLOTS", 2),
    ("COMMAND", -1),
    ("COMMAND|COUNT", 2),
    ("COMMAND|DOCS", -2),
    ("COMMAND|GETKEYS", -3),
    ("COMMAND|INFO", -2),
    ("COMMAND|LIST", -2),
    ("CONFIG", -2),
    ("CONFIG|GET", -3),
    ("CONFIG|RESETSTAT", 2),
    ("CONFIG|REWRITE", 2),
    ("CONFIG|SET", -4),
    ("COPY", -3),
    ("DBSIZE", 1),
    ("DEBUG", -2),
    ("DECR", 2),
    ("DECRBY", 3),
    ("DEL", -2),
    ("DISCARD", 1),
    ("ECHO", 2),
    ("EXEC", 1),
    ("EXISTS", -2),
    ("EXPIRE", -3),
    ("EXPIREAT", -3),
    ("EXPIRETIME", 2),
    ("FLUSHALL", -1),
    ("FLUSHDB", -1),
    ("GET", 2),
    ("GETBIT", 3),
    ("GETDEL", 2),
    ("GETEX", -2),
    ("GETRANGE", 4),
    ("GETSET", 3),
    ("HDEL", -3),
    ("HELLO", -1),
    ("HEXISTS", 3),
    ("HGET", 3),
    ("HGETALL", 2),
    ("HINCRBY", 4),
    ("HINCRBYFLOAT", 4),
    ("HKEYS", 2),
    ("HLEN", 2),
    ("HMGET", -3),
    ("HMSET", -4),
    ("HRANDFIELD", -2),
    ("HSCAN", -3),
    ("HSET", -4),
    ("HSETNX", 4),
    ("HSTRLEN", 3),
    ("HVALS", 2),
    ("INCR", 2),
    ("INCRBY", 3),
    ("INCRBYFLOAT", 3),
    ("INFO", -1),
    ("KEYS", 2),
    ("LINDEX", 3),
    ("LINSERT", 5),
    ("LLEN", 2),
    ("LMOVE", 5),
    ("LPOP", -2),
    ("LPOS", -3),
    ("LPUSH", -3),
    ("LPUSHX", -3),
    ("LRANGE", 4),
    ("LREM", 4),
    ("LSET", 4),
    ("LTRIM", 4),
    ("MEMORY", -2),
    ("MEMORY|DOCTOR", 2),
    ("MEMORY|USAGE", -3),
    ("MGET", -2),
    ("MSET", -3),
    ("MSETNX", -3),
    ("MULTI", 1),
    ("OBJECT", -2),
    ("OBJECT|ENCODING", 3),
    ("OBJECT|FREQ", 3),
    ("OBJECT|HELP", 2),
    ("OBJECT|IDLETIME", 3),
    ("OBJECT|REFCOUNT", 3),
    ("PERSIST", 2),
    ("PEXPIRE", -3),
    ("PEXPIREAT", -3),
    ("PEXPIRETIME", 2),
    ("PING", -1),
    ("PSETEX", 4),
    ("PSUBSCRIBE", -2),
    ("PTTL", 2),
    ("PUBLISH", 3),
    ("PUBSUB", -2),
    ("PUBSUB|CHANNELS", -2),
    ("PUBSUB|HELP", 2),
    ("PUBSUB|NUMPAT", 2),
    ("PUBSUB|NUMSUB", -2),
    ("PUBSUB|SHARDCHANNELS", -2),
    ("PUBSUB|SHARDNUMSUB", -2),
    ("PUNSUBSCRIBE", -1),
    ("QUIT", -1),
    ("RANDOMKEY", 1),
    ("RENAME", 3),
    ("RENAMENX", 3),
    ("RESET", 1),
    ("RPOP", -2),
    ("RPOPLPUSH", 3),
    ("RPUSH", -3),
    ("RPUSHX", -3),
    ("SADD", -3),
    ("SCAN", -2),
    ("SCARD", 2),
    ("SDIFF", -2),
    ("SDIFFSTORE", -3),
    ("SELECT", 2),
    ("SET", -3),
    ("SETBIT", 4),
    ("SETEX", 4),
    ("SETNX", 3),
    ("SETRANGE", 4),
    ("SINTER", -2),
    ("SINTERCARD", -3),
    ("SINTERSTORE", -3),
    ("SISMEMBER", 3),
    ("SMEMBERS", 2),
    ("SMISMEMBER", -3),
    ("SMOVE", 4),
    ("SPOP", -2),
    ("SPUBLISH", 3),
    ("SRANDMEMBER", -2),
    ("SREM", -3),
    ("SSCAN", -3),
    ("SSUBSCRIBE", -2),
    ("STRLEN", 2),
    ("SUBSCRIBE", -2),
    ("SUNION", -2),
    ("SUNIONSTORE", -3),
    ("SUNSUBSCRIBE", -1),
    ("TIME", 1),
    ("TOUCH", -2),
    ("TTL", 2),
    ("TYPE", 2),
    ("UNLINK", -2),
    ("UNSUBSCRIBE", -1),
    ("UNWATCH", 1),
    ("WATCH", -2),
    ("ZADD", -4),
    ("ZCARD", 2),
    ("ZCOUNT", 4),
    ("ZDIFF", -3),
    ("ZDIFFSTORE", -4),
    ("ZINCRBY", 4),
    ("ZINTER", -3),
    ("ZINTERSTORE", -4),
    ("ZLEXCOUNT", 4),
    ("ZMPOP", -4),
    ("ZMSCORE", -3),
    ("ZPOPMAX", -2),
    ("ZPOPMIN", -2),
    ("ZRANDMEMBER", -2),
    ("ZRANGE", -4),
    ("ZRANGEBYLEX", -4),
    ("ZRANGEBYSCORE", -4),
    ("ZRANGESTORE", -5),
    ("ZRANK", -3),
    ("ZREM", -3),
    ("ZREVRANGE", -4),
    ("ZREVRANGEBYLEX", -4),
    ("ZREVRANGEBYSCORE", -4),
    ("ZREVRANK", -3),
    ("ZSCAN", -3),
    ("ZSCORE", 3),
    ("ZUNION", -3),
    ("ZUNIONSTORE", -4),
];

/// Commands whose second argument names a subcommand with an arity of its own.
const CONTAINERS: &[&[u8]] =
    &[b"CLIENT", b"CLUSTER", b"COMMAND", b"CONFIG", b"MEMORY", b"OBJECT", b"PUBSUB"];

fn lookup(name: &[u8]) -> Option<i32> {
    ARITY
        .binary_search_by(|(n, _)| n.as_bytes().cmp(name))
        .ok()
        .map(|i| ARITY[i].1)
}

fn fits(arity: i32, argc: usize) -> bool {
    let n = arity.unsigned_abs() as usize;
    if arity >= 0 {
        argc == n
    } else {
        argc >= n
    }
}

/// Valkey's reply to a command with the wrong number of arguments. `name` is
/// lowercased here, so callers can pass the name in whatever case they hold it.
pub fn message(name: &[u8]) -> String {
    format!(
        "ERR wrong number of arguments for '{}' command",
        String::from_utf8_lossy(name).to_ascii_lowercase()
    )
}

/// The arity error for `args`, if Valkey would refuse them for their count.
/// `cmd` is `args[0]` in upper case. A command or subcommand this server does
/// not implement gets `None`: refusing it is its handler's business, in its own
/// words, as Valkey's unknown-command and unknown-subcommand replies are.
pub fn check(cmd: &[u8], args: &[Vec<u8>]) -> Option<String> {
    let arity = lookup(cmd)?;
    if args.len() >= 2 && CONTAINERS.contains(&cmd) {
        let mut full = cmd.to_vec();
        full.push(b'|');
        full.extend(args[1].iter().map(u8::to_ascii_uppercase));
        let sub = lookup(&full)?;
        return (!fits(sub, args.len())).then(|| message(&full));
    }
    (!fits(arity, args.len())).then(|| message(cmd))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn argv(words: &[&str]) -> Vec<Vec<u8>> {
        words.iter().map(|w| w.as_bytes().to_vec()).collect()
    }

    fn run(words: &[&str]) -> Option<String> {
        let args = argv(words);
        check(&args[0].to_ascii_uppercase(), &args)
    }

    #[test]
    fn table_is_sorted_and_unique_so_the_binary_search_finds_everything() {
        for w in ARITY.windows(2) {
            assert!(w[0].0 < w[1].0, "{} must sort before {}", w[0].0, w[1].0);
        }
        for (name, arity) in ARITY {
            assert_eq!(lookup(name.as_bytes()), Some(*arity), "{name}");
            assert_eq!(name.to_ascii_uppercase(), *name, "{name} must be upper case");
        }
    }

    #[test]
    fn every_subcommand_belongs_to_a_listed_container() {
        for (name, _) in ARITY {
            if let Some((container, _)) = name.split_once('|') {
                assert!(CONTAINERS.contains(&container.as_bytes()), "{name}");
                assert!(lookup(container.as_bytes()).is_some(), "{name}");
            }
        }
    }

    #[test]
    fn too_few_arguments_name_the_command_in_lower_case() {
        let get = Some("ERR wrong number of arguments for 'get' command".to_string());
        assert_eq!(run(&["GET"]), get);
        assert_eq!(run(&["gEt"]), get);
        assert_eq!(
            run(&["hset", "k", "f"]),
            Some("ERR wrong number of arguments for 'hset' command".to_string())
        );
    }

    #[test]
    fn an_exact_arity_refuses_one_too_many_and_a_minimum_does_not() {
        assert!(run(&["GET", "k", "x"]).is_some());
        assert!(run(&["DBSIZE", "x"]).is_some());
        assert_eq!(run(&["GET", "k"]), None);
        assert_eq!(run(&["DEL", "a", "b", "c", "d"]), None);
        assert_eq!(run(&["PING"]), None);
    }

    #[test]
    fn a_subcommand_is_named_container_bar_sub() {
        assert_eq!(
            run(&["client", "SetName"]),
            Some("ERR wrong number of arguments for 'client|setname' command".to_string())
        );
        assert_eq!(run(&["CLIENT", "SETNAME", "n"]), None);
        assert_eq!(
            run(&["CLIENT"]),
            Some("ERR wrong number of arguments for 'client' command".to_string())
        );
    }

    #[test]
    fn what_this_server_does_not_implement_is_left_to_its_handler() {
        assert_eq!(run(&["NOSUCHCOMMAND"]), None);
        assert_eq!(run(&["CLIENT", "NOSUCHSUB"]), None);
        assert_eq!(run(&["BF.ADD"]), None);
    }
}
