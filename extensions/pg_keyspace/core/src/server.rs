//! One slot worker: a readiness-driven event loop over non-blocking sockets,
//! parsing RESP and dispatching against its own shared-memory partition. No
//! transaction is ever opened on this path; a command is a shmem read/write
//! plus, for logged tiers, a handoff to the commit batcher. This is the
//! hot path the latency benchmarks measure.
//!
//! Readiness comes from `mio` (epoll on Linux, kqueue on macOS); we still own
//! the sockets and shared memory ourselves via libc and only register the raw
//! fds with mio through `SourceFd`. mio is edge-triggered, which is safe here
//! because every fd is drained to `EAGAIN` on each wake (reads, writes, accept,
//! and the cross-worker wake fd).

use crate::aggr;
use crate::batcher::{Batcher, Tier};
use crate::crc16;
use crate::pubsub;
use crate::resp::{self, Parse};
use crate::ring;
use crate::store::{now_micros, Lookup, Store, KIND_HASH, KIND_LIST, KIND_SET, KIND_ZSET};
use mio::unix::SourceFd;
use mio::{Events, Interest, Poll, Registry, Token};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{Read, Write};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

const POLL_MAX: usize = 1024;
const READ_CHUNK: usize = 64 * 1024;

/// A staged write: (key, value, expires_at_micros, kind). `kind` is the value's
/// type tag so aggregates persist and recover as the right type.
pub type PendingWrite = (Vec<u8>, Vec<u8>, i64, u8);

/// Sentinel `expires_at` marking a delete (tombstone) carried through the ring,
/// so the persistence worker removes the key from the backing table instead of
/// upserting it — otherwise a deleted key would resurrect on crash recovery.
pub const DELETE_TOMBSTONE: i64 = -1;

/// This worker's place in the slot->worker map, plus the address of every peer,
/// so a key it does not own can be answered with `MOVED <slot> <host>:<port>`,
/// and the whole map can be published through `CLUSTER SLOTS`/`SHARDS`/`NODES`.
struct Routing {
    index: usize,
    nworkers: usize,
    /// `host:port` of each slot worker, indexed by worker number.
    endpoints: Vec<String>,
    /// A stable 40-hex-char node id per worker. Cluster clients key their cached
    /// topology on these, so they are derived from the endpoint rather than
    /// randomised — a worker keeps its id across restarts.
    node_ids: Vec<String>,
}

impl Routing {
    fn host_port(&self, w: usize) -> (&str, u16) {
        let ep = self.endpoints.get(w).map(String::as_str).unwrap_or("");
        match ep.rsplit_once(':') {
            Some((h, p)) => (h, p.parse().unwrap_or(0)),
            None => (ep, 0),
        }
    }

    fn node_id(&self, w: usize) -> &str {
        self.node_ids.get(w).map(String::as_str).unwrap_or("")
    }
}

/// A deterministic 40-hex-char cluster node id for `endpoint`.
fn node_id_for(endpoint: &str) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(endpoint.as_bytes());
    digest.iter().take(20).map(|b| format!("{b:02x}")).collect()
}


// ---- COMMAND introspection -----------------------------------------------
// Cluster clients that do not ship a static command table (redis-py, most
// notably) call `COMMAND` at connect and extract each command's key positions
// from the reply. Without it they can route nothing — the slot map alone is not
// enough. Positions here must agree with `key_indices`, which is what the server
// actually treats as keys.

/// `(first_key, last_key, step)` for one command shape.
struct CmdSpec {
    name: &'static str,
    write: bool,
    first: i64,
    last: i64,
    step: i64,
    /// Keys are located by a `numkeys` argument, so a client must ask
    /// `COMMAND GETKEYS` rather than read fixed positions.
    movable: bool,
}

/// Key at argument 1 — the overwhelming majority of commands.
const CMD_KEY1_READ: &[&str] = &[
    "GET", "TTL", "PTTL", "TYPE", "STRLEN", "GETRANGE", "EXPIRETIME", "PEXPIRETIME",
    "HGET", "HMGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS", "HSTRLEN", "HRANDFIELD",
    "LLEN", "LINDEX", "LRANGE", "LPOS",
    "ZSCORE", "ZMSCORE", "ZCARD", "ZRANK", "ZREVRANK", "ZRANGE", "ZREVRANGE",
    "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZCOUNT", "ZRANDMEMBER", "ZRANGEBYLEX",
    "ZREVRANGEBYLEX", "ZLEXCOUNT",
    "SCARD", "SISMEMBER", "SMISMEMBER", "SMEMBERS", "SRANDMEMBER",
    "HSCAN", "SSCAN", "ZSCAN",
];
const CMD_KEY1_WRITE: &[&str] = &[
    "SET", "SETNX", "GETSET", "INCR", "DECR", "INCRBY", "DECRBY", "EXPIRE", "PEXPIRE",
    "EXPIREAT", "PEXPIREAT", "PERSIST", "APPEND", "GETDEL", "SETEX", "PSETEX", "GETEX",
    "SETRANGE", "INCRBYFLOAT",
    "HSET", "HMSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
    "LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LTRIM", "LINSERT", "LREM",
    "ZADD", "ZREM", "ZINCRBY", "ZPOPMIN", "ZPOPMAX",
    "SADD", "SREM", "SPOP",
];
/// Every argument is a key.
const CMD_ALLKEYS_READ: &[&str] = &["EXISTS", "MGET", "TOUCH", "SUNION", "SINTER", "SDIFF", "WATCH"];
const CMD_ALLKEYS_WRITE: &[&str] =
    &["DEL", "UNLINK", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE"];
/// Source and destination key.
const CMD_TWOKEY_WRITE: &[&str] =
    &["RENAME", "RENAMENX", "COPY", "LMOVE", "RPOPLPUSH", "SMOVE", "ZRANGESTORE"];
/// `numkeys`-style: key count is an argument, so positions are not fixed.
const CMD_MOVABLE_READ: &[&str] = &["ZUNION", "ZINTER", "ZDIFF", "SINTERCARD"];
const CMD_MOVABLE_WRITE: &[&str] = &["ZUNIONSTORE", "ZINTERSTORE", "ZDIFFSTORE", "ZMPOP"];
/// Commands that take no keys; a cluster client may send these to any worker.
const CMD_KEYLESS: &[&str] = &[
    "PING", "ECHO", "INFO", "DBSIZE", "FLUSHALL", "FLUSHDB", "SCAN", "KEYS", "COMMAND",
    "CLUSTER", "HELLO", "AUTH", "CLIENT", "CONFIG", "SELECT", "RESET", "MEMORY", "DEBUG",
    "TIME", "MULTI", "EXEC", "DISCARD", "UNWATCH", "SUBSCRIBE", "UNSUBSCRIBE", "PSUBSCRIBE",
    "PUNSUBSCRIBE", "PUBLISH", "PUBSUB", "QUIT",
];

fn command_specs() -> Vec<CmdSpec> {
    let mut v = Vec::with_capacity(160);
    let mut add = |names: &[&'static str], write: bool, first: i64, last: i64, step: i64, movable: bool| {
        for name in names {
            v.push(CmdSpec { name, write, first, last, step, movable });
        }
    };
    add(CMD_KEY1_READ, false, 1, 1, 1, false);
    add(CMD_KEY1_WRITE, true, 1, 1, 1, false);
    add(CMD_ALLKEYS_READ, false, 1, -1, 1, false);
    add(CMD_ALLKEYS_WRITE, true, 1, -1, 1, false);
    add(&["MSET", "MSETNX"], true, 1, -1, 2, false);
    add(CMD_TWOKEY_WRITE, true, 1, 2, 1, false);
    add(CMD_MOVABLE_READ, false, 0, 0, 0, true);
    add(CMD_MOVABLE_WRITE, true, 0, 0, 0, true);
    add(&["OBJECT"], false, 2, 2, 1, false);
    add(CMD_KEYLESS, false, 0, 0, 0, false);
    v
}

/// One `COMMAND` reply entry: `[name, arity, flags, first, last, step]`.
fn write_command_entry(out: &mut Vec<u8>, c: &CmdSpec) {
    resp::array_header(out, 6);
    resp::bulk(out, c.name.to_ascii_lowercase().as_bytes());
    // Variadic minimum: every listed command takes at least its own name, and
    // keyed ones at least one key. Clients use this only as a lower bound.
    resp::integer(out, if c.first > 0 { -2 } else { -1 });
    let mut flags: Vec<&str> = vec![if c.write { "write" } else { "readonly" }];
    if c.movable {
        flags.push("movablekeys");
    }
    resp::array_header(out, flags.len());
    for f in flags {
        resp::simple(out, f);
    }
    resp::integer(out, c.first);
    resp::integer(out, c.last);
    resp::integer(out, c.step);
}

/// Enqueue one record into the ring sharded by key slot (same shard function as
/// the write path, so a key's writes and deletes always reach the same worker).
///
/// Backpressure: if the ring is full, wait (bounded) for the persistence worker
/// to drain rather than giving up immediately. Under sustained overload this
/// throttles the RESP write path to the drain rate. Past the deadline the push
/// fails and is counted (`ring_stats.dropped`); in a sync-ack tier the caller
/// turns that into a client-visible error rather than a `+OK`, so a failure
/// here is never silent data loss.
///
/// The wait is still inline on the event loop, so it stalls every connection on
/// this worker for up to `PUSH_DEADLINE`. That is why the deadline is short and
/// tunable; parking the connection and resuming it on drain, the way
/// `resolve_acks` defers replies, is the proper fix and is not done here.
pub const PUSH_DEADLINE: Duration = Duration::from_secs(2);

/// How many bytes of held reply a single connection may accumulate while its
/// durable writes are still committing.
///
/// A sync-ack tier holds a connection's replies until the records behind them
/// commit, so a pipelining client can queue replies faster than the persist
/// worker retires them. This caps that queue: past it the connection stops
/// being read until an ack resolves, which is the same backpressure that used
/// to apply at the very first outstanding write. At roughly five bytes per
/// `+OK` this is a deep pipeline, and for reads — where the bytes actually
/// are — it bounds memory rather than command count, which is the useful
/// thing to bound.
pub const MAX_HELD_REPLY_BYTES: usize = 1 << 20;

/// Reply for a write the store could not hold. Redis uses this exact text when
/// memory pressure prevents a write, and clients special-case it, so reusing it
/// means an existing client library handles the condition it already knows.
/// `resp::DEFAULT_MAX_BULK_LEN` as an i32, for the GUC definition.
pub const DEFAULT_MAX_VALUE_BYTES: i32 = 512 * 1024 * 1024;

pub const OOM_ERR: &str = "OOM command not allowed when used memory > 'maxmemory'.";

/// Report a store write that did not happen, instead of replying success.
///
/// `Store::set`/`set_typed` return false when the value cannot be allocated:
/// larger than the arena, or the arena is full of entries that cannot be
/// evicted (a referenced value awaiting persistence, for instance). Every call
/// site used to discard that, so the client was told `+OK` for a value the very
/// next `GET` would not return.
#[must_use]
fn wrote(out: &mut Vec<u8>, ok: bool) -> bool {
    if !ok {
        resp::error(out, OOM_ERR);
    }
    ok
}

/// Values at or below this are copied into the ring; larger ones are staged by
/// reference (see [`ring::KIND_REF`]).
///
/// 8 KiB is the store's largest slab class, so this is exactly the line between
/// a value that fits a size class and one that takes the OVERSIZED path. Below
/// it the copy is a few microseconds and buys complete independence from
/// eviction, which is worth keeping for the overwhelming majority of traffic.
/// Above it the copy is both expensive and the thing that made ring capacity a
/// ceiling on value size, so those go by reference.
pub const INLINE_MAX: usize = 8 * 1024;

/// Which ring a key's records go to. A key always maps to the same shard, so
/// its writes and deletes stay ordered and cannot conflict on `ON CONFLICT`.
#[inline]
fn shard_of(n: usize, key: &[u8]) -> usize {
    if n <= 1 {
        0
    } else {
        crc16::key_slot(key) as usize % n
    }
}

fn shard_push(
    producers: &[ring::Producer],
    key: &[u8],
    val: &[u8],
    exp: i64,
    kind: u8,
) -> Option<(usize, u64)> {
    let n = producers.len();
    if n == 0 {
        return None;
    }
    let shard = shard_of(n, key);
    let p = &producers[shard];
    if let Some(seq) = p.push(key, val, exp, kind) {
        return Some((shard, seq));
    }
    let deadline = Instant::now() + PUSH_DEADLINE;
    loop {
        std::thread::sleep(Duration::from_micros(50));
        if let Some(seq) = p.push(key, val, exp, kind) {
            return Some((shard, seq));
        }
        if Instant::now() >= deadline {
            // Persistence is wedged or the value cannot be encoded. Count it
            // and let the caller decide: a sync-ack tier answers with an error.
            p.note_drop();
            return None;
        }
    }
}

// ---- security: RESP AUTH -> role, keyspace ACL, forced tenant scoping ----

/// A RESP credential: maps an AUTH username to a Postgres role + tenant.
///
/// `secret` is either a hashed verifier `sha256$<salt_hex>$<hash_hex>` (produced
/// by `supacache.set_credential`, the recommended path — the plaintext is never
/// stored) or, for local/legacy use, a bare plaintext string. `verify` accepts
/// both and always compares in constant time.
#[derive(Clone)]
pub struct Cred {
    pub secret: String,
    pub role: String,
    pub tenant: String,
}

/// Constant-time byte comparison — no early exit on the first mismatch, so it
/// leaks neither which byte differs nor (for equal lengths) how many match.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
        .collect()
}

impl Cred {
    /// True if `presented` matches this credential's secret.
    pub fn verify(&self, presented: &[u8]) -> bool {
        if let Some(rest) = self.secret.strip_prefix("sha256$") {
            // sha256$<salt_hex>$<hash_hex>
            let mut parts = rest.splitn(2, '$');
            let (salt_hex, hash_hex) = match (parts.next(), parts.next()) {
                (Some(s), Some(h)) => (s, h),
                _ => return false,
            };
            let (salt, expected) = match (hex_decode(salt_hex), hex_decode(hash_hex)) {
                (Some(s), Some(h)) => (s, h),
                _ => return false,
            };
            use sha2::{Digest, Sha256};
            let mut hasher = Sha256::new();
            hasher.update(&salt);
            hasher.update(presented);
            let got = hasher.finalize();
            ct_eq(got.as_slice(), &expected)
        } else {
            // legacy plaintext (local/dev)
            ct_eq(self.secret.as_bytes(), presented)
        }
    }
}

/// One keyspace ACL rule: a role may read/write keys under `prefix`.
#[derive(Clone)]
pub struct AclRule {
    pub prefix: Vec<u8>,
    pub can_read: bool,
    pub can_write: bool,
}

/// The full auth configuration, loaded from SQL by the extension and handed to
/// the worker. When present, RESP AUTH is required for keyed commands; when
/// absent, the worker runs in local/no-auth mode (matches local dev).
pub struct AuthConfig {
    pub creds: HashMap<String, Cred>,
    pub acl: HashMap<String, Vec<AclRule>>,
    pub exempt: HashSet<String>, // roles that bypass ACL + scoping
}

/// A live config swap applied by `run_with` (SIGHUP hot-reload). Each field is
/// `Some(new)` to change it or `None` to leave it as-is; the inner `Option` is
/// the value itself (e.g. `Some(None)` clears auth -> no-auth mode).
#[derive(Default)]
pub struct Reload {
    pub auth: Option<Option<AuthConfig>>,
    pub tls: Option<Option<Arc<rustls::ServerConfig>>>,
}

/// Per-iteration control returned by the `run_with` tick closure.
pub enum Tick {
    /// Keep running.
    Continue,
    /// Stop the event loop (e.g. SIGTERM).
    Stop,
    /// Hot-reload credentials and/or the TLS cert on SIGHUP. New TLS only affects
    /// connections accepted after the swap; existing sessions keep their session.
    Reload(Reload),
}

enum Deny {
    NoAuth,
    Perm,
    Nil,
}

/// Per-connection CLIENT TRACKING state (server-assisted client-side caching).
/// `on` is the master switch; the rest select the flavour negotiated by
/// `CLIENT TRACKING ON [REDIRECT id] [PREFIX p ...] [BCAST] [OPTIN|OPTOUT]`.
#[derive(Default)]
struct Tracking {
    /// Tracking enabled on this connection.
    on: bool,
    /// Broadcasting mode: invalidations are driven by `prefixes` at write time
    /// rather than by recording each read. With no prefixes, the whole keyspace.
    bcast: bool,
    /// Scoped key prefixes to broadcast on (BCAST). Empty => match every key.
    prefixes: Vec<Vec<u8>>,
    /// OPTIN: cache only reads that follow `CLIENT CACHING YES`.
    optin: bool,
    /// OPTOUT: cache every read except those following `CLIENT CACHING NO`.
    optout: bool,
    /// One-shot `CLIENT CACHING` flag consumed by the next command: `Some(true)`
    /// opts the next read in, `Some(false)` opts it out.
    caching: Option<bool>,
    /// REDIRECT target client id (== fd); 0 delivers to this connection itself.
    /// A RESP2 connection may only track when it redirects to another client.
    redirect: RawFd,
}

struct Conn {
    rbuf: Vec<u8>,
    wbuf: Vec<u8>,
    wpos: usize,
    want_write: bool,
    closing: bool,
    // auth state (only meaningful when the worker has an AuthConfig)
    authed: bool,
    role: String,
    tenant: String,
    exempt: bool,
    // durable sync-ack: the highest (ring, seq) per ring that this connection's
    // held replies are waiting on. While non-empty nothing is flushed, so every
    // reply behind it stays behind it and command order is preserved by `wbuf`
    // alone. Commands ARE still read and applied meanwhile, up to
    // `MAX_HELD_REPLY_BYTES` of held reply — that is what lets a pipelined
    // client put more than one write into a single persist window.
    //
    // One entry per ring, holding the highest sequence seen, because a ring
    // commits in order: waiting for the newest record on a ring implies every
    // earlier one on it has committed too.
    ack: Vec<(usize, u64)>,
    // Backpressure park: the persistence ring had no room for this command's
    // record, so the command was NOT applied and NOT consumed from `rbuf`. It
    // is retried verbatim once the ring drains. Nothing is mutated and nothing
    // is replied while this is set, which is what keeps the shared-memory
    // store and `supacache.kv` from diverging under overload.
    parked: bool,
    // When set, this connection is TLS: ciphertext on the socket, plaintext in
    // rbuf/wbuf. `wpos` then counts wbuf bytes already fed to the TLS writer.
    tls: Option<Box<rustls::ServerConnection>>,
    // pub/sub: channels and glob patterns this connection is subscribed
    // to. Non-empty => the connection is in RESP2 subscribe mode.
    subs: HashSet<Vec<u8>>,
    psubs: HashSet<Vec<u8>>,
    // transactions: inside MULTI, commands are queued (raw argv) rather than run,
    // then executed atomically on EXEC. `watch` snapshots (scoped key, version)
    // at WATCH time; EXEC aborts (null array) if any snapshot no longer matches.
    in_multi: bool,
    queued: Vec<Vec<Vec<u8>>>,
    watch: Vec<(Vec<u8>, Option<u64>)>,
    // RESP3 (set by `HELLO 3`): reply nulls/maps/sets/doubles use the RESP3 wire
    // forms and pub/sub + invalidations are delivered as push (`>`) frames.
    resp3: bool,
    // Server-assisted client-side caching (CLIENT TRACKING). Keys this
    // connection reads are recorded in the worker's `tracked` table (default /
    // OPTIN / OPTOUT modes) or matched by prefix at write time (BCAST); an
    // `invalidate` push is then sent to this connection or its REDIRECT target.
    track: Tracking,
}

impl Conn {
    /// Bytes already written into this connection's reply buffer but not yet
    /// sent. While a durable ack is outstanding `flush` holds all of it, so this
    /// is how far ahead of its commits a pipelining client has been allowed to
    /// run.
    fn held_reply_bytes(&self) -> usize {
        self.wbuf.len().saturating_sub(self.wpos)
    }
}

pub struct Worker {
    store: Arc<Store>,
    batcher: Option<Arc<Batcher>>,
    tier: Tier,
    listen_fd: RawFd,
    // Readiness poller (epoll/kqueue). `registry` is a clone of `poll`'s registry
    // so fds can be (re)registered without borrowing `poll` while it is being
    // polled. Each fd registers under `Token(fd)`, so an event's token is the fd.
    poll: Poll,
    registry: Registry,
    conns: HashMap<RawFd, Conn>,
    args: Vec<(usize, usize)>,
    // when non-empty, every write is enqueued into one of these shared-memory
    // rings (sharded by key slot) and a dedicated persistence worker drains each
    // — the RESP path never touches SPI. Multiple rings scale durable writes.
    producers: Vec<ring::Producer>,
    // when set, RESP AUTH is required and keys are ACL-checked + tenant-scoped.
    auth: Option<AuthConfig>,
    // durable tier: hold each write's RESP reply until its ring record commits.
    sync_ack: bool,
    // Cluster routing. When set, this worker serves only the keys whose CRC16
    // slot it owns and answers anything else with a Redis-Cluster `MOVED`
    // redirect. Enabled for persisted multi-worker deployments, where a write
    // taken by the wrong worker would be recovered into the owning worker's
    // segment after a restart and so silently vanish from the one that took it.
    routing: Option<Routing>,
    // Largest bulk string accepted from a client, from
    // `pg_keyspace.max_value_bytes`. The arena is the real bound on what
    // can be stored; this bounds what will even be buffered, so a hostile
    // client cannot make the server hold an arbitrary amount for a value
    // that is going to be refused anyway.
    max_value_bytes: usize,
    // TLS: when set, every accepted connection is wrapped in a TLS session so
    // the RESP wire is encrypted (the AUTH password is otherwise sent in clear).
    tls_config: Option<Arc<rustls::ServerConfig>>,
    // pub/sub: reverse indexes channel/pattern -> subscriber fds, so a
    // PUBLISH fans out without scanning every connection. Local to this worker.
    channels: HashMap<Vec<u8>, HashSet<RawFd>>,
    patterns: HashMap<Vec<u8>, HashSet<RawFd>>,
    // cross-worker pub/sub: when workers share a process (the scale-out
    // daemon), a shared Bus routes a PUBLISH to subscribers on *other* workers.
    // `None` for the single-worker in-PG extension (local delivery only).
    bus: Option<Arc<pubsub::Bus>>,
    worker_id: usize,
    // Server-assisted client-side caching: scoped key -> the fds that read it
    // while CLIENT TRACKING was on. A write to a key sends each of those fds an
    // `invalidate` push and drops the entry (the client re-reads to re-track).
    // Local to this worker — correct because each worker owns an independent
    // keyspace segment (daemon and in-PG extension alike): a key is only ever
    // read and written on the one worker that owns it, so no invalidation ever
    // needs to cross workers.
    tracked: HashMap<Vec<u8>, HashSet<RawFd>>,
    // Connections in BCAST tracking mode. A write checks each against its
    // prefixes instead of the per-read `tracked` table. Kept as a set so the
    // write path pays nothing when no connection is broadcasting.
    bcast_subs: HashSet<RawFd>,
}

impl Worker {
    pub fn new(
        store: Arc<Store>,
        batcher: Option<Arc<Batcher>>,
        tier: Tier,
        addr: &str,
        port: u16,
    ) -> io::Result<Worker> {
        let listen_fd = listen(addr, port)?;
        let poll = Poll::new()?;
        let registry = poll.registry().try_clone()?;
        registry.register(
            &mut SourceFd(&listen_fd),
            Token(listen_fd as usize),
            Interest::READABLE,
        )?;
        Ok(Worker {
            store,
            batcher,
            tier,
            listen_fd,
            poll,
            registry,
            conns: HashMap::new(),
            args: Vec::with_capacity(8),
            producers: Vec::new(),
            auth: None,
            sync_ack: false,
            routing: None,
            max_value_bytes: resp::DEFAULT_MAX_BULK_LEN,
            tls_config: None,
            channels: HashMap::new(),
            patterns: HashMap::new(),
            bus: None,
            worker_id: 0,
            tracked: HashMap::new(),
            bcast_subs: HashSet::new(),
        })
    }

    /// Join a cross-worker pub/sub Bus as worker `worker_id`. The Bus's wake fd
    /// is registered with this worker's poller so remote deliveries wake it
    /// promptly. Only used by the multi-worker daemon; the in-PG extension runs
    /// a single worker and never calls this.
    pub fn set_bus(&mut self, bus: Arc<pubsub::Bus>, worker_id: usize) {
        let wfd = bus.wake_fd(worker_id);
        let _ = self
            .registry
            .register(&mut SourceFd(&wfd), Token(wfd as usize), Interest::READABLE);
        self.bus = Some(bus);
        self.worker_id = worker_id;
    }

    /// Durable tier: hold each write's reply until its ring record has committed.
    pub fn set_sync_ack(&mut self, on: bool) {
        self.sync_ack = on;
    }

    /// Largest bulk string to accept from a client (`pg_keyspace.max_value_bytes`).
    pub fn set_max_value_bytes(&mut self, n: usize) {
        self.max_value_bytes = n.max(1);
    }

    /// Enable TLS: every accepted connection is wrapped in a server-side TLS
    /// session, so the RESP wire (including the AUTH password) is encrypted.
    pub fn set_tls_config(&mut self, cfg: Arc<rustls::ServerConfig>) {
        self.tls_config = Some(cfg);
    }

    /// Enable persistence: writes are sharded by key slot across these rings,
    /// each drained by its own persistence worker.
    pub fn set_ring_producers(&mut self, producers: Vec<ring::Producer>) {
        self.producers = producers;
    }

    /// Serve only this worker's slot range, redirecting every other key with a
    /// Redis-Cluster `MOVED`.
    ///
    /// `endpoints[w]` is the `host:port` a client should retry against for a key
    /// worker `w` owns. Persisted multi-worker deployments must enable this:
    /// recovery restores each key into the segment its slot range covers, so a
    /// worker that accepted a key it does not own would lose that key on the next
    /// restart — after having acked the write as durable.
    pub fn set_slot_routing(&mut self, index: usize, nworkers: usize, endpoints: Vec<String>) {
        self.routing = if nworkers > 1 {
            let node_ids = endpoints.iter().map(|e| node_id_for(e)).collect();
            Some(Routing { index, nworkers, endpoints, node_ids })
        } else {
            None
        };
    }

    /// Enable access control: RESP AUTH required, keyspace ACL + tenant scope.
    pub fn set_auth_config(&mut self, auth: AuthConfig) {
        self.auth = Some(auth);
    }

    pub fn run(&mut self) -> io::Result<()> {
        self.run_with(|| Tick::Continue, -1)
    }

    /// Run the event loop, calling `tick()` once per iteration (and whenever a
    /// signal interrupts the wait). `timeout_ms` bounds each poll so the
    /// tick runs even when idle (a Postgres background worker uses this to notice
    /// SIGTERM/SIGHUP). `Tick::Stop` ends the loop; `Tick::Reload(cfg)` swaps the
    /// auth config live — the hot-reload hook the extension drives on SIGHUP, so
    /// credential changes need no restart (`None` = switch to no-auth).
    pub fn run_with<F: FnMut() -> Tick>(&mut self, mut tick: F, timeout_ms: i32) -> io::Result<()> {
        let mut events = Events::with_capacity(POLL_MAX);
        // `timeout_ms < 0` means block indefinitely (matches the old epoll_wait -1).
        let timeout = if timeout_ms < 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms as u64))
        };
        loop {
            match tick() {
                Tick::Stop => return Ok(()),
                Tick::Reload(r) => {
                    if let Some(a) = r.auth {
                        self.auth = a;
                    }
                    if let Some(t) = r.tls {
                        self.tls_config = t;
                    }
                }
                Tick::Continue => {}
            }
            if let Err(e) = self.poll.poll(&mut events, timeout) {
                // A signal (e.g. SIGTERM/SIGHUP in the bgworker) interrupts the
                // wait; loop so `tick` observes it, exactly as with epoll_wait+EINTR.
                if e.kind() == io::ErrorKind::Interrupted {
                    continue;
                }
                return Err(e);
            }
            for ev in events.iter() {
                let fd = ev.token().0 as RawFd;
                if fd == self.listen_fd {
                    self.accept_all();
                } else if self.bus.as_ref().map_or(false, |b| fd == b.wake_fd(self.worker_id)) {
                    // A remote worker published to a channel we subscribe to, or
                    // invalidated a key we may be tracking: drain the inbox and
                    // apply each locally (never re-broadcast — these are local-only
                    // so a cross-worker message can't ping-pong).
                    let msgs = self.bus.as_ref().unwrap().drain(self.worker_id);
                    for m in msgs {
                        match m {
                            pubsub::BusMsg::Publish(channel, msg) => {
                                self.deliver_local(&channel, &msg);
                            }
                            // -1 is not a live fd, so nothing local is skipped: the
                            // writer that made the change is on another worker.
                            pubsub::BusMsg::Invalidate(Some(key)) => {
                                self.invalidate_key(&key, -1);
                            }
                            pubsub::BusMsg::Invalidate(None) => {
                                self.invalidate_all(-1);
                            }
                        }
                    }
                } else {
                    if ev.is_readable() {
                        self.on_readable(fd);
                    }
                    if self.conns.contains_key(&fd) && ev.is_writable() {
                        self.flush(fd);
                    }
                }
            }
            // Publish how far persistence has committed so eviction knows which
            // referenced values are safe to drop. The minimum across rings is
            // used because sequence numbers are per-ring: conservative, and
            // exact for the default single persist shard.
            if !self.producers.is_empty() {
                let w = self
                    .producers
                    .iter()
                    .map(|p| p.committed())
                    .min()
                    .unwrap_or(0);
                self.store.set_commit_watermark(w);
            }
            // Durable tier: release replies whose ring records have committed,
            // then retry anything parked waiting for ring space to free up.
            if self.sync_ack {
                self.resolve_acks();
                self.resume_parked();
            }
        }
    }

    /// Fold this command's acks into the connection's, keeping the highest
    /// sequence per ring.
    ///
    /// Replacing rather than merging was safe only while a connection could
    /// have one write in flight. With a pipeline it loses waits: a first write
    /// on ring 0 and a second on ring 1 would leave only ring 1's sequence, and
    /// the first write's reply would flush before its record committed.
    fn merge_acks(dst: &mut Vec<(usize, u64)>, src: Vec<(usize, u64)>) {
        for (shard, seq) in src {
            match dst.iter_mut().find(|(sh, _)| *sh == shard) {
                Some(slot) => slot.1 = slot.1.max(seq),
                None => dst.push((shard, seq)),
            }
        }
    }

    /// Whether every ring this command will write to can take its record(s).
    ///
    /// Sizes are upper bounds, deliberately. A string write persists the value
    /// it was given, so that one is exact. An aggregate write (`HSET`, `LPUSH`,
    /// `ZADD`, …) persists the *whole* post-mutation blob, whose size is not
    /// known until after the mutation we are trying to avoid, so bound it by
    /// the current blob plus every argument byte: the new blob cannot exceed
    /// what is already stored plus what is being added. Over-estimating parks
    /// slightly early, which is the safe direction.
    fn rings_have_room(&self, cmd: &[u8], args: &[Vec<u8>], key_idxs: &[usize]) -> bool {
        let n = self.producers.len();
        if n == 0 {
            return true;
        }
        let arg_bytes: usize = args.iter().map(|a| a.len()).sum();
        let aggregate = is_aggregate_write(cmd);
        for &ki in key_idxs {
            let key = match args.get(ki) {
                Some(k) => k,
                None => continue,
            };
            let inline_bound = if aggregate {
                // current blob (if any) + everything this command could add
                let cur = self
                    .store
                    .get_typed(key)
                    .map(|(_, _, blob)| blob.len())
                    .unwrap_or(0);
                cur + arg_bytes
            } else {
                arg_bytes
            };
            // A record never carries more than `INLINE_MAX` of value: past that
            // the value is staged by reference and the record holds an 8-byte
            // version instead. Sizing this check by the value itself made a
            // write larger than `ring_mb` demand ring space it would never use,
            // so `has_room` could never be satisfied and the connection parked
            // forever with no error and no timeout.
            let val_bound = inline_bound.min(INLINE_MAX);
            if !self.producers[shard_of(n, key)].has_room(key.len(), val_bound) {
                return false;
            }
        }
        true
    }

    /// Retry connections parked on a full persistence ring. Called once per
    /// event-loop pass in a sync-ack tier: the poll has a bounded timeout, so
    /// a parked connection is retried promptly without needing its own timer,
    /// and the retry is just `process` re-reading the command still sitting in
    /// `rbuf`.
    fn resume_parked(&mut self) {
        let parked: Vec<RawFd> = self
            .conns
            .iter()
            .filter(|(_, c)| c.parked)
            .map(|(fd, _)| *fd)
            .collect();
        for fd in parked {
            if let Some(c) = self.conns.get_mut(&fd) {
                c.parked = false; // re-evaluated by the pre-flight on retry
            }
            self.process(fd);
            if self.conns.contains_key(&fd) {
                self.flush(fd);
            }
        }
    }

    /// Send the held reply for any connection whose durable write(s) have now
    /// committed, then resume reading that connection.
    fn resolve_acks(&mut self) {
        let ready: Vec<RawFd> = self
            .conns
            .iter()
            .filter(|(_, c)| {
                !c.ack.is_empty()
                    && c.ack.iter().all(|&(sh, seq)| {
                        self.producers.get(sh).map_or(true, |p| p.committed() >= seq)
                    })
            })
            .map(|(fd, _)| *fd)
            .collect();
        for fd in ready {
            if let Some(c) = self.conns.get_mut(&fd) {
                c.ack.clear();
            }
            self.flush(fd);
            if self.conns.contains_key(&fd) {
                self.process(fd); // handle any commands buffered behind the ack
                if self.conns.contains_key(&fd) {
                    self.flush(fd);
                }
            }
        }
    }

    fn accept_all(&mut self) {
        loop {
            let cfd = accept_nonblocking(self.listen_fd);
            if cfd < 0 {
                break; // EAGAIN
            }
            set_nodelay(cfd);
            if self
                .registry
                .register(&mut SourceFd(&cfd), Token(cfd as usize), Interest::READABLE)
                .is_err()
            {
                unsafe { libc::close(cfd) };
                continue;
            }
            let tls = match &self.tls_config {
                Some(cfg) => match rustls::ServerConnection::new(cfg.clone()) {
                    Ok(s) => Some(Box::new(s)),
                    Err(_) => {
                        let _ = self.registry.deregister(&mut SourceFd(&cfd));
                        unsafe { libc::close(cfd) };
                        continue;
                    }
                },
                None => None,
            };
            self.conns.insert(
                cfd,
                Conn {
                    rbuf: Vec::with_capacity(READ_CHUNK),
                    wbuf: Vec::with_capacity(READ_CHUNK),
                    wpos: 0,
                    want_write: false,
                    closing: false,
                    authed: false,
                    role: String::new(),
                    tenant: String::new(),
                    exempt: false,
                    ack: Vec::new(),
                    parked: false,
                    tls,
                    subs: HashSet::new(),
                    psubs: HashSet::new(),
                    in_multi: false,
                    queued: Vec::new(),
                    watch: Vec::new(),
                    resp3: false,
                    track: Tracking::default(),
                },
            );
        }
    }

    fn on_readable(&mut self, fd: RawFd) {
        let is_tls = self.conns.get(&fd).map(|c| c.tls.is_some()).unwrap_or(false);
        if is_tls {
            if self.tls_read(fd) {
                return; // connection closed during TLS read
            }
        } else {
            let mut scratch = [0u8; READ_CHUNK];
            loop {
                let r = unsafe {
                    libc::read(fd, scratch.as_mut_ptr() as *mut libc::c_void, scratch.len())
                };
                if r > 0 {
                    let c = self.conns.get_mut(&fd).unwrap();
                    c.rbuf.extend_from_slice(&scratch[..r as usize]);
                    if (r as usize) < scratch.len() {
                        break; // drained the socket
                    }
                } else if r == 0 {
                    self.close(fd);
                    return;
                } else {
                    let e = io::Error::last_os_error();
                    match e.raw_os_error() {
                        Some(libc::EAGAIN) => break,
                        Some(libc::EINTR) => continue,
                        _ => {
                            self.close(fd);
                            return;
                        }
                    }
                }
            }
        }
        self.process(fd);
        if self.conns.contains_key(&fd) {
            self.flush(fd);
        }
    }

    /// Pump ciphertext from the socket through the TLS session into `rbuf`
    /// (plaintext). Returns true if the connection was closed (EOF or TLS error).
    /// Handshake round-trips flow through here too — the response is sent by the
    /// subsequent `flush` (which drains `tls.wants_write()`).
    fn tls_read(&mut self, fd: RawFd) -> bool {
        enum Res {
            Ok,
            Close,
        }
        let res = {
            let c = match self.conns.get_mut(&fd) {
                Some(c) => c,
                None => return true,
            };
            let tls = c.tls.as_mut().unwrap();
            let mut sock = FdIo(fd);
            let mut eof = false;
            let mut res = Res::Ok;
            loop {
                match tls.read_tls(&mut sock) {
                    Ok(0) => {
                        eof = true;
                        break;
                    }
                    Ok(_) => {
                        if tls.process_new_packets().is_err() {
                            res = Res::Close;
                            break;
                        }
                    }
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        res = Res::Close;
                        break;
                    }
                }
            }
            if matches!(res, Res::Ok) {
                let mut tmp = [0u8; READ_CHUNK];
                loop {
                    match tls.reader().read(&mut tmp) {
                        Ok(0) => break,
                        Ok(n) => c.rbuf.extend_from_slice(&tmp[..n]),
                        Err(e) if e.kind() == io::ErrorKind::WouldBlock => break,
                        Err(_) => break,
                    }
                }
                if eof && c.rbuf.is_empty() {
                    res = Res::Close;
                }
            }
            res
        };
        match res {
            Res::Ok => false,
            Res::Close => {
                self.close(fd);
                true
            }
        }
    }

    fn process(&mut self, fd: RawFd) {
        // A pending durable reply no longer stops the connection being read.
        // Replies are appended to `wbuf` in command order and `flush` holds the
        // whole buffer until every outstanding ack commits, so order is
        // preserved without stalling: what used to be one write per persist
        // window per connection can now fill a window.
        //
        // The stall is kept only as a memory bound. Past MAX_HELD_REPLY_BYTES of
        // held reply the connection waits for an ack to resolve, and
        // `resolve_acks` resumes it.
        if self
            .conns
            .get(&fd)
            .map(|c| c.held_reply_bytes() >= MAX_HELD_REPLY_BYTES)
            .unwrap_or(true)
        {
            return;
        }
        let mut consumed_total = 0usize;
        // Copied out before the borrows below: `self.args` is taken mutably
        // inside the block, so `self.max_value_bytes` cannot also be read there.
        let max_value_bytes = self.max_value_bytes;
        loop {
            // Parse one command and materialise its args as owned bytes, so the
            // immutable borrow of rbuf is dropped before we touch wbuf/store.
            let (parse, cmd_args) = {
                let c = match self.conns.get(&fd) {
                    Some(c) => c,
                    None => return,
                };
                let buf = &c.rbuf[consumed_total..];
                let parse = resp::parse(buf, &mut self.args, max_value_bytes);
                let cmd_args: Vec<Vec<u8>> = if let Parse::Complete { .. } = parse {
                    self.args.iter().map(|&(s, e)| buf[s..e].to_vec()).collect()
                } else {
                    Vec::new()
                };
                (parse, cmd_args)
            };
            match parse {
                Parse::Incomplete => break,
                Parse::Error => {
                    self.close(fd);
                    return;
                }
                Parse::Complete { consumed } => {
                    self.dispatch(fd, &cmd_args);
                    // Parked on a full ring: the command was neither applied
                    // nor answered, so it must stay in `rbuf` to be retried
                    // verbatim. Consuming it here would lose the write.
                    if self.conns.get(&fd).map(|c| c.parked).unwrap_or(false) {
                        break;
                    }
                    consumed_total += consumed;
                    let stop = self
                        .conns
                        .get(&fd)
                        .map(|c| c.closing || c.held_reply_bytes() >= MAX_HELD_REPLY_BYTES)
                        .unwrap_or(true);
                    if stop {
                        break; // connection closing, or its held replies are capped
                    }
                }
            }
        }
        if consumed_total > 0 {
            if let Some(c) = self.conns.get_mut(&fd) {
                c.rbuf.drain(..consumed_total);
            }
        }
    }

    /// Dispatch one command (owned args); reply is appended to the conn's write
    /// buffer. `args[0]` is the command name.
    fn dispatch(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        if args.is_empty() {
            return;
        }
        let nargs = args.len();
        let mut cmd = args[0].clone();
        cmd.make_ascii_uppercase();
        // RESP3 wire form for this connection (typed nulls/maps/sets/doubles and
        // push frames). Read once up front so every reply site — including the
        // auth-gate nil below — can use the right encoding.
        let resp3 = self.conns.get(&fd).map(|c| c.resp3).unwrap_or(false);

        // ---- AUTH command ----
        if cmd == b"AUTH" {
            self.handle_auth(fd, args);
            return;
        }

        // ---- HELLO: RESP2/RESP3 negotiation ----
        if cmd == b"HELLO" {
            self.handle_hello(fd, args);
            return;
        }

        // ---- CLIENT (incl. TRACKING for client-side caching) ----
        if cmd == b"CLIENT" {
            self.handle_client(fd, args);
            return;
        }

        // ---- pub/sub: handled before keyed-command scoping. Channels
        // are tenant-scoped for non-exempt authed roles (same `{tenant}:` prefix
        // as keys); the scoping is transparent — every reply/message frame echoes
        // the client's own unscoped name. ----
        match cmd.as_slice() {
            b"SUBSCRIBE" => return self.handle_subscribe(fd, args, false),
            b"PSUBSCRIBE" => return self.handle_subscribe(fd, args, true),
            b"UNSUBSCRIBE" => return self.handle_unsubscribe(fd, args, false),
            b"PUNSUBSCRIBE" => return self.handle_unsubscribe(fd, args, true),
            b"PUBLISH" => return self.handle_publish(fd, args),
            _ => {}
        }
        // In RESP2 subscribe mode only (P)(UN)SUBSCRIBE / PING / QUIT / RESET run.
        let subscribed = self
            .conns
            .get(&fd)
            .map(|c| !c.subs.is_empty() || !c.psubs.is_empty())
            .unwrap_or(false);
        if subscribed && !matches!(cmd.as_slice(), b"PING" | b"QUIT" | b"RESET") {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::error(
                out,
                "ERR Can't execute command: only (P)SUBSCRIBE / (P)UNSUBSCRIBE / PING / QUIT / RESET are allowed in subscribe context",
            );
            return;
        }
        if cmd == b"QUIT" {
            let c = self.conns.get_mut(&fd).unwrap();
            resp::simple(&mut c.wbuf, "OK");
            c.closing = true; // flushed then closed by on_readable/flush
            return;
        }

        // ---- transactions: MULTI / EXEC / DISCARD / WATCH / UNWATCH ----
        match cmd.as_slice() {
            b"MULTI" => return self.handle_multi(fd),
            b"EXEC" => return self.handle_exec(fd),
            b"DISCARD" => return self.handle_discard(fd),
            b"WATCH" => return self.handle_watch(fd, args),
            b"UNWATCH" => {
                let c = self.conns.get_mut(&fd).unwrap();
                c.watch.clear();
                resp::simple(&mut c.wbuf, "OK");
                return;
            }
            _ => {}
        }
        // Inside MULTI, every other command is queued (not run) and answered
        // `+QUEUED`; the queue is executed atomically by EXEC.
        if self.conns.get(&fd).map(|c| c.in_multi).unwrap_or(false) {
            let c = self.conns.get_mut(&fd).unwrap();
            c.queued.push(args.to_vec());
            resp::simple(&mut c.wbuf, "QUEUED");
            return;
        }

        // ---- auth gate + forced tenant scoping for keyed commands ----
        // `eff` holds the args actually used below; key positions are rewritten
        // to `{tenant}:{key}` for non-exempt authenticated roles.
        let mut eff: Vec<Vec<u8>> = Vec::new();
        let key_idxs = key_indices(&cmd, args);
        if self.auth.is_some() && !key_idxs.is_empty() {
            eff = args.to_vec();
            if let Err(d) = self.apply_auth(fd, &cmd, &key_idxs, &mut eff) {
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                match d {
                    Deny::NoAuth => resp::error(out, "NOAUTH Authentication required."),
                    Deny::Perm => resp::error(
                        out,
                        "NOPERM this user has no permissions to access one of the keys used as arguments",
                    ),
                    Deny::Nil => resp::null(out, resp3),
                }
                return;
            }
        }
        // From here on, use the (possibly scoped) args.
        let args: &[Vec<u8>] = if eff.is_empty() { args } else { &eff };

        // ---- cluster routing: refuse keys this worker does not own ----
        // Checked on the tenant-scoped key, which is what gets stored, sharded
        // into a ring and persisted, so the slot here is the slot recovery will
        // route by. A multi-key command touching another worker's key is a
        // genuine CROSSSLOT: the workers are shared-nothing, so no single worker
        // can serve it.
        if let Some(r) = &self.routing {
            let mut owners = key_idxs.iter().filter_map(|&i| {
                args.get(i).map(|k| (crc16::key_owner(k, r.nworkers), i))
            });
            if let Some((first, idx)) = owners.next() {
                // All keys on one worker: serve it here, or redirect the client
                // there. Keys split across workers cannot be served by anyone.
                let split = owners.any(|(o, _)| o != first);
                if split {
                    let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                    resp::error(out, "CROSSSLOT Keys in request don't hash to the same slot");
                    return;
                }
                if first != r.index {
                    let slot = crc16::key_slot(&args[idx]);
                    let ep = r.endpoints.get(first).cloned().unwrap_or_default();
                    let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                    resp::error(out, &format!("MOVED {slot} {ep}"));
                    return;
                }
            }
        }

        let store = self.store.clone();
        let batcher = self.batcher.clone();
        let tier = self.tier;
        let persist_on = !self.producers.is_empty();
        let sync_ack = self.sync_ack;
        // Tenant scope for SCAN/KEYS (None for unauthed/exempt): keys are stored
        // as `{tenant}:{key}`, so a scoped connection only sees — and only reports
        // the unscoped form of — keys under its own prefix. Computed before the
        // `out` borrow below, which takes `self` mutably.
        let scan_prefix = self.conn_prefix(fd);
        // A write to enqueue for persistence, applied after the match so
        // it does not tangle with the `out` borrow.
        let mut stages: Vec<PendingWrite> = Vec::new();
        // (ring, seq) records enqueued this command; a durable write's reply is
        // held until all of them commit.
        let mut acks: Vec<(usize, u64)> = Vec::new();

        // ---- backpressure pre-flight (sync-ack tiers only) ----------------
        // Check the ring BEFORE touching the store. Applying the write first
        // and discovering afterwards that it cannot be queued leaves the
        // shared-memory store holding a value that will never be durable: the
        // client is told the write failed, the very next GET returns it, and a
        // restart loses it. Ordering the check first makes that impossible.
        //
        // The check is sound without a lock because there is one producer per
        // ring (this event loop) and the consumer only frees space, so room
        // seen here still exists at push time.
        if sync_ack && persist_on && is_write_cmd(&cmd) {
            if !self.rings_have_room(&cmd, args, &key_idxs) {
                // Park: nothing mutated, nothing replied. `process` leaves the
                // command in `rbuf` and retries it once the ring drains.
                if let Some(c) = self.conns.get_mut(&fd) {
                    c.parked = true;
                }
                return;
            }
        }

        // Where this command's reply begins, so a late failure can rewind it.
        let reply_start = self.conns.get(&fd).map_or(0, |c| c.wbuf.len());
        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;

        match cmd.as_slice() {
            b"PING" => {
                if nargs >= 2 {
                    resp::bulk(out, &args[1]);
                } else {
                    resp::simple(out, "PONG");
                }
            }
            b"QUIT" => {
                resp::simple(out, "OK");
                self.conns.get_mut(&fd).unwrap().closing = true;
            }
            // HELLO and CLIENT are handled before this match.
            b"CONFIG" | b"SELECT" | b"RESET" => resp::simple(out, "OK"),
            b"COMMAND" => {
                let specs = command_specs();
                match args.get(1).map(|a| a.to_ascii_uppercase()).as_deref() {
                    None => {
                        resp::array_header(out, specs.len());
                        for c in &specs {
                            write_command_entry(out, c);
                        }
                    }
                    Some(b"COUNT") => resp::integer(out, specs.len() as i64),
                    Some(b"INFO") => {
                        let names: Vec<Vec<u8>> =
                            args[2..].iter().map(|a| a.to_ascii_uppercase()).collect();
                        let wanted: Vec<&Vec<u8>> = names.iter().collect();
                        if wanted.is_empty() {
                            resp::array_header(out, specs.len());
                            for c in &specs {
                                write_command_entry(out, c);
                            }
                        } else {
                            resp::array_header(out, wanted.len());
                            for want in wanted {
                                match specs.iter().find(|c| c.name.as_bytes() == want.as_slice()) {
                                    Some(c) => write_command_entry(out, c),
                                    None => resp::nil_array(out),
                                }
                            }
                        }
                    }
                    // The `numkeys` commands are flagged movablekeys, so clients
                    // ask here instead of reading fixed positions. Answered from
                    // key_indices, the same function routing uses.
                    Some(b"GETKEYS") if nargs >= 3 => {
                        let sub: Vec<Vec<u8>> = args[2..].to_vec();
                        let name = sub[0].to_ascii_uppercase();
                        let idxs = key_indices(&name, &sub);
                        if idxs.is_empty() {
                            resp::error(out, "ERR The command has no key arguments");
                        } else {
                            resp::array_header(out, idxs.len());
                            for i in idxs {
                                resp::bulk(out, &sub[i]);
                            }
                        }
                    }
                    Some(b"DOCS") => resp::map_header(out, 0, resp3),
                    _ => resp::array_header(out, 0),
                }
            }
            b"DBSIZE" => {
                let mut total = 0i64;
                for p in 0..store.num_partitions() {
                    total += store.stats(p).entries as i64;
                }
                resp::integer(out, total);
            }
            b"GET" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'get'");
                    return;
                }
                if !check_string(&store, &args[1], out) {
                    return;
                }
                match store.get(&args[1]) {
                    Lookup::Hit(v) => resp::bulk(out, v),
                    Lookup::Miss => resp::null(out, resp3),
                }
            }
            b"SET" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'set'");
                    return;
                }
                // Options are a mix of pairs (EX 10) and bare flags (KEEPTTL, NX).
                // The old parser stepped two at a time and so never saw a flag at
                // all: `SET k v KEEPTTL` silently dropped the TTL and `SET k v NX`
                // silently overwrote. Unknown options are a syntax error, as in
                // Redis — accepting and ignoring one is how those bugs hid.
                let mut ttl_micros = 0i64;
                let mut keep_ttl = false;
                let (mut only_if_absent, mut only_if_present, mut want_old) = (false, false, false);
                let mut i = 3;
                let mut bad_syntax = false;
                while i < nargs {
                    let mut opt = args[i].clone();
                    opt.make_ascii_uppercase();
                    // Pair options consume the next argument as a number.
                    let mut pair = |factor: i64, absolute: bool| -> Option<i64> {
                        let raw: i64 = args
                            .get(i + 1)
                            .and_then(|a| std::str::from_utf8(a).ok())
                            .and_then(|t| t.parse().ok())?;
                        Some(if absolute {
                            // EXAT/PXAT are absolute deadlines; store TTL is relative.
                            (raw * factor - now_micros()).max(1)
                        } else {
                            raw * factor
                        })
                    };
                    match opt.as_slice() {
                        b"EX" => match pair(1_000_000, false) {
                            Some(v) => { ttl_micros = v; i += 2; }
                            None => { bad_syntax = true; break; }
                        },
                        b"PX" => match pair(1_000, false) {
                            Some(v) => { ttl_micros = v; i += 2; }
                            None => { bad_syntax = true; break; }
                        },
                        b"EXAT" => match pair(1_000_000, true) {
                            Some(v) => { ttl_micros = v; i += 2; }
                            None => { bad_syntax = true; break; }
                        },
                        b"PXAT" => match pair(1_000, true) {
                            Some(v) => { ttl_micros = v; i += 2; }
                            None => { bad_syntax = true; break; }
                        },
                        b"KEEPTTL" => { keep_ttl = true; i += 1; }
                        b"NX" => { only_if_absent = true; i += 1; }
                        b"XX" => { only_if_present = true; i += 1; }
                        b"GET" => { want_old = true; i += 1; }
                        _ => { bad_syntax = true; break; }
                    }
                }
                if bad_syntax || (only_if_absent && only_if_present) {
                    resp::error(out, "ERR syntax error");
                    return;
                }
                // Only the option-bearing forms need the prior value; a plain
                // `SET k v` must not pay for a lookup and a copy on the hot path.
                let needs_existing = want_old || only_if_absent || only_if_present || keep_ttl;
                let existing = if needs_existing {
                    store.get_typed(&args[1]).map(|(k, e, v)| (k, e, v.to_vec()))
                } else {
                    None
                };
                // GET reports the previous value, and it must be a string.
                if want_old {
                    if let Some((kind, _, _)) = &existing {
                        if *kind != b's' as u32 {
                            resp::error(
                                out,
                                "WRONGTYPE Operation against a key holding the wrong kind of value",
                            );
                            return;
                        }
                    }
                }
                let present = existing.is_some();
                if (only_if_absent && present) || (only_if_present && !present) {
                    // Not set. GET still reports the old value; otherwise nil.
                    match (want_old, &existing) {
                        (true, Some((_, _, v))) => resp::bulk(out, v),
                        _ => resp::null(out, resp3),
                    }
                    return;
                }
                // KEEPTTL retains the current deadline; an explicit EX/PX wins.
                if keep_ttl && ttl_micros == 0 {
                    if let Some((_, exp, _)) = &existing {
                        if *exp > 0 {
                            ttl_micros = (*exp - now_micros()).max(1);
                        }
                    }
                }
                if !wrote(out, store.set(&args[1], &args[2], ttl_micros)) {
                    return;
                }
                match (want_old, &existing) {
                    (true, Some((_, _, v))) => resp::bulk(out, v),
                    (true, None) => resp::null(out, resp3),
                    _ => resp::simple(out, "OK"),
                }
                durable_log(&batcher, tier, &args[1], &args[2]);
                if persist_on {
                    let exp = if ttl_micros > 0 { now_micros() + ttl_micros } else { 0 };
                    stages.push((args[1].clone(), args[2].clone(), exp, b's'));
                }
            }
            b"SETNX" => {
                let exists = matches!(store.get(&args[1]), Lookup::Hit(_));
                let n = if exists {
                    0
                } else {
                    if !wrote(out, store.set(&args[1], &args[2], 0)) {
                        return;
                    }
                    durable_log(&batcher, tier, &args[1], &args[2]);
                    if persist_on {
                        stages.push((args[1].clone(), args[2].clone(), 0, b's'));
                    }
                    1
                };
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::integer(out, n);
            }
            b"GETSET" => {
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let old = match store.get(&args[1]) {
                    Lookup::Hit(v) => Some(v.to_vec()),
                    Lookup::Miss => None,
                };
                if !wrote(out, store.set(&args[1], &args[2], 0)) {
                    return;
                }
                durable_log(&batcher, tier, &args[1], &args[2]);
                if persist_on {
                    stages.push((args[1].clone(), args[2].clone(), 0, b's'));
                }
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                match old {
                    Some(v) => resp::bulk(out, &v),
                    None => resp::null(out, resp3),
                }
            }
            b"DEL" | b"UNLINK" => {
                let mut count = 0i64;
                for a in &args[1..] {
                    if store.del(a) {
                        count += 1;
                        if persist_on {
                            // propagate the delete so it does not resurrect on
                            // crash recovery (key is already tenant-scoped in eff)
                            if let Some(sa) = shard_push(&self.producers, a, b"", DELETE_TOMBSTONE, b's')
                            {
                                acks.push(sa);
                            }
                        }
                    }
                }
                resp::integer(out, count);
            }
            b"EXISTS" => {
                let mut count = 0i64;
                for a in &args[1..] {
                    if matches!(store.get(a), Lookup::Hit(_)) {
                        count += 1;
                    }
                }
                resp::integer(out, count);
            }
            b"TTL" | b"PTTL" => {
                if nargs != 2 {
                    resp::error(out, "ERR wrong number of arguments for 'ttl'");
                    return;
                }
                match store.get_typed(&args[1]) {
                    None => resp::integer(out, -2), // no such key
                    Some((_, 0, _)) => resp::integer(out, -1), // key exists, no TTL
                    Some((_, exp, _)) => {
                        let ms = (exp - now_micros()).max(0) / 1000;
                        if cmd == b"PTTL" {
                            resp::integer(out, ms);
                        } else {
                            resp::integer(out, (ms + 500) / 1000); // round to seconds
                        }
                    }
                }
            }
            b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'expire'");
                    return;
                }
                let n: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()) {
                    Some(v) => v,
                    None => {
                        resp::error(out, "ERR value is not an integer or out of range");
                        return;
                    }
                };
                let exp = match cmd.as_slice() {
                    b"EXPIRE" => now_micros() + n.saturating_mul(1_000_000),
                    b"PEXPIRE" => now_micros() + n.saturating_mul(1_000),
                    b"EXPIREAT" => n.saturating_mul(1_000_000),
                    _ => n.saturating_mul(1_000), // PEXPIREAT (unix millis)
                };
                let existed = store.set_expiry(&args[1], exp);
                // Persist the new expiry (or a tombstone, if `exp` was in the past
                // and deleted the key) so it survives crash recovery on the durable
                // tiers rather than reverting to the last SET's TTL.
                if existed && persist_on {
                    stages.push(stage_current_state(&store, &args[1]));
                }
                resp::integer(out, if existed { 1 } else { 0 });
            }
            b"PERSIST" => {
                if nargs != 2 {
                    resp::error(out, "ERR wrong number of arguments for 'persist'");
                    return;
                }
                // 1 only if the key exists AND had a TTL to remove.
                let had_ttl = matches!(store.get_typed(&args[1]), Some((_, exp, _)) if exp != 0);
                if had_ttl {
                    store.set_expiry(&args[1], 0);
                    // Persist the cleared expiry through to the durable tier.
                    if persist_on {
                        stages.push(stage_current_state(&store, &args[1]));
                    }
                }
                resp::integer(out, if had_ttl { 1 } else { 0 });
            }
            b"SCAN" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'scan'");
                    return;
                }
                let cursor: u64 =
                    std::str::from_utf8(&args[1]).ok().and_then(|t| t.parse().ok()).unwrap_or(0);
                let mut pattern: Option<&[u8]> = None;
                let mut count: usize = 10;
                let mut i = 2;
                while i + 1 < nargs {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"MATCH" => pattern = Some(&args[i + 1]),
                        b"COUNT" => {
                            if let Some(n) =
                                std::str::from_utf8(&args[i + 1]).ok().and_then(|t| t.parse::<usize>().ok())
                            {
                                count = n.max(1);
                            }
                        }
                        _ => {}
                    }
                    i += 2;
                }
                let (next, raw) = store.scan(cursor, count);
                let mut facing: Vec<&[u8]> = Vec::new();
                for k in &raw {
                    let f: &[u8] = match &scan_prefix {
                        Some(pfx) => {
                            if !k.starts_with(pfx.as_slice()) {
                                continue;
                            }
                            &k[pfx.len()..]
                        }
                        None => k.as_slice(),
                    };
                    if let Some(pat) = pattern {
                        if !glob_match(pat, f) {
                            continue;
                        }
                    }
                    facing.push(f);
                }
                resp::array_header(out, 2);
                resp::bulk(out, next.to_string().as_bytes());
                resp::array_header(out, facing.len());
                for f in facing {
                    resp::bulk(out, f);
                }
            }
            b"KEYS" => {
                if nargs != 2 {
                    resp::error(out, "ERR wrong number of arguments for 'keys'");
                    return;
                }
                let pattern = &args[1];
                let mut facing: Vec<Vec<u8>> = Vec::new();
                let mut cursor = 0u64;
                loop {
                    let (next, raw) = store.scan(cursor, 256);
                    for k in raw {
                        let f: &[u8] = match &scan_prefix {
                            Some(pfx) => {
                                if !k.starts_with(pfx.as_slice()) {
                                    continue;
                                }
                                &k[pfx.len()..]
                            }
                            None => k.as_slice(),
                        };
                        if glob_match(pattern, f) {
                            facing.push(f.to_vec());
                        }
                    }
                    if next == 0 {
                        break;
                    }
                    cursor = next;
                }
                resp::array_header(out, facing.len());
                for f in &facing {
                    resp::bulk(out, f);
                }
            }
            b"INCR" | b"DECR" | b"INCRBY" | b"DECRBY" => {
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let mut by: i64 = if cmd == b"INCRBY" || cmd == b"DECRBY" {
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(0)
                } else {
                    1
                };
                if cmd == b"DECR" || cmd == b"DECRBY" {
                    by = -by;
                }
                match store.incr(&args[1], by) {
                    Some(v) => {
                        resp::integer(out, v);
                        let s = itoa(v);
                        durable_log(&batcher, tier, &args[1], &s);
                        if persist_on {
                            stages.push((args[1].clone(), s, 0, b's'));
                        }
                    }
                    None => resp::error(out, "ERR value is not an integer or out of range"),
                }
            }
            b"MGET" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'mget'");
                    return;
                }
                resp::array_header(out, nargs - 1);
                for k in &args[1..] {
                    match store.get_typed(k) {
                        // miss OR non-string -> nil; MGET never errors on WRONGTYPE
                        Some((crate::store::KIND_STR, _, v)) => resp::bulk(out, v),
                        _ => resp::null(out, resp3),
                    }
                }
            }
            b"MSET" => {
                if nargs < 3 || (nargs - 1) % 2 != 0 {
                    resp::error(out, "ERR wrong number of arguments for 'mset'");
                    return;
                }
                let mut i = 1;
                while i + 1 < nargs {
                    if !wrote(out, store.set(&args[i], &args[i + 1], 0)) {
                        return;
                    }
                    durable_log(&batcher, tier, &args[i], &args[i + 1]);
                    if persist_on {
                        stages.push((args[i].clone(), args[i + 1].clone(), 0, b's'));
                    }
                    i += 2;
                }
                resp::simple(out, "OK");
            }
            b"MSETNX" => {
                if nargs < 3 || (nargs - 1) % 2 != 0 {
                    resp::error(out, "ERR wrong number of arguments for 'msetnx'");
                    return;
                }
                // All-or-nothing: set only if NONE of the keys already exist.
                let mut any = false;
                let mut i = 1;
                while i + 1 < nargs {
                    if matches!(store.get(&args[i]), Lookup::Hit(_)) {
                        any = true;
                        break;
                    }
                    i += 2;
                }
                if any {
                    resp::integer(out, 0);
                } else {
                    let mut i = 1;
                    while i + 1 < nargs {
                        if !wrote(out, store.set(&args[i], &args[i + 1], 0)) {
                            return;
                        }
                        durable_log(&batcher, tier, &args[i], &args[i + 1]);
                        if persist_on {
                            stages.push((args[i].clone(), args[i + 1].clone(), 0, b's'));
                        }
                        i += 2;
                    }
                    resp::integer(out, 1);
                }
            }
            b"SETEX" | b"PSETEX" => {
                // SETEX key seconds value / PSETEX key millis value
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'setex'");
                    return;
                }
                let n: i64 = match std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()) {
                    Some(v) => v,
                    None => {
                        resp::error(out, "ERR value is not an integer or out of range");
                        return;
                    }
                };
                if n <= 0 {
                    resp::error(out, "ERR invalid expire time in 'setex' command");
                    return;
                }
                let ttl = if cmd == b"PSETEX" { n * 1_000 } else { n * 1_000_000 };
                if !wrote(out, store.set(&args[1], &args[3], ttl)) {
                    return;
                }
                resp::simple(out, "OK");
                durable_log(&batcher, tier, &args[1], &args[3]);
                if persist_on {
                    stages.push((args[1].clone(), args[3].clone(), now_micros() + ttl, b's'));
                }
            }
            b"GETDEL" => {
                if !check_string(&store, &args[1], out) {
                    return;
                }
                match store.get(&args[1]) {
                    Lookup::Hit(v) => {
                        let val = v.to_vec();
                        store.del(&args[1]);
                        resp::bulk(out, &val);
                        if persist_on {
                            stages.push((args[1].clone(), Vec::new(), DELETE_TOMBSTONE, b's'));
                        }
                    }
                    Lookup::Miss => resp::null(out, resp3),
                }
            }
            b"GETEX" => {
                // GETEX key [EX s | PX ms | EXAT ts | PXAT ms | PERSIST]
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let (val, cur_exp) = match store.get_typed(&args[1]) {
                    Some((_, exp, v)) => (v.to_vec(), exp),
                    None => {
                        resp::null(out, resp3);
                        return;
                    }
                };
                let mut new_exp: Option<i64> = None; // Some(0)=persist, Some(e)=set
                if nargs >= 2 {
                    let opt = args.get(2).map(|a| a.to_ascii_uppercase());
                    let n = || -> i64 {
                        args.get(3)
                            .and_then(|a| std::str::from_utf8(a).ok())
                            .and_then(|t| t.parse().ok())
                            .unwrap_or(0)
                    };
                    match opt.as_deref() {
                        Some(b"EX") => new_exp = Some(now_micros() + n() * 1_000_000),
                        Some(b"PX") => new_exp = Some(now_micros() + n() * 1_000),
                        Some(b"EXAT") => new_exp = Some(n() * 1_000_000),
                        Some(b"PXAT") => new_exp = Some(n() * 1_000),
                        Some(b"PERSIST") => new_exp = Some(0),
                        _ => {}
                    }
                }
                if let Some(e) = new_exp {
                    store.set_expiry(&args[1], e);
                    let _ = cur_exp;
                    // Persist the new expiry (GETEX EX/PX/EXAT/PXAT/PERSIST) so it
                    // survives crash recovery on the durable tiers.
                    if persist_on {
                        stages.push(stage_current_state(&store, &args[1]));
                    }
                }
                resp::bulk(out, &val);
            }
            b"APPEND" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'append'");
                    return;
                }
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let (mut buf, exp) = match store.get_typed(&args[1]) {
                    Some((_, e, v)) => (v.to_vec(), e),
                    None => (Vec::new(), 0),
                };
                buf.extend_from_slice(&args[2]);
                let ttl = if exp > 0 { (exp - now_micros()).max(1) } else { 0 };
                if !wrote(out, store.set(&args[1], &buf, ttl)) {
                    return;
                }
                resp::integer(out, buf.len() as i64);
                durable_log(&batcher, tier, &args[1], &buf);
                if persist_on {
                    stages.push((args[1].clone(), buf, if exp > 0 { exp } else { 0 }, b's'));
                }
            }
            b"STRLEN" => {
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let len = match store.get(&args[1]) {
                    Lookup::Hit(v) => v.len() as i64,
                    Lookup::Miss => 0,
                };
                resp::integer(out, len);
            }
            b"GETRANGE" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'getrange'");
                    return;
                }
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let v = match store.get(&args[1]) {
                    Lookup::Hit(v) => v.to_vec(),
                    Lookup::Miss => Vec::new(),
                };
                let (s, e) = (
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse::<i64>().ok()),
                    std::str::from_utf8(&args[3]).ok().and_then(|t| t.parse::<i64>().ok()),
                );
                match (s, e) {
                    (Some(s), Some(e)) => resp::bulk(out, substr(&v, s, e)),
                    _ => resp::error(out, "ERR value is not an integer or out of range"),
                }
            }
            b"SETRANGE" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'setrange'");
                    return;
                }
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let off: usize = match std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()) {
                    Some(v) => v,
                    None => {
                        resp::error(out, "ERR value is not an integer or out of range");
                        return;
                    }
                };
                let (mut buf, exp) = match store.get_typed(&args[1]) {
                    Some((_, e, v)) => (v.to_vec(), e),
                    None => (Vec::new(), 0),
                };
                if args[3].is_empty() {
                    // no-op write: just report current length
                    resp::integer(out, buf.len() as i64);
                    return;
                }
                let end = off + args[3].len();
                if buf.len() < end {
                    buf.resize(end, 0); // zero-pad the gap, as Redis does
                }
                buf[off..end].copy_from_slice(&args[3]);
                let ttl = if exp > 0 { (exp - now_micros()).max(1) } else { 0 };
                if !wrote(out, store.set(&args[1], &buf, ttl)) {
                    return;
                }
                resp::integer(out, buf.len() as i64);
                durable_log(&batcher, tier, &args[1], &buf);
                if persist_on {
                    stages.push((args[1].clone(), buf, if exp > 0 { exp } else { 0 }, b's'));
                }
            }
            b"INCRBYFLOAT" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'incrbyfloat'");
                    return;
                }
                if !check_string(&store, &args[1], out) {
                    return;
                }
                let (cur, exp) = match store.get_typed(&args[1]) {
                    Some((_, e, v)) => {
                        match std::str::from_utf8(v).ok().and_then(|t| t.trim().parse::<f64>().ok()) {
                            Some(f) => (f, e),
                            None => {
                                resp::error(out, "ERR value is not a valid float");
                                return;
                            }
                        }
                    }
                    None => (0.0, 0),
                };
                let by = match std::str::from_utf8(&args[2]).ok().and_then(|t| t.trim().parse::<f64>().ok()) {
                    Some(f) => f,
                    None => {
                        resp::error(out, "ERR value is not a valid float");
                        return;
                    }
                };
                let nv = cur + by;
                if !nv.is_finite() {
                    resp::error(out, "ERR increment would produce NaN or Infinity");
                    return;
                }
                let s = aggr::fmt_score(nv).into_bytes();
                let ttl = if exp > 0 { (exp - now_micros()).max(1) } else { 0 };
                if !wrote(out, store.set(&args[1], &s, ttl)) {
                    return;
                }
                resp::bulk(out, &s);
                durable_log(&batcher, tier, &args[1], &s);
                if persist_on {
                    stages.push((args[1].clone(), s, if exp > 0 { exp } else { 0 }, b's'));
                }
            }
            // ---- key-space management -----------------------------
            b"RENAME" | b"RENAMENX" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'rename'");
                    return;
                }
                let src = args[1].clone();
                let dst = args[2].clone();
                let (kind, exp, blob) = match store.get_typed(&src) {
                    Some((k, e, v)) => (k, e, v.to_vec()),
                    None => {
                        resp::error(out, "ERR no such key");
                        return;
                    }
                };
                let dst_exists = matches!(store.get_typed(&dst), Some(_));
                if cmd == b"RENAMENX" && (dst_exists || src == dst) {
                    // NX: refuse if the destination is already taken.
                    resp::integer(out, 0);
                    return;
                }
                if src == dst {
                    // RENAME onto itself is a no-op that still validates existence.
                    resp::simple(out, "OK");
                    return;
                }
                if !wrote(out, store.set_typed(&dst, &blob, remaining_ttl(exp), kind)) {
                    return;
                }
                store.del(&src);
                if persist_on {
                    stages.push((dst, blob, if exp > 0 { exp } else { 0 }, kind as u8));
                    stages.push((src, Vec::new(), DELETE_TOMBSTONE, b's'));
                }
                if cmd == b"RENAMENX" {
                    resp::integer(out, 1);
                } else {
                    resp::simple(out, "OK");
                }
            }
            b"COPY" => {
                // COPY source destination [REPLACE] [DB n]
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'copy'");
                    return;
                }
                let replace = args[3..]
                    .iter()
                    .any(|a| a.eq_ignore_ascii_case(b"REPLACE"));
                let (kind, exp, blob) = match store.get_typed(&args[1]) {
                    Some((k, e, v)) => (k, e, v.to_vec()),
                    None => {
                        resp::integer(out, 0);
                        return;
                    }
                };
                if !replace && matches!(store.get_typed(&args[2]), Some(_)) {
                    resp::integer(out, 0);
                    return;
                }
                if !wrote(out, store.set_typed(&args[2], &blob, remaining_ttl(exp), kind)) {
                    return;
                }
                if persist_on {
                    stages.push((args[2].clone(), blob, if exp > 0 { exp } else { 0 }, kind as u8));
                }
                resp::integer(out, 1);
            }
            b"TOUCH" => {
                // Count the keys that exist (a get refreshes the CLOCK ref bit).
                let mut count = 0i64;
                for a in &args[1..] {
                    if matches!(store.get(a), Lookup::Hit(_)) {
                        count += 1;
                    }
                }
                resp::integer(out, count);
            }
            b"RANDOMKEY" => {
                // Pick a live key, returned in its client-facing (unscoped) form.
                // Start the scan at a time-seeded position so repeated calls do not
                // always return the same key, wrapping to the head once if needed.
                let stride = store.num_partitions() as u64;
                let total = stride.max(1);
                let mut start = (now_micros() as u64).wrapping_mul(0x9E3779B97F4A7C15) % total;
                let mut chosen: Option<Vec<u8>> = None;
                for _ in 0..2 {
                    let mut cursor = start;
                    loop {
                        let (next, raw) = store.scan(cursor, 128);
                        for k in &raw {
                            let f: &[u8] = match &scan_prefix {
                                Some(pfx) => {
                                    if !k.starts_with(pfx.as_slice()) {
                                        continue;
                                    }
                                    &k[pfx.len()..]
                                }
                                None => k.as_slice(),
                            };
                            chosen = Some(f.to_vec());
                            break;
                        }
                        if chosen.is_some() || next == 0 {
                            break;
                        }
                        cursor = next;
                    }
                    if chosen.is_some() {
                        break;
                    }
                    start = 0; // second pass from the head to cover the wrap
                }
                match chosen {
                    Some(k) => resp::bulk(out, &k),
                    None => resp::null(out, resp3),
                }
            }
            b"EXPIRETIME" | b"PEXPIRETIME" => {
                if nargs != 2 {
                    resp::error(out, "ERR wrong number of arguments for 'expiretime'");
                    return;
                }
                match store.get_typed(&args[1]) {
                    None => resp::integer(out, -2),          // no such key
                    Some((_, 0, _)) => resp::integer(out, -1), // exists, no expiry
                    Some((_, exp, _)) => {
                        if cmd == b"PEXPIRETIME" {
                            resp::integer(out, exp / 1_000);
                        } else {
                            resp::integer(out, exp / 1_000_000);
                        }
                    }
                }
            }
            b"OBJECT" => {
                // OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ key  (+ HELP)
                let sub = args.get(1).map(|a| a.to_ascii_uppercase());
                match sub.as_deref() {
                    Some(b"HELP") => {
                        resp::array_header(out, 1);
                        resp::bulk(out, b"OBJECT ENCODING|REFCOUNT|IDLETIME|FREQ <key>");
                    }
                    Some(b"ENCODING") | Some(b"REFCOUNT") | Some(b"IDLETIME") | Some(b"FREQ")
                        if nargs >= 3 =>
                    {
                        match store.get_typed(&args[2]) {
                            None => resp::error(out, "ERR no such key"),
                            Some((kind, _, v)) => match sub.as_deref() {
                                Some(b"ENCODING") => {
                                    let enc: &[u8] = match kind {
                                        KIND_HASH => b"hashtable",
                                        k if k == crate::store::KIND_LIST => b"quicklist",
                                        k if k == crate::store::KIND_ZSET => b"skiplist",
                                        k if k == crate::store::KIND_SET => b"hashtable",
                                        // integer strings report "int" as Redis does
                                        _ if std::str::from_utf8(v)
                                            .ok()
                                            .and_then(|t| t.parse::<i64>().ok())
                                            .is_some() =>
                                        {
                                            b"int"
                                        }
                                        _ => b"embstr",
                                    };
                                    resp::bulk(out, enc);
                                }
                                Some(b"REFCOUNT") => resp::integer(out, 1),
                                _ => resp::integer(out, 0), // IDLETIME / FREQ
                            },
                        }
                    }
                    _ => resp::error(
                        out,
                        "ERR Unknown OBJECT subcommand or wrong number of arguments",
                    ),
                }
            }
            // ---- server / admin -----------------------------------
            b"ECHO" => {
                if nargs != 2 {
                    resp::error(out, "ERR wrong number of arguments for 'echo'");
                    return;
                }
                resp::bulk(out, &args[1]);
            }
            b"TIME" => {
                let us = now_micros();
                resp::array_header(out, 2);
                resp::bulk(out, (us / 1_000_000).to_string().as_bytes());
                resp::bulk(out, (us % 1_000_000).to_string().as_bytes());
            }
            b"INFO" => {
                let mut keys = 0u64;
                let mut used = 0u64;
                for p in 0..store.num_partitions() {
                    let st = store.stats(p);
                    keys += st.entries;
                    used += st.data_used;
                }
                // Clients decide whether to speak cluster from `redis_mode` /
                // `cluster_enabled`, so these must track slot routing exactly.
                let clustered = self.routing.is_some();
                let mode = if clustered { "cluster" } else { "standalone" };
                let cluster_enabled = u8::from(clustered);
                let body = format!(
                    "# Server\r\nredis_version:7.4.0\r\nredis_mode:{mode}\r\nos:Linux\r\narch_bits:64\r\n\
                     # Clients\r\nblocked_clients:0\r\n\
                     # Memory\r\nused_memory:{used}\r\nmaxmemory:0\r\n\
                     # Persistence\r\nloading:0\r\n\
                     # Replication\r\nrole:master\r\nconnected_slaves:0\r\n\
                     # Cluster\r\ncluster_enabled:{cluster_enabled}\r\n\
                     # Keyspace\r\ndb0:keys={keys},expires=0,avg_ttl=0\r\n"
                );
                resp::bulk(out, body.as_bytes());
            }
            // ---- cluster topology discovery ----
            // Cluster clients bootstrap by asking for the slot map before they
            // will route anything, so MOVED alone is not enough to make them
            // work: without these they fail at connect. Only served when slot
            // routing is on (persisted, workers > 1) — advertising a slot map a
            // single-worker or ephemeral deployment does not enforce would be a
            // lie the client then routes by.
            b"CLUSTER" => {
                let sub = args.get(1).map(|a| a.to_ascii_uppercase()).unwrap_or_default();
                let r = match &self.routing {
                    Some(r) => r,
                    None => {
                        // Redis's own wording when built without cluster support.
                        match sub.as_slice() {
                            b"KEYSLOT" if nargs >= 3 => {
                                resp::integer(out, crc16::key_slot(&args[2]) as i64)
                            }
                            b"INFO" => resp::bulk(
                                out,
                                b"cluster_enabled:0\r\ncluster_state:ok\r\ncluster_slots_assigned:0\r\n",
                            ),
                            _ => resp::error(out, "ERR This instance has cluster support disabled"),
                        }
                        return;
                    }
                };
                let n = r.nworkers;
                match sub.as_slice() {
                    // [[start, end, [ip, port, id, metadata]], …] — one range per
                    // worker, no replicas (a worker's standby is Postgres's own).
                    b"SLOTS" => {
                        resp::array_header(out, n);
                        for w in 0..n {
                            let (lo, hi) = crc16::slot_range(w, n);
                            let (ip, port) = r.host_port(w);
                            resp::array_header(out, 3);
                            resp::integer(out, lo as i64);
                            resp::integer(out, hi as i64 - 1); // CLUSTER SLOTS is inclusive
                            resp::array_header(out, 4);
                            resp::bulk(out, ip.as_bytes());
                            resp::integer(out, port as i64);
                            resp::bulk(out, r.node_id(w).as_bytes());
                            resp::array_header(out, 0);
                        }
                    }
                    b"SHARDS" => {
                        resp::array_header(out, n);
                        for w in 0..n {
                            let (lo, hi) = crc16::slot_range(w, n);
                            let (ip, port) = r.host_port(w);
                            resp::map_header(out, 2, resp3);
                            resp::bulk(out, b"slots");
                            resp::array_header(out, 2);
                            resp::integer(out, lo as i64);
                            resp::integer(out, hi as i64 - 1);
                            resp::bulk(out, b"nodes");
                            resp::array_header(out, 1);
                            resp::map_header(out, 7, resp3);
                            resp::bulk(out, b"id");
                            resp::bulk(out, r.node_id(w).as_bytes());
                            resp::bulk(out, b"port");
                            resp::integer(out, port as i64);
                            resp::bulk(out, b"ip");
                            resp::bulk(out, ip.as_bytes());
                            resp::bulk(out, b"endpoint");
                            resp::bulk(out, ip.as_bytes());
                            resp::bulk(out, b"role");
                            resp::bulk(out, b"master");
                            resp::bulk(out, b"replication-offset");
                            resp::integer(out, 0);
                            resp::bulk(out, b"health");
                            resp::bulk(out, b"online");
                        }
                    }
                    // nodes.conf line format — lettuce and Jedis parse this one.
                    b"NODES" => {
                        let mut body = String::new();
                        for w in 0..n {
                            let (lo, hi) = crc16::slot_range(w, n);
                            let (ip, port) = r.host_port(w);
                            let flags = if w == r.index { "myself,master" } else { "master" };
                            body.push_str(&format!(
                                "{} {}:{}@{} {} - 0 0 {} connected {}-{}\n",
                                r.node_id(w),
                                ip,
                                port,
                                port as u32 + 10_000, // conventional cluster bus port
                                flags,
                                w,
                                lo,
                                hi - 1,
                            ));
                        }
                        resp::bulk(out, body.as_bytes());
                    }
                    b"MYID" => resp::bulk(out, r.node_id(r.index).as_bytes()),
                    b"INFO" => {
                        let body = format!(
                            "cluster_enabled:1\r\ncluster_state:ok\r\ncluster_slots_assigned:{}\r\n\
                             cluster_slots_ok:{}\r\ncluster_slots_pfail:0\r\ncluster_slots_fail:0\r\n\
                             cluster_known_nodes:{n}\r\ncluster_size:{n}\r\ncluster_current_epoch:{n}\r\n\
                             cluster_my_epoch:{}\r\n",
                            crc16::NUM_SLOTS,
                            crc16::NUM_SLOTS,
                            r.index,
                        );
                        resp::bulk(out, body.as_bytes());
                    }
                    b"KEYSLOT" if nargs >= 3 => {
                        resp::integer(out, crc16::key_slot(&args[2]) as i64)
                    }
                    b"COUNTKEYSINSLOT" => resp::integer(out, 0),
                    // Topology is fixed by pg_keyspace.workers, so there is no
                    // resharding surface to expose.
                    _ => resp::error(
                        out,
                        "ERR Unknown CLUSTER subcommand or wrong number of arguments",
                    ),
                }
            }
            b"MEMORY" => {
                match args.get(1).map(|a| a.to_ascii_uppercase()).as_deref() {
                    Some(b"USAGE") if nargs >= 3 => match store.get_typed(&args[2]) {
                        None => resp::null(out, resp3),
                        // rough estimate: value + key bytes + fixed entry overhead
                        Some((_, _, v)) => {
                            resp::integer(out, (v.len() + args[2].len() + 64) as i64)
                        }
                    },
                    Some(b"DOCTOR") => resp::bulk(out, b"Sam, I detected a few issues in this Redis instance memory implants:\n\n * No issues detected.\n"),
                    _ => resp::error(
                        out,
                        "ERR Unknown MEMORY subcommand or wrong number of arguments",
                    ),
                }
            }
            b"DEBUG" => {
                // Enough of DEBUG for tooling/tests; nothing mutates state.
                match args.get(1).map(|a| a.to_ascii_uppercase()).as_deref() {
                    Some(b"OBJECT") if nargs >= 3 => match store.get_typed(&args[2]) {
                        None => resp::error(out, "ERR no such key"),
                        Some((_, _, v)) => resp::simple(
                            out,
                            &format!(
                                "Value at:0x0 refcount:1 encoding:raw serializedlength:{} lru:0 lru_seconds_idle:0",
                                v.len()
                            ),
                        ),
                    },
                    // DEBUG SLEEP would block the single-threaded loop, so it is a
                    // no-op OK rather than an actual stall.
                    _ => resp::simple(out, "OK"),
                }
            }
            b"FLUSHALL" | b"FLUSHDB" => {
                // Collect first, then delete, so we don't mutate mid-scan. A scoped
                // (tenant) connection only clears its own prefix; an unscoped/exempt
                // connection clears everything.
                let mut victims: Vec<Vec<u8>> = Vec::new();
                let mut cursor = 0u64;
                loop {
                    let (next, raw) = store.scan(cursor, 512);
                    for k in raw {
                        match &scan_prefix {
                            Some(pfx) if !k.starts_with(pfx.as_slice()) => continue,
                            _ => victims.push(k),
                        }
                    }
                    if next == 0 {
                        break;
                    }
                    cursor = next;
                }
                for k in &victims {
                    store.del(k);
                    if persist_on {
                        if let Some(sa) =
                            shard_push(&self.producers, k, b"", DELETE_TOMBSTONE, b's')
                        {
                            acks.push(sa);
                        }
                    }
                }
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::simple(out, "OK");
            }
            // ---- TYPE + hashes ------------------------------------
            b"TYPE" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'type'");
                } else {
                    let t = match store.get_typed(&args[1]) {
                        None => "none",
                        Some((KIND_HASH, _, _)) => "hash",
                        Some((k, _, _)) if k == crate::store::KIND_LIST => "list",
                        Some((k, _, _)) if k == crate::store::KIND_ZSET => "zset",
                        Some((k, _, _)) if k == crate::store::KIND_SET => "set",
                        Some(_) => "string",
                    };
                    resp::simple(out, t);
                }
            }
            b"HSET" | b"HMSET" => {
                // HSET key field value [field value ...]
                if nargs < 4 || (nargs - 2) % 2 != 0 {
                    resp::error(out, "ERR wrong number of arguments for 'hset'");
                    return;
                }
                let (mut h, exp) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut added = 0i64;
                let mut i = 2;
                while i + 1 < nargs {
                    if h.set(&args[i], &args[i + 1]) {
                        added += 1;
                    }
                    i += 2;
                }
                if !wrote(out, store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH)) {
                    return;
                }
                if cmd == b"HMSET" {
                    resp::simple(out, "OK");
                } else {
                    resp::integer(out, added);
                }
            }
            b"HSETNX" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'hsetnx'");
                    return;
                }
                let (mut h, exp) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if h.get(&args[2]).is_some() {
                    resp::integer(out, 0);
                } else {
                    h.set(&args[2], &args[3]);
                    if !wrote(out, store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH)) {
                        return;
                    }
                    resp::integer(out, 1);
                }
            }
            b"HGET" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'hget'");
                    return;
                }
                let raw = match hash_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                match raw.and_then(|b| aggr::hash_probe(b, &args[2])) {
                    Some(v) => resp::bulk(out, v),
                    None => resp::null(out, resp3),
                }
            }
            b"HMGET" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'hmget'");
                    return;
                }
                let raw = match hash_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::array_header(out, nargs - 2);
                for f in &args[2..] {
                    match raw.and_then(|b| aggr::hash_probe(b, f)) {
                        Some(v) => resp::bulk(out, v),
                        None => resp::null(out, resp3),
                    }
                }
            }
            b"HDEL" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'hdel'");
                    return;
                }
                let (mut h, exp) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut removed = 0i64;
                for f in &args[2..] {
                    if h.del(f) {
                        removed += 1;
                    }
                }
                if h.is_empty() {
                    store.del(&args[1]); // Redis drops an emptied hash
                } else {
                    if !wrote(out, store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH)) {
                        return;
                    }
                }
                resp::integer(out, removed);
            }
            b"HGETALL" => {
                let (h, _) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::map_header(out, h.len(), resp3); // RESP3 map, RESP2 flat array
                for (f, v) in &h.entries {
                    resp::bulk(out, f);
                    resp::bulk(out, v);
                }
            }
            b"HKEYS" | b"HVALS" => {
                let (h, _) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::array_header(out, h.len());
                for (f, v) in &h.entries {
                    resp::bulk(out, if cmd == b"HKEYS" { f } else { v });
                }
            }
            b"HLEN" => {
                let raw = match hash_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(out, raw.map(aggr::hash_count).unwrap_or(0) as i64);
            }
            b"HEXISTS" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'hexists'");
                    return;
                }
                let raw = match hash_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(out, i64::from(raw.and_then(|b| aggr::hash_probe(b, &args[2])).is_some()));
            }
            b"HSTRLEN" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'hstrlen'");
                    return;
                }
                let raw = match hash_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(
                    out,
                    raw.and_then(|b| aggr::hash_probe(b, &args[2])).map(|v| v.len()).unwrap_or(0) as i64,
                );
            }
            b"HINCRBY" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'hincrby'");
                    return;
                }
                let by: i64 = match std::str::from_utf8(&args[3]).ok().and_then(|s| s.parse().ok()) {
                    Some(n) => n,
                    None => {
                        resp::error(out, "ERR value is not an integer or out of range");
                        return;
                    }
                };
                let (mut h, exp) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let cur: i64 = match h.get(&args[2]) {
                    None => 0,
                    Some(v) => match std::str::from_utf8(v).ok().and_then(|s| s.parse().ok()) {
                        Some(n) => n,
                        None => {
                            resp::error(out, "ERR hash value is not an integer");
                            return;
                        }
                    },
                };
                let next = cur + by;
                h.set(&args[2], &itoa(next));
                if !wrote(out, store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH)) {
                    return;
                }
                resp::integer(out, next);
            }
            // ---- lists --------------------------------------------
            b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let xonly = cmd == b"LPUSHX" || cmd == b"RPUSHX";
                let left = cmd == b"LPUSH" || cmd == b"LPUSHX";
                let (mut l, exp) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if xonly && l.is_empty() {
                    resp::integer(out, 0); // *PUSHX no-ops on a missing key
                    return;
                }
                for v in &args[2..] {
                    if left {
                        l.lpush(v);
                    } else {
                        l.rpush(v);
                    }
                }
                let n = l.len() as i64;
                if !save_list(&store, &args[1], &l, exp, out) {
                    return;
                }
                resp::integer(out, n);
            }
            b"LPOP" | b"RPOP" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                // optional count argument (Redis 6.2+): returns an array
                let count: Option<i64> = if nargs >= 3 {
                    match std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()) {
                        Some(n) if n >= 0 => Some(n),
                        _ => {
                            resp::error(out, "ERR value is out of range, must be positive");
                            return;
                        }
                    }
                } else {
                    None
                };
                let (mut l, exp) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let left = cmd == b"LPOP";
                match count {
                    None => {
                        let popped = if left { l.lpop() } else { l.rpop() };
                        match popped {
                            Some(v) => resp::bulk(out, &v),
                            None => resp::null(out, resp3),
                        }
                    }
                    Some(c) => {
                        if l.is_empty() {
                            resp::null(out, resp3);
                            return;
                        }
                        let mut taken = Vec::new();
                        for _ in 0..c {
                            match if left { l.lpop() } else { l.rpop() } {
                                Some(v) => taken.push(v),
                                None => break,
                            }
                        }
                        resp::array_header(out, taken.len());
                        for v in &taken {
                            resp::bulk(out, v);
                        }
                    }
                }
                if !save_list(&store, &args[1], &l, exp, out) {
                    return;
                }
            }
            b"LLEN" => {
                let raw = match list_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(out, raw.map(aggr::list_len).unwrap_or(0) as i64);
            }
            b"LINDEX" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'lindex'");
                    return;
                }
                let i: i64 = std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let raw = match list_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let hit = raw.and_then(|b| {
                    let n = aggr::list_len(b) as i64;
                    let idx = if i < 0 { n + i } else { i };
                    if idx >= 0 && idx < n {
                        aggr::list_get(b, idx as usize)
                    } else {
                        None
                    }
                });
                match hit {
                    Some(v) => resp::bulk(out, v),
                    None => resp::null(out, resp3),
                }
            }
            b"LRANGE" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'lrange'");
                    return;
                }
                let start: i64 = std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let stop: i64 = std::str::from_utf8(&args[3]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let raw = match list_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                match raw {
                    None => resp::array_header(out, 0),
                    Some(b) => {
                        let (lo, hi) = aggr::rank_bounds(aggr::list_len(b), start, stop);
                        resp::array_header(out, hi - lo);
                        for idx in lo..hi {
                            if let Some(v) = aggr::list_get(b, idx) {
                                resp::bulk(out, v);
                            }
                        }
                    }
                }
            }
            b"LSET" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'lset'");
                    return;
                }
                let i: i64 = std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let (mut l, exp) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if l.is_empty() {
                    resp::error(out, "ERR no such key");
                    return;
                }
                match l.real_index(i) {
                    Some(idx) => {
                        l.items[idx] = args[3].clone();
                        if !save_list(&store, &args[1], &l, exp, out) {
                            return;
                        }
                        resp::simple(out, "OK");
                    }
                    None => resp::error(out, "ERR index out of range"),
                }
            }
            b"LTRIM" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'ltrim'");
                    return;
                }
                let start: i64 = std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let stop: i64 = std::str::from_utf8(&args[3]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let (mut l, exp) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let (lo, hi) = l.range_bounds(start, stop);
                l.items = l.items[lo..hi].to_vec();
                if !save_list(&store, &args[1], &l, exp, out) {
                    return;
                }
                resp::simple(out, "OK");
            }
            // ---- sorted sets --------------------------------------
            b"ZADD" => {
                // ZADD key [NX|XX] [CH] score member [score member ...]
                if nargs < 4 {
                    resp::error(out, "ERR wrong number of arguments for 'zadd'");
                    return;
                }
                let mut i = 2;
                let (mut nx, mut xx, mut ch) = (false, false, false);
                while i < nargs {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"NX" => { nx = true; i += 1; }
                        b"XX" => { xx = true; i += 1; }
                        b"CH" => { ch = true; i += 1; }
                        _ => break,
                    }
                }
                if nx && xx {
                    resp::error(out, "ERR XX and NX options at the same time are not compatible");
                    return;
                }
                if i >= nargs || (nargs - i) % 2 != 0 {
                    resp::error(out, "ERR syntax error");
                    return;
                }
                // validate all scores first (atomic-ish)
                let mut pairs: Vec<(f64, &[u8])> = Vec::new();
                let mut j = i;
                while j + 1 < nargs {
                    match aggr::parse_score(&args[j]) {
                        Some(s) => pairs.push((s, &args[j + 1])),
                        None => {
                            resp::error(out, "ERR value is not a valid float");
                            return;
                        }
                    }
                    j += 2;
                }
                let (mut z, exp) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let (mut added, mut changed) = (0i64, 0i64);
                for (s, m) in pairs {
                    let exists = z.score(m).is_some();
                    if (nx && exists) || (xx && !exists) {
                        continue;
                    }
                    let (was_added, was_changed) = z.add(m, s);
                    if was_added {
                        added += 1;
                    }
                    if was_changed {
                        changed += 1;
                    }
                }
                if !save_zset(&store, &args[1], &z, exp, out) {
                    return;
                }
                resp::integer(out, if ch { changed } else { added });
            }
            b"ZSCORE" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'zscore'");
                    return;
                }
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                match raw.and_then(|b| aggr::zset_score(b, &args[2])) {
                    Some(s) => resp::double(out, &aggr::fmt_score(s), resp3),
                    None => resp::null(out, resp3),
                }
            }
            b"ZMSCORE" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'zmscore'");
                    return;
                }
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::array_header(out, nargs - 2);
                for m in &args[2..] {
                    match raw.and_then(|b| aggr::zset_score(b, m)) {
                        Some(s) => resp::double(out, &aggr::fmt_score(s), resp3),
                        None => resp::null(out, resp3),
                    }
                }
            }
            b"ZCARD" => {
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(out, raw.map(aggr::zset_card).unwrap_or(0) as i64);
            }
            b"ZREM" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'zrem'");
                    return;
                }
                let (mut z, exp) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut removed = 0i64;
                for m in &args[2..] {
                    if z.remove(m) {
                        removed += 1;
                    }
                }
                if !save_zset(&store, &args[1], &z, exp, out) {
                    return;
                }
                resp::integer(out, removed);
            }
            b"ZINCRBY" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'zincrby'");
                    return;
                }
                let by = match aggr::parse_score(&args[2]) {
                    Some(s) => s,
                    None => {
                        resp::error(out, "ERR value is not a valid float");
                        return;
                    }
                };
                let (mut z, exp) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                // Checked before the member is touched: an increment that
                // would refuse must leave the set exactly as it found it.
                let next = match aggr::incr_score(z.score(&args[3]).unwrap_or(0.0), by) {
                    Some(v) => v,
                    None => {
                        resp::error(out, "ERR resulting score is not a number (NaN)");
                        return;
                    }
                };
                z.add(&args[3], next);
                if !save_zset(&store, &args[1], &z, exp, out) {
                    return;
                }
                resp::double(out, &aggr::fmt_score(next), resp3);
            }
            b"ZRANK" | b"ZREVRANK" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let (card, rank) = match raw {
                    Some(b) => (aggr::zset_card(b), aggr::zset_rank(b, &args[2])),
                    None => (0, None),
                };
                match rank {
                    Some(r) => {
                        let r = if cmd == b"ZREVRANK" { card - 1 - r } else { r };
                        resp::integer(out, r as i64);
                    }
                    None => resp::null(out, resp3),
                }
            }
            b"ZRANGE" | b"ZREVRANGE" => {
                if nargs < 4 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let start: i64 = std::str::from_utf8(&args[2]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let stop: i64 = std::str::from_utf8(&args[3]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                let withscores = args[4..].iter().any(|a| a.eq_ignore_ascii_case(b"WITHSCORES"));
                let rev = cmd == b"ZREVRANGE"
                    || args[4..].iter().any(|a| a.eq_ignore_ascii_case(b"REV"));
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let items: Vec<(Vec<u8>, f64)> = match raw {
                    None => Vec::new(),
                    Some(b) => {
                        let card = aggr::zset_card(b);
                        let (lo, hi) = aggr::rank_bounds(card, start, stop);
                        if rev {
                            // reversed[lo..hi] maps to ascending [card-hi, card-lo), reversed
                            let mut v = aggr::zset_range_by_rank(b, card - hi, card - lo);
                            v.reverse();
                            v
                        } else {
                            aggr::zset_range_by_rank(b, lo, hi)
                        }
                    }
                };
                if withscores {
                    reply_scored(out, &items, resp3, resp3);
                } else {
                    resp::array_header(out, items.len());
                    for (m, _) in &items {
                        resp::bulk(out, m);
                    }
                }
            }
            b"ZRANGEBYSCORE" | b"ZREVRANGEBYSCORE" => {
                if nargs < 4 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let rev = cmd == b"ZREVRANGEBYSCORE";
                // for REV, args are (max min); normalise to (min, max)
                let (minb, maxb) = if rev { (&args[3], &args[2]) } else { (&args[2], &args[3]) };
                let (min, max) = match (aggr::ScoreBound::parse(minb), aggr::ScoreBound::parse(maxb)) {
                    (Some(a), Some(b)) => (a, b),
                    _ => {
                        resp::error(out, "ERR min or max is not a float");
                        return;
                    }
                };
                let withscores = args[4..].iter().any(|a| a.eq_ignore_ascii_case(b"WITHSCORES"));
                // optional LIMIT offset count
                let mut offset = 0usize;
                let mut count: Option<usize> = None;
                for w in 4..nargs {
                    if args[w].eq_ignore_ascii_case(b"LIMIT") && w + 2 < nargs {
                        offset = std::str::from_utf8(&args[w + 1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                        let c: i64 = std::str::from_utf8(&args[w + 2]).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);
                        count = if c < 0 { None } else { Some(c as usize) };
                    }
                }
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut items = match raw {
                    Some(b) => aggr::zset_range_by_score(b, &min, &max),
                    None => Vec::new(),
                };
                if rev {
                    items.reverse();
                }
                let items: Vec<_> = items.into_iter().skip(offset).take(count.unwrap_or(usize::MAX)).collect();
                if withscores {
                    reply_scored(out, &items, resp3, resp3);
                } else {
                    resp::array_header(out, items.len());
                    for (m, _) in &items {
                        resp::bulk(out, m);
                    }
                }
            }
            b"ZCOUNT" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'zcount'");
                    return;
                }
                let (min, max) = match (aggr::ScoreBound::parse(&args[2]), aggr::ScoreBound::parse(&args[3])) {
                    (Some(a), Some(b)) => (a, b),
                    _ => {
                        resp::error(out, "ERR min or max is not a float");
                        return;
                    }
                };
                let raw = match zset_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(out, raw.map(|b| aggr::zset_count(b, &min, &max)).unwrap_or(0) as i64);
            }
            // ---- aggregate gaps: hash ------------------------------
            b"HINCRBYFLOAT" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'hincrbyfloat'");
                    return;
                }
                let by = match aggr::parse_score(&args[3]) {
                    Some(f) => f,
                    None => {
                        resp::error(out, "ERR value is not a valid float");
                        return;
                    }
                };
                let (mut h, exp) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let cur = match h.get(&args[2]) {
                    None => 0.0,
                    Some(v) => match aggr::parse_score(v) {
                        Some(f) => f,
                        None => {
                            resp::error(out, "ERR hash value is not a float");
                            return;
                        }
                    },
                };
                let nv = cur + by;
                if !nv.is_finite() {
                    resp::error(out, "ERR increment would produce NaN or Infinity");
                    return;
                }
                let s = aggr::fmt_score(nv);
                h.set(&args[2], s.as_bytes());
                if !wrote(out, store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH)) {
                    return;
                }
                resp::bulk(out, s.as_bytes());
            }
            b"HRANDFIELD" => {
                // HRANDFIELD key [count [WITHVALUES]]
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'hrandfield'");
                    return;
                }
                let (h, _) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if nargs < 3 {
                    if h.is_empty() {
                        resp::null(out, resp3);
                    } else {
                        let i = (rng_next(&mut rand_seed()) as usize) % h.len();
                        resp::bulk(out, &h.entries[i].0);
                    }
                    return;
                }
                let count: i64 =
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(0);
                let withvals = nargs >= 4 && args[3].eq_ignore_ascii_case(b"WITHVALUES");
                if h.is_empty() || count == 0 {
                    resp::array_header(out, 0);
                    return;
                }
                let idx = pick_indices(h.len(), count, rand_seed());
                if withvals && resp3 {
                    // RESP3 pairs each field with its value as a two-element array.
                    resp::array_header(out, idx.len());
                    for i in idx {
                        resp::array_header(out, 2);
                        resp::bulk(out, &h.entries[i].0);
                        resp::bulk(out, &h.entries[i].1);
                    }
                } else {
                    resp::array_header(out, idx.len() * if withvals { 2 } else { 1 });
                    for i in idx {
                        resp::bulk(out, &h.entries[i].0);
                        if withvals {
                            resp::bulk(out, &h.entries[i].1);
                        }
                    }
                }
            }
            // ---- aggregate gaps: list ------------------------------
            b"LINSERT" => {
                // LINSERT key BEFORE|AFTER pivot element
                if nargs != 5 {
                    resp::error(out, "ERR wrong number of arguments for 'linsert'");
                    return;
                }
                let before = match args[2].to_ascii_uppercase().as_slice() {
                    b"BEFORE" => true,
                    b"AFTER" => false,
                    _ => {
                        resp::error(out, "ERR syntax error");
                        return;
                    }
                };
                let (mut l, exp) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if l.is_empty() {
                    resp::integer(out, 0); // key does not exist
                    return;
                }
                match l.items.iter().position(|v| v == &args[3]) {
                    Some(idx) => {
                        let at = if before { idx } else { idx + 1 };
                        l.items.insert(at, args[4].clone());
                        let n = l.len() as i64;
                        if !save_list(&store, &args[1], &l, exp, out) {
                            return;
                        }
                        resp::integer(out, n);
                    }
                    None => resp::integer(out, -1), // pivot not found
                }
            }
            b"LREM" => {
                // LREM key count element
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'lrem'");
                    return;
                }
                let count: i64 =
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(0);
                let (mut l, exp) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut removed = 0i64;
                if count >= 0 {
                    // head → tail; count == 0 removes every match
                    let mut i = 0;
                    while i < l.items.len() {
                        if l.items[i] == args[3] {
                            l.items.remove(i);
                            removed += 1;
                            if count != 0 && removed == count {
                                break;
                            }
                        } else {
                            i += 1;
                        }
                    }
                } else {
                    // tail → head, up to |count|
                    let lim = -count;
                    let mut i = l.items.len();
                    while i > 0 {
                        i -= 1;
                        if l.items[i] == args[3] {
                            l.items.remove(i);
                            removed += 1;
                            if removed == lim {
                                break;
                            }
                        }
                    }
                }
                if !save_list(&store, &args[1], &l, exp, out) {
                    return;
                }
                resp::integer(out, removed);
            }
            b"LPOS" => {
                // LPOS key element [RANK rank] [COUNT num]
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'lpos'");
                    return;
                }
                let mut rank: i64 = 1;
                let mut count: Option<i64> = None;
                let mut i = 3;
                while i + 1 < nargs {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"RANK" => {
                            rank = std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|t| t.parse().ok())
                                .unwrap_or(1);
                        }
                        b"COUNT" => {
                            count = std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|t| t.parse().ok());
                        }
                        _ => {}
                    }
                    i += 2;
                }
                if rank == 0 {
                    resp::error(out, "ERR RANK can't be zero");
                    return;
                }
                let (l, _) = match load_list(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                // Gather matching indices in the search direction.
                let mut hits: Vec<i64> = Vec::new();
                let n = l.items.len();
                let matches: Vec<usize> =
                    (0..n).filter(|&j| l.items[j] == args[2]).collect();
                let ordered: Vec<usize> = if rank > 0 {
                    matches.clone()
                } else {
                    matches.iter().rev().cloned().collect()
                };
                let skip = (rank.unsigned_abs() as usize).saturating_sub(1);
                for &m in ordered.iter().skip(skip) {
                    hits.push(m as i64);
                    if let Some(c) = count {
                        if c != 0 && hits.len() as i64 >= c {
                            break;
                        }
                    } else {
                        break; // no COUNT: first match only
                    }
                }
                match count {
                    None => match hits.first() {
                        Some(&p) => resp::integer(out, p),
                        None => resp::null(out, resp3),
                    },
                    Some(_) => {
                        resp::array_header(out, hits.len());
                        for p in hits {
                            resp::integer(out, p);
                        }
                    }
                }
            }
            b"LMOVE" | b"RPOPLPUSH" => {
                // RPOPLPUSH src dst == LMOVE src dst RIGHT LEFT
                let (from_left, to_left) = if cmd == b"RPOPLPUSH" {
                    if nargs != 3 {
                        resp::error(out, "ERR wrong number of arguments for 'rpoplpush'");
                        return;
                    }
                    (false, true)
                } else {
                    if nargs != 5 {
                        resp::error(out, "ERR wrong number of arguments for 'lmove'");
                        return;
                    }
                    let f = match args[3].to_ascii_uppercase().as_slice() {
                        b"LEFT" => true,
                        b"RIGHT" => false,
                        _ => {
                            resp::error(out, "ERR syntax error");
                            return;
                        }
                    };
                    let t = match args[4].to_ascii_uppercase().as_slice() {
                        b"LEFT" => true,
                        b"RIGHT" => false,
                        _ => {
                            resp::error(out, "ERR syntax error");
                            return;
                        }
                    };
                    (f, t)
                };
                let same = args[1] == args[2];
                if same {
                    let (mut l, exp) = match load_list(&store, &args[1], out) {
                        Some(x) => x,
                        None => return,
                    };
                    let val = if from_left { l.lpop() } else { l.rpop() };
                    match val {
                        None => {
                            resp::null(out, resp3);
                            return;
                        }
                        Some(v) => {
                            if to_left {
                                l.lpush(&v);
                            } else {
                                l.rpush(&v);
                            }
                            if !save_list(&store, &args[1], &l, exp, out) {
                                return;
                            }
                            resp::bulk(out, &v);
                        }
                    }
                } else {
                    // Validate both types before mutating either side.
                    let (mut src, sexp) = match load_list(&store, &args[1], out) {
                        Some(x) => x,
                        None => return,
                    };
                    let (mut dst, dexp) = match load_list(&store, &args[2], out) {
                        Some(x) => x,
                        None => return,
                    };
                    let val = if from_left { src.lpop() } else { src.rpop() };
                    match val {
                        None => {
                            resp::null(out, resp3);
                            return;
                        }
                        Some(v) => {
                            if to_left {
                                dst.lpush(&v);
                            } else {
                                dst.rpush(&v);
                            }
                            if !save_list(&store, &args[1], &src, sexp, out) {
                                return;
                            }
                            if !save_list(&store, &args[2], &dst, dexp, out) {
                                return;
                            }
                            resp::bulk(out, &v);
                        }
                    }
                }
                // Two-key write: stage both keys' final state (auto-stage only
                // covers args[1]).
                if persist_on {
                    for k in [&args[1], &args[2]] {
                        stages.push(match store.get_typed(k) {
                            Some((kind, exp, blob)) => (k.clone(), blob.to_vec(), exp, kind as u8),
                            None => (k.clone(), Vec::new(), DELETE_TOMBSTONE, b's'),
                        });
                    }
                }
            }
            // ---- aggregate gaps: sorted set ------------------------
            b"ZPOPMIN" | b"ZPOPMAX" => {
                // key [count]
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'zpopmin'");
                    return;
                }
                let count_given = nargs >= 3;
                let count: usize = if count_given {
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(1)
                } else {
                    1
                };
                let (mut z, exp) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let sorted = z.sorted(); // ascending
                let take = count.min(sorted.len());
                let chosen: Vec<(Vec<u8>, f64)> = if cmd == b"ZPOPMIN" {
                    sorted[..take].to_vec()
                } else {
                    sorted[sorted.len() - take..].iter().rev().cloned().collect()
                };
                for (m, _) in &chosen {
                    z.remove(m);
                }
                if !save_zset(&store, &args[1], &z, exp, out) {
                    return;
                }
                // Valkey pairs the counted form under RESP3; the bare form stays flat.
                reply_scored(out, &chosen, resp3 && count_given, resp3);
            }
            b"ZRANDMEMBER" => {
                // ZRANDMEMBER key [count [WITHSCORES]]
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'zrandmember'");
                    return;
                }
                let (z, _) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if nargs < 3 {
                    if z.is_empty() {
                        resp::null(out, resp3);
                    } else {
                        let i = (rng_next(&mut rand_seed()) as usize) % z.len();
                        resp::bulk(out, &z.members[i].0);
                    }
                    return;
                }
                let count: i64 =
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(0);
                let withscores = nargs >= 4 && args[3].eq_ignore_ascii_case(b"WITHSCORES");
                if z.is_empty() || count == 0 {
                    resp::array_header(out, 0);
                    return;
                }
                let idx = pick_indices(z.len(), count, rand_seed());
                if withscores {
                    let picked: Vec<(Vec<u8>, f64)> =
                        idx.iter().map(|&i| z.members[i].clone()).collect();
                    reply_scored(out, &picked, resp3, resp3);
                } else {
                    resp::array_header(out, idx.len());
                    for i in idx {
                        resp::bulk(out, &z.members[i].0);
                    }
                }
            }
            // ---- sets ----------------------------------------------
            b"SADD" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'sadd'");
                    return;
                }
                let (mut s, exp) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut added = 0i64;
                for m in &args[2..] {
                    if s.add(m) {
                        added += 1;
                    }
                }
                if !save_set(&store, &args[1], &s, exp, out) {
                    return;
                }
                resp::integer(out, added);
            }
            b"SREM" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'srem'");
                    return;
                }
                let (mut s, exp) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut removed = 0i64;
                for m in &args[2..] {
                    if s.remove(m) {
                        removed += 1;
                    }
                }
                if !save_set(&store, &args[1], &s, exp, out) {
                    return;
                }
                resp::integer(out, removed);
            }
            b"SCARD" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'scard'");
                    return;
                }
                let raw = match set_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::integer(out, raw.map(aggr::set_card).unwrap_or(0) as i64);
            }
            b"SISMEMBER" => {
                if nargs != 3 {
                    resp::error(out, "ERR wrong number of arguments for 'sismember'");
                    return;
                }
                let raw = match set_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let hit = raw.map(|b| aggr::set_contains(b, &args[2])).unwrap_or(false);
                resp::integer(out, if hit { 1 } else { 0 });
            }
            b"SMISMEMBER" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'smismember'");
                    return;
                }
                let raw = match set_raw(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::array_header(out, nargs - 2);
                for m in &args[2..] {
                    let hit = raw.map(|b| aggr::set_contains(b, m)).unwrap_or(false);
                    resp::integer(out, if hit { 1 } else { 0 });
                }
            }
            b"SMEMBERS" => {
                if nargs != 2 {
                    resp::error(out, "ERR wrong number of arguments for 'smembers'");
                    return;
                }
                let (s, _) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::set_header(out, s.len(), resp3); // RESP3 set, RESP2 array
                for m in &s.members {
                    resp::bulk(out, m);
                }
            }
            b"SPOP" => {
                // SPOP key [count]
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'spop'");
                    return;
                }
                let (mut s, exp) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if nargs < 3 {
                    if s.is_empty() {
                        resp::null(out, resp3);
                        return;
                    }
                    let i = (rng_next(&mut rand_seed()) as usize) % s.len();
                    let m = s.members.remove(i);
                    if !save_set(&store, &args[1], &s, exp, out) {
                        return;
                    }
                    resp::bulk(out, &m);
                    return;
                }
                let count: i64 =
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(0);
                if count < 0 {
                    resp::error(out, "ERR value is out of range, must be positive");
                    return;
                }
                if s.is_empty() || count == 0 {
                    resp::array_header(out, 0);
                    return;
                }
                // distinct indices, removed high→low so earlier removes don't shift
                let mut idx = pick_indices(s.len(), count, rand_seed());
                idx.sort_unstable_by(|a, b| b.cmp(a));
                let mut popped: Vec<Vec<u8>> = Vec::with_capacity(idx.len());
                for i in idx {
                    popped.push(s.members.remove(i));
                }
                if !save_set(&store, &args[1], &s, exp, out) {
                    return;
                }
                resp::array_header(out, popped.len());
                for m in &popped {
                    resp::bulk(out, m);
                }
            }
            b"SRANDMEMBER" => {
                // SRANDMEMBER key [count]  (count < 0 allows repeats)
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'srandmember'");
                    return;
                }
                let (s, _) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                if nargs < 3 {
                    if s.is_empty() {
                        resp::null(out, resp3);
                    } else {
                        let i = (rng_next(&mut rand_seed()) as usize) % s.len();
                        resp::bulk(out, &s.members[i]);
                    }
                    return;
                }
                let count: i64 =
                    std::str::from_utf8(&args[2]).ok().and_then(|t| t.parse().ok()).unwrap_or(0);
                if s.is_empty() || count == 0 {
                    resp::array_header(out, 0);
                    return;
                }
                let idx = pick_indices(s.len(), count, rand_seed());
                resp::array_header(out, idx.len());
                for i in idx {
                    resp::bulk(out, &s.members[i]);
                }
            }
            b"SMOVE" => {
                // SMOVE source destination member
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'smove'");
                    return;
                }
                // Validate both types before mutating either side.
                let (mut src, sexp) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let (mut dst, dexp) = match load_set(&store, &args[2], out) {
                    Some(x) => x,
                    None => return,
                };
                if !src.remove(&args[3]) {
                    resp::integer(out, 0); // member not in source
                    return;
                }
                dst.add(&args[3]);
                if !save_set(&store, &args[1], &src, sexp, out) {
                    return;
                }
                if !save_set(&store, &args[2], &dst, dexp, out) {
                    return;
                }
                resp::integer(out, 1);
                if persist_on {
                    for k in [&args[1], &args[2]] {
                        stages.push(match store.get_typed(k) {
                            Some((kind, exp, blob)) => (k.clone(), blob.to_vec(), exp, kind as u8),
                            None => (k.clone(), Vec::new(), DELETE_TOMBSTONE, b's'),
                        });
                    }
                }
            }
            b"SUNION" | b"SINTER" | b"SDIFF" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let mut sets: Vec<aggr::Set> = Vec::with_capacity(nargs - 1);
                for k in &args[1..] {
                    match load_set(&store, k, out) {
                        Some((s, _)) => sets.push(s),
                        None => return,
                    }
                }
                let result = set_combine(&cmd, &sets);
                resp::set_header(out, result.len(), resp3); // RESP3 set, RESP2 array
                for m in &result {
                    resp::bulk(out, m);
                }
            }
            b"SUNIONSTORE" | b"SINTERSTORE" | b"SDIFFSTORE" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let mut sets: Vec<aggr::Set> = Vec::with_capacity(nargs - 2);
                for k in &args[2..] {
                    match load_set(&store, k, out) {
                        Some((s, _)) => sets.push(s),
                        None => return,
                    }
                }
                let result = set_combine(&cmd, &sets);
                // Store at the destination, dropping any prior value/TTL (Redis
                // clears the destination's TTL on *STORE). Auto-stage persists it.
                let mut ns = aggr::Set::new();
                for m in &result {
                    ns.add(m);
                }
                if ns.is_empty() {
                    store.del(&args[1]);
                } else {
                    if !wrote(out, store.set_typed(&args[1], &ns.encode(), 0, KIND_SET)) {
                        return;
                    }
                }
                resp::integer(out, ns.len() as i64);
            }
            // ---- container scans -----------------------------------
            // Each aggregate is one decoded blob, so a single call returns the
            // whole (optionally MATCH-filtered) collection with next cursor "0"
            // — a valid Redis SCAN result. COUNT is a hint and ignored.
            b"HSCAN" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'hscan'");
                    return;
                }
                let (pattern, novalues) = scan_opts(args, 3, true);
                let (h, _) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let items: Vec<&(Vec<u8>, Vec<u8>)> = h
                    .entries
                    .iter()
                    .filter(|(f, _)| pattern.as_ref().map_or(true, |p| glob_match(p, f)))
                    .collect();
                resp::array_header(out, 2);
                resp::bulk(out, b"0");
                resp::array_header(out, items.len() * if novalues { 1 } else { 2 });
                for (f, v) in items {
                    resp::bulk(out, f);
                    if !novalues {
                        resp::bulk(out, v);
                    }
                }
            }
            b"SSCAN" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'sscan'");
                    return;
                }
                let (pattern, _) = scan_opts(args, 3, false);
                let (s, _) = match load_set(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let items: Vec<&Vec<u8>> = s
                    .members
                    .iter()
                    .filter(|m| pattern.as_ref().map_or(true, |p| glob_match(p, m)))
                    .collect();
                resp::array_header(out, 2);
                resp::bulk(out, b"0");
                resp::array_header(out, items.len());
                for m in items {
                    resp::bulk(out, m);
                }
            }
            b"ZSCAN" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'zscan'");
                    return;
                }
                let (pattern, _) = scan_opts(args, 3, false);
                let (z, _) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let items: Vec<&(Vec<u8>, f64)> = z
                    .members
                    .iter()
                    .filter(|(m, _)| pattern.as_ref().map_or(true, |p| glob_match(p, m)))
                    .collect();
                resp::array_header(out, 2);
                resp::bulk(out, b"0");
                resp::array_header(out, items.len() * 2);
                for (m, s) in items {
                    resp::bulk(out, m);
                    resp::bulk(out, aggr::fmt_score(*s).as_bytes());
                }
            }
            // ---- sorted-set set operations -------------------------
            b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" | b"ZUNION" | b"ZINTER"
            | b"ZDIFF" => {
                let store_variant = cmd.ends_with(b"STORE");
                let numkeys_pos = if store_variant { 2 } else { 1 };
                let numkeys: usize = args
                    .get(numkeys_pos)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(0);
                let first_key = numkeys_pos + 1;
                if numkeys == 0 || first_key + numkeys > nargs {
                    resp::error(out, "ERR at least 1 input key is needed");
                    return;
                }
                // Parse the trailing [WEIGHTS …] [AGGREGATE …] [WITHSCORES].
                let mut weights = vec![1.0f64; numkeys];
                let mut agg = Agg::Sum;
                let mut withscores = false;
                let mut i = first_key + numkeys;
                while i < nargs {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"WEIGHTS" if i + numkeys < nargs => {
                            for j in 0..numkeys {
                                weights[j] =
                                    aggr::parse_score(&args[i + 1 + j]).unwrap_or(1.0);
                            }
                            i += 1 + numkeys;
                        }
                        b"AGGREGATE" if i + 1 < nargs => {
                            agg = match args[i + 1].to_ascii_uppercase().as_slice() {
                                b"MIN" => Agg::Min,
                                b"MAX" => Agg::Max,
                                _ => Agg::Sum,
                            };
                            i += 2;
                        }
                        b"WITHSCORES" => {
                            withscores = true;
                            i += 1;
                        }
                        _ => i += 1,
                    }
                }
                let op: &[u8] = if cmd.starts_with(b"ZUNION") {
                    b"UNION"
                } else if cmd.starts_with(b"ZINTER") {
                    b"INTER"
                } else {
                    b"DIFF"
                };
                let mut sets: Vec<(aggr::ZSet, f64)> = Vec::with_capacity(numkeys);
                for (idx, k) in args[first_key..first_key + numkeys].iter().enumerate() {
                    match load_zset(&store, k, out) {
                        Some((z, _)) => sets.push((z, weights[idx])),
                        None => return,
                    }
                }
                let result = zset_setop(op, &sets, agg);
                if store_variant {
                    let mut z = aggr::ZSet::new();
                    for (m, s) in &result {
                        z.add(m, *s);
                    }
                    if z.is_empty() {
                        store.del(&args[1]);
                    } else {
                        if !wrote(out, store.set_typed(&args[1], &z.encode(), 0, KIND_ZSET)) {
                            return;
                        }
                    }
                    resp::integer(out, result.len() as i64);
                } else if withscores {
                    reply_scored(out, &result, resp3, resp3);
                } else {
                    resp::array_header(out, result.len());
                    for (m, _) in &result {
                        resp::bulk(out, m);
                    }
                }
            }
            b"ZMPOP" => {
                // ZMPOP numkeys key [key ...] MIN|MAX [COUNT n]
                let numkeys: usize = args
                    .get(1)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(0);
                let first_key = 2;
                let dir_pos = first_key + numkeys;
                if numkeys == 0 || dir_pos >= nargs {
                    resp::error(out, "ERR syntax error");
                    return;
                }
                let from_min = match args[dir_pos].to_ascii_uppercase().as_slice() {
                    b"MIN" => true,
                    b"MAX" => false,
                    _ => {
                        resp::error(out, "ERR syntax error");
                        return;
                    }
                };
                let mut count = 1usize;
                if dir_pos + 2 < nargs && args[dir_pos + 1].eq_ignore_ascii_case(b"COUNT") {
                    count = std::str::from_utf8(&args[dir_pos + 2])
                        .ok()
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(1);
                }
                // Pop from the first non-empty key.
                for k in &args[first_key..first_key + numkeys] {
                    let (mut z, exp) = match load_zset(&store, k, out) {
                        Some(x) => x,
                        None => return,
                    };
                    if z.is_empty() {
                        continue;
                    }
                    let sorted = z.sorted();
                    let take = count.min(sorted.len());
                    let chosen: Vec<(Vec<u8>, f64)> = if from_min {
                        sorted[..take].to_vec()
                    } else {
                        sorted[sorted.len() - take..].iter().rev().cloned().collect()
                    };
                    for (m, _) in &chosen {
                        z.remove(m);
                    }
                    if !save_zset(&store, k, &z, exp, out) {
                        return;
                    }
                    resp::array_header(out, 2);
                    resp::bulk(out, k);
                    resp::array_header(out, chosen.len());
                    for (m, s) in &chosen {
                        resp::array_header(out, 2);
                        resp::bulk(out, m);
                        resp::double(out, &aggr::fmt_score(*s), resp3);
                    }
                    if persist_on {
                        stages.push(match store.get_typed(k) {
                            Some((kind, e, blob)) => (k.clone(), blob.to_vec(), e, kind as u8),
                            None => (k.clone(), Vec::new(), DELETE_TOMBSTONE, b's'),
                        });
                    }
                    return;
                }
                resp::null(out, resp3); // all input sets empty
            }
            b"ZLEXCOUNT" => {
                if nargs != 4 {
                    resp::error(out, "ERR wrong number of arguments for 'zlexcount'");
                    return;
                }
                let (lo, hi) = match (parse_lex(&args[2]), parse_lex(&args[3])) {
                    (Some(a), Some(b)) => (a, b),
                    _ => {
                        resp::error(out, "ERR min or max not valid string range item");
                        return;
                    }
                };
                let (z, _) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let n = z
                    .members
                    .iter()
                    .filter(|(m, _)| lex_ge(m, &lo) && lex_le(m, &hi))
                    .count();
                resp::integer(out, n as i64);
            }
            b"ZRANGEBYLEX" | b"ZREVRANGEBYLEX" => {
                if nargs < 4 {
                    resp::error(out, "ERR wrong number of arguments");
                    return;
                }
                let rev = cmd == b"ZREVRANGEBYLEX";
                // REV takes (max min); normalise to (lo, hi) ascending.
                let (lob, hib) = if rev { (&args[3], &args[2]) } else { (&args[2], &args[3]) };
                let (lo, hi) = match (parse_lex(lob), parse_lex(hib)) {
                    (Some(a), Some(b)) => (a, b),
                    _ => {
                        resp::error(out, "ERR min or max not valid string range item");
                        return;
                    }
                };
                let mut offset = 0usize;
                let mut count: Option<usize> = None;
                for w in 4..nargs {
                    if args[w].eq_ignore_ascii_case(b"LIMIT") && w + 2 < nargs {
                        offset = std::str::from_utf8(&args[w + 1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                        let c: i64 = std::str::from_utf8(&args[w + 2]).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);
                        count = if c < 0 { None } else { Some(c as usize) };
                    }
                }
                let (z, _) = match load_zset(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                let mut ms: Vec<Vec<u8>> = z
                    .members
                    .iter()
                    .filter(|(m, _)| lex_ge(m, &lo) && lex_le(m, &hi))
                    .map(|(m, _)| m.clone())
                    .collect();
                ms.sort();
                if rev {
                    ms.reverse();
                }
                let ms: Vec<Vec<u8>> =
                    ms.into_iter().skip(offset).take(count.unwrap_or(usize::MAX)).collect();
                resp::array_header(out, ms.len());
                for m in &ms {
                    resp::bulk(out, m);
                }
            }
            b"ZRANGESTORE" => {
                // ZRANGESTORE dst src min max [BYSCORE|BYLEX] [REV] [LIMIT off cnt]
                if nargs < 5 {
                    resp::error(out, "ERR wrong number of arguments for 'zrangestore'");
                    return;
                }
                let byscore = args[5..].iter().any(|a| a.eq_ignore_ascii_case(b"BYSCORE"));
                let bylex = args[5..].iter().any(|a| a.eq_ignore_ascii_case(b"BYLEX"));
                let rev = args[5..].iter().any(|a| a.eq_ignore_ascii_case(b"REV"));
                let mut offset = 0usize;
                let mut limit: Option<usize> = None;
                for w in 5..nargs {
                    if args[w].eq_ignore_ascii_case(b"LIMIT") && w + 2 < nargs {
                        offset = std::str::from_utf8(&args[w + 1]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                        let c: i64 = std::str::from_utf8(&args[w + 2]).ok().and_then(|s| s.parse().ok()).unwrap_or(-1);
                        limit = if c < 0 { None } else { Some(c as usize) };
                    }
                }
                let (z, _) = match load_zset(&store, &args[2], out) {
                    Some(x) => x,
                    None => return,
                };
                // Build the selected (member, score) items per range mode.
                let mut items: Vec<(Vec<u8>, f64)> = if bylex {
                    let (lob, hib) = if rev { (&args[4], &args[3]) } else { (&args[3], &args[4]) };
                    match (parse_lex(lob), parse_lex(hib)) {
                        (Some(lo), Some(hi)) => {
                            let mut v: Vec<(Vec<u8>, f64)> = z
                                .members
                                .iter()
                                .filter(|(m, _)| lex_ge(m, &lo) && lex_le(m, &hi))
                                .map(|(m, s)| (m.clone(), *s))
                                .collect();
                            v.sort_by(|a, b| a.0.cmp(&b.0));
                            v
                        }
                        _ => {
                            resp::error(out, "ERR min or max not valid string range item");
                            return;
                        }
                    }
                } else if byscore {
                    let (minb, maxb) = if rev { (&args[4], &args[3]) } else { (&args[3], &args[4]) };
                    match (aggr::ScoreBound::parse(minb), aggr::ScoreBound::parse(maxb)) {
                        (Some(min), Some(max)) => z.by_score(&min, &max),
                        _ => {
                            resp::error(out, "ERR min or max is not a float");
                            return;
                        }
                    }
                } else {
                    // by rank (index)
                    let start: i64 = std::str::from_utf8(&args[3]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                    let stop: i64 = std::str::from_utf8(&args[4]).ok().and_then(|s| s.parse().ok()).unwrap_or(0);
                    let sorted = z.sorted();
                    let (l, h) = aggr::rank_bounds(sorted.len(), start, stop);
                    sorted[l..h].to_vec()
                };
                if rev && !bylex {
                    items.reverse();
                }
                if bylex || byscore {
                    items = items.into_iter().skip(offset).take(limit.unwrap_or(usize::MAX)).collect();
                }
                // Store at the destination (clears its prior value/TTL).
                let mut nz = aggr::ZSet::new();
                for (m, s) in &items {
                    nz.add(m, *s);
                }
                let n = nz.len();
                if nz.is_empty() {
                    store.del(&args[1]);
                } else {
                    if !wrote(out, store.set_typed(&args[1], &nz.encode(), 0, KIND_ZSET)) {
                        return;
                    }
                }
                resp::integer(out, n as i64);
                if persist_on {
                    stages.push(match store.get_typed(&args[1]) {
                        Some((kind, e, blob)) => (args[1].clone(), blob.to_vec(), e, kind as u8),
                        None => (args[1].clone(), Vec::new(), DELETE_TOMBSTONE, b's'),
                    });
                }
            }
            b"SINTERCARD" => {
                // SINTERCARD numkeys key [key ...] [LIMIT n]
                let numkeys: usize = args
                    .get(1)
                    .and_then(|a| std::str::from_utf8(a).ok())
                    .and_then(|t| t.parse().ok())
                    .unwrap_or(0);
                let first_key = 2;
                if numkeys == 0 || first_key + numkeys > nargs {
                    resp::error(out, "ERR numkeys should be greater than 0");
                    return;
                }
                let mut limit = usize::MAX;
                let opt_pos = first_key + numkeys;
                if opt_pos + 1 < nargs && args[opt_pos].eq_ignore_ascii_case(b"LIMIT") {
                    let l: i64 = std::str::from_utf8(&args[opt_pos + 1])
                        .ok()
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(0);
                    if l < 0 {
                        resp::error(out, "ERR LIMIT can't be negative");
                        return;
                    }
                    if l > 0 {
                        limit = l as usize;
                    }
                }
                let mut sets: Vec<aggr::Set> = Vec::with_capacity(numkeys);
                for k in &args[first_key..first_key + numkeys] {
                    match load_set(&store, k, out) {
                        Some((s, _)) => sets.push(s),
                        None => return,
                    }
                }
                // Count members present in every set, stopping early at LIMIT.
                let mut count = 0usize;
                'outer: for m in &sets[0].members {
                    for other in &sets[1..] {
                        if !other.contains(m) {
                            continue 'outer;
                        }
                    }
                    count += 1;
                    if count >= limit {
                        break;
                    }
                }
                resp::integer(out, count as i64);
            }
            other => resp::error(
                out,
                &format!("ERR unknown command '{}'", String::from_utf8_lossy(other)),
            ),
        }

        // durable aggregates: a mutation of a hash/list/zset persists
        // its whole (kind-tagged) blob from the final store state — or a tombstone
        // if the key was emptied/deleted — so it recovers as the right type.
        if persist_on && stages.is_empty() && is_aggregate_write(&cmd) && nargs >= 2 {
            stages.push(match store.get_typed(&args[1]) {
                Some((kind, exp, blob)) => (args[1].clone(), blob.to_vec(), exp, kind as u8),
                None => (args[1].clone(), Vec::new(), DELETE_TOMBSTONE, b's'),
            });
        }

        let mut queue_failed = false;
        for (k, v, e, kind) in stages {
            // sharded so a given key always lands on the same ring/persist worker
            // — no cross-worker key conflicts on ON CONFLICT.
            //
            // Large values are staged by reference: the record carries the
            // entry's version instead of the value, and the persistence worker
            // reads the bytes straight out of the shared segment. That removes
            // the second copy of the value and stops ring capacity from
            // bounding how large a value may be. `stage_by_ref` also records
            // the sequence on the entry so eviction cannot drop it before the
            // worker has committed it.
            let staged = match (v.len() > INLINE_MAX && e != DELETE_TOMBSTONE)
                .then(|| store.version_of(&k))
                .flatten()
            {
                Some(version) => {
                    let r = shard_push(
                        &self.producers,
                        &k,
                        &version.to_le_bytes(),
                        e,
                        kind | ring::KIND_REF,
                    );
                    // Marked after the push so the sequence is known. Safe to
                    // do in this order: eviction runs on this same thread, so
                    // nothing can drop the entry in between.
                    if let Some((_, seq)) = r {
                        store.set_staged_seq(&k, seq);
                    }
                    r
                }
                // Small value, a tombstone, or a key that vanished under us:
                // copy it into the ring as before.
                None => shard_push(&self.producers, &k, &v, e, kind),
            };
            match staged {
                Some(sa) => acks.push(sa),
                None => {
                    queue_failed = true;
                    break;
                }
            }
        }
        if queue_failed && sync_ack {
            // A sync-ack tier promises that a successful reply means the write
            // reached supacache.kv. The record could not even be queued, so the
            // promise cannot be kept: replace the reply already written with an
            // error instead of flushing a +OK for a write that is not durable.
            //
            // The shared-memory mutation has already been applied and is NOT
            // rolled back: undoing it is not generally possible (INCR, LPUSH and
            // the aggregate commands are not invertible from here). So the value
            // may be readable until the next restart while never becoming
            // durable. The error says exactly that, and it is the honest report
            // of an ambiguous outcome rather than a false success.
            let c = self.conns.get_mut(&fd).unwrap();
            c.wbuf.truncate(reply_start);
            resp::error(
                &mut c.wbuf,
                "ERR persistence backlog full: write applied in memory but NOT durable",
            );
            // Deliberately not clearing `c.ack`: any entry there belongs to an
            // EARLIER command in this pipeline whose reply is still held, and
            // dropping it would flush that reply before its record committed.
            // This command simply contributes no wait — its reply is an error,
            // which promises nothing, and `wbuf` still keeps it behind the
            // replies of the commands before it.
        } else if sync_ack && !acks.is_empty() {
            // Durable tier: hold this command's reply until its record(s) commit,
            // alongside whatever earlier commands in the pipeline are waiting on.
            Self::merge_acks(&mut self.conns.get_mut(&fd).unwrap().ack, acks);
        }

        // ---- server-assisted client-side caching (CLIENT TRACKING) ----
        // A write invalidates every tracker of the keys it touched; a read from a
        // tracking connection records the keys it just read so a later write to
        // them fires an invalidation. FLUSH invalidates the whole keyspace.
        if matches!(cmd.as_slice(), b"FLUSHALL" | b"FLUSHDB") {
            self.invalidate_all(fd);
            self.broadcast_invalidation(None);
        } else if !key_idxs.is_empty() {
            if is_write_cmd(&cmd) {
                for &i in &key_idxs {
                    if let Some(k) = args.get(i) {
                        self.invalidate_key(k, fd);
                        self.broadcast_invalidation(Some(k));
                    }
                }
            } else if self.should_record_reads(fd) {
                // Default / OPTIN / OPTOUT register each read key; BCAST does not
                // (its invalidations come from prefix matching at write time).
                for &i in &key_idxs {
                    if let Some(k) = args.get(i) {
                        self.tracked.entry(k.to_vec()).or_default().insert(fd);
                    }
                }
            }
        }
        // CLIENT CACHING is a one-shot flag for the command that follows it; this
        // command has now consumed it (CLIENT itself never reaches here).
        if let Some(c) = self.conns.get_mut(&fd) {
            c.track.caching = None;
        }
    }

    /// MULTI — open a transaction: subsequent commands queue until EXEC/DISCARD.
    fn handle_multi(&mut self, fd: RawFd) {
        let c = self.conns.get_mut(&fd).unwrap();
        if c.in_multi {
            resp::error(&mut c.wbuf, "ERR MULTI calls can not be nested");
            return;
        }
        c.in_multi = true;
        c.queued.clear();
        resp::simple(&mut c.wbuf, "OK");
    }

    /// DISCARD — abandon a transaction and drop any WATCHes.
    fn handle_discard(&mut self, fd: RawFd) {
        let c = self.conns.get_mut(&fd).unwrap();
        if !c.in_multi {
            resp::error(&mut c.wbuf, "ERR DISCARD without MULTI");
            return;
        }
        c.in_multi = false;
        c.queued.clear();
        c.watch.clear();
        resp::simple(&mut c.wbuf, "OK");
    }

    /// WATCH key [key …] — snapshot each (tenant-scoped) key's version so EXEC
    /// can abort if it changed. Not allowed once inside MULTI.
    fn handle_watch(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        if args.len() < 2 {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::error(out, "ERR wrong number of arguments for 'watch'");
            return;
        }
        if self.conns.get(&fd).map(|c| c.in_multi).unwrap_or(false) {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::error(out, "ERR WATCH inside MULTI is not allowed");
            return;
        }
        for k in &args[1..] {
            let scoped = self.scope_name(fd, k);
            let v = self.store.version(&scoped);
            self.conns.get_mut(&fd).unwrap().watch.push((scoped, v));
        }
        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
        resp::simple(out, "OK");
    }

    /// EXEC — run the queued commands atomically (nothing else runs on this
    /// single-threaded worker in between), unless a WATCHed key changed, in which
    /// case the whole transaction aborts with a null array.
    fn handle_exec(&mut self, fd: RawFd) {
        if !self.conns.get(&fd).map(|c| c.in_multi).unwrap_or(false) {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::error(out, "ERR EXEC without MULTI");
            return;
        }
        let (queued, watch) = {
            let c = self.conns.get_mut(&fd).unwrap();
            c.in_multi = false;
            (std::mem::take(&mut c.queued), std::mem::take(&mut c.watch))
        };
        // Abort if any WATCHed key's version no longer matches its snapshot.
        let aborted = watch.iter().any(|(k, snap)| self.store.version(k) != *snap);
        if aborted {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::nil_array(out);
            return;
        }
        // The array header, then each queued command's reply appended in order —
        // re-dispatching applies auth/tenant scoping at execution time.
        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
        resp::array_header(out, queued.len());
        for cmd_args in queued {
            self.dispatch(fd, &cmd_args);
        }
    }

    /// RESP `AUTH [user] pass` — resolve the credential and set the connection's
    /// role/tenant/exempt state. Computed in two phases so the borrow of
    /// `self.auth` is released before the connection is mutated.
    fn handle_auth(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        enum R {
            Ok(String, String, bool),
            Wrong,
            BadArgs,
            NoCfg,
        }
        let r = match &self.auth {
            None => R::NoCfg,
            Some(cfg) => {
                if args.len() < 2 {
                    R::BadArgs
                } else {
                    let (user, pass): (String, &Vec<u8>) = if args.len() >= 3 {
                        (String::from_utf8_lossy(&args[1]).into_owned(), &args[2])
                    } else {
                        ("default".to_string(), &args[1])
                    };
                    match cfg.creds.get(&user) {
                        Some(c) if c.verify(pass.as_slice()) => {
                            R::Ok(c.role.clone(), c.tenant.clone(), cfg.exempt.contains(&c.role))
                        }
                        _ => R::Wrong,
                    }
                }
            }
        };
        let c = self.conns.get_mut(&fd).unwrap();
        match r {
            R::NoCfg => resp::simple(&mut c.wbuf, "OK"),
            R::BadArgs => resp::error(&mut c.wbuf, "ERR wrong number of arguments for 'auth'"),
            R::Wrong => resp::error(
                &mut c.wbuf,
                "WRONGPASS invalid username-password pair or user is disabled.",
            ),
            R::Ok(role, tenant, exempt) => {
                c.authed = true;
                c.role = role;
                c.tenant = tenant;
                c.exempt = exempt;
                resp::simple(&mut c.wbuf, "OK");
            }
        }
    }

    /// HELLO [protover [AUTH user pass] [SETNAME name]] — negotiate the protocol
    /// and reply with the server handshake map. `HELLO 3` switches the connection
    /// to RESP3 (typed replies + push-framed pub/sub); `HELLO 2` (or none) is
    /// RESP2. An optional inline AUTH is applied before the reply.
    fn handle_hello(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        let mut proto: i64 = if self.conns.get(&fd).map(|c| c.resp3).unwrap_or(false) {
            3
        } else {
            2
        };
        let mut i = 1;
        if args.len() >= 2 {
            match std::str::from_utf8(&args[1]).ok().and_then(|t| t.parse::<i64>().ok()) {
                Some(p) if p == 2 || p == 3 => {
                    proto = p;
                    i = 2;
                }
                _ => {
                    let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                    resp::error(
                        out,
                        "NOPROTO unsupported protocol version",
                    );
                    return;
                }
            }
        }
        // Optional AUTH / SETNAME arguments after the version.
        while i < args.len() {
            match args[i].to_ascii_uppercase().as_slice() {
                b"AUTH" if i + 2 < args.len() => {
                    let user = String::from_utf8_lossy(&args[i + 1]).into_owned();
                    // Resolve the credential without holding a `conns` borrow.
                    enum A {
                        NoCfg,
                        Ok(String, String, bool),
                        Wrong,
                    }
                    let outcome = match &self.auth {
                        None => A::NoCfg,
                        Some(cfg) => match cfg.creds.get(&user) {
                            Some(c) if c.verify(args[i + 2].as_slice()) => {
                                A::Ok(c.role.clone(), c.tenant.clone(), cfg.exempt.contains(&c.role))
                            }
                            _ => A::Wrong,
                        },
                    };
                    match outcome {
                        A::Wrong => {
                            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                            resp::error(
                                out,
                                "WRONGPASS invalid username-password pair or user is disabled.",
                            );
                            return;
                        }
                        A::Ok(role, tenant, exempt) => {
                            let c = self.conns.get_mut(&fd).unwrap();
                            c.authed = true;
                            c.role = role;
                            c.tenant = tenant;
                            c.exempt = exempt;
                        }
                        A::NoCfg => {} // no auth configured: HELLO AUTH is a no-op OK
                    }
                    i += 3;
                }
                b"SETNAME" if i + 1 < args.len() => i += 2, // client name: accepted, unused
                _ => {
                    let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                    resp::error(out, "ERR syntax error in HELLO");
                    return;
                }
            }
        }
        self.conns.get_mut(&fd).unwrap().resp3 = proto == 3;
        let resp3 = proto == 3;
        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
        resp::map_header(out, 7, resp3);
        resp::bulk(out, b"server");
        resp::bulk(out, b"redis");
        resp::bulk(out, b"version");
        resp::bulk(out, b"7.4.0");
        resp::bulk(out, b"proto");
        resp::integer(out, proto);
        resp::bulk(out, b"id");
        resp::integer(out, fd as i64);
        resp::bulk(out, b"mode");
        resp::bulk(out, b"standalone");
        resp::bulk(out, b"role");
        resp::bulk(out, b"master");
        resp::bulk(out, b"modules");
        resp::array_header(out, 0);
    }

    /// CLIENT subcommands. TRACKING ON/OFF drives server-assisted client-side
    /// caching; the rest are the benign ones clients send at connect (ID, GETNAME,
    /// SETNAME, SETINFO, NO-EVICT, …), answered OK.
    fn handle_client(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        match args.get(1).map(|a| a.to_ascii_uppercase()).as_deref() {
            Some(b"ID") => {
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::integer(out, fd as i64);
            }
            Some(b"GETNAME") => {
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::bulk(out, b"");
            }
            Some(b"TRACKING") => self.handle_tracking(fd, args),
            Some(b"CACHING") => {
                // One-shot opt for the next command; only valid under OPTIN/OPTOUT.
                let yes = match args.get(2).map(|a| a.to_ascii_uppercase()).as_deref() {
                    Some(b"YES") => true,
                    Some(b"NO") => false,
                    _ => {
                        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                        resp::error(out, "ERR syntax error");
                        return;
                    }
                };
                let c = self.conns.get_mut(&fd).unwrap();
                if !c.track.on || (!c.track.optin && !c.track.optout) {
                    resp::error(
                        &mut c.wbuf,
                        "ERR CLIENT CACHING can be called only when the client is in tracking mode with OPTIN or OPTOUT mode enabled",
                    );
                    return;
                }
                // YES is meaningful under OPTIN, NO under OPTOUT; store either as
                // the one-shot flag the next read consults.
                c.track.caching = Some(yes);
                resp::simple(&mut c.wbuf, "OK");
            }
            _ => {
                // SETNAME / SETINFO / NO-EVICT / NO-TOUCH / UNPAUSE / …
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::simple(out, "OK");
            }
        }
    }

    /// `CLIENT TRACKING ON|OFF [REDIRECT id] [PREFIX p ...] [BCAST] [OPTIN] [OPTOUT]
    /// [NOLOOP]`. ON without REDIRECT needs a RESP3 connection (the invalidations
    /// arrive as push frames); with REDIRECT the pushes go to another client id, so
    /// a RESP2 connection may track too. Rejects the combinations Valkey rejects
    /// (OPTIN with OPTOUT; PREFIX without BCAST) so a client that mis-negotiates
    /// hears about it rather than silently getting the wrong invalidations.
    fn handle_tracking(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        let onoff = args.get(2).map(|a| a.to_ascii_uppercase());
        match onoff.as_deref() {
            Some(b"OFF") => {
                self.bcast_subs.remove(&fd);
                self.untrack_fd(fd);
                let was_on = self.conns.get(&fd).map(|c| c.track.on).unwrap_or(false);
                let c = self.conns.get_mut(&fd).unwrap();
                c.track = Tracking::default();
                resp::simple(&mut c.wbuf, "OK");
                if was_on {
                    if let Some(bus) = &self.bus {
                        bus.tracker_remove();
                    }
                }
            }
            Some(b"ON") => {
                let resp3 = self.conns.get(&fd).map(|c| c.resp3).unwrap_or(false);
                let mut t = Tracking { on: true, ..Tracking::default() };
                let mut raw_prefixes: Vec<Vec<u8>> = Vec::new();
                let mut i = 3;
                while i < args.len() {
                    match args[i].to_ascii_uppercase().as_slice() {
                        b"REDIRECT" if i + 1 < args.len() => {
                            let id: i64 = std::str::from_utf8(&args[i + 1])
                                .ok()
                                .and_then(|s| s.parse().ok())
                                .unwrap_or(-1);
                            // 0 means "no redirect"; otherwise the id must be a live
                            // client (our client id is its fd).
                            if id != 0 && !self.conns.contains_key(&(id as RawFd)) {
                                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                                resp::error(
                                    out,
                                    "ERR The client ID you want redirect to does not exist",
                                );
                                return;
                            }
                            t.redirect = id as RawFd;
                            i += 2;
                        }
                        b"PREFIX" if i + 1 < args.len() => {
                            raw_prefixes.push(args[i + 1].clone());
                            i += 2;
                        }
                        b"BCAST" => {
                            t.bcast = true;
                            i += 1;
                        }
                        b"OPTIN" => {
                            t.optin = true;
                            i += 1;
                        }
                        b"OPTOUT" => {
                            t.optout = true;
                            i += 1;
                        }
                        b"NOLOOP" => {
                            // Accepted: this server never echoes an invalidation to
                            // the connection that issued the write (see invalidate_key),
                            // so NOLOOP is already the effective behaviour.
                            i += 1;
                        }
                        _ => {
                            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                            resp::error(out, "ERR syntax error in CLIENT TRACKING");
                            return;
                        }
                    }
                }
                // Validate the negotiated combination.
                let err = if t.optin && t.optout {
                    Some("ERR You can't specify both OPTIN mode and OPTOUT mode")
                } else if !raw_prefixes.is_empty() && !t.bcast {
                    Some("ERR PREFIX option requires BCAST mode to be enabled")
                } else if (t.optin || t.optout) && t.bcast {
                    Some("ERR OPTIN and OPTOUT are not compatible with BCAST")
                } else if t.redirect == 0 && !resp3 {
                    Some("ERR Client tracking requires RESP3; send HELLO 3 first or REDIRECT to a client")
                } else {
                    None
                };
                if let Some(msg) = err {
                    let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                    resp::error(out, msg);
                    return;
                }
                // Scope the client-facing prefixes to this tenant so BCAST matches
                // the scoped keys the write path sees.
                t.prefixes = raw_prefixes.iter().map(|p| self.scope_name(fd, p)).collect();
                // Re-negotiating from a previous mode: clear any stale registrations.
                self.untrack_fd(fd);
                if t.bcast {
                    self.bcast_subs.insert(fd);
                } else {
                    self.bcast_subs.remove(&fd);
                }
                let was_on = self.conns.get(&fd).map(|c| c.track.on).unwrap_or(false);
                let c = self.conns.get_mut(&fd).unwrap();
                c.track = t;
                resp::simple(&mut c.wbuf, "OK");
                // Count this connection toward the cross-worker tracking gate the
                // first time it turns tracking on (re-negotiating an already-on
                // connection does not double-count).
                if !was_on {
                    if let Some(bus) = &self.bus {
                        bus.tracker_add();
                    }
                }
            }
            _ => {
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::error(out, "ERR syntax error in CLIENT TRACKING");
            }
        }
    }

    /// Send an `invalidate` push for `key` to every connection tracking it, then
    /// drop the entry (Redis drops a key from the table once it invalidates; the
    /// client re-reads to re-track). Each tracker sees the key in its own
    /// tenant-facing (unscoped) form. Guarded by the live conn's `tracking`+`resp3`
    /// so a reused fd can never receive a stray push into a non-tracking stream.
    fn invalidate_key(&mut self, key: &[u8], except: RawFd) {
        // Default / OPTIN / OPTOUT trackers that recorded a read of this key.
        if let Some(fds) = self.tracked.remove(key) {
            for reader in fds {
                // Skip the writer itself (NOLOOP): it already knows it changed the
                // key, and flushing its own fd mid-command would race the ack.
                if reader == except {
                    continue;
                }
                let prefix = self.conn_prefix(reader);
                let facing = strip_scope(key, &prefix).to_vec();
                self.deliver_invalidation(reader, Some(&facing));
            }
        }
        // BCAST trackers: no per-read table — a write matches the key against each
        // broadcasting connection's registered prefixes (empty => whole keyspace).
        if !self.bcast_subs.is_empty() {
            let readers: Vec<RawFd> = self
                .bcast_subs
                .iter()
                .copied()
                .filter(|&r| r != except)
                .filter(|&r| {
                    self.conns
                        .get(&r)
                        .map_or(false, |c| bcast_matches(&c.track.prefixes, key))
                })
                .collect();
            for reader in readers {
                let prefix = self.conn_prefix(reader);
                let facing = strip_scope(key, &prefix).to_vec();
                self.deliver_invalidation(reader, Some(&facing));
            }
        }
    }

    /// Fan a client-side-caching invalidation out to the other workers over the
    /// Bus so each notifies its own trackers (`None` = whole keyspace, for FLUSH).
    /// A no-op without a Bus (the single-worker in-PG extension) and skipped
    /// entirely when no connection anywhere is tracking, so a write pays only one
    /// relaxed atomic load when client-side caching is unused. Cross-worker
    /// invalidation is conservative: workers own independent keyspace segments, so
    /// a same-named key on another worker may be told to drop a still-valid cache
    /// entry (it re-fetches — never serves stale data). It is exactly right once a
    /// worker set shares one store.
    fn broadcast_invalidation(&self, key: Option<&[u8]>) {
        if let Some(bus) = &self.bus {
            if bus.tracking_active() {
                bus.invalidate(self.worker_id, key);
            }
        }
    }

    /// Deliver one invalidation to `reader`'s sink. The sink is the REDIRECT
    /// target when set, else `reader` itself: a RESP3 sink receives a push
    /// (`>2 invalidate …`), a RESP2 sink (only reachable via REDIRECT) the pub/sub
    /// message form on `__redis__:invalidate`. `facing` is the tenant-facing key,
    /// or `None` for the whole-keyspace (FLUSH) signal.
    fn deliver_invalidation(&mut self, reader: RawFd, facing: Option<&[u8]>) {
        let target = match self.conns.get(&reader) {
            Some(c) if c.track.redirect != 0 => c.track.redirect,
            Some(_) => reader,
            None => return,
        };
        let resp3 = match self.conns.get(&target) {
            Some(c) => c.resp3,
            None => return, // redirect target already gone
        };
        let out = &mut self.conns.get_mut(&target).unwrap().wbuf;
        if resp3 {
            resp::push_header(out, 2, true);
            resp::bulk(out, b"invalidate");
        } else {
            resp::array_header(out, 3);
            resp::bulk(out, b"message");
            resp::bulk(out, b"__redis__:invalidate");
        }
        match facing {
            Some(k) => {
                resp::array_header(out, 1);
                resp::bulk(out, k);
            }
            None => resp::null(out, resp3),
        }
        self.flush(target);
    }

    /// Invalidate everything (FLUSHALL/FLUSHDB): send a null `invalidate` — Redis's
    /// "the whole keyspace changed" signal — to every tracking connection (except
    /// the caller) and clear the per-read table.
    fn invalidate_all(&mut self, except: RawFd) {
        self.tracked.clear();
        let readers: Vec<RawFd> = self
            .conns
            .iter()
            .filter(|(&f, c)| f != except && c.track.on)
            .map(|(&f, _)| f)
            .collect();
        for reader in readers {
            self.deliver_invalidation(reader, None);
        }
    }

    /// Whether a read from `fd` should be recorded in the per-read `tracked`
    /// table: tracking on, not BCAST, and — under OPTIN — the previous command was
    /// `CLIENT CACHING YES`; under OPTOUT — it was not `CLIENT CACHING NO`.
    fn should_record_reads(&self, fd: RawFd) -> bool {
        match self.conns.get(&fd) {
            Some(c) if c.track.on && !c.track.bcast => {
                if c.track.optin {
                    c.track.caching == Some(true)
                } else if c.track.optout {
                    c.track.caching != Some(false)
                } else {
                    true
                }
            }
            _ => false,
        }
    }

    /// Remove `fd` from every tracked-key set (on CLIENT TRACKING OFF or close).
    fn untrack_fd(&mut self, fd: RawFd) {
        self.tracked.retain(|_, fds| {
            fds.remove(&fd);
            !fds.is_empty()
        });
    }

    /// True if the connection may run pub/sub commands (authed, or no auth
    /// configured). Writes NOAUTH and returns false otherwise.
    fn pubsub_authed(&mut self, fd: RawFd) -> bool {
        if self.auth.is_none() {
            return true;
        }
        if self.conns.get(&fd).map(|c| c.authed).unwrap_or(false) {
            return true;
        }
        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
        resp::error(out, "NOAUTH Authentication required.");
        false
    }

    /// SUBSCRIBE / PSUBSCRIBE: register the connection on each channel/pattern and
    /// reply with a confirmation carrying the running subscription count.
    fn handle_subscribe(&mut self, fd: RawFd, args: &[Vec<u8>], pattern: bool) {
        if args.len() < 2 {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::error(out, "ERR wrong number of arguments");
            return;
        }
        if !self.pubsub_authed(fd) {
            self.flush(fd);
            return;
        }
        let word: &[u8] = if pattern { b"psubscribe" } else { b"subscribe" };
        for ch in &args[1..] {
            // Store the tenant-scoped name internally; reply with the client's.
            let eff = self.scope_name(fd, ch);
            let fresh = if pattern {
                self.patterns.entry(eff.clone()).or_default().insert(fd)
            } else {
                self.channels.entry(eff.clone()).or_default().insert(fd)
            };
            // Advertise the new subscriber to the cross-worker routing table so
            // remote PUBLISHes reach it (once per genuinely-new fd+key).
            if fresh {
                if let Some(bus) = &self.bus {
                    bus.subscribe(self.worker_id, &eff, pattern);
                }
            }
            let count = {
                let c = self.conns.get_mut(&fd).unwrap();
                if pattern {
                    c.psubs.insert(eff);
                } else {
                    c.subs.insert(eff);
                }
                c.subs.len() + c.psubs.len()
            };
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::array_header(out, 3);
            resp::bulk(out, word);
            resp::bulk(out, ch);
            resp::integer(out, count as i64);
        }
        self.flush(fd);
    }

    /// UNSUBSCRIBE / PUNSUBSCRIBE: remove the given channels/patterns (all, if
    /// none given) and reply with a confirmation per channel.
    fn handle_unsubscribe(&mut self, fd: RawFd, args: &[Vec<u8>], pattern: bool) {
        let word: &[u8] = if pattern { b"punsubscribe" } else { b"unsubscribe" };
        // Which channels to drop, as (internal scoped name, client-facing name):
        // the named ones (scope them), or everything currently held (stored
        // scoped -> strip the prefix for the reply).
        let list: Vec<(Vec<u8>, Vec<u8>)> = if args.len() >= 2 {
            args[1..].iter().map(|a| (self.scope_name(fd, a), a.clone())).collect()
        } else {
            let prefix = self.conn_prefix(fd);
            self.conns
                .get(&fd)
                .map(|c| {
                    let held: Vec<Vec<u8>> = if pattern {
                        c.psubs.iter().cloned().collect()
                    } else {
                        c.subs.iter().cloned().collect()
                    };
                    held.into_iter()
                        .map(|s| {
                            let facing = strip_scope(&s, &prefix).to_vec();
                            (s, facing)
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        if list.is_empty() {
            // Nothing subscribed: Redis still replies with a nil-channel frame.
            let count = self
                .conns
                .get(&fd)
                .map(|c| c.subs.len() + c.psubs.len())
                .unwrap_or(0);
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::array_header(out, 3);
            resp::bulk(out, word);
            resp::nil(out);
            resp::integer(out, count as i64);
            self.flush(fd);
            return;
        }
        for (ch, facing) in &list {
            let removed = if pattern {
                match self.patterns.get_mut(ch) {
                    Some(set) => {
                        let r = set.remove(&fd);
                        if set.is_empty() {
                            self.patterns.remove(ch);
                        }
                        r
                    }
                    None => false,
                }
            } else {
                match self.channels.get_mut(ch) {
                    Some(set) => {
                        let r = set.remove(&fd);
                        if set.is_empty() {
                            self.channels.remove(ch);
                        }
                        r
                    }
                    None => false,
                }
            };
            if removed {
                if let Some(bus) = &self.bus {
                    bus.unsubscribe(self.worker_id, ch, pattern);
                }
            }
            let count = {
                let c = self.conns.get_mut(&fd).unwrap();
                if pattern {
                    c.psubs.remove(ch);
                } else {
                    c.subs.remove(ch);
                }
                c.subs.len() + c.psubs.len()
            };
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::array_header(out, 3);
            resp::bulk(out, word);
            resp::bulk(out, facing);
            resp::integer(out, count as i64);
        }
        self.flush(fd);
    }

    /// PUBLISH channel message: deliver to every local channel subscriber and
    /// every pattern subscriber whose glob matches, returning the delivery count.
    fn handle_publish(&mut self, fd: RawFd, args: &[Vec<u8>]) {
        if args.len() != 3 {
            let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
            resp::error(out, "ERR wrong number of arguments for 'publish'");
            return;
        }
        if !self.pubsub_authed(fd) {
            self.flush(fd);
            return;
        }
        let channel = self.scope_name(fd, &args[1]); // tenant-scoped internally
        let msg = args[2].clone();
        // Deliver to subscribers on this worker, then (if part of a scale-out
        // Bus) route to subscribers on the other workers.
        let mut receivers = self.deliver_local(&channel, &msg) as i64;
        if let Some(bus) = self.bus.clone() {
            receivers += bus.publish(self.worker_id, &channel, &msg, |p, c| glob_match(p, c)) as i64;
        }
        let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
        resp::integer(out, receivers);
        self.flush(fd);
    }

    /// Deliver `msg` to every subscriber *on this worker* — direct channel
    /// subscribers and pattern subscribers whose glob matches — and flush them.
    /// Returns the local receiver count. Local-only: it never touches the Bus,
    /// so it is safe to call both for a PUBLISH originating here and for a
    /// message routed in from another worker (no re-broadcast loop).
    fn deliver_local(&mut self, channel: &[u8], msg: &[u8]) -> usize {
        // Collect targets first (releases the channels/patterns borrows).
        let mut targets: Vec<(RawFd, Option<Vec<u8>>)> = Vec::new();
        if let Some(set) = self.channels.get(channel) {
            for &sfd in set {
                targets.push((sfd, None));
            }
        }
        for (pat, set) in &self.patterns {
            if glob_match(pat, channel) {
                for &sfd in set {
                    targets.push((sfd, Some(pat.clone())));
                }
            }
        }
        let mut receivers = 0usize;
        for (sfd, pat) in &targets {
            // Show each subscriber its OWN unscoped view: strip that connection's
            // tenant prefix from the channel (and matched pattern). All matches of
            // a scoped channel share its tenant, so this restores the client's name.
            let prefix = self.conn_prefix(*sfd);
            let fch = strip_scope(channel, &prefix);
            let fpat = pat.as_ref().map(|p| strip_scope(p, &prefix));
            if let Some(c) = self.conns.get_mut(sfd) {
                // RESP3 delivers pub/sub out-of-band as a push (`>`) frame; RESP2
                // uses a normal array. push_header emits the right one.
                match fpat {
                    None => {
                        resp::push_header(&mut c.wbuf, 3, c.resp3);
                        resp::bulk(&mut c.wbuf, b"message");
                        resp::bulk(&mut c.wbuf, fch);
                        resp::bulk(&mut c.wbuf, msg);
                    }
                    Some(p) => {
                        resp::push_header(&mut c.wbuf, 4, c.resp3);
                        resp::bulk(&mut c.wbuf, b"pmessage");
                        resp::bulk(&mut c.wbuf, p);
                        resp::bulk(&mut c.wbuf, fch);
                        resp::bulk(&mut c.wbuf, msg);
                    }
                }
                receivers += 1;
            }
        }
        // Flush deliveries (dedup fds so a doubly-subscribed conn flushes once).
        let mut flushed: HashSet<RawFd> = HashSet::new();
        for (sfd, _) in &targets {
            if flushed.insert(*sfd) {
                self.flush(*sfd);
            }
        }
        receivers
    }

    /// The tenant scope prefix (`{tenant}:`) for this connection, or `None` when
    /// no scoping applies (no auth configured, or an exempt/service role). Pub/sub
    /// channels are scoped by the same prefix as keys, so one tenant's
    /// SUBSCRIBE/PUBLISH cannot reach another's — the isolation keys already have.
    fn conn_prefix(&self, fd: RawFd) -> Option<Vec<u8>> {
        if self.auth.is_none() {
            return None;
        }
        let c = self.conns.get(&fd)?;
        if c.authed && !c.exempt {
            let mut p = Vec::with_capacity(c.tenant.len() + 1);
            p.extend_from_slice(c.tenant.as_bytes());
            p.push(b':');
            Some(p)
        } else {
            None // no auth on this conn, or an exempt role: raw (unscoped) namespace
        }
    }

    /// Scope a client-supplied channel/pattern to this connection's tenant
    /// namespace (a no-op for exempt/no-auth connections).
    fn scope_name(&self, fd: RawFd, raw: &[u8]) -> Vec<u8> {
        match self.conn_prefix(fd) {
            Some(mut p) => {
                p.extend_from_slice(raw);
                p
            }
            None => raw.to_vec(),
        }
    }

    /// Enforce the keyspace ACL and rewrite each key to `{tenant}:{key}` for a
    /// non-exempt authenticated role. Exempt roles (service_role,
    /// per `supatype_mask.exempt_roles`) bypass both. Returns the denial kind on
    /// the first key the role may not touch.
    fn apply_auth(
        &self,
        fd: RawFd,
        cmd: &[u8],
        key_idxs: &[usize],
        eff: &mut [Vec<u8>],
    ) -> Result<(), Deny> {
        let cfg = self.auth.as_ref().unwrap();
        let conn = &self.conns[&fd];
        if !conn.authed {
            return Err(Deny::NoAuth);
        }
        if conn.exempt {
            return Ok(()); // service_role / superuser: no scope, no ACL
        }
        let rules = cfg.acl.get(&conn.role);
        let write = is_write_cmd(cmd);
        for &i in key_idxs {
            let key = &eff[i];
            let ok = rules.map_or(false, |rs| {
                rs.iter().any(|r| {
                    key.starts_with(&r.prefix) && if write { r.can_write } else { r.can_read }
                })
            });
            if !ok {
                return Err(if cmd == b"GET" { Deny::Nil } else { Deny::Perm });
            }
            let mut scoped = Vec::with_capacity(conn.tenant.len() + 1 + key.len());
            scoped.extend_from_slice(conn.tenant.as_bytes());
            scoped.push(b':');
            scoped.extend_from_slice(key);
            eff[i] = scoped;
        }
        Ok(())
    }

    fn flush(&mut self, fd: RawFd) {
        if self.conns.get(&fd).map(|c| c.tls.is_some()).unwrap_or(false) {
            self.flush_tls(fd);
            return;
        }
        let c = match self.conns.get_mut(&fd) {
            Some(c) => c,
            None => return,
        };
        // Hold the reply while a durable write is still waiting to commit.
        if !c.ack.is_empty() {
            return;
        }
        while c.wpos < c.wbuf.len() {
            let slice = &c.wbuf[c.wpos..];
            let w = unsafe {
                libc::write(fd, slice.as_ptr() as *const libc::c_void, slice.len())
            };
            if w > 0 {
                c.wpos += w as usize;
            } else if w < 0 {
                let e = io::Error::last_os_error();
                match e.raw_os_error() {
                    Some(libc::EAGAIN) => {
                        if !c.want_write {
                            set_interest(&self.registry, fd, true);
                            c.want_write = true;
                        }
                        return;
                    }
                    Some(libc::EINTR) => continue,
                    _ => {
                        self.close(fd);
                        return;
                    }
                }
            }
        }
        // fully drained
        c.wbuf.clear();
        c.wpos = 0;
        if c.want_write {
            set_interest(&self.registry, fd, false);
            c.want_write = false;
        }
        if c.closing {
            self.close(fd);
        }
    }

    /// TLS write path: feed pending plaintext (`wbuf`) into the TLS session, then
    /// drain the resulting ciphertext to the socket. Also drives handshake writes
    /// (when `wbuf` is empty but the session `wants_write`).
    fn flush_tls(&mut self, fd: RawFd) {
        enum Act {
            None,
            WantWrite,
            Done,
            Close,
        }
        let act = {
            let c = match self.conns.get_mut(&fd) {
                Some(c) => c,
                None => return,
            };
            if !c.ack.is_empty() {
                return; // hold reply until its durable write commits
            }
            let tls = c.tls.as_mut().unwrap();
            // Feed not-yet-encrypted plaintext into the session (in-memory).
            if c.wpos < c.wbuf.len() {
                if let Ok(n) = tls.writer().write(&c.wbuf[c.wpos..]) {
                    c.wpos += n;
                }
            }
            // Drain ciphertext (handshake and/or app data) to the socket.
            let mut sock = FdIo(fd);
            let mut act = Act::None;
            while tls.wants_write() {
                match tls.write_tls(&mut sock) {
                    Ok(0) => break,
                    Ok(_) => continue,
                    Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                        act = Act::WantWrite;
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                    Err(_) => {
                        act = Act::Close;
                        break;
                    }
                }
            }
            if matches!(act, Act::None) {
                if c.wpos >= c.wbuf.len() && !tls.wants_write() {
                    c.wbuf.clear();
                    c.wpos = 0;
                    act = Act::Done;
                } else {
                    // short plaintext buffering or pending ciphertext: keep going
                    act = Act::WantWrite;
                }
            }
            act
        };
        match act {
            Act::WantWrite => {
                if self.conns.get(&fd).map(|c| !c.want_write).unwrap_or(false) {
                    set_interest(&self.registry, fd, true);
                    if let Some(c) = self.conns.get_mut(&fd) {
                        c.want_write = true;
                    }
                }
            }
            Act::Done => {
                let (ww, closing) = self
                    .conns
                    .get(&fd)
                    .map(|c| (c.want_write, c.closing))
                    .unwrap_or((false, false));
                if ww {
                    set_interest(&self.registry, fd, false);
                    if let Some(c) = self.conns.get_mut(&fd) {
                        c.want_write = false;
                    }
                }
                if closing {
                    self.close(fd);
                }
            }
            Act::Close => self.close(fd),
            Act::None => {}
        }
    }

    fn close(&mut self, fd: RawFd) {
        // drop this fd from every channel/pattern it was subscribed to.
        // Take the subscription sets out so we can mutate the reverse indexes and
        // the Bus without holding a borrow on self.conns.
        let (subs, psubs) = match self.conns.get_mut(&fd) {
            Some(c) => (std::mem::take(&mut c.subs), std::mem::take(&mut c.psubs)),
            None => (HashSet::new(), HashSet::new()),
        };
        for ch in &subs {
            if let Some(set) = self.channels.get_mut(ch) {
                if set.remove(&fd) {
                    if let Some(bus) = &self.bus {
                        bus.unsubscribe(self.worker_id, ch, false);
                    }
                }
                if set.is_empty() {
                    self.channels.remove(ch);
                }
            }
        }
        for pat in &psubs {
            if let Some(set) = self.patterns.get_mut(pat) {
                if set.remove(&fd) {
                    if let Some(bus) = &self.bus {
                        bus.unsubscribe(self.worker_id, pat, true);
                    }
                }
                if set.is_empty() {
                    self.patterns.remove(pat);
                }
            }
        }
        // Drop this fd from the client-side-caching tracking table (and the BCAST
        // set) so a future connection reusing the fd never inherits a stale
        // invalidation target.
        if !self.tracked.is_empty() {
            self.untrack_fd(fd);
        }
        self.bcast_subs.remove(&fd);
        if self.conns.get(&fd).map(|c| c.track.on).unwrap_or(false) {
            if let Some(bus) = &self.bus {
                bus.tracker_remove();
            }
        }
        // Deregister before closing: the fd must still be valid for deregister.
        let _ = self.registry.deregister(&mut SourceFd(&fd));
        unsafe { libc::close(fd) };
        self.conns.remove(&fd);
    }
}

#[inline]
fn itoa(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// Strip a tenant scope prefix from a channel/pattern for a client-facing frame.
/// `None` prefix (exempt / no-auth) or a non-matching name is returned as-is.
/// Whether a BCAST tracker with these (scoped) prefixes should be told about a
/// change to `key`. No prefixes means "the whole keyspace" (Valkey's default).
fn bcast_matches(prefixes: &[Vec<u8>], key: &[u8]) -> bool {
    prefixes.is_empty() || prefixes.iter().any(|p| key.starts_with(p))
}

fn strip_scope<'a>(name: &'a [u8], prefix: &Option<Vec<u8>>) -> &'a [u8] {
    match prefix {
        Some(p) if name.starts_with(p) => &name[p.len()..],
        _ => name,
    }
}

/// Redis-style glob match (PSUBSCRIBE / KEYS semantics): `*` any run, `?` one
/// byte, `[...]` a class (ranges with `-`, negated with `^`), `\` escapes.
fn glob_match(mut p: &[u8], mut s: &[u8]) -> bool {
    while let Some(&pc) = p.first() {
        match pc {
            b'*' => {
                while p.first() == Some(&b'*') {
                    p = &p[1..];
                }
                if p.is_empty() {
                    return true;
                }
                loop {
                    if glob_match(p, s) {
                        return true;
                    }
                    if s.is_empty() {
                        return false;
                    }
                    s = &s[1..];
                }
            }
            b'?' => {
                if s.is_empty() {
                    return false;
                }
                s = &s[1..];
                p = &p[1..];
            }
            b'[' => {
                if s.is_empty() {
                    return false;
                }
                let sc = s[0];
                p = &p[1..];
                let negate = p.first() == Some(&b'^');
                if negate {
                    p = &p[1..];
                }
                let mut matched = false;
                while let Some(&c) = p.first() {
                    if c == b']' {
                        break;
                    }
                    if c == b'\\' && p.len() >= 2 {
                        if p[1] == sc {
                            matched = true;
                        }
                        p = &p[2..];
                    } else if p.len() >= 3 && p[1] == b'-' && p[2] != b']' {
                        let (lo, hi) = (c.min(p[2]), c.max(p[2]));
                        if sc >= lo && sc <= hi {
                            matched = true;
                        }
                        p = &p[3..];
                    } else {
                        if c == sc {
                            matched = true;
                        }
                        p = &p[1..];
                    }
                }
                if p.first() == Some(&b']') {
                    p = &p[1..];
                }
                if matched == negate {
                    return false;
                }
                s = &s[1..];
            }
            b'\\' if p.len() >= 2 => {
                if s.is_empty() || s[0] != p[1] {
                    return false;
                }
                s = &s[1..];
                p = &p[2..];
            }
            c => {
                if s.is_empty() || s[0] != c {
                    return false;
                }
                s = &s[1..];
                p = &p[1..];
            }
        }
    }
    s.is_empty()
}

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";

/// Remaining TTL (micros) to re-apply so a read-modify-write of an aggregate
/// preserves the key's expiry; 0 (no expiry) stays 0.
fn remaining_ttl(exp: i64) -> i64 {
    if exp > 0 {
        (exp - now_micros()).max(1)
    } else {
        0
    }
}

/// A persistence record capturing a key's current stored state — its value,
/// kind and absolute expiry — or a tombstone if the key is gone. Called after a
/// TTL-only mutation (EXPIRE/PERSIST/GETEX) so the changed expiry is written
/// through to the durable tier instead of reverting to the last SET's TTL on
/// crash recovery; the enqueued record also carries the durable-tier ack.
fn stage_current_state(store: &Store, key: &[u8]) -> PendingWrite {
    match store.get_typed(key) {
        Some((kind, exp, blob)) => (key.to_vec(), blob.to_vec(), exp, kind as u8),
        None => (key.to_vec(), Vec::new(), DELETE_TOMBSTONE, b's'),
    }
}

/// Guard a string command: returns true if `key` is absent or holds a string;
/// otherwise writes `WRONGTYPE` and returns false. (A cached aggregate must not
/// be readable as a raw blob through `GET`/`INCR`/etc.)
/// Redis GETRANGE slice: inclusive `[start, end]`, negative indices count from
/// the end; out-of-range or start>end yields an empty slice.
/// xorshift64* — a tiny, dependency-free PRNG for the *RAND* commands, which
/// need no cryptographic quality. Seeded from the clock per call site.
fn rng_next(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

fn rand_seed() -> u64 {
    ((now_micros() as u64) ^ 0x9E37_79B9_7F4A_7C15) | 1
}

/// Choose element indices for HRANDFIELD / ZRANDMEMBER. `count >= 0` yields at
/// most `count` distinct indices (a partial Fisher–Yates shuffle); `count < 0`
/// yields exactly `|count|` indices with repeats allowed. `n` must be > 0.
fn pick_indices(n: usize, count: i64, seed: u64) -> Vec<usize> {
    let mut st = seed;
    if count < 0 {
        let k = count.unsigned_abs() as usize;
        (0..k).map(|_| (rng_next(&mut st) as usize) % n).collect()
    } else {
        let k = (count as usize).min(n);
        let mut idx: Vec<usize> = (0..n).collect();
        for i in 0..k {
            let j = i + (rng_next(&mut st) as usize) % (n - i);
            idx.swap(i, j);
        }
        idx.truncate(k);
        idx
    }
}

/// Emit a member+score list. RESP2 is a flat array `[m, s, m, s, …]` with the
/// scores as bulk strings; RESP3 uses double replies (`,`) for the scores and,
/// when `nested` is set, wraps each element as a two-element `[member, double]`
/// array (the shape real clients decode into member→score maps). Callers pass
/// `nested = resp3 && withscores` for the ZRANGE/ZUNION family and
/// `resp3 && count_given` for ZPOPMIN/ZPOPMAX (Valkey only pairs the counted
/// form). The flat, non-scored case is left to the caller.
fn reply_scored(out: &mut Vec<u8>, items: &[(Vec<u8>, f64)], nested: bool, resp3: bool) {
    if nested {
        resp::array_header(out, items.len());
        for (m, s) in items {
            resp::array_header(out, 2);
            resp::bulk(out, m);
            resp::double(out, &aggr::fmt_score(*s), resp3);
        }
    } else {
        resp::array_header(out, items.len() * 2);
        for (m, s) in items {
            resp::bulk(out, m);
            resp::double(out, &aggr::fmt_score(*s), resp3);
        }
    }
}

fn substr(v: &[u8], start: i64, end: i64) -> &[u8] {
    let len = v.len() as i64;
    if len == 0 {
        return &[];
    }
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if end < 0 { len + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e >= len {
        e = len - 1;
    }
    if s > e || s >= len {
        return &[];
    }
    &v[s as usize..=e as usize]
}

fn check_string(store: &Store, key: &[u8], out: &mut Vec<u8>) -> bool {
    match store.get_typed(key) {
        Some((k, _, _)) if k != crate::store::KIND_STR => {
            resp::error(out, WRONGTYPE);
            false
        }
        _ => true,
    }
}

/// Load the hash at `key` (empty if absent). Returns None and writes `WRONGTYPE`
/// to `out` if the key holds a non-hash value.
fn load_hash(store: &Store, key: &[u8], out: &mut Vec<u8>) -> Option<(aggr::Hash, i64)> {
    match store.get_typed(key) {
        None => Some((aggr::Hash::new(), 0)),
        Some((KIND_HASH, exp, v)) => Some((aggr::Hash::decode(v), exp)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Borrow the raw hash blob at `key` for an O(1)-average point read (HGET etc.)
/// without decoding every field. Outer `None` = WRONGTYPE (written to `out`);
/// inner `None` = key absent. The returned slice is the stored value verbatim,
/// consumed by `aggr::hash_probe`/`aggr::hash_count`.
fn hash_raw<'a>(store: &'a Store, key: &[u8], out: &mut Vec<u8>) -> Option<Option<&'a [u8]>> {
    match store.get_typed(key) {
        None => Some(None),
        Some((KIND_HASH, _, v)) => Some(Some(v)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Borrow the raw list blob at `key` for O(1) index/length reads (LLEN/LINDEX/
/// LRANGE) without decoding every element. Outer `None` = WRONGTYPE (written to
/// `out`); inner `None` = key absent. Consumed by `aggr::list_len`/`list_get`.
fn list_raw<'a>(store: &'a Store, key: &[u8], out: &mut Vec<u8>) -> Option<Option<&'a [u8]>> {
    match store.get_typed(key) {
        None => Some(None),
        Some((KIND_LIST, _, v)) => Some(Some(v)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Load the list at `key` (empty if absent). Returns None and writes `WRONGTYPE`
/// to `out` if the key holds a non-list value.
fn load_list(store: &Store, key: &[u8], out: &mut Vec<u8>) -> Option<(aggr::List, i64)> {
    match store.get_typed(key) {
        None => Some((aggr::List::new(), 0)),
        Some((KIND_LIST, exp, v)) => Some((aggr::List::decode(v), exp)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Store a list back, or delete the key if it became empty (Redis semantics).
#[must_use]
fn save_list(store: &Store, key: &[u8], l: &aggr::List, exp: i64, out: &mut Vec<u8>) -> bool {
    if l.is_empty() {
        store.del(key);
        return true;
    }
    wrote(out, store.set_typed(key, &l.encode(), remaining_ttl(exp), KIND_LIST))
}

/// Borrow the raw zset blob at `key` for sub-linear reads (ZSCORE/ZRANK/ZRANGE/
/// …) without decoding + re-sorting the whole set. Outer `None` = WRONGTYPE
/// (written to `out`); inner `None` = key absent. Consumed by the `aggr::zset_*`
/// raw readers.
fn zset_raw<'a>(store: &'a Store, key: &[u8], out: &mut Vec<u8>) -> Option<Option<&'a [u8]>> {
    match store.get_typed(key) {
        None => Some(None),
        Some((KIND_ZSET, _, v)) => Some(Some(v)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Load the sorted set at `key` (empty if absent). Returns None and writes
/// `WRONGTYPE` to `out` if the key holds a non-zset value.
fn load_zset(store: &Store, key: &[u8], out: &mut Vec<u8>) -> Option<(aggr::ZSet, i64)> {
    match store.get_typed(key) {
        None => Some((aggr::ZSet::new(), 0)),
        Some((KIND_ZSET, exp, v)) => Some((aggr::ZSet::decode(v), exp)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Store a sorted set back, or delete the key if it became empty.
#[must_use]
fn save_zset(store: &Store, key: &[u8], z: &aggr::ZSet, exp: i64, out: &mut Vec<u8>) -> bool {
    if z.is_empty() {
        store.del(key);
        return true;
    }
    wrote(out, store.set_typed(key, &z.encode(), remaining_ttl(exp), KIND_ZSET))
}

/// Borrow the raw set blob at `key` for O(1) SISMEMBER/SCARD off the stored
/// value. Outer `None` = WRONGTYPE (written to `out`); inner `None` = key absent.
fn set_raw<'a>(store: &'a Store, key: &[u8], out: &mut Vec<u8>) -> Option<Option<&'a [u8]>> {
    match store.get_typed(key) {
        None => Some(None),
        Some((KIND_SET, _, v)) => Some(Some(v)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Load the set at `key` (empty if absent). Returns None and writes `WRONGTYPE`
/// if the key holds a non-set value.
fn load_set(store: &Store, key: &[u8], out: &mut Vec<u8>) -> Option<(aggr::Set, i64)> {
    match store.get_typed(key) {
        None => Some((aggr::Set::new(), 0)),
        Some((KIND_SET, exp, v)) => Some((aggr::Set::decode(v), exp)),
        Some(_) => {
            resp::error(out, WRONGTYPE);
            None
        }
    }
}

/// Store a set back, or delete the key if it became empty (Redis semantics).
#[must_use]
fn save_set(store: &Store, key: &[u8], s: &aggr::Set, exp: i64, out: &mut Vec<u8>) -> bool {
    if s.is_empty() {
        store.del(key);
        return true;
    }
    wrote(out, store.set_typed(key, &s.encode(), remaining_ttl(exp), KIND_SET))
}

/// Parse the trailing options of an H/S/ZSCAN (`[MATCH p] [COUNT n] [NOVALUES]`)
/// starting at argument `start`. Returns the MATCH pattern (if any) and whether
/// NOVALUES was given (only meaningful, and only accepted, for HSCAN). COUNT is
/// a hint we ignore, but is consumed so it isn't mistaken for a pattern.
fn scan_opts(args: &[Vec<u8>], start: usize, allow_novalues: bool) -> (Option<Vec<u8>>, bool) {
    let mut pattern = None;
    let mut novalues = false;
    let mut i = start;
    while i < args.len() {
        match args[i].to_ascii_uppercase().as_slice() {
            b"MATCH" if i + 1 < args.len() => {
                pattern = Some(args[i + 1].clone());
                i += 2;
            }
            b"COUNT" if i + 1 < args.len() => i += 2,
            b"NOVALUES" if allow_novalues => {
                novalues = true;
                i += 1;
            }
            _ => i += 1,
        }
    }
    (pattern, novalues)
}

/// Union / intersection / difference of the given sets, selected by the command
/// name (the plain and *STORE variants share this). INTER/DIFF are computed
/// relative to the first set; an empty input yields an empty result.
fn set_combine(cmd: &[u8], sets: &[aggr::Set]) -> Vec<Vec<u8>> {
    if cmd == b"SUNION" || cmd == b"SUNIONSTORE" {
        let mut r = aggr::Set::new();
        for s in sets {
            for m in &s.members {
                r.add(m);
            }
        }
        return r.members;
    }
    if sets.is_empty() {
        return Vec::new();
    }
    let inter = cmd == b"SINTER" || cmd == b"SINTERSTORE";
    let mut r = Vec::new();
    'outer: for m in &sets[0].members {
        for s in &sets[1..] {
            // INTER keeps members present in every set; DIFF keeps members in
            // none of the rest.
            if s.contains(m) != inter {
                continue 'outer;
            }
        }
        r.push(m.clone());
    }
    r
}

/// AGGREGATE mode for the zset union/intersection commands.
#[derive(Clone, Copy)]
enum Agg {
    Sum,
    Min,
    Max,
}

fn agg_combine(agg: Agg, a: f64, b: f64) -> f64 {
    match agg {
        Agg::Sum => a + b,
        Agg::Min => a.min(b),
        Agg::Max => a.max(b),
    }
}

/// Weighted union / intersection / difference of sorted sets, returned in
/// (score, member) order. `op` is b"UNION" | b"INTER" | b"DIFF" (DIFF ignores
/// weights and aggregate: it keeps first-set members absent from the rest, with
/// their original scores).
fn zset_setop(op: &[u8], sets: &[(aggr::ZSet, f64)], agg: Agg) -> Vec<(Vec<u8>, f64)> {
    let mut result: Vec<(Vec<u8>, f64)> = if op == b"DIFF" {
        if sets.is_empty() {
            return Vec::new();
        }
        let mut r = Vec::new();
        'outer: for (m, s) in &sets[0].0.members {
            for (other, _) in &sets[1..] {
                if other.score(m).is_some() {
                    continue 'outer;
                }
            }
            r.push((m.clone(), *s));
        }
        r
    } else {
        use std::collections::HashMap;
        let mut acc: HashMap<Vec<u8>, f64> = HashMap::new();
        let mut counts: HashMap<Vec<u8>, usize> = HashMap::new();
        for (z, w) in sets {
            for (m, s) in &z.members {
                let val = s * w;
                acc.entry(m.clone())
                    .and_modify(|e| *e = agg_combine(agg, *e, val))
                    .or_insert(val);
                *counts.entry(m.clone()).or_insert(0) += 1;
            }
        }
        let n = sets.len();
        let inter = op == b"INTER";
        acc.into_iter()
            .filter(|(m, _)| !inter || counts.get(m).copied().unwrap_or(0) == n)
            .collect()
    };
    result.sort_by(|a, b| {
        a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0))
    });
    result
}

/// A ZRANGEBYLEX / ZLEXCOUNT bound: `-`, `+`, `[member` (inclusive) or
/// `(member` (exclusive).
enum LexBound {
    NegInf,
    PosInf,
    Incl(Vec<u8>),
    Excl(Vec<u8>),
}

fn parse_lex(b: &[u8]) -> Option<LexBound> {
    match b.first()? {
        b'-' if b.len() == 1 => Some(LexBound::NegInf),
        b'+' if b.len() == 1 => Some(LexBound::PosInf),
        b'[' => Some(LexBound::Incl(b[1..].to_vec())),
        b'(' => Some(LexBound::Excl(b[1..].to_vec())),
        _ => None,
    }
}

fn lex_ge(m: &[u8], lo: &LexBound) -> bool {
    match lo {
        LexBound::NegInf => true,
        LexBound::PosInf => false,
        LexBound::Incl(x) => m >= x.as_slice(),
        LexBound::Excl(x) => m > x.as_slice(),
    }
}

fn lex_le(m: &[u8], hi: &LexBound) -> bool {
    match hi {
        LexBound::PosInf => true,
        LexBound::NegInf => false,
        LexBound::Incl(x) => m <= x.as_slice(),
        LexBound::Excl(x) => m < x.as_slice(),
    }
}

/// Which argument positions of a command are keys (for ACL + tenant scoping).
/// Takes the full argv because some commands (the `numkeys`-prefixed set/zset
/// operations) only know which positions are keys after reading a count arg.
fn key_indices(cmd: &[u8], args: &[Vec<u8>]) -> Vec<usize> {
    let nargs = args.len();
    // `CMD numkeys key… [options]` — the keys are the `numkeys` args after the
    // count. Used by ZUNIONSTORE/ZINTERSTORE/ZDIFFSTORE/ZUNION/ZINTER/ZDIFF/
    // ZMPOP/SINTERCARD; `dst_first` also scopes the destination at arg 1.
    let numkeyed = |count_pos: usize, dst_first: bool| -> Vec<usize> {
        let n: usize = args
            .get(count_pos)
            .and_then(|a| std::str::from_utf8(a).ok())
            .and_then(|t| t.parse().ok())
            .unwrap_or(0);
        let mut v = Vec::new();
        if dst_first {
            v.push(1);
        }
        let first_key = count_pos + 1;
        for i in first_key..(first_key + n).min(nargs) {
            v.push(i);
        }
        v
    };
    match cmd {
        b"GET" | b"SET" | b"SETNX" | b"GETSET" | b"INCR" | b"DECR" | b"INCRBY" | b"DECRBY"
        | b"TTL" | b"PTTL" | b"EXPIRE" | b"PEXPIRE" | b"EXPIREAT" | b"PEXPIREAT" | b"PERSIST"
        | b"TYPE" | b"STRLEN" | b"APPEND" | b"GETDEL" | b"SETEX" | b"PSETEX" | b"GETEX"
        | b"GETRANGE" | b"SETRANGE" | b"INCRBYFLOAT" | b"EXPIRETIME" | b"PEXPIRETIME"
        // hashes + lists: the key is always the first argument
        | b"HSET" | b"HMSET" | b"HSETNX" | b"HGET" | b"HMGET" | b"HDEL" | b"HGETALL"
        | b"HKEYS" | b"HVALS" | b"HLEN" | b"HEXISTS" | b"HSTRLEN" | b"HINCRBY"
        | b"HINCRBYFLOAT" | b"HRANDFIELD"
        | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LPOP" | b"RPOP" | b"LLEN"
        | b"LINDEX" | b"LRANGE" | b"LSET" | b"LTRIM" | b"LINSERT" | b"LREM" | b"LPOS"
        | b"ZADD" | b"ZSCORE" | b"ZMSCORE" | b"ZCARD" | b"ZREM" | b"ZINCRBY"
        | b"ZRANK" | b"ZREVRANK" | b"ZRANGE" | b"ZREVRANGE" | b"ZRANGEBYSCORE"
        | b"ZREVRANGEBYSCORE" | b"ZCOUNT" | b"ZPOPMIN" | b"ZPOPMAX" | b"ZRANDMEMBER"
        | b"ZRANGEBYLEX" | b"ZREVRANGEBYLEX" | b"ZLEXCOUNT"
        // sets: the key is always the first argument
        | b"SADD" | b"SREM" | b"SCARD" | b"SISMEMBER" | b"SMISMEMBER" | b"SMEMBERS"
        | b"SPOP" | b"SRANDMEMBER"
        // container scans: the key is the first argument
        | b"HSCAN" | b"SSCAN" | b"ZSCAN" => {
            if nargs > 1 {
                vec![1]
            } else {
                vec![]
            }
        }
        b"DEL" | b"UNLINK" | b"EXISTS" | b"MGET" | b"TOUCH" => (1..nargs).collect(),
        // MSET/MSETNX interleave key value key value … — scope every key slot.
        b"MSET" | b"MSETNX" => (1..nargs).step_by(2).collect(),
        // RENAME/COPY/LMOVE/RPOPLPUSH/SMOVE take a source and destination key.
        b"RENAME" | b"RENAMENX" | b"COPY" | b"LMOVE" | b"RPOPLPUSH" | b"SMOVE" => {
            if nargs > 2 {
                vec![1, 2]
            } else {
                vec![]
            }
        }
        // Set operations: every argument is a key (dst + sources for the STORE
        // variants; all operands otherwise).
        b"SUNION" | b"SINTER" | b"SDIFF" | b"SUNIONSTORE" | b"SINTERSTORE" | b"SDIFFSTORE" => {
            (1..nargs).collect()
        }
        // OBJECT <SUBCOMMAND> key — the key is the third argument.
        b"OBJECT" => {
            if nargs > 2 {
                vec![2]
            } else {
                vec![]
            }
        }
        // ZRANGESTORE dst src … — destination and source keys.
        b"ZRANGESTORE" => {
            if nargs > 2 {
                vec![1, 2]
            } else {
                vec![]
            }
        }
        // `dst numkeys key…` — destination at arg 1, then `numkeys` source keys.
        b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE" => numkeyed(2, true),
        // `numkeys key…` — no destination.
        b"ZUNION" | b"ZINTER" | b"ZDIFF" | b"ZMPOP" | b"SINTERCARD" => numkeyed(1, false),
        _ => vec![],
    }
}

/// Commands that write (need `can_write` in the ACL).
fn is_write_cmd(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"SET" | b"SETNX"
            | b"GETSET"
            | b"INCR"
            | b"DECR"
            | b"INCRBY"
            | b"DECRBY"
            | b"DEL"
            | b"UNLINK"
            | b"EXPIRE"
            | b"PEXPIRE"
            | b"EXPIREAT"
            | b"PEXPIREAT"
            | b"PERSIST"
            | b"APPEND"
            | b"GETDEL"
            | b"MSET"
            | b"MSETNX"
            | b"SETEX"
            | b"PSETEX"
            | b"GETEX"
            | b"SETRANGE"
            | b"INCRBYFLOAT"
            | b"RENAME"
            | b"RENAMENX"
            | b"COPY"
            | b"FLUSHALL"
            | b"FLUSHDB"
            // hash mutations
            | b"HSET"
            | b"HMSET"
            | b"HSETNX"
            | b"HDEL"
            | b"HINCRBY"
            | b"HINCRBYFLOAT"
            // list mutations
            | b"LPUSH"
            | b"RPUSH"
            | b"LPUSHX"
            | b"RPUSHX"
            | b"LPOP"
            | b"RPOP"
            | b"LSET"
            | b"LTRIM"
            | b"LINSERT"
            | b"LREM"
            | b"LMOVE"
            | b"RPOPLPUSH"
            // zset mutations
            | b"ZADD"
            | b"ZREM"
            | b"ZINCRBY"
            | b"ZPOPMIN"
            | b"ZPOPMAX"
            | b"ZUNIONSTORE"
            | b"ZINTERSTORE"
            | b"ZDIFFSTORE"
            | b"ZRANGESTORE"
            | b"ZMPOP"
            // set mutations
            | b"SADD"
            | b"SREM"
            | b"SPOP"
            | b"SMOVE"
            | b"SUNIONSTORE"
            | b"SINTERSTORE"
            | b"SDIFFSTORE"
    )
}

/// Aggregate (hash/list/zset) mutations — their whole blob is persisted from the
/// final store state after dispatch (durable aggregates).
fn is_aggregate_write(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"HSET" | b"HMSET" | b"HSETNX" | b"HDEL" | b"HINCRBY" | b"HINCRBYFLOAT"
            | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LPOP" | b"RPOP" | b"LSET" | b"LTRIM"
            | b"LINSERT" | b"LREM"
            | b"ZADD" | b"ZREM" | b"ZINCRBY" | b"ZPOPMIN" | b"ZPOPMAX"
            // *STORE persists its destination (arg 1); ZMPOP/ZRANGESTORE stage manually
            | b"ZUNIONSTORE" | b"ZINTERSTORE" | b"ZDIFFSTORE"
            // sets: SMOVE stages both keys manually, so it is not auto-staged here
            | b"SADD" | b"SREM" | b"SPOP" | b"SUNIONSTORE" | b"SINTERSTORE" | b"SDIFFSTORE"
    )
}

/// Hand a write to the commit batcher for logged tiers. Ephemeral writes
/// stay shmem-authoritative and never touch disk.
#[inline]
fn durable_log(batcher: &Option<Arc<Batcher>>, tier: Tier, key: &[u8], val: &[u8]) {
    if let Some(b) = batcher {
        if tier != Tier::Ephemeral {
            let mut rec = Vec::with_capacity(key.len() + val.len() + 1);
            rec.extend_from_slice(key);
            rec.push(b'=');
            rec.extend_from_slice(val);
            b.commit(tier, &rec);
        }
    }
}

// ---- TLS ----------------------------------------------------------------

/// A `Read`/`Write` adapter over a raw non-blocking fd for `rustls`' buffered
/// `read_tls`/`write_tls`. `EAGAIN` surfaces as `WouldBlock`, which rustls
/// handles by processing whatever it already has.
struct FdIo(RawFd);

impl Read for FdIo {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let r = unsafe { libc::read(self.0, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if r >= 0 {
            Ok(r as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }
}

impl Write for FdIo {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let w = unsafe { libc::write(self.0, buf.as_ptr() as *const libc::c_void, buf.len()) };
        if w >= 0 {
            Ok(w as usize)
        } else {
            Err(io::Error::last_os_error())
        }
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Build a rustls server config from PEM cert + key files (TLS). Accepts a
/// PKCS#8, RSA, or SEC1/EC private key. Used by the extension when both
/// `pg_keyspace.tls_cert_file` and `pg_keyspace.tls_key_file` are set.
pub fn load_tls_config(cert_path: &str, key_path: &str) -> io::Result<Arc<rustls::ServerConfig>> {
    use std::fs::File;
    use std::io::BufReader;

    let mut cert_rd = BufReader::new(File::open(cert_path)?);
    let certs: Vec<rustls::Certificate> = rustls_pemfile::certs(&mut cert_rd)?
        .into_iter()
        .map(rustls::Certificate)
        .collect();
    if certs.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("no certificates in {cert_path}"),
        ));
    }

    let key = {
        let der = {
            let mut rd = BufReader::new(File::open(key_path)?);
            let mut k = rustls_pemfile::pkcs8_private_keys(&mut rd)?;
            if k.is_empty() {
                let mut rd = BufReader::new(File::open(key_path)?);
                k = rustls_pemfile::rsa_private_keys(&mut rd)?;
            }
            if k.is_empty() {
                let mut rd = BufReader::new(File::open(key_path)?);
                k = rustls_pemfile::ec_private_keys(&mut rd)?;
            }
            k.into_iter().next().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("no private key in {key_path}"),
                )
            })?
        };
        rustls::PrivateKey(der)
    };

    let config = rustls::ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
    Ok(Arc::new(config))
}

// ---- socket / poller helpers --------------------------------------------

fn listen(addr: &str, port: u16) -> io::Result<RawFd> {
    unsafe {
        // SOCK_NONBLOCK on socket() is a Linux extension; create the socket and
        // set O_NONBLOCK portably instead (macOS has no SOCK_NONBLOCK).
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        set_nonblocking(fd);
        let one: libc::c_int = 1;
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEADDR,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as u32,
        );
        libc::setsockopt(
            fd,
            libc::SOL_SOCKET,
            libc::SO_REUSEPORT,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as u32,
        );
        let mut sa: libc::sockaddr_in = std::mem::zeroed();
        // sin_family is u16 on Linux, u8 on the BSDs/macOS — `as _` picks the
        // right width from the field type. (macOS's extra sin_len stays 0, which
        // bind accepts.)
        sa.sin_family = libc::AF_INET as _;
        sa.sin_port = port.to_be();
        sa.sin_addr.s_addr = inet_addr(addr);
        if libc::bind(
            fd,
            &sa as *const _ as *const libc::sockaddr,
            std::mem::size_of::<libc::sockaddr_in>() as u32,
        ) != 0
        {
            let e = io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }
        if libc::listen(fd, 1024) != 0 {
            let e = io::Error::last_os_error();
            libc::close(fd);
            return Err(e);
        }
        Ok(fd)
    }
}

/// Accept one connection, returning a non-blocking client fd (or <0 on EAGAIN /
/// error). Linux does this in one syscall (`accept4` + `SOCK_NONBLOCK`); macOS
/// has no `accept4`, so accept then set O_NONBLOCK.
fn accept_nonblocking(listen_fd: RawFd) -> RawFd {
    #[cfg(target_os = "linux")]
    unsafe {
        libc::accept4(
            listen_fd,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            libc::SOCK_NONBLOCK,
        )
    }
    #[cfg(not(target_os = "linux"))]
    unsafe {
        let fd = libc::accept(listen_fd, std::ptr::null_mut(), std::ptr::null_mut());
        if fd >= 0 {
            set_nonblocking(fd);
        }
        fd
    }
}

/// Set O_NONBLOCK on `fd` (portable; used where SOCK_NONBLOCK isn't available).
fn set_nonblocking(fd: RawFd) {
    unsafe {
        let flags = libc::fcntl(fd, libc::F_GETFL, 0);
        if flags >= 0 {
            libc::fcntl(fd, libc::F_SETFL, flags | libc::O_NONBLOCK);
        }
    }
}

fn inet_addr(addr: &str) -> u32 {
    // supports dotted-quad only (127.0.0.1 / 0.0.0.0)
    let mut bytes = [0u8; 4];
    for (i, part) in addr.split('.').enumerate().take(4) {
        bytes[i] = part.parse().unwrap_or(0);
    }
    u32::from_ne_bytes(bytes)
}

fn set_nodelay(fd: RawFd) {
    let one: libc::c_int = 1;
    unsafe {
        libc::setsockopt(
            fd,
            libc::IPPROTO_TCP,
            libc::TCP_NODELAY,
            &one as *const _ as *const libc::c_void,
            std::mem::size_of::<libc::c_int>() as u32,
        );
    }
}

/// (Re)set a connection's readiness interest: always readable, plus writable
/// while its write buffer is backed up (`want_write`). Takes the registry by ref
/// so callers can invoke it while holding a `&mut` borrow of `conns`.
fn set_interest(registry: &Registry, fd: RawFd, want_write: bool) {
    let interest = if want_write {
        Interest::READABLE | Interest::WRITABLE
    } else {
        Interest::READABLE
    };
    let _ = registry.reregister(&mut SourceFd(&fd), Token(fd as usize), interest);
}

#[cfg(test)]
mod auth_tests {
    use super::*;

    fn cred(secret: &str) -> Cred {
        Cred { secret: secret.to_string(), role: "r".into(), tenant: "".into() }
    }

    #[test]
    fn plaintext_verify() {
        let c = cred("hunter2");
        assert!(c.verify(b"hunter2"));
        assert!(!c.verify(b"hunter3"));
        assert!(!c.verify(b"hunter2 "));
    }

    #[test]
    fn hashed_verify_roundtrip() {
        // build sha256$<salt>$<hash> exactly as supacache.set_credential does
        use sha2::{Digest, Sha256};
        let salt = [0xA1u8, 0xB2, 0xC3, 0xD4, 0xE5, 0xF6, 0x07, 0x18];
        let pass = b"correct horse battery staple";
        let mut h = Sha256::new();
        h.update(salt);
        h.update(pass);
        let digest = h.finalize();
        let salt_hex: String = salt.iter().map(|b| format!("{b:02x}")).collect();
        let hash_hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
        let c = cred(&format!("sha256${salt_hex}${hash_hex}"));
        assert!(c.verify(pass));
        assert!(!c.verify(b"wrong"));
        assert!(!c.verify(b""));
    }

    #[test]
    fn ct_eq_basics() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
        assert!(ct_eq(b"", b""));
    }

    #[test]
    fn malformed_hash_is_rejected() {
        assert!(!cred("sha256$nothex$deadbeef").verify(b"x"));
        assert!(!cred("sha256$").verify(b"x"));
        assert!(!cred("sha256$aa").verify(b"x")); // missing hash segment
    }

    #[test]
    fn glob_matches() {
        assert!(glob_match(b"*", b"anything"));
        assert!(glob_match(b"news.*", b"news.tech"));
        assert!(!glob_match(b"news.*", b"sports.x"));
        assert!(glob_match(b"h?llo", b"hello"));
        assert!(glob_match(b"h?llo", b"hallo"));
        assert!(!glob_match(b"h?llo", b"hllo")); // ? needs exactly one byte
        assert!(glob_match(b"h[ae]llo", b"hello"));
        assert!(glob_match(b"h[ae]llo", b"hallo"));
        assert!(!glob_match(b"h[ae]llo", b"hillo"));
        assert!(glob_match(b"h[a-c]t", b"hbt"));
        assert!(!glob_match(b"h[a-c]t", b"hdt"));
        assert!(glob_match(b"h[^x]t", b"hat"));
        assert!(!glob_match(b"h[^x]t", b"hxt"));
        assert!(glob_match(b"a\\*b", b"a*b")); // escaped star is literal
        assert!(!glob_match(b"a\\*b", b"axb"));
        assert!(glob_match(b"", b""));
        assert!(!glob_match(b"", b"x"));
        assert!(glob_match(b"*.*.*", b"a.b.c"));
    }
}
