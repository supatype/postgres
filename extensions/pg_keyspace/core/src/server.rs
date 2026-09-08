//! One slot worker: an epoll event loop over non-blocking sockets, parsing RESP
//! and dispatching against its own shared-memory partition (§3.1). No
//! transaction is ever opened on this path; a command is a shmem read/write
//! plus, for logged tiers, a handoff to the commit batcher (§3.4). This is the
//! hot path the latency benchmarks measure.

use crate::aggr;
use crate::batcher::{Batcher, Tier};
use crate::crc16;
use crate::pubsub;
use crate::resp::{self, Parse};
use crate::ring;
use crate::store::{now_micros, Lookup, Store, KIND_HASH, KIND_LIST, KIND_ZSET};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{Read, Write};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

const EPOLL_MAX: usize = 1024;
const READ_CHUNK: usize = 64 * 1024;

/// A staged write: (key, value, expires_at_micros, kind). `kind` is the value's
/// type tag (§5) so aggregates persist and recover as the right type.
pub type PendingWrite = (Vec<u8>, Vec<u8>, i64, u8);

/// Sentinel `expires_at` marking a delete (tombstone) carried through the ring,
/// so the persistence worker removes the key from the backing table instead of
/// upserting it — otherwise a deleted key would resurrect on crash recovery.
pub const DELETE_TOMBSTONE: i64 = -1;

/// Enqueue one record into the ring sharded by key slot (same shard function as
/// the write path, so a key's writes and deletes always reach the same worker).
///
/// No-loss backpressure: if the ring is full, wait (bounded) for the persistence
/// worker to drain rather than dropping the write. Under sustained overload this
/// throttles the RESP write path to the drain rate instead of silently losing
/// durability. The bound only trips if persistence is wedged, in which case the
/// record is dropped and counted (`ring_stats.dropped`) — a loud, rare event.
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
    let shard = if n == 1 {
        0
    } else {
        crc16::key_slot(key) as usize % n
    };
    let p = &producers[shard];
    if let Some(seq) = p.push(key, val, exp, kind) {
        return Some((shard, seq));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        std::thread::sleep(Duration::from_micros(50));
        if let Some(seq) = p.push(key, val, exp, kind) {
            return Some((shard, seq));
        }
        if Instant::now() >= deadline {
            p.note_drop(); // persistence wedged: drop + count, rather than hang forever
            return None;
        }
    }
}

// ---- P2 security: RESP AUTH -> role, keyspace ACL, forced tenant scoping ----

/// A RESP credential (§4.5): maps an AUTH username to a Postgres role + tenant.
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

/// One keyspace ACL rule: a role may read/write keys under `prefix` (§4.5).
#[derive(Clone)]
pub struct AclRule {
    pub prefix: Vec<u8>,
    pub can_read: bool,
    pub can_write: bool,
}

/// The full auth configuration, loaded from SQL by the extension and handed to
/// the worker. When present, RESP AUTH is required for keyed commands; when
/// absent, the worker runs in local/no-auth mode (matches §10 local dev).
pub struct AuthConfig {
    pub creds: HashMap<String, Cred>,
    pub acl: HashMap<String, Vec<AclRule>>,
    pub exempt: HashSet<String>, // roles that bypass ACL + scoping (§4.5)
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
    // durable sync-ack: (ring, seq) the connection's reply is waiting on. While
    // non-empty the reply is held (not flushed) and no further commands are read.
    ack: Vec<(usize, u64)>,
    // When set, this connection is TLS: ciphertext on the socket, plaintext in
    // rbuf/wbuf. `wpos` then counts wbuf bytes already fed to the TLS writer.
    tls: Option<Box<rustls::ServerConnection>>,
    // P3 pub/sub (§5): channels and glob patterns this connection is subscribed
    // to. Non-empty => the connection is in RESP2 subscribe mode.
    subs: HashSet<Vec<u8>>,
    psubs: HashSet<Vec<u8>>,
}

pub struct Worker {
    store: Arc<Store>,
    batcher: Option<Arc<Batcher>>,
    tier: Tier,
    listen_fd: RawFd,
    epfd: RawFd,
    conns: HashMap<RawFd, Conn>,
    args: Vec<(usize, usize)>,
    // P1: when non-empty, every write is enqueued into one of these shared-memory
    // rings (sharded by key slot) and a dedicated persistence worker drains each
    // — the RESP path never touches SPI. Multiple rings scale durable writes.
    producers: Vec<ring::Producer>,
    // P2: when set, RESP AUTH is required and keys are ACL-checked + tenant-scoped.
    auth: Option<AuthConfig>,
    // durable tier: hold each write's RESP reply until its ring record commits.
    sync_ack: bool,
    // P2 TLS: when set, every accepted connection is wrapped in a TLS session so
    // the RESP wire is encrypted (the AUTH password is otherwise sent in clear).
    tls_config: Option<Arc<rustls::ServerConfig>>,
    // P3 pub/sub (§5): reverse indexes channel/pattern -> subscriber fds, so a
    // PUBLISH fans out without scanning every connection. Local to this worker.
    channels: HashMap<Vec<u8>, HashSet<RawFd>>,
    patterns: HashMap<Vec<u8>, HashSet<RawFd>>,
    // P3 cross-worker pub/sub (§5): when workers share a process (the scale-out
    // daemon), a shared Bus routes a PUBLISH to subscribers on *other* workers.
    // `None` for the single-worker in-PG extension (local delivery only).
    bus: Option<Arc<pubsub::Bus>>,
    worker_id: usize,
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
        let epfd = unsafe { libc::epoll_create1(0) };
        if epfd < 0 {
            return Err(io::Error::last_os_error());
        }
        epoll_add(epfd, listen_fd, libc::EPOLLIN as u32)?;
        Ok(Worker {
            store,
            batcher,
            tier,
            listen_fd,
            epfd,
            conns: HashMap::new(),
            args: Vec::with_capacity(8),
            producers: Vec::new(),
            auth: None,
            sync_ack: false,
            tls_config: None,
            channels: HashMap::new(),
            patterns: HashMap::new(),
            bus: None,
            worker_id: 0,
        })
    }

    /// Join a cross-worker pub/sub Bus as worker `worker_id`. The Bus's wake
    /// eventfd is added to this worker's epoll set so remote deliveries arrive
    /// promptly. Only used by the multi-worker daemon; the in-PG extension runs
    /// a single worker and never calls this.
    pub fn set_bus(&mut self, bus: Arc<pubsub::Bus>, worker_id: usize) {
        let wfd = bus.wake_fd(worker_id);
        let _ = epoll_add(self.epfd, wfd, libc::EPOLLIN as u32);
        self.bus = Some(bus);
        self.worker_id = worker_id;
    }

    /// Durable tier: hold each write's reply until its ring record has committed.
    pub fn set_sync_ack(&mut self, on: bool) {
        self.sync_ack = on;
    }

    /// Enable TLS: every accepted connection is wrapped in a server-side TLS
    /// session, so the RESP wire (including the AUTH password) is encrypted.
    pub fn set_tls_config(&mut self, cfg: Arc<rustls::ServerConfig>) {
        self.tls_config = Some(cfg);
    }

    /// Enable P1 persistence: writes are sharded by key slot across these rings,
    /// each drained by its own persistence worker.
    pub fn set_ring_producers(&mut self, producers: Vec<ring::Producer>) {
        self.producers = producers;
    }

    /// Enable P2 access control: RESP AUTH required, keyspace ACL + tenant scope.
    pub fn set_auth_config(&mut self, auth: AuthConfig) {
        self.auth = Some(auth);
    }

    pub fn run(&mut self) -> io::Result<()> {
        self.run_with(|| Tick::Continue, -1)
    }

    /// Run the event loop, calling `tick()` once per iteration (and whenever a
    /// signal interrupts the wait). `timeout_ms` bounds each `epoll_wait` so the
    /// tick runs even when idle (a Postgres background worker uses this to notice
    /// SIGTERM/SIGHUP). `Tick::Stop` ends the loop; `Tick::Reload(cfg)` swaps the
    /// auth config live — the hot-reload hook the extension drives on SIGHUP, so
    /// credential changes need no restart (`None` = switch to no-auth).
    pub fn run_with<F: FnMut() -> Tick>(&mut self, mut tick: F, timeout_ms: i32) -> io::Result<()> {
        let mut events: Vec<libc::epoll_event> =
            vec![unsafe { std::mem::zeroed() }; EPOLL_MAX];
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
            let n = unsafe {
                libc::epoll_wait(self.epfd, events.as_mut_ptr(), EPOLL_MAX as i32, timeout_ms)
            };
            if n < 0 {
                let e = io::Error::last_os_error();
                if e.raw_os_error() == Some(libc::EINTR) {
                    continue;
                }
                return Err(e);
            }
            for ev in events.iter().take(n as usize) {
                let fd = ev.u64 as RawFd;
                if fd == self.listen_fd {
                    self.accept_all();
                } else if self.bus.as_ref().map_or(false, |b| fd == b.wake_fd(self.worker_id)) {
                    // A remote worker published to a channel/pattern we hold a
                    // subscriber for: drain the inbox and deliver locally (never
                    // re-broadcast — deliver_local is local-only).
                    let msgs = self.bus.as_ref().unwrap().drain(self.worker_id);
                    for (channel, msg) in msgs {
                        self.deliver_local(&channel, &msg);
                    }
                } else {
                    let flags = ev.events;
                    if flags & (libc::EPOLLIN as u32) != 0 {
                        self.on_readable(fd);
                    }
                    if self.conns.contains_key(&fd)
                        && flags & (libc::EPOLLOUT as u32) != 0
                    {
                        self.flush(fd);
                    }
                }
            }
            // Durable tier: release replies whose ring records have committed.
            if self.sync_ack {
                self.resolve_acks();
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
            let cfd = unsafe {
                libc::accept4(self.listen_fd, std::ptr::null_mut(), std::ptr::null_mut(), libc::SOCK_NONBLOCK)
            };
            if cfd < 0 {
                break; // EAGAIN
            }
            set_nodelay(cfd);
            if epoll_add(self.epfd, cfd, libc::EPOLLIN as u32).is_err() {
                unsafe { libc::close(cfd) };
                continue;
            }
            let tls = match &self.tls_config {
                Some(cfg) => match rustls::ServerConnection::new(cfg.clone()) {
                    Ok(s) => Some(Box::new(s)),
                    Err(_) => {
                        let _ = epoll_del(self.epfd, cfd);
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
                    tls,
                    subs: HashSet::new(),
                    psubs: HashSet::new(),
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
        // Do not read more commands while a durable reply is still pending — the
        // reply must land before the next command's, and this backpressures the
        // connection to its own commit rate.
        if self.conns.get(&fd).map(|c| !c.ack.is_empty()).unwrap_or(true) {
            return;
        }
        let mut consumed_total = 0usize;
        loop {
            // Parse one command and materialise its args as owned bytes, so the
            // immutable borrow of rbuf is dropped before we touch wbuf/store.
            let (parse, cmd_args) = {
                let c = match self.conns.get(&fd) {
                    Some(c) => c,
                    None => return,
                };
                let buf = &c.rbuf[consumed_total..];
                let parse = resp::parse(buf, &mut self.args);
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
                    consumed_total += consumed;
                    let stop = self
                        .conns
                        .get(&fd)
                        .map(|c| c.closing || !c.ack.is_empty())
                        .unwrap_or(true);
                    if stop {
                        break; // durable reply pending or connection closing
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

        // ---- P2: AUTH command ----
        if cmd == b"AUTH" {
            self.handle_auth(fd, args);
            return;
        }

        // ---- P3 pub/sub (§5): handled before keyed-command scoping. Channels
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

        // ---- P2: auth gate + forced tenant scoping for keyed commands ----
        // `eff` holds the args actually used below; key positions are rewritten
        // to `{tenant}:{key}` for non-exempt authenticated roles.
        let mut eff: Vec<Vec<u8>> = Vec::new();
        let key_idxs = key_indices(&cmd, nargs);
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
                    Deny::Nil => resp::nil(out),
                }
                return;
            }
        }
        // From here on, use the (possibly scoped) args.
        let args: &[Vec<u8>] = if eff.is_empty() { args } else { &eff };

        let store = self.store.clone();
        let batcher = self.batcher.clone();
        let tier = self.tier;
        let persist_on = !self.producers.is_empty();
        let sync_ack = self.sync_ack;
        // A write to enqueue for persistence (§3.3), applied after the match so
        // it does not tangle with the `out` borrow.
        let mut stage: Option<PendingWrite> = None;
        // (ring, seq) records enqueued this command; a durable write's reply is
        // held until all of them commit.
        let mut acks: Vec<(usize, u64)> = Vec::new();
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
            b"HELLO" => resp::error(out, "NOPROTO unsupported, use RESP2"),
            b"CLIENT" | b"CONFIG" | b"SELECT" | b"RESET" => resp::simple(out, "OK"),
            b"COMMAND" => resp::array_header(out, 0),
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
                    Lookup::Miss => resp::nil(out),
                }
            }
            b"SET" => {
                if nargs < 3 {
                    resp::error(out, "ERR wrong number of arguments for 'set'");
                    return;
                }
                let mut ttl_micros = 0i64;
                let mut i = 3;
                while i + 1 < nargs {
                    let mut opt = args[i].clone();
                    opt.make_ascii_uppercase();
                    let n: i64 = std::str::from_utf8(&args[i + 1])
                        .ok()
                        .and_then(|t| t.parse().ok())
                        .unwrap_or(0);
                    match opt.as_slice() {
                        b"EX" => ttl_micros = n * 1_000_000,
                        b"PX" => ttl_micros = n * 1_000,
                        _ => {}
                    }
                    i += 2;
                }
                store.set(&args[1], &args[2], ttl_micros);
                resp::simple(out, "OK");
                durable_log(&batcher, tier, &args[1], &args[2]);
                if persist_on {
                    let exp = if ttl_micros > 0 { now_micros() + ttl_micros } else { 0 };
                    stage = Some((args[1].clone(), args[2].clone(), exp, b's'));
                }
            }
            b"SETNX" => {
                let exists = matches!(store.get(&args[1]), Lookup::Hit(_));
                let n = if exists {
                    0
                } else {
                    store.set(&args[1], &args[2], 0);
                    durable_log(&batcher, tier, &args[1], &args[2]);
                    if persist_on {
                        stage = Some((args[1].clone(), args[2].clone(), 0, b's'));
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
                store.set(&args[1], &args[2], 0);
                durable_log(&batcher, tier, &args[1], &args[2]);
                if persist_on {
                    stage = Some((args[1].clone(), args[2].clone(), 0, b's'));
                }
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                match old {
                    Some(v) => resp::bulk(out, &v),
                    None => resp::nil(out),
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
                            stage = Some((args[1].clone(), s, 0, b's'));
                        }
                    }
                    None => resp::error(out, "ERR value is not an integer or out of range"),
                }
            }
            // ---- P3 §5: TYPE + hashes ------------------------------------
            b"TYPE" => {
                if nargs < 2 {
                    resp::error(out, "ERR wrong number of arguments for 'type'");
                } else {
                    let t = match store.get_typed(&args[1]) {
                        None => "none",
                        Some((KIND_HASH, _, _)) => "hash",
                        Some((k, _, _)) if k == crate::store::KIND_LIST => "list",
                        Some((k, _, _)) if k == crate::store::KIND_ZSET => "zset",
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
                store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH);
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
                    store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH);
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
                    None => resp::nil(out),
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
                        None => resp::nil(out),
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
                    store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH);
                }
                resp::integer(out, removed);
            }
            b"HGETALL" => {
                let (h, _) = match load_hash(&store, &args[1], out) {
                    Some(x) => x,
                    None => return,
                };
                resp::array_header(out, h.len() * 2);
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
                store.set_typed(&args[1], &h.encode(), remaining_ttl(exp), KIND_HASH);
                resp::integer(out, next);
            }
            // ---- P3 §5: lists --------------------------------------------
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
                save_list(&store, &args[1], &l, exp);
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
                            None => resp::nil(out),
                        }
                    }
                    Some(c) => {
                        if l.is_empty() {
                            resp::nil(out);
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
                save_list(&store, &args[1], &l, exp);
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
                    None => resp::nil(out),
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
                        save_list(&store, &args[1], &l, exp);
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
                save_list(&store, &args[1], &l, exp);
                resp::simple(out, "OK");
            }
            // ---- P3 §5: sorted sets --------------------------------------
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
                save_zset(&store, &args[1], &z, exp);
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
                    Some(s) => resp::bulk(out, aggr::fmt_score(s).as_bytes()),
                    None => resp::nil(out),
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
                        Some(s) => resp::bulk(out, aggr::fmt_score(s).as_bytes()),
                        None => resp::nil(out),
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
                save_zset(&store, &args[1], &z, exp);
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
                let next = z.score(&args[3]).unwrap_or(0.0) + by;
                z.add(&args[3], next);
                save_zset(&store, &args[1], &z, exp);
                resp::bulk(out, aggr::fmt_score(next).as_bytes());
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
                    None => resp::nil(out),
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
                resp::array_header(out, if withscores { items.len() * 2 } else { items.len() });
                for (m, s) in &items {
                    resp::bulk(out, m);
                    if withscores {
                        resp::bulk(out, aggr::fmt_score(*s).as_bytes());
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
                resp::array_header(out, if withscores { items.len() * 2 } else { items.len() });
                for (m, s) in &items {
                    resp::bulk(out, m);
                    if withscores {
                        resp::bulk(out, aggr::fmt_score(*s).as_bytes());
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
            _ => resp::error(out, "ERR unknown command"),
        }

        // P3 durable aggregates (§5/§3.3): a mutation of a hash/list/zset persists
        // its whole (kind-tagged) blob from the final store state — or a tombstone
        // if the key was emptied/deleted — so it recovers as the right type.
        if persist_on && stage.is_none() && is_aggregate_write(&cmd) && nargs >= 2 {
            stage = match store.get_typed(&args[1]) {
                Some((kind, exp, blob)) => {
                    Some((args[1].clone(), blob.to_vec(), exp, kind as u8))
                }
                None => Some((args[1].clone(), Vec::new(), DELETE_TOMBSTONE, b's')),
            };
        }

        if let Some((k, v, e, kind)) = stage {
            // sharded so a given key always lands on the same ring/persist worker
            // — no cross-worker key conflicts on ON CONFLICT.
            if let Some(sa) = shard_push(&self.producers, &k, &v, e, kind) {
                acks.push(sa);
            }
        }
        // Durable tier: hold this command's reply until its record(s) commit.
        if sync_ack && !acks.is_empty() {
            self.conns.get_mut(&fd).unwrap().ack = acks;
        }
    }

    /// RESP `AUTH [user] pass` — resolve the credential and set the connection's
    /// role/tenant/exempt state (§4.5). Computed in two phases so the borrow of
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
                match fpat {
                    None => {
                        resp::array_header(&mut c.wbuf, 3);
                        resp::bulk(&mut c.wbuf, b"message");
                        resp::bulk(&mut c.wbuf, fch);
                        resp::bulk(&mut c.wbuf, msg);
                    }
                    Some(p) => {
                        resp::array_header(&mut c.wbuf, 4);
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
    /// channels are scoped by the same prefix as keys (§4.4/§4.5), so one tenant's
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
    /// non-exempt authenticated role (§4.4/§4.5). Exempt roles (service_role,
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
                            let _ = epoll_mod(
                                self.epfd,
                                fd,
                                (libc::EPOLLIN | libc::EPOLLOUT) as u32,
                            );
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
            let _ = epoll_mod(self.epfd, fd, libc::EPOLLIN as u32);
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
                    let _ = epoll_mod(self.epfd, fd, (libc::EPOLLIN | libc::EPOLLOUT) as u32);
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
                    let _ = epoll_mod(self.epfd, fd, libc::EPOLLIN as u32);
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
        // P3: drop this fd from every channel/pattern it was subscribed to.
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
        let _ = epoll_del(self.epfd, fd);
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

/// Guard a string command: returns true if `key` is absent or holds a string;
/// otherwise writes `WRONGTYPE` and returns false. (A cached aggregate must not
/// be readable as a raw blob through `GET`/`INCR`/etc.)
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
fn save_list(store: &Store, key: &[u8], l: &aggr::List, exp: i64) {
    if l.is_empty() {
        store.del(key);
    } else {
        store.set_typed(key, &l.encode(), remaining_ttl(exp), KIND_LIST);
    }
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
fn save_zset(store: &Store, key: &[u8], z: &aggr::ZSet, exp: i64) {
    if z.is_empty() {
        store.del(key);
    } else {
        store.set_typed(key, &z.encode(), remaining_ttl(exp), KIND_ZSET);
    }
}

/// Which argument positions of a command are keys (for ACL + tenant scoping).
fn key_indices(cmd: &[u8], nargs: usize) -> Vec<usize> {
    match cmd {
        b"GET" | b"SET" | b"SETNX" | b"GETSET" | b"INCR" | b"DECR" | b"INCRBY" | b"DECRBY"
        | b"TTL" | b"EXPIRE" | b"PERSIST" | b"TYPE" | b"STRLEN" | b"APPEND" | b"GETDEL"
        // P3 hashes + lists: the key is always the first argument
        | b"HSET" | b"HMSET" | b"HSETNX" | b"HGET" | b"HMGET" | b"HDEL" | b"HGETALL"
        | b"HKEYS" | b"HVALS" | b"HLEN" | b"HEXISTS" | b"HSTRLEN" | b"HINCRBY"
        | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LPOP" | b"RPOP" | b"LLEN"
        | b"LINDEX" | b"LRANGE" | b"LSET" | b"LTRIM"
        | b"ZADD" | b"ZSCORE" | b"ZMSCORE" | b"ZCARD" | b"ZREM" | b"ZINCRBY"
        | b"ZRANK" | b"ZREVRANK" | b"ZRANGE" | b"ZREVRANGE" | b"ZRANGEBYSCORE"
        | b"ZREVRANGEBYSCORE" | b"ZCOUNT" => {
            if nargs > 1 {
                vec![1]
            } else {
                vec![]
            }
        }
        b"DEL" | b"UNLINK" | b"EXISTS" | b"MGET" => (1..nargs).collect(),
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
            | b"PERSIST"
            | b"APPEND"
            | b"GETDEL"
            // P3 hash mutations
            | b"HSET"
            | b"HMSET"
            | b"HSETNX"
            | b"HDEL"
            | b"HINCRBY"
            // P3 list mutations
            | b"LPUSH"
            | b"RPUSH"
            | b"LPUSHX"
            | b"RPUSHX"
            | b"LPOP"
            | b"RPOP"
            | b"LSET"
            | b"LTRIM"
            // P3 zset mutations
            | b"ZADD"
            | b"ZREM"
            | b"ZINCRBY"
    )
}

/// Aggregate (hash/list/zset) mutations — their whole blob is persisted from the
/// final store state after dispatch (P3 durable aggregates).
fn is_aggregate_write(cmd: &[u8]) -> bool {
    matches!(
        cmd,
        b"HSET" | b"HMSET" | b"HSETNX" | b"HDEL" | b"HINCRBY"
            | b"LPUSH" | b"RPUSH" | b"LPUSHX" | b"RPUSHX" | b"LPOP" | b"RPOP" | b"LSET" | b"LTRIM"
            | b"ZADD" | b"ZREM" | b"ZINCRBY"
    )
}

/// Hand a write to the commit batcher for logged tiers (§3.4). Ephemeral writes
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

/// Build a rustls server config from PEM cert + key files (§4.5 TLS). Accepts a
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

// ---- socket / epoll helpers ---------------------------------------------

fn listen(addr: &str, port: u16) -> io::Result<RawFd> {
    unsafe {
        let fd = libc::socket(libc::AF_INET, libc::SOCK_STREAM | libc::SOCK_NONBLOCK, 0);
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
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
        sa.sin_family = libc::AF_INET as u16;
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

fn epoll_ctl(epfd: RawFd, op: libc::c_int, fd: RawFd, events: u32) -> io::Result<()> {
    let mut ev = libc::epoll_event {
        events,
        u64: fd as u64,
    };
    let r = unsafe { libc::epoll_ctl(epfd, op, fd, &mut ev) };
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

fn epoll_add(epfd: RawFd, fd: RawFd, events: u32) -> io::Result<()> {
    epoll_ctl(epfd, libc::EPOLL_CTL_ADD, fd, events)
}
fn epoll_mod(epfd: RawFd, fd: RawFd, events: u32) -> io::Result<()> {
    epoll_ctl(epfd, libc::EPOLL_CTL_MOD, fd, events)
}
fn epoll_del(epfd: RawFd, fd: RawFd) -> io::Result<()> {
    let mut ev = libc::epoll_event { events: 0, u64: 0 };
    let r = unsafe { libc::epoll_ctl(epfd, libc::EPOLL_CTL_DEL, fd, &mut ev) };
    if r < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
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
