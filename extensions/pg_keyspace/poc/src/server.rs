//! One slot worker: an epoll event loop over non-blocking sockets, parsing RESP
//! and dispatching against its own shared-memory partition (§3.1). No
//! transaction is ever opened on this path; a command is a shmem read/write
//! plus, for logged tiers, a handoff to the commit batcher (§3.4). This is the
//! path the P0 kill criterion measures.

use crate::batcher::{Batcher, Tier};
use crate::crc16;
use crate::resp::{self, Parse};
use crate::ring;
use crate::store::{now_micros, Lookup, Store};
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::{Read, Write};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::{Duration, Instant};

const EPOLL_MAX: usize = 1024;
const READ_CHUNK: usize = 64 * 1024;

/// A staged write: (key, value, expires_at_micros).
pub type PendingWrite = (Vec<u8>, Vec<u8>, i64);

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
    if let Some(seq) = p.push(key, val, exp) {
        return Some((shard, seq));
    }
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        std::thread::sleep(Duration::from_micros(50));
        if let Some(seq) = p.push(key, val, exp) {
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

/// Per-iteration control returned by the `run_with` tick closure.
pub enum Tick {
    /// Keep running.
    Continue,
    /// Stop the event loop (e.g. SIGTERM).
    Stop,
    /// Replace the auth config live: `Some(cfg)` enforces it, `None` switches to
    /// no-auth. Used for SIGHUP hot-reload of credentials.
    Reload(Option<AuthConfig>),
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
        })
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
                Tick::Reload(cfg) => self.auth = cfg,
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
                    stage = Some((args[1].clone(), args[2].clone(), exp));
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
                        stage = Some((args[1].clone(), args[2].clone(), 0));
                    }
                    1
                };
                let out = &mut self.conns.get_mut(&fd).unwrap().wbuf;
                resp::integer(out, n);
            }
            b"GETSET" => {
                let old = match store.get(&args[1]) {
                    Lookup::Hit(v) => Some(v.to_vec()),
                    Lookup::Miss => None,
                };
                store.set(&args[1], &args[2], 0);
                durable_log(&batcher, tier, &args[1], &args[2]);
                if persist_on {
                    stage = Some((args[1].clone(), args[2].clone(), 0));
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
                            if let Some(sa) = shard_push(&self.producers, a, b"", DELETE_TOMBSTONE)
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
                            stage = Some((args[1].clone(), s, 0));
                        }
                    }
                    None => resp::error(out, "ERR value is not an integer or out of range"),
                }
            }
            _ => resp::error(out, "ERR unknown command"),
        }

        if let Some((k, v, e)) = stage {
            // sharded so a given key always lands on the same ring/persist worker
            // — no cross-worker key conflicts on ON CONFLICT.
            if let Some(sa) = shard_push(&self.producers, &k, &v, e) {
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
        let _ = epoll_del(self.epfd, fd);
        unsafe { libc::close(fd) };
        self.conns.remove(&fd);
    }
}

#[inline]
fn itoa(n: i64) -> Vec<u8> {
    n.to_string().into_bytes()
}

/// Which argument positions of a command are keys (for ACL + tenant scoping).
fn key_indices(cmd: &[u8], nargs: usize) -> Vec<usize> {
    match cmd {
        b"GET" | b"SET" | b"SETNX" | b"GETSET" | b"INCR" | b"DECR" | b"INCRBY" | b"DECRBY"
        | b"TTL" | b"EXPIRE" | b"PERSIST" | b"TYPE" | b"STRLEN" | b"APPEND" | b"GETDEL" => {
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
}
