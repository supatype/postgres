//! pg_keyspace — a Postgres-native RESP keyspace, packaged as a real extension.
//!
//! Loaded via `shared_preload_libraries`, this extension:
//!   * requests a Postgres shared-memory segment in `shmem_request_hook`
//!     and initialises the keyspace store over it in `shmem_startup_hook`;
//!   * registers a background worker (a real Postgres backend) that runs the
//!     epoll RESP event loop against that segment — the read hot path,
//!     served on a TCP port for `ioredis`/`redis-cli`;
//!   * exposes the `supacache.*` SQL surface, which reads the *same*
//!     segment directly in the calling backend — the in-process ~1-2µs path.
//!
//! The performance-critical modules are shared verbatim with the standalone
//! `core` crate (via `#[path]`), so the code measured here is the same code the
//! standalone benchmarks measure.

use core::ffi::c_void;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::{AnyElement, FromDatum, IntoDatum, PgBuiltInOids, PgOid};
use std::ffi::CStr;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicPtr, AtomicU32, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

pgrx::pg_module_magic!();

// ---- shared core (identical to the standalone core crate) ----------------
#[path = "../../core/src/crc16.rs"]
mod crc16;
#[path = "../../core/src/shmem.rs"]
mod shmem;
#[path = "../../core/src/store.rs"]
mod store;
#[path = "../../core/src/resp.rs"]
mod resp;
#[path = "../../core/src/batcher.rs"]
mod batcher;
#[path = "../../core/src/ring.rs"]
mod ring;
#[path = "../../core/src/rowcache_key.rs"]
mod rowcache_key;
#[path = "../../core/src/prob.rs"]
mod prob;
#[path = "../../core/src/pubsub.rs"]
mod pubsub;
#[path = "../../core/src/pubsub_shm.rs"]
mod pubsub_shm;
#[path = "../../core/src/repl.rs"]
mod repl;
#[path = "../../core/src/aggr.rs"]
mod aggr;
#[path = "../../core/src/bitmap.rs"]
mod bitmap;
#[path = "../../core/src/share.rs"]
mod share;
#[path = "../../core/src/server.rs"]
mod server;

use batcher::Tier;
use server::{AclRule, AuthConfig, Cred};
use std::collections::{HashMap, HashSet};
use store::{Config, Lookup, Store};

const SEG_NAME: &CStr = c"pg_keyspace_segment";
const RING_NAME: &CStr = c"pg_keyspace_ring";
// Mode B row cache lives in its OWN segment — never RESP-addressable (Mode A
// and Mode B "must not share a code path").
const ROWCACHE_NAME: &CStr = c"pg_keyspace_rowcache";
// Cross-process pub/sub routing table and inboxes.
const PUBSUB_NAME: &CStr = c"pg_keyspace_pubsub";
// Per-worker liveness, so a worker that goes away can be noticed and relaunched.
const HEALTH_NAME: &CStr = c"pg_keyspace_health";
const CLOCK_NAME: &CStr = c"pg_keyspace_clock";
// Serialises WRITES to the row-cache segment. See `rowcache_write`.
const RC_LOCK_NAME: &CStr = c"pg_keyspace_rowcache_write";
// Which databases the row cache serves, and how each one is doing (#120).
const RC_DB_NAME: &CStr = c"pg_keyspace_rowcache_dbs";
// Serialises allocation of a directory entry. Never held across SPI.
const RC_DB_LOCK_NAME: &CStr = c"pg_keyspace_rowcache_dbs_lock";
/// Serialises publishers that are not slot workers. Held only by backends
/// running `supacache.publish()`, never by a worker, so it contends with
/// nothing on the RESP hot path.
const PS_EXT_LOCK_NAME: &CStr = c"pg_keyspace_pubsub_external_lock";

// Base address of the Postgres shared-memory segment, published by the startup
// hook and inherited by every forked backend. Each context rebuilds a cheap
// `Store` view over it on demand.
static SEG_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the SPSC persistence ring (RESP worker -> persistence worker).
static RING_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the Mode B row-cache segment (read by the planner-hook custom scan).
static ROWCACHE_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// The LWLock serialising row-cache WRITES (#127). Resolved once per process in
// the shmem startup hook and inherited by every forked backend.
/// Base of the row-cache writer-lock tranche: one `LWLockPadded` per row-cache
/// partition, indexed by `Store::partition_of` (#127, partitioned for #120).
static ROWCACHE_LOCKS: AtomicPtr<pg_sys::LWLockPadded> = AtomicPtr::new(std::ptr::null_mut());
// Base of the cross-process pub/sub segment.
static PUBSUB_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the worker liveness table.
static HEALTH_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the row-cache database directory, and the lock guarding allocation
// of an entry in it (#120).
static RC_DB_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
static RC_DB_LOCK: AtomicPtr<pg_sys::LWLock> = AtomicPtr::new(std::ptr::null_mut());
// Guards the external pub/sub lane (see PS_EXT_LOCK_NAME).
static PS_EXT_LOCK: AtomicPtr<pg_sys::LWLock> = AtomicPtr::new(std::ptr::null_mut());
// The bus itself, built by the postmaster in the shmem startup hook so that the
// wake descriptors it opens are inherited by every worker that forks from it.
// Built after the fork, each worker would hold private descriptors and wake
// nobody, which is indistinguishable from having no subscribers.
static BUS: OnceLock<Arc<pubsub::Bus>> = OnceLock::new();

// GUCs (fixed at postmaster start; the segment is sized from them).
static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(6380);
static GUC_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(1);
static GUC_KEYS: GucSetting<i32> = GucSetting::<i32>::new(1_000_000);
static GUC_VAL_BYTES: GucSetting<i32> = GucSetting::<i32>::new(512);
// Largest bulk string accepted from a RESP client. Matches Valkey/Redis
// proto-max-bulk-len so a client that works against them works here. The
// real bound on what can be stored is the keyspace arena; this bounds what
// the server will buffer for a value that may be refused anyway.
static GUC_MAX_VALUE_BYTES: GucSetting<i32> =
    GucSetting::<i32>::new(server::DEFAULT_MAX_VALUE_BYTES);
static GUC_DURABILITY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"ephemeral"));
static GUC_COMMIT_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
// persistence: which database holds supacache.kv, and how often the worker
// flushes staged writes to it in one batched transaction.
static GUC_DATABASE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"postgres"));
static GUC_PERSIST_WINDOW_MS: GucSetting<i32> = GucSetting::<i32>::new(10);
static GUC_RING_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static GUC_PUBSUB_ROUTES: GucSetting<i32> = GucSetting::<i32>::new(4096);
static GUC_PUBSUB_RING_KB: GucSetting<i32> = GucSetting::<i32>::new(256);
/// Per-subscriber cap on undelivered pub/sub, in MiB (0 = no limit).
static GUC_PUBSUB_BUFFER_MB: GucSetting<i32> = GucSetting::<i32>::new(32);
static GUC_WATCHDOG_SECS: GucSetting<i32> = GucSetting::<i32>::new(30);
static GUC_PERSIST_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(1);
// TTL by partition drop: time-bucket width and sweep interval.
static GUC_TTL_BUCKET_SECS: GucSetting<i32> = GucSetting::<i32>::new(10);
static GUC_TTL_SWEEP_SECS: GucSetting<i32> = GucSetting::<i32>::new(5);
// Mode B row cache segment size.
static GUC_ROWCACHE_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
/// How many partitions the Mode B row-cache segment is carved into, which is
/// also how many writer locks it has (#120).
static GUC_ROWCACHE_PARTITIONS: GucSetting<i32> = GucSetting::<i32>::new(8);
/// Size of the bounded pool of invalidation workers (#120). Worker count is
/// something an operator sets, not a function of how many databases exist.
static GUC_ROWCACHE_INVALIDATION_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(1);
/// How long one pool worker holds a database before handing its slot on, when
/// there are more participating databases than workers.
static GUC_ROWCACHE_LEASE_MS: GucSetting<i32> = GucSetting::<i32>::new(5_000);
/// Entries in the shared row-cache database directory.
static GUC_ROWCACHE_MAX_DATABASES: GucSetting<i32> = GucSetting::<i32>::new(32);

/// Whether the RESP worker requires `supatype_mask` to be loaded (and outermost)
/// before it will serve. Default OFF: pg_keyspace runs standalone as a
/// plain Postgres-native keyspace + RLS-aware row cache, with no dependency on
/// supatype_mask. Set ON in the Supatype platform, where RESP must never expose
/// rows the mask would have rewritten — the worker then fails closed unless mask
/// is loaded and outermost. When mask IS present, its load order is checked
/// either way.
static GUC_REQUIRE_MASK: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Keep the extension's SQL catalogue in step with the library that is running
/// (#136). On by default: the failure it prevents is silent, and the operator
/// who most needs the fix is the one who does not know to run the command.
static GUC_AUTO_UPGRADE: GucSetting<bool> = GucSetting::<bool>::new(true);

/// TLS for the RESP wire: when both a cert and key file are set, every
/// RESP connection is wrapped in TLS, so the AUTH password and values are
/// encrypted in transit. Empty (default) = plaintext (put TLS termination in
/// front, or set these).
static GUC_TLS_CERT: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
static GUC_TLS_KEY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

/// Reuse the cluster's own TLS material for the RESP port instead of a
/// second, separately-managed cert. Off by default, and deliberately so:
/// turning it on implicitly would encrypt a port that every existing
/// plaintext client is already talking to, breaking them all at the next
/// restart. Opting in is the operator saying "the cert I already rotate for
/// libpq is the cert I want here".
static GUC_TLS_USE_PG_CERT: GucSetting<bool> = GucSetting::<bool>::new(false);

/// Mode B: enable the keys-only logical-decoding invalidation worker,
/// which consumes a replication slot (output plugin `supacache_keys`) and drops
/// changed rows from the row cache so it stays coherent with committed writes.
/// Off by default — it needs `wal_level = logical` and holds a replication slot.
/// Host advertised in `MOVED` redirects and the `CLUSTER` topology (the workers
/// bind 0.0.0.0, so they cannot infer an address a remote client can reach).
/// Deliberately unset by default: a persisted multi-worker cluster refuses to
/// start without it rather than publishing an address that may only resolve on
/// the server host.
static GUC_ANNOUNCE_HOST: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
static GUC_ROWCACHE_DECODE: GucSetting<bool> = GucSetting::<bool>::new(false);
static GUC_ROWCACHE_SLOT: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"supacache_rowcache"));
static GUC_ROWCACHE_DECODE_MS: GucSetting<i32> = GucSetting::<i32>::new(200);
/// When on, the invalidation worker REFILLS a changed hot key with the current
/// row (re-read via SPI, raw bytes) instead of only dropping it, so a hot key
/// stays served from cache across writes. Off = drop-only (refill is lazy on the
/// next read). Deleted rows are always dropped, never refilled.
static GUC_ROWCACHE_REFILL: GucSetting<bool> = GucSetting::<bool>::new(false);
static GUC_ROWCACHE_READTHROUGH: GucSetting<bool> = GucSetting::<bool>::new(false);
/// Per-tenant share of each persistence ring (#43). On by default.
static GUC_TENANT_RING_SHARE: GucSetting<bool> = GucSetting::<bool>::new(true);
/// Tenant-scoped row-cache eviction (#43). On by default.
static GUC_TENANT_SCOPED_EVICTION: GucSetting<bool> = GucSetting::<bool>::new(true);

/// Cap any one tenant at this percentage of a segment partition's entries
/// (0 = off, the default).
///
/// Scoped eviction stops a flood from evicting you; it does not guarantee you
/// a share. A tenant that grows steadily rather than flooding is never the one
/// inserting under pressure, so the preference never points at it and it keeps
/// everything it has. A budget is what takes space back (#102).
static GUC_TENANT_ARENA_PCT: GucSetting<i32> = GucSetting::<i32>::new(0);
/// Per-tenant command rate (#43). 0 = no limit, which is the default.
static GUC_TENANT_OPS_PER_SEC: GucSetting<i32> = GucSetting::<i32>::new(0);

/// The tenant a *SQL* backend acts as, which is what lets `supacache.publish()`
/// scope a channel to `{tenant}:` the way a RESP connection's credential does.
///
/// Declared `Suset` for a reason. A `Userset` GUC would be worthless here --
/// a tenant would simply `SET pg_keyspace.tenant` to somebody else's value and
/// publish into their channels. `Suset` means only a superuser may `SET` it,
/// while `ALTER ROLE tenant_a SET pg_keyspace.tenant = 'acme'` still applies at
/// that role's login and cannot be overridden or `RESET` by the role itself.
///
/// That covers both deployments with one mechanism: a cluster-wide value in
/// `postgresql.conf` for a single-tenant cluster, or a per-role value for a
/// multi-tenant one, held in Postgres's own `pg_db_role_setting` rather than in
/// a table this extension would have to define, dump and upgrade.
///
/// `Suset` alone is not quite the whole boundary -- `GRANT SET ON PARAMETER`
/// (PG15+) can hand the right to `SET` it to any role -- so `publish()` also
/// checks where the value came from. See `tenant_scope()`.
static GUC_TENANT: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

/// Channels whose messages are relayed to the peers in `supacache.peer`, as a
/// comma-separated glob list. Empty (the default) relays nothing.
///
/// Opt-in per pattern rather than all-or-nothing, and that is the design rather
/// than caution. Relaying every channel reproduces the problem redis 7 added
/// sharded pub/sub to escape: a broadcast whose cost grows with the number of
/// instances, paid on the highest-volume, lowest-value traffic in most systems
/// (presence, typing indicators, cursor positions). The channels that genuinely
/// need to cross an instance boundary -- cache invalidation, session
/// revocation -- are few and low-rate, and naming them keeps the fan-out bill
/// proportional to the value.
static GUC_RELAY_CHANNELS: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
/// RESP credential the relay subscribes with, when the cluster requires AUTH.
/// What it may relay is bounded by this credential's scope, which is the point:
/// a tenant-scoped role relays only that tenant's channels.
static GUC_RELAY_USER: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
static GUC_RELAY_SECRET: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);

/// KV store config for the Mode B row cache: keys are (relid,pk) 12-byte tuples,
/// values are raw heap-tuple bytes.
/// One megabyte, the smallest a row-cache partition is allowed to be. Each
/// partition is a separate arena with its own CLOCK eviction, so one too small
/// to hold a working set evicts continuously and caches nothing.
const RC_MIN_PARTITION_BYTES: u64 = 1024 * 1024;

fn rowcache_config() -> Config {
    // `rowcache_mb` is the size of the WHOLE segment and always has been, so
    // the partitions divide it. `Config::partitioned` is what enforces that,
    // and the count it returns can be lower than asked for on a small segment.
    Config::partitioned(
        GUC_ROWCACHE_MB.get().max(1) as u64 * 1024 * 1024,
        200_000,
        GUC_ROWCACHE_PARTITIONS.get().clamp(1, 64) as u32,
        RC_MIN_PARTITION_BYTES,
    )
}

/// How many partitions the row-cache segment has, which is also how many
/// writer locks its tranche needs (#120).
fn rowcache_partitions() -> u32 {
    rowcache_config().num_partitions
}

/// Whether read-through warming is both enabled and safe to act on.
///
/// Read-through requires automatic invalidation. Without it the cache would
/// populate itself from every read and nothing would ever invalidate a row, so
/// the keyspace would fill with entries that go stale and stay stale. Manual
/// mode is safe today precisely because caching is a deliberate act: the
/// operator chooses what to cache and knows it is theirs to keep current.
///
/// Enabling one without the other is a configuration mistake rather than a
/// working setup, so it is refused here and reported at startup.
fn rowcache_readthrough_active() -> bool {
    GUC_ROWCACHE_READTHROUGH.get() && GUC_ROWCACHE_DECODE.get()
}

/// Whether the row cache's coherence can be trusted right now.
///
/// The cache holds raw pre-policy rows and is only as correct as the worker that
/// invalidates them. If that worker is not running, entries go stale with no
/// bound and nothing says so: #39 called this out as the case where the cache
/// "serves stale rows indefinitely with no alarm".
///
/// Fail closed, but only against the configuration that asked for automatic
/// coherence. With `pg_keyspace.rowcache_decode = off` there is no invalidation
/// worker by design -- the cache is warmed and managed by hand -- and refusing
/// to serve it would break a deliberate choice rather than protect anyone.
///
/// A database that has never been drained reads as not coherent, so the window
/// between a cluster starting and the first drain is closed rather than open.
///
/// PER-DATABASE (#120), and that is a correctness property rather than a
/// reporting one. The cache fails closed on incoherence, so "which database is
/// incoherent" decides which queries are served from the cache at all. Judged
/// against the *directory's* `last_drained_us` for this database, not against a
/// pool worker's heartbeat: under cycling a worker is alive and draining some
/// OTHER database for most of its life, so its heartbeat says nothing about
/// whether this database is current. One database's stalled worker must not read
/// as the whole cache being incoherent, and a healthy pool must not read as
/// every database being current.
fn rowcache_coherent() -> bool {
    if !GUC_ROWCACHE_DECODE.get() {
        return true; // manual coherence, operator's choice
    }
    if RC_DB_BASE.load(Ordering::Acquire).is_null() {
        return true; // no directory at all: nothing to judge against
    }
    match rcdb_find(rc_db()) {
        // This database is not in the directory, so nothing is decoding for it.
        // Refusing is the point: a database nobody is invalidating is exactly
        // the case that used to be cached and served stale forever.
        None => false,
        Some(s) => {
            if s.slot_lost.load(Ordering::Acquire) != 0 {
                // The server cut this database's slot loose, so an unknown set
                // of invalidations was never delivered. Whatever is cached may
                // disagree with the heap, and no heartbeat can say otherwise.
                return false;
            }
            let last = s.last_drained_us.load(Ordering::Acquire);
            last != 0 && store::now_micros().saturating_sub(last) < rcdb_stale_us()
        }
    }
}

/// A row-cache Store view over the Mode B segment (any backend).
/// Run `f` as the only writer to `key`'s partition of the row-cache segment
/// (#127, partitioned for #120).
///
/// `Store`'s public API is documented as "called only by the owning worker for
/// partition p", and every RESP keyspace segment honours that: one worker
/// process owns it. The Mode B row-cache segment has no owner. Its writers are
/// ordinary backends -- `rowcache_put`, registration, and above all the
/// read-through path in `rc_access`, which runs in EVERY backend that misses.
///
/// Two of those at once corrupt the arena: `ensure_alloc` walks the slab free
/// lists and can call `evict_one`, which moves the bucket array, the entry
/// array and the bump pointer. The first backend to follow a torn pointer
/// segfaults, and Postgres answers a segfault by killing every other backend
/// and crash-restarting the cluster. Measured: four pgbench clients, 80% read
/// / 20% update against a registered table, dead in under a second.
///
/// So writers take an exclusive LWLock and readers take nothing. That is not a
/// half-measure -- concurrent reader against a single writer is the pattern the
/// store is already built for, and `get_stable`'s seqlock is what makes it
/// safe. Restoring "one writer at a time" is the whole of the fix, and it keeps
/// the read path, which is the hot one, at zero added cost.
///
/// The lock lives here rather than in `core/` deliberately: `core/` is shared
/// with the standalone daemon, which has no Postgres LWLocks and does not have
/// this problem, because there every segment has exactly one writing process.
///
/// ONE lock for the whole segment was right while the segment served one
/// database. #120 makes every database read-through into it, and a single lock
/// would then be a cluster-wide serialisation point for row-cache writes: one
/// database with a cold cache and heavy read-through would stall caching for
/// every other one. So the lock follows the segment's own partitioning —
/// `Store::partition_of` names the partition, and each partition has its own
/// lock. Two writers in different partitions never wait on each other, and a
/// writer still cannot race another writer in the arena it is mutating, which
/// is the whole of what #127 needed.
#[inline]
fn rowcache_write<T>(view: &Store, key: &[u8], f: impl FnOnce() -> T, default: T) -> T {
    let base = ROWCACHE_LOCKS.load(Ordering::Acquire);
    if base.is_null() {
        // No lock means the shmem startup hook did not run, which means there
        // is no segment to write either. Refusing is right: writing unguarded
        // is what this function exists to prevent.
        return default;
    }
    // The partition comes from the same view the closure writes through, so the
    // lock and the arena cannot come from different geometries.
    let p = view.partition_of(key) as usize;
    let lock = unsafe { std::ptr::addr_of_mut!((*base.add(p)).lock) };
    unsafe {
        pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE);
    }
    // The callers are plain shared-memory manipulations with no allocation and
    // no SPI, so there is nothing here that can ereport past the release. The
    // lock is also released by Postgres on transaction abort regardless, so a
    // panic cannot leave it held for the life of the cluster.
    let out = f();
    unsafe {
        pg_sys::LWLockRelease(lock);
    }
    out
}

fn rowcache_view() -> Option<Store> {
    let base = ROWCACHE_BASE.load(Ordering::Acquire);
    if base.is_null() {
        return None;
    }
    Some(unsafe { Store::from_raw(base, &rowcache_config(), false) })
}

// Row-cache key layout, and the decode-slot name derived from a database oid.
//
// A key is `tag ++ datoid ++ relid ++ canonical pk bytes`. The pk part is the
// type's output-function text (the "canonical" form): identical bytes are
// produced by the planner hook (from the query Const), by `rowcache_put`/refill
// (from the heap tuple), and by the `supacache_keys` decode plugin (from the WAL
// change), so all three agree for the same logical row regardless of the pk's
// type — int, uuid, text, etc. (Integers are still their decimal text, e.g. `1`.)
//
// The layout itself lives in `core/rowcache_key.rs`, where it can be unit-tested
// without standing up a cluster. #117 was a key-layout bug, and a layout that
// can only be checked by running Postgres is one that reaches production.
use rowcache_key::{
    loaded_key as rc_loaded_key_for, reg_key as rc_reg_key_for, row_key as rc_key_for,
    slot_name_for,
};

/// The database this backend is connected to, as row-cache key bytes.
///
/// The row-cache segment is cluster-wide shared memory and
/// `shared_preload_libraries` installs the planner hook in *every* database, so
/// a key without the database in it is ambiguous across databases. Relids are
/// per-database and `CREATE DATABASE ... TEMPLATE` copies `pg_class` physically,
/// so two cloned databases have *identical* relids -- which made the collision
/// certain rather than unlikely in the per-project-database pattern, and served
/// one database's rows to another (#117).
#[inline]
fn rc_db() -> u32 {
    unsafe { pg_sys::MyDatabaseId.as_u32() }
}

fn rc_key(relid: u32, pk: &[u8]) -> Vec<u8> {
    rc_key_for(rc_db(), relid, pk)
}

/// Canonical pk bytes for a datum of `typoid`: the type's output-function text.
/// Used everywhere a pk value must become a cache key (planner Const, heap
/// tuple, refill), so the encoding is one function and cannot drift between
/// sites. Returns None for a NULL datum.
unsafe fn canon_pk(typoid: pg_sys::Oid, datum: pg_sys::Datum) -> Vec<u8> {
    let mut foutoid = pg_sys::InvalidOid;
    let mut isvarlena = false;
    pg_sys::getTypeOutputInfo(typoid, &mut foutoid, &mut isvarlena);
    let cstr = pg_sys::OidOutputFunctionCall(foutoid, datum);
    let bytes = CStr::from_ptr(cstr).to_bytes().to_vec();
    pg_sys::pfree(cstr as *mut c_void);
    bytes
}

/// Join canonical pk parts into the one key a composite-keyed row is cached under.
///
/// NUL is the separator, and it is safe as one rather than merely unlikely: each
/// part is the output of a type's output function, which Postgres returns as a
/// C string, so no part can contain a NUL byte. A single-column key is therefore
/// byte-identical to the part itself, which is what it has always been.
///
/// Every side that builds a cache key goes through here -- the planner, the
/// executor's miss fallback, the invalidation refill and the SQL surface -- so
/// there is one definition of "the key for this row" rather than four that have
/// to be kept in step.
fn compose_pk(parts: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::with_capacity(parts.iter().map(|p| p.len() + 1).sum());
    for (i, p) in parts.iter().enumerate() {
        if i > 0 {
            out.push(0);
        }
        out.extend_from_slice(p);
    }
    out
}

/// Split a composed key back into its parts. Inverse of [`compose_pk`].
fn split_pk(key: &[u8]) -> Vec<Vec<u8>> {
    key.split(|b| *b == 0).map(|p| p.to_vec()).collect()
}

/// Lowercase-hex encode (the decode plugin emits the pk this way, so an
/// arbitrary-byte canonical pk survives the whitespace-delimited line format).
fn hex_encode(b: &[u8]) -> String {
    let mut s = String::with_capacity(b.len() * 2);
    for &x in b {
        s.push(char::from_digit((x >> 4) as u32, 16).unwrap());
        s.push(char::from_digit((x & 0xf) as u32, 16).unwrap());
    }
    s
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if b.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i < b.len() {
        let hi = (b[i] as char).to_digit(16)?;
        let lo = (b[i + 1] as char).to_digit(16)?;
        out.push(((hi << 4) | lo) as u8);
        i += 2;
    }
    Some(out)
}

fn ttl_bucket_us() -> i64 {
    GUC_TTL_BUCKET_SECS.get().max(1) as i64 * 1_000_000
}

/// Persistence shards **per slot worker**: within one slot worker, writes are
/// sharded across this many rings by key slot, so a key's writes and deletes
/// always reach the same persistence worker in order.
fn persist_shards() -> usize {
    GUC_PERSIST_WORKERS.get().max(1) as usize
}
/// Total rings = one per (slot worker, persistence shard).
///
/// The ring is strictly single-producer/single-consumer, and slot workers are
/// separate *processes*, so they cannot share one ring: each slot worker owns
/// its own set of `persist_shards()` rings. Persistence worker `s` then consumes
/// ring `(w, s)` for every `w`, which keeps one consumer per ring while holding
/// the persistence-worker process count independent of `pg_keyspace.workers`.
fn ring_count() -> usize {
    worker_count() * persist_shards()
}
/// Flat index of slot worker `w`'s shard-`s` ring within the contiguous block.
fn ring_index(w: usize, shard: usize) -> usize {
    w * persist_shards() + shard
}
/// Bytes for one ring (header + power-of-two capacity).
fn ring_stride() -> usize {
    ring::bytes_for((GUC_RING_MB.get().max(1) as usize) * 1024 * 1024)
}

/// One worker's liveness record.
///
/// `pg_terminate_backend` on a background worker calls `TerminateBackgroundWorker`,
/// which makes the postmaster *deregister* it rather than restart it, whatever
/// `bgw_restart_time` says. A persistence worker lost that way stays lost for the
/// life of the cluster, and since a durable write then holds its acknowledgement
/// rather than failing, the only outward sign is that writes stop completing.
///
/// So every worker beats here on the tick it already has, and every worker also
/// scans for gaps. As long as one of them survives, the rest come back. There is
/// deliberately no supervisor process, because a supervisor is one more thing
/// that can be terminated.
#[repr(C)]
struct WorkerSlot {
    /// Last heartbeat, microseconds. 0 means the slot has never been claimed.
    last_seen_us: AtomicI64,
    /// When a relaunch was last attempted, so several workers noticing the same
    /// gap produce one relaunch between them rather than one each.
    last_launch_us: AtomicI64,
    owner_pid: AtomicU32,
    _pad: u32,
}

const HEALTH_STRIDE: usize = 64;

/// Slots are laid out RESP workers, then persistence shards, then the expiry
/// worker, then the row-cache invalidation POOL, so an index maps back to
/// exactly what to relaunch.
fn health_slot_count() -> usize {
    worker_count() + persist_shards() + 1 + rowcache_pool_size()
}

fn health_expiry_slot() -> usize {
    worker_count() + persist_shards()
}

/// The health slot for pool worker `k`.
///
/// A range rather than a single slot (#120): the pool is bounded by
/// `pg_keyspace.rowcache_invalidation_workers`, and each member claims its own
/// slot so two workers can never both drain one database's slot.
///
/// Note what this is NOT: it is not where a database's coherence is judged. A
/// pool worker's heartbeat says only that the worker is alive, and under
/// cycling it is alive on some OTHER database half the time. Coherence is
/// per-database and lives in the directory's `last_drained_us`.
fn health_invalidation_slot(k: usize) -> usize {
    worker_count() + persist_shards() + 1 + k
}

fn rowcache_pool_size() -> usize {
    GUC_ROWCACHE_INVALIDATION_WORKERS.get().clamp(1, 64) as usize
}

fn health_bytes() -> usize {
    health_slot_count() * HEALTH_STRIDE
}

fn health_slot(i: usize) -> Option<&'static WorkerSlot> {
    let base = HEALTH_BASE.load(Ordering::Acquire);
    if base.is_null() || i >= health_slot_count() {
        return None;
    }
    unsafe { Some(&*(base.add(i * HEALTH_STRIDE) as *const WorkerSlot)) }
}

/// How old a heartbeat may get before the worker is presumed gone.
fn health_stale_us() -> i64 {
    (GUC_WATCHDOG_SECS.get().max(1) as i64) * 1_000_000
}

fn health_beat(i: usize) {
    if let Some(sl) = health_slot(i) {
        sl.last_seen_us.store(store::now_micros(), Ordering::Release);
    }
}

/// Take ownership of a slot, or refuse because somebody live already holds it.
///
/// This is what makes an over-eager relaunch harmless rather than destructive.
/// The persistence ring is single-producer/single-consumer, so two workers
/// draining one shard would corrupt it; a worker that cannot claim its slot
/// exits instead of draining.
fn health_claim(i: usize) -> bool {
    let sl = match health_slot(i) {
        Some(s) => s,
        None => return true, // no table: nothing to coordinate through
    };
    let now = store::now_micros();
    let last = sl.last_seen_us.load(Ordering::Acquire);
    let prev = sl.owner_pid.load(Ordering::Acquire);
    let me = unsafe { pg_sys::MyProcPid } as u32;
    // A fresh heartbeat is not enough on its own to refuse. Postgres restarts a
    // worker that exits non-zero after two seconds, well inside the staleness
    // window, so the replacement would be locked out by the heartbeat its own
    // predecessor left behind. What matters is whether that predecessor is still
    // running.
    if last != 0
        && now.saturating_sub(last) < health_stale_us()
        && prev != 0
        && prev != me
        && pid_alive(prev)
    {
        return false;
    }
    if sl
        .owner_pid
        .compare_exchange(prev, me, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return false;
    }
    sl.last_seen_us.store(now, Ordering::Release);
    true
}

/// Acknowledge any pending procsignal barrier.
///
/// Postgres emits a barrier for operations that need every backend to let go of
/// something before they can proceed, DROP TABLESPACE being the common one, and
/// then waits for all of them to acknowledge it. A backend acknowledges from
/// inside CHECK_FOR_INTERRUPTS, which a normal backend reaches constantly while
/// executing a query.
///
/// These workers never execute a query on their own behalf. The RESP worker
/// sits in a mio poll, the persistence worker in a drain loop and the expiry
/// worker in a sleep, so none of them passed through the interrupt machinery at
/// all and none of them ever acknowledged. The result was that DROP TABLESPACE
/// hung for as long as pg_keyspace was loaded, with no error and nothing in the
/// log to say which backend had not answered.
///
/// Only the barrier is processed here, not the full interrupt path: these loops
/// handle their own SIGTERM and unwinding them from an arbitrary point would
/// abandon a connection's parked write.
fn absorb_procsignal_barrier() {
    unsafe {
        if pg_sys::ProcSignalBarrierPending != 0 {
            pg_sys::ProcessProcSignalBarrier();
        }
    }
}

/// Whether a recorded owner is still running. Signal 0 checks for the process
/// without sending anything.
fn pid_alive(pid: u32) -> bool {
    pid != 0 && unsafe { libc::kill(pid as i32, 0) } == 0
}

fn health_release(i: usize) {
    if let Some(sl) = health_slot(i) {
        sl.owner_pid.store(0, Ordering::Release);
        sl.last_seen_us.store(0, Ordering::Release);
    }
}

// ==== the row-cache database directory (#120) ========================
//
// The row cache serves every database, and the thing that decides which ones is
// not the catalogue and not the keys: it is INVALIDATION. A logical replication
// slot belongs to the database it was created in and only ever decodes changes
// from that database, so a database is served only if some worker is holding a
// slot in it and draining. A table registered anywhere else would be cached and
// then never invalidated -- stale indefinitely, while coherence still reported
// healthy, because coherence described the worker rather than your table.
//
// This directory is the shared answer to "which databases, and how is each one
// doing". It has to be in shared memory rather than in a catalogue, because
// every reader of it is in a DIFFERENT database: a backend deciding whether to
// serve its own database's cache cannot query a table in another one, and a
// worker choosing which database to bind to next has not connected to any
// database yet. Postgres has no cross-database read, which is the whole reason
// this is a fixed array of oids rather than a table.
//
// It is a CACHE of the per-database `supacache.rowcache_reg` catalogues, not the
// truth. Shared memory does not survive a restart, so after one the directory is
// empty and is rebuilt by probing: a worker binds to a database, looks for
// registrations, and records what it found. Registration publishes directly as
// well, so the probe is only ever catching up after a restart rather than being
// the normal path.

/// What the directory knows about one database.
///
/// Every field is atomic and written without the directory lock, which is held
/// only to ALLOCATE an entry. Readers are backends on the hot path -- a planner
/// hook asking "is my database's cache trustworthy right now" -- and making them
/// take a lock would put a cluster-wide serialisation point in front of every
/// plan of every registered table.
#[repr(C)]
struct DbSlot {
    /// Database oid, or 0 for a free entry. Written last when claiming, so a
    /// reader never sees a half-initialised entry.
    datoid: AtomicU32,
    /// `DB_*` below.
    state: AtomicU32,
    /// Rows in that database's `supacache.rowcache_reg` as last observed.
    registrations: AtomicI64,
    /// When a worker last completed a drain pass for this database. This, not
    /// the worker's heartbeat, is what coherence is judged against: a pool
    /// worker that is alive and serving a DIFFERENT database says nothing about
    /// whether this one is current.
    last_drained_us: AtomicI64,
    /// When this database was last probed for registrations.
    last_probe_us: AtomicI64,
    /// When a worker last TOOK A TURN on this database, successful or not.
    ///
    /// Separate from `last_drained_us` because a turn that failed still has to
    /// count for scheduling. A database that cannot get a slot -- because
    /// `max_replication_slots` is exhausted -- drains never, so ordering by
    /// `last_drained_us` alone would put it first every single time: it would
    /// be leased, fail, exit, be relaunched immediately (Postgres restarts a
    /// cleanly-exited worker at once) and lease itself again, starving every
    /// healthy database behind it. Ordering by the attempt instead sends it to
    /// the back of the queue, where a database that cannot be served belongs.
    last_attempt_us: AtomicI64,
    /// The pool worker currently holding this database, and until when.
    owner_pid: AtomicU32,
    lease_until_us: AtomicI64,
    /// The server invalidated this database's slot (`wal_status = 'lost'`),
    /// usually because it reserved more WAL than `max_slot_wal_keep_size`.
    /// Everything committed since it stopped advancing is an invalidation that
    /// will never arrive, so the cache for this database is NOT trustworthy
    /// until it has been purged and the slot rebuilt.
    slot_lost: AtomicU32,
    /// Database name, for the stats views. Fixed width because this is shared
    /// memory; written once, before `datoid` is published.
    datname: [u8; DB_NAME_MAX],
}

/// Never probed, or probed and the answer is not known yet.
const DB_UNKNOWN: u32 = 0;
/// Has registrations: needs a slot and a turn in the cycle.
const DB_PARTICIPATING: u32 = 1;
/// Probed and has no registrations: no slot, no turn. This is the "launch
/// lazily" half -- most clusters have one or two participating databases and
/// should pay nothing for the rest.
const DB_IDLE: u32 = 2;

const DB_NAME_MAX: usize = 64;
const DB_STRIDE: usize = 128;

fn rcdb_count() -> usize {
    GUC_ROWCACHE_MAX_DATABASES.get().clamp(1, 1024) as usize
}

fn rcdb_bytes() -> usize {
    rcdb_count() * DB_STRIDE
}

// An entry must fit its stride, or entry N would overlap entry N+1 and two
// databases would share a heartbeat -- which reads as one of them being
// perpetually current.
const _: () = assert!(std::mem::size_of::<DbSlot>() <= DB_STRIDE);

fn rcdb_at(i: usize) -> Option<&'static DbSlot> {
    let base = RC_DB_BASE.load(Ordering::Acquire);
    if base.is_null() || i >= rcdb_count() {
        return None;
    }
    unsafe { Some(&*(base.add(i * DB_STRIDE) as *const DbSlot)) }
}

fn rcdb_find(datoid: u32) -> Option<&'static DbSlot> {
    (0..rcdb_count())
        .filter_map(rcdb_at)
        .find(|s| s.datoid.load(Ordering::Acquire) == datoid)
}

fn rcdb_name(s: &DbSlot) -> String {
    let raw = &s.datname;
    let end = raw.iter().position(|&b| b == 0).unwrap_or(raw.len());
    String::from_utf8_lossy(&raw[..end]).into_owned()
}

/// Record a database in the directory, creating its entry if it has none.
///
/// Returns None only when the directory is full, which is an operator-visible
/// misconfiguration rather than something to paper over: a database that cannot
/// be recorded cannot be served, and silently not serving it is the failure
/// this whole issue exists to remove.
fn rcdb_publish(datoid: u32, datname: &str, state: u32) -> Option<&'static DbSlot> {
    if let Some(s) = rcdb_find(datoid) {
        if state != DB_UNKNOWN {
            s.state.store(state, Ordering::Release);
        }
        return Some(s);
    }
    let lock = RC_DB_LOCK.load(Ordering::Acquire);
    if lock.is_null() {
        return None;
    }
    unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    // Re-check under the lock: two backends registering in the same database at
    // the same moment must end up with ONE entry, or the database would get two
    // slots and two workers draining them.
    let found = rcdb_find(datoid).or_else(|| {
        let free = (0..rcdb_count())
            .filter_map(rcdb_at)
            .find(|s| s.datoid.load(Ordering::Acquire) == 0)?;
        let bytes = datname.as_bytes();
        let n = bytes.len().min(DB_NAME_MAX - 1);
        unsafe {
            let dst = free.datname.as_ptr() as *mut u8;
            std::ptr::write_bytes(dst, 0, DB_NAME_MAX);
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), dst, n);
        }
        free.state.store(state, Ordering::Release);
        free.registrations.store(0, Ordering::Release);
        free.last_drained_us.store(0, Ordering::Release);
        free.last_probe_us.store(0, Ordering::Release);
        free.last_attempt_us.store(0, Ordering::Release);
        free.owner_pid.store(0, Ordering::Release);
        free.lease_until_us.store(0, Ordering::Release);
        free.slot_lost.store(0, Ordering::Release);
        // Published LAST: an entry is visible to a lock-free reader only once
        // everything it describes is already written.
        free.datoid.store(datoid, Ordering::Release);
        Some(free)
    });
    unsafe { pg_sys::LWLockRelease(lock) };
    if let Some(s) = found {
        if state != DB_UNKNOWN {
            s.state.store(state, Ordering::Release);
        }
    }
    found
}

/// How many databases currently need a slot and a turn.
fn rcdb_participating() -> usize {
    (0..rcdb_count())
        .filter_map(rcdb_at)
        .filter(|s| {
            s.datoid.load(Ordering::Acquire) != 0
                && s.state.load(Ordering::Acquire) == DB_PARTICIPATING
        })
        .count()
}

/// How long one database may go undrained before its cache stops being trusted.
///
/// Not simply `watchdog_secs`. With more participating databases than pool
/// workers, invalidation CYCLES: a worker leases a database, drains it, and
/// hands the slot on. A database waiting its turn is behind by the cycle time
/// and that is by design, so judging it against a fixed heartbeat window would
/// declare a perfectly healthy cluster incoherent as soon as it had a few
/// databases -- and the cache fails closed, so that is not a cosmetic error, it
/// is the cache switching itself off.
///
/// So the window is the worse of the two: the watchdog's own staleness bound,
/// and a full cycle with headroom. It is reported as a column, because an
/// operator cannot be expected to derive it and it is exactly the number that
/// says how this design scales.
fn rcdb_stale_us() -> i64 {
    let pool = rowcache_pool_size().max(1);
    let dbs = rcdb_participating().max(1);
    let turns = dbs.div_ceil(pool) as i64;
    let lease = (GUC_ROWCACHE_LEASE_MS.get().max(100) as i64) * 1000;
    // Twice a cycle, plus the relaunch gap between one lease ending and the
    // next worker binding, so an ordinary cycle never reads as a stall.
    let cycle = turns.saturating_mul(lease.saturating_add(RC_RELAUNCH_US)) * 2;
    health_stale_us().max(cycle)
}

/// How long Postgres waits before restarting a pool worker that exited, which
/// is the gap between one database's turn ending and the next one beginning.
const RC_RELAUNCH_US: i64 = 1_000_000;

/// Whether `max_slot_wal_keep_size` bounds how much WAL a slot may retain.
///
/// `-1` (the default) means unbounded, which is the operational hazard this
/// whole design creates: one slot per participating database, and a slot holds
/// WAL until it is consumed, so any single database's stalled invalidation pins
/// WAL for the WHOLE CLUSTER. Bounded, the server invalidates the offending slot
/// instead, which this extension detects and answers by marking that database
/// incoherent -- trading a little cache for the cluster staying up.
/// Whether `max_replication_slots` looks too small for the databases that want
/// one, and if so, what to say about it.
///
/// One slot per participating database is not a design choice that could have
/// gone another way: slots are inherently per-database. So the cluster-wide slot
/// budget becomes a limit on how many databases can be cached, and it is a
/// budget shared with whatever real replication the cluster is already doing.
fn replication_slots_short() -> Option<String> {
    let cap: i64 = pg_setting(c"max_replication_slots")?.parse().ok()?;
    let used = Spi::get_one::<i64>("SELECT count(*) FROM pg_replication_slots")
        .ok()
        .flatten()
        .unwrap_or(0);
    let want = rcdb_participating() as i64;
    if used < cap && want < cap {
        return None;
    }
    Some(format!(
        "max_replication_slots is {cap} and {used} slot(s) are already in use, with \
         {want} database(s) registered for the row cache"
    ))
}

fn wal_keep_size_bounded() -> bool {
    pg_setting(c"max_slot_wal_keep_size")
        .and_then(|v| {
            // Reported with a unit, e.g. "-1" or "1024MB".
            let digits: String = v
                .chars()
                .take_while(|c| c.is_ascii_digit() || *c == '-')
                .collect();
            digits.parse::<i64>().ok()
        })
        .map(|v| v >= 0)
        .unwrap_or(false)
}

/// Relaunch any worker whose heartbeat has gone stale.
///
/// Called from the tick of every pg_keyspace worker. Registration is dynamic
/// because a statically registered worker cannot be re-registered after the
/// postmaster has dropped it.
fn health_watchdog() {
    if GUC_WATCHDOG_SECS.get() <= 0 || HEALTH_BASE.load(Ordering::Acquire).is_null() {
        return;
    }
    let persisted = ks_tier() != Tier::Ephemeral;
    let now = store::now_micros();
    let stale = health_stale_us();
    let nworkers = worker_count();
    for i in 0..health_slot_count() {
        // Workers that are not supposed to be running are not gaps. The
        // invalidation pool does not follow the persistence tiers -- Mode B
        // runs on an ephemeral cluster too -- so it is gated on its own GUC
        // rather than on `persisted`.
        if i > health_expiry_slot() {
            if !GUC_ROWCACHE_DECODE.get() {
                continue;
            }
        } else if !persisted && i >= nworkers {
            continue;
        }
        let sl = match health_slot(i) {
            Some(s) => s,
            None => continue,
        };
        let last = sl.last_seen_us.load(Ordering::Acquire);
        // Never claimed means nobody has started yet, which startup handles.
        if last == 0 || now.saturating_sub(last) < stale {
            continue;
        }
        // One relaunch between all the workers that noticed, not one each.
        let launched = sl.last_launch_us.load(Ordering::Acquire);
        if now.saturating_sub(launched) < stale {
            continue;
        }
        if sl
            .last_launch_us
            .compare_exchange(launched, now, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            continue;
        }
        health_relaunch(i, nworkers);
    }
}

fn health_relaunch(i: usize, nworkers: usize) {
    let (name, func, arg) = if i < nworkers {
        (format!("pg_keyspace: RESP slot worker {i}"), "pg_keyspace_worker_main", i as i32)
    } else if i < health_expiry_slot() {
        let sh = i - nworkers;
        (format!("pg_keyspace: persistence worker {sh}"), "pg_keyspace_persist_main", sh as i32)
    } else if i == health_expiry_slot() {
        ("pg_keyspace: expiry worker".to_string(), "pg_keyspace_expiry_main", 0)
    } else {
        // A pool member (#120). The argument is the POOL INDEX, never a
        // database: a background worker has to choose its database before it
        // connects, and it makes that choice from the shared directory on
        // start. So a relaunch needs to carry nothing but which slot to claim,
        // and the replacement picks up whatever is unserved at that moment
        // rather than re-binding to a database that may no longer need it.
        let k = i - health_expiry_slot() - 1;
        (
            format!("pg_keyspace: rowcache invalidation worker {k}"),
            "pg_keyspace_invalidation_main",
            k as i32,
        )
    };
    log!("pg_keyspace watchdog: '{name}' has not beaten in {}s; relaunching it", GUC_WATCHDOG_SECS.get());
    let built = BackgroundWorkerBuilder::new(&name)
        .set_library("pg_keyspace")
        .set_function(func)
        .set_argument(arg.into_datum())
        .set_restart_time(Some(Duration::from_secs(2)))
        .enable_spi_access()
        .set_notify_pid(0)
        .load_dynamic();
    if built.is_err() {
        log!(
            "pg_keyspace watchdog: could not relaunch '{name}' (max_worker_processes reached?);              will retry"
        );
    }
}

/// Shared bytes for the cross-process pub/sub bus.
fn pubsub_bytes() -> usize {
    pubsub_shm::bytes_for(
        worker_count(),
        GUC_PUBSUB_ROUTES.get().max(16) as usize,
        (GUC_PUBSUB_RING_KB.get().max(4) as usize) * 1024,
    )
}
/// Total shared memory for all rings, laid out contiguously.
fn ring_total_bytes() -> usize {
    ring_count() * ring_stride()
}

static mut PREV_SHMEM_REQUEST_HOOK: Option<unsafe extern "C" fn()> = None;
static mut PREV_SHMEM_STARTUP_HOOK: Option<unsafe extern "C" fn()> = None;

fn ks_config() -> Config {
    let keys = GUC_KEYS.get().max(1024) as u32;
    let val = GUC_VAL_BYTES.get().max(1) as u64;
    Config::for_capacity(1, keys, val)
}

/// Number of shared-nothing RESP slot workers. Each owns one keyspace
/// segment of `ks_config().total_bytes()`, laid out contiguously in the shared
/// block, and listens on `port + index`.
fn worker_count() -> usize {
    GUC_WORKERS.get().max(1) as usize
}

/// Base pointer of worker `w`'s keyspace segment within the contiguous block.
fn seg_base_for(w: usize) -> *mut u8 {
    let base = SEG_BASE.load(Ordering::Acquire);
    if base.is_null() {
        return std::ptr::null_mut();
    }
    unsafe { base.add(w * ks_config().total_bytes()) }
}

fn ks_tier() -> Tier {
    match GUC_DURABILITY.get().and_then(|c| c.to_str().ok()) {
        Some("relaxed") => Tier::Relaxed,
        Some("durable") => Tier::Durable,
        Some("replicated") => Tier::Replicated,
        _ => Tier::Ephemeral,
    }
}

/// True when `synchronous_standby_names` is set to something Postgres will
/// actually wait for.
///
/// This is the whole basis of the `replicated` tier. The persist transaction
/// runs with `synchronous_commit = remote_apply`, and Postgres only waits when
/// `synchronous_standby_names` is non-empty. With it unset, `remote_apply` does
/// not wait at all and an acknowledged "replicated" write is local-durable
/// only.
///
/// Note what is NOT a degradation: a standby that is named but currently
/// disconnected does not silently fall back, Postgres blocks the commit until
/// one appears. That surfaces correctly as a stalled persist worker, held acks
/// and (once the ring fills) client-visible errors, which is the right
/// behaviour for a synchronous tier.
///
/// The dangerous case is the empty setting, and because
/// `synchronous_standby_names` is `sighup` context it can be emptied at
/// runtime with `pg_reload_conf()`. A startup-only check would therefore
/// guarantee nothing after the first reload, which is why the persist worker
/// re-checks this before every commit.
fn sync_standby_configured() -> bool {
    unsafe {
        let s = pg_sys::GetConfigOption(c"synchronous_standby_names".as_ptr(), true, false);
        if s.is_null() {
            false
        } else {
            !CStr::from_ptr(s).to_string_lossy().trim().is_empty()
        }
    }
}

/// Read a Postgres GUC as a trimmed, non-empty string.
fn pg_setting(name: &CStr) -> Option<String> {
    unsafe {
        let s = pg_sys::GetConfigOption(name.as_ptr(), true, false);
        if s.is_null() {
            return None;
        }
        let v = CStr::from_ptr(s).to_string_lossy().trim().to_string();
        if v.is_empty() {
            None
        } else {
            Some(v)
        }
    }
}

/// Resolve a Postgres file setting the way the server itself does: an
/// absolute path is used as-is, a relative one is relative to the data
/// directory. `ssl_cert_file` defaults to the bare name `server.crt`, so
/// inheriting it without this would look for the cert in whatever directory
/// the worker happens to have started in.
fn resolve_in_datadir(p: &str) -> String {
    if p.starts_with('/') {
        return p.to_string();
    }
    match pg_setting(c"data_directory") {
        Some(d) => format!("{}/{}", d.trim_end_matches('/'), p),
        None => p.to_string(),
    }
}

/// `standby.signal` or `recovery.signal` in the data directory, if either is
/// present.
///
/// Postgres reads these at startup to decide whether to enter recovery, and
/// unlike `RecoveryInProgress()` they are readable from `_PG_init`, which runs
/// before the shared memory that call needs exists. The postmaster has already
/// chdir'd into the data directory by then, so a bare name resolves even when
/// `data_directory` is unset (started with `-D` rather than from the config).
fn recovery_signal_file() -> Option<&'static str> {
    let dir = pg_setting(c"data_directory").unwrap_or_else(|| ".".into());
    for name in ["standby.signal", "recovery.signal"] {
        if std::path::Path::new(&format!("{}/{}", dir.trim_end_matches('/'), name)).exists() {
            return Some(name);
        }
    }
    None
}

/// What the RESP listener should do about TLS.
enum TlsChoice {
    /// Nothing configured: serve the RESP wire in the clear.
    Plaintext,
    /// Serve TLS from these files. `from` names the settings they came from,
    /// so the log line says which knob produced the cert in use.
    Serve {
        cert: String,
        key: String,
        from: &'static str,
    },
    /// TLS was asked for but cannot be honoured; the string says why.
    Refuse(String),
}

/// Decide where the RESP port's certificate comes from.
///
/// `pg_keyspace.tls_cert_file`/`tls_key_file` win whenever they are set — an
/// explicit cert for the RESP port is always the more specific instruction.
/// With neither set and `pg_keyspace.tls_use_postgres_cert` on, fall back to
/// the cluster's own `ssl_cert_file`/`ssl_key_file`: the certificate the
/// operator already supplies and renews for `libpq`, reused here so there is
/// no second cert to manage or rotate.
///
/// `ssl = on` is required for that fallback rather than merely preferred.
/// `ssl_cert_file` has a non-empty *default* (`server.crt`), so its value says
/// nothing about whether the operator actually configured TLS; `ssl` is the
/// setting that does. And since the fallback only runs when the operator
/// opted in, a cluster with `ssl = off` is a contradiction to report, not a
/// reason to quietly serve plaintext.
fn tls_choice() -> TlsChoice {
    let explicit = |g: &GucSetting<Option<&'static CStr>>| {
        g.get()
            .and_then(|c| c.to_str().ok().map(str::to_string))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
    };
    match (explicit(&GUC_TLS_CERT), explicit(&GUC_TLS_KEY)) {
        (Some(cert), Some(key)) => {
            return TlsChoice::Serve {
                cert,
                key,
                from: "pg_keyspace.tls_cert_file/tls_key_file",
            }
        }
        (None, None) => {}
        _ => {
            return TlsChoice::Refuse(
                "set BOTH pg_keyspace.tls_cert_file and pg_keyspace.tls_key_file, or neither"
                    .to_string(),
            )
        }
    }

    if !GUC_TLS_USE_PG_CERT.get() {
        return TlsChoice::Plaintext;
    }

    if pg_setting(c"ssl").as_deref() != Some("on") {
        return TlsChoice::Refuse(
            "pg_keyspace.tls_use_postgres_cert is on but the cluster has ssl = off, so there \
             is no Postgres certificate to inherit. Set ssl = on (and ssl_cert_file / \
             ssl_key_file), or give pg_keyspace its own cert with \
             pg_keyspace.tls_cert_file/tls_key_file"
                .to_string(),
        );
    }
    match (
        pg_setting(c"ssl_cert_file"),
        pg_setting(c"ssl_key_file"),
    ) {
        (Some(cert), Some(key)) => TlsChoice::Serve {
            cert: resolve_in_datadir(&cert),
            key: resolve_in_datadir(&key),
            from: "ssl_cert_file/ssl_key_file",
        },
        _ => TlsChoice::Refuse(
            "pg_keyspace.tls_use_postgres_cert is on but ssl_cert_file/ssl_key_file are empty"
                .to_string(),
        ),
    }
}

/// Human-readable reason the `replicated` tier cannot be honoured, if any.
fn check_sync_standby() -> Result<(), String> {
    if !sync_standby_configured() {
        return Err(
            "pg_keyspace.durability = 'replicated' requires synchronous_standby_names \
             to be set: without it synchronous_commit = remote_apply does not wait for \
             any standby, so an acknowledged write would be local-durable only. Set \
             synchronous_standby_names, or use pg_keyspace.durability = 'durable' if \
             local durability is what you want"
                .to_string(),
        );
    }
    Ok(())
}

/// A cheap `Store` view over the shared segment, valid in any backend.
fn store_view() -> Option<Store> {
    store_view_for(0)
}

/// A `Store` view over worker `w`'s segment.
///
/// Shared memory holds one segment per RESP worker, and a key lives only in
/// the segment of the worker that owns its slot. Anything resolving a value on
/// behalf of a specific worker, rather than for the local backend, has to say
/// which one: a by-reference ring record read against the wrong segment finds
/// either the wrong bytes or nothing at all, and finding nothing is
/// indistinguishable from a legitimately superseded write, so it would lose a
/// durable write silently.
///
/// `store_view()` is worker 0 and is correct for single-worker deployments and
/// for the SQL surface, which today only serves worker 0's keyspace.
fn store_view_for(w: usize) -> Option<Store> {
    let base = seg_base_for(w);
    if base.is_null() {
        return None;
    }
    // Opted in per segment, not globally: this is the tenant-scoped RESP
    // keyspace, whose keys carry a real `{tenant}:` prefix. The row cache must
    // NOT opt in -- its keys are `relid_le_bytes ++ pk`, and one relid in every
    // 256 has 0x3a as its low byte, which would read as a one-byte ":" tenant.
    let mut st = unsafe { Store::from_raw(base, &ks_config(), false) };
    st.set_scoped_eviction(GUC_TENANT_SCOPED_EVICTION.get());
    st.set_tenant_arena_pct(GUC_TENANT_ARENA_PCT.get().max(0) as u32);
    Some(st)
}

/// A cheap `Store` view over slot worker `w`'s segment, valid in any backend.
fn store_view_for_worker(w: usize) -> Option<Store> {
    let base = seg_base_for(w);
    if base.is_null() {
        return None;
    }
    let mut st = unsafe { Store::from_raw(base, &ks_config(), false) };
    st.set_scoped_eviction(GUC_TENANT_SCOPED_EVICTION.get());
    st.set_tenant_arena_pct(GUC_TENANT_ARENA_PCT.get().max(0) as u32);
    Some(st)
}

/// A `Store` view over the segment that owns `key`.
///
/// Workers are shared-nothing: a key lives in exactly one segment, the one whose
/// slot range covers it (`crc16::key_owner`). The SQL surface must route by the
/// same function the RESP workers and recovery use, or `supacache.get` would
/// read worker 0's segment for a key that lives on worker 3 and report a miss.
fn store_view_for_key(key: &[u8]) -> Option<Store> {
    store_view_for_worker(crc16::key_owner(key, worker_count()))
}

// ---- extension init ------------------------------------------------------

#[pg_guard]
pub extern "C" fn _PG_init() {
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        error!("pg_keyspace must be loaded via shared_preload_libraries");
    }

    GucRegistry::define_int_guc(
        "pg_keyspace.port",
        "TCP port the RESP slot worker listens on",
        "Cluster clients connect here; stock RESP2.",
        &GUC_PORT,
        1,
        65535,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.keys",
        "Keyspace capacity (entries) for the shared-memory segment",
        "Sizes the hash table, entry arena and slab arena at startup.",
        &GUC_KEYS,
        1024,
        1_000_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.workers",
        "Number of shared-nothing RESP slot workers",
        "Each worker owns its own shared-memory segment and listens on \
         pg_keyspace.port + its index; clients shard keys across the ports \
         (Redis-Cluster style). Aggregate throughput scales ~linearly. Persistence \
         and the Mode B row cache stay single-worker in this slice, so >1 forces \
         the ephemeral tier. Changing it needs a restart (it resizes shared memory).",
        &GUC_WORKERS,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.val_bytes",
        "Average value size used to size the slab arena",
        "",
        &GUC_VAL_BYTES,
        1,
        1_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.max_value_bytes",
        "Largest value (bulk string) accepted from a RESP client",
        "Equivalent to Valkey/Redis proto-max-bulk-len. A value still has to fit          the keyspace arena to be stored; one that does not is refused with the          standard OOM error rather than acknowledged.",
        &GUC_MAX_VALUE_BYTES,
        1024,
        i32::MAX,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.commit_window_us",
        "Commit-batching window in microseconds for logged durability tiers",
        "One fsync is amortised across all writes staged within a window.",
        &GUC_COMMIT_WINDOW_US,
        0,
        1_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.durability",
        "Durability tier for RESP writes: ephemeral|relaxed|durable|replicated",
        "ephemeral keeps writes shmem-only; non-ephemeral persists to supacache.kv.",
        &GUC_DURABILITY,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.database",
        "Database that holds the supacache.kv backing tables",
        "",
        &GUC_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.persist_window_ms",
        "How often the persistence worker drains the ring when idle, in ms",
        "Under load it drains continuously; this only bounds idle latency.",
        &GUC_PERSIST_WINDOW_MS,
        1,
        60_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.watchdog_secs",
        "Relaunch a pg_keyspace worker whose heartbeat is this old, 0 to disable",
        "pg_terminate_backend deregisters a background worker permanently rather          than restarting it, so without this persistence stops until the cluster does.",
        &GUC_WATCHDOG_SECS,
        0,
        3600,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.pubsub_routes",
        "Distinct pub/sub channels and patterns the shared routing table holds",
        "Subscriptions beyond this are refused and counted in pubsub_stats().",
        &GUC_PUBSUB_ROUTES,
        16,
        1_048_576,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.pubsub_buffer_mb",
        "Undelivered pub/sub one subscriber may hold before it is disconnected (0 = no limit)",
        "A subscriber that stops reading has its messages buffered by the worker, \
         and nothing else bounds that: pg_keyspace.max_held_reply_bytes applies on \
         the dispatch path, so it only ever fires for a connection that is SENDING \
         commands, and a pure subscriber sends none. Measured before this limit \
         existed, publishing 256 MB to a subscriber that never read took a worker \
         from 708 MB RSS to 961 MB, linear in what was published -- which in a \
         background worker ends at the OOM killer, taking the cluster with it. \
         Past the limit the subscriber is disconnected and the reason logged, \
         which is what redis does (its pubsub class defaults to the same 32 MiB) \
         and the only option that leaves the client able to tell: one that has \
         silently missed messages but is still subscribed looks exactly like one \
         that has not. 0 disables the limit and restores the old behaviour.",
        &GUC_PUBSUB_BUFFER_MB,
        0,
        64 * 1024,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.pubsub_ring_kb",
        "Queue per ordered worker pair for cross-worker pub/sub, in KB",
        "A publish to a worker whose queue is full is dropped and counted.",
        &GUC_PUBSUB_RING_KB,
        4,
        65_536,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.ring_mb",
        "Size of each RESP->persistence ring buffer, in MB",
        "Absorbs write bursts so the RESP path never blocks on persistence.",
        &GUC_RING_MB,
        1,
        4096,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.persist_workers",
        "Number of persistence workers (and rings) draining writes in parallel",
        "Writes are sharded by key slot; more workers scale durable throughput.",
        &GUC_PERSIST_WORKERS,
        1,
        16,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.ttl_bucket_secs",
        "Width of a TTL time-bucket partition, in seconds",
        "Keys with a TTL persist into supacache.kv_ttl, range-partitioned by expiry bucket.",
        &GUC_TTL_BUCKET_SECS,
        1,
        86_400,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.ttl_sweep_secs",
        "How often the expiry worker drops fully-past TTL partitions, in seconds",
        "",
        &GUC_TTL_SWEEP_SECS,
        1,
        3_600,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.cluster_announce_host",
        "Host advertised in MOVED redirects and CLUSTER topology replies",
        "Slot workers bind 0.0.0.0, so a redirect or slot map must name an address \
         the client can reach. Required when a persisted tier runs with workers > 1; \
         set it to the hostname or IP your clients connect to (e.g. 127.0.0.1 for \
         a purely local deployment).",
        &GUC_ANNOUNCE_HOST,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_mb",
        "Size of the Mode B row-cache shared-memory segment, in MB",
        "Holds raw heap-tuple bytes keyed by (relid, pk); read by the planner-hook custom scan.",
        &GUC_ROWCACHE_MB,
        1,
        4096,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    GucRegistry::define_string_guc(
        "pg_keyspace.tls_cert_file",
        "PEM certificate file for RESP TLS; set with tls_key_file to enable TLS",
        "When both cert and key are set, the RESP port serves TLS so the AUTH password \
         is encrypted on the wire. Empty = plaintext.",
        &GUC_TLS_CERT,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.tls_key_file",
        "PEM private-key file for RESP TLS; set with tls_cert_file to enable TLS",
        "",
        &GUC_TLS_KEY,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.tls_use_postgres_cert",
        "Serve RESP TLS with the cluster's own ssl_cert_file/ssl_key_file",
        "Off (default): the RESP port is plaintext unless pg_keyspace.tls_cert_file and \
         tls_key_file are set. On: with those unset, inherit the certificate Postgres \
         already serves libpq with, so there is no second cert to supply or rotate. \
         Requires ssl = on. Left off by default because enabling TLS implicitly would \
         break every plaintext RESP client at the next restart.",
        &GUC_TLS_USE_PG_CERT,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.auto_upgrade",
        "Bring the extension's SQL catalogue up to the running library at worker start",
        "On (default): worker 0 runs ALTER EXTENSION pg_keyspace UPDATE when it finds the \
         installed extension older than the library's default_version. Postgres runs an \
         extension's SQL only at CREATE EXTENSION, so a cluster that takes a newer \
         pg_keyspace.so otherwise keeps the catalogue it was created with: functions and \
         views the library provides are absent, and a signature that gained columns reports \
         the old shape without raising -- a failure with no error and no log line. Applies \
         to the database named by pg_keyspace.database; a background worker connects to one \
         database, so any other database holding the extension is still upgraded by hand. \
         Off: the catalogue is the operator's to update, and the version skew is reported \
         at start rather than repaired.",
        &GUC_AUTO_UPGRADE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.require_mask",
        "Require supatype_mask to be loaded (and outermost) before serving",
        "Off (default): pg_keyspace runs standalone, no supatype_mask dependency. \
         On: fail closed unless mask is loaded and outermost (the Supatype platform \
         sets this). When mask is present its order is checked either way.",
        &GUC_REQUIRE_MASK,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.rowcache_decode",
        "Enable the keys-only Mode B invalidation worker",
        "Consumes a logical replication slot (plugin supacache_keys) and drops changed \
         rows from the row cache. Requires wal_level=logical; holds a replication slot.",
        &GUC_ROWCACHE_DECODE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.rowcache_slot",
        "Replication slot name for the Mode B invalidation worker",
        "Created on demand with the keys-only supacache_keys output plugin.",
        &GUC_ROWCACHE_SLOT,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_decode_ms",
        "How often the Mode B invalidation worker drains the slot, in ms",
        "",
        &GUC_ROWCACHE_DECODE_MS,
        10,
        60_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_partitions",
        "How many partitions the Mode B row-cache segment is carved into",
        "Also the number of writer locks it has: row-cache writers take one \
         exclusive LWLock per partition, so two backends writing different \
         partitions never wait on each other. It matters because the writers \
         are ordinary BACKENDS -- rowcache_put, registration, and above all \
         read-through, which makes a writer of every backend that misses -- and \
         with the row cache serving every database, one lock for the whole \
         segment would be a cluster-wide serialisation point. Each partition is \
         a separate arena carved out of `pg_keyspace.rowcache_mb`, which still \
         means the size of the WHOLE segment, so raising this divides the \
         segment rather than growing it. Rounded up to a power of two.",
        &GUC_ROWCACHE_PARTITIONS,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_invalidation_workers",
        "Size of the bounded pool of Mode B invalidation workers",
        "The row cache serves every database that has registrations, and each \
         one needs its own logical replication slot, because a slot only ever \
         decodes changes from the database it was created in. This bounds how \
         many are drained AT ONCE: worker count is something you set, not a \
         function of how many databases exist. With no more participating \
         databases than workers, every database has its own worker and \
         invalidation latency is `rowcache_decode_ms`, exactly as with one \
         database. Above that, workers CYCLE -- each leases a database for \
         `rowcache_lease_ms`, drains it, and hands the slot on -- so latency \
         becomes the cycle time, which `supacache.pg_stat_keyspace_rowcache_\
         databases.stale_after_ms` reports rather than leaving you to derive. \
         Raise it with `max_worker_processes`, which must also cover the RESP, \
         persistence and expiry workers.",
        &GUC_ROWCACHE_INVALIDATION_WORKERS,
        1,
        64,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_lease_ms",
        "How long a pooled invalidation worker holds one database before handing it on",
        "Only has an effect when there are more participating databases than \
         `rowcache_invalidation_workers`, because otherwise nothing is waiting \
         for a turn and a worker keeps its database indefinitely. Shorter means \
         a fairer cycle and more worker restarts; longer means fewer restarts \
         and a longer wait for the databases queued behind.",
        &GUC_ROWCACHE_LEASE_MS,
        100,
        600_000,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_max_databases",
        "How many databases the row cache can serve at once",
        "Sizes the shared directory that records which databases have row-cache \
         registrations and how current each one is. A database that cannot be \
         recorded cannot be served, so registration is REFUSED rather than \
         accepted into a cache nothing would invalidate -- the failure this \
         directory exists to prevent. Cheap: 128 bytes per entry.",
        &GUC_ROWCACHE_MAX_DATABASES,
        1,
        1024,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.tenant_ops_per_sec",
        "Commands per second one tenant may issue (0 = no limit)",
        "Bounds how much of a worker's event loop one tenant can ask for. The \
         ring share bounds persistence and scoped eviction bounds cache memory, \
         but neither sees a tenant issuing only reads: it stages no ring records \
         and evicts nothing, and can still saturate the loop. A tenant may burst \
         up to one second's worth and then proceeds at the rate. Over-rate \
         commands are held and retried, not refused, so no client sees a new \
         error. Connections with no tenant scope are never limited. 0 (default) \
         disables it entirely, including the clock read.",
        &GUC_TENANT_OPS_PER_SEC,
        0,
        10_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.tenant_scoped_eviction",
        "Evict a tenant's own cold keys before another tenant's",
        "On (default) makes the cache's CLOCK sweep prefer a victim under the \
         same `{tenant}:` prefix as the key being inserted, so a cold-key flood \
         from one tenant recycles its own space instead of evicting everyone \
         else's working set. It is a preference, not a budget: a tenant with \
         nothing evictable of its own still falls through to the ordinary \
         sweep, so a small or new tenant is never starved. Keys with no `:` -- \
         an unscoped or exempt deployment -- are evicted exactly as before.",
        &GUC_TENANT_SCOPED_EVICTION,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.tenant_arena_pct",
        "Cap any one tenant at this percentage of a partition's cache entries",
        "0 (default) = no budget: tenant_scoped_eviction still stops a flood from \
         evicting other tenants, but nothing caps the share one tenant may hold. \
         Above 0, a tenant over its share is evicted from first, whoever is \
         inserting. A preference protects you from a flood; a budget also reclaims \
         from a tenant that simply grew.",
        &GUC_TENANT_ARENA_PCT,
        0,
        100,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.tenant_ring_share",
        "Give each tenant a share of the persistence ring rather than first come, first served",
        "On (default) caps how many ring bytes one tenant may have in flight at \
         once, to capacity/active-tenants, once a ring is more than half full. A \
         tenant over its share waits for its own records to drain instead of \
         taking space other tenants' writes need; below half full, and for \
         connections with no tenant scope (unauthenticated, or an exempt service \
         role), nothing is enforced. Off restores first come, first served, where \
         one tenant writing hard enough to keep a ring full stalls every other \
         tenant sharing it.",
        &GUC_TENANT_RING_SHARE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.relay_channels",
        "Channels relayed to the peers in supacache.peer (comma-separated globs)",
        "Empty (the default) relays nothing, and cross-instance pub/sub is off. \
         A channel matching one of these globs is additionally delivered to every \
         enabled row of supacache.peer, by a background worker over libpq -- so \
         no WAL, no new protocol, and Postgres's own authentication, TLS and \
         pg_hba apply to the peer link. Opt in per pattern rather than relaying \
         everything: a relay whose cost grows with the number of instances is \
         what sharded pub/sub exists to avoid, and paying it on presence or \
         typing traffic is the worst version of that. The receiver count \
         PUBLISH returns stays LOCAL: there is no cheap honest cross-instance \
         answer, and redis reports per node too.",
        &GUC_RELAY_CHANNELS,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.relay_user",
        "RESP username the relay subscribes with (empty = no AUTH)",
        "Needed only where the cluster requires AUTH. What the relay may forward \
         is bounded by this credential's scope, which is deliberate: give it an \
         exempt service role to relay every tenant's channels, or a \
         tenant-scoped one to relay only that tenant's. The relay is a RESP \
         client like any other, so the permission model is the existing one \
         rather than a second one invented for peers.",
        &GUC_RELAY_USER,
        GucContext::Sighup,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.relay_secret",
        "Secret for pg_keyspace.relay_user",
        "Superuser-visible only. Set it in postgresql.conf alongside relay_user.",
        &GUC_RELAY_SECRET,
        GucContext::Sighup,
        GucFlags::SUPERUSER_ONLY,
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.tenant",
        "Tenant a SQL backend acts as, scoping supacache.publish() to {tenant}:",
        "Unset (default) leaves supacache.publish() superuser-only, as it was \
         before this setting existed. Set, a non-superuser may publish, and the \
         channel name is forced to `{tenant}:{channel}` -- the same prefix a RESP \
         connection's credential forces, so a subscriber authenticated as that \
         tenant receives it under the name it subscribed to and no other tenant \
         can be reached. Set it cluster-wide in postgresql.conf for a \
         single-tenant cluster, or per role with ALTER ROLE ... SET for a \
         multi-tenant one. A role cannot set or reset it for itself. If an \
         operator hands that right over with GRANT SET ON PARAMETER, a value the \
         caller set in its own session is refused rather than trusted.",
        &GUC_TENANT,
        GucContext::Suset,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.rowcache_readthrough",
        "Populate the row cache on a miss instead of requiring rowcache_put",
        "Off (default) caches only what rowcache_put places, and a pk lookup for an \
         uncached row takes the ordinary index path. On, a registered table's pk \
         lookups are served by the row cache whether or not the row is cached yet, \
         and a miss reads the row and stores it. That removes the manual warming \
         step, at the cost of making a miss a fetch through the cache node rather \
         than a plain index scan -- worth it for a working set that fits \
         pg_keyspace.rowcache_mb, not for random access over a much larger table.",
        &GUC_ROWCACHE_READTHROUGH,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_bool_guc(
        "pg_keyspace.rowcache_refill",
        "Refill a changed hot key with the current row instead of only dropping it",
        "Off (default) is drop-only; the next read repopulates lazily. Deleted rows \
         are always dropped, never refilled.",
        &GUC_ROWCACHE_REFILL,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    // Mode B: register the custom-scan methods and install the pathlist hook.
    rowcache_planner_init();

    // Chain the shmem hooks so the segment is requested and initialised.
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(ks_shmem_request);
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(ks_shmem_startup);
    }

    // Register the RESP slot worker (a real Postgres background worker). When a
    // logged durability tier is configured it needs an SPI database connection
    // to persist into supacache.kv; ephemeral needs only shared memory.
    // The RESP worker always needs SPI now: for recovery (persisted tiers) and
    // to load the RESP AUTH credentials / keyspace ACL at startup.
    // N shared-nothing RESP slot workers: each attaches its own keyspace
    // segment and listens on port + its index. The index is the bgworker arg.
    let nworkers = worker_count();
    let persisted = ks_tier() != Tier::Ephemeral;
    for w in 0..nworkers {
        BackgroundWorkerBuilder::new(&format!("pg_keyspace: RESP slot worker {w}"))
            .set_library("pg_keyspace")
            .set_function("pg_keyspace_worker_main")
            .set_argument((w as i32).into_datum())
            .set_restart_time(Some(Duration::from_secs(2)))
            .enable_spi_access()
            .load();
    }

    // Dedicated persistence workers: each drains its own rings and bulk-upserts
    // into supacache.kv, so the RESP worker never touches SPI on the hot path.
    // Each is passed its *shard* index as the bgworker argument and drains that
    // shard's ring from every slot worker, so the process count is set by
    // pg_keyspace.persist_workers alone and does not scale with workers.
    if persisted {
        for i in 0..persist_shards() {
            BackgroundWorkerBuilder::new(&format!("pg_keyspace: persistence worker {i}"))
                .set_library("pg_keyspace")
                .set_function("pg_keyspace_persist_main")
                .set_argument((i as i32).into_datum())
                .set_restart_time(Some(Duration::from_secs(2)))
                .enable_spi_access()
                .load();
        }
        // expiry worker: drops fully-past TTL partitions.
        BackgroundWorkerBuilder::new("pg_keyspace: expiry worker")
            .set_library("pg_keyspace")
            .set_function("pg_keyspace_expiry_main")
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
    }

    // A misconfiguration that would otherwise be silent: read-through fills the
    // cache from ordinary reads, and without the invalidation worker nothing
    // would ever take a row back out again.
    if GUC_ROWCACHE_READTHROUGH.get() && !GUC_ROWCACHE_DECODE.get() {
        log!(
            "pg_keyspace: pg_keyspace.rowcache_readthrough is on but \
             pg_keyspace.rowcache_decode is off, so nothing would invalidate what \
             read-through caches. Read-through is INACTIVE until decode is enabled; \
             the row cache still serves whatever rowcache_put places in it."
        );
    }

    // Mode B: a BOUNDED POOL of keys-only invalidation workers keeps the row
    // cache coherent in every database that has registrations (#120).
    //
    // Bounded, and by a setting rather than by how many databases exist. A
    // worker per database would make the cluster's process count a function of
    // its tenant count, which is the thing per-project-database provisioning is
    // least able to afford. The autovacuum launcher makes the same trade.
    //
    // Each member is given its POOL INDEX, not a database. A background worker
    // must choose its database before it connects and can never change it, so a
    // member reads the shared directory on start, leases a database that nobody
    // is serving, and binds to that. Handing a turn on means exiting and being
    // restarted, which is what `set_restart_time` below is for -- and it is why
    // the restart interval is a second rather than the five the other workers
    // use, since here it is not a failure path but the cycle itself.
    // Registered only when relaying is configured, so a deployment that does not
    // relay carries no extra process. relay_channels is SIGHUP, but this read
    // happens at _PG_init: adding a pattern while the worker runs takes effect
    // on its next reconnect, whereas going from none to some needs a restart to
    // start the worker at all. Documented rather than papered over with a
    // second GUC.
    if !relay_patterns().is_empty() {
        BackgroundWorkerBuilder::new("pg_keyspace: cross-instance relay")
            .set_function("pg_keyspace_relay_main")
            .set_library("pg_keyspace")
            .enable_spi_access()
            .set_restart_time(Some(Duration::from_secs(5)))
            .load();
    }
    if GUC_ROWCACHE_DECODE.get() {
        for k in 0..rowcache_pool_size() {
            BackgroundWorkerBuilder::new(&format!(
                "pg_keyspace: rowcache invalidation worker {k}"
            ))
            .set_library("pg_keyspace")
            .set_function("pg_keyspace_invalidation_main")
            .set_argument((k as i32).into_datum())
            .set_restart_time(Some(Duration::from_secs(1)))
            .enable_spi_access()
            .load();
        }
        // One slot per participating database, and a slot holds WAL until it is
        // consumed. With one database that risk was singular and visible; with
        // several, ANY ONE stalled database pins WAL for the whole cluster, so
        // one slow tenant can fill the WAL volume for everyone. Postgres already
        // solves this and the solution is off by default, so say so at the only
        // moment an operator is definitely reading the log.
        if !wal_keep_size_bounded() {
            log!(
                "pg_keyspace: max_slot_wal_keep_size is unset (-1), so a row-cache \
                 invalidation slot can retain WAL without bound. The row cache holds one \
                 replication slot PER participating database, so a single database whose \
                 invalidation stalls can fill the WAL volume for the entire cluster. Set \
                 max_slot_wal_keep_size; a slot that exceeds it is invalidated by the \
                 server, and pg_keyspace then marks that database's cache incoherent and \
                 rebuilds it rather than serving rows it can no longer keep current."
            );
        }
    }

    // Registered is not started. Every worker above asks for SPI, and
    // `enable_spi_access()` registers it with `BgWorkerStartTime::RecoveryFinished`
    // because SPI is not reachable before recovery ends. On a streaming standby
    // recovery never ends, so Postgres never launches any of them -- and nothing
    // fails, so the only symptom is a RESP port that never answers and a log with
    // not one line about it. Say it here, which is the last pg_keyspace code that
    // runs on a standby.
    if let Some(sig) = recovery_signal_file() {
        log!(
            "pg_keyspace: this cluster is starting in recovery ({sig} present), so its \
             RESP workers are deferred: they use SPI, and Postgres does not launch such \
             workers until recovery finishes. The RESP port will NOT answer while this \
             cluster is a standby. Nothing further is needed -- the workers start on their \
             own when recovery ends, whether that is a promotion or a point-in-time restore \
             reaching its target."
        );
    }

    log!("pg_keyspace: initialised (shmem hooks + {nworkers} RESP worker(s) registered)");
}

#[pg_guard]
extern "C" fn ks_shmem_request() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
            prev();
        }
        // One keyspace segment per shared-nothing slot worker, contiguous.
        pg_sys::RequestAddinShmemSpace(worker_count() * ks_config().total_bytes());
        pg_sys::RequestAddinShmemSpace(ring_total_bytes());
        pg_sys::RequestAddinShmemSpace(rowcache_config().total_bytes());
        pg_sys::RequestAddinShmemSpace(pubsub_bytes());
        pg_sys::RequestAddinShmemSpace(health_bytes());
        // One LWLock per row-cache partition, to serialise row-cache writers
        // without serialising them against each other across the whole segment
        // (#127, partitioned for #120). Requested here because
        // RequestNamedLWLockTranche is only legal from the shmem request hook;
        // resolved to a pointer in the startup hook.
        pg_sys::RequestNamedLWLockTranche(RC_LOCK_NAME.as_ptr(), rowcache_partitions() as i32);
        // Which databases the row cache serves (#120), plus the one lock that
        // guards allocating an entry in it.
        pg_sys::RequestAddinShmemSpace(rcdb_bytes());
        pg_sys::RequestNamedLWLockTranche(RC_DB_LOCK_NAME.as_ptr(), 1);
        pg_sys::RequestNamedLWLockTranche(PS_EXT_LOCK_NAME.as_ptr(), 1);
    }
}

#[pg_guard]
extern "C" fn ks_shmem_startup() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
            prev();
        }
        let cfg = ks_config();
        let size = cfg.total_bytes();
        let nworkers = worker_count();
        let block = nworkers * size;
        let mut found = false;
        let ptr = pg_sys::ShmemInitStruct(SEG_NAME.as_ptr(), block, &mut found) as *mut u8;
        if ptr.is_null() {
            error!("pg_keyspace: ShmemInitStruct returned NULL");
        }
        // First backend (postmaster) initialises every worker's segment; the rest
        // just publish the block base. Each worker uses base + w*size.
        if !found {
            for w in 0..nworkers {
                let _view = Store::from_raw(ptr.add(w * size), &cfg, true);
            }
        } else {
            // Attaching to a segment this process did not lay out. Reading it
            // with the wrong geometry does not fail, it just lands on the wrong
            // offsets for the life of the process, so refuse instead.
            for w in 0..nworkers {
                if let Err(why) = Store::check_header(ptr.add(w * size), &cfg) {
                    error!("pg_keyspace: shared segment for worker {w} is unusable: {why}");
                }
            }
        }
        SEG_BASE.store(ptr, Ordering::Release);

        // Persistence ring segment: N contiguous rings of `ring_stride()` bytes.
        let rbytes = ring_total_bytes();
        let stride = ring_stride();
        let cap = (GUC_RING_MB.get().max(1) as usize) * 1024 * 1024;
        let mut rfound = false;
        let rptr = pg_sys::ShmemInitStruct(RING_NAME.as_ptr(), rbytes, &mut rfound) as *mut u8;
        if !rptr.is_null() {
            if !rfound {
                std::ptr::write_bytes(rptr, 0, rbytes);
                for i in 0..ring_count() {
                    ring::init(rptr.add(i * stride), cap);
                }
            }
            RING_BASE.store(rptr, Ordering::Release);
        }

        // Mode B row-cache segment.
        let rc_cfg = rowcache_config();
        let rc_bytes = rc_cfg.total_bytes();
        let mut rc_found = false;
        let rcptr = pg_sys::ShmemInitStruct(ROWCACHE_NAME.as_ptr(), rc_bytes, &mut rc_found) as *mut u8;
        if !rcptr.is_null() {
            if rc_found {
                if let Err(why) = Store::check_header(rcptr, &rc_cfg) {
                    error!("pg_keyspace: row-cache segment is unusable: {why}");
                }
            } else {
                let _ = Store::from_raw(rcptr, &rc_cfg, true);
            }
            ROWCACHE_BASE.store(rcptr, Ordering::Release);
        }
        // Resolve the row-cache writer locks (#127). GetNamedLWLockTranche must
        // run with AddinShmemInitLock held, which is exactly this hook; every
        // backend then inherits the pointer across the fork. The tranche is
        // `rowcache_partitions()` locks laid out contiguously, so only the base
        // is stored and `rowcache_write` indexes it.
        let tranche = pg_sys::GetNamedLWLockTranche(RC_LOCK_NAME.as_ptr());
        if !tranche.is_null() {
            ROWCACHE_LOCKS.store(tranche, Ordering::Release);
        }

        // The row-cache database directory (#120). Zeroed means every entry is
        // free and no database is known, which is what a fresh cluster should
        // believe: registrations are rediscovered by probing, and until a
        // database is in here its cache is not served at all.
        let db_bytes = rcdb_bytes();
        let mut db_found = false;
        let dbptr = pg_sys::ShmemInitStruct(RC_DB_NAME.as_ptr(), db_bytes, &mut db_found) as *mut u8;
        if !dbptr.is_null() {
            if !db_found {
                std::ptr::write_bytes(dbptr, 0, db_bytes);
            }
            RC_DB_BASE.store(dbptr, Ordering::Release);
        }
        let db_tranche = pg_sys::GetNamedLWLockTranche(RC_DB_LOCK_NAME.as_ptr());
        if !db_tranche.is_null() {
            RC_DB_LOCK.store(std::ptr::addr_of_mut!((*db_tranche).lock), Ordering::Release);
        }
        let ps_tranche = pg_sys::GetNamedLWLockTranche(PS_EXT_LOCK_NAME.as_ptr());
        if !ps_tranche.is_null() {
            PS_EXT_LOCK.store(std::ptr::addr_of_mut!((*ps_tranche).lock), Ordering::Release);
        }
        // The TTL clock anchor (#110), shared so every process agrees.
        //
        // A per-process anchor would be worse than the bug it fixes: backends
        // that started either side of a wall-clock step would disagree about
        // whether a key is expired, turning a global shift into per-process
        // inconsistency. Written once by whoever creates the segment; every
        // other process adopts it. On EXEC_BACKEND platforms this hook re-runs
        // per backend and `found` is true, so they adopt rather than re-anchor;
        // on fork platforms children inherit the adopted statics anyway.
        let mut clk_found = false;
        let ckptr = pg_sys::ShmemInitStruct(
            CLOCK_NAME.as_ptr(),
            std::mem::size_of::<[i64; 2]>(),
            &mut clk_found,
        ) as *mut i64;
        if !ckptr.is_null() {
            if !clk_found {
                let (r, b) = store::capture_clock_anchor();
                std::ptr::write(ckptr, r);
                std::ptr::write(ckptr.add(1), b);
            }
            store::adopt_clock_anchor(std::ptr::read(ckptr), std::ptr::read(ckptr.add(1)));
        }

        // Worker liveness table. Zeroed means "never claimed", which is what
        // startup expects, so nothing else needs initialising here.
        let h_bytes = health_bytes();
        let mut h_found = false;
        let hptr = pg_sys::ShmemInitStruct(HEALTH_NAME.as_ptr(), h_bytes, &mut h_found) as *mut u8;
        if !hptr.is_null() {
            if !h_found {
                std::ptr::write_bytes(hptr, 0, h_bytes);
            }
            HEALTH_BASE.store(hptr, Ordering::Release);
        }

        // Cross-process pub/sub. Only the creator builds the Bus: it is the
        // postmaster, and its wake descriptors are what every worker inherits.
        let ps_bytes = pubsub_bytes();
        let mut ps_found = false;
        let psptr = pg_sys::ShmemInitStruct(PUBSUB_NAME.as_ptr(), ps_bytes, &mut ps_found) as *mut u8;
        if !psptr.is_null() {
            if !ps_found {
                let bus = pubsub::Bus::new_shared(
                    psptr,
                    worker_count(),
                    GUC_PUBSUB_ROUTES.get().max(16) as usize,
                    (GUC_PUBSUB_RING_KB.get().max(4) as usize) * 1024,
                );
                let _ = BUS.set(Arc::new(bus));
            }
            PUBSUB_BASE.store(psptr, Ordering::Release);
        }

        log!(
            "pg_keyspace: shmem ready (store {} bytes, {} rings x {} bytes, rowcache {} bytes, found={})",
            size,
            ring_count(),
            stride,
            rc_bytes,
            found
        );
    }
}

// ---- the background worker: the RESP event loop --------------------------

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_worker_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    // Which shared-nothing slot worker this is: its segment is seg_base_for(w),
    // its RESP port is pg_keyspace.port + w.
    let w = unsafe { i32::from_datum(arg, false) }.unwrap_or(0).max(0) as usize;

    // load-order assertion: supatype_mask must be loaded AFTER pg_keyspace
    // (outermost) so the Query is masked before pg_keyspace ever sees it. If the
    // operator misordered shared_preload_libraries, FAIL CLOSED — park without
    // binding the RESP port rather than serve on an unverified security posture.
    if let Err(why) = check_load_order() {
        log!("pg_keyspace worker: REFUSING to start — {why}");
        while !BackgroundWorker::sigterm_received() {
            std::thread::sleep(Duration::from_secs(1));
        }
        return;
    }

    let base = seg_base_for(w);
    if base.is_null() {
        log!("pg_keyspace worker {w}: shared segment not ready, exiting");
        return;
    }
    let cfg = ks_config();
    // Checked once here, at the point this worker takes up the segment, rather
    // than in the per-query views built over the same base.
    if let Err(why) = unsafe { Store::check_header(base, &cfg) } {
        log!("pg_keyspace worker {w}: shared segment is unusable: {why}; exiting");
        return;
    }
    let store = Arc::new(unsafe {
        let mut st = Store::from_raw(base, &cfg, false);
        st.set_scoped_eviction(GUC_TENANT_SCOPED_EVICTION.get());
        st.set_tenant_arena_pct(GUC_TENANT_ARENA_PCT.get().max(0) as u32);
        st
    });
    // Persistence and recovery are per slot worker: this worker owns a disjoint
    // slot range (crc16::slot_range), its own segment, and its own ring set, so
    // durability and scale-out compose instead of excluding each other.
    let persisted = ks_tier() != Tier::Ephemeral;

    let port = GUC_PORT.get() as u16 + w as u16;
    let mut worker = match server::Worker::new(store.clone(), None, Tier::Ephemeral, "0.0.0.0", port)
    {
        Ok(w) => w,
        Err(e) => {
            log!("pg_keyspace worker: cannot listen on :{port}: {e}");
            return;
        }
    };
    worker.set_max_value_bytes(GUC_MAX_VALUE_BYTES.get().max(1024) as usize);
    worker.set_tenant_rate_limit(GUC_TENANT_OPS_PER_SEC.get().max(0) as u32);
    // Cross-worker pub/sub. Without this a SUBSCRIBE here never sees a PUBLISH
    // on another worker, and the publisher's reply counts only its own local
    // subscribers, so neither side can tell the message was lost.
    if let Some(bus) = BUS.get() {
        worker.set_bus(bus.clone(), w);
    } else if worker_count() > 1 {
        log!(
            "pg_keyspace worker {w}: WARNING no shared pub/sub bus; PUBLISH and              SUBSCRIBE reach only clients connected to this worker"
        );
    }

    // TLS: if a cert+key are configured, wrap the RESP wire in TLS. If TLS
    // was requested but the files fail to load, FAIL CLOSED — park rather than
    // fall back to plaintext on an operator who asked for encryption.
    match tls_choice() {
        TlsChoice::Serve { cert, key, from } => match server::load_tls_config(&cert, &key) {
            Ok(cfg) => {
                worker.set_tls_config(cfg);
                log!("pg_keyspace worker: RESP TLS enabled (cert '{cert}' from {from})");
            }
            Err(e) => {
                log!("pg_keyspace worker: REFUSING to start — TLS requested but cert/key \
                      failed to load ({e}); fix {from}");
                while !BackgroundWorker::sigterm_received() {
                    std::thread::sleep(Duration::from_secs(1));
                }
                return;
            }
        },
        TlsChoice::Plaintext => {}
        TlsChoice::Refuse(why) => {
            log!("pg_keyspace worker: REFUSING to start — {why}");
            while !BackgroundWorker::sigterm_received() {
                std::thread::sleep(Duration::from_secs(1));
            }
            return;
        }
    }

    // Fail closed on a durability promise the cluster cannot keep, the same way
    // a bad TLS cert refuses above. Serving `replicated` with no synchronous
    // standby would acknowledge writes as replicated that are only local.
    if matches!(ks_tier(), Tier::Replicated) {
        if let Err(why) = check_sync_standby() {
            log!("pg_keyspace worker: REFUSING to start — {why}");
            while !BackgroundWorker::sigterm_received() {
                std::thread::sleep(Duration::from_secs(1));
            }
            return;
        }
    }

    // Connect SPI (always): needed to create/read the schema, run the
    // security self-check, load RESP AUTH credentials, and recover from tables.
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // The extension owns the supacache schema; only touch it once CREATE EXTENSION
    // has run (see extension_installed). Without it there is no SQL surface and no
    // backing tables, so serve RESP in ephemeral mode until the operator installs it.
    let ext_ready = extension_installed();
    if ext_ready {
        // Only worker 0 runs the (idempotent) DDL, so N workers do not race on
        // concurrent CREATE ... IF NOT EXISTS at startup.
        if w == 0 {
            // Before the schema DDL, not after: everything below this point --
            // the stats views, the registration catalogue, the functions the
            // worker itself calls -- is the catalogue the library expects, and
            // repairing it first means the rest of startup runs against it.
            pg_catalogue_upgrade();
            pg_ensure_schema();
        }
    } else if persisted {
        log!(
            "pg_keyspace worker: the pg_keyspace extension is not installed in database \
             '{dbname}'; run CREATE EXTENSION pg_keyspace and restart to enable persistence. \
             Serving RESP in ephemeral mode until then."
        );
    }
    let persisted = persisted && ext_ready;

    // security label self-check: a Mode A backing table must have no
    // `supatype` label. If one was added by hand, FAIL CLOSED.
    if ext_ready && kv_has_supatype_label() {
        log!(
            "pg_keyspace worker: REFUSING to start — a supatype security label exists on a \
             supacache relation; Mode A tables must not be masked"
        );
        while !BackgroundWorker::sigterm_received() {
            std::thread::sleep(Duration::from_secs(1));
        }
        return;
    }

    // load RESP AUTH + keyspace ACL. When credentials exist, enforcement is
    // on; when absent, the worker runs in local/no-auth mode (local dev). The
    // credential/ACL tables only exist once the extension is installed; without it
    // there are no credentials to load, so stay in no-auth mode.
    match ext_ready.then(load_auth_config).flatten() {
        Some(auth) => {
            let n = auth.creds.len();
            worker.set_auth_config(auth);
            log!("pg_keyspace worker: AUTH enforced ({n} credentials, ACL + tenant scoping on)");
        }
        None => log!("pg_keyspace worker: no RESP credentials configured — local/no-auth mode"),
    }

    // A persisted multi-worker cluster publishes a slot map and redirects
    // clients by address, so it must know an address clients can reach. Guessing
    // (127.0.0.1) would hand remote clients a topology pointing at themselves —
    // a confusing connection failure rather than a clear misconfiguration. Fail
    // closed, as with a bad TLS cert.
    let announce_host = GUC_ANNOUNCE_HOST
        .get()
        .and_then(|c| c.to_str().ok().map(str::to_string))
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());
    if persisted && worker_count() > 1 && announce_host.is_none() {
        log!(
            "pg_keyspace worker {w}: REFUSING to start — a persisted tier with \
             pg_keyspace.workers > 1 redirects clients by address, so \
             pg_keyspace.cluster_announce_host must be set to a host your clients \
             can reach (use '127.0.0.1' for a local-only deployment)"
        );
        while !BackgroundWorker::sigterm_received() {
            std::thread::sleep(Duration::from_secs(1));
        }
        return;
    }

    // storage & durability: recover shmem from the tables at startup; the
    // steady-state persistence is offloaded to the persistence worker via the
    // ring, so the RESP hot path never touches SPI.
    if persisted {
        for _ in 0..30 {
            if pg_table_ready() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let t0 = std::time::Instant::now();
        let nworkers = worker_count();
        let (lo, hi) = crc16::slot_range(w, nworkers);
        let n = pg_recover(&store, w, nworkers);
        log!(
            "pg_keyspace worker {w}: recovered {n} keys (slots {lo}..{hi}) from \
             supacache.kv in {:?}",
            t0.elapsed()
        );
        // Unconditional, and deliberately outside the persistence branch below:
        // a subscriber that stops reading grows the worker whatever tier it is,
        // and an ephemeral cluster is if anything likelier to be the one with a
        // chatty pub/sub workload and no persistence to slow it down.
        worker.set_pubsub_buf_limit((GUC_PUBSUB_BUFFER_MB.get().max(0) as usize) << 20);
        let rbase = RING_BASE.load(Ordering::Acquire);
        if !rbase.is_null() {
            let stride = ring_stride();
            let ps = persist_shards();
            // This worker's own rings only. The ring is SPSC and slot workers are
            // separate processes, so two workers must never share a producer end.
            let producers: Vec<ring::Producer> = (0..ps)
                .map(|sh| unsafe { ring::Producer::attach(rbase.add(ring_index(w, sh) * stride)) })
                .collect();
            worker.set_ring_producers(producers);
            worker.set_tenant_ring_share(GUC_TENANT_RING_SHARE.get());
            // durable/replicated: hold each write's RESP OK until it commits.
            let sync_ack = matches!(ks_tier(), Tier::Durable | Tier::Replicated);
            worker.set_sync_ack(sync_ack);
            log!(
                "pg_keyspace worker {w}: persistence ON ({ps} ring(s) -> {ps} persistence \
                 worker(s), sync_ack={sync_ack})"
            );
            // Persisted + multi-worker: serve only this worker's slot range and
            // redirect the rest. Recovery restores each key into the segment its
            // slot range covers, so a worker that accepted a key it does not own
            // would lose it on the next restart despite having acked it durable.
            // A cluster-aware client follows the MOVED and lands on the right
            // port; a single-port client gets a loud error instead of silent loss.
            if nworkers > 1 {
                // Checked above: a persisted multi-worker cluster does not reach
                // here without an announce host.
                let host = announce_host.clone().unwrap_or_default();
                let base = GUC_PORT.get() as u16;
                let endpoints: Vec<String> =
                    (0..nworkers).map(|i| format!("{host}:{}", base + i as u16)).collect();
                worker.set_slot_routing(w, nworkers, endpoints);
                let (lo, hi) = crc16::slot_range(w, nworkers);
                log!(
                    "pg_keyspace worker {w}: serving slots {lo}..{hi}; keys outside it \
                     answer MOVED, topology published via CLUSTER SLOTS/SHARDS/NODES \
                     (announced as {host}:{})",
                    base + w as u16
                );
            }
        }
    }

    log!("pg_keyspace worker: RESP listening on 0.0.0.0:{port} (persisted={persisted})");
    // Poll frequently in sync-ack mode so committed durable writes ack promptly.
    let timeout_ms = if matches!(ks_tier(), Tier::Durable | Tier::Replicated) {
        2
    } else {
        500
    };
    // SIGHUP hot-reloads RESP credentials + ACL from the table (and refreshes
    // GUC-derived values like exempt_roles) with no restart: change the creds via
    // supacache.set_credential, then `SELECT pg_reload_conf()`.
    // The RESP worker's own liveness, and the watchdog for everyone else's.
    health_claim(w);
    let mut last_watch = std::time::Instant::now();
    let _ = worker.run_with(
        || {
            if BackgroundWorker::sigterm_received() {
                // No health_release: see the persistence worker's shutdown.
                return server::Tick::Stop;
            }
            // The heartbeat is one atomic store, so it goes on every tick: a
            // beat that is only as fresh as the scan interval would leave this
            // worker looking half-stale to everyone else. The scan itself walks
            // every slot, so it is rate-limited.
            health_beat(w);
            absorb_procsignal_barrier();
            if last_watch.elapsed() >= Duration::from_secs(5) {
                last_watch = std::time::Instant::now();
                health_watchdog();
            }
            if BackgroundWorker::sighup_received() {
                unsafe { pg_sys::ProcessConfigFile(pg_sys::GucContext::PGC_SIGHUP) };
                let cfg = if extension_installed() {
                    load_auth_config()
                } else {
                    None
                };
                let n = cfg.as_ref().map(|c| c.creds.len()).unwrap_or(0);
                // Cert rotation: re-read the same cert/key paths so an
                // in-place renewal (e.g. cert-manager) takes effect with no
                // restart. Only new connections use the new cert. A failed reload
                // keeps the old cert (never drops TLS mid-flight) and warns.
                let mut reload = server::Reload {
                    auth: Some(cfg),
                    tls: None,
                };
                match tls_choice() {
                    TlsChoice::Serve { cert, key, .. } => {
                        match server::load_tls_config(&cert, &key) {
                            Ok(c) => {
                                reload.tls = Some(Some(c));
                                log!("pg_keyspace worker: SIGHUP — reloaded auth ({n} creds) + TLS cert");
                            }
                            Err(e) => log!(
                                "pg_keyspace worker: SIGHUP — reloaded auth ({n} creds); TLS cert \
                                 reload FAILED ({e}), keeping the current cert"
                            ),
                        }
                    }
                    // Both arms keep the running cert. The GUCs behind this
                    // choice are `postmaster`-context, so a reload cannot
                    // legitimately turn TLS off underneath live connections —
                    // and dropping to plaintext is never the safe reading of
                    // an ambiguous reload anyway.
                    TlsChoice::Plaintext => {
                        log!("pg_keyspace worker: SIGHUP — reloaded auth ({n} credentials)");
                    }
                    TlsChoice::Refuse(why) => log!(
                        "pg_keyspace worker: SIGHUP — reloaded auth ({n} creds); TLS cert \
                         reload SKIPPED ({why}), keeping the current cert"
                    ),
                }
                return server::Tick::Reload(reload);
            }
            server::Tick::Continue
        },
        timeout_ms,
    );
    log!("pg_keyspace worker: shutting down");
}

/// The dedicated persistence worker: drains the ring and bulk-upserts into
/// `supacache.kv`. Runs in its own process with its own SPI connection, so the
/// RESP worker's event loop is never blocked by Postgres.
#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_persist_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let idx = unsafe { i32::from_datum(arg, false) }.unwrap_or(0).max(0) as usize;
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    // The RESP worker owns schema creation (once the extension is installed); all
    // persist workers wait for it. If the tables never appear — no CREATE EXTENSION
    // — exit cleanly and let the restart timer re-check, rather than erroring.
    let mut ready = false;
    for _ in 0..100 {
        if pg_table_ready() {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !ready {
        log!(
            "pg_keyspace persist {idx}: supacache tables not present (extension not \
             installed in '{dbname}'?); exiting, will re-check on restart"
        );
        return;
    }

    // Claim this shard before attaching to any ring. The rings are strictly
    // single-consumer, so a second worker draining the same shard would corrupt
    // them; the watchdog relaunches on a heartbeat, which is a guess about
    // liveness, and this is what makes a wrong guess harmless.
    let health = worker_count() + idx;
    if !health_claim(health) {
        log!(
            "pg_keyspace persist {idx}: shard {idx} is already being drained by a live              worker; exiting rather than sharing its rings"
        );
        return;
    }

    let rbase = RING_BASE.load(Ordering::Acquire);
    if rbase.is_null() {
        log!("pg_keyspace persist {idx}: ring not ready, exiting");
        health_release(health);
        return;
    }
    // This worker owns shard `idx` of every slot worker's ring set — one
    // consumer per ring, so the ring stays strictly SPSC while N slot workers
    // feed a single persistence process. Their keyspaces are disjoint (each slot
    // worker owns its own slot range), so records from different rings can share
    // one transaction without ever colliding on a key.
    let stride = ring_stride();
    let nworkers = worker_count();
    let consumers: Vec<ring::Consumer> = (0..nworkers)
        .map(|w| unsafe { ring::Consumer::attach(rbase.add(ring_index(w, idx) * stride)) })
        .collect();
    let idle = Duration::from_millis(GUC_PERSIST_WINDOW_MS.get().max(1) as u64);
    let sync_commit: &'static str = match ks_tier() {
        Tier::Durable => "on",
        Tier::Replicated => "remote_apply", // needs a synchronous standby
        _ => "off",                          // relaxed: RESP already acked
    };
    let replicated = matches!(ks_tier(), Tier::Replicated);
    log!(
        "pg_keyspace persist {idx}: draining shard {idx} of {nworkers} slot worker(s)          -> supacache.kv (synchronous_commit={sync_commit})"
    );

    // `per_ring` caps each ring's share of a batch so one hot slot worker
    // cannot starve the others.
    let per_ring = (20_000 / nworkers.max(1)).max(1_000);

    // Backoff after a failed batch. The records stay in their rings, so a retry
    // is safe and lossless; the delay stops a permanently broken batch (bad
    // permissions, disk full) from spinning the worker at full tilt.
    let mut backoff = Duration::from_millis(0);
    let mut last_watch = std::time::Instant::now();
    while !BackgroundWorker::sigterm_received() {
        health_beat(health);
        absorb_procsignal_barrier();
        if last_watch.elapsed() >= Duration::from_secs(5) {
            last_watch = std::time::Instant::now();
            health_watchdog();
        }
        let mut batch: Vec<server::PendingWrite> = Vec::with_capacity(8192);
        let slices = peek_rings(&consumers, per_ring, idx, &mut batch);
        let count: usize = slices.iter().map(|(n, _)| n).sum();
        if batch.is_empty() {
            std::thread::sleep(idle);
            continue;
        }
        // `synchronous_standby_names` is sighup context, so the promise the
        // replicated tier makes can be withdrawn at runtime by a reload. The
        // startup refusal cannot cover that, and committing anyway would ack
        // writes as replicated that Postgres never waited to replicate. Fail
        // closed: leave the batch in the rings, hold its acks, and say so.
        if replicated && !sync_standby_configured() {
            log!(
                "pg_keyspace persist {idx}: REFUSING to commit {count} record(s):                  durability is 'replicated' but synchronous_standby_names is now empty,                  so Postgres would not wait for any standby. Records retained in the                  rings and durable acks held until it is restored"
            );
            std::thread::sleep(Duration::from_secs(1));
            continue;
        }
        // Bracket the attempt on every ring: a Postgres ERROR unwinds out of
        // this worker rather than returning, so counting only on the Err branch
        // would miss the most common failure entirely.
        for c in &consumers {
            c.note_attempt();
        }
        match bulk_upsert(batch, sync_commit, ttl_bucket_us()) {
            Ok(()) => {
                // Only now are these records durable: release the ring space
                // and the durable acks waiting on them, for every ring that
                // contributed to this batch.
                for c in &consumers {
                    c.note_commit();
                }
                commit_rings(&consumers, &slices);
                // Outside the transaction bulk_upsert just committed, so the
                // pending entries are complete and publishable.
                flush_worker_stats();
                backoff = Duration::from_millis(0);
            }
            Err(e) => {
                // Nothing is committed, so no `head` moves and every record is
                // re-read next pass with its durable ack still held. This is
                // why the batch spans N rings safely: a failure cannot acknowledge
                // one ring's share while losing another's.
                log!(
                    "pg_keyspace persist {idx}: batch of {count} record(s) FAILED to                      persist ({e}); records retained in the rings, durable acks held,                      retrying in {backoff:?}"
                );
                backoff = (backoff + Duration::from_millis(50)).min(Duration::from_secs(5));
                std::thread::sleep(backoff);
            }
        }
    }
    // final drain on shutdown
    let mut tail: Vec<server::PendingWrite> = Vec::new();
    let tslices = peek_rings(&consumers, usize::MAX, idx, &mut tail);
    let tcount: usize = tslices.iter().map(|(n, _)| n).sum();
    if !tail.is_empty() {
        match bulk_upsert(tail, sync_commit, ttl_bucket_us()) {
            Ok(()) => {
                commit_rings(&consumers, &tslices);
                flush_worker_stats();
            }
            Err(e) => log!(
                "pg_keyspace persist {idx}: final batch of {tcount} record(s) FAILED                  to persist ({e}); they remain in the rings for the next start"
            ),
        }
    }
    // Deliberately no health_release here. A cluster shutdown and a
    // pg_terminate_backend deliver the same SIGTERM, so clearing the slot would
    // erase the one case this table exists to catch. Leaving the heartbeat to go
    // stale is correct for both: on a real shutdown the segment is destroyed
    // anyway, and a postmaster that is shutting down refuses new registrations.
    log!("pg_keyspace persist {idx}: shutting down");
}

/// Publish this worker's pending table statistics to shared memory.
///
/// A background worker never runs the backend main loop, which is what normally
/// calls `pgstat_report_stat` after each command. Without this the persist
/// worker's row counts accumulate in process-local pending entries and reach
/// `pg_stat_user_tables` only when the shutdown hook flushes them as the worker
/// exits — so a running cluster reports zero inserts, zero updates and zero dead
/// tuples on the supacache tables no matter how much it has written, and
/// autovacuum, whose thresholds are computed from exactly those counters, never
/// sees the churn. Restart such a cluster and every write appears at once.
///
/// Must be called outside a transaction: the pending entries are flushed at
/// transaction end and `pgstat_report_stat` asserts it is not inside one.
fn flush_worker_stats() {
    unsafe { pg_sys::pgstat_report_stat(true) };
}

/// Read from every ring this worker owns without consuming anything, appending
/// to `batch`. Returns, per ring, how many records were read and how many bytes
/// they occupy: what [`ring::Consumer::commit`] needs once they are durable.
///
/// A record whose kind carries [`ring::KIND_REF`] does not hold the value. Its
/// payload is the entry version at stage time, and the value is still in the
/// producing slot worker's keyspace segment, so it has to be resolved against
/// *that* worker's segment. `consumers[w]` is worker `w`'s ring, which is what
/// makes the index meaningful here. Resolving against worker 0's segment
/// instead, as a single-segment view would, finds either the wrong bytes or
/// nothing at all, and finding nothing is indistinguishable from a legitimately
/// superseded write: it would lose large durable writes in silence.
fn peek_rings(
    consumers: &[ring::Consumer],
    max: usize,
    idx: usize,
    batch: &mut Vec<server::PendingWrite>,
) -> Vec<(usize, u64)> {
    let mut slices = Vec::with_capacity(consumers.len());
    for (w, c) in consumers.iter().enumerate() {
        slices.push(c.peek(max, |k, v, e, kind| {
            if kind & ring::KIND_REF == 0 {
                batch.push((k.to_vec(), v.to_vec(), e, kind));
                return;
            }
            let version = if v.len() == 8 {
                u64::from_le_bytes([v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7]])
            } else {
                return;
            };
            match store_view_for_worker(w).and_then(|st| st.read_staged(k, version)) {
                Some((_, _, val)) => {
                    batch.push((k.to_vec(), val, e, kind & !ring::KIND_REF))
                }
                // Usually benign: the key was overwritten after staging, so a
                // newer record is queued behind this one. It is also what
                // resolving against the wrong segment looks like, which is not
                // benign, so say so rather than dropping in silence.
                None => {
                    c.note_unresolved();
                    log!(
                        "pg_keyspace persist {idx}: reference from worker {w} did not resolve (superseded, or not in that segment)"
                    )
                }
            }
        }));
    }
    slices
}

/// Release the records a preceding [`peek_rings`] read, ring by ring, once the
/// transaction carrying them has committed.
fn commit_rings(consumers: &[ring::Consumer], slices: &[(usize, u64)]) {
    for (c, (count, bytes)) in consumers.iter().zip(slices.iter()) {
        c.commit(*count, *bytes);
    }
}

/// verify `supatype_mask` is present in shared_preload_libraries AND
/// loaded after `pg_keyspace` (so it is the outermost planner hook). Returns
/// Err with a human-readable reason when the posture is wrong.
fn check_load_order() -> Result<(), String> {
    let spl = unsafe {
        let s = pg_sys::GetConfigOption(c"shared_preload_libraries".as_ptr(), true, false);
        if s.is_null() {
            String::new()
        } else {
            CStr::from_ptr(s).to_string_lossy().into_owned()
        }
    };
    let libs: Vec<&str> = spl.split(',').map(|s| s.trim()).collect();
    let ks = libs.iter().position(|&x| x == "pg_keyspace");
    let mask = libs.iter().position(|&x| x == "supatype_mask");
    let require_mask = GUC_REQUIRE_MASK.get();
    match (ks, mask) {
        (None, _) => Err(format!(
            "pg_keyspace not found in shared_preload_libraries ('{spl}')"
        )),
        // Standalone mode: mask not required. Absent is fine; if present, its
        // order is still enforced so a later flip to require_mask=on is safe.
        (_, None) if !require_mask => Ok(()),
        (_, None) => Err(format!(
            "supatype_mask not in shared_preload_libraries ('{spl}'); \
             RESP would serve rows the mask never rewrote. Set \
             pg_keyspace.require_mask=off to run standalone without it"
        )),
        (Some(k), Some(m)) if k >= m => Err(format!(
            "supatype_mask (pos {m}) must load AFTER pg_keyspace (pos {k}) so it is \
             outermost; fix the order in shared_preload_libraries ('{spl}')"
        )),
        _ => Ok(()),
    }
}

/// True if supacache.kv exists yet (created by the persistence worker).
fn pg_table_ready() -> bool {
    BackgroundWorker::transaction(|| {
        Spi::get_one::<bool>("SELECT to_regclass('supacache.kv') IS NOT NULL")
            .ok()
            .flatten()
            .unwrap_or(false)
    })
}

/// True if the `pg_keyspace` extension is installed in the connected database.
///
/// `CREATE EXTENSION` owns the `supacache` schema (and supplies the SQL surface).
/// The worker must not create any schema object before then: a free-standing
/// `supacache` schema makes the later `CREATE EXTENSION` fail with "schema
/// supacache is not a member of extension". So the worker gates all of its DDL —
/// schema, backing tables, recovery — on the extension being present, and runs
/// RESP-only (ephemeral) until it is.
fn extension_installed() -> bool {
    BackgroundWorker::transaction(|| {
        Spi::get_one::<bool>("SELECT count(*) > 0 FROM pg_extension WHERE extname = 'pg_keyspace'")
            .ok()
            .flatten()
            .unwrap_or(false)
    })
}

/// Bring the extension's SQL catalogue up to the running library (#136).
///
/// Postgres executes an extension's SQL exactly once, at CREATE EXTENSION. Take
/// a newer pg_keyspace.so and the catalogue does not move with it: functions and
/// views the library provides are simply absent, and -- worse, because it is
/// quiet -- a function whose result columns changed keeps describing the old
/// shape, so a call returns the old columns and raises nothing. Nothing in the
/// logs marks the difference. The operator most exposed to that is the one who
/// never learned there was a command to run, which is why this is on by default.
///
/// Deliberately cannot kill the worker. ALTER EXTENSION raises for reasons that
/// are not emergencies -- no update path between two versions, an upgrade script
/// that is absent from this install -- and a worker that dies on one would take
/// the keyspace out of service on a five-second relaunch loop (#130). The
/// statement runs inside a DO block with an exception handler, so a failure
/// becomes a WARNING in the server log and this function returns normally.
fn pg_catalogue_upgrade() {
    // The version skew is worth reporting either way, so the check runs even
    // when the repair does not.
    let (installed, available) = match BackgroundWorker::transaction(|| {
        (
            Spi::get_one::<String>(
                "SELECT extversion FROM pg_extension WHERE extname = 'pg_keyspace'",
            )
            .ok()
            .flatten(),
            Spi::get_one::<String>(
                "SELECT default_version FROM pg_available_extensions WHERE name = 'pg_keyspace'",
            )
            .ok()
            .flatten(),
        )
    }) {
        (Some(i), Some(a)) => (i, a),
        // No extension, or a library whose control file this cluster cannot see.
        // Neither is this function's business to report on.
        _ => return,
    };
    if installed == available {
        return;
    }

    if !GUC_AUTO_UPGRADE.get() {
        log!(
            "pg_keyspace worker: the installed extension is version {installed} but this \
             library ships {available}, and pg_keyspace.auto_upgrade is off. Objects added \
             since {installed} are missing, and a function whose columns changed reports the \
             old shape without raising. Run: ALTER EXTENSION pg_keyspace UPDATE"
        );
        return;
    }

    // A standby replays its catalogue from the primary. ALTER EXTENSION there
    // fails as a read-only transaction, and it would fail again at every start.
    if unsafe { pg_sys::RecoveryInProgress() } {
        log!(
            "pg_keyspace worker: the installed extension is version {installed} and this \
             library ships {available}, but this cluster is in recovery. The catalogue \
             follows the primary; upgrade there."
        );
        return;
    }

    BackgroundWorker::transaction(|| {
        let _ = Spi::run(
            "DO $ks_upgrade$ BEGIN \
               ALTER EXTENSION pg_keyspace UPDATE; \
             EXCEPTION WHEN OTHERS THEN \
               RAISE WARNING 'pg_keyspace: automatic catalogue upgrade failed: % (%). \
Run ALTER EXTENSION pg_keyspace UPDATE by hand to see the full error.', SQLERRM, SQLSTATE; \
             END $ks_upgrade$",
        );
    });

    // Reported from the catalogue rather than from having asked for it, so the
    // log says what is true and not what was attempted.
    let now = BackgroundWorker::transaction(|| {
        Spi::get_one::<String>("SELECT extversion FROM pg_extension WHERE extname = 'pg_keyspace'")
            .ok()
            .flatten()
            .unwrap_or_else(|| "unknown".to_string())
    });
    if now == available {
        log!("pg_keyspace worker: upgraded the extension catalogue {installed} -> {now}");
    } else {
        log!(
            "pg_keyspace worker: the extension catalogue is still {now} and this library \
             ships {available}; see the warning above. Objects added since {now} are \
             missing until ALTER EXTENSION pg_keyspace UPDATE succeeds."
        );
    }
}

/// Whether a relation exists, by name, without naming it in a statement that
/// would fail to parse if it does not (#111).
///
/// The stats views select from functions that read the worker-created tables,
/// and those tables appear later than the extension does. A guard has to be its
/// own statement: Postgres resolves relations at parse time, so
/// `CASE WHEN to_regclass(x) IS NULL THEN 0 ELSE (SELECT count(*) FROM x) END`
/// still errors on a missing `x`, unreachable branch or not.
fn table_exists(qualified: &str) -> bool {
    Spi::get_one_with_args::<bool>(
        "SELECT to_regclass($1) IS NOT NULL",
        vec![(PgBuiltInOids::TEXTOID.oid(), qualified.into_datum())],
    )
    .ok()
    .flatten()
    .unwrap_or(false)
}

/// The row-cache registration catalogue, as statements that can be run in ANY
/// database and by any number of callers at once (#120).
///
/// A registration is configuration, not cache content, and shared memory is the
/// wrong home for configuration: a segment reinitialisation (watchdog relaunch,
/// crash-restart, a terminated worker) took the pinned entry with it, and the
/// table then silently stopped being cached, with no error and a healthy-looking
/// coherence check (#103). This table is the source of truth; the pinned
/// shared-memory entry is a cache of it, reloaded whenever the segment turns out
/// to be empty. Keyed by name rather than by oid, so a dump/restore or a table
/// recreated by a migration keeps its registration; the oid is resolved afresh
/// on every reload.
///
/// Every statement is wrapped in a plpgsql `EXCEPTION` handler rather than left
/// as a bare `IF NOT EXISTS`, because `IF NOT EXISTS` is NOT race-free: two
/// sessions can both pass the existence check before either inserts its
/// catalogue row, and the loser raises `duplicate_schema`/`duplicate_table`.
/// That killed 44 persistence workers a minute under load in #130 -- a Postgres
/// ERROR inside SPI longjmps out and aborts the transaction, which pgrx surfaces
/// as a panic that takes the worker with it, so the discarded `Result` never
/// sees it. `let _ =` was never protection.
///
/// The handler opens an implicit subtransaction, so the duplicate is caught and
/// rolled back without touching the caller's transaction. Here the entrants are
/// real: registration runs in ordinary backends, and several of them can
/// register in a database that has no catalogue yet at the same moment.
fn rowcache_catalogue_ddl() -> Vec<String> {
    vec![
        // duplicate_object and unique_violation alongside duplicate_schema for the
        // same reason as the table below: a concurrent loser does not reliably
        // reach the check that raises the tidy, specific error.
        "DO $rc$ BEGIN CREATE SCHEMA supacache; \
         EXCEPTION WHEN duplicate_schema OR duplicate_object OR unique_violation \
         THEN NULL; END $rc$"
            .to_string(),
        // CREATE TABLE also creates the relation's composite type, and a racing
        // loser can fail at the TYPE rather than at the relation: `type
        // "rowcache_reg" already exists` is duplicate_object (42710), not
        // duplicate_table (42P07). Catching only the latter left the #130 hole
        // open in the one place this whole design most expects a crowd --
        // several backends registering in a fresh database while the pool probes
        // it. Measured: one escape in four storms of 8 databases x 20 racers.
        "DO $rc$ BEGIN \
           CREATE TABLE supacache.rowcache_reg (\
             tbl text PRIMARY KEY, attnums smallint[] NOT NULL, \
             registered_at timestamptz NOT NULL DEFAULT now()); \
         EXCEPTION WHEN duplicate_table OR duplicate_object OR unique_violation \
         THEN NULL; END $rc$"
            .to_string(),
        // #111: the stats functions read this over SPI, and SPI inside a
        // function runs as the CALLER -- the view owner's privileges do not
        // reach down into it -- so a pg_monitor member needs these directly.
        // Guarded like the rest: a concurrent GRANT of the same privilege is
        // harmless, but the role may not exist on an exotic install.
        "DO $rc$ BEGIN GRANT USAGE ON SCHEMA supacache TO pg_monitor; \
           GRANT SELECT ON supacache.rowcache_reg TO pg_monitor; \
         EXCEPTION WHEN undefined_object OR undefined_table THEN NULL; END $rc$"
            .to_string(),
    ]
}

/// Ensure this database has a row-cache registration catalogue.
///
/// Callable from an ordinary backend with SPI already connected -- which is
/// where it matters, because registration is the act that makes a database
/// participate, and until #120 the catalogue only ever existed in
/// `pg_keyspace.database` because only a worker there created it.
fn ensure_rowcache_catalogue() -> bool {
    for stmt in rowcache_catalogue_ddl() {
        if Spi::run(&stmt).is_err() {
            return false;
        }
    }
    table_exists("supacache.rowcache_reg")
}

/// Create the `supacache` schema and the hash-partitioned `supacache.kv`
/// backing table if absent. Idempotent; runs in one transaction.
fn pg_ensure_schema() {
    BackgroundWorker::transaction(|| {
        let _ = Spi::run("CREATE SCHEMA IF NOT EXISTS supacache");
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.kv (\
             tenant text NOT NULL DEFAULT '', key bytea NOT NULL, slot int NOT NULL, \
             kind \"char\" NOT NULL DEFAULT 's', val bytea, \
             expires_at bigint NOT NULL DEFAULT 0, version bigint NOT NULL DEFAULT 1, \
             PRIMARY KEY (tenant, key)) PARTITION BY HASH (tenant, key)",
        );
        for i in 0..8 {
            let _ = Spi::run(&format!(
                "CREATE TABLE IF NOT EXISTS supacache.kv_p{i} PARTITION OF supacache.kv \
                 FOR VALUES WITH (MODULUS 8, REMAINDER {i})"
            ));
        }
        // RESP credential -> role/tenant map and keyspace ACL.
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.resp_credential (\
             username text PRIMARY KEY, secret text NOT NULL, \
             role_name text NOT NULL, tenant text NOT NULL DEFAULT '')",
        );
        // Peers this instance relays to. Modelled on Citus's pg_dist_node: the
        // membership is a table an operator maintains, and the transport is
        // libpq, so authentication, TLS and pg_hba are Postgres's rather than
        // something this extension invents and has to be trusted about.
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.peer (\
             name text PRIMARY KEY, conninfo text NOT NULL, \
             enabled boolean NOT NULL DEFAULT true, \
             added_at timestamptz NOT NULL DEFAULT now())",
        );
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.acl (\
             role_name text NOT NULL, prefix text NOT NULL, \
             can_read boolean NOT NULL DEFAULT true, \
             can_write boolean NOT NULL DEFAULT true, \
             PRIMARY KEY (role_name, prefix))",
        );
        // TTL'd keys persist here, RANGE-partitioned by expiry time bucket,
        // so expiry is a whole-partition DROP (O(1), no vacuum churn) rather than
        // row-by-row DELETE. Partitions are created on demand by the persist
        // worker and dropped by the expiry worker.
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.kv_ttl (\
             tenant text NOT NULL DEFAULT '', key bytea NOT NULL, slot int NOT NULL, \
             kind \"char\" NOT NULL DEFAULT 's', val bytea, \
             expires_at bigint NOT NULL, bucket bigint NOT NULL, \
             PRIMARY KEY (bucket, tenant, key)) PARTITION BY RANGE (bucket)",
        );
        // A `kind` column lets a TTL'd aggregate (a hash/list/set/zset that gained
        // a TTL via EXPIRE, or a SET EX on any type) recover as the right type
        // instead of a raw string. Older clusters created kv_ttl without it.
        let _ = Spi::run(
            "ALTER TABLE supacache.kv_ttl ADD COLUMN IF NOT EXISTS kind \"char\" NOT NULL DEFAULT 's'",
        );
        // The worker layout the persisted keyspace was written under (#101).
        // supacache.kv.slot is stable, so a worker-count change never loses a
        // key -- but it does move most of them to a different worker's segment,
        // which drops that much of the warm cache and re-recovers it. Recording
        // the layout is what lets the change be reported instead of happening
        // silently.
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.topology (\
             id int PRIMARY KEY DEFAULT 1 CHECK (id = 1), workers int NOT NULL, \
             updated_at timestamptz NOT NULL DEFAULT now())",
        );
        // Row-cache registrations. Created here too, so a cluster that only
        // ever uses `pg_keyspace.database` has them at startup exactly as
        // before; the shared definition lives in `rowcache_catalogue_ddl`
        // because every other database has to be able to create the same thing
        // for itself (#120).
        for stmt in rowcache_catalogue_ddl() {
            let _ = Spi::run(&stmt);
        }
        // Crash recovery in a multi-worker cluster reads one contiguous slot
        // range per worker (see pg_recover), so index the column it ranges over.
        // Single-worker recovery scans unfiltered and ignores these.
        let _ = Spi::run("CREATE INDEX IF NOT EXISTS kv_slot_idx ON supacache.kv (slot)");
        let _ = Spi::run("CREATE INDEX IF NOT EXISTS kv_ttl_slot_idx ON supacache.kv_ttl (slot)");
        // #111: two of the stats functions read these tables over SPI, and SPI
        // inside a function runs as the CALLER -- the view owner's privileges
        // do not reach down into it. Without these grants,
        // supacache.pg_stat_keyspace_topology and _rowcache fail for a
        // pg_monitor member with "permission denied for table topology", while
        // the other eight views work, which is a confusing way to find out.
        //
        // They live here rather than in the extension's SQL because the worker
        // creates these tables, so at CREATE EXTENSION time there is nothing to
        // grant on. SELECT only, on two small configuration tables: which
        // relations are row-cached, and the worker count the keyspace was last
        // persisted under.
        let _ = Spi::run("GRANT USAGE ON SCHEMA supacache TO pg_monitor");
        let _ = Spi::run(
            "GRANT SELECT ON supacache.topology, supacache.rowcache_reg TO pg_monitor",
        );
    });
}

/// Ensure the TTL bucket partition for `bucket` exists (idempotent, and safe
/// against another persistence worker doing the same thing at the same time).
///
/// `CREATE TABLE IF NOT EXISTS` is NOT race-free: two sessions can both pass the
/// existence check before either inserts its catalogue row, and the loser raises
/// `duplicate_table`. Partitions are created on demand by whichever worker first
/// sees a write land in a new time bucket, so with `persist_workers > 1` every
/// bucket rollover is a race with that many entrants -- measured, four
/// persistence workers killed in a three-minute soak, one per bucket boundary
/// (#130).
///
/// `let _ =` was never protection. A Postgres ERROR inside SPI longjmps out and
/// aborts the transaction, which pgrx surfaces as a panic that takes the worker
/// down; the discarded `Result` never sees it. The watchdog relaunches ~2s
/// later, so nothing is lost -- the records are retained and retried -- but the
/// shard stops draining for that gap, acknowledged durable writes back up, and
/// clients see errors.
///
/// A plpgsql EXCEPTION handler opens an implicit subtransaction, so the
/// duplicate is caught and rolled back without touching the outer persist
/// transaction. One subtransaction per new bucket per worker -- once every
/// `ttl_bucket_secs` -- which is nothing. `invalid_object_definition` covers the
/// overlapping-bound form of the same race.
///
/// `duplicate_object` covers a third form, found in #120 on the row-cache
/// catalogue and the same here: creating a table also creates its composite
/// type, and a racing loser can fail at the TYPE rather than at the relation --
/// `type "kv_ttl_b123" already exists` is duplicate_object (42710), not
/// duplicate_table (42P07). Catching only the latter leaves exactly the escape
/// this function exists to prevent, just rarer than the one it was written for.
fn ensure_ttl_partition(client: &mut pgrx::spi::SpiClient, bucket: i64) {
    let _ = client.update(
        &format!(
            "DO $ttl$ BEGIN \
               CREATE TABLE supacache.kv_ttl_b{bucket} PARTITION OF supacache.kv_ttl \
                 FOR VALUES FROM ({bucket}) TO ({}); \
             EXCEPTION WHEN duplicate_table OR duplicate_object \
                          OR invalid_object_definition THEN NULL; \
             END $ttl$",
            bucket + 1
        ),
        None,
        None,
    );
}

/// self-check: Mode A backing tables must carry NO `supatype` security
/// label — access control for the keyspace is the ACL layer, not masking. If a
/// label was added by hand, refuse to serve (fail closed). Returns true if any
/// `supacache.*` relation has a `supatype` label.
fn kv_has_supatype_label() -> bool {
    BackgroundWorker::transaction(|| {
        Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_seclabel l \
             JOIN pg_class c ON c.oid = l.objoid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE l.provider = 'supatype' AND n.nspname = 'supacache'",
        )
        .ok()
        .flatten()
        .unwrap_or(0)
            > 0
    })
}

/// Load RESP credentials + keyspace ACL from SQL. Returns None when no
/// credentials are configured — the worker then runs in local/no-auth mode.
/// Exempt roles come from `supatype_mask.exempt_roles` so the two never drift.
fn load_auth_config() -> Option<AuthConfig> {
    use std::panic::AssertUnwindSafe;
    let exempt: HashSet<String> = {
        let raw = unsafe {
            let s = pg_sys::GetConfigOption(c"supatype_mask.exempt_roles".as_ptr(), true, false);
            if s.is_null() {
                String::new()
            } else {
                CStr::from_ptr(s).to_string_lossy().into_owned()
            }
        };
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let mut creds: HashMap<String, Cred> = HashMap::new();
        let mut acl: HashMap<String, Vec<AclRule>> = HashMap::new();
        let _ = Spi::connect(|client| {
            let t = client.select(
                "SELECT username, secret, role_name, tenant FROM supacache.resp_credential",
                None,
                None,
            )?;
            for row in t {
                let u: String = row.get::<String>(1)?.unwrap_or_default();
                let s: String = row.get::<String>(2)?.unwrap_or_default();
                let r: String = row.get::<String>(3)?.unwrap_or_default();
                let tn: String = row.get::<String>(4)?.unwrap_or_default();
                if !u.is_empty() {
                    creds.insert(u, Cred { secret: s, role: r, tenant: tn });
                }
            }
            let t = client.select(
                "SELECT role_name, prefix, can_read, can_write FROM supacache.acl",
                None,
                None,
            )?;
            for row in t {
                let r: String = row.get::<String>(1)?.unwrap_or_default();
                let p: String = row.get::<String>(2)?.unwrap_or_default();
                let cr: bool = row.get::<bool>(3)?.unwrap_or(false);
                let cw: bool = row.get::<bool>(4)?.unwrap_or(false);
                acl.entry(r).or_default().push(AclRule {
                    prefix: p.into_bytes(),
                    can_read: cr,
                    can_write: cw,
                });
            }
            Ok::<(), pgrx::spi::Error>(())
        });
        if creds.is_empty() {
            None
        } else {
            Some(AuthConfig { creds, acl, exempt })
        }
    }))
}

/// Rows per cursor fetch during recovery. The point is that worker memory stays
/// flat regardless of key count, so this bounds a batch at roughly this many
/// row widths. Deliberately modest rather than maximal: a row can carry a value
/// of several megabytes, and recovery is already about 3.5us per key, so the
/// extra round trips disappear into the noise while the memory ceiling does not.
const RECOVER_BATCH: i64 = 1_000;

/// How often recovery reports progress. A large keyspace takes minutes, and the
/// RESP port is not served until recovery finishes, so silence for the whole of
/// it is indistinguishable from a hang — which is how it has been read before.
const RECOVER_LOG_EVERY: i64 = 100_000;

/// Evictions summed across a store's partitions.
fn total_evictions(store: &Store) -> u64 {
    (0..store.num_partitions())
        .map(|p| store.stats(p).evictions)
        .sum()
}

/// Load live keys from `supacache.kv` into shmem at startup (crash recovery).
/// Expired rows are skipped. Returns the number of keys restored.
fn pg_recover(store: &Store, w: usize, nworkers: usize) -> i64 {
    use std::panic::AssertUnwindSafe;
    let now = store::now_micros();
    let t0 = std::time::Instant::now();
    // Recover only the slot range this worker serves. `supacache.kv.slot` is the
    // key's CRC16 slot, written on every persist, so a key comes back into the
    // same segment the RESP path and the SQL surface will look for it in. A
    // single-worker cluster owns everything, so it keeps the unfiltered scan.
    let (lo, hi) = crc16::slot_range(w, nworkers);
    let sharded = nworkers > 1;
    // A too-small keyspace announces itself here as eviction, so take the delta
    // rather than the absolute: a relaunched worker recovers into a segment that
    // may already carry its predecessor's counts.
    let evicted_before = total_evictions(store);
    // Worker 0 only: one record for the cluster, and one log line rather than
    // the same warning from every worker.
    if w == 0 {
        if let Some((was, moved)) = record_topology(nworkers) {
            let pct = moved as f64 * 100.0 / crc16::NUM_SLOTS as f64;
            log!(
                "pg_keyspace worker 0: WORKER COUNT CHANGED {was} -> {nworkers}. \
                 {moved} of {} slots ({pct:.1}%) now belong to a different worker, so that \
                 much of the persisted keyspace is recovering into a different segment. \
                 No data is lost (supacache.kv.slot is stable) but expect a cold cache \
                 for those keys",
                crc16::NUM_SLOTS
            );
        }
    }
    log!(
        "pg_keyspace worker {w}: recovering slots {lo}..{hi} from supacache.kv \
         (no RESP traffic is served until this finishes)"
    );
    let recovered = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            let mut cnt = 0i64;
            // no-TTL keys from kv, then non-expired TTL keys from kv_ttl (latest
            // expiry per key wins, so a re-SET into a newer bucket takes effect).
            // Through a cursor rather than one unbounded SELECT: SPI materialises
            // a whole result set in the worker, so the old form's peak memory
            // scaled with the persisted key count on top of the segment itself.
            // Fetching keeps the query plan and access path identical — only the
            // delivery changes.
            let mut cur = if sharded {
                client.try_open_cursor(
                    "SELECT key, val, expires_at, kind::text FROM supacache.kv \
                     WHERE slot >= $1 AND slot < $2",
                    Some(vec![
                        (PgOid::BuiltIn(PgBuiltInOids::INT4OID), (lo as i32).into_datum()),
                        (PgOid::BuiltIn(PgBuiltInOids::INT4OID), (hi as i32).into_datum()),
                    ]),
                )?
            } else {
                client.try_open_cursor(
                    "SELECT key, val, expires_at, kind::text FROM supacache.kv",
                    None,
                )?
            };
            loop {
                let tup = cur.fetch(RECOVER_BATCH as _)?;
                if tup.is_empty() {
                    break;
                }
                for row in tup {
                    let k: Option<Vec<u8>> = row.get(1)?;
                    let v: Option<Vec<u8>> = row.get(2)?;
                    let e: i64 = row.get::<i64>(3)?.unwrap_or(0);
                    let kind = row
                        .get::<String>(4)?
                        .and_then(|s| s.bytes().next())
                        .unwrap_or(b's') as u32;
                    if let (Some(k), Some(v)) = (k, v) {
                        if e > 0 && e <= now {
                            continue; // already expired
                        }
                        let ttl = if e > 0 { e - now } else { 0 };
                        store.set_typed(&k, &v, ttl, kind);
                        cnt += 1;
                        if cnt % RECOVER_LOG_EVERY == 0 {
                            log!(
                                "pg_keyspace worker {w}: recovery in progress, \
                                 {cnt} keys in {:?}",
                                t0.elapsed()
                            );
                        }
                    }
                }
            }
            // The DISTINCT ON sort happens server side either way; the cursor
            // only stops its whole output landing in the worker at once.
            let mut cur = if sharded {
                client.try_open_cursor(
                    "SELECT DISTINCT ON (key) key, val, expires_at, kind::text \
                     FROM supacache.kv_ttl \
                     WHERE expires_at > $1 AND slot >= $2 AND slot < $3 \
                     ORDER BY key, expires_at DESC",
                    Some(vec![
                        (PgOid::BuiltIn(PgBuiltInOids::INT8OID), now.into_datum()),
                        (PgOid::BuiltIn(PgBuiltInOids::INT4OID), (lo as i32).into_datum()),
                        (PgOid::BuiltIn(PgBuiltInOids::INT4OID), (hi as i32).into_datum()),
                    ]),
                )?
            } else {
                client.try_open_cursor(
                    "SELECT DISTINCT ON (key) key, val, expires_at, kind::text \
                     FROM supacache.kv_ttl \
                     WHERE expires_at > $1 ORDER BY key, expires_at DESC",
                    Some(vec![(
                        PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                        now.into_datum(),
                    )]),
                )?
            };
            loop {
                let tup = cur.fetch(RECOVER_BATCH as _)?;
                if tup.is_empty() {
                    break;
                }
                for row in tup {
                    let k: Option<Vec<u8>> = row.get(1)?;
                    let v: Option<Vec<u8>> = row.get(2)?;
                    let e: i64 = row.get::<i64>(3)?.unwrap_or(0);
                    let kind = row
                        .get::<String>(4)?
                        .and_then(|s| s.bytes().next())
                        .unwrap_or(b's') as u32;
                    if let (Some(k), Some(v)) = (k, v) {
                        store.set_typed(&k, &v, (e - now).max(1), kind);
                        cnt += 1;
                        if cnt % RECOVER_LOG_EVERY == 0 {
                            log!(
                                "pg_keyspace worker {w}: recovery in progress, \
                                 {cnt} keys in {:?}",
                                t0.elapsed()
                            );
                        }
                    }
                }
            }
            Ok::<i64, pgrx::spi::Error>(cnt)
        })
        .unwrap_or(0)
    }));
    // Recovery evicts as it loads when the persisted set does not fit, and the
    // cache then comes back quietly partial: every lookup still answers, just
    // some of them with a miss for a key that is durably stored. Say so.
    let evicted = total_evictions(store).saturating_sub(evicted_before);
    if evicted > 0 {
        warning!(
            "pg_keyspace worker {w}: recovery evicted {evicted} key(s) while loading — \
             the persisted set for slots {lo}..{hi} does not fit in pg_keyspace.keys \
             ({}), so the cache has come back partial",
            GUC_KEYS.get()
        );
    }
    recovered
}

/// Apply a drained batch in one transaction. Deduplicated by key
/// (last op wins), then split three ways: no-TTL upserts -> `supacache.kv`;
/// TTL'd upserts -> `supacache.kv_ttl` (range-partitioned by expiry bucket, so
/// expiry is a partition DROP); tombstones -> delete from both.
fn bulk_upsert(
    batch: Vec<server::PendingWrite>,
    sync_commit: &'static str,
    bucket_us: i64,
) -> Result<(), pgrx::spi::Error> {
    use std::collections::{HashMap, HashSet};
    if batch.is_empty() {
        return Ok(());
    }
    let mut latest: HashMap<Vec<u8>, (Vec<u8>, i64, u8)> = HashMap::with_capacity(batch.len());
    for (k, v, e, kind) in batch {
        latest.insert(k, (v, e, kind)); // last op for a key wins (SET then DEL -> DEL)
    }
    // no-TTL upserts -> kv (with the value's type tag; durable aggregates)
    let (mut keys, mut slots, mut vals, mut kinds) = (
        Vec::<Vec<u8>>::new(),
        Vec::<i32>::new(),
        Vec::<Vec<u8>>::new(),
        Vec::<String>::new(),
    );
    // TTL upserts -> kv_ttl
    let (mut tkeys, mut tslots, mut tkinds, mut tvals, mut texps, mut tbuckets) = (
        Vec::<Vec<u8>>::new(),
        Vec::<i32>::new(),
        Vec::<String>::new(),
        Vec::<Vec<u8>>::new(),
        Vec::<i64>::new(),
        Vec::<i64>::new(),
    );
    let mut del_keys: Vec<Vec<u8>> = Vec::new();
    let mut buckets_seen: HashSet<i64> = HashSet::new();
    for (k, (v, e, kind)) in latest {
        if e == server::DELETE_TOMBSTONE {
            del_keys.push(k);
        } else if e > 0 {
            // A TTL'd key of any type (SET EX, or EXPIRE on a hash/list/set/zset)
            // carries its kind so it recovers as the right type.
            let b = e / bucket_us;
            buckets_seen.insert(b);
            tslots.push(crc16::key_slot(&k) as i32);
            tkinds.push((kind as char).to_string());
            tvals.push(v);
            texps.push(e);
            tbuckets.push(b);
            tkeys.push(k);
        } else {
            slots.push(crc16::key_slot(&k) as i32);
            vals.push(v);
            kinds.push((kind as char).to_string());
            keys.push(k);
        }
    }
    // A key lives in exactly one table at a time, keyed on its *current* TTL
    // state, so recovery (which reads kv then kv_ttl) never resurrects a stale
    // row: a no-TTL write (SET, PERSIST) removes any prior kv_ttl row, and a
    // TTL'd write (SET EX, EXPIRE) removes any prior kv row.
    let clear_from_ttl = keys.clone();
    let clear_from_kv = tkeys.clone();

    BackgroundWorker::transaction(move || {
        Spi::connect(|mut client| {
            // Durability tier: relaxed=off (async, RESP already acked),
            // durable=on (fsync), replicated=remote_apply (needs a standby).
            //
            // Propagated, not discarded: if this fails the batch would commit
            // at the cluster default durability instead of the configured
            // tier, which is exactly the silent downgrade a durable ack must
            // never hide.
            client.update(
                &format!("SET LOCAL synchronous_commit = '{sync_commit}'"),
                None,
                None,
            )?;
            if !keys.is_empty() {
                let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), keys.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID), slots.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), vals.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID), kinds.into_datum()),
                ];
                client.update(
                    "INSERT INTO supacache.kv (tenant,key,slot,kind,val,expires_at,version) \
                     SELECT '', k, s, ki::\"char\", v, 0, 1 \
                     FROM unnest($1::bytea[], $2::int[], $3::bytea[], $4::text[]) AS t(k, s, v, ki) \
                     ON CONFLICT (tenant,key) DO UPDATE SET \
                     val=EXCLUDED.val, kind=EXCLUDED.kind, expires_at=0, slot=EXCLUDED.slot, \
                     version=supacache.kv.version+1",
                    None,
                    Some(args),
                )?;
            }
            if !tkeys.is_empty() {
                for b in &buckets_seen {
                    ensure_ttl_partition(&mut client, *b);
                }
                let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), tkeys.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID), tslots.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::TEXTARRAYOID), tkinds.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), tvals.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID), texps.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID), tbuckets.into_datum()),
                ];
                client.update(
                    "INSERT INTO supacache.kv_ttl (tenant,key,slot,kind,val,expires_at,bucket) \
                     SELECT '', k, s, ki::\"char\", v, e, b \
                     FROM unnest($1::bytea[], $2::int[], $3::text[], $4::bytea[], $5::bigint[], $6::bigint[]) \
                          AS t(k, s, ki, v, e, b) \
                     ON CONFLICT (bucket,tenant,key) DO UPDATE SET \
                     kind=EXCLUDED.kind, val=EXCLUDED.val, expires_at=EXCLUDED.expires_at, slot=EXCLUDED.slot",
                    None,
                    Some(args),
                )?;
            }
            // Cross-table cleanup so each key sits in exactly one table for its
            // current TTL state (see note above): drop the old opposite-table row.
            if !clear_from_ttl.is_empty() {
                client.update(
                    "DELETE FROM supacache.kv_ttl WHERE tenant='' AND key = ANY($1::bytea[])",
                    None,
                    Some(vec![(
                        PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID),
                        clear_from_ttl.into_datum(),
                    )]),
                )?;
            }
            if !clear_from_kv.is_empty() {
                client.update(
                    "DELETE FROM supacache.kv WHERE tenant='' AND key = ANY($1::bytea[])",
                    None,
                    Some(vec![(
                        PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID),
                        clear_from_kv.into_datum(),
                    )]),
                )?;
            }
            if !del_keys.is_empty() {
                let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![(
                    PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID),
                    del_keys.into_datum(),
                )];
                client.update(
                    "DELETE FROM supacache.kv WHERE tenant='' AND key = ANY($1::bytea[])",
                    None,
                    Some(args.clone()),
                )?;
                client.update(
                    "DELETE FROM supacache.kv_ttl WHERE tenant='' AND key = ANY($1::bytea[])",
                    None,
                    Some(args),
                )?;
            }
            Ok::<(), pgrx::spi::Error>(())
        })
    })
}

/// Comma-separated globs from `pg_keyspace.relay_channels`, trimmed and
/// non-empty. Empty result means relaying is off.
fn relay_patterns() -> Vec<String> {
    GUC_RELAY_CHANNELS
        .get()
        .and_then(|c| c.to_str().ok().map(|s| s.to_string()))
        .unwrap_or_default()
        .split(',')
        .map(|p| p.trim().to_string())
        .filter(|p| !p.is_empty())
        .collect()
}

/// One RESP array, encoded for the wire.
fn resp_cmd(parts: &[&[u8]]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("*{}\r\n", parts.len()).as_bytes());
    for p in parts {
        out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
        out.extend_from_slice(p);
        out.extend_from_slice(b"\r\n");
    }
    out
}

/// The cross-instance relay: a RESP client that forwards what it hears.
///
/// It subscribes to `pg_keyspace.relay_channels` on this instance's own RESP
/// port and calls `supacache.publish_relayed()` on each enabled peer.
///
/// A RESP client rather than a bus participant, and that is the whole design.
/// Giving the relay its own inbox would have meant sizing the pub/sub bus for
/// `nworkers + 1`, which adds `2n+1` rings to hand ONE participant a mailbox --
/// quadratic growth (+768 KB at 1 worker, +4.25 MB at 8) paid for by every
/// deployment whether or not it relays. As a subscriber it needs no shared
/// memory at all: the fan-out, the tenant scoping and the wakeups are the ones
/// that already exist, and relaying off costs exactly nothing.
///
/// Delivery is AT-MOST-ONCE, matching valkey: "if the subscriber is unable to
/// handle the message (for example, due to an error or a network disconnect)
/// the message is forever lost". A message published while a peer is down is
/// not replayed, and this worker deliberately has no catch-up: valkey's answer
/// for a client that cannot miss an invalidation is for that client to ping its
/// invalidation channel and flush its cache when the channel breaks, which is
/// unchanged by whether a relay exists. Flushing peers' caches on every relay
/// blip would be worse than the problem.
#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_relay_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    let patterns = relay_patterns();
    log!(
        "pg_keyspace relay: forwarding {} channel pattern(s) to peers in supacache.peer",
        patterns.len()
    );

    let mut relayed: u64 = 0;
    let mut failed: u64 = 0;
    let mut last_report = std::time::Instant::now();
    // The subscription lives across iterations. The first version rebuilt the
    // connection every pass and treated a read timeout as a disconnect, so the
    // relay held a subscription for a few milliseconds at a time and PUBSUB
    // NUMPAT on its own instance read 0 -- it was never subscribed when anything
    // was published.
    let mut sock: Option<std::net::TcpStream> = None;
    let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);

    while !BackgroundWorker::sigterm_received() {
        if sock.is_none() {
            sock = relay_connect(&patterns);
            if sock.is_none() {
                // Nothing to subscribe to yet (the worker may be up before the
                // RESP port is). Back off rather than spin.
                BackgroundWorker::wait_latch(Some(Duration::from_secs(1)));
                continue;
            }
            buf.clear();
            log!("pg_keyspace relay: subscribed to {} pattern(s)", patterns.len());
        }
        // One read, then whatever complete frames it completed. A timeout is the
        // idle case and keeps the connection; only EOF or an error drops it, and
        // losing the subscription is expected -- exceeding the output-buffer
        // limit is how a relay that cannot keep up is shed -- so it reconnects.
        if !relay_read_once(sock.as_mut().unwrap(), &mut buf, &mut relayed, &mut failed) {
            log!("pg_keyspace relay: subscription dropped, reconnecting");
            sock = None;
        }
        if last_report.elapsed() >= Duration::from_secs(60) {
            log!("pg_keyspace relay: {relayed} message(s) forwarded, {failed} peer call(s) failed");
            last_report = std::time::Instant::now();
        }
    }
    log!("pg_keyspace relay: exiting ({relayed} forwarded, {failed} failed)");
}

/// Connect to this instance's RESP port, AUTH if configured, and PSUBSCRIBE.
fn relay_connect(patterns: &[String]) -> Option<std::net::TcpStream> {
    use std::io::Write;
    let port = GUC_PORT.get() as u16;
    let sock = std::net::TcpStream::connect(("127.0.0.1", port)).ok()?;
    sock.set_read_timeout(Some(Duration::from_millis(500))).ok()?;
    let mut w = &sock;
    let user = GUC_RELAY_USER.get().and_then(|c| c.to_str().ok().map(String::from));
    if let Some(u) = user.filter(|u| !u.is_empty()) {
        let secret = GUC_RELAY_SECRET
            .get()
            .and_then(|c| c.to_str().ok().map(String::from))
            .unwrap_or_default();
        w.write_all(&resp_cmd(&[b"AUTH", u.as_bytes(), secret.as_bytes()])).ok()?;
    }
    let mut cmd: Vec<&[u8]> = vec![b"PSUBSCRIBE"];
    for p in patterns {
        cmd.push(p.as_bytes());
    }
    w.write_all(&resp_cmd(&cmd)).ok()?;
    w.flush().ok()?;
    Some(sock)
}

/// One read from the subscription, then every complete frame it completed.
///
/// Returns false when the connection is gone and should be rebuilt. A read
/// TIMEOUT is not that: it is the idle case, and returning on it was the bug
/// that kept this relay unsubscribed.
fn relay_read_once(
    sock: &mut std::net::TcpStream,
    buf: &mut Vec<u8>,
    relayed: &mut u64,
    failed: &mut u64,
) -> bool {
    use std::io::Read;
    let mut chunk = [0u8; 16 * 1024];
    match sock.read(&mut chunk) {
        Ok(0) => return false, // peer closed
        Ok(n) => buf.extend_from_slice(&chunk[..n]),
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
            ) =>
        {
            return true // idle; keep the subscription
        }
        Err(_) => return false,
    }
    let mut args: Vec<(usize, usize)> = Vec::new();
    loop {
        match resp::parse(buf, &mut args, 512 * 1024 * 1024) {
            resp::Parse::Complete { consumed } => {
                // pmessage | pattern | channel | payload
                if args.len() == 4 && &buf[args[0].0..args[0].1] == b"pmessage" {
                    let channel = buf[args[2].0..args[2].1].to_vec();
                    let payload = buf[args[3].0..args[3].1].to_vec();
                    match relay_to_peers(&channel, &payload) {
                        Ok(n) => *relayed += n,
                        Err(()) => *failed += 1,
                    }
                }
                buf.drain(..consumed);
            }
            resp::Parse::Incomplete => return true,
            resp::Parse::Error => return false,
        }
    }
}

/// Call `supacache.publish_relayed()` on every enabled peer, over dblink.
///
/// dblink rather than a socket of our own: it is libpq, so authentication, TLS
/// and pg_hba are Postgres's, and the peer needs no listener this extension
/// invented. The call is synchronous, which is safe HERE and nowhere else --
/// this is the relay's own process, so a slow peer delays only relaying. Doing
/// it inline on the RESP path would have put a dead peer on the event loop.
///
/// `connect_timeout` bounds how long a dead peer can hold this worker, and the
/// channel is passed through already tenant-scoped: `publish_relayed` applies
/// no scope of its own and never relays onward, so nothing can loop.
fn relay_to_peers(channel: &[u8], payload: &[u8]) -> Result<u64, ()> {
    let ch = String::from_utf8_lossy(channel).replace('\'', "''");
    let hex = payload.iter().map(|b| format!("{b:02x}")).collect::<String>();
    let sql = format!(
        "SELECT sum(t.n)::bigint FROM supacache.peer p, \
         LATERAL dblink(p.conninfo || ' connect_timeout=2', \
           'SELECT supacache.publish_relayed(''{ch}'', ''\\x{hex}''::bytea)') AS t(n bigint) \
         WHERE p.enabled"
    );
    match Spi::get_one::<i64>(&sql) {
        Ok(Some(n)) => Ok(n.max(0) as u64),
        Ok(None) => Ok(0), // no enabled peers
        Err(_) => Err(()),
    }
}

/// The expiry worker: periodically DROP TTL partitions whose whole
/// time bucket is in the past. This is O(1) DDL per partition — no row-by-row
/// DELETE, no vacuum churn. Shmem expiry stays lazy-on-read.
#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_expiry_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    let mut ready = false;
    for _ in 0..100 {
        if pg_table_ready() {
            ready = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !ready {
        log!(
            "pg_keyspace expiry: supacache tables not present (extension not installed \
             in '{dbname}'?); exiting, will re-check on restart"
        );
        return;
    }
    let sweep = Duration::from_secs(GUC_TTL_SWEEP_SECS.get().max(1) as u64);
    log!("pg_keyspace expiry: dropping past TTL partitions every {sweep:?}");
    let expiry_slot = health_expiry_slot();
    if !health_claim(expiry_slot) {
        log!("pg_keyspace expiry: another expiry worker is live; exiting");
        return;
    }
    while !BackgroundWorker::sigterm_received() {
        health_beat(expiry_slot);
        absorb_procsignal_barrier();
        health_watchdog();
        let now_bucket = store::now_micros() / ttl_bucket_us();
        let dropped = drop_expired_partitions(now_bucket);
        if dropped > 0 {
            log!("pg_keyspace expiry: dropped {dropped} expired TTL partition(s)");
        }
        std::thread::sleep(sweep);
    }
    log!("pg_keyspace expiry: shutting down");
}

// ==== Mode B: keys-only logical-decoding invalidation worker =========
//
// The row cache holds RAW pre-policy tuples, so it MUST be dropped the instant
// the underlying row changes, or a stale row would be served (masking is still
// re-applied above, but the *data* would be wrong). We learn what changed from a
// logical replication slot whose output plugin (`supacache_keys`) emits ONLY
// `<I|U|D> <relid> <pk>` — never a column value. So this worker cannot store WAL
// values even in principle ("decoding worker stores WAL values"): the values
// never leave the plugin. Changed keys are dropped from the cache; a deleted key
// stays dropped, and with pg_keyspace.rowcache_refill a still-hot key is re-read
// and re-cached. Either way the next read is correct.

/// Parse one `supacache_keys` line: `<action> <relid> <hexpk>` -> (action,
/// relid, canonical pk bytes). action is 'I' (insert), 'U' (update) or 'D'
/// (delete); the pk is hex of the type's output-function text (see `rc_key`).
/// `ACTION RELID HEXPART [HEXPART ...]` -- one hex token per primary-key column,
/// in ascending attnum order, which is the order the plugin emits them and the
/// order the registration stores its attnums in.
fn parse_change(line: &str) -> Option<(char, u32, Vec<u8>)> {
    let mut it = line.split_whitespace();
    let action = it.next()?.chars().next()?;
    let relid: u32 = it.next()?.parse().ok()?;
    let mut parts = Vec::new();
    for tok in it {
        parts.push(hex_decode(tok)?);
    }
    if parts.is_empty() {
        return None;
    }
    Some((action, relid, compose_pk(&parts)))
}

/// The configured stem every row-cache decode slot name is built from.
fn rowcache_slot_base() -> String {
    GUC_ROWCACHE_SLOT
        .get()
        .and_then(|c| c.to_str().ok().map(|s| s.to_string()))
        .unwrap_or_else(|| "supacache_rowcache".to_string())
}

/// Create the keys-only replication slot if it does not exist yet. Requires
/// `wal_level = logical`; returns Err with the reason otherwise.
fn ensure_decode_slot(slot: &str) -> Result<(), String> {
    use std::panic::AssertUnwindSafe;
    // Check in its own transaction and let it commit first: pg_create_logical_
    // replication_slot refuses to run once the transaction has been assigned an
    // xid, so the create must be the sole statement of a fresh transaction.
    let exists = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
            vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())],
        )
        .ok()
        .flatten()
        .unwrap_or(false)
    }));
    if exists {
        return Ok(());
    }
    // Create under an advisory lock, re-checking existence while holding it.
    //
    // `IF NOT EXISTS` has no equivalent here and the check above is not a
    // guard: two workers can both pass it before either creates, and the loser
    // gets `duplicate_object` -- the #130 shape, where a Postgres ERROR inside
    // SPI longjmps out and takes the worker with it, with the discarded
    // `Result` never seeing it. A pool of invalidation workers probing the same
    // database at once (#120) makes that a race with real entrants rather than
    // a theoretical one.
    //
    // An advisory lock rather than the plpgsql `EXCEPTION WHEN duplicate_object`
    // handler used for TTL partitions, because this whole call has to run
    // through the READ-ONLY SPI path -- pg_create_logical_replication_slot
    // refuses once the transaction has an xid and the read-write path assigns
    // one -- and a `DO` block is a utility statement that read-only SPI will not
    // run. Advisory locks are not heap writes, so they assign no xid either, and
    // the check and the create can finally sit in one transaction.
    // Capacity is checked BEFORE attempting, not caught afterwards.
    //
    // `pg_create_logical_replication_slot` answers exhaustion with a Postgres
    // ERROR, and an ERROR inside SPI longjmps out and aborts the transaction,
    // which pgrx surfaces as a panic that takes the worker with it -- the
    // `map_err` below would never see it (#130). The worker would then die, be
    // relaunched a second later, and die again, for as long as the cluster was
    // short of slots: a crash loop in place of the one clear line an operator
    // needs.
    //
    // Slots are a cluster-wide resource and this design needs one per
    // participating database, so running out is an ordinary misconfiguration
    // rather than an exotic failure. Asking first turns it into a returned
    // error, a log line naming the database, and a cache that fails closed.
    // ONE lock for all row-cache slot creation, not one per slot name, and the
    // capacity check INSIDE it.
    //
    // Keying the lock on the slot name only serialises creators of the SAME
    // slot, which is the wrong race. Slots are a CLUSTER-wide resource: the
    // contention that matters is several databases -- different slot names, so
    // different locks -- reaching for the last free slot at once. Each passes a
    // capacity check taken outside any lock, one wins, and the losers get
    // `all replication slots are in use` from
    // pg_create_logical_replication_slot. That is a Postgres ERROR inside SPI:
    // it longjmps out, the `map_err` below never sees it (#130), and the worker
    // dies. Checking first turned the crash LOOP into a single death per
    // starved database; it could not remove the death, because a check outside
    // the lock is not a guard.
    //
    // Holding one lock across check-capacity/check-exists/create makes the
    // losers observe the full cluster and return the operator-facing error
    // instead of raising. Slot creation happens once per database lifetime, so
    // serialising it cluster-wide costs nothing.
    //
    // This does not make exhaustion impossible -- a slot can still be taken by
    // something outside pg_keyspace between the check and the create -- but it
    // removes the self-inflicted race, which is the one this design creates by
    // needing a slot per database.
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let outcome: Result<Result<(), String>, pgrx::spi::Error> = Spi::connect(|client| {
            client.select(
                "SELECT pg_advisory_xact_lock($1, $2)",
                None,
                Some(vec![
                    (PgBuiltInOids::INT4OID.oid(), SLOT_ADVISORY_NS.into_datum()),
                    (PgBuiltInOids::INT4OID.oid(), SLOT_ADVISORY_KEY.into_datum()),
                ]),
            )?;
            let taken = client
                .select(
                    "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)",
                    None,
                    Some(vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())]),
                )?
                .first()
                .get::<bool>(1)
                .ok()
                .flatten()
                .unwrap_or(false);
            if taken {
                return Ok(Ok(()));
            }
            let room = client
                .select(
                    "SELECT (SELECT count(*) FROM pg_replication_slots) \
                          < current_setting('max_replication_slots')::int",
                    None,
                    None,
                )?
                .first()
                .get::<bool>(1)
                .ok()
                .flatten()
                .unwrap_or(true); // a failed read must not block slot creation
            if !room {
                return Ok(Err("every replication slot is in use; raise \
                     max_replication_slots. The row cache needs one slot per database \
                     that registers a table, on top of whatever replication this \
                     cluster already does"
                    .to_string()));
            }
            client.select(
                "SELECT pg_create_logical_replication_slot($1, 'supacache_keys')",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())]),
            )?;
            Ok(Ok(()))
        });
        match outcome {
            Ok(inner) => inner,
            Err(e) => Err(e.to_string()),
        }
    }))
}

/// Advisory-lock namespace for row-cache slot creation. Arbitrary, but fixed:
/// `pg_advisory_xact_lock(ns, key)` partitions the advisory space by its first
/// argument, so this only has to not collide with another caller's choice.
const SLOT_ADVISORY_NS: i32 = 0x7073_6b73; // "pgks"

/// The single key within that namespace. Fixed rather than derived from the
/// slot name, because slots are a cluster-wide resource and the race worth
/// serialising is between DIFFERENT databases competing for the last one.
const SLOT_ADVISORY_KEY: i32 = 1;

/// How much WAL one drain pass consumes at most.
///
/// A pass decodes a bounded *window* of WAL rather than a bounded number of
/// changes, because the slot is advanced to the end of the window and that
/// only works if the window is an LSN the caller chose. Bounded at all because
/// the batch is materialised in worker memory, and a backlog (the worker down
/// for a while, or one bulk UPDATE) is otherwise unbounded.
///
/// A transaction larger than this window is not a problem: the window simply
/// does not reach its commit, nothing of it is decoded, the slot advances to
/// the last commit that fits, and the next pass extends the window. Progress is
/// guaranteed either way.
const DRAIN_MAX_WAL_BYTES: u64 = 64 * 1024 * 1024;

/// Parse a `X/Y` LSN into the 64-bit position Postgres stores it as.
fn lsn_parse(s: &str) -> Option<u64> {
    let (hi, lo) = s.split_once('/')?;
    let hi = u64::from_str_radix(hi.trim(), 16).ok()?;
    let lo = u64::from_str_radix(lo.trim(), 16).ok()?;
    Some((hi << 32) | lo)
}

/// Render a 64-bit position back into Postgres's `X/Y` LSN text.
fn lsn_text(v: u64) -> String {
    format!("{:X}/{:X}", v >> 32, v & 0xFFFF_FFFF)
}

/// What one drain pass did.
#[derive(Default)]
struct Drain {
    /// Cache entries touched (dropped or refilled).
    reconciled: u64,
    /// Records the plugin emitted that `parse_change` could not read.
    unparsed: u64,
    /// The pass stopped at its WAL window rather than at the end of the log, so
    /// more is already pending and the caller should come straight back instead
    /// of sleeping.
    full: bool,
    /// The pass actually read the slot, as opposed to giving up before it got
    /// there (no segment mapped, no slot row, an LSN that would not parse).
    ///
    /// This is what the per-database coherence heartbeat is recorded on (#120),
    /// and the distinction is the point: "a worker ran" and "this database's
    /// changes have been consumed up to now" are different claims, and only the
    /// second one makes a cached row safe to serve. Recording the first as if
    /// it were the second is how a cache reports itself healthy while nothing
    /// is invalidating it.
    reached: bool,
}

/// Drain pending changes from the slot and apply them to the row cache.
///
/// Three phases, and the order is the point. Phase 1 *peeks* the change list in
/// one transaction; phase 2 applies each change (a transaction per refill, so
/// that SPI does not nest inside the peek's); phase 3 advances the slot past the
/// batch phase 2 just applied.
///
/// Peek-apply-advance rather than get-apply because `pg_logical_slot_get_changes`
/// *consumes*: it advances the slot when the calling transaction commits, which
/// is before a single change has been applied. A worker that died in that window
/// lost those invalidations permanently and then served the stale rows forever,
/// with nothing logged and nothing to notice — it came back healthy, just behind
/// reality (#66).
///
/// Delivery is therefore at-least-once, which is safe here because both terminal
/// actions are idempotent: dropping an absent key is a no-op, and a refill
/// re-reads whatever the heap holds now. Replaying a batch converges on the same
/// cache state as applying it once.
///
/// The deliberate trade: a change that cannot be applied at all now blocks the
/// channel — the slot stops advancing and WAL accumulates — instead of being
/// consumed and forgotten. A stalled slot is loud, bounded by
/// `max_slot_wal_keep_size` and recoverable; a cache that quietly disagrees with
/// the heap is none of those things.
///
/// Only *hot* keys (currently cached) are touched — a change to an uncached row
/// is ignored, so the cache never fills with cold rows.
fn drain_invalidations(slot: &str) -> Drain {
    use std::panic::AssertUnwindSafe;
    let view = match rowcache_view() {
        Some(v) => v,
        None => return Drain::default(),
    };
    // Phase 1: pick the window, then peek it. Non-destructive, so the slot
    // stays put until phase 3.
    //
    // The window is chosen FIRST and everything after is expressed in terms of
    // it, because the slot has to be advanced to an LSN this code names. The
    // obvious alternative -- decode a batch and advance to the last change's
    // LSN -- does not work, and failed silently: a change's LSN lies before its
    // own transaction's commit record, and `pg_replication_slot_advance` only
    // ever moves `confirmed_flush_lsn` to a commit it has passed. Advancing to
    // the last change of the last transaction therefore left the slot exactly
    // where it was. Nothing errored. The same changes came back on the next
    // poll, and the next, invalidating anything the cache had warmed in between
    // -- so with decode on, the row cache could never hold an entry for longer
    // than one poll interval, and the slot pinned WAL forever.
    //
    // `lsn::text` throughout because pg_lsn has no pgrx datum mapping, and the
    // arithmetic (`lsn_parse`/`lsn_text`) is done here rather than in SQL
    // because `pg_lsn + numeric` only exists from PostgreSQL 15.
    let window = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            let t = client.select(
                "SELECT confirmed_flush_lsn::text, restart_lsn::text, \
                 pg_current_wal_lsn()::text FROM pg_replication_slots WHERE slot_name = $1",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())]),
            )?;
            let mut out = None;
            for row in t {
                let confirmed = row.get::<String>(1)?.or(row.get::<String>(2)?);
                let current = row.get::<String>(3)?;
                out = confirmed.zip(current);
            }
            Ok::<_, pgrx::spi::Error>(out)
        })
        .ok()
        .flatten()
    }));
    let (from, current) = match window.as_ref().and_then(|(c, n)| {
        lsn_parse(c).zip(lsn_parse(n))
    }) {
        Some(v) => v,
        // No slot row, or an LSN that did not parse: nothing safe to advance
        // to, so do nothing this pass rather than guess.
        None => return Drain::default(),
    };
    // Cap the pass at a window of WAL rather than at a number of changes, so the
    // point the slot is advanced to is one this code picked and can name.
    let target = current.min(from.saturating_add(DRAIN_MAX_WAL_BYTES));
    let target_text = lsn_text(target);

    let (changes, unparsed) = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let mut out: Vec<(char, u32, Vec<u8>)> = Vec::new();
        let mut bad = 0u64;
        let _ = Spi::connect(|client| {
            let t = client.select(
                "SELECT data FROM pg_logical_slot_peek_changes($1, $2::pg_lsn, NULL)",
                None,
                Some(vec![
                    (PgBuiltInOids::TEXTOID.oid(), slot.into_datum()),
                    (PgBuiltInOids::TEXTOID.oid(), target_text.clone().into_datum()),
                ]),
            )?;
            for row in t {
                let data: String = row.get::<String>(1)?.unwrap_or_default();
                match parse_change(&data) {
                    Some(c) => out.push(c),
                    // Counted and logged rather than dropped in silence: a
                    // plugin/format mismatch would otherwise degrade the
                    // cache with no symptom but wrong answers. The slot still
                    // advances past it -- a record that will never parse would
                    // otherwise stall the channel permanently.
                    None => bad += 1,
                }
            }
            Ok::<(), pgrx::spi::Error>(())
        });
        (out, bad)
    }));

    // Phase 2: apply. Drop-only unless refill is enabled and the row still
    // exists. A failure here propagates and takes the worker with it, which is
    // the safe direction: the slot has not moved, so the batch replays.
    let refill = GUC_ROWCACHE_REFILL.get();
    let mut reconciled = 0u64;
    for (action, relid, pk) in changes {
        let key = rc_key(relid, &pk);
        if !matches!(view.get(&key), Lookup::Hit(_)) {
            continue; // cold key — nothing cached to keep coherent
        }
        if refill && action != 'D' {
            let pk = pk.clone();
            let outcome = BackgroundWorker::transaction(AssertUnwindSafe(|| unsafe {
                rowcache_refill_locked(pg_sys::Oid::from(relid), &pk)
            }));
            if !matches!(outcome, Refill::Stored) {
                // The worker is the only process that deletes, but it shares the
                // segment with every backend that writes, so it takes the same
                // lock; a lock one writer skips protects nothing (#127).
                rowcache_write(&view, &key, || view.del(&key), false); // gone/unresolvable -> invalidate
            }
        } else {
            rowcache_write(&view, &key, || view.del(&key), false);
        }
        reconciled += 1;
    }

    // Phase 3: everything committed at or before the window's end has been
    // applied, so it is finally safe to consume up to it. `advance` stops at the
    // last commit within the window, which is exactly what has been applied --
    // it cannot skip a transaction whose changes this pass did not see.
    advance_decode_slot(slot, &target_text);
    Drain {
        reconciled,
        unparsed,
        full: target < current,
        reached: true,
    }
}

/// Consume everything up to `upto` on the decode slot, releasing its WAL.
///
/// Run through the READ-ONLY SPI path for the same reason slot creation is: this
/// is not a heap write, and the read-write path would assign the transaction an
/// xid. A failure is logged and otherwise ignored — the slot simply stays where
/// it is and the batch replays harmlessly on the next pass — but it is worth
/// logging, because a slot that never advances retains WAL until
/// `max_slot_wal_keep_size` invalidates it.
fn advance_decode_slot(slot: &str, upto: &str) {
    use std::panic::AssertUnwindSafe;
    let res: Result<(), String> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT pg_replication_slot_advance($1, $2::pg_lsn)",
                    None,
                    Some(vec![
                        (PgBuiltInOids::TEXTOID.oid(), slot.into_datum()),
                        (PgBuiltInOids::TEXTOID.oid(), upto.into_datum()),
                    ]),
                )
                .map(|_| ())
        })
        .map_err(|e| e.to_string())
    }));
    if let Err(why) = res {
        log!("pg_keyspace invalidation: cannot advance slot '{slot}' to {upto}: {why}");
    }
}

/// Drop the pre-#120 slot named for the configuration rather than the database.
///
/// Before slots were per-database there was exactly one, named
/// `pg_keyspace.rowcache_slot` verbatim. After an upgrade nothing consumes it,
/// and an unconsumed slot pins WAL from its `restart_lsn` **forever** — the
/// single worst operational failure this subsystem has, and it would arrive
/// silently, on a cluster that had done nothing but upgrade.
///
/// Dropping it loses nothing. The row cache is empty after the restart an
/// upgrade requires, so there is no cached row whose invalidation could be
/// missed; the new slot starts from the current WAL position and is correct
/// from its first pass.
///
/// Narrow on purpose: only a slot with exactly the configured name, only if it
/// is ours by output plugin, and never the one now in use. Anything else is
/// somebody's replication slot and is left alone.
fn drop_legacy_decode_slot(base: &str, in_use: &str) {
    use std::panic::AssertUnwindSafe;
    if base == in_use {
        return;
    }
    let found = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::get_one_with_args::<bool>(
            // `database = current_database()` narrows it to the one place the
            // pre-#120 slot could have been created. Without it, every pool
            // worker in every database would try to drop the same slot, and all
            // but one would log a failure for a slot that was already gone.
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
             WHERE slot_name = $1 AND plugin = 'supacache_keys' AND NOT active \
               AND database = current_database())",
            vec![(PgBuiltInOids::TEXTOID.oid(), base.into_datum())],
        )
        .ok()
        .flatten()
        .unwrap_or(false)
    }));
    if !found {
        return;
    }
    let res: Result<(), String> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT pg_drop_replication_slot($1)",
                    None,
                    Some(vec![(PgBuiltInOids::TEXTOID.oid(), base.into_datum())]),
                )
                .map(|_| ())
        })
        .map_err(|e| e.to_string())
    }));
    match res {
        Ok(()) => log!(
            "pg_keyspace invalidation: dropped the pre-per-database slot '{base}'; \
             this database now decodes through '{in_use}'. Nothing was lost -- the \
             row cache is empty after a restart -- and leaving it would have pinned \
             WAL with nothing consuming it."
        ),
        Err(why) => log!(
            "pg_keyspace invalidation: could not drop the pre-per-database slot \
             '{base}': {why}. It is no longer consumed and will retain WAL from its \
             restart_lsn until it is dropped by hand: \
             SELECT pg_drop_replication_slot('{base}')."
        ),
    }
}

/// Hand pool slot `k` to a fresh worker, then let this one exit.
///
/// A pool member changes database by being REBORN -- a background worker binds
/// to one database for its life -- so every hand-over is an exit, and something
/// has to bring the replacement back. That something cannot be Postgres's own
/// `bgw_restart_time`: measured, a pool worker that exits CLEANLY is not
/// restarted at all, so the first hand-over ended with the pool empty, no slot
/// ever created, and every database reading incoherent. It was not a subtle
/// failure -- it was the whole feature not running -- and it took a real cluster
/// to see, because nothing errors and the logs just stop.
///
/// Nor can it be `health_watchdog`: the outgoing worker releases its health slot
/// so the replacement can claim it, and a released slot reads as "never started"
/// rather than as a gap to fill.
///
/// So the outgoing worker launches its own replacement, the way `health_relaunch`
/// launches any other worker. Ordering matters: the health slot is released
/// FIRST, or the replacement's `health_claim` would find this process still
/// alive and holding it, refuse, and exit -- leaving the pool slot empty for
/// good. And if Postgres does also restart the old registration, `health_claim`
/// is what makes the duplicate exit harmlessly.
fn rowcache_pool_relaunch(k: usize) {
    let health = health_invalidation_slot(k);
    // Released before the launch, never after: the replacement's `health_claim`
    // would otherwise find this process still alive and holding the slot,
    // refuse, and exit -- leaving the pool slot empty for good.
    health_release(health);
    let name = format!("pg_keyspace: rowcache invalidation worker {k}");
    let built = BackgroundWorkerBuilder::new(&name)
        .set_library("pg_keyspace")
        .set_function("pg_keyspace_invalidation_main")
        .set_argument((k as i32).into_datum())
        .set_restart_time(Some(Duration::from_secs(1)))
        .enable_spi_access()
        .set_notify_pid(0)
        .load_dynamic();
    if built.is_err() {
        // Hand the slot to the watchdog rather than leaving it empty. A released
        // slot reads as "never started", which `health_watchdog` deliberately
        // skips -- so without this the one case where the relaunch is most
        // likely to fail (`max_worker_processes` exhausted, which is a
        // transient thing an operator fixes) would also be the one case nothing
        // ever retried. Backdating the heartbeat makes it look like the gap it
        // actually is.
        if let Some(sl) = health_slot(health) {
            sl.last_seen_us.store(
                store::now_micros().saturating_sub(health_stale_us() * 2),
                Ordering::Release,
            );
        }
        log!(
            "pg_keyspace invalidation {k}: could not launch a replacement worker \
             (max_worker_processes reached?). This pool slot is empty, so the \
             databases it was serving are not being invalidated and their caches \
             fail closed. The watchdog will retry in {}s.",
            GUC_WATCHDOG_SECS.get()
        );
    }
}

/// Take a turn on a database, if one is free.
///
/// Called BEFORE connecting, which is the constraint the whole pool design is
/// shaped by: a background worker chooses its database once and can never
/// change it, so "connect to X, drain it, move on to Y" has to mean a new
/// process for Y. That is why this reads shared memory rather than a catalogue,
/// and why handing a turn on is an `exit` rather than a reconnect.
///
/// Preference order: a participating database nobody is draining, oldest first,
/// so the cycle is fair and the database furthest behind is served next; then an
/// unprobed one, which costs a connection to find out whether it participates at
/// all. A database whose owner is gone (crashed worker, expired lease) is free
/// again -- `pid_alive` is what stops one dead worker parking a database forever.
fn rcdb_lease() -> Option<(u32, String)> {
    let lock = RC_DB_LOCK.load(Ordering::Acquire);
    if lock.is_null() {
        return None;
    }
    let now = store::now_micros();
    let me = unsafe { pg_sys::MyProcPid } as u32;
    unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
    let free = |s: &DbSlot| rcdb_free(s, me, now);
    let pick = (0..rcdb_count())
        .filter_map(rcdb_at)
        .filter(|s| s.datoid.load(Ordering::Acquire) != 0 && free(s))
        .filter(|s| {
            let st = s.state.load(Ordering::Acquire);
            st == DB_PARTICIPATING || st == DB_UNKNOWN
        })
        .min_by_key(|s| {
            // LEAST RECENTLY ATTEMPTED first. Round-robin, and the ordering is
            // load-bearing rather than a preference.
            //
            // Ranking participating databases ahead of unprobed ones seems
            // obviously right and is not: a database that is already
            // participating has just been drained, so it would outrank every
            // unprobed database FOREVER. The worker took its turn on the one
            // database it knew about, handed over because others were waiting,
            // and its replacement picked the same database again -- measured,
            // seven turns in twenty seconds, with two other databases never
            // probed at all. After any restart, when the directory is empty and
            // every database starts unprobed, exactly one would ever be served.
            //
            // Attempt time is what makes the cycle actually turn: every lease
            // stamps it, so nothing can be picked twice while something else
            // waits. It also handles the database that can NEVER be served --
            // one that cannot get a replication slot drains never, so ordering
            // by drain time would put it first every single time.
            let st = s.state.load(Ordering::Acquire);
            let rank = if st == DB_PARTICIPATING { 0i64 } else { 1 };
            (
                s.last_attempt_us.load(Ordering::Acquire),
                rank,
                s.last_drained_us.load(Ordering::Acquire),
            )
        });
    let out = pick.map(|s| {
        s.last_attempt_us.store(now, Ordering::Release);
        s.owner_pid.store(me, Ordering::Release);
        s.lease_until_us.store(
            now + (GUC_ROWCACHE_LEASE_MS.get().max(100) as i64) * 1000,
            Ordering::Release,
        );
        (s.datoid.load(Ordering::Acquire), rcdb_name(s))
    });
    unsafe { pg_sys::LWLockRelease(lock) };
    out
}

/// Is this database available to a worker other than its current owner?
///
/// Ordinarily NO while the owner is alive: handing a turn on is the owner
/// exiting and releasing, not somebody else taking the database out from under
/// it. Two workers on one slot is not a subtle problem -- Postgres answers it
/// with "replication slot is active for PID" and the loser dies every pass.
///
/// The lease is a safety valve for the case liveness cannot see: a worker whose
/// process is alive but which has stopped draining. It is renewed on every
/// successful pass, so a healthy owner's lease never expires however long it
/// holds the database, and only a wedged one's does. `health_stale_us` of grace
/// on top, so a slow pass is not mistaken for a wedged worker.
#[inline]
fn rcdb_free(s: &DbSlot, me: u32, now: i64) -> bool {
    let owner = s.owner_pid.load(Ordering::Acquire);
    if owner == 0 || owner == me || !pid_alive(owner) {
        return true;
    }
    let until = s.lease_until_us.load(Ordering::Acquire);
    until != 0 && now.saturating_sub(until) > health_stale_us()
}

/// Take ownership of a database this worker has already connected to.
///
/// The fallback path needs it and so does correctness. A worker that found the
/// directory empty connected to `pg_keyspace.database` without leasing anything,
/// and with a pool of more than one they ALL did -- then all of them would drain
/// one slot, which Postgres answers with "replication slot is active for PID"
/// and a dead worker per pass. Claiming here, after the connection has told us
/// which database we are actually in, is what makes exactly one of them proceed.
///
/// Refuses only to a LIVE owner, for the same reason `health_claim` does: a
/// worker restarted two seconds after its predecessor exited must not be locked
/// out by the lease that predecessor left behind.
fn rcdb_own(datoid: u32) -> bool {
    let s = match rcdb_find(datoid) {
        Some(s) => s,
        None => return false,
    };
    let me = unsafe { pg_sys::MyProcPid } as u32;
    let now = store::now_micros();
    loop {
        let owner = s.owner_pid.load(Ordering::Acquire);
        if owner == me {
            return true;
        }
        if !rcdb_free(s, me, now) {
            return false;
        }
        if s.owner_pid
            .compare_exchange(owner, me, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            s.lease_until_us.store(
                now + (GUC_ROWCACHE_LEASE_MS.get().max(100) as i64) * 1000,
                Ordering::Release,
            );
            return true;
        }
    }
}

/// Give a database back, so another worker can take it.
fn rcdb_release(datoid: u32) {
    if let Some(s) = rcdb_find(datoid) {
        let me = unsafe { pg_sys::MyProcPid } as u32;
        let _ = s.owner_pid.compare_exchange(me, 0, Ordering::AcqRel, Ordering::Acquire);
        s.lease_until_us.store(0, Ordering::Release);
    }
}

/// Is some database waiting for a worker that is not busy on it already?
///
/// What makes cycling happen at all. With no more participating databases than
/// pool workers this is always false, every worker keeps its database, and
/// invalidation latency is `rowcache_decode_ms` exactly as with one database --
/// the common case pays nothing for the mechanism.
fn rcdb_waiting(except: u32) -> bool {
    let now = store::now_micros();
    (0..rcdb_count()).filter_map(rcdb_at).any(|s| {
        let oid = s.datoid.load(Ordering::Acquire);
        if oid == 0 || oid == except {
            return false;
        }
        let st = s.state.load(Ordering::Acquire);
        if st != DB_PARTICIPATING && st != DB_UNKNOWN {
            return false;
        }
        rcdb_free(s, unsafe { pg_sys::MyProcPid } as u32, now)
    })
}

/// Record every connectable database in the directory, so the pool can find the
/// ones it has never seen.
///
/// Needed only because shared memory does not survive a restart: registration
/// publishes its own database directly, so in steady state there is nothing here
/// to discover. After a restart the directory is empty and this is what rebuilds
/// it -- one cheap catalogue read, then a connection per unknown database to
/// find out whether it actually has registrations.
///
/// Template databases and those with `datallowconn = false` are skipped: a
/// worker cannot connect to them, so listing them would make the pool cycle
/// through databases it can never serve.
fn rcdb_discover() {
    use std::panic::AssertUnwindSafe;
    let rows: Vec<(u32, String)> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            let mut out = Vec::new();
            let t = match client.select(
                "SELECT oid::int8, datname::text FROM pg_database \
                 WHERE datallowconn AND NOT datistemplate",
                None,
                None,
            ) {
                Ok(t) => t,
                Err(_) => return out,
            };
            for row in t {
                if let (Ok(Some(oid)), Ok(Some(name))) =
                    (row.get::<i64>(1), row.get::<String>(2))
                {
                    out.push((oid as u32, name));
                }
            }
            out
        })
    }));
    if rows.is_empty() {
        return; // the read failed; do not conclude every database is gone
    }
    // Forget databases that no longer exist. Not housekeeping: a directory entry
    // for a dropped database is one a pool worker will lease and then fail to
    // connect to, dying and being restarted a second later, forever.
    //
    // Each candidate is re-checked against `pg_database` on its own before it is
    // cleared. The list above is a snapshot, and a database CREATED after it was
    // taken -- and published by a registration in the meantime -- would look
    // missing and have its entry taken away, leaving it registered and never
    // invalidated until something published it again. The re-check is a query
    // per dropped database, which is as rare as dropping databases.
    let now = store::now_micros();
    let me = unsafe { pg_sys::MyProcPid } as u32;
    let candidates: Vec<u32> = (0..rcdb_count())
        .filter_map(rcdb_at)
        .filter_map(|s| {
            let oid = s.datoid.load(Ordering::Acquire);
            if oid == 0 || rows.iter().any(|(o, _)| *o == oid) {
                return None;
            }
            // Never pull a database out from under the worker draining it.
            if !rcdb_free(s, me, now) {
                return None;
            }
            Some(oid)
        })
        .collect();
    for oid in candidates {
        let still_there = BackgroundWorker::transaction(AssertUnwindSafe(|| {
            Spi::get_one_with_args::<bool>(
                "SELECT EXISTS(SELECT 1 FROM pg_database WHERE oid = $1)",
                vec![(PgBuiltInOids::OIDOID.oid(), pg_sys::Oid::from(oid).into_datum())],
            )
            .ok()
            .flatten()
            .unwrap_or(true) // a failed read must never be read as "it is gone"
        }));
        if still_there {
            continue;
        }
        if let Some(s) = rcdb_find(oid) {
            // Re-checked, because the entry may have been reused for another
            // database between the scan above and here.
            if s.datoid.load(Ordering::Acquire) == oid && rcdb_free(s, me, store::now_micros()) {
                s.state.store(DB_IDLE, Ordering::Release);
                s.datoid.store(0, Ordering::Release);
                log!(
                    "pg_keyspace invalidation: database oid {oid} is gone; freed its \
                     directory entry"
                );
            }
        }
    }
    let full = rows.len();
    let mut recorded = 0usize;
    for (oid, name) in rows {
        if rcdb_publish(oid, &name, DB_UNKNOWN).is_some() {
            recorded += 1;
        }
    }
    if recorded < full {
        log!(
            "pg_keyspace invalidation: the row-cache database directory is full at \
             {} entries, so {} database(s) cannot be served and their registrations \
             would never be invalidated. Raise pg_keyspace.rowcache_max_databases.",
            rcdb_count(),
            full - recorded
        );
    }
}

/// Does the database this worker is connected to have row-cache registrations?
///
/// The catalogue is the truth and the directory is a cache of it, so this is the
/// only thing that decides whether a database gets a slot. A database with no
/// registrations gets no slot and no turn, which is the "launch lazily" rule:
/// most clusters have one or two participating databases and should pay nothing
/// at all for the rest.
fn rcdb_probe_self() -> i64 {
    use std::panic::AssertUnwindSafe;
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        if !table_exists("supacache.rowcache_reg") {
            return 0;
        }
        Spi::get_one::<i64>("SELECT count(*) FROM supacache.rowcache_reg")
            .ok()
            .flatten()
            .unwrap_or(0)
    }))
}

/// Whether the server has invalidated this slot, i.e. cut it loose rather than
/// let it retain WAL past `max_slot_wal_keep_size`.
///
/// Returns the `wal_status` text: `reserved`/`extended` are healthy, `unreserved`
/// means it is about to be at risk, and `lost` means the slot can no longer be
/// read from and the changes it had not yet delivered are gone.
fn slot_wal_status(slot: &str) -> Option<String> {
    use std::panic::AssertUnwindSafe;
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::get_one_with_args::<String>(
            "SELECT wal_status::text FROM pg_replication_slots WHERE slot_name = $1",
            vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())],
        )
        .ok()
        .flatten()
    }))
}

/// Drop every cached ROW belonging to one database, leaving its registrations
/// and its loaded-marker alone.
///
/// Returns how many entries went. Used when a slot has been lost: an unknown set
/// of invalidations was never delivered, so every row cached for that database
/// is suspect and none of it can be told apart from the rest. Registrations
/// survive deliberately -- they are configuration, not cache content, and
/// dropping them would silently stop caching the tables an operator asked for
/// (#103) on top of the outage that caused this.
fn rowcache_purge_database(datoid: u32) -> u64 {
    let view = match rowcache_view() {
        Some(v) => v,
        None => return 0,
    };
    let prefix = rc_key_for(datoid, 0, &[]);
    let prefix = &prefix[..5]; // tag byte + database oid
    let mut doomed: Vec<Vec<u8>> = Vec::new();
    let mut cursor = 0u64;
    loop {
        let (next, batch) = view.scan(cursor, 512);
        for k in batch {
            if k.starts_with(prefix) {
                doomed.push(k);
            }
        }
        if next == 0 {
            break;
        }
        cursor = next;
    }
    // Collected first, then deleted: `del` mutates the partition the scan is
    // walking, and moving the ground under a cursor loses entries.
    let mut gone = 0u64;
    for k in doomed {
        if rowcache_write(&view, &k, || view.del(&k), false) {
            gone += 1;
        }
    }
    gone
}

/// Rebuild a database's invalidation after its slot was lost.
///
/// The order is the whole of the correctness here. The database is marked
/// incoherent FIRST, so reads stop being served from the cache before anything
/// else happens; then the cached rows go, because the gap in the slot is exactly
/// a set of invalidations that will never arrive and there is no way to tell
/// which rows they were; then a new slot is created, which starts from the
/// current WAL position; and only then is the database trusted again.
///
/// Resuming without the purge is the failure this system must never have: a
/// stale row served as truth, with a healthy-looking slot in front of it.
fn recover_lost_slot(datoid: u32, slot: &str) {
    use std::panic::AssertUnwindSafe;
    if let Some(s) = rcdb_find(datoid) {
        s.slot_lost.store(1, Ordering::Release);
    }
    log!(
        "pg_keyspace invalidation: slot '{slot}' has been invalidated by the server \
         (wal_status = lost), which happens when it retains more WAL than \
         max_slot_wal_keep_size. Changes committed since it stopped advancing were \
         never delivered, so this database's cached rows are being dropped and the \
         slot rebuilt. Reads fall back to the heap until that is done."
    );
    let dropped = rowcache_purge_database(datoid);
    let res: Result<(), String> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT pg_drop_replication_slot($1)",
                    None,
                    Some(vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())]),
                )
                .map(|_| ())
        })
        .map_err(|e| e.to_string())
    }));
    if let Err(why) = res {
        log!("pg_keyspace invalidation: cannot drop lost slot '{slot}': {why}");
        return; // stays marked incoherent; retried next pass
    }
    if let Err(why) = ensure_decode_slot(slot) {
        log!("pg_keyspace invalidation: cannot recreate slot '{slot}': {why}");
        return;
    }
    // A rebuilt slot can be invalidated again before it ever reserves WAL.
    // Observed one second after the rebuild with `restart_lsn` still null, on a
    // cluster whose max_slot_wal_keep_size cannot cover its own WAL rate.
    //
    // Clearing `slot_lost` anyway reports this database COHERENT with a dead
    // slot, which is the failure this whole path exists to prevent, reached from
    // the other side: the cache was purged moments ago, so read-through refills
    // it with rows nothing will ever invalidate, and `rowcache_coherence()` --
    // which judges on this flag and the heartbeat, never on the slot -- says
    // everything is fine. Leave it marked and let the next pass retry. A
    // database whose slot cannot survive is exactly the case that must fail
    // closed.
    if slot_wal_status(slot).as_deref() == Some("lost") {
        log!(
            "pg_keyspace invalidation: slot '{slot}' was invalidated again as soon as it \
             was rebuilt, so this database stays incoherent and its reads keep falling \
             back to the heap. max_slot_wal_keep_size is too small for this cluster's \
             WAL rate; raise it, or this database cannot hold a cache at all."
        );
        return;
    }
    // The registrations were deliberately not purged, but the marker may have
    // been lost with the segment at some earlier point; reloading is O(1) when
    // there is nothing to do.
    if !registrations_loaded() {
        load_registrations_worker();
    }
    if let Some(s) = rcdb_find(datoid) {
        s.slot_lost.store(0, Ordering::Release);
    }
    log!(
        "pg_keyspace invalidation: slot '{slot}' rebuilt; dropped {dropped} cached row(s) \
         for this database, which is the whole of what it could no longer keep current."
    );
}

/// Drop a database's decode slot, because it no longer has registrations.
///
/// Retiring a database matters more than tidiness: a slot retains WAL until it
/// is consumed, and one left behind for a database nobody caches any more would
/// pin WAL for the whole cluster with nothing to show for it.
fn drop_decode_slot(slot: &str) {
    use std::panic::AssertUnwindSafe;
    let exists = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::get_one_with_args::<bool>(
            "SELECT EXISTS(SELECT 1 FROM pg_replication_slots \
             WHERE slot_name = $1 AND plugin = 'supacache_keys' AND NOT active \
               AND database = current_database())",
            vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())],
        )
        .ok()
        .flatten()
        .unwrap_or(false)
    }));
    if !exists {
        return;
    }
    let res: Result<(), String> = BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            client
                .select(
                    "SELECT pg_drop_replication_slot($1)",
                    None,
                    Some(vec![(PgBuiltInOids::TEXTOID.oid(), slot.into_datum())]),
                )
                .map(|_| ())
        })
        .map_err(|e| e.to_string())
    }));
    match res {
        Ok(()) => log!(
            "pg_keyspace invalidation: dropped slot '{slot}'; this database has no \
             row-cache registrations, so it no longer needs one and would otherwise \
             retain WAL for nothing."
        ),
        Err(why) => log!("pg_keyspace invalidation: cannot drop unused slot '{slot}': {why}"),
    }
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_invalidation_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let k = unsafe { i32::from_polymorphic_datum(arg, false, pg_sys::INT4OID) }.unwrap_or(0) as usize;

    // Claim the pool slot BEFORE choosing a database, so two workers cannot both
    // take a turn and then both drain one database's slot -- which the store's
    // single-writer contract and the slot's own semantics would both refuse.
    let health = health_invalidation_slot(k);
    if !health_claim(health) {
        log!("pg_keyspace invalidation {k}: another worker holds this pool slot; exiting");
        return;
    }

    // Choose the database before connecting. This is the constraint the pool is
    // shaped by: a background worker binds to one database for its whole life,
    // so "move on to the next database" is an exit and a relaunch, and the
    // choice has to be made from shared memory because nothing else is readable
    // yet. With nothing in the directory -- a fresh cluster, where shared memory
    // is empty and no registration has published yet -- fall back to
    // `pg_keyspace.database`, which is the one database that certainly exists,
    // and rebuild the directory from there.
    let fallback = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    let leased = rcdb_lease();
    let dbname = leased
        .as_ref()
        .map(|(_, n)| n.clone())
        .unwrap_or_else(|| fallback.clone());
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);

    // Now that there is a connection, this database's own oid is authoritative
    // -- and for the fallback path it is the only way to learn it.
    let datoid = rc_db();
    let base = rowcache_slot_base();
    let slot = slot_name_for(&base, datoid);
    drop_legacy_decode_slot(&base, &slot);
    rcdb_discover();
    rcdb_publish(datoid, &dbname, DB_UNKNOWN);
    let poll = Duration::from_millis(GUC_ROWCACHE_DECODE_MS.get().max(10) as u64);
    // Nothing to do in a database with no registrations: no slot, no turn. Long
    // enough that an idle cluster is not a background load, short enough that a
    // first registration is picked up promptly.
    let idle_poll = Duration::from_millis(1000);
    let lease_us = (GUC_ROWCACHE_LEASE_MS.get().max(100) as i64) * 1000;

    // Claim it for real now that the connection has said which database this
    // actually is. On the fallback path nothing was leased, and with a pool
    // larger than one EVERY member would otherwise land on
    // `pg_keyspace.database` and drain one slot between them -- which Postgres
    // answers with "replication slot is active for PID" and a dead worker per
    // pass.
    //
    // Losing the claim means this worker is surplus: the pool is larger than
    // the number of databases that want one. It PARKS rather than exiting,
    // because Postgres restarts a cleanly-exited worker IMMEDIATELY, so exiting
    // here would be a hot loop of fork/connect/exit for as long as the pool
    // stayed oversized. Parked, it costs a wakeup a second and is ready to
    // rebind the moment a database needs it.
    if !rcdb_own(datoid) {
        log!(
            "pg_keyspace invalidation {k}: database '{dbname}' is already being drained \
             by another pool worker; parking until a database needs one"
        );
        while !BackgroundWorker::sigterm_received() {
            health_beat(health);
            if rcdb_waiting(0) {
                // Something is unserved. Exit so the relaunch can lease it --
                // this worker cannot change database without being reborn.
                break;
            }
            std::thread::sleep(idle_poll);
        }
        if BackgroundWorker::sigterm_received() {
            health_release(health);
        } else {
            rowcache_pool_relaunch(k);
        }
        return;
    }

    let started = store::now_micros();
    let mut serving = false;
    let mut announced = false;

    // The drain loop runs several times a second; the catalogue and the
    // database list change on human timescales. Re-asking at drain speed would
    // put a `count(*)` and a `pg_database` scan in front of every pass for no
    // new information, so both are throttled and the drain is left alone.
    const PROBE_EVERY_US: i64 = 2_000_000;
    const DISCOVER_EVERY_US: i64 = 30_000_000;
    let mut probed_at = 0i64;
    let mut discovered_at = store::now_micros();
    let mut regs = 0i64;

    while !BackgroundWorker::sigterm_received() {
        health_beat(health);
        // The catalogue is the truth about whether this database participates,
        // and it can change under us: an operator registers a first table, or
        // unregisters the last one, in a session this worker knows nothing about.
        let now = store::now_micros();
        if now.saturating_sub(probed_at) >= PROBE_EVERY_US || probed_at == 0 {
            probed_at = now;
            regs = rcdb_probe_self();
            let state = if regs > 0 { DB_PARTICIPATING } else { DB_IDLE };
            if let Some(s) = rcdb_publish(datoid, &dbname, state) {
                s.registrations.store(regs, Ordering::Release);
                s.last_probe_us.store(now, Ordering::Release);
            }
        }

        if regs == 0 {
            // Retire the database rather than leaving a slot behind. A slot
            // retains WAL until it is consumed, and one kept for a database
            // nobody caches any more pins WAL for the whole cluster with
            // nothing to show for it.
            if serving {
                drop_decode_slot(&slot);
                serving = false;
            }
            // Somebody else needs a worker more than this database does.
            if rcdb_waiting(datoid) {
                break;
            }
            if now.saturating_sub(discovered_at) >= DISCOVER_EVERY_US {
                discovered_at = now;
                rcdb_discover();
            }
            std::thread::sleep(idle_poll);
            continue;
        }

        if !serving {
            if let Err(why) = ensure_decode_slot(&slot) {
                // No slot means no invalidation, and no invalidation must NOT
                // read as a healthy cache: leaving `last_drained_us` alone is
                // what makes this database fail closed rather than serve rows
                // nothing is keeping current.
                log!(
                    "pg_keyspace invalidation {k}: cannot create slot '{slot}' for database \
                     '{dbname}' (is wal_level=logical, and does max_replication_slots cover \
                     one slot per participating database?): {why}. This database's row cache \
                     is NOT being invalidated and reads will fall back to the heap."
                );
                rcdb_release(datoid);
                // Postgres restarts a cleanly-exited worker IMMEDIATELY, so
                // returning here would fork, connect, fail and exit in a tight
                // loop for as long as the cause persisted -- and
                // `max_replication_slots` being too small persists until an
                // operator changes it and restarts. Pausing first turns that
                // into one attempt a second. The database keeps failing closed
                // throughout, which is the part that matters: `last_drained_us`
                // was never set, so its cache is not served.
                std::thread::sleep(idle_poll);
                if BackgroundWorker::sigterm_received() {
                    health_release(health);
                } else {
                    rowcache_pool_relaunch(k);
                }
                return;
            }
            serving = true;
        }
        if !announced {
            log!(
                "pg_keyspace invalidation {k}: draining slot '{slot}' for database \
                 '{dbname}' every {poll:?} (keys-only)"
            );
            announced = true;
        }

        // A slot the server cut loose is not a slow slot, it is a hole: the
        // changes it had not delivered are gone. Checked before draining, so a
        // pass never applies a partial view of a gap and then reports success.
        if slot_wal_status(&slot).as_deref() == Some("lost") {
            recover_lost_slot(datoid, &slot);
            continue;
        }

        // A segment with no load marker is a fresh one -- this worker was
        // relaunched, or the segment was reinitialised underneath a worker that
        // was not -- so the pinned registrations in the previous instance are
        // gone. Reload them from the catalogue before draining, or the tables
        // an operator registered would quietly stop being cached (#103). O(1)
        // when nothing is wrong, which is every pass but the first.
        if !registrations_loaded() {
            let n = load_registrations_worker();
            if n > 0 {
                log!(
                    "pg_keyspace invalidation {k}: loaded {n} row-cache registration(s) \
                     for database '{dbname}' into a fresh segment"
                );
            }
        }
        let d = drain_invalidations(&slot);
        // The heartbeat that coherence is judged against, and it is recorded on
        // the DATABASE rather than on this worker. A pool worker's own liveness
        // says nothing about whether this database is current -- under cycling
        // it is alive and draining somebody else for most of its life.
        if d.reached {
            if let Some(s) = rcdb_find(datoid) {
                let t = store::now_micros();
                s.last_drained_us.store(t, Ordering::Release);
                // Renew while healthy. Without this a worker that legitimately
                // keeps its database -- which is every worker when the pool is
                // big enough, i.e. the common case -- would look wedged once its
                // first lease elapsed, and another worker would take the
                // database out from under it.
                s.lease_until_us.store(t + lease_us, Ordering::Release);
            }
        }
        if d.reconciled > 0 {
            log!(
                "pg_keyspace invalidation {k}: reconciled {} changed row-cache entr(ies) \
                 in database '{dbname}'",
                d.reconciled
            );
        }
        if d.unparsed > 0 {
            log!(
                "pg_keyspace invalidation {k}: skipped {} unreadable change record(s) on slot \
                 '{slot}' (supacache_keys output format mismatch?)",
                d.unparsed
            );
        }

        // Hand the turn on, but only once this database is actually current and
        // only if somebody is waiting. With no more participating databases than
        // pool workers nothing ever waits, every worker keeps its database, and
        // latency is `rowcache_decode_ms` exactly as it was with one database --
        // the common case pays nothing at all for the pool existing.
        //
        // `!d.full` is what makes "current" mean something: a pass that stopped
        // at its WAL window has a backlog behind it, and handing over mid-drain
        // would leave this database further behind than the cycle time claims.
        if !d.full
            && store::now_micros().saturating_sub(started) >= lease_us
            && rcdb_waiting(datoid)
        {
            log!(
                "pg_keyspace invalidation {k}: lease on database '{dbname}' is up and \
                 another database is waiting; handing the turn on"
            );
            break;
        }

        // A batch that came back full means more is already waiting: come
        // straight back for it rather than letting a backlog drain one poll
        // interval at a time. Each pass does a batch of real work, so this is
        // not a spin, and sigterm is still checked between passes.
        if d.full {
            continue;
        }
        std::thread::sleep(poll);
    }
    rcdb_release(datoid);
    // Released so the replacement can claim it immediately rather than waiting
    // out a staleness window; the slot itself is retained, so the next worker on
    // this database resumes where this one stopped.
    // Only relaunch when this is a hand-over. On SIGTERM the cluster is going
    // down and a replacement would just have to be shut down again.
    if BackgroundWorker::sigterm_received() {
        health_release(health);
    } else {
        rowcache_pool_relaunch(k);
    }
    log!(
        "pg_keyspace invalidation {k}: stopping (slot '{slot}' for database '{dbname}' \
         retained for resume)"
    );
}

/// DROP every `supacache.kv_ttl_b<N>` partition with N < `now_bucket` (fully
/// past). Returns how many were dropped. O(1) DDL per partition.
fn drop_expired_partitions(now_bucket: i64) -> i64 {
    use std::panic::AssertUnwindSafe;
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let names: Vec<String> = Spi::connect(|client| {
            let mut v = Vec::new();
            let t = client.select(
                "SELECT c.relname::text FROM pg_inherits i \
                 JOIN pg_class c ON c.oid = i.inhrelid \
                 JOIN pg_class p ON p.oid = i.inhparent \
                 JOIN pg_namespace n ON n.oid = p.relnamespace \
                 WHERE n.nspname='supacache' AND p.relname='kv_ttl'",
                None,
                None,
            )?;
            for row in t {
                if let Some(name) = row.get::<String>(1)? {
                    v.push(name);
                }
            }
            Ok::<Vec<String>, pgrx::spi::Error>(v)
        })
        .unwrap_or_default();

        let mut dropped = 0i64;
        for name in names {
            if let Some(nstr) = name.strip_prefix("kv_ttl_b") {
                if let Ok(bucket) = nstr.parse::<i64>() {
                    if bucket < now_bucket {
                        let _ = Spi::run(&format!("DROP TABLE supacache.{name}"));
                        dropped += 1;
                    }
                }
            }
        }
        dropped
    }))
}

// ==== Mode B: transparent row cache via a planner custom scan =======
//
// set_rel_pathlist_hook adds a CustomPath for a registered cached relation with
// a `pk = Const` restriction whose row is currently cached. The CustomScan sets
// scanrelid = the base rel, so ExecInitCustomScan builds the scan slot from the
// table's tupdesc AND initialises ps.qual (from plan.qual) and the projection
// (from plan.targetlist). We pass the rel's restriction clauses through as
// plan.qual and keep the (already mask-rewritten) targetlist, so ExecScan
// re-applies RLS quals and the mask CASE to the cached row. We serve the
// RAW row only; never post-policy output.

use core::ffi::c_char;

struct SyncPtr<T>(T);
unsafe impl<T> Sync for SyncPtr<T> {}

static RC_SCAN_METHODS: SyncPtr<pg_sys::CustomScanMethods> = SyncPtr(pg_sys::CustomScanMethods {
    CustomName: c"pg_keyspace_rowcache".as_ptr() as *const c_char,
    CreateCustomScanState: Some(rc_create_state),
});

static RC_EXEC_METHODS: SyncPtr<pg_sys::CustomExecMethods> = SyncPtr(pg_sys::CustomExecMethods {
    CustomName: c"pg_keyspace_rowcache".as_ptr() as *const c_char,
    BeginCustomScan: Some(rc_begin),
    ExecCustomScan: Some(rc_exec),
    EndCustomScan: Some(rc_end),
    ReScanCustomScan: Some(rc_rescan),
    MarkPosCustomScan: None,
    RestrPosCustomScan: None,
    EstimateDSMCustomScan: None,
    InitializeDSMCustomScan: None,
    ReInitializeDSMCustomScan: None,
    InitializeWorkerCustomScan: None,
    ShutdownCustomScan: None,
    ExplainCustomScan: None,
});

static RC_PATH_METHODS: SyncPtr<pg_sys::CustomPathMethods> = SyncPtr(pg_sys::CustomPathMethods {
    CustomName: c"pg_keyspace_rowcache".as_ptr() as *const c_char,
    PlanCustomPath: Some(rc_plan),
    ReparameterizeCustomPathByChild: None,
});

static mut PREV_PATHLIST_HOOK: pg_sys::set_rel_pathlist_hook_type = None;

/// Execution state; `css` must be first so a `*CustomScanState` aliases it.
#[repr(C)]
struct RcScanState {
    css: pg_sys::CustomScanState,
    // canonical pk bytes (palloc'd), decoded from the bytea Const in
    // custom_private; POD pointer+len so it is safe inside this palloc0 struct.
    pk_ptr: *const u8,
    pk_len: usize,
    // pk column attnums, from the planner, packed little-endian. Needed to
    // resolve the columns on a cache miss without consulting the (evictable)
    // registration entry. A pointer+len rather than a Vec because this struct is
    // palloc0'd and must stay POD.
    att_ptr: *const u8,
    att_len: usize,
    done: bool,
}

/// The pk attnums the planner carried into this scan.
unsafe fn rc_state_attnums(st: *mut RcScanState) -> Vec<i16> {
    if (*st).att_ptr.is_null() || (*st).att_len < 2 {
        return Vec::new();
    }
    std::slice::from_raw_parts((*st).att_ptr, (*st).att_len)
        .chunks_exact(2)
        .map(|c| i16::from_le_bytes([c[0], c[1]]))
        .collect()
}

fn rowcache_planner_init() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(&RC_SCAN_METHODS.0);
        PREV_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
        pg_sys::set_rel_pathlist_hook = Some(rc_pathlist_hook);
    }
}

fn rc_reg_key(relid: u32) -> [u8; 9] {
    rc_reg_key_for(rc_db(), relid)
}

/// Marks that this instance of the row-cache segment has had ONE DATABASE's
/// registrations, the ones in that database's `supacache.rowcache_reg`, loaded
/// into it.
///
/// A distinct tag byte from `rc_key`/`rc_reg_key`, so it can never collide with
/// a row or a registration. Pinned like the registrations it vouches for, so
/// its absence means exactly one thing: this is a *fresh* segment as far as
/// this database is concerned, and whatever was pinned into the previous one is
/// gone.
///
/// Per-database (#120), and that is load-bearing rather than tidy. The marker
/// used to be one fixed constant for the whole segment. With several databases
/// loading into one segment, the first to finish would mark the segment loaded
/// for all of them, and every other database's registrations would never be
/// loaded at all -- their tables silently not cached, with a healthy-looking
/// coherence check, which is exactly the failure #103 was about.
fn rc_loaded_key() -> [u8; 5] {
    rc_loaded_key_for(rc_db())
}

/// Have THIS database's registrations been loaded into the segment currently
/// mapped?
///
/// O(1), so the invalidation worker can ask on every pass. It deliberately
/// tests the marker rather than tracking a generation number in worker-local
/// state: a worker that never restarted still needs to notice a segment that
/// was reinitialised underneath it.
fn registrations_loaded() -> bool {
    match rowcache_view() {
        Some(v) => matches!(v.get(&rc_loaded_key()), Lookup::Hit(_)),
        // No segment at all: nothing to load into, and nothing to report.
        None => true,
    }
}

/// Load every registration from `supacache.rowcache_reg` into the row-cache
/// segment and mark it loaded. Returns how many were pinned.
///
/// Must be called with an SPI connection already open (a backend, or inside
/// `BackgroundWorker::transaction`).
///
/// Names are resolved to oids here rather than stored as oids, so a table that
/// was dropped is skipped and one that was recreated picks up its registration
/// again. `to_regclass` returns NULL instead of raising for a name that no
/// longer resolves, which is the common case after a migration.
fn load_registrations_spi() -> i64 {
    let view = match rowcache_view() {
        Some(v) => v,
        None => return 0,
    };
    let rows: Vec<(u32, Vec<i16>)> = Spi::connect(|client| {
        let mut out = Vec::new();
        let t = match client.select(
            "SELECT to_regclass(r.tbl)::oid, r.attnums FROM supacache.rowcache_reg r \
             WHERE to_regclass(r.tbl) IS NOT NULL",
            None,
            None,
        ) {
            Ok(t) => t,
            Err(_) => return out,
        };
        for row in t {
            if let (Ok(Some(oid)), Ok(Some(atts))) = (
                row.get::<pg_sys::Oid>(1),
                row.get::<Vec<i16>>(2),
            ) {
                if !atts.is_empty() {
                    out.push((oid.as_u32(), atts));
                }
            }
        }
        out
    });
    let mut n = 0i64;
    for (relid, attnums) in rows {
        let packed: Vec<u8> = attnums.iter().flat_map(|a| a.to_le_bytes()).collect();
        if rowcache_write(&view, &rc_reg_key(relid), || view.set_pinned(&rc_reg_key(relid), &packed), false) {
            n += 1;
        }
    }
    // Last, and only on the same view: a marker written before the
    // registrations would claim a segment was loaded that is not.
    let marker = rc_loaded_key();
    rowcache_write(&view, &marker, || view.set_pinned(&marker, b"1"), false);
    n
}

/// Record the running worker count, reporting a change from what the persisted
/// keyspace was last written under.
///
/// Returns `(previous, moved)` when the layout changed. `pg_keyspace.workers` is
/// `Postmaster` context, so this runs once per start rather than on a timer.
///
/// This does not *prevent* anything: the change has already happened by the time
/// a worker is running, and `supacache.kv.slot` is stable so nothing is lost.
/// What it prevents is the change being invisible -- an operator who doubles the
/// worker count and then wonders why the hit rate collapsed for an hour has no
/// way to connect the two today.
fn record_topology(running: usize) -> Option<(usize, u32)> {
    use std::panic::AssertUnwindSafe;
    if !extension_installed() {
        return None;
    }
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let previous = Spi::get_one::<i32>("SELECT workers FROM supacache.topology WHERE id = 1")
            .ok()
            .flatten();
        let _ = Spi::run_with_args(
            "INSERT INTO supacache.topology(id, workers, updated_at) VALUES (1, $1, now()) \
             ON CONFLICT (id) DO UPDATE SET workers = EXCLUDED.workers, updated_at = now()",
            Some(vec![(PgBuiltInOids::INT4OID.oid(), (running as i32).into_datum())]),
        );
        match previous {
            Some(p) if p as usize != running && p > 0 => {
                Some((p as usize, crc16::slots_moved(p as usize, running)))
            }
            _ => None,
        }
    }))
}

/// `load_registrations_spi` from a background worker, which has to open its own
/// transaction.
fn load_registrations_worker() -> i64 {
    use std::panic::AssertUnwindSafe;
    if !extension_installed() {
        return 0;
    }
    BackgroundWorker::transaction(AssertUnwindSafe(load_registrations_spi))
}

/// Find a `pkcol = Const` restriction on the given attnum and return the pk in
/// canonical byte form (the type's output text). The Const is canonicalized via
/// its own type: for the common `int8col = <int4 literal>` case Postgres keeps
/// the literal as int4 (via the int8=int4 operator, no fold), but int2/int4/int8
/// all output the same decimal, so the bytes still match the tuple/plugin side.
/// A `pkcol = Const` only ever reaches us for a plain Var (RLS `owner = …` and
/// other expression clauses are filtered by `classify`), and the only implicit
/// cross-type `=` operators over a plain Var are within the value-preserving
/// integer family — so this cannot produce a wrong (incoherent) key; at worst a
/// non-matching literal misses the cache and falls back to the index path.
/// The canonical key for this scan, or None if the query does not pin *every*
/// primary-key column to a constant.
///
/// All-or-nothing is the correctness rule for a composite key, not a
/// conservatism: matching a query that constrains only some of the key columns
/// to a row cached under the whole key would serve one row where the query asks
/// for a set. That is an incoherence, not a miss, and it is the reason composite
/// keys were refused outright rather than half-supported.
unsafe fn find_pk_parts(rel: *mut pg_sys::RelOptInfo, attnums: &[i16]) -> Option<Vec<u8>> {
    if attnums.is_empty() {
        return None;
    }
    let mut parts = Vec::with_capacity(attnums.len());
    for &a in attnums {
        parts.push(find_pk_bytes(rel, a)?);
    }
    Some(compose_pk(&parts))
}

unsafe fn find_pk_bytes(rel: *mut pg_sys::RelOptInfo, attnum: i16) -> Option<Vec<u8>> {
    // An empty Postgres List IS a null pointer -- NIL, with no empty-list object
    // to point at -- so a base relation with no restriction clauses arrives here
    // with `baserestrictinfo == NULL`. Reading through it segfaulted the backend
    // at PLAN time, which Postgres answers by killing every other backend and
    // crash-restarting the cluster (#128).
    //
    // `SELECT count(*) FROM t` and `SELECT * FROM t` are exactly that shape, so
    // a registered table took the cluster down on the first unfiltered query
    // against it. The existing row-cache harnesses all query by primary key,
    // which is the case that works.
    //
    // None is the honest answer as well as the safe one: no clauses means no
    // `pk = const` to find, and the caller then leaves the ordinary path alone.
    let list = (*rel).baserestrictinfo;
    if list.is_null() {
        return None;
    }
    let cell = (*list).elements;
    let n = (*list).length;
    if cell.is_null() || n <= 0 {
        return None;
    }
    for i in 0..n {
        let ri = (*cell.offset(i as isize)).ptr_value as *mut pg_sys::RestrictInfo;
        if ri.is_null() {
            continue;
        }
        let clause = (*ri).clause as *mut pg_sys::Node;
        if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
            continue;
        }
        let op = clause as *mut pg_sys::OpExpr;
        // must be the "=" operator
        let opname = pg_sys::get_opname((*op).opno);
        if opname.is_null() || CStr::from_ptr(opname).to_bytes() != b"=" {
            continue;
        }
        let args = (*op).args;
        if args.is_null() || (*args).length != 2 {
            continue;
        }
        let a0 = (*(*args).elements.offset(0)).ptr_value as *mut pg_sys::Node;
        let a1 = (*(*args).elements.offset(1)).ptr_value as *mut pg_sys::Node;
        // Skip clauses that aren't `Var = Const` (e.g. an RLS `owner =
        // CURRENT_USER` predicate) rather than aborting the whole search.
        let (var, cst) = match classify(a0, a1) {
            Some(vc) => vc,
            None => continue,
        };
        if (*var).varattno != attnum {
            continue;
        }
        if (*cst).constisnull {
            continue;
        }
        return Some(canon_pk((*cst).consttype, (*cst).constvalue));
    }
    None
}

/// Return (Var, Const) from a pair in either order, if it is exactly that shape.
unsafe fn classify(
    a: *mut pg_sys::Node,
    b: *mut pg_sys::Node,
) -> Option<(*mut pg_sys::Var, *mut pg_sys::Const)> {
    let ta = (*a).type_;
    let tb = (*b).type_;
    if ta == pg_sys::NodeTag::T_Var && tb == pg_sys::NodeTag::T_Const {
        Some((a as *mut pg_sys::Var, b as *mut pg_sys::Const))
    } else if ta == pg_sys::NodeTag::T_Const && tb == pg_sys::NodeTag::T_Var {
        Some((b as *mut pg_sys::Var, a as *mut pg_sys::Const))
    } else {
        None
    }
}

#[pg_guard]
unsafe extern "C" fn rc_pathlist_hook(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    if let Some(prev) = PREV_PATHLIST_HOOK {
        prev(root, rel, rti, rte);
    }
    // A refill re-read must see the live table, not the cache it is refreshing.
    if RC_BYPASS.load(Ordering::SeqCst) {
        return;
    }
    // Invalidation asked for but not running: leave the ordinary index path in
    // place rather than substitute a cache nothing is keeping current.
    if !rowcache_coherent() {
        return;
    }
    if (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
        || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION
    {
        return;
    }
    // Never substitute the scan that feeds a data-modifying command's target,
    // or a row locked FOR UPDATE/SHARE: those need the REAL heap tuple (a valid
    // ctid) to lock and re-fetch. The cache serves a fabricated tuple with no
    // ctid, which would fail with "failed to fetch tuple being updated".
    let parse = (*root).parse;
    if parse.is_null() {
        return;
    }
    if (*parse).commandType != pg_sys::CmdType::CMD_SELECT
        && (*parse).resultRelation as u32 == rti
    {
        return;
    }
    if !pg_sys::get_parse_rowmark(parse, rti).is_null() {
        return;
    }
    let relid_u32 = (*rte).relid.as_u32();
    let view = match rowcache_view() {
        Some(v) => v,
        None => return,
    };
    let pk_attnums = match reg_attnums(relid_u32) {
        Some(a) => a,
        None => return,
    };
    let pk = match find_pk_parts(rel, &pk_attnums) {
        Some(v) => v,
        None => return,
    };
    // Normally only substitute when the row is actually cached, so an uncached
    // row takes the ordinary index path and pays nothing for the cache existing.
    //
    // With read-through on, substitute for any registered table: the miss is what
    // populates the cache, so declining here would mean it never warms without a
    // manual rowcache_put. Safe only because a miss now reads the row rather than
    // reporting end of scan (#85) -- before that, this would have turned every
    // cold read into zero rows.
    if !rowcache_readthrough_active()
        && !matches!(view.get(&rc_key(relid_u32, &pk)), Lookup::Hit(_))
    {
        return;
    }

    let cpath = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
    (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
    (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
    (*cpath).path.parent = rel;
    (*cpath).path.pathtarget = (*rel).reltarget;
    (*cpath).path.rows = 1.0;
    (*cpath).path.startup_cost = 0.0;
    (*cpath).path.total_cost = 0.0001; // beat the index path so this is chosen
    (*cpath).methods = &RC_PATH_METHODS.0;
    // carry the canonical pk bytes to execution via a bytea Const in
    // custom_private (survives copyObject during planning).
    let pk_datum = match pk.clone().into_datum() {
        Some(d) => d,
        None => return,
    };
    let pkc = pg_sys::makeConst(
        pg_sys::BYTEAOID,
        -1,
        pg_sys::InvalidOid,
        -1,
        pk_datum,
        false,
        false,
    );
    // The pk attnums travel with the plan for the same reason the pk bytes do:
    // everything execution needs must survive the cache entry it came from.
    // Packed little-endian as a bytea rather than a single int2, so a composite
    // key carries all of its columns.
    let att_bytes: Vec<u8> = pk_attnums.iter().flat_map(|a| a.to_le_bytes()).collect();
    let att_datum = match att_bytes.into_datum() {
        Some(d) => d,
        None => return,
    };
    let attc = pg_sys::makeConst(
        pg_sys::BYTEAOID,
        -1,
        pg_sys::InvalidOid,
        -1,
        att_datum,
        false,
        false,
    );
    (*cpath).custom_private = pg_sys::lappend(std::ptr::null_mut(), pkc as *mut core::ffi::c_void);
    (*cpath).custom_private =
        pg_sys::lappend((*cpath).custom_private, attc as *mut core::ffi::c_void);
    pg_sys::add_path(rel, cpath as *mut pg_sys::Path);
}

#[pg_guard]
unsafe extern "C" fn rc_plan(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    let cscan = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
    (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
    (*cscan).scan.plan.targetlist = tlist;
    // Re-apply the rel's restriction clauses (incl. RLS) above our scan.
    (*cscan).scan.plan.qual = pg_sys::extract_actual_clauses(clauses, false);
    (*cscan).scan.scanrelid = (*rel).relid;
    (*cscan).flags = (*best_path).flags;
    (*cscan).custom_plans = custom_plans;
    (*cscan).custom_private = (*best_path).custom_private;
    (*cscan).methods = &RC_SCAN_METHODS.0;
    cscan as *mut pg_sys::Plan
}

#[pg_guard]
unsafe extern "C" fn rc_create_state(cscan: *mut pg_sys::CustomScan) -> *mut pg_sys::Node {
    let st = pg_sys::palloc0(std::mem::size_of::<RcScanState>()) as *mut RcScanState;
    (*st).css.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    (*st).css.methods = &RC_EXEC_METHODS.0;
    // serve the cached tuple through a virtual slot (deform on read)
    (*st).css.slotOps = &pg_sys::TTSOpsVirtual;
    let pkc = pg_sys::list_nth((*cscan).custom_private, 0) as *mut pg_sys::Const;
    let bytes: Vec<u8> = if pkc.is_null() || (*pkc).constisnull {
        Vec::new()
    } else {
        Vec::<u8>::from_datum((*pkc).constvalue, false).unwrap_or_default()
    };
    // Copy into a palloc'd buffer owned by the scan's memory context.
    let n = bytes.len();
    let buf = pg_sys::palloc(n.max(1)) as *mut u8;
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, n);
    (*st).pk_ptr = buf;
    (*st).pk_len = n;
    let attc = pg_sys::list_nth((*cscan).custom_private, 1) as *mut pg_sys::Const;
    let atts: Vec<u8> = if attc.is_null() || (*attc).constisnull {
        Vec::new()
    } else {
        Vec::<u8>::from_datum((*attc).constvalue, false).unwrap_or_default()
    };
    let an = atts.len();
    let abuf = pg_sys::palloc(an.max(1)) as *mut u8;
    std::ptr::copy_nonoverlapping(atts.as_ptr(), abuf, an);
    (*st).att_ptr = abuf;
    (*st).att_len = an;
    (*st).done = false;
    st as *mut pg_sys::Node
}

#[pg_guard]
unsafe extern "C" fn rc_begin(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: core::ffi::c_int,
) {
    (node as *mut RcScanState).as_mut().unwrap().done = false;
}

#[pg_guard]
unsafe extern "C" fn rc_exec(node: *mut pg_sys::CustomScanState) -> *mut pg_sys::TupleTableSlot {
    pg_sys::ExecScan(
        &mut (*node).ss,
        Some(rc_access),
        Some(rc_recheck),
    )
}

/// Access method: return the cached row once (raw), then an empty slot to end.
#[pg_guard]
unsafe extern "C" fn rc_access(ss: *mut pg_sys::ScanState) -> *mut pg_sys::TupleTableSlot {
    let st = ss as *mut RcScanState;
    let slot = (*ss).ss_ScanTupleSlot;
    if (*st).done {
        return pg_sys::ExecClearTuple(slot);
    }
    (*st).done = true;
    let rel = (*ss).ss_currentRelation;
    let relid = (*rel).rd_id.as_u32();
    let view = match rowcache_view() {
        Some(v) => v,
        None => return pg_sys::ExecClearTuple(slot),
    };
    let pk = std::slice::from_raw_parts((*st).pk_ptr, (*st).pk_len);
    // A miss here is NOT end of scan. Whether to use the cache is decided at
    // plan time, but the lookup happens now, and anything that removes the entry
    // in between -- an eviction, an invalidation, a flush -- would otherwise turn
    // a correct query into an empty result with no error. A cached plan makes
    // that window unbounded: the plan outlives the entry, and row-cache activity
    // does not invalidate plans. Measured, a prepared statement returned zero
    // rows for a row sitting in the heap the whole time (#85).
    //
    // So a miss falls back to reading the row, and costs a fetch rather than an
    // answer. The read runs with the row-cache substitution bypassed (the guard
    // inside `fetch_row_and_pk`) so it cannot recurse into this node, and
    // read-only so it observes the running query's snapshot rather than taking a
    // fresh one mid-scan.
    //
    // Deliberately not repopulating the cache here: that is read-through warming
    // (#10), and doing it as a side effect of a miss would let one scan of
    // evicted rows churn the whole cache.
    // Checked here as well as at plan time, because a plan outlives the
    // condition: a statement planned while invalidation was healthy would go on
    // serving cached rows through a worker outage otherwise. `rc_access` runs
    // once per scan, so this costs one clock read per scan rather than per row.
    let trusted = rowcache_coherent();
    let fallback;
    // get_stable, not get (#127). `get` hands back a BORROW into shared memory
    // and the bytes are copied a few lines below; a writer between those two
    // points -- another backend's read-through, or the invalidation worker's
    // refill -- rewrites them in place, and the copy takes the head of one
    // tuple and the tail of another. That is then deformed as a HeapTuple,
    // which reads lengths and offsets out of the spliced bytes.
    //
    // The writer lock does not help here and is not meant to: readers are
    // deliberately not serialised, because a lock on the read path would cost
    // more than the cache saves. `get_stable` re-reads through the entry's
    // seqlock and retries, which is the same thing every SQL read of the RESP
    // keyspace already does for exactly this reason -- the row cache was the
    // one reader that still borrowed.
    let hit: Option<Vec<u8>> = if trusted {
        view.get_stable(&rc_key(relid, pk))
    } else {
        None
    };
    let bytes: &[u8] = match hit {
        Some(ref b) => &b[..],
        None => {
            // Resolved from the attnum the planner carried, not from the
            // registration entry: that entry is an ordinary cache entry and a
            // busy cache evicts it, which is exactly the situation a miss means
            // we are in.
            let attnums = rc_state_attnums(st);
            let meta = match rowcache_meta_for_attnums((*rel).rd_id, &attnums) {
                Some(m) => m,
                None => return pg_sys::ExecClearTuple(slot),
            };
            let parts = split_pk(pk);
            if parts.len() != meta.cols.len() {
                return pg_sys::ExecClearTuple(slot);
            }
            match fetch_row_and_pk(&meta, &parts, true) {
                // Genuinely absent: the row does not exist, so no rows is the
                // right answer and matches what an index scan would return.
                None => return pg_sys::ExecClearTuple(slot),
                Some((raw, _)) => {
                    // Read-through: the miss that just cost a fetch is exactly the
                    // moment the cache should learn the row. Off by default, since
                    // repopulating on every miss would let one scan over evicted
                    // rows churn a cache that is doing its job.
                    if rowcache_readthrough_active() {
                        // THE #127 CRASH SITE. This runs in an ordinary backend,
                        // and every backend that misses reaches it, so without
                        // the lock this is N concurrent writers into one arena.
                        rowcache_write(&view, &rc_key(relid, pk), || view.set(&rc_key(relid, pk), &raw, 0), false);
                    }
                    fallback = raw;
                    &fallback[..]
                }
            }
        }
    };
    // Copy the cached tuple bytes into an aligned palloc buffer, wrap as a
    // HeapTuple, deform into the (virtual) scan slot, and store.
    let n = bytes.len();
    let buf = pg_sys::palloc(n) as *mut u8;
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, n);
    let ht = pg_sys::palloc0(std::mem::size_of::<pg_sys::HeapTupleData>()) as *mut pg_sys::HeapTupleData;
    (*ht).t_len = n as u32;
    (*ht).t_data = buf as *mut pg_sys::HeapTupleHeaderData;
    (*ht).t_tableOid = (*rel).rd_id;
    pg_sys::ExecClearTuple(slot);
    let tupdesc = (*slot).tts_tupleDescriptor;
    pg_sys::heap_deform_tuple(ht, tupdesc, (*slot).tts_values, (*slot).tts_isnull);
    pg_sys::ExecStoreVirtualTuple(slot)
}

#[pg_guard]
unsafe extern "C" fn rc_recheck(
    _ss: *mut pg_sys::ScanState,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    true
}

#[pg_guard]
unsafe extern "C" fn rc_end(_node: *mut pg_sys::CustomScanState) {}

#[pg_guard]
unsafe extern "C" fn rc_rescan(node: *mut pg_sys::CustomScanState) {
    (node as *mut RcScanState).as_mut().unwrap().done = false;
}

/// Quote an SQL identifier (schema/column) via the server's own routine.
fn quote_ident(s: &str) -> String {
    let c = match std::ffi::CString::new(s) {
        Ok(c) => c,
        Err(_) => return String::new(),
    };
    unsafe {
        let q = pg_sys::quote_identifier(c.as_ptr());
        std::ffi::CStr::from_ptr(q).to_string_lossy().into_owned()
    }
}

/// Run a single-row SELECT and return the raw heap-tuple bytes of the first
/// row (HeapTupleHeader + data), copied out before the SPI context is freed.
/// Set while a refill re-reads a row from the heap: the Mode B pathlist hook
/// checks it and skips substitution, so the re-read hits the real table and not
/// the very cache entry we are refreshing (a one-shot/custom plan folds a bound
/// `$1` to a Const, so parameterizing alone is NOT enough to dodge the hook —
/// this flag is). Per-backend and single-threaded, so a plain flag is safe.
static RC_BYPASS: AtomicBool = AtomicBool::new(false);

struct BypassGuard;
impl BypassGuard {
    fn new() -> Self {
        RC_BYPASS.store(true, Ordering::SeqCst);
        BypassGuard
    }
}
impl Drop for BypassGuard {
    fn drop(&mut self) {
        RC_BYPASS.store(false, Ordering::SeqCst);
    }
}

// `toast_flatten_tuple` (access/heaptoast.h) builds a tuple with no out-of-line
// fields — it is a real exported backend symbol but is not in pgrx's generated
// bindings, so declare it directly. Links against the running backend.
extern "C" {
    fn toast_flatten_tuple(
        tup: pg_sys::HeapTuple,
        tuple_desc: pg_sys::TupleDesc,
    ) -> pg_sys::HeapTuple;
}

/// Fetch the row matching `where_sql` (a predicate over the pk column using
/// `$1`) and return (raw heap-tuple bytes, canonical pk bytes of that row's pk
/// column `col`). `rel_q` is identifier-quoted; `col` is the *unquoted* pk
/// column name (for `SPI_fnumber`). `$1` is bound from (`argtype`, `argdatum`).
/// Runs with the row-cache substitution bypassed (see `RC_BYPASS`) so the read
/// reflects the live table, never a stale cache entry. Keying by the *fetched*
/// row's canonical pk (not the lookup literal) makes put and the WAL decode path
/// agree even when the caller passes a non-canonical literal.
unsafe fn fetch_row_and_pk(
    meta: &RegMeta,
    args: &[Vec<u8>],
    read_only: bool,
) -> Option<(Vec<u8>, Vec<u8>)> {
    if args.len() != meta.cols.len() {
        return None;
    }
    let _bypass = BypassGuard::new();
    let rel_q = &meta.rel_q;
    let where_sql = meta.where_sql();
    let query = format!("SELECT * FROM {rel_q} WHERE {where_sql}");
    let q = std::ffi::CString::new(query).ok()?;
    let col_cs: Vec<std::ffi::CString> = meta
        .cols
        .iter()
        .map(|c| std::ffi::CString::new(c.as_str()))
        .collect::<Result<_, _>>()
        .ok()?;
    // Every pk part binds as text and is cast to the column type in SQL. That is
    // what the decode worker has always had to do -- WAL decode only ever yields
    // canonical text -- and unifying on it means one query shape serves the
    // worker, the executor's miss fallback and the SQL surface, instead of three
    // that could drift apart on a composite key.
    //
    // Built before SPI_connect so the datums live in the caller's context rather
    // than the SPI one, which is freed by SPI_finish before they are read back.
    let mut argtypes: Vec<pg_sys::Oid> = vec![pg_sys::TEXTOID; args.len()];
    let mut values: Vec<pg_sys::Datum> = Vec::with_capacity(args.len());
    for a in args {
        values.push(String::from_utf8_lossy(a).into_owned().into_datum()?);
    }
    if pg_sys::SPI_connect() != pg_sys::SPI_OK_CONNECT as i32 {
        return None;
    }
    // A refill passes read_only = false so it takes a fresh snapshot and sees the
    // row as of now -- the just-committed change -- rather than as of the worker
    // transaction's start.
    //
    // A read served from inside a running query passes true, and must: taking a
    // new snapshot mid-scan would let one leaf of a query see a row as of a
    // later moment than the rest of it, which no index scan would ever do.
    let rc = pg_sys::SPI_execute_with_args(
        q.as_ptr(),
        args.len() as i32,
        argtypes.as_mut_ptr(),
        values.as_mut_ptr(),
        std::ptr::null(),
        read_only,
        1,
    );
    let out = if rc == pg_sys::SPI_OK_SELECT as i32 && pg_sys::SPI_processed >= 1 {
        let tuptable = pg_sys::SPI_tuptable;
        let tupdesc = (*tuptable).tupdesc;
        let tup = *(*tuptable).vals.offset(0);
        // If any column is stored out of line (TOASTed), the raw tuple holds a
        // pointer into the table's toast relation, not the value — caching those
        // bytes would leave a pointer that dangles once the toast chunks are
        // vacuumed. Flatten the tuple so every value is inline and the cached
        // bytes are fully self-contained. `toast_flatten_tuple` pulls in the
        // external values while leaving cheap inline-compressed ones as-is.
        let has_external =
            (*(*tup).t_data).t_infomask & pg_sys::HEAP_HASEXTERNAL as u16 != 0;
        let flat = if has_external {
            toast_flatten_tuple(tup, tupdesc)
        } else {
            tup
        };
        let len = (*flat).t_len as usize;
        let mut buf = vec![0u8; len];
        std::ptr::copy_nonoverlapping((*flat).t_data as *const u8, buf.as_mut_ptr(), len);
        if flat != tup {
            pg_sys::heap_freetuple(flat);
        }
        // Canonical pk of the fetched row, composed from every pk column in the
        // same order the key was built in (from the original tuple -- pk columns
        // are never external, and the value is identical either way).
        //
        // Recomputed from the row rather than echoed back from the lookup
        // arguments, so the entry is keyed by what the row actually holds. A
        // lookup that matched through a cast ('01' finding a bigint 1) still
        // caches under the row's own canonical form.
        let mut parts = Vec::with_capacity(col_cs.len());
        let mut any_null = false;
        for col_c in &col_cs {
            let fno = pg_sys::SPI_fnumber(tupdesc, col_c.as_ptr());
            let mut isnull = false;
            let d = pg_sys::SPI_getbinval(tup, tupdesc, fno, &mut isnull);
            let coltyp = pg_sys::SPI_gettypeid(tupdesc, fno);
            if isnull {
                any_null = true;
                break;
            }
            parts.push(canon_pk(coltyp, d));
        }
        // A NULL pk column cannot identify a row; report it the way an absent
        // canonical pk has always been reported, so callers treat it as "gone".
        let canon = if any_null { Vec::new() } else { compose_pk(&parts) };
        Some((buf, canon))
    } else {
        None
    };
    pg_sys::SPI_finish();
    out
}

/// Outcome of refilling one row-cache entry from the live table.
enum Refill {
    Stored,  // the current row was read and cached
    Gone,    // the row no longer exists (deleted) — caller should invalidate
    Skipped, // relation not registered / not resolvable
}

/// Read the current row of `relid` where the registered pk column = `pk` and
/// store its RAW heap-tuple bytes in the row cache. Shared by the SQL
/// `rowcache_put` surface and the invalidation worker's refill path. Assumes a
/// transaction is open (SPI usable). The raw bytes are pre-policy; RLS and the
/// mask re-apply above the Custom Scan on read, so this is the same trust
/// model as the manual put — the decode stream is still keys-only.
unsafe fn rowcache_refill_locked(relid: pg_sys::Oid, pk_lookup: &[u8]) -> Refill {
    let view = match rowcache_view() {
        Some(v) => v,
        None => return Refill::Skipped,
    };
    let meta = match rowcache_reg_meta(relid) {
        Some(m) => m,
        None => return Refill::Skipped,
    };
    // The WAL decode emits each pk part in canonical text form, joined by
    // `compose_pk`; split it back into one argument per pk column.
    let parts = split_pk(pk_lookup);
    if parts.len() != meta.cols.len() {
        return Refill::Skipped;
    }
    match fetch_row_and_pk(&meta, &parts, false) {
        Some((raw, canon)) if !canon.is_empty() => {
            rowcache_write(&view, &rc_key(relid.as_u32(), &canon), || view.set(&rc_key(relid.as_u32(), &canon), &raw, 0), false);
            Refill::Stored
        }
        _ => Refill::Gone,
    }
}

/// Registration metadata for a cached relation: the pk column name, its type
/// name (SQL-castable), and the relation's `regclass` text — resolved from the
/// stored attnum. `None` if the relation is not registered / not resolvable.
struct RegMeta {
    /// pk column names, ascending by attnum -- the same order the decode plugin
    /// emits its parts in, and the order `compose_pk` joins them in.
    cols: Vec<String>,
    /// SQL-castable type name per column, positionally matching `cols`.
    typenames: Vec<String>,
    rel_q: String,
}

impl RegMeta {
    /// `c1 = $1::t1 AND c2 = $2::t2 ...`
    ///
    /// Every part binds as text and casts in SQL, which is what lets one code
    /// path serve the decode worker (which only ever has canonical text), the
    /// executor's miss fallback and the SQL surface alike. The cast keeps the
    /// primary-key index usable.
    fn where_sql(&self) -> String {
        self.cols
            .iter()
            .zip(self.typenames.iter())
            .enumerate()
            .map(|(i, (c, t))| format!("{} = ${}::{}", quote_ident(c), i + 1, t))
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

/// The pk attnums a relation is registered with, ascending. The entry is simply
/// the attnums packed little-endian, so a single-column registration is the two
/// bytes it has always been.
fn reg_attnums(relid: u32) -> Option<Vec<i16>> {
    let view = rowcache_view()?;
    match view.get(&rc_reg_key(relid)) {
        Lookup::Hit(b) if b.len() >= 2 && b.len() % 2 == 0 => Some(
            b.chunks_exact(2)
                .map(|c| i16::from_le_bytes([c[0], c[1]]))
                .collect(),
        ),
        _ => None,
    }
}

unsafe fn rowcache_reg_meta(relid: pg_sys::Oid) -> Option<RegMeta> {
    let attnums = reg_attnums(relid.as_u32())?;
    rowcache_meta_for_attnums(relid, &attnums)
}

/// The catalogue half of `rowcache_reg_meta`, for a pk column already known.
///
/// Split out because the registration entry lives in the row cache and is
/// evictable like any other entry, so a busy cache can lose it. Execution must
/// not depend on that: the planner knew the attnum when it chose this scan, and
/// carries it in `custom_private`, so the column is resolved from the catalogues
/// here rather than looked up in a cache that may since have dropped it.
unsafe fn rowcache_meta_for_attnums(relid: pg_sys::Oid, attnums: &[i16]) -> Option<RegMeta> {
    if attnums.is_empty() {
        return None;
    }
    let mut cols = Vec::with_capacity(attnums.len());
    let mut typenames = Vec::with_capacity(attnums.len());
    for &attnum in attnums {
        let attname = pg_sys::get_attname(relid, attnum, false);
        if attname.is_null() {
            return None;
        }
        cols.push(CStr::from_ptr(attname).to_string_lossy().into_owned());
        let coltypid = pg_sys::get_atttype(relid, attnum);
        if coltypid == pg_sys::InvalidOid {
            return None;
        }
        let tn = pg_sys::format_type_be(coltypid);
        typenames.push(CStr::from_ptr(tn).to_string_lossy().into_owned());
        pg_sys::pfree(tn as *mut c_void);
    }
    let rel_q = Spi::get_one_with_args::<String>(
        "SELECT $1::regclass::text",
        vec![(PgBuiltInOids::OIDOID.oid(), relid.into_datum())],
    )
    .ok()
    .flatten()?;
    Some(RegMeta { cols, typenames, rel_q })
}

// ---- the SQL surface: in-backend shared-memory reads/writes ---------

#[pg_schema]
mod supacache {
    use super::*;

    /// Direct shared-memory read in the calling backend — no socket, no copy
    /// beyond the returned value. This is the path a `supatype_mask` read
    /// predicate would use for a permission-set lookup.
    ///
    /// STABLE, never IMMUTABLE: forbids anything downstream of a mask
    /// predicate from being folded/cached by identity — an IMMUTABLE cache read
    /// would let the planner bake one caller's value into a generic plan.
    /// Reads through the entry's seqlock rather than borrowing the shared
    /// bytes directly: this runs in an ordinary backend, not the worker that
    /// owns the partition, so the value can be rewritten underneath it. A
    /// plain borrow could return the head of one value and the tail of the
    /// next.
    #[pg_extern(stable, parallel_safe)]
    fn get(key: &str) -> Option<Vec<u8>> {
        store_view_for_key(key.as_bytes())?.get_stable(key.as_bytes())
    }

    #[pg_extern]
    fn set(key: &str, val: &[u8], ttl_seconds: default!(i64, 0)) -> bool {
        let store = match store_view_for_key(key.as_bytes()) {
            Some(s) => s,
            None => return false,
        };
        let ttl_micros = if ttl_seconds > 0 {
            ttl_seconds * 1_000_000
        } else {
            0
        };
        store.set(key.as_bytes(), val, ttl_micros)
    }

    #[pg_extern]
    fn incr(key: &str, by: default!(i64, 1)) -> Option<i64> {
        store_view_for_key(key.as_bytes()).and_then(|s| s.incr(key.as_bytes(), by))
    }

    #[pg_extern]
    fn del(key: &str) -> bool {
        store_view_for_key(key.as_bytes())
            .map(|s| s.del(key.as_bytes()))
            .unwrap_or(false)
    }

    #[pg_extern]
    fn getset(key: &str, val: &[u8]) -> Option<Vec<u8>> {
        let store = store_view_for_key(key.as_bytes())?;
        // Seqlock read: this is an ordinary backend, not the worker that owns
        // the partition, so the value can be rewritten mid-read.
        let old = store.get_stable(key.as_bytes());
        store.set(key.as_bytes(), val, 0);
        old
    }

    #[pg_extern]
    fn ping() -> &'static str {
        "PONG"
    }

    /// Whether the `replicated` tier's promise is currently being kept.
    ///
    /// `tier` is the configured durability. `standby_configured` reflects
    /// `synchronous_standby_names`, which is what decides whether Postgres
    /// waits at all; it is `sighup` context, so it can change under a running
    /// server. `sync_standbys_connected` counts standbys in `pg_stat_replication`
    /// currently in a synchronous state.
    ///
    /// `honoured` is the one to alert on: false means acknowledged writes are
    /// not getting the durability the tier advertises. Note that
    /// `standby_configured` true with zero connected standbys is not a silent
    /// downgrade, Postgres blocks the commit instead, which shows up as a
    /// growing `ring_stats().backlog_bytes` rather than as lost durability.
    #[pg_extern]
    fn replication_status() -> TableIterator<
        'static,
        (
            name!(tier, String),
            name!(standby_configured, bool),
            name!(sync_standbys_connected, i64),
            name!(honoured, bool),
        ),
    > {
        let tier = match ks_tier() {
            Tier::Ephemeral => "ephemeral",
            Tier::Relaxed => "relaxed",
            Tier::Durable => "durable",
            Tier::Replicated => "replicated",
        };
        let configured = sync_standby_configured();
        let connected = Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_stat_replication WHERE sync_state IN ('sync','quorum')",
        )
        .ok()
        .flatten()
        .unwrap_or(0);
        // Only the replicated tier makes a replication promise; the others are
        // trivially honoured because they promise nothing about a standby.
        let honoured = !matches!(ks_tier(), Tier::Replicated) || configured;
        TableIterator::once((tier.to_string(), configured, connected, honoured))
    }

    /// Register/replace a RESP AUTH credential, storing a SALTED SHA-256 verifier
    /// (`sha256$<salt>$<hash>`) — the plaintext secret is never written to the
    /// table (hardening). The worker verifies with a constant-time compare
    /// and picks up the change on `SELECT pg_reload_conf()` (hot reload). Salt is
    /// 128 bits from `gen_random_uuid()`. Use this instead of inserting into
    /// `supacache.resp_credential` directly.
    #[pg_extern]
    fn set_credential(username: &str, secret: &str, role: &str, tenant: &str) -> bool {
        let salt = match Spi::get_one::<String>(
            "SELECT replace(gen_random_uuid()::text, '-', '')",
        ) {
            Ok(Some(s)) => s,
            _ => return false,
        };
        let stored = match Spi::get_one_with_args::<String>(
            "SELECT 'sha256$' || $1 || '$' || encode(sha256(decode($1,'hex') || $2::bytea), 'hex')",
            vec![
                (PgBuiltInOids::TEXTOID.oid(), salt.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), secret.into_datum()),
            ],
        ) {
            Ok(Some(s)) => s,
            _ => return false,
        };
        Spi::run_with_args(
            "INSERT INTO supacache.resp_credential(username, secret, role_name, tenant) \
             VALUES ($1, $2, $3, $4) \
             ON CONFLICT (username) DO UPDATE SET \
               secret = EXCLUDED.secret, role_name = EXCLUDED.role_name, tenant = EXCLUDED.tenant",
            Some(vec![
                (PgBuiltInOids::TEXTOID.oid(), username.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), stored.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), role.into_datum()),
                (PgBuiltInOids::TEXTOID.oid(), tenant.into_datum()),
            ]),
        )
        .is_ok()
    }

    /// Persistence ring diagnostics: total writes enqueued, writes dropped due
    /// to ring-full backpressure, and current unconsumed backlog in bytes.
    #[pg_extern]
    /// Health of the persistence path, summed across rings.
    ///
    /// `lag` is `pushed - committed`: acknowledged writes still waiting on
    /// Postgres. In a sync-ack tier it should sit near zero and return there;
    /// a number that climbs and stays is persistence falling behind.
    ///
    /// `failed_batches` is MISNAMED and kept only for compatibility -- use
    /// `supacache.pg_stat_keyspace_persist.uncommitted_batches`, which reports
    /// the same number under a name that matches it.
    ///
    /// It is a GAUGE, not a count of failures. The ring increments it before
    /// attempting a batch and decrements it after the commit succeeds --
    /// bracketing the attempt, because a Postgres ERROR unwinds out of the
    /// worker and an `Err` branch never runs. A batch in flight therefore reads
    /// as one "failed", and under sustained writes this sits at a small number
    /// and oscillates rather than accumulating.
    ///
    /// What it does report is an increment that was never cancelled: a batch
    /// that failed, or whose worker died mid-commit. So the signal is a floor
    /// that does not drain -- when writes stop, this should return to zero, and
    /// whatever remains never committed. Alerting on any nonzero value alerts
    /// on ordinary traffic.
    ///
    /// `unresolved` counts by-reference records whose value could not be read
    /// back. Usually benign, since a key overwritten after staging has a newer
    /// record queued behind it, but it is also what resolving against the wrong
    /// keyspace segment looks like, and that silently loses a durable write.
    fn ring_stats() -> TableIterator<
        'static,
        (
            name!(pushed, i64),
            name!(dropped, i64),
            name!(backlog_bytes, i64),
            name!(committed, i64),
            name!(lag, i64),
            name!(failed_batches, i64),
            name!(unresolved, i64),
        ),
    > {
        let base = RING_BASE.load(Ordering::Acquire);
        let mut rows = Vec::new();
        if !base.is_null() {
            let stride = ring_stride();
            let (mut p, mut d, mut b) = (0i64, 0i64, 0i64);
            let (mut c_, mut e, mut u) = (0i64, 0i64, 0i64);
            for i in 0..ring_count() {
                let c = unsafe { ring::Consumer::attach(base.add(i * stride)) };
                let (pushed, dropped, backlog, committed, errors, unresolved) = c.stats();
                p += pushed as i64;
                d += dropped as i64;
                b += backlog as i64;
                c_ += committed as i64;
                e += errors as i64;
                u += unresolved as i64;
            }
            rows.push((p, d, b, c_, (p - c_).max(0), e, u));
        }
        TableIterator::new(rows)
    }

    /// Source strings `pg_settings.source` reports for a value an *operator*
    /// put there, as opposed to one the caller asserted about itself.
    ///
    /// Deliberately an allow-list. `"session"` (a `SET` in this backend) and
    /// `"client"` (a connection-string `options=`) are the two a caller can
    /// drive, but anything unrecognised is refused too rather than assumed
    /// harmless.
    const TRUSTED_TENANT_SOURCES: [&str; 6] = [
        "configuration file",
        "command line",
        "environment variable",
        "database",      // ALTER DATABASE ... SET
        "user",          // ALTER ROLE ... SET
        "database user", // ALTER ROLE ... IN DATABASE ... SET
    ];

    thread_local! {
        /// Memo of the last `pg_keyspace.tenant` this backend resolved and
        /// whether its source was trusted.
        ///
        /// Keyed on the value, which is what makes it sound: any change of value
        /// misses the memo, and a change of *source* that leaves the value alone
        /// cannot change the answer -- a role re-`SET`ting the value it was
        /// already given publishes to the same place either way.
        static TENANT_SOURCE_MEMO: std::cell::RefCell<Option<(String, bool)>> =
            const { std::cell::RefCell::new(None) };
    }

    /// Whether the current `pg_keyspace.tenant` was put there by an operator.
    ///
    /// `Suset` already stops a role setting it for itself, but PG15+
    /// `GRANT SET ON PARAMETER` can hand that right over -- and a boundary a
    /// single grant removes is the boundary this function exists to avoid.
    /// `pg_settings.source` is the only place the provenance is readable from an
    /// extension: `GetConfigOptionByNum`, which is how `pg_settings` itself gets
    /// it, stopped being exported in PG16. So it costs one lookup, memoised per
    /// value above.
    fn tenant_source_trusted(tenant: &str) -> bool {
        if let Some(hit) = TENANT_SOURCE_MEMO.with(|m| {
            m.borrow()
                .as_ref()
                .filter(|(v, _)| v == tenant)
                .map(|(_, t)| *t)
        }) {
            return hit;
        }
        let src = Spi::get_one::<String>(
            "SELECT source FROM pg_settings WHERE name = 'pg_keyspace.tenant'",
        )
        .ok()
        .flatten()
        .unwrap_or_default();
        let trusted = TRUSTED_TENANT_SOURCES.contains(&src.as_str());
        TENANT_SOURCE_MEMO.with(|m| *m.borrow_mut() = Some((tenant.to_string(), trusted)));
        trusted
    }

    /// The channel `supacache.publish()` may actually publish to, or an error
    /// saying why this caller may not publish at all.
    fn publish_scope(channel: &str) -> String {
        // A superuser is unrestricted here as everywhere else in Postgres, and
        // publishes the name as given. This is exactly what the function did
        // before `pg_keyspace.tenant` existed, so nothing that worked stops
        // working -- and an operator who wants an unscoped admin publish on a
        // cluster that does set the GUC still has one.
        if unsafe { pg_sys::superuser() } {
            return channel.to_string();
        }
        let tenant = GUC_TENANT
            .get()
            .map(|c| c.to_string_lossy().into_owned())
            .unwrap_or_default();
        if tenant.is_empty() {
            error!(
                "supacache.publish() is superuser-only until pg_keyspace.tenant is set: RESP \
                 channels are tenant-scoped, and with no tenant for this backend the function \
                 cannot scope a channel name. Set it in postgresql.conf for a single-tenant \
                 cluster, or per role with ALTER ROLE <role> SET pg_keyspace.tenant = '<tenant>'"
            );
        }
        if !tenant_source_trusted(&tenant) {
            error!(
                "supacache.publish(): pg_keyspace.tenant was set in this session rather than by \
                 the operator, so it says nothing about which tenant the caller is. Set it in \
                 postgresql.conf or with ALTER ROLE ... SET -- a value the caller chooses for \
                 itself is not a tenant scope"
            );
        }
        if tenant.contains(':') {
            error!(
                "supacache.publish(): pg_keyspace.tenant = '{tenant}' contains ':', which is the \
                 tenant prefix separator, so the scope it would produce is ambiguous"
            );
        }
        format!("{tenant}:{channel}")
    }

    /// Publish to RESP subscribers from SQL.
    ///
    /// The gap this closes: a backend can read and write the keyspace through
    /// `supacache.*`, but until now had no way to reach a subscriber. A trigger
    /// that wanted to tell a RESP client something had to go out through the
    /// application and back in over the wire.
    ///
    /// Returns the number of subscribers the message was queued for, the same
    /// count a RESP `PUBLISH` answers with.
    ///
    /// **Who may call it, and under what name.** RESP channels are force-scoped
    /// `{tenant}:` per credential, so a function that published a
    /// caller-supplied name unscoped would let anyone who could call it inject
    /// into any tenant's channels -- and count their subscribers, which is a
    /// read of another tenant's activity through a write-only primitive.
    ///
    /// A Postgres role is not a RESP credential, so the caller's tenant has to
    /// come from somewhere else: `pg_keyspace.tenant`, which only an operator
    /// can set (see that GUC, and `tenant_source_trusted`). Resolved by
    /// `publish_scope`:
    ///
    /// | caller | `pg_keyspace.tenant` | result |
    /// |---|---|---|
    /// | superuser | anything | publishes the name as given, unscoped |
    /// | anyone else | set by the operator | publishes to `{tenant}:{channel}` |
    /// | anyone else | set in this session | refused -- self-asserted |
    /// | anyone else | unset | refused |
    ///
    /// The checks live here rather than in a `GRANT`, which would put the
    /// boundary outside the code, one well-meaning grant away from being gone.
    #[pg_extern]
    fn publish(channel: &str, message: &[u8]) -> i64 {
        // Before anything else, including the no-bus early return: a caller who
        // may not publish is told so, not quietly told nobody was listening.
        let scoped = publish_scope(channel);
        let bus = match BUS.get() {
            Some(b) => b,
            // The in-process bus (standalone daemon) has no shared backing, and
            // an unconfigured cluster has no bus at all. Either way there is no
            // subscriber a backend could reach.
            None => return 0,
        };
        let lock = PS_EXT_LOCK.load(Ordering::Acquire);
        if lock.is_null() {
            error!("supacache.publish(): the pub/sub bus is not initialised in this cluster");
        }
        // Held across the push only. Contends with other `supacache.publish()`
        // callers and with nothing else: no slot worker ever produces into the
        // rings this lane uses.
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        let n = bus.publish_external(scoped.as_bytes(), message, |p, c| {
            server::glob_match(p, c)
        });
        unsafe { pg_sys::LWLockRelease(lock) };
        n.unwrap_or(0) as i64
    }

    /// The endpoint a peer's relay worker calls. **Not** for applications.
    ///
    /// `supacache.publish()` is the front door: it scopes the channel to the
    /// caller's tenant and, where `pg_keyspace.relay_channels` matches, hands
    /// the message to the relay worker for the peers. This is the back door a
    /// peer arrives through, and it differs in exactly two ways, both of which
    /// are why it is a separate function rather than a flag on the other one:
    ///
    /// * the channel arrives **already scoped** -- the publishing instance
    ///   applied its own tenant prefix, and re-applying this instance's would
    ///   deliver into the wrong namespace or nowhere;
    /// * it **never relays onward**. That is what stops A -> B -> A. A message
    ///   crosses at most one instance boundary, always, and the rule is
    ///   structural rather than a hop count that has to be right.
    ///
    /// Superuser-only, like the relay connection itself. An operator who wants
    /// a peer link that cannot publish into arbitrary namespaces gives it a
    /// tenant-scoped role and lets it call `publish()` instead; this function
    /// is for a trusted relay, and says so.
    #[pg_extern]
    fn publish_relayed(channel: &str, message: &[u8]) -> i64 {
        if !unsafe { pg_sys::superuser() } {
            error!(
                "supacache.publish_relayed() is superuser-only: it publishes a \
                 pre-scoped channel name without applying this instance's tenant \
                 scope, which is safe only for a peer relay. Applications want \
                 supacache.publish()"
            );
        }
        let bus = match BUS.get() {
            Some(b) => b,
            None => return 0,
        };
        let lock = PS_EXT_LOCK.load(Ordering::Acquire);
        if lock.is_null() {
            error!("supacache.publish_relayed(): the pub/sub bus is not initialised in this cluster");
        }
        unsafe { pg_sys::LWLockAcquire(lock, pg_sys::LWLockMode::LW_EXCLUSIVE) };
        let n = bus.publish_external(channel.as_bytes(), message, |p, c| {
            server::glob_match(p, c)
        });
        unsafe { pg_sys::LWLockRelease(lock) };
        n.unwrap_or(0) as i64
    }

    /// Health of the cross-worker pub/sub bus.
    ///
    /// Every column counts a message that was not delivered, which is the part
    /// pub/sub cannot report for itself: PUBLISH answers with a subscriber
    /// count, and a message dropped on the way to another worker still leaves
    /// that count looking plausible to the client that sent it.
    ///
    /// `dropped` is a publish that did not fit in the target worker's queue.
    /// Dropping is deliberate, since blocking a publisher on a worker that is
    /// not draining would turn one stalled subscriber into a stalled keyspace,
    /// but a number climbing here means subscribers are missing messages and
    /// `pg_keyspace.pubsub_ring_kb` is too small for the burst.
    ///
    /// `route_full` is a subscription refused because the routing table was
    /// full: raise `pg_keyspace.pubsub_routes`. Those clients are subscribed as
    /// far as they know and will receive nothing.
    ///
    /// `name_too_long` is a channel or pattern longer than the table stores.
    /// Such a subscription is refused rather than truncated, because truncating
    /// would merge two channels into one and cross their traffic.
    #[pg_extern(stable, parallel_safe)]
    fn pubsub_stats() -> TableIterator<
        'static,
        (
            name!(dropped, i64),
            name!(route_full, i64),
            name!(name_too_long, i64),
        ),
    > {
        let base = PUBSUB_BASE.load(Ordering::Acquire);
        let mut rows = Vec::new();
        if !base.is_null() {
            if let Some(bus) = unsafe { pubsub_shm::ShmBus::attach(base) } {
                let (d, r, n) = bus.stats();
                rows.push((d as i64, r as i64, n as i64));
            }
        }
        TableIterator::new(rows)
    }

    /// The worker layout the persisted keyspace was last written under, against
    /// the one running now, and what a change between them costs (#101).
    ///
    /// `slots_moved` is the number that matters and it is not intuitive:
    /// doubling the worker count moves 87.5% of slots, not half, because the
    /// ranges are contiguous and only worker 0's first sub-range keeps its
    /// owner.
    #[pg_extern]
    fn topology_change() -> TableIterator<
        'static,
        (
            name!(recorded_workers, Option<i32>),
            name!(running_workers, i32),
            name!(slots_moved, i32),
            name!(pct_moved, f64),
        ),
    > {
        let running = super::worker_count();
        // Guarded for the same reason as rowcache_registration_status(): the
        // worker creates supacache.topology, so a scrape can arrive before it
        // exists, and an erroring stats view reads as an outage (#111).
        let recorded = if table_exists("supacache.topology") {
            Spi::get_one::<i32>("SELECT workers FROM supacache.topology WHERE id = 1")
                .ok()
                .flatten()
        } else {
            None
        };
        let moved = recorded
            .filter(|p| *p > 0)
            .map(|p| crc16::slots_moved(p as usize, running))
            .unwrap_or(0);
        let pct = moved as f64 * 100.0 / crc16::NUM_SLOTS as f64;
        TableIterator::once((recorded, running as i32, moved as i32, pct))
    }

    /// The slot range and RESP port of every shared-nothing slot worker.
    ///
    /// This is the routing table a client must follow: worker `w` serves exactly
    /// the keys whose CRC16 slot falls in `slot_lo ..= slot_hi`, on `port` --
    /// inclusive at both ends, the same convention `CLUSTER SLOTS` reports. It is
    /// also the mapping crash recovery uses to put each persisted key back into
    /// the segment that will serve it, so a client that shards by these ranges
    /// gets its data back after a restart, on the same worker.
    #[pg_extern(stable, parallel_safe)]
    fn slot_ranges() -> TableIterator<
        'static,
        (
            name!(worker, i32),
            name!(port, i32),
            name!(slot_lo, i32),
            name!(slot_hi, i32),
        ),
    > {
        let n = worker_count();
        let base = GUC_PORT.get();
        let rows: Vec<(i32, i32, i32, i32)> = (0..n)
            .map(|w| {
                let (lo, hi) = crc16::slot_range(w, n);
                // INCLUSIVE, matching CLUSTER SLOTS / SHARDS / NODES, which all
                // emit `hi - 1`. crc16::slot_range is half-open, and reporting
                // it raw made adjacent workers overlap -- worker 0 ending at
                // 4096 and worker 1 starting at 4096 -- so a client sharding
                // from this table sent boundary slots to the wrong worker. The
                // doc comment said `[slot_lo, slot_hi)`, but a table whose rows
                // overlap is read, not read about (#121).
                (w as i32, base + w as i32, lo as i32, hi as i32 - 1)
            })
            .collect();
        TableIterator::new(rows)
    }

    /// Which slot worker owns `key`, by the same function the RESP workers, the
    /// SQL surface, and crash recovery use. Pairs with `slot_ranges()` for
    /// routing a key to its port.
    #[pg_extern(stable, parallel_safe)]
    fn key_worker(key: &str) -> i32 {
        crc16::key_owner(key.as_bytes(), worker_count()) as i32
    }

    // ---- in-backend micro-benchmarks --------------------------------
    // These time the raw shared-memory op inside the calling backend, with no
    // client protocol round-trip, so they isolate the ~1-2µs claim from the
    // ~30-50µs libpq round-trip that a plain `SELECT supacache.get()` incurs.

    /// Returns nanoseconds per `get` hit, averaged over `iters` iterations.
    #[pg_extern]
    fn bench_get(key: &str, val: &[u8], iters: i64) -> f64 {
        let store = match store_view_for_key(key.as_bytes()) {
            Some(s) => s,
            None => return -1.0,
        };
        store.set(key.as_bytes(), val, 0);
        let k = key.as_bytes();
        let n = iters.max(1);
        let t0 = std::time::Instant::now();
        let mut acc = 0u64;
        for _ in 0..n {
            if let Lookup::Hit(v) = store.get(std::hint::black_box(k)) {
                acc = acc.wrapping_add(v.len() as u64);
            }
        }
        let el = t0.elapsed();
        std::hint::black_box(acc);
        el.as_nanos() as f64 / n as f64
    }

    /// Returns nanoseconds per `set` (overwrite in place), over `iters`.
    #[pg_extern]
    fn bench_set(key: &str, val: &[u8], iters: i64) -> f64 {
        let store = match store_view_for_key(key.as_bytes()) {
            Some(s) => s,
            None => return -1.0,
        };
        let k = key.as_bytes();
        let n = iters.max(1);
        let t0 = std::time::Instant::now();
        for _ in 0..n {
            store.set(std::hint::black_box(k), std::hint::black_box(val), 0);
        }
        t0.elapsed().as_nanos() as f64 / n as f64
    }

    /// Returns nanoseconds per `incr`, over `iters`.
    #[pg_extern]
    fn bench_incr(key: &str, iters: i64) -> f64 {
        let store = match store_view_for_key(key.as_bytes()) {
            Some(s) => s,
            None => return -1.0,
        };
        let k = key.as_bytes();
        let n = iters.max(1);
        let t0 = std::time::Instant::now();
        for _ in 0..n {
            store.incr(std::hint::black_box(k), 1);
        }
        t0.elapsed().as_nanos() as f64 / n as f64
    }

    /// Preload `n` keys of `val_bytes` each, so RESP GET benchmarks hit.
    #[pg_extern]
    fn warm(n: i64, val_bytes: i64) -> i64 {
        // Each key goes into the segment that owns it, so a RESP GET on the
        // worker a cluster client routes to actually hits.
        let nworkers = worker_count();
        let views: Vec<Option<Store>> = (0..nworkers).map(store_view_for_worker).collect();
        if views.iter().all(|v| v.is_none()) {
            return 0;
        }
        let val = vec![b'x'; val_bytes.max(1) as usize];
        let mut done = 0i64;
        for i in 0..n.max(0) {
            let key = format!("key:{i}");
            let owner = crc16::key_owner(key.as_bytes(), nworkers);
            if let Some(Some(store)) = views.get(owner) {
                if store.set(key.as_bytes(), &val, 0) {
                    done += 1;
                }
            }
        }
        done
    }

    // ---- Mode B: transparent row cache control surface ------------
    // The planner custom scan (see the parent module) substitutes a cached row
    // for a `pk = Const` lookup on a *registered* relation. These functions
    // register a relation's pk column and populate the cache. Populating here is
    // the explicit warm/backfill path; the logical-decoding invalidation/refill
    // worker maintains the cache live. It stores the RAW heap-tuple bytes
    // so the scan node re-applies RLS + mask above it (never post-policy output).

    /// Register `tbl`'s primary-key attribute number so the planner hook will
    /// consider substituting cached rows for `pk = Const` lookups on it.
    #[pg_extern]
    fn rowcache_register(tbl: &str, pk_attnum: i32) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        // Guard: the named column must be the table's ENTIRE primary key.
        // Registering one column of a composite key would let the planner match
        // a query constraining that column alone to a row cached under the whole
        // key -- serving one row where the query asks for a set, which is an
        // incoherence rather than a miss.
        //
        // Composite keys are supported, but not through this entry point: there
        // is no sensible single `pk_attnum` for them, so they go through the
        // one-argument `rowcache_register(tbl)`, which takes the key from the
        // catalogue. Any pk type is allowed (int, uuid, text, …).
        let ok_pk = Spi::get_one_with_args::<bool>(
            "SELECT EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = $1 \
               AND i.indisprimary AND i.indnkeyatts = 1 AND i.indkey[0] = $2)",
            vec![
                (PgBuiltInOids::OIDOID.oid(), relid.into_datum()),
                (PgBuiltInOids::INT2OID.oid(), (pk_attnum as i16).into_datum()),
            ],
        )
        .ok()
        .flatten()
        .unwrap_or(false);
        if !ok_pk {
            return false; // not a single-column primary key on that attnum
        }
        rowcache_store_registration(relid, &[pk_attnum as i16])
    }

    /// Register a table's whole primary key, whatever its arity — the general
    /// form of `rowcache_register`, and the only one that can register a
    /// composite key.
    ///
    /// Takes no attnum because there is nothing for the caller to choose: the
    /// key is whatever `pg_index` says it is. The two-argument form remains for
    /// callers that want to state the column explicitly, and still refuses
    /// anything but a single-column key.
    #[pg_extern(name = "rowcache_register")]
    fn rowcache_register_pk(tbl: &str) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        // Ascending attnum order, which is what the decode plugin emits its key
        // parts in. Index-column order would not do: a primary key declared
        // (b, a) has indkey [b, a] but attnums [a, b], and the plugin reads its
        // columns out of a bitmapset, which is inherently ascending. Sorting
        // here is what lets neither side describe its ordering to the other.
        let attnums: Vec<i16> = match Spi::get_one_with_args::<Vec<i16>>(
            "SELECT array_agg(k ORDER BY k)::smallint[] \
             FROM pg_index i, unnest(i.indkey[0:i.indnkeyatts-1]) k \
             WHERE i.indrelid = $1 AND i.indisprimary AND k > 0",
            vec![(PgBuiltInOids::OIDOID.oid(), relid.into_datum())],
        ) {
            Ok(Some(v)) if !v.is_empty() => v,
            _ => return false, // no primary key, or one over a system column
        };
        rowcache_store_registration(relid, &attnums)
    }

    /// Store a registration: the pk attnums packed little-endian, ascending.
    ///
    /// Pinned, because a registration is configuration, not cache content.
    /// Stored as an ordinary entry it competed with the cached rows for the same
    /// arena, so a busy cache evicted it and `rc_pathlist_hook` then stopped
    /// substituting for the table -- caching silently turned itself off under
    /// exactly the load it exists to serve (#87).
    fn rowcache_store_registration(relid: pg_sys::Oid, attnums: &[i16]) -> bool {
        if attnums.is_empty() {
            return false;
        }
        let view = match rowcache_view() {
            Some(v) => v,
            None => return false,
        };
        // Any database may register (#120, subsuming #118). What used to be
        // refused here was not a limitation of the catalogue or of the keys but
        // of INVALIDATION: the one slot lived in `pg_keyspace.database`, and a
        // logical slot only ever decodes changes from the database it belongs
        // to, so a table registered anywhere else would be cached and then never
        // invalidated -- stale indefinitely, with coherence still reporting
        // healthy because it described the worker rather than your table.
        //
        // Every database now gets its own slot and its own turn in a bounded
        // pool, so the only thing left to refuse is a database the cluster
        // cannot actually serve. Publishing into the directory is what says "a
        // worker should come and drain this one", and it is done BEFORE the
        // registration is recorded: a directory entry with no registration is a
        // database a worker visits, probes, and marks idle -- free. A
        // registration with no directory entry would be a cached table nothing
        // invalidates, which is the failure this whole issue exists to remove.
        //
        // `::text` is load-bearing: current_database() returns `name`, and
        // reading that as a String comes back empty.
        let here = Spi::get_one::<String>("SELECT current_database()::text")
            .ok()
            .flatten()
            .unwrap_or_default();
        if rcdb_publish(rc_db(), &here, DB_PARTICIPATING).is_none() {
            warning!(
                "pg_keyspace: the row-cache database directory is full at {} entries, so \
                 database '{here}' cannot be served and a table registered here would be \
                 cached with nothing to invalidate it. Refused. Raise \
                 pg_keyspace.rowcache_max_databases (it costs 128 bytes per entry) and \
                 restart.",
                rcdb_count()
            );
            return false;
        }
        // One replication slot per participating database, and slots are a
        // cluster-wide resource with a low default ceiling. Warn rather than
        // refuse: the slot is created by a worker, not here, and refusing on a
        // count that may have changed by then would be guessing. A database that
        // does not get its slot reads as INCOHERENT rather than healthy, so the
        // failure is safe either way -- it is just easier to fix if somebody is
        // told about it at the moment they ask for it.
        if let Some(short) = replication_slots_short() {
            warning!(
                "pg_keyspace: {short}. The row cache needs one replication slot per \
                 database that registers a table, and a database that cannot get one is \
                 marked incoherent and served from the heap instead of the cache."
            );
        }
        // The catalogue is created by a background worker, so between cluster
        // start and that worker's first pass -- and after a DROP/CREATE
        // EXTENSION -- there is a window where it is absent and registration
        // failed with `relation "supacache.rowcache_reg" does not exist`.
        // Creating it here closes that window, and is race-safe (#130).
        if !ensure_rowcache_catalogue() {
            warning!(
                "pg_keyspace: could not create supacache.rowcache_reg in this database, \
                 so the registration cannot be recorded durably and is refused. A \
                 registration that lives only in shared memory is lost by the next \
                 segment reinitialisation, with the table then silently not cached (#103)."
            );
            return false;
        }
        // The catalogue first, and a failure here fails the call. Pinning
        // succeeds far more often than it survives: a segment reinitialisation
        // takes the pinned entry with it, and before #103 the caller was told
        // the registration had succeeded and the table then silently stopped
        // being cached. Durable first means the worst case is a registration
        // that is recorded and not yet loaded -- a miss, which is safe -- rather
        // than one that is loaded and not recorded.
        let recorded = Spi::run_with_args(
            // Schema-qualified explicitly. `$1::regclass::text` renders
            // relative to the *writer's* search_path, so a table registered as
            // `public.reg` comes back as bare `reg` -- which a reload running
            // under a different search_path could fail to resolve, or resolve
            // to a different table of the same name in another schema.
            "INSERT INTO supacache.rowcache_reg(tbl, attnums) \
             SELECT format('%I.%I', n.nspname, c.relname), $2 \
             FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1 \
             ON CONFLICT (tbl) DO UPDATE SET attnums = EXCLUDED.attnums, registered_at = now()",
            Some(vec![
                (PgBuiltInOids::OIDOID.oid(), relid.into_datum()),
                (PgBuiltInOids::INT2ARRAYOID.oid(), attnums.to_vec().into_datum()),
            ]),
        )
        .is_ok();
        if !recorded {
            return false;
        }
        let packed: Vec<u8> = attnums.iter().flat_map(|a| a.to_le_bytes()).collect();
        if !rowcache_write(&view, &rc_reg_key(relid.as_u32()), || view.set_pinned(&rc_reg_key(relid.as_u32()), &packed), false) {
            return false;
        }
        // Read it back rather than trusting the write. set_pinned already
        // reports the entry being lost before its flag landed; this also covers
        // the segment going away underneath the whole call. It narrows the
        // window rather than closing it -- a reinit one instruction later still
        // loses the entry -- which is why the catalogue above is the real fix
        // and this is only the fast failure.
        reg_attnums(relid.as_u32()).is_some()
    }

    /// Which pk columns a table is registered with, ascending by attnum. Empty
    /// if it is not registered.
    #[pg_extern]
    fn rowcache_registration(tbl: &str) -> Vec<String> {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return Vec::new(),
        };
        let attnums = match reg_attnums(relid.as_u32()) {
            Some(a) => a,
            None => return Vec::new(),
        };
        unsafe {
            rowcache_meta_for_attnums(relid, &attnums)
                .map(|m| m.cols)
                .unwrap_or_default()
        }
    }

    /// Drop a relation's registration; the planner stops substituting for it.
    #[pg_extern]
    fn rowcache_unregister(tbl: &str) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        // The catalogue row goes too, or the next reload would bring the
        // registration back from the dead.
        let _ = Spi::run_with_args(
            "DELETE FROM supacache.rowcache_reg WHERE tbl = (\
               SELECT format('%I.%I', n.nspname, c.relname) FROM pg_class c \
               JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.oid = $1)",
            Some(vec![(PgBuiltInOids::OIDOID.oid(), relid.into_datum())]),
        );
        let dropped = rowcache_view()
            .map(|v| rowcache_write(&v, &rc_reg_key(relid.as_u32()), || v.del(&rc_reg_key(relid.as_u32())), false))
            .unwrap_or(false);
        // Tell the directory straight away when that was the last one, rather
        // than waiting for a pool worker's next probe to notice. Retiring a
        // database is not tidiness: its slot retains WAL until it is consumed,
        // and one kept for a database nobody caches any more pins WAL for the
        // whole cluster with nothing to show for it. The worker still does the
        // dropping -- this only shortens how long it takes to find out.
        let left = Spi::get_one::<i64>("SELECT count(*) FROM supacache.rowcache_reg")
            .ok()
            .flatten()
            .unwrap_or(1);
        if left == 0 {
            if let Some(s) = rcdb_find(rc_db()) {
                s.registrations.store(0, Ordering::Release);
                s.state.store(DB_IDLE, Ordering::Release);
            }
        }
        dropped
    }

    /// Reload registrations from `supacache.rowcache_reg` into the row-cache
    /// segment, returning how many were loaded.
    ///
    /// The invalidation worker does this by itself whenever it finds a fresh
    /// segment, so this is for forcing the issue: after restoring a dump, or to
    /// assert in a test that the catalogue really is the source of truth.
    #[pg_extern]
    fn rowcache_reload_registrations() -> i64 {
        load_registrations_spi()
    }

    /// Cache the current row for `tbl` where the registered pk column = `pk`.
    /// `pk` is `anyelement`, so any pk type works: `rowcache_put('t', 1)`,
    /// `rowcache_put('t', 'a1b2…'::uuid)`, `rowcache_put('t', 'key')`. The row
    /// is keyed by its *canonical* pk (the column type's output text), matching
    /// the planner hook and the WAL decode path. Stores the heap-tuple bytes
    /// (pre-policy), flattened so any out-of-line (TOASTed) column is pulled
    /// inline and the cached row is self-contained (see `fetch_row_and_pk`).
    #[pg_extern]
    fn rowcache_put(tbl: &str, pk: AnyElement) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        unsafe {
            let meta = match rowcache_reg_meta(relid) {
                Some(m) => m,
                None => return false,
            };
            // Single-column form: canonicalise the caller's value the same way
            // every other side does, then take the one shared lookup path.
            if meta.cols.len() != 1 {
                return false; // composite key: use rowcache_put_pk
            }
            let canon = canon_pk(pk.oid(), pk.datum());
            match fetch_row_and_pk(&meta, std::slice::from_ref(&canon), false) {
                Some((raw, canon)) if !canon.is_empty() => {
                    if let Some(view) = rowcache_view() {
                        return rowcache_write(
                            &view,
                            &rc_key(relid.as_u32(), &canon),
                            || view.set(&rc_key(relid.as_u32(), &canon), &raw, 0),
                            false,
                        );
                    }
                    false
                }
                _ => false,
            }
        }
    }

    /// `rowcache_put` for a composite primary key.
    ///
    /// Parts are the canonical text of each pk column, in ascending attnum
    /// order — the same order `rowcache_registration` reports. Text rather than
    /// a typed value because a composite key has several types and SQL has no
    /// heterogeneous array; each part is cast to its column's type in the
    /// lookup, exactly as the decode worker's parts are.
    #[pg_extern]
    fn rowcache_put_pk(tbl: &str, pk: Vec<Option<String>>) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        let parts: Vec<Vec<u8>> = pk
            .into_iter()
            .map(|p| p.unwrap_or_default().into_bytes())
            .collect();
        unsafe {
            let meta = match rowcache_reg_meta(relid) {
                Some(m) => m,
                None => return false,
            };
            if parts.len() != meta.cols.len() {
                return false;
            }
            match fetch_row_and_pk(&meta, &parts, false) {
                Some((raw, canon)) if !canon.is_empty() => rowcache_view()
                    .map(|v| {
                        rowcache_write(&v, &rc_key(relid.as_u32(), &canon), || v.set(&rc_key(relid.as_u32(), &canon), &raw, 0), false)
                    })
                    .unwrap_or(false),
                _ => false,
            }
        }
    }

    /// `rowcache_cached_has_external` for a composite primary key, taking the
    /// same canonical-text parts as `rowcache_put_pk`. NULL when not cached.
    #[pg_extern]
    fn rowcache_cached_pk_has_external(tbl: &str, pk: Vec<Option<String>>) -> Option<bool> {
        let relid = Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        )
        .ok()
        .flatten()?;
        let parts: Vec<Vec<u8>> = pk
            .into_iter()
            .map(|p| p.unwrap_or_default().into_bytes())
            .collect();
        let view = rowcache_view()?;
        match view.get(&rc_key(relid.as_u32(), &compose_pk(&parts))) {
            Lookup::Hit(bytes)
                if bytes.len() >= std::mem::size_of::<pg_sys::HeapTupleHeaderData>() =>
            {
                let hdr = bytes.as_ptr() as *const pg_sys::HeapTupleHeaderData;
                let infomask = unsafe { (*hdr).t_infomask };
                Some(infomask & pg_sys::HEAP_HASEXTERNAL as u16 != 0)
            }
            _ => None,
        }
    }

    /// Diagnostic: does the *cached* copy of `tbl`'s row `pk` still hold an
    /// out-of-line (TOASTed) value? Reads the stored tuple's info-mask. Returns
    /// None if the row isn't cached. After `rowcache_put` this is always `false`
    /// even when the live row has external values — proof the cache flattened
    /// them inline (see `bench/run_toast.sh`).
    #[pg_extern]
    fn rowcache_cached_has_external(tbl: &str, pk: AnyElement) -> Option<bool> {
        let relid = Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        )
        .ok()
        .flatten()?;
        let view = rowcache_view()?;
        let canon = unsafe { canon_pk(pk.oid(), pk.datum()) };
        match view.get(&rc_key(relid.as_u32(), &canon)) {
            Lookup::Hit(bytes) if bytes.len() >= std::mem::size_of::<pg_sys::HeapTupleHeaderData>() => {
                let hdr = bytes.as_ptr() as *const pg_sys::HeapTupleHeaderData;
                let infomask = unsafe { (*hdr).t_infomask };
                Some(infomask & pg_sys::HEAP_HASEXTERNAL as u16 != 0)
            }
            _ => None,
        }
    }

    /// Whether the row cache is currently trusted **in this database**, and how
    /// long it has been since anything invalidated it.
    ///
    /// The point of #39 was that a stopped invalidation worker left the cache
    /// serving stale rows "indefinitely with no alarm". Reads now fail closed on
    /// their own, but an operator still needs to be able to see it, and a test
    /// needs to be able to wait for invalidation to come up rather than sleep
    /// and hope.
    ///
    /// One row, for the database you are connected to (#120). That is not a
    /// simplification: coherence IS per-database now, because each database has
    /// its own slot and its own turn, and the answer for one says nothing about
    /// another. One database's stalled worker must not read as the whole cache
    /// being incoherent, nor a healthy pool as every database being current.
    /// `supacache.pg_stat_keyspace_rowcache_databases` is the cluster-wide view.
    ///
    /// `beat_age_ms` is how long since this database was last drained, and is
    /// NULL when it never has been -- nothing has started yet, or this database
    /// has no registrations and so gets no slot and no turn. `stale_after_ms` is
    /// the window it is judged against, which grows with the number of
    /// participating databases when they outnumber the pool, because a database
    /// waiting its turn is behind by the cycle time BY DESIGN. `slot_lost` is
    /// the other way to be incoherent: the server cut this database's slot loose
    /// for retaining too much WAL, so an unknown set of invalidations was never
    /// delivered.
    #[pg_extern]
    fn rowcache_coherence() -> TableIterator<
        'static,
        (
            name!(coherent, bool),
            name!(decode_enabled, bool),
            name!(beat_age_ms, Option<i64>),
            name!(stale_after_ms, i64),
            name!(datname, String),
            name!(slot_lost, bool),
            name!(participating_databases, i64),
        ),
    > {
        let decode = GUC_ROWCACHE_DECODE.get();
        let me = rcdb_find(rc_db());
        let age = me.and_then(|s| {
            let last = s.last_drained_us.load(Ordering::Acquire);
            if last == 0 {
                None
            } else {
                Some(store::now_micros().saturating_sub(last) / 1000)
            }
        });
        let datname = Spi::get_one::<String>("SELECT current_database()::text")
            .ok()
            .flatten()
            .unwrap_or_default();
        TableIterator::once((
            rowcache_coherent(),
            decode,
            age,
            rcdb_stale_us() / 1000,
            datname,
            me.map(|s| s.slot_lost.load(Ordering::Acquire) != 0).unwrap_or(false),
            rcdb_participating() as i64,
        ))
    }

    /// One row per database the row cache knows about (#120).
    ///
    /// `supacache.rowcache_coherence()` answers for the database you happen to
    /// be connected to. This answers for the cluster, which is the question an
    /// operator actually has once more than one database is served: **which**
    /// database is incoherent, and why.
    ///
    /// It is not a dashboard nicety. The cache fails closed on incoherence, so
    /// "which database is incoherent" decides which queries are served from the
    /// cache at all. A single arbitrary database's numbers reported as the
    /// cluster's would be worse than no numbers.
    ///
    /// `state` is `participating` (has registrations, needs a slot and a turn),
    /// `idle` (probed, no registrations, deliberately gets neither), or
    /// `unknown` (not probed yet). `worker_pid` is the pool worker holding this
    /// database right now, and is NULL while it waits its turn -- which is
    /// normal under cycling, not a fault.
    #[pg_extern]
    fn rowcache_databases() -> TableIterator<
        'static,
        (
            name!(datoid, i64),
            name!(datname, String),
            name!(state, String),
            name!(registrations, i64),
            name!(coherent, bool),
            name!(slot_lost, bool),
            name!(beat_age_ms, Option<i64>),
            name!(stale_after_ms, i64),
            name!(slot_name, String),
            name!(worker_pid, Option<i32>),
        ),
    > {
        let now = store::now_micros();
        let stale = rcdb_stale_us();
        let base = rowcache_slot_base();
        let decode = GUC_ROWCACHE_DECODE.get();
        let mut rows = Vec::new();
        for i in 0..rcdb_count() {
            let s = match rcdb_at(i) {
                Some(s) => s,
                None => continue,
            };
            let oid = s.datoid.load(Ordering::Acquire);
            if oid == 0 {
                continue;
            }
            let state = match s.state.load(Ordering::Acquire) {
                DB_PARTICIPATING => "participating",
                DB_IDLE => "idle",
                _ => "unknown",
            };
            let lost = s.slot_lost.load(Ordering::Acquire) != 0;
            let last = s.last_drained_us.load(Ordering::Acquire);
            let age = if last == 0 {
                None
            } else {
                Some(now.saturating_sub(last) / 1000)
            };
            let pid = s.owner_pid.load(Ordering::Acquire);
            rows.push((
                oid as i64,
                rcdb_name(s),
                state.to_string(),
                s.registrations.load(Ordering::Acquire),
                // The same test `rowcache_coherent()` applies, evaluated for a
                // database other than the caller's. Written once here rather
                // than duplicated, so the view can never disagree with what the
                // planner hook actually does.
                !decode || (!lost && last != 0 && now.saturating_sub(last) < stale),
                lost,
                age,
                stale / 1000,
                slot_name_for(&base, oid),
                if pid == 0 { None } else { Some(pid as i32) },
            ));
        }
        rows.sort_by(|a, b| a.1.cmp(&b.1));
        TableIterator::new(rows)
    }

    /// Row-cache occupancy: entries (registrations + rows), bytes used/cap.
    #[pg_extern]
    fn rowcache_stats() -> TableIterator<
        'static,
        (
            name!(entries, i64),
            name!(hits, i64),
            name!(misses, i64),
            name!(data_used, i64),
            name!(data_cap, i64),
        ),
    > {
        let mut rows = Vec::new();
        if let Some(view) = rowcache_view() {
            // Summed across partitions, not read from partition 0. The segment
            // is carved into `pg_keyspace.rowcache_partitions` of them so that
            // writers can take one lock each (#120), and reading a single one
            // would report a fraction of the cache as the whole of it -- an
            // arena that looks 8x too small, a hit rate computed from an eighth
            // of the traffic, and `data_used` that never approaches `data_cap`.
            let (mut entries, mut hits, mut misses, mut used, mut cap) = (0i64, 0i64, 0i64, 0i64, 0i64);
            for p in 0..view.num_partitions() {
                let s = view.stats(p);
                entries += s.entries as i64;
                hits += s.hits as i64;
                misses += s.misses as i64;
                used += s.data_used as i64;
                cap += s.data_cap as i64;
            }
            rows.push((entries, hits, misses, used, cap));
        }
        TableIterator::new(rows)
    }

    /// Per (slot worker, partition) counters and arena occupancy, with the
    /// worker index reported as its own column (#111).
    ///
    /// `stats()` fuses the two into one `partition` index, which is fine to
    /// read by eye and useless to a collector: undoing it needs the partition
    /// count, which is a GUC the collector does not have. This is the same data
    /// with the join key present, and it is what
    /// `supacache.pg_stat_keyspace_workers` is built on.
    ///
    /// `hits`, `misses`, `evictions`, `sets`, `tombstones` and `rehashes` are
    /// CUMULATIVE since the segment was created, the way Postgres's own stats
    /// counters are, so a collector's rate() is meaningful. `entries`,
    /// `arena_used_bytes` and `arena_capacity_bytes` are GAUGES.
    #[pg_extern(stable, parallel_safe)]
    fn worker_stats() -> TableIterator<
        'static,
        (
            name!(worker, i32),
            name!(partition, i32),
            name!(entries, i64),
            name!(hits, i64),
            name!(misses, i64),
            name!(evictions, i64),
            name!(sets, i64),
            name!(tombstones, i64),
            name!(rehashes, i64),
            name!(arena_used_bytes, i64),
            name!(arena_capacity_bytes, i64),
        ),
    > {
        let mut rows = Vec::new();
        for w in 0..worker_count() {
            let store = match store_view_for_worker(w) {
                Some(s) => s,
                None => continue,
            };
            for p in 0..store.num_partitions() {
                let s = store.stats(p);
                rows.push((
                    w as i32,
                    p as i32,
                    s.entries as i64,
                    s.hits as i64,
                    s.misses as i64,
                    s.evictions as i64,
                    s.sets as i64,
                    s.tombstones as i64,
                    s.rehashes as i64,
                    s.data_used as i64,
                    s.data_cap as i64,
                ));
            }
        }
        TableIterator::new(rows)
    }

    /// Persistence ring health, ONE ROW PER RING rather than summed (#111).
    ///
    /// `ring_stats()` adds every ring together, which hides the failure this
    /// surface exists to catch: persistence falls behind per shard, because a
    /// shard is a single consumer draining a single ring. One ring at its drop
    /// threshold inside a healthy-looking total is invisible in the sum and
    /// obvious here.
    ///
    /// Rings are indexed `worker * pg_keyspace.persist_workers + shard`, which
    /// is the layout `ring_index` uses; both halves are reported so a collector
    /// can group by either.
    ///
    /// `pushed`, `dropped`, `committed` and `unresolved` are CUMULATIVE;
    /// `backlog_bytes`, `lag` and `uncommitted_batches` are GAUGES.
    ///
    /// `uncommitted_batches` is NOT a failure count, despite what the
    /// underlying ring counter's name suggests. `note_attempt` increments it
    /// before a batch is tried and `note_commit` decrements it after the commit
    /// succeeds -- bracketing the attempt, because a Postgres ERROR unwinds out
    /// of the worker and an `Err` branch never runs. So a batch in flight reads
    /// as one outstanding, and under load this sits at a small number and
    /// oscillates.
    ///
    /// What it does report is a batch whose increment was never cancelled:
    /// one that failed, or whose worker died mid-commit. The signal is
    /// therefore a FLOOR THAT DOES NOT DRAIN -- when writes stop, this should
    /// return to zero, and whatever is left never committed. Alerting on any
    /// nonzero value alerts on ordinary traffic.
    #[pg_extern(stable, parallel_safe)]
    fn persist_shard_stats() -> TableIterator<
        'static,
        (
            name!(worker, i32),
            name!(shard, i32),
            name!(pushed, i64),
            name!(dropped, i64),
            name!(backlog_bytes, i64),
            name!(committed, i64),
            name!(lag, i64),
            name!(uncommitted_batches, i64),
            name!(unresolved, i64),
        ),
    > {
        let base = RING_BASE.load(Ordering::Acquire);
        let mut rows = Vec::new();
        if !base.is_null() {
            let stride = ring_stride();
            let shards = persist_shards();
            for w in 0..worker_count() {
                for sh in 0..shards {
                    let i = ring_index(w, sh);
                    let c = unsafe { ring::Consumer::attach(base.add(i * stride)) };
                    let (pushed, dropped, backlog, committed, errors, unresolved) = c.stats();
                    rows.push((
                        w as i32,
                        sh as i32,
                        pushed as i64,
                        dropped as i64,
                        backlog as i64,
                        committed as i64,
                        (pushed as i64 - committed as i64).max(0),
                        errors as i64,
                        unresolved as i64,
                    ));
                }
            }
        }
        TableIterator::new(rows)
    }

    /// Measured arena occupancy per tenant, summed across slot workers (#111).
    ///
    /// This is the accounting #102 added for the per-tenant budget, which until
    /// now was reachable only by parsing the RESP `INFO` text. `tenant` is the
    /// scope without its trailing `:`.
    ///
    /// Both columns are GAUGES. The numbers are measured by scanning the live
    /// entries on each call, so they cost O(entries) per worker -- fine at a
    /// collector's 15s, not something to put in a tight loop.
    #[pg_extern(stable, parallel_safe)]
    fn tenant_stats() -> TableIterator<
        'static,
        (
            name!(tenant, String),
            name!(arena_bytes, i64),
            name!(entries, i64),
        ),
    > {
        let mut agg: Vec<(String, i64, i64)> = Vec::new();
        for w in 0..worker_count() {
            let store = match store_view_for_worker(w) {
                Some(s) => s,
                None => continue,
            };
            for (scope, bytes, entries) in store.tenant_usage() {
                let name = String::from_utf8_lossy(
                    scope.strip_suffix(b":").unwrap_or(&scope),
                )
                .into_owned();
                match agg.iter_mut().find(|(n, _, _)| *n == name) {
                    Some(e) => {
                        e.1 += bytes as i64;
                        e.2 += entries as i64;
                    }
                    None => agg.push((name, bytes as i64, entries as i64)),
                }
            }
        }
        agg.sort_by(|a, b| b.1.cmp(&a.1));
        TableIterator::new(agg)
    }

    /// One row per background worker the watchdog tracks, with its heartbeat
    /// age (#111).
    ///
    /// The watchdog already relaunches a worker that stops beating, so this is
    /// not how a dead worker gets restarted -- it is how an operator sees that
    /// it keeps happening. A worker crash-looping is relaunched every time and
    /// looks, from outside, exactly like a worker that is running.
    ///
    /// `role` is the slot's job, derived from its index: slots are laid out
    /// RESP workers, then persistence shards, then expiry, then row-cache
    /// invalidation. `beat_age_ms` is NULL for a slot that has never beaten,
    /// which is a worker that has not started rather than one that has died.
    /// `alive` is the watchdog's own test: beating within
    /// `pg_keyspace.watchdog_secs`.
    #[pg_extern(stable, parallel_safe)]
    fn worker_health() -> TableIterator<
        'static,
        (
            name!(slot, i32),
            name!(role, String),
            name!(worker, Option<i32>),
            name!(pid, Option<i32>),
            name!(beat_age_ms, Option<i64>),
            name!(stale_after_ms, i64),
            name!(alive, bool),
        ),
    > {
        let nworkers = worker_count();
        let shards = persist_shards();
        let stale_ms = health_stale_us() / 1000;
        let now = store::now_micros();
        let mut rows = Vec::new();
        for i in 0..health_slot_count() {
            let (role, idx) = if i < nworkers {
                ("resp", Some(i as i32))
            } else if i < nworkers + shards {
                ("persist", Some((i - nworkers) as i32))
            } else if i == health_expiry_slot() {
                ("expiry", None)
            } else {
                // A member of the invalidation pool (#120), numbered so a crash
                // loop can be attributed to one of them. Which DATABASE it is
                // draining is not here on purpose -- it changes over the
                // worker's life, and the per-database answer lives in
                // `supacache.pg_stat_keyspace_rowcache_databases`.
                ("invalidation", Some((i - health_expiry_slot() - 1) as i32))
            };
            let sl = match health_slot(i) {
                Some(sl) => sl,
                None => continue,
            };
            let last = sl.last_seen_us.load(Ordering::Acquire);
            let pid = sl.owner_pid.load(Ordering::Acquire);
            let age = if last == 0 {
                None
            } else {
                Some(now.saturating_sub(last) / 1000)
            };
            rows.push((
                i as i32,
                role.to_string(),
                idx,
                if pid == 0 { None } else { Some(pid as i32) },
                age,
                stale_ms,
                age.map(|a| a <= stale_ms).unwrap_or(false),
            ));
        }
        TableIterator::new(rows)
    }

    /// How far behind the row cache's WAL decoder is, in bytes (#111).
    ///
    /// The row cache is only as coherent as this worker is current, and
    /// `rowcache_coherence()` answers the binary question ("is it beating?").
    /// This answers the one that comes next: beating, but how far behind? A
    /// decoder that is alive and losing ground serves rows that are stale for
    /// exactly as long as it takes to catch up, and the heartbeat looks fine
    /// throughout.
    ///
    /// `decode_lag_bytes` is `pg_current_wal_lsn() - confirmed_flush_lsn`: WAL
    /// generated but not yet decoded. It is never zero on a busy cluster --
    /// most WAL is not row-cache traffic and the slot only advances per drain
    /// window -- so alert on the trend, not the value.
    ///
    /// `retained_bytes` is against `restart_lsn`, which is what the slot is
    /// actually pinning on disk. That one is a disk-space risk: a stopped
    /// decoder holds WAL until `max_slot_wal_keep_size` cuts the slot loose.
    ///
    /// All columns are GAUGES. Rows are absent (not zero) when decoding is off
    /// or the slot does not exist yet, which distinguishes "not configured"
    /// from "configured and stuck at zero".
    #[pg_extern]
    fn invalidation_stats() -> TableIterator<
        'static,
        (
            name!(datid, i64),
            name!(datname, Option<String>),
            name!(slot_name, String),
            name!(active, bool),
            name!(wal_status, Option<String>),
            name!(confirmed_flush_lsn, Option<String>),
            name!(restart_lsn, Option<String>),
            name!(current_lsn, Option<String>),
            name!(decode_lag_bytes, Option<i64>),
            name!(retained_bytes, Option<i64>),
            name!(max_slot_wal_keep_size, Option<String>),
        ),
    > {
        let mut rows = Vec::new();
        if !GUC_ROWCACHE_DECODE.get() {
            return TableIterator::new(rows);
        }
        // ONE ROW PER DATABASE (#120). Resolving a single slot name and
        // returning at most one row was correct while there was one slot; with
        // one per participating database it would show an operator a single
        // arbitrary database's decode lag and let them believe it was the
        // cluster's -- which is worse than showing nothing, because the number
        // looks right.
        //
        // No cross-database query is needed for this and that is not luck:
        // replication slots are CLUSTER-WIDE objects and `pg_replication_slots`
        // carries the database each one belongs to, so the whole picture is
        // readable from wherever this happens to be called.
        //
        // Matched on the output plugin as well as the name, so a slot somebody
        // else created that happens to share the prefix is not reported as ours.
        let prefix = format!("{}\\_%", rowcache_slot_base());
        // `current_setting` rather than `GetConfigOption`: the latter returns the
        // bare number in the GUC's own unit ("32" for 32MB, "-1" for unbounded),
        // and a column reading "32" tells an operator nothing about what it is
        // 32 of. This is the one place the value is shown to a person.
        let keep = Spi::get_one::<String>("SELECT current_setting('max_slot_wal_keep_size')")
            .ok()
            .flatten();
        let found: Vec<_> = Spi::connect(|client| {
            let mut out = Vec::new();
            let t = match client.select(
                "SELECT COALESCE(d.oid, 0)::int8, d.datname::text, s.slot_name::text, s.active, \
                        s.wal_status::text, \
                        s.confirmed_flush_lsn::text, s.restart_lsn::text, \
                        pg_current_wal_lsn()::text, \
                        (pg_current_wal_lsn() - s.confirmed_flush_lsn)::bigint, \
                        (pg_current_wal_lsn() - s.restart_lsn)::bigint \
                 FROM pg_replication_slots s \
                 LEFT JOIN pg_database d ON d.datname = s.database \
                 WHERE s.plugin = 'supacache_keys' AND s.slot_name LIKE $1 \
                 ORDER BY d.datname",
                None,
                Some(vec![(PgBuiltInOids::TEXTOID.oid(), prefix.into_datum())]),
            ) {
                Ok(t) => t,
                Err(_) => return out,
            };
            for row in t {
                out.push((
                    row.get::<i64>(1).ok().flatten().unwrap_or(0),
                    row.get::<String>(2).ok().flatten(),
                    row.get::<String>(3).ok().flatten().unwrap_or_default(),
                    row.get::<bool>(4).ok().flatten().unwrap_or(false),
                    row.get::<String>(5).ok().flatten(),
                    row.get::<String>(6).ok().flatten(),
                    row.get::<String>(7).ok().flatten(),
                    row.get::<String>(8).ok().flatten(),
                    row.get::<i64>(9).ok().flatten(),
                    row.get::<i64>(10).ok().flatten(),
                ));
            }
            out
        });
        for (oid, name, slot, active, status, flush, restart, cur, lag, retained) in found {
            rows.push((
                oid, name, slot, active, status, flush, restart, cur, lag, retained,
                keep.clone(),
            ));
        }
        TableIterator::new(rows)
    }

    /// Whether the row cache's registrations are currently resident in the
    /// shared segment, and how many the catalogue holds (#111).
    ///
    /// `loaded` false with `registered` above zero is the #103 window: a fresh
    /// segment the invalidation worker has not refilled yet. Rows for those
    /// tables are not being cached, and nothing else reports it.
    #[pg_extern]
    fn rowcache_registration_status() -> TableIterator<
        'static,
        (name!(registered, i64), name!(loaded, bool)),
    > {
        // to_regclass in a SEPARATE statement, because Postgres resolves
        // relations when it parses, before it evaluates anything: a CASE with
        // the table named in the unreachable branch still fails with
        // `relation "supacache.rowcache_reg" does not exist`. Only a second
        // statement, parsed once the first says the table is there, is actually
        // guarded.
        //
        // Worth guarding because the background worker creates this table, not
        // CREATE EXTENSION, so there is a window at startup -- and after a
        // DROP/CREATE EXTENSION -- where it is absent. An erroring stats view
        // reads to a collector as the database being down.
        let registered = if table_exists("supacache.rowcache_reg") {
            Spi::get_one::<i64>("SELECT count(*) FROM supacache.rowcache_reg")
                .ok()
                .flatten()
                .unwrap_or(0)
        } else {
            0
        };
        TableIterator::once((registered, registrations_loaded()))
    }

    #[pg_extern]
    fn stats() -> TableIterator<
        'static,
        (
            name!(partition, i32),
            name!(entries, i64),
            name!(hits, i64),
            name!(misses, i64),
            name!(evictions, i64),
            name!(sets, i64),
            name!(tombstones, i64),
            name!(rehashes, i64),
            name!(data_used, i64),
            name!(data_cap, i64),
        ),
    > {
        let mut rows = Vec::new();
        // One row per (slot worker, partition). `partition` is the global index
        // `worker * num_partitions + partition`, so with the default single
        // partition per segment it reads as the slot worker index.
        for w in 0..worker_count() {
            let store = match store_view_for_worker(w) {
                Some(s) => s,
                None => continue,
            };
            let nparts = store.num_partitions();
            for p in 0..nparts {
                let s = store.stats(p);
                rows.push((
                    (w as u32 * nparts + p) as i32,
                    s.entries as i64,
                    s.hits as i64,
                    s.misses as i64,
                    s.evictions as i64,
                    s.sets as i64,
                    s.tombstones as i64,
                    s.rehashes as i64,
                    s.data_used as i64,
                    s.data_cap as i64,
                ));
            }
        }
        TableIterator::new(rows)
    }
}

// silence unused warnings for the c_void import used only in casts on some paths
#[allow(dead_code)]
fn _keep(_: *mut c_void) {}

// --------------------------------------------------------------------------
// #111: operational metrics, shaped as pg_stat_* views.
//
// The instinct here is a Redis-style exporter process and a Grafana dashboard.
// That is a second artefact to version, deploy and watch. Postgres already has
// a monitoring ecosystem -- postgres_exporter, pgwatch, Datadog, pganalyze, and
// a `SELECT` in cron -- and all of it collects from `pg_stat_*`-shaped views.
// Exposing the numbers that way means every one of those tools picks the cache
// up with NO new configuration. Borrowing an ecosystem beats building one, and
// it is a thing a standalone keyspace structurally cannot do.
//
// Conventions, chosen to match what a collector expects rather than what is
// convenient here:
//
//   * COUNTERS are cumulative since the segment or process started, never
//     reset by reading, so `rate()` over two scrapes is meaningful. GAUGES are
//     instantaneous. The two are never mixed in a column, and the README lists
//     which is which per view.
//   * A view is EMPTY, not zero-filled, when the thing it reports is switched
//     off. `pg_stat_keyspace_invalidation` with no rows means decoding is
//     disabled; a row of zeroes would mean decoding is on and stuck, and an
//     alert cannot tell those apart if they look the same.
//   * Views, not functions, are the supported surface. The functions stay
//     callable, but their shapes are free to change; the views are the
//     contract.
//
// Created with `finalize` so they are emitted after every function they select
// from, whatever order pgrx generates the rest in.
extension_sql!(
    r#"
-- One row: the cluster-wide rollup, for a dashboard's top line.
CREATE VIEW supacache.pg_stat_keyspace AS
SELECT
    (SELECT count(DISTINCT worker)::int FROM supacache.worker_stats())      AS workers,
    (SELECT count(*)::int FROM supacache.worker_stats())                    AS partitions,
    COALESCE(sum(w.entries), 0)::bigint                                     AS entries,
    COALESCE(sum(w.hits), 0)::bigint                                        AS hits,
    COALESCE(sum(w.misses), 0)::bigint                                      AS misses,
    COALESCE(sum(w.evictions), 0)::bigint                                   AS evictions,
    COALESCE(sum(w.sets), 0)::bigint                                        AS sets,
    COALESCE(sum(w.tombstones), 0)::bigint                                  AS tombstones,
    COALESCE(sum(w.rehashes), 0)::bigint                                    AS rehashes,
    COALESCE(sum(w.arena_used_bytes), 0)::bigint                            AS arena_used_bytes,
    COALESCE(sum(w.arena_capacity_bytes), 0)::bigint                        AS arena_capacity_bytes,
    -- NULL rather than 0 before the first lookup: a cache nobody has read from
    -- has no hit ratio, and graphing it as 0% invents an outage.
    CASE WHEN COALESCE(sum(w.hits + w.misses), 0) > 0
         THEN round(sum(w.hits)::numeric * 100 / sum(w.hits + w.misses), 2)
    END                                                                     AS hit_pct
FROM supacache.worker_stats() w;

-- One row per (slot worker, partition).
CREATE VIEW supacache.pg_stat_keyspace_workers AS
SELECT w.worker, w.partition, w.entries, w.hits, w.misses, w.evictions,
       w.sets, w.tombstones, w.rehashes,
       w.arena_used_bytes,
       (w.arena_capacity_bytes - w.arena_used_bytes)                        AS arena_free_bytes,
       w.arena_capacity_bytes,
       CASE WHEN w.arena_capacity_bytes > 0
            THEN round(w.arena_used_bytes::numeric * 100 / w.arena_capacity_bytes, 2)
       END                                                                  AS arena_used_pct,
       r.slot_lo, r.slot_hi, r.port
FROM supacache.worker_stats() w
LEFT JOIN supacache.slot_ranges() r ON r.worker = w.worker;

-- One row per background worker the watchdog tracks.
CREATE VIEW supacache.pg_stat_keyspace_activity AS
SELECT slot, role, worker, pid, beat_age_ms, stale_after_ms, alive
FROM supacache.worker_health();

-- One row per persistence ring. Summed in pg_stat_keyspace_persist_total.
CREATE VIEW supacache.pg_stat_keyspace_persist AS
SELECT worker, shard, pushed, dropped, backlog_bytes, committed, lag,
       uncommitted_batches, unresolved
FROM supacache.persist_shard_stats();

CREATE VIEW supacache.pg_stat_keyspace_persist_total AS
SELECT COALESCE(sum(pushed), 0)::bigint          AS pushed,
       COALESCE(sum(dropped), 0)::bigint         AS dropped,
       COALESCE(sum(backlog_bytes), 0)::bigint   AS backlog_bytes,
       COALESCE(sum(committed), 0)::bigint       AS committed,
       COALESCE(sum(lag), 0)::bigint             AS lag,
       COALESCE(sum(uncommitted_batches), 0)::bigint AS uncommitted_batches,
       COALESCE(sum(unresolved), 0)::bigint      AS unresolved,
       -- The ring with the deepest backlog, which is the one that will start
       -- dropping. A healthy total hides it completely.
       (SELECT max(backlog_bytes) FROM supacache.persist_shard_stats()) AS worst_ring_backlog_bytes
FROM supacache.persist_shard_stats();

-- One row per tenant with resident entries.
CREATE VIEW supacache.pg_stat_keyspace_tenants AS
SELECT tenant, arena_bytes, entries
FROM supacache.tenant_stats();

-- Row cache: segment-wide occupancy, and coherence FOR THIS DATABASE.
--
-- The occupancy columns are the whole segment, which is cluster-wide and shared
-- by every database the cache serves. The coherence columns are this database's
-- alone, because coherence is per-database (#120) -- so the view names the
-- database it is describing rather than leaving a reader to assume. Which
-- databases are incoherent, and why, is pg_stat_keyspace_rowcache_databases.
CREATE VIEW supacache.pg_stat_keyspace_rowcache AS
SELECT s.entries, s.hits, s.misses, s.data_used AS arena_used_bytes,
       s.data_cap AS arena_capacity_bytes,
       CASE WHEN s.hits + s.misses > 0
            THEN round(s.hits::numeric * 100 / (s.hits + s.misses), 2)
       END                                       AS hit_pct,
       c.coherent, c.decode_enabled, c.beat_age_ms, c.stale_after_ms,
       g.registered AS registrations, g.loaded AS registrations_loaded,
       c.datname, c.slot_lost, c.participating_databases,
       (SELECT count(*) FROM supacache.rowcache_databases()
         WHERE state = 'participating' AND NOT coherent)::bigint
                                                 AS incoherent_databases
FROM supacache.rowcache_stats() s
CROSS JOIN supacache.rowcache_coherence() c
CROSS JOIN supacache.rowcache_registration_status() g;

-- One row per database the row cache knows about.
--
-- The cache FAILS CLOSED on incoherence, so which database is incoherent
-- decides which queries are served from the cache at all -- this is load-bearing
-- for correctness, not only for dashboards. `stale_after_ms` is the window each
-- database is judged against, and it grows with the number of participating
-- databases once they outnumber pg_keyspace.rowcache_invalidation_workers,
-- because a database waiting its turn is behind by the cycle time by design.
CREATE VIEW supacache.pg_stat_keyspace_rowcache_databases AS
SELECT datoid, datname, state, registrations, coherent, slot_lost,
       beat_age_ms, stale_after_ms, slot_name, worker_pid
FROM supacache.rowcache_databases();

-- One row PER DATABASE with a row-cache slot. Empty when decoding is off or no
-- slot has been created yet.
--
-- Keyed by database because there is one slot per participating database: a
-- single row would show one arbitrary database's decode lag, and an operator
-- watching it would believe it was the cluster's.
CREATE VIEW supacache.pg_stat_keyspace_invalidation AS
SELECT datid, datname, slot_name, active, wal_status,
       confirmed_flush_lsn, restart_lsn, current_lsn,
       decode_lag_bytes, retained_bytes, max_slot_wal_keep_size
FROM supacache.invalidation_stats();

CREATE VIEW supacache.pg_stat_keyspace_pubsub AS
SELECT dropped, route_full, name_too_long FROM supacache.pubsub_stats();

CREATE VIEW supacache.pg_stat_keyspace_topology AS
SELECT recorded_workers, running_workers, slots_moved, pct_moved
FROM supacache.topology_change();

-- A monitoring role gets these and nothing else. `pg_monitor` is the role
-- postgres_exporter, pgwatch and Datadog are already told to use, so this is
-- the whole of the setup: install the extension, and an existing collector
-- picks the cache up. A team using a different role name grants it pg_monitor,
-- which is the one line every Postgres monitoring guide already tells them to
-- run.
--
-- EXECUTE is granted on the functions too, and that is not belt-and-braces. A
-- view's *table* references are checked against the view owner, but a SET
-- FUNCTION in its FROM clause is checked against the CALLER -- the executor
-- asks pg_proc_aclcheck(..., GetUserId(), ACL_EXECUTE), and GetUserId() is the
-- collector. Granting SELECT on the view alone gets "permission denied for
-- function worker_stats", which is what run_pg_stat_views.sh reported when
-- these grants were missing. Granting EXECUTE explicitly also means an operator
-- hardening the install with REVOKE ... FROM PUBLIC does not break monitoring.
GRANT USAGE ON SCHEMA supacache TO pg_monitor;
GRANT EXECUTE ON FUNCTION
    supacache.worker_stats(),
    supacache.persist_shard_stats(),
    supacache.tenant_stats(),
    supacache.worker_health(),
    supacache.invalidation_stats(),
    supacache.rowcache_registration_status(),
    supacache.rowcache_stats(),
    supacache.rowcache_coherence(),
    supacache.rowcache_databases(),
    supacache.pubsub_stats(),
    supacache.topology_change(),
    supacache.slot_ranges()
TO pg_monitor;
GRANT SELECT ON
    supacache.pg_stat_keyspace,
    supacache.pg_stat_keyspace_workers,
    supacache.pg_stat_keyspace_activity,
    supacache.pg_stat_keyspace_persist,
    supacache.pg_stat_keyspace_persist_total,
    supacache.pg_stat_keyspace_tenants,
    supacache.pg_stat_keyspace_rowcache,
    supacache.pg_stat_keyspace_rowcache_databases,
    supacache.pg_stat_keyspace_invalidation,
    supacache.pg_stat_keyspace_pubsub,
    supacache.pg_stat_keyspace_topology
TO pg_monitor;

COMMENT ON VIEW supacache.pg_stat_keyspace IS
  'pg_keyspace: cluster-wide keyspace rollup. Counters are cumulative since segment creation; entries and arena_*_bytes are gauges.';
COMMENT ON VIEW supacache.pg_stat_keyspace_workers IS
  'pg_keyspace: per (slot worker, partition) counters, arena occupancy and the slot range/port the worker serves.';
COMMENT ON VIEW supacache.pg_stat_keyspace_activity IS
  'pg_keyspace: heartbeat age per background worker. alive=false is a worker the watchdog is about to relaunch; a row that flips repeatedly is a crash loop.';
COMMENT ON VIEW supacache.pg_stat_keyspace_persist IS
  'pg_keyspace: persistence ring health, one row per (worker, shard). lag is acknowledged writes not yet committed. uncommitted_batches is a GAUGE of batches in flight, not a failure count: alert on a floor that never drains, not on it being nonzero.';
COMMENT ON VIEW supacache.pg_stat_keyspace_persist_total IS
  'pg_keyspace: persistence rings summed, plus the deepest single ring backlog, which a sum hides.';
COMMENT ON VIEW supacache.pg_stat_keyspace_tenants IS
  'pg_keyspace: measured arena occupancy per tenant (gauges). Scans live entries, so cost is O(entries) per call.';
COMMENT ON VIEW supacache.pg_stat_keyspace_rowcache IS
  'pg_keyspace: Mode B row cache occupancy (segment-wide) and coherence (for datname, the database you are connected to). coherent=false means invalidation is configured but not current for THIS database; reads fail closed. incoherent_databases counts them cluster-wide.';
COMMENT ON VIEW supacache.pg_stat_keyspace_rowcache_databases IS
  'pg_keyspace: one row per database the row cache knows about. state=participating has registrations and needs a slot; idle has none and deliberately gets neither. stale_after_ms grows with participating databases once they outnumber rowcache_invalidation_workers, because a database waiting its turn is behind by the cycle time by design.';
COMMENT ON VIEW supacache.pg_stat_keyspace_invalidation IS
  'pg_keyspace: WAL decode lag for the row cache, one row per database with a slot. Empty when decoding is off. retained_bytes is WAL that database''s slot is pinning on disk; wal_status=lost means the server cut it loose for exceeding max_slot_wal_keep_size, and that database is then marked incoherent and rebuilt.';
COMMENT ON VIEW supacache.pg_stat_keyspace_pubsub IS
  'pg_keyspace: pub/sub messages NOT delivered. Every column is a cumulative loss counter; PUBLISH cannot report these to the client.';
COMMENT ON VIEW supacache.pg_stat_keyspace_topology IS
  'pg_keyspace: worker layout the persisted keyspace was written under vs the one running now, and what a change between them costs.';
"#,
    name = "pg_stat_keyspace_views",
    finalize
);
