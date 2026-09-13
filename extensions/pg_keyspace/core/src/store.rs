//! The shared-memory keyspace store. One `Store` maps the whole segment; it is
//! carved into `num_partitions` disjoint partitions. Each partition is written
//! by exactly one slot worker, so the hot path takes no locks and no
//! atomics. Layout, per partition:
//!
//! ```text
//! partition {
//!   PartMeta                       // counts, CLOCK hand, slab free lists
//!   buckets:  [u32; B]             // open-addressed hash -> entry_idx+1
//!   entries:  [Entry; E]           // key_off/val_off/val_len/expires_at/version/flags
//!   data:     [u8; D]              // size-classed slab arena, 32B..8KB
//! }
//! ```
//!
//! Not MVCC. No tuple headers. No vacuum. Entries are overwritten in place.

use crate::shmem::Shmem;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

// v3 widens Entry with `staged_seq` and PartMeta with `commit_watermark`, for
// values handed to the persistence worker by reference instead of being copied
// through the ring. The layout is not compatible with v2.
const MAGIC: u64 = 0x70_67_6b_73_5f_76_33_00; // "pgks_v3\0"
/// Layout version inside a given MAGIC. Bumped when the meaning of the header's
/// own fields changes; MAGIC is bumped when the partition layout does.
const VERSION: u32 = 2;

// Size classes for the slab allocator ("size-classed, 32B..8KB").
const CLASS_SIZES: [usize; 9] = [32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];
const NUM_CLASSES: usize = CLASS_SIZES.len();
const OVERSIZED: u32 = u32::MAX; // value larger than 8KB: bump-only ("overflow to heap-only")

const BUCKET_EMPTY: u32 = 0;
const BUCKET_TOMB: u32 = u32::MAX;

const FLAG_OCCUPIED: u32 = 1;
const FLAG_REF: u32 = 2; // CLOCK reference bit
/// Never evicted. For entries that are configuration rather than cache
/// content, whose loss silently changes behaviour instead of costing a lookup.
///
/// Pinned entries are still deleted on request and still expire on TTL; only
/// the CLOCK sweep skips them. Keep their number small and bounded: an arena
/// that is entirely pinned cannot free space, and `ensure_alloc` then fails the
/// write rather than looping, so caching degrades to not caching.
const FLAG_PINNED: u32 = 4;

pub const KIND_STR: u32 = b's' as u32;
/// aggregate kinds: value blob is a serialized hash/list/sorted-set.
pub const KIND_HASH: u32 = b'h' as u32;
pub const KIND_LIST: u32 = b'l' as u32;
pub const KIND_ZSET: u32 = b'z' as u32;
// 'S' (distinct from KIND_STR 's'): a serialized unordered set of members.
pub const KIND_SET: u32 = b'S' as u32;
pub const KIND_BLOOM: u32 = b'b' as u32;
pub const KIND_CUCKOO: u32 = b'c' as u32;

pub fn type_name(kind: u32) -> &'static str {
    match kind {
        KIND_HASH => "hash",
        KIND_LIST => "list",
        KIND_ZSET => "zset",
        KIND_SET => "set",
        KIND_BLOOM => "MBbloom--",
        KIND_CUCKOO => "MBbloomCF",
        _ => "string",
    }
}

pub fn encoding_name(kind: u32, val: &[u8]) -> &'static [u8] {
    match kind {
        KIND_HASH | KIND_SET => b"hashtable",
        KIND_LIST => b"quicklist",
        KIND_ZSET => b"skiplist",
        KIND_BLOOM | KIND_CUCKOO => b"raw",
        _ if std::str::from_utf8(val)
            .ok()
            .and_then(|t| t.parse::<i64>().ok())
            .is_some() =>
        {
            b"int"
        }
        _ => b"embstr",
    }
}

#[repr(C)]
struct SegHeader {
    magic: u64,
    version: u32,
    num_partitions: u32,
    partition_bytes: u64,
    buckets_per_part: u32,
    entries_per_part: u32,
    data_bytes_per_part: u64,
    _pad: u64,
}

#[repr(C)]
struct PartMeta {
    entry_count: u32,
    free_entry_head: u32, // idx+1, 0 = none
    entry_bump: u32,
    clock_hand: u32,
    data_bump: u64,
    free_class: [u64; NUM_CLASSES], // offset+1, 0 = none
    // Free list of reclaimed OVERSIZED blocks (>8KB values). Each free block
    // stores its capacity at base[0..8] and the next link (base+1, 0=none) at
    // base[8..16]; this head is that link for the first free block. Without it,
    // an oversized value that is rewritten (e.g. a large hash growing field by
    // field) would leak its old region on every write and exhaust the arena.
    free_oversized: u64,
    // Highest ring sequence the persistence worker has durably committed, as
    // last observed by this worker's event loop. Eviction compares an entry's
    // `staged_seq` against it: at or below, the value is safe in Postgres and
    // the entry can go; above, its only copy is here.
    //
    // Written and read by the same single worker thread, so a plain field is
    // sufficient. Conservative with several persist shards, since the minimum
    // across rings is used and sequence numbers are per-ring.
    commit_watermark: u64,
    hits: u64,
    misses: u64,
    evictions: u64,
    sets: u64,
    tombstones: u64,
    rehashes: u64,
    // Per-tenant arena usage (#102), MEASURED rather than accumulated.
    //
    // The obvious implementation is a running total per tenant, adjusted on
    // every insert, overwrite, eviction and expiry. It is also the wrong one
    // here: `set_in` alone changes a value's length in four places, and a
    // counter that drifts enforces something fictional -- evicting a tenant
    // that is not over, silently. There is no cheap way to notice.
    //
    // So this is a snapshot recomputed by one linear pass over the entry array,
    // amortised across USAGE_REFRESH_EVERY evictions. It cannot drift, because
    // nothing accumulates: every read of it was measured. The cost is that it
    // is up to that many evictions stale, which a budget tolerates -- it is a
    // policy about who to take space from, not an invariant anything depends on.
    usage_evictions: u64, // value of `evictions` when the snapshot was taken
    usage_valid: u32,     // occupied slots in `usage`
    _usage_pad: u32,
    usage: [TenantUse; TENANT_SLOTS],
}

#[repr(C)]
struct Entry {
    key_hash: u64,
    key_off: u64,
    val_off: u64,
    key_len: u32,
    val_len: u32,
    key_class: u32,
    val_class: u32,
    expires_at: i64, // unix micros, 0 = no expiry
    // Seqlock. Even means the entry is stable; odd means a writer is partway
    // through replacing the value. A reader in another backend samples this
    // before and after copying and retries if it moved, which is what stops it
    // splicing the head of a new value onto the tail of an old one.
    //
    // Also the change counter WATCH compares, and what a by-reference ring
    // record carries so the persistence worker can tell whether the value it
    // is about to read is still the one it was told about.
    version: AtomicU64,
    // Ring sequence of this entry's last *referenced* staged write, or 0 when
    // there is none. A large value is handed to the persistence worker by
    // reference rather than copied through the ring, so the worker reads it
    // back out of this segment at commit time. Until that commit lands the
    // value must still be here: eviction therefore skips an entry whose
    // `staged_seq` is above the persist worker's commit watermark.
    //
    // Only the referenced path sets this. Small values are copied into the
    // ring as before and stay freely evictable, so this is 0 for them and the
    // comparison is trivially true.
    staged_seq: u64,
    flags: u32,
    kind: u32,
}

#[inline]
fn align_up(n: usize, a: usize) -> usize {
    (n + a - 1) & !(a - 1)
}

#[inline]
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in bytes {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

#[inline]
fn class_for(size: usize) -> u32 {
    for (i, &c) in CLASS_SIZES.iter().enumerate() {
        if size <= c {
            return i as u32;
        }
    }
    OVERSIZED
}

/// Open a write on an entry: version goes odd, so a concurrent reader in
/// another backend knows the value is being replaced and retries.
/// Read a settled version, waiting out a write that is in progress. Bounded:
/// a writer holds the odd window for a memcpy, so this spins only briefly, and
/// giving up returns the odd value rather than looping forever if a writer
/// died mid-update.
#[inline]
unsafe fn stable_version(e: *mut Entry) -> u64 {
    for _ in 0..1024 {
        let v = (*e).version.load(Ordering::Acquire);
        if v & 1 == 0 {
            return v;
        }
        std::hint::spin_loop();
    }
    (*e).version.load(Ordering::Acquire)
}

#[inline]
unsafe fn seq_begin(e: *mut Entry) {
    (*e).version.fetch_add(1, Ordering::AcqRel);
}

/// Close a write: version goes even again, one higher than any reader that
/// sampled it before the write started, so the retry sees the change.
#[inline]
unsafe fn seq_end(e: *mut Entry) {
    (*e).version.fetch_add(1, Ordering::Release);
}

#[inline]
/// A clock that is immune to wall-clock *steps* but still reports wall-clock
/// values (#110).
///
/// TTLs used to compare against `CLOCK_REALTIME` directly, so a clock step moved
/// every key's deadline at once. Forward: everything with a deadline inside the
/// jump expired together, which from the application's side is a cache that
/// emptied itself for no reason. Backward: keys outlived their TTL by the size
/// of the jump, which for a TTL used as a lock lease or a rate-limit window is a
/// correctness problem. Both silent, neither diagnosable afterwards. Steps are
/// not exotic: a VM resuming from suspend, a container host correcting a large
/// offset, a first NTP sync after booting with a bad RTC.
///
/// The fix is the clock, not the deadlines. An anchor -- realtime and boottime
/// captured together -- is advanced by the *boottime* delta, so the value is
/// still an absolute unix timestamp and every persisted `expires_at`, every
/// `kv_ttl` bucket and every comparison keeps working untouched. Only steps stop
/// being visible.
///
/// `CLOCK_BOOTTIME` rather than `CLOCK_MONOTONIC`: time a machine spends
/// suspended should count toward a TTL, or a host suspended for an hour resumes
/// with every key an hour past its deadline still live. macOS has no
/// `CLOCK_BOOTTIME`, but its `CLOCK_MONOTONIC` already includes sleep, so the
/// two are equivalent there.
///
/// Slew is ignored along with steps, so over long uptime this drifts slightly
/// from true wall time. Irrelevant for a relative TTL -- both ends use this
/// clock, so a 300-second TTL is accurate to microseconds -- and visible only
/// for an absolute deadline set via `EXPIREAT`. A restart re-anchors.
#[inline]
fn boot_micros() -> i64 {
    #[cfg(target_os = "linux")]
    const SRC: libc::clockid_t = libc::CLOCK_BOOTTIME;
    #[cfg(not(target_os = "linux"))]
    const SRC: libc::clockid_t = libc::CLOCK_MONOTONIC;
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(SRC, &mut ts);
        ts.tv_sec as i64 * 1_000_000 + ts.tv_nsec as i64 / 1_000
    }
}

#[inline]
fn real_micros() -> i64 {
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts);
        ts.tv_sec as i64 * 1_000_000 + ts.tv_nsec as i64 / 1_000
    }
}

static ANCHOR_REAL: AtomicI64 = AtomicI64::new(0);
static ANCHOR_BOOT: AtomicI64 = AtomicI64::new(0);
static ANCHOR_SET: AtomicBool = AtomicBool::new(false);

/// Adopt an anchor captured elsewhere -- in practice one written to shared
/// memory when the segment was created.
///
/// The anchor MUST be shared across processes. A per-process anchor would have
/// backends that started either side of a step disagreeing about whether a key
/// is expired, which is worse than the bug being fixed: a global shift becomes
/// per-process inconsistency.
pub fn adopt_clock_anchor(real_us: i64, boot_us: i64) {
    ANCHOR_REAL.store(real_us, Ordering::Release);
    ANCHOR_BOOT.store(boot_us, Ordering::Release);
    ANCHOR_SET.store(true, Ordering::Release);
}

/// Capture an anchor for sharing. Called once, by whoever creates the segment.
pub fn capture_clock_anchor() -> (i64, i64) {
    (real_micros(), boot_micros())
}

pub fn now_micros() -> i64 {
    if !ANCHOR_SET.load(Ordering::Acquire) {
        // No shared anchor: the standalone daemon, or a unit test. Single
        // process, so a local anchor is consistent by construction. Raced
        // adopters land within microseconds of each other, and only one wins.
        let (r, b) = capture_clock_anchor();
        if ANCHOR_SET
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            ANCHOR_REAL.store(r, Ordering::Release);
            ANCHOR_BOOT.store(b, Ordering::Release);
        }
    }
    anchored_now(
        ANCHOR_REAL.load(Ordering::Acquire),
        ANCHOR_BOOT.load(Ordering::Acquire),
        boot_micros(),
    )
}

/// The whole of the clock, as arithmetic.
///
/// Split out so it can be tested without touching the process-global anchor --
/// tests run in parallel threads and share it, so a test that adopted a
/// deliberately bogus anchor would break every other test that reads the clock.
#[inline]
fn anchored_now(anchor_real: i64, anchor_boot: i64, boot_now: i64) -> i64 {
    anchor_real + (boot_now - anchor_boot)
}

#[derive(Clone, Copy, Default)]
pub struct PartStats {
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub sets: u64,
    pub tombstones: u64,
    pub rehashes: u64,
    pub data_used: u64,
    pub data_cap: u64,
}

pub struct Config {
    pub num_partitions: u32,
    pub buckets_per_part: u32, // must be power of two
    pub entries_per_part: u32,
    pub data_bytes_per_part: u64,
}

impl Config {
    /// A sensible default sized by target key count per partition.
    pub fn for_capacity(num_partitions: u32, keys_per_part: u32, avg_val: u64) -> Config {
        let buckets = (keys_per_part * 2).next_power_of_two().max(1024);
        let entries = (keys_per_part as f64 * 1.1) as u32 + 16;
        // data: (key + value + slab rounding) per entry, generous headroom.
        let data = entries as u64 * (align_up(64 + avg_val as usize, 64) as u64) + (1 << 20);
        Config {
            num_partitions,
            buckets_per_part: buckets,
            entries_per_part: entries,
            data_bytes_per_part: data,
        }
    }

    fn partition_bytes(&self) -> usize {
        let mut off = std::mem::size_of::<PartMeta>();
        off = align_up(off, 64) + self.buckets_per_part as usize * 4;
        off = align_up(off, 64) + self.entries_per_part as usize * std::mem::size_of::<Entry>();
        off = align_up(off, 64) + self.data_bytes_per_part as usize;
        align_up(off, 64)
    }

    pub fn total_bytes(&self) -> usize {
        align_up(std::mem::size_of::<SegHeader>(), 64)
            + self.num_partitions as usize * self.partition_bytes()
    }
}

#[allow(dead_code)]
enum Backing {
    /// POSIX shared memory owned by this handle (core daemon, tests).
    Posix(Shmem),
    /// A raw region owned by someone else — e.g. a Postgres shared-memory
    /// segment from `ShmemInitStruct`, mapped at the same address in every
    /// backend. The store only borrows it.
    Raw,
}

/// How many entries the scoped look-ahead inspects before giving up and letting
/// the ordinary sweep decide.
///
/// Deliberately a small constant rather than a full revolution. A tenant that
/// owns a large share of the arena -- which is exactly the tenant worth
/// scoping eviction to -- will have a victim within a few dozen slots, while a
/// tenant that owns almost nothing falls through immediately at a fixed, tiny
/// cost. Sweeping a whole revolution looking for a tenant with no entries would
/// add an O(entries) scan to every eviction for the tenants least responsible
/// for the pressure.
const SCOPED_EVICT_PROBE: u32 = 64;

/// How many tenants a partition tracks arena usage for. A fixed array rather
/// than a map because this lives in shared memory and is read on the eviction
/// path; tenants past the cap are simply not budgeted, which is the safe
/// direction (no budget is the old behaviour).
const TENANT_SLOTS: usize = 32;

/// Longest tenant scope recorded in a usage slot, including its `:`.
const TENANT_SCOPE_MAX: usize = 40;

/// Refresh the usage snapshot after this many evictions in a partition.
///
/// The snapshot is a *measurement*, not a running total, so this is the only
/// thing that makes it stale -- and staleness is harmless for a budget, which
/// is a policy rather than an invariant.
const USAGE_REFRESH_EVERY: u64 = 512;

/// One tenant's measured arena usage in one partition.
#[repr(C)]
#[derive(Clone, Copy)]
struct TenantUse {
    scope_hash: u64,
    bytes: u64,
    entries: u32,
    scope_len: u32,
    scope: [u8; TENANT_SCOPE_MAX],
}

impl TenantUse {
    const EMPTY: TenantUse = TenantUse {
        scope_hash: 0,
        bytes: 0,
        entries: 0,
        scope_len: 0,
        scope: [0u8; TENANT_SCOPE_MAX],
    };
    #[inline]
    fn scope_bytes(&self) -> &[u8] {
        &self.scope[..self.scope_len as usize]
    }
}

/// The tenant a key belongs to, for eviction purposes: everything up to and
/// including the first `:`.
///
/// Keys are force-scoped to `{tenant}:` server-side for non-exempt roles, so
/// this is the tenant boundary rather than a guess at one. A key with no `:` --
/// an unscoped or exempt deployment -- has no scope and is evicted exactly as
/// before.
///
/// Only meaningful for the RESP keyspace, whose keys are client-supplied byte
/// strings. It is nonsense for a segment holding binary keys, which is why
/// scoping is enabled per [`Store`] rather than globally: a row-cache key is
/// `relid_le_bytes ++ pk`, and one table in every 256 has a relid whose low
/// byte is 0x3a, so its rows would all appear to belong to a one-byte ":"
/// tenant.
#[inline]
fn tenant_scope(key: &[u8]) -> Option<&[u8]> {
    key.iter().position(|b| *b == b':').map(|i| &key[..=i])
}

pub struct Store {
    // Held for its `Drop`: the `Posix` variant owns the mmap and must outlive
    // the store, so this field is load-bearing despite never being read.
    #[allow(dead_code)]
    backing: Backing,
    base: *mut u8,
    num_partitions: u32,
    buckets: u32,
    bucket_mask: u32,
    entries: u32,
    data_bytes: u64,
    partition_bytes: usize,
    header_bytes: usize,
    /// Whether eviction prefers a victim from the same tenant as the key being
    /// inserted (#43). Off unless the caller turns it on, because it is only
    /// meaningful for a segment whose keys are the tenant-scoped RESP keyspace
    /// -- see [`tenant_scope`].
    scoped_eviction: bool,
    arena_pct: u32,
}

// A partition is written by exactly one worker; SQL-surface readers in other
// backends observe a consistent-enough view (the plan notes a seqlock is the
// stronger guarantee). The raw base is stable across backends.
unsafe impl Send for Store {}
unsafe impl Sync for Store {}

/// Outcome of a value lookup on the hot path.
pub enum Lookup<'a> {
    Hit(&'a [u8]),
    Miss,
}

impl Store {
    pub fn create(name: &str, cfg: &Config) -> std::io::Result<Store> {
        assert!(cfg.buckets_per_part.is_power_of_two());
        let total = cfg.total_bytes();
        let shmem = Shmem::create(name, total)?;
        let base = shmem.base();
        let s = Store::from_parts(Backing::Posix(shmem), base, cfg);
        s.init_header();
        Ok(s)
    }

    pub fn attach(name: &str, cfg: &Config) -> std::io::Result<Store> {
        let total = cfg.total_bytes();
        let shmem = Shmem::attach(name, total)?;
        let base = shmem.base();
        // Refuse a segment laid out differently from the way we are about to
        // index into it. Mapping it anyway does not fail — it just reads the
        // wrong offsets, quietly, forever.
        unsafe { Store::check_header(base, cfg) }.map_err(|why| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("pg_keyspace segment '{name}': {why}"),
            )
        })?;
        Ok(Store::from_parts(Backing::Posix(shmem), base, cfg))
    }

    /// Build a store over a caller-owned region (e.g. Postgres shared memory).
    /// `base` must point at `cfg.total_bytes()` writable bytes. When `init` is
    /// true the region is zeroed and the header written (first backend only);
    /// other backends pass `init = false` and just build a view.
    ///
    /// # Safety
    /// `base` must be valid for `cfg.total_bytes()` for the store's lifetime.
    pub unsafe fn from_raw(base: *mut u8, cfg: &Config, init: bool) -> Store {
        assert!(cfg.buckets_per_part.is_power_of_two());
        let s = Store::from_parts(Backing::Raw, base, cfg);
        if init {
            std::ptr::write_bytes(base, 0, cfg.total_bytes());
            s.init_header();
        }
        s
    }

    fn from_parts(backing: Backing, base: *mut u8, cfg: &Config) -> Store {
        Store {
            backing,
            base,
            num_partitions: cfg.num_partitions,
            buckets: cfg.buckets_per_part,
            bucket_mask: cfg.buckets_per_part - 1,
            entries: cfg.entries_per_part,
            data_bytes: cfg.data_bytes_per_part,
            partition_bytes: cfg.partition_bytes(),
            header_bytes: align_up(std::mem::size_of::<SegHeader>(), 64),
            scoped_eviction: false,
            arena_pct: 0,
        }
    }

    /// Prefer a victim from the inserting key's tenant when evicting (#43).
    ///
    /// Set this only on a segment holding the tenant-scoped RESP keyspace. On
    /// one holding binary keys -- the row cache -- `:` is just a byte that
    /// turns up, and the preference would group unrelated rows together.
    pub fn set_scoped_eviction(&mut self, on: bool) {
        self.scoped_eviction = on;
    }

    /// Cap any one tenant at this percentage of a partition's entries
    /// (0 = no budget, the default and the pre-#102 behaviour).
    ///
    /// A share rather than an absolute, because the arena size is already a
    /// setting and the tenant count varies.
    pub fn set_tenant_arena_pct(&mut self, pct: u32) {
        self.arena_pct = pct.min(100);
    }

    /// Measured arena usage per tenant in this segment: (scope, bytes, entries),
    /// summed across partitions, largest first.
    ///
    /// Forces a refresh rather than reusing the eviction path's snapshot, which
    /// is deliberately allowed to lag. Reporting that lag as current numbers
    /// would make the stats surface quietly wrong -- it over-counted by every
    /// entry deleted since the last eviction-driven refresh, which is how the
    /// drift test caught it.
    pub fn tenant_usage(&self) -> Vec<(Vec<u8>, u64, u64)> {
        let mut agg: Vec<(Vec<u8>, u64, u64)> = Vec::new();
        for p in 0..self.num_partitions {
            unsafe {
                // Forced: a caller asking for stats wants a current answer, not
                // whatever the eviction path last happened to need.
                self.refresh_usage(p, true);
                let meta = self.meta(p);
                for i in 0..(*meta).usage_valid as usize {
                    let u = (*meta).usage[i];
                    if u.scope_len == 0 {
                        continue;
                    }
                    let k = u.scope_bytes().to_vec();
                    match agg.iter_mut().find(|(s, _, _)| *s == k) {
                        Some(e) => {
                            e.1 += u.bytes;
                            e.2 += u.entries as u64;
                        }
                        None => agg.push((k, u.bytes, u.entries as u64)),
                    }
                }
            }
        }
        agg.sort_by(|a, b| b.1.cmp(&a.1));
        agg
    }

    fn init_header(&self) {
        unsafe {
            let h = self.base as *mut SegHeader;
            (*h).magic = MAGIC;
            (*h).version = VERSION;
            (*h).num_partitions = self.num_partitions;
            (*h).partition_bytes = self.partition_bytes as u64;
            (*h).buckets_per_part = self.buckets;
            (*h).entries_per_part = self.entries;
            (*h).data_bytes_per_part = self.data_bytes;
        }
    }

    /// Compare the header an existing segment carries against the layout this
    /// process is about to read it with.
    ///
    /// The header has been written since the first version and never read back.
    /// That is safe only while every process mapping the segment agrees on the
    /// layout by construction — true inside Postgres, which recreates the
    /// segment on every start from Postmaster-level GUCs, and not true of a
    /// segment that outlives the process which created it. Getting it wrong is
    /// silent rather than loud: the offsets simply land in the wrong places and
    /// lookups return plausible garbage for the life of the process.
    ///
    /// Reports every field that disagrees rather than stopping at the first, so
    /// an operator correcting sizing sees the whole story at once.
    ///
    /// # Safety
    /// `base` must point at a readable region of at least
    /// `size_of::<SegHeader>()` bytes.
    pub unsafe fn check_header(base: *const u8, cfg: &Config) -> Result<(), String> {
        let h = &*(base as *const SegHeader);
        if h.magic != MAGIC {
            return Err(format!(
                "not a pg_keyspace segment: magic {:#018x}, expected {:#018x} \
                 (a stale segment of the same name, or one written by a build \
                 with a different layout)",
                h.magic, MAGIC
            ));
        }
        if h.version != VERSION {
            return Err(format!(
                "segment layout version {}, this build understands {}",
                h.version, VERSION
            ));
        }
        let fields: [(&str, u64, u64); 5] = [
            (
                "partitions",
                h.num_partitions as u64,
                cfg.num_partitions as u64,
            ),
            (
                "partition bytes",
                h.partition_bytes,
                cfg.partition_bytes() as u64,
            ),
            (
                "buckets per partition",
                h.buckets_per_part as u64,
                cfg.buckets_per_part as u64,
            ),
            (
                "entries per partition",
                h.entries_per_part as u64,
                cfg.entries_per_part as u64,
            ),
            (
                "data bytes per partition",
                h.data_bytes_per_part,
                cfg.data_bytes_per_part,
            ),
        ];
        let bad: Vec<String> = fields
            .iter()
            .filter(|(_, found, want)| found != want)
            .map(|(what, found, want)| format!("{what} {found}, expected {want}"))
            .collect();
        if bad.is_empty() {
            Ok(())
        } else {
            Err(format!(
                "segment layout is not the one this process expects: {}",
                bad.join("; ")
            ))
        }
    }

    #[inline]
    pub fn num_partitions(&self) -> u32 {
        self.num_partitions
    }

    #[inline]
    unsafe fn part_base(&self, p: u32) -> *mut u8 {
        self.base
            .add(self.header_bytes + p as usize * self.partition_bytes)
    }

    #[inline]
    unsafe fn meta(&self, p: u32) -> *mut PartMeta {
        self.part_base(p) as *mut PartMeta
    }

    #[inline]
    unsafe fn buckets_ptr(&self, p: u32) -> *mut u32 {
        let off = align_up(std::mem::size_of::<PartMeta>(), 64);
        self.part_base(p).add(off) as *mut u32
    }

    #[inline]
    unsafe fn entries_ptr(&self, p: u32) -> *mut Entry {
        let mut o = align_up(std::mem::size_of::<PartMeta>(), 64);
        o = align_up(o + self.buckets as usize * 4, 64);
        self.part_base(p).add(o) as *mut Entry
    }

    #[inline]
    unsafe fn data_ptr(&self, p: u32) -> *mut u8 {
        let mut o = align_up(std::mem::size_of::<PartMeta>(), 64);
        o = align_up(o + self.buckets as usize * 4, 64);
        o = align_up(o + self.entries as usize * std::mem::size_of::<Entry>(), 64);
        self.part_base(p).add(o)
    }

    #[inline]
    fn partition_for_hash(&self, hash: u64) -> u32 {
        // Distribute keys across partitions by a slice of the hash. The real
        // extension routes by CRC16 slot range; both give a stable owner.
        (hash % self.num_partitions as u64) as u32
    }

    // ---- slab allocator ---------------------------------------------------

    unsafe fn slab_alloc(&self, p: u32, size: usize) -> Option<(u64, u32)> {
        let meta = self.meta(p);
        let cls = class_for(size);
        if cls == OVERSIZED {
            // An oversized block is `[cap: u64][data...]`; the returned offset
            // points at the data (base+8). Capacity is rounded up to a power of
            // two so a value that grows in place (a large hash gaining fields)
            // reuses its block until the next doubling — amortized O(1) growth
            // instead of leaking a fresh region on every write.
            let need = align_up(size, 8) as u64;
            // First-fit reuse from the oversized free list.
            let mut link = &mut (*meta).free_oversized as *mut u64;
            while *link != 0 {
                let base = *link - 1;
                let cap = *(self.data_ptr(p).add(base as usize) as *const u64);
                let next = *(self.data_ptr(p).add(base as usize + 8) as *const u64);
                if cap >= need {
                    *link = next; // unlink
                    return Some((base + 8, OVERSIZED));
                }
                link = self.data_ptr(p).add(base as usize + 8) as *mut u64;
            }
            // None fit: bump a new block with growth slack.
            let cap = need.max(16).next_power_of_two();
            let total = 8 + cap;
            if (*meta).data_bump + total > self.data_bytes {
                return None;
            }
            let base = (*meta).data_bump;
            (*meta).data_bump += total;
            *(self.data_ptr(p).add(base as usize) as *mut u64) = cap;
            return Some((base + 8, OVERSIZED));
        }
        let ci = cls as usize;
        let head = (*meta).free_class[ci];
        if head != 0 {
            let off = head - 1;
            // next pointer stored at block start
            let next = *(self.data_ptr(p).add(off as usize) as *const u64);
            (*meta).free_class[ci] = next;
            return Some((off, cls));
        }
        let need = CLASS_SIZES[ci] as u64;
        if (*meta).data_bump + need > self.data_bytes {
            return None;
        }
        let off = (*meta).data_bump;
        (*meta).data_bump += need;
        Some((off, cls))
    }

    /// Return an oversized block (`base` points at its `[cap: u64]` header) to
    /// the free list, merging it with any physically adjacent free blocks and
    /// giving space back to the bump pointer when it lands at the top.
    ///
    /// The naive version — push onto a LIFO list and never merge — leaks
    /// capacity in a way no amount of eviction recovers. Blocks are laid out
    /// contiguously by bump and freed blocks were never coalesced, so a
    /// workload writing growing values (10 MiB, then 20, then 40) consumed
    /// fresh bump space every time while the free list filled with blocks that
    /// were individually too small to satisfy the next request. `data_bump`
    /// only ever moved up, so eventually every large allocation failed with
    /// most of the arena sitting free but unusable. Eviction did not help: it
    /// frees onto the same list.
    ///
    /// Keeping the list ordered by offset makes both fixes cheap. Adjacency is
    /// decidable because a block occupies exactly `8 + cap` bytes, so the
    /// neighbour begins where this block ends.
    unsafe fn oversized_free(&self, p: u32, base: u64) {
        let meta = self.meta(p);
        let data = self.data_ptr(p);
        let cap_at = |b: u64| *(data.add(b as usize) as *const u64);
        let next_at = |b: u64| *(data.add(b as usize + 8) as *const u64);
        let set_next = |b: u64, v: u64| *(data.add(b as usize + 8) as *mut u64) = v;

        // Ordered insert: find the last free block before `base`.
        let mut prev: Option<u64> = None;
        let mut cur = (*meta).free_oversized;
        while cur != 0 && cur - 1 < base {
            prev = Some(cur - 1);
            cur = next_at(cur - 1);
        }
        let next = if cur == 0 { None } else { Some(cur - 1) };

        set_next(base, cur);
        match prev {
            Some(pb) => set_next(pb, base + 1),
            None => (*meta).free_oversized = base + 1,
        }

        // Merge forward: this block's end meets the next free block's start.
        let mut cap = cap_at(base);
        if let Some(nb) = next {
            if base + 8 + cap == nb {
                let ncap = cap_at(nb);
                cap += 8 + ncap; // absorb the neighbour, header included
                *(data.add(base as usize) as *mut u64) = cap;
                set_next(base, next_at(nb));
            }
        }

        // Merge backward: the previous free block's end meets this one's start.
        let mut head = base;
        if let Some(pb) = prev {
            let pcap = cap_at(pb);
            if pb + 8 + pcap == base {
                let merged = pcap + 8 + cap;
                *(data.add(pb as usize) as *mut u64) = merged;
                set_next(pb, next_at(base));
                head = pb;
                cap = merged;
            }
        }

        // At the top of the arena: hand it back to the bump pointer rather than
        // holding it on the list, so the space is available at any size again.
        if head + 8 + cap == (*meta).data_bump {
            // unlink `head`
            let mut link = &mut (*meta).free_oversized as *mut u64;
            while *link != 0 {
                let b = *link - 1;
                if b == head {
                    *link = next_at(b);
                    break;
                }
                link = data.add(b as usize + 8) as *mut u64;
            }
            (*meta).data_bump = head;
        }
    }

    unsafe fn slab_free(&self, p: u32, off: u64, cls: u32) {
        if cls == OVERSIZED {
            self.oversized_free(p, off - 8);
            return;
        }
        let meta = self.meta(p);
        let ci = cls as usize;
        let head = (*meta).free_class[ci];
        *(self.data_ptr(p).add(off as usize) as *mut u64) = head;
        (*meta).free_class[ci] = off + 1;
    }

    // ---- entry arena ------------------------------------------------------

    unsafe fn entry_alloc(&self, p: u32) -> Option<u32> {
        let meta = self.meta(p);
        if (*meta).free_entry_head != 0 {
            let idx = (*meta).free_entry_head - 1;
            let e = self.entries_ptr(p).add(idx as usize);
            (*meta).free_entry_head = (*e).key_off as u32;
            return Some(idx);
        }
        if (*meta).entry_bump < self.entries {
            let idx = (*meta).entry_bump;
            (*meta).entry_bump += 1;
            return Some(idx);
        }
        None
    }

    unsafe fn entry_free(&self, p: u32, idx: u32) {
        let meta = self.meta(p);
        let e = self.entries_ptr(p).add(idx as usize);
        (*e).flags = 0;
        (*e).key_off = (*meta).free_entry_head as u64;
        (*meta).free_entry_head = idx + 1;
    }

    // ---- hash table -------------------------------------------------------

    /// Find the bucket holding `key` (an occupied match) and the first
    /// tombstone/empty slot seen (for insertion). The scan is bounded by the
    /// bucket count: linear probing with tombstones can otherwise spin forever
    /// once every bucket is occupied-or-tombstoned, which is the failure mode
    /// that a naive first cut of this table hits under heavy churn. `insert` is
    /// `None` only when the table is saturated; callers rehash and retry.
    unsafe fn probe(&self, p: u32, hash: u64, key: &[u8]) -> (Option<usize>, Option<usize>) {
        let buckets = self.buckets_ptr(p);
        let entries = self.entries_ptr(p);
        let mut b = (hash & self.bucket_mask as u64) as usize;
        let mut insert: Option<usize> = None;
        for _ in 0..self.buckets as usize {
            let v = *buckets.add(b);
            if v == BUCKET_EMPTY {
                if insert.is_none() {
                    insert = Some(b);
                }
                return (None, insert);
            } else if v == BUCKET_TOMB {
                if insert.is_none() {
                    insert = Some(b);
                }
            } else {
                let idx = v - 1;
                let e = entries.add(idx as usize);
                if (*e).key_hash == hash && (*e).key_len as usize == key.len() {
                    let kp = self.data_ptr(p).add((*e).key_off as usize);
                    if slice_eq(kp, key) {
                        return (Some(b), Some(b));
                    }
                }
            }
            b = (b + 1) & self.bucket_mask as usize;
        }
        (None, insert)
    }

    /// Rebuild the bucket array from live entries, dropping all tombstones.
    /// O(buckets + entries); triggered when load (live + tombstones) gets high.
    unsafe fn rehash(&self, p: u32) {
        let buckets = self.buckets_ptr(p);
        for i in 0..self.buckets as usize {
            *buckets.add(i) = BUCKET_EMPTY;
        }
        let meta = self.meta(p);
        let bump = (*meta).entry_bump;
        for idx in 0..bump {
            let e = self.entries_ptr(p).add(idx as usize);
            if (*e).flags & FLAG_OCCUPIED != 0 {
                let mut b = ((*e).key_hash & self.bucket_mask as u64) as usize;
                loop {
                    if *buckets.add(b) == BUCKET_EMPTY {
                        *buckets.add(b) = idx + 1;
                        break;
                    }
                    b = (b + 1) & self.bucket_mask as usize;
                }
            }
        }
        (*meta).tombstones = 0;
        (*meta).rehashes += 1;
    }

    #[inline]
    unsafe fn need_rehash(&self, p: u32) -> bool {
        let meta = self.meta(p);
        // keep the table below 70% (live + tombstones) so an EMPTY slot always
        // exists and probe chains stay short.
        ((*meta).entry_count as u64 + (*meta).tombstones + 1) * 10 >= self.buckets as u64 * 7
    }

    // ---- public API (called only by the owning worker for partition p) ----

    /// GET: returns a slice into shmem valid until the next mutation of this
    /// partition. Lazily expires. Updates CLOCK ref bit and stats.
    pub fn get<'a>(&'a self, key: &[u8]) -> Lookup<'a> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let meta = self.meta(p);
            match found {
                None => {
                    (*meta).misses += 1;
                    Lookup::Miss
                }
                Some(b) => {
                    let idx = *self.buckets_ptr(p).add(b) - 1;
                    let e = self.entries_ptr(p).add(idx as usize);
                    let exp = (*e).expires_at;
                    if exp != 0 && exp <= now_micros() {
                        self.remove_at(p, b, idx);
                        (*meta).misses += 1;
                        return Lookup::Miss;
                    }
                    (*e).flags |= FLAG_REF;
                    (*meta).hits += 1;
                    let vp = self.data_ptr(p).add((*e).val_off as usize);
                    let val = std::slice::from_raw_parts(vp, (*e).val_len as usize);
                    Lookup::Hit(val)
                }
            }
        }
    }

    /// SET with optional TTL (micros from now, 0 = none). Overwrites in place.
    pub fn set(&self, key: &[u8], val: &[u8], ttl_micros: i64) -> bool {
        self.set_typed(key, val, ttl_micros, KIND_STR)
    }

    /// SET a typed value (aggregates): same as `set` but tags the entry's
    /// `kind` so `get_typed` can enforce Redis `WRONGTYPE` semantics.
    pub fn set_typed(&self, key: &[u8], val: &[u8], ttl_micros: i64, kind: u32) -> bool {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        let exp = if ttl_micros > 0 {
            now_micros() + ttl_micros
        } else {
            0
        };
        unsafe {
            (*self.meta(p)).sets += 1;
            self.set_in(p, hash, key, val, exp, kind)
        }
    }

    /// Store `key` and mark it never-evictable (see `FLAG_PINNED`).
    ///
    /// For entries whose disappearance changes behaviour rather than costing a
    /// lookup — the row cache's table registrations are the case this exists
    /// for: they live in the same arena as the cached rows, so a busy cache
    /// evicted them and silently stopped caching the very tables it was
    /// configured for.
    ///
    /// Sets the flag after the write so it survives an update of an existing
    /// entry as well as a fresh insert.
    pub fn set_pinned(&self, key: &[u8], val: &[u8]) -> bool {
        if !self.set_typed(key, val, 0, KIND_STR) {
            return false;
        }
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            if let Some(b) = found {
                let idx = *self.buckets_ptr(p).add(b) - 1;
                (*self.entries_ptr(p).add(idx as usize)).flags |= FLAG_PINNED;
                true
            } else {
                // Written and then evicted before the flag landed, which an
                // arena under pressure can do. The caller sees the failure
                // rather than a registration that is not actually pinned.
                false
            }
        }
    }

    /// Like `get`, but also returns the entry's `kind` and absolute expiry (0 =
    /// none) so a caller can enforce `WRONGTYPE` and preserve TTL on read-modify-
    /// write of an aggregate. Lazy-expires like `get`.
    pub fn get_typed<'a>(&'a self, key: &[u8]) -> Option<(u32, i64, &'a [u8])> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let meta = self.meta(p);
            match found {
                None => {
                    (*meta).misses += 1;
                    None
                }
                Some(b) => {
                    let idx = *self.buckets_ptr(p).add(b) - 1;
                    let e = self.entries_ptr(p).add(idx as usize);
                    let exp = (*e).expires_at;
                    if exp != 0 && exp <= now_micros() {
                        self.remove_at(p, b, idx);
                        (*meta).misses += 1;
                        return None;
                    }
                    (*e).flags |= FLAG_REF;
                    (*meta).hits += 1;
                    let vp = self.data_ptr(p).add((*e).val_off as usize);
                    let val = std::slice::from_raw_parts(vp, (*e).val_len as usize);
                    Some(((*e).kind, exp, val))
                }
            }
        }
    }

    unsafe fn set_in(
        &self,
        p: u32,
        hash: u64,
        key: &[u8],
        val: &[u8],
        exp: i64,
        kind: u32,
    ) -> bool {
        let (found, _first_insert) = self.probe(p, hash, key);
        if let Some(b) = found {
            // overwrite value in place, reusing the slab if the class matches.
            let idx = *self.buckets_ptr(p).add(b) - 1;
            let mut e = self.entries_ptr(p).add(idx as usize);
            let want = class_for(val.len());
            // Reuse the existing region in place when it still fits: same size
            // class, or an oversized block whose capacity covers the new length.
            let reuse = want == (*e).val_class
                && (want != OVERSIZED || {
                    let cap = *(self.data_ptr(p).add(((*e).val_off - 8) as usize) as *const u64);
                    (val.len() as u64) <= cap
                });
            let mut installed = true;
            if reuse {
                seq_begin(e);
                let vp = self.data_ptr(p).add((*e).val_off as usize);
                std::ptr::copy_nonoverlapping(val.as_ptr(), vp, val.len());
                (*e).val_len = val.len() as u32;
            } else {
                // Allocate the replacement BEFORE releasing the old block.
                //
                // Freeing first and then failing to allocate left the entry
                // pointing into the free list: the previous value was destroyed
                // even though the write failed, and a later read returned
                // whatever had since been handed to another key. A failed
                // overwrite must be a no-op, not a silent corruption.
                let got = self.ensure_alloc(p, val.len(), tenant_scope(key));
                let (voff, vcls) = match got {
                    Some(x) => x,
                    // Nothing to undo: the old value is still intact and
                    // readable, which is the correct outcome for a write that
                    // could not be served.
                    None => return false,
                };
                // `ensure_alloc` evicts to make room and CLOCK can evict *this*
                // entry, so the earlier pointer may now be a free slot. Re-probe
                // rather than writing through it.
                match self.probe(p, hash, key).0 {
                    Some(b2) => {
                        let idx2 = *self.buckets_ptr(p).add(b2) - 1;
                        e = self.entries_ptr(p).add(idx2 as usize);
                        // Opened only now: the allocation above can evict, and
                        // an entry left with an odd version while that ran
                        // would spin every reader for no reason.
                        seq_begin(e);
                        self.slab_free(p, (*e).val_off, (*e).val_class);
                        let vp = self.data_ptr(p).add(voff as usize);
                        std::ptr::copy_nonoverlapping(val.as_ptr(), vp, val.len());
                        (*e).val_off = voff;
                        (*e).val_class = vcls;
                        (*e).val_len = val.len() as u32;
                    }
                    None => {
                        // Evicted while we were making room for it. Release the
                        // block we took and insert the key afresh below.
                        self.slab_free(p, voff, vcls);
                        installed = false;
                    }
                }
            }
            if installed {
                (*e).expires_at = exp;
                (*e).kind = kind;
                (*e).flags |= FLAG_REF;
                seq_end(e);
                return true;
            }
        }

        // insert new. Compact first if the table is getting full, so a probe
        // for this key is guaranteed to terminate on an EMPTY slot.
        if self.need_rehash(p) {
            self.rehash(p);
        }
        // May require eviction to free an entry slot or data.
        let idx = loop {
            match self.entry_alloc(p) {
                Some(i) => break i,
                None => {
                    if !self.evict_one(p, tenant_scope(key)) {
                        return false;
                    }
                }
            }
        };
        let (koff, kcls) = match self.ensure_alloc(p, key.len(), tenant_scope(key)) {
            Some(x) => x,
            None => {
                self.entry_free(p, idx);
                return false;
            }
        };
        let (voff, vcls) = match self.ensure_alloc(p, val.len(), tenant_scope(key)) {
            Some(x) => x,
            None => {
                self.slab_free(p, koff, kcls);
                self.entry_free(p, idx);
                return false;
            }
        };
        let kp = self.data_ptr(p).add(koff as usize);
        std::ptr::copy_nonoverlapping(key.as_ptr(), kp, key.len());
        let vp = self.data_ptr(p).add(voff as usize);
        std::ptr::copy_nonoverlapping(val.as_ptr(), vp, val.len());

        let e = self.entries_ptr(p).add(idx as usize);
        (*e).key_hash = hash;
        (*e).key_off = koff;
        (*e).key_len = key.len() as u32;
        (*e).key_class = kcls;
        (*e).val_off = voff;
        (*e).val_len = val.len() as u32;
        (*e).val_class = vcls;
        (*e).expires_at = exp;
        (*e).version.store(2, Ordering::Release); // even: stable
        (*e).flags = FLAG_OCCUPIED | FLAG_REF;
        (*e).kind = kind;

        // re-probe insert slot in case eviction shuffled tombstones. After a
        // rehash and successful entry alloc, an EMPTY/TOMB slot is guaranteed.
        let (_f, insert) = self.probe(p, hash, key);
        let insert = match insert {
            Some(b) => b,
            None => {
                // extreme saturation: compact and retry once.
                self.rehash(p);
                self.probe(p, hash, key).1.expect("slot after rehash")
            }
        };
        let old = *self.buckets_ptr(p).add(insert);
        *self.buckets_ptr(p).add(insert) = idx + 1;
        if old == BUCKET_TOMB {
            let meta = self.meta(p);
            (*meta).tombstones = (*meta).tombstones.saturating_sub(1);
        }
        (*self.meta(p)).entry_count += 1;
        true
    }

    /// Allocate slab space, evicting under CLOCK until it fits.
    /// Arena bytes a `size`-byte allocation actually consumes, matching what
    /// `slab_alloc` will reserve: a size class rounds to its class, an
    /// oversized block rounds its capacity to a power of two and adds the
    /// 8-byte capacity header.
    fn alloc_footprint(&self, size: usize) -> u64 {
        let cls = class_for(size);
        if cls == OVERSIZED {
            8 + (align_up(size, 8) as u64).max(16).next_power_of_two()
        } else {
            CLASS_SIZES[cls as usize] as u64
        }
    }

    /// Whether the arena could ever hold a `len`-byte value, measured the way
    /// `slab_alloc` reserves it. Lets a caller refuse before it builds the bytes.
    pub fn can_hold(&self, len: usize) -> bool {
        self.alloc_footprint(len) <= self.data_bytes
    }

    unsafe fn ensure_alloc(&self, p: u32, size: usize, prefer: Option<&[u8]>) -> Option<(u64, u32)> {
        // Refuse an allocation the arena could never satisfy, before evicting
        // anything. Without this, a single write too large for the arena evicts
        // the entire keyspace one entry at a time and then fails regardless:
        // the write does not land and every other key is gone with it.
        //
        // This must measure what `slab_alloc` will actually ask for, not the
        // caller's size. An oversized block rounds its capacity up to a power
        // of two and carries an 8-byte header, so a 9 MiB value really needs
        // 16 MiB: checking the raw size let it through, and the doomed
        // eviction loop ran anyway.
        if self.alloc_footprint(size) > self.data_bytes {
            return None;
        }
        loop {
            if let Some(x) = self.slab_alloc(p, size) {
                return Some(x);
            }
            if !self.evict_one(p, prefer) {
                return None;
            }
        }
    }

    /// DEL: returns true if a live key was removed.
    pub fn del(&self, key: &[u8]) -> bool {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            match found {
                None => false,
                Some(b) => {
                    let idx = *self.buckets_ptr(p).add(b) - 1;
                    self.remove_at(p, b, idx);
                    true
                }
            }
        }
    }

    /// Set (or clear, when `exp_micros == 0`) the absolute expiry of an existing
    /// key in place, without rewriting its value. Returns whether the key was
    /// present. A non-zero `exp_micros` already in the past deletes the key (as
    /// Redis EXPIRE/EXPIREAT with a past time do) and still returns `true`.
    pub fn set_expiry(&self, key: &[u8], exp_micros: i64) -> bool {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let b = match found {
                Some(b) => b,
                None => return false,
            };
            let idx = *self.buckets_ptr(p).add(b) - 1;
            let e = self.entries_ptr(p).add(idx as usize);
            let cur = (*e).expires_at;
            if cur != 0 && cur <= now_micros() {
                // key is already expired (not yet lazily reaped): treat as absent
                self.remove_at(p, b, idx);
                return false;
            }
            if exp_micros != 0 && exp_micros <= now_micros() {
                self.remove_at(p, b, idx);
                return true;
            }
            (*e).expires_at = exp_micros;
            true
        }
    }

    /// The version counter of the live entry at `key` (bumped on every write),
    /// or None if the key is absent/expired. Used by WATCH to snapshot a key and
    /// detect whether it changed before EXEC. Lazily expires like `get`.
    pub fn version(&self, key: &[u8]) -> Option<u64> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let b = found?;
            let idx = *self.buckets_ptr(p).add(b) - 1;
            let e = self.entries_ptr(p).add(idx as usize);
            let exp = (*e).expires_at;
            if exp != 0 && exp <= now_micros() {
                self.remove_at(p, b, idx);
                return None;
            }
            Some(stable_version(e))
        }
    }

    /// Iterate live keys for SCAN. `cursor` is an opaque position (0 starts a new
    /// iteration); `limit` bounds how many keys one call returns. Returns the
    /// keys found and the next cursor (0 once iteration is complete). Keys are
    /// copied out (they outlive the borrow). Like Redis SCAN, coverage is only
    /// guaranteed for keys present for the whole iteration; heavy concurrent
    /// churn may miss or repeat a key.
    pub fn scan(&self, cursor: u64, limit: usize) -> (u64, Vec<Vec<u8>>) {
        let stride = self.entries as u64;
        let total = self.num_partitions as u64 * stride;
        let budget = limit.max(1);
        let now = now_micros();
        let mut pos = cursor;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        unsafe {
            while pos < total && keys.len() < budget {
                let p = (pos / stride) as u32;
                let idx = (pos % stride) as u32;
                pos += 1;
                let meta = self.meta(p);
                if idx >= (*meta).entry_bump {
                    // nothing allocated past entry_bump — skip this partition's tail
                    pos = (p as u64 + 1) * stride;
                    continue;
                }
                let e = self.entries_ptr(p).add(idx as usize);
                if (*e).flags & FLAG_OCCUPIED == 0 {
                    continue;
                }
                let exp = (*e).expires_at;
                if exp != 0 && exp <= now {
                    continue;
                }
                let k = std::slice::from_raw_parts(
                    self.data_ptr(p).add((*e).key_off as usize),
                    (*e).key_len as usize,
                );
                keys.push(k.to_vec());
            }
        }
        let next = if pos >= total { 0 } else { pos };
        (next, keys)
    }

    /// INCR by `by`. Values are stored as decimal text (Redis semantics).
    /// Returns the new value, or None on overflow / non-integer.
    pub fn incr(&self, key: &[u8], by: i64) -> Option<i64> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let cur = match found {
                None => 0i64,
                Some(b) => {
                    let idx = *self.buckets_ptr(p).add(b) - 1;
                    let e = self.entries_ptr(p).add(idx as usize);
                    let exp = (*e).expires_at;
                    if exp != 0 && exp <= now_micros() {
                        self.remove_at(p, b, idx);
                        0
                    } else {
                        let vp = self.data_ptr(p).add((*e).val_off as usize);
                        let s = std::slice::from_raw_parts(vp, (*e).val_len as usize);
                        match std::str::from_utf8(s).ok().and_then(|t| t.parse::<i64>().ok()) {
                            Some(n) => n,
                            None => return None,
                        }
                    }
                }
            };
            let next = cur.checked_add(by)?;
            let mut buf = [0u8; 20];
            let s = i64_to_bytes(next, &mut buf);
            (*self.meta(p)).sets += 1;
            if self.set_in(p, hash, key, s, 0, KIND_STR) {
                Some(next)
            } else {
                None
            }
        }
    }

    pub fn with_value_mut<R>(&self, key: &[u8], f: impl FnOnce(&mut [u8]) -> R) -> Option<R> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let b = found?;
            let idx = *self.buckets_ptr(p).add(b) - 1;
            let e = self.entries_ptr(p).add(idx as usize);
            let exp = (*e).expires_at;
            if exp != 0 && exp <= now_micros() {
                self.remove_at(p, b, idx);
                return None;
            }
            struct SeqGuard(*mut Entry, *mut PartMeta);
            impl Drop for SeqGuard {
                fn drop(&mut self) {
                    unsafe {
                        (*self.0).flags |= FLAG_REF;
                        (*self.1).sets += 1;
                        seq_end(self.0);
                    }
                }
            }
            seq_begin(e);
            let guard = SeqGuard(e, self.meta(p));
            let vp = self.data_ptr(p).add((*e).val_off as usize);
            let val = std::slice::from_raw_parts_mut(vp, (*e).val_len as usize);
            let out = f(val);
            drop(guard);
            Some(out)
        }
    }

    unsafe fn remove_at(&self, p: u32, bucket: usize, idx: u32) {
        let e = self.entries_ptr(p).add(idx as usize);
        self.slab_free(p, (*e).key_off, (*e).key_class);
        self.slab_free(p, (*e).val_off, (*e).val_class);
        self.entry_free(p, idx);
        *self.buckets_ptr(p).add(bucket) = BUCKET_TOMB;
        let meta = self.meta(p);
        (*meta).tombstones += 1;
        if (*meta).entry_count > 0 {
            (*meta).entry_count -= 1;
        }
    }

    /// CLOCK sweep: clear ref bits until an unreferenced occupied entry is
    /// found, then evict it. Returns false only if the arena is empty.
    /// A bounded CLOCK sweep restricted to one tenant's own entries.
    ///
    /// Entries belonging to other tenants are stepped over without touching
    /// their reference bits, so a flood cannot age another tenant's data merely
    /// by looking for a victim. Within the tenant this is ordinary CLOCK: a
    /// referenced entry gets its second chance and the first unreferenced one is
    /// taken.
    ///
    /// Giving the tenant's own entries their second chance here rather than
    /// skipping them is the whole mechanism. A reference bit is set on *write*,
    /// so a flood's freshly written keys all look referenced; a version of this
    /// that skipped them found nothing, every time, and fell through to the
    /// global sweep -- which is the behaviour being fixed. A tenant competing
    /// with itself is exactly who should be clearing its own reference bits.
    /// Recompute this partition's per-tenant usage, if the snapshot has gone
    /// stale. One linear pass over the entry array.
    ///
    /// Measured, never accumulated -- see the note on `PartMeta::usage`. The
    /// pass is over a contiguous array of fixed-size entries, so it is a
    /// sequential read rather than a pointer chase, and it happens once per
    /// USAGE_REFRESH_EVERY evictions rather than per operation.
    ///
    /// Tenants past TENANT_SLOTS are dropped from the snapshot, smallest first.
    /// That is the safe direction: a tenant we are not tracking is a tenant we
    /// never decide is over budget, which is the behaviour from before there
    /// were budgets at all.
    unsafe fn refresh_usage(&self, p: u32, force: bool) {
        let meta = self.meta(p);
        if !force
            && (*meta).usage_valid > 0
            && (*meta).evictions.saturating_sub((*meta).usage_evictions) < USAGE_REFRESH_EVERY
        {
            return;
        }
        let mut slots = [TenantUse::EMPTY; TENANT_SLOTS];
        let mut used = 0usize;
        let bump = (*meta).entry_bump;
        for idx in 0..bump {
            let e = self.entries_ptr(p).add(idx as usize);
            if (*e).flags & FLAG_OCCUPIED == 0 {
                continue;
            }
            let key = std::slice::from_raw_parts(
                self.data_ptr(p).add((*e).key_off as usize),
                (*e).key_len as usize,
            );
            // Unscoped keys are nobody's tenant and are never budgeted.
            let scope = match tenant_scope(key) {
                Some(s) if s.len() <= TENANT_SCOPE_MAX => s,
                _ => continue,
            };
            let bytes = (*e).key_len as u64 + (*e).val_len as u64;
            let h = fnv1a(scope);
            if let Some(slot) = slots[..used].iter_mut().find(|s| s.scope_hash == h) {
                slot.bytes += bytes;
                slot.entries += 1;
                continue;
            }
            if used < TENANT_SLOTS {
                let slot = &mut slots[used];
                slot.scope_hash = h;
                slot.bytes = bytes;
                slot.entries = 1;
                slot.scope_len = scope.len() as u32;
                slot.scope[..scope.len()].copy_from_slice(scope);
                used += 1;
                continue;
            }
            // Full: displace the smallest, but only if this one is bigger.
            // Budgets are about the largest tenants, so losing the tail costs
            // nothing that the policy would have acted on.
            if let Some((i, _)) = slots
                .iter()
                .enumerate()
                .min_by_key(|(_, s)| s.bytes)
                .filter(|(_, s)| s.bytes < bytes)
                .map(|(i, s)| (i, s.bytes))
            {
                let slot = &mut slots[i];
                slot.scope_hash = h;
                slot.bytes = bytes;
                slot.entries = 1;
                slot.scope_len = scope.len() as u32;
                slot.scope[..scope.len()].copy_from_slice(scope);
            }
        }
        (*meta).usage = slots;
        (*meta).usage_valid = used as u32;
        (*meta).usage_evictions = (*meta).evictions;
    }

    /// The tenant holding more than the configured share of this partition's
    /// entries, if any. Largest first, so the worst offender is taken from.
    ///
    /// Measured against entry count rather than bytes: the arena is carved into
    /// size classes, so a tenant's share of *capacity* is what its entries
    /// occupy, and entries are what a partition runs out of first.
    unsafe fn over_budget_scope(&self, p: u32) -> Option<[u8; TENANT_SCOPE_MAX]> {
        if self.arena_pct == 0 {
            return None;
        }
        // Not forced: on the eviction path, a snapshot up to
        // USAGE_REFRESH_EVERY evictions old is what keeps this amortised, and a
        // budget tolerates that staleness.
        self.refresh_usage(p, false);
        let meta = self.meta(p);
        let cap = self.entries as u64;
        if cap == 0 {
            return None;
        }
        let budget = cap * self.arena_pct as u64 / 100;
        let mut best: Option<(u64, [u8; TENANT_SCOPE_MAX])> = None;
        for i in 0..(*meta).usage_valid as usize {
            let u = (*meta).usage[i];
            if u.scope_len == 0 || u.entries as u64 <= budget {
                continue;
            }
            if best.map(|(b, _)| u.entries as u64 > b).unwrap_or(true) {
                best = Some((u.entries as u64, u.scope));
            }
        }
        best.map(|(_, s)| s)
    }

    unsafe fn evict_scoped(&self, p: u32, prefer: &[u8]) -> bool {
        let meta = self.meta(p);
        let bump = (*meta).entry_bump;
        if bump == 0 {
            return false;
        }
        let steps = SCOPED_EVICT_PROBE.min(bump);
        for _ in 0..steps {
            let idx = (*meta).clock_hand % bump;
            (*meta).clock_hand = (idx + 1) % bump;
            let e = self.entries_ptr(p).add(idx as usize);
            if (*e).flags & FLAG_OCCUPIED == 0 {
                continue;
            }
            // The same two the ordinary sweep must never take: a staged write
            // whose only copy is here, and a pinned registration.
            if (*e).staged_seq > (*meta).commit_watermark || (*e).flags & FLAG_PINNED != 0 {
                continue;
            }
            let key = std::slice::from_raw_parts(
                self.data_ptr(p).add((*e).key_off as usize),
                (*e).key_len as usize,
            );
            if !key.starts_with(prefer) {
                continue; // another tenant's: not ours to age or to take
            }
            if (*e).flags & FLAG_REF != 0 {
                (*e).flags &= !FLAG_REF;
                continue;
            }
            let (found, _) = self.probe(p, (*e).key_hash, key);
            if let Some(b) = found {
                self.remove_at(p, b, idx);
                (*meta).evictions += 1;
                return true;
            }
        }
        false
    }

    /// Free one entry, preferring a victim from `prefer`'s tenant if there is a
    /// cold one close to hand (#43).
    ///
    /// Without the preference, one tenant writing cold keys hard enough to keep
    /// the arena full simply evicts everyone else: CLOCK admits cold keys
    /// unconditionally and takes whatever is unreferenced, which is every other
    /// tenant's data as readily as its own. Preferring the inserting tenant's
    /// own cold entries makes a flood recycle its own space.
    ///
    /// It is a preference, not a budget. A tenant with nothing evictable of its
    /// own still falls through to the ordinary sweep, so a small or new tenant
    /// is never starved of the arena by this.
    unsafe fn evict_one(&self, p: u32, prefer: Option<&[u8]>) -> bool {
        // A tenant over its budget is taken from first, whoever is inserting.
        // This is what makes the budget a budget rather than the preference
        // scoped eviction already gives: without it, a tenant that grew
        // steadily rather than flooding keeps everything it has, because it
        // always has an evictable entry of its own to recycle (#102).
        if let Some(scope) = self.over_budget_scope(p) {
            let len = scope.iter().position(|b| *b == 0).unwrap_or(TENANT_SCOPE_MAX);
            if len > 0 && self.evict_scoped(p, &scope[..len]) {
                return true;
            }
            // Nothing evictable of theirs right now (all pinned, all staged, or
            // out of probe range): fall through rather than refusing to evict.
        }
        if self.scoped_eviction {
            if let Some(scope) = prefer {
                if self.evict_scoped(p, scope) {
                    return true;
                }
            }
        }
        let meta = self.meta(p);
        let bump = (*meta).entry_bump;
        if bump == 0 {
            return false;
        }
        let mut scanned = 0u32;
        loop {
            let idx = (*meta).clock_hand % bump;
            (*meta).clock_hand = (idx + 1) % bump;
            let e = self.entries_ptr(p).add(idx as usize);
            if (*e).flags & FLAG_OCCUPIED != 0 {
                // A referenced staged write that has not committed yet: this
                // segment holds the only copy of the value, because it was
                // never copied into the ring. Evicting it would lose a write
                // the client is still waiting to have acknowledged. Leave the
                // reference bit alone so it is reconsidered on the next sweep.
                if (*e).staged_seq > (*meta).commit_watermark {
                    scanned += 1;
                    if scanned > bump * 2 + 4 {
                        return false;
                    }
                    continue;
                }
                // Pinned: configuration, not cache content. Skipped without
                // touching its reference bit, exactly like a staged write.
                if (*e).flags & FLAG_PINNED != 0 {
                    scanned += 1;
                    if scanned > bump * 2 + 4 {
                        return false;
                    }
                    continue;
                }
                if (*e).flags & FLAG_REF != 0 {
                    (*e).flags &= !FLAG_REF;
                } else {
                    // find its bucket and remove
                    let key = std::slice::from_raw_parts(
                        self.data_ptr(p).add((*e).key_off as usize),
                        (*e).key_len as usize,
                    );
                    let (found, _) = self.probe(p, (*e).key_hash, key);
                    if let Some(b) = found {
                        self.remove_at(p, b, idx);
                        (*meta).evictions += 1;
                        return true;
                    }
                }
            }
            scanned += 1;
            if scanned > bump * 2 + 4 {
                return false; // nothing evictable (all pinned this pass)
            }
        }
    }

    /// The entry's current `version`, or None if the key is absent. A ring
    /// record staged by reference carries this so the persistence worker can
    /// tell whether the value it is about to read is still the one it was told
    /// about.
    pub fn version_of(&self, key: &[u8]) -> Option<u64> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            let b = found?;
            let idx = *self.buckets_ptr(p).add(b) - 1;
            Some(stable_version(self.entries_ptr(p).add(idx as usize)))
        }
    }

    /// Record that this key's value is staged by reference at ring sequence
    /// `seq`, so eviction leaves it in place until the persistence worker has
    /// committed that far.
    pub fn set_staged_seq(&self, key: &[u8], seq: u64) {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            let (found, _) = self.probe(p, hash, key);
            if let Some(b) = found {
                let idx = *self.buckets_ptr(p).add(b) - 1;
                (*self.entries_ptr(p).add(idx as usize)).staged_seq = seq;
            }
        }
    }

    /// Publish how far the persistence worker has committed. Eviction uses it
    /// to decide when a referenced value is safe to drop; call it once per
    /// event-loop pass with the minimum committed sequence across rings.
    pub fn set_commit_watermark(&self, w: u64) {
        for p in 0..self.num_partitions {
            unsafe { (*self.meta(p)).commit_watermark = w };
        }
    }

    /// Read a value staged by reference, for the persistence worker.
    ///
    /// `version` is what the ring record recorded at stage time. A mismatch
    /// means the key was overwritten after staging, so a *newer* record for
    /// the same key is already queued behind this one (a key always maps to
    /// one ring, and records are consumed in order). Returning None there is
    /// correct and lossless: the newer record carries the value that should
    /// win, and skipping avoids copying a value that is being rewritten.
    pub fn read_staged(&self, key: &[u8], version: u64) -> Option<(u32, i64, Vec<u8>)> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            for _ in 0..256 {
                let (found, _) = self.probe(p, hash, key);
                let b = found?;
                let idx = *self.buckets_ptr(p).add(b) - 1;
                let e = self.entries_ptr(p).add(idx as usize);
                let v1 = (*e).version.load(Ordering::Acquire);
                if v1 & 1 == 1 {
                    std::hint::spin_loop();
                    continue; // mid-write; wait for it to settle
                }
                if v1 != version {
                    return None; // superseded; a newer record is queued behind
                }
                let exp = (*e).expires_at;
                let kind = (*e).kind;
                let off = (*e).val_off as usize;
                let len = (*e).val_len as usize;
                let mut out = vec![0u8; len];
                std::ptr::copy_nonoverlapping(self.data_ptr(p).add(off), out.as_mut_ptr(), len);
                // Re-check only after the copy. Validating first and handing
                // back a borrow, as this used to, left the caller copying
                // outside the guard: a rewrite in that window put a spliced
                // value into supacache.kv.
                if (*e).version.load(Ordering::Acquire) == v1 {
                    return Some((kind, exp, out));
                }
                std::hint::spin_loop();
            }
            None
        }
    }

    /// Copy a value out under the seqlock, for a reader in another backend.
    ///
    /// `get` hands back a slice that points straight into shared memory, which
    /// is right for the RESP worker (it owns its partition and is the only
    /// writer) and wrong for anyone else: the worker can rewrite the entry
    /// while the caller is still reading, so the caller can see the head of
    /// one value and the tail of another. This samples the version, copies,
    /// and re-samples; a change means the copy is untrustworthy and it starts
    /// again.
    ///
    /// Returns `None` if the key is absent or expired. Contention is resolved
    /// by retrying, since a writer holds the window only for a memcpy.
    pub fn get_stable(&self, key: &[u8]) -> Option<Vec<u8>> {
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        unsafe {
            for _ in 0..256 {
                let (found, _) = self.probe(p, hash, key);
                let b = found?;
                let idx = *self.buckets_ptr(p).add(b) - 1;
                let e = self.entries_ptr(p).add(idx as usize);
                let exp = (*e).expires_at;
                if exp != 0 && exp <= now_micros() {
                    return None;
                }
                let v1 = (*e).version.load(Ordering::Acquire);
                if v1 & 1 == 1 {
                    std::hint::spin_loop();
                    continue; // a write is in flight
                }
                let off = (*e).val_off as usize;
                let len = (*e).val_len as usize;
                let mut out = vec![0u8; len];
                std::ptr::copy_nonoverlapping(self.data_ptr(p).add(off), out.as_mut_ptr(), len);
                if (*e).version.load(Ordering::Acquire) == v1 {
                    return Some(out); // nothing moved underneath us
                }
                std::hint::spin_loop();
            }
            None
        }
    }

    pub fn stats(&self, p: u32) -> PartStats {
        unsafe {
            let m = self.meta(p);
            PartStats {
                entries: (*m).entry_count as u64,
                hits: (*m).hits,
                misses: (*m).misses,
                evictions: (*m).evictions,
                sets: (*m).sets,
                tombstones: (*m).tombstones,
                rehashes: (*m).rehashes,
                data_used: (*m).data_bump,
                data_cap: self.data_bytes,
            }
        }
    }
}

#[inline]
unsafe fn slice_eq(ptr: *const u8, b: &[u8]) -> bool {
    let a = std::slice::from_raw_parts(ptr, b.len());
    a == b
}

#[inline]
fn i64_to_bytes(mut n: i64, buf: &mut [u8; 20]) -> &[u8] {
    if n == 0 {
        buf[0] = b'0';
        return &buf[..1];
    }
    let neg = n < 0;
    let mut i = buf.len();
    // handle i64::MIN safely via u64
    let mut u = if neg {
        (n as i128).unsigned_abs() as u64
    } else {
        n as u64
    };
    let _ = &mut n;
    while u > 0 {
        i -= 1;
        buf[i] = b'0' + (u % 10) as u8;
        u /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    // shift to front
    let len = buf.len() - i;
    buf.copy_within(i.., 0);
    &buf[..len]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_has_a_type_and_an_encoding_name() {
        for (kind, name, enc) in [
            (KIND_STR, "string", &b"embstr"[..]),
            (KIND_HASH, "hash", b"hashtable"),
            (KIND_LIST, "list", b"quicklist"),
            (KIND_ZSET, "zset", b"skiplist"),
            (KIND_SET, "set", b"hashtable"),
            (KIND_BLOOM, "MBbloom--", b"raw"),
            (KIND_CUCKOO, "MBbloomCF", b"raw"),
        ] {
            assert_eq!(type_name(kind), name, "kind {kind}");
            assert_eq!(encoding_name(kind, b"x"), enc, "kind {kind}");
        }
        assert_eq!(encoding_name(KIND_STR, b"42"), b"int");
        assert_eq!(encoding_name(KIND_STR, b"4.2"), b"embstr");
        assert_eq!(type_name(b'?' as u32), "string");
    }

    fn store(name: &str) -> Store {
        let cfg = Config::for_capacity(2, 10_000, 128);
        Store::create(name, &cfg).unwrap()
    }

    #[test]
    fn header_check_accepts_the_segment_it_wrote() {
        let cfg = Config::for_capacity(2, 10_000, 128);
        let s = Store::create("t_hdr_ok", &cfg).unwrap();
        assert!(unsafe { Store::check_header(s.base, &cfg) }.is_ok());
    }

    #[test]
    fn header_check_reports_every_field_that_disagrees() {
        let cfg = Config::for_capacity(2, 10_000, 128);
        let s = Store::create("t_hdr_layout", &cfg).unwrap();
        // Same partition count, different sizing: exactly the shape of a daemon
        // restarted against a live segment with different capacity flags.
        let other = Config::for_capacity(2, 5_000, 128);
        let why = unsafe { Store::check_header(s.base, &other) }.unwrap_err();
        assert!(why.contains("entries per partition"), "{why}");
        assert!(why.contains("buckets per partition"), "{why}");
        assert!(why.contains("partition bytes"), "{why}");
    }

    #[test]
    fn header_check_rejects_memory_that_is_not_a_segment() {
        let zeros = vec![0u8; std::mem::size_of::<SegHeader>()];
        let cfg = Config::for_capacity(2, 10_000, 128);
        let why = unsafe { Store::check_header(zeros.as_ptr(), &cfg) }.unwrap_err();
        assert!(why.contains("not a pg_keyspace segment"), "{why}");
    }

    #[test]
    fn attach_takes_a_matching_segment_and_refuses_a_mismatched_one() {
        let cfg = Config::for_capacity(2, 10_000, 128);
        let _owner = Store::create("t_attach_chk", &cfg).unwrap();
        // The legitimate path still works — this is also the only coverage
        // Store::attach has, since nothing in the tree calls it yet.
        assert!(Store::attach("t_attach_chk", &cfg).is_ok());
        // A smaller layout maps cleanly because it fits inside the segment, so
        // the header is the only thing standing between it and silent garbage.
        let other = Config::for_capacity(2, 5_000, 128);
        let e = match Store::attach("t_attach_chk", &other) {
            Ok(_) => panic!("attached a segment laid out for a different capacity"),
            Err(e) => e,
        };
        assert_eq!(e.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn set_get_del() {
        let s = store("t_setget");
        assert!(matches!(s.get(b"a"), Lookup::Miss));
        assert!(s.set(b"a", b"hello", 0));
        match s.get(b"a") {
            Lookup::Hit(v) => assert_eq!(v, b"hello"),
            _ => panic!("miss"),
        }
        assert!(s.set(b"a", b"worldworld", 0)); // grow across class
        match s.get(b"a") {
            Lookup::Hit(v) => assert_eq!(v, b"worldworld"),
            _ => panic!("miss"),
        }
        assert!(s.del(b"a"));
        assert!(!s.del(b"a"));
        assert!(matches!(s.get(b"a"), Lookup::Miss));
    }

    #[test]
    fn incr_counts() {
        let s = store("t_incr");
        assert_eq!(s.incr(b"c", 1), Some(1));
        assert_eq!(s.incr(b"c", 41), Some(42));
        assert_eq!(s.incr(b"c", -50), Some(-8));
        s.set(b"c", b"notanint", 0);
        assert_eq!(s.incr(b"c", 1), None);
    }

    #[test]
    fn ttl_expires() {
        let s = store("t_ttl");
        s.set(b"k", b"v", 1); // 1 micro
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(matches!(s.get(b"k"), Lookup::Miss));
    }

    #[test]
    fn set_expiry_update_persist_and_past() {
        let s = store("t_setexp");
        assert!(s.set(b"k", b"v", 0));
        // absent key
        assert!(!s.set_expiry(b"missing", now_micros() + 1_000_000));
        // set a future expiry in place (value unchanged)
        assert!(s.set_expiry(b"k", now_micros() + 60_000_000));
        assert!(matches!(s.get_typed(b"k"), Some((_, exp, v)) if exp > now_micros() && v == b"v"));
        // clear the expiry (PERSIST)
        assert!(s.set_expiry(b"k", 0));
        assert!(matches!(s.get_typed(b"k"), Some((_, 0, _))));
        // a past expiry deletes the key but still reports it existed
        assert!(s.set_expiry(b"k", 1));
        assert!(matches!(s.get(b"k"), Lookup::Miss));
    }

    #[test]
    fn version_bumps_on_write_and_clears_on_delete() {
        let s = store("t_version");
        assert_eq!(s.version(b"k"), None); // absent
        s.set(b"k", b"v1", 0);
        let v1 = s.version(b"k").expect("present after set");
        s.set(b"k", b"v2", 0);
        let v2 = s.version(b"k").expect("present after overwrite");
        assert!(v2 > v1, "version must advance on overwrite ({v1} -> {v2})");
        s.del(b"k");
        assert_eq!(s.version(b"k"), None); // gone
    }

    #[test]
    fn scan_visits_every_live_key_and_skips_expired() {
        let s = store("t_scan");
        for i in 0..500 {
            s.set(format!("key:{i}").as_bytes(), b"v", 0);
        }
        let mut seen = std::collections::HashSet::new();
        let mut cursor = 0u64;
        loop {
            let (next, batch) = s.scan(cursor, 32);
            for k in batch {
                seen.insert(k);
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert_eq!(seen.len(), 500);

        // an expired key is never returned by SCAN
        s.set(b"soon", b"v", 1); // ~immediate expiry
        std::thread::sleep(std::time::Duration::from_millis(2));
        let mut cursor = 0u64;
        let mut found_soon = false;
        loop {
            let (next, batch) = s.scan(cursor, 64);
            if batch.iter().any(|k| k.as_slice() == b"soon") {
                found_soon = true;
            }
            if next == 0 {
                break;
            }
            cursor = next;
        }
        assert!(!found_soon);
    }

    #[test]
    fn eviction_under_pressure() {
        // tiny data region forces CLOCK eviction; store must stay live.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 64 * 1024,
        };
        let s = Store::create("t_evict", &cfg).unwrap();
        for i in 0..5000u32 {
            let k = format!("key{i}");
            assert!(s.set(k.as_bytes(), b"0123456789abcdef", 0), "set {i}");
        }
        let st = s.stats(0);
        assert!(st.evictions > 0, "expected evictions, got {}", st.evictions);
        // latest key must still be present
        assert!(matches!(s.get(b"key4999"), Lookup::Hit(_)));
    }

    /// #102: the gap scoped eviction leaves. A tenant that grows *steadily*
    /// rather than flooding is never the one inserting when the arena is
    /// pressured, so the preference never points at it and it keeps whatever it
    /// has. Only a budget takes space back from it.
    ///
    /// `hog` writes first and stops; `late` then writes a modest working set.
    /// Without a budget the hog keeps the arena; with one it is cut back to
    /// roughly its share.
    fn steady_hog_share(pct: u32) -> (usize, usize) {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 256 * 1024,
        };
        let name = format!("t_budget_{pct}");
        let mut s = Store::create(&name, &cfg).unwrap();
        s.set_scoped_eviction(true);
        s.set_tenant_arena_pct(pct);
        // The hog fills most of the arena and then goes quiet.
        for i in 0..400u32 {
            s.set(format!("hog:{i}").as_bytes(), b"0123456789abcdef", 0);
        }
        // A second tenant arrives and writes steadily, needing room.
        for round in 0..6u32 {
            for i in 0..100u32 {
                s.set(format!("late:{round}:{i}").as_bytes(), b"0123456789abcdef", 0);
            }
        }
        let hog = (0..400u32)
            .filter(|i| matches!(s.get(format!("hog:{i}").as_bytes()), Lookup::Hit(_)))
            .count();
        let late = (0..100u32)
            .filter(|i| matches!(s.get(format!("late:5:{i}").as_bytes()), Lookup::Hit(_)))
            .count();
        (hog, late)
    }

    #[test]
    fn a_budget_cuts_back_a_steadily_grown_tenant() {
        let (hog_off, _) = steady_hog_share(0);
        let (hog_on, _) = steady_hog_share(25);
        assert!(
            hog_on < hog_off,
            "a budget must reclaim from the hog: {hog_off} keys without one, {hog_on} with"
        );
    }

    #[test]
    fn a_budget_does_not_starve_the_tenant_it_protects() {
        // The failure mode of an over-eager budget: reclaim so hard that the
        // arriving tenant cannot keep its own recent writes either.
        let (_, late) = steady_hog_share(25);
        assert!(late > 50, "the arriving tenant should hold its recent keys, got {late}/100");
    }

    #[test]
    fn no_budget_is_the_default_and_changes_nothing() {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 256,
            entries_per_part: 128,
            data_bytes_per_part: 32 * 1024,
        };
        let s = Store::create("t_budget_default", &cfg).unwrap();
        assert_eq!(s.arena_pct, 0, "budgets are off unless a caller opts in");
        assert!(unsafe { s.over_budget_scope(0) }.is_none());
    }

    /// The snapshot is measured, never accumulated, so it cannot drift -- this
    /// is the property the design was chosen for, and it is worth asserting
    /// rather than assuming. Churn the arena hard, then check the snapshot
    /// against a fresh count of what is actually resident.
    #[test]
    fn usage_matches_a_fresh_count_after_heavy_churn() {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 256 * 1024,
        };
        let mut s = Store::create("t_usage_drift", &cfg).unwrap();
        s.set_tenant_arena_pct(50);
        for round in 0..8u32 {
            for i in 0..300u32 {
                s.set(format!("a:{round}:{i}").as_bytes(), b"0123456789abcdef", 0);
                s.set(format!("b:{i}").as_bytes(), b"0123456789abcdefghij", 0);
            }
            for i in 0..100u32 {
                s.del(format!("a:{round}:{i}").as_bytes());
            }
        }
        let reported: u64 = s.tenant_usage().iter().map(|(_, _, n)| *n).sum();
        // Count what is really resident, independently of the snapshot.
        let mut actual = 0u64;
        unsafe {
            let meta = s.meta(0);
            for idx in 0..(*meta).entry_bump {
                let e = s.entries_ptr(0).add(idx as usize);
                if (*e).flags & FLAG_OCCUPIED == 0 {
                    continue;
                }
                let key = std::slice::from_raw_parts(
                    s.data_ptr(0).add((*e).key_off as usize),
                    (*e).key_len as usize,
                );
                if tenant_scope(key).is_some() {
                    actual += 1;
                }
            }
        }
        assert_eq!(
            reported, actual,
            "measured usage must equal a fresh count: reported {reported}, actual {actual}"
        );
    }

    /// #110: a wall-clock step must not move the clock.
    ///
    /// A step moves `CLOCK_REALTIME` and leaves `CLOCK_BOOTTIME` alone. A clock
    /// anchored before the step keeps advancing on boottime, so it does not
    /// move -- the anchor's realtime half is a fixed reference, never re-read.
    ///
    /// Tested as arithmetic rather than by re-adopting an anchor: the first
    /// version of this test re-adopted the *same* anchor while its comment
    /// claimed the clock had lurched an hour, which proved nothing at all.
    /// bench/run_ttl_clock_step.sh does the real thing, against a live cluster
    /// with the system clock actually set.
    #[test]
    fn a_wall_clock_step_does_not_move_the_clock() {
        let (r0, b0) = (1_700_000_000_000_000i64, 500_000i64);
        // Two reads 10us apart in boottime terms.
        let before = anchored_now(r0, b0, b0 + 10);
        let after = anchored_now(r0, b0, b0 + 20);
        assert_eq!(after - before, 10, "the clock must advance on boottime alone");
        // The step itself: realtime is now an hour ahead. Nothing in the
        // anchored clock reads it, so the answer is unchanged.
        let real_after_step = r0 + 3_600_000_000;
        assert_ne!(
            anchored_now(r0, b0, b0 + 20),
            real_after_step,
            "a stepped wall clock must not be what the clock reports"
        );
        assert_eq!(anchored_now(r0, b0, b0 + 20), r0 + 20);
    }

    /// The failure mode of the code this replaces, stated so the test says what
    /// it is protecting against: reading CLOCK_REALTIME directly means the step
    /// lands in the answer, and every deadline moves with it.
    #[test]
    fn reading_realtime_directly_would_have_moved_everything() {
        let (r0, b0) = (1_700_000_000_000_000i64, 500_000i64);
        let naive_before = r0;
        let naive_after = r0 + 3_600_000_000; // the step, read straight through
        assert_eq!(
            naive_after - naive_before,
            3_600_000_000,
            "this is what the old clock did, and why TTLs all fired at once"
        );
        // The anchored clock, over the same interval, moves by the boottime
        // delta and nothing else.
        assert_eq!(anchored_now(r0, b0, b0 + 20) - anchored_now(r0, b0, b0), 20);
    }

    #[test]
    fn the_clock_never_goes_backwards() {
        let mut last = now_micros();
        for _ in 0..10_000 {
            let n = now_micros();
            assert!(n >= last, "now_micros went backwards: {last} -> {n}");
            last = n;
        }
    }

    #[test]
    fn the_clock_reports_wall_clock_values() {
        // It must still be an absolute unix timestamp: persisted expires_at and
        // the kv_ttl bucket boundaries depend on that, and a monotonic-valued
        // clock would silently make every persisted deadline meaningless.
        let n = now_micros();
        let real = unsafe {
            let mut ts: libc::timespec = std::mem::zeroed();
            libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts);
            ts.tv_sec as i64 * 1_000_000 + ts.tv_nsec as i64 / 1_000
        };
        assert!(
            (n - real).abs() < 5_000_000,
            "clock reads {n} but the wall clock says {real}; it must stay an absolute timestamp"
        );
    }

    #[test]
    fn every_process_sharing_one_anchor_agrees() {
        // The property a per-process anchor would break: two processes reading
        // the same anchor at the same boottime must produce the same answer, or
        // backends disagree about whether a key is expired.
        let (r0, b0) = (1_700_000_000_000_000i64, 500_000i64);
        assert_eq!(anchored_now(r0, b0, b0 + 42), anchored_now(r0, b0, b0 + 42));
        // And two anchors captured at different moments must NOT: that is the
        // reason the anchor lives in shared memory rather than in each process.
        assert_ne!(anchored_now(r0, b0, b0 + 42), anchored_now(r0 + 999, b0, b0 + 42));
    }

    /// The case #43 describes: "a cold-key flood from one tenant simply evicts
    /// everyone else". The victim writes a small working set once, then the
    /// flood writes cold keys until the arena turns over many times.
    fn flood_survival(scoped: bool) -> usize {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 64 * 1024,
        };
        let name = if scoped { "t_eb_on" } else { "t_eb_off" };
        let mut s = Store::create(name, &cfg).unwrap();
        s.set_scoped_eviction(scoped);
        for i in 0..40u32 {
            assert!(s.set(format!("victim:{i}").as_bytes(), b"0123456789abcdef", 0));
        }
        for i in 0..4000u32 {
            s.set(format!("flood:{i}").as_bytes(), b"0123456789abcdef", 0);
        }
        (0..40u32)
            .filter(|i| matches!(s.get(format!("victim:{i}").as_bytes()), Lookup::Hit(_)))
            .count()
    }

    #[test]
    fn a_cold_flood_evicts_its_own_keys_before_another_tenants() {
        let unscoped = flood_survival(false);
        let scoped = flood_survival(true);
        eprintln!("victim keys surviving a 4000-key cold flood: scoped={scoped}/40 unscoped={unscoped}/40");
        assert!(
            scoped > unscoped,
            "scoped eviction should protect the victim tenant: \
             {scoped} of 40 survived with it on, {unscoped} with it off"
        );
    }

    #[test]
    fn a_tenant_with_nothing_to_evict_still_gets_arena() {
        // The preference must not become a budget: a tenant that owns almost
        // none of the arena has no victim of its own, and must fall through to
        // the ordinary sweep rather than fail its write.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 64 * 1024,
        };
        let mut s = Store::create("t_ev_small", &cfg).unwrap();
        s.set_scoped_eviction(true);
        for i in 0..4000u32 {
            s.set(format!("big:{i}").as_bytes(), b"0123456789abcdef", 0);
        }
        // A brand-new tenant writing into a full arena owned entirely by another.
        for i in 0..20u32 {
            assert!(
                s.set(format!("newcomer:{i}").as_bytes(), b"0123456789abcdef", 0),
                "a tenant with no entries of its own must still be able to write"
            );
        }
        assert!(matches!(s.get(b"newcomer:19"), Lookup::Hit(_)));
    }

    #[test]
    fn a_key_with_no_tenant_scope_is_evicted_as_before() {
        // No ':' means no scope, so the ordinary sweep decides and behaviour is
        // exactly what it was.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 64 * 1024,
        };
        let mut s = Store::create("t_ev_noscope", &cfg).unwrap();
        s.set_scoped_eviction(true);
        for i in 0..5000u32 {
            assert!(s.set(format!("key{i}").as_bytes(), b"0123456789abcdef", 0), "set {i}");
        }
        assert!(s.stats(0).evictions > 0);
        assert!(matches!(s.get(b"key4999"), Lookup::Hit(_)));
    }

    #[test]
    fn a_segment_with_binary_keys_is_never_scoped() {
        // A row-cache key is `relid_le_bytes ++ pk`, and one relid in every 256
        // has 0x3a as its low byte -- so `tenant_scope` finds a ":" that means
        // nothing. Such a segment must simply never turn the preference on.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 64 * 1024,
        };
        let s = Store::create("t_ev_binary", &cfg).unwrap();
        assert!(!s.scoped_eviction, "off unless a caller opts in");
        // 16186 = 0x3F3A: little-endian low byte is ':'.
        let mut key = 16186u32.to_le_bytes().to_vec();
        key.extend_from_slice(b"1");
        assert_eq!(
            tenant_scope(&key),
            Some(&b":"[..]),
            "which is exactly why this segment must not opt in"
        );
        assert!(s.set(&key, b"v", 0));
        assert!(matches!(s.get(&key), Lookup::Hit(_)));
    }

    #[test]
    fn tenant_scope_is_the_prefix_through_the_first_colon() {
        assert_eq!(tenant_scope(b"ta:user:1"), Some(&b"ta:"[..]));
        assert_eq!(tenant_scope(b"ta:"), Some(&b"ta:"[..]));
        assert_eq!(tenant_scope(b"nocolon"), None);
    }

    #[test]
    fn pinned_entries_survive_eviction_pressure() {
        // Same pressure as `eviction_under_pressure`, with one pinned entry
        // written first. Ordinary entries are evicted around it; the pinned one
        // must still be readable at the end.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 64 * 1024,
        };
        let s = Store::create("t_pinned", &cfg).unwrap();
        assert!(s.set_pinned(b"registration", b"cfg"), "pinned write");
        for i in 0..5000u32 {
            let k = format!("key{i}");
            assert!(s.set(k.as_bytes(), b"0123456789abcdef", 0), "set {i}");
        }
        // The pressure has to be real, or the survival below proves nothing.
        let st = s.stats(0);
        assert!(st.evictions > 0, "expected evictions, got {}", st.evictions);
        match s.get(b"registration") {
            Lookup::Hit(v) => assert_eq!(v, b"cfg"),
            Lookup::Miss => panic!("pinned entry was evicted after {} evictions", st.evictions),
        }
    }

    #[test]
    fn pinned_entries_are_still_deletable() {
        // Pinning exempts an entry from the CLOCK sweep, not from an explicit
        // delete -- otherwise rowcache_unregister could never take effect.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 256,
            entries_per_part: 128,
            data_bytes_per_part: 32 * 1024,
        };
        let s = Store::create("t_pinned_del", &cfg).unwrap();
        assert!(s.set_pinned(b"reg", b"v"));
        assert!(matches!(s.get(b"reg"), Lookup::Hit(_)));
        assert!(s.del(b"reg"));
        assert!(matches!(s.get(b"reg"), Lookup::Miss));
    }

    #[test]
    fn oversized_value_reuse_does_not_leak() {
        // A modest data arena and a single key whose (>8KB) oversized value is
        // rewritten thousands of times. Before the oversized free list this
        // leaked a fresh region per write and the arena exhausted in ~dozens of
        // writes; now the block is reused in place (or reclaimed on regrow), so
        // every write succeeds and the value reads back correctly.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: 4 * 1024 * 1024, // 4MB: far smaller than 2000×16KB
        };
        let s = Store::create("t_oversize_reuse", &cfg).unwrap();
        let big = vec![b'x'; 16 * 1024]; // 16KB -> OVERSIZED
        for i in 0..2000u32 {
            assert!(s.set(b"big", &big, 0), "oversized set {i} failed (arena leak?)");
        }
        match s.get(b"big") {
            Lookup::Hit(v) => assert_eq!(v.len(), 16 * 1024),
            _ => panic!("miss"),
        }

        // Grow-then-shrink across doublings, and reuse a freed oversized block
        // for a different key.
        assert!(s.set(b"big", &vec![b'y'; 40 * 1024], 0)); // grow (regrow block)
        assert!(s.set(b"big", &vec![b'z'; 9 * 1024], 0)); // shrink (still oversized)
        assert!(s.del(b"big")); // frees the oversized block
        assert!(s.set(b"other", &vec![b'q'; 20 * 1024], 0)); // reuses from free list
        match s.get(b"other") {
            Lookup::Hit(v) => assert_eq!(v.len(), 20 * 1024),
            _ => panic!("miss other"),
        }
    }

    /// A value staged by reference is the only copy there is: it was never
    /// copied into the ring, so evicting it before the persistence worker
    /// commits would lose a write the client is still waiting on. Eviction
    /// must therefore skip it until the commit watermark catches up.
    #[test]
    fn eviction_spares_a_referenced_value_until_it_commits() {
        // Small arena so CLOCK is forced to evict on almost every write.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 256,
            data_bytes_per_part: 256 * 1024,
        };
        let s = Store::create("t_staged_evict", &cfg).unwrap();

        let big = vec![b'v'; 16 * 1024]; // OVERSIZED, the referenced path
        assert!(s.set(b"staged", &big, 0));
        let version = s.version_of(b"staged").expect("entry present");
        s.set_staged_seq(b"staged", 42); // staged at ring seq 42
        s.set_commit_watermark(41); // ... not committed yet

        // Hammer the arena. Without the guard CLOCK reclaims the big value.
        for i in 0..2000u32 {
            s.set(format!("filler{i}").as_bytes(), &[b'x'; 512], 0);
        }
        match s.read_staged(b"staged", version) {
            Some((_, _, v)) => assert_eq!(v.len(), big.len(), "value corrupted"),
            None => panic!("uncommitted referenced value was evicted: the write is lost"),
        }

        // Once the worker has committed past it, it is ordinary cache data.
        s.set_commit_watermark(42);
        for i in 0..2000u32 {
            s.set(format!("later{i}").as_bytes(), &[b'x'; 512], 0);
        }
        assert!(
            s.version_of(b"staged").is_none(),
            "a committed entry must be evictable again, or the arena fills with pins"
        );
    }

    /// The version on the record is what makes a stale reference safe: if the
    /// key was overwritten after staging, the worker must not persist the new
    /// bytes under the old record. A newer record is already queued for it.
    #[test]
    fn read_staged_rejects_a_superseded_version() {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 256,
            data_bytes_per_part: 1024 * 1024,
        };
        let s = Store::create("t_staged_version", &cfg).unwrap();
        let big = vec![b'a'; 16 * 1024];
        assert!(s.set(b"k", &big, 0));
        let v1 = s.version_of(b"k").unwrap();
        assert!(s.read_staged(b"k", v1).is_some());

        // Overwrite: the old reference must stop resolving.
        assert!(s.set(b"k", &vec![b'b'; 16 * 1024], 0));
        assert!(
            s.read_staged(b"k", v1).is_none(),
            "a superseded reference must not resolve to the newer value"
        );
        let v2 = s.version_of(b"k").unwrap();
        assert_ne!(v1, v2);
        assert!(s.read_staged(b"k", v2).is_some());
    }

    /// Oversized space must be genuinely reclaimed, not merely listed.
    ///
    /// Blocks are laid out contiguously by bump. Freeing one used to push it
    /// onto a LIFO list with no coalescing and no way to move `data_bump` back
    /// down, so a workload of growing values consumed fresh arena on every step
    /// while the free list filled with blocks individually too small to serve
    /// the next request. Eviction could not recover it either, since eviction
    /// frees onto the same list.
    ///
    /// `data_used` is `data_bump`, so the reclamation is directly observable.
    #[test]
    fn oversized_space_is_reclaimed_not_just_listed() {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 256,
            data_bytes_per_part: 8 * 1024 * 1024,
        };
        let s = Store::create("t_oversize_frag", &cfg).unwrap();
        let used = || s.stats(0).data_used;

        // Warm up so the key's own slab block (a size class, not oversized) is
        // already bumped and reused thereafter; only the value's oversized
        // block should move the bump pointer from here on.
        assert!(s.set(b"big", b"small", 0));
        assert!(s.del(b"big"));
        let baseline = used();

        assert!(s.set(b"big", &vec![b'x'; 1024 * 1024], 0));
        let peak = used();
        assert!(peak > baseline + 1024 * 1024, "1 MiB write should consume arena");
        assert!(s.del(b"big"));
        assert_eq!(
            used(),
            baseline,
            "freeing the top block must return its space to the bump pointer,              not strand it on the free list"
        );

        // A strictly growing series, each value freed before the next. Every
        // step needs more than any block the free list holds, so without
        // reclamation each one consumes fresh arena and the total far exceeds
        // the 8 MiB available.
        assert!(s.set(b"grow", b"small", 0)); // warm the key's slab block
        assert!(s.del(b"grow"));
        let grow_baseline = used();
        let mut size = 64 * 1024usize;
        while size <= 4 * 1024 * 1024 {
            assert!(
                s.set(b"grow", &vec![b'y'; size], 0),
                "{size}-byte write failed: freed oversized space never became                  reusable (data_used {} of {})",
                used(),
                s.stats(0).data_cap
            );
            assert!(s.del(b"grow"));
            assert_eq!(used(), grow_baseline, "{size}-byte round leaked arena");
            size *= 2;
        }
    }

    /// A failed overwrite must leave the previous value intact.
    ///
    /// The old code released the existing block before allocating the
    /// replacement, so when the allocation then failed the entry still pointed
    /// at freed space: the previous value was destroyed even though the write
    /// failed, and a later read returned whatever had since been handed to
    /// another key. Allocation also evicts, and CLOCK could evict the very
    /// entry being overwritten, after which the old code wrote through a
    /// pointer to a freed slot.
    #[test]
    fn failed_overwrite_leaves_the_old_value_intact() {
        // Room for one oversized value and little else.
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 64,
            data_bytes_per_part: 512 * 1024,
        };
        let s = Store::create("t_failed_overwrite", &cfg).unwrap();

        let original = vec![b'a'; 64 * 1024];
        assert!(s.set(b"k", &original, 0), "initial write should fit");

        // Ask for more than the arena can ever hold. This must fail...
        let impossible = vec![b'b'; 4 * 1024 * 1024];
        assert!(!s.set(b"k", &impossible, 0), "oversized overwrite must fail");

        // ...and must not have disturbed what was already there.
        match s.get(b"k") {
            Lookup::Hit(v) => {
                assert_eq!(v.len(), original.len(), "old value truncated by a failed write");
                assert!(
                    v.iter().all(|&b| b == b'a'),
                    "old value corrupted by a failed write: read back bytes that are                      not the value we stored, so the entry pointed at reclaimed space"
                );
            }
            Lookup::Miss => panic!("a failed overwrite destroyed the existing value"),
        }

        // The key is still writable afterwards, so nothing was left wedged.
        assert!(s.set(b"k", &vec![b'c'; 1024], 0));
        match s.get(b"k") {
            Lookup::Hit(v) => assert!(v.len() == 1024 && v.iter().all(|&b| b == b'c')),
            Lookup::Miss => panic!("key lost after a successful rewrite"),
        }
    }

    /// A write too large for the arena must be refused without evicting.
    ///
    /// `ensure_alloc` evicts in a loop until the allocation succeeds or nothing
    /// is evictable. For a request larger than the whole arena that loop can
    /// never succeed, so it used to evict every entry in the keyspace and then
    /// fail anyway: one oversized write destroyed the entire cache.
    #[test]
    fn impossible_write_does_not_evict_the_keyspace() {
        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 256,
            data_bytes_per_part: 512 * 1024,
        };
        let s = Store::create("t_impossible_write", &cfg).unwrap();

        for i in 0..100u32 {
            assert!(s.set(format!("k{i}").as_bytes(), &[b'v'; 256], 0));
        }
        let before = s.stats(0).evictions;

        // Larger than the whole arena: cannot ever be served.
        assert!(!s.set(b"impossible", &vec![b'x'; 4 * 1024 * 1024], 0));

        let after = s.stats(0).evictions;
        assert_eq!(
            before, after,
            "an impossible write evicted {} entries before giving up",
            after - before
        );
        let mut alive = 0;
        for i in 0..100u32 {
            if matches!(s.get(format!("k{i}").as_bytes()), Lookup::Hit(_)) {
                alive += 1;
            }
        }
        assert_eq!(alive, 100, "an impossible write destroyed existing keys");
    }

    /// A reader in another backend must never see half of one value and half
    /// of another.
    ///
    /// `get` hands back a slice into shared memory, so a caller that is not the
    /// owning worker reads it while the worker may be rewriting that key in
    /// place. This drives exactly that race: one thread alternates between two
    /// values of the same length made of distinct bytes, another reads
    /// concurrently, and every observation must be wholly one or wholly the
    /// other. Run with `get` instead of `get_stable` and it fails.
    #[test]
    fn concurrent_reader_never_observes_a_spliced_value() {
        use std::sync::atomic::{AtomicBool, Ordering as O};
        use std::sync::Arc;

        let cfg = Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 64,
            data_bytes_per_part: 1024 * 1024,
        };
        let s = Arc::new(Store::create("t_seqlock_tear", &cfg).unwrap());
        // 4 KiB: a size class, so rewrites land in place, which is the case
        // that splices rather than swapping a pointer.
        let a = vec![b'a'; 4096];
        let b = vec![b'b'; 4096];
        assert!(s.set(b"k", &a, 0));

        let stop = Arc::new(AtomicBool::new(false));
        let writer = {
            let (s, stop, a, b) = (s.clone(), stop.clone(), a.clone(), b.clone());
            std::thread::spawn(move || {
                while !stop.load(O::Relaxed) {
                    s.set(b"k", &a, 0);
                    s.set(b"k", &b, 0);
                }
            })
        };

        let mut reads = 0u64;
        let mut torn = 0u64;
        for _ in 0..200_000 {
            if let Some(v) = s.get_stable(b"k") {
                reads += 1;
                let first = v[0];
                if v.len() != 4096 || v.iter().any(|&c| c != first) {
                    torn += 1;
                }
            }
        }
        stop.store(true, O::Relaxed);
        writer.join().unwrap();

        assert!(reads > 1000, "test did not actually read much ({reads})");
        assert_eq!(
            torn, 0,
            "{torn} of {reads} reads observed a spliced value: the reader saw              bytes from two different writes in one buffer"
        );
    }

    #[test]
    fn with_value_mut_returns_none_for_an_absent_key() {
        let s = store("t_wvm_absent");
        assert!(s.with_value_mut(b"nope", |_v| ()).is_none());
    }

    #[test]
    fn with_value_mut_edits_the_value_in_place() {
        let s = store("t_wvm_inplace");
        let val = vec![0x0fu8; 64];
        assert!(s.set_typed(b"f", &val, 60_000_000, KIND_BLOOM));
        let (_, exp_before, _) = s.get_typed(b"f").expect("present after set");

        let n = s
            .with_value_mut(b"f", |v| {
                for b in v.iter_mut() {
                    *b ^= 0xff;
                }
                v.len()
            })
            .expect("present");
        assert_eq!(n, 64);

        match s.get(b"f") {
            Lookup::Hit(v) => assert_eq!(v, vec![0xf0u8; 64].as_slice()),
            _ => panic!("miss after in-place edit"),
        }
        let (kind, exp_after, v) = s.get_typed(b"f").expect("still present");
        assert_eq!(kind, KIND_BLOOM);
        assert_eq!(exp_after, exp_before);
        assert_eq!(v, vec![0xf0u8; 64].as_slice());
    }

    #[test]
    fn with_value_mut_bumps_the_version_and_leaves_it_even() {
        let s = store("t_wvm_version");
        assert!(s.set(b"f", &[1u8; 64], 0));
        let before = s.version(b"f").expect("present after set");
        assert!(s.with_value_mut(b"f", |v| v[0] = 2).is_some());
        let after = s.version(b"f").expect("present after edit");
        assert!(
            after >= before + 2,
            "version must advance by a full write ({before} -> {after})"
        );
        assert_eq!(after % 2, 0, "version must settle even ({after})");
    }

    #[test]
    fn with_value_mut_closes_the_seqlock_when_the_closure_panics() {
        let s = store("t_wvm_panic");
        assert!(s.set(b"f", &[1u8; 64], 0));
        let before = s.version(b"f").expect("present after set");
        let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            s.with_value_mut(b"f", |v| {
                v[0] = 9;
                panic!("the closure fails halfway");
            })
        }));
        assert!(r.is_err(), "the panic must reach the caller");
        let after = s.version(b"f").expect("present after the panic");
        assert_eq!(after % 2, 0, "version must settle even ({after})");
        assert!(after >= before + 2, "version must advance ({before} -> {after})");
        assert!(s.with_value_mut(b"f", |v| v[0] = 3).is_some());
        assert_eq!(s.get_typed(b"f").unwrap().2[0], 3);
    }

    #[test]
    fn with_value_mut_returns_none_for_an_expired_key() {
        let s = store("t_wvm_expired");
        assert!(s.set(b"f", &[1u8; 64], 1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        assert!(s.with_value_mut(b"f", |v| v[0] = 2).is_none());
        assert!(matches!(s.get(b"f"), Lookup::Miss));
        assert_eq!(s.version(b"f"), None);
    }

    #[test]
    fn with_value_mut_edits_an_oversized_value_in_place() {
        let s = store("t_wvm_oversized");
        let len = 16 * 1024;
        assert!(s.set_typed(b"big", &vec![0u8; len], 0, KIND_CUCKOO));
        assert!(s
            .with_value_mut(b"big", |v| {
                assert_eq!(v.len(), len);
                v[len - 1] = 7;
                v[0] = 7;
            })
            .is_some());
        let (kind, _, v) = s.get_typed(b"big").expect("present");
        assert_eq!(kind, KIND_CUCKOO);
        assert_eq!(v.len(), len);
        assert_eq!(v[0], 7);
        assert_eq!(v[len - 1], 7);
        assert!(v[1..len - 1].iter().all(|&b| b == 0));
    }
}
