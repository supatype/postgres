//! The shared-memory keyspace store. One `Store` maps the whole segment; it is
//! carved into `num_partitions` disjoint partitions. Each partition is written
//! by exactly one slot worker (§3.1), so the hot path takes no locks and no
//! atomics. Layout, per partition (§3.2):
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

const MAGIC: u64 = 0x70_67_6b_73_5f_76_31_00; // "pgks_v1\0"

// Size classes for the slab allocator (§3.2 "size-classed, 32B..8KB").
const CLASS_SIZES: [usize; 9] = [32, 64, 128, 256, 512, 1024, 2048, 4096, 8192];
const NUM_CLASSES: usize = CLASS_SIZES.len();
const OVERSIZED: u32 = u32::MAX; // value larger than 8KB: bump-only ("overflow to heap-only")

const BUCKET_EMPTY: u32 = 0;
const BUCKET_TOMB: u32 = u32::MAX;

const FLAG_OCCUPIED: u32 = 1;
const FLAG_REF: u32 = 2; // CLOCK reference bit

pub const KIND_STR: u32 = b's' as u32;

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
    hits: u64,
    misses: u64,
    evictions: u64,
    sets: u64,
    tombstones: u64,
    rehashes: u64,
    _pad: [u64; 1],
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
    version: u64,
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

#[inline]
pub fn now_micros() -> i64 {
    unsafe {
        let mut ts: libc::timespec = std::mem::zeroed();
        libc::clock_gettime(libc::CLOCK_REALTIME, &mut ts);
        ts.tv_sec as i64 * 1_000_000 + ts.tv_nsec as i64 / 1_000
    }
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
    /// POSIX shared memory owned by this handle (poc daemon, tests).
    Posix(Shmem),
    /// A raw region owned by someone else — e.g. a Postgres shared-memory
    /// segment from `ShmemInitStruct`, mapped at the same address in every
    /// backend (§3.2). The store only borrows it.
    Raw,
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
}

// A partition is written by exactly one worker; SQL-surface readers in other
// backends observe a consistent-enough view for a POC (the plan notes a seqlock
// is the production answer). The raw base is stable across backends.
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
        }
    }

    fn init_header(&self) {
        unsafe {
            let h = self.base as *mut SegHeader;
            (*h).magic = MAGIC;
            (*h).version = 1;
            (*h).num_partitions = self.num_partitions;
            (*h).partition_bytes = self.partition_bytes as u64;
            (*h).buckets_per_part = self.buckets;
            (*h).entries_per_part = self.entries;
            (*h).data_bytes_per_part = self.data_bytes;
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
            let need = align_up(size, 8) as u64;
            if (*meta).data_bump + need > self.data_bytes {
                return None;
            }
            let off = (*meta).data_bump;
            (*meta).data_bump += need;
            return Some((off, OVERSIZED));
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

    unsafe fn slab_free(&self, p: u32, off: u64, cls: u32) {
        if cls == OVERSIZED {
            return; // bump-only; reclaimed on segment reset (rare in practice)
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
    /// partition. Lazily expires (§3.3). Updates CLOCK ref bit and stats.
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
        let hash = fnv1a(key);
        let p = self.partition_for_hash(hash);
        let exp = if ttl_micros > 0 {
            now_micros() + ttl_micros
        } else {
            0
        };
        unsafe {
            (*self.meta(p)).sets += 1;
            self.set_in(p, hash, key, val, exp, KIND_STR)
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
            let e = self.entries_ptr(p).add(idx as usize);
            let want = class_for(val.len());
            if want == (*e).val_class && want != OVERSIZED {
                let vp = self.data_ptr(p).add((*e).val_off as usize);
                std::ptr::copy_nonoverlapping(val.as_ptr(), vp, val.len());
                (*e).val_len = val.len() as u32;
            } else {
                self.slab_free(p, (*e).val_off, (*e).val_class);
                let (voff, vcls) = match self.ensure_alloc(p, val.len()) {
                    Some(x) => x,
                    None => return false,
                };
                let vp = self.data_ptr(p).add(voff as usize);
                std::ptr::copy_nonoverlapping(val.as_ptr(), vp, val.len());
                (*e).val_off = voff;
                (*e).val_class = vcls;
                (*e).val_len = val.len() as u32;
            }
            (*e).expires_at = exp;
            (*e).version += 1;
            (*e).flags |= FLAG_REF;
            (*e).kind = kind;
            return true;
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
                    if !self.evict_one(p) {
                        return false;
                    }
                }
            }
        };
        let (koff, kcls) = match self.ensure_alloc(p, key.len()) {
            Some(x) => x,
            None => {
                self.entry_free(p, idx);
                return false;
            }
        };
        let (voff, vcls) = match self.ensure_alloc(p, val.len()) {
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
        (*e).version = 1;
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
    unsafe fn ensure_alloc(&self, p: u32, size: usize) -> Option<(u64, u32)> {
        loop {
            if let Some(x) = self.slab_alloc(p, size) {
                return Some(x);
            }
            if !self.evict_one(p) {
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
    unsafe fn evict_one(&self, p: u32) -> bool {
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

    fn store(name: &str) -> Store {
        let cfg = Config::for_capacity(2, 10_000, 128);
        Store::create(name, &cfg).unwrap()
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
}
