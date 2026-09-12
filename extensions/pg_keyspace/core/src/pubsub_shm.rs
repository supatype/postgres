//! Shared-memory backing for the pub/sub bus, for deployments where the workers
//! are separate *processes* rather than threads of one.
//!
//! [`crate::pubsub::Bus`] routes a PUBLISH on one worker to subscribers on any
//! other. Its in-process backing is a `Mutex<HashMap>` plus per-worker
//! `VecDeque` inboxes, which is correct only while the workers share an address
//! space. The in-PG extension runs N background workers, each its own process,
//! so that backing silently delivers nothing across workers: a SUBSCRIBE on
//! worker 0 never sees a PUBLISH on worker 2, and the publisher's reply counts
//! only its own local subscribers, so neither side can tell.
//!
//! This module is the same structure laid out in a shared segment:
//!
//! - a **routing table** of fixed-width entries (channel or pattern to
//!   per-worker subscriber counts), guarded by one spinlock. Subscribe and
//!   unsubscribe happen at connection setup, so the lock is uncontended in
//!   practice, and PUBLISH holds it only long enough to total the target set.
//! - an **inbox per ordered worker pair**. Making the rings per `(from, to)`
//!   rather than per `to` keeps every one of them single-producer /
//!   single-consumer, which is the discipline [`crate::ring`] already relies on,
//!   and avoids a lock on the delivery path entirely. The cost is `n^2` rings,
//!   which is why they are small and the worker count is bounded.
//!
//! Waking the reader is left to the caller. It already owns a wake descriptor
//! per worker, and because Postgres forks its background workers from the
//! postmaster, a descriptor created before the fork is usable by every worker
//! without any further machinery.

use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

/// Upper bound on workers a shared bus can address.
///
/// Fixed rather than configurable because it sizes every routing entry: the
/// per-worker counts are inline, so a PUBLISH totals them without chasing a
/// pointer through the segment.
pub const MAX_WORKERS: usize = 64;

/// Longest channel or pattern name the routing table stores.
pub const MAX_CHAN: usize = 224;

const HDR_ALIGN: usize = 64;
const MAGIC: u64 = 0x7067_6b73_6275_7331;

/// Message kinds as framed in an inbox ring.
const KIND_PUBLISH: u8 = 1;
const KIND_INVAL_KEY: u8 = 2;
const KIND_INVAL_ALL: u8 = 3;

/// Frame header: kind, then the two payload lengths.
const FRAME_HDR: usize = 1 + 4 + 4;

#[repr(C)]
struct Header {
    magic: u64,
    nworkers: u32,
    max_routes: u32,
    ring_bytes: u64,
    /// Connections with CLIENT TRACKING on, summed across every worker.
    trackers: AtomicU64,
    /// Routing table spinlock: 0 free, 1 held.
    routes_lock: AtomicU32,
    _pad: u32,
    /// Messages a publisher could not enqueue because the target ring was full.
    dropped: AtomicU64,
    /// Routing inserts refused because the table was full.
    route_full: AtomicU64,
    /// Subscriptions refused because the name exceeded `MAX_CHAN`.
    name_too_long: AtomicU64,
}

/// One routing entry. `state` is 0 when free; a live entry owns `key[..key_len]`.
#[repr(C)]
struct Route {
    state: AtomicU32,
    is_pattern: u32,
    key_len: u32,
    _pad: u32,
    counts: [AtomicU32; MAX_WORKERS],
    key: [u8; MAX_CHAN],
}

/// SPSC byte ring header for one ordered worker pair.
#[repr(C)]
struct RingHdr {
    head: AtomicU64,
    tail: AtomicU64,
}

fn align_up(n: usize, a: usize) -> usize {
    (n + a - 1) & !(a - 1)
}

fn hdr_bytes() -> usize {
    align_up(std::mem::size_of::<Header>(), HDR_ALIGN)
}

fn route_bytes() -> usize {
    align_up(std::mem::size_of::<Route>(), HDR_ALIGN)
}

fn ring_hdr_bytes() -> usize {
    align_up(std::mem::size_of::<RingHdr>(), HDR_ALIGN)
}

/// Bytes needed for a bus serving `nworkers`, with `max_routes` distinct
/// channels and patterns and `ring_bytes` of queue per ordered worker pair.
///
/// The ring count is `nworkers * nworkers` rather than `nworkers`, because each
/// `(from, to)` pair gets its own single-producer ring. The diagonal is never
/// used, since a worker delivers to its own subscribers directly, but it is
/// allocated anyway so indexing stays a multiply.
pub fn bytes_for(nworkers: usize, max_routes: usize, ring_bytes: usize) -> usize {
    let cap = ring_bytes.next_power_of_two();
    hdr_bytes() + max_routes * route_bytes() + nworkers * nworkers * (ring_hdr_bytes() + cap)
}

/// A bus laid out in a shared segment.
///
/// Copying is cheap and every copy addresses the same segment: this is a handle,
/// not an owner, and dropping it frees nothing.
#[derive(Clone, Copy)]
pub struct ShmBus {
    base: *mut u8,
    nworkers: usize,
    max_routes: usize,
    ring_cap: usize,
}

// Safety: every field is a plain value or a pointer into a shared segment whose
// mutable state is exclusively atomics or spinlock-guarded. The handle itself
// carries no interior mutability.
unsafe impl Send for ShmBus {}
unsafe impl Sync for ShmBus {}

impl ShmBus {
    /// Initialise a bus in `base`. The creator calls this exactly once.
    ///
    /// # Safety
    /// `base` must point at `bytes_for(nworkers, max_routes, ring_bytes)`
    /// writable bytes that no other process is reading yet.
    pub unsafe fn init(
        base: *mut u8,
        nworkers: usize,
        max_routes: usize,
        ring_bytes: usize,
    ) -> ShmBus {
        assert!(nworkers >= 1 && nworkers <= MAX_WORKERS);
        let cap = ring_bytes.next_power_of_two();
        let h = base as *mut Header;
        (*h).magic = MAGIC;
        (*h).nworkers = nworkers as u32;
        (*h).max_routes = max_routes as u32;
        (*h).ring_bytes = cap as u64;
        (*h).trackers.store(0, Ordering::Relaxed);
        (*h).routes_lock.store(0, Ordering::Relaxed);
        (*h).dropped.store(0, Ordering::Relaxed);
        (*h).route_full.store(0, Ordering::Relaxed);
        (*h).name_too_long.store(0, Ordering::Relaxed);

        let bus = ShmBus { base, nworkers, max_routes, ring_cap: cap };
        for i in 0..max_routes {
            (*bus.route(i)).state.store(0, Ordering::Relaxed);
        }
        for i in 0..nworkers * nworkers {
            let r = bus.ring_hdr(i);
            (*r).head.store(0, Ordering::Relaxed);
            (*r).tail.store(0, Ordering::Relaxed);
        }
        bus
    }

    /// Attach to a bus another process initialised.
    ///
    /// `None` when the segment does not carry this module's magic, which is the
    /// case worth catching: a stale or mismatched segment read as a routing
    /// table would deliver messages to arbitrary workers.
    ///
    /// # Safety
    /// `base` must point at a region previously passed to [`ShmBus::init`].
    pub unsafe fn attach(base: *mut u8) -> Option<ShmBus> {
        let h = base as *const Header;
        if (*h).magic != MAGIC {
            return None;
        }
        Some(ShmBus {
            base,
            nworkers: (*h).nworkers as usize,
            max_routes: (*h).max_routes as usize,
            ring_cap: (*h).ring_bytes as usize,
        })
    }

    pub fn nworkers(&self) -> usize {
        self.nworkers
    }

    fn hdr(&self) -> *mut Header {
        self.base as *mut Header
    }

    unsafe fn route(&self, i: usize) -> *mut Route {
        self.base.add(hdr_bytes() + i * route_bytes()) as *mut Route
    }

    fn rings_base(&self) -> usize {
        hdr_bytes() + self.max_routes * route_bytes()
    }

    fn ring_stride(&self) -> usize {
        ring_hdr_bytes() + self.ring_cap
    }

    unsafe fn ring_hdr(&self, idx: usize) -> *mut RingHdr {
        self.base.add(self.rings_base() + idx * self.ring_stride()) as *mut RingHdr
    }

    unsafe fn ring_data(&self, idx: usize) -> *mut u8 {
        self.base
            .add(self.rings_base() + idx * self.ring_stride() + ring_hdr_bytes())
    }

    /// Index of the ring carrying `from` to `to`.
    fn ring_index(&self, from: usize, to: usize) -> usize {
        from * self.nworkers + to
    }

    // ---- routing table -------------------------------------------------

    fn lock_routes(&self) {
        let h = self.hdr();
        unsafe {
            let mut spins = 0u32;
            while (*h)
                .routes_lock
                .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                .is_err()
            {
                spins = spins.wrapping_add(1);
                if spins % 64 == 0 {
                    std::thread::yield_now();
                } else {
                    std::hint::spin_loop();
                }
            }
        }
    }

    fn unlock_routes(&self) {
        unsafe { (*self.hdr()).routes_lock.store(0, Ordering::Release) };
    }

    /// Find a live entry for `key`. The caller holds the routes lock.
    unsafe fn find(&self, key: &[u8], pattern: bool) -> Option<usize> {
        for i in 0..self.max_routes {
            let r = self.route(i);
            if (*r).state.load(Ordering::Relaxed) == 0 {
                continue;
            }
            if ((*r).is_pattern != 0) != pattern {
                continue;
            }
            let n = (*r).key_len as usize;
            if n == key.len() && &(&(*r).key)[..n] == key {
                return Some(i);
            }
        }
        None
    }

    /// Record that worker `wid` gained a subscriber on `key`.
    ///
    /// A name longer than [`MAX_CHAN`] is counted and ignored rather than
    /// truncated. Truncating would silently merge two different channels into
    /// one routing entry, delivering each one's messages to the other's
    /// subscribers.
    pub fn subscribe(&self, wid: usize, key: &[u8], pattern: bool) {
        if key.len() > MAX_CHAN || wid >= self.nworkers {
            unsafe { (*self.hdr()).name_too_long.fetch_add(1, Ordering::Relaxed) };
            return;
        }
        self.lock_routes();
        unsafe {
            let idx = match self.find(key, pattern) {
                Some(i) => Some(i),
                None => {
                    let mut free = None;
                    for i in 0..self.max_routes {
                        if (*self.route(i)).state.load(Ordering::Relaxed) == 0 {
                            free = Some(i);
                            break;
                        }
                    }
                    match free {
                        Some(i) => {
                            let r = self.route(i);
                            (*r).is_pattern = pattern as u32;
                            (*r).key_len = key.len() as u32;
                            (&mut (*r).key)[..key.len()].copy_from_slice(key);
                            for w in 0..MAX_WORKERS {
                                (*r).counts[w].store(0, Ordering::Relaxed);
                            }
                            (*r).state.store(1, Ordering::Relaxed);
                            Some(i)
                        }
                        None => {
                            (*self.hdr()).route_full.fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    }
                }
            };
            if let Some(i) = idx {
                (*self.route(i)).counts[wid].fetch_add(1, Ordering::Relaxed);
            }
        }
        self.unlock_routes();
    }

    /// Record that worker `wid` lost a subscriber on `key`, freeing the entry
    /// once no worker holds one.
    pub fn unsubscribe(&self, wid: usize, key: &[u8], pattern: bool) {
        if key.len() > MAX_CHAN || wid >= self.nworkers {
            return;
        }
        self.lock_routes();
        unsafe {
            if let Some(i) = self.find(key, pattern) {
                let r = self.route(i);
                let c = &(*r).counts[wid];
                if c.load(Ordering::Relaxed) > 0 {
                    c.fetch_sub(1, Ordering::Relaxed);
                }
                let any = (0..self.nworkers).any(|w| (*r).counts[w].load(Ordering::Relaxed) > 0);
                if !any {
                    (*r).state.store(0, Ordering::Relaxed);
                }
            }
        }
        self.unlock_routes();
    }

    /// Workers other than `from` holding a subscriber for `channel`, with their
    /// subscriber counts, matching patterns through `glob`.
    pub fn targets<F: Fn(&[u8], &[u8]) -> bool>(
        &self,
        from: usize,
        channel: &[u8],
        glob: F,
    ) -> Vec<(usize, usize)> {
        let mut acc = vec![0usize; self.nworkers];
        self.lock_routes();
        unsafe {
            for i in 0..self.max_routes {
                let r = self.route(i);
                if (*r).state.load(Ordering::Relaxed) == 0 {
                    continue;
                }
                let n = (*r).key_len as usize;
                let name = &(&(*r).key)[..n];
                let hit = if (*r).is_pattern != 0 {
                    glob(name, channel)
                } else {
                    name == channel
                };
                if !hit {
                    continue;
                }
                for (w, slot) in acc.iter_mut().enumerate() {
                    *slot += (*r).counts[w].load(Ordering::Relaxed) as usize;
                }
            }
        }
        self.unlock_routes();
        acc.into_iter()
            .enumerate()
            .filter(|&(w, c)| c > 0 && w != from)
            .collect()
    }

    // ---- inbox rings ---------------------------------------------------

    /// Append a frame to the `from` to `to` ring. False when it would not fit,
    /// in which case the message is dropped and counted.
    ///
    /// Dropping is deliberate. The alternative is blocking the publisher on a
    /// worker that is not draining, which turns one stalled subscriber into a
    /// stalled keyspace; Redis makes the same trade for slow pub/sub consumers.
    /// The counter is what stops it being silent.
    unsafe fn push(&self, from: usize, to: usize, kind: u8, a: &[u8], b: &[u8]) -> bool {
        let idx = self.ring_index(from, to);
        let h = self.ring_hdr(idx);
        let cap = self.ring_cap as u64;
        let need = (FRAME_HDR + a.len() + b.len()) as u64;
        let head = (*h).head.load(Ordering::Acquire);
        let tail = (*h).tail.load(Ordering::Relaxed);
        if need > cap || tail.wrapping_sub(head) + need > cap {
            (*self.hdr()).dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        let data = self.ring_data(idx);
        let mut pos = tail;
        let mut hdr = [0u8; FRAME_HDR];
        hdr[0] = kind;
        hdr[1..5].copy_from_slice(&(a.len() as u32).to_le_bytes());
        hdr[5..9].copy_from_slice(&(b.len() as u32).to_le_bytes());
        for chunk in [&hdr[..], a, b] {
            let mut off = 0usize;
            while off < chunk.len() {
                let at = (pos & (cap - 1)) as usize;
                let n = std::cmp::min(chunk.len() - off, self.ring_cap - at);
                std::ptr::copy_nonoverlapping(chunk.as_ptr().add(off), data.add(at), n);
                off += n;
                pos = pos.wrapping_add(n as u64);
            }
        }
        (*h).tail.store(pos, Ordering::Release);
        true
    }

    /// Read one frame from the `from` to `to` ring, or `None` when empty.
    unsafe fn pop(&self, from: usize, to: usize) -> Option<(u8, Vec<u8>, Vec<u8>)> {
        let idx = self.ring_index(from, to);
        let h = self.ring_hdr(idx);
        let cap = self.ring_cap as u64;
        let head = (*h).head.load(Ordering::Relaxed);
        let tail = (*h).tail.load(Ordering::Acquire);
        let avail = tail.wrapping_sub(head);
        if avail < FRAME_HDR as u64 {
            return None;
        }
        let data = self.ring_data(idx);
        let read = |n: usize, pos: &mut u64| -> Vec<u8> {
            let mut out = vec![0u8; n];
            let mut off = 0usize;
            while off < n {
                let at = (*pos & (cap - 1)) as usize;
                let take = std::cmp::min(n - off, self.ring_cap - at);
                std::ptr::copy_nonoverlapping(data.add(at), out.as_mut_ptr().add(off), take);
                off += take;
                *pos = pos.wrapping_add(take as u64);
            }
            out
        };
        let mut pos = head;
        let hdr = read(FRAME_HDR, &mut pos);
        let kind = hdr[0];
        let alen = u32::from_le_bytes([hdr[1], hdr[2], hdr[3], hdr[4]]) as usize;
        let blen = u32::from_le_bytes([hdr[5], hdr[6], hdr[7], hdr[8]]) as usize;
        if avail < (FRAME_HDR + alen + blen) as u64 {
            return None; // the writer has not published the whole frame yet
        }
        let a = read(alen, &mut pos);
        let b = read(blen, &mut pos);
        (*h).head.store(pos, Ordering::Release);
        Some((kind, a, b))
    }

    /// Enqueue a published message for every worker holding a subscriber,
    /// returning how many remote subscribers it will reach.
    pub fn publish<F: Fn(&[u8], &[u8]) -> bool>(
        &self,
        from: usize,
        channel: &[u8],
        msg: &[u8],
        glob: F,
        mut wake: impl FnMut(usize),
    ) -> usize {
        let mut remote = 0usize;
        for (wid, count) in self.targets(from, channel, glob) {
            if unsafe { self.push(from, wid, KIND_PUBLISH, channel, msg) } {
                remote += count;
                wake(wid);
            }
        }
        remote
    }

    /// Broadcast an invalidation to every other worker. Unlike `publish` this
    /// ignores the routing table, because tracking tables live per worker.
    pub fn invalidate(&self, from: usize, key: Option<&[u8]>, mut wake: impl FnMut(usize)) {
        let (kind, k) = match key {
            Some(k) => (KIND_INVAL_KEY, k),
            None => (KIND_INVAL_ALL, &[][..]),
        };
        for wid in 0..self.nworkers {
            if wid == from {
                continue;
            }
            if unsafe { self.push(from, wid, kind, k, &[]) } {
                wake(wid);
            }
        }
    }

    /// Drain every frame addressed to `wid`, from all producers.
    pub fn drain(&self, wid: usize) -> Vec<crate::pubsub::BusMsg> {
        let mut out = Vec::new();
        for from in 0..self.nworkers {
            if from == wid {
                continue;
            }
            while let Some((kind, a, b)) = unsafe { self.pop(from, wid) } {
                out.push(match kind {
                    KIND_PUBLISH => crate::pubsub::BusMsg::Publish(a, b),
                    KIND_INVAL_KEY => crate::pubsub::BusMsg::Invalidate(Some(a)),
                    _ => crate::pubsub::BusMsg::Invalidate(None),
                });
            }
        }
        out
    }

    // ---- tracking ------------------------------------------------------

    pub fn tracker_add(&self) {
        unsafe { (*self.hdr()).trackers.fetch_add(1, Ordering::Relaxed) };
    }

    pub fn tracker_remove(&self) {
        unsafe {
            let t = &(*self.hdr()).trackers;
            let mut cur = t.load(Ordering::Relaxed);
            while cur > 0 {
                match t.compare_exchange_weak(cur, cur - 1, Ordering::Relaxed, Ordering::Relaxed) {
                    Ok(_) => break,
                    Err(v) => cur = v,
                }
            }
        }
    }

    pub fn tracking_active(&self) -> bool {
        unsafe { (*self.hdr()).trackers.load(Ordering::Relaxed) > 0 }
    }

    /// `(dropped, route_full, name_too_long)`: the three ways the bus can fail
    /// to carry a message, each of which is otherwise invisible.
    pub fn stats(&self) -> (u64, u64, u64) {
        unsafe {
            let h = self.hdr();
            (
                (*h).dropped.load(Ordering::Relaxed),
                (*h).route_full.load(Ordering::Relaxed),
                (*h).name_too_long.load(Ordering::Relaxed),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::BusMsg;

    /// Back a bus with a plain heap allocation so the layout can be exercised
    /// without a shared segment.
    struct Region(Vec<u8>);

    impl Region {
        fn new(nworkers: usize, routes: usize, ring: usize) -> (Region, ShmBus) {
            let mut v = vec![0u8; bytes_for(nworkers, routes, ring)];
            let bus = unsafe { ShmBus::init(v.as_mut_ptr(), nworkers, routes, ring) };
            (Region(v), bus)
        }
    }

    fn exact(pat: &[u8], ch: &[u8]) -> bool {
        pat == ch
    }

    #[test]
    fn delivers_across_workers() {
        let (_r, bus) = Region::new(3, 16, 4096);
        bus.subscribe(2, b"news", false);
        let mut woken = Vec::new();
        let n = bus.publish(0, b"news", b"hello", exact, |w| woken.push(w));
        assert_eq!(n, 1, "one remote subscriber");
        assert_eq!(woken, vec![2]);
        let got = bus.drain(2);
        assert_eq!(got.len(), 1);
        match &got[0] {
            BusMsg::Publish(c, m) => {
                assert_eq!(c, b"news");
                assert_eq!(m, b"hello");
            }
            _ => panic!("wrong kind"),
        }
        assert!(bus.drain(2).is_empty(), "drained twice");
    }

    #[test]
    fn publisher_is_not_sent_its_own_message() {
        let (_r, bus) = Region::new(3, 16, 4096);
        bus.subscribe(0, b"news", false);
        let n = bus.publish(0, b"news", b"x", exact, |_| {});
        assert_eq!(n, 0, "local subscribers are the caller's own business");
        assert!(bus.drain(0).is_empty());
    }

    #[test]
    fn unsubscribe_frees_the_entry_and_stops_delivery() {
        let (_r, bus) = Region::new(2, 4, 4096);
        bus.subscribe(1, b"c", false);
        bus.unsubscribe(1, b"c", false);
        assert_eq!(bus.publish(0, b"c", b"x", exact, |_| {}), 0);
        assert!(bus.drain(1).is_empty());
        // The slot is reusable: four more distinct channels must still fit.
        for k in [&b"a"[..], b"b", b"d", b"e"] {
            bus.subscribe(1, k, false);
        }
        assert_eq!(bus.stats().1, 0, "no route_full after the free");
    }

    #[test]
    fn a_full_ring_drops_rather_than_blocking() {
        // Small ring, one subscriber, publish past capacity.
        let (_r, bus) = Region::new(2, 4, 64);
        bus.subscribe(1, b"c", false);
        let big = vec![b'x'; 40];
        assert!(bus.publish(0, b"c", &big, exact, |_| {}) > 0);
        let before = bus.stats().0;
        bus.publish(0, b"c", &big, exact, |_| {});
        assert!(bus.stats().0 > before, "the drop must be counted");
    }

    #[test]
    fn wraparound_preserves_frames() {
        let (_r, bus) = Region::new(2, 4, 128);
        bus.subscribe(1, b"c", false);
        // Each round pushes and drains, walking the ring past its capacity.
        for i in 0..64u8 {
            let payload = [i; 20];
            assert_eq!(bus.publish(0, b"c", &payload, exact, |_| {}), 1);
            let got = bus.drain(1);
            assert_eq!(got.len(), 1, "round {i}");
            match &got[0] {
                BusMsg::Publish(_, m) => assert_eq!(m, &payload, "round {i} payload intact"),
                _ => panic!("wrong kind"),
            }
        }
    }

    #[test]
    fn patterns_route_through_the_supplied_matcher() {
        let (_r, bus) = Region::new(2, 8, 4096);
        bus.subscribe(1, b"news.*", true);
        let glob = |p: &[u8], c: &[u8]| p.ends_with(b"*") && c.starts_with(&p[..p.len() - 1]);
        assert_eq!(bus.publish(0, b"news.uk", b"m", glob, |_| {}), 1);
        assert_eq!(bus.publish(0, b"sport.uk", b"m", glob, |_| {}), 0);
    }

    #[test]
    fn an_over_long_name_is_refused_not_truncated() {
        let (_r, bus) = Region::new(2, 8, 4096);
        let long = vec![b'a'; MAX_CHAN + 1];
        bus.subscribe(1, &long, false);
        assert_eq!(bus.stats().2, 1, "counted");
        // Truncation would have made this collide with the over-long name.
        let prefix = vec![b'a'; MAX_CHAN];
        assert_eq!(bus.publish(0, &prefix, b"m", exact, |_| {}), 0);
    }

    #[test]
    fn invalidate_reaches_every_other_worker() {
        let (_r, bus) = Region::new(3, 4, 4096);
        let mut woken = Vec::new();
        bus.invalidate(1, Some(b"k"), |w| woken.push(w));
        woken.sort_unstable();
        assert_eq!(woken, vec![0, 2]);
        assert!(matches!(bus.drain(0)[0], BusMsg::Invalidate(Some(_))));
        assert!(bus.drain(1).is_empty(), "not sent to itself");
    }

    #[test]
    fn attach_rejects_a_segment_without_the_magic() {
        let mut v = vec![0u8; bytes_for(2, 4, 4096)];
        assert!(unsafe { ShmBus::attach(v.as_mut_ptr()) }.is_none());
    }

    #[test]
    fn attach_recovers_the_layout() {
        let (mut r, bus) = Region::new(3, 16, 4096);
        bus.subscribe(2, b"news", false);
        let again = unsafe { ShmBus::attach(r.0.as_mut_ptr()) }.expect("magic present");
        assert_eq!(again.nworkers(), 3);
        assert_eq!(again.publish(0, b"news", b"m", exact, |_| {}), 1);
    }
}
