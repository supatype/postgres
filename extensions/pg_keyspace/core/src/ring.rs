//! A single-producer/single-consumer byte ring in shared memory, used to hand
//! writes from the RESP slot worker (producer) to a dedicated persistence worker
//! (consumer) without ever blocking the event loop on SPI ("never block
//! on a Postgres lock" applied to the persistence path). The producer does a
//! ~nanosecond enqueue; all Postgres work happens in the other process.
//!
//! Records are length-prefixed: `[u32 key_len][u32 val_len][i64 expires][key][val]`.
//! Positions are monotonically increasing u64 masked to a power-of-two capacity;
//! wraparound is handled with two-part copies. `head`/`tail` are the only shared
//! mutable state and are ordered with acquire/release, which is sound for one
//! producer and one consumer.

use std::sync::atomic::{AtomicU64, Ordering};

const HDR_ALIGN: usize = 64;
const REC_HDR: usize = 16; // u32 + u32 + i64

#[repr(C)]
struct RingHeader {
    capacity: u64,
    head: AtomicU64,      // bytes consumed (consumer writes)
    tail: AtomicU64,      // bytes produced (producer writes)
    dropped: AtomicU64,   // records dropped because the ring was full
    pushed: AtomicU64,    // total records enqueued (producer); also assigns seq
    drained: AtomicU64,   // total records read by the consumer
    committed: AtomicU64, // total records durably committed (persist worker)
}

fn hdr_bytes() -> usize {
    (std::mem::size_of::<RingHeader>() + HDR_ALIGN - 1) & !(HDR_ALIGN - 1)
}

/// Total shared-memory bytes needed for a ring of `capacity` (rounded to a
/// power of two).
pub fn bytes_for(capacity: usize) -> usize {
    hdr_bytes() + capacity.next_power_of_two()
}

/// Initialise a ring in a region (call once, by the creator).
///
/// Every counter is reset, not just the ones Postgres would have zeroed for
/// us. `drained` and `committed` matter most: a stale `committed` left over
/// from a previous life of the segment sits above every sequence number the
/// new producer will issue, so durable writes would ack immediately with
/// nothing persisted. Postgres hands out zeroed shared memory, but the
/// standalone daemon attaches to a pre-existing POSIX segment, so this must
/// not depend on the caller.
///
/// # Safety
/// `base` must point at `bytes_for(capacity)` writable bytes.
pub unsafe fn init(base: *mut u8, capacity: usize) {
    let cap = capacity.next_power_of_two() as u64;
    let h = base as *mut RingHeader;
    (*h).capacity = cap;
    (*h).head.store(0, Ordering::Relaxed);
    (*h).tail.store(0, Ordering::Relaxed);
    (*h).dropped.store(0, Ordering::Relaxed);
    (*h).pushed.store(0, Ordering::Relaxed);
    (*h).drained.store(0, Ordering::Relaxed);
    (*h).committed.store(0, Ordering::Relaxed);
}

struct Ring {
    hdr: *mut RingHeader,
    data: *mut u8,
    mask: u64,
}

impl Ring {
    unsafe fn new(base: *mut u8) -> Ring {
        let hdr = base as *mut RingHeader;
        let cap = (*hdr).capacity;
        Ring {
            hdr,
            data: base.add(hdr_bytes()),
            mask: cap - 1,
        }
    }

    #[inline]
    unsafe fn write_wrapped(&self, pos: u64, bytes: &[u8]) {
        let start = (pos & self.mask) as usize;
        let cap = (self.mask + 1) as usize;
        let first = std::cmp::min(bytes.len(), cap - start);
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), self.data.add(start), first);
        if first < bytes.len() {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr().add(first),
                self.data,
                bytes.len() - first,
            );
        }
    }

    #[inline]
    unsafe fn read_wrapped(&self, pos: u64, out: &mut [u8]) {
        let start = (pos & self.mask) as usize;
        let cap = (self.mask + 1) as usize;
        let first = std::cmp::min(out.len(), cap - start);
        std::ptr::copy_nonoverlapping(self.data.add(start), out.as_mut_ptr(), first);
        if first < out.len() {
            std::ptr::copy_nonoverlapping(
                self.data,
                out.as_mut_ptr().add(first),
                out.len() - first,
            );
        }
    }
}

/// The producer end (the RESP slot worker).
pub struct Producer(Ring);
/// The consumer end (the persistence worker).
pub struct Consumer(Ring);

unsafe impl Send for Producer {}
unsafe impl Sync for Producer {}
unsafe impl Send for Consumer {}
unsafe impl Sync for Consumer {}

impl Producer {
    /// # Safety: `base` must be a ring region initialised by [`init`].
    pub unsafe fn attach(base: *mut u8) -> Producer {
        Producer(Ring::new(base))
    }

    /// Enqueue one write. Returns the record's sequence number on success, or
    /// None if the ring is full (the caller applies backpressure or drops). The
    /// seq lets a durable write wait until `committed() >= seq`.
    ///
    /// `kind` (the value's type tag) is packed into the free top byte of the
    /// `val_len` field — values are far below the 16MB that byte would encroach
    /// on — so the record layout and size are unchanged.
    pub fn push(&self, key: &[u8], val: &[u8], expires: i64, kind: u8) -> Option<u64> {
        let rec = REC_HDR + key.len() + val.len();
        unsafe {
            let h = &*self.0.hdr;
            let head = h.head.load(Ordering::Acquire);
            let tail = h.tail.load(Ordering::Relaxed);
            let cap = self.0.mask + 1;
            if cap - (tail - head) < rec as u64 {
                return None; // full — caller decides (backpressure vs drop)
            }
            let val_field = (val.len() as u32 & 0x00FF_FFFF) | ((kind as u32) << 24);
            self.0
                .write_wrapped(tail, &(key.len() as u32).to_le_bytes());
            self.0.write_wrapped(tail + 4, &val_field.to_le_bytes());
            self.0.write_wrapped(tail + 8, &expires.to_le_bytes());
            self.0.write_wrapped(tail + 16, key);
            self.0.write_wrapped(tail + 16 + key.len() as u64, val);
            h.tail.store(tail + rec as u64, Ordering::Release);
            // seq = the record's 1-based index; committed catches up to it.
            Some(h.pushed.fetch_add(1, Ordering::Relaxed) + 1)
        }
    }

    /// Record that a write was ultimately dropped (persistence wedged past the
    /// backpressure deadline). Rare and loud; surfaced via `Consumer::stats`.
    pub fn note_drop(&self) {
        unsafe { (*self.0.hdr).dropped.fetch_add(1, Ordering::Relaxed) };
    }

    /// The number of records durably committed so far (for durable sync-ack).
    pub fn committed(&self) -> u64 {
        unsafe { (*self.0.hdr).committed.load(Ordering::Acquire) }
    }
}

impl Consumer {
    /// # Safety: `base` must be a ring region initialised by [`init`].
    pub unsafe fn attach(base: *mut u8) -> Consumer {
        Consumer(Ring::new(base))
    }

    /// Read up to `max` records WITHOUT consuming them, calling
    /// `f(key, val, expires, kind)` for each. Returns `(count, bytes)`: the
    /// number of records read and the number of ring bytes they occupy, which
    /// is what [`Consumer::commit`] needs to release them.
    ///
    /// Reading is deliberately separated from consuming. The records stay in
    /// the ring, and `head` does not move, until the caller has actually made
    /// them durable and calls `commit`. A consumer whose downstream transaction
    /// fails, or which is killed mid-batch, therefore loses nothing: the
    /// restarted consumer re-reads exactly the same records. This is what makes
    /// a durable ack truthful, since `committed` can only ever advance over
    /// records that reached Postgres.
    pub fn peek<F: FnMut(&[u8], &[u8], i64, u8)>(&self, max: usize, mut f: F) -> (usize, u64) {
        unsafe {
            let h = &*self.0.hdr;
            let tail = h.tail.load(Ordering::Acquire);
            let start = h.head.load(Ordering::Relaxed);
            let mut pos = start;
            let mut count = 0usize;
            let mut hdr = [0u8; REC_HDR];
            while pos < tail && count < max {
                self.0.read_wrapped(pos, &mut hdr);
                let key_len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
                let val_field = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]);
                let val_len = (val_field & 0x00FF_FFFF) as usize;
                let kind = (val_field >> 24) as u8;
                let expires = i64::from_le_bytes([
                    hdr[8], hdr[9], hdr[10], hdr[11], hdr[12], hdr[13], hdr[14], hdr[15],
                ]);
                let mut key = vec![0u8; key_len];
                let mut val = vec![0u8; val_len];
                self.0.read_wrapped(pos + 16, &mut key);
                self.0.read_wrapped(pos + 16 + key_len as u64, &mut val);
                f(&key, &val, expires, kind);
                pos += (REC_HDR + key_len + val_len) as u64;
                count += 1;
            }
            (count, pos - start)
        }
    }

    /// Release the `count` records occupying `bytes`, exactly as returned by a
    /// preceding [`Consumer::peek`], and publish them as durably committed.
    ///
    /// Call this ONLY after the records are safe in Postgres. It advances
    /// `head` (freeing ring space for the producer) and `committed` (releasing
    /// any durable ack waiting on `committed() >= seq`) as one step, so the two
    /// can never disagree.
    pub fn commit(&self, count: usize, bytes: u64) {
        if count == 0 {
            return;
        }
        unsafe {
            let h = &*self.0.hdr;
            let head = h.head.load(Ordering::Relaxed);
            // head first: frees space for the producer.
            h.head.store(head + bytes, Ordering::Release);
            // Records are consumed strictly in order, so the running drained
            // count is also the seq of the last committed record.
            let d = h.drained.fetch_add(count as u64, Ordering::Relaxed) + count as u64;
            h.committed.store(d, Ordering::Release);
        }
    }

    /// Read and immediately commit up to `max` records, returning how many.
    ///
    /// Only safe where there is no failure point between reading a record and
    /// it being durable. The persistence worker must NOT use this: it has a
    /// Postgres transaction in between, so it needs `peek` then `commit`.
    #[cfg(test)]
    pub fn drain<F: FnMut(&[u8], &[u8], i64, u8)>(&self, max: usize, f: F) -> usize {
        let (count, bytes) = self.peek(max, f);
        self.commit(count, bytes);
        count
    }

    pub fn stats(&self) -> (u64, u64, u64) {
        unsafe {
            let h = &*self.0.hdr;
            (
                h.pushed.load(Ordering::Relaxed),
                h.dropped.load(Ordering::Relaxed),
                h.tail.load(Ordering::Relaxed) - h.head.load(Ordering::Relaxed),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spsc_roundtrip_and_wrap() {
        let cap = 4096usize;
        let mut buf = vec![0u8; bytes_for(cap)];
        let base = buf.as_mut_ptr();
        unsafe { init(base, cap) };
        let prod = unsafe { Producer::attach(base) };
        let cons = unsafe { Consumer::attach(base) };

        // push/drain many times to force wraparound past `cap`
        let mut produced = 0u64;
        let mut consumed = 0u64;
        for round in 0..2000u64 {
            let key = format!("key{round}");
            let val = format!("val-{}", round % 7);
            if prod.push(key.as_bytes(), val.as_bytes(), round as i64, b's').is_some() {
                produced += 1;
            }
            // drain occasionally so the ring never overflows
            if round % 3 == 0 {
                consumed += cons.drain(100, |k, v, e, _kind| {
                    assert!(k.starts_with(b"key"));
                    assert!(v.starts_with(b"val-"));
                    let _ = e;
                }) as u64;
            }
        }
        consumed += cons.drain(usize::MAX, |_, _, _, _| {}) as u64;
        assert_eq!(produced, consumed, "every pushed record must be drained");
    }

    /// Saturation is refusal, not overwrite: a full ring rejects the push and
    /// leaves the unread records intact, so the producer can apply backpressure.
    /// (`shard_push` in server.rs retries for 5s, then drops and counts.)
    #[test]
    fn full_ring_refuses_the_push_and_keeps_earlier_records() {
        let cap = 1024usize;
        let mut buf = vec![0u8; bytes_for(cap)];
        let base = buf.as_mut_ptr();
        unsafe { init(base, cap) };
        let prod = unsafe { Producer::attach(base) };
        let cons = unsafe { Consumer::attach(base) };

        let val = vec![b'x'; 100];
        let mut accepted = 0u64;
        while prod.push(b"k", &val, 0, b's').is_some() {
            accepted += 1;
            assert!(accepted < 1000, "ring never reported full");
        }
        assert!(accepted > 0, "ring rejected even the first record");
        // Refused, not silently overwritten: everything accepted is still readable.
        let drained = cons.drain(usize::MAX, |k, v, _, _| {
            assert_eq!(k, b"k");
            assert_eq!(v.len(), 100);
        }) as u64;
        assert_eq!(
            drained, accepted,
            "a full ring must not overwrite records it already accepted"
        );
        // Space freed, the producer is unblocked again.
        assert!(prod.push(b"k", &val, 0, b's').is_some());
    }

    /// A durable ack must never be released for a write whose persist batch did
    /// not commit.
    ///
    /// The failure this pins down: `peek` must not move `head` or `drained`, so
    /// a batch whose Postgres transaction fails (disk full, permission error,
    /// serialization failure) or whose worker is SIGKILLed mid-batch stays in
    /// the ring. The restarted worker re-reads exactly those records, and
    /// `committed` never runs ahead of what actually reached `supacache.kv`.
    ///
    /// Before the two-phase change this failed with `committed=4`, releasing
    /// the ack for k1 whose batch never committed.
    #[test]
    fn committed_must_not_cover_a_batch_that_never_committed() {
        let cap = 4096usize;
        let mut buf = vec![0u8; bytes_for(cap)];
        let base = buf.as_mut_ptr();
        unsafe { init(base, cap) };
        let prod = unsafe { Producer::attach(base) };
        let cons = unsafe { Consumer::attach(base) };

        // Three durable writes. Each RESP connection is holding its +OK until
        // committed() >= its seq.
        let s1 = prod.push(b"k1", b"v1", 0, b's').expect("push k1");
        let s2 = prod.push(b"k2", b"v2", 0, b's').expect("push k2");
        let s3 = prod.push(b"k3", b"v3", 0, b's').expect("push k3");
        assert_eq!((s1, s2, s3), (1, 2, 3));

        // The persist worker reads them into its batch, then bulk_upsert fails
        // or the worker is killed. `commit` is never reached for this batch.
        let (n, _bytes) = cons.peek(usize::MAX, |_, _, _, _| {});
        assert_eq!(n, 3);
        assert_eq!(prod.committed(), 0, "no batch has committed yet");

        // The worker restarts. Nothing was lost: it re-reads the same three
        // records, plus anything written since.
        prod.push(b"k4", b"v4", 0, b's').expect("push k4");
        let mut seen: Vec<Vec<u8>> = Vec::new();
        let (n2, bytes2) = cons.peek(usize::MAX, |k, _, _, _| seen.push(k.to_vec()));
        assert_eq!(
            seen,
            vec![b"k1".to_vec(), b"k2".to_vec(), b"k3".to_vec(), b"k4".to_vec()],
            "an uncommitted batch must stay in the ring for the restarted worker"
        );
        assert_eq!(n2, 4);

        // Only now, once they are durable, does the watermark move.
        cons.commit(n2, bytes2);
        assert_eq!(
            prod.committed(),
            4,
            "committed must cover exactly the records that reached Postgres"
        );
    }

    /// `committed` must never run ahead of the number of records actually
    /// committed, however the batches are sliced.
    #[test]
    fn committed_tracks_only_committed_records() {
        let cap = 8192usize;
        let mut buf = vec![0u8; bytes_for(cap)];
        let base = buf.as_mut_ptr();
        unsafe { init(base, cap) };
        let prod = unsafe { Producer::attach(base) };
        let cons = unsafe { Consumer::attach(base) };

        for i in 0..10u64 {
            prod.push(format!("k{i}").as_bytes(), b"v", 0, b's').expect("push");
        }
        // Commit in uneven slices, with a failed batch in the middle.
        let (n1, b1) = cons.peek(3, |_, _, _, _| {});
        cons.commit(n1, b1);
        assert_eq!(prod.committed(), 3);

        let (nf, _bf) = cons.peek(4, |_, _, _, _| {}); // batch fails: no commit
        assert_eq!(nf, 4);
        assert_eq!(prod.committed(), 3, "a failed batch must not advance committed");

        let (n2, b2) = cons.peek(usize::MAX, |_, _, _, _| {});
        assert_eq!(n2, 7, "the failed batch's records are still queued");
        cons.commit(n2, b2);
        assert_eq!(prod.committed(), 10);
    }

    /// `init` must reset every counter, not only the ones Postgres would have
    /// zeroed. A stale `committed` on a reused region sits above every seq the
    /// new producer will hand out, so durable writes ack instantly with nothing
    /// persisted. Before the fix this failed with `committed=5`.
    #[test]
    fn init_resets_every_counter_even_on_a_dirty_region() {
        let cap = 4096usize;
        let mut buf = vec![0u8; bytes_for(cap)];
        let base = buf.as_mut_ptr();

        // First life of the segment: push and commit some records.
        unsafe { init(base, cap) };
        let prod = unsafe { Producer::attach(base) };
        let cons = unsafe { Consumer::attach(base) };
        for i in 0..5u64 {
            prod.push(format!("k{i}").as_bytes(), b"v", 0, b's').expect("push");
        }
        cons.drain(usize::MAX, |_, _, _, _| {});
        assert_eq!(prod.committed(), 5);

        // Re-init the same region without zeroing it.
        unsafe { init(base, cap) };
        let prod2 = unsafe { Producer::attach(base) };
        assert_eq!(
            prod2.committed(),
            0,
            "init left committed={} on a reused region: the next durable write \
             (seq 1) acks immediately with nothing persisted",
            prod2.committed()
        );
    }
}
