//! A single-producer/single-consumer byte ring in shared memory, used to hand
//! writes from the RESP slot worker (producer) to a dedicated persistence worker
//! (consumer) without ever blocking the event loop on SPI (§3.1's "never block
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
    head: AtomicU64,    // bytes consumed (consumer writes)
    tail: AtomicU64,    // bytes produced (producer writes)
    dropped: AtomicU64, // records dropped because the ring was full
    pushed: AtomicU64,
}

fn hdr_bytes() -> usize {
    (std::mem::size_of::<RingHeader>() + HDR_ALIGN - 1) & !(HDR_ALIGN - 1)
}

/// Total shared-memory bytes needed for a ring of `capacity` (rounded to a
/// power of two).
pub fn bytes_for(capacity: usize) -> usize {
    hdr_bytes() + capacity.next_power_of_two()
}

/// Initialise a ring in a freshly-zeroed region (call once, by the creator).
///
/// # Safety
/// `base` must point at `bytes_for(capacity)` writable, zeroed bytes.
pub unsafe fn init(base: *mut u8, capacity: usize) {
    let cap = capacity.next_power_of_two() as u64;
    let h = base as *mut RingHeader;
    (*h).capacity = cap;
    (*h).head.store(0, Ordering::Relaxed);
    (*h).tail.store(0, Ordering::Relaxed);
    (*h).dropped.store(0, Ordering::Relaxed);
    (*h).pushed.store(0, Ordering::Relaxed);
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

    /// Enqueue one write. Returns false (and counts a drop) if the ring is full,
    /// which under sustained overload is the backpressure signal.
    pub fn push(&self, key: &[u8], val: &[u8], expires: i64) -> bool {
        let rec = REC_HDR + key.len() + val.len();
        unsafe {
            let h = &*self.0.hdr;
            let head = h.head.load(Ordering::Acquire);
            let tail = h.tail.load(Ordering::Relaxed);
            let cap = self.0.mask + 1;
            if cap - (tail - head) < rec as u64 {
                return false; // full — caller decides (backpressure vs drop)
            }
            self.0
                .write_wrapped(tail, &(key.len() as u32).to_le_bytes());
            self.0
                .write_wrapped(tail + 4, &(val.len() as u32).to_le_bytes());
            self.0.write_wrapped(tail + 8, &expires.to_le_bytes());
            self.0.write_wrapped(tail + 16, key);
            self.0.write_wrapped(tail + 16 + key.len() as u64, val);
            h.tail.store(tail + rec as u64, Ordering::Release);
            h.pushed.fetch_add(1, Ordering::Relaxed);
            true
        }
    }

    /// Record that a write was ultimately dropped (persistence wedged past the
    /// backpressure deadline). Rare and loud; surfaced via `Consumer::stats`.
    pub fn note_drop(&self) {
        unsafe { (*self.0.hdr).dropped.fetch_add(1, Ordering::Relaxed) };
    }
}

impl Consumer {
    /// # Safety: `base` must be a ring region initialised by [`init`].
    pub unsafe fn attach(base: *mut u8) -> Consumer {
        Consumer(Ring::new(base))
    }

    /// Drain up to `max` records, calling `f(key, val, expires)` for each.
    /// Returns the number of records consumed.
    pub fn drain<F: FnMut(&[u8], &[u8], i64)>(&self, max: usize, mut f: F) -> usize {
        unsafe {
            let h = &*self.0.hdr;
            let tail = h.tail.load(Ordering::Acquire);
            let mut head = h.head.load(Ordering::Relaxed);
            let mut count = 0usize;
            let mut hdr = [0u8; REC_HDR];
            while head < tail && count < max {
                self.0.read_wrapped(head, &mut hdr);
                let key_len = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]) as usize;
                let val_len = u32::from_le_bytes([hdr[4], hdr[5], hdr[6], hdr[7]]) as usize;
                let expires = i64::from_le_bytes([
                    hdr[8], hdr[9], hdr[10], hdr[11], hdr[12], hdr[13], hdr[14], hdr[15],
                ]);
                let mut key = vec![0u8; key_len];
                let mut val = vec![0u8; val_len];
                self.0.read_wrapped(head + 16, &mut key);
                self.0.read_wrapped(head + 16 + key_len as u64, &mut val);
                f(&key, &val, expires);
                head += (REC_HDR + key_len + val_len) as u64;
                count += 1;
            }
            if count > 0 {
                h.head.store(head, Ordering::Release);
            }
            count
        }
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
            if prod.push(key.as_bytes(), val.as_bytes(), round as i64) {
                produced += 1;
            }
            // drain occasionally so the ring never overflows
            if round % 3 == 0 {
                consumed += cons.drain(100, |k, v, e| {
                    assert!(k.starts_with(b"key"));
                    assert!(v.starts_with(b"val-"));
                    let _ = e;
                }) as u64;
            }
        }
        consumed += cons.drain(usize::MAX, |_, _, _| {}) as u64;
        assert_eq!(produced, consumed, "every pushed record must be drained");
    }
}
