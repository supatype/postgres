//! Cross-worker pub/sub bus.
//!
//! The slot workers are shared-nothing on the *key* path, but pub/sub
//! channels are not sharded — a PUBLISH on one worker must reach subscribers on
//! any worker. When workers are threads of one process (the scale-out daemon)
//! they share this `Bus`: a routing table (channel/pattern -> which workers hold
//! subscribers, and how many) plus a per-worker inbox and a wake fd (an
//! `eventfd` on Linux, a self-pipe on macOS) to wake that worker's poller so
//! delivery is prompt, not poll-latency bound.
//!
//! The in-PG extension runs a single RESP worker, so it uses no bus (local
//! delivery only); a future multi-*process* deployment would back the same
//! interface with shared memory instead of these in-process structures.

use std::collections::{HashMap, VecDeque};
use std::os::unix::io::RawFd;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

/// Which workers hold subscribers for a key, and how many each has.
#[derive(Default)]
struct Routing {
    channels: HashMap<Vec<u8>, HashMap<usize, usize>>,
    patterns: HashMap<Vec<u8>, HashMap<usize, usize>>,
}

/// One item in a worker's cross-worker inbox: either a pub/sub message to
/// deliver to local subscribers, or a client-side-caching invalidation to apply
/// to local trackers (`None` key = the whole keyspace changed, i.e. FLUSH).
pub enum BusMsg {
    Publish(Vec<u8>, Vec<u8>), // (channel, message)
    Invalidate(Option<Vec<u8>>), // scoped key, or None for flush-all
}

struct Slot {
    inbox: Mutex<VecDeque<BusMsg>>,
    // Wake fd registered in the worker's poller. On Linux this is a single
    // eventfd (read == write). On macOS it is a self-pipe: `wake_fd` is the read
    // end (what the worker polls and drains), `wake_w` the write end.
    wake_fd: RawFd,
    wake_w: RawFd,
}

/// Create a wake handle: (read/poll fd, write fd). One eventfd on Linux; a
/// non-blocking, close-on-exec self-pipe on macOS.
fn make_wake() -> (RawFd, RawFd) {
    #[cfg(target_os = "linux")]
    unsafe {
        let fd = libc::eventfd(0, libc::EFD_NONBLOCK | libc::EFD_CLOEXEC);
        (fd, fd)
    }
    #[cfg(not(target_os = "linux"))]
    unsafe {
        let mut fds = [0 as RawFd; 2];
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            return (-1, -1);
        }
        for &fd in &fds {
            let fl = libc::fcntl(fd, libc::F_GETFL, 0);
            if fl >= 0 {
                libc::fcntl(fd, libc::F_SETFL, fl | libc::O_NONBLOCK);
            }
            let fdfl = libc::fcntl(fd, libc::F_GETFD, 0);
            if fdfl >= 0 {
                libc::fcntl(fd, libc::F_SETFD, fdfl | libc::FD_CLOEXEC);
            }
        }
        (fds[0], fds[1]) // (read, write)
    }
}

pub struct Bus {
    routing: Mutex<Routing>,
    slots: Vec<Slot>,
    // Number of connections across all workers with CLIENT TRACKING on. The
    // write path consults this to skip broadcasting invalidations entirely when
    // nobody is tracking, so tracking costs the hot path nothing when unused.
    trackers: AtomicUsize,
}

impl Bus {
    /// Create a bus for `nworkers`, each with its own wake handle.
    pub fn new(nworkers: usize) -> Bus {
        let mut slots = Vec::with_capacity(nworkers);
        for _ in 0..nworkers {
            let (wake_fd, wake_w) = make_wake();
            slots.push(Slot {
                inbox: Mutex::new(VecDeque::new()),
                wake_fd,
                wake_w,
            });
        }
        Bus {
            routing: Mutex::new(Routing::default()),
            slots,
            trackers: AtomicUsize::new(0),
        }
    }

    /// Record that a connection turned CLIENT TRACKING on / off. `tracking_active`
    /// then tells the write path whether any worker has a tracker at all.
    pub fn tracker_add(&self) {
        self.trackers.fetch_add(1, Ordering::Relaxed);
    }
    pub fn tracker_remove(&self) {
        // saturating: never wrap below zero if counts ever get out of step.
        let mut cur = self.trackers.load(Ordering::Relaxed);
        while cur > 0 {
            match self.trackers.compare_exchange_weak(
                cur,
                cur - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }
    /// Whether any connection anywhere has tracking on.
    pub fn tracking_active(&self) -> bool {
        self.trackers.load(Ordering::Relaxed) > 0
    }

    /// The fd a worker registers with its poller to learn of deliveries (the
    /// eventfd on Linux, the pipe's read end on macOS).
    pub fn wake_fd(&self, wid: usize) -> RawFd {
        self.slots[wid].wake_fd
    }

    fn table<'a>(r: &'a mut Routing, pattern: bool) -> &'a mut HashMap<Vec<u8>, HashMap<usize, usize>> {
        if pattern {
            &mut r.patterns
        } else {
            &mut r.channels
        }
    }

    /// Record that worker `wid` gained a subscriber on `key`.
    pub fn subscribe(&self, wid: usize, key: &[u8], pattern: bool) {
        let mut r = self.routing.lock().unwrap();
        *Self::table(&mut r, pattern)
            .entry(key.to_vec())
            .or_default()
            .entry(wid)
            .or_insert(0) += 1;
    }

    /// Record that worker `wid` lost a subscriber on `key`.
    pub fn unsubscribe(&self, wid: usize, key: &[u8], pattern: bool) {
        let mut r = self.routing.lock().unwrap();
        let map = Self::table(&mut r, pattern);
        if let Some(per) = map.get_mut(key) {
            if let Some(c) = per.get_mut(&wid) {
                *c -= 1;
                if *c == 0 {
                    per.remove(&wid);
                }
            }
            if per.is_empty() {
                map.remove(key);
            }
        }
    }

    /// Route a published message to every *other* worker that has a subscriber
    /// for `channel` (directly or via a matching pattern): enqueue it and wake
    /// that worker. Returns the number of remote subscribers it will reach (the
    /// caller adds its own local delivery count).
    pub fn publish<F: Fn(&[u8], &[u8]) -> bool>(
        &self,
        from: usize,
        channel: &[u8],
        msg: &[u8],
        glob: F,
    ) -> usize {
        // Collect target workers + their subscriber counts under the lock.
        let mut targets: HashMap<usize, usize> = HashMap::new();
        {
            let r = self.routing.lock().unwrap();
            if let Some(per) = r.channels.get(channel) {
                for (&wid, &c) in per {
                    *targets.entry(wid).or_insert(0) += c;
                }
            }
            for (pat, per) in &r.patterns {
                if glob(pat, channel) {
                    for (&wid, &c) in per {
                        *targets.entry(wid).or_insert(0) += c;
                    }
                }
            }
        }
        let mut remote = 0usize;
        for (wid, count) in targets {
            if wid == from {
                continue; // the caller delivers to its own subscribers directly
            }
            remote += count;
            {
                let mut ib = self.slots[wid].inbox.lock().unwrap();
                ib.push_back(BusMsg::Publish(channel.to_vec(), msg.to_vec()));
            }
            self.wake(wid);
        }
        remote
    }

    /// Broadcast a client-side-caching invalidation to every *other* worker so
    /// each can notify its own trackers of the changed key (`None` = the whole
    /// keyspace, for FLUSH). Unlike `publish` this fans out to all workers rather
    /// than routing by subscription, because the tracking tables live per-worker.
    /// Callers gate this on `tracking_active` so it never runs when nobody tracks.
    pub fn invalidate(&self, from: usize, key: Option<&[u8]>) {
        for wid in 0..self.slots.len() {
            if wid == from {
                continue; // the caller already invalidated its own trackers
            }
            {
                let mut ib = self.slots[wid].inbox.lock().unwrap();
                ib.push_back(BusMsg::Invalidate(key.map(|k| k.to_vec())));
            }
            self.wake(wid);
        }
    }

    /// Wake a worker's poller. Writes an 8-byte value to the wake fd: on Linux
    /// that bumps the eventfd counter; on macOS it queues 8 bytes in the pipe.
    /// Either way the worker's poll fires and `drain` empties it.
    fn wake(&self, wid: usize) {
        let one: u64 = 1;
        unsafe {
            libc::write(
                self.slots[wid].wake_w,
                &one as *const u64 as *const libc::c_void,
                8,
            );
        }
    }

    /// Drain this worker's inbox (called after its wake fd fires). Also fully
    /// drains the wake fd itself so it does not re-fire spuriously — read until
    /// EAGAIN, which empties both an eventfd counter and a pipe with queued bytes
    /// (mio is edge-triggered, so a partial drain would miss later wakes).
    pub fn drain(&self, wid: usize) -> Vec<BusMsg> {
        let mut buf = [0u8; 64];
        loop {
            let n = unsafe {
                libc::read(
                    self.slots[wid].wake_fd,
                    buf.as_mut_ptr() as *mut libc::c_void,
                    buf.len(),
                )
            };
            if n <= 0 {
                break; // EAGAIN (drained) or error
            }
        }
        let mut ib = self.slots[wid].inbox.lock().unwrap();
        ib.drain(..).collect()
    }
}

impl Drop for Bus {
    fn drop(&mut self) {
        for s in &self.slots {
            unsafe {
                libc::close(s.wake_fd);
                // On Linux read==write (one eventfd); only close the write end
                // separately when it is a distinct fd (the macOS self-pipe).
                if s.wake_w != s.wake_fd {
                    libc::close(s.wake_w);
                }
            }
        }
    }
}
