//! Commit batching and the four durability tiers (§3.4). Each slot worker
//! accumulates writes over `commit_window` and commits them in one transaction:
//! "one fsync amortised across hundreds of operations". This module models that
//! against a real on-disk WAL file so the amortisation is measured, not assumed.
//!
//! | Tier        | Mechanism                                   | Client waits for |
//! |-------------|---------------------------------------------|------------------|
//! | ephemeral   | shmem only, never touches disk              | nothing          |
//! | relaxed     | appended, fsync on the batch timer          | nothing (async)  |
//! | durable     | appended, one fsync per batch               | its batch fsync  |
//! | replicated  | appended, fsync + replica ack, unbatched    | fsync + RTT      |

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::os::unix::io::AsRawFd;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Tier {
    Ephemeral,
    Relaxed,
    Durable,
    Replicated,
}

struct Shared {
    pending: Vec<u8>,      // records staged for the next fsync
    staged_seq: u64,       // highest seq accepted into `pending`
    flushed_seq: u64,      // highest seq durably fsynced
    stop: bool,
}

pub struct Batcher {
    shared: Arc<(Mutex<Shared>, Condvar, Condvar)>, // (state, work-ready, flushed)
    file: Arc<Mutex<File>>,
    commit_window: Duration,
    replica_rtt: Duration,
    flusher: Option<JoinHandle<()>>,
}

impl Batcher {
    pub fn new(wal_path: &str, commit_window: Duration, replica_rtt: Duration) -> std::io::Result<Batcher> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(wal_path)?;
        let shared = Arc::new((
            Mutex::new(Shared {
                pending: Vec::with_capacity(1 << 16),
                staged_seq: 0,
                flushed_seq: 0,
                stop: false,
            }),
            Condvar::new(),
            Condvar::new(),
        ));
        let file = Arc::new(Mutex::new(file));

        let s2 = shared.clone();
        let f2 = file.clone();
        let win = commit_window;
        let flusher = thread::spawn(move || flush_loop(s2, f2, win));

        Ok(Batcher {
            shared,
            file,
            commit_window,
            replica_rtt,
            flusher: Some(flusher),
        })
    }

    /// Commit one write record under `tier`; returns when the tier's guarantee
    /// is satisfied. `record` is the bytes that would be WAL-logged.
    pub fn commit(&self, tier: Tier, record: &[u8]) {
        match tier {
            Tier::Ephemeral => { /* shmem authoritative; nothing hits disk */ }
            Tier::Relaxed => {
                // stage and return; the timer fsyncs later (synchronous_commit=off)
                let (m, work, _flushed) = &*self.shared;
                let mut g = m.lock().unwrap();
                g.staged_seq += 1;
                append_record(&mut g.pending, record);
                work.notify_one();
            }
            Tier::Durable => {
                let (m, work, flushed) = &*self.shared;
                let mut g = m.lock().unwrap();
                g.staged_seq += 1;
                let my_seq = g.staged_seq;
                append_record(&mut g.pending, record);
                work.notify_one();
                // wait for the batch that includes my_seq to be fsynced
                while g.flushed_seq < my_seq {
                    g = flushed.wait(g).unwrap();
                }
            }
            Tier::Replicated => {
                // unbatched: own fsync then a simulated replica round-trip
                let mut f = self.file.lock().unwrap();
                let mut rec = Vec::with_capacity(record.len() + 8);
                append_record(&mut rec, record);
                f.write_all(&rec).unwrap();
                fsync(&f);
                drop(f);
                if self.replica_rtt > Duration::ZERO {
                    thread::sleep(self.replica_rtt);
                }
            }
        }
    }

    pub fn commit_window(&self) -> Duration {
        self.commit_window
    }
}

impl Drop for Batcher {
    fn drop(&mut self) {
        {
            let (m, work, _f) = &*self.shared;
            let mut g = m.lock().unwrap();
            g.stop = true;
            work.notify_all();
        }
        if let Some(h) = self.flusher.take() {
            let _ = h.join();
        }
    }
}

fn flush_loop(
    shared: Arc<(Mutex<Shared>, Condvar, Condvar)>,
    file: Arc<Mutex<File>>,
    window: Duration,
) {
    let (m, work, flushed) = &*shared;
    loop {
        let mut g = m.lock().unwrap();
        // wait until there is work or we are told to stop, bounded by the window
        while g.pending.is_empty() && !g.stop {
            let (ng, _to) = work.wait_timeout(g, window).unwrap();
            g = ng;
            if !g.pending.is_empty() {
                break;
            }
            if g.stop {
                break;
            }
        }
        if g.stop && g.pending.is_empty() {
            return;
        }
        // Let a batch accrue for the commit window, then flush once.
        let seq_at_wake = g.staged_seq;
        drop(g);
        thread::sleep(window);

        let mut g = m.lock().unwrap();
        let batch = std::mem::take(&mut g.pending);
        let batch_seq = g.staged_seq.max(seq_at_wake);
        drop(g);

        if !batch.is_empty() {
            let mut f = file.lock().unwrap();
            f.write_all(&batch).unwrap();
            fsync(&f);
            drop(f);
        }

        let mut g = m.lock().unwrap();
        g.flushed_seq = batch_seq;
        flushed.notify_all();
        if g.stop && g.pending.is_empty() {
            return;
        }
    }
}

#[inline]
fn append_record(buf: &mut Vec<u8>, record: &[u8]) {
    buf.extend_from_slice(&(record.len() as u32).to_le_bytes());
    buf.extend_from_slice(record);
}

#[inline]
fn fsync(f: &File) {
    unsafe {
        libc::fsync(f.as_raw_fd());
    }
}
