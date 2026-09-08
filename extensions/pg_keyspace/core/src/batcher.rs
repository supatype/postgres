//! Commit batching and the four durability tiers (§3.4). Each slot worker
//! accumulates writes over `commit_window` and commits them in one transaction:
//! "one fsync amortised across hundreds of operations". This module models that
//! against a real on-disk WAL file so the amortisation is measured, not assumed.
//!
//! | Tier        | Mechanism                                   | Client waits for   |
//! |-------------|---------------------------------------------|--------------------|
//! | ephemeral   | shmem only, never touches disk              | nothing            |
//! | relaxed     | appended, fsync on the batch timer          | nothing (async)    |
//! | durable     | appended, one fsync per batch               | its batch fsync    |
//! | replicated  | batch fsync + streamed to a standby         | fsync + standby ack|
//!
//! The `replicated` tier is real synchronous replication (§3.4, `repl.rs`): the
//! batch flusher streams each fsynced batch to a standby over a socket, the
//! standby appends+fsyncs it to its own WAL and acks, and a replicated commit
//! returns only once the standby has acked its sequence.

use crate::repl::{self, Replica};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::{Shutdown, TcpStream};
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
    flushed_seq: u64,      // highest seq durably fsynced on the primary
    replicated_seq: u64,   // highest seq acked by the standby
    stop: bool,
}

pub struct Batcher {
    shared: Arc<(Mutex<Shared>, Condvar, Condvar)>, // (state, work-ready, flushed/acked)
    file: Arc<Mutex<File>>,
    commit_window: Duration,
    has_replica: bool,
    // A clone of the primary→standby stream, kept only to shut the socket down on
    // drop so the flusher/ack-reader/standby threads unblock and exit.
    repl_stream: Option<TcpStream>,
    flusher: Option<JoinHandle<()>>,
    ack_reader: Option<JoinHandle<()>>,
    standby: Option<JoinHandle<()>>,
}

impl Batcher {
    /// Non-replicated batcher (ephemeral/relaxed/durable). A `replicated` commit
    /// falls back to local durable (fsync only) since no standby is attached.
    pub fn new(wal_path: &str, commit_window: Duration) -> std::io::Result<Batcher> {
        Batcher::with_replica(wal_path, commit_window, Replica::Off)
    }

    /// Batcher with a standby for the replicated tier. `replica` is `Loopback`
    /// (a co-located standby thread) or `External(addr)` (a `pgks-replica`).
    pub fn with_replica(
        wal_path: &str,
        commit_window: Duration,
        replica: Replica,
    ) -> std::io::Result<Batcher> {
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
                replicated_seq: 0,
                stop: false,
            }),
            Condvar::new(),
            Condvar::new(),
        ));
        let file = Arc::new(Mutex::new(file));

        // Establish the standby link (if any) before starting the flusher.
        let (send_stream, ack_stream, keep_stream, standby) = match replica {
            Replica::Off => (None, None, None, None),
            r => match repl::connect(&r) {
                Ok((stream, standby)) => {
                    let ack = stream.try_clone()?;
                    let keep = stream.try_clone()?;
                    (Some(stream), Some(ack), Some(keep), standby)
                }
                Err(e) => {
                    // A configured replica that will not connect is fatal — a
                    // replicated commit must never silently degrade.
                    return Err(e);
                }
            },
        };
        let has_replica = send_stream.is_some();

        let s2 = shared.clone();
        let f2 = file.clone();
        let win = commit_window;
        let flusher = thread::spawn(move || flush_loop(s2, f2, win, send_stream));

        // Ack reader: cumulative acks from the standby advance `replicated_seq`.
        let ack_reader = ack_stream.map(|mut s| {
            let s3 = shared.clone();
            thread::spawn(move || {
                let mut buf = [0u8; 8];
                use std::io::Read;
                loop {
                    if s.read_exact(&mut buf).is_err() {
                        return; // standby gone / shutting down
                    }
                    let seq = u64::from_le_bytes(buf);
                    let (m, _work, flushed) = &*s3;
                    let mut g = m.lock().unwrap();
                    if seq > g.replicated_seq {
                        g.replicated_seq = seq;
                        flushed.notify_all();
                    }
                }
            })
        });

        Ok(Batcher {
            shared,
            file,
            commit_window,
            has_replica,
            repl_stream: keep_stream,
            flusher: Some(flusher),
            ack_reader,
            standby,
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
            Tier::Durable | Tier::Replicated => {
                let (m, work, flushed) = &*self.shared;
                let mut g = m.lock().unwrap();
                g.staged_seq += 1;
                let my_seq = g.staged_seq;
                append_record(&mut g.pending, record);
                work.notify_one();
                // durable: wait for the local fsync of my batch.
                while g.flushed_seq < my_seq {
                    g = flushed.wait(g).unwrap();
                }
                // replicated: additionally wait for the standby to ack it.
                if tier == Tier::Replicated && self.has_replica {
                    while g.replicated_seq < my_seq {
                        g = flushed.wait(g).unwrap();
                    }
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
        // Tear the socket down so the flusher (sending), ack reader (blocked on
        // read) and standby (blocked on read) all unblock and exit.
        if let Some(s) = self.repl_stream.take() {
            let _ = s.shutdown(Shutdown::Both);
        }
        if let Some(h) = self.flusher.take() {
            let _ = h.join();
        }
        if let Some(h) = self.ack_reader.take() {
            let _ = h.join();
        }
        if let Some(h) = self.standby.take() {
            let _ = h.join();
        }
    }
}

fn flush_loop(
    shared: Arc<(Mutex<Shared>, Condvar, Condvar)>,
    file: Arc<Mutex<File>>,
    window: Duration,
    mut repl_stream: Option<TcpStream>,
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
            // Stream the fsynced batch to the standby (real synchronous rep).
            if let Some(s) = repl_stream.as_mut() {
                if repl::send_batch(s, batch_seq, &batch).is_err() {
                    repl_stream = None; // link lost; stop streaming (Drop will exit)
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replicated_commit_waits_for_standby_wal() {
        let dir = std::env::temp_dir();
        let pid = std::process::id();
        let pwal = format!("{}/pgks_repltest_{pid}.wal", dir.display());
        let rwal = format!("{}/pgks_repltest_{pid}.replica.wal", dir.display());
        let b = Batcher::with_replica(
            &pwal,
            Duration::from_micros(200),
            Replica::Loopback { wal_path: rwal.clone(), link_delay: Duration::ZERO },
        )
        .unwrap();

        // A replicated commit returns only after the standby fsynced the batch,
        // so the payload must already be in the standby's WAL when commit returns.
        let payload = b"REPLICATED_PAYLOAD_42";
        b.commit(Tier::Replicated, payload);
        let replica_bytes = std::fs::read(&rwal).unwrap();
        assert!(
            replica_bytes.windows(payload.len()).any(|w| w == payload),
            "standby WAL missing the replicated record after commit returned"
        );

        // A second record streams and acks too (cumulative).
        b.commit(Tier::Replicated, b"SECOND_ONE");
        drop(b); // clean shutdown: sockets torn down, threads joined
        let replica_bytes = std::fs::read(&rwal).unwrap();
        assert!(replica_bytes.windows(10).any(|w| w == b"SECOND_ONE"));

        let _ = std::fs::remove_file(&pwal);
        let _ = std::fs::remove_file(&rwal);
    }

    #[test]
    fn durable_without_replica_still_commits() {
        // Off replica: replicated falls back to local durable, no deadlock.
        let dir = std::env::temp_dir();
        let pwal = format!("{}/pgks_noreptest_{}.wal", dir.display(), std::process::id());
        let b = Batcher::new(&pwal, Duration::from_micros(200)).unwrap();
        b.commit(Tier::Durable, b"x");
        b.commit(Tier::Replicated, b"y"); // no standby -> behaves as durable
        drop(b);
        let _ = std::fs::remove_file(&pwal);
    }
}
