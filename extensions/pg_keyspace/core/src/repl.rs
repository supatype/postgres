//! Real streaming replication for the `replicated` durability tier.
//!
//! The primary ships each fsynced WAL batch to a standby over a stream socket;
//! the standby appends it to its OWN WAL, fsyncs, and acks the batch's end
//! sequence. A `replicated` commit returns only once the standby has acked a seq
//! ≥ its own — real synchronous replication (Postgres `synchronous_commit =
//! remote_write`), not a `sleep`. The same `serve` loop backs both an in-process
//! standby (a thread over a loopback socket, used by the benchmark) and the
//! standalone `pgks-replica` binary (a separate process/host).
//!
//! Wire format, primary → standby:  `[end_seq u64][len u32][batch bytes]`
//!            standby → primary:     `[acked_seq u64]`  (cumulative)

use std::io::{self, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::unix::io::AsRawFd;
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// How a `Batcher` reaches its standby for the replicated tier.
#[derive(Clone)]
pub enum Replica {
    /// No standby: the replicated tier falls back to local durable (fsync only).
    Off,
    /// Spawn a co-located standby thread over a loopback socket (self-contained;
    /// still a real socket + a real second fsync). `link_delay` models the
    /// primary↔standby network latency the standby waits out before acking.
    Loopback { wal_path: String, link_delay: Duration },
    /// Connect to an external standby (the `pgks-replica` binary) at `host:port`.
    External(String),
}

/// Send one framed batch to the standby.
pub fn send_batch<W: Write>(w: &mut W, end_seq: u64, batch: &[u8]) -> io::Result<()> {
    w.write_all(&end_seq.to_le_bytes())?;
    w.write_all(&(batch.len() as u32).to_le_bytes())?;
    w.write_all(batch)?;
    w.flush()
}

/// Standby side: receive batches on `s`, append+fsync each to `wal_path`, and ack
/// its end seq (after an optional `link_delay`). Returns when the primary closes
/// the stream (EOF) or on I/O error.
pub fn serve<S: Read + Write>(mut s: S, wal_path: &str, link_delay: Duration) -> io::Result<()> {
    use std::fs::OpenOptions;
    let mut wal = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(wal_path)?;
    let mut hdr = [0u8; 12];
    loop {
        // read [end_seq u64][len u32]; a clean EOF here ends the session
        if let Err(e) = read_full(&mut s, &mut hdr) {
            return if e.kind() == io::ErrorKind::UnexpectedEof { Ok(()) } else { Err(e) };
        }
        let end_seq = u64::from_le_bytes(hdr[..8].try_into().unwrap());
        let len = u32::from_le_bytes(hdr[8..].try_into().unwrap()) as usize;
        let mut batch = vec![0u8; len];
        read_full(&mut s, &mut batch)?;
        wal.write_all(&batch)?;
        fsync(wal.as_raw_fd());
        if link_delay > Duration::ZERO {
            thread::sleep(link_delay);
        }
        s.write_all(&end_seq.to_le_bytes())?;
        s.flush()?;
    }
}

/// Run a standby that accepts one primary connection on `addr` and serves it.
/// Used by the standalone `pgks-replica` binary.
pub fn serve_listener(addr: &str, wal_path: &str, link_delay: Duration) -> io::Result<()> {
    let listener = TcpListener::bind(addr)?;
    let (conn, _) = listener.accept()?;
    conn.set_nodelay(true).ok();
    serve(conn, wal_path, link_delay)
}

/// Establish the primary→standby link. Returns the connected stream (the
/// primary's end) and, for the in-process case, the standby thread's handle.
pub fn connect(replica: &Replica) -> io::Result<(TcpStream, Option<JoinHandle<()>>)> {
    match replica {
        Replica::Off => Err(io::Error::new(io::ErrorKind::NotConnected, "no replica")),
        Replica::External(addr) => {
            let s = TcpStream::connect(addr)?;
            s.set_nodelay(true).ok();
            Ok((s, None))
        }
        Replica::Loopback { wal_path, link_delay } => {
            let listener = TcpListener::bind("127.0.0.1:0")?;
            let addr = listener.local_addr()?;
            let wal = wal_path.clone();
            let delay = *link_delay;
            let h = thread::spawn(move || {
                if let Ok((conn, _)) = listener.accept() {
                    conn.set_nodelay(true).ok();
                    let _ = serve(conn, &wal, delay);
                }
            });
            let s = TcpStream::connect(addr)?;
            s.set_nodelay(true).ok();
            Ok((s, Some(h)))
        }
    }
}

fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> io::Result<()> {
    let mut n = 0;
    while n < buf.len() {
        match r.read(&mut buf[n..]) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "eof")),
            Ok(k) => n += k,
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}

#[inline]
fn fsync(fd: std::os::unix::io::RawFd) {
    unsafe {
        libc::fsync(fd);
    }
}
