//! pgks-replica — a standalone synchronous-replication standby for the
//! `replicated` durability tier (§3.4). It accepts one primary connection per
//! slot worker, appends+fsyncs each streamed WAL batch to its own WAL, and acks
//! the batch — the same `repl::serve` loop the in-process (loopback) standby
//! runs, so a real cross-process/cross-host standby is the identical code path.
//!
//! Usage:
//!   pgks-replica [--host 0.0.0.0] [--port 7400] [--workers 1]
//!                [--wal-dir /tmp] [--link-delay-us 0]
//! Point a primary at it with:
//!   pgkeyspaced --tier replicated --replica-addr <host> --replica-port 7400

use pgks::repl::serve_listener;
use std::time::Duration;

fn arg(name: &str, def: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    for i in 1..a.len() {
        if a[i] == name {
            return a.get(i + 1).cloned().unwrap_or_else(|| def.into());
        }
    }
    def.into()
}

fn main() {
    let host = arg("--host", "0.0.0.0");
    let port: u16 = arg("--port", "7400").parse().unwrap_or(7400);
    let workers: u16 = arg("--workers", "1").parse().unwrap_or(1);
    let wal_dir = arg("--wal-dir", "/tmp");
    let link_delay = Duration::from_micros(arg("--link-delay-us", "0").parse().unwrap_or(0));

    println!(
        "pgks-replica: standby on {host}:{}..{} workers={workers} wal-dir={wal_dir} link-delay={link_delay:?}",
        port,
        port + workers - 1
    );

    let mut handles = Vec::new();
    for w in 0..workers {
        let addr = format!("{host}:{}", port + w);
        let wal = format!("{wal_dir}/pgks_replica_w{w}.wal");
        handles.push(std::thread::spawn(move || {
            if let Err(e) = serve_listener(&addr, &wal, link_delay) {
                eprintln!("replica worker {w} ({addr}) exited: {e}");
            }
        }));
    }
    for h in handles {
        let _ = h.join();
    }
}
