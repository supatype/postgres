//! Isolated commit-batcher benchmark (§3.4). The in-PG RESP path serializes
//! durable writes because one slot worker blocks on each commit's fsync; this
//! binary measures the batcher itself under `threads` concurrent committers,
//! which models N slot workers (or deferred acks within one) all staging into
//! the same commit window. It shows the amortisation the plan claims: one fsync
//! shared across many in-flight writes.
//!
//! Usage: durability_bench [--tier durable|relaxed|durable|replicated]
//!                         [--threads N] [--per-thread M] [--commit-window-us W]

use pgks::batcher::{Batcher, Tier};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn arg(name: &str, def: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    for i in 1..a.len() {
        if a[i] == name {
            return a.get(i + 1).cloned().unwrap_or_else(|| def.into());
        }
    }
    def.into()
}

fn bench(tier: Tier, threads: usize, per_thread: usize, window: Duration) {
    let wal = format!("/tmp/pgks_durbench_{:?}.wal", tier);
    let batcher = Arc::new(Batcher::new(&wal, window, Duration::from_micros(200)).unwrap());
    let record = vec![b'x'; 64];

    let start = Instant::now();
    let mut handles = Vec::new();
    for _ in 0..threads {
        let b = batcher.clone();
        let rec = record.clone();
        handles.push(std::thread::spawn(move || {
            let mut lat = Vec::with_capacity(per_thread);
            for _ in 0..per_thread {
                let t0 = Instant::now();
                b.commit(tier, &rec);
                lat.push(t0.elapsed().as_nanos() as u64);
            }
            lat
        }));
    }
    let mut all: Vec<u64> = Vec::new();
    for h in handles {
        all.extend(h.join().unwrap());
    }
    let elapsed = start.elapsed();
    all.sort_unstable();
    let total = all.len();
    let p = |q: f64| all[((total as f64 * q) as usize).min(total - 1)] as f64 / 1000.0;
    let rps = total as f64 / elapsed.as_secs_f64();
    println!(
        "{:<11} threads={:<3} ops={:<8} | {:>10.0} ops/s | p50={:>8.1}us p99={:>8.1}us max={:>8.1}us",
        format!("{:?}", tier),
        threads,
        total,
        rps,
        p(0.50),
        p(0.99),
        p(1.0) - 0.0001,
    );
    let _ = std::fs::remove_file(&wal);
}

fn main() {
    let threads: usize = arg("--threads", "0").parse().unwrap_or(0);
    let per_thread: usize = arg("--per-thread", "20000").parse().unwrap_or(20000);
    let window = Duration::from_micros(arg("--commit-window-us", "500").parse().unwrap_or(500));

    println!(
        "# commit-batcher benchmark, commit_window={:?}, per_thread={}",
        window, per_thread
    );
    println!("# 'threads' models concurrent slot workers all staging into one window.\n");

    let tiers = [Tier::Ephemeral, Tier::Relaxed, Tier::Durable, Tier::Replicated];
    let thread_counts: Vec<usize> = if threads > 0 {
        vec![threads]
    } else {
        vec![1, 8, 64]
    };
    for &t in &tiers {
        for &n in &thread_counts {
            // replicated is unbatched and slow; keep its op count modest.
            let per = if t == Tier::Replicated {
                (per_thread / 10).max(1000)
            } else {
                per_thread
            };
            bench(t, n, per, window);
        }
        println!();
    }
}
