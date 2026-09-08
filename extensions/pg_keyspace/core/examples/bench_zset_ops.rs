//! Microbenchmark for the native large-sorted-set structure. Two ops, inline vs
//! indexed, at the same member count:
//!   ZSCORE  — inline scans every member (O(n)); indexed is a bucket lookup O(1).
//!   ZRANK   — inline re-sorts the whole set (O(n log n)); indexed binary-searches
//!             the pre-sorted offset array (O(log n)).
//! Data structure only (no sockets/store).
//!
//!   cargo run --release --example bench_zset_ops

use pgks::aggr::{zset_rank, zset_score, ZSet};
use std::time::Instant;

fn build(n: usize) -> ZSet {
    let mut z = ZSet::new();
    for i in 0..n {
        z.add(format!("m{i:08}").as_bytes(), (i % 100) as f64);
    }
    z
}

fn bench<F: Fn(&[u8], &[u8])>(blob: &[u8], sample: &[Vec<u8>], reps: usize, f: F) -> f64 {
    for m in sample {
        f(blob, m);
    }
    let t = Instant::now();
    for _ in 0..reps {
        for m in sample {
            f(blob, m);
        }
    }
    t.elapsed().as_nanos() as f64 / (reps * sample.len()) as f64
}

fn main() {
    println!("# ZSCORE and ZRANK latency: inline vs indexed (ns/op)");
    println!(
        "# {:>7} | {:>11} {:>11} {:>8} | {:>11} {:>11} {:>8}",
        "members", "score-inl", "score-idx", "spd", "rank-inl", "rank-idx", "spd"
    );
    const SAMPLE: usize = 32;
    for &n in &[128usize, 512, 1000, 5000, 10000] {
        let z = build(n);
        let step = (n / SAMPLE).max(1);
        let sample: Vec<Vec<u8>> =
            (0..n).step_by(step).take(SAMPLE).map(|i| format!("m{i:08}").into_bytes()).collect();
        let inline = z.force_encode(false);
        let indexed = z.force_encode(true);
        let reps = (500_000 / n).max(2);
        let si = bench(&inline, &sample, reps, |b, m| {
            std::hint::black_box(zset_score(b, m));
        });
        let sx = bench(&indexed, &sample, reps, |b, m| {
            std::hint::black_box(zset_score(b, m));
        });
        let ri = bench(&inline, &sample, reps, |b, m| {
            std::hint::black_box(zset_rank(b, m));
        });
        let rx = bench(&indexed, &sample, reps, |b, m| {
            std::hint::black_box(zset_rank(b, m));
        });
        println!(
            "  {:>7} | {:>11.1} {:>11.1} {:>7.0}x | {:>11.1} {:>11.1} {:>7.0}x",
            n, si, sx, si / sx, ri, rx, ri / rx
        );
    }
    println!("\n# indexed ZSCORE = O(1) bucket lookup; ZRANK = O(log n) binary search");
    println!("# over a pre-sorted index (inline re-sorts the whole set each call).");
}
