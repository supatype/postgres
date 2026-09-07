//! Microbenchmark for the native large-list structure: LINDEX-style random
//! index access, inline (walk every length-prefix from the head) vs indexed (an
//! explicit offset table), at the SAME element count. Data structure only — no
//! sockets, no store — so the O(n) -> O(1) difference is visible directly.
//!
//!   cargo run --release --example bench_list_index

use pgks::aggr::{list_get, List};
use std::time::Instant;

fn build(n: usize) -> List {
    let mut l = List::new();
    for i in 0..n {
        l.rpush(format!("element-{i:08}").as_bytes());
    }
    l
}

// A fixed, size-independent sample of indices spread across the list, so we
// measure per-access cost, not sweep length.
fn bench(blob: &[u8], idxs: &[usize], reps: usize) -> f64 {
    let mut sink = 0usize;
    for &i in idxs {
        sink += list_get(blob, i).map(|v| v.len()).unwrap_or(0);
    }
    let t = Instant::now();
    for _ in 0..reps {
        for &i in idxs {
            sink += list_get(blob, i).map(|v| v.len()).unwrap_or(0);
        }
    }
    let ns = t.elapsed().as_nanos() as f64;
    std::hint::black_box(sink);
    ns / (reps * idxs.len()) as f64
}

fn main() {
    println!("# LINDEX random-access latency: inline walk vs indexed table (ns/op)");
    println!("# {:>8}  {:>12}  {:>12}  {:>10}", "elems", "inline(ns)", "indexed(ns)", "speedup");
    const SAMPLE: usize = 64;
    for &n in &[128usize, 512, 1000, 5000, 10000, 50000] {
        let l = build(n);
        let step = (n / SAMPLE).max(1);
        let idxs: Vec<usize> = (0..n).step_by(step).take(SAMPLE).collect();
        let inline = l.force_encode(false);
        let indexed = l.force_encode(true);
        let ti = bench(&inline, &idxs, 2000);
        let tx = bench(&indexed, &idxs, 2000);
        println!("  {:>8}  {:>12.1}  {:>12.1}  {:>9.1}x", n, ti, tx, ti / tx);
    }
    println!("\n# inline walks from the head (O(n)); indexed reads offsets[i] (O(1)).");
}
