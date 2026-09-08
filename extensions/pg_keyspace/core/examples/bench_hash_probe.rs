//! Microbenchmark isolating the native large-collection win: HGET-style point
//! reads on a hash, inline (flat scan) vs indexed (in-value bucket table), at
//! the SAME field count. This measures only the data structure — no sockets, no
//! store — so the O(n) -> O(1) difference is visible without RESP/syscall noise.
//!
//!   cargo run --release --example bench_hash_probe

use pgks::aggr::{hash_probe, Hash};
use std::time::Instant;

fn build(n: usize) -> Hash {
    let mut h = Hash::new();
    for i in 0..n {
        h.set(format!("field-{i:08}").as_bytes(), format!("value-{i}").as_bytes());
    }
    h
}

// Probe a fixed sample of existing fields (spread across the whole hash) and
// return ns/op. The sample size is constant so the total work per call scales
// only with the per-probe cost — exactly the O(n) vs O(1) axis we want to see.
fn bench(blob: &[u8], sample: &[Vec<u8>], reps: usize) -> f64 {
    let mut sink = 0usize;
    for f in sample {
        sink += hash_probe(blob, f).map(|v| v.len()).unwrap_or(0);
    }
    let t = Instant::now();
    for _ in 0..reps {
        for f in sample {
            sink += hash_probe(blob, f).map(|v| v.len()).unwrap_or(0);
        }
    }
    let elapsed = t.elapsed().as_nanos() as f64;
    std::hint::black_box(sink);
    elapsed / (reps * sample.len()) as f64
}

fn main() {
    println!("# HGET point-read latency: inline scan vs indexed table (ns/op)");
    println!("# {:>8}  {:>12}  {:>12}  {:>10}", "fields", "inline(ns)", "indexed(ns)", "speedup");
    // 64 existing fields spread evenly across the hash — a fixed, size-independent
    // sample so we measure per-probe cost, not sweep length.
    const SAMPLE: usize = 64;
    for &n in &[128usize, 512, 1000, 5000, 10000, 50000] {
        let h = build(n);
        let step = (n / SAMPLE).max(1);
        let sample: Vec<Vec<u8>> = (0..n)
            .step_by(step)
            .take(SAMPLE)
            .map(|i| format!("field-{i:08}").into_bytes())
            .collect();
        let inline = h.force_encode(false);
        let indexed = h.force_encode(true);
        let reps = 2000;
        let ti = bench(&inline, &sample, reps);
        let tx = bench(&indexed, &sample, reps);
        println!(
            "  {:>8}  {:>12.1}  {:>12.1}  {:>9.1}x",
            n, ti, tx, ti / tx
        );
    }
    println!("\n# inline grows ~linearly with field count (O(n) scan);");
    println!("# indexed stays ~flat (O(1) average bucket lookup).");
}
