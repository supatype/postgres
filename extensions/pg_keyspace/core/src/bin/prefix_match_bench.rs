//! What per-key durability costs the write path (#164).
//!
//! `Policy::tier_for` runs on every write, and the op it sits in front of is
//! measured in nanoseconds -- `supacache.bench_set()` reports the raw
//! shared-memory store write, and the README quotes single-digit nanoseconds
//! for the read. A matcher that walked every rule, or allocated, would be
//! visible against that immediately, so the cost is measured rather than
//! assumed.
//!
//! The interesting number is the **miss**: a key matching no rule at all,
//! which on a mostly-ephemeral instance is nearly every key. That path exists
//! to be rejected by the first-byte bitmap and the minimum-length check before
//! any comparison, and it is what the CI gate bounds. Hits are reported too,
//! but a hit is a key the operator asked to treat specially, and it pays for
//! the staging that follows anyway.
//!
//! This deliberately does not go through `supacache.bench_set()`: that writes
//! straight to the store from the calling backend and never reaches the RESP
//! dispatch where the policy is consulted, so it cannot see this at all.
//!
//! Usage: prefix_match_bench [--iters N] [--max-ns X]

use pgks::batcher::Tier;
use pgks::durability::Policy;
use std::hint::black_box;
use std::time::Instant;

fn arg(name: &str, def: &str) -> String {
    let a: Vec<String> = std::env::args().collect();
    for i in 1..a.len() {
        if a[i] == name {
            return a.get(i + 1).cloned().unwrap_or_else(|| def.into());
        }
    }
    def.into()
}

/// `n` rules that look like real namespaces rather than single letters, so the
/// first-byte bitmap is not trivially sparse.
fn policy_with(n: usize) -> Policy {
    let rules: Vec<(Vec<u8>, Tier)> = (0..n)
        .map(|i| {
            let p = format!("svc{i:02}:region{}:", i % 7);
            (p.into_bytes(), if i % 3 == 0 { Tier::Durable } else { Tier::Relaxed })
        })
        .collect();
    Policy::from_rules(Tier::Ephemeral, rules)
}

fn time(iters: u64, keys: &[Vec<u8>], pol: &Policy) -> f64 {
    // Warm the branch predictor and the cache lines the rules live on, or the
    // first sample measures the warm-up rather than the steady state.
    for k in keys {
        black_box(pol.tier_for(black_box(k)));
    }
    let t0 = Instant::now();
    let mut acc = 0u64;
    for i in 0..iters {
        let k = &keys[(i as usize) % keys.len()];
        acc += pol.tier_for(black_box(k)) as u64;
    }
    let ns = t0.elapsed().as_nanos() as f64 / iters as f64;
    black_box(acc);
    ns
}

fn main() {
    let iters: u64 = arg("--iters", "2000000").parse().unwrap_or(2_000_000);
    let max_ns: f64 = arg("--max-ns", "0").parse().unwrap_or(0.0);
    let max_hit_ns: f64 = arg("--max-hit-ns", "0").parse().unwrap_or(0.0);

    // A cache key of the shape a REST response cache actually produces, and
    // one under a rule. Several of each so the loop is not one hot key.
    let miss: Vec<Vec<u8>> = (0..16)
        .map(|i| format!("cache:GET:/rest/v1/todos?select=*&id=eq.{i}").into_bytes())
        .collect();
    let hit_short: Vec<Vec<u8>> =
        (0..16).map(|i| format!("svc00:region0:key{i}").into_bytes()).collect();
    let hit_long: Vec<Vec<u8>> =
        (0..16).map(|i| format!("svc63:region0:key{i}").into_bytes()).collect();

    println!("# prefix match cost, {iters} iterations per sample");
    println!("# {:<10} {:<10} {:>10}", "rules", "case", "ns/op");
    let mut worst_miss = 0.0f64;
    let mut worst_hit = 0.0f64;
    for n in [0usize, 1, 4, 8, 64] {
        let pol = policy_with(n);
        let ns = time(iters, &miss, &pol);
        worst_miss = worst_miss.max(ns);
        println!("  {:<10} {:<10} {:>10.2}", n, "miss", ns);
        // A hit needs a rule to hit, and `hit_long` only exists past 63.
        if n >= 1 {
            let h = time(iters, &hit_short, &pol);
            worst_hit = worst_hit.max(h);
            println!("  {:<10} {:<10} {:>10.2}", n, "hit-first", h);
        }
        if n >= 64 {
            let h = time(iters, &hit_long, &pol);
            worst_hit = worst_hit.max(h);
            println!("  {:<10} {:<10} {:>10.2}", n, "hit-last", h);
        }
    }

    let mut failed = false;
    if max_ns > 0.0 || max_hit_ns > 0.0 {
        println!();
    }
    if max_ns > 0.0 {
        if worst_miss > max_ns {
            println!("FAIL  worst miss {worst_miss:.2} ns/op exceeds the {max_ns:.2} ns budget");
            failed = true;
        } else {
            println!("PASS  worst miss {worst_miss:.2} ns/op is within the {max_ns:.2} ns budget");
        }
    }
    if max_hit_ns > 0.0 {
        if worst_hit > max_hit_ns {
            println!("FAIL  worst hit {worst_hit:.2} ns/op exceeds the {max_hit_ns:.2} ns budget");
            failed = true;
        } else {
            println!("PASS  worst hit {worst_hit:.2} ns/op is within the {max_hit_ns:.2} ns budget");
        }
    }
    if failed {
        std::process::exit(1);
    }
}
