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
fn literal_rules(n: usize) -> Vec<(Vec<u8>, Tier)> {
    (0..n)
        .map(|i| {
            let p = format!("svc{i:02}:region{}:", i % 7);
            (p.into_bytes(), if i % 3 == 0 { Tier::Durable } else { Tier::Relaxed })
        })
        .collect()
}

fn policy_with(n: usize) -> Policy {
    Policy::from_rules(Tier::Ephemeral, literal_rules(n))
}

/// `n` segment rules, the first of them `*:mau:`, beside `n` literal rules:
/// the Cloud platform shape, where every key is tenant-scoped and the segment
/// form is the only way to name a namespace in every tenant.
fn segment_policy_with(n: usize) -> Policy {
    let mut segments = vec![(b"mau:".to_vec(), Tier::Ephemeral)];
    segments.extend((1..n).map(|i| (format!("ns{i:02}:").into_bytes(), Tier::Relaxed)));
    Policy::with_segments(Tier::Durable, literal_rules(n), segments)
}

/// Sixteen keys from one shape, so a timing loop is not one hot key.
fn keys(shape: impl Fn(usize) -> String) -> Vec<Vec<u8>> {
    (0..16).map(|i| shape(i).into_bytes()).collect()
}

/// A tenant scope the length a UUID tenant id gives, which is what a segment
/// lookup has to walk past to find where the tenant ends.
fn tenant_key(i: usize, rest: &str) -> String {
    format!("6f1c2a9e-0b7d-4e3a-9c55-{i:012}:{rest}")
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

/// Which budget a case is held to.
#[derive(Clone, Copy)]
enum Kind {
    Miss,
    Hit,
}

/// The slowest miss and the slowest hit seen, which is what the budgets bound.
#[derive(Default)]
struct Worst {
    miss: f64,
    hit: f64,
}

impl Worst {
    /// Time one case, print its row and fold it into the worst of its kind.
    fn record(&mut self, iters: u64, label: &str, case: (&str, Kind), keys: &[Vec<u8>], pol: &Policy) {
        let ns = time(iters, keys, pol);
        let slot = match case.1 {
            Kind::Miss => &mut self.miss,
            Kind::Hit => &mut self.hit,
        };
        *slot = slot.max(ns);
        println!("  {:<10} {:<12} {:>10.2}", label, case.0, ns);
    }
}

/// Literal rules only: every deployment that existed before the segment form.
fn literal_cases(iters: u64, worst: &mut Worst) {
    // A cache key of the shape a REST response cache actually produces, and
    // one under a rule.
    let miss = keys(|i| format!("cache:GET:/rest/v1/todos?select=*&id=eq.{i}"));
    let hit_short = keys(|i| format!("svc00:region0:key{i}"));
    let hit_long = keys(|i| format!("svc63:region0:key{i}"));
    for n in [0usize, 1, 4, 8, 64] {
        let pol = policy_with(n);
        let label = n.to_string();
        worst.record(iters, &label, ("miss", Kind::Miss), &miss, &pol);
        // A hit needs a rule to hit, and `hit_long` only exists past 63.
        if n >= 1 {
            worst.record(iters, &label, ("hit-first", Kind::Hit), &hit_short, &pol);
        }
        if n >= 64 {
            worst.record(iters, &label, ("hit-last", Kind::Hit), &hit_long, &pol);
        }
    }
}

/// Segment rules beside literal ones, on tenant-scoped keys. A miss here still
/// has to find the end of the tenant scope, so it is gated by the same budget
/// as the literal miss rather than a looser one.
fn segment_cases(iters: u64, worst: &mut Worst) {
    let miss = keys(|i| tenant_key(i, "config"));
    let hit = keys(|i| tenant_key(i, &format!("mau:org:O:d:2026-11-{i:02}")));
    for n in [1usize, 8, 64] {
        let pol = segment_policy_with(n);
        let label = format!("{n}+{n}*");
        worst.record(iters, &label, ("seg-miss", Kind::Miss), &miss, &pol);
        worst.record(iters, &label, ("seg-hit", Kind::Hit), &hit, &pol);
    }
}

/// Print the verdict for one budget; true when it was exceeded. A budget of 0
/// is not checked.
fn over_budget(kind: &str, worst: f64, budget: f64) -> bool {
    if budget <= 0.0 {
        return false;
    }
    let verdict = if worst > budget { "FAIL" } else { "PASS" };
    let relation = if worst > budget { "exceeds" } else { "is within" };
    println!("{verdict}  worst {kind} {worst:.2} ns/op {relation} the {budget:.2} ns budget");
    worst > budget
}

fn main() {
    let iters: u64 = arg("--iters", "2000000").parse().unwrap_or(2_000_000);
    let max_ns: f64 = arg("--max-ns", "0").parse().unwrap_or(0.0);
    let max_hit_ns: f64 = arg("--max-hit-ns", "0").parse().unwrap_or(0.0);

    println!("# prefix match cost, {iters} iterations per sample");
    println!("# rules: n literal, n+n* literal plus segment");
    println!("# {:<10} {:<12} {:>10}", "rules", "case", "ns/op");
    let mut worst = Worst::default();
    literal_cases(iters, &mut worst);
    segment_cases(iters, &mut worst);

    if max_ns > 0.0 || max_hit_ns > 0.0 {
        println!();
    }
    let miss_failed = over_budget("miss", worst.miss, max_ns);
    let hit_failed = over_budget("hit", worst.hit, max_hit_ns);
    if miss_failed || hit_failed {
        std::process::exit(1);
    }
}
