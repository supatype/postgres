//! Per-tenant share of a persistence ring (#43).
//!
//! Keys are force-scoped to `{tenant}:` for non-exempt roles, and that scoping
//! is enforced server-side rather than by convention — but the prefix is an
//! addressing and ACL device and carries no accounting. One shared SPSC ring
//! per persist shard, FIFO, no per-tenant share, means a tenant writing hard
//! enough to keep the ring full starves every other tenant on the same shard:
//! their writes meet a full ring, park, and either wait out `PUSH_DEADLINE` or
//! are refused. Measured, the gap was stark — the flooding tenant sustained
//! tens of thousands of writes a second while a single-connection victim on the
//! same shard managed tens.
//!
//! This is the accounting the prefix does not carry: how many ring bytes each
//! tenant currently has in flight, and a ceiling on it.
//!
//! # How the accounting stays exact without shared state
//!
//! The ring is single-producer: one worker event loop owns the producer end, so
//! every push for a given ring happens on one thread and the bookkeeping can
//! live in that worker's own memory — no atomics, no second shared region, no
//! cost to the consumer.
//!
//! A record leaves the ring exactly when the consumer's `head` passes the
//! record's end position, and records are consumed strictly in order. So the
//! producer keeps a FIFO of `(end position, tenant, bytes)` and, before each
//! decision, drops everything the current `head` has passed. That is exact
//! rather than approximate, and it needs nothing from the consumer beyond the
//! `head` it already publishes.
//!
//! A restarted worker starts with an empty FIFO while the ring may still hold
//! records it did not push. It then under-counts until those drain, which is
//! at most one ring's worth of leniency, once, at startup.
//!
//! # The policy
//!
//! Two properties matter more than the exact numbers:
//!
//! - **It is inert until there is contention.** Below half-full nobody is
//!   policed, because there is nothing to be fair about: a single-tenant
//!   deployment, or any ring that is keeping up, never sees this code change a
//!   decision. Unauthenticated and exempt (service-role) connections have no
//!   tenant and are never policed at all, so a deployment that does not use
//!   tenant scoping is unaffected.
//! - **The share is dynamic, not a fixed percentage.** A tenant may hold up to
//!   `capacity / active tenants`, recomputed per decision. One tenant alone
//!   gets the whole ring; two contending tenants get half each. A fixed
//!   percentage would either strand capacity when few tenants are active or let
//!   a handful of tenants wedge the ring anyway.
//!
//! # Active means demanding, not occupying
//!
//! The first version of this counted a tenant as active only while it had bytes
//! in flight, and measured as no improvement whatsoever: a victim tenant got one
//! write through in ten seconds with the share on, the same as with it off. The
//! reason is circular. A tenant the flood has shut out never gets a record into
//! the ring, so it has no bytes in flight, so it is not counted, so the flood is
//! the only active tenant, so its share is the whole ring, so it is never held
//! back. The tenant that most needs the policy was the one the policy could not
//! see.
//!
//! So a tenant counts as active from the moment it *attempts* a write, whether
//! or not the attempt got anywhere.
//!
//! "Recently" is measured in ring bytes drained rather than in seconds: a tenant
//! is active if it attempted a write within the last capacity's worth of
//! consumer progress. The ring's `head` is already a monotonic byte counter, so
//! this costs no clock read on the write path, and it scales itself with
//! throughput -- under heavy drain the window is short in wall-clock terms, and
//! on an idle ring it does not expire at all, which is the right behaviour in
//! both cases.
//!
//! [`MIN_TENANT_SHARE`] floors the share so that a tenant is never cut below a
//! useful chunk of work by the mere existence of many others. Past
//! `capacity / MIN_TENANT_SHARE` active tenants the floors sum to more than the
//! ring, and the share stops being a hard division — the ring is simply too
//! small for that many tenants and wants raising (`pg_keyspace.ring_mb`).

use std::collections::{HashMap, VecDeque};

/// Smallest in-flight allowance a tenant can be cut to, regardless of how many
/// others are active.
///
/// A share below one record is a deadlock rather than a quota, and a share of a
/// few records is a stall: the tenant would spend its time parked waiting for
/// its own previous write to drain. 256 KiB is roughly thirty `INLINE_MAX`
/// records, or thousands of ordinary small ones.
pub const MIN_TENANT_SHARE: u64 = 256 * 1024;

/// Ring fullness at which the share starts being enforced, as a fraction.
///
/// Below this the ring is keeping up and there is nothing to allocate.
const POLICE_ABOVE_NUM: u64 = 1;
const POLICE_ABOVE_DEN: u64 = 2;

/// A tenant's identity for accounting purposes: a hash of the scope name.
///
/// Hashing rather than interning keeps the hot path a field read on the
/// connection — the hash is computed once, at AUTH. A collision would merge two
/// tenants' accounting (they would share one allowance); it is not an isolation
/// boundary, the `{tenant}:` key prefix is, so the consequence is a fairness
/// imprecision of vanishing probability rather than a leak.
pub type TenantId = u64;

/// FNV-1a. Small, no dependency, and stable across processes and restarts,
/// which a `DefaultHasher` is explicitly not.
pub fn tenant_id(tenant: &str) -> TenantId {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in tenant.as_bytes() {
        h ^= *b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    // 0 means "no tenant" (unauthenticated or exempt), so never hand it out.
    if h == 0 {
        1
    } else {
        h
    }
}

/// Per-tenant in-flight accounting for one ring.
#[derive(Default)]
pub struct RingShare {
    inflight: HashMap<TenantId, u64>,
    /// `(end position, tenant, bytes)` in push order.
    queue: VecDeque<(u64, TenantId, u32)>,
    /// Times a write was held back because its tenant was over its share.
    /// Surfaced through the stats view: an operator wants to know that fairness
    /// is engaging, and which tenant it is engaging against.
    held: HashMap<TenantId, u64>,
    /// The consumer position at each tenant's most recent write *attempt*. This
    /// is what makes a shut-out tenant visible to the share calculation; see the
    /// module docs.
    seen: HashMap<TenantId, u64>,
}

impl RingShare {
    pub fn new() -> RingShare {
        RingShare::default()
    }

    /// Drop everything the consumer has passed. Call before any decision.
    pub fn reclaim(&mut self, head: u64) {
        while let Some(&(end, who, bytes)) = self.queue.front() {
            if end > head {
                break;
            }
            self.queue.pop_front();
            if let Some(v) = self.inflight.get_mut(&who) {
                *v = v.saturating_sub(bytes as u64);
                if *v == 0 {
                    self.inflight.remove(&who);
                }
            }
        }
    }

    /// Note that `who` is trying to write, whether or not the write gets
    /// anywhere. Call this before [`RingShare::would_exceed`], or a tenant the
    /// ring is shutting out stays invisible to the share calculation.
    pub fn note_attempt(&mut self, who: TenantId, head: u64) {
        if who != 0 {
            self.seen.insert(who, head);
        }
    }

    /// Whether `bytes` more from `who` would put them over their share.
    ///
    /// `used` is the ring's current in-flight byte count, `capacity` its size,
    /// and `head` the consumer position (for the activity window). Returns false
    /// for the no-tenant case and whenever the ring is not under pressure.
    pub fn would_exceed(
        &self,
        who: TenantId,
        bytes: u64,
        used: u64,
        capacity: u64,
        head: u64,
    ) -> bool {
        if who == 0 || capacity == 0 {
            return false;
        }
        if used * POLICE_ABOVE_DEN < capacity * POLICE_ABOVE_NUM {
            return false;
        }
        let share = (capacity / self.active_demand(who, capacity, head)).max(MIN_TENANT_SHARE);
        let cur = self.inflight.get(&who).copied().unwrap_or(0);
        cur + bytes > share
    }

    /// How many tenants the ring is currently being asked to serve: those that
    /// attempted a write within the activity window, plus any holding bytes in
    /// flight, plus `who` whether or not either applies. Never zero.
    fn active_demand(&self, who: TenantId, capacity: u64, head: u64) -> u64 {
        let mut n = 0u64;
        for (t, at) in &self.seen {
            if *t == who {
                continue;
            }
            if self.inflight.contains_key(t) || head.saturating_sub(*at) <= capacity {
                n += 1;
            }
        }
        // Bytes in flight with no recorded attempt: a worker that restarted
        // under load has an empty `seen` and a ring that is not empty.
        for t in self.inflight.keys() {
            if *t != who && !self.seen.contains_key(t) {
                n += 1;
            }
        }
        n + 1
    }

    /// Note that a write was held back for being over share.
    pub fn note_held(&mut self, who: TenantId) {
        if who != 0 {
            *self.held.entry(who).or_insert(0) += 1;
        }
    }

    /// Record a pushed record. `end` is the ring position just past it.
    pub fn record(&mut self, who: TenantId, bytes: u64, end: u64) {
        if who == 0 {
            return;
        }
        *self.inflight.entry(who).or_insert(0) += bytes;
        self.queue.push_back((end, who, bytes.min(u32::MAX as u64) as u32));
    }

    /// `(tenant, in-flight bytes, times held back)` for every tenant this ring
    /// has seen.
    ///
    /// Keyed off attempts rather than current occupancy, so a tenant that has
    /// written at all has a row -- including one currently holding nothing,
    /// which is exactly the tenant an operator is looking for when they ask.
    pub fn snapshot(&self) -> Vec<(TenantId, u64, u64)> {
        let mut ids: Vec<TenantId> = self.seen.keys().copied().collect();
        for k in self.inflight.keys().chain(self.held.keys()) {
            if !self.seen.contains_key(k) {
                ids.push(*k);
            }
        }
        ids.sort_unstable();
        ids.dedup();
        ids.into_iter()
            .map(|k| {
                (
                    k,
                    self.inflight.get(&k).copied().unwrap_or(0),
                    self.held.get(&k).copied().unwrap_or(0),
                )
            })
            .collect()
    }

    /// Number of tenants with bytes in flight right now.
    pub fn active(&self) -> usize {
        self.inflight.len()
    }
}

/// Per-tenant request rate limiting (#43).
///
/// The third axis the issue names: "One event loop per worker, no scheduling,
/// no rate limiting." The ring share bounds how much *persistence* one tenant
/// can hold and scoped eviction bounds how much *cache* it can take, but
/// neither bounds how much of the worker's time it can ask for -- a tenant
/// issuing reads has no ring records and evicts nothing, and can still saturate
/// the loop.
///
/// A token bucket per tenant, refilled continuously at `limit` tokens a second
/// and capped at one second's worth, so a tenant may burst up to its per-second
/// rate and then proceeds at it.
///
/// Off unless configured (`limit == 0`), and never applied to a connection with
/// no tenant scope, which keeps the clock read off the hot path entirely for
/// deployments that do not use it.
#[derive(Default)]
pub struct TenantRates {
    limit: u32,
    buckets: HashMap<TenantId, Bucket>,
    throttled: HashMap<TenantId, u64>,
}

struct Bucket {
    tokens: f64,
    last: std::time::Instant,
}

impl TenantRates {
    pub fn new() -> TenantRates {
        TenantRates::default()
    }

    /// Commands per second per tenant; 0 disables.
    pub fn set_limit(&mut self, ops: u32) {
        self.limit = ops;
    }

    pub fn limit(&self) -> u32 {
        self.limit
    }

    /// Take a token for `who`, or report that it is over its rate.
    ///
    /// The caller's response to `false` is to park the connection and retry,
    /// which is the same thing a full persistence ring already does: the tenant
    /// is slowed rather than told no, so no client learns a new error and no
    /// command is lost.
    pub fn allow(&mut self, who: TenantId) -> bool {
        if self.limit == 0 || who == 0 {
            return true;
        }
        let now = std::time::Instant::now();
        let cap = self.limit as f64;
        let b = self.buckets.entry(who).or_insert(Bucket { tokens: cap, last: now });
        let elapsed = now.duration_since(b.last).as_secs_f64();
        b.last = now;
        b.tokens = (b.tokens + elapsed * cap).min(cap);
        if b.tokens >= 1.0 {
            b.tokens -= 1.0;
            true
        } else {
            *self.throttled.entry(who).or_insert(0) += 1;
            false
        }
    }

    /// How many commands this tenant has been held back, for the stats surface.
    pub fn throttled(&self, who: TenantId) -> u64 {
        self.throttled.get(&who).copied().unwrap_or(0)
    }

    /// Every tenant that has been throttled at least once.
    pub fn throttled_tenants(&self) -> Vec<(TenantId, u64)> {
        let mut v: Vec<(TenantId, u64)> = self.throttled.iter().map(|(k, n)| (*k, *n)).collect();
        v.sort_unstable();
        v
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CAP: u64 = 1 << 20;

    /// `would_exceed` with the attempt recorded first, which is how the server
    /// calls it -- and forgetting the attempt is the bug these tests exist for.
    fn ask(s: &mut RingShare, who: TenantId, bytes: u64, used: u64, head: u64) -> bool {
        s.note_attempt(who, head);
        s.would_exceed(who, bytes, used, CAP, head)
    }

    #[test]
    fn no_tenant_is_never_policed() {
        let mut s = RingShare::new();
        assert!(!ask(&mut s, 0, CAP, CAP, 0));
    }

    #[test]
    fn a_quiet_ring_is_not_policed() {
        let mut s = RingShare::new();
        let a = tenant_id("a");
        // A quarter full: under the threshold, so even a huge ask passes.
        s.record(a, CAP / 4, CAP / 4);
        assert!(!ask(&mut s, a, CAP / 2, CAP / 4, 0));
    }

    #[test]
    fn one_tenant_alone_may_use_the_whole_ring() {
        let mut s = RingShare::new();
        let a = tenant_id("a");
        s.record(a, CAP * 3 / 4, CAP * 3 / 4);
        // Sole active tenant: its share is the whole ring, so what is left of
        // the ring is the only limit -- and that limit is the ring's, not this.
        assert!(!ask(&mut s, a, CAP / 8, CAP * 3 / 4, 0));
    }

    /// The case the first version of this got wrong, and the reason the measured
    /// improvement was zero: a tenant shut out of a full ring holds no bytes, so
    /// counting only occupancy makes the flood the sole active tenant and gives
    /// it the whole ring.
    #[test]
    fn a_tenant_shut_out_of_the_ring_still_counts_against_the_flood() {
        let mut s = RingShare::new();
        let (flood, victim) = (tenant_id("flood"), tenant_id("victim"));
        // The flood owns the entire ring; the victim has never landed a record.
        s.record(flood, CAP, CAP);
        // The victim tries and gets nowhere -- but it has now been seen.
        s.note_attempt(victim, 0);
        assert!(
            s.would_exceed(flood, 4096, CAP, CAP, 0),
            "the flood must be held back once a second tenant is asking"
        );
    }

    #[test]
    fn a_flood_cannot_take_more_than_its_half_from_a_second_tenant() {
        let mut s = RingShare::new();
        let (a, b) = (tenant_id("a"), tenant_id("b"));
        s.record(b, 4096, 4096);
        s.record(a, CAP / 2, CAP / 2 + 4096);
        let used = CAP / 2 + 4096;
        assert!(ask(&mut s, a, 4096, used, 0), "the flood is over its half");
        assert!(!ask(&mut s, b, 4096, used, 0), "the victim still has room");
    }

    #[test]
    fn an_idle_tenant_stops_counting_once_the_ring_has_turned_over() {
        let mut s = RingShare::new();
        let (a, b) = (tenant_id("a"), tenant_id("b"));
        s.note_attempt(b, 0); // b asked once, long ago
        s.record(a, CAP * 3 / 4, CAP * 3 / 4);
        // Still inside the window: two tenants, so a is over its half.
        assert!(s.would_exceed(a, 4096, CAP * 3 / 4, CAP, CAP));
        // A full capacity's worth of drain later, b's ask has aged out and a is
        // alone again.
        assert!(!s.would_exceed(a, 4096, CAP * 3 / 4, CAP, CAP + 1));
    }

    #[test]
    fn the_share_is_never_cut_below_the_floor() {
        let mut s = RingShare::new();
        // Enough tenants that an equal division would be a few bytes each.
        for i in 0..1000u32 {
            let t = tenant_id(&format!("t{i}"));
            s.note_attempt(t, 0);
            s.record(t, 64, 64 * (i as u64 + 1));
        }
        let who = tenant_id("t0");
        assert!(
            !s.would_exceed(who, MIN_TENANT_SHARE - 64, CAP, CAP, 0),
            "a tenant keeps at least MIN_TENANT_SHARE however many others there are"
        );
        assert!(s.would_exceed(who, MIN_TENANT_SHARE + 1, CAP, CAP, 0));
    }

    #[test]
    fn draining_returns_the_allowance() {
        let mut s = RingShare::new();
        let (a, b) = (tenant_id("a"), tenant_id("b"));
        s.record(b, 4096, 4096);
        s.record(a, CAP / 2, CAP / 2 + 4096);
        assert!(ask(&mut s, a, 4096, CAP / 2 + 4096, 0));
        // The consumer passes both records.
        s.reclaim(CAP / 2 + 4096);
        assert_eq!(s.active(), 0);
        assert!(!ask(&mut s, a, 4096, 0, CAP / 2 + 4096));
    }

    #[test]
    fn reclaim_stops_at_the_first_record_still_in_the_ring() {
        let mut s = RingShare::new();
        let a = tenant_id("a");
        s.record(a, 100, 100);
        s.record(a, 100, 200);
        s.record(a, 100, 300);
        s.reclaim(200);
        assert_eq!(s.snapshot(), vec![(a, 100, 0)]);
    }

    #[test]
    fn a_tenant_that_has_written_has_a_row_even_holding_nothing() {
        let mut s = RingShare::new();
        let a = tenant_id("a");
        s.note_attempt(a, 0);
        s.record(a, 100, 100);
        s.reclaim(100);
        assert_eq!(s.snapshot(), vec![(a, 0, 0)], "still reported, at zero");
    }

    #[test]
    fn held_counts_survive_the_tenant_going_idle() {
        let mut s = RingShare::new();
        let a = tenant_id("a");
        s.note_held(a);
        s.note_held(a);
        assert_eq!(s.snapshot(), vec![(a, 0, 2)]);
    }

    #[test]
    fn a_disabled_limiter_allows_everything() {
        let mut r = TenantRates::new();
        let a = tenant_id("a");
        for _ in 0..10_000 {
            assert!(r.allow(a));
        }
        assert_eq!(r.throttled(a), 0);
    }

    #[test]
    fn an_unscoped_connection_is_never_limited() {
        let mut r = TenantRates::new();
        r.set_limit(1);
        for _ in 0..1000 {
            assert!(r.allow(0), "no tenant scope: not this policy's business");
        }
    }

    #[test]
    fn a_tenant_may_burst_its_rate_then_is_held() {
        let mut r = TenantRates::new();
        r.set_limit(100);
        let a = tenant_id("a");
        // A full bucket is one second's worth, so the first 100 go straight
        // through -- a burst is allowed, a sustained overrate is not.
        let allowed = (0..100).filter(|_| r.allow(a)).count();
        assert_eq!(allowed, 100);
        assert!(!r.allow(a), "the 101st in the same instant is over the rate");
        assert!(r.throttled(a) >= 1);
    }

    #[test]
    fn one_tenants_rate_does_not_touch_anothers() {
        let mut r = TenantRates::new();
        r.set_limit(10);
        let (a, b) = (tenant_id("a"), tenant_id("b"));
        for _ in 0..10 {
            r.allow(a);
        }
        assert!(!r.allow(a), "a has spent its bucket");
        assert!(r.allow(b), "b's bucket is its own");
    }

    #[test]
    fn a_bucket_refills_over_time() {
        let mut r = TenantRates::new();
        r.set_limit(1000);
        let a = tenant_id("a");
        while r.allow(a) {}
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(r.allow(a), "20ms at 1000/s is ~20 tokens back");
    }

    #[test]
    fn tenant_ids_are_stable_and_never_zero() {
        assert_eq!(tenant_id("acme"), tenant_id("acme"));
        assert_ne!(tenant_id("acme"), tenant_id("acmf"));
        assert_ne!(tenant_id(""), 0);
    }
}
