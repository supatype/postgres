//! Per-key durability: which tier an individual key is written at.
//!
//! `pg_keyspace.durability` sets one tier for the whole instance, which forces
//! a deployment needing *any* durable data to run its entire keyspace durable
//! — every cache write through the WAL to protect the small part that matters.
//! This module is the exception list: a prefix, the tier keys under it are
//! written at, and the instance tier as the default for everything else.
//!
//! **Why the key and not the command.** The decision has to be one the server
//! can make from what a *stock* client already sends. The whole premise of the
//! RESP surface is that an off-the-shelf client works unmodified, and an
//! off-the-shelf client cannot opt a write into a tier: a `SET k v PERSIST`
//! extension would restrict per-key durability to code the operator wrote
//! themselves, which excludes the cases that ask for it (an ACME plugin storing
//! certificates beside a response cache, say). So it is decided by
//! configuration, against the only thing configuration can name: the key.
//!
//! **Why the prefix.** Redis's other namespacing axis is not available here —
//! `SELECT` is accepted and ignored, so there are no logical databases to hang
//! this off. The prefix is what clients already use to namespace a shared
//! keyspace (`cache:`, `sess:`, `kong_acme:`), precisely *because* Redis
//! databases are not worth using. It also covers tenants at no extra cost:
//! forced tenant scoping stores keys as `{tenant}:{key}`, so `tenantA:` is a
//! prefix like any other and a per-tenant durability policy needs no second
//! surface.
//!
//! Matching is against the key **as stored**, which is the tenant-scoped form,
//! because that is the key that reaches the ring and the one `supacache.kv`
//! holds. Recovery applies the same policy, so a prefix dropped from the map
//! stops being served after a restart rather than coming back as if it were
//! still durable.

use crate::batcher::Tier;

/// Equal-length rules up to this many are scanned rather than binary-searched.
/// See `tier_for`: the crossover is where log(n) cache-missing probes start to
/// beat n cheap comparisons, which `prefix_match_bench` puts around a dozen.
const LINEAR_SCAN_MAX: usize = 12;

/// A prefix → tier map with a default, resolving any key to exactly one tier.
///
/// Longest-prefix-wins is what makes `a:` and `a:b:` both configurable without
/// the result depending on the order the operator happened to write them in.
///
/// Rules are held sorted by descending length, then by bytes, and `groups`
/// indexes the runs of equal length. A lookup walks the length groups from
/// longest to shortest and binary-searches within each, so the first match it
/// finds is the longest one and the cost is (distinct lengths x log n) rather
/// than a scan of every rule.
///
/// The scan came first and a benchmark rejected it: with 64 rules of the same
/// length -- which is what a map of same-shaped namespaces looks like -- a key
/// matching the last of them cost 154 ns/op, against a store write measured in
/// single-digit nanoseconds. The same case is ~5 ns this way.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    default: Tier,
    /// (prefix, tier), sorted by descending `prefix.len()` then ascending
    /// bytes, so each equal-length run is ordered for binary search.
    rules: Vec<(Vec<u8>, Tier)>,
    /// `(len, start, end)` per run of equal-length rules in `rules`, in
    /// descending length order.
    groups: Vec<(usize, usize, usize)>,
    /// Set bit `b` means some rule's first byte is `b`. A key whose first byte
    /// is not in here cannot match any rule, which is the common case on a
    /// mostly-ephemeral instance and is worth one lookup to skip the search.
    first_bytes: [u64; 4],
    /// Length of the shortest rule; a key shorter than this cannot match.
    min_len: usize,
}

impl Policy {
    /// One tier for every key — what an instance without overrides has, and
    /// what `tier_for` collapses to with no branching worth measuring.
    pub fn uniform(default: Tier) -> Policy {
        Policy {
            default,
            rules: Vec::new(),
            groups: Vec::new(),
            first_bytes: [0; 4],
            min_len: usize::MAX,
        }
    }

    /// Parse the `pg_keyspace.durability_overrides` spec: a comma-separated
    /// list of `prefix=tier`, e.g. `acme:=durable, metrics:=relaxed`.
    ///
    /// Errors quote the offending entry. A durability setting that silently
    /// ignores half its configuration is worse than one that refuses to start,
    /// so every reject here is meant to reach an operator verbatim.
    pub fn parse(default: Tier, spec: &str) -> Result<Policy, String> {
        let mut rules: Vec<(Vec<u8>, Tier)> = Vec::new();
        for entry in spec.split(',') {
            let entry = entry.trim();
            if entry.is_empty() {
                // Tolerates a trailing comma and the empty spec, which is how
                // an instance with no overrides is written.
                continue;
            }
            let (prefix, tier) = entry.split_once('=').ok_or_else(|| {
                format!("'{entry}' is not 'prefix=tier' (expected something like 'acme:=durable')")
            })?;
            let prefix = prefix.trim();
            let tier = tier.trim();
            if prefix.is_empty() {
                // An empty prefix matches every key, which is not an override
                // at all -- it is a different default, and there is already a
                // setting for that. Saying so beats honouring it and leaving
                // two settings that contradict each other.
                return Err(format!(
                    "'{entry}' has an empty prefix, which would match every key; \
                     set pg_keyspace.durability instead"
                ));
            }
            let tier = parse_tier(tier).ok_or_else(|| {
                format!(
                    "'{entry}' names an unknown tier '{tier}' \
                     (expected ephemeral, relaxed, durable or replicated)"
                )
            })?;
            let prefix = prefix.as_bytes().to_vec();
            if let Some((dup, _)) = rules.iter().find(|(p, _)| *p == prefix) {
                // Two tiers for one prefix has no defensible reading: neither
                // "first wins" nor "last wins" is something an operator should
                // have to know.
                return Err(format!(
                    "prefix '{}' is given more than once",
                    String::from_utf8_lossy(dup)
                ));
            }
            rules.push((prefix, tier));
        }
        Ok(Policy::from_rules(default, rules))
    }

    /// Build from already-validated rules, establishing the sort order and the
    /// scan shortcuts `tier_for` relies on.
    pub fn from_rules(default: Tier, mut rules: Vec<(Vec<u8>, Tier)>) -> Policy {
        // Length descending so the first group that matches is the longest
        // prefix; bytes ascending within a length so the group is searchable.
        rules.sort_by(|a, b| b.0.len().cmp(&a.0.len()).then_with(|| a.0.cmp(&b.0)));
        let mut first_bytes = [0u64; 4];
        let mut min_len = usize::MAX;
        for (p, _) in &rules {
            if let Some(&b) = p.first() {
                first_bytes[(b >> 6) as usize] |= 1u64 << (b & 63);
            }
            min_len = min_len.min(p.len());
        }
        let mut groups: Vec<(usize, usize, usize)> = Vec::new();
        let mut i = 0;
        while i < rules.len() {
            let len = rules[i].0.len();
            let mut j = i;
            while j < rules.len() && rules[j].0.len() == len {
                j += 1;
            }
            groups.push((len, i, j));
            i = j;
        }
        Policy { default, rules, groups, first_bytes, min_len }
    }

    /// The tier `key` is written at. Runs on every write, so the miss path is
    /// the one that matters: a first-byte bitmap and a minimum length reject
    /// most keys before any comparison.
    #[inline]
    pub fn tier_for(&self, key: &[u8]) -> Tier {
        if self.rules.is_empty() || key.len() < self.min_len {
            return self.default;
        }
        let b = match key.first() {
            Some(&b) => b,
            None => return self.default,
        };
        if self.first_bytes[(b >> 6) as usize] & (1u64 << (b & 63)) == 0 {
            return self.default;
        }
        for &(len, start, end) in &self.groups {
            // Groups are length-descending, so the ones too long for this key
            // come first and every later group is short enough.
            if len > key.len() {
                continue;
            }
            let head = &key[..len];
            let group = &self.rules[start..end];
            // Each prefix is its own allocation, so every binary-search probe
            // is a likely cache miss -- measured at roughly 10 ns apiece. That
            // makes the search slower than a scan until the group is big
            // enough for log(n) to win, which the benchmark puts at about a
            // dozen. Below that, walk it; above, search it.
            if group.len() <= LINEAR_SCAN_MAX {
                for (p, tier) in group {
                    if p.as_slice() == head {
                        return *tier;
                    }
                }
            } else if let Ok(i) = group.binary_search_by(|(p, _)| p.as_slice().cmp(head)) {
                return group[i].1;
            }
        }
        self.default
    }

    /// Whether a write to `key` is staged for persistence at all.
    #[inline]
    pub fn persists(&self, key: &[u8]) -> bool {
        self.tier_for(key) != Tier::Ephemeral
    }

    /// Whether a write to `key` holds its RESP reply until the record commits.
    /// `relaxed` persists but acks immediately, so this is not the same
    /// question as `persists`.
    #[inline]
    pub fn holds_reply(&self, key: &[u8]) -> bool {
        matches!(self.tier_for(key), Tier::Durable | Tier::Replicated)
    }

    /// True when every key gets the same tier, so callers can keep the cheaper
    /// instance-wide paths (and the startup log can say so plainly).
    pub fn is_uniform(&self) -> bool {
        self.rules.is_empty()
    }

    /// The tier every key not covered by a rule is written at.
    pub fn default_tier(&self) -> Tier {
        self.default
    }

    /// Rules in match order (longest prefix first).
    pub fn rules(&self) -> &[(Vec<u8>, Tier)] {
        &self.rules
    }

    /// Every tier this policy can produce, including the default. Used to
    /// reject a configuration whose tiers cannot be served together.
    pub fn tiers_used(&self) -> Vec<Tier> {
        let mut out = vec![self.default];
        for (_, t) in &self.rules {
            if !out.contains(t) {
                out.push(*t);
            }
        }
        out
    }

    /// The strongest tier this policy can produce, over the default and every
    /// rule.
    ///
    /// This is what the *instance* must be provisioned for, and it is not the
    /// same as the configured default tier. An instance whose default is
    /// `ephemeral` with one `durable` prefix still needs rings, persistence
    /// workers and a synchronous commit -- provisioning from the default tier
    /// would leave the durable prefix with nowhere to go. Provisioning from
    /// the strongest is always safe in the other direction: a tier only ever
    /// promises less than a stronger one, so a `relaxed` key committed with
    /// `synchronous_commit = on` is over-served, never under-served.
    pub fn strongest(&self) -> Tier {
        let mut best = self.default;
        for (_, t) in &self.rules {
            if tier_rank(*t) > tier_rank(best) {
                best = *t;
            }
        }
        best
    }

    /// True when any key at all can be persisted. An instance whose default is
    /// ephemeral and whose every override is ephemeral needs no rings.
    pub fn any_persisted(&self) -> bool {
        self.default != Tier::Ephemeral || self.rules.iter().any(|(_, t)| *t != Tier::Ephemeral)
    }

    /// Render for a startup log line, in match order.
    pub fn describe(&self) -> String {
        if self.rules.is_empty() {
            return format!("{} for every key", tier_name(self.default));
        }
        let mut s = String::new();
        for (p, t) in &self.rules {
            if !s.is_empty() {
                s.push_str(", ");
            }
            s.push_str(&format!("{}*={}", String::from_utf8_lossy(p), tier_name(*t)));
        }
        format!("{s}, otherwise {}", tier_name(self.default))
    }
}

/// How much a tier promises, for comparing two of them. Ephemeral promises
/// nothing; replicated promises the most.
pub fn tier_rank(t: Tier) -> u8 {
    match t {
        Tier::Ephemeral => 0,
        Tier::Relaxed => 1,
        Tier::Durable => 2,
        Tier::Replicated => 3,
    }
}

/// Tier names as the GUCs spell them, in both directions.
pub fn parse_tier(s: &str) -> Option<Tier> {
    match s.to_ascii_lowercase().as_str() {
        "ephemeral" => Some(Tier::Ephemeral),
        "relaxed" => Some(Tier::Relaxed),
        "durable" => Some(Tier::Durable),
        "replicated" => Some(Tier::Replicated),
        _ => None,
    }
}

pub fn tier_name(t: Tier) -> &'static str {
    match t {
        Tier::Ephemeral => "ephemeral",
        Tier::Relaxed => "relaxed",
        Tier::Durable => "durable",
        Tier::Replicated => "replicated",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(spec: &str) -> Policy {
        Policy::parse(Tier::Ephemeral, spec).expect("spec should parse")
    }

    #[test]
    fn empty_spec_is_the_default_for_everything() {
        let pol = p("");
        assert!(pol.is_uniform());
        assert_eq!(pol.tier_for(b"anything"), Tier::Ephemeral);
        assert_eq!(pol.tier_for(b""), Tier::Ephemeral);
        assert!(!pol.persists(b"anything"));
    }

    #[test]
    fn a_prefix_overrides_the_default() {
        let pol = p("acme:=durable");
        assert_eq!(pol.tier_for(b"acme:cert:example.com"), Tier::Durable);
        assert_eq!(pol.tier_for(b"cache:GET:/rest/v1/todos"), Tier::Ephemeral);
        assert!(pol.persists(b"acme:x"));
        assert!(!pol.persists(b"cache:x"));
    }

    #[test]
    fn the_default_can_be_durable_with_ephemeral_exceptions() {
        // The mostly-durable deployment, which a boolean list of durable
        // prefixes could not express.
        let pol = Policy::parse(Tier::Durable, "cache:=ephemeral").unwrap();
        assert_eq!(pol.tier_for(b"routing:tenant-a"), Tier::Durable);
        assert_eq!(pol.tier_for(b"cache:x"), Tier::Ephemeral);
    }

    #[test]
    fn longest_prefix_wins_whichever_order_it_is_written() {
        let a = p("a:=durable, a:b:=ephemeral");
        let b = p("a:b:=ephemeral, a:=durable");
        for pol in [&a, &b] {
            assert_eq!(pol.tier_for(b"a:x"), Tier::Durable);
            assert_eq!(pol.tier_for(b"a:b:x"), Tier::Ephemeral);
            assert_eq!(pol.tier_for(b"a:bx"), Tier::Durable);
        }
        assert_eq!(a, b);
    }

    #[test]
    fn a_tenant_scoped_key_matches_a_tenant_prefix() {
        // Forced scoping stores `{tenant}:{key}`, so per-tenant durability is
        // just a prefix rule -- this is the whole of the tenant story.
        let pol = p("t1:=durable");
        assert_eq!(pol.tier_for(b"t1:session:abc"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t2:session:abc"), Tier::Ephemeral);
    }

    #[test]
    fn a_prefix_shorter_than_the_key_is_required_not_equality() {
        let pol = p("acme:=durable");
        assert_eq!(pol.tier_for(b"acme:"), Tier::Durable);
        assert_eq!(pol.tier_for(b"acme"), Tier::Ephemeral); // shorter than the rule
        assert_eq!(pol.tier_for(b"acm"), Tier::Ephemeral);
    }

    #[test]
    fn whitespace_and_a_trailing_comma_are_tolerated() {
        let pol = p("  acme: = durable ,  metrics:=relaxed , ");
        assert_eq!(pol.tier_for(b"acme:x"), Tier::Durable);
        assert_eq!(pol.tier_for(b"metrics:x"), Tier::Relaxed);
    }

    #[test]
    fn tier_names_are_case_insensitive() {
        let pol = p("acme:=DURABLE");
        assert_eq!(pol.tier_for(b"acme:x"), Tier::Durable);
    }

    #[test]
    fn an_entry_without_an_equals_is_rejected_by_name() {
        let e = Policy::parse(Tier::Ephemeral, "acme:").unwrap_err();
        assert!(e.contains("acme:"), "{e}");
        assert!(e.contains("prefix=tier"), "{e}");
    }

    #[test]
    fn an_unknown_tier_is_rejected_by_name() {
        let e = Policy::parse(Tier::Ephemeral, "acme:=forever").unwrap_err();
        assert!(e.contains("forever"), "{e}");
        assert!(e.contains("ephemeral"), "{e}"); // lists what is accepted
    }

    #[test]
    fn an_empty_prefix_is_rejected_and_points_at_the_default_setting() {
        let e = Policy::parse(Tier::Ephemeral, "=durable").unwrap_err();
        assert!(e.contains("pg_keyspace.durability"), "{e}");
    }

    #[test]
    fn a_repeated_prefix_is_rejected() {
        let e = Policy::parse(Tier::Ephemeral, "acme:=durable, acme:=relaxed").unwrap_err();
        assert!(e.contains("acme:"), "{e}");
        assert!(e.contains("more than once"), "{e}");
    }

    #[test]
    fn relaxed_persists_but_does_not_hold_the_reply() {
        let pol = p("r:=relaxed, d:=durable, e:=ephemeral");
        assert!(pol.persists(b"r:x") && !pol.holds_reply(b"r:x"));
        assert!(pol.persists(b"d:x") && pol.holds_reply(b"d:x"));
        assert!(!pol.persists(b"e:x") && !pol.holds_reply(b"e:x"));
    }

    #[test]
    fn tiers_used_reports_the_default_and_every_override() {
        let pol = p("a:=durable, b:=durable, c:=replicated");
        let used = pol.tiers_used();
        assert!(used.contains(&Tier::Ephemeral)); // the default
        assert!(used.contains(&Tier::Durable));
        assert!(used.contains(&Tier::Replicated));
        assert_eq!(used.len(), 3, "duplicates should collapse: {used:?}");
    }

    #[test]
    fn any_persisted_sees_an_override_under_an_ephemeral_default() {
        assert!(!Policy::uniform(Tier::Ephemeral).any_persisted());
        assert!(p("acme:=durable").any_persisted());
        assert!(!p("a:=ephemeral").any_persisted());
        assert!(Policy::uniform(Tier::Durable).any_persisted());
    }

    #[test]
    fn the_first_byte_shortcut_does_not_change_any_answer() {
        // The bitmap and min-length rejects are an optimisation; brute force is
        // the specification. Any disagreement is a bug in the shortcut.
        let pol = p("acme:=durable, metrics:=relaxed, zz=durable, a:b:=ephemeral");
        let brute = |key: &[u8]| {
            let mut best: Option<(usize, Tier)> = None;
            for (p, t) in pol.rules() {
                if key.starts_with(p) && best.map_or(true, |(n, _)| p.len() > n) {
                    best = Some((p.len(), *t));
                }
            }
            best.map(|(_, t)| t).unwrap_or(pol.default_tier())
        };
        for key in [
            &b""[..], b"a", b"a:", b"a:b", b"a:b:", b"a:b:c", b"acme", b"acme:", b"acme:x",
            b"metrics:x", b"zz", b"zzz", b"z", b"\x00", b"\xff\xfe", b"Acme:", b"~",
        ] {
            assert_eq!(pol.tier_for(key), brute(key), "key {:?}", String::from_utf8_lossy(key));
        }
    }

    /// The grouped binary search replaced a linear scan, so the oracle is
    /// worth running over something larger than a handful of rules: many
    /// prefixes, overlapping at several lengths, probed with keys that hit,
    /// miss, and stop one byte short of a boundary.
    #[test]
    fn grouped_search_agrees_with_brute_force_at_scale() {
        let mut rules: Vec<(Vec<u8>, Tier)> = Vec::new();
        for i in 0..64u32 {
            // Same-length siblings, which is the shape that made the scan O(n).
            rules.push((format!("svc{i:02}:").into_bytes(), Tier::Durable));
            // ...and a longer, more specific rule inside some of them.
            if i % 3 == 0 {
                rules.push((format!("svc{i:02}:cold:").into_bytes(), Tier::Ephemeral));
            }
            // ...and a shorter one that several keys also match.
            if i % 8 == 0 {
                rules.push((format!("s{i}").into_bytes(), Tier::Relaxed));
            }
        }
        rules.push((b"a".to_vec(), Tier::Relaxed));
        rules.push((b"ab".to_vec(), Tier::Durable));
        rules.push((b"abc".to_vec(), Tier::Ephemeral));
        let pol = Policy::from_rules(Tier::Ephemeral, rules);

        let brute = |key: &[u8]| {
            let mut best: Option<(usize, Tier)> = None;
            for (p, t) in pol.rules() {
                if key.starts_with(p) && best.map_or(true, |(n, _)| p.len() > n) {
                    best = Some((p.len(), *t));
                }
            }
            best.map(|(_, t)| t).unwrap_or(pol.default_tier())
        };

        let mut probes: Vec<Vec<u8>> = vec![
            b"".to_vec(), b"a".to_vec(), b"ab".to_vec(), b"abc".to_vec(), b"abcd".to_vec(),
            b"s".to_vec(), b"sv".to_vec(), b"svc".to_vec(), b"zzz".to_vec(),
        ];
        for i in 0..70u32 {
            probes.push(format!("svc{i:02}:key").into_bytes());
            probes.push(format!("svc{i:02}:cold:key").into_bytes());
            probes.push(format!("svc{i:02}").into_bytes()); // one byte short
            probes.push(format!("s{i}x").into_bytes());
        }
        for key in &probes {
            assert_eq!(
                pol.tier_for(key),
                brute(key),
                "key {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }

    #[test]
    fn strongest_is_what_the_instance_must_be_provisioned_for() {
        // The self-host shape: an ephemeral default with one durable prefix
        // still needs rings and a synchronous commit.
        assert_eq!(p("acme:=durable").strongest(), Tier::Durable);
        assert_eq!(p("a:=relaxed, b:=durable").strongest(), Tier::Durable);
        assert_eq!(p("a:=relaxed").strongest(), Tier::Relaxed);
        assert_eq!(p("a:=ephemeral").strongest(), Tier::Ephemeral);
        assert_eq!(p("a:=durable, b:=replicated").strongest(), Tier::Replicated);
        // A weaker override never lowers what the instance provides.
        assert_eq!(
            Policy::parse(Tier::Durable, "cache:=ephemeral").unwrap().strongest(),
            Tier::Durable
        );
        assert_eq!(Policy::uniform(Tier::Ephemeral).strongest(), Tier::Ephemeral);
    }

    #[test]
    fn describe_reads_in_match_order() {
        let d = p("a:=durable, a:b:=ephemeral").describe();
        assert!(d.starts_with("a:b:*=ephemeral"), "{d}");
        assert!(d.ends_with("otherwise ephemeral"), "{d}");
        assert_eq!(Policy::uniform(Tier::Durable).describe(), "durable for every key");
    }
}
