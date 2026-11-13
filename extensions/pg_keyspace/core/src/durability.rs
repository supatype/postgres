//! Per-key durability: which tier an individual key is written at.
//!
//! `pg_keyspace.durability` sets one tier for the whole instance, which forces
//! a deployment needing *any* durable data to run its entire keyspace durable,
//! sending every cache write through the WAL to protect the small part that
//! matters. This module is the exception list: a prefix, the tier keys under it
//! are written at, and the instance tier as the default for everything else.
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
//! **Why the prefix.** Redis's other namespacing axis is not available here:
//! `SELECT` is accepted and ignored, so there are no logical databases to hang
//! this off. The prefix is what clients already use to namespace a shared
//! keyspace (`cache:`, `sess:`, `kong_acme:`), precisely *because* Redis
//! databases are not worth using. It also covers tenants at no extra cost:
//! forced tenant scoping stores keys as `{tenant}:{key}`, so `tenantA:` is a
//! prefix like any other and a per-tenant durability policy needs no second
//! surface.
//!
//! **Why a segment form as well.** The reverse question, "this namespace in
//! *every* tenant", is one a prefix cannot answer, because the tenant comes
//! first and differs per tenant. A deployment whose every key is scoped could
//! not keep `dbcred:` durable and `mau:` ephemeral without listing every
//! tenant. A rule spelled `*:mau:` matches `mau:` immediately after the tenant
//! scope: everything up to and including the key's first `:`, the same
//! boundary the store uses for tenant eviction budgets.
//!
//! Matching is against the key **as stored**, which is the tenant-scoped form,
//! because that is the key that reaches the ring and the one `supacache.kv`
//! holds. Recovery applies the same policy, so a prefix dropped from the map
//! stops being served after a restart rather than coming back as if it were
//! still durable.

use crate::batcher::Tier;
use crate::store::tenant_scope_len;

/// Equal-length rules up to this many are scanned rather than binary-searched.
/// See `PrefixSet::search`: the crossover is where log(n) cache-missing probes
/// start to beat n cheap comparisons, which `prefix_match_bench` puts around a
/// dozen.
const LINEAR_SCAN_MAX: usize = 12;

/// How an override spells "after any tenant": `*:mau:=ephemeral`.
const SEGMENT_MARK: &str = "*:";

/// A set of byte prefixes with a tier each, answering "which is the longest
/// one this key starts with, and how long is it".
///
/// Rules are held sorted by descending length, then by bytes, and `groups`
/// indexes the runs of equal length. A lookup walks the length groups from
/// longest to shortest and binary-searches within each, so the first match it
/// finds is the longest one and the cost is (distinct lengths x log n) rather
/// than a scan of every rule.
///
/// The scan came first and a benchmark rejected it: with 64 rules of the same
/// length (which is what a map of same-shaped namespaces looks like) a key
/// matching the last of them cost 154 ns/op, against a store write measured in
/// single-digit nanoseconds. The same case is ~5 ns this way.
#[derive(Clone, Debug, PartialEq, Eq)]
struct PrefixSet {
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

impl PrefixSet {
    /// Establish the sort order and the scan shortcuts `longest` relies on.
    fn new(mut rules: Vec<(Vec<u8>, Tier)>) -> PrefixSet {
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
        PrefixSet { rules, groups, first_bytes, min_len }
    }

    fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }

    /// The longest rule `key` starts with, as (its length, its tier). The miss
    /// path is the one that matters: a first-byte bitmap and a minimum length
    /// reject most keys before any comparison.
    #[inline]
    fn longest(&self, key: &[u8]) -> Option<(usize, Tier)> {
        if key.len() < self.min_len {
            return None;
        }
        let b = *key.first()?;
        if self.first_bytes[(b >> 6) as usize] & (1u64 << (b & 63)) == 0 {
            return None;
        }
        // Groups are length-descending, so the ones too long for this key
        // come first and every later group is short enough.
        self.groups
            .iter()
            .filter(|&&(len, _, _)| len <= key.len())
            .find_map(|&(len, start, end)| {
                Self::search(&self.rules[start..end], &key[..len]).map(|t| (len, t))
            })
    }

    /// The tier of the rule in `group` equal to `head`, if there is one.
    #[inline]
    fn search(group: &[(Vec<u8>, Tier)], head: &[u8]) -> Option<Tier> {
        // Each prefix is its own allocation, so every binary-search probe is a
        // likely cache miss, measured at roughly 10 ns apiece. That makes the
        // search slower than a scan until the group is big enough for log(n)
        // to win, which the benchmark puts at about a dozen. Below that, walk
        // it; above, search it.
        if group.len() <= LINEAR_SCAN_MAX {
            return group.iter().find(|(p, _)| p.as_slice() == head).map(|(_, t)| *t);
        }
        group.binary_search_by(|(p, _)| p.as_slice().cmp(head)).ok().map(|i| group[i].1)
    }
}

/// A map from key to tier with a default, resolving any key to exactly one
/// tier. Two rule forms:
///
/// - a **literal** prefix of the stored key, `acme:=durable`;
/// - a **segment**, `*:mau:=ephemeral`, a prefix of whatever follows the
///   tenant scope (the key's first `:`), for any tenant.
///
/// Both are measured the same way, by how much of the stored key the rule
/// covers: a literal covers its own length, and a segment covers the tenant
/// scope plus its own length. **The longest match wins**, which is what makes
/// `a:` and `a:b:` both configurable without the result depending on the order
/// the operator happened to write them in, and it carries over unchanged: on
/// `t1:mau:x`, `t1:=durable` covers 3 bytes and `*:mau:` covers 7, so the
/// segment wins, while `t1:mau:org:` would cover 11 and beat it. On a tie the
/// literal wins, because it names the tenant and the segment does not.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    default: Tier,
    literal: PrefixSet,
    /// Matched against the key after its tenant scope. Empty on every
    /// deployment that does not use the form, which keeps `tier_for` to one
    /// extra branch for them.
    segment: PrefixSet,
}

impl Policy {
    /// One tier for every key: what an instance without overrides has, and
    /// what `tier_for` collapses to with no branching worth measuring.
    pub fn uniform(default: Tier) -> Policy {
        Policy::with_segments(default, Vec::new(), Vec::new())
    }

    /// Parse the `pg_keyspace.durability_overrides` spec: a comma-separated
    /// list of `prefix=tier`, e.g. `acme:=durable, *:mau:=ephemeral`.
    ///
    /// Errors quote the offending entry. A durability setting that silently
    /// ignores half its configuration is worse than one that refuses to start,
    /// so every reject here is meant to reach an operator verbatim.
    pub fn parse(default: Tier, spec: &str) -> Result<Policy, String> {
        let mut literal: Vec<(Vec<u8>, Tier)> = Vec::new();
        let mut segment: Vec<(Vec<u8>, Tier)> = Vec::new();
        // Tolerates a trailing comma and the empty spec, which is how an
        // instance with no overrides is written.
        for entry in spec.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let rule = parse_entry(entry)?;
            let (rules, mark) =
                if rule.segment { (&mut segment, SEGMENT_MARK) } else { (&mut literal, "") };
            if rules.iter().any(|(p, _)| *p == rule.bytes) {
                // Two tiers for one prefix has no defensible reading: neither
                // "first wins" nor "last wins" is something an operator should
                // have to know.
                return Err(format!(
                    "prefix '{mark}{}' is given more than once",
                    String::from_utf8_lossy(&rule.bytes)
                ));
            }
            rules.push((rule.bytes, rule.tier));
        }
        Ok(Policy::with_segments(default, literal, segment))
    }

    /// Build from already-validated literal rules.
    pub fn from_rules(default: Tier, rules: Vec<(Vec<u8>, Tier)>) -> Policy {
        Policy::with_segments(default, rules, Vec::new())
    }

    /// Build from already-validated literal and segment rules. A segment rule
    /// is given without its `*:` mark: `mau:` for `*:mau:`.
    pub fn with_segments(
        default: Tier,
        literal: Vec<(Vec<u8>, Tier)>,
        segment: Vec<(Vec<u8>, Tier)>,
    ) -> Policy {
        Policy { default, literal: PrefixSet::new(literal), segment: PrefixSet::new(segment) }
    }

    /// The tier `key` is written at. Runs on every write; a deployment with no
    /// segment rules pays one branch over the literal lookup.
    #[inline]
    pub fn tier_for(&self, key: &[u8]) -> Tier {
        let literal = self.literal.longest(key);
        if self.segment.is_empty() {
            return literal.map_or(self.default, |(_, t)| t);
        }
        // A segment wins only by covering strictly more: a tie goes to the
        // literal, which names the tenant.
        match (literal, self.segment_match(key)) {
            (Some((ln, _)), Some((sn, st))) if sn > ln => st,
            (Some((_, t)), _) | (None, Some((_, t))) => t,
            (None, None) => self.default,
        }
    }

    /// The longest segment rule matching what follows `key`'s tenant scope, as
    /// (bytes of `key` it covers, tier). A key with no `:` has no tenant scope
    /// and so no segment.
    #[inline]
    fn segment_match(&self, key: &[u8]) -> Option<(usize, Tier)> {
        // The scope is at least the `:`, so a key this short cannot hold one
        // and a rule after it; skip the search for the `:`.
        if key.len() <= self.segment.min_len {
            return None;
        }
        let scope = tenant_scope_len(key)?;
        self.segment.longest(&key[scope..]).map(|(n, t)| (scope + n, t))
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
        self.literal.is_empty() && self.segment.is_empty()
    }

    /// The tier every key not covered by a rule is written at.
    pub fn default_tier(&self) -> Tier {
        self.default
    }

    /// Literal rules, longest first.
    pub fn rules(&self) -> &[(Vec<u8>, Tier)] {
        &self.literal.rules
    }

    /// Segment rules without their `*:` mark, longest first.
    pub fn segment_rules(&self) -> &[(Vec<u8>, Tier)] {
        &self.segment.rules
    }

    /// The tier of every override rule, of either form.
    fn rule_tiers(&self) -> impl Iterator<Item = Tier> + '_ {
        self.literal.rules.iter().chain(&self.segment.rules).map(|(_, t)| *t)
    }

    /// Whether any override rule, of either form, names `tier`.
    pub fn overrides_name(&self, tier: Tier) -> bool {
        self.rule_tiers().any(|t| t == tier)
    }

    /// Every tier this policy can produce, including the default. Used to
    /// reject a configuration whose tiers cannot be served together.
    pub fn tiers_used(&self) -> Vec<Tier> {
        let mut out = vec![self.default];
        for t in self.rule_tiers() {
            if !out.contains(&t) {
                out.push(t);
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
        self.rule_tiers()
            .fold(self.default, |best, t| if tier_rank(t) > tier_rank(best) { t } else { best })
    }

    /// True when any key at all can be persisted. An instance whose default is
    /// ephemeral and whose every override is ephemeral needs no rings.
    pub fn any_persisted(&self) -> bool {
        self.default != Tier::Ephemeral || self.rule_tiers().any(|t| t != Tier::Ephemeral)
    }

    /// Render for a startup log line: literal rules longest first, then
    /// segment rules longest first. Across the two lists the longest match
    /// wins, so on a key both cover the order here is not the answer.
    pub fn describe(&self) -> String {
        if self.is_uniform() {
            return format!("{} for every key", tier_name(self.default));
        }
        let literal = self.literal.rules.iter().map(|r| ("", r));
        let segment = self.segment.rules.iter().map(|r| (SEGMENT_MARK, r));
        let mut parts: Vec<String> = literal
            .chain(segment)
            .map(|(mark, (p, t))| {
                format!("{mark}{}*={}", String::from_utf8_lossy(p), tier_name(*t))
            })
            .collect();
        parts.push(format!("otherwise {}", tier_name(self.default)));
        parts.join(", ")
    }
}

/// One parsed `pg_keyspace.durability_overrides` entry.
struct Entry {
    /// True for `*:segment=tier`, whose `bytes` are the segment alone.
    segment: bool,
    bytes: Vec<u8>,
    tier: Tier,
}

/// Parse one non-empty, trimmed entry, quoting it in every error.
fn parse_entry(entry: &str) -> Result<Entry, String> {
    let (prefix, tier) = entry.split_once('=').ok_or_else(|| {
        format!("'{entry}' is not 'prefix=tier' (expected something like 'acme:=durable')")
    })?;
    let (prefix, tier) = (prefix.trim(), tier.trim());
    if prefix.is_empty() {
        // An empty prefix matches every key, which is not an override at all:
        // it is a different default, and there is already a setting for that.
        // Saying so beats honouring it and leaving two settings that
        // contradict each other.
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
    match prefix.strip_prefix(SEGMENT_MARK) {
        Some("") => Err(format!(
            "'{entry}' names no segment after the tenant; a segment rule reads \
             '*:mau:=ephemeral', matching 'mau:' after any tenant"
        )),
        Some(seg) => Ok(Entry { segment: true, bytes: seg.as_bytes().to_vec(), tier }),
        None => Ok(Entry { segment: false, bytes: prefix.as_bytes().to_vec(), tier }),
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
                if key.starts_with(p) && best.is_none_or(|(n, _)| p.len() > n) {
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
                if key.starts_with(p) && best.is_none_or(|(n, _)| p.len() > n) {
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

    /// The Cloud platform shape: every key is `{tenant}:...`, and the tenant
    /// is a different string per environment, so no literal prefix can name
    /// `mau:` without naming the tenant too.
    #[test]
    fn a_segment_rule_matches_after_any_tenant() {
        let pol = Policy::parse(Tier::Durable, "*:mau:=ephemeral").unwrap();
        assert_eq!(pol.tier_for(b"t1:mau:org:O:d:2026-11-13"), Tier::Ephemeral);
        assert_eq!(pol.tier_for(b"env-7f3a.prod_2:mau:org:O:d:2026-11-13"), Tier::Ephemeral);
        assert!(!pol.persists(b"t2:mau:x"));
    }

    #[test]
    fn a_durable_key_stays_durable_beside_an_ephemeral_mau_key_of_the_same_tenant() {
        let pol = Policy::parse(Tier::Durable, "*:mau:=ephemeral").unwrap();
        assert_eq!(pol.tier_for(b"t1:config"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t1:dbcred:gen3"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t1:mau:org:O:d:2026-11-13"), Tier::Ephemeral);
        assert!(pol.holds_reply(b"t1:dbcred:gen3"));
        assert!(!pol.holds_reply(b"t1:mau:org:O:d:2026-11-13"));
        // ...and the other way round, under an ephemeral default.
        let pol = p("*:config=durable, *:dbcred:=durable");
        assert_eq!(pol.tier_for(b"t1:config"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t1:dbcred:gen3"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t1:mau:org:O:d:2026-11-13"), Tier::Ephemeral);
    }

    #[test]
    fn the_segment_starts_exactly_after_the_first_colon() {
        let pol = Policy::parse(Tier::Durable, "*:mau:=ephemeral").unwrap();
        // An empty tenant is the scope of a credential stored with tenant ''.
        assert_eq!(pol.tier_for(b":mau:x"), Tier::Ephemeral);
        // Deeper in the key is not after the tenant.
        assert_eq!(pol.tier_for(b"t1:x:mau:y"), Tier::Durable);
        // An unscoped key: its first colon ends `mau`, so nothing follows it
        // that starts with `mau:`.
        assert_eq!(pol.tier_for(b"mau:x"), Tier::Durable);
        // No colon at all, and a tenant run into the segment.
        assert_eq!(pol.tier_for(b"t1mau:x"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t1"), Tier::Durable);
        // One byte short of the segment.
        assert_eq!(pol.tier_for(b"t1:mau"), Tier::Durable);
    }

    #[test]
    fn the_longest_match_wins_across_both_rule_forms() {
        // The segment match covers `t1:mau:`, longer than `t1:`.
        let pol = p("t1:=durable, *:mau:=ephemeral");
        assert_eq!(pol.tier_for(b"t1:mau:x"), Tier::Ephemeral);
        assert_eq!(pol.tier_for(b"t1:config"), Tier::Durable);
        // A longer literal beats the segment for the tenant it names.
        let pol = p("*:mau:=relaxed, t1:mau:org:=durable");
        assert_eq!(pol.tier_for(b"t1:mau:org:x"), Tier::Durable);
        assert_eq!(pol.tier_for(b"t2:mau:org:x"), Tier::Relaxed);
        // Equal length: the literal names the tenant, so it is the more
        // specific of the two and wins in either order.
        let a = p("t1:mau:=relaxed, *:mau:=durable");
        let b = p("*:mau:=durable, t1:mau:=relaxed");
        for pol in [&a, &b] {
            assert_eq!(pol.tier_for(b"t1:mau:x"), Tier::Relaxed);
            assert_eq!(pol.tier_for(b"t2:mau:x"), Tier::Durable);
        }
        assert_eq!(a, b);
    }

    #[test]
    fn segment_rules_count_towards_provisioning() {
        let pol = p("*:dbcred:=durable");
        assert_eq!(pol.strongest(), Tier::Durable);
        assert!(pol.any_persisted());
        assert!(!pol.is_uniform());
        assert!(pol.tiers_used().contains(&Tier::Durable));
        assert!(p("*:x:=replicated").tiers_used().contains(&Tier::Replicated));
    }

    #[test]
    fn an_empty_segment_is_rejected() {
        let e = Policy::parse(Tier::Ephemeral, "*:=durable").unwrap_err();
        assert!(e.contains("'*:=durable'"), "{e}");
        assert!(e.contains("segment"), "{e}");
    }

    #[test]
    fn a_repeated_segment_is_rejected() {
        let e = Policy::parse(Tier::Ephemeral, "*:mau:=ephemeral, *:mau:=durable").unwrap_err();
        assert!(e.contains("*:mau:"), "{e}");
        assert!(e.contains("more than once"), "{e}");
    }

    #[test]
    fn describe_reads_in_match_order() {
        let d = p("a:=durable, a:b:=ephemeral").describe();
        assert!(d.starts_with("a:b:*=ephemeral"), "{d}");
        assert!(d.ends_with("otherwise ephemeral"), "{d}");
        assert_eq!(Policy::uniform(Tier::Durable).describe(), "durable for every key");
    }

    #[test]
    fn describe_names_segment_rules_with_their_mark() {
        let pol = Policy::parse(Tier::Durable, "*:mau:=ephemeral, t1:=relaxed").unwrap();
        assert_eq!(pol.describe(), "t1:*=relaxed, *:mau:*=ephemeral, otherwise durable");
        assert_eq!(pol.segment_rules(), &[(b"mau:".to_vec(), Tier::Ephemeral)][..]);
        assert_eq!(pol.rules(), &[(b"t1:".to_vec(), Tier::Relaxed)][..]);
    }

    #[test]
    fn overrides_name_sees_both_forms() {
        // What the worker's refusal of a mixed `replicated` tier reads: a
        // segment rule naming it must not slip past.
        assert!(p("*:x:=replicated").overrides_name(Tier::Replicated));
        assert!(p("x:=replicated").overrides_name(Tier::Replicated));
        assert!(!p("*:x:=durable").overrides_name(Tier::Replicated));
        assert!(!Policy::uniform(Tier::Replicated).overrides_name(Tier::Replicated));
    }

    /// The specification both rule forms are held to: every rule's covered
    /// length computed the slow way, the longest wins, and a literal beats a
    /// segment of the same length.
    fn brute_force(pol: &Policy, key: &[u8]) -> Tier {
        let mut best: Option<(usize, bool, Tier)> = None;
        let mut offer = |len: usize, literal: bool, t: Tier| {
            let better = match best {
                None => true,
                Some((n, lit, _)) => len > n || (len == n && literal && !lit),
            };
            if better {
                best = Some((len, literal, t));
            }
        };
        for (p, t) in pol.rules() {
            if key.starts_with(p) {
                offer(p.len(), true, *t);
            }
        }
        if let Some(colon) = key.iter().position(|b| *b == b':') {
            for (s, t) in pol.segment_rules() {
                if key[colon + 1..].starts_with(s) {
                    offer(colon + 1 + s.len(), false, *t);
                }
            }
        }
        best.map(|(_, _, t)| t).unwrap_or(pol.default_tier())
    }

    /// The grouped search runs over the segment set too, so it gets the same
    /// oracle at a size that takes the binary-search branch, mixed with
    /// literals that tie with, undercut and outrun the segment matches.
    #[test]
    fn literal_and_segment_rules_agree_with_brute_force_at_scale() {
        let mut literal: Vec<(Vec<u8>, Tier)> = Vec::new();
        let mut segment: Vec<(Vec<u8>, Tier)> = Vec::new();
        for i in 0..40u32 {
            literal.push((format!("t{i:02}:").into_bytes(), Tier::Relaxed));
            if i % 5 == 0 {
                literal.push((format!("t{i:02}:mau:").into_bytes(), Tier::Durable));
                literal.push((format!("t{i:02}:mau:org:").into_bytes(), Tier::Durable));
            }
            // Same-length segment siblings, enough to take the binary search.
            segment.push((format!("ns{i:02}:").into_bytes(), Tier::Durable));
        }
        segment.push((b"mau:".to_vec(), Tier::Ephemeral));
        segment.push((b"mau:org:".to_vec(), Tier::Relaxed));
        segment.push((b"m".to_vec(), Tier::Durable));
        segment.push((b"config".to_vec(), Tier::Durable));
        let pol = Policy::with_segments(Tier::Ephemeral, literal, segment);

        let mut probes: Vec<Vec<u8>> = vec![
            b"".to_vec(), b":".to_vec(), b"::".to_vec(), b":mau:".to_vec(), b"mau:".to_vec(),
            b"m".to_vec(), b"t".to_vec(), b"x:m".to_vec(), b"x:config".to_vec(),
            b"x:confi".to_vec(), b"a:b:mau:x".to_vec(),
        ];
        for i in 0..45u32 {
            for tail in ["", "x", "mau", "mau:", "mau:org", "mau:org:k", "config", "m"] {
                probes.push(format!("t{i:02}:{tail}").into_bytes());
                probes.push(format!("ns{i:02}:{tail}").into_bytes());
                probes.push(format!("u{i}:ns{i:02}:{tail}").into_bytes());
                probes.push(format!("u{i}:ns{i:02}{tail}").into_bytes());
            }
        }
        for key in &probes {
            assert_eq!(
                pol.tier_for(key),
                brute_force(&pol, key),
                "key {:?}",
                String::from_utf8_lossy(key)
            );
        }
    }
}
