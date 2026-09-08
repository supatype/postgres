//! aggregate value types: hashes, lists, sorted sets.
//!
//! Each aggregate is stored in the keyspace as a single self-describing blob in
//! the slab (the entry's `kind` tags which type it is), decoded on read and
//! re-encoded on write. Encoding is length-prefixed and endian-fixed so a
//! persisted blob is portable: each element is `len: u32-le` followed by `len`
//! bytes.
//!
//! ## Native large-collection structure (hashes)
//!
//! A small hash is stored inline (a flat listpack-style scan of field/value
//! pairs) — compact, and O(n) lookup is fine when n is tiny (Redis's
//! `hash-max-listpack-entries` philosophy). Past a threshold a hash is instead
//! stored in an **indexed** encoding: an in-value open-addressed bucket table
//! (bucket heads + chained entries) laid out in the *same* single blob. Point
//! reads — HGET/HMGET/HEXISTS/HSTRLEN/HLEN — then run in O(1) average instead of
//! scanning every field, which is the difference that matters on a 10k-field
//! hash. Crucially the collection remains one keyspace value: it is still
//! evicted atomically (CLOCK never tears half a hash away), shipped to the
//! durability ring as one record, and invalidated as one key — so the indexed
//! form is a pure encoding upgrade with no new failure modes. Writes still
//! rebuild the blob (O(n), unchanged); it is the reads that go sub-linear.
//!
//! Every aggregate value's first byte is an encoding tag (inline vs indexed),
//! so a reader dispatches without a schema. Lists and sorted sets promote the
//! same way: a large list gains an explicit offset table (O(1) LINDEX/LRANGE
//! seek, O(1) LLEN); a large sorted set gains a member→score bucket table (O(1)
//! ZSCORE) plus a pre-sorted offset array (O(log n) ZRANK/ZRANGEBYSCORE, O(k)
//! ZRANGE). In every case writes rebuild the one blob (O(n)); the reads go
//! sub-linear, and the collection stays a single atomically-managed value.

/// A hash with more than this many fields is stored in the indexed encoding.
/// Mirrors Redis's `hash-max-listpack-entries` default (128).
pub const HASH_INDEX_THRESHOLD: usize = 128;
/// ...or if any field/value is longer than this (Redis `hash-max-listpack-value`
/// is 64; a big member also makes a linear scan expensive).
pub const HASH_INDEX_VALUE_MAX: usize = 64;

const H_INLINE: u8 = 0;
const H_INDEXED: u8 = 1;

/// A list with more than this many elements uses the indexed encoding (an
/// explicit offset table), so LINDEX/LRANGE/LLEN are O(1) instead of walking
/// every length-prefix from the head.
pub const LIST_INDEX_THRESHOLD: usize = 128;
const L_INLINE: u8 = 0;
const L_INDEXED: u8 = 1;

/// A sorted set with more than this many members uses the indexed encoding: a
/// member→score bucket table (O(1) ZSCORE) plus a pre-sorted offset array
/// (O(log n) ZRANK/ZRANGEBYSCORE, O(k) ZRANGE), instead of re-sorting the whole
/// set on every range/rank op.
pub const ZSET_INDEX_THRESHOLD: usize = 128;
const Z_INLINE: u8 = 0;
const Z_INDEXED: u8 = 1;

/// FNV-1a 64-bit — the bucket hash for the indexed encoding. Self-contained so
/// the blob's layout does not depend on the store's (private) key hash.
fn fieldhash(b: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf29ce484222325;
    for &x in b {
        h ^= x as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn put_bytes(out: &mut Vec<u8>, b: &[u8]) {
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn take_bytes<'a>(buf: &mut &'a [u8]) -> Option<&'a [u8]> {
    if buf.len() < 4 {
        return None;
    }
    let n = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    if buf.len() < 4 + n {
        return None;
    }
    let (_, rest) = buf.split_at(4);
    let (val, rest) = rest.split_at(n);
    *buf = rest;
    Some(val)
}

// ---- Hash -----------------------------------------------------------------

/// A Redis hash: ordered field→value pairs (insertion order preserved, as
/// listpack does). Fields are unique; lookups are linear, which is what small
/// hashes want.
#[derive(Default)]
pub struct Hash {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl Hash {
    pub fn new() -> Hash {
        Hash { entries: Vec::new() }
    }

    /// Decode a stored blob (either encoding). A malformed tail is ignored
    /// (returns what parsed), so a truncated blob degrades to a shorter hash
    /// rather than a panic. Fields come back in insertion order for both
    /// encodings.
    pub fn decode(buf: &[u8]) -> Hash {
        match buf.first() {
            None => Hash::new(),
            Some(&H_INDEXED) => decode_indexed(buf),
            _ => decode_inline(&buf[1..]),
        }
    }

    /// Encode, choosing the inline or indexed layout by size. The chosen tag is
    /// the first byte, so a reader can dispatch without a schema.
    pub fn encode(&self) -> Vec<u8> {
        let big = self.entries.len() > HASH_INDEX_THRESHOLD
            || self
                .entries
                .iter()
                .any(|(f, v)| f.len() > HASH_INDEX_VALUE_MAX || v.len() > HASH_INDEX_VALUE_MAX);
        self.force_encode(big)
    }

    /// Encode with an explicit layout choice, bypassing the size heuristic.
    /// Used by tests and benchmarks to compare the two encodings at one size;
    /// production code calls `encode`.
    pub fn force_encode(&self, indexed: bool) -> Vec<u8> {
        if indexed {
            encode_indexed(&self.entries)
        } else {
            let mut out = Vec::with_capacity(1);
            out.push(H_INLINE);
            for (f, v) in &self.entries {
                put_bytes(&mut out, f);
                put_bytes(&mut out, v);
            }
            out
        }
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn get(&self, field: &[u8]) -> Option<&[u8]> {
        self.entries
            .iter()
            .find(|(f, _)| f == field)
            .map(|(_, v)| v.as_slice())
    }

    /// Insert or overwrite. Returns true if the field was newly added.
    pub fn set(&mut self, field: &[u8], val: &[u8]) -> bool {
        if let Some(e) = self.entries.iter_mut().find(|(f, _)| f == field) {
            e.1 = val.to_vec();
            false
        } else {
            self.entries.push((field.to_vec(), val.to_vec()));
            true
        }
    }

    /// Remove a field. Returns true if it was present.
    pub fn del(&mut self, field: &[u8]) -> bool {
        if let Some(i) = self.entries.iter().position(|(f, _)| f == field) {
            self.entries.remove(i);
            true
        } else {
            false
        }
    }
}

// ---- Hash: encoding internals + O(1) raw-buffer point reads ---------------

fn decode_inline(mut buf: &[u8]) -> Hash {
    let mut h = Hash::new();
    while let (Some(f), Some(v)) = {
        let f = take_bytes(&mut buf);
        let v = if f.is_some() { take_bytes(&mut buf) } else { None };
        (f, v)
    } {
        h.entries.push((f.to_vec(), v.to_vec()));
    }
    h
}

/// Indexed layout: `[H_INDEXED][nbuckets u32][count u32][heads: nbuckets×u32]
/// [entries]`. Each bucket head is `offset+1` into the entries region (0 =
/// empty). Each entry is `flen u32, field, vlen u32, val, next u32` where `next`
/// is the `offset+1` of the next entry in the same bucket (chained, newest
/// first). The entries region is written in insertion order, so a linear walk
/// recovers that order.
fn encode_indexed(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let nbuckets = entries.len().next_power_of_two().max(8);
    let mask = (nbuckets - 1) as u64;
    let mut heads = vec![0u32; nbuckets];
    let mut region: Vec<u8> = Vec::new();
    for (f, v) in entries {
        let off = region.len() as u32;
        let b = (fieldhash(f) & mask) as usize;
        put_bytes(&mut region, f);
        put_bytes(&mut region, v);
        region.extend_from_slice(&heads[b].to_le_bytes()); // next = old head
        heads[b] = off + 1;
    }
    let mut out = Vec::with_capacity(9 + 4 * nbuckets + region.len());
    out.push(H_INDEXED);
    out.extend_from_slice(&(nbuckets as u32).to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for h in &heads {
        out.extend_from_slice(&h.to_le_bytes());
    }
    out.extend_from_slice(&region);
    out
}

fn rd_u32(b: &[u8], at: usize) -> Option<u32> {
    b.get(at..at + 4)
        .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

/// Parse the entry at `region[off..]`: returns (field, value, next, end-offset).
fn read_entry(region: &[u8], off: usize) -> Option<(&[u8], &[u8], u32, usize)> {
    let mut p = off;
    let fl = rd_u32(region, p)? as usize;
    p += 4;
    let f = region.get(p..p + fl)?;
    p += fl;
    let vl = rd_u32(region, p)? as usize;
    p += 4;
    let v = region.get(p..p + vl)?;
    p += vl;
    let next = rd_u32(region, p)?;
    p += 4;
    Some((f, v, next, p))
}

fn decode_indexed(buf: &[u8]) -> Hash {
    let mut h = Hash::new();
    let nbuckets = match rd_u32(buf, 1) {
        Some(n) => n as usize,
        None => return h,
    };
    let region_base = 9 + 4 * nbuckets;
    let region = match buf.get(region_base..) {
        Some(r) => r,
        None => return h,
    };
    let mut off = 0usize;
    while off < region.len() {
        match read_entry(region, off) {
            Some((f, v, _next, end)) => {
                h.entries.push((f.to_vec(), v.to_vec()));
                off = end;
            }
            None => break,
        }
    }
    h
}

/// O(1)-average point read straight off the stored blob — no full decode. Works
/// for both encodings (inline falls back to a linear scan, which is what small
/// hashes want anyway).
pub fn hash_probe<'a>(buf: &'a [u8], field: &[u8]) -> Option<&'a [u8]> {
    match buf.first() {
        None => None,
        Some(&H_INDEXED) => {
            let nbuckets = rd_u32(buf, 1)? as usize;
            let region_base = 9 + 4 * nbuckets;
            let region = buf.get(region_base..)?;
            let b = (fieldhash(field) & (nbuckets as u64 - 1)) as usize;
            let mut head = rd_u32(buf, 9 + 4 * b)?;
            while head != 0 {
                let (f, v, next, _) = read_entry(region, (head - 1) as usize)?;
                if f == field {
                    return Some(v);
                }
                head = next;
            }
            None
        }
        _ => {
            let mut p = &buf[1..];
            loop {
                let f = take_bytes(&mut p)?;
                let v = take_bytes(&mut p)?;
                if f == field {
                    return Some(v);
                }
            }
        }
    }
}

/// O(1) field count for the indexed encoding (header field); O(n) for inline.
pub fn hash_count(buf: &[u8]) -> usize {
    match buf.first() {
        None => 0,
        Some(&H_INDEXED) => rd_u32(buf, 5).unwrap_or(0) as usize,
        _ => {
            let mut p = &buf[1..];
            let mut n = 0;
            while take_bytes(&mut p).is_some() && take_bytes(&mut p).is_some() {
                n += 1;
            }
            n
        }
    }
}

// ---- List -----------------------------------------------------------------

/// A Redis list: an ordered sequence of elements (head = index 0). Backed by a
/// `Vec`; head ops are O(n) on rewrite anyway (the whole blob is re-encoded), so
/// a Vec is as good as a deque here and keeps encoding trivial.
#[derive(Default)]
pub struct List {
    pub items: Vec<Vec<u8>>,
}

impl List {
    pub fn new() -> List {
        List { items: Vec::new() }
    }

    /// Decode a stored blob (either encoding), preserving element order.
    pub fn decode(buf: &[u8]) -> List {
        match buf.first() {
            None => List::new(),
            Some(&L_INDEXED) => {
                let mut l = List::new();
                let count = rd_u32(buf, 1).unwrap_or(0) as usize;
                let data_base = 9 + 4 * count;
                if let Some(data) = buf.get(data_base..) {
                    let mut p = data;
                    while let Some(v) = take_bytes(&mut p) {
                        l.items.push(v.to_vec());
                    }
                }
                l
            }
            _ => {
                let mut p = &buf[1..];
                let mut l = List::new();
                while let Some(v) = take_bytes(&mut p) {
                    l.items.push(v.to_vec());
                }
                l
            }
        }
    }

    /// Encode, choosing inline or indexed by size (first byte is the tag).
    pub fn encode(&self) -> Vec<u8> {
        self.force_encode(self.items.len() > LIST_INDEX_THRESHOLD)
    }

    /// Encode with an explicit layout choice (tests/benches compare the two at
    /// one size). Indexed layout: `[L_INDEXED][count u32][data_len u32]
    /// [offsets: count×u32][data]`, each offset being the start of an element's
    /// length-prefix within the data region — so LINDEX(i) is O(1).
    pub fn force_encode(&self, indexed: bool) -> Vec<u8> {
        if !indexed {
            let mut out = Vec::with_capacity(1);
            out.push(L_INLINE);
            for v in &self.items {
                put_bytes(&mut out, v);
            }
            return out;
        }
        let count = self.items.len();
        let mut data: Vec<u8> = Vec::new();
        let mut offs: Vec<u32> = Vec::with_capacity(count);
        for v in &self.items {
            offs.push(data.len() as u32);
            put_bytes(&mut data, v);
        }
        let mut out = Vec::with_capacity(9 + 4 * count + data.len());
        out.push(L_INDEXED);
        out.extend_from_slice(&(count as u32).to_le_bytes());
        out.extend_from_slice(&(data.len() as u32).to_le_bytes());
        for o in &offs {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&data);
        out
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn lpush(&mut self, v: &[u8]) {
        self.items.insert(0, v.to_vec());
    }

    pub fn rpush(&mut self, v: &[u8]) {
        self.items.push(v.to_vec());
    }

    pub fn lpop(&mut self) -> Option<Vec<u8>> {
        if self.items.is_empty() {
            None
        } else {
            Some(self.items.remove(0))
        }
    }

    pub fn rpop(&mut self) -> Option<Vec<u8>> {
        self.items.pop()
    }

    /// Resolve a possibly-negative index to a real 0-based index, if in range.
    pub fn real_index(&self, i: i64) -> Option<usize> {
        let n = self.items.len() as i64;
        let idx = if i < 0 { n + i } else { i };
        if idx >= 0 && idx < n {
            Some(idx as usize)
        } else {
            None
        }
    }

    /// Normalise a `[start, stop]` inclusive range (Redis semantics) to a
    /// half-open `[lo, hi)` over the current items; empty range -> lo == hi.
    pub fn range_bounds(&self, start: i64, stop: i64) -> (usize, usize) {
        let n = self.items.len() as i64;
        if n == 0 {
            return (0, 0);
        }
        let mut s = if start < 0 { n + start } else { start };
        let mut e = if stop < 0 { n + stop } else { stop };
        if s < 0 {
            s = 0;
        }
        if e >= n {
            e = n - 1;
        }
        if s > e || s >= n {
            return (0, 0);
        }
        (s as usize, (e + 1) as usize)
    }
}

/// O(1) element count off the raw list blob (indexed: header; inline: walk).
pub fn list_len(buf: &[u8]) -> usize {
    match buf.first() {
        None => 0,
        Some(&L_INDEXED) => rd_u32(buf, 1).unwrap_or(0) as usize,
        _ => {
            let mut p = &buf[1..];
            let mut n = 0;
            while take_bytes(&mut p).is_some() {
                n += 1;
            }
            n
        }
    }
}

/// Element at index `i` (0-based, already resolved to be in range) straight off
/// the raw blob. O(1) for the indexed encoding (offset table), O(i) for inline.
pub fn list_get(buf: &[u8], i: usize) -> Option<&[u8]> {
    match buf.first() {
        None => None,
        Some(&L_INDEXED) => {
            let count = rd_u32(buf, 1)? as usize;
            if i >= count {
                return None;
            }
            let data_base = 9 + 4 * count;
            let off = rd_u32(buf, 9 + 4 * i)? as usize;
            let region = buf.get(data_base..)?;
            let mut p = region.get(off..)?;
            take_bytes(&mut p)
        }
        _ => {
            let mut p = &buf[1..];
            let mut idx = 0;
            loop {
                let v = take_bytes(&mut p)?;
                if idx == i {
                    return Some(v);
                }
                idx += 1;
            }
        }
    }
}

// ---- Sorted set -----------------------------------------------------------

/// Format a score the way clients expect: integers without a decimal point,
/// `inf`/`-inf` for infinities, else the shortest string that round-trips.
/// (Redis uses `%.17g`, which prints more digits for inexact doubles; this
/// round-trips identically and is cleaner. Tests use exact scores.)
pub fn fmt_score(s: f64) -> String {
    if s.is_infinite() {
        return if s > 0.0 { "inf".into() } else { "-inf".into() };
    }
    if s == s.trunc() && s.abs() < 1e17 {
        return format!("{}", s as i64);
    }
    format!("{s}")
}

/// Parse a score, accepting `inf`/`+inf`/`-inf`/`infinity`.
pub fn parse_score(b: &[u8]) -> Option<f64> {
    let s = std::str::from_utf8(b).ok()?.trim();
    match s.to_ascii_lowercase().as_str() {
        "inf" | "+inf" | "infinity" | "+infinity" => Some(f64::INFINITY),
        "-inf" | "-infinity" => Some(f64::NEG_INFINITY),
        _ => s.parse::<f64>().ok(),
    }
}

/// One bound of a score range (`ZRANGEBYSCORE`): value + inclusivity. `(` prefix
/// means exclusive; `-inf`/`+inf` are accepted.
pub struct ScoreBound {
    pub value: f64,
    pub inclusive: bool,
}

impl ScoreBound {
    pub fn parse(b: &[u8]) -> Option<ScoreBound> {
        if b.first() == Some(&b'(') {
            Some(ScoreBound {
                value: parse_score(&b[1..])?,
                inclusive: false,
            })
        } else {
            Some(ScoreBound {
                value: parse_score(b)?,
                inclusive: true,
            })
        }
    }
}

/// A Redis sorted set: unique members each with an f64 score. Stored unordered;
/// range/rank ops sort a view by (score, member) — the Redis total order.
#[derive(Default)]
pub struct ZSet {
    pub members: Vec<(Vec<u8>, f64)>,
}

impl ZSet {
    pub fn new() -> ZSet {
        ZSet { members: Vec::new() }
    }

    /// Decode a stored blob (either encoding). Members come back in stored order
    /// (insertion order for inline, sorted order for indexed — callers that need
    /// a specific order re-sort, and range/rank ops read the blob directly).
    pub fn decode(buf: &[u8]) -> ZSet {
        match buf.first() {
            None => ZSet::new(),
            Some(&Z_INDEXED) => {
                let mut z = ZSet::new();
                let count = rd_u32(buf, 1).unwrap_or(0) as usize;
                let nbuckets = rd_u32(buf, 5).unwrap_or(0) as usize;
                let region_base = 9 + 4 * nbuckets + 4 * count;
                if let Some(region) = buf.get(region_base..) {
                    let mut off = 0usize;
                    while off < region.len() {
                        match zread_entry(region, off) {
                            Some((m, s, _next, end)) => {
                                z.members.push((m.to_vec(), s));
                                off = end;
                            }
                            None => break,
                        }
                    }
                }
                z
            }
            _ => {
                let mut p = &buf[1..];
                let mut z = ZSet::new();
                while let Some(m) = take_bytes(&mut p) {
                    if p.len() < 8 {
                        break;
                    }
                    let mut s = [0u8; 8];
                    s.copy_from_slice(&p[..8]);
                    p = &p[8..];
                    z.members.push((m.to_vec(), f64::from_le_bytes(s)));
                }
                z
            }
        }
    }

    /// Encode, choosing inline or indexed by size (first byte is the tag).
    pub fn encode(&self) -> Vec<u8> {
        self.force_encode(self.members.len() > ZSET_INDEX_THRESHOLD)
    }

    /// Encode with an explicit layout choice (tests/benches compare the two).
    /// Indexed: `[Z_INDEXED][count u32][nbuckets u32][buckets: nbuckets×u32]
    /// [sorted: count×u32][entries]`. Each entry is `mlen u32, member, score f64,
    /// next u32` (bucket chain, newest first). `buckets` gives O(1) member→score;
    /// `sorted` holds entry offsets in (score, member) order for O(log n) rank /
    /// range-by-score and O(k) range-by-rank. Entries are written in sorted
    /// order, so `sorted[i]` is the i-th entry's offset.
    pub fn force_encode(&self, indexed: bool) -> Vec<u8> {
        if !indexed {
            let mut out = Vec::with_capacity(1);
            out.push(Z_INLINE);
            for (m, s) in &self.members {
                put_bytes(&mut out, m);
                out.extend_from_slice(&s.to_le_bytes());
            }
            return out;
        }
        let ordered = self.sorted(); // (score, member) ascending
        let count = ordered.len();
        let nbuckets = count.next_power_of_two().max(8);
        let mask = (nbuckets - 1) as u64;
        let mut heads = vec![0u32; nbuckets];
        let mut sorted_offs = Vec::with_capacity(count);
        let mut region: Vec<u8> = Vec::new();
        for (m, s) in &ordered {
            let off = region.len() as u32;
            sorted_offs.push(off);
            let b = (fieldhash(m) & mask) as usize;
            put_bytes(&mut region, m);
            region.extend_from_slice(&s.to_le_bytes());
            region.extend_from_slice(&heads[b].to_le_bytes()); // next = old head
            heads[b] = off + 1;
        }
        let mut out = Vec::with_capacity(9 + 4 * nbuckets + 4 * count + region.len());
        out.push(Z_INDEXED);
        out.extend_from_slice(&(count as u32).to_le_bytes());
        out.extend_from_slice(&(nbuckets as u32).to_le_bytes());
        for h in &heads {
            out.extend_from_slice(&h.to_le_bytes());
        }
        for o in &sorted_offs {
            out.extend_from_slice(&o.to_le_bytes());
        }
        out.extend_from_slice(&region);
        out
    }

    pub fn len(&self) -> usize {
        self.members.len()
    }

    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    pub fn score(&self, member: &[u8]) -> Option<f64> {
        self.members.iter().find(|(m, _)| m == member).map(|(_, s)| *s)
    }

    /// Insert or update. Returns (added, changed).
    pub fn add(&mut self, member: &[u8], score: f64) -> (bool, bool) {
        if let Some(e) = self.members.iter_mut().find(|(m, _)| m == member) {
            let changed = e.1 != score;
            e.1 = score;
            (false, changed)
        } else {
            self.members.push((member.to_vec(), score));
            (true, true)
        }
    }

    pub fn remove(&mut self, member: &[u8]) -> bool {
        if let Some(i) = self.members.iter().position(|(m, _)| m == member) {
            self.members.remove(i);
            true
        } else {
            false
        }
    }

    /// A copy of the members in sorted (score asc, then member bytewise) order.
    pub fn sorted(&self) -> Vec<(Vec<u8>, f64)> {
        let mut v = self.members.clone();
        v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal).then(a.0.cmp(&b.0)));
        v
    }

    /// 0-based rank of a member in ascending order, or None if absent.
    pub fn rank(&self, member: &[u8]) -> Option<usize> {
        self.sorted().iter().position(|(m, _)| m == member)
    }

    /// Members with `min <= score <= max` (honoring exclusivity), sorted asc.
    pub fn by_score(&self, min: &ScoreBound, max: &ScoreBound) -> Vec<(Vec<u8>, f64)> {
        self.sorted()
            .into_iter()
            .filter(|(_, s)| {
                (if min.inclusive { *s >= min.value } else { *s > min.value })
                    && (if max.inclusive { *s <= max.value } else { *s < max.value })
            })
            .collect()
    }
}

// ---- ZSet: indexed-encoding raw readers -----------------------------------

/// Parse the zset entry at `region[off..]`: (member, score, next, end-offset).
fn zread_entry(region: &[u8], off: usize) -> Option<(&[u8], f64, u32, usize)> {
    let mlen = rd_u32(region, off)? as usize;
    let m = region.get(off + 4..off + 4 + mlen)?;
    let sp = off + 4 + mlen;
    let sb = region.get(sp..sp + 8)?;
    let score = f64::from_le_bytes([sb[0], sb[1], sb[2], sb[3], sb[4], sb[5], sb[6], sb[7]]);
    let next = rd_u32(region, sp + 8)?;
    Some((m, score, next, sp + 12))
}

// Layout offsets for an indexed zset blob.
fn z_layout(buf: &[u8]) -> Option<(usize, usize, usize)> {
    let count = rd_u32(buf, 1)? as usize;
    let nbuckets = rd_u32(buf, 5)? as usize;
    let sorted_base = 9 + 4 * nbuckets;
    let region_base = sorted_base + 4 * count;
    Some((count, sorted_base, region_base))
}

/// Total order used by the sorted index: score ascending, then member bytewise.
fn z_less(es: f64, em: &[u8], score: f64, member: &[u8]) -> bool {
    match es.partial_cmp(&score) {
        Some(std::cmp::Ordering::Less) => true,
        Some(std::cmp::Ordering::Greater) => false,
        _ => em < member,
    }
}

/// O(1) cardinality off the raw blob.
pub fn zset_card(buf: &[u8]) -> usize {
    match buf.first() {
        None => 0,
        Some(&Z_INDEXED) => rd_u32(buf, 1).unwrap_or(0) as usize,
        _ => {
            let mut p = &buf[1..];
            let mut n = 0;
            while take_bytes(&mut p).is_some() {
                if p.len() < 8 {
                    break;
                }
                p = &p[8..];
                n += 1;
            }
            n
        }
    }
}

/// O(1)-average member→score off the raw blob (indexed: bucket chain).
pub fn zset_score(buf: &[u8], member: &[u8]) -> Option<f64> {
    match buf.first() {
        None => None,
        Some(&Z_INDEXED) => {
            let nbuckets = rd_u32(buf, 5)? as usize;
            let (_, _, region_base) = z_layout(buf)?;
            let region = buf.get(region_base..)?;
            let b = (fieldhash(member) & (nbuckets as u64 - 1)) as usize;
            let mut head = rd_u32(buf, 9 + 4 * b)?;
            while head != 0 {
                let (m, s, next, _) = zread_entry(region, (head - 1) as usize)?;
                if m == member {
                    return Some(s);
                }
                head = next;
            }
            None
        }
        _ => ZSet::decode(buf).score(member),
    }
}

// Read the (member, score) at sorted position `i` of an indexed blob.
fn z_at<'a>(buf: &[u8], sorted_base: usize, region: &'a [u8], i: usize) -> Option<(&'a [u8], f64)> {
    let off = rd_u32(buf, sorted_base + 4 * i)? as usize;
    zread_entry(region, off).map(|(m, s, _, _)| (m, s))
}

/// 0-based rank (ascending) of `member`, or None if absent. O(log n) indexed.
pub fn zset_rank(buf: &[u8], member: &[u8]) -> Option<usize> {
    match buf.first() {
        Some(&Z_INDEXED) => {
            let score = zset_score(buf, member)?;
            let (count, sorted_base, region_base) = z_layout(buf)?;
            let region = buf.get(region_base..)?;
            // first index whose (score, member) is not less than the target
            let (mut lo, mut hi) = (0usize, count);
            while lo < hi {
                let mid = (lo + hi) / 2;
                let (em, es) = z_at(buf, sorted_base, region, mid)?;
                if z_less(es, em, score, member) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            Some(lo)
        }
        _ => ZSet::decode(buf).rank(member),
    }
}

/// Members in rank range `[lo, hi)` (already normalized), ascending, with scores.
pub fn zset_range_by_rank(buf: &[u8], lo: usize, hi: usize) -> Vec<(Vec<u8>, f64)> {
    match buf.first() {
        Some(&Z_INDEXED) => {
            let mut out = Vec::new();
            if let Some((_, sorted_base, region_base)) = z_layout(buf) {
                if let Some(region) = buf.get(region_base..) {
                    for i in lo..hi {
                        if let Some((m, s)) = z_at(buf, sorted_base, region, i) {
                            out.push((m.to_vec(), s));
                        }
                    }
                }
            }
            out
        }
        _ => {
            let s = ZSet::decode(buf).sorted();
            s.get(lo..hi.min(s.len())).map(|x| x.to_vec()).unwrap_or_default()
        }
    }
}

// Binary search: first sorted index whose score satisfies the lower predicate
// (`pred` is false for a prefix then true) — used for score-range bounds.
fn z_lower_bound<F: Fn(f64) -> bool>(
    buf: &[u8],
    count: usize,
    sorted_base: usize,
    region: &[u8],
    pred: F,
) -> usize {
    let (mut lo, mut hi) = (0usize, count);
    while lo < hi {
        let mid = (lo + hi) / 2;
        match z_at(buf, sorted_base, region, mid) {
            Some((_, es)) if pred(es) => hi = mid,
            _ => lo = mid + 1,
        }
    }
    lo
}

/// Members with score in [min, max] (honoring exclusivity), ascending. O(log n
/// + k) indexed.
pub fn zset_range_by_score(buf: &[u8], min: &ScoreBound, max: &ScoreBound) -> Vec<(Vec<u8>, f64)> {
    match buf.first() {
        Some(&Z_INDEXED) => {
            let (count, sorted_base, region_base) = match z_layout(buf) {
                Some(x) => x,
                None => return Vec::new(),
            };
            let region = match buf.get(region_base..) {
                Some(r) => r,
                None => return Vec::new(),
            };
            let minv = min.value;
            let mininc = min.inclusive;
            let start = z_lower_bound(buf, count, sorted_base, region, |es| {
                if mininc {
                    es >= minv
                } else {
                    es > minv
                }
            });
            let maxv = max.value;
            let maxinc = max.inclusive;
            // first index whose score is beyond the max bound
            let end = z_lower_bound(buf, count, sorted_base, region, |es| {
                if maxinc {
                    es > maxv
                } else {
                    es >= maxv
                }
            });
            let mut out = Vec::new();
            for i in start..end {
                if let Some((m, s)) = z_at(buf, sorted_base, region, i) {
                    out.push((m.to_vec(), s));
                }
            }
            out
        }
        _ => ZSet::decode(buf).by_score(min, max),
    }
}

/// Count of members with score in [min, max]. O(log n) indexed.
pub fn zset_count(buf: &[u8], min: &ScoreBound, max: &ScoreBound) -> usize {
    match buf.first() {
        Some(&Z_INDEXED) => {
            let (count, sorted_base, region_base) = match z_layout(buf) {
                Some(x) => x,
                None => return 0,
            };
            let region = match buf.get(region_base..) {
                Some(r) => r,
                None => return 0,
            };
            let (minv, mininc, maxv, maxinc) = (min.value, min.inclusive, max.value, max.inclusive);
            let start = z_lower_bound(buf, count, sorted_base, region, |es| {
                if mininc { es >= minv } else { es > minv }
            });
            let end = z_lower_bound(buf, count, sorted_base, region, |es| {
                if maxinc { es > maxv } else { es >= maxv }
            });
            end.saturating_sub(start)
        }
        _ => ZSet::decode(buf).by_score(min, max).len(),
    }
}

/// Normalise a `[start, stop]` inclusive rank range over `n` elements to a
/// half-open `[lo, hi)`; empty range -> lo == hi.
pub fn rank_bounds(n: usize, start: i64, stop: i64) -> (usize, usize) {
    let n = n as i64;
    if n == 0 {
        return (0, 0);
    }
    let mut s = if start < 0 { n + start } else { start };
    let mut e = if stop < 0 { n + stop } else { stop };
    if s < 0 {
        s = 0;
    }
    if e >= n {
        e = n - 1;
    }
    if s > e || s >= n {
        return (0, 0);
    }
    (s as usize, (e + 1) as usize)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn zset_add_score_range_rank() {
        let mut z = ZSet::new();
        assert_eq!(z.add(b"a", 1.0), (true, true));
        assert_eq!(z.add(b"b", 3.0), (true, true));
        assert_eq!(z.add(b"c", 2.0), (true, true));
        assert_eq!(z.add(b"a", 1.0), (false, false)); // no change
        assert_eq!(z.add(b"a", 5.0), (false, true)); // changed
        assert_eq!(z.score(b"b"), Some(3.0));
        // sorted by score: c(2), b(3), a(5)
        let s: Vec<_> = z.sorted().into_iter().map(|(m, _)| m).collect();
        assert_eq!(s, vec![b"c".to_vec(), b"b".to_vec(), b"a".to_vec()]);
        assert_eq!(z.rank(b"c"), Some(0));
        assert_eq!(z.rank(b"a"), Some(2));
        assert_eq!(z.rank(b"zz"), None);

        // ties break by member bytewise
        let mut z2 = ZSet::new();
        z2.add(b"y", 1.0);
        z2.add(b"x", 1.0);
        let s2: Vec<_> = z2.sorted().into_iter().map(|(m, _)| m).collect();
        assert_eq!(s2, vec![b"x".to_vec(), b"y".to_vec()]);

        // by_score inclusive/exclusive
        let lo = ScoreBound { value: 3.0, inclusive: true };
        let hi = ScoreBound { value: f64::INFINITY, inclusive: true };
        let r: Vec<_> = z.by_score(&lo, &hi).into_iter().map(|(m, _)| m).collect();
        assert_eq!(r, vec![b"b".to_vec(), b"a".to_vec()]); // 3 and 5
        let lo_ex = ScoreBound { value: 3.0, inclusive: false };
        let r2: Vec<_> = z.by_score(&lo_ex, &hi).into_iter().map(|(m, _)| m).collect();
        assert_eq!(r2, vec![b"a".to_vec()]); // >3 only

        // encode/decode round-trip
        let z3 = ZSet::decode(&z.encode());
        assert_eq!(z3.len(), 3);
        assert_eq!(z3.score(b"a"), Some(5.0));
    }

    #[test]
    fn zset_indexed_matches_inline_at_scale() {
        // 1000 members with many score ties, to exercise (score, member) order.
        let mut z = ZSet::new();
        for i in 0..1000u32 {
            z.add(format!("m{i:04}").as_bytes(), (i % 10) as f64);
        }
        let blob = z.encode();
        assert_eq!(blob[0], Z_INDEXED, "1000 members must use the indexed layout");
        assert_eq!(zset_card(&blob), 1000);

        // ZSCORE for every member matches the inline set
        for i in 0..1000u32 {
            let m = format!("m{i:04}");
            assert_eq!(zset_score(&blob, m.as_bytes()), z.score(m.as_bytes()), "score {m}");
        }
        assert_eq!(zset_score(&blob, b"absent"), None);

        // the whole sorted order (by rank) equals the inline sorted() ground truth
        let want = z.sorted();
        let got = zset_range_by_rank(&blob, 0, 1000);
        assert_eq!(got, want, "full rank order mismatch");

        // ZRANK for a spread of members
        for i in (0..1000u32).step_by(37) {
            let m = format!("m{i:04}");
            assert_eq!(zset_rank(&blob, m.as_bytes()), z.rank(m.as_bytes()), "rank {m}");
        }
        assert_eq!(zset_rank(&blob, b"absent"), None);

        // ZRANGEBYSCORE / ZCOUNT across inclusive & exclusive bounds
        let cases = [
            (ScoreBound { value: 3.0, inclusive: true }, ScoreBound { value: 6.0, inclusive: true }),
            (ScoreBound { value: 3.0, inclusive: false }, ScoreBound { value: 6.0, inclusive: false }),
            (ScoreBound { value: f64::NEG_INFINITY, inclusive: true }, ScoreBound { value: 0.0, inclusive: true }),
            (ScoreBound { value: 9.0, inclusive: true }, ScoreBound { value: f64::INFINITY, inclusive: true }),
        ];
        for (min, max) in &cases {
            assert_eq!(zset_range_by_score(&blob, min, max), z.by_score(min, max), "by_score");
            assert_eq!(zset_count(&blob, min, max), z.by_score(min, max).len(), "count");
        }

        // decode round-trips as a set (same members+scores, order-independent)
        let z2 = ZSet::decode(&blob);
        assert_eq!(z2.len(), 1000);
        let mut a: Vec<_> = z2.members.clone();
        let mut b: Vec<_> = z.members.clone();
        a.sort_by(|x, y| x.0.cmp(&y.0));
        b.sort_by(|x, y| x.0.cmp(&y.0));
        assert_eq!(a, b);
    }

    #[test]
    fn zset_small_stays_inline() {
        let mut z = ZSet::new();
        z.add(b"a", 1.0);
        z.add(b"b", 2.0);
        let blob = z.encode();
        assert_eq!(blob[0], Z_INLINE);
        assert_eq!(zset_card(&blob), 2);
        assert_eq!(zset_score(&blob, b"b"), Some(2.0));
        assert_eq!(zset_rank(&blob, b"b"), Some(1));
    }

    #[test]
    fn score_formatting_and_parse() {
        assert_eq!(fmt_score(1.0), "1");
        assert_eq!(fmt_score(-2.0), "-2");
        assert_eq!(fmt_score(1.5), "1.5");
        assert_eq!(fmt_score(f64::INFINITY), "inf");
        assert_eq!(fmt_score(f64::NEG_INFINITY), "-inf");
        assert_eq!(parse_score(b"3"), Some(3.0));
        assert_eq!(parse_score(b"-inf"), Some(f64::NEG_INFINITY));
        assert_eq!(parse_score(b"+inf"), Some(f64::INFINITY));
        assert!(parse_score(b"abc").is_none());
        assert_eq!(rank_bounds(5, 1, 3), (1, 4));
        assert_eq!(rank_bounds(5, -2, -1), (3, 5));
        assert_eq!(rank_bounds(5, 3, 1), (0, 0));
    }

    #[test]
    fn list_push_pop_range() {
        let mut l = List::new();
        l.rpush(b"a");
        l.rpush(b"b");
        l.lpush(b"z"); // [z, a, b]
        assert_eq!(l.len(), 3);
        assert_eq!(l.items[0], b"z");
        assert_eq!(l.lpop(), Some(b"z".to_vec()));
        assert_eq!(l.rpop(), Some(b"b".to_vec()));
        assert_eq!(l.items, vec![b"a".to_vec()]);

        let mut l2 = List::decode(&{
            let mut m = List::new();
            for x in [b"0", b"1", b"2", b"3", b"4"] {
                m.rpush(x);
            }
            m.encode()
        });
        assert_eq!(l2.real_index(-1), Some(4));
        assert_eq!(l2.real_index(2), Some(2));
        assert_eq!(l2.real_index(5), None);
        assert_eq!(l2.range_bounds(1, 3), (1, 4));
        assert_eq!(l2.range_bounds(-2, -1), (3, 5));
        assert_eq!(l2.range_bounds(3, 1), (0, 0)); // empty
        l2.items.clear();
        assert_eq!(l2.range_bounds(0, -1), (0, 0));
    }

    #[test]
    fn list_small_stays_inline() {
        let mut l = List::new();
        for x in ["a", "b", "c"] {
            l.rpush(x.as_bytes());
        }
        let blob = l.encode();
        assert_eq!(blob[0], L_INLINE);
        assert_eq!(list_len(&blob), 3);
        assert_eq!(list_get(&blob, 0), Some(&b"a"[..]));
        assert_eq!(list_get(&blob, 2), Some(&b"c"[..]));
        assert_eq!(list_get(&blob, 3), None);
    }

    #[test]
    fn list_promotes_to_indexed_and_indexes() {
        let mut l = List::new();
        for i in 0..1000u32 {
            l.rpush(format!("e{i}").as_bytes());
        }
        let blob = l.encode();
        assert_eq!(blob[0], L_INDEXED, "1000 elements must use the indexed layout");
        assert_eq!(list_len(&blob), 1000);
        // O(1) index access agrees with position, at both ends and middle
        assert_eq!(list_get(&blob, 0), Some(&b"e0"[..]));
        assert_eq!(list_get(&blob, 500), Some(&b"e500"[..]));
        assert_eq!(list_get(&blob, 999), Some(&b"e999"[..]));
        assert_eq!(list_get(&blob, 1000), None);
        // full decode round-trips in order
        let l2 = List::decode(&blob);
        assert_eq!(l2.len(), 1000);
        assert_eq!(l2.items[0], b"e0");
        assert_eq!(l2.items[999], b"e999");
        // binary-safe elements
        let mut lb = List::new();
        for _ in 0..200 {
            lb.rpush(b"\x00\xff\x00");
        }
        let bb = lb.encode();
        assert_eq!(bb[0], L_INDEXED);
        assert_eq!(list_get(&bb, 7), Some(&b"\x00\xff\x00"[..]));
    }

    #[test]
    fn hash_roundtrip_and_ops() {
        let mut h = Hash::new();
        assert!(h.set(b"a", b"1"));
        assert!(h.set(b"b", b"22"));
        assert!(!h.set(b"a", b"111")); // overwrite -> not new
        assert_eq!(h.get(b"a"), Some(&b"111"[..]));
        assert_eq!(h.get(b"b"), Some(&b"22"[..]));
        assert_eq!(h.get(b"missing"), None);
        assert_eq!(h.len(), 2);

        let blob = h.encode();
        let h2 = Hash::decode(&blob);
        assert_eq!(h2.len(), 2);
        assert_eq!(h2.get(b"a"), Some(&b"111"[..]));
        assert_eq!(h2.get(b"b"), Some(&b"22"[..]));
        // insertion order preserved
        assert_eq!(h2.entries[0].0, b"a");
        assert_eq!(h2.entries[1].0, b"b");
    }

    #[test]
    fn hash_del_and_empty() {
        let mut h = Hash::decode(&[]);
        assert!(h.is_empty());
        h.set(b"x", b"y");
        assert!(h.del(b"x"));
        assert!(!h.del(b"x"));
        assert!(h.is_empty());
        assert_eq!(Hash::decode(&h.encode()).len(), 0);
    }

    #[test]
    fn hash_binary_safe() {
        let mut h = Hash::new();
        h.set(b"\x00\xff", b"\x01\x02\x00");
        let h2 = Hash::decode(&h.encode());
        assert_eq!(h2.get(b"\x00\xff"), Some(&b"\x01\x02\x00"[..]));
    }

    #[test]
    fn hash_small_stays_inline() {
        let mut h = Hash::new();
        h.set(b"a", b"1");
        h.set(b"b", b"2");
        let blob = h.encode();
        assert_eq!(blob[0], H_INLINE);
        // point read off the raw blob agrees with a full decode
        assert_eq!(hash_probe(&blob, b"a"), Some(&b"1"[..]));
        assert_eq!(hash_probe(&blob, b"b"), Some(&b"2"[..]));
        assert_eq!(hash_probe(&blob, b"z"), None);
        assert_eq!(hash_count(&blob), 2);
    }

    #[test]
    fn hash_promotes_to_indexed_and_probes() {
        let mut h = Hash::new();
        for i in 0..1000u32 {
            h.set(format!("field-{i}").as_bytes(), format!("val-{i}").as_bytes());
        }
        let blob = h.encode();
        assert_eq!(blob[0], H_INDEXED, "1000 fields must use the indexed layout");
        assert_eq!(hash_count(&blob), 1000);

        // every field is found by the O(1) raw probe, with the right value
        for i in 0..1000u32 {
            let want = format!("val-{i}");
            assert_eq!(
                hash_probe(&blob, format!("field-{i}").as_bytes()),
                Some(want.as_bytes()),
                "probe miss for field-{i}"
            );
        }
        assert_eq!(hash_probe(&blob, b"field-1000"), None);
        assert_eq!(hash_probe(&blob, b"absent"), None);

        // full decode round-trips all fields in insertion order
        let h2 = Hash::decode(&blob);
        assert_eq!(h2.len(), 1000);
        assert_eq!(h2.entries[0].0, b"field-0");
        assert_eq!(h2.entries[999].0, b"field-999");
        assert_eq!(h2.get(b"field-500"), Some(&b"val-500"[..]));
    }

    #[test]
    fn hash_promotes_on_big_value() {
        // few fields but a long value -> indexed (linear scan of big members is
        // what the value threshold guards against)
        let mut h = Hash::new();
        h.set(b"k", &vec![b'x'; HASH_INDEX_VALUE_MAX + 1]);
        let blob = h.encode();
        assert_eq!(blob[0], H_INDEXED);
        assert_eq!(hash_probe(&blob, b"k").map(|v| v.len()), Some(HASH_INDEX_VALUE_MAX + 1));
    }

    #[test]
    fn hash_indexed_binary_safe_and_update() {
        let mut h = Hash::new();
        for i in 0..200u32 {
            h.set(&i.to_le_bytes(), b"\x00\xff\x00");
        }
        // overwrite one, delete one, decode-modify-reencode across the threshold
        let blob = h.encode();
        let mut h2 = Hash::decode(&blob);
        assert!(!h2.set(&7u32.to_le_bytes(), b"new")); // existing -> not added
        assert!(h2.del(&9u32.to_le_bytes()));
        let blob2 = h2.encode();
        assert_eq!(hash_probe(&blob2, &7u32.to_le_bytes()), Some(&b"new"[..]));
        assert_eq!(hash_probe(&blob2, &9u32.to_le_bytes()), None);
        assert_eq!(hash_count(&blob2), 199);
    }
}
