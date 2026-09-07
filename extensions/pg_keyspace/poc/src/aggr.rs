//! P3 aggregate value types (§5): hashes, lists, sorted sets.
//!
//! Each aggregate is stored in the keyspace as a single self-describing blob in
//! the slab (the entry's `kind` tags which type it is), decoded on read and
//! re-encoded on write. This matches Redis's small-collection philosophy
//! (listpack) — compact and simple, O(n) per op — and is a good fit for a cache
//! whose collections are small. Very large collections would want a native
//! shmem structure; that is a later upgrade, not a correctness issue.
//!
//! Encoding is length-prefixed and endian-fixed so a persisted blob is portable:
//! each element is `len: u32-le` followed by `len` bytes.

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

    /// Decode a stored blob. A malformed tail is ignored (returns what parsed),
    /// so a truncated blob degrades to a shorter hash rather than a panic.
    pub fn decode(mut buf: &[u8]) -> Hash {
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

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for (f, v) in &self.entries {
            put_bytes(&mut out, f);
            put_bytes(&mut out, v);
        }
        out
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

    pub fn decode(mut buf: &[u8]) -> List {
        let mut l = List::new();
        while let Some(v) = take_bytes(&mut buf) {
            l.items.push(v.to_vec());
        }
        l
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for v in &self.items {
            put_bytes(&mut out, v);
        }
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

#[cfg(test)]
mod tests {
    use super::*;

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
}
