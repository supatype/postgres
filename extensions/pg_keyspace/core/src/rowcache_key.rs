//! Mode B row-cache key layout and decode-slot naming.
//!
//! Three things have to agree, byte for byte, on what a cached row is called:
//! the planner hook (which builds a key from a query `Const`), the put/refill
//! path (which builds one from a heap tuple) and the `supacache_keys` output
//! plugin (which builds one from a WAL change). A disagreement is not a miss --
//! it is a row that is cached and then never invalidated, or worse, one
//! database's row served to another.
//!
//! That is not hypothetical. #117 was exactly this bug: keys were `relid ++ pk`
//! with no database in them, and `CREATE DATABASE ... TEMPLATE` copies `pg_class`
//! physically, so two cloned databases had *identical* relids and one was served
//! the other's rows -- under a correct-looking `Custom Scan` plan, with no error,
//! and with RLS unable to help because it is re-applied *above* the cache.
//!
//! It lives in `core/` rather than in the extension for one reason: here it can
//! be unit-tested without a running Postgres. A key-layout bug that can only be
//! caught by standing up a cluster is a key-layout bug that reaches production.

/// Tag byte on a cached ROW entry.
pub const TAG_ROW: u8 = 0x00;
/// Tag byte on a table's REGISTRATION entry.
pub const TAG_REG: u8 = 0xff;
/// Tag byte on the per-database "registrations are loaded" marker.
pub const TAG_LOADED: u8 = 0xfe;

/// A cached row: `TAG_ROW ++ datoid ++ relid ++ canonical pk bytes`.
///
/// The tag byte is not decoration. Without it a row key whose leading bytes
/// happened to match could collide with a registration key -- a latent hazard in
/// the original `relid`-first scheme, and a real one once a database oid sits in
/// front of both.
pub fn row_key(datoid: u32, relid: u32, pk: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(9 + pk.len());
    k.push(TAG_ROW);
    k.extend_from_slice(&datoid.to_le_bytes());
    k.extend_from_slice(&relid.to_le_bytes());
    k.extend_from_slice(pk);
    k
}

/// A table's registration: `TAG_REG ++ datoid ++ relid`. Fixed width, because a
/// registration has no pk part.
pub fn reg_key(datoid: u32, relid: u32) -> [u8; 9] {
    let mut k = [0u8; 9];
    k[0] = TAG_REG;
    k[1..5].copy_from_slice(&datoid.to_le_bytes());
    k[5..].copy_from_slice(&relid.to_le_bytes());
    k
}

/// The marker saying one database's registrations are loaded into the segment
/// currently mapped: `TAG_LOADED ++ datoid`.
///
/// Per-database, and that is load-bearing rather than tidy. As one fixed
/// constant for the whole segment, the first database to finish loading would
/// mark the segment loaded for all of them, and every other database's
/// registrations would never be loaded at all -- their tables silently not
/// cached, with a healthy-looking coherence check.
pub fn loaded_key(datoid: u32) -> [u8; 5] {
    let mut k = [0u8; 5];
    k[0] = TAG_LOADED;
    k[1..].copy_from_slice(&datoid.to_le_bytes());
    k
}

/// Postgres identifier limit (`NAMEDATALEN - 1`), which a replication slot name
/// is held to.
pub const SLOT_NAME_MAX: usize = 63;

/// The decode slot name for one database.
///
/// A logical slot belongs to the database it was created in and only ever
/// decodes changes from that database, so serving more than one database means
/// more than one slot, which means the name has to carry the database.
///
/// The **oid**, not the name. A slot name is limited to `SLOT_NAME_MAX`, and a
/// database name can already fill that on its own, so a name-derived slot would
/// have to be truncated -- and two long database names sharing a prefix would
/// truncate to the SAME slot. Two databases sharing one slot is the #117
/// cross-database failure again, one layer down, and this time it would be the
/// invalidations crossing rather than the rows.
///
/// The stem is truncated instead, on a `char` boundary, and only ever needs to
/// be when an operator has set a very long `pg_keyspace.rowcache_slot`.
pub fn slot_name_for(base: &str, datoid: u32) -> String {
    let suffix = format!("_{datoid}");
    let room = SLOT_NAME_MAX.saturating_sub(suffix.len());
    let mut end = base.len().min(room);
    while end > 0 && !base.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}{}", &base[..end], suffix)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The #117 collision class, asserted directly rather than by standing up
    /// two cloned databases.
    ///
    /// Cloned databases have identical relids, so the database oid is the only
    /// thing separating their keys. Every key this module can produce, over a
    /// grid of databases and relations, must be distinct from every other.
    #[test]
    fn no_two_keys_collide_across_databases_and_relations() {
        let mut seen: Vec<Vec<u8>> = Vec::new();
        for db in [1u32, 5, 16384, 16385, u32::MAX] {
            for relid in [1u32, 16384, 16385, u32::MAX] {
                // The same relid in different databases is the exact shape
                // `CREATE DATABASE ... TEMPLATE` produces.
                for pk in [&b""[..], b"1", b"11", b"a1b2c3"] {
                    seen.push(row_key(db, relid, pk));
                }
                seen.push(reg_key(db, relid).to_vec());
            }
            seen.push(loaded_key(db).to_vec());
        }
        let total = seen.len();
        seen.sort();
        seen.dedup();
        assert_eq!(total, seen.len(), "two distinct inputs produced the same key");
    }

    /// Distinctness is not enough on its own: these are keys in a hash table
    /// that stores raw bytes, so a key that is a *prefix* of another is fine,
    /// but a row key must never be byte-identical to a registration or marker
    /// key for any input at all. The tag byte is what guarantees it, so assert
    /// the tag byte does its job rather than trusting it.
    #[test]
    fn the_three_key_families_never_overlap() {
        for db in [1u32, 16384, u32::MAX] {
            for relid in [1u32, 16384, u32::MAX] {
                let reg = reg_key(db, relid);
                let marker = loaded_key(db);
                assert_ne!(reg[0], marker[0]);
                // A row key with a pk chosen specifically to make the rest of
                // the bytes line up with a registration key.
                let row = row_key(db, relid, &[]);
                assert_eq!(row.len(), reg.len());
                assert_ne!(row.as_slice(), &reg[..]);
                assert_ne!(row.first(), Some(&TAG_LOADED));
            }
        }
    }

    /// A slot name that overflowed `NAMEDATALEN` would be rejected by Postgres
    /// at creation time; one that collided would be worse, because two
    /// databases would share a slot and each would see the other's changes and
    /// none of its own.
    #[test]
    fn slot_names_fit_and_are_unique_per_database() {
        let bases = [
            "supacache_rowcache",
            "s",
            // An operator who set `rowcache_slot` to the longest thing Postgres
            // would accept.
            &"x".repeat(SLOT_NAME_MAX),
        ];
        for base in bases {
            let mut names = Vec::new();
            for db in [1u32, 5, 16384, 16385, 4_000_000_000, u32::MAX] {
                let n = slot_name_for(base, db);
                assert!(
                    n.len() <= SLOT_NAME_MAX,
                    "slot name {n:?} is {} bytes, over the {SLOT_NAME_MAX}-byte limit",
                    n.len()
                );
                assert!(n.ends_with(&format!("_{db}")), "{n:?} does not name db {db}");
                names.push(n);
            }
            let total = names.len();
            names.sort();
            names.dedup();
            assert_eq!(total, names.len(), "two databases share a slot name: {names:?}");
        }
    }

    /// Truncation must not split a multi-byte character, or the name is not
    /// valid UTF-8 text and the `format!` below would panic rather than
    /// truncate. Identifiers are rarely non-ASCII, but a GUC is a free-form
    /// string and this is the cheap way to never find out the hard way.
    #[test]
    fn slot_name_truncation_respects_char_boundaries() {
        let base = "é".repeat(SLOT_NAME_MAX); // 2 bytes each
        let n = slot_name_for(&base, 16384);
        assert!(n.len() <= SLOT_NAME_MAX);
        assert!(n.ends_with("_16384"));
        assert!(n.is_char_boundary(n.len() - "_16384".len()));
    }

    /// The common case must be the obvious string, or every operator runbook
    /// and every `pg_replication_slots` query in a dashboard has to learn a
    /// mangled name.
    #[test]
    fn slot_name_is_the_plain_stem_plus_the_oid() {
        assert_eq!(
            slot_name_for("supacache_rowcache", 16384),
            "supacache_rowcache_16384"
        );
    }
}
