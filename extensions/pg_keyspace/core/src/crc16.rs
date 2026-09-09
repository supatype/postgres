//! CRC16 (CCITT, XMODEM) — the exact function Redis/Valkey Cluster uses to map
//! a key to one of 16384 hash slots. Reproduced here so that a stock
//! cluster-aware client (ioredis, go-redis, lettuce) routes a key to the same
//! slot pg_keyspace assigns it to. Validates: "Each [worker] owns a disjoint
//! slot range of the 16384-slot CRC16 keyspace."

pub const NUM_SLOTS: u16 = 16384;

const CRC16_TAB: [u16; 256] = build_table();

const fn build_table() -> [u16; 256] {
    let mut tab = [0u16; 256];
    let mut i = 0usize;
    while i < 256 {
        let mut crc = (i as u16) << 8;
        let mut j = 0;
        while j < 8 {
            if crc & 0x8000 != 0 {
                crc = (crc << 1) ^ 0x1021;
            } else {
                crc <<= 1;
            }
            j += 1;
        }
        tab[i] = crc;
        i += 1;
    }
    tab
}

fn crc16(bytes: &[u8]) -> u16 {
    let mut crc: u16 = 0;
    for &b in bytes {
        crc = (crc << 8) ^ CRC16_TAB[(((crc >> 8) ^ b as u16) & 0xff) as usize];
    }
    crc
}

/// Redis-compatible key -> slot, honouring `{hashtag}` semantics so that
/// `{user:1}:session` and `{user:1}:token` land on the same worker.
pub fn key_slot(key: &[u8]) -> u16 {
    let sub = hashtag(key);
    crc16(sub) % NUM_SLOTS
}

/// Which of `n` shared-nothing slot workers owns `slot`.
///
/// The 16384 slots are split into `n` contiguous, disjoint ranges — the same
/// shape a Redis Cluster client expects from `CLUSTER SLOTS`, so a cluster-aware
/// client that routes by slot range reaches the worker that actually holds the
/// key. This is the single definition of ownership: the RESP slot workers, the
/// SQL surface, and crash recovery all route through it, so a key persisted by
/// one worker is recovered into that same worker's segment.
pub fn worker_for_slot(slot: u16, n: usize) -> usize {
    if n <= 1 {
        return 0;
    }
    // Exact inverse of `slot_range`, whose bounds are floor(w * NUM_SLOTS / n):
    // the owner is the largest `w` with floor(w * NUM_SLOTS / n) <= slot, which
    // is floor(((slot + 1) * n - 1) / NUM_SLOTS). It cannot reach `n` because
    // `slot < NUM_SLOTS`. (Plain floor(slot * n / NUM_SLOTS) is off by one at
    // range boundaries when `n` does not divide NUM_SLOTS.)
    ((slot as usize + 1) * n - 1) / NUM_SLOTS as usize
}

/// The half-open slot range `[lo, hi)` owned by worker `w` of `n`.
///
/// Ranges are contiguous and partition the whole 16384-slot space with no gaps
/// and no overlaps; sizes differ by at most one slot when `n` does not divide
/// 16384 evenly.
pub fn slot_range(w: usize, n: usize) -> (u16, u16) {
    if n <= 1 {
        return (0, NUM_SLOTS);
    }
    let n_slots = NUM_SLOTS as usize;
    let lo = (w * n_slots) / n;
    let hi = ((w + 1) * n_slots) / n;
    (lo as u16, hi.min(n_slots) as u16)
}

/// Which of `n` slot workers owns `key` (honouring `{hashtag}` semantics, so a
/// hashtag group is never split across workers).
pub fn key_owner(key: &[u8], n: usize) -> usize {
    worker_for_slot(key_slot(key), n)
}

fn hashtag(key: &[u8]) -> &[u8] {
    if let Some(open) = key.iter().position(|&c| c == b'{') {
        if let Some(rel_close) = key[open + 1..].iter().position(|&c| c == b'}') {
            if rel_close > 0 {
                return &key[open + 1..open + 1 + rel_close];
            }
        }
    }
    key
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        // Cross-checked against redis-cli CLUSTER KEYSLOT.
        assert_eq!(key_slot(b"foo"), 12182);
        assert_eq!(key_slot(b"123456789"), 0x31c3);
        assert_eq!(key_slot(b"{user:1}:session"), key_slot(b"{user:1}:token"));
    }

    #[test]
    fn slot_ranges_partition_the_keyspace() {
        for n in 1..=16usize {
            // Ranges are contiguous, non-empty, and cover [0, NUM_SLOTS) exactly.
            let mut next = 0u16;
            for w in 0..n {
                let (lo, hi) = slot_range(w, n);
                assert_eq!(lo, next, "gap/overlap at worker {w} of {n}");
                assert!(hi > lo, "empty range for worker {w} of {n}");
                next = hi;
            }
            assert_eq!(next, NUM_SLOTS, "ranges must cover every slot for n={n}");
        }
    }

    #[test]
    fn worker_for_slot_inverts_slot_range() {
        for n in 1..=16usize {
            for slot in 0..NUM_SLOTS {
                let w = worker_for_slot(slot, n);
                assert!(w < n, "owner {w} out of range for n={n}");
                let (lo, hi) = slot_range(w, n);
                assert!(
                    slot >= lo && slot < hi,
                    "slot {slot} owned by {w} but outside its range [{lo},{hi}) for n={n}"
                );
            }
        }
    }

    #[test]
    fn hashtag_groups_stay_on_one_worker() {
        for n in [2usize, 3, 4, 8] {
            assert_eq!(
                key_owner(b"{user:1}:session", n),
                key_owner(b"{user:1}:token", n)
            );
        }
    }
}
