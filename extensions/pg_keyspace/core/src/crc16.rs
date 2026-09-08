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
}
