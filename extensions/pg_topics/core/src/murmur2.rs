const SEED: i32 = 0x9747b28cu32 as i32;
const M: i32 = 0x5bd1e995u32 as i32;
const R: u32 = 24;

fn logical_shr(v: i32, n: u32) -> i32 {
    ((v as u32) >> n) as i32
}

pub fn hash(data: &[u8]) -> i32 {
    let length = data.len();
    let mut h = SEED ^ (length as i32);
    let chunk_count = length / 4;

    for i in 0..chunk_count {
        let base = i * 4;
        let mut k = (data[base] as i32 & 0xff)
            | ((data[base + 1] as i32 & 0xff) << 8)
            | ((data[base + 2] as i32 & 0xff) << 16)
            | ((data[base + 3] as i32 & 0xff) << 24);
        k = k.wrapping_mul(M);
        k ^= logical_shr(k, R);
        k = k.wrapping_mul(M);
        h = h.wrapping_mul(M);
        h ^= k;
    }

    let tail = chunk_count * 4;
    let remainder = length % 4;
    if remainder == 3 {
        h ^= (data[tail + 2] as i32 & 0xff) << 16;
    }
    if remainder >= 2 {
        h ^= (data[tail + 1] as i32 & 0xff) << 8;
    }
    if remainder >= 1 {
        h ^= data[tail] as i32 & 0xff;
        h = h.wrapping_mul(M);
    }

    h ^= logical_shr(h, 13);
    h = h.wrapping_mul(M);
    h ^= logical_shr(h, 15);
    h
}

pub fn to_positive(h: i32) -> i32 {
    h & 0x7fffffff
}

pub fn band_for(key: &[u8], band_count: u32) -> u32 {
    (to_positive(hash(key)) as u32) % band_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn band_for_matches_the_kafka_reference_pair() {
        assert_eq!(band_for(b"bottle-1", 4), 2);
    }

    #[test]
    fn band_for_matches_the_second_reference_pair() {
        assert_eq!(band_for(b"a", 1024), 636);
    }
}
