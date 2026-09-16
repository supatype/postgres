//! Bitmap commands — `SETBIT`, `GETBIT`, `BITCOUNT`, `BITPOS`, `BITOP`,
//! `BITFIELD` and `BITFIELD_RO`.
//!
//! A bitmap is not a type. It is a string addressed by bit rather than by byte,
//! so everything here operates on a plain byte buffer that the caller has loaded
//! from — and stores back into — the keyspace as an ordinary `KIND_STR` value.
//! Persistence, crash recovery, expiry, replication and tenant scoping are
//! therefore exactly what strings already do, and nothing in this module knows
//! about any of them. What it owns is the arithmetic.
//!
//! Bit numbering is Redis's: bit 0 is the **most** significant bit of byte 0, so
//! `SETBIT k 0 1` produces `0x80` and not `0x01`. Every offset below is in that
//! space, and the byte holding bit `n` is `n >> 3`.

pub const ERR_BIT_OFFSET: &str = "ERR bit offset is not an integer or out of range";
pub const ERR_BIT_VALUE: &str = "ERR bit is not an integer or out of range";
pub const ERR_NOT_INT: &str = "ERR value is not an integer or out of range";
pub const ERR_SYNTAX: &str = "ERR syntax error";
pub const ERR_BIT_ARG: &str = "ERR The bit argument must be 1 or 0.";
pub const ERR_BITOP_NOT: &str = "ERR BITOP NOT must be called with a single source key.";
pub const ERR_BITFIELD_TYPE: &str = "ERR Invalid bitfield type. Use something like i16 u8. \
                                     Note that u64 is not supported but i64 is.";
pub const ERR_OVERFLOW_TYPE: &str = "ERR Invalid OVERFLOW type specified";
pub const ERR_BITFIELD_RO: &str = "ERR BITFIELD_RO only supports the GET subcommand";

/// Whether a `BITCOUNT`/`BITPOS` range counts in bytes (the default) or bits.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Unit {
    Byte,
    Bit,
}

/// `BYTE` / `BIT`, case-insensitively; `None` for anything else (a syntax error
/// at the call site).
pub fn parse_unit(arg: &[u8]) -> Option<Unit> {
    if arg.eq_ignore_ascii_case(b"BYTE") {
        Some(Unit::Byte)
    } else if arg.eq_ignore_ascii_case(b"BIT") {
        Some(Unit::Bit)
    } else {
        None
    }
}

/// Parse an integer argument the way Redis's `string2ll` does: an optional `-`,
/// then digits, with no leading `+`, no leading zeros (`08` is not 8), no
/// surrounding whitespace and no overflow.
///
/// Stricter than `str::parse`, deliberately. A stock client that sends `SETBIT k
/// 08 1` is told the offset is invalid by a real Redis, and has to be told the
/// same here — accepting it would set a bit the client never asked to set.
pub fn parse_int(arg: &[u8]) -> Option<i64> {
    let (neg, digits) = match arg.split_first() {
        Some((b'-', rest)) => (true, rest),
        _ => (false, arg),
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return None;
    }
    // "0" alone is the only string that may start with a zero, and "-0" is not
    // one of them.
    if digits[0] == b'0' && (digits.len() > 1 || neg) {
        return None;
    }
    std::str::from_utf8(arg).ok()?.parse::<i64>().ok()
}

/// Parse a bit offset for `SETBIT`/`GETBIT`/`BITFIELD`.
///
/// Offsets past `max_bytes` are refused rather than allocated. The caller passes
/// `pg_keyspace.max_value_bytes` — the same bound the RESP parser applies to an
/// inbound bulk string — which is what stops `SETBIT k 34359738367 1` from
/// asking the arena for half a gigabyte of zeros.
///
/// `width` is the number of bits the operation will touch at that offset (1 for
/// `SETBIT`/`GETBIT`), so a `BITFIELD u64`-sized read at the very top of the
/// permitted range is refused rather than silently reaching past it. `hash`
/// enables `BITFIELD`'s `#`-prefixed form, where the offset is counted in
/// fields of `width` bits instead of in bits.
pub fn bit_offset(
    arg: &[u8],
    max_bytes: usize,
    width: u32,
    hash: bool,
) -> Result<u64, &'static str> {
    let text = std::str::from_utf8(arg).map_err(|_| ERR_BIT_OFFSET)?;
    let (text, usehash) = match text.strip_prefix('#') {
        Some(rest) if hash => (rest, true),
        Some(_) => return Err(ERR_BIT_OFFSET),
        None => (text, false),
    };
    let raw = parse_int(text.as_bytes()).ok_or(ERR_BIT_OFFSET)?;
    if raw < 0 {
        return Err(ERR_BIT_OFFSET);
    }
    let off = if usehash {
        (raw as u64).checked_mul(width as u64).ok_or(ERR_BIT_OFFSET)?
    } else {
        raw as u64
    };
    // The last byte the operation touches must stay inside the bound, which for
    // a multi-bit field is `width - 1` bits past the offset.
    let last_byte = (off >> 3).checked_add(((width.max(1) - 1) / 8) as u64).ok_or(ERR_BIT_OFFSET)?;
    if last_byte >= max_bytes as u64 {
        return Err(ERR_BIT_OFFSET);
    }
    Ok(off)
}

/// The bit at `off`, zero for anything past the end of the value.
pub fn get_bit(buf: &[u8], off: u64) -> i64 {
    let byte = (off >> 3) as usize;
    match buf.get(byte) {
        Some(b) => ((b >> (7 - (off & 7))) & 1) as i64,
        None => 0,
    }
}

/// Set the bit at `off`, zero-extending `buf` to reach it, and return its
/// previous value.
pub fn set_bit(buf: &mut Vec<u8>, off: u64, bit: bool) -> i64 {
    let byte = (off >> 3) as usize;
    if buf.len() <= byte {
        buf.resize(byte + 1, 0);
    }
    let mask = 1u8 << (7 - (off & 7));
    let old = i64::from(buf[byte] & mask != 0);
    if bit {
        buf[byte] |= mask;
    } else {
        buf[byte] &= !mask;
    }
    old
}

/// Resolve an inclusive `[start, end]` range against a value of `len` bytes,
/// returning the absolute bit indices it covers, or `None` if it is empty.
///
/// Negative indices count from the end. The `start < 0 && end < 0 && start >
/// end` case is checked before normalisation, exactly as Redis does: after
/// normalisation those two would compare as a non-empty range.
pub fn resolve_range(len: usize, start: i64, end: i64, unit: Unit) -> Option<(u64, u64)> {
    if len == 0 {
        return None;
    }
    let total = match unit {
        Unit::Byte => len as i64,
        Unit::Bit => (len as i64) * 8,
    };
    if start < 0 && end < 0 && start > end {
        return None;
    }
    let mut s = if start < 0 { total + start } else { start };
    let mut e = if end < 0 { total + end } else { end };
    if s < 0 {
        s = 0;
    }
    if e < 0 {
        e = 0;
    }
    if e >= total {
        e = total - 1;
    }
    if s > e {
        return None;
    }
    Some(match unit {
        Unit::Byte => ((s as u64) * 8, (e as u64) * 8 + 7),
        Unit::Bit => (s as u64, e as u64),
    })
}

/// Bits `from..=to` within one byte, counted MSB-first (bit 0 is `0x80`).
fn byte_mask(from: u64, to: u64) -> u8 {
    let mut m = 0u8;
    for i in from..=to {
        m |= 0x80 >> i;
    }
    m
}

/// Population count over an inclusive bit range, as returned by
/// [`resolve_range`] — so `None` is an empty range and counts nothing. The whole
/// value is the range `0..=-1`, which is how the caller expresses `BITCOUNT`
/// with no range at all.
pub fn count(buf: &[u8], range: Option<(u64, u64)>) -> i64 {
    let (first, last) = match range {
        None => return 0,
        Some(r) => r,
    };
    let fb = (first >> 3) as usize;
    if fb >= buf.len() {
        return 0;
    }
    let lb = ((last >> 3) as usize).min(buf.len() - 1);
    if fb == lb {
        // A range inside one byte: mask both ends at once. `last` may point past
        // the value, in which case the mask runs to the end of the byte.
        let hi = if (last >> 3) as usize > fb { 7 } else { last & 7 };
        return (buf[fb] & byte_mask(first & 7, hi)).count_ones() as i64;
    }
    let mut total = (buf[fb] & byte_mask(first & 7, 7)).count_ones();
    for b in &buf[fb + 1..lb] {
        total += b.count_ones();
    }
    // The final byte is whole unless `last` stops inside it.
    let hi = if (last >> 3) as usize > lb { 7 } else { last & 7 };
    total += (buf[lb] & byte_mask(0, hi)).count_ones();
    total as i64
}

/// First bit equal to `bit` within an inclusive bit range, as an absolute bit
/// index. `None` means the range holds no such bit.
pub fn pos(buf: &[u8], bit: bool, first: u64, last: u64) -> Option<u64> {
    let fb = (first >> 3) as usize;
    if fb >= buf.len() {
        return None;
    }
    let lb = ((last >> 3) as usize).min(buf.len() - 1);
    for b in fb..=lb {
        let lo = if b == fb { first & 7 } else { 0 };
        let hi = if b == lb && ((last >> 3) as usize) == lb { last & 7 } else { 7 };
        let mask = byte_mask(lo, hi);
        // Looking for a clear bit is the same search over the complement, with
        // the mask keeping bits outside the range from matching either way.
        let v = if bit { buf[b] & mask } else { !buf[b] & mask };
        if v != 0 {
            return Some((b as u64) * 8 + v.leading_zeros() as u64);
        }
    }
    None
}

// ---- BITOP ---------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

pub fn parse_bitop(arg: &[u8]) -> Option<BitOp> {
    if arg.eq_ignore_ascii_case(b"AND") {
        Some(BitOp::And)
    } else if arg.eq_ignore_ascii_case(b"OR") {
        Some(BitOp::Or)
    } else if arg.eq_ignore_ascii_case(b"XOR") {
        Some(BitOp::Xor)
    } else if arg.eq_ignore_ascii_case(b"NOT") {
        Some(BitOp::Not)
    } else {
        None
    }
}

/// Combine `srcs` bitwise. The result is as long as the longest source; shorter
/// ones (and missing keys, which the caller passes as empty) are zero-extended,
/// which is what makes `AND` with a missing key produce zeros rather than the
/// other operand.
pub fn apply_bitop(op: BitOp, srcs: &[Vec<u8>]) -> Vec<u8> {
    let len = srcs.iter().map(|s| s.len()).max().unwrap_or(0);
    if len == 0 {
        return Vec::new();
    }
    if op == BitOp::Not {
        return srcs[0].iter().map(|b| !b).collect();
    }
    let mut out = vec![0u8; len];
    for i in 0..len {
        let mut acc = *srcs[0].get(i).unwrap_or(&0);
        for s in &srcs[1..] {
            let b = *s.get(i).unwrap_or(&0);
            acc = match op {
                BitOp::And => acc & b,
                BitOp::Or => acc | b,
                BitOp::Xor => acc ^ b,
                BitOp::Not => unreachable!("NOT takes a single source"),
            };
        }
        out[i] = acc;
    }
    out
}

// ---- BITFIELD ------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Overflow {
    Wrap,
    Sat,
    Fail,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum FieldOp {
    Get,
    Set(i64),
    Incr(i64),
}

/// One `GET`/`SET`/`INCRBY` of a `BITFIELD`, with the `OVERFLOW` mode in force
/// where it appears (the mode applies to the operations that follow it, so it is
/// resolved at parse time and carried per operation).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub struct Field {
    pub op: FieldOp,
    pub signed: bool,
    pub bits: u32,
    pub offset: u64,
    pub overflow: Overflow,
}

impl Field {
    pub fn is_write(&self) -> bool {
        !matches!(self.op, FieldOp::Get)
    }
    /// One past the last byte this operation touches.
    pub fn end_byte(&self) -> u64 {
        (self.offset + self.bits as u64 - 1) / 8 + 1
    }
}

/// `i8`, `u16`, … → `(signed, bits)`. `u64` is deliberately absent: its range
/// does not fit the integer reply type, which is what Redis's error text says.
fn parse_encoding(arg: &[u8]) -> Result<(bool, u32), &'static str> {
    let (signed, rest) = match arg.split_first() {
        Some((b'i', rest)) => (true, rest),
        Some((b'u', rest)) => (false, rest),
        _ => return Err(ERR_BITFIELD_TYPE),
    };
    let bits: u32 = std::str::from_utf8(rest)
        .ok()
        .and_then(|t| t.parse().ok())
        .ok_or(ERR_BITFIELD_TYPE)?;
    if bits == 0 || (signed && bits > 64) || (!signed && bits > 63) {
        return Err(ERR_BITFIELD_TYPE);
    }
    Ok((signed, bits))
}

/// Parse a whole `BITFIELD` argument list (`args[0]` is the command, `args[1]`
/// the key). Every operation is parsed before any is applied, so a syntax error
/// in the last one leaves the value untouched.
pub fn parse_bitfield(
    args: &[Vec<u8>],
    max_bytes: usize,
    readonly: bool,
) -> Result<Vec<Field>, &'static str> {
    let mut ops = Vec::new();
    let mut overflow = Overflow::Wrap;
    let mut j = 2;
    while j < args.len() {
        let word = args[j].to_ascii_uppercase();
        if word == b"OVERFLOW" {
            // Allowed in BITFIELD_RO: only SET/INCRBY are refused there, and an
            // OVERFLOW that governs nothing is harmless.
            let arg = args.get(j + 1).ok_or(ERR_SYNTAX)?;
            overflow = if arg.eq_ignore_ascii_case(b"WRAP") {
                Overflow::Wrap
            } else if arg.eq_ignore_ascii_case(b"SAT") {
                Overflow::Sat
            } else if arg.eq_ignore_ascii_case(b"FAIL") {
                Overflow::Fail
            } else {
                return Err(ERR_OVERFLOW_TYPE);
            };
            j += 2;
            continue;
        }
        let takes_value = match word.as_slice() {
            b"GET" => false,
            b"SET" | b"INCRBY" => {
                if readonly {
                    return Err(ERR_BITFIELD_RO);
                }
                true
            }
            _ => return Err(ERR_SYNTAX),
        };
        let needed = if takes_value { 4 } else { 3 };
        if j + needed > args.len() {
            return Err(ERR_SYNTAX);
        }
        let (signed, bits) = parse_encoding(&args[j + 1])?;
        let offset = bit_offset(&args[j + 2], max_bytes, bits, true)?;
        let op = if takes_value {
            let v = parse_int(&args[j + 3]).ok_or(ERR_NOT_INT)?;
            if word == b"SET" {
                FieldOp::Set(v)
            } else {
                FieldOp::Incr(v)
            }
        } else {
            FieldOp::Get
        };
        ops.push(Field { op, signed, bits, offset, overflow });
        j += needed;
    }
    Ok(ops)
}

/// Bits `[offset, offset+bits)` read MSB-first, zero for anything past the end.
fn get_unsigned(buf: &[u8], offset: u64, bits: u32) -> u64 {
    let mut v = 0u64;
    for i in 0..bits as u64 {
        v = (v << 1) | get_bit(buf, offset + i) as u64;
    }
    v
}

fn get_signed(buf: &[u8], offset: u64, bits: u32) -> i64 {
    let v = get_unsigned(buf, offset, bits);
    // Sign-extend from the field's own width.
    if bits < 64 && v & (1u64 << (bits - 1)) != 0 {
        (v | (u64::MAX << bits)) as i64
    } else {
        v as i64
    }
}

/// Write `bits` of `value` MSB-first at `offset`. The caller has already grown
/// `buf` to cover the field.
fn set_unsigned(buf: &mut [u8], offset: u64, bits: u32, value: u64) {
    for i in 0..bits as u64 {
        let bit = (value >> (bits as u64 - 1 - i)) & 1;
        let byte = ((offset + i) >> 3) as usize;
        let mask = 1u8 << (7 - ((offset + i) & 7));
        if bit != 0 {
            buf[byte] |= mask;
        } else {
            buf[byte] &= !mask;
        }
    }
}

/// Overflow check for an unsigned field, ported from Redis's
/// `checkUnsignedBitfieldOverflow` so the wrap arithmetic agrees bit for bit.
/// Returns whether `value + incr` overflows the field, and the value to store
/// instead under `WRAP`/`SAT`.
fn check_unsigned(value: u64, incr: i64, bits: u32, ow: Overflow) -> (bool, u64) {
    let max: u64 = if bits == 64 { u64::MAX } else { (1u64 << bits) - 1 };
    // Deliberately wrapping, as in the C: these are only read after the range
    // test below has established that they are meaningful.
    let maxincr = max.wrapping_sub(value) as i64;
    let minincr = (0u64.wrapping_sub(value)) as i64;

    let over = if value > max || (incr > 0 && incr > maxincr) {
        Some(max)
    } else if incr < 0 && incr < minincr {
        Some(0)
    } else {
        return (false, 0);
    };
    let limit = match ow {
        Overflow::Sat => over.unwrap(),
        // WRAP (and FAIL, whose value the caller discards): keep the low `bits`.
        _ => {
            let res = value.wrapping_add(incr as u64);
            if bits == 64 {
                res
            } else {
                res & !(u64::MAX << bits)
            }
        }
    };
    (true, limit)
}

/// Overflow check for a signed field, ported from Redis's
/// `checkSignedBitfieldOverflow`.
fn check_signed(value: i64, incr: i64, bits: u32, ow: Overflow) -> (bool, i64) {
    let max: i64 = if bits == 64 { i64::MAX } else { (1i64 << (bits - 1)) - 1 };
    let min: i64 = (-max) - 1;
    let maxincr = max.wrapping_sub(value);
    let minincr = min.wrapping_sub(value);

    let sat = if value > max
        || (bits != 64 && incr > maxincr)
        || (value >= 0 && incr > 0 && incr > maxincr)
    {
        max
    } else if value < min
        || (bits != 64 && incr < minincr)
        || (value < 0 && incr < 0 && incr < minincr)
    {
        min
    } else {
        return (false, 0);
    };
    let limit = match ow {
        Overflow::Sat => sat,
        _ => {
            let mut c = (value as u64).wrapping_add(incr as u64);
            if bits < 64 {
                let msb = 1u64 << (bits - 1);
                if c & msb != 0 {
                    c |= u64::MAX << bits;
                } else {
                    c &= (1u64 << bits) - 1;
                }
            }
            c as i64
        }
    };
    (true, limit)
}

/// Apply parsed `BITFIELD` operations in order, growing `buf` as the writes
/// require. Each operation contributes one reply: the value read for `GET`, the
/// *previous* value for `SET`, the *new* value for `INCRBY`, or `None` where
/// `OVERFLOW FAIL` refused the write.
pub fn apply_bitfield(buf: &mut Vec<u8>, ops: &[Field]) -> Vec<Option<i64>> {
    // One extension up front, to the farthest byte any write reaches — so a
    // failed write still creates the key, and a GET of a higher offset than any
    // write still reads zeros rather than extending anything.
    if let Some(end) = ops.iter().filter(|o| o.is_write()).map(|o| o.end_byte()).max() {
        if (buf.len() as u64) < end {
            buf.resize(end as usize, 0);
        }
    }
    let mut out = Vec::with_capacity(ops.len());
    for f in ops {
        match f.op {
            FieldOp::Get => out.push(Some(if f.signed {
                get_signed(buf, f.offset, f.bits)
            } else {
                get_unsigned(buf, f.offset, f.bits) as i64
            })),
            FieldOp::Set(v) | FieldOp::Incr(v) => {
                let incrby = matches!(f.op, FieldOp::Incr(_));
                let (overflowed, newval, retval) = if f.signed {
                    let old = get_signed(buf, f.offset, f.bits);
                    if incrby {
                        let (o, wrapped) = check_signed(old, v, f.bits, f.overflow);
                        let n = if o { wrapped } else { old.wrapping_add(v) };
                        (o, n as u64, n)
                    } else {
                        let (o, wrapped) = check_signed(v, 0, f.bits, f.overflow);
                        let n = if o { wrapped } else { v };
                        (o, n as u64, old)
                    }
                } else {
                    let old = get_unsigned(buf, f.offset, f.bits);
                    if incrby {
                        let (o, wrapped) = check_unsigned(old, v, f.bits, f.overflow);
                        let n = if o { wrapped } else { old.wrapping_add(v as u64) };
                        (o, n, n as i64)
                    } else {
                        let (o, wrapped) = check_unsigned(v as u64, 0, f.bits, f.overflow);
                        let n = if o { wrapped } else { v as u64 };
                        (o, n, old as i64)
                    }
                };
                if overflowed && f.overflow == Overflow::Fail {
                    out.push(None);
                } else {
                    set_unsigned(buf, f.offset, f.bits, newval);
                    out.push(Some(retval));
                }
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    const MAX: usize = 512 * 1024 * 1024;

    fn bits(buf: &[u8]) -> String {
        buf.iter().map(|b| format!("{b:08b}")).collect()
    }

    // ---- SETBIT / GETBIT -------------------------------------------------

    #[test]
    fn setbit_is_msb_first() {
        let mut v = Vec::new();
        assert_eq!(set_bit(&mut v, 0, true), 0);
        assert_eq!(v, vec![0x80]);
        assert_eq!(set_bit(&mut v, 7, true), 0);
        assert_eq!(v, vec![0x81]);
    }

    #[test]
    fn setbit_returns_the_previous_bit() {
        let mut v = Vec::new();
        assert_eq!(set_bit(&mut v, 3, true), 0);
        assert_eq!(set_bit(&mut v, 3, true), 1);
        assert_eq!(set_bit(&mut v, 3, false), 1);
        assert_eq!(set_bit(&mut v, 3, false), 0);
    }

    #[test]
    fn setbit_zero_pads_the_gap() {
        let mut v = Vec::new();
        set_bit(&mut v, 100, true);
        assert_eq!(v.len(), 13);
        assert_eq!(v[..12], [0u8; 12]);
        assert_eq!(get_bit(&v, 100), 1);
    }

    #[test]
    fn getbit_past_the_end_is_zero() {
        assert_eq!(get_bit(&[0xff], 7), 1);
        assert_eq!(get_bit(&[0xff], 8), 0);
        assert_eq!(get_bit(&[], 0), 0);
    }

    #[test]
    fn offsets_are_bounded_by_max_value_bytes() {
        assert_eq!(bit_offset(b"0", MAX, 1, false), Ok(0));
        // The last addressable bit, and the first that is not.
        assert_eq!(bit_offset(b"4294967295", MAX, 1, false), Ok(4294967295));
        assert_eq!(bit_offset(b"4294967296", MAX, 1, false), Err(ERR_BIT_OFFSET));
        assert_eq!(bit_offset(b"-1", MAX, 1, false), Err(ERR_BIT_OFFSET));
        assert_eq!(bit_offset(b"nope", MAX, 1, false), Err(ERR_BIT_OFFSET));
        // Offsets go through the strict parse too: `08` is not 8.
        assert_eq!(bit_offset(b"08", MAX, 1, false), Err(ERR_BIT_OFFSET));
        assert_eq!(bit_offset(b"+1", MAX, 1, false), Err(ERR_BIT_OFFSET));
        // A wide field must fit entirely inside the bound: the last byte an
        // i64 at this offset would touch is one past it.
        assert_eq!(bit_offset(b"4294967240", MAX, 64, false), Err(ERR_BIT_OFFSET));
        assert_eq!(bit_offset(b"4294967232", MAX, 64, false), Ok(4294967232));
        // `#` is BITFIELD-only, and multiplies by the field width.
        assert_eq!(bit_offset(b"#2", MAX, 8, true), Ok(16));
        assert_eq!(bit_offset(b"#2", MAX, 8, false), Err(ERR_BIT_OFFSET));
    }

    // ---- BITCOUNT --------------------------------------------------------

    #[test]
    fn integers_parse_as_redis_parses_them() {
        assert_eq!(parse_int(b"0"), Some(0));
        assert_eq!(parse_int(b"-1"), Some(-1));
        assert_eq!(parse_int(b"9223372036854775807"), Some(i64::MAX));
        assert_eq!(parse_int(b"-9223372036854775808"), Some(i64::MIN));
        // Everything a real Redis refuses.
        assert_eq!(parse_int(b""), None);
        assert_eq!(parse_int(b"+1"), None);
        assert_eq!(parse_int(b"08"), None);
        assert_eq!(parse_int(b"-0"), None);
        assert_eq!(parse_int(b"-"), None);
        assert_eq!(parse_int(b" 1"), None);
        assert_eq!(parse_int(b"1 "), None);
        assert_eq!(parse_int(b"1x"), None);
        assert_eq!(parse_int(b"9223372036854775808"), None);
    }

    #[test]
    fn count_whole_value() {
        let all = |v: &[u8]| count(v, resolve_range(v.len(), 0, -1, Unit::Byte));
        assert_eq!(all(b"foobar"), 26);
        assert_eq!(all(b""), 0);
        // An empty range counts nothing — it is not "the whole value".
        assert_eq!(count(b"foobar", None), 0);
    }

    #[test]
    fn count_byte_ranges() {
        let v = b"foobar";
        let r = |s, e| count(v, resolve_range(v.len(), s, e, Unit::Byte));
        assert_eq!(r(0, 0), 4);
        assert_eq!(r(1, 1), 6);
        assert_eq!(r(0, 5), 26);
        assert_eq!(r(0, -5), 10);
        assert_eq!(r(-2, -1), 7);
        // start past the end, and an inverted range
        assert_eq!(r(6, 9), 0);
        assert_eq!(r(3, 1), 0);
    }

    #[test]
    fn count_bit_ranges() {
        let v = b"foobar";
        let r = |s, e| count(v, resolve_range(v.len(), s, e, Unit::Bit));
        assert_eq!(r(0, 0), 0);
        assert_eq!(r(5, 30), 17);
        assert_eq!(r(0, -5), 25);
        assert_eq!(r(-8, -1), 4);
    }

    #[test]
    fn count_range_inside_one_byte() {
        // 0b1010_1010: bits 0,2,4,6 set.
        let v = [0xaa];
        assert_eq!(count(&v, resolve_range(1, 0, 3, Unit::Bit)), 2);
        assert_eq!(count(&v, resolve_range(1, 1, 1, Unit::Bit)), 0);
        assert_eq!(count(&v, resolve_range(1, 0, 0, Unit::Bit)), 1);
    }

    #[test]
    fn empty_ranges_resolve_to_none() {
        assert_eq!(resolve_range(0, 0, -1, Unit::Byte), None);
        // Both negative and inverted: empty before normalisation would hide it.
        assert_eq!(resolve_range(6, -1, -2, Unit::Byte), None);
        assert_eq!(resolve_range(6, 4, 2, Unit::Byte), None);
    }

    // ---- BITPOS ----------------------------------------------------------

    #[test]
    fn pos_finds_the_first_matching_bit() {
        // 0x00 0xff 0xf0
        let v = [0x00, 0xff, 0xf0];
        assert_eq!(pos(&v, true, 0, 23), Some(8));
        assert_eq!(pos(&v, false, 0, 23), Some(0));
        assert_eq!(pos(&v, false, 8, 23), Some(20));
    }

    #[test]
    fn pos_respects_the_range_edges() {
        let v = [0xff, 0xf0];
        // Looking for a clear bit inside an all-ones range finds nothing.
        assert_eq!(pos(&v, false, 0, 7), None);
        // A range ending mid-byte must not match a bit past its end.
        assert_eq!(pos(&v, false, 8, 11), None);
        assert_eq!(pos(&v, false, 8, 12), Some(12));
        // A range starting mid-byte must not match a bit before its start.
        assert_eq!(pos(&[0x40], true, 0, 7), Some(1));
        assert_eq!(pos(&[0x40], true, 2, 7), None);
    }

    #[test]
    fn pos_past_the_value_finds_nothing() {
        assert_eq!(pos(&[0xff], true, 8, 15), None);
        assert_eq!(pos(&[], true, 0, 7), None);
    }

    // ---- BITOP -----------------------------------------------------------

    #[test]
    fn bitop_combines_to_the_longest_source() {
        let a = b"abc".to_vec();
        let b = b"abcdef".to_vec();
        assert_eq!(apply_bitop(BitOp::Or, &[a.clone(), b.clone()]).len(), 6);
        // AND zero-extends the short operand, so the tail is zeroed.
        let and = apply_bitop(BitOp::And, &[a.clone(), b.clone()]);
        assert_eq!(and.len(), 6);
        assert_eq!(&and[3..], &[0, 0, 0]);
        // XOR of a value with itself is all zeros, of the declared length.
        assert_eq!(apply_bitop(BitOp::Xor, &[a.clone(), a.clone()]), vec![0, 0, 0]);
    }

    #[test]
    fn bitop_not_complements_one_source() {
        assert_eq!(apply_bitop(BitOp::Not, &[vec![0x00, 0xff]]), vec![0xff, 0x00]);
        assert!(apply_bitop(BitOp::Not, &[Vec::new()]).is_empty());
    }

    #[test]
    fn bitop_over_missing_keys_is_empty() {
        assert!(apply_bitop(BitOp::And, &[Vec::new(), Vec::new()]).is_empty());
        assert!(apply_bitop(BitOp::Or, &[]).is_empty());
    }

    // ---- BITFIELD --------------------------------------------------------

    fn field(args: &[&str]) -> Vec<Field> {
        let owned: Vec<Vec<u8>> = std::iter::once("BITFIELD")
            .chain(std::iter::once("k"))
            .chain(args.iter().copied())
            .map(|s| s.as_bytes().to_vec())
            .collect();
        parse_bitfield(&owned, MAX, false).expect("parses")
    }

    fn field_err(args: &[&str]) -> &'static str {
        let owned: Vec<Vec<u8>> = std::iter::once("BITFIELD")
            .chain(std::iter::once("k"))
            .chain(args.iter().copied())
            .map(|s| s.as_bytes().to_vec())
            .collect();
        parse_bitfield(&owned, MAX, false).expect_err("rejects")
    }

    #[test]
    fn bitfield_encodings() {
        assert_eq!(parse_encoding(b"i8"), Ok((true, 8)));
        assert_eq!(parse_encoding(b"u63"), Ok((false, 63)));
        assert_eq!(parse_encoding(b"i64"), Ok((true, 64)));
        // u64 does not fit the integer reply, i65 does not exist, and a bare
        // width has no sign.
        assert_eq!(parse_encoding(b"u64"), Err(ERR_BITFIELD_TYPE));
        assert_eq!(parse_encoding(b"i65"), Err(ERR_BITFIELD_TYPE));
        assert_eq!(parse_encoding(b"u0"), Err(ERR_BITFIELD_TYPE));
        assert_eq!(parse_encoding(b"8"), Err(ERR_BITFIELD_TYPE));
    }

    #[test]
    fn bitfield_parse_errors() {
        assert_eq!(field_err(&["GET", "u8"]), ERR_SYNTAX);
        assert_eq!(field_err(&["BOGUS", "u8", "0"]), ERR_SYNTAX);
        assert_eq!(field_err(&["OVERFLOW", "NOPE"]), ERR_OVERFLOW_TYPE);
        assert_eq!(field_err(&["SET", "u8", "0", "notanint"]), ERR_NOT_INT);
        assert_eq!(field_err(&["GET", "u8", "-1"]), ERR_BIT_OFFSET);
    }

    #[test]
    fn bitfield_overflow_applies_to_later_ops_only() {
        let ops = field(&["SET", "u8", "0", "1", "OVERFLOW", "SAT", "SET", "u8", "0", "2"]);
        assert_eq!(ops[0].overflow, Overflow::Wrap);
        assert_eq!(ops[1].overflow, Overflow::Sat);
    }

    #[test]
    fn bitfield_readonly_rejects_writes() {
        // OVERFLOW is not a write, so BITFIELD_RO accepts it.
        let args: Vec<Vec<u8>> = ["BITFIELD_RO", "k", "OVERFLOW", "SAT", "GET", "u8", "0"]
            .iter()
            .map(|s| s.as_bytes().to_vec())
            .collect();
        assert_eq!(parse_bitfield(&args, MAX, true).map(|o| o.len()), Ok(1));
        let args: Vec<Vec<u8>> = ["BITFIELD_RO", "k", "SET", "u8", "0", "1"]
            .iter()
            .map(|s| s.as_bytes().to_vec())
            .collect();
        assert_eq!(parse_bitfield(&args, MAX, true), Err(ERR_BITFIELD_RO));
        let args: Vec<Vec<u8>> = ["BITFIELD_RO", "k", "GET", "u8", "0"]
            .iter()
            .map(|s| s.as_bytes().to_vec())
            .collect();
        assert_eq!(parse_bitfield(&args, MAX, true).map(|o| o.len()), Ok(1));
    }

    #[test]
    fn bitfield_set_returns_the_old_value() {
        let mut v = Vec::new();
        let r = apply_bitfield(&mut v, &field(&["SET", "u8", "0", "255"]));
        assert_eq!(r, vec![Some(0)]);
        let r = apply_bitfield(&mut v, &field(&["SET", "u8", "0", "1"]));
        assert_eq!(r, vec![Some(255)]);
        assert_eq!(v, vec![1]);
    }

    #[test]
    fn bitfield_incrby_returns_the_new_value() {
        let mut v = Vec::new();
        let r = apply_bitfield(&mut v, &field(&["INCRBY", "u8", "0", "10"]));
        assert_eq!(r, vec![Some(10)]);
        let r = apply_bitfield(&mut v, &field(&["INCRBY", "u8", "0", "-4"]));
        assert_eq!(r, vec![Some(6)]);
    }

    #[test]
    fn bitfield_get_does_not_extend() {
        let mut v = Vec::new();
        let r = apply_bitfield(&mut v, &field(&["GET", "u8", "800"]));
        assert_eq!(r, vec![Some(0)]);
        assert!(v.is_empty());
    }

    #[test]
    fn bitfield_write_extends_to_the_farthest_field() {
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "u8", "16", "1"]));
        assert_eq!(v.len(), 3);
    }

    #[test]
    fn bitfield_unsigned_overflow_modes() {
        let mut v = Vec::new();
        // WRAP is the default: 255 + 10 wraps to 9.
        apply_bitfield(&mut v, &field(&["SET", "u8", "0", "255"]));
        assert_eq!(apply_bitfield(&mut v, &field(&["INCRBY", "u8", "0", "10"])), vec![Some(9)]);

        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "u8", "0", "250"]));
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "SAT", "INCRBY", "u8", "0", "10"]));
        assert_eq!(r, vec![Some(255)]);
        // And down: saturates at zero rather than wrapping past it.
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "SAT", "INCRBY", "u8", "0", "-300"]));
        assert_eq!(r, vec![Some(0)]);

        let mut v = vec![255u8];
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "FAIL", "INCRBY", "u8", "0", "10"]));
        assert_eq!(r, vec![None]);
        assert_eq!(v, vec![255], "a failed write leaves the value alone");
    }

    #[test]
    fn bitfield_signed_overflow_modes() {
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "i8", "0", "127"]));
        assert_eq!(apply_bitfield(&mut v, &field(&["INCRBY", "i8", "0", "1"])), vec![Some(-128)]);

        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "i8", "0", "127"]));
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "SAT", "INCRBY", "i8", "0", "10"]));
        assert_eq!(r, vec![Some(127)]);
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "SAT", "INCRBY", "i8", "0", "-300"]));
        assert_eq!(r, vec![Some(-128)]);
    }

    #[test]
    fn bitfield_set_out_of_range_value_wraps() {
        // 300 does not fit u8; WRAP keeps the low 8 bits.
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "u8", "0", "300"]));
        assert_eq!(v, vec![44]);
        // SAT clamps instead.
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["OVERFLOW", "SAT", "SET", "u8", "0", "300"]));
        assert_eq!(v, vec![255]);
        // FAIL writes nothing but still created the key.
        let mut v = Vec::new();
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "FAIL", "SET", "u8", "0", "300"]));
        assert_eq!(r, vec![None]);
        assert_eq!(v, vec![0]);
    }

    #[test]
    fn bitfield_signs_extend_on_read() {
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "i4", "0", "-1"]));
        assert_eq!(bits(&v), "11110000");
        assert_eq!(apply_bitfield(&mut v, &field(&["GET", "i4", "0"])), vec![Some(-1)]);
        assert_eq!(apply_bitfield(&mut v, &field(&["GET", "u4", "0"])), vec![Some(15)]);
    }

    #[test]
    fn bitfield_fields_straddle_byte_boundaries() {
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "u16", "4", "65535"]));
        assert_eq!(bits(&v), "000011111111111111110000");
        assert_eq!(apply_bitfield(&mut v, &field(&["GET", "u16", "4"])), vec![Some(65535)]);
    }

    #[test]
    fn bitfield_i64_round_trips_its_extremes() {
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "i64", "0", &i64::MIN.to_string()]));
        assert_eq!(apply_bitfield(&mut v, &field(&["GET", "i64", "0"])), vec![Some(i64::MIN)]);
        apply_bitfield(&mut v, &field(&["SET", "i64", "0", &i64::MAX.to_string()]));
        assert_eq!(apply_bitfield(&mut v, &field(&["GET", "i64", "0"])), vec![Some(i64::MAX)]);
        // And the extremes overflow in the direction you would expect.
        let r = apply_bitfield(&mut v, &field(&["OVERFLOW", "SAT", "INCRBY", "i64", "0", "1"]));
        assert_eq!(r, vec![Some(i64::MAX)]);
    }

    #[test]
    fn bitfield_hash_offsets_index_by_field() {
        let mut v = Vec::new();
        apply_bitfield(&mut v, &field(&["SET", "u8", "#2", "7"]));
        assert_eq!(v, vec![0, 0, 7]);
    }

    #[test]
    fn bitfield_ops_apply_in_order() {
        let mut v = Vec::new();
        let r = apply_bitfield(
            &mut v,
            &field(&["SET", "u8", "0", "5", "GET", "u8", "0", "INCRBY", "u8", "0", "1", "GET", "u8", "0"]),
        );
        assert_eq!(r, vec![Some(0), Some(5), Some(6), Some(6)]);
    }
}
