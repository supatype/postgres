//! Minimal RESP2/RESP3 codec — enough for `ioredis`/`go-redis`/`redis-benchmark`
//! to drive pg_keyspace unmodified. Parses the array-of-bulk-strings request form
//! (what all real clients send) plus inline commands (for `redis-cli`/telnet),
//! and provides the reply encoders. The request grammar is identical in both
//! protocols; the RESP3 encoders (`null`/`map_header`/`set_header`/`push_header`/
//! `double`/`boolean`) take the connection's protocol and fall back to the RESP2
//! wire form when it is not on RESP3.

/// Default for the largest bulk string this server will accept, overridden at
/// run time by `pg_keyspace.max_value_bytes`.
///
/// Matches Valkey/Redis `proto-max-bulk-len`, so a client that works against
/// them works here. The practical bound is the store arena, not this: a value
/// still has to fit in shared memory to be served, exactly as it has to fit in
/// `maxmemory` under Valkey. A write the arena cannot hold is refused with the
/// standard OOM error rather than acknowledged.
pub const DEFAULT_MAX_BULK_LEN: usize = 512 * 1024 * 1024;

/// Largest number of arguments in one command. Bounds `args` growth (and the
/// buffering that a huge announced array would otherwise force) so a hostile
/// client cannot exhaust memory with a header alone.
pub const MAX_MULTIBULK_LEN: i64 = 1024 * 1024;

/// Result of trying to parse one request from the front of `buf`.
pub enum Parse {
    /// A full command: `args` holds (start,end) byte ranges into `buf`;
    /// `consumed` bytes should be dropped from the front.
    Complete { consumed: usize },
    /// Need more bytes; caller should read more and retry.
    Incomplete,
    /// Protocol error; caller should close the connection.
    Error,
}

/// Parse one command, appending argument byte-ranges into `args`.
pub fn parse(buf: &[u8], args: &mut Vec<(usize, usize)>, max_bulk: usize) -> Parse {
    args.clear();
    if buf.is_empty() {
        return Parse::Incomplete;
    }
    if buf[0] == b'*' {
        parse_array(buf, args, max_bulk)
    } else {
        parse_inline(buf, args)
    }
}

fn parse_array(buf: &[u8], args: &mut Vec<(usize, usize)>, max_bulk: usize) -> Parse {
    let mut pos = 0;
    let (n, adv) = match read_int_line(&buf[pos..]) {
        Some(x) => x,
        None => return Parse::Incomplete,
    };
    if n < 0 || n > MAX_MULTIBULK_LEN {
        return Parse::Error;
    }
    pos += adv;
    for _ in 0..n {
        if pos >= buf.len() {
            return Parse::Incomplete;
        }
        if buf[pos] != b'$' {
            return Parse::Error;
        }
        let (len, adv) = match read_int_line(&buf[pos..]) {
            Some(x) => x,
            None => return Parse::Incomplete,
        };
        // Reject on the announced length, before buffering the payload: an
        // oversized bulk string must not be accepted (it would desynchronise
        // the persistence ring) and must not be buffered either (announcing a
        // huge length is otherwise a memory-exhaustion vector, since the
        // connection would keep reading until the bytes arrived).
        if len < 0 || len as usize > max_bulk {
            return Parse::Error;
        }
        pos += adv;
        let end = pos + len as usize;
        if end + 2 > buf.len() {
            return Parse::Incomplete;
        }
        if &buf[end..end + 2] != b"\r\n" {
            return Parse::Error;
        }
        args.push((pos, end));
        pos = end + 2;
    }
    Parse::Complete { consumed: pos }
}

fn parse_inline(buf: &[u8], args: &mut Vec<(usize, usize)>) -> Parse {
    let nl = match buf.iter().position(|&c| c == b'\n') {
        Some(i) => i,
        None => return Parse::Incomplete,
    };
    let line_end = if nl > 0 && buf[nl - 1] == b'\r' {
        nl - 1
    } else {
        nl
    };
    let mut i = 0;
    while i < line_end {
        while i < line_end && buf[i] == b' ' {
            i += 1;
        }
        let start = i;
        while i < line_end && buf[i] != b' ' {
            i += 1;
        }
        if i > start {
            args.push((start, i));
        }
    }
    Parse::Complete { consumed: nl + 1 }
}

/// Parse `<prefix><int>\r\n` starting at buf[0] (prefix already at [0]).
/// Returns (value, bytes_consumed_including_crlf).
fn read_int_line(buf: &[u8]) -> Option<(i64, usize)> {
    let nl = buf.iter().position(|&c| c == b'\n')?;
    if nl < 2 || buf[nl - 1] != b'\r' {
        return None;
    }
    let mut val: i64 = 0;
    let mut neg = false;
    let mut i = 1; // skip prefix byte
    if i < nl - 1 && buf[i] == b'-' {
        neg = true;
        i += 1;
    }
    while i < nl - 1 {
        let c = buf[i];
        if !c.is_ascii_digit() {
            return None;
        }
        val = val * 10 + (c - b'0') as i64;
        i += 1;
    }
    Some((if neg { -val } else { val }, nl + 1))
}

// ---- reply encoders (append into an output buffer) -----------------------

pub fn simple(out: &mut Vec<u8>, s: &str) {
    out.push(b'+');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn error(out: &mut Vec<u8>, s: &str) {
    out.push(b'-');
    out.extend_from_slice(s.as_bytes());
    out.extend_from_slice(b"\r\n");
}

pub fn integer(out: &mut Vec<u8>, n: i64) {
    out.push(b':');
    write_int(out, n);
    out.extend_from_slice(b"\r\n");
}

pub fn bulk(out: &mut Vec<u8>, b: &[u8]) {
    out.push(b'$');
    write_int(out, b.len() as i64);
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(b);
    out.extend_from_slice(b"\r\n");
}

pub fn nil(out: &mut Vec<u8>) {
    out.extend_from_slice(b"$-1\r\n");
}

/// A null array (`*-1`), distinct from an empty array — used by EXEC when a
/// WATCHed key changed, so the transaction is aborted.
pub fn nil_array(out: &mut Vec<u8>) {
    out.extend_from_slice(b"*-1\r\n");
}

// ---- RESP3 ----------------------------------------------------------------
// RESP2 and RESP3 share bulk/integer/simple/error/array verbatim; only these
// reply shapes differ. Each takes the connection's protocol so one call site
// serves both: on a RESP2 connection they fall back to the RESP2 wire form
// (which is what every client understood before HELLO 3).

/// Null: RESP3 `_`, RESP2 `$-1` (both accepted by lenient clients).
pub fn null(out: &mut Vec<u8>, resp3: bool) {
    if resp3 {
        out.extend_from_slice(b"_\r\n");
    } else {
        out.extend_from_slice(b"$-1\r\n");
    }
}

/// Map header of `pairs` field/value pairs: RESP3 `%pairs`, RESP2 `*2*pairs`
/// (a flat array). The caller emits 2×`pairs` elements either way.
pub fn map_header(out: &mut Vec<u8>, pairs: usize, resp3: bool) {
    if resp3 {
        out.push(b'%');
        write_int(out, pairs as i64);
        out.extend_from_slice(b"\r\n");
    } else {
        array_header(out, pairs * 2);
    }
}

/// Set header of `n` members: RESP3 `~n`, RESP2 `*n`.
pub fn set_header(out: &mut Vec<u8>, n: usize, resp3: bool) {
    if resp3 {
        out.push(b'~');
        write_int(out, n as i64);
        out.extend_from_slice(b"\r\n");
    } else {
        array_header(out, n);
    }
}

/// Push header of `n` elements (out-of-band: pub/sub messages, invalidations):
/// RESP3 `>n`, RESP2 `*n`.
pub fn push_header(out: &mut Vec<u8>, n: usize, resp3: bool) {
    if resp3 {
        out.push(b'>');
        write_int(out, n as i64);
        out.extend_from_slice(b"\r\n");
    } else {
        array_header(out, n);
    }
}

/// A double: RESP3 `,<v>` (with `inf`/`-inf`/`nan`), RESP2 a bulk string. `text`
/// is the already-formatted number (e.g. `aggr::fmt_score`).
pub fn double(out: &mut Vec<u8>, text: &str, resp3: bool) {
    if resp3 {
        out.push(b',');
        out.extend_from_slice(text.as_bytes());
        out.extend_from_slice(b"\r\n");
    } else {
        bulk(out, text.as_bytes());
    }
}

/// A boolean: RESP3 `#t`/`#f`, RESP2 integer `:1`/`:0`.
pub fn boolean(out: &mut Vec<u8>, v: bool, resp3: bool) {
    if resp3 {
        out.extend_from_slice(if v { b"#t\r\n" } else { b"#f\r\n" });
    } else {
        integer(out, if v { 1 } else { 0 });
    }
}

pub fn array_header(out: &mut Vec<u8>, n: usize) {
    out.push(b'*');
    write_int(out, n as i64);
    out.extend_from_slice(b"\r\n");
}

fn write_int(out: &mut Vec<u8>, n: i64) {
    let mut buf = [0u8; 20];
    let mut i = buf.len();
    let neg = n < 0;
    let mut u = if neg {
        (n as i128).unsigned_abs() as u64
    } else {
        n as u64
    };
    if u == 0 {
        out.push(b'0');
        return;
    }
    while u > 0 {
        i -= 1;
        buf[i] = b'0' + (u % 10) as u8;
        u /= 10;
    }
    if neg {
        i -= 1;
        buf[i] = b'-';
    }
    out.extend_from_slice(&buf[i..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(out: &[u8]) -> String {
        String::from_utf8_lossy(out).into_owned()
    }

    #[test]
    fn resp3_types_differ_from_resp2() {
        // null
        let (mut a, mut b) = (Vec::new(), Vec::new());
        null(&mut a, true);
        null(&mut b, false);
        assert_eq!(s(&a), "_\r\n");
        assert_eq!(s(&b), "$-1\r\n");

        // map header: RESP3 %n, RESP2 flat *2n
        let (mut a, mut b) = (Vec::new(), Vec::new());
        map_header(&mut a, 2, true);
        map_header(&mut b, 2, false);
        assert_eq!(s(&a), "%2\r\n");
        assert_eq!(s(&b), "*4\r\n");

        // set header: RESP3 ~n, RESP2 *n
        let (mut a, mut b) = (Vec::new(), Vec::new());
        set_header(&mut a, 3, true);
        set_header(&mut b, 3, false);
        assert_eq!(s(&a), "~3\r\n");
        assert_eq!(s(&b), "*3\r\n");

        // push header: RESP3 >n, RESP2 *n
        let (mut a, mut b) = (Vec::new(), Vec::new());
        push_header(&mut a, 2, true);
        push_header(&mut b, 2, false);
        assert_eq!(s(&a), ">2\r\n");
        assert_eq!(s(&b), "*2\r\n");

        // double: RESP3 ,v, RESP2 bulk string
        let (mut a, mut b) = (Vec::new(), Vec::new());
        double(&mut a, "1.5", true);
        double(&mut b, "1.5", false);
        assert_eq!(s(&a), ",1.5\r\n");
        assert_eq!(s(&b), "$3\r\n1.5\r\n");

        // boolean: RESP3 #t/#f, RESP2 :1/:0
        let (mut a, mut b) = (Vec::new(), Vec::new());
        boolean(&mut a, true, true);
        boolean(&mut b, true, false);
        assert_eq!(s(&a), "#t\r\n");
        assert_eq!(s(&b), ":1\r\n");
    }

    /// An announced bulk length over the cap is rejected on the header, before
    /// the payload is buffered. Rejecting late would still be a memory
    /// exhaustion vector, because the connection would keep reading until the
    /// announced bytes arrived; rejecting by truncation would desynchronise
    /// the persistence ring (see `ring::MAX_REC_VAL`).
    #[test]
    fn oversized_bulk_length_is_rejected_on_the_header() {
        let mut args = Vec::new();
        // Announce one byte over the cap, and send no payload at all: the
        // decision must not depend on having the bytes.
        let over = DEFAULT_MAX_BULK_LEN + 1;
        let req = format!("*2\r\n$3\r\nSET\r\n${over}\r\n");
        assert!(
            matches!(parse(req.as_bytes(), &mut args, DEFAULT_MAX_BULK_LEN), Parse::Error),
            "a bulk length of {over} must be refused without buffering it"
        );

        // At the cap it is accepted as a well-formed header and simply needs
        // more bytes, so the limit is inclusive and off-by-one free.
        let at = DEFAULT_MAX_BULK_LEN;
        let req = format!("*2\r\n$3\r\nSET\r\n${at}\r\n");
        assert!(matches!(parse(req.as_bytes(), &mut args, DEFAULT_MAX_BULK_LEN), Parse::Incomplete));
    }

    /// A huge announced argument count is refused too, so `args` growth and
    /// the buffering it would force are both bounded.
    #[test]
    fn oversized_multibulk_count_is_rejected() {
        let mut args = Vec::new();
        let over = MAX_MULTIBULK_LEN + 1;
        let req = format!("*{over}\r\n");
        assert!(matches!(parse(req.as_bytes(), &mut args, DEFAULT_MAX_BULK_LEN), Parse::Error));
    }

    /// Ordinary commands are unaffected by the caps.
    #[test]
    fn normal_command_still_parses_under_the_caps() {
        let mut args = Vec::new();
        match parse(
            b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$2\r\nhi\r\n",
            &mut args,
            DEFAULT_MAX_BULK_LEN,
        ) {
            Parse::Complete { consumed } => {
                // *3\r\n $3\r\nSET\r\n $1\r\nk\r\n $2\r\nhi\r\n
                assert_eq!(consumed, 28);
                assert_eq!(args.len(), 3);
            }
            _ => panic!("a normal SET must still parse"),
        }
    }
}
