//! Minimal RESP2 codec — enough for `ioredis`/`go-redis`/`redis-benchmark` to
//! drive the POC unmodified (§5). Parses the array-of-bulk-strings request form
//! (what all real clients send) plus inline commands (for `redis-cli`/telnet),
//! and provides the reply encoders.

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
pub fn parse(buf: &[u8], args: &mut Vec<(usize, usize)>) -> Parse {
    args.clear();
    if buf.is_empty() {
        return Parse::Incomplete;
    }
    if buf[0] == b'*' {
        parse_array(buf, args)
    } else {
        parse_inline(buf, args)
    }
}

fn parse_array(buf: &[u8], args: &mut Vec<(usize, usize)>) -> Parse {
    let mut pos = 0;
    let (n, adv) = match read_int_line(&buf[pos..]) {
        Some(x) => x,
        None => return Parse::Incomplete,
    };
    if n < 0 {
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
        if len < 0 {
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
