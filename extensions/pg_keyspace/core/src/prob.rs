use crate::resp;
use crate::store::{now_micros, Store, KIND_BLOOM, KIND_CUCKOO};

const TAG: u8 = 1;
const OFF_ERROR: usize = 1;
const OFF_EXPANSION: usize = 9;
const OFF_FLAGS: usize = 13;
const OFF_ITEMS: usize = 17;
const OFF_FILTERS: usize = 25;
const HDR: usize = 29;
const SUB_HDR: usize = 28;
const FLAG_NONSCALING: u32 = 1;

const DEFAULT_CAPACITY: u64 = 100;
const DEFAULT_ERROR: f64 = 0.01;
const DEFAULT_EXPANSION: u32 = 2;
const MAX_CAPACITY: u64 = 1 << 30;
const MAX_FILTERS: usize = 32;
const MAX_HASHES: u32 = 64;
const MAX_BLOB_BYTES: usize = 512 * 1024 * 1024;
const CHUNK_BYTES: usize = 16 * 1024 * 1024;
const TIGHTEN: f64 = 0.5;

const WRONGTYPE: &str = "WRONGTYPE Operation against a key holding the wrong kind of value";
const NOT_FOUND: &str = "ERR not found";
const ITEM_EXISTS: &str = "ERR item exists";
const BAD_ERROR_RATE: &str = "ERR bad error rate";
const BAD_CAPACITY: &str = "ERR bad capacity";
const BAD_EXPANSION: &str = "ERR bad expansion";
const ERROR_RANGE: &str = "ERR error rate must be in the range (0.000000, 1.000000)";
const CAPACITY_RANGE: &str = "ERR capacity must be in the range [1, 1073741824]";
const NONSCALING_EXPAND: &str = "Nonscaling filters cannot expand";
const INVALID_INFO: &str = "Invalid information value";
const INSERT_BAD_CAPACITY: &str = "Bad capacity";
const INSERT_BAD_ERROR: &str = "Bad error rate";
const INSERT_BAD_EXPANSION: &str = "Bad expansion";
const UNKNOWN_ARG: &str = "Unknown argument received";
const FILTER_FULL: &str = "ERR non scaling filter is full";
const SCANDUMP_NUMERIC: &str = "Second argument must be numeric";
const LOADCHUNK_NUMERIC: &str = "ERR Second argument must be numeric";
const BAD_DATA: &str = "ERR received bad data";

pub fn type_name(kind: u32) -> Option<&'static str> {
    match kind {
        KIND_BLOOM => Some("MBbloom--"),
        KIND_CUCKOO => Some("MBbloomCF"),
        _ => None,
    }
}

pub fn encoding_name(kind: u32) -> Option<&'static str> {
    match kind {
        KIND_BLOOM | KIND_CUCKOO => Some("raw"),
        _ => None,
    }
}

pub fn dispatch(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    match cmd {
        b"BF.RESERVE" => reserve(store, cmd, args, out),
        b"BF.ADD" => add(store, cmd, args, out, resp3),
        b"BF.MADD" => madd(store, cmd, args, out, resp3),
        b"BF.INSERT" => insert(store, cmd, args, out, resp3),
        b"BF.EXISTS" => exists(store, cmd, args, out, resp3),
        b"BF.MEXISTS" => mexists(store, cmd, args, out, resp3),
        b"BF.INFO" => info(store, cmd, args, out, resp3),
        b"BF.CARD" => card(store, cmd, args, out),
        b"BF.SCANDUMP" => scandump(store, cmd, args, out),
        b"BF.LOADCHUNK" => loadchunk(store, cmd, args, out),
        other => resp::error(
            out,
            &format!("ERR unknown command '{}'", String::from_utf8_lossy(other)),
        ),
    }
}

fn arity(out: &mut Vec<u8>, cmd: &[u8]) {
    resp::error(
        out,
        &format!(
            "ERR wrong number of arguments for '{}' command",
            String::from_utf8_lossy(cmd).to_lowercase()
        ),
    );
}

fn remaining_ttl(exp: i64) -> i64 {
    if exp > 0 {
        (exp - now_micros()).max(1)
    } else {
        0
    }
}

fn arg_f64(b: &[u8]) -> Option<f64> {
    std::str::from_utf8(b)
        .ok()?
        .parse::<f64>()
        .ok()
        .filter(|v| v.is_finite())
}

fn arg_i64(b: &[u8]) -> Option<i64> {
    std::str::from_utf8(b).ok()?.parse::<i64>().ok()
}

fn eq(a: &[u8], b: &str) -> bool {
    a.eq_ignore_ascii_case(b.as_bytes())
}

fn rd_u32(b: &[u8], at: usize) -> Option<u32> {
    b.get(at..at + 4)
        .map(|s| u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn rd_u64(b: &[u8], at: usize) -> Option<u64> {
    b.get(at..at + 8)
        .map(|s| u64::from_le_bytes([s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7]]))
}

fn rd_f64(b: &[u8], at: usize) -> Option<f64> {
    rd_u64(b, at).map(f64::from_bits)
}

const MURMUR_M: u64 = 0xc6a4a793_5bd1e995;
const MURMUR_R: u32 = 47;
const MURMUR_SEED: u64 = 0xc6a4a793_5bd1e995;

fn murmur64a(data: &[u8], seed: u64) -> u64 {
    let mut h = seed ^ (data.len() as u64).wrapping_mul(MURMUR_M);
    let blocks = data.len() / 8;
    for i in 0..blocks {
        let mut k = u64::from_le_bytes(data[i * 8..i * 8 + 8].try_into().unwrap());
        k = k.wrapping_mul(MURMUR_M);
        k ^= k >> MURMUR_R;
        k = k.wrapping_mul(MURMUR_M);
        h ^= k;
        h = h.wrapping_mul(MURMUR_M);
    }
    let tail = &data[blocks * 8..];
    if !tail.is_empty() {
        let mut t: u64 = 0;
        for (i, &b) in tail.iter().enumerate() {
            t |= (b as u64) << (8 * i);
        }
        h ^= t;
        h = h.wrapping_mul(MURMUR_M);
    }
    h ^= h >> MURMUR_R;
    h = h.wrapping_mul(MURMUR_M);
    h ^= h >> MURMUR_R;
    h
}

fn item_hashes(item: &[u8]) -> (u64, u64) {
    let h1 = murmur64a(item, MURMUR_SEED);
    (h1, murmur64a(item, h1))
}

fn bits_per_item(error: f64) -> f64 {
    -error.ln() / (std::f64::consts::LN_2 * std::f64::consts::LN_2)
}

fn hash_count(bpe: f64) -> u32 {
    ((bpe * std::f64::consts::LN_2).ceil() as u32).clamp(1, MAX_HASHES)
}

fn bit_count(capacity: u64, bpe: f64) -> u64 {
    let n = (capacity as f64 * bpe).ceil() as u64;
    n.div_ceil(64).max(1) * 64
}

struct Sub {
    capacity: u64,
    items: u64,
    bits: u64,
    hashes: u32,
    hdr: usize,
    bitmap: usize,
}

struct Filter {
    error: f64,
    expansion: u32,
    flags: u32,
    items: u64,
    subs: Vec<Sub>,
}

impl Filter {
    fn nonscaling(&self) -> bool {
        self.flags & FLAG_NONSCALING != 0
    }

    fn capacity(&self) -> u64 {
        self.subs.iter().map(|s| s.capacity).sum()
    }
}

enum Shape {
    Ok(Filter),
    Partial,
    Bad,
}

fn shape(blob: &[u8]) -> Shape {
    match blob.first() {
        None => return Shape::Partial,
        Some(&TAG) => {}
        Some(_) => return Shape::Bad,
    }
    if blob.len() < HDR {
        return Shape::Partial;
    }
    let error = rd_f64(blob, OFF_ERROR).unwrap_or(f64::NAN);
    let expansion = rd_u32(blob, OFF_EXPANSION).unwrap_or(0);
    let flags = rd_u32(blob, OFF_FLAGS).unwrap_or(0);
    let items = rd_u64(blob, OFF_ITEMS).unwrap_or(0);
    let filters = rd_u32(blob, OFF_FILTERS).unwrap_or(0) as usize;
    if !(error > 0.0 && error < 1.0) || filters == 0 || filters > MAX_FILTERS {
        return Shape::Bad;
    }
    let mut subs = Vec::with_capacity(filters);
    let mut at = HDR;
    for _ in 0..filters {
        if blob.len() < at + SUB_HDR {
            return Shape::Partial;
        }
        let capacity = rd_u64(blob, at).unwrap_or(0);
        let sub_items = rd_u64(blob, at + 8).unwrap_or(0);
        let bits = rd_u64(blob, at + 16).unwrap_or(0);
        let hashes = rd_u32(blob, at + 24).unwrap_or(0);
        if capacity == 0
            || capacity > MAX_CAPACITY
            || bits == 0
            || bits % 64 != 0
            || bits / 8 > MAX_BLOB_BYTES as u64
            || hashes == 0
            || hashes > MAX_HASHES
            || sub_items > capacity
        {
            return Shape::Bad;
        }
        let bitmap = at + SUB_HDR;
        let end = bitmap + (bits / 8) as usize;
        if blob.len() < end {
            return Shape::Partial;
        }
        subs.push(Sub {
            capacity,
            items: sub_items,
            bits,
            hashes,
            hdr: at,
            bitmap,
        });
        at = end;
    }
    if at != blob.len() {
        return Shape::Bad;
    }
    Shape::Ok(Filter {
        error,
        expansion,
        flags,
        items,
        subs,
    })
}

fn parse(blob: &[u8]) -> Option<Filter> {
    match shape(blob) {
        Shape::Ok(f) => Some(f),
        _ => None,
    }
}

fn get_bit(bits: &[u8], i: u64) -> bool {
    bits[(i / 8) as usize] & (1 << (i % 8)) != 0
}

fn set_bit(bits: &mut [u8], i: u64) {
    bits[(i / 8) as usize] |= 1 << (i % 8);
}

fn sub_has(blob: &[u8], s: &Sub, h1: u64, h2: u64) -> bool {
    let bits = &blob[s.bitmap..s.bitmap + (s.bits / 8) as usize];
    (0..s.hashes).all(|i| get_bit(bits, h1.wrapping_add((i as u64).wrapping_mul(h2)) % s.bits))
}

fn push_sub(out: &mut Vec<u8>, capacity: u64, error: f64) -> Option<()> {
    let bpe = bits_per_item(error);
    let bits = bit_count(capacity, bpe);
    let bytes = (bits / 8) as usize;
    if out.len() + SUB_HDR + bytes > MAX_BLOB_BYTES {
        return None;
    }
    out.extend_from_slice(&capacity.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&bits.to_le_bytes());
    out.extend_from_slice(&hash_count(bpe).to_le_bytes());
    out.resize(out.len() + bytes, 0);
    Some(())
}

fn new_blob(error: f64, capacity: u64, expansion: u32, nonscaling: bool) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(HDR + SUB_HDR);
    out.push(TAG);
    out.extend_from_slice(&error.to_le_bytes());
    out.extend_from_slice(&expansion.to_le_bytes());
    out.extend_from_slice(&if nonscaling { FLAG_NONSCALING } else { 0 }.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    push_sub(&mut out, capacity, error)?;
    Some(out)
}

fn grown(blob: &[u8]) -> Option<Vec<u8>> {
    let f = parse(blob)?;
    if f.subs.len() >= MAX_FILTERS {
        return None;
    }
    let last = f.subs.last()?;
    let capacity = last
        .capacity
        .saturating_mul(f.expansion.max(1) as u64)
        .min(MAX_CAPACITY);
    let error = f.error * TIGHTEN.powi(f.subs.len() as i32);
    let mut out = blob.to_vec();
    push_sub(&mut out, capacity, error)?;
    out[OFF_FILTERS..HDR].copy_from_slice(&((f.subs.len() + 1) as u32).to_le_bytes());
    Some(out)
}

#[derive(PartialEq)]
enum Add {
    Added,
    Present,
    Grow,
    Full,
    Oom,
    Bad,
}

fn add_in_place(blob: &mut [u8], item: &[u8]) -> Add {
    let f = match parse(blob) {
        Some(f) => f,
        None => return Add::Bad,
    };
    let (h1, h2) = item_hashes(item);
    for s in &f.subs {
        if sub_has(blob, s, h1, h2) {
            return Add::Present;
        }
    }
    let last = match f.subs.last() {
        Some(s) => s,
        None => return Add::Bad,
    };
    if last.items >= last.capacity {
        return if f.nonscaling() { Add::Full } else { Add::Grow };
    }
    let bits = &mut blob[last.bitmap..last.bitmap + (last.bits / 8) as usize];
    for i in 0..last.hashes {
        set_bit(bits, h1.wrapping_add((i as u64).wrapping_mul(h2)) % last.bits);
    }
    blob[last.hdr + 8..last.hdr + 16].copy_from_slice(&(last.items + 1).to_le_bytes());
    blob[OFF_ITEMS..OFF_FILTERS].copy_from_slice(&(f.items + 1).to_le_bytes());
    Add::Added
}

enum Kind {
    Absent,
    Bloom(i64),
    Other,
}

fn kind_of(store: &Store, key: &[u8]) -> Kind {
    match store.get_typed(key) {
        None => Kind::Absent,
        Some((KIND_BLOOM, exp, _)) => Kind::Bloom(exp),
        Some(_) => Kind::Other,
    }
}

struct Spec {
    error: f64,
    capacity: u64,
    expansion: u32,
    nonscaling: bool,
}

impl Default for Spec {
    fn default() -> Spec {
        Spec {
            error: DEFAULT_ERROR,
            capacity: DEFAULT_CAPACITY,
            expansion: DEFAULT_EXPANSION,
            nonscaling: false,
        }
    }
}

fn create(store: &Store, key: &[u8], spec: &Spec) -> bool {
    match new_blob(spec.error, spec.capacity, spec.expansion, spec.nonscaling) {
        Some(b) => store.set_typed(key, &b, 0, KIND_BLOOM),
        None => false,
    }
}

fn add_item(store: &Store, key: &[u8], item: &[u8]) -> Add {
    match store.with_value_mut(key, |v| add_in_place(v, item)) {
        None => Add::Bad,
        Some(Add::Grow) => {
            let (exp, blob) = match store.get_typed(key) {
                Some((KIND_BLOOM, exp, v)) => (exp, v.to_vec()),
                _ => return Add::Bad,
            };
            let mut next = match grown(&blob) {
                Some(b) => b,
                None => return Add::Oom,
            };
            let r = add_in_place(&mut next, item);
            if !store.set_typed(key, &next, remaining_ttl(exp), KIND_BLOOM) {
                return Add::Oom;
            }
            r
        }
        Some(other) => other,
    }
}

fn add_error(out: &mut Vec<u8>, r: &Add) -> bool {
    match r {
        Add::Full => resp::error(out, FILTER_FULL),
        Add::Oom => resp::error(out, crate::server::OOM_ERR),
        Add::Bad => resp::error(out, BAD_DATA),
        _ => return false,
    }
    true
}

fn prepare(store: &Store, key: &[u8], spec: &Spec, nocreate: bool, out: &mut Vec<u8>) -> bool {
    match kind_of(store, key) {
        Kind::Bloom(_) => true,
        Kind::Other => {
            resp::error(out, WRONGTYPE);
            false
        }
        Kind::Absent => {
            if nocreate {
                resp::error(out, NOT_FOUND);
                return false;
            }
            if !create(store, key, spec) {
                resp::error(out, crate::server::OOM_ERR);
                return false;
            }
            true
        }
    }
}

fn add_many(store: &Store, key: &[u8], items: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let r = add_item(store, key, item);
        if matches!(r, Add::Full | Add::Oom | Add::Bad) {
            add_error(out, &r);
            return;
        }
        results.push(r == Add::Added);
    }
    resp::array_header(out, results.len());
    for ok in results {
        resp::boolean(out, ok, resp3);
    }
}

fn reserve(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() < 4 {
        return arity(out, cmd);
    }
    let error = match arg_f64(&args[2]) {
        Some(v) => v,
        None => return resp::error(out, BAD_ERROR_RATE),
    };
    if !(error > 0.0 && error < 1.0) {
        return resp::error(out, ERROR_RANGE);
    }
    let capacity = match arg_i64(&args[3]) {
        Some(v) => v,
        None => return resp::error(out, BAD_CAPACITY),
    };
    if capacity < 1 || capacity as u64 > MAX_CAPACITY {
        return resp::error(out, CAPACITY_RANGE);
    }
    let mut spec = Spec {
        error,
        capacity: capacity as u64,
        ..Spec::default()
    };
    let mut saw_expansion = false;
    let mut saw_nonscaling = false;
    let mut i = 4;
    while i < args.len() {
        if eq(&args[i], "EXPANSION") && i + 1 < args.len() {
            let n = match arg_i64(&args[i + 1]) {
                Some(v) if v >= 0 => v,
                _ => return resp::error(out, BAD_EXPANSION),
            };
            saw_expansion = true;
            spec.expansion = n as u32;
            spec.nonscaling = n == 0;
            i += 2;
        } else if eq(&args[i], "NONSCALING") {
            saw_nonscaling = true;
            spec.nonscaling = true;
            i += 1;
        } else {
            i += 1;
        }
    }
    if saw_expansion && saw_nonscaling {
        return resp::error(out, NONSCALING_EXPAND);
    }
    match kind_of(store, &args[1]) {
        Kind::Other => resp::error(out, WRONGTYPE),
        Kind::Bloom(_) => resp::error(out, ITEM_EXISTS),
        Kind::Absent => {
            if create(store, &args[1], &spec) {
                resp::simple(out, "OK");
            } else {
                resp::error(out, crate::server::OOM_ERR);
            }
        }
    }
}

fn add(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    if !prepare(store, &args[1], &Spec::default(), false, out) {
        return;
    }
    let r = add_item(store, &args[1], &args[2]);
    if !add_error(out, &r) {
        resp::boolean(out, r == Add::Added, resp3);
    }
}

fn madd(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() < 3 {
        return arity(out, cmd);
    }
    if !prepare(store, &args[1], &Spec::default(), false, out) {
        return;
    }
    add_many(store, &args[1], &args[2..], out, resp3);
}

fn insert(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() < 4 {
        return arity(out, cmd);
    }
    let mut spec = Spec::default();
    let mut nocreate = false;
    let mut i = 2;
    loop {
        if i >= args.len() {
            return arity(out, cmd);
        }
        if eq(&args[i], "ITEMS") {
            break;
        }
        if eq(&args[i], "NOCREATE") {
            nocreate = true;
            i += 1;
        } else if eq(&args[i], "NONSCALING") {
            spec.nonscaling = true;
            i += 1;
        } else if eq(&args[i], "CAPACITY") || eq(&args[i], "ERROR") || eq(&args[i], "EXPANSION") {
            if i + 1 >= args.len() {
                return arity(out, cmd);
            }
            if eq(&args[i], "CAPACITY") {
                match arg_i64(&args[i + 1]) {
                    Some(v) if v >= 1 && v as u64 <= MAX_CAPACITY => spec.capacity = v as u64,
                    _ => return resp::error(out, INSERT_BAD_CAPACITY),
                }
            } else if eq(&args[i], "ERROR") {
                match arg_f64(&args[i + 1]) {
                    Some(v) if v > 0.0 && v < 1.0 => spec.error = v,
                    _ => return resp::error(out, INSERT_BAD_ERROR),
                }
            } else {
                match arg_i64(&args[i + 1]) {
                    Some(v) if v >= 0 => {
                        spec.expansion = v as u32;
                        spec.nonscaling |= v == 0;
                    }
                    _ => return resp::error(out, INSERT_BAD_EXPANSION),
                }
            }
            i += 2;
        } else {
            return resp::error(out, UNKNOWN_ARG);
        }
    }
    let items = &args[i + 1..];
    if items.is_empty() {
        return arity(out, cmd);
    }
    if !prepare(store, &args[1], &spec, nocreate, out) {
        return;
    }
    add_many(store, &args[1], items, out, resp3);
}

fn contains(store: &Store, key: &[u8], item: &[u8]) -> bool {
    let blob = match store.get_typed(key) {
        Some((KIND_BLOOM, _, v)) => v,
        _ => return false,
    };
    let f = match parse(blob) {
        Some(f) => f,
        None => return false,
    };
    let (h1, h2) = item_hashes(item);
    f.subs.iter().any(|s| sub_has(blob, s, h1, h2))
}

fn exists(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    resp::boolean(out, contains(store, &args[1], &args[2]), resp3);
}

fn mexists(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() < 3 {
        return arity(out, cmd);
    }
    resp::array_header(out, args.len() - 2);
    for item in &args[2..] {
        resp::boolean(out, contains(store, &args[1], item), resp3);
    }
}

fn info_field(out: &mut Vec<u8>, name: &str, val: Option<i64>, resp3: bool) {
    resp::simple(out, name);
    match val {
        Some(v) => resp::integer(out, v),
        None => resp::null(out, resp3),
    }
}

fn info(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() < 2 || args.len() > 3 {
        return arity(out, cmd);
    }
    let blob = match store.get_typed(&args[1]) {
        Some((KIND_BLOOM, _, v)) => v,
        Some(_) => return resp::error(out, WRONGTYPE),
        None => return resp::error(out, NOT_FOUND),
    };
    let f = match parse(blob) {
        Some(f) => f,
        None => return resp::error(out, BAD_DATA),
    };
    let expansion = if f.nonscaling() {
        None
    } else {
        Some(f.expansion as i64)
    };
    if args.len() == 2 {
        resp::map_header(out, 5, resp3);
        info_field(out, "Capacity", Some(f.capacity() as i64), resp3);
        info_field(out, "Size", Some(blob.len() as i64), resp3);
        info_field(out, "Number of filters", Some(f.subs.len() as i64), resp3);
        info_field(out, "Number of items inserted", Some(f.items as i64), resp3);
        info_field(out, "Expansion rate", expansion, resp3);
        return;
    }
    let (name, val) = if eq(&args[2], "CAPACITY") {
        ("Capacity", Some(f.capacity() as i64))
    } else if eq(&args[2], "SIZE") {
        ("Size", Some(blob.len() as i64))
    } else if eq(&args[2], "FILTERS") {
        ("Number of filters", Some(f.subs.len() as i64))
    } else if eq(&args[2], "ITEMS") {
        ("Number of items inserted", Some(f.items as i64))
    } else if eq(&args[2], "EXPANSION") {
        ("Expansion rate", expansion)
    } else {
        return resp::error(out, INVALID_INFO);
    };
    if resp3 {
        resp::map_header(out, 1, true);
        info_field(out, name, val, true);
    } else {
        resp::array_header(out, 1);
        match val {
            Some(v) => resp::integer(out, v),
            None => resp::null(out, false),
        }
    }
}

fn card(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() != 2 {
        return arity(out, cmd);
    }
    match store.get_typed(&args[1]) {
        None => resp::integer(out, 0),
        Some((KIND_BLOOM, _, v)) => match parse(v) {
            Some(f) => resp::integer(out, f.items as i64),
            None => resp::error(out, BAD_DATA),
        },
        Some(_) => resp::error(out, WRONGTYPE),
    }
}

fn scandump(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    let it = match arg_i64(&args[2]) {
        Some(v) => v,
        None => return resp::error(out, SCANDUMP_NUMERIC),
    };
    let blob = match store.get_typed(&args[1]) {
        Some((KIND_BLOOM, _, v)) => v,
        _ => return resp::error(out, NOT_FOUND),
    };
    let off = if it <= 0 { 0 } else { (it - 1) as usize };
    resp::array_header(out, 2);
    if it < 0 || off >= blob.len() {
        resp::integer(out, 0);
        resp::bulk(out, b"");
        return;
    }
    let end = off.saturating_add(CHUNK_BYTES).min(blob.len());
    resp::integer(out, (end + 1) as i64);
    resp::bulk(out, &blob[off..end]);
}

fn loadchunk(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() != 4 {
        return arity(out, cmd);
    }
    let it = match arg_i64(&args[2]) {
        Some(v) => v,
        None => return resp::error(out, LOADCHUNK_NUMERIC),
    };
    if it < 1 {
        return resp::error(out, NOT_FOUND);
    }
    let off = (it - 1) as usize;
    let (exp, mut next) = if off == 0 {
        match kind_of(store, &args[1]) {
            Kind::Other => return resp::error(out, BAD_DATA),
            Kind::Bloom(e) => (e, Vec::new()),
            Kind::Absent => (0, Vec::new()),
        }
    } else {
        match store.get_typed(&args[1]) {
            Some((KIND_BLOOM, e, v)) if v.len() == off => (e, v.to_vec()),
            _ => return resp::error(out, BAD_DATA),
        }
    };
    if next.len() + args[3].len() > MAX_BLOB_BYTES {
        return resp::error(out, BAD_DATA);
    }
    next.extend_from_slice(&args[3]);
    if matches!(shape(&next), Shape::Bad) {
        return resp::error(out, BAD_DATA);
    }
    if store.set_typed(&args[1], &next, remaining_ttl(exp), KIND_BLOOM) {
        resp::simple(out, "OK");
    } else {
        resp::error(out, crate::server::OOM_ERR);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Config;

    fn cfg(data: u64) -> Config {
        Config {
            num_partitions: 1,
            buckets_per_part: 1024,
            entries_per_part: 512,
            data_bytes_per_part: data,
        }
    }

    fn run(store: &Store, argv: &[&str]) -> String {
        let args: Vec<Vec<u8>> = argv.iter().map(|a| a.as_bytes().to_vec()).collect();
        let cmd = argv[0].to_uppercase().into_bytes();
        let mut out = Vec::new();
        dispatch(store, &cmd, &args, &mut out, false);
        String::from_utf8_lossy(&out).into_owned()
    }

    #[test]
    fn a_header_and_its_sub_filters_survive_a_round_trip() {
        let blob = new_blob(0.005, 500, 3, false).unwrap();
        let f = parse(&blob).expect("a fresh blob must parse");
        assert_eq!(f.error, 0.005);
        assert_eq!(f.expansion, 3);
        assert_eq!(f.items, 0);
        assert!(!f.nonscaling());
        assert_eq!(f.subs.len(), 1);
        assert_eq!(f.subs[0].capacity, 500);
        assert_eq!(f.subs[0].items, 0);
        assert_eq!(f.subs[0].bits % 64, 0);
        assert_eq!(blob.len(), HDR + SUB_HDR + (f.subs[0].bits / 8) as usize);

        let ns = new_blob(0.01, 100, 2, true).unwrap();
        assert!(parse(&ns).unwrap().nonscaling());

        let two = grown(&blob).unwrap();
        let g = parse(&two).expect("a grown blob must parse");
        assert_eq!(g.subs.len(), 2);
        assert_eq!(g.subs[1].capacity, 1500);
        assert_eq!(g.error, 0.005);
    }

    #[test]
    fn an_added_item_is_reported_as_present() {
        let mut blob = new_blob(0.01, 100, 2, false).unwrap();
        assert!(matches!(add_in_place(&mut blob, b"hello"), Add::Added));
        assert!(matches!(add_in_place(&mut blob, b"hello"), Add::Present));
        let f = parse(&blob).unwrap();
        assert_eq!(f.items, 1);
        assert_eq!(f.subs[0].items, 1);
        let (h1, h2) = item_hashes(b"hello");
        assert!(sub_has(&blob, &f.subs[0], h1, h2));
    }

    #[test]
    fn items_that_were_never_added_are_almost_always_absent() {
        let mut blob = new_blob(0.01, 1000, 2, false).unwrap();
        for i in 0..1000u32 {
            add_in_place(&mut blob, format!("in{i}").as_bytes());
        }
        let f = parse(&blob).unwrap();
        let mut hits = 0;
        for i in 0..1000u32 {
            let (h1, h2) = item_hashes(format!("out{i}").as_bytes());
            if f.subs.iter().any(|s| sub_has(&blob, s, h1, h2)) {
                hits += 1;
            }
        }
        assert!(hits < 50, "{hits} false positives in 1000 misses");
    }

    #[test]
    fn the_false_positive_rate_stays_under_twice_the_configured_error() {
        let n = 100_000u32;
        let mut blob = new_blob(0.01, n as u64, 2, false).unwrap();
        for i in 0..n {
            add_in_place(&mut blob, format!("member-{i}").as_bytes());
        }
        let f = parse(&blob).unwrap();
        assert_eq!(f.subs.len(), 1, "the filter must not have grown");
        let mut hits = 0u32;
        for i in 0..n {
            let (h1, h2) = item_hashes(format!("absent-{i}").as_bytes());
            if f.subs.iter().any(|s| sub_has(&blob, s, h1, h2)) {
                hits += 1;
            }
        }
        let rate = hits as f64 / n as f64;
        assert!(rate < 0.02, "false positive rate {rate} on {n} misses");
        for i in 0..n {
            let (h1, h2) = item_hashes(format!("member-{i}").as_bytes());
            assert!(
                f.subs.iter().any(|s| sub_has(&blob, s, h1, h2)),
                "member-{i} must still be present"
            );
        }
    }

    #[test]
    fn a_full_scaling_filter_grows_and_keeps_every_item() {
        let mut blob = new_blob(0.01, 100, 2, false).unwrap();
        let mut added = 0u64;
        for i in 0..700u32 {
            let item = format!("g{i}");
            loop {
                match add_in_place(&mut blob, item.as_bytes()) {
                    Add::Added => {
                        added += 1;
                        break;
                    }
                    Add::Present => break,
                    Add::Grow => blob = grown(&blob).expect("growth must succeed"),
                    _ => panic!("g{i} was refused by a scaling filter"),
                }
            }
        }
        let f = parse(&blob).unwrap();
        assert_eq!(f.subs.len(), 3, "100+200+400 holds 700 items");
        assert_eq!(f.capacity(), 700);
        assert_eq!(f.items, added);
        for (i, s) in f.subs.iter().enumerate() {
            assert_eq!(s.capacity, 100 * 2u64.pow(i as u32));
        }
        for i in 0..700u32 {
            let (h1, h2) = item_hashes(format!("g{i}").as_bytes());
            assert!(
                f.subs.iter().any(|s| sub_has(&blob, s, h1, h2)),
                "g{i} was lost by growth"
            );
        }
    }

    #[test]
    fn a_nonscaling_filter_refuses_to_grow() {
        let mut blob = new_blob(0.01, 5, 2, true).unwrap();
        for i in 0..5u32 {
            assert!(matches!(
                add_in_place(&mut blob, format!("n{i}").as_bytes()),
                Add::Added
            ));
        }
        assert!(matches!(add_in_place(&mut blob, b"overflow"), Add::Full));
        assert_eq!(parse(&blob).unwrap().items, 5);
    }

    #[test]
    fn bits_per_item_and_hash_count_are_pinned_for_a_one_percent_filter() {
        let bpe = bits_per_item(0.01);
        assert!(
            (9.5850..9.5852).contains(&bpe),
            "bits per item drifted to {bpe}"
        );
        assert_eq!(hash_count(bpe), 7);
        assert_eq!(bit_count(100, bpe), 960);
        assert_eq!(hash_count(bits_per_item(0.001)), 10);
    }

    #[test]
    fn a_dump_larger_than_one_chunk_reassembles_byte_for_byte() {
        let s = Store::create("t_bloom_chunks", &cfg(64 * 1024 * 1024)).unwrap();
        let blob = new_blob(0.01, 15_000_000, 2, false).unwrap();
        assert!(blob.len() > CHUNK_BYTES, "the dump must need two chunks");
        assert!(s.set_typed(b"src", &blob, 0, KIND_BLOOM));

        let mut rebuilt: Vec<u8> = Vec::new();
        let mut it = 0i64;
        let mut chunks = 0;
        loop {
            let mut out = Vec::new();
            let args = vec![
                b"BF.SCANDUMP".to_vec(),
                b"src".to_vec(),
                it.to_string().into_bytes(),
            ];
            dispatch(&s, b"BF.SCANDUMP", &args, &mut out, false);
            let (next, data) = decode_scandump(&out);
            if next == 0 {
                assert!(data.is_empty());
                break;
            }
            rebuilt.extend_from_slice(&data);
            it = next;
            chunks += 1;
            assert!(chunks <= 4, "the chunk loop did not terminate");
        }
        assert_eq!(chunks, 2);
        assert_eq!(rebuilt, blob);
    }

    fn decode_scandump(out: &[u8]) -> (i64, Vec<u8>) {
        let nl = out.iter().position(|&c| c == b'\n').unwrap();
        let rest = &out[nl + 1..];
        assert_eq!(rest[0], b':');
        let nl2 = rest.iter().position(|&c| c == b'\n').unwrap();
        let next: i64 = std::str::from_utf8(&rest[1..nl2 - 1]).unwrap().parse().unwrap();
        let body = &rest[nl2 + 1..];
        assert_eq!(body[0], b'$');
        let nl3 = body.iter().position(|&c| c == b'\n').unwrap();
        let len: usize = std::str::from_utf8(&body[1..nl3 - 1]).unwrap().parse().unwrap();
        (next, body[nl3 + 1..nl3 + 1 + len].to_vec())
    }

    #[test]
    fn a_dump_round_trips_through_scandump_and_loadchunk() {
        let s = Store::create("t_bloom_dump", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "src", "0.01", "200"]), "+OK\r\n");
        for i in 0..50u32 {
            run(&s, &["BF.ADD", "src", &format!("d{i}")]);
        }
        let mut out = Vec::new();
        let args = vec![b"BF.SCANDUMP".to_vec(), b"src".to_vec(), b"0".to_vec()];
        dispatch(&s, b"BF.SCANDUMP", &args, &mut out, false);
        let (next, data) = decode_scandump(&out);

        let load = vec![
            b"BF.LOADCHUNK".to_vec(),
            b"dst".to_vec(),
            b"1".to_vec(),
            data.clone(),
        ];
        let mut out = Vec::new();
        dispatch(&s, b"BF.LOADCHUNK", &load, &mut out, false);
        assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");

        let mut out = Vec::new();
        let args = vec![
            b"BF.SCANDUMP".to_vec(),
            b"src".to_vec(),
            next.to_string().into_bytes(),
        ];
        dispatch(&s, b"BF.SCANDUMP", &args, &mut out, false);
        assert_eq!(decode_scandump(&out).0, 0);

        assert_eq!(run(&s, &["BF.CARD", "dst"]), ":50\r\n");
        for i in 0..50u32 {
            assert_eq!(
                run(&s, &["BF.EXISTS", "dst", &format!("d{i}")]),
                ":1\r\n",
                "d{i} did not survive the dump"
            );
        }
        assert_eq!(run(&s, &["BF.EXISTS", "dst", "never-added"]), ":0\r\n");
    }

    #[test]
    fn loadchunk_refuses_a_chunk_that_is_not_a_filter() {
        let s = Store::create("t_bloom_baddata", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["BF.LOADCHUNK", "k", "1", "xx"]),
            format!("-{BAD_DATA}\r\n")
        );
        assert_eq!(run(&s, &["BF.LOADCHUNK", "k", "0", "xx"]), format!("-{NOT_FOUND}\r\n"));
    }

    #[test]
    fn bf_add_mutates_the_blob_without_changing_its_length() {
        let s = Store::create("t_bloom_inplace", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "f", "0.01", "10000"]), "+OK\r\n");
        let before = s.get_typed(b"f").unwrap().2.len();
        for i in 0..1000u32 {
            assert_eq!(run(&s, &["BF.ADD", "f", &format!("k{i}")]), ":1\r\n");
            assert_eq!(
                s.get_typed(b"f").unwrap().2.len(),
                before,
                "the blob was rebuilt at item {i}"
            );
        }
        assert_eq!(run(&s, &["BF.CARD", "f"]), ":1000\r\n");
        assert_eq!(run(&s, &["BF.ADD", "f", "k0"]), ":0\r\n");
    }

    #[test]
    fn the_handlers_answer_with_the_redis_error_texts() {
        let s = Store::create("t_bloom_errors", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["BF.RESERVE", "k"]),
            "-ERR wrong number of arguments for 'bf.reserve' command\r\n"
        );
        assert_eq!(run(&s, &["BF.RESERVE", "k", "abc", "100"]), format!("-{BAD_ERROR_RATE}\r\n"));
        assert_eq!(run(&s, &["BF.RESERVE", "k", "0.01", "abc"]), format!("-{BAD_CAPACITY}\r\n"));
        assert_eq!(run(&s, &["BF.RESERVE", "k", "1.0", "100"]), format!("-{ERROR_RANGE}\r\n"));
        assert_eq!(run(&s, &["BF.RESERVE", "k", "0.01", "0"]), format!("-{CAPACITY_RANGE}\r\n"));
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "100", "EXPANSION", "2", "NONSCALING"]),
            format!("-{NONSCALING_EXPAND}\r\n")
        );
        assert_eq!(run(&s, &["BF.RESERVE", "k", "0.01", "100"]), "+OK\r\n");
        assert_eq!(run(&s, &["BF.RESERVE", "k", "0.01", "100"]), format!("-{ITEM_EXISTS}\r\n"));
        assert_eq!(run(&s, &["BF.INFO", "k", "BOGUS"]), format!("-{INVALID_INFO}\r\n"));
        assert_eq!(run(&s, &["BF.INFO", "missing"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["BF.CARD", "missing"]), ":0\r\n");
        assert_eq!(run(&s, &["BF.INSERT", "n", "NOCREATE", "ITEMS", "a"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["BF.INSERT", "n", "BOGUS", "ITEMS", "a"]), format!("-{UNKNOWN_ARG}\r\n"));
        assert_eq!(
            run(&s, &["BF.INSERT", "n", "CAPACITY", "0", "ITEMS", "a"]),
            format!("-{INSERT_BAD_CAPACITY}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.INSERT", "n", "ERROR", "2", "ITEMS", "a"]),
            format!("-{INSERT_BAD_ERROR}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.INSERT", "n", "EXPANSION", "x", "ITEMS", "a"]),
            format!("-{INSERT_BAD_EXPANSION}\r\n")
        );
        assert_eq!(run(&s, &["BF.SCANDUMP", "k", "abc"]), format!("-{SCANDUMP_NUMERIC}\r\n"));
        assert_eq!(run(&s, &["BF.SCANDUMP", "missing", "0"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(
            run(&s, &["BF.LOADCHUNK", "k", "abc", "d"]),
            format!("-{LOADCHUNK_NUMERIC}\r\n")
        );
        assert_eq!(run(&s, &["BF.WHAT", "k"]), "-ERR unknown command 'BF.WHAT'\r\n");

        assert_eq!(run(&s, &["BF.RESERVE", "ns", "0.01", "3", "NONSCALING"]), "+OK\r\n");
        for i in 0..3u32 {
            assert_eq!(run(&s, &["BF.ADD", "ns", &format!("x{i}")]), ":1\r\n");
        }
        assert_eq!(run(&s, &["BF.ADD", "ns", "over"]), format!("-{FILTER_FULL}\r\n"));
        assert_eq!(run(&s, &["BF.MADD", "ns", "a", "b"]), format!("-{FILTER_FULL}\r\n"));
    }

    #[test]
    fn a_key_of_another_type_answers_wrongtype_where_redis_does() {
        let s = Store::create("t_bloom_wrongtype", &cfg(1024 * 1024)).unwrap();
        assert!(s.set(b"str", b"v", 0));
        let wt = format!("-{WRONGTYPE}\r\n");
        assert_eq!(run(&s, &["BF.ADD", "str", "x"]), wt);
        assert_eq!(run(&s, &["BF.MADD", "str", "x"]), wt);
        assert_eq!(run(&s, &["BF.INSERT", "str", "ITEMS", "x"]), wt);
        assert_eq!(run(&s, &["BF.RESERVE", "str", "0.01", "100"]), wt);
        assert_eq!(run(&s, &["BF.INFO", "str"]), wt);
        assert_eq!(run(&s, &["BF.CARD", "str"]), wt);
        assert_eq!(run(&s, &["BF.EXISTS", "str", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["BF.MEXISTS", "str", "x"]), "*1\r\n:0\r\n");
    }

    #[test]
    fn info_matches_the_redis_reply_shape_in_both_protocols() {
        let s = Store::create("t_bloom_info", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.ADD", "b", "hello"]), ":1\r\n");
        assert_eq!(
            run(&s, &["BF.INFO", "b"]),
            "*10\r\n+Capacity\r\n:100\r\n+Size\r\n:177\r\n+Number of filters\r\n:1\r\n\
             +Number of items inserted\r\n:1\r\n+Expansion rate\r\n:2\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "b", "capacity"]), "*1\r\n:100\r\n");
        assert_eq!(run(&s, &["BF.INFO", "b", "FILTERS"]), "*1\r\n:1\r\n");

        let args: Vec<Vec<u8>> = vec![b"BF.INFO".to_vec(), b"b".to_vec()];
        let mut out = Vec::new();
        dispatch(&s, b"BF.INFO", &args, &mut out, true);
        assert!(String::from_utf8_lossy(&out).starts_with("%5\r\n+Capacity\r\n:100\r\n"));

        let args: Vec<Vec<u8>> = vec![b"BF.INFO".to_vec(), b"b".to_vec(), b"CAPACITY".to_vec()];
        let mut out = Vec::new();
        dispatch(&s, b"BF.INFO", &args, &mut out, true);
        assert_eq!(String::from_utf8_lossy(&out), "%1\r\n+Capacity\r\n:100\r\n");

        assert_eq!(run(&s, &["BF.RESERVE", "ns", "0.01", "10", "NONSCALING"]), "+OK\r\n");
        assert!(run(&s, &["BF.INFO", "ns"]).ends_with("+Expansion rate\r\n$-1\r\n"));
        assert_eq!(run(&s, &["BF.INFO", "ns", "EXPANSION"]), "*1\r\n$-1\r\n");
    }

    #[test]
    fn resp3_answers_add_and_exists_with_booleans() {
        let s = Store::create("t_bloom_resp3", &cfg(1024 * 1024)).unwrap();
        let call = |argv: &[&str]| {
            let args: Vec<Vec<u8>> = argv.iter().map(|a| a.as_bytes().to_vec()).collect();
            let cmd = argv[0].to_uppercase().into_bytes();
            let mut out = Vec::new();
            dispatch(&s, &cmd, &args, &mut out, true);
            String::from_utf8_lossy(&out).into_owned()
        };
        assert_eq!(call(&["BF.ADD", "r", "a"]), "#t\r\n");
        assert_eq!(call(&["BF.ADD", "r", "a"]), "#f\r\n");
        assert_eq!(call(&["BF.MADD", "r", "a", "b"]), "*2\r\n#f\r\n#t\r\n");
        assert_eq!(call(&["BF.EXISTS", "r", "a"]), "#t\r\n");
        assert_eq!(call(&["BF.MEXISTS", "r", "a", "zz"]), "*2\r\n#t\r\n#f\r\n");
        assert_eq!(call(&["BF.CARD", "r"]), ":2\r\n");
    }

    #[test]
    fn insert_creates_scales_and_honours_its_options() {
        let s = Store::create("t_bloom_insert", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.INSERT", "i", "ITEMS", "a", "b", "c"]), "*3\r\n:1\r\n:1\r\n:1\r\n");
        assert_eq!(run(&s, &["BF.INSERT", "i", "ITEMS", "a", "d"]), "*2\r\n:0\r\n:1\r\n");
        assert_eq!(run(&s, &["BF.CARD", "i"]), ":4\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "j", "CAPACITY", "1000", "ERROR", "0.001", "EXPANSION", "3", "ITEMS", "z"]),
            "*1\r\n:1\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "j", "CAPACITY"]), "*1\r\n:1000\r\n");
        assert_eq!(run(&s, &["BF.INFO", "j", "EXPANSION"]), "*1\r\n:3\r\n");
        assert_eq!(run(&s, &["BF.INSERT", "e", "EXPANSION", "0", "ITEMS", "z"]), "*1\r\n:1\r\n");
        assert_eq!(run(&s, &["BF.INFO", "e", "EXPANSION"]), "*1\r\n$-1\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "i", "ITEMS"]),
            "-ERR wrong number of arguments for 'bf.insert' command\r\n"
        );
    }

    #[test]
    fn a_scaling_filter_grows_through_the_store() {
        let s = Store::create("t_bloom_grow", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "g", "0.01", "10"]), "+OK\r\n");
        for i in 0..40u32 {
            assert_eq!(run(&s, &["BF.ADD", "g", &format!("s{i}")]), ":1\r\n");
        }
        assert_eq!(run(&s, &["BF.INFO", "g", "FILTERS"]), "*1\r\n:3\r\n");
        assert_eq!(run(&s, &["BF.INFO", "g", "CAPACITY"]), "*1\r\n:70\r\n");
        assert_eq!(run(&s, &["BF.CARD", "g"]), ":40\r\n");
        for i in 0..40u32 {
            assert_eq!(run(&s, &["BF.EXISTS", "g", &format!("s{i}")]), ":1\r\n");
        }
    }

    #[test]
    fn a_rewritten_filter_keeps_the_keys_ttl() {
        let s = Store::create("t_bloom_ttl", &cfg(1024 * 1024)).unwrap();
        let blob = new_blob(0.01, 2, 2, false).unwrap();
        assert!(s.set_typed(b"t", &blob, 60_000_000, KIND_BLOOM));
        for i in 0..6u32 {
            assert_eq!(run(&s, &["BF.ADD", "t", &format!("t{i}")]), ":1\r\n");
        }
        assert_eq!(run(&s, &["BF.INFO", "t", "FILTERS"]), "*1\r\n:2\r\n");
        let exp = s.get_typed(b"t").unwrap().1;
        assert!(exp > now_micros(), "growth dropped the TTL");
    }

    #[test]
    fn the_type_and_encoding_names_match_redis() {
        assert_eq!(type_name(KIND_BLOOM), Some("MBbloom--"));
        assert_eq!(type_name(KIND_CUCKOO), Some("MBbloomCF"));
        assert_eq!(type_name(crate::store::KIND_STR), None);
        assert_eq!(encoding_name(KIND_BLOOM), Some("raw"));
        assert_eq!(encoding_name(crate::store::KIND_SET), None);
    }
}
