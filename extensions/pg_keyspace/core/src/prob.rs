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
const MAX_FILTERS: usize = 1024;
const CF_MAX_FILTERS: usize = 32;
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
const CANNOT_CREATE: &str = "ERR could not create filter";
const MIN_ERROR: f64 = 9.881312916824931e-324;
const MAX_COUNTER: u64 = 1 << 62;
const NO_EXPANSION: &str = "ERR no expansion";
const EXPANSION_RANGE: &str = "ERR expansion must be in the range [0, 32768]";

const CTAG: u8 = 2;
const C_OFF_BUCKET: usize = 1;
const C_OFF_MAXITER: usize = 5;
const C_OFF_EXPANSION: usize = 9;
const C_OFF_ITEMS: usize = 13;
const C_OFF_DELETES: usize = 21;
const C_OFF_FILTERS: usize = 29;
const C_HDR: usize = 33;
const C_SUB_HDR: usize = 8;

const CF_DEFAULT_CAPACITY: u64 = 1024;
const CF_DEFAULT_BUCKET: u32 = 2;
const CF_DEFAULT_MAXITER: u32 = 20;
const CF_DEFAULT_EXPANSION: u32 = 1;
const CF_MAX_BUCKET: u32 = 255;
const CF_MAX_MAXITER: u32 = 65535;
const CF_MAX_EXPANSION: u32 = 32768;

const CF_NOT_FOUND: &str = "Not found";
const CF_CAPACITY_RANGE: &str = "Capacity must be in the range [2 * BUCKETSIZE, 1073741824]";
const CF_INSERT_CAPACITY_RANGE: &str =
    "Capacity must be in the range [cf-bucket-size * 2, 1073741824]";
const CF_PARSE_BUCKETSIZE: &str = "Couldn't parse BUCKETSIZE";
const CF_BUCKETSIZE_RANGE: &str = "BUCKETSIZE: value must be in the range [1, 255]";
const CF_PARSE_MAXITER: &str = "Couldn't parse MAXITERATIONS";
const CF_MAXITER_RANGE: &str = "MAXITERATIONS: value must be in the range [1, 65535]";
const CF_PARSE_EXPANSION: &str = "Couldn't parse EXPANSION";
const CF_EXPANSION_RANGE: &str = "EXPANSION: value must be in the range [0, 32768]";
const CF_FULL: &str = "Filter is full";
const MAX_EXPANSIONS: &str = "Maximum expansions reached";
const CF_INVALID_POSITION: &str = "Invalid position";
const CF_INVALID_HEADER: &str = "Invalid header";

pub fn dispatch(
    store: &Store,
    cmd: &[u8],
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
    resp3: bool,
    max_bulk: usize,
) {
    match cmd {
        b"BF.RESERVE" => reserve(store, cmd, args, out),
        b"BF.ADD" => add(store, cmd, args, out, resp3),
        b"BF.MADD" => madd(store, cmd, args, out, resp3),
        b"BF.INSERT" => insert(store, cmd, args, out, resp3),
        b"BF.EXISTS" => exists(store, cmd, args, out, resp3),
        b"BF.MEXISTS" => mexists(store, cmd, args, out, resp3),
        b"BF.INFO" => info(store, cmd, args, out, resp3),
        b"BF.CARD" => card(store, cmd, args, out),
        b"BF.SCANDUMP" => scandump(store, cmd, args, out, max_bulk),
        b"BF.LOADCHUNK" => loadchunk(store, cmd, args, out),
        b"CF.RESERVE" => cf_reserve(store, cmd, args, out),
        b"CF.ADD" => cf_add(store, cmd, args, out, resp3, false),
        b"CF.ADDNX" => cf_add(store, cmd, args, out, resp3, true),
        b"CF.INSERT" => cf_insert(store, cmd, args, out, resp3, false),
        b"CF.INSERTNX" => cf_insert(store, cmd, args, out, resp3, true),
        b"CF.EXISTS" => cf_exists(store, cmd, args, out, resp3),
        b"CF.MEXISTS" => cf_mexists(store, cmd, args, out, resp3),
        b"CF.DEL" => cf_del(store, cmd, args, out, resp3),
        b"CF.COUNT" => cf_count(store, cmd, args, out),
        b"CF.INFO" => cf_info(store, cmd, args, out, resp3),
        b"CF.SCANDUMP" => cf_scandump(store, cmd, args, out, resp3, max_bulk),
        b"CF.LOADCHUNK" => cf_loadchunk(store, cmd, args, out),
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

fn chunk_bytes(max_bulk: usize) -> usize {
    CHUNK_BYTES.min(max_bulk).max(C_HDR)
}

fn chunk_start(it: i64, len: usize) -> Option<usize> {
    (it as usize - 1).checked_sub(len)
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

fn bits_fit(capacity: u64, error: f64) -> bool {
    let n = capacity as f64 * bits_per_item(error);
    n.is_finite() && n < (MAX_BLOB_BYTES as u64 * 8) as f64
}

fn bit_count(capacity: u64, bpe: f64) -> u64 {
    let cap_bits = MAX_BLOB_BYTES as u64 * 8;
    let n = capacity as f64 * bpe;
    if !n.is_finite() || n >= cap_bits as f64 {
        return cap_bits;
    }
    (n.ceil() as u64).div_ceil(64).max(1) * 64
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
    if at != blob.len() || items > subs.iter().map(|s| s.capacity).sum::<u64>() {
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
    let last = f.subs.last()?;
    let capacity = last
        .capacity
        .saturating_mul(f.expansion.max(1) as u64)
        .min(MAX_CAPACITY);
    let error = (f.error * TIGHTEN.powi(f.subs.len() as i32)).max(MIN_ERROR);
    if !bits_fit(capacity, error) {
        return None;
    }
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
    MaxGrow,
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
        if f.nonscaling() {
            return Add::Full;
        }
        if f.subs.len() >= MAX_FILTERS {
            return Add::MaxGrow;
        }
        return Add::Grow;
    }
    let bits = &mut blob[last.bitmap..last.bitmap + (last.bits / 8) as usize];
    for i in 0..last.hashes {
        set_bit(bits, h1.wrapping_add((i as u64).wrapping_mul(h2)) % last.bits);
    }
    blob[last.hdr + 8..last.hdr + 16].copy_from_slice(&last.items.saturating_add(1).to_le_bytes());
    blob[OFF_ITEMS..OFF_FILTERS].copy_from_slice(&f.items.saturating_add(1).to_le_bytes());
    Add::Added
}

enum Kind {
    Absent,
    Filter,
    Other,
}

fn kind_of(store: &Store, key: &[u8]) -> Kind {
    match store.get_typed(key) {
        None => Kind::Absent,
        Some((KIND_BLOOM, _, _)) => Kind::Filter,
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

fn bloom_blob_bytes(error: f64, capacity: u64) -> usize {
    HDR + SUB_HDR + (bit_count(capacity, bits_per_item(error)) / 8) as usize
}

fn create(store: &Store, key: &[u8], spec: &Spec) -> bool {
    if !store.can_hold(bloom_blob_bytes(spec.error, spec.capacity)) {
        return false;
    }
    match new_blob(spec.error, spec.capacity, spec.expansion, spec.nonscaling) {
        Some(b) => store.set_typed(key, &b, 0, KIND_BLOOM),
        None => false,
    }
}

fn add_item(store: &Store, key: &[u8], item: &[u8], spec: &Spec) -> Add {
    let first = match store.with_value_mut(key, |v| add_in_place(v, item)) {
        Some(r) => Some(r),
        None => {
            if !create(store, key, spec) {
                return Add::Oom;
            }
            store.with_value_mut(key, |v| add_in_place(v, item))
        }
    };
    let r = match first {
        Some(r) => r,
        None => return Add::Oom,
    };
    if r != Add::Grow {
        return r;
    }
    let (exp, blob) = match store.get_typed(key) {
        Some((KIND_BLOOM, exp, v)) => (exp, v.to_vec()),
        _ => return Add::Bad,
    };
    let mut next = match grown(&blob) {
        Some(b) => b,
        None => return Add::MaxGrow,
    };
    let r = add_in_place(&mut next, item);
    if r == Add::Bad {
        return Add::Bad;
    }
    if !store.set_typed(key, &next, remaining_ttl(exp), KIND_BLOOM) {
        return Add::Oom;
    }
    r
}

fn add_error(out: &mut Vec<u8>, r: &Add) -> bool {
    match r {
        Add::Full => resp::error(out, FILTER_FULL),
        Add::MaxGrow => resp::error(out, MAX_EXPANSIONS),
        Add::Oom => resp::error(out, crate::server::OOM_ERR),
        Add::Bad => resp::error(out, BAD_DATA),
        _ => return false,
    }
    true
}

fn prepare(store: &Store, key: &[u8], spec: &Spec, nocreate: bool, out: &mut Vec<u8>) -> bool {
    match kind_of(store, key) {
        Kind::Filter => true,
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

fn add_many(
    store: &Store,
    key: &[u8],
    items: &[Vec<u8>],
    spec: &Spec,
    out: &mut Vec<u8>,
    resp3: bool,
) {
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let r = add_item(store, key, item, spec);
        let stop = matches!(r, Add::Full | Add::MaxGrow | Add::Oom | Add::Bad);
        results.push(r);
        if stop {
            break;
        }
    }
    resp::array_header(out, results.len());
    for r in &results {
        if !add_error(out, r) {
            resp::boolean(out, *r == Add::Added, resp3);
        }
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
    if error < MIN_ERROR {
        return resp::error(out, CANNOT_CREATE);
    }
    let capacity = match arg_i64(&args[3]) {
        Some(v) => v,
        None => return resp::error(out, BAD_CAPACITY),
    };
    if capacity < 1 || capacity as u64 > MAX_CAPACITY {
        return resp::error(out, CAPACITY_RANGE);
    }
    if !bits_fit(capacity as u64, error) {
        return resp::error(out, CANNOT_CREATE);
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
        if eq(&args[i], "EXPANSION") {
            if saw_expansion {
                if i + 1 < args.len() {
                    return arity(out, cmd);
                }
                i += 1;
                continue;
            }
            if i + 1 >= args.len() {
                return resp::error(out, NO_EXPANSION);
            }
            let n = match arg_i64(&args[i + 1]) {
                Some(v) => v,
                None => return resp::error(out, BAD_EXPANSION),
            };
            if !(0..=CF_MAX_EXPANSION as i64).contains(&n) {
                return resp::error(out, EXPANSION_RANGE);
            }
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
        Kind::Filter => resp::error(out, ITEM_EXISTS),
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
    let r = add_item(store, &args[1], &args[2], &Spec::default());
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
    add_many(store, &args[1], &args[2..], &Spec::default(), out, resp3);
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
                    Some(v) if v > 0.0 && v < 1.0 => {
                        if v < MIN_ERROR {
                            return resp::error(out, CANNOT_CREATE);
                        }
                        spec.error = v;
                    }
                    _ => return resp::error(out, INSERT_BAD_ERROR),
                }
            } else {
                match arg_i64(&args[i + 1]) {
                    Some(v) if (0..=CF_MAX_EXPANSION as i64).contains(&v) => {
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
    if !bits_fit(spec.capacity, spec.error) {
        return resp::error(out, CANNOT_CREATE);
    }
    if !prepare(store, &args[1], &spec, nocreate, out) {
        return;
    }
    add_many(store, &args[1], items, &spec, out, resp3);
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

fn readable(store: &Store, key: &[u8]) -> bool {
    match store.get_typed(key) {
        Some((KIND_BLOOM, _, v)) => parse(v).is_some(),
        _ => true,
    }
}

fn exists(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    if !readable(store, &args[1]) {
        return resp::error(out, BAD_DATA);
    }
    resp::boolean(out, contains(store, &args[1], &args[2]), resp3);
}

fn mexists(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() < 3 {
        return arity(out, cmd);
    }
    if !readable(store, &args[1]) {
        return resp::error(out, BAD_DATA);
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

fn scandump(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, max_bulk: usize) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    let it = match arg_i64(&args[2]) {
        Some(v) => v,
        None => return resp::error(out, SCANDUMP_NUMERIC),
    };
    let blob = match store.get_typed(&args[1]) {
        Some((KIND_BLOOM, _, v)) => v,
        Some(_) => return resp::error(out, WRONGTYPE),
        None => return resp::error(out, NOT_FOUND),
    };
    let off = if it <= 0 { 0 } else { (it - 1) as usize };
    resp::array_header(out, 2);
    if it < 0 || off >= blob.len() {
        resp::integer(out, 0);
        resp::bulk(out, b"");
        return;
    }
    let end = off.saturating_add(chunk_bytes(max_bulk)).min(blob.len());
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
    let start = chunk_start(it, args[3].len());
    let (exp, mut next) = match kind_of(store, &args[1]) {
        Kind::Other => return resp::error(out, WRONGTYPE),
        Kind::Absent => match start {
            Some(0) => (0, Vec::new()),
            _ => return resp::error(out, BAD_DATA),
        },
        Kind::Filter => match store.get_typed(&args[1]) {
            Some((KIND_BLOOM, e, v)) if start == Some(v.len()) => (e, v.to_vec()),
            _ => return resp::error(out, BAD_DATA),
        },
    };
    if next.len() + args[3].len() > MAX_BLOB_BYTES {
        return resp::error(out, BAD_DATA);
    }
    next.extend_from_slice(&args[3]);
    if next.len() < HDR || matches!(shape(&next), Shape::Bad) {
        return resp::error(out, BAD_DATA);
    }
    if store.set_typed(&args[1], &next, remaining_ttl(exp), KIND_BLOOM) {
        resp::simple(out, "OK");
    } else {
        resp::error(out, crate::server::OOM_ERR);
    }
}

struct CSub {
    buckets: u64,
    data: usize,
}

struct Cuckoo {
    bucket: u32,
    maxiter: u32,
    expansion: u32,
    items: u64,
    deletes: u64,
    subs: Vec<CSub>,
}

enum CShape {
    Ok(Cuckoo),
    Partial,
    Bad,
}

fn cshape(blob: &[u8]) -> CShape {
    match blob.first() {
        None => return CShape::Partial,
        Some(&CTAG) => {}
        Some(_) => return CShape::Bad,
    }
    if blob.len() < C_HDR {
        return CShape::Partial;
    }
    let bucket = rd_u32(blob, C_OFF_BUCKET).unwrap_or(0);
    let maxiter = rd_u32(blob, C_OFF_MAXITER).unwrap_or(0);
    let expansion = rd_u32(blob, C_OFF_EXPANSION).unwrap_or(u32::MAX);
    let items = rd_u64(blob, C_OFF_ITEMS).unwrap_or(0);
    let deletes = rd_u64(blob, C_OFF_DELETES).unwrap_or(0);
    let filters = rd_u32(blob, C_OFF_FILTERS).unwrap_or(0) as usize;
    if bucket == 0
        || bucket > CF_MAX_BUCKET
        || maxiter == 0
        || maxiter > CF_MAX_MAXITER
        || expansion > CF_MAX_EXPANSION
        || filters == 0
        || filters > CF_MAX_FILTERS
    {
        return CShape::Bad;
    }
    let mut subs = Vec::with_capacity(filters);
    let mut at = C_HDR;
    for _ in 0..filters {
        if blob.len() < at + C_SUB_HDR {
            return CShape::Partial;
        }
        let buckets = rd_u64(blob, at).unwrap_or(0);
        if !(2..=MAX_CAPACITY).contains(&buckets) || !buckets.is_power_of_two() {
            return CShape::Bad;
        }
        let data = at + C_SUB_HDR;
        let end = data.saturating_add((buckets * bucket as u64) as usize);
        if end > MAX_BLOB_BYTES {
            return CShape::Bad;
        }
        if blob.len() < end {
            return CShape::Partial;
        }
        subs.push(CSub { buckets, data });
        at = end;
    }
    let slots = subs.iter().map(|s| s.buckets).sum::<u64>() * bucket as u64;
    if at != blob.len() || items > slots || deletes > MAX_COUNTER {
        return CShape::Bad;
    }
    CShape::Ok(Cuckoo {
        bucket,
        maxiter,
        expansion,
        items,
        deletes,
        subs,
    })
}

fn cf_parse(blob: &[u8]) -> Option<Cuckoo> {
    match cshape(blob) {
        CShape::Ok(c) => Some(c),
        _ => None,
    }
}

fn cf_buckets(capacity: u64, bucket: u32) -> u64 {
    (capacity / bucket as u64).max(2).next_power_of_two()
}

fn xorshift(s: &mut u64) -> u64 {
    *s ^= *s << 13;
    *s ^= *s >> 7;
    *s ^= *s << 17;
    *s
}

fn cf_parts(item: &[u8]) -> (u64, u8) {
    let (h1, h2) = item_hashes(item);
    let fp = (h2 & 0xff) as u8;
    (h1, if fp == 0 { 1 } else { fp })
}

fn cf_alt(i: u64, fp: u8, buckets: u64) -> u64 {
    i ^ ((murmur64a(&[fp], MURMUR_SEED) & (buckets - 1)) | 1)
}

fn cf_slot(data: &[u8], i: u64, bucket: u32, want: u8) -> Option<usize> {
    let at = (i * bucket as u64) as usize;
    data[at..at + bucket as usize]
        .iter()
        .position(|&b| b == want)
        .map(|k| at + k)
}

fn cf_matches(data: &[u8], i: u64, bucket: u32, fp: u8) -> u64 {
    let at = (i * bucket as u64) as usize;
    data[at..at + bucket as usize].iter().filter(|&&b| b == fp).count() as u64
}

fn cf_sub_bytes(c: &Cuckoo, s: &CSub) -> usize {
    (s.buckets * c.bucket as u64) as usize
}

fn cf_sub_place(data: &mut [u8], buckets: u64, bucket: u32, h: u64, fp: u8) -> bool {
    let i1 = h & (buckets - 1);
    if let Some(k) = cf_slot(data, i1, bucket, 0) {
        data[k] = fp;
        return true;
    }
    let i2 = cf_alt(i1, fp, buckets);
    if let Some(k) = cf_slot(data, i2, bucket, 0) {
        data[k] = fp;
        return true;
    }
    false
}

fn cf_sub_kick(data: &mut [u8], buckets: u64, bucket: u32, maxiter: u32, h: u64, fp: u8) -> bool {
    let i1 = h & (buckets - 1);
    let i2 = cf_alt(i1, fp, buckets);
    let mut seed = h | 1;
    let mut i = if xorshift(&mut seed) & 1 == 0 { i1 } else { i2 };
    let mut cur = fp;
    let mut trail: Vec<(usize, u8)> = Vec::new();
    for _ in 0..maxiter {
        let slot = (xorshift(&mut seed) % bucket as u64) as usize;
        let at = (i * bucket as u64) as usize + slot;
        let victim = data[at];
        data[at] = cur;
        trail.push((at, victim));
        cur = victim;
        i = cf_alt(i, cur, buckets);
        if let Some(k) = cf_slot(data, i, bucket, 0) {
            data[k] = cur;
            return true;
        }
    }
    for (at, victim) in trail.into_iter().rev() {
        data[at] = victim;
    }
    false
}

fn cf_has(blob: &[u8], c: &Cuckoo, h: u64, fp: u8) -> bool {
    c.subs.iter().any(|s| {
        let data = &blob[s.data..s.data + cf_sub_bytes(c, s)];
        let i1 = h & (s.buckets - 1);
        cf_slot(data, i1, c.bucket, fp).is_some()
            || cf_slot(data, cf_alt(i1, fp, s.buckets), c.bucket, fp).is_some()
    })
}

fn cf_count_blob(blob: &[u8], c: &Cuckoo, h: u64, fp: u8) -> u64 {
    let mut n = 0;
    for s in &c.subs {
        let data = &blob[s.data..s.data + cf_sub_bytes(c, s)];
        let i1 = h & (s.buckets - 1);
        let i2 = cf_alt(i1, fp, s.buckets);
        n += cf_matches(data, i1, c.bucket, fp) + cf_matches(data, i2, c.bucket, fp);
    }
    n
}

fn cf_push_sub(out: &mut Vec<u8>, buckets: u64, bucket: u32) -> Option<()> {
    let bytes = (buckets * bucket as u64) as usize;
    if out.len() + C_SUB_HDR + bytes > MAX_BLOB_BYTES {
        return None;
    }
    out.extend_from_slice(&buckets.to_le_bytes());
    out.resize(out.len() + bytes, 0);
    Some(())
}

fn cf_new_blob(spec: &CSpec) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(C_HDR + C_SUB_HDR);
    out.push(CTAG);
    out.extend_from_slice(&spec.bucket.to_le_bytes());
    out.extend_from_slice(&spec.maxiter.to_le_bytes());
    out.extend_from_slice(&spec.expansion.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes());
    out.extend_from_slice(&1u32.to_le_bytes());
    cf_push_sub(&mut out, cf_buckets(spec.capacity, spec.bucket), spec.bucket)?;
    Some(out)
}

fn cf_grown(blob: &[u8]) -> Option<Vec<u8>> {
    let c = cf_parse(blob)?;
    let last = c.subs.last()?;
    let buckets = last
        .buckets
        .saturating_mul(c.expansion.max(1) as u64)
        .min(MAX_CAPACITY)
        .next_power_of_two();
    let mut out = blob.to_vec();
    cf_push_sub(&mut out, buckets, c.bucket)?;
    out[C_OFF_FILTERS..C_HDR].copy_from_slice(&((c.subs.len() + 1) as u32).to_le_bytes());
    Some(out)
}

fn cf_add_in_place(blob: &mut [u8], item: &[u8], nx: bool) -> Add {
    let c = match cf_parse(blob) {
        Some(c) => c,
        None => return Add::Bad,
    };
    let (h, fp) = cf_parts(item);
    if nx && cf_has(blob, &c, h, fp) {
        return Add::Present;
    }
    let mut placed = false;
    for s in c.subs.iter().rev() {
        let end = s.data + cf_sub_bytes(&c, s);
        if cf_sub_place(&mut blob[s.data..end], s.buckets, c.bucket, h, fp) {
            placed = true;
            break;
        }
    }
    if !placed {
        let last = match c.subs.last() {
            Some(s) => s,
            None => return Add::Bad,
        };
        let end = last.data + cf_sub_bytes(&c, last);
        placed = cf_sub_kick(
            &mut blob[last.data..end],
            last.buckets,
            c.bucket,
            c.maxiter,
            h,
            fp,
        );
    }
    if !placed {
        if c.expansion == 0 {
            return Add::Full;
        }
        if c.subs.len() >= CF_MAX_FILTERS {
            return Add::MaxGrow;
        }
        return Add::Grow;
    }
    blob[C_OFF_ITEMS..C_OFF_DELETES].copy_from_slice(&c.items.saturating_add(1).to_le_bytes());
    Add::Added
}

fn cf_del_in_place(blob: &mut [u8], item: &[u8]) -> Option<bool> {
    let c = cf_parse(blob)?;
    let (h, fp) = cf_parts(item);
    let mut hit = None;
    for s in &c.subs {
        let data = &blob[s.data..s.data + cf_sub_bytes(&c, s)];
        let i1 = h & (s.buckets - 1);
        let i2 = cf_alt(i1, fp, s.buckets);
        if let Some(k) = cf_slot(data, i1, c.bucket, fp).or_else(|| cf_slot(data, i2, c.bucket, fp))
        {
            hit = Some(s.data + k);
            break;
        }
    }
    match hit {
        None => Some(false),
        Some(at) => {
            blob[at] = 0;
            blob[C_OFF_ITEMS..C_OFF_DELETES]
                .copy_from_slice(&c.items.saturating_sub(1).to_le_bytes());
            blob[C_OFF_DELETES..C_OFF_FILTERS]
                .copy_from_slice(&c.deletes.saturating_add(1).to_le_bytes());
            Some(true)
        }
    }
}

struct CSpec {
    capacity: u64,
    bucket: u32,
    maxiter: u32,
    expansion: u32,
}

impl Default for CSpec {
    fn default() -> CSpec {
        CSpec {
            capacity: CF_DEFAULT_CAPACITY,
            bucket: CF_DEFAULT_BUCKET,
            maxiter: CF_DEFAULT_MAXITER,
            expansion: CF_DEFAULT_EXPANSION,
        }
    }
}

fn cf_kind_of(store: &Store, key: &[u8]) -> Kind {
    match store.get_typed(key) {
        None => Kind::Absent,
        Some((KIND_CUCKOO, _, _)) => Kind::Filter,
        Some(_) => Kind::Other,
    }
}

fn cuckoo_blob_bytes(capacity: u64, bucket: u32) -> usize {
    C_HDR + C_SUB_HDR + (cf_buckets(capacity, bucket) * bucket as u64) as usize
}

fn cf_create(store: &Store, key: &[u8], spec: &CSpec) -> bool {
    if !store.can_hold(cuckoo_blob_bytes(spec.capacity, spec.bucket)) {
        return false;
    }
    match cf_new_blob(spec) {
        Some(b) => store.set_typed(key, &b, 0, KIND_CUCKOO),
        None => false,
    }
}

fn cf_prepare(store: &Store, key: &[u8], spec: &CSpec, nocreate: bool, out: &mut Vec<u8>) -> bool {
    match cf_kind_of(store, key) {
        Kind::Filter => true,
        Kind::Other => {
            resp::error(out, WRONGTYPE);
            false
        }
        Kind::Absent => {
            if nocreate {
                resp::error(out, NOT_FOUND);
                return false;
            }
            if !cf_create(store, key, spec) {
                resp::error(out, crate::server::OOM_ERR);
                return false;
            }
            true
        }
    }
}

fn cf_add_item(store: &Store, key: &[u8], item: &[u8], nx: bool, spec: &CSpec) -> Add {
    let first = match store.with_value_mut(key, |v| cf_add_in_place(v, item, nx)) {
        Some(r) => Some(r),
        None => {
            if !cf_create(store, key, spec) {
                return Add::Oom;
            }
            store.with_value_mut(key, |v| cf_add_in_place(v, item, nx))
        }
    };
    let r = match first {
        Some(r) => r,
        None => return Add::Oom,
    };
    if r != Add::Grow {
        return r;
    }
    let (exp, blob) = match store.get_typed(key) {
        Some((KIND_CUCKOO, exp, v)) => (exp, v.to_vec()),
        _ => return Add::Bad,
    };
    let mut next = match cf_grown(&blob) {
        Some(b) => b,
        None => return Add::Oom,
    };
    let r = cf_add_in_place(&mut next, item, nx);
    if r == Add::Bad {
        return Add::Bad;
    }
    if !store.set_typed(key, &next, remaining_ttl(exp), KIND_CUCKOO) {
        return Add::Oom;
    }
    r
}

fn cf_add_error(out: &mut Vec<u8>, r: &Add) -> bool {
    match r {
        Add::Full => resp::error(out, CF_FULL),
        Add::MaxGrow => resp::error(out, MAX_EXPANSIONS),
        Add::Oom => resp::error(out, crate::server::OOM_ERR),
        Add::Bad => resp::error(out, CF_INVALID_HEADER),
        _ => return false,
    }
    true
}

fn cf_opts(cmd: &[u8], args: &[Vec<u8>], from: usize, spec: &mut CSpec, out: &mut Vec<u8>) -> bool {
    let mut seen = [false; 3];
    let mut i = from;
    while i < args.len() {
        let (which, text, range, lo, hi) = if eq(&args[i], "BUCKETSIZE") {
            (0usize, CF_PARSE_BUCKETSIZE, CF_BUCKETSIZE_RANGE, 1i64, CF_MAX_BUCKET as i64)
        } else if eq(&args[i], "MAXITERATIONS") {
            (1, CF_PARSE_MAXITER, CF_MAXITER_RANGE, 1, CF_MAX_MAXITER as i64)
        } else if eq(&args[i], "EXPANSION") {
            (2, CF_PARSE_EXPANSION, CF_EXPANSION_RANGE, 0, CF_MAX_EXPANSION as i64)
        } else {
            i += 1;
            continue;
        };
        if i + 1 >= args.len() {
            arity(out, cmd);
            return false;
        }
        let v = match arg_i64(&args[i + 1]) {
            Some(v) => v,
            None => {
                resp::error(out, text);
                return false;
            }
        };
        if v < lo || v > hi {
            resp::error(out, range);
            return false;
        }
        if !seen[which] {
            seen[which] = true;
            match which {
                0 => spec.bucket = v as u32,
                1 => spec.maxiter = v as u32,
                _ => spec.expansion = v as u32,
            }
        }
        i += 2;
    }
    true
}

fn cf_reserve(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() < 3 {
        return arity(out, cmd);
    }
    let capacity = match arg_i64(&args[2]) {
        Some(v) => v,
        None => return resp::error(out, INSERT_BAD_CAPACITY),
    };
    if (args.len() - 3) % 2 != 0 {
        return arity(out, cmd);
    }
    let mut spec = CSpec::default();
    if !cf_opts(cmd, args, 3, &mut spec, out) {
        return;
    }
    if capacity < 2 * spec.bucket as i64 || capacity as u64 > MAX_CAPACITY {
        return resp::error(out, CF_CAPACITY_RANGE);
    }
    spec.capacity = capacity as u64;
    match cf_kind_of(store, &args[1]) {
        Kind::Other => resp::error(out, WRONGTYPE),
        Kind::Filter => resp::error(out, ITEM_EXISTS),
        Kind::Absent => {
            if cf_create(store, &args[1], &spec) {
                resp::simple(out, "OK");
            } else {
                resp::error(out, crate::server::OOM_ERR);
            }
        }
    }
}

fn cf_add(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool, nx: bool) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    if !cf_prepare(store, &args[1], &CSpec::default(), false, out) {
        return;
    }
    let r = cf_add_item(store, &args[1], &args[2], nx, &CSpec::default());
    if !cf_add_error(out, &r) {
        resp::boolean(out, r == Add::Added, resp3);
    }
}

fn cf_insert(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool, nx: bool) {
    if args.len() < 4 {
        return arity(out, cmd);
    }
    let mut spec = CSpec::default();
    let mut capacity: i64 = CF_DEFAULT_CAPACITY as i64;
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
        } else if eq(&args[i], "CAPACITY") {
            if i + 1 >= args.len() {
                return arity(out, cmd);
            }
            capacity = match arg_i64(&args[i + 1]) {
                Some(v) => v,
                None => return resp::error(out, INSERT_BAD_CAPACITY),
            };
            i += 2;
        } else {
            return resp::error(out, UNKNOWN_ARG);
        }
    }
    if capacity < 2 * spec.bucket as i64 || capacity as u64 > MAX_CAPACITY {
        return resp::error(out, CF_INSERT_CAPACITY_RANGE);
    }
    spec.capacity = capacity as u64;
    let items = &args[i + 1..];
    if items.is_empty() {
        return arity(out, cmd);
    }
    if !cf_prepare(store, &args[1], &spec, nocreate, out) {
        return;
    }
    let mut results = Vec::with_capacity(items.len());
    for item in items {
        let r = cf_add_item(store, &args[1], item, nx, &spec);
        match r {
            Add::Added => results.push(1i64),
            Add::Present => results.push(0),
            Add::Full | Add::MaxGrow => results.push(-1),
            _ => {
                cf_add_error(out, &r);
                return;
            }
        }
    }
    resp::array_header(out, results.len());
    for v in results {
        if nx || !resp3 {
            resp::integer(out, v);
        } else {
            resp::boolean(out, v == 1, true);
        }
    }
}

fn cf_contains(store: &Store, key: &[u8], item: &[u8]) -> bool {
    let blob = match store.get_typed(key) {
        Some((KIND_CUCKOO, _, v)) => v,
        _ => return false,
    };
    let c = match cf_parse(blob) {
        Some(c) => c,
        None => return false,
    };
    let (h, fp) = cf_parts(item);
    cf_has(blob, &c, h, fp)
}

fn cf_occurrences(store: &Store, key: &[u8], item: &[u8]) -> u64 {
    let blob = match store.get_typed(key) {
        Some((KIND_CUCKOO, _, v)) => v,
        _ => return 0,
    };
    let c = match cf_parse(blob) {
        Some(c) => c,
        None => return 0,
    };
    let (h, fp) = cf_parts(item);
    cf_count_blob(blob, &c, h, fp)
}

fn cf_readable(store: &Store, key: &[u8]) -> bool {
    match store.get_typed(key) {
        Some((KIND_CUCKOO, _, v)) => cf_parse(v).is_some(),
        _ => true,
    }
}

fn cf_exists(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    if !cf_readable(store, &args[1]) {
        return resp::error(out, CF_INVALID_HEADER);
    }
    resp::boolean(out, cf_contains(store, &args[1], &args[2]), resp3);
}

fn cf_mexists(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() < 3 {
        return arity(out, cmd);
    }
    if !cf_readable(store, &args[1]) {
        return resp::error(out, CF_INVALID_HEADER);
    }
    resp::array_header(out, args.len() - 2);
    for item in &args[2..] {
        resp::boolean(out, cf_contains(store, &args[1], item), resp3);
    }
}

fn cf_count(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    if !cf_readable(store, &args[1]) {
        return resp::error(out, CF_INVALID_HEADER);
    }
    resp::integer(out, cf_occurrences(store, &args[1], &args[2]) as i64);
}

fn cf_del(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    if !matches!(cf_kind_of(store, &args[1]), Kind::Filter) {
        return resp::error(out, CF_NOT_FOUND);
    }
    match store.with_value_mut(&args[1], |v| cf_del_in_place(v, &args[2])) {
        Some(Some(hit)) => resp::boolean(out, hit, resp3),
        _ => resp::error(out, CF_INVALID_HEADER),
    }
}

fn cf_info(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>, resp3: bool) {
    if args.len() != 2 {
        return arity(out, cmd);
    }
    let blob = match store.get_typed(&args[1]) {
        Some((KIND_CUCKOO, _, v)) => v,
        Some(_) => return resp::error(out, WRONGTYPE),
        None => return resp::error(out, NOT_FOUND),
    };
    let c = match cf_parse(blob) {
        Some(c) => c,
        None => return resp::error(out, CF_INVALID_HEADER),
    };
    resp::map_header(out, 8, resp3);
    info_field(out, "Size", Some(blob.len() as i64), resp3);
    info_field(out, "Number of buckets", Some(c.subs[0].buckets as i64), resp3);
    info_field(out, "Number of filters", Some(c.subs.len() as i64), resp3);
    info_field(out, "Number of items inserted", Some(c.items as i64), resp3);
    info_field(out, "Number of items deleted", Some(c.deletes as i64), resp3);
    info_field(out, "Bucket size", Some(c.bucket as i64), resp3);
    info_field(out, "Expansion rate", Some(c.expansion as i64), resp3);
    info_field(out, "Max iterations", Some(c.maxiter as i64), resp3);
}

fn cf_scandump(
    store: &Store,
    cmd: &[u8],
    args: &[Vec<u8>],
    out: &mut Vec<u8>,
    resp3: bool,
    max_bulk: usize,
) {
    if args.len() != 3 {
        return arity(out, cmd);
    }
    let it = match arg_i64(&args[2]) {
        Some(v) if v >= 0 => v,
        _ => return resp::error(out, CF_INVALID_POSITION),
    };
    let blob = match store.get_typed(&args[1]) {
        Some((KIND_CUCKOO, _, v)) => v,
        Some(_) => return resp::error(out, WRONGTYPE),
        None => return resp::error(out, NOT_FOUND),
    };
    let off = if it == 0 { 0 } else { (it - 1) as usize };
    resp::array_header(out, 2);
    if off >= blob.len() {
        resp::integer(out, 0);
        resp::null(out, resp3);
        return;
    }
    let end = off.saturating_add(chunk_bytes(max_bulk)).min(blob.len());
    resp::integer(out, (end + 1) as i64);
    resp::bulk(out, &blob[off..end]);
}

fn cf_loadchunk(store: &Store, cmd: &[u8], args: &[Vec<u8>], out: &mut Vec<u8>) {
    if args.len() != 4 {
        return arity(out, cmd);
    }
    let it = match arg_i64(&args[2]) {
        Some(v) if v >= 1 => v,
        _ => return resp::error(out, CF_INVALID_POSITION),
    };
    let start = chunk_start(it, args[3].len());
    let (exp, mut next) = match cf_kind_of(store, &args[1]) {
        Kind::Other => return resp::error(out, WRONGTYPE),
        Kind::Absent => match start {
            Some(0) => (0, Vec::new()),
            Some(_) => return resp::error(out, CF_INVALID_POSITION),
            None => return resp::error(out, CF_INVALID_HEADER),
        },
        Kind::Filter => match store.get_typed(&args[1]) {
            Some((KIND_CUCKOO, e, v)) if start == Some(v.len()) => (e, v.to_vec()),
            _ => return resp::error(out, ITEM_EXISTS),
        },
    };
    if next.len() + args[3].len() > MAX_BLOB_BYTES {
        return resp::error(out, CF_INVALID_HEADER);
    }
    next.extend_from_slice(&args[3]);
    if next.len() < C_HDR || matches!(cshape(&next), CShape::Bad) {
        return resp::error(out, CF_INVALID_HEADER);
    }
    if store.set_typed(&args[1], &next, remaining_ttl(exp), KIND_CUCKOO) {
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
        dispatch(store, &cmd, &args, &mut out, false, CHUNK_BYTES);
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
            dispatch(&s, b"BF.SCANDUMP", &args, &mut out, false, CHUNK_BYTES);
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

    fn bf_dump(s: &Store, key: &str) -> Vec<(i64, Vec<u8>)> {
        let mut it = 0i64;
        let mut chunks = Vec::new();
        loop {
            let args = vec![
                b"BF.SCANDUMP".to_vec(),
                key.as_bytes().to_vec(),
                it.to_string().into_bytes(),
            ];
            let mut out = Vec::new();
            dispatch(s, b"BF.SCANDUMP", &args, &mut out, false, CHUNK_BYTES);
            let (next, data) = decode_scandump(&out);
            if next == 0 {
                assert!(data.is_empty());
                return chunks;
            }
            chunks.push((next, data));
            it = next;
            assert!(chunks.len() <= 8, "the chunk loop did not terminate");
        }
    }

    #[test]
    fn a_dump_round_trips_through_scandump_and_loadchunk() {
        let s = Store::create("t_bloom_dump", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "src", "0.01", "200"]), "+OK\r\n");
        for i in 0..50u32 {
            run(&s, &["BF.ADD", "src", &format!("d{i}")]);
        }
        let chunks = bf_dump(&s, "src");
        assert_eq!(chunks.len(), 1);
        for (it, data) in &chunks {
            let load = vec![
                b"BF.LOADCHUNK".to_vec(),
                b"dst".to_vec(),
                it.to_string().into_bytes(),
                data.clone(),
            ];
            let mut out = Vec::new();
            dispatch(&s, b"BF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
            assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");
        }
        assert_eq!(
            s.get_typed(b"dst").unwrap().2,
            s.get_typed(b"src").unwrap().2
        );
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
    fn a_multi_chunk_bloom_dump_reloads_through_the_client_loop() {
        let s = Store::create("t_bloom_reload", &cfg(64 * 1024 * 1024)).unwrap();
        let blob = new_blob(0.01, 15_000_000, 2, false).unwrap();
        assert!(blob.len() > CHUNK_BYTES, "the dump must need two chunks");
        assert!(s.set_typed(b"src", &blob, 0, KIND_BLOOM));
        let chunks = bf_dump(&s, "src");
        assert_eq!(chunks.len(), 2);
        for (it, data) in &chunks {
            let load = vec![
                b"BF.LOADCHUNK".to_vec(),
                b"dst".to_vec(),
                it.to_string().into_bytes(),
                data.clone(),
            ];
            let mut out = Vec::new();
            dispatch(&s, b"BF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
            assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");
        }
        assert_eq!(s.get_typed(b"dst").unwrap().2, blob);
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
        assert_eq!(run(&s, &["BF.MADD", "ns", "a", "b"]), format!("*1\r\n-{FILTER_FULL}\r\n"));
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
        dispatch(&s, b"BF.INFO", &args, &mut out, true, CHUNK_BYTES);
        assert!(String::from_utf8_lossy(&out).starts_with("%5\r\n+Capacity\r\n:100\r\n"));

        let args: Vec<Vec<u8>> = vec![b"BF.INFO".to_vec(), b"b".to_vec(), b"CAPACITY".to_vec()];
        let mut out = Vec::new();
        dispatch(&s, b"BF.INFO", &args, &mut out, true, CHUNK_BYTES);
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
            dispatch(&s, &cmd, &args, &mut out, true, CHUNK_BYTES);
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
        assert_eq!(crate::store::type_name(KIND_BLOOM), "MBbloom--");
        assert_eq!(crate::store::type_name(KIND_CUCKOO), "MBbloomCF");
        assert_eq!(crate::store::encoding_name(KIND_BLOOM, b""), b"raw");
        assert_eq!(crate::store::encoding_name(KIND_CUCKOO, b""), b"raw");
    }

    fn place(data: &mut [u8], buckets: u64, bucket: u32, maxiter: u32, h: u64, fp: u8) -> bool {
        cf_sub_place(data, buckets, bucket, h, fp)
            || cf_sub_kick(data, buckets, bucket, maxiter, h, fp)
    }

    fn cspec(capacity: u64) -> CSpec {
        CSpec {
            capacity,
            ..CSpec::default()
        }
    }

    fn decode_cf_scandump(out: &[u8]) -> (i64, Option<Vec<u8>>) {
        let nl = out.iter().position(|&c| c == b'\n').unwrap();
        let rest = &out[nl + 1..];
        assert_eq!(rest[0], b':');
        let nl2 = rest.iter().position(|&c| c == b'\n').unwrap();
        let next: i64 = std::str::from_utf8(&rest[1..nl2 - 1]).unwrap().parse().unwrap();
        let body = &rest[nl2 + 1..];
        assert_eq!(body[0], b'$');
        let nl3 = body.iter().position(|&c| c == b'\n').unwrap();
        let len: i64 = std::str::from_utf8(&body[1..nl3 - 1]).unwrap().parse().unwrap();
        if len < 0 {
            return (next, None);
        }
        (next, Some(body[nl3 + 1..nl3 + 1 + len as usize].to_vec()))
    }

    fn cf_dump(s: &Store, key: &str) -> Vec<(i64, Vec<u8>)> {
        let mut it = 0i64;
        let mut chunks = Vec::new();
        loop {
            let args = vec![
                b"CF.SCANDUMP".to_vec(),
                key.as_bytes().to_vec(),
                it.to_string().into_bytes(),
            ];
            let mut out = Vec::new();
            dispatch(s, b"CF.SCANDUMP", &args, &mut out, false, CHUNK_BYTES);
            let (next, data) = decode_cf_scandump(&out);
            if next == 0 {
                assert!(data.is_none(), "the terminal chunk must be a nil bulk");
                return chunks;
            }
            chunks.push((next, data.unwrap()));
            it = next;
            assert!(chunks.len() <= 8, "the chunk loop did not terminate");
        }
    }

    #[test]
    fn a_cuckoo_header_and_its_sub_filters_survive_a_round_trip() {
        let blob = cf_new_blob(&CSpec {
            capacity: 1000,
            bucket: 4,
            maxiter: 7,
            expansion: 2,
        })
        .unwrap();
        let c = cf_parse(&blob).expect("a fresh cuckoo blob must parse");
        assert_eq!(c.bucket, 4);
        assert_eq!(c.maxiter, 7);
        assert_eq!(c.expansion, 2);
        assert_eq!(c.items, 0);
        assert_eq!(c.deletes, 0);
        assert_eq!(c.subs.len(), 1);
        assert_eq!(c.subs[0].buckets, 256);
        assert_eq!(blob.len(), C_HDR + C_SUB_HDR + 256 * 4);

        let two = cf_grown(&blob).unwrap();
        let g = cf_parse(&two).expect("a grown cuckoo blob must parse");
        assert_eq!(g.subs.len(), 2);
        assert_eq!(g.subs[1].buckets, 512);
        assert_eq!(g.bucket, 4);
        assert!(matches!(cshape(&two[..C_HDR - 1]), CShape::Partial));
        assert!(matches!(cshape(&[TAG, 0, 0]), CShape::Bad));
        assert!(cf_parse(&new_blob(0.01, 100, 2, false).unwrap()).is_none());
    }

    #[test]
    fn capacity_rounds_up_to_a_power_of_two_of_buckets() {
        assert_eq!(cf_buckets(100, 2), 64);
        assert_eq!(cf_buckets(1000, 2), 512);
        assert_eq!(cf_buckets(1024, 2), 512);
        assert_eq!(cf_buckets(1500, 2), 1024);
        assert_eq!(cf_buckets(4, 2), 2);
        assert_eq!(cf_buckets(5, 2), 2);
        assert_eq!(cf_buckets(100, 4), 32);
        assert_eq!(cf_buckets(16, 8), 2);
    }

    #[test]
    fn a_reserved_cuckoo_filter_of_one_thousand_pins_its_byte_layout() {
        let s = Store::create("t_cf_size", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "k", "1000"]), "+OK\r\n");
        assert_eq!(s.get_typed(b"k").unwrap().2.len(), 1065);
        assert_eq!(
            run(&s, &["CF.INFO", "k"]),
            "*16\r\n+Size\r\n:1065\r\n+Number of buckets\r\n:512\r\n+Number of filters\r\n:1\r\n\
             +Number of items inserted\r\n:0\r\n+Number of items deleted\r\n:0\r\n\
             +Bucket size\r\n:2\r\n+Expansion rate\r\n:1\r\n+Max iterations\r\n:20\r\n"
        );
    }

    #[test]
    fn an_added_cuckoo_item_is_reported_as_present() {
        let s = Store::create("t_cf_add", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.ADD", "f", "hello"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "f", "hello"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "f", "never-added"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "hello"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.ADD", "f", "hello"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "hello"]), ":2\r\n");
        assert_eq!(run(&s, &["CF.MEXISTS", "f", "hello", "zz"]), "*2\r\n:1\r\n:0\r\n");
        assert_eq!(run(&s, &["TYPE"]), "-ERR unknown command 'TYPE'\r\n");
    }

    #[test]
    fn addnx_refuses_a_duplicate_and_add_does_not() {
        let s = Store::create("t_cf_addnx", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.ADDNX", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.ADDNX", "f", "a"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.ADD", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "a"]), ":2\r\n");
        assert_eq!(run(&s, &["CF.INSERTNX", "f", "ITEMS", "a", "b"]), "*2\r\n:0\r\n:1\r\n");
        assert_eq!(run(&s, &["CF.INSERT", "f", "ITEMS", "b", "c"]), "*2\r\n:1\r\n:1\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "b"]), ":2\r\n");
    }

    #[test]
    fn delete_removes_one_copy_and_the_counters_follow() {
        let s = Store::create("t_cf_del", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.ADD", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.ADD", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.ADD", "f", "b"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.DEL", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.DEL", "f", "a"]), ":1\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "f", "a"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "f", "a"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.DEL", "f", "a"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "f", "b"]), ":1\r\n");
        assert_eq!(
            run(&s, &["CF.INFO", "f"]),
            "*16\r\n+Size\r\n:1065\r\n+Number of buckets\r\n:512\r\n+Number of filters\r\n:1\r\n\
             +Number of items inserted\r\n:1\r\n+Number of items deleted\r\n:2\r\n\
             +Bucket size\r\n:2\r\n+Expansion rate\r\n:1\r\n+Max iterations\r\n:20\r\n"
        );
    }

    #[test]
    fn a_deleted_item_never_comes_back_when_other_items_arrive() {
        let s = Store::create("t_cf_resurrect", &cfg(4 * 1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "f", "100000"]), "+OK\r\n");
        for i in 0..400u32 {
            assert_eq!(run(&s, &["CF.ADD", "f", &format!("old-{i}")]), ":1\r\n");
        }
        for i in 0..200u32 {
            assert_eq!(run(&s, &["CF.DEL", "f", &format!("old-{i}")]), ":1\r\n");
        }
        for i in 0..200u32 {
            assert_eq!(
                run(&s, &["CF.EXISTS", "f", &format!("old-{i}")]),
                ":0\r\n",
                "old-{i} survived its own delete"
            );
        }
        for i in 0..400u32 {
            assert_eq!(run(&s, &["CF.ADD", "f", &format!("new-{i}")]), ":1\r\n");
        }
        for i in 0..200u32 {
            assert_eq!(
                run(&s, &["CF.EXISTS", "f", &format!("old-{i}")]),
                ":0\r\n",
                "old-{i} was resurrected by later adds"
            );
        }
        for i in 200..400u32 {
            assert_eq!(
                run(&s, &["CF.EXISTS", "f", &format!("old-{i}")]),
                ":1\r\n",
                "old-{i} was lost"
            );
        }
    }

    #[test]
    fn kick_outs_hold_more_items_than_direct_placement() {
        let items: Vec<String> = (0..168).map(|i| format!("kick-{i}")).collect();
        let mut direct = 0;
        let mut table = vec![0u8; 128];
        for it in &items {
            let (h, fp) = cf_parts(it.as_bytes());
            if place(&mut table, 64, 2, 0, h, fp) {
                direct += 1;
            }
        }
        let mut placed = Vec::new();
        let mut table = vec![0u8; 128];
        for it in &items {
            let (h, fp) = cf_parts(it.as_bytes());
            if place(&mut table, 64, 2, 20, h, fp) {
                placed.push(it.clone());
            }
        }
        assert!(
            placed.len() > direct,
            "kick-outs placed {} items, direct placement placed {direct}",
            placed.len()
        );
        for it in &placed {
            let (h, fp) = cf_parts(it.as_bytes());
            let i1 = h & 63;
            let i2 = cf_alt(i1, fp, 64);
            assert!(
                cf_matches(&table, i1, 2, fp) + cf_matches(&table, i2, 2, fp) > 0,
                "{it} was lost by a kick-out"
            );
        }
        assert_eq!(table.iter().filter(|&&b| b != 0).count(), placed.len());
    }

    #[test]
    fn a_failed_kick_out_leaves_every_earlier_item_in_place() {
        let mut table = vec![0u8; 128];
        let mut placed = Vec::new();
        for i in 0..400u32 {
            let item = format!("fail-{i}");
            let (h, fp) = cf_parts(item.as_bytes());
            if place(&mut table, 64, 2, 20, h, fp) {
                placed.push(item);
            }
        }
        assert!(placed.len() > 100, "a 128 slot table must fill up");
        assert_eq!(table.iter().filter(|&&b| b != 0).count(), placed.len());
        for item in &placed {
            let (h, fp) = cf_parts(item.as_bytes());
            let i1 = h & 63;
            let i2 = cf_alt(i1, fp, 64);
            assert!(
                cf_matches(&table, i1, 2, fp) + cf_matches(&table, i2, 2, fp) > 0,
                "{item} was dropped by a failed insert"
            );
        }
    }

    #[test]
    fn a_full_cuckoo_filter_appends_a_sub_filter_and_keeps_every_item() {
        let s = Store::create("t_cf_grow", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "g", "8"]), "+OK\r\n");
        for i in 0..60u32 {
            assert_eq!(run(&s, &["CF.ADD", "g", &format!("it{i}")]), ":1\r\n");
        }
        let c = cf_parse(s.get_typed(b"g").unwrap().2).unwrap();
        assert!(c.subs.len() > 1, "the filter did not grow");
        assert_eq!(c.items, 60);
        assert_eq!(c.subs[0].buckets, 4);
        for s2 in &c.subs {
            assert_eq!(s2.buckets, 4, "expansion 1 keeps every sub filter the same size");
        }
        let info = run(&s, &["CF.INFO", "g"]);
        assert!(
            info.contains(&format!("+Number of filters\r\n:{}\r\n", c.subs.len())),
            "{info}"
        );
        assert!(info.contains("+Number of buckets\r\n:4\r\n"), "{info}");
        for i in 0..60u32 {
            assert_eq!(
                run(&s, &["CF.EXISTS", "g", &format!("it{i}")]),
                ":1\r\n",
                "it{i} was lost by growth"
            );
        }
    }

    #[test]
    fn expansion_two_doubles_each_new_sub_filter() {
        let s = Store::create("t_cf_grow2", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "g", "8", "EXPANSION", "2"]), "+OK\r\n");
        for i in 0..200u32 {
            assert_eq!(run(&s, &["CF.ADD", "g", &format!("x{i}")]), ":1\r\n");
        }
        let c = cf_parse(s.get_typed(b"g").unwrap().2).unwrap();
        assert!(c.subs.len() > 1);
        for (i, sub) in c.subs.iter().enumerate() {
            assert_eq!(sub.buckets, 4 * 2u64.pow(i as u32));
        }
        for i in 0..200u32 {
            assert_eq!(run(&s, &["CF.EXISTS", "g", &format!("x{i}")]), ":1\r\n");
        }
    }

    #[test]
    fn the_cuckoo_false_positive_rate_stays_under_three_percent() {
        let n = 100_000u32;
        let mut blob = cf_new_blob(&cspec(n as u64)).unwrap();
        let mut added = 0u32;
        for i in 0..n {
            let item = format!("member-{i}");
            loop {
                match cf_add_in_place(&mut blob, item.as_bytes(), false) {
                    Add::Added => {
                        added += 1;
                        break;
                    }
                    Add::Grow => blob = cf_grown(&blob).expect("growth must succeed"),
                    other => panic!("member-{i} was refused ({})", other == Add::Full),
                }
            }
        }
        assert_eq!(added, n);
        let c = cf_parse(&blob).unwrap();
        let mut hits = 0u32;
        for i in 0..n {
            let (h, fp) = cf_parts(format!("absent-{i}").as_bytes());
            if cf_count_blob(&blob, &c, h, fp) > 0 {
                hits += 1;
            }
        }
        let rate = hits as f64 / n as f64;
        assert!(rate < 0.03, "false positive rate {rate} over {n} misses");
        for i in 0..n {
            let (h, fp) = cf_parts(format!("member-{i}").as_bytes());
            assert!(
                cf_count_blob(&blob, &c, h, fp) > 0,
                "member-{i} must still be present"
            );
        }
    }

    fn cf_fill(blob: &mut Vec<u8>, item: &[u8]) {
        loop {
            match cf_add_in_place(blob, item, false) {
                Add::Added => return,
                Add::Grow => *blob = cf_grown(blob).expect("growth must succeed"),
                other => panic!("insert refused ({})", other == Add::Full),
            }
        }
    }

    #[test]
    fn a_right_sized_filter_holds_its_capacity_in_two_sub_filters() {
        let n = 1_000_000u32;
        let mut blob = cf_new_blob(&cspec(n as u64)).unwrap();
        for i in 0..n {
            cf_fill(&mut blob, format!("item-{i}").as_bytes());
        }
        let c = cf_parse(&blob).unwrap();
        assert!(
            c.subs.len() <= 2,
            "{n} items in a capacity-{n} filter needed {} sub filters",
            c.subs.len()
        );
        assert_eq!(c.items, n as u64);
        for i in (0..n).step_by(997) {
            let (h, fp) = cf_parts(format!("item-{i}").as_bytes());
            assert!(cf_has(&blob, &c, h, fp), "item-{i} was lost");
        }
    }

    #[test]
    fn three_million_adds_from_a_million_item_space_stay_under_eight_sub_filters() {
        let space = 1_000_000u64;
        let mut blob = cf_new_blob(&cspec(space)).unwrap();
        let mut seed = 0x9e3779b97f4a7c15u64;
        for _ in 0..3_000_000u32 {
            let k = xorshift(&mut seed) % space;
            cf_fill(&mut blob, format!("item:{k}").as_bytes());
        }
        let c = cf_parse(&blob).unwrap();
        assert!(
            c.subs.len() <= 8,
            "3 000 000 adds needed {} sub filters",
            c.subs.len()
        );
        assert_eq!(c.items, 3_000_000);
    }

    #[test]
    fn cf_add_mutates_the_blob_without_changing_its_length() {
        let s = Store::create("t_cf_inplace", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "f", "10000"]), "+OK\r\n");
        let before = s.get_typed(b"f").unwrap().2.len();
        for i in 0..1000u32 {
            assert_eq!(run(&s, &["CF.ADD", "f", &format!("k{i}")]), ":1\r\n");
            assert_eq!(
                s.get_typed(b"f").unwrap().2.len(),
                before,
                "the blob was rebuilt at item {i}"
            );
        }
        assert_eq!(run(&s, &["CF.DEL", "f", "k0"]), ":1\r\n");
        assert_eq!(s.get_typed(b"f").unwrap().2.len(), before);
        assert_eq!(run(&s, &["CF.INFO", "f", "x"]), "-ERR wrong number of arguments for 'cf.info' command\r\n");
    }

    #[test]
    fn a_cuckoo_dump_round_trips_through_scandump_and_loadchunk() {
        let s = Store::create("t_cf_dump", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "src", "2000"]), "+OK\r\n");
        for i in 0..50u32 {
            assert_eq!(run(&s, &["CF.ADD", "src", &format!("d{i}")]), ":1\r\n");
        }
        let chunks = cf_dump(&s, "src");
        assert_eq!(chunks.len(), 1);
        for (it, data) in &chunks {
            let load = vec![
                b"CF.LOADCHUNK".to_vec(),
                b"dst".to_vec(),
                it.to_string().into_bytes(),
                data.clone(),
            ];
            let mut out = Vec::new();
            dispatch(&s, b"CF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
            assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");
        }
        assert_eq!(
            s.get_typed(b"dst").unwrap().2,
            s.get_typed(b"src").unwrap().2
        );
        for i in 0..50u32 {
            assert_eq!(
                run(&s, &["CF.EXISTS", "dst", &format!("d{i}")]),
                ":1\r\n",
                "d{i} did not survive the dump"
            );
        }
        assert_eq!(run(&s, &["CF.EXISTS", "dst", "never-added"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.LOADCHUNK", "dst", "1", "zz"]), format!("-{ITEM_EXISTS}\r\n"));
    }

    #[test]
    fn a_cuckoo_dump_larger_than_one_chunk_reassembles_byte_for_byte() {
        let s = Store::create("t_cf_chunks", &cfg(64 * 1024 * 1024)).unwrap();
        let blob = cf_new_blob(&cspec(10_000_000)).unwrap();
        assert!(blob.len() > CHUNK_BYTES, "the dump must need two chunks");
        assert!(s.set_typed(b"src", &blob, 0, KIND_CUCKOO));
        let chunks = cf_dump(&s, "src");
        assert_eq!(chunks.len(), 2);
        let mut rebuilt = Vec::new();
        for (_, data) in &chunks {
            rebuilt.extend_from_slice(data);
        }
        assert_eq!(rebuilt, blob);
        for (it, data) in &chunks {
            let load = vec![
                b"CF.LOADCHUNK".to_vec(),
                b"dst".to_vec(),
                it.to_string().into_bytes(),
                data.clone(),
            ];
            let mut out = Vec::new();
            dispatch(&s, b"CF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
            assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");
        }
        assert_eq!(s.get_typed(b"dst").unwrap().2, blob);
    }

    #[test]
    fn the_cuckoo_handlers_answer_with_the_redis_error_texts() {
        let s = Store::create("t_cf_errors", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["CF.RESERVE", "k"]),
            "-ERR wrong number of arguments for 'cf.reserve' command\r\n"
        );
        assert_eq!(run(&s, &["CF.ADD", "k"]), "-ERR wrong number of arguments for 'cf.add' command\r\n");
        assert_eq!(run(&s, &["CF.RESERVE", "k", "abc"]), format!("-{INSERT_BAD_CAPACITY}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "0"]), format!("-{CF_CAPACITY_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "3"]), format!("-{CF_CAPACITY_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "4", "BUCKETSIZE", "8"]), format!("-{CF_CAPACITY_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "1073741825"]), format!("-{CF_CAPACITY_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "abc", "BUCKETSIZE", "abc"]), format!("-{INSERT_BAD_CAPACITY}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "0", "BUCKETSIZE", "abc"]), format!("-{CF_PARSE_BUCKETSIZE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "BUCKETSIZE", "0"]), format!("-{CF_BUCKETSIZE_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "BUCKETSIZE", "256"]), format!("-{CF_BUCKETSIZE_RANGE}\r\n"));
        assert_eq!(
            run(&s, &["CF.RESERVE", "k", "100", "BUCKETSIZE"]),
            "-ERR wrong number of arguments for 'cf.reserve' command\r\n"
        );
        assert_eq!(
            run(&s, &["CF.RESERVE", "k", "100", "MAXITERATIONS"]),
            "-ERR wrong number of arguments for 'cf.reserve' command\r\n"
        );
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "MAXITERATIONS", "abc"]), format!("-{CF_PARSE_MAXITER}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "MAXITERATIONS", "0"]), format!("-{CF_MAXITER_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "MAXITERATIONS", "65536"]), format!("-{CF_MAXITER_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "EXPANSION", "abc"]), format!("-{CF_PARSE_EXPANSION}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "EXPANSION", "-1"]), format!("-{CF_EXPANSION_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "EXPANSION", "32769"]), format!("-{CF_EXPANSION_RANGE}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "BOGUS", "1"]), "+OK\r\n");
        assert_eq!(
            run(&s, &["CF.RESERVE", "k9", "1000", "BOGUS"]),
            "-ERR wrong number of arguments for 'cf.reserve' command\r\n"
        );
        assert_eq!(
            run(&s, &["CF.RESERVE", "k9", "1000", "BOGUS", "1", "2"]),
            "-ERR wrong number of arguments for 'cf.reserve' command\r\n"
        );
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100"]), format!("-{ITEM_EXISTS}\r\n"));
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "BUCKETSIZE", "abc"]), format!("-{CF_PARSE_BUCKETSIZE}\r\n"));

        assert_eq!(run(&s, &["CF.INFO", "missing"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["CF.COUNT", "missing", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "missing", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.DEL", "missing", "x"]), format!("-{CF_NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["CF.SCANDUMP", "missing", "0"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["CF.SCANDUMP", "k", "abc"]), format!("-{CF_INVALID_POSITION}\r\n"));
        assert_eq!(run(&s, &["CF.SCANDUMP", "k", "-5"]), format!("-{CF_INVALID_POSITION}\r\n"));
        assert_eq!(run(&s, &["CF.LOADCHUNK", "d", "abc", "x"]), format!("-{CF_INVALID_POSITION}\r\n"));
        assert_eq!(run(&s, &["CF.LOADCHUNK", "d", "0", "x"]), format!("-{CF_INVALID_POSITION}\r\n"));
        assert_eq!(run(&s, &["CF.LOADCHUNK", "d", "1", "zzzz"]), format!("-{CF_INVALID_HEADER}\r\n"));
        assert_eq!(run(&s, &["CF.LOADCHUNK", "d", "9", "zzzz"]), format!("-{CF_INVALID_POSITION}\r\n"));

        assert_eq!(run(&s, &["CF.INSERT", "n", "NOCREATE", "ITEMS", "a"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["CF.INSERTNX", "n", "NOCREATE", "ITEMS", "a"]), format!("-{NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["CF.INSERT", "n", "BOGUS", "ITEMS", "a"]), format!("-{UNKNOWN_ARG}\r\n"));
        assert_eq!(run(&s, &["CF.INSERT", "n", "BUCKETSIZE", "4", "ITEMS", "a"]), format!("-{UNKNOWN_ARG}\r\n"));
        assert_eq!(run(&s, &["CF.INSERT", "n", "CAPACITY", "abc", "ITEMS", "a"]), format!("-{INSERT_BAD_CAPACITY}\r\n"));
        assert_eq!(
            run(&s, &["CF.INSERT", "n", "CAPACITY", "0", "ITEMS", "a"]),
            format!("-{CF_INSERT_CAPACITY_RANGE}\r\n")
        );
        assert_eq!(
            run(&s, &["CF.INSERT", "n", "CAPACITY", "100"]),
            "-ERR wrong number of arguments for 'cf.insert' command\r\n"
        );
        assert_eq!(
            run(&s, &["CF.INSERT", "n", "ITEMS"]),
            "-ERR wrong number of arguments for 'cf.insert' command\r\n"
        );
        assert_eq!(run(&s, &["CF.WHAT", "k"]), "-ERR unknown command 'CF.WHAT'\r\n");

        assert_eq!(run(&s, &["CF.RESERVE", "nf", "128", "EXPANSION", "0"]), "+OK\r\n");
        let mut full = 0;
        for i in 0..400u32 {
            if run(&s, &["CF.ADD", "nf", &format!("z{i}")]) == format!("-{CF_FULL}\r\n") {
                full += 1;
            }
        }
        assert!(full > 0, "a nonscaling cuckoo filter must fill up");
        let fresh = (0..40u32)
            .map(|i| format!("fresh-{i}"))
            .find(|n| run(&s, &["CF.COUNT", "nf", n]) == ":0\r\n")
            .expect("a full filter must still miss some item");
        assert_eq!(run(&s, &["CF.INSERT", "nf", "ITEMS", &fresh]), "*1\r\n:-1\r\n");
        assert_eq!(run(&s, &["CF.INSERTNX", "nf", "ITEMS", &fresh]), "*1\r\n:-1\r\n");
    }

    #[test]
    fn a_cuckoo_chain_at_the_filter_limit_reports_maximum_expansions() {
        let s = Store::create("t_cf_maxgrow", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["CF.RESERVE", "m", "4"]), "+OK\r\n");
        let mut stopped = None;
        for i in 0..400u32 {
            let r = run(&s, &["CF.ADD", "m", &format!("m{i}")]);
            if r != ":1\r\n" {
                stopped = Some(r);
                break;
            }
        }
        assert_eq!(stopped, Some(format!("-{MAX_EXPANSIONS}\r\n")));
        assert_eq!(
            cf_parse(s.get_typed(b"m").unwrap().2).unwrap().subs.len(),
            CF_MAX_FILTERS
        );
    }

    #[test]
    fn a_cuckoo_key_of_another_type_answers_where_redis_does() {
        let s = Store::create("t_cf_wrongtype", &cfg(1024 * 1024)).unwrap();
        assert!(s.set(b"str", b"v", 0));
        let wt = format!("-{WRONGTYPE}\r\n");
        assert_eq!(run(&s, &["CF.ADD", "str", "x"]), wt);
        assert_eq!(run(&s, &["CF.ADDNX", "str", "x"]), wt);
        assert_eq!(run(&s, &["CF.INSERT", "str", "ITEMS", "x"]), wt);
        assert_eq!(run(&s, &["CF.INSERTNX", "str", "ITEMS", "x"]), wt);
        assert_eq!(run(&s, &["CF.RESERVE", "str", "100"]), wt);
        assert_eq!(run(&s, &["CF.INFO", "str"]), wt);
        assert_eq!(run(&s, &["CF.SCANDUMP", "str", "0"]), wt);
        assert_eq!(run(&s, &["CF.LOADCHUNK", "str", "1", "x"]), wt);
        assert_eq!(run(&s, &["CF.EXISTS", "str", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.MEXISTS", "str", "x"]), "*1\r\n:0\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "str", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.DEL", "str", "x"]), format!("-{CF_NOT_FOUND}\r\n"));
        assert!(s.set_typed(b"bloom", &new_blob(0.01, 100, 2, false).unwrap(), 0, KIND_BLOOM));
        assert_eq!(run(&s, &["CF.ADD", "bloom", "x"]), wt);
        assert_eq!(run(&s, &["BF.ADD", "str", "x"]), wt);
    }

    #[test]
    fn resp3_answers_the_cuckoo_commands_the_way_redis_does() {
        let s = Store::create("t_cf_resp3", &cfg(1024 * 1024)).unwrap();
        let call = |argv: &[&str]| {
            let args: Vec<Vec<u8>> = argv.iter().map(|a| a.as_bytes().to_vec()).collect();
            let cmd = argv[0].to_uppercase().into_bytes();
            let mut out = Vec::new();
            dispatch(&s, &cmd, &args, &mut out, true, CHUNK_BYTES);
            String::from_utf8_lossy(&out).into_owned()
        };
        assert_eq!(call(&["CF.ADD", "r", "a"]), "#t\r\n");
        assert_eq!(call(&["CF.ADD", "r", "a"]), "#t\r\n");
        assert_eq!(call(&["CF.ADDNX", "r", "a"]), "#f\r\n");
        assert_eq!(call(&["CF.EXISTS", "r", "a"]), "#t\r\n");
        assert_eq!(call(&["CF.MEXISTS", "r", "a", "zz"]), "*2\r\n#t\r\n#f\r\n");
        assert_eq!(call(&["CF.COUNT", "r", "a"]), ":2\r\n");
        assert_eq!(call(&["CF.DEL", "r", "a"]), "#t\r\n");
        assert_eq!(call(&["CF.DEL", "r", "zz"]), "#f\r\n");
        assert_eq!(call(&["CF.INSERT", "r", "ITEMS", "p", "q"]), "*2\r\n#t\r\n#t\r\n");
        assert_eq!(call(&["CF.INSERTNX", "r", "ITEMS", "p", "s"]), "*2\r\n:0\r\n:1\r\n");
        assert!(call(&["CF.INFO", "r"]).starts_with("%8\r\n+Size\r\n:1065\r\n"));
    }

    #[test]
    fn a_rewritten_cuckoo_filter_keeps_the_keys_ttl() {
        let s = Store::create("t_cf_ttl", &cfg(1024 * 1024)).unwrap();
        let blob = cf_new_blob(&cspec(4)).unwrap();
        assert!(s.set_typed(b"t", &blob, 60_000_000, KIND_CUCKOO));
        for i in 0..12u32 {
            assert_eq!(run(&s, &["CF.ADD", "t", &format!("t{i}")]), ":1\r\n");
        }
        assert!(cf_parse(s.get_typed(b"t").unwrap().2).unwrap().subs.len() > 1);
        let exp = s.get_typed(b"t").unwrap().1;
        assert!(exp > now_micros(), "growth dropped the TTL");
    }

    #[test]
    fn bf_reserve_rejects_an_expansion_token_with_no_value() {
        let s = Store::create("t_bloom_noexp", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "100", "EXPANSION"]),
            format!("-{NO_EXPANSION}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "100", "EXPANSION", "-1"]),
            format!("-{EXPANSION_RANGE}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "100", "EXPANSION", "99999"]),
            format!("-{EXPANSION_RANGE}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "100", "EXPANSION", "abc"]),
            format!("-{BAD_EXPANSION}\r\n")
        );
        assert_eq!(run(&s, &["BF.RESERVE", "k32768", "0.01", "100", "EXPANSION", "32768"]), "+OK\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "j", "EXPANSION", "-1", "ITEMS", "x"]),
            format!("-{INSERT_BAD_EXPANSION}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.INSERT", "j", "EXPANSION", "99999", "ITEMS", "x"]),
            format!("-{INSERT_BAD_EXPANSION}\r\n")
        );
        assert_eq!(run(&s, &["BF.INSERT", "j32768", "EXPANSION", "32768", "ITEMS", "x"]), "*1\r\n:1\r\n");
        assert_eq!(run(&s, &["BF.EXISTS", "k", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["BF.RESERVE", "k", "0.01", "100", "EXPANSION", "3"]), "+OK\r\n");
        assert_eq!(run(&s, &["BF.INFO", "k", "EXPANSION"]), "*1\r\n:3\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "j", "EXPANSION", "ITEMS", "x"]),
            format!("-{INSERT_BAD_EXPANSION}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.INSERT", "j", "EXPANSION"]),
            "-ERR wrong number of arguments for 'bf.insert' command\r\n"
        );
    }

    #[test]
    fn a_subnormal_error_rate_is_refused_the_way_redis_does() {
        let s = Store::create("t_bloom_tiny", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["BF.RESERVE", "tiny", "5e-324", "1"]),
            format!("-{CANNOT_CREATE}\r\n")
        );
        assert_eq!(run(&s, &["BF.EXISTS", "tiny", "a"]), ":0\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "ti", "ERROR", "5e-324", "ITEMS", "x"]),
            format!("-{CANNOT_CREATE}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.RESERVE", "small", "9.881312916824931e-324", "1"]),
            "+OK\r\n"
        );
        for i in 0..8u32 {
            assert_eq!(run(&s, &["BF.ADD", "small", &format!("s{i}")]), ":1\r\n");
        }
        assert_eq!(run(&s, &["BF.EXISTS", "small", "s0"]), ":1\r\n");
        assert_eq!(run(&s, &["BF.CARD", "small"]), ":8\r\n");
    }

    #[test]
    fn bit_count_saturates_instead_of_overflowing() {
        let cap_bits = MAX_BLOB_BYTES as u64 * 8;
        assert_eq!(bit_count(1, f64::INFINITY), cap_bits);
        assert_eq!(bit_count(u64::MAX, 1.0), cap_bits);
        assert_eq!(bit_count(1_000_000_000, 1e300), cap_bits);
        assert_eq!(bit_count(1, bits_per_item(MIN_ERROR)), 1600);
        assert_eq!(bit_count(100, bits_per_item(0.01)), 960);
        let deep = (0..MAX_FILTERS)
            .map(|n| (MIN_ERROR * TIGHTEN.powi(n as i32)).max(MIN_ERROR))
            .all(|e| e >= MIN_ERROR && bits_per_item(e).is_finite());
        assert!(deep, "a tightened error must stay positive and finite");
    }

    #[test]
    fn a_loaded_header_with_an_impossible_counter_is_refused() {
        let s = Store::create("t_prob_counters", &cfg(1024 * 1024)).unwrap();
        let mut blob = new_blob(0.01, 100, 2, false).unwrap();
        blob[OFF_ITEMS..OFF_FILTERS].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(shape(&blob), Shape::Bad));
        let load = vec![
            b"BF.LOADCHUNK".to_vec(),
            b"k".to_vec(),
            (blob.len() + 1).to_string().into_bytes(),
            blob.clone(),
        ];
        let mut out = Vec::new();
        dispatch(&s, b"BF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
        assert_eq!(String::from_utf8_lossy(&out), format!("-{BAD_DATA}\r\n"));
        assert_eq!(run(&s, &["BF.CARD", "k"]), ":0\r\n");

        let mut c = cf_new_blob(&cspec(100)).unwrap();
        c[C_OFF_ITEMS..C_OFF_DELETES].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(cshape(&c), CShape::Bad));
        let mut d = cf_new_blob(&cspec(100)).unwrap();
        d[C_OFF_DELETES..C_OFF_FILTERS].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(cshape(&d), CShape::Bad));
        let load = vec![
            b"CF.LOADCHUNK".to_vec(),
            b"ck".to_vec(),
            (c.len() + 1).to_string().into_bytes(),
            c.clone(),
        ];
        let mut out = Vec::new();
        dispatch(&s, b"CF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
        assert_eq!(String::from_utf8_lossy(&out), format!("-{CF_INVALID_HEADER}\r\n"));
        assert_eq!(run(&s, &["CF.COUNT", "ck", "x"]), ":0\r\n");
    }

    #[test]
    fn a_grown_blob_that_does_not_parse_is_reported_as_bad() {
        let mut g = grown(&new_blob(0.01, 4, 2, false).unwrap()).unwrap();
        assert!(matches!(add_in_place(&mut g, b"x"), Add::Added));
        g.truncate(g.len() - 1);
        assert!(matches!(add_in_place(&mut g, b"y"), Add::Bad));

        let mut cg = cf_grown(&cf_new_blob(&cspec(8)).unwrap()).unwrap();
        assert!(matches!(cf_add_in_place(&mut cg, b"x", false), Add::Added));
        cg.truncate(cg.len() - 1);
        assert!(matches!(cf_add_in_place(&mut cg, b"y", false), Add::Bad));

        let s = Store::create("t_prob_grow_guard", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "g", "0.01", "2"]), "+OK\r\n");
        for i in 0..20u32 {
            let r = run(&s, &["BF.ADD", "g", &format!("g{i}")]);
            assert!(r == ":1\r\n" || r == ":0\r\n", "BF.ADD answered {r} at item {i}");
            assert!(
                parse(s.get_typed(b"g").unwrap().2).is_some(),
                "the stored filter stopped parsing at item {i}"
            );
        }
    }

    #[test]
    fn madd_and_insert_answer_the_array_with_the_error_element() {
        let s = Store::create("t_bloom_madd_full", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "mn", "0.01", "2", "NONSCALING"]), "+OK\r\n");
        assert_eq!(
            run(&s, &["BF.MADD", "mn", "p", "q", "r", "s"]),
            format!("*3\r\n:1\r\n:1\r\n-{FILTER_FULL}\r\n")
        );
        assert_eq!(run(&s, &["BF.MADD", "mn", "zzz"]), format!("*1\r\n-{FILTER_FULL}\r\n"));
        assert_eq!(run(&s, &["BF.MEXISTS", "mn", "p", "q", "r"]), "*3\r\n:1\r\n:1\r\n:0\r\n");

        assert_eq!(run(&s, &["BF.RESERVE", "mn2", "0.01", "2", "NONSCALING"]), "+OK\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "mn2", "ITEMS", "p", "q", "r", "s"]),
            format!("*3\r\n:1\r\n:1\r\n-{FILTER_FULL}\r\n")
        );

        assert_eq!(run(&s, &["BF.RESERVE", "mn3", "0.01", "2", "NONSCALING"]), "+OK\r\n");
        assert_eq!(
            run(&s, &["BF.MADD", "mn3", "p", "p", "q", "r"]),
            format!("*4\r\n:1\r\n:0\r\n:1\r\n-{FILTER_FULL}\r\n")
        );
    }

    #[test]
    fn bloom_scandump_and_loadchunk_answer_wrongtype_on_a_string() {
        let s = Store::create("t_bloom_wt_dump", &cfg(1024 * 1024)).unwrap();
        assert!(s.set(b"str", b"v", 0));
        let wt = format!("-{WRONGTYPE}\r\n");
        assert_eq!(run(&s, &["BF.SCANDUMP", "str", "0"]), wt);
        assert_eq!(run(&s, &["BF.LOADCHUNK", "str", "1", "zz"]), wt);
        assert_eq!(run(&s, &["CF.SCANDUMP", "str", "0"]), wt);
        assert_eq!(run(&s, &["CF.LOADCHUNK", "str", "1", "zz"]), wt);
    }

    #[test]
    fn an_under_positioned_chunk_iterator_is_refused() {
        assert_eq!(chunk_start(1, 0), Some(0));
        assert_eq!(chunk_start(9, 8), Some(0));
        assert_eq!(chunk_start(9, 4), Some(4));
        assert_eq!(chunk_start(1, 4), None);

        let s = Store::create("t_prob_chunk_pos", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["BF.LOADCHUNK", "k", "1", "not-a-filter"]),
            format!("-{BAD_DATA}\r\n")
        );
        assert_eq!(
            run(&s, &["CF.LOADCHUNK", "k", "1", "not-a-filter"]),
            format!("-{CF_INVALID_HEADER}\r\n")
        );
        assert_eq!(run(&s, &["CF.RESERVE", "src", "2000"]), "+OK\r\n");
        let chunks = cf_dump(&s, "src");
        let (it, data) = &chunks[0];
        let bad = vec![
            b"CF.LOADCHUNK".to_vec(),
            b"dst".to_vec(),
            (it - 1).to_string().into_bytes(),
            data.clone(),
        ];
        let mut out = Vec::new();
        dispatch(&s, b"CF.LOADCHUNK", &bad, &mut out, false, CHUNK_BYTES);
        assert_eq!(
            String::from_utf8_lossy(&out),
            format!("-{CF_INVALID_HEADER}\r\n")
        );
        assert!(s.get_typed(b"dst").is_none(), "a bad iterator must not create the key");
    }

    #[test]
    fn a_reserve_too_big_for_the_arena_is_refused_before_the_bytes_are_built() {
        let s = Store::create("t_prob_oom", &cfg(1024 * 1024)).unwrap();
        assert!(!s.can_hold(400_000_000));
        assert!(s.can_hold(1024));
        let oom = format!("-{}\r\n", crate::server::OOM_ERR);
        assert_eq!(run(&s, &["BF.RESERVE", "zz", "0.01", "400000000"]), oom);
        assert_eq!(run(&s, &["CF.RESERVE", "cz", "400000000"]), oom);
        assert!(s.get_typed(b"zz").is_none());
        assert!(s.get_typed(b"cz").is_none());
        assert!(bloom_blob_bytes(0.01, 400_000_000) > 400_000_000);
        assert_eq!(cuckoo_blob_bytes(1000, 2), 1065);
    }

    #[test]
    fn scandump_chunks_never_exceed_the_max_bulk_limit() {
        let s = Store::create("t_prob_chunk_limit", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "b", "0.01", "20000"]), "+OK\r\n");
        let blob_len = s.get_typed(b"b").unwrap().2.len();
        assert!(blob_len > 4096, "the dump must need several small chunks");
        let mut it = 0i64;
        let mut total = 0usize;
        let mut chunks = 0;
        loop {
            let args = vec![b"BF.SCANDUMP".to_vec(), b"b".to_vec(), it.to_string().into_bytes()];
            let mut out = Vec::new();
            dispatch(&s, b"BF.SCANDUMP", &args, &mut out, false, 4096);
            let (next, data) = decode_scandump(&out);
            if next == 0 {
                break;
            }
            assert!(data.len() <= 4096, "a chunk of {} exceeds max_bulk", data.len());
            total += data.len();
            it = next;
            chunks += 1;
            assert!(chunks < 100, "the chunk loop did not terminate");
        }
        assert_eq!(total, blob_len);
        assert!(chunks > 1);
        assert_eq!(chunk_bytes(0), C_HDR);
        assert_eq!(chunk_bytes(usize::MAX), CHUNK_BYTES);
    }

    #[test]
    fn an_option_keyword_with_no_value_never_reads_past_the_arguments() {
        let s = Store::create("t_prob_bounds", &cfg(1024 * 1024)).unwrap();
        let cf = "-ERR wrong number of arguments for 'cf.reserve' command\r\n";
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "X", "BUCKETSIZE"]), cf);
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "X", "MAXITERATIONS"]), cf);
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "X", "EXPANSION"]), cf);
        assert_eq!(run(&s, &["CF.RESERVE", "k", "100", "A", "B", "C", "BUCKETSIZE"]), cf);
        assert!(s.get_typed(b"k").is_none());
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "100", "X", "EXPANSION"]),
            format!("-{NO_EXPANSION}\r\n")
        );
        assert_eq!(
            run(&s, &["BF.INSERT", "k", "CAPACITY"]),
            "-ERR wrong number of arguments for 'bf.insert' command\r\n"
        );
        assert_eq!(
            run(&s, &["CF.INSERT", "k", "CAPACITY"]),
            "-ERR wrong number of arguments for 'cf.insert' command\r\n"
        );
        assert_eq!(
            run(&s, &["BF.INSERT", "k", "ITEMS", "a", "ERROR"]),
            "*2\r\n:1\r\n:1\r\n"
        );
    }

    #[test]
    fn duplicate_and_unknown_option_tokens_follow_redis() {
        let s = Store::create("t_prob_dupopts", &cfg(1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "b1", "0.01", "100", "BOGUS"]), "+OK\r\n");
        assert_eq!(run(&s, &["BF.RESERVE", "b2", "0.01", "100", "BOGUS", "BOGUS"]), "+OK\r\n");
        assert_eq!(
            run(&s, &["BF.RESERVE", "b3", "0.01", "100", "NONSCALING", "NONSCALING"]),
            "+OK\r\n"
        );
        assert_eq!(
            run(&s, &["BF.RESERVE", "b4", "0.01", "100", "EXPANSION", "2", "EXPANSION", "3"]),
            "-ERR wrong number of arguments for 'bf.reserve' command\r\n"
        );
        assert_eq!(
            run(&s, &["BF.RESERVE", "b5", "0.01", "100", "EXPANSION", "2", "EXPANSION"]),
            "+OK\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "b5", "EXPANSION"]), "*1\r\n:2\r\n");
        assert_eq!(
            run(&s, &["BF.RESERVE", "b6", "0.01", "100", "X", "EXPANSION", "3"]),
            "+OK\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "b6", "EXPANSION"]), "*1\r\n:3\r\n");
        assert_eq!(
            run(&s, &["BF.RESERVE", "b7", "0.01", "100", "EXPANSION", "3", "X"]),
            "+OK\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "b7", "EXPANSION"]), "*1\r\n:3\r\n");

        assert_eq!(
            run(&s, &["CF.RESERVE", "c1", "1000", "BUCKETSIZE", "4", "BUCKETSIZE", "8"]),
            "+OK\r\n"
        );
        assert_eq!(cf_parse(s.get_typed(b"c1").unwrap().2).unwrap().bucket, 4);
        assert_eq!(
            run(&s, &["CF.RESERVE", "c2", "1000", "EXPANSION", "2", "EXPANSION", "4"]),
            "+OK\r\n"
        );
        assert_eq!(cf_parse(s.get_typed(b"c2").unwrap().2).unwrap().expansion, 2);
        assert_eq!(
            run(&s, &["CF.RESERVE", "c3", "1000", "MAXITERATIONS", "5", "MAXITERATIONS", "9"]),
            "+OK\r\n"
        );
        assert_eq!(cf_parse(s.get_typed(b"c3").unwrap().2).unwrap().maxiter, 5);

        assert_eq!(
            run(&s, &["BF.INSERT", "i1", "CAPACITY", "200", "CAPACITY", "300", "ITEMS", "x"]),
            "*1\r\n:1\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "i1", "CAPACITY"]), "*1\r\n:300\r\n");
        assert_eq!(
            run(&s, &["BF.INSERT", "i2", "EXPANSION", "2", "EXPANSION", "3", "ITEMS", "x"]),
            "*1\r\n:1\r\n"
        );
        assert_eq!(run(&s, &["BF.INFO", "i2", "EXPANSION"]), "*1\r\n:3\r\n");
        assert_eq!(
            run(&s, &["CF.INSERT", "i3", "CAPACITY", "2000", "CAPACITY", "3000", "ITEMS", "x"]),
            "*1\r\n:1\r\n"
        );
        assert_eq!(
            cf_parse(s.get_typed(b"i3").unwrap().2).unwrap().subs[0].buckets,
            2048
        );
    }

    #[test]
    fn an_empty_first_chunk_creates_no_key() {
        let s = Store::create("t_prob_empty_chunk", &cfg(1024 * 1024)).unwrap();
        let load = vec![
            b"BF.LOADCHUNK".to_vec(),
            b"zz".to_vec(),
            b"1".to_vec(),
            Vec::new(),
        ];
        let mut out = Vec::new();
        dispatch(&s, b"BF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
        assert_eq!(String::from_utf8_lossy(&out), format!("-{BAD_DATA}\r\n"));
        assert!(s.get_typed(b"zz").is_none(), "an empty chunk must not create a key");
        assert_eq!(run(&s, &["BF.RESERVE", "zz", "0.01", "100"]), "+OK\r\n");

        let load = vec![
            b"CF.LOADCHUNK".to_vec(),
            b"cz".to_vec(),
            b"1".to_vec(),
            Vec::new(),
        ];
        let mut out = Vec::new();
        dispatch(&s, b"CF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
        assert_eq!(String::from_utf8_lossy(&out), format!("-{CF_INVALID_HEADER}\r\n"));
        assert!(s.get_typed(b"cz").is_none());
        assert_eq!(run(&s, &["CF.RESERVE", "cz", "100"]), "+OK\r\n");

        assert_eq!(run(&s, &["BF.LOADCHUNK", "hz", "2", "\u{1}"]), format!("-{BAD_DATA}\r\n"));
        assert!(s.get_typed(b"hz").is_none());
        assert_eq!(run(&s, &["CF.LOADCHUNK", "hc", "2", "\u{2}"]), format!("-{CF_INVALID_HEADER}\r\n"));
        assert!(s.get_typed(b"hc").is_none());
        assert!(chunk_bytes(1) >= C_HDR, "a dump chunk must carry a whole header");
    }

    #[test]
    fn a_bloom_chain_at_its_cap_reports_maximum_expansions() {
        let s = Store::create("t_bloom_maxgrow", &cfg(8 * 1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "mfx", "0.01", "1", "EXPANSION", "1"]), "+OK\r\n");
        for i in 0..60u32 {
            let r = run(&s, &["BF.ADD", "mfx", &format!("f{i}")]);
            assert!(r == ":1\r\n" || r == ":0\r\n", "BF.ADD answered {r} at item {i}");
        }
        assert!(parse(s.get_typed(b"mfx").unwrap().2).unwrap().subs.len() > 32);

        assert_eq!(run(&s, &["BF.RESERVE", "cap", "0.01", "1", "EXPANSION", "1"]), "+OK\r\n");
        let mut stopped = None;
        for i in 0..4000u32 {
            let r = run(&s, &["BF.ADD", "cap", &format!("c{i}")]);
            if r != ":1\r\n" && r != ":0\r\n" {
                stopped = Some(r);
                break;
            }
        }
        assert_eq!(stopped, Some(format!("-{MAX_EXPANSIONS}\r\n")));
        assert_eq!(
            parse(s.get_typed(b"cap").unwrap().2).unwrap().subs.len(),
            MAX_FILTERS
        );
    }

    #[test]
    fn an_error_rate_outside_the_range_is_a_bad_error_rate() {
        let s = Store::create("t_prob_errrange", &cfg(1024 * 1024)).unwrap();
        for bad in ["-1", "0", "2"] {
            assert_eq!(
                run(&s, &["BF.INSERT", "e", "ERROR", bad, "ITEMS", "a"]),
                format!("-{INSERT_BAD_ERROR}\r\n"),
                "BF.INSERT ERROR {bad}"
            );
            assert_eq!(
                run(&s, &["BF.RESERVE", "r", "0.01", "100"]).is_empty(),
                false
            );
        }
        for bad in ["-1", "0", "1.0", "2"] {
            assert_eq!(
                run(&s, &["BF.RESERVE", "rr", bad, "100"]),
                format!("-{ERROR_RANGE}\r\n"),
                "BF.RESERVE {bad}"
            );
        }
        assert_eq!(
            run(&s, &["BF.INSERT", "e", "ERROR", "5e-324", "ITEMS", "a"]),
            format!("-{CANNOT_CREATE}\r\n")
        );
    }

    #[test]
    fn an_add_retries_when_the_key_lapses_inside_the_command() {
        let s = Store::create("t_prob_lapse", &cfg(1024 * 1024)).unwrap();
        let blob = new_blob(0.01, 100, 2, false).unwrap();
        assert!(s.set_typed(b"g", &blob, 1, KIND_BLOOM));
        assert!(matches!(add_item(&s, b"g", b"x", &Spec::default()), Add::Added));
        assert_eq!(run(&s, &["BF.EXISTS", "g", "x"]), ":1\r\n");
        assert_eq!(run(&s, &["BF.CARD", "g"]), ":1\r\n");

        let cblob = cf_new_blob(&cspec(1000)).unwrap();
        assert!(s.set_typed(b"c", &cblob, 1, KIND_CUCKOO));
        assert!(matches!(
            cf_add_item(&s, b"c", b"y", false, &CSpec::default()),
            Add::Added
        ));
        assert_eq!(run(&s, &["CF.EXISTS", "c", "y"]), ":1\r\n");
    }

    #[test]
    fn a_capacity_whose_bits_cannot_fit_the_blob_is_refused() {
        let s = Store::create("t_prob_bits", &cfg(1024 * 1024)).unwrap();
        assert_eq!(
            run(&s, &["BF.RESERVE", "k", "0.01", "1000000000"]),
            format!("-{CANNOT_CREATE}\r\n")
        );
        assert!(s.get_typed(b"k").is_none());
        assert_eq!(
            run(&s, &["BF.INSERT", "j", "CAPACITY", "1000000000", "ITEMS", "x"]),
            format!("-{CANNOT_CREATE}\r\n")
        );
        assert!(s.get_typed(b"j").is_none());
        assert!(bits_fit(448_000_000, 0.01));
        assert!(!bits_fit(1_000_000_000, 0.01));
        assert!(!bits_fit(u64::MAX, 0.01));
        assert!(bits_fit(100, 0.01));
        assert_eq!(
            run(&s, &["BF.RESERVE", "ok", "0.01", "400000000"]),
            format!("-{}\r\n", crate::server::OOM_ERR),
            "a capacity whose bits fit is refused by the arena, not by the bit bound"
        );
    }

    #[test]
    fn a_chain_that_cannot_size_its_next_filter_reports_maximum_expansions() {
        let mut blob = new_blob(0.01, 400_000_000, 4, false).unwrap();
        assert!(grown(&blob).is_none(), "the next sub-filter cannot fit");
        let f = parse(&blob).unwrap();
        blob[f.subs[0].hdr + 8..f.subs[0].hdr + 16]
            .copy_from_slice(&f.subs[0].capacity.to_le_bytes());
        assert!(matches!(add_in_place(&mut blob, b"x"), Add::Grow));

        let s = Store::create("t_prob_growbits", &cfg(1024 * 1024 * 1024)).unwrap();
        assert!(s.set_typed(b"g", &blob, 0, KIND_BLOOM));
        assert_eq!(run(&s, &["BF.ADD", "g", "x"]), format!("-{MAX_EXPANSIONS}\r\n"));
    }

    #[test]
    fn a_filter_blob_that_does_not_parse_is_reported_not_treated_as_absent() {
        let s = Store::create("t_prob_partial", &cfg(4 * 1024 * 1024)).unwrap();
        assert_eq!(run(&s, &["BF.RESERVE", "src", "0.01", "5000"]), "+OK\r\n");
        assert_eq!(run(&s, &["BF.ADD", "src", "a"]), ":1\r\n");
        let head = s.get_typed(b"src").unwrap().2[..64].to_vec();
        let load = vec![
            b"BF.LOADCHUNK".to_vec(),
            b"part".to_vec(),
            (head.len() + 1).to_string().into_bytes(),
            head,
        ];
        let mut out = Vec::new();
        dispatch(&s, b"BF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
        assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");
        assert!(parse(s.get_typed(b"part").unwrap().2).is_none());
        let bad = format!("-{BAD_DATA}\r\n");
        assert_eq!(run(&s, &["BF.EXISTS", "part", "a"]), bad);
        assert_eq!(run(&s, &["BF.MEXISTS", "part", "a", "b"]), bad);
        assert_eq!(run(&s, &["BF.CARD", "part"]), bad);
        assert_eq!(run(&s, &["BF.INFO", "part"]), bad);

        assert_eq!(run(&s, &["CF.RESERVE", "csrc", "5000"]), "+OK\r\n");
        assert_eq!(run(&s, &["CF.ADD", "csrc", "a"]), ":1\r\n");
        let chead = s.get_typed(b"csrc").unwrap().2[..64].to_vec();
        let load = vec![
            b"CF.LOADCHUNK".to_vec(),
            b"cpart".to_vec(),
            (chead.len() + 1).to_string().into_bytes(),
            chead,
        ];
        let mut out = Vec::new();
        dispatch(&s, b"CF.LOADCHUNK", &load, &mut out, false, CHUNK_BYTES);
        assert_eq!(String::from_utf8_lossy(&out), "+OK\r\n");
        assert!(cf_parse(s.get_typed(b"cpart").unwrap().2).is_none());
        let cbad = format!("-{CF_INVALID_HEADER}\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "cpart", "a"]), cbad);
        assert_eq!(run(&s, &["CF.MEXISTS", "cpart", "a", "b"]), cbad);
        assert_eq!(run(&s, &["CF.COUNT", "cpart", "a"]), cbad);
        assert_eq!(run(&s, &["CF.DEL", "cpart", "a"]), cbad);
        assert_eq!(run(&s, &["CF.INFO", "cpart"]), cbad);
    }

    #[test]
    fn a_wrong_type_key_still_reads_as_absent_the_way_redis_does() {
        let s = Store::create("t_prob_wt_reads", &cfg(1024 * 1024)).unwrap();
        assert!(s.set(b"str", b"v", 0));
        assert_eq!(run(&s, &["BF.EXISTS", "str", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["BF.MEXISTS", "str", "x", "y"]), "*2\r\n:0\r\n:0\r\n");
        assert_eq!(run(&s, &["CF.EXISTS", "str", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.MEXISTS", "str", "x", "y"]), "*2\r\n:0\r\n:0\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "str", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.DEL", "str", "x"]), format!("-{CF_NOT_FOUND}\r\n"));
        assert_eq!(run(&s, &["BF.EXISTS", "gone", "x"]), ":0\r\n");
        assert_eq!(run(&s, &["CF.COUNT", "gone", "x"]), ":0\r\n");
    }
}
