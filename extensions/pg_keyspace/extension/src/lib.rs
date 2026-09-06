//! pg_keyspace — Postgres-native RESP keyspace, P0 spike as a real extension.
//!
//! Loaded via `shared_preload_libraries`, this extension:
//!   * requests a Postgres shared-memory segment (§3.2) in `shmem_request_hook`
//!     and initialises the keyspace store over it in `shmem_startup_hook`;
//!   * registers a background worker (a real Postgres backend) that runs the
//!     epoll RESP event loop against that segment (§3.1) — the hot path the P0
//!     kill criterion measures, served on a TCP port for `ioredis`/`redis-cli`;
//!   * exposes the `supacache.*` SQL surface (§6), which reads the *same*
//!     segment directly in the calling backend — the in-process ~1-2µs path.
//!
//! The performance-critical modules are shared verbatim with the standalone
//! `poc` crate (via `#[path]`), so the code measured here is the same code the
//! standalone benchmarks measure.

use core::ffi::c_void;
use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::guc::{GucContext, GucFlags, GucRegistry, GucSetting};
use pgrx::prelude::*;
use pgrx::{IntoDatum, PgBuiltInOids, PgOid};
use std::ffi::CStr;
use std::sync::atomic::{AtomicPtr, Ordering};
use std::sync::Arc;
use std::time::Duration;

pgrx::pg_module_magic!();

// ---- shared core (identical to the standalone poc) -----------------------
#[path = "../../poc/src/crc16.rs"]
mod crc16;
#[path = "../../poc/src/shmem.rs"]
mod shmem;
#[path = "../../poc/src/store.rs"]
mod store;
#[path = "../../poc/src/resp.rs"]
mod resp;
#[path = "../../poc/src/batcher.rs"]
mod batcher;
#[path = "../../poc/src/ring.rs"]
mod ring;
#[path = "../../poc/src/server.rs"]
mod server;

use batcher::Tier;
use store::{Config, Lookup, Store};

const SEG_NAME: &CStr = c"pg_keyspace_segment";
const RING_NAME: &CStr = c"pg_keyspace_ring";

// Base address of the Postgres shared-memory segment, published by the startup
// hook and inherited by every forked backend. Each context rebuilds a cheap
// `Store` view over it on demand.
static SEG_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the SPSC persistence ring (RESP worker -> persistence worker).
static RING_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());

// GUCs (fixed at postmaster start; the segment is sized from them).
static GUC_PORT: GucSetting<i32> = GucSetting::<i32>::new(6380);
static GUC_KEYS: GucSetting<i32> = GucSetting::<i32>::new(1_000_000);
static GUC_VAL_BYTES: GucSetting<i32> = GucSetting::<i32>::new(512);
static GUC_DURABILITY: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"ephemeral"));
static GUC_COMMIT_WINDOW_US: GucSetting<i32> = GucSetting::<i32>::new(500);
// P1 persistence: which database holds supacache.kv, and how often the worker
// flushes staged writes to it in one batched transaction.
static GUC_DATABASE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"postgres"));
static GUC_PERSIST_WINDOW_MS: GucSetting<i32> = GucSetting::<i32>::new(10);
static GUC_RING_MB: GucSetting<i32> = GucSetting::<i32>::new(64);
static GUC_PERSIST_WORKERS: GucSetting<i32> = GucSetting::<i32>::new(1);

/// Number of persistence workers = number of rings (writes are sharded across
/// them by key slot).
fn ring_count() -> usize {
    GUC_PERSIST_WORKERS.get().max(1) as usize
}
/// Bytes for one ring (header + power-of-two capacity).
fn ring_stride() -> usize {
    ring::bytes_for((GUC_RING_MB.get().max(1) as usize) * 1024 * 1024)
}
/// Total shared memory for all rings, laid out contiguously.
fn ring_total_bytes() -> usize {
    ring_count() * ring_stride()
}

static mut PREV_SHMEM_REQUEST_HOOK: Option<unsafe extern "C" fn()> = None;
static mut PREV_SHMEM_STARTUP_HOOK: Option<unsafe extern "C" fn()> = None;

fn ks_config() -> Config {
    let keys = GUC_KEYS.get().max(1024) as u32;
    let val = GUC_VAL_BYTES.get().max(1) as u64;
    Config::for_capacity(1, keys, val)
}

fn ks_tier() -> Tier {
    match GUC_DURABILITY.get().and_then(|c| c.to_str().ok()) {
        Some("relaxed") => Tier::Relaxed,
        Some("durable") => Tier::Durable,
        Some("replicated") => Tier::Replicated,
        _ => Tier::Ephemeral,
    }
}

/// A cheap `Store` view over the shared segment, valid in any backend.
fn store_view() -> Option<Store> {
    let base = SEG_BASE.load(Ordering::Acquire);
    if base.is_null() {
        return None;
    }
    Some(unsafe { Store::from_raw(base, &ks_config(), false) })
}

// ---- extension init ------------------------------------------------------

#[pg_guard]
pub extern "C" fn _PG_init() {
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        error!("pg_keyspace must be loaded via shared_preload_libraries");
    }

    GucRegistry::define_int_guc(
        "pg_keyspace.port",
        "TCP port the RESP slot worker listens on",
        "Cluster clients connect here; stock RESP2.",
        &GUC_PORT,
        1,
        65535,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.keys",
        "Keyspace capacity (entries) for the shared-memory segment",
        "Sizes the hash table, entry arena and slab arena at startup.",
        &GUC_KEYS,
        1024,
        1_000_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.val_bytes",
        "Average value size used to size the slab arena",
        "",
        &GUC_VAL_BYTES,
        1,
        1_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.commit_window_us",
        "Commit-batching window in microseconds for logged durability tiers",
        "One fsync is amortised across all writes staged within a window (§3.4).",
        &GUC_COMMIT_WINDOW_US,
        0,
        1_000_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.durability",
        "Durability tier for RESP writes: ephemeral|relaxed|durable|replicated",
        "ephemeral keeps writes shmem-only; non-ephemeral persists to supacache.kv (§3.3).",
        &GUC_DURABILITY,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_string_guc(
        "pg_keyspace.database",
        "Database that holds the supacache.kv backing tables",
        "",
        &GUC_DATABASE,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.persist_window_ms",
        "How often the persistence worker drains the ring when idle, in ms",
        "Under load it drains continuously; this only bounds idle latency.",
        &GUC_PERSIST_WINDOW_MS,
        1,
        60_000,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.ring_mb",
        "Size of each RESP->persistence ring buffer, in MB",
        "Absorbs write bursts so the RESP path never blocks on persistence.",
        &GUC_RING_MB,
        1,
        4096,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.persist_workers",
        "Number of persistence workers (and rings) draining writes in parallel",
        "Writes are sharded by key slot; more workers scale durable throughput.",
        &GUC_PERSIST_WORKERS,
        1,
        16,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    // Chain the shmem hooks so the segment is requested and initialised.
    unsafe {
        PREV_SHMEM_REQUEST_HOOK = pg_sys::shmem_request_hook;
        pg_sys::shmem_request_hook = Some(ks_shmem_request);
        PREV_SHMEM_STARTUP_HOOK = pg_sys::shmem_startup_hook;
        pg_sys::shmem_startup_hook = Some(ks_shmem_startup);
    }

    // Register the RESP slot worker (a real Postgres background worker). When a
    // logged durability tier is configured it needs an SPI database connection
    // to persist into supacache.kv; ephemeral needs only shared memory.
    let persisted = ks_tier() != Tier::Ephemeral;
    let builder = BackgroundWorkerBuilder::new("pg_keyspace: RESP slot worker")
        .set_library("pg_keyspace")
        .set_function("pg_keyspace_worker_main")
        .set_restart_time(Some(Duration::from_secs(2)));
    let builder = if persisted {
        builder.enable_spi_access()
    } else {
        builder.enable_shmem_access(None)
    };
    builder.load();

    // Dedicated persistence workers: each drains its own ring and bulk-upserts
    // into supacache.kv, so the RESP worker never touches SPI on the hot path.
    // Each is passed its ring index as the bgworker argument.
    if persisted {
        for i in 0..ring_count() {
            BackgroundWorkerBuilder::new(&format!("pg_keyspace: persistence worker {i}"))
                .set_library("pg_keyspace")
                .set_function("pg_keyspace_persist_main")
                .set_argument((i as i32).into_datum())
                .set_restart_time(Some(Duration::from_secs(2)))
                .enable_spi_access()
                .load();
        }
    }

    log!("pg_keyspace: initialised (shmem hooks + RESP worker registered)");
}

#[pg_guard]
extern "C" fn ks_shmem_request() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_REQUEST_HOOK {
            prev();
        }
        pg_sys::RequestAddinShmemSpace(ks_config().total_bytes());
        pg_sys::RequestAddinShmemSpace(ring_total_bytes());
    }
}

#[pg_guard]
extern "C" fn ks_shmem_startup() {
    unsafe {
        if let Some(prev) = PREV_SHMEM_STARTUP_HOOK {
            prev();
        }
        let cfg = ks_config();
        let size = cfg.total_bytes();
        let mut found = false;
        let ptr = pg_sys::ShmemInitStruct(SEG_NAME.as_ptr(), size, &mut found) as *mut u8;
        if ptr.is_null() {
            error!("pg_keyspace: ShmemInitStruct returned NULL");
        }
        // First backend (postmaster) initialises; the rest just publish the base.
        let _view = Store::from_raw(ptr, &cfg, !found);
        SEG_BASE.store(ptr, Ordering::Release);

        // Persistence ring segment: N contiguous rings of `ring_stride()` bytes.
        let rbytes = ring_total_bytes();
        let stride = ring_stride();
        let cap = (GUC_RING_MB.get().max(1) as usize) * 1024 * 1024;
        let mut rfound = false;
        let rptr = pg_sys::ShmemInitStruct(RING_NAME.as_ptr(), rbytes, &mut rfound) as *mut u8;
        if !rptr.is_null() {
            if !rfound {
                std::ptr::write_bytes(rptr, 0, rbytes);
                for i in 0..ring_count() {
                    ring::init(rptr.add(i * stride), cap);
                }
            }
            RING_BASE.store(rptr, Ordering::Release);
        }
        log!(
            "pg_keyspace: shmem ready (store {} bytes, {} rings x {} bytes, found={})",
            size,
            ring_count(),
            stride,
            found
        );
    }
}

// ---- the background worker: the RESP event loop --------------------------

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);

    let base = SEG_BASE.load(Ordering::Acquire);
    if base.is_null() {
        log!("pg_keyspace worker: shared segment not ready, exiting");
        return;
    }
    let cfg = ks_config();
    let store = Arc::new(unsafe { Store::from_raw(base, &cfg, false) });
    let persisted = ks_tier() != Tier::Ephemeral;

    let port = GUC_PORT.get() as u16;
    let mut worker = match server::Worker::new(store.clone(), None, Tier::Ephemeral, "0.0.0.0", port)
    {
        Ok(w) => w,
        Err(e) => {
            log!("pg_keyspace worker: cannot listen on :{port}: {e}");
            return;
        }
    };

    // P1 storage & durability: connect SPI only to recover shmem from the
    // backing tables at startup (crash recovery); all steady-state persistence
    // is offloaded to the persistence worker via the ring, so the RESP hot path
    // never touches SPI.
    if persisted {
        let dbname = GUC_DATABASE
            .get()
            .and_then(|c| c.to_str().ok())
            .unwrap_or("postgres")
            .to_string();
        BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
        // wait (bounded) for the persistence worker to create the table
        for _ in 0..30 {
            if pg_table_ready() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        let t0 = std::time::Instant::now();
        let n = pg_recover(&store);
        log!(
            "pg_keyspace worker: recovered {n} keys from supacache.kv in {:?}",
            t0.elapsed()
        );
        let rbase = RING_BASE.load(Ordering::Acquire);
        if !rbase.is_null() {
            let stride = ring_stride();
            let n = ring_count();
            let producers: Vec<ring::Producer> =
                (0..n).map(|i| unsafe { ring::Producer::attach(rbase.add(i * stride)) }).collect();
            worker.set_ring_producers(producers);
            log!("pg_keyspace worker: persistence ON ({n} rings -> {n} workers, db={dbname})");
        } else {
            log!("pg_keyspace worker: ring not ready; running ephemeral");
        }
    }

    log!("pg_keyspace worker: RESP listening on 0.0.0.0:{port} (persisted={persisted})");
    let _ = worker.run_with(|| !BackgroundWorker::sigterm_received(), 500);
    log!("pg_keyspace worker: shutting down");
}

/// The dedicated persistence worker: drains the ring and bulk-upserts into
/// `supacache.kv`. Runs in its own process with its own SPI connection, so the
/// RESP worker's event loop is never blocked by Postgres.
#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_persist_main(arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let idx = unsafe { i32::from_datum(arg, false) }.unwrap_or(0).max(0) as usize;
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    // Worker 0 owns schema creation; the others wait for it (avoids concurrent DDL).
    if idx == 0 {
        pg_ensure_schema();
    } else {
        for _ in 0..50 {
            if pg_table_ready() {
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    let rbase = RING_BASE.load(Ordering::Acquire);
    if rbase.is_null() {
        log!("pg_keyspace persist {idx}: ring not ready, exiting");
        return;
    }
    let consumer = unsafe { ring::Consumer::attach(rbase.add(idx * ring_stride())) };
    let idle = Duration::from_millis(GUC_PERSIST_WINDOW_MS.get().max(1) as u64);
    log!("pg_keyspace persist {idx}: draining ring {idx} -> supacache.kv (db={dbname})");

    while !BackgroundWorker::sigterm_received() {
        let mut batch: Vec<server::PendingWrite> = Vec::with_capacity(8192);
        consumer.drain(20_000, |k, v, e| batch.push((k.to_vec(), v.to_vec(), e)));
        if batch.is_empty() {
            std::thread::sleep(idle);
            continue;
        }
        bulk_upsert(batch);
    }
    // final drain on shutdown
    let mut tail: Vec<server::PendingWrite> = Vec::new();
    consumer.drain(usize::MAX, |k, v, e| tail.push((k.to_vec(), v.to_vec(), e)));
    if !tail.is_empty() {
        bulk_upsert(tail);
    }
    log!("pg_keyspace persist: shutting down");
}

/// True if supacache.kv exists yet (created by the persistence worker).
fn pg_table_ready() -> bool {
    BackgroundWorker::transaction(|| {
        Spi::get_one::<bool>("SELECT to_regclass('supacache.kv') IS NOT NULL")
            .ok()
            .flatten()
            .unwrap_or(false)
    })
}

/// Create the `supacache` schema and the hash-partitioned `supacache.kv`
/// backing table (§3.3) if absent. Idempotent; runs in one transaction.
fn pg_ensure_schema() {
    BackgroundWorker::transaction(|| {
        let _ = Spi::run("CREATE SCHEMA IF NOT EXISTS supacache");
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.kv (\
             tenant text NOT NULL DEFAULT '', key bytea NOT NULL, slot int NOT NULL, \
             kind \"char\" NOT NULL DEFAULT 's', val bytea, \
             expires_at bigint NOT NULL DEFAULT 0, version bigint NOT NULL DEFAULT 1, \
             PRIMARY KEY (tenant, key)) PARTITION BY HASH (tenant, key)",
        );
        for i in 0..8 {
            let _ = Spi::run(&format!(
                "CREATE TABLE IF NOT EXISTS supacache.kv_p{i} PARTITION OF supacache.kv \
                 FOR VALUES WITH (MODULUS 8, REMAINDER {i})"
            ));
        }
    });
}

/// Load live keys from `supacache.kv` into shmem at startup (crash recovery).
/// Expired rows are skipped. Returns the number of keys restored.
fn pg_recover(store: &Store) -> i64 {
    use std::panic::AssertUnwindSafe;
    let now = store::now_micros();
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            let mut cnt = 0i64;
            let tup = client.select("SELECT key, val, expires_at FROM supacache.kv", None, None)?;
            for row in tup {
                let k: Option<Vec<u8>> = row.get(1)?;
                let v: Option<Vec<u8>> = row.get(2)?;
                let e: i64 = row.get::<i64>(3)?.unwrap_or(0);
                if let (Some(k), Some(v)) = (k, v) {
                    if e > 0 && e <= now {
                        continue; // already expired
                    }
                    let ttl = if e > 0 { e - now } else { 0 };
                    store.set(&k, &v, ttl);
                    cnt += 1;
                }
            }
            Ok::<i64, pgrx::spi::Error>(cnt)
        })
        .unwrap_or(0)
    }))
}

/// Bulk-upsert a drained batch into `supacache.kv` in one transaction (§3.4).
/// The batch is deduplicated by key (last write wins) so a single `ON CONFLICT`
/// command never touches the same row twice, then sent as four parallel arrays
/// through `unnest(...)` — one plan, one execution, one commit for the batch.
fn bulk_upsert(batch: Vec<server::PendingWrite>) {
    use std::collections::HashMap;
    if batch.is_empty() {
        return;
    }
    let mut latest: HashMap<Vec<u8>, (Vec<u8>, i64)> = HashMap::with_capacity(batch.len());
    for (k, v, e) in batch {
        latest.insert(k, (v, e));
    }
    let n = latest.len();
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut slots: Vec<i32> = Vec::with_capacity(n);
    let mut vals: Vec<Vec<u8>> = Vec::with_capacity(n);
    let mut exps: Vec<i64> = Vec::with_capacity(n);
    for (k, (v, e)) in latest {
        slots.push(crc16::key_slot(&k) as i32);
        vals.push(v);
        exps.push(e);
        keys.push(k);
    }

    BackgroundWorker::transaction(move || {
        let _ = Spi::connect(|mut client| {
            let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![
                (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), keys.into_datum()),
                (PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID), slots.into_datum()),
                (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), vals.into_datum()),
                (PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID), exps.into_datum()),
            ];
            client.update(
                "INSERT INTO supacache.kv (tenant,key,slot,kind,val,expires_at,version) \
                 SELECT '', k, s, 's', v, e, 1 \
                 FROM unnest($1::bytea[], $2::int[], $3::bytea[], $4::bigint[]) \
                      AS t(k, s, v, e) \
                 ON CONFLICT (tenant,key) DO UPDATE SET \
                 val=EXCLUDED.val, expires_at=EXCLUDED.expires_at, \
                 slot=EXCLUDED.slot, version=supacache.kv.version+1",
                None,
                Some(args),
            )?;
            Ok::<(), pgrx::spi::Error>(())
        });
    });
}

// ---- the SQL surface (§6): in-backend shared-memory reads/writes ---------

#[pg_schema]
mod supacache {
    use super::*;

    /// Direct shared-memory read in the calling backend — no socket, no copy
    /// beyond the returned value (§6). This is the path a `supatype_mask` read
    /// predicate would use for a permission-set lookup.
    ///
    /// STABLE, never IMMUTABLE: §4.3b forbids anything downstream of a mask
    /// predicate from being folded/cached by identity — an IMMUTABLE cache read
    /// would let the planner bake one caller's value into a generic plan.
    #[pg_extern(stable, parallel_safe)]
    fn get(key: &str) -> Option<Vec<u8>> {
        let store = store_view()?;
        match store.get(key.as_bytes()) {
            Lookup::Hit(v) => Some(v.to_vec()),
            Lookup::Miss => None,
        }
    }

    #[pg_extern]
    fn set(key: &str, val: &[u8], ttl_seconds: default!(i64, 0)) -> bool {
        let store = match store_view() {
            Some(s) => s,
            None => return false,
        };
        let ttl_micros = if ttl_seconds > 0 {
            ttl_seconds * 1_000_000
        } else {
            0
        };
        store.set(key.as_bytes(), val, ttl_micros)
    }

    #[pg_extern]
    fn incr(key: &str, by: default!(i64, 1)) -> Option<i64> {
        store_view().and_then(|s| s.incr(key.as_bytes(), by))
    }

    #[pg_extern]
    fn del(key: &str) -> bool {
        store_view().map(|s| s.del(key.as_bytes())).unwrap_or(false)
    }

    #[pg_extern]
    fn getset(key: &str, val: &[u8]) -> Option<Vec<u8>> {
        let store = store_view()?;
        let old = match store.get(key.as_bytes()) {
            Lookup::Hit(v) => Some(v.to_vec()),
            Lookup::Miss => None,
        };
        store.set(key.as_bytes(), val, 0);
        old
    }

    #[pg_extern]
    fn ping() -> &'static str {
        "PONG"
    }

    /// Persistence ring diagnostics: total writes enqueued, writes dropped due
    /// to ring-full backpressure, and current unconsumed backlog in bytes.
    #[pg_extern]
    fn ring_stats() -> TableIterator<
        'static,
        (
            name!(pushed, i64),
            name!(dropped, i64),
            name!(backlog_bytes, i64),
        ),
    > {
        let base = RING_BASE.load(Ordering::Acquire);
        let mut rows = Vec::new();
        if !base.is_null() {
            let stride = ring_stride();
            let (mut p, mut d, mut b) = (0i64, 0i64, 0i64);
            for i in 0..ring_count() {
                let c = unsafe { ring::Consumer::attach(base.add(i * stride)) };
                let (pushed, dropped, backlog) = c.stats();
                p += pushed as i64;
                d += dropped as i64;
                b += backlog as i64;
            }
            rows.push((p, d, b));
        }
        TableIterator::new(rows)
    }

    // ---- in-backend micro-benchmarks (§6) --------------------------------
    // These time the raw shared-memory op inside the calling backend, with no
    // client protocol round-trip, so they isolate the ~1-2µs claim from the
    // ~30-50µs libpq round-trip that a plain `SELECT supacache.get()` incurs.

    /// Returns nanoseconds per `get` hit, averaged over `iters` iterations.
    #[pg_extern]
    fn bench_get(key: &str, val: &[u8], iters: i64) -> f64 {
        let store = match store_view() {
            Some(s) => s,
            None => return -1.0,
        };
        store.set(key.as_bytes(), val, 0);
        let k = key.as_bytes();
        let n = iters.max(1);
        let t0 = std::time::Instant::now();
        let mut acc = 0u64;
        for _ in 0..n {
            if let Lookup::Hit(v) = store.get(std::hint::black_box(k)) {
                acc = acc.wrapping_add(v.len() as u64);
            }
        }
        let el = t0.elapsed();
        std::hint::black_box(acc);
        el.as_nanos() as f64 / n as f64
    }

    /// Returns nanoseconds per `set` (overwrite in place), over `iters`.
    #[pg_extern]
    fn bench_set(key: &str, val: &[u8], iters: i64) -> f64 {
        let store = match store_view() {
            Some(s) => s,
            None => return -1.0,
        };
        let k = key.as_bytes();
        let n = iters.max(1);
        let t0 = std::time::Instant::now();
        for _ in 0..n {
            store.set(std::hint::black_box(k), std::hint::black_box(val), 0);
        }
        t0.elapsed().as_nanos() as f64 / n as f64
    }

    /// Returns nanoseconds per `incr`, over `iters`.
    #[pg_extern]
    fn bench_incr(key: &str, iters: i64) -> f64 {
        let store = match store_view() {
            Some(s) => s,
            None => return -1.0,
        };
        let k = key.as_bytes();
        let n = iters.max(1);
        let t0 = std::time::Instant::now();
        for _ in 0..n {
            store.incr(std::hint::black_box(k), 1);
        }
        t0.elapsed().as_nanos() as f64 / n as f64
    }

    /// Preload `n` keys of `val_bytes` each, so RESP GET benchmarks hit.
    #[pg_extern]
    fn warm(n: i64, val_bytes: i64) -> i64 {
        let store = match store_view() {
            Some(s) => s,
            None => return 0,
        };
        let val = vec![b'x'; val_bytes.max(1) as usize];
        let mut done = 0i64;
        for i in 0..n.max(0) {
            let key = format!("key:{i}");
            if store.set(key.as_bytes(), &val, 0) {
                done += 1;
            }
        }
        done
    }

    #[pg_extern]
    fn stats() -> TableIterator<
        'static,
        (
            name!(partition, i32),
            name!(entries, i64),
            name!(hits, i64),
            name!(misses, i64),
            name!(evictions, i64),
            name!(sets, i64),
            name!(tombstones, i64),
            name!(rehashes, i64),
            name!(data_used, i64),
            name!(data_cap, i64),
        ),
    > {
        let mut rows = Vec::new();
        if let Some(store) = store_view() {
            for p in 0..store.num_partitions() {
                let s = store.stats(p);
                rows.push((
                    p as i32,
                    s.entries as i64,
                    s.hits as i64,
                    s.misses as i64,
                    s.evictions as i64,
                    s.sets as i64,
                    s.tombstones as i64,
                    s.rehashes as i64,
                    s.data_used as i64,
                    s.data_cap as i64,
                ));
            }
        }
        TableIterator::new(rows)
    }
}

// silence unused warnings for the c_void import used only in casts on some paths
#[allow(dead_code)]
fn _keep(_: *mut c_void) {}
