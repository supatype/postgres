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
use server::{AclRule, AuthConfig, Cred};
use std::collections::{HashMap, HashSet};
use store::{Config, Lookup, Store};

const SEG_NAME: &CStr = c"pg_keyspace_segment";
const RING_NAME: &CStr = c"pg_keyspace_ring";
// Mode B row cache lives in its OWN segment — never RESP-addressable (§0: Mode A
// and Mode B "must not share a code path").
const ROWCACHE_NAME: &CStr = c"pg_keyspace_rowcache";

// Base address of the Postgres shared-memory segment, published by the startup
// hook and inherited by every forked backend. Each context rebuilds a cheap
// `Store` view over it on demand.
static SEG_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the SPSC persistence ring (RESP worker -> persistence worker).
static RING_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());
// Base of the Mode B row-cache segment (read by the planner-hook custom scan).
static ROWCACHE_BASE: AtomicPtr<u8> = AtomicPtr::new(std::ptr::null_mut());

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
// TTL by partition drop (§3.3): time-bucket width and sweep interval.
static GUC_TTL_BUCKET_SECS: GucSetting<i32> = GucSetting::<i32>::new(10);
static GUC_TTL_SWEEP_SECS: GucSetting<i32> = GucSetting::<i32>::new(5);
// Mode B row cache (§7.1) segment size.
static GUC_ROWCACHE_MB: GucSetting<i32> = GucSetting::<i32>::new(64);

/// KV store config for the Mode B row cache: keys are (relid,pk) 12-byte tuples,
/// values are raw heap-tuple bytes.
fn rowcache_config() -> Config {
    let mb = GUC_ROWCACHE_MB.get().max(1) as u64;
    let bytes = mb * 1024 * 1024;
    let entries = 200_000u32;
    let buckets = (entries * 2).next_power_of_two();
    Config {
        num_partitions: 1,
        buckets_per_part: buckets,
        entries_per_part: entries,
        data_bytes_per_part: bytes,
    }
}

/// A row-cache Store view over the Mode B segment (any backend).
fn rowcache_view() -> Option<Store> {
    let base = ROWCACHE_BASE.load(Ordering::Acquire);
    if base.is_null() {
        return None;
    }
    Some(unsafe { Store::from_raw(base, &rowcache_config(), false) })
}

/// Row-cache key = relid (u32 LE) ++ pk (i64 LE).
fn rc_key(relid: u32, pk: i64) -> [u8; 12] {
    let mut k = [0u8; 12];
    k[..4].copy_from_slice(&relid.to_le_bytes());
    k[4..].copy_from_slice(&pk.to_le_bytes());
    k
}

fn ttl_bucket_us() -> i64 {
    GUC_TTL_BUCKET_SECS.get().max(1) as i64 * 1_000_000
}

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
    GucRegistry::define_int_guc(
        "pg_keyspace.ttl_bucket_secs",
        "Width of a TTL time-bucket partition, in seconds (§3.3)",
        "Keys with a TTL persist into supacache.kv_ttl, range-partitioned by expiry bucket.",
        &GUC_TTL_BUCKET_SECS,
        1,
        86_400,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.ttl_sweep_secs",
        "How often the expiry worker drops fully-past TTL partitions, in seconds",
        "",
        &GUC_TTL_SWEEP_SECS,
        1,
        3_600,
        GucContext::Postmaster,
        GucFlags::empty(),
    );
    GucRegistry::define_int_guc(
        "pg_keyspace.rowcache_mb",
        "Size of the Mode B row-cache shared-memory segment, in MB (§7.1)",
        "Holds raw heap-tuple bytes keyed by (relid, pk); read by the planner-hook custom scan.",
        &GUC_ROWCACHE_MB,
        1,
        4096,
        GucContext::Postmaster,
        GucFlags::empty(),
    );

    // Mode B: register the custom-scan methods and install the pathlist hook.
    rowcache_planner_init();

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
    // The RESP worker always needs SPI now: for recovery (persisted tiers) and
    // to load the RESP AUTH credentials / keyspace ACL (§4.5) at startup.
    let persisted = ks_tier() != Tier::Ephemeral;
    BackgroundWorkerBuilder::new("pg_keyspace: RESP slot worker")
        .set_library("pg_keyspace")
        .set_function("pg_keyspace_worker_main")
        .set_restart_time(Some(Duration::from_secs(2)))
        .enable_spi_access()
        .load();

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
        // §3.3 expiry worker: drops fully-past TTL partitions.
        BackgroundWorkerBuilder::new("pg_keyspace: expiry worker")
            .set_library("pg_keyspace")
            .set_function("pg_keyspace_expiry_main")
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
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
        pg_sys::RequestAddinShmemSpace(rowcache_config().total_bytes());
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

        // Mode B row-cache segment (§7.1).
        let rc_cfg = rowcache_config();
        let rc_bytes = rc_cfg.total_bytes();
        let mut rc_found = false;
        let rcptr = pg_sys::ShmemInitStruct(ROWCACHE_NAME.as_ptr(), rc_bytes, &mut rc_found) as *mut u8;
        if !rcptr.is_null() {
            let _ = Store::from_raw(rcptr, &rc_cfg, !rc_found);
            ROWCACHE_BASE.store(rcptr, Ordering::Release);
        }
        log!(
            "pg_keyspace: shmem ready (store {} bytes, {} rings x {} bytes, rowcache {} bytes, found={})",
            size,
            ring_count(),
            stride,
            rc_bytes,
            found
        );
    }
}

// ---- the background worker: the RESP event loop --------------------------

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_worker_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);

    // §4.1 load-order assertion: supatype_mask must be loaded AFTER pg_keyspace
    // (outermost) so the Query is masked before pg_keyspace ever sees it. If the
    // operator misordered shared_preload_libraries, FAIL CLOSED — park without
    // binding the RESP port rather than serve on an unverified security posture.
    if let Err(why) = check_load_order() {
        log!("pg_keyspace worker: REFUSING to start — {why}");
        while !BackgroundWorker::sigterm_received() {
            std::thread::sleep(Duration::from_secs(1));
        }
        return;
    }

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

    // Connect SPI (always): needed to create/read the schema, run the §4.5
    // security self-check, load RESP AUTH credentials, and recover from tables.
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    pg_ensure_schema();

    // §4.5 security label self-check: a Mode A backing table must have no
    // `supatype` label. If one was added by hand, FAIL CLOSED.
    if kv_has_supatype_label() {
        log!(
            "pg_keyspace worker: REFUSING to start — a supatype security label exists on a \
             supacache relation; Mode A tables must not be masked (§4.5)"
        );
        while !BackgroundWorker::sigterm_received() {
            std::thread::sleep(Duration::from_secs(1));
        }
        return;
    }

    // §4.5 load RESP AUTH + keyspace ACL. When credentials exist, enforcement is
    // on; when absent, the worker runs in local/no-auth mode (§10 local dev).
    match load_auth_config() {
        Some(auth) => {
            let n = auth.creds.len();
            worker.set_auth_config(auth);
            log!("pg_keyspace worker: AUTH enforced ({n} credentials, ACL + tenant scoping on)");
        }
        None => log!("pg_keyspace worker: no RESP credentials configured — local/no-auth mode"),
    }

    // P1 storage & durability: recover shmem from the tables at startup; the
    // steady-state persistence is offloaded to the persistence worker via the
    // ring, so the RESP hot path never touches SPI.
    if persisted {
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
            let nr = ring_count();
            let producers: Vec<ring::Producer> =
                (0..nr).map(|i| unsafe { ring::Producer::attach(rbase.add(i * stride)) }).collect();
            worker.set_ring_producers(producers);
            // durable/replicated: hold each write's RESP OK until it commits.
            let sync_ack = matches!(ks_tier(), Tier::Durable | Tier::Replicated);
            worker.set_sync_ack(sync_ack);
            log!(
                "pg_keyspace worker: persistence ON ({nr} rings -> {nr} workers, sync_ack={sync_ack})"
            );
        }
    }

    log!("pg_keyspace worker: RESP listening on 0.0.0.0:{port} (persisted={persisted})");
    // Poll frequently in sync-ack mode so committed durable writes ack promptly.
    let timeout_ms = if matches!(ks_tier(), Tier::Durable | Tier::Replicated) {
        2
    } else {
        500
    };
    let _ = worker.run_with(|| !BackgroundWorker::sigterm_received(), timeout_ms);
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
    // The RESP worker owns schema creation; all persist workers wait for it.
    for _ in 0..100 {
        if pg_table_ready() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    let rbase = RING_BASE.load(Ordering::Acquire);
    if rbase.is_null() {
        log!("pg_keyspace persist {idx}: ring not ready, exiting");
        return;
    }
    let consumer = unsafe { ring::Consumer::attach(rbase.add(idx * ring_stride())) };
    let idle = Duration::from_millis(GUC_PERSIST_WINDOW_MS.get().max(1) as u64);
    let sync_commit: &'static str = match ks_tier() {
        Tier::Durable => "on",
        Tier::Replicated => "remote_apply", // needs a synchronous standby
        _ => "off",                          // relaxed: RESP already acked
    };
    log!("pg_keyspace persist {idx}: draining ring {idx} -> supacache.kv (synchronous_commit={sync_commit})");

    while !BackgroundWorker::sigterm_received() {
        let mut batch: Vec<server::PendingWrite> = Vec::with_capacity(8192);
        consumer.drain(20_000, |k, v, e| batch.push((k.to_vec(), v.to_vec(), e)));
        if batch.is_empty() {
            std::thread::sleep(idle);
            continue;
        }
        bulk_upsert(batch, sync_commit, ttl_bucket_us());
        consumer.mark_committed(); // release durable acks waiting on these records
    }
    // final drain on shutdown
    let mut tail: Vec<server::PendingWrite> = Vec::new();
    consumer.drain(usize::MAX, |k, v, e| tail.push((k.to_vec(), v.to_vec(), e)));
    if !tail.is_empty() {
        bulk_upsert(tail, sync_commit, ttl_bucket_us());
        consumer.mark_committed();
    }
    log!("pg_keyspace persist: shutting down");
}

/// §4.1: verify `supatype_mask` is present in shared_preload_libraries AND
/// loaded after `pg_keyspace` (so it is the outermost planner hook). Returns
/// Err with a human-readable reason when the posture is wrong.
fn check_load_order() -> Result<(), String> {
    let spl = unsafe {
        let s = pg_sys::GetConfigOption(c"shared_preload_libraries".as_ptr(), true, false);
        if s.is_null() {
            String::new()
        } else {
            CStr::from_ptr(s).to_string_lossy().into_owned()
        }
    };
    let libs: Vec<&str> = spl.split(',').map(|s| s.trim()).collect();
    let ks = libs.iter().position(|&x| x == "pg_keyspace");
    let mask = libs.iter().position(|&x| x == "supatype_mask");
    match (ks, mask) {
        (None, _) => Err(format!(
            "pg_keyspace not found in shared_preload_libraries ('{spl}')"
        )),
        (_, None) => Err(format!(
            "supatype_mask not in shared_preload_libraries ('{spl}'); \
             RESP would serve rows the mask never rewrote"
        )),
        (Some(k), Some(m)) if k >= m => Err(format!(
            "supatype_mask (pos {m}) must load AFTER pg_keyspace (pos {k}) so it is \
             outermost; fix the order in shared_preload_libraries ('{spl}')"
        )),
        _ => Ok(()),
    }
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
        // P2: RESP credential -> role/tenant map (§4.5) and keyspace ACL.
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.resp_credential (\
             username text PRIMARY KEY, secret text NOT NULL, \
             role_name text NOT NULL, tenant text NOT NULL DEFAULT '')",
        );
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.acl (\
             role_name text NOT NULL, prefix text NOT NULL, \
             can_read boolean NOT NULL DEFAULT true, \
             can_write boolean NOT NULL DEFAULT true, \
             PRIMARY KEY (role_name, prefix))",
        );
        // §3.3: TTL'd keys persist here, RANGE-partitioned by expiry time bucket,
        // so expiry is a whole-partition DROP (O(1), no vacuum churn) rather than
        // row-by-row DELETE. Partitions are created on demand by the persist
        // worker and dropped by the expiry worker.
        let _ = Spi::run(
            "CREATE TABLE IF NOT EXISTS supacache.kv_ttl (\
             tenant text NOT NULL DEFAULT '', key bytea NOT NULL, slot int NOT NULL, \
             val bytea, expires_at bigint NOT NULL, bucket bigint NOT NULL, \
             PRIMARY KEY (bucket, tenant, key)) PARTITION BY RANGE (bucket)",
        );
    });
}

/// Ensure the TTL bucket partition for `bucket` exists (idempotent).
fn ensure_ttl_partition(client: &mut pgrx::spi::SpiClient, bucket: i64) {
    let _ = client.update(
        &format!(
            "CREATE TABLE IF NOT EXISTS supacache.kv_ttl_b{bucket} \
             PARTITION OF supacache.kv_ttl FOR VALUES FROM ({bucket}) TO ({})",
            bucket + 1
        ),
        None,
        None,
    );
}

/// §4.5 self-check: Mode A backing tables must carry NO `supatype` security
/// label — access control for the keyspace is the ACL layer, not masking. If a
/// label was added by hand, refuse to serve (fail closed). Returns true if any
/// `supacache.*` relation has a `supatype` label.
fn kv_has_supatype_label() -> bool {
    BackgroundWorker::transaction(|| {
        Spi::get_one::<i64>(
            "SELECT count(*) FROM pg_seclabel l \
             JOIN pg_class c ON c.oid = l.objoid \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE l.provider = 'supatype' AND n.nspname = 'supacache'",
        )
        .ok()
        .flatten()
        .unwrap_or(0)
            > 0
    })
}

/// Load RESP credentials + keyspace ACL from SQL (§4.5). Returns None when no
/// credentials are configured — the worker then runs in local/no-auth mode.
/// Exempt roles come from `supatype_mask.exempt_roles` so the two never drift.
fn load_auth_config() -> Option<AuthConfig> {
    use std::panic::AssertUnwindSafe;
    let exempt: HashSet<String> = {
        let raw = unsafe {
            let s = pg_sys::GetConfigOption(c"supatype_mask.exempt_roles".as_ptr(), true, false);
            if s.is_null() {
                String::new()
            } else {
                CStr::from_ptr(s).to_string_lossy().into_owned()
            }
        };
        raw.split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect()
    };

    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let mut creds: HashMap<String, Cred> = HashMap::new();
        let mut acl: HashMap<String, Vec<AclRule>> = HashMap::new();
        let _ = Spi::connect(|client| {
            let t = client.select(
                "SELECT username, secret, role_name, tenant FROM supacache.resp_credential",
                None,
                None,
            )?;
            for row in t {
                let u: String = row.get::<String>(1)?.unwrap_or_default();
                let s: String = row.get::<String>(2)?.unwrap_or_default();
                let r: String = row.get::<String>(3)?.unwrap_or_default();
                let tn: String = row.get::<String>(4)?.unwrap_or_default();
                if !u.is_empty() {
                    creds.insert(u, Cred { secret: s, role: r, tenant: tn });
                }
            }
            let t = client.select(
                "SELECT role_name, prefix, can_read, can_write FROM supacache.acl",
                None,
                None,
            )?;
            for row in t {
                let r: String = row.get::<String>(1)?.unwrap_or_default();
                let p: String = row.get::<String>(2)?.unwrap_or_default();
                let cr: bool = row.get::<bool>(3)?.unwrap_or(false);
                let cw: bool = row.get::<bool>(4)?.unwrap_or(false);
                acl.entry(r).or_default().push(AclRule {
                    prefix: p.into_bytes(),
                    can_read: cr,
                    can_write: cw,
                });
            }
            Ok::<(), pgrx::spi::Error>(())
        });
        if creds.is_empty() {
            None
        } else {
            Some(AuthConfig { creds, acl, exempt })
        }
    }))
}

/// Load live keys from `supacache.kv` into shmem at startup (crash recovery).
/// Expired rows are skipped. Returns the number of keys restored.
fn pg_recover(store: &Store) -> i64 {
    use std::panic::AssertUnwindSafe;
    let now = store::now_micros();
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        Spi::connect(|client| {
            let mut cnt = 0i64;
            // no-TTL keys from kv, then non-expired TTL keys from kv_ttl (latest
            // expiry per key wins, so a re-SET into a newer bucket takes effect).
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
            let tup = client.select(
                "SELECT DISTINCT ON (key) key, val, expires_at FROM supacache.kv_ttl \
                 WHERE expires_at > $1 ORDER BY key, expires_at DESC",
                None,
                Some(vec![(
                    PgOid::BuiltIn(PgBuiltInOids::INT8OID),
                    now.into_datum(),
                )]),
            )?;
            for row in tup {
                let k: Option<Vec<u8>> = row.get(1)?;
                let v: Option<Vec<u8>> = row.get(2)?;
                let e: i64 = row.get::<i64>(3)?.unwrap_or(0);
                if let (Some(k), Some(v)) = (k, v) {
                    store.set(&k, &v, (e - now).max(1));
                    cnt += 1;
                }
            }
            Ok::<i64, pgrx::spi::Error>(cnt)
        })
        .unwrap_or(0)
    }))
}

/// Apply a drained batch in one transaction (§3.4, §3.3). Deduplicated by key
/// (last op wins), then split three ways: no-TTL upserts -> `supacache.kv`;
/// TTL'd upserts -> `supacache.kv_ttl` (range-partitioned by expiry bucket, so
/// expiry is a partition DROP); tombstones -> delete from both.
fn bulk_upsert(batch: Vec<server::PendingWrite>, sync_commit: &'static str, bucket_us: i64) {
    use std::collections::{HashMap, HashSet};
    if batch.is_empty() {
        return;
    }
    let mut latest: HashMap<Vec<u8>, (Vec<u8>, i64)> = HashMap::with_capacity(batch.len());
    for (k, v, e) in batch {
        latest.insert(k, (v, e)); // last op for a key wins (SET then DEL -> DEL)
    }
    // no-TTL upserts -> kv
    let (mut keys, mut slots, mut vals) =
        (Vec::<Vec<u8>>::new(), Vec::<i32>::new(), Vec::<Vec<u8>>::new());
    // TTL upserts -> kv_ttl
    let (mut tkeys, mut tslots, mut tvals, mut texps, mut tbuckets) = (
        Vec::<Vec<u8>>::new(),
        Vec::<i32>::new(),
        Vec::<Vec<u8>>::new(),
        Vec::<i64>::new(),
        Vec::<i64>::new(),
    );
    let mut del_keys: Vec<Vec<u8>> = Vec::new();
    let mut buckets_seen: HashSet<i64> = HashSet::new();
    for (k, (v, e)) in latest {
        if e == server::DELETE_TOMBSTONE {
            del_keys.push(k);
        } else if e > 0 {
            let b = e / bucket_us;
            buckets_seen.insert(b);
            tslots.push(crc16::key_slot(&k) as i32);
            tvals.push(v);
            texps.push(e);
            tbuckets.push(b);
            tkeys.push(k);
        } else {
            slots.push(crc16::key_slot(&k) as i32);
            vals.push(v);
            keys.push(k);
        }
    }

    BackgroundWorker::transaction(move || {
        let _ = Spi::connect(|mut client| {
            // Durability tier per §3.4: relaxed=off (async, RESP already acked),
            // durable=on (fsync), replicated=remote_apply (needs a standby).
            let _ = client.update(
                &format!("SET LOCAL synchronous_commit = '{sync_commit}'"),
                None,
                None,
            );
            if !keys.is_empty() {
                let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), keys.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID), slots.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), vals.into_datum()),
                ];
                client.update(
                    "INSERT INTO supacache.kv (tenant,key,slot,kind,val,expires_at,version) \
                     SELECT '', k, s, 's', v, 0, 1 \
                     FROM unnest($1::bytea[], $2::int[], $3::bytea[]) AS t(k, s, v) \
                     ON CONFLICT (tenant,key) DO UPDATE SET \
                     val=EXCLUDED.val, expires_at=0, slot=EXCLUDED.slot, \
                     version=supacache.kv.version+1",
                    None,
                    Some(args),
                )?;
            }
            if !tkeys.is_empty() {
                for b in &buckets_seen {
                    ensure_ttl_partition(&mut client, *b);
                }
                let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), tkeys.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT4ARRAYOID), tslots.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID), tvals.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID), texps.into_datum()),
                    (PgOid::BuiltIn(PgBuiltInOids::INT8ARRAYOID), tbuckets.into_datum()),
                ];
                client.update(
                    "INSERT INTO supacache.kv_ttl (tenant,key,slot,val,expires_at,bucket) \
                     SELECT '', k, s, v, e, b \
                     FROM unnest($1::bytea[], $2::int[], $3::bytea[], $4::bigint[], $5::bigint[]) \
                          AS t(k, s, v, e, b) \
                     ON CONFLICT (bucket,tenant,key) DO UPDATE SET \
                     val=EXCLUDED.val, expires_at=EXCLUDED.expires_at, slot=EXCLUDED.slot",
                    None,
                    Some(args),
                )?;
            }
            if !del_keys.is_empty() {
                let args: Vec<(PgOid, Option<pg_sys::Datum>)> = vec![(
                    PgOid::BuiltIn(PgBuiltInOids::BYTEAARRAYOID),
                    del_keys.into_datum(),
                )];
                client.update(
                    "DELETE FROM supacache.kv WHERE tenant='' AND key = ANY($1::bytea[])",
                    None,
                    Some(args.clone()),
                )?;
                client.update(
                    "DELETE FROM supacache.kv_ttl WHERE tenant='' AND key = ANY($1::bytea[])",
                    None,
                    Some(args),
                )?;
            }
            Ok::<(), pgrx::spi::Error>(())
        });
    });
}

/// The expiry worker (§3.1, §3.3): periodically DROP TTL partitions whose whole
/// time bucket is in the past. This is O(1) DDL per partition — no row-by-row
/// DELETE, no vacuum churn. Shmem expiry stays lazy-on-read.
#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_keyspace_expiry_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM | SignalWakeFlags::SIGHUP);
    let dbname = GUC_DATABASE
        .get()
        .and_then(|c| c.to_str().ok())
        .unwrap_or("postgres")
        .to_string();
    BackgroundWorker::connect_worker_to_spi(Some(&dbname), None);
    for _ in 0..100 {
        if pg_table_ready() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let sweep = Duration::from_secs(GUC_TTL_SWEEP_SECS.get().max(1) as u64);
    log!("pg_keyspace expiry: dropping past TTL partitions every {sweep:?}");
    while !BackgroundWorker::sigterm_received() {
        let now_bucket = store::now_micros() / ttl_bucket_us();
        let dropped = drop_expired_partitions(now_bucket);
        if dropped > 0 {
            log!("pg_keyspace expiry: dropped {dropped} expired TTL partition(s)");
        }
        std::thread::sleep(sweep);
    }
    log!("pg_keyspace expiry: shutting down");
}

/// DROP every `supacache.kv_ttl_b<N>` partition with N < `now_bucket` (fully
/// past). Returns how many were dropped. O(1) DDL per partition.
fn drop_expired_partitions(now_bucket: i64) -> i64 {
    use std::panic::AssertUnwindSafe;
    BackgroundWorker::transaction(AssertUnwindSafe(|| {
        let names: Vec<String> = Spi::connect(|client| {
            let mut v = Vec::new();
            let t = client.select(
                "SELECT c.relname::text FROM pg_inherits i \
                 JOIN pg_class c ON c.oid = i.inhrelid \
                 JOIN pg_class p ON p.oid = i.inhparent \
                 JOIN pg_namespace n ON n.oid = p.relnamespace \
                 WHERE n.nspname='supacache' AND p.relname='kv_ttl'",
                None,
                None,
            )?;
            for row in t {
                if let Some(name) = row.get::<String>(1)? {
                    v.push(name);
                }
            }
            Ok::<Vec<String>, pgrx::spi::Error>(v)
        })
        .unwrap_or_default();

        let mut dropped = 0i64;
        for name in names {
            if let Some(nstr) = name.strip_prefix("kv_ttl_b") {
                if let Ok(bucket) = nstr.parse::<i64>() {
                    if bucket < now_bucket {
                        let _ = Spi::run(&format!("DROP TABLE supacache.{name}"));
                        dropped += 1;
                    }
                }
            }
        }
        dropped
    }))
}

// ==== Mode B: transparent row cache via a planner custom scan (§7.1) =======
//
// set_rel_pathlist_hook adds a CustomPath for a registered cached relation with
// a `pk = Const` restriction whose row is currently cached. The CustomScan sets
// scanrelid = the base rel, so ExecInitCustomScan builds the scan slot from the
// table's tupdesc AND initialises ps.qual (from plan.qual) and the projection
// (from plan.targetlist). We pass the rel's restriction clauses through as
// plan.qual and keep the (already mask-rewritten) targetlist, so ExecScan
// re-applies RLS quals and the mask CASE to the cached row (§4.6). We serve the
// RAW row only; never post-policy output.

use core::ffi::c_char;

struct SyncPtr<T>(T);
unsafe impl<T> Sync for SyncPtr<T> {}

static RC_SCAN_METHODS: SyncPtr<pg_sys::CustomScanMethods> = SyncPtr(pg_sys::CustomScanMethods {
    CustomName: c"pg_keyspace_rowcache".as_ptr() as *const c_char,
    CreateCustomScanState: Some(rc_create_state),
});

static RC_EXEC_METHODS: SyncPtr<pg_sys::CustomExecMethods> = SyncPtr(pg_sys::CustomExecMethods {
    CustomName: c"pg_keyspace_rowcache".as_ptr() as *const c_char,
    BeginCustomScan: Some(rc_begin),
    ExecCustomScan: Some(rc_exec),
    EndCustomScan: Some(rc_end),
    ReScanCustomScan: Some(rc_rescan),
    MarkPosCustomScan: None,
    RestrPosCustomScan: None,
    EstimateDSMCustomScan: None,
    InitializeDSMCustomScan: None,
    ReInitializeDSMCustomScan: None,
    InitializeWorkerCustomScan: None,
    ShutdownCustomScan: None,
    ExplainCustomScan: None,
});

static RC_PATH_METHODS: SyncPtr<pg_sys::CustomPathMethods> = SyncPtr(pg_sys::CustomPathMethods {
    CustomName: c"pg_keyspace_rowcache".as_ptr() as *const c_char,
    PlanCustomPath: Some(rc_plan),
    ReparameterizeCustomPathByChild: None,
});

static mut PREV_PATHLIST_HOOK: pg_sys::set_rel_pathlist_hook_type = None;

/// Execution state; `css` must be first so a `*CustomScanState` aliases it.
#[repr(C)]
struct RcScanState {
    css: pg_sys::CustomScanState,
    pk: i64,
    done: bool,
}

fn rowcache_planner_init() {
    unsafe {
        pg_sys::RegisterCustomScanMethods(&RC_SCAN_METHODS.0);
        PREV_PATHLIST_HOOK = pg_sys::set_rel_pathlist_hook;
        pg_sys::set_rel_pathlist_hook = Some(rc_pathlist_hook);
    }
}

fn rc_reg_key(relid: u32) -> [u8; 5] {
    let mut k = [0xffu8; 5];
    k[1..].copy_from_slice(&relid.to_le_bytes());
    k
}

/// Find a `pkcol = Const` restriction on the given attnum and return the pk.
unsafe fn find_pk_const(rel: *mut pg_sys::RelOptInfo, attnum: i16) -> Option<i64> {
    let cell = (*(*rel).baserestrictinfo).elements;
    let n = (*(*rel).baserestrictinfo).length;
    for i in 0..n {
        let ri = (*cell.offset(i as isize)).ptr_value as *mut pg_sys::RestrictInfo;
        if ri.is_null() {
            continue;
        }
        let clause = (*ri).clause as *mut pg_sys::Node;
        if clause.is_null() || (*clause).type_ != pg_sys::NodeTag::T_OpExpr {
            continue;
        }
        let op = clause as *mut pg_sys::OpExpr;
        // must be the "=" operator
        let opname = pg_sys::get_opname((*op).opno);
        if opname.is_null() || CStr::from_ptr(opname).to_bytes() != b"=" {
            continue;
        }
        let args = (*op).args;
        if args.is_null() || (*args).length != 2 {
            continue;
        }
        let a0 = (*(*args).elements.offset(0)).ptr_value as *mut pg_sys::Node;
        let a1 = (*(*args).elements.offset(1)).ptr_value as *mut pg_sys::Node;
        // Skip clauses that aren't `Var = Const` (e.g. an RLS `owner =
        // CURRENT_USER` predicate) rather than aborting the whole search.
        let (var, cst) = match classify(a0, a1) {
            Some(vc) => vc,
            None => continue,
        };
        if (*var).varattno != attnum {
            continue;
        }
        if let Some(pk) = const_i64(cst) {
            return Some(pk);
        }
    }
    None
}

/// Return (Var, Const) from a pair in either order, if it is exactly that shape.
unsafe fn classify(
    a: *mut pg_sys::Node,
    b: *mut pg_sys::Node,
) -> Option<(*mut pg_sys::Var, *mut pg_sys::Const)> {
    let ta = (*a).type_;
    let tb = (*b).type_;
    if ta == pg_sys::NodeTag::T_Var && tb == pg_sys::NodeTag::T_Const {
        Some((a as *mut pg_sys::Var, b as *mut pg_sys::Const))
    } else if ta == pg_sys::NodeTag::T_Const && tb == pg_sys::NodeTag::T_Var {
        Some((b as *mut pg_sys::Var, a as *mut pg_sys::Const))
    } else {
        None
    }
}

unsafe fn const_i64(cst: *mut pg_sys::Const) -> Option<i64> {
    if (*cst).constisnull {
        return None;
    }
    match (*cst).consttype {
        pg_sys::INT8OID => Some((*cst).constvalue.value() as i64),
        pg_sys::INT4OID => Some((*cst).constvalue.value() as i32 as i64),
        pg_sys::INT2OID => Some((*cst).constvalue.value() as i16 as i64),
        _ => None,
    }
}

#[pg_guard]
unsafe extern "C" fn rc_pathlist_hook(
    root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    rti: pg_sys::Index,
    rte: *mut pg_sys::RangeTblEntry,
) {
    if let Some(prev) = PREV_PATHLIST_HOOK {
        prev(root, rel, rti, rte);
    }
    if (*rel).reloptkind != pg_sys::RelOptKind::RELOPT_BASEREL
        || (*rte).rtekind != pg_sys::RTEKind::RTE_RELATION
    {
        return;
    }
    let relid_u32 = (*rte).relid.as_u32();
    let view = match rowcache_view() {
        Some(v) => v,
        None => return,
    };
    let reg = match view.get(&rc_reg_key(relid_u32)) {
        Lookup::Hit(b) if b.len() >= 2 => [b[0], b[1]],
        _ => return,
    };
    let pk_attnum = i16::from_le_bytes(reg);
    let pk = match find_pk_const(rel, pk_attnum) {
        Some(v) => v,
        None => return,
    };
    // Only substitute if the row is actually cached (else normal index path).
    if !matches!(view.get(&rc_key(relid_u32, pk)), Lookup::Hit(_)) {
        return;
    }

    let cpath = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomPath>()) as *mut pg_sys::CustomPath;
    (*cpath).path.type_ = pg_sys::NodeTag::T_CustomPath;
    (*cpath).path.pathtype = pg_sys::NodeTag::T_CustomScan;
    (*cpath).path.parent = rel;
    (*cpath).path.pathtarget = (*rel).reltarget;
    (*cpath).path.rows = 1.0;
    (*cpath).path.startup_cost = 0.0;
    (*cpath).path.total_cost = 0.0001; // beat the index path so this is chosen
    (*cpath).methods = &RC_PATH_METHODS.0;
    // carry pk to execution via a Const in custom_private
    let pkc = pg_sys::makeConst(
        pg_sys::INT8OID,
        -1,
        pg_sys::InvalidOid,
        8,
        pg_sys::Datum::from(pk),
        false,
        true,
    );
    (*cpath).custom_private = pg_sys::lappend(std::ptr::null_mut(), pkc as *mut core::ffi::c_void);
    pg_sys::add_path(rel, cpath as *mut pg_sys::Path);
}

#[pg_guard]
unsafe extern "C" fn rc_plan(
    _root: *mut pg_sys::PlannerInfo,
    rel: *mut pg_sys::RelOptInfo,
    best_path: *mut pg_sys::CustomPath,
    tlist: *mut pg_sys::List,
    clauses: *mut pg_sys::List,
    custom_plans: *mut pg_sys::List,
) -> *mut pg_sys::Plan {
    let cscan = pg_sys::palloc0(std::mem::size_of::<pg_sys::CustomScan>()) as *mut pg_sys::CustomScan;
    (*cscan).scan.plan.type_ = pg_sys::NodeTag::T_CustomScan;
    (*cscan).scan.plan.targetlist = tlist;
    // Re-apply the rel's restriction clauses (incl. RLS) above our scan (§4.6).
    (*cscan).scan.plan.qual = pg_sys::extract_actual_clauses(clauses, false);
    (*cscan).scan.scanrelid = (*rel).relid;
    (*cscan).flags = (*best_path).flags;
    (*cscan).custom_plans = custom_plans;
    (*cscan).custom_private = (*best_path).custom_private;
    (*cscan).methods = &RC_SCAN_METHODS.0;
    cscan as *mut pg_sys::Plan
}

#[pg_guard]
unsafe extern "C" fn rc_create_state(cscan: *mut pg_sys::CustomScan) -> *mut pg_sys::Node {
    let st = pg_sys::palloc0(std::mem::size_of::<RcScanState>()) as *mut RcScanState;
    (*st).css.ss.ps.type_ = pg_sys::NodeTag::T_CustomScanState;
    (*st).css.methods = &RC_EXEC_METHODS.0;
    // serve the cached tuple through a virtual slot (deform on read)
    (*st).css.slotOps = &pg_sys::TTSOpsVirtual;
    let pkc = pg_sys::list_nth((*cscan).custom_private, 0) as *mut pg_sys::Const;
    (*st).pk = if pkc.is_null() {
        0
    } else {
        (*pkc).constvalue.value() as i64
    };
    (*st).done = false;
    st as *mut pg_sys::Node
}

#[pg_guard]
unsafe extern "C" fn rc_begin(
    node: *mut pg_sys::CustomScanState,
    _estate: *mut pg_sys::EState,
    _eflags: core::ffi::c_int,
) {
    (node as *mut RcScanState).as_mut().unwrap().done = false;
}

#[pg_guard]
unsafe extern "C" fn rc_exec(node: *mut pg_sys::CustomScanState) -> *mut pg_sys::TupleTableSlot {
    pg_sys::ExecScan(
        &mut (*node).ss,
        Some(rc_access),
        Some(rc_recheck),
    )
}

/// Access method: return the cached row once (raw), then an empty slot to end.
#[pg_guard]
unsafe extern "C" fn rc_access(ss: *mut pg_sys::ScanState) -> *mut pg_sys::TupleTableSlot {
    let st = ss as *mut RcScanState;
    let slot = (*ss).ss_ScanTupleSlot;
    if (*st).done {
        return pg_sys::ExecClearTuple(slot);
    }
    (*st).done = true;
    let rel = (*ss).ss_currentRelation;
    let relid = (*rel).rd_id.as_u32();
    let view = match rowcache_view() {
        Some(v) => v,
        None => return pg_sys::ExecClearTuple(slot),
    };
    let bytes = match view.get(&rc_key(relid, (*st).pk)) {
        Lookup::Hit(b) => b,
        Lookup::Miss => return pg_sys::ExecClearTuple(slot),
    };
    // Copy the cached tuple bytes into an aligned palloc buffer, wrap as a
    // HeapTuple, deform into the (virtual) scan slot, and store.
    let n = bytes.len();
    let buf = pg_sys::palloc(n) as *mut u8;
    std::ptr::copy_nonoverlapping(bytes.as_ptr(), buf, n);
    let ht = pg_sys::palloc0(std::mem::size_of::<pg_sys::HeapTupleData>()) as *mut pg_sys::HeapTupleData;
    (*ht).t_len = n as u32;
    (*ht).t_data = buf as *mut pg_sys::HeapTupleHeaderData;
    (*ht).t_tableOid = (*rel).rd_id;
    pg_sys::ExecClearTuple(slot);
    let tupdesc = (*slot).tts_tupleDescriptor;
    pg_sys::heap_deform_tuple(ht, tupdesc, (*slot).tts_values, (*slot).tts_isnull);
    pg_sys::ExecStoreVirtualTuple(slot)
}

#[pg_guard]
unsafe extern "C" fn rc_recheck(
    _ss: *mut pg_sys::ScanState,
    _slot: *mut pg_sys::TupleTableSlot,
) -> bool {
    true
}

#[pg_guard]
unsafe extern "C" fn rc_end(_node: *mut pg_sys::CustomScanState) {}

#[pg_guard]
unsafe extern "C" fn rc_rescan(node: *mut pg_sys::CustomScanState) {
    (node as *mut RcScanState).as_mut().unwrap().done = false;
}

/// Quote an SQL identifier (schema/column) via the server's own routine.
fn quote_ident(s: &str) -> String {
    let c = match std::ffi::CString::new(s) {
        Ok(c) => c,
        Err(_) => return String::new(),
    };
    unsafe {
        let q = pg_sys::quote_identifier(c.as_ptr());
        std::ffi::CStr::from_ptr(q).to_string_lossy().into_owned()
    }
}

/// Run a single-row SELECT and return the raw heap-tuple bytes of the first
/// row (HeapTupleHeader + data), copied out before the SPI context is freed.
unsafe fn fetch_raw_tuple(query: &str) -> Option<Vec<u8>> {
    let q = std::ffi::CString::new(query).ok()?;
    if pg_sys::SPI_connect() != pg_sys::SPI_OK_CONNECT as i32 {
        return None;
    }
    let rc = pg_sys::SPI_execute(q.as_ptr(), true, 1);
    let out = if rc == pg_sys::SPI_OK_SELECT as i32 && pg_sys::SPI_processed >= 1 {
        let tuptable = pg_sys::SPI_tuptable;
        let tup = *(*tuptable).vals.offset(0);
        let len = (*tup).t_len as usize;
        let mut buf = vec![0u8; len];
        std::ptr::copy_nonoverlapping((*tup).t_data as *const u8, buf.as_mut_ptr(), len);
        Some(buf)
    } else {
        None
    };
    pg_sys::SPI_finish();
    out
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

    // ---- Mode B: transparent row cache control surface (§7.1) ------------
    // The planner custom scan (see the parent module) substitutes a cached row
    // for a `pk = Const` lookup on a *registered* relation. These functions
    // register a relation's pk column and populate the cache. Populating is a
    // POC stand-in for the logical-decoding invalidation/refill worker (§3.5);
    // it stores the RAW heap-tuple bytes so the scan node re-applies RLS + mask
    // above it (never post-policy output).

    /// Register `tbl`'s primary-key attribute number so the planner hook will
    /// consider substituting cached rows for `pk = Const` lookups on it.
    #[pg_extern]
    fn rowcache_register(tbl: &str, pk_attnum: i32) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        let view = match rowcache_view() {
            Some(v) => v,
            None => return false,
        };
        let attn = (pk_attnum as i16).to_le_bytes();
        view.set(&rc_reg_key(relid.as_u32()), &attn, 0)
    }

    /// Drop a relation's registration; the planner stops substituting for it.
    #[pg_extern]
    fn rowcache_unregister(tbl: &str) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        rowcache_view()
            .map(|v| v.del(&rc_reg_key(relid.as_u32())))
            .unwrap_or(false)
    }

    /// Cache the current row for `tbl` where the registered pk column = `pk`.
    /// Stores the raw heap-tuple bytes (pre-policy) keyed by (relid, pk).
    /// POC note: rows with out-of-line (TOASTed) values are not supported —
    /// the raw tuple would carry a toast pointer, not the datum.
    #[pg_extern]
    fn rowcache_put(tbl: &str, pk: i64) -> bool {
        let relid = match Spi::get_one_with_args::<pg_sys::Oid>(
            "SELECT $1::regclass::oid",
            vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
        ) {
            Ok(Some(o)) => o,
            _ => return false,
        };
        let view = match rowcache_view() {
            Some(v) => v,
            None => return false,
        };
        // must be registered; pk column name comes from the registered attnum
        let attnum = match view.get(&rc_reg_key(relid.as_u32())) {
            Lookup::Hit(b) if b.len() >= 2 => i16::from_le_bytes([b[0], b[1]]),
            _ => return false,
        };
        let raw = unsafe {
            let attname = pg_sys::get_attname(relid, attnum, false);
            if attname.is_null() {
                return false;
            }
            let col = std::ffi::CStr::from_ptr(attname)
                .to_string_lossy()
                .into_owned();
            // Fully-qualified relation name, identifier-safe.
            let rel_q = match Spi::get_one_with_args::<String>(
                "SELECT $1::regclass::text",
                vec![(PgBuiltInOids::TEXTOID.oid(), tbl.into_datum())],
            ) {
                Ok(Some(s)) => s,
                _ => return false,
            };
            let col_q = quote_ident(&col);
            let query = format!("SELECT * FROM {rel_q} WHERE {col_q} = {pk}::int8");
            fetch_raw_tuple(&query)
        };
        match raw {
            Some(bytes) => view.set(&rc_key(relid.as_u32(), pk), &bytes, 0),
            None => false,
        }
    }

    /// Row-cache occupancy: entries (registrations + rows), bytes used/cap.
    #[pg_extern]
    fn rowcache_stats() -> TableIterator<
        'static,
        (
            name!(entries, i64),
            name!(hits, i64),
            name!(misses, i64),
            name!(data_used, i64),
            name!(data_cap, i64),
        ),
    > {
        let mut rows = Vec::new();
        if let Some(view) = rowcache_view() {
            let s = view.stats(0);
            rows.push((
                s.entries as i64,
                s.hits as i64,
                s.misses as i64,
                s.data_used as i64,
                s.data_cap as i64,
            ));
        }
        TableIterator::new(rows)
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
