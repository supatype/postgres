//! pgkeyspaced — the standalone daemon. Spawns N slot workers, each a
//! shared-nothing epoll loop on its own port backed by its own shared-memory
//! segment (§3.1: "no locks, no atomics, no cache line ping-pong on the hot
//! path"). N=1 is the single-worker configuration the latency benchmarks
//! measure; N>1 demonstrates scale-out.
//!
//! Usage:
//!   pgkeyspaced [--workers N] [--port 6380] [--host 127.0.0.1]
//!               [--keys-per-worker 1000000] [--val-bytes 512] [--shmem-mb 256]
//!               [--tier ephemeral|relaxed|durable|replicated]
//!               [--commit-window-us 500] [--replica-rtt-us 200]
//!               [--wal-dir /tmp]

use pgks::batcher::{Batcher, Tier};
use pgks::pubsub::Bus;
use pgks::repl::Replica;
use pgks::server::Worker;
use pgks::store::{Config, Store};
use std::sync::Arc;
use std::time::Duration;

struct Args {
    workers: u32,
    port: u16,
    host: String,
    keys_per_worker: u32,
    val_bytes: u64,
    shmem_mb: u64,
    tier: Tier,
    commit_window_us: u64,
    replica_rtt_us: u64,
    wal_dir: String,
    replica_addr: Option<String>, // external standby host for the replicated tier
    replica_port: u16,            // its base port (worker w -> replica_port+w)
}

impl Default for Args {
    fn default() -> Self {
        Args {
            workers: 1,
            port: 6380,
            host: "127.0.0.1".into(),
            keys_per_worker: 1_000_000,
            val_bytes: 512,
            shmem_mb: 0, // 0 => derive from capacity
            tier: Tier::Ephemeral,
            commit_window_us: 500,
            replica_rtt_us: 200,
            wal_dir: "/tmp".into(),
            replica_addr: None,
            replica_port: 7400,
        }
    }
}

fn parse_args() -> Args {
    let mut a = Args::default();
    let argv: Vec<String> = std::env::args().collect();
    let val = |i: usize| -> String { argv.get(i + 1).cloned().unwrap_or_default() };
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--workers" => a.workers = val(i).parse().unwrap_or(1),
            "--port" => a.port = val(i).parse().unwrap_or(6380),
            "--host" => a.host = val(i),
            "--keys-per-worker" => a.keys_per_worker = val(i).parse().unwrap_or(1_000_000),
            "--val-bytes" => a.val_bytes = val(i).parse().unwrap_or(512),
            "--shmem-mb" => a.shmem_mb = val(i).parse().unwrap_or(0),
            "--commit-window-us" => a.commit_window_us = val(i).parse().unwrap_or(500),
            "--replica-rtt-us" => a.replica_rtt_us = val(i).parse().unwrap_or(200),
            "--replica-addr" => a.replica_addr = Some(val(i)),
            "--replica-port" => a.replica_port = val(i).parse().unwrap_or(7400),
            "--wal-dir" => a.wal_dir = val(i),
            "--tier" => {
                a.tier = match val(i).as_str() {
                    "relaxed" => Tier::Relaxed,
                    "durable" => Tier::Durable,
                    "replicated" => Tier::Replicated,
                    _ => Tier::Ephemeral,
                }
            }
            other => {
                if other.starts_with("--") {
                    eprintln!("warning: ignoring unknown arg {other}");
                }
            }
        }
        i += 1;
    }
    a
}

fn main() {
    let a = parse_args();

    let cfg = if a.shmem_mb > 0 {
        // fixed-size segment: derive bucket/entry counts from the byte budget
        let bytes = a.shmem_mb * 1024 * 1024;
        let entries = a.keys_per_worker;
        let buckets = (entries * 2).next_power_of_two().max(1024);
        let overhead = std::mem::size_of::<u32>() as u64 * buckets as u64 + 256 * entries as u64;
        let data = bytes.saturating_sub(overhead).max(1 << 20);
        Config {
            num_partitions: 1,
            buckets_per_part: buckets,
            entries_per_part: entries,
            data_bytes_per_part: data,
        }
    } else {
        Config::for_capacity(1, a.keys_per_worker, a.val_bytes)
    };

    let seg_mb = cfg.total_bytes() as f64 / (1024.0 * 1024.0);
    println!(
        "pgkeyspaced: workers={} port={}..{} tier={:?} keys/worker={} val={}B seg/worker={:.0}MB commit_window={}us",
        a.workers,
        a.port,
        a.port + a.workers as u16 - 1,
        a.tier,
        a.keys_per_worker,
        a.val_bytes,
        seg_mb,
        a.commit_window_us,
    );

    // Cross-worker pub/sub: workers are threads of this process, so they share
    // one Bus. A PUBLISH on any worker reaches subscribers on all workers (§5).
    let bus = Arc::new(Bus::new(a.workers as usize));

    let mut handles = Vec::new();
    for w in 0..a.workers {
        let seg_name = format!("pgks_w{w}_{}", std::process::id());
        let store = Arc::new(
            Store::create(&seg_name, &cfg).unwrap_or_else(|e| {
                eprintln!("worker {w}: shmem create failed: {e}");
                std::process::exit(1);
            }),
        );
        let batcher = if a.tier != Tier::Ephemeral {
            let wal = format!("{}/pgks_w{w}_{}.wal", a.wal_dir, std::process::id());
            let window = Duration::from_micros(a.commit_window_us);
            // The replicated tier needs a real standby: an external one at
            // --replica-addr (base+w, one per worker), else a co-located loopback
            // standby (--replica-rtt-us models the link latency).
            let b = if a.tier == Tier::Replicated {
                let replica = if let Some(addr) = &a.replica_addr {
                    Replica::External(format!("{addr}:{}", a.replica_port + w as u16))
                } else {
                    Replica::Loopback {
                        wal_path: format!("{}/pgks_w{w}_{}.replica.wal", a.wal_dir, std::process::id()),
                        link_delay: Duration::from_micros(a.replica_rtt_us),
                    }
                };
                Batcher::with_replica(&wal, window, replica).expect("batcher+replica")
            } else {
                Batcher::new(&wal, window).expect("batcher")
            };
            Some(Arc::new(b))
        } else {
            None
        };
        let host = a.host.clone();
        let port = a.port + w as u16;
        let tier = a.tier;
        let bus = bus.clone();
        let h = std::thread::Builder::new()
            .name(format!("slot-worker-{w}"))
            .spawn(move || {
                let mut worker = Worker::new(store, batcher, tier, &host, port)
                    .unwrap_or_else(|e| {
                        eprintln!("worker {w}: listen {host}:{port} failed: {e}");
                        std::process::exit(1);
                    });
                worker.set_bus(bus, w as usize);
                println!("worker {w} listening on {host}:{port}");
                if let Err(e) = worker.run() {
                    eprintln!("worker {w} exited: {e}");
                }
            })
            .expect("spawn");
        handles.push(h);
    }

    for h in handles {
        let _ = h.join();
    }
}
