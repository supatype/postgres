# pg_keyspace — P0 spike

A proof-of-concept for the [pg_keyspace technical plan](../../docs): a
Postgres-native cache and RESP-compatible keyspace intended to replace Valkey in
the Supatype stack. This is the **P0 spike** from §12 of the plan — the
throwaway measurement whose kill criterion is *"if a hit is not under 80µs, stop
and keep Valkey."*

**It is under 80µs. It ties Valkey.** See [`results/REPORT.md`](results/REPORT.md)
for the full write-up and the concerns matrix.

> This is a performance/architecture spike. It implements **none** of the
> security model (§4) — no AUTH, no tenant isolation, no `supatype_mask`
> integration. That is P2, and per §14 it is the phase that decides whether the
> project ships. Do not deploy this.

## What it is

A real Postgres 16 extension, loaded via `shared_preload_libraries`, that:

- allocates a **Postgres shared-memory segment** (`shmem_request_hook` +
  `shmem_startup_hook`) and lays an open-addressed hash table + size-classed
  slab allocator + CLOCK eviction over it (§3.2);
- runs an **`epoll` RESP event loop in a background worker** (a real Postgres
  backend) on TCP `:6380`, so stock `redis-cli`/`ioredis`/`redis-benchmark`
  drive it unmodified (§3.1, §5);
- exposes a **`supacache.*` SQL surface** that reads the same segment directly
  in the calling backend — the ~nanosecond in-process path the plan's
  mask-predicate accelerator depends on (§6);
- models the **four durability tiers** with a commit batcher (§3.4).

## Layout

```
extensions/pg_keyspace/
├── README.md                 ← you are here
├── poc/                      ← the shared core (Rust, libc only) + tools
│   ├── src/
│   │   ├── store.rs          open-addressed hash, slab allocator, CLOCK eviction (§3.2)
│   │   ├── shmem.rs          POSIX shmem backing (standalone); PG shmem is used in-extension
│   │   ├── resp.rs           RESP2 codec (§5)
│   │   ├── server.rs         epoll event loop, RESP dispatch (§3.1)
│   │   ├── batcher.rs        commit batching, four durability tiers (§3.4)
│   │   ├── crc16.rs          cluster slot hashing (§3.1)
│   │   └── bin/
│   │       ├── pgkeyspaced.rs        standalone daemon (scale-out demo)
│   │       └── durability_bench.rs   isolated batcher amortisation benchmark
│   └── Cargo.toml
├── extension/                ← the pgrx extension (compiles poc/src verbatim via #[path])
│   ├── src/lib.rs            _PG_init, shmem hooks, background worker, supacache.* SQL
│   ├── Cargo.toml
│   └── pg_keyspace.control
├── bench/                    ← benchmark harnesses
│   ├── run_benchmarks.sh     latency + throughput vs Redis, in-backend §6, libpq
│   ├── run_durability.sh     per-tier RESP SET (§3.4)
│   └── run_scaleout.sh       shared-nothing scaling (§3.1)
└── results/                  ← REPORT.md + raw benchmark outputs
```

The extension shares the `poc/src/*.rs` modules **verbatim** (via `#[path]`), so
the code measured standalone is the same code that runs inside Postgres.

## Build & test

```bash
# 1. core unit tests (no Postgres needed)
cd extensions/pg_keyspace/poc
cargo test

# 2. build + install the extension into a system PostgreSQL 16
#    (needs postgresql-server-dev-16 and cargo-pgrx 0.12.9 init'd against pg16)
cd ../extension
cargo pgrx install --release --pg-config /usr/bin/pg_config
```

Then in a cluster with `shared_preload_libraries = 'pg_keyspace'`:

```sql
CREATE EXTENSION pg_keyspace;
SELECT supacache.set('k', 'v'::bytea);
SELECT convert_from(supacache.get('k'), 'UTF8');   -- 'v'
SELECT * FROM supacache.stats();
```

```bash
redis-cli -p 6380 ping           # PONG  (RESP served by the background worker)
redis-cli -p 6380 set foo bar
redis-cli -p 6380 get foo        # "bar" (same shared-memory segment as SQL)
```

Relevant GUCs (all `Postmaster` context — set in `postgresql.conf`):

| GUC | default | meaning |
|---|---|---|
| `pg_keyspace.port` | 6380 | RESP listen port |
| `pg_keyspace.keys` | 1000000 | keyspace capacity (sizes the segment) |
| `pg_keyspace.val_bytes` | 512 | avg value size (sizes the slab arena) |
| `pg_keyspace.durability` | `ephemeral` | `ephemeral`\|`relaxed`\|`durable`\|`replicated` (§3.4) |
| `pg_keyspace.commit_window_us` | 500 | batch window for logged tiers |

## Results in one line

Latency **34µs** (ties Valkey, kill criterion was `<80µs`); single-worker
throughput **500–556k/s**; 4-worker **2.46M/s** (Valkey-class); in-backend read
**9ns** (§6, 100× better than projected). Full report and the
concerns/threat matrix: [`results/REPORT.md`](results/REPORT.md).
