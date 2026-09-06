# pg_keyspace — P0 spike

A proof-of-concept for the [pg_keyspace technical plan](../../docs): a
Postgres-native cache and RESP-compatible keyspace intended to replace Valkey in
the Supatype stack. This is the **P0 spike** from §12 of the plan — the
throwaway measurement whose kill criterion is *"if a hit is not under 80µs, stop
and keep Valkey."*

**It is under 80µs. It ties Valkey.** See [`results/REPORT.md`](results/REPORT.md)
for the full write-up and the concerns matrix.

> Scope so far: **P0** (latency/throughput vs Valkey), **P1** (storage,
> durability, crash recovery, off-event-loop persistence), **P2 security for
> Mode A** — validated on the real base (PG17 + `supatype_mask` + `pg_guard`):
> load-order assertion, seclabel self-check, RESP `AUTH`→role, keyspace ACL,
> forced tenant scoping (§4) — and **P6 Mode B (the transparent row cache):** a
> planner hook + `CustomScan` that substitutes a cached row *only at the scan
> leaf*, so RLS and `supatype_mask` re-apply above it (§4.6). Its security suite
> passes 10/10 (`results/p6_security.txt`): a non-owner is denied a physically
> cached foreign row and a non-exempt role gets NULL for a masked column that is
> genuinely present, unmasked, in the cache. The Custom Scan roughly halves
> executor time for a single-row pk lookup (`results/p6_rowcache.txt`), and the
> §4.3c masked-read concern is quantified (`results/p6_maskcost.txt`). The only
> remaining P6 piece is the logical-decoding invalidation worker (§3.5); the cache
> is populated here via `supacache.rowcache_put` as a stand-in. Still a POC — auth
> secrets are compared in clear and credentials load at worker start; don't deploy
> as-is.

## What it is

A real Postgres extension (built here against PG17.6 for P2/P6; P0/P1 numbers
came from a PG16 spike box), loaded via `shared_preload_libraries`, that:

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
│   │   ├── ring.rs           SPSC shmem ring: RESP worker -> persistence worker (P1)
│   │   ├── crc16.rs          cluster slot hashing (§3.1)
│   │   └── bin/
│   │       ├── pgkeyspaced.rs        standalone daemon (scale-out demo)
│   │       └── durability_bench.rs   isolated batcher amortisation benchmark
│   └── Cargo.toml
├── extension/                ← the pgrx extension (compiles poc/src verbatim via #[path])
│   ├── src/lib.rs            _PG_init, shmem hooks, RESP worker, persistence worker, supacache.* SQL
│   ├── Cargo.toml
│   └── pg_keyspace.control
├── bench/                    ← benchmark harnesses
│   ├── run_benchmarks.sh     latency + throughput vs Redis, in-backend §6, libpq
│   ├── run_durability.sh     per-tier RESP SET (§3.4)
│   ├── run_scaleout.sh       shared-nothing scaling (§3.1)
│   ├── run_p2_threats.sh     Mode A security threat table (§4.7)
│   ├── run_p6_maskcost.sh    cost of a masked read + §6 accelerator (§4.3c)
│   ├── run_p6_security.sh    Mode B row-cache RLS/mask/generic-plan suite (§4.6/§4.7)
│   └── run_p6_rowcache.sh    Mode B Custom Scan vs index-scan latency (§7.1)
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

> **Bootstrap order.** The `supacache` schema and SQL surface are owned by
> `CREATE EXTENSION`, so run it before relying on persistence or the SQL API. The
> worker never creates schema objects before the extension exists (doing so would
> make `CREATE EXTENSION` fail with *"schema supacache is not a member"*); until
> then it serves RESP in ephemeral mode. With a durable tier set, install the
> extension in `pg_keyspace.database` and restart once to enable persistence — the
> log says so if it is missing.

With `pg_keyspace.durability = 'relaxed'` (P1), RESP writes also persist to the
hash-partitioned `supacache.kv` table and survive a crash — the worker rebuilds
shmem from the table on startup:

```sql
SELECT count(*) FROM supacache.kv;                       -- rows persisted from RESP
SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='foo';  -- 'bar'
-- kill -9 the cluster, restart:  redis-cli -p 6380 get foo  ->  still "bar"
```

**Mode B — transparent row cache (P6, §7.1).** Register a table's primary-key
column and cache a row; a `pk = Const` lookup is then served from shared memory
by a `CustomScan`, transparently, with RLS and `supatype_mask` still applied:

```sql
SELECT supacache.rowcache_register('public.orders', 1);  -- pk is attnum 1
SELECT supacache.rowcache_put('public.orders', 42);       -- cache row id=42
EXPLAIN SELECT * FROM public.orders WHERE id = 42;
--  Custom Scan (pg_keyspace_rowcache) on orders  (Filter: id = 42)
SELECT * FROM supacache.rowcache_stats();
```

The scan serves the **raw** cached row at the leaf; the relation's RLS quals and
mask `CASE` expressions re-apply above it, so a role that couldn't see the row (or
a masked column) via a normal query still can't via the cache (§4.6). Populating
is manual here — a stand-in for the logical-decoding refill worker (§3.5).

Relevant GUCs (all `Postmaster` context — set in `postgresql.conf`):

| GUC | default | meaning |
|---|---|---|
| `pg_keyspace.port` | 6380 | RESP listen port |
| `pg_keyspace.keys` | 1000000 | keyspace capacity (sizes the segment) |
| `pg_keyspace.val_bytes` | 512 | avg value size (sizes the slab arena) |
| `pg_keyspace.durability` | `ephemeral` | `ephemeral` = shmem only; any other value persists to `supacache.kv` (§3.3/§3.4) |
| `pg_keyspace.database` | `postgres` | database holding the `supacache.kv` backing tables |
| `pg_keyspace.persist_window_ms` | 10 | how often the persistence worker drains the ring when idle |
| `pg_keyspace.ring_mb` | 64 | size of each RESP→persistence ring buffer (burst absorption) |
| `pg_keyspace.persist_workers` | 1 | persistence workers/rings draining in parallel (writes sharded by key slot) |
| `pg_keyspace.ttl_bucket_secs` | 10 | TTL time-bucket width; TTL'd keys persist to range-partitioned `supacache.kv_ttl` (§3.3) |
| `pg_keyspace.ttl_sweep_secs` | 5 | how often the expiry worker drops fully-past TTL partitions |
| `pg_keyspace.commit_window_us` | 500 | standalone file-batcher window (durability microbench) |
| `pg_keyspace.rowcache_mb` | 64 | size of the Mode B row-cache segment (separate from Mode A; never RESP-addressable) |
| `pg_keyspace.require_mask` | `on` | require `supatype_mask` loaded + outermost before serving (§4.1); `off` runs standalone with no mask dependency |

## Results in one line

Latency **34µs** (ties Valkey, kill criterion was `<80µs`); single-worker
throughput **500–556k/s**; 4-worker **2.46M/s** (Valkey-class); in-backend read
**9ns** (§6, 100× better than projected); durable writes **145k/s** across 4
persist workers; **P2 security 9/9** on the real PG17 base. Full report and the
concerns/threat matrix: [`results/REPORT.md`](results/REPORT.md).
