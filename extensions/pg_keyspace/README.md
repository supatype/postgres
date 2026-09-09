# pg_keyspace

**A Redis/Valkey-compatible cache that lives *inside* PostgreSQL.**

`pg_keyspace` is a Postgres extension that runs a RESP2/RESP3 (Redis wire
protocol) server in a background worker over a shared-memory keyspace, plus a transparent
row cache for PostgREST. Stock clients — `redis-cli`, `ioredis`, `redis-py`,
`redis-benchmark` — talk to it unmodified on `:6381`, while the *same bytes* are
readable and writable from SQL. One system, one thing to run, one security model.

A cache read lands in **~34 µs — tying Valkey** — and it is a complete
extension: a broad RESP command surface ([coverage](#command-coverage)) across
strings, hashes, lists, sets, sorted sets, pub/sub and transactions; four
durability tiers with real crash recovery and synchronous replication; native
large-collection structures; a transparent PostgREST row cache; horizontal
multi-worker scale-out; and an optional Postgres-native security layer.

**pg_keyspace runs standalone.** It has **no dependency on any other extension** —
not `supatype_mask`, not `pg_guard`. Load it on its own and it is a
Postgres-native RESP keyspace + RLS-aware row cache. The Supatype platform can
*optionally* layer column masking on top (`pg_keyspace.require_mask`, see
[Security](#resp-auth-tenant-scoping--tls)); that integration is opt-in, not a
prerequisite.

> **Status:** built and benchmarked against PostgreSQL 17.6. A working extension;
> some capabilities (persistence, the row cache) are single-worker in this
> version — see [Current limitations](#current-limitations).

---

## Why pg_keyspace?

If you put PostgREST in front of Postgres, you typically also run Valkey/Redis
as a *second* stateful service: another thing to deploy, secure, monitor, back
up, and keep in sync with the database. `pg_keyspace` collapses that into
Postgres itself.

- **One service, not two.** The cache is a background worker inside your existing
  Postgres. No separate cluster to provision, patch, or page someone about.
- **The same data, two ways.** A key written over RESP is readable from SQL
  (`supacache.get`) and vice-versa — the RESP worker and the SQL functions map
  the *same* shared-memory segment. In-backend reads are **~9 ns** (no socket, no
  copy), which is the path a mask predicate or a stored function can use directly.
- **Security you already trust.** RESP `AUTH` maps to a Postgres role; keys are
  ACL-checked and **force-scoped per tenant** (`{tenant}:{key}`); pub/sub channels
  are tenant-scoped too. The wire is TLS (native rustls). All of this is built in
  and needs no other extension. When `supatype_mask` *is* present, pg_keyspace
  additionally honours its column masking and `exempt_roles` — an optional layer,
  never a requirement.
- **A transparent cache for PostgREST (Mode B).** Register a table's primary key
  and a `WHERE pk = $1` lookup is served from shared memory by a planner
  `CustomScan` — **no application changes**. RLS still applies *above* the cached
  row (and column masking too, when `supatype_mask` is loaded), and a
  **keys-only** logical-decoding worker keeps it
  coherent automatically. No cache-invalidation glue to write and get wrong.
- **Durability is a dial, per deployment.** `ephemeral` (Valkey-parity, shmem
  only) → `relaxed` (async persist) → `durable` (fsync before ack) → `replicated`
  (**real** synchronous streaming to a standby). Writes survive `kill -9` and are
  rebuilt from Postgres tables on restart.
- **Real Redis data types**, with native large-collection structures: strings,
  counters, TTLs, hashes, lists, sorted sets, and pub/sub — and past a threshold,
  hashes/lists/zsets switch to indexed encodings so `HGET`/`LINDEX`/`ZSCORE`/
  `ZRANK` stay O(1)/O(log n) at 10 k+ elements instead of O(n).
- **Horizontal scale-out inside Postgres.** `pg_keyspace.workers = N` runs N
  shared-nothing slot workers; aggregate throughput scales roughly linearly.

---

## pg_keyspace vs Valkey

Honest, feature-by-feature. Valkey is a mature, battle-tested, best-in-class
in-memory store; `pg_keyspace` is an early extension that trades a little raw
ceiling for deep Postgres integration.

| | **pg_keyspace** | **Valkey / Redis** |
|---|---|---|
| Deployment | A background worker inside Postgres — no extra service | Separate service/cluster to run & operate |
| Cache-hit latency (`GET`, closed loop) | **~34–39 µs** (ties Valkey) | ~35–39 µs |
| Pipelined throughput (1 worker) | ~500–556 k ops/s | ~537–628 k ops/s |
| Horizontal scale-out | N shared-nothing workers (**2.46 M SET/s** at 4) | Cluster mode / multiple shards |
| SQL access to the same data | **Yes** — `supacache.*`, ~9 ns in-backend | No (separate datastore) |
| Transparent PostgREST/row cache | **Yes** — planner `CustomScan`, auto-coherent | No (app-managed) |
| Auth / ACL / multi-tenant isolation | Postgres roles, keyspace ACL, forced tenant scoping | Redis ACLs (separate user store) |
| Column masking / RLS on cached rows | **Yes** — re-applied above the cache | N/A |
| Durability | Tiered: ephemeral→relaxed→durable(fsync)→replicated; crash-recovers from PG tables | RDB / AOF snapshots & log |
| Synchronous replication | **Yes** — ack held until standby fsync | Async by default (WAIT for quorum) |
| Data types | strings, hashes, lists, sets, sorted sets, pub/sub (+ TTL), transactions | Superset (adds streams, HLL, bitmaps, geo, scripting) |
| Raw write ceiling under no-persistence load | Lower (bounded by 1 event loop / worker) | **Higher** — purpose-built |
| Maturity / ecosystem / ops tooling | New, focused feature set | **Mature**, huge ecosystem |

**Use `pg_keyspace` when** the cache and the database should be one system: you
want SQL and RESP over the same data, a transparent row cache for PostgREST,
per-key durability, and Postgres-native security — without operating a second
stateful service. **Stay on Valkey when** you need maximum single-node write
throughput, the full command/type surface, or mature cluster operations today.

---

## Command coverage

A stock client (`redis-cli`, `ioredis`, `redis-py`, `valkey-go`) drives
`pg_keyspace` unmodified, in **RESP2 or RESP3**: `HELLO 3` negotiates RESP3
(typed map/set/double/null replies and push-framed pub/sub), and `CLIENT
TRACKING` enables server-assisted client-side caching (`invalidate` pushes),
so `valkey-go`/`rueidis` can run with client-side caching on. Coverage below;
anything not listed replies `ERR unknown command`.

**Supported**

| Group | Commands |
|---|---|
| Connection / server | `PING` `ECHO` `AUTH` `HELLO` (2/3) `QUIT` `SELECT` `RESET` `CLIENT` (incl. `TRACKING`) `CONFIG` `COMMAND` `INFO` `TIME` `DBSIZE` `DEBUG` `MEMORY` |
| Keys / generic | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `SCAN` `TTL` `PTTL` `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `EXPIRETIME` `PEXPIRETIME` `PERSIST` `RENAME` `RENAMENX` `COPY` `TOUCH` `RANDOMKEY` `OBJECT` `FLUSHDB` `FLUSHALL` |
| Strings | `GET` `SET` `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `GETEX` `APPEND` `STRLEN` `GETRANGE` `SETRANGE` `MGET` `MSET` `MSETNX` `INCR` `DECR` `INCRBY` `DECRBY` `INCRBYFLOAT` |
| Hashes | `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HSTRLEN` `HINCRBY` `HINCRBYFLOAT` `HRANDFIELD` `HSCAN` |
| Lists | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LLEN` `LINDEX` `LRANGE` `LSET` `LTRIM` `LINSERT` `LREM` `LPOS` `LMOVE` `RPOPLPUSH` |
| Sets | `SADD` `SREM` `SCARD` `SISMEMBER` `SMISMEMBER` `SMEMBERS` `SPOP` `SRANDMEMBER` `SMOVE` `SSCAN` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE` `SINTERCARD` |
| Sorted sets | `ZADD` `ZREM` `ZSCORE` `ZMSCORE` `ZCARD` `ZINCRBY` `ZRANK` `ZREVRANK` `ZCOUNT` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGEBYLEX` `ZLEXCOUNT` `ZRANGESTORE` `ZPOPMIN` `ZPOPMAX` `ZRANDMEMBER` `ZMPOP` `ZSCAN` `ZUNION` `ZINTER` `ZDIFF` `ZUNIONSTORE` `ZINTERSTORE` `ZDIFFSTORE` |
| Pub/sub | `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` |
| Transactions | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |

**Not yet supported** — scripting (`EVAL`/`FUNCTION`), streams (`XADD`…),
blocking ops (`BLPOP`/`BRPOP`/`BZPOPMIN`…), HyperLogLog / bitmaps / geo, and
cluster commands. `CLIENT TRACKING` runs in default (per-read) mode over RESP3;
BCAST/OPTIN/REDIRECT modes and cross-worker invalidation on the in-PG shared
store are follow-ups (correct today on the daemon, where each worker owns an
independent store).

**Known divergences:** queuing a malformed command inside `MULTI` does not
pre-flag `EXECABORT` (it errors as that command's element in the `EXEC` array;
atomic apply and `WATCH`-abort are exact); `HSCAN`/`SSCAN`/`ZSCAN` return the
whole collection in one call with cursor `0` (each aggregate is one blob), so
`COUNT` is a hint; sorted-set scores print via Rust's shortest round-trip rather
than `%.17g` (equal values, possibly different text).

---

## Benchmarks

Measured on a 4-vCPU Linux box, 512-byte values, against Redis 7.0.15 as the
reference. Reproduce any of these with the scripts in [`bench/`](bench/) (they
print their own pass/fail and numbers).

### Latency & throughput vs Redis (`bench/run_benchmarks.sh`)

| Operation | pg_keyspace | Redis 7.0.15 |
|---|---:|---:|
| `SET` closed-loop p50 | **39 µs** | 39 µs |
| `GET` closed-loop p50 | **39 µs** | 39 µs |
| `SET` pipelined (`-c50 -P16`) | **556 k/s** | 537 k/s |
| `GET` pipelined (`-c50 -P16`) | 500 k/s | 628 k/s |
| In-backend SQL read (in-process) | **8.9 ns** | — (n/a) |
| SQL surface via libpq (`SELECT supacache.get`) | 0.048 ms, 21 k tps | — |

### Horizontal scale-out (`bench/run_scaleout.sh`, aggregate rps)

| Workers | SET agg | GET agg |
|---:|---:|---:|
| 1 | 568 k/s | 604 k/s |
| 2 | 1.10 M/s | 1.13 M/s |
| 4 | **2.46 M/s** | 2.11 M/s |

In-PG (`pg_keyspace.workers=4`, over TLS): single worker 105 k SET/s → 4-worker
aggregate **609 k SET/s**, shared-nothing (a key on one worker is invisible on
the others). — `bench/run_scaleout_inpg.sh`

### Native large collections — indexed vs flat encoding (ns/op)

Past a threshold, collections switch to an indexed in-value structure. Point
reads go from O(n) to O(1)/O(log n) — dramatic at scale, byte-for-byte
Redis-compatible (`bench/run_big{hash,list,zset}.sh`, `core/examples/bench_*`):

| Op | Elements | Flat (inline) | Indexed | Speedup |
|---|---:|---:|---:|---:|
| `HGET` | 50 k | 97 µs | **14.9 ns** | 6,528× |
| `LINDEX` | 50 k | 50 µs | **1.8 ns** | 27,468× |
| `ZSCORE` | 10 k | 271 µs | **11.3 ns** | 23,972× |
| `ZRANK` | 10 k | 1,265 µs | **66.9 ns** | 18,905× |

### Durability tiers (`bench/run_durability.sh`)

| Tier | closed-loop p50 | ack means |
|---|---:|---|
| ephemeral | 39 µs | in shmem (Valkey-parity) |
| relaxed | 39 µs | queued, persisted async |
| durable | 1.04 ms | committed to `supacache.kv` (fsync) |
| replicated | ~1.67 ms | **fsync + standby ack** (real sync rep) |

- Durable writes are **off the event loop** (a shared-memory ring drained by
  dedicated persist workers): ~107 k/s sustained per worker, scaling to **145 k/s**
  across 4 (`bench/run_persist_scaleout.sh`), while reads stay unaffected (write
  flood tail cut from 170 ms → 5 ms).
- **Crash recovery:** after `kill -9`, keys rebuild from `supacache.kv` at
  ~3.5 µs/key; every acked durable write survives.
- **TTL expiry** is an O(1) partition `DROP` (3.2 ms) vs an O(n) `DELETE`
  (141 ms for 100 k rows) — no vacuum churn.

### Mode B row cache (masked-read cost, `bench/run_maskcost.sh`)

A row-independent mask predicate plans as an `InitPlan` (evaluated **once** per
scan): **18.6 ms** for 100 k masked rows vs **1,127 ms** for a naive per-row
predicate — collapsing a 96× overhead to ~1.5× over the unmasked baseline.

---

## Getting it

Three ways, easiest first:

1. **The `supatype/postgres` image** — pg_keyspace is compiled from source into
   the image (amd64 and arm64) and shipped ready to `CREATE EXTENSION`. It is
   bundled but not auto-loaded; enable it by adding `pg_keyspace` to
   `shared_preload_libraries` and restarting (see below).
2. **Prebuilt packages** — every `v*` release attaches
   `pg_keyspace-v<ver>-pg17-<arch>-linux-gnu.deb` and a matching `.tar.gz` for a
   stock PGDG PostgreSQL 17. `dpkg -i pg_keyspace-*.deb` (or untar over the PG
   prefix), then continue at the config step below.
3. **Build from source** — the developer path, shown next.

## Install & use

Building from source requires only PostgreSQL (built/tested against 17.6) and
`cargo-pgrx` 0.12.9. `supatype_mask` is **optional** — load it to add column
masking; skip it to run standalone (`pg_keyspace.require_mask = off`). `pg_guard`
is not required at all.

```bash
# 1. core unit tests (no Postgres needed)
cd extensions/pg_keyspace/core && cargo test

# 2. build + install the extension
cd ../extension
cargo pgrx install --release --pg-config /path/to/pg_config

# 3. (Mode B invalidation only) build the keys-only decode plugin
cd ../plugin && make install PG_CONFIG=/path/to/pg_config
```

Add to `postgresql.conf` and restart. **Standalone** (no other extension needed):

```ini
shared_preload_libraries = 'pg_keyspace'
pg_keyspace.port = 6381
pg_keyspace.require_mask = off        # run standalone (no supatype_mask)
```

To *optionally* add Supatype column masking, load `supatype_mask` after
`pg_keyspace` and leave `require_mask` on:

```ini
shared_preload_libraries = 'pg_keyspace, supatype_mask'  # pg_keyspace BEFORE mask
```

```sql
CREATE EXTENSION pg_keyspace;
```

Now stock RESP clients and SQL share one keyspace:

```bash
redis-cli -p 6381 SET foo bar
redis-cli -p 6381 GET foo          # "bar"
```
```sql
SELECT convert_from(supacache.get('foo'), 'UTF8');   -- 'bar'  (same segment)
SELECT * FROM supacache.stats();
```

Redis data types work as expected (`HSET/HGET`, `LPUSH/LRANGE`, `SADD/SUNIONSTORE`,
`ZADD/ZRANGE`, `SUBSCRIBE/PUBLISH`, `MULTI/EXEC`, …), verified for Redis parity
at 10 k-element scale — see the full [command coverage](#command-coverage). Key
lifetime and iteration are covered too: `SET … EX/PX`, `TTL`/`PTTL`,
`EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT`, `PERSIST`, and `SCAN`/`KEYS`.

**RESP2 and RESP3.** A client using RESP2 works unchanged. `HELLO 3` switches a
connection to RESP3 — typed replies (map/set/double/null) and push-framed
pub/sub — and `CLIENT TRACKING ON` turns on server-assisted client-side
caching: keys the connection reads are tracked, and an `invalidate` push is
sent when one changes (a null push on `FLUSHALL`/`FLUSHDB`). So `valkey-go`
/`rueidis` can run with client-side caching enabled rather than
`DisableCache`. Tracking is default (per-read) mode; see the caveats under
[command coverage](#command-coverage).

### Durability & crash recovery

```ini
pg_keyspace.durability = 'durable'     # ephemeral | relaxed | durable | replicated
```
```sql
SELECT count(*) FROM supacache.kv;     -- RESP writes persisted here
-- kill -9 the cluster, restart:  redis-cli -p 6381 GET foo  ->  still "bar"
```

The `replicated` tier means *durable + a synchronous standby ack*. Real
socket-streamed synchronous replication (the ack is held until the standby has
fsynced the record) is implemented in the standalone daemon with a companion
`pgks-replica` standby:

```bash
# standby:
pgks-replica --host 0.0.0.0 --port 7400 --workers 1 --wal-dir /var/lib/pgks-standby
# primary:
pgkeyspaced --tier replicated --replica-addr standby-host --replica-port 7400
```

Inside the extension, `pg_keyspace.durability = 'replicated'` runs the durable
persist path with `synchronous_commit = remote_apply`, so it relies on Postgres's
own streaming replication for the standby.

### Multi-worker scale-out

```ini
pg_keyspace.workers = 4     # N shared-nothing workers on port, port+1, … port+N-1
```
Clients shard keys across the ports (Redis-Cluster style). Persistence/row-cache
stay single-worker in this slice, so `workers > 1` runs the ephemeral tier.

### Mode B — transparent PostgREST row cache

```sql
SELECT supacache.rowcache_register('public.orders', 1);  -- pk = attnum 1 (int/uuid/text)
SELECT supacache.rowcache_put('public.orders', 42);       -- warm one row
EXPLAIN SELECT * FROM public.orders WHERE id = 42;
--  Custom Scan (pg_keyspace_rowcache) on orders
```

The scan serves the **raw** cached row at the leaf; the relation's RLS quals and
mask `CASE` expressions re-apply above it, so a role that couldn't see the row (or
a masked column) via a normal query still can't via the cache. Any single-column
primary-key type works (int, `uuid`, `text`), and rows with out-of-line (TOASTed)
values are flattened inline so the cached copy is self-contained. Enable
automatic coherence:

```ini
wal_level = logical
pg_keyspace.rowcache_decode = on     # keys-only decode worker drops changed keys
pg_keyspace.rowcache_refill = on     # (optional) re-cache a changed hot key instead of dropping
```

### RESP AUTH, tenant scoping & TLS

```sql
SELECT supacache.set_credential('alice', 's3cret', 'tenant_a_role', 'tenant_a');
SELECT pg_reload_conf();   -- worker hot-reloads creds + ACL on SIGHUP, no restart
```
```bash
redis-cli --tls --user alice -a s3cret -p 6381 GET session:1   # NOAUTH without it
```

Secrets are stored as salted SHA-256 and verified in constant time. Keys and
pub/sub channels are force-scoped to `{tenant}:` for non-exempt roles, so one
tenant cannot address or subscribe to another's. Point `tls_cert_file` /
`tls_key_file` at a PEM cert+key to serve TLS (rotate by swapping the files and
`SELECT pg_reload_conf()` — no restart).

### Configuration (GUCs)

All are `Postmaster` context (set in `postgresql.conf`).

| GUC | default | meaning |
|---|---|---|
| `pg_keyspace.port` | 6380 | RESP listen port (worker *w* uses `port + w`); examples here set 6381 |
| `pg_keyspace.workers` | 1 | shared-nothing RESP slot workers; >1 forces ephemeral |
| `pg_keyspace.keys` | 1000000 | keyspace capacity per worker (sizes the segment) |
| `pg_keyspace.val_bytes` | 512 | avg value size (sizes the slab arena) |
| `pg_keyspace.durability` | `ephemeral` | `ephemeral` \| `relaxed` \| `durable` \| `replicated` |
| `pg_keyspace.database` | `postgres` | database holding `supacache.kv` backing tables |
| `pg_keyspace.persist_workers` | 1 | persist workers/rings draining in parallel |
| `pg_keyspace.ring_mb` | 64 | per-worker RESP→persist ring size (burst absorption) |
| `pg_keyspace.ttl_bucket_secs` | 10 | TTL time-bucket width (range-partitioned `supacache.kv_ttl`) |
| `pg_keyspace.require_mask` | `off` | `off` (default) runs standalone; `on` fails closed unless `supatype_mask` is loaded + outermost — set by the Supatype platform |
| `pg_keyspace.tls_cert_file` / `tls_key_file` | *(empty)* | PEM cert + key → serve RESP over TLS |
| `pg_keyspace.rowcache_mb` | 64 | Mode B row-cache segment size (never RESP-addressable) |
| `pg_keyspace.rowcache_decode` | `off` | keys-only Mode B invalidation worker (needs `wal_level=logical`) |
| `pg_keyspace.rowcache_refill` | `off` | on: re-cache a changed hot key; off: drop-only (lazy) |

---

## Layout

```
extensions/pg_keyspace/
├── core/                     shared core (Rust, libc only) + tools
│   └── src/
│       ├── store.rs          open-addressed hash, size-classed slab, CLOCK eviction
│       ├── server.rs         epoll RESP2/RESP3 event loop + command dispatch
│       ├── resp.rs           RESP2/RESP3 codec
│       ├── aggr.rs           hashes/lists/sorted sets, incl. indexed large-collection encodings
│       ├── pubsub.rs         cross-worker pub/sub bus
│       ├── batcher.rs        commit batching + the four durability tiers
│       ├── repl.rs           real synchronous replication to a standby
│       ├── ring.rs           SPSC shmem ring: RESP worker → persistence worker
│       ├── crc16.rs          cluster slot hashing
│       └── bin/
│           ├── pgkeyspaced.rs      standalone daemon (scale-out demo)
│           ├── pgks-replica.rs     standalone replication standby
│           └── durability_bench.rs commit-batcher microbenchmark
├── extension/                the pgrx extension (compiles core/src verbatim via #[path])
│   └── src/lib.rs            _PG_init, shmem hooks, N RESP workers, persist/expiry/invalidation
│                             workers, Mode B CustomScan, supacache.* SQL surface
├── plugin/                   supacache_keys: keys-only logical-decoding output plugin
└── bench/                    reproducible benchmark + conformance harnesses (run_*.sh)
```

The extension shares the `core/src/*.rs` modules **verbatim** (via `#[path]`), so
the code measured standalone is the same code that runs inside Postgres.

### Tests & benches

`cargo test` in `core/` runs the self-contained unit tests (data structures, slab
allocator, auth, replication). The `bench/` scripts are integration + conformance
harnesses, each named for what it checks: Redis parity for every type
(`run_hashes.sh`, `run_lists.sh`, `run_zsets.sh`, `run_pubsub.sh`), security
(`run_hardening.sh`, `run_tls.sh`, `run_threats.sh`, `run_security.sh`), Mode B
row-cache coherence for int/uuid/text/TOAST PKs (`run_rowcache.sh`,
`run_nonint_pk.sh`, `run_toast.sh`, `run_invalidation.sh`), tenant-scoped pub/sub
(`run_pubsub_tenant.sh`), real synchronous replication (`run_replication.sh`), a
real PostgREST v12.2.3 end-to-end (`run_postgrest_e2e.sh`), and scale-out
(`run_scaleout.sh`, `run_scaleout_inpg.sh`). Each prints its own `# result: N
passed, M failed`.

---

## Current limitations

Scoping for this version — the extension works; these are the edges to know:

- **Persistence and the Mode B row cache are single-worker.** `pg_keyspace.workers
  > 1` runs the ephemeral (Mode A) tier only.
- **Pub/sub is cross-worker within one process** (the scale-out daemon), not yet
  cross-*process* for N in-PG background workers.
- **Mode B caches single-column primary keys** (composite keys are refused); the
  cache is warmed manually (`rowcache_put`) though invalidation is automatic.
- Sorted-set score formatting uses Rust's shortest round-trip rather than Redis's
  `%.17g`, so inexact doubles can print differently (values compare equal).
- TLS is bring-your-own-cert (in-place rotation on `SIGHUP`; no managed CA). The
  durable/replicated tiers are correct but not throughput-optimised — they
  serialize on the Postgres WAL by design.
