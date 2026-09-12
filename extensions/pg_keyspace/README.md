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

> **Status:** built and benchmarked against PostgreSQL 17.6. A working extension
> — see [Current limitations](#current-limitations) for the edges to know.

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
| Transparent PostgREST/row cache | **Yes** — planner `CustomScan`; coherent within a bounded window ([details](#what-the-row-cache-guarantees-and-what-it-does-not)) | No (app-managed) |
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
(typed map/set/double/null replies, `WITHSCORES`/`WITHVALUES` member–value
pairs, and push-framed pub/sub), and `CLIENT TRACKING` enables server-assisted
client-side caching (`invalidate` pushes) in every mode — default, `BCAST`
(with `PREFIX`), `OPTIN`/`OPTOUT` (with `CLIENT CACHING`), and `REDIRECT` — so
`valkey-go`/`rueidis` can run with client-side caching on rather than
`DisableCache`. Coverage below; anything not listed replies
`ERR unknown command`.

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
cluster commands. `CLIENT TRACKING` supports every mode (default, `BCAST` with
`PREFIX`, `OPTIN`/`OPTOUT` with `CLIENT CACHING`, `REDIRECT`). Invalidations are
delivered as RESP3 pushes, or — for a RESP2 `REDIRECT` target — as
`__redis__:invalidate` pub/sub messages. `NOLOOP` is accepted and is always in
effect: the connection that issued a write is never sent an invalidation for its
own change. When tracking is active, invalidations also cross workers over the
pub/sub Bus; since each worker owns an independent keyspace segment this is
conservative (a same-named key on another worker may be told to re-fetch — it
never serves stale data), and becomes exact once a worker set shares one store.

**Known divergences:** queuing a malformed command inside `MULTI` does not
pre-flag `EXECABORT` (it errors as that command's element in the `EXEC` array;
atomic apply and `WATCH`-abort are exact); `HSCAN`/`SSCAN`/`ZSCAN` return the
whole collection in one call with cursor `0` (each aggregate is one blob), so
`COUNT` is a hint; sorted-set scores print exactly as Valkey 8 prints them,
verified against it.

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
  ~3.5 µs/key; every acked durable write survives. Measured against key count by
  `bench/run_recovery_bench.sh` — the per-key time holds to 1M, but peak memory
  is the constraint that decides how large a keyspace can be restarted, so see
  [the table](#recovery-cost-against-key-count) before sizing one.
- **TTL expiry** is an O(1) partition `DROP` (3.2 ms) vs an O(n) `DELETE`
  (141 ms for 100 k rows) — no vacuum churn.

#### Recovery cost against key count

Recovery loads a worker's whole slot range into the segment before its RESP port
opens, so what it costs is a startup outage and a memory spike. Reproduce with
`bench/run_recovery_bench.sh`; rows are written straight into `supacache.kv`,
since recovery reads that table and populating it through RESP would be hours of
sequential durable acks.

| keys | recovery time | µs/key | peak RSS | over baseline |
|------|---------------|--------|----------|---------------|
| 0 | 3.96 ms | – | 22 MB | baseline |
| 10 k | 48.5 ms | 4.85 | 43 MB | 21 MB |
| 100 k | 470 ms | 4.70 | 93 MB | 71 MB |
| 1 M | 6.70 s | 6.70 | 595 MB | 573 MB |

Single run, **debug build**, 64-byte values, one worker, segment sized for 1.2M
keys throughout so peak RSS is comparable across rows. Debug costs roughly twice
release, so the 1M row is about 3.4 µs/key release-equivalent — the ~3.5 µs/key
figure above holds at 1M, which is as far as it has been checked.

Two things the table says that the per-key figure alone does not:

- **Time per key is flat to 100k and rises at 1M** (4.85, 4.70, 6.70). Extrapolate
  the headline figure past 1M with that in mind.
- **Memory is the binding constraint, and it is not simply per-key.** The jump
  from 10k to 100k costs 3.4× the memory for 10× the keys, because the bucket
  array is sized for the whole segment and even a small load hashes across all of
  it; from 100k to 1M it costs 8.1×, approaching linear. At 1M the 573 MB is
  around three times what the segment's own entries, buckets and slab arena
  should need for this key and value size, so something transient still scales
  with key count even though the load now streams through a cursor. Worth
  understanding before trusting a 10M extrapolation, which this shape would put
  near 6 GB.

10M and beyond are not measured here; they need a machine with the memory to
hold the answer.

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
connection to RESP3 — typed replies (map/set/double/null), `WITHSCORES`
/`WITHVALUES` member–value pairs, and push-framed pub/sub — and `CLIENT
TRACKING ON` turns on server-assisted client-side caching: keys the connection
reads are tracked, and an `invalidate` push is sent when one changes (a null
push on `FLUSHALL`/`FLUSHDB`). All tracking modes are supported — default,
`BCAST` (`PREFIX`), `OPTIN`/`OPTOUT` (`CLIENT CACHING`), and `REDIRECT` (which
lets a RESP2 client receive invalidations as `__redis__:invalidate` pub/sub
messages) — and invalidations cross workers over the Bus. So `valkey-go`
/`rueidis` can run with client-side caching enabled rather than `DisableCache`.
See the notes under [command coverage](#command-coverage).

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
own streaming replication for the standby. **It refuses to start unless
`synchronous_standby_names` is set**, and the persist worker re-checks before
every commit: with that setting empty Postgres does not wait for anything, so
the tier would quietly be plain `durable`. Query `supacache.replication_status()`
for the live picture.

#### What an acknowledgement means

The point of the tiers is that a successful reply means something specific.
These are the exact guarantees, and they are what `bench/run_durability_pg.sh`
asserts under injected failure on every CI run:

| Tier | A successful reply means |
|---|---|
| `ephemeral` | Visible in shared memory on this worker. Lost on restart, crash or eviction. Nothing is persisted. |
| `relaxed` | Visible in shared memory **and** queued for persistence. Lost if Postgres fails before the persist worker commits, bounded by `persist_window_ms` plus one batch. |
| `durable` | The persist transaction has committed with `synchronous_commit=on`. Survives `kill -9` and restart. |
| `replicated` | As `durable`, and a synchronous standby has applied the record. |

Three properties hold across all of them:

- **A write is never acknowledged and then lost.** If the record cannot be
  queued, a sync-ack tier holds the reply rather than answering `+OK`; if the
  persist transaction fails, the records stay in the ring, the acks stay held,
  and the same records commit once the failure clears.
- **A write the store cannot hold is refused**, with Redis's `OOM command not
  allowed when used memory > 'maxmemory'.` rather than acknowledged. A value
  larger than the arena is refused without evicting anything.
- **Backpressure is latency, not loss.** When the persistence ring is full a
  sync-ack connection parks and the command is retried when the ring drains.
  Nothing is dropped; `supacache.ring_stats().dropped` should stay at zero.

What is *not* guaranteed: with concurrent writers to one key, an intermediate
value may never reach `supacache.kv` at all. Writes are coalesced per key per
batch, so the durable store converges on the last write rather than replaying
every one. And the ring lives in Postgres shared memory, so anything in flight
and **not yet acknowledged** is gone if the whole cluster restarts, which is
what "not yet acknowledged" means.

### Multi-worker scale-out

```ini
pg_keyspace.workers = 4     # N shared-nothing workers on port, port+1, … port+N-1
```
Clients shard keys across the ports (Redis-Cluster style). Each worker owns a
disjoint, contiguous range of the 16384-slot CRC16 keyspace, its own shared-memory
segment, and its own persistence rings, so **scale-out composes with every
durability tier** — `workers > 1` no longer forces the ephemeral tier.

The slot map is the routing table, and it is queryable:

```sql
SELECT * FROM supacache.slot_ranges();   -- worker | port | slot_lo | slot_hi
SELECT supacache.key_worker('user:42');  -- which worker owns this key
```

**Routing is enforced when a persisted tier runs with `workers > 1`.** A key sent
to a worker that does not own it is answered with a Redis-Cluster `MOVED <slot>
<host>:<port>` (or `CROSSSLOT` for a multi-key command spanning workers) rather
than served. This is a durability requirement, not a style preference: crash
recovery restores each key into the segment whose slot range covers it, so a
worker that accepted a key it does not own would lose that key on the next
restart — after having acked the write as durable.

In that mode the cluster is discoverable, so a stock cluster client configures
itself: `INFO` reports `redis_mode:cluster` / `cluster_enabled:1`, and `CLUSTER
SLOTS`, `CLUSTER SHARDS`, `CLUSTER NODES`, `CLUSTER MYID`, `CLUSTER INFO` and
`CLUSTER KEYSLOT` publish the same map `supacache.slot_ranges()` returns. Node
ids are derived from the endpoint, so a worker keeps its id across restarts and
clients do not see the topology churn. Point the cluster constructor at any
worker's port and it discovers the rest:

```js
new Redis.Cluster([{ host: 'db.internal', port: 6379 }])   // ioredis
```
```go
redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{"db.internal:6379"}})
```

Because those clients connect to the addresses the topology names,
**`pg_keyspace.cluster_announce_host` is required** here — the workers bind
`0.0.0.0` and cannot infer an address a client can reach, and a wrong one is a
connection failure rather than a confusing error. A persisted multi-worker
cluster refuses to start without it; use `127.0.0.1` for a local-only deployment.

A *standalone* client (the default constructor in every driver — `new Redis()`,
`redis.NewClient()`, `JedisPool`) does not follow `MOVED` and will surface it as
an error. Multi-worker durability needs the cluster constructor. Clients that
auto-detect (valkey-go/rueidis, StackExchange.Redis) pick cluster mode up from
`INFO` and the slot map with no code change.

Ephemeral multi-worker deployments are unchanged: no slot enforcement, no cluster
advertisement (`redis_mode:standalone`), any key may live on any worker.

Two sizing notes: the keyspace segment and the persistence rings are both
allocated **per worker**, so `workers = 4` with `ring_mb = 64` reserves 256 MB of
rings (`pg_keyspace.persist_workers` multiplies this further, as it is a count of
rings *per worker*). Persistence worker *processes* do not scale with `workers` —
persistence worker `s` drains shard `s` of every slot worker's ring set.

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

#### What the row cache guarantees, and what it does not

The invalidation worker polls the replication slot, so coherence is **eventual
with a bound, not read-your-writes**:

> A row served from the cache reflects a state committed at or before the read,
> and no more than `pg_keyspace.rowcache_decode_ms` (default **200 ms**) plus
> decode time behind the current committed state.

Concretely:

- `INSERT` then `SELECT` is safe. A row not in the cache falls through to the
  normal index path; the cache is only consulted for keys it already holds.
- `ROLLBACK` then `SELECT` is safe. Invalidation is driven by logical decoding,
  which only ever sees committed changes, so an aborted transaction never
  poisons the cache.
- A write and a read **in the same transaction** are safe: the cache path is
  skipped for a relation that is the target of a data-modifying statement and
  for anything carrying a `FOR UPDATE`/`FOR SHARE` rowmark.
- `UPDATE` then an immediate `SELECT` **in another session** can return the
  previous row for up to that window. `DELETE` likewise: the deleted row can
  still be served until the invalidation lands.

Two operational consequences worth planning for. The decode slot is **retained
across shutdown** so invalidation can resume, which means a stopped worker pins
WAL from its `restart_lsn`: set `max_slot_wal_keep_size`. And if the slot is
lost, the worker exits and the cache keeps serving whatever it holds with no
further invalidation, so alert on the worker being alive rather than assuming.

RLS is not subject to any of this. The quals re-apply above the cached row on
every query, so a stale row is still filtered by the *current* policy for the
*current* role.

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
| `pg_keyspace.max_value_bytes` | 536870912 | largest value accepted from a client; matches Valkey/Redis `proto-max-bulk-len` |
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

### Sizing

All of it is Postgres shared memory, reserved at postmaster start. That is
deliberate: a configuration that will not fit fails at startup rather than
degrading at 3am, the same bargain as `shared_buffers`. It also means you pay
for it whether or not the cache is full.

Per worker, with k = `keys` and v = `val_bytes`:

```text
buckets = max(1024, next_pow2(2k))   x 4 B
entries = floor(1.1k) + 16           x 72 B
arena   = entries x align64(64 + v) + 1 MiB
```

Measured, and pinned by `keyspace_memory_sizing_is_a_closed_formula` so a
change to the layout breaks a test rather than every deployment's memory floor:

| Configuration | RAM per worker |
|---|---|
| 100K keys / 128 B | 28.9 MiB |
| 1M keys / 128 B | 277.6 MiB |
| **1M keys / 512 B (defaults)** | **680.4 MiB** |
| 10M keys / 512 B | 6.68 GiB |
| 1M keys / 1 KB | 1.19 GiB |

Total is `workers x` the above, plus `persist_workers x ring_mb` for the rings
and `rowcache_mb` for the row cache. Two things that catch people out:

- **`keys` is per worker, and the keyspace is sharded across workers, not
  replicated.** Raising `workers` without lowering `keys` multiplies the
  reservation for the same logical capacity. This is the most likely
  misconfiguration here.
- **Rings are allocated even when `durability = ephemeral`**, so an ephemeral
  deployment still reserves `ring_mb` it will never use.

The arena is also the real ceiling on a single value: `max_value_bytes` is a
protocol limit, but a value still has to fit in shared memory to be stored,
exactly as it has to fit `maxmemory` under Valkey. One that does not is refused
with the Redis OOM error rather than acknowledged.

Values over 8 KiB take the OVERSIZED path, which bump-allocates with a
coalescing free list. Sustained churn of *large* values of many different sizes
can therefore fragment the arena, so leave headroom if that is the workload.

---

## Layout

```
extensions/pg_keyspace/
├── core/                     shared core (Rust: libc + mio) + tools
│   └── src/
│       ├── store.rs          open-addressed hash, size-classed slab, CLOCK eviction
│       ├── server.rs         RESP2/RESP3 event loop (mio: epoll/kqueue) + dispatch
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
(`run_pubsub_tenant.sh`), RESP3 typed replies and every `CLIENT TRACKING` mode
(`run_resp3.sh`) including cross-worker invalidation (`run_tracking_xworker.sh`),
real synchronous replication (`run_replication.sh`), a real PostgREST v12.2.3
end-to-end (`run_postgrest_e2e.sh`), and scale-out (`run_scaleout.sh`,
`run_scaleout_inpg.sh`) including durability and scale-out together — slot-routed
writes, `MOVED` on misrouting, and per-worker crash recovery
(`run_persist_multiworker.sh`). Each prints its own `# result: N passed, M failed`.

Those all drive the standalone daemon. `run_durability_pg.sh` is the exception
and covers what only exists in-process: it installs the extension into a real
PostgreSQL 17, starts a cluster with it preloaded, and exercises the persistence
tiers, crash recovery, the large-value reference path, slab reclamation and the
replicated tier's startup refusal, **including injected failures**. A
persistence transaction is failed with a `CHECK (false) NOT VALID` constraint,
the persist worker is killed by name, and saturation comes from `ring_mb = 1`;
no test-only hook ships in the extension. It runs in CI on every change to
`extensions/pg_keyspace/`.

---

## Backup, restore and upgrade

`supacache.kv` and the `supacache.kv_ttl` partitions are ordinary tables, so
most of this falls out for free: `pg_dump` includes them, physical backups and
PITR include them, TTLs survive (they are a column, not runtime state), and a
restored cluster rebuilds the cache on the next worker start through the same
path as crash recovery.

The consequence worth knowing: **a dump of a busy cache silently contains the
whole cache**, which can surprise on both dump size and data retention. If the
cache is disposable, exclude it:

```bash
pg_dump --exclude-schema=supacache ...
```

The shared-memory layout is versioned (`pgks_v3`) but Postgres recreates the
segment on every start, so an upgrade never has to migrate it. Persisted data
evolves by additive columns, so an older binary reading a newer table ignores
what it does not know. Downgrade is untested.

---

## Current limitations

Scoping for this version — the extension works; these are the edges to know:

- **A persisted multi-worker cluster requires the client's *cluster* constructor**
  (`Redis.Cluster`, `NewClusterClient`, `JedisCluster`, …), not the standalone
  default, since each worker serves only its own slot range and redirects the
  rest. The topology is discoverable over `CLUSTER SLOTS`/`SHARDS`/`NODES`, so no
  client-side slot table is needed — see
  [Multi-worker scale-out](#multi-worker-scale-out). Single-worker and ephemeral
  multi-worker deployments are unaffected. Online resharding (live slot
  migration, `ASKING`/`MIGRATE`) is not supported; changing `pg_keyspace.workers`
  is a restart.
- **Pub/sub is cross-worker within one process** (the scale-out daemon), not yet
  cross-*process* for N in-PG background workers.
- **Mode B caches single-column primary keys** (composite keys are refused); the
  cache is warmed manually (`rowcache_put`) though invalidation is automatic.
- **Sorted-set scores match Valkey 8's text, with one exception.** Older Redis
  used `%.17g`; Valkey 8 uses the shortest representation that round-trips, and
  so does this, including its thresholds for printing an integral score as an
  integer and for switching to exponent form. Checked value by value against
  `valkey/valkey:8`: 399 of 400 random doubles print identically. The remainder
  are cases where Valkey's own text does not round-trip, because the Grisu2
  implementation it formats through is not always optimal; matching those byte
  for byte would mean emitting digits that parse back to a different double, so
  this prints the correctly rounded shortest form instead.
- TLS is bring-your-own-cert (in-place rotation on `SIGHUP`; no managed CA). The
  durable/replicated tiers are correct but not throughput-optimised — they
  serialize on the Postgres WAL by design.
- **TTL expiry is wall-clock, not monotonic.** Expiry compares against
  `CLOCK_REALTIME`, so a system clock *step* moves every key's deadline; NTP
  slew is harmless. On-disk reclamation also lags expiry by up to
  `ttl_bucket_secs + ttl_sweep_secs` (about 15 s at defaults), though reads
  filter on `expires_at` so nothing expired is ever served.
- **`pg_terminate_backend` on a pg_keyspace worker is survivable, but only
  because of the watchdog.** Terminating a background worker calls
  `TerminateBackgroundWorker`, which deregisters it in the postmaster rather
  than restarting it, so neither `bgw_restart_time` nor the worker count GUCs
  bring it back. Every pg_keyspace worker therefore beats a heartbeat in shared
  memory and relaunches any peer whose heartbeat goes stale, controlled by
  `pg_keyspace.watchdog_secs` (default 30, 0 disables). Set it to 0 if you need
  a worker to stay stopped.

- **There are no per-tenant quotas.** Keys and channels are force-scoped to
  `{tenant}:`, which is an isolation boundary, not an accounting one: one tenant
  can evict another's hot data or fill the persistence ring.
- Operational metrics are partial. `supacache.stats()`, `ring_stats()`,
  `rowcache_stats()` and `replication_status()` exist, and `ring_stats()` reports
  commit lag, failed batches and unresolved references; row cache invalidation
  lag is still not exposed.
