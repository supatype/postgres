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
| Data types | strings, hashes, lists, sets, sorted sets, Bloom filters, Cuckoo filters, pub/sub (+ TTL), transactions | Superset (adds streams, HLL, bitmaps, geo, scripting) |
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
| Bloom filter | `BF.RESERVE` `BF.ADD` `BF.MADD` `BF.INSERT` `BF.EXISTS` `BF.MEXISTS` `BF.INFO` `BF.CARD` `BF.SCANDUMP` `BF.LOADCHUNK` |
| Cuckoo filter | `CF.RESERVE` `CF.ADD` `CF.ADDNX` `CF.INSERT` `CF.INSERTNX` `CF.EXISTS` `CF.MEXISTS` `CF.DEL` `CF.COUNT` `CF.INFO` `CF.SCANDUMP` `CF.LOADCHUNK` |

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
  dedicated persist workers), so reads stay unaffected: a write flood's read tail
  is cut from 170 ms → 5 ms. The ring **drains** into `supacache.kv` at ~107 k/s
  per worker, scaling to 145 k/s across 4 (`bench/run_persist_scaleout.sh`).
  Read that as the capacity of the persistence machinery. What a *client* can
  obtain durable acknowledgements at depends on **how deeply it pipelines**,
  because a durable reply is held until its record commits while commands keep
  being read and applied.

  Durable tier, 256-byte values, default `ring_mb` and `persist_window_ms = 10`,
  pipelining as deeply as the connection allows:

  | connections | durable writes/s |
  |---:|---:|
  | 1 | **23 327** |
  | 2 | **34 550** |
  | 8 | **49 989** |

  One connection is enough to reach tens of thousands of durable writes a
  second; the ceiling is the persistence machinery, not the connection handler
  ([#81](https://github.com/supatype/postgres/pull/81) measured 34 581/s on one
  connection and 91 237/s at 32 on a quieter box).

  **Pipeline depth is the variable that matters**, and a shallow pipeline hides
  all of this. A sync-ack reply is held for up to one persist window, so a client
  with at most *N* writes in flight is capped near *N* per window whatever the
  server can do — at the default 10 ms window that is ~92/s waiting for each
  reply, ~1 500/s at depth 16, ~15 000/s for ten connections at depth 16. Those
  are properties of the client, not of the tier: benchmark with a depth that
  actually fills a window, or you will measure your own `-P` flag.

  Use **distinct keys** too. `redis-benchmark -t set` writes one key over and
  over unless `-r` is given, and same-key writes collapse within a persist
  window — it reports 174 k/s here and leaves `supacache.kv` holding a single
  row. The table above writes distinct keys, so it is the rate at which
  *different* rows reach Postgres.

  Before [#81](https://github.com/supatype/postgres/pull/81) (closing
  [#78](https://github.com/supatype/postgres/issues/78)) none of that was
  available: a sync-ack connection stopped being read the moment one write was
  in flight, so depth was pinned at 1 and even a deeply pipelining client got
  ~90/s. `relaxed` and `ephemeral` are unaffected and take over 350 k/s on a
  single connection.
- **Crash recovery:** after `kill -9`, keys rebuild from `supacache.kv` at
  ~3.5 µs/key; every acked durable write survives. Measured against key count by
  `bench/run_recovery_bench.sh` — the per-key time holds to 1M, but peak memory
  is the constraint that decides how large a keyspace can be restarted, so see
  [the table](#recovery-cost-against-key-count) before sizing one.
- **TTL expiry** is an O(1) partition `DROP` (3.2 ms) vs an O(n) `DELETE`
  (141 ms for 100 k rows) — no vacuum churn.

#### Tuning durable throughput: which knob actually moves it

The durable tiers commit through `supacache.kv`, so their ceiling is Postgres
commit throughput. Two settings look like they should raise it. Only one does.

Durable tier, 6 deeply-pipelined connections, distinct keys, 256-byte values,
3 reps of 8 s per cell, 4-vCPU container, PG16 — **median durable writes/s**:

| `persist_window_ms` | `persist_workers = 1` | `persist_workers = 4` |
|---:|---:|---:|
| 10 | 42 810 * | 49 768 |
| 25 | **59 923** | 48 223 |
| 50 | **64 290** | 47 536 |
| 100 | 62 780 | 58 591 |

\* two valid reps rather than three in that cell.

**`persist_window_ms` is the knob.** Going from the default 10 ms to 25 ms buys
roughly 40%, 25 → 50 ms a few percent more, and past 50 ms the curve is flat —
by then the window is no longer what any write is waiting on. What a wider
window costs is exactly the window: a durable ack is held up to that much
longer, and `relaxed`'s loss bound grows by the same amount. Measured
closed-loop at 2 000 writes/s of distinct keys (`bench/k6/ladder.js`):

| tier | write p50 | write p95 |
|---|---:|---:|
| `ephemeral` | 0.2 ms | 0.3 ms |
| `durable`, `persist_window_ms = 10` | 6.5 ms | 13.5 ms |
| `durable`, `persist_window_ms = 50` | 27.2 ms | 53.8 ms |

So the 50 ms window that buys ~50% more durable throughput costs about **4× the
write latency**. The default stays at 10 ms because it favours latency;
**25–50 ms is the range worth trying if durable write throughput is the
constraint and your writers can wait.**

**More `persist_workers` does not raise the ceiling.** At a well-chosen window
four workers are no better than one and usually worse, with a much wider spread
(one cell ran 64 023, 48 223, 47 488 across its three reps). That is the
expected result rather than a surprising one: `persist_workers` multiplies ring
and drain capacity, and the ring is not the bottleneck — commit is. Four
workers commit the same rows through four transactions instead of one, so the
batching gets worse before the parallelism pays. It helps in one place only, at
a 10 ms window, where a single worker's window *is* the constraint. Raise
`persist_workers` to add ring capacity under a write flood
([per-tenant fairness](#per-tenant-fairness) and `ring_mb` are the relevant
knobs there), not to chase throughput.

The third option often suggested — a WAL-bypass fast log with its own fsync for
the non-replicated `durable` tier — is deliberately **not** implemented. It
would break the property the tier exists to provide: that a durable ack means
the row is already committed in `supacache.kv`, visible to SQL, and recovered by
Postgres's own crash recovery rather than by a second recovery path of ours.
That is a trade worth making only against a measured need this benchmark does
not show.

#### What a durable write costs in WAL and storage

Operations per second says nothing about what the durable tiers cost on disk.
Reproduce with `bench/run_wal_amplification.sh`, which measures WAL bytes per
logical cache write across key skew and reads the row counts out of the WAL with
`pg_waldump` rather than the statistics views (see the note at the end of this
section for why).

200 k writes per pass, 128-byte values, 20 k keyspace, one worker, one persist
worker, `persist_window_ms = 10`, `full_page_writes = on`, release build,
4-vCPU container, PG16.

| key skew | pass | WAL/write | FPI share | row versions | dedup |
|---|---|---:|---:|---:|---:|
| uniform | cold | 225 B | 12% | 199 919 | 1.0× |
| uniform | warm | 220 B | 11% | 199 959 | 1.0× |
| Zipfian (s=1) | cold | 211 B | 13% | 190 750 | 1.0× |
| Zipfian (s=1) | warm | 208 B | 13% | 190 793 | 1.0× |
| single hot key | cold | 23 B | 0% | 25 008 | 8.0× |
| single hot key | warm | 23 B | 0% | 25 005 | 8.0× |

**Cold is the pass straight after a `CHECKPOINT`, warm the pass straight after
that.** Every configuration is measured twice because the first touch of a page
after a checkpoint carries a full-page image; a real deployment sits between the
two rows and moves with checkpoint frequency, so neither is "the" number. Each
configuration primes its whole keyspace first, unmeasured, so both measured
passes are pure overwrites rather than one insert pass and one overwrite pass.

**Skew is what moves the answer, and it moves it by an order of magnitude.** The
persist worker collapses a window's writes to the last one per key before the
statement runs, so a hot key rewritten many times in one window costs one row
version. A single hot key costs 23 B/write against uniform's 225 B — 8× fewer row
versions for the same client traffic. Ordinary skew does not get you this: over a
20 k keyspace even a Zipfian distribution dedupes only 1.0×, because at the rate a
client can actually issue durable writes each 10 ms window holds too few writes to
collide. Budget with the uniform row unless you know you have genuinely hot keys.

**Overwrites do not bloat the tables.** 400 k overwrites of 20 k rows grew the
whole `supacache` schema by 0.3 MB: cache overwrites are HOT updates, which
opportunistic page pruning reclaims without waiting for a vacuum.

**Normal SQL is not starved by the flood.** With the row cache registered and
`pgbench` reading a cached table throughout, SQL held 24 415 tps at 0.15 ms mean
latency, worst interval 21 668 tps.

Two limits on these figures worth stating:

- **The rate sweep could not be exercised.** The harness sweeps offered write
  rates, but no rate above ~700/s was reachable when it was run, because every
  durable connection then stalled on each reply
  ([#78](https://github.com/supatype/postgres/issues/78), since fixed in
  [#81](https://github.com/supatype/postgres/pull/81)) — so every rate row is
  really a max-rate row and the harness marks them "(not reached)". Since window
  occupancy is what drives dedup, the dedup column above is a lower bound: a
  deployment that can fill its persist windows will dedupe more than this, and
  a re-run on a build that pipelines should now reach the higher rates.
- **The statistics views cannot be used to measure any of it.** The persist
  worker never flushes its pending statistics, so `pg_stat_user_tables` reports
  zero rows written for the `supacache` tables for the entire life of a running
  cluster ([#77](https://github.com/supatype/postgres/issues/77)). That is also
  why autovacuum never fires on them, and why this benchmark reads the WAL
  instead.

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

### Bloom and Cuckoo filters

A Bloom filter and a Cuckoo filter answer "have I seen this item?" in a fixed
amount of memory: a false positive is possible at a rate you choose, a false
negative is not. A Cuckoo filter also deletes an item and counts how many copies
of it are present; a Bloom filter does neither.

```bash
redis-cli -p 6381 BF.RESERVE seen 0.01 1000000
redis-cli -p 6381 BF.ADD seen user:42         # 1
redis-cli -p 6381 BF.EXISTS seen user:42      # 1
```
```bash
redis-cli -p 6381 CF.RESERVE recent 1000000
redis-cli -p 6381 CF.ADD recent user:42       # 1
redis-cli -p 6381 CF.DEL recent user:42       # 1
redis-cli -p 6381 CF.EXISTS recent user:42    # 0
```

`TYPE` answers `MBbloom--` and `MBbloomCF`, exactly as RedisBloom does, so a
stock client works unchanged — `redis-py`'s `bf()` and `cf()` helpers,
`NRedisStack`, `redis-om`. Error texts and reply shapes match Redis 8 reply for
reply: the two conformance harnesses (`bench/run_bloom.sh`,
`bench/run_cuckoo.sh`) exercise every command, then replay the same sequences
against a real Redis 8 in Docker and compare the replies, on every CI run.

#### How a filter behaves in the store

One key holds one value blob, the same as a hash or a set. `BF.ADD` and `CF.ADD`
set bits or fingerprints **in place**, under the entry's seqlock, so an add
costs the same whatever the filter size. A filter that fills up grows by
appending a sub-filter, which rewrites the blob once: a Bloom filter appends
capacity × expansion and halves the error rate each time, a Cuckoo filter
appends the same bucket count each time. Defaults match RedisBloom — Bloom
capacity 100, error 0.01, expansion 2; Cuckoo bucket size 2, max iterations 20,
expansion 1.

Hashing is MurmurHash64A with the same double-hashing scheme RedisBloom uses, so
false-positive behaviour is close to it. The blob layout is pg_keyspace's own.

#### Benchmarks (`bench/run_prob_bench.sh`)

Measured on a 24-vCPU WSL2 box against Redis 8.10.1 in Docker. Closed-loop p50,
and pipelined throughput at `-c50 -P16`. **The Redis column is measured inside
the container**: the published Docker port adds about 0.08 ms to every round
trip, which would flatter pg_keyspace — the script prints both columns so you
can see that cost rather than take it on trust.

| Operation | pg_keyspace p50 | Redis 8 p50 | pg_keyspace pipelined | Redis 8 pipelined |
|---|---:|---:|---:|---:|
| `BF.ADD`, 1 M-item filter | **0.070 ms** | 0.071 ms | 949 k/s | 952 k/s |
| `BF.EXISTS`, 1 M-item filter | 0.063 ms | 0.063 ms | **1.17 M/s** | 1.07 M/s |
| `BF.ADD`, 10 M-item filter | **0.057 ms** | 0.063 ms | **944 k/s** | 899 k/s |
| `BF.EXISTS`, 10 M-item filter | **0.053 ms** | 0.055 ms | **1.06 M/s** | 1.01 M/s |
| `CF.ADD`, 1 M-item filter | **0.050 ms** | 0.055 ms | **1.13 M/s** | 1.07 M/s |
| `CF.EXISTS`, 1 M-item filter | **0.049 ms** | 0.055 ms | **1.08 M/s** | 1.03 M/s |
| `CF.DEL`, 1 M-item filter | **0.050 ms** | 0.055 ms | 996 k/s | 995 k/s |
| `GET` (baseline) | 0.050 ms | 0.055 ms | — | — |

A **filled** 10 M-item filter answers `BF.EXISTS` at the speed of an empty one —
1.38 M/s against 1.40 M/s pipelined. That is what the in-place path buys.

Memory, from `BF.INFO SIZE` and `CF.INFO Size` against the same reserve:

| Filter | pg_keyspace | Redis 8 |
|---|---:|---:|
| Bloom, 1 M items at 1% | **1 198 193 B** | 1 378 568 B |
| Bloom, 10 M items at 1% | **11 981 385 B** | 13 784 792 B |
| Cuckoo, `CF.RESERVE 1000000` | 1 048 617 B | 1 048 632 B |

#### What a filter costs to persist

Filters ride the existing aggregate path, so in the `relaxed`, `durable` and
`replicated` tiers a dirty filter is written to `supacache.kv` once per persist
window, **whole**. A 1.2 MB filter (1 M items at 1%) under steady adds is about
120 MB/s of WAL at the default 10 ms window. A filter of tens of MB cannot keep
up, and its writers park on the ring. So: any size in `ephemeral`; in a
persisted tier, only filters that are small or write-cold.

Crash recovery brings a persisted filter back with its type, its items and its
TTL, and a filter whose TTL expired stays gone —
`bench/run_prob_durability_pg.sh` asserts all of that in CI, across 30 checks. A
per-item delta log is the fix for the write cost, and is not in this change.

#### Sizing a filter

A filter is a value, so it has to fit the arena. Three consequences:

- **The oversized allocator rounds to a power of two**, so a 120 MB filter
  reserves 128 MB.
- **A scaling chain is not free.** It holds two to three times the bits of one
  right-sized filter, because each sub-filter carries a tighter error rate.
- **One filter blob stops at 512 MB**, which is also the
  `pg_keyspace.max_value_bytes` default — about 400 M items at 1%. Raising
  `max_value_bytes` past its default does not raise this ceiling.

What to do about it: `BF.RESERVE` with the real capacity, so the filter
allocates once; `NONSCALING` when the population is known; and raise
`pg_keyspace.val_bytes` and `pg_keyspace.keys` so the arena holds the filters
*plus* the cache. Note also that the oversized allocator is first-fit and does
not split blocks, so many filters of different sizes growing at different times
fragment the arena — the same warning [Sizing](#sizing) gives for large values.

#### `SCANDUMP` and `LOADCHUNK`

The iterator is a byte offset into the blob, and a chunk is at most 16 MiB, or
`pg_keyspace.max_value_bytes` when that is lower. A dump round-trips within
pg_keyspace. A dump made by RedisBloom does **not** load here: the blob layout
is pg_keyspace's own. `LOADCHUNK` validates every header field and refuses a
chunk that does not describe a filter.

#### Limits worth knowing

- `BF.INFO SIZE` and `CF.INFO Size` report the pg_keyspace blob size, not
  RedisBloom's — compare them across servers with that in mind.
- `CF.INFO` "Number of buckets" reports the *first* sub-filter's bucket count,
  as RedisBloom does, not the total across a grown chain.
- There are no `BF.*`/`CF.*` SQL functions yet. The filters are reachable over
  RESP only.

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
SELECT supacache.rowcache_register('public.orders');      -- the whole primary key, any arity
SELECT supacache.rowcache_put('public.orders', 42);       -- warm one row
EXPLAIN SELECT * FROM public.orders WHERE id = 42;
--  Custom Scan (pg_keyspace_rowcache) on orders
```

The one-argument form takes no column because there is nothing to choose: the key
is whatever `pg_index` says it is, single-column or composite. `rowcache_register(tbl, attnum)`
still exists and is still single-column only. `rowcache_registration(tbl)` reports
which columns a table is registered with, in key order.

**The row cache serves one database — the one named by `pg_keyspace.database`.**
Not a choice but a consequence of logical decoding: the invalidation worker's
replication slot belongs to that database, and a logical slot only ever decodes
changes from the database it was created in. A table registered anywhere else
would be cached and then *never invalidated* — stale indefinitely, while
`rowcache_coherence()` still reported healthy, because coherence describes the
worker rather than your table. `rowcache_register` therefore refuses from any
other database and says so.

Keys carry the database oid for the same reason. The segment is cluster-wide and
`shared_preload_libraries` installs the planner hook in *every* database, so an
unqualified `relid`-keyed entry was ambiguous across databases — and relids
collide: `CREATE DATABASE ... TEMPLATE` copies `pg_class` physically, so cloned
databases have **identical** relids, which made the collision certain in a
per-project-database deployment rather than a remote possibility. Before
[#117](https://github.com/supatype/postgres/issues/117) that served one
database's rows to another, under a correct-looking `Custom Scan` plan, with no
error — and RLS could not help, since it is re-applied above the cache and would
evaluate the *querying* database's policies against the *other* database's row.
Asserted by `bench/run_rowcache_database_scope.sh`, which clones two databases
from one template precisely so their relids match.

**Registrations are durable.** They are rows in `supacache.rowcache_reg`, recorded
under the table's schema-qualified name, with the pinned shared-memory entry as a
cache of that. Anything that reinitialises the row-cache segment — a watchdog
relaunch, a crash-restart, `pg_terminate_backend` on a worker, an ordinary
restart — loses the cached rows, as a cache should, but not the registrations:
the invalidation worker reloads them on its next pass. Recording the *name*
rather than the oid means a table dropped and recreated by a migration, or
restored from a dump, keeps its registration. `rowcache_reload_registrations()`
forces a reload if you ever need to.

Before this ([#103](https://github.com/supatype/postgres/issues/103)) a
registration lived only in shared memory. `rowcache_register` returned true, the
entry could then vanish, and the table silently stopped being cached — with no
error and `rowcache_coherence()` still reporting healthy, because coherence
describes the invalidation worker rather than your registration. Verified by
`bench/run_rowcache_registration.sh`, which asserts the cached row is gone after
a restart *and* the registration is not, since only that pair distinguishes a
reload from a segment that happened to survive.

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
pg_keyspace.rowcache_readthrough = on  # (optional) warm on a miss, instead of only via rowcache_put
```

`rowcache_readthrough` is what removes the manual warming step: a primary-key
lookup that misses caches the row it just read. It is off by default and is
ignored unless `rowcache_decode` is on — a row warmed automatically that nothing
is watching would be served stale indefinitely, so the two are deliberately
coupled.

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
tenant cannot address or subscribe to another's.

For TLS, either point `tls_cert_file` / `tls_key_file` at a PEM cert+key, or
**reuse the certificate the cluster already serves**:

```conf
ssl = on                                  # the cluster's own TLS, as usual
ssl_cert_file = 'server.crt'
ssl_key_file  = 'server.key'
pg_keyspace.tls_use_postgres_cert = on    # RESP serves that same certificate
```

Whichever way, rotation is in place: swap the files and `SELECT
pg_reload_conf()` — no restart, and existing connections keep the cert they
started on. Inheriting means there is no second certificate to obtain or renew,
so a `cert-manager` or ACME renewal that already covers the Postgres port
covers the RESP port with it.

Three things about `tls_use_postgres_cert` are deliberate:

- **It is off by default**, and stays off after an upgrade. Turning it on
  implicitly would encrypt a port that plaintext clients are already connected
  to, and break all of them at the next restart. Opting in is the operator
  saying the libpq cert is the cert they want here.
- **`ssl_cert_file` is resolved against the data directory**, the way Postgres
  resolves it — its default is the bare name `server.crt`.
- **It fails closed.** With the flag on and `ssl = off` there is no certificate
  to inherit, so the worker refuses to serve rather than quietly falling back to
  plaintext, and the log says so. An explicit `pg_keyspace.tls_cert_file` always
  wins over the inherited one, so enabling the flag cannot take over a
  deployment that configured its own cert.

All four behaviours are checked against the wire — including by fingerprint,
that the RESP port and Postgres serve the same certificate — by
`bench/run_tls_inherit.sh`.

### Per-tenant fairness

Writes cross from the RESP worker to Postgres through one shared ring per
persist shard, FIFO. Without a share, a tenant writing hard enough to keep that
ring full starves every other tenant on it: their writes meet a full ring and
park. Measured with a 1 MiB ring, 1 KiB values and eight flooding connections against
a victim tenant doing ordinary sequential writes on four connections, three
rounds of ten seconds each (`bench/run_tenant_fairness.sh`):

| durable tier | share off | share on |
|---|---:|---:|
| victim writes per round | 33, 32, 29 | **270, 270, 305** |
| victim writes, total | 94 | **845** |
| victim p50 | 363.8 ms | **12.6 ms** |
| flood writes, total | 206 750 | 202 693 |

**9×** the throughput and **29×** the median latency for the victim, with no
overlap between the two distributions, and the flood is unaffected (−2%) —
capping it costs it nothing measurable.

The per-round numbers are given because one round is a sample, not a result: an
earlier version of this measurement used a *single* victim connection, whose
counts (0–38) were small enough that scheduling noise sometimes ranked the two
configurations the wrong way round.

`pg_keyspace.tenant_ring_share` is on by default, and is inert until there is
something to be fair about:

- **Below half a ring nobody is policed.** A ring that is keeping up never sees
  this change a decision.
- **Connections with no tenant scope are never policed** — unauthenticated, or
  an exempt service role — so a deployment that does not use tenant scoping is
  unaffected.
- **The share is dynamic**: a tenant may hold up to `capacity / active tenants`,
  recomputed per decision, floored at 256 KiB. One tenant alone gets the whole
  ring; two contending tenants get half each.

A tenant counts as active from the moment it *attempts* a write, not from when
it has bytes in flight — a tenant a full ring is shutting out holds nothing, and
counting only occupancy makes the tenant that most needs the policy invisible to
it.

#### Cache memory

The same shape of problem one layer up: a cold-key flood from one tenant simply
evicted everyone else, because CLOCK admits cold keys unconditionally and takes
whatever is unreferenced — another tenant's hot data as readily as its own.

`pg_keyspace.tenant_scoped_eviction` (on by default) makes the sweep prefer a
victim under the same `{tenant}:` prefix as the key being inserted, so a flood
recycles its own space. A victim tenant's 200-key working set against a
40 000-key cold flood, in-Postgres:

| | scoped eviction off | on |
|---|---:|---:|
| victim keys surviving | 0 / 200 | **193 / 200** |

It is a preference, not a budget: a tenant with nothing evictable of its own
falls through to the ordinary sweep, so a small or new tenant is never starved,
and the flooding tenant keeps its own recent keys. Keys with no `:` — an
unscoped or exempt deployment — are evicted exactly as before.

#### Request rate

`pg_keyspace.tenant_ops_per_sec` (0 = off, the default) bounds how much of a
worker's event loop one tenant may ask for. The other two axes do not see a
tenant issuing only reads: it stages no ring records and evicts nothing, and can
still saturate the loop.

A tenant may burst up to one second's worth and then proceeds at the rate.
Over-rate commands are **held and retried, not refused**, which is the same
contract a full persistence ring already has — no client learns a new error and
no command is lost.

#### Seeing it

`INFO` reports a `# Tenants` section with in-flight ring bytes, writes held back
by the share, and commands held back by the rate limit, for the asking
connection's own tenant (an unscoped or exempt connection sees every tenant).
Measure all of it with `bench/run_tenant_fairness.sh`, which runs the same loads
with each policy off and on and prints both.


#### Changing the worker count

`pg_keyspace.workers` is a restart, and the restart reshuffles which worker owns
which slot. `supacache.kv.slot` is stable so **nothing is lost** — but most of
the persisted keyspace comes back into a *different* worker's segment, which is
that much of the warm cache dropped and re-recovered.

How much moves is not intuitive:

| change | slots moving |
|---|---:|
| 1 → 2 | 50% |
| 2 → 4 | **75%** |
| 4 → 8 | **87.5%** |

The guess that "each range splits in half, so half stays put" is wrong. The
ranges are contiguous, so only worker 0's first sub-range keeps its owner —
with 4 workers slot 4096 belongs to worker 1, with 8 workers it belongs to
worker 2, and so on up the range.

The layout is recorded in `supacache.topology`, so a change is logged at startup
with what it costs, and `supacache.topology_change()` reports it:

```sql
SELECT * FROM supacache.topology_change();
-- recorded_workers | running_workers | slots_moved | pct_moved
--                2 |               4 |       12288 |      75.0
```

This is the first slice of
[#101](https://github.com/supatype/postgres/issues/101) and only that: the
change is made visible, not made online. Verified by
`bench/run_topology_change.sh`, which asserts both the reported number and that
every key still reads back afterwards — the "no data is lost" half of the
warning is worth asserting rather than just claiming.

#### Cache memory: a preference, and a budget

`pg_keyspace.tenant_scoped_eviction` (on by default) makes the CLOCK sweep prefer
a victim under the same `{tenant}:` prefix as the key being inserted, so a
cold-key flood recycles its own space. A victim tenant's 200-key working set
against a 40 000-key flood survives **193/200**, against 0/200 without it.

That is a *preference*, and it leaves a gap: it points at whoever is inserting.
A tenant that arrived first and grew steadily is never the one inserting under
pressure, so the preference never points at it and it keeps everything it has.

`pg_keyspace.tenant_arena_pct` (0 = off, the default) closes that. Above 0, a
tenant holding more than that share of a partition's entries is evicted from
first, whoever is inserting. A 900-key hog against a 1500-key arena, with a
second tenant then writing steadily (`bench/run_tenant_fairness.sh`):

| the second tenant's writes | hog, no budget | hog, `tenant_arena_pct = 25` |
|---:|---:|---:|
| 900 | 887 | 766 |
| 2 700 | 873 | **382** |
| 5 400 | 873 | **382** |

It **converges on the share and stops** — 382 against a 375-key budget, and more
pressure does not push it lower. Without a budget the hog holds 873 however long
the contention lasts, which is the gap in one number.

Two properties worth knowing:

- **It reclaims on demand, not proactively.** Space is taken from an over-budget
  tenant when somebody needs a slot, never by background-trimming space nobody
  wants. So a budget does nothing at all on an uncontended cache, and converges
  as contention continues — which is why the 900-write row above only dents it.
- **Usage is measured, not accumulated.** The obvious implementation is a running
  total per tenant adjusted on every insert, overwrite, eviction and expiry.
  `set_in` alone changes a value's length in four places, and a counter that
  drifts enforces something fictional — evicting a tenant that is not over,
  silently, with no cheap way to notice. Instead the snapshot is recomputed by
  one linear pass over the entry array, amortised across evictions. It cannot
  drift, because nothing accumulates. The cost is that the eviction path's view
  is slightly stale, which a policy tolerates.

`INFO` reports `tenant_<name>_arena:bytes=…,entries=…` beside the ring rows,
whether or not a budget is configured — knowing whether a deployment actually
has this problem is useful before enforcing anything about it.
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
| `pg_keyspace.database` | `postgres` | database holding `supacache.kv` backing tables, **and the only database the row cache serves** |
| `pg_keyspace.persist_workers` | 1 | persist workers/rings draining in parallel |
| `pg_keyspace.ring_mb` | 64 | per-worker RESP→persist ring size (burst absorption) |
| `pg_keyspace.ttl_bucket_secs` | 10 | TTL time-bucket width (range-partitioned `supacache.kv_ttl`) |
| `pg_keyspace.require_mask` | `off` | `off` (default) runs standalone; `on` fails closed unless `supatype_mask` is loaded + outermost — set by the Supatype platform |
| `pg_keyspace.tls_cert_file` / `tls_key_file` | *(empty)* | PEM cert + key → serve RESP over TLS |
| `pg_keyspace.tls_use_postgres_cert` | `off` | with those unset, serve RESP with the cluster's `ssl_cert_file`/`ssl_key_file` (needs `ssl = on`) |
| `pg_keyspace.rowcache_mb` | 64 | Mode B row-cache segment size (never RESP-addressable) |
| `pg_keyspace.rowcache_decode` | `off` | keys-only Mode B invalidation worker (needs `wal_level=logical`) |
| `pg_keyspace.rowcache_refill` | `off` | on: re-cache a changed hot key; off: drop-only (lazy) |
| `pg_keyspace.rowcache_readthrough` | `off` | on: a pk lookup that misses caches the row it read (ignored unless `rowcache_decode` is on) |
| `pg_keyspace.tenant_ring_share` | `on` | give each tenant a share of the persistence ring instead of first come, first served |
| `pg_keyspace.tenant_scoped_eviction` | `on` | evict a tenant's own cold keys before another tenant's |
| `pg_keyspace.tenant_arena_pct` | 0 | cap one tenant at this % of a partition's entries (0 = off); over-budget tenants are evicted from first |
| `pg_keyspace.tenant_ops_per_sec` | 0 | commands per second one tenant may issue (0 = no limit) |

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
│       ├── prob.rs           Bloom (`BF.*`) and Cuckoo (`CF.*`) filters: blob formats, hashing, handlers
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
(`run_hashes.sh`, `run_lists.sh`, `run_zsets.sh`, `run_pubsub.sh`), the
probabilistic filters — every `BF.*`/`CF.*` command, then the same sequences
against a real Redis 8 in Docker reply for reply (`run_bloom.sh`,
`run_cuckoo.sh`) — security
(`run_hardening.sh`, `run_tls.sh`, `run_threats.sh`, `run_security.sh`), Mode B
row-cache coherence for int/uuid/text/TOAST PKs (`run_rowcache.sh`,
`run_nonint_pk.sh`, `run_toast.sh`, `run_invalidation.sh`), tenant-scoped pub/sub
(`run_pubsub_tenant.sh`) and the per-tenant ring share
(`run_tenant_fairness.sh`), RESP3 typed replies and every `CLIENT TRACKING` mode
(`run_resp3.sh`) including cross-worker invalidation (`run_tracking_xworker.sh`),
real synchronous replication (`run_replication.sh`), a real PostgREST v12.2.3
end-to-end (`run_postgrest_e2e.sh`), and scale-out (`run_scaleout.sh`,
`run_scaleout_inpg.sh`) including durability and scale-out together — slot-routed
writes, `MOVED` on misrouting, and per-worker crash recovery
(`run_persist_multiworker.sh`). Each prints its own `# result: N passed, M
failed`. `run_prob_bench.sh` is a benchmark rather than a harness: it prints the
`BF.*`/`CF.*` latency, throughput and `INFO SIZE` tables above, against the same
Redis 8.

Those all drive the standalone daemon. `run_durability_pg.sh` is the exception
and covers what only exists in-process: it installs the extension into a real
PostgreSQL 17, starts a cluster with it preloaded, and exercises the persistence
tiers, crash recovery, the large-value reference path, slab reclamation and the
replicated tier's startup refusal, **including injected failures**. A
persistence transaction is failed with a `CHECK (false) NOT VALID` constraint,
the persist worker is killed by name, and saturation comes from `ring_mb = 1`;
no test-only hook ships in the extension. `run_prob_durability_pg.sh` runs the
same way on its own cluster, for the filters: it writes a Bloom filter and a
Cuckoo filter in the `durable` tier, kills the cluster with `kill -9`, and
asserts that each one comes back with its type and its items — and that a filter
whose TTL expired stays gone. Both run in CI on every change to
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

#### How the persisted schema evolves

There is no migration script and no version table. `pg_ensure_schema()` runs the
whole DDL as `CREATE TABLE IF NOT EXISTS` / `ALTER TABLE ... ADD COLUMN IF NOT
EXISTS` / `CREATE INDEX IF NOT EXISTS` at every worker start, before recovery
reads a single row, so the tables converge on whatever the running binary
expects. Worker 0 runs it alone, so N workers do not race on it.

Adding a column is therefore two edits, not one: the `CREATE TABLE` (which only
covers clusters that do not exist yet) **and** an `ADD COLUMN IF NOT EXISTS ...
DEFAULT ...` retrofit beside it (which covers every cluster that already does).
Exactly one column has been added since the first release — `kv_ttl.kind`, so a
TTL'd hash/list/set/zset recovers as its own type instead of a raw string — and
it carries both halves.

**Upgrading** — a newer binary against older tables — is what that buys. The
retrofit runs ahead of recovery in the same worker, so by the time anything
reads `supacache.kv_ttl` the column is there.

**Downgrading** — an older binary against newer tables — works structurally.
Every statement names its columns explicitly, so a column the old binary has
never heard of is ignored on read and omitted on write. That is safe only
because retrofitted columns carry a `DEFAULT`; a `NOT NULL` column added
without one would make the old binary's `INSERT` fail outright. Neither
direction loses a row.

What both directions can lose is what a row *was*, in the one case where a
column records meaning rather than data. `kv_ttl.kind` is that column, and it
behaves the same way whichever direction you crossed it in: a TTL'd
hash/list/set/zset that was persisted without it — written by an older binary,
or written before the retrofit existed — carries the default `'s'`, so a
current binary recovers it as a string. The bytes are intact, but `TYPE` says
`string` and the aggregate's own commands answer `WRONGTYPE`. Rewriting the key
fixes it permanently, since writes after the retrofit record the type again.
Plain strings, and anything in `supacache.kv`, are unaffected.

Both directions are asserted in section AC of `bench/run_durability_pg.sh`,
which runs in CI: the upgrade case by dropping `kv_ttl.kind` to reproduce the
pre-retrofit table exactly and restarting into it, the downgrade case by
replaying the older binary's statements verbatim against today's tables — one
row given the identical bytes of a live hash through the old column list, so
that the type column is the only difference between a key that comes back a
hash and a key that comes back a string. That section also fails if any future
`NOT NULL` column lands without a default.

The shared-memory segment is versioned separately (`pgks_v3` plus a layout
version and the five geometry fields) and is never migrated: Postgres recreates
it on every start. A mismatch is refused at attach time with the fields named
rather than mis-read.

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
- **Mode B read-through is opt-in and needs the decode worker.**
  `pg_keyspace.rowcache_readthrough` is off by default and does nothing unless
  `rowcache_decode` is also on — warming a row the cluster cannot invalidate
  would serve it stale forever, so read-through refuses to warm what nothing is
  watching. With it off, the cache holds only what `rowcache_put` places.
  Composite primary keys are supported, but **all-or-nothing at plan time**: a
  query must pin every key column to a constant to be served from the cache, and
  a partial key takes the ordinary index path (it asks for a set, and one cached
  row is not one).
- **Sorted-set scores match Valkey 8's text, with one exception.** Older Redis
  used `%.17g`; Valkey 8 uses the shortest representation that round-trips, and
  so does this, including its thresholds for printing an integral score as an
  integer and for switching to exponent form. Checked value by value against
  `valkey/valkey:8`: 399 of 400 random doubles print identically. The remainder
  are cases where Valkey's own text does not round-trip, because the Grisu2
  implementation it formats through is not always optimal; matching those byte
  for byte would mean emitting digits that parse back to a different double, so
  this prints the correctly rounded shortest form instead.
- **TLS certificates come from the operator, not from a CA integration.**
  Either supply them directly (`tls_cert_file`/`tls_key_file`) or set
  `tls_use_postgres_cert` to reuse the cluster's — the latter means whatever
  already renews the Postgres certificate renews this one too. There is no ACME
  client in the extension itself, which is the right place for it not to be: a
  background worker inside Postgres is a poor place to be answering HTTP-01
  challenges, and the deployments that want automated certs already terminate
  or renew at a gateway.
- **The durable/replicated tiers serialize on the Postgres WAL by design**, so
  their ceiling is commit throughput rather than anything in the cache. That is
  a correctness choice, not an unoptimised path: a durable ack means the row is
  committed in `supacache.kv`. `persist_window_ms` is the setting that moves the
  number and `persist_workers` is not — see
  [tuning durable throughput](#tuning-durable-throughput-which-knob-actually-moves-it)
  for the measured curve.
- **A Bloom or Cuckoo filter in a persisted tier is rewritten whole, once per
  persist window.** A 1.2 MB filter under steady adds is about 120 MB/s of WAL
  at the default window, and a filter of tens of MB cannot keep up — its writers
  park on the ring. Large filters belong in the `ephemeral` tier until a
  per-item delta log exists; see
  [what a filter costs to persist](#what-a-filter-costs-to-persist).
- **TTL expiry is immune to wall-clock steps, and still reports wall-clock
  times.** Expiry compares against an anchor — realtime and `CLOCK_BOOTTIME`
  captured together, then advanced by the boottime delta — so a clock *step*
  does not move any deadline, while `expires_at` stays an absolute unix
  timestamp and every persisted value and `kv_ttl` bucket keeps its meaning.
  Time a machine spends suspended counts toward a TTL, which is why
  `CLOCK_BOOTTIME` rather than `CLOCK_MONOTONIC`. The anchor lives in shared
  memory so every backend agrees; a per-process anchor would have processes
  that started either side of a step disagreeing about whether a key is
  expired. Slew is ignored along with steps, so over long uptime the clock
  drifts slightly from true wall time — irrelevant for a relative TTL, visible
  only for an absolute deadline set by `EXPIREAT`, and re-anchored on restart.
  Verified by `bench/run_ttl_clock_step.sh`, which sets the system clock for
  real ([#110](https://github.com/supatype/postgres/issues/110)).
- **`pg_terminate_backend` on a pg_keyspace worker is survivable, but only
  because of the watchdog.** Terminating a background worker calls
  `TerminateBackgroundWorker`, which deregisters it in the postmaster rather
  than restarting it, so neither `bgw_restart_time` nor the worker count GUCs
  bring it back. Every pg_keyspace worker therefore beats a heartbeat in shared
  memory and relaunches any peer whose heartbeat goes stale, controlled by
  `pg_keyspace.watchdog_secs` (default 30, 0 disables). Set it to 0 if you need
  a worker to stay stopped.

- **Per-tenant fairness covers the ring, cache memory and request rate; worker
  placement is still unmanaged.** Keys map to workers by CRC16 slot, so a tenant
  with a hot key range concentrates on one worker with no rebalancing, and
  changing `pg_keyspace.workers` is a restart
  ([#101](https://github.com/supatype/postgres/issues/101)). Slots cannot be
  moved between running workers: there is no slot map to consult, no
  migrating/importing state, and no `ASK` redirection. What a worker-count
  change *does* do is now reported rather than silent — see below.
- Operational metrics are partial. `supacache.stats()`, `ring_stats()`,
  `rowcache_stats()` and `replication_status()` exist, and `ring_stats()` reports
  commit lag, failed batches and unresolved references; row cache invalidation
  lag is still not exposed.
