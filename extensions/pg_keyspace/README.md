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
| Data types | strings, hashes, lists, sets, sorted sets, bitmaps, Bloom filters, Cuckoo filters, pub/sub (+ TTL), transactions | Superset (adds streams, HLL, geo, scripting) |
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
| Bitmaps | `SETBIT` `GETBIT` `BITCOUNT` `BITPOS` `BITOP` `BITFIELD` `BITFIELD_RO` |
| Hashes | `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HLEN` `HEXISTS` `HSTRLEN` `HINCRBY` `HINCRBYFLOAT` `HRANDFIELD` `HSCAN` |
| Lists | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LLEN` `LINDEX` `LRANGE` `LSET` `LTRIM` `LINSERT` `LREM` `LPOS` `LMOVE` `RPOPLPUSH` |
| Sets | `SADD` `SREM` `SCARD` `SISMEMBER` `SMISMEMBER` `SMEMBERS` `SPOP` `SRANDMEMBER` `SMOVE` `SSCAN` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE` `SINTERCARD` |
| Sorted sets | `ZADD` `ZREM` `ZSCORE` `ZMSCORE` `ZCARD` `ZINCRBY` `ZRANK` `ZREVRANK` `ZCOUNT` `ZRANGE` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZRANGEBYLEX` `ZREVRANGEBYLEX` `ZLEXCOUNT` `ZRANGESTORE` `ZPOPMIN` `ZPOPMAX` `ZRANDMEMBER` `ZMPOP` `ZSCAN` `ZUNION` `ZINTER` `ZDIFF` `ZUNIONSTORE` `ZINTERSTORE` `ZDIFFSTORE` |
| Pub/sub | `SUBSCRIBE` `UNSUBSCRIBE` `PSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB` |
| Transactions | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |
| Bloom filter | `BF.RESERVE` `BF.ADD` `BF.MADD` `BF.INSERT` `BF.EXISTS` `BF.MEXISTS` `BF.INFO` `BF.CARD` `BF.SCANDUMP` `BF.LOADCHUNK` |
| Cuckoo filter | `CF.RESERVE` `CF.ADD` `CF.ADDNX` `CF.INSERT` `CF.INSERTNX` `CF.EXISTS` `CF.MEXISTS` `CF.DEL` `CF.COUNT` `CF.INFO` `CF.SCANDUMP` `CF.LOADCHUNK` |

**Not yet supported** — scripting (`EVAL`/`FUNCTION`), streams (`XADD`…),
blocking ops (`BLPOP`/`BRPOP`/`BZPOPMIN`…), HyperLogLog, geo, and cluster
commands. HyperLogLog is not planned (Postgres extensions do it better over the
same data); geo is declined outright, because PostGIS is one `CREATE EXTENSION`
away and operates on the same rows. `CLIENT TRACKING` supports every mode (default, `BCAST` with
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

`PUBSUB CHANNELS`/`NUMSUB`/`NUMPAT` answer for the **whole instance**, where
Redis Cluster answers per node. That is deliberate, and it follows from what
this is: a cache inside Postgres that speaks Valkey, not a Redis replica. A
Redis Cluster node is a peer the client chose and can address; a pg_keyspace
worker is an implementation detail of one cache, and which worker a connection
landed on is not something the client picked, can see, or could act on. An
answer scoped to it would describe our internals rather than the keyspace the
client believes it is talking to. Every worker therefore gives the same answer,
read from the shared routing table, and `bench/run_pubsub_introspect.sh`
asserts it by subscribing on workers 1 and 2 and asking worker 0.

Where the same reasoning does not apply, parity wins: the replies, the ordering
rules and the error text are matched verbatim against a real Redis, including
which of three different messages an invalid subcommand produces. Tenant-scoped
connections see only their own namespace, unprefixed — enumeration is precisely
the leak that scoping exists to stop, since a tenant able to list another's
channels learns what it is doing without receiving a message.

`PUBSUB SHARDCHANNELS`/`SHARDNUMSUB` are **not** implemented, because sharded
pub/sub (`SSUBSCRIBE`/`SPUBLISH`) is not. `PUBSUB HELP` lists only what this
build actually serves, rather than copying Redis's help text and advertising
subcommands that would answer "unknown command".

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

**The row cache serves every database that registers a table.** Register from
wherever the table is; there is nothing else to configure.

It did not always. Until [#120](https://github.com/supatype/postgres/issues/120)
it served exactly one database — the one named by `pg_keyspace.database` — and
`rowcache_register` refused from anywhere else. That refusal was right at the
time, and the reason is worth keeping in view because it still shapes the
design. It was never the catalogue and never the keys: it was **invalidation**. A
logical replication slot belongs to the database it was created in and only ever
decodes changes from that database, so with one slot in one database, a table
registered anywhere else would be cached and then *never invalidated* — stale
indefinitely, while `rowcache_coherence()` still reported healthy, because
coherence described the worker rather than your table.

So the fix was not to relax the check. Every database with registrations gets
**its own replication slot**, and a **bounded pool** of invalidation workers
drains them:

```ini
pg_keyspace.rowcache_invalidation_workers = 1   # the pool size — YOU set this
pg_keyspace.rowcache_lease_ms = 5000            # a turn, when databases outnumber workers
pg_keyspace.rowcache_max_databases = 32         # directory size; 128 bytes each
```

The pool is bounded by that setting and **not** by how many databases exist —
the same trade the autovacuum launcher makes, and the reason this is a pool
rather than a worker per database: otherwise a cluster's process count becomes a
function of its tenant count, which is the thing per-project-database
provisioning can least afford. Databases are picked up **lazily**: one with no
registrations gets no slot, no worker and no turn, so a cluster with fifty
databases and two that cache pays for two.

`supacache.pg_stat_keyspace_rowcache_databases` is where this is visible — one
row per database, with its state, its slot, how long since it was last
invalidated, and the window it is judged against.

##### What it costs, per database that registers a table

| | cost |
|---|---|
| replication slots | 1 |
| worker processes | 0 — the pool is shared and bounded |
| WAL retained | bounded by `max_slot_wal_keep_size`, once you set it |

`max_replication_slots` (default 10) has to cover one slot per participating
database **plus** whatever replication the cluster already does.
`max_worker_processes` (default 8) has to cover the pool **plus** the RESP,
persistence and expiry workers. A database that cannot get a slot is marked
**incoherent** rather than served — it fails closed, like every other way of not
being invalidated.

##### The hazard, and why `max_slot_wal_keep_size` is not optional

**A replication slot retains WAL until it is consumed.** With one slot the risk
was singular and visible. One per database means **any single database's stalled
invalidation pins WAL for the whole cluster** — one slow tenant can fill the WAL
volume for everyone.

Postgres already solves this and the solution is off by default. Set
`max_slot_wal_keep_size`, and a slot reserving more than that is invalidated by
the server instead of being allowed to pin WAL indefinitely. pg_keyspace logs a
warning at startup when it is unset, and reports it as a column on
`pg_stat_keyspace_invalidation`.

**Size the bound against the decode interval, not just against the disk.** It
bounds the *cluster*, not one database against another. A healthy decoder still
retains up to one `rowcache_decode_ms` worth of WAL between advances, and that
WAL is whatever the whole cluster generated — so a bound tight enough to catch a
stalled database can also cut loose a perfectly healthy one that simply had a
busy neighbour. Observed while building
`bench/run_rowcache_slot_invalidation.sh`: at `max_slot_wal_keep_size = 32MB`, a
flood in one database invalidated the *other* database's slot, which was doing
nothing wrong. Nothing was served stale — the recovery below is the same either
way — but that database lost its cache and had to rebuild it. Leave room for
`decode interval × peak cluster WAL rate` above whatever a stall would reserve.

When the server does invalidate a slot, that is a **gap**: the changes it had not
yet delivered are gone, and there is no way to tell which rows they were. So
pg_keyspace marks that database incoherent (its reads fall back to the heap
immediately), **drops its cached rows**, rebuilds the slot, and only then serves
it again. Resuming over the gap would mean serving a stale row as truth at the
exact moment everything reported healthy — the worst failure this system can
produce. Its registrations are kept: they are configuration, not cache content,
and dropping them would silently stop caching the tables you asked for on top of
the outage. Asserted end to end by
`bench/run_rowcache_slot_invalidation.sh`.

##### Worth comparing against: one cluster per project

One database per project is what this work makes possible. One **cluster** per
project maps onto the original design with no code at all, no extra slots and no
shared WAL hazard. The cost is density — a postmaster each. If you are choosing
between them, that is the trade; this section exists so the comparison is a fair
one rather than an implicit vote for the thing that was built.

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

##### Who is allowed to write the row-cache segment

The RESP keyspace segments each have exactly one writer: the worker that owns
them. The row-cache segment has none. Its writers are ordinary **backends** —
`rowcache_put`, registration, and above all read-through, which makes a writer
of every backend that misses.

`Store`'s API is single-writer-per-partition by contract, so two of those at
once corrupted the arena and segfaulted a backend, which Postgres answers by
killing every other backend and crash-restarting the cluster. It took four
pgbench clients under a second ([#127](https://github.com/supatype/postgres/issues/127)).

Writers now take an exclusive LWLock; **readers take nothing**. That is not a
half-measure — one writer against many readers is the pattern the store is
already built for, and the seqlock in `get_stable` is what makes it safe. The
read path, which is the hot one, costs the same as before. The row cache was
also the last reader that borrowed shared bytes rather than reading through the
seqlock, so a concurrent rewrite could splice two tuples together; it now reads
the way every SQL read of the RESP keyspace already did.

**One lock, though, is one lock for the whole cluster.** Every mutation of the
store is confined to a single partition — the probe, the slab allocator, and
above all `evict_one`, which moves the bucket array, the entry array and the
bump pointer — so the lock follows the segment's own partitioning rather than
sitting in front of all of it. `pg_keyspace.rowcache_partitions` (default 8)
sets both, and writers in different partitions never wait on each other.

That matters more than it used to. The row cache serves every database
([#120](https://github.com/supatype/postgres/issues/120)), so a single lock
would be a cluster-wide serialisation point for row-cache writes: one database
with a cold cache and heavy read-through would stall caching for every other
one. Partitions **divide** `rowcache_mb` rather than multiplying it — the
setting has always meant the size of the whole segment — so raising the count
never quietly grows the shared-memory request.

The correctness risk this introduces is specific and worth naming: a lock chosen
for one partition while the write lands in another is the #127 corruption again,
with a lock in front of it saying it cannot happen. `Store::partition_of` is
therefore a thin wrapper over the same two lines `get`/`set` route through
rather than a second implementation, `rowcache_write` takes its partition from
the same view the write goes through, and `bench/run_rowcache_lock_contention.sh`
asserts that rows still read back well-formed at both 1 and 8 partitions after a
concurrent read-through race. It prints the contention numbers too, as
measurement rather than as a gate.

The lock lives in the extension rather than in `core/`, because `core/` is
shared with the standalone daemon, where every segment does have exactly one
writing process and none of this applies.

Asserted by `bench/run_rowcache_concurrency.sh`, which also covers
[#128](https://github.com/supatype/postgres/issues/128): a query with **no
`WHERE` clause** against a registered table dereferenced a null
`baserestrictinfo` at plan time. An empty Postgres `List` is a null pointer, and
`SELECT count(*) FROM t` is exactly that shape — so a registered table went down
on the first unfiltered query against it. Every row-cache harness queried by
primary key, which is the case that worked.

#### What the row cache guarantees, and what it does not

The invalidation worker polls the replication slot, so coherence is **eventual
with a bound, not read-your-writes**:

> A row served from the cache reflects a state committed at or before the read,
> and no more than `pg_keyspace.rowcache_decode_ms` (default **200 ms**) plus
> decode time behind the current committed state.

**That bound holds per database while the pool is big enough.** With no more
participating databases than `pg_keyspace.rowcache_invalidation_workers`, every
database has a worker to itself, nothing waits for a turn, and the window above
is exactly what it always was.

Above that, invalidation **cycles**: a worker leases a database, drains it, and
hands the slot on after `pg_keyspace.rowcache_lease_ms`. The window is then the
**cycle time**, not the decode interval:

> ⌈participating databases ÷ pool size⌉ × (lease + relaunch)

So it grows linearly with the number of databases you cache and shrinks
linearly with the pool size. With the defaults — a pool of 1 and a 5 s lease —
three participating databases put each of them roughly 18 s behind rather than
200 ms. That is a knob, not a wall: raise the pool (bounded by
`max_worker_processes`) and the window comes back down.

Two things stop this being a silent degradation. The window is **reported**, per
database, as `stale_after_ms` on
`supacache.pg_stat_keyspace_rowcache_databases` — you do not have to derive it.
And the cache **fails closed** against that same window, per database: a
database that has not been drained within it stops being served from the cache
and reads the heap instead. A cycle that cannot keep up costs you the cache, not
your correctness.

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

##### Removing it from a database

`supacache.rowcache_reg` is created the first time you register a table in a
database, and it is **not** an extension member — like `supacache.kv` and
`supacache.acl`, it holds configuration you entered, so `DROP EXTENSION` must
not quietly take it with them. The consequence is that a database with
registrations needs

```sql
SELECT supacache.rowcache_unregister('public.orders');  -- or: DROP TABLE supacache.rowcache_reg;
DROP EXTENSION pg_keyspace;
```

rather than `DROP EXTENSION` alone (`CASCADE` also works and takes the
registrations with it). That was always true of `pg_keyspace.database`; since
[#120](https://github.com/supatype/postgres/issues/120) it is true of any
database you register a table in.

##### The slot is named for the database, not for the configuration

`pg_keyspace.rowcache_slot` is the **stem** of the slot name; the slot itself is
`<stem>_<database oid>`, e.g. `supacache_rowcache_16384`. A logical slot belongs
to the database it was created in and only ever decodes changes from that
database, so the slot has never been able to mean anything else, and naming it
for the database is what lets there be more than one
([#120](https://github.com/supatype/postgres/issues/120)).

The oid rather than the name, because a slot name is capped at 63 characters and
a database name can fill that on its own — a name-derived slot would have to be
truncated, and two long database names sharing a prefix would truncate to the
**same** slot. Two databases sharing one slot is the #117 cross-database failure
one layer down, with the invalidations crossing instead of the rows.

**Upgrading:** a cluster that ran an earlier version has a slot named for the
configuration verbatim. Nothing consumes it after the upgrade, and an unconsumed
slot pins WAL from its `restart_lsn` forever, so the worker drops it on first
start and logs that it did. Nothing is lost — the row cache is empty after the
restart an upgrade requires, and the new slot is correct from its first pass. If
the drop fails (it is refused if the slot is somehow still active) the log says
so and names the `pg_drop_replication_slot` call to run by hand.

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

#### Publishing from SQL as a tenant

`supacache.publish(channel text, message bytea) -> bigint` reaches RESP
subscribers from a backend — a trigger can tell a RESP client something without
going out through the application and back in over the wire. It returns the
same receiver count a RESP `PUBLISH` answers with.

A RESP connection's channel names are force-scoped `{tenant}:` by its
credential. A Postgres role is not a credential, so a SQL caller's tenant comes
from `pg_keyspace.tenant`:

| caller | `pg_keyspace.tenant` | result |
|---|---|---|
| superuser | anything | publishes the name as given, unscoped |
| anyone else | set by the operator | publishes to `{tenant}:{channel}` |
| anyone else | set in this session | refused — self-asserted |
| anyone else | unset | refused |

Set it in `postgresql.conf` for a single-tenant cluster, or per role for a
multi-tenant one:

```sql
ALTER ROLE tenant_a SET pg_keyspace.tenant = 'acme';
```

The setting is `SUSET`, so a role can neither set nor `RESET` it for itself,
and the per-role value lives in Postgres's own `pg_db_role_setting` rather than
in a table this extension would have to define, dump and upgrade. Because PG15+
`GRANT SET ON PARAMETER` can hand over the right to `SET` it, the value's
source is checked too: one the caller put there in its own session is refused
rather than believed. A subscriber authenticated as `acme` that subscribed to
`demo` receives the message under the name `demo`, exactly as it would from a
RESP `PUBLISH` — asserted end to end by `bench/run_sql_publish.sh`.


#### Cross-instance pub/sub

A `PUBLISH` reaches every subscriber on **this** instance. It does not reach
another instance unless you say so, and this is opt-in per channel pattern:

```conf
pg_keyspace.relay_channels = 'inval:*'   # SIGHUP; empty (the default) = off
```

```sql
INSERT INTO supacache.peer(name, conninfo)
VALUES ('eu-west', 'host=db2.internal port=5432 user=relay dbname=postgres');
```

With at least one pattern set, the extension runs one extra background worker
that subscribes to those patterns on its own RESP port and calls
`supacache.publish_relayed()` on every enabled row of `supacache.peer`, over
**dblink** — so authentication, TLS and `pg_hba` are Postgres's, not something
this extension invented. `CREATE EXTENSION dblink` is required in the relaying
database; a missing one is reported by name in the server log.

The worker is a RESP *client*, not a participant in the shared-memory pub/sub
bus. Giving it its own inbox would have meant sizing the bus for `nworkers+1`
— quadratic, and paid by every deployment whether it relays or not. As a
subscriber it needs no shared memory at all, and relaying off costs nothing:
with no patterns set, the worker is never registered.

What the relay is, and is not:

| | |
|---|---|
| delivery | **at-most-once**, like valkey. A message published while a peer is unreachable is lost |
| catch-up | none. There is no replay and no backlog; the link recovers for what comes *next* |
| loops | structurally impossible: `publish_relayed()` delivers locally and never relays onward, so `A → B → A` cannot form |
| scope | only the patterns in `relay_channels`; everything else stays local |
| tenancy | the channel crosses already tenant-scoped, and `pg_keyspace.relay_user` bounds what the relay may forward — a tenant-scoped credential relays only that tenant's channels |
| a dead peer | costs that peer's messages only. It cannot wedge the RESP port (the fan-out runs in the relay's own process), cannot kill the worker (each peer is tried inside its own exception block), and cannot flood the log (the warning is rate-limited to one a minute) |
| turning it on | a restart, because a worker has to be registered. Changing the patterns afterwards is a `SIGHUP` |

This is a **cache** that speaks Valkey, not a replica: the relay propagates
invalidations between instances, it does not make them one keyspace. A client
that cannot miss an invalidation should ping its invalidation channel and flush
on breakage — which is what it would do against a real valkey, and is unchanged
by whether a relay exists.

`bench/run_relay.sh` asserts all of the above against two independent
postmasters, including that nothing is replayed to a peer that was down.


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
### Monitoring (`pg_stat_keyspace*`)

Everything an operator needs to watch is a **view in the `supacache` schema**,
shaped like Postgres's own `pg_stat_*`. There is no exporter to deploy, no
sidecar, no dashboard to import. postgres_exporter, pgwatch, Datadog, pganalyze
and a `SELECT` in cron all collect from views like these already, and all of
them are already pointed at a role that is a member of `pg_monitor` — which is
what `CREATE EXTENSION` grants these to. **The whole of the setup is installing
the extension.**

That is the point of doing it this way rather than the Redis-shaped way. A
standalone keyspace has to ship its own exporter, because there is no ecosystem
to borrow. Living inside Postgres means there is.

| view | grain | what it answers |
|---|---|---|
| `pg_stat_keyspace` | 1 row | the top line: entries, hits/misses, `hit_pct`, arena used/capacity |
| `pg_stat_keyspace_workers` | (worker, partition) | per-worker counters, `arena_used_pct`, and the slot range + port that worker serves |
| `pg_stat_keyspace_activity` | background worker | heartbeat age and pid per RESP/persist/expiry/invalidation worker |
| `pg_stat_keyspace_persist` | (worker, shard) | per-ring `pushed`/`committed`/`lag`/`backlog_bytes`/`dropped`/`uncommitted_batches` |
| `pg_stat_keyspace_persist_total` | 1 row | the above summed, plus `worst_ring_backlog_bytes` |
| `pg_stat_keyspace_tenants` | tenant | measured arena bytes and entries per tenant |
| `pg_stat_keyspace_rowcache` | 1 row | row-cache occupancy (segment-wide), plus `coherent` for the database you are connected to |
| `pg_stat_keyspace_rowcache_databases` | 1/database | per-database `state`, `coherent`, `slot_lost`, `beat_age_ms` and the `stale_after_ms` it is judged against |
| `pg_stat_keyspace_invalidation` | 1/database | `decode_lag_bytes`, `retained_bytes`, `wal_status` and `max_slot_wal_keep_size`, per decode slot |
| `pg_stat_keyspace_pubsub` | 1 row | messages **not** delivered: `dropped`, `route_full`, `name_too_long` |
| `pg_stat_keyspace_topology` | 1 row | recorded vs running worker count, and what a change between them costs |

Three conventions, each chosen because a collector depends on it:

**Counters are cumulative; gauges are instantaneous; the two never share a
column.** `hits`, `misses`, `sets`, `evictions`, `tombstones`, `rehashes`,
`pushed`, `dropped`, `committed`, `unresolved` and the pub/sub columns count
since the segment or process started and are *not* reset by reading, so
`rate()` over two scrapes means something. `entries`, `arena_*`,
`backlog_bytes`, `lag`, `beat_age_ms`, `decode_lag_bytes`, `retained_bytes` and
`uncommitted_batches` are gauges.

`uncommitted_batches` deserves its own note, because the underlying counter's
legacy name (`ring_stats().failed_batches`) says something it does not mean. The
ring increments it *before* attempting a batch and decrements it after the
commit succeeds — bracketing the attempt, because a Postgres `ERROR` unwinds out
of the worker and an `Err` branch never runs. So **a batch in flight reads as
one outstanding**, and under sustained writes this sits at a small number and
oscillates. Measured on a soak: it moved between 2 and 4 across 4 rings the
whole run, with nothing wrong.

What it really reports is an increment that was never cancelled — a batch that
failed, or whose worker died mid-commit. **The signal is a floor that does not
drain**: when writes stop, it should return to zero, and whatever is left never
committed. An alert on "nonzero" is an alert on ordinary traffic.

**A switched-off feature returns NO ROWS, not a row of zeroes.** With
`rowcache_decode = off`, `pg_stat_keyspace_invalidation` is empty.
`decode_lag_bytes = 0` means the decoder is current; *no row* means there is no
decoder. An alert that cannot tell those apart reads "invalidation has been off
in production for a week" as perfect health.

**Per-shard, not only summed.** `ring_stats()` adds every persistence ring
together, and persistence falls behind *per shard* — one ring at its drop
threshold disappears into a healthy-looking total.
`pg_stat_keyspace_persist` has a row each, and
`pg_stat_keyspace_persist_total.worst_ring_backlog_bytes` carries the maximum
into the rollup for anyone who only scrapes the one row.

Three things worth alerting on, in order of how quietly they fail:

| condition | means |
|---|---|
| `pg_stat_keyspace_rowcache.coherent = false` | invalidation is configured but not current **for this database**. Reads fail closed, so this is an availability signal, not a correctness one |
| `pg_stat_keyspace_rowcache.incoherent_databases > 0` | some database is not being invalidated. `pg_stat_keyspace_rowcache_databases` says which, and whether it is a stall or a `slot_lost` |
| `pg_stat_keyspace_invalidation.wal_status = 'lost'` | the server cut that database's slot loose for exceeding `max_slot_wal_keep_size`. pg_keyspace purges and rebuilds; a slot that keeps being lost means the bound is too small for the write rate |
| `pg_stat_keyspace_persist.lag` climbing and not returning | persistence is falling behind; `dropped > 0` next means acknowledged writes are being discarded |
| `pg_stat_keyspace_activity.alive = false`, flapping | a worker is crash-looping. The watchdog relaunches it every time, so from outside it looks like a worker that is running |

`decode_lag_bytes` is never zero on a busy cluster — most WAL is not row-cache
traffic and the slot advances one drain window at a time — so alert on its
trend. `retained_bytes` is the one with a disk-space consequence: a stopped
decoder pins WAL until `max_slot_wal_keep_size` cuts the slot loose.

Under the views are ordinary functions (`supacache.worker_stats()`,
`persist_shard_stats()`, `tenant_stats()`, `worker_health()`,
`invalidation_stats()`, and the pre-existing `stats()`, `ring_stats()`,
`rowcache_stats()`, `rowcache_coherence()`, `topology_change()`). They stay
callable, but **the views are the supported surface** — the function shapes are
free to change.

Two details that cost a debugging session each, both asserted in
`bench/run_pg_stat_views.sh` rather than assumed:

- A view's *table* references are checked against the view owner, but a
  **set-returning function in its `FROM` clause is checked against the caller**.
  Granting `SELECT` on the views alone gets `permission denied for function
  worker_stats`. `EXECUTE` is therefore granted to `pg_monitor` explicitly,
  which also means an operator hardening the install with `REVOKE … FROM PUBLIC`
  does not break monitoring.
- `supacache.topology` and `supacache.rowcache_reg` are created by the
  background worker, not by `CREATE EXTENSION`, so a scrape can arrive before
  they exist. The functions check with `to_regclass` **in a separate statement**,
  because Postgres resolves relations when it parses: a `CASE` with the table
  named in the unreachable branch still errors. An erroring stats view reads to
  a collector as the database being down.

Using a role other than `pg_monitor`:

```sql
CREATE ROLE metrics LOGIN;
GRANT pg_monitor TO metrics;   -- that is the whole of it
```

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
| `pg_keyspace.database` | `postgres` | database holding `supacache.kv` backing tables (Mode A), and the one worker 0 auto-upgrades. Since #120 it does **not** bound the row cache: Mode B serves every database that registers a table |
| `pg_keyspace.persist_workers` | 1 | persist workers/rings draining in parallel |
| `pg_keyspace.ring_mb` | 64 | per-worker RESP→persist ring size (burst absorption) |
| `pg_keyspace.ttl_bucket_secs` | 10 | TTL time-bucket width (range-partitioned `supacache.kv_ttl`) |
| `pg_keyspace.require_mask` | `off` | `off` (default) runs standalone; `on` fails closed unless `supatype_mask` is loaded + outermost — set by the Supatype platform |
| `pg_keyspace.tls_cert_file` / `tls_key_file` | *(empty)* | PEM cert + key → serve RESP over TLS |
| `pg_keyspace.tls_use_postgres_cert` | `off` | with those unset, serve RESP with the cluster's `ssl_cert_file`/`ssl_key_file` (needs `ssl = on`) |
| `pg_keyspace.rowcache_mb` | 64 | Mode B row-cache segment size (never RESP-addressable) |
| `pg_keyspace.rowcache_partitions` | 8 | partitions the row-cache segment is carved into, and writer locks it has; divides `rowcache_mb`, does not multiply it |
| `pg_keyspace.rowcache_invalidation_workers` | 1 | bounded pool draining the per-database decode slots; below the number of participating databases, invalidation cycles |
| `pg_keyspace.rowcache_lease_ms` | 5000 | how long one pooled worker holds a database before handing it on (only bites when cycling) |
| `pg_keyspace.rowcache_max_databases` | 32 | databases the row cache can serve at once; registration is refused rather than uninvalidated beyond it |
| `pg_keyspace.rowcache_decode` | `off` | keys-only Mode B invalidation worker (needs `wal_level=logical`) |
| `pg_keyspace.rowcache_refill` | `off` | on: re-cache a changed hot key; off: drop-only (lazy) |
| `pg_keyspace.rowcache_readthrough` | `off` | on: a pk lookup that misses caches the row it read (ignored unless `rowcache_decode` is on) |
| `pg_keyspace.tenant_ring_share` | `on` | give each tenant a share of the persistence ring instead of first come, first served |
| `pg_keyspace.tenant_scoped_eviction` | `on` | evict a tenant's own cold keys before another tenant's |
| `pg_keyspace.tenant_arena_pct` | 0 | cap one tenant at this % of a partition's entries (0 = off); over-budget tenants are evicted from first |
| `pg_keyspace.tenant_ops_per_sec` | 0 | commands per second one tenant may issue (0 = no limit) |
| `pg_keyspace.tenant` | *(unset)* | tenant a SQL backend publishes as, scoping `supacache.publish()` to `{tenant}:`; unset leaves it superuser-only |
| `pg_keyspace.pubsub_buffer_mb` | 32 | per-connection output buffer a subscriber may fall behind by before it is disconnected (0 = unbounded) |
| `pg_keyspace.relay_channels` | *(empty)* | channel globs relayed to the peers in `supacache.peer`; empty means cross-instance pub/sub is off and no relay worker runs |
| `pg_keyspace.relay_user` | *(empty)* | RESP username the relay subscribes with; bounds what it may forward |
| `pg_keyspace.relay_secret` | *(empty)* | secret for `pg_keyspace.relay_user` (superuser-visible only) |

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
`run_nonint_pk.sh`, `run_toast.sh`, `run_invalidation.sh`), the row cache across
databases — per-database slots, bounded cycling and no cross-database leak
(`run_rowcache_database_scope.sh`, `run_rowcache_multidb.sh`), what happens when
a slot is invalidated (`run_rowcache_slot_invalidation.sh`) and the races a pool
creates (`run_rowcache_registration_race.sh`,
`run_rowcache_lock_contention.sh`), tenant-scoped pub/sub
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

`run_pg_stat_views.sh` covers the monitoring surface the same way: a live
cluster, a real `pg_monitor` member scraping every view, and the negative
control that a role *without* `pg_monitor` is refused all ten. It also asserts
the views are extension members (so `pg_dump` and `DROP EXTENSION` handle them),
that counters do not reset on read, and that switching a feature off empties its
view rather than zeroing it.

`run_rowcache_concurrency.sh` is a crash test, so its assertions are the absence
of `signal 11` in the server log rather than a query result. It covers the two
row-cache segfaults (#127, #128) and is sized so an unfixed build fails within
seconds: without the writer lock it dies at around 1,400 transactions, with it
the same run does over 600,000.

`run_soak.sh` is the sustained, multi-tenant, fault-injected soak
([#113](https://github.com/supatype/postgres/issues/113)). It runs a
checksum-verifying RESP workload (`bench/k6/mixed.js`) and a pgbench row-cache
workload together, injects faults on a schedule, samples the `pg_stat_keyspace*`
views throughout, and then **judges the series** — RSS, live entries, eviction
rate, persist lag, WAL decode lag — rather than reporting a headline rate.
`bench/soak/drift.sh` does the sampling and the judging and runs standalone, so
the generator can live on a different machine from the server. See
`bench/k6/README.md`.

What it is **not** is an answer to #113. That is gated on hardware: with the
generator on the same box as the server, every throughput comparison is
meaningless, and the numbers above are from a 4-vCPU container. What it does
establish on any hardware is correctness under concurrency, drift, and recovery
under load.

Three conventions it enforces, each learned by getting them wrong first:

- **A miss is not an error, and neither is an `OOM` refusal.** The cache is
  entitled to have evicted anything, and a server correctly refusing a write it
  has no room for is doing its job. Lumping either into an error count buries a
  real failure under hundreds of legitimate ones — the first version reported
  1.4M "errors" on a healthy run.
- **A run with fault injection cannot demand zero errors**, because killing a
  worker drops the connections it was holding. The budget is derived from the
  fault schedule; with `NO_FAULTS=1` it is zero, and that control run is the one
  that has to come back clean. It is also the run that found
  [#130](https://github.com/supatype/postgres/issues/130).
- **Judge queues on medians and drained tails, trends on halves.** Persist lag
  and decode lag are sawtooths that a deliberate fault spikes; averaging halves
  let one 34 MB sample report a decoder that was 0.2s behind as "falling
  behind".

`run_ttl_partition_race.sh` puts four persistence workers under TTL write load
with a two-second bucket, so every rollover is contested. `CREATE TABLE IF NOT
EXISTS` does not settle that race — two sessions can both pass the existence
check — and the loser used to die with `duplicate_table`, taking its shard out
of service until the watchdog returned it (#130). With the fix reverted the test
records 44 worker deaths in a minute; with it, none.

`run_extension_upgrade.sh` builds a genuine 0.1.0 install from the archived
release schema, runs `ALTER EXTENSION pg_keyspace UPDATE`, and requires the
result to be **identical** to a fresh `CREATE EXTENSION` — signatures, argument
names, ACLs, comments and view definitions, all 48 objects. That equivalence is
what keeps the upgrade scripts from drifting away from
`lib.rs`: delete one `COMMENT ON VIEW` from the upgrade script and it fails on
that comment; delete the `pg_monitor` grant and it fails on ten ACLs and on the
`pg_monitor` read. It also records the pre-upgrade symptoms, including the quiet
one — see below.

`run_extension_autoupgrade.sh` covers the other half: that nobody has to run the
command. It creates a genuine 0.1.0 install, restarts, and asserts the catalogue
reached 0.3.0 with no `ALTER EXTENSION` anywhere in the test — then that a second
restart is a silent no-op, that `pg_keyspace.auto_upgrade = off` leaves the
version where it is while still reporting the skew, and that turning it back on
repairs the same cluster. Section 6 is the one that earns its place: it removes
the **last hop** of the chain, so the update can start and *cannot* finish, and requires the worker to
log one warning, start exactly once, and go on serving its segment — a worker
that died there would crash-loop, which is #130's failure mode, not a new one.

---

## Backup, restore and upgrade

`supacache.kv` and the `supacache.kv_ttl` partitions are ordinary tables, so
most of this falls out for free: `pg_dump` includes them, physical backups and
PITR include them, TTLs survive (they are a column, not runtime state), and a
restored cluster rebuilds the cache on the next worker start through the same
path as crash recovery.

### Your existing Postgres backups already cover the cache

This is worth saying out loud, because no other cache offers it.

The keyspace lives in the WAL, inside the same cluster as the data it fronts, so
`pg_basebackup` plus WAL archiving backs the cache up with everything else — and
a point-in-time restore brings it back **to an instant consistent with the rows
it caches**. One backup, one restore procedure, one timeline. Valkey cannot do
this at any price: RDB and AOF are a separate artefact on a separate schedule
with a separate restore, and nothing makes the cache and the database agree
about when "now" was.

`bench/run_pitr_restore.sh` asserts that rather than assuming it. Restoring a
base backup to a timestamp taken mid-traffic, against a durable-tier cluster:

| asserted | |
|---|---|
| every write acknowledged before the target is back | ✓ |
| nothing written after it is | ✓ |
| the table the cache fronts stopped at the same instant | ✓ |
| the worker rebuilt the keyspace from the restored tables | ✓ |
| a live TTL came back with its remaining expiry | ✓ |
| an already-expired key was not resurrected | ✓ |

The boundary is exact because the durable tier holds the RESP reply until the
record commits: a `+OK` is a promise that the row is in `supacache.kv`, so PITR
has to keep every promise made before the target and none made after it. It
does, and the recovering worker says how many keys it rebuilt:

```
LOG:  starting point-in-time recovery to 2026-09-16 09:19:44.246393+00
LOG:  recovery stopping before commit of transaction 1001
LOG:  pg_keyspace worker 0: recovered 51 keys (slots 0..16384) from supacache.kv
```

**One thing to watch when restoring.** `pg_keyspace.workers` comes from the
restored cluster's `postgresql.conf`, which is usually edited by hand — so a
restore is a plausible place to change the worker count by accident. A persisted
tier with more than one worker redirects clients by address and so also needs
`pg_keyspace.cluster_announce_host`. Without it every worker refuses to start
and names the setting, rather than coming up and serving an empty keyspace that
would look exactly like a restore that lost everything:

```
LOG:  pg_keyspace worker 0: REFUSING to start — a persisted tier with
      pg_keyspace.workers > 1 redirects clients by address, so
      pg_keyspace.cluster_announce_host must be set ...
```

Set it and the restore proceeds. The keyspace is re-sharded across the new
worker count and says so; nothing is lost, because `supacache.kv.slot` is stable
and a key simply recovers into a different segment:

```
LOG:  pg_keyspace worker 0: WORKER COUNT CHANGED 1 -> 3. 10923 of 16384 slots
      (66.7%) now belong to a different worker ...
```

The consequence worth knowing: **a dump of a busy cache silently contains the
whole cache**, which can surprise on both dump size and data retention. If the
cache is disposable, exclude it:

```bash
pg_dump --exclude-schema=supacache ...
```

#### How the extension catalogue evolves

Two independent things change across a release, and only one of them looks after
itself. The persisted tables converge on the running binary at every worker
start (below). The extension's **catalogue** — its functions and views — does
not: Postgres runs an extension's SQL exactly once, at `CREATE EXTENSION`. A
cluster that takes a newer image keeps whatever catalogue it had when the
extension was first created, so fixes that live in the shared library arrive on
their own and SQL objects never do.

**In the normal case you do not have to do anything.** At start-up worker 0
compares the installed extension against the library's `default_version` and, if
they differ, runs the update itself:

```
LOG:  pg_keyspace worker: upgraded the extension catalogue 0.1.0 -> 0.3.0
```

So taking a newer `pg_keyspace.so` and restarting is the whole procedure. This
lives in the extension rather than in any image's bootstrap on purpose:
pg_keyspace runs standalone on plain Postgres, an AMI, bare metal or someone
else's container, and a fix wired into one project's init scripts would reach
none of them.

Three things bound it, all deliberate:

- **It applies to the database named by `pg_keyspace.database`.** A background
  worker connects to one database. Any *other* database holding the extension is
  still yours to update by hand — and since #120 that is no longer a corner case.
  Using the row cache in a database means creating the extension there, so a
  cluster with ten tenant databases has ten catalogues, of which worker 0
  upgrades one. `pg_extension` is per-database, so no single query finds the
  rest: list the candidates and run the update in each.

  ```bash
  for d in $(psql -At -c "SELECT datname FROM pg_database
                           WHERE datallowconn AND NOT datistemplate"); do
    psql -d "$d" -c 'ALTER EXTENSION pg_keyspace UPDATE' 2>/dev/null
  done
  ```

  It is a no-op where the version already matches, and errors harmlessly where
  the extension is not installed.

  A stale catalogue in a tenant database fails the way described above: missing
  objects announce themselves, a stale *signature* does not.
- **It never runs on a standby.** A replica's catalogue is replayed from the
  primary, so the check reports the skew and leaves it alone; upgrade the primary.
- **It cannot take the worker down.** `ALTER EXTENSION` raises for reasons that
  are not emergencies — no update path between two versions, an upgrade script
  missing from the install — and a worker that died on one would take the
  keyspace out of service on a relaunch loop. The statement runs inside a
  `DO ... EXCEPTION` block, so a failure is a `WARNING` in the log and start-up
  continues on the old catalogue.

Set `pg_keyspace.auto_upgrade = off` to keep the catalogue under your own
control. The version check still runs and still reports a skew — it just tells
you the command instead of running it:

```sql
ALTER EXTENSION pg_keyspace UPDATE;
SELECT extversion FROM pg_extension WHERE extname = 'pg_keyspace';  -- 0.3.0
```

The path is a **chain** — `0.1.0 -> 0.2.0 -> 0.3.0` — and Postgres walks it on
its own from a single `ALTER EXTENSION`. `0.1.0 -> 0.2.0` adds seventeen
functions and ten `pg_stat_keyspace*` views, and replaces `ring_stats()`, which
went from three columns to seven. `0.2.0 -> 0.3.0` is #120: `invalidation_stats()`
and `rowcache_coherence()` change their **return type**, which `CREATE OR REPLACE`
cannot do, so both are dropped and recreated along with the two views that select
from them; `rowcache_databases()` and
`supacache.pg_stat_keyspace_rowcache_databases` are new, making eleven views.

That last one is worth knowing because of how it fails without the update.
Missing objects announce themselves — `supacache.pg_stat_keyspace` simply does
not exist. A *stale* entry does not: the 0.1.0 catalogue still describes
`ring_stats()` as three columns, so calling it against a newer library raises
nothing and returns `pushed`, `dropped`, `backlog_bytes` exactly as before.
`committed`, `lag`, `failed_batches` and `unresolved` are not absent so much as
invisible. Nothing in the logs marks the difference.

Version `0.1.0` is what the v17.2.4 and v17.2.5 images shipped, and both shipped
the same catalogue — their generated schemas differ only in pgrx's deliberately
unstable statement ordering — so one script covers either.

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
- **Pub/sub crosses instances only where you opt in, and at most once.** Within
  one instance it crosses processes by itself: the routing table and the
  per-worker inboxes live in Postgres shared memory, so a `SUBSCRIBE` on one
  worker and a `PUBLISH` on another meet even though the workers are separate
  processes — asserted in CI by section Q of `bench/run_durability_pg.sh`, which
  publishes on worker 0 and receives on worker N-1 — and a SQL backend reaches
  the same subscribers through `supacache.publish()`. Across *instances*,
  `pg_keyspace.relay_channels` plus rows in `supacache.peer` forward the
  patterns you name to the peers you name (see
  [Cross-instance pub/sub](#cross-instance-pubsub)). What that is not is a
  replicated keyspace: delivery is at-most-once and there is no catch-up, so a
  message published while a peer is unreachable is lost, exactly as in valkey.
  Channels outside `relay_channels` never leave the instance, and the failure
  there is silent — the subscriber simply never receives a message.
  `LISTEN`/`NOTIFY` is no substitute: it cannot even be registered on a standby,
  so it does not reach the failover case.
- **A standby serves no RESP.** Every pg_keyspace background worker uses SPI, so
  Postgres registers it with `BgWorkerStartTime::RecoveryFinished` and does not
  launch it until recovery ends — which on a streaming standby never happens.
  The RESP port on a replica does not answer, and the keyspace is reachable
  there only through SQL over `supacache.kv`. The cluster says so at startup,
  and the deferral lifts by itself: promote the standby and the workers start
  and serve the keyspace they inherited (`bench/run_standby_notice.sh`).
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
- Operational metrics are exposed as `pg_stat_*`-shaped views granted to
  `pg_monitor` — see [Monitoring](#monitoring-pg_stat_keyspace). Row-cache
  invalidation lag, per-ring persistence depth, per-tenant arena occupancy and
  per-worker heartbeats are all covered. What is *not* there is a historical
  store: these are instantaneous reads, and retention is whatever your collector
  keeps.
