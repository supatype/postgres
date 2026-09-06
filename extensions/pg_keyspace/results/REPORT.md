# pg_keyspace — P0 spike report (+ P1 storage/durability in progress)

**Status:** P0 kill criterion **PASSED**. A RESP GET hit served by a Postgres
background worker over a Postgres shared-memory segment costs **~34µs** on this
hardware — well under the plan's `< 80µs` kill criterion (§12) and under the
`< 60µs` GET-hit acceptance (§11). The extension runs inside Postgres via
`shared_preload_libraries`, the SQL surface reads the same segment in-process,
and the shared-nothing worker design scales to Valkey-class aggregate
throughput.

**P1 (storage & durability) now landed too:** RESP writes persist to a
hash-partitioned `supacache.kv` backing table via batched SPI, the same bytes
are queryable in SQL ("SQL access to the same bytes", §14), and after a hard
`kill -9` of the whole cluster the worker rebuilds shmem from the tables —
104k keys recovered in 367ms. See §5b. Security (§4) remains untouched: that is
P2, the ship-decider.

This report presents *measured* numbers, cross-checks them against the plan's
targets, and separates what the POC validates empirically from what it
validates only by construction.

---

## 1. What was built

A real Postgres 16 extension (`extensions/pg_keyspace/extension`, pgrx 0.12.9,
Apache-2.0) loaded via `shared_preload_libraries = 'pg_keyspace'`. It:

- **requests a Postgres shared-memory segment** in `shmem_request_hook` and
  initialises the keyspace store over it in `shmem_startup_hook` (§3.2 — here
  `ShmemInitStruct`, the pre-PG17 equivalent of `GetNamedDSMSegment`);
- **registers a background worker** — a real Postgres backend — that runs an
  `epoll` RESP event loop against that segment (§3.1), served on TCP `:6380`
  for stock clients (`redis-cli`, `ioredis`, `redis-benchmark`);
- **exposes the `supacache.*` SQL surface** (§6): `get/set/incr/del/getset/
  stats` read and write the *same* segment directly in the calling backend.

The performance-critical modules — the open-addressed hash + size-classed slab
allocator + CLOCK eviction (§3.2), the RESP2 codec (§5), the epoll loop (§3.1),
CRC16 slot routing (§3.1), and the commit batcher for the four durability tiers
(§3.4) — live in `extensions/pg_keyspace/poc/src` and are compiled **verbatim**
into the extension (via `#[path]`). So the code measured standalone is the same
code running inside Postgres.

Verified end-to-end before benchmarking: a value `SET` over RESP is readable by
`supacache.get()` in a backend, and a value written by `supacache.set()` is
readable over RESP — same segment, both paths.

### Environment

| | |
|---|---|
| CPU | Intel Xeon @ 2.10GHz, 4 vCPU |
| RAM | 16 GB |
| OS | Linux 6.18 |
| Postgres | 16.13 (system), `shared_buffers=256MB` |
| Baseline | Redis 7.0.15, single-threaded, persistence off (`--save '' --appendonly no`) — a fair Valkey stand-in; Valkey forked from Redis 7.2 |
| Loopback | client and server co-located (localhost TCP) |

Redis stands in for Valkey: identical RESP2 wire protocol and single-threaded
event-loop architecture. The absolute Valkey numbers would be within a few
percent.

---

## 2. Latency — the P0 kill criterion

Closed-loop, single connection, no pipelining (`redis-benchmark -c 1 -P 1`),
200k requests, 512-byte values. This is the client-observed single-request
latency the §11 table targets.

| Op | Server | avg | p50 | p99 |
|---|---|---:|---:|---:|
| GET | Redis | 35µs | 39µs | 63µs |
| GET | **pg_keyspace** | **34µs** | **39µs** | **63µs** |
| SET | Redis | 36µs | 39µs | 63µs |
| SET | **pg_keyspace** | **33µs** | **39µs** | **55µs** |
| INCR | Redis | 36µs | 39µs | 63µs |
| INCR | **pg_keyspace** | **34µs** | **39µs** | **55µs** |

**pg_keyspace matches Redis to within a microsecond on every operation**, and
beats every acceptance threshold in §11 (GET hit `< 60µs`, SET ephemeral
`< 80µs`, INCR `< 60µs`).

Why they are equal: at one in-flight request the latency is dominated by the
loopback TCP round-trip and syscalls, which both servers pay identically. The
store operation itself is nanoseconds (§5 below). This is precisely the plan's
own framing (§11): *"Throughput is where you lose, not latency … over a real
network every row in that table collapses to within a few percent."* Confirmed.

### vs the §11 targets

| Operation | Valkey (plan) | Target (plan) | Acceptance | **Measured (pgks)** | Verdict |
|---|---|---|---|---|---|
| GET, shmem hit | ~35µs | 40–50µs | < 60µs | **34µs (p50 39)** | ✅ beats target |
| SET, ephemeral | ~40µs | 45–60µs | < 80µs | **33µs (p50 39)** | ✅ beats target |
| INCR | ~35µs | ~40µs | < 60µs | **34µs (p50 39)** | ✅ beats target |

---

## 3. Throughput — pipelined

`redis-benchmark -c 50 -P 16`, 1M requests, 512-byte values, 100k-key keyspace.

| Op | Redis | pg_keyspace (1 worker) |
|---|---:|---:|
| SET | 537k/s | **556k/s** |
| GET | 628k/s | **500k/s** |

A single pg_keyspace worker already clears the §11 acceptance (`> 300k/s`) and
lands in/above the target band (400–800k/s). It edges Redis on SET and trails
on GET — expected, since a single shared-nothing worker is one event loop, same
as single-threaded Redis.

### Scale-out (§3.1 / P4)

Each worker owns a disjoint partition in its own segment on its own port, so
there is **no cross-worker contention**. N workers driven by N parallel clients,
aggregate rps (`-c 25 -P 16`, 512B):

| Workers | SET agg | GET agg | SET scaling |
|---:|---:|---:|---:|
| 1 | 569k/s | 604k/s | 1.0× |
| 2 | 1.10M/s | 1.13M/s | 1.9× |
| 4 | **2.46M/s** | **2.11M/s** | 4.3× |

Near-linear, and **4-worker SET (2.46M/s) matches Valkey's ~2.1M/s reference**
from §11 — on a 4-core box where the load generators are *also* competing for
those 4 cores. A dedicated load host would show more headroom. This directly
answers the plan's central throughput worry: the shared-nothing design scales
past a single Valkey instance.

---

## 4. In-backend shared-memory op (§6)

The `supacache.*` functions read the segment directly in the calling backend —
no socket. Measured in-process over 5M iterations (isolating the op from the
libpq round-trip):

| Op | ns/op |
|---|---:|
| get (hit) | **8.9 ns** |
| set (overwrite) | **11.8 ns** |
| incr | **36.4 ns** |

The plan (§6) conservatively estimated 1–2µs. The raw op is **~100× faster** —
single-digit to tens of nanoseconds. For the headline §6 use case (a
`supatype_mask` read predicate consulting a permission set), this is the
difference between a **~9ns in-process lookup** and a **~35µs Valkey network
hop** — roughly 4000×, per masked column per row.

For completeness, the same call over a full libpq round-trip
(`SELECT supacache.get(...)` via pgbench) is **48µs / 21k tps** — that is the
protocol cost, not the store cost, and is the number the `/rpc/get` path (§7.2)
would see.

---

## 5. Durability tiers (§3.4) — the differentiator

### Per-tier RESP SET, in-PG

`commit_window = 500µs`, WAL on the container filesystem.

| Tier | closed-loop p50 | closed-loop avg | concurrent (`-c 50 -P 1`) |
|---|---:|---:|---:|
| ephemeral | 39µs | 33µs | 86.8k/s |
| relaxed | 39µs | 35µs | 98.4k/s |
| durable | 1039µs | 1069µs | 995/s |
| replicated | 591µs | 615µs | 1.8k/s |

`relaxed` (async, `synchronous_commit=off` analog) matches `ephemeral` latency
because the client does not wait for the fsync. `durable` waits ~one commit
window per write, as designed.

### Finding: the batcher amortises, but the single worker can't exploit it

The `durable` *concurrent* row above (995/s) is **a real finding, not the
batcher's ceiling.** The single-threaded epoll worker **blocks** on each
durable commit's fsync, so writes serialise instead of accumulating into a
batch. Measuring the batcher directly under concurrent committers (modelling N
slot workers, or deferred RESP acks) shows the amortisation the plan claims:

| Tier | 1 committer | 8 committers | 64 committers |
|---|---:|---:|---:|
| durable | 1.0k ops/s | 8.4k ops/s | **38.5k ops/s** |
| relaxed | 4.2M ops/s | 2.1M ops/s | 1.8M ops/s |

Durable throughput scales **~38×** with concurrency while per-write latency
stays near the commit window — "one fsync amortised across hundreds of
operations" (§3.4), confirmed. The mechanism is sound; the integration needs
one of: (a) multiple slot workers (the real design has N), or (b) deferred acks
so a single worker keeps serving while a batch settles. **This should be a
P1 design note.**

---

## 5b. P1 — real backing tables, persistence, crash recovery

P0 modelled durability with a stand-in WAL file. P1 makes it real: the in-PG
worker connects SPI, ensures a hash-partitioned `supacache.kv` table (§3.3),
and — for any non-ephemeral tier — flushes staged RESP writes into it in one
batched transaction per window, then rebuilds shmem from the table on startup.

### SQL access to the same bytes (§14 differentiator)

A value written over RESP is immediately in shmem and, within one flush window,
in `supacache.kv`. All three views agree:

```
redis-cli -p 6380 set persist:key:42 value-number-42
SELECT convert_from(val,'UTF8') FROM supacache.kv WHERE key='persist:key:42';  -- value-number-42
SELECT convert_from(supacache.get('persist:key:42'),'UTF8');                    -- value-number-42
```

Writes distribute evenly across the 8 hash partitions (118–134 rows each at
n=1000). This is the capability Valkey structurally cannot offer.

### Crash recovery (§12 P1)

Hard `kill -9` of the entire cluster (shmem destroyed), then restart:

| Keys | Recovery time | Per key |
|---:|---:|---:|
| 5,000 | 6.9 ms | ~1.4µs |
| 104,290 | 366.9 ms | ~3.5µs |

Postgres replays the committed `supacache.kv` rows in WAL crash recovery first;
the worker then loads them into a fresh shmem segment. Every key survived and is
served over RESP after restart.

### Cost of persistence (single worker, 512B)

| | ephemeral | relaxed (persisted) |
|---|---:|---:|
| pipelined SET (`-c50 -P16`) | 556k/s | **32.4k/s** |
| closed-loop SET p50 | 39µs | 39µs |
| closed-loop SET avg | 33µs | 69µs |
| GET (unaffected) | 500k/s | 500k/s |

**Reads are unaffected** — still full ephemeral speed. Persisted *write*
throughput drops ~17× because the POC upserts row-by-row via SPI and the flush
transaction blocks the single event-loop thread (visible as 18k–52k/s jitter).
`relaxed`'s async ack keeps write *latency* at ephemeral levels (p50 39µs). The
finding: production persistence must not upsert row-by-row on the event loop —
use multi-row `INSERT`/`COPY`, a dedicated I/O worker, or the
`XACT_EVENT_COMMIT` publication path (§3.5). This is the same class of finding
as the durable-serialisation one in §5.

## 6. Concerns validation matrix

### 6a. Performance & architecture — validated empirically

| Concern (plan ref) | Result |
|---|---|
| RESP hit latency < 80µs kill criterion (§12) | ✅ 34µs measured |
| GET/SET/INCR beat §11 acceptance | ✅ all under threshold |
| Single-worker throughput > 300k/s (§11) | ✅ 500–556k/s |
| Shared-nothing workers scale (§3.1) | ✅ 4.3× at 4 workers, 2.46M/s |
| Shmem hash + slab + CLOCK works under churn (§3.2) | ✅ eviction test + 5M-op benches, no corruption |
| No transaction on the read path (§3.1) | ✅ worker opens none; `backend_xmin` stays null (see §7) |
| In-backend SQL read is ~µs (§6) | ✅ 9ns — 100× better than estimate |
| Per-prefix durability tiers exist (§3.4) | ✅ all four tiers run in-PG |
| Commit batching amortises fsync (§3.4) | ✅ 38× at 64 committers |
| bgworker can run an epoll loop inside PG (§3.1) | ✅ runs, binds, serves, shuts down on SIGTERM |
| PG shared memory segment via hooks (§3.2) | ✅ 713MB segment, shared across backends |
| Hash-partitioned backing tables (§3.3, P1) | ✅ `supacache.kv`, 8 partitions, even |
| SQL access to the same bytes (§14, P1) | ✅ RESP write ↔ SELECT ↔ `supacache.get` agree |
| Batched commit to real tables (§3.4, P1) | ✅ one transaction per window (SPI) |
| Crash recovery from tables (§12 P1) | ✅ 104k keys reloaded in 367ms after `kill -9` |
| bgworker is a real backend w/ SPI (§3.1) | ✅ `connect_worker_to_spi`, no read-path txn |

### 6b. Security model (§4) — NOT implemented in this P0; validated only on paper

The plan is explicit (§12) that security is **P2** and *"the phase that decides
whether the project ships."* This spike implements none of it. The §4.7 threat
table is design analysis here, not test evidence:

| §4.7 case | Status in POC |
|---|---|
| `shared_preload_libraries` misordered vs `supatype_mask` | ❌ not implemented (load-order assertion is P2) |
| `register_keyspace` on a `supatype`-labelled relation | ❌ no `register_keyspace` / seclabel check yet |
| Seclabel added after registration → fail closed | ❌ not implemented |
| RESP client requests another tenant's key | ❌ no tenant scoping / AUTH yet (single namespace) |
| Tenant grants self `supacache_admin` | ❌ pg_guard reserved-membership config, P2 |
| `force_generic_plan`, warm cache, two identities | ❌ Mode B row cache not built (P6) |
| Masked column, warm cache, non-exempt role | ❌ Mode B, P6 |
| Decoding worker stores WAL values | ❌ decoding worker not built (P1/P6) |

**Do not read this POC as evidence the security model holds.** It is evidence
that the *performance* substrate the security model sits on is fast enough to be
worth securing.

### 6c. Bugs / risks surfaced by building it

- **Probe infinite loop under tombstone saturation (fixed).** The first cut of
  the open-addressed table looped forever once live entries + tombstones filled
  every bucket (a lookup only stops on an EMPTY slot). Fixed with a bounded
  probe + tombstone accounting + rehash-to-compact at 70% load. This is the
  single most likely way a naive shmem hash table wedges a worker under churn;
  worth calling out for the real implementation. See `poc/src/store.rs`.
- **Durable writes serialise on a single worker (open).** See §5. P1 design note.
- **`redis-benchmark` "Could not fetch server CONFIG" warning (cosmetic).** The
  POC replies `+OK` to `CONFIG GET` rather than the expected 2-element array;
  benchmarks run fine. Trivial to implement properly.

---

## 7. Notes on the "worker pins xmin" risk (§13)

The plan flags a long-lived worker holding a transaction and pinning global
`xmin` as *"the single most likely way to take down a customer's database."*
The ephemeral RESP worker in this POC opens **no** transaction and makes **no**
SPI call on the read/write path — it only touches shared memory. A backend's
`pg_stat_activity.backend_xmin` stays null. The logged durability tiers, which
in the full design would use SPI/WAL, are exactly where this rule must be
enforced; the POC's batcher writes its own WAL file and still opens no PG
transaction, but the production version integrating with real WAL must keep the
commit off the worker's snapshot.

---

## 8. Limitations (what this P0 is not)

- No security: no AUTH, no keyspace ACL, no tenant scoping, no `supatype_mask`
  integration, no load-order assertion (all P2, §4).
- No Mode B row cache / planner hook (P6, §7.1).
- No logical-decoding invalidation worker (§3.5).
- Command set is P0-minimal: strings, counters, DEL/EXISTS, TTL on strings.
  Hashes/lists/sorted-sets/pub-sub are P3 (§5).
- Single in-PG worker; multi-worker scale-out shown via the standalone daemon
  (the in-PG version would register N background workers).
- `ShmemInitStruct` (PG16) rather than `GetNamedDSMSegment` (PG17); equivalent
  for this purpose and does not affect latency.
- P1 persistence (in-PG) upserts row-by-row via SPI on the event-loop thread —
  correct and crash-safe, but write-throughput-limited (§5b). The standalone
  file batcher (§5) remains the model for raw fsync amortisation. Production
  needs multi-row/COPY persistence off the event loop.
- Only `relaxed`/async persistence is wired to real tables in-PG so far;
  `durable`/`replicated` sync-ack-to-commit semantics against `supacache.kv`
  are the next P1 increment (the standalone batcher already characterises their
  fsync cost).
- DEL is not yet propagated to the backing table (upserts only); TTL partition
  drop (§3.3) not built.

---

## 9. Reproduce

```bash
# core unit tests (hash table, slab, eviction, TTL, CRC16)
cd extensions/pg_keyspace/poc && cargo test

# build + install the extension into system PG16
cd extensions/pg_keyspace/extension
cargo pgrx install --release --pg-config /usr/bin/pg_config

# start a cluster with the extension preloaded (see bench/ for the exact conf),
# then:
bash extensions/pg_keyspace/bench/run_benchmarks.sh   # latency, throughput, §6
bash extensions/pg_keyspace/bench/run_durability.sh   # per-tier RESP SET
bash extensions/pg_keyspace/bench/run_scaleout.sh     # shared-nothing scaling
./extensions/pg_keyspace/poc/target/release/durability_bench  # batcher amortisation
```

Raw `redis-benchmark` outputs are in `results/raw_*.txt`; summaries in
`results/summary.txt`, `durability.txt`, `durability_batcher.txt`,
`scaleout.txt`.

---

## 10. Recommendation

The engineering risk the P0 spike exists to retire — *can a Postgres background
worker serve a shared-memory keyspace over RESP at Valkey-class latency?* — is
**retired**. Measured latency ties Valkey; single-worker throughput clears the
bar; the design scales to Valkey-class aggregate throughput; and the in-process
SQL read (§6) is 100× better than projected, which is the strongest standalone
argument in the whole plan (the mask-predicate accelerator, §4.3c/§6).

Per the plan's own §14 sequencing, the honest next step is **not** to keep
building performance. It is **P2 security**, which this POC deliberately does
not touch and which decides whether the project ships. The numbers here justify
funding that phase; they do not substitute for it.
