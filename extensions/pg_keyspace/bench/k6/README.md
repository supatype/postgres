# k6 load tests

Closed-loop, application-shaped load, as a complement to the rest of `bench/`.

The other benchmarks measure **ceilings**: how fast the machinery goes when one
client pipelines as deeply as it can. That is the right question for capacity
planning and the wrong one for "what will my application see". Real traffic is
closed-loop — each request waits for its reply — and concurrency comes from many
such clients rather than from depth on one. These two scripts measure that.

```bash
go install go.k6.io/k6@latest        # or grafana.com/docs/k6

# sustained mixed workload, ramping concurrency, with pass/fail thresholds
TARGET=redis://127.0.0.1:6379 LABEL=mine k6 run soak.js

# latency against a FIXED offered rate (open model)
TARGET=redis://127.0.0.1:6379 RATE=8000 OP=get  k6 run ladder.js
TARGET=redis://127.0.0.1:6379 RATE=2000 OP=set  k6 run ladder.js
```

`soak.js` runs a read-dominant mix (hits, misses, TTL'd writes, hash ops) with
ramping VUs and thresholds that fail the run. `ladder.js` holds a fixed arrival
rate so that a struggling target shows up as latency rather than as a lower
rate, and reports achieved-vs-offered so starvation is visible.

## Two traps these scripts are shaped around

**k6 gives scripts no sub-millisecond clock.** There is no `performance.now()`,
so any latency computed in JS is quantised to 1 ms — useless against a store
that answers in ~40 µs. `iteration_duration` is timed by k6 itself, in Go, at
microsecond resolution. So each scenario performs exactly **one** operation per
iteration and the operation's latency *is* the iteration duration. This is why
the scripts look the way they do; a "tidier" version that times several
operations inside one iteration measures nothing.

**Size the VU pool for the slowest target, not the fastest.** A durable write is
held until its batch commits, so it takes milliseconds where a cache read takes
microseconds. With a pool sized for the fast case, k6 runs out of VUs, drops
iterations, and reports **its own starvation as the server's throughput** — in
development this showed a durable tier "achieving" 536/s against an offered
2 000/s that it in fact sustained in full. `VUS` / `MAXVUS` override the
defaults.

## Read the achieved rate before believing the latencies

On a small or shared machine the k6 process is itself a significant load, and it
saturates long before a competent server does. Measured here on a 4-vCPU
container running the generator *and* the server:

| offered | pg_keyspace achieved | redis 7.0.15 achieved |
|---:|---:|---:|
| 2 000/s | 1 927/s | 1 937/s |
| 8 000/s | 7 738/s | 7 695/s |
| 20 000/s | 19 105/s | 19 163/s |

The two track each other within a few percent at every rate, and both show p95
climbing to ~8 ms at 20 000/s. Redis does not knee at 20 000 ops/s on real
hardware — that is the box, not either server. **On a machine like this these
scripts cannot tell two fast targets apart**; they can only show that one is not
anomalously worse than the other. Run the generator on a separate machine before
reading a head-to-head as a result.

What *does* survive a generator-bound box is any difference large enough to
dwarf it — the durable tier's write latency, for instance, which is a property of
the design rather than of the load:

| tier | p50 | p95 | achieved |
|---|---:|---:|---:|
| ephemeral | 0.2 ms | 0.3 ms | 2 000/2 000 |
| durable, `persist_window_ms = 10` | 6.5 ms | 13.5 ms | 2 000/2 000 |
| durable, `persist_window_ms = 50` | 27.2 ms | 53.8 ms | 2 000/2 000 |

All at 2 000 writes/s of distinct keys, 256-byte values. A durable ack is held
until its batch commits, so write latency tracks the persist window — which is
the cost side of the throughput curve in the main README's tuning section.

## `mixed.js` — the soak workload (#113)

`soak.js` and `ladder.js` measure latency. `mixed.js` is shaped to **find
things** instead, which is what #113 asks for: the k6 runs before it found no
bugs, and a two-minute happy-path run that finds nothing is weak evidence.

Driven by `bench/run_soak.sh`, which also runs pgbench against the Mode B row
cache (a SQL path the redis client cannot reach), injects faults, and judges
drift. Run it directly only to point load at a server somewhere else:

```bash
HOST=cache.internal PORT=6380 WORKERS=2 TENANTS=3 SECRET=... \
  DURATION=4h PEAK=64 MAX_ERRORS=0 k6 run mixed.js
```

Four things it does that `soak.js` does not:

**Every read verifies a checksum.** A value is a pure function of its key, so a
reader needs no shared state to know what it should have got back. A cache
confidently returning the *wrong* bytes fails the run; one that only checks for
null cannot tell that from working. `run_soak.sh` then re-derives the same
hash in SQL and checks every persisted row, closing the loop from client through
the ring to the table.

**Multi-tenant.** Each VU authenticates as one of several tenants, so keys are
force-scoped server-side and the fairness paths are under real contention.

**Workload variety, together.** Hot and cold key distributions, aggregates at
size, TTL churn heavy enough to cycle partitions, and pub/sub concurrent with
writes — the combination, which is what had never been exercised.

**Writers and readers overlap** on the same keys, rather than seeding a corpus
and then only reading it. A pre-seeded read-only corpus cannot expose a race.

### Two traps specific to this script

**The client is cluster-aware, and must be.** pg_keyspace shards across workers
and answers `MOVED` for a key it does not own. A plain client does not follow
that: against two workers, every operation failed and the run reported 0
iterations while its checksum threshold passed vacuously. `WORKERS` builds the
node list.

**Not every command exists on the k6 client.** `publish` does not, and calling
it threw instantly — so that scenario ran 5.1 *million* no-op iterations,
inflating both the iteration count and the error count while publishing nothing.
`sendCommand` is the way, and `setup()` now probes every operation the run uses
before the run starts, so a missing method fails immediately instead of becoming
four minutes of counterfeit load.
