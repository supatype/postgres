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
