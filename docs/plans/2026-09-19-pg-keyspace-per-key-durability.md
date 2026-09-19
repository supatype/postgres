# pg_keyspace: per-key durability

**Issue:** [supatype/postgres#164](https://github.com/supatype/postgres/issues/164)

**Goal:** Let one pg_keyspace instance serve durable and ephemeral keys at the
same time, chosen by key prefix, so a deployment no longer has to run its whole
keyspace durable to protect the small part that matters.

**Architecture:** A prefix → tier map GUC, evaluated against the stored key at
the three points where a write reaches the persistence ring. Recovery applies
the same predicate. No change to the wire protocol, no new commands, no client
changes.

**Repos:** `supatype/postgres` (pieces 1, 1.5, 3, 4), `supatype` (piece 2).

---

## Problem

Durability is one `PGC_POSTMASTER` GUC that collapses to a single per-worker
boolean:

```rust
// core/src/server.rs:1627
let persist_on = !self.producers.is_empty();
```

There is no third state, so an instance needing *any* durable data must run
everything durable. Self-host needs both on one instance: Kong's ACME
certificates must survive a restart, the user's general REST cache must not pay
a WAL write and a held `+OK` per `SET`. Today self-host runs a separate Valkey
container for both, which is the second stateful service pg_keyspace exists to
remove.

`extensions/pg_keyspace/README.md:92` already advertises per-key durability as a
reason to choose pg_keyspace over Valkey, so this is a documentation overclaim
as much as a missing feature.

## Approach

Durability must be **configuration-driven, not protocol-driven**. The
extension's value is that stock clients work unmodified — Kong's ACME plugin
cannot emit a custom `SET … PERSIST` flag, and neither can anyone else's
off-the-shelf app. A custom command would restrict per-key durability to code
the operator wrote themselves, which excludes the case that motivates it.

So the server decides from what it can already see: the key. Redis's other
namespacing axis is not available here — `SELECT` is a no-op `+OK`
(`core/src/server.rs:1717`), so there are no logical databases to hang
durability off. That leaves the prefix, which is what everyone already uses
(`cache:`, `sess:`, `kong_acme:`) precisely because Redis databases are
useless. It is the universal idiom, not a local convention.

### Surface

```ini
pg_keyspace.durability = 'ephemeral'                          # default tier, unchanged
pg_keyspace.durability_overrides = 'acme:=durable, metrics:=relaxed'
```

| Property | Rule |
|---|---|
| Default tier | `pg_keyspace.durability`, exactly as today |
| Match against | The **stored** key, after tenant scoping |
| Overlap | Longest prefix wins |
| Empty map | Byte-identical behaviour to today |
| Context | `PGC_POSTMASTER`, matching `pg_keyspace.durability` |
| Unparseable entry | Refuse to start, naming the entry |
| `replicated` in the map | Rejected at startup in piece 1 — see piece 1.5 |

A map rather than a boolean `durable_prefixes` list, for three reasons. Both
directions come free (`durability = 'durable'` with `cache:=ephemeral` is the
mostly-durable deployment). `relaxed` becomes reachable per prefix, which is
what most caches actually want — persisted, but the reply is not held until
commit. And the surface will not need to change when the remaining tier lands,
which matters more than implementing every tier now, because the GUC is the
compatibility promise.

Tenant granularity falls out at no cost: forced scoping already rewrites keys to
`{tenant}:{key}` (`core/src/server.rs:6431`), so `tenantA:` is just a prefix.
That covers the Cloud model without a second surface, and it is why prefixes
beat a tenant-keyed GUC — self-host runs no RESP auth at all (Kong connects
with no password, `supatype/packages/cli/src/kong-config.ts:105`), so `tenant`
is empty there and a tenant-keyed surface would not unblock it.

`PGC_POSTMASTER` over `PGC_SIGHUP`: flipping a prefix ephemeral → durable at
runtime leaves already-written keys unpersisted, so they survive until the
restart and then vanish — the worst possible shape for a durability setting.

### The four tiers

| Tier | Ring | Reply held? | `synchronous_commit` |
|---|---|---|---|
| `ephemeral` | no push | no | — |
| `relaxed` | push | no — acked immediately | `off` |
| `durable` | push | until local commit | `on` (local fsync) |
| `replicated` | push | until standby applies | `remote_apply` |

## Where it lands

The ~40 `if persist_on` sites are not 40 decision points. Every write reaches
the ring through one of three places:

| Site | Path |
|---|---|
| `core/src/server.rs:5046` | The `stages` drain loop — all normal writes |
| `core/src/server.rs:1958` | `DEL` / `UNLINK`, direct `shard_push` |
| `core/src/server.rs:3175` | `FLUSHDB` victim sweep, direct `shard_push` |

Plus the backpressure pre-flight at `core/src/server.rs:1678`, which must
consult the same predicate or a full ring parks ephemeral writes, defeating the
point.

Everything else falls out of those four edits:

- **No ring record → no entry in `acks`**, so a durable-tier instance replies
  immediately for an ephemeral key with no extra code.
- **`relaxed` vs `durable` per command** is only whether `acks` is merged into
  `c.ack` at `core/src/server.rs:5119`.
- **The row cache is unaffected** — it never reaches `server.rs`, so no prefix
  can accidentally make PostgREST entries durable.

The matcher belongs in `core/` (`Tier` already lives at `core/src/batcher.rs:28`),
so it is unit-testable against the standalone daemon with no Postgres. The
extension parses the GUC and calls a setter, mirroring `set_sync_ack`.

## Non-goals

- Per-key durability chosen at the protocol level (a `SET … PERSIST` flag).
- Mixing `replicated` with other tiers in one instance (piece 1.5).
- Making owner-addressed recovery client-transparent across a resize — not
  achievable; see piece 3.

---

# Piece 1 — per-key durability at `workers = 1`

*`supatype/postgres`. This is the whole self-host unblock and depends on
nothing else.*

`workers = 1` is what self-host runs (`config/pg_keyspace.conf` does not set it
and the GUC defaults to 1). At that count `self.routing` is `None`, no MOVED is
ever issued, and `cluster_announce_host` is not required
(`extension/src/lib.rs:2353`). None of the multi-worker routing problem that
issue #164 spends most of its length on is on this path.

## Task 1.1: The matcher

**Files:** create `core/src/durability.rs`; modify `core/src/lib.rs`

- A `Policy` type holding the default `Tier` and an ordered prefix → `Tier` list.
- `Policy::tier_for(&self, key: &[u8]) -> Tier`, longest prefix wins.
- `Policy::parse(default: Tier, spec: &str) -> Result<Policy, String>` for the
  GUC string, so a bad entry is reported with the entry in the message.
- Store prefixes sorted by descending length so the first match is the longest;
  with a small map that is a handful of byte compares.

**Unit tests (in-file):** empty map is the default tier for everything; longest
prefix wins with `a:` and `a:b:` both set; a tenant-scoped key `t1:acme:x`
matches `t1:acme:`; unparseable specs produce an error naming the entry;
`replicated` parses but is flagged so the extension can reject it.

## Task 1.2: Wire the policy into the server

**Files:** modify `core/src/server.rs`

- `set_durability_policy(&mut self, policy: Policy)`, mirroring
  `set_sync_ack` (`core/src/server.rs:810`).
- Replace the `persist_on` boolean at the four sites with a per-key decision:
  - `:5046` — skip staging a record whose tier is `ephemeral`.
  - `:1958` — `DEL` / `UNLINK`.
  - `:3175` — `FLUSHDB` victim sweep.
  - `:1678` — the pre-flight only reserves room for keys that will actually be
    pushed.
- `relaxed` keys push but do not merge into `c.ack` at `:5119`.

**Tombstones are pushed unconditionally.** A `DEL` of a key whose prefix *was*
durable under a previous boot must still delete the row, or the key resurrects
at the next restart. Cheap, and it makes editing the map safe.

## Task 1.3: The GUC

**Files:** modify `extension/src/lib.rs`

- `GUC_DURABILITY_OVERRIDES` alongside `GUC_DURABILITY` (`:127`), defined next
  to it in `_PG_init` (`:1414`), `GucContext::Postmaster`.
- Parse at worker start, log the resolved map once per worker.
- Refuse to start, holding the worker in the existing sigterm-wait loop, on:
  - an unparseable entry, naming it;
  - a `replicated` override (piece 1.5);
  - a non-empty map with `workers > 1` (piece 4).

A loud refusal, never silent partial durability. Same posture the codebase
already takes for a missing `cluster_announce_host` and a bad TLS cert.

## Task 1.4: Recovery applies the same predicate

**Files:** modify `extension/src/lib.rs` (`pg_recover`, `:3364`)

A row under a prefix that is no longer durable must not come back into shmem as
if nothing changed. Filter the recovery scan by the current policy so stale rows
are inert, whether or not they are ever deleted. This is the correctness half of
the map-narrowing problem and it needs no maintenance task.

## Task 1.5: Cleanup for rows no longer covered

**Files:** modify `extension/src/lib.rs`

Only `supacache.kv` accumulates: `kv_ttl` is range-partitioned by expiry bucket
and dropped wholesale once fully expired (`extension/src/lib.rs:5333`), so the
garbage window there is bounded by the bucket width.

- `supacache.undurable_rows()` — read-only: rows, bytes and distinct key
  prefixes not covered by the current map. Look before you leap.
- `supacache.prune_undurable()` — deletes them, returns the count.

**Not automatic on boot.** If an operator typos the GUC, or a config include
fails to load, the map comes up empty and a blanket "delete everything not
matching a durable prefix" destroys the entire durable dataset — irreversibly,
in the one situation where the configuration cannot be trusted. So
`prune_undurable()` refuses when it would delete all rows, or more than a
configurable fraction, unless explicitly forced. The task 1.4 filter means the
garbage can sit there indefinitely without ever being served wrong.

An opt-in `pg_keyspace.prune_on_recovery = off` with the same all-rows guard is
acceptable, but not the default.

## Task 1.6: Observability

**Files:** modify `extension/src/lib.rs`

- `supacache.key_durability(key text) -> text`, mirroring
  `supacache.key_worker()` (`:6813`). An operator must be able to ask "is this
  key durable?" without doing prefix arithmetic in their head.
- The durable / ephemeral split reported in the existing stats views.

## Task 1.7: Hot-path cost, as a gate

**Files:** create `core/src/bin/prefix_match_bench.rs`;
create `bench/run_prefix_match_cost.sh`; modify `.github/workflows/test-pg-keyspace.yml`

The matcher runs on every write, against a documented ~1–2 µs op. Measure it,
do not assume it.

- Microbenchmark beside `core/src/bin/durability_bench.rs`: match cost at
  0 / 1 / 8 / 64 prefixes, hits and misses.
- End-to-end, which is the number that matters: `supacache.bench_set()`
  (`extension/src/lib.rs:6846`) times the raw shared-memory op inside the
  backend with no protocol round-trip. Compare an empty map against 64 prefixes
  and **fail the job** on a regression beyond a stated threshold.
- If the map grows past what a sorted linear scan handles, switch to a trie —
  but only with this benchmark saying so.

## Task 1.8: End-to-end test

**Files:** create `bench/run_per_key_durability.sh`;
modify `.github/workflows/test-pg-keyspace.yml` (`durability-in-postgres` job)

Write to a durable prefix and an ephemeral prefix, restart, and assert:

- exactly the durable set returns;
- `supacache.kv` holds only those rows;
- a `DEL` of a durable key does not resurrect it across a restart;
- an ephemeral write on a `durable` instance is acked without waiting on a
  commit;
- narrowing the map leaves the now-ephemeral keys unserved after a restart
  (task 1.4), and `undurable_rows()` reports them (task 1.5).

## Task 1.9: Documentation

**Files:** modify `extensions/pg_keyspace/README.md`

Fix the `:92` overclaim in the same PR that makes it true, and document the GUC,
the longest-prefix rule, the cleanup functions and the `replicated` restriction.

---

# Piece 1.5 — `replicated` in a mixed map

*`supatype/postgres`. Optional, its own PR. Nothing currently needs it.*

Three things make `replicated` the awkward tier:

1. **The commit setting is per transaction, and there is one transaction per
   flush.** `sync_commit` is chosen once at persist-worker startup
   (`extension/src/lib.rs:2575`) and applied as `SET LOCAL synchronous_commit`
   per batch (`:3483`). If `acme:` is `replicated` and `sessions:` is `durable`,
   one transaction cannot honour both: `remote_apply` for the batch makes every
   durable write pay standby latency, and `on` breaks the replicated promise.
   Fix: split the batch by tier, one transaction per tier per flush. Safe,
   because a key's tier is a function of the key, so splitting never reorders
   any single key's history.

2. **The fail-closed path does not split as cleanly, and this is the real
   cost.** `synchronous_standby_names` is SIGHUP context, so the replicated
   promise can be withdrawn at runtime. The persist worker handles this today
   (`:2613`): if the standby vanishes it refuses to commit, leaves the whole
   batch in the rings, holds the durable acks, and logs it. Under a mixed map
   that is wrong — it would stall `durable:`-prefix writes behind a standby
   outage unrelated to them, filling the ring and eventually parking those
   connections. Partial retention (hold the replicated records, commit the
   durable ones) is new behaviour, not a refactor: the ring's commit
   bookkeeping is currently all-or-nothing per batch.

3. **Nobody has asked for it.** Self-host runs no synchronous standby. Cloud's
   case in #164 is `durable` + `relaxed`.

If built, it needs its own fault-injection run through `bench/run_durability_pg.sh`.

---

# Piece 2 — self-host wiring

*`supatype`. Depends on piece 1.*

**Files:**
- Modify: `packages/cli/src/self-host-compose.ts`
- Modify: `packages/cli/src/kong-config.ts`
- Modify: `packages/cli/tests/compose-services.test.ts`,
  `packages/cli/tests/runtime-contract.test.ts`
- Modify (postgres repo): `config/pg_keyspace.conf`

- Drop the Valkey service (block at ~`:428`), its volume and its depends-on.
- Point `SUPATYPE_VALKEY_ADDR` (`:644`) and `acme.redisHost` (`:941`) at
  Postgres:6379.
- Set `storage_config.redis.namespace` explicitly in the ACME block
  (`kong-config.ts:105`) so the durable prefix is deterministic rather than
  depending on Kong's default.
- `pg_keyspace.durability_overrides` in `config/pg_keyspace.conf` naming that
  namespace as `durable`; the REST cache stays on the `ephemeral` default.

---

# Piece 3 — owner-addressed recovery

*`supatype/postgres`. Blocks only the multi-worker case — the Cloud platform
keyspace, not self-host.*

Lands the prototype on `proto/owner-addressed-recovery` (`69c27565`). Its core
diagnosis is right: `kv.slot` is written at persist time, indexed, and read by
exactly one consumer, `pg_recover`'s range filter, so the coupling that forces
MOVED is incidental rather than structural. Four things need settling first.

## Task 3.1: Make the recovery predicate sargable

`WHERE owner % $1 = $2` cannot use a plain btree on `owner`, so recovery
seq-scans all of `supacache.kv` once per worker — a regression against slot
mode, where `slot >= $1 AND slot < $2` does use `kv_slot_idx`. The
`pg_keyspace.workers` GUC caps at 64 (`extension/src/lib.rs:1379`), so the owner
domain is 0..63 and the predicate can be `owner = ANY($1::int[])` with the ≤64
values precomputed. Sargable, identical semantics.

## Task 3.2: Handle resize rather than documenting it

**The constraint cannot be engineered away.** The server does not know the
client's routing function. Either it tells the client where a key lives (the
slot map plus MOVED), or placement happens to equal what the client computes, or
the client is pinned to one worker. Owner addressing takes the second, which
holds while the worker count is stable and breaks when it is not.

**Growth is broken too, not only non-divisor shrinks.** 4→2 works because
`(h%4)%2 == h%2`. 2→4 does not: a key with `h%2 == 1` was written by worker 1
and recovers at `1%4 = 1`, but a client sharding `h%4` looks at 1 or 3, and for
`h=3` it looks at 3 and misses. Only a shrink to a divisor preserves
client-visible placement. Re-stamping owners at recovery makes placement stable
but still not predictable; re-placing by key hash is the rejected
"forward the persist" option under another name.

So "handled" means made impossible to hit silently, in three parts:

- **Refuse the resize by default.** The old worker count is already recorded in
  `supacache.topology` and read at startup. Under
  `recovery_addressing = 'owner'` with persisted rows present, a changed count
  refuses to start, naming the options: restore the previous count, rehome, or
  accept a cold cache with an explicit `pg_keyspace.allow_owner_resize = on`.
- **An explicit rehome.** `supacache.rehome(workers int)` rewrites the owner
  column deterministically for a target count and reports the resulting
  placement, so a resize is a deliberate operation with a known outcome.
- **Make the duplicate-write hazard visible.** This is the sharp edge, not the
  miss: after a bad resize the client misses, rewrites the key under a new
  owner, and the single `kv` row collapses that at the next restart — the old
  value gone with nothing said. A counter for "key recovered under owner O, then
  written by worker W ≠ O", surfaced in the stats views. *Confirm the write path
  can see the recovered owner cheaply before committing to this one.*

## Task 3.3: Make the SQL surface addressing-aware

`supacache.slot_ranges()` and `key_worker()` (`extension/src/lib.rs:6782`,
`:6813`) compute from `crc16` unconditionally, and the view at `:7975` joins on
them. Under `recovery_addressing = 'owner'` they report a map recovery does not
use, and `bench/run_persist_multiworker.sh` asserts against exactly those
functions. Either make them addressing-aware or have them error under owner
mode, but do not let them lie. Same for `supacache.topology_change()`, whose
`slots_moved` / `pct_moved` are meaningless in owner mode.

## Task 3.4: Harnesses

- `bench/proto/run_owner_recovery.sh` verifies `expect = original_owner % N` —
  it checks the rule, not a client. Add a verify pass that shards the way a real
  standalone client does (`hash(key) % N`), expected to fail at non-divisor
  counts and saying so, the way `run_slot_control.sh` earns its result by
  failing.
- The three gaps #164 already names: `bench/run_durability_pg.sh`'s fault matrix
  under owner mode, pub/sub interaction, `topology_change()`.
- Move the harnesses out of `bench/proto/` into the CI suite.

Keep from the prototype: the log and message work, and carrying `owner` with the
record in `bulk_upsert` so dedupe cannot pair a surviving value with a
superseded owner.

---

# Piece 4 — lift the `workers = 1` restriction

*`supatype/postgres`. Mechanical once 1 and 3 are both in.*

Drop the task 1.3 refusal; extend `bench/run_per_key_durability.sh` across
workers.

---

## Sequencing

```
Piece 1 ──► Piece 2          self-host unblocked here
   │
   ├──────► Piece 1.5 (optional)
   │
   └──────► Piece 4 ◄──────── Piece 3
```

Pieces 1 and 2 unblock self-host and depend on nothing else. Piece 3 is the
larger, riskier change; task 3.2 may need its own design round before it is
properly scoped, which is the main reason not to let self-host queue behind it.

## Risks

| Risk | Handling |
|---|---|
| Prefix match erodes the ~1–2 µs op | Task 1.7, as a CI gate rather than a note |
| Operator narrows the map, old rows linger | Task 1.4 makes them inert; task 1.5 removes them, with an all-rows guard |
| `replicated` overrides silently degrade | Rejected at startup in piece 1, with the reason stated |
| Owner-mode resize loses data silently | Task 3.2: refuse by default, explicit rehome, conflict counter |
| Recovery slower under owner addressing | Task 3.1 |
