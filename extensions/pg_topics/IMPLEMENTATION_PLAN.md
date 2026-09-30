# pg_topics implementation plan

Source design: `docs/plans/2026-09-27-pg-topics-design.md` (called "the design" below).
This plan tells an implementer what to build, in which order, and how to prove each phase.
Execute the phases in order. Each phase ends green and in one commit.

---

## 1. Done means

The work is done when all of these are true:

1. `CREATE EXTENSION pg_topics` installs on a stock PostgreSQL 17 with `shared_preload_libraries = 'pg_topics'`.
2. A SQL user can create a topic, publish in their own transaction, read by offset, and commit a group position.
3. The stamper assigns gap-free offsets per band, only to committed rows, in visibility order.
4. The partition worker creates partitions ahead of time and drops old ones in the design order (detach, check, read floor, drop).
5. The sync worker keeps a base table current and puts failed records in the `_qe` table.
6. A stock Kafka client (Java, librdkafka, Confluent JS) produces, consumes, joins a group, and commits offsets over SASL PLAIN on TLS.
7. An idempotent Java producer (the default since Kafka 3.0) works, and a retried batch does not write a second row.
8. Kafka admin tools (`kafka-topics`, `kafka-configs`, `kafka-consumer-groups`) work through the admin APIs.
9. Every path runs as a Postgres role, and the permission tests show the fences hold.
10. `pg_topics.health()` and the monitoring views report every signal in the design's Monitoring table.
11. The harnesses prove durability under `kill -9`, the DETACH CONCURRENTLY wait, ordering under load, and a failover.
12. All gates in section 4 pass, and every phase has its own commit.

The final acceptance check is the command list in Phase 13.

---

## 2. Out of scope

- Design "Order of work" item 3, the work queue. Its API document does not exist.
- The logical decoding spike (design "What was rejected" row 4).
- `SCRAM-SHA-256` auth. See Deviations D10.
- Kafka transactions, exactly once, follower fetch, and multi-broker features (the design excludes these).
- `Fetch` compression (the design defers it).
- A split of the stamper per band (the design defers it).
- One launcher for many databases (the design lists it as an open question).
- Upgrade scripts. This is version 0.1.0 and has no earlier version.
- The Supatype declaration syntax and the typed client.
- A publish throughput target. Phase 2 measures the number. It does not gate on it.

---

## 3. Decisions made with evidence

### 3.1 Kafka codec: use the `kafka-protocol` crate

Decision: use `kafka-protocol = "0.18.0"` with its default features. Do not write a codec.

Evidence (commands run in the scratchpad on 2026-09-27):

- `cargo info kafka-protocol` returned version 0.18.0, licence MIT/Apache-2.0, features `client, broker, gzip, zstd, snappy, lz4`.
- `cargo tree -e normal` returned 23 crates: `anyhow, bytes, crc, crc32c, flate2, indexmap, lz4 (lz4-sys), snap, uuid, zstd (zstd-sys)` and their children.
- A pgrx 0.12.9 cdylib with this crate built, and `cargo pgrx test pg17` returned `2 passed`.
  Inside a Postgres backend, a record batch v2 encode and decode round trip passed for all five codecs:

  ```
  none:   n=2 off=100 attr=0 json={"b":2} hdr_client=Some("probe")
  gzip:   n=2 off=100 attr=1 ...
  snappy: n=2 off=100 attr=2 ...
  lz4:    n=2 off=100 attr=3 ...
  zstd:   n=2 off=100 attr=4 ...
  ```

- The crate has request and response types for every API in the design. Version ranges from the crate source:

  | API | Crate range | API | Crate range |
  |---|---|---|---|
  | ApiVersions | 0-4 | OffsetCommit | 2-9 |
  | SaslHandshake | 0-1 | OffsetFetch | 1-9 |
  | SaslAuthenticate | 0-2 | InitProducerId | 0-5 |
  | Metadata | 0-13 | CreateTopics | 2-7 |
  | Produce | 3-13 | DeleteTopics | 1-6 |
  | Fetch | 4-18 | DescribeConfigs | 1-4 |
  | ListOffsets | 1-10 | AlterConfigs | 0-2 |
  | FindCoordinator | 0-6 | DeleteGroups | 0-2 |
  | JoinGroup | 0-9 | DescribeCluster | 0-2 |
  | SyncGroup | 0-5 | ListGroups | 0-5 |
  | Heartbeat | 0-4 | DescribeGroups | 0-6 |
  | LeaveGroup | 0-5 | | |

- The crate computes the batch CRC32C itself (`records.rs`, `crc32c(buf.range(content_start..batch_end))`).
- The crate handles classic and flexible framing. `RequestHeader`/`ResponseHeader` take a header version, and `ApiKey::request_header_version(v)` / `response_header_version(v)` give it.

Two gaps, both small:

1. The encoder never sets attribute bit 3 (LogAppendTime). Evidence: `attr=0` above for `timestamp_type: LogAppend`, and `records.rs` builds `attributes` from compression, transactional, control and delete-horizon only. A re-check with `grep timestamp_type records.rs` confirmed the encoder does not read it.
2. The encoder groups records into one batch only when `offset - sequence` is the same for each record. For a fetched batch, set `sequence = offset as i32`. Then the batch carries a real base sequence, which is wrong for a non-idempotent batch.

Fix for both: one core function `finish_fetch_batch(&mut [u8])`. It sets attribute bit 3, writes base sequence `-1` at byte 53, and recomputes the CRC32C over bytes 21 to the end. It has one unit test that decodes the result and checks `TimestampType::LogAppend` and the CRC. Add `crc32c` as a direct dependency (it is already in the tree).

Advertised versions: cap every API below the first version that uses topic IDs, because pg_topics has no topic IDs.
Start table (in `core/src/versions.rs`): ApiVersions 0-3, SaslHandshake 1-1, SaslAuthenticate 0-2, Metadata 0-12, Produce 3-9, Fetch 4-12, ListOffsets 1-7, FindCoordinator 0-4, JoinGroup 0-9, SyncGroup 0-5, Heartbeat 0-4, LeaveGroup 0-5, OffsetCommit 2-8, OffsetFetch 1-8, InitProducerId 0-4, CreateTopics 2-7, DeleteTopics 1-5, DescribeConfigs 1-4, AlterConfigs 0-2, IncrementalAlterConfigs 0-1, DeleteGroups 0-2, DescribeCluster 0-1, ListGroups 0-4, DescribeGroups 0-5.
The client matrix in Phases 6 and 13 can lower a ceiling. It cannot raise one above the crate range.

### 3.2 How the listener runs SQL as the client's role

Decision: one OS thread per client connection inside the listener background worker.
Each thread holds one `postgres` crate (`postgres = "0.19"`) connection to its own database over the loopback socket, opened with the client's SASL PLAIN user and password.
A blocking `Fetch` waits on `LISTEN pg_topics_stamped` on that connection. The stamper sends `pg_notify('pg_topics_stamped', '<schema>.<topic>')` in its batch transaction.

Evidence:

- In a pgrx 0.12.9 backend, a `std::thread` opened a `postgres::Client`, ran a query, ran `LISTEN`, received a `NOTIFY` from a second connection through `notifications().timeout_iter(3s)`, and returned: `probe: user=ricky notify=Some("hi")`.
- `system_user` shows how Postgres authenticated the connection. On a temp cluster (`--auth-local=trust --auth-host=scram-sha-256`): a trust connection returned `NULL`. A password connection returned `scram-sha-256:alice`. A wrong password returned `FATAL: password authentication failed for user "alice"`.

Why this approach:

- It is the simplest correct model. A thread reads a request, runs SQL, writes the response, in order. Kafka also processes one request per connection at a time, so ordering is correct with no extra code.
- The listener code lives in the core crate, and the core crate has no pgrx dependency. So the compiler proves that no thread calls a pgrx or Postgres server API.
- `LISTEN/NOTIFY` delivers at commit, so a woken `Fetch` always sees the stamped rows. It also replaces the shared latch, and the SQL consumer path already uses `NOTIFY` in the design. One wake path serves both.
- The payload names the topic, so a thread re-reads only when one of its topics changed. This removes the design's "every stamp wakes every blocked Fetch" cost.
- The idle connection holds no query while it waits. The design's cost model ("a long poll costs a connection, not a busy backend") stays true.

Safety rules (mandatory):

1. The loopback connection uses TCP: `hostaddr=127.0.0.1 port=<port>`. Never the Unix socket. A `local … trust` or `local … peer` line in `pg_hba.conf` then never matches.
2. After the connection opens, run `SELECT system_user`. Accept only a value that starts with one of `scram-sha-256:`, `md5:`, `password:`, `ldap:`, `pam:`, `radius:`. Every other value (`NULL`, `trust`, `peer`, `ident`, `cert`, `gss`, `sspi`) fails closed: close the connection and answer `SASL_AUTHENTICATION_FAILED`. Without this check, a `trust` line for 127.0.0.1 lets any Kafka client become any role.
3. The README states the requirement: `pg_hba.conf` must have a password line for `127.0.0.1/32`, for example `host all all 127.0.0.1/32 scram-sha-256`.

### 3.3 Kafka clients for tests

Local facts: `kcat`, `java` and Python `confluent_kafka` are not installed. Docker 26.1.2 runs natively and has network access.

Evidence:

- `docker pull confluentinc/cp-kafka:7.7.1` worked. The image has OpenJDK 17.0.12, `kafka-clients-7.7.1-ccs.jar`, the `jdk.compiler` module, and `kafka-console-producer`, `kafka-console-consumer`, `kafka-verifiable-producer`, `kafka-verifiable-consumer`, `kafka-consumer-groups`, `kafka-topics`, `kafka-configs`, `kafka-broker-api-versions`, `kafka-producer-perf-test`.
- `docker run python:3.12-slim pip install confluent-kafka` gave `2.15.1 ('2.15.1', ...)`, which bundles librdkafka 2.15.1.
- The Java image ran a single-file program against `Utils.murmur2` and printed key to band pairs, for example `bottle-1 4 2` and `a 1024 636`.

Light harness (Phase 6 onward):

- librdkafka: `bench/clients/python/Dockerfile` (`python:3.12-slim` plus `confluent-kafka==2.15.1`) and small Python scripts. Run with `--network host`.
- Java: the stock `confluentinc/cp-kafka:7.7.1` image CLI tools. Run with `--network host`.

Full matrix (Phase 13), all in Docker with `--network host`:

| Client | Image | Gate |
|---|---|---|
| Java 3.7 | `confluentinc/cp-kafka:7.7.1` CLI tools | Must pass |
| librdkafka 2.15 | `bench/clients/python` | Must pass |
| Confluent JS | `bench/clients/node` (`node:22-slim` plus `@confluentinc/kafka-javascript`) | Must pass |
| kafkajs | `bench/clients/node` (plus `kafkajs`) | Report only |
| franz-go | `bench/clients/go` (`golang:1.27-alpine` plus `github.com/twmb/franz-go`) | Report only |

Pin every client version in its Dockerfile or lock file.

### 3.4 TLS and auth

- TLS uses `rustls` 0.23 with the `ring` provider, in the core crate. pg_keyspace uses `rustls` 0.21 (`extensions/pg_keyspace/core/Cargo.toml`), and it builds in a pgrx cdylib. Version 0.23 is the maintained line.
- Settings: `pg_topics.tls_cert_file`, `pg_topics.tls_key_file`, `pg_topics.tls_use_postgres_cert` (the pg_keyspace pattern: fall back to `ssl_cert_file`/`ssl_key_file`, relative to the data directory, see `extensions/pg_keyspace/extension/src/lib.rs` near `ssl_cert_file`).
- Without a certificate, the listener does not bind. It reports the reason in `pg_stat_activity.query` for its backend. There is no plaintext mode, because every request needs a role and PLAIN needs TLS.
- The harness makes a self-signed certificate with `openssl req -x509 -newkey rsa:2048 -nodes -days 2 -subj /CN=localhost -addext subjectAltName=DNS:localhost,IP:127.0.0.1` (the pg_keyspace pattern, `bench/run_tls_inherit.sh`). Clients trust it as a CA file (`ssl.ca.location` for librdkafka and Confluent JS; `ssl.truststore.type=PEM` for Java).
- SCRAM: skip it. See Deviations D10.

### 3.5 Local environment: three blockers found, and the fixes

1. 117 crate directories under `~/.cargo/registry/src/index.crates.io-*` belong to `root` with mode `0640`. A build failed with `couldn't read .../fnv-1.0.7/lib.rs: Permission denied`. An earlier `sudo` harness run made them.
   Fix: the user runs `sudo chown -R ricky:ricky ~/.cargo/registry` once. Until then, export `CARGO_HOME=$HOME/.cache/pg_topics/cargo`. Both work. The scratch build used the second one.
2. `/usr/share/postgresql/17/extension` and `/usr/lib/postgresql/17/lib` belong to `root`. `cargo pgrx install --pg-config /usr/lib/postgresql/17/bin/pg_config` failed. `cargo pgrx init --pg17 download` failed with `configure: error: bison not found`.
   Fix: copy the system PostgreSQL 17 into a user-owned prefix. PostgreSQL computes its paths relative to its binary, so the copy works. Evidence: the copy's `pg_config --sharedir` printed the copy's path, and `cargo pgrx test pg17` passed against it with `PGRX_HOME` set to a private directory.
3. A Unix socket path above 107 bytes stops the pgrx test server (`Unix-domain socket path ... is too long (maximum 107 bytes)`). The repo path `extensions/pg_topics/extension/target/test-pgdata/.s.PGSQL.NNNNN` is about 85 bytes, so it is safe. Do not move `CARGO_TARGET_DIR` to a long path.

Warning: never run a pg_topics harness with `sudo`. It makes root-owned files in the cargo registry again.

### 3.6 Other facts checked

- `DETACH PARTITION ... CONCURRENTLY` waits for an open transaction that inserted into the partition. Evidence: a session inserted and slept 4 s. A detach started 1 s later took `3.009868187 s`, and the detached table held the committed row.
- `DETACH ... CONCURRENTLY` fails from a function: `ERROR: ALTER TABLE ... DETACH CONCURRENTLY cannot be executed from a function`. So SPI cannot run it. The partition worker must send it as a top-level statement on a client connection.
- Inside a `SECURITY DEFINER` function after `SET LOCAL ROLE tenant_a`, a pgrx function that reads `pg_sys::GetOuterUserId()` returned `tenant_a`, while `current_user` and `session_user` returned the owner. Output: `current/outer/session = ricky/tenant_a/ricky`. So `session_user` is wrong for PostgREST-style `SET ROLE`, and `GetOuterUserId()` is right.
- `SET LOCAL ROLE` inside a `SECURITY DEFINER` function fails: `ERROR: cannot set parameter "role" within security-definer function`. Inside a `SECURITY INVOKER` PL/pgSQL function called by a superuser, `SET LOCAL ROLE alice` then `RESET ROLE` worked and returned `alice/postgres`.
- Warning: that second result is the reason `SET ROLE` is not safe for the workers. `session_user` stays the superuser, so a tenant trigger, `CHECK` function or index function can run `RESET ROLE` and become the superuser. A design review proved this on PostgreSQL 17.11. Section 4.7 gives the mechanism that replaces it.
- pgrx runs a `#[pg_test]` function as `"tests"."<name>"()`. A test outside `mod tests` fails with `function tests.show_results() does not exist`. Put every `#[pg_test]` in `mod tests`.
- pg_keyspace has no `#[pg_test]`. Its extension tests are shell harnesses. pg_topics uses both.

---

## 4. Conventions for every phase

### 4.1 Code rules

- Schema rule (D19): every SQL object (table, function, view, sequence, type) lives in schema `topic`. GUCs keep the `pg_topics.` prefix. Where this plan writes `pg_topics.<function, table or view>`, read `topic.<name>`.

- LAW 1 (comments): write no code comments. The only exceptions:
  - one line where the logic is truly difficult, a reviewer reads it wrong without help, and the line states a constraint, an invariant, or a reason;
  - the reason for an error suppression, an `unsafe` block, or an `allow` attribute (one line);
  - text that a tool needs (`// SAFETY:` when clippy asks for it, `#[allow(...)]` reasons, the licence header if the repo needs one).
  Put every other explanation in the commit message. Do not copy the comment density of pg_keyspace.
- LAW 4 (least code): reuse repo helpers, the standard library, native Postgres features, and installed dependencies before new code. No interface with one implementation. No config value that never changes. Delete before add. Each non-trivial logic path keeps one runnable check.
- Write SQL logic in PL/pgSQL or SQL in `extension/sql/pg_topics.sql`. Write Rust only for the murmur2 hash, the caller identity, the background workers, and the Kafka listener.
- Every function in schema `pg_topics` (definer and invoker) has `SET search_path = pg_catalog, pg_temp` and schema-qualifies every object. Every worker session and the listener's loopback connection run `SET search_path = pg_catalog, pg_temp` at start.
- Every DDL string built from a name uses `format('%I')` or `quote_ident`. Never concatenate a name.
- A background worker never reads or writes a tenant table as a superuser, and never uses `SET ROLE` for it. Section 4.7 gives the only permitted mechanism.
- Error handling: no silent catch. The only swallowing catch is in the two event triggers, and it logs a `WARNING`.
- Rust: `cargo fmt`, `cargo clippy -D warnings`, no `unwrap()` on a value from the network or from SQL in the listener.

### 4.2 Layout

```
extensions/pg_topics/
  IMPLEMENTATION_PLAN.md
  LICENSE                      copy of extensions/pg_keyspace/LICENSE
  README.md                    written in Phase 11
  .gitignore                   copy of extensions/pg_keyspace/.gitignore, minus the C and redis lines
  core/                        pure Rust crate, package pg_topics_core, lib name pgt
    Cargo.toml
    src/lib.rs
    src/murmur2.rs             Kafka partitioner
    src/versions.rs            advertised API versions
    src/batch.rs               finish_fetch_batch
    src/listener/…             accept loop, TLS, SASL, request dispatch (Phase 6+)
    src/partitions.rs          partition worker tick over a postgres::Client (Phase 3)
    tests/golden.rs            murmur2 golden test
    testdata/murmur2_golden.tsv
  extension/                   pgrx 0.12.9 crate, package pg_topics
    Cargo.toml                 depends on core by path
    pg_topics.control
    src/lib.rs                 _PG_init, GUCs, workers, pg_externs, #[pg_test] mod tests
    src/bin/pgrx_embed.rs      ::pgrx::pgrx_embed!();
    sql/pg_topics.sql          schema, tables, functions (included with extension_sql_file!)
  bench/
    lib.sh                     shared harness helpers
    setup_local_pg.sh          user-owned PG17 copy + pgrx init
    gen_golden.sh, Golden.java golden file generator
    run_*.sh                   one harness per concern
    clients/python|node|go/    client images
.github/workflows/test-pg-topics.yml
```

The extension depends on the core crate through `pg_topics_core = { path = "../core" }`. pg_keyspace shares files through `#[path]`. A path dependency is standard Cargo and needs no second copy of the dependency list.

### 4.3 Environment for the gates

Run once per machine:

```
bash extensions/pg_topics/bench/setup_local_pg.sh
```

It copies `/usr/lib/postgresql/17`, `/usr/share/postgresql/17` and `/usr/include/postgresql/17` to `$HOME/.cache/pg_topics/pg17/usr/...`, if the copy does not exist. Then it runs `cargo pgrx init --pg17 <copy>/usr/lib/postgresql/17/bin/pg_config` with `PGRX_HOME=$HOME/.cache/pg_topics/pgrx`. It never edits `~/.pgrx`.

Export before every gate:

```
export PGRX_HOME=$HOME/.cache/pg_topics/pgrx
export PG_CONFIG=$HOME/.cache/pg_topics/pg17/usr/lib/postgresql/17/bin/pg_config
```

Also export `CARGO_HOME=$HOME/.cache/pg_topics/cargo` until the registry ownership is fixed (3.5 item 1).

### 4.4 Gates (run at the end of every phase)

```
cd extensions/pg_topics/core
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked

cd ../extension
cargo fmt --check
cargo clippy --locked --all-targets --no-default-features --features "pg17 pg_test" -- -D warnings
cargo pgrx test pg17

cd ..
for s in <every bench/run_*.sh that exists and is marked "gate">; do bash "$s" || exit 1; done
```

A harness marked "measure" prints numbers and exits 0. It is not a gate.
A phase is green only when every gate harness from every earlier phase still passes.

### 4.5 Harness rules

- Each `bench/run_*.sh` sources `bench/lib.sh`. It makes its own cluster with `initdb` in `mktemp -d`, on its own port, as the normal user. It installs the extension with `cargo pgrx install --pg-config "$PG_CONFIG"`. It removes the cluster on exit (`trap`).
- `lib.sh` gives `chk <name> <expected> <actual>` (PASS/FAIL lines, the pg_keyspace pattern from `bench/run_sql_publish.sh`), `start_pg`, `stop_pg`, `kill9_pg`, `psql_as <role> <sql>`, `make_cert`, `wait_for <cmd>`.
- The cluster config always has `shared_preload_libraries = 'pg_topics'`, `pg_topics.databases = 'postgres'`, `pg_topics.failover_is_fenced = on`. It does not need `track_commit_timestamp` (D20).
- `pg_hba.conf`: `local all all trust` for the harness superuser and the workers, `host all all 127.0.0.1/32 scram-sha-256` for the listener's loopback connections. No `host … trust` line.
- Exit code is 0 only when every `chk` passed.
- Docker clients run with `--network host` and `pg_topics.advertised_host = '127.0.0.1'`.

### 4.6 Commit rule

One commit per phase. Subject line up to 72 characters, for example `feat(pg_topics): stamper worker and durability harness`. Body up to 3 lines, only when the reason is not obvious. Ask the user before the first `git` command of the session.

### 4.7 Worker access to tenant tables (mandatory)

Every read or write of a tenant table by a worker runs in a restricted owner context. This covers:

- the stamp `UPDATE` of the queue table;
- the retention check for NULL offsets and the floor read;
- the sync read of the queue and the `MERGE` into the base table;
- `retry_errors` and every `_qe` insert.

Mechanism (the one `VACUUM` and `REFRESH MATERIALIZED VIEW` use):

1. Save the context with `pg_sys::GetUserIdAndSecContext(&mut uid, &mut ctx)`.
2. Call `pg_sys::SetUserIdAndSecContext(owner_oid, ctx | SECURITY_LOCAL_USERID_CHANGE | SECURITY_RESTRICTED_OPERATION)`. In this context Postgres refuses `SET ROLE`, `RESET ROLE` and `SET SESSION AUTHORIZATION`.
3. Run `SET LOCAL row_security = off` before the switch (it is refused inside a restricted operation). With it, a table with `FORCE ROW LEVEL SECURITY` raises an error instead of a silent filter. A silent filter would let retention drop acknowledged rows that it did not see.
4. Run the tenant-table SPI statements.
5. Restore the saved `uid` and `ctx`, also on error. Use one small Rust function `as_owner(owner_oid, || …)` with `PgTryBuilder` and `.finally(…)` for the restore. Transaction and subtransaction abort also restore the context, so the guard and the abort agree.

Control-table reads and writes run outside that context, as the superuser, in the same transaction.

So `stamp_topic`, `sync_topic` and `retention_check` are Rust `#[pg_extern]` functions, with `EXECUTE` revoked from `PUBLIC` (superuser only). `retry_errors` is also a Rust `#[pg_extern]`, but a tenant calls it: `EXECUTE` goes to `PUBLIC`, and it first checks `pg_has_role(caller(), <base owner>, 'member')`, then runs its writes through `as_owner`. A member of the owner role already holds the owner's rights, so this gives nothing new. PL/pgSQL keeps every function that touches no tenant table.

`as_owner` has exactly the callers listed above. It is not a general helper.

---

## 5. Data model (the design tables, with the deviations applied)

Schema `pg_topics`. The design's control tables keep their columns, except:

- `topic_config` adds `partition_interval interval NOT NULL DEFAULT '1 day' CHECK (partition_interval >= interval '1 minute')` (D6).
- `topic_config` adds `detaching regclass` (nullable): the partition that retention is detaching now (Phase 3).
- `topic_config` drops `seq_ordered_to` (D20) and `min_session_ms`, `max_session_ms` (D21).
- `topic_groups` is keyed by `group_name` only, not by topic, and adds `expired_members bigint NOT NULL DEFAULT 0` (D4, D12).
- `topic_group_members` is keyed by `(group_name, member_id)`, and adds `joined_generation integer`: the generation for which the member last sent `JoinGroup`.
- `topic_groups`, `topic_group_members`, `topic_offsets`: `SELECT` granted to `PUBLIC`, with row level security `USING (pg_has_role(current_user, owner_role, 'member'))`, so a tenant reads its own lag in SQL. `INSERT`, `UPDATE`, `DELETE` stay revoked.
- `topic_offsets` keeps `(schema_name, topic, group_name, band)` and references `topic_groups (group_name)` and `topic_band_position (schema_name, topic, band)`, both `ON DELETE CASCADE`.
- Queue table name rule: schema and table match `^[A-Za-z0-9_-]{1,63}$`, the table ends in `_q`, and the table is at most 47 characters (a partition adds `_p` plus 14 digits).
- Partition name: `<topic>_p<YYYYMMDDHH24MISS of the lower bound, UTC>`.
- Queue table triggers, created by `create_topic`:
  - `BEFORE INSERT FOR EACH ROW WHEN (NEW.log_offset IS NOT NULL OR NEW.published_by <> current_user)`: raise an error. The `WHEN` clause runs in C, so a normal insert does not call the function.
  - `BEFORE INSERT FOR EACH STATEMENT`: a `SECURITY DEFINER` function reads the topic's `topic_config` row, refuses the insert when `backlog_age > max_backlog_age`, and raises `synchronous_commit` with `set_config(..., true)` to the topic floor. It never lowers the setting.
- Band `CHECK (band BETWEEN 0 AND <band_count - 1>)`, built from a validated integer.
- `UPDATE (band_count)` on `topic_config` is revoked from every role, including the owner (the design).

---

## 6. Phases

Complexity: **simple** means a mechanical change that follows a pattern that already exists in this plan or repo. **complex** means concurrency, protocol state, security, or a new pattern.

### Phase 0 — Scaffold, partitioner, harness base (simple)

Goal: an empty extension that builds, installs, passes CI, and has the Kafka partitioner pinned by a golden file.

Deliverables:

- `core/Cargo.toml` (`pg_topics_core`, lib `pgt`, edition 2021, licence PostgreSQL), `core/src/lib.rs`, `core/src/murmur2.rs`.
- `core/testdata/murmur2_golden.tsv`: columns `key<TAB>band_count<TAB>band`. At least 200 keys: ASCII, empty string, 1 to 40 characters, multi-byte UTF-8, keys of length 1 to 7 mod 4 (tail bytes). Band counts 1, 2, 3, 4, 7, 16, 1024.
- `bench/Golden.java` and `bench/gen_golden.sh`: runs `docker run --rm -v ...:/g confluentinc/cp-kafka:7.7.1 java -cp "/usr/share/java/kafka/*" /g/Golden.java > core/testdata/murmur2_golden.tsv`. It computes `Utils.toPositive(Utils.murmur2(key.getBytes(UTF_8))) % n`. The file is committed. CI does not regenerate it.
- `core/tests/golden.rs`: reads the file and checks every row.
- `extension/Cargo.toml` (copy the pg_keyspace shape: `crate-type = ["cdylib", "lib"]`, `[[bin]] pgrx_embed_pg_topics`, features `pg14`..`pg17`, `pg_test`, `default = ["pg17"]`, `pgrx = "=0.12.9"`, `pgrx-tests = "=0.12.9"`, `panic = "unwind"`).
- `extension/pg_topics.control`: `default_version = '0.1.0'`, `module_pathname = '$libdir/pg_topics'`, `relocatable = false`, `schema = pg_topics`, `superuser = true`, `trusted = false`.
- `extension/src/lib.rs`: `pg_module_magic!`, `_PG_init` with the `process_shared_preload_libraries_in_progress` guard (the pg_keyspace pattern, `extension/src/lib.rs` line 1545), `pg_topics.band_for(key text, band_count int) RETURNS int` (immutable, strict, calls core), `mod tests`, `pg_test` module with `postgresql_conf_options() -> vec!["track_commit_timestamp = on"]`.
- `extension/sql/pg_topics.sql` (empty schema file, included with `extension_sql_file!`).
- `LICENSE` (copy), `.gitignore` (copy, minus `*.o`, `*.bc`, `dump.rdb`).
- `bench/lib.sh`, `bench/setup_local_pg.sh`.
- `.github/workflows/test-pg-topics.yml`: copy the `changes` job and the `unit-and-build` job shape from `test-pg-keyspace.yml` with the filter `^(extensions/pg_topics/|\.github/workflows/test-pg-topics\.yml$)`. Jobs:
  - `unit-and-build`: core fmt, clippy, test; PGDG install of PostgreSQL 17 plus `clang libclang-dev llvm-dev`; `cargo install cargo-pgrx --version 0.12.9 --locked`; `bash bench/setup_local_pg.sh`; extension fmt, clippy, `cargo pgrx test pg17`.
  - `harnesses`: the same setup, then each gate harness as the runner user, no `sudo`. Later phases add lines here.
  - The "untouched" success step for both jobs, as in pg_keyspace.

Acceptance checks:

1. `cargo test --locked` in `core` passes the golden test for every row.
2. `cargo pgrx test pg17` passes `band_for_matches_golden_sample` (5 rows from the golden file through SQL) and `band_for_rejects_bad_count` (`band_count` 0 and 1025 raise an error).
3. `SELECT pg_topics.band_for('bottle-1', 4)` returns `2` (from the Java run in 3.3).

Tests that must exist: `core/tests/golden.rs`; `#[pg_test] band_for_matches_golden_sample`; `#[pg_test] band_for_rejects_bad_count`.

Gates: section 4.4 (no harness yet).

Commit: `feat(pg_topics): scaffold the extension and pin Kafka's partitioner`.

### Phase 1 — Storage, publish and the stamp function (complex)

Goal: the queue table, all control tables, `create_topic`, SQL `publish`, and `stamp_topic`. This phase retires the riskiest assumptions: that the stamper gives gap-free, visibility-ordered offsets, that the caller identity is correct inside `SECURITY DEFINER`, and that the restricted owner context (4.7) works from pgrx.

Simplest design: the stamp is one Rust `#[pg_extern]` with a few SPI statements and the `as_owner` switch (4.7). The worker in Phase 2 only calls it in a loop. Remove `track_commit_timestamp = on` from the Phase 0 `postgresql_conf_options()`, because nothing needs it now (D20).

Deliverables in `sql/pg_topics.sql` and `src/lib.rs`:

- The six control tables (section 5), with `fillfactor = 50` on `topic_band_position` and `topic_config`.
- `pg_topics.caller() RETURNS name` in Rust: returns the name of `GetOuterUserId()`. Every control plane fence uses it (D8).
- `pg_topics.create_topic(topic text, band_count int DEFAULT 4, retention interval DEFAULT '7 days', min_durability text DEFAULT 'durable', partition_interval interval DEFAULT '1 day') RETURNS void`. `SECURITY DEFINER`. It:
  - splits `schema.table` on the first dot and validates both parts (section 5);
  - checks that `caller()` has `CREATE` on the schema;
  - refuses `durable` and `replicated` when `pg_topics.failover_is_fenced` is off, and `replicated` when `synchronous_standby_names` is empty;
  - creates the queue table owned by `caller()`, the partial `(seq) WHERE log_offset IS NULL` index and the BRIN `(log_offset)` index on the parent, the two triggers, the current partition and the next partition, and the unique `(band, log_offset) WHERE log_offset IS NOT NULL` index on each leaf;
  - inserts `topic_config` and one `topic_band_position` row per band.
- `pg_topics.publish(topic text, value jsonb, key text DEFAULT NULL, headers jsonb DEFAULT NULL) RETURNS void`. `SECURITY INVOKER`. The band is `band_for(key, band_count)` when the key is not NULL. When the key is NULL, the band is a per-backend round robin counter (D9). It does a plain `INSERT` as the caller.
- `pg_topics.stamp_topic(schema_name text, topic text, max_rows int DEFAULT 10000) RETURNS int` (rows stamped). Rust `#[pg_extern]`, `EXECUTE` revoked from `PUBLIC`. Only the superuser worker (and the superuser test) calls it. In one transaction it:
  1. as the superuser: `SET LOCAL row_security = off`, then lock the topic's `topic_band_position` rows `FOR UPDATE` and read `next_offset` per band;
  2. inside `as_owner(<queue owner oid>)`: select unstamped rows with `WHERE log_offset IS NULL ORDER BY seq LIMIT max_rows` (served by the partial `(seq)` index), and set `log_offset = next_offset[band] + row_number() OVER (PARTITION BY band ORDER BY seq) - 1` in one `UPDATE … FROM`;
  3. as the superuser: write `next_offset`, `stamped_by` and `backlog_age`;
  4. send `pg_notify('pg_topics_stamped', schema_name || '.' || topic)` when it stamped at least one row.
  `stamped_by` is `system_identifier || '/' || timeline_id` from `pg_control_system()` and `pg_control_checkpoint()`. When the old value differs, raise a `WARNING` (D11).
  `backlog_age` is `now() - published_at` of the lowest-`seq` row still unstamped after the batch, or `0`.
- Grants: revoke all on the control tables from `PUBLIC`, except the `SELECT` grants in section 5. Grant `EXECUTE` on `publish`, `band_for`, `caller` to `PUBLIC`.

Warning: the `LIMIT` must come after `ORDER BY xact, seq`. Offsets follow `(xact, seq)` order among visible rows (D20). An arbitrary `LIMIT` would break the causal rule.

Acceptance checks (all `#[pg_test]` unless marked):

1. `create_topic('public.bottles_q', 4)` makes the table, 2 partitions, 4 band rows, the `CHECK (band BETWEEN 0 AND 3)`.
2. `create_topic` refuses `a.b.c_q`, `public.bottles` (no `_q`), a 48-character table, band count 0 and 1025, and `public."x;drop"` with a clear error, and makes nothing.
3. Publish 10 rows over 2 bands, stamp: each band has offsets `0..n-1` with no gap, and `next_offset = n`.
4. Stamp with `max_rows = 1` twice: offsets follow `seq` order. Use two committed transactions in a harness, because a `#[pg_test]` runs inside one transaction (see check 8).
5. A caller-supplied `log_offset` or `published_by` on a raw `INSERT` fails.
6. `publish` fails when `backlog_age > max_backlog_age` (set `backlog_age` by hand in the test).
7. `caller()` inside a `SECURITY DEFINER` function after `SET LOCAL ROLE r` returns `r`.
8. Harness `bench/run_stamp_order.sh` (gate): two psql sessions. Session A inserts and commits. Then session B inserts and commits. `stamp_topic(..., 1)` twice gives A offset 0 and B offset 1. An open, uncommitted insert is not stamped, and the next committed row still gets the next offset with no gap.

9. `#[pg_test] stamp_runs_tenant_code_as_owner`: a tenant `BEFORE UPDATE` trigger on the queue table records `current_user` into a tenant table. After a stamp, the recorded value is the queue owner.
10. `#[pg_test] stamp_refuses_forced_rls`: `ALTER TABLE … FORCE ROW LEVEL SECURITY` with a policy that hides rows makes `stamp_topic` raise an error. It stamps nothing and hides nothing.

Tests that must exist: `create_topic_builds_objects`, `create_topic_refuses_bad_names`, `stamp_is_gap_free_per_band`, `raw_insert_cannot_forge_offset`, `publish_refuses_on_backlog`, `caller_sees_set_role_inside_definer`, `publish_uses_kafka_partitioner` (key `bottle-1` on 4 bands lands in band 2), `stamp_runs_tenant_code_as_owner`, `stamp_refuses_forced_rls`, and `bench/run_stamp_order.sh`.

Commit: `feat(pg_topics): queue tables, SQL publish and the stamp function`.

### Phase 2 — Stamper worker, durability, ordering and throughput (complex)

Goal: design items 4, 1 (durability and ordering part) and 2.

Simplest design: `pg_topics.databases` (a comma list, postmaster context) names the databases. `_PG_init` registers four static workers per named database with `BackgroundWorkerBuilder`, `set_restart_time(Some(5s))`, `enable_spi_access()`, and the database name as the argument (D5). This phase adds the stamper only. Each later phase adds its own worker.

Stamper loop (`pg_topics_stamper_main`):

1. Connect to the database as the bootstrap superuser. Sleep and retry while `pg_topics` is not installed in the database.
2. Every loop: read the topic list. For each topic, in its own transaction:
   - take `pg_try_advisory_lock(<pg_topics namespace int>, hashtext(schema || '.' || topic))`; skip the topic when another stamper holds it;
   - call `stamp_topic` (it does the owner switch, 4.7); release the lock.
3. Sleep on the latch for 1 ms when a topic stamped rows, else 50 ms. Exit on `SIGTERM`.
4. On an error, log it, back off 1 s, and continue. The postmaster restarts a crashed worker after 5 s.

Deliverables: the worker in `src/lib.rs`, GUC `pg_topics.databases`, harnesses below.

Acceptance checks:

1. Harness `run_stamper.sh` (gate): publish 1000 rows over 3 topics. All rows have offsets within 2 s. `kill -9` the stamper process: it restarts, and publishing continues with no gap and no duplicate offset. A `pg_notify` arrives on a `LISTEN pg_topics_stamped` session.
2. Harness `run_durability.sh` (gate, design item 1): a client publishes in a loop at `durable` and writes each acknowledged id to a file. The harness sends `kill -9` to the postmaster at 5 random times, restarts, and checks that every acknowledged id is present exactly once and that every row gets an offset after the restart.
3. Harness `run_ordering.sh` (gate, design item 1): 16 concurrent publishers, each with its own key and a per-key counter in `value`. Each publisher waits for its acknowledgement before the next publish. After all rows are stamped, per key, `log_offset` order equals counter order. Also check the causal rule: a row acknowledged before another row's transaction started has the lower offset when both are in one band.
4. Harness `run_publish_throughput.sh` (measure, design item 2): `pgbench -n -c {1,4,8} -T 30 -f publish.sql` for `relaxed` and `durable`. It prints events per second and the steady `backlog_age`. It also prints the stamper rate: rows stamped per second at the largest client count. It also prints the catch-up time: hold the stamper's advisory lock, publish 1 000 000 rows, release the lock, and time until `backlog_age` is `0`.
5. Harness `run_stamper_split.sh` (gate, design item 11 part): set `stamped_by` to `'other/1'` by hand, publish, and check that the stamper log has the `WARNING` for a changed node identity.

Tests that must exist: the five harnesses above; `#[pg_test] databases_guc_parses` only if the list parsing is in Rust.

Commit: `feat(pg_topics): the stamper worker, with durability and ordering proofs`.

### Phase 3 — Time partitions and retention (complex)

Goal: design item 5 and the DETACH CONCURRENTLY proof from item 1.

Simplest design: the partition worker keeps a `postgres::Client` over the Unix socket as the bootstrap superuser, because `DETACH ... CONCURRENTLY` cannot run through SPI (3.6). This connection issues DDL and control-table statements only. It never runs `SET ROLE` or `RESET ROLE`, and it never reads a tenant table directly. The two tenant-table reads go through the Rust function `pg_topics.retention_check`, which uses `as_owner` (4.7). The tick logic is `core/src/partitions.rs` and takes `&mut postgres::Client`. The worker main thread calls it and then waits on the latch for 10 s.

Tick, per topic, each step in its own transaction or savepoint, so one topic's failure does not stop the others:

1. Create partitions until three future partitions exist after the current one. Creation runs first on every tick. In one transaction: `CREATE TABLE … PARTITION OF … FOR VALUES …`, then `ALTER TABLE … OWNER TO <queue owner>`, then the unique `(band, log_offset) WHERE log_offset IS NOT NULL` index on the leaf. The index build then runs as the owner.
2. Retention uses `topic_config.detaching` as its only record of work in progress. It never finds a table to drop by its name. A table that the user detached is never touched.
   1. When `detaching` is NULL: pick the oldest partition whose upper bound is older than `now() - retention_interval`. Write its oid to `detaching` and commit.
   2. When `detaching` names a table that no longer exists, set `detaching` to NULL.
   3. When the table is still attached: `SET lock_timeout = '1s'`, then `ALTER TABLE … DETACH PARTITION … CONCURRENTLY` as a top-level statement. On a lock timeout, retry next tick. When `pg_inherits.inhdetachpending` is true for it: run `… DETACH PARTITION … FINALIZE`.
   4. When the table is detached: call `pg_topics.retention_check(detaching)`. It returns the count of rows with `log_offset IS NULL`. When the count is above 0, run `ALTER TABLE … ATTACH PARTITION …`, set `detaching` to NULL, and raise a `WARNING`.
   5. Else, in one transaction: lock the topic's `topic_band_position` rows `FOR UPDATE`; read the floor per band with `pg_topics.retention_floor(schema, topic)` (Rust, `as_owner`, `coalesce(min(log_offset), next_offset)` over the surviving partitions); `DROP TABLE` the detached table; set each band's `oldest_offset` to the floor; set `detaching` to NULL.
   Every tick runs steps 2.2 to 2.5 when `detaching` is not NULL, so a crash at any point resumes on the next tick, not only at start.
3. Delete `topic_producers` rows older than 1 day (D13), and `Empty` groups with no members older than `offset_retention` when it is set.
4. Every 360 ticks (about 1 hour), run `pg_topics.check_duplicates(schema, topic, full => true)` and raise a `WARNING` for each duplicate.

Warning: the worker is a superuser for DDL. Before this phase is done, prove with a test that `CREATE TABLE … PARTITION OF`, `DETACH`, `ATTACH` and `DROP` run no tenant function as the superuser. Use a tenant `CHECK` constraint, a tenant expression index and a tenant trigger on the parent, each calling a function that records `current_user`. If `ATTACH` runs an inherited `CHECK` function or an index function as the superuser, the reattach must run through a Rust function that uses `as_owner` instead.

Deliverables: worker `pg_topics_partition_main`, `core/src/partitions.rs`, Rust functions `pg_topics.retention_check(regclass) RETURNS bigint` and `pg_topics.retention_floor(schema text, topic text) RETURNS TABLE(band smallint, floor bigint)` (both superuser-only `EXECUTE`), SQL functions `pg_topics.check_duplicates(schema text, topic text, full bool DEFAULT false) RETURNS TABLE(band smallint, log_offset bigint, copies bigint)` (newest partition only when `full` is false).

Acceptance checks:

1. Harness `run_retention.sh` (gate): a topic with `partition_interval = '1 minute'` and `retention = '2 minutes'`. After the worker runs, 3 future partitions exist, old partitions are dropped, and `oldest_offset` equals the lowest surviving offset per band. A consumer read below `oldest_offset` returns zero rows (the Kafka error comes in Phase 6).
2. Same harness, DETACH wait proof: session A inserts into the oldest partition and stays open. The worker's detach does not complete while A is open, and publishes to other partitions keep working. After A commits, A's row is stamped before the drop, or the drop waits for it.
3. Same harness, crash proof: kill the worker with `kill -9` when the log shows the detach line. On the next tick, the table named in `detaching` is checked and dropped, and `oldest_offset` is correct.
4. A partition with an unstamped row (stop the stamper with a held advisory lock) is reattached, not dropped.
5. `check_duplicates` returns zero rows on a clean topic and one row after a forced duplicate in a second partition.
6. A partition that the user detached by hand, with an expired range and a name that matches the pattern, is still present after 3 ticks.
7. A band with no surviving stamped rows gets `oldest_offset = next_offset`.
8. `FORCE ROW LEVEL SECURITY` with a policy that hides every row makes `retention_check` raise an error, and the partition is not dropped.
9. The DDL proof from the warning above: the recorded `current_user` is never a superuser.

Tests that must exist: `run_retention.sh` (checks 1 to 9); `#[pg_test] check_duplicates_finds_cross_partition_copy`; one `core` unit test for the partition name and bound arithmetic.

Commit: `feat(pg_topics): partition worker with detach, check and drop retention`.

### Phase 4 — Sync worker, base table and error table (complex)

Goal: design item 6.

Simplest design: one `MERGE` per batch per topic, built from a cached column list, and `jsonb_populate_record` for the cast from `value` to the base row type. Postgres does the type work. The sync worker serves every synced topic in turn. The queue read, the `MERGE` and the `_qe` insert run inside `as_owner(<base owner oid>)` (4.7), with `row_security = off` set before the switch.

Deliverables:

- `pg_topics.attach(base regclass, sync_key text, band_count int DEFAULT 4, retention interval DEFAULT '7 days', min_durability text DEFAULT 'durable') RETURNS text` (the topic name). It creates `<base>_q` when missing (through `create_topic`), creates `<base>_qe` with its index, sets `sync_table`, `sync_key`, `sync_enabled = true`, and inserts the reserved group `__pg_topics_sync:<schema>.<topic>` with the base owner as `owner_role`. The sync position starts at each band's `oldest_offset`.
- `pg_topics.create_table_topic(table_name text, columns jsonb, sync_key text, …)`: `columns` is `{"name": "type", …}`. Each type passes `to_regtype()` first. It runs `CREATE TABLE` with `%I %s` built from the validated type name, adds `event_at timestamptz NOT NULL`, then calls `attach`. The `CREATE TABLE` runs as `caller()`.
- Event triggers `pg_topics_ddl_end` (`ddl_command_end`) and `pg_topics_sql_drop` (`sql_drop`), as in the design. `SECURITY DEFINER`, pinned search path, the whole body in `BEGIN … EXCEPTION WHEN OTHERS THEN RAISE WARNING …`.
- `pg_topics.sync_topic(schema text, topic text, max_rows int DEFAULT 1000) RETURNS int`. Rust `#[pg_extern]`, `EXECUTE` revoked from `PUBLIC`, the `stamp_topic` pattern: control-table statements as the superuser, tenant-table statements inside `as_owner`. The queue owner and the base owner can differ; the queue read runs as the queue owner and the writes as the base owner. Per batch:
  1. read the group position per band and `shape_version`; rebuild the column cache when the version changed; stop the sync (set `sync_enabled = false`, `WARNING`) when `sync_key` no longer names a column;
  2. read stamped rows above the position, in offset order;
  3. reduce to one row per sync key: newest `published_at`, tie broken by higher `(band, log_offset)`;
  4. run the `MERGE` under a savepoint:
     - `WHEN MATCHED AND r.value IS NULL AND t.event_at < r.published_at THEN DELETE`;
     - `WHEN MATCHED AND t.event_at < r.published_at THEN UPDATE SET col = CASE WHEN r.value ? 'col' THEN (jsonb_populate_record(NULL::base, r.value)).col ELSE t.col END, …, event_at = r.published_at`;
     - `WHEN NOT MATCHED AND r.value IS NOT NULL THEN INSERT …`;
  5. on an error, redo the batch one record per savepoint, for at most the first 50 records (above 64 open subtransactions Postgres overflows the subtransaction cache). A record that fails goes to `_qe` with the error text. The position then advances past those 50 only, and the next batch tries the bulk `MERGE` again;
  6. advance the group position in `topic_offsets`.
  Both `sync_topic` and `retry_errors` take the same transaction advisory lock per topic, so they never run at the same time (D14).
- `pg_topics.retry_errors(topic text) RETURNS TABLE(retried int, failed int)`: Rust, owner-fenced as in 4.7. It runs each `_qe` row through the same `MERGE`, in groups of at most 50 savepoints per transaction. It deletes a row that succeeds and updates `error`, `failed_at` for a row that fails again.
- Worker `pg_topics_sync_main`: the stamper loop shape, calling `sync_topic` for each topic with `sync_enabled`.

Acceptance checks (`#[pg_test]` calls `stamp_topic` and `sync_topic` directly):

1. A record with fields `bottle_id, name, abv` and an extra field upserts one row. The extra field is ignored.
2. Two updates to one `bottle_id` in one batch give one row with the newest data (no "cannot affect row a second time" error).
3. An older record after a newer one (other band) does not overwrite the row.
4. A tombstone deletes the row (see Assumption A2 for the key).
5. A missing `NOT NULL` field and a bad cast (`abv = "strong"`) go to `_qe`. The batch continues, and the good records are written.
6. `ALTER TABLE bottles ADD COLUMN colour text` bumps `shape_version`, and the next batch writes `colour`.
7. `DROP TABLE bottles` stops the sync and does not break the `DROP`. A `CREATE TABLE` by a role with no topics still works when the event trigger body fails (force it with a broken `topic_config` row in the test).
8. After the cause is fixed, `retry_errors` deletes the fixed rows. A row that still fails gets a new `failed_at`.
9. `#[pg_test] sync_runs_tenant_code_as_owner`: a tenant trigger on the base table records `current_user`, and the value is the base owner.
10. `#[pg_test] sync_fallback_caps_savepoints`: a batch of 200 records where every record fails writes 50 `_qe` rows in one call and advances the position by 50.
11. Harness `run_sync.sh` (gate): the worker keeps the base table current for 10 000 random updates over 100 keys on 4 bands. The final table equals the newest record per key.

Tests that must exist: one `#[pg_test]` per check 1 to 10, and `run_sync.sh`.

Commit: `feat(pg_topics): sync worker, error table and retry_errors`.

### Phase 5 — Control plane, SQL consume and group functions (complex)

Goal: all control plane functions, the SQL consumer path, and the group coordinator as SQL functions that the listener calls in Phases 7 to 9.

Simplest design: the group coordinator is PL/pgSQL over `topic_groups` and `topic_group_members`, driven by its callers (D4). There is no coordinator process. Every group function starts with `INSERT INTO topic_groups … ON CONFLICT DO NOTHING` (only `group_join` creates a group), then `SELECT … FROM topic_groups WHERE group_name = $1 FOR UPDATE`. That row lock serializes every change to one group. Each call is a short transaction; no caller holds the lock while it waits for a held response. After the lock, each call runs `expire_members(group)` and closes a join window that is due. A state change sends `pg_notify('pg_topics_group', group_name)`.

Deliverables:

- Control plane (each `SECURITY DEFINER`, each checks `pg_has_role(caller(), <table owner>, 'member')`, each raises SQLSTATE `42501` on refusal so the listener can map it to `POLICY_VIOLATION` or an authorization error):
  `drop_topic(topic)` (with `lock_timeout = '2s'` and 3 retries), `set_retention(topic, interval)`, `set_sync(topic, base regclass, sync_key text)`, `set_durability(topic, tier)`, `set_sync_enabled(topic, bool)`, `set_backlog_limit(topic, interval)`, `delete_group(group_name)`. Every change is an `UPDATE`, never a delete and insert.
- SQL consumer:
  - `pg_topics.fetch(topic text, band int, from_offset bigint, max_rows int DEFAULT 500, filter jsonb DEFAULT NULL) RETURNS TABLE(log_offset bigint, key text, value jsonb, headers jsonb, published_at timestamptz)`. `SECURITY INVOKER`, so row level security applies. `filter` is applied as `value @> filter`, a bound parameter (D15). It raises `OFFSET_OUT_OF_RANGE` (custom SQLSTATE `PT001`) when `from_offset < oldest_offset`.
  - `pg_topics.band_offsets(topic text) RETURNS TABLE(band smallint, oldest_offset bigint, next_offset bigint)`. `SECURITY DEFINER`. It checks `has_table_privilege(caller(), queue, 'SELECT')`.
  - `pg_topics.offset_for_time(topic text, band int, ts timestamptz) RETURNS bigint`: the lowest `log_offset` with `published_at >= ts`. It prunes partitions.
- GUCs `pg_topics.group_min_session_ms` (int, default 6000) and `pg_topics.group_max_session_ms` (int, default 1800000), `Suset` (D21).
- Groups:
  - `group_join(group_name, member_id, client_id, session_ms, rebalance_ms, protocol_type, protocols jsonb) RETURNS record`: returns `MEMBER_ID_REQUIRED` plus a new member id when `member_id` is empty (JoinGroup v4 and later need this). It refuses a group name that starts with `__pg_topics_sync:` (reserved for the sync, Phase 4). It checks `session_ms` against `pg_topics.group_min_session_ms` and `pg_topics.group_max_session_ms` (D21), sets the member's `joined_generation` to the group's current `generation_id`, moves the group to `PreparingRebalance`, and returns the current state.
  - `group_join_poll(group_name, member_id) RETURNS record`: closes the window when every member with `joined_generation` = the last generation has rejoined (its `joined_generation` equals the current value), or when the largest `rebalance_ms` passed. It then removes the members that did not rejoin, increments `generation_id`, picks the leader, sets `CompletingRebalance`, and returns the join result (the member list for the leader).
  - `group_sync(group_name, member_id, generation, assignments jsonb)`, `group_sync_poll(...)`, `group_heartbeat(...)`, `group_leave(...)`, `expire_members(group_name)` (scans rows with `last_heartbeat_at < now() - pg_topics.group_max_session_ms` through the index, then re-checks each against its own timeout, and adds to `expired_members`).
  - `commit_offset(topic, group_name, band, offset, generation) RETURNS text` (the Kafka error name or `NONE`): the design's fenced upsert, the read-back with three cases, the generation check on both success branches, the cap at `next_offset`, and a foreign key violation mapped to `UNKNOWN_TOPIC_OR_PARTITION`.
  - `fetch_offset(topic, group_name, band) RETURNS bigint`.
  - A group with `owner_role` = the `caller()` that created it. Every group function checks `pg_has_role(caller(), owner_role, 'member')` and returns `GROUP_AUTHORIZATION_FAILED` on refusal.

Acceptance checks (`#[pg_test]`):

1. Each control plane function refuses a non-owner and works for the owner and for a member of the owner role.
2. `drop_topic` removes the table, the partitions, and every control row, including a group that committed an offset (the cascade the design warns about).
3. `fetch` returns rows in offset order, respects `filter`, and raises `PT001` below `oldest_offset`.
4. The commit fence: a stale generation returns `ILLEGAL_GENERATION`; a repeat returns `NONE`; a late superseded retry returns `NONE`; a retry after a rebalance returns `ILLEGAL_GENERATION`; a commit above `next_offset` is capped.
5. A first commit with a claimed generation that does not match the group writes nothing.
6. Join window: three members join; the window closes only when all three have joined or the timeout passed; one generation increment; one leader.
7. A member with an old heartbeat is expired, the group goes to `PreparingRebalance`, and `expired_members` goes up.
8. Harness `run_sql_consumer.sh` (gate): two SQL consumers in one group over psql sessions (join, poll, sync with a hand-made assignment, fetch, commit). One leaves, and the other gets all bands after the rebalance.
9. Same harness, concurrent join: 8 parallel psql sessions call `group_join` and then `group_join_poll` in a loop on a new group. Exactly one generation increment happens, exactly one member is the leader, and every member gets the same `generation_id`.
10. `group_join` refuses the name `__pg_topics_sync:public.bottles_q`.

Tests that must exist: one `#[pg_test]` per check 1 to 7 and 10, and `run_sql_consumer.sh` (checks 8 and 9).

Commit: `feat(pg_topics): control plane, SQL consumer and group coordinator functions`.

### Phase 6 — Kafka listener, part 1: produce and plain consume (complex)

Goal: design item 7, first half: `ApiVersions`, `SaslHandshake`, `SaslAuthenticate`, `Metadata`, `Produce`, `Fetch`, `ListOffsets`.

Simplest design: section 3.2. Core modules:

- `listener/mod.rs`: `run(cfg, shutdown: Arc<AtomicBool>)`. A non-blocking `TcpListener` polled every 50 ms. Each accepted socket goes to a new thread with a blocking socket.
- `listener/conn.rs`: TLS handshake (rustls `StreamOwned`), frame read (4-byte length, refuse above `max_message_bytes + 64 KiB`), header decode with the right header version, dispatch, response encode.
- Pre-auth limits (constants, D16): at most 64 connections that have not authenticated, and a 10 s read timeout until SASL completes.
- `max_clients`: a counter of authenticated connections. Above the limit, close the connection after `SaslAuthenticate`.
- SASL: `SaslHandshake` offers `PLAIN` only after TLS (always true here, since TLS is mandatory). `SaslAuthenticate` opens the `postgres::Client` with `hostaddr=127.0.0.1 port=<port> dbname=<db> user=<u> password=<p>` (TCP, never the Unix socket), runs `SET search_path = pg_catalog, pg_temp`, checks `system_user` against the allowlist (3.2), runs `LISTEN pg_topics_stamped`.
- A request before authentication, other than `ApiVersions`, `SaslHandshake`, `SaslAuthenticate`, closes the connection.
- `ApiVersions`: from `versions.rs`. An unknown version gets the v0 response with `UNSUPPORTED_VERSION`, as Kafka does.
- `Metadata`: one broker (`node_id 0`, `advertised_host`, `port`). Topics: queue tables in `topic_config` where `caller` has `SELECT` or column `INSERT` on the table. Partitions = bands, leader 0. Auto-create is refused.
- `Produce`: per partition batch, decode with `RecordBatchDecoder` (all five codecs), refuse a batch above `max_message_bytes` (`MESSAGE_TOO_LARGE`). In one transaction on the client connection:
  - `SET LOCAL synchronous_commit` from `acks` (`-1` gives `on`, or `remote_apply` when `synchronous_standby_names` is set; `0` and `1` give `off`);
  - `SET LOCAL statement_timeout` = the request `timeout_ms` for the `INSERT` only, then `SET LOCAL statement_timeout = 0` before `COMMIT` (the design: no safe timeout on the commit). Do not use `RESET`: it restores a role-level `ALTER ROLE … SET statement_timeout`, which would then apply to the `COMMIT`;
  - one `INSERT … SELECT FROM unnest($band, $keys::text[], $values::text[]::jsonb[], $headers::jsonb[], $ts::timestamptz[])`;
  - `COMMIT`.
  Key: refuse bytes that are not UTF-8 or longer than 40 characters. Value: Postgres parses it as `jsonb`; SQLSTATE `22P02`, `22021` or `22001` maps to `INVALID_RECORD`. Headers: a JSON array of `{"key": …, "value": …}`, or `{"key": …, "value_base64": …}` for a value that is not UTF-8 (D17). A record timestamp of `-1` becomes a NULL `producer_timestamp`. `acks = 0` sends no response.
  Response `base_offset`: the `log_offset` of the first row, after the stamp (D18), `log_append_time_ms` = the `published_at` of the first row.
- `Fetch`: per requested band, `SELECT log_offset, key, value::text, headers, published_at FROM <queue> WHERE band = $1 AND log_offset >= $2 AND log_offset IS NOT NULL ORDER BY log_offset LIMIT $3`, as the client role. Build one batch per band with `RecordBatchEncoder` (no compression), then `finish_fetch_batch`. Respect `partition_max_bytes` and `max_bytes`, but always return at least the first record. `high_watermark` = `next_offset`, `log_start_offset` = `oldest_offset` (from `band_offsets`). `PT001` maps to `OFFSET_OUT_OF_RANGE`. When the total is below `min_bytes`, wait on `notifications().timeout_iter(min(max_wait_ms remaining, 5 s))` and re-read only when a payload names a requested topic.
- `ListOffsets`: `-2` gives `oldest_offset`, `-1` gives `next_offset`, a timestamp gives `offset_for_time`.
- Error map (design table): SQLSTATE `42501` to `TOPIC_AUTHORIZATION_FAILED`; unknown topic or band to `UNKNOWN_TOPIC_OR_PARTITION`; `PT001` to `OFFSET_OUT_OF_RANGE`; anything else to `UNKNOWN_SERVER_ERROR` with the Postgres error in the log.
- Extension side: worker `pg_topics_listener_main`. It connects with SPI once to read `pg_topics.port`, `advertised_host`, `max_clients`, `max_message_bytes` and the TLS settings, and to report status with `pgstat_report_activity`. It starts `pgt::listener::run` on a thread and waits on the latch until `SIGTERM`. Port 0 means no listener. A bind failure is reported as `bind failed: <error>` in its `pg_stat_activity.query`, and the worker then sleeps.
- GUCs: `pg_topics.port` (int, default 9092, `Suset`, so `ALTER DATABASE … SET` works), `pg_topics.advertised_host` (default `localhost`), `pg_topics.max_clients` (default 100), `pg_topics.max_message_bytes` (default 1048576), `pg_topics.failover_is_fenced` (bool, default off), the three TLS settings.

First slice (spike, do it before the rest of the phase):

1. Check that a worker reads a per-database `ALTER DATABASE postgres SET pg_topics.port = N` value. NOT VERIFIED today. If it does not, read `pg_db_role_setting` through SPI instead.
2. Produce with Java (`kafka-console-producer`, default idempotence **off** for this spike: `enable.idempotence=false`) and with librdkafka, and check that both accept `base_offset = -1`. If a client refuses `-1`, switch to this fallback: after `COMMIT`, wait for the stamp notification, read the `log_offset` of the first row by its primary key `(published_at, seq)` from `INSERT … RETURNING`, and return it.

Acceptance checks:

1. `core` unit tests: frame parse refuses an oversize length; `finish_fetch_batch` output decodes with `LogAppend` and a valid CRC; every advertised version is inside the crate range and below the topic ID cutoffs; the error map.
2. Harness `run_kafka_basic.sh` (gate):
   - a Java `kafka-console-producer` over `SASL_SSL` with PLAIN writes 100 records; `kafka-console-consumer --from-beginning --partition N --offset 0` reads them back, with `check.crcs=true` (the Java default);
   - librdkafka (Python) produces 100 keyed records with `partitioner=murmur2_random` and each key lands on `band_for(key, n)`;
   - librdkafka produces with each of `none`, `gzip`, `snappy`, `lz4`, `zstd`;
   - a value that is not JSON gets `INVALID_RECORD`; a 41-character key gets `INVALID_RECORD`; a batch above the limit gets `MESSAGE_TOO_LARGE`;
   - a wrong password fails auth; a role without `SELECT` gets `TOPIC_AUTHORIZATION_FAILED` on `Fetch`;
   - a `pg_hba.conf` line with `trust` for 127.0.0.1 makes PLAIN auth fail (the `system_user` check), and a `local all all trust` line has no effect on the listener;
   - a producer role with `ALTER ROLE … SET statement_timeout = '3s'` produces with `acks=all`. With `log_statement = 'all'`, the server log shows `SET LOCAL statement_timeout = 0` as the last statement before each `COMMIT` of that role;
   - a blocking `Fetch` with `fetch.wait.max.ms=5000` returns within 200 ms after a publish;
   - a consumer below `oldest_offset` gets `OFFSET_OUT_OF_RANGE` and resets by `auto.offset.reset=earliest`;
   - `ListOffsets` earliest, latest and by time return the right offsets;
   - 70 idle TCP connections that never authenticate: the 65th and later are closed, and a real client still connects;
   - two databases on one port: the second listener reports `bind failed` in `pg_stat_activity`.
3. Harness `run_listener_restart.sh` (gate): `kill -9` the listener worker during traffic. It restarts, and clients reconnect.

Tests that must exist: the `core` unit tests above, `run_kafka_basic.sh`, `run_listener_restart.sh`, `bench/clients/python/Dockerfile` and its scripts.

Commit: `feat(pg_topics): Kafka listener with produce, fetch and list offsets`.

### Phase 7 — Kafka listener, part 2: consumer groups (complex)

Goal: design item 7, second half: `FindCoordinator`, `JoinGroup`, `SyncGroup`, `Heartbeat`, `LeaveGroup`, `OffsetCommit`, `OffsetFetch`.

Simplest design: each handler calls the Phase 5 SQL function on the client connection. A held `JoinGroup` or `SyncGroup` response waits on `LISTEN pg_topics_group` plus a 100 ms poll of `group_join_poll` / `group_sync_poll`. The Java client opens a second connection to the coordinator, so a held response does not block its `Fetch`.

Deliverables: the seven handlers, `FindCoordinator` returning node 0 (v4 batched keys too), `OffsetFetch` v8 batched groups, `OffsetCommit` one statement per band.

Acceptance checks:

1. Harness `run_kafka_groups.sh` (gate):
   - three librdkafka consumers in one group on a 6-band topic each get 2 bands; one closes, and the other two get 3 each after one rebalance;
   - a consumer paused longer than `session.timeout.ms` is expired, and the rest rebalance;
   - Java `kafka-console-consumer --group g` reads, commits, and on restart continues from the committed offset;
   - `enable.auto.commit=true` works;
   - `cooperative-sticky` and `range` assignors both reach `Stable`.
   The stale generation fence is proved at the SQL level in Phase 5 check 4. The handler only maps the returned name to the Kafka error code, and a `core` unit test covers that map.
2. `core` unit test: a `JoinGroup` v4 request with an empty member id returns `MEMBER_ID_REQUIRED` plus an id (handler level, with a fake SQL result).

Tests that must exist: `run_kafka_groups.sh`, the unit test.

Commit: `feat(pg_topics): Kafka consumer groups over the SQL coordinator`.

### Phase 8 — InitProducerId and idempotent producers (complex)

Goal: design item 8.

Simplest design: `InitProducerId` takes `nextval('pg_topics.producer_id_seq')` and epoch 0, through a `SECURITY DEFINER` SQL function. `Produce` with a producer id at or above 0 calls `pg_topics.produce_check(schema, topic, producer_id, epoch, band, first_seq, last_seq)` in the same transaction as the `INSERT`:

- It locks the five `topic_producers` rows for `(topic, producer_id, band)` `FOR UPDATE`.
- A row with `first_sequence = first_seq` is a retry: return `DUPLICATE` and the primary key of that row's first queue row. The handler then rolls back the savepoint of its insert and answers success with the `log_offset` of that queue row (D18).
- `first_seq` is not the next expected sequence of the newest row: return `OUT_OF_ORDER_SEQUENCE_NUMBER`. The next expected sequence is `last_sequence + 1`, and it wraps from `2147483647` to `0`, as in Kafka.
- No row yet: accept any `first_seq` (Kafka accepts a new producer state after its state expired).
- Else write the batch into the next ring slot (oldest evicted first), with the primary key of the first queue row of the batch (`base_published_at`, `base_seq`, D18) and `updated_at = now()`.

Acceptance checks:

1. `#[pg_test]`: ring order over 7 batches keeps the newest 5; a retry of batch 3 of 5 in flight matches by `first_sequence`; a gap returns `OUT_OF_ORDER_SEQUENCE_NUMBER`; a 200-record batch retry matches on its first sequence; a batch with `last_sequence = 2147483647` accepts a next batch with `first_sequence = 0` (`#[pg_test] producer_sequence_wraps`).
2. Harness `run_kafka_idempotent.sh` (gate):
   - Java `kafka-verifiable-producer --producer.config` with `enable.idempotence=true`, `acks=all` writes 10 000 records, and the count in the table is exactly 10 000;
   - inject retries: the harness drops the listener's TCP connection mid-stream (restart the listener worker with `kill -9`) and the final count still equals the acknowledged count, with no duplicate;
   - librdkafka with `enable.idempotence=true` gives the same result.
3. The design's open question ("does a modern Java producer hard fail when `InitProducerId` is refused?") gets its answer recorded in the commit message, from one run with the handler returning `UNSUPPORTED_VERSION` for `InitProducerId` on a local branch that is not committed.

Tests that must exist: the `#[pg_test]` set above and `run_kafka_idempotent.sh`.

Commit: `feat(pg_topics): idempotent producers`.

### Phase 9 — Admin APIs (simple)

Goal: design item 9: `CreateTopics`, `DeleteTopics`, `DescribeConfigs`, `AlterConfigs`, `DeleteGroups`, `DescribeCluster`, `ListGroups`, `DescribeGroups`.

Simplest design: each handler calls a Phase 1 or Phase 5 SQL function as the client role. The fences apply with no extra code.

Mapping:

- `CreateTopics`: name `schema.table_q`, `num_partitions` gives `band_count` (`-1` gives 4), replication factor 1 or `-1` gives the requested tier; a replication factor N from 2 to 1 + k, where k is the number of synchronous standbys that `synchronous_standby_names` requires (`topic.sync_copies`), gives the `replicated` tier when `pg_topics.failover_is_fenced` is on; any other N gives `INVALID_REPLICATION_FACTOR` with the number of copies the standbys back (D26), config `retention.ms` and `pg_topics.min_durability`, any other config gives `INVALID_CONFIG` naming the key. `validate_only` runs the checks in a transaction that it rolls back.
- `DeleteTopics`: `drop_topic`.
- `DescribeConfigs` (topic resource): `retention.ms`, `cleanup.policy=delete` (read only), `message.timestamp.type=LogAppendTime` (read only), `max.message.bytes` (read only), `pg_topics.min_durability`, `pg_topics.replication_factor` (read only: 1 + k for a `replicated` topic, else 1). Broker resource: `advertised.listeners`-style entries are not needed; return an empty list.
- `AlterConfigs`: `retention.ms` to `set_retention`, `pg_topics.min_durability` to `set_durability`; any other key gives `INVALID_CONFIG` naming it.
- `DeleteGroups`: `delete_group`.
- `DescribeCluster`: one broker, cluster id = `system_identifier` as text, controller 0.
- `ListGroups`: groups whose `owner_role` the caller is a member of.
- `DescribeGroups`: state, protocol, members with their opaque metadata and assignment. The sync group shows as `Empty` with its committed offsets through `OffsetFetch`.

Acceptance checks:

1. Harness `run_kafka_admin.sh` (gate), with the Java CLI tools:
   - `kafka-topics --create --topic public.orders_q --partitions 6 --replication-factor 1` works; `--replication-factor 3` fails with `INVALID_REPLICATION_FACTOR` (the harness has no standby); `--topic orders` gives an error naming the `schema.table_q` rule;
   - `kafka-topics --describe` shows 6 partitions;
   - `kafka-configs --alter --add-config retention.ms=3600000` works; `cleanup.policy=compact` fails with `INVALID_CONFIG`;
   - `kafka-consumer-groups --list` and `--describe --group g` show members and lag; the sync group shows its lag;
   - `kafka-consumer-groups --delete --group g` works;
   - `kafka-topics --delete` removes the topic and every control row;
   - `kafka-broker-api-versions` lists exactly the table in `versions.rs`.

Tests that must exist: `run_kafka_admin.sh`.

Commit: `feat(pg_topics): Kafka admin APIs`.

### Phase 10 — Permissions across every path (complex)

Goal: design item 10. Five paths agree: SQL, the listener, the stamper, the sync worker, the control plane.

Deliverables:

- The control-table grants and row policies from section 5 exist from Phase 1. This phase proves them. The group functions are `SECURITY DEFINER` and fence with `caller()`, so the policy only governs direct `SELECT`.
- `pg_topics.grant_publish(topic text, role name)` and `pg_topics.grant_consume(topic text, role name)`: owner-fenced helpers that run `GRANT INSERT (band, key, value, headers, producer_timestamp)` and `GRANT SELECT` on the queue table (D1). Two small helpers, because a hand-written whole-table `GRANT INSERT` is the easy mistake, even with the trigger guard.
- A lint in `run_security.sh`: every function in schema `pg_topics` (definer and invoker) has `search_path=pg_catalog, pg_temp` in `proconfig`; no function is owned by a role other than the extension owner; no role in the database has `BYPASSRLS` because of pg_topics.
- The listener maps `42501` from a group function to `GROUP_AUTHORIZATION_FAILED`, and from a control plane function to `POLICY_VIOLATION` for admin APIs.

Acceptance checks:

1. Harness `run_security.sh` (gate):
   - tenant A cannot publish to, read, alter, drop, or commit on tenant B's topic or group, through SQL and through Kafka (`TOPIC_AUTHORIZATION_FAILED`, `GROUP_AUTHORIZATION_FAILED`, `POLICY_VIOLATION`);
   - a role with only `grant_publish` can publish but cannot set `log_offset`, `published_by` or `published_at`, and cannot read;
   - a role with only `grant_consume` can fetch but cannot publish;
   - a member of an owner role passes the fences;
   - a tenant trigger on its own queue table and on its own base table runs as the tenant during stamping and syncing (`current_user` recorded by the trigger equals the owner, not a superuser);
   - escape attempts: a tenant trigger that runs `RESET ROLE`, and one that runs `SET ROLE postgres`, on the queue table (fires during the stamp) and on the base table (fires during the sync). Each fails with an error. The stamp or sync of that topic fails and retries, other topics continue, and no tenant statement runs as a superuser (a tenant function records `current_user` and `(SELECT rolsuper FROM pg_roles WHERE rolname = current_user)`, and the second value is always false);
   - the same escape attempt through a tenant `CHECK` function and a tenant expression index function on the queue table, during the stamp;
   - a tenant can `SELECT` its own rows in `topic_offsets` and sees no row of another tenant;
   - DDL injection: `create_topic('public."a""; DROP TABLE x; --_q"')` and a `columns` entry with type `int; DROP TABLE x` fail and drop nothing;
   - a role cannot `UPDATE pg_topics.topic_offsets` directly;
   - a PostgREST-style session (login as `authenticator`, `SET LOCAL ROLE tenant_a`) is fenced as `tenant_a`;
   - the lint above passes.

Tests that must exist: `run_security.sh`; `#[pg_test] offsets_policy_hides_other_tenants`; `#[pg_test] tenant_trigger_cannot_reset_role_during_stamp`; `#[pg_test] tenant_trigger_cannot_reset_role_during_sync`.

Commit: `feat(pg_topics): permission fences on every path`.

### Phase 11 — Monitoring views, health() and README (simple)

Goal: the design's Monitoring section and the user documentation.

Deliverables (views in schema `pg_topics`, `SELECT` granted to `pg_monitor`):

| View or column | Source |
|---|---|
| `stamp_backlog` | `topic_config.backlog_age` |
| `oldest_xact` | `pg_stat_activity` `min(xact_start)` |
| `detach_waiting` | `pg_stat_activity` where `query ILIKE '%DETACH PARTITION%'` and `wait_event_type = 'Lock'`, plus pending detaches in `pg_inherits.inhdetachpending` |
| `write_partition_dead_tuples` | `pg_stat_user_tables.n_dead_tup` for the current partition |
| `partition_headroom` | upper bound of the newest partition minus `now()` |
| `worker_headroom` | `max_worker_processes` minus background workers in `pg_stat_activity` |
| `listener_status` | `pg_stat_activity.query` of the listener backend |
| `group_expiry` | `topic_groups.expired_members` |
| `error_rows` | row count per `_qe` table, and rows with `failed_at > now() - interval '1 hour'` |
| `producer_rows` | `topic_producers` count per topic |
| `syncrep_waiters` | `pg_stat_activity` where `wait_event = 'SyncRep'` |
| `sync_lag` | `next_offset - committed_offset` for the sync group |
| `consumer_lag` | `next_offset - committed_offset` per group and band |
| `duplicate_offsets` | `check_duplicates(…, false)` per topic |

`pg_topics.health()` returns one row per topic with those values and `ok boolean`. `ok` is false when a threshold is crossed: backlog above `max_backlog_age / 2`, headroom below `least(interval '1 day', partition_interval)`, listener not bound, a duplicate, any SyncRep waiter.

`README.md`: prerequisites (the design table plus `pg_topics.databases`), the SQL API, the grants, the `pg_hba.conf` requirement (a password line for `127.0.0.1/32`, 3.2), the group-name risk (section 9 item 4), the Kafka client settings (SASL_SSL, PLAIN, `partitioner=murmur2_random` for librdkafka, see A3), the deviations, and the design's warnings. Written in plain short English.

Acceptance checks:

1. Harness `run_monitoring.sh` (gate): stop the stamper (hold its advisory lock) and `health()` reports the backlog; make a `_qe` row and `error_rows` counts it; a `pg_monitor` role reads every view; a role without `pg_monitor` is refused.
2. `#[pg_test] health_returns_one_row_per_topic`.

Commit: `feat(pg_topics): monitoring views and health()`.

### Phase 12 — Failover and replica reads (complex)

Goal: design item 11, and the unfenced failover part of item 1.

Deliverables: harness `run_failover.sh` (gate), as the normal user:

1. A primary and a streaming standby (`pg_basebackup -R`) on two ports.
2. Publish 1000 rows and stamp them. On the standby, a SQL `fetch` reads the history, and `pg_last_wal_replay_lsn()` is reported.
3. Promote the standby. The workers start on it (they wait for recovery to end), the stamper continues offsets with no gap, and the stamper logs the changed `stamped_by` `WARNING` (new timeline).
4. Unfenced case: keep the old primary running and publish on both. Check that both timelines assigned the same offsets and that each side's stamper logs the node change on its first stamp after the split. The harness prints the split point from the commit timestamps. This proves the detection signal. It does not repair anything (the design).
5. A commit through `commit_offset` on the new primary cannot exceed that band's `next_offset` there.

Commit: `test(pg_topics): failover, split detection and replica reads`.

### Phase 13 — End-to-end client matrix and final review (complex)

Goal: every must-pass client works; the whole system is ready for the user's review.

Deliverables:

- `bench/clients/node/` (`node:22-slim`, `@confluentinc/kafka-javascript`, `kafkajs`, pinned in `package-lock.json`) and `bench/clients/go/` (`golang:1.27-alpine`, `franz-go`, pinned in `go.sum`).
- `bench/run_client_matrix.sh` (gate for Java, librdkafka, Confluent JS; report only for kafkajs and franz-go). Per client:
  1. produce 1000 keyed JSON records with headers over SASL_SSL PLAIN;
  2. check each key's band equals `band_for` (librdkafka and Confluent JS with `partitioner=murmur2_random`);
  3. consume them in a two-member group and commit;
  4. restart and check the group resumes at the committed offset;
  5. Java with `check.crcs=true` reads every batch.
- `bench/run_soak.sh` (measure): 10 minutes of mixed produce and consume with the Java perf tools. It prints throughput, p99 produce latency, `backlog_age`, and dead tuples.
- CI: add every gate harness to the `harnesses` job. Docker is on `ubuntu-latest`.
- Update the "Open questions" answers found during the work in the final summary for the user: the stamper rate, the throughput numbers, the advertised version floor, the `InitProducerId` answer.

Final acceptance (run in this order, all must pass):

```
bash extensions/pg_topics/bench/setup_local_pg.sh
(cd extensions/pg_topics/core && cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked)
(cd extensions/pg_topics/extension && cargo fmt --check && cargo clippy --locked --all-targets --no-default-features --features "pg17 pg_test" -- -D warnings && cargo pgrx test pg17)
for s in run_stamp_order run_stamper run_durability run_ordering run_stamper_split run_retention run_sync \
         run_sql_consumer run_kafka_basic run_listener_restart run_kafka_groups run_kafka_idempotent \
         run_kafka_admin run_security run_monitoring run_failover run_replicated run_client_matrix; do
  bash extensions/pg_topics/bench/$s.sh || { echo "FAIL $s"; exit 1; }
done
bash extensions/pg_topics/bench/run_publish_throughput.sh
bash extensions/pg_topics/bench/run_soak.sh
```

Commit: `test(pg_topics): end-to-end Kafka client matrix`.

---

## 7. Deviations from the design

| # | Design says | This plan does | Reason |
|---|---|---|---|
| D1 | Nobody gets plain `INSERT`; a publisher gets `EXECUTE` on a `SECURITY DEFINER` publish function | `publish` is `SECURITY INVOKER`. A publisher gets column `INSERT (band, key, value, headers, producer_timestamp)`. Triggers refuse a caller `log_offset` or `published_by`, enforce the backlog limit, and raise `synchronous_commit` on every insert path | A single `EXECUTE` grant cannot say which topic a role may publish to. Column privileges are native and per topic. The triggers keep every rule the design wanted on raw inserts too, so less code is trusted |
| D2 | The listener runs a non-blocking event loop and a blocking `Fetch` waits on a latch set by the stamper | One thread per client; the wait is `LISTEN/NOTIFY` on the client's own connection | Simpler, ordered per connection, one wake path shared with SQL consumers, fewer false wakes. The thread code is in a crate with no pgrx dependency |
| D3 | The stamper sets the listener's latch | The stamper sends `pg_notify('pg_topics_stamped', topic)` | Follows from D2. Delivered at commit, so a woken reader sees the rows |
| D4 | Groups are keyed by `(schema_name, topic, group_name)`; the coordinator lives in the listener | Groups and members are keyed by `group_name`; `topic_offsets` stays per topic and band. The coordinator is PL/pgSQL driven by its callers | A Kafka group can subscribe to many topics, and `JoinGroup` does not name the topics (the subscription is opaque). SQL functions give one coordination model for SQL and Kafka consumers, as the design asks |
| D5 | One listener per database; how workers find databases is not stated | `pg_topics.databases` names the databases that get the four workers | A background worker must pick its database before it connects. The design names "nominate which databases may host topics" as a way out |
| D6 | Partition width is not stated | `topic_config.partition_interval`, default 1 day | Retention granularity equals partition width, and tests need minute partitions |
| D7 | The stamper holds `BYPASSRLS` and the sync worker writes every base table, each under its own `NOLOGIN` role | Workers connect as the bootstrap superuser. Every tenant-table statement runs inside `SetUserIdAndSecContext(owner, SECURITY_LOCAL_USERID_CHANGE \| SECURITY_RESTRICTED_OPERATION)` with `row_security = off` (4.7). The partition worker stays superuser for DDL only and gives each new partition to the queue owner. No `BYPASSRLS` role exists | Tenant code then runs as that tenant and cannot `SET ROLE` or `RESET ROLE` out, the same protection `VACUUM` uses. `SET ROLE` is not safe: `session_user` stays superuser, so tenant code can `RESET ROLE`. This removes the two most powerful roles and their blast radius |
| D8 | Fences check `pg_has_role(current_user, …)` | Fences check `pg_has_role(pg_topics.caller(), …)`, where `caller()` returns `GetOuterUserId()` | Inside `SECURITY DEFINER`, `current_user` is the function owner (checked: `ricky/tenant_a/ricky`). `session_user` is wrong for PostgREST `SET ROLE` |
| D9 | A round robin counter per topic for null keys | A round robin counter per backend | No shared state and no extra object per topic. Kafka's own spread is per producer, not per topic |
| D10 | `SCRAM-SHA-256` is optional | Not built | It needs table-wide read on `pg_authid`. PLAIN over TLS is the design's default |
| D11 | A changed `stamped_by` is a monitored signal | The stamper raises a `WARNING` on a change; `health()` reports duplicates | The design keeps no history column, and a legal failover also changes the value. The duplicate check is the authoritative signal |
| D12 | A count of members expired by missed heartbeat | `topic_groups.expired_members` counter | The signal needs state that the design tables do not hold |
| D13 | Reap `topic_producers` by `updated_at`; interval not stated | Ring rows are reaped after 1 day, and a `topic.producer_ids` row after 7 days with no use (constants) | Kafka's `producer.id.expiration.ms` is 1 day and `transactional.id.expiration.ms` is 7 days. One value each, no setting |
| D14 | `retry_errors` and the sync share one `INSERT … ON CONFLICT` statement | They share one `MERGE` and one row lock on the topic's reserved sync group row | `ON CONFLICT … DO UPDATE` cannot see which fields the record carried. `MERGE` can, and it also does the tombstone delete in the same statement |
| D15 | Server side filter as typed parameters against named fields | `value @> filter` with `filter` as a bound `jsonb` parameter | Native, parameterised, and served by a GIN index. Equality only; ranges are not in v1 |
| D16 | Cap connections that wait to authenticate and drop them after a few seconds | Constants: 64 pending, 10 s | One value each, no setting |
| D17 | `headers jsonb`; format not stated | A JSON array of header objects, in record order. A UTF-8 value is `{"key": k, "value": "text"}`, any other value is `{"key": k, "value_base64": "..."}`, and a null value is `{"key": k, "value": null}`. `Fetch` gives back the exact bytes. A header key that is not UTF-8 gives `CORRUPT_MESSAGE` for the batch | Kafka allows repeated header names and keeps their order, so an object loses data. JSON cannot hold raw bytes, and some frameworks send binary values. The protocol defines a header key as a string |
| D18 | `Produce` answers a retry with "the original offset"; `topic_producers.base_offset` is "the offset the client was told" | `Produce` returns the real `base_offset` of each partition batch. Before `COMMIT`, `topic.produced_row` finds the primary key of the first row of the batch. After `COMMIT`, the listener waits on `pg_topics_stamped` until `topic.produced_offset` returns that row's `log_offset`, for at most the request `timeout_ms` (30 s when `timeout_ms` is 0 or less); then it answers `REQUEST_TIMED_OUT`. When `produced_row` finds no row (a tenant `BEFORE` trigger dropped or moved it), the listener skips the ring write and answers `base_offset = -1` with no error. `acks = 0` does not wait. While it waits, the listener reads and commits the next `Produce` requests of the same connection (at most 5 open answers). Before it reads each request, it sends every answer at the front of the queue that is ready, in request order. So a later request can hold back an earlier answer by at most its own run time; a late answer still carries the real offset when the row is stamped. For 100 ms after a topic had work, the worker polls that topic every 1 ms; it lists and polls all topics every 50 ms. `topic_producers` stores the primary key of the first row (`base_published_at`, `base_seq`), and a `DUPLICATE` answer reads that row's `log_offset` | The user decided it. Both functions run the queue query as the queue owner and return only rows with `published_by = caller()`, so a role with only `grant_publish` gets its offsets and no other row's. A client computes `base_offset + i`, so the stamper keeps the rows of one transaction together in each band (D20) |
| D19 | `topic.publish(...)` in one example, `pg_topics.health()` in another | Every SQL object lives in schema `topic`: `topic.publish(...)`, `topic.health()`, `topic.topic_config` | Postgres refuses a schema name that starts with `pg_` (SQLSTATE `42939`) unless `allow_system_table_mods` is on, and a generic extension cannot ask for that |
| D20 | The stamper orders a batch by `pg_xact_commit_timestamp(xmin)`, then `seq`; `track_commit_timestamp = on`; clock must slew; `seq_ordered_to` for truncated timestamps | Each queue row stores `xact xid8 DEFAULT pg_current_xact_id()`, the top-level transaction id (also inside a savepoint). The stamper orders by `(xact, seq)` over the partial index `(xact, seq) WHERE log_offset IS NULL`, so the rows of one transaction get consecutive offsets in each band. It takes `max_rows` rows and then the rest of the last transaction. It cuts the last transaction only when its `xact` is below `pg_snapshot_xmin(pg_current_snapshot())`. `track_commit_timestamp` is not needed, the clock rule goes, and `seq_ordered_to` is removed | The stamper sees only visible rows, so offsets still follow visibility order. A transaction acknowledged before another started has the lower `xact`, because xids are assigned in order, so causal order holds. The exact rule: offsets follow the order in which each transaction first wrote to the queue. A SQL transaction that writes, then reads the committed rows of another transaction, then writes again, can get offsets below those rows (`run_stamp_order.sh` shows it). To avoid it, publish from a transaction that does not write to the queue before it reads. A Kafka `Produce` request is its own transaction and gets its xid at the insert, so Kafka producers get append order. A cut is safe only when no older transaction is open, because then no new row can sort before the rest of the cut transaction. A raw insert that sets `xact` fails. `backlog_age` is the age of the first unstamped row in `(xact, seq)` order. The first row of each transaction is its oldest row, and the first rows follow xid order, so this is the oldest unstamped row. There are two exceptions: a race of microseconds between the `published_at` default and the xid assignment, and a cut transaction, whose remaining rows can be younger than the first row of a later transaction by at most the time the cut transaction took to insert `max_rows` rows. The stamp sets `enable_seqscan`, `enable_bitmapscan`, `enable_hashjoin`, `enable_mergejoin` and `jit` off in its own GUC nest level (`NewGUCNestLevel`, `AtEOXact_GUC`), only after the pending check finds work, so the settings end when `stamp_topic` returns. The estimates grow with the queue, not with the batch, and at 600 000 rows the planner chose a sequential scan of the queue and a 40 ms JIT compile for each stamp. It removes an O(backlog²) sort and three failure modes |
| D21 | `topic_config.min_session_ms`, `max_session_ms` per topic | GUCs `pg_topics.group_min_session_ms` (6000) and `pg_topics.group_max_session_ms` (1800000) | Groups are not per topic (D4). Kafka also sets these per broker (`group.min.session.timeout.ms`) |
| D22 | Loopback to Postgres with the client's credentials; transport not stated | TCP to `127.0.0.1` only, and `system_user` must show a password-style method | A `local` trust or peer line then never matches, and any other method fails closed |
| D23 | Retention recovers a crashed detach by scanning for tables that match the partition naming | `topic_config.detaching` records the target before the detach, and every tick resumes from it | A name match could drop a table the user detached on purpose |
| D26 | `CreateTopics` refuses any replication factor but 1 | A replication factor N is accepted when N <= 1 + k, where k is the number of synchronous standbys that `synchronous_standby_names` requires (`FIRST k`, `k (...)` or `ANY k`; a plain list is 1), and `pg_topics.failover_is_fenced` is on. The topic gets the `replicated` tier. `Metadata` still lists node 0 as the only replica; the read-only config `pg_topics.replication_factor` reports 1 + k from the current setting. `bench/run_replicated.sh` is the gate | The user decided it. The design's rule stays true: N is accepted only when Postgres keeps N copies of each commit, so a 3 never means fewer. Node ids for standbys would be brokers that do not exist: librdkafka 2.15.1 `describe_topics` shows them with no host and port 0 |

---

## 8. Assumptions

- A1: On a sync `UPDATE`, a field missing from the record keeps the column's current value. On an `INSERT`, a missing field is NULL. This reads "the sync only writes the columns that match a field in the record" as the rule and the "written as null" row as the insert case.
- A2: A tombstone (key present, value NULL) has no fields, so it cannot carry the sync key. The record key is taken as the sync key value for a tombstone, cast to the sync key column type.
- A3: librdkafka's default partitioner is `consistent_random`, not Kafka's murmur2. NOT VERIFIED locally; librdkafka's CONFIGURATION.md states it. Phase 6 check 2 uses `partitioner=murmur2_random`, and the README tells librdkafka users to set it when SQL publishers and Kafka producers must agree on a key's band.
- A4: A background worker applies `ALTER DATABASE … SET` values at connect. NOT VERIFIED; Phase 6 slice 1 checks it and names the fallback.
- A5: The Java client accepts a PEM CA file through `ssl.truststore.type=PEM`. NOT VERIFIED; if not, the harness makes a PKCS12 truststore with `keytool` from the same image.

## 9. Decisions taken (the user reviews these at the end)

1. A1 stands: on a sync update a missing field keeps its value; on an insert it is NULL.
2. A2 stands: the record key identifies the base row for a tombstone.
3. `CARGO_HOME` stays private: `$HOME/.cache/pg_topics/cargo`.
4. The group namespace is one per database, as in one Kafka cluster. Any role that may call `group_join` can take a free group name first. The README states this risk and tells tenants to prefix group names with their schema.

Open questions for the requester: none.

## 10. Known limits accepted for v1

- `DETACH … CONCURRENTLY` needs a local superuser connection over the Unix socket. If `pg_hba.conf` forbids it, retention stops and `health()` shows headroom and backlog. Stamping continues.
- A delete by tombstone leaves no marker, so an older update that arrives later on another band inserts the row again.
- A pending detach completes only with `DETACH … FINALIZE`, and FINALIZE waits for every older snapshot in the database. A long query anywhere in the database stops retention on that topic until the query ends.
- The oldest expired partition that holds an unstamped row is attached again, and retention holds that topic for one `retention_interval` (`topic_config.retention_hold_until`). Newer expired partitions wait behind it. A late row therefore stays readable for at least one retention interval after it gets its offset.
- The sync compares `event_at` only. Two records for one sync key with the same `published_at` in different bands can resolve in either order across batches. Inside one batch the tie goes to the higher `(band, log_offset)`.
- D24: the sync reads the base table columns from `pg_attribute` on each batch. `shape_version` and its column cache are removed, because the event trigger does not see every DDL that changes a table's shape (inheritance parents, domains), and its `topic_config` write let a tenant's open DDL transaction block the workers.
- The `kafka-protocol` 0.18.0 crate is vendored in `vendor/kafka-protocol` with a 13-line patch in `src`. Four lines cap `Vec::with_capacity` and `reserve` at the bytes that remain in the buffer. Without them, a 12-byte hostile request aborts the process. Nine more lines change `Record.headers` from a map to a `Vec` of (key, value) pairs, so repeated header names and their order stay (D17); six of the nine are in the crate's own tests. Remove the copy when upstream has both fixes.
- A group can mix SQL and Kafka members only when every SQL member sends Kafka consumer-protocol metadata as a base64 string. A Kafka leader does not see a SQL member without it, so that member gets an empty assignment. Empty metadata crashes librdkafka 2.15.1, which is why the member is left out.
- The stamper orders by the transaction id in `xact` (D20). `pg_dump` keeps that column, so after a restore into another cluster, the restored rows that have no offset yet have xids from the old cluster. They are usually higher than the new xids, so a new publish that the stamper sees in the same batch can get a lower offset than a restored row. A restored xid can also be equal to a new xid of the restored cluster. The rows of the two transactions then form one group, so a Kafka batch of the new transaction can get offsets that are not consecutive, and its answer is wrong. Stamp every row before the dump, or do not publish until the restored rows have offsets.
- A transaction that publishes more than `max_rows` rows into one topic is stamped in one batch while an older transaction is open anywhere in the cluster (D20). A long open transaction thus makes the stamp of a bulk load one large transaction.
- A producer id that is idle for more than 7 days is reaped. The producer then gets `UNKNOWN_PRODUCER_ID`, and Java 7.7.1 retries until its records expire. Restart such a producer.
- D25: `INVALID_PRODUCER_EPOCH` is raised for a batch with an epoch lower than the ring's epoch. The design said it is never raised, but Java (KIP-360) bumps the epoch after any failed batch, and a higher epoch starts a new ring.
- Publishing to a topic means trusting its owner. A trigger that the queue owner puts on the queue table runs as the publisher, the same as any Postgres trigger. So it can call `topic.*` functions with the publisher's rights. This applies to Kafka `Produce` too, because the listener inserts as the client role.
- A renamed queue table leaves its `topic_config` row behind. The topic then stops working, and another role with `CREATE` on that schema can make a new table with the old name. That table is stamped as its own owner.
- The red runs for the Phase 10 fences that already existed (the owner fence in `grant_publish`, the `topic_offsets` policy, the context switch in `as_owner`) were not run, because the environment refused the edits that weaken a fence. To run them by hand, weaken one fence, run `run_security.sh` and the matching pg_tests, see them fail, and restore the fence.
- `stamped_by` takes its timeline from the WAL insertion position (`pg_walfile_name(pg_current_wal_insert_lsn())`), not from `pg_control_checkpoint()`. The checkpoint value stays on the old timeline for the length of the first checkpoint after a promotion, which is exactly when a split happens.
- An unfenced split is visible on one node only as the promoted node's `stamped_by` WARNING. The old primary keeps its timeline and does not warn. `check_duplicates` and `health()` read one node, so they do not see a split across two nodes. To find the split point, compare the rows of the two nodes by `published_at`. `seq` is not comparable across nodes after a promotion.
- When `pg_topics.databases` is empty, `create_topic` does not refuse, but no database has workers, so a publish fails after `max_backlog_age`. When the setting names databases, `create_topic` refuses in a database that it does not name. The pgrx test cluster uses the empty setting, because a running worker blocks the drop of the test database.
