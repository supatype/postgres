# Prototype harnesses: owner-addressed recovery

These are not part of the CI suite. They exist to make the prototype on
`proto/owner-addressed-recovery` reproducible, and they are Docker-driven
rather than following the convention of the scripts in `bench/`, because they
need to restart the postmaster at several different `pg_keyspace.workers`
values and that is a postmaster-context GUC.

## Building the image they run against

The toolchain mirrors the `pgks-builder` stage of the repo `Dockerfile` but
copies no source, so the extension rebuilds incrementally:

```sh
docker build -t pgks-dev:toolchain - <<'DOCKERFILE'
FROM postgres:17-bookworm
ARG PGRX_VERSION=0.12.9
ENV CARGO_HOME=/usr/local/cargo RUSTUP_HOME=/usr/local/rustup PATH=/usr/local/cargo/bin:$PATH
RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates gnupg lsb-release git build-essential pkg-config \
      libssl-dev clang libclang-dev llvm-dev \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends postgresql-server-dev-17 \
  && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable \
  && cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked
RUN cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config
DOCKERFILE

# Build the extension, then layer it over a released image.
docker volume create pgks-target
docker run --rm -v "$PWD/../..":/src -v pgks-target:/target \
  -w /src/pg_keyspace/extension -e CARGO_TARGET_DIR=/target \
  pgks-dev:toolchain cargo pgrx package --pg-config /usr/lib/postgresql/17/bin/pg_config
```

Copy `/target/release/pg_keyspace-pg17/usr` out of the volume and `COPY` it over
`supatype/postgres:17.2.7` as `pgks-proto:owner`. The crate's default feature is
`pg16`, so a bare `cargo check` fails with "Postgres `pg16` is not managed by
pgrx"; pass `--no-default-features --features pg17`.

## Running

```sh
IMAGE=pgks-proto:owner ./run_owner_recovery.sh   # must PASS
IMAGE=pgks-proto:owner ./run_slot_control.sh     # must PASS (its first phase
                                                 # asserts a plain client is
                                                 # REFUSED by slot addressing)
```

`run_owner_recovery.sh` writes 100 plain and 60 TTL'd keys through a plain
(non-cluster) client across four durable workers, then restarts at 2, 4, 3, 1
and 4 workers, requiring every key back each time at the worker `owner % workers`
predicts.

`run_slot_control.sh` is the control. Its first phase is expected to fail at the
first `SET` with `MOVED`, which is the reason owner addressing exists: a
non-cluster client cannot use a persisted multi-worker cluster today, and all
three Supatype clients use standalone constructors. Its second phase is the
regression check that the default `slot` path still works with a cluster-aware
client.

## One trap worth keeping

A worker recovers *before* it listens, and each worker does so independently, so
waiting on the RESP ports is not a sound readiness check: a verify can run while
a later worker is still loading and read an empty segment as data loss. An
earlier version of this harness did exactly that and reported a convincing false
failure (25 keys "missing", one worker's worth). `ready()` waits for every
worker to have both recovered and opened its port.
