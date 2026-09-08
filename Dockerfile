# ---------------------------------------------------------------------------
# pg_keyspace builder — compiles the pgrx extension + the keys-only decode
# plugin from in-repo source (extensions/pg_keyspace/) against PGDG
# PostgreSQL 17. Building here (rather than pulling a pre-published .deb) keeps
# the final image self-contained: it needs no prior GitHub release, and the
# same Dockerfile produces a working image on both amd64 and arm64 (the release
# pipeline builds each arch on a native runner). Only the built artifacts are
# copied into the runtime image below — the Rust toolchain never lands there.
# ---------------------------------------------------------------------------
FROM postgres:17-bookworm AS pgks-builder

ARG PGRX_VERSION=0.12.9
ENV CARGO_HOME=/usr/local/cargo \
    RUSTUP_HOME=/usr/local/rustup \
    PATH=/usr/local/cargo/bin:$PATH

RUN apt-get update && apt-get install -y --no-install-recommends \
      curl ca-certificates gnupg lsb-release git build-essential pkg-config \
      libssl-dev clang libclang-dev llvm-dev \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
     | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
     > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends \
     postgresql-server-dev-17 \
  && rm -rf /var/lib/apt/lists/*

# Rust toolchain + cargo-pgrx pinned to the extension's pgrx version.
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable \
  && cargo install cargo-pgrx --version "${PGRX_VERSION}" --locked

# Point pgrx at the image's already-installed PG17 rather than compiling one.
RUN cargo pgrx init --pg17 /usr/lib/postgresql/17/bin/pg_config

COPY extensions/pg_keyspace/ /src/pg_keyspace/

# Extension (.so + .control + .sql): pgrx packages into a usr/ tree rooted at
# the pg_config prefix (usr/lib/postgresql/17/lib + usr/share/postgresql/17/...).
RUN cd /src/pg_keyspace/extension \
  && cargo pgrx package --pg-config /usr/lib/postgresql/17/bin/pg_config \
  && mkdir -p /out \
  && cp -a target/release/pg_keyspace-pg17/usr /out/

# Keys-only logical-decode plugin (Mode B row cache), beside the extension .so.
RUN cd /src/pg_keyspace/plugin \
  && make PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config \
  && install -Dm755 supacache_keys.so /out/usr/lib/postgresql/17/lib/supacache_keys.so

# ---------------------------------------------------------------------------
# Runtime image
# ---------------------------------------------------------------------------
FROM postgres:17-bookworm

ARG PGVECTOR_VERSION=0.8.0
ARG PG_NET_VERSION=0.14.0
ARG PG_GRAPHQL_VERSION=1.5.9
ARG PGJWT_COMMIT=f3d82fd30151e754e19ce5d6a06c71c20689ce3d

# PGDG apt repo (for pg_cron, postgis, wal2json, pgsodium)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates gnupg lsb-release \
  && curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc \
     | gpg --dearmor -o /usr/share/keyrings/pgdg.gpg \
  && echo "deb [signed-by=/usr/share/keyrings/pgdg.gpg] https://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
     > /etc/apt/sources.list.d/pgdg.list \
  && apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    pkg-config \
    libssl-dev \
    libcurl4-openssl-dev \
    postgresql-server-dev-17 \
    postgresql-17-postgis-3 \
    postgresql-17-cron \
    postgresql-17-wal2json \
  && rm -rf /var/lib/apt/lists/*

# pgvector — OPTFLAGS="" avoids -march=native (AVX-512 SIGILL on older CPUs / GH Actions).
RUN git clone --depth 1 --branch v${PGVECTOR_VERSION} https://github.com/pgvector/pgvector.git /tmp/pgvector \
  && cd /tmp/pgvector && make OPTFLAGS="" && make install && rm -rf /tmp/pgvector

# pg_net (async HTTP from triggers)
RUN git clone --depth 1 --branch v${PG_NET_VERSION} https://github.com/supabase/pg_net.git /tmp/pg_net \
  && cd /tmp/pg_net && make && make install && rm -rf /tmp/pg_net

# pgjwt (JWT verification in SQL)
RUN git clone https://github.com/michelp/pgjwt.git /tmp/pgjwt \
  && cd /tmp/pgjwt && git checkout ${PGJWT_COMMIT} && make install && rm -rf /tmp/pgjwt

# pg_safeupdate (prevents UPDATE/DELETE without WHERE)
RUN git clone --depth 1 https://github.com/eradman/pg-safeupdate.git /tmp/pg_safeupdate \
  && cd /tmp/pg_safeupdate && make && make install && rm -rf /tmp/pg_safeupdate

# pg_plan_filter (query cost limits for shared tenancy)
RUN git clone --depth 1 https://github.com/pgexperts/pg_plan_filter.git /tmp/plan_filter \
  && cd /tmp/plan_filter && make && make install && rm -rf /tmp/plan_filter

# pg_graphql — pre-built deb (Rust/pgrx — too slow to compile in CI)
RUN ARCH=$(dpkg --print-architecture) \
  && curl -fsSL "https://github.com/supabase/pg_graphql/releases/download/v${PG_GRAPHQL_VERSION}/pg_graphql-v${PG_GRAPHQL_VERSION}-pg17-${ARCH}-linux-gnu.deb" \
     -o /tmp/pg_graphql.deb \
  && dpkg -i /tmp/pg_graphql.deb && rm /tmp/pg_graphql.deb

# pg_keyspace — Postgres-native RESP keyspace + PostgREST row cache. Compiled
# from in-repo source in the pgks-builder stage above and copied in here, so the
# image is self-contained (no dependency on a pre-published release) and builds
# identically on amd64 and arm64. Bundled and creatable, but NOT auto-loaded: it
# serves RESP only once an operator adds it to shared_preload_libraries (it
# requires that to run). See /etc/postgresql-custom/pg_keyspace.conf to enable it.
COPY --from=pgks-builder /out/usr/ /usr/

# pg_guard — role/extension privilege enforcement (bundled in extensions/)
COPY extensions/pg_guard/ /tmp/pg_guard/
RUN cd /tmp/pg_guard && make clean && make && make install && rm -rf /tmp/pg_guard

# supatype_mask — per-column read masking and write rejection (bundled in extensions/)
COPY extensions/supatype_mask/ /tmp/supatype_mask/
RUN cd /tmp/supatype_mask && make clean && make && make install && rm -rf /tmp/supatype_mask

# Remove build tools
RUN apt-get purge -y build-essential git postgresql-server-dev-17 pkg-config libssl-dev libcurl4-openssl-dev \
  && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# PostgreSQL config
RUN mkdir -p /etc/postgresql-custom/extension-custom-scripts
COPY config/postgresql.conf /etc/postgresql/postgresql.conf
COPY config/pg_hba.conf /etc/postgresql/pg_hba.conf
COPY config/pg_ident.conf /etc/postgresql/pg_ident.conf
COPY config/pg_guard.conf /etc/postgresql-custom/pg_guard.conf
COPY config/supatype_mask.conf /etc/postgresql-custom/supatype_mask.conf
COPY config/pg_keyspace.conf /etc/postgresql-custom/pg_keyspace.conf
COPY config/extension-custom-scripts/ /etc/postgresql-custom/extension-custom-scripts/

# Bootstrap migrations: the stock postgres entrypoint only runs *.sh / *.sql in this
# directory itself — not in subfolders. migrate.sh applies init-scripts/ + migrations/.
COPY migrations/db/init-scripts/ /docker-entrypoint-initdb.d/init-scripts/
COPY migrations/db/migrations/    /docker-entrypoint-initdb.d/migrations/
COPY migrations/db/migrate.sh /docker-entrypoint-initdb.d/99-supatype-migrate.sh
RUN chmod +x /docker-entrypoint-initdb.d/99-supatype-migrate.sh

ENV POSTGRES_USER=supatype_admin

HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD pg_isready -U supatype_admin

EXPOSE 5432

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
