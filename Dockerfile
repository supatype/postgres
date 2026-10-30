# ---------------------------------------------------------------------------
# pg_keyspace builder: compiles the pgrx extension + the keys-only decode
# plugin from in-repo source (extensions/pg_keyspace/) against PGDG
# PostgreSQL 17. Building here (rather than pulling a pre-published .deb) keeps
# the final image self-contained: it needs no prior GitHub release, and the
# same Dockerfile produces a working image on both amd64 and arm64 (the release
# pipeline builds each arch on a native runner). Only the built artifacts are
# copied into the runtime image below; the Rust toolchain never lands there.
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

# The repository's licence, shipped in the image and declared on it (the PostgreSQL License).
LABEL org.opencontainers.image.licenses="PostgreSQL"
COPY LICENSE /LICENSE

ARG PG_GRAPHQL_VERSION=1.5.9

# PGDG apt repo (for PostGIS), and what the extension builds below need
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
  && rm -rf /var/lib/apt/lists/*

# pgvector, wal2json, pg_cron, pg_safeupdate, pg_plan_filter, pgjwt and pg_net, built by the script
# the native archives build them with, at the pins in that script, so the image and native dev run
# the same versions from one place. wal2json used to be PGDG's package pinned to a release, which
# stops installing as soon as PGDG drops that release; pg_cron, pg_safeupdate and pg_plan_filter
# were unpinned. PostGIS stays PGDG's package above, which carries raster.
COPY scripts/build-native-extensions.sh scripts/lib.sh /tmp/native-build/
RUN BUILD_DIR=/tmp/native-build/work \
    bash /tmp/native-build/build-native-extensions.sh /usr/lib/postgresql/17/bin/pg_config image \
  && rm -rf /tmp/native-build

# pg_graphql: pre-built deb (Rust/pgrx, too slow to compile in CI)
RUN ARCH=$(dpkg --print-architecture) \
  && curl -fsSL "https://github.com/supabase/pg_graphql/releases/download/v${PG_GRAPHQL_VERSION}/pg_graphql-v${PG_GRAPHQL_VERSION}-pg17-${ARCH}-linux-gnu.deb" \
     -o /tmp/pg_graphql.deb \
  && dpkg -i /tmp/pg_graphql.deb && rm /tmp/pg_graphql.deb

# pg_keyspace: Postgres-native RESP keyspace + PostgREST row cache. Compiled
# from in-repo source in the pgks-builder stage above and copied in here, so the
# image is self-contained (no dependency on a pre-published release) and builds
# identically on amd64 and arm64. Bundled and creatable, but NOT auto-loaded: it
# serves RESP only once an operator adds it to shared_preload_libraries (it
# requires that to run). See /etc/postgresql-custom/pg_keyspace.conf to enable it.
COPY --from=pgks-builder /out/usr/ /usr/

# pg_guard: role/extension privilege enforcement (bundled in extensions/)
COPY extensions/pg_guard/ /tmp/pg_guard/
RUN cd /tmp/pg_guard && make clean && make && make install && rm -rf /tmp/pg_guard

# supatype_mask: per-column read masking and write rejection (bundled in extensions/)
COPY extensions/supatype_mask/ /tmp/supatype_mask/
RUN cd /tmp/supatype_mask && make clean && make && make install && rm -rf /tmp/supatype_mask

# Remove build tools
RUN apt-get purge -y build-essential git postgresql-server-dev-17 pkg-config libssl-dev libcurl4-openssl-dev \
  && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# PostgreSQL config
RUN mkdir -p /etc/postgresql-custom/extension-custom-scripts
COPY config/postgresql.conf /etc/postgresql/postgresql.conf
COPY config/pg_hba.conf /etc/postgresql/pg_hba.conf
COPY config/pg_hba.migrate.conf /etc/postgresql/pg_hba.migrate.conf
COPY config/pg_ident.conf /etc/postgresql/pg_ident.conf
COPY config/pg_guard.conf /etc/postgresql-custom/pg_guard.conf
COPY config/supatype_mask.conf /etc/postgresql-custom/supatype_mask.conf
COPY config/pg_keyspace.conf /etc/postgresql-custom/pg_keyspace.conf
COPY config/pg_cron.conf /etc/postgresql-custom/pg_cron.conf
COPY config/extension-custom-scripts/ /etc/postgresql-custom/extension-custom-scripts/

# Bootstrap: the stock postgres entrypoint only runs *.sh / *.sql in this
# directory itself (not in subfolders), and only when it has just created the
# data directory. migrate.sh applies init-scripts/ + migrations/.
COPY migrations/db/init-scripts/ /docker-entrypoint-initdb.d/init-scripts/
COPY migrations/db/migrations/    /docker-entrypoint-initdb.d/migrations/
# `00-`, not `99-`: the entrypoint runs this directory in glob order, and it is
# also where users mount their own init scripts. Running the image's bootstrap
# LAST meant a user's 01-seed.sql executed against a database with no auth
# schema, no extensions and none of the grants this creates. It has to go first.
#
# `bootstrap`, not `migrate`: this creates the postgres role, runs init-scripts/,
# sets the role passwords and resets stats, then hands the migrations themselves
# to `supatype migrate`. The old name described a fraction of it, and sat one
# character away from the command below.
COPY migrations/db/migrate.sh /docker-entrypoint-initdb.d/00-supatype-bootstrap.sh
RUN chmod +x /docker-entrypoint-initdb.d/00-supatype-bootstrap.sh

# `supatype <command>` resolves subcommands out of libexec, so a later
# `supatype keyspace` is a file dropped in beside `migrate` rather than another
# top-level binary. Nothing in /docker-entrypoint-initdb.d/ can hold a tool like
# this: everything in that directory is executed on first boot.
COPY scripts/supatype /usr/local/bin/supatype
COPY migrations/db/apply-migrations.sh /usr/local/libexec/supatype/migrate

# Wraps the stock entrypoint to apply migrations to a data directory that already
# exists, which the stock entrypoint skips -- see the script, and #138.
COPY scripts/supatype-entrypoint.sh /usr/local/bin/supatype-entrypoint.sh
RUN chmod +x /usr/local/bin/supatype \
             /usr/local/libexec/supatype/migrate \
             /usr/local/bin/supatype-entrypoint.sh

ENV POSTGRES_USER=supatype_admin

# Over TCP, not the socket: the entrypoint applies pending migrations against a
# temporary server that listens on the Unix socket alone, so a socket probe
# reports healthy mid-migration and dependents wired to `service_healthy` would
# start and fail to connect. A TCP answer means the real server is up.
# -d postgres, and not only for tidiness: without it libpq defaults the database to the user name,
# so every probe logs `FATAL: database "supatype_admin" does not exist`. The check still passed:
# pg_isready asks whether the server answers, not whether the connection would succeed. But at one
# line every five seconds it is most of what this image's log contains, and it buries anything worth
# reading. It cost two CI cycles of an unreadable `docker logs --tail` to notice.
HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD pg_isready -h 127.0.0.1 -U supatype_admin -d postgres

EXPOSE 5432

# Overrides the base image's ENTRYPOINT ["docker-entrypoint.sh"]; the wrapper
# execs into that same script once it has dealt with pending migrations.
ENTRYPOINT ["/usr/local/bin/supatype-entrypoint.sh"]

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
