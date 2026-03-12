FROM postgres:17-bookworm

ARG PGVECTOR_VERSION=0.8.0
ARG PG_NET_VERSION=0.14.0
ARG PG_GRAPHQL_VERSION=1.5.9
ARG PGJWT_COMMIT=f3d82fd30151e754e19ce5d6a06c71c20689ce3d
ARG PG_GUARD_VERSION=0.24.0

# PGDG apt repo (for pg_cron, postgis, pgsodium)
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
  && rm -rf /var/lib/apt/lists/*

# pgvector
RUN git clone --depth 1 --branch v${PGVECTOR_VERSION} https://github.com/pgvector/pgvector.git /tmp/pgvector \
  && cd /tmp/pgvector && make && make install && rm -rf /tmp/pgvector

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

# pg_guard — role/extension privilege enforcement (critical for cloud security)
RUN git clone --depth 1 --branch v${PG_GUARD_VERSION} https://github.com/supatype/pg_guard.git /tmp/pg_guard \
  && cd /tmp/pg_guard && make && make install && rm -rf /tmp/pg_guard

# Remove build tools
RUN apt-get purge -y build-essential git postgresql-server-dev-17 pkg-config libssl-dev libcurl4-openssl-dev \
  && apt-get autoremove -y && rm -rf /var/lib/apt/lists/*

# PostgreSQL config
RUN mkdir -p /etc/postgresql-custom/extension-custom-scripts
COPY config/postgresql.conf /etc/postgresql/postgresql.conf
COPY config/pg_hba.conf /etc/postgresql/pg_hba.conf
COPY config/pg_ident.conf /etc/postgresql/pg_ident.conf
COPY config/pg_guard.conf /etc/postgresql-custom/pg_guard.conf
COPY config/extension-custom-scripts/ /etc/postgresql-custom/extension-custom-scripts/

# Bootstrap migrations (postgres entrypoint runs these on first start)
COPY migrations/db/init-scripts/ /docker-entrypoint-initdb.d/init-scripts/
COPY migrations/db/migrations/    /docker-entrypoint-initdb.d/migrations/

ENV POSTGRES_USER=supatype_admin

HEALTHCHECK --interval=5s --timeout=3s --retries=10 \
  CMD pg_isready -U supatype_admin

EXPOSE 5432

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]
