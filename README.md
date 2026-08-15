# supatype/postgres

PostgreSQL 17 with a curated set of extensions and security hardening for the [Supatype](https://github.com/supatype/supatype) platform. This is the default Postgres image for **`supatype dev`**, **self-host Compose**, and cloud project databases.

**Product:** [github.com/supatype](https://github.com/supatype) · **Docs:** [supatype.github.io/supatype](https://supatype.github.io/supatype/)

Published as a multi-arch Docker image and native binary archives for all major platforms.

---

## Docker image

```bash
docker pull supatype/postgres:17-latest
```

| Tag | Description |
|---|---|
| `17-latest` | Latest build from `main` |
| `x.y.z` | Pinned release |
| `latest` | Alias for the most recent release tag |
| `develop` | Latest build from `develop` branch |

Multi-arch: `linux/amd64` and `linux/arm64`.

### Quick start

```bash
docker run -d \
  --name supatype-postgres \
  -e POSTGRES_PASSWORD=your-password \
  -p 5432:5432 \
  supatype/postgres:17-latest
```

Default superuser is `supatype_admin` (set via `POSTGRES_USER`).

---

## Extensions

| Extension | Purpose |
|---|---|
| [PostGIS](https://postgis.net/) | Geography and geometry types, spatial queries |
| [pgvector](https://github.com/pgvector/pgvector) | Vector embeddings and similarity search |
| [pg_graphql](https://github.com/supabase/pg_graphql) | GraphQL API directly over your schema |
| [pg_net](https://github.com/supabase/pg_net) | Async HTTP requests from triggers and functions |
| [pgjwt](https://github.com/michelp/pgjwt) | JWT generation and verification in SQL |
| [pg_cron](https://github.com/citusdata/pg_cron) | Scheduled jobs inside PostgreSQL |
| [pg_safeupdate](https://github.com/eradman/pg-safeupdate) | Prevents UPDATE/DELETE without a WHERE clause |
| [pg_plan_filter](https://github.com/pgexperts/pg_plan_filter) | Rejects queries that exceed a cost threshold |
| [wal2json](https://github.com/eulerto/wal2json) | Logical decoding output plugin (JSON CDC for realtime) |
| [pg_guard](extensions/pg_guard/) | Role and extension privilege enforcement (bundled) |

### Auto-loaded libraries

```
shared_preload_libraries = 'pg_stat_statements, pg_cron, pg_net, plan_filter, safeupdate'
session_preload_libraries = 'pg_guard'
```

---

## Configuration defaults

| Setting | Value | Notes |
|---|---|---|
| `wal_level` | `logical` | Ready for logical replication (wal2json plugin bundled) |
| `max_replication_slots` | `5` | |
| `max_wal_senders` | `10` | |
| `row_security` | `on` | RLS enforced by default |
| `password_encryption` | `scram-sha-256` | |
| `log_statement` | `ddl` | DDL logged; DML is not |
| `timezone` | `UTC` | |

Full config: [config/postgresql.conf](config/postgresql.conf)

PgBouncer session and transaction pooling configs are included in [config/](config/) for deployments that run a sidecar pooler.

---

## Repository structure

```
Dockerfile                      Main image build (FROM postgres:17-bookworm)
config/
  postgresql.conf               PostgreSQL configuration
  pg_hba.conf                   Client authentication rules
  pg_guard.conf                 pg_guard GUC settings
  pgbouncer-session.ini         PgBouncer session pooling config
  pgbouncer-transaction.ini     PgBouncer transaction pooling config
  extension-custom-scripts/     Per-extension post-install SQL hooks
extensions/
  pg_guard/                     Bundled pg_guard C extension source
migrations/
  db/
    init-scripts/               Run once on first database initialisation
    migrations/                 Incremental schema migrations
    migrate.sh                  Migration runner (called by Docker entrypoint)
scripts/
  build-native.sh               Local native build script (mirrors CI)
tests/
  pg_upgrade/                   PostgreSQL upgrade regression tests
```

---

## CI

| Workflow | Trigger | What it does |
|---|---|---|
| `build-release.yml` | Push to `main`/`develop`/`release/*`, tag | Builds multi-arch image, pushes to Docker Hub |
| `docker-image-test.yml` | PRs, push to `develop` | Builds image, verifies extensions load correctly |
| `native-archives.yml` | Tag push | Builds native PG17 tarballs for all platforms, uploads to CDN and GitHub Release |
| `test-pg-guard.yml` | PRs touching `extensions/pg_guard/**` | Runs pg_guard regression suite |

---

## Native archives

For deployments that run PostgreSQL natively (no Docker), pre-built archives are published on every release to the CDN and attached to GitHub Releases.

| Platform | Archive |
|---|---|
| Linux x86-64 | `supatype-pg-17-linux-amd64.tar.gz` |
| Linux arm64 | `supatype-pg-17-linux-arm64.tar.gz` |
| macOS Apple Silicon | `supatype-pg-17-darwin-arm64.tar.gz` |
| macOS Intel | `supatype-pg-17-darwin-amd64.tar.gz` |
| Windows x64 | `supatype-pg-17-windows-amd64.zip` |

pg_guard is bundled in all archives (except the cross-compiled linux-arm64 build where cross-compilation of the extension is not supported).

To build a native archive locally:

```bash
./scripts/build-native.sh --target linux-amd64
# Output: supatype-pg-17-linux-amd64.tar.gz + .sha256
```

---

## pg_guard

pg_guard is a PostgreSQL extension bundled in `extensions/pg_guard/` that provides controlled superuser delegation for dedicated Postgres instances:

- Restricts which roles can install extensions (extension management without full superuser)
- Prevents privilege escalation via `CREATE ROLE ... SUPERUSER`
- Enforces a designated privileged role (`pg_guard.privileged_role`)
- Enables safe creation of publications and event triggers without granting superuser
- Loaded via `session_preload_libraries` — active on every connection; can be overridden per-role via `ALTER ROLE ... SET session_preload_libraries`

Regression tests run on every PR that touches the extension source:

```bash
PG_CONFIG=/usr/lib/postgresql/17/bin/pg_config \
  make -C extensions/pg_guard check
```

---

## Building locally

```bash
docker build -t supatype/postgres:local .

docker run -d \
  --name supatype-postgres-local \
  -e POSTGRES_PASSWORD=postgres \
  -p 5432:5432 \
  supatype/postgres:local
```

---

## License

[The PostgreSQL License](LICENSE)
