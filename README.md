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
| `17-latest` | Most recent PG17 **release** — moves only when a `vX.Y.Z` tag is published, not on `develop` pushes |
| `x.y.z` | Pinned, immutable release |
| `latest` | Alias for the most recent release (same digest as `17-latest`) |
| `develop` | Latest build from the `develop` branch |

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

## Migrations and upgrades

The image ships a set of SQL migrations in [migrations/db/migrations/](migrations/db/migrations/) and applies them in two places:

- **First boot**, from `00-supatype-bootstrap.sh`, which the stock Postgres entrypoint runs once against a data directory it has just created.
- **Every subsequent start**, from the image's entrypoint, which applies anything the cluster has not already run before it lets clients connect.

The second one matters when you pull a newer tag over a volume you already have. The stock entrypoint runs `/docker-entrypoint-initdb.d/` **only** on an empty data directory, so without this an in-place update would skip every migration added since that volume was created — silently, with no error.

Each database records what it has run in `supatype_migrations.applied`:

```bash
docker exec -e POSTGRES_PASSWORD=... -e POSTGRES_DB=supatype_admin \
  supatype-postgres supatype migrate status
```

| Column | Meaning |
|---|---|
| `filename` | The migration, named as it ships |
| `applied_at` | When this database ran it |
| `backfilled` | `true` if it was *assumed* applied rather than observed running — see below |

`supatype migrate doctor` checks whether the load-bearing migrations actually took effect, and `doctor --fix` applies any that did not.

### Volumes created before this change

Clusters created by an earlier image have no ledger, and nothing on disk records which migrations they ran. **On the first start under a ledger-carrying image, such a volume gets a ledger in which every shipped migration is marked applied without being run** (`backfilled = true`). This happens once per database, and the container log says so.

That choice is deliberate: marking them applied never replays DDL against live data. Replaying all 52 instead would repair a volume that was behind, but only if every one of them is safe to re-run against a populated database — which has not been established, so it is not the default.

What it costs:

- A volume that was **already up to date** is now tracked correctly. Nothing further to do.
- A volume that was **behind stays behind**. The migrations it never ran are recorded as applied and will not run on their own.

Nothing distinguishes those two cases after the fact, so check directly.

#### Check whether your volume was behind

`doctor` asks the database rather than the ledger: it verifies that the migrations whose absence is silent and consequential actually took effect.

```bash
docker exec -e POSTGRES_PASSWORD=... -e POSTGRES_DB=supatype_admin \
  supatype-postgres supatype migrate doctor
```

```
  OK       supatype_privileged_role exists
  MISSING  supatype_mask extension present
           migration: 20260809214500_supatype_mask.sql (ledger: assumed applied, never run here)
           fix: supatype migrate replay 20260809214500_supatype_mask.sql
  OK       pg_guard preloaded for authenticator
supatype migrate: doctor: 3 checks, 1 need attention.
```

Exit status is 0 when everything checks out, non-zero otherwise, so it drops straight into a health script. The entrypoint runs these same checks automatically after adopting a volume, so the container log already tells you which case you are in.

#### Repair

```bash
docker exec -e POSTGRES_PASSWORD=... -e POSTGRES_DB=supatype_admin \
  supatype-postgres supatype migrate doctor --fix
```

This applies **only** the migrations whose check failed, in filename order, and then re-runs the checks and reports whether it worked. A check that passes is left alone, so this is not a replay of work already done — a failing check means that migration's effect is absent.

`supatype migrate replay <filename>` remains available for anything outside the checked set; `supatype migrate status` lists every migration flagged `backfilled`.

#### What each one leaves broken

| Check | Migration | Until repaired |
|---|---|---|
| `pg_guard preloaded for authenticator` | `20260810150000_authenticator_session_preload.sql` | **`pg_guard` does not load for PostgREST sessions.** The `authenticator` role keeps an inherited `session_preload_libraries` naming only `safeupdate`, so privilege enforcement is off across the whole API surface — silently, with no error |
| `supatype_mask extension present` | `20260809214500_supatype_mask.sql` | A masked-write rejection raises `supatype_mask.deny() is not available` instead of a permission error |
| `supatype_privileged_role exists` | `20260211120934_supabase_privileged_role.sql` | The `supatype_privileged_role` role does not exist |

### Notes

- A failed migration **stops the container** rather than letting clients reach a half-migrated database. Set `SUPATYPE_SKIP_MIGRATIONS=1` to start anyway.
- Standbys are skipped: a replica gets its schema by replaying the primary's WAL.
- Migrations run against a temporary server that listens on the Unix socket only, before the real server starts, so nothing outside the container can connect mid-migration.

---

## Repository structure

```
Dockerfile                      Main image build (FROM postgres:17-bookworm)
config/
  postgresql.conf               PostgreSQL configuration
  pg_hba.conf                   Client authentication rules
  pg_hba.migrate.conf           Client auth for the migration window only
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
    migrate.sh                  First-boot bootstrap (installed as 00-supatype-bootstrap.sh)
    apply-migrations.sh         Migration runner and ledger (installed as `supatype migrate`)
scripts/
  supatype                      `supatype <command>` dispatcher (installed on PATH)
  supatype-entrypoint.sh        Image ENTRYPOINT; applies migrations to existing clusters
  build-native.sh               Local native build script (mirrors CI)
tests/
  pg_upgrade/                   PostgreSQL upgrade regression tests
```

---

## CI

| Workflow | Trigger | What it does |
|---|---|---|
| `build-release.yml` | Push to `main`/`develop`/`release/*`, tag | Builds multi-arch image, pushes to Docker Hub |
| `docker-image-test.yml` | PRs, push to `develop` | Builds image, verifies extensions load, that migrations apply on both a fresh volume and an existing one, and that `doctor --fix` repairs a migration that never took effect |
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
