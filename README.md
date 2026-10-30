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
| `17-latest` | Most recent PG17 **release**; moves only when a `vX.Y.Z` tag is published, not on `develop` pushes |
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
| [pg_graphql](https://github.com/supabase/pg_graphql) | GraphQL API directly over your schema (image only: no native archive carries it, see [Native archives](#native-archives)) |
| [pg_net](https://github.com/supabase/pg_net) | Async HTTP requests from triggers and functions |
| [pgjwt](https://github.com/michelp/pgjwt) | JWT generation and verification in SQL |
| [pg_cron](https://github.com/citusdata/pg_cron) | Scheduled jobs inside PostgreSQL |
| [pg_safeupdate](https://github.com/eradman/pg-safeupdate) | Prevents UPDATE/DELETE without a WHERE clause |
| [pg_plan_filter](https://github.com/pgexperts/pg_plan_filter) | Rejects queries that exceed a cost threshold |
| [wal2json](https://github.com/eulerto/wal2json) | Logical decoding output plugin (JSON CDC for realtime) |
| [pg_guard](extensions/pg_guard/) | Role and extension privilege enforcement (bundled) |
| [pg_keyspace](extensions/pg_keyspace/) | RESP (Redis/Valkey-compatible) keyspace in shared memory + PostgREST row cache (bundled, opt-in) |
| [supatype_mask](extensions/supatype_mask/) | Per-column read masking and write rejection, enforced in the planner (bundled) |

### Auto-loaded libraries

```
shared_preload_libraries = 'pg_stat_statements, pg_cron, pg_net, plan_filter, safeupdate'
session_preload_libraries = 'pg_guard'
```

`pg_keyspace` is bundled and creatable but deliberately **not** auto-loaded: it
registers background workers and requests shared memory at postmaster start, so
turning it on is a restart rather than a runtime toggle. To enable it, add it to
`shared_preload_libraries`, **before** `supatype_mask`, and restart:

```
shared_preload_libraries = '..., pg_keyspace, supatype_mask'
```

See [config/pg_keyspace.conf](config/pg_keyspace.conf) for the settings, and
[extensions/pg_keyspace/README.md](extensions/pg_keyspace/README.md) for what it
serves.

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

The second one matters when you pull a newer tag over a volume you already have. The stock entrypoint runs `/docker-entrypoint-initdb.d/` **only** on an empty data directory, so without this an in-place update would skip every migration added since that volume was created, silently and with no error.

Each database records what it has run in `supatype_migrations.applied`:

```bash
docker exec -e POSTGRES_PASSWORD=... -e POSTGRES_DB=supatype_admin \
  supatype-postgres supatype migrate status
```

| Column | Meaning |
|---|---|
| `filename` | The migration, named as it ships |
| `applied_at` | When this database ran it |
| `backfilled` | `true` if it was *assumed* applied rather than observed running (see below) |

`supatype migrate doctor` checks whether the load-bearing migrations actually took effect, and `doctor --fix` applies any that did not.

### Volumes created before this change

Clusters created by an earlier image have no ledger, and nothing on disk records which migrations they ran. **On the first start under a ledger-carrying image, such a volume gets a ledger in which every shipped migration is marked applied without being run** (`backfilled = true`). This happens once per database, and the container log says so.

That choice is deliberate: marking them applied never replays DDL against live data. Replaying all of them instead would repair a volume that was behind, but only if every one is safe to re-run against a populated database. That has not been established, so it is not the default.

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
  OK       new tables in public are not granted to anon or authenticated
supatype migrate: doctor: 4 checks, 1 need attention.
```

Exit status is 0 when everything checks out, non-zero otherwise, so it drops straight into a health script. The entrypoint runs these same checks automatically after adopting a volume, so the container log already tells you which case you are in.

#### Repair

```bash
docker exec -e POSTGRES_PASSWORD=... -e POSTGRES_DB=supatype_admin \
  supatype-postgres supatype migrate doctor --fix
```

This applies **only** the migrations whose check failed, in filename order, and then re-runs the checks and reports whether it worked. A check that passes is left alone, so this is not a replay of work already done: a failing check means that migration's effect is absent. A check listed as `(operator)` below is not a migration: it is printed as a note, does not fail `doctor`, and `--fix` leaves it alone.

`supatype migrate replay <filename>` remains available for anything outside the checked set; `supatype migrate status` lists every migration flagged `backfilled`.

#### What each one leaves broken

| Check | Migration | Until repaired |
|---|---|---|
| `an ordinary postgres role is not a superuser` | `10000000000000_demote-postgres.sql` | **`postgres` keeps SUPERUSER.** Anything that can log in as it, including the Studio SQL editor if it were ever made a member, has every privilege in the cluster |
| `a bootstrap superuser postgres has no usable password` | none: `(operator)` | **A superuser login with a known password.** Under an operator such as CloudNativePG, `postgres` is the bootstrap superuser and cannot be demoted. The image leaves it alone and reports it here if it also has a usable password. Only the operator can change that (for CloudNativePG, `enableSuperuserAccess: false`). Printed as `NOTE`, it does not fail `doctor`, and `doctor --fix` leaves it alone. A migration role that cannot read `pg_authid` cannot check this; the row then says `not checked` and is a `NOTE` too |
| `pg_guard preloaded for authenticator` | `20260810150000_authenticator_session_preload.sql` | **`pg_guard` does not load for PostgREST sessions.** The `authenticator` role keeps an inherited `session_preload_libraries` naming only `safeupdate`, so privilege enforcement is off across the whole API surface, silently and with no error |
| `supatype_mask extension present` | `20260809214500_supatype_mask.sql` | A masked-write rejection raises `supatype_mask.deny() is not available` instead of a permission error |
| `supatype_privileged_role exists` | `20260211120934_supabase_privileged_role.sql` | The `supatype_privileged_role` role does not exist |
| `new tables in public are not granted to anon or authenticated` | `20261012210000_private_table_defaults.sql` | **A table made by hand in `public` is readable and writable with the anon key.** The open default privileges described below are still in place |

### Default privileges in `public`

A new table in `public` is granted to `service_role` (and `postgres`, and its owner) and to nobody else. `anon` and `authenticated` get a table's privileges only when something grants them: the Supatype schema engine writes a model table's grants from its access rules on every push, so a model behaves as its schema says and a table made by hand stays private until you grant it.

Earlier images granted every privilege on new tables in `public` to `anon` and `authenticated` by default, so any table made by hand was open to the anon key.

What stays as it was:

- **Sequences.** New sequences in `public` are still granted to `anon`, `authenticated` and `service_role`. An insert into a table with a `serial` or identity column needs `USAGE` on its sequence, and older schema engines do not grant it on a model's sequences themselves.
- **Functions** in `public`. This change is about tables.
- **Every other schema.** The defaults in `storage`, `graphql`, `graphql_public`, `extensions`, `auth`, `realtime` and `cron` are unchanged, because the stack's services rely on them.
- **Tables that already exist.** Nothing revokes a grant a table already has.

**Existing volumes.** The initial schema runs only on an empty data directory, so a volume created by an earlier image keeps the open defaults until something removes them. Two things do, and either is enough:

- `20261012210000_private_table_defaults.sql` revokes them the next time this image starts on the volume. On a volume adopted without a ledger (above) it is marked applied without running; `supatype migrate doctor` reports it and `doctor --fix` applies it.
- The Supatype CLI revokes them on every `supatype dev` and on `supatype push` to a local compose stack, so a project the CLI has already reached is unchanged by the migration.

### Notes

- A failed migration **stops the container** rather than letting clients reach a half-migrated database. Set `SUPATYPE_SKIP_MIGRATIONS=1` to start anyway.
- Standbys are skipped: a replica gets its schema by replaying the primary's WAL.
- Migrations run against a temporary server that listens on the Unix socket only, before the real server starts, so nothing outside the container can connect mid-migration.

---

### Under an operator (CloudNativePG)

An operator such as CloudNativePG starts `postgres` through its own instance manager, so neither the image's entrypoint nor `/docker-entrypoint-initdb.d/` runs. The operator runs `initdb` itself, which makes `postgres` the bootstrap superuser (Postgres refuses to demote that role, and the demote migration leaves it alone, see above), and it creates `supatype_admin` ahead of the bootstrap when that role is declared under `managed.roles` with a password from a Secret. What the entrypoint would have done then falls to a Job the platform runs against the socket, once the cluster is ready:

```bash
# first boot: roles, schemas, every migration, and the service passwords
POSTGRES_DB=app POSTGRES_HOST=/controller/run \
AUTHENTICATOR_PASSWORD=... SUPATYPE_SQL_EDITOR_PASSWORD=... \
  /docker-entrypoint-initdb.d/00-supatype-bootstrap.sh

# every later start, and after every image upgrade
POSTGRES_DB=app POSTGRES_HOST=/controller/run supatype migrate sync
```

- `supatype_admin` under `managed.roles` needs `login: true` and `superuser: true`: every migration runs as it, as does the image's own first boot, where it is `POSTGRES_USER`. Set `enableSuperuserAccess: false` as well, so the bootstrap `postgres`, which cannot be demoted, has no password.
- `POSTGRES_PASSWORD` stays unset: the operator owns `supatype_admin`'s password, and the bootstrap leaves the role as it found it. Set, it would be written over the operator's, and the operator would not put its own back until its Secret next changed.
- `AUTHENTICATOR_PASSWORD` is required, since there is no `POSTGRES_PASSWORD` to fall back to. Without it the bootstrap refuses to start, before it has touched the database.
- The bootstrap runs as the `postgres` OS user and connects as both `postgres` and `supatype_admin` over the socket. The operator's own `pg_hba` rule for the socket is `local all all peer map=local`, so `pg_ident` needs the line `local postgres supatype_admin` (map `local`, OS user `postgres`, role `supatype_admin`) beside the operator's `local postgres postgres`.
- The image is Debian-based: set `postgresUID` and `postgresGID` to 999.
- `config_file` and `hba_file` cannot be passed under an operator. Every setting in [config/](config/) is a plain parameter, including `cron.database_name`, which the entrypoint would otherwise write from `POSTGRES_DB`.

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
  pg_keyspace/                  Bundled RESP keyspace + row cache (Rust/pgrx; built in-image)
  supatype_mask/                Bundled column masking C extension source
migrations/
  db/
    init-scripts/               Run once on first database initialisation
    migrations/                 Incremental schema migrations
    migrate.sh                  First-boot bootstrap (installed as 00-supatype-bootstrap.sh)
    apply-migrations.sh         Migration runner and ledger (installed as `supatype migrate`)
scripts/
  supatype                      `supatype <command>` dispatcher (installed on PATH)
  supatype-entrypoint.sh        Image ENTRYPOINT; applies migrations to existing clusters
  build-native.sh               Linux and macOS archive build, run by CI and locally
  build-native-windows.sh       Windows archive build, run in MSYS2 by CI and by build-native-windows.ps1
  build-native-extensions.sh    The upstream extensions every native archive carries, and their pins
  lib.sh                        Helpers the native build scripts share
  linux-postgres-wrapper.sh     bin/postgres in the Linux archives: points OpenSSL at the host's CA bundle
tests/
  pg_upgrade/                   PostgreSQL upgrade regression tests
  native-archive/               Native archive checks: every Windows DLL loads, and an archive starts the way `supatype dev` starts it, creates its extensions and decodes with wal2json, and a Linux archive does the same in a clean Fedora container
```

---

## CI

| Workflow | Trigger | What it does |
|---|---|---|
| `build-release.yml` | Push to `main`/`develop`/`release/*`, tag | Builds multi-arch image, pushes to Docker Hub |
| `docker-image-test.yml` | PRs, push to `develop` | Builds image, verifies extensions load, that migrations apply on both a fresh volume and an existing one, and that `doctor --fix` repairs a migration that never took effect |
| `native-archives.yml` | Tag push | Builds native PG17 tarballs for all platforms, uploads to CDN and GitHub Release |
| `test-pg-guard.yml` | PRs touching `extensions/pg_guard/**` | Runs pg_guard regression suite |
| `test-pg-keyspace.yml` | All PRs, push to `main`/`develop` | Unit tests, RESP conformance against a real redis, macOS/kqueue build, and durability + fault injection inside PostgreSQL. No `paths:` filter: two of its checks are required on `develop`, and GitHub never queues a workflow whose path filter misses, so the filtering lives in a `changes` job every other job keys off |
| `test-supatype-mask.yml` | PRs touching `extensions/supatype_mask/**` | Runs the masking regression suite |

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

Every archive carries the server, the client programs and contrib (pgcrypto, unaccent, pg_stat_statements and the rest). The extensions on top of that, at the same versions as the image (pinned in `scripts/build-native-extensions.sh`, which the Dockerfile runs too; the image's PostGIS is PGDG's package instead):

| Extension | Linux | macOS | Windows | Notes |
|---|---|---|---|---|
| pg_guard, supatype_mask | yes | yes | yes | Built from this repo |
| pgvector, pgjwt | yes | yes | yes | |
| wal2json | yes | yes | yes | The output plugin realtime decodes with |
| pg_cron, pg_safeupdate, pg_plan_filter | yes | yes | yes | |
| pg_net | yes | yes | no | Its event loop is epoll or kqueue; there is no Windows one |
| PostGIS (no raster, no protobuf) | yes | no | yes | GEOS, PROJ and their data are bundled; macOS does not bundle them yet |
| pg_keyspace, supacache_keys | yes | yes | no | pgrx has no Windows target |
| pg_graphql | no | no | no | Docker image only, see below |

pg_cron, pg_net, pg_plan_filter and pg_safeupdate only work preloaded, the way `config/postgresql.conf` preloads them for the image, and pg_cron also needs `cron.database_name`; whoever starts the server (native `supatype dev`) has to pass those settings. Realtime needs `wal_level = logical`.

**pg_graphql is Docker-only.** It is a pgrx extension, and the image installs the upstream release package, which targets a PGDG Postgres rather than these archives. The `/graphql/v1` endpoint therefore answers only on a stack running the `supatype/postgres` image; native dev has no GraphQL.

Extension libraries live in the server's `$libdir` (`lib/postgresql/`). The Linux archives are self-contained above glibc: every other shared library they load (ICU, OpenSSL, readline, GEOS, PROJ, libcurl and the rest) is in `lib/`, and every program and module finds it through an `$ORIGIN` RUNPATH, so they run on any distribution with glibc 2.35 (Ubuntu 22.04) or later and its C++ runtime. Both Linux archives are built on Ubuntu 22.04 for that reason, linux-arm64 on a native arm64 runner; one cross-compiled from an x86_64 host has none of the C or Rust extensions. The archive's OpenSSL was built to look for CA certificates in Ubuntu's `/usr/lib/ssl`, so `bin/postgres` is a small script that starts the server (`bin/postgres.bin`) with `SSL_CERT_FILE` set to the host's CA bundle, the first of the usual locations that exists, unless `SSL_CERT_FILE` or `SSL_CERT_DIR` is already set: without it, pg_net's HTTPS requests fail certificate verification on Fedora, RHEL and anything else that keeps them elsewhere.

To build a native archive locally, the same way CI does (`--help` lists the options, such as `--ccache`):

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
- Loaded via `session_preload_libraries`, so it is active on every connection; can be overridden per-role via `ALTER ROLE ... SET session_preload_libraries`

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
