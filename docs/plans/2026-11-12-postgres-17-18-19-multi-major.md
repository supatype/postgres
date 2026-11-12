# Postgres 17, 18 and 19: build all three, default to the newest, keep them current

**Status:** proposed, 2026-11-12
**Repos:** `supatype/postgres` (image + native archives), `supatype/supatype` (CLI), `supatype/cloud` (control plane)
**Owner:** nic-jennings

## Why

`supatype/postgres` builds one Postgres major. The number is hard-coded in the Dockerfile, in
every workflow, in the archive names, in the release tag scheme (`v17.2.14`) and in the CDN layout.
PG 18 has been GA for fourteen months, PG 19 reaches GA on 2026-11-19, and the concern that the
platform is stuck on 17 is fair. Two things make it worse than "one major behind":

- **The native archives build PostgreSQL 17.2 from source**, pinned in `scripts/build-native.sh:16`
  and `.github/workflows/native-archives.yml:12`. 17.2 is from November 2024. 17.12 shipped today
  with fixes for 52 CVEs. Every `supatype dev` on a native archive runs a server ten minors behind.
  The Docker image does not have this problem (it takes the current minor from `postgres:17-bookworm`
  at build time), so the image and the archives already disagree about which Postgres "17.2.14" is.
- **The release tag doubles as the Postgres version.** `v17.2.N` cannot express "the same repo
  commit, built for 18", and `17-latest` / `latest` are the same digest by construction.

This plan makes the major a build parameter, ships 17, 18 and 19 from one commit, moves the
default to 18 now and to 19 once it is GA and complete, and sets up the machinery to stay
current without anyone having to remember.

## Where things stand upstream (verified 2026-11-12)

| | PG 17 | PG 18 | PG 19 |
|---|---|---|---|
| Status | 17.12 (today) | 18.7 (today), GA 2025-09-25 | RC2 (today), GA planned 2026-11-19 |
| EOL | 2029-11-08 | 2030-11-14 | ~Nov 2031 |
| Official image | `17-bookworm`, `17-trixie` (default trixie) | `18-bookworm`, `18-trixie` | `19rc2-bookworm`, `19rc2-trixie`; GA tags after 11-19 |
| PGDG apt | yes | `postgresql-server-dev-18`, `postgresql-18-postgis-3` | `postgresql-server-dev-19`, `postgresql-19-postgis-3` = PostGIS 3.7.0 (3.6.4 build crashed on RC1: ABI churn) |

Extension readiness against the pins we ship today (`scripts/build-native-extensions.sh`, `Dockerfile`):

| Extension | Pinned | Needed for 18 | Needed for 19 | Note |
|---|---|---|---|---|
| pgvector | 0.8.0 | ≥ 0.8.1 (use 0.8.7) | 0.8.7 (CI-tested, no changelog line) | |
| pg_graphql | 1.5.9 | ≥ 1.5.12 (use **1.6.3**, security fix, pg18 .deb) | **none**: pgrx 0.19.2, no pg19 feature | blocker for a complete 19 image |
| pg_net | 0.14.0 | ≥ 0.19 | ≥ 0.20.5 ("pg19beta1 compatibility") | 0.14 → 0.20 is a large jump; check the `net.*` surface supatype calls |
| pgjwt | commit f3d82fd | fine (SQL only) | fine | |
| pg_cron | 1.6.8 | fine | fine (1.6.8 added 19) | |
| pg_safeupdate | 1.7 | fine | fine (1.7 added 19) | |
| pg_plan_filter | commit c1afc73 | master ≥ 2026-07-21 ("14–18") | **open issue #11** (2026-11-04) | tiny planner hook; patch or omit on 19 |
| wal2json | 2.7 | fine | fine | |
| PostGIS (native) | 3.6.4 | fine | **3.7.0** | image uses PGDG's package |
| pgrx (pg_keyspace, pg_topics) | **0.12.9** | ≥ 0.16.1 | **0.20.0** (released today; pg15–19, Rust 1.99.1) | biggest single porting item |
| pg_guard | in repo | already has `PG_MODULE_MAGIC_EXT` for 18 | compile-check against RC2 | uses ProcessUtility + fmgr hooks only |
| supatype_mask | in repo | compile-check | compile-check | uses `planner_hook` only; the 19 removals (`get_relation_info_hook`, `bit*` typedefs) do not touch it |
| supacache_keys plugin | in repo | compile-check | compile-check | logical decoding; `-Werror=incompatible-pointer-types` will catch struct changes |

Two PG 18 facts that reach into the CLI and cloud, not just the image:

- **PGDATA moved.** The official `postgres:18` image sets `PGDATA=/var/lib/postgresql/18/docker` and
  declares `VOLUME /var/lib/postgresql`. Everything we have mounts at `/var/lib/postgresql/data`
  (`docker-postgres.ts:69`, `self-host-compose.ts:1049`, cloud `project-template/postgres.yaml`).
  Mounting the old path on an 18 image has caused silent data loss upstream (docker-library #1400).
- **Checksums default on.** `pg_upgrade` refuses when old and new clusters disagree. Cloud passes
  `--data-checksums` already; the CLI never does, so every dev cluster made by `supatype dev` has
  checksums off and the upgrade tool must initdb the new cluster with `--no-data-checksums` (or run
  `pg_checksums --enable` on the old one first).

Reference point: Supabase ships 15 and 17, closed its PG 18 PR in favour of 19, and has no 19 branch
yet. Shipping 18 today puts us ahead of them; shipping 19 at GA puts us ahead by a year.

## Decisions

These are the calls the rest of the plan depends on. Each has a recommendation; change it here
and the phases below follow.

1. **Debian base: stay on bookworm for all three majors now; move to trixie later, all majors at once.**
   The in-place upgrade path copies the old major's binaries out of the old image into the new
   image's container, which only works when both are the same distro (the same constraint
   CloudNativePG documents). Mixing `17-bookworm` with `18-trixie` would break every 17 → 18 upgrade.
   Trixie is a separate, later change that rebuilds everything.
2. **Release tags stay `v<major>.<minor>.<build>`, one tag per major, cut from the same commit.**
   `v17.12.0`, `v18.7.0`, `v19.0.0`. The CLI already takes the major from the first component
   (`postgresArchiveTag`, `pinnedImage`) and compares full versions for its feature gates
   (`KEYSPACE_TOGGLE_SINCE = "17.2.9"` etc.), so an 18.x version passes every gate without code
   changes. Each tag builds only its own major, so a hotfix to one major does not rebuild five
   native targets for the others. The build asserts the running server's minor equals the tag's,
   so a tag can no longer claim 17.2 while shipping 17.12.
3. **The 18 and 19 images set `PGDATA=/var/lib/postgresql/data` explicitly**, matching 17, so the
   one mount path every consumer already uses keeps working and nothing in the CLI or cloud has to
   pick a path by major. The upgrade tool works inside that directory (old cluster moved to
   `data/pg17-old`, new cluster built beside it, hard-linked, then promoted). Adopting upstream's
   per-major layout can be revisited when the mount path is changed for other reasons.
4. **PG 19 ships as `preview` without pg_graphql** until upstream (or our fork) adds pg19.
   `/graphql/v1` already answers 501 when the extension is missing, so this degrades cleanly.
   pg_plan_filter is patched locally if the fix is small, otherwise omitted on 19 only.
5. **Default major: 18 now, 19 after its first minor (19.1, ~February 2027) and pg_graphql.**
   17 stays `supported` until upstream EOL. We do not skip 18: it is GA with every extension ready,
   and 19 is not GA.
6. **Nothing upgrades an existing database automatically.** New projects get the default major.
   Existing ones upgrade when the user (CLI) or operator (cloud) runs the upgrade command, after
   a backup, with the old data directory kept for rollback until they remove it.

## Phase 0: hygiene, independent of multi-major (this week)

These are overdue regardless and each is a small PR against `supatype/postgres`:

- [ ] Bump native `PG_VERSION` 17.2 → 17.12 (+ sha256) in `scripts/build-native.sh`,
      `scripts/build-native-windows.sh`, `native-archives.yml`. This also deletes the macOS
      `strchrnul` workaround (`build-native.sh:228-231` says so) and the Windows `-std=gnu17`
      note about 17.2's `bool` typedef can be re-checked.
- [ ] `PG_GRAPHQL_VERSION` 1.5.9 → 1.6.3 (security release 2026-11-02).
- [ ] `PGVECTOR_VERSION` 0.8.0 → 0.8.7.
- [ ] `PG_NET_VERSION` 0.14.0 → latest 0.20.x, **after** checking what supatype calls
      (`net.http_get/post/delete`, `net._http_response`, worker settings). This one may need a
      migration; if so, split it out.
- [ ] Add a step to `docker-image-test.yml` and `build-release.yml` that reads
      `postgres --version` from the built image and fails unless its major.minor matches the tag
      (tag builds) or the pin file (branch builds). Prevents the 17.2-vs-17.12 drift recurring.
- [ ] Delete `tests/pg_upgrade/` (Supabase leftovers: `debug.sh` references files that do not
      exist) so the name is free for the real thing in Phase 1.
- [ ] Release as `v17.12.0`. The CLI's `release/images.json` pin and `bump-pins.yml` handle a
      version that is not `17.2.x` already (the tests in `postgres-image.test.ts` are the place to
      prove it).

## Phase 1: `supatype/postgres` builds any major; ship 18

**Goal:** `supatype/postgres:18-latest`, `supatype-pg-18-*` archives and `postgres/18/latest.json`
exist and pass the same tests as 17. `supatype/postgres:17-latest` is unchanged for consumers.

### 1a. One place per major for the version facts

```
majors/17.env   PG_MAJOR=17 PG_VERSION=17.12 PG_SHA256=… PG_GRAPHQL_VERSION=1.6.3 POSTGIS_VERSION=3.6.4 STATUS=supported
majors/18.env   PG_MAJOR=18 PG_VERSION=18.7  PG_SHA256=… PG_GRAPHQL_VERSION=1.6.3 POSTGIS_VERSION=3.6.4 STATUS=default
majors/19.env   PG_MAJOR=19 PG_VERSION=19.0  PG_SHA256=… PG_GRAPHQL_VERSION=    POSTGIS_VERSION=3.7.0 STATUS=preview
```

Extension pins that do not vary by major stay in `build-native-extensions.sh` between the
`# pins: begin/end` markers (the ccache key already hashes those lines). Anything that does vary
(PostGIS on 19, pg_graphql absent on 19) moves to the env file. A `scripts/majors.sh` helper
answers `supported`, `default`, `preview` and `for-tag v18.7.0 → 18` so workflows and scripts stop
parsing by hand.

### 1b. Dockerfile parameterised by `ARG PG_MAJOR`

- `FROM postgres:${PG_MAJOR}-bookworm` in both stages (ARG before the first FROM).
- `/usr/lib/postgresql/${PG_MAJOR}/…`, `postgresql-server-dev-${PG_MAJOR}`,
  `postgresql-${PG_MAJOR}-postgis-3`, `cargo pgrx init --pg${PG_MAJOR}`,
  `cargo pgrx package` output dir `pg_keyspace-pg${PG_MAJOR}`.
- pg_graphql: install only when `PG_GRAPHQL_VERSION` is non-empty; `.deb` name carries
  `pg${PG_MAJOR}`.
- `ENV PGDATA=/var/lib/postgresql/data` (decision 3). `tzdata-legacy` is not needed on bookworm.
- OCI labels: `org.opencontainers.image.version` = release tag, plus a
  `com.supatype.postgres.major` label so cloud and the CLI can read the major off an image without
  starting it.
- Copy `majors/${PG_MAJOR}.env` into the image at `/etc/supatype/postgres.env` so the entrypoint,
  `supatype migrate` and the upgrade tool know which major they are.

### 1c. pgrx 0.12.9 → 0.20.0 for pg_keyspace (and pg_topics)

This is the largest engineering item and the one most likely to slip. 0.12 → 0.20 crosses several
breaking pgrx releases (SPI API, `pg_sys` bindings, `#[pg_guard]` changes, the bgworker builder,
shared memory types). Approach:

- Bump in a branch with features `pg17`, `pg18`, `pg19`; drop `pg13`–`pg16` (0.20 dropped 13/14
  and we ship none of them). Default feature `pg18`.
- Rust toolchain pin to what pgrx 0.20 needs (1.99.1+) in `rust-toolchain.toml`; the keyspace
  workflow's "ubuntu-24.04 because 0.12.9 cannot build bindings on 26.04" comment goes away.
- Run the whole `test-pg-keyspace.yml` suite (unit, RESP conformance, durability + fault
  injection, macOS kqueue) on 17 and 18 before merging; 19 can be allow-failure until GA.
- The bench scripts under `extensions/pg_keyspace/bench/` default `PGBIN` to
  `/usr/lib/postgresql/17/bin`; switch them to read `PG_MAJOR` with 17 as fallback.

If this slips, 18 can ship first **without** pg_keyspace in the image (the CLI falls back to the
Valkey sidecar when the library is missing; cloud's keyspace tiers would need a gate) and the
pgrx bump lands as its own release. Decide at the end of week 2.

### 1d. In-repo C extensions

- `pg_guard`: compiles on 18 already (`PG_MODULE_MAGIC_EXT` guard at `pg_guard.c:36`,
  `PG_EQ18` in the Makefile). Add 18 and 19 to `test-pg-guard.yml`'s matrix.
- `supatype_mask`: `planner_hook` only; compile and run the regression suite on 18 and 19.
  Its `test/Dockerfile` and `test/run-tests.sh` take the major as a parameter.
- `supacache_keys`: compile on 18 and 19; the `ReorderBuffer` changes between 16 and 17 were the
  last break and the Makefile's `-Werror=incompatible-pointer-types` catches the next one.
- All three test workflows: `matrix.pg: [17, 18, 19]`, 19 `continue-on-error: true` until GA.

### 1e. Workflows derive everything from the tag or the matrix

- `build-release.yml`: tag `v18.7.0` → `PG_MAJOR=18`; builds `${IMAGE}:18.7.0-{amd64,arm64}`,
  merges to `18.7.0`, re-points `18-latest`. `latest` is re-pointed **only** when the major is the
  default in `majors/`. Branch builds matrix over every non-EOL major: `develop-17`, `develop-18`,
  `develop-19`, with `develop` = default major.
- `docker-image-test.yml`: matrix over majors on PRs. Three 30-minute jobs in parallel; the gha
  cache scope gains the major. The "verify extensions" list skips pg_graphql where the env file
  leaves it empty.
- `native-archives.yml`: tag → major; `supatype-pg-${PG_MAJOR}-<target>`; CDN path stays
  `postgres/v<release>/…` (release tags are unique across majors because the first component
  differs). `latest.json` becomes **per major**: `postgres/18/latest.json`, and the existing
  `postgres/latest.json` is rewritten to the **default** major's release so CLIs in the field
  keep resolving. The checksums manifest and minisign signature are unchanged in shape.
- A `release.yml` `workflow_dispatch` with input `majors` (default: all supported) that creates
  the N tags from one commit, so "release everything" is one click and not three `git tag`s.

### 1f. Upgrade tooling that lives in the image

`supatype upgrade` (a new libexec subcommand beside `migrate`, following the dispatcher pattern in
`scripts/supatype`) does the in-place major upgrade inside a container that has the data volume
mounted and the **old major's binaries** available at a known path:

1. Refuse unless the server is stopped and `PG_VERSION` in the data dir is lower than this image.
2. Check checksum state with `pg_controldata`; initdb the new cluster to match
   (`--no-data-checksums` when the old one has none), with the same locale/encoding/`--auth` the
   bootstrap would use.
3. Drop logical replication slots (realtime's wal2json slots, pg_keyspace's row-cache slot) and
   record them; they are recreated by their owners on the next start.
4. `pg_upgrade --link --check`, then `pg_upgrade --link`, old cluster at `data/pg<old>-old`,
   new one promoted to `data/`.
5. `ALTER EXTENSION … UPDATE` for every extension with a new default version
   (`postgis_extensions_upgrade()` for PostGIS), `supatype migrate sync`, `supatype migrate doctor`,
   `vacuumdb --all --analyze-in-stages` (18's pg_upgrade keeps planner stats, 17's did not).
6. Leave `data/pg<old>-old` and a `data/UPGRADE.json` (from, to, when, slots dropped) for
   rollback; `supatype upgrade --discard-old` removes it.

The old binaries come from the old image: the caller runs the old image once with
`--entrypoint cp` to copy `/usr/lib/postgresql/<old>` and `/usr/share/postgresql/<old>` into a
scratch volume mounted into the new image at `/opt/supatype/pg<old>`. That is what CloudNativePG's
upgrade Job does with an init container and why decision 1 (same distro) holds.

A new `tests/pg_upgrade/` CI job: start the 17 image on a volume with fixtures (the SQL under the
old `tests/pg_upgrade/tests/` is reusable), stop it, run the 18 image's `supatype upgrade`,
assert the data, the extensions, `doctor`, and that realtime can create a wal2json slot again.

### 1g. Docs

README gains a **support matrix** at the top (major, status, image tag, archive name, upstream
EOL, our default since/until) and the tag table becomes per-major. The "PostgreSQL 17 with a
curated set of extensions" line goes.

**Exit criteria for Phase 1:** `v17.12.0` and `v18.7.0` released from one commit; the pg_upgrade
CI job green for 17 → 18; the support matrix published.

## Phase 2: PG 19 preview (from 2026-11-19)

- Add `majors/19.env` with `STATUS=preview`, `PG_GRAPHQL_VERSION=` empty, PostGIS 3.7.0 for the
  native build, `postgresql-19-postgis-3` in the image.
- Rebuild **after GA**, never from RC headers: PGDG's own PostGIS for 19 crashed because it was
  built against beta. Our beta/RC builds are for CI only and are never tagged.
- pg_plan_filter: try the one-line fix for the removed `bit*` typedefs / hook changes against
  upstream master; if it is more than that, exclude it on 19 and open the issue upstream.
- pg_graphql: open an upstream PR adding a `pg19` feature on pgrx 0.20 (their Cargo.toml pins
  0.19.2). If upstream is slow, carry a fork built in a Dockerfile stage like pg_keyspace rather
  than a .deb. Until then 19 lacks `/graphql/v1`.
- `19-latest` and `supatype-pg-19-*` publish like the others. `latest` and `postgres/latest.json`
  do not move to 19.
- Promotion to `default` is a one-line change in `majors/` once 19.1 is out and pg_graphql is in.

## Phase 3: `supatype` CLI knows about majors

The CLI already derives the major from the version string everywhere, so this is mostly pins,
a default, and safety rails. File references are to `packages/cli/src/`.

- [ ] `release/images.json`: one postgres pin per major plus `defaultMajor`. `scripts/release-images.mjs`,
      `scripts/pin.sh`, `.github/workflows/bump-pins.yml` learn the shape; `generated/release-images.ts`
      exports `PINNED_POSTGRES_BY_MAJOR` and `DEFAULT_POSTGRES_MAJOR`. `release-images.ts`
      `pinnedComponentVersion("postgres")` takes the major.
- [ ] Config: `versions.postgres` keeps its semantics (`"18"` → `18-latest`, `"18.7.0"` exact).
      When unset, the resolver (`postgres-image.ts:130-137`) uses the pinned release of
      `defaultMajor`. `init.ts:1083` writes the current default major into new configs so a project
      created on 18 stays on 18 when the default later moves to 19. Fix the stale comment at
      `project-config.ts:210-211`.
- [ ] Native: `binary-cache.ts` `fetchLatestVersion` reads `postgres/<major>/latest.json`;
      `resolveVersionFor` takes the major. Delete the duplicated archive-name template at
      `dev.ts:1326` in favour of `binaryName`.
- [ ] **Data safety (the new code that matters):**
      - Native: `postgres-ctl.ts ensureCluster` reads `<dataDir>/PG_VERSION` and refuses to start
        binaries of another major, with the upgrade command in the message. Today it only checks
        the file exists (`:213-229`).
      - Docker: before `docker run`, read `PG_VERSION` from the volume (a throwaway
        `alpine cat` run, or the image's own `supatype upgrade --check`) and refuse on mismatch.
        Volume names (`${name}-data`, `DB_VOLUME_KEY`) stay major-agnostic so an upgraded volume
        keeps its name.
- [ ] `supatype db upgrade [--to 18]`:
      - Native: both archives are in `~/.supatype/cache/postgres/<version>/`; run the new
        archive's `pg_upgrade` with `-b <old>/bin -B <new>/bin` and the steps from 1f.
      - Docker / self-host compose: stop the db service, copy old binaries out of the old image
        into a scratch volume, run `supatype upgrade` in the new image with both volumes mounted,
        then update `SUPATYPE_POSTGRES_IMAGE` in `.env` and start.
      - Always: `pg_dumpall --globals-only` + `pg_dump -Fc` to `~/.supatype/backups/` first unless
        `--no-backup`.
- [ ] `supatype db check` / `db-preflight.ts:428`: keep `MIN_MAJOR = 17` as the floor; add
      "server major N is newer than the newest tested major M" as a warning and "major N is
      deprecated, EOL <date>" as a notice. The SQLSTATEs measured against the image in
      `packages/realtime` and the preflight were measured on 17; re-measure on 18 and 19 and
      record the major they were measured on.
- [ ] Tests under `packages/cli/tests/` with a literal 17 (`postgres-image.test.ts`,
      `archive-name-alias.test.ts`, `pg-spawn-env.test.ts`, `support/child-script.ts`,
      `postgres-ensure-cluster.test.ts`, `native-pg-auth.test.ts`, …) run their matrix over
      17 and 18. `packages/realtime/tests/fixtures/tier2/make-db.sh`, `packages/storage/tests/support/stack.ts`
      and `tests/e2e/masking/...` move off `postgres:17-alpine` to the pinned image.
- [ ] Docs: `UPGRADING.md` gets a "Changing the Postgres major" section; `docs-content/`
      mentions of 17 become "the default major (18)"; `skills/supatype/references/self-host.md`
      pin table gains 18 and 19 rows; `examples/*/supatype.config.ts` pins update;
      `docs/postgres.html` tag table becomes per-major.

**Exit criteria:** a fresh `supatype dev` runs 18 on every platform; a project with a 17 data dir
is refused by 18 binaries with a message naming `supatype db upgrade`; that command works for
native and Docker on CI.

## Phase 4: `supatype/cloud` stores and chooses the major

There is no CloudNativePG in cloud. Every Postgres is a one-replica StatefulSet with one PVC, the
image is the literal `supatype/postgres:17-latest` in three files, no per-project version is stored,
and nothing upgrades. Sequence against the open PRs (all by nic-jennings; see the end of this doc):

- [ ] **Land #84 first** (it refactors `buildVars` into `identityVars / credentialVars / imageVars /
      postgresVars / keyspaceVars`). The Postgres image variable belongs in its `imageVars()`;
      doing it before #84 means a conflict in `project-resources.ts`.
- [ ] Schema (`src/db.ts`): `cloud.projects.pg_major SMALLINT NOT NULL DEFAULT 17` with a
      `CHECK` against the supported set, and the same column on `cloud.environments` if staging
      may run ahead of production. Backfill is the default. Widen `provision_jobs.kind` with
      `upgrade_postgres`.
- [ ] Template: `image: "{{POSTGRES_IMAGE}}"` in `infra/k8s/project-template/postgres.yaml:26`,
      populated from a per-major map in env/config
      (`SUPATYPE_POSTGRES_IMAGE_17=supatype/postgres:17.12.0`, `_18=…`). **Exact tags, not
      `-latest`**, and an explicit `imagePullPolicy: IfNotPresent`: today a pod rescheduled to a
      fresh node silently picks up a newer `17-latest` digest. `public-env.test.ts` (from #84)
      asserts no `{{…}}` survives rendering, so the var must be in `buildVars`.
- [ ] Platform DB and Kong (`infra/k8s/platform/postgres.yaml:70,238`): a Pulumi config key
      `platformPostgresImage` with a transformation in `infra/pulumi/platform/index.ts`, plus a
      `pulumi-set-postgres-image.yml` dispatch workflow modelled on `pulumi-set-server-image.yml`.
      Add a Postgres row to `CANONICAL-IMAGES.md`.
- [ ] `src/config/shared-postgres.ts:221` is deleted by **#73**; do not touch it.
- [ ] Any new Service/StatefulSet naming goes through **#48**'s `stack-naming.ts`.
- [ ] Decide how an image change reaches a running StatefulSet in light of **#82**, which turns
      `kubeApplyYaml` from delete-and-recreate into server-side apply. Either way, the upgrade
      flow must **never** let a provision/resume point an 18 image at a 17 PVC: the image var is
      derived from the stored `pg_major`, so re-provisioning a 17 project renders a 17 image.
- [ ] Validation gate for the default: **#55**'s `db-crash-hunt.yml` has a `postgres_image`
      input. Run it and `pnpm test:db` against `supatype/postgres:18.7.0` and make green a
      precondition for setting the new-project default to 18. PostgREST is bumped to 14.18 in
      **#39**; 14.x supports 18, verify against 19 before 19 goes default.
- [ ] Upgrade Job (`src/services/postgres-upgrade.ts`), run from a `provision_jobs` row:
      1. `ProjectResources.pause()` (scales the data plane and the STS to 0; exists).
      2. A `batch/v1 Job` in the project namespace: init container = old image, copies
         `/usr/lib/postgresql/17` + `/usr/share/postgresql/17` into an `emptyDir`; main
         container = new image, mounts the PVC at `/var/lib/postgresql/data` and the emptyDir at
         `/opt/supatype/pg17`, runs `supatype upgrade` (1f). `backoffLimit: 0`.
      3. On success: `UPDATE cloud.projects SET pg_major = 18`, patch the STS image via the
         normal render path, `resume()`, `audit_log` `project.postgres_upgraded`.
      4. On failure: the old cluster is untouched at `data/pg17-old`; the Job's log is the
         `last_error`; `resume()` with the 17 image after the operator restores the directory
         (a `supatype upgrade --rollback` in the image does the move back).
      5. PVC headroom: `--link` needs little, but `ensurePostgresVolume` should check free space
         against the `pg_controldata` size before starting.
      Backups are "coming soon" per **#36**, and `backups.ts` never executes `pg_dump`. Until a
      real backup exists, the upgrade Job takes a `pg_dump -Fc` of the project into the storage
      bucket first and refuses without it. That gives the backup controller its first real caller.
- [ ] Surfaces: Studio shows the project's major and "upgrade available" when `pg_major <
      defaultMajor`; CLI `supatype cloud projects` lists it; the create-project route accepts
      `pg_major` from the supported set and defaults it.
- [ ] Docs: `field-access.mdx:230` and `external-database.mdx:105` say "17 or later", which stays
      true; `provision-runbook.md` gains the upgrade runbook and error codes.

**Exit criteria:** new projects provision on 18; a staging environment upgraded 17 → 18 through
the Job and rolled back once in a test cluster; every existing project still renders a 17 image.

## Phase 5: move the defaults

| When | Change |
|---|---|
| Phase 1 done | `majors/18.env STATUS=default`; `latest` and `postgres/latest.json` → 18. CLI `defaultMajor = 18`. Cloud new-project default 18 after the crash-hunt gate. |
| 19.0 GA (2026-11-19) + rebuild | 19 `preview` everywhere; opt-in via `versions.postgres: "19"` / `pg_major: 19`. |
| 19.1 (~Feb 2027) + pg_graphql on 19 | 19 `default`; 18 `supported`. |
| 2027-H2 | Nudge: CLI notice and Studio banner for projects still on 17. |
| 2029-11 | 17 EOL upstream: `STATUS=eol`, no more builds; refuse new projects on it. |

## Keeping on top of versions

The honest answer to "how do I keep on top of this" is that the current setup had no signal at
all: nothing told anyone that native archives were on 17.2, that pg_graphql had a security
release, or that 18 existed. The fix is mostly automation plus a written policy.

### Automation

1. **Renovate on `supatype/postgres`** (Mend's GitHub app; free for public repos). Custom
   `regexManagers` cover the shapes we already have:
   - `majors/*.env` `PG_VERSION=` against the `docker` datasource for `postgres` (or
     docker-library's `versions.json`), one PR per major when a minor lands.
   - the `# pins: begin … end` block in `build-native-extensions.sh` against `github-tags`
     (pgvector, wal2json, pg_cron, pg_safeupdate, pg_net) and `git-refs` (pgjwt, pg_plan_filter
     by commit), with the sha256 lines regenerated by a `postUpgradeTasks` script.
   - `PGRX_VERSION` and the `pgrx = "=x.y.z"` in both `Cargo.toml`s grouped into one PR.
   - `PG_GRAPHQL_VERSION` against supabase/pg_graphql releases.
   - GitHub Actions and the `FROM postgres:<major>-bookworm` lines pinned by digest.
   Renovate PRs carry the upstream release notes, which is the "what changed" the existing
   `extension_upgrade.md` PR template asks reviewers to check. CI (Phase 1 matrix) is the gate.
2. **A quarterly "Postgres minor" routine.** Upstream minors land the second Thursday of
   February, May, August and November (plus out-of-cycle security releases). Renovate opens the
   PRs the same day; the policy is that they are merged and `release.yml` run for every supported
   major **within seven days**. A GitHub Actions schedule (`cron: '0 9 * * 4'`) that compares the
   latest `v<major>.*` tag's minor with docker-library's `versions.json` and opens an issue titled
   "PG <major>.<minor> released, we ship <major>.<old>" is the reminder when a PR is sitting.
3. **A beta canary.** From each May (beta1) a `continue-on-error` matrix entry builds the image and
   runs the extension suites against `postgres:<next>beta` / `<next>rc`. The 19 cycle showed why:
   the hook removals, the `bit*` typedefs and the beta → RC ABI shift would each have surfaced
   months earlier. The canary is never tagged or published.
4. **CLI pins follow releases.** `bump-pins.yml` in `supatype/supatype` already commits the
   postgres pin; extend it to one pin per major so a `v18.7.1` release reaches the CLI's default
   without a human copying a number. `supatype update` tells the user when their project's pinned
   major is behind the default or within twelve months of EOL.
5. **Cloud drift report.** A scheduled job in cloud lists projects by `pg_major` and by exact image
   tag, and flags any running a tag that is not the current release for its major (the pod kept an
   old digest) or a major past `supported`. It is the input to a rolling restart during the
   maintenance window, which is how minors reach running projects.

### Policy (one paragraph in the README, kept true by the automation above)

- A new major is `preview` within two weeks of upstream GA, `default` after its first minor
  release and once every extension we ship is available on it.
- Every major we build gets every upstream minor within seven days, on all platforms, from one
  commit, as `v<major>.<minor>.<build>`.
- A major is `supported` until upstream EOL and `eol` after; `eol` majors are not built and new
  projects cannot choose them.
- The image's extension versions are pinned, bumped by Renovate, and identical between the image
  and the native archives (already true today; keep it).
- Nothing upgrades a database across majors without an explicit command, a backup, and a kept
  copy of the old cluster.

## Risks

| Risk | Mitigation |
|---|---|
| pgrx 0.12.9 → 0.20.0 port of pg_keyspace takes longer than the rest of Phase 1 | Ship 18 without pg_keyspace behind a gate if not done by end of week 2; CLI already falls back to Valkey |
| pg_net 0.14 → 0.20 changes the SQL surface | Phase 0 item; test against supatype's hook/functions paths before bundling the bump into the major work |
| PG 19 ABI churn until GA | Build 19 only from GA sources; canary builds are never published |
| Checksum mismatch stops `pg_upgrade` on CLI-made clusters | Upgrade tool reads `pg_controldata` and initdbs to match |
| A cloud re-provision points a new image at an old PVC | Image is rendered from the stored `pg_major`, never from a global default |
| Three majors triple CI minutes (macOS runners especially) | Per-major tags build one major at a time; branch builds skip native archives; `docker-image-test` runs 17 and the default on PRs, all majors on `develop` |
| Users land on 18 and are asked to move to 19 within a year | Nobody is forced: 18 is supported to 2030; the nudge is a notice, not a block |

## Open PRs on `supatype/cloud` relevant to this plan (2026-11-12)

52 open, all by nic-jennings; #34–#78 are a 45-deep stacked release train for 0.7.0. None touch
the Postgres major or the `supatype/postgres` tag. The ones that shape Phase 4:

| PR | Why it matters | Do |
|---|---|---|
| #84 feat(provision): environment server `/supatype-env.json` | Refactors `buildVars` into `imageVars()` etc. | Land first; add `POSTGRES_IMAGE` there |
| #82 feat(hooks,functions): in-place worker applies | `kubeApplyYaml` → server-side apply; immutable-field refusal | Decide rollout semantics for image changes against it |
| #73 chore/remove-shared-postgres | Deletes `shared-postgres.ts` (one of three `17-latest` literals) | Leave that file alone |
| #48 fix/db-password-reset | Centralises "an environment's own Postgres" naming in `stack-naming.ts` | Route any new STS/Service through it |
| #55 fix/ci-split | Adds `db-crash-hunt.yml` with a `postgres_image` input and `pnpm test:db` | Use as the 18/19 validation gate |
| #39 fix/pgrst-up | PostgREST 14.18 everywhere | Verify against 19 before 19 goes default |
| #36 fix/docs | Marks backups/PITR "coming soon" | Upgrade Job provides the first real `pg_dump` caller |
| #52, #54, #60 | Provisioner role model and SQL editor grants | A dump/restore path must preserve them; `--link` upgrade does automatically |

`supatype/postgres` and `supatype/supatype` have no open PRs.

## Sequencing summary

```
wk 1      Phase 0 (hygiene, v17.12.0)            postgres
wk 1–3    Phase 1a–e (parameterise, 18 builds)   postgres
wk 1–3    Phase 1c (pgrx 0.20 port)              postgres   ← critical path
wk 3–4    Phase 1f–g (upgrade tool, matrix docs) postgres   → v17.12.0 + v18.7.0 together
wk 2–5    Phase 3 (CLI pins, safety, db upgrade) supatype
wk 2–6    Phase 4 (cloud column, template, Job)  cloud      after #84
wk 2+     Phase 2 (19 preview after GA)          postgres
wk 6      Phase 5 defaults → 18
ongoing   Renovate, quarterly minors, beta canary
```
