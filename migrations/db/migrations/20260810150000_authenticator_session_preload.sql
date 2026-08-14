-- migrate:up

-- PostgREST now connects as `authenticator` rather than as `supatype_admin` (P5.3), which
-- makes this role's per-role settings load-bearing for the entire API surface for the
-- first time.
--
-- A per-role `session_preload_libraries` **replaces** the postgresql.conf value, it does
-- not add to it. The legacy Supabase-inherited setting named only `safeupdate`, so
-- connecting as this role would have silently stopped `pg_guard` loading for every API
-- session. Name both explicitly rather than relying on either default.
--
-- `supatype_mask` is deliberately absent here and must stay absent: it lives in
-- `shared_preload_libraries` (PGC_POSTMASTER) precisely so that no per-role setting can
-- drop it. Adding it here would reintroduce the bypass that placement closed.
-- Unquoted, comma-separated — same form as 20220224211803. Quoting the whole value makes
-- Postgres treat "pg_guard, safeupdate" as a single filename and every login then fails
-- with `FATAL: could not access file`, which for this role means the entire REST API is
-- down. Verified both ways before committing.
ALTER ROLE authenticator SET session_preload_libraries = pg_guard, safeupdate;

-- migrate:down
