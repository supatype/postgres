/* pg_keyspace 0.1.0 -> 0.2.0
 *
 * Brings an existing install up to the catalogue the 0.2.0 shared library
 * expects. Postgres runs an extension's SQL only at CREATE EXTENSION, so a
 * cluster that takes the new pg_keyspace.so keeps whatever catalogue it had
 * when the extension was first created: fixes that live in the binary arrive
 * on their own, SQL objects do not.
 *
 * Every 0.1.0 install has the same catalogue. The v17.2.4 and v17.2.5 images
 * generate identical object sets -- their schema files differ only in pgrx's
 * deliberately unstable statement ordering -- so one script keyed at 0.1.0
 * covers both.
 *
 * Generated from the two schemas rather than transcribed by hand, and kept
 * honest by bench/run_extension_upgrade.sh, which asserts that an upgraded
 * 0.1.0 install and a fresh CREATE EXTENSION reach the same catalogue, down
 * to argument names, ACLs and comments.
 */

/* ---------------------------------------------------------------------------
 * Changed signatures.
 *
 * Dropped and recreated rather than replaced: CREATE OR REPLACE cannot change
 * a function's OUT parameters. Leaving the 0.1.0 entry in place would be worse
 * than having none -- the catalogue would promise a row shape the 0.2.0
 * wrapper no longer returns.
 * ------------------------------------------------------------------------ */

DROP FUNCTION IF EXISTS supacache.ring_stats();
CREATE  FUNCTION supacache."ring_stats"() RETURNS TABLE (
	"pushed" bigint,  /* i64 */
	"dropped" bigint,  /* i64 */
	"backlog_bytes" bigint,  /* i64 */
	"committed" bigint,  /* i64 */
	"lag" bigint,  /* i64 */
	"failed_batches" bigint,  /* i64 */
	"unresolved" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'ring_stats_wrapper';

/* ---------------------------------------------------------------------------
 * New functions.
 * ------------------------------------------------------------------------ */

CREATE  FUNCTION supacache."invalidation_stats"() RETURNS TABLE (
	"slot_name" TEXT,  /* alloc::string::String */
	"active" bool,  /* bool */
	"confirmed_flush_lsn" TEXT,  /* core::option::Option<alloc::string::String> */
	"restart_lsn" TEXT,  /* core::option::Option<alloc::string::String> */
	"current_lsn" TEXT,  /* core::option::Option<alloc::string::String> */
	"decode_lag_bytes" bigint,  /* core::option::Option<i64> */
	"retained_bytes" bigint  /* core::option::Option<i64> */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'invalidation_stats_wrapper';

CREATE  FUNCTION supacache."key_worker"(
	"key" TEXT /* &str */
) RETURNS INT /* i32 */
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'key_worker_wrapper';

CREATE  FUNCTION supacache."persist_shard_stats"() RETURNS TABLE (
	"worker" INT,  /* i32 */
	"shard" INT,  /* i32 */
	"pushed" bigint,  /* i64 */
	"dropped" bigint,  /* i64 */
	"backlog_bytes" bigint,  /* i64 */
	"committed" bigint,  /* i64 */
	"lag" bigint,  /* i64 */
	"uncommitted_batches" bigint,  /* i64 */
	"unresolved" bigint  /* i64 */
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'persist_shard_stats_wrapper';

CREATE  FUNCTION supacache."pubsub_stats"() RETURNS TABLE (
	"dropped" bigint,  /* i64 */
	"route_full" bigint,  /* i64 */
	"name_too_long" bigint  /* i64 */
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'pubsub_stats_wrapper';

CREATE  FUNCTION supacache."replication_status"() RETURNS TABLE (
	"tier" TEXT,  /* alloc::string::String */
	"standby_configured" bool,  /* bool */
	"sync_standbys_connected" bigint,  /* i64 */
	"honoured" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'replication_status_wrapper';

CREATE  FUNCTION supacache."rowcache_cached_pk_has_external"(
	"tbl" TEXT, /* &str */
	"pk" TEXT[] /* alloc::vec::Vec<core::option::Option<alloc::string::String>> */
) RETURNS bool /* core::option::Option<bool> */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_cached_pk_has_external_wrapper';

CREATE  FUNCTION supacache."rowcache_coherence"() RETURNS TABLE (
	"coherent" bool,  /* bool */
	"decode_enabled" bool,  /* bool */
	"beat_age_ms" bigint,  /* core::option::Option<i64> */
	"stale_after_ms" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_coherence_wrapper';

CREATE  FUNCTION supacache."rowcache_put_pk"(
	"tbl" TEXT, /* &str */
	"pk" TEXT[] /* alloc::vec::Vec<core::option::Option<alloc::string::String>> */
) RETURNS bool /* bool */
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_put_pk_wrapper';

CREATE  FUNCTION supacache."rowcache_register"(
	"tbl" TEXT /* &str */
) RETURNS bool /* bool */
STRICT 
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_register_pk_wrapper';

CREATE  FUNCTION supacache."rowcache_registration"(
	"tbl" TEXT /* &str */
) RETURNS TEXT[] /* alloc::vec::Vec<alloc::string::String> */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_registration_wrapper';

CREATE  FUNCTION supacache."rowcache_registration_status"() RETURNS TABLE (
	"registered" bigint,  /* i64 */
	"loaded" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_registration_status_wrapper';

CREATE  FUNCTION supacache."rowcache_reload_registrations"() RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_reload_registrations_wrapper';

CREATE  FUNCTION supacache."slot_ranges"() RETURNS TABLE (
	"worker" INT,  /* i32 */
	"port" INT,  /* i32 */
	"slot_lo" INT,  /* i32 */
	"slot_hi" INT  /* i32 */
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'slot_ranges_wrapper';

CREATE  FUNCTION supacache."tenant_stats"() RETURNS TABLE (
	"tenant" TEXT,  /* alloc::string::String */
	"arena_bytes" bigint,  /* i64 */
	"entries" bigint  /* i64 */
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'tenant_stats_wrapper';

CREATE  FUNCTION supacache."topology_change"() RETURNS TABLE (
	"recorded_workers" INT,  /* core::option::Option<i32> */
	"running_workers" INT,  /* i32 */
	"slots_moved" INT,  /* i32 */
	"pct_moved" double precision  /* f64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'topology_change_wrapper';

CREATE  FUNCTION supacache."worker_health"() RETURNS TABLE (
	"slot" INT,  /* i32 */
	"role" TEXT,  /* alloc::string::String */
	"worker" INT,  /* core::option::Option<i32> */
	"pid" INT,  /* core::option::Option<i32> */
	"beat_age_ms" bigint,  /* core::option::Option<i64> */
	"stale_after_ms" bigint,  /* i64 */
	"alive" bool  /* bool */
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'worker_health_wrapper';

CREATE  FUNCTION supacache."worker_stats"() RETURNS TABLE (
	"worker" INT,  /* i32 */
	"partition" INT,  /* i32 */
	"entries" bigint,  /* i64 */
	"hits" bigint,  /* i64 */
	"misses" bigint,  /* i64 */
	"evictions" bigint,  /* i64 */
	"sets" bigint,  /* i64 */
	"tombstones" bigint,  /* i64 */
	"rehashes" bigint,  /* i64 */
	"arena_used_bytes" bigint,  /* i64 */
	"arena_capacity_bytes" bigint  /* i64 */
)
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'worker_stats_wrapper';

/* ---------------------------------------------------------------------------
 * Operational metrics views (#111), with the grants and comments that go with
 * them -- verbatim from the extension's own schema, so the two cannot drift.
 * ------------------------------------------------------------------------ */

-- One row: the cluster-wide rollup, for a dashboard's top line.
CREATE VIEW supacache.pg_stat_keyspace AS
SELECT
    (SELECT count(DISTINCT worker)::int FROM supacache.worker_stats())      AS workers,
    (SELECT count(*)::int FROM supacache.worker_stats())                    AS partitions,
    COALESCE(sum(w.entries), 0)::bigint                                     AS entries,
    COALESCE(sum(w.hits), 0)::bigint                                        AS hits,
    COALESCE(sum(w.misses), 0)::bigint                                      AS misses,
    COALESCE(sum(w.evictions), 0)::bigint                                   AS evictions,
    COALESCE(sum(w.sets), 0)::bigint                                        AS sets,
    COALESCE(sum(w.tombstones), 0)::bigint                                  AS tombstones,
    COALESCE(sum(w.rehashes), 0)::bigint                                    AS rehashes,
    COALESCE(sum(w.arena_used_bytes), 0)::bigint                            AS arena_used_bytes,
    COALESCE(sum(w.arena_capacity_bytes), 0)::bigint                        AS arena_capacity_bytes,
    -- NULL rather than 0 before the first lookup: a cache nobody has read from
    -- has no hit ratio, and graphing it as 0% invents an outage.
    CASE WHEN COALESCE(sum(w.hits + w.misses), 0) > 0
         THEN round(sum(w.hits)::numeric * 100 / sum(w.hits + w.misses), 2)
    END                                                                     AS hit_pct
FROM supacache.worker_stats() w;

-- One row per (slot worker, partition).
CREATE VIEW supacache.pg_stat_keyspace_workers AS
SELECT w.worker, w.partition, w.entries, w.hits, w.misses, w.evictions,
       w.sets, w.tombstones, w.rehashes,
       w.arena_used_bytes,
       (w.arena_capacity_bytes - w.arena_used_bytes)                        AS arena_free_bytes,
       w.arena_capacity_bytes,
       CASE WHEN w.arena_capacity_bytes > 0
            THEN round(w.arena_used_bytes::numeric * 100 / w.arena_capacity_bytes, 2)
       END                                                                  AS arena_used_pct,
       r.slot_lo, r.slot_hi, r.port
FROM supacache.worker_stats() w
LEFT JOIN supacache.slot_ranges() r ON r.worker = w.worker;

-- One row per background worker the watchdog tracks.
CREATE VIEW supacache.pg_stat_keyspace_activity AS
SELECT slot, role, worker, pid, beat_age_ms, stale_after_ms, alive
FROM supacache.worker_health();

-- One row per persistence ring. Summed in pg_stat_keyspace_persist_total.
CREATE VIEW supacache.pg_stat_keyspace_persist AS
SELECT worker, shard, pushed, dropped, backlog_bytes, committed, lag,
       uncommitted_batches, unresolved
FROM supacache.persist_shard_stats();

CREATE VIEW supacache.pg_stat_keyspace_persist_total AS
SELECT COALESCE(sum(pushed), 0)::bigint          AS pushed,
       COALESCE(sum(dropped), 0)::bigint         AS dropped,
       COALESCE(sum(backlog_bytes), 0)::bigint   AS backlog_bytes,
       COALESCE(sum(committed), 0)::bigint       AS committed,
       COALESCE(sum(lag), 0)::bigint             AS lag,
       COALESCE(sum(uncommitted_batches), 0)::bigint AS uncommitted_batches,
       COALESCE(sum(unresolved), 0)::bigint      AS unresolved,
       -- The ring with the deepest backlog, which is the one that will start
       -- dropping. A healthy total hides it completely.
       (SELECT max(backlog_bytes) FROM supacache.persist_shard_stats()) AS worst_ring_backlog_bytes
FROM supacache.persist_shard_stats();

-- One row per tenant with resident entries.
CREATE VIEW supacache.pg_stat_keyspace_tenants AS
SELECT tenant, arena_bytes, entries
FROM supacache.tenant_stats();

-- Row cache: occupancy, coherence, and whether registrations are resident.
CREATE VIEW supacache.pg_stat_keyspace_rowcache AS
SELECT s.entries, s.hits, s.misses, s.data_used AS arena_used_bytes,
       s.data_cap AS arena_capacity_bytes,
       CASE WHEN s.hits + s.misses > 0
            THEN round(s.hits::numeric * 100 / (s.hits + s.misses), 2)
       END                                       AS hit_pct,
       c.coherent, c.decode_enabled, c.beat_age_ms, c.stale_after_ms,
       g.registered AS registrations, g.loaded AS registrations_loaded
FROM supacache.rowcache_stats() s
CROSS JOIN supacache.rowcache_coherence() c
CROSS JOIN supacache.rowcache_registration_status() g;

-- Empty when decoding is off or the slot has not been created yet.
CREATE VIEW supacache.pg_stat_keyspace_invalidation AS
SELECT slot_name, active, confirmed_flush_lsn, restart_lsn, current_lsn,
       decode_lag_bytes, retained_bytes
FROM supacache.invalidation_stats();

CREATE VIEW supacache.pg_stat_keyspace_pubsub AS
SELECT dropped, route_full, name_too_long FROM supacache.pubsub_stats();

CREATE VIEW supacache.pg_stat_keyspace_topology AS
SELECT recorded_workers, running_workers, slots_moved, pct_moved
FROM supacache.topology_change();

-- A monitoring role gets these and nothing else. `pg_monitor` is the role
-- postgres_exporter, pgwatch and Datadog are already told to use, so this is
-- the whole of the setup: install the extension, and an existing collector
-- picks the cache up. A team using a different role name grants it pg_monitor,
-- which is the one line every Postgres monitoring guide already tells them to
-- run.
--
-- EXECUTE is granted on the functions too, and that is not belt-and-braces. A
-- view's *table* references are checked against the view owner, but a SET
-- FUNCTION in its FROM clause is checked against the CALLER -- the executor
-- asks pg_proc_aclcheck(..., GetUserId(), ACL_EXECUTE), and GetUserId() is the
-- collector. Granting SELECT on the view alone gets "permission denied for
-- function worker_stats", which is what run_pg_stat_views.sh reported when
-- these grants were missing. Granting EXECUTE explicitly also means an operator
-- hardening the install with REVOKE ... FROM PUBLIC does not break monitoring.
GRANT USAGE ON SCHEMA supacache TO pg_monitor;
GRANT EXECUTE ON FUNCTION
    supacache.worker_stats(),
    supacache.persist_shard_stats(),
    supacache.tenant_stats(),
    supacache.worker_health(),
    supacache.invalidation_stats(),
    supacache.rowcache_registration_status(),
    supacache.rowcache_stats(),
    supacache.rowcache_coherence(),
    supacache.pubsub_stats(),
    supacache.topology_change(),
    supacache.slot_ranges()
TO pg_monitor;
GRANT SELECT ON
    supacache.pg_stat_keyspace,
    supacache.pg_stat_keyspace_workers,
    supacache.pg_stat_keyspace_activity,
    supacache.pg_stat_keyspace_persist,
    supacache.pg_stat_keyspace_persist_total,
    supacache.pg_stat_keyspace_tenants,
    supacache.pg_stat_keyspace_rowcache,
    supacache.pg_stat_keyspace_invalidation,
    supacache.pg_stat_keyspace_pubsub,
    supacache.pg_stat_keyspace_topology
TO pg_monitor;

COMMENT ON VIEW supacache.pg_stat_keyspace IS
  'pg_keyspace: cluster-wide keyspace rollup. Counters are cumulative since segment creation; entries and arena_*_bytes are gauges.';
COMMENT ON VIEW supacache.pg_stat_keyspace_workers IS
  'pg_keyspace: per (slot worker, partition) counters, arena occupancy and the slot range/port the worker serves.';
COMMENT ON VIEW supacache.pg_stat_keyspace_activity IS
  'pg_keyspace: heartbeat age per background worker. alive=false is a worker the watchdog is about to relaunch; a row that flips repeatedly is a crash loop.';
COMMENT ON VIEW supacache.pg_stat_keyspace_persist IS
  'pg_keyspace: persistence ring health, one row per (worker, shard). lag is acknowledged writes not yet committed. uncommitted_batches is a GAUGE of batches in flight, not a failure count: alert on a floor that never drains, not on it being nonzero.';
COMMENT ON VIEW supacache.pg_stat_keyspace_persist_total IS
  'pg_keyspace: persistence rings summed, plus the deepest single ring backlog, which a sum hides.';
COMMENT ON VIEW supacache.pg_stat_keyspace_tenants IS
  'pg_keyspace: measured arena occupancy per tenant (gauges). Scans live entries, so cost is O(entries) per call.';
COMMENT ON VIEW supacache.pg_stat_keyspace_rowcache IS
  'pg_keyspace: Mode B row cache occupancy and coherence. coherent=false means invalidation is configured but not beating; reads fail closed.';
COMMENT ON VIEW supacache.pg_stat_keyspace_invalidation IS
  'pg_keyspace: WAL decode lag for the row cache. Empty when decoding is off. retained_bytes is WAL the slot is pinning on disk.';
COMMENT ON VIEW supacache.pg_stat_keyspace_pubsub IS
  'pg_keyspace: pub/sub messages NOT delivered. Every column is a cumulative loss counter; PUBLISH cannot report these to the client.';
COMMENT ON VIEW supacache.pg_stat_keyspace_topology IS
  'pg_keyspace: worker layout the persisted keyspace was written under vs the one running now, and what a change between them costs.';
