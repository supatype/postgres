/* pg_keyspace 0.2.0 -> 0.3.0
 *
 * The row cache serves every database now (#120), and that changes the shape of
 * the catalogue rather than only its contents:
 *
 *   * `invalidation_stats()` returned at most ONE row, because there was one
 *     decode slot. There is now one per participating database, so it gains
 *     `datid`/`datname` to key them, plus `wal_status` and
 *     `max_slot_wal_keep_size` -- a lost slot and the bound that causes it are
 *     operational state an operator has to be able to see.
 *   * `rowcache_coherence()` answers for the database you are connected to, and
 *     now says which that is, whether its slot was lost, and how many databases
 *     are participating.
 *   * `rowcache_databases()` and `pg_stat_keyspace_rowcache_databases` are new:
 *     the cluster-wide answer to WHICH database is incoherent and why. The cache
 *     fails closed per database, so that is load-bearing for correctness rather
 *     than a dashboard nicety.
 *
 * Both function return types change, so they are dropped and recreated; the
 * views that select from them have to go first and come back after. Generated
 * from the 0.2.0 and 0.3.0 schemas rather than transcribed, so the definitions
 * below are the same text `CREATE EXTENSION` would install --
 * bench/run_extension_upgrade.sh asserts that equivalence across every object.
 */

-- Views first: they depend on the functions being replaced below.
DROP VIEW IF EXISTS supacache.pg_stat_keyspace_rowcache;
DROP VIEW IF EXISTS supacache.pg_stat_keyspace_invalidation;

-- Return types change, so CREATE OR REPLACE is not enough.
DROP FUNCTION IF EXISTS supacache.invalidation_stats();
DROP FUNCTION IF EXISTS supacache.rowcache_coherence();

CREATE  FUNCTION supacache."rowcache_databases"() RETURNS TABLE (
	"datoid" bigint,  /* i64 */
	"datname" TEXT,  /* alloc::string::String */
	"state" TEXT,  /* alloc::string::String */
	"registrations" bigint,  /* i64 */
	"coherent" bool,  /* bool */
	"slot_lost" bool,  /* bool */
	"beat_age_ms" bigint,  /* core::option::Option<i64> */
	"stale_after_ms" bigint,  /* i64 */
	"slot_name" TEXT,  /* alloc::string::String */
	"worker_pid" INT  /* core::option::Option<i32> */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_databases_wrapper';

CREATE  FUNCTION supacache."rowcache_coherence"() RETURNS TABLE (
	"coherent" bool,  /* bool */
	"decode_enabled" bool,  /* bool */
	"beat_age_ms" bigint,  /* core::option::Option<i64> */
	"stale_after_ms" bigint,  /* i64 */
	"datname" TEXT,  /* alloc::string::String */
	"slot_lost" bool,  /* bool */
	"participating_databases" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rowcache_coherence_wrapper';

CREATE  FUNCTION supacache."invalidation_stats"() RETURNS TABLE (
	"datid" bigint,  /* i64 */
	"datname" TEXT,  /* core::option::Option<alloc::string::String> */
	"slot_name" TEXT,  /* alloc::string::String */
	"active" bool,  /* bool */
	"wal_status" TEXT,  /* core::option::Option<alloc::string::String> */
	"confirmed_flush_lsn" TEXT,  /* core::option::Option<alloc::string::String> */
	"restart_lsn" TEXT,  /* core::option::Option<alloc::string::String> */
	"current_lsn" TEXT,  /* core::option::Option<alloc::string::String> */
	"decode_lag_bytes" bigint,  /* core::option::Option<i64> */
	"retained_bytes" bigint,  /* core::option::Option<i64> */
	"max_slot_wal_keep_size" TEXT  /* core::option::Option<alloc::string::String> */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'invalidation_stats_wrapper';


CREATE VIEW supacache.pg_stat_keyspace_rowcache AS
SELECT s.entries, s.hits, s.misses, s.data_used AS arena_used_bytes,
       s.data_cap AS arena_capacity_bytes,
       CASE WHEN s.hits + s.misses > 0
            THEN round(s.hits::numeric * 100 / (s.hits + s.misses), 2)
       END                                       AS hit_pct,
       c.coherent, c.decode_enabled, c.beat_age_ms, c.stale_after_ms,
       g.registered AS registrations, g.loaded AS registrations_loaded,
       c.datname, c.slot_lost, c.participating_databases,
       (SELECT count(*) FROM supacache.rowcache_databases()
         WHERE state = 'participating' AND NOT coherent)::bigint
                                                 AS incoherent_databases
FROM supacache.rowcache_stats() s
CROSS JOIN supacache.rowcache_coherence() c
CROSS JOIN supacache.rowcache_registration_status() g;

CREATE VIEW supacache.pg_stat_keyspace_rowcache_databases AS
SELECT datoid, datname, state, registrations, coherent, slot_lost,
       beat_age_ms, stale_after_ms, slot_name, worker_pid
FROM supacache.rowcache_databases();

CREATE VIEW supacache.pg_stat_keyspace_invalidation AS
SELECT datid, datname, slot_name, active, wal_status,
       confirmed_flush_lsn, restart_lsn, current_lsn,
       decode_lag_bytes, retained_bytes, max_slot_wal_keep_size
FROM supacache.invalidation_stats();

GRANT EXECUTE ON FUNCTION
    supacache.worker_stats(),
    supacache.persist_shard_stats(),
    supacache.tenant_stats(),
    supacache.worker_health(),
    supacache.invalidation_stats(),
    supacache.rowcache_registration_status(),
    supacache.rowcache_stats(),
    supacache.rowcache_coherence(),
    supacache.rowcache_databases(),
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
    supacache.pg_stat_keyspace_rowcache_databases,
    supacache.pg_stat_keyspace_invalidation,
    supacache.pg_stat_keyspace_pubsub,
    supacache.pg_stat_keyspace_topology
TO pg_monitor;

COMMENT ON VIEW supacache.pg_stat_keyspace_rowcache IS
  'pg_keyspace: Mode B row cache occupancy (segment-wide) and coherence (for datname, the database you are connected to). coherent=false means invalidation is configured but not current for THIS database; reads fail closed. incoherent_databases counts them cluster-wide.';

COMMENT ON VIEW supacache.pg_stat_keyspace_rowcache_databases IS
  'pg_keyspace: one row per database the row cache knows about. state=participating has registrations and needs a slot; idle has none and deliberately gets neither. stale_after_ms grows with participating databases once they outnumber rowcache_invalidation_workers, because a database waiting its turn is behind by the cycle time by design.';

COMMENT ON VIEW supacache.pg_stat_keyspace_invalidation IS
  'pg_keyspace: WAL decode lag for the row cache, one row per database with a slot. Empty when decoding is off. retained_bytes is WAL that database''s slot is pinning on disk; wal_status=lost means the server cut it loose for exceeding max_slot_wal_keep_size, and that database is then marked incoherent and rebuilt.';

