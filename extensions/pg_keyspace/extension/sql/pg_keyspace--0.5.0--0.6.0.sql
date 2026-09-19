/* pg_keyspace 0.5.0 -> 0.6.0
 *
 * Everything #164 adds to the catalogue, in one version because it lands as
 * one feature: per-key durability and the owner-addressed recovery that lets
 * it run on more than one worker. Two bumps for one stack would make an
 * operator walk a chain step that never existed on its own.
 *
 * Editing this rather than adding 0.6.0--0.7.0 is the rule the earlier
 * scripts state: a new version is needed because the old one ALREADY SHIPS
 * and an addition inside it would never reach a cluster running it. 0.6.0
 * does not ship -- it is unreleased, in this same stack -- so there is no
 * such cluster and nothing to miss the addition.
 *
 * Per-key durability. `pg_keyspace.durability_overrides` lets one instance
 * write a prefix durably while the keys beside it stay ephemeral, and these
 * three functions are its SQL surface:
 *
 *   supacache.key_durability(key)   which tier a key resolves to, by the same
 *                                   policy the RESP write path and recovery use
 *   supacache.undurable_rows()      rows in supacache.kv the current policy no
 *                                   longer persists, grouped by key prefix --
 *                                   what narrowing a prefix leaves behind
 *   supacache.prune_undurable()     delete those rows
 *
 * Owner-addressed recovery. supacache.rehome(workers) rewrites every row's
 * owner to the worker a given worker count puts its key on, which is what
 * makes an otherwise-unsafe resize safe; and supacache.topology_change()
 * gains `addressing` and `resize_safe`, with slots_moved / pct_moved becoming
 * NULLABLE -- they count a reshard of the CRC16 slot map, and under owner
 * addressing nothing is keyed on that map, so a number there invites
 * reconciling against a scheme the cluster is not using.
 *
 * A new version rather than an edit to the 0.4.0--0.5.0 script, because 0.5.0
 * already ships: the catalogue self-upgrade compares the installed version
 * against the library's `default_version`, so an addition made inside a
 * shipped version never reaches a cluster already running it.
 *
 * The definitions must match what pgrx generates for a fresh install exactly.
 * `run_extension_autoupgrade.sh` dumps both catalogues and diffs them object by
 * object, so a different declaration fails as loudly as a missing one. These
 * were taken from `cargo pgrx schema` rather than written by hand.
 */

CREATE  FUNCTION supacache."key_durability"(
	"key" TEXT /* &str */
) RETURNS TEXT /* alloc::string::String */
STRICT STABLE PARALLEL SAFE
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'key_durability_wrapper';

CREATE  FUNCTION supacache."undurable_rows"() RETURNS TABLE (
	"key_prefix" TEXT,  /* alloc::string::String */
	"rows" bigint,  /* i64 */
	"bytes" bigint  /* i64 */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'undurable_rows_wrapper';

CREATE  FUNCTION supacache."prune_undurable"(
	"force" bool DEFAULT false /* bool */
) RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'prune_undurable_wrapper';

/* ---------------------------------------------------------------------------
 * Grants, matching the ones the fresh-install schema issues.
 *
 * key_durability() and undurable_rows() are read-only and go to pg_monitor
 * with the rest of the observability surface. prune_undurable() deliberately
 * does not: a monitoring role should be able to see the strandage without
 * being able to delete it.
 * ------------------------------------------------------------------------ */

GRANT EXECUTE ON FUNCTION
    supacache.key_durability(text),
    supacache.undurable_rows()
TO pg_monitor;

/* ---------------------------------------------------------------------------
 * Changed shape. The view has to go first: it depends on the function.
 * ------------------------------------------------------------------------ */

DROP VIEW IF EXISTS supacache.pg_stat_keyspace_topology;
DROP FUNCTION IF EXISTS supacache.topology_change();

CREATE  FUNCTION supacache."topology_change"() RETURNS TABLE (
	"recorded_workers" INT,  /* core::option::Option<i32> */
	"running_workers" INT,  /* i32 */
	"slots_moved" INT,  /* core::option::Option<i32> */
	"pct_moved" double precision,  /* core::option::Option<f64> */
	"addressing" TEXT,  /* alloc::string::String */
	"resize_safe" bool  /* bool */
)
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'topology_change_wrapper';

CREATE VIEW supacache.pg_stat_keyspace_topology AS
SELECT recorded_workers, running_workers, slots_moved, pct_moved, addressing, resize_safe
FROM supacache.topology_change();

-- DROP VIEW took the comment with it, and a fresh install has one. The
-- upgrade-vs-fresh catalogue diff compares comments as well as definitions and
-- ACLs, so restoring it is not cosmetic: without this the two catalogues
-- differ on a view that is otherwise identical. Must match the text in the
-- pg_stat_keyspace_views block exactly.
COMMENT ON VIEW supacache.pg_stat_keyspace_topology IS
  'pg_keyspace: worker layout the persisted keyspace was written under vs the one running now, and what a change between them costs.';

/* ---------------------------------------------------------------------------
 * New function.
 * ------------------------------------------------------------------------ */

CREATE  FUNCTION supacache."rehome"(
	"workers" INT /* i32 */
) RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'rehome_wrapper';

/* ---------------------------------------------------------------------------
 * Grants, matching the fresh-install schema. topology_change() is re-granted
 * because the DROP above took its ACL with it. rehome() is deliberately not
 * granted: it rewrites rows, and a monitoring role should see a resize coming
 * without being able to perform one.
 * ------------------------------------------------------------------------ */

GRANT EXECUTE ON FUNCTION supacache.topology_change() TO pg_monitor;
GRANT SELECT ON supacache.pg_stat_keyspace_topology TO pg_monitor;
