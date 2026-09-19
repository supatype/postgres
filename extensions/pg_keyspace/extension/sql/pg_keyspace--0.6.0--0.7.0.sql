/* pg_keyspace 0.6.0 -> 0.7.0
 *
 * Owner-addressed recovery (#164): `pg_keyspace.recovery_addressing = 'owner'`
 * brings each persisted key back to the worker that wrote it, so a persisted
 * multi-worker cluster no longer has to redirect clients by slot.
 *
 * Two catalogue changes come with it:
 *
 *   supacache.rehome(workers)   rewrite every row's owner to the worker a
 *                               given worker count puts its key on, which is
 *                               what makes an otherwise-unsafe resize safe
 *   supacache.topology_change() gains `addressing` and `resize_safe`, and
 *                               slots_moved / pct_moved become NULLABLE
 *
 * slots_moved counts a reshard of the CRC16 slot map. Under owner addressing
 * nothing is keyed on that map, so a number there invites an operator to
 * reconcile against a scheme the cluster is not using -- it is NULL in that
 * mode, which says "not this question" where 0 would have said "no movement".
 * That is a change of column type, so the function is dropped and recreated
 * rather than added to, and the view over it is replaced: a catalogue left
 * promising the old row shape is worse than none, because it returns the old
 * columns without raising.
 *
 * The definitions must match what pgrx generates for a fresh install exactly.
 * `run_extension_autoupgrade.sh` dumps both catalogues and diffs them object
 * by object. These came from `cargo pgrx schema`, not from typing.
 */

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
