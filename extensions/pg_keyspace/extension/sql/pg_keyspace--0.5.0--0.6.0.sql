/* pg_keyspace 0.5.0 -> 0.6.0
 *
 * Per-key durability (#164). `pg_keyspace.durability_overrides` lets one
 * instance write a prefix durably while the keys beside it stay ephemeral, and
 * these three functions are its SQL surface:
 *
 *   supacache.key_durability(key)   which tier a key resolves to, by the same
 *                                   policy the RESP write path and recovery use
 *   supacache.undurable_rows()      rows in supacache.kv the current policy no
 *                                   longer persists, grouped by key prefix --
 *                                   what narrowing a prefix leaves behind
 *   supacache.prune_undurable()     delete those rows
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
