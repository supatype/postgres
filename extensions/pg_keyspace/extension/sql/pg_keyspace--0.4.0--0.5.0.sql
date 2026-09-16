/* pg_keyspace 0.4.0 -> 0.5.0
 *
 * Adds `supacache.publish_relayed(channel text, message bytea)`, the endpoint a
 * peer instance's relay worker calls to deliver a message that was published
 * somewhere else.
 *
 * It is a separate function from `supacache.publish()` rather than a flag on it
 * for two reasons that are both about what the caller is allowed to assume: the
 * channel arrives ALREADY tenant-scoped by the publishing instance, and it is
 * never relayed onward, which is what structurally prevents A -> B -> A.
 *
 * A new version rather than an edit to an earlier script: 0.4.0 already ships,
 * and the catalogue self-upgrade compares the installed version against the
 * library's `default_version`, so an addition made inside a shipped version
 * never reaches a cluster already running it.
 *
 * The definition must match what pgrx generates for a fresh install exactly --
 * `run_extension_autoupgrade.sh` dumps both catalogues and diffs them object by
 * object, so a different declaration fails as loudly as a missing one.
 *
 * `supacache.peer` is deliberately NOT here. The worker creates it at startup
 * with the other operational tables (resp_credential, acl, topology), so it is
 * not an extension-owned object and must not appear in either catalogue.
 */

CREATE FUNCTION supacache."publish_relayed"(
	"channel" TEXT, /* &str */
	"message" bytea /* &[u8] */
) RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'publish_relayed_wrapper';
