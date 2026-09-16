/* pg_keyspace 0.3.0 -> 0.4.0
 *
 * Adds `supacache.publish(channel text, message bytea)`, so a backend can reach
 * RESP subscribers from SQL -- a trigger or a job telling a subscribed client
 * something, rather than going out through the application and back in over the
 * wire.
 *
 * A new version rather than an edit to the 0.2.0--0.3.0 script, because 0.3.0
 * already ships: a cluster running it would never see an addition made there.
 * The catalogue self-upgrade compares the installed version against the
 * library's `default_version`, so only a bump reaches an install that is
 * already current.
 *
 * The definition must match what pgrx generates for a fresh install exactly.
 * `run_extension_autoupgrade.sh` dumps both catalogues and diffs them, so an
 * upgraded cluster that ends up one function short -- or with the same function
 * declared differently -- fails there rather than in someone's deployment.
 */

CREATE FUNCTION supacache."publish"(
	"channel" TEXT, /* &str */
	"message" bytea /* &[u8] */
) RETURNS bigint /* i64 */
STRICT
LANGUAGE c /* Rust */
AS 'MODULE_PATHNAME', 'publish_wrapper';
