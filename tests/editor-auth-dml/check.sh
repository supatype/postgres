#!/bin/sh
# Asserts that Studio's SQL editor role can read and change rows in `auth` and `storage`, and can
# change nothing else about those schemas (SQ5).
#
# Since SEC-SQL the editor logs in as supatype_sql_editor, which is not a superuser and is a member
# of the project owner only. The auth and storage tables belong to their services' roles, so the
# editor had no USAGE on either schema and `update auth.users ...` failed with "permission denied
# for schema auth". The editor gets row access (SELECT, INSERT, UPDATE, DELETE, and the sequences
# an insert needs) on every table there, including tables the services create later, and nothing
# that changes a table's shape, ownership or policies.
#
# The fixture stands in for the services: email_confirmed_at and RLS on auth.users as the auth
# server's migrations add them, storage tables as the storage server creates them (as supatype_admin) with RLS on
# storage.objects as the engine turns it on, and one table made later in auth by each role that
# creates tables there. Idempotent, so it can run before and after a restart.
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in the environment
#   when the container's pg_hba asks for one over the socket. The editor logs in over 127.0.0.1,
#   which the image's pg_hba trusts inside the container.
set -eu

container=$1
db=${2:-supatype_admin}
failed=0

as_admin() {
	docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" -e PGOPTIONS="-c client_min_messages=warning" \
		-i "$container" psql -v ON_ERROR_STOP=1 -U supatype_admin -d "$db" -tAq -c "$1"
}

as_editor() {
	docker exec -i "$container" \
		psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U supatype_sql_editor -d "$db" -tAq -c "$1"
}

# allowed <label> <sql> <expected output>
allowed() {
	if got=$(as_editor "$2" 2>&1) && [ "$got" = "$3" ]; then
		echo "ok      $1"
	else
		echo "FAIL    $1 (expected [$3], got [$got])"
		failed=$((failed + 1))
	fi
}

# refused <label> <sql>: must fail on a privilege error, not on anything else (a typo would
# otherwise pass as a refusal).
refused() {
	if got=$(as_editor "$2" 2>&1); then
		echo "FAIL    $1 (was allowed: [$got])"
		failed=$((failed + 1))
		return
	fi
	case "$got" in
		*"permission denied"* | *"must be owner"* | *"must be able to SET ROLE"*)
			echo "ok      $1 (refused)" ;;
		*)
			echo "FAIL    $1 (failed for another reason: [$got])"
			failed=$((failed + 1)) ;;
	esac
}

as_admin "
ALTER TABLE auth.users ADD COLUMN IF NOT EXISTS email_confirmed_at timestamptz;
ALTER TABLE auth.users ENABLE ROW LEVEL SECURITY;
INSERT INTO auth.users (instance_id, id, aud, role, email)
  VALUES ('00000000-0000-0000-0000-000000000000', '00000000-0000-0000-0000-0000000005c1',
          'authenticated', 'authenticated', 'sq5@example.com')
  ON CONFLICT (id) DO NOTHING;
CREATE TABLE IF NOT EXISTS auth.sq5_later_admin (id int PRIMARY KEY);
SET ROLE supatype_auth_admin;
CREATE TABLE IF NOT EXISTS auth.sq5_later_auth_admin (id serial PRIMARY KEY, note text);
RESET ROLE;
CREATE TABLE IF NOT EXISTS storage.buckets (
  id text PRIMARY KEY, name text NOT NULL UNIQUE, public boolean NOT NULL DEFAULT false);
CREATE TABLE IF NOT EXISTS storage.objects (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), bucket_id text REFERENCES storage.buckets (id),
  name text NOT NULL, owner uuid);
ALTER TABLE storage.objects ENABLE ROW LEVEL SECURITY;
INSERT INTO storage.buckets (id, name) VALUES ('sq5', 'sq5') ON CONFLICT (id) DO NOTHING;
INSERT INTO storage.objects (id, bucket_id, name)
  VALUES ('00000000-0000-0000-0000-0000000005c2', 'sq5', 'a.txt') ON CONFLICT (id) DO NOTHING;
" > /dev/null

user_id="'00000000-0000-0000-0000-0000000005c1'"

# Rows: every row is visible past RLS, and can be changed.
allowed "editor reads auth.users past RLS" \
	"SELECT email FROM auth.users WHERE id = $user_id" "sq5@example.com"
allowed "editor confirms a user's email in auth.users" \
	"WITH u AS (UPDATE auth.users SET email_confirmed_at = now() WHERE id = $user_id RETURNING 1) SELECT count(*) FROM u" "1"
allowed "editor inserts into and deletes from auth.users" \
	"WITH i AS (INSERT INTO auth.users (instance_id, id, aud, role, email) VALUES ('00000000-0000-0000-0000-000000000000', gen_random_uuid(), 'authenticated', 'authenticated', 'sq5-tmp@example.com') RETURNING id) SELECT count(*) FROM i; DELETE FROM auth.users WHERE email = 'sq5-tmp@example.com'" "1"
allowed "editor reads a table supatype_admin made in auth after the grant" \
	"SELECT count(*) FROM auth.sq5_later_admin" "0"
allowed "editor inserts through a sequence into a table supatype_auth_admin made later" \
	"WITH i AS (INSERT INTO auth.sq5_later_auth_admin (note) VALUES ('x') RETURNING id) SELECT count(*) FROM i" "1"
allowed "editor reads storage.buckets" \
	"SELECT name FROM storage.buckets WHERE id = 'sq5'" "sq5"
allowed "editor updates storage.buckets" \
	"WITH u AS (UPDATE storage.buckets SET public = true WHERE id = 'sq5' RETURNING 1) SELECT count(*) FROM u" "1"
allowed "editor reads storage.objects past RLS" \
	"SELECT name FROM storage.objects WHERE bucket_id = 'sq5'" "a.txt"
allowed "editor updates storage.objects past RLS" \
	"WITH u AS (UPDATE storage.objects SET name = 'a.txt' WHERE bucket_id = 'sq5' RETURNING 1) SELECT count(*) FROM u" "1"

# Shape, ownership and policies stay out of reach.
refused "ALTER TABLE auth.users" "ALTER TABLE auth.users ADD COLUMN sq5 int"
refused "DROP TABLE auth.users" "DROP TABLE auth.users"
refused "CREATE TABLE in auth" "CREATE TABLE auth.sq5_x (id int)"
refused "TRUNCATE auth.users" "TRUNCATE auth.users"
refused "ALTER TABLE auth.users OWNER" "ALTER TABLE auth.users OWNER TO supatype_sql_editor"
refused "CREATE POLICY on auth.users" "CREATE POLICY sq5 ON auth.users FOR SELECT USING (true)"
refused "DISABLE RLS on auth.users" "ALTER TABLE auth.users DISABLE ROW LEVEL SECURITY"
refused "CREATE TRIGGER on auth.users" "CREATE TRIGGER sq5 BEFORE UPDATE ON auth.users FOR EACH ROW EXECUTE FUNCTION suppress_redundant_updates_trigger()"

# Without a grant option Postgres answers GRANT with a warning rather than an error, so what
# matters is that nothing was granted.
as_editor "GRANT SELECT ON auth.sq5_later_admin TO anon" > /dev/null 2>&1 || true
if [ "$(as_admin "SELECT has_table_privilege('anon', 'auth.sq5_later_admin', 'SELECT')")" = "f" ]; then
	echo "ok      GRANT on an auth table passes nothing on"
else
	echo "FAIL    GRANT on an auth table passes nothing on (anon can now read it)"
	failed=$((failed + 1))
fi
refused "ALTER TABLE storage.objects" "ALTER TABLE storage.objects ADD COLUMN sq5 int"
refused "DROP TABLE storage.buckets" "DROP TABLE storage.buckets"
refused "CREATE TABLE in storage" "CREATE TABLE storage.sq5_x (id int)"
refused "TRUNCATE storage.objects" "TRUNCATE storage.objects"
refused "DISABLE RLS on storage.objects" "ALTER TABLE storage.objects DISABLE ROW LEVEL SECURITY"
refused "setval on an auth sequence" "SELECT setval('auth.sq5_later_auth_admin_id_seq', 1000)"
refused "read pg_authid" "SELECT count(*) FROM pg_authid"

# Masking still exempts the editor, and nothing above made it a superuser.
allowed "editor is in supatype_mask.exempt_roles" \
	"SELECT 'supatype_sql_editor' = ANY (string_to_array(replace(current_setting('supatype_mask.exempt_roles'), ' ', ''), ','))" "t"
allowed "editor is still not a superuser" \
	"SELECT rolsuper FROM pg_roles WHERE rolname = current_user" "f"

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all editor auth and storage checks passed"
