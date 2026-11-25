#!/bin/sh
# Asserts that Studio's SQL editor can hand what it creates to the database owner, and still
# cannot SET ROLE into the owner.
#
# The editor (supatype_sql_editor) is a member of the owner WITH INHERIT TRUE, SET FALSE. The
# server's SQL runner hands the editor's new objects to the owner before commit, and on Postgres
# 16 and later a plain REASSIGN OWNED BY supatype_sql_editor run as the editor fails with
# `must be able to SET ROLE "<owner>"`. The handover goes through
# public.supatype_sql_editor_reassign_owned() instead: SECURITY DEFINER, owned by the NOLOGIN
# supatype_sql_editor_reassigner, executable by the editor alone, no arguments.
#
# Usage: check.sh <container> [database]
#   Runs against a running container of this image. Needs POSTGRES_PASSWORD in the environment
#   when the container's pg_hba asks for one over the socket. The editor logs in over 127.0.0.1,
#   which the image's pg_hba trusts inside the container.
set -eu

container=$1
db=${2:-supatype_admin}
failed=0
fn='public.supatype_sql_editor_reassign_owned()'

as_admin() {
	docker exec -e PGPASSWORD="${POSTGRES_PASSWORD:-}" -e PGOPTIONS="-c client_min_messages=warning" \
		-i "$container" psql -v ON_ERROR_STOP=1 -U supatype_admin -d "$db" -tAq -c "$1"
}

as_editor() {
	docker exec -e PGOPTIONS="-c client_min_messages=warning" -i "$container" \
		psql -v ON_ERROR_STOP=1 -h 127.0.0.1 -U supatype_sql_editor -d "$db" -tAq -c "$1"
}

pass() { echo "ok      $1"; }
fail() { echo "FAIL    $1"; failed=$((failed + 1)); }

# expect <label> <sql run as admin> <wanted>
expect() {
	got=$(as_admin "$2" 2>&1) || true
	if [ "$got" = "$3" ]; then pass "$1 ($got)"; else fail "$1 (expected [$3], got [$got])"; fi
}

# refused <label> <sql run as the editor> <error text it must fail with>
refused() {
	if got=$(as_editor "$2" 2>&1); then
		fail "$1 (was allowed: [$got])"
		return
	fi
	case "$got" in
		*"$3"*) pass "$1 (refused)" ;;
		*) fail "$1 (failed for another reason: [$got])" ;;
	esac
}

owner=$(as_admin "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()")
echo "database owner: $owner"

# Leftovers from an earlier run would make the ownership checks below read stale objects.
as_admin "DROP VIEW IF EXISTS public.editor_reassign_v; DROP TABLE IF EXISTS public.editor_reassign_t;
          DROP FUNCTION IF EXISTS public.editor_reassign_f()" > /dev/null

# The handover: what the editor creates belongs to the owner once the transaction commits.
if got=$(as_editor "BEGIN;
	CREATE TABLE public.editor_reassign_t (id serial PRIMARY KEY);
	CREATE VIEW public.editor_reassign_v AS SELECT id FROM public.editor_reassign_t;
	CREATE FUNCTION public.editor_reassign_f() RETURNS int LANGUAGE sql AS 'SELECT 1';
	SELECT $fn;
	COMMIT;" 2>&1); then
	pass "editor creates objects and hands them over in one transaction"
else
	fail "editor creates objects and hands them over in one transaction ([$got])"
fi
expect "the table, its sequence and index, and the view belong to the owner" \
	"SELECT count(*) FROM pg_class WHERE relname IN ('editor_reassign_t', 'editor_reassign_t_id_seq', 'editor_reassign_t_pkey', 'editor_reassign_v') AND relowner = '$owner'::regrole" "4"
expect "the function belongs to the owner" \
	"SELECT count(*) FROM pg_proc WHERE proname = 'editor_reassign_f' AND proowner = '$owner'::regrole" "1"
expect "the editor owns nothing in this database" \
	"SELECT count(*) FROM pg_shdepend WHERE refobjid = 'supatype_sql_editor'::regrole AND deptype = 'o' AND dbid = (SELECT oid FROM pg_database WHERE datname = current_database())" "0"
as_admin "DROP VIEW IF EXISTS public.editor_reassign_v; DROP TABLE IF EXISTS public.editor_reassign_t;
          DROP FUNCTION IF EXISTS public.editor_reassign_f()" > /dev/null

# The containment the function exists to keep.
refused "editor cannot SET ROLE to the owner" "SET ROLE $owner" "permission denied to set role"
refused "editor cannot SET ROLE to the reassigner" \
	"SET ROLE supatype_sql_editor_reassigner" "permission denied to set role"
refused "a raw REASSIGN by the editor is still refused" \
	"BEGIN; CREATE TABLE public.editor_reassign_raw (id int); REASSIGN OWNED BY supatype_sql_editor TO $owner; COMMIT;" \
	"must be able to SET ROLE"
expect "the editor's membership in the owner is INHERIT TRUE, SET FALSE" \
	"SELECT inherit_option || ',' || set_option FROM pg_auth_members WHERE member = 'supatype_sql_editor'::regrole AND roleid = '$owner'::regrole" "true,false"

# The function: SECURITY DEFINER, fixed search_path, no arguments, owned by the reassigner.
expect "function is SECURITY DEFINER with a fixed search_path and no arguments" \
	"SELECT prosecdef || ',' || array_to_string(proconfig, ';') || ',' || pronargs FROM pg_proc WHERE oid = '$fn'::regprocedure" \
	"true,search_path=pg_catalog, pg_temp,0"
expect "function is owned by supatype_sql_editor_reassigner" \
	"SELECT pg_get_userbyid(proowner) FROM pg_proc WHERE oid = '$fn'::regprocedure" "supatype_sql_editor_reassigner"
expect "PUBLIC cannot execute it" \
	"SELECT count(*) FROM pg_proc p, aclexplode(p.proacl) a WHERE p.oid = '$fn'::regprocedure AND a.grantee = 0" "0"
expect "no other non-superuser role can execute it (anon, authenticated, service_role, the owner...)" \
	"SELECT coalesce(string_agg(rolname, ','), '') FROM pg_roles WHERE NOT rolsuper AND rolname NOT IN ('supatype_sql_editor', 'supatype_sql_editor_reassigner') AND has_function_privilege(oid, '$fn'::regprocedure, 'EXECUTE')" ""

# The reassigner: no login, no superuser, granted to nobody, and it only SETs the owner.
expect "reassigner cannot log in and is not a superuser" \
	"SELECT rolcanlogin || ',' || rolsuper || ',' || rolcreaterole || ',' || rolcreatedb || ',' || rolbypassrls FROM pg_roles WHERE rolname = 'supatype_sql_editor_reassigner'" \
	"false,false,false,false,false"
expect "no role is a member of the reassigner" \
	"SELECT count(*) FROM pg_auth_members WHERE roleid = 'supatype_sql_editor_reassigner'::regrole" "0"
expect "reassigner inherits the editor" \
	"SELECT inherit_option || ',' || set_option FROM pg_auth_members WHERE roleid = 'supatype_sql_editor'::regrole AND member = 'supatype_sql_editor_reassigner'::regrole" "true,false"
expect "reassigner may SET ROLE to the owner without inheriting it" \
	"SELECT inherit_option || ',' || set_option FROM pg_auth_members WHERE roleid = '$owner'::regrole AND member = 'supatype_sql_editor_reassigner'::regrole" "false,true"
expect "reassigner is a member of nothing else" \
	"SELECT count(*) FROM pg_auth_members WHERE member = 'supatype_sql_editor_reassigner'::regrole AND roleid NOT IN ('supatype_sql_editor'::regrole, '$owner'::regrole)" "0"

# The target is the database owner and never a superuser: with the database handed to a
# superuser for the length of a rolled-back transaction, the function refuses on its own guard
# (not merely because the reassigner cannot SET ROLE to a superuser).
expect "function refuses a superuser database owner" \
	"BEGIN; ALTER DATABASE $db OWNER TO supatype_admin; SET LOCAL ROLE supatype_sql_editor;
	 DO \$\$ BEGIN PERFORM $fn; RAISE EXCEPTION 'handed over';
	 EXCEPTION WHEN insufficient_privilege THEN
	   IF SQLERRM NOT LIKE 'supatype: the SQL editor cannot hand%' THEN RAISE; END IF;
	 END \$\$;
	 RESET ROLE; SELECT 'refused'; ROLLBACK;" "refused"
expect "the database owner is back after the rollback" \
	"SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()" "$owner"

if [ "$failed" -ne 0 ]; then
	echo "$failed check(s) failed"
	exit 1
fi
echo "all editor reassign checks passed"
