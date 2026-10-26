-- migrate:up

-- Narrow, audited helpers for the Studio operations that need more than the SQL editor's
-- non-superuser role can do (SQ2): turning row level security on or off, and creating or
-- dropping a policy, on the auth and storage tables.
--
-- Since SEC-SQL the SQL editor connects as supatype_sql_editor, which is not a superuser and
-- is not a member of the roles that own the auth and storage tables, so it cannot alter them.
-- Rather than hand that back to a superuser, each schema gets a SECURITY DEFINER helper owned
-- by that schema's own admin role (supatype_auth_admin / supatype_storage_admin, both
-- non-superuser). The helper runs as its owner, so the DDL happens as the table's owning role
-- and never as a superuser, and the helper can only ever reach its own schema's tables. The
-- gateway and the control plane call these instead of sending the DDL through the editor.
--
-- Every function fixes its search_path, validates its target is a base table in its own
-- schema, and whitelists the policy command, so the only free text is the policy's own USING
-- and WITH CHECK expressions, which run as the non-superuser owner.

-- A boolean-returning guard: the table must be an ordinary table in `want_schema`.
CREATE OR REPLACE FUNCTION public.supatype_studio_assert_table(want_schema text, p_table text)
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog, pg_temp
AS $fn$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_class c
      JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = want_schema AND c.relname = p_table
       AND c.relkind IN ('r', 'p')
  ) THEN
    RAISE EXCEPTION 'supatype: % is not a table in the % schema', p_table, want_schema
      USING ERRCODE = 'undefined_table';
  END IF;
END;
$fn$;

-- The policy command whitelist, shared by both schemas' helpers.
CREATE OR REPLACE FUNCTION public.supatype_studio_assert_command(p_command text)
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog, pg_temp
AS $fn$
BEGIN
  IF upper(p_command) NOT IN ('ALL', 'SELECT', 'INSERT', 'UPDATE', 'DELETE') THEN
    RAISE EXCEPTION 'supatype: % is not a policy command', p_command
      USING ERRCODE = 'invalid_parameter_value';
  END IF;
END;
$fn$;

-- Build the body shared by a schema's two helpers. `want_schema` fixes which schema the
-- helper may touch; the caller supplies it as a literal, never from a request.
CREATE OR REPLACE FUNCTION public.supatype_studio_set_rls_body(want_schema text, p_table text, p_enable boolean)
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog, pg_temp
AS $fn$
BEGIN
  PERFORM public.supatype_studio_assert_table(want_schema, p_table);
  EXECUTE pg_catalog.format('ALTER TABLE %I.%I %s ROW LEVEL SECURITY',
    want_schema, p_table, CASE WHEN p_enable THEN 'ENABLE' ELSE 'DISABLE' END);
END;
$fn$;

CREATE OR REPLACE FUNCTION public.supatype_studio_write_policy_body(
  want_schema text, p_op text, p_table text, p_name text,
  p_command text, p_roles text[], p_using text, p_check text)
  RETURNS void
  LANGUAGE plpgsql
  SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
  stmt text;
BEGIN
  PERFORM public.supatype_studio_assert_table(want_schema, p_table);
  IF lower(p_op) = 'drop' THEN
    EXECUTE pg_catalog.format('DROP POLICY IF EXISTS %I ON %I.%I', p_name, want_schema, p_table);
    RETURN;
  ELSIF lower(p_op) <> 'create' THEN
    RAISE EXCEPTION 'supatype: % is not create or drop', p_op USING ERRCODE = 'invalid_parameter_value';
  END IF;

  PERFORM public.supatype_studio_assert_command(p_command);
  stmt := pg_catalog.format('CREATE POLICY %I ON %I.%I FOR %s', p_name, want_schema, p_table, upper(p_command));
  IF p_roles IS NOT NULL AND pg_catalog.array_length(p_roles, 1) > 0 THEN
    stmt := stmt || ' TO ' || (SELECT pg_catalog.string_agg(pg_catalog.quote_ident(r), ', ') FROM unnest(p_roles) AS r);
  END IF;
  IF p_using IS NOT NULL AND pg_catalog.btrim(p_using) <> '' THEN
    stmt := stmt || pg_catalog.format(' USING (%s)', p_using);
  END IF;
  IF p_check IS NOT NULL AND pg_catalog.btrim(p_check) <> '' THEN
    stmt := stmt || pg_catalog.format(' WITH CHECK (%s)', p_check);
  END IF;
  EXECUTE stmt;
END;
$fn$;

-- The auth schema's helpers, owned by supatype_auth_admin.
CREATE OR REPLACE FUNCTION auth.supatype_studio_set_rls(p_table text, p_enable boolean)
  RETURNS void
  LANGUAGE sql
  SECURITY DEFINER
  SET search_path = pg_catalog, pg_temp
AS $fn$ SELECT public.supatype_studio_set_rls_body('auth', p_table, p_enable) $fn$;

CREATE OR REPLACE FUNCTION auth.supatype_studio_write_policy(
  p_op text, p_table text, p_name text, p_command text, p_roles text[], p_using text, p_check text)
  RETURNS void
  LANGUAGE sql
  SECURITY DEFINER
  SET search_path = pg_catalog, pg_temp
AS $fn$ SELECT public.supatype_studio_write_policy_body('auth', p_op, p_table, p_name, p_command, p_roles, p_using, p_check) $fn$;

ALTER FUNCTION auth.supatype_studio_set_rls(text, boolean) OWNER TO supatype_auth_admin;
ALTER FUNCTION auth.supatype_studio_write_policy(text, text, text, text, text[], text, text) OWNER TO supatype_auth_admin;

-- A SECURITY DEFINER function is executable by PUBLIC unless that is revoked, which here would
-- let any role -- the editor, anon, anyone -- run DDL as the schema admin. Revoke it. The
-- gateway and the control plane reach these through their superuser admin connection, which
-- executes regardless; nobody else may. The building-block functions in public are not
-- SECURITY DEFINER, so they run as whoever calls them: the admin roles need EXECUTE to reach
-- them from inside the definer functions, and PUBLIC must not.
REVOKE EXECUTE ON FUNCTION public.supatype_studio_assert_table(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.supatype_studio_assert_command(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.supatype_studio_set_rls_body(text, text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION public.supatype_studio_write_policy_body(text, text, text, text, text, text[], text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION auth.supatype_studio_set_rls(text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION auth.supatype_studio_write_policy(text, text, text, text, text[], text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.supatype_studio_assert_table(text, text) TO supatype_auth_admin;
GRANT EXECUTE ON FUNCTION public.supatype_studio_assert_command(text) TO supatype_auth_admin;
GRANT EXECUTE ON FUNCTION public.supatype_studio_set_rls_body(text, text, boolean) TO supatype_auth_admin;
GRANT EXECUTE ON FUNCTION public.supatype_studio_write_policy_body(text, text, text, text, text, text[], text, text) TO supatype_auth_admin;

-- The storage schema's helpers, owned by supatype_storage_admin (only when storage exists).
DO $$
BEGIN
  IF EXISTS (SELECT FROM pg_namespace WHERE nspname = 'storage')
     AND EXISTS (SELECT FROM pg_roles WHERE rolname = 'supatype_storage_admin') THEN
    EXECUTE $def$
      CREATE OR REPLACE FUNCTION storage.supatype_studio_set_rls(p_table text, p_enable boolean)
        RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
      AS 'SELECT public.supatype_studio_set_rls_body(''storage'', p_table, p_enable)';
    $def$;
    EXECUTE $def$
      CREATE OR REPLACE FUNCTION storage.supatype_studio_write_policy(
        p_op text, p_table text, p_name text, p_command text, p_roles text[], p_using text, p_check text)
        RETURNS void LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
      AS 'SELECT public.supatype_studio_write_policy_body(''storage'', p_op, p_table, p_name, p_command, p_roles, p_using, p_check)';
    $def$;
    EXECUTE 'ALTER FUNCTION storage.supatype_studio_set_rls(text, boolean) OWNER TO supatype_storage_admin';
    EXECUTE 'ALTER FUNCTION storage.supatype_studio_write_policy(text, text, text, text, text[], text, text) OWNER TO supatype_storage_admin';
    EXECUTE 'REVOKE EXECUTE ON FUNCTION storage.supatype_studio_set_rls(text, boolean) FROM PUBLIC';
    EXECUTE 'REVOKE EXECUTE ON FUNCTION storage.supatype_studio_write_policy(text, text, text, text, text[], text, text) FROM PUBLIC';
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.supatype_studio_assert_table(text, text) TO supatype_storage_admin';
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.supatype_studio_assert_command(text) TO supatype_storage_admin';
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.supatype_studio_set_rls_body(text, text, boolean) TO supatype_storage_admin';
    EXECUTE 'GRANT EXECUTE ON FUNCTION public.supatype_studio_write_policy_body(text, text, text, text, text, text[], text, text) TO supatype_storage_admin';
  END IF;
END $$;

-- migrate:down
-- Deliberately empty: migrate.sh applies this file with plain psql -f, so a DROP here would
-- undo the functions above. See 20260809214500_supatype_mask.sql.
