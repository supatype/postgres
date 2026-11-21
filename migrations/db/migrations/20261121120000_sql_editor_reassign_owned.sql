-- migrate:up

-- How the Studio SQL editor hands what it creates to the database owner, without being able to
-- SET ROLE into the owner.
--
-- The editor (supatype_sql_editor) is a member of the owner WITH INHERIT TRUE, SET FALSE (see
-- 20261015120000_supatype_sql_editor.sql), so submitted SQL cannot SET ROLE into the owner. The
-- server's SQL runner hands the editor's new objects to the owner before commit with REASSIGN
-- OWNED BY supatype_sql_editor TO <owner>. From Postgres 16 that changes each object's owner only
-- for a role that can SET ROLE to the new owner, so as the editor it failed with
-- `must be able to SET ROLE "supatype_owner"` whenever the statement created anything.
--
-- The reassign now runs in public.supatype_sql_editor_reassign_owned(), a SECURITY DEFINER
-- function the editor alone may execute. It takes no arguments: the source is always
-- supatype_sql_editor and the target is always the current database's owner, and it refuses a
-- target that is a superuser or is not a role the editor already inherits. So the function can
-- only ever move the editor's objects to the role whose rights the editor already has.
--
-- Its owner is a dedicated role, supatype_sql_editor_reassigner, because no existing role fits:
--   * The owner itself cannot own it. REASSIGN OWNED needs the privileges of the source role,
--     and granting the editor to the owner would make the two members of each other, which
--     Postgres refuses.
--   * A superuser would work, but then the function runs as a superuser.
-- The reassigner is NOLOGIN and is granted to nobody. It inherits the editor (so it has the
-- privileges of the role it reassigns from) and may SET ROLE to the owner without inheriting it
-- (which is all ALTER ... OWNER asks of the receiving side). Its only use is this function's
-- fixed body.
--
--   * Self-host: the database is owned by supatype_owner (the editor migration moved it), which
--     is granted to the reassigner here.
--   * A database owned by a plain non-superuser the editor inherits gets the same grant.
--   * Cloud: the database is owned by the superuser <ref>_role when this runs, so no owner is
--     granted here. The provisioner creates <ref>_owner, makes it the database owner and must
--     grant it to the reassigner (WITH INHERIT FALSE, SET TRUE) alongside the editor's grant.
--
-- Idempotent: the role and grants are re-asserted and the function is recreated on every run.
DO $$
DECLARE
  owner_role text;
  owner_is_superuser boolean;
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supatype_sql_editor_reassigner') THEN
    CREATE ROLE supatype_sql_editor_reassigner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOINHERIT NOREPLICATION NOBYPASSRLS;
  END IF;
  ALTER ROLE supatype_sql_editor_reassigner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
    NOINHERIT NOREPLICATION NOBYPASSRLS;

  GRANT supatype_sql_editor TO supatype_sql_editor_reassigner WITH INHERIT TRUE, SET FALSE;

  SELECT pg_get_userbyid(d.datdba), r.rolsuper
    INTO owner_role, owner_is_superuser
    FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba
   WHERE d.datname = current_database();

  IF NOT owner_is_superuser AND owner_role <> 'supatype_sql_editor'
     AND pg_has_role('supatype_sql_editor', owner_role, 'USAGE') THEN
    EXECUTE format('GRANT %I TO supatype_sql_editor_reassigner WITH INHERIT FALSE, SET TRUE',
      owner_role);
  END IF;
END $$;

-- Dropped and made again rather than replaced, so no grant left on an older copy survives.
DROP FUNCTION IF EXISTS public.supatype_sql_editor_reassign_owned();

CREATE FUNCTION public.supatype_sql_editor_reassign_owned()
  RETURNS void
  LANGUAGE plpgsql
  SECURITY DEFINER
  SET search_path = pg_catalog, pg_temp
AS $fn$
DECLARE
  target name;
  target_is_superuser boolean;
BEGIN
  SELECT r.rolname, r.rolsuper
    INTO target, target_is_superuser
    FROM pg_catalog.pg_database d
    JOIN pg_catalog.pg_roles r ON r.oid = d.datdba
   WHERE d.datname = pg_catalog.current_database();

  IF target IS NULL OR target_is_superuser OR target = 'supatype_sql_editor'
     OR NOT pg_catalog.pg_has_role('supatype_sql_editor', target, 'USAGE') THEN
    RAISE EXCEPTION 'supatype: the SQL editor cannot hand its objects to the database owner %',
      target USING ERRCODE = 'insufficient_privilege';
  END IF;

  EXECUTE pg_catalog.format('REASSIGN OWNED BY supatype_sql_editor TO %I', target);
END;
$fn$;

-- Default privileges in public grant EXECUTE on new functions to the API roles. Take every grant
-- away while supatype_admin still owns it, then hand it to the reassigner and let only the
-- editor run it.
DO $$
DECLARE
  grantee_role text;
BEGIN
  REVOKE ALL ON FUNCTION public.supatype_sql_editor_reassign_owned() FROM PUBLIC;
  FOR grantee_role IN
    SELECT DISTINCT pg_get_userbyid(a.grantee)
      FROM pg_proc p, aclexplode(p.proacl) a
     WHERE p.oid = 'public.supatype_sql_editor_reassign_owned()'::regprocedure
       AND a.grantee <> 0 AND a.grantee <> p.proowner
  LOOP
    EXECUTE format('REVOKE ALL ON FUNCTION public.supatype_sql_editor_reassign_owned() FROM %I',
      grantee_role);
  END LOOP;
END $$;

ALTER FUNCTION public.supatype_sql_editor_reassign_owned() OWNER TO supatype_sql_editor_reassigner;
GRANT EXECUTE ON FUNCTION public.supatype_sql_editor_reassign_owned() TO supatype_sql_editor;

-- migrate:down
-- Deliberately empty: migrate.sh applies this file with plain psql -f, so a DROP here would
-- immediately undo the function above. See 20260809214500_supatype_mask.sql.
