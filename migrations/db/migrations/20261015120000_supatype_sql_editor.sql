-- migrate:up

-- The role the Studio SQL editor connects as (SEC-SQL).
--
-- Before this, Studio's SQL editor connected as the database's owning role, which on this
-- image is a superuser (supatype_admin). A superuser session can always switch back to
-- itself, so no sandbox built with SET ROLE could hold: the submitted SQL could RESET
-- ROLE, SET SESSION AUTHORIZATION DEFAULT or set_config('role', ...) and regain superuser,
-- then COPY ... TO PROGRAM, ALTER SYSTEM, read pg_authid or CREATE ROLE. The only thing
-- that actually contains it is logging in as a role that is not a superuser.
--
-- So: a LOGIN role, deliberately NOT a superuser, listed in supatype_mask.exempt_roles so it
-- still sees masked columns as it did when it was a superuser. Each host sets its password
-- out of band.
--
-- What the editor is a member of -- the role that lets it run DDL and read every row -- is
-- set per host, NOT here, because a project database must be owned by a role that is not a
-- superuser (SQ1a):
--
--   * On this image's own databases and on a freshly provisioned project database, the owner
--     (pg_database.datdba) is still a superuser. The migration therefore grants NOTHING: it
--     would be the very escalation this change removes. The host then gives the database a
--     non-superuser owner and grants THAT to the editor -- self-host transfers the stack
--     database to the demoted `postgres` role; Cloud's provisioner creates `<ref>_owner`.
--   * A database already owned by a non-superuser (the bare image's `postgres` database) is
--     inherited straight away.
--
-- Membership is granted WITH INHERIT TRUE (the editor uses the owner's rights without SET
-- ROLE) and SET FALSE (so submitted SQL cannot `SET ROLE <owner>` into the owner session).
-- The editor reassigns objects it creates to the owner before commit (REASSIGN OWNED BY
-- supatype_sql_editor TO <owner>), so nothing stays owned by it.
--
-- Runs against a database being created and one that already exists: CREATE ROLE and the
-- grant are both guarded.
DO $$
DECLARE
  owner_role text;
  owner_is_superuser boolean;
  obj record;
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supatype_sql_editor') THEN
    CREATE ROLE supatype_sql_editor LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
      NOINHERIT NOREPLICATION NOBYPASSRLS;
  END IF;

  SELECT pg_get_userbyid(d.datdba), r.rolsuper
    INTO owner_role, owner_is_superuser
    FROM pg_database d JOIN pg_roles r ON r.oid = d.datdba
   WHERE d.datname = current_database();

  IF NOT owner_is_superuser THEN
    -- A non-superuser already owns this database (the bare image's `postgres` database):
    -- inherit it straight away.
    EXECUTE format('GRANT %I TO supatype_sql_editor WITH INHERIT TRUE, SET FALSE', owner_role);

  ELSIF owner_role = 'supatype_admin'
        AND EXISTS (SELECT FROM pg_roles WHERE rolname = 'postgres' AND NOT rolsuper) THEN
    -- Self-host (SQ1a): a project database owned by the superuser `supatype_admin` must be
    -- re-owned by a role that is not a superuser. The demoted `postgres` role is the one the
    -- image already keeps for exactly this. Move the database, and the project's own objects in
    -- `public`, from supatype_admin to postgres so an existing project moves too. The system
    -- schemas (auth, storage, realtime, graphql, cron, ...) that supatype_admin also owns are
    -- left alone: they are managed by their own admin roles and are not the project's data.
    -- Cloud reaches neither branch: its database is owned by `<ref>_role`, and its provisioner
    -- creates `<ref>_owner` and grants it here.
    EXECUTE format('ALTER DATABASE %I OWNER TO postgres', current_database());

    FOR obj IN
      SELECT c.oid::regclass AS ident, c.relkind
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'public' AND c.relowner = 'supatype_admin'::regrole
         AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
    LOOP
      EXECUTE format('ALTER %s %s OWNER TO postgres',
        CASE obj.relkind WHEN 'S' THEN 'SEQUENCE'
                         WHEN 'v' THEN 'VIEW'
                         WHEN 'm' THEN 'MATERIALIZED VIEW'
                         ELSE 'TABLE' END,
        obj.ident);
    END LOOP;

    FOR obj IN
      SELECT p.oid::regprocedure AS ident
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'public' AND p.proowner = 'supatype_admin'::regrole
    LOOP
      EXECUTE format('ALTER ROUTINE %s OWNER TO postgres', obj.ident);
    END LOOP;

    EXECUTE 'GRANT postgres TO supatype_sql_editor WITH INHERIT TRUE, SET FALSE';
  END IF;
  -- Any other superuser owner (Cloud's `<ref>_role`): the host gives the database a
  -- non-superuser owner and grants it to the editor itself.
END $$;

-- migrate:down
-- Deliberately empty: migrate.sh applies this file with plain `psql -f`, so a DROP here
-- would immediately undo the role above. See 20260809214500_supatype_mask.sql.
