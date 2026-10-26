-- migrate:up

-- The role the Studio SQL editor connects as (SEC-SQL), and the non-superuser role that owns
-- the project's data (SQ1a).
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
-- What it is a member of decides what it can reach, and that must be ONLY the project owner's
-- ownership rights -- not a role that carries predefined-role memberships. The demoted
-- `postgres` role is a member of pg_read_all_data (which reads pg_authid, so a superuser's
-- password hash could be cracked offline), pg_monitor (other sessions' query text) and
-- pg_signal_backend (cancel other sessions), so the editor must NOT inherit `postgres`.
-- Instead the project database is owned by a dedicated role, `supatype_owner`, created here
-- with no login, no superuser and no predefined-role memberships, and the editor is a member
-- of that and nothing else.
--
--   * Self-host: the database is owned by the superuser supatype_admin. The migration re-owns
--     it, and the project's own objects in public, to supatype_owner (an existing project
--     moves too), and grants supatype_owner to the editor. The system schemas supatype_admin
--     also owns (auth, storage, realtime, graphql, cron, ...) are left alone; they are managed
--     by their own admin roles and are not the project's data.
--   * Cloud: the database is owned by the superuser <ref>_role, so this migration does nothing
--     with ownership; the provisioner creates <ref>_owner (also a plain non-superuser with no
--     predefined memberships) and grants it to the editor.
--   * A database already owned by a plain non-superuser is inherited straight away.
--
-- Membership is WITH INHERIT TRUE (the editor uses the owner's rights without SET ROLE) and
-- SET FALSE (so submitted SQL cannot SET ROLE into the owner). The editor reassigns objects it
-- creates to the owner before commit (REASSIGN OWNED BY supatype_sql_editor TO <owner>). The
-- migration is idempotent and runs for a new or existing database.
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

  -- Self-host: a superuser (supatype_admin) owns the database, or a prior run of this migration
  -- left it owned by the demoted `postgres` role. Either way, move it to the dedicated
  -- supatype_owner, which carries no predefined-role memberships, and make the editor a member
  -- of that alone. Cloud's database is owned by <ref>_role, which is neither, so it is skipped.
  IF owner_role IN ('supatype_admin', 'postgres') THEN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supatype_owner') THEN
      CREATE ROLE supatype_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE
        NOINHERIT NOREPLICATION NOBYPASSRLS;
    END IF;

    EXECUTE format('ALTER DATABASE %I OWNER TO supatype_owner', current_database());

    FOR obj IN
      SELECT c.oid::regclass AS ident, c.relkind
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
       WHERE n.nspname = 'public' AND pg_get_userbyid(c.relowner) = owner_role
         AND c.relkind IN ('r', 'p', 'v', 'm', 'S')
    LOOP
      EXECUTE format('ALTER %s %s OWNER TO supatype_owner',
        CASE obj.relkind WHEN 'S' THEN 'SEQUENCE'
                         WHEN 'v' THEN 'VIEW'
                         WHEN 'm' THEN 'MATERIALIZED VIEW'
                         ELSE 'TABLE' END,
        obj.ident);
    END LOOP;

    FOR obj IN
      SELECT p.oid::regprocedure AS ident
        FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
       WHERE n.nspname = 'public' AND pg_get_userbyid(p.proowner) = owner_role
    LOOP
      EXECUTE format('ALTER ROUTINE %s OWNER TO supatype_owner', obj.ident);
    END LOOP;

    -- Drop any membership a prior run of this migration gave the editor in the demoted
    -- `postgres` role, so it is left a member of supatype_owner only.
    EXECUTE 'REVOKE postgres FROM supatype_sql_editor';
    EXECUTE 'GRANT supatype_owner TO supatype_sql_editor WITH INHERIT TRUE, SET FALSE';

  ELSIF NOT owner_is_superuser THEN
    -- A plain non-superuser already owns this database: inherit it straight away.
    EXECUTE format('GRANT %I TO supatype_sql_editor WITH INHERIT TRUE, SET FALSE', owner_role);
  END IF;
  -- Any other superuser owner (Cloud's <ref>_role): the provisioner gives the database a
  -- non-superuser owner and grants it to the editor itself.
END $$;

-- migrate:down
-- Deliberately empty: migrate.sh applies this file with plain `psql -f`, so a DROP here
-- would immediately undo the role above. See 20260809214500_supatype_mask.sql.
