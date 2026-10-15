-- migrate:up

-- Studio's SQL editor may read and change rows in `auth` and `storage`, and nothing more there
-- (SQ5).
--
-- Since SEC-SQL the editor logs in as supatype_sql_editor, which is not a superuser and is a member
-- of the project owner only. The auth and storage schemas and tables belong to the service roles
-- (supatype_admin, supatype_auth_admin, supatype_storage_admin), so the editor had no USAGE on
-- either schema: an admin could not look up a user or confirm an email from SQL. This is the same
-- reach Supabase gives its SQL editor role:
--
--   * USAGE on both schemas, and SELECT, INSERT, UPDATE, DELETE on every table and view in them,
--     with USAGE and SELECT on their sequences so an insert can take a default id.
--   * The same on tables created later, through default privileges for every role that creates
--     tables there: the service roles, the schemas' owners and whoever already owns a table there.
--     The auth and storage servers create their tables after this runs, as the role they log in as.
--   * BYPASSRLS. The auth server turns row level security on for its tables and the engine does
--     for storage.objects, with policies only for the API roles, so without it every SELECT would
--     return no rows and every UPDATE would change none, silently. The editor already reads every
--     row of the project's own tables, as a member of their owner; this extends that to the
--     service tables, as it was when the editor was a superuser.
--
-- No CREATE on either schema, no TRUNCATE, REFERENCES or TRIGGER, no grant option and no
-- ownership, so it cannot change a table's shape, owner or grants. RLS switches and policies on
-- these tables stay with the SECURITY DEFINER helpers in 20261015130000_studio_admin_helpers.sql.
--
-- Granted to supatype_sql_editor itself, not to the project owner, whose reach is broader and is
-- what the data plane's owner connections hold. Idempotent; runs for a new or existing database.
DO $$
DECLARE
  sch text;
  creator text;
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'supatype_sql_editor') THEN
    RETURN;
  END IF;

  ALTER ROLE supatype_sql_editor BYPASSRLS;

  FOREACH sch IN ARRAY ARRAY['auth', 'storage'] LOOP
    CONTINUE WHEN NOT EXISTS (SELECT FROM pg_namespace WHERE nspname = sch);

    EXECUTE format('GRANT USAGE ON SCHEMA %I TO supatype_sql_editor', sch);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA %I TO supatype_sql_editor', sch);
    EXECUTE format('GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA %I TO supatype_sql_editor', sch);

    FOR creator IN
      SELECT rolname FROM pg_roles
       WHERE rolname IN ('supatype_admin', 'supatype_auth_admin', 'supatype_storage_admin', 'postgres')
      UNION
      SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname = sch
      UNION
      SELECT pg_get_userbyid(relowner) FROM pg_class
       WHERE relnamespace = sch::regnamespace AND relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
    LOOP
      CONTINUE WHEN creator = 'supatype_sql_editor';
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO supatype_sql_editor',
        creator, sch);
      EXECUTE format(
        'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA %I GRANT USAGE, SELECT ON SEQUENCES TO supatype_sql_editor',
        creator, sch);
    END LOOP;
  END LOOP;
END $$;

-- migrate:down
-- Deliberately empty: migrate.sh applies this file with plain `psql -f`, so a REVOKE here
-- would immediately undo the grants above. See 20260809214500_supatype_mask.sql.
