-- migrate:up

-- The schema engine keeps its own state in schema _supatype (versions, field rules, realtime
-- channels, studio members). The API roles need USAGE on it: an engine-generated rule table is
-- read by anon and authenticated, and without USAGE that read fails with "permission denied for
-- schema _supatype".
--
-- The engine grants this itself when it pushes as the schema's owner, but on a host where it
-- pushes as a non-owner role (Supatype Cloud: <ref>_role) that grant is a no-op, so the image
-- makes it. The schema is created here, owned by supatype_admin, when it is missing; an existing
-- one keeps its owner. Only schema USAGE is granted: nothing on the tables or functions in it.
-- Each grant is guarded on the role existing, so this is safe on a database without them, and
-- GRANT is idempotent, so it is safe to run again.
CREATE SCHEMA IF NOT EXISTS _supatype AUTHORIZATION supatype_admin;

DO $$
DECLARE
  api_role text;
BEGIN
  FOREACH api_role IN ARRAY ARRAY['anon', 'authenticated', 'service_role'] LOOP
    IF EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = api_role) THEN
      EXECUTE format('GRANT USAGE ON SCHEMA _supatype TO %I', api_role);
    END IF;
  END LOOP;
END $$;

-- migrate:down
-- Deliberately empty: migrate.sh applies this file with plain psql -f, so a REVOKE here would
-- undo the grants above.
