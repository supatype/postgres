-- migrate:up

-- auth.jwt(), auth.claim(path) and auth.roles() ship with the image.
--
-- Row-level security written Supabase-style calls them, and on a project whose owner role cannot
-- create objects in schema auth the schema engine's push cannot install them, so a policy that
-- uses one failed with "function does not exist". The init script creates them on a new database;
-- this brings a database created before that up to date, and is safe to run on one that has them.
-- They are replaced with the same definitions the engine installs, so a push that replaces them
-- again changes nothing.

-- Gets the verified claims: request.jwt.claims, else an object built from the legacy per-claim
-- settings, else an empty object. An empty claims setting reads as anonymous rather than as a JSON error.
create or replace function auth.jwt() returns jsonb as $$
  select coalesce(
    nullif(current_setting('request.jwt.claims', true), '')::jsonb,
    jsonb_strip_nulls(jsonb_build_object(
      'sub', nullif(current_setting('request.jwt.claim.sub', true), ''),
      'role', nullif(current_setting('request.jwt.claim.role', true), ''),
      'email', nullif(current_setting('request.jwt.claim.email', true), '')
    ))
  );
$$ language sql stable;

-- Gets a nested claim by dotted path, e.g. 'app_metadata.tier'. NULL when the path is absent.
create or replace function auth.claim(claim_path text) returns jsonb as $$
  select auth.jwt() #> string_to_array(claim_path, '.');
$$ language sql stable;

-- Gets the roles claim as a text array.
create or replace function auth.roles() returns text[] as $$
  select array(
    select jsonb_array_elements_text(coalesce(auth.jwt() -> 'roles', '[]'::jsonb))
  );
$$ language sql stable;

-- Owned by the same role as auth.uid, auth.role and auth.email (see 20211124212715_update-auth-owner).
DO $$
BEGIN
    ALTER FUNCTION auth.jwt() OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.jwt to supatype_auth_admin';
END $$;

DO $$
BEGIN
    ALTER FUNCTION auth.claim(text) OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.claim to supatype_auth_admin';
END $$;

DO $$
BEGIN
    ALTER FUNCTION auth.roles() OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.roles to supatype_auth_admin';
END $$;

-- migrate:down
