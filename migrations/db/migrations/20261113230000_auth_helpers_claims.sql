-- migrate:up

-- All seven auth helpers (uid, role, email, jwt, claim, claim_text, roles) ship with the image and read the
-- claims PostgREST sets.
--
-- auth.uid, auth.role and auth.email used to read only the legacy per-claim settings, which
-- PostgREST no longer sets, so on a database created before the init script was fixed they return
-- NULL for every real request. auth.jwt, auth.claim and auth.roles are called by Row-level security
-- written Supabase-style, and on a project whose owner role cannot create objects in schema auth the
-- schema engine's push cannot install them. The init script creates all six on a new database; this
-- brings an existing one up to date, and is safe to run on one that has them. The engine's own
-- definitions of jwt, claim, claim_text and roles are matched, so a push that replaces them changes nothing.

-- Gets the User ID from the request cookie
create or replace function auth.uid() returns uuid as $$
  select nullif(
    case when c is not null then c ->> 'sub' else current_setting('request.jwt.claim.sub', true) end,
    '')::uuid
  from (select nullif(current_setting('request.jwt.claims', true), '')::jsonb as c) as claims;
$$ language sql stable;

-- Gets the application role
create or replace function auth.role() returns text as $$
  select case
    when c is not null then coalesce(c -> 'app_metadata' ->> 'role', c ->> 'role', 'anon')
    else coalesce(nullif(current_setting('request.jwt.claim.role', true), ''), 'anon')
  end
  from (select nullif(current_setting('request.jwt.claims', true), '')::jsonb as c) as claims;
$$ language sql stable;

-- Gets the User email
create or replace function auth.email() returns text as $$
  select case
    when c is not null then coalesce(c ->> 'email', '')
    else coalesce(nullif(current_setting('request.jwt.claim.email', true), ''), '')
  end
  from (select nullif(current_setting('request.jwt.claims', true), '')::jsonb as c) as claims;
$$ language sql stable;

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

-- Gets the same claim as text, for scalar comparisons.
create or replace function auth.claim_text(claim_path text) returns text as $$
  select auth.jwt() #>> string_to_array(claim_path, '.');
$$ language sql stable;

-- Gets the roles claim as a text array.
create or replace function auth.roles() returns text[] as $$
  select array(
    select jsonb_array_elements_text(coalesce(auth.jwt() -> 'roles', '[]'::jsonb))
  );
$$ language sql stable;

-- Owned by the same role for all seven (see 20211124212715_update-auth-owner). The first three
-- are already moved by that migration on a database that ran it; this covers the rest.
DO $$
BEGIN
    ALTER FUNCTION auth.uid() OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.uid to supatype_auth_admin';
END $$;

DO $$
BEGIN
    ALTER FUNCTION auth.role() OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.role to supatype_auth_admin';
END $$;

DO $$
BEGIN
    ALTER FUNCTION auth.email() OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.email to supatype_auth_admin';
END $$;

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
    ALTER FUNCTION auth.claim_text(text) OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.claim_text to supatype_auth_admin';
END $$;

DO $$
BEGIN
    ALTER FUNCTION auth.roles() OWNER TO supatype_auth_admin;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'Error encountered when changing owner of auth.roles to supatype_auth_admin';
END $$;

-- migrate:down
