-- migrate:up

-- demote postgres user
GRANT ALL ON DATABASE postgres TO postgres;
GRANT ALL ON SCHEMA auth TO postgres;
GRANT ALL ON SCHEMA extensions TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA auth TO postgres;
GRANT ALL ON ALL TABLES IN SCHEMA extensions TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA auth TO postgres;
GRANT ALL ON ALL SEQUENCES IN SCHEMA extensions TO postgres;
GRANT ALL ON ALL ROUTINES IN SCHEMA auth TO postgres;
GRANT ALL ON ALL ROUTINES IN SCHEMA extensions TO postgres;
do $$
begin
  if exists (select from pg_namespace where nspname = 'storage') then
    GRANT ALL ON SCHEMA storage TO postgres;
    GRANT ALL ON ALL TABLES IN SCHEMA storage TO postgres;
    GRANT ALL ON ALL SEQUENCES IN SCHEMA storage TO postgres;
    GRANT ALL ON ALL ROUTINES IN SCHEMA storage TO postgres;
  end if;
end $$;
-- Postgres 17 refuses to strip SUPERUSER from the role initdb created (oid 10,
-- the "bootstrap superuser"):
--
--   ERROR:  permission denied to alter role
--   DETAIL:  The bootstrap superuser must have the SUPERUSER attribute.
--
-- In this image initdb creates supatype_admin (POSTGRES_USER), so postgres is
-- an ordinary role and the demotion goes through. Under an operator such as
-- CloudNativePG initdb creates postgres itself, the operator owns that role,
-- and the demotion would abort the whole bootstrap. Skip it there: the role
-- has no password under the operator, so leaving it SUPERUSER exposes nothing.
do $$
begin
  if exists (select from pg_roles where rolname = 'postgres' and oid = 10) then
    raise notice 'postgres is the bootstrap superuser; leaving it alone';
  else
    ALTER ROLE postgres NOSUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS;
  end if;
end $$;

-- migrate:down
