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
-- and the demotion would abort the whole bootstrap. Skip it there, loudly.
--
-- The operator is expected to leave that role without a password (for
-- CloudNativePG, enableSuperuserAccess: false). If it has a usable one (login
-- allowed, not expired), a known password opens a superuser login, so that
-- gets a warning of its own. `migrate doctor` keeps watch on both: an ordinary
-- postgres that is still a superuser, and a bootstrap postgres with a usable
-- password.
do $$
begin
  if exists (select from pg_roles where rolname = 'postgres' and oid = 10) then
    raise warning 'postgres is the bootstrap superuser and cannot be demoted; leaving it SUPERUSER';
    if exists (select from pg_authid
                where rolname = 'postgres' and rolcanlogin and rolpassword is not null
                  and (rolvaliduntil is null or rolvaliduntil > now())) then
      raise warning 'the bootstrap superuser postgres has a usable password; it can log in as a superuser';
    end if;
  else
    ALTER ROLE postgres NOSUPERUSER CREATEDB CREATEROLE LOGIN REPLICATION BYPASSRLS;
  end if;
end $$;

-- migrate:down
