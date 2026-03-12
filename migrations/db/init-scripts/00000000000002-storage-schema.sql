-- migrate:up

CREATE SCHEMA IF NOT EXISTS storage AUTHORIZATION supatype_admin;

CREATE USER supatype_storage_admin NOINHERIT CREATEROLE LOGIN NOREPLICATION;
ALTER USER supatype_storage_admin SET search_path = "storage";
GRANT CREATE ON DATABASE postgres TO supatype_storage_admin;

do $$
begin
  if exists (select from pg_namespace where nspname = 'storage') then
    grant usage on schema storage to postgres, anon, authenticated, service_role;
    alter default privileges in schema storage grant all on tables to postgres, anon, authenticated, service_role;
    alter default privileges in schema storage grant all on functions to postgres, anon, authenticated, service_role;
    alter default privileges in schema storage grant all on sequences to postgres, anon, authenticated, service_role;

    grant all on schema storage to supatype_storage_admin with grant option;
  end if;
end $$;

-- migrate:down
