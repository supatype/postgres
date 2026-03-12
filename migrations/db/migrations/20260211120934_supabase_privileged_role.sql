-- migrate:up
do $$
begin
  if not exists (select from pg_roles where rolname = 'supatype_privileged_role') then
    create role supatype_privileged_role;
    grant supatype_privileged_role to postgres, supatype_etl_admin;
  end if;
end $$;

-- migrate:down
