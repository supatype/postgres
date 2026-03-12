-- migrate:up
grant authenticator to supatype_storage_admin;
revoke anon, authenticated, service_role from supatype_storage_admin;

-- migrate:down
