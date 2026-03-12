-- migrate:up
alter role supatype_admin set log_statement = none;
alter role supatype_auth_admin set log_statement = none;
alter role supatype_storage_admin set log_statement = none;

-- migrate:down
