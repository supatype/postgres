-- migrate:up
alter role supatype_read_only_user set default_transaction_read_only = on;

-- migrate:down
