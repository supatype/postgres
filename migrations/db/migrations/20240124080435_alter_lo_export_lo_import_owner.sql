-- migrate:up
alter function pg_catalog.lo_export owner to supatype_admin;
alter function pg_catalog.lo_import(text) owner to supatype_admin;
alter function pg_catalog.lo_import(text, oid) owner to supatype_admin;

-- migrate:down
