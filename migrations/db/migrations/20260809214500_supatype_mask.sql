-- migrate:up

-- Create the masking extension so field-level access is available out of the box.
--
-- The library is preloaded through `session_preload_libraries`, which is what registers
-- the `supatype` security label provider; the extension itself supplies only the
-- rejection function the planner rewrite plants where a masked column may not be written
-- or aggregated. Without it, a rejection raises "supatype_mask.deny() is not available"
-- rather than the intended permission error.
--
-- `IF NOT EXISTS` because this also runs against databases restored from a dump that
-- already carries it.
CREATE EXTENSION IF NOT EXISTS supatype_mask;

-- migrate:down
-- Deliberately empty, like every other migration here. The default bootstrap path in
-- migrate.sh applies these files with plain `psql -f`, which executes the whole file --
-- so a DROP in this section would immediately undo the CREATE above.
