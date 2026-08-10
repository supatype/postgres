\echo Use "CREATE EXTENSION supatype_mask" to load this file. \quit

-- The planner rewrite plants this in a CASE branch where a column value would have
-- been, so it has to be able to stand in for a column of any type.
--
-- VOLATILE and not STRICT, deliberately: an IMMUTABLE function with constant arguments
-- is folded at plan time, which would fire the error before the CASE could
-- short-circuit and break every statement that touches the column.
CREATE FUNCTION supatype_mask.deny(message text, sample anyelement)
  RETURNS anyelement
  AS 'MODULE_PATHNAME', 'supatype_mask_deny'
  LANGUAGE c VOLATILE;

COMMENT ON FUNCTION supatype_mask.deny(text, anyelement) IS
  'Raises insufficient_privilege. Planted by supatype_mask where a masked column may '
  'not be written or aggregated.';

-- It only ever raises, so there is nothing to withhold. The grants matter the other
-- way round: a caller who cannot execute it gets "permission denied for schema
-- supatype_mask" instead of the intended error.
GRANT USAGE ON SCHEMA supatype_mask TO PUBLIC;
GRANT EXECUTE ON FUNCTION supatype_mask.deny(text, anyelement) TO PUBLIC;
