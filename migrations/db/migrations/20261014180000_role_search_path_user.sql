-- migrate:up

-- The roles' search path names `$user` again, not a schema called `\$user`.
--
-- The initial setup set these paths with `"\$user"`, in a file psql runs with `-f`, where nothing
-- unescapes the backslash: the stored path began with a schema named `\$user`, which never exists,
-- instead of the role's own schema. The setup now writes `"$user"`; this corrects a cluster created
-- before that, the next time the image starts on it. Only an entry that still has the backslash is
-- rewritten, so a path an operator has changed since is left as it is.
DO $fix$
DECLARE
  r record;
BEGIN
  FOR r IN
    SELECT s.setrole::regrole AS role, substr(c, length('search_path=') + 1) AS path
      FROM pg_db_role_setting s, unnest(s.setconfig) AS c
     WHERE s.setdatabase = 0
       AND c LIKE 'search_path=%'
       AND position('"\$user"' IN c) > 0
  LOOP
    EXECUTE format(
      'ALTER ROLE %s SET search_path TO %s',
      r.role,
      replace(r.path, '"\$user"', '"$user"')
    );
  END LOOP;
END
$fix$;

-- migrate:down
