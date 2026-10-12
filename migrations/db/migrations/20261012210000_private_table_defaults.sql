-- migrate:up

-- Tables in public stop being open to anon and authenticated by default.
--
-- The initial schema granted every privilege on a new table in public to anon and authenticated,
-- so a table made by hand was readable and writable with the anon key until something revoked it.
-- A new cluster no longer gets those defaults (see the initial schema); this takes them out of a
-- cluster created before that, the next time the image starts on it.
--
-- Only the defaults change. Tables that already exist keep the grants they have: some of them are
-- served on purpose, and the schema engine writes a model table's grants from its access rules on
-- every push in any case. service_role, postgres and each table's owner keep theirs, and the
-- sequence and function defaults stay as they are.
--
-- The same statement as the Supatype CLI runs on `supatype dev` and on a compose `supatype push`,
-- so a cluster the CLI has already reached is unchanged by this. Every role with defaults in
-- public is covered, not only the two the initial schema wrote, because a role an operator added
-- would otherwise go on handing out the same access.
DO $$
DECLARE owner_role text;
BEGIN
  FOR owner_role IN
    SELECT DISTINCT pg_get_userbyid(defaclrole) FROM pg_default_acl
    WHERE defaclnamespace = 'public'::regnamespace AND defaclobjtype = 'r'
  LOOP
    EXECUTE format(
      'ALTER DEFAULT PRIVILEGES FOR ROLE %I IN SCHEMA public REVOKE ALL ON TABLES FROM anon, authenticated',
      owner_role
    );
  END LOOP;
END $$;

-- migrate:down
