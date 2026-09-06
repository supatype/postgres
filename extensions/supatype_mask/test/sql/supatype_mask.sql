-- supatype_mask regression tests.
--
-- The assertions here are the ones the design was gated on: masking survives plan
-- reuse across callers, a qual cannot be used as an oracle, an UPDATE round-trip does
-- not destroy a value the caller never saw, and every fail-closed path stays closed.

LOAD 'supatype_mask';

CREATE ROLE mask_app;
CREATE ROLE service_role;

CREATE TABLE posts (
  id        int primary key,
  title     text,
  salary    numeric DEFAULT 0,
  notes     text    DEFAULT 'default notes',
  author_id text
);

INSERT INTO posts (id, title, salary, notes, author_id) VALUES
  (1, 'alice post', 100, 'alice notes', 'alice'),
  (2, 'bob post',   200, 'bob notes',   'bob');

-- The predicates deliberately mirror what the engine generates: a whole-row argument
-- expanded into a one-row derived table aliased with the table's own name, so the rule
-- expression is used verbatim.  `mask.uid` stands in for the JWT claim.
CREATE FUNCTION can_read_posts__salary(posts posts) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT author_id = current_setting('mask.uid', true) FROM (SELECT ($1).*) AS posts;
  $$;

CREATE FUNCTION can_write_posts__salary(posts posts) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT author_id = current_setting('mask.uid', true)
       AND current_setting('mask.writer', true) = 'on'
      FROM (SELECT ($1).*) AS posts;
  $$;

-- Readable by everyone, writable only by alice: an identity-only write rule, which is
-- the shape that still works on INSERT.
CREATE FUNCTION can_read_posts__notes(posts posts) RETURNS boolean
  LANGUAGE sql STABLE AS $$ SELECT true FROM (SELECT ($1).*) AS posts; $$;

CREATE FUNCTION can_write_posts__notes(posts posts) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT current_setting('mask.uid', true) = 'alice' FROM (SELECT ($1).*) AS posts;
  $$;

SECURITY LABEL FOR supatype ON COLUMN posts.salary
  IS 'MASK READ public.can_read_posts__salary WRITE public.can_write_posts__salary';
SECURITY LABEL FOR supatype ON COLUMN posts.notes
  IS 'MASK READ public.can_read_posts__notes WRITE public.can_write_posts__notes';

CREATE VIEW posts_view AS SELECT id, title, salary FROM posts;

GRANT SELECT, INSERT, UPDATE, DELETE ON posts TO mask_app, service_role;
GRANT SELECT ON posts_view TO mask_app;

SET ROLE mask_app;

-- ============================================================================
-- Reads mask
-- ============================================================================

SET mask.uid = 'alice';
SELECT id, title, salary, notes FROM posts ORDER BY id;

SET mask.uid = 'bob';
SELECT id, title, salary, notes FROM posts ORDER BY id;

-- `SELECT *` is the request PostgREST sends when no `select` is given, and it must come
-- back masked rather than 403.
SET mask.uid = 'alice';
SELECT * FROM posts ORDER BY id;

-- ============================================================================
-- Plan reuse across callers.  The whole risk in one test: a plan built for one
-- identity must not answer for another.
-- ============================================================================

SET plan_cache_mode = force_generic_plan;
PREPARE leak AS SELECT id, salary FROM posts ORDER BY id;

SET mask.uid = 'alice';
EXECUTE leak;
SET mask.uid = 'bob';
EXECUTE leak;

DEALLOCATE leak;
RESET plan_cache_mode;

-- ============================================================================
-- No oracle through the qual.  Masking only the target list would let a dozen
-- requests binary-search the value.
-- ============================================================================

SET mask.uid = 'alice';
SELECT id FROM posts WHERE salary > 150 ORDER BY id;
SELECT id FROM posts WHERE salary::text LIKE '2%' ORDER BY id;
SELECT id FROM posts ORDER BY salary NULLS LAST, id;
SELECT id FROM posts GROUP BY id, salary ORDER BY id;

-- Outer reference from inside a correlated subquery: the level stack is what makes
-- this one masked rather than compared against the real value.
SELECT p.id FROM posts p
 WHERE EXISTS (SELECT 1 FROM posts q WHERE q.id <> p.id AND q.salary = p.salary);

-- A view is expanded before the planner hook runs, so it cannot route around masking.
SELECT * FROM posts_view ORDER BY id;

-- ============================================================================
-- Aggregates reject per row rather than returning a quietly partial number
-- ============================================================================

SELECT sum(salary) FROM posts;             -- errors: row 2 is not readable
SELECT sum(salary) FROM posts WHERE id = 1;  -- correct for a caller entitled to it
SELECT count(*) FROM posts;                -- no column reference, unaffected

-- ============================================================================
-- Writes: preserve, accept, reject
-- ============================================================================

-- Never saw it, so the null it round-tripped is an artefact, not an intent to clear.
SET mask.uid = 'alice';
UPDATE posts SET salary = NULL WHERE id = 2;
SET mask.uid = 'bob';
SELECT id, salary FROM posts WHERE id = 2;

-- Saw it and tried to change it, without the write rule: rejected loudly.
SET mask.uid = 'alice';
UPDATE posts SET salary = 999 WHERE id = 1;

-- With the write rule satisfied: accepted.
SET mask.writer = 'on';
UPDATE posts SET salary = 999 WHERE id = 1;
SELECT id, salary FROM posts WHERE id = 1;
RESET mask.writer;

-- An unrelated column is untouched by any of this.
UPDATE posts SET title = 'retitled' WHERE id = 1;
SELECT id, title FROM posts WHERE id = 1;

-- RETURNING is a projection, so it masks.
SET mask.uid = 'bob';
UPDATE posts SET title = 'bob retitled' WHERE id = 2 RETURNING id, title, salary;

-- A read of a masked column on the right-hand side of an assignment is still a read.
SET mask.uid = 'alice';
UPDATE posts SET title = coalesce(salary::text, 'masked') WHERE id = 2;
SELECT id, title FROM posts WHERE id = 2;

-- ============================================================================
-- INSERT has no old row, so an unauthorised value coerces to the column default
-- ============================================================================

-- Row-dependent write rule: cannot be evaluated before the row exists, so `salary`
-- falls back to its default for everyone.  Identity-only `notes` is accepted for
-- alice and refused for bob.
SET mask.uid = 'alice';
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (3, 'alice new', 555, 'alice wrote this', 'alice');

SET mask.uid = 'bob';
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (4, 'bob new', 777, 'bob wrote this', 'bob');

RESET ROLE;
SELECT id, salary, notes FROM posts WHERE id IN (3, 4) ORDER BY id;
SET ROLE mask_app;

-- ============================================================================
-- Constructs that fail closed rather than guess
-- ============================================================================

SET mask.uid = 'alice';

-- A whole-row reference would expand to the unmasked column.
SELECT to_jsonb(posts) FROM posts WHERE id = 1;

-- MERGE's action lists are not covered by the assignment rewrite.
CREATE TEMP TABLE post_source (id int, salary numeric);
INSERT INTO post_source VALUES (1, 1);
MERGE INTO posts p USING post_source s ON p.id = s.id
  WHEN MATCHED THEN UPDATE SET salary = s.salary;

-- COPY FROM goes straight to the insert path, so no coercion can reach it. The refusal
-- happens in ProcessUtility, so it lands before COPY's own file-permission check.
COPY posts FROM '/dev/null';

-- COPY TO is rewritten through the planner and masks.
COPY posts (id, salary) TO STDOUT;

-- ============================================================================
-- A WRITE-only label restricts writes and leaves reads alone
-- ============================================================================

RESET ROLE;
CREATE TABLE locked (id int, tier text);
INSERT INTO locked VALUES (1, 'free');
CREATE FUNCTION can_write_locked__tier(locked locked) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT current_setting('mask.uid', true) = 'alice' FROM (SELECT ($1).*) AS locked;
  $$;
SECURITY LABEL FOR supatype ON COLUMN locked.tier
  IS 'MASK WRITE public.can_write_locked__tier';
GRANT SELECT, UPDATE ON locked TO mask_app;
SET ROLE mask_app;

-- Reads are untouched: no predicate call, and a whole-row reference still works
-- because there is nothing to disclose.
SET mask.uid = 'bob';
SELECT id, tier FROM locked;
SELECT to_jsonb(locked) FROM locked;

-- The write is rejected loudly rather than silently preserved: the caller can read the
-- column, so they saw what they were changing.
UPDATE locked SET tier = 'pro' WHERE id = 1;

SET mask.uid = 'alice';
UPDATE locked SET tier = 'pro' WHERE id = 1;
SELECT id, tier FROM locked;

-- ============================================================================
-- Exemption is an explicit role list, never "is the table owner"
-- ============================================================================

RESET ROLE;
SET ROLE service_role;
SET mask.uid = 'nobody';
SELECT id, salary FROM posts WHERE id = 1;
RESET ROLE;

-- ============================================================================
-- Masking cannot be switched off from SQL
-- ============================================================================
--
-- Both settings are PGC_SIGHUP, so neither a caller nor a superuser can reach them with
-- a statement. This is the weakest link if it is reachable: dropping a label or the
-- extension is a schema change the differ reports on the next push, whereas
-- `ALTER ROLE ... SET supatype_mask.enabled = off` would be a permanent, invisible
-- bypass of every column at once.

SET ROLE mask_app;
SET supatype_mask.enabled = off;
SET supatype_mask.exempt_roles = 'mask_app';
RESET ROLE;

-- Not even as a superuser, and not pinned onto a role or a database either.
SET supatype_mask.enabled = off;
ALTER ROLE mask_app SET supatype_mask.enabled = off;
ALTER DATABASE :"DBNAME" SET supatype_mask.enabled = off;

-- Still masked.
SET ROLE mask_app;
SET mask.uid = 'bob';
SELECT id, salary FROM posts WHERE id = 1;
RESET ROLE;

-- ============================================================================
-- Label validation
-- ============================================================================

SECURITY LABEL FOR supatype ON COLUMN posts.title IS 'MASK';
SECURITY LABEL FOR supatype ON COLUMN posts.title IS 'HIDE READ f';
SECURITY LABEL FOR supatype ON COLUMN posts.title IS 'MASK WRITE public.can_write_posts__notes';
SECURITY LABEL FOR supatype ON TABLE posts IS 'MASK READ public.can_read_posts__notes';

-- ============================================================================
-- An unresolvable or IMMUTABLE predicate masks the column, it does not expose it
-- ============================================================================

CREATE TABLE secrets (id int, value text);
INSERT INTO secrets VALUES (1, 'shhh');
GRANT SELECT ON secrets TO mask_app;

SECURITY LABEL FOR supatype ON COLUMN secrets.value
  IS 'MASK READ public.no_such_predicate';
SET ROLE mask_app;
SELECT * FROM secrets;
RESET ROLE;

-- An immutable predicate can be folded into a cached plan and reused across callers,
-- which is exactly the leak the plan-reuse test above exists to catch.
CREATE FUNCTION can_read_secrets__value(secrets secrets) RETURNS boolean
  LANGUAGE sql IMMUTABLE AS $$ SELECT true FROM (SELECT ($1).*) AS secrets; $$;
SECURITY LABEL FOR supatype ON COLUMN secrets.value
  IS 'MASK READ public.can_read_secrets__value';
SET ROLE mask_app;
SELECT * FROM secrets;
RESET ROLE;

-- Dropping the label restores the column.
SECURITY LABEL FOR supatype ON COLUMN secrets.value IS NULL;
SET ROLE mask_app;
SELECT * FROM secrets;
RESET ROLE;

-- ============================================================================
-- Row-independent predicate.  A zero-argument overload declares that the answer
-- depends on session state, not the row, so it is emitted as an uncorrelated
-- (SELECT pred()) that the planner hoists to a once-per-scan InitPlan -- yet it
-- is still re-evaluated each execution, so a cached plan built for one identity
-- does not answer for another (the same guarantee as the per-row form).
-- ============================================================================

CREATE TABLE ri (id int, value text);
INSERT INTO ri VALUES (1, 'a'), (2, 'b');
GRANT SELECT ON ri TO mask_app;

-- Only a zero-arg overload exists: that is what declares row-independence.
CREATE FUNCTION can_read_ri() RETURNS boolean LANGUAGE sql STABLE AS
  $$ SELECT current_setting('mask.uid', true) = 'alice' $$;
SECURITY LABEL FOR supatype ON COLUMN ri.value IS 'MASK READ public.can_read_ri';

-- The predicate becomes an InitPlan (evaluated once), not a per-row call.
SET ROLE mask_app;
SET mask.uid = 'alice';
EXPLAIN (COSTS OFF) SELECT id, value FROM ri ORDER BY id;
SELECT id, value FROM ri ORDER BY id;      -- alice: readable
SET mask.uid = 'bob';
SELECT id, value FROM ri ORDER BY id;      -- bob: masked
RESET ROLE;

-- Plan reuse across callers: the InitPlan re-runs each execution.
SET plan_cache_mode = force_generic_plan;
PREPARE ri_leak AS SELECT id, value FROM ri ORDER BY id;
SET ROLE mask_app;
SET mask.uid = 'alice';
EXECUTE ri_leak;                            -- readable
SET mask.uid = 'bob';
EXECUTE ri_leak;                            -- masked, same cached plan
RESET ROLE;
DEALLOCATE ri_leak;
RESET plan_cache_mode;

-- When both a whole-row and a zero-arg overload exist, the whole-row (per-row)
-- form is preferred, so the rule stays row-dependent.
CREATE FUNCTION can_read_ri(ri ri) RETURNS boolean LANGUAGE sql STABLE AS
  $$ SELECT ($1).id = 1 $$;
SECURITY LABEL FOR supatype ON COLUMN ri.value IS 'MASK READ public.can_read_ri';
SET ROLE mask_app;
SET mask.uid = 'bob';
SELECT id, value FROM ri ORDER BY id;      -- per-row: only id = 1 is readable
RESET ROLE;

SECURITY LABEL FOR supatype ON COLUMN ri.value IS NULL;
DROP FUNCTION can_read_ri(), can_read_ri(ri);
REVOKE ALL ON ri FROM mask_app;
DROP TABLE ri;

DROP VIEW posts_view;
DROP FUNCTION can_read_posts__salary(posts), can_write_posts__salary(posts),
              can_read_posts__notes(posts), can_write_posts__notes(posts),
              can_read_secrets__value(secrets), can_write_locked__tier(locked);
REVOKE ALL ON posts, secrets FROM mask_app, service_role;
REVOKE ALL ON locked FROM mask_app;
DROP TABLE posts, secrets, post_source, locked;
DROP ROLE mask_app, service_role;
