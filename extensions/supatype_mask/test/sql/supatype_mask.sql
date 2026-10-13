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
-- A subquery inside an aggregate is its own scope.  The aggregate consumes what the
-- subquery returns, which is already masked, so only a column the aggregate itself
-- consumes is refused.  This is the shape pg_graphql emits for a nested relation:
-- the child connection is a correlated subquery inside the parent's jsonb_agg.
-- ============================================================================

SET mask.uid = 'alice';

-- No masked column selected, but `q.*` names every column.
SELECT jsonb_agg((SELECT jsonb_agg(r.title ORDER BY r.id)
                    FROM (SELECT q.* FROM posts q WHERE q.id = p.id) r) ORDER BY p.id)
  FROM posts p;

-- A masked column selected in the subquery comes back masked, not refused.
SELECT jsonb_agg((SELECT q.salary FROM posts q WHERE q.id = p.id) ORDER BY p.id)
  FROM posts p;
SELECT jsonb_agg((WITH recs AS (SELECT q.* FROM posts q WHERE q.id = p.id)
                  SELECT jsonb_agg(recs.salary) FROM recs) ORDER BY p.id)
  FROM posts p;

-- An outer column read inside a subquery that the aggregate wraps is still aggregated.
SELECT sum((SELECT p.salary)) FROM posts p;
-- So is one aggregated by an outer-level aggregate written inside a subquery.
SELECT (SELECT sum(p.salary) FROM (VALUES (1)) v) FROM posts p;
SELECT max(salary), string_agg(salary::text, ',') FROM posts;

-- ============================================================================
-- Window functions.  A window aggregate consumes its input exactly as a plain
-- aggregate does, so masking that input to NULL would make `sum() OVER ()` total
-- only the rows the caller may read and say nothing.  It is refused per unreadable
-- row instead, FILTER included.
--
-- PARTITION BY and ORDER BY in a window are target-list entries like a plain ORDER
-- BY or GROUP BY, so they see the masked value: every unreadable row is NULL and
-- they all tie.  Ranks, partitions and running totals therefore carry no order the
-- caller could not compute from what they can already read.  Value functions
-- (`lag`, `first_value`) return the masked value, as a plain select does.
-- ============================================================================

RESET ROLE;
CREATE TABLE staff (id int primary key, dept text, salary numeric, owner text);
INSERT INTO staff VALUES
  (1, 'a', 400, 'alice'), (2, 'a', 100, 'bob'),
  (3, 'b', 300, 'bob'),   (4, 'b', 200, 'alice');
CREATE FUNCTION can_read_staff__salary(staff staff) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT owner = current_setting('mask.uid', true) FROM (SELECT ($1).*) AS staff;
  $$;
SECURITY LABEL FOR supatype ON COLUMN staff.salary
  IS 'MASK READ public.can_read_staff__salary';
GRANT SELECT ON staff TO mask_app;
SET ROLE mask_app;
SET mask.uid = 'alice';

-- Refused: rows 2 and 3 are not readable.
SELECT id, sum(salary) OVER () FROM staff ORDER BY id;
SELECT id, sum(salary) OVER (PARTITION BY dept) FROM staff ORDER BY id;
SELECT id, avg(salary) OVER (ORDER BY id) FROM staff ORDER BY id;
SELECT id, count(*) FILTER (WHERE salary > 150) OVER () FROM staff ORDER BY id;
SELECT id, count(salary) OVER (), rank() OVER (ORDER BY id) FROM staff ORDER BY id;

-- Correct for a caller entitled to every row the window covers.
SELECT id, sum(salary) OVER () FROM staff WHERE owner = 'alice' ORDER BY id;
SELECT id, sum(salary) OVER (ORDER BY salary) FROM staff WHERE owner = 'alice' ORDER BY id;

-- No order leak: unreadable rows rank, number and partition as tied NULLs.
SELECT id, rank() OVER (ORDER BY salary), dense_rank() OVER (ORDER BY salary DESC),
       row_number() OVER (ORDER BY salary, id),
       percent_rank() OVER (ORDER BY salary)
  FROM staff ORDER BY id;
SELECT id, rank() OVER (PARTITION BY dept ORDER BY salary),
       count(*) OVER (PARTITION BY salary),
       count(*) OVER (ORDER BY salary)
  FROM staff ORDER BY id;
SELECT id, lag(salary) OVER (ORDER BY id), first_value(salary) OVER (ORDER BY id DESC),
       ntile(2) OVER (ORDER BY salary, id)
  FROM staff ORDER BY id;

-- A window aggregate over a subquery's output consumes values that are already
-- masked, the same scoping as a plain aggregate over a subquery.
SELECT id, count(s) OVER (), sum(s) OVER ()
  FROM (SELECT id, salary AS s FROM staff) q ORDER BY id;
-- A subquery's own window aggregate is still refused.
SELECT id, (SELECT sum(t.salary) OVER () FROM staff t WHERE t.id = q.id)
  FROM staff q ORDER BY id;

-- Window masking survives plan reuse across callers.
SET plan_cache_mode = force_generic_plan;
PREPARE win AS SELECT id, sum(salary) OVER () FROM staff WHERE owner = 'alice' ORDER BY id;
SET mask.uid = 'alice';
EXECUTE win;
SET mask.uid = 'bob';
EXECUTE win;
DEALLOCATE win;
RESET plan_cache_mode;

RESET ROLE;
REVOKE ALL ON staff FROM mask_app;
DROP FUNCTION can_read_staff__salary(staff);
DROP TABLE staff;
SET ROLE mask_app;
SET mask.uid = 'alice';

-- ============================================================================
-- A whole-row reference becomes the masked row: every masked column in it is
-- replaced by its mask, so the row carries exactly what a column-by-column select
-- would.  This is what a PostgREST computed column passes to its function.
-- ============================================================================

RESET ROLE;
CREATE FUNCTION post_summary(posts) RETURNS text
  LANGUAGE sql STABLE AS $$ SELECT concat($1.title, ' salary=', $1.salary) $$;
SET ROLE mask_app;

SET mask.uid = 'alice';
SELECT to_jsonb(posts) FROM posts ORDER BY id;
SELECT row_to_json(p) FROM posts p ORDER BY p.id;
SELECT p FROM posts p ORDER BY p.id;
SELECT id, post_summary(posts) FROM posts ORDER BY id;
SELECT id, can_read_posts__salary(posts) FROM posts ORDER BY id;

-- No oracle through a whole-row qual or sort.
SELECT id FROM posts WHERE to_jsonb(posts)->>'salary' = '200';
SELECT id FROM posts WHERE post_summary(posts) LIKE '%salary=2%';
SELECT id FROM posts p ORDER BY to_jsonb(p)->>'salary' DESC NULLS LAST, id;

-- The nullable side of an outer join stays a NULL row, not a row of NULLs.
SELECT v.x, to_jsonb(p) FROM (VALUES (1), (9)) v(x)
  LEFT JOIN posts p ON p.id = v.x ORDER BY v.x;

-- Aggregating the row consumes its masked column, so it is refused per unreadable row.
SELECT jsonb_agg(posts) FROM posts;
SELECT jsonb_agg(posts) FROM posts WHERE id = 1;

-- Whole-row masking survives plan reuse across callers.
SET plan_cache_mode = force_generic_plan;
PREPARE wholerow AS SELECT to_jsonb(posts) FROM posts ORDER BY id;
SET mask.uid = 'alice';
EXECUTE wholerow;
SET mask.uid = 'bob';
EXECUTE wholerow;
DEALLOCATE wholerow;
RESET plan_cache_mode;

-- A dropped column keeps its slot in the row type, and a row-independent predicate
-- masks the whole row the same way.
RESET ROLE;
CREATE TABLE wr (id int, gone int, secret text);
INSERT INTO wr VALUES (1, 0, 'classified');
ALTER TABLE wr DROP COLUMN gone;
CREATE FUNCTION can_read_wr() RETURNS boolean LANGUAGE sql STABLE AS
  $$ SELECT current_setting('mask.uid', true) = 'alice' $$;
SECURITY LABEL FOR supatype ON COLUMN wr.secret IS 'MASK READ public.can_read_wr';
GRANT SELECT ON wr TO mask_app;
SET ROLE mask_app;
SET mask.uid = 'alice';
SELECT wr, to_jsonb(wr) FROM wr;
SET mask.uid = 'bob';
SELECT wr, to_jsonb(wr) FROM wr;
RESET ROLE;
SECURITY LABEL FOR supatype ON COLUMN wr.secret IS NULL;
DROP TABLE wr;
DROP FUNCTION can_read_wr(), post_summary(posts);
SET ROLE mask_app;

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
-- INSERT judges a write rule against the row being inserted, as an RLS WITH CHECK
-- does, and refuses a value the rule does not allow
-- ============================================================================

-- A rule that reads the row: alice, holding the write claim, may set the salary of a
-- row she authors, and the value is stored.  Identity-only `notes` is hers to write.
SET mask.uid = 'alice';
SET mask.writer = 'on';
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (3, 'alice new', 555, 'alice wrote this', 'alice');

-- INSERT ... SELECT is the shape PostgREST sends, and a multi-row VALUES judges each
-- row by itself.
BEGIN;
INSERT INTO posts (id, title, salary, author_id)
  SELECT 10, 'alice select', 321, 'alice';
INSERT INTO posts (id, title, salary, author_id)
  VALUES (11, 'alice row a', 11, 'alice'), (12, 'alice row b', 12, 'alice');
RESET ROLE;
SELECT id, salary FROM posts WHERE id >= 10 ORDER BY id;
ROLLBACK;
SET ROLE mask_app;

-- The same writer setting the salary of a row someone else authors: refused, never
-- stored as NULL.
INSERT INTO posts (id, title, salary, author_id) VALUES (13, 'for bob', 1, 'bob');
INSERT INTO posts (id, title, salary, author_id)
  VALUES (14, 'alice ok', 14, 'alice'), (15, 'for bob', 15, 'bob');
RESET mask.writer;

-- Her own row without the write claim: the rule is NULL, not true, so refused too.
INSERT INTO posts (id, title, salary, author_id) VALUES (16, 'no claim', 16, 'alice');

-- A generic plan built for one row judges the next row afresh.
SET plan_cache_mode = force_generic_plan;
PREPARE post_insert(int, numeric, text) AS
  INSERT INTO posts (id, title, salary, author_id) VALUES ($1, 'prepared', $2, $3);
SET mask.writer = 'on';
BEGIN;
EXECUTE post_insert(20, 20, 'alice');
RESET ROLE;
SELECT id, salary FROM posts WHERE id = 20;
ROLLBACK;
SET ROLE mask_app;
EXECUTE post_insert(21, 21, 'bob');
RESET mask.writer;
EXECUTE post_insert(22, 22, 'alice');
DEALLOCATE post_insert;
RESET plan_cache_mode;

-- An identity-only rule bob does not meet: refused.
SET mask.uid = 'bob';
INSERT INTO posts (id, title, notes, author_id) VALUES (4, 'bob new', 'bob wrote this', 'bob');

-- Leaving a column out, naming DEFAULT, or sending NULL is not a write: the column takes
-- its default, so a whole-object POST still works for a caller who may not write it.
INSERT INTO posts (id, title, author_id) VALUES (4, 'bob new', 'bob');
BEGIN;
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (17, 'bob nulls', NULL, NULL, 'bob');
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (18, 'bob defaults', DEFAULT, DEFAULT, 'bob');
INSERT INTO posts (id, title, salary, notes, author_id)
  SELECT 19, 'bob select nulls', NULL, NULL, 'bob';
-- A multi-row VALUES naming DEFAULT puts the default in the VALUES list, not the target list.
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (23, 'bob values a', DEFAULT, DEFAULT, 'bob'), (24, 'bob values b', NULL, NULL, 'bob');
-- PostgREST's bulk POST with `?columns=` and `Prefer: missing=default` (MK4), verbatim but
-- for the table: a key a row leaves out is filled with the column's default as a JSON
-- value before the insert sees it.
INSERT INTO posts (id, title, salary, notes, author_id)
  SELECT b.id, b.title, b.salary, b.notes, b.author_id
    FROM (SELECT '[{"id": 25, "title": "bob bulk", "author_id": "bob"}]'::jsonb AS json_data) p,
         LATERAL (SELECT jsonb_agg(jsonb_build_object('salary', 0, 'notes', 'default notes')
                                   || elem) AS val
                    FROM jsonb_array_elements(p.json_data) elem) d,
         LATERAL (SELECT id, title, salary, notes, author_id
                    FROM jsonb_to_recordset(d.val)
                      AS _(id int, title text, salary numeric, notes text, author_id text)) b;
-- The default's own value, sent explicitly, is no write either (MK6).
INSERT INTO posts (id, title, salary, notes, author_id)
  VALUES (26, 'bob sends defaults', 0, 'default notes', 'bob');
RESET ROLE;
SELECT id, salary, notes FROM posts WHERE id >= 17 ORDER BY id;
ROLLBACK;

RESET ROLE;
SELECT id, salary, notes FROM posts WHERE id IN (3, 4) ORDER BY id;

-- The judged row is built without evaluating a volatile default a second time: a serial
-- key is drawn once per row, so the stored keys have no gaps.
CREATE TABLE ledger (
  id     serial primary key,
  owner  text,
  secret text
);
CREATE FUNCTION can_read_ledger__secret(ledger ledger) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT owner = current_setting('mask.uid', true) FROM (SELECT ($1).*) AS ledger;
  $$;
SECURITY LABEL FOR supatype ON COLUMN ledger.secret
  IS 'MASK READ public.can_read_ledger__secret';
GRANT SELECT, INSERT ON ledger TO mask_app;
GRANT USAGE ON SEQUENCE ledger_id_seq TO mask_app;
SET ROLE mask_app;
SET mask.uid = 'alice';
INSERT INTO ledger (owner, secret) VALUES ('alice', 'one');
INSERT INTO ledger (owner, secret) VALUES ('alice', 'two');
RESET ROLE;
SELECT id, owner, secret FROM ledger ORDER BY id;
SECURITY LABEL FOR supatype ON COLUMN ledger.secret IS NULL;
DROP FUNCTION can_read_ledger__secret(ledger);
DROP TABLE ledger;

-- The rule judges the values stored, defaults included (MK3): a serial key, a random key
-- and an owner filled in by their defaults are what the rules read, so the owner's secret
-- is stored. `owner` carries a rule of its own, and its default is no write by the caller.
CREATE TABLE vault (
  id     serial primary key,
  ref    uuid DEFAULT gen_random_uuid(),
  owner  text DEFAULT current_setting('mask.uid', true),
  secret text
);
CREATE FUNCTION can_read_vault__owner(vault vault) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT owner = current_setting('mask.uid', true) FROM (SELECT ($1).*) AS vault;
  $$;
CREATE FUNCTION can_read_vault__secret(vault vault) RETURNS boolean
  LANGUAGE sql STABLE AS $$
    SELECT owner = current_setting('mask.uid', true) AND ref IS NOT NULL AND id > 0
      FROM (SELECT ($1).*) AS vault;
  $$;
SECURITY LABEL FOR supatype ON COLUMN vault.owner IS 'MASK READ public.can_read_vault__owner';
SECURITY LABEL FOR supatype ON COLUMN vault.secret IS 'MASK READ public.can_read_vault__secret';
GRANT SELECT, INSERT ON vault TO mask_app;
GRANT USAGE ON SEQUENCE vault_id_seq TO mask_app;
SET ROLE mask_app;
SET mask.uid = 'alice';
INSERT INTO vault (secret) VALUES ('one');
INSERT INTO vault (secret) VALUES ('two'), ('three');
INSERT INTO vault (secret) SELECT 'four';
-- Bob may not write a row as alice's: refused on the owner column itself.
SET mask.uid = 'bob';
INSERT INTO vault (owner, secret) VALUES ('alice', 'planted');
RESET ROLE;
SELECT id, ref IS NOT NULL AS has_ref, owner, secret FROM vault ORDER BY id;
SECURITY LABEL FOR supatype ON COLUMN vault.owner IS NULL;
SECURITY LABEL FOR supatype ON COLUMN vault.secret IS NULL;
DROP FUNCTION can_read_vault__owner(vault), can_read_vault__secret(vault);
DROP TABLE vault;
SET ROLE mask_app;

-- ============================================================================
-- Constructs that fail closed rather than guess
-- ============================================================================

SET mask.uid = 'alice';

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
