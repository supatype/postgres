DO $$
BEGIN
    IF EXISTS (
        SELECT FROM pg_catalog.pg_namespace n
        WHERE n.nspname = 'topic'
          AND (EXISTS (SELECT FROM pg_catalog.aclexplode(n.nspacl) a
                       WHERE a.grantee <> n.nspowner AND NOT (a.grantee = 0 AND a.privilege_type = 'USAGE'))
               OR NOT EXISTS (SELECT FROM pg_catalog.pg_roles r WHERE r.oid = n.nspowner AND r.rolsuper))
    ) THEN
        RAISE EXCEPTION 'pg_topics: schema topic already exists. A non-superuser owns it, or a grant exists on it.'
            USING HINT = 'Drop schema topic, or give it to a superuser with no grants. Then install pg_topics again.';
    END IF;
END
$$;

CREATE TABLE topic.topic_config (
    schema_name        text     NOT NULL,
    topic              text     NOT NULL,
    band_count         smallint NOT NULL DEFAULT 4 CHECK (band_count BETWEEN 1 AND 1024),
    retention_interval interval NOT NULL,
    min_durability     text     NOT NULL DEFAULT 'durable'
        CHECK (min_durability IN ('relaxed', 'durable', 'replicated')),
    max_backlog_age    interval NOT NULL DEFAULT '60 seconds',
    offset_retention   interval,
    partition_interval interval NOT NULL DEFAULT '1 day'
        CHECK (partition_interval >= interval '1 minute'),
    detaching          regclass,
    detaching_name     text,
    detaching_bound    text,
    retention_hold_until timestamptz,
    sync_table         regclass,
    sync_key           text,
    sync_enabled       boolean  NOT NULL DEFAULT false,
    backlog_age        interval NOT NULL DEFAULT '0',
    stamped_at         timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, topic),
    CHECK ((sync_table IS NULL) = (sync_key IS NULL)),
    CHECK (NOT sync_enabled OR sync_table IS NOT NULL),
    CHECK (retention_interval > interval '0'),
    CHECK (max_backlog_age >= interval '2 seconds')
) WITH (fillfactor = 50);

CREATE TABLE topic.topic_band_position (
    schema_name   text     NOT NULL,
    topic         text     NOT NULL,
    band          smallint NOT NULL,
    next_offset   bigint   NOT NULL DEFAULT 0,
    oldest_offset bigint   NOT NULL DEFAULT 0,
    stamped_by    text,
    PRIMARY KEY (schema_name, topic, band),
    FOREIGN KEY (schema_name, topic)
        REFERENCES topic.topic_config (schema_name, topic) ON DELETE CASCADE
) WITH (fillfactor = 50);

CREATE TABLE topic.topic_groups (
    group_name       text    PRIMARY KEY,
    owner_role       name    NOT NULL,
    generation_id    integer NOT NULL DEFAULT 0,
    leader_member_id text,
    protocol_type    text,
    protocol_name    text,
    state            text    NOT NULL DEFAULT 'Empty'
        CHECK (state IN ('Empty', 'PreparingRebalance', 'CompletingRebalance', 'Stable', 'Dead')),
    expired_members  bigint  NOT NULL DEFAULT 0,
    rebalance_started_at timestamptz,
    pending_members  jsonb   NOT NULL DEFAULT '{}',
    updated_at       timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE topic.topic_group_members (
    group_name         text        NOT NULL REFERENCES topic.topic_groups (group_name) ON DELETE CASCADE,
    member_id          text        NOT NULL,
    owner_role         name        NOT NULL,
    client_id          text,
    session_timeout_ms integer     NOT NULL,
    rebalance_ms       integer     NOT NULL,
    protocols          jsonb,
    assignment         jsonb,
    last_heartbeat_at  timestamptz NOT NULL DEFAULT now(),
    joined_generation  integer,
    PRIMARY KEY (group_name, member_id)
);

CREATE INDEX ON topic.topic_group_members (last_heartbeat_at);

CREATE TABLE topic.topic_offsets (
    schema_name      text     NOT NULL,
    topic            text     NOT NULL,
    group_name       text     NOT NULL REFERENCES topic.topic_groups (group_name) ON DELETE CASCADE,
    band             smallint NOT NULL,
    owner_role       name     NOT NULL,
    committed_offset bigint   NOT NULL DEFAULT -1,
    generation_id    integer  NOT NULL DEFAULT 0,
    PRIMARY KEY (schema_name, topic, group_name, band),
    FOREIGN KEY (schema_name, topic, band)
        REFERENCES topic.topic_band_position (schema_name, topic, band) ON DELETE CASCADE
);

CREATE TABLE topic.topic_producers (
    schema_name    text        NOT NULL,
    topic          text        NOT NULL,
    producer_id    bigint      NOT NULL,
    producer_epoch smallint    NOT NULL,
    band           smallint    NOT NULL,
    slot           smallint    NOT NULL CHECK (slot BETWEEN 0 AND 4),
    first_sequence integer     NOT NULL,
    last_sequence  integer     NOT NULL,
    base_published_at timestamptz NOT NULL,
    base_seq       bigint      NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (schema_name, topic, producer_id, band, slot),
    FOREIGN KEY (schema_name, topic, band)
        REFERENCES topic.topic_band_position (schema_name, topic, band) ON DELETE CASCADE
);

CREATE TABLE topic.producer_ids (
    producer_id bigint      PRIMARY KEY,
    owner_role   name        NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz NOT NULL DEFAULT now()
);

ALTER TABLE topic.topic_groups ENABLE ROW LEVEL SECURITY;
ALTER TABLE topic.topic_group_members ENABLE ROW LEVEL SECURITY;
ALTER TABLE topic.topic_offsets ENABLE ROW LEVEL SECURITY;
CREATE POLICY owner_reads ON topic.topic_groups FOR SELECT
    USING (coalesce(pg_catalog.pg_has_role(current_user, pg_catalog.to_regrole(pg_catalog.quote_ident(owner_role)), 'USAGE'), false));
CREATE POLICY owner_reads ON topic.topic_group_members FOR SELECT
    USING (coalesce(pg_catalog.pg_has_role(current_user, pg_catalog.to_regrole(pg_catalog.quote_ident(owner_role)), 'USAGE'), false));
CREATE POLICY owner_reads ON topic.topic_offsets FOR SELECT
    USING (coalesce(pg_catalog.pg_has_role(current_user, pg_catalog.to_regrole(pg_catalog.quote_ident(owner_role)), 'USAGE'), false));

CREATE FUNCTION topic.band_count(schema_name text, topic text) RETURNS smallint
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    SELECT c.band_count FROM topic.topic_config c WHERE c.schema_name = $1 AND c.topic = $2
$$;

CREATE FUNCTION topic.refuse_forged_insert() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    RAISE EXCEPTION 'topic: an insert into %.% must not set log_offset, published_by or xact, or a published_at later than the current time',
        TG_TABLE_SCHEMA, TG_TABLE_NAME;
END
$$;

CREATE FUNCTION topic.raise_synchronous_commit(min_durability text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    levels text[] := ARRAY['off', 'local', 'remote_write', 'on', 'remote_apply'];
    wanted text := CASE min_durability WHEN 'relaxed' THEN 'off' WHEN 'durable' THEN 'on' ELSE 'remote_apply' END;
BEGIN
    IF array_position(levels, wanted) > array_position(levels, current_setting('synchronous_commit')) THEN
        PERFORM set_config('synchronous_commit', wanted, true);
    END IF;
END
$$;

CREATE FUNCTION topic.publish_floor() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    c topic.topic_config;
BEGIN
    SELECT * INTO c FROM topic.topic_config t
    WHERE t.schema_name = TG_TABLE_SCHEMA AND t.topic = TG_TABLE_NAME;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic: %.% has no topic_config row', TG_TABLE_SCHEMA, TG_TABLE_NAME;
    END IF;
    IF c.backlog_age > c.max_backlog_age THEN
        RAISE EXCEPTION 'topic: %.% backlog_age % is above max_backlog_age %',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, c.backlog_age, c.max_backlog_age;
    END IF;
    IF clock_timestamp() - c.stamped_at > c.max_backlog_age THEN
        RAISE EXCEPTION 'topic: the stamper has not run on %.% since %, which is longer than max_backlog_age %',
            TG_TABLE_SCHEMA, TG_TABLE_NAME, c.stamped_at, c.max_backlog_age;
    END IF;
    PERFORM topic.raise_synchronous_commit(c.min_durability);
    RETURN NULL;
END
$$;

CREATE FUNCTION topic.ensure_partitions(schema_name text, topic text, ahead int) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp SET DateStyle = ISO SET TimeZone = UTC
AS $$
DECLARE
    c topic.topic_config;
    parent regclass;
    owner_name name;
    lo timestamptz;
    p text;
    existing regclass;
BEGIN
    SELECT * INTO STRICT c FROM topic.topic_config t
    WHERE t.schema_name = ensure_partitions.schema_name AND t.topic = ensure_partitions.topic;
    parent := format('%I.%I', c.schema_name, c.topic)::regclass;
    SELECT r.rolname INTO owner_name FROM pg_class k JOIN pg_roles r ON r.oid = k.relowner
    WHERE k.oid = parent AND k.relkind = 'p';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic.ensure_partitions: % is not a partitioned table', parent;
    END IF;
    FOR i IN 0..ahead LOOP
        lo := date_bin(c.partition_interval, now(), timestamptz '2000-01-01 00:00:00+00') + i * c.partition_interval;
        p := c.topic || '_p' || to_char(lo, 'YYYYMMDDHH24MISS');
        existing := to_regclass(format('%I.%I', c.schema_name, p));
        IF existing IS NOT NULL THEN
            IF NOT EXISTS (SELECT FROM pg_inherits h WHERE h.inhrelid = existing AND h.inhparent = parent) THEN
                RAISE WARNING 'topic: % exists and is not a partition of %, so % gets no partition from %',
                    existing, parent, parent, lo;
            END IF;
            CONTINUE;
        END IF;
        EXECUTE format('CREATE TABLE %I.%I (LIKE %s INCLUDING DEFAULTS INCLUDING CONSTRAINTS)', c.schema_name, p, parent);
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', c.schema_name, p, owner_name);
        EXECUTE format('CREATE UNIQUE INDEX ON %I.%I (band, log_offset) WHERE log_offset IS NOT NULL', c.schema_name, p);
        EXECUTE format('ALTER TABLE %s ATTACH PARTITION %I.%I FOR VALUES FROM (%L) TO (%L)',
            parent, c.schema_name, p, lo, lo + c.partition_interval);
    END LOOP;
END
$$;

CREATE FUNCTION topic.sync_copies(standby_names text) RETURNS int
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp
AS $$
    SELECT CASE WHEN btrim(standby_names) = '' THEN 1
        ELSE 1 + coalesce((regexp_match(standby_names, '^\s*(?:(?:any|first)\s+)?(\d{1,9})\s*\(', 'i'))[1]::int, 1) END
$$;

CREATE FUNCTION topic.create_topic(
    topic text,
    band_count int DEFAULT 4,
    retention interval DEFAULT '7 days',
    min_durability text DEFAULT 'durable',
    partition_interval interval DEFAULT '1 day'
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET DateStyle = ISO SET TimeZone = UTC
AS $$
DECLARE
    s text := split_part(create_topic.topic, '.', 1);
    t text := substr(create_topic.topic, length(s) + 2);
    owner_name text := topic.caller();
BEGIN
    IF s !~ '^[A-Za-z0-9_-]{1,63}$' OR t !~ '^[A-Za-z0-9_-]{1,47}$' OR right(t, 2) <> '_q' THEN
        RAISE EXCEPTION 'topic.create_topic: % is not a valid topic name', create_topic.topic
            USING HINT = 'Use schema.table. The table ends in _q and has at most 47 characters from A-Z, a-z, 0-9, _ and -.';
    END IF;
    IF NOT has_schema_privilege(owner_name, s, 'CREATE') THEN
        RAISE EXCEPTION 'topic.create_topic: role % has no CREATE privilege on schema %', owner_name, s
            USING ERRCODE = '42501';
    END IF;
    IF current_setting('pg_topics.databases') <> ''
       AND NOT EXISTS (SELECT FROM unnest(string_to_array(current_setting('pg_topics.databases'), ',')) d(name)
                       WHERE btrim(d.name) = current_database()) THEN
        RAISE EXCEPTION 'topic.create_topic: database % is not in pg_topics.databases. No worker ever stamps its topics.',
            current_database()
            USING HINT = 'Add the database to pg_topics.databases and restart PostgreSQL.';
    END IF;
    IF min_durability IN ('durable', 'replicated') AND NOT current_setting('pg_topics.failover_is_fenced')::bool THEN
        RAISE EXCEPTION 'topic.create_topic: min_durability % needs pg_topics.failover_is_fenced = on', min_durability;
    END IF;
    IF min_durability = 'replicated' AND current_setting('synchronous_standby_names') = '' THEN
        RAISE EXCEPTION 'topic.create_topic: min_durability replicated needs synchronous_standby_names';
    END IF;

    INSERT INTO topic.topic_config (schema_name, topic, band_count, retention_interval, min_durability, partition_interval)
    VALUES (s, t, band_count, retention, min_durability, partition_interval);
    INSERT INTO topic.topic_band_position (schema_name, topic, band)
    SELECT s, t, b FROM generate_series(0, band_count - 1) b;

    EXECUTE format(
        'CREATE TABLE %I.%I (
            seq                bigint      NOT NULL GENERATED ALWAYS AS IDENTITY,
            log_offset         bigint,
            band               smallint    NOT NULL CHECK (band BETWEEN 0 AND %s),
            key                varchar(40),
            value              jsonb,
            headers            jsonb,
            published_by       name        NOT NULL DEFAULT current_user,
            published_at       timestamptz NOT NULL DEFAULT clock_timestamp(),
            producer_timestamp timestamptz,
            xact               xid8        NOT NULL DEFAULT pg_current_xact_id(),
            PRIMARY KEY (published_at, seq)
        ) PARTITION BY RANGE (published_at)', s, t, band_count - 1);
    EXECUTE format('CREATE INDEX ON %I.%I (xact, seq) WHERE log_offset IS NULL', s, t);
    EXECUTE format('CREATE INDEX ON %I.%I USING brin (log_offset) WHERE log_offset IS NOT NULL', s, t);
    EXECUTE format(
        'CREATE TRIGGER topic_refuse_forged BEFORE INSERT ON %I.%I FOR EACH ROW
         WHEN (NEW.log_offset IS NOT NULL OR NEW.published_by <> current_user OR NEW.xact <> pg_current_xact_id()
               OR NEW.published_at > clock_timestamp() + interval ''1 second'')
         EXECUTE FUNCTION topic.refuse_forged_insert()', s, t);
    EXECUTE format(
        'CREATE TRIGGER topic_publish_floor BEFORE INSERT ON %I.%I FOR EACH STATEMENT
         EXECUTE FUNCTION topic.publish_floor()', s, t);
    EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, t, owner_name);
    PERFORM topic.ensure_partitions(s, t, 1);
END
$$;

CREATE FUNCTION topic.publish(topic text, value jsonb, key text DEFAULT NULL, headers jsonb DEFAULT NULL)
RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(publish.topic, '.', 1);
    t text := substr(publish.topic, length(s) + 2);
    n int := topic.band_count(s, t);
    rr bigint;
    b int;
BEGIN
    IF n IS NULL THEN
        RAISE EXCEPTION 'topic.publish: topic % does not exist', publish.topic;
    END IF;
    IF key IS NULL THEN
        rr := coalesce(nullif(current_setting('pg_topics.round_robin', true), ''), '0')::bigint;
        PERFORM set_config('pg_topics.round_robin', (rr + 1)::text, false);
        b := rr % n;
    ELSE
        b := topic.band_for(key, n);
    END IF;
    EXECUTE format('INSERT INTO %I.%I (band, key, value, headers) VALUES ($1, $2, $3, $4)', s, t)
        USING b, key, value, headers;
END
$$;

CREATE FUNCTION topic.retention_next(schema_name text, topic text) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp SET DateStyle = ISO SET TimeZone = UTC
AS $$
DECLARE
    c topic.topic_config;
    parent regclass;
    pending boolean;
BEGIN
    SELECT * INTO STRICT c FROM topic.topic_config t
    WHERE t.schema_name = retention_next.schema_name AND t.topic = retention_next.topic;
    IF clock_timestamp() < c.retention_hold_until THEN
        RETURN NULL;
    END IF;
    parent := format('%I.%I', c.schema_name, c.topic)::regclass;
    IF c.detaching IS NOT NULL AND NOT EXISTS (SELECT FROM pg_class k WHERE k.oid = c.detaching) THEN
        c.detaching := NULL;
        UPDATE topic.topic_config t SET detaching = NULL
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
    END IF;
    IF c.detaching IS NULL THEN
        SELECT h.inhrelid, pg_get_expr(k.relpartbound, k.oid) INTO c.detaching, c.detaching_bound
        FROM pg_inherits h JOIN pg_class k ON k.oid = h.inhrelid
        CROSS JOIN LATERAL substring(pg_get_expr(k.relpartbound, k.oid) FROM ' TO \(''([^'']+)''\)$') b(upper)
        WHERE h.inhparent = parent AND NOT h.inhdetachpending
          AND b.upper::timestamptz < now() - c.retention_interval
        ORDER BY b.upper::timestamptz LIMIT 1;
        IF c.detaching IS NULL THEN
            RETURN NULL;
        END IF;
        UPDATE topic.topic_config t
        SET detaching = c.detaching, detaching_name = c.detaching::text, detaching_bound = c.detaching_bound
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
        c.detaching_name := c.detaching::text;
    END IF;

    SELECT h.inhdetachpending INTO pending FROM pg_inherits h WHERE h.inhrelid = c.detaching AND h.inhparent = parent;
    IF FOUND THEN
        RAISE LOG 'topic: retention detaches % from %', c.detaching, parent;
        RETURN format('ALTER TABLE %s DETACH PARTITION %s %s', parent, c.detaching,
                      CASE WHEN pending THEN 'FINALIZE' ELSE 'CONCURRENTLY' END);
    END IF;
    IF c.detaching::text IS DISTINCT FROM c.detaching_name
       OR EXISTS (SELECT FROM pg_inherits h WHERE h.inhrelid = c.detaching) THEN
        UPDATE topic.topic_config t SET detaching = NULL
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
        RAISE WARNING 'topic: % was renamed or attached to another table after retention detached it from %, so retention leaves it',
            c.detaching, parent;
        RETURN NULL;
    END IF;

    EXECUTE format('LOCK TABLE %s IN ACCESS EXCLUSIVE MODE', c.detaching);
    IF topic.retention_check(c.detaching) > 0 THEN
        EXECUTE format('ALTER TABLE %s ATTACH PARTITION %s %s', parent, c.detaching, c.detaching_bound);
        UPDATE topic.topic_config t SET detaching = NULL, retention_hold_until = clock_timestamp() + c.retention_interval
        WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
        RAISE WARNING 'topic: % has rows with no log_offset, so retention attached it again to %', c.detaching, parent;
        RETURN NULL;
    END IF;
    UPDATE topic.topic_band_position p SET oldest_offset = f.floor
    FROM topic.retention_floor(c.schema_name, c.topic) f
    WHERE p.schema_name = c.schema_name AND p.topic = c.topic AND p.band = f.band;
    RAISE LOG 'topic: retention drops %', c.detaching;
    EXECUTE format('DROP TABLE %s', c.detaching);
    UPDATE topic.topic_config t SET detaching = NULL
    WHERE t.schema_name = c.schema_name AND t.topic = c.topic;
    RETURN NULL;
END
$$;

CREATE FUNCTION topic.reap() RETURNS void
LANGUAGE sql SET search_path = pg_catalog, pg_temp
AS $$
    DELETE FROM topic.producer_ids i
    WHERE i.last_used_at < now() - interval '7 days'
      AND NOT EXISTS (SELECT FROM topic.topic_producers p
                      WHERE p.producer_id = i.producer_id AND p.updated_at >= now() - interval '1 day');
    DELETE FROM topic.topic_producers WHERE updated_at < now() - interval '1 day';
    DELETE FROM topic.topic_groups g
    WHERE g.state = 'Empty'
      AND NOT starts_with(g.group_name, '__pg_topics_sync:')
      AND NOT EXISTS (SELECT FROM topic.topic_group_members m WHERE m.group_name = g.group_name)
      AND g.updated_at < now() - (
          SELECT max(c.offset_retention)
          FROM topic.topic_offsets o
          JOIN topic.topic_config c ON c.schema_name = o.schema_name AND c.topic = o.topic
          WHERE o.group_name = g.group_name
          HAVING bool_and(c.offset_retention IS NOT NULL));
    DELETE FROM topic.topic_groups g
    WHERE g.state = 'Empty'
      AND NOT starts_with(g.group_name, '__pg_topics_sync:')
      AND NOT EXISTS (SELECT FROM topic.topic_group_members m WHERE m.group_name = g.group_name)
      AND NOT EXISTS (SELECT FROM topic.topic_offsets o WHERE o.group_name = g.group_name)
      AND g.updated_at < now() - interval '1 day';
$$;

CREATE FUNCTION topic.sync_base_owner(base regclass, sync_key text) RETURNS name
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    who text := topic.caller();
    base_owner name;
BEGIN
    SELECT r.rolname INTO base_owner FROM pg_class k JOIN pg_roles r ON r.oid = k.relowner
    WHERE k.oid = sync_base_owner.base AND k.relkind IN ('r', 'p');
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic: % is not an ordinary table', base;
    END IF;
    IF NOT pg_has_role(who, base_owner, 'USAGE') THEN
        RAISE EXCEPTION 'topic: role % is not a member of %, the owner of %', who, base_owner, base
            USING ERRCODE = '42501';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_attribute a WHERE a.attrelid = base AND a.attname = sync_base_owner.sync_key
                   AND a.attnum > 0 AND NOT a.attisdropped) THEN
        RAISE EXCEPTION 'topic: % has no column %', base, sync_base_owner.sync_key;
    END IF;
    IF NOT EXISTS (SELECT FROM pg_attribute a WHERE a.attrelid = base AND a.attname = sync_base_owner.sync_key
                   AND a.attnotnull) THEN
        RAISE EXCEPTION 'topic: column % of % allows NULL', sync_base_owner.sync_key, base
            USING HINT = 'The sync key column must be NOT NULL.';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_attribute a WHERE a.attrelid = base AND a.attname = 'event_at'
                   AND a.atttypid = 'timestamptz'::regtype AND a.attnotnull AND NOT a.attisdropped) THEN
        RAISE EXCEPTION 'topic: % has no column event_at timestamptz NOT NULL', base;
    END IF;
    RETURN base_owner;
END
$$;

CREATE FUNCTION topic.attach(
    base regclass,
    sync_key text,
    band_count int DEFAULT 4,
    retention interval DEFAULT '7 days',
    min_durability text DEFAULT 'durable'
) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    who text := topic.caller();
    s text;
    t text;
    base_owner name;
    queue_owner name;
    grp text;
    errors regclass;
    errors_kind "char";
    errors_owner name;
    errors_shape text[];
BEGIN
    base_owner := topic.sync_base_owner(base, attach.sync_key);
    SELECT n.nspname, k.relname || '_q' INTO s, t
    FROM pg_class k JOIN pg_namespace n ON n.oid = k.relnamespace WHERE k.oid = attach.base;
    IF NOT has_schema_privilege(who, s, 'CREATE') THEN
        RAISE EXCEPTION 'topic.attach: role % has no CREATE privilege on schema %', who, s USING ERRCODE = '42501';
    END IF;
    IF NOT EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = s AND c.topic = t) THEN
        PERFORM topic.create_topic(s || '.' || t, band_count, retention, min_durability);
    END IF;
    SELECT r.rolname INTO queue_owner FROM pg_class k JOIN pg_roles r ON r.oid = k.relowner
    WHERE k.oid = format('%I.%I', s, t)::regclass;
    IF NOT pg_has_role(who, queue_owner, 'USAGE') THEN
        RAISE EXCEPTION 'topic.attach: role % is not a member of %, the owner of %.%', who, queue_owner, s, t
            USING ERRCODE = '42501';
    END IF;

    errors := to_regclass(format('%I.%I', s, t || 'e'));
    IF errors IS NOT NULL THEN
        SELECT k.relkind, r.rolname INTO errors_kind, errors_owner
        FROM pg_class k JOIN pg_roles r ON r.oid = k.relowner WHERE k.oid = errors;
        SELECT array_agg(a.attname || ' ' || format_type(a.atttypid, a.atttypmod)
                          || CASE WHEN a.attnotnull THEN ' NOT NULL' ELSE '' END ORDER BY a.attnum)
        INTO errors_shape
        FROM pg_attribute a WHERE a.attrelid = errors AND a.attnum > 0 AND NOT a.attisdropped;
        IF errors_kind IS DISTINCT FROM 'r' OR errors_owner IS DISTINCT FROM base_owner
           OR errors_shape IS DISTINCT FROM ARRAY[
               'band smallint NOT NULL', 'log_offset bigint NOT NULL', 'seq bigint NOT NULL',
               'key character varying(40)', 'value jsonb', 'headers jsonb',
               'published_by name NOT NULL', 'published_at timestamp with time zone NOT NULL',
               'producer_timestamp timestamp with time zone', 'failed_at timestamp with time zone NOT NULL',
               'error text NOT NULL']
           OR NOT EXISTS (SELECT FROM pg_constraint k WHERE k.conrelid = errors AND k.contype = 'p'
                          AND pg_get_constraintdef(k.oid) = 'PRIMARY KEY (band, log_offset)') THEN
            RAISE EXCEPTION 'topic.attach: table %.% exists. Its shape or its owner does not match the error table pg_topics expects.',
                s, t || 'e'
                USING HINT = 'Drop it, or make its columns, primary key and owner match, then attach again.';
        END IF;
    ELSE
        EXECUTE format(
            'CREATE TABLE %I.%I (
                band               smallint    NOT NULL,
                log_offset         bigint      NOT NULL,
                seq                bigint      NOT NULL,
                key                varchar(40),
                value              jsonb,
                headers            jsonb,
                published_by       name        NOT NULL,
                published_at       timestamptz NOT NULL,
                producer_timestamp timestamptz,
                failed_at          timestamptz NOT NULL DEFAULT now(),
                error              text        NOT NULL,
                PRIMARY KEY (band, log_offset)
            )', s, t || 'e');
        EXECUTE format('CREATE INDEX ON %I.%I (failed_at)', s, t || 'e');
        EXECUTE format('ALTER TABLE %I.%I OWNER TO %I', s, t || 'e', base_owner);
    END IF;

    grp := '__pg_topics_sync:' || s || '.' || t;
    UPDATE topic.topic_config c SET sync_table = base, sync_key = attach.sync_key, sync_enabled = true
    WHERE c.schema_name = s AND c.topic = t;
    INSERT INTO topic.topic_groups (group_name, owner_role) VALUES (grp, base_owner);
    INSERT INTO topic.topic_offsets (schema_name, topic, group_name, band, owner_role, committed_offset)
    SELECT p.schema_name, p.topic, grp, p.band, base_owner, p.oldest_offset
    FROM topic.topic_band_position p WHERE p.schema_name = s AND p.topic = t;
    RETURN s || '.' || t;
END
$$;

CREATE FUNCTION topic.create_table_topic(
    table_name text,
    columns json,
    sync_key text,
    band_count int DEFAULT 4,
    retention interval DEFAULT '7 days',
    min_durability text DEFAULT 'durable'
) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(table_name, '.', 1);
    t text := substr(table_name, length(s) + 2);
    defs text := '';
    c record;
BEGIN
    IF json_typeof(columns) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'topic.create_table_topic: columns must be a JSON object of "name": "type"';
    END IF;
    FOR c IN SELECT * FROM json_each(columns) LOOP
        IF json_typeof(c.value) <> 'string' OR to_regtype(c.value #>> '{}') IS NULL THEN
            RAISE EXCEPTION 'topic.create_table_topic: % is not a type', c.value;
        END IF;
        defs := defs || format('%I %s, ', c.key,
            format_type(to_regtype(c.value #>> '{}'), to_regtypemod(c.value #>> '{}')));
    END LOOP;
    EXECUTE format('CREATE TABLE %I.%I (%s event_at timestamptz NOT NULL, PRIMARY KEY (%I))', s, t, defs, sync_key);
    RETURN topic.attach(format('%I.%I', s, t)::regclass, sync_key, band_count, retention, min_durability);
END
$$;

CREATE FUNCTION topic.owned_topic(topic text, OUT schema_name text, OUT topic_name text)
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    who text := topic.caller();
    owner_name name;
BEGIN
    schema_name := split_part(owned_topic.topic, '.', 1);
    topic_name := substr(owned_topic.topic, length(schema_name) + 2);
    SELECT pg_get_userbyid(k.relowner) INTO owner_name
    FROM topic.topic_config c
    JOIN pg_namespace n ON n.nspname = c.schema_name
    JOIN pg_class k ON k.relnamespace = n.oid AND k.relname = c.topic
    WHERE c.schema_name = owned_topic.schema_name AND c.topic = topic_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic: topic % does not exist', owned_topic.topic USING ERRCODE = '42P01';
    END IF;
    IF NOT pg_has_role(who, owner_name, 'USAGE') THEN
        RAISE EXCEPTION 'topic: role % is not a member of %, the owner of topic %', who, owner_name, owned_topic.topic
            USING ERRCODE = '42501';
    END IF;
END
$$;

CREATE FUNCTION topic.drop_topic(topic text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp SET lock_timeout = '2s'
AS $$
DECLARE
    q record;
BEGIN
    SELECT * INTO q FROM topic.owned_topic(drop_topic.topic);
    FOR attempt IN 1..3 LOOP
        BEGIN
            EXECUTE format('DROP TABLE %I.%I', q.schema_name, q.topic_name);
            EXIT;
        EXCEPTION WHEN lock_not_available THEN
            IF attempt = 3 THEN
                RAISE;
            END IF;
        END;
    END LOOP;
    IF EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = q.schema_name AND c.topic = q.topic_name) THEN
        RAISE EXCEPTION 'topic.drop_topic: the control rows of topic % remain after the drop', drop_topic.topic;
    END IF;
END
$$;

CREATE FUNCTION topic.set_retention(topic text, retention interval) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    UPDATE topic.topic_config c SET retention_interval = $2
    FROM topic.owned_topic($1) q WHERE c.schema_name = q.schema_name AND c.topic = q.topic_name
$$;

CREATE FUNCTION topic.set_backlog_limit(topic text, max_backlog_age interval) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    UPDATE topic.topic_config c SET max_backlog_age = $2
    FROM topic.owned_topic($1) q WHERE c.schema_name = q.schema_name AND c.topic = q.topic_name
$$;

CREATE FUNCTION topic.set_sync_enabled(topic text, enabled boolean) RETURNS void
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    UPDATE topic.topic_config c SET sync_enabled = $2
    FROM topic.owned_topic($1) q WHERE c.schema_name = q.schema_name AND c.topic = q.topic_name
$$;

CREATE FUNCTION topic.set_durability(topic text, tier text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
BEGIN
    SELECT * INTO q FROM topic.owned_topic(set_durability.topic);
    IF tier IN ('durable', 'replicated') AND NOT current_setting('pg_topics.failover_is_fenced')::bool THEN
        RAISE EXCEPTION 'topic.set_durability: min_durability % needs pg_topics.failover_is_fenced = on', tier;
    END IF;
    IF tier = 'replicated' AND current_setting('synchronous_standby_names') = '' THEN
        RAISE EXCEPTION 'topic.set_durability: min_durability replicated needs synchronous_standby_names';
    END IF;
    UPDATE topic.topic_config c SET min_durability = tier
    WHERE c.schema_name = q.schema_name AND c.topic = q.topic_name;
END
$$;

CREATE FUNCTION topic.set_sync(topic text, base regclass, sync_key text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
    sync_owner name;
BEGIN
    SELECT * INTO q FROM topic.owned_topic(set_sync.topic);
    SELECT g.owner_role INTO sync_owner FROM topic.topic_groups g
    WHERE g.group_name = '__pg_topics_sync:' || q.schema_name || '.' || q.topic_name;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic.set_sync: topic % has no sync', set_sync.topic USING HINT = 'Use topic.attach first.';
    END IF;
    IF topic.sync_base_owner(base, set_sync.sync_key) <> sync_owner THEN
        RAISE EXCEPTION 'topic.set_sync: the owner of % is not %, the owner of the sync of topic %',
            base, sync_owner, set_sync.topic;
    END IF;
    UPDATE topic.topic_config c SET sync_table = base, sync_key = set_sync.sync_key
    WHERE c.schema_name = q.schema_name AND c.topic = q.topic_name;
END
$$;

CREATE FUNCTION topic.grant_publish(topic text, role name) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
BEGIN
    SELECT * INTO q FROM topic.owned_topic(grant_publish.topic);
    IF role = 'public' THEN
        RAISE EXCEPTION 'topic.grant_publish: the role name public means every role, so grant_publish refuses it'
            USING ERRCODE = '22023', HINT = 'To give it to every role on purpose, run GRANT INSERT (band, key, value, headers, producer_timestamp) ON the queue table TO PUBLIC.';
    END IF;
    EXECUTE format('GRANT INSERT (band, key, value, headers, producer_timestamp) ON %I.%I TO %I',
        q.schema_name, q.topic_name, role);
END
$$;

CREATE FUNCTION topic.grant_consume(topic text, role name) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
BEGIN
    SELECT * INTO q FROM topic.owned_topic(grant_consume.topic);
    IF role = 'public' THEN
        RAISE EXCEPTION 'topic.grant_consume: the role name public means every role, so grant_consume refuses it'
            USING ERRCODE = '22023', HINT = 'To give it to every role on purpose, run GRANT SELECT ON the queue table TO PUBLIC.';
    END IF;
    EXECUTE format('GRANT SELECT ON %I.%I TO %I', q.schema_name, q.topic_name, role);
END
$$;

CREATE FUNCTION topic.band_offsets(topic text) RETURNS TABLE (band smallint, oldest_offset bigint, next_offset bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(band_offsets.topic, '.', 1);
    t text := substr(band_offsets.topic, length(s) + 2);
BEGIN
    IF NOT EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = s AND c.topic = t) THEN
        RAISE EXCEPTION 'topic.band_offsets: role % may not read topic %', topic.caller(), band_offsets.topic
            USING ERRCODE = '42501';
    ELSIF NOT has_table_privilege(topic.caller(), format('%I.%I', s, t), 'SELECT') THEN
        RAISE EXCEPTION 'topic.band_offsets: role % may not read topic %', topic.caller(), band_offsets.topic
            USING ERRCODE = '42501';
    END IF;
    RETURN QUERY SELECT p.band, p.oldest_offset, p.next_offset FROM topic.topic_band_position p
    WHERE p.schema_name = s AND p.topic = t ORDER BY p.band;
END
$$;

CREATE FUNCTION topic.kafka_topics(names text[]) RETURNS TABLE (topic text, band_count smallint, visible boolean)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    SELECT c.schema_name || '.' || c.topic, c.band_count,
           has_table_privilege(topic.caller(), r.oid, 'SELECT')
           OR has_any_column_privilege(topic.caller(), r.oid, 'INSERT')
    FROM topic.topic_config c
    JOIN pg_namespace n ON n.nspname = c.schema_name
    JOIN pg_class r ON r.relnamespace = n.oid AND r.relname = c.topic
    WHERE $1 IS NULL OR c.schema_name || '.' || c.topic = ANY ($1)
    ORDER BY 1
$$;

CREATE SEQUENCE topic.producer_id_seq;

CREATE FUNCTION topic.init_producer_id() RETURNS bigint
LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    INSERT INTO topic.producer_ids (producer_id, owner_role) VALUES (nextval('topic.producer_id_seq'), topic.caller())
    RETURNING producer_id
$$;

CREATE FUNCTION topic.produce_check(schema_name text, topic text, producer_id bigint, epoch smallint,
                                    band smallint, first_seq int, last_seq int,
                                    row_published_at timestamptz, row_seq bigint,
                                    OUT duplicate boolean, OUT base_published_at timestamptz, OUT base_seq bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    queue regclass := to_regclass(format('%I.%I', produce_check.schema_name, produce_check.topic));
    owner name;
    used timestamptz;
    ring topic.topic_producers[];
    ring_epoch smallint;
    newest smallint;
BEGIN
    IF queue IS NULL
       OR NOT EXISTS (SELECT FROM topic.topic_config c
                      WHERE c.schema_name = produce_check.schema_name AND c.topic = produce_check.topic)
       OR NOT has_any_column_privilege(topic.caller(), queue, 'INSERT') THEN
        RAISE EXCEPTION 'topic.produce_check: role % may not write topic %.%',
            topic.caller(), produce_check.schema_name, produce_check.topic USING ERRCODE = '42501';
    END IF;
    -- A retry on a new connection waits here until its first attempt commits or aborts, and then sees its ring row.
    SELECT i.owner_role, i.last_used_at INTO owner, used
    FROM topic.producer_ids i WHERE i.producer_id = produce_check.producer_id FOR UPDATE;
    IF NOT coalesce(pg_has_role(topic.caller(), to_regrole(quote_ident(owner)), 'USAGE'), false) THEN
        RAISE EXCEPTION 'topic.produce_check: producer % is not known to role %', produce_check.producer_id, topic.caller()
            USING ERRCODE = 'PT004';
    END IF;
    IF used < now() - interval '1 minute' THEN
        UPDATE topic.producer_ids i SET last_used_at = now() WHERE i.producer_id = produce_check.producer_id;
    END IF;
    ring := ARRAY(SELECT p FROM topic.topic_producers p
                  WHERE p.schema_name = produce_check.schema_name AND p.topic = produce_check.topic
                    AND p.producer_id = produce_check.producer_id AND p.band = produce_check.band);
    ring_epoch := (SELECT max(r.producer_epoch) FROM unnest(ring) r);
    IF epoch < ring_epoch THEN
        RAISE EXCEPTION 'topic.produce_check: producer % sent epoch %, which is older than epoch %',
            produce_check.producer_id, epoch, ring_epoch USING ERRCODE = 'PT003';
    ELSIF epoch > ring_epoch THEN
        DELETE FROM topic.topic_producers p
        WHERE p.schema_name = produce_check.schema_name AND p.topic = produce_check.topic
          AND p.producer_id = produce_check.producer_id AND p.band = produce_check.band;
        ring := '{}';
    END IF;
    SELECT true, r.base_published_at, r.base_seq INTO duplicate, base_published_at, base_seq
    FROM unnest(ring) r WHERE r.first_sequence = first_seq;
    IF duplicate THEN
        RETURN;
    END IF;
    SELECT r.slot INTO newest FROM unnest(ring) r
    WHERE CASE WHEN r.last_sequence = 2147483647 THEN 0 ELSE r.last_sequence + 1 END = first_seq;
    IF cardinality(ring) = 0 AND first_seq <> 0 AND ring_epoch IS NULL THEN
        RAISE EXCEPTION 'topic.produce_check: producer % has no state on %.% band %, and sent sequence %, not 0',
            produce_check.producer_id, produce_check.schema_name, produce_check.topic, produce_check.band, first_seq
            USING ERRCODE = 'PT004';
    END IF;
    IF (cardinality(ring) > 0 AND newest IS NULL) OR (cardinality(ring) = 0 AND first_seq <> 0) THEN
        RAISE EXCEPTION 'topic.produce_check: producer % sent sequence % on %.% band %, which is not the next sequence',
            produce_check.producer_id, first_seq, produce_check.schema_name, produce_check.topic, produce_check.band
            USING ERRCODE = 'PT002';
    END IF;
    INSERT INTO topic.topic_producers
        (schema_name, topic, producer_id, producer_epoch, band, slot, first_sequence, last_sequence,
         base_published_at, base_seq)
    VALUES (produce_check.schema_name, produce_check.topic, produce_check.producer_id, epoch, produce_check.band,
            coalesce((newest + 1) % 5, 0), first_seq, last_seq, row_published_at, row_seq)
    ON CONFLICT ON CONSTRAINT topic_producers_pkey DO UPDATE
    SET producer_epoch = EXCLUDED.producer_epoch, first_sequence = EXCLUDED.first_sequence,
        last_sequence = EXCLUDED.last_sequence, base_published_at = EXCLUDED.base_published_at,
        base_seq = EXCLUDED.base_seq, updated_at = now();
    SELECT false, row_published_at, row_seq INTO duplicate, base_published_at, base_seq;
END
$$;

CREATE FUNCTION topic.fetch(topic text, band int, from_offset bigint, max_rows int DEFAULT 500, filter jsonb DEFAULT NULL)
RETURNS TABLE (log_offset bigint, key text, value jsonb, headers jsonb, published_at timestamptz)
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part($1, '.', 1);
    t text := substr($1, length(s) + 2);
    oldest bigint;
BEGIN
    SELECT b.oldest_offset INTO oldest FROM topic.band_offsets($1) b WHERE b.band = $2;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'topic.fetch: topic % has no band %', $1, $2 USING ERRCODE = '42P01';
    END IF;
    IF from_offset < oldest THEN
        RAISE EXCEPTION 'topic.fetch: offset % is below the oldest offset % of band % of topic %',
            from_offset, oldest, $2, $1 USING ERRCODE = 'PT001';
    END IF;
    RETURN QUERY EXECUTE format(
        'SELECT q.log_offset, q.key::text, q.value, q.headers, q.published_at FROM %I.%I q
         WHERE q.band = $1 AND q.log_offset >= $2 AND ($4 IS NULL OR q.value @> $4)
         ORDER BY q.log_offset LIMIT $3', s, t)
        USING $2, from_offset, max_rows, filter;
END
$$;

CREATE FUNCTION topic.offset_for_time(topic text, band int, ts timestamptz) RETURNS bigint
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(offset_for_time.topic, '.', 1);
    t text := substr(offset_for_time.topic, length(s) + 2);
    found_offset bigint;
BEGIN
    EXECUTE format('SELECT min(q.log_offset) FROM %I.%I q WHERE q.band = $1 AND q.published_at >= $2', s, t)
        INTO found_offset USING offset_for_time.band, ts;
    RETURN found_offset;
END
$$;

CREATE FUNCTION topic.wire_bytes(value jsonb) RETURNS bytea
LANGUAGE sql IMMUTABLE SET search_path = pg_catalog, pg_temp
AS $$
    SELECT CASE WHEN jsonb_typeof(value) = 'string'
                 AND value #>> '{}' ~ '^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$'
           THEN decode(value #>> '{}', 'base64') END
$$;

CREATE FUNCTION topic.group_rebalance(group_name text) RETURNS void
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
BEGIN
    IF EXISTS (SELECT FROM topic.topic_group_members m WHERE m.group_name = group_rebalance.group_name) THEN
        UPDATE topic.topic_groups g
        SET state = 'PreparingRebalance', updated_at = now(),
            rebalance_started_at = CASE WHEN g.state = 'PreparingRebalance' THEN g.rebalance_started_at ELSE now() END
        WHERE g.group_name = group_rebalance.group_name;
    ELSE
        UPDATE topic.topic_groups g
        SET state = 'Empty', generation_id = g.generation_id + 1, leader_member_id = NULL, protocol_type = NULL,
            protocol_name = NULL, rebalance_started_at = NULL, updated_at = now()
        WHERE g.group_name = group_rebalance.group_name;
    END IF;
    PERFORM pg_notify('pg_topics_group', group_rebalance.group_name);
END
$$;

CREATE FUNCTION topic.expire_members(group_name text) RETURNS int
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    n int;
BEGIN
    DELETE FROM topic.topic_group_members m
    WHERE m.group_name = expire_members.group_name
      AND m.last_heartbeat_at < now() - m.session_timeout_ms * interval '1 millisecond';
    GET DIAGNOSTICS n = ROW_COUNT;
    IF n > 0 THEN
        UPDATE topic.topic_groups g SET expired_members = g.expired_members + n
        WHERE g.group_name = expire_members.group_name;
        PERFORM topic.group_rebalance(expire_members.group_name);
    END IF;
    RETURN n;
END
$$;

CREATE FUNCTION topic.group_protocol(group_name text) RETURNS text
LANGUAGE sql STABLE SET search_path = pg_catalog, pg_temp
AS $$
    WITH offers AS (
        SELECT m.member_id, p.value->>'name' AS name, p.i
        FROM topic.topic_group_members m, jsonb_array_elements(m.protocols) WITH ORDINALITY p(value, i)
        WHERE m.group_name = $1),
    common AS (
        SELECT o.name FROM offers o GROUP BY o.name
        HAVING count(DISTINCT o.member_id) = (SELECT count(DISTINCT a.member_id) FROM offers a)),
    votes AS (
        SELECT DISTINCT ON (o.member_id) o.name FROM offers o JOIN common c ON c.name = o.name
        ORDER BY o.member_id, o.i)
    SELECT v.name FROM votes v GROUP BY v.name ORDER BY count(*) DESC, v.name LIMIT 1
$$;

CREATE FUNCTION topic.group_enter(group_name text) RETURNS text
LANGUAGE plpgsql SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    g topic.topic_groups;
    from_empty boolean;
    all_rejoined boolean;
    longest int;
BEGIN
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = group_enter.group_name;
    IF NOT FOUND THEN
        RETURN 'UNKNOWN_MEMBER_ID';
    END IF;
    IF NOT coalesce(pg_has_role(topic.caller(), to_regrole(quote_ident(g.owner_role)), 'USAGE'),
                    (SELECT r.rolsuper FROM pg_roles r WHERE r.rolname = topic.caller()), false) THEN
        RETURN 'GROUP_AUTHORIZATION_FAILED';
    END IF;
    PERFORM FROM topic.topic_groups t WHERE t.group_name = g.group_name FOR UPDATE;
    IF NOT FOUND THEN
        RETURN 'UNKNOWN_MEMBER_ID';
    END IF;
    PERFORM topic.expire_members(g.group_name);
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = g.group_name;
    IF g.state <> 'PreparingRebalance' THEN
        RETURN NULL;
    END IF;
    from_empty := g.leader_member_id IS NULL;
    SELECT bool_and(m.joined_generation = g.generation_id), max(m.rebalance_ms) INTO all_rejoined, longest
    FROM topic.topic_group_members m WHERE m.group_name = g.group_name;
    IF from_empty THEN
        longest := least(longest, current_setting('pg_topics.group_initial_rebalance_delay_ms')::int);
    END IF;
    IF (from_empty OR NOT all_rejoined) AND now() < g.rebalance_started_at + longest * interval '1 millisecond' THEN
        RETURN NULL;
    END IF;
    DELETE FROM topic.topic_group_members m
    WHERE m.group_name = g.group_name AND m.joined_generation IS DISTINCT FROM g.generation_id;
    IF NOT EXISTS (SELECT FROM topic.topic_group_members m WHERE m.group_name = g.group_name) THEN
        PERFORM topic.group_rebalance(g.group_name);
        RETURN NULL;
    END IF;
    UPDATE topic.topic_group_members m SET assignment = NULL WHERE m.group_name = g.group_name;
    UPDATE topic.topic_groups t
    SET generation_id = t.generation_id + 1, state = 'CompletingRebalance', rebalance_started_at = NULL,
        updated_at = now(), protocol_name = topic.group_protocol(t.group_name),
        leader_member_id = (SELECT m.member_id FROM topic.topic_group_members m WHERE m.group_name = t.group_name
                            ORDER BY m.member_id IS NOT DISTINCT FROM t.leader_member_id DESC, m.member_id LIMIT 1)
    WHERE t.group_name = g.group_name;
    PERFORM pg_notify('pg_topics_group', g.group_name);
    RETURN NULL;
END
$$;

CREATE FUNCTION topic.expire_groups() RETURNS void
LANGUAGE sql SET search_path = pg_catalog, pg_temp
AS $$
    SELECT topic.group_enter(d.group_name)
    FROM (SELECT DISTINCT m.group_name FROM topic.topic_group_members m
          WHERE m.last_heartbeat_at < now() - current_setting('pg_topics.group_min_session_ms')::int * interval '1 millisecond'
            AND m.last_heartbeat_at < now() - m.session_timeout_ms * interval '1 millisecond') d
$$;

CREATE FUNCTION topic.group_join_poll(
    group_name text, INOUT member_id text,
    OUT error text, OUT generation_id int, OUT protocol_name text, OUT leader_id text, OUT members jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    g topic.topic_groups;
    me topic.topic_group_members;
BEGIN
    error := topic.group_enter(group_join_poll.group_name);
    IF error IS NOT NULL THEN
        RETURN;
    END IF;
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = group_join_poll.group_name;
    UPDATE topic.topic_group_members m SET last_heartbeat_at = now()
    WHERE m.group_name = g.group_name AND m.member_id = group_join_poll.member_id
    RETURNING * INTO me;
    IF NOT FOUND THEN
        error := 'UNKNOWN_MEMBER_ID';
        RETURN;
    END IF;
    IF g.state = 'PreparingRebalance' AND me.joined_generation = g.generation_id THEN
        RETURN;
    END IF;
    IF g.state = 'PreparingRebalance' OR me.joined_generation <> g.generation_id - 1 THEN
        error := 'REBALANCE_IN_PROGRESS';
        RETURN;
    END IF;
    error := 'NONE';
    generation_id := g.generation_id;
    protocol_name := g.protocol_name;
    leader_id := g.leader_member_id;
    SELECT coalesce(jsonb_agg(jsonb_build_object('member_id', m.member_id, 'metadata', p.value->'metadata')
                              ORDER BY m.member_id), '[]')
    INTO members
    FROM topic.topic_group_members m, jsonb_array_elements(m.protocols) p(value)
    WHERE m.group_name = g.group_name AND p.value->>'name' = g.protocol_name
      AND g.leader_member_id = group_join_poll.member_id;
END
$$;

CREATE FUNCTION topic.group_join(
    group_name text, INOUT member_id text, client_id text, session_ms int, rebalance_ms int,
    protocol_type text, protocols jsonb,
    OUT error text, OUT generation_id int, OUT protocol_name text, OUT leader_id text, OUT members jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    g topic.topic_groups;
BEGIN
    IF group_join.group_name = '' OR starts_with(group_join.group_name, '__pg_topics_sync:') THEN
        error := 'INVALID_GROUP_ID';
        RETURN;
    END IF;
    IF session_ms IS NULL OR rebalance_ms IS NULL OR rebalance_ms < 0
       OR session_ms NOT BETWEEN current_setting('pg_topics.group_min_session_ms')::int
                             AND current_setting('pg_topics.group_max_session_ms')::int THEN
        error := 'INVALID_SESSION_TIMEOUT';
        RETURN;
    END IF;
    IF jsonb_typeof(protocols) IS DISTINCT FROM 'array' OR protocols = '[]' THEN
        error := 'INCONSISTENT_GROUP_PROTOCOL';
        RETURN;
    END IF;
    IF EXISTS (SELECT FROM jsonb_array_elements(protocols) p(value)
               WHERE jsonb_typeof(p.value->'name') IS DISTINCT FROM 'string') THEN
        error := 'INCONSISTENT_GROUP_PROTOCOL';
        RETURN;
    END IF;
    INSERT INTO topic.topic_groups (group_name, owner_role) VALUES (group_join.group_name, topic.caller())
    ON CONFLICT DO NOTHING;
    error := topic.group_enter(group_join.group_name);
    IF error IS NOT NULL THEN
        RETURN;
    END IF;
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = group_join.group_name;
    IF member_id = '' THEN
        member_id := coalesce(client_id, '') || '-' || gen_random_uuid();
        UPDATE topic.topic_groups t
        SET updated_at = now(),
            pending_members = jsonb_build_object(group_join.member_id, now() + session_ms * interval '1 millisecond')
                || (SELECT coalesce(jsonb_object_agg(p.key, p.value), '{}') FROM jsonb_each_text(t.pending_members) p
                    WHERE p.value::timestamptz > now())
        WHERE t.group_name = g.group_name;
        error := 'MEMBER_ID_REQUIRED';
        RETURN;
    END IF;
    IF NOT EXISTS (SELECT FROM topic.topic_group_members m
                   WHERE m.group_name = g.group_name AND m.member_id = group_join.member_id)
       AND NOT coalesce((g.pending_members ->> group_join.member_id)::timestamptz > now(), false) THEN
        error := 'UNKNOWN_MEMBER_ID';
        RETURN;
    END IF;
    IF (g.protocol_type IS DISTINCT FROM group_join.protocol_type
        AND EXISTS (SELECT FROM topic.topic_group_members m
                    WHERE m.group_name = g.group_name AND m.member_id <> group_join.member_id))
       OR NOT EXISTS (SELECT FROM jsonb_array_elements(protocols) p(value)
                      WHERE NOT EXISTS (SELECT FROM topic.topic_group_members m
                                        WHERE m.group_name = g.group_name AND m.member_id <> group_join.member_id
                                          AND NOT m.protocols @> jsonb_build_array(jsonb_build_object('name', p.value->'name')))) THEN
        error := 'INCONSISTENT_GROUP_PROTOCOL';
        RETURN;
    END IF;
    IF EXISTS (SELECT FROM topic.topic_group_members m
               WHERE m.group_name = g.group_name AND m.member_id = group_join.member_id
                 AND m.joined_generation = g.generation_id - 1 AND m.protocols = group_join.protocols
                 AND (g.state = 'CompletingRebalance' OR (g.state = 'Stable' AND m.member_id <> g.leader_member_id))) THEN
        SELECT p.error, p.generation_id, p.protocol_name, p.leader_id, p.members
        INTO error, generation_id, protocol_name, leader_id, members
        FROM topic.group_join_poll(g.group_name, group_join.member_id) p;
        RETURN;
    END IF;
    INSERT INTO topic.topic_group_members AS m
        (group_name, member_id, owner_role, client_id, session_timeout_ms, rebalance_ms, protocols, joined_generation)
    VALUES (g.group_name, group_join.member_id, g.owner_role, client_id, session_ms, rebalance_ms, protocols, g.generation_id)
    ON CONFLICT ON CONSTRAINT topic_group_members_pkey DO UPDATE
    SET client_id = EXCLUDED.client_id, session_timeout_ms = EXCLUDED.session_timeout_ms,
        rebalance_ms = EXCLUDED.rebalance_ms, protocols = EXCLUDED.protocols,
        joined_generation = EXCLUDED.joined_generation, last_heartbeat_at = now();
    UPDATE topic.topic_groups t
    SET protocol_type = group_join.protocol_type, pending_members = t.pending_members - group_join.member_id
    WHERE t.group_name = g.group_name;
    PERFORM topic.group_rebalance(g.group_name);
    SELECT p.error, p.generation_id, p.protocol_name, p.leader_id, p.members
    INTO error, generation_id, protocol_name, leader_id, members
    FROM topic.group_join_poll(g.group_name, group_join.member_id) p;
END
$$;

CREATE FUNCTION topic.group_sync_poll(group_name text, member_id text, generation int, OUT error text, OUT assignment jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    g topic.topic_groups;
    me topic.topic_group_members;
BEGIN
    error := topic.group_enter(group_sync_poll.group_name);
    IF error IS NOT NULL THEN
        RETURN;
    END IF;
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = group_sync_poll.group_name;
    UPDATE topic.topic_group_members m SET last_heartbeat_at = now()
    WHERE m.group_name = g.group_name AND m.member_id = group_sync_poll.member_id
    RETURNING * INTO me;
    IF NOT FOUND THEN
        error := 'UNKNOWN_MEMBER_ID';
    ELSIF generation <> g.generation_id THEN
        error := 'ILLEGAL_GENERATION';
    ELSIF g.state = 'PreparingRebalance' THEN
        error := 'REBALANCE_IN_PROGRESS';
    ELSIF g.state = 'Stable' THEN
        error := 'NONE';
        assignment := me.assignment;
    END IF;
END
$$;

CREATE FUNCTION topic.group_sync(group_name text, member_id text, generation int, assignments jsonb,
    OUT error text, OUT assignment jsonb)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    g topic.topic_groups;
BEGIN
    error := topic.group_enter(group_sync.group_name);
    IF error IS NOT NULL THEN
        RETURN;
    END IF;
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = group_sync.group_name;
    IF g.state = 'CompletingRebalance' AND g.generation_id = generation AND g.leader_member_id = group_sync.member_id THEN
        UPDATE topic.topic_group_members m SET assignment = assignments -> m.member_id WHERE m.group_name = g.group_name;
        UPDATE topic.topic_groups t SET state = 'Stable', updated_at = now() WHERE t.group_name = g.group_name;
        PERFORM pg_notify('pg_topics_group', g.group_name);
    END IF;
    SELECT p.error, p.assignment INTO error, assignment
    FROM topic.group_sync_poll(g.group_name, group_sync.member_id, generation) p;
END
$$;

CREATE FUNCTION topic.group_heartbeat(group_name text, member_id text, generation int) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    err text := topic.group_enter(group_heartbeat.group_name);
    g topic.topic_groups;
BEGIN
    IF err IS NOT NULL THEN
        RETURN err;
    END IF;
    UPDATE topic.topic_group_members m SET last_heartbeat_at = now()
    WHERE m.group_name = group_heartbeat.group_name AND m.member_id = group_heartbeat.member_id;
    IF NOT FOUND THEN
        RETURN 'UNKNOWN_MEMBER_ID';
    END IF;
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = group_heartbeat.group_name;
    IF generation <> g.generation_id THEN
        RETURN 'ILLEGAL_GENERATION';
    END IF;
    IF g.state = 'PreparingRebalance' THEN
        RETURN 'REBALANCE_IN_PROGRESS';
    END IF;
    RETURN 'NONE';
END
$$;

CREATE FUNCTION topic.group_leave(group_name text, member_id text) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    err text := topic.group_enter(group_leave.group_name);
BEGIN
    IF err IS NOT NULL THEN
        RETURN err;
    END IF;
    DELETE FROM topic.topic_group_members m
    WHERE m.group_name = group_leave.group_name AND m.member_id = group_leave.member_id;
    IF NOT FOUND THEN
        RETURN 'UNKNOWN_MEMBER_ID';
    END IF;
    PERFORM topic.group_rebalance(group_leave.group_name);
    RETURN 'NONE';
END
$$;

CREATE FUNCTION topic.commit_offset(topic text, group_name text, band int, new_offset bigint, generation int) RETURNS text
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(commit_offset.topic, '.', 1);
    t text := substr(commit_offset.topic, length(s) + 2);
    queue regclass := to_regclass(format('%I.%I', s, t));
    err text;
    n bigint;
    k int;
    readable boolean := queue IS NOT NULL
        AND EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = s AND c.topic = t)
        AND has_table_privilege(topic.caller(), queue, 'SELECT');
BEGIN
    IF commit_offset.group_name = '' OR starts_with(commit_offset.group_name, '__pg_topics_sync:') THEN
        RETURN 'INVALID_GROUP_ID';
    END IF;
    IF commit_offset.generation = -1 AND readable THEN
        INSERT INTO topic.topic_groups (group_name, owner_role) VALUES (commit_offset.group_name, topic.caller())
        ON CONFLICT DO NOTHING;
    END IF;
    err := topic.group_enter(commit_offset.group_name);
    IF err IS NOT NULL THEN
        RETURN err;
    END IF;
    IF NOT readable THEN
        RETURN 'TOPIC_AUTHORIZATION_FAILED';
    END IF;
    IF new_offset IS NULL OR new_offset < 0 THEN
        RETURN 'OFFSET_OUT_OF_RANGE';
    END IF;
    n := least(new_offset, (SELECT p.next_offset FROM topic.topic_band_position p
                            WHERE p.schema_name = s AND p.topic = t AND p.band = commit_offset.band));
    BEGIN
        INSERT INTO topic.topic_offsets AS o
            (schema_name, topic, group_name, band, owner_role, committed_offset, generation_id)
        SELECT s, t, g.group_name, commit_offset.band, g.owner_role, n, g.generation_id
        FROM topic.topic_groups g
        WHERE g.group_name = commit_offset.group_name
          AND (g.generation_id = commit_offset.generation OR (commit_offset.generation = -1 AND g.state = 'Empty'))
        ON CONFLICT ON CONSTRAINT topic_offsets_pkey DO UPDATE
        SET committed_offset = EXCLUDED.committed_offset, generation_id = EXCLUDED.generation_id
        WHERE o.generation_id <= EXCLUDED.generation_id;
        GET DIAGNOSTICS k = ROW_COUNT;
    EXCEPTION WHEN foreign_key_violation OR numeric_value_out_of_range THEN
        RETURN 'UNKNOWN_TOPIC_OR_PARTITION';
    END;
    IF k = 1 THEN
        RETURN 'NONE';
    END IF;
    RETURN 'ILLEGAL_GENERATION';
END
$$;

CREATE FUNCTION topic.fetch_offset(topic text, group_name text, band int) RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    s text := split_part(fetch_offset.topic, '.', 1);
    t text := substr(fetch_offset.topic, length(s) + 2);
    queue regclass := to_regclass(format('%I.%I', s, t));
    group_owner name;
BEGIN
    SELECT g.owner_role INTO group_owner FROM topic.topic_groups g WHERE g.group_name = fetch_offset.group_name;
    IF NOT coalesce(pg_has_role(topic.caller(), to_regrole(quote_ident(group_owner)), 'USAGE'), group_owner IS NULL)
       OR queue IS NULL
       OR NOT EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = s AND c.topic = t)
       OR NOT has_table_privilege(topic.caller(), queue, 'SELECT') THEN
        RAISE EXCEPTION 'topic.fetch_offset: role % may not read group % on topic %',
            topic.caller(), fetch_offset.group_name, fetch_offset.topic USING ERRCODE = '42501';
    END IF;
    RETURN (SELECT o.committed_offset FROM topic.topic_offsets o
            WHERE o.schema_name = s AND o.topic = t AND o.group_name = fetch_offset.group_name AND o.band = fetch_offset.band);
END
$$;

CREATE FUNCTION topic.delete_group(group_name text) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    err text;
BEGIN
    IF starts_with(delete_group.group_name, '__pg_topics_sync:') THEN
        RAISE EXCEPTION 'topic.delete_group: the group name % is reserved for the sync', delete_group.group_name
            USING ERRCODE = '42501';
    END IF;
    err := topic.group_enter(delete_group.group_name);
    IF err = 'UNKNOWN_MEMBER_ID' THEN
        RAISE EXCEPTION 'topic.delete_group: group % does not exist', delete_group.group_name USING ERRCODE = '42704';
    END IF;
    IF err = 'GROUP_AUTHORIZATION_FAILED' THEN
        RAISE EXCEPTION 'topic.delete_group: role % may not delete group %', topic.caller(), delete_group.group_name
            USING ERRCODE = '42501';
    END IF;
    IF EXISTS (SELECT FROM topic.topic_group_members m WHERE m.group_name = delete_group.group_name) THEN
        RAISE EXCEPTION 'topic.delete_group: group % has members', delete_group.group_name USING ERRCODE = '55006';
    END IF;
    DELETE FROM topic.topic_groups g WHERE g.group_name = delete_group.group_name;
END
$$;

CREATE FUNCTION topic.ddl_end() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
BEGIN
    IF EXISTS (
        SELECT FROM pg_event_trigger_ddl_commands() d
        JOIN pg_trigger g ON g.tgrelid = d.objid AND g.tgfoid = 'topic.publish_floor()'::regprocedure
        JOIN pg_class r ON r.oid = d.objid
        JOIN pg_namespace n ON n.oid = r.relnamespace
        WHERE d.classid = 'pg_class'::regclass
          AND NOT EXISTS (SELECT FROM topic.topic_config c WHERE c.schema_name = n.nspname AND c.topic = r.relname))
    THEN
        RAISE EXCEPTION 'topic: a queue table must keep its name and schema'
            USING HINT = 'Make a new topic with topic.create_topic, and drop the old one with topic.drop_topic.';
    END IF;
    BEGIN
        FOR q IN
            SELECT c.schema_name, c.topic, c.band_count,
                   array_agg(pg_get_constraintdef(k.oid)) FILTER (WHERE k.oid IS NOT NULL) AS checks
            FROM (SELECT DISTINCT d.objid FROM pg_event_trigger_ddl_commands() d
                  WHERE d.classid = 'pg_class'::regclass) d
            JOIN pg_class r ON r.oid = d.objid
            JOIN pg_namespace n ON n.oid = r.relnamespace
            JOIN topic.topic_config c ON c.schema_name = n.nspname AND c.topic = r.relname
            LEFT JOIN pg_attribute a ON a.attrelid = r.oid AND a.attname = 'band'
            LEFT JOIN pg_constraint k ON k.conrelid = r.oid AND k.contype = 'c' AND a.attnum = ANY (k.conkey)
            GROUP BY c.schema_name, c.topic, c.band_count
        LOOP
            IF q.checks IS DISTINCT FROM ARRAY[format('CHECK (((band >= 0) AND (band <= %s)))', q.band_count - 1)] THEN
                RAISE WARNING 'topic: the CHECK constraints on band of %.% are %, which do not match band_count %',
                    q.schema_name, q.topic, q.checks, q.band_count;
            END IF;
        END LOOP;
    EXCEPTION WHEN OTHERS THEN
        RAISE WARNING 'topic: the % event trigger failed on %: %', TG_EVENT, TG_TAG, SQLERRM;
    END;
END
$$;

CREATE FUNCTION topic.sql_drop() RETURNS event_trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    r record;
BEGIN
    FOR r IN
        UPDATE topic.topic_config c SET sync_enabled = false
        FROM pg_event_trigger_dropped_objects() d
        WHERE c.sync_enabled AND d.classid = 'pg_class'::regclass AND d.objid = c.sync_table
          AND (d.objsubid = 0 OR d.address_names[3] = c.sync_key)
        RETURNING c.schema_name, c.topic
    LOOP
        RAISE WARNING 'topic: %.% stops syncing, because its base table or its sync key column was dropped',
            r.schema_name, r.topic;
    END LOOP;
    FOR r IN
        SELECT c.schema_name, c.topic FROM pg_event_trigger_dropped_objects() d
        JOIN topic.topic_config c ON c.schema_name = d.schema_name AND c.topic = d.object_name
        WHERE d.classid = 'pg_class'::regclass AND d.objsubid = 0 AND d.object_type = 'table'
    LOOP
        DELETE FROM topic.topic_groups g WHERE g.group_name = '__pg_topics_sync:' || r.schema_name || '.' || r.topic;
        DELETE FROM topic.topic_config c WHERE c.schema_name = r.schema_name AND c.topic = r.topic;
    END LOOP;
EXCEPTION WHEN OTHERS THEN
    RAISE WARNING 'topic: the % event trigger failed on %: %', TG_EVENT, TG_TAG, SQLERRM;
END
$$;

CREATE FUNCTION topic.describe_configs(topic text)
RETURNS TABLE (name text, value text, editable boolean)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    q record;
    c topic.topic_config;
BEGIN
    SELECT * INTO q FROM topic.owned_topic(describe_configs.topic);
    SELECT * INTO c FROM topic.topic_config t WHERE t.schema_name = q.schema_name AND t.topic = q.topic_name;
    RETURN QUERY VALUES
        ('retention.ms', (extract(epoch FROM c.retention_interval) * 1000)::bigint::text, true),
        ('cleanup.policy', 'delete', false),
        ('message.timestamp.type', 'LogAppendTime', false),
        ('max.message.bytes', current_setting('pg_topics.max_message_bytes'), false),
        ('pg_topics.min_durability', c.min_durability, true),
        ('pg_topics.replication_factor', CASE c.min_durability WHEN 'replicated'
            THEN topic.sync_copies(current_setting('synchronous_standby_names')) ELSE 1 END::text, false);
END
$$;

CREATE FUNCTION topic.describe_group(group_name text)
RETURNS TABLE (found boolean, state text, protocol_type text, protocol_name text,
               member_id text, client_id text, metadata bytea, assignment bytea)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    g topic.topic_groups;
BEGIN
    SELECT * INTO g FROM topic.topic_groups t WHERE t.group_name = describe_group.group_name;
    IF NOT FOUND OR NOT coalesce(pg_has_role(topic.caller(), to_regrole(quote_ident(g.owner_role)), 'USAGE'), false) THEN
        RETURN QUERY SELECT false, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::bytea, NULL::bytea;
        RETURN;
    END IF;
    RETURN QUERY
    SELECT true, g.state, g.protocol_type, g.protocol_name, m.member_id, m.client_id,
           topic.wire_bytes((SELECT p.value -> 'metadata' FROM jsonb_array_elements(m.protocols) p(value)
                             WHERE p.value ->> 'name' = g.protocol_name)),
           topic.wire_bytes(m.assignment)
    FROM topic.topic_group_members m WHERE m.group_name = g.group_name;
    IF NOT FOUND THEN
        RETURN QUERY SELECT true, g.state, g.protocol_type, g.protocol_name,
                            NULL::text, NULL::text, NULL::bytea, NULL::bytea;
    END IF;
END
$$;

CREATE VIEW topic.stamp_backlog AS
    SELECT schema_name, topic, backlog_age, stamped_at FROM topic.topic_config;

CREATE VIEW topic.oldest_xact AS
    SELECT min(xact_start) AS xact_start, now() - min(xact_start) AS age
    FROM pg_catalog.pg_stat_activity WHERE xact_start IS NOT NULL;

CREATE VIEW topic.detach_waiting AS
    SELECT c.schema_name, c.topic, c.detaching::text AS detaching,
           coalesce(h.inhdetachpending, false) AS pending,
           EXISTS (SELECT FROM pg_catalog.pg_stat_activity a
                   WHERE a.backend_type = 'pg_topics partition' AND a.wait_event_type = 'Lock'
                     AND a.query ILIKE '%DETACH PARTITION%') AS worker_waiting
    FROM topic.topic_config c
    LEFT JOIN pg_catalog.pg_inherits h ON h.inhrelid = c.detaching
    WHERE c.detaching IS NOT NULL;

CREATE VIEW topic.write_partition_dead_tuples AS
    SELECT c.schema_name, c.topic, s.relname AS partition, s.n_dead_tup
    FROM topic.topic_config c
    JOIN pg_catalog.pg_stat_user_tables s
      ON s.schemaname = c.schema_name
     AND s.relname = c.topic || '_p' || to_char(
             date_bin(c.partition_interval, now(), timestamptz '2000-01-01 00:00:00+00') AT TIME ZONE 'UTC',
             'YYYYMMDDHH24MISS');

CREATE VIEW topic.partition_headroom AS
    SELECT c.schema_name, c.topic, max(b.upper::timestamptz) - now() AS headroom
    FROM topic.topic_config c
    JOIN pg_catalog.pg_inherits h
      ON h.inhparent = to_regclass(format('%I.%I', c.schema_name, c.topic))
    JOIN pg_catalog.pg_class k ON k.oid = h.inhrelid
    CROSS JOIN LATERAL substring(pg_get_expr(k.relpartbound, k.oid) FROM ' TO \(''([^'']+)''\)$') b(upper)
    GROUP BY c.schema_name, c.topic;

CREATE VIEW topic.worker_headroom AS
    SELECT current_setting('max_worker_processes')::int - count(*) AS headroom
    FROM pg_catalog.pg_stat_activity
    WHERE backend_type NOT IN ('client backend', 'walsender', 'background writer', 'checkpointer',
                                'walwriter', 'archiver', 'startup', 'walreceiver', 'walsummarizer',
                                'autovacuum launcher', 'autovacuum worker', 'slotsync worker');

CREATE VIEW topic.listener_status AS
    SELECT datname, pid, query FROM pg_catalog.pg_stat_activity WHERE backend_type = 'pg_topics listener';

CREATE VIEW topic.group_expiry AS
    SELECT group_name, expired_members FROM topic.topic_groups;

CREATE FUNCTION topic.error_rows() RETURNS TABLE (schema_name text, topic text, rows bigint, recent bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    c record;
BEGIN
    FOR c IN SELECT t.schema_name, t.topic FROM topic.topic_config t WHERE t.sync_table IS NOT NULL LOOP
        CONTINUE WHEN to_regclass(format('%I.%I', c.schema_name, c.topic || 'e')) IS NULL;
        RETURN QUERY EXECUTE format(
            'SELECT %L::text, %L::text, count(*), count(*) FILTER (WHERE failed_at > now() - interval ''1 hour'')
             FROM %I.%I',
            c.schema_name, c.topic, c.schema_name, c.topic || 'e');
    END LOOP;
END
$$;

CREATE VIEW topic.producer_rows AS
    SELECT schema_name, topic, count(*) AS rows FROM topic.topic_producers GROUP BY schema_name, topic;

CREATE VIEW topic.syncrep_waiters AS
    SELECT pid, usename, query FROM pg_catalog.pg_stat_activity WHERE wait_event = 'SyncRep';

CREATE VIEW topic.sync_lag AS
    SELECT o.schema_name, o.topic, o.band, p.next_offset - o.committed_offset AS lag
    FROM topic.topic_offsets o
    JOIN topic.topic_band_position p
      ON p.schema_name = o.schema_name AND p.topic = o.topic AND p.band = o.band
    WHERE o.group_name = '__pg_topics_sync:' || o.schema_name || '.' || o.topic;

CREATE VIEW topic.consumer_lag AS
    SELECT o.schema_name, o.topic, o.group_name, o.band, p.next_offset - o.committed_offset AS lag
    FROM topic.topic_offsets o
    JOIN topic.topic_band_position p
      ON p.schema_name = o.schema_name AND p.topic = o.topic AND p.band = o.band;

CREATE FUNCTION topic.duplicate_offsets()
RETURNS TABLE (schema_name text, topic text, band smallint, log_offset bigint, copies bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
DECLARE
    c record;
BEGIN
    FOR c IN SELECT t.schema_name, t.topic FROM topic.topic_config t LOOP
        CONTINUE WHEN to_regclass(format('%I.%I', c.schema_name, c.topic)) IS NULL;
        RETURN QUERY
        SELECT c.schema_name, c.topic, d.band, d.log_offset, d.copies
        FROM topic.check_duplicates(c.schema_name, c.topic, false) d;
    END LOOP;
END
$$;

CREATE FUNCTION topic.health() RETURNS TABLE (
    schema_name text,
    topic text,
    backlog_age interval,
    stamped_at timestamptz,
    partition_headroom interval,
    listener_bound boolean,
    duplicate boolean,
    syncrep_waiting boolean,
    ok boolean
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, pg_temp
AS $$
    WITH listener AS (
        SELECT coalesce(bool_or(query LIKE 'listening on port%'), false) AS bound
        FROM pg_catalog.pg_stat_activity
        WHERE backend_type = 'pg_topics listener' AND datname = current_database()),
    syncrep AS (
        SELECT EXISTS (SELECT FROM pg_catalog.pg_stat_activity WHERE wait_event = 'SyncRep') AS waiting),
    duplicates AS (SELECT DISTINCT d.schema_name, d.topic FROM topic.duplicate_offsets() d)
    SELECT c.schema_name, c.topic, c.backlog_age, c.stamped_at, h.headroom, listener.bound,
           dd.schema_name IS NOT NULL, syncrep.waiting,
           to_regclass(format('%I.%I', c.schema_name, c.topic)) IS NOT NULL
               AND now() - c.stamped_at <= c.max_backlog_age / 2
               AND coalesce(h.headroom, interval '0') >= least(interval '1 day', c.partition_interval)
               AND listener.bound
               AND dd.schema_name IS NULL
               AND NOT syncrep.waiting
    FROM topic.topic_config c
    LEFT JOIN topic.partition_headroom h ON h.schema_name = c.schema_name AND h.topic = c.topic
    LEFT JOIN duplicates dd ON dd.schema_name = c.schema_name AND dd.topic = c.topic
    CROSS JOIN listener CROSS JOIN syncrep
$$;

GRANT USAGE ON SCHEMA topic TO PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA topic FROM PUBLIC;
SELECT pg_catalog.pg_extension_config_dump('topic.topic_config', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_band_position', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_groups', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_group_members', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_offsets', '');
SELECT pg_catalog.pg_extension_config_dump('topic.topic_producers', '');
SELECT pg_catalog.pg_extension_config_dump('topic.producer_id_seq', '');
SELECT pg_catalog.pg_extension_config_dump('topic.producer_ids', '');
GRANT SELECT ON topic.topic_groups, topic.topic_group_members, topic.topic_offsets TO PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.stamp_topic(text, text, int) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.raise_synchronous_commit(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.retention_check(oid) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.retention_floor(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.ensure_partitions(text, text, int) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.retention_next(text, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.reap() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.check_duplicates(text, text, boolean) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.sync_topic(text, text, int) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.sync_base_owner(regclass, text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.owned_topic(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.group_rebalance(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.expire_members(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.group_protocol(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.group_enter(text) FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.expire_groups() FROM PUBLIC;
GRANT SELECT ON topic.stamp_backlog, topic.oldest_xact, topic.detach_waiting, topic.write_partition_dead_tuples,
    topic.partition_headroom, topic.worker_headroom, topic.listener_status, topic.group_expiry,
    topic.producer_rows, topic.syncrep_waiters, topic.sync_lag, topic.consumer_lag TO pg_monitor;
REVOKE EXECUTE ON FUNCTION topic.error_rows() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.duplicate_offsets() FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION topic.health() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION topic.error_rows(), topic.duplicate_offsets(), topic.health() TO pg_monitor;
CREATE EVENT TRIGGER pg_topics_ddl_end ON ddl_command_end EXECUTE FUNCTION topic.ddl_end();
CREATE EVENT TRIGGER pg_topics_sql_drop ON sql_drop EXECUTE FUNCTION topic.sql_drop();
