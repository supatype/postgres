use pgrx::datum::Interval;
use pgrx::prelude::*;
use pgrx::spi::{self, quote_qualified_identifier};

use crate::{as_owner, queue_owner, read_one, text_arg, topic_worker};

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn stamp_topic(schema_name: &str, topic: &str, max_rows: default!(i32, 10000)) -> spi::Result<i32> {
    stamp(schema_name, topic, max_rows, false)
}

fn stamp(schema_name: &str, topic: &str, max_rows: i32, skip_locked: bool) -> spi::Result<i32> {
    let names = || vec![text_arg(schema_name), text_arg(topic)];
    let taken = || -> spi::Result<bool> {
        Ok(!skip_locked
            || Spi::get_one_with_args::<bool>(
                "SELECT EXISTS (SELECT FROM topic.topic_band_position
                                WHERE schema_name = $1 AND topic = $2 AND band = 0 FOR NO KEY UPDATE SKIP LOCKED)",
                names(),
            )? == Some(true))
    };
    let owner = queue_owner("topic.stamp_topic", schema_name, topic)?;
    let queue = quote_qualified_identifier(schema_name, topic);
    let beat = |backlog: Option<Interval>| {
        let mut args = names();
        args.push((PgBuiltInOids::INTERVALOID.oid(), backlog.into_datum()));
        let stale = read_one::<bool>(
            "SELECT backlog_age IS DISTINCT FROM $3 OR stamped_at < clock_timestamp() - interval '1 second'
             FROM topic.topic_config WHERE schema_name = $1 AND topic = $2",
            args.clone(),
        )?;
        if stale != Some(true) || !taken()? {
            return Ok(());
        }
        Spi::run_with_args(
            "UPDATE topic.topic_config
             SET backlog_age = $3,
                 stamped_at = CASE WHEN stamped_at < clock_timestamp() - interval '1 second'
                                   THEN clock_timestamp() ELSE stamped_at END
             WHERE schema_name = $1 AND topic = $2",
            Some(args),
        )
    };
    let pending = as_owner(owner, || {
        read_one::<bool>(
            &format!(
                "SELECT (SELECT xact FROM {queue} WHERE log_offset IS NULL ORDER BY xact, seq LIMIT 1) IS NOT NULL"
            ),
            vec![],
        )
    })?;
    if pending != Some(true) {
        beat(Interval::new(0, 0, 0).ok())?;
        return Ok(0);
    }
    if !taken()? {
        return Ok(0);
    }
    Spi::run_with_args(
        "SELECT topic.raise_synchronous_commit(min_durability) FROM topic.topic_config
         WHERE schema_name = $1 AND topic = $2",
        Some(names()),
    )?;
    let level = plain_plans();
    let (next, stamped_by) = Spi::get_two_with_args::<Vec<i64>, Vec<Option<String>>>(
        "SELECT array_agg(next_offset ORDER BY band), array_agg(stamped_by ORDER BY band)
         FROM (SELECT band, next_offset, stamped_by FROM topic.topic_band_position
               WHERE schema_name = $1 AND topic = $2 FOR NO KEY UPDATE) b",
        names(),
    )?;
    let node = Spi::get_one::<String>(
        "SELECT s.system_identifier || '/'
                || ('x' || left(pg_catalog.pg_walfile_name(pg_catalog.pg_current_wal_insert_lsn()), 8))::bit(32)::int
         FROM pg_catalog.pg_control_system() s",
    )?
    .unwrap_or_default();

    let (bands, counts, backlog) = as_owner(owner, || -> spi::Result<_> {
        // Cut the last transaction only when no older one is open: no new row can then sort before its rest.
        let (bands, counts, exact) = Spi::get_three_with_args::<Vec<i16>, Vec<i64>, bool>(
            &format!(
                "WITH w AS (
                     SELECT xact, seq, published_at, band FROM {queue}
                     WHERE log_offset IS NULL ORDER BY xact, seq LIMIT $1),
                 last AS (SELECT xact, seq FROM w ORDER BY xact DESC, seq DESC LIMIT 1),
                 batch AS (
                     SELECT seq, published_at, band, row_number() OVER (PARTITION BY band ORDER BY xact, seq) - 1 AS n
                     FROM (SELECT * FROM w
                           UNION ALL
                           SELECT q.xact, q.seq, q.published_at, q.band FROM {queue} q, last
                           WHERE q.log_offset IS NULL AND q.xact = last.xact AND q.seq > last.seq
                             AND last.xact >= pg_snapshot_xmin(pg_current_snapshot())) s),
                 stamped AS (
                     UPDATE {queue} q SET log_offset = ($2::bigint[])[b.band + 1] + b.n
                     FROM batch b WHERE q.published_at = b.published_at AND q.seq = b.seq
                     RETURNING q.band, q.log_offset)
                 SELECT coalesce(array_agg(band), '{{}}'), coalesce(array_agg(k), '{{}}'),
                        coalesce(sum(k), 0) = (SELECT count(*) FROM batch) AND coalesce(bool_and(exact), true)
                 FROM (SELECT band, count(*) AS k,
                              min(log_offset) = ($2::bigint[])[band + 1]
                              AND max(log_offset) = ($2::bigint[])[band + 1] + count(*) - 1
                              AND count(DISTINCT log_offset) = count(*) AS exact
                       FROM stamped GROUP BY band) c"
            ),
            vec![
                (PgBuiltInOids::INT4OID.oid(), max_rows.into_datum()),
                (PgBuiltInOids::INT8ARRAYOID.oid(), next.into_datum()),
            ],
        )?;
        if exact != Some(true) {
            error!(
                "topic.stamp_topic: {schema_name}.{topic} got offsets that do not match the batch"
            );
        }
        let backlog = Spi::get_one::<Interval>(&format!(
            "SELECT coalesce((SELECT clock_timestamp() - published_at FROM {queue}
                              WHERE log_offset IS NULL ORDER BY xact, seq LIMIT 1), interval '0')"
        ))?;
        Ok((
            bands.unwrap_or_default(),
            counts.unwrap_or_default(),
            backlog,
        ))
    })?;

    let stamped_by = stamped_by.unwrap_or_default();
    for &band in &bands {
        if let Some(Some(old)) = stamped_by.get(band as usize) {
            if *old != node {
                warning!("topic.stamp_topic: {schema_name}.{topic} band {band} was stamped by {old}, now by {node}");
            }
        }
    }
    let total: i64 = counts.iter().sum();
    let mut args = names();
    args.push(text_arg(&node));
    args.push((PgBuiltInOids::INT2ARRAYOID.oid(), bands.into_datum()));
    args.push((PgBuiltInOids::INT8ARRAYOID.oid(), counts.into_datum()));
    Spi::run_with_args(
        "UPDATE topic.topic_band_position p
         SET next_offset = p.next_offset + u.k, stamped_by = $3
         FROM unnest($4::smallint[], $5::bigint[]) u(band, k)
         WHERE p.schema_name = $1 AND p.topic = $2 AND p.band = u.band",
        Some(args),
    )?;
    beat(backlog)?;

    if total > 0 && !skip_locked {
        Spi::run_with_args(
            "SELECT pg_catalog.pg_notify('pg_topics_stamped', $1 || '.' || $2)",
            Some(names()),
        )?;
    }
    unsafe { pg_sys::AtEOXact_GUC(true, level) };
    Ok(total as i32)
}

fn plain_plans() -> i32 {
    let level = unsafe { pg_sys::NewGUCNestLevel() };
    // The estimates grow with the queue, not the batch: without these, the plans scan the queue and JIT compiles them.
    for name in [
        c"enable_seqscan",
        c"enable_bitmapscan",
        c"enable_hashjoin",
        c"enable_mergejoin",
        c"jit",
    ] {
        unsafe {
            pg_sys::set_config_option(
                name.as_ptr(),
                c"off".as_ptr(),
                pg_sys::GucContext::PGC_USERSET,
                pg_sys::GucSource::PGC_S_SESSION,
                pg_sys::GucAction::GUC_ACTION_SAVE,
                true,
                pg_sys::ERROR as i32,
                false,
            )
        };
    }
    level
}

fn stamp_locked(schema_name: &str, topic: &str) -> spi::Result<i32> {
    stamp(schema_name, topic, 10000, true)
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_stamper_main(_arg: pg_sys::Datum) {
    topic_worker(
        "stamper",
        "min_durability <> 'replicated'",
        stamp_locked,
        true,
    )
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_replicated_stamper_main(_arg: pg_sys::Datum) {
    topic_worker(
        "replicated stamper",
        "min_durability = 'replicated'",
        stamp_locked,
        true,
    )
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::tests::{error_of, one, tenant};
    use pgrx::prelude::*;

    #[pg_test]
    fn stamp_is_gap_free_per_band() {
        Spi::run("SELECT topic.create_topic('public.gap_q', 2)").unwrap();
        Spi::run("SELECT topic.publish('public.gap_q', jsonb_build_object('i', i)) FROM generate_series(1, 10) i").unwrap();
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'gap_q')"),
            Some(10)
        );
        assert_eq!(
            one::<String>("SELECT current_setting('enable_seqscan') || current_setting('jit')"),
            Some("onon".into())
        );
        for band in 0..2 {
            assert_eq!(
                one::<Vec<i64>>(&format!(
                    "SELECT array_agg(log_offset ORDER BY log_offset) FROM public.gap_q WHERE band = {band}"
                )),
                Some(vec![0, 1, 2, 3, 4])
            );
            assert_eq!(
                one::<i64>(&format!(
                    "SELECT next_offset FROM topic.topic_band_position
                     WHERE schema_name = 'public' AND topic = 'gap_q' AND band = {band}"
                )),
                Some(5)
            );
        }
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'gap_q')"),
            Some(0)
        );
    }

    #[pg_test]
    fn stamp_runs_tenant_code_as_owner() {
        tenant("pgt_owner");
        Spi::run("SET LOCAL ROLE pgt_owner").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_owner.own_q', 1);
             CREATE TABLE pgt_owner.seen (who name);
             CREATE FUNCTION pgt_owner.record() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN INSERT INTO pgt_owner.seen VALUES (current_user); RETURN NEW; END $$;
             CREATE TRIGGER record BEFORE UPDATE ON pgt_owner.own_q FOR EACH ROW EXECUTE FUNCTION pgt_owner.record();
             SELECT topic.publish('pgt_owner.own_q', '{}');",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            one::<bool>(
                "SELECT has_function_privilege('pgt_owner', 'topic.stamp_topic(text, text, int)', 'EXECUTE')"
            ),
            Some(false)
        );
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('pgt_owner', 'own_q')"),
            Some(1)
        );
        assert_eq!(
            one::<Vec<String>>("SELECT array_agg(who::text) FROM pgt_owner.seen"),
            Some(vec!["pgt_owner".into()])
        );

        Spi::run(
            "CREATE FUNCTION pgt_owner.escape() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RESET ROLE; RETURN NEW; END $$;
             CREATE TRIGGER escape BEFORE UPDATE ON pgt_owner.own_q FOR EACH ROW EXECUTE FUNCTION pgt_owner.escape();
             SET LOCAL ROLE pgt_owner;
             SELECT topic.publish('pgt_owner.own_q', '{}');
             RESET ROLE;",
        )
        .unwrap();
        let escaped = error_of("SELECT topic.stamp_topic('pgt_owner', 'own_q')");
        assert!(
            escaped
                .as_deref()
                .is_some_and(|e| e.contains("cannot set parameter \"role\"")),
            "{escaped:?}"
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pgt_owner.own_q WHERE log_offset IS NULL"),
            Some(1)
        );
    }

    #[pg_test]
    fn stamp_blocks_tenant_create_topic() {
        tenant("pgt_evil");
        Spi::run("SET LOCAL ROLE pgt_evil").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_evil.e_q', 1);
             CREATE FUNCTION pgt_evil.squat() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN PERFORM topic.create_topic('public.squat_q', 1); RETURN NEW; END $$;
             CREATE TRIGGER squat BEFORE UPDATE ON pgt_evil.e_q FOR EACH ROW EXECUTE FUNCTION pgt_evil.squat();
             SELECT topic.publish('pgt_evil.e_q', '{}');",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        let refused = error_of("SELECT topic.stamp_topic('pgt_evil', 'e_q')");
        assert!(
            refused
                .as_deref()
                .is_some_and(|e| e.contains("security-restricted operation")),
            "{refused:?}"
        );
        assert_eq!(
            one::<bool>("SELECT to_regclass('public.squat_q') IS NULL"),
            Some(true)
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pgt_evil.e_q WHERE log_offset IS NULL"),
            Some(1)
        );
    }

    #[pg_test]
    fn stamp_refuses_skipped_row() {
        tenant("pgt_skip");
        Spi::run("SET LOCAL ROLE pgt_skip").unwrap();
        Spi::run(
            r#"SELECT topic.create_topic('pgt_skip.s_q', 1);
             CREATE FUNCTION pgt_skip.skip() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN IF OLD.value ? 'skip' THEN RETURN NULL; END IF; RETURN NEW; END $$;
             CREATE TRIGGER skip BEFORE UPDATE ON pgt_skip.s_q FOR EACH ROW EXECUTE FUNCTION pgt_skip.skip();
             SELECT topic.publish('pgt_skip.s_q', '{}');
             SELECT topic.publish('pgt_skip.s_q', '{"skip": 1}');
             SELECT topic.publish('pgt_skip.s_q', '{}');"#,
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        let refused = error_of("SELECT topic.stamp_topic('pgt_skip', 's_q')");
        assert!(
            refused.as_deref().is_some_and(|e| e.contains("offsets")),
            "{refused:?}"
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pgt_skip.s_q WHERE log_offset IS NULL"),
            Some(3)
        );
    }

    #[pg_test]
    fn stamp_refuses_forced_rls() {
        tenant("pgt_rls");
        Spi::run("SET LOCAL ROLE pgt_rls").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_rls.hidden_q', 1);
             SELECT topic.publish('pgt_rls.hidden_q', '{}') FROM generate_series(1, 3);
             ALTER TABLE pgt_rls.hidden_q ENABLE ROW LEVEL SECURITY;
             ALTER TABLE pgt_rls.hidden_q FORCE ROW LEVEL SECURITY;
             CREATE POLICY hide ON pgt_rls.hidden_q USING (false);",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        assert!(error_of("SELECT topic.stamp_topic('pgt_rls', 'hidden_q')")
            .unwrap()
            .contains("row-level security"));
        assert_eq!(
            one::<Vec<i64>>("SELECT ARRAY[count(*), count(log_offset)] FROM pgt_rls.hidden_q"),
            Some(vec![3, 0])
        );
    }
}
