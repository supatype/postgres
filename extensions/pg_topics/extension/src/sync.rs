use std::panic::AssertUnwindSafe;

use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::spi::{self, quote_identifier, quote_literal, quote_qualified_identifier};

use crate::{as_owner, caller, queue_owner, read_one, text_arg};

const SAVEPOINTS: usize = 50;

struct Target {
    enabled: bool,
    errors_owner: Option<pg_sys::Oid>,
    key: Option<String>,
    base: Option<(pg_sys::Oid, pg_sys::Oid, String)>,
}

fn sync_target(who: &str, schema_name: &str, topic: &str) -> spi::Result<Target> {
    Spi::connect(|client| {
        let row = client
            .select(
                "SELECT t.sync_enabled,
                        (SELECT e.relowner FROM pg_catalog.pg_class e
                         WHERE e.oid = pg_catalog.to_regclass(pg_catalog.format('%I.%I', t.schema_name, t.topic || 'e'))),
                        t.sync_key, c.oid, c.relowner, c.relkind::text,
                        c.oid::pg_catalog.regclass::text,
                        EXISTS (SELECT FROM pg_catalog.pg_attribute a WHERE a.attrelid = c.oid
                                AND a.attname = t.sync_key AND a.attnum > 0 AND NOT a.attisdropped)
                 FROM topic.topic_config t LEFT JOIN pg_catalog.pg_class c ON c.oid = t.sync_table
                 WHERE t.schema_name = $1 AND t.topic = $2",
                Some(1),
                Some(vec![text_arg(schema_name), text_arg(topic)]),
            )?
            .first();
        if row.is_empty() {
            error!("{who}: topic {schema_name}.{topic} does not exist");
        }
        let name = row.get::<String>(7)?.unwrap_or_default();
        let base = match (row.get::<String>(6)?.as_deref(), row.get::<bool>(8)?) {
            (Some("r" | "p"), Some(true)) => Some((
                row.get::<pg_sys::Oid>(5)?.unwrap_or(pg_sys::InvalidOid),
                row.get::<pg_sys::Oid>(4)?.unwrap_or(pg_sys::InvalidOid),
                name,
            )),
            (Some("r" | "p"), _) | (None, _) => None,
            (Some(_), _) => error!("{who}: {name} is not an ordinary table"),
        };
        Ok(Target {
            enabled: row.get::<bool>(1)?.unwrap_or(false),
            errors_owner: row.get::<pg_sys::Oid>(2)?,
            key: row.get::<String>(3)?,
            base,
        })
    })
}

fn columns(base: pg_sys::Oid) -> spi::Result<Vec<String>> {
    Ok(read_one::<Vec<String>>(
        "SELECT coalesce(array_agg(attname::text ORDER BY attnum), '{}') FROM pg_catalog.pg_attribute
         WHERE attrelid = $1 AND attnum > 0 AND NOT attisdropped AND attgenerated = ''
           AND attidentity <> 'a' AND attname <> 'event_at'",
        vec![(PgBuiltInOids::OIDOID.oid(), base.into_datum())],
    )?
    .unwrap_or_default())
}

fn merge_sql(base: &str, key: &str, columns: &[String], source: &str) -> String {
    let k = quote_identifier(key);
    let quoted: Vec<String> = columns.iter().map(quote_identifier).collect();
    let set = columns
        .iter()
        .zip(&quoted)
        .map(|(c, q)| {
            format!(
                "{q} = CASE WHEN r.value ? {} THEN (r.p).{q} ELSE t.{q} END",
                quote_literal(c)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let values = quoted
        .iter()
        .map(|q| format!("(r.p).{q}"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "WITH r AS (
             SELECT DISTINCT ON ((s.p).{k}) s.* FROM (
                 SELECT u.band, u.log_offset, u.value, u.published_at,
                        pg_catalog.jsonb_populate_record(NULL::{base},
                            coalesce(u.value, pg_catalog.jsonb_build_object({lit}, u.key))) AS p
                 FROM {source} u) s
             ORDER BY (s.p).{k}, s.published_at DESC, s.band DESC, s.log_offset DESC)
         MERGE INTO {base} t USING r ON t.{k} = (r.p).{k}
         WHEN MATCHED AND r.value IS NULL AND t.event_at <= r.published_at THEN DELETE
         WHEN MATCHED AND t.event_at <= r.published_at THEN UPDATE SET {set}, event_at = r.published_at
         WHEN NOT MATCHED AND r.value IS NOT NULL THEN INSERT ({names}, event_at) VALUES ({values}, r.published_at)",
        lit = quote_literal(key),
        names = quoted.join(", "),
    )
}

fn bad_record(code: PgSqlErrorCode) -> bool {
    [
        PgSqlErrorCode::ERRCODE_DATA_EXCEPTION,
        PgSqlErrorCode::ERRCODE_INTEGRITY_CONSTRAINT_VIOLATION,
    ]
    .iter()
    .any(|class| (*class as isize ^ code as isize) & 0xFFF == 0)
}

fn savepoint<R>(f: impl FnOnce() -> spi::Result<R>) -> Result<R, String> {
    let (cx, owner) = unsafe { (pg_sys::CurrentMemoryContext, pg_sys::CurrentResourceOwner) };
    let restore = move || unsafe {
        pg_sys::MemoryContextSwitchTo(cx);
        pg_sys::CurrentResourceOwner = owner;
    };
    unsafe {
        pg_sys::BeginInternalSubTransaction(std::ptr::null());
        pg_sys::MemoryContextSwitchTo(cx);
    }
    PgTryBuilder::new(AssertUnwindSafe(|| {
        let r = f().unwrap_or_else(|e| error!("{e}"));
        unsafe { pg_sys::ReleaseCurrentSubTransaction() };
        restore();
        Ok(r)
    }))
    .catch_others(|e| {
        restore();
        unsafe {
            pg_sys::FlushErrorState();
            pg_sys::RollbackAndReleaseCurrentSubTransaction();
        }
        restore();
        let (CaughtError::PostgresError(report)
        | CaughtError::ErrorReport(report)
        | CaughtError::RustPanic {
            ereport: report, ..
        }) = e;
        if !bad_record(report.sql_error_code()) {
            ereport!(ERROR, report.sql_error_code(), report.message());
        }
        Err(report.message().to_string())
    })
    .execute()
}

fn lock(schema_name: &str, topic: &str, wait: bool) -> spi::Result<bool> {
    let sql = format!(
        "SELECT EXISTS (SELECT FROM topic.topic_groups WHERE group_name = $1 FOR NO KEY UPDATE{})",
        if wait { "" } else { " SKIP LOCKED" }
    );
    Ok(Spi::get_one_with_args::<bool>(
        &sql,
        vec![text_arg(&format!("__pg_topics_sync:{schema_name}.{topic}"))],
    )? == Some(true))
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn sync_topic(schema_name: &str, topic: &str, max_rows: default!(i32, 1000)) -> spi::Result<i32> {
    let reader = queue_owner("topic.sync_topic", schema_name, topic)?;
    let target = sync_target("topic.sync_topic", schema_name, topic)?;
    if !target.enabled {
        return Ok(0);
    }
    let errors_owner = target.errors_owner;
    let Some((writer, base, base_name)) = target
        .base
        .filter(|(owner, _, _)| errors_owner == Some(*owner))
    else {
        Spi::run_with_args(
            "UPDATE topic.topic_config SET sync_enabled = false WHERE schema_name = $1 AND topic = $2",
            Some(vec![text_arg(schema_name), text_arg(topic)]),
        )?;
        warning!(
            "topic.sync_topic: {schema_name}.{topic} stops syncing, because its base table or the column {} is gone, or its error table has another owner or is gone",
            target.key.unwrap_or_default()
        );
        return Ok(0);
    };
    let group = format!("__pg_topics_sync:{schema_name}.{topic}");
    let names = || vec![text_arg(schema_name), text_arg(topic), text_arg(&group)];
    let read_positions = || {
        Spi::connect(|client| {
            client
            .select(
                "SELECT array_agg(o.band ORDER BY o.band), array_agg(o.committed_offset ORDER BY o.band),
                        coalesce(bool_or(o.committed_offset < p.next_offset), false)
                 FROM topic.topic_offsets o JOIN topic.topic_band_position p
                   ON p.schema_name = o.schema_name AND p.topic = o.topic AND p.band = o.band
                 WHERE o.schema_name = $1 AND o.topic = $2 AND o.group_name = $3",
                Some(1),
                Some(names()),
            )?
            .first()
            .get_three::<Vec<i16>, Vec<i64>, bool>()
        })
    };
    if read_positions()?.2 != Some(true) || !lock(schema_name, topic, false)? {
        return Ok(0);
    }
    let (bands, positions, ahead) = read_positions()?;
    if ahead != Some(true) {
        return Ok(0);
    }
    let cols = columns(base)?;
    let key = target.key.unwrap_or_default();
    let queue = quote_qualified_identifier(schema_name, topic);
    let records = as_owner(reader, || {
        read_one::<Vec<String>>(
            &format!(
                "SELECT coalesce(array_agg(pg_catalog.to_jsonb(b)::text ORDER BY b.log_offset, b.band), '{{}}')
                 FROM (SELECT x.* FROM unnest($1::smallint[], $2::bigint[]) p(band, pos)
                       CROSS JOIN LATERAL (
                           SELECT q.band, q.log_offset, q.seq, q.key, q.value::text AS value, q.headers,
                                  q.published_by, q.published_at, q.producer_timestamp
                           FROM {queue} q WHERE q.band = p.band AND q.log_offset >= p.pos
                           ORDER BY q.log_offset LIMIT $3) x
                       ORDER BY x.log_offset, x.band LIMIT $3) b"
            ),
            vec![
                (PgBuiltInOids::INT2ARRAYOID.oid(), bands.into_datum()),
                (PgBuiltInOids::INT8ARRAYOID.oid(), positions.into_datum()),
                (PgBuiltInOids::INT4OID.oid(), max_rows.into_datum()),
            ],
        )
    })?
    .unwrap_or_default();
    if records.is_empty() {
        return Ok(0);
    }
    let merge = merge_sql(
        &base_name,
        &key,
        &cols,
        "(SELECT band, log_offset, key, value::jsonb AS value, published_at
          FROM pg_catalog.jsonb_to_recordset($1::jsonb)
               AS x(band smallint, log_offset bigint, key text, value text, published_at timestamptz))",
    );
    let apply = |batch: &[String]| {
        let json = format!("[{}]", batch.join(","));
        savepoint(|| {
            as_owner(writer, || {
                Spi::run_with_args(&merge, Some(vec![text_arg(&json)]))
            })
        })
    };
    let done = if apply(&records).is_ok() {
        records.len()
    } else {
        let done = records.len().min(SAVEPOINTS);
        let (failed, errors): (Vec<String>, Vec<String>) = records[..done]
            .iter()
            .filter_map(|r| apply(std::slice::from_ref(r)).err().map(|e| (r.clone(), e)))
            .unzip();
        if !failed.is_empty() {
            let errors_table = quote_qualified_identifier(schema_name, &format!("{topic}e"));
            as_owner(writer, || {
                Spi::run_with_args(
                    &format!(
                        "INSERT INTO {errors_table} (band, log_offset, seq, key, value, headers, published_by,
                                                     published_at, producer_timestamp, error)
                         SELECT u.band, u.log_offset, u.seq, u.key, u.value::jsonb, u.headers, u.published_by,
                                u.published_at, u.producer_timestamp, e.error
                         FROM unnest($1::text[], $2::text[]) e(rec, error),
                              pg_catalog.jsonb_to_record(e.rec::jsonb) AS u(
                                  band smallint, log_offset bigint, seq bigint, key text, value text,
                                  headers jsonb, published_by name, published_at timestamptz,
                                  producer_timestamp timestamptz)
                         ON CONFLICT (band, log_offset)
                         DO UPDATE SET error = EXCLUDED.error, failed_at = EXCLUDED.failed_at"
                    ),
                    Some(vec![
                        (PgBuiltInOids::TEXTARRAYOID.oid(), failed.into_datum()),
                        (PgBuiltInOids::TEXTARRAYOID.oid(), errors.into_datum()),
                    ]),
                )
            })?;
        }
        done
    };
    let mut args = names();
    args.push(text_arg(&format!("[{}]", records[..done].join(","))));
    Spi::run_with_args(
        "UPDATE topic.topic_offsets o SET committed_offset = n.next
         FROM (SELECT u.band, max(u.log_offset) + 1 AS next
               FROM pg_catalog.jsonb_to_recordset($4::jsonb) AS u(band smallint, log_offset bigint)
               GROUP BY u.band) n
         WHERE o.schema_name = $1 AND o.topic = $2 AND o.group_name = $3 AND o.band = n.band",
        Some(args),
    )?;
    Ok(done as i32)
}

#[pg_extern(security_definer)]
#[search_path(pg_catalog, pg_temp)]
fn retry_errors(
    topic: &str,
) -> spi::Result<TableIterator<'static, (name!(retried, i32), name!(failed, i32))>> {
    let who = caller();
    let (schema_name, topic) = topic.split_once('.').unwrap_or((topic, ""));
    if read_one::<bool>(
        "SELECT EXISTS (SELECT FROM topic.topic_config t JOIN pg_catalog.pg_class c ON c.oid = t.sync_table
                        WHERE t.schema_name = $2 AND t.topic = $3
                          AND pg_catalog.pg_has_role($1::name, c.relowner, 'USAGE'))",
        vec![text_arg(&who), text_arg(schema_name), text_arg(topic)],
    )? != Some(true)
    {
        ereport!(
            ERROR,
            PgSqlErrorCode::ERRCODE_INSUFFICIENT_PRIVILEGE,
            format!(
                "topic.retry_errors: role {who} may not retry the errors of {schema_name}.{topic}"
            )
        );
    }
    let target = sync_target("topic.retry_errors", schema_name, topic)?;
    let Some((writer, base, base_name)) = target.base else {
        error!(
            "topic.retry_errors: {schema_name}.{topic} has no base table with its sync key column"
        );
    };
    lock(schema_name, topic, true)?;
    read_one::<String>(
        "SELECT pg_catalog.set_config('row_security', 'off', true)",
        vec![],
    )?;
    let cols = columns(base)?;
    let errors_table = quote_qualified_identifier(schema_name, &format!("{topic}e"));
    let merge = merge_sql(
        &base_name,
        &target.key.unwrap_or_default(),
        &cols,
        &format!(
            "(SELECT band, log_offset, key, value, published_at FROM {errors_table}
              WHERE band = $1 AND log_offset = $2)"
        ),
    );
    let (retried, failed) = as_owner(writer, || -> spi::Result<(i32, i32)> {
        let rows = Spi::connect(|client| {
            client
                .select(
                    &format!(
                        "SELECT band, log_offset FROM {errors_table}
                         ORDER BY failed_at, band, log_offset LIMIT {SAVEPOINTS}"
                    ),
                    None,
                    None,
                )?
                .map(|row| {
                    Ok((
                        row.get::<i16>(1)?.unwrap_or(0),
                        row.get::<i64>(2)?.unwrap_or(0),
                    ))
                })
                .collect::<spi::Result<Vec<_>>>()
        })?;
        let mut ok: (Vec<i16>, Vec<i64>) = Default::default();
        let mut bad: (Vec<i16>, Vec<i64>, Vec<String>) = Default::default();
        for (band, offset) in rows {
            let args = vec![
                (PgBuiltInOids::INT2OID.oid(), band.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), offset.into_datum()),
            ];
            match savepoint(|| Spi::run_with_args(&merge, Some(args))) {
                Ok(()) => {
                    ok.0.push(band);
                    ok.1.push(offset);
                }
                Err(e) => {
                    bad.0.push(band);
                    bad.1.push(offset);
                    bad.2.push(e);
                }
            }
        }
        let counts = (ok.0.len() as i32, bad.0.len() as i32);
        Spi::run_with_args(
            &format!(
                "DELETE FROM {errors_table} q USING unnest($1::smallint[], $2::bigint[]) d(band, log_offset)
                 WHERE q.band = d.band AND q.log_offset = d.log_offset"
            ),
            Some(vec![
                (PgBuiltInOids::INT2ARRAYOID.oid(), ok.0.into_datum()),
                (PgBuiltInOids::INT8ARRAYOID.oid(), ok.1.into_datum()),
            ]),
        )?;
        Spi::run_with_args(
            &format!(
                "UPDATE {errors_table} q SET error = e.error, failed_at = pg_catalog.now()
                 FROM unnest($1::smallint[], $2::bigint[], $3::text[]) e(band, log_offset, error)
                 WHERE q.band = e.band AND q.log_offset = e.log_offset"
            ),
            Some(vec![
                (PgBuiltInOids::INT2ARRAYOID.oid(), bad.0.into_datum()),
                (PgBuiltInOids::INT8ARRAYOID.oid(), bad.1.into_datum()),
                (PgBuiltInOids::TEXTARRAYOID.oid(), bad.2.into_datum()),
            ]),
        )?;
        Ok(counts)
    })?;
    Ok(TableIterator::once((retried, failed)))
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::tests::{error_of, one, tenant};
    use pgrx::prelude::*;

    const U1: &str = "00000000-0000-0000-0000-000000000001";
    const U2: &str = "00000000-0000-0000-0000-000000000002";
    const U3: &str = "00000000-0000-0000-0000-000000000003";
    const U4: &str = "00000000-0000-0000-0000-000000000004";
    const GROUP: &str = "__pg_topics_sync:public.bottles_q";

    fn bottles() {
        assert_eq!(
            one::<String>(
                r#"SELECT topic.create_table_topic('public.bottles',
                   '{"bottle_id": "uuid", "name": "text", "abv": "numeric(4,2)"}', 'bottle_id')"#
            ),
            Some("public.bottles_q".into())
        );
        Spi::run("ALTER TABLE public.bottles ALTER COLUMN name SET NOT NULL").unwrap();
    }

    fn publish(value: &str) {
        Spi::run(&format!(
            "SELECT topic.publish('public.bottles_q', {value})"
        ))
        .unwrap();
    }

    fn sync() -> Option<i32> {
        Spi::run("SELECT topic.stamp_topic('public', 'bottles_q')").unwrap();
        one::<i32>("SELECT topic.sync_topic('public', 'bottles_q')")
    }

    fn row(id: &str) -> Option<String> {
        one::<String>(&format!(
            "SELECT coalesce((SELECT concat_ws('|', name, abv) FROM public.bottles WHERE bottle_id = '{id}'), 'none')"
        ))
    }

    fn position() -> Option<i64> {
        one::<i64>(&format!(
            "SELECT sum(committed_offset)::bigint FROM topic.topic_offsets WHERE group_name = '{GROUP}'"
        ))
    }

    #[pg_test]
    fn sync_upserts_and_ignores_extra_fields() {
        bottles();
        assert_eq!(
            one::<Vec<String>>(
                "SELECT array_agg(attname || ' ' || format_type(atttypid, atttypmod) ORDER BY attnum)
                 FROM pg_attribute WHERE attrelid = 'public.bottles'::regclass AND attnum > 0"
            ),
            Some(
                ["bottle_id uuid", "name text", "abv numeric(4,2)", "event_at timestamp with time zone"]
                    .map(String::from)
                    .to_vec()
            )
        );
        publish(&format!(
            r#"'{{"bottle_id": "{U1}", "name": "Ardbeg", "abv": 46, "region": "Islay"}}'"#
        ));
        publish(&format!(
            r#"'{{"bottle_id": "{U2}", "name": "Lagavulin"}}'"#
        ));
        assert_eq!(sync(), Some(2));
        assert_eq!(row(U1), Some("Ardbeg|46.00".into()));
        assert_eq!(row(U2), Some("Lagavulin".into()));
        assert_eq!(
            one::<bool>(&format!(
                "SELECT b.event_at = q.published_at FROM public.bottles b JOIN public.bottles_q q
                 ON q.value->>'bottle_id' = b.bottle_id::text WHERE b.bottle_id = '{U1}'"
            )),
            Some(true)
        );
        publish(&format!(r#"'{{"bottle_id": "{U1}", "abv": 50}}'"#));
        assert_eq!(sync(), Some(1));
        assert_eq!(row(U1), Some("Ardbeg|50.00".into()));
        assert_eq!(one::<i64>("SELECT count(*) FROM public.bottles"), Some(2));
        assert_eq!(position(), Some(3));
        assert_eq!(sync(), Some(0));
    }

    #[pg_test]
    fn sync_takes_the_newest_of_two_updates_in_one_batch() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "old"}}'"#));
        Spi::run(
            "SELECT topic.publish('public.bottles_q', jsonb_build_object('bottle_id', gen_random_uuid(), 'name', 'n'))
             FROM generate_series(1, 50)",
        )
        .unwrap();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "new"}}'"#));
        assert_eq!(sync(), Some(52));
        assert_eq!(row(U1), Some("new".into()));
        assert_eq!(one::<i64>("SELECT count(*) FROM public.bottles"), Some(51));
        assert_eq!(
            one::<i64>("SELECT count(*) FROM public.bottles_qe"),
            Some(0)
        );
    }

    #[pg_test]
    fn sync_keeps_a_newer_row_when_an_older_record_comes_later() {
        bottles();
        Spi::run(&format!(
            r#"INSERT INTO public.bottles_q (band, value) VALUES (1, '{{"bottle_id": "{U1}", "name": "new"}}')"#
        ))
        .unwrap();
        assert_eq!(sync(), Some(1));
        Spi::run(&format!(
            r#"INSERT INTO public.bottles_q (band, value, published_at)
               VALUES (0, '{{"bottle_id": "{U1}", "name": "old"}}', clock_timestamp() - interval '1 minute')"#
        ))
        .unwrap();
        assert_eq!(sync(), Some(1));
        assert_eq!(row(U1), Some("new".into()));
        assert_eq!(
            one::<bool>(&format!(
                "SELECT b.event_at = q.published_at FROM public.bottles b, public.bottles_q q
                 WHERE b.bottle_id = '{U1}' AND q.band = 1"
            )),
            Some(true)
        );
    }

    #[pg_test]
    fn sync_applies_a_later_record_with_the_same_published_at() {
        bottles();
        for name in ["first", "second"] {
            Spi::run(&format!(
                r#"INSERT INTO public.bottles_q (band, value, published_at)
                   VALUES (0, '{{"bottle_id": "{U1}", "name": "{name}"}}', now() - interval '1 minute')"#
            ))
            .unwrap();
            assert_eq!(sync(), Some(1));
        }
        assert_eq!(row(U1), Some("second".into()));
    }

    #[pg_test]
    fn sync_deletes_on_a_tombstone() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        publish(&format!(
            r#"'{{"bottle_id": "{U2}", "name": "Lagavulin"}}'"#
        ));
        assert_eq!(sync(), Some(2));
        publish(&format!("NULL, '{U1}'"));
        assert_eq!(sync(), Some(1));
        assert_eq!(row(U1), Some("none".into()));
        assert_eq!(row(U2), Some("Lagavulin".into()));
        assert_eq!(
            one::<i64>("SELECT count(*) FROM public.bottles_q WHERE value IS NULL"),
            Some(1)
        );
    }

    #[pg_test]
    fn sync_puts_bad_records_in_the_error_table() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "good"}}'"#));
        publish(&format!(r#"'{{"bottle_id": "{U2}", "abv": 40}}'"#));
        publish(&format!(
            r#"'{{"bottle_id": "{U3}", "name": "x", "abv": "strong"}}'"#
        ));
        publish(&format!(
            r#"'{{"bottle_id": "{U4}", "name": "also good"}}'"#
        ));
        assert_eq!(sync(), Some(4));
        assert_eq!(row(U1), Some("good".into()));
        assert_eq!(row(U4), Some("also good".into()));
        assert_eq!(one::<i64>("SELECT count(*) FROM public.bottles"), Some(2));
        assert_eq!(
            one::<Vec<String>>(
                "SELECT array_agg((value->>'bottle_id') || ' ' || error ORDER BY value->>'bottle_id')
                 FROM public.bottles_qe"
            ),
            Some(vec![
                format!("{U2} null value in column \"name\" of relation \"bottles\" violates not-null constraint"),
                format!("{U3} invalid input syntax for type numeric: \"strong\""),
            ])
        );
        assert_eq!(
            one::<bool>(
                "SELECT bool_and(e.seq = q.seq AND e.published_at = q.published_at AND e.published_by = q.published_by)
                 FROM public.bottles_qe e JOIN public.bottles_q q USING (band, log_offset)"
            ),
            Some(true)
        );
        assert_eq!(position(), Some(4));
    }

    #[pg_test]
    fn sync_writes_a_column_added_by_alter_table() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        assert_eq!(sync(), Some(1));
        Spi::run("ALTER TABLE public.bottles ADD COLUMN colour text").unwrap();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "colour": "amber"}}'"#));
        assert_eq!(sync(), Some(1));
        assert_eq!(
            one::<String>(&format!(
                "SELECT name || ' ' || colour FROM public.bottles WHERE bottle_id = '{U1}'"
            )),
            Some("Ardbeg amber".into())
        );
    }

    #[pg_test]
    fn sync_fills_a_column_added_to_an_inheritance_parent() {
        let sync = || {
            Spi::run("SELECT topic.stamp_topic('public', 'barrels_q')").unwrap();
            one::<i32>("SELECT topic.sync_topic('public', 'barrels_q')")
        };
        Spi::run(
            r#"CREATE TABLE public.kinds (note text);
               CREATE TABLE public.barrels (barrel_id int PRIMARY KEY, event_at timestamptz NOT NULL) INHERITS (public.kinds);
               SELECT topic.attach('public.barrels', 'barrel_id');
               SELECT topic.publish('public.barrels_q', '{"barrel_id": 1, "note": "a"}')"#,
        )
        .unwrap();
        assert_eq!(sync(), Some(1));
        Spi::run(
            r#"ALTER TABLE public.kinds ADD COLUMN colour text;
               SELECT topic.publish('public.barrels_q', '{"barrel_id": 1, "colour": "amber"}')"#,
        )
        .unwrap();
        assert_eq!(sync(), Some(1));
        assert_eq!(
            one::<String>("SELECT note || ' ' || colour FROM public.barrels"),
            Some("a amber".into())
        );
    }

    #[pg_test]
    fn sync_stops_when_the_error_table_has_another_owner() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        Spi::run("CREATE ROLE pgt_new_owner; ALTER TABLE public.bottles OWNER TO pgt_new_owner")
            .unwrap();
        assert_eq!(sync(), Some(0));
        assert_eq!(
            one::<bool>("SELECT sync_enabled FROM topic.topic_config WHERE topic = 'bottles_q'"),
            Some(false)
        );
        assert_eq!(row(U1), Some("none".into()));
    }

    #[pg_test]
    fn sync_stops_when_the_error_table_is_gone() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        Spi::run("DROP TABLE public.bottles_qe").unwrap();
        assert_eq!(sync(), Some(0));
        assert_eq!(
            one::<bool>("SELECT sync_enabled FROM topic.topic_config WHERE topic = 'bottles_q'"),
            Some(false)
        );
    }

    #[pg_test]
    fn dropping_the_base_table_stops_the_sync() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        Spi::run("DROP TABLE public.bottles").unwrap();
        assert_eq!(
            one::<bool>("SELECT sync_enabled FROM topic.topic_config WHERE topic = 'bottles_q'"),
            Some(false)
        );
        assert_eq!(sync(), Some(0));

        Spi::run(
            "CREATE TABLE public.casks (cask_id int PRIMARY KEY, event_at timestamptz NOT NULL);
             SELECT topic.attach('public.casks', 'cask_id');
             ALTER TABLE public.casks DROP COLUMN cask_id",
        )
        .unwrap();
        assert_eq!(
            one::<bool>("SELECT sync_enabled FROM topic.topic_config WHERE topic = 'casks_q'"),
            Some(false)
        );

        Spi::run("DROP TABLE public.bottles_q").unwrap();
        assert_eq!(
            one::<i64>(&format!(
                "SELECT (SELECT count(*) FROM topic.topic_config WHERE topic = 'bottles_q')
                      + (SELECT count(*) FROM topic.topic_band_position WHERE topic = 'bottles_q')
                      + (SELECT count(*) FROM topic.topic_offsets WHERE topic = 'bottles_q')
                      + (SELECT count(*) FROM topic.topic_groups WHERE group_name = '{GROUP}')"
            )),
            Some(0)
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_config WHERE topic = 'casks_q'"),
            Some(1)
        );
    }

    #[pg_test]
    fn ddl_works_when_the_event_trigger_body_fails() {
        tenant("pgt_plain");
        Spi::run("ALTER TABLE topic.topic_config RENAME COLUMN band_count TO broken").unwrap();
        Spi::run("SET LOCAL ROLE pgt_plain").unwrap();
        let created = error_of("CREATE TABLE pgt_plain.t (id int)");
        let dropped = error_of("DROP TABLE pgt_plain.t");
        Spi::run("RESET ROLE").unwrap();
        assert_eq!((created, dropped), (None, None));
    }

    #[pg_test]
    fn retry_errors_writes_fixed_rows_and_keeps_failing_ones() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U2}", "abv": 40}}'"#));
        publish(&format!(
            r#"'{{"bottle_id": "{U3}", "name": "x", "abv": "strong"}}'"#
        ));
        assert_eq!(sync(), Some(2));
        Spi::run(
            "ALTER TABLE public.bottles ALTER COLUMN name DROP NOT NULL;
             UPDATE public.bottles_qe SET failed_at = now() - interval '1 hour', error = 'old'",
        )
        .unwrap();

        Spi::run("CREATE ROLE pgt_stranger").unwrap();
        Spi::run("SET LOCAL ROLE pgt_stranger").unwrap();
        let refused = error_of("SELECT topic.retry_errors('public.bottles_q')");
        let unknown = error_of("SELECT topic.retry_errors('public.nope_q')");
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            (refused, unknown),
            (
                Some("topic.retry_errors: role pgt_stranger may not retry the errors of public.bottles_q".into()),
                Some("topic.retry_errors: role pgt_stranger may not retry the errors of public.nope_q".into())
            )
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM public.bottles_qe WHERE error = 'old'"),
            Some(2)
        );

        assert_eq!(
            one::<String>(
                "SELECT retried || ' ' || failed FROM topic.retry_errors('public.bottles_q')"
            ),
            Some("1 1".into())
        );
        assert_eq!(row(U2), Some("40.00".into()));
        assert_eq!(
            one::<String>(
                "SELECT (value->>'bottle_id') || ' ' || (failed_at > now() - interval '1 minute') || ' ' || error
                 FROM public.bottles_qe"
            ),
            Some(format!("{U3} true invalid input syntax for type numeric: \"strong\""))
        );
    }

    #[pg_test]
    fn sync_runs_tenant_code_as_owner() {
        tenant("pgt_queue");
        tenant("pgt_base");
        Spi::run("GRANT USAGE, CREATE ON SCHEMA pgt_base TO pgt_queue").unwrap();
        Spi::run("SET LOCAL ROLE pgt_queue").unwrap();
        Spi::run("SELECT topic.create_topic('pgt_base.bottles_q', 1)").unwrap();
        Spi::run("SET LOCAL ROLE pgt_base").unwrap();
        Spi::run(
            "CREATE TABLE pgt_base.bottles (bottle_id int PRIMARY KEY, name text, event_at timestamptz NOT NULL);
             CREATE TABLE pgt_base.seen (who name, super boolean);
             CREATE FUNCTION pgt_base.record() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN
                 INSERT INTO pgt_base.seen SELECT current_user, rolsuper FROM pg_roles WHERE rolname = current_user;
                 RETURN NEW;
             END $$;
             CREATE TRIGGER record BEFORE INSERT ON pgt_base.bottles FOR EACH ROW EXECUTE FUNCTION pgt_base.record();",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        Spi::run("SELECT topic.attach('pgt_base.bottles', 'bottle_id')").unwrap();
        Spi::run("SET LOCAL ROLE pgt_queue").unwrap();
        Spi::run(r#"SELECT topic.publish('pgt_base.bottles_q', '{"bottle_id": 1, "name": "a"}')"#)
            .unwrap();
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            one::<bool>("SELECT has_table_privilege('pgt_base', 'pgt_base.bottles_q', 'SELECT')"),
            Some(false)
        );
        Spi::run("SELECT topic.stamp_topic('pgt_base', 'bottles_q')").unwrap();
        assert_eq!(
            one::<i32>("SELECT topic.sync_topic('pgt_base', 'bottles_q')"),
            Some(1)
        );
        assert_eq!(
            one::<String>("SELECT string_agg(who || ' ' || super, ',') FROM pgt_base.seen"),
            Some("pgt_base false".into())
        );
        assert_eq!(
            one::<String>("SELECT pg_get_userbyid(relowner)::text FROM pg_class WHERE oid = 'pgt_base.bottles_qe'::regclass"),
            Some("pgt_base".into())
        );
    }

    #[pg_test]
    fn tenant_trigger_cannot_reset_role_during_sync() {
        tenant("pgt_sync_esc");
        Spi::run("SET LOCAL ROLE pgt_sync_esc").unwrap();
        Spi::run(
            r#"SELECT topic.create_table_topic('pgt_sync_esc.kegs', '{"keg_id": "int"}', 'keg_id', 1);
               CREATE FUNCTION pgt_sync_esc.escape() RETURNS trigger LANGUAGE plpgsql AS $$
               BEGIN RESET ROLE; RETURN NEW; END $$;
               CREATE TRIGGER escape BEFORE INSERT ON pgt_sync_esc.kegs
                   FOR EACH ROW EXECUTE FUNCTION pgt_sync_esc.escape();
               SELECT topic.publish('pgt_sync_esc.kegs_q', '{"keg_id": 1}');"#,
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        Spi::run("SELECT topic.stamp_topic('pgt_sync_esc', 'kegs_q')").unwrap();
        let escaped = error_of("SELECT topic.sync_topic('pgt_sync_esc', 'kegs_q')");
        assert!(
            escaped
                .as_deref()
                .is_some_and(|e| e.contains("cannot set parameter \"role\"")),
            "{escaped:?}"
        );
        assert_eq!(
            one::<String>(
                "SELECT (SELECT count(*) FROM pgt_sync_esc.kegs) || '/' || (SELECT count(*) FROM pgt_sync_esc.kegs_qe)"
            ),
            Some("0/0".into())
        );
    }

    #[pg_test]
    fn sync_refuses_a_view_in_place_of_the_base() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        Spi::run(
            "CREATE VIEW public.bottles_v AS SELECT * FROM public.bottles;
             UPDATE topic.topic_config SET sync_table = 'public.bottles_v' WHERE topic = 'bottles_q'",
        )
        .unwrap();
        Spi::run("SELECT topic.stamp_topic('public', 'bottles_q')").unwrap();
        for call in [
            "SELECT topic.sync_topic('public', 'bottles_q')",
            "SELECT count(*) FROM topic.retry_errors('public.bottles_q')",
        ] {
            let refused = error_of(call);
            assert!(
                refused
                    .as_deref()
                    .is_some_and(|e| e.contains("public.bottles_v is not an ordinary table")),
                "{call}: {refused:?}"
            );
        }
    }

    #[pg_test]
    fn sync_fails_the_batch_on_an_error_that_is_not_about_the_record() {
        bottles();
        publish(&format!(r#"'{{"bottle_id": "{U1}", "name": "Ardbeg"}}'"#));
        Spi::run(
            "CREATE FUNCTION public.refuse() RETURNS trigger LANGUAGE plpgsql AS $$
             BEGIN RAISE EXCEPTION 'refused by the test'; END $$;
             CREATE TRIGGER refuse BEFORE INSERT ON public.bottles FOR EACH ROW EXECUTE FUNCTION public.refuse();
             SELECT topic.stamp_topic('public', 'bottles_q')",
        )
        .unwrap();
        assert_eq!(
            error_of("SELECT topic.sync_topic('public', 'bottles_q')"),
            Some("refused by the test".into())
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM public.bottles_qe"),
            Some(0)
        );
        assert_eq!(position(), Some(0));
    }

    #[pg_test]
    fn sync_fallback_caps_savepoints() {
        bottles();
        Spi::run(
            "SELECT topic.publish('public.bottles_q', jsonb_build_object('bottle_id', gen_random_uuid()))
             FROM generate_series(1, 200)",
        )
        .unwrap();
        assert_eq!(sync(), Some(50));
        assert_eq!(
            one::<i64>("SELECT count(*) FROM public.bottles_qe"),
            Some(50)
        );
        assert_eq!(position(), Some(50));
        assert_eq!(sync(), Some(50));
        assert_eq!(position(), Some(100));
    }

    #[pg_test]
    fn attach_starts_at_oldest_offset() {
        Spi::run(
            "SELECT topic.create_topic('public.casks_q', 2);
             UPDATE topic.topic_band_position SET oldest_offset = 5, next_offset = 7 WHERE topic = 'casks_q' AND band = 1;
             CREATE TABLE public.casks (cask_id int PRIMARY KEY, event_at timestamptz NOT NULL)",
        )
        .unwrap();
        assert_eq!(
            one::<String>("SELECT topic.attach('public.casks', 'cask_id')"),
            Some("public.casks_q".into())
        );
        assert_eq!(
            one::<Vec<i64>>(
                "SELECT array_agg(committed_offset ORDER BY band) FROM topic.topic_offsets
                 WHERE group_name = '__pg_topics_sync:public.casks_q' AND owner_role = current_user"
            ),
            Some(vec![0, 5])
        );
        assert!(error_of("SELECT topic.attach('public.casks', 'nope')")
            .unwrap()
            .contains("has no column nope"));
        assert!(error_of(
            r#"SELECT topic.create_table_topic('public.inj', '{"a": "int; DROP TABLE public.casks"}', 'a')"#
        )
        .is_some());
        assert_eq!(
            one::<bool>("SELECT to_regclass('public.inj') IS NULL AND to_regclass('public.casks') IS NOT NULL"),
            Some(true)
        );
        Spi::run("CREATE TABLE public.loose (id int, event_at timestamptz NOT NULL)").unwrap();
        assert!(error_of("SELECT topic.attach('public.loose', 'id')")
            .unwrap()
            .contains("column id of public.loose allows NULL"));
        Spi::run("CREATE TABLE public.plain (id int NOT NULL)").unwrap();
        assert!(error_of("SELECT topic.attach('public.plain', 'id')")
            .unwrap()
            .contains("event_at timestamptz NOT NULL"));
    }

    #[pg_test]
    fn attach_reuses_a_matching_error_table_left_by_drop_topic() {
        Spi::run(
            "CREATE TABLE public.kegs (keg_id int PRIMARY KEY, event_at timestamptz NOT NULL);
             SELECT topic.attach('public.kegs', 'keg_id')",
        )
        .unwrap();
        Spi::run(
            "INSERT INTO public.kegs_qe (band, log_offset, seq, key, value, headers, published_by,
                                         published_at, failed_at, error)
             VALUES (0, 0, 0, NULL, NULL, NULL, current_user, now(), now(), 'boom')",
        )
        .unwrap();
        Spi::run("SELECT topic.drop_topic('public.kegs_q')").unwrap();
        assert_eq!(
            one::<String>("SELECT topic.attach('public.kegs', 'keg_id')"),
            Some("public.kegs_q".into())
        );
        assert_eq!(one::<i64>("SELECT count(*) FROM public.kegs_qe"), Some(1));
    }

    #[pg_test]
    fn attach_refuses_a_mismatched_error_table_left_by_drop_topic() {
        Spi::run(
            "CREATE TABLE public.casks2 (cask_id int PRIMARY KEY, event_at timestamptz NOT NULL);
             SELECT topic.attach('public.casks2', 'cask_id');
             SELECT topic.drop_topic('public.casks2_q');
             DROP TABLE public.casks2_qe;
             CREATE TABLE public.casks2_qe (band int)",
        )
        .unwrap();
        assert!(error_of("SELECT topic.attach('public.casks2', 'cask_id')")
            .unwrap()
            .contains("does not match the error table"));
    }
}
