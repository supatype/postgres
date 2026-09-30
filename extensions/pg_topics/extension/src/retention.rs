use std::ffi::CStr;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::datum::Interval;
use pgrx::prelude::*;
use pgrx::spi::{self, quote_qualified_identifier};

use crate::{as_owner, in_transaction, queue_owner, read_one, text_arg, wait_latch};

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn retention_check(detached: pg_sys::Oid) -> spi::Result<i64> {
    let (schema_name, topic) = Spi::connect(|client| {
        client
            .select(
                "SELECT schema_name, topic FROM topic.topic_config WHERE detaching = $1",
                Some(1),
                Some(vec![(PgBuiltInOids::OIDOID.oid(), detached.into_datum())]),
            )?
            .first()
            .get_two::<String, String>()
    })?;
    let (Some(schema_name), Some(topic)) = (schema_name, topic) else {
        error!(
            "topic.retention_check: {} is not a table that retention detaches",
            detached.as_u32()
        );
    };
    let owner = queue_owner("topic.retention_check", &schema_name, &topic)?;
    let table = read_one::<String>(
        "SELECT $1::pg_catalog.regclass::text",
        vec![(PgBuiltInOids::OIDOID.oid(), detached.into_datum())],
    )?
    .unwrap_or_default();
    as_owner(owner, || {
        read_one::<i64>(
            &format!("SELECT count(*) FROM {table} WHERE log_offset IS NULL"),
            vec![],
        )
    })
    .map(Option::unwrap_or_default)
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn retention_floor(
    schema_name: &str,
    topic: &str,
) -> spi::Result<TableIterator<'static, (name!(band, i16), name!(floor, i64))>> {
    let owner = queue_owner("topic.retention_floor", schema_name, topic)?;
    let (bands, next) = Spi::get_two_with_args::<Vec<i16>, Vec<i64>>(
        "SELECT array_agg(band ORDER BY band), array_agg(next_offset ORDER BY band)
         FROM (SELECT band, next_offset FROM topic.topic_band_position
               WHERE schema_name = $1 AND topic = $2 FOR NO KEY UPDATE) b",
        vec![text_arg(schema_name), text_arg(topic)],
    )?;
    let (bands, next) = (bands.unwrap_or_default(), next.unwrap_or_default());
    let queue = quote_qualified_identifier(schema_name, topic);
    let lowest = as_owner(owner, || {
        read_one::<Vec<Option<i64>>>(
            &format!(
                "SELECT array_agg((SELECT min(q.log_offset) FROM {queue} q
                                   WHERE q.band = b AND q.log_offset IS NOT NULL) ORDER BY b)
                 FROM unnest($1::smallint[]) b"
            ),
            vec![(
                PgBuiltInOids::INT2ARRAYOID.oid(),
                bands.clone().into_datum(),
            )],
        )
    })?
    .unwrap_or_default();
    Ok(TableIterator::new(
        bands
            .into_iter()
            .zip(next)
            .zip(lowest)
            .map(|((band, next), low)| (band, low.unwrap_or(next))),
    ))
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn check_duplicates(
    schema_name: &str,
    topic: &str,
    full: default!(bool, false),
) -> spi::Result<
    TableIterator<'static, (name!(band, i16), name!(log_offset, i64), name!(copies, i64))>,
> {
    let owner = queue_owner("topic.check_duplicates", schema_name, topic)?;
    let interval = read_one::<Interval>(
        "SELECT partition_interval FROM topic.topic_config WHERE schema_name = $1 AND topic = $2",
        vec![text_arg(schema_name), text_arg(topic)],
    )?;
    let queue = quote_qualified_identifier(schema_name, topic);
    let sql = if full {
        format!(
            "SELECT q.band, q.log_offset, count(*) FROM {queue} q WHERE q.log_offset IS NOT NULL
             GROUP BY q.band, q.log_offset HAVING count(*) > 1 ORDER BY 1, 2"
        )
    } else {
        format!(
            "SELECT DISTINCT d.band, d.log_offset, d.copies FROM (
                 SELECT r.band, r.log_offset,
                        (SELECT count(*) FROM {queue} q WHERE q.band = r.band AND q.log_offset = r.log_offset) AS copies
                 FROM {queue} r
                 WHERE r.log_offset IS NOT NULL
                   AND r.published_at >= pg_catalog.date_bin($1, pg_catalog.now(), timestamptz '2000-01-01 00:00:00+00')) d
             WHERE d.copies > 1 ORDER BY 1, 2"
        )
    };
    let rows = as_owner(owner, || {
        Spi::connect(|client| {
            client
                .select(
                    &sql,
                    None,
                    Some(vec![(
                        PgBuiltInOids::INTERVALOID.oid(),
                        interval.into_datum(),
                    )]),
                )?
                .map(|row| {
                    Ok((
                        row.get::<i16>(1)?.unwrap_or_default(),
                        row.get::<i64>(2)?.unwrap_or_default(),
                        row.get::<i64>(3)?.unwrap_or_default(),
                    ))
                })
                .collect::<spi::Result<Vec<_>>>()
        })
    })?;
    Ok(TableIterator::new(rows))
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_partition_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    let database = BackgroundWorker::get_extra();
    BackgroundWorker::connect_worker_to_spi(Some(database), None);
    let user = in_transaction(|| {
        read_one::<String>(
            "SELECT rolname::text FROM pg_catalog.pg_authid WHERE oid = 10",
            vec![],
        )
    })
    .unwrap_or_default();
    let sockets = unsafe { CStr::from_ptr(pg_sys::Unix_socket_directories) }.to_string_lossy();
    let socket = sockets.split(',').next().unwrap_or_default().trim();
    let port = unsafe { pg_sys::PostPortNumber } as u16;
    let mut client = match pgt::partitions::connect(socket, port, &user, database) {
        Ok(client) => client,
        Err(e) => {
            warning!(
                "pg_topics partition worker: cannot connect to {database} over the socket in {socket:?}: {}. The worker tries again in 5 s.",
                pgt::partitions::message(&e)
            );
            unsafe { pg_sys::proc_exit(1) }
        }
    };
    let mut ticks: u64 = 0;
    loop {
        let result = pgt::partitions::tick(&mut client, ticks.is_multiple_of(360), &mut |m| {
            warning!("pg_topics partition worker: {m}")
        });
        if let Err(e) = result {
            warning!(
                "pg_topics partition worker: {}. The worker tries again in 5 s.",
                pgt::partitions::message(&e)
            );
            break;
        }
        ticks += 1;
        unsafe { pg_sys::pgstat_report_stat(false) };
        if !wait_latch(10_000) {
            break;
        }
    }
    unsafe { pg_sys::proc_exit(1) }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use crate::tests::one;
    use pgrx::prelude::*;

    #[pg_test]
    fn check_duplicates_finds_cross_partition_copy() {
        Spi::run("SELECT topic.create_topic('public.dup_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run("SELECT topic.publish('public.dup_q', '{}') FROM generate_series(1, 3)").unwrap();
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'dup_q')"),
            Some(3)
        );
        let found = |full: bool| {
            one::<Vec<String>>(&format!(
                "SELECT coalesce(array_agg(band || ':' || log_offset || ':' || copies), '{{}}')
                 FROM topic.check_duplicates('public', 'dup_q', full => {full})"
            ))
        };
        assert_eq!(found(true), Some(vec![]));
        assert_eq!(found(false), Some(vec![]));
        Spi::run(
            "SET LOCAL session_replication_role = replica;
             INSERT INTO public.dup_q (band, value, published_at)
             VALUES (0, '{}', date_bin('1 hour', now(), '2000-01-01') + interval '1 hour');
             UPDATE public.dup_q SET log_offset = 1 WHERE log_offset IS NULL",
        )
        .unwrap();
        assert_eq!(
            one::<i64>("SELECT count(DISTINCT tableoid) FROM public.dup_q WHERE log_offset = 1"),
            Some(2)
        );
        assert_eq!(found(true), Some(vec!["0:1:2".to_string()]));
        assert_eq!(found(false), Some(vec!["0:1:2".to_string()]));
    }
}
