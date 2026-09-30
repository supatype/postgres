// pgrx::pg_module_magic! checks every pgrx pgNN feature; only pg14..pg17 are declared here.
#![allow(unexpected_cfgs)]

use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::ffi::CStr;
use std::panic::{AssertUnwindSafe, UnwindSafe};
use std::time::{Duration, Instant};

use pgrx::bgworkers::{BackgroundWorker, BackgroundWorkerBuilder, SignalWakeFlags};
use pgrx::pg_sys::panic::CaughtError;
use pgrx::prelude::*;
use pgrx::spi;
use pgrx::{GucContext, GucFlags, GucRegistry, GucSetting};

mod listener;
mod retention;
mod stamper;
mod sync;

pgrx::pg_module_magic!();

extension_sql_file!("../sql/pg_topics.sql", finalize);

static FAILOVER_IS_FENCED: GucSetting<bool> = GucSetting::<bool>::new(false);
static DATABASES: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
static GROUP_MIN_SESSION_MS: GucSetting<i32> = GucSetting::<i32>::new(6000);
static GROUP_MAX_SESSION_MS: GucSetting<i32> = GucSetting::<i32>::new(1_800_000);
static GROUP_INITIAL_REBALANCE_DELAY_MS: GucSetting<i32> = GucSetting::<i32>::new(3000);
static PORT: GucSetting<i32> = GucSetting::<i32>::new(9092);
static ADVERTISED_HOST: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(Some(c"localhost"));
static MAX_CLIENTS: GucSetting<i32> = GucSetting::<i32>::new(100);
static MAX_MESSAGE_BYTES: GucSetting<i32> = GucSetting::<i32>::new(1_048_576);
static TLS_CERT_FILE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
static TLS_KEY_FILE: GucSetting<Option<&'static CStr>> =
    GucSetting::<Option<&'static CStr>>::new(None);
static TLS_USE_POSTGRES_CERT: GucSetting<bool> = GucSetting::<bool>::new(false);

#[pg_extern(immutable, strict)]
#[search_path(pg_catalog, pg_temp)]
fn band_for(key: &str, band_count: i32) -> i32 {
    if !(1..=1024).contains(&band_count) {
        error!("topic.band_for: band_count must be between 1 and 1024, got {band_count}");
    }
    pgt::murmur2::band_for(key.as_bytes(), band_count as u32) as i32
}

#[pg_extern]
#[search_path(pg_catalog, pg_temp)]
fn caller() -> String {
    if unsafe { pg_sys::InSecurityRestrictedOperation() } {
        error!("topic.caller: refused inside a security-restricted operation");
    }
    unsafe { CStr::from_ptr(pg_sys::GetUserNameFromId(pg_sys::GetOuterUserId(), false)) }
        .to_string_lossy()
        .into_owned()
}

thread_local! {
    static PLANS: RefCell<HashMap<String, spi::OwnedPreparedStatement>> = RefCell::new(HashMap::new());
}

fn planned<R>(
    sql: &str,
    args: Vec<(PgOid, Option<pg_sys::Datum>)>,
    read: impl for<'c> FnOnce(spi::SpiTupleTable<'c>) -> spi::Result<R>,
) -> spi::Result<R> {
    PLANS.with_borrow_mut(|plans| {
        Spi::connect(|client| {
            let plan = match plans.entry(sql.to_string()) {
                Entry::Occupied(e) => e.into_mut(),
                Entry::Vacant(e) => e.insert(
                    client
                        .prepare(sql, Some(args.iter().map(|a| a.0).collect()))?
                        .keep(),
                ),
            };
            read(client.select(
                &*plan,
                Some(1),
                Some(args.into_iter().map(|a| a.1).collect()),
            )?)
        })
    })
}

fn topic_owner(schema_name: &str, topic: &str) -> spi::Result<Option<pg_sys::Oid>> {
    planned(
        "SELECT (SELECT c.relowner FROM topic.topic_config t
                 JOIN pg_catalog.pg_namespace n ON n.nspname = t.schema_name
                 JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.topic AND c.relkind = 'p'
                 WHERE t.schema_name = $1 AND t.topic = $2)",
        vec![text_arg(schema_name), text_arg(topic)],
        |t| t.first().get_one::<pg_sys::Oid>(),
    )
}

#[pg_extern(security_definer)]
#[search_path(pg_catalog, pg_temp)]
fn produced_row(
    schema_name: &str,
    topic: &str,
    band: i16,
    rows: i32,
) -> spi::Result<
    TableIterator<'static, (name!(published_at, TimestampWithTimeZone), name!(seq, i64))>,
> {
    let who = caller();
    let Some(owner) = topic_owner(schema_name, topic)?.filter(|_| rows > 0) else {
        return Ok(TableIterator::new(vec![]));
    };
    let queue = spi::quote_qualified_identifier(schema_name, topic);
    let (published_at, seq) = as_owner(owner, || {
        planned(
            &format!(
                "SELECT r.published_at, r.seq FROM (VALUES (1)) v
                 LEFT JOIN LATERAL (SELECT published_at, seq FROM {queue}
                     WHERE log_offset IS NULL AND xact = (SELECT pg_current_xact_id()) AND band = $1
                       AND published_by = $2
                     ORDER BY xact DESC, seq DESC OFFSET $3 - 1 LIMIT 1) r ON true"
            ),
            vec![
                (PgBuiltInOids::INT2OID.oid(), band.into_datum()),
                text_arg(&who),
                (PgBuiltInOids::INT4OID.oid(), rows.into_datum()),
            ],
            |t| t.first().get_two::<TimestampWithTimeZone, i64>(),
        )
    })?;
    Ok(TableIterator::new(published_at.zip(seq)))
}

#[pg_extern(security_definer)]
#[search_path(pg_catalog, pg_temp)]
fn produced_offset(
    schema_name: &str,
    topic: &str,
    published_at: TimestampWithTimeZone,
    seq: i64,
) -> spi::Result<Option<i64>> {
    let who = caller();
    let Some(owner) = topic_owner(schema_name, topic)? else {
        return Ok(None);
    };
    let queue = spi::quote_qualified_identifier(schema_name, topic);
    as_owner(owner, || {
        planned(
            &format!(
                "SELECT (SELECT log_offset FROM {queue} WHERE published_at = $1 AND seq = $2 AND published_by = $3)"
            ),
            vec![
                (PgBuiltInOids::TIMESTAMPTZOID.oid(), published_at.into_datum()),
                (PgBuiltInOids::INT8OID.oid(), seq.into_datum()),
                text_arg(&who),
            ],
            |t| t.first().get_one::<i64>(),
        )
    })
}

fn as_owner<R>(owner: pg_sys::Oid, f: impl FnOnce() -> R) -> R {
    let mut saved_uid = pg_sys::InvalidOid;
    let mut saved_ctx = 0;
    unsafe {
        pg_sys::GetUserIdAndSecContext(&mut saved_uid, &mut saved_ctx);
        pg_sys::SetUserIdAndSecContext(
            owner,
            saved_ctx
                | (pg_sys::SECURITY_LOCAL_USERID_CHANGE | pg_sys::SECURITY_RESTRICTED_OPERATION)
                    as i32,
        );
    }
    PgTryBuilder::new(AssertUnwindSafe(f))
        .finally(|| unsafe { pg_sys::SetUserIdAndSecContext(saved_uid, saved_ctx) })
        .execute()
}

fn text_arg(value: &str) -> (PgOid, Option<pg_sys::Datum>) {
    (PgBuiltInOids::TEXTOID.oid(), value.into_datum())
}

fn read_one<T: FromDatum + IntoDatum>(
    sql: &str,
    args: Vec<(PgOid, Option<pg_sys::Datum>)>,
) -> spi::Result<Option<T>> {
    Spi::connect(|client| client.select(sql, Some(1), Some(args))?.first().get_one())
}

fn queue_owner(caller: &str, schema_name: &str, topic: &str) -> spi::Result<pg_sys::Oid> {
    read_one::<String>(
        "SELECT pg_catalog.set_config('row_security', 'off', true)",
        vec![],
    )?;
    let (owner, kind) = Spi::connect(|client| {
        client
            .select(
                "SELECT r.relowner, r.relkind::text FROM (VALUES (1)) v
                 LEFT JOIN LATERAL (SELECT c.relowner, c.relkind FROM topic.topic_config t
                     JOIN pg_catalog.pg_namespace n ON n.nspname = t.schema_name
                     JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid AND c.relname = t.topic
                     WHERE t.schema_name = $1 AND t.topic = $2) r ON true",
                Some(1),
                Some(vec![text_arg(schema_name), text_arg(topic)]),
            )?
            .first()
            .get_two::<pg_sys::Oid, String>()
    })?;
    match (owner, kind.as_deref()) {
        (Some(owner), Some("p")) => Ok(owner),
        (Some(_), _) => error!("{caller}: {schema_name}.{topic} is not a partitioned table"),
        _ => error!("{caller}: topic {schema_name}.{topic} does not exist"),
    }
}

fn database_names(list: &str) -> Vec<&str> {
    let mut names = Vec::new();
    for name in list.split(',').map(str::trim) {
        if !name.is_empty() && !names.contains(&name) {
            names.push(name);
        }
    }
    names
}

fn in_transaction<R>(f: impl FnOnce() -> spi::Result<R> + UnwindSafe) -> R {
    BackgroundWorker::transaction(AssertUnwindSafe(|| f().unwrap_or_else(|e| error!("{e}"))))
}

fn error_text(e: CaughtError) -> String {
    let (CaughtError::PostgresError(report)
    | CaughtError::ErrorReport(report)
    | CaughtError::RustPanic {
        ereport: report, ..
    }) = e;
    report.message().to_string()
}

fn guarded<R>(worker: &str, f: impl FnOnce() -> R + UnwindSafe) -> Option<R> {
    PgTryBuilder::new(|| Some(f()))
        .catch_others(|e| {
            unsafe { pg_sys::AbortCurrentTransaction() };
            warning!(
                "pg_topics {worker}: {}. The {worker} tries again in 1 s.",
                error_text(e)
            );
            None
        })
        .execute()
}

fn topic_list(filter: &str) -> spi::Result<Option<Vec<(String, String)>>> {
    if read_one::<bool>(
        "SELECT EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = 'pg_topics')",
        vec![],
    )? != Some(true)
    {
        return Ok(None);
    }
    let (schemas, topics) = Spi::connect(|client| {
        client
            .select(
                &format!(
                    "SELECT array_agg(schema_name ORDER BY schema_name, topic),
                            array_agg(topic ORDER BY schema_name, topic)
                     FROM topic.topic_config WHERE {filter}"
                ),
                Some(1),
                None,
            )?
            .first()
            .get_two::<Vec<String>, Vec<String>>()
    })?;
    Ok(Some(
        schemas
            .unwrap_or_default()
            .into_iter()
            .zip(topics.unwrap_or_default())
            .collect(),
    ))
}

fn wait_latch(ms: i64) -> bool {
    unsafe {
        pg_sys::WaitLatch(
            pg_sys::MyLatch,
            (pg_sys::WL_LATCH_SET | pg_sys::WL_TIMEOUT | pg_sys::WL_EXIT_ON_PM_DEATH) as i32,
            ms,
            pg_sys::PG_WAIT_EXTENSION,
        );
        pg_sys::ResetLatch(pg_sys::MyLatch);
        pg_sys::check_for_interrupts!();
    }
    !BackgroundWorker::sigterm_received()
}

fn topic_worker(worker: &str, filter: &str, work: fn(&str, &str) -> spi::Result<i32>, wake: bool) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    BackgroundWorker::connect_worker_to_spi(Some(BackgroundWorker::get_extra()), None);
    in_transaction(|| {
        Spi::run("SET search_path = pg_catalog, pg_temp; SET lock_timeout = '100ms'")
    });
    let mut retry_after: HashMap<(String, String), Instant> = HashMap::new();
    let mut worked: HashMap<(String, String), Instant> = HashMap::new();
    let mut topics = Vec::new();
    let mut listed_at: Option<Instant> = None;
    let mut pause = 50;
    while wait_latch(pause) {
        worked.retain(|_, at| at.elapsed() < Duration::from_millis(100));
        let full = listed_at.is_none_or(|at| at.elapsed() >= Duration::from_millis(50));
        if full {
            let Some(Some(list)) = guarded(worker, || in_transaction(|| topic_list(filter))) else {
                pause = 1000;
                continue;
            };
            topics = list;
            listed_at = Some(Instant::now());
        }
        for key in &topics {
            if !full && !worked.contains_key(key)
                || retry_after.get(key).is_some_and(|at| Instant::now() < *at)
            {
                continue;
            }
            match guarded(worker, || in_transaction(|| work(&key.0, &key.1))) {
                Some(n) => {
                    if n > 0 {
                        worked.insert(key.clone(), Instant::now());
                        if wake {
                            guarded(worker, || in_transaction(|| notify_stamped(&key.0, &key.1)));
                        }
                    }
                    retry_after.remove(key);
                }
                None => {
                    retry_after.insert(key.clone(), Instant::now() + Duration::from_secs(1));
                }
            }
        }
        unsafe { pg_sys::pgstat_report_stat(false) };
        pause = if worked.is_empty() { 50 } else { 1 };
    }
    unsafe { pg_sys::proc_exit(1) }
}

fn notify_stamped(schema_name: &str, topic: &str) -> spi::Result<()> {
    // NOTIFY holds a cluster-wide lock until commit ends, so it must not share a commit that waits on a standby.
    Spi::run("SET LOCAL synchronous_commit = off")?;
    Spi::run_with_args(
        "SELECT pg_catalog.pg_notify('pg_topics_stamped', $1 || '.' || $2)",
        Some(vec![text_arg(schema_name), text_arg(topic)]),
    )
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_sync_main(_arg: pg_sys::Datum) {
    topic_worker(
        "sync worker",
        "sync_enabled",
        |schema_name, topic| {
            Ok(read_one::<i32>(
                "SELECT topic.sync_topic($1, $2)",
                vec![text_arg(schema_name), text_arg(topic)],
            )?
            .unwrap_or(0))
        },
        false,
    )
}

#[pg_guard]
pub extern "C" fn _PG_init() {
    if unsafe { !pg_sys::process_shared_preload_libraries_in_progress } {
        error!("pg_topics must be loaded via shared_preload_libraries");
    }
    GucRegistry::define_bool_guc(
        "pg_topics.failover_is_fenced",
        "The operator has fenced the old primary on failover.",
        "create_topic refuses durable and replicated topics while this is off.",
        &FAILOVER_IS_FENCED,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_topics.databases",
        "The databases that get the pg_topics workers.",
        "A comma list of database names. A change needs a server restart.",
        &DATABASES,
        GucContext::Postmaster,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_topics.group_min_session_ms",
        "The lowest session timeout in ms that a group member may ask for.",
        "topic.group_join refuses a lower session_ms with INVALID_SESSION_TIMEOUT.",
        &GROUP_MIN_SESSION_MS,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_topics.group_max_session_ms",
        "The highest session timeout in ms that a group member may ask for.",
        "topic.group_join refuses a higher session_ms with INVALID_SESSION_TIMEOUT.",
        &GROUP_MAX_SESSION_MS,
        1,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_topics.group_initial_rebalance_delay_ms",
        "The longest time in ms that the first join window of an empty group stays open.",
        "The window closes at the lower of this value and the largest rebalance_ms of the members.",
        &GROUP_INITIAL_REBALANCE_DELAY_MS,
        0,
        i32::MAX,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_topics.port",
        "The TCP port of the Kafka listener. 0 means no listener.",
        "The listener reads it at start. Set it per database with ALTER DATABASE.",
        &PORT,
        0,
        65535,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_topics.advertised_host",
        "The host name that Metadata gives to Kafka clients.",
        "The listener reads it at start.",
        &ADVERTISED_HOST,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_topics.max_clients",
        "The largest number of authenticated Kafka clients per listener.",
        "Each client holds one Postgres connection. Set max_connections above it.",
        &MAX_CLIENTS,
        1,
        100_000,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_int_guc(
        "pg_topics.max_message_bytes",
        "The largest record batch in bytes that Produce accepts.",
        "A larger batch gets MESSAGE_TOO_LARGE.",
        &MAX_MESSAGE_BYTES,
        1024,
        1 << 30,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_topics.tls_cert_file",
        "The PEM certificate file of the Kafka listener.",
        "A relative path starts at the data directory.",
        &TLS_CERT_FILE,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_string_guc(
        "pg_topics.tls_key_file",
        "The PEM private key file of the Kafka listener.",
        "A relative path starts at the data directory.",
        &TLS_KEY_FILE,
        GucContext::Suset,
        GucFlags::default(),
    );
    GucRegistry::define_bool_guc(
        "pg_topics.tls_use_postgres_cert",
        "The Kafka listener uses ssl_cert_file and ssl_key_file.",
        "Only when pg_topics.tls_cert_file and pg_topics.tls_key_file are empty and ssl is on.",
        &TLS_USE_POSTGRES_CERT,
        GucContext::Suset,
        GucFlags::default(),
    );
    let list = DATABASES
        .get()
        .map(|list| list.to_string_lossy().into_owned())
        .unwrap_or_default();
    for database in database_names(&list) {
        BackgroundWorkerBuilder::new(&format!("pg_topics stamper {database}"))
            .set_type("pg_topics stamper")
            .set_library("pg_topics")
            .set_function("pg_topics_stamper_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
        BackgroundWorkerBuilder::new(&format!("pg_topics replicated stamper {database}"))
            .set_type("pg_topics replicated stamper")
            .set_library("pg_topics")
            .set_function("pg_topics_replicated_stamper_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
        BackgroundWorkerBuilder::new(&format!("pg_topics partition {database}"))
            .set_type("pg_topics partition")
            .set_library("pg_topics")
            .set_function("pg_topics_partition_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
        BackgroundWorkerBuilder::new(&format!("pg_topics sync {database}"))
            .set_type("pg_topics sync")
            .set_library("pg_topics")
            .set_function("pg_topics_sync_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
        BackgroundWorkerBuilder::new(&format!("pg_topics listener {database}"))
            .set_type("pg_topics listener")
            .set_library("pg_topics")
            .set_function("pg_topics_listener_main")
            .set_extra(database)
            .set_restart_time(Some(Duration::from_secs(5)))
            .enable_spi_access()
            .load();
    }
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use pgrx::prelude::*;

    #[pg_test]
    fn band_for_matches_golden_sample() {
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('a', 7)").unwrap(),
            Some(5)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('bottle-1', 4)").unwrap(),
            Some(2)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('Zürich', 1024)").unwrap(),
            Some(49)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('θάλασσα', 16)").unwrap(),
            Some(2)
        );
        assert_eq!(
            Spi::get_one::<i32>("SELECT topic.band_for('मुंबई', 7)").unwrap(),
            Some(4)
        );
    }

    #[pg_test]
    fn band_for_rejects_bad_count() {
        let too_low =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| crate::band_for("x", 0)));
        assert!(too_low.is_err());

        let too_high =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| crate::band_for("x", 1025)));
        assert!(too_high.is_err());
    }

    #[pg_test]
    fn databases_guc_parses() {
        assert_eq!(
            crate::database_names(" postgres, app ,, other_db, app"),
            vec!["postgres", "app", "other_db"]
        );
        assert!(crate::database_names(" , ").is_empty());
    }

    pub(crate) fn one<T: IntoDatum + FromDatum>(sql: &str) -> Option<T> {
        Spi::get_one::<T>(sql).unwrap()
    }

    pub(crate) fn error_of(sql: &str) -> Option<String> {
        Spi::run(
            "DO $do$ BEGIN
             IF to_regprocedure('pg_temp.error_of(text)') IS NULL THEN
                 CREATE FUNCTION pg_temp.error_of(q text) RETURNS text LANGUAGE plpgsql AS $$
                 BEGIN EXECUTE q; RETURN NULL; EXCEPTION WHEN OTHERS THEN RETURN SQLERRM; END $$;
             END IF;
             END $do$",
        )
        .unwrap();
        Spi::get_one_with_args::<String>(
            "SELECT pg_temp.error_of($1)",
            vec![(PgBuiltInOids::TEXTOID.oid(), sql.into_datum())],
        )
        .unwrap()
    }

    pub(crate) fn tenant(role: &str) {
        Spi::run(&format!(
            "CREATE ROLE {role}; CREATE SCHEMA {role} AUTHORIZATION {role}"
        ))
        .unwrap();
    }

    #[pg_test]
    fn create_topic_builds_objects() {
        Spi::run("SELECT topic.create_topic('public.bottles_q', 4)").unwrap();
        assert_eq!(
            one::<String>(
                "SELECT relkind::text FROM pg_class WHERE oid = 'public.bottles_q'::regclass"
            ),
            Some("p".into())
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM pg_inherits WHERE inhparent = 'public.bottles_q'::regclass"
            ),
            Some(2)
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM pg_index i JOIN pg_inherits h ON h.inhrelid = i.indrelid
                 WHERE h.inhparent = 'public.bottles_q'::regclass AND i.indisunique AND NOT i.indisprimary"
            ),
            Some(2)
        );
        assert_eq!(
            one::<String>(
                "SELECT pg_get_expr(i.indpred, i.indrelid) FROM pg_index i
                 JOIN pg_class c ON c.oid = i.indexrelid JOIN pg_am a ON a.oid = c.relam
                 WHERE i.indrelid = 'public.bottles_q'::regclass AND a.amname = 'brin'"
            ),
            Some("(log_offset IS NOT NULL)".into())
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM topic.topic_band_position
                 WHERE schema_name = 'public' AND topic = 'bottles_q'"
            ),
            Some(4)
        );
        assert_eq!(
            one::<String>(
                "SELECT pg_get_constraintdef(oid) FROM pg_constraint
                 WHERE conrelid = 'public.bottles_q'::regclass AND contype = 'c'"
            ),
            Some("CHECK (((band >= 0) AND (band <= 3)))".into())
        );
    }

    #[pg_test]
    fn create_topic_refuses_bad_names() {
        let long = format!("public.{}_q", "x".repeat(46));
        for call in [
            "SELECT topic.create_topic('a.b.c_q')".to_string(),
            "SELECT topic.create_topic('public.bottles')".to_string(),
            format!("SELECT topic.create_topic('{long}')"),
            "SELECT topic.create_topic('public.zero_q', 0)".to_string(),
            "SELECT topic.create_topic('public.many_q', 1025)".to_string(),
            "SELECT topic.create_topic('public.\"x;drop\"')".to_string(),
        ] {
            assert!(error_of(&call).is_some(), "not refused: {call}");
        }
        tenant("pgt_no_create");
        Spi::run("SET LOCAL ROLE pgt_no_create").unwrap();
        let refused = error_of("SELECT topic.create_topic('public.theirs_q')");
        Spi::run("RESET ROLE").unwrap();
        assert!(refused.unwrap().contains("no CREATE privilege"));
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_config"),
            Some(0)
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM pg_class WHERE relname LIKE '%\\_q%' AND relnamespace = 'public'::regnamespace"),
            Some(0)
        );
    }

    #[pg_test]
    fn create_topic_refuses_unfenced_durability() {
        assert!(error_of(
            "SELECT topic.create_topic('public.rep_q', 1, min_durability => 'replicated')"
        )
        .unwrap()
        .contains("synchronous_standby_names"));
        Spi::run("SET LOCAL pg_topics.failover_is_fenced = off").unwrap();
        assert!(error_of("SELECT topic.create_topic('public.dur_q', 1)")
            .unwrap()
            .contains("failover_is_fenced"));
        Spi::run("SELECT topic.create_topic('public.rel_q', 1, min_durability => 'relaxed')")
            .unwrap();
    }

    #[pg_test]
    fn sync_copies_counts_the_primary_and_the_required_standbys() {
        for (names, copies) in [
            ("", 1),
            ("s1", 2),
            ("s1, s2, s3", 2),
            ("*", 2),
            ("2, s1", 2),
            ("\"FIRST 3 (a, b, c)\", s2", 2),
            ("2 (s1, s2)", 3),
            ("FIRST 2 (s1, s2)", 3),
            ("first 3 (s1, \"s 2\", *)", 4),
            ("ANY 1 (s1, s2)", 2),
            ("  any\t2(\"ANY 5 (x)\", *)", 3),
            ("ANY 99999999999 (s1)", 2),
        ] {
            assert_eq!(
                Spi::get_one_with_args::<i32>(
                    "SELECT topic.sync_copies($1)",
                    vec![(PgBuiltInOids::TEXTOID.oid(), names.into_datum())],
                )
                .unwrap(),
                Some(copies),
                "{names}"
            );
        }
    }

    #[pg_test]
    fn describe_configs_gives_one_copy_below_the_replicated_tier() {
        Spi::run("SELECT topic.create_topic('public.rf_q', 1)").unwrap();
        assert_eq!(
            one::<String>(
                "SELECT value FROM topic.describe_configs('public.rf_q')
                 WHERE name = 'pg_topics.replication_factor' AND NOT editable"
            ),
            Some("1".to_string())
        );
    }

    #[pg_test]
    fn produced_offsets_answer_only_the_publisher() {
        tenant("pgt_po");
        Spi::run(
            "CREATE ROLE pgt_pub_a; CREATE ROLE pgt_pub_b;
             GRANT USAGE ON SCHEMA pgt_po TO pgt_pub_a, pgt_pub_b;
             SET LOCAL ROLE pgt_po;
             SELECT topic.create_topic('pgt_po.po_q', 1);
             SELECT topic.grant_publish('pgt_po.po_q', 'pgt_pub_a');
             SELECT topic.grant_publish('pgt_po.po_q', 'pgt_pub_b');
             SET LOCAL ROLE pgt_pub_a;
             INSERT INTO pgt_po.po_q (band, value) VALUES (0, '1');
             INSERT INTO pgt_po.po_q (band, value) VALUES (0, '2'), (0, '3');
             RESET ROLE",
        )
        .unwrap();
        let as_role = |role: &str, sql: &str| {
            Spi::run(&format!("SET LOCAL ROLE {role}")).unwrap();
            let got = one::<String>(sql);
            Spi::run("RESET ROLE").unwrap();
            got
        };
        let row = "SELECT string_agg(seq::text, ',') FROM topic.produced_row('pgt_po', 'po_q', 0::smallint, 2)";
        assert_eq!(
            as_role("pgt_pub_a", row),
            one::<String>("SELECT seq::text FROM pgt_po.po_q WHERE value = '2'")
        );
        assert_eq!(as_role("pgt_pub_b", row), None);
        Spi::run("SELECT topic.stamp_topic('pgt_po', 'po_q')").unwrap();
        let pk = one::<String>(
            "SELECT format('SELECT coalesce(topic.produced_offset(''pgt_po'', ''po_q'', %L, %s)::text, ''none'')',
                           published_at, seq) FROM pgt_po.po_q WHERE value = '2'",
        )
        .unwrap();
        assert_eq!(as_role("pgt_pub_a", &pk), Some("1".into()));
        assert_eq!(as_role("pgt_pub_b", &pk), Some("none".into()));
    }

    #[pg_test]
    fn raw_insert_cannot_forge_offset() {
        tenant("pgt_forger");
        Spi::run("SET LOCAL ROLE pgt_forger").unwrap();
        Spi::run("SELECT topic.create_topic('pgt_forger.forge_q', 1)").unwrap();
        let offset = error_of("INSERT INTO pgt_forger.forge_q (band, log_offset) VALUES (0, 7)");
        let author =
            error_of("INSERT INTO pgt_forger.forge_q (band, published_by) VALUES (0, 'someone')");
        let group = error_of("INSERT INTO pgt_forger.forge_q (band, xact) VALUES (0, '1')");
        let future = error_of(
            "INSERT INTO pgt_forger.forge_q (band, published_at) VALUES (0, clock_timestamp() + interval '1 minute')",
        );
        let plain = error_of("INSERT INTO pgt_forger.forge_q (band) VALUES (0)");
        Spi::run("RESET ROLE").unwrap();
        assert!(offset.unwrap().contains("must not set log_offset"));
        assert!(author.unwrap().contains("must not set log_offset"));
        assert!(group.unwrap().contains("must not set log_offset"));
        assert!(
            future
                .as_deref()
                .is_some_and(|e| e.contains("published_at later than the current time")),
            "{future:?}"
        );
        assert_eq!(plain, None);
    }

    #[pg_test]
    fn publish_refuses_on_backlog() {
        Spi::run("SELECT topic.create_topic('public.slow_q', 1)").unwrap();
        Spi::run("UPDATE topic.topic_config SET backlog_age = '2 minutes' WHERE topic = 'slow_q'")
            .unwrap();
        assert!(error_of("SELECT topic.publish('public.slow_q', '{}')")
            .unwrap()
            .contains("above max_backlog_age"));
    }

    #[pg_test]
    fn publish_refuses_without_recent_stamp() {
        Spi::run("SELECT topic.create_topic('public.stale_q', 1)").unwrap();
        Spi::run("SELECT topic.publish('public.stale_q', '{}')").unwrap();
        Spi::run(
            "UPDATE topic.topic_config SET stamped_at = now() - interval '2 minutes' WHERE topic = 'stale_q'",
        )
        .unwrap();
        assert!(error_of("SELECT topic.publish('public.stale_q', '{}')")
            .unwrap()
            .contains("the stamper has not run"));
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'stale_q')"),
            Some(1)
        );
        Spi::run("SELECT topic.publish('public.stale_q', '{}')").unwrap();
    }

    #[pg_test]
    fn max_backlog_age_is_at_least_2_seconds() {
        Spi::run("SELECT topic.create_topic('public.short_q', 1)").unwrap();
        assert!(error_of(
            "UPDATE topic.topic_config SET max_backlog_age = '1 second' WHERE topic = 'short_q'"
        )
        .unwrap()
        .contains("check constraint"));
        Spi::run(
            "UPDATE topic.topic_config SET max_backlog_age = '2 seconds' WHERE topic = 'short_q'",
        )
        .unwrap();
    }

    #[pg_test]
    fn publish_raises_synchronous_commit() {
        Spi::run("SELECT topic.create_topic('public.floor_q', 1)").unwrap();
        Spi::run("SET LOCAL synchronous_commit = off").unwrap();
        Spi::run("SELECT topic.publish('public.floor_q', '{}')").unwrap();
        assert_eq!(one::<String>("SHOW synchronous_commit"), Some("on".into()));
    }

    #[pg_test]
    fn a_queue_table_keeps_its_name_and_schema() {
        Spi::run("SELECT topic.create_topic('public.named_q', 1); CREATE SCHEMA pgt_elsewhere")
            .unwrap();
        for ddl in [
            "ALTER TABLE public.named_q RENAME TO other_q",
            "ALTER TABLE public.named_q SET SCHEMA pgt_elsewhere",
        ] {
            assert_eq!(
                error_of(ddl),
                Some("topic: a queue table must keep its name and schema".into()),
                "{ddl}"
            );
        }
        Spi::run("ALTER TABLE public.named_q ADD COLUMN note text").unwrap();
        assert_eq!(
            one::<bool>("SELECT to_regclass('public.named_q') IS NOT NULL"),
            Some(true)
        );
    }

    #[pg_test]
    fn a_superuser_deletes_a_group_whose_owner_is_gone() {
        Spi::run(
            "INSERT INTO topic.topic_groups (group_name, owner_role) VALUES ('pgt_orphan', 'pgt_gone_owner');
             SELECT topic.delete_group('pgt_orphan')",
        )
        .unwrap();
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_groups WHERE group_name = 'pgt_orphan'"),
            Some(0)
        );
    }

    #[pg_test]
    fn stamp_raises_synchronous_commit() {
        Spi::run(
            "SELECT topic.create_topic('public.floor_q', 1);
             SELECT topic.publish('public.floor_q', '{}');
             SET LOCAL synchronous_commit = off",
        )
        .unwrap();
        assert_eq!(
            one::<i32>("SELECT topic.stamp_topic('public', 'floor_q')"),
            Some(1)
        );
        assert_eq!(one::<String>("SHOW synchronous_commit"), Some("on".into()));
    }

    #[pg_test]
    fn caller_sees_set_role_inside_definer() {
        Spi::run("CREATE ROLE pgt_caller").unwrap();
        Spi::run(
            "CREATE FUNCTION public.pgt_who() RETURNS text SECURITY DEFINER LANGUAGE sql
             AS $$ SELECT topic.caller() || '/' || current_user $$",
        )
        .unwrap();
        let me = one::<String>("SELECT current_user::text").unwrap();
        Spi::run("SET LOCAL ROLE pgt_caller").unwrap();
        let seen = one::<String>("SELECT public.pgt_who()");
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(seen, Some(format!("pgt_caller/{me}")));
    }

    #[pg_test]
    fn publish_uses_kafka_partitioner() {
        Spi::run("SELECT topic.create_topic('public.keyed_q', 4)").unwrap();
        Spi::run("SELECT topic.publish('public.keyed_q', '{}', 'bottle-1')").unwrap();
        assert_eq!(one::<i16>("SELECT band FROM public.keyed_q"), Some(2));
    }

    #[pg_test]
    fn kafka_topics_shows_what_the_caller_may_read_or_write() {
        tenant("pgt_meta");
        Spi::run("CREATE ROLE pgt_writer; CREATE ROLE pgt_nobody").unwrap();
        Spi::run("SET LOCAL ROLE pgt_meta").unwrap();
        Spi::run(
            "SELECT topic.create_topic('pgt_meta.m_q', 3);
             GRANT INSERT (band, key, value, headers, producer_timestamp) ON pgt_meta.m_q TO pgt_writer;
             RESET ROLE",
        )
        .unwrap();
        let seen = |role: &str, names: &str| {
            Spi::run(&format!("SET LOCAL ROLE {role}")).unwrap();
            let rows = one::<Vec<String>>(&format!(
                "SELECT coalesce(array_agg(topic || ':' || band_count || ':' || visible), '{{}}')
                 FROM topic.kafka_topics({names})"
            ));
            Spi::run("RESET ROLE").unwrap();
            rows
        };
        let visible = Some(vec!["pgt_meta.m_q:3:true".to_string()]);
        assert_eq!(seen("pgt_meta", "NULL"), visible);
        assert_eq!(seen("pgt_writer", "NULL"), visible);
        assert_eq!(
            seen("pgt_nobody", "ARRAY['pgt_meta.m_q', 'pgt_meta.none_q']"),
            Some(vec!["pgt_meta.m_q:3:false".to_string()])
        );
    }

    #[pg_test]
    fn create_topic_bounds_ignore_session_datestyle() {
        Spi::run("SET LOCAL DateStyle = 'SQL, MDY'; SET LOCAL TimeZone = 'Asia/Kolkata'").unwrap();
        Spi::run("SELECT topic.create_topic('public.tz_q', 1)").unwrap();
        Spi::run("SET LOCAL DateStyle = 'ISO'; SET LOCAL TimeZone = 'UTC'").unwrap();
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
                 WHERE h.inhparent = 'public.tz_q'::regclass
                 AND pg_get_expr(c.relpartbound, c.oid) LIKE format('FOR VALUES FROM (%L)%%',
                     to_char(to_timestamp(right(c.relname, 14), 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS+00'))"
            ),
            Some(2)
        );
    }

    #[pg_test]
    fn every_function_pins_search_path() {
        assert_eq!(
            one::<Vec<String>>(
                "SELECT coalesce(array_agg(proname::text), '{}') FROM pg_proc
                 WHERE pronamespace = 'topic'::regnamespace
                 AND NOT coalesce('search_path=pg_catalog, pg_temp' = ANY (proconfig), false)"
            ),
            Some(vec![])
        );
    }

    #[pg_test]
    fn ensure_partitions_adds_owned_indexed_partitions() {
        tenant("pgt_parts");
        Spi::run("SET LOCAL ROLE pgt_parts").unwrap();
        Spi::run("SELECT topic.create_topic('pgt_parts.p_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run("RESET ROLE").unwrap();
        Spi::run("SELECT topic.ensure_partitions('pgt_parts', 'p_q', 3)").unwrap();
        Spi::run("SELECT topic.ensure_partitions('pgt_parts', 'p_q', 3)").unwrap();
        Spi::run("SET LOCAL TimeZone = 'UTC'").unwrap();
        let parts = "FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid,
                     LATERAL (SELECT to_timestamp(right(c.relname, 14), 'YYYYMMDDHH24MISS') AS lo) b
                     WHERE h.inhparent = 'pgt_parts.p_q'::regclass";
        assert_eq!(
            one::<Vec<i32>>(&format!(
                "SELECT array_agg((extract(epoch FROM b.lo - date_bin('1 hour', now(), '2000-01-01')) / 3600)::int
                                  ORDER BY b.lo) {parts}"
            )),
            Some(vec![0, 1, 2, 3])
        );
        assert_eq!(
            one::<Vec<String>>(&format!(
                "SELECT array_agg(format('%s %s %s',
                    pg_get_expr(c.relpartbound, c.oid) = format('FOR VALUES FROM (%L) TO (%L)',
                        to_char(b.lo, 'YYYY-MM-DD HH24:MI:SS+00'),
                        to_char(b.lo + interval '1 hour', 'YYYY-MM-DD HH24:MI:SS+00')),
                    pg_get_userbyid(c.relowner),
                    EXISTS (SELECT FROM pg_index x WHERE x.indrelid = c.oid AND x.indisunique)))
                 {parts}"
            )),
            Some(vec!["t pgt_parts t".to_string(); 4])
        );
    }

    #[pg_test]
    fn ensure_partitions_skips_a_table_that_has_a_partition_name() {
        Spi::run("SELECT topic.create_topic('public.clash_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run(
            "SET LOCAL TimeZone = 'UTC';
             CREATE TABLE public.foreign_t ();
             DO $$ BEGIN EXECUTE format('ALTER TABLE public.foreign_t RENAME TO %I',
                 'clash_q_p' || to_char(date_bin('1 hour', now(), '2000-01-01') + interval '2 hours', 'YYYYMMDDHH24MISS'));
             END $$",
        )
        .unwrap();
        Spi::run("SELECT topic.ensure_partitions('public', 'clash_q', 3)").unwrap();
        assert_eq!(
            one::<Vec<i32>>(
                "SELECT array_agg((extract(epoch FROM to_timestamp(right(c.relname, 14), 'YYYYMMDDHH24MISS')
                                   - date_bin('1 hour', now(), '2000-01-01')) / 3600)::int ORDER BY c.relname)
                 FROM pg_inherits h JOIN pg_class c ON c.oid = h.inhrelid
                 WHERE h.inhparent = 'public.clash_q'::regclass"
            ),
            Some(vec![0, 1, 3])
        );
    }

    #[pg_test]
    fn queue_functions_refuse_a_view_in_place_of_the_queue() {
        tenant("pgt_view");
        Spi::run("SET LOCAL ROLE pgt_view").unwrap();
        Spi::run("SELECT topic.create_topic('pgt_view.v_q', 1, partition_interval => '1 hour')")
            .unwrap();
        Spi::run("RESET ROLE; SET LOCAL session_replication_role = replica").unwrap();
        Spi::run("ALTER TABLE pgt_view.v_q RENAME TO gone").unwrap();
        Spi::run("RESET session_replication_role; SET LOCAL ROLE pgt_view").unwrap();
        Spi::run(
            "DROP TABLE pgt_view.gone;
             CREATE VIEW pgt_view.v_q AS
                 SELECT 0::smallint AS band, 0::bigint AS log_offset, now() AS published_at, 0::bigint AS seq",
        )
        .unwrap();
        Spi::run("RESET ROLE").unwrap();
        for call in [
            "SELECT topic.stamp_topic('pgt_view', 'v_q')",
            "SELECT count(*) FROM topic.check_duplicates('pgt_view', 'v_q', full => true)",
            "SELECT count(*) FROM topic.retention_floor('pgt_view', 'v_q')",
            "SELECT topic.ensure_partitions('pgt_view', 'v_q', 3)",
        ] {
            let refused = error_of(call);
            assert!(
                refused
                    .as_deref()
                    .is_some_and(|e| e.contains("is not a partitioned table")),
                "{call}: {refused:?}"
            );
        }
    }

    #[pg_test]
    fn reap_removes_old_producers_and_expired_groups() {
        Spi::run(
            "SELECT topic.create_topic('public.kept_q', 1, partition_interval => '1 hour');
             SELECT topic.create_topic('public.reaped_q', 1, partition_interval => '1 hour');
             UPDATE topic.topic_config SET offset_retention = '1 hour' WHERE topic = 'reaped_q';
             INSERT INTO topic.topic_producers
                 (schema_name, topic, producer_id, producer_epoch, band, slot, first_sequence, last_sequence, base_published_at, base_seq, updated_at)
             VALUES ('public', 'kept_q', 1, 0, 0, 0, 0, 0, now(), 1, now() - interval '25 hours'),
                    ('public', 'kept_q', 2, 0, 0, 0, 0, 0, now(), 1, now() - interval '23 hours');
             INSERT INTO topic.producer_ids (producer_id, owner_role, created_at, last_used_at)
             VALUES (1, 'postgres', now() - interval '9 days', now() - interval '8 days'),
                    (2, 'postgres', now() - interval '9 days', now() - interval '8 days'),
                    (3, 'postgres', now() - interval '9 days', now() - interval '6 days'),
                    (4, 'postgres', now() - interval '9 days', now() - interval '1 hour');
             INSERT INTO topic.topic_groups (group_name, owner_role, state, updated_at)
             VALUES ('old_empty', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('new_empty', 'postgres', 'Empty', now() - interval '30 minutes'),
                    ('old_stable', 'postgres', 'Stable', now() - interval '2 hours'),
                    ('old_member', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('old_forever', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('old_no_offsets', 'postgres', 'Empty', now() - interval '2 hours'),
                    ('day_old_no_offsets', 'postgres', 'Empty', now() - interval '25 hours'),
                    ('day_old_member', 'postgres', 'Empty', now() - interval '25 hours'),
                    ('day_old_stable', 'postgres', 'Stable', now() - interval '25 hours'),
                    ('__pg_topics_sync:public.reaped_q', 'postgres', 'Empty', now() - interval '2 hours');
             INSERT INTO topic.topic_group_members (group_name, member_id, owner_role, session_timeout_ms, rebalance_ms)
             VALUES ('old_member', 'm', 'postgres', 6000, 6000), ('day_old_member', 'm', 'postgres', 6000, 6000);
             INSERT INTO topic.topic_offsets (schema_name, topic, group_name, band, owner_role)
             SELECT 'public', 'reaped_q', g, 0, 'postgres'
             FROM unnest(ARRAY['old_empty', 'new_empty', 'old_stable', 'old_member', 'old_forever',
                               '__pg_topics_sync:public.reaped_q']) g;
             INSERT INTO topic.topic_offsets (schema_name, topic, group_name, band, owner_role)
             VALUES ('public', 'kept_q', 'old_forever', 0, 'postgres');
             SELECT topic.reap();",
        )
        .unwrap();
        assert_eq!(
            one::<Vec<i64>>(
                "SELECT array_agg(producer_id ORDER BY producer_id) FROM topic.topic_producers"
            ),
            Some(vec![2])
        );
        assert_eq!(
            one::<Vec<i64>>(
                "SELECT array_agg(producer_id ORDER BY producer_id) FROM topic.producer_ids"
            ),
            Some(vec![2, 3, 4])
        );
        assert_eq!(
            one::<Vec<String>>(
                "SELECT array_agg(group_name ORDER BY group_name COLLATE \"C\") FROM topic.topic_groups"
            ),
            Some(
                [
                    "__pg_topics_sync:public.reaped_q",
                    "day_old_member",
                    "day_old_stable",
                    "new_empty",
                    "old_forever",
                    "old_member",
                    "old_no_offsets",
                    "old_stable"
                ]
                .map(String::from)
                .to_vec()
            )
        );
    }

    fn state_of(sql: &str) -> Option<String> {
        Spi::run(
            "DO $do$ BEGIN
             IF to_regprocedure('pg_temp.state_of(text)') IS NULL THEN
                 CREATE FUNCTION pg_temp.state_of(q text) RETURNS text LANGUAGE plpgsql AS $$
                 BEGIN EXECUTE q; RETURN NULL; EXCEPTION WHEN OTHERS THEN RETURN SQLSTATE; END $$;
             END IF;
             END $do$",
        )
        .unwrap();
        Spi::get_one_with_args::<String>(
            "SELECT pg_temp.state_of($1)",
            vec![(PgBuiltInOids::TEXTOID.oid(), sql.into_datum())],
        )
        .unwrap()
    }

    fn member(group: &str) -> String {
        one::<String>(&format!(
            r#"SELECT concat_ws('/', error, member_id) FROM topic.group_join('{group}', '', 'client', 10000, 60000,
                   'consumer', '[{{"name": "range"}}]')"#
        ))
        .unwrap()
        .strip_prefix("MEMBER_ID_REQUIRED/")
        .unwrap()
        .to_string()
    }

    fn join(group: &str, member: &str, session_ms: i32) -> Option<String> {
        one::<String>(&format!(
            r#"SELECT concat_ws('/', coalesce(error, 'WAIT'), generation_id, jsonb_array_length(members))
               FROM topic.group_join('{group}', '{member}', 'client', {session_ms}, 60000, 'consumer',
                                     '[{{"name": "range", "metadata": "{member}"}}]')"#
        ))
    }

    fn poll(group: &str, member: &str) -> Option<String> {
        one::<String>(&format!(
            "SELECT concat_ws('/', coalesce(error, 'WAIT'), generation_id, jsonb_array_length(members))
             FROM topic.group_join_poll('{group}', '{member}')"
        ))
    }

    fn time_out_window(group: &str) {
        Spi::run(&format!(
            "UPDATE topic.topic_groups SET rebalance_started_at = now() - interval '1 hour'
             WHERE group_name = '{group}'"
        ))
        .unwrap();
    }

    fn commit(topic: &str, group: &str, band: i32, offset: i64, generation: i32) -> Option<String> {
        one::<String>(&format!(
            "SELECT topic.commit_offset('{topic}', '{group}', {band}, {offset}, {generation})"
        ))
    }

    #[pg_test]
    fn control_plane_refuses_non_owner_serves_owner_members() {
        tenant("pgt_cp_owner");
        Spi::run("CREATE ROLE pgt_cp_member IN ROLE pgt_cp_owner; CREATE ROLE pgt_cp_other")
            .unwrap();
        Spi::run("SET LOCAL ROLE pgt_cp_owner").unwrap();
        for t in ["kegs", "casks"] {
            Spi::run(&format!(
                r#"SELECT topic.create_table_topic('pgt_cp_owner.{t}', '{{"id": "int"}}', 'id', 1);
                   CREATE TABLE pgt_cp_owner.{t}2 (id int PRIMARY KEY, event_at timestamptz NOT NULL);
                   SELECT topic.group_join('pgt_cp_{t}', '', 'client', 10000, 1000, 'consumer', '[{{"name": "range"}}]');"#
            ))
            .unwrap();
        }
        Spi::run("RESET ROLE").unwrap();
        let ops = |t: &str| {
            [
                format!("SELECT topic.set_retention('pgt_cp_owner.{t}_q', '1 day')"),
                format!("SELECT topic.set_backlog_limit('pgt_cp_owner.{t}_q', '30 seconds')"),
                format!("SELECT topic.set_durability('pgt_cp_owner.{t}_q', 'relaxed')"),
                format!("SELECT topic.set_sync_enabled('pgt_cp_owner.{t}_q', false)"),
                format!("SELECT topic.set_sync('pgt_cp_owner.{t}_q', 'pgt_cp_owner.{t}2', 'id')"),
                format!("SELECT topic.delete_group('pgt_cp_{t}')"),
                format!("SELECT topic.drop_topic('pgt_cp_owner.{t}_q')"),
            ]
        };
        let run_as = |role: &str, sqls: &[String]| -> Vec<Option<String>> {
            Spi::run(&format!("SET LOCAL ROLE {role}")).unwrap();
            let states = sqls.iter().map(|sql| state_of(sql)).collect();
            Spi::run("RESET ROLE").unwrap();
            states
        };
        let config = |t: &str| {
            one::<String>(&format!(
                "SELECT concat_ws('/', retention_interval, max_backlog_age, min_durability, sync_enabled, sync_table,
                                  (SELECT count(*) FROM topic.topic_groups WHERE group_name = 'pgt_cp_{t}'))
                 FROM topic.topic_config WHERE schema_name = 'pgt_cp_owner' AND topic = '{t}_q'"
            ))
        };
        assert_eq!(
            run_as("pgt_cp_other", &ops("kegs")),
            vec![Some("42501".to_string()); 7]
        );
        assert_eq!(
            config("kegs"),
            Some("7 days/00:01:00/durable/t/pgt_cp_owner.kegs/1".into())
        );
        for (role, t) in [("pgt_cp_member", "kegs"), ("pgt_cp_owner", "casks")] {
            let [before_drop @ .., drop] = ops(t);
            assert_eq!(run_as(role, &before_drop), vec![None; 6], "{role}");
            assert_eq!(
                config(t),
                Some(format!("1 day/00:00:30/relaxed/f/pgt_cp_owner.{t}2/0"))
            );
            assert_eq!(run_as(role, &[drop]), vec![None], "{role}");
            assert_eq!(
                one::<bool>(&format!("SELECT to_regclass('pgt_cp_owner.{t}_q') IS NULL")),
                Some(true)
            );
        }
    }

    #[pg_test]
    fn grant_publish_and_grant_consume_are_owner_fenced() {
        tenant("pgt_gr_owner");
        Spi::run(
            "CREATE ROLE pgt_gr_member IN ROLE pgt_gr_owner; CREATE ROLE pgt_gr_other;
             CREATE ROLE pgt_gr_pub; CREATE ROLE pgt_gr_con;
             SET LOCAL ROLE pgt_gr_owner;
             SELECT topic.create_topic('pgt_gr_owner.g_q', 1);
             RESET ROLE",
        )
        .unwrap();
        let grants = || {
            one::<String>(
                "SELECT concat_ws('/',
                     (SELECT string_agg(attname, ',' ORDER BY attname) FROM pg_attribute
                      WHERE attrelid = 'pgt_gr_owner.g_q'::regclass AND attnum > 0 AND NOT attisdropped
                        AND has_column_privilege('pgt_gr_pub', attrelid, attnum, 'INSERT')),
                     has_table_privilege('pgt_gr_pub', 'pgt_gr_owner.g_q', 'SELECT'),
                     has_table_privilege('pgt_gr_con', 'pgt_gr_owner.g_q', 'SELECT'),
                     has_any_column_privilege('pgt_gr_con', 'pgt_gr_owner.g_q', 'INSERT'))",
            )
        };
        let run_as = |role: &str| {
            Spi::run(&format!("SET LOCAL ROLE {role}")).unwrap();
            let states = [
                state_of("SELECT topic.grant_publish('pgt_gr_owner.g_q', 'pgt_gr_pub')"),
                state_of("SELECT topic.grant_consume('pgt_gr_owner.g_q', 'pgt_gr_con')"),
            ];
            Spi::run("RESET ROLE").unwrap();
            states
        };
        assert_eq!(
            run_as("pgt_gr_other"),
            [Some("42501".to_string()), Some("42501".to_string())]
        );
        assert_eq!(grants(), Some("f/f/f".into()));
        assert_eq!(run_as("pgt_gr_member"), [None, None]);
        assert_eq!(
            grants(),
            Some("band,headers,key,producer_timestamp,value/f/t/f".into())
        );
        Spi::run("SET LOCAL ROLE pgt_gr_owner").unwrap();
        let to_public = [
            state_of("SELECT topic.grant_publish('pgt_gr_owner.g_q', 'public')"),
            state_of("SELECT topic.grant_consume('pgt_gr_owner.g_q', 'public')"),
        ];
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            to_public,
            [Some("22023".to_string()), Some("22023".to_string())]
        );
        assert_eq!(
            one::<String>(
                "SELECT has_any_column_privilege('public', 'pgt_gr_owner.g_q', 'INSERT') || '/'
                     || has_table_privilege('public', 'pgt_gr_owner.g_q', 'SELECT')"
            ),
            Some("false/false".into())
        );
    }

    #[pg_test]
    fn fences_refuse_a_member_without_inherit() {
        tenant("pgt_ni_owner");
        Spi::run(
            "CREATE ROLE pgt_ni_member; GRANT pgt_ni_owner TO pgt_ni_member WITH INHERIT FALSE;
             SET LOCAL ROLE pgt_ni_owner;
             SELECT topic.create_topic('pgt_ni_owner.n_q', 1);
             SELECT topic.commit_offset('pgt_ni_owner.n_q', 'pgt_ni_g', 0, 0, -1);
             RESET ROLE",
        )
        .unwrap();
        Spi::run("SET LOCAL ROLE pgt_ni_member").unwrap();
        let seen = (
            state_of("SELECT topic.set_retention('pgt_ni_owner.n_q', '1 day')"),
            state_of("SELECT topic.grant_consume('pgt_ni_owner.n_q', 'pgt_ni_member')"),
            one::<String>("SELECT topic.group_heartbeat('pgt_ni_g', 'm', 0)"),
            one::<i64>("SELECT count(*) FROM topic.topic_offsets"),
        );
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            seen,
            (
                Some("42501".into()),
                Some("42501".into()),
                Some("GROUP_AUTHORIZATION_FAILED".into()),
                Some(0)
            )
        );
    }

    #[pg_test]
    fn group_policy_survives_a_dropped_owner() {
        tenant("pgt_gone");
        tenant("pgt_kept");
        let join = |group: &str| {
            format!(
                r#"SELECT (topic.group_join('{group}', m.member_id, 'c', 1800000, 5000, 'consumer', '[{{"name": "range"}}]')).error
                   FROM topic.group_join('{group}', '', 'c', 1800000, 5000, 'consumer', '[{{"name": "range"}}]') m"#
            )
        };
        Spi::run(&format!(
            "SET LOCAL ROLE pgt_kept;
             SELECT topic.create_topic('pgt_kept.k_q', 1);
             GRANT SELECT ON pgt_kept.k_q TO pgt_gone;
             SELECT topic.commit_offset('pgt_kept.k_q', 'pgt_kept_g', 0, 0, -1);
             {};
             SET LOCAL ROLE pgt_gone;
             SELECT topic.commit_offset('pgt_kept.k_q', 'pgt_gone_g', 0, 0, -1);
             {};
             RESET ROLE;
             DROP OWNED BY pgt_gone; DROP ROLE pgt_gone",
            join("pgt_kept_j"),
            join("pgt_gone_j")
        ))
        .unwrap();
        assert_eq!(
            one::<i64>(
                "SELECT (SELECT count(*) FROM topic.topic_groups WHERE owner_role = 'pgt_gone')
                      + (SELECT count(*) FROM topic.topic_group_members WHERE owner_role = 'pgt_gone')
                      + (SELECT count(*) FROM topic.topic_offsets WHERE owner_role = 'pgt_gone')"
            ),
            Some(4)
        );
        Spi::run("SET LOCAL ROLE pgt_kept").unwrap();
        let seen = error_of(
            "SELECT 1 FROM topic.topic_groups, topic.topic_group_members, topic.topic_offsets",
        )
        .or_else(|| {
            one::<String>(
                "SELECT concat_ws('|', (SELECT string_agg(group_name, ',' ORDER BY group_name) FROM topic.topic_groups),
                                       (SELECT string_agg(group_name, ',') FROM topic.topic_group_members),
                                       (SELECT string_agg(group_name, ',') FROM topic.topic_offsets))",
            )
        });
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            seen,
            Some("pgt_kept_g,pgt_kept_j|pgt_kept_j|pgt_kept_g".into())
        );
    }

    #[pg_test]
    fn offsets_policy_hides_other_tenants() {
        for t in ["pgt_pol_a", "pgt_pol_b"] {
            tenant(t);
            Spi::run(&format!(
                "SET LOCAL ROLE {t};
                 SELECT topic.create_topic('{t}.o_q', 1);
                 SELECT topic.commit_offset('{t}.o_q', '{t}_g', 0, 0, -1);
                 RESET ROLE"
            ))
            .unwrap();
        }
        let seen = |role: &str| {
            Spi::run(&format!("SET LOCAL ROLE {role}")).unwrap();
            let groups = one::<Vec<String>>(
                "SELECT coalesce(array_agg(group_name ORDER BY group_name), '{}') FROM topic.topic_offsets",
            );
            Spi::run("RESET ROLE").unwrap();
            groups
        };
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM topic.topic_offsets WHERE group_name LIKE 'pgt_pol_%'"
            ),
            Some(2)
        );
        assert_eq!(seen("pgt_pol_a"), Some(vec!["pgt_pol_a_g".to_string()]));
        assert_eq!(seen("pgt_pol_b"), Some(vec!["pgt_pol_b_g".to_string()]));
    }

    #[pg_test]
    fn drop_topic_removes_table_partitions_and_control_rows() {
        Spi::run(
            "SELECT topic.create_topic('public.dropped_q', 2);
             SELECT topic.publish('public.dropped_q', '{}') FROM generate_series(1, 4);
             SELECT topic.stamp_topic('public', 'dropped_q');
             INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state)
             VALUES ('pgt_dropped', current_user, 1, 'Stable');
             INSERT INTO topic.topic_producers
                 (schema_name, topic, producer_id, producer_epoch, band, slot, first_sequence, last_sequence, base_published_at, base_seq)
             VALUES ('public', 'dropped_q', 1, 0, 0, 0, 0, 0, now(), 1);",
        )
        .unwrap();
        assert_eq!(
            commit("public.dropped_q", "pgt_dropped", 0, 2, 1),
            Some("NONE".into())
        );
        Spi::run("SELECT topic.drop_topic('public.dropped_q')").unwrap();
        assert_eq!(
            one::<Vec<i64>>(
                "SELECT ARRAY[
                     (SELECT count(*) FROM pg_class WHERE relname LIKE 'dropped\\_q%'),
                     (SELECT count(*) FROM topic.topic_config WHERE topic = 'dropped_q'),
                     (SELECT count(*) FROM topic.topic_band_position WHERE topic = 'dropped_q'),
                     (SELECT count(*) FROM topic.topic_offsets WHERE topic = 'dropped_q'),
                     (SELECT count(*) FROM topic.topic_producers WHERE topic = 'dropped_q'),
                     (SELECT count(*) FROM topic.topic_groups WHERE group_name = 'pgt_dropped')]"
            ),
            Some(vec![0, 0, 0, 0, 0, 1])
        );
    }

    #[pg_test]
    fn fetch_orders_filters_and_refuses_below_oldest() {
        Spi::run(
            "SELECT topic.create_topic('public.fetched_q', 1);
             SELECT topic.publish('public.fetched_q', jsonb_build_object('i', i, 'odd', i % 2 = 1))
             FROM generate_series(1, 5) i;
             SELECT topic.stamp_topic('public', 'fetched_q');",
        )
        .unwrap();
        let offsets = |call: &str| {
            one::<Vec<i64>>(&format!(
                "SELECT coalesce(array_agg(log_offset), '{{}}') FROM topic.fetch('public.fetched_q', {call})"
            ))
        };
        assert_eq!(offsets("0, 0"), Some(vec![0, 1, 2, 3, 4]));
        assert_eq!(offsets("0, 2, 2"), Some(vec![2, 3]));
        assert_eq!(
            offsets(r#"0, 0, filter => '{"odd": true}'"#),
            Some(vec![0, 2, 4])
        );
        assert_eq!(
            one::<Vec<i64>>(
                "SELECT ARRAY[topic.offset_for_time('public.fetched_q', 0,
                                  (SELECT published_at FROM public.fetched_q WHERE log_offset = 3)),
                              coalesce(topic.offset_for_time('public.fetched_q', 0, now() + interval '1 day'), -1)]"
            ),
            Some(vec![3, -1])
        );
        Spi::run(
            "UPDATE topic.topic_band_position SET oldest_offset = 2 WHERE topic = 'fetched_q'",
        )
        .unwrap();
        assert_eq!(
            state_of("SELECT * FROM topic.fetch('public.fetched_q', 0, 1)"),
            Some("PT001".into())
        );
        assert_eq!(offsets("0, 2"), Some(vec![2, 3, 4]));
        assert_eq!(
            state_of("SELECT * FROM topic.fetch('public.fetched_q', 1, 0)"),
            Some("42P01".into())
        );
        Spi::run("CREATE ROLE pgt_fetch_none").unwrap();
        Spi::run("SET LOCAL ROLE pgt_fetch_none").unwrap();
        let refused = state_of("SELECT * FROM topic.band_offsets('public.fetched_q')");
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(refused, Some("42501".into()));
    }

    #[pg_test]
    fn commit_offset_is_fenced_by_generation() {
        Spi::run(
            "SELECT topic.create_topic('public.fenced_q', 1);
             SELECT topic.publish('public.fenced_q', '{}') FROM generate_series(1, 20);
             SELECT topic.stamp_topic('public', 'fenced_q');
             INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state)
             VALUES ('pgt_fence', current_user, 5, 'Stable');",
        )
        .unwrap();
        let stored = || {
            one::<String>(
                "SELECT committed_offset || '@' || generation_id FROM topic.topic_offsets WHERE group_name = 'pgt_fence'",
            )
        };
        let c = |offset: i64, generation: i32| {
            commit("public.fenced_q", "pgt_fence", 0, offset, generation)
        };
        assert_eq!(c(10, 5), Some("NONE".into()));
        assert_eq!(c(12, 4), Some("ILLEGAL_GENERATION".into()));
        assert_eq!(c(10, 5), Some("NONE".into()));
        assert_eq!(c(8, 5), Some("NONE".into()));
        assert_eq!(stored(), Some("8@5".into()));
        Spi::run("UPDATE topic.topic_groups SET generation_id = 6 WHERE group_name = 'pgt_fence'")
            .unwrap();
        assert_eq!(c(10, 5), Some("ILLEGAL_GENERATION".into()));
        assert_eq!(c(8, 5), Some("ILLEGAL_GENERATION".into()));
        assert_eq!(c(15, 6), Some("NONE".into()));
        assert_eq!(stored(), Some("15@6".into()));
        assert_eq!(c(100, 6), Some("NONE".into()));
        assert_eq!(stored(), Some("20@6".into()));
        assert_eq!(
            one::<i64>("SELECT topic.fetch_offset('public.fenced_q', 'pgt_fence', 0)"),
            Some(20)
        );
        assert_eq!(
            commit("public.fenced_q", "pgt_fence", 7, 1, 6),
            Some("UNKNOWN_TOPIC_OR_PARTITION".into())
        );
        assert_eq!(
            commit("public.nothing_q", "pgt_fence", 0, 1, 6),
            Some("TOPIC_AUTHORIZATION_FAILED".into())
        );
        Spi::run("CREATE ROLE pgt_fence_other").unwrap();
        Spi::run("SET LOCAL ROLE pgt_fence_other").unwrap();
        let refused = c(1, 6);
        let read = state_of("SELECT topic.fetch_offset('public.fenced_q', 'pgt_fence', 0)");
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            (refused, read),
            (
                Some("GROUP_AUTHORIZATION_FAILED".into()),
                Some("42501".into())
            )
        );
        assert_eq!(stored(), Some("20@6".into()));
        for bad in ["NULL", "-1"] {
            assert_eq!(
                one::<String>(&format!(
                    "SELECT topic.commit_offset('public.fenced_q', 'pgt_fence', 0, {bad}, 6)"
                )),
                Some("OFFSET_OUT_OF_RANGE".into()),
                "{bad}"
            );
        }
        assert_eq!(stored(), Some("20@6".into()));
        Spi::run(
            "INSERT INTO topic.topic_groups (group_name, owner_role) VALUES ('__pg_topics_sync:public.fenced_q', current_user)",
        )
        .unwrap();
        assert_eq!(
            commit(
                "public.fenced_q",
                "__pg_topics_sync:public.fenced_q",
                0,
                5,
                0
            ),
            Some("INVALID_GROUP_ID".into())
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(*) FROM topic.topic_offsets WHERE group_name LIKE '\\_\\_pg%'"
            ),
            Some(0)
        );
    }

    #[pg_test]
    fn offsets_need_select_on_the_topic() {
        Spi::run(
            "SELECT topic.create_topic('public.secret_q', 1);
             SELECT topic.publish('public.secret_q', '{}') FROM generate_series(1, 3);
             SELECT topic.stamp_topic('public', 'secret_q');
             CREATE ROLE pgt_snoop;
             INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state)
             VALUES ('pgt_snoop', 'pgt_snoop', 1, 'Stable');",
        )
        .unwrap();
        let as_snoop = || {
            Spi::run("SET LOCAL ROLE pgt_snoop").unwrap();
            let r = (
                commit("public.secret_q", "pgt_snoop", 0, 100, 1),
                state_of("SELECT topic.fetch_offset('public.secret_q', 'pgt_snoop', 0)"),
            );
            Spi::run("RESET ROLE").unwrap();
            r
        };
        assert_eq!(
            as_snoop(),
            (
                Some("TOPIC_AUTHORIZATION_FAILED".into()),
                Some("42501".into())
            )
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_offsets WHERE group_name = 'pgt_snoop'"),
            Some(0)
        );
        Spi::run("GRANT SELECT ON public.secret_q TO pgt_snoop").unwrap();
        assert_eq!(as_snoop(), (Some("NONE".into()), None));
    }

    #[pg_test]
    fn admin_commit_moves_an_empty_group_to_any_offset() {
        Spi::run(
            "SELECT topic.create_topic('public.reset_q', 1);
             SELECT topic.publish('public.reset_q', '{}') FROM generate_series(1, 20);
             SELECT topic.stamp_topic('public', 'reset_q');
             INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state)
             VALUES ('pgt_reset', current_user, 4, 'Empty');",
        )
        .unwrap();
        let stored = || {
            one::<String>(
                "SELECT committed_offset || '@' || generation_id FROM topic.topic_offsets WHERE group_name = 'pgt_reset'",
            )
        };
        let c = |offset: i64, generation: i32| {
            commit("public.reset_q", "pgt_reset", 0, offset, generation)
        };
        assert_eq!(c(10, 4), Some("NONE".into()));
        assert_eq!(c(3, 4), Some("NONE".into()));
        assert_eq!(stored(), Some("3@4".into()));
        assert_eq!(c(3, -1), Some("NONE".into()));
        assert_eq!(stored(), Some("3@4".into()));
        assert_eq!(c(99, -1), Some("NONE".into()));
        assert_eq!(stored(), Some("20@4".into()));
        assert_eq!(c(-1, -1), Some("OFFSET_OUT_OF_RANGE".into()));
        Spi::run("UPDATE topic.topic_groups SET state = 'Stable' WHERE group_name = 'pgt_reset'")
            .unwrap();
        assert_eq!(c(1, -1), Some("ILLEGAL_GENERATION".into()));
        assert_eq!(stored(), Some("20@4".into()));
    }

    #[pg_test]
    fn offset_functions_answer_a_missing_topic_like_an_unreadable_one() {
        Spi::run(
            "SELECT topic.create_topic('public.hidden_q', 1);
             CREATE ROLE pgt_blind;
             INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state)
             VALUES ('pgt_blind', 'pgt_blind', 1, 'Stable');",
        )
        .unwrap();
        Spi::run("SET LOCAL ROLE pgt_blind").unwrap();
        let answer = |topic: &str| {
            (
                commit(topic, "pgt_blind", 0, 1, 1),
                commit(topic, "pgt_blind", 0, 1, -1),
                state_of(&format!(
                    "SELECT topic.fetch_offset('{topic}', 'pgt_blind', 0)"
                )),
                error_of(&format!(
                    "SELECT topic.fetch_offset('{topic}', 'pgt_blind', 0)"
                ))
                .map(|e| e.replace(topic, "<topic>")),
            )
        };
        let (hidden, missing) = (answer("public.hidden_q"), answer("public.ghost_q"));
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(hidden, missing);
        assert_eq!(hidden.0, Some("TOPIC_AUTHORIZATION_FAILED".into()));
        assert_eq!(hidden.2, Some("42501".into()));
    }

    #[pg_test]
    fn an_admin_commit_creates_a_missing_group() {
        Spi::run(
            "SELECT topic.create_topic('public.assign_q', 1);
             SELECT topic.publish('public.assign_q', '{}') FROM generate_series(1, 5);
             SELECT topic.stamp_topic('public', 'assign_q');
             CREATE ROLE pgt_assign; GRANT SELECT ON public.assign_q TO pgt_assign;",
        )
        .unwrap();
        Spi::run("SET LOCAL ROLE pgt_assign").unwrap();
        let committed = (
            commit("public.assign_q", "pgt_assigned", 0, 3, 5),
            commit("public.assign_q", "pgt_assigned", 0, 3, -1),
            commit("public.ghost_q", "pgt_ghost", 0, 3, -1),
            commit("public.assign_q", "", 0, 3, -1),
            commit(
                "public.assign_q",
                "__pg_topics_sync:public.assign_q",
                0,
                3,
                -1,
            ),
            one::<i64>("SELECT topic.fetch_offset('public.assign_q', 'pgt_assigned', 0)"),
        );
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(
            committed,
            (
                Some("UNKNOWN_MEMBER_ID".into()),
                Some("NONE".into()),
                Some("UNKNOWN_MEMBER_ID".into()),
                Some("INVALID_GROUP_ID".into()),
                Some("INVALID_GROUP_ID".into()),
                Some(3)
            )
        );
        assert_eq!(
            one::<String>(
                "SELECT string_agg(group_name || '/' || owner_role || '/' || state, ',' ORDER BY group_name)
                 FROM topic.topic_groups WHERE owner_role = 'pgt_assign'"
            ),
            Some("pgt_assigned/pgt_assign/Empty".into())
        );
    }

    #[pg_test]
    fn wire_bytes_reads_only_a_base64_string() {
        assert_eq!(
            one::<String>(
                r#"SELECT string_agg(coalesce(encode(topic.wire_bytes(v), 'hex'), 'NULL'), ',' ORDER BY n)
                   FROM unnest('{"\"AAEC\"", "\"AAE=\"", "\"\"", "\"m\"", "\"AA\"", "[0]", "null"}'::jsonb[])
                        WITH ORDINALITY u(v, n)"#
            ),
            Some("000102,0001,,NULL,NULL,NULL,NULL".into())
        );
        assert_eq!(one::<Vec<u8>>("SELECT topic.wire_bytes(NULL)"), None);
    }

    #[pg_test]
    fn fetch_offset_only_reads() {
        Spi::run("SELECT topic.create_topic('public.none_q', 1)").unwrap();
        let [a, b] = [member("pgt_read"), member("pgt_read")];
        assert_eq!(join("pgt_read", &a, 6000), Some("WAIT".into()));
        assert_eq!(join("pgt_read", &b, 6000), Some("WAIT".into()));
        Spi::run(&format!(
            "UPDATE topic.topic_group_members SET last_heartbeat_at = now() - interval '1 minute' WHERE member_id = '{b}';
             UPDATE topic.topic_groups SET rebalance_started_at = now() - interval '1 hour' WHERE group_name = 'pgt_read'"
        ))
        .unwrap();
        assert_eq!(
            one::<i64>("SELECT coalesce(topic.fetch_offset('public.none_q', 'pgt_read', 0), -1)"),
            Some(-1)
        );
        assert_eq!(
            one::<String>(
                "SELECT concat_ws('/', state, generation_id, expired_members,
                                  (SELECT count(*) FROM topic.topic_group_members WHERE group_name = 'pgt_read'))
                 FROM topic.topic_groups WHERE group_name = 'pgt_read'"
            ),
            Some("PreparingRebalance/0/0/2".into())
        );
    }

    #[pg_test]
    fn expire_groups_rebalances_a_group_nobody_calls() {
        Spi::run(
            "INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state, leader_member_id)
             VALUES ('pgt_idle', current_user, 1, 'Stable', 'a'), ('pgt_alive', current_user, 1, 'Stable', 'c');
             INSERT INTO topic.topic_group_members
                 (group_name, member_id, owner_role, session_timeout_ms, rebalance_ms, joined_generation, last_heartbeat_at)
             VALUES ('pgt_idle', 'a', current_user, 6000, 1000, 0, now() - interval '7 seconds'),
                    ('pgt_idle', 'b', current_user, 6000, 1000, 0, now()),
                    ('pgt_alive', 'c', current_user, 20000, 1000, 0, now() - interval '7 seconds');
             SELECT topic.expire_groups();",
        )
        .unwrap();
        assert_eq!(
            one::<Vec<String>>(
                "SELECT array_agg(concat_ws('/', group_name, state, expired_members) ORDER BY group_name)
                 FROM topic.topic_groups WHERE group_name IN ('pgt_idle', 'pgt_alive')"
            ),
            Some(vec![
                "pgt_alive/Stable/0".to_string(),
                "pgt_idle/PreparingRebalance/1".to_string()
            ])
        );
    }

    #[pg_test]
    fn a_first_commit_with_a_wrong_generation_writes_nothing() {
        Spi::run(
            "SELECT topic.create_topic('public.first_q', 1);
             INSERT INTO topic.topic_groups (group_name, owner_role, generation_id, state)
             VALUES ('pgt_first', current_user, 3, 'Stable');",
        )
        .unwrap();
        assert_eq!(
            commit("public.first_q", "pgt_first", 0, 0, 2),
            Some("ILLEGAL_GENERATION".into())
        );
        assert_eq!(
            commit("public.first_q", "pgt_first", 0, 0, 4),
            Some("ILLEGAL_GENERATION".into())
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_offsets WHERE group_name = 'pgt_first'"),
            Some(0)
        );
    }

    #[pg_test]
    fn join_window_waits_for_every_member_or_the_timeout() {
        let ids = [
            member("pgt_window"),
            member("pgt_window"),
            member("pgt_window"),
        ];
        let [a, b, c] = [&ids[0], &ids[1], &ids[2]];
        for m in &ids {
            assert_eq!(join("pgt_window", m, 10000), Some("WAIT".into()));
        }
        assert_eq!(poll("pgt_window", a), Some("WAIT".into()));
        time_out_window("pgt_window");
        let first: Vec<String> = ids.iter().map(|m| poll("pgt_window", m).unwrap()).collect();
        assert_eq!(
            first.iter().filter(|r| *r == "NONE/1/3").count(),
            1,
            "{first:?}"
        );
        assert_eq!(
            first.iter().filter(|r| *r == "NONE/1/0").count(),
            2,
            "{first:?}"
        );
        assert_eq!(
            one::<String>(
                "SELECT concat_ws('/', state, generation_id, protocol_name) FROM topic.topic_groups
                 WHERE group_name = 'pgt_window'"
            ),
            Some("CompletingRebalance/1/range".into())
        );
        let d = member("pgt_window");
        assert_eq!(join("pgt_window", &d, 10000), Some("WAIT".into()));
        assert_eq!(join("pgt_window", a, 10000), Some("WAIT".into()));
        assert_eq!(join("pgt_window", b, 10000), Some("WAIT".into()));
        assert_eq!(poll("pgt_window", a), Some("WAIT".into()));
        let closing = join("pgt_window", c, 10000).unwrap();
        let second: Vec<String> = [a, b, &d]
            .iter()
            .map(|m| poll("pgt_window", m).unwrap())
            .chain([closing])
            .collect();
        assert_eq!(
            second.iter().filter(|r| *r == "NONE/2/4").count(),
            1,
            "{second:?}"
        );
        assert_eq!(
            second.iter().filter(|r| *r == "NONE/2/0").count(),
            3,
            "{second:?}"
        );
        let follower = one::<String>(&format!(
            "SELECT member_id FROM topic.topic_group_members
             WHERE group_name = 'pgt_window' AND member_id IN ('{a}', '{b}')
               AND member_id <> (SELECT leader_member_id FROM topic.topic_groups WHERE group_name = 'pgt_window')
             LIMIT 1"
        ))
        .unwrap();
        assert_eq!(
            join("pgt_window", &follower, 10000),
            Some("NONE/2/0".into())
        );
        assert_eq!(
            one::<String>(
                "SELECT state || '/' || generation_id FROM topic.topic_groups WHERE group_name = 'pgt_window'"
            ),
            Some("CompletingRebalance/2".into())
        );
    }

    #[pg_test]
    fn a_member_with_an_old_heartbeat_is_expired() {
        let [a, b, c] = [
            member("pgt_expiry"),
            member("pgt_expiry"),
            member("pgt_expiry"),
        ];
        assert_eq!(join("pgt_expiry", &a, 6000), Some("WAIT".into()));
        assert_eq!(join("pgt_expiry", &b, 6000), Some("WAIT".into()));
        assert_eq!(join("pgt_expiry", &c, 20000), Some("WAIT".into()));
        time_out_window("pgt_expiry");
        for m in [&a, &b, &c] {
            assert!(poll("pgt_expiry", m).unwrap().starts_with("NONE/1/"));
        }
        let leader = one::<String>(
            "SELECT leader_member_id FROM topic.topic_groups WHERE group_name = 'pgt_expiry'",
        )
        .unwrap();
        assert_eq!(
            one::<String>(&format!(
                r#"SELECT error FROM topic.group_sync('pgt_expiry', '{leader}', 1,
                       '{{"{a}": [0], "{b}": [1], "{c}": [2]}}')"#
            )),
            Some("NONE".into())
        );
        assert_eq!(
            one::<String>(&format!(
                "SELECT error || ' ' || assignment FROM topic.group_sync_poll('pgt_expiry', '{b}', 1)"
            )),
            Some("NONE [1]".into())
        );
        let survivor = if leader == b { &a } else { &leader };
        Spi::run(&format!(
            "UPDATE topic.topic_group_members SET last_heartbeat_at = now() - interval '10 seconds'
             WHERE group_name = 'pgt_expiry' AND member_id IN ('{b}', '{c}')"
        ))
        .unwrap();
        assert_eq!(
            one::<String>(&format!(
                "SELECT topic.group_heartbeat('pgt_expiry', '{survivor}', 1)"
            )),
            Some("REBALANCE_IN_PROGRESS".into())
        );
        let mut kept = [a.clone(), c.clone()];
        kept.sort();
        assert_eq!(
            one::<String>(
                "SELECT concat_ws('/', state, expired_members,
                                  (SELECT string_agg(member_id, ',' ORDER BY member_id COLLATE \"C\")
                                   FROM topic.topic_group_members WHERE group_name = 'pgt_expiry'))
                 FROM topic.topic_groups WHERE group_name = 'pgt_expiry'"
            ),
            Some(format!("PreparingRebalance/1/{}", kept.join(",")))
        );
    }

    #[pg_test]
    fn a_window_from_empty_closes_after_the_initial_delay() {
        Spi::run("SET LOCAL pg_topics.group_initial_rebalance_delay_ms = 1000").unwrap();
        let a = member("pgt_delay");
        assert_eq!(join("pgt_delay", &a, 10000), Some("WAIT".into()));
        Spi::run(
            "UPDATE topic.topic_groups SET rebalance_started_at = now() - interval '900 milliseconds'
             WHERE group_name = 'pgt_delay'",
        )
        .unwrap();
        assert_eq!(poll("pgt_delay", &a), Some("WAIT".into()));
        Spi::run(
            "UPDATE topic.topic_groups SET rebalance_started_at = now() - interval '1100 milliseconds'
             WHERE group_name = 'pgt_delay'",
        )
        .unwrap();
        assert_eq!(poll("pgt_delay", &a), Some("NONE/1/1".into()));
    }

    #[pg_test]
    fn group_join_refuses_the_sync_group_name_and_bad_requests() {
        assert_eq!(
            join("__pg_topics_sync:public.bottles_q", "a", 10000),
            Some("INVALID_GROUP_ID".into())
        );
        assert_eq!(
            state_of("SELECT topic.delete_group('__pg_topics_sync:public.bottles_q')"),
            Some("42501".into())
        );
        assert_eq!(
            join("pgt_bad", "a", 5999),
            Some("INVALID_SESSION_TIMEOUT".into())
        );
        assert_eq!(
            join("pgt_bad", "a", 1_800_001),
            Some("INVALID_SESSION_TIMEOUT".into())
        );
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.topic_groups"),
            Some(0)
        );
        for (session, rebalance) in [("NULL", "1000"), ("10000", "NULL"), ("10000", "-1")] {
            assert_eq!(
                one::<String>(&format!(
                    r#"SELECT error FROM topic.group_join('pgt_bad', 'a', 'client', {session}, {rebalance},
                           'consumer', '[{{"name": "range"}}]')"#
                )),
                Some("INVALID_SESSION_TIMEOUT".into()),
                "{session} {rebalance}"
            );
        }
        assert_eq!(
            join("pgt_bad", "not-known", 10000),
            Some("UNKNOWN_MEMBER_ID".into())
        );
        Spi::run("SET LOCAL pg_topics.group_min_session_ms = 1000").unwrap();
        let a = member("pgt_bad");
        assert_eq!(join("pgt_bad", &a, 1000), Some("WAIT".into()));
        let late = member("pgt_bad");
        Spi::run(&format!(
            "UPDATE topic.topic_groups SET pending_members = pending_members || jsonb_build_object('{late}', now())
             WHERE group_name = 'pgt_bad'"
        ))
        .unwrap();
        assert_eq!(
            join("pgt_bad", &late, 1000),
            Some("UNKNOWN_MEMBER_ID".into())
        );
        assert_eq!(
            one::<String>(
                r#"SELECT error || ' ' || (member_id LIKE 'client-%') FROM topic.group_join('pgt_bad', '', 'client',
                       10000, 1000, 'consumer', '[{"name": "range"}]')"#
            ),
            Some("MEMBER_ID_REQUIRED true".into())
        );
    }

    fn produce_check(epoch: i16, first: i32, last: i32) -> String {
        format!("SELECT duplicate FROM topic.produce_check('public', 'idem_q', 7, {epoch}::smallint, 0::smallint, {first}, {last}, now(), {first})")
    }

    fn accepted(epoch: i16, first: i32, last: i32) -> Option<bool> {
        one::<bool>(&produce_check(epoch, first, last)).map(|duplicate| !duplicate)
    }

    fn ring() -> Option<String> {
        one::<String>(
            "SELECT string_agg(producer_epoch || ':' || first_sequence || '-' || last_sequence, ',' ORDER BY slot)
             FROM topic.topic_producers WHERE topic = 'idem_q' AND producer_id = 7",
        )
    }

    fn seed_ring(first: i32, last: i32) {
        Spi::run(&format!(
            "INSERT INTO topic.topic_producers (schema_name, topic, producer_id, producer_epoch, band, slot,
                                                first_sequence, last_sequence, base_published_at, base_seq)
             VALUES ('public', 'idem_q', 7, 0, 0, 0, {first}, {last}, now(), 0)"
        ))
        .unwrap();
    }

    fn idem_topic() {
        Spi::run(
            "SELECT topic.create_topic('public.idem_q', 1);
             INSERT INTO topic.producer_ids (producer_id, owner_role) VALUES (7, current_user)",
        )
        .unwrap();
    }

    #[pg_test]
    fn producer_ring_keeps_the_newest_five_in_ring_order() {
        idem_topic();
        for i in 0..7 {
            assert_eq!(accepted(0, i * 10, i * 10 + 9), Some(true));
        }
        assert_eq!(
            ring(),
            Some("0:50-59,0:60-69,0:20-29,0:30-39,0:40-49".into())
        );
    }

    #[pg_test]
    fn producer_retry_of_an_in_flight_batch_matches_by_first_sequence() {
        idem_topic();
        for i in 0..5 {
            assert_eq!(accepted(0, i * 5, i * 5 + 4), Some(true));
        }
        assert_eq!(accepted(0, 10, 14), Some(false));
        assert_eq!(ring(), Some("0:0-4,0:5-9,0:10-14,0:15-19,0:20-24".into()));
        assert_eq!(accepted(0, 25, 29), Some(true));
    }

    #[pg_test]
    fn producer_gap_is_out_of_order() {
        idem_topic();
        assert_eq!(accepted(0, 0, 9), Some(true));
        assert_eq!(state_of(&produce_check(0, 11, 20)), Some("PT002".into()));
        assert_eq!(ring(), Some("0:0-9".into()));
    }

    #[pg_test]
    fn producer_retry_answers_with_the_first_row_of_the_first_attempt() {
        idem_topic();
        assert_eq!(accepted(0, 0, 9), Some(true));
        assert_eq!(
            one::<String>(
                "SELECT duplicate || ' ' || base_seq FROM topic.produce_check('public', 'idem_q', 7,
                     0::smallint, 0::smallint, 0, 9, now(), 99)"
            ),
            Some("true 0".into())
        );
    }

    #[pg_test]
    fn producer_retry_of_a_200_record_batch_matches_its_first_sequence() {
        idem_topic();
        assert_eq!(accepted(0, 0, 199), Some(true));
        assert_eq!(accepted(0, 0, 199), Some(false));
        assert_eq!(ring(), Some("0:0-199".into()));
    }

    #[pg_test]
    fn producer_sequence_wraps() {
        idem_topic();
        seed_ring(2147483500, 2147483599);
        assert_eq!(accepted(0, 2147483600, 2147483647), Some(true));
        assert_eq!(accepted(0, 0, 9), Some(true));
        assert_eq!(accepted(0, 0, 9), Some(false));
        assert_eq!(
            ring(),
            Some("0:2147483500-2147483599,0:2147483600-2147483647,0:0-9".into())
        );
    }

    #[pg_test]
    fn producer_batch_across_the_wrap_is_followed_by_its_next_sequence() {
        idem_topic();
        seed_ring(2147483500, 2147483599);
        assert_eq!(accepted(0, 2147483600, 12), Some(true));
        assert_eq!(accepted(0, 13, 20), Some(true));
    }

    #[pg_test]
    fn producer_with_no_state_must_start_at_sequence_0() {
        idem_topic();
        assert_eq!(state_of(&produce_check(0, 10, 19)), Some("PT004".into()));
        assert_eq!(ring(), None);
        assert_eq!(accepted(0, 0, 9), Some(true));
        assert_eq!(state_of(&produce_check(1, 10, 19)), Some("PT002".into()));
        assert_eq!(ring(), Some("0:0-9".into()));
    }

    #[pg_test]
    fn producer_epoch_bump_starts_a_fresh_ring_old_epoch_fails() {
        idem_topic();
        assert_eq!(accepted(0, 0, 9), Some(true));
        assert_eq!(accepted(0, 10, 19), Some(true));
        assert_eq!(accepted(1, 0, 4), Some(true));
        assert_eq!(ring(), Some("1:0-4".into()));
        assert_eq!(accepted(1, 0, 4), Some(false));
        assert_eq!(state_of(&produce_check(0, 20, 29)), Some("PT003".into()));
    }

    #[pg_test]
    fn produce_check_refuses_a_caller_without_insert() {
        idem_topic();
        Spi::run("CREATE ROLE pgt_idem; UPDATE topic.producer_ids SET owner_role = 'pgt_idem'")
            .unwrap();
        Spi::run("SET LOCAL ROLE pgt_idem").unwrap();
        let refused = state_of(&produce_check(0, 0, 9));
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(refused, Some("42501".into()));
        assert_eq!(ring(), None);
        Spi::run("GRANT INSERT (value) ON public.idem_q TO pgt_idem; SET LOCAL ROLE pgt_idem")
            .unwrap();
        let granted = accepted(0, 0, 9);
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(granted, Some(true));
    }

    #[pg_test]
    fn producer_id_serves_only_its_owner_and_unknown_ids_fail() {
        idem_topic();
        Spi::run(
            "CREATE ROLE pgt_a; CREATE ROLE pgt_b;
             GRANT INSERT (value) ON public.idem_q TO pgt_a, pgt_b",
        )
        .unwrap();
        let check = |role: &str, id: i64| {
            Spi::run(&format!("SET LOCAL ROLE {role}")).unwrap();
            let state = state_of(&format!(
                "SELECT topic.produce_check('public', 'idem_q', {id}, 0::smallint, 0::smallint, 0, 9, now(), 0)"
            ));
            Spi::run("RESET ROLE").unwrap();
            state
        };
        Spi::run("SET LOCAL ROLE pgt_a").unwrap();
        let id = one::<i64>("SELECT topic.init_producer_id()").unwrap();
        Spi::run("RESET ROLE").unwrap();
        assert_eq!(check("pgt_b", id), Some("PT004".into()));
        assert_eq!(check("pgt_a", id), None);
        assert_eq!(check("pgt_a", 99), Some("PT004".into()));
        Spi::run(
            "INSERT INTO topic.producer_ids (producer_id, owner_role) VALUES (98, 'pgt_gone')",
        )
        .unwrap();
        assert_eq!(check("pgt_a", 98), Some("PT004".into()));
    }

    #[pg_test]
    fn produce_check_marks_the_producer_used_at_most_once_a_minute() {
        idem_topic();
        let used_after = |age: &str, first: i32| {
            Spi::run(&format!(
                "UPDATE topic.producer_ids SET last_used_at = now() - interval '{age}' WHERE producer_id = 7"
            ))
            .unwrap();
            accepted(0, first, first + 9);
            one::<String>(
                "SELECT (now() - last_used_at)::text FROM topic.producer_ids WHERE producer_id = 7",
            )
        };
        assert_eq!(used_after("2 minutes", 0), Some("00:00:00".into()));
        assert_eq!(used_after("30 seconds", 10), Some("00:00:30".into()));
    }

    #[pg_test]
    fn health_returns_one_row_per_topic() {
        Spi::run(
            "SELECT topic.create_topic('public.health_a_q', 1);
             SELECT topic.create_topic('public.health_b_q', 2)",
        )
        .unwrap();
        assert_eq!(
            one::<i64>("SELECT count(*) FROM topic.health()"),
            one::<i64>("SELECT count(*) FROM topic.topic_config")
        );
        assert_eq!(
            one::<i64>(
                "SELECT count(DISTINCT (schema_name, topic)) FROM topic.health()
                 WHERE (schema_name, topic) IN (('public', 'health_a_q'), ('public', 'health_b_q'))"
            ),
            Some(2)
        );
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![
            "shared_preload_libraries = 'pg_topics'",
            "pg_topics.failover_is_fenced = on",
        ]
    }
}
