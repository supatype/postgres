use std::ffi::CString;
use std::net::TcpListener;

use pgrx::bgworkers::{BackgroundWorker, SignalWakeFlags};
use pgrx::prelude::*;
use pgrx::spi;

use crate::{in_transaction, wait_latch};

type ListenerSettings = Result<Option<pgt::listener::Config>, String>;

fn listener_settings(database: &str) -> spi::Result<ListenerSettings> {
    let settings = Spi::get_one::<Vec<String>>(
        "SELECT ARRAY[current_setting('pg_topics.port'), current_setting('pg_topics.advertised_host'),
                      current_setting('pg_topics.max_clients'), current_setting('pg_topics.max_message_bytes'),
                      current_setting('pg_topics.tls_cert_file'), current_setting('pg_topics.tls_key_file'),
                      current_setting('pg_topics.tls_use_postgres_cert'), current_setting('ssl'),
                      current_setting('ssl_cert_file'), current_setting('ssl_key_file'),
                      current_setting('data_directory'), current_setting('port')]",
    )?
    .unwrap_or_default();
    Ok(listener_config(database, &settings))
}

fn listener_config(database: &str, settings: &[String]) -> ListenerSettings {
    let [port, host, max_clients, max_bytes, cert, key, use_pg, ssl, ssl_cert, ssl_key, data_dir, pg_port] =
        settings
    else {
        return Err("no listener: the settings cannot be read".into());
    };
    let number = |name: &str, value: &str| {
        value
            .parse::<usize>()
            .map_err(|e| format!("no listener: {name} = {value:?}: {e}"))
    };
    let port = number("pg_topics.port", port)? as u16;
    if port == 0 {
        return Ok(None);
    }
    let in_data = |p: &str| match p.starts_with('/') {
        true => p.to_string(),
        false => format!("{data_dir}/{p}"),
    };
    let (cert, key) = match (cert.trim(), key.trim()) {
        ("", "") if use_pg == "on" && ssl == "on" => (in_data(ssl_cert), in_data(ssl_key)),
        ("", "") => return Err(
            "no listener: set pg_topics.tls_cert_file and pg_topics.tls_key_file, or pg_topics.tls_use_postgres_cert with ssl = on".into(),
        ),
        ("", _) | (_, "") => return Err(
            "no listener: set both pg_topics.tls_cert_file and pg_topics.tls_key_file".into(),
        ),
        (cert, key) => (in_data(cert), in_data(key)),
    };
    let tls = pgt::listener::tls_config(&cert, &key).map_err(|e| format!("no listener: {e}"))?;
    Ok(Some(pgt::listener::Config {
        port,
        pg_port: number("port", pg_port)? as u16,
        database: database.to_string(),
        advertised_host: host.clone(),
        max_clients: number("pg_topics.max_clients", max_clients)?,
        max_message_bytes: number("pg_topics.max_message_bytes", max_bytes)?,
        tls,
    }))
}

#[no_mangle]
#[pg_guard]
pub extern "C" fn pg_topics_listener_main(_arg: pg_sys::Datum) {
    BackgroundWorker::attach_signal_handlers(SignalWakeFlags::SIGTERM);
    let database = BackgroundWorker::get_extra();
    BackgroundWorker::connect_worker_to_spi(Some(database), None);
    let status = match in_transaction(|| listener_settings(database)) {
        Err(reason) => reason,
        Ok(None) => "no listener: pg_topics.port is 0".to_string(),
        Ok(Some(cfg)) => match TcpListener::bind(("0.0.0.0", cfg.port)) {
            Err(e) => format!("bind failed: {e}"),
            Ok(listener) => {
                let port = cfg.port;
                match std::thread::Builder::new()
                    .name("pg_topics listener".into())
                    .spawn(move || pgt::listener::run(listener, cfg))
                {
                    Ok(_) => format!("listening on port {port}"),
                    Err(e) => format!("no listener: {e}"),
                }
            }
        },
    };
    log!("pg_topics listener {database}: {status}");
    let status = CString::new(status).unwrap_or_default();
    unsafe { pg_sys::pgstat_report_activity(pg_sys::BackendState::STATE_RUNNING, status.as_ptr()) };
    while wait_latch(10_000) {}
    unsafe { pg_sys::proc_exit(1) }
}
