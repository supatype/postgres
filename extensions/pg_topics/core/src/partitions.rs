use postgres::{Client, Config, Error, NoTls};

pub fn connect(socket_dir: &str, port: u16, user: &str, dbname: &str) -> Result<Client, Error> {
    let mut client = Config::new()
        .host_path(socket_dir)
        .port(port)
        .user(user)
        .dbname(dbname)
        .application_name("pg_topics partition worker")
        .connect(NoTls)?;
    client.batch_execute("SET search_path = pg_catalog, pg_temp; SET lock_timeout = '1s'")?;
    Ok(client)
}

pub fn message(e: &Error) -> String {
    e.as_db_error()
        .map_or_else(|| e.to_string(), |db| db.message().to_string())
}

fn retention(client: &mut Client, schema: &str, topic: &str) -> Result<(), Error> {
    while let Some(ddl) = client
        .query_one("SELECT topic.retention_next($1, $2)", &[&schema, &topic])?
        .get::<_, Option<String>>(0)
    {
        if let Err(e) = client.batch_execute(&ddl) {
            client.execute(
                "UPDATE topic.topic_config t SET detaching = NULL
                 WHERE t.schema_name = $1 AND t.topic = $2
                 AND NOT EXISTS (SELECT FROM pg_catalog.pg_inherits h
                                 WHERE h.inhrelid = t.detaching AND h.inhdetachpending)",
                &[&schema, &topic],
            )?;
            return Err(e);
        }
    }
    Ok(())
}

fn duplicates(client: &mut Client, schema: &str, topic: &str) -> Result<Vec<String>, Error> {
    Ok(client
        .query(
            "SELECT band, log_offset, copies FROM topic.check_duplicates($1, $2, true)",
            &[&schema, &topic],
        )?
        .iter()
        .map(|row| {
            format!(
                "{schema}.{topic} band {} has {} rows with log_offset {}",
                row.get::<_, i16>(0),
                row.get::<_, i64>(2),
                row.get::<_, i64>(1)
            )
        })
        .collect())
}

fn report(
    client: &Client,
    warn: &mut dyn FnMut(String),
    what: &str,
    result: Result<(), Error>,
) -> Result<(), Error> {
    match result {
        Err(e) if client.is_closed() => Err(e),
        Err(e) => {
            warn(format!("{what}: {}", message(&e)));
            Ok(())
        }
        Ok(()) => Ok(()),
    }
}

pub fn tick(client: &mut Client, sweep: bool, warn: &mut dyn FnMut(String)) -> Result<(), Error> {
    let result = topics_tick(client, sweep, warn);
    report(client, warn, "tick", result)
}

fn topics_tick(
    client: &mut Client,
    sweep: bool,
    warn: &mut dyn FnMut(String),
) -> Result<(), Error> {
    let installed: bool = client
        .query_one(
            "SELECT EXISTS (SELECT FROM pg_catalog.pg_extension WHERE extname = 'pg_topics')",
            &[],
        )?
        .get(0);
    if !installed {
        return Ok(());
    }
    let topics = client.query(
        "SELECT schema_name, topic FROM topic.topic_config ORDER BY schema_name, topic",
        &[],
    )?;
    for row in topics {
        let (schema, topic): (String, String) = (row.get(0), row.get(1));
        let created = client
            .execute(
                "SELECT topic.ensure_partitions($1, $2, 3)",
                &[&schema, &topic],
            )
            .map(drop);
        report(
            client,
            warn,
            &format!("{schema}.{topic} partition creation"),
            created,
        )?;
        let retained = retention(client, &schema, &topic);
        report(
            client,
            warn,
            &format!("{schema}.{topic} retention"),
            retained,
        )?;
        if sweep {
            let swept = duplicates(client, &schema, &topic).map(|found| {
                for line in found {
                    warn(format!("duplicate offset: {line}"));
                }
            });
            report(
                client,
                warn,
                &format!("{schema}.{topic} duplicate check"),
                swept,
            )?;
        }
    }
    let expired = client
        .execute("SELECT topic.expire_groups()", &[])
        .map(drop);
    report(client, warn, "group expiry", expired)?;
    let reaped = client.execute("SELECT topic.reap()", &[]).map(drop);
    report(client, warn, "reap", reaped)
}
