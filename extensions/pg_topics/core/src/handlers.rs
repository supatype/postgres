use std::net::{IpAddr, Ipv4Addr};
use std::time::{Duration, Instant, SystemTime};

use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::produce_request::PartitionProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{
    ApiVersionsResponse, BrokerId, FetchRequest, FetchResponse, InitProducerIdRequest,
    InitProducerIdResponse, ListOffsetsRequest, ListOffsetsResponse, MetadataRequest,
    MetadataResponse, ProduceRequest, ProduceResponse, ProducerId, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{
    Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType,
};
use kafka_protocol::ResponseError;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::{Client, Config, Error, IsolationLevel, NoTls, Transaction};

use crate::batch::{decode_produce, finish_fetch_batch, Budget, DecodeError};
use crate::partitions::message;
use crate::versions::VERSIONS;

const PASSWORD_METHODS: [&str; 6] = [
    "scram-sha-256:",
    "md5:",
    "password:",
    "ldap:",
    "pam:",
    "radius:",
];

const FETCH_SQL: &str = "SELECT f.log_offset, f.key, f.value::text, h.keys, h.vals,
        floor(extract(epoch FROM f.published_at) * 1000)::int8
    FROM topic.fetch($1, $2, $3, $4) f
    LEFT JOIN LATERAL (
        SELECT array_agg(e->>'key' ORDER BY n) AS keys,
               array_agg(CASE WHEN e ? 'value_base64' THEN topic.wire_bytes(e->'value_base64')
                              ELSE convert_to(e->>'value', 'UTF8') END ORDER BY n) AS vals
        FROM jsonb_array_elements(CASE WHEN jsonb_typeof(f.headers) = 'array' THEN f.headers END)
             WITH ORDINALITY x(e, n)
        WHERE jsonb_typeof(e) = 'object' AND e->>'key' IS NOT NULL) h ON true
    ORDER BY f.log_offset";

pub fn error_code(sqlstate: &str) -> i16 {
    match sqlstate {
        "22P02" | "22021" | "22001" | "22P05" => ResponseError::InvalidRecord.code(),
        "42501" => ResponseError::TopicAuthorizationFailed.code(),
        "PT001" => ResponseError::OffsetOutOfRange.code(),
        "PT002" => ResponseError::OutOfOrderSequenceNumber.code(),
        "PT003" => ResponseError::InvalidProducerEpoch.code(),
        "PT004" => ResponseError::UnknownProducerId.code(),
        "42P01" => ResponseError::UnknownTopicOrPartition.code(),
        "57014" => ResponseError::RequestTimedOut.code(),
        _ => ResponseError::UnknownServerError.code(),
    }
}

fn db_code(e: Error, topic: &str) -> Result<i16, Error> {
    let Some(db) = e.as_db_error() else {
        return Err(e);
    };
    let code = error_code(db.code().code());
    if code == ResponseError::UnknownServerError.code() {
        eprintln!("pg_topics listener: {topic}: {}", db.message());
    }
    Ok(code)
}

fn split(topic: &str) -> (&str, &str) {
    topic.split_once('.').unwrap_or((topic, ""))
}

fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn json_string(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
}

pub fn api_versions(error_code: i16) -> ApiVersionsResponse {
    ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(
            VERSIONS
                .iter()
                .map(|&(key, lo, hi)| {
                    ApiVersion::default()
                        .with_api_key(key as i16)
                        .with_min_version(lo)
                        .with_max_version(hi)
                })
                .collect(),
        )
}

pub fn authenticate(pg_port: u16, database: &str, auth_bytes: &[u8]) -> Result<Client, String> {
    let text = std::str::from_utf8(auth_bytes).map_err(|_| "the PLAIN message is not UTF-8")?;
    let mut parts = text.split('\0');
    let (Some(authzid), Some(user), Some(password), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err("the PLAIN message does not have three parts".into());
    };
    if user.is_empty() || !(authzid.is_empty() || authzid == user) {
        return Err("the PLAIN message has no user, or an authzid that is not the user".into());
    }
    let mut config = Config::new();
    config
        .hostaddr(IpAddr::V4(Ipv4Addr::LOCALHOST))
        .port(pg_port)
        .dbname(database)
        .user(user)
        .password(password)
        .application_name("pg_topics listener")
        .connect_timeout(Duration::from_secs(10));
    let mut client = config.connect(NoTls).map_err(|e| message(&e))?;
    client
        .batch_execute("SET search_path = pg_catalog, pg_temp")
        .map_err(|e| message(&e))?;
    let method: Option<String> = client
        .query_one("SELECT system_user", &[])
        .and_then(|row| row.try_get(0))
        .map_err(|e| message(&e))?;
    match method {
        Some(m) if PASSWORD_METHODS.iter().any(|p| m.starts_with(p)) => Ok(client),
        m => Err(format!(
            "the connection to 127.0.0.1 did not use a password method (system_user is {m:?}); pg_hba.conf needs a password line for 127.0.0.1/32"
        )),
    }
}

pub(crate) fn cluster_id(db: &mut Client) -> Result<String, Error> {
    let id: i64 = db
        .query_one("SELECT system_identifier FROM pg_control_system()", &[])?
        .try_get(0)?;
    Ok(id.to_string())
}

pub fn metadata(
    db: &mut Client,
    host: &str,
    port: u16,
    req: &MetadataRequest,
    version: i16,
) -> Result<MetadataResponse, Error> {
    let names: Option<Vec<String>> = req
        .topics
        .as_ref()
        .filter(|t| version > 0 || !t.is_empty())
        .map(|t| {
            t.iter()
                .filter_map(|t| t.name.as_ref().map(|n| n.to_string()))
                .collect()
        });
    let mut found = Vec::new();
    for row in db.query(
        "SELECT topic, band_count, visible FROM topic.kafka_topics($1)",
        &[&names],
    )? {
        found.push((
            row.try_get::<_, String>(0)?,
            row.try_get::<_, i16>(1)?,
            row.try_get::<_, bool>(2)?,
        ));
    }
    let topic = |name: &str, error: i16, bands: i16| {
        MetadataResponseTopic::default()
            .with_name(Some(TopicName(StrBytes::from_string(name.to_string()))))
            .with_error_code(error)
            .with_partitions(
                (0..i32::from(bands))
                    .map(|b| {
                        MetadataResponsePartition::default()
                            .with_partition_index(b)
                            .with_leader_id(BrokerId(0))
                            .with_replica_nodes(vec![BrokerId(0)])
                            .with_isr_nodes(vec![BrokerId(0)])
                    })
                    .collect(),
            )
    };
    let mut topics: Vec<MetadataResponseTopic> = found
        .iter()
        .filter(|(_, _, visible)| *visible || names.is_some())
        .map(|(name, bands, visible)| match visible {
            true => topic(name, 0, *bands),
            false => topic(name, ResponseError::TopicAuthorizationFailed.code(), 0),
        })
        .collect();
    for name in names.iter().flatten() {
        if !found.iter().any(|(n, _, _)| n == name) {
            topics.push(topic(
                name,
                ResponseError::TopicAuthorizationFailed.code(),
                0,
            ));
        }
    }
    Ok(MetadataResponse::default()
        .with_brokers(vec![MetadataResponseBroker::default()
            .with_node_id(BrokerId(0))
            .with_host(StrBytes::from_string(host.to_string()))
            .with_port(i32::from(port))])
        .with_cluster_id(Some(StrBytes::from_string(cluster_id(db)?)))
        .with_controller_id(BrokerId(0))
        .with_topics(topics))
}

fn band_of(band_count: Option<i16>, index: i32) -> Option<i16> {
    band_count
        .filter(|&n| (0..i32::from(n)).contains(&index))
        .map(|_| index as i16)
}

struct Rows {
    keys: Vec<Option<String>>,
    values: Vec<Option<String>>,
    headers: Vec<Option<String>>,
    timestamps: Vec<i64>,
}

fn rows(records: &[Record]) -> Option<Rows> {
    let text = |b: &Bytes| std::str::from_utf8(b).ok().map(str::to_string);
    let mut rows = Rows {
        keys: Vec::with_capacity(records.len()),
        values: Vec::with_capacity(records.len()),
        headers: Vec::with_capacity(records.len()),
        timestamps: Vec::with_capacity(records.len()),
    };
    for r in records {
        let key = match &r.key {
            Some(k) => Some(text(k).filter(|k| k.chars().count() <= 40)?),
            None => None,
        };
        let value = match &r.value {
            Some(v) => Some(text(v)?),
            None => None,
        };
        let headers = if r.headers.is_empty() {
            None
        } else {
            let mut json = String::from("[");
            for (i, (k, v)) in r.headers.iter().enumerate() {
                json.push_str(if i == 0 { "{\"key\":" } else { ",{\"key\":" });
                json_string(&mut json, k.as_str());
                match v {
                    Some(v) => match std::str::from_utf8(v) {
                        Ok(v) => {
                            json.push_str(",\"value\":");
                            json_string(&mut json, v);
                        }
                        Err(_) => {
                            json.push_str(",\"value_base64\":\"");
                            STANDARD.encode_string(v, &mut json);
                            json.push('"');
                        }
                    },
                    None => json.push_str(",\"value\":null"),
                }
                json.push('}');
            }
            json.push(']');
            Some(json)
        };
        rows.keys.push(key);
        rows.values.push(value);
        rows.headers.push(headers);
        rows.timestamps.push(r.timestamp);
    }
    Some(rows)
}

type FirstRow = (SystemTime, i64);

const DEFAULT_PRODUCE_WAIT: Duration = Duration::from_secs(30);

fn produce_band(
    tx: &mut Transaction,
    topic: &str,
    band_count: Option<i16>,
    part: &PartitionProduceData,
    max_message_bytes: usize,
    budget: &Budget,
) -> Result<Result<(i64, Option<FirstRow>), i16>, Error> {
    let records = part.records.clone().unwrap_or_default();
    if records.len() > max_message_bytes {
        return Ok(Err(ResponseError::MessageTooLarge.code()));
    }
    let Some(band_count) = band_count else {
        return Ok(Err(ResponseError::TopicAuthorizationFailed.code()));
    };
    let Some(band) = band_of(Some(band_count), part.index) else {
        return Ok(Err(ResponseError::UnknownTopicOrPartition.code()));
    };
    let records = match decode_produce(records, budget) {
        Ok(records) => records,
        Err(DecodeError::TooLarge) => return Ok(Err(ResponseError::MessageTooLarge.code())),
        Err(DecodeError::Corrupt(e)) => {
            eprintln!("pg_topics listener: {topic}: a produce batch does not decode: {e}");
            return Ok(Err(ResponseError::CorruptMessage.code()));
        }
    };
    let Some(rows) = rows(&records) else {
        return Ok(Err(ResponseError::InvalidRecord.code()));
    };
    let (schema, table) = split(topic);
    let sql = format!(
        "WITH ins AS (
             INSERT INTO {}.{} (band, key, value, headers, producer_timestamp)
             SELECT $1::smallint, u.k, u.v::jsonb, u.h::jsonb, to_timestamp(nullif(u.ts, -1) / 1000.0)
             FROM unnest($2::text[], $3::text[], $4::text[], $5::int8[]) WITH ORDINALITY u(k, v, h, ts, n)
             ORDER BY u.n
             RETURNING clock_timestamp() AS at)
         SELECT floor(extract(epoch FROM i.at) * 1000)::int8, r.published_at, r.seq
         FROM (SELECT min(at) AS at, count(*)::int AS n FROM ins) i
         LEFT JOIN LATERAL topic.produced_row($6, $7, $1::smallint, i.n) r ON true",
        quote(schema),
        quote(table)
    );
    let mut sp = tx.savepoint("pg_topics_band")?;
    let mut write = || -> Result<(bool, Option<i64>, Option<FirstRow>), Error> {
        let row = sp.query_one(
            &sql,
            &[
                &band,
                &rows.keys,
                &rows.values,
                &rows.headers,
                &rows.timestamps,
                &schema,
                &table,
            ],
        )?;
        let (at, published_at, seq) = (
            row.try_get::<_, Option<i64>>(0)?,
            row.try_get::<_, Option<SystemTime>>(1)?,
            row.try_get::<_, Option<i64>>(2)?,
        );
        let idempotent = records.first().filter(|r| r.producer_id >= 0);
        let (Some(first), Some(last), Some((published_at, seq))) =
            (idempotent, last_sequence(&records), published_at.zip(seq))
        else {
            return Ok((false, at, published_at.zip(seq)));
        };
        let check = sp.query_one(
            "SELECT duplicate, base_published_at, base_seq
             FROM topic.produce_check($1, $2, $3, $4, $5, $6, $7, $8, $9)",
            &[
                &schema,
                &table,
                &first.producer_id,
                &first.producer_epoch,
                &band,
                &first.sequence,
                &last,
                &published_at,
                &seq,
            ],
        )?;
        let duplicate = check.try_get::<_, bool>(0)?;
        let base = check
            .try_get::<_, Option<SystemTime>>(1)?
            .zip(check.try_get::<_, Option<i64>>(2)?);
        Ok((duplicate, if duplicate { None } else { at }, base))
    };
    match write() {
        Ok((duplicate, at, base)) => {
            if duplicate {
                sp.rollback()?;
            } else {
                sp.commit()?;
            }
            Ok(Ok((at.unwrap_or(-1), base)))
        }
        Err(e) => {
            sp.rollback()?;
            Ok(Err(db_code(e, topic)?))
        }
    }
}

fn last_sequence(records: &[Record]) -> Option<i32> {
    records.last().map(|r| r.sequence & i32::MAX)
}

pub fn produce(
    db: &mut Client,
    req: &ProduceRequest,
    max_message_bytes: usize,
) -> Result<Produced, Error> {
    let wait = match req.timeout_ms {
        ms if ms > 0 => max_wait(ms),
        _ => DEFAULT_PRODUCE_WAIT,
    };
    let deadline = Instant::now() + wait;
    let mut band_counts = Vec::with_capacity(req.topic_data.len());
    for t in &req.topic_data {
        let (schema, table) = split(t.name.as_str());
        band_counts.push(
            db.query_one(
                "SELECT CASE WHEN EXISTS (
                     SELECT FROM pg_catalog.pg_class r JOIN pg_catalog.pg_namespace n ON n.oid = r.relnamespace
                     WHERE n.nspname = $1 AND r.relname = $2
                       AND pg_catalog.has_any_column_privilege(r.oid, 'INSERT'))
                 THEN topic.band_count($1, $2) END",
                &[&schema, &table],
            )?
                .try_get::<_, Option<i16>>(0)?,
        );
    }
    let mut tx = db
        .build_transaction()
        .isolation_level(IsolationLevel::ReadCommitted)
        .start()?;
    tx.execute(
        "SELECT set_config('synchronous_commit',
             CASE WHEN NOT $1 THEN 'off'
                  WHEN current_setting('synchronous_standby_names') = '' THEN 'on'
                  ELSE 'remote_apply' END, true)",
        &[&(req.acks == -1)],
    )?;
    tx.batch_execute(&format!(
        "SET LOCAL statement_timeout = {}",
        wait.as_millis()
    ))?;
    let budget = Budget::new(max_message_bytes.saturating_mul(16));
    let mut responses = Vec::with_capacity(req.topic_data.len());
    let mut pending = Vec::new();
    for (t, band_count) in req.topic_data.iter().zip(band_counts) {
        let mut parts = Vec::with_capacity(t.partition_data.len());
        for p in &t.partition_data {
            let (error, at) = match produce_band(
                &mut tx,
                t.name.as_str(),
                band_count,
                p,
                max_message_bytes,
                &budget,
            )? {
                Ok((at, first)) => {
                    if let Some(first) = first {
                        pending.push((responses.len(), parts.len(), first));
                    }
                    (0, at)
                }
                Err(code) => (code, -1),
            };
            parts.push(
                PartitionProduceResponse::default()
                    .with_index(p.index)
                    .with_error_code(error)
                    .with_base_offset(-1)
                    .with_log_append_time_ms(at),
            );
        }
        responses.push(
            TopicProduceResponse::default()
                .with_name(t.name.clone())
                .with_partition_responses(parts),
        );
    }
    tx.batch_execute("SET LOCAL statement_timeout = 0")?;
    if let Err(e) = tx.commit() {
        let code = db_code(e, "COMMIT")?;
        for p in responses
            .iter_mut()
            .flat_map(|t| t.partition_responses.iter_mut())
        {
            if p.error_code == 0 {
                p.error_code = code;
                p.log_append_time_ms = -1;
            }
        }
        pending.clear();
    }
    Ok(Produced {
        responses,
        pending,
        deadline,
    })
}

pub struct Produced {
    responses: Vec<TopicProduceResponse>,
    pending: Vec<Pending>,
    deadline: Instant,
}

pub fn produce_ready(db: &mut Client, produced: &mut Produced) -> Result<bool, Error> {
    if !produced.pending.is_empty() {
        read_base_offsets(db, &mut produced.responses, &mut produced.pending)?;
    }
    Ok(produced.pending.is_empty())
}

pub fn finish_produce(db: &mut Client, mut produced: Produced) -> Result<ProduceResponse, Error> {
    base_offsets(
        db,
        &mut produced.responses,
        produced.pending,
        produced.deadline,
    )?;
    Ok(ProduceResponse::default().with_responses(produced.responses))
}

type Pending = (usize, usize, FirstRow);

fn read_base_offsets(
    db: &mut Client,
    responses: &mut [TopicProduceResponse],
    pending: &mut Vec<Pending>,
) -> Result<(), Error> {
    let names: Vec<(&str, &str)> = pending
        .iter()
        .map(|&(t, _, _)| split(responses[t].name.as_str()))
        .collect();
    let offsets = db.query(
        "SELECT topic.produced_offset(u.s, u.t, u.at, u.seq)
         FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::int8[]) WITH ORDINALITY u(s, t, at, seq, n)
         ORDER BY u.n",
        &[
            &names.iter().map(|n| n.0).collect::<Vec<_>>(),
            &names.iter().map(|n| n.1).collect::<Vec<_>>(),
            &pending.iter().map(|&(_, _, (at, _))| at).collect::<Vec<_>>(),
            &pending.iter().map(|&(_, _, (_, seq))| seq).collect::<Vec<_>>(),
        ],
    )?;
    let mut left = Vec::new();
    for (p, row) in pending.drain(..).zip(offsets) {
        match row.try_get::<_, Option<i64>>(0)? {
            Some(offset) => responses[p.0].partition_responses[p.1].base_offset = offset,
            None => left.push(p),
        }
    }
    *pending = left;
    Ok(())
}

fn base_offsets(
    db: &mut Client,
    responses: &mut [TopicProduceResponse],
    mut pending: Vec<Pending>,
    deadline: Instant,
) -> Result<(), Error> {
    if !pending.is_empty() {
        read_base_offsets(db, responses, &mut pending)?;
    }
    if !pending.is_empty() {
        db.batch_execute("LISTEN pg_topics_stamped")?;
        read_base_offsets(db, responses, &mut pending)?;
        while !pending.is_empty() {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let woke = db
                .notifications()
                .timeout_iter(left.min(Duration::from_secs(5)))
                .next()?;
            if woke.is_some_and(|n| {
                pending
                    .iter()
                    .any(|&(t, _, _)| responses[t].name.as_str() == n.payload())
            }) {
                read_base_offsets(db, responses, &mut pending)?;
            }
        }
        db.batch_execute("UNLISTEN pg_topics_stamped")?;
        db.notifications().iter().count()?;
    }
    for (t, p, _) in pending {
        let part = &mut responses[t].partition_responses[p];
        part.error_code = ResponseError::RequestTimedOut.code();
        part.log_append_time_ms = -1;
    }
    Ok(())
}

pub fn init_producer_id(
    db: &mut Client,
    req: &InitProducerIdRequest,
) -> Result<InitProducerIdResponse, Error> {
    if req.transactional_id.is_some() {
        return Ok(InitProducerIdResponse::default()
            .with_error_code(ResponseError::TransactionalIdAuthorizationFailed.code())
            .with_producer_epoch(-1));
    }
    let id = db
        .query_one("SELECT topic.init_producer_id()", &[])?
        .try_get(0)?;
    Ok(InitProducerIdResponse::default().with_producer_id(ProducerId(id)))
}

type BandOffsets = Result<Vec<(i32, i64, i64)>, i16>;

pub(crate) fn band_offsets(db: &mut Client, topic: &str) -> Result<BandOffsets, Error> {
    match db.query(
        "SELECT band::int, oldest_offset, next_offset FROM topic.band_offsets($1)",
        &[&topic],
    ) {
        Ok(rows) => Ok(Ok(rows
            .iter()
            .map(|r| Ok((r.try_get(0)?, r.try_get(1)?, r.try_get(2)?)))
            .collect::<Result<_, Error>>()?)),
        Err(e) => Ok(Err(db_code(e, topic)?)),
    }
}

pub(crate) fn band_row(offsets: &BandOffsets, band: i32) -> Result<(i64, i64), i16> {
    offsets
        .as_ref()
        .map_err(|code| *code)?
        .iter()
        .find(|r| r.0 == band)
        .map(|r| (r.1, r.2))
        .ok_or(ResponseError::UnknownTopicOrPartition.code())
}

fn read_band(
    db: &mut Client,
    topic: &str,
    band: i32,
    from: i64,
    budget: usize,
    total: &mut usize,
    max_bytes: usize,
) -> Result<Result<Option<Bytes>, i16>, Error> {
    let limit: i32 = 2000;
    let params: [&(dyn ToSql + Sync); 4] = [&topic, &band, &from, &limit];
    let mut rows = match db.query_raw(FETCH_SQL, params) {
        Ok(rows) => rows,
        Err(e) => return Ok(Err(db_code(e, topic)?)),
    };
    let mut records = Vec::new();
    let mut size = 0usize;
    loop {
        let row = match rows.next() {
            Ok(Some(row)) => row,
            Ok(None) => break,
            Err(e) => return Ok(Err(db_code(e, topic)?)),
        };
        let key: Option<String> = row.try_get(1)?;
        let value: Option<String> = row.try_get(2)?;
        let keys: Option<Vec<String>> = row.try_get(3)?;
        let vals: Option<Vec<Option<Vec<u8>>>> = row.try_get(4)?;
        let mut headers = Vec::new();
        for (k, v) in keys
            .unwrap_or_default()
            .into_iter()
            .zip(vals.unwrap_or_default())
        {
            headers.push((StrBytes::from_string(k), v.map(Bytes::from)));
        }
        let len = 24
            + key.as_ref().map_or(0, String::len)
            + value.as_ref().map_or(0, String::len)
            + headers
                .iter()
                .map(|(k, v)| k.len() + v.as_ref().map_or(0, Bytes::len) + 8)
                .sum::<usize>();
        if !(records.is_empty() && *total == 0)
            && (size + len > budget || *total + size + len > max_bytes)
        {
            break;
        }
        size += len;
        let offset: i64 = row.try_get(0)?;
        records.push(Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::LogAppend,
            offset,
            sequence: offset as i32,
            timestamp: row.try_get(5)?,
            key: key.map(Bytes::from),
            value: value.map(Bytes::from),
            headers,
        });
    }
    drop(rows);
    *total += size;
    if records.is_empty() {
        return Ok(Ok(None));
    }
    let mut buf = BytesMut::new();
    let options = RecordEncodeOptions {
        version: 2,
        compression: Compression::None,
    };
    if let Err(e) = RecordBatchEncoder::encode(&mut buf, &records, &options) {
        eprintln!("pg_topics listener: {topic}: a fetch batch does not encode: {e}");
        return Ok(Err(ResponseError::UnknownServerError.code()));
    }
    finish_fetch_batch(&mut buf);
    Ok(Ok(Some(buf.freeze())))
}

fn read_fetch(
    db: &mut Client,
    req: &FetchRequest,
) -> Result<(Vec<FetchableTopicResponse>, usize), Error> {
    let max_bytes = usize::try_from(req.max_bytes)
        .ok()
        .filter(|&n| n > 0)
        .unwrap_or(usize::MAX);
    let mut total = 0usize;
    let mut responses = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let name = t.topic.as_str();
        let offsets = band_offsets(db, name)?;
        let mut parts = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let mut data = PartitionData::default().with_partition_index(p.partition);
            match band_row(&offsets, p.partition) {
                Err(code) => data.error_code = code,
                Ok((oldest, next)) => {
                    data = data
                        .with_high_watermark(next)
                        .with_last_stable_offset(next)
                        .with_log_start_offset(oldest);
                    if p.fetch_offset > next {
                        data.error_code = ResponseError::OffsetOutOfRange.code();
                    } else if p.fetch_offset < next {
                        let budget = usize::try_from(p.partition_max_bytes).unwrap_or(0);
                        match read_band(
                            db,
                            name,
                            p.partition,
                            p.fetch_offset,
                            budget,
                            &mut total,
                            max_bytes,
                        )? {
                            Ok(records) => data.records = records,
                            Err(code) => data.error_code = code,
                        }
                    }
                }
            }
            parts.push(data);
        }
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(t.topic.clone())
                .with_partitions(parts),
        );
    }
    Ok((responses, total))
}

fn max_wait(ms: i32) -> Duration {
    Duration::from_millis(ms.clamp(0, 30_000) as u64)
}

pub fn fetch(db: &mut Client, req: &FetchRequest) -> Result<FetchResponse, Error> {
    let deadline = Instant::now() + max_wait(req.max_wait_ms);
    let short = |read: &(Vec<FetchableTopicResponse>, usize)| {
        (read.1 as i64) < i64::from(req.min_bytes)
            && read
                .0
                .iter()
                .flat_map(|t| &t.partitions)
                .all(|p| p.error_code == 0)
    };
    let mut read = read_fetch(db, req)?;
    if short(&read) && Instant::now() < deadline {
        db.batch_execute("LISTEN pg_topics_stamped")?;
        read = read_fetch(db, req)?;
        while short(&read) {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let woke = db
                .notifications()
                .timeout_iter(left.min(Duration::from_secs(5)))
                .next()?;
            if woke.is_some_and(|n| req.topics.iter().any(|t| t.topic.as_str() == n.payload())) {
                read = read_fetch(db, req)?;
            }
        }
        db.batch_execute("UNLISTEN pg_topics_stamped")?;
        db.notifications().iter().count()?;
    }
    Ok(FetchResponse::default().with_responses(read.0))
}

pub fn list_offsets(
    db: &mut Client,
    req: &ListOffsetsRequest,
) -> Result<ListOffsetsResponse, Error> {
    let mut topics = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let name = t.name.as_str();
        let offsets = band_offsets(db, name)?;
        let mut parts = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let mut r =
                ListOffsetsPartitionResponse::default().with_partition_index(p.partition_index);
            match (band_row(&offsets, p.partition_index), p.timestamp) {
                (Err(code), _) => r.error_code = code,
                (Ok((oldest, _)), -2) => r.offset = oldest,
                (Ok((_, next)), -1) => r.offset = next,
                (Ok((oldest, next)), -3) if next > oldest => r.offset = next - 1,
                (Ok(_), -3) => {}
                (Ok(_), ts) => match db.query(
                    "SELECT f.log_offset, floor(extract(epoch FROM f.published_at) * 1000)::int8
                     FROM topic.fetch($1, $2, topic.offset_for_time($1, $2, to_timestamp($3::int8 / 1000.0)), 1) f",
                    &[&name, &p.partition_index, &ts],
                ) {
                    Ok(rows) => {
                        if let Some(row) = rows.first() {
                            r.offset = row.try_get(0)?;
                            r.timestamp = row.try_get(1)?;
                        }
                    }
                    Err(e) => r.error_code = db_code(e, name)?,
                },
            }
            parts.push(r);
        }
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(t.name.clone())
                .with_partitions(parts),
        );
    }
    Ok(ListOffsetsResponse::default().with_topics(topics))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_maps_the_design_table() {
        for (state, code) in [
            ("22P02", ResponseError::InvalidRecord),
            ("22021", ResponseError::InvalidRecord),
            ("22001", ResponseError::InvalidRecord),
            ("22P05", ResponseError::InvalidRecord),
            ("42501", ResponseError::TopicAuthorizationFailed),
            ("PT001", ResponseError::OffsetOutOfRange),
            ("PT002", ResponseError::OutOfOrderSequenceNumber),
            ("42P01", ResponseError::UnknownTopicOrPartition),
            ("PT003", ResponseError::InvalidProducerEpoch),
            ("PT004", ResponseError::UnknownProducerId),
            ("57014", ResponseError::RequestTimedOut),
            ("P0001", ResponseError::UnknownServerError),
        ] {
            assert_eq!(error_code(state), code.code(), "{state}");
        }
    }

    #[test]
    fn last_sequence_of_a_batch_across_the_wrap_restarts_at_0() {
        let records: Vec<Record> = (0..61)
            .map(|i| Record {
                producer_id: 1,
                offset: i,
                sequence: 2147483600i32.wrapping_add(i as i32),
                ..record(None, Some(b"1"))
            })
            .collect();
        let mut buf = BytesMut::new();
        let options = RecordEncodeOptions {
            version: 2,
            compression: Compression::None,
        };
        RecordBatchEncoder::encode(&mut buf, &records, &options).unwrap();
        let decoded = decode_produce(buf.freeze(), &Budget::new(1 << 20)).unwrap();
        assert_eq!(decoded[0].sequence, 2147483600);
        assert_eq!(last_sequence(&decoded), Some(12));
    }

    #[test]
    fn max_wait_is_capped_at_30_seconds() {
        assert_eq!(max_wait(500), Duration::from_millis(500));
        assert_eq!(max_wait(i32::MAX), Duration::from_secs(30));
        assert_eq!(max_wait(-1), Duration::ZERO);
    }

    #[test]
    fn band_of_refuses_a_band_outside_band_count() {
        assert_eq!(band_of(Some(4), 3), Some(3));
        assert_eq!(band_of(Some(4), 4), None);
        assert_eq!(band_of(Some(4), -1), None);
        assert_eq!(band_of(None, 0), None);
    }

    fn record(key: Option<&'static [u8]>, value: Option<&'static [u8]>) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: -1,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: 0,
            timestamp: -1,
            key: key.map(Bytes::from_static),
            value: value.map(Bytes::from_static),
            headers: Vec::new(),
        }
    }

    #[test]
    fn rows_refuse_bad_keys_and_values() {
        let long = b"0123456789012345678901234567890123456789x";
        assert!(rows(&[record(Some(&long[..40]), Some(b"{}"))]).is_some());
        assert!(rows(&[record(Some(long), Some(b"{}"))]).is_none());
        assert!(rows(&[record(Some(b"\xff"), None)]).is_none());
        assert!(rows(&[record(None, Some(b"\xc3"))]).is_none());
    }

    #[test]
    fn rows_keep_repeated_binary_null_and_empty_header_values() {
        let mut r = record(None, None);
        r.headers = [
            ("a".into(), Some(Bytes::from_static(b"1"))),
            ("a".into(), Some(Bytes::from_static(b"2"))),
            ("bin".into(), Some(Bytes::from_static(b"\xff\x00\x01"))),
            ("n".into(), None),
            ("e".into(), Some(Bytes::new())),
        ]
        .into();
        assert_eq!(
            rows(&[r]).map(|rows| rows.headers),
            Some(vec![Some(
                concat!(
                    r#"[{"key":"a","value":"1"},{"key":"a","value":"2"},"#,
                    r#"{"key":"bin","value_base64":"/wAB"},{"key":"n","value":null},"#,
                    r#"{"key":"e","value":""}]"#
                )
                .to_string()
            )])
        );
    }

    #[test]
    fn rows_write_headers_as_a_json_array() {
        let mut r = record(Some("Zürich".as_bytes()), None);
        r.headers.push((
            StrBytes::from_static_str("a\"b"),
            Some(Bytes::from_static(b"x\n\\")),
        ));
        r.headers.push((StrBytes::from_static_str("n"), None));
        let rows = rows(&[r]).unwrap();
        assert_eq!(rows.keys, vec![Some("Zürich".to_string())]);
        assert_eq!(rows.values, vec![None]);
        assert_eq!(rows.timestamps, vec![-1]);
        assert_eq!(
            rows.headers,
            vec![Some(
                r#"[{"key":"a\"b","value":"x\u000a\\"},{"key":"n","value":null}]"#.to_string()
            )]
        );
    }
}
