use std::thread;
use std::time::{Duration, Instant};

use bytes::Bytes;
use kafka_protocol::messages::find_coordinator_response::Coordinator;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{
    BrokerId, FindCoordinatorRequest, FindCoordinatorResponse, GroupId, HeartbeatRequest,
    HeartbeatResponse, JoinGroupRequest, JoinGroupResponse, LeaveGroupRequest, LeaveGroupResponse,
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse,
    SyncGroupRequest, SyncGroupResponse, TopicName,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::ResponseError;
use postgres::fallible_iterator::FallibleIterator;
use postgres::types::ToSql;
use postgres::{Client, Error, Row};

use crate::handlers::{band_offsets, band_row};
use crate::listener::IDLE_TIMEOUT;

const POLL: Duration = Duration::from_millis(100);

const GROUP_ERRORS: [(&str, ResponseError); 13] = [
    (
        "GROUP_AUTHORIZATION_FAILED",
        ResponseError::GroupAuthorizationFailed,
    ),
    ("ILLEGAL_GENERATION", ResponseError::IllegalGeneration),
    ("UNKNOWN_MEMBER_ID", ResponseError::UnknownMemberId),
    ("REBALANCE_IN_PROGRESS", ResponseError::RebalanceInProgress),
    ("MEMBER_ID_REQUIRED", ResponseError::MemberIdRequired),
    (
        "INVALID_SESSION_TIMEOUT",
        ResponseError::InvalidSessionTimeout,
    ),
    (
        "INCONSISTENT_GROUP_PROTOCOL",
        ResponseError::InconsistentGroupProtocol,
    ),
    ("NON_EMPTY_GROUP", ResponseError::NonEmptyGroup),
    ("INVALID_GROUP_ID", ResponseError::InvalidGroupId),
    (
        "TOPIC_AUTHORIZATION_FAILED",
        ResponseError::TopicAuthorizationFailed,
    ),
    ("OFFSET_OUT_OF_RANGE", ResponseError::OffsetOutOfRange),
    (
        "UNKNOWN_TOPIC_OR_PARTITION",
        ResponseError::UnknownTopicOrPartition,
    ),
    ("UNKNOWN_SERVER_ERROR", ResponseError::UnknownServerError),
];

pub fn group_error(name: &str) -> i16 {
    if name == "NONE" {
        return 0;
    }
    match GROUP_ERRORS.iter().find(|(n, _)| *n == name) {
        Some((_, e)) => e.code(),
        None => {
            eprintln!("pg_topics listener: a group function returned the unknown error {name}");
            ResponseError::UnknownServerError.code()
        }
    }
}

fn call(
    db: &mut Client,
    sql: &str,
    params: &[&(dyn ToSql + Sync)],
    group: &str,
) -> Result<Result<Row, String>, Error> {
    match db.query_one(sql, params) {
        Ok(row) => Ok(Ok(row)),
        Err(e) => match e.as_db_error() {
            None => Err(e),
            Some(db) if db.code().code() == "42501" => Ok(Err("GROUP_AUTHORIZATION_FAILED".into())),
            Some(db) => {
                eprintln!("pg_topics listener: group {group}: {}", db.message());
                Ok(Err("UNKNOWN_SERVER_ERROR".into()))
            }
        },
    }
}

fn hold<T>(
    db: &mut Client,
    group: &str,
    wait: Duration,
    mut poll: impl FnMut(&mut Client) -> Result<Option<T>, Error>,
) -> Result<Option<T>, Error> {
    let mut tick = Instant::now();
    let deadline = tick + wait.min(IDLE_TIMEOUT);
    db.batch_execute("LISTEN pg_topics_group")?;
    let found = loop {
        if let Some(found) = poll(db)? {
            break Some(found);
        }
        let now = Instant::now();
        if now >= deadline {
            break None;
        }
        tick = (tick + POLL).max(now);
        thread::sleep(tick.min(deadline) - now);
        let end = (tick + POLL).min(deadline);
        while let Some(n) = db
            .notifications()
            .timeout_iter(end.saturating_duration_since(Instant::now()))
            .next()?
        {
            if n.payload() == group {
                break;
            }
        }
    };
    db.batch_execute("UNLISTEN pg_topics_group")?;
    db.notifications().iter().count()?;
    Ok(found)
}

fn millis(ms: i32) -> Duration {
    Duration::from_millis(u64::try_from(ms).unwrap_or(0))
}

pub fn find_coordinator(
    req: &FindCoordinatorRequest,
    version: i16,
    host: &str,
    port: u16,
) -> FindCoordinatorResponse {
    let error = match req.key_type {
        0 | 1 => 0,
        _ => ResponseError::InvalidRequest.code(),
    };
    let node = |key: StrBytes| {
        let found = error == 0;
        Coordinator::default()
            .with_key(key)
            .with_node_id(BrokerId(if found { 0 } else { -1 }))
            .with_host(StrBytes::from_string(if found {
                host.to_string()
            } else {
                String::new()
            }))
            .with_port(if found { i32::from(port) } else { -1 })
            .with_error_code(error)
    };
    if version >= 4 {
        return FindCoordinatorResponse::default()
            .with_coordinators(req.coordinator_keys.iter().cloned().map(node).collect());
    }
    let one = node(req.key.clone());
    FindCoordinatorResponse::default()
        .with_error_code(one.error_code)
        .with_node_id(one.node_id)
        .with_host(one.host)
        .with_port(one.port)
}

pub struct Joined {
    pub member_id: String,
    pub error: Option<String>,
    pub generation: Option<i32>,
    pub protocol: Option<String>,
    pub leader: Option<String>,
    pub members: Vec<(String, Vec<u8>)>,
}

const JOINED: &str = "SELECT j.member_id, j.error, j.generation_id, j.protocol_name, j.leader_id,
        (SELECT array_agg(coalesce(m->>'member_id', '') ORDER BY n)
         FROM jsonb_array_elements(j.members) WITH ORDINALITY x(m, n)
         WHERE octet_length(topic.wire_bytes(m->'metadata')) > 0),
        (SELECT array_agg(topic.wire_bytes(m->'metadata') ORDER BY n)
         FROM jsonb_array_elements(j.members) WITH ORDINALITY x(m, n)
         WHERE octet_length(topic.wire_bytes(m->'metadata')) > 0)";

fn joined(row: Result<Row, String>, member_id: &str) -> Result<Joined, Error> {
    let row = match row {
        Ok(row) => row,
        Err(error) => {
            return Ok(Joined {
                member_id: member_id.to_string(),
                error: Some(error),
                generation: None,
                protocol: None,
                leader: None,
                members: Vec::new(),
            })
        }
    };
    let ids: Option<Vec<String>> = row.try_get(5)?;
    let metadata: Option<Vec<Vec<u8>>> = row.try_get(6)?;
    Ok(Joined {
        member_id: row
            .try_get::<_, Option<String>>(0)?
            .unwrap_or_else(|| member_id.to_string()),
        error: row.try_get(1)?,
        generation: row.try_get(2)?,
        protocol: row.try_get(3)?,
        leader: row.try_get(4)?,
        members: ids
            .unwrap_or_default()
            .into_iter()
            .zip(metadata.unwrap_or_default())
            .collect(),
    })
}

pub fn join_response(j: Joined, protocol_type: &StrBytes) -> JoinGroupResponse {
    let error = group_error(j.error.as_deref().unwrap_or("REBALANCE_IN_PROGRESS"));
    JoinGroupResponse::default()
        .with_error_code(error)
        .with_generation_id(j.generation.unwrap_or(-1))
        .with_protocol_type(Some(protocol_type.clone()))
        .with_protocol_name(Some(StrBytes::from_string(j.protocol.unwrap_or_default())))
        .with_leader(StrBytes::from_string(j.leader.unwrap_or_default()))
        .with_member_id(StrBytes::from_string(j.member_id))
        .with_members(
            j.members
                .into_iter()
                .map(|(id, metadata)| {
                    JoinGroupResponseMember::default()
                        .with_member_id(StrBytes::from_string(id))
                        .with_metadata(Bytes::from(metadata))
                })
                .collect(),
        )
}

pub fn rejoin_id(version: i16, sent: &str, j: &Joined) -> Option<String> {
    (version < 4 && sent.is_empty() && j.error.as_deref() == Some("MEMBER_ID_REQUIRED"))
        .then(|| j.member_id.clone())
}

pub fn join_group(
    db: &mut Client,
    req: &JoinGroupRequest,
    version: i16,
    client_id: &str,
) -> Result<JoinGroupResponse, Error> {
    let group = req.group_id.as_str();
    let rebalance_ms = match version {
        0 => req.session_timeout_ms,
        _ => req.rebalance_timeout_ms,
    };
    let names: Vec<&str> = req.protocols.iter().map(|p| p.name.as_str()).collect();
    let metadata: Vec<&[u8]> = req.protocols.iter().map(|p| &p.metadata[..]).collect();
    let sql = format!(
        "{JOINED}, greatest($5, (SELECT max(m.rebalance_ms) FROM topic.topic_group_members m WHERE m.group_name = $1))
         FROM topic.group_join($1, $2, $3, $4, $5, $6,
             (SELECT coalesce(jsonb_agg(jsonb_build_object('name', u.n,
                                        'metadata', translate(encode(u.m, 'base64'), E'\\n', '')) ORDER BY u.i), '[]')
              FROM unnest($7::text[], $8::bytea[]) WITH ORDINALITY u(n, m, i))) j"
    );
    let mut member_id = req.member_id.to_string();
    let mut wait = rebalance_ms;
    let mut j = loop {
        let row = call(
            db,
            &sql,
            &[
                &group,
                &member_id,
                &client_id,
                &req.session_timeout_ms,
                &rebalance_ms,
                &req.protocol_type.as_str(),
                &names,
                &metadata,
            ],
            group,
        )?;
        if let Ok(row) = &row {
            wait = row.try_get::<_, Option<i32>>(7)?.unwrap_or(rebalance_ms);
        }
        let j = joined(row, &member_id)?;
        match rejoin_id(version, &member_id, &j) {
            Some(id) => member_id = id,
            None => break j,
        }
    };
    if j.error.is_none() {
        let poll = format!("{JOINED} FROM topic.group_join_poll($1, $2) j");
        let member_id = j.member_id.clone();
        let held = hold(db, group, millis(wait), |db| {
            let row = call(db, &poll, &[&group, &member_id], group)?;
            let j = joined(row, &member_id)?;
            Ok(j.error.is_some().then_some(j))
        })?;
        if let Some(held) = held {
            j = held;
        }
    }
    Ok(join_response(j, &req.protocol_type))
}

type Synced = (Option<String>, Option<Vec<u8>>);

fn synced(row: Result<Row, String>, group: &str, member_id: &str) -> Result<Synced, Error> {
    let row = match row {
        Ok(row) => row,
        Err(error) => return Ok((Some(error), None)),
    };
    if row.try_get(2)? {
        eprintln!(
            "pg_topics listener: WARNING: group {group}: the assignment of member {member_id} is not a base64 string, so the member gets an empty assignment"
        );
    }
    Ok((row.try_get(0)?, row.try_get(1)?))
}

pub fn sync_response(req: &SyncGroupRequest, result: Synced) -> SyncGroupResponse {
    SyncGroupResponse::default()
        .with_error_code(group_error(
            result.0.as_deref().unwrap_or("REBALANCE_IN_PROGRESS"),
        ))
        .with_protocol_type(req.protocol_type.clone())
        .with_protocol_name(req.protocol_name.clone())
        .with_assignment(Bytes::from(result.1.unwrap_or_default()))
}

const SYNCED: &str = "SELECT s.error, topic.wire_bytes(s.assignment),
        s.assignment IS NOT NULL AND topic.wire_bytes(s.assignment) IS NULL";

pub fn sync_group(db: &mut Client, req: &SyncGroupRequest) -> Result<SyncGroupResponse, Error> {
    let group = req.group_id.as_str();
    let member_id = req.member_id.as_str();
    let ids: Vec<&str> = req
        .assignments
        .iter()
        .map(|a| a.member_id.as_str())
        .collect();
    let shares: Vec<&[u8]> = req.assignments.iter().map(|a| &a.assignment[..]).collect();
    let row = call(
        db,
        &format!(
            "{SYNCED}, (SELECT m.rebalance_ms FROM topic.topic_group_members m
                        WHERE m.group_name = $1 AND m.member_id = $2)
             FROM topic.group_sync($1, $2, $3,
                 (SELECT coalesce(jsonb_object_agg(u.k, translate(encode(u.v, 'base64'), E'\\n', '')), '{{}}')
                  FROM unnest($4::text[], $5::bytea[]) u(k, v))) s"
        ),
        &[&group, &member_id, &req.generation_id, &ids, &shares],
        group,
    )?;
    let wait = match &row {
        Ok(row) => row.try_get::<_, Option<i32>>(3)?.unwrap_or(0),
        Err(_) => 0,
    };
    let mut result = synced(row, group, member_id)?;
    if result.0.is_none() {
        let poll = format!("{SYNCED} FROM topic.group_sync_poll($1, $2, $3) s");
        result = hold(db, group, millis(wait), |db| {
            let row = call(db, &poll, &[&group, &member_id, &req.generation_id], group)?;
            let result = synced(row, group, member_id)?;
            Ok(result.0.is_some().then_some(result))
        })?
        .unwrap_or((None, None));
    }
    Ok(sync_response(req, result))
}

fn text(row: Result<Row, String>) -> Result<String, Error> {
    match row {
        Ok(row) => Ok(row
            .try_get::<_, Option<String>>(0)?
            .unwrap_or_else(|| "UNKNOWN_SERVER_ERROR".into())),
        Err(error) => Ok(error),
    }
}

pub fn heartbeat(db: &mut Client, req: &HeartbeatRequest) -> Result<HeartbeatResponse, Error> {
    let group = req.group_id.as_str();
    let row = call(
        db,
        "SELECT topic.group_heartbeat($1, $2, $3)",
        &[&group, &req.member_id.as_str(), &req.generation_id],
        group,
    )?;
    Ok(HeartbeatResponse::default().with_error_code(group_error(&text(row)?)))
}

pub fn leave_group(
    db: &mut Client,
    req: &LeaveGroupRequest,
    version: i16,
) -> Result<LeaveGroupResponse, Error> {
    let group = req.group_id.as_str();
    let ids: Vec<StrBytes> = match version {
        0..=2 => vec![req.member_id.clone()],
        _ => req.members.iter().map(|m| m.member_id.clone()).collect(),
    };
    let mut members = Vec::with_capacity(ids.len());
    for id in ids {
        let row = call(
            db,
            "SELECT topic.group_leave($1, $2)",
            &[&group, &id.as_str()],
            group,
        )?;
        members.push(
            MemberResponse::default()
                .with_member_id(id)
                .with_error_code(group_error(&text(row)?)),
        );
    }
    Ok(leave_response(members, version))
}

pub fn leave_response(members: Vec<MemberResponse>, version: i16) -> LeaveGroupResponse {
    match version {
        0..=2 => LeaveGroupResponse::default()
            .with_error_code(members.first().map_or(0, |m| m.error_code)),
        _ => LeaveGroupResponse::default().with_members(members),
    }
}

pub fn offset_commit(
    db: &mut Client,
    req: &OffsetCommitRequest,
) -> Result<OffsetCommitResponse, Error> {
    let group = req.group_id.as_str();
    let mut topics = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        let name = t.name.as_str();
        let offsets = band_offsets(db, name)?;
        let mut parts = Vec::with_capacity(t.partitions.len());
        for p in &t.partitions {
            let error = match band_row(&offsets, p.partition_index) {
                Err(code) => code,
                Ok(_) => {
                    let row = call(
                        db,
                        "SELECT topic.commit_offset($1, $2, $3, $4, $5)",
                        &[
                            &name,
                            &group,
                            &p.partition_index,
                            &p.committed_offset,
                            &req.generation_id_or_member_epoch,
                        ],
                        group,
                    )?;
                    group_error(&text(row)?)
                }
            };
            parts.push(
                OffsetCommitResponsePartition::default()
                    .with_partition_index(p.partition_index)
                    .with_error_code(error),
            );
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(t.name.clone())
                .with_partitions(parts),
        );
    }
    Ok(OffsetCommitResponse::default().with_topics(topics))
}

pub type Committed = Vec<(TopicName, Vec<(i32, i64, i16)>)>;
type Wanted = Option<Vec<(TopicName, Vec<i32>)>>;

fn fetch_group(db: &mut Client, group: &str, wanted: Wanted) -> Result<(Committed, i16), Error> {
    let listed = wanted.is_none();
    let wanted = match wanted {
        Some(wanted) => wanted,
        None => {
            let mut all = Vec::new();
            for row in db.query(
                "SELECT o.schema_name || '.' || o.topic, array_agg(o.band::int ORDER BY o.band)
                 FROM topic.topic_offsets o WHERE o.group_name = $1 GROUP BY 1 ORDER BY 1",
                &[&group],
            )? {
                all.push((
                    TopicName(StrBytes::from_string(row.try_get(0)?)),
                    row.try_get(1)?,
                ));
            }
            all
        }
    };
    let mut group_code = 0;
    let mut topics = Vec::with_capacity(wanted.len());
    for (name, bands) in wanted {
        let offsets = band_offsets(db, name.as_str())?;
        if listed && offsets == Err(ResponseError::TopicAuthorizationFailed.code()) {
            continue;
        }
        let mut parts = Vec::with_capacity(bands.len());
        for band in bands {
            let (offset, error) = match band_row(&offsets, band) {
                Err(code) => (-1, code),
                Ok(_) => match call(
                    db,
                    "SELECT coalesce(topic.fetch_offset($1, $2, $3), -1)",
                    &[&name.as_str(), &group, &band],
                    group,
                )? {
                    Ok(row) => (row.try_get(0)?, 0),
                    Err(error) => (-1, group_error(&error)),
                },
            };
            if error == ResponseError::GroupAuthorizationFailed.code() {
                group_code = error;
            }
            parts.push((band, offset, error));
        }
        topics.push((name, parts));
    }
    Ok((topics, group_code))
}

pub fn offset_fetch(
    db: &mut Client,
    req: &OffsetFetchRequest,
    version: i16,
) -> Result<OffsetFetchResponse, Error> {
    let wanted: Vec<(GroupId, Wanted)> = match version {
        0..=7 => vec![(
            req.group_id.clone(),
            req.topics.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|t| (t.name.clone(), t.partition_indexes.clone()))
                    .collect()
            }),
        )],
        _ => req
            .groups
            .iter()
            .map(|g| {
                (
                    g.group_id.clone(),
                    g.topics.as_ref().map(|topics| {
                        topics
                            .iter()
                            .map(|t| (t.name.clone(), t.partition_indexes.clone()))
                            .collect()
                    }),
                )
            })
            .collect(),
    };
    let mut groups = Vec::with_capacity(wanted.len());
    for (group, topics) in wanted {
        let (topics, code) = fetch_group(db, group.as_str(), topics)?;
        groups.push((group, topics, code));
    }
    Ok(fetch_response(groups, version))
}

pub fn fetch_response(groups: Vec<(GroupId, Committed, i16)>, version: i16) -> OffsetFetchResponse {
    if version < 8 {
        let Some((_, topics, code)) = groups.into_iter().next() else {
            return OffsetFetchResponse::default();
        };
        return OffsetFetchResponse::default()
            .with_error_code(code)
            .with_topics(
                topics
                    .into_iter()
                    .map(|(name, parts)| {
                        OffsetFetchResponseTopic::default()
                            .with_name(name)
                            .with_partitions(
                                parts
                                    .into_iter()
                                    .map(|(band, offset, error)| {
                                        OffsetFetchResponsePartition::default()
                                            .with_partition_index(band)
                                            .with_committed_offset(offset)
                                            .with_metadata(Some(StrBytes::default()))
                                            .with_error_code(error)
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            );
    }
    OffsetFetchResponse::default().with_groups(
        groups
            .into_iter()
            .map(|(group, topics, code)| {
                OffsetFetchResponseGroup::default()
                    .with_group_id(group)
                    .with_error_code(code)
                    .with_topics(
                        topics
                            .into_iter()
                            .map(|(name, parts)| {
                                OffsetFetchResponseTopics::default()
                                    .with_name(name)
                                    .with_partitions(
                                        parts
                                            .into_iter()
                                            .map(|(band, offset, error)| {
                                                OffsetFetchResponsePartitions::default()
                                                    .with_partition_index(band)
                                                    .with_committed_offset(offset)
                                                    .with_metadata(Some(StrBytes::default()))
                                                    .with_error_code(error)
                                            })
                                            .collect(),
                                    )
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::messages::ApiKey;
    use kafka_protocol::protocol::{Decodable, Encodable};

    use super::*;
    use crate::versions::VERSIONS;

    #[test]
    fn group_error_maps_every_name_the_group_functions_return() {
        for (name, code) in [
            ("NONE", 0),
            ("GROUP_AUTHORIZATION_FAILED", 30),
            ("ILLEGAL_GENERATION", 22),
            ("UNKNOWN_MEMBER_ID", 25),
            ("REBALANCE_IN_PROGRESS", 27),
            ("MEMBER_ID_REQUIRED", 79),
            ("INVALID_SESSION_TIMEOUT", 26),
            ("INCONSISTENT_GROUP_PROTOCOL", 23),
            ("NON_EMPTY_GROUP", 68),
            ("INVALID_GROUP_ID", 24),
            ("TOPIC_AUTHORIZATION_FAILED", 29),
            ("OFFSET_OUT_OF_RANGE", 1),
            ("UNKNOWN_TOPIC_OR_PARTITION", 3),
            ("UNKNOWN_SERVER_ERROR", -1),
            ("SOMETHING_NEW", -1),
        ] {
            assert_eq!(group_error(name), code, "{name}");
        }
    }

    fn sql_result(member_id: &str, error: Option<&str>) -> Joined {
        Joined {
            member_id: member_id.into(),
            error: error.map(str::to_string),
            generation: None,
            protocol: None,
            leader: None,
            members: Vec::new(),
        }
    }

    #[test]
    fn a_join_v4_with_an_empty_member_id_gets_member_id_required_and_an_id() {
        let joined = sql_result("rdkafka-8f1c", Some("MEMBER_ID_REQUIRED"));
        assert_eq!(rejoin_id(4, "", &joined), None);
        let resp = join_response(joined, &StrBytes::from_static_str("consumer"));
        assert_eq!(resp.error_code, ResponseError::MemberIdRequired.code());
        assert_eq!(resp.member_id.as_str(), "rdkafka-8f1c");
        assert_eq!(resp.generation_id, -1);
        assert!(resp.members.is_empty());
    }

    #[test]
    fn a_join_below_v4_with_an_empty_member_id_joins_again_with_the_new_id() {
        let required = sql_result("rdkafka-8f1c", Some("MEMBER_ID_REQUIRED"));
        assert_eq!(rejoin_id(0, "", &required), Some("rdkafka-8f1c".into()));
        assert_eq!(rejoin_id(3, "", &required), Some("rdkafka-8f1c".into()));
        assert_eq!(rejoin_id(3, "rdkafka-8f1c", &required), None);
        assert_eq!(rejoin_id(3, "", &sql_result("m", None)), None);
        assert_eq!(rejoin_id(3, "", &sql_result("m", Some("NONE"))), None);
    }

    #[test]
    fn a_held_join_that_times_out_asks_the_member_to_rejoin() {
        let resp = join_response(
            sql_result("m", None),
            &StrBytes::from_static_str("consumer"),
        );
        assert_eq!(resp.error_code, ResponseError::RebalanceInProgress.code());
    }

    fn round_trip<R: Encodable + Decodable + std::fmt::Debug>(
        key: ApiKey,
        build: impl Fn(i16) -> R,
    ) {
        let Some(&(_, lo, hi)) = VERSIONS.iter().find(|v| v.0 == key) else {
            panic!("{key:?} is not advertised");
        };
        for version in lo..=hi {
            let resp = build(version);
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("{key:?} v{version} does not encode: {e}"));
            let back = R::decode(&mut wire.clone().freeze(), version)
                .unwrap_or_else(|e| panic!("{key:?} v{version} does not decode: {e}"));
            let mut again = BytesMut::new();
            back.encode(&mut again, version).unwrap();
            assert_eq!(wire, again, "{key:?} v{version}: {resp:?}");
        }
    }

    #[test]
    fn every_group_response_round_trips_at_every_advertised_version() {
        let name = || StrBytes::from_static_str("consumer");
        for error in [None, Some("NONE"), Some("MEMBER_ID_REQUIRED")] {
            round_trip(ApiKey::JoinGroup, |_| {
                let mut joined = sql_result("m-1", error);
                joined.generation = Some(3);
                joined.protocol = Some("range".into());
                joined.leader = Some("m-1".into());
                joined.members = vec![("m-1".into(), vec![0, 1]), ("m-2".into(), Vec::new())];
                join_response(joined, &name())
            });
        }
        for protocol in [None, Some(name())] {
            let req = SyncGroupRequest::default()
                .with_protocol_type(protocol.clone())
                .with_protocol_name(protocol);
            round_trip(ApiKey::SyncGroup, |_| {
                sync_response(&req, (Some("NONE".into()), Some(vec![0, 1, 2])))
            });
            round_trip(ApiKey::SyncGroup, |_| sync_response(&req, (None, None)));
        }
        round_trip(ApiKey::Heartbeat, |_| {
            HeartbeatResponse::default().with_error_code(group_error("REBALANCE_IN_PROGRESS"))
        });
        round_trip(ApiKey::LeaveGroup, |version| {
            leave_response(
                vec![MemberResponse::default()
                    .with_member_id(StrBytes::from_static_str("m-1"))
                    .with_error_code(group_error("UNKNOWN_MEMBER_ID"))],
                version,
            )
        });
        round_trip(ApiKey::OffsetCommit, |_| {
            OffsetCommitResponse::default().with_topics(vec![OffsetCommitResponseTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("s.t_q")))
                .with_partitions(vec![OffsetCommitResponsePartition::default()
                    .with_partition_index(1)
                    .with_error_code(group_error("ILLEGAL_GENERATION"))])])
        });
        round_trip(ApiKey::OffsetFetch, |version| {
            let topics = vec![(
                TopicName(StrBytes::from_static_str("s.t_q")),
                vec![(0, 7, 0), (1, -1, 3)],
            )];
            fetch_response(
                vec![
                    (GroupId(StrBytes::from_static_str("g")), topics.clone(), 30),
                    (GroupId(StrBytes::from_static_str("h")), topics, 0),
                ],
                version,
            )
        });
        for key_type in [0, 1] {
            let req = FindCoordinatorRequest::default()
                .with_key(StrBytes::from_static_str("g"))
                .with_key_type(key_type)
                .with_coordinator_keys(vec![StrBytes::from_static_str("g")]);
            round_trip(ApiKey::FindCoordinator, |version| {
                find_coordinator(&req, version, "h", 9092)
            });
        }
    }

    #[test]
    fn find_coordinator_answers_node_0_for_groups_and_transactions_and_an_error_for_others() {
        let mut req = FindCoordinatorRequest::default()
            .with_key(StrBytes::from_static_str("g"))
            .with_coordinator_keys(vec![StrBytes::from_static_str("g")]);
        let resp = find_coordinator(&req, 3, "h", 9092);
        assert_eq!(
            (resp.error_code, resp.node_id, resp.port),
            (0, BrokerId(0), 9092)
        );
        let batched = find_coordinator(&req, 4, "h", 9092);
        assert_eq!(batched.coordinators.len(), 1);
        assert_eq!(batched.coordinators[0].host.as_str(), "h");
        req.key_type = 1;
        assert_eq!(
            find_coordinator(&req, 4, "h", 9092).coordinators[0].error_code,
            0
        );
        req.key_type = 2;
        let resp = find_coordinator(&req, 4, "h", 9092);
        assert_eq!(
            resp.coordinators[0].error_code,
            ResponseError::InvalidRequest.code()
        );
        assert_eq!(resp.coordinators[0].node_id, BrokerId(-1));
    }
}
