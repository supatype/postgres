use bytes::Bytes;
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::alter_configs_response::AlterConfigsResourceResponse;
use kafka_protocol::messages::create_topics_request::CreatableTopic;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::describe_cluster_response::DescribeClusterBroker;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::describe_configs_response::{
    DescribeConfigsResourceResult, DescribeConfigsResult,
};
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource as IncAlterConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_response::AlterConfigsResourceResponse as IncAlterConfigsResourceResponse;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{
    AlterConfigsRequest, AlterConfigsResponse, BrokerId, CreateTopicsRequest, CreateTopicsResponse,
    DeleteGroupsRequest, DeleteGroupsResponse, DeleteTopicsRequest, DeleteTopicsResponse,
    DescribeClusterResponse, DescribeConfigsRequest, DescribeConfigsResponse,
    DescribeGroupsRequest, DescribeGroupsResponse, GroupId, IncrementalAlterConfigsRequest,
    IncrementalAlterConfigsResponse, ListGroupsRequest, ListGroupsResponse,
};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::ResponseError;
use postgres::{Client, Error, Transaction};

const TOPIC_RESOURCE: i8 = 2;
const OP_SET: i8 = 0;
const OP_DELETE: i8 = 1;

fn detail(db: &postgres::error::DbError) -> String {
    match db.hint() {
        Some(hint) => format!("{} ({hint})", db.message()),
        None => db.message().to_string(),
    }
}

fn topic_error(e: Error, action: &str) -> Result<(i16, Option<String>), Error> {
    let Some(db) = e.as_db_error() else {
        return Err(e);
    };
    Ok(match db.code().code() {
        "42501" | "42P01" | "3F000" => (ResponseError::TopicAuthorizationFailed.code(), None),
        "23505" => (ResponseError::TopicAlreadyExists.code(), Some(detail(db))),
        "P0001" => (ResponseError::InvalidRequest.code(), Some(detail(db))),
        _ => {
            eprintln!("pg_topics listener: {action}: {}", db.message());
            (ResponseError::UnknownServerError.code(), None)
        }
    })
}

type ConfigError = (i16, String);
const DEFAULT_RETENTION_MS: i64 = 7 * 24 * 3600 * 1000;
const MAX_RETENTION_MS: i64 = 100 * 365 * 24 * 3600 * 1000;
const DEFAULT_MIN_DURABILITY: &str = "durable";

fn validate_retention_ms(text: &str) -> Result<i64, ConfigError> {
    let ms: i64 = text.parse().map_err(|_| {
        (
            ResponseError::InvalidConfig.code(),
            format!("retention.ms value {text} is not an integer"),
        )
    })?;
    if ms == -1 {
        return Err((
            ResponseError::InvalidConfig.code(),
            "pg_topics needs a finite retention, not -1 (Kafka's keep forever)".to_string(),
        ));
    }
    if !(1..=MAX_RETENTION_MS).contains(&ms) {
        return Err((
            ResponseError::InvalidConfig.code(),
            format!("retention.ms must be between 1 and {MAX_RETENTION_MS}, not {ms}"),
        ));
    }
    Ok(ms)
}

fn resolve_band_count(num_partitions: i32) -> Result<i32, ConfigError> {
    if num_partitions == -1 {
        return Ok(4);
    }
    if !(1..=1024).contains(&num_partitions) {
        return Err((
            ResponseError::InvalidPartitions.code(),
            format!("pg_topics needs 1 to 1024 partitions, not {num_partitions}"),
        ));
    }
    Ok(num_partitions)
}

fn resolve_replication(
    replication_factor: i16,
    copies: i32,
    fenced: bool,
    min_durability: Option<String>,
) -> Result<(i16, String), ConfigError> {
    if matches!(replication_factor, 1 | -1) {
        return Ok((
            1,
            min_durability.unwrap_or_else(|| DEFAULT_MIN_DURABILITY.to_string()),
        ));
    }
    if !(1..=copies).contains(&i32::from(replication_factor)) {
        return Err((
            ResponseError::InvalidReplicationFactor.code(),
            format!(
                "synchronous_standby_names keeps {copies} copies of every band (the primary and {} synchronous standbys), so replication_factor must be -1 or 1 to {copies}, not {replication_factor}",
                copies - 1
            ),
        ));
    }
    if !fenced {
        return Err((
            ResponseError::InvalidReplicationFactor.code(),
            format!(
                "replication_factor {replication_factor} needs pg_topics.failover_is_fenced = on"
            ),
        ));
    }
    match min_durability.as_deref() {
        None | Some("replicated") => Ok((replication_factor, "replicated".to_string())),
        Some(tier) => Err((
            ResponseError::InvalidConfig.code(),
            format!("replication_factor {replication_factor} needs pg_topics.min_durability replicated, not {tier}"),
        )),
    }
}

fn topic_configs(
    configs: &[(&str, i8, Option<&str>)],
) -> Result<(Option<i64>, Option<String>), ConfigError> {
    let mut retention_ms = None;
    let mut min_durability = None;
    for (key, op, value) in configs {
        if !matches!(*key, "retention.ms" | "pg_topics.min_durability") {
            return Err((
                ResponseError::InvalidConfig.code(),
                format!("pg_topics does not implement the config {key}"),
            ));
        }
        match (*op, key, value) {
            (OP_SET, &"retention.ms", Some(value)) => {
                retention_ms = Some(validate_retention_ms(value)?)
            }
            (OP_SET, _, Some(value)) => min_durability = Some((*value).to_string()),
            (OP_SET, _, None) => {
                return Err((
                    ResponseError::InvalidConfig.code(),
                    format!("pg_topics needs a value for {key}"),
                ))
            }
            (OP_DELETE, &"retention.ms", _) => retention_ms = Some(DEFAULT_RETENTION_MS),
            (OP_DELETE, _, _) => min_durability = Some(DEFAULT_MIN_DURABILITY.to_string()),
            _ => {
                return Err((
                    ResponseError::InvalidConfig.code(),
                    format!("pg_topics does not support that operation on {key}"),
                ))
            }
        }
    }
    Ok((retention_ms, min_durability))
}

fn create_topic_one(
    tx: &mut Transaction,
    t: &CreatableTopic,
    validate_only: bool,
) -> Result<CreatableTopicResult, Error> {
    let base = CreatableTopicResult::default().with_name(t.name.clone());
    let band_count = match resolve_band_count(t.num_partitions) {
        Ok(band_count) => band_count,
        Err((code, msg)) => {
            return Ok(base
                .with_error_code(code)
                .with_error_message(Some(StrBytes::from_string(msg))))
        }
    };
    let configs: Vec<(&str, i8, Option<&str>)> = t
        .configs
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                OP_SET,
                c.value.as_ref().map(|v| v.as_str()),
            )
        })
        .collect();
    let (retention_ms, min_durability) = match topic_configs(&configs) {
        Ok(parsed) => parsed,
        Err((code, msg)) => {
            return Ok(base
                .with_error_code(code)
                .with_error_message(Some(StrBytes::from_string(msg))))
        }
    };
    let (copies, fenced) = if matches!(t.replication_factor, 1 | -1) {
        (1, false)
    } else {
        let row = tx.query_one(
            "SELECT topic.sync_copies(current_setting('synchronous_standby_names')),
                    current_setting('pg_topics.failover_is_fenced')::bool",
            &[],
        )?;
        (row.try_get(0)?, row.try_get(1)?)
    };
    let (replication_factor, min_durability) =
        match resolve_replication(t.replication_factor, copies, fenced, min_durability) {
            Ok(resolved) => resolved,
            Err((code, msg)) => {
                return Ok(base
                    .with_error_code(code)
                    .with_error_message(Some(StrBytes::from_string(msg))))
            }
        };
    let retention_ms = retention_ms.unwrap_or(DEFAULT_RETENTION_MS);
    let mut sp = tx.savepoint("pg_topics_create_topic")?;
    let outcome = sp.execute(
        "SELECT topic.create_topic($1, $2, ($3 || ' milliseconds')::interval, $4)",
        &[
            &t.name.as_str(),
            &band_count,
            &retention_ms.to_string(),
            &min_durability,
        ],
    );
    match outcome {
        Ok(_) => {
            if validate_only {
                sp.rollback()?;
            } else {
                sp.commit()?;
            }
            Ok(base
                .with_num_partitions(band_count)
                .with_replication_factor(replication_factor))
        }
        Err(e) => {
            sp.rollback()?;
            let (code, msg) = topic_error(e, "CreateTopics")?;
            Ok(base
                .with_error_code(code)
                .with_error_message(msg.map(StrBytes::from_string)))
        }
    }
}

pub fn create_topics(
    db: &mut Client,
    req: &CreateTopicsRequest,
) -> Result<CreateTopicsResponse, Error> {
    let mut tx = db.transaction()?;
    let mut results = Vec::with_capacity(req.topics.len());
    for t in &req.topics {
        results.push(create_topic_one(&mut tx, t, req.validate_only)?);
    }
    tx.commit()?;
    Ok(CreateTopicsResponse::default().with_topics(results))
}

pub fn delete_topics(
    db: &mut Client,
    req: &DeleteTopicsRequest,
) -> Result<DeleteTopicsResponse, Error> {
    let mut responses = Vec::with_capacity(req.topic_names.len());
    for name in &req.topic_names {
        let (code, msg) = match db.execute("SELECT topic.drop_topic($1)", &[&name.as_str()]) {
            Ok(_) => (0, None),
            Err(e) => topic_error(e, "DeleteTopics")?,
        };
        responses.push(
            DeletableTopicResult::default()
                .with_name(Some(name.clone()))
                .with_error_code(code)
                .with_error_message(msg.map(StrBytes::from_string)),
        );
    }
    Ok(DeleteTopicsResponse::default().with_responses(responses))
}

pub fn describe_configs(
    db: &mut Client,
    req: &DescribeConfigsRequest,
) -> Result<DescribeConfigsResponse, Error> {
    let mut results = Vec::with_capacity(req.resources.len());
    for r in &req.resources {
        let base = DescribeConfigsResult::default()
            .with_resource_type(r.resource_type)
            .with_resource_name(r.resource_name.clone());
        let result = if r.resource_type != TOPIC_RESOURCE {
            base
        } else {
            match db.query(
                "SELECT name, value, editable FROM topic.describe_configs($1)",
                &[&r.resource_name.as_str()],
            ) {
                Ok(rows) => {
                    let wanted = req_keys(r);
                    let mut configs = Vec::with_capacity(rows.len());
                    for row in &rows {
                        let name: String = row.try_get(0)?;
                        if wanted.as_ref().is_some_and(|w| !w.contains(&name)) {
                            continue;
                        }
                        let value: String = row.try_get(1)?;
                        let editable: bool = row.try_get(2)?;
                        let is_default = match name.as_str() {
                            "retention.ms" => value == DEFAULT_RETENTION_MS.to_string(),
                            "pg_topics.min_durability" => value == DEFAULT_MIN_DURABILITY,
                            _ => true,
                        };
                        configs.push(
                            DescribeConfigsResourceResult::default()
                                .with_name(StrBytes::from_string(name))
                                .with_value(Some(StrBytes::from_string(value)))
                                .with_read_only(!editable)
                                .with_config_source(if is_default { 5 } else { 1 }),
                        );
                    }
                    base.with_configs(configs)
                }
                Err(e) => {
                    let (code, msg) = topic_error(e, "DescribeConfigs")?;
                    base.with_error_code(code)
                        .with_error_message(msg.map(StrBytes::from_string))
                }
            }
        };
        results.push(result);
    }
    Ok(DescribeConfigsResponse::default().with_results(results))
}

fn req_keys(r: &DescribeConfigsResource) -> Option<Vec<String>> {
    r.configuration_keys
        .as_ref()
        .map(|keys| keys.iter().map(|k| k.to_string()).collect())
}

fn legacy_configs(
    entries: &[(&str, Option<&str>)],
    max_message_bytes: &str,
    replication_factor: &str,
) -> Result<(i64, String), ConfigError> {
    let mut retention_ms = DEFAULT_RETENTION_MS;
    let mut min_durability = DEFAULT_MIN_DURABILITY.to_string();
    for (key, value) in entries {
        let Some(value) = value else {
            return Err((
                ResponseError::InvalidConfig.code(),
                format!("pg_topics needs a value for {key}"),
            ));
        };
        match (*key, *value) {
            ("retention.ms", ms) => retention_ms = validate_retention_ms(ms)?,
            ("pg_topics.min_durability", tier) => min_durability = tier.to_string(),
            ("cleanup.policy", "delete") => {}
            ("message.timestamp.type", "LogAppendTime") => {}
            ("max.message.bytes", given) if given == max_message_bytes => {}
            ("pg_topics.replication_factor", given) if given == replication_factor => {}
            (
                "cleanup.policy"
                | "message.timestamp.type"
                | "max.message.bytes"
                | "pg_topics.replication_factor",
                _,
            ) => {
                return Err((
                    ResponseError::InvalidConfig.code(),
                    format!("pg_topics does not allow changing {key}"),
                ))
            }
            _ => {
                return Err((
                    ResponseError::InvalidConfig.code(),
                    format!("pg_topics does not implement the config {key}"),
                ))
            }
        }
    }
    Ok((retention_ms, min_durability))
}

fn alter_one(
    db: &mut Client,
    r: &AlterConfigsResource,
    validate_only: bool,
) -> Result<(i16, Option<String>), Error> {
    if r.resource_type != TOPIC_RESOURCE {
        return Ok((
            ResponseError::InvalidRequest.code(),
            Some("pg_topics only alters topic configs".to_string()),
        ));
    }
    let current = match db.query_one(
        "SELECT current_setting('pg_topics.max_message_bytes'), value
         FROM topic.describe_configs($1) WHERE name = 'pg_topics.replication_factor'",
        &[&r.resource_name.as_str()],
    ) {
        Ok(row) => row,
        Err(e) => return topic_error(e, "AlterConfigs"),
    };
    let max_message_bytes: String = current.try_get(0)?;
    let replication_factor: String = current.try_get(1)?;
    let entries: Vec<(&str, Option<&str>)> = r
        .configs
        .iter()
        .map(|c| (c.name.as_str(), c.value.as_ref().map(|v| v.as_str())))
        .collect();
    let resolved = legacy_configs(&entries, &max_message_bytes, &replication_factor)
        .map(|(ms, tier)| (Some(ms), Some(tier)));
    apply_alterable(
        db,
        r.resource_name.as_str(),
        validate_only,
        "AlterConfigs",
        resolved,
    )
}

fn incremental_alter_one(
    db: &mut Client,
    r: &IncAlterConfigsResource,
    validate_only: bool,
) -> Result<(i16, Option<String>), Error> {
    if r.resource_type != TOPIC_RESOURCE {
        return Ok((
            ResponseError::InvalidRequest.code(),
            Some("pg_topics only alters topic configs".to_string()),
        ));
    }
    let configs: Vec<(&str, i8, Option<&str>)> = r
        .configs
        .iter()
        .map(|c| {
            (
                c.name.as_str(),
                c.config_operation,
                c.value.as_ref().map(|v| v.as_str()),
            )
        })
        .collect();
    apply_alterable(
        db,
        r.resource_name.as_str(),
        validate_only,
        "IncrementalAlterConfigs",
        topic_configs(&configs),
    )
}

fn apply_alterable(
    db: &mut Client,
    name: &str,
    validate_only: bool,
    action: &str,
    resolved: Result<(Option<i64>, Option<String>), ConfigError>,
) -> Result<(i16, Option<String>), Error> {
    let (retention_ms, min_durability) = match resolved {
        Ok(parsed) => parsed,
        Err((code, msg)) => return Ok((code, Some(msg))),
    };
    let mut tx = db.transaction()?;
    let outcome = apply_configs(&mut tx, name, retention_ms, min_durability);
    match outcome {
        Ok(()) => {
            if validate_only {
                tx.rollback()?;
            } else {
                tx.commit()?;
            }
            Ok((0, None))
        }
        Err(e) => {
            tx.rollback()?;
            topic_error(e, action)
        }
    }
}

fn apply_configs(
    tx: &mut Transaction,
    name: &str,
    retention_ms: Option<i64>,
    min_durability: Option<String>,
) -> Result<(), Error> {
    if let Some(ms) = retention_ms {
        tx.execute(
            "SELECT topic.set_retention($1, ($2 || ' milliseconds')::interval)",
            &[&name, &ms.to_string()],
        )?;
    }
    if let Some(tier) = &min_durability {
        tx.execute("SELECT topic.set_durability($1, $2)", &[&name, tier])?;
    }
    Ok(())
}

pub fn alter_configs(
    db: &mut Client,
    req: &AlterConfigsRequest,
) -> Result<AlterConfigsResponse, Error> {
    let mut responses = Vec::with_capacity(req.resources.len());
    for r in &req.resources {
        let (code, msg) = alter_one(db, r, req.validate_only)?;
        responses.push(
            AlterConfigsResourceResponse::default()
                .with_error_code(code)
                .with_error_message(msg.map(StrBytes::from_string))
                .with_resource_type(r.resource_type)
                .with_resource_name(r.resource_name.clone()),
        );
    }
    Ok(AlterConfigsResponse::default().with_responses(responses))
}

pub fn incremental_alter_configs(
    db: &mut Client,
    req: &IncrementalAlterConfigsRequest,
) -> Result<IncrementalAlterConfigsResponse, Error> {
    let mut responses = Vec::with_capacity(req.resources.len());
    for r in &req.resources {
        let (code, msg) = incremental_alter_one(db, r, req.validate_only)?;
        responses.push(
            IncAlterConfigsResourceResponse::default()
                .with_error_code(code)
                .with_error_message(msg.map(StrBytes::from_string))
                .with_resource_type(r.resource_type)
                .with_resource_name(r.resource_name.clone()),
        );
    }
    Ok(IncrementalAlterConfigsResponse::default().with_responses(responses))
}

pub fn delete_groups(
    db: &mut Client,
    req: &DeleteGroupsRequest,
) -> Result<DeleteGroupsResponse, Error> {
    let mut results = Vec::with_capacity(req.groups_names.len());
    for g in &req.groups_names {
        let code = match db.execute("SELECT topic.delete_group($1)", &[&g.as_str()]) {
            Ok(_) => 0,
            Err(e) => match e.as_db_error() {
                None => return Err(e),
                Some(db_err) => match db_err.code().code() {
                    "42501" | "42704" => ResponseError::GroupAuthorizationFailed.code(),
                    "55006" => ResponseError::NonEmptyGroup.code(),
                    _ => {
                        eprintln!("pg_topics listener: DeleteGroups: {}", db_err.message());
                        ResponseError::UnknownServerError.code()
                    }
                },
            },
        };
        results.push(
            DeletableGroupResult::default()
                .with_group_id(g.clone())
                .with_error_code(code),
        );
    }
    Ok(DeleteGroupsResponse::default().with_results(results))
}

pub fn describe_cluster(
    db: &mut Client,
    host: &str,
    port: u16,
) -> Result<DescribeClusterResponse, Error> {
    Ok(DescribeClusterResponse::default()
        .with_cluster_id(StrBytes::from_string(crate::handlers::cluster_id(db)?))
        .with_controller_id(BrokerId(0))
        .with_brokers(vec![DescribeClusterBroker::default()
            .with_broker_id(BrokerId(0))
            .with_host(StrBytes::from_string(host.to_string()))
            .with_port(i32::from(port))]))
}

pub fn list_groups(db: &mut Client, req: &ListGroupsRequest) -> Result<ListGroupsResponse, Error> {
    let states: Vec<String> = req.states_filter.iter().map(|s| s.to_string()).collect();
    let rows = db.query(
        "SELECT group_name, coalesce(protocol_type, ''), state FROM topic.topic_groups
         WHERE cardinality($1::text[]) = 0 OR state = ANY ($1)",
        &[&states],
    )?;
    let mut groups = Vec::with_capacity(rows.len());
    for row in &rows {
        groups.push(
            ListedGroup::default()
                .with_group_id(GroupId(StrBytes::from_string(row.try_get::<_, String>(0)?)))
                .with_protocol_type(StrBytes::from_string(row.try_get::<_, String>(1)?))
                .with_group_state(StrBytes::from_string(row.try_get::<_, String>(2)?))
                .with_group_type(StrBytes::from_static_str("classic")),
        );
    }
    Ok(ListGroupsResponse::default().with_groups(groups))
}

pub fn describe_groups(
    db: &mut Client,
    req: &DescribeGroupsRequest,
) -> Result<DescribeGroupsResponse, Error> {
    let mut groups = Vec::with_capacity(req.groups.len());
    for g in &req.groups {
        let rows = db.query(
            "SELECT found, state, protocol_type, protocol_name, member_id, client_id, metadata, assignment
             FROM topic.describe_group($1)",
            &[&g.as_str()],
        )?;
        let Some(first) = rows.first() else {
            eprintln!(
                "pg_topics listener: DescribeGroups: describe_group returned no rows for {}",
                g.as_str()
            );
            groups.push(
                DescribedGroup::default()
                    .with_error_code(ResponseError::UnknownServerError.code())
                    .with_group_id(g.clone()),
            );
            continue;
        };
        let found: bool = first.try_get(0)?;
        if !found {
            groups.push(
                DescribedGroup::default()
                    .with_group_id(g.clone())
                    .with_group_state(StrBytes::from_static_str("Dead")),
            );
            continue;
        }
        let state: String = first.try_get(1)?;
        let protocol_type: Option<String> = first.try_get(2)?;
        let protocol_name: Option<String> = first.try_get(3)?;
        let mut members = Vec::with_capacity(rows.len());
        for row in &rows {
            let member_id: Option<String> = row.try_get(4)?;
            let Some(member_id) = member_id else { continue };
            let client_id: Option<String> = row.try_get(5)?;
            let metadata: Option<Vec<u8>> = row.try_get(6)?;
            let assignment: Option<Vec<u8>> = row.try_get(7)?;
            members.push(
                DescribedGroupMember::default()
                    .with_member_id(StrBytes::from_string(member_id))
                    .with_client_id(StrBytes::from_string(client_id.unwrap_or_default()))
                    .with_member_metadata(Bytes::from(metadata.unwrap_or_default()))
                    .with_member_assignment(Bytes::from(assignment.unwrap_or_default())),
            );
        }
        groups.push(
            DescribedGroup::default()
                .with_group_id(g.clone())
                .with_group_state(StrBytes::from_string(state))
                .with_protocol_type(StrBytes::from_string(protocol_type.unwrap_or_default()))
                .with_protocol_data(StrBytes::from_string(protocol_name.unwrap_or_default()))
                .with_members(members),
        );
    }
    Ok(DescribeGroupsResponse::default().with_groups(groups))
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::messages::{ApiKey, TopicName};
    use kafka_protocol::protocol::{Decodable, Encodable};

    use super::*;
    use crate::versions::VERSIONS;

    #[test]
    fn topic_configs_validates_keys_and_parses_or_refuses_values() {
        assert_eq!(topic_configs(&[]), Ok((None, None)));
        assert_eq!(
            topic_configs(&[("retention.ms", OP_SET, Some("3600000"))]),
            Ok((Some(3_600_000), None))
        );
        assert_eq!(
            topic_configs(&[("pg_topics.min_durability", OP_SET, Some("relaxed"))]),
            Ok((None, Some("relaxed".to_string())))
        );
        assert_eq!(
            topic_configs(&[
                ("retention.ms", OP_SET, Some("60000")),
                ("pg_topics.min_durability", OP_SET, Some("durable"))
            ]),
            Ok((Some(60_000), Some("durable".to_string())))
        );
        assert_eq!(
            topic_configs(&[("cleanup.policy", OP_SET, Some("compact"))])
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            topic_configs(&[("retention.ms", OP_SET, None)])
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            topic_configs(&[("retention.ms", OP_SET, Some("not-a-number"))])
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            topic_configs(&[("retention.ms", OP_DELETE, None)]),
            Ok((Some(DEFAULT_RETENTION_MS), None))
        );
        assert_eq!(
            topic_configs(&[("pg_topics.min_durability", OP_DELETE, None)]),
            Ok((None, Some(DEFAULT_MIN_DURABILITY.to_string())))
        );
        assert_eq!(
            topic_configs(&[("retention.ms", 2, Some("60000"))])
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
    }

    #[test]
    fn validate_retention_ms_refuses_forever_and_out_of_range_values() {
        assert_eq!(validate_retention_ms("60000"), Ok(60_000));
        let (code, msg) = validate_retention_ms("-1").unwrap_err();
        assert_eq!(code, ResponseError::InvalidConfig.code());
        assert!(msg.contains("finite"), "{msg}");
        assert_eq!(
            validate_retention_ms("0").unwrap_err().0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            validate_retention_ms("-2").unwrap_err().0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            validate_retention_ms(&(MAX_RETENTION_MS + 1).to_string())
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            topic_configs(&[("retention.ms", OP_SET, Some("-1"))])
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
    }

    #[test]
    fn resolve_replication_accepts_only_the_copies_the_standbys_back() {
        let durable = || DEFAULT_MIN_DURABILITY.to_string();
        let replicated = || "replicated".to_string();
        assert_eq!(resolve_replication(1, 1, false, None), Ok((1, durable())));
        assert_eq!(
            resolve_replication(-1, 3, true, Some("relaxed".to_string())),
            Ok((1, "relaxed".to_string()))
        );
        assert_eq!(resolve_replication(3, 3, true, None), Ok((3, replicated())));
        assert_eq!(resolve_replication(2, 3, true, None), Ok((2, replicated())));
        assert_eq!(
            resolve_replication(2, 2, true, Some(replicated())),
            Ok((2, replicated()))
        );
        for (rf, copies, fenced, needle) in [
            (3, 2, true, "keeps 2 copies"),
            (2, 1, true, "keeps 1 copies"),
            (3, 3, false, "failover_is_fenced"),
            (3, 2, false, "keeps 2 copies"),
            (0, 3, true, "not 0"),
            (-2, 3, true, "not -2"),
        ] {
            let (code, msg) = resolve_replication(rf, copies, fenced, None).unwrap_err();
            assert_eq!(code, ResponseError::InvalidReplicationFactor.code(), "{rf}");
            assert!(msg.contains(needle), "{msg}");
        }
        let (code, msg) = resolve_replication(3, 3, true, Some("durable".to_string())).unwrap_err();
        assert_eq!(code, ResponseError::InvalidConfig.code());
        assert!(msg.contains("replicated"), "{msg}");
    }

    #[test]
    fn resolve_band_count_accepts_the_default_and_the_1_to_1024_range() {
        assert_eq!(resolve_band_count(-1), Ok(4));
        assert_eq!(resolve_band_count(1), Ok(1));
        assert_eq!(resolve_band_count(1024), Ok(1024));
        assert_eq!(
            resolve_band_count(0).unwrap_err().0,
            ResponseError::InvalidPartitions.code()
        );
        assert_eq!(
            resolve_band_count(1025).unwrap_err().0,
            ResponseError::InvalidPartitions.code()
        );
        assert_eq!(
            resolve_band_count(2000).unwrap_err().0,
            ResponseError::InvalidPartitions.code()
        );
    }

    #[test]
    fn legacy_configs_resets_omitted_keys_and_checks_read_only_keys_against_the_given_value() {
        assert_eq!(
            legacy_configs(&[("retention.ms", Some("3600000"))], "1048576", "1"),
            Ok((3_600_000, DEFAULT_MIN_DURABILITY.to_string()))
        );
        assert_eq!(
            legacy_configs(&[], "1048576", "1"),
            Ok((DEFAULT_RETENTION_MS, DEFAULT_MIN_DURABILITY.to_string()))
        );
        assert_eq!(
            legacy_configs(&[("cleanup.policy", Some("delete"))], "1048576", "1"),
            Ok((DEFAULT_RETENTION_MS, DEFAULT_MIN_DURABILITY.to_string()))
        );
        assert_eq!(
            legacy_configs(&[("max.message.bytes", Some("1048576"))], "1048576", "1"),
            Ok((DEFAULT_RETENTION_MS, DEFAULT_MIN_DURABILITY.to_string()))
        );
        assert_eq!(
            legacy_configs(&[("cleanup.policy", Some("compact"))], "1048576", "1")
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            legacy_configs(&[("max.message.bytes", Some("2000000"))], "1048576", "1")
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            legacy_configs(
                &[("pg_topics.replication_factor", Some("3"))],
                "1048576",
                "3"
            ),
            Ok((DEFAULT_RETENTION_MS, DEFAULT_MIN_DURABILITY.to_string()))
        );
        assert_eq!(
            legacy_configs(
                &[("pg_topics.replication_factor", Some("3"))],
                "1048576",
                "1"
            )
            .unwrap_err()
            .0,
            ResponseError::InvalidConfig.code()
        );
        assert_eq!(
            legacy_configs(&[("retention.ms", Some("-1"))], "1048576", "1")
                .unwrap_err()
                .0,
            ResponseError::InvalidConfig.code()
        );
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
    fn every_admin_response_round_trips_at_every_advertised_version() {
        let name = |s: &'static str| StrBytes::from_static_str(s);
        let topic = |s: &'static str| TopicName(name(s));
        for (code, msg) in [
            (0, None),
            (
                ResponseError::InvalidReplicationFactor.code(),
                Some("bad rf"),
            ),
        ] {
            round_trip(ApiKey::CreateTopics, |_| {
                CreateTopicsResponse::default().with_topics(vec![CreatableTopicResult::default()
                    .with_name(topic("s.t_q"))
                    .with_error_code(code)
                    .with_error_message(msg.map(|m| StrBytes::from_string(m.to_string())))
                    .with_num_partitions(4)
                    .with_replication_factor(1)])
            });
        }
        for (code, msg) in [
            (0, None),
            (ResponseError::TopicAuthorizationFailed.code(), None),
        ] {
            round_trip(ApiKey::DeleteTopics, |_| {
                DeleteTopicsResponse::default().with_responses(vec![DeletableTopicResult::default(
                )
                .with_name(Some(topic("s.t_q")))
                .with_error_code(code)
                .with_error_message(msg.map(|m: &str| StrBytes::from_string(m.to_string())))])
            });
        }
        round_trip(ApiKey::DescribeConfigs, |_| {
            DescribeConfigsResponse::default().with_results(vec![DescribeConfigsResult::default()
                .with_resource_type(TOPIC_RESOURCE)
                .with_resource_name(name("s.t_q"))
                .with_configs(vec![DescribeConfigsResourceResult::default()
                    .with_name(name("retention.ms"))
                    .with_value(Some(name("604800000")))
                    .with_read_only(false)
                    .with_config_source(1)])])
        });
        round_trip(ApiKey::AlterConfigs, |_| {
            AlterConfigsResponse::default().with_responses(vec![
                AlterConfigsResourceResponse::default()
                    .with_error_code(ResponseError::InvalidConfig.code())
                    .with_error_message(Some(name("pg_topics does not implement the config x")))
                    .with_resource_type(TOPIC_RESOURCE)
                    .with_resource_name(name("s.t_q")),
            ])
        });
        round_trip(ApiKey::IncrementalAlterConfigs, |_| {
            IncrementalAlterConfigsResponse::default().with_responses(vec![
                IncAlterConfigsResourceResponse::default()
                    .with_error_code(ResponseError::InvalidConfig.code())
                    .with_error_message(Some(name(
                        "pg_topics does not support that operation on x",
                    )))
                    .with_resource_type(TOPIC_RESOURCE)
                    .with_resource_name(name("s.t_q")),
            ])
        });
        round_trip(ApiKey::DeleteGroups, |_| {
            DeleteGroupsResponse::default().with_results(vec![DeletableGroupResult::default()
                .with_group_id(GroupId(name("g")))
                .with_error_code(ResponseError::GroupAuthorizationFailed.code())])
        });
        round_trip(ApiKey::DescribeCluster, |_| {
            DescribeClusterResponse::default()
                .with_cluster_id(name("123456"))
                .with_controller_id(BrokerId(0))
                .with_brokers(vec![DescribeClusterBroker::default()
                    .with_broker_id(BrokerId(0))
                    .with_host(name("localhost"))
                    .with_port(9092)])
        });
        round_trip(ApiKey::ListGroups, |_| {
            ListGroupsResponse::default().with_groups(vec![ListedGroup::default()
                .with_group_id(GroupId(name("g")))
                .with_protocol_type(name("consumer"))
                .with_group_state(name("Stable"))
                .with_group_type(name("classic"))])
        });
        round_trip(ApiKey::DescribeGroups, |_| {
            DescribeGroupsResponse::default().with_groups(vec![DescribedGroup::default()
                .with_group_id(GroupId(name("g")))
                .with_group_state(name("Stable"))
                .with_protocol_type(name("consumer"))
                .with_protocol_data(name("range"))
                .with_members(vec![DescribedGroupMember::default()
                    .with_member_id(name("m-1"))
                    .with_client_id(name("rdkafka"))
                    .with_member_metadata(Bytes::from_static(b"\x00\x01"))
                    .with_member_assignment(Bytes::from_static(b"\x00\x02"))])])
        });
        round_trip(ApiKey::DescribeGroups, |_| {
            DescribeGroupsResponse::default().with_groups(vec![DescribedGroup::default()
                .with_group_id(GroupId(name("ghost")))
                .with_group_state(name("Dead"))])
        });
    }
}
