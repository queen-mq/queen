//! CreateTopics, DeleteTopics, CreatePartitions, DescribeConfigs.
//!
//! A topic is a queue. Its width is declared in KV before the queue exists
//! (crate::topics). Of a new topic's configs, `retention.ms` and
//! `cleanup.policy` become its queue's retention ([`retention_of`]), as the
//! first facade mapped them; the others are accepted and not applied yet
//! (phase 2), and a describe answers none rather than values nothing enforces.

use std::sync::Arc;

use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::delete_topics_response::DeletableTopicResult;
use kafka_protocol::messages::describe_configs_response::DescribeConfigsResult;
use kafka_protocol::messages::{
    CreatePartitionsRequest, CreatePartitionsResponse, CreateTopicsRequest, CreateTopicsResponse,
    DeleteTopicsRequest, DeleteTopicsResponse, DescribeConfigsRequest, DescribeConfigsResponse,
};

use super::s;
use crate::conn::Conn;
use crate::errors::*;
use crate::server::Ctx;
use crate::topics::valid_topic_name;

/// The width a topic may be created with or grown to: every partition is a
/// line of every Metadata answer that names the topic.
const MAX_PARTITIONS: i32 = 10_000;
const TOPIC: i8 = 2;
const BROKER: i8 = 4;

pub async fn create_topics(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: CreateTopicsRequest,
    version: i16,
) -> CreateTopicsResponse {
    let bearer = conn.bearer.as_deref();
    let voters = ctx.broker.cluster().nodes.len().max(1) as i16;
    let snap = ctx.topics.refresh(bearer).await;
    let mut seen: std::collections::HashMap<String, usize> = std::collections::HashMap::new();
    for t in &req.topics {
        *seen.entry(t.name.0.to_string()).or_default() += 1;
    }
    let mut out = Vec::with_capacity(req.topics.len());
    for t in req.topics {
        let name = t.name.0.to_string();
        let partitions = if t.num_partitions < 0 {
            ctx.topics.default_partitions()
        } else {
            t.num_partitions
        };
        let retention = retention_of(
            t.configs
                .iter()
                .map(|c| (c.name.as_str(), c.value.as_ref().map(|v| v.as_str()))),
        );
        let (error, message): (i16, Option<String>) = if seen.get(&name).copied().unwrap_or(0) > 1 {
            (
                INVALID_REQUEST,
                Some(format!(
                    "Create topics request from client contains a duplicate topic: {name}"
                )),
            )
        } else if !valid_topic_name(&name) || name.starts_with("__") {
            (
                INVALID_TOPIC_EXCEPTION,
                Some("topic names are 1-249 of [a-zA-Z0-9._-] and do not start with __".into()),
            )
        } else if let Err(why) = &retention {
            (INVALID_CONFIG, Some(why.clone()))
        } else if !t.assignments.is_empty() {
            (INVALID_REPLICA_ASSIGNMENT, Some("every partition is led by the raft leader; manual replica assignment is not supported".into()))
        } else if !(1..=MAX_PARTITIONS).contains(&partitions) {
            (
                INVALID_PARTITIONS,
                Some(format!("partitions must be 1..={MAX_PARTITIONS}")),
            )
        } else {
            match &snap {
                Err(e) => (of_call(e), Some(e.to_string())),
                Ok(s) if s.widths.contains_key(&name) => (
                    TOPIC_ALREADY_EXISTS,
                    Some(format!("Topic '{name}' already exists.")),
                ),
                Ok(_) if req.validate_only => (NONE, None),
                Ok(_) => match ctx.topics.declare(bearer, &name, partitions).await {
                    Err(e) => (of_call(&e), Some(e.to_string())),
                    Ok(()) => match ctx
                        .queen
                        .create_queue(
                            bearer,
                            &name,
                            ctx.cfg.dedup_window_seconds,
                            retention.clone().ok().flatten(),
                        )
                        .await
                    {
                        Ok(()) => (NONE, None),
                        Err(e) => (of_call(&e), Some(e.to_string())),
                    },
                },
            }
        };
        let mut r = CreatableTopicResult::default()
            .with_name(t.name)
            .with_error_code(error)
            .with_error_message(message.map(s));
        if version >= 5 {
            r = r
                .with_num_partitions(if error == NONE { partitions } else { -1 })
                .with_replication_factor(if error == NONE { voters } else { -1 })
                .with_configs(Some(Vec::new()));
        }
        out.push(r);
    }
    ctx.topics.invalidate();
    CreateTopicsResponse::default().with_topics(out)
}

pub async fn delete_topics(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: DeleteTopicsRequest,
    version: i16,
) -> DeleteTopicsResponse {
    let bearer = conn.bearer.as_deref();
    let names: Vec<_> = if version >= 6 {
        req.topics.into_iter().filter_map(|t| t.name).collect()
    } else {
        req.topic_names
    };
    let mut out = Vec::with_capacity(names.len());
    for n in names {
        let name = n.0.to_string();
        let error = match ctx.queen.delete_queue(bearer, &name).await {
            Ok(true) => {
                if let Err(e) = ctx.topics.forget(bearer, &name).await {
                    tracing::warn!(target: "kafka", topic = %name, error = %e, "cannot drop the width row");
                }
                NONE
            }
            Ok(false) => UNKNOWN_TOPIC_OR_PARTITION,
            Err(e) => of_call(&e),
        };
        let mut r = DeletableTopicResult::default()
            .with_name(Some(n))
            .with_error_code(error);
        if version >= 5 && error == UNKNOWN_TOPIC_OR_PARTITION {
            r = r.with_error_message(Some(s("This server does not host this topic-partition.")));
        }
        out.push(r);
    }
    ctx.topics.invalidate();
    DeleteTopicsResponse::default().with_responses(out)
}

pub async fn create_partitions(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: CreatePartitionsRequest,
) -> CreatePartitionsResponse {
    let bearer = conn.bearer.as_deref();
    let snap = ctx.topics.refresh(bearer).await;
    let mut out = Vec::with_capacity(req.topics.len());
    for t in req.topics {
        let name = t.name.0.to_string();
        let (error, message): (i16, Option<String>) = match &snap {
            Err(e) => (of_call(e), Some(e.to_string())),
            Ok(_) if t.assignments.as_ref().is_some_and(|a| !a.is_empty()) => (
                INVALID_REPLICA_ASSIGNMENT,
                Some("every partition is led by the raft leader; manual replica assignment is not supported".into()),
            ),
            Ok(s) => match s.widths.get(&name) {
                None => (UNKNOWN_TOPIC_OR_PARTITION, None),
                Some(&now) if t.count == now => {
                    (INVALID_PARTITIONS, Some(format!("Topic already has {now} partition(s).")))
                }
                Some(&now) if t.count < now => (
                    INVALID_PARTITIONS,
                    Some(format!(
                        "The topic {name} currently has {now} partition(s); {} would not be an increase.",
                        t.count
                    )),
                ),
                Some(_) if t.count > MAX_PARTITIONS => {
                    (INVALID_PARTITIONS, Some(format!("partitions must be 1..={MAX_PARTITIONS}")))
                }
                Some(_) if req.validate_only => (NONE, None),
                Some(_) => match ctx.topics.declare(bearer, &name, t.count).await {
                    Ok(()) => (NONE, None),
                    Err(e) => (of_call(&e), Some(e.to_string())),
                },
            },
        };
        out.push(
            CreatePartitionsTopicResult::default()
                .with_name(t.name)
                .with_error_code(error)
                .with_error_message(message.map(s)),
        );
    }
    ctx.topics.invalidate();
    CreatePartitionsResponse::default().with_results(out)
}

/// The queue retention a new topic's configs ask for, in whole seconds:
/// `Ok(None)` keeps every record. The first facade's mapping:
///
/// - `retention.ms`: `-1` (the default) keeps every record; 1000 and more is
///   the retention, rounded down to whole seconds; anything else is refused,
///   because less than a second rounds to a retention of nothing.
/// - `cleanup.policy`: `delete` (the default) lets that retention delete;
///   `compact` keeps every record whatever `retention.ms` says, since Queen does
///   not compact and the last value of every key must stay; `compact,delete`
///   deletes by age and compacts nothing.
///
/// Other configs are accepted and not applied. A value that is not one of
/// these is `Err`, which the request answers with `INVALID_CONFIG`.
pub(crate) fn retention_of<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Option<i64>, String> {
    let mut ms: i64 = -1;
    let mut deletes = true;
    for (name, value) in configs {
        let value = value.unwrap_or("").trim();
        match name {
            "retention.ms" => {
                ms = match value.parse::<i64>() {
                    Ok(-1) => -1,
                    Ok(v) if v >= 1000 => v,
                    _ => {
                        return Err(format!(
                            "retention.ms={value}: expected -1 (keep every record) or at least 1000"
                        ))
                    }
                }
            }
            "cleanup.policy" => {
                let mut parts: Vec<String> = value
                    .split(',')
                    .map(|p| p.trim().to_ascii_lowercase())
                    .collect();
                parts.sort();
                parts.dedup();
                deletes = match parts.iter().map(String::as_str).collect::<Vec<_>>()[..] {
                    ["delete"] => true,
                    ["compact"] => false,
                    ["compact", "delete"] => true,
                    _ => {
                        return Err(format!(
                            "cleanup.policy={value}: expected delete, compact or compact,delete"
                        ))
                    }
                }
            }
            _ => {}
        }
    }
    Ok((deletes && ms > 0).then_some(ms / 1000))
}

pub async fn describe_configs(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: DescribeConfigsRequest,
) -> DescribeConfigsResponse {
    let snap = ctx.topics.snapshot(conn.bearer.as_deref()).await;
    let results = req
        .resources
        .into_iter()
        .map(|r| {
            let error = match r.resource_type {
                TOPIC => match &snap {
                    Ok(s) if s.widths.contains_key(r.resource_name.as_str()) => NONE,
                    Ok(_) => UNKNOWN_TOPIC_OR_PARTITION,
                    Err(e) => of_call(e),
                },
                BROKER => NONE,
                _ => INVALID_REQUEST,
            };
            DescribeConfigsResult::default()
                .with_error_code(error)
                .with_resource_type(r.resource_type)
                .with_resource_name(r.resource_name)
        })
        .collect();
    DescribeConfigsResponse::default().with_results(results)
}

#[cfg(test)]
mod tests {
    use super::retention_of;

    fn of(pairs: &[(&'static str, &'static str)]) -> Result<Option<i64>, String> {
        retention_of(pairs.iter().map(|(n, v)| (*n, Some(*v))))
    }

    #[test]
    fn retention_ms_becomes_the_queues_retention_in_whole_seconds() {
        assert_eq!(of(&[]), Ok(None));
        assert_eq!(of(&[("retention.ms", "-1")]), Ok(None));
        assert_eq!(of(&[("retention.ms", "900000")]), Ok(Some(900)));
        assert_eq!(of(&[("retention.ms", "1999")]), Ok(Some(1)));
        assert_eq!(of(&[("retention.ms", "604800000")]), Ok(Some(604_800)));
        // configs the facade does not apply are accepted
        assert_eq!(
            of(&[("segment.bytes", "1073741824"), ("retention.ms", "60000")]),
            Ok(Some(60))
        );
    }

    #[test]
    fn a_compacted_topic_keeps_every_record_and_compact_delete_deletes_by_age() {
        assert_eq!(
            of(&[("cleanup.policy", "compact"), ("retention.ms", "60000")]),
            Ok(None)
        );
        assert_eq!(
            of(&[("retention.ms", "60000"), ("cleanup.policy", "compact")]),
            Ok(None)
        );
        assert_eq!(
            of(&[
                ("cleanup.policy", "compact,delete"),
                ("retention.ms", "60000")
            ]),
            Ok(Some(60))
        );
        assert_eq!(
            of(&[
                ("cleanup.policy", "Delete, Compact"),
                ("retention.ms", "60000")
            ]),
            Ok(Some(60))
        );
        assert_eq!(of(&[("cleanup.policy", "delete")]), Ok(None));
    }

    #[test]
    fn a_value_that_would_round_to_nothing_or_does_not_parse_is_refused() {
        for v in ["0", "999", "-2", "abc", ""] {
            let e = of(&[("retention.ms", v)]).unwrap_err();
            assert!(e.starts_with("retention.ms="), "{e}");
        }
        let e = of(&[("cleanup.policy", "archive")]).unwrap_err();
        assert!(e.contains("cleanup.policy=archive"), "{e}");
        assert!(retention_of([("retention.ms", None)]).is_err());
    }
}
