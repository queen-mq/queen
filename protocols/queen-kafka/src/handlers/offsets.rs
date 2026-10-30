//! OffsetCommit and OffsetFetch → the Queen consumer group's positions.
//!
//! A Kafka committed offset is a Queen position (the next offset the group
//! reads), so a Kafka group and a Queen group of the same name are one group:
//! the console shows its lag, a native consumer resumes where it stopped. A
//! commit is checked against the group's generation on the coordinator (the
//! raft leader) and written as one transaction.

use std::collections::BTreeMap;
use std::sync::Arc;

use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{
    OffsetCommitRequest, OffsetCommitResponse, OffsetFetchRequest, OffsetFetchResponse, TopicName,
};

use super::s;
use crate::conn::Conn;
use crate::core::{PositionOp, MAX_POSITION_READS, MAX_POSITION_WRITES};
use crate::errors::*;
use crate::server::Ctx;

pub async fn commit(ctx: &Arc<Ctx>, conn: &Conn, req: OffsetCommitRequest) -> OffsetCommitResponse {
    let group = req.group_id.0.to_string();
    // Counted before anything is built: one commit is one Queen transaction,
    // and the broker takes MAX_POSITION_WRITES positions in one.
    let count: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    let mut error = if !ctx.broker.cluster().leads() {
        NOT_COORDINATOR
    } else if !super::group::valid_group(&group) {
        INVALID_GROUP_ID
    } else if count > MAX_POSITION_WRITES {
        tracing::debug!(target: "kafka", %group, count, "offset commit over its ceiling");
        INVALID_COMMIT_OFFSET_SIZE
    } else {
        ctx.groups.may_commit(
            &group,
            req.member_id.as_str(),
            req.generation_id_or_member_epoch,
        )
    };
    if error == NONE {
        let ops: Vec<PositionOp> = req
            .topics
            .iter()
            .flat_map(|t| {
                let queue = t.name.0.to_string();
                let group = group.clone();
                t.partitions.iter().map(move |p| PositionOp {
                    queue: queue.clone(),
                    partition: p.partition_index.to_string(),
                    group: group.clone(),
                    offset: (p.committed_offset >= 0).then_some(p.committed_offset),
                    metadata: p
                        .committed_metadata
                        .as_ref()
                        .map(|m| m.to_string())
                        .unwrap_or_default(),
                })
            })
            .collect();
        if !ops.is_empty() {
            let landed = ctx.queen.commit(conn.bearer.as_deref(), &ops).await;
            if landed.is_ok() {
                ctx.groups
                    .note_commit(&group, conn.bearer.as_deref(), std::time::Instant::now());
            }
            if let Err(e) = landed {
                tracing::warn!(target: "kafka", %group, error = %e, "offset commit failed");
                error = match &e {
                    crate::core::CallError::Status { code: 404, .. } => UNKNOWN_TOPIC_OR_PARTITION,
                    _ => of_call_for_group(&e),
                };
            }
        }
    }
    let topics = req
        .topics
        .into_iter()
        .map(|t| {
            OffsetCommitResponseTopic::default()
                .with_name(t.name)
                .with_partitions(
                    t.partitions
                        .into_iter()
                        .map(|p| {
                            OffsetCommitResponsePartition::default()
                                .with_partition_index(p.partition_index)
                                .with_error_code(error)
                        })
                        .collect(),
                )
        })
        .collect();
    OffsetCommitResponse::default().with_topics(topics)
}

pub async fn fetch(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: OffsetFetchRequest,
    version: i16,
) -> OffsetFetchResponse {
    let group = req.group_id.0.to_string();
    let asked: Option<Vec<(TopicName, Vec<i32>)>> = req.topics.map(|ts| {
        ts.into_iter()
            .map(|t| (t.name, t.partition_indexes))
            .collect()
    });
    // Counted before anything is built: the broker reads MAX_POSITION_READS
    // positions in one call.
    let count: usize = asked
        .as_ref()
        .map_or(0, |ts| ts.iter().map(|(_, ps)| ps.len()).sum());
    let error = if !ctx.broker.cluster().leads() {
        NOT_COORDINATOR
    } else if !super::group::valid_group(&group) {
        INVALID_GROUP_ID
    } else if count > MAX_POSITION_READS {
        tracing::debug!(target: "kafka", %group, count, "offset fetch over its ceiling");
        INVALID_REQUEST
    } else {
        NONE
    };
    let read = if error != NONE {
        Err(error)
    } else {
        let entries: Option<Vec<(String, String)>> = asked.as_ref().map(|ts| {
            ts.iter()
                .flat_map(|(t, ps)| ps.iter().map(move |p| (t.0.to_string(), p.to_string())))
                .collect()
        });
        if entries.as_ref().is_some_and(Vec::is_empty) {
            Ok(Vec::new())
        } else {
            ctx.queen
                .positions(conn.bearer.as_deref(), &group, entries.as_deref())
                .await
                .map_err(|e| {
                    tracing::warn!(target: "kafka", %group, error = %e, "offset fetch failed");
                    of_call_for_group(&e)
                })
        }
    };
    let partition = |index: i32, offset: i64, metadata: String, error: i16| {
        OffsetFetchResponsePartition::default()
            .with_partition_index(index)
            .with_committed_offset(offset)
            .with_metadata(Some(s(metadata)))
            .with_error_code(error)
    };
    let topics: Vec<OffsetFetchResponseTopic> = match (&read, asked) {
        (Err(code), Some(asked)) if version < 2 => asked
            .into_iter()
            .map(|(name, ps)| {
                OffsetFetchResponseTopic::default()
                    .with_name(name)
                    .with_partitions(
                        ps.into_iter()
                            .map(|p| partition(p, -1, String::new(), *code))
                            .collect(),
                    )
            })
            .collect(),
        (Err(_), _) => Vec::new(),
        (Ok(positions), Some(asked)) => {
            let found: BTreeMap<(String, String), (i64, String)> = positions
                .iter()
                .map(|p| {
                    (
                        (p.queue.clone(), p.partition.clone()),
                        (p.offset.unwrap_or(-1), p.metadata.clone()),
                    )
                })
                .collect();
            asked
                .into_iter()
                .map(|(name, ps)| {
                    let q = name.0.to_string();
                    OffsetFetchResponseTopic::default()
                        .with_name(name)
                        .with_partitions(
                            ps.into_iter()
                                .map(|p| {
                                    let (offset, md) = found
                                        .get(&(q.clone(), p.to_string()))
                                        .cloned()
                                        .unwrap_or((-1, String::new()));
                                    partition(p, offset, md, NONE)
                                })
                                .collect(),
                        )
                })
                .collect()
        }
        (Ok(positions), None) => {
            let mut by_topic: BTreeMap<String, Vec<OffsetFetchResponsePartition>> = BTreeMap::new();
            for p in positions {
                let (Ok(index), Some(offset)) = (p.partition.parse::<i32>(), p.offset) else {
                    continue;
                };
                by_topic.entry(p.queue.clone()).or_default().push(partition(
                    index,
                    offset,
                    p.metadata.clone(),
                    NONE,
                ));
            }
            by_topic
                .into_iter()
                .map(|(t, ps)| {
                    OffsetFetchResponseTopic::default()
                        .with_name(TopicName(s(t)))
                        .with_partitions(ps)
                })
                .collect()
        }
    };
    let resp = OffsetFetchResponse::default().with_topics(topics);
    match (&read, version >= 2) {
        (Err(code), true) => resp.with_error_code(*code),
        _ => resp,
    }
}
