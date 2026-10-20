//! AddPartitionsToTxn, AddOffsetsToTxn, TxnOffsetCommit, EndTxn: the
//! transaction coordinator ([`crate::txn`]) on the raft leader. Any other node
//! answers NOT_COORDINATOR and the client asks FindCoordinator again.

use std::sync::Arc;

use kafka_protocol::messages::add_partitions_to_txn_response::{
    AddPartitionsToTxnPartitionResult, AddPartitionsToTxnTopicResult,
};
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::{
    AddOffsetsToTxnRequest, AddOffsetsToTxnResponse, AddPartitionsToTxnRequest,
    AddPartitionsToTxnResponse, EndTxnRequest, EndTxnResponse, TxnOffsetCommitRequest,
    TxnOffsetCommitResponse,
};

use crate::conn::Conn;
use crate::core::PositionOp;
use crate::errors::*;
use crate::server::Ctx;

fn coordinates(ctx: &Ctx) -> bool {
    ctx.broker.cluster().leads()
}

pub async fn add_partitions(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: AddPartitionsToTxnRequest,
) -> AddPartitionsToTxnResponse {
    let tid = req.v3_and_below_transactional_id.0.to_string();
    let wanted: Vec<(String, i32)> = req
        .v3_and_below_topics
        .iter()
        .flat_map(|t| {
            let name = t.name.0.to_string();
            t.partitions.iter().map(move |p| (name.clone(), *p))
        })
        .collect();
    let code = if !coordinates(ctx) {
        NOT_COORDINATOR
    } else {
        ctx.txns
            .add_partitions(
                conn.bearer.as_deref(),
                &tid,
                req.v3_and_below_producer_id.0,
                req.v3_and_below_producer_epoch,
                &wanted,
            )
            .await
    };
    let results = req
        .v3_and_below_topics
        .into_iter()
        .map(|t| {
            AddPartitionsToTxnTopicResult::default()
                .with_name(t.name)
                .with_results_by_partition(
                    t.partitions
                        .into_iter()
                        .map(|p| {
                            AddPartitionsToTxnPartitionResult::default()
                                .with_partition_index(p)
                                .with_partition_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect();
    AddPartitionsToTxnResponse::default().with_results_by_topic_v3_and_below(results)
}

pub async fn add_offsets(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: AddOffsetsToTxnRequest,
) -> AddOffsetsToTxnResponse {
    let code = if !coordinates(ctx) {
        NOT_COORDINATOR
    } else {
        ctx.txns
            .add_offsets(
                conn.bearer.as_deref(),
                req.transactional_id.0.as_str(),
                req.producer_id.0,
                req.producer_epoch,
                req.group_id.0.as_str(),
            )
            .await
    };
    AddOffsetsToTxnResponse::default().with_error_code(code)
}

pub async fn offset_commit(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: TxnOffsetCommitRequest,
    version: i16,
) -> TxnOffsetCommitResponse {
    let group = req.group_id.0.to_string();
    let mut code = if !coordinates(ctx) {
        NOT_COORDINATOR
    } else if !super::group::valid_group(&group) {
        INVALID_GROUP_ID
    } else if version >= 3 && req.generation_id >= 0 {
        // v3 carries the member: a consume-transform-produce loop commits only
        // for a generation it still belongs to.
        ctx.groups
            .may_commit(&group, req.member_id.as_str(), req.generation_id)
    } else {
        NONE
    };
    if code == NONE {
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
        code = ctx
            .txns
            .stage_offsets(
                conn.bearer.as_deref(),
                req.transactional_id.0.as_str(),
                req.producer_id.0,
                req.producer_epoch,
                ops,
            )
            .await;
    }
    let topics = req
        .topics
        .into_iter()
        .map(|t| {
            TxnOffsetCommitResponseTopic::default()
                .with_name(t.name)
                .with_partitions(
                    t.partitions
                        .into_iter()
                        .map(|p| {
                            TxnOffsetCommitResponsePartition::default()
                                .with_partition_index(p.partition_index)
                                .with_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect();
    TxnOffsetCommitResponse::default().with_topics(topics)
}

pub async fn end(ctx: &Arc<Ctx>, conn: &Conn, req: EndTxnRequest) -> EndTxnResponse {
    let code = if !coordinates(ctx) {
        NOT_COORDINATOR
    } else {
        ctx.txns
            .end(
                conn.bearer.as_deref(),
                req.transactional_id.0.as_str(),
                req.producer_id.0,
                req.producer_epoch,
                req.committed,
            )
            .await
    };
    EndTxnResponse::default().with_error_code(code)
}
