//! ApiVersions, Metadata, FindCoordinator, InitProducerId.
//!
//! The cluster a client sees is the raft cluster: every voter is a broker, the
//! raft leader leads every partition and coordinates every group, and every
//! voter is a replica in sync (an acknowledged write is on a majority).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use bytes::Bytes;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::metadata_response::{
    MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
};
use kafka_protocol::messages::{
    ApiKey, ApiVersionsResponse, BrokerId, FindCoordinatorRequest, FindCoordinatorResponse,
    InitProducerIdRequest, InitProducerIdResponse, MetadataRequest, MetadataResponse, ProducerId,
    TopicName,
};

use super::s;
use crate::conn::{encode, Conn};
use crate::errors::*;
use crate::server::Ctx;
use crate::topics::{now_ms, valid_topic_name};
use crate::versions::ADVERTISED;

pub const CLUSTER_ID: &str = "queen";

/// ApiVersions. A version this side does not know is answered in v0 with
/// UNSUPPORTED_VERSION and the table, so the client retries with one it does
/// (KIP-511).
pub fn api_versions(version: i16, correlation: i32) -> Option<Bytes> {
    let known = (0..=3).contains(&version);
    let resp = ApiVersionsResponse::default()
        .with_error_code(if known { NONE } else { UNSUPPORTED_VERSION })
        .with_api_keys(
            ADVERTISED
                .iter()
                .map(|a| {
                    ApiVersion::default()
                        .with_api_key(a.key as i16)
                        .with_min_version(a.min)
                        .with_max_version(a.max)
                })
                .collect(),
        );
    encode(
        ApiKey::ApiVersions,
        if known { version } else { 0 },
        correlation,
        &resp,
    )
}

pub async fn metadata(
    ctx: &Arc<Ctx>,
    conn: &Conn,
    req: MetadataRequest,
    version: i16,
) -> MetadataResponse {
    let view = ctx.broker.cluster();
    let bearer = conn.bearer.as_deref();
    let brokers = view
        .nodes
        .iter()
        .map(|n| {
            MetadataResponseBroker::default()
                .with_node_id(BrokerId(n.id))
                .with_host(s(n.host.clone()))
                .with_port(n.port)
        })
        .collect();
    let replicas: Vec<BrokerId> = view.nodes.iter().map(|n| BrokerId(n.id)).collect();
    let leader = view.leader.unwrap_or(-1);
    let resp = MetadataResponse::default()
        .with_brokers(brokers)
        .with_cluster_id(Some(s(CLUSTER_ID)))
        .with_controller_id(BrokerId(leader));

    // v0: an empty list is every topic; v1+: null is every topic.
    let requested: Option<Vec<String>> = match req.topics {
        None => None,
        Some(t) if t.is_empty() && version == 0 => None,
        Some(t) => Some(
            t.into_iter()
                .filter_map(|t| t.name.map(|n| n.0.to_string()))
                .collect(),
        ),
    };
    let allow_create = ctx.cfg.auto_create && (version < 4 || req.allow_auto_topic_creation);

    let snap = match ctx.topics.snapshot(bearer).await {
        Ok(s) => s,
        Err(e) => {
            tracing::warn!(target: "kafka", error = %e, "cannot list the topics");
            let topics = requested
                .unwrap_or_default()
                .into_iter()
                .map(|n| {
                    MetadataResponseTopic::default()
                        .with_name(Some(TopicName(s(n))))
                        .with_error_code(LEADER_NOT_AVAILABLE)
                })
                .collect();
            return resp.with_topics(topics);
        }
    };

    let partitions = |width: i32| -> Vec<MetadataResponsePartition> {
        (0..width)
            .map(|i| {
                MetadataResponsePartition::default()
                    .with_error_code(if leader < 0 {
                        LEADER_NOT_AVAILABLE
                    } else {
                        NONE
                    })
                    .with_partition_index(i)
                    .with_leader_id(BrokerId(leader))
                    .with_leader_epoch(0)
                    .with_replica_nodes(replicas.clone())
                    .with_isr_nodes(replicas.clone())
            })
            .collect()
    };
    let topic = |name: &str, error: i16, width: i32| {
        MetadataResponseTopic::default()
            .with_name(Some(TopicName(s(name))))
            .with_error_code(error)
            .with_partitions(if error == NONE {
                partitions(width)
            } else {
                Vec::new()
            })
    };

    let mut topics = Vec::new();
    match requested {
        None => {
            let mut names: Vec<(&String, &i32)> = snap.widths.iter().collect();
            names.sort();
            for (name, width) in names {
                topics.push(topic(name, NONE, *width));
            }
        }
        Some(names) => {
            let mut created = false;
            for name in names {
                if !valid_topic_name(&name) {
                    topics.push(topic(&name, INVALID_TOPIC_EXCEPTION, 0));
                } else if let Some(w) = snap.widths.get(&name) {
                    topics.push(topic(&name, NONE, *w));
                } else if allow_create {
                    match ctx.queen.create_queue(bearer, &name).await {
                        Ok(()) => {
                            created = true;
                            tracing::info!(target: "kafka", topic = %name, "topic created on first use");
                            topics.push(topic(&name, NONE, ctx.topics.default_partitions()));
                        }
                        Err(e) => {
                            tracing::warn!(target: "kafka", topic = %name, error = %e, "cannot create the topic");
                            topics.push(topic(&name, LEADER_NOT_AVAILABLE, 0));
                        }
                    }
                } else {
                    topics.push(topic(&name, UNKNOWN_TOPIC_OR_PARTITION, 0));
                }
            }
            if created {
                ctx.topics.invalidate();
            }
        }
    }
    resp.with_topics(topics)
}

pub fn find_coordinator(
    ctx: &Arc<Ctx>,
    _req: FindCoordinatorRequest,
    _version: i16,
) -> FindCoordinatorResponse {
    let view = ctx.broker.cluster();
    match view.leader_node() {
        Some(n) => FindCoordinatorResponse::default()
            .with_error_code(NONE)
            .with_node_id(BrokerId(n.id))
            .with_host(s(n.host.clone()))
            .with_port(n.port),
        None => FindCoordinatorResponse::default()
            .with_error_code(COORDINATOR_NOT_AVAILABLE)
            .with_node_id(BrokerId(-1))
            .with_host(s(""))
            .with_port(-1),
    }
}

/// An idempotent producer's id. Unique without a round trip: the clock, the
/// node, and a counter. Transactional producers are phase 2.
pub fn init_producer_id(ctx: &Arc<Ctx>, req: InitProducerIdRequest) -> InitProducerIdResponse {
    if req.transactional_id.is_some() {
        return InitProducerIdResponse::default()
            .with_error_code(TRANSACTIONAL_ID_AUTHORIZATION_FAILED)
            .with_producer_id(ProducerId(-1))
            .with_producer_epoch(-1);
    }
    if req.producer_id.0 >= 0 && req.producer_epoch >= 0 && req.producer_epoch < i16::MAX - 1 {
        // KIP-360: the producer bumps its own epoch and keeps its id.
        return InitProducerIdResponse::default()
            .with_producer_id(req.producer_id)
            .with_producer_epoch(req.producer_epoch + 1);
    }
    let seq = ctx.producer_ids.fetch_add(1, Ordering::Relaxed);
    let node = ctx.broker.cluster().node_id.max(0) as i64;
    let id = ((now_ms() & ((1 << 41) - 1)) << 20) | ((node & 0xf) << 16) | (seq as i64 & 0xffff);
    InitProducerIdResponse::default()
        .with_producer_id(ProducerId(id))
        .with_producer_epoch(0)
}
