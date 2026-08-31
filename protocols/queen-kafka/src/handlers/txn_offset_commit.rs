//! TxnOffsetCommit (28) — the offsets of a consume-transform-produce loop.
//!
//! The second half of KIP-447, and the request that makes exactly-once
//! processing exactly-once: the offsets committed here are written **in the
//! same Postgres transaction as the records the loop produced**, so a crash
//! cannot leave "the output was written and the input was not consumed" or the
//! other way round. Nothing is written when this request is answered — the
//! offsets are STAGED, and `EndTxn(commit)` is what writes them
//! ([`crate::txn`]).
//!
//! Note that in Apache Kafka this request goes to the **group** coordinator and
//! not to the transaction coordinator. In single-node mode they are the same
//! process on the same connection, which is one more reason transactions are
//! refused when `QUEEN_KAFKA_NODE_ID` is set.
//!
//! ## Membership is checked by the SAME function every other commit uses
//!
//! [`crate::coordinator::Coordinator::check_commit`], so the group APIs cannot
//! grow two opinions about what a valid committer is — including its one
//! deliberate hole, the simple `assign()`-based consumer that manages no
//! membership and commits with generation -1.
//!
//! ## v3 is mandatory, and it is measured
//!
//! `TxnOffsetCommitRequest$Builder.build(short)` in kafka-clients 3.9.2 throws
//! `UnsupportedVersionException` — *"Broker doesn't support group metadata
//! commit API on version"* — for any version below 3 whenever group metadata is
//! set, and every KIP-447 loop sets it. So advertising this API below v3 would
//! make the flagship use case throw before a single byte reached the wire.
//!
//! ## `group_instance_id` arrives at v3, and it is NOT the DescribeGroups case
//!
//! The table's rule is that a version carrying a field this facade does not
//! model must not be advertised, which is why JoinGroup stops at v4 and
//! Sync/Heartbeat/Leave at v2. This one is different for a checkable reason:
//! **no consumer of this facade can ever have a `group.instance.id`**, because
//! it is only expressible at JoinGroup v5, which is outside the advertised
//! window — a Java consumer configured with one fails at join and never reaches
//! a transactional offset commit. The field arrives structurally and can only
//! ever be null.
//!
//! The handler still does not pretend: a NON-NULL `group_instance_id` is
//! answered UNKNOWN_MEMBER_ID (25) per partition. That is the honest answer —
//! the member it names cannot be in a group this coordinator formed — and it is
//! a code the Java client already handles on the offset family.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::txn_offset_commit_response::{
    TxnOffsetCommitResponsePartition, TxnOffsetCommitResponseTopic,
};
use kafka_protocol::messages::{TxnOffsetCommitRequest, TxnOffsetCommitResponse};

use crate::handlers::metadata;
use crate::idempotent;
use crate::offsets::{self, Committed};
use crate::txn::{self, Full};
use crate::Facade;

/// Kafka's "no offset": the only negative a client may commit.
const NO_OFFSET: i64 = -1;

/// Handle one TxnOffsetCommit. The membership check is the only await.
pub async fn handle(
    facade: &Facade,
    req: &TxnOffsetCommitRequest,
    token: Option<&str>,
) -> TxnOffsetCommitResponse {
    let group = req.group_id.0.as_str();
    if let Some(e) = crate::coordinator::invalid_group_id(group) {
        return refuse_all(req, e);
    }
    // A member id this coordinator cannot have issued. See the module header.
    if req.group_instance_id.is_some() {
        return refuse_all(req, ResponseError::UnknownMemberId);
    }
    let Some(id) = idempotent::transactional_id(Some(req.transactional_id.0.as_str())) else {
        return refuse_all(req, txn::Fault::Unknown.code());
    };
    // The same membership fence an ordinary OffsetCommit passes, from the same
    // function: a member of a generation that has ended must not overwrite the
    // progress of the one that replaced it, and a transaction does not exempt
    // it from that.
    if let Some(e) = facade
        .coordinator
        .check_commit(group, req.member_id.as_str(), req.generation_id)
        .await
    {
        tracing::debug!(
            target: "kafka",
            group,
            member = req.member_id.as_str(),
            generation = req.generation_id,
            error = ?e,
            "transactional offset commit refused"
        );
        return refuse_all(req, e);
    }

    let tenant = facade.catalog.tenant_key(token);
    let (pid, epoch) = (req.producer_id.0, req.producer_epoch);
    // Registering the group HERE as well as at AddOffsetsToTxn is deliberate:
    // the two requests carry the same group and a producer that sent only this
    // one still needs the commit's index row written under the right name. It
    // is also where a group that disagrees with the one already registered is
    // refused.
    match facade.txns.add_offsets(&tenant, id, pid, epoch, group) {
        Ok(Ok(())) => {}
        Ok(Err(fault)) | Err(fault) => return refuse_all(req, fault.code()),
    }

    let now = offsets::now_millis();
    let mut topics = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let name = topic.name.0.as_str();
        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for p in &topic.partitions {
            let error = stage(
                facade,
                &tenant,
                id,
                pid,
                epoch,
                group,
                name,
                p.partition_index,
                p.committed_offset,
                p.committed_metadata.as_ref().map(|m| m.as_str()),
                now,
            );
            partitions.push(answer(p.partition_index, error));
        }
        topics.push(
            TxnOffsetCommitResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(partitions),
        );
    }
    TxnOffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(topics)
}

/// Stage one partition's offset, or say why it cannot be.
///
/// The refusals are the ones an ordinary OffsetCommit applies, in the same
/// order and with the same codes ([`crate::handlers::offset_commit`]), plus the
/// budget of §5.2 — and the budget answers INVALID_COMMIT_OFFSET_SIZE for the
/// same reason a key that will not fit does: **a commit this facade cannot
/// store must not read back later as "never committed"**.
#[allow(clippy::too_many_arguments)]
fn stage(
    facade: &Facade,
    tenant: &crate::identity::TenantKey,
    id: &str,
    pid: i64,
    epoch: i16,
    group: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    metadata: Option<&str>,
    now: i64,
) -> Option<ResponseError> {
    if let Some(e) = metadata::not_a_topic_here(topic) {
        return Some(e);
    }
    if partition < 0 {
        return Some(ResponseError::UnknownTopicOrPartition);
    }
    if offset < NO_OFFSET {
        return Some(ResponseError::InvalidCommitOffsetSize);
    }
    let metadata = metadata.unwrap_or_default();
    if metadata.len() > offsets::MAX_METADATA_BYTES {
        return Some(ResponseError::OffsetMetadataTooLarge);
    }
    let Some(key) = offsets::key(group, topic, partition) else {
        return Some(ResponseError::InvalidCommitOffsetSize);
    };
    let committed = Committed {
        offset,
        metadata: metadata.to_string(),
        ts: now,
    };
    match facade
        .txns
        .stage_offset(tenant, id, pid, epoch, key, committed)
    {
        Ok(Ok(())) => None,
        Ok(Err(Full::Offsets)) => {
            tracing::warn!(
                target: "kafka",
                transactional_id = %id,
                group,
                max = txn::MAX_TXN_OFFSETS,
                "a transaction is committing more partitions than one bundle's KV rider holds"
            );
            Some(ResponseError::InvalidCommitOffsetSize)
        }
        // Unreachable: `stage_offset` answers only `Full::Offsets`.
        Ok(Err(_)) => Some(ResponseError::UnknownServerError),
        Err(fault) => Some(fault.code()),
    }
}

/// Answer every partition of the request with one error. This API has no
/// top-level error code, exactly as OffsetCommit has none.
fn refuse_all(req: &TxnOffsetCommitRequest, error: ResponseError) -> TxnOffsetCommitResponse {
    TxnOffsetCommitResponse::default()
        .with_throttle_time_ms(0)
        .with_topics(
            req.topics
                .iter()
                .map(|topic| {
                    TxnOffsetCommitResponseTopic::default()
                        .with_name(topic.name.clone())
                        .with_partitions(
                            topic
                                .partitions
                                .iter()
                                .map(|p| answer(p.partition_index, Some(error)))
                                .collect(),
                        )
                })
                .collect(),
        )
}

fn answer(partition: i32, error: Option<ResponseError>) -> TxnOffsetCommitResponsePartition {
    TxnOffsetCommitResponsePartition::default()
        .with_partition_index(partition)
        .with_error_code(error.map_or(0, |e| e.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::NO_GENERATION;
    use crate::handlers::testing::{facade, facade_and_queen};
    use kafka_protocol::messages::txn_offset_commit_request::{
        TxnOffsetCommitRequestPartition, TxnOffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{GroupId, ProducerId, TopicName, TransactionalId};
    use kafka_protocol::protocol::StrBytes;
    use std::time::Duration;

    const PID: i64 = 7;

    fn request(id: &str, group: &str, topics: &[(&str, &[(i32, i64)])]) -> TxnOffsetCommitRequest {
        TxnOffsetCommitRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_producer_id(ProducerId(PID))
            .with_producer_epoch(0)
            .with_generation_id(NO_GENERATION)
            .with_member_id(StrBytes::from_static_str(""))
            .with_topics(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        TxnOffsetCommitRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partitions(
                                partitions
                                    .iter()
                                    .map(|(index, offset)| {
                                        TxnOffsetCommitRequestPartition::default()
                                            .with_partition_index(*index)
                                            .with_committed_offset(*offset)
                                            .with_committed_metadata(Some(
                                                StrBytes::from_static_str(""),
                                            ))
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn bound(id: &str) -> Facade {
        let f = facade(&[("orders", 4)]);
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, id, PID, 0, 100, 1, Duration::from_secs(60))
            .unwrap();
        f
    }

    fn codes(resp: &TxnOffsetCommitResponse) -> Vec<i16> {
        resp.topics
            .iter()
            .flat_map(|t| t.partitions.iter().map(|p| p.error_code))
            .collect()
    }

    /// The decisive property of this handler: it writes NOTHING. The offsets
    /// are staged, and `EndTxn(commit)` is what puts them in Queen beside the
    /// records — which is the whole of exactly-once processing here.
    #[tokio::test]
    async fn offsets_are_staged_and_nothing_is_written() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, "tx", PID, 0, 100, 1, Duration::from_secs(60))
            .unwrap();
        let resp = handle(
            &f,
            &request("tx", "g", &[("orders", &[(0, 42), (1, 7)])]),
            None,
        )
        .await;
        assert_eq!(codes(&resp), vec![0, 0]);
        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "a transactional offset commit wrote to the store"
        );
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 0, |t| t.offsets.clone())
                .unwrap(),
            vec![
                (
                    "qk:group:g:orders:0".to_string(),
                    Committed {
                        offset: 42,
                        metadata: String::new(),
                        ts: f
                            .txns
                            .with(&tenant, "tx", PID, 0, |t| t.offsets[0].1.ts)
                            .unwrap()
                    }
                ),
                (
                    "qk:group:g:orders:1".to_string(),
                    Committed {
                        offset: 7,
                        metadata: String::new(),
                        ts: f
                            .txns
                            .with(&tenant, "tx", PID, 0, |t| t.offsets[1].1.ts)
                            .unwrap()
                    }
                ),
            ]
        );
    }

    #[tokio::test]
    async fn an_unknown_transactional_id_refuses_every_partition() {
        let f = facade(&[("orders", 4)]);
        let resp = handle(&f, &request("tx", "g", &[("orders", &[(0, 1)])]), None).await;
        assert_eq!(codes(&resp), vec![ResponseError::InvalidTxnState.code()]);
    }

    #[tokio::test]
    async fn a_fenced_producer_commits_no_offset() {
        let f = bound("tx");
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, "tx", PID, 1, 101, 2, Duration::from_secs(60))
            .unwrap();
        let resp = handle(&f, &request("tx", "g", &[("orders", &[(0, 1)])]), None).await;
        assert_eq!(codes(&resp), vec![ResponseError::ProducerFenced.code()]);
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 1, |t| t.offsets.len())
                .unwrap(),
            0
        );
    }

    /// A non-null `group_instance_id` names a member this coordinator cannot
    /// have formed, because static membership is outside every advertised
    /// window. Answered, not ignored.
    #[tokio::test]
    async fn a_static_member_is_unknown_here() {
        let f = bound("tx");
        let req = request("tx", "g", &[("orders", &[(0, 1)])])
            .with_group_instance_id(Some(StrBytes::from_static_str("instance-1")));
        let resp = handle(&f, &req, None).await;
        assert_eq!(codes(&resp), vec![ResponseError::UnknownMemberId.code()]);
    }

    /// The budget of the bundle's KV rider, surfaced with the code
    /// `crate::offsets` already chose for a commit that cannot be stored.
    #[tokio::test]
    async fn more_partitions_than_the_bundle_holds_are_refused() {
        let f = bound("tx");
        let all: Vec<(i32, i64)> = (0..txn::MAX_TXN_OFFSETS as i32 + 2)
            .map(|p| (p, i64::from(p)))
            .collect();
        let resp = handle(&f, &request("tx", "g", &[("orders", &all)]), None).await;
        let codes = codes(&resp);
        assert!(codes[..txn::MAX_TXN_OFFSETS].iter().all(|c| *c == 0));
        assert_eq!(
            codes[txn::MAX_TXN_OFFSETS],
            ResponseError::InvalidCommitOffsetSize.code()
        );
    }

    #[tokio::test]
    async fn the_ordinary_commit_refusals_still_apply() {
        let f = bound("tx");
        let resp = handle(
            &f,
            &request(
                "tx",
                "g",
                &[
                    ("__consumer_offsets", &[(0, 1)]),
                    ("orders", &[(-1, 1), (0, -2)]),
                ],
            ),
            None,
        )
        .await;
        assert_eq!(
            codes(&resp),
            vec![
                ResponseError::UnknownTopicOrPartition.code(),
                ResponseError::UnknownTopicOrPartition.code(),
                ResponseError::InvalidCommitOffsetSize.code(),
            ]
        );
    }

    /// A commit whose group disagrees with the one already in the transaction
    /// is refused rather than silently spending the budget on a second index
    /// row that the bundle has no operation for.
    #[tokio::test]
    async fn a_second_group_is_refused() {
        let f = bound("tx");
        handle(&f, &request("tx", "g", &[("orders", &[(0, 1)])]), None).await;
        let resp = handle(&f, &request("tx", "other", &[("orders", &[(0, 1)])]), None).await;
        assert_eq!(codes(&resp), vec![ResponseError::InvalidTxnState.code()]);
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        use bytes::BytesMut;
        use kafka_protocol::protocol::{Decodable, Encodable, Message};

        let f = facade(&[("orders", 4)]);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::TxnOffsetCommit as i16)
            .expect("TxnOffsetCommit is advertised");
        assert!(
            row.min >= TxnOffsetCommitRequest::VERSIONS.min
                && row.max <= TxnOffsetCommitRequest::VERSIONS.max
        );
        let tenant = f.catalog.tenant_key(f.token());

        for version in row.min..=row.max {
            let id = format!("tx-v{version}");
            f.txns
                .bind(&tenant, &id, PID, 0, 100, 1, Duration::from_secs(60))
                .unwrap();
            let mut wire = BytesMut::new();
            request(&id, "g", &[("orders", &[(0, 42)])])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = TxnOffsetCommitRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = TxnOffsetCommitResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(codes(&back), vec![0], "v{version}");
        }
    }
}
