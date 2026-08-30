//! OffsetDelete (key 47), v0 — `kafka-consumer-groups.sh --delete-offsets`.
//!
//! The last thing that tool could not do here, and the one group API whose
//! guard is not membership but SUBSCRIPTION.
//!
//! ## Say this part out loud, as DeleteGroups does
//!
//! This is an irreversible delete of committed offsets, reachable from any
//! authenticated Kafka client. It is not a privilege escalation — the same
//! bearer can already remove the same KV keys over `POST /api/v1/kv` — but a
//! group that loses its position runs `auto.offset.reset` on its next start: a
//! replay of the whole topic, or a jump past everything it had not read. That
//! is exactly Apache Kafka's behaviour, and Kafka's own guard against it is the
//! rule below.
//!
//! ## Kafka's rule, kept exactly
//!
//! `GroupCoordinator.handleDeleteOffsets`, measured against
//! `apache/kafka:3.9.1` rather than recalled:
//!
//!   * an unknown or Dead group is GROUP_ID_NOT_FOUND at the TOP level;
//!   * an EMPTY group has every named partition deletable;
//!   * a LIVE group running the consumer protocol has a partition deletable
//!     only if the group is not subscribed to its topic. A subscribed topic's
//!     partitions answer GROUP_SUBSCRIBED_TO_TOPIC (86) — and an UNSUBSCRIBED
//!     topic's partitions are deleted even though the group is live, which is
//!     the half of the rule that is easy to get wrong by being merely
//!     conservative;
//!   * a live group of any other protocol type — a simple `assign()` group,
//!     Kafka Connect — has everything deletable.
//!
//! ## The one place this facade parses a member's metadata
//!
//! The subscription is the union of the members' `topics`, decoded from the
//! bytes each member sent at JoinGroup for the elected protocol. Those bytes
//! are a two-byte version followed by a `ConsumerProtocolSubscription` body,
//! and the coordinator hands them through verbatim — it never parses them, by
//! rule ([`crate::coordinator::MemberDescription`]), so the parsing is here.
//!
//! **If any member's bytes fail to decode, the group is treated as subscribed
//! to every topic the request named.** Conservative on purpose: the failure
//! mode is then a refused delete, never a delete Kafka would have refused. It
//! is also new surface, and it is where a malformed client could make
//! `--delete-offsets` stop working for its own group; one sampled log line says
//! so rather than leaving it to be discovered.
//!
//! ## The `qk:groups:` index is NOT touched, and that is a deviation
//!
//! OffsetDelete removes offsets; it does not remove the group. So the durable
//! existence row ([`crate::offsets::index_key`]) is left exactly where it is,
//! which keeps `handlers::delete_groups` the one and only thing that makes a
//! group stop existing.
//!
//! Measured deviation, recorded rather than assumed: on `apache/kafka:3.9.1`,
//! deleting the LAST offsets of an already-empty group makes that group vanish
//! from `--list` and answer GROUP_ID_NOT_FOUND to the next request, while a
//! partial delete leaves it listed. Here it stays listed either way. The
//! alternative — deleting the index row when the group has no offsets left —
//! would need a prefix walk on every OffsetDelete to find out, and would make
//! this API a second way to delete a group. See `compat/ERRORS.md`.
//!
//! ## Cluster mode: the same gate as OffsetCommit, and the same fence
//!
//! This is a group-addressed WRITE. The argument that put the ownership guard
//! in front of the simple consumer's commit — two nodes, last writer wins,
//! nothing to catch it — applies identically to a delete, and there is a second
//! one: a non-owner must not be allowed to read the group's membership to
//! decide the subscription rule at all, which is `delete_groups`' argument word
//! for word. The guard answers at the TOP level here, because unlike
//! OffsetCommit this API has a top-level error field.
//!
//! Inside the write, the same compare-and-set fence the commit path rides: a
//! node stale about the live set — and therefore still believing the guard
//! passed — removes NOTHING rather than removing the real owner's offsets.

use std::collections::HashSet;

use bytes::{Buf, Bytes};
use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::offset_delete_response::{
    OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use kafka_protocol::messages::{
    ConsumerProtocolSubscription, OffsetDeleteRequest, OffsetDeleteResponse, TopicName,
};
use kafka_protocol::protocol::{Decodable, Message, StrBytes};

use crate::cluster::fence::FenceOp;
use crate::coordinator::{self, GroupDescription};
use crate::handlers::metadata;
use crate::offsets;
use crate::queen::KvOp;
use crate::Facade;

/// The protocol type whose member metadata is a `ConsumerProtocolSubscription`.
/// Kafka's own discriminator: `group.isConsumerGroup` is this string and nothing
/// else, and every other protocol type has everything deletable.
const CONSUMER_PROTOCOL: &str = "consumer";

/// How many partitions one request may name.
///
/// `MAX_KV_OPS_PER_CALL * 16` — above the widest plausible topic here and well
/// past what any tool sends, so it is a ceiling and not a limit anyone meets.
/// It exists because without it one frame buys an unbounded run of admin calls
/// on a connection that is muted until the whole response is written.
const MAX_DELETED_PARTITIONS: usize = crate::queen::MAX_KV_OPS_PER_CALL * 16;

/// One line per window when a member's subscription bytes cannot be read.
static UNDECODABLE: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// What one requested (topic, partition) turns into.
enum Slot {
    /// Answered without touching the store.
    Refused(ResponseError),
    /// Nothing to delete, and that is a SUCCESS: an offset that was never
    /// committed is an offset Kafka answers 0 for.
    Zero,
    /// The index of this partition's key in the delete batch.
    Delete(usize),
}

pub async fn handle(
    facade: &Facade,
    req: &OffsetDeleteRequest,
    token: Option<&str>,
) -> OffsetDeleteResponse {
    let group = req.group_id.0.as_str();

    if let Some(e) = coordinator::invalid_group_id(group) {
        return refuse(e);
    }
    // Cluster mode's ownership guard, before anything is read or written. In
    // single mode this is `None` without reading anything.
    if let Some(e) = facade.cluster.group_guard(group) {
        tracing::debug!(
            target: "kafka",
            group,
            error = ?e,
            "offset delete refused: this node does not coordinate the group"
        );
        return refuse(e);
    }
    let named: usize = req.topics.iter().map(|t| t.partitions.len()).sum();
    if named > MAX_DELETED_PARTITIONS {
        tracing::warn!(
            target: "kafka",
            group,
            named,
            cap = MAX_DELETED_PARTITIONS,
            "one OffsetDelete named more partitions than this facade deletes at a time"
        );
        return refuse(ResponseError::InvalidRequest);
    }

    // EXISTENCE, in the cheap order: the live actor first, because it is a
    // message to a task in this process, and the durable index only when there
    // is no actor. `load_index` is one `getMany` for one key, not a prefix
    // walk.
    let described = facade.coordinator.describe_group(group).await;
    if described.is_none() {
        match offsets::load_index(facade.queen.as_ref(), &[group.to_string()], token).await {
            Ok(rows) if rows.first().is_some_and(Option::is_some) => {}
            // Nothing in the registry and no durable trace: Kafka's own answer
            // for a group it has never heard of, and the one `delete_groups`
            // already measured.
            Ok(_) => return refuse(ResponseError::GroupIdNotFound),
            Err(e) => {
                tracing::warn!(
                    target: "kafka",
                    group,
                    error = %e,
                    "OffsetDelete cannot read the group index"
                );
                return refuse(offsets::kafka_error(&e));
            }
        }
    }

    let subscription = subscription_of(described.as_ref(), group);

    // The plan, in request order, and the keys it needs in batch order.
    let mut keys: Vec<String> = Vec::new();
    let mut slots: Vec<Vec<Slot>> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let name = topic.name.0.as_str();
        slots.push(
            topic
                .partitions
                .iter()
                .map(|p| stage(&mut keys, &subscription, group, name, p.partition_index))
                .collect(),
        );
    }

    // The fence, and the delete. Both are skipped entirely when the request
    // named nothing this facade could have stored — a delete of no keys must
    // not buy a fence read.
    let results = if keys.is_empty() {
        Vec::new()
    } else {
        match fence_for(facade, group, token).await {
            Ok(fence) => {
                offsets::delete_offsets(facade.queen.as_ref(), group, &keys, fence.as_ref(), token)
                    .await
            }
            Err(e) => return refuse(e),
        }
    };

    render(req, &slots, &results)
}

/// A top-level refusal: the error code and nothing else.
///
/// The shape Apache Kafka's own `OffsetDeleteRequest.getErrorResponse` builds —
/// an empty `topics` list — and the shape the Java AdminClient reads, which
/// checks the top-level code before it looks at a partition.
fn refuse(error: ResponseError) -> OffsetDeleteResponse {
    OffsetDeleteResponse::default().with_error_code(error.code())
}

/// What the group is subscribed to, for the purposes of Kafka's rule.
///
/// `None` — the empty set — for an empty group, for a group with no live actor
/// and for any protocol type but `consumer`, all of which have everything
/// deletable. See the module header for the conservative fallback.
enum Subscription {
    Topics(HashSet<String>),
    /// A member's bytes could not be read, so nothing is deletable.
    Everything,
}

impl Subscription {
    fn covers(&self, topic: &str) -> bool {
        match self {
            Subscription::Topics(topics) => topics.contains(topic),
            Subscription::Everything => true,
        }
    }
}

fn subscription_of(described: Option<&GroupDescription>, group: &str) -> Subscription {
    let Some(described) = described else {
        return Subscription::Topics(HashSet::new());
    };
    // The protocol type is cleared when a group empties, so an Empty group
    // lands here as `None` and has everything deletable — which is Kafka's
    // first rule, arrived at without a second branch.
    if described.protocol_type.as_deref() != Some(CONSUMER_PROTOCOL) {
        return Subscription::Topics(HashSet::new());
    }
    let mut topics = HashSet::new();
    for member in &described.members {
        match subscribed_topics(&member.metadata) {
            Some(named) => topics.extend(named),
            None => {
                if let Some(suppressed) = UNDECODABLE.tick_now() {
                    tracing::warn!(
                        target: "kafka",
                        group,
                        member = member.id.as_str(),
                        suppressed,
                        "a member's subscription bytes could not be read; OffsetDelete is \
                         refusing every topic of this group until they can be"
                    );
                }
                return Subscription::Everything;
            }
        }
    }
    Subscription::Topics(topics)
}

/// The topics one member's JoinGroup metadata names, or `None` if the bytes are
/// not a subscription this facade can read.
///
/// The wire shape is Kafka's `ConsumerProtocol`: a two-byte version, then the
/// `ConsumerProtocolSubscription` body at that version. The version is CLAMPED
/// upwards exactly as `ConsumerProtocol.checkVersionCompatibility` clamps it, so
/// a member from a newer client is read at the newest schema this build has
/// rather than refused — the fields this cares about are `0-3` and have never
/// moved. Trailing bytes are left alone for the same reason Kafka leaves them:
/// they are a later version's fields, not a corruption.
fn subscribed_topics(metadata: &Bytes) -> Option<Vec<String>> {
    let mut buf = metadata.clone();
    if buf.remaining() < 2 {
        return None;
    }
    let version = buf.get_i16();
    if version < 0 {
        return None;
    }
    let version = version.min(ConsumerProtocolSubscription::VERSIONS.max);
    let subscription = ConsumerProtocolSubscription::decode(&mut buf, version).ok()?;
    Some(
        subscription
            .topics
            .iter()
            .map(|t| t.as_str().to_string())
            .collect(),
    )
}

/// One requested partition's verdict, and its key when there is one to delete.
fn stage(
    keys: &mut Vec<String>,
    subscription: &Subscription,
    group: &str,
    topic: &str,
    partition: i32,
) -> Slot {
    // The same name rule as every other non-Metadata API, in the one code they
    // may answer.
    if let Some(e) = metadata::not_a_topic_here(topic) {
        return Slot::Refused(e);
    }
    if subscription.covers(topic) {
        return Slot::Refused(ResponseError::GroupSubscribedToTopic);
    }
    if partition < 0 {
        return Slot::Refused(ResponseError::UnknownTopicOrPartition);
    }
    // A key longer than the store's key column could never have been COMMITTED
    // — `handlers::offset_commit` refuses exactly this pair with
    // INVALID_COMMIT_OFFSET_SIZE — so it was never there, and Kafka answers 0
    // for deleting an offset that does not exist.
    let Some(key) = offsets::key(group, topic, partition) else {
        return Slot::Zero;
    };
    keys.push(key);
    Slot::Delete(keys.len() - 1)
}

/// The group's fence for this delete, or `None` in single mode.
///
/// The commit path threads a remembered version through `cluster::fence`; this
/// one READS the version it is going to expect, in one `getMany`, because a
/// delete is not on the commit path and has no cell to remember. The protection
/// is the same and it is the one that matters: whatever the fence holds now,
/// the delete's own conditional write is `required`, so an owner that commits
/// between this read and the delete ABORTS it, and nothing is removed.
///
/// It is a takeover when the fence names another node, exactly as the commit
/// path's one retry is: the ownership guard above has already established that
/// this node is the rendezvous owner under a view that is not stale.
async fn fence_for(
    facade: &Facade,
    group: &str,
    token: Option<&str>,
) -> Result<Option<FenceOp>, ResponseError> {
    let Some(state) = facade.cluster.state() else {
        return Ok(None);
    };
    let Some(key) = offsets::fence_key(group) else {
        // Unreachable: a fence key is shorter than any offset key of the same
        // group, so a group with a storable offset key has a storable fence.
        // Refused rather than deleted unfenced — an unfenced write in cluster
        // mode is the defect the fence exists for.
        return Err(ResponseError::CoordinatorNotAvailable);
    };
    let ops = [KvOp::GetMany {
        ns: offsets::NAMESPACE.to_string(),
        keys: vec![key.clone()],
    }];
    let answers = facade
        .queen
        .kv(&ops, token)
        .await
        .map_err(|e| offsets::kafka_error(&e))?;
    // 0 is "not there", which is what `expect` means by "must not exist" —
    // the same number a group's first commit sends.
    let expect = answers
        .first()
        .and_then(|a| a.rows.iter().find(|r| r.key == key))
        .map_or(0, |r| r.version);
    Ok(Some(FenceOp {
        key,
        value: serde_json::json!({
            "node": state.me.id,
            "incarnation": state.me.incarnation,
            // A delete belongs to no generation. `-1` is the same "no
            // generation" a simple consumer's commit writes, and this field is
            // only ever read by a human reading the row.
            "generation": coordinator::NO_GENERATION,
            "ts": std::time::UNIX_EPOCH
                .elapsed()
                .map(|d| d.as_millis() as i64)
                .unwrap_or_default(),
        }),
        expect,
    }))
}

/// The response, in the request's own order.
fn render(
    req: &OffsetDeleteRequest,
    slots: &[Vec<Slot>],
    results: &[crate::queen::Result<()>],
) -> OffsetDeleteResponse {
    let topics = req
        .topics
        .iter()
        .zip(slots)
        .map(|(topic, slots)| {
            OffsetDeleteResponseTopic::default()
                .with_name(TopicName(StrBytes::from_string(
                    topic.name.0.as_str().to_string(),
                )))
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .zip(slots)
                        .map(|(p, slot)| {
                            let error = match slot {
                                Slot::Refused(e) => Some(*e),
                                Slot::Zero => None,
                                Slot::Delete(at) => match results.get(*at) {
                                    Some(Ok(())) => None,
                                    Some(Err(e)) => Some(offsets::kafka_error(e)),
                                    // Unreachable: `delete_offsets` answers one
                                    // result per key. Reported rather than
                                    // assumed to have deleted.
                                    None => Some(ResponseError::CoordinatorNotAvailable),
                                },
                            };
                            OffsetDeleteResponsePartition::default()
                                .with_partition_index(p.partition_index)
                                .with_error_code(error.map_or(0, |e| e.code()))
                        })
                        .collect(),
                )
        })
        .collect();
    OffsetDeleteResponse::default().with_topics(topics)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::{clustered, facade_and_queen};
    use crate::offsets::Committed;
    use crate::queen::Error;
    use bytes::BytesMut;
    use kafka_protocol::messages::consumer_protocol_subscription::ConsumerProtocolSubscription;
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };
    use kafka_protocol::messages::{
        GroupId, JoinGroupRequest, ListGroupsRequest, OffsetCommitRequest,
    };
    use kafka_protocol::protocol::Encodable;

    const THREE: [(i32, &str, u16); 3] = [
        (1, "kafka-1.example.com", 9092),
        (2, "kafka-2.example.com", 9092),
        (3, "kafka-3.example.com", 9092),
    ];

    fn request(group: &str, topics: &[(&str, &[i32])]) -> OffsetDeleteRequest {
        OffsetDeleteRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_topics(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        OffsetDeleteRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string((*name).to_string())))
                            .with_partitions(
                                partitions
                                    .iter()
                                    .map(|p| {
                                        OffsetDeleteRequestPartition::default()
                                            .with_partition_index(*p)
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn code(resp: &OffsetDeleteResponse, topic: &str, partition: i32) -> i16 {
        resp.topics
            .iter()
            .find(|t| t.name.0.as_str() == topic)
            .unwrap_or_else(|| panic!("{topic} is not in the answer"))
            .partitions
            .iter()
            .find(|p| p.partition_index == partition)
            .unwrap_or_else(|| panic!("{topic}-{partition} is not in the answer"))
            .error_code
    }

    /// The bytes a real consumer sends at JoinGroup: the two-byte protocol
    /// version, then the subscription body.
    fn subscription_bytes(topics: &[&str], version: i16) -> Bytes {
        let mut out = BytesMut::new();
        out.extend_from_slice(&version.to_be_bytes());
        ConsumerProtocolSubscription::default()
            .with_topics(
                topics
                    .iter()
                    .map(|t| StrBytes::from_string((*t).to_string()))
                    .collect(),
            )
            .encode(&mut out, version)
            .expect("a subscription encodes");
        out.freeze()
    }

    async fn commit(f: &Facade, group: &str, topic: &str, partitions: &[i32]) {
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(coordinator::NO_GENERATION)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_string(topic.to_string())))
                .with_partitions(
                    partitions
                        .iter()
                        .map(|p| {
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(*p)
                                .with_committed_offset(41)
                        })
                        .collect(),
                )]);
        crate::handlers::offset_commit::handle(f, &req, None).await;
    }

    /// A live consumer group: one member whose metadata is a REAL encoded
    /// subscription naming `topics`.
    async fn join_subscribed(f: &Facade, group: &str, topics: &[&str], protocol_type: &str) {
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_string(protocol_type.to_string()))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(subscription_bytes(topics, 1))]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c", "/127.0.0.1").await;
        crate::handlers::join_group::handle(
            f,
            &req.with_member_id(minted.member_id.clone()),
            4,
            "c",
            "/127.0.0.1",
        )
        .await;
    }

    /// ...and one whose metadata is not a subscription at all.
    async fn join_with_metadata(f: &Facade, group: &str, metadata: Bytes) {
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str(CONSUMER_PROTOCOL))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(metadata)]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c", "/127.0.0.1").await;
        crate::handlers::join_group::handle(
            f,
            &req.with_member_id(minted.member_id.clone()),
            4,
            "c",
            "/127.0.0.1",
        )
        .await;
    }

    /// Kafka's first rule: an empty group has every named partition deleted,
    /// and the keys removed are exactly the ones the request named.
    #[tokio::test]
    async fn an_empty_group_has_every_partition_deleted() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "od1-g", "orders", &[0, 1, 2]).await;

        let resp = handle(&f, &request("od1-g", &[("orders", &[0, 2])]), None).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(code(&resp, "orders", 0), 0);
        assert_eq!(code(&resp, "orders", 2), 0);

        assert_eq!(
            api.kv_get(
                offsets::NAMESPACE,
                &offsets::key("od1-g", "orders", 0).unwrap()
            ),
            None
        );
        assert_eq!(
            api.kv_get(
                offsets::NAMESPACE,
                &offsets::key("od1-g", "orders", 2).unwrap()
            ),
            None
        );
        // The partition the request did not name is untouched.
        assert!(api
            .kv_get(
                offsets::NAMESPACE,
                &offsets::key("od1-g", "orders", 1).unwrap()
            )
            .is_some());
    }

    /// The guard, with a member whose metadata is a real encoded
    /// `ConsumerProtocolSubscription` naming the topic.
    #[tokio::test]
    async fn a_subscribed_topic_is_refused_86_and_keeps_its_offsets() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "od2-g", "orders", &[0]).await;
        join_subscribed(&f, "od2-g", &["orders"], CONSUMER_PROTOCOL).await;

        let resp = handle(&f, &request("od2-g", &[("orders", &[0])]), None).await;
        assert_eq!(resp.error_code, 0, "the refusal is per partition");
        assert_eq!(
            code(&resp, "orders", 0),
            ResponseError::GroupSubscribedToTopic.code()
        );
        assert!(
            api.kv_get(
                offsets::NAMESPACE,
                &offsets::key("od2-g", "orders", 0).unwrap()
            )
            .is_some(),
            "a refused delete removed the offset"
        );
    }

    /// ...and the half of the rule that proves the facade is not merely
    /// conservative: a LIVE group's offsets for a topic it is not subscribed to
    /// are deleted. Measured on apache/kafka:3.9.1, which does the same.
    #[tokio::test]
    async fn an_unsubscribed_topic_of_a_live_group_is_deleted() {
        let (f, api) = facade_and_queen(&[("orders", 4), ("clicks", 4)]);
        commit(&f, "od3-g", "orders", &[0]).await;
        commit(&f, "od3-g", "clicks", &[0]).await;
        join_subscribed(&f, "od3-g", &["orders"], CONSUMER_PROTOCOL).await;

        let resp = handle(
            &f,
            &request("od3-g", &[("orders", &[0]), ("clicks", &[0])]),
            None,
        )
        .await;
        assert_eq!(
            code(&resp, "orders", 0),
            ResponseError::GroupSubscribedToTopic.code()
        );
        assert_eq!(code(&resp, "clicks", 0), 0);
        assert!(api
            .kv_get(
                offsets::NAMESPACE,
                &offsets::key("od3-g", "clicks", 0).unwrap()
            )
            .is_none());
        assert!(api
            .kv_get(
                offsets::NAMESPACE,
                &offsets::key("od3-g", "orders", 0).unwrap()
            )
            .is_some());
    }

    /// The conservative fallback: bytes this facade cannot read make the whole
    /// group count as subscribed to everything, so nothing is deleted.
    #[tokio::test]
    async fn undecodable_member_metadata_refuses_everything() {
        for metadata in [
            Bytes::new(),
            Bytes::from_static(&[0x00]),
            // A negative protocol version, which Kafka's own reader refuses.
            Bytes::from_static(&[0xff, 0xff, 0x00, 0x00]),
            Bytes::from_static(b"not a subscription"),
        ] {
            let (f, api) = facade_and_queen(&[("orders", 4)]);
            commit(&f, "od4-g", "orders", &[0]).await;
            join_with_metadata(&f, "od4-g", metadata.clone()).await;

            let resp = handle(&f, &request("od4-g", &[("orders", &[0])]), None).await;
            assert_eq!(
                code(&resp, "orders", 0),
                ResponseError::GroupSubscribedToTopic.code(),
                "{metadata:?}"
            );
            assert!(
                api.kv_get(
                    offsets::NAMESPACE,
                    &offsets::key("od4-g", "orders", 0).unwrap()
                )
                .is_some(),
                "{metadata:?}: an unreadable subscription still deleted an offset"
            );
        }
    }

    /// A live group of any other protocol type — Kafka Connect, or a simple
    /// `assign()` group — has everything deletable, which is Kafka's rule.
    #[tokio::test]
    async fn a_group_that_is_not_a_consumer_group_has_everything_deleted() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "od5-g", "orders", &[0]).await;
        // The metadata NAMES the topic; the protocol type is what decides.
        join_subscribed(&f, "od5-g", &["orders"], "connect").await;

        let resp = handle(&f, &request("od5-g", &[("orders", &[0])]), None).await;
        assert_eq!(code(&resp, "orders", 0), 0);
    }

    /// A group nobody has heard of is GROUP_ID_NOT_FOUND at the TOP level, with
    /// no partitions in the answer — Kafka's own shape.
    #[tokio::test]
    async fn an_unknown_group_is_group_id_not_found_at_the_top() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &request("od6-nobody", &[("orders", &[0])]), None).await;
        assert_eq!(resp.error_code, ResponseError::GroupIdNotFound.code());
        assert!(resp.topics.is_empty());
    }

    /// The `applied: false` trap. Deleting an offset that was never committed
    /// is error 0, because that is what Kafka answers — getting this wrong
    /// turns `--delete-offsets` on a fresh group into a spurious failure.
    #[tokio::test]
    async fn deleting_an_offset_that_was_never_committed_is_error_zero() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        // The group EXISTS (it committed partition 0) but partition 3 never did.
        commit(&f, "od7-g", "orders", &[0]).await;
        let resp = handle(&f, &request("od7-g", &[("orders", &[3])]), None).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(code(&resp, "orders", 3), 0);

        // ...and so is a second delete of one that was.
        handle(&f, &request("od7-g", &[("orders", &[0])]), None).await;
        let again = handle(&f, &request("od7-g", &[("orders", &[0])]), None).await;
        assert_eq!(code(&again, "orders", 0), 0);
    }

    /// A `__` name, an illegal name and a negative index are all the one code
    /// this API may answer, and none of them stops its neighbours.
    #[tokio::test]
    async fn bad_names_and_negative_indexes_are_unknown_topic() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "od8-g", "orders", &[0]).await;
        let resp = handle(
            &f,
            &request(
                "od8-g",
                &[
                    ("__consumer_offsets", &[0]),
                    ("bad name", &[0]),
                    ("orders", &[-1, 0]),
                ],
            ),
            None,
        )
        .await;
        let want = ResponseError::UnknownTopicOrPartition.code();
        assert_eq!(code(&resp, "__consumer_offsets", 0), want);
        assert_eq!(code(&resp, "bad name", 0), want);
        assert_eq!(code(&resp, "orders", -1), want);
        assert_eq!(code(&resp, "orders", 0), 0, "a neighbour was refused too");
    }

    /// The deviation, pinned: OffsetDelete removes offsets and NOT the group.
    /// After every one of a group's offsets is deleted, its index row is still
    /// there and ListGroups still lists it.
    #[tokio::test]
    async fn the_group_index_row_survives_and_the_group_still_lists() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "od9-g", "orders", &[0, 1]).await;

        let resp = handle(&f, &request("od9-g", &[("orders", &[0, 1])]), None).await;
        assert_eq!(code(&resp, "orders", 0), 0);
        assert_eq!(code(&resp, "orders", 1), 0);

        assert!(
            api.kv_get(offsets::NAMESPACE, &offsets::index_key("od9-g").unwrap())
                .is_some(),
            "OffsetDelete removed the group's existence row"
        );
        let listed =
            crate::handlers::list_groups::handle(&f, &ListGroupsRequest::default(), 0, None).await;
        assert!(
            listed
                .groups
                .iter()
                .any(|g| g.group_id.0.as_str() == "od9-g"),
            "the group stopped listing: {:?}",
            listed.groups
        );
    }

    /// More partitions than one request deletes: refused at the top level, and
    /// nothing is written.
    #[tokio::test]
    async fn more_than_the_cap_is_invalid_request() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let partitions: Vec<i32> = (0..=MAX_DELETED_PARTITIONS as i32).collect();
        let resp = handle(&f, &request("od10-g", &[("orders", &partitions)]), None).await;
        assert_eq!(resp.error_code, ResponseError::InvalidRequest.code());
        assert!(resp.topics.is_empty());
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_illegal_group_id_is_refused_before_anything_is_read() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let huge = "g".repeat(coordinator::MAX_GROUP_ID_CHARS + 1);
        let resp = handle(&f, &request(&huge, &[("orders", &[0])]), None).await;
        assert_eq!(resp.error_code, ResponseError::InvalidGroupId.code());
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    /// A store that fails while the handler is still deciding whether the group
    /// EXISTS is a top-level refusal: nothing has been decided per partition
    /// yet, and answering 0 for the partitions would report a delete that never
    /// happened. Authorization is the one code here that is not retriable, and
    /// it must be reportable by name.
    #[tokio::test]
    async fn a_failed_existence_read_is_answered_at_the_top_level() {
        for (queen_error, kafka) in [
            (
                Error::Transport("connection refused".into()),
                ResponseError::CoordinatorNotAvailable,
            ),
            (
                Error::status(403, "forbidden"),
                ResponseError::GroupAuthorizationFailed,
            ),
        ] {
            let (f, api) = facade_and_queen(&[("orders", 4)]);
            let group = format!("od11-{}", kafka.code());
            // A simple consumer's commit leaves NO actor, so the existence
            // check is the durable index read — and that is the call this
            // failure lands on.
            commit(&f, &group, "orders", &[0]).await;
            api.fail_kv(queen_error.clone());
            let resp = handle(&f, &request(&group, &[("orders", &[0])]), None).await;
            assert_eq!(resp.error_code, kafka.code(), "{queen_error}");
            assert!(resp.topics.is_empty(), "{queen_error}");
            assert!(
                api.kv_get(
                    offsets::NAMESPACE,
                    &offsets::key(&group, "orders", 0).unwrap()
                )
                .is_some(),
                "{queen_error}: a refused delete removed the offset anyway"
            );
        }
    }

    /// ...and a store that fails during the DELETE is a retriable code per
    /// partition, never a reported success: a client told an offset was deleted
    /// when it was not would stop retrying.
    #[tokio::test]
    async fn a_failed_delete_is_never_reported_as_deleted() {
        let (f, api) = facade_and_queen(&[("orders", 4), ("clicks", 4)]);
        commit(&f, "od12-g", "orders", &[0]).await;
        // A live member subscribed to a DIFFERENT topic: the group has an actor,
        // so the existence check costs no call and the next one out is the
        // delete itself.
        join_subscribed(&f, "od12-g", &["clicks"], CONSUMER_PROTOCOL).await;
        api.fail_kv(Error::Transport("connection refused".into()));

        let resp = handle(&f, &request("od12-g", &[("orders", &[0])]), None).await;
        assert_eq!(resp.error_code, 0, "refused at the top instead");
        assert_eq!(
            code(&resp, "orders", 0),
            ResponseError::CoordinatorNotAvailable.code()
        );
        assert!(
            api.kv_get(
                offsets::NAMESPACE,
                &offsets::key("od12-g", "orders", 0).unwrap()
            )
            .is_some(),
            "a failed delete removed the offset anyway"
        );
    }

    /// Cluster mode: a non-owner refuses at the top level and touches nothing —
    /// it must not even read the group's membership to decide the subscription
    /// rule.
    #[tokio::test]
    async fn a_non_owner_refuses_at_the_top_level_and_writes_nothing() {
        // The pinned rendezvous table puts `orders-consumer` on node 2.
        let (stranger, api) = clustered(&[("orders", 4)], &THREE, 1);
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::key("orders-consumer", "orders", 0).unwrap(),
            serde_json::json!({"offset": 50, "metadata": "", "ts": 1}),
        );

        let resp = handle(
            &stranger,
            &request("orders-consumer", &[("orders", &[0])]),
            None,
        )
        .await;
        assert_eq!(resp.error_code, ResponseError::NotCoordinator.code());
        assert!(resp.topics.is_empty());
        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "a refused delete reached Queen"
        );
        assert!(api
            .kv_get(
                offsets::NAMESPACE,
                &offsets::key("orders-consumer", "orders", 0).unwrap()
            )
            .is_some());
    }

    /// ...and the owner carries the fence: operation 0 of the delete batch is
    /// the group's fence key, `required`.
    #[tokio::test]
    async fn the_owner_carries_the_fence() {
        let (owner, api) = clustered(&[("orders", 4)], &THREE, 2);
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::index_key("orders-consumer").unwrap(),
            serde_json::json!({"pt": "consumer", "ts": 1}),
        );
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::key("orders-consumer", "orders", 0).unwrap(),
            serde_json::json!({"offset": 50, "metadata": "", "ts": 1}),
        );
        api.kv_calls.lock().unwrap().clear();

        let resp = handle(
            &owner,
            &request("orders-consumer", &[("orders", &[0])]),
            None,
        )
        .await;
        assert_eq!(code(&resp, "orders", 0), 0);

        let calls = api.kv_calls.lock().unwrap().clone();
        let batch = calls
            .iter()
            .find(|call| call.iter().any(|op| matches!(op, KvOp::Delete { .. })))
            .expect("no delete batch was sent");
        match &batch[0] {
            KvOp::Put { key, required, .. } => {
                assert_eq!(key, &offsets::fence_key("orders-consumer").unwrap());
                assert!(*required, "the fence must abort the transaction it loses");
            }
            other => panic!("the delete batch went out without a fence: {other:?}"),
        }
        assert!(api
            .kv_get(
                offsets::NAMESPACE,
                &offsets::key("orders-consumer", "orders", 0).unwrap()
            )
            .is_none());
    }

    /// A fence another node left behind is TAKEN OVER, not tripped over: the
    /// ownership guard has already established that this node is the rendezvous
    /// owner under a view that is not stale, so the delete expects the version
    /// it read and lands. That is the same verdict the commit path's one retry
    /// reaches for the same situation.
    ///
    /// The case this does NOT cover — the fence moving BETWEEN the read and the
    /// write — is the one `offsets::delete_offsets` covers directly, because
    /// only there can a stale `expect` be handed in on purpose.
    #[tokio::test]
    async fn a_fence_another_node_left_behind_is_taken_over() {
        let (owner, api) = clustered(&[("orders", 4)], &THREE, 2);
        let key = offsets::key("orders-consumer", "orders", 0).unwrap();
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::index_key("orders-consumer").unwrap(),
            serde_json::json!({"pt": "consumer", "ts": 1}),
        );
        api.kv_seed(offsets::NAMESPACE, &key, serde_json::json!({"offset": 50}));
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::fence_key("orders-consumer").unwrap(),
            serde_json::json!({"node": 3, "incarnation": "elsewhere"}),
        );

        let resp = handle(
            &owner,
            &request("orders-consumer", &[("orders", &[0])]),
            None,
        )
        .await;
        assert_eq!(code(&resp, "orders", 0), 0);
        assert!(api.kv_get(offsets::NAMESPACE, &key).is_none());
        let fence = api
            .kv_get(
                offsets::NAMESPACE,
                &offsets::fence_key("orders-consumer").unwrap(),
            )
            .expect("the fence is written");
        assert_eq!(fence["node"], 2, "the owner did not take the fence over");
    }

    #[tokio::test]
    async fn the_connections_credential_is_what_deletes() {
        let (root, api) = facade_and_queen(&[("orders", 4)]);
        let f = root.for_connection(None).authenticated_as("tenant-key");
        commit(&f, "od15-g", "orders", &[0]).await;
        api.tokens.lock().unwrap().clear();
        handle(
            &f,
            &request("od15-g", &[("orders", &[0])]),
            Some("tenant-key"),
        )
        .await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(
            tokens.iter().all(|t| t.as_deref() == Some("tenant-key")),
            "a delete went out under the wrong credential: {tokens:?}"
        );
    }

    /// One version, and it round-trips. There is nothing else to walk.
    #[tokio::test]
    async fn the_exchange_round_trips_at_the_one_advertised_version() {
        use kafka_protocol::protocol::Encodable as _;

        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::OffsetDelete as i16)
            .expect("OffsetDelete is advertised");
        assert_eq!((row.min, row.max), (0, 0));
        assert!(
            row.max <= <OffsetDeleteRequest as Message>::VERSIONS.max,
            "advertised past the request schema"
        );

        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "od13-g", "orders", &[0]).await;

        let mut wire = BytesMut::new();
        request("od13-g", &[("orders", &[0])])
            .encode(&mut wire, 0)
            .expect("encode request");
        let mut buf = wire.freeze();
        let decoded = OffsetDeleteRequest::decode(&mut buf, 0).expect("decode request");
        assert!(buf.is_empty(), "trailing request bytes");

        let resp = handle(&f, &decoded, None).await;
        let mut wire = BytesMut::new();
        resp.encode(&mut wire, 0).expect("encode response");
        let mut buf = wire.freeze();
        let back = OffsetDeleteResponse::decode(&mut buf, 0).expect("decode response");
        assert!(buf.is_empty(), "trailing response bytes");
        assert_eq!(back.error_code, 0);
        assert_eq!(code(&back, "orders", 0), 0);
    }

    /// The subscription decoder, on its own: every version of the consumer
    /// protocol this build can be sent, and the shapes that are not one.
    #[test]
    fn the_subscription_decoder_reads_every_version_and_refuses_the_rest() {
        for version in 0..=ConsumerProtocolSubscription::VERSIONS.max {
            assert_eq!(
                subscribed_topics(&subscription_bytes(&["a", "b"], version)),
                Some(vec!["a".to_string(), "b".to_string()]),
                "v{version}"
            );
        }
        // A version from a client newer than this build is CLAMPED, not
        // refused, exactly as Kafka's own reader clamps it.
        let mut newer = BytesMut::new();
        newer.extend_from_slice(&9i16.to_be_bytes());
        newer.extend_from_slice(&subscription_bytes(&["a"], 3)[2..]);
        assert_eq!(
            subscribed_topics(&newer.freeze()),
            Some(vec!["a".to_string()])
        );

        assert_eq!(subscribed_topics(&Bytes::new()), None);
        assert_eq!(subscribed_topics(&Bytes::from_static(&[0x00])), None);
        assert_eq!(
            subscribed_topics(&Bytes::from_static(&[0xff, 0xff, 0x00, 0x00])),
            None
        );
    }

    /// The keys this API composes are the SAME keys the commit path stores, and
    /// nothing else: a delete that composed a different key would report
    /// success and remove nothing.
    #[tokio::test]
    async fn the_deleted_keys_are_exactly_the_commit_paths_keys() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        for p in 0..3 {
            let pair = (
                offsets::key("od14-g", "orders", p).unwrap(),
                Committed {
                    offset: 1,
                    metadata: String::new(),
                    ts: 1,
                },
            );
            offsets::store(api.as_ref(), &[pair], None, None).await;
        }
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::index_key("od14-g").unwrap(),
            serde_json::json!({"pt": "", "ts": 1}),
        );
        api.kv_calls.lock().unwrap().clear();

        handle(&f, &request("od14-g", &[("orders", &[0, 1, 2])]), None).await;
        let deleted: Vec<String> = api
            .kv_calls
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .filter_map(|op| match op {
                KvOp::Delete { key, .. } => Some(key.clone()),
                _ => None,
            })
            .collect();
        assert_eq!(
            deleted,
            (0..3)
                .map(|p| offsets::key("od14-g", "orders", p).unwrap())
                .collect::<Vec<String>>()
        );
    }
}
