//! OffsetCommit — where a group records how far it has read.
//!
//! It is the one request in the milestone that WRITES something durable, and
//! the durability is Queen's: each (group, topic, partition) is one key in the
//! key/value store ([`crate::offsets`]), and a commit is a batch of upserts.
//! Membership is checked first — a member of a generation that has ended must
//! not overwrite the progress of the one that replaced it — and that check is
//! the coordinator's ([`crate::coordinator::Coordinator::check_commit`]),
//! including its one deliberate hole: a SIMPLE CONSUMER, which manages no
//! membership and commits with generation -1 and an empty member id.
//!
//! ## Every refusal is per partition
//!
//! OffsetCommit has no top-level error field: the response is one error code
//! per partition, and a client reads them individually. So a group-wide refusal
//! is written into every partition of the response rather than reported once,
//! which is also what Apache Kafka does with the same situation.
//!
//! ## What is refused before Queen is asked
//!
//! A topic name Metadata would not show (`__`-prefixed, or not a legal Kafka
//! name) — the same rule as `handlers::fetch` and `handlers::list_offsets`, in
//! the same one code ([`metadata::not_a_topic_here`]), so a topic that is
//! invisible on the read path cannot become visible by having an offset
//! committed against it. A negative partition index, which is not a
//! lane. A negative offset other than -1, which is Kafka's own "no offset" and
//! the only negative that means anything. Metadata past
//! [`crate::offsets::MAX_METADATA_BYTES`], which is Kafka's
//! `offset.metadata.max.bytes` and its own error code. And a (group, topic)
//! pair whose composed key is longer than the store's key column
//! ([`crate::offsets::key`]) — INVALID_COMMIT_OFFSET_SIZE, because a commit
//! this facade cannot store must fail loudly rather than read back later as
//! "never committed".

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::{OffsetCommitRequest, OffsetCommitResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::handlers::metadata;
use crate::offsets::{self, Committed};
use crate::Facade;

/// Kafka's "no offset": the only negative a client may commit.
const NO_OFFSET: i64 = -1;

/// What one requested partition resolved to.
enum Slot {
    /// Entry `index` of the batched write is this partition's.
    Write(usize),
    /// Answer this error, and write nothing.
    Reject(ResponseError),
}

/// Handle one OffsetCommit.
pub async fn handle(
    facade: &Facade,
    req: &OffsetCommitRequest,
    token: Option<&str>,
) -> OffsetCommitResponse {
    let group = req.group_id.0.as_str();
    if let Some(e) = crate::coordinator::invalid_group_id(group) {
        return refuse_all(req, e);
    }
    // The fence, before anything is written. `generation_id_or_member_epoch` is
    // the generation at every version this facade advertises — the "member
    // epoch" half of that name belongs to the KIP-848 protocol, which is out of
    // scope and unreachable here.
    if let Some(e) = facade
        .coordinator
        .check_commit(
            group,
            req.member_id.as_str(),
            req.generation_id_or_member_epoch,
        )
        .await
    {
        tracing::debug!(
            target: "kafka",
            group,
            member = req.member_id.as_str(),
            generation = req.generation_id_or_member_epoch,
            error = ?e,
            "offset commit refused"
        );
        return refuse_all(req, e);
    }

    // `retention_time_ms` (v2..=v4) is accepted and not acted on, and that is
    // the honest shape of this store rather than an oversight: offsets here
    // never expire (see `crate::offsets`), so there is no retention for a
    // client to shorten. Ignoring it can only keep offsets LONGER than asked,
    // which is the safe direction — the unsafe one would be a group resuming
    // from `auto.offset.reset` because a number in a request expired its
    // progress.
    let mut writes: Vec<(String, Committed)> = Vec::new();
    let mut slots: Vec<Vec<Slot>> = Vec::with_capacity(req.topics.len());
    let now = now_millis();
    for topic in &req.topics {
        let name = topic.name.0.as_str();
        slots.push(
            topic
                .partitions
                .iter()
                .map(|p| {
                    stage(
                        &mut writes,
                        group,
                        name,
                        p.partition_index,
                        p.committed_offset,
                        p.committed_metadata.as_ref().map(|m| m.as_str()),
                        now,
                    )
                })
                .collect(),
        );
    }

    let stored = offsets::store(facade.queen.as_ref(), &writes, token).await;
    render(req, &slots, &stored)
}

/// Resolve one requested partition into a write or a refusal.
fn stage(
    writes: &mut Vec<(String, Committed)>,
    group: &str,
    topic: &str,
    partition: i32,
    offset: i64,
    metadata: Option<&str>,
    now: i64,
) -> Slot {
    if let Some(e) = metadata::not_a_topic_here(topic) {
        return Slot::Reject(e);
    }
    if partition < 0 {
        return Slot::Reject(ResponseError::UnknownTopicOrPartition);
    }
    if offset < NO_OFFSET {
        return Slot::Reject(ResponseError::InvalidCommitOffsetSize);
    }
    let metadata = metadata.unwrap_or_default();
    if metadata.len() > offsets::MAX_METADATA_BYTES {
        return Slot::Reject(ResponseError::OffsetMetadataTooLarge);
    }
    let Some(key) = offsets::key(group, topic, partition) else {
        return Slot::Reject(ResponseError::InvalidCommitOffsetSize);
    };
    writes.push((
        key,
        Committed {
            offset,
            metadata: metadata.to_string(),
            ts: now,
        },
    ));
    Slot::Write(writes.len() - 1)
}

/// Wall-clock milliseconds. Not tokio's clock: this is a timestamp that is
/// STORED and read by a person, and a paused test clock would write 1970 into
/// the database.
fn now_millis() -> i64 {
    std::time::UNIX_EPOCH
        .elapsed()
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}

fn render(
    req: &OffsetCommitRequest,
    slots: &[Vec<Slot>],
    stored: &[crate::queen::Result<()>],
) -> OffsetCommitResponse {
    let mut topics = Vec::with_capacity(req.topics.len());
    for (topic, row) in req.topics.iter().zip(slots) {
        let name = topic.name.0.as_str();
        let mut failed: Option<(i32, String)> = None;
        let partitions = topic
            .partitions
            .iter()
            .zip(row)
            .map(|(p, slot)| {
                let error = match slot {
                    Slot::Reject(e) => Some(*e),
                    // `get` and not an index: a handler must not panic on
                    // anything, including a store that answered short.
                    Slot::Write(i) => match stored.get(*i) {
                        Some(Ok(())) => None,
                        Some(Err(e)) => {
                            failed.get_or_insert_with(|| (p.partition_index, e.to_string()));
                            Some(offsets::kafka_error(e))
                        }
                        None => Some(ResponseError::UnknownServerError),
                    },
                };
                answer(p.partition_index, error)
            })
            .collect();
        if let Some((partition, error)) = &failed {
            tracing::warn!(
                target: "kafka",
                group = req.group_id.0.as_str(),
                topic = name,
                partition,
                %error,
                "offset commit failed"
            );
        }
        topics.push(
            OffsetCommitResponseTopic::default()
                .with_name(TopicName(StrBytes::from_string(name.to_string())))
                .with_partitions(partitions),
        );
    }
    OffsetCommitResponse::default().with_topics(topics)
}

/// Answer every partition of the request with one error. See the module header.
fn refuse_all(req: &OffsetCommitRequest, error: ResponseError) -> OffsetCommitResponse {
    OffsetCommitResponse::default().with_topics(
        req.topics
            .iter()
            .map(|topic| {
                OffsetCommitResponseTopic::default()
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

fn answer(partition: i32, error: Option<ResponseError>) -> OffsetCommitResponsePartition {
    OffsetCommitResponsePartition::default()
        .with_partition_index(partition)
        .with_error_code(error.map_or(0, |e| e.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::NO_GENERATION;
    use crate::handlers::testing::facade_and_queen;
    use crate::queen::testing::FakeQueen;
    use crate::queen::Error;
    use crate::Facade;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{GroupId, JoinGroupRequest};
    use kafka_protocol::protocol::{Decodable, Encodable, Message};
    use std::sync::Arc;

    /// One partition of a fixture commit: index, offset, metadata.
    type Commit<'a> = (i32, i64, &'a str);

    /// `[(topic, [(partition, offset, metadata)])]` → a commit from a simple
    /// consumer (no membership).
    fn request(group: &str, topics: &[(&str, &[Commit<'_>])]) -> OffsetCommitRequest {
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(NO_GENERATION)
            .with_topics(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        OffsetCommitRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partitions(
                                partitions
                                    .iter()
                                    .map(|(index, offset, metadata)| {
                                        OffsetCommitRequestPartition::default()
                                            .with_partition_index(*index)
                                            .with_committed_offset(*offset)
                                            .with_committed_metadata(Some(StrBytes::from_string(
                                                metadata.to_string(),
                                            )))
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn code(resp: &OffsetCommitResponse, topic: &str, partition: i32) -> i16 {
        resp.topics
            .iter()
            .find(|t| t.name.0.as_str() == topic)
            .unwrap_or_else(|| panic!("{topic} is not in the response"))
            .partitions
            .iter()
            .find(|p| p.partition_index == partition)
            .unwrap_or_else(|| panic!("{topic}/{partition} is not in the response"))
            .error_code
    }

    async fn one_member_group(f: &Facade, group: &str) -> (String, i32) {
        use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::new())]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c").await;
        let joined = crate::handlers::join_group::handle(
            f,
            &req.clone().with_member_id(minted.member_id),
            4,
            "c",
        )
        .await;
        (joined.member_id.to_string(), joined.generation_id)
    }

    // ---------------------------------------------------------------- writing

    /// A simple consumer's commit lands in Queen, at the documented key, with
    /// the client's own metadata beside the offset.
    #[tokio::test]
    async fn a_commit_writes_one_key_per_partition() {
        let (f, api) = facade_and_queen(&[]);
        let resp = handle(
            &f,
            &request("g", &[("orders", &[(0, 41, "batch-7"), (3, 12, "")])]),
            None,
        )
        .await;
        assert_eq!(code(&resp, "orders", 0), 0);
        assert_eq!(code(&resp, "orders", 3), 0);

        let stored = api
            .kv_get(offsets::NAMESPACE, "qk:group:g:orders:0")
            .expect("nothing was written for partition 0");
        assert_eq!(stored["offset"], 41);
        assert_eq!(stored["metadata"], "batch-7");
        assert!(stored["ts"].as_i64().is_some_and(|ts| ts > 0));
        assert_eq!(
            api.kv_get(offsets::NAMESPACE, "qk:group:g:orders:3")
                .unwrap()["offset"],
            12
        );
    }

    /// Every partition of one commit is ONE call to Queen: a consumer commits
    /// its whole assignment on a timer, and a call per partition would be a
    /// round trip per lane per interval.
    #[tokio::test]
    async fn a_whole_commit_is_one_call() {
        let (f, api) = facade_and_queen(&[]);
        let partitions: Vec<(i32, i64, &str)> = (0..64).map(|p| (p, i64::from(p), "")).collect();
        handle(
            &f,
            &request(
                "g",
                &[("orders", &partitions), ("clicks", &partitions[..8])],
            ),
            None,
        )
        .await;
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);
        assert_eq!(api.kv_ops().len(), 72);
    }

    // ------------------------------------------------------------- the fence

    /// A member of a generation that ended must not write over the one that
    /// replaced it — and nothing reaches Queen when it tries.
    #[tokio::test]
    async fn a_stale_generation_is_refused_and_writes_nothing() {
        let (f, api) = facade_and_queen(&[]);
        let (member, generation) = one_member_group(&f, "g").await;

        let stale = request("g", &[("orders", &[(0, 41, "")])])
            .with_member_id(StrBytes::from_string(member.clone()))
            .with_generation_id_or_member_epoch(generation - 1);
        let resp = handle(&f, &stale, None).await;
        assert_eq!(
            code(&resp, "orders", 0),
            ResponseError::IllegalGeneration.code()
        );

        let stranger = request("g", &[("orders", &[(0, 41, "")])])
            .with_member_id(StrBytes::from_static_str("nobody"))
            .with_generation_id_or_member_epoch(generation);
        let resp = handle(&f, &stranger, None).await;
        assert_eq!(
            code(&resp, "orders", 0),
            ResponseError::UnknownMemberId.code()
        );

        // A simple-consumer commit underneath a live group is the same refusal.
        let simple = request("g", &[("orders", &[(0, 41, "")])]);
        let resp = handle(&f, &simple, None).await;
        assert_eq!(
            code(&resp, "orders", 0),
            ResponseError::UnknownMemberId.code()
        );

        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "a refused commit reached Queen"
        );
    }

    /// The member of the current generation commits.
    #[tokio::test]
    async fn a_member_of_the_current_generation_commits() {
        let (f, api) = facade_and_queen(&[]);
        let (member, generation) = one_member_group(&f, "g").await;
        let req = request("g", &[("orders", &[(0, 41, "")])])
            .with_member_id(StrBytes::from_string(member))
            .with_generation_id_or_member_epoch(generation);
        let resp = handle(&f, &req, None).await;
        assert_eq!(code(&resp, "orders", 0), 0);
        assert!(api
            .kv_get(offsets::NAMESPACE, "qk:group:g:orders:0")
            .is_some());
    }

    /// A simple consumer — `assign()`, `kafka-console-consumer --group`, an
    /// offset tool — commits without ever joining.
    #[tokio::test]
    async fn a_simple_consumer_commits_without_joining() {
        let (f, api) = facade_and_queen(&[]);
        let resp = handle(&f, &request("g", &[("orders", &[(0, 7, "")])]), None).await;
        assert_eq!(code(&resp, "orders", 0), 0);
        assert!(api
            .kv_get(offsets::NAMESPACE, "qk:group:g:orders:0")
            .is_some());
        assert_eq!(f.coordinator.live_groups(), 0, "a group was conjured");
    }

    #[tokio::test]
    async fn an_empty_group_id_is_refused_for_every_partition() {
        let (f, api) = facade_and_queen(&[]);
        let resp = handle(
            &f,
            &request("", &[("orders", &[(0, 7, ""), (1, 8, "")])]),
            None,
        )
        .await;
        for partition in [0, 1] {
            assert_eq!(
                code(&resp, "orders", partition),
                ResponseError::InvalidGroupId.code()
            );
        }
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    // ------------------------------------------------------------- refusals

    /// The shapes refused before Queen is asked, each with the code a client
    /// acts on — and the partitions beside them still commit.
    #[tokio::test]
    async fn unstorable_partitions_are_refused_one_by_one() {
        let (f, api) = facade_and_queen(&[]);
        let long_metadata = "m".repeat(offsets::MAX_METADATA_BYTES + 1);
        let mut req = request(
            "g",
            &[
                ("orders", &[(0, 41, "")]),
                // Reserved and unnameable topics, the same rule the read path
                // applies.
                ("__consumer_offsets", &[(0, 1, "")]),
                ("not a topic", &[(0, 1, "")]),
                // A partition index that is not one, and a negative offset that
                // is not Kafka's -1.
                ("clicks", &[(-1, 1, ""), (0, -2, "")]),
            ],
        );
        req.topics.push(
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("big")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(1)
                    .with_committed_metadata(Some(StrBytes::from_string(long_metadata)))]),
        );

        let resp = handle(&f, &req, None).await;
        assert_eq!(code(&resp, "orders", 0), 0, "a good partition was refused");
        assert_eq!(
            code(&resp, "__consumer_offsets", 0),
            ResponseError::UnknownTopicOrPartition.code()
        );
        // UNKNOWN and not INVALID_TOPIC_EXCEPTION, which Metadata answers for
        // the same name: the Java consumer's commit path raises a bare
        // `KafkaException` for a code outside the set it knows, and
        // INVALID_TOPIC_EXCEPTION is outside it. See
        // [`metadata::not_a_topic_here`].
        assert_eq!(
            code(&resp, "not a topic", 0),
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(
            code(&resp, "clicks", -1),
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(
            code(&resp, "clicks", 0),
            ResponseError::InvalidCommitOffsetSize.code()
        );
        assert_eq!(
            code(&resp, "big", 0),
            ResponseError::OffsetMetadataTooLarge.code()
        );
        // Only the one good partition was written.
        assert_eq!(api.kv_ops().len(), 1);
    }

    /// Kafka's own -1 is a legal commit — it is how a client says "no offset" —
    /// and it is stored rather than refused.
    #[tokio::test]
    async fn the_no_offset_sentinel_is_a_legal_commit() {
        let (f, api) = facade_and_queen(&[]);
        let resp = handle(&f, &request("g", &[("orders", &[(0, -1, "")])]), None).await;
        assert_eq!(code(&resp, "orders", 0), 0);
        assert_eq!(
            api.kv_get(offsets::NAMESPACE, "qk:group:g:orders:0")
                .unwrap()["offset"],
            -1
        );
    }

    /// A (group, topic) pair long enough that the composed key would not fit is
    /// refused, not truncated: a commit stored under a shortened key reads back
    /// as never committed.
    ///
    /// Both halves are at their own legal maximum — the longest group id this
    /// facade accepts and the longest topic name Kafka does — because that is
    /// the only way left to exceed the key column now that a group id is
    /// bounded on the way in.
    #[tokio::test]
    async fn a_key_that_cannot_be_stored_is_refused() {
        let (f, api) = facade_and_queen(&[]);
        let group = "g".repeat(crate::coordinator::MAX_GROUP_ID_CHARS);
        let topic = "t".repeat(249);
        let resp = handle(&f, &request(&group, &[(&topic, &[(0, 1, "")])]), None).await;
        assert_eq!(
            code(&resp, &topic, 0),
            ResponseError::InvalidCommitOffsetSize.code()
        );
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    /// ...and a group id past the bound is refused for every partition, with
    /// the code a client treats as a configuration error rather than retrying.
    #[tokio::test]
    async fn a_group_id_past_the_bound_is_refused_for_every_partition() {
        let (f, api) = facade_and_queen(&[]);
        let group = "g".repeat(crate::coordinator::MAX_GROUP_ID_CHARS + 1);
        let resp = handle(
            &f,
            &request(&group, &[("orders", &[(0, 7, ""), (1, 8, "")])]),
            None,
        )
        .await;
        for partition in [0, 1] {
            assert_eq!(
                code(&resp, "orders", partition),
                ResponseError::InvalidGroupId.code()
            );
        }
        assert!(api.kv_calls.lock().unwrap().is_empty());
        assert_eq!(f.coordinator.live_groups(), 0, "a group was conjured");
    }

    /// A store that fails is reported as a code the client retries — never as
    /// a successful commit.
    #[tokio::test]
    async fn a_failed_write_is_never_reported_as_committed() {
        for (queen_error, kafka) in [
            (
                Error::Transport("connection refused".into()),
                ResponseError::CoordinatorNotAvailable,
            ),
            (
                Error::status(403, "forbidden"),
                ResponseError::GroupAuthorizationFailed,
            ),
            (
                Error::status(500, "boom"),
                ResponseError::UnknownServerError,
            ),
        ] {
            let api = FakeQueen::with(&[]);
            api.fail_kv(queen_error.clone());
            let f = crate::handlers::testing::over(
                Arc::clone(&api) as Arc<dyn crate::queen::QueenApi>,
                Default::default(),
            );
            let resp = handle(&f, &request("g", &[("orders", &[(0, 41, "")])]), None).await;
            assert_eq!(code(&resp, "orders", 0), kafka.code(), "{queen_error}");
        }
    }

    #[tokio::test]
    async fn the_token_reaches_the_store() {
        let (f, api) = facade_and_queen(&[]);
        handle(
            &f,
            &request("g", &[("orders", &[(0, 41, "")])]),
            Some("tenant-a"),
        )
        .await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }

    // -------------------------------------------------------------- the wire

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::OffsetCommit as i16)
            .expect("OffsetCommit is advertised");
        assert!(
            row.min >= OffsetCommitRequest::VERSIONS.min
                && row.max <= OffsetCommitRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let (f, api) = facade_and_queen(&[]);
            let mut wire = BytesMut::new();
            request("g", &[("orders", &[(0, 41, "batch-7")])])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = OffsetCommitRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = OffsetCommitResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            assert_eq!(code(&back, "orders", 0), 0, "v{version}");
            assert_eq!(
                api.kv_get(offsets::NAMESPACE, "qk:group:g:orders:0")
                    .unwrap()["offset"],
                41,
                "v{version}"
            );
        }
    }
}
