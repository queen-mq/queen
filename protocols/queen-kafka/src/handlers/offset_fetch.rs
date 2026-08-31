//! OffsetFetch — where a group resumes.
//!
//! Every consumer sends it once per assignment, right after SyncGroup, and the
//! answer decides where it starts reading. There are two forms and both are
//! implemented:
//!
//!   * NAMED topics — the one a consumer sends for the partitions it was just
//!     assigned. One `getMany` per chunk of keys ([`crate::offsets::load`]).
//!   * ALL topics, i.e. a null topics array — what `kafka-consumer-groups
//!     --describe` and an admin `listConsumerGroupOffsets()` send. It is
//!     answerable because the KV surface has `getPrefix` with a keyset cursor,
//!     and the key layout puts the group first
//!     ([`crate::offsets::load_group`]). Without either of those it would have
//!     had to be an error.
//!
//! ## Missing is not an error, and that is the whole contract
//!
//! A partition with no committed offset is answered `-1` with error code 0.
//! That is not a shrug: `-1` is precisely what makes a client apply
//! `auto.offset.reset` (earliest or latest, its choice), which is the correct
//! and only behaviour for a group that has never committed. Answering an ERROR
//! instead would leave a brand-new consumer group unable to start, and
//! answering `0` would silently replay a topic from its beginning for a group
//! that meant to start at the end.
//!
//! ## An unreadable answer is refused, not guessed
//!
//! The one thing worse than an error here is a WRONG -1. If the store cannot be
//! read, or answers without covering every key asked for
//! ([`crate::offsets::Loaded::Unread`]), the whole request is refused with a
//! retriable code and the client asks again. Reporting those partitions as
//! "never committed" would reset a consumer that had committed perfectly well.
//!
//! ## ...and every refusal is the GROUP's, never a partition's
//!
//! No partition of this response ever carries an error code. That is Apache
//! Kafka's own shape — the broker looks each `(topic, partition)` up in the
//! group's committed offsets and answers `-1` with error 0 for every one it does
//! not hold, whatever the name looks like and whether or not the topic exists —
//! and every client is built on it: the Java consumer raises a `KafkaException`
//! out of `poll()` for ANY per-partition code here, so a partition-level answer
//! this facade invented would end a consumer that a real broker would have
//! started. The refusals this handler does have are all group-level, where the
//! client's own retry loop is waiting for them. See `compat/ERRORS.md`.
//!
//! ## There is deliberately NO cluster-mode ownership guard here
//!
//! The other five group-addressed APIs — Join, Sync, Heartbeat, Leave and
//! OffsetCommit — answer NOT_COORDINATOR at a node that does not own the group
//! ([`crate::cluster`]). This one does not, and the omission is a decision, not
//! an oversight, so nobody adds one for symmetry:
//!
//!   * it is a READ of shared state. The offsets are rows in `queen.kv`
//!     (024_kv.sql), so every node answers the same thing; there is no second
//!     copy for a redirect to protect.
//!   * refusing it would break the `assign()`-based simple consumer, which
//!     never runs FindCoordinator and may hold a connection to any node. It
//!     would be told to go somewhere it has no way of finding.
//!
//! The commit path is where the ownership matters, because a commit can rewind
//! what another node wrote, and that is exactly where the guard and the fence
//! both are.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponsePartition, OffsetFetchResponseTopic,
};
use kafka_protocol::messages::{OffsetFetchRequest, OffsetFetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::handlers::metadata;
use crate::offsets::{self, Loaded};
use crate::Facade;

/// Kafka's "this group has not committed here".
const NO_OFFSET: i64 = -1;

/// The leader epoch of a committed offset. -1 everywhere: the facade holds no
/// elections to number, the same answer Metadata and ListOffsets give.
const NO_EPOCH: i32 = -1;

/// What one requested partition resolved to.
enum Slot {
    /// Entry `index` of the batched read is this partition's.
    Read(usize),
    /// Answerable without reading: a key this facade could never have written.
    NoOffset,
}

/// Handle one OffsetFetch.
pub async fn handle(
    facade: &Facade,
    req: &OffsetFetchRequest,
    token: Option<&str>,
) -> OffsetFetchResponse {
    let group = req.group_id.0.as_str();
    if let Some(e) = crate::coordinator::invalid_group_id(group) {
        return refuse_all(req, e);
    }
    // `require_stable` (v7) asks the broker to withhold offsets belonging to an
    // open transaction. There are none — `handlers::produce` refuses every
    // shape of transaction — so every offset here is stable and the flag is
    // satisfied by construction rather than ignored.
    match &req.topics {
        Some(topics) => named(facade, group, topics, token).await,
        None => all_topics(facade, group, token).await,
    }
}

/// The form a consumer sends: these topics, these partitions.
async fn named(
    facade: &Facade,
    group: &str,
    topics: &[kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic],
    token: Option<&str>,
) -> OffsetFetchResponse {
    let mut keys: Vec<String> = Vec::new();
    let mut slots: Vec<Vec<Slot>> = Vec::with_capacity(topics.len());
    for topic in topics {
        let name = topic.name.0.as_str();
        slots.push(
            topic
                .partition_indexes
                .iter()
                .map(|index| stage(&mut keys, group, name, *index))
                .collect(),
        );
    }

    let loaded = match offsets::load(facade.queen.as_ref(), &keys, token).await {
        Ok(loaded) => loaded,
        Err(e) => {
            tracing::warn!(target: "kafka", group, error = %e, "offset fetch failed");
            return refuse_named(topics, offsets::kafka_error(&e));
        }
    };
    // A key the store did not cover is not "never committed" — see the module
    // header. One retriable refusal for the whole request, because a
    // partition-level error on this path is not something every client handles
    // gracefully, while a group-level retriable one is exactly what they are
    // built to wait out.
    if loaded.contains(&Loaded::Unread) {
        tracing::warn!(
            target: "kafka",
            group,
            partitions = loaded.iter().filter(|l| **l == Loaded::Unread).count(),
            "the offset store did not return every key asked for"
        );
        return refuse_named(topics, ResponseError::CoordinatorLoadInProgress);
    }
    // Unreachable — `offsets::load` answers one entry per key — and checked
    // anyway, HERE, because the only other place to notice it is inside the
    // per-partition mapping below, where the only answers available are a `-1`
    // this handler has just promised never to guess and a per-partition error
    // code the client cannot survive. Refused the same way an unread key is.
    if loaded.len() != keys.len() {
        tracing::error!(
            target: "kafka",
            group,
            asked = keys.len(),
            answered = loaded.len(),
            "the offset store answered a different number of keys than it was asked for"
        );
        return refuse_named(topics, ResponseError::CoordinatorLoadInProgress);
    }

    OffsetFetchResponse::default().with_topics(
        topics
            .iter()
            .zip(&slots)
            .map(|(topic, row)| {
                OffsetFetchResponseTopic::default()
                    .with_name(topic.name.clone())
                    .with_partitions(
                        topic
                            .partition_indexes
                            .iter()
                            .zip(row)
                            .map(|(index, slot)| match slot {
                                Slot::NoOffset => partition(*index, None, None),
                                // `Loaded::Unread` and a short answer are both
                                // refused above, so the only miss left here is
                                // a key the store reported absent: never
                                // committed, which IS a -1.
                                Slot::Read(i) => match loaded.get(*i) {
                                    Some(Loaded::Found(c)) => partition(*index, Some(c), None),
                                    _ => partition(*index, None, None),
                                },
                            })
                            .collect(),
                    )
            })
            .collect(),
    )
}

/// Resolve one requested partition into a read or a "-1 without asking".
///
/// Three shapes never reach the store, and all three get the same `-1`: a name
/// Metadata would not show (`__`-prefixed or not a legal Kafka name), a negative
/// partition index, and a `(group, topic)` pair whose composed key is longer
/// than the store's key column ([`offsets::key`]). Each is a key nothing was
/// ever committed under and nothing ever will be — `handlers::offset_commit`
/// refuses all three rather than silently dropping them — so "this group has not
/// committed here" is the true answer, the stable one, and the one Kafka's own
/// broker gives for a partition it does not hold. It is emphatically NOT a
/// wrong `-1` of the kind the module header warns about: those are keys the
/// store could not be READ for.
fn stage(keys: &mut Vec<String>, group: &str, topic: &str, index: i32) -> Slot {
    if metadata::reserved_or_invalid(topic).is_some() || index < 0 {
        return Slot::NoOffset;
    }
    let Some(key) = offsets::key(group, topic, index) else {
        return Slot::NoOffset;
    };
    keys.push(key);
    Slot::Read(keys.len() - 1)
}

/// The form an admin tool sends: everything this group has committed.
async fn all_topics(facade: &Facade, group: &str, token: Option<&str>) -> OffsetFetchResponse {
    let rows = match offsets::load_group(facade.queen.as_ref(), group, token).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::warn!(target: "kafka", group, error = %e, "offset fetch (all topics) failed");
            return OffsetFetchResponse::default().with_error_code(offsets::kafka_error(&e).code());
        }
    };
    // Grouped by topic, in the order the store returned them — which is byte
    // order on the key, so a topic's partitions arrive together.
    let mut topics: Vec<OffsetFetchResponseTopic> = Vec::new();
    for (topic, index, committed) in rows {
        // A topic that would not be visible through Metadata is not made
        // visible here either: it can only be one committed against before the
        // name rule existed, or by something that is not a Kafka client.
        if metadata::reserved_or_invalid(&topic).is_some() {
            continue;
        }
        let answer = partition(index, Some(&committed), None);
        match topics
            .iter_mut()
            .find(|t| t.name.0.as_str() == topic.as_str())
        {
            Some(t) => t.partitions.push(answer),
            None => topics.push(
                OffsetFetchResponseTopic::default()
                    .with_name(TopicName(StrBytes::from_string(topic)))
                    .with_partitions(vec![answer]),
            ),
        }
    }
    OffsetFetchResponse::default().with_topics(topics)
}

fn partition(
    index: i32,
    committed: Option<&offsets::Committed>,
    error: Option<ResponseError>,
) -> OffsetFetchResponsePartition {
    OffsetFetchResponsePartition::default()
        .with_partition_index(index)
        .with_committed_offset(committed.map_or(NO_OFFSET, |c| c.offset))
        .with_committed_leader_epoch(NO_EPOCH)
        // `Some("")` and not `None`: the field is nullable, but a client that
        // reads metadata without checking for null is a client this facade can
        // break for no reason at all.
        .with_metadata(Some(StrBytes::from_string(
            committed.map(|c| c.metadata.clone()).unwrap_or_default(),
        )))
        .with_error_code(error.map_or(0, |e| e.code()))
}

/// Refuse the whole request. The top-level `error_code` exists from v2 and is
/// what a client checks first; the per-partition codes carry the same refusal
/// to v1, which has no top-level field.
fn refuse_all(req: &OffsetFetchRequest, error: ResponseError) -> OffsetFetchResponse {
    match &req.topics {
        Some(topics) => refuse_named(topics, error),
        None => OffsetFetchResponse::default().with_error_code(error.code()),
    }
}

fn refuse_named(
    topics: &[kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic],
    error: ResponseError,
) -> OffsetFetchResponse {
    OffsetFetchResponse::default()
        .with_error_code(error.code())
        .with_topics(
            topics
                .iter()
                .map(|topic| {
                    OffsetFetchResponseTopic::default()
                        .with_name(topic.name.clone())
                        .with_partitions(
                            topic
                                .partition_indexes
                                .iter()
                                .map(|index| partition(*index, None, Some(error)))
                                .collect(),
                        )
                })
                .collect(),
        )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::NO_GENERATION;
    use crate::handlers::testing::{facade, facade_and_queen};
    use crate::queen::Error;
    use bytes::BytesMut;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::{GroupId, OffsetCommitRequest};
    use kafka_protocol::protocol::{Decodable, Encodable, Message};

    /// `[(topic, [partition])]` → the named-topics form.
    fn request(group: &str, topics: &[(&str, &[i32])]) -> OffsetFetchRequest {
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_topics(Some(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        OffsetFetchRequestTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partition_indexes(partitions.to_vec())
                    })
                    .collect(),
            ))
    }

    /// The all-topics form: a null topics array.
    fn request_all(group: &str) -> OffsetFetchRequest {
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_topics(None)
    }

    fn answer<'a>(
        resp: &'a OffsetFetchResponse,
        topic: &str,
        partition: i32,
    ) -> &'a OffsetFetchResponsePartition {
        resp.topics
            .iter()
            .find(|t| t.name.0.as_str() == topic)
            .unwrap_or_else(|| panic!("{topic} is not in the response"))
            .partitions
            .iter()
            .find(|p| p.partition_index == partition)
            .unwrap_or_else(|| panic!("{topic}/{partition} is not in the response"))
    }

    /// One partition of a fixture commit: index, offset, metadata.
    type Commit<'a> = (i32, i64, &'a str);

    /// Commit through the real handler, so the keys are the ones the protocol
    /// would have written.
    async fn commit(f: &Facade, group: &str, topics: &[(&str, &[Commit<'_>])]) {
        let req = OffsetCommitRequest::default()
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
            );
        let resp = crate::handlers::offset_commit::handle(f, &req, None).await;
        for topic in &resp.topics {
            for p in &topic.partitions {
                assert_eq!(p.error_code, 0, "the fixture commit failed");
            }
        }
    }

    // --------------------------------------------------------- the round trip

    /// What was committed comes back, metadata included, and a partition that
    /// was not committed comes back as -1 with NO error — which is what makes
    /// a new consumer apply auto.offset.reset instead of failing.
    #[tokio::test]
    async fn a_commit_reads_back_and_a_missing_one_is_minus_one() {
        let f = facade(&[]);
        commit(&f, "g", &[("orders", &[(0, 41, "batch-7")])]).await;

        let resp = handle(&f, &request("g", &[("orders", &[0, 1])]), None).await;
        let committed = answer(&resp, "orders", 0);
        assert_eq!(committed.error_code, 0);
        assert_eq!(committed.committed_offset, 41);
        assert_eq!(committed.metadata.as_ref().unwrap().as_str(), "batch-7");
        assert_eq!(committed.committed_leader_epoch, -1);

        let never = answer(&resp, "orders", 1);
        assert_eq!(never.error_code, 0, "a missing offset is not an error");
        assert_eq!(never.committed_offset, -1);
        assert_eq!(never.metadata.as_ref().unwrap().as_str(), "");
    }

    /// One group cannot read another's, even for the same topic and partition.
    #[tokio::test]
    async fn groups_do_not_see_each_others_offsets() {
        let f = facade(&[]);
        commit(&f, "a", &[("orders", &[(0, 41, "")])]).await;
        commit(&f, "b", &[("orders", &[(0, 99, "")])]).await;

        assert_eq!(
            handle(&f, &request("a", &[("orders", &[0])]), None)
                .await
                .topics[0]
                .partitions[0]
                .committed_offset,
            41
        );
        assert_eq!(
            handle(&f, &request("b", &[("orders", &[0])]), None)
                .await
                .topics[0]
                .partitions[0]
                .committed_offset,
            99
        );
    }

    /// A whole assignment is read in as few calls as the store allows, not one
    /// per partition.
    #[tokio::test]
    async fn a_whole_assignment_is_read_in_one_call() {
        let (f, api) = facade_and_queen(&[]);
        let partitions: Vec<i32> = (0..64).collect();
        api.kv_calls.lock().unwrap().clear();
        handle(&f, &request("g", &[("orders", &partitions)]), None).await;
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);
    }

    // ------------------------------------------------------------ all topics

    /// The admin form: everything the group has, grouped by topic, and nothing
    /// belonging to anyone else.
    #[tokio::test]
    async fn the_all_topics_form_answers_the_whole_group() {
        let f = facade(&[]);
        commit(
            &f,
            "g",
            &[
                ("orders", &[(0, 10, "a"), (1, 11, "")]),
                ("clicks", &[(7, 70, "")]),
            ],
        )
        .await;
        commit(&f, "other", &[("orders", &[(0, 999, "")])]).await;

        let resp = handle(&f, &request_all("g"), None).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.topics.len(), 2, "{:?}", resp.topics);
        assert_eq!(answer(&resp, "orders", 0).committed_offset, 10);
        assert_eq!(
            answer(&resp, "orders", 0)
                .metadata
                .as_ref()
                .unwrap()
                .as_str(),
            "a"
        );
        assert_eq!(answer(&resp, "orders", 1).committed_offset, 11);
        assert_eq!(answer(&resp, "clicks", 7).committed_offset, 70);
    }

    /// A group that has committed nothing is an empty answer, not an error:
    /// `--describe` on a fresh group prints no rows and does not fail.
    #[tokio::test]
    async fn the_all_topics_form_of_an_unknown_group_is_empty() {
        let f = facade(&[]);
        let resp = handle(&f, &request_all("never-committed"), None).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.topics.is_empty());
    }

    // --------------------------------------------------------------- refusals

    /// A key this facade could never have written is answered `-1` with NO error
    /// code, not a per-partition refusal — the answer Kafka's own broker gives
    /// for a `(topic, partition)` the group has not committed, and the only one a
    /// Java consumer survives (any per-partition code here is a `KafkaException`
    /// out of `poll()`). It still costs nothing: none of them reaches the store.
    #[tokio::test]
    async fn a_key_that_was_never_committable_is_no_offset_without_reading() {
        let (f, api) = facade_and_queen(&[]);
        let resp = handle(
            &f,
            &request(
                "g",
                &[
                    ("__consumer_offsets", &[0]),
                    ("not a topic", &[0]),
                    ("clicks", &[-1]),
                ],
            ),
            None,
        )
        .await;
        for (topic, index) in [
            ("__consumer_offsets", 0),
            ("not a topic", 0),
            ("clicks", -1),
        ] {
            let p = answer(&resp, topic, index);
            assert_eq!(p.error_code, 0, "{topic}/{index}");
            assert_eq!(p.committed_offset, -1, "{topic}/{index}");
        }
        // Every partition was answered locally, so nothing was asked of Queen.
        assert_eq!(api.kv_ops().len(), 0);
    }

    #[tokio::test]
    async fn an_empty_group_id_is_refused_in_both_forms() {
        let f = facade(&[]);
        let named = handle(&f, &request("", &[("orders", &[0])]), None).await;
        assert_eq!(named.error_code, ResponseError::InvalidGroupId.code());
        assert_eq!(
            answer(&named, "orders", 0).error_code,
            ResponseError::InvalidGroupId.code(),
            "v1 has no top-level error field to read"
        );

        let all = handle(&f, &request_all(""), None).await;
        assert_eq!(all.error_code, ResponseError::InvalidGroupId.code());
    }

    /// A store that cannot be read is a retriable refusal — NEVER a -1, which
    /// a consumer would act on by resetting.
    #[tokio::test]
    async fn an_unreadable_store_is_refused_and_not_answered_minus_one() {
        let (f, api) = facade_and_queen(&[]);
        api.fail_kv(Error::Transport("connection refused".into()));
        let resp = handle(&f, &request("g", &[("orders", &[0])]), None).await;
        assert_eq!(
            resp.error_code,
            ResponseError::CoordinatorNotAvailable.code()
        );
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::CoordinatorNotAvailable.code());
        assert_eq!(p.committed_offset, -1);

        api.fail_kv(Error::status(403, "forbidden"));
        let resp = handle(&f, &request_all("g"), None).await;
        assert_eq!(
            resp.error_code,
            ResponseError::GroupAuthorizationFailed.code()
        );
    }

    /// The same rule for a read the byte budget cut short: the partitions it
    /// did not cover are not "never committed".
    #[tokio::test]
    async fn a_truncated_read_is_refused_rather_than_reported_as_missing() {
        let (f, api) = facade_and_queen(&[]);
        commit(
            &f,
            "g",
            &[("orders", &[(0, 1, ""), (1, 2, ""), (2, 3, "")])],
        )
        .await;
        api.kv_truncate_reads_at(1);

        let resp = handle(&f, &request("g", &[("orders", &[0, 1, 2])]), None).await;
        assert_eq!(
            resp.error_code,
            ResponseError::CoordinatorLoadInProgress.code()
        );
        for p in &resp.topics[0].partitions {
            assert_eq!(
                p.error_code,
                ResponseError::CoordinatorLoadInProgress.code(),
                "partition {} was answered as if it had no offset",
                p.partition_index
            );
        }
    }

    #[tokio::test]
    async fn the_token_reaches_the_store() {
        let (f, api) = facade_and_queen(&[]);
        handle(&f, &request("g", &[("orders", &[0])]), Some("tenant-a")).await;
        handle(&f, &request_all("g"), Some("tenant-a")).await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }

    // -------------------------------------------------------------- the wire

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::OffsetFetch as i16)
            .expect("OffsetFetch is advertised");
        assert!(
            row.min >= OffsetFetchRequest::VERSIONS.min
                && row.max <= OffsetFetchRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let f = facade(&[]);
            commit(&f, "g", &[("orders", &[(3, 41, "batch-7")])]).await;

            let mut wire = BytesMut::new();
            request("g", &[("orders", &[3])])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = OffsetFetchRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = OffsetFetchResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            let p = answer(&back, "orders", 3);
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.committed_offset, 41, "v{version}");
            assert_eq!(
                p.metadata.as_ref().unwrap().as_str(),
                "batch-7",
                "v{version}"
            );
        }
    }

    /// The all-topics form is a NULL topics array, and it has to survive the
    /// wire as null rather than as an empty list — an empty list is "no
    /// partitions at all", which is a different question with a different
    /// answer.
    #[tokio::test]
    async fn the_all_topics_form_survives_the_wire() {
        let row =
            crate::versions::lookup(kafka_protocol::messages::ApiKey::OffsetFetch as i16).unwrap();
        for version in row.min..=row.max {
            let f = facade(&[]);
            commit(&f, "g", &[("orders", &[(0, 5, "")])]).await;

            let mut wire = BytesMut::new();
            if request_all("g").encode(&mut wire, version).is_err() {
                // v1 has no null form: the field is not nullable there, which
                // is exactly why the version exists to be checked.
                assert_eq!(version, 1, "v{version} refused a null topics array");
                continue;
            }
            let mut buf = wire.freeze();
            let decoded = OffsetFetchRequest::decode(&mut buf, version).unwrap();
            assert!(decoded.topics.is_none(), "v{version}: the null was lost");

            let resp = handle(&f, &decoded, None).await;
            assert_eq!(answer(&resp, "orders", 0).committed_offset, 5, "v{version}");
        }
    }
}
