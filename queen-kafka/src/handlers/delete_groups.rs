//! DeleteGroups (key 42), v0-v2 — `kafka-consumer-groups.sh --delete`.
//!
//! ## Say this part out loud
//!
//! This is an IRREVERSIBLE delete of committed offsets, reachable from any
//! authenticated Kafka client. It is not a privilege escalation — the same
//! bearer can already delete the same KV keys over `POST /api/v1/kv`, and the
//! connection's credential is the only thing that reaches Queen — but until M7
//! F2 nothing removed a committed offset at all, and this is the first time
//! `--delete` reaches one. Two consequences, designed rather than discovered:
//!
//! 1. A group that is merely STOPPED loses its position. Its next start runs
//!    `auto.offset.reset`: a replay of the whole topic, or a jump to its end
//!    past everything it had not read. That is exactly Apache Kafka's behaviour
//!    and it is still a footgun, which is what Kafka's own emptiness rule below
//!    exists to blunt.
//! 2. Offsets still never expire on their own. This is a TOOL, not a policy:
//!    nothing here adds `offsets.retention.minutes`, and a group nobody deletes
//!    keeps its offsets for ever ([`crate::offsets`]).
//!
//! ## Kafka's rule, kept exactly: only an empty group is deletable
//!
//! A group with members answers NON_EMPTY_GROUP (68) and NOTHING is touched.
//! That rule is the whole guard: without it, one `--delete` typed against a
//! running fleet would silently reset every consumer in it. Measured against
//! `apache/kafka:3.9.1` rather than assumed — a live group answers 68, an empty
//! one answers 0, and a group nobody has heard of answers GROUP_ID_NOT_FOUND
//! (69).
//!
//! ## The sequence, and what a failure part-way through leaves
//!
//! Per group id: validate the name, check this node coordinates it, refuse a
//! group with members, delete every offset under its prefix, delete its index
//! row, and drop its (empty) actor so the DescribeGroups that follows says
//! `Dead` rather than `Empty`.
//!
//! There is no transaction across a KV batch boundary and this does not pretend
//! there is. A failure part-way through leaves a partially deleted group, it is
//! answered with the retriable code the failure maps to
//! ([`crate::offsets::kafka_error`]), and re-running the delete finishes the job
//! because every step is idempotent. Stated here and in `compat/ERRORS.md`
//! rather than papered over.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::{DeleteGroupsRequest, DeleteGroupsResponse, GroupId};
use kafka_protocol::protocol::StrBytes;

use crate::offsets;
use crate::Facade;

/// How many groups one request deletes.
///
/// Each delete is a prefix walk plus one delete batch per page, in sequence, on
/// a connection that is muted until the whole response is written — the same
/// argument that bounds the create and delete paths of the topics trio, and the
/// same number. The groups past it are answered with a RETRIABLE code, so a
/// request naming a thousand groups converges over a few calls.
const MAX_DELETED_GROUPS: usize = 100;

/// One line per window when the per-request ceiling binds.
static DELETE_CAP: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

pub async fn handle(
    facade: &Facade,
    req: &DeleteGroupsRequest,
    token: Option<&str>,
) -> DeleteGroupsResponse {
    let mut results = Vec::with_capacity(req.groups_names.len());
    let mut deleted = 0usize;
    let mut deferred = 0usize;

    for id in &req.groups_names {
        let group = id.0.as_str();
        if let Some(e) = crate::coordinator::invalid_group_id(group) {
            results.push(answer(group, Some(e)));
            continue;
        }
        // Cluster mode, before anything is read or written: a node that does
        // not coordinate this group cannot see whether it has members, so it
        // cannot apply Kafka's emptiness rule — and deleting the offsets of a
        // group another node is running is exactly the thing that rule exists
        // to stop. NOT_COORDINATOR is the redirect.
        if let Some(e) = facade.cluster.group_guard(group) {
            results.push(answer(group, Some(e)));
            continue;
        }
        if deleted >= MAX_DELETED_GROUPS {
            deferred += 1;
            results.push(answer(group, Some(ResponseError::CoordinatorNotAvailable)));
            continue;
        }
        deleted += 1;

        // Kafka's rule. `Some(false)` is a group with members — refused, and
        // nothing is touched. `Some(true)` is an empty group, now reaped, so
        // the next DescribeGroups says `Dead`. `None` is no actor at all, which
        // is not a refusal: the group's offsets may still be in Queen and are
        // exactly what this call is for.
        let had_actor = match facade.coordinator.discard_if_empty(group).await {
            Some(false) => {
                results.push(answer(group, Some(ResponseError::NonEmptyGroup)));
                continue;
            }
            Some(true) => true,
            None => false,
        };

        match offsets::delete_group(facade.queen.as_ref(), group, token).await {
            Ok(0) if !had_actor => {
                // Nothing in the registry and nothing in the store. Kafka's own
                // answer, measured.
                results.push(answer(group, Some(ResponseError::GroupIdNotFound)));
            }
            Ok(removed) => {
                tracing::info!(
                    target: "kafka",
                    group,
                    keys = removed,
                    "deleted a consumer group and its committed offsets (DeleteGroups)"
                );
                results.push(answer(group, None));
            }
            Err(e) => {
                tracing::error!(
                    target: "kafka",
                    group,
                    error = %e,
                    "DeleteGroups could not remove the group; it may be partly deleted, and \
                     re-running the delete finishes it"
                );
                results.push(answer(group, Some(offsets::kafka_error(&e))));
            }
        }
    }

    if deferred > 0 {
        if let Some(suppressed) = DELETE_CAP.tick_now() {
            tracing::warn!(
                target: "kafka",
                deleted,
                deferred,
                suppressed,
                "one request asked to delete more consumer groups than are deleted at a time; the \
                 rest were answered COORDINATOR_NOT_AVAILABLE and go as the client retries"
            );
        }
    }

    DeleteGroupsResponse::default().with_results(results)
}

fn answer(group: &str, error: Option<ResponseError>) -> DeletableGroupResult {
    DeletableGroupResult::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_error_code(error.map_or(0, |e| e.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::{clustered, facade_and_queen};
    use crate::queen::Error;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::describe_groups_request::DescribeGroupsRequest;
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{JoinGroupRequest, OffsetCommitRequest, TopicName};
    use kafka_protocol::protocol::{Decodable, Encodable, Message};

    fn request(groups: &[&str]) -> DeleteGroupsRequest {
        DeleteGroupsRequest::default().with_groups_names(
            groups
                .iter()
                .map(|g| GroupId(StrBytes::from_string((*g).to_string())))
                .collect(),
        )
    }

    fn code(resp: &DeleteGroupsResponse, group: &str) -> i16 {
        resp.results
            .iter()
            .find(|r| r.group_id.0.as_str() == group)
            .unwrap_or_else(|| panic!("{group} is not in the answer"))
            .error_code
    }

    async fn join(f: &Facade, group: &str) -> String {
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::new())]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c", "/127.0.0.1").await;
        let member = minted.member_id.clone();
        crate::handlers::join_group::handle(
            f,
            &req.with_member_id(member.clone()),
            4,
            "c",
            "/127.0.0.1",
        )
        .await;
        member.to_string()
    }

    async fn commit(f: &Facade, group: &str, partitions: i32) {
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(crate::coordinator::NO_GENERATION)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(
                    (0..partitions)
                        .map(|p| {
                            OffsetCommitRequestPartition::default()
                                .with_partition_index(p)
                                .with_committed_offset(41)
                        })
                        .collect(),
                )]);
        crate::handlers::offset_commit::handle(f, &req, None).await;
    }

    async fn described_state(f: &Facade, group: &str) -> String {
        let req = DescribeGroupsRequest::default()
            .with_groups(vec![GroupId(StrBytes::from_string(group.to_string()))]);
        crate::handlers::describe_groups::handle(f, &req, None)
            .await
            .groups[0]
            .group_state
            .as_str()
            .to_string()
    }

    /// The happy path, end to end through the real handlers: a stopped group's
    /// offsets and its index row are gone, and the group reads back `Dead`.
    #[tokio::test]
    async fn a_stopped_group_and_its_offsets_are_removed() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "dl1-stopped", 3).await;
        assert_eq!(described_state(&f, "dl1-stopped").await, "Empty");
        assert_eq!(
            api.kv_keys()
                .iter()
                .filter(|k| k.starts_with("qk:group"))
                .count(),
            4,
            "three offsets and one index row"
        );

        let resp = handle(&f, &request(&["dl1-stopped"]), None).await;
        assert_eq!(code(&resp, "dl1-stopped"), 0);
        assert!(
            api.kv_keys().iter().all(|k| !k.starts_with("qk:group")),
            "the group left keys behind: {:?}",
            api.kv_keys()
        );
        assert_eq!(described_state(&f, "dl1-stopped").await, "Dead");
    }

    /// Kafka's guard, and the reason it exists: a `--delete` typed against a
    /// running fleet must not silently reset it.
    #[tokio::test]
    async fn a_group_with_members_is_refused_and_keeps_its_offsets() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        join(&f, "dl2-live").await;
        commit(&f, "dl2-live", 2).await;
        let before = api.kv_keys().len();

        let resp = handle(&f, &request(&["dl2-live"]), None).await;
        assert_eq!(code(&resp, "dl2-live"), ResponseError::NonEmptyGroup.code());
        assert_eq!(api.kv_keys().len(), before, "a refused delete removed keys");
        assert_eq!(
            f.coordinator.live_groups(),
            1,
            "a refused delete reaped the actor"
        );
    }

    /// ...and once the members are gone, the same delete goes through and the
    /// actor goes with it.
    #[tokio::test]
    async fn the_same_group_deletes_once_its_members_have_left() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let member = join(&f, "dl3-live").await;
        commit(&f, "dl3-live", 1).await;

        let leave = kafka_protocol::messages::LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("dl3-live")))
            .with_member_id(StrBytes::from_string(member));
        crate::handlers::leave_group::handle(&f, &leave).await;
        assert_eq!(described_state(&f, "dl3-live").await, "Empty");

        assert_eq!(
            code(&handle(&f, &request(&["dl3-live"]), None).await, "dl3-live"),
            0
        );
        // The actor is reaped rather than left to answer `Empty` for the next
        // five minutes, which is what an operator would read as a failed delete.
        for _ in 0..1_000 {
            if f.coordinator.live_groups() == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(f.coordinator.live_groups(), 0);
        assert_eq!(described_state(&f, "dl3-live").await, "Dead");
    }

    /// Measured against apache/kafka:3.9.1: a group nobody has heard of is
    /// GROUP_ID_NOT_FOUND, not a success and not an unknown-topic code.
    #[tokio::test]
    async fn a_group_nobody_ever_heard_of_is_not_found() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &request(&["never-existed"]), None).await;
        assert_eq!(
            code(&resp, "never-existed"),
            ResponseError::GroupIdNotFound.code()
        );
    }

    /// A delete is idempotent, which is what makes a partially failed one
    /// re-runnable: the second call finds nothing left and says so.
    #[tokio::test]
    async fn a_second_delete_of_the_same_group_is_not_found() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "dl5-g", 2).await;
        assert_eq!(
            code(&handle(&f, &request(&["dl5-g"]), None).await, "dl5-g"),
            0
        );
        assert_eq!(
            code(&handle(&f, &request(&["dl5-g"]), None).await, "dl5-g"),
            ResponseError::GroupIdNotFound.code()
        );
    }

    /// One group's answer does not move another's, and a bad name does not take
    /// its neighbours with it — this API has no top-level error field either.
    #[tokio::test]
    async fn results_line_up_with_the_request() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "dl6-a", 1).await;
        join(&f, "dl6-c").await;
        let huge = "g".repeat(crate::coordinator::MAX_GROUP_ID_CHARS + 1);

        let resp = handle(&f, &request(&["dl6-a", "dl6-b", "dl6-c", &huge]), None).await;
        let names: Vec<&str> = resp.results.iter().map(|r| r.group_id.0.as_str()).collect();
        assert_eq!(names, ["dl6-a", "dl6-b", "dl6-c", huge.as_str()]);
        assert_eq!(resp.results[0].error_code, 0);
        assert_eq!(
            resp.results[1].error_code,
            ResponseError::GroupIdNotFound.code()
        );
        assert_eq!(
            resp.results[2].error_code,
            ResponseError::NonEmptyGroup.code()
        );
        assert_eq!(
            resp.results[3].error_code,
            ResponseError::InvalidGroupId.code()
        );
    }

    /// A store that fails is a retriable code and never a reported success: a
    /// client told its group was deleted when it was not would stop retrying.
    #[tokio::test]
    async fn a_failed_delete_is_never_reported_as_deleted() {
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
            let (f, api) = facade_and_queen(&[("orders", 4)]);
            let group = format!("dl7-{}", kafka.code());
            commit(&f, &group, 1).await;
            api.fail_kv(queen_error.clone());
            let resp = handle(&f, &request(&[&group]), None).await;
            assert_eq!(code(&resp, &group), kafka.code(), "{queen_error}");
        }
    }

    /// Cluster mode: a non-owner cannot see whether the group has members, so
    /// it must not delete its offsets. It answers the redirect and touches
    /// nothing.
    #[tokio::test]
    async fn a_non_owner_deletes_nothing() {
        const THREE: [(i32, &str, u16); 3] = [
            (1, "kafka-1.example.com", 9092),
            (2, "kafka-2.example.com", 9092),
            (3, "kafka-3.example.com", 9092),
        ];
        let (stranger, api) = clustered(&[("orders", 4)], &THREE, 1);
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::key("orders-consumer", "orders", 0).unwrap(),
            serde_json::json!({"offset": 50, "metadata": "", "ts": 1}),
        );

        let resp = handle(&stranger, &request(&["orders-consumer"]), None).await;
        assert_eq!(
            code(&resp, "orders-consumer"),
            ResponseError::NotCoordinator.code()
        );
        assert!(
            api.kv_get(
                offsets::NAMESPACE,
                &offsets::key("orders-consumer", "orders", 0).unwrap()
            )
            .is_some(),
            "a non-owner deleted a group's offsets"
        );
    }

    #[tokio::test]
    async fn one_request_deletes_at_most_the_bound() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let names: Vec<String> = (0..MAX_DELETED_GROUPS + 3)
            .map(|i| format!("g{i:04}"))
            .collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let resp = handle(&f, &request(&refs), None).await;
        assert_eq!(resp.results.len(), names.len());
        for r in resp.results.iter().skip(MAX_DELETED_GROUPS) {
            assert_eq!(r.error_code, ResponseError::CoordinatorNotAvailable.code());
        }
    }

    #[tokio::test]
    async fn the_connections_credential_is_what_deletes() {
        let (root, api) = facade_and_queen(&[("orders", 4)]);
        let f = root.for_connection(None).authenticated_as("tenant-key");
        handle(&f, &request(&["g"]), Some("tenant-key")).await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(
            tokens.iter().all(|t| t.as_deref() == Some("tenant-key")),
            "a delete went out under the wrong credential: {tokens:?}"
        );
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::DeleteGroups as i16)
            .expect("DeleteGroups is advertised");
        assert!(
            row.min >= DeleteGroupsRequest::VERSIONS.min
                && row.max <= DeleteGroupsRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let (f, _) = facade_and_queen(&[("orders", 4)]);
            let group = format!("dl9-g-{version}");
            commit(&f, &group, 2).await;

            let mut wire = BytesMut::new();
            request(&[&group])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = DeleteGroupsRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = DeleteGroupsResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(code(&back, &group), 0, "v{version}");
        }
    }
}
