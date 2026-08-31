//! DescribeGroups (key 15), v0-v3 — a group's members, verbatim.
//!
//! This is what makes `kafka-consumer-groups.sh --describe --group g` print its
//! table, and the whole value of the API is that two byte strings per member
//! are passed through UNTOUCHED: the subscription this member sent at JoinGroup
//! for the elected protocol, and the slice the leader posted for it at
//! SyncGroup. The facade parses neither — the same rule the coordinator is
//! built on — and a client decodes them with its own assignor. The lag beside
//! them is not this API's: it rides OffsetFetch and ListOffsets, which have
//! been advertised since M4 and M3.
//!
//! ## The unknown-group answer was MEASURED, not assumed
//!
//! Apache Kafka 3.9.1, asked about a group it has never heard of, answers
//! `error_code = 0` with state `Dead` — not an error. Read off the wire against
//! `apache/kafka:3.9.1` before this handler was written, at v0, v3 and v5, and
//! it is the answer here. It matters because it is counter-intuitive: a client
//! that asked about a typo gets a group description back, and a handler written
//! from the name of the code would have answered GROUP_ID_NOT_FOUND and broken
//! every tool that branches on the state.
//!
//! The same run settled the rest of the table: a group whose last member left
//! is `Empty` with its protocol type RETAINED, a deleted group is `Dead` again,
//! and `authorized_operations` is a real bitfield rather than the sentinel —
//! see below for why this facade answers the sentinel anyway.
//!
//! ## Three answers, from the facade's two halves
//!
//! | The group is | state | protocol_type | members |
//! | --- | --- | --- | --- |
//! | live here | the FSM's | the members' | every member |
//! | only in the durable index | `Empty` | the index's | none |
//! | in neither | `Dead` | `""` | none |
//!
//! The middle row is the one that needs the index ([`crate::offsets`]): a
//! group's actor is reaped `GroupConfig::empty_reap` after its last member
//! leaves, and answering `Dead` for a group that has committed offsets and
//! merely stopped would tell an operator their group was gone.
//!
//! ## `authorized_operations` is the sentinel, deliberately
//!
//! Kafka 3.9.1 with no authorizer answers 328 — READ | DELETE | DESCRIBE. This
//! facade answers `i32::MIN`, which is Kafka's own
//! `AUTHORIZED_OPERATIONS_OMITTED`: the Java client turns it into `null` and
//! tools render "unknown". The reason is that the facade has NO ACL model —
//! what a credential may do is Queen's to say, per call, and it says so by
//! answering 401 or 403 to the call itself. A bitfield computed here would be a
//! permission set this process invented, and a UI that greyed out a button on
//! it would be acting on a guess. It is answered whether or not
//! `include_authorized_operations` was set, because "omitted" is true either
//! way.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::{DescribeGroupsRequest, DescribeGroupsResponse, GroupId};
use kafka_protocol::protocol::StrBytes;

use crate::coordinator::group::State;
use crate::coordinator::GroupDescription;
use crate::handlers::list_groups::state_name;
use crate::offsets;
use crate::Facade;

/// Kafka's `AUTHORIZED_OPERATIONS_OMITTED`. See the module header.
const AUTHORIZED_OPERATIONS_OMITTED: i32 = i32::MIN;

/// How many groups one request may ask about.
///
/// Each group past the live registry costs a lookup in the durable index, and
/// the index reads are chunked against the store's own key ceiling — so a
/// request naming a hundred thousand group ids is a hundred thousand keys read
/// on a connection that is muted until the whole answer is written. A thousand
/// is more groups than any tool describes at once (`kafka-consumer-groups.sh`
/// describes one, a UI a page) and the groups past it are answered with a
/// RETRIABLE code, so a client that really wants them gets them over several
/// calls.
const MAX_DESCRIBED_GROUPS: usize = 1_000;

/// One line per window when the per-request ceiling binds.
static DESCRIBE_CAP: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// What one requested group resolved to before the index was read.
enum Slot {
    /// Answer this error and read nothing.
    Refuse(ResponseError),
    /// A group with an actor here.
    Live(Box<GroupDescription>),
    /// No actor. Whether it is `Empty` or `Dead` is the index's to say.
    Absent,
}

pub async fn handle(
    facade: &Facade,
    req: &DescribeGroupsRequest,
    token: Option<&str>,
) -> DescribeGroupsResponse {
    let mut slots: Vec<Slot> = Vec::with_capacity(req.groups.len());
    let mut asked = 0usize;
    let mut deferred = 0usize;
    for group in &req.groups {
        let group = group.0.as_str();
        // The same rule the other five group-addressed APIs apply, in the same
        // one function, so a name JoinGroup refuses and this one describes
        // cannot exist ([`crate::coordinator::invalid_group_id`]). Apache Kafka
        // has no length bound of its own and answers `Dead` for these; the
        // bound is this facade's, because every copy of a group id — the
        // registry key, the composed KV key, every log line — is this facade's.
        if let Some(e) = crate::coordinator::invalid_group_id(group) {
            slots.push(Slot::Refuse(e));
            continue;
        }
        // Cluster mode: this node cannot see the members of a group it does not
        // coordinate, and `Empty` would be a plausible wrong answer. Both codes
        // the guard answers are retriable, and a client's response to
        // NOT_COORDINATOR is to re-run FindCoordinator — which is the redirect.
        if let Some(e) = facade.cluster.group_guard(group) {
            slots.push(Slot::Refuse(e));
            continue;
        }
        if asked >= MAX_DESCRIBED_GROUPS {
            deferred += 1;
            slots.push(Slot::Refuse(ResponseError::CoordinatorNotAvailable));
            continue;
        }
        asked += 1;
        slots.push(match facade.coordinator.describe_group(group).await {
            Some(live) => Slot::Live(Box::new(live)),
            None => Slot::Absent,
        });
    }

    if deferred > 0 {
        if let Some(suppressed) = DESCRIBE_CAP.tick_now() {
            tracing::warn!(
                target: "kafka",
                described = asked,
                deferred,
                suppressed,
                "one request asked about more consumer groups than are described at a time; the \
                 rest were answered COORDINATOR_NOT_AVAILABLE and go as the client retries"
            );
        }
    }

    // The index, for every group whose answer still depends on it: one that has
    // no actor (Empty or Dead?) and one whose actor is EMPTY (the FSM clears
    // the protocol type when the last member leaves, and the index is what
    // remembers what it was — which is also what Kafka reports).
    let wanted: Vec<String> = req
        .groups
        .iter()
        .zip(&slots)
        .filter(|(_, slot)| match slot {
            Slot::Absent => true,
            Slot::Live(live) => live.protocol_type.is_none(),
            Slot::Refuse(_) => false,
        })
        .map(|(g, _)| g.0.as_str().to_string())
        .collect();
    let indexed = match offsets::load_index(facade.queen.as_ref(), &wanted, token).await {
        Ok(rows) => Ok(wanted.into_iter().zip(rows).collect::<Vec<_>>()),
        Err(e) => {
            tracing::warn!(
                target: "kafka",
                error = %e,
                "DescribeGroups could not read the durable group index"
            );
            // NOT answered as `Dead`: without the index this facade cannot tell
            // a group that never existed from one whose consumers are stopped,
            // and `Dead` for the second is what an operator would read as
            // "somebody deleted my group". The retriable code is the honest one.
            Err(offsets::kafka_error(&e))
        }
    };

    let groups = req
        .groups
        .iter()
        .zip(slots)
        .map(|(id, slot)| {
            let name = id.0.as_str();
            match slot {
                Slot::Refuse(e) => refused(name, e),
                Slot::Live(live) => {
                    let remembered = remembered_type(&indexed, name);
                    described(name, &live, remembered)
                }
                Slot::Absent => match &indexed {
                    Err(e) => refused(name, *e),
                    Ok(rows) => match rows.iter().find(|(g, _)| g == name) {
                        // It exists in Queen and nothing is in it. Kafka's own
                        // answer for the same group.
                        Some((_, Some(row))) => empty(name, &row.protocol_type),
                        // Nobody has ever heard of it. Measured: error 0, Dead.
                        _ => empty_dead(name),
                    },
                },
            }
        })
        .collect();

    DescribeGroupsResponse::default().with_groups(groups)
}

/// What the index remembers about `group`'s protocol type, or the empty string
/// when it remembers nothing (or could not be read — a missing protocol type is
/// a cosmetic loss, unlike a missing state).
fn remembered_type(
    indexed: &Result<Vec<(String, Option<offsets::Indexed>)>, ResponseError>,
    group: &str,
) -> String {
    indexed
        .as_ref()
        .ok()
        .and_then(|rows| rows.iter().find(|(g, _)| g == group))
        .and_then(|(_, row)| row.as_ref())
        .map(|row| row.protocol_type.clone())
        .unwrap_or_default()
}

fn described(group: &str, live: &GroupDescription, remembered: String) -> DescribedGroup {
    base(group)
        .with_error_code(0)
        .with_group_state(StrBytes::from_string(state_name(live.state).to_string()))
        .with_protocol_type(StrBytes::from_string(
            live.protocol_type.clone().unwrap_or(remembered),
        ))
        .with_protocol_data(StrBytes::from_string(
            live.protocol_name.clone().unwrap_or_default(),
        ))
        .with_members(
            live.members
                .iter()
                .map(|m| {
                    DescribedGroupMember::default()
                        .with_member_id(StrBytes::from_string(m.id.clone()))
                        .with_client_id(StrBytes::from_string(m.client_id.clone()))
                        .with_client_host(StrBytes::from_string(m.client_host.clone()))
                        // Verbatim, both of them. This is the API.
                        .with_member_metadata(m.metadata.clone())
                        .with_member_assignment(m.assignment.clone())
                        // Static membership is out of scope and the advertised
                        // window stops one version below where this field is
                        // encoded; null is what it means anyway.
                        .with_group_instance_id(None)
                })
                .collect(),
        )
}

/// A group the durable index has and no actor holds.
fn empty(group: &str, protocol_type: &str) -> DescribedGroup {
    base(group)
        .with_error_code(0)
        .with_group_state(StrBytes::from_string(state_name(State::Empty).to_string()))
        .with_protocol_type(StrBytes::from_string(protocol_type.to_string()))
}

/// A group nobody has ever heard of. Error 0 and `Dead`, measured against
/// Apache Kafka 3.9.1 — see the module header.
fn empty_dead(group: &str) -> DescribedGroup {
    base(group)
        .with_error_code(0)
        .with_group_state(StrBytes::from_string(state_name(State::Dead).to_string()))
}

fn refused(group: &str, error: ResponseError) -> DescribedGroup {
    base(group).with_error_code(error.code())
}

/// The fields every answer carries whatever happened to it: the group's own id,
/// so a client can index the results, and the omitted-operations sentinel.
fn base(group: &str) -> DescribedGroup {
    DescribedGroup::default()
        .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
        .with_authorized_operations(AUTHORIZED_OPERATIONS_OMITTED)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::{clustered, facade_and_queen};
    use crate::queen::Error;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
    use kafka_protocol::messages::{
        JoinGroupRequest, OffsetCommitRequest, SyncGroupRequest, TopicName,
    };
    use kafka_protocol::protocol::{Decodable, Encodable, Message};

    const HOST: &str = "/10.1.2.3";
    const SUBSCRIPTION: &[u8] = b"the-subscription-bytes";
    const ASSIGNMENT: &[u8] = b"the-assignment-bytes";

    fn request(groups: &[&str]) -> DescribeGroupsRequest {
        DescribeGroupsRequest::default()
            .with_groups(
                groups
                    .iter()
                    .map(|g| GroupId(StrBytes::from_string((*g).to_string())))
                    .collect(),
            )
            .with_include_authorized_operations(true)
    }

    fn got<'a>(resp: &'a DescribeGroupsResponse, group: &str) -> &'a DescribedGroup {
        resp.groups
            .iter()
            .find(|g| g.group_id.0.as_str() == group)
            .unwrap_or_else(|| panic!("{group} is not in the answer"))
    }

    /// A group with one member that has joined AND synced, so both byte strings
    /// exist — which is the only state in which this API says anything.
    async fn stable_group(f: &Facade, group: &str, client: &str) -> (String, i32) {
        let join = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(SUBSCRIPTION))]);
        let minted = crate::handlers::join_group::handle(f, &join, 4, client, HOST).await;
        let member = minted.member_id.clone();
        let joined = crate::handlers::join_group::handle(
            f,
            &join.with_member_id(member.clone()),
            4,
            client,
            HOST,
        )
        .await;
        let sync = SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(member.clone())
            .with_generation_id(joined.generation_id)
            .with_assignments(vec![SyncGroupRequestAssignment::default()
                .with_member_id(member.clone())
                .with_assignment(Bytes::from_static(ASSIGNMENT))]);
        crate::handlers::sync_group::handle(f, &sync).await;
        (member.to_string(), joined.generation_id)
    }

    /// A simple consumer's commit: no membership at all. It is REFUSED against
    /// a group that has members (that is `check_commit`'s whole point), so a
    /// live group commits through [`commit_as`] instead.
    async fn commit(f: &Facade, group: &str) {
        commit_inner(f, group, "", crate::coordinator::NO_GENERATION).await
    }

    /// A member of the current generation committing, which is what a real
    /// consumer does and what puts a LIVE group's protocol type in the index.
    async fn commit_as(f: &Facade, group: &str, member: &str, generation: i32) {
        commit_inner(f, group, member, generation).await
    }

    async fn commit_inner(f: &Facade, group: &str, member: &str, generation: i32) {
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_string(member.to_string()))
            .with_generation_id_or_member_epoch(generation)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(41)])]);
        crate::handlers::offset_commit::handle(f, &req, None).await;
    }

    /// The whole point of the API: the member's identity and its two opaque
    /// byte strings, unaltered.
    #[tokio::test]
    async fn a_live_member_is_described_with_its_bytes_untouched() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let (member, _) = stable_group(&f, "orders-consumer", "consumer-1").await;

        let resp = handle(&f, &request(&["orders-consumer"]), None).await;
        let g = got(&resp, "orders-consumer");
        assert_eq!(g.error_code, 0);
        assert_eq!(g.group_state.as_str(), "Stable");
        assert_eq!(g.protocol_type.as_str(), "consumer");
        assert_eq!(g.protocol_data.as_str(), "range");
        assert_eq!(g.members.len(), 1);

        let m = &g.members[0];
        assert_eq!(m.member_id.as_str(), member);
        assert_eq!(m.client_id.as_str(), "consumer-1");
        assert_eq!(
            m.client_host.as_str(),
            HOST,
            "the peer address never reached the coordinator"
        );
        assert_eq!(&m.member_metadata[..], SUBSCRIPTION);
        assert_eq!(&m.member_assignment[..], ASSIGNMENT);
        assert_eq!(m.group_instance_id, None);
        // The sentinel, whatever the request asked for.
        assert_eq!(g.authorized_operations, AUTHORIZED_OPERATIONS_OMITTED);
        let no_flag = handle(
            &f,
            &request(&["orders-consumer"]).with_include_authorized_operations(false),
            None,
        )
        .await;
        assert_eq!(
            got(&no_flag, "orders-consumer").authorized_operations,
            AUTHORIZED_OPERATIONS_OMITTED
        );
    }

    /// MEASURED against apache/kafka:3.9.1: a group nobody has ever heard of is
    /// error 0 and `Dead`, not an error. A handler written from the name of the
    /// code would have answered GROUP_ID_NOT_FOUND.
    #[tokio::test]
    async fn a_group_nobody_ever_heard_of_is_dead_and_not_an_error() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &request(&["never-existed"]), None).await;
        let g = got(&resp, "never-existed");
        assert_eq!(g.error_code, 0);
        assert_eq!(g.group_state.as_str(), "Dead");
        assert_eq!(g.protocol_type.as_str(), "");
        assert!(g.members.is_empty());
    }

    /// ...and a group that has committed and has no members is `Empty` with the
    /// protocol type the index remembers — which is what distinguishes "your
    /// consumers are stopped" from "there is no such group".
    #[tokio::test]
    async fn a_stopped_group_is_empty_and_not_dead() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "dg3-stopped").await;
        assert_eq!(
            f.coordinator.live_groups(),
            0,
            "the fixture conjured a group"
        );

        let g = handle(&f, &request(&["dg3-stopped"]), None).await;
        let g = got(&g, "dg3-stopped");
        assert_eq!(g.error_code, 0);
        assert_eq!(g.group_state.as_str(), "Empty");
        assert!(g.members.is_empty());
    }

    /// A live group whose last member left keeps its protocol type in the
    /// answer, because the index kept it — the FSM deliberately does not.
    #[tokio::test]
    async fn an_emptied_group_still_reports_the_kind_of_group_it_is() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let (member, generation) = stable_group(&f, "dg4-consumer", "c").await;
        // As the MEMBER, not as a simple consumer: a generation -1 commit
        // underneath a live group is refused UNKNOWN_MEMBER_ID, which is
        // `check_commit` doing its job.
        commit_as(&f, "dg4-consumer", &member, generation).await;

        let leave = kafka_protocol::messages::LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("dg4-consumer")))
            .with_member_id(StrBytes::from_string(member));
        crate::handlers::leave_group::handle(&f, &leave).await;

        let resp = handle(&f, &request(&["dg4-consumer"]), None).await;
        let g = got(&resp, "dg4-consumer");
        assert_eq!(g.group_state.as_str(), "Empty");
        assert_eq!(
            g.protocol_type.as_str(),
            "consumer",
            "an emptied group forgot what kind of group it was"
        );
        assert!(g.members.is_empty());
    }

    /// Every refusal, one per group, and the good group beside them is still
    /// described: this API has no top-level error field, so a bad name must not
    /// take its neighbours with it.
    #[tokio::test]
    async fn a_refused_group_does_not_take_its_neighbours_with_it() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        stable_group(&f, "good", "c").await;
        let huge = "g".repeat(crate::coordinator::MAX_GROUP_ID_CHARS + 1);

        let resp = handle(&f, &request(&["good", "", &huge, "never"]), None).await;
        assert_eq!(got(&resp, "good").error_code, 0);
        assert_eq!(
            got(&resp, "").error_code,
            ResponseError::InvalidGroupId.code()
        );
        assert_eq!(
            got(&resp, &huge).error_code,
            ResponseError::InvalidGroupId.code()
        );
        assert_eq!(got(&resp, "never").error_code, 0);
        assert_eq!(got(&resp, "never").group_state.as_str(), "Dead");
        // ...and the answers line up with the request, name for name.
        let names: Vec<&str> = resp.groups.iter().map(|g| g.group_id.0.as_str()).collect();
        assert_eq!(names, ["good", "", huge.as_str(), "never"]);
        assert_eq!(
            f.coordinator.live_groups(),
            1,
            "a described group was conjured"
        );
    }

    /// An index that cannot be read is a retriable code and never `Dead`: the
    /// facade cannot tell a group that never existed from one whose consumers
    /// are stopped, and `Dead` for the second reads as "somebody deleted it".
    #[tokio::test]
    async fn an_unreadable_index_is_retriable_and_never_dead() {
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
            api.fail_kv(queen_error.clone());
            let resp = handle(&f, &request(&["stopped"]), None).await;
            assert_eq!(
                got(&resp, "stopped").error_code,
                kafka.code(),
                "{queen_error}"
            );
        }
    }

    /// Cluster mode: a node that does not coordinate the group answers the
    /// redirect rather than a plausible `Empty`, and spawns nothing.
    #[tokio::test]
    async fn a_non_owner_answers_not_coordinator() {
        const THREE: [(i32, &str, u16); 3] = [
            (1, "kafka-1.example.com", 9092),
            (2, "kafka-2.example.com", 9092),
            (3, "kafka-3.example.com", 9092),
        ];
        let (stranger, _) = clustered(&[("orders", 4)], &THREE, 1);
        let resp = handle(&stranger, &request(&["orders-consumer"]), None).await;
        assert_eq!(
            got(&resp, "orders-consumer").error_code,
            ResponseError::NotCoordinator.code()
        );
        assert_eq!(stranger.coordinator.live_groups(), 0);

        let (owner, _) = clustered(&[("orders", 4)], &THREE, 2);
        let resp = handle(&owner, &request(&["orders-consumer"]), None).await;
        assert_eq!(got(&resp, "orders-consumer").error_code, 0);
    }

    #[tokio::test]
    async fn one_request_describes_at_most_the_bound() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let names: Vec<String> = (0..MAX_DESCRIBED_GROUPS + 3)
            .map(|i| format!("g{i:05}"))
            .collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        let resp = handle(&f, &request(&refs), None).await;
        assert_eq!(resp.groups.len(), names.len());
        for g in resp.groups.iter().skip(MAX_DESCRIBED_GROUPS) {
            assert_eq!(g.error_code, ResponseError::CoordinatorNotAvailable.code());
        }
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::DescribeGroups as i16)
            .expect("DescribeGroups is advertised");
        assert!(
            row.min >= DescribeGroupsRequest::VERSIONS.min
                && row.max <= DescribeGroupsRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let (f, _) = facade_and_queen(&[("orders", 4)]);
            stable_group(&f, "orders-consumer", "c").await;

            let mut wire = BytesMut::new();
            // `include_authorized_operations` is v3's, and the encoder refuses a
            // field set on a version that does not carry it — which is what
            // makes this walk a check on the window and not only on the handler.
            request(&["orders-consumer"])
                .with_include_authorized_operations(version >= 3)
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = DescribeGroupsRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = DescribeGroupsResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            let g = got(&back, "orders-consumer");
            assert_eq!(g.error_code, 0, "v{version}");
            assert_eq!(g.group_state.as_str(), "Stable", "v{version}");
            assert_eq!(
                &g.members[0].member_assignment[..],
                ASSIGNMENT,
                "v{version}"
            );
            assert_eq!(g.members[0].client_host.as_str(), HOST, "v{version}");
            // `authorized_operations` is a v3 field; below it the wire carries
            // none and the decoder answers its own default, which is the same
            // sentinel.
            assert_eq!(
                g.authorized_operations, AUTHORIZED_OPERATIONS_OMITTED,
                "v{version}"
            );
        }
    }
}
