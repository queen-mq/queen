//! ListGroups (key 16), v0-v4 — the groups this tenant has, live or merely
//! stopped.
//!
//! ## Why merely advertising it is already a fix
//!
//! `Confluent.Kafka`'s `AdminClient.ListConsumerGroupsAsync` does not fail
//! against a broker that omits key 16: it aborts the PROCESS with a glibc
//! double-free before a request is ever written (CLIENT_MATRIX.md). librdkafka
//! resolves the API from the ApiVersions answer and walks off the end of the
//! array when it is not there. So the row in `crate::versions` is half the
//! value of this handler, and the acceptance check for it is a .NET process
//! that exits 0.
//!
//! ## The answer is TWO halves, and they are the facade's two halves
//!
//! A group's actor is reaped `GroupConfig::empty_reap` after its last member
//! leaves, so a registry-only listing shows nothing for a group whose consumers
//! are merely STOPPED — which is exactly the group an operator opens the tool to
//! look at. Kafka shows it, because `__consumer_offsets` is durable. So this
//! merges:
//!
//!   * **liveness**, from [`crate::coordinator`] — the state, and the protocol
//!     type the members agreed on; and
//!   * **existence**, from the durable index in Queen
//!     ([`crate::offsets::index_key`]) — every group of this tenant that has
//!     ever committed an offset, whether or not anything is running.
//!
//! A group in both takes the live state. A group only in the index is `Empty`
//! with the protocol type the index remembers, which is true: it exists, it has
//! committed offsets, and nobody is in it.
//!
//! ## Two tenant scopes, and both were already correct
//!
//! The registry is walked through [`crate::coordinator::Coordinator::live`],
//! which FILTERS by the connection's own scope rather than enumerating a
//! process-wide map — an enumeration would hand one tenant's admin tool
//! another's group ids. The index read carries the connection's bearer, which
//! Queen scopes by tenant on its own side. Neither needed anything new.
//!
//! ## In cluster mode, existence is shared and liveness is not
//!
//! The index is in Queen, so a listing from any node lists every group of the
//! tenant. The registry is one process's, so a group whose members are attached
//! to ANOTHER facade is not live here — and answering `Empty` for it would be a
//! plausible wrong answer, which is the one thing this codebase refuses to
//! produce. Such a group is answered `Unknown`, which is a real Kafka state
//! string meaning exactly that. With `QUEEN_KAFKA_NODE_ID` unset every group is
//! owned by this node, nothing is `Unknown`, and the bytes are what they would
//! have been without any of this.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{GroupId, ListGroupsRequest, ListGroupsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::cluster::Owner;
use crate::coordinator::group::State;
use crate::offsets;
use crate::Facade;

/// The version from which a group carries a STATE on the wire, and from which a
/// request may filter on one (KIP-518).
const STATES_FROM: i16 = 4;

/// How many groups one answer carries.
///
/// The same number as `coordinator::MAX_GROUPS`, and that is the argument for
/// it: past this many groups the facade is not coordinating them anyway. There
/// is no truncation flag on this API — a client cannot be told the list was cut
/// — which is precisely why the bound has to be one that cannot bind in a real
/// deployment, and why reaching it is a log line.
const MAX_LISTED_GROUPS: usize = 10_000;

/// One line per window when the listing bound binds, or when the durable half
/// could not be read. Both are conditions a tool repeats on a refresh timer.
static PARTIAL: crate::obs::Sampler = crate::obs::Sampler::new(60_000);

/// Kafka's own spellings. A client matches `states_filter` against these
/// literally, so a state spelled differently is a filter that silently answers
/// nothing — and `handlers::describe_groups` puts the same strings in its own
/// `group_state` field, which is why there is one function and not two.
pub(crate) fn state_name(state: State) -> &'static str {
    match state {
        State::Empty => "Empty",
        State::PreparingRebalance => "PreparingRebalance",
        State::CompletingRebalance => "CompletingRebalance",
        State::Stable => "Stable",
        State::Dead => "Dead",
    }
}

/// The state of a group this node does not coordinate. A real Kafka state
/// string, and the only honest one: this process cannot see that group's
/// members. See the module header.
const UNKNOWN_STATE: &str = "Unknown";

/// One group of the merged answer.
struct Listed {
    group: String,
    protocol_type: String,
    state: String,
}

pub async fn handle(
    facade: &Facade,
    req: &ListGroupsRequest,
    api_version: i16,
    token: Option<&str>,
) -> ListGroupsResponse {
    // The live half first, and without any I/O: it is a filtered read of a map
    // this process owns.
    let mut listed: Vec<Listed> = Vec::new();
    for group in facade.coordinator.live() {
        // A group whose actor reaped itself between the walk and the ask is not
        // an error: it is a group that just went away, and the durable half
        // below still has it if it ever committed.
        let Some(snapshot) = facade.coordinator.describe(&group).await else {
            continue;
        };
        listed.push(Listed {
            state: match facade.cluster.owner_of_group(&group) {
                Owner::Us => state_name(snapshot.state).to_string(),
                _ => UNKNOWN_STATE.to_string(),
            },
            protocol_type: snapshot.protocol_type.unwrap_or_default(),
            group,
        });
    }

    // ...then the durable half. A failure here is answered by SERVING WHAT WE
    // HAVE rather than by failing the call, with one exception: an
    // authorization failure is not a moment in time, and a client shown a
    // partial list with error 0 would take it for the whole one. Everything
    // else is Queen being unreachable this second, and a tool rendering a page
    // is better served by the live groups plus a log line than by an error.
    match offsets::list_index(facade.queen.as_ref(), token, MAX_LISTED_GROUPS).await {
        Ok((rows, truncated)) => {
            if truncated {
                if let Some(suppressed) = PARTIAL.tick_now() {
                    tracing::warn!(
                        target: "kafka",
                        max = MAX_LISTED_GROUPS,
                        suppressed,
                        "this tenant has more consumer groups than one ListGroups answer carries; \
                         the rest are not listed, and this API has no flag to say so"
                    );
                }
            }
            // Indexed by group id rather than scanned per row: both halves are
            // bounded by MAX_LISTED_GROUPS, and a linear scan of one inside a
            // loop over the other is that bound SQUARED — a hundred million
            // string comparisons on an interactive admin call.
            let live_at: std::collections::HashMap<String, usize> = listed
                .iter()
                .enumerate()
                .map(|(i, l)| (l.group.clone(), i))
                .collect();
            for (group, indexed) in rows {
                match live_at.get(&group).map(|i| &mut listed[*i]) {
                    // Live wins for the state. It wins for the protocol type
                    // too, EXCEPT when the group is empty: the FSM clears the
                    // protocol type when the last member leaves (so the next
                    // group under this id may be a different kind), and the
                    // index is what remembers what the last one was — which is
                    // also what Kafka reports for an empty group.
                    Some(live) => {
                        if live.protocol_type.is_empty() {
                            live.protocol_type = indexed.protocol_type;
                        }
                    }
                    None => listed.push(Listed {
                        group,
                        protocol_type: indexed.protocol_type,
                        // It exists and has committed offsets, and nothing is
                        // in it. That is Empty, and it is what Kafka answers
                        // for the same group.
                        state: state_name(State::Empty).to_string(),
                    }),
                }
            }
        }
        Err(e) => {
            let error = offsets::kafka_error(&e);
            if error == ResponseError::GroupAuthorizationFailed {
                tracing::warn!(
                    target: "kafka",
                    error = %e,
                    "ListGroups could not read the durable group index"
                );
                return ListGroupsResponse::default().with_error_code(error.code());
            }
            if let Some(suppressed) = PARTIAL.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    error = %e,
                    suppressed,
                    "ListGroups answered only the groups this facade holds members for: the \
                     durable index in Queen could not be read, so groups whose consumers are \
                     stopped are missing from the answer"
                );
            }
        }
    }

    // KIP-518's filter, applied and not ignored: a tool asking for `Stable`
    // must not be handed `Empty` groups. Below v4 the field does not exist on
    // the wire, so an empty filter is the only thing that can arrive and it
    // means "everything".
    if api_version >= STATES_FROM && !req.states_filter.is_empty() {
        listed.retain(|l| req.states_filter.iter().any(|w| w.as_str() == l.state));
    }
    // Sorted by group id: the two halves arrive in registry order (a HashMap's,
    // i.e. none) and byte order, and a listing that reshuffles itself between
    // two refreshes of the same page is a listing an operator cannot read.
    listed.sort_by(|a, b| a.group.cmp(&b.group));
    // ...and the bound applies to the MERGED list, not only to the index read:
    // the live half can add group ids the index has never seen (a group that
    // has joined and not yet committed), so a cap on one half alone is not a
    // cap. Sorted first, so what survives a truncation is stable between two
    // refreshes rather than whichever half happened to be walked.
    if listed.len() > MAX_LISTED_GROUPS {
        if let Some(suppressed) = PARTIAL.tick_now() {
            tracing::warn!(
                target: "kafka",
                max = MAX_LISTED_GROUPS,
                listed = listed.len(),
                suppressed,
                "this tenant has more consumer groups than one ListGroups answer carries; the \
                 rest are not listed, and this API has no flag on the wire to say so"
            );
        }
        listed.truncate(MAX_LISTED_GROUPS);
    }

    ListGroupsResponse::default()
        .with_error_code(0)
        .with_groups(
            listed
                .into_iter()
                .map(|l| {
                    ListedGroup::default()
                        .with_group_id(GroupId(StrBytes::from_string(l.group)))
                        .with_protocol_type(StrBytes::from_string(l.protocol_type))
                        // Encoded from v4 up and dropped below it by the
                        // encoder; set at every version anyway, because a field
                        // left wrong is only invisible until the window moves.
                        .with_group_state(StrBytes::from_string(l.state))
                })
                .collect(),
        )
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

    fn request(states: &[&str]) -> ListGroupsRequest {
        ListGroupsRequest::default().with_states_filter(
            states
                .iter()
                .map(|s| StrBytes::from_string((*s).to_string()))
                .collect(),
        )
    }

    /// `(group, state)` pairs of an answer, sorted, so an assertion reads as
    /// the thing it is about.
    fn seen(resp: &ListGroupsResponse) -> Vec<(String, String)> {
        resp.groups
            .iter()
            .map(|g| {
                (
                    g.group_id.0.as_str().to_string(),
                    g.group_state.as_str().to_string(),
                )
            })
            .collect()
    }

    /// A group with one live member that has joined AND synced, so its state is
    /// `Stable` — a group that has only joined is `CompletingRebalance`, which
    /// is a different answer and would make every state assertion below about
    /// the wrong thing.
    async fn live_group(f: &Facade, group: &str) -> String {
        let join = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"subscription"))]);
        let minted = crate::handlers::join_group::handle(f, &join, 4, "c", "/127.0.0.1").await;
        let member = minted.member_id.clone();
        let joined = crate::handlers::join_group::handle(
            f,
            &join.with_member_id(member.clone()),
            4,
            "c",
            "/127.0.0.1",
        )
        .await;
        let sync = SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(member.clone())
            .with_generation_id(joined.generation_id)
            .with_assignments(vec![SyncGroupRequestAssignment::default()
                .with_member_id(member.clone())
                .with_assignment(Bytes::from_static(b"slice"))]);
        crate::handlers::sync_group::handle(f, &sync).await;
        member.to_string()
    }

    /// A simple consumer's commit — no membership at all, which is what puts a
    /// group in the durable index and nowhere else.
    ///
    /// NOTE for whoever adds a test here: the index write is skipped for a
    /// (tenant, group) this PROCESS has already written one for
    /// (`offset_commit`'s seen-set, which is a `static`), and the test binary is
    /// one process. So every test below uses group ids of its own. Two tests
    /// sharing a name would leave the second one with a group that has offsets
    /// and no index row — a real state, but not the one the test is about.
    async fn commit(f: &Facade, group: &str, token: Option<&str>) {
        let req = OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(crate::coordinator::NO_GENERATION)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(41)])]);
        crate::handlers::offset_commit::handle(f, &req, token).await;
    }

    /// THE thing this API exists for: a group whose consumers are STOPPED is
    /// still listed, because its existence is in Queen and not in this process.
    #[tokio::test]
    async fn a_stopped_group_is_listed_beside_a_live_one() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        // `stopped` commits and never joins — the simple-consumer shape, and
        // also what a group that has been reaped looks like.
        commit(&f, "lg1-stopped", None).await;
        live_group(&f, "lg1-live").await;
        commit(&f, "lg1-live", None).await;

        let resp = handle(&f, &request(&[]), 4, None).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(
            seen(&resp),
            [
                ("lg1-live".to_string(), "Stable".to_string()),
                ("lg1-stopped".to_string(), "Empty".to_string()),
            ]
        );
        // The live group's protocol type comes from its members; the stopped
        // one's is the empty string a simple consumer has, and neither is
        // invented.
        let types: Vec<&str> = resp
            .groups
            .iter()
            .map(|g| g.protocol_type.as_str())
            .collect();
        assert_eq!(types, ["consumer", ""]);
    }

    /// KIP-518's filter is HONOURED. A tool asking for `Stable` must not be
    /// handed `Empty` groups, and a state nothing is in answers an empty list
    /// rather than an error — which is what Kafka 3.9.1 does with an unknown
    /// state string too.
    #[tokio::test]
    async fn the_state_filter_is_applied_and_not_ignored() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        commit(&f, "lg2-stopped", None).await;
        live_group(&f, "lg2-live").await;
        commit(&f, "lg2-live", None).await;

        assert_eq!(
            seen(&handle(&f, &request(&["Stable"]), 4, None).await),
            [("lg2-live".to_string(), "Stable".to_string())]
        );
        assert_eq!(
            seen(&handle(&f, &request(&["Empty"]), 4, None).await),
            [("lg2-stopped".to_string(), "Empty".to_string())]
        );
        assert_eq!(
            seen(&handle(&f, &request(&["Stable", "Empty"]), 4, None).await).len(),
            2
        );
        let nonsense = handle(&f, &request(&["Nonsense"]), 4, None).await;
        assert_eq!(nonsense.error_code, 0);
        assert!(nonsense.groups.is_empty());

        // Below v4 the field is not on the wire, so a filter that somehow
        // arrived is not applied: the client could not have sent it and cannot
        // read the state it would have filtered on.
        assert_eq!(
            handle(&f, &request(&["Stable"]), 3, None)
                .await
                .groups
                .len(),
            2
        );
    }

    /// One tenant's tool must not see another tenant's group ids — from either
    /// half. The registry is process-wide and the walk FILTERS it; the index is
    /// scoped by Queen against the bearer.
    #[tokio::test]
    async fn one_tenants_listing_is_only_its_own_groups() {
        let (root, api) = facade_and_queen(&[("orders", 4)]);
        api.answer_identity(Some("key-a"), "cluster-1");
        api.answer_identity(Some("key-b"), "cluster-2");
        let a = root.for_connection(None).authenticated_as("key-a");
        let b = root.for_connection(None).authenticated_as("key-b");
        a.verify("key-a").await.unwrap();
        b.verify("key-b").await.unwrap();

        live_group(&a, "a-only").await;
        live_group(&b, "b-only").await;
        assert_eq!(root.coordinator.live_groups(), 2, "the fixture is wrong");

        let listed_a = handle(&a, &request(&[]), 4, Some("key-a")).await;
        assert_eq!(
            listed_a
                .groups
                .iter()
                .map(|g| g.group_id.0.as_str())
                .collect::<Vec<_>>(),
            ["a-only"],
            "a tenant's listing carried another tenant's group id"
        );
        let listed_b = handle(&b, &request(&[]), 4, Some("key-b")).await;
        assert_eq!(
            listed_b
                .groups
                .iter()
                .map(|g| g.group_id.0.as_str())
                .collect::<Vec<_>>(),
            ["b-only"]
        );
    }

    /// A store that cannot be read does not empty the page: the live half is
    /// served, the failure is logged, and the client is not handed an error for
    /// a listing it can partly have. The one exception is authorization, which
    /// is not a moment in time.
    #[tokio::test]
    async fn a_failed_index_read_serves_the_live_half() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        live_group(&f, "lg4-live").await;

        api.fail_kv(Error::Transport("connection refused".into()));
        let resp = handle(&f, &request(&[]), 4, None).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(
            seen(&resp),
            [("lg4-live".to_string(), "Stable".to_string())],
            "a transient failure emptied the page"
        );

        api.fail_kv(Error::status(403, "forbidden"));
        let refused = handle(&f, &request(&[]), 4, None).await;
        assert_eq!(
            refused.error_code,
            ResponseError::GroupAuthorizationFailed.code()
        );
        assert!(
            refused.groups.is_empty(),
            "a refused listing carried groups anyway"
        );
    }

    /// Cluster mode: existence is shared, liveness is not. A group this node
    /// does not coordinate is still LISTED — the index is in Queen — and its
    /// state is `Unknown`, never the plausible `Empty`.
    #[tokio::test]
    async fn a_group_this_node_does_not_own_is_listed_as_unknown() {
        const THREE: [(i32, &str, u16); 3] = [
            (1, "kafka-1.example.com", 9092),
            (2, "kafka-2.example.com", 9092),
            (3, "kafka-3.example.com", 9092),
        ];
        // The pinned rendezvous table puts `orders-consumer` on node 2, so
        // node 2 owns it and node 1 does not.
        let (owner, _) = clustered(&[("orders", 4)], &THREE, 2);
        live_group(&owner, "orders-consumer").await;
        assert_eq!(
            seen(&handle(&owner, &request(&[]), 4, None).await),
            [("orders-consumer".to_string(), "Stable".to_string())]
        );

        // A facade that holds an actor for a group it does not own — which is
        // what a node that has just lost the group to a membership change is.
        let (stranger, _) = clustered(&[("orders", 4)], &THREE, 1);
        stranger
            .coordinator
            .join(
                "orders-consumer",
                crate::coordinator::JoinRequest {
                    member_id: String::new(),
                    client_id: "c".to_string(),
                    client_host: "/127.0.0.1".to_string(),
                    protocol_type: "consumer".to_string(),
                    protocols: vec![crate::coordinator::Protocol {
                        name: "range".to_string(),
                        metadata: Bytes::new(),
                    }],
                    session_timeout_ms: 10_000,
                    rebalance_timeout_ms: 60_000,
                    member_id_required: false,
                },
            )
            .await;
        assert_eq!(
            seen(&handle(&stranger, &request(&[]), 4, None).await),
            [("orders-consumer".to_string(), "Unknown".to_string())],
            "a node answered a state for a group whose members it cannot see"
        );
    }

    #[tokio::test]
    async fn an_empty_facade_lists_nothing_without_failing() {
        let (f, _) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &request(&[]), 4, None).await;
        assert_eq!(resp.error_code, 0);
        assert!(resp.groups.is_empty());
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::ListGroups as i16)
            .expect("ListGroups is advertised");
        assert!(
            row.min >= ListGroupsRequest::VERSIONS.min
                && row.max <= ListGroupsRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let (f, _) = facade_and_queen(&[("orders", 4)]);
            let group = format!("lg6-stopped-{version}");
            commit(&f, &group, None).await;

            let mut wire = BytesMut::new();
            request(&[])
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = ListGroupsRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, version, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = ListGroupsResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            assert_eq!(back.error_code, 0, "v{version}");
            assert_eq!(back.groups.len(), 1, "v{version}");
            assert_eq!(back.groups[0].group_id.0.as_str(), group, "v{version}");
            // The state is a v4 field: below it the wire carries none and the
            // decoder answers the empty string, which is the shape a v0-v3
            // client expects.
            let state = back.groups[0].group_state.as_str();
            if version >= STATES_FROM {
                assert_eq!(state, "Empty", "v{version}");
            } else {
                assert_eq!(state, "", "v{version}");
            }
        }
    }
}
