//! One module per Kafka API. A handler owns the semantics of its API and
//! nothing else: it takes the decoded request and returns the response body
//! together with the version that body must be ENCODED at (normally the request
//! version — see `api_versions` for the one case where it is not). The framing,
//! the headers and the dispatch live in `conn`.
//!
//! ## The error codes, in one table
//!
//! `compat/ERRORS.md` is the inventory: every non-zero code each API here can
//! put on the wire, where it comes from, and whether a client retries it. It is
//! the M6 error-code audit written down, and the thing to read before adding a
//! code — a code that reads as more precise but is not on the CLOSED set the
//! client accepts for that API ends the application rather than making it
//! recover. The rule that came out of the audit lives in
//! [`metadata::not_a_topic_here`].

pub mod acls;
pub mod add_offsets_to_txn;
pub mod add_partitions_to_txn;
pub mod alter_configs;
pub mod api_versions;
pub mod create_partitions;
pub mod create_topics;
pub mod delete_groups;
pub mod delete_topics;
pub mod describe_configs;
pub mod describe_groups;
pub mod end_txn;
pub mod fetch;
pub mod find_coordinator;
pub mod heartbeat;
pub mod incremental_alter_configs;
pub mod init_producer_id;
pub mod join_group;
pub mod leave_group;
pub mod list_groups;
pub mod list_offsets;
pub mod metadata;
pub mod offset_commit;
pub mod offset_delete;
pub mod offset_fetch;
pub mod produce;
pub mod sasl_authenticate;
pub mod sasl_handshake;
pub mod sync_group;
pub mod txn_offset_commit;

/// Fixtures shared by the handler tests. One place to build a [`crate::Facade`]
/// so that a new field on it does not have to be invented eleven times, and so
/// the group knobs the tests want (an instant join window — the clock is not
/// what these tests are about) are set once.
#[cfg(test)]
pub mod testing {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::cluster::{self, Cluster};
    use crate::coordinator::{Coordinator, GroupConfig};
    use crate::queen::testing::FakeQueen;
    use crate::queen::QueenApi;
    use crate::txn::Txns;
    use crate::{Facade, Policy};

    /// A facade over a fake Queen holding `queues`, as `(name, live lanes)`.
    pub fn facade(queues: &[(&str, i64)]) -> Facade {
        with_queen(FakeQueen::with(queues))
    }

    /// The same, with the double kept so a test can inspect what was asked.
    pub fn facade_and_queen(queues: &[(&str, i64)]) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        (with_queen(Arc::clone(&api)), api)
    }

    fn with_queen(api: Arc<FakeQueen>) -> Facade {
        over(api as Arc<dyn QueenApi>, Policy::default())
    }

    /// A CLUSTERED facade: `nodes` are the live set as `(id, host, port)` and
    /// `me` is which of them this process is. The view is fresh, which is what
    /// a facade looks like from its boot call onwards.
    pub fn clustered(
        queues: &[(&str, i64)],
        nodes: &[(i32, &str, u16)],
        me: i32,
    ) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        let state = cluster::testing::state(nodes, me);
        let (host, port) = (state.me.host.clone(), state.me.port);
        let facade = build(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Policy::default(),
            Cluster::Enabled(state),
            host,
            port,
            Arc::new(Txns::default()),
        );
        (facade, api)
    }

    /// The full constructor, for the tests that care about the listener policy
    /// or about the lanes — everything else takes the helpers above.
    pub fn over(api: Arc<dyn QueenApi>, policy: Policy) -> Facade {
        build(
            api,
            policy,
            Cluster::Single,
            "kafka.example.com".to_string(),
            9092,
            Arc::new(Txns::default()),
        )
    }

    /// A facade whose transaction stage carries `limits` rather than the
    /// defaults — the only way a cap test can reach a ceiling without staging
    /// eight megabytes of fixture.
    pub fn facade_with_txn_limits(
        queues: &[(&str, i64)],
        limits: crate::txn::Limits,
    ) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        let facade = build(
            Arc::clone(&api) as Arc<dyn QueenApi>,
            Policy::default(),
            Cluster::Single,
            "kafka.example.com".to_string(),
            9092,
            Arc::new(Txns::new(limits)),
        );
        (facade, api)
    }

    fn build(
        api: Arc<dyn QueenApi>,
        policy: Policy,
        cluster: Cluster,
        advertised_host: String,
        advertised_port: u16,
        txns: Arc<Txns>,
    ) -> Facade {
        Facade::new(
            advertised_host,
            advertised_port,
            cluster,
            4,
            None,
            api,
            Coordinator::new(GroupConfig {
                // The join window is the coordinator's own subject and is
                // tested there, under a paused clock. A handler test asks a
                // different question — does this request become that answer —
                // and a three-second wait inside every one of them would be
                // three seconds of nothing.
                join_delay: Duration::ZERO,
                ..GroupConfig::default()
            }),
            txns,
            policy,
        )
    }
}

/// The cluster-mode ownership gate, across the six group-addressed APIs at
/// once.
///
/// It is one module rather than a test per handler because the property is a
/// property of the SET: five of them refuse at a non-owner and the sixth
/// deliberately does not, and a test that only ever looked at one of them would
/// not notice the day somebody made them agree.
#[cfg(test)]
mod gate {
    use super::testing::clustered;
    use crate::coordinator::NO_GENERATION;
    use crate::offsets;
    use bytes::Bytes;
    use kafka_protocol::error::ResponseError;
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::list_offsets_request::{ListOffsetsPartition, ListOffsetsTopic};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::offset_fetch_request::OffsetFetchRequestTopic;
    use kafka_protocol::messages::{
        GroupId, HeartbeatRequest, JoinGroupRequest, LeaveGroupRequest, ListOffsetsRequest,
        OffsetCommitRequest, OffsetFetchRequest, SyncGroupRequest, TopicName,
    };
    use kafka_protocol::protocol::StrBytes;

    const THREE: [(i32, &str, u16); 3] = [
        (1, "kafka-1.example.com", 9092),
        (2, "kafka-2.example.com", 9092),
        (3, "kafka-3.example.com", 9092),
    ];

    /// The pinned rendezvous table (cluster::rendezvous) puts this group on
    /// node 2, so nodes 1 and 3 are its non-owners.
    const GROUP: &str = "orders-consumer";
    const NOT_THE_OWNER: i32 = 1;

    fn group_id() -> GroupId {
        GroupId(StrBytes::from_static_str(GROUP))
    }

    /// Every group RPC a non-owner refuses is answered NOT_COORDINATOR — the
    /// code every client answers by re-running FindCoordinator — and none of
    /// them spawns a group actor on the way, which is what keeps a redirect
    /// from being a way to spend this facade's memory.
    #[tokio::test]
    async fn the_five_writes_are_refused_at_a_non_owner_without_spawning_anything() {
        let (f, api) = clustered(&[("orders", 4)], &THREE, NOT_THE_OWNER);
        let want = ResponseError::NotCoordinator.code();

        let join = JoinGroupRequest::default()
            .with_group_id(group_id())
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::new())]);
        assert_eq!(
            super::join_group::handle(&f, &join, 4, "c", "/127.0.0.1")
                .await
                .error_code,
            want,
            "JoinGroup"
        );

        let sync = SyncGroupRequest::default()
            .with_group_id(group_id())
            .with_member_id(StrBytes::from_static_str("m"))
            .with_generation_id(1);
        assert_eq!(
            super::sync_group::handle(&f, &sync).await.error_code,
            want,
            "SyncGroup"
        );

        let heartbeat = HeartbeatRequest::default()
            .with_group_id(group_id())
            .with_member_id(StrBytes::from_static_str("m"))
            .with_generation_id(1);
        assert_eq!(
            super::heartbeat::handle(&f, &heartbeat).await.error_code,
            want,
            "Heartbeat"
        );

        let leave = LeaveGroupRequest::default()
            .with_group_id(group_id())
            .with_member_id(StrBytes::from_static_str("m"));
        assert_eq!(
            super::leave_group::handle(&f, &leave).await.error_code,
            want,
            "LeaveGroup"
        );

        // ...including the SIMPLE consumer's commit (generation -1, no member
        // id). Exempting it would leave exactly the measured hole open: two
        // simple consumers on two nodes, last writer wins, nothing to catch it.
        let commit = OffsetCommitRequest::default()
            .with_group_id(group_id())
            .with_member_id(StrBytes::from_static_str(""))
            .with_generation_id_or_member_epoch(NO_GENERATION)
            .with_topics(vec![OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![OffsetCommitRequestPartition::default()
                    .with_partition_index(0)
                    .with_committed_offset(41)])]);
        let resp = super::offset_commit::handle(&f, &commit, None).await;
        assert_eq!(
            resp.topics[0].partitions[0].error_code, want,
            "OffsetCommit"
        );

        assert_eq!(
            f.coordinator.live_groups(),
            0,
            "a non-owner spawned a group actor for a group it refused"
        );
        assert!(
            api.kv_calls.lock().unwrap().is_empty(),
            "a refused commit reached Queen"
        );
    }

    /// ...and the reads are NOT gated. OffsetFetch answers the shared store the
    /// same way at every node, and gating it would break the `assign()`-based
    /// simple consumer, which never runs FindCoordinator at all.
    #[tokio::test]
    async fn offset_fetch_and_list_offsets_are_served_at_a_non_owner() {
        let (f, api) = clustered(&[("orders", 4)], &THREE, NOT_THE_OWNER);
        api.kv_seed(
            offsets::NAMESPACE,
            &offsets::key(GROUP, "orders", 0).unwrap(),
            serde_json::json!({"offset": 50, "metadata": "", "ts": 1}),
        );

        let fetch = OffsetFetchRequest::default()
            .with_group_id(group_id())
            .with_topics(Some(vec![OffsetFetchRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partition_indexes(vec![0])]));
        let resp = super::offset_fetch::handle(&f, &fetch, None).await;
        assert_eq!(resp.error_code, 0, "OffsetFetch was gated");
        assert_eq!(resp.topics[0].partitions[0].committed_offset, 50);

        let bounds = ListOffsetsRequest::default()
            .with_replica_id((-1).into())
            .with_isolation_level(0)
            .with_topics(vec![ListOffsetsTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders")))
                .with_partitions(vec![ListOffsetsPartition::default()
                    .with_partition_index(0)
                    .with_timestamp(-1)])]);
        let resp = super::list_offsets::handle(&f, &bounds, None).await;
        assert_eq!(
            resp.topics[0].partitions[0].error_code, 0,
            "ListOffsets was gated"
        );
    }

    /// The OWNER serves all six, which is what makes the refusals above a
    /// redirect rather than an outage.
    #[tokio::test]
    async fn the_owner_serves_the_group() {
        let (f, _) = clustered(&[("orders", 4)], &THREE, 2);
        let join = JoinGroupRequest::default()
            .with_group_id(group_id())
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::new())]);
        let minted = super::join_group::handle(&f, &join, 4, "c", "/127.0.0.1").await;
        assert_eq!(
            minted.error_code,
            ResponseError::MemberIdRequired.code(),
            "the owner refused the join"
        );
        assert_eq!(f.coordinator.live_groups(), 1);
    }
}
