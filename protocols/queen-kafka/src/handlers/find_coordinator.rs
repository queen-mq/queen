//! FindCoordinator — which broker owns this group's membership and offsets.
//!
//! In a real cluster the answer is a lookup: a group id hashes onto a partition
//! of `__consumer_offsets`, and the coordinator is that partition's leader.
//! With one facade there is one broker and it is this process, so the answer is
//! always node 0 at `QUEEN_KAFKA_ADVERTISED_ADDR` — the same node, host and
//! port `handlers::metadata` advertises, from the same two fields, so a client
//! that connects to the coordinator connects to the address it already has
//! open.
//!
//! That makes this the shortest handler in the crate, and it still cannot be
//! skipped: every group client sends it before its first JoinGroup and refuses
//! to proceed without an answer.
//!
//! ## In cluster mode it becomes the lookup it describes
//!
//! With `QUEEN_KAFKA_NODE_ID` set, the answer is the rendezvous owner of the
//! group over the live node set ([`crate::cluster`]) — with **error code 0
//! whether or not that owner is us**. That is the whole of Kafka's dance: this
//! request never refuses to name a live owner, and the five group-addressed
//! writes answer NOT_COORDINATOR at a node that is not it, which is how a
//! client gets redirected instead of served twice.
//!
//! The one refusal cluster mode adds is a STALE view: a node whose last
//! successful registry read is older than the TTL cannot tell who owns
//! anything, so it answers COORDINATOR_NOT_AVAILABLE — "I cannot tell you", the
//! code every client retries — rather than guessing itself.
//!
//! ## The TRANSACTION coordinator (`key_type` 1), and the 20 seconds it cost
//!
//! In single-node mode this process IS the transaction coordinator (M9,
//! [`crate::txn`]) and answers itself, exactly as it answers a group key.
//!
//! In CLUSTER mode there is none, and the refusal is deliberately FATAL rather
//! than retriable. That is a correction, and the reason is measured: this
//! handler used to answer COORDINATOR_NOT_AVAILABLE (15), which is RETRIABLE,
//! and the Java `FindCoordinatorHandler` answers a retriable code by
//! re-enqueueing the lookup. So `initTransactions()` looped on coordinator
//! discovery for the whole of `max.block.ms` and never sent InitProducerId at
//! all — the 20 seconds `compat/java-matrix/README.md` recorded, whose stated
//! mechanism is *"FindCoordinator succeeds, so the client keeps waiting on a
//! response it will never negotiate"*. Advertising key 22 does not fix it,
//! because the client never gets far enough to send key 22.
//!
//! TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) does fix it: it is fatal in the
//! Java client, so the call returns in milliseconds. It is also the same code
//! and the same sentence [`crate::handlers::init_producer_id`] gives a
//! transactional id under cluster mode, so a user meets ONE message about
//! transactions and not two.
//!
//! ## The other thing it says no to
//!
//! An EMPTY group id. Kafka reserves it — a consumer with `group.id=""` is a
//! configuration mistake, not a group — and INVALID_GROUP_ID is a permanent
//! error, so the client stops rather than retrying a name that will never work.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{FindCoordinatorRequest, FindCoordinatorResponse};
use kafka_protocol::protocol::StrBytes;

use crate::cluster::Owner;
use crate::handlers::metadata::SINGLE_NODE_ID;
use crate::Facade;

/// `key_type` 0: the group coordinator. v0 has no such field and means this.
const KEY_TYPE_GROUP: i8 = 0;
/// `key_type` 1: the transaction coordinator.
const KEY_TYPE_TRANSACTION: i8 = 1;

/// What a refusal reports where the address would be.
const NO_NODE: i32 = -1;

/// Handle one FindCoordinator request. Takes no I/O and no token: the answer is
/// configuration, plus — in cluster mode — the live set this process last read.
///
/// The order is fixed and every step of it matters:
///
///   1. an invalid group id, which is permanent and must not be handed an
///      address to send a doomed join to;
///   2. a transaction coordinator in cluster mode, which does not exist;
///   3. single mode, which answers self, byte for byte as it always has —
///      including for a transaction key, which this process now coordinates;
///   4. cluster mode with a stale view, which cannot answer;
///   5. cluster mode with a fresh view, which answers the OWNER — with error
///      code 0 whether or not the owner is this node.
pub fn handle(facade: &Facade, req: &FindCoordinatorRequest) -> FindCoordinatorResponse {
    let key = req.key.as_str();
    if let Some((error, message)) = refusal(key, req.key_type, facade.cluster.state().is_some()) {
        tracing::debug!(
            target: "kafka",
            key_type = req.key_type,
            error = ?error,
            "find coordinator refused"
        );
        return refused(error, message);
    }
    // A TRANSACTION key never reaches the group rendezvous: cluster mode has
    // already refused it above, and in single mode the answer is this process
    // without a hash — which is what `owner_of_group` answers for `Single` too,
    // so the two paths meet at the same arm rather than at the same number.
    let (node_id, host, port) =
        match facade.cluster.owner_of_group(key) {
            // Single mode reaches this arm too, and it is the same answer it has
            // always given: this process, at the address Metadata advertises.
            Owner::Us => (
                facade
                    .cluster
                    .state()
                    .map_or(SINGLE_NODE_ID, |state| state.me.id),
                facade.advertised_host.clone(),
                facade.advertised_port,
            ),
            Owner::Peer(node) => (node.id, node.host, node.port),
            Owner::Unknown => return refused(
                ResponseError::CoordinatorNotAvailable,
                "this facade's view of the cluster's live nodes is stale, so it cannot say which \
                 node coordinates this group. It is still serving produce and fetch; retry."
                    .to_string(),
            ),
        };
    FindCoordinatorResponse::default()
        .with_error_code(0)
        .with_node_id(node_id.into())
        .with_host(StrBytes::from_string(host))
        .with_port(i32::from(port))
}

/// A refusal, in the shape Kafka gives one: no node and no port, so a client
/// cannot mistake the answer for an address.
fn refused(error: ResponseError, message: String) -> FindCoordinatorResponse {
    FindCoordinatorResponse::default()
        .with_error_code(error.code())
        .with_error_message(Some(StrBytes::from_string(message)))
        .with_node_id(NO_NODE.into())
        .with_host(StrBytes::from_static_str(""))
        .with_port(NO_NODE)
}

/// Why this request cannot be answered with an address, if it cannot.
///
/// `clustered` is read from CONFIGURATION and not from the live view, and that
/// is the whole of the transaction gate: a cluster-mode deployment that happens
/// to have one live node must not serve transactions, because a node joining
/// would break them mid-flight. Deterministic beats opportunistic.
fn refusal(key: &str, key_type: i8, clustered: bool) -> Option<(ResponseError, String)> {
    match key_type {
        // The same rule the six group-addressed APIs apply, from the same
        // place: a name this facade will refuse to join must not be answered
        // here with an address to send the join to.
        KEY_TYPE_GROUP if crate::coordinator::invalid_group_id(key).is_some() => Some((
            ResponseError::InvalidGroupId,
            format!(
                "a group id must be between 1 and {} characters",
                crate::coordinator::MAX_GROUP_ID_CHARS
            ),
        )),
        KEY_TYPE_GROUP => None,
        // Single mode: this process is the transaction coordinator, and the
        // answer falls through to the same self-address a group key gets.
        KEY_TYPE_TRANSACTION if !clustered => None,
        // Cluster mode: FATAL, and see the module header for why a retriable
        // code here costs `max.block.ms` instead of a millisecond.
        KEY_TYPE_TRANSACTION => Some((
            ResponseError::TransactionalIdAuthorizationFailed,
            "queen-kafka serves transactions in single-node mode only, and QUEEN_KAFKA_NODE_ID is \
             set on this facade: a transaction's records are staged on the node that received the \
             produce, and its EndTxn arrives at the coordinator, which in a cluster is a different \
             process. Unset QUEEN_KAFKA_NODE_ID, or remove transactional.id from this producer."
                .to_string(),
        )),
        other => Some((
            ResponseError::InvalidRequest,
            format!("coordinator key type {other} is not a type this broker serves"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use bytes::BytesMut;
    use kafka_protocol::protocol::{Decodable, Encodable, Message};

    fn request(key: &str, key_type: i8) -> FindCoordinatorRequest {
        FindCoordinatorRequest::default()
            .with_key(StrBytes::from_string(key.to_string()))
            .with_key_type(key_type)
    }

    /// The whole point: the coordinator is this process, at the address every
    /// client already has.
    #[test]
    fn the_coordinator_is_this_facade() {
        let f = facade(&[]);
        let resp = handle(&f, &request("orders-consumer", KEY_TYPE_GROUP));
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.node_id.0, SINGLE_NODE_ID);
        assert_eq!(resp.host.as_str(), f.advertised_host);
        assert_eq!(resp.port, i32::from(f.advertised_port));
    }

    /// ...and it is the same broker Metadata advertises, which is what makes a
    /// client reuse the connection it already has instead of opening a second
    /// one to a name that might not resolve.
    #[tokio::test]
    async fn the_coordinator_is_the_broker_metadata_advertises() {
        use kafka_protocol::messages::MetadataRequest;

        let f = facade(&[("orders", 1)]);
        let metadata =
            crate::handlers::metadata::handle(&f, &MetadataRequest::default(), 9, None).await;
        let coordinator = handle(&f, &request("g", KEY_TYPE_GROUP));
        assert_eq!(metadata.brokers.len(), 1);
        assert_eq!(coordinator.node_id.0, metadata.brokers[0].node_id.0);
        assert_eq!(coordinator.host.as_str(), metadata.brokers[0].host.as_str());
        assert_eq!(coordinator.port, metadata.brokers[0].port);
    }

    #[test]
    fn an_empty_group_id_is_invalid_and_permanent() {
        let resp = handle(&facade(&[]), &request("", KEY_TYPE_GROUP));
        assert_eq!(resp.error_code, ResponseError::InvalidGroupId.code());
        assert_eq!(resp.node_id.0, NO_NODE);
        assert_eq!(resp.port, NO_NODE);
    }

    /// M9: in single-node mode this process IS the transaction coordinator, and
    /// it answers the same address it answers a group key with — which is what
    /// makes a transactional producer reuse the connection it already has.
    #[test]
    fn the_transaction_coordinator_is_this_facade_in_single_mode() {
        let f = facade(&[]);
        let resp = handle(&f, &request("txn-1", KEY_TYPE_TRANSACTION));
        assert_eq!(resp.error_code, 0);
        assert_eq!(resp.node_id.0, SINGLE_NODE_ID);
        assert_eq!(resp.host.as_str(), f.advertised_host);
        assert_eq!(resp.port, i32::from(f.advertised_port));
        // ...and it is the SAME answer a group gets, byte for byte.
        let group = handle(&f, &request("txn-1", KEY_TYPE_GROUP));
        assert_eq!(resp.node_id.0, group.node_id.0);
        assert_eq!(resp.host.as_str(), group.host.as_str());
        assert_eq!(resp.port, group.port);
    }

    #[test]
    fn a_key_type_this_broker_does_not_know_is_an_invalid_request() {
        let resp = handle(&facade(&[]), &request("whatever", 7));
        assert_eq!(resp.error_code, ResponseError::InvalidRequest.code());
    }

    /// Every advertised version encodes and decodes cleanly, both ways — and
    /// the fields a version does not have are not the ones the answer rests on.
    #[test]
    fn the_exchange_round_trips_at_every_advertised_version() {
        let f = facade(&[]);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::FindCoordinator as i16)
            .expect("FindCoordinator is advertised");
        assert!(
            row.min >= FindCoordinatorRequest::VERSIONS.min
                && row.max <= FindCoordinatorRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let mut wire = BytesMut::new();
            request("orders-consumer", KEY_TYPE_GROUP)
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = FindCoordinatorRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded);
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = FindCoordinatorResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            assert_eq!(back.error_code, 0, "v{version}");
            assert_eq!(back.host.as_str(), f.advertised_host, "v{version}");
            assert_eq!(back.port, i32::from(f.advertised_port), "v{version}");
        }
    }

    // ------------------------------------------------------------- clustered

    const THREE: [(i32, &str, u16); 3] = [
        (1, "kafka-1.example.com", 9092),
        (2, "kafka-2.example.com", 9092),
        (3, "kafka-3.example.com", 9092),
    ];

    /// THE fix for "every FindCoordinator answers self": all three nodes name
    /// the SAME coordinator for a group, and it is a real address of a live
    /// node — with error code 0 from every one of them, including the two it is
    /// not. A client that asks any node gets the same answer, which is what
    /// makes the redirect converge instead of ping-pong.
    #[tokio::test]
    async fn every_node_answers_the_same_coordinator() {
        for group in ["orders-consumer", "svc.billing", "g"] {
            let mut answers = Vec::new();
            for me in [1, 2, 3] {
                let (f, _) = crate::handlers::testing::clustered(&[], &THREE, me);
                let resp = handle(&f, &request(group, KEY_TYPE_GROUP));
                assert_eq!(resp.error_code, 0, "{group} at node {me}");
                answers.push((resp.node_id.0, resp.host.as_str().to_string(), resp.port));
            }
            assert!(
                answers.windows(2).all(|w| w[0] == w[1]),
                "{group}: {answers:?}"
            );
            // ...and the address is the one that node advertises, not this one's.
            let (id, host, port) = answers[0].clone();
            assert_eq!(host, format!("kafka-{id}.example.com"));
            assert_eq!(port, 9092);
        }
    }

    /// A node whose registry view has gone stale says "I cannot tell you"
    /// rather than guessing itself — the answer that would put two coordinators
    /// on one group.
    #[tokio::test(start_paused = true)]
    async fn a_stale_view_cannot_name_a_coordinator() {
        let (f, _) = crate::handlers::testing::clustered(&[], &THREE, 1);
        let ttl = f.cluster.state().unwrap().ttl;
        tokio::time::advance(ttl + std::time::Duration::from_secs(1)).await;

        let resp = handle(&f, &request("orders-consumer", KEY_TYPE_GROUP));
        assert_eq!(
            resp.error_code,
            ResponseError::CoordinatorNotAvailable.code()
        );
        assert_eq!(resp.node_id.0, NO_NODE);
        assert_eq!(resp.port, NO_NODE);
        assert!(resp
            .error_message
            .as_ref()
            .is_some_and(|m| m.as_str().contains("stale")));
    }

    /// THE fast-fail. In cluster mode a transaction key is refused with a code
    /// the Java client treats as FATAL, so `initTransactions()` raises in
    /// milliseconds instead of re-enqueueing the lookup for the whole of
    /// `max.block.ms` — the measured 20 seconds of
    /// `compat/java-matrix/README.md`. A retriable code here is the defect;
    /// this test is what stops one coming back.
    #[tokio::test]
    async fn a_clustered_facade_refuses_a_transaction_coordinator_fatally() {
        let (f, _) = crate::handlers::testing::clustered(&[], &THREE, 1);
        let resp = handle(&f, &request("txn-1", KEY_TYPE_TRANSACTION));
        assert_eq!(
            resp.error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code(),
            "a retriable code here costs the client max.block.ms"
        );
        assert!(!ResponseError::TransactionalIdAuthorizationFailed.is_retriable());
        assert!(resp
            .error_message
            .as_ref()
            .is_some_and(|m| m.as_str().contains("QUEEN_KAFKA_NODE_ID")));
        assert_eq!(resp.node_id.0, NO_NODE);
        assert_eq!(resp.port, NO_NODE);
    }

    /// ...and the gate is on CONFIGURATION, not on the live view: a clustered
    /// facade that is currently alone still refuses, because a node joining
    /// would break a transaction that was already staged.
    #[tokio::test]
    async fn a_clustered_facade_alone_still_refuses_transactions() {
        let (f, _) = crate::handlers::testing::clustered(&[], &[(1, "only.example.com", 9092)], 1);
        assert_eq!(
            handle(&f, &request("txn-1", KEY_TYPE_TRANSACTION)).error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code()
        );
        // A GROUP key at the same node is served: this refusal is about
        // transactions and about nothing else.
        assert_eq!(handle(&f, &request("g", KEY_TYPE_GROUP)).error_code, 0);
    }

    /// The refusals that come FIRST keep coming first: a clustered facade must
    /// not hand an address to a group id it would refuse to join.
    #[tokio::test]
    async fn the_existing_refusals_still_take_precedence_in_a_cluster() {
        let (f, _) = crate::handlers::testing::clustered(&[], &THREE, 2);
        assert_eq!(
            handle(&f, &request("", KEY_TYPE_GROUP)).error_code,
            ResponseError::InvalidGroupId.code()
        );
        assert_eq!(
            handle(&f, &request("txn-1", KEY_TYPE_TRANSACTION)).error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code()
        );
        assert_eq!(
            handle(&f, &request("whatever", 7)).error_code,
            ResponseError::InvalidRequest.code()
        );
    }
}
