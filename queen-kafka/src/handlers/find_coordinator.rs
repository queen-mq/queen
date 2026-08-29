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
//! ## The two things it says no to
//!
//! A TRANSACTION coordinator (`key_type` 1). There is none: transactions are
//! excluded by PLAN_QUEEN_KAFKA.md and `handlers::produce` refuses every shape
//! of one, so pointing a transactional producer at this process would only move
//! the failure to `InitProducerId` — an API this build does not advertise,
//! whose absence closes the connection. COORDINATOR_NOT_AVAILABLE is the answer
//! Kafka itself gives while a coordinator cannot be resolved, and a client's
//! response to it is to retry rather than to crash, which is the correct
//! behaviour for a facade that may one day grow one.
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
///   2. a transaction coordinator, which does not exist;
///   3. single mode, which answers self, byte for byte as it always has;
///   4. cluster mode with a stale view, which cannot answer;
///   5. cluster mode with a fresh view, which answers the OWNER — with error
///      code 0 whether or not the owner is this node.
pub fn handle(facade: &Facade, req: &FindCoordinatorRequest) -> FindCoordinatorResponse {
    let key = req.key.as_str();
    if let Some((error, message)) = refusal(key, req.key_type) {
        tracing::debug!(
            target: "kafka",
            key_type = req.key_type,
            error = ?error,
            "find coordinator refused"
        );
        return refused(error, message);
    }
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
fn refusal(key: &str, key_type: i8) -> Option<(ResponseError, String)> {
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
        KEY_TYPE_TRANSACTION => Some((
            ResponseError::CoordinatorNotAvailable,
            "this broker has no transaction coordinator: queen-kafka does not implement \
             transactions or exactly-once semantics"
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

    /// A transactional producer is told there is no transaction coordinator
    /// rather than being handed one that cannot honour a transaction.
    #[test]
    fn there_is_no_transaction_coordinator() {
        let resp = handle(&facade(&[]), &request("txn-1", KEY_TYPE_TRANSACTION));
        assert_eq!(
            resp.error_code,
            ResponseError::CoordinatorNotAvailable.code()
        );
        assert!(resp
            .error_message
            .as_ref()
            .is_some_and(|m| m.as_str().contains("transaction")));
        assert_eq!(resp.node_id.0, NO_NODE);
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
            ResponseError::CoordinatorNotAvailable.code()
        );
        assert_eq!(
            handle(&f, &request("whatever", 7)).error_code,
            ResponseError::InvalidRequest.code()
        );
    }
}
