//! JoinGroup — a consumer announcing itself, and the request that PARKS.
//!
//! It is the first half of every rebalance: a client sends the assignors it
//! speaks and the metadata each of them wants distributed, and the response
//! tells it the generation the group agreed on, which member id it is, who the
//! leader is — and, if it IS the leader, everybody's metadata so it can compute
//! the assignment.
//!
//! That response cannot be written when the request arrives, because the
//! generation it must carry does not exist until the join window closes. So the
//! handler waits, with its connection muted (conn.rs), exactly as a long-poll
//! Fetch does and exactly as Apache Kafka's own coordinator does. The wait is
//! bounded by the client's rebalance timeout, which is the number it sent in
//! this very request.
//!
//! Everything about the waiting, the window and the election is
//! [`crate::coordinator`]'s. This file is the translation either side of it:
//! the request into a [`JoinRequest`], the answer into the wire shape, and the
//! one thing the coordinator cannot know — that this is JoinGroup v4 or above,
//! where an empty member id is answered with a minted one instead of joining.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::{JoinGroupRequest, JoinGroupResponse};
use kafka_protocol::protocol::StrBytes;

use crate::coordinator::{JoinAnswer, JoinRequest, Protocol};
use crate::Facade;

/// The version from which an empty member id is answered MEMBER_ID_REQUIRED
/// rather than accepted (KIP-394). Below it, the same request joins outright
/// and the minted id comes back with the successful response.
const MEMBER_ID_REQUIRED_FROM: i16 = 4;

/// Handle one JoinGroup.
///
/// `client_id` is the connection's, from the request header — the coordinator
/// uses it to make a minted member id readable. `client_host` is the peer
/// address `conn::serve` accepted, which exists nowhere else in the facade and
/// which DescribeGroups answers as the member's HOST column: the first thing an
/// operator looks at when a partition is stuck on a member.
pub async fn handle(
    facade: &Facade,
    req: &JoinGroupRequest,
    api_version: i16,
    client_id: &str,
    client_host: &str,
) -> JoinGroupResponse {
    let group = req.group_id.0.as_str();
    if let Some(e) = crate::coordinator::invalid_group_id(group) {
        return refused(e, req.member_id.as_str());
    }
    // Cluster mode: a node that does not own this group answers NOT_COORDINATOR
    // rather than forming a second generation of it, and it does so BEFORE the
    // coordinator is touched — so no group actor is spawned here for a group
    // that lives on another node ([`crate::cluster`]). `None` in single mode,
    // without reading anything.
    if let Some(e) = facade.cluster.group_guard(group) {
        tracing::debug!(target: "kafka", group, error = ?e, "join refused: not this node's group");
        return refused(e, req.member_id.as_str());
    }
    let answer = facade
        .coordinator
        .join(
            group,
            JoinRequest {
                member_id: req.member_id.to_string(),
                client_id: client_id.to_string(),
                client_host: client_host.to_string(),
                protocol_type: req.protocol_type.to_string(),
                protocols: req
                    .protocols
                    .iter()
                    .map(|p| Protocol {
                        name: p.name.to_string(),
                        // Opaque, and cloned rather than copied: `Bytes` is
                        // refcounted, so the subscription a client sent is
                        // carried to the leader without being touched.
                        metadata: p.metadata.clone(),
                    })
                    .collect(),
                session_timeout_ms: req.session_timeout_ms,
                rebalance_timeout_ms: req.rebalance_timeout_ms,
                member_id_required: api_version >= MEMBER_ID_REQUIRED_FROM,
            },
        )
        .await;
    render(answer)
}

fn render(answer: JoinAnswer) -> JoinGroupResponse {
    JoinGroupResponse::default()
        .with_error_code(answer.error.map_or(0, |e| e.code()))
        .with_generation_id(answer.generation)
        // Encoded from v7 up, which is above this facade's window — set anyway,
        // because a field left wrong is only invisible until the window moves.
        .with_protocol_type(answer.protocol_type.map(StrBytes::from_string))
        // `Some("")` and not `None` below v7: the field is a non-nullable
        // string there, and a client decoding a null where it expects a string
        // fails the whole response rather than the request.
        .with_protocol_name(Some(
            answer
                .protocol_name
                .map(StrBytes::from_string)
                .unwrap_or_default(),
        ))
        .with_leader(StrBytes::from_string(answer.leader))
        .with_member_id(StrBytes::from_string(answer.member_id))
        .with_members(
            answer
                .members
                .into_iter()
                .map(|(id, metadata)| {
                    JoinGroupResponseMember::default()
                        .with_member_id(StrBytes::from_string(id))
                        .with_metadata(metadata)
                })
                .collect(),
        )
}

fn refused(error: ResponseError, member_id: &str) -> JoinGroupResponse {
    render(JoinAnswer::refused(error, member_id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
    use kafka_protocol::messages::GroupId;
    use kafka_protocol::protocol::{Decodable, Encodable, Message};

    /// The peer address the accept loop would have handed in, in Apache
    /// Kafka's own spelling.
    const HOST: &str = "/127.0.0.1";

    fn request(group: &str, member: &str) -> JoinGroupRequest {
        JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_string(member.to_string()))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"subscription"))])
    }

    /// The v4 round trip, on the wire: an empty member id comes back
    /// MEMBER_ID_REQUIRED with the id to use, and that id joins.
    #[tokio::test]
    async fn the_member_id_round_trip_happens_at_v4() {
        let f = facade(&[]);
        let minted = handle(&f, &request("orders-consumer", ""), 4, "kcat", HOST).await;
        assert_eq!(minted.error_code, ResponseError::MemberIdRequired.code());
        assert!(minted.member_id.as_str().starts_with("kcat-"));
        assert_eq!(minted.generation_id, -1);

        let joined = handle(
            &f,
            &request("orders-consumer", minted.member_id.as_str()),
            4,
            "kcat",
            HOST,
        )
        .await;
        assert_eq!(joined.error_code, 0);
        assert_eq!(joined.member_id, minted.member_id);
        assert_eq!(joined.generation_id, 1);
        assert_eq!(joined.leader, minted.member_id, "the only member leads");
        assert_eq!(joined.protocol_name.as_ref().unwrap().as_str(), "range");
        // The leader is handed its own metadata, verbatim.
        assert_eq!(joined.members.len(), 1);
        assert_eq!(&joined.members[0].metadata[..], b"subscription");
    }

    /// Below v4 the same empty id joins outright — one round trip, and the id
    /// arrives with the assignment.
    #[tokio::test]
    async fn an_empty_member_id_below_v4_joins_outright() {
        let f = facade(&[]);
        let joined = handle(&f, &request("orders-consumer", ""), 3, "kcat", HOST).await;
        assert_eq!(joined.error_code, 0);
        assert!(!joined.member_id.is_empty());
        assert_eq!(joined.generation_id, 1);
    }

    #[tokio::test]
    async fn an_empty_group_id_is_refused_without_a_coordinator() {
        let f = facade(&[]);
        let resp = handle(&f, &request("", ""), 4, "kcat", HOST).await;
        assert_eq!(resp.error_code, ResponseError::InvalidGroupId.code());
        assert_eq!(f.coordinator.live_groups(), 0, "a group was created");
    }

    /// A group id is a name, and the protocol does not say so: `GroupId` is a
    /// Kafka string, so at the non-flexible versions this facade advertises a
    /// client may send tens of kilobytes of one — and every copy of it (the
    /// registry key, the actor's state, every log line about the group) would
    /// be that long, for a group nobody asked for.
    #[tokio::test]
    async fn a_group_id_that_is_not_a_name_is_refused_without_a_coordinator() {
        let f = facade(&[]);
        let huge = "g".repeat(crate::coordinator::MAX_GROUP_ID_CHARS + 1);
        let resp = handle(&f, &request(&huge, ""), 4, "kcat", HOST).await;
        assert_eq!(resp.error_code, ResponseError::InvalidGroupId.code());
        assert_eq!(f.coordinator.live_groups(), 0, "a group was created");

        // The longest name that IS one still works, so the bound refuses only
        // what it means to.
        let longest = "g".repeat(crate::coordinator::MAX_GROUP_ID_CHARS);
        let resp = handle(&f, &request(&longest, ""), 4, "kcat", HOST).await;
        assert_eq!(resp.error_code, ResponseError::MemberIdRequired.code());
    }

    /// The client id names the member, and it is a string the client chooses:
    /// a member id that carried all of it would be an unbounded string in the
    /// group's state and in every line the coordinator logs about it.
    #[tokio::test]
    async fn a_huge_client_id_does_not_become_a_huge_member_id() {
        let f = facade(&[]);
        let minted = handle(
            &f,
            &request("orders-consumer", ""),
            4,
            &"c".repeat(40_000),
            HOST,
        )
        .await;
        assert_eq!(minted.error_code, ResponseError::MemberIdRequired.code());
        assert!(minted.member_id.len() < 200, "{}", minted.member_id.len());
    }

    /// A refusal still parses as a response at every version: the fields that
    /// are strings must be strings, not nulls, and the fields that name a
    /// generation must say there is none.
    #[tokio::test]
    async fn a_refusal_encodes_at_every_advertised_version() {
        let f = facade(&[]);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::JoinGroup as i16)
            .expect("JoinGroup is advertised");
        assert!(
            row.min >= JoinGroupRequest::VERSIONS.min && row.max <= JoinGroupRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let resp = handle(&f, &request("g", "invented"), version, "kcat", HOST).await;
            assert_eq!(
                resp.error_code,
                ResponseError::UnknownMemberId.code(),
                "v{version}"
            );
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = JoinGroupResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing bytes");
            assert_eq!(back.generation_id, -1, "v{version}");
            assert_eq!(back.member_id.as_str(), "invented", "v{version}");
        }
    }

    /// A successful join round-trips at every advertised version, roster
    /// included.
    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::JoinGroup as i16)
            .expect("JoinGroup is advertised");
        for version in row.min..=row.max {
            let f = facade(&[]);
            // At v4 and up the id has to be minted first; below it the empty id
            // joins straight away.
            let member = if version >= MEMBER_ID_REQUIRED_FROM {
                handle(&f, &request("g", ""), version, "c", HOST)
                    .await
                    .member_id
                    .to_string()
            } else {
                String::new()
            };

            let mut wire = BytesMut::new();
            request("g", &member)
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = JoinGroupRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, version, "c", HOST).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = JoinGroupResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            assert_eq!(back.error_code, 0, "v{version}");
            assert_eq!(back.generation_id, 1, "v{version}");
            assert_eq!(back.members.len(), 1, "v{version}");
            assert_eq!(&back.members[0].metadata[..], b"subscription", "v{version}");
        }
    }
}
