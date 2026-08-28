//! SyncGroup — the leader posts the assignment, everyone else collects theirs.
//!
//! The second half of a rebalance, and the second request that PARKS: a
//! follower's SyncGroup carries nothing and waits, and it is answered the
//! instant the leader's arrives with the assignments. The leader's own request
//! carries one entry per member and is answered with its own.
//!
//! ## The assignment is bytes, in and out
//!
//! What a client puts in an assignment — the partitions it is giving that
//! member, the user data its assignor round-trips, the generation its sticky
//! strategy remembers — is encoded by the assignor named in the JoinGroup
//! response, and this facade does not know any of them. So the bytes are stored
//! and handed on untouched. A coordinator that decoded an assignment to "check"
//! it would work against `range` and `roundrobin` and fail against the next
//! strategy somebody writes, which is the whole reason the protocol made it
//! opaque.

use kafka_protocol::messages::{SyncGroupRequest, SyncGroupResponse};

use crate::coordinator::{SyncAnswer, SyncRequest};
use crate::Facade;

/// Handle one SyncGroup.
pub async fn handle(facade: &Facade, req: &SyncGroupRequest) -> SyncGroupResponse {
    let group = req.group_id.0.as_str();
    if let Some(e) = crate::coordinator::invalid_group_id(group) {
        return render(SyncAnswer::refused(e));
    }
    let answer = facade
        .coordinator
        .sync(
            group,
            SyncRequest {
                member_id: req.member_id.to_string(),
                generation: req.generation_id,
                assignments: req
                    .assignments
                    .iter()
                    .map(|a| (a.member_id.to_string(), a.assignment.clone()))
                    .collect(),
            },
        )
        .await;
    render(answer)
}

fn render(answer: SyncAnswer) -> SyncGroupResponse {
    SyncGroupResponse::default()
        .with_error_code(answer.error.map_or(0, |e| e.code()))
        .with_assignment(answer.assignment)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use crate::Facade;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::error::ResponseError;
    use kafka_protocol::messages::sync_group_request::SyncGroupRequestAssignment;
    use kafka_protocol::messages::{GroupId, JoinGroupRequest};
    use kafka_protocol::protocol::{Decodable, Encodable, Message, StrBytes};

    fn request(group: &str, member: &str, generation: i32) -> SyncGroupRequest {
        SyncGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_string(member.to_string()))
            .with_generation_id(generation)
    }

    /// Join one member through the real handler, so the ids and the generation
    /// are the ones the protocol would have produced.
    async fn one_member_group(f: &Facade, group: &str) -> (String, i32) {
        use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::from_static(b"subscription"))]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c").await;
        let joined = crate::handlers::join_group::handle(
            f,
            &req.clone().with_member_id(minted.member_id),
            4,
            "c",
        )
        .await;
        assert_eq!(joined.error_code, 0);
        (joined.member_id.to_string(), joined.generation_id)
    }

    /// The leader's own assignment comes back to it, byte for byte.
    #[tokio::test]
    async fn the_leader_gets_its_own_slice_verbatim() {
        let f = facade(&[]);
        let (member, generation) = one_member_group(&f, "orders-consumer").await;
        let resp = handle(
            &f,
            &request("orders-consumer", &member, generation).with_assignments(vec![
                SyncGroupRequestAssignment::default()
                    .with_member_id(StrBytes::from_string(member.clone()))
                    .with_assignment(Bytes::from_static(b"\x00\x01opaque")),
            ]),
        )
        .await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(&resp.assignment[..], b"\x00\x01opaque");
    }

    #[tokio::test]
    async fn a_sync_is_fenced_by_member_and_generation() {
        let f = facade(&[]);
        let (member, generation) = one_member_group(&f, "orders-consumer").await;

        let stale = handle(&f, &request("orders-consumer", &member, generation - 1)).await;
        assert_eq!(stale.error_code, ResponseError::IllegalGeneration.code());
        assert!(stale.assignment.is_empty());

        let stranger = handle(&f, &request("orders-consumer", "nobody", generation)).await;
        assert_eq!(stranger.error_code, ResponseError::UnknownMemberId.code());
    }

    /// A group nobody has joined has no members, so a sync into it is a member
    /// the coordinator does not know — not a crash and not a new group.
    #[tokio::test]
    async fn a_sync_into_an_unknown_group_is_an_unknown_member() {
        let f = facade(&[]);
        let resp = handle(&f, &request("never-seen", "someone", 3)).await;
        assert_eq!(resp.error_code, ResponseError::UnknownMemberId.code());
    }

    #[tokio::test]
    async fn an_empty_group_id_is_refused() {
        let f = facade(&[]);
        let resp = handle(&f, &request("", "someone", 1)).await;
        assert_eq!(resp.error_code, ResponseError::InvalidGroupId.code());
        assert_eq!(f.coordinator.live_groups(), 0);
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::SyncGroup as i16)
            .expect("SyncGroup is advertised");
        assert!(
            row.min >= SyncGroupRequest::VERSIONS.min && row.max <= SyncGroupRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let f = facade(&[]);
            let (member, generation) = one_member_group(&f, "g").await;
            let req = request("g", &member, generation).with_assignments(vec![
                SyncGroupRequestAssignment::default()
                    .with_member_id(StrBytes::from_string(member.clone()))
                    .with_assignment(Bytes::from_static(b"slice")),
            ]);

            let mut wire = BytesMut::new();
            req.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = SyncGroupRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = SyncGroupResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");

            assert_eq!(back.error_code, 0, "v{version}");
            assert_eq!(&back.assignment[..], b"slice", "v{version}");
        }
    }
}
