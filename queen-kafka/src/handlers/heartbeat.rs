//! Heartbeat — proof of life, and the channel a rebalance is announced on.
//!
//! A consumer sends this every `heartbeat.interval.ms` (3 seconds by default)
//! between polls, and its ERROR CODE is the only way the group tells it
//! anything. REBALANCE_IN_PROGRESS is how a member that caused nothing learns
//! that somebody else joined or died and it must rejoin; ILLEGAL_GENERATION and
//! UNKNOWN_MEMBER_ID are how it learns it has been left behind. A facade that
//! answered 0 to everything would have a group that forms once and never
//! changes again.
//!
//! All of that is [`crate::coordinator`]'s decision. This file is the
//! translation, and it is the shortest one in the milestone.

use kafka_protocol::messages::{HeartbeatRequest, HeartbeatResponse};

use crate::Facade;

/// Handle one Heartbeat.
pub async fn handle(facade: &Facade, req: &HeartbeatRequest) -> HeartbeatResponse {
    let group = req.group_id.0.as_str();
    let error = if let Some(e) = crate::coordinator::invalid_group_id(group) {
        Some(e)
    } else {
        facade
            .coordinator
            .heartbeat(group, req.member_id.as_str(), req.generation_id)
            .await
    };
    HeartbeatResponse::default().with_error_code(error.map_or(0, |e| e.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use crate::Facade;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::error::ResponseError;
    use kafka_protocol::messages::{GroupId, JoinGroupRequest};
    use kafka_protocol::protocol::{Decodable, Encodable, Message, StrBytes};

    fn request(group: &str, member: &str, generation: i32) -> HeartbeatRequest {
        HeartbeatRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_string(member.to_string()))
            .with_generation_id(generation)
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

    #[tokio::test]
    async fn a_member_of_the_current_generation_heartbeats_clean() {
        let f = facade(&[]);
        let (member, generation) = one_member_group(&f, "orders-consumer").await;
        let resp = handle(&f, &request("orders-consumer", &member, generation)).await;
        assert_eq!(resp.error_code, 0);
    }

    #[tokio::test]
    async fn a_stale_or_unknown_member_is_told_which_it_is() {
        let f = facade(&[]);
        let (member, generation) = one_member_group(&f, "orders-consumer").await;

        let stale = handle(&f, &request("orders-consumer", &member, generation - 1)).await;
        assert_eq!(stale.error_code, ResponseError::IllegalGeneration.code());

        let stranger = handle(&f, &request("orders-consumer", "nobody", generation)).await;
        assert_eq!(stranger.error_code, ResponseError::UnknownMemberId.code());

        // A group this coordinator has never heard of: the member cannot be
        // one of its, and rejoining is the answer.
        let unknown = handle(&f, &request("never-seen", "someone", 1)).await;
        assert_eq!(unknown.error_code, ResponseError::UnknownMemberId.code());
    }

    #[tokio::test]
    async fn an_empty_group_id_is_refused() {
        let f = facade(&[]);
        assert_eq!(
            handle(&f, &request("", "someone", 1)).await.error_code,
            ResponseError::InvalidGroupId.code()
        );
        assert_eq!(f.coordinator.live_groups(), 0);
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let f = facade(&[]);
        let (member, generation) = one_member_group(&f, "g").await;
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::Heartbeat as i16)
            .expect("Heartbeat is advertised");
        assert!(
            row.min >= HeartbeatRequest::VERSIONS.min && row.max <= HeartbeatRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let mut wire = BytesMut::new();
            request("g", &member, generation)
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = HeartbeatRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = HeartbeatResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(back.error_code, 0, "v{version}");
        }
    }
}
