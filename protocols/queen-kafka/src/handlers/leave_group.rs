//! LeaveGroup — a consumer saying goodbye, so the group does not wait out its
//! session timeout to find out.
//!
//! It is the difference between a rolling restart that reassigns partitions in
//! milliseconds and one that stalls for `session.timeout.ms` per instance, and
//! it is the whole of what a well-behaved client sends on shutdown.
//!
//! ## One member per request, because v3 is capped away
//!
//! From v3 a LeaveGroup carries a LIST of members to remove, and the reason it
//! does is static membership: an administrator removing a `group.instance.id`
//! that no longer exists. Static membership is out of scope
//! (`crate::coordinator`), the version cap in `crate::versions` keeps it that
//! way, and below v3 the request is exactly one member id — which is the shape
//! this handler and the coordinator both have.

use kafka_protocol::messages::{LeaveGroupRequest, LeaveGroupResponse};

use crate::Facade;

/// Handle one LeaveGroup.
pub async fn handle(facade: &Facade, req: &LeaveGroupRequest) -> LeaveGroupResponse {
    let group = req.group_id.0.as_str();
    // The same ownership guard as the other four group APIs
    // ([`crate::cluster`]). A leave at a non-owner is answered NOT_COORDINATOR
    // rather than silently accepted: accepting it would tell a departing
    // consumer its seat was freed while the owner still holds it, and the group
    // would wait out the session timeout after all.
    let error = if let Some(e) = crate::coordinator::invalid_group_id(group) {
        Some(e)
    } else if let Some(e) = facade.cluster.group_guard(group) {
        Some(e)
    } else {
        facade
            .coordinator
            .leave(group, req.member_id.as_str())
            .await
    };
    LeaveGroupResponse::default().with_error_code(error.map_or(0, |e| e.code()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::coordinator::group::State;
    use crate::handlers::testing::facade;
    use crate::Facade;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::error::ResponseError;
    use kafka_protocol::messages::{GroupId, JoinGroupRequest};
    use kafka_protocol::protocol::{Decodable, Encodable, Message, StrBytes};

    fn request(group: &str, member: &str) -> LeaveGroupRequest {
        LeaveGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_member_id(StrBytes::from_string(member.to_string()))
    }

    async fn one_member_group(f: &Facade, group: &str) -> String {
        use kafka_protocol::messages::join_group_request::JoinGroupRequestProtocol;
        let req = JoinGroupRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
            .with_protocol_type(StrBytes::from_static_str("consumer"))
            .with_session_timeout_ms(10_000)
            .with_rebalance_timeout_ms(60_000)
            .with_protocols(vec![JoinGroupRequestProtocol::default()
                .with_name(StrBytes::from_static_str("range"))
                .with_metadata(Bytes::new())]);
        let minted = crate::handlers::join_group::handle(f, &req, 4, "c", "/127.0.0.1").await;
        crate::handlers::join_group::handle(
            f,
            &req.clone().with_member_id(minted.member_id),
            4,
            "c",
            "/127.0.0.1",
        )
        .await
        .member_id
        .to_string()
    }

    /// The last member leaving empties the group — which is what makes the next
    /// consumer's join form a new one rather than inherit a dead generation.
    #[tokio::test]
    async fn the_last_member_leaving_empties_the_group() {
        let f = facade(&[]);
        let member = one_member_group(&f, "orders-consumer").await;
        let resp = handle(&f, &request("orders-consumer", &member)).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(
            f.coordinator
                .describe("orders-consumer")
                .await
                .unwrap()
                .state,
            State::Empty
        );
    }

    #[tokio::test]
    async fn leaving_twice_or_leaving_a_group_that_is_not_there_is_unknown() {
        let f = facade(&[]);
        let member = one_member_group(&f, "orders-consumer").await;
        handle(&f, &request("orders-consumer", &member)).await;

        let again = handle(&f, &request("orders-consumer", &member)).await;
        assert_eq!(again.error_code, ResponseError::UnknownMemberId.code());

        let stranger = handle(&f, &request("never-seen", "someone")).await;
        assert_eq!(stranger.error_code, ResponseError::UnknownMemberId.code());
    }

    #[tokio::test]
    async fn an_empty_group_id_is_refused() {
        let f = facade(&[]);
        assert_eq!(
            handle(&f, &request("", "someone")).await.error_code,
            ResponseError::InvalidGroupId.code()
        );
        assert_eq!(f.coordinator.live_groups(), 0);
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::LeaveGroup as i16)
            .expect("LeaveGroup is advertised");
        assert!(
            row.min >= LeaveGroupRequest::VERSIONS.min
                && row.max <= LeaveGroupRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let f = facade(&[]);
            let member = one_member_group(&f, "g").await;

            let mut wire = BytesMut::new();
            request("g", &member)
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = LeaveGroupRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = LeaveGroupResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(back.error_code, 0, "v{version}");
        }
    }
}
