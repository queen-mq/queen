//! AddOffsetsToTxn (25) — the consumer group whose offsets ride in this
//! transaction.
//!
//! It is the first half of KIP-447's consume-transform-produce loop:
//! `sendOffsetsToTransaction(offsets, groupMetadata)` sends this, and then a
//! `TxnOffsetCommit` to the group coordinator. In Apache Kafka this exists so
//! the transaction coordinator can add the `__consumer_offsets` partition to
//! the transaction and put a marker in it at the end. Here there are no
//! markers and no `__consumer_offsets`: the offsets are KV writes that ride
//! **in the same Postgres transaction as the records** ([`crate::txn`]), which
//! is what makes the loop atomic without a second coordinator agreeing to
//! anything.
//!
//! So all this request does is register the group id, with no I/O. It is worth
//! keeping rather than answering blindly for two reasons: the group is what the
//! commit's `qk:groups:` index row is written for, and registering it is where
//! a SECOND group in one transaction is refused.
//!
//! ## One group per transaction, and it is a stated deviation
//!
//! Apache Kafka lets a transaction carry offsets for several groups. This one
//! does not, and the reason is arithmetic rather than taste: the offset budget
//! ([`crate::txn::MAX_TXN_OFFSETS`]) is `WIRE_KV_MAX_OPS − 1 fence − 1 group
//! index`, so a second group would silently shrink the number of partitions a
//! transaction can commit. A silent shrink is the failure mode this codebase
//! refuses; INVALID_TXN_STATE is loud, and the shape nobody uses.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{AddOffsetsToTxnRequest, AddOffsetsToTxnResponse};

use crate::idempotent;
use crate::txn;
use crate::Facade;

/// Handle one AddOffsetsToTxn. No I/O; see the module header.
pub fn handle(
    facade: &Facade,
    req: &AddOffsetsToTxnRequest,
    token: Option<&str>,
) -> AddOffsetsToTxnResponse {
    let group = req.group_id.0.as_str();
    // The same rule the six group-addressed APIs apply, from the same place: a
    // group id this facade would refuse to join must not be accepted into a
    // transaction whose commit would then write offsets under it.
    if let Some(e) = crate::coordinator::invalid_group_id(group) {
        return answer(e);
    }
    let Some(id) = idempotent::transactional_id(Some(req.transactional_id.0.as_str())) else {
        return answer(txn::Fault::Unknown.code());
    };
    let tenant = facade.catalog.tenant_key(token);
    match facade
        .txns
        .add_offsets(&tenant, id, req.producer_id.0, req.producer_epoch, group)
    {
        Ok(Ok(())) => AddOffsetsToTxnResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0),
        // A second, different group. The inner fault is deliberately the same
        // INVALID_TXN_STATE the outer ones use: from the client's side this is
        // "this transaction cannot continue", and its answer is to abort.
        Ok(Err(fault)) | Err(fault) => {
            tracing::debug!(
                target: "kafka",
                transactional_id = %id,
                group,
                fault = ?fault,
                "AddOffsetsToTxn refused"
            );
            answer(fault.code())
        }
    }
}

fn answer(error: ResponseError) -> AddOffsetsToTxnResponse {
    AddOffsetsToTxnResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error.code())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use crate::txn::TxnState;
    use kafka_protocol::messages::{GroupId, ProducerId, TransactionalId};
    use kafka_protocol::protocol::StrBytes;
    use std::time::Duration;

    const PID: i64 = 7;

    fn request(id: &str, pid: i64, epoch: i16, group: &str) -> AddOffsetsToTxnRequest {
        AddOffsetsToTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
            .with_producer_id(ProducerId(pid))
            .with_producer_epoch(epoch)
            .with_group_id(GroupId(StrBytes::from_string(group.to_string())))
    }

    fn bound(id: &str) -> Facade {
        let f = facade(&[("orders", 4)]);
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, id, PID, 0, 100, 1, Duration::from_secs(60))
            .unwrap();
        f
    }

    /// A consume-only EOS loop commits offsets and produces nothing, so this
    /// request legitimately OPENS a transaction that has no partitions.
    #[test]
    fn registering_a_group_opens_the_transaction() {
        let f = bound("tx");
        assert_eq!(handle(&f, &request("tx", PID, 0, "g"), None).error_code, 0);
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 0, |t| (t.state, t.group.clone()))
                .unwrap(),
            (TxnState::Open, Some("g".to_string()))
        );
    }

    #[test]
    fn the_same_group_twice_is_not_a_second_group() {
        let f = bound("tx");
        assert_eq!(handle(&f, &request("tx", PID, 0, "g"), None).error_code, 0);
        assert_eq!(handle(&f, &request("tx", PID, 0, "g"), None).error_code, 0);
    }

    /// The stated deviation: Kafka allows several groups in one transaction and
    /// this refuses the second, because the offset budget is derived from there
    /// being exactly one group-index operation in the bundle.
    #[test]
    fn a_second_group_in_one_transaction_is_refused() {
        let f = bound("tx");
        handle(&f, &request("tx", PID, 0, "g"), None);
        assert_eq!(
            handle(&f, &request("tx", PID, 0, "other"), None).error_code,
            ResponseError::InvalidTxnState.code()
        );
    }

    #[test]
    fn an_unknown_transactional_id_is_a_fatal_state_error() {
        let f = facade(&[("orders", 4)]);
        assert_eq!(
            handle(&f, &request("tx", PID, 0, "g"), None).error_code,
            ResponseError::InvalidTxnState.code()
        );
    }

    #[test]
    fn a_fenced_producer_cannot_add_a_group() {
        let f = bound("tx");
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, "tx", PID, 1, 101, 2, Duration::from_secs(60))
            .unwrap();
        assert_eq!(
            handle(&f, &request("tx", PID, 0, "g"), None).error_code,
            ResponseError::ProducerFenced.code()
        );
    }

    /// A group id this facade would refuse to join is refused here too, before
    /// the transaction can carry a commit under it.
    #[test]
    fn an_invalid_group_id_is_refused() {
        let f = bound("tx");
        assert_eq!(
            handle(&f, &request("tx", PID, 0, ""), None).error_code,
            ResponseError::InvalidGroupId.code()
        );
    }

    #[test]
    fn the_exchange_round_trips_at_every_advertised_version() {
        use bytes::BytesMut;
        use kafka_protocol::protocol::{Decodable, Encodable, Message};

        let f = facade(&[("orders", 4)]);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::AddOffsetsToTxn as i16)
            .expect("AddOffsetsToTxn is advertised");
        assert!(
            row.min >= AddOffsetsToTxnRequest::VERSIONS.min
                && row.max <= AddOffsetsToTxnRequest::VERSIONS.max
        );
        let tenant = f.catalog.tenant_key(f.token());

        for version in row.min..=row.max {
            // Its own id per version: a fixture reusing one would meet the
            // group the previous version registered and be answered a second
            // group rather than a first.
            let id = format!("tx-v{version}");
            f.txns
                .bind(&tenant, &id, PID, 0, 100, 1, Duration::from_secs(60))
                .unwrap();
            let mut wire = BytesMut::new();
            request(&id, PID, 0, "g")
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = AddOffsetsToTxnRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None);
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = AddOffsetsToTxnResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(back.error_code, 0, "v{version}");
        }
    }
}
