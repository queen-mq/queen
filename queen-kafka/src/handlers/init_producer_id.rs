//! InitProducerId (22) — the grant an idempotent producer opens with (M7 F3).
//!
//! This is the first request a stock Java producer sends, before a single
//! record: `enable.idempotence` has defaulted to true since 3.0, and its
//! `Sender` will not produce anything until a producer id is in hand. Until this
//! handler existed the facade advertised no support for key 22, and the measured
//! consequence (compat/CLIENT_MATRIX.md) was fatal on the FIRST send about
//! 400 ms in, with the producer parked in `FATAL_ERROR` for the life of the
//! bean — while librdkafka's whole family refused before any wire traffic at
//! all ("Idempotent producer not supported by any of the 1 connected
//! broker(s)"). One property had to be found and set before anything worked.
//!
//! ## The grant costs nothing, and that is the point
//!
//! This handler makes NO call to Queen. The connection is already
//! authenticated, so a grant cannot fail for infrastructure reasons — there is
//! no catalog to read, no push to make and nothing to be unavailable. That is
//! exactly the property the biggest onboarding papercut wants: the answer is a
//! number and a zero, written back on the same turn of the connection loop.
//!
//! The enforcement is the other half and lives in [`crate::idempotent`], which
//! is where the sequence window, its bounds and its honest caveat are.
//!
//! ## Transactions are refused here, in under a second
//!
//! A non-empty `transactional_id` is answered TRANSACTIONAL_ID_AUTHORIZATION_FAILED
//! (53) — the same code and the same sentence `handlers::produce` gives a
//! transactional id, so a user meets ONE message about transactions and not two.
//! In the Java client that is a fatal error out of `InitProducerIdHandler`, so
//! `initTransactions()` raises immediately instead of blocking for the whole of
//! `max.block.ms`. The campaign measured 20 s for that call before this handler
//! existed, and the 20 s was never a slow answer: it was the `Sender` holding a
//! request for which no node advertised support. Advertising the key is what
//! makes the refusal fast.
//!
//! ## Epochs, and why v3 is inside the advertised window
//!
//! v3 is KIP-360, where the request carries the producer's CURRENT
//! `(producer_id, epoch)` so a producer that met OUT_OF_ORDER_SEQUENCE_NUMBER
//! can ask for a bump and reset its own sequences rather than dying. Without
//! that version in the window, a sequence window this facade lost — a restart,
//! an evicted entry — would be a fatal `OutOfOrderSequenceException` in the Java
//! producer. With it, the same loss is a bump, a reset, and a producer that
//! keeps running. That is why the version is load-bearing rather than a nicety,
//! and it is measured rather than trusted: `compat/go/idempotent_test.go`
//! restarts the facade under a live producer.
//!
//! Apache Kafka's own non-transactional path answers a bump request with a
//! BRAND NEW producer id at epoch 0 (`TransactionCoordinator.handleInitProducerId`
//! blindly allocates when the transactional id is null). This facade answers the
//! SAME id at `epoch + 1` instead, and the difference is deliberate: the epoch is
//! what discriminates one producer session from the next in
//! [`crate::idempotent`]'s key, so bumping it keeps a producer's identity stable
//! across a recovery while still invalidating everything remembered under the
//! old epoch. Either answer satisfies every client — the Java client takes
//! whatever the response says (`TransactionManager.setProducerIdAndEpoch`) and
//! resets its sequences because it asked for a bump — and this one costs no
//! entropy per recovery.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::idempotent;
use crate::Facade;

/// Kafka's "I have no producer id yet", and what every v0-v2 request carries by
/// construction because the field does not exist below v3.
const NO_PRODUCER_ID: i64 = idempotent::NO_PRODUCER_ID;

/// The epoch a fresh grant starts at.
const FIRST_EPOCH: i16 = 0;

/// Handle one InitProducerId request.
///
/// Synchronous, and visibly so: there is nothing to await because there is
/// nothing to ask anyone. See the module header.
pub fn handle(facade: &Facade, req: &InitProducerIdRequest) -> InitProducerIdResponse {
    if let Some(id) =
        idempotent::transactional_id(req.transactional_id.as_ref().map(|id| id.0.as_str()))
    {
        tracing::warn!(
            target: "kafka",
            transactional_id = %id,
            "InitProducerId with a transactional id: transactions are out of scope"
        );
        // The same code the produce path answers a transactional id with. It is
        // fatal and final in every client — which is the kindness here: an
        // `initTransactions()` that raises in milliseconds is a better answer
        // than one that blocks for `max.block.ms` and then raises anyway.
        return refused(
            ResponseError::TransactionalIdAuthorizationFailed,
            "queen-kafka does not implement transactions, so it will not grant a producer id for \
             the transactional id",
        );
    }

    let asked = req.producer_id.0;
    if asked == NO_PRODUCER_ID {
        // The ordinary opening move: a producer with no identity yet. Every
        // v0-v2 request lands here too, because the field it would have asked
        // with does not exist below v3.
        let id = idempotent::new_producer_id();
        tracing::debug!(
            target: "kafka",
            producer_id = id,
            "granted a producer id to an idempotent producer"
        );
        return granted(id, FIRST_EPOCH);
    }

    // KIP-360's bump. The client is telling us the epoch it is on; a facade with
    // no transactions has nothing to fence it against and nothing to verify the
    // claim with, and there is nothing to gain by doubting it — the id is the
    // client's own, the state under it is the client's own, and the only thing a
    // wrong claim can cost is the client's own sequence window.
    if req.producer_epoch == i16::MAX {
        // Exhaustion, and Kafka's own rule for it: a fresh id at epoch 0. The
        // Java client does the same thing on its own side
        // (`TransactionManager.bumpIdempotentProducerEpoch` calls
        // `resetIdempotentProducerId` at `Short.MAX_VALUE`), so the two agree
        // without either having to know about the other.
        let id = idempotent::new_producer_id();
        tracing::info!(
            target: "kafka",
            previous = asked,
            producer_id = id,
            "an idempotent producer exhausted its epochs; granting a fresh producer id"
        );
        facade.producers.forget(&tenant(facade), asked);
        return granted(id, FIRST_EPOCH);
    }
    let epoch = req.producer_epoch + 1;
    // Everything remembered under the OLD epoch belongs to a session that no
    // longer exists: the client resets its own sequences on a bump
    // (`TransactionManager.resetSequenceNumbers`), so keeping the ranges could
    // only produce a wrong answer. The produce path also resets on a higher
    // epoch — this is the half that returns the memory now rather than waiting
    // for the LRU.
    facade.producers.forget(&tenant(facade), asked);
    tracing::info!(
        target: "kafka",
        producer_id = asked,
        epoch,
        "bumped an idempotent producer's epoch (KIP-360) and dropped its sequence window"
    );
    granted(asked, epoch)
}

/// The scope this connection's producer state is filed under: the same key the
/// coordinator and the catalog use, read synchronously because authentication
/// has already resolved it ([`crate::identity`]).
fn tenant(facade: &Facade) -> crate::identity::TenantKey {
    facade.catalog.tenant_key(facade.token())
}

fn granted(producer_id: i64, producer_epoch: i16) -> InitProducerIdResponse {
    InitProducerIdResponse::default()
        // Nothing was asked of Queen, so there is nothing to have been throttled
        // by. Zero here is a measurement and not a placeholder.
        .with_throttle_time_ms(0)
        .with_error_code(0)
        .with_producer_id(ProducerId(producer_id))
        .with_producer_epoch(producer_epoch)
}

/// A refusal carries the sentinels for the two fields it has no answer for,
/// which is what Kafka itself writes and what every client reads as "there is
/// no id here" rather than as an id of -1.
fn refused(error: ResponseError, why: &str) -> InitProducerIdResponse {
    tracing::debug!(target: "kafka", %why, code = error.code(), "refusing InitProducerId");
    InitProducerIdResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(error.code())
        .with_producer_id(ProducerId(NO_PRODUCER_ID))
        .with_producer_epoch(-1)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::facade;
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::protocol::StrBytes;

    fn request() -> InitProducerIdRequest {
        InitProducerIdRequest::default()
            .with_producer_id(ProducerId(NO_PRODUCER_ID))
            .with_producer_epoch(-1)
            .with_transaction_timeout_ms(60_000)
    }

    fn with_id(id: &str) -> InitProducerIdRequest {
        request()
            .with_transactional_id(Some(TransactionalId(StrBytes::from_string(id.to_string()))))
    }

    // --------------------------------------------------------------- the grant

    #[test]
    fn a_fresh_producer_is_granted_an_id_at_epoch_zero() {
        let f = facade(&[("orders", 4)]);
        let r = handle(&f, &request());
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_epoch, 0);
        assert!(r.producer_id.0 > 0, "producer id {:?}", r.producer_id);
        assert_eq!(r.throttle_time_ms, 0);
    }

    #[test]
    fn two_producers_are_granted_two_ids() {
        let f = facade(&[("orders", 4)]);
        let a = handle(&f, &request());
        let b = handle(&f, &request());
        assert_ne!(a.producer_id.0, b.producer_id.0);
    }

    /// v0-v2 have no `producer_id` field at all, so the decoded request carries
    /// the schema default. That has to read as "mint me one" and not as "bump
    /// producer -1".
    #[test]
    fn a_request_below_v3_is_a_fresh_grant() {
        let f = facade(&[("orders", 4)]);
        let r = handle(&f, &InitProducerIdRequest::default());
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_epoch, 0);
        assert!(r.producer_id.0 > 0);
    }

    // ---------------------------------------------------------------- the bump

    #[test]
    fn a_known_producer_id_is_answered_with_the_same_id_at_the_next_epoch() {
        let f = facade(&[("orders", 4)]);
        let r = handle(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(7),
        );
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_id.0, 4_242);
        assert_eq!(r.producer_epoch, 8);
    }

    /// The bump is a reset, and this is where the window is actually dropped —
    /// the property `idempotent::forget` exists for.
    #[test]
    fn a_bump_drops_the_sequence_window_of_that_producer() {
        use kafka_protocol::records::{BatchDecodeInfo, Compression, TimestampType};

        let f = facade(&[("orders", 4)]);
        let info = BatchDecodeInfo {
            record_count: 3,
            timestamp_type: TimestampType::Creation,
            min_offset: 0,
            min_timestamp: 0,
            base_sequence: 0,
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: 4_242,
            producer_epoch: 0,
            compression: Compression::None,
            version: 2,
        };
        let key = f.catalog.tenant_key(f.token());
        let verdict = f
            .producers
            .check(&key, "orders", 0, std::slice::from_ref(&info));
        let idempotent::Verdict::Accept(pending) = verdict else {
            panic!("expected an accept, got {verdict:?}");
        };
        f.producers
            .commit(&pending, 100, pending.records() as usize);
        assert_eq!(f.producers.tracked(), 1);

        handle(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(0),
        );
        assert_eq!(f.producers.tracked(), 0);
    }

    /// Kafka's exhaustion rule, and the Java client's own: at `Short.MAX_VALUE`
    /// the id is replaced rather than the epoch incremented into a negative.
    #[test]
    fn an_exhausted_epoch_mints_a_fresh_id_instead_of_overflowing() {
        let f = facade(&[("orders", 4)]);
        let r = handle(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(i16::MAX),
        );
        assert_eq!(r.error_code, 0);
        assert_ne!(r.producer_id.0, 4_242);
        assert!(r.producer_id.0 > 0);
        assert_eq!(r.producer_epoch, 0);
    }

    // ------------------------------------------------------------ transactions

    #[test]
    fn a_transactional_id_is_refused_with_the_transaction_code() {
        let f = facade(&[("orders", 4)]);
        let r = handle(&f, &with_id("tx-1"));
        assert_eq!(
            r.error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code()
        );
        assert_eq!(r.producer_id.0, NO_PRODUCER_ID);
        assert_eq!(r.producer_epoch, -1);
    }

    /// brod's encoder writes a null transactional id as `""`, and an empty id is
    /// not a transactional id anywhere in this facade. The one helper both sites
    /// use is what keeps produce and this handler from drifting apart.
    #[test]
    fn an_empty_transactional_id_is_a_plain_grant() {
        let f = facade(&[("orders", 4)]);
        let r = handle(&f, &with_id(""));
        assert_eq!(r.error_code, 0);
        assert!(r.producer_id.0 > 0);
    }

    #[test]
    fn only_a_non_empty_transactional_id_is_refused() {
        let f = facade(&[("orders", 4)]);
        for id in ["tx", " ", "my-app-tx", "0"] {
            assert_eq!(
                handle(&f, &with_id(id)).error_code,
                ResponseError::TransactionalIdAuthorizationFailed.code(),
                "transactional_id={id:?}"
            );
        }
    }

    // ---------------------------------------------------------------- scoping

    /// Two tenants bumping the same producer id must not reach each other's
    /// window. The id is 62 bits of entropy so this cannot happen by accident;
    /// the test is here because the day it does is the day records go missing.
    #[test]
    fn a_bump_only_forgets_the_calling_tenants_producer() {
        let f = facade(&[("orders", 4)]);
        let other = crate::identity::TenantKey::Tenant("globex".into());
        let info = kafka_protocol::records::BatchDecodeInfo {
            record_count: 3,
            timestamp_type: kafka_protocol::records::TimestampType::Creation,
            min_offset: 0,
            min_timestamp: 0,
            base_sequence: 0,
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: 4_242,
            producer_epoch: 0,
            compression: kafka_protocol::records::Compression::None,
            version: 2,
        };
        for key in [f.catalog.tenant_key(f.token()), other.clone()] {
            let idempotent::Verdict::Accept(p) =
                f.producers
                    .check(&key, "orders", 0, std::slice::from_ref(&info))
            else {
                panic!("expected an accept");
            };
            f.producers.commit(&p, 100, p.records() as usize);
        }
        assert_eq!(f.producers.tracked(), 2);
        handle(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(0),
        );
        // The other tenant's entry is untouched.
        assert_eq!(f.producers.tracked(), 1);
        assert_eq!(
            f.producers.check(&other, "orders", 0, &[info]),
            idempotent::Verdict::Duplicate(100)
        );
    }
}
