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
//! ## The transactional branch (M9)
//!
//! A non-empty `transactional_id` is where a transaction's IDENTITY is claimed,
//! and the claim is one compare-and-set against Queen ([`crate::txn`]):
//!
//!   1. `putIfAbsent` a fresh `{pid, epoch: 0}` under `qk:txn:<id>`. Applied
//!      means this producer owns the id and the answer is `(pid, 0)`.
//!   2. A LOST claim carries the winner's value AND version in the same answer
//!      (024_kv.sql:1467-1471), so the bump costs no extra read: one `put`
//!      expecting that version, the same pid, `epoch + 1`. **That bump IS the
//!      fencing** — the previous producer's next Produce, AddPartitionsToTxn or
//!      EndTxn carries the old epoch and is refused, and its staged records can
//!      no longer be committed because the version its commit would `expect`
//!      has moved.
//!   3. Losing the bump as well is CONCURRENT_TRANSACTIONS (51), retriable.
//!      ONE retry and then the backoff is the client's — the rule
//!      024_kv.sql:585-587 imposes on every CAS in this product, and the same
//!      bound `cluster::fence` obeys.
//!
//! Cost: one KV round trip on a fresh id, two on a re-init, and
//! `initTransactions()` happens once per producer lifetime.
//!
//! **In CLUSTER mode transactions are refused**, with
//! TRANSACTIONAL_ID_AUTHORIZATION_FAILED (53) and the same sentence
//! `handlers::find_coordinator` gives — so a user meets ONE message about
//! transactions and not two. The reason is routing rather than fencing and it
//! is in [`crate::txn`]: `Produce` goes to the partition leader and `EndTxn` to
//! the coordinator, which in a cluster are different processes, so the stage
//! lands on one facade and the commit arrives at another.
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

use std::time::Duration;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{InitProducerIdRequest, InitProducerIdResponse, ProducerId};

use crate::idempotent;
use crate::queen::{self, KvOp};
use crate::txn;
use crate::Facade;

/// Kafka's "I have no producer id yet", and what every v0-v2 request carries by
/// construction because the field does not exist below v3.
const NO_PRODUCER_ID: i64 = idempotent::NO_PRODUCER_ID;

/// The epoch a fresh grant starts at.
const FIRST_EPOCH: i16 = 0;

/// Handle one InitProducerId request.
///
/// The NON-transactional path awaits nothing and is byte for byte what M7 F3
/// shipped: a producer id is minted from process state, so the grant cannot
/// fail for infrastructure reasons and cannot be slow. Only the transactional
/// branch talks to Queen, and it does so at most twice.
pub async fn handle(
    facade: &Facade,
    req: &InitProducerIdRequest,
    conn: txn::ConnId,
    token: Option<&str>,
) -> InitProducerIdResponse {
    if let Some(id) =
        idempotent::transactional_id(req.transactional_id.as_ref().map(|id| id.0.as_str()))
    {
        return transactional(facade, req, id, conn, token).await;
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

/// Claim, or take over, one `transactional.id`. See the module header.
async fn transactional(
    facade: &Facade,
    req: &InitProducerIdRequest,
    id: &str,
    conn: txn::ConnId,
    token: Option<&str>,
) -> InitProducerIdResponse {
    // THE cluster gate, read from CONFIGURATION and not from the live view: a
    // clustered deployment that happens to have one live node must not serve
    // transactions, because a node joining would break one already staged.
    if facade.cluster.state().is_some() {
        tracing::warn!(
            target: "kafka",
            transactional_id = %id,
            "InitProducerId with a transactional id on a clustered facade: transactions are \
             single-node only"
        );
        return refused(
            ResponseError::TransactionalIdAuthorizationFailed,
            "queen-kafka serves transactions in single-node mode only, and QUEEN_KAFKA_NODE_ID is \
             set on this facade",
        );
    }
    // The key is checked BEFORE anything is minted, so an id this facade could
    // not store leaves no state behind. Unlike a group's, it has no longer
    // partner key to be shorter than, so it is bounded on its own.
    let Some(key) = txn::key(id) else {
        return refused(
            ResponseError::InvalidRequest,
            "the transactional id is longer than the key column this facade stores it in",
        );
    };
    let limits = facade.txns.limits();
    // Kafka's own refusal, with Kafka's own code: a producer asking for more
    // than `transaction.max.timeout.ms` is told the number is too large rather
    // than silently given less. A NON-POSITIVE timeout is refused for the same
    // reason and is not a client any producer sends.
    let asked_ms = i64::from(req.transaction_timeout_ms);
    if asked_ms <= 0 || asked_ms > limits.max_timeout.as_millis() as i64 {
        return refused(
            ResponseError::InvalidTransactionTimeout,
            "transaction.timeout.ms is outside what this facade will hold a stage for \
             (QUEEN_KAFKA_TXN_MAX_TIMEOUT_MS)",
        );
    }
    let timeout = Duration::from_millis(asked_ms as u64);
    let tenant = tenant(facade);
    let node = facade
        .cluster
        .state()
        .map_or(crate::handlers::metadata::SINGLE_NODE_ID, |s| s.me.id);
    let incarnation = incarnation();
    let now = crate::offsets::now_millis();

    // Step 1: claim it, optimistically. One round trip on a fresh id.
    let pid = idempotent::new_producer_id();
    let claim = KvOp::put_if_absent(
        crate::offsets::NAMESPACE,
        &key,
        txn::marker(
            pid,
            FIRST_EPOCH,
            txn::Outcome::Aborted,
            0,
            node,
            incarnation,
            now,
        ),
    );
    let answer = match facade.queen.kv(std::slice::from_ref(&claim), token).await {
        Ok(mut answers) if !answers.is_empty() => answers.remove(0),
        Ok(_) => {
            return refused(
                ResponseError::CoordinatorNotAvailable,
                "the transaction store answered nothing for the claim",
            )
        }
        Err(e) => return unavailable(id, &e),
    };
    if answer.applied == Some(true) {
        return bind(
            facade,
            &tenant,
            id,
            pid,
            FIRST_EPOCH,
            answer.version,
            conn,
            timeout,
        );
    }

    // Step 2: somebody holds it. The loser already has the winner's value and
    // version, so the bump is one more write and never a read.
    let (held_pid, held_epoch) = match txn::read_marker(&answer.value) {
        Some((pid, epoch, _)) => (pid, epoch),
        // A row under this key that is not one of ours. Overwritten at the
        // version it holds rather than left in place: the alternative is a
        // producer permanently unable to use its own id because something once
        // wrote a value this facade cannot read.
        None => {
            tracing::warn!(
                target: "kafka",
                transactional_id = %id,
                "the transaction key holds a value this facade cannot read; replacing it"
            );
            (pid, -1)
        }
    };
    // Kafka's exhaustion rule and the Java client's own
    // (`TransactionManager.bumpIdempotentProducerEpoch` resets at
    // `Short.MAX_VALUE`): a fresh id at epoch 0 rather than an epoch that wraps
    // negative.
    let (next_pid, next_epoch) = if held_epoch == i16::MAX {
        (idempotent::new_producer_id(), FIRST_EPOCH)
    } else {
        (held_pid, held_epoch + 1)
    };
    let bump = KvOp::put_expecting(
        crate::offsets::NAMESPACE,
        &key,
        txn::marker(
            next_pid,
            next_epoch,
            txn::Outcome::Aborted,
            0,
            node,
            incarnation,
            now,
        ),
        answer.version,
    );
    let bumped = match facade.queen.kv(std::slice::from_ref(&bump), token).await {
        Ok(mut answers) if !answers.is_empty() => answers.remove(0),
        Ok(_) => {
            return refused(
                ResponseError::CoordinatorNotAvailable,
                "the transaction store answered nothing for the epoch bump",
            )
        }
        Err(e) => return unavailable(id, &e),
    };
    if bumped.applied != Some(true) {
        // A THIRD producer took the id between the two calls. One retry and
        // then the backoff is the client's — retrying here would be a CAS loop
        // in a request handler, which is what 024_kv.sql:585-587 forbids.
        return refused(
            ResponseError::ConcurrentTransactions,
            "another producer is claiming this transactional id right now",
        );
    }
    tracing::info!(
        target: "kafka",
        transactional_id = %id,
        producer_id = next_pid,
        epoch = next_epoch,
        "a transactional id was taken over; the previous producer is fenced"
    );
    bind(
        facade,
        &tenant,
        id,
        next_pid,
        next_epoch,
        bumped.version,
        conn,
        timeout,
    )
}

/// Install the binding and answer the grant.
///
/// The bind is what DROPS whatever the previous epoch had staged, and it is
/// belt and braces rather than the mechanism: the bump above has already made
/// that stage uncommittable, because the version its commit would `expect` has
/// moved. This returns the memory at the moment of fencing instead of at the
/// timeout sweep.
#[allow(clippy::too_many_arguments)]
fn bind(
    facade: &Facade,
    tenant: &crate::identity::TenantKey,
    id: &str,
    pid: i64,
    epoch: i16,
    version: i64,
    conn: txn::ConnId,
    timeout: Duration,
) -> InitProducerIdResponse {
    match facade
        .txns
        .bind(tenant, id, pid, epoch, version, conn, timeout)
    {
        Ok(()) => granted(pid, epoch),
        // The open-transaction cap. Retriable and literally true: another
        // transaction finishing makes room.
        Err(()) => refused(
            ResponseError::ConcurrentTransactions,
            "this facade is holding as many open transactions as \
             QUEEN_KAFKA_TXN_MAX_OPEN allows",
        ),
    }
}

/// A Queen failure on the claim path.
///
/// A 429 becomes CONCURRENT_TRANSACTIONS rather than a throttle, per
/// `throttle.rs`'s rule that the throttle belongs on the calls whose VOLUME is
/// what the cap is about — `initTransactions()` happens once per producer
/// lifetime. Everything else is COORDINATOR_NOT_AVAILABLE, which the Java
/// client's `InitProducerIdHandler` retries.
fn unavailable(id: &str, e: &queen::Error) -> InitProducerIdResponse {
    tracing::warn!(
        target: "kafka",
        transactional_id = %id,
        error = %e,
        "the transaction store could not be reached to claim a transactional id"
    );
    match e {
        queen::Error::Status { code: 429, .. } => refused(
            ResponseError::ConcurrentTransactions,
            "the transaction store is rate limited right now",
        ),
        _ => refused(
            ResponseError::CoordinatorNotAvailable,
            "the transaction store could not be reached",
        ),
    }
}

/// This process's incarnation token, drawn once.
///
/// Written into every `qk:txn:` row for the cluster follow-up and for operator
/// forensics — "which process last decided this transaction" is the first
/// question of any transaction incident, and it is unanswerable after the fact
/// if nothing wrote it down.
pub(crate) fn incarnation() -> &'static str {
    static TOKEN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    TOKEN.get_or_init(crate::cluster::new_incarnation)
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
    use crate::handlers::testing::{facade, facade_and_queen};
    use crate::txn::{Limits, TxnState};
    use kafka_protocol::messages::TransactionalId;
    use kafka_protocol::protocol::StrBytes;

    /// The connection every fixture runs on. Only the fencing tests care which
    /// one it is.
    const CONN: txn::ConnId = 1;

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

    async fn grant(f: &Facade, req: &InitProducerIdRequest) -> InitProducerIdResponse {
        handle(f, req, CONN, f.token()).await
    }

    // --------------------------------------------------------------- the grant

    #[tokio::test]
    async fn a_fresh_producer_is_granted_an_id_at_epoch_zero() {
        let f = facade(&[("orders", 4)]);
        let r = grant(&f, &request()).await;
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_epoch, 0);
        assert!(r.producer_id.0 > 0, "producer id {:?}", r.producer_id);
        assert_eq!(r.throttle_time_ms, 0);
    }

    #[tokio::test]
    async fn two_producers_are_granted_two_ids() {
        let f = facade(&[("orders", 4)]);
        let a = grant(&f, &request()).await;
        let b = grant(&f, &request()).await;
        assert_ne!(a.producer_id.0, b.producer_id.0);
    }

    /// v0-v2 have no `producer_id` field at all, so the decoded request carries
    /// the schema default. That has to read as "mint me one" and not as "bump
    /// producer -1".
    #[tokio::test]
    async fn a_request_below_v3_is_a_fresh_grant() {
        let f = facade(&[("orders", 4)]);
        let r = grant(&f, &InitProducerIdRequest::default()).await;
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_epoch, 0);
        assert!(r.producer_id.0 > 0);
    }

    /// The non-transactional path asks Queen NOTHING, and that is the property
    /// the papercut fix wants: the grant cannot fail for infrastructure reasons
    /// and cannot be slow. M9 added a transactional branch that does call, so
    /// this is asserted rather than assumed from here on.
    #[tokio::test]
    async fn an_idempotent_grant_makes_no_call_to_queen() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        grant(&f, &request()).await;
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    // ---------------------------------------------------------------- the bump

    #[tokio::test]
    async fn a_known_producer_id_is_answered_with_the_same_id_at_the_next_epoch() {
        let f = facade(&[("orders", 4)]);
        let r = grant(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(7),
        )
        .await;
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_id.0, 4_242);
        assert_eq!(r.producer_epoch, 8);
    }

    /// The bump is a reset, and this is where the window is actually dropped —
    /// the property `idempotent::forget` exists for.
    #[tokio::test]
    async fn a_bump_drops_the_sequence_window_of_that_producer() {
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

        grant(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(0),
        )
        .await;
        assert_eq!(f.producers.tracked(), 0);
    }

    /// Kafka's exhaustion rule, and the Java client's own: at `Short.MAX_VALUE`
    /// the id is replaced rather than the epoch incremented into a negative.
    #[tokio::test]
    async fn an_exhausted_epoch_mints_a_fresh_id_instead_of_overflowing() {
        let f = facade(&[("orders", 4)]);
        let r = grant(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(i16::MAX),
        )
        .await;
        assert_eq!(r.error_code, 0);
        assert_ne!(r.producer_id.0, 4_242);
        assert!(r.producer_id.0 > 0);
        assert_eq!(r.producer_epoch, 0);
    }

    // ------------------------------------------------------------ transactions

    /// The claim, and what it costs: ONE round trip on a fresh id, and a
    /// binding this facade can then stage records against.
    #[tokio::test]
    async fn a_fresh_transactional_id_is_claimed_in_one_round_trip() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let r = grant(&f, &with_id("tx-1")).await;
        assert_eq!(r.error_code, 0);
        assert_eq!(r.producer_epoch, 0);
        assert!(r.producer_id.0 > 0);
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);
        // ...and the claim is a putIfAbsent on the transaction's own key, kept
        // forever, because a TTL would expire between two transactions of a
        // slow producer and abort a legitimate commit.
        match &api.kv_calls.lock().unwrap()[0][0] {
            KvOp::Put {
                ns,
                key,
                forever,
                expect,
                required,
                ..
            } => {
                assert_eq!(ns, crate::offsets::NAMESPACE);
                assert_eq!(key, "qk:txn:tx-1");
                assert!(*forever);
                assert_eq!(*expect, Some(0));
                assert!(!*required, "a lost claim is a verdict, not an abort");
            }
            other => panic!("the claim is not a put: {other:?}"),
        }
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(
            f.txns
                .with(&tenant, "tx-1", r.producer_id.0, 0, |t| t.state)
                .unwrap(),
            TxnState::Empty
        );
    }

    /// THE fencing: a second producer taking the same id is granted `epoch + 1`
    /// on the SAME pid, in two round trips, and the first producer's binding is
    /// gone from this facade the moment it happens.
    #[tokio::test]
    async fn a_second_producer_takes_the_id_at_the_next_epoch() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let first = grant(&f, &with_id("tx-1")).await;
        let second = handle(&f, &with_id("tx-1"), 2, f.token()).await;
        assert_eq!(second.error_code, 0);
        assert_eq!(
            second.producer_id.0, first.producer_id.0,
            "the pid is stable"
        );
        assert_eq!(second.producer_epoch, 1);
        assert_eq!(
            api.kv_calls.lock().unwrap().len(),
            3,
            "one claim, one claim, one bump"
        );
        // The old epoch is fenced HERE, not merely at the commit.
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(
            f.txns
                .with(&tenant, "tx-1", first.producer_id.0, 0, |_| ())
                .unwrap_err(),
            txn::Fault::Fenced
        );
    }

    /// ...and the stage the fenced producer had built is dropped at the moment
    /// of fencing rather than at the timeout sweep.
    #[tokio::test]
    async fn taking_an_id_over_drops_what_the_previous_epoch_staged() {
        let f = facade(&[("orders", 4)]);
        let first = grant(&f, &with_id("tx-1")).await;
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .add_partitions(
                &tenant,
                "tx-1",
                first.producer_id.0,
                0,
                &[("orders".into(), 0)],
            )
            .unwrap();
        f.txns
            .stage_records(
                &tenant,
                "tx-1",
                first.producer_id.0,
                0,
                "orders",
                0,
                vec![crate::queen::PushItem {
                    queue: "orders".into(),
                    partition: "0".into(),
                    payload: serde_json::json!({}),
                }],
                512,
            )
            .unwrap()
            .unwrap();
        assert_eq!(f.txns.staged_bytes(), 512);
        handle(&f, &with_id("tx-1"), 2, f.token()).await;
        assert_eq!(f.txns.staged_bytes(), 0);
    }

    /// A THIRD producer racing between the claim and the bump is answered
    /// CONCURRENT_TRANSACTIONS: retriable, literally true, and NOT a CAS loop
    /// inside a request handler. ONE retry and then the backoff is the
    /// client's, which is the rule 024_kv.sql:585-587 imposes on every
    /// compare-and-set in this product.
    #[tokio::test]
    async fn losing_the_bump_as_well_is_a_concurrent_transaction() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        grant(&f, &with_id("tx-1")).await;
        // Nothing before the second producer's CLAIM; a third writer lands
        // before its BUMP, moving the version the bump expects.
        *api.kv_interpose.lock().unwrap() = [
            None,
            Some(KvOp::put(
                crate::offsets::NAMESPACE,
                "qk:txn:tx-1",
                serde_json::json!({"pid": 9, "epoch": 6, "state": "aborted", "seq": 0}),
            )),
        ]
        .into();
        let r = handle(&f, &with_id("tx-1"), 2, f.token()).await;
        assert_eq!(
            r.error_code,
            ResponseError::ConcurrentTransactions.code(),
            "a lost bump must hand the backoff to the client"
        );
        assert_eq!(r.producer_id.0, NO_PRODUCER_ID);
        // THREE calls for this producer at most — the claim and one bump — and
        // then it stops. A loop here would be a request handler spinning on a
        // contended key.
        assert_eq!(api.kv_calls.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn a_transactional_id_that_will_not_fit_is_refused_before_anything_is_minted() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let r = grant(&f, &with_id(&"a".repeat(600))).await;
        assert_eq!(r.error_code, ResponseError::InvalidRequest.code());
        assert_eq!(r.producer_id.0, NO_PRODUCER_ID);
        assert!(api.kv_calls.lock().unwrap().is_empty());
        assert_eq!(f.txns.len(), 0);
    }

    /// Kafka's own answer for a timeout above `transaction.max.timeout.ms`, and
    /// the same code, so a producer that meets it on a real broker meets it
    /// here.
    #[tokio::test]
    async fn a_timeout_above_the_cap_is_refused_with_kafkas_own_code() {
        let (f, api) = crate::handlers::testing::facade_with_txn_limits(
            &[("orders", 4)],
            Limits {
                max_timeout: Duration::from_secs(60),
                ..Limits::default()
            },
        );
        let r = handle(
            &f,
            &with_id("tx-1").with_transaction_timeout_ms(120_000),
            CONN,
            None,
        )
        .await;
        assert_eq!(
            r.error_code,
            ResponseError::InvalidTransactionTimeout.code()
        );
        assert!(api.kv_calls.lock().unwrap().is_empty());
        // ...and one inside the cap is granted.
        assert_eq!(
            handle(
                &f,
                &with_id("tx-1").with_transaction_timeout_ms(30_000),
                CONN,
                None
            )
            .await
            .error_code,
            0
        );
    }

    #[tokio::test]
    async fn the_open_transaction_cap_is_a_retriable_refusal() {
        let (f, _) = crate::handlers::testing::facade_with_txn_limits(
            &[("orders", 4)],
            Limits {
                max_open: 1,
                ..Limits::default()
            },
        );
        assert_eq!(handle(&f, &with_id("a"), CONN, None).await.error_code, 0);
        assert_eq!(
            handle(&f, &with_id("b"), CONN, None).await.error_code,
            ResponseError::ConcurrentTransactions.code()
        );
    }

    /// A Queen that cannot be reached is retriable, and the producer id is the
    /// sentinel rather than a number that was never claimed.
    #[tokio::test]
    async fn an_unreachable_store_is_answered_retriably() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        *api.kv_error.lock().unwrap() = Some(crate::queen::Error::Transport("down".into()));
        let r = grant(&f, &with_id("tx-1")).await;
        assert_eq!(r.error_code, ResponseError::CoordinatorNotAvailable.code());
        assert_eq!(r.producer_id.0, NO_PRODUCER_ID);
        assert_eq!(f.txns.len(), 0);
    }

    /// A 429 is CONCURRENT_TRANSACTIONS and NOT a throttle: the throttle
    /// belongs on the calls whose volume is what a rate cap is about, and
    /// `initTransactions()` happens once per producer lifetime.
    #[tokio::test]
    async fn a_rate_capped_store_is_not_a_throttle_here() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        *api.kv_error.lock().unwrap() = Some(crate::queen::Error::Status {
            code: 429,
            body: "slow down".into(),
            retry_after_ms: Some(5_000),
        });
        let r = grant(&f, &with_id("tx-1")).await;
        assert_eq!(r.error_code, ResponseError::ConcurrentTransactions.code());
        assert_eq!(r.throttle_time_ms, 0);
    }

    /// The cluster gate, and the ONE message a user meets: the same code
    /// `find_coordinator` answers, so the two cannot tell different stories.
    #[tokio::test]
    async fn a_clustered_facade_refuses_a_transactional_id() {
        let (f, api) = crate::handlers::testing::clustered(
            &[("orders", 4)],
            &[
                (1, "kafka-1.example.com", 9092),
                (2, "kafka-2.example.com", 9092),
            ],
            1,
        );
        let r = handle(&f, &with_id("tx-1"), CONN, None).await;
        assert_eq!(
            r.error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code()
        );
        assert_eq!(r.producer_id.0, NO_PRODUCER_ID);
        assert_eq!(r.producer_epoch, -1);
        assert!(api.kv_calls.lock().unwrap().is_empty());
        // The IDEMPOTENT producer is untouched by the gate: a clustered facade
        // still grants a plain producer id.
        assert_eq!(handle(&f, &request(), CONN, None).await.error_code, 0);
    }

    /// brod's encoder writes a null transactional id as `""`, and an empty id is
    /// not a transactional id anywhere in this facade. The one helper both sites
    /// use is what keeps produce and this handler from drifting apart.
    #[tokio::test]
    async fn an_empty_transactional_id_is_a_plain_grant() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let r = grant(&f, &with_id("")).await;
        assert_eq!(r.error_code, 0);
        assert!(r.producer_id.0 > 0);
        assert!(api.kv_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn only_a_non_empty_transactional_id_takes_the_transactional_path() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        for id in ["tx", " ", "my-app-tx", "0"] {
            assert_eq!(grant(&f, &with_id(id)).await.error_code, 0, "id={id:?}");
        }
        assert_eq!(api.kv_calls.lock().unwrap().len(), 4);
    }

    // ---------------------------------------------------------------- scoping

    /// Two tenants bumping the same producer id must not reach each other's
    /// window. The id is 62 bits of entropy so this cannot happen by accident;
    /// the test is here because the day it does is the day records go missing.
    #[tokio::test]
    async fn a_bump_only_forgets_the_calling_tenants_producer() {
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
        grant(
            &f,
            &request()
                .with_producer_id(ProducerId(4_242))
                .with_producer_epoch(0),
        )
        .await;
        // The other tenant's entry is untouched.
        assert_eq!(f.producers.tracked(), 1);
        assert_eq!(
            f.producers.check(&other, "orders", 0, &[info]),
            idempotent::Verdict::Duplicate(100)
        );
    }
}
