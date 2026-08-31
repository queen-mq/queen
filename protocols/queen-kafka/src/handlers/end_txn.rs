//! EndTxn (26) — the one request that writes a transaction.
//!
//! Everything before this staged; this is where the whole set becomes durable,
//! in **one** call to `POST /api/v1/transaction`, which is one Postgres
//! transaction. The bundle is:
//!
//! ```text
//!   operations: [ { type: "push", items: [ every staged record ] } ]
//!   kv:         [ 0: the qk:txn: fence, required, expecting our version
//!                 1: the group index row, when this transaction commits offsets
//!                 2..: the staged offsets, unconditional ]
//! ```
//!
//! **`required: true` at index 0 is the whole mechanism.** Without it a lost
//! precondition is a verdict that rolls nothing back and the records would land
//! anyway; with it, a fenced producer writes exactly zero records and zero
//! offsets, because `kv_apply_v1` raises 23514 and the bundle rolls back
//! (005_log_ack.sql). It is the cluster fence's mechanism verbatim
//! ([`crate::cluster::fence`]), pointed at a transaction instead of at a group.
//!
//! The records and the offsets are in ONE Postgres transaction, and that
//! sentence is the whole of atomic consume-transform-produce. No server change
//! was needed for it: `kv` has been a top-level rider of this route since
//! `PLAN_KV_TIMERS`.
//!
//! ## The verdict table, and the one rule that must not be got backwards
//!
//! | What came back | The answer | The stage |
//! | --- | --- | --- |
//! | 200 with results | `error_code = 0` | freed, version advanced |
//! | a lost precondition | PRODUCER_FENCED (90) | dropped |
//! | a transport failure, a 5xx, a 429 | COORDINATOR_NOT_AVAILABLE (15) | **KEPT** |
//!
//! The last row is the one to get right. Dropping the stage on a RETRIABLE
//! failure would turn the client's retry into a silent empty commit — a
//! transaction the application believes wrote its records and which wrote none.
//! There is no retry inside the facade either: EndTxn is not a CAS loop, and a
//! lost precondition here is a FENCE rather than a race, because in single-node
//! mode there is no legitimate second writer of `qk:txn:<id>`.
//!
//! ## Abort writes nothing, and says so before it tries to
//!
//! `committed = false` drops the stage and answers 0. The `state: "aborted"`
//! marker is then written best-effort with `required: false`: if it loses or
//! fails, the answer is still true, because the abort is a CLIENT-SIDE fact —
//! nothing was ever written. That is the asymmetry `cluster::registry` already
//! draws between a `required: true` write that must gate a bundle and a
//! `required: false` write that must not.
//!
//! An abort of a transaction this facade does not hold is also answered 0, and
//! that is not laxity: **a lost stage IS an aborted transaction**, because
//! nothing of it ever reached the log. A FENCED producer is still told it is
//! fenced, because that is a fact about the producer rather than about the
//! transaction.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::{EndTxnRequest, EndTxnResponse};

use crate::idempotent;
use crate::queen::{self, KvOp};
use crate::txn::{self, Fault};
use crate::{throttle, Facade};

/// Handle one EndTxn.
pub async fn handle(facade: &Facade, req: &EndTxnRequest, token: Option<&str>) -> EndTxnResponse {
    let Some(id) = idempotent::transactional_id(Some(req.transactional_id.0.as_str())) else {
        return refused(Fault::Unknown.code(), None);
    };
    let tenant = facade.catalog.tenant_key(token);
    let (pid, epoch) = (req.producer_id.0, req.producer_epoch);
    if req.committed {
        commit(facade, &tenant, id, pid, epoch, token).await
    } else {
        abort(facade, &tenant, id, pid, epoch, token).await
    }
}

/// `committed = true`: one bundle, and the verdict table of the module header.
async fn commit(
    facade: &Facade,
    tenant: &crate::identity::TenantKey,
    id: &str,
    pid: i64,
    epoch: i16,
    token: Option<&str>,
) -> EndTxnResponse {
    let bundle = match facade.txns.begin_commit(tenant, id, pid, epoch) {
        Ok(b) => b,
        Err(fault) => {
            tracing::warn!(
                target: "kafka",
                transactional_id = %id,
                fault = ?fault,
                "a transaction commit was refused; NOTHING was written for it"
            );
            return refused(fault.code(), None);
        }
    };
    // A transaction that staged nothing writes nothing, and there is no bundle
    // to send: the fence would be the only operation in it, and advancing a
    // version to record that nothing happened costs a round trip on the hot
    // path of a consume-only loop that filtered every record away.
    if bundle.items.is_empty() && bundle.offsets.is_empty() {
        facade.txns.commit_landed(tenant, id, bundle.version);
        return committed();
    }

    // The protocol type of the group, for the index row — `consumer` for a
    // consumer group and the empty string for a simple `assign()`-based one,
    // which is what Kafka reports and what `listConsumerGroups` accepts.
    let protocol_type = match bundle.group.as_deref() {
        Some(group) => facade
            .coordinator
            .describe(group)
            .await
            .and_then(|s| s.protocol_type)
            .unwrap_or_default(),
        None => String::new(),
    };
    let node = facade
        .cluster
        .state()
        .map_or(crate::handlers::metadata::SINGLE_NODE_ID, |s| s.me.id);
    let Some(ops) = bundle.kv_ops(
        id,
        pid,
        epoch,
        node,
        crate::handlers::init_producer_id::incarnation(),
        crate::offsets::now_millis(),
        &protocol_type,
    ) else {
        // Unreachable: InitProducerId refused a transactional id whose key does
        // not fit, and TxnOffsetCommit refused every offset key that does not.
        // Reported rather than assumed, and the stage is dropped because a
        // bundle that cannot be built cannot be retried into existence.
        tracing::error!(
            target: "kafka",
            transactional_id = %id,
            "a transaction bundle could not be built; its records were dropped"
        );
        facade.txns.discard(tenant, id);
        return refused(ResponseError::InvalidTxnState, None);
    };
    debug_assert!(ops.len() <= queen::WIRE_KV_MAX_OPS);

    let records = bundle.items.len();
    match facade.queen.transaction(&bundle.items, &ops, token).await {
        Ok(answers) => {
            // The fence is index 0 by construction and its answer is where the
            // next transaction's `expect` comes from.
            let version = match answers.first() {
                Some(a) if a.applied == Some(true) => a.version,
                // Unreachable: `required: true` turns a lost precondition into
                // an aborted transaction, which arrives as `Precondition`
                // below. Reported and never assumed — a fence that answered
                // anything else is a broker that changed under us — and the
                // stage is KEPT, because this facade cannot tell whether the
                // bundle landed.
                other => {
                    tracing::error!(
                        target: "kafka",
                        transactional_id = %id,
                        answer = ?other,
                        "the transaction fence answered something other than applied; the outcome \
                         of this commit is unknown to this facade"
                    );
                    facade.txns.commit_failed(tenant, id);
                    return refused(ResponseError::CoordinatorNotAvailable, None);
                }
            };
            facade.txns.commit_landed(tenant, id, version);
            tracing::info!(
                target: "kafka",
                transactional_id = %id,
                producer_id = pid,
                epoch,
                records,
                offsets = bundle.offsets.len(),
                "a transaction committed"
            );
            committed()
        }
        // THE fence. A second producer took this id, so its version moved and
        // the whole bundle rolled back: not one record and not one offset.
        Err(queen::Error::Precondition { version, .. }) => {
            tracing::warn!(
                target: "kafka",
                transactional_id = %id,
                producer_id = pid,
                epoch,
                records,
                winner_version = version,
                "a fenced producer's commit wrote nothing: another producer holds this \
                 transactional id"
            );
            facade.txns.discard(tenant, id);
            refused(ResponseError::ProducerFenced, None)
        }
        Err(e) => {
            // RETRIABLE, and the stage is kept so the client's retry can commit
            // it. See the module header.
            tracing::error!(
                target: "kafka",
                transactional_id = %id,
                records,
                error = %e,
                "a transaction commit could not be sent; its stage is kept for the retry"
            );
            facade.txns.commit_failed(tenant, id);
            refused(
                ResponseError::CoordinatorNotAvailable,
                throttle::for_error(&e),
            )
        }
    }
}

/// `committed = false`: drop the stage, answer 0, then write the marker.
async fn abort(
    facade: &Facade,
    tenant: &crate::identity::TenantKey,
    id: &str,
    pid: i64,
    epoch: i16,
    token: Option<&str>,
) -> EndTxnResponse {
    // The binding is checked so a FENCED producer is still told it is fenced —
    // that is a fact about the producer, and a producer that believes it still
    // owns its transactional id will go on to open another transaction it
    // cannot commit.
    match facade.txns.with(tenant, id, pid, epoch, |_| ()) {
        Ok(()) => {}
        Err(fault @ (Fault::Fenced | Fault::AheadOfUs | Fault::WrongProducer)) => {
            return refused(fault.code(), None)
        }
        Err(Fault::InFlight) => return refused(Fault::InFlight.code(), None),
        // No binding at all: there is nothing to drop and nothing to mark.
        Err(Fault::Unknown) => return committed(),
        // Expired, NotOpen, Abortable: nothing of this transaction was ever
        // written, so "aborted" is simply true. This falls THROUGH rather than
        // answering here, because the fall-through is what resets the binding
        // to `Empty` — a producer that aborts a transaction its deadline or a
        // cap has poisoned must be able to open the next one without
        // re-initialising, and `Txn::expire` deliberately leaves a state that
        // refuses everything else.
        Err(_) => {}
    }
    let Some((version, seq)) = facade.txns.discard(tenant, id) else {
        return committed();
    };

    // Best effort, and AFTER the answer is decided. `required: false`, so a
    // lost marker aborts nothing: the transaction is aborted either way,
    // because nothing of it was ever written.
    let node = facade
        .cluster
        .state()
        .map_or(crate::handlers::metadata::SINGLE_NODE_ID, |s| s.me.id);
    let Some(key) = txn::key(id) else {
        return committed();
    };
    let op = KvOp::put_expecting(
        crate::offsets::NAMESPACE,
        &key,
        txn::marker(
            pid,
            epoch,
            txn::Outcome::Aborted,
            seq + 1,
            node,
            crate::handlers::init_producer_id::incarnation(),
            crate::offsets::now_millis(),
        ),
        version,
    );
    match facade.queen.kv(std::slice::from_ref(&op), token).await {
        Ok(answers) => match answers.first() {
            Some(a) if a.applied == Some(true) => facade.txns.note_version(tenant, id, a.version),
            // Lost to a producer that took the id. Nothing to do: the abort is
            // true, and the next request of this producer meets the fence.
            _ => tracing::debug!(
                target: "kafka",
                transactional_id = %id,
                "the abort marker lost its precondition; the transaction is aborted regardless"
            ),
        },
        Err(e) => tracing::warn!(
            target: "kafka",
            transactional_id = %id,
            error = %e,
            "the abort marker could not be written; the transaction is aborted regardless, \
             because nothing of it was ever written"
        ),
    }
    committed()
}

/// `error_code = 0`, which on this API means the transaction reached the
/// outcome the client asked for.
fn committed() -> EndTxnResponse {
    EndTxnResponse::default()
        .with_throttle_time_ms(0)
        .with_error_code(0)
}

fn refused(error: ResponseError, throttle_ms: Option<i32>) -> EndTxnResponse {
    EndTxnResponse::default()
        .with_throttle_time_ms(throttle_ms.unwrap_or(0))
        .with_error_code(error.code())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::testing::{facade, facade_and_queen};
    use crate::offsets::Committed;
    use crate::queen::PushItem;
    use crate::queen::QueenApi;
    use crate::txn::TxnState;
    use kafka_protocol::messages::{ProducerId, TransactionalId};
    use kafka_protocol::protocol::StrBytes;
    use std::sync::Arc;
    use std::time::Duration;

    const PID: i64 = 7;

    fn request(id: &str, pid: i64, epoch: i16, committed: bool) -> EndTxnRequest {
        EndTxnRequest::default()
            .with_transactional_id(TransactionalId(StrBytes::from_string(id.to_string())))
            .with_producer_id(ProducerId(pid))
            .with_producer_epoch(epoch)
            .with_committed(committed)
    }

    fn item(topic: &str, partition: i32, v: &str) -> PushItem {
        PushItem {
            queue: topic.to_string(),
            partition: partition.to_string(),
            payload: serde_json::json!({ "v": v }),
        }
    }

    /// A facade with one transaction bound, open, and holding `records`
    /// records and `offsets` staged offsets.
    async fn staged(
        records: usize,
        offsets: usize,
    ) -> (Facade, Arc<crate::queen::testing::FakeQueen>) {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let tenant = f.catalog.tenant_key(f.token());
        // The claim InitProducerId would have made, so the fence at index 0 of
        // the commit has a real row and a real version to expect. A fixture
        // that bound a version nothing wrote would make every commit look
        // fenced.
        let claimed = api
            .kv(
                &[KvOp::put_if_absent(
                    crate::offsets::NAMESPACE,
                    "qk:txn:tx",
                    crate::txn::marker(PID, 0, crate::txn::Outcome::Aborted, 0, 0, "inc", 0),
                )],
                None,
            )
            .await
            .unwrap();
        f.txns
            .bind(
                &tenant,
                "tx",
                PID,
                0,
                claimed[0].version,
                1,
                Duration::from_secs(60),
            )
            .unwrap();
        f.txns
            .add_partitions(&tenant, "tx", PID, 0, &[("orders".into(), 0)])
            .unwrap();
        if records > 0 {
            f.txns
                .stage_records(
                    &tenant,
                    "tx",
                    PID,
                    0,
                    "orders",
                    0,
                    (0..records)
                        .map(|i| item("orders", 0, &format!("r{i}")))
                        .collect(),
                    64 * records,
                )
                .unwrap()
                .unwrap();
        }
        if offsets > 0 {
            f.txns
                .add_offsets(&tenant, "tx", PID, 0, "g")
                .unwrap()
                .unwrap();
            for p in 0..offsets {
                f.txns
                    .stage_offset(
                        &tenant,
                        "tx",
                        PID,
                        0,
                        crate::offsets::key("g", "orders", p as i32).unwrap(),
                        Committed {
                            offset: 100 + p as i64,
                            metadata: String::new(),
                            ts: 1,
                        },
                    )
                    .unwrap()
                    .unwrap();
            }
        }
        (f, api)
    }

    /// THE bundle: records and offsets in ONE call, with the fence at index 0.
    /// This is the shape the whole design rests on, asserted on the wire the
    /// double recorded rather than on intent.
    #[tokio::test]
    async fn a_commit_is_one_bundle_with_the_fence_at_index_zero() {
        let (f, api) = staged(3, 2).await;
        let resp = handle(&f, &request("tx", PID, 0, true), None).await;
        assert_eq!(resp.error_code, 0);

        let sent = api.transactions.lock().unwrap().clone();
        assert_eq!(sent.len(), 1, "a transaction must be exactly one call");
        let (items, ops) = &sent[0];
        assert_eq!(items.len(), 3);
        assert_eq!(ops.len(), 4, "the fence, the group index, two offsets");
        match &ops[0] {
            KvOp::Put {
                key,
                expect,
                required,
                forever,
                ..
            } => {
                assert_eq!(key, "qk:txn:tx");
                assert!(expect.is_some_and(|v| v > 0), "the fence expects nothing");
                assert!(*required, "without this a fenced commit writes its records");
                assert!(*forever);
            }
            other => panic!("index 0 is not the fence: {other:?}"),
        }
        // Nothing went through the plain push route, and the only KV call is
        // the fixture's own claim: a transaction that used two calls would not
        // be atomic.
        assert!(api.pushes.lock().unwrap().is_empty());
        assert_eq!(api.kv_calls.lock().unwrap().len(), 1);

        // The stage is freed and the version advanced, so the NEXT transaction
        // expects the version the fence just took.
        let tenant = f.catalog.tenant_key(f.token());
        assert_eq!(f.txns.staged_bytes(), 0);
        let (state, version, seq) = f
            .txns
            .with(&tenant, "tx", PID, 0, |t| (t.state, t.version, t.seq))
            .unwrap();
        assert_eq!(state, TxnState::Empty);
        assert_eq!(seq, 1);
        assert!(version > 0, "the fence's new version was not taken");
    }

    /// ...and the records really are in the log afterwards, at contiguous
    /// offsets, because a committed transaction of N records advances the log
    /// end offset by exactly N — where Kafka advances it by N+1.
    #[tokio::test]
    async fn the_records_are_readable_after_the_commit_and_not_before() {
        let (f, api) = staged(3, 0).await;
        let before = api
            .fetch(
                &[crate::queen::FetchEntry {
                    queue: "orders".into(),
                    partition: "0".into(),
                    offset: 0,
                    max_bytes: 1024,
                }],
                0,
                0,
                None,
            )
            .await
            .unwrap();
        assert_eq!(before[0].high_watermark, 0, "a staged record was visible");

        handle(&f, &request("tx", PID, 0, true), None).await;
        let after = api
            .fetch(
                &[crate::queen::FetchEntry {
                    queue: "orders".into(),
                    partition: "0".into(),
                    offset: 0,
                    max_bytes: 1024,
                }],
                0,
                0,
                None,
            )
            .await
            .unwrap();
        assert_eq!(after[0].high_watermark, 3);
        assert_eq!(after[0].records.len(), 3);
    }

    /// THE fencing proof, at the level that matters: a fenced commit writes
    /// ZERO records — asserted by reading the log, not by trusting the code.
    #[tokio::test]
    async fn a_fenced_commit_writes_no_record_at_all() {
        let (f, api) = staged(5, 1).await;
        let tenant = f.catalog.tenant_key(f.token());
        // A second producer takes the id: the fence version moves.
        api.kv(
            &[KvOp::put(
                crate::offsets::NAMESPACE,
                "qk:txn:tx",
                serde_json::json!({"pid": PID, "epoch": 1, "state": "aborted", "seq": 0}),
            )],
            None,
        )
        .await
        .unwrap();

        let resp = handle(&f, &request("tx", PID, 0, true), None).await;
        assert_eq!(resp.error_code, ResponseError::ProducerFenced.code());

        let read = api
            .fetch(
                &[crate::queen::FetchEntry {
                    queue: "orders".into(),
                    partition: "0".into(),
                    offset: 0,
                    max_bytes: 1024,
                }],
                0,
                0,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            read[0].high_watermark, 0,
            "a fenced producer's records reached the log"
        );
        // ...and the offset it staged is not in the store either.
        let rows = api
            .kv(
                &[KvOp::GetMany {
                    ns: crate::offsets::NAMESPACE.to_string(),
                    keys: vec!["qk:group:g:orders:0".to_string()],
                }],
                None,
            )
            .await
            .unwrap();
        assert_eq!(rows[0].missing.len(), 1, "a fenced commit wrote an offset");
        // The stage is dropped: a fenced producer has nothing to retry. The
        // BINDING is left where it was, because this facade does not learn the
        // winner's epoch from a rolled-back bundle — the producer learns it is
        // fenced from the code, and its next InitProducerId is what rebinds.
        assert_eq!(f.txns.staged_bytes(), 0);
        assert_eq!(
            f.txns
                .with(&tenant, "tx", PID, 0, |t| (t.staged.len(), t.offsets.len()))
                .unwrap(),
            (0, 0)
        );
    }

    /// The rule that must not be got backwards: a retriable failure KEEPS the
    /// stage, so the client's retry commits the records rather than an empty
    /// transaction.
    #[tokio::test]
    async fn a_transport_failure_keeps_the_stage_for_the_retry() {
        let (f, api) = staged(4, 0).await;
        *api.transaction_error.lock().unwrap() =
            Some(crate::queen::Error::Transport("reset".into()));
        let resp = handle(&f, &request("tx", PID, 0, true), None).await;
        assert_eq!(
            resp.error_code,
            ResponseError::CoordinatorNotAvailable.code()
        );
        assert!(
            ResponseError::CoordinatorNotAvailable.is_retriable(),
            "a commit failure the client cannot retry would lose the stage"
        );
        assert_eq!(f.txns.staged_bytes(), 64 * 4, "the stage was dropped");

        // The retry commits the same four records.
        let resp = handle(&f, &request("tx", PID, 0, true), None).await;
        assert_eq!(resp.error_code, 0);
        assert_eq!(api.transactions.lock().unwrap()[1].0.len(), 4);
    }

    /// A 429 from a Cloud rate cap reaches the client as the throttle every
    /// producer already sleeps on, beside a retriable code.
    #[tokio::test]
    async fn a_rate_capped_commit_carries_the_throttle() {
        let (f, api) = staged(1, 0).await;
        *api.transaction_error.lock().unwrap() = Some(crate::queen::Error::Status {
            code: 429,
            body: "slow down".into(),
            retry_after_ms: Some(3_000),
        });
        let resp = handle(&f, &request("tx", PID, 0, true), None).await;
        assert_eq!(
            resp.error_code,
            ResponseError::CoordinatorNotAvailable.code()
        );
        assert_eq!(resp.throttle_time_ms, 3_000);
    }

    /// A transaction with nothing in it costs no round trip: the fence would be
    /// the only operation in the bundle, and there is nothing for it to gate.
    #[tokio::test]
    async fn an_empty_transaction_commits_without_a_call() {
        let (f, api) = staged(0, 0).await;
        assert_eq!(
            handle(&f, &request("tx", PID, 0, true), None)
                .await
                .error_code,
            0
        );
        assert!(api.transactions.lock().unwrap().is_empty());
    }

    /// A commit for a transaction this facade never held. Fatal, and it has to
    /// be: this is the crash path, and the only answer that cannot let an
    /// application believe an uncommitted commit.
    #[tokio::test]
    async fn a_commit_with_no_stage_is_fatal_and_writes_nothing() {
        let (f, api) = facade_and_queen(&[("orders", 4)]);
        let resp = handle(&f, &request("tx", PID, 0, true), None).await;
        assert_eq!(resp.error_code, ResponseError::InvalidTxnState.code());
        assert!(api.transactions.lock().unwrap().is_empty());
    }

    // ------------------------------------------------------------- the abort

    #[tokio::test]
    async fn an_abort_drops_the_stage_and_writes_no_record() {
        let (f, api) = staged(3, 1).await;
        let resp = handle(&f, &request("tx", PID, 0, false), None).await;
        assert_eq!(resp.error_code, 0);
        assert!(api.transactions.lock().unwrap().is_empty());
        assert_eq!(f.txns.staged_bytes(), 0);
        // The marker is written, best effort, and it says aborted.
        // The fixture's own claim, then the abort marker.
        let calls = api.kv_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        match &calls[1][0] {
            KvOp::Put {
                key,
                value,
                expect,
                required,
                ..
            } => {
                assert_eq!(key, "qk:txn:tx");
                assert!(
                    expect.is_some_and(|v| v > 0),
                    "the abort marker expects nothing"
                );
                assert!(!*required, "an abort marker must gate nothing");
                assert_eq!(value.get("state").and_then(|s| s.as_str()), Some("aborted"));
            }
            other => panic!("the abort marker is not a put: {other:?}"),
        }
    }

    /// A lost or failed marker does not change the answer: the abort is a
    /// client-side fact, because nothing was ever written.
    #[tokio::test]
    async fn an_abort_whose_marker_fails_is_still_an_abort() {
        let (f, api) = staged(3, 0).await;
        *api.kv_error.lock().unwrap() = Some(crate::queen::Error::Transport("down".into()));
        assert_eq!(
            handle(&f, &request("tx", PID, 0, false), None)
                .await
                .error_code,
            0
        );
        assert_eq!(f.txns.staged_bytes(), 0);
    }

    /// An abort of a transaction this facade lost IS an abort: nothing of it
    /// ever reached the log, so "aborted" is simply true.
    #[tokio::test]
    async fn an_abort_with_no_stage_is_a_success() {
        let f = facade(&[("orders", 4)]);
        assert_eq!(
            handle(&f, &request("tx", PID, 0, false), None)
                .await
                .error_code,
            0
        );
    }

    /// ...but a FENCED producer is still told it is fenced, because that is a
    /// fact about the producer and not about the transaction: one that believes
    /// it still owns its id would open another transaction it cannot commit.
    #[tokio::test]
    async fn a_fenced_producer_aborting_is_still_told_it_is_fenced() {
        let (f, _) = staged(1, 0).await;
        let tenant = f.catalog.tenant_key(f.token());
        f.txns
            .bind(&tenant, "tx", PID, 1, 101, 2, Duration::from_secs(60))
            .unwrap();
        assert_eq!(
            handle(&f, &request("tx", PID, 0, false), None)
                .await
                .error_code,
            ResponseError::ProducerFenced.code()
        );
    }

    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        use bytes::BytesMut;
        use kafka_protocol::protocol::{Decodable, Encodable, Message};

        let f = facade(&[("orders", 4)]);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::EndTxn as i16)
            .expect("EndTxn is advertised");
        assert!(row.min >= EndTxnRequest::VERSIONS.min && row.max <= EndTxnRequest::VERSIONS.max);
        let tenant = f.catalog.tenant_key(f.token());

        for version in row.min..=row.max {
            // Its own id per version: a fixture reusing one would meet the
            // binding the previous version left and be answered differently.
            let id = format!("tx-v{version}");
            f.txns
                .bind(&tenant, &id, PID, 0, 100, 1, Duration::from_secs(60))
                .unwrap();
            let mut wire = BytesMut::new();
            request(&id, PID, 0, true)
                .encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = EndTxnRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing request bytes");

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = EndTxnResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(buf.is_empty(), "v{version}: trailing response bytes");
            assert_eq!(back.error_code, 0, "v{version}");
        }
    }
}
