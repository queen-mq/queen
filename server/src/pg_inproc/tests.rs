//! The in-process connectors against the real state machine: [`LocalQueen`]
//! over a single-node [`RaftFacade`] (receiver, planner, apply, reads), and
//! the [`Manager`] reading documents from the broker-internal tenant.
//!
//! What they prove:
//! - the source's exactly-once commit works through the adapter: pushes and a
//!   fenced pointer in ONE transaction; the same bundle again is the
//!   `duplicate` verdict and a stale pointer version the `kv_precondition`
//!   verdict — 200 answers the engine reads, with nothing of the bundle
//!   written;
//! - the sink's loop works through it: a grouped pop with leases, a lease
//!   extension, an ack, and the cursor moved;
//! - KV answers are the route's (a lost `required` fence is `ok:false`), per
//!   tenant;
//! - a node that does not serve typed calls answers a retryable 503 and runs
//!   no unit;
//! - documents become units and leave with them, per tenant; a disabled one
//!   runs nothing, one whose password this node cannot unseal says so;
//! - [`start_with`] runs the manager on its own runtime, reports under
//!   `/status`, and stops inside its grace (needs the crate's engines).

use std::sync::atomic::AtomicU64;

use queen_pg::queen::{AckItem, AckStatus};
use serde_json::json;

use super::*;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{PushReq, RsmBuildCtx};

static SEQ: AtomicU64 = AtomicU64::new(0);

const TENANT_A: &str = "11111111-1111-4111-8111-111111111111";
const TENANT_B: &str = "22222222-2222-4222-8222-222222222222";

/// One single-node broker. Its data directory (a few hundred KB) is left
/// behind: the facade's threads outlive the test's handles on it.
struct Broker {
    rsm: Arc<dyn Rsm>,
}

impl Broker {
    fn open(tag: &str) -> Broker {
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-pg-inproc-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let facade = RaftFacade::open(&RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            // A nearly full developer disk must not turn the writes into 507s.
            disk_high_pct: 99.5,
            disk_low_pct: 99.0,
        })
        .expect("open the facade");
        Broker {
            rsm: Arc::new(facade),
        }
    }

    fn queen(&self, tenant: &str) -> LocalQueen {
        LocalQueen::new(
            Arc::clone(&self.rsm),
            tokio::runtime::Handle::current(),
            tenant,
        )
    }

    async fn kv_as(&self, tenant: &str, ops: Vec<Value>) -> Vec<Value> {
        self.rsm
            .kv(
                ReqCtx::new(tenant, Deadline::after(Duration::from_secs(30))),
                crate::rsm::facade::KvReq { ops },
            )
            .await
            .unwrap_or_else(|f| panic!("a KV call as {tenant}: {f:?}"))
            .results
    }

    /// Push `items` (push-body items) as `tenant`.
    async fn push_as(&self, tenant: &str, items: Vec<Value>) {
        let out = self
            .rsm
            .push(
                ReqCtx::new(tenant, Deadline::after(Duration::from_secs(30))),
                PushReq {
                    raw: json!({ "items": items }).to_string().into_bytes(),
                },
            )
            .await
            .expect("push");
        let v: Value = serde_json::from_str(&out.body).expect("push body");
        for it in v.as_array().expect("push answers an array") {
            assert_eq!(it["status"], "queued", "{it}");
        }
    }

    /// Store a connector document the way the API does.
    async fn put_doc(&self, tenant: &str, name: &str, doc: Value) {
        let r = self
            .kv_as(
                SYSTEM_TENANT,
                vec![
                    json!({"op": "put", "ns": "queen-pg", "key": doc_key(tenant, name),
                            "value": doc, "forever": true}),
                ],
            )
            .await;
        assert_eq!(r[0]["applied"], true, "{r:?}");
    }

    async fn delete_doc(&self, tenant: &str, name: &str) {
        self.kv_as(
            SYSTEM_TENANT,
            vec![json!({"op": "delete", "ns": "queen-pg", "key": doc_key(tenant, name)})],
        )
        .await;
    }
}

/// Wait until `done` holds, or fail naming `what` with the value it saw.
async fn until(what: &str, mut probe: impl FnMut() -> Value, done: impl Fn(&Value) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let v = probe();
        if done(&v) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}: {v}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The entry of `(tenant, name)` in a status block, or `null`.
fn unit_of(status: &Value, tenant: &str, name: &str) -> Value {
    status["connectors"]
        .as_array()
        .and_then(|units| {
            units
                .iter()
                .find(|u| u["tenant"] == tenant && u["name"] == name)
                .cloned()
        })
        .unwrap_or(Value::Null)
}

/// A sink document pointing at a port nothing listens on.
fn sink_doc(enabled: bool, updated_at: &str) -> Value {
    json!({
        "kind": "sink",
        "enabled": enabled,
        "connection": {"host": "127.0.0.1", "port": 1, "database": "app", "user": "writer",
                       "sslMode": "disable", "connectTimeoutMs": 1000},
        "sink": {"queue": "orders", "table": "public.orders_copy", "mode": "append"},
        "updatedAt": updated_at,
    })
}

fn source_doc(enabled: bool, updated_at: &str) -> Value {
    json!({
        "kind": "source",
        "enabled": enabled,
        "connection": {"host": "127.0.0.1", "port": 1, "database": "app", "user": "cdc",
                       "sslMode": "disable", "connectTimeoutMs": 1000},
        "source": {"tables": [{"table": "public.orders", "queue": "orders"}]},
        "updatedAt": updated_at,
    })
}

fn manager(broker: &Broker, encryption: Arc<Encryption>) -> (Manager, Arc<Status>) {
    let knobs = Arc::new(NodeKnobs::defaults("node-test"));
    let metrics = Metrics::new();
    let status = Arc::new(Status::new(
        knobs.threads,
        knobs.reload_ms,
        Arc::clone(&metrics),
    ));
    let m = Manager::new(
        knobs,
        Arc::clone(&broker.rsm),
        tokio::runtime::Handle::current(),
        encryption,
        metrics,
        Arc::clone(&status),
    );
    (m, status)
}

// ---------------------------------------------------------------------------
// LocalQueen
// ---------------------------------------------------------------------------

/// A source bundle: `ids` pushed to `orders` with the source's id scheme, and
/// the pointer fenced at `expect`.
fn bundle(lsn: &str, expect: i64, ids: &[&str]) -> String {
    let items: Vec<Value> = ids
        .iter()
        .map(|id| {
            json!({"queue": "orders", "partition": "42", "payload": {"id": id},
                   "transactionId": format!("pg:4f1c2a9b:{id}")})
        })
        .collect();
    json!({
        "operations": [{"type": "push", "items": items}],
        "kv": [KvOp::fence("src:orders-src:pointer", json!({"v": 1, "lsn": lsn}), expect).to_json()],
    })
    .to_string()
}

/// The transaction ids a fresh group finds in `queue`.
async fn transaction_ids(queen: &LocalQueen, queue: &str, group: &str) -> Vec<String> {
    let popped = queen
        .pop(PopRequest {
            queue: queue.to_string(),
            group: group.to_string(),
            batch: 100,
            wait_ms: 0,
            lease_seconds: 30,
            subscription_mode: "all".to_string(),
            max_partitions: Some(16),
        })
        .await
        .expect("a pop");
    let mut ids: Vec<String> = popped
        .messages
        .into_iter()
        .map(|m| m.transaction_id)
        .collect();
    ids.sort();
    ids
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bundle_commits_its_pushes_with_its_pointer_and_a_repeat_is_a_verdict() {
    let broker = Broker::open("txn");
    let queen = broker.queen(TENANT_A);

    let first = queen
        .transaction(bundle("0/10", 0, &["a", "b"]))
        .await
        .expect("a transaction");
    assert!(first.success, "{first:?}");
    let v1 = first.kv_result(0).expect("the pointer's result").version;
    assert!(v1 > 0, "{first:?}");
    let read = queen
        .kv(vec![KvOp::get("src:orders-src:pointer")])
        .await
        .expect("a read");
    assert_eq!(read.results[0].version, v1);
    assert_eq!(read.results[0].value["lsn"], "0/10");

    // The same bundle again — a source retrying after an in-doubt answer: its
    // pushes are duplicates, so the WHOLE bundle rolls back. A verdict the
    // engine reads, never an error.
    let again = queen
        .transaction(bundle("0/10", 0, &["a", "b"]))
        .await
        .expect("a verdict, not an error");
    assert!(again.is_duplicate(), "{again:?}");

    // Another owner moved the pointer: a stale version rolls the bundle back
    // and names the winner's.
    let stale = queen
        .transaction(bundle("0/20", v1 + 1, &["c", "d"]))
        .await
        .expect("a verdict, not an error");
    assert!(stale.is_precondition(), "{stale:?}");
    assert_eq!(stale.version, Some(v1), "{stale:?}");

    // Nothing of either rollback was written.
    let read = queen
        .kv(vec![KvOp::get("src:orders-src:pointer")])
        .await
        .expect("a read");
    assert_eq!(read.results[0].version, v1);
    assert_eq!(read.results[0].value["lsn"], "0/10");
    assert_eq!(
        transaction_ids(&queen, "orders", "check-1").await,
        vec!["pg:4f1c2a9b:a".to_string(), "pg:4f1c2a9b:b".to_string()]
    );

    // With the version held, it moves on.
    let next = queen
        .transaction(bundle("0/20", v1, &["c", "d"]))
        .await
        .expect("a transaction");
    assert!(next.success, "{next:?}");
    assert_ne!(next.kv_result(0).expect("the pointer").version, v1);
    assert_eq!(transaction_ids(&queen, "orders", "check-2").await.len(), 4);

    // Every row of it is the connector's tenant's: the default tenant has
    // neither the pointer nor the messages.
    let other = broker.queen(DEFAULT_TENANT);
    let read = other
        .kv(vec![KvOp::get("src:orders-src:pointer")])
        .await
        .expect("a read");
    assert_eq!(read.results[0].found, Some(false));
    assert!(transaction_ids(&other, "orders", "check-3")
        .await
        .is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_grouped_pop_is_leased_extended_acked_and_the_cursor_moves() {
    let broker = Broker::open("pop");
    let queen = broker.queen(DEFAULT_TENANT);
    broker
        .push_as(
            DEFAULT_TENANT,
            vec![
                json!({"queue": "jobs", "partition": "p0", "payload": {"n": 1}, "transactionId": "t1"}),
                json!({"queue": "jobs", "partition": "p0", "payload": {"n": 2}, "transactionId": "t2"}),
                json!({"queue": "jobs", "partition": "p1", "payload": {"n": 12345678901234567890u64}, "transactionId": "t3"}),
            ],
        )
        .await;
    let ask = |wait_ms: u64| PopRequest {
        queue: "jobs".to_string(),
        group: "pg-copy".to_string(),
        batch: 10,
        wait_ms,
        lease_seconds: 30,
        subscription_mode: "all".to_string(),
        max_partitions: Some(4),
    };
    let popped = queen.pop(ask(0)).await.expect("a pop");
    assert_eq!(popped.messages.len(), 3, "{:?}", popped.messages);
    for m in &popped.messages {
        assert!(!m.lease_id.is_empty(), "{m:?}");
        assert!(m.partition_number().is_some(), "{m:?}");
        assert!(m.offset >= 0, "{m:?}");
    }
    // The payload is handed over as stored: a big integer stays exact.
    let t3 = popped
        .messages
        .iter()
        .find(|m| m.transaction_id == "t3")
        .expect("t3");
    assert_eq!(t3.data.get(), r#"{"n":12345678901234567890}"#);

    // A long batch extends its leases.
    for m in &popped.messages {
        queen
            .extend_lease(m.lease_id.clone(), 30)
            .await
            .expect("a lease extension");
    }
    let acked = queen
        .ack(AckRequest {
            group: "pg-copy".to_string(),
            items: popped
                .messages
                .iter()
                .map(|m| AckItem {
                    transaction_id: m.transaction_id.clone(),
                    partition_id: m.partition_id.clone(),
                    lease_id: m.lease_id.clone(),
                    status: AckStatus::Ok,
                    error: None,
                })
                .collect(),
        })
        .await
        .expect("an ack");
    assert_eq!(acked.results.len(), 3, "{:?}", acked.results);
    assert!(
        acked.results.iter().all(|r| r.success),
        "{:?}",
        acked.results
    );

    // The group's cursor moved: nothing left, and a waiting pop holds for
    // its wait — not for the 30 s budget of a call.
    let t0 = Instant::now();
    let empty = queen.pop(ask(300)).await.expect("an empty pop");
    assert!(empty.messages.is_empty(), "{:?}", empty.messages);
    assert!(
        t0.elapsed() < Duration::from_secs(5),
        "a 300 ms long-poll held {:?}",
        t0.elapsed()
    );
    // An empty batch of acks is answered without a command.
    let none = queen
        .ack(AckRequest {
            group: "pg-copy".to_string(),
            items: Vec::new(),
        })
        .await
        .expect("an empty ack");
    assert!(none.results.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kv_answers_are_the_routes_and_a_lost_fence_writes_nothing() {
    let broker = Broker::open("kv");
    let queen = broker.queen(TENANT_B);
    let put = queen
        .kv(vec![KvOp::put("src:s:lease", json!({"node": "n1"}))])
        .await
        .expect("a put");
    assert!(put.ok);
    assert!(put.results[0].did_apply(), "{put:?}");
    let v = put.results[0].version;
    let got = queen
        .kv(vec![KvOp::get("src:s:lease")])
        .await
        .expect("a get");
    assert_eq!(
        got.results[0].value_if_found(),
        Some(&json!({"node": "n1"}))
    );
    assert_eq!(got.results[0].version, v);

    // A lease claim that loses: not applied, with the winner's row.
    let claim = queen
        .kv(vec![KvOp::put_if_absent_ttl(
            "src:s:lease",
            json!({"node": "n2"}),
            10,
        )])
        .await
        .expect("a claim");
    assert!(claim.ok, "{claim:?}");
    assert!(!claim.results[0].did_apply(), "{claim:?}");
    assert_eq!(claim.results[0].reason.as_deref(), Some("exists"));
    assert_eq!(claim.results[0].version, v);

    // A fenced write on a stale version, beside an unconditional one: the
    // whole call is the 200 `ok:false` verdict, and neither is written.
    let fenced = queen
        .kv(vec![
            KvOp::fence_ttl("src:s:lease", json!({"node": "n3"}), v + 1, 10),
            KvOp::put("src:s:pointer", json!({"lsn": "0/1"})),
        ])
        .await
        .expect("a verdict, not an error");
    assert!(!fenced.ok, "{fenced:?}");
    assert_eq!(fenced.reason, "kv_precondition");
    assert_eq!(fenced.failed_index, 0);
    assert_eq!(fenced.kv_reason.as_deref(), Some("version"));
    assert_eq!(fenced.version, v);
    let read = queen
        .kv(vec![KvOp::get("src:s:lease"), KvOp::get("src:s:pointer")])
        .await
        .expect("a read");
    assert_eq!(read.results[0].value["node"], "n1");
    assert_eq!(read.results[1].found, Some(false));

    // Another tenant's KV is another KV.
    let other = broker
        .queen(TENANT_A)
        .kv(vec![KvOp::get("src:s:lease")])
        .await
        .expect("a read");
    assert_eq!(other.results[0].found, Some(false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_are_the_routes_statuses() {
    let broker = Broker::open("refusals");
    let queen = broker.queen(DEFAULT_TENANT);
    // A body the transaction wire refuses before planning: its 400.
    match queen.transaction("{\"operations\": 7}".to_string()).await {
        Err(QueenError::Status {
            code: 400, body, ..
        }) => {
            assert!(body.contains("operations"), "{body}")
        }
        other => panic!("expected a 400, got {other:?}"),
    }
    // A name past the store's key bound: the route's 413, not retryable.
    let e = queen
        .pop(PopRequest {
            queue: "q".repeat(600),
            group: "g".to_string(),
            batch: 1,
            wait_ms: 0,
            lease_seconds: 30,
            subscription_mode: "all".to_string(),
            max_partitions: None,
        })
        .await
        .expect_err("an over-long name");
    assert!(
        matches!(&e, QueenError::Status { code: 413, body, .. } if body.contains("name_too_long")),
        "{e:?}"
    );
    assert!(!e.is_retryable());
    // A key the broker refuses (empty): the KV route's 400.
    let e = queen
        .kv(vec![KvOp::put("", json!(1))])
        .await
        .expect_err("an empty key");
    assert!(matches!(e, QueenError::Status { code: 400, .. }), "{e:?}");
}

/// A follower of a cluster with `QUEEN_RAFT_CLIENT_OFFLOAD=false`: it serves
/// no typed call.
struct Follower;

#[async_trait::async_trait]
impl Rsm for Follower {
    fn route(&self) -> Route {
        Route::Leader("10.0.0.1:6632".to_string())
    }
    async fn push(&self, _: ReqCtx, _: PushReq) -> Result<crate::rsm::facade::PushOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn pop_wildcard(
        &self,
        _: ReqCtx,
        _: PopReq,
    ) -> Result<crate::rsm::facade::PopOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn pop_pinned(
        &self,
        _: ReqCtx,
        _: crate::rsm::facade::PopPinnedReq,
    ) -> Result<crate::rsm::facade::PopOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn pop_discover(
        &self,
        _: ReqCtx,
        _: crate::rsm::facade::PopDiscoverReq,
    ) -> Result<crate::rsm::facade::PopOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn ack(&self, _: ReqCtx, _: AckReq) -> Result<crate::rsm::facade::AckOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn renew(
        &self,
        _: ReqCtx,
        _: RenewReq,
    ) -> Result<crate::rsm::facade::RenewOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn transaction(
        &self,
        _: ReqCtx,
        _: TxnReq,
    ) -> Result<crate::rsm::facade::TxnOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn dlq_head(
        &self,
        _: ReqCtx,
        _: crate::rsm::facade::DlqHeadReq,
    ) -> Result<crate::rsm::facade::DlqHeadOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn has_pending(
        &self,
        _: ReqCtx,
        _: crate::rsm::facade::PendingReq,
    ) -> Result<bool, RsmError> {
        Err(RsmError::Unsupported)
    }
    async fn depth(
        &self,
        _: ReqCtx,
        _: crate::rsm::facade::DepthReq,
    ) -> Result<crate::rsm::facade::DepthOut, RsmError> {
        Err(RsmError::Unsupported)
    }
    fn health(&self) -> crate::rsm::facade::RaftHealth {
        crate::rsm::facade::NotReady::new().health()
    }
}

fn assert_not_served(e: &QueenError) {
    match e {
        QueenError::Status {
            code: 503,
            body,
            retry_after_ms: Some(30_000),
        } => assert!(body.contains("QUEEN_RAFT_CLIENT_OFFLOAD=false"), "{body}"),
        other => panic!("expected the not-served 503, got {other:?}"),
    }
    assert!(e.is_retryable());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_node_that_does_not_serve_typed_calls_answers_503_and_runs_nothing() {
    let rsm: Arc<dyn Rsm> = Arc::new(Follower);
    let queen = LocalQueen::new(
        Arc::clone(&rsm),
        tokio::runtime::Handle::current(),
        DEFAULT_TENANT,
    );
    assert_not_served(&queen.transaction("{}".into()).await.unwrap_err());
    assert_not_served(&queen.kv(vec![KvOp::get("k")]).await.unwrap_err());
    assert_not_served(
        &queen
            .pop(PopRequest {
                queue: "q".into(),
                group: "g".into(),
                batch: 1,
                wait_ms: 0,
                lease_seconds: 30,
                subscription_mode: "all".into(),
                max_partitions: None,
            })
            .await
            .unwrap_err(),
    );
    assert_not_served(
        &queen
            .ack(AckRequest {
                group: "g".into(),
                items: vec![AckItem {
                    transaction_id: "t".into(),
                    partition_id: "1".into(),
                    lease_id: "l".into(),
                    status: AckStatus::Ok,
                    error: None,
                }],
            })
            .await
            .unwrap_err(),
    );
    assert_not_served(&queen.extend_lease("l".into(), 30).await.unwrap_err());

    // Its manager runs no unit and says why.
    let knobs = Arc::new(NodeKnobs::defaults("follower"));
    let metrics = Metrics::new();
    let status = Arc::new(Status::new(1, 5_000, Arc::clone(&metrics)));
    let mut m = Manager::new(
        knobs,
        rsm,
        tokio::runtime::Handle::current(),
        Encryption::for_test([1u8; 32]),
        metrics,
        Arc::clone(&status),
    );
    m.reconcile().await;
    let st = status.render();
    assert_eq!(st["phase"], "not_serving", "{st}");
    assert_eq!(st["connectors"], json!([]), "{st}");
}

// ---------------------------------------------------------------------------
// The manager
// ---------------------------------------------------------------------------

/// Documents become units — one per (tenant, name), so two tenants' `copy`
/// never meet — and leave with them. None of these runs an engine: a disabled
/// connector runs nothing, one this node cannot unseal says so, one that does
/// not parse says that; so this needs nothing of the crate but its document
/// type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn documents_become_units_per_tenant_and_leave_with_them() {
    let broker = Broker::open("manager");
    let encryption = Encryption::for_test([7u8; 32]);
    let (mut m, status) = manager(&broker, Arc::clone(&encryption));

    broker
        .put_doc(
            TENANT_A,
            "copy",
            sink_doc(false, "2026-10-02T10:00:00.000001Z"),
        )
        .await;
    broker
        .put_doc(
            TENANT_B,
            "copy",
            sink_doc(false, "2026-10-02T10:00:00.000001Z"),
        )
        .await;
    // Sealed by another cell's key: this node cannot open it.
    let foreign = Encryption::for_test([8u8; 32]);
    let mut sealed = source_doc(true, "2026-10-02T10:00:00.000001Z");
    sealed["connection"]["passwordSealed"] = Value::String(
        String::from_utf8(foreign.encrypt(b"s3cr3t").expect("sealed")).expect("envelope text"),
    );
    broker.put_doc(TENANT_A, "cdc", sealed).await;
    // Written by a broker that knows a field this one does not.
    broker
        .put_doc(
            TENANT_B,
            "broken",
            json!({"kind": "sink", "futureField": 1, "updatedAt": "2026-10-02T10:00:00.000001Z"}),
        )
        .await;
    // Not a connector document at all: skipped.
    broker
        .kv_as(
            SYSTEM_TENANT,
            vec![
                json!({"op": "put", "ns": "queen-pg", "key": "conn:no-name-here",
                        "value": {}, "forever": true}),
            ],
        )
        .await;

    m.reconcile().await;
    until(
        "every unit settled",
        || status.render(),
        |st| {
            unit_of(st, TENANT_A, "copy")["phase"] == "disabled"
                && unit_of(st, TENANT_B, "copy")["phase"] == "disabled"
                && unit_of(st, TENANT_A, "cdc")["phase"] == "error"
                && unit_of(st, TENANT_B, "broken")["phase"] == "error"
        },
    )
    .await;
    let st = status.render();
    assert_eq!(st["phase"], "running", "{st}");
    assert_eq!(st["documentsError"], Value::Null, "{st}");
    assert_eq!(st["connectors"].as_array().map(Vec::len), Some(4), "{st}");
    let a = unit_of(&st, TENANT_A, "copy");
    assert_eq!(a["kind"], "sink", "{a}");
    let b = unit_of(&st, TENANT_B, "copy");
    assert_ne!(a["generation"], b["generation"], "two units: {st}");
    let cdc = unit_of(&st, TENANT_A, "cdc");
    assert_eq!(cdc["kind"], "source", "{cdc}");
    assert_eq!(cdc["error"]["code"], "unseal", "{cdc}");
    assert!(
        cdc["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("QUEEN_ENCRYPTION_KEY")),
        "{cdc}"
    );
    let broken = unit_of(&st, TENANT_B, "broken");
    assert_eq!(broken["error"]["code"], "config", "{broken}");
    assert!(
        broken["error"]["message"]
            .as_str()
            .is_some_and(|m| m.contains("futureField")),
        "{broken}"
    );
    // The API's view of one connector is the same entry.
    assert_eq!(
        status.render_one(&(TENANT_A.to_string(), "copy".to_string())),
        Some(a.clone())
    );

    // A reload that finds nothing new restarts nothing.
    m.reconcile().await;
    assert_eq!(
        unit_of(&status.render(), TENANT_A, "copy")["generation"],
        a["generation"]
    );
    // A changed document is a new unit (a new generation).
    broker
        .put_doc(
            TENANT_A,
            "copy",
            sink_doc(false, "2026-10-02T11:00:00.000001Z"),
        )
        .await;
    m.reconcile().await;
    until(
        "A's copy restarted",
        || status.render(),
        |st| {
            let u = unit_of(st, TENANT_A, "copy");
            u["phase"] == "disabled" && u["generation"] != a["generation"]
        },
    )
    .await;

    // Removed documents: stopped, then forgotten; the others stay.
    broker.delete_doc(TENANT_B, "copy").await;
    broker.delete_doc(TENANT_A, "cdc").await;
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        m.reconcile().await;
        let st = status.render();
        if unit_of(&st, TENANT_B, "copy").is_null() && unit_of(&st, TENANT_A, "cdc").is_null() {
            break;
        }
        assert!(Instant::now() < deadline, "the removed units stayed: {st}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let st = status.render();
    assert_eq!(st["connectors"].as_array().map(Vec::len), Some(2), "{st}");
    assert_eq!(unit_of(&st, TENANT_A, "copy")["phase"], "disabled", "{st}");

    m.stop_everything().await;
}

// ---------------------------------------------------------------------------
// With the crate's engines (connector.rs, config_validate.rs, metrics.rs).
// ---------------------------------------------------------------------------

/// Enabled connectors pointed at a port nothing listens on: each unit runs its
/// engine, which cannot connect and reports why — an error with a code that is
/// neither a panic nor a configuration refusal — while the supervisor keeps
/// it. (Whether the engine retries inside and says `connecting`, or returns and
/// is restarted, is the engine's choice; both report the error.) A DISABLED
/// source that is being deleted runs its engine too: its slot must still be
/// dropped, so it tries to connect rather than sit `disabled`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_enabled_connector_that_cannot_connect_reports_why() {
    let broker = Broker::open("unreachable");
    let (mut m, status) = manager(&broker, Encryption::for_test([7u8; 32]));
    broker
        .put_doc(
            DEFAULT_TENANT,
            "copy",
            sink_doc(true, "2026-10-02T10:00:00.000001Z"),
        )
        .await;
    broker
        .put_doc(
            TENANT_A,
            "cdc",
            source_doc(true, "2026-10-02T10:00:00.000001Z"),
        )
        .await;
    let mut going = source_doc(false, "2026-10-02T10:00:00.000001Z");
    going["deleting"] = json!({"dropSlot": true, "requestedAt": "2026-10-02T10:00:00.000001Z"});
    broker.put_doc(TENANT_B, "going", going).await;
    m.reconcile().await;
    for (tenant, name) in [
        (DEFAULT_TENANT, "copy"),
        (TENANT_A, "cdc"),
        (TENANT_B, "going"),
    ] {
        until(
            &format!("{tenant}/{name} reporting its error"),
            || status.render(),
            |st| unit_of(st, tenant, name)["error"]["code"].is_string(),
        )
        .await;
        let u = unit_of(&status.render(), tenant, name);
        assert_ne!(u["error"]["code"], "panic", "{u}");
        assert_ne!(u["error"]["code"], "config", "{u}");
        assert_ne!(u["phase"], "disabled", "{u}");
    }
    m.stop_everything().await;
    let st = status.render();
    assert_eq!(
        unit_of(&st, DEFAULT_TENANT, "copy")["phase"],
        "stopped",
        "{st}"
    );
    assert_eq!(unit_of(&st, TENANT_A, "cdc")["phase"], "stopped", "{st}");
    assert_eq!(unit_of(&st, TENANT_B, "going")["phase"], "stopped", "{st}");
    // The document of the source being deleted is still there: nothing was
    // torn down (the database was never reached).
    let doc = broker
        .kv_as(
            SYSTEM_TENANT,
            vec![json!({"op": "get", "ns": "queen-pg", "key": doc_key(TENANT_B, "going")})],
        )
        .await;
    assert_eq!(doc[0]["found"], true, "{doc:?}");
}

/// The only test that calls [`start_with`]: it sets the process-global status.
/// The manager on its own `queen-pg` runtime reads the documents, reports
/// them under `/status` and its families in the Prometheus text, and stops
/// inside its grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_runs_the_manager_on_its_own_runtime_and_stops_inside_the_grace() {
    let broker = Broker::open("start");
    broker
        .put_doc(
            TENANT_B,
            "start-copy",
            sink_doc(false, "2026-10-02T10:00:00.000001Z"),
        )
        .await;
    let knobs = NodeKnobs {
        reload_ms: 100,
        shutdown_grace_ms: 10_000,
        ..NodeKnobs::defaults("node-start")
    };
    let running = start_with(
        knobs,
        Arc::clone(&broker.rsm),
        Encryption::for_test([7u8; 32]),
    );
    until(
        "the document became a unit",
        || status_value().unwrap_or(Value::Null),
        |st| unit_of(st, TENANT_B, "start-copy")["phase"] == "disabled",
    )
    .await;
    let st = status_value().expect("the connectors report under /status");
    assert_eq!(st["mode"], "in-process", "{st}");
    assert_eq!(st["threads"], 2, "{st}");
    assert_eq!(st["reloadMs"], 100, "{st}");
    assert_eq!(
        status_of(TENANT_B, "start-copy").map(|u| u["phase"].clone()),
        Some(json!("disabled"))
    );
    assert!(status_of(TENANT_B, "nope").is_none());
    assert!(prometheus_text().is_some());

    // Removed while running: gone from the status within a reload or two.
    broker.delete_doc(TENANT_B, "start-copy").await;
    until(
        "the unit left",
        || status_value().unwrap_or(Value::Null),
        |st| unit_of(st, TENANT_B, "start-copy").is_null(),
    )
    .await;

    let stopped = Instant::now();
    running.shutdown().await;
    assert!(
        stopped.elapsed() < Duration::from_secs(10),
        "inside the grace"
    );
    assert_eq!(status_value().expect("still reported")["phase"], "stopped");
}

// ---------------------------------------------------------------------------
// Pieces
// ---------------------------------------------------------------------------

#[test]
fn the_connector_threads_are_not_core_threads() {
    use crate::obs::panic_policy::is_core_thread_name;
    assert!(!is_core_thread_name(THREAD_NAME));
    assert!(!is_core_thread_name(&format!("{THREAD_NAME}-main")));
}

#[test]
fn document_keys_name_a_tenant_and_a_connector() {
    let key = doc_key(TENANT_A, "orders-src");
    assert_eq!(key, format!("conn:{TENANT_A}:orders-src"));
    assert_eq!(parse_doc_key(&key), Some((TENANT_A, "orders-src")));
    for bad in [
        "conn:",
        "conn:no-name-here",
        "conn::orders",
        "conn:t:Orders",
        "conn:t:has:colon",
        "other:t:orders",
    ] {
        assert_eq!(parse_doc_key(bad), None, "{bad}");
    }
}

#[test]
fn the_fingerprint_follows_every_api_write() {
    let row = |doc: Value| KvRow {
        key: doc_key(TENANT_A, "cdc"),
        value: doc,
        version: 1,
    };
    let base = source_doc(true, "2026-10-02T10:00:00.000001Z");
    let a = Wanted::from_row(row(base.clone())).expect("a document");
    let mut resync = base.clone();
    resync["resyncRequestedAt"] = json!("2026-10-02T10:00:02Z");
    let mut deleting = base.clone();
    deleting["deleting"] = json!({"dropSlot": true, "requestedAt": "2026-10-02T10:00:03Z"});
    let mut edited = base.clone();
    edited["updatedAt"] = json!("2026-10-02T10:00:04Z");
    for changed in [resync, deleting, edited] {
        let b = Wanted::from_row(row(changed)).expect("a document");
        assert_ne!(a.fingerprint, b.fingerprint);
    }
    assert!(a.doc.is_ok(), "{:?}", a.doc.as_ref().err());
    assert_eq!(a.kind.as_deref(), Some("source"));
}
