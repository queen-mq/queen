//! The in-process sink against the real state machine: [`LocalQueen`] over a
//! single-node [`RaftFacade`] (receiver, planner, apply, reads), the crate's
//! [`Sink`] on top, an in-memory bucket in place of S3.
//!
//! What they prove:
//! - a queue reaches the lake exactly once — every record, its transaction id,
//!   its stamp, and its payload as the bytes it was stored as (a big integer
//!   stays exact) — while records keep arriving;
//! - the windows close on an idle broker: the sink's own lease refreshes are
//!   entries, and they move the applied clock the windows close against;
//! - a sink that stops gives its queue back, and the next one continues from
//!   the commit pointer without a duplicate;
//! - the retention hold reads the commit pointer this sink writes (the key, the
//!   namespace and the `tEnd` format agree end to end);
//! - [`start`] runs the sink on its own runtime, reports it under `/status` and
//!   in the Prometheus text, and stops it inside its grace.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};

use queen_s3::s3::MemoryStore;
use serde::Deserialize;
use serde_json::{json, Value};

use super::*;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{PushReq, RsmBuildCtx};

static SEQ: AtomicU64 = AtomicU64::new(0);

/// One single-node broker: the facade, the same facade as the trait object the
/// sink is handed, and the router. Its data directory (a few hundred KB) is
/// left behind: the router's state holds the facade, so it never shuts down
/// before the test process ends.
struct Broker {
    facade: Arc<RaftFacade>,
    rsm: Arc<dyn Rsm>,
    router: axum::Router,
}

impl Broker {
    fn open(tag: &str) -> Broker {
        std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
        let dir = std::env::temp_dir().join(format!(
            "queen-s3-inproc-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::env::set_var("QUEEN_RAFT_DIR", dir.join("cfg").display().to_string());
        let cfg = crate::config::load();
        let facade = Arc::new(
            RaftFacade::open(&RsmBuildCtx {
                data_dir: dir.display().to_string(),
                notifier: crate::notify::Notifier::new(false),
                disk_high_pct: 99.5,
                disk_low_pct: 99.0,
            })
            .expect("open the facade"),
        );
        let rsm: Arc<dyn Rsm> = facade.clone();
        let state = crate::handlers::raft::build_raft_state_with(&cfg, Some(Arc::clone(&rsm)))
            .expect("raft state");
        let router = crate::handlers::raft::build_raft_router(state, auth_off(), false);
        Broker {
            facade,
            rsm,
            router,
        }
    }

    fn queen(&self) -> Arc<dyn QueenApi> {
        Arc::new(LocalQueen::new(
            Arc::clone(&self.rsm),
            self.router.clone(),
            tokio::runtime::Handle::current(),
        ))
    }

    /// Push `items`, each one item of a push body as RAW JSON text: built as
    /// text so a payload reaches the broker exactly as written (a
    /// `serde_json::Value` would already have turned a big integer into a
    /// float on this side).
    async fn push(&self, items: Vec<String>) {
        self.push_as(crate::config::DEFAULT_TENANT, items).await
    }

    /// [`Broker::push`] for `tenant`.
    async fn push_as(&self, tenant: &str, items: Vec<String>) {
        let out = self
            .facade
            .push(
                ctx_for(tenant),
                PushReq {
                    raw: format!("{{\"items\":[{}]}}", items.join(",")).into_bytes(),
                },
            )
            .await
            .expect("push");
        let v: Value = serde_json::from_str(&out.body).expect("push body");
        for it in v.as_array().expect("push answers an array") {
            assert_eq!(it["status"], "queued", "{it}");
        }
    }

    async fn api(&self, method: &str, path: &str, body: Value) -> (u16, Value) {
        let out = self
            .facade
            .api(
                ctx(),
                ApiReq {
                    method: method.to_string(),
                    path: path.to_string(),
                    query: None,
                    body: body.to_string().into_bytes(),
                },
            )
            .await
            .expect("api call");
        let v = serde_json::from_str(&out.body).unwrap_or(Value::Null);
        (out.status, v)
    }

    /// The log itself, read with the sink's own fetch twin: every record of
    /// every partition of `queue`, as `(partition, offset) -> (txn, ts, payload)`.
    async fn log(&self, queue: &str) -> BTreeMap<(String, i64), (String, i64, Vec<u8>)> {
        self.log_as(crate::config::DEFAULT_TENANT, queue).await
    }

    /// [`Broker::log`] for `tenant`.
    async fn log_as(
        &self,
        tenant: &str,
        queue: &str,
    ) -> BTreeMap<(String, i64), (String, i64, Vec<u8>)> {
        let answer = self
            .rsm
            .partitions_changed(
                ctx_for(tenant),
                vec![ChangedAsk {
                    queue: queue.to_string(),
                    since_us: None,
                    after: None,
                    limit: 1000,
                }],
            )
            .await
            .expect("discovery");
        let partitions = &answer.entries[0].partitions;
        assert!(partitions.len() < 1000, "one page lists the test's queue");
        let mut out = BTreeMap::new();
        for p in partitions {
            let mut offset = 0u64;
            loop {
                let read = self
                    .rsm
                    .fetch_log(
                        ctx_for(tenant),
                        vec![RecordFetch {
                            queue: queue.to_string(),
                            partition: p.name.clone(),
                            offset,
                            max_bytes: 8 << 20,
                        }],
                        0,
                        1,
                    )
                    .await
                    .expect("fetch_log");
                let entry = &read[0];
                assert_eq!(entry.error, None, "{}", p.name);
                for r in &entry.records {
                    out.insert(
                        (p.name.clone(), r.offset as i64),
                        (
                            r.txn.clone().expect("fetch_log carries the txn id"),
                            r.created_at_us,
                            r.payload.to_vec(),
                        ),
                    );
                }
                match entry.records.last() {
                    Some(last) if last.offset + 1 < entry.high_watermark => {
                        offset = last.offset + 1
                    }
                    _ => break,
                }
            }
        }
        out
    }

    async fn safe_time(&self) -> i64 {
        self.rsm
            .partitions_changed(ctx(), Vec::new())
            .await
            .expect("discovery")
            .safe_time_us
    }

    /// One KV batch for `tenant`, answered as its results.
    async fn kv_as(&self, tenant: &str, ops: Vec<Value>) -> Vec<Value> {
        self.rsm
            .kv(ctx_for(tenant), crate::rsm::facade::KvReq { ops })
            .await
            .unwrap_or_else(|_| panic!("a KV batch for {tenant}"))
            .results
    }

    /// The control plane's row of a customer (a proxy tenant, the owner of
    /// clusters) with `status`.
    async fn put_owner(&self, owner: &str, status: &str) {
        use queen_proxy::store::schema::{ns, TenantDoc, PROXY_TENANT};
        let doc: TenantDoc = serde_json::from_value(json!({
            "id": owner,
            "slug": format!("owner-{status}"),
            "name": "Owner",
            "status": status,
            "created_at_us": 0,
        }))
        .expect("a TenantDoc");
        let results = self
            .kv_as(
                PROXY_TENANT,
                vec![
                    json!({"op": "put", "ns": ns::TENANTS, "key": format!("#{owner}"),
                           "value": serde_json::to_value(&doc).unwrap(), "forever": true}),
                ],
            )
            .await;
        assert!(results.iter().all(|r| r["applied"] == true), "{results:?}");
    }

    /// What the control plane writes for a cluster of `owner` whose broker
    /// tenant is `tenant`: its cluster row (status `status`) and its sink row,
    /// the secret sealed with `encryption` — the proxy's own documents, typed
    /// through its structs so a field drifting on either side fails here.
    #[allow(clippy::too_many_arguments)]
    async fn configure_tenant(
        &self,
        owner: &str,
        tenant: &str,
        cluster: &str,
        status: &str,
        bucket: &str,
        enabled: bool,
        encryption: &crate::encryption::Encryption,
        updated_at_us: i64,
    ) {
        use queen_proxy::store::schema::{ns, ClusterDoc, S3SinkDoc, PROXY_TENANT};
        let cluster_doc: ClusterDoc = serde_json::from_value(json!({
            "id": cluster,
            "tenant_id": owner,
            "cell_id": "44444444-4444-4444-8444-444444444444",
            "plan_id": "55555555-5555-4555-8555-555555555555",
            "slug": format!("c-{bucket}"),
            "broker_tenant_uuid": tenant,
            "status": status,
            "limit_overrides": {},
            "created_at_us": 0,
        }))
        .expect("a ClusterDoc");
        let sealed = String::from_utf8(
            encryption
                .encrypt(format!("secret-of-{bucket}").as_bytes())
                .expect("sealed with the test key"),
        )
        .expect("the envelope is text");
        let sink_doc: S3SinkDoc = serde_json::from_value(json!({
            "cluster_id": cluster,
            "broker_tenant": tenant,
            "enabled": enabled,
            "config": {
                "endpoint": "http://lake.invalid:7070",
                "region": "us-east-1",
                "bucket": bucket,
                "accessKey": format!("ak-{bucket}"),
                "queues": "orders",
                "start": "earliest",
                "align": "none",
                "maxWindowMs": 200,
                "compression": "none",
            },
            "secret_key_sealed": sealed,
            "updated_at_us": updated_at_us,
        }))
        .expect("an S3SinkDoc");
        let results = self
            .kv_as(
                PROXY_TENANT,
                vec![
                    json!({"op": "put", "ns": ns::CLUSTERS, "key": format!("#{cluster}"),
                           "value": serde_json::to_value(&cluster_doc).unwrap(), "forever": true}),
                    json!({"op": "put", "ns": ns::S3SINKS, "key": format!("#{cluster}"),
                           "value": serde_json::to_value(&sink_doc).unwrap(), "forever": true}),
                ],
            )
            .await;
        assert!(results.iter().all(|r| r["applied"] == true), "{results:?}");
    }

    /// The control plane deleting a cluster's sink row.
    async fn unconfigure_cluster(&self, cluster: &str) {
        use queen_proxy::store::schema::{ns, PROXY_TENANT};
        self.kv_as(
            PROXY_TENANT,
            vec![json!({"op": "delete", "ns": ns::S3SINKS, "key": format!("#{cluster}")})],
        )
        .await;
    }
}

fn ctx() -> ReqCtx {
    ctx_for(crate::config::DEFAULT_TENANT)
}

fn ctx_for(tenant: &str) -> ReqCtx {
    ReqCtx::new(tenant, Deadline::after(Duration::from_secs(30)))
}

fn auth_off() -> Arc<crate::auth::Authenticator> {
    crate::auth::Authenticator::new(crate::config::AuthConfig {
        enabled: false,
        algorithm: "HS256".into(),
        secret: String::new(),
        public_key: String::new(),
        jwks_url: String::new(),
        jwks_refresh_interval_seconds: 3600,
        jwks_request_timeout_ms: 5000,
        issuer: String::new(),
        audience: String::new(),
        clock_skew_seconds: 30,
        skip_paths: Vec::new(),
        roles_claim: "role".into(),
        roles_array_claim: "roles".into(),
        role_admin: "admin".into(),
        role_read_write: "read-write".into(),
        role_read_only: "read-only".into(),
        role_write_only: "write-only".into(),
    })
}

/// A sink of `queues`, as node `instance`, with windows short enough for a
/// test: no alignment, a 200 ms age close, no guard (the applied clock is
/// sound on its own), uncompressed JSONL, and a 1 s lease — refreshed every
/// third of it, which is also what moves an idle broker's clock.
fn config(queues: &str, instance: &str) -> Config {
    Config::from_pairs_with(
        &[
            ("QUEEN_S3_QUEUES", queues),
            ("QUEEN_S3_ENDPOINT", "http://lake.invalid:7070"),
            ("QUEEN_S3_REGION", "us-east-1"),
            ("QUEEN_S3_BUCKET", "lake"),
            ("QUEEN_S3_ACCESS_KEY", "ak"),
            ("QUEEN_S3_SECRET_KEY", "sk"),
            ("QUEEN_S3_START", "earliest"),
            ("QUEEN_S3_ALIGN", "none"),
            ("QUEEN_S3_MAX_WINDOW_MS", "200"),
            ("QUEEN_S3_SAFE_GUARD_MS", "0"),
            ("QUEEN_S3_DISCOVERY_INTERVAL_MS", "20"),
            ("QUEEN_S3_COMPRESSION", "none"),
            ("QUEEN_S3_LEASE_TTL_MS", "1000"),
        ],
        instance,
    )
    .expect("the test configuration parses")
}

/// Run `sink` on a task until the returned sender fires.
fn run(
    sink: &Arc<Sink>,
) -> (
    tokio::sync::oneshot::Sender<()>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();
    let sink = Arc::clone(sink);
    let handle = tokio::spawn(async move {
        sink.run(async {
            let _ = rx.await;
        })
        .await
    });
    (tx, handle)
}

async fn stop(tx: tokio::sync::oneshot::Sender<()>, handle: tokio::task::JoinHandle<()>) {
    let _ = tx.send(());
    tokio::time::timeout(Duration::from_secs(30), handle)
        .await
        .expect("the sink drains inside 30 s")
        .expect("the sink's run does not panic");
}

/// One queue's row of a status document, or `null`.
fn row(status: &Value, queue: &str) -> Value {
    status["queues"]
        .as_array()
        .and_then(|qs| qs.iter().find(|q| q["name"] == queue).cloned())
        .unwrap_or(Value::Null)
}

/// Wait until `done` holds, or fail naming `what` with the status it saw.
async fn until(what: &str, mut status: impl FnMut() -> Value, done: impl Fn(&Value) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let st = status();
        if done(&st) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; status: {st}"
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// One line of an uncompressed JSONL object, its payload kept as raw text.
#[derive(Deserialize)]
struct Line<'a> {
    partition: String,
    offset: i64,
    #[serde(rename = "transactionId")]
    transaction_id: String,
    ts: String,
    #[serde(borrow)]
    payload: Option<&'a RawValue>,
}

/// Every record line of every data object of the bucket, as
/// `((partition, offset), (txn, ts, payload text))`, in key order. Duplicates
/// are kept: the caller counts them.
/// One record line of the lake: `(partition, offset)` and `(txn, ts, payload)`.
type LakeRow = ((String, i64), (String, String, Option<String>));

fn lake(store: &MemoryStore) -> Vec<LakeRow> {
    let mut keys: Vec<String> = store
        .keys()
        .into_iter()
        .filter(|k| !k.contains("/_queen/"))
        .collect();
    keys.sort();
    let mut out = Vec::new();
    for key in keys {
        assert!(
            key.ends_with(".jsonl"),
            "uncompressed JSONL objects only: {key}"
        );
        let bytes = store.bytes_of(&key).expect("a listed object");
        let text = std::str::from_utf8(&bytes).expect("JSONL is UTF-8");
        for line in text.lines().filter(|l| !l.is_empty()) {
            let l: Line<'_> = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("{key}: not an envelope line: {e}: {line}"));
            out.push((
                (l.partition, l.offset),
                (
                    l.transaction_id,
                    l.ts,
                    l.payload.map(|p| p.get().to_string()),
                ),
            ));
        }
    }
    out
}

/// The lake holds `log` exactly: every record once, nothing else, each with
/// the log's transaction id, stamp and payload bytes.
fn assert_lake_is(store: &MemoryStore, log: &BTreeMap<(String, i64), (String, i64, Vec<u8>)>) {
    let rows = lake(store);
    let mut seen: BTreeMap<(String, i64), usize> = BTreeMap::new();
    for (key, (txn, ts, payload)) in &rows {
        *seen.entry(key.clone()).or_default() += 1;
        let Some((want_txn, want_ts, want_payload)) = log.get(key) else {
            panic!("the lake holds {key:?}, which is not in the log");
        };
        assert_eq!(txn, want_txn, "{key:?}: transaction id");
        assert_eq!(
            ts,
            &crate::rsm::planner::timers::iso_us(*want_ts),
            "{key:?}: ts is the record's stamp"
        );
        let want = (!want_payload.is_empty()).then(|| {
            String::from_utf8(want_payload.clone()).expect("the test's payloads are UTF-8")
        });
        assert_eq!(payload, &want, "{key:?}: the payload is the stored bytes");
    }
    let dups: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
    assert!(dups.is_empty(), "records written more than once: {dups:?}");
    let missing: Vec<_> = log.keys().filter(|k| !seen.contains_key(*k)).collect();
    assert!(
        missing.is_empty(),
        "records the lake does not hold: {missing:?}"
    );
}

/// `n` push items as raw JSON text, spread over `partitions` partitions of
/// `queue`, with payloads a JSON round trip would change: integers beyond 64
/// bits, a float with a trailing zero, escapes and non-ASCII text.
fn records(queue: &str, partitions: usize, n: usize, tag: &str) -> Vec<String> {
    (0..n)
        .map(|i| {
            format!(
                r#"{{"queue":"{queue}","partition":"p{}","payload":{{"tag":"{tag}","i":{i},"big":123456789012345678901234567890{i},"f":1.50,"s":"ü\n\"{i}\""}}}}"#,
                i % partitions
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// The lake
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_reaches_the_lake_exactly_once_with_its_payloads_as_stored() {
    let broker = Broker::open("once");
    for chunk in records("orders", 12, 300, "a").chunks(50) {
        broker.push(chunk.to_vec()).await;
    }
    let store = Arc::new(MemoryStore::new());
    let sink = Arc::new(
        Sink::new(
            config("orders", "node-1@test"),
            broker.queen(),
            Some(store.clone()),
        )
        .expect("the sink builds"),
    );
    let (tx, handle) = run(&sink);
    until(
        "the first 300 records committed",
        || sink.status(),
        |st| row(st, "orders")["records"] == 300,
    )
    .await;

    // More arrive while it runs, and it ships them too.
    for chunk in records("orders", 17, 200, "b").chunks(40) {
        broker.push(chunk.to_vec()).await;
    }
    until(
        "all 500 records committed",
        || sink.status(),
        |st| row(st, "orders")["records"] == 500,
    )
    .await;
    let st = sink.status();
    let q = row(&st, "orders");
    assert_eq!(q["ownedHere"], true, "{st}");
    assert_eq!(q["recordsLost"], 0, "{st}");
    stop(tx, handle).await;

    let log = broker.log("orders").await;
    assert_eq!(log.len(), 500, "the oracle reads every pushed record");
    assert_lake_is(&store, &log);

    // The last window closed although nothing was pushed after its records:
    // the sink's lease refreshes are entries, and they moved the clock.
    let newest = log.values().map(|(_, ts, _)| *ts).max().expect("records");
    assert!(
        broker.safe_time().await > newest,
        "an idle broker's safeTime moved past the newest record"
    );
    // A big integer is in the lake digit for digit.
    let any = &lake(&store)[0].1 .2;
    assert!(
        any.as_deref()
            .is_some_and(|p| p.contains("123456789012345678901234567890")),
        "{any:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sink_that_stops_hands_its_queue_to_the_next_one() {
    let broker = Broker::open("handover");
    broker.push(records("orders", 5, 100, "a")).await;
    let store = Arc::new(MemoryStore::new());

    let first = Arc::new(
        Sink::new(
            config("orders", "node-1@test"),
            broker.queen(),
            Some(store.clone()),
        )
        .expect("the first sink builds"),
    );
    let (tx, handle) = run(&first);
    until(
        "node 1 committed the first 100",
        || first.status(),
        |st| row(st, "orders")["records"] == 100,
    )
    .await;
    stop(tx, handle).await;

    broker.push(records("orders", 9, 150, "b")).await;
    let second = Arc::new(
        Sink::new(
            config("orders", "node-2@test"),
            broker.queen(),
            Some(store.clone()),
        )
        .expect("the second sink builds"),
    );
    let claimed = Instant::now();
    let (tx, handle) = run(&second);
    until(
        "node 2 committed the next 150",
        || second.status(),
        |st| row(st, "orders")["records"] == 150,
    )
    .await;
    stop(tx, handle).await;
    assert!(
        claimed.elapsed() < Duration::from_secs(30),
        "the stopped sink gave its lease back"
    );

    let log = broker.log("orders").await;
    assert_eq!(log.len(), 250);
    assert_lake_is(&store, &log);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_retention_hold_follows_the_commit_pointer_the_sink_writes() {
    let broker = Broker::open("hold");
    // Retention would cut at now − 60 s; the hold keeps it at the commit
    // pointer's tEnd − 60 s, under the default 7-day cap.
    for (queue, options) in [
        (
            "held",
            json!({"retentionEnabled": true, "retentionSeconds": 60, "retentionSinkHold": "default"}),
        ),
        (
            "free",
            json!({"retentionEnabled": true, "retentionSeconds": 60}),
        ),
    ] {
        let (status, body) = broker
            .api(
                "POST",
                "/api/v1/configure",
                json!({"queue": queue, "options": options}),
            )
            .await;
        assert_eq!(status, 200, "configure {queue}: {body}");
    }
    broker.push(records("held", 4, 40, "a")).await;
    let store = Arc::new(MemoryStore::new());
    let sink = Arc::new(
        Sink::new(
            config("held", "node-1@test"),
            broker.queen(),
            Some(store.clone()),
        )
        .expect("the sink builds"),
    );
    let (tx, handle) = run(&sink);
    until(
        "the queue committed",
        || sink.status(),
        |st| row(st, "held")["records"] == 40,
    )
    .await;
    stop(tx, handle).await;

    // The pointer the sink wrote, read the way the hold reads it.
    let out = broker
        .rsm
        .kv(
            ctx(),
            crate::rsm::facade::KvReq {
                ops: vec![json!({"op":"get","ns":"queen-s3","key":"s3:default:held:committed"})],
            },
        )
        .await
        .unwrap_or_else(|_| panic!("the KV read of the commit pointer"));
    let body = &out.results[0];
    let t_end = body["value"]["tEnd"]
        .as_str()
        .unwrap_or_else(|| panic!("tEnd is an ISO string: {body}"))
        .to_string();
    let t_end_ms = crate::util::parse_iso_ms(&t_end).expect("the hold parses tEnd");

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_micros() as i64;
    let held = broker
        .facade
        .retention_cutoffs_for_test(crate::config::DEFAULT_TENANT, "held", now)
        .expect("the queue exists");
    let free = broker
        .facade
        .retention_cutoffs_for_test(crate::config::DEFAULT_TENANT, "free", now)
        .expect("the queue exists");
    let floor = t_end_ms * 1_000 - 60_000_000;
    assert!(floor < now - 60_000_000, "the pointer is behind now");
    assert!(
        format!("{held:?}").contains(&format!("all: Some({floor})")),
        "the held queue's cutoff is the pointer's tEnd − 60 s ({floor}): {held:?}"
    );
    assert!(
        format!("{free:?}").contains(&format!("all: Some({})", now - 60_000_000)),
        "a queue without the hold cuts at now − 60 s: {free:?}"
    );
}

/// The fence, through the adapter: a `required` write that loses its
/// precondition is the sink's `Precondition` — "another node owns this queue",
/// which stops the queue task without a retry — never a status error the sink
/// would read as a terminal failure, and nothing of the batch is applied.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_lost_fence_reaches_the_sink_as_a_precondition_and_writes_nothing() {
    let broker = Broker::open("fence");
    let queen = broker.queen();
    let fenced = queen
        .kv(vec![
            KvOp::Put {
                key: "s3:default:orders:lease".into(),
                value: json!({"instance": "node-9"}),
                ttl_seconds: Some(30),
                expect: Some(7),
                required: true,
            },
            KvOp::put("s3:default:orders:committed", json!({"k": 1})),
        ])
        .await;
    match fenced {
        Err(SinkError::Precondition { failed_index, .. }) => assert_eq!(failed_index, 0),
        other => panic!("expected the fence to answer a precondition, got {other:?}"),
    }
    let read = queen
        .kv(vec![KvOp::get("s3:default:orders:committed")])
        .await
        .expect("a read");
    assert_eq!(
        read[0].found,
        Some(false),
        "the batch rolled back: {:?}",
        read[0]
    );
    // And a write that holds goes through, read back by the same path.
    queen
        .kv(vec![KvOp::put(
            "s3:default:orders:committed",
            json!({"k": 2}),
        )])
        .await
        .expect("an unconditional write");
    let read = queen
        .kv(vec![KvOp::get("s3:default:orders:committed")])
        .await
        .expect("a read");
    assert_eq!(read[0].value["k"], 2, "{:?}", read[0]);
}

// ---------------------------------------------------------------------------
// The in-process runtime
// ---------------------------------------------------------------------------

const TENANT_A: &str = "11111111-1111-4111-8111-111111111111";
const TENANT_B: &str = "22222222-2222-4222-8222-222222222222";
const TENANT_C: &str = "66666666-6666-4666-8666-666666666666";
const TENANT_D: &str = "77777777-7777-4777-8777-777777777777";
const TENANT_E: &str = "88888888-8888-4888-8888-888888888888";
const CLUSTER_E: &str = "eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee";
/// The customers (proxy tenants) owning the clusters.
const OWNER: &str = "33333333-3333-4333-8333-333333333333";
const OWNER_SUSPENDED: &str = "99999999-9999-4999-8999-999999999999";
const CLUSTER_A: &str = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa";
const CLUSTER_B: &str = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb";
const CLUSTER_C: &str = "cccccccc-cccc-4ccc-8ccc-cccccccccccc";
const CLUSTER_D: &str = "dddddddd-dddd-4ddd-8ddd-dddddddddddd";

/// The sink entry of `tenant` in an `s3` status block, or `null`.
fn sink_of(status: &Value, tenant: &str) -> Value {
    status["sinks"]
        .as_array()
        .and_then(|s| s.iter().find(|s| s["tenant"] == tenant).cloned())
        .unwrap_or(Value::Null)
}

/// Every key of `store` is under `tenant`'s own paths.
fn assert_keys_are_tenants(store: &MemoryStore, tenant: &str) {
    let keys = store.keys();
    assert!(!keys.is_empty(), "the bucket of {tenant} holds nothing");
    for k in keys {
        assert!(
            k.starts_with(&format!("queen/tenant={tenant}/"))
                || k.starts_with(&format!("queen/_queen/tenant={tenant}/")),
            "{k} is not under tenant {tenant}"
        );
    }
}

/// The only test that calls [`start_with`]: it sets the process-global status.
///
/// The default tenant's sink from the environment, and two tenants the control
/// plane configured — all three writing a queue named `orders` — each into its
/// own bucket, under its own `tenant=` path, with its commit pointers in its
/// own KV; one status entry and one set of labelled metrics per tenant; a
/// tenant disabled and another deleted stop and leave both; then the whole
/// sink stops inside its grace.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_runs_every_tenants_sink_into_its_own_bucket_and_stops_them() {
    let broker = Broker::open("start");
    broker.push(records("orders", 3, 30, "default")).await;
    broker
        .push_as(TENANT_A, records("orders", 2, 20, "a"))
        .await;
    broker
        .push_as(TENANT_B, records("orders", 4, 40, "b"))
        .await;
    let encryption = crate::encryption::Encryption::for_test([7u8; 32]);
    broker.put_owner(OWNER, "active").await;
    broker.put_owner(OWNER_SUSPENDED, "suspended").await;
    broker
        .configure_tenant(
            OWNER,
            TENANT_A,
            CLUSTER_A,
            "active",
            "lake-a",
            true,
            &encryption,
            1,
        )
        .await;
    broker
        .configure_tenant(
            OWNER,
            TENANT_B,
            CLUSTER_B,
            "push_blocked",
            "lake-b",
            true,
            &encryption,
            1,
        )
        .await;
    // C's secret was sealed by another cell's key: this node cannot open it,
    // says so, and runs the others.
    let foreign = crate::encryption::Encryption::for_test([8u8; 32]);
    broker
        .configure_tenant(
            OWNER, TENANT_C, CLUSTER_C, "active", "lake-c", true, &foreign, 1,
        )
        .await;
    // D's cluster is suspended: no sink.
    broker
        .configure_tenant(
            OWNER,
            TENANT_D,
            CLUSTER_D,
            "suspended",
            "lake-d",
            true,
            &encryption,
            1,
        )
        .await;
    // E's cluster is active but its owner is suspended: the worse status
    // wins, as on the data plane — no sink.
    broker
        .configure_tenant(
            OWNER_SUSPENDED,
            TENANT_E,
            CLUSTER_E,
            "active",
            "lake-e",
            true,
            &encryption,
            1,
        )
        .await;

    let buckets: Arc<Mutex<HashMap<String, Arc<MemoryStore>>>> = Arc::default();
    let bucket = {
        let buckets = Arc::clone(&buckets);
        move |name: &str| -> Arc<MemoryStore> {
            Arc::clone(
                buckets
                    .lock()
                    .unwrap()
                    .entry(name.to_string())
                    .or_insert_with(|| Arc::new(MemoryStore::new())),
            )
        }
    };
    let store_for: StoreFor = {
        let bucket = bucket.clone();
        Arc::new(move |cfg: &Config| {
            let store: Arc<dyn ObjectStore> = bucket(&cfg.bucket);
            Some(store)
        })
    };
    let env = config("orders", "node-1@test");
    let pre = Preflight {
        node: env.node_knobs(),
        env: Some(env),
    };
    let knobs = S3SinkConfig {
        enabled: true,
        shutdown_grace_ms: 20_000,
    };
    let running = start_with(
        &knobs,
        pre,
        broker.router.clone(),
        Arc::clone(&broker.rsm),
        Some(store_for),
        Arc::clone(&encryption),
    );
    let default = crate::config::DEFAULT_TENANT;
    until(
        "every tenant's sink committed its queue",
        || status_value().unwrap_or(Value::Null),
        |st| {
            row(&sink_of(st, default), "orders")["records"] == 30
                && row(&sink_of(st, TENANT_A), "orders")["records"] == 20
                && row(&sink_of(st, TENANT_B), "orders")["records"] == 40
        },
    )
    .await;

    let st = status_value().expect("the sinks report under /status");
    assert_eq!(st["mode"], "in-process", "{st}");
    assert_eq!(st["phase"], "running", "{st}");
    assert!(st["threads"].as_u64().is_some_and(|n| n >= 1), "{st}");
    assert_eq!(st["controlPlane"]["error"], Value::Null, "{st}");
    // default, A, B, and C in error; never D (its cluster is suspended) nor
    // E (its owner is).
    assert_eq!(st["sinks"].as_array().map(Vec::len), Some(4), "{st}");
    assert_eq!(sink_of(&st, TENANT_D), Value::Null, "{st}");
    assert_eq!(sink_of(&st, TENANT_E), Value::Null, "{st}");
    let c = sink_of(&st, TENANT_C);
    assert_eq!(c["phase"], "error", "{c}");
    assert!(
        c["error"]
            .as_str()
            .is_some_and(|e| e.contains("QUEEN_ENCRYPTION_KEY")),
        "{c}"
    );
    assert!(
        bucket("lake-c").keys().is_empty(),
        "nothing was written for C"
    );
    let d = sink_of(&st, default);
    assert_eq!(d["source"], "env", "{d}");
    assert_eq!(d["phase"], "running", "{d}");
    assert_eq!(d["instance"], "node-1@test", "{d}");
    assert_eq!(d["bucket"]["name"], "lake", "{d}");
    assert_eq!(d["bucket"]["reachable"], true, "{d}");
    let a = sink_of(&st, TENANT_A);
    assert_eq!(a["source"], "cp", "{a}");
    assert_eq!(a["cluster"], CLUSTER_A, "{a}");
    assert_eq!(a["bucket"]["name"], "lake-a", "{a}");
    assert_eq!(sink_of(&st, TENANT_B)["bucket"]["name"], "lake-b", "{st}");

    // Each bucket holds its tenant's records and nothing else, under its
    // tenant's paths: the three `orders` queues never meet.
    for (tenant, name) in [
        (default, "lake"),
        (TENANT_A, "lake-a"),
        (TENANT_B, "lake-b"),
    ] {
        let store = bucket(name);
        assert_keys_are_tenants(&store, tenant);
        assert_lake_is(&store, &broker.log_as(tenant, "orders").await);
    }
    // The commit pointers live in each tenant's own KV, where the retention
    // hold of its queues reads them.
    for tenant in [default, TENANT_A, TENANT_B] {
        let got = broker
            .kv_as(
                tenant,
                vec![json!({"op":"get","ns":"queen-s3","key":"s3:default:orders:committed"})],
            )
            .await;
        assert_eq!(got[0]["found"], true, "{tenant}: {got:?}");
    }

    // One exposition: every family once, a tenant label on each tenant's.
    let text = prometheus_text().expect("the sinks' metrics");
    assert_eq!(
        text.matches("# TYPE queen_s3_lag_seconds ").count(),
        1,
        "{text}"
    );
    assert!(text.contains(&format!("tenant=\"{TENANT_A}\"")), "{text}");
    assert!(text.contains(&format!("tenant=\"{TENANT_B}\"")), "{text}");

    // The control plane disables A and deletes B: both stop, drain and leave
    // the status and the metrics; the default tenant's sink runs on.
    broker
        .configure_tenant(
            OWNER,
            TENANT_A,
            CLUSTER_A,
            "active",
            "lake-a",
            false,
            &encryption,
            2,
        )
        .await;
    broker.unconfigure_cluster(CLUSTER_B).await;
    broker.unconfigure_cluster(CLUSTER_C).await;
    until(
        "A, B and C removed",
        || status_value().unwrap_or(Value::Null),
        |st| st["sinks"].as_array().map(Vec::len) == Some(1),
    )
    .await;
    let st = status_value().expect("the sinks report under /status");
    assert_eq!(sink_of(&st, default)["phase"], "running", "{st}");
    let text = prometheus_text().expect("the sinks' metrics");
    assert!(
        !text.contains(TENANT_A) && !text.contains(TENANT_B),
        "{text}"
    );

    let stopped = Instant::now();
    running.shutdown().await;
    assert!(
        stopped.elapsed() < Duration::from_secs(20),
        "inside the grace"
    );
    let st = status_value().expect("still reported");
    assert_eq!(st["phase"], "stopped", "{st}");
    assert_eq!(sink_of(&st, default)["phase"], "stopped", "{st}");
    assert_eq!(sink_of(&st, default)["running"], false, "{st}");
}

/// The control plane's side: a document checked by the sink's own rules, a
/// secret sealed with the cell's key and opened by it — and refused outright
/// on a cell without one.
#[test]
fn the_control_plane_seals_with_the_cells_key_and_refuses_without_one() {
    use queen_proxy::s3::S3Sinks;
    let encryption = crate::encryption::Encryption::for_test([9u8; 32]);
    let hooks = ControlPlaneHooks::with(Arc::clone(&encryption));
    let sealed = hooks.seal("s3cr3t").expect("a cell with a key seals");
    assert!(!sealed.contains("s3cr3t"), "{sealed}");
    assert_eq!(
        encryption
            .decrypt_payload_bytes(sealed.as_bytes())
            .as_deref(),
        Some(&b"s3cr3t"[..])
    );

    let keyless =
        ControlPlaneHooks::with(Arc::new(crate::encryption::Encryption::disabled_for_test()));
    let refused = keyless.seal("s3cr3t").expect_err("no key, no secret");
    assert!(refused.contains("QUEEN_ENCRYPTION_KEY"), "{refused}");

    let ok = json!({"endpoint":"https://s3.example.com","region":"eu-central-1","bucket":"lake",
                    "accessKey":"AKIA","queues":"*"});
    hooks.validate(TENANT_A, &ok).expect("a complete document");
    let missing = json!({"endpoint":"https://s3.example.com","region":"eu-central-1",
                         "accessKey":"AKIA","queues":"*"});
    let why = hooks.validate(TENANT_A, &missing).expect_err("no bucket");
    assert!(why.contains("bucket"), "{why}");
}

// ---------------------------------------------------------------------------
// Pieces
// ---------------------------------------------------------------------------

#[test]
fn the_sink_threads_are_not_core_threads() {
    use crate::obs::panic_policy::is_core_thread_name;
    assert!(!is_core_thread_name(THREAD_NAME));
    assert!(!is_core_thread_name(&format!("{THREAD_NAME}-main")));
}

#[test]
fn a_payload_is_handed_over_as_the_bytes_it_was_stored_as() {
    let big = br#"{"n":123456789012345678901234567890,"f":1.50}"#;
    assert_eq!(
        payload_value(big).map(|p| p.get().to_string()).as_deref(),
        Some(r#"{"n":123456789012345678901234567890,"f":1.50}"#)
    );
    assert_eq!(payload_value(b"").map(|p| p.get().to_string()), None);
    assert_eq!(
        payload_value(b"\"text\"")
            .map(|p| p.get().to_string())
            .as_deref(),
        Some("\"text\"")
    );
    // Not JSON (a payload the broker could not decrypt): a JSON string of its
    // lossy UTF-8, as the route renders it.
    assert_eq!(
        payload_value(b"\xffnot json")
            .map(|p| p.get().to_string())
            .as_deref(),
        Some("\"\u{fffd}not json\"")
    );
}

#[test]
fn the_queue_list_is_read_for_its_names() {
    assert_eq!(
        queue_names(r#"{"queues":[{"name":"a","messages":3},{"name":""},{"x":1},{"name":"b"}]}"#)
            .unwrap(),
        vec!["a".to_string(), "b".to_string()]
    );
    assert!(queue_names("{}").unwrap().is_empty());
    assert!(queue_names("not json").is_err());
}

/// An explicit instance — one value for every pod, easily — is made this
/// node's own, so two nodes never take each other's leases for their own.
#[test]
fn an_explicit_instance_is_made_unique_to_the_node() {
    assert_eq!(unique_instance("lake-writer", 2), "lake-writer/node-2");
    assert_eq!(
        unique_instance("lake-writer/node-2", 2),
        "lake-writer/node-2"
    );
    assert_ne!(
        unique_instance("lake-writer", 1),
        unique_instance("lake-writer", 3)
    );
}

/// The default already names the node: it is used as it is, not suffixed a
/// second time (`node-1@host/node-1` before this test).
#[test]
fn only_an_explicit_instance_gets_the_node_suffix() {
    assert_eq!(
        lease_instance("node-1@queen-mq-v2-0", false, 1),
        "node-1@queen-mq-v2-0"
    );
    assert_eq!(lease_instance("lake-writer", true, 2), "lake-writer/node-2");
}

#[test]
fn the_default_instance_names_the_node_and_the_host() {
    let instance = instance_default();
    assert!(instance.starts_with("node-"), "{instance}");
    assert!(instance.contains('@'), "{instance}");
}
