//! Scoped requests through the broker router (`scope.rs`, `queen_protocol::scope`):
//! a client the proxy scoped reaches only the names its grants cover.
//!
//! Each test opens one node over its own data directory and calls the router
//! in-process, the way the embedded proxy hands a request on. The scope rides
//! in the header (as on a relay) unless a test says otherwise. What they prove:
//! - a push, a pop, an ack, a transaction, a KV call and a timer call that name
//!   anything outside the grants are refused whole with `403 scope_denied`,
//!   and nothing of them is written;
//! - an ack, which names its partition by number, cannot move the cursor of a
//!   partition outside the grants, with or without a lease;
//! - every route a scoped client has no business on is refused, a near-miss
//!   spelling included, and a scope that does not decode refuses the request;
//! - a request with no scope is served as before.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use axum::Router;
use queen_protocol::scope::{layer, Grant, Scope, SCOPE_HEADER};
use serde_json::{json, Value};
use tower::ServiceExt;

use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{Rsm, RsmBuildCtx};

static SEQ: AtomicU64 = AtomicU64::new(0);

fn scratch(tag: &str) -> PathBuf {
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-scope-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

fn build_ctx(dir: &Path) -> RsmBuildCtx {
    RsmBuildCtx {
        data_dir: dir.display().to_string(),
        notifier: crate::notify::Notifier::new(false),
        disk_high_pct: 99.0,
        disk_low_pct: 98.0,
    }
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

struct Node {
    router: Router,
    facade: Arc<RaftFacade>,
    dir: PathBuf,
}

impl Drop for Node {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// One node, its router as the embedded proxy sees it: tenancy on, broker JWT
/// off.
fn node(tag: &str) -> Node {
    let dir = scratch(tag);
    let facade = Arc::new(RaftFacade::open(&build_ctx(&dir)).expect("open facade"));
    std::env::set_var("QUEEN_RAFT_DIR", dir.join("cfg").display().to_string());
    let cfg = crate::config::load();
    let rsm: Arc<dyn Rsm> = facade.clone();
    let state = crate::handlers::raft::build_raft_state_with(&cfg, Some(rsm)).expect("state");
    let router = crate::handlers::raft::build_raft_router(state, auth_off(), true);
    Node {
        router,
        facade,
        dir,
    }
}

/// The scope of a token for `sub` minted from a key with `grants`.
fn scope(grants: Value, sub: &str) -> Scope {
    let grants: Vec<Grant> = serde_json::from_value(grants).expect("grants");
    queen_protocol::scope::check_grants(&grants).expect("well-formed grants");
    Scope::new(vec![layer(&grants, Some(sub))])
}

/// The device role every test starts from: its own telemetry partition to
/// push to, its own command partition to consume, its own KV keys and timers.
fn device(sub: &str) -> Scope {
    scope(
        json!([
            {"queue": "telemetry", "partition": "{sub}", "ops": ["produce"]},
            {"queue": "commands", "partition": "{sub}", "ops": ["consume"]},
            {"kv": "state", "key": "{sub}/*", "ops": ["read", "write"]},
            {"kv": "config", "ops": ["read"]},
            {"timers": "reminders", "key": "{sub}:*", "partition": "{sub}", "ops": ["read", "write"]}
        ]),
        sub,
    )
}

enum Via<'a> {
    None,
    Header(&'a Scope),
    Raw(&'a str),
    Extension(&'a Scope),
}

async fn call(
    n: &Node,
    method: Method,
    uri: &str,
    via: Via<'_>,
    body: Option<Vec<u8>>,
) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri);
    match &via {
        Via::Header(s) => req = req.header(SCOPE_HEADER, s.encode()),
        Via::Raw(v) => req = req.header(SCOPE_HEADER, *v),
        Via::None | Via::Extension(_) => {}
    }
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b)
        }
        None => Body::empty(),
    };
    let mut req = req.body(body).expect("request");
    if let Via::Extension(s) = via {
        req.extensions_mut()
            .insert::<crate::scope::ClientScope>(Arc::new(s.clone()));
    }
    let resp = n.router.clone().oneshot(req).await.expect("answer");
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("body");
    let v = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    (status, v)
}

fn js(v: Value) -> Option<Vec<u8>> {
    Some(v.to_string().into_bytes())
}

fn assert_denied((s, v): &(StatusCode, Value), what: &str) {
    assert_eq!(*s, StatusCode::FORBIDDEN, "{what}: {v}");
    assert_eq!(v["code"], "scope_denied", "{what}: {v}");
}

/// Push one message to `queue`/`partition`, unscoped.
async fn seed(n: &Node, queue: &str, partition: &str, txn: &str) {
    let (s, v) = call(
        n,
        Method::POST,
        "/api/v1/push",
        Via::None,
        js(json!({"items": [{"queue": queue, "partition": partition, "transactionId": txn, "payload": {"txn": txn}}]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "seed {queue}/{partition}: {v}");
    assert_eq!(v[0]["status"], "queued", "seed: {v}");
}

/// What an unscoped queue-mode pop of `queue`/`partition` holds: the
/// transaction ids, oldest first, and the answer itself.
async fn peek(n: &Node, queue: &str, partition: &str) -> (Vec<String>, Value) {
    let (s, v) = call(
        n,
        Method::GET,
        &format!("/api/v1/pop/queue/{queue}/partition/{partition}?batch=100&wait=false"),
        Via::None,
        None,
    )
    .await;
    if s == StatusCode::NO_CONTENT {
        return (Vec::new(), Value::Null);
    }
    assert_eq!(s, StatusCode::OK, "pop {queue}/{partition}: {v}");
    let txns = v["messages"]
        .as_array()
        .expect("messages")
        .iter()
        .map(|m| m["transactionId"].as_str().unwrap_or_default().to_string())
        .collect();
    (txns, v)
}

/// The binary push body (`RaftFacade::push_binary`): one group.
fn binary_push(queue: &str, partition: &str, payloads: &[&str]) -> Vec<u8> {
    let mut b = b"QPB1".to_vec();
    b.push(0);
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&(queue.len() as u16).to_le_bytes());
    b.extend_from_slice(queue.as_bytes());
    b.extend_from_slice(&(partition.len() as u16).to_le_bytes());
    b.extend_from_slice(partition.as_bytes());
    b.extend_from_slice(&(payloads.len() as u32).to_le_bytes());
    for p in payloads {
        b.extend_from_slice(&(p.len() as u32).to_le_bytes());
        b.extend_from_slice(p.as_bytes());
    }
    b
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_scoped_push_reaches_only_its_partitions() {
    let n = node("push");
    let dev1 = device("dev1");

    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        js(json!({"items": [{"queue": "telemetry", "partition": "dev1", "transactionId": "a1", "payload": {"t": 1}}]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "its own partition: {v}");
    assert_eq!(v[0]["status"], "queued", "{v}");

    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        js(json!({"items": [{"queue": "telemetry", "partition": "dev2", "payload": {"t": 2}}]})),
    )
    .await;
    assert_denied(&r, "another device's partition");
    assert!(
        r.1["error"].as_str().unwrap_or_default().contains("dev2"),
        "the refusal names what was asked: {}",
        r.1
    );

    // One name outside the grants refuses the whole push: its own item too.
    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        js(json!({"items": [
            {"queue": "telemetry", "partition": "dev1", "transactionId": "a2", "payload": {}},
            {"queue": "telemetry", "partition": "dev2", "transactionId": "b1", "payload": {}}
        ]})),
    )
    .await;
    assert_denied(&r, "a mixed push");
    // No partition is the `Default` partition, which no grant names.
    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        js(json!({"items": [{"queue": "telemetry", "payload": {}}]})),
    )
    .await;
    assert_denied(&r, "the default partition");
    // Produce only: the command queue is consume.
    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        js(json!({"items": [{"queue": "commands", "partition": "dev1", "payload": {}}]})),
    )
    .await;
    assert_denied(&r, "a queue it may only consume");
    // The binary body names its partitions inside its groups: checked there.
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        Some(binary_push("telemetry", "dev1", &["{\"b\":1}"])),
    )
    .await;
    assert_eq!(
        s,
        StatusCode::CREATED,
        "a binary push to its own partition: {v}"
    );
    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&dev1),
        Some(binary_push("telemetry", "dev2", &["{\"b\":2}"])),
    )
    .await;
    assert_denied(&r, "a binary push to another partition");

    let (dev1_txns, _) = peek(&n, "telemetry", "dev1").await;
    assert_eq!(
        dev1_txns.len(),
        2,
        "a1 and the binary push, not a2: {dev1_txns:?}"
    );
    assert_eq!(dev1_txns[0], "a1");
    let (dev2_txns, _) = peek(&n, "telemetry", "dev2").await;
    assert!(dev2_txns.is_empty(), "nothing reached dev2: {dev2_txns:?}");
    let (default_txns, _) = peek(&n, "telemetry", "Default").await;
    assert!(default_txns.is_empty(), "{default_txns:?}");
    drop(n.facade.clone());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_scoped_pop_names_a_granted_partition_and_group() {
    let n = node("pop");
    seed(&n, "commands", "dev1", "c1").await;
    seed(&n, "commands", "dev2", "c2").await;
    let dev1 = device("dev1");

    let (s, v) = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/commands/partition/dev1?wait=false",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "its own partition: {v}");
    assert_eq!(v["messages"][0]["transactionId"], "c1", "{v}");

    let r = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/commands/partition/dev2?wait=false",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_denied(&r, "another device's partition");
    // A pop that lets the broker pick could pick dev2.
    let r = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/commands?wait=false",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_denied(&r, "a pop of any partition");
    let r = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/telemetry/partition/dev1?wait=false",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_denied(&r, "a queue it may only push to");

    // A worker role: every partition of `jobs`, as group `workers` only.
    seed(&n, "jobs", "p1", "j1").await;
    let worker = scope(
        json!([{"queue": "jobs", "group": "workers", "ops": ["consume"]}]),
        "w1",
    );
    let (s, v) = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/jobs?consumerGroup=workers&subscriptionMode=all&wait=false",
        Via::Header(&worker),
        None,
    )
    .await;
    assert_eq!(
        s,
        StatusCode::OK,
        "any partition of its queue, as its group: {v}"
    );
    assert_eq!(v["messages"][0]["transactionId"], "j1", "{v}");
    let r = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/jobs?consumerGroup=auditors&wait=false",
        Via::Header(&worker),
        None,
    )
    .await;
    assert_denied(&r, "another group");
    let r = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/jobs?wait=false",
        Via::Header(&worker),
        None,
    )
    .await;
    assert_denied(&r, "queue mode is a group of its own");
    // The second device's message is still there for it.
    let (c2, _) = peek(&n, "commands", "dev2").await;
    assert_eq!(c2, vec!["c2".to_string()]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_scoped_ack_cannot_move_another_partitions_cursor() {
    let n = node("ack");
    seed(&n, "commands", "dev1", "c1").await;
    seed(&n, "commands", "dev2", "c2").await;
    let dev1 = device("dev1");

    // dev2's message, leased by an unscoped consumer: its number and lease.
    let (_, popped) = peek(&n, "commands", "dev2").await;
    let pid2 = popped["partitionId"]
        .as_str()
        .expect("partitionId")
        .to_string();
    let lease2 = popped["leaseId"].as_str().expect("leaseId").to_string();

    for (what, body) in [
        (
            "with the lease",
            json!({"transactionId": "c2", "partitionId": pid2, "leaseId": lease2, "status": "completed"}),
        ),
        (
            "without a lease, which would move the cursor",
            json!({"transactionId": "c2", "partitionId": pid2, "status": "completed"}),
        ),
        (
            "as a dead letter",
            json!({"transactionId": "c2", "partitionId": pid2, "status": "dlq"}),
        ),
    ] {
        let r = call(
            &n,
            Method::POST,
            "/api/v1/ack",
            Via::Header(&dev1),
            js(body),
        )
        .await;
        assert_denied(&r, what);
    }
    let r = call(
        &n,
        Method::POST,
        "/api/v1/ack/batch",
        Via::Header(&dev1),
        js(json!({"acknowledgments": [
            {"transactionId": "c2", "partitionId": pid2, "status": "completed"}
        ]})),
    )
    .await;
    assert_denied(&r, "in a batch");

    // The unscoped consumer still holds dev2's message and acks it.
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/ack",
        Via::None,
        js(json!({"transactionId": "c2", "partitionId": pid2, "leaseId": lease2, "status": "completed"})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v[0]["success"], true, "the scoped acks moved nothing: {v}");

    // dev1 acks its own message.
    let (s, own) = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/commands/partition/dev1?wait=false",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{own}");
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/ack",
        Via::Header(&dev1),
        js(json!({
            "transactionId": "c1",
            "partitionId": own["partitionId"],
            "leaseId": own["leaseId"],
            "status": "completed"
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v[0]["success"], true, "its own ack: {v}");
    // Lease renewal takes the lease id the pop returned: the id is the proof.
    let (s, v) = call(
        &n,
        Method::POST,
        &format!(
            "/api/v1/lease/{}/extend",
            own["leaseId"].as_str().unwrap_or_default()
        ),
        Via::Header(&dev1),
        js(json!({"seconds": 30})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_scoped_transaction_is_refused_whole() {
    let n = node("txn");
    seed(&n, "commands", "dev1", "c1").await;
    seed(&n, "commands", "dev2", "c2").await;
    let dev1 = device("dev1");
    let (_, own) = call(
        &n,
        Method::GET,
        "/api/v1/pop/queue/commands/partition/dev1?wait=false",
        Via::Header(&dev1),
        None,
    )
    .await;
    let ack_own = json!({
        "type": "ack", "transactionId": "c1", "partitionId": own["partitionId"],
        "leaseId": own["leaseId"], "status": "completed"
    });

    // The worker step the Queen way: ack the input, push the next event,
    // write KV state, schedule a timer. All of it inside the grants.
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/transaction",
        Via::Header(&dev1),
        js(json!({
            "operations": [
                ack_own,
                {"type": "push", "items": [{"queue": "telemetry", "partition": "dev1", "transactionId": "t1", "payload": {"done": "c1"}}]}
            ],
            "kv": [{"op": "put", "ns": "state", "key": "dev1/last", "value": "c1", "forever": true}],
            "timers": [{"op": "schedule", "queue": "reminders", "timerKey": "dev1:wake", "partition": "dev1", "delayMs": 60000, "txn": "w1", "payload": "e30="}]
        })),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "{v}");
    assert_eq!(v["success"], true, "a step inside its grants: {v}");

    let refused = [
        (
            "a push to another partition",
            json!({"operations": [
                {"type": "push", "items": [{"queue": "telemetry", "partition": "dev1", "transactionId": "t2", "payload": {}}]},
                {"type": "push", "items": [{"queue": "telemetry", "partition": "dev2", "transactionId": "t3", "payload": {}}]}
            ]}),
        ),
        (
            "an ack of another partition",
            json!({"operations": [
                {"type": "ack", "transactionId": "c2", "partitionId": peek_pid(&n, "dev2").await, "status": "completed"}
            ]}),
        ),
        (
            "a KV write outside its keys",
            json!({"operations": [{"type": "push", "items": [{"queue": "telemetry", "partition": "dev1", "transactionId": "t4", "payload": {}}]}],
                   "kv": [{"op": "put", "ns": "state", "key": "dev2/last", "value": 1, "forever": true}]}),
        ),
        (
            "a KV write to a namespace it may only read",
            json!({"kv": [{"op": "put", "ns": "config", "key": "x", "value": 1, "forever": true}]}),
        ),
        (
            "a timer that fires into another partition",
            json!({"timers": [{"op": "schedule", "queue": "reminders", "timerKey": "dev1:x", "partition": "dev2", "delayMs": 1000, "txn": "x1", "payload": "e30="}]}),
        ),
        (
            "a timer under another device's key",
            json!({"timers": [{"op": "cancel", "queue": "reminders", "timerKey": "dev2:wake"}]}),
        ),
        (
            "a consumer-group position",
            json!({"positions": [{"queue": "commands", "partition": "dev1", "consumerGroup": "g", "offset": 0}]}),
        ),
    ];
    for (what, body) in refused {
        let r = call(
            &n,
            Method::POST,
            "/api/v1/transaction",
            Via::Header(&dev1),
            js(body),
        )
        .await;
        assert_denied(&r, what);
    }
    // Nothing of the refused bundles reached the log.
    let (t, _) = peek(&n, "telemetry", "dev1").await;
    assert_eq!(t, vec!["t1".to_string()], "only the committed step's push");
    let (c2, _) = peek(&n, "commands", "dev2").await;
    assert_eq!(
        c2,
        vec!["c2".to_string()],
        "dev2's command is still unacked"
    );
}

/// dev2's partition number, from an unscoped look at its queue.
async fn peek_pid(n: &Node, partition: &str) -> String {
    let (_, v) = call(
        n,
        Method::GET,
        &format!("/api/v1/pop/queue/commands/partition/{partition}?wait=false&consumerGroup=probe&subscriptionMode=all"),
        Via::None,
        None,
    )
    .await;
    v["partitionId"].as_str().expect("partitionId").to_string()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn scoped_kv_calls_reach_only_their_keys() {
    let n = node("kv");
    let dev1 = device("dev1");

    let (s, v) = call(
        &n,
        Method::PUT,
        "/api/v1/kv/state/dev1/battery",
        Via::Header(&dev1),
        js(json!({"value": 87, "forever": true})),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "its own key: {v}");
    let (s, v) = call(
        &n,
        Method::GET,
        "/api/v1/kv/state/dev1/battery",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!((s, &v["value"]), (StatusCode::OK, &json!(87)), "{v}");
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/kv",
        Via::Header(&dev1),
        js(json!([{"op": "getPrefix", "ns": "state", "prefix": "dev1/"}])),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a prefix inside its keys: {v}");

    for (what, method, uri, body) in [
        (
            "another device's key",
            Method::GET,
            "/api/v1/kv/state/dev2/battery",
            None,
        ),
        (
            "a write to another device's key",
            Method::PUT,
            "/api/v1/kv/state/dev2/battery",
            js(json!({"value": 1, "forever": true})),
        ),
        (
            "a delete of another device's key",
            Method::DELETE,
            "/api/v1/kv/state/dev2/battery",
            None,
        ),
        (
            "a write to a namespace it may only read",
            Method::PUT,
            "/api/v1/kv/config/x",
            js(json!({"value": 1, "forever": true})),
        ),
        (
            "a prefix that reaches every device",
            Method::POST,
            "/api/v1/kv",
            js(json!([{"op": "getPrefix", "ns": "state", "prefix": "dev"}])),
        ),
        (
            "one op outside its keys in a batch",
            Method::POST,
            "/api/v1/kv",
            js(json!([
                {"op": "get", "ns": "state", "key": "dev1/battery"},
                {"op": "getMany", "ns": "state", "keys": ["dev1/battery", "dev2/battery"]}
            ])),
        ),
        (
            "an increment of another device's counter",
            Method::POST,
            "/api/v1/kv",
            js(
                json!([{"op": "incr", "ns": "state", "key": "dev2/n", "delta": 1, "forever": true}]),
            ),
        ),
    ] {
        let r = call(&n, method, uri, Via::Header(&dev1), body).await;
        assert_denied(&r, what);
    }
    let (s, v) = call(
        &n,
        Method::GET,
        "/api/v1/kv/config/anything",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a namespace it may read: {v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn scoped_timer_calls_reach_only_their_keys_and_partitions() {
    let n = node("timers");
    let dev1 = device("dev1");

    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/timers",
        Via::Header(&dev1),
        js(json!([{"op": "schedule", "queue": "reminders", "timerKey": "dev1:wake", "partition": "dev1", "delayMs": 60000, "txn": "w1", "payload": "e30="}])),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "its own timer: {v}");
    let (s, v) = call(
        &n,
        Method::GET,
        "/api/v1/timers/reminders/dev1:wake",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a peek of its own timer: {v}");
    let (s, v) = call(
        &n,
        Method::GET,
        "/api/v1/timers/reminders?mode=count&prefix=dev1:",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a count of its own timers: {v}");

    for (what, method, uri, body) in [
        (
            "a timer that fires into another partition",
            Method::POST,
            "/api/v1/timers",
            js(
                json!([{"op": "schedule", "queue": "reminders", "timerKey": "dev1:x", "partition": "dev2", "delayMs": 1000, "txn": "x1", "payload": "e30="}]),
            ),
        ),
        (
            "a timer under another device's key",
            Method::POST,
            "/api/v1/timers",
            js(
                json!([{"op": "schedule", "queue": "reminders", "timerKey": "dev2:x", "partition": "dev1", "delayMs": 1000, "txn": "x2", "payload": "e30="}]),
            ),
        ),
        (
            "a cancel of another device's timer",
            Method::DELETE,
            "/api/v1/timers/reminders/dev2:wake",
            None,
        ),
        (
            "a peek of another device's timer",
            Method::GET,
            "/api/v1/timers/reminders/dev2:wake",
            None,
        ),
        (
            "a list of every key",
            Method::GET,
            "/api/v1/timers/reminders",
            None,
        ),
        (
            "a count of every device's timers",
            Method::GET,
            "/api/v1/timers/reminders?mode=count&prefix=dev",
            None,
        ),
    ] {
        let r = call(&n, method, uri, Via::Header(&dev1), body).await;
        assert_denied(&r, what);
    }
    let (s, v) = call(
        &n,
        Method::DELETE,
        "/api/v1/timers/reminders/dev1:wake",
        Via::Header(&dev1),
        None,
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a cancel of its own timer: {v}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_scoped_request_off_the_allowed_routes_is_refused() {
    let n = node("routes");
    seed(&n, "commands", "dev1", "c1").await;
    let dev1 = device("dev1");
    for (method, uri, body) in [
        (Method::GET, "/api/v1/resources/queues", None),
        (Method::GET, "/api/v1/resources/queues/commands", None),
        (
            Method::POST,
            "/api/v1/configure",
            js(json!({"queue": "commands"})),
        ),
        (Method::DELETE, "/api/v1/resources/queues/commands", None),
        (
            Method::GET,
            "/api/v1/pop?namespace=commands&wait=false",
            None,
        ),
        (Method::POST, "/api/v1/locks", js(json!([]))),
        (
            Method::POST,
            "/api/v1/resources/kv/list",
            js(json!({"namespace": "state"})),
        ),
        (Method::POST, "/api/v1/ephemeral/push", js(json!({}))),
        (Method::GET, "/api/v1/messages", None),
        // A near-miss spelling of an allowed route reaches the fallback.
        (
            Method::POST,
            "/api/v1/push/",
            js(json!({"items": [{"queue": "telemetry", "partition": "dev1", "payload": {}}]})),
        ),
        (Method::GET, "/", None),
    ] {
        let r = call(&n, method.clone(), uri, Via::Header(&dev1), body).await;
        assert_denied(&r, &format!("{method} {uri}"));
    }
    // A scope that does not decode refuses the request: never ignored.
    for raw in [
        "not json",
        r#"{"v":2,"layers":[]}"#,
        r#"{"v":1,"layers":[[{"kind":"queue","name":"=telemetry","partition":"*","group":"*","key":"*","ops":["produce"],"lane":"x"}]]}"#,
    ] {
        let (s, v) = call(
            &n,
            Method::POST,
            "/api/v1/push",
            Via::Raw(raw),
            js(json!({"items": [{"queue": "telemetry", "partition": "dev1", "payload": {}}]})),
        )
        .await;
        assert_eq!(s, StatusCode::FORBIDDEN, "{raw}: {v}");
        assert_eq!(v["code"], "scope_invalid", "{raw}: {v}");
    }
    // An empty scope allows nothing.
    let empty = Scope::new(vec![Vec::new()]);
    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Header(&empty),
        js(json!({"items": [{"queue": "telemetry", "partition": "dev1", "payload": {}}]})),
    )
    .await;
    assert_denied(&r, "a scope with no grant");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn the_in_process_scope_is_enforced_like_the_header() {
    let n = node("ext");
    let dev1 = device("dev1");
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Extension(&dev1),
        js(json!({"items": [{"queue": "telemetry", "partition": "dev1", "payload": {}}]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let r = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::Extension(&dev1),
        js(json!({"items": [{"queue": "telemetry", "partition": "dev2", "payload": {}}]})),
    )
    .await;
    assert_denied(&r, "the extension, another partition");
    let r = call(
        &n,
        Method::GET,
        "/api/v1/resources/queues",
        Via::Extension(&dev1),
        None,
    )
    .await;
    assert_denied(&r, "the extension, a route off the list");
    // With no scope, the same calls are the tenant's, as before.
    let (s, v) = call(
        &n,
        Method::POST,
        "/api/v1/push",
        Via::None,
        js(json!({"items": [{"queue": "telemetry", "partition": "dev2", "payload": {}}]})),
    )
    .await;
    assert_eq!(s, StatusCode::CREATED, "{v}");
    let (s, v) = call(&n, Method::GET, "/api/v1/resources/queues", Via::None, None).await;
    assert_eq!(s, StatusCode::OK, "{v}");
}

