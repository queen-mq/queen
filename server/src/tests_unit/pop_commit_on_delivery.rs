//! The pop's commit-at-delivery switch on every pop route: `commitOnDelivery`,
//! the documented name, and `autoAck`, the deprecated alias the released Go and
//! Rust SDKs and the CLI still send.
//!
//! What it pins:
//!
//! - both names parse on the queue, partition and discovery routes, either one
//!   set to true commits, and a request that sends both is not refused;
//! - a pop under either name commits the group's cursor at delivery: an empty
//!   `leaseId`, and nothing comes back once a lease would have run out, while a
//!   pop with neither name leases the batch and gets it again after the lease;
//! - conflation refuses either name with a 400 that names both;
//! - the ephemeral pop reads both names too.
//!
//! The module is a child of `handlers/data.rs` (`#[path]`, resolved from
//! `src/handlers/`), so `use super::*` reaches its private items.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::http::{HeaderMap, Uri};

use super::*;
use crate::auth::AuthedSub;
use crate::config::DEFAULT_TENANT;
use crate::rsm::facade::real::RaftFacade;
use crate::rsm::facade::{ApiReq, Deadline, ReqCtx, Rsm, RsmBuildCtx};
use crate::tenant::Tenant;

/// The handler's own extractor over a query string, as a request reaches it.
fn query<T: serde::de::DeserializeOwned>(qs: &str) -> Query<T> {
    let uri: Uri = format!("/api/v1/pop?{qs}").parse().expect("uri");
    Query::try_from_uri(&uri).expect("the query parses")
}

async fn body_of(resp: Response) -> (StatusCode, serde_json::Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .expect("read body");
    let v = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
    (status, v)
}

/// A broker state over the not-ready stub: enough for everything a handler
/// decides before it reaches the state machine.
fn stub_state() -> Arc<AppState> {
    let mut cfg = crate::config::load();
    cfg.ephemeral_require_grant = false;
    cfg.tenancy_header = false;
    let stub: Arc<dyn Rsm> = Arc::new(crate::rsm::facade::NotReady::new());
    crate::handlers::raft::build_raft_state_with(&cfg, Some(stub)).expect("state")
}

const CASES: [(&str, bool); 8] = [
    ("", false),
    ("commitOnDelivery=true", true),
    ("autoAck=true", true),
    ("commitOnDelivery=true&autoAck=true", true),
    ("commitOnDelivery=false&autoAck=true", true),
    ("commitOnDelivery=true&autoAck=false", true),
    ("commitOnDelivery=false", false),
    ("autoAck=false", false),
];

#[test]
fn either_name_commits_on_every_pop_route() {
    for (qs, want) in CASES {
        let p: Query<PopParams> = query(qs);
        assert_eq!(p.commits_on_delivery(), want, "queue route, `{qs}`");
        let d: Query<PopDiscoverParams> = query(&format!("namespace=ns&{qs}"));
        assert_eq!(d.commits_on_delivery(), want, "discovery route, `{qs}`");
    }
}

#[tokio::test]
async fn conflation_refuses_commit_on_delivery_under_either_name() {
    let st = stub_state();
    let tenant = || Extension(Tenant::default_tenant());
    for name in ["commitOnDelivery", "autoAck"] {
        let qs = format!("consumerGroup=g&conflation=true&{name}=true");
        let answers = [
            handle_pop(State(st.clone()), tenant(), Path("q".into()), query(&qs)).await,
            handle_pop_partition(
                State(st.clone()),
                tenant(),
                Path(("q".into(), "p".into())),
                query(&qs),
            )
            .await,
            handle_pop_discover(
                State(st.clone()),
                tenant(),
                query(&format!("namespace=ns&{qs}")),
            )
            .await,
        ];
        for (route, resp) in ["queue", "partition", "discovery"].iter().zip(answers) {
            let (status, v) = body_of(resp).await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "{route} route, {name}: {v}"
            );
            let error = v["error"].as_str().unwrap_or_default();
            assert!(
                error.contains("commitOnDelivery") && error.contains("autoAck"),
                "{route} route, {name}: the refusal names both: {error}"
            );
        }
    }
    // Control: conflation alone passes the guard and reaches the stub.
    let resp = handle_pop(
        State(st.clone()),
        tenant(),
        Path("q".into()),
        query("consumerGroup=g&conflation=true"),
    )
    .await;
    assert_ne!(resp.status(), StatusCode::BAD_REQUEST);
}

/// Through the handlers over a real state machine. Each queue gets two
/// messages and one group pop with a one-second lease; the pops that commit
/// at delivery answer an empty `leaseId`, and once the leased batch has come
/// back (so the lease has run out) they have nothing left to deliver.
#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn a_commit_on_delivery_pop_takes_no_lease_and_is_not_redelivered() {
    let dir = std::env::temp_dir().join(format!(
        "queen-commit-on-delivery-{}-{}",
        std::process::id(),
        crate::util::uuidv7_bytes()[15]
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::env::set_var("QUEEN_RAFT_MAP_BYTES", (256usize << 20).to_string());
    let facade = Arc::new(
        RaftFacade::open(&RsmBuildCtx {
            data_dir: dir.display().to_string(),
            notifier: crate::notify::Notifier::new(false),
            // The host's disk usage is not under test.
            disk_high_pct: 100.0,
            disk_low_pct: 100.0,
        })
        .expect("open the facade"),
    );
    let rsm: Arc<dyn Rsm> = facade.clone();
    let st = crate::handlers::raft::build_raft_state_with(&crate::config::load(), Some(rsm))
        .expect("raft state");
    let tenant = || Extension(Tenant::default_tenant());

    // The discovery pop finds its queue by namespace.
    let configured = st
        .rsm
        .api(
            ReqCtx::new(DEFAULT_TENANT, Deadline::after(Duration::from_secs(5))),
            ApiReq {
                method: "POST".into(),
                path: "/api/v1/configure".into(),
                query: None,
                body: br#"{"queue":"cod-discovery","namespace":"cod-ns"}"#.to_vec(),
            },
        )
        .await
        .expect("configure");
    assert_eq!(configured.status, 200, "{}", configured.body);

    const QUEUES: [&str; 5] = [
        "cod-named",
        "cod-alias",
        "cod-partition",
        "cod-discovery",
        "cod-leased",
    ];
    let items: Vec<serde_json::Value> = QUEUES
        .iter()
        .flat_map(|q| (0..2).map(move |n| serde_json::json!({"queue": q, "payload": {"n": n}})))
        .collect();
    let resp = handle_push(
        State(st.clone()),
        Extension(AuthedSub(None)),
        tenant(),
        Bytes::from(serde_json::json!({ "items": items }).to_string()),
    )
    .await;
    assert_eq!(resp.status(), StatusCode::CREATED);

    let group = "consumerGroup=g&subscriptionMode=all&leaseSeconds=1&batch=10";
    let pop = |route: &'static str, queue: &'static str, extra: &'static str| {
        let st = st.clone();
        async move {
            let qs = format!("{group}&{extra}");
            let resp = match route {
                "queue" => handle_pop(State(st), tenant(), Path(queue.into()), query(&qs)).await,
                "partition" => {
                    handle_pop_partition(
                        State(st),
                        tenant(),
                        Path((queue.into(), "Default".into())),
                        query(&qs),
                    )
                    .await
                }
                _ => {
                    handle_pop_discover(
                        State(st),
                        tenant(),
                        query(&format!("namespace=cod-ns&{qs}")),
                    )
                    .await
                }
            };
            body_of(resp).await
        }
    };
    let lease_ids = |v: &serde_json::Value| -> Vec<String> {
        v["messages"]
            .as_array()
            .expect("messages")
            .iter()
            .map(|m| m["leaseId"].as_str().expect("leaseId").to_string())
            .collect()
    };

    let committing = [
        ("queue", "cod-named", "commitOnDelivery=true"),
        ("queue", "cod-alias", "autoAck=true"),
        ("partition", "cod-partition", "commitOnDelivery=true"),
        ("discovery", "cod-discovery", "commitOnDelivery=true"),
    ];
    for (route, queue, extra) in committing {
        let (status, v) = pop(route, queue, extra).await;
        assert_eq!(status, StatusCode::OK, "{queue}: {v}");
        assert_eq!(v["leaseId"], "", "{queue}: no lease: {v}");
        assert_eq!(lease_ids(&v), ["", ""], "{queue}: {v}");
    }
    let (status, first) = pop("queue", "cod-leased", "").await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let leased = lease_ids(&first);
    assert_eq!(leased.len(), 2, "{first}");
    assert!(leased.iter().all(|l| !l.is_empty()), "leased: {first}");

    // The leased batch comes back once its lease has run out.
    let mut again = serde_json::Value::Null;
    for _ in 0..100 {
        let (status, v) = pop("queue", "cod-leased", "").await;
        if status == StatusCode::OK {
            again = v;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        again["messages"].as_array().map(Vec::len),
        Some(2),
        "the leased batch is redelivered after its lease: {again}"
    );
    assert_eq!(again["messages"][0]["deliveryAttempt"], 2, "{again}");

    // By now any lease would have run out: the committed batches stay gone.
    for (route, queue, extra) in committing {
        let (status, v) = pop(route, queue, extra).await;
        assert_eq!(
            status,
            StatusCode::NO_CONTENT,
            "{queue}: not redelivered: {v}"
        );
    }

    drop(st);
    if let Ok(f) = Arc::try_unwrap(facade) {
        f.shutdown().await;
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The ephemeral pop reads both names: a commit at delivery frees the message
/// at once, where a leased pop keeps it until the ack or the lease's end.
#[tokio::test]
async fn the_ephemeral_pop_commits_on_either_name() {
    let st = stub_state();
    for (queue, extra, held) in [
        ("eph-named", "&commitOnDelivery=true", 0),
        ("eph-alias", "&autoAck=true", 0),
        ("eph-leased", "", 1),
    ] {
        st.ephemeral
            .push(
                DEFAULT_TENANT,
                queue,
                "Default",
                vec![b"{\"n\":1}".to_vec().into_boxed_slice()],
                crate::util::now_epoch_ms(),
            )
            .expect("push");
        let qs = format!("queue={queue}&group=g{extra}");
        let uri: Uri = format!("/api/v1/ephemeral/pop?{qs}").parse().expect("uri");
        let map: Query<HashMap<String, String>> = Query::try_from_uri(&uri).expect("query");
        let resp = crate::handlers::handle_ephemeral_pop(
            State(st.clone()),
            Extension(AuthedSub(None)),
            Extension(Tenant::default_tenant()),
            HeaderMap::new(),
            uri,
            map,
        )
        .await;
        let (status, v) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "{queue}: {v}");
        assert_eq!(
            v["messages"].as_array().map(Vec::len),
            Some(1),
            "{queue}: {v}"
        );
        let depth = st.ephemeral.depth(DEFAULT_TENANT, queue).expect("depth");
        assert_eq!(depth.0, held, "{queue}: messages still held after the pop");
    }
}
