//! The Queen wire, over a socket.
//!
//! Every route [`queen_s3::queen::HttpQueen`] speaks, against a local axum
//! server on an ephemeral port that ASSERTS THE REQUEST — path, bearer, body —
//! and answers a canned response. The unit tests in `src/queen.rs` pin the
//! codecs; these pin what actually goes on the wire, which is the half a codec
//! test cannot see: the method, the header, the URL and the timeout override.
//!
//! The canned answers are the broker's own shapes, copied from
//! server/src/handlers/fetch.rs, 033_log_partitions_changed.sql and
//! server/src/handlers/kv.rs — including the two that are not what they look
//! like: a 429 carrying `Retry-After`, and a lost KV precondition arriving as
//! HTTP 200.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use axum::body::Bytes;
use axum::extract::{OriginalUri, State};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Router;

use queen_s3::queen::{HttpQueen, KvOp, QueenApi};
use queen_s3::types::{ChangedRequestEntry, FetchRequestEntry, Micros, SinkError};

// ---------------------------------------------------------------------------
// A recording mock
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct Recorded {
    method: String,
    path: String,
    query: String,
    authorization: Option<String>,
    content_type: Option<String>,
    host: Option<String>,
    body: String,
}

impl Recorded {
    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or(serde_json::Value::Null)
    }
}

#[derive(Debug, Clone)]
struct Canned {
    status: u16,
    headers: Vec<(String, String)>,
    body: String,
}

impl Canned {
    fn ok(body: &str) -> Canned {
        Canned {
            status: 200,
            headers: Vec::new(),
            body: body.to_string(),
        }
    }
}

#[derive(Default)]
struct MockState {
    seen: Mutex<Vec<Recorded>>,
    answers: Mutex<VecDeque<Canned>>,
}

struct Mock {
    base: String,
    state: Arc<MockState>,
}

impl Mock {
    async fn start(answers: Vec<Canned>) -> Mock {
        let state = Arc::new(MockState {
            seen: Mutex::new(Vec::new()),
            answers: Mutex::new(answers.into_iter().collect()),
        });
        let app = Router::new().fallback(handle).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Mock {
            base: format!("http://{addr}"),
            state,
        }
    }

    fn seen(&self) -> Vec<Recorded> {
        self.state.seen.lock().unwrap().clone()
    }

    fn only(&self) -> Recorded {
        let seen = self.seen();
        assert_eq!(seen.len(), 1, "expected exactly one request, saw {seen:#?}");
        seen.into_iter().next().unwrap()
    }

    fn queen(&self, token: Option<&str>) -> HttpQueen {
        HttpQueen::new(&self.base, token.map(|t| t.to_string())).unwrap()
    }
}

async fn handle(
    State(state): State<Arc<MockState>>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(|v| v.to_string())
    };
    state.seen.lock().unwrap().push(Recorded {
        method: method.to_string(),
        path: uri.path().to_string(),
        query: uri.query().unwrap_or("").to_string(),
        authorization: header("authorization"),
        content_type: header("content-type"),
        host: header("host"),
        body: String::from_utf8_lossy(&body).into_owned(),
    });
    let canned = state.answers.lock().unwrap().pop_front();
    match canned {
        Some(c) => {
            let mut resp = (
                StatusCode::from_u16(c.status).unwrap(),
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                c.body,
            )
                .into_response();
            for (name, value) in c.headers {
                resp.headers_mut().insert(
                    axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                    value.parse().unwrap(),
                );
            }
            resp
        }
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "the mock ran out of answers",
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// fetch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_puts_the_entries_the_bearer_and_the_poll_on_the_wire() {
    let mock = Mock::start(vec![Canned::ok(
        r#"{"entries":[{"queue":"orders","partition":"cust-0420","records":[
            {"offset":1400,"transactionId":"ord-9f21","payload":{"type":"paid","amount":1290},
             "ts":"2026-09-04T10:03:41.918204Z"}],
            "highWatermark":1401,"logStartOffset":1400}]}"#,
    )])
    .await;

    let entries = vec![FetchRequestEntry {
        queue: "orders".into(),
        partition: "cust-0420".into(),
        offset: 1400,
        max_bytes: Some(1_048_576),
    }];
    let out = mock
        .queen(Some("sk_test"))
        .fetch(entries, 500, 1)
        .await
        .unwrap();

    let req = mock.only();
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/api/v1/fetch");
    assert_eq!(req.authorization.as_deref(), Some("Bearer sk_test"));
    assert_eq!(req.content_type.as_deref(), Some("application/json"));
    let body = req.json();
    assert_eq!(body["maxWaitMs"], 500);
    assert_eq!(body["minBytes"], 1);
    assert_eq!(body["entries"][0]["queue"], "orders");
    assert_eq!(body["entries"][0]["partition"], "cust-0420");
    assert_eq!(body["entries"][0]["offset"], 1400);
    assert_eq!(body["entries"][0]["maxBytes"], 1_048_576);

    assert_eq!(out.len(), 1);
    assert_eq!(out[0].records.len(), 1);
    assert_eq!(out[0].records[0].transaction_id, "ord-9f21");
    assert_eq!(
        out[0].records[0].payload.as_ref().unwrap().get(),
        "{\"type\":\"paid\",\"amount\":1290}"
    );
    assert_eq!(out[0].high_watermark, 1401);
}

#[tokio::test]
async fn a_fetch_with_no_entries_never_leaves_the_process() {
    let mock = Mock::start(Vec::new()).await;
    assert!(mock
        .queen(None)
        .fetch(Vec::new(), 0, 1)
        .await
        .unwrap()
        .is_empty());
    assert!(mock.seen().is_empty(), "an empty batch is not a round trip");
}

#[tokio::test]
async fn no_token_means_no_authorization_header() {
    let mock = Mock::start(vec![Canned::ok(r#"{"queues":[]}"#)]).await;
    mock.queen(None).list_queues().await.unwrap();
    assert_eq!(mock.only().authorization, None);
}

// ---------------------------------------------------------------------------
// partitions/changed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn discovery_speaks_the_contract_of_plan_5_1() {
    let mock = Mock::start(vec![Canned::ok(
        r#"{"safeTime":"2026-09-04T10:04:57.412331Z","safeTimeDegraded":false,
            "entries":[{"queue":"orders","partitions":[
                {"name":"cust-0420","lastOffset":1811,"logStart":1400,
                 "lastWriteAt":"2026-09-04T10:04:00.000000Z"}],
              "next":"t|1788516240000000|cust-0420"}]}"#,
    )])
    .await;

    let entries = vec![ChangedRequestEntry {
        queue: "orders".into(),
        since: Some(Micros::parse_iso("2026-09-04T10:00:00Z").unwrap()),
        after: None,
        limit: 1000,
    }];
    let out = mock
        .queen(Some("sk_test"))
        .partitions_changed(entries)
        .await
        .unwrap();

    let req = mock.only();
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/api/v1/partitions/changed");
    let body = req.json();
    assert_eq!(body["entries"][0]["queue"], "orders");
    assert_eq!(body["entries"][0]["since"], "2026-09-04T10:00:00.000000Z");
    assert_eq!(body["entries"][0]["after"], serde_json::Value::Null);
    assert_eq!(body["entries"][0]["limit"], 1000);

    assert_eq!(
        out.safe_time,
        Micros::parse_iso("2026-09-04T10:04:57.412331Z").unwrap()
    );
    assert!(!out.safe_time_degraded);
    let p = &out.entries[0].partitions[0];
    assert_eq!(&*p.name, "cust-0420");
    assert_eq!(p.last_offset, 1811);
    assert_eq!(p.log_start, 1400);
    assert_eq!(
        p.last_write_at,
        Some(Micros::parse_iso("2026-09-04T10:04:00Z").unwrap())
    );
    assert_eq!(
        out.entries[0].next.as_deref(),
        Some("t|1788516240000000|cust-0420")
    );
}

#[tokio::test]
async fn a_degraded_safe_time_is_carried_rather_than_hidden() {
    let mock = Mock::start(vec![Canned::ok(
        r#"{"safeTime":"2026-09-04T10:04:27.000000Z","safeTimeDegraded":true,"entries":[]}"#,
    )])
    .await;
    let out = mock
        .queen(None)
        .partitions_changed(Vec::new())
        .await
        .unwrap();
    assert!(
        out.safe_time_degraded,
        "the floor answer must reach the sink"
    );
}

// ---------------------------------------------------------------------------
// kv
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_commit_batch_goes_out_as_operations_with_its_fence_at_index_zero() {
    let mock = Mock::start(vec![Canned::ok(
        r#"{"results":[{"index":0,"op":"put","applied":true,"version":11},
                       {"index":1,"op":"put","applied":true,"version":12}]}"#,
    )])
    .await;

    let ops = vec![
        KvOp::fence(
            "s3:default:orders:lease",
            serde_json::json!({"instance": "a"}),
            7,
        ),
        KvOp::put(
            "s3:default:orders:committed",
            serde_json::json!({"k": 42, "tEnd": 1788516300000000i64}),
        ),
    ];
    let out = mock.queen(Some("sk_test")).kv(ops).await.unwrap();

    let req = mock.only();
    assert_eq!(req.method, "POST");
    assert_eq!(req.path, "/api/v1/kv");
    let body = req.json();
    let fence = &body["operations"][0];
    assert_eq!(fence["ns"], "queen-s3");
    assert_eq!(fence["op"], "put");
    assert_eq!(fence["key"], "s3:default:orders:lease");
    assert_eq!(fence["expect"], 7);
    assert_eq!(fence["required"], true);
    assert_eq!(fence["forever"], true);
    let commit = &body["operations"][1];
    assert_eq!(commit["forever"], true);
    assert!(
        commit.get("required").is_none(),
        "only the fence aborts the batch"
    );

    assert!(out[0].did_apply());
    assert_eq!(out[1].version, 12);
}

#[tokio::test]
async fn a_lost_precondition_arrives_as_a_two_hundred_and_becomes_an_error() {
    let mock = Mock::start(vec![Canned::ok(
        r#"{"ok":false,"reason":"kv_precondition","failedIndex":0,"kvReason":"version",
            "version":99,"value":{"instance":"the-other-one"}}"#,
    )])
    .await;

    let err = mock
        .queen(Some("sk_test"))
        .kv(vec![KvOp::fence(
            "s3:default:orders:lease",
            serde_json::json!({"instance": "me"}),
            7,
        )])
        .await
        .unwrap_err();

    match err {
        SinkError::Precondition {
            failed_index,
            reason,
            version,
            value,
        } => {
            assert_eq!(failed_index, 0);
            assert_eq!(reason, "version");
            assert_eq!(
                version, 99,
                "the WINNER's version, for a fence to re-expect"
            );
            assert_eq!(value["instance"], "the-other-one");
        }
        other => panic!("expected a precondition, got {other:?}"),
    }
    // And it must NOT be retried: this is another instance owning the queue.
    assert!(!SinkError::Precondition {
        failed_index: 0,
        reason: String::new(),
        version: 0,
        value: serde_json::Value::Null,
    }
    .is_retriable());
}

// ---------------------------------------------------------------------------
// The proxy's two answers
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_429_carries_its_retry_after_in_milliseconds() {
    let mock = Mock::start(vec![Canned {
        status: 429,
        headers: vec![("retry-after".to_string(), "7".to_string())],
        body: r#"{"error":"rate_limited"}"#.to_string(),
    }])
    .await;

    let err = mock
        .queen(Some("sk_test"))
        .fetch(
            vec![FetchRequestEntry {
                queue: "orders".into(),
                partition: "0".into(),
                offset: 0,
                max_bytes: None,
            }],
            0,
            1,
        )
        .await
        .unwrap_err();

    match err {
        SinkError::Status {
            code,
            retry_after_ms,
            ref body,
        } => {
            assert_eq!(code, 429);
            assert_eq!(retry_after_ms, Some(7_000));
            assert!(body.contains("rate_limited"));
        }
        other => panic!("expected a status, got {other:?}"),
    }
    assert!(err.is_retriable(), "a 429 is a wait, not a stop");
}

#[tokio::test]
async fn a_five_hundred_is_retriable_and_a_four_oh_three_is_not() {
    for (status, retriable) in [(503u16, true), (403, false), (400, false)] {
        let mock = Mock::start(vec![Canned {
            status,
            headers: Vec::new(),
            body: "{}".to_string(),
        }])
        .await;
        let err = mock.queen(None).list_queues().await.unwrap_err();
        assert_eq!(err.is_retriable(), retriable, "status {status}");
    }
}

// ---------------------------------------------------------------------------
// list_queues
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_queues_reads_names_and_ignores_everything_else_the_broker_sends() {
    // The real body, with the dashboard fields the sink must not choke on.
    let mock = Mock::start(vec![Canned::ok(
        r#"{"queues":[
             {"name":"orders","partitions":10420,"retainedBytes":123456,"id":"abc"},
             {"name":"clicks","partitions":0,"messages":0},
             {"partitions":3}],
           "kvBytes":0,"timerBytes":0}"#,
    )])
    .await;
    let names = mock.queen(Some("sk_test")).list_queues().await.unwrap();
    assert_eq!(names, vec!["orders", "clicks"], "a nameless row is dropped");
    let req = mock.only();
    assert_eq!(req.method, "GET");
    assert_eq!(req.path, "/api/v1/resources/queues");
    assert_eq!(req.query, "");
}

#[tokio::test]
async fn the_host_header_can_be_overridden_for_a_shared_ingress() {
    let mock = Mock::start(vec![Canned::ok(r#"{"queues":[]}"#)]).await;
    let queen = mock.queen(Some("sk_test")).with_host("cell-7.queen.cloud");
    queen.list_queues().await.unwrap();
    assert_eq!(mock.only().host.as_deref(), Some("cell-7.queen.cloud"));
}

// ---------------------------------------------------------------------------
// The ceilings, refused here rather than discovered as a 400
// ---------------------------------------------------------------------------

#[tokio::test]
async fn the_batch_ceilings_are_checked_before_the_round_trip() {
    let mock = Mock::start(Vec::new()).await;
    let queen = mock.queen(None);

    let too_many_entries: Vec<FetchRequestEntry> = (0..1025)
        .map(|i| FetchRequestEntry {
            queue: "orders".into(),
            partition: i.to_string().into(),
            offset: 0,
            max_bytes: None,
        })
        .collect();
    let err = queen.fetch(too_many_entries, 0, 1).await.unwrap_err();
    assert!(matches!(err, SinkError::Config(_)), "{err:?}");

    let too_many_queues: Vec<ChangedRequestEntry> = (0..65)
        .map(|i| ChangedRequestEntry {
            queue: format!("q{i}"),
            since: None,
            after: None,
            limit: 10,
        })
        .collect();
    let err = queen.partitions_changed(too_many_queues).await.unwrap_err();
    assert!(matches!(err, SinkError::Config(_)), "{err:?}");

    assert!(mock.seen().is_empty(), "neither may reach the broker");
}
