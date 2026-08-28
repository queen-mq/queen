//! `queen::HttpQueen` against a real socket.
//!
//! The unit tests in `src/queen.rs` and `src/handlers/metadata.rs` drive the
//! policy through a double, which is the right shape for the policy and reaches
//! none of the HTTP: the URL that gets built, the bearer header, the JSON that
//! comes back. That is the half a broker change would break silently, so it is
//! tested against a socket — a hand-rolled HTTP/1.1 responder rather than a
//! server framework, because the facade has no server dependency and this needs
//! about forty lines.
//!
//! What it serves is the broker's own answer, copied from
//! `queen.get_queues_v2` + the live enrichment (server/src/handlers/queues.rs),
//! from `queen.configure_queue_v1` (server/sql/procedures/012_configure.sql)
//! and from `render_push_results` (server/src/handlers/data.rs).

use std::sync::{Arc, Mutex};

use queen_kafka::queen::{FetchEntry, HttpQueen, KvOp, PushItem, QueenApi};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One request as the stub server saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    line: String,
    authorization: Option<String>,
    /// EVERY `Host` header the request carried, in order. A `Vec` and not an
    /// `Option` on purpose: the M5 SNI forwarding sets this header explicitly,
    /// and the failure mode worth catching is hyper ADDING its own beside ours
    /// rather than leaving the one we set.
    host: Vec<String>,
    body: String,
}

/// A canned reply: status line code and body, plus any header the facade reads
/// off the answer rather than out of it.
#[derive(Clone)]
struct Canned {
    status: u16,
    body: &'static str,
    /// Extra header lines, already CRLF-terminated. `Retry-After` is the one
    /// the facade reads (see `throttle`), and it only ever arrives on a 429.
    extra: &'static str,
}

impl Canned {
    fn new(status: u16, body: &'static str) -> Canned {
        Canned {
            status,
            body,
            extra: "",
        }
    }
}

/// Serve `replies` in order, one per connection, recording what was asked.
/// Returns the base URL and the log.
async fn stub(replies: Vec<Canned>) -> (String, Arc<Mutex<Vec<Seen>>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let seen = Arc::new(Mutex::new(Vec::new()));

    let log = Arc::clone(&seen);
    tokio::spawn(async move {
        let mut replies = replies.into_iter();
        loop {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let log = Arc::clone(&log);
            let reply = replies.next().unwrap_or(Canned::new(
                500,
                "{\"error\":\"the stub ran out of replies\"}",
            ));
            tokio::spawn(async move {
                let mut raw = Vec::new();
                let mut chunk = [0u8; 4096];
                // Read headers, then exactly Content-Length more bytes.
                let head_end = loop {
                    if let Some(i) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
                        break i + 4;
                    }
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => return,
                        Ok(n) => raw.extend_from_slice(&chunk[..n]),
                    }
                };
                let head = String::from_utf8_lossy(&raw[..head_end]).to_string();
                let len: usize = header(&head, "content-length")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(0);
                while raw.len() < head_end + len {
                    match stream.read(&mut chunk).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => raw.extend_from_slice(&chunk[..n]),
                    }
                }
                log.lock().unwrap().push(Seen {
                    line: head.lines().next().unwrap_or_default().to_string(),
                    authorization: header(&head, "authorization"),
                    host: headers(&head, "host"),
                    body: String::from_utf8_lossy(&raw[head_end..]).to_string(),
                });

                let out = format!(
                    "HTTP/1.1 {} X\r\nContent-Type: application/json\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.status,
                    reply.extra,
                    reply.body.len(),
                    reply.body
                );
                let _ = stream.write_all(out.as_bytes()).await;
                let _ = stream.flush().await;
            });
        }
    });

    (base, seen)
}

/// Every value a header was sent with, in order.
fn headers(head: &str, name: &str) -> Vec<String> {
    head.lines()
        .skip(1)
        .filter_map(|l| {
            let (k, v) = l.split_once(':')?;
            k.trim()
                .eq_ignore_ascii_case(name)
                .then(|| v.trim().to_string())
        })
        .collect()
}

/// Case-insensitive header lookup over a raw request head.
fn header(head: &str, name: &str) -> Option<String> {
    head.lines().skip(1).find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim()
            .eq_ignore_ascii_case(name)
            .then(|| v.trim().to_string())
    })
}

const LIST_BODY: &str = r#"{"queues":[
  {"id":"0d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a11","name":"orders","namespace":"","task":"",
   "createdAt":"2026-08-27T10:00:00.000Z","partitions":12,"retainedBytes":4096,
   "segments":{"segments":3,"messages":900},
   "messages":{"total":900,"pending":10,"processing":2}}
],"kvBytes":0,"timerBytes":0}"#;

const CONFIGURE_BODY: &str = r#"{"configured":true,"queueId":"0d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a11",
 "partitionId":null,"queue":"orders","namespace":"","task":"","storage":"segments",
 "options":{"priority":0,"leaseTime":300,"retryLimit":3,"ttl":3600}}"#;

#[tokio::test]
async fn the_queue_list_is_fetched_from_the_documented_route() {
    let (base, seen) = stub(vec![Canned::new(200, LIST_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();

    let queues = api.list_queues(None).await.unwrap();
    assert_eq!(queues.len(), 1);
    assert_eq!(queues[0].name, "orders");
    assert_eq!(queues[0].partitions, 12);

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].line, "GET /api/v1/resources/queues HTTP/1.1");
    assert_eq!(seen[0].authorization, None, "no token, no header");
}

#[tokio::test]
async fn a_queue_is_created_by_posting_its_name_to_configure() {
    let (base, seen) = stub(vec![Canned::new(200, CONFIGURE_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();

    api.create_queue("orders", None).await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].line, "POST /api/v1/configure HTTP/1.1");
    assert_eq!(seen[0].body, r#"{"queue":"orders"}"#);
}

/// The token is a per-call argument, and it lands as the Bearer header the
/// broker's auth layer reads (server/src/auth.rs::extract_bearer).
#[tokio::test]
async fn the_token_travels_as_a_bearer_header_on_each_call() {
    let (base, seen) = stub(vec![
        Canned::new(200, LIST_BODY),
        Canned::new(200, CONFIGURE_BODY),
    ])
    .await;
    let api = HttpQueen::new(&base).unwrap();

    api.list_queues(Some("tenant-token")).await.unwrap();
    api.create_queue("orders", Some("tenant-token"))
        .await
        .unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    for s in seen {
        assert_eq!(s.authorization.as_deref(), Some("Bearer tenant-token"));
    }
}

/// A base URL with a trailing slash must not produce `//api/v1/...`.
#[tokio::test]
async fn a_trailing_slash_does_not_double_up() {
    let (base, seen) = stub(vec![Canned::new(200, LIST_BODY)]).await;
    let api = HttpQueen::new(&format!("{base}/")).unwrap();
    api.list_queues(None).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[0].line,
        "GET /api/v1/resources/queues HTTP/1.1"
    );
}

#[tokio::test]
async fn an_error_status_is_an_error_and_says_what_came_back() {
    let (base, _) = stub(vec![Canned::new(401, "{\"error\":\"invalid token\"}")]).await;
    let api = HttpQueen::new(&base).unwrap();
    let err = api.list_queues(None).await.unwrap_err().to_string();
    assert!(err.contains("401"), "{err}");
    assert!(err.contains("invalid token"), "{err}");
}

/// The failure mode that is NOT a bad status: `handle_configure` can answer 200
/// with the stored procedure's `{"error":…}` inside it. Reporting that as a
/// created queue would leave the facade advertising a topic Queen does not have.
#[tokio::test]
async fn a_200_that_carries_an_error_is_not_a_created_queue() {
    let (base, _) = stub(vec![Canned::new(
        200,
        "{\"error\":\"configure failed: relation does not exist\"}",
    )])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    assert!(api.create_queue("orders", None).await.is_err());
}

/// ...and neither is a 200 that simply does not confirm.
#[tokio::test]
async fn a_200_without_configured_true_is_not_a_created_queue() {
    let (base, _) = stub(vec![Canned::new(200, "{\"queue\":\"orders\"}")]).await;
    let api = HttpQueen::new(&base).unwrap();
    assert!(api.create_queue("orders", None).await.is_err());
}

// ------------------------------------------------------------------- push

/// The exact body `POST /api/v1/push` answers, copied from
/// `render_push_results`: `index`, the two ids, the queue, the status, and — as
/// of C1 — the assigned absolute `offset`.
const PUSH_BODY: &str = r#"[
 {"index":0,"message_id":"0190aaaa-0000-7000-8000-000000000001","transaction_id":"t1",
  "queueName":"orders","status":"queued","offset":41},
 {"index":1,"message_id":"0190aaaa-0000-7000-8000-000000000002","transaction_id":"t2",
  "queueName":"orders","status":"queued","offset":42}
]"#;

fn items() -> Vec<PushItem> {
    vec![
        PushItem {
            queue: "orders".into(),
            partition: "3".into(),
            payload: serde_json::json!({"k": null, "v": "b25l"}),
        },
        PushItem {
            queue: "orders".into(),
            partition: "3".into(),
            payload: serde_json::json!({"k": null, "v": "dHdv"}),
        },
    ]
}

#[tokio::test]
async fn a_push_posts_the_documented_body_and_reads_the_offsets_back() {
    let (base, seen) = stub(vec![Canned::new(201, PUSH_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();

    let results = api.push(&items(), Some("tenant-token")).await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].line, "POST /api/v1/push HTTP/1.1");
    assert_eq!(
        seen[0].authorization.as_deref(),
        Some("Bearer tenant-token")
    );
    // The shape `PushBody`/`PushItem` takes (server/src/handlers/data.rs): the
    // partition is the Kafka index as a NAME, and no transactionId is sent.
    assert_eq!(
        seen[0].body,
        r#"{"items":[{"queue":"orders","partition":"3","payload":{"k":null,"v":"b25l"}},{"queue":"orders","partition":"3","payload":{"k":null,"v":"dHdv"}}]}"#
    );

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].status, "queued");
    assert_eq!(results[0].offset, Some(41));
    assert_eq!(results[1].offset, Some(42));
}

/// C1 is ADDITIVE: a broker that predates it answers the same objects without
/// an `offset` key. That has to parse — and report "no offset", which is what
/// makes the produce handler refuse to guess one.
#[tokio::test]
async fn a_pre_c1_push_response_parses_with_no_offsets() {
    let (base, _) = stub(vec![Canned::new(
        201,
        r#"[{"index":0,"message_id":"m","transaction_id":"t","queueName":"orders","status":"queued"},
                  {"index":1,"message_id":"m","transaction_id":"t","queueName":"orders","status":"buffered"}]"#,
    )])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    let results = api.push(&items(), None).await.unwrap();
    assert_eq!(results[0].offset, None);
    assert_eq!(results[1].status, "buffered");
}

/// Results are matched to items by the `index` the broker stamps, not by
/// position: a produce answer built from the wrong item reports one partition's
/// offset for another's records.
#[tokio::test]
async fn results_are_aligned_by_index_not_by_arrival_order() {
    let (base, _) = stub(vec![Canned::new(
        201,
        r#"[{"index":1,"status":"queued","offset":9},{"index":0,"status":"queued","offset":8}]"#,
    )])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    let results = api.push(&items(), None).await.unwrap();
    assert_eq!(results[0].offset, Some(8));
    assert_eq!(results[1].offset, Some(9));
}

/// A response that does not cover every item exactly once is an error rather
/// than a shifted answer.
#[tokio::test]
async fn a_push_response_that_does_not_cover_the_items_is_an_error() {
    for body in [
        // One result for two items.
        r#"[{"index":0,"status":"queued","offset":1}]"#,
        // An index nothing was sent for.
        r#"[{"index":0,"status":"queued","offset":1},{"index":7,"status":"queued","offset":2}]"#,
        // The same item answered twice, and item 1 never.
        r#"[{"index":0,"status":"queued","offset":1},{"index":0,"status":"queued","offset":2}]"#,
        // Not the shape at all.
        r#"{"error":"push failed"}"#,
    ] {
        let (base, _) = stub(vec![Canned::new(201, body)]).await;
        let api = HttpQueen::new(&base).unwrap();
        assert!(
            api.push(&items(), None).await.is_err(),
            "{body} was accepted"
        );
    }
}

/// The status codes the produce handler maps to Kafka errors have to arrive as
/// `Error::Status` with the code intact — 413 is the payload-size refusal.
#[tokio::test]
async fn a_rejected_push_reports_its_status_code() {
    let (base, _) = stub(vec![Canned::new(413, "{\"error\":\"body too large\"}")]).await;
    let api = HttpQueen::new(&base).unwrap();
    let err = api.push(&items(), None).await.unwrap_err().to_string();
    assert!(err.contains("413"), "{err}");
}

// ------------------------------------------------------------------ fetch

/// The exact body `POST /api/v1/fetch` answers, copied from `render_fetch`:
/// each entry names its own queue and partition, every record carries its
/// absolute offset and the segment's `transactionId`/`ts`, and the bounds are
/// there whether or not a record was.
const FETCH_BODY: &str = r#"{"entries":[
 {"queue":"orders","partition":"3","records":[
   {"offset":41,"transactionId":"t1","payload":{"k":null,"v":"b25l"},"ts":"2026-08-27T10:00:00.500000Z"},
   {"offset":42,"transactionId":"t2","payload":{"k":null,"v":"dHdv"},"ts":"2026-08-27T10:00:00.500000Z"}
  ],"highWatermark":43,"logStartOffset":7},
 {"queue":"orders","partition":"4","records":[],"highWatermark":9,"logStartOffset":9,
  "error":"OFFSET_OUT_OF_RANGE"}
]}"#;

fn entries() -> Vec<FetchEntry> {
    vec![
        FetchEntry {
            queue: "orders".into(),
            partition: "3".into(),
            offset: 41,
            max_bytes: 1048576,
        },
        FetchEntry {
            queue: "orders".into(),
            partition: "4".into(),
            offset: 99,
            max_bytes: 1048576,
        },
    ]
}

#[tokio::test]
async fn a_fetch_posts_the_documented_body_and_reads_the_records_back() {
    let (base, seen) = stub(vec![Canned::new(200, FETCH_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();

    let got = api
        .fetch(&entries(), 500, 1, Some("tenant-token"))
        .await
        .unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].line, "POST /api/v1/fetch HTTP/1.1");
    assert_eq!(
        seen[0].authorization.as_deref(),
        Some("Bearer tenant-token")
    );
    // The shape server/src/handlers/fetch.rs takes: the partition is the Kafka
    // index as a NAME, and the long poll rides on the request, not the entry.
    assert_eq!(
        seen[0].body,
        r#"{"entries":[{"queue":"orders","partition":"3","offset":41,"maxBytes":1048576},{"queue":"orders","partition":"4","offset":99,"maxBytes":1048576}],"maxWaitMs":500,"minBytes":1}"#
    );

    assert_eq!(got.len(), 2);
    assert_eq!(got[0].records.len(), 2);
    assert_eq!(got[0].records[0].offset, 41);
    assert_eq!(got[0].records[0].payload["v"], "b25l");
    assert_eq!(got[0].records[1].offset, 42);
    assert_eq!(got[0].high_watermark, 43);
    assert_eq!(got[0].log_start_offset, 7);
    assert_eq!(got[0].error, None);
    // The segment timestamp becomes the epoch millis a Kafka record carries.
    assert_eq!(got[0].records[0].timestamp_ms(), Some(1_787_824_800_500));

    // An entry that errored still reports the bounds — which is exactly what
    // makes it the ListOffsets probe.
    assert_eq!(got[1].error.as_deref(), Some("OFFSET_OUT_OF_RANGE"));
    assert!(got[1].records.is_empty());
    assert_eq!(got[1].high_watermark, 9);
    assert_eq!(got[1].log_start_offset, 9);
}

/// The bounds probe as ListOffsets sends it: offset `i64::MAX`, one byte of
/// budget, no parking. It has to survive JSON round-tripping as an integer.
#[tokio::test]
async fn the_bounds_probe_serializes_its_sentinel_offset() {
    let (base, seen) = stub(vec![Canned::new(
        200,
        r#"{"entries":[{"queue":"orders","partition":"0","records":[],
                  "highWatermark":12,"logStartOffset":4,"error":"OFFSET_OUT_OF_RANGE"}]}"#,
    )])
    .await;
    let api = HttpQueen::new(&base).unwrap();

    let probe = vec![FetchEntry {
        queue: "orders".into(),
        partition: "0".into(),
        offset: i64::MAX,
        max_bytes: 1,
    }];
    let got = api.fetch(&probe, 0, 0, None).await.unwrap();

    assert_eq!(
        seen.lock().unwrap()[0].body,
        r#"{"entries":[{"queue":"orders","partition":"0","offset":9223372036854775807,"maxBytes":1}],"maxWaitMs":0,"minBytes":0}"#
    );
    assert_eq!((got[0].high_watermark, got[0].log_start_offset), (12, 4));
}

/// An answer that does not line up with the entries sent is an error, not
/// records read against the wrong partition.
#[tokio::test]
async fn a_fetch_response_that_does_not_match_the_entries_is_an_error() {
    for body in [
        // One entry for two asked.
        r#"{"entries":[{"queue":"orders","partition":"3","records":[],"highWatermark":0,"logStartOffset":0}]}"#,
        // The right count, the wrong lanes.
        r#"{"entries":[
             {"queue":"orders","partition":"9","records":[],"highWatermark":0,"logStartOffset":0},
             {"queue":"orders","partition":"4","records":[],"highWatermark":0,"logStartOffset":0}]}"#,
        // Not the shape at all.
        r#"{"error":"fetch failed"}"#,
    ] {
        let (base, _) = stub(vec![Canned::new(200, body)]).await;
        let api = HttpQueen::new(&base).unwrap();
        assert!(
            api.fetch(&entries(), 0, 1, None).await.is_err(),
            "{body} was accepted"
        );
    }
}

#[tokio::test]
async fn a_rejected_fetch_reports_its_status_code() {
    let (base, _) = stub(vec![Canned::new(401, "{\"error\":\"invalid token\"}")]).await;
    let api = HttpQueen::new(&base).unwrap();
    let err = api
        .fetch(&entries(), 0, 1, None)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("401"), "{err}");
}

// ------------------------------------------------------------------ POST /kv
//
// The offset store (M4). The body shape and the `{"results":[…]}` envelope are
// server/src/handlers/kv.rs's, and the per-result fields are the ones
// `kv_apply_v1` builds (server/sql/procedures/024_kv.sql).

/// A real answer to a put + a getMany, with the results OUT of operation order:
/// the stored procedure applies writes in key order and reports by ordinal, so
/// the `index` is the only thing that lines them up.
const KV_BODY: &str = r#"{"results":[
  {"index":1,"op":"getMany",
   "rows":[{"key":"qk:group:g:orders:0","value":{"offset":41,"metadata":"batch-7","ts":1787824800500},
            "version":91005,"expiresAt":null,"updatedAt":"2026-08-27T10:00:00.500000Z"}],
   "missing":["qk:group:g:orders:1"],"truncated":false},
  {"index":0,"op":"put","applied":true,"key":"qk:group:g:orders:0",
   "value":{"offset":41,"metadata":"batch-7","ts":1787824800500},"version":91005}
]}"#;

#[tokio::test]
async fn a_kv_call_posts_the_documented_body_and_reads_the_results_back() {
    let (base, seen) = stub(vec![Canned::new(200, KV_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();

    let ops = vec![
        KvOp::put(
            "queen-kafka",
            "qk:group:g:orders:0",
            serde_json::json!({"offset": 41, "metadata": "batch-7", "ts": 1_787_824_800_500i64}),
        ),
        KvOp::GetMany {
            ns: "queen-kafka".into(),
            keys: vec!["qk:group:g:orders:0".into(), "qk:group:g:orders:1".into()],
        },
    ];
    let got = api.kv(&ops, Some("tenant-token")).await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].line, "POST /api/v1/kv HTTP/1.1");
    assert_eq!(
        seen[0].authorization.as_deref(),
        Some("Bearer tenant-token")
    );
    // `{"operations":[…]}`, the same key the transaction wire takes, and a put
    // that declares its expiry: exactly one of ttlSeconds and forever is
    // mandatory, and a committed offset's answer is never.
    assert_eq!(
        seen[0].body,
        r#"{"operations":[{"op":"put","ns":"queen-kafka","key":"qk:group:g:orders:0","value":{"metadata":"batch-7","offset":41,"ts":1787824800500},"forever":true},{"op":"getMany","ns":"queen-kafka","keys":["qk:group:g:orders:0","qk:group:g:orders:1"]}]}"#
    );

    // Realigned by `index`, not by arrival.
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].op, "put");
    assert_eq!(got[0].applied, Some(true));
    assert_eq!(got[1].op, "getMany");
    assert_eq!(got[1].rows.len(), 1);
    assert_eq!(got[1].rows[0].key, "qk:group:g:orders:0");
    assert_eq!(got[1].rows[0].value["offset"], 41);
    assert_eq!(got[1].missing, ["qk:group:g:orders:1"]);
    assert!(!got[1].truncated);
}

/// The KV surface answers a refusal as a status plus its own error envelope
/// (`kv_bad_request`, `payload_too_large`, `kv_unavailable`), and every one of
/// them has to reach the caller as an error rather than as an empty read.
#[tokio::test]
async fn a_rejected_kv_call_reports_its_status_code() {
    let (base, _) = stub(vec![Canned::new(
        429,
        r#"{"error":"rate_limited","reason":"rate_limited"}"#,
    )])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    let err = api
        .kv(
            &[KvOp::put("queen-kafka", "k", serde_json::Value::Null)],
            None,
        )
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("429"), "{err}");
}

#[tokio::test]
async fn an_unreachable_broker_is_a_transport_error_not_a_panic() {
    // Bind and drop: the port is almost certainly free and refuses instantly.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    drop(listener);

    let api = HttpQueen::new(&base).unwrap();
    assert!(api.list_queues(None).await.is_err());
}

// ------------------------------------------------------- M5: host and throttle

/// The SNI forwarding, at the only level where it is real: the header on the
/// wire.
///
/// The proxy routes by `Host` — its first DNS label names the cluster, unless
/// the name is one of `QUEEN_PROXY_SHARED_HOSTS`, where the credential names it
/// (proxy/src/acting.rs) — so a facade in front of many tenants has to send the
/// name each connection asked for. The trap this pins is hyper's: it fills in a
/// `Host` from the URL's authority when the request does not already carry one,
/// and a second one beside ours would be a request with two `Host` headers.
#[tokio::test]
async fn a_host_scoped_client_sends_exactly_that_host() {
    let (base, seen) = stub(vec![Canned::new(200, LIST_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();
    let scoped = api
        .with_host("acme.queenmq.cloud")
        .expect("the real client can stamp a Host");

    scoped.list_queues(Some("tenant-token")).await.unwrap();

    let seen = seen.lock().unwrap().clone();
    assert_eq!(
        seen[0].host,
        ["acme.queenmq.cloud"],
        "the Host the proxy routes on is not the one the connection asked for"
    );
    // ...and the URL is unchanged: only the header moves, so the socket still
    // goes to QUEEN_URL.
    assert_eq!(seen[0].line, "GET /api/v1/resources/queues HTTP/1.1");
    assert_eq!(
        seen[0].authorization.as_deref(),
        Some("Bearer tenant-token")
    );
}

/// Without it, the client is exactly what it was: one `Host`, the URL's own.
#[tokio::test]
async fn an_unscoped_client_sends_the_url_host() {
    let (base, seen) = stub(vec![Canned::new(200, LIST_BODY)]).await;
    let api = HttpQueen::new(&base).unwrap();
    api.list_queues(None).await.unwrap();
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen[0].host.len(), 1, "{:?}", seen[0].host);
    assert!(
        seen[0].host[0].starts_with("127.0.0.1:"),
        "{:?}",
        seen[0].host
    );
}

/// A rate-capped tenant: the proxy's `Retry-After` (proxy/src/errors.rs
/// `err_429`) has to survive the trip into `throttle_time_ms`, or every Kafka
/// client backs off by its own default instead of by what the cap said.
#[tokio::test]
async fn a_429_carries_its_retry_after_into_the_kafka_throttle() {
    let (base, _) = stub(vec![Canned {
        status: 429,
        body: r#"{"error":"request rate limit exceeded","code":"rate_limited"}"#,
        extra: "Retry-After: 5\r\n",
    }])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    let err = api.list_queues(None).await.unwrap_err();

    assert_eq!(err.retry_after_ms(), Some(5_000));
    assert_eq!(queen_kafka::throttle::for_error(&err), Some(5_000));
}

/// A 429 with no hint is still a throttle — with the default wait, never zero.
#[tokio::test]
async fn a_429_without_a_hint_still_throttles() {
    let (base, _) = stub(vec![Canned::new(429, r#"{"code":"rate_limited"}"#)]).await;
    let api = HttpQueen::new(&base).unwrap();
    let err = api.list_queues(None).await.unwrap_err();

    assert_eq!(err.retry_after_ms(), None);
    assert_eq!(
        queen_kafka::throttle::for_error(&err),
        Some(queen_kafka::throttle::DEFAULT_MS)
    );
}

/// A `Retry-After` this facade cannot read is `None`, not a guess: the value
/// becomes a sleep, and a misread date is a consumer parked for hours.
#[tokio::test]
async fn an_unreadable_retry_after_is_not_guessed_at() {
    for value in [
        // RFC 9110 also allows an HTTP-date. The proxy never writes one.
        "Retry-After: Wed, 21 Oct 2026 07:28:00 GMT\r\n",
        "Retry-After: soon\r\n",
        "Retry-After: -5\r\n",
        "Retry-After: \r\n",
    ] {
        let (base, _) = stub(vec![Canned {
            status: 429,
            body: r#"{"code":"rate_limited"}"#,
            extra: value,
        }])
        .await;
        let api = HttpQueen::new(&base).unwrap();
        let err = api.list_queues(None).await.unwrap_err();
        assert_eq!(err.retry_after_ms(), None, "{value:?} was read as a wait");
        // ...and the answer is still a throttle, with the default beside it.
        assert_eq!(
            queen_kafka::throttle::for_error(&err),
            Some(queen_kafka::throttle::DEFAULT_MS)
        );
    }
}
