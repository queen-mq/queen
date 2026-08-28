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
//! `queen.get_queues_v2` + the live enrichment (server/src/handlers/queues.rs)
//! and from `queen.configure_queue_v1` (server/sql/procedures/012_configure.sql).

use std::sync::{Arc, Mutex};

use queen_kafka::queen::{HttpQueen, QueenApi};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One request as the stub server saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    line: String,
    authorization: Option<String>,
    body: String,
}

/// A canned reply: status line code and body.
#[derive(Clone)]
struct Canned {
    status: u16,
    body: &'static str,
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
            let reply = replies.next().unwrap_or(Canned {
                status: 500,
                body: "{\"error\":\"the stub ran out of replies\"}",
            });
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
                    body: String::from_utf8_lossy(&raw[head_end..]).to_string(),
                });

                let out = format!(
                    "HTTP/1.1 {} X\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                    reply.status,
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
    let (base, seen) = stub(vec![Canned {
        status: 200,
        body: LIST_BODY,
    }])
    .await;
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
    let (base, seen) = stub(vec![Canned {
        status: 200,
        body: CONFIGURE_BODY,
    }])
    .await;
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
        Canned {
            status: 200,
            body: LIST_BODY,
        },
        Canned {
            status: 200,
            body: CONFIGURE_BODY,
        },
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
    let (base, seen) = stub(vec![Canned {
        status: 200,
        body: LIST_BODY,
    }])
    .await;
    let api = HttpQueen::new(&format!("{base}/")).unwrap();
    api.list_queues(None).await.unwrap();
    assert_eq!(
        seen.lock().unwrap()[0].line,
        "GET /api/v1/resources/queues HTTP/1.1"
    );
}

#[tokio::test]
async fn an_error_status_is_an_error_and_says_what_came_back() {
    let (base, _) = stub(vec![Canned {
        status: 401,
        body: "{\"error\":\"invalid token\"}",
    }])
    .await;
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
    let (base, _) = stub(vec![Canned {
        status: 200,
        body: "{\"error\":\"configure failed: relation does not exist\"}",
    }])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    assert!(api.create_queue("orders", None).await.is_err());
}

/// ...and neither is a 200 that simply does not confirm.
#[tokio::test]
async fn a_200_without_configured_true_is_not_a_created_queue() {
    let (base, _) = stub(vec![Canned {
        status: 200,
        body: "{\"queue\":\"orders\"}",
    }])
    .await;
    let api = HttpQueen::new(&base).unwrap();
    assert!(api.create_queue("orders", None).await.is_err());
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
