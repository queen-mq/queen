//! `queen.feed()`: the reader against scripted HTTP answers (feed.test.js,
//! FeedReader): what it sends, where it moves, and the nodes it asks when a
//! leader answers 421 or a follower does not answer.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use queen_mq::feed::{FeedFrom, FeedOptions};
use queen_mq::{Config, Queen};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// An HTTP server on 127.0.0.1 that answers from a list and records what it was asked.
struct Stub {
    url: String,
    addr: String,
    calls: Arc<Mutex<Vec<(String, Value)>>>,
}

async fn stub(answers: Vec<(u16, Value)>) -> Stub {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let answers = Arc::new(Mutex::new(VecDeque::from(answers)));
    let (c, a) = (Arc::clone(&calls), Arc::clone(&answers));
    tokio::spawn(async move {
        loop {
            let Ok((mut sock, _)) = listener.accept().await else {
                return;
            };
            let (c, a) = (Arc::clone(&c), Arc::clone(&a));
            tokio::spawn(async move {
                let mut buf = Vec::new();
                loop {
                    // One request: the head, then content-length bytes.
                    let head_end = loop {
                        if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            break i + 4;
                        }
                        let mut chunk = [0u8; 8192];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    };
                    let head = String::from_utf8_lossy(&buf[..head_end]).to_string();
                    let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                    let len = head
                        .lines()
                        .find_map(|l| {
                            let (k, v) = l.split_once(':')?;
                            k.trim()
                                .eq_ignore_ascii_case("content-length")
                                .then(|| v.trim().parse::<usize>().ok())?
                        })
                        .unwrap_or(0);
                    while buf.len() < head_end + len {
                        let mut chunk = [0u8; 8192];
                        match sock.read(&mut chunk).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => buf.extend_from_slice(&chunk[..n]),
                        }
                    }
                    let body: Value = serde_json::from_slice(&buf[head_end..head_end + len])
                        .unwrap_or(Value::Null);
                    buf.drain(..head_end + len);
                    c.lock().unwrap().push((path, body));
                    let (status, answer) = a
                        .lock()
                        .unwrap()
                        .pop_front()
                        .unwrap_or((500, json!({ "error": "no answer left" })));
                    let text = answer.to_string();
                    let out = format!(
                        "HTTP/1.1 {status} X\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{text}",
                        text.len()
                    );
                    if sock.write_all(out.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    Stub {
        url: format!("http://{addr}"),
        addr,
        calls,
    }
}

impl Stub {
    fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().unwrap().clone()
    }
}

fn entry(position: u64, n: u64) -> Value {
    json!({
        "id": format!("m{n}"), "kind": "push", "offset": n, "partition": "c1", "partitionId": "pid-1",
        "payload": { "n": n }, "position": position, "queue": "orders",
        "transactionId": format!("t{n}"), "ts": "2026-11-06T09:00:00.000000Z"
    })
}

fn page(entries: Vec<Value>, next: u64, caught_up: bool, applied: u64) -> Value {
    json!({ "applied": applied, "caughtUp": caught_up, "entries": entries, "next": next, "resolvedTs": "2026-11-06T09:00:01.000000Z" })
}

fn client(url: &str) -> Queen {
    Queen::connect(Config::new(url).retry_attempts(1)).unwrap()
}

#[tokio::test]
async fn sends_the_read_moves_to_next_and_stops_once_caught_up_when_not_following() {
    let s = stub(vec![
        (200, page(vec![entry(5, 1), entry(6, 2)], 7, false, 9)),
        (200, page(vec![entry(9, 3)], 10, true, 9)),
    ])
    .await;
    let q = client(&s.url);
    let mut reader = q
        .feed(
            FeedOptions::new()
                .queues(["orders*"])
                .where_("data.country", "=", json!("IT"))
                .from(FeedFrom::Earliest)
                .follow(false),
        )
        .unwrap();
    let mut seen = Vec::new();
    while let Some(e) = reader.next_entry().await.unwrap() {
        seen.push(e.to_value()["data"]["n"].clone());
    }
    assert_eq!(seen, [json!(1), json!(2), json!(3)]);
    assert_eq!(reader.position(), FeedFrom::Position(10));
    let calls = s.calls();
    assert_eq!(calls[0].0, "/api/v1/feed");
    assert_eq!(calls[0].1["queues"], json!(["orders*"]));
    assert_eq!(
        calls[0].1["where"],
        json!([{ "field": "data.country", "op": "=", "value": "IT" }])
    );
    assert_eq!(calls[0].1["from"], "earliest");
    assert_eq!(calls[0].1["maxWaitMs"], 0, "follow(false) never long-polls");
    assert_eq!(calls[1].1["from"], 7);
    assert_eq!(calls.len(), 2, "caught up: no third read");
}

#[tokio::test]
async fn a_leader_that_answers_421_sends_the_reader_to_the_followers_it_names() {
    let f2 = stub(vec![
        (200, page(vec![entry(5, 1)], 6, true, 5)),
        (200, page(vec![entry(6, 2)], 7, true, 6)),
    ])
    .await;
    let f3 = stub(vec![]).await;
    let main = stub(vec![(
        421,
        json!({
            "error": "the feed is read from followers", "code": "feed_on_leader",
            "followers": [{ "nodeId": 2, "httpAddr": f2.addr, "voter": true }, { "nodeId": 3, "httpAddr": f3.addr, "voter": true }]
        }),
    )])
    .await;
    let q = client(&main.url);
    let mut reader = q
        .feed(FeedOptions::new().from(FeedFrom::Position(1)))
        .unwrap();
    let first = reader.read(Some(0)).await.unwrap();
    assert_eq!(
        first
            .entries
            .iter()
            .map(|e| e.to_value()["data"]["n"].clone())
            .collect::<Vec<_>>(),
        [json!(1)]
    );
    // The next read goes straight to the follower.
    let second = reader.read(Some(0)).await.unwrap();
    assert_eq!(
        second
            .entries
            .iter()
            .map(|e| e.to_value()["data"]["n"].clone())
            .collect::<Vec<_>>(),
        [json!(2)]
    );
    assert_eq!(main.calls().len(), 1);
    assert_eq!(f2.calls().len(), 2);
    assert_eq!(
        f3.calls().len(),
        0,
        "named, not asked: the first follower answered"
    );
    assert_eq!(f2.calls()[1].1["from"], 6);
}

#[tokio::test]
async fn a_follower_that_does_not_answer_passes_the_read_to_the_next_node() {
    // A port nothing listens on.
    let gone = {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        l.local_addr().unwrap().to_string()
    };
    let main = stub(vec![(200, page(vec![entry(5, 1)], 6, true, 5))]).await;
    let q = client(&main.url);
    let mut reader = q
        .feed(
            FeedOptions::new()
                .from(FeedFrom::Position(1))
                .follower_urls([format!("http://{gone}")]),
        )
        .unwrap();
    let p = reader.read(Some(0)).await.unwrap();
    assert_eq!(
        p.entries
            .iter()
            .map(|e| e.to_value()["data"]["n"].clone())
            .collect::<Vec<_>>(),
        [json!(1)]
    );
    assert_eq!(main.calls().len(), 1, "then the configured URL");
}

#[tokio::test]
async fn a_leader_that_names_no_follower_that_answers_fails_with_the_reason() {
    let main = stub(vec![
        (
            421,
            json!({ "error": "leader", "code": "feed_on_leader", "followers": [] }),
        ),
        (
            421,
            json!({ "error": "leader", "code": "feed_on_leader", "followers": [] }),
        ),
        (
            421,
            json!({ "error": "leader", "code": "feed_on_leader", "followers": [] }),
        ),
    ])
    .await;
    let q = client(&main.url);
    let mut reader = q
        .feed(FeedOptions::new().from(FeedFrom::Position(1)))
        .unwrap();
    let e = reader.read(Some(0)).await.unwrap_err();
    assert!(
        e.to_string()
            .contains("every node asked is the leader or did not answer"),
        "{e}"
    );
    assert_eq!(main.calls().len(), 3, "the configured URL, three times");
}
