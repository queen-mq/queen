//! The feed streams' broker stand-in for the runner tests: the conformance
//! kit's fake broker (clients/conformance/feed/fake-broker.mjs), started with
//! node on a port of its own for each test, and driven through its control
//! routes (a virtual clock, transactions pushed at a time, KV writes,
//! injected failures, a dump of everything).
//!
//! A `Proc` is one process of the stream's: its own tokio runtime, so that a
//! test can kill it (`crash`): its loop, its lock's renewals and its commits
//! stop at once, as with SIGKILL.

#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use queen_mq::streams::feed::{FeedHandle, FeedRunOptions, FeedStream};
use queen_mq::{Config, Queen};
use serde_json::{json, Value};

/// 2026-11-06T09:00:00.000Z, where the fake broker's clock starts.
pub const T0: f64 = 1_793_955_600_000.0;
pub const NS: &str = "queen-feed";

pub fn iso(ms: f64) -> String {
    let secs = (ms / 1000.0).floor() as i64;
    let milli = (ms - secs as f64 * 1000.0).round() as i64;
    let days = secs.div_euclid(86_400);
    let rem = secs.rem_euclid(86_400);
    // civil from days
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{milli:03}Z",
        rem / 3600,
        rem / 60 % 60,
        rem % 60
    )
}

pub struct Fake {
    child: Child,
    pub url: String,
    http: reqwest::Client,
    now: Arc<AtomicU64>,
}

impl Drop for Fake {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Fake {
    pub fn start() -> Fake {
        let script = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../conformance/feed/fake-broker.mjs"
        );
        let mut child = Command::new("node")
            .arg(script)
            .args(["--port", "0", "--quiet"])
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("node runs the fake broker (clients/conformance/feed/fake-broker.mjs)");
        let mut line = String::new();
        BufReader::new(child.stdout.take().expect("stdout"))
            .read_line(&mut line)
            .expect("the fake broker says its port");
        let port = serde_json::from_str::<Value>(&line).expect("{\"port\":N}")["port"]
            .as_u64()
            .expect("a port");
        Fake {
            child,
            url: format!("http://127.0.0.1:{port}"),
            http: reqwest::Client::new(),
            now: Arc::new(AtomicU64::new(T0.to_bits())),
        }
    }

    pub fn now_ms(&self) -> f64 {
        f64::from_bits(self.now.load(Ordering::SeqCst))
    }

    /// The runner's clock: the broker's (as JS tests pass `now: () => b.nowMs`).
    pub fn clock(&self) -> impl Fn() -> f64 + Send + Sync + 'static {
        let now = Arc::clone(&self.now);
        move || f64::from_bits(now.load(Ordering::SeqCst))
    }

    pub async fn post(&self, path: &str, body: Value) -> Value {
        let bytes = self
            .http
            .post(format!("{}{path}", self.url))
            .header("content-type", "application/json")
            .body(serde_json::to_vec(&body).unwrap())
            .send()
            .await
            .expect("the fake broker answers")
            .bytes()
            .await
            .unwrap_or_default();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    pub async fn get(&self, path: &str) -> Value {
        let bytes = self
            .http
            .get(format!("{}{path}", self.url))
            .send()
            .await
            .expect("the fake broker answers")
            .bytes()
            .await
            .unwrap_or_default();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    fn bump(&self, ms: f64) {
        let cur = self.now_ms();
        if ms > cur {
            self.now.store(ms.to_bits(), Ordering::SeqCst);
        }
    }

    /// The broker's clock moves to `T0 + ms` (never back); readers wake.
    pub async fn at(&self, ms: f64) {
        self.bump(T0 + ms);
        self.post("/_control/clock", json!({ "at": iso(T0 + ms) }))
            .await;
    }

    /// One transaction of a producer: its pushes share a position. `(queue, partition, data)`.
    pub async fn tx(&self, items: &[(&str, &str, Value)]) -> u64 {
        let push: Vec<Value> = items
            .iter()
            .map(|(q, p, d)| json!({ "queue": q, "partition": p, "data": d }))
            .collect();
        let r = self
            .post(
                "/_control/tx",
                json!({ "txs": [{ "at": iso(self.now_ms()), "push": push }] }),
            )
            .await;
        r["positions"][0].as_u64().expect("a position")
    }

    pub async fn tx_ids(&self, items: &[(&str, &str, Value, &str)]) -> u64 {
        let push: Vec<Value> = items
            .iter()
            .map(|(q, p, d, id)| json!({ "queue": q, "partition": p, "data": d, "transactionId": id }))
            .collect();
        let r = self
            .post(
                "/_control/tx",
                json!({ "txs": [{ "at": iso(self.now_ms()), "push": push }] }),
            )
            .await;
        r["positions"][0].as_u64().expect("a position")
    }

    pub async fn push(&self, queue: &str, partition: &str, data: Value) -> u64 {
        self.tx(&[(queue, partition, data)]).await
    }

    /// At `T0 + ms`, one push.
    pub async fn push_at(&self, ms: f64, queue: &str, partition: &str, data: Value) -> u64 {
        self.at(ms).await;
        self.push(queue, partition, data).await
    }

    pub async fn kv_put(&self, ns: &str, key: &str, value: Value) -> i64 {
        self.post(
            "/_control/kv",
            json!({ "ns": ns, "key": key, "value": value }),
        )
        .await["version"]
            .as_i64()
            .unwrap_or(0)
    }

    pub async fn inject(&self, route: &str, when: &str, status: u16, times: u32) {
        self.post(
            "/_control/inject",
            json!({ "route": route, "when": when, "status": status, "times": times }),
        )
        .await;
    }

    pub async fn dump(&self) -> Value {
        self.get("/_control/dump").await
    }

    /// The entries of a queue, as stored.
    pub async fn messages(&self, queue: &str) -> Vec<Value> {
        self.dump().await["log"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["queue"] == queue)
            .cloned()
            .collect()
    }

    pub async fn payloads(&self, queue: &str) -> Vec<Value> {
        self.messages(queue)
            .await
            .into_iter()
            .map(|e| e["payload"].clone())
            .collect()
    }

    /// `(value, version)` of a KV key.
    pub async fn kv(&self, ns: &str, key: &str) -> Option<(Value, i64)> {
        self.dump().await["kv"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|r| {
                (r["ns"] == ns && r["key"] == key)
                    .then(|| (r["value"].clone(), r["version"].as_i64().unwrap_or(0)))
            })
    }

    pub async fn record(&self, key: &str) -> Option<Value> {
        self.kv(NS, key).await.map(|r| r.0)
    }

    pub async fn kv_keys(&self, ns: &str, prefix: &str) -> Vec<String> {
        let mut out: Vec<String> = self.dump().await["kv"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|r| r["ns"] == ns && r["key"].as_str().unwrap_or("").starts_with(prefix))
            .map(|r| r["key"].as_str().unwrap().to_string())
            .collect();
        out.sort();
        out
    }

    pub async fn stats(&self) -> Value {
        self.dump().await["stats"].clone()
    }

    pub async fn kv_delete(&self, ns: &str, key: &str) {
        let r = self
            .post(
                "/api/v1/kv",
                json!({ "operations": [{ "op": "delete", "ns": ns, "key": key }] }),
            )
            .await;
        assert!(r["results"][0]["applied"] == true, "deleted {key}: {r}");
    }

    /// Somebody else takes the lock `name` (it expired, and another holder came):
    /// the holder's token stops working.
    pub async fn steal_lock(&self, name: &str) {
        for _ in 0..200 {
            let got = self
                .post(
                    "/api/v1/locks",
                    json!({ "operations": [{ "op": "get", "name": name }] }),
                )
                .await;
            let token = got["results"][0]["holders"][0]["token"].clone();
            let mut ops = Vec::new();
            if !token.is_null() {
                ops.push(json!({ "op": "release", "name": name, "token": token }));
            }
            ops.push(
                json!({ "op": "acquire", "name": name, "owner": "someone-else", "ttlSeconds": 60 }),
            );
            let r = self
                .post("/api/v1/locks", json!({ "operations": ops }))
                .await;
            let results = r["results"].as_array().cloned().unwrap_or_default();
            if results.last().is_some_and(|x| x["acquired"] == true) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("could not take the lock {name}");
    }

    /// The pieces of one snapshot layer, as stored: each `{ v, s, k, set, del }`,
    /// decoded here from the chunks (base64 of deflate-raw, `m: 1` goes on).
    pub async fn layer_pieces(&self, id: &str, sid: &str) -> Vec<Value> {
        let prefix = format!("{id}#snap#{sid}#");
        let kv = self.dump().await["kv"].as_array().cloned().unwrap();
        let mut chunks: Vec<(usize, Value)> = kv
            .iter()
            .filter(|r| r["ns"] == NS && r["key"].as_str().unwrap_or("").starts_with(&prefix))
            .map(|r| {
                (
                    r["key"].as_str().unwrap()[prefix.len()..]
                        .parse::<usize>()
                        .unwrap(),
                    r["value"].clone(),
                )
            })
            .collect();
        chunks.sort_by_key(|c| c.0);
        let mut pieces = Vec::new();
        let mut text = String::new();
        for (_, v) in chunks {
            text.push_str(v["d"].as_str().expect("a chunk's d"));
            if v["m"] == 1 {
                continue;
            }
            pieces.push(
                serde_json::from_slice(&inflate_raw(&b64_decode(&text))).expect("a piece is JSON"),
            );
            text.clear();
        }
        assert!(text.is_empty(), "a layer ends on a chunk without m");
        pieces
    }

    /// A process of the stream's, on this broker.
    pub fn proc(&self) -> Proc {
        Proc::new(&self.url)
    }
}

/// One process: a runtime of its own and a client.
pub struct Proc {
    rt: Option<tokio::runtime::Runtime>,
    pub queen: Queen,
}

impl Proc {
    pub fn new(url: &str) -> Proc {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("a runtime");
        let queen = Queen::connect(Config::new(url).retry_attempts(1)).expect("a client");
        Proc {
            rt: Some(rt),
            queen,
        }
    }

    /// `run()` in this process.
    pub async fn run(
        &self,
        stream: FeedStream,
        opts: FeedRunOptions,
    ) -> queen_mq::Result<FeedHandle> {
        self.rt
            .as_ref()
            .expect("a live process")
            .spawn(async move { stream.run(opts).await })
            .await
            .expect("run() did not panic")
    }

    /// The process dies: nothing of it runs any more.
    pub fn crash(&mut self) {
        if let Some(rt) = self.rt.take() {
            rt.shutdown_background();
        }
    }
}

impl Drop for Proc {
    fn drop(&mut self) {
        self.crash();
    }
}

/// `fast()`: short polls, commits at once, short retries.
pub fn fast(query_id: &str) -> FeedRunOptions {
    FeedRunOptions::new(query_id)
        .wait_millis(20)
        .commit_every_millis(0.0)
        .retry_delay_millis(1.0)
}

/// Waits until `cond` holds (or panics after `ms`).
pub async fn until<F, Fut>(ms: u64, what: &str, mut cond: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let end = std::time::Instant::now() + Duration::from_millis(ms);
    loop {
        if cond().await {
            return;
        }
        if std::time::Instant::now() > end {
            panic!("timed out waiting for {what}");
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub fn m(h: &FeedHandle, k: &str) -> f64 {
    h.metrics().get(k).and_then(|v| v.as_f64()).unwrap_or(0.0)
}

pub fn role(h: &FeedHandle) -> String {
    h.metrics()["role"].as_str().unwrap_or("").to_string()
}

// ------------------------------------------------------- snapshot bytes

const B64: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn b64_encode(bytes: &[u8]) -> String {
    let mut out = String::new();
    for c in bytes.chunks(3) {
        let n = (u32::from(c[0]) << 16)
            | (u32::from(*c.get(1).unwrap_or(&0)) << 8)
            | u32::from(*c.get(2).unwrap_or(&0));
        out.push(B64[(n >> 18) as usize & 63] as char);
        out.push(B64[(n >> 12) as usize & 63] as char);
        out.push(if c.len() > 1 {
            B64[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if c.len() > 2 {
            B64[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

pub fn b64_decode(text: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut acc, mut bits) = (0u32, 0);
    for c in text.bytes() {
        let v = match B64.iter().position(|&x| x == c) {
            Some(v) => v as u32,
            None if c == b'=' => break,
            None => panic!("not base64: {c}"),
        };
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
            acc &= (1 << bits) - 1;
        }
    }
    out
}

pub fn deflate_raw(bytes: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(bytes).unwrap();
    e.finish().unwrap()
}

pub fn inflate_raw(bytes: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    flate2::read::DeflateDecoder::new(bytes)
        .read_to_end(&mut out)
        .expect("deflate-raw");
    out
}

// --------------------------------------------------------------- a proxy

/// A TCP proxy in front of the fake broker, for one process: cut, its
/// connections drop and new ones are refused (a partition, a long pause).
/// `cut_at_feed_call(n)` cuts it as the n-th `POST /api/v1/feed` goes through.
pub struct Proxy {
    pub url: String,
    cut: Arc<AtomicBool>,
    epoch: Arc<tokio::sync::watch::Sender<u64>>,
    feed_calls: Arc<AtomicU64>,
    cut_at: Arc<AtomicU64>,
}

impl Proxy {
    pub async fn start(upstream: &str) -> Proxy {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let up = upstream.trim_start_matches("http://").to_string();
        let cut = Arc::new(AtomicBool::new(false));
        let (tx, _) = tokio::sync::watch::channel(0u64);
        let epoch = Arc::new(tx);
        let feed_calls = Arc::new(AtomicU64::new(0));
        let cut_at = Arc::new(AtomicU64::new(0));
        let (c, e, f, a) = (
            Arc::clone(&cut),
            Arc::clone(&epoch),
            Arc::clone(&feed_calls),
            Arc::clone(&cut_at),
        );
        tokio::spawn(async move {
            loop {
                let Ok((inbound, _)) = listener.accept().await else {
                    return;
                };
                if c.load(Ordering::SeqCst) {
                    drop(inbound);
                    continue;
                }
                let (up, c, e, f, a) = (
                    up.clone(),
                    Arc::clone(&c),
                    Arc::clone(&e),
                    Arc::clone(&f),
                    Arc::clone(&a),
                );
                let mut cut_seen = e.subscribe();
                tokio::spawn(async move {
                    let Ok(outbound) = tokio::net::TcpStream::connect(&up).await else {
                        return;
                    };
                    let (mut ri, mut wi) = inbound.into_split();
                    let (mut ro, mut wo) = outbound.into_split();
                    let to_broker = async {
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            let n = match ri.read(&mut buf).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => n,
                            };
                            let calls = buf[..n]
                                .windows(18)
                                .filter(|w| w == b"POST /api/v1/feed ")
                                .count() as u64;
                            if calls > 0 {
                                let total = f.fetch_add(calls, Ordering::SeqCst) + calls;
                                let at = a.load(Ordering::SeqCst);
                                if at > 0 && total >= at {
                                    c.store(true, Ordering::SeqCst);
                                    e.send_modify(|x| *x += 1);
                                    return;
                                }
                            }
                            if wo.write_all(&buf[..n]).await.is_err() {
                                return;
                            }
                        }
                    };
                    let to_client = async {
                        let _ = tokio::io::copy(&mut ro, &mut wi).await;
                    };
                    tokio::select! {
                        _ = to_broker => {}
                        _ = to_client => {}
                        _ = cut_seen.changed() => {}
                    }
                });
            }
        });
        Proxy {
            url,
            cut,
            epoch,
            feed_calls,
            cut_at,
        }
    }

    pub fn cut(&self, on: bool) {
        self.cut.store(on, Ordering::SeqCst);
        if on {
            self.epoch.send_modify(|x| *x += 1);
        }
    }

    pub fn cut_at_feed_call(&self, n: u64) {
        self.cut_at.store(n, Ordering::SeqCst);
    }

    pub fn is_cut(&self) -> bool {
        self.cut.load(Ordering::SeqCst)
    }

    pub fn feed_calls(&self) -> u64 {
        self.feed_calls.load(Ordering::SeqCst)
    }

    /// A process whose every call goes through this proxy.
    pub fn proc(&self) -> Proc {
        Proc::new(&self.url)
    }
}
