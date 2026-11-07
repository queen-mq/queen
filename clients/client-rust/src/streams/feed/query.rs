//! Live state of a running feed stream, read from outside its process
//! (feed/query.js).
//!
//! Each active runner (each shard) answers questions on an ephemeral queue
//! of its own, `queen-feed-q.<hash of the queryId>.<shard>`; the asker mints
//! an inbox (`queen-feed-r.<hex>`), sends the question with it as `replyTo`,
//! and waits there. Nothing is stored, no extra port is opened.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::js;
use crate::streams::hash::sha256_hex;
use crate::Queen;
use queen_protocol::kv::KvOperation;

use super::runner::{Shared, POSITION_NAMESPACE};

const DEFAULT_TIMEOUT_MILLIS: u64 = 5000;
const SERVE_WAIT_MILLIS: u64 = 10_000;

/// The ephemeral queue the runner of `query_id` (shard `index`) answers on.
pub fn query_queue_name(query_id: &str, index: u32) -> String {
    format!(
        "queen-feed-q.{}.{index}",
        &sha256_hex(&format!("queen-feed\n{query_id}"))[..16]
    )
}

fn hex(n: usize) -> String {
    (0..n)
        .map(|_| format!("{:02x}", rand::random::<u8>()))
        .collect()
}

/// The loop that answers questions for one active runner.
pub(crate) struct StateServer {
    stopped: Arc<AtomicBool>,
}

impl StateServer {
    pub fn start(queen: Queen, queue: String, sh: Arc<Shared>, wait_millis: Option<u64>) -> Self {
        let stopped = Arc::new(AtomicBool::new(false));
        let flag = Arc::clone(&stopped);
        let wait = wait_millis.unwrap_or(SERVE_WAIT_MILLIS);
        tokio::spawn(async move {
            let mut backoff = 250u64;
            let mut warned = false;
            while !flag.load(Ordering::SeqCst) {
                let got = queen
                    .ephemeral()
                    .pop(&queue)
                    .wait(true)
                    .poll_timeout(Duration::from_millis(wait))
                    .batch(32)
                    .commit_on_delivery(true)
                    .send()
                    .await;
                let got = match got {
                    Ok(g) => {
                        backoff = 250;
                        g
                    }
                    Err(err) => {
                        if flag.load(Ordering::SeqCst) {
                            return;
                        }
                        if err.code().map(|c| c.as_str()) == Some("ephemeral_unsupported")
                            || err.status() == Some(404)
                        {
                            tracing::warn!(queue = %queue, error = %err, "FeedStateServer.unsupported");
                            return;
                        }
                        if !warned {
                            warned = true;
                            tracing::warn!(queue = %queue, error = %err, "FeedStateServer.pop");
                        }
                        tokio::time::sleep(Duration::from_millis(backoff)).await;
                        backoff = (backoff * 2).min(5000);
                        continue;
                    }
                };
                for m in got.messages {
                    let req = m.payload;
                    let Some(reply_to) = req
                        .get("replyTo")
                        .and_then(|r| r.as_str())
                        .map(str::to_string)
                    else {
                        continue;
                    };
                    if !req.is_object() {
                        continue;
                    }
                    let reply = match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        sh.answer(&req)
                    })) {
                        Ok(r) => r,
                        Err(_) => json!({ "error": "the answer failed" }),
                    };
                    let mut out: Map<String, Value> =
                        reply.as_object().cloned().unwrap_or_default();
                    if let Some(id) = req.get("id") {
                        out.insert("id".into(), id.clone());
                    }
                    if let Err(e) = queen
                        .ephemeral()
                        .push(&reply_to, Value::Object(out))
                        .send()
                        .await
                    {
                        tracing::warn!(queue = %queue, error = %e, "FeedStateServer.reply");
                    }
                }
            }
        });
        Self { stopped }
    }

    /// Stops after the long poll in hand answers.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }
}

/// The options of [`Stream::view`](crate::streams::Stream::view).
#[derive(Debug, Clone)]
pub struct ViewOptions {
    /// How many shards the stream runs in; default: read from its layout in KV, else 1.
    pub shards: Option<u32>,
    /// How long to wait for the answers.
    pub timeout_millis: u64,
}

impl Default for ViewOptions {
    fn default() -> Self {
        Self {
            shards: None,
            timeout_millis: DEFAULT_TIMEOUT_MILLIS,
        }
    }
}

/// Ask the runners of a stream about their live state.
#[derive(Clone)]
pub struct FeedView {
    queen: Queen,
    query_id: String,
    shards: Option<u32>,
    timeout: u64,
}

impl FeedView {
    pub(crate) fn new(queen: &Queen, query_id: &str, opts: ViewOptions) -> Result<Self> {
        if query_id.is_empty() {
            return Err(Error::Invalid(
                "Stream.view: queryId is the stream's queryId".into(),
            ));
        }
        Ok(Self {
            queen: queen.clone(),
            query_id: query_id.to_string(),
            shards: opts.shards,
            timeout: opts.timeout_millis,
        })
    }

    /// The value of one key in one step: `{ found, value?, shard?, position? }`.
    pub async fn get(&self, step: &str, key: &str) -> Result<Value> {
        let answers = self
            .ask(json!({ "op": "get", "step": step, "key": key }))
            .await?;
        if let Some(hit) = answers.iter().find(|a| js::truthy(a.get("found"))) {
            let mut m = Map::new();
            m.insert("found".into(), json!(true));
            for k in ["value", "shard", "position"] {
                if let Some(v) = hit.get(k) {
                    m.insert(k.into(), v.clone());
                }
            }
            return Ok(Value::Object(m));
        }
        let err = answers.iter().find(|a| js::truthy(a.get("error")));
        if let Some(e) = err {
            if !answers.iter().any(|a| !js::truthy(a.get("error"))) {
                return Err(Error::Invalid(format!(
                    "Stream.view: {}",
                    js::string_of(e.get("error"))
                )));
            }
        }
        Ok(json!({ "found": false, "answered": answers.len() }))
    }

    /// Up to `limit` entries of one step, from every shard: `{ items, truncated }`.
    pub async fn list(&self, step: &str, limit: usize) -> Result<Value> {
        let answers = self
            .ask(json!({ "op": "list", "step": step, "limit": limit }))
            .await?;
        let mut items = Vec::new();
        let mut truncated = false;
        for a in &answers {
            if js::truthy(a.get("error")) {
                return Err(Error::Invalid(format!(
                    "Stream.view: {}",
                    js::string_of(a.get("error"))
                )));
            }
            if let Some(Value::Array(xs)) = a.get("items") {
                items.extend(xs.iter().cloned());
            }
            truncated = truncated || js::truthy(a.get("truncated"));
        }
        let more = items.len() > limit;
        items.truncate(limit);
        Ok(json!({ "items": items, "truncated": truncated || more, "answered": answers.len() }))
    }

    /// Every shard's runner: role, position, lag, open state, per step.
    pub async fn status(&self) -> Result<Vec<Value>> {
        self.ask(json!({ "op": "status" })).await
    }

    async fn shard_count(&self) -> u32 {
        if let Some(n) = self.shards.filter(|n| *n > 0) {
            return n;
        }
        let key = format!("{}#layout", self.query_id);
        if let Ok(out) = self
            .queen
            .kv()
            .batch(vec![KvOperation::get(POSITION_NAMESPACE, key)])
            .await
        {
            if let Some(r) = out.results().first() {
                if r.found() {
                    if let Some(n) = r.value.as_ref().and_then(|v| js::int_of(v.get("shards"))) {
                        return n.max(0) as u32;
                    }
                }
            }
        }
        1
    }

    async fn ask(&self, question: Value) -> Result<Vec<Value>> {
        let count = self.shard_count().await;
        let inbox = format!("queen-feed-r.{}", hex(8));
        let id = hex(6);
        let eph = self.queen.ephemeral();
        let mut q = question.as_object().cloned().unwrap_or_default();
        q.insert("id".into(), json!(id));
        q.insert("replyTo".into(), json!(inbox));
        let payload = Value::Object(q);
        let mut sends = Vec::new();
        for i in 0..count {
            let queue = query_queue_name(&self.query_id, if count > 1 { i } else { 0 });
            let eph = eph.clone();
            let payload = payload.clone();
            sends.push(tokio::spawn(async move {
                eph.push(&queue, payload).send().await
            }));
        }
        for s in sends {
            match s.await {
                Ok(r) => {
                    r?;
                }
                Err(e) => return Err(Error::Invalid(format!("Stream.view: {e}"))),
            }
        }
        let mut answers: Vec<Value> = Vec::new();
        let deadline = Instant::now() + Duration::from_millis(self.timeout);
        while answers.len() < count as usize {
            let left = deadline.saturating_duration_since(Instant::now());
            if left.is_zero() {
                break;
            }
            let r = eph
                .pop(&inbox)
                .wait(true)
                .poll_timeout(left.min(Duration::from_secs(30)))
                .batch(count.max(1) as i32)
                .commit_on_delivery(true)
                .send()
                .await?;
            for m in r.messages {
                if m.payload.get("id").and_then(|x| x.as_str()) == Some(id.as_str()) {
                    answers.push(m.payload);
                }
            }
        }
        // The inbox goes away with the broker's own cleanup too; this is a courtesy.
        let eph2 = eph.clone();
        let inbox2 = inbox.clone();
        tokio::spawn(async move {
            let _ = eph2.delete(&inbox2).await;
        });
        if answers.is_empty() {
            return Err(Error::Feed {
                code: queen_protocol::ErrorCode::Other("feed_view_timeout".into()),
                message: format!(
                    "Stream.view: no runner of {} answered within {} ms (is it running, with serveState on?)",
                    self.query_id, self.timeout
                ),
            });
        }
        Ok(answers)
    }
}
