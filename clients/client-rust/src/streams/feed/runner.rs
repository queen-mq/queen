//! The runner of a feed stream (runtime/FeedRunner.js).
//!
//! A cycle reads one page of the feed from the runner's position and runs the
//! chain on each message, in log order. What the cycle produced commits in
//! ONE transaction: the pushes to the sink queues and the new position, a KV
//! key (`queen-feed` / queryId) written with `expect` on its version. Exactly
//! once into Queen; `.foreach` effects run before the position moves, so they
//! are at least once.
//!
//! Two positions, not a copy of the state at every commit: the record holds
//! the committed position and the position of the last SNAPSHOT (the whole
//! state of every step, written now and then in chunks, snapshot.rs). A
//! restart loads the snapshot and reads the feed again from the snapshot's
//! position to the committed one, sending nothing: the feed replays in one
//! order with the times it had, so the state comes back as it was.
//!
//! One runner at a time per queryId: a lock (`queen-feed/<queryId>`), each
//! commit guarded by it; another process with the same queryId waits and
//! takes over. Shards, a poison policy, ticks that move the broker's clock on
//! a quiet feed, live state reads: as in JS, with the same records in KV.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use serde_json::{json, Map, Value};
use tokio::sync::watch;

use crate::error::{Error, Result};
use crate::feed::{
    FeedEntry, FeedFrom, FeedOptions, FeedPage, FeedReader, FeedShard, WhereCondition,
};
use crate::js;
use crate::locks::{Lock, LockOptions};
use crate::streams::hash::sha256_hex;
use crate::transaction::TxOutcome;
use crate::Queen;
use queen_protocol::kv::{Expiry, KvOperation};

use super::batch::BatchItem;
use super::env::{snapshot_of, Env};
use super::ops::{Fn1, FnResult};
use super::pipeline::{Emit, Pipeline, PipelineCp, Release};
use super::query::{query_queue_name, StateServer};
use super::shards::FeedShardGroup;
use super::snapshot::{
    decode_snapshot, delete_keys, layer_keys, layers_of, new_snapshot_id, pointer_of, read_chunks,
    read_layer, stray_chunk_keys, Layer, LayerWriter, PieceBuf, DEFAULT_CHUNK_CHARS,
    DEFAULT_PIECE_BYTES, MAX_LAYERS,
};
use super::state::{shard_of, CLOSE_BATCH};
use super::{CompiledFeed, FeedSink, FeedSource, SinkPartition, Terminal};

/// The KV namespace of the position records (and snapshots, and layouts).
pub const POSITION_NAMESPACE: &str = "queen-feed";
const POSITION_COMMIT_MILLIS: f64 = 5000.0;
const COMMIT_EVERY_MILLIS: f64 = 1000.0;
const MAX_PENDING_OUTPUTS: usize = 5000;
const MAX_BACKOFF_MILLIS: f64 = 10_000.0;
const RETRIES: u32 = 3;
const RETRY_DELAY_MILLIS: f64 = 100.0;
const SNAPSHOT_EVERY_MILLIS: f64 = 60_000.0;
const SNAPSHOT_MAX_ENTRIES: usize = 200_000;
// Skips remembered since the snapshot; past this many, a snapshot is due.
const MAX_SKIPS: usize = 1000;
const REPLAY_CATCH_UP_MILLIS: u64 = 60_000;
const TICK_MIN_MILLIS: f64 = 500.0;
const TICK_READ_WAIT_MILLIS: u64 = 100;
const CLOCK_SYNC_MILLIS: f64 = 5000.0;
const QUERY_ID_MAX_BYTES: usize = 200;
const LOCK_NOT_HELD: &str = "LOCK_NOT_HELD";

fn sha(s: &str) -> String {
    sha256_hex(s)
}

/// The prefix of every transaction id a query writes, every shard of it: its reads leave these out.
pub fn output_prefix(query_id: &str) -> String {
    format!("fd-{}-", &sha(&format!("queen-feed\n{query_id}"))[..8])
}

/// The transaction id of the `i`-th output of one feed entry (the entry in
/// the JS shape: `position`, `queue`, `partition`, `offset`): the same on
/// every retry. `scope` is the shard's id (the queryId, unsharded).
pub fn feed_output_id(query_id: &str, entry: &Value, i: u64, epoch: i64, scope: &str) -> String {
    let s = |k: &str| js::string_of(entry.get(k));
    let h = sha(&format!(
        "{scope}\n{epoch}\n{}\n{}\n{}\n{}\n{i}",
        s("position"),
        s("queue"),
        s("partition"),
        s("offset")
    ));
    format!("{}{}", output_prefix(query_id), &h[..40])
}

/// The KV key of one shard of a query (or of the query, unsharded).
pub fn shard_id(query_id: &str, shard: Option<(u32, u32)>) -> String {
    match shard {
        Some((index, count)) => format!("{query_id}#shard{index}of{count}"),
        None => query_id.to_string(),
    }
}

fn lock_name(query_id: &str, shard: Option<(u32, u32)>) -> String {
    match shard {
        Some((index, count)) => format!("queen-feed/{query_id}/shard-{index}-of-{count}"),
        None => format!("queen-feed/{query_id}"),
    }
}

// ----------------------------------------------------------------- options

/// `reset`: start over at every start (`Always`), or once per token.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Reset {
    #[default]
    No,
    Always,
    Token(String),
}

/// What a message (or a result) whose function keeps failing does.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum Poison {
    /// Commit what came before it and stop.
    #[default]
    Stop,
    /// Leave it out.
    Skip,
    /// Push it, with the error, to this queue.
    DeadLetter(String),
}

/// How shards split the stream.
#[derive(Clone, Default)]
pub enum ShardBy {
    /// By partition name (the broker filters).
    #[default]
    Partition,
    /// By `String(f(msg))` (filtered here).
    Key(Fn1),
}

impl ShardBy {
    pub fn key<F, R>(f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: super::IntoFeedValue,
    {
        Self::Key(super::ops::fn1(f))
    }
}

/// What the `on_error` hook hears: the error, and its context
/// (`queryId`, `shard`, `phase`, `position`, `action`, `attempts`, ...).
#[derive(Debug, Clone)]
pub struct FeedErrorInfo {
    pub message: String,
    pub code: Option<String>,
    pub ctx: Value,
}

pub(crate) type ErrorHook = Arc<dyn Fn(&FeedErrorInfo) + Send + Sync>;
pub(crate) type NowFn = Arc<dyn Fn() -> f64 + Send + Sync>;

/// The options of a feed stream's `run` (the JS `run({ queryId, ... })`),
/// with the same defaults.
#[derive(Clone)]
pub struct FeedRunOptions {
    pub query_id: String,
    /// Where a NEW query starts (over the source's).
    pub from: Option<FeedFrom>,
    pub wait_millis: u64,
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
    pub reset: Reset,
    pub on_error: Option<ErrorHook>,
    pub position_commit_millis: f64,
    pub commit_every_millis: f64,
    pub max_pending_outputs: usize,
    pub exclusive: bool,
    /// The lock's lifetime, as given (`"15s"` by default; a number is seconds).
    pub lock_ttl: super::FeedDuration,
    pub on_poison: Poison,
    pub retries: u32,
    pub retry_delay_millis: f64,
    pub snapshot_every_millis: f64,
    pub snapshot_max_entries: usize,
    pub snapshot_chunk_chars: usize,
    pub snapshot_piece_bytes: usize,
    /// One slice: `(index, count)`.
    pub shard: Option<(u32, u32)>,
    /// All the slices, in this process.
    pub shards: Option<u32>,
    pub shard_by: ShardBy,
    pub serve_state: bool,
    pub serve_wait_millis: Option<u64>,
    /// The clock (epoch ms) the runner times its commits and snapshots by.
    pub now: Option<NowFn>,
}

impl FeedRunOptions {
    pub fn new(query_id: impl Into<String>) -> Self {
        Self {
            query_id: query_id.into(),
            from: None,
            wait_millis: 5000,
            max_records: None,
            max_bytes: None,
            reset: Reset::No,
            on_error: None,
            position_commit_millis: POSITION_COMMIT_MILLIS,
            commit_every_millis: COMMIT_EVERY_MILLIS,
            max_pending_outputs: MAX_PENDING_OUTPUTS,
            exclusive: true,
            lock_ttl: super::FeedDuration::Text("15s".into()),
            on_poison: Poison::Stop,
            retries: RETRIES,
            retry_delay_millis: RETRY_DELAY_MILLIS,
            snapshot_every_millis: SNAPSHOT_EVERY_MILLIS,
            snapshot_max_entries: SNAPSHOT_MAX_ENTRIES,
            snapshot_chunk_chars: DEFAULT_CHUNK_CHARS,
            snapshot_piece_bytes: DEFAULT_PIECE_BYTES,
            shard: None,
            shards: None,
            shard_by: ShardBy::Partition,
            serve_state: true,
            serve_wait_millis: None,
            now: None,
        }
    }

    pub fn from(mut self, from: FeedFrom) -> Self {
        self.from = Some(from);
        self
    }
    pub fn wait_millis(mut self, ms: u64) -> Self {
        self.wait_millis = ms;
        self
    }
    pub fn max_records(mut self, n: u64) -> Self {
        self.max_records = Some(n);
        self
    }
    pub fn reset(mut self, r: Reset) -> Self {
        self.reset = r;
        self
    }
    pub fn on_error<F: Fn(&FeedErrorInfo) + Send + Sync + 'static>(mut self, f: F) -> Self {
        self.on_error = Some(Arc::new(f));
        self
    }
    pub fn commit_every_millis(mut self, ms: f64) -> Self {
        self.commit_every_millis = ms;
        self
    }
    pub fn position_commit_millis(mut self, ms: f64) -> Self {
        self.position_commit_millis = ms;
        self
    }
    pub fn max_pending_outputs(mut self, n: usize) -> Self {
        self.max_pending_outputs = n;
        self
    }
    pub fn exclusive(mut self, on: bool) -> Self {
        self.exclusive = on;
        self
    }
    /// `"15s"`, `"1m"`, or a number of seconds.
    pub fn lock_ttl(mut self, ttl: impl Into<super::FeedDuration>) -> Self {
        self.lock_ttl = ttl.into();
        self
    }
    pub fn on_poison(mut self, p: Poison) -> Self {
        self.on_poison = p;
        self
    }
    pub fn retries(mut self, n: u32) -> Self {
        self.retries = n;
        self
    }
    pub fn retry_delay_millis(mut self, ms: f64) -> Self {
        self.retry_delay_millis = ms;
        self
    }
    pub fn snapshot_every_millis(mut self, ms: f64) -> Self {
        self.snapshot_every_millis = ms;
        self
    }
    pub fn snapshot_max_entries(mut self, n: usize) -> Self {
        self.snapshot_max_entries = n;
        self
    }
    pub fn snapshot_chunk_chars(mut self, n: usize) -> Self {
        self.snapshot_chunk_chars = n;
        self
    }
    pub fn snapshot_piece_bytes(mut self, n: usize) -> Self {
        self.snapshot_piece_bytes = n;
        self
    }
    pub fn shard(mut self, index: u32, count: u32) -> Self {
        self.shard = Some((index, count));
        self
    }
    pub fn shards(mut self, n: u32) -> Self {
        self.shards = Some(n);
        self
    }
    pub fn shard_by(mut self, by: ShardBy) -> Self {
        self.shard_by = by;
        self
    }
    pub fn serve_state(mut self, on: bool) -> Self {
        self.serve_state = on;
        self
    }
    pub fn serve_wait_millis(mut self, ms: u64) -> Self {
        self.serve_wait_millis = Some(ms);
        self
    }
    pub fn now<F: Fn() -> f64 + Send + Sync + 'static>(mut self, f: F) -> Self {
        self.now = Some(Arc::new(f));
        self
    }
}

// ------------------------------------------------------------------ errors

/// An error inside the runner, with the properties the JS runner branches on.
#[derive(Debug, Clone, Default)]
pub(crate) struct FeedErr {
    pub message: String,
    pub code: Option<String>,
    pub fatal: bool,
    pub aborted: bool,
    pub stopping: bool,
    pub reason: Option<String>,
}

impl FeedErr {
    pub fn msg(m: impl Into<String>) -> Self {
        Self {
            message: m.into(),
            ..Default::default()
        }
    }
    pub fn fatal(m: impl Into<String>, code: &str) -> Self {
        Self {
            message: m.into(),
            code: Some(code.into()),
            fatal: true,
            ..Default::default()
        }
    }
    pub fn coded(m: impl Into<String>, code: &str) -> Self {
        Self {
            message: m.into(),
            code: Some(code.into()),
            ..Default::default()
        }
    }
    pub fn lock_not_held(name: &str) -> Self {
        Self::coded(
            format!("lock '{name}' is not held; there is nothing to guard with"),
            LOCK_NOT_HELD,
        )
    }
    fn aborted() -> Self {
        Self {
            message: "the read was cancelled by stop()".into(),
            aborted: true,
            ..Default::default()
        }
    }
    fn stopping() -> Self {
        Self {
            message: "stopping".into(),
            stopping: true,
            ..Default::default()
        }
    }
    fn no_feed_route(&self) -> bool {
        self.code.as_deref() == Some(crate::feed::NO_FEED_ROUTE)
    }
    fn into_error(self) -> Error {
        match self.code {
            Some(c) => Error::Feed {
                code: queen_protocol::ErrorCode::Other(c),
                message: self.message,
            },
            None => Error::Invalid(self.message),
        }
    }
}

impl From<Error> for FeedErr {
    fn from(e: Error) -> Self {
        Self {
            message: match &e {
                Error::Feed { message, .. } => message.clone(),
                other => other.to_string(),
            },
            code: e.code().map(|c| c.as_str().to_string()),
            ..Default::default()
        }
    }
}

impl From<String> for FeedErr {
    fn from(m: String) -> Self {
        Self::msg(m)
    }
}

// ------------------------------------------------------------------ shared

#[derive(Clone)]
struct Pending {
    outputs: Vec<Output>,
    next: Option<u64>,
    work: u64,
    effects: u64,
    emit_seq: u64,
    // The feed time `next` is at, and whether the read was caught up there.
    read_to: Option<f64>,
    caught_up: Option<bool>,
    // The results a .foreach_batch() keeps, and when the first of them came.
    batch: Vec<PendingItem>,
    batch_since: Option<f64>,
}

fn fresh_pending() -> Pending {
    Pending {
        outputs: Vec::new(),
        next: None,
        work: 0,
        effects: 0,
        emit_seq: 0,
        read_to: None,
        caught_up: None,
        batch: Vec::new(),
        batch_since: None,
    }
}

/// A result a `.foreach_batch()` keeps until its batch goes: what the
/// function gets, the unit it came from (for reports), and the partition a
/// dead letter of it goes to.
#[derive(Clone)]
struct PendingItem {
    value: Option<Value>,
    ctx: Value,
    id: String,
    subject: Option<Arc<ItemSubject>>,
    partition: String,
}

impl PendingItem {
    fn item(&self) -> BatchItem {
        BatchItem {
            value: self.value.clone(),
            ctx: self.ctx.clone(),
            id: self.id.clone(),
        }
    }
}

/// A unit for a report, kept by the batch items it made: a feed message, or
/// a step's result (with the message an event-time step held, if any).
enum ItemSubject {
    Entry(Arc<Value>),
    Emit {
        step: usize,
        kind: Value,
        key: String,
        entry: Option<Arc<Value>>,
    },
}

/// `#describe`: what a report says of a unit (`what`), and where it was.
fn describe(
    entry: Option<&Value>,
    emit: Option<(usize, &Value, &str)>,
) -> (Map<String, Value>, String) {
    let mut what = Map::new();
    if let Some(e) = entry {
        for k in ["position", "queue", "partition", "offset", "transactionId"] {
            if let Some(v) = e.get(k) {
                what.insert(k.into(), v.clone());
            }
        }
        let where_ = format!(
            "the message at position {} ({}/{}, offset {})",
            js::string_of(e.get("position")),
            js::string_of(e.get("queue")),
            js::string_of(e.get("partition")),
            js::string_of(e.get("offset"))
        );
        return (what, where_);
    }
    let Some((step, kind, key)) = emit else {
        return (what, "a result".into());
    };
    what.insert("step".into(), json!(step));
    what.insert("result".into(), kind.clone());
    what.insert("key".into(), json!(key));
    (
        what,
        format!(
            "the {} result for key {}",
            js::string_of(Some(kind)),
            js::string_json(key)
        ),
    )
}

fn describe_item(subject: Option<&ItemSubject>) -> (Map<String, Value>, String) {
    match subject {
        None => describe(None, None),
        Some(ItemSubject::Entry(e)) => describe(Some(e), None),
        Some(ItemSubject::Emit {
            step,
            kind,
            key,
            entry,
        }) => describe(entry.as_deref(), Some((*step, kind, key))),
    }
}

/// The error a `stopped` record keeps, at most.
const STOP_ERROR_MAX_CHARS: usize = 500;

/// A text cut at `max` UTF-16 units, with `…` (JS `clipText`).
fn clip_text(s: &str, max: usize) -> String {
    if js::utf16_len(s) <= max {
        return s.to_string();
    }
    let units: Vec<u16> = s.encode_utf16().take(max).collect();
    format!("{}…", String::from_utf16_lossy(&units))
}

#[derive(Clone)]
struct Output {
    queue: String,
    partition: String,
    data: Value,
    transaction_id: String,
}

/// The counters of `metrics()`, by their JS names.
struct Counters {
    m: Map<String, Value>,
    pending_outputs: usize,
    pending_batch: usize,
    clock: f64,
    units: u64,
    snap_position: Value,
    reader_position: Value,
    committed: Value,
}

const COUNTERS: [&str; 34] = [
    "cycles",
    "entriesRead",
    "emitted",
    "outputs",
    "effects",
    "batches",
    "commits",
    "conflicts",
    "errors",
    "retries",
    "poisoned",
    "skipped",
    "deadLettered",
    "late",
    "ticks",
    "takeovers",
    "snapshots",
    "snapshotBytes",
    "snapshotChunks",
    "snapshotsFull",
    "snapshotLayers",
    "snapshotMillis",
    "units",
    "closeCommits",
    "replays",
    "replayEntries",
    "replayMillis",
    "replaySkipped",
    "tableRows",
    "position",
    "applied",
    "lag",
    "stopped",
    "role",
];

impl Counters {
    fn new() -> Self {
        let mut m = Map::new();
        m.insert("role".into(), json!("starting"));
        for k in COUNTERS {
            if !m.contains_key(k) {
                m.insert(
                    k.into(),
                    if matches!(k, "position" | "applied" | "lag" | "stopped") {
                        Value::Null
                    } else {
                        json!(0)
                    },
                );
            }
        }
        Self {
            m,
            pending_outputs: 0,
            pending_batch: 0,
            clock: 0.0,
            units: 0,
            snap_position: Value::Null,
            reader_position: Value::Null,
            committed: Value::Null,
        }
    }
    fn add(&mut self, k: &str, n: u64) {
        let cur = self.m.get(k).and_then(|v| v.as_u64()).unwrap_or(0);
        self.m.insert(k.to_string(), json!(cur + n));
    }
    fn set(&mut self, k: &str, v: Value) {
        self.m.insert(k.to_string(), v);
    }
    fn get_u64(&self, k: &str) -> u64 {
        self.m.get(k).and_then(|v| v.as_u64()).unwrap_or(0)
    }
    fn role(&self) -> String {
        js::string_of(self.m.get("role"))
    }
}

pub(crate) struct Shared {
    pub query_id: String,
    pub id: String,
    pub shard: Option<(u32, u32)>,
    pipeline: Mutex<Pipeline>,
    counters: Mutex<Counters>,
    stopping: AtomicBool,
    // stop() asked for the stop (the last commit's record then says so).
    stop_asked: AtomicBool,
    stop_tx: watch::Sender<bool>,
    lock_lost: AtomicBool,
    active: AtomicBool,
    server: Mutex<Option<StateServer>>,
    done_tx: watch::Sender<bool>,
}

fn lock_ok<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

impl Shared {
    fn p(&self) -> MutexGuard<'_, Pipeline> {
        lock_ok(&self.pipeline)
    }
    fn c(&self) -> MutexGuard<'_, Counters> {
        lock_ok(&self.counters)
    }
    fn set_role(&self, role: &str) {
        self.c().set("role", json!(role));
    }
    fn stop_server(&self) {
        if let Some(s) = lock_ok(&self.server).take() {
            s.stop();
        }
    }

    /// `metrics()`: the counters, by their JS names.
    pub fn metrics(&self) -> Value {
        let mut p = self.p();
        let c = self.c();
        let mut m = c.m.clone();
        m.insert("pendingOutputs".into(), json!(c.pending_outputs));
        m.insert("pendingBatch".into(), json!(c.pending_batch));
        if let Some((index, count)) = self.shard {
            m.insert("shard".into(), json!({ "index": index, "count": count }));
        }
        if p.has_state() {
            let clock = c.clock;
            m.insert("open".into(), json!(p.open()));
            m.insert("stateKeys".into(), json!(p.state_keys()));
            m.insert("steps".into(), p.step_metrics(clock));
            m.insert("late".into(), json!(p.late));
            m.insert(
                "clock".into(),
                if clock > 0.0 {
                    json!(js::iso(clock))
                } else {
                    Value::Null
                },
            );
            if let Some(w) = p.watermark(clock) {
                m.insert("watermark".into(), json!(js::iso(w)));
            }
            m.insert("snapshotPosition".into(), c.snap_position.clone());
            m.insert("units".into(), json!(c.units));
        }
        Value::Object(m)
    }

    /// A question from outside (query.rs), answered from the live state.
    pub fn answer(&self, req: &Value) -> Value {
        let (role, position, committed) = {
            let c = self.c();
            (c.role(), c.reader_position.clone(), c.committed.clone())
        };
        let mut base = Map::new();
        base.insert("shard".into(), json!(self.shard.map_or(0, |s| s.0)));
        base.insert("shards".into(), json!(self.shard.map_or(1, |s| s.1)));
        base.insert("role".into(), json!(role));
        base.insert("position".into(), position);
        base.insert("committed".into(), committed);
        let op = req.get("op").and_then(|o| o.as_str()).unwrap_or("");
        let merge = |base: &mut Map<String, Value>, v: Value| {
            if let Value::Object(o) = v {
                for (k, x) in o {
                    base.insert(k, x);
                }
            }
        };
        if op == "status" {
            base.insert("metrics".into(), self.metrics());
            return Value::Object(base);
        }
        let p = self.p();
        if !p.has_state() {
            base.insert("error".into(), json!("this stream keeps no state"));
            return Value::Object(base);
        }
        let step = js::string_of(req.get("step"));
        match op {
            "get" => {
                let key = js::string_of(req.get("key"));
                merge(&mut base, p.inspect(&step, &key));
            }
            "list" => {
                let n = js::as_num(req.get("limit"))
                    .filter(|x| *x != 0.0)
                    .or_else(|| {
                        req.get("limit")
                            .and_then(|v| v.as_str())
                            .and_then(|s| s.trim().parse::<f64>().ok())
                            .filter(|x| *x != 0.0)
                    })
                    .unwrap_or(100.0);
                let limit = n.clamp(1.0, 1000.0) as usize;
                merge(&mut base, p.list(&step, limit));
            }
            _ => {
                base.insert(
                    "error".into(),
                    json!(format!(
                        "unknown question {} (get, list, status)",
                        js::stringify(req.get("op").unwrap_or(&Value::Null))
                    )),
                );
            }
        }
        Value::Object(base)
    }
}

// ------------------------------------------------------------------ handle

/// One feed runner: `stop()`, `done()`, `metrics()`, `state(step)`.
#[derive(Clone)]
pub struct FeedRunner {
    sh: Arc<Shared>,
}

/// Live reads of one step, in this process.
pub struct FeedStateView {
    steps: Vec<Arc<Shared>>,
    step: String,
}

impl FeedStateView {
    pub(crate) fn over(steps: Vec<Arc<Shared>>, step: &str) -> Self {
        Self {
            steps,
            step: step.to_string(),
        }
    }

    /// `{ found, value? }` (or `{ found: false, error }` for a step that is not there).
    pub fn get(&self, key: &str) -> Value {
        if self.steps.len() == 1 {
            return self.steps[0].p().inspect(&self.step, key);
        }
        for sh in &self.steps {
            let v = sh.p().inspect(&self.step, key);
            if js::truthy(v.get("found")) || v.get("error").is_some() {
                let mut o = v.as_object().cloned().unwrap_or_default();
                o.insert("shard".into(), json!(sh.shard.map_or(0, |s| s.0)));
                return Value::Object(o);
            }
        }
        json!({ "found": false })
    }

    /// `{ items, truncated, total }`, up to `limit` (default 100).
    pub fn list(&self, limit: usize) -> Value {
        if self.steps.len() == 1 {
            return self.steps[0].p().list(&self.step, limit);
        }
        let mut items = Vec::new();
        let mut truncated = false;
        for sh in &self.steps {
            let part = sh.p().list(&self.step, limit);
            if part.get("error").is_some() {
                return part;
            }
            if let Some(Value::Array(xs)) = part.get("items") {
                items.extend(xs.iter().cloned());
            }
            truncated = truncated || js::truthy(part.get("truncated"));
        }
        let more = items.len() > limit;
        items.truncate(limit);
        json!({ "items": items, "truncated": truncated || more })
    }
}

impl FeedRunner {
    /// Stop after the cycle in hand, commit what is pending (and a
    /// snapshot), give the lock back; resolves once done.
    pub async fn stop(&self) {
        self.sh.stop_asked.store(true, Ordering::SeqCst);
        self.sh.stopping.store(true, Ordering::SeqCst);
        self.sh.stop_tx.send_replace(true);
        self.done().await;
        self.sh.stop_server();
    }

    /// Resolves when the runner has stopped: by `stop()`, a poison message,
    /// a broker without the feed, or a fatal error.
    pub async fn done(&self) {
        let mut rx = self.sh.done_tx.subscribe();
        let _ = rx.wait_for(|d| *d).await;
    }

    pub fn is_done(&self) -> bool {
        *self.sh.done_tx.borrow()
    }

    /// The counters, by their JS names (`role`, `emitted`, `outputs`, ...).
    pub fn metrics(&self) -> Value {
        self.sh.metrics()
    }

    /// This runner's shard, `(index, count)`.
    pub fn shard(&self) -> Option<(u32, u32)> {
        self.sh.shard
    }

    /// The live state of one step, in this process.
    pub fn state(&self, step: &str) -> FeedStateView {
        FeedStateView {
            steps: vec![Arc::clone(&self.sh)],
            step: step.to_string(),
        }
    }

    pub(crate) fn shared(&self) -> Arc<Shared> {
        Arc::clone(&self.sh)
    }

    pub(crate) async fn start(compiled: CompiledFeed, opts: FeedRunOptions) -> Result<FeedRunner> {
        let core = Core::new(compiled, opts).map_err(FeedErr::into_error)?;
        let sh = Arc::clone(&core.sh);
        let core = core.start().await.map_err(FeedErr::into_error)?;
        tokio::spawn(core.run());
        Ok(FeedRunner { sh })
    }
}

impl std::fmt::Debug for FeedRunner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FeedRunner")
            .field("id", &self.sh.id)
            .field("metrics", &self.metrics())
            .finish()
    }
}

/// What `run` started: one runner, or the shards of one stream.
#[derive(Debug)]
pub enum FeedHandle {
    Runner(FeedRunner),
    Group(FeedShardGroup),
}

impl FeedHandle {
    pub async fn stop(&self) {
        match self {
            Self::Runner(r) => r.stop().await,
            Self::Group(g) => g.stop().await,
        }
    }

    pub async fn done(&self) {
        match self {
            Self::Runner(r) => r.done().await,
            Self::Group(g) => g.done().await,
        }
    }

    pub fn metrics(&self) -> Value {
        match self {
            Self::Runner(r) => r.metrics(),
            Self::Group(g) => g.metrics(),
        }
    }

    pub fn state(&self, step: &str) -> FeedStateView {
        match self {
            Self::Runner(r) => r.state(step),
            Self::Group(g) => g.state(step),
        }
    }
}

pub(crate) async fn run(compiled: CompiledFeed, opts: FeedRunOptions) -> Result<FeedHandle> {
    if let Some(n) = opts.shards {
        if n != 1 {
            return Ok(FeedHandle::Group(
                FeedShardGroup::start(compiled, opts).await?,
            ));
        }
    }
    let mut opts = opts;
    opts.shards = None;
    Ok(FeedHandle::Runner(FeedRunner::start(compiled, opts).await?))
}

// -------------------------------------------------------------------- core

/// A pattern of queue names (`*`, `?`), as the JS `glob`.
fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let (mut star, mut mark) = (None::<usize>, 0usize);
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' && t[ti] != '\n' || p[pi] == t[ti] && p[pi] != '*') {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ti;
            pi += 1;
        } else if let Some(s) = star {
            if t[mark] == '\n' {
                return false;
            }
            pi = s + 1;
            mark += 1;
            ti = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

fn ts_of(entry: &FeedEntry) -> f64 {
    js::parse_date(&entry.ts).unwrap_or(0.0)
}

struct Group {
    position: u64,
    entries: Vec<FeedEntry>,
}

/// The entries of a page by position: one position is one transaction.
fn groups(entries: &[FeedEntry], below: Option<u64>) -> Vec<Group> {
    let mut out: Vec<Group> = Vec::new();
    for e in entries {
        if below.is_some_and(|b| e.position >= b) {
            break;
        }
        match out.last_mut() {
            Some(g) if g.position == e.position => g.entries.push(e.clone()),
            _ => out.push(Group {
                position: e.position,
                entries: vec![e.clone()],
            }),
        }
    }
    out
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Flow {
    Ok,
    Stop,
    Abandon,
    // An item of a batch stopped the stream (rolled back to the last commit already).
    Halt,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Route {
    Table,
    Stream,
    Skip,
}

enum Subject<'a> {
    Entry(&'a FeedEntry),
    Emit(&'a Emit),
}

/// What a unit runs: a message, a table row, a close's result, a held message.
enum Work<'a> {
    Row(&'a FeedEntry),
    Entry(&'a FeedEntry, f64),
    Emit(usize, &'a Emit, f64),
    Release(usize, &'a Release, f64),
}

struct PoisonStop {
    where_: String,
    what: Map<String, Value>,
    attempts: u32,
    reason: String,
    // An item of a batch, not a message.
    batch: bool,
}

/// The snapshot a commit names.
struct SnapFor {
    pointer: Value,
    wrote: bool,
    moved: bool,
    full: bool,
    replaced: Vec<Layer>,
    keys: Vec<String>,
    millis: u64,
}

struct Core {
    sh: Arc<Shared>,
    queen: Queen,
    o: FeedRunOptions,
    terminal: Option<Terminal>,
    hash: String,
    source: FeedSource,
    pushdown: Vec<WhereCondition>,
    reader: Option<FeedReader>,
    broker_queues: Option<Vec<String>>,
    broker_partitions: Option<Vec<String>>,
    broker_where: Vec<WhereCondition>,
    broker_shard: Option<FeedShard>,
    local_where: Vec<WhereCondition>,
    local_partitions: Option<HashSet<String>>,
    stream_queues: Option<Vec<String>>,
    local_shard: bool,
    table_queues: Vec<String>,
    lock: Option<Lock>,
    lock_acquired: Arc<tokio::sync::Notify>,
    position: FeedFrom,
    version: i64,
    epoch: i64,
    reset_token: Option<String>,
    committed_clock: f64,
    units: u64,
    committed_units: u64,
    replay_cap: Option<u64>,
    burst_target: Option<f64>,
    snap: Option<Value>,
    entries_since_snap: usize,
    snap_wall_at: f64,
    old_keys: Vec<String>,
    had_stray: bool,
    skips: Vec<String>,
    pending_skips: Vec<String>,
    skip_set: HashSet<String>,
    layout_version: i64,
    initial_snap: bool,
    pending: Pending,
    clock: f64,
    replaying: bool,
    poison_stop: Option<PoisonStop>,
    last_commit_at: f64,
    last_position_commit: f64,
    last_tick_at: f64,
    resolved_ms: f64,
    resolved_at: f64,
    offset: f64,
    last_now: f64,
    next_wait: Option<u64>,
    last_said: HashMap<String, Instant>,
    // The feed time the committed position is at, and whether it was read caught up.
    committed_read_to: f64,
    committed_caught_up: bool,
    // Why the stream stops, for the record of its last commit: `{ reason, at, ... }`.
    stop_mark: Option<Value>,
    // What the stream is (about.rs; `None`: not made yet), and whether the
    // next commit writes it: the first commit of each activation does.
    about: Option<Option<Value>>,
    about_owed: bool,
    steps: Vec<super::pipeline::PlanStep>,
    // The unit running now (with a .foreach_batch()): what a batch item came from.
    subject: Option<Arc<ItemSubject>>,
}

type LoadFuture<'a> = std::pin::Pin<
    Box<dyn std::future::Future<Output = std::result::Result<(), FeedErr>> + Send + 'a>,
>;

/// Counted closes and the checkpoint a stop comes back to.
struct Cp {
    state: PipelineCp,
    outputs: usize,
    work: u64,
    effects: u64,
    emit_seq: u64,
    clock: f64,
    units: u64,
    late: u64,
    skips: usize,
    batch: usize,
    batch_since: Option<f64>,
    // A unit tried again counts its results once.
    emitted: u64,
}

fn require_query_id(q: &str) -> std::result::Result<(), FeedErr> {
    let bad = q.is_empty()
        || q.len() > QUERY_ID_MAX_BYTES
        || q.chars()
            .any(|c| (c as u32) < 0x20 || c == '\u{7f}' || c == '#');
    if bad {
        return Err(FeedErr::msg(format!(
            "Stream.fromFeed: queryId is a non-empty string of at most {QUERY_ID_MAX_BYTES} bytes, without '#' \
             and control characters (it names a KV key and a lock), got {}",
            js::string_json(q)
        )));
    }
    Ok(())
}

impl Core {
    fn new(compiled: CompiledFeed, o: FeedRunOptions) -> std::result::Result<Core, FeedErr> {
        require_query_id(&o.query_id)?;
        let shard = match o.shard {
            None => None,
            Some((index, count)) => {
                if !(1..=4096).contains(&count) || index >= count {
                    return Err(FeedErr::msg(format!(
                        "Stream.fromFeed: shard is {{ index, count }}, count from 1 to 4096 and index from 0 to count - 1, got {}",
                        js::stringify(&json!({ "index": index, "count": count }))
                    )));
                }
                (count != 1).then_some((index, count))
            }
        };
        if let Reset::Token(t) = &o.reset {
            if t.is_empty() {
                return Err(FeedErr::msg(
                    "Stream.fromFeed: reset is true (start over at every start) or a string (start over once per string)",
                ));
            }
        }
        if let Poison::DeadLetter(q) = &o.on_poison {
            if q.is_empty() {
                return Err(FeedErr::msg(
                    "Stream.fromFeed: onPoison is 'stop', 'skip' or a queue for the messages that fail (queen.queue('feed-dlq'))",
                ));
            }
        }
        if compiled.terminal.is_none() {
            return Err(FeedErr::msg(
                "Stream.fromFeed: end the chain with .to(queen.queue(...)), .foreach(fn) or .foreachBatch(fn)",
            ));
        }
        let id = shard_id(&o.query_id, shard);
        // The chain's clocks may refuse it (idle on the feed's clock, ...).
        let pipeline = Pipeline::new(&compiled.steps).map_err(FeedErr::msg)?;
        let steps_plan = compiled.steps.clone();
        let (stop_tx, _) = watch::channel(false);
        let (done_tx, _) = watch::channel(false);
        let sh = Arc::new(Shared {
            query_id: o.query_id.clone(),
            id,
            shard,
            pipeline: Mutex::new(pipeline),
            counters: Mutex::new(Counters::new()),
            stopping: AtomicBool::new(false),
            stop_asked: AtomicBool::new(false),
            stop_tx,
            lock_lost: AtomicBool::new(false),
            active: AtomicBool::new(false),
            server: Mutex::new(None),
            done_tx,
        });
        Ok(Core {
            sh,
            queen: compiled.queen,
            terminal: compiled.terminal,
            hash: compiled.chain_hash,
            source: compiled.source,
            pushdown: compiled.pushdown,
            o,
            reader: None,
            broker_queues: None,
            broker_partitions: None,
            broker_where: Vec::new(),
            broker_shard: None,
            local_where: Vec::new(),
            local_partitions: None,
            stream_queues: None,
            local_shard: false,
            table_queues: Vec::new(),
            lock: None,
            lock_acquired: Arc::new(tokio::sync::Notify::new()),
            position: FeedFrom::Now,
            version: 0,
            epoch: 0,
            reset_token: None,
            committed_clock: 0.0,
            units: 0,
            committed_units: 0,
            replay_cap: None,
            burst_target: None,
            snap: None,
            entries_since_snap: 0,
            snap_wall_at: 0.0,
            old_keys: Vec::new(),
            had_stray: false,
            skips: Vec::new(),
            pending_skips: Vec::new(),
            skip_set: HashSet::new(),
            layout_version: 0,
            initial_snap: false,
            pending: fresh_pending(),
            clock: 0.0,
            replaying: false,
            poison_stop: None,
            last_commit_at: 0.0,
            last_position_commit: 0.0,
            last_tick_at: 0.0,
            resolved_ms: 0.0,
            resolved_at: 0.0,
            offset: f64::NEG_INFINITY,
            last_now: 0.0,
            next_wait: None,
            last_said: HashMap::new(),
            committed_read_to: 0.0,
            committed_caught_up: false,
            stop_mark: None,
            about: None,
            about_owed: false,
            steps: steps_plan,
            subject: None,
        })
    }

    fn now(&self) -> f64 {
        match &self.o.now {
            Some(f) => f(),
            None => js::now_ms(),
        }
    }

    fn p(&self) -> MutexGuard<'_, Pipeline> {
        self.sh.p()
    }

    fn stopping(&self) -> bool {
        self.sh.stopping.load(Ordering::SeqCst)
    }

    fn set_stopping(&self) {
        self.sh.stopping.store(true, Ordering::SeqCst);
    }

    fn active(&self) -> bool {
        self.sh.active.load(Ordering::SeqCst)
    }

    fn set_active(&self, on: bool) {
        self.sh.active.store(on, Ordering::SeqCst);
    }

    fn count(&self, k: &str, n: u64) {
        self.sh.c().add(k, n);
    }

    /// Mirror what `metrics()` and the live answers read of the loop's own state.
    fn publish(&self) {
        let snap_position = self
            .snap
            .as_ref()
            .and_then(|s| s.get("position"))
            .cloned()
            .unwrap_or(Value::Null);
        let reader_position = match &self.reader {
            Some(r) => r.position().to_value(),
            None => self.position.to_value(),
        };
        let mut c = self.sh.c();
        c.pending_outputs = self.pending.outputs.len();
        c.pending_batch = self.pending.batch.len();
        c.clock = self.clock;
        c.units = self.units;
        c.snap_position = snap_position;
        c.reader_position = reader_position;
        c.committed = self.position.to_value();
    }

    async fn wait_stop(&self) {
        let mut rx = self.sh.stop_tx.subscribe();
        let _ = rx.wait_for(|s| *s).await;
    }

    async fn sleep(&self, ms: f64) {
        let d = Duration::from_millis(ms.max(0.0) as u64);
        tokio::select! {
            _ = tokio::time::sleep(d) => {}
            _ = self.wait_stop() => {}
        }
    }

    // ---- start ---------------------------------------------------------------

    /// Check the chain, take the lock if it is free, load the position and state.
    async fn start(mut self) -> std::result::Result<Core, FeedErr> {
        self.plan_reads();
        let mut reader = self
            .make_reader(FeedFrom::Now, None, false)
            .map_err(FeedErr::from)?;
        // A broker without the feed (or a feed nobody can serve) fails run()
        // here, with the reason, instead of a stream that silently never runs.
        reader.probe().await.map_err(FeedErr::from)?;
        self.reader = Some(reader);
        self.check_layout().await?;
        if self.o.exclusive {
            let ttl_ms = match &self.o.lock_ttl {
                super::FeedDuration::Seconds(s) => s * 1000.0,
                super::FeedDuration::Text(t) => js::parse_duration_ms(t).map_err(FeedErr::msg)?,
            };
            let ttl = Duration::from_millis(ttl_ms.max(1.0) as u64);
            let lock = self.queen.lock_with(
                lock_name(&self.sh.query_id, self.sh.shard),
                LockOptions::new(ttl)
                    .retry(Duration::from_millis(250), Duration::from_millis(2000)),
            );
            self.watch_lock(lock.clone());
            self.lock = Some(lock);
        }
        // Active now when the lock is free, so that a chain the stored state was
        // not written by fails run() itself; otherwise a standby, waiting.
        let free = match &self.lock {
            None => true,
            Some(l) => l.acquire().await.map_err(FeedErr::from)?,
        };
        if free {
            if let Err(e) = self.become_active(true).await {
                if let Some(l) = &self.lock {
                    let _ = l.release().await;
                }
                self.sh.stop_tx.send_replace(true);
                return Err(e);
            }
        } else {
            self.sh.set_role("standby");
        }
        self.publish();
        tracing::debug!(query_id = %self.sh.id, role = %self.sh.c().role(), "FeedRunner.start");
        Ok(self)
    }

    /// The lock's on-lost hook: a standby from now on, and no more live answers.
    fn watch_lock(&self, lock: Lock) {
        let sh = Arc::clone(&self.sh);
        let acquired = Arc::clone(&self.lock_acquired);
        let mut stop = self.sh.stop_tx.subscribe();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = lock.lost() => {}
                    _ = stop.wait_for(|s| *s) => return,
                }
                sh.lock_lost.store(true, Ordering::SeqCst);
                if sh.active.load(Ordering::SeqCst) && !sh.stopping.load(Ordering::SeqCst) {
                    sh.set_role("standby");
                }
                sh.stop_server();
                tracing::warn!(query_id = %sh.id, "FeedRunner.lockLost");
                // Until the lock is taken again.
                loop {
                    tokio::select! {
                        _ = acquired.notified() => {}
                        _ = stop.wait_for(|s| *s) => return,
                    }
                    if !lock.is_lost() {
                        break;
                    }
                }
            }
        });
    }

    /// What the broker filters and what this process filters.
    fn plan_reads(&mut self) {
        let queues = self.source.queues.clone().filter(|q| !q.is_empty());
        let partitions = self.source.partitions.clone().filter(|p| !p.is_empty());
        let tq = self.p().table_queues();
        self.table_queues = tq;
        // Never read back what this query wrote: its outputs carry its prefix.
        let own = WhereCondition {
            field: "transactionId".into(),
            op: "!prefix".into(),
            value: Some(Value::String(output_prefix(&self.sh.query_id))),
        };
        let shard = self.sh.shard;
        let by_partition = matches!(self.o.shard_by, ShardBy::Partition);
        if self.table_queues.is_empty() {
            self.broker_queues = queues;
            self.broker_partitions = partitions;
            self.broker_where = self.pushdown.clone();
            self.broker_where.push(own);
            if let (Some((index, count)), true) = (shard, by_partition) {
                self.broker_shard = Some(FeedShard {
                    count,
                    index,
                    queues: None,
                });
            }
        } else {
            // A table queue is read whole: its rows pass no filter of the
            // stream's and no shard, so those filters move here.
            self.broker_queues = queues.as_ref().map(|q| {
                let mut all: Vec<String> = Vec::new();
                for x in q.iter().chain(self.table_queues.iter()) {
                    if !all.contains(x) {
                        all.push(x.clone());
                    }
                }
                all
            });
            self.broker_where = vec![own];
            self.local_where = self.pushdown.clone();
            self.local_partitions = partitions.map(|p| p.into_iter().collect());
            self.stream_queues = queues.clone();
            if let (Some((index, count)), true) = (shard, by_partition) {
                // The broker splits only the stream's own queues: never a
                // table queue, which every shard reads whole.
                let split: Option<Vec<String>> = queues.as_ref().map(|q| {
                    q.iter()
                        .filter(|x| !self.table_queues.contains(x))
                        .cloned()
                        .collect()
                });
                let hits_table = split.as_ref().is_some_and(|s| {
                    s.iter().any(|pat| {
                        pat.contains(['*', '?'])
                            && self.table_queues.iter().any(|t| glob_match(pat, t))
                    })
                });
                match split {
                    Some(s) if !s.is_empty() && !hits_table => {
                        self.broker_shard = Some(FeedShard {
                            count,
                            index,
                            queues: Some(s),
                        })
                    }
                    _ => self.local_shard = true,
                }
            }
        }
        if shard.is_some() && !by_partition {
            self.local_shard = true;
        }
    }

    fn make_reader(
        &self,
        from: FeedFrom,
        since: Option<crate::feed::Since>,
        tables_only: bool,
    ) -> Result<FeedReader> {
        let s = &self.source;
        let max_records = self.o.max_records.or(s.max_records);
        let max_bytes = self.o.max_bytes.or(s.max_bytes);
        let mut o = FeedOptions::new();
        o.follower_urls = s.follower_urls.clone();
        o.from = Some(from);
        o.max_records = max_records;
        o.max_bytes = max_bytes;
        if tables_only {
            o.queues = Some(self.table_queues.clone());
            o.conditions = vec![WhereCondition {
                field: "transactionId".into(),
                op: "!prefix".into(),
                value: Some(Value::String(output_prefix(&self.sh.query_id))),
            }];
            o.wait_millis = 0;
        } else {
            o.queues = self.broker_queues.clone();
            o.partitions = self.broker_partitions.clone();
            o.kinds = s.kinds.clone();
            o.since = since;
            o.conditions = self.broker_where.clone();
            o.shard = self.broker_shard.clone();
            o.wait_millis = self.o.wait_millis;
        }
        // The runner reads each message from its shape: no second copy of the payload.
        let mut reader = self.queen.feed(o)?;
        reader.set_shape_only();
        Ok(reader)
    }

    fn reader(&mut self) -> &mut FeedReader {
        self.reader.as_mut().expect("the runner's reader")
    }

    /// Where a message goes: a table's rows, the stream, or nowhere.
    fn route(&self, entry: &FeedEntry) -> Route {
        if self.table_queues.contains(&entry.queue) {
            return Route::Table;
        }
        if let Some(qs) = &self.stream_queues {
            if !qs.iter().any(|p| glob_match(p, &entry.queue)) {
                return Route::Skip;
            }
        }
        if let Some(ps) = &self.local_partitions {
            if !ps.contains(&entry.partition) {
                return Route::Skip;
            }
        }
        for c in &self.local_where {
            if !c.holds(entry.shape()) {
                return Route::Skip;
            }
        }
        if self.local_shard && matches!(self.o.shard_by, ShardBy::Partition) {
            if let Some((index, count)) = self.sh.shard {
                if shard_of(&entry.partition, count) != index {
                    return Route::Skip;
                }
            }
        }
        Route::Stream
    }

    /// Whether this shard takes a stream message, by `shard_by`: run inside
    /// the message's unit, so a function that fails meets the poison policy
    /// -- in shard 0 only; the other shards leave it.
    fn owns(&self, entry: &FeedEntry) -> FnResult<bool> {
        if !self.local_shard {
            return Ok(true);
        }
        let ShardBy::Key(f) = &self.o.shard_by else {
            return Ok(true);
        };
        let Some((index, count)) = self.sh.shard else {
            return Ok(true);
        };
        match f(entry.shape()) {
            Ok(k) => Ok(shard_of(&js::string_of(k.as_ref()), count) == index),
            Err(e) if index == 0 => Err(e),
            Err(_) => Ok(false),
        }
    }

    // ---- layout ----------------------------------------------------------------

    async fn kv_get(&self, key: &str) -> std::result::Result<queen_protocol::KvResult, FeedErr> {
        let out = self
            .queen
            .kv()
            .batch(vec![KvOperation::get(POSITION_NAMESPACE, key)])
            .await
            .map_err(FeedErr::from)?;
        Ok(out.results().first().cloned().unwrap_or_default())
    }

    /// The shard count (and how shards split) is part of the stream, kept in
    /// `queen-feed` / `<queryId>#layout`. Another one is refused.
    async fn check_layout(&mut self) -> std::result::Result<(), FeedErr> {
        let key = format!("{}#layout", self.sh.query_id);
        let count = self.sh.shard.map_or(1, |s| s.1);
        let by = if matches!(self.o.shard_by, ShardBy::Key(_)) {
            "function"
        } else {
            "partition"
        };
        for _ in 0..16 {
            let row = self.kv_get(&key).await?;
            let found = row.found();
            let stored = row
                .value
                .as_ref()
                .filter(|v| found && v.is_object() && js::is_integer(v.get("shards")))
                .cloned();
            let token_differs = |stored: &Option<Value>, t: &str| match stored {
                None => true,
                Some(s) => s.get("resetToken").and_then(|x| x.as_str()) != Some(t),
            };
            let resetting = match &self.o.reset {
                Reset::Always => true,
                Reset::Token(t) => token_differs(&stored, t),
                Reset::No => false,
            };
            if let Some(s) = &stored {
                let shards = js::as_num(s.get("shards")).unwrap_or(0.0) as u32;
                let stored_by = s
                    .get("shardBy")
                    .and_then(|x| x.as_str())
                    .unwrap_or("partition");
                if shards == count && (count == 1 || stored_by == by) {
                    self.layout_version = row.version.unwrap_or(0);
                    return Ok(());
                }
            }
            if stored.is_none() && count == 1 {
                self.layout_version = 0;
                return Ok(());
            }
            match &stored {
                None => {
                    // First sharded run: refuse when the query ran whole and kept a position.
                    let whole = self.kv_get(&self.sh.query_id.clone()).await?;
                    if whole.found() && !resetting {
                        return Err(FeedErr::coded(
                            format!(
                                "{} ran as one stream (its position is in KV); running it in {count} shards would move keys \
                                 between shards with their state: run it with {{ reset: true }} (or a reset string) to start the shards over",
                                self.sh.query_id
                            ),
                            "feed_layout_changed",
                        ));
                    }
                }
                Some(s) => {
                    if !resetting {
                        let shards = js::as_num(s.get("shards")).unwrap_or(0.0) as u32;
                        let stored_by = s
                            .get("shardBy")
                            .and_then(|x| x.as_str())
                            .unwrap_or("partition");
                        return Err(FeedErr::coded(
                            format!(
                                "{} runs in {shards} shard{} split by {stored_by}; running it in {count} split by {by} would move keys \
                                 between shards with their state: run it as before, or with {{ reset: true }} (or a reset string) to start over",
                                self.sh.query_id,
                                if shards == 1 { "" } else { "s" }
                            ),
                            "feed_layout_changed",
                        ));
                    }
                }
            }
            let mut value = Map::new();
            value.insert("shards".into(), json!(count));
            value.insert("shardBy".into(), json!(by));
            value.insert("updatedAt".into(), json!(js::iso(js::now_ms())));
            if let Reset::Token(t) = &self.o.reset {
                value.insert("resetToken".into(), json!(t));
            }
            let value = Value::Object(value);
            let op = if count == 1 {
                KvOperation::delete(POSITION_NAMESPACE, key.clone())
            } else if found {
                KvOperation::put(POSITION_NAMESPACE, key.clone(), value, Expiry::Forever)
                    .map_err(FeedErr::msg)?
                    .expect(row.version.unwrap_or(0))
                    .required()
            } else {
                KvOperation::put_if_absent(POSITION_NAMESPACE, key.clone(), value, Expiry::Forever)
                    .map_err(FeedErr::msg)?
                    .required()
            };
            let res = match self
                .queen
                .transaction()
                .kv(op)
                .commit_outcome()
                .await
                .map_err(FeedErr::from)?
            {
                TxOutcome::Answered(r) => r,
                TxOutcome::LockNotHeld(n) => return Err(FeedErr::lock_not_held(&n)),
            };
            if !res.success {
                // Another process wrote it first: read it again, and judge that one.
                if res.reason.as_deref() == Some("kv_precondition") {
                    continue;
                }
                return Err(FeedErr::msg(format!(
                    "FeedRunner: the shard layout could not be written ({})",
                    res.reason.as_deref().unwrap_or("unknown")
                )));
            }
            if count == 1 {
                self.layout_version = 0;
            } else {
                self.layout_version = match version_of(&res, &key) {
                    Some(v) => v,
                    None => self.kv_get(&key).await?.version.unwrap_or(0),
                };
            }
            return Ok(());
        }
        Err(FeedErr::msg("FeedRunner: the shard layout keeps changing"))
    }

    /// The layout changed under this process: it stops.
    async fn layout_moved(&self) -> std::result::Result<(), FeedErr> {
        let row = self.kv_get(&format!("{}#layout", self.sh.query_id)).await?;
        let v = if row.found() {
            row.version.unwrap_or(0)
        } else {
            0
        };
        if v == self.layout_version {
            return Ok(());
        }
        let now = if row.found() {
            format!(
                "{} shards",
                js::string_of(row.value.as_ref().and_then(|x| x.get("shards")))
            )
        } else {
            "one stream".into()
        };
        Err(FeedErr::fatal(
            format!(
                "the shard layout of {} changed while this process ran it ({now} now): another deploy runs it; this process stops",
                self.sh.query_id
            ),
            "feed_layout_changed",
        ))
    }

    // ---- the loop ----------------------------------------------------------------

    async fn run(mut self) {
        let mut backoff = 500.0;
        while !self.stopping() {
            match self.iteration().await {
                Ok(()) => backoff = 500.0,
                Err(err) => {
                    // stop() cancelled the read in flight: what the pages before
                    // it did is still pending, and the stop below commits it.
                    if self.stopping() && err.aborted {
                        break;
                    }
                    if err.stopping {
                        if self.active() {
                            self.rollback();
                        }
                        break;
                    }
                    if err.fatal {
                        self.report(
                            &err,
                            json!({ "phase": "load", "position": self.position.to_value() }),
                        );
                        self.sh.c().set(
                            "stopped",
                            json!({ "reason": "config", "error": err.message }),
                        );
                        self.set_stopping();
                        break;
                    }
                    self.count("errors", 1);
                    if self.active() {
                        self.rollback();
                    }
                    if err.code.as_deref() == Some(LOCK_NOT_HELD) {
                        self.stand_down("expired");
                        continue;
                    }
                    let phase = if self.active() { "cycle" } else { "load" };
                    self.report(
                        &err,
                        json!({ "phase": phase, "position": self.position.to_value() }),
                    );
                    // A broker without the feed will not grow one: stop rather than retry.
                    if err.no_feed_route() {
                        self.set_stopping();
                        break;
                    }
                    self.sleep(backoff).await;
                    backoff = (backoff * 2.0).min(MAX_BACKOFF_MILLIS);
                }
            }
            self.publish();
        }
        // Stopping: a batch that waits goes to its function first, tried once (one
        // that fails now is delivered again at the next start); then what is done
        // and not yet committed goes in, with a snapshot, so the next start has
        // nothing to read again.
        let held = |l: &Option<Lock>| l.as_ref().is_none_or(|l| l.held());
        if self.active()
            && !self.pending.batch.is_empty()
            && held(&self.lock)
            && self.poison_stop.is_none()
        {
            if let Err(err) = self.flush_batch().await {
                if !err.stopping {
                    self.report(
                        &err,
                        json!({ "phase": "stop", "position": self.position.to_value() }),
                    );
                }
                self.rollback();
            }
        }
        let lock_gone = !held(&self.lock);
        let snapshot_owed = {
            let p = self.p();
            p.has_state() && p.changed_since_snapshot()
        };
        // Asked to stop: the record of this last commit says so (`stopped`), so
        // that a stream stopped on purpose is not taken for one that died.
        // (Only at a position: a stream stopped before its first read has none to write.)
        let at_position =
            self.pending.next.is_some() || matches!(self.position, FeedFrom::Position(_));
        let asked = self.sh.stop_asked.load(Ordering::SeqCst)
            && self.sh.c().m.get("stopped").is_none_or(|v| v.is_null())
            && at_position;
        if self.active()
            && (self.has_pending() || snapshot_owed || asked)
            && !lock_gone
            && self.poison_stop.is_none()
        {
            if asked {
                self.stop_mark = Some(json!({ "reason": "stop", "at": js::iso(self.now()) }));
            }
            let r = self.commit().await;
            self.stop_mark = None;
            if let Err(err) = r {
                self.report(
                    &err,
                    json!({ "phase": "stop", "position": self.position.to_value() }),
                );
            }
        }
        self.set_active(false);
        self.sh.stop_server();
        if let Some(l) = &self.lock {
            let _ = l.release().await;
        }
        if let Some(r) = self.reader.as_mut() {
            r.close();
        }
        self.sh.set_role("stopped");
        self.publish();
        self.sh.stop_tx.send_replace(true);
        self.sh.done_tx.send_replace(true);
    }

    async fn iteration(&mut self) -> std::result::Result<(), FeedErr> {
        if !self.active() {
            if let Some(lock) = self.lock.clone() {
                if !lock.held() {
                    self.sh.set_role("standby");
                    let got = tokio::select! {
                        r = lock.acquire_within(Duration::from_secs(3600)) => r.map_err(FeedErr::from)?,
                        _ = self.wait_stop() => false,
                    };
                    if !got || self.stopping() {
                        return Ok(());
                    }
                    self.count("takeovers", 1);
                }
            }
            self.become_active(false).await?;
        }
        if let Some(l) = &self.lock {
            if self.sh.lock_lost.load(Ordering::SeqCst) || !l.held() {
                self.stand_down("lost");
                return Ok(());
            }
        }
        self.cycle().await
    }

    async fn become_active(&mut self, first: bool) -> std::result::Result<(), FeedErr> {
        self.load(first).await?;
        // The first commit of each activation writes what the stream is.
        self.about_owed = true;
        self.sh.lock_lost.store(false, Ordering::SeqCst);
        self.lock_acquired.notify_one();
        self.set_active(true);
        self.sh.set_role("active");
        if self.o.serve_state {
            self.sh.stop_server();
            let queue = query_queue_name(&self.sh.query_id, self.sh.shard.map_or(0, |s| s.0));
            let server = StateServer::start(
                self.queen.clone(),
                queue,
                Arc::clone(&self.sh),
                self.o.serve_wait_millis,
            );
            *lock_ok(&self.sh.server) = Some(server);
        }
        self.publish();
        Ok(())
    }

    /// The lock is gone: drop what was not committed and wait to hold it again.
    fn stand_down(&mut self, why: &str) {
        self.rollback();
        self.initial_snap = false;
        self.set_active(false);
        self.sh.stop_server();
        self.sh.set_role("standby");
        tracing::debug!(query_id = %self.sh.id, why, "FeedRunner.standby");
    }

    fn report(&mut self, err: &FeedErr, ctx: Value) {
        tracing::error!(query_id = %self.sh.id, error = %err.message, "FeedRunner");
        let mut c = Map::new();
        c.insert("queryId".into(), json!(self.sh.query_id));
        c.insert(
            "shard".into(),
            match self.sh.shard {
                Some((index, count)) => json!({ "index": index, "count": count }),
                None => Value::Null,
            },
        );
        if let Value::Object(o) = &ctx {
            for (k, v) in o {
                c.insert(k.clone(), v.clone());
            }
        }
        if let Some(hook) = &self.o.on_error {
            let info = FeedErrorInfo {
                message: err.message.clone(),
                code: err.code.clone(),
                ctx: Value::Object(c),
            };
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| hook(&info)));
            return;
        }
        // No hook: say it on stderr, once per message every 30 s.
        let now = Instant::now();
        let again = self
            .last_said
            .get(&err.message)
            .is_none_or(|t| now.duration_since(*t) >= Duration::from_secs(30));
        if again {
            self.last_said.insert(err.message.clone(), now);
            let phase = ctx.get("phase").and_then(|p| p.as_str()).unwrap_or("");
            let retrying = (phase == "cycle" || phase == "load") && !err.no_feed_route();
            eprintln!(
                "[queen feed] {}: {}{}",
                self.sh.id,
                err.message,
                if retrying { " (retrying)" } else { "" }
            );
        }
    }

    // ---- one cycle ---------------------------------------------------------------

    async fn cycle(&mut self) -> std::result::Result<(), FeedErr> {
        let wait = self.next_wait.take().unwrap_or(self.o.wait_millis);
        // stop() cancels a long poll in flight: nothing of it was processed yet.
        let page: FeedPage = {
            let stop = self.sh.stop_tx.subscribe();
            let reader = self.reader.as_mut().expect("reader");
            tokio::select! {
                r = reader.read(Some(wait)) => r.map_err(FeedErr::from)?,
                _ = async move { let mut s = stop; let _ = s.wait_for(|x| *x).await; } => return Err(FeedErr::aborted()),
            }
        };
        {
            let mut c = self.sh.c();
            c.add("cycles", 1);
            c.add("entriesRead", page.entries.len() as u64);
            c.set(
                "applied",
                page.applied.map(Value::from).unwrap_or(Value::Null),
            );
            c.set(
                "lag",
                match page.applied {
                    Some(a) => json!((a as i64 - page.next as i64 + 1).max(0)),
                    None => Value::Null,
                },
            );
        }
        let resolved = page.resolved_ts.as_deref().and_then(js::parse_date);
        if let Some(resolved) = resolved {
            let now = self.now();
            // This machine's clock was set back: learn the distance again.
            if now < self.last_now {
                self.offset = f64::NEG_INFINITY;
            }
            self.last_now = now;
            if resolved > self.resolved_ms {
                self.resolved_ms = resolved;
                self.resolved_at = now;
            }
            self.offset = self.offset.max(resolved - now);
        }
        // Lost the lock while the read was out: what it read is the next holder's.
        if let Some(l) = &self.lock {
            if self.sh.lock_lost.load(Ordering::SeqCst) || !l.held() {
                self.stand_down("lost");
                return Ok(());
            }
        }
        if self.initial_snap {
            self.initial_snap = false;
            let first = page.entries.first();
            let mut snap = Map::new();
            snap.insert("id".into(), Value::Null);
            snap.insert("chunks".into(), json!(0));
            snap.insert("bytes".into(), json!(0));
            snap.insert(
                "position".into(),
                json!(first.map_or(page.next, |e| e.position)),
            );
            snap.insert(
                "clock".into(),
                js::num(match first {
                    Some(e) => ts_of(e),
                    None => resolved.unwrap_or(0.0),
                }),
            );
            snap.insert("metas".into(), self.p().metas());
            // Nothing closed yet where the stream starts: a replay from here counts its closes from this.
            snap.insert("units".into(), json!(self.units));
            self.snap = Some(Value::Object(snap));
            self.p().snapshot_taken();
        }
        let r = self.process_page(&page, resolved).await?;
        if r == Flow::Abandon {
            return Ok(());
        }
        // Halt: an item of a batch stopped the stream (rolled back to the last commit already).
        if r == Flow::Stop || r == Flow::Halt {
            self.stop_on_poison().await?;
            return Ok(());
        }
        self.settle(&page).await
    }

    async fn process_page(
        &mut self,
        page: &FeedPage,
        resolved: Option<f64>,
    ) -> std::result::Result<Flow, FeedErr> {
        for g in groups(&page.entries, None) {
            let mut cp = self.checkpoint();
            let r = self.process_group(&g, Some(&mut cp)).await?;
            if r == Flow::Abandon || r == Flow::Halt {
                return Ok(r);
            }
            if r == Flow::Stop {
                // The whole transaction the poisoned message came in stays for later.
                self.restore(&cp);
                self.pending.next = Some(g.position);
                return Ok(Flow::Stop);
            }
            self.pending.next = Some(g.position + 1);
            self.read_to(ts_of(&g.entries[0]), false);
            // Enough waiting outputs, or a full batch: commit them now, between two positions.
            if self.pending.outputs.len() >= self.o.max_pending_outputs || self.batch_full() {
                let c = self.commit_all().await?;
                if c == Flow::Halt {
                    return Ok(Flow::Halt);
                }
                if c != Flow::Ok {
                    return Ok(Flow::Abandon);
                }
            }
        }
        let feed_clock = {
            let p = self.p();
            p.has_state() && p.needs_feed_clock()
        };
        if feed_clock {
            let mut cp = self.checkpoint();
            let t = self.clock.max(resolved.unwrap_or(0.0));
            self.clock = t;
            let r = self.advance_to(t, Some(&mut cp)).await?;
            if r == Flow::Abandon || r == Flow::Halt {
                return Ok(r);
            }
            if r == Flow::Stop {
                self.restore(&cp);
                self.pending.next = Some(page.next);
                self.read_to(resolved.unwrap_or(0.0), page.caught_up);
                return Ok(Flow::Stop);
            }
        }
        self.pending.next = Some(page.next);
        self.read_to(resolved.unwrap_or(0.0), page.caught_up);
        Ok(Flow::Ok)
    }

    /// The feed time the pending position is at (it only moves forward), and
    /// whether the read was caught up there.
    fn read_to(&mut self, t: f64, caught_up: bool) {
        let cur = self.pending.read_to.unwrap_or(self.committed_read_to);
        self.pending.read_to = Some(cur.max(if t.is_finite() { t } else { 0.0 }));
        self.pending.caught_up = Some(caught_up);
    }

    async fn process_group(
        &mut self,
        g: &Group,
        mut cp: Option<&mut Cp>,
    ) -> std::result::Result<Flow, FeedErr> {
        // One position is one transaction of the log: its messages share a time.
        let t = self.clock.max(ts_of(&g.entries[0]));
        let has_state = self.p().has_state();
        if has_state {
            // Before the group's messages: a commit in the middle of this close names the group's position.
            let r = self.advance_to(t, cp.as_deref_mut()).await?;
            if r != Flow::Ok {
                return Ok(r);
            }
        }
        self.clock = t;
        // A table row of the transaction counts before its other messages.
        let mut routed: Vec<(&FeedEntry, Route)> =
            g.entries.iter().map(|e| (e, self.route(e))).collect();
        routed.sort_by_key(|(_, r)| if *r == Route::Table { 0 } else { 1 });
        for (entry, route) in routed {
            match route {
                Route::Skip => continue,
                Route::Table => {
                    if self.unit(Subject::Entry(entry), Work::Row(entry)).await? == Flow::Stop {
                        return Ok(Flow::Stop);
                    }
                    self.count("tableRows", 1);
                    self.pending.work += 1;
                }
                Route::Stream => {
                    if self
                        .unit(Subject::Entry(entry), Work::Entry(entry, t))
                        .await?
                        == Flow::Stop
                    {
                        return Ok(Flow::Stop);
                    }
                    if has_state {
                        self.pending.work += 1;
                    }
                }
            }
        }
        self.entries_since_snap += g.entries.len();
        // An event-time step may have moved its watermark. After the group's
        // messages: a commit in the middle of this close names the next position.
        if has_state {
            self.pending.next = Some(g.position + 1);
            self.read_to(t, false);
            let r = self.advance_to(t, cp).await?;
            if r != Flow::Ok {
                return Ok(r);
            }
        }
        Ok(Flow::Ok)
    }

    /// Close what is due at `t`; each result is a unit of its own, counted
    /// (`units`). When many results wait, they commit in the middle of the
    /// close; a replay stops at the count the stored record names.
    async fn advance_to(
        &mut self,
        t: f64,
        mut cp: Option<&mut Cp>,
    ) -> std::result::Result<Flow, FeedErr> {
        if !t.is_finite() {
            return Ok(Flow::Ok);
        }
        let live = !self.replaying;
        let max_units: Option<u64> = match (live, self.replay_cap) {
            (false, Some(cap)) => Some(cap.saturating_sub(self.units)),
            _ => None,
        };
        let outer = self.burst_target;
        self.burst_target = Some(outer.map_or(t, |o| o.max(t)));
        let r = self.advance_loop(t, max_units, live, &mut cp).await;
        self.burst_target = outer;
        r
    }

    async fn advance_loop(
        &mut self,
        t: f64,
        max_units: Option<u64>,
        live: bool,
        cp: &mut Option<&mut Cp>,
    ) -> std::result::Result<Flow, FeedErr> {
        let mut done: u64 = 0;
        loop {
            if max_units.is_some_and(|m| done >= m) {
                return Ok(Flow::Ok);
            }
            let Some((at, when)) = self.p().next_close(t) else {
                return Ok(Flow::Ok);
            };
            // At most CLOSE_BATCH at once, and never more than may run.
            let limit = match max_units {
                Some(m) => (CLOSE_BATCH as u64).min(m - done) as usize,
                None => CLOSE_BATCH,
            };
            let took = self.p().take(at, when, limit);
            if let Some(rel) = took.release {
                let em = self.p().release_emit(at, &rel);
                let r = self
                    .unit(Subject::Emit(&em), Work::Release(at, &rel, when))
                    .await?;
                if r == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                self.units += 1;
                done += 1;
                if live {
                    let b = self.on_batch(cp).await?;
                    if b != Flow::Ok {
                        return Ok(b);
                    }
                }
                continue;
            }
            let n = took.emits.len();
            for em in &took.emits {
                let r = self
                    .unit(Subject::Emit(em), Work::Emit(at, em, when))
                    .await?;
                if r == Flow::Stop {
                    return Ok(Flow::Stop);
                }
                self.units += 1;
                done += 1;
            }
            if live && n > 0 {
                let b = self.on_batch(cp).await?;
                if b != Flow::Ok {
                    return Ok(b);
                }
            }
            if n >= CLOSE_BATCH {
                tokio::task::yield_now().await;
            }
        }
    }

    /// After a batch of closes: commit in the middle once enough results
    /// wait, or a full batch (Abandon, or Halt: an item stopped the stream).
    async fn on_batch(&mut self, cp: &mut Option<&mut Cp>) -> std::result::Result<Flow, FeedErr> {
        if self.pending.outputs.len() < self.o.max_pending_outputs && !self.batch_full() {
            return Ok(Flow::Ok);
        }
        self.count("closeCommits", 1);
        let c = self.commit_all().await?;
        if c == Flow::Halt {
            return Ok(Flow::Halt);
        }
        if c != Flow::Ok {
            return Ok(Flow::Abandon);
        }
        if let Some(c) = cp.as_deref_mut() {
            *c = self.checkpoint();
        }
        Ok(Flow::Ok)
    }

    /// One unit's work, once: sync steps under the pipeline's lock, the sink's async parts after.
    async fn run_work(&mut self, work: &Work<'_>) -> std::result::Result<(), FeedErr> {
        let mut ctr: u64 = 0;
        match work {
            Work::Row(entry) => {
                self.p().apply_row(entry.shape()).map_err(FeedErr::msg)?;
            }
            Work::Entry(entry, t) => {
                if !self.owns(entry).map_err(FeedErr::msg)? {
                    return Ok(());
                }
                let envs = self.p().entry(entry.shape(), *t).map_err(FeedErr::msg)?;
                for env in envs {
                    self.out(env, Some(entry), &mut ctr).await?;
                }
            }
            Work::Emit(at, em, when) => {
                let envs = self.p().run_emit(*at, em, *when).map_err(FeedErr::msg)?;
                for env in envs {
                    self.out(env, None, &mut ctr).await?;
                }
            }
            Work::Release(at, rel, when) => {
                let (envs, _) = self
                    .p()
                    .run_release(*at, rel, *when)
                    .map_err(FeedErr::msg)?;
                for env in envs {
                    self.out(env, None, &mut ctr).await?;
                }
            }
        }
        Ok(())
    }

    /// Run `work` until it passes, at most `retries` more times, each time
    /// from the state it saw the first time. Then the poison policy. In a
    /// replay a failure is left out at once.
    async fn unit(
        &mut self,
        subject: Subject<'_>,
        work: Work<'_>,
    ) -> std::result::Result<Flow, FeedErr> {
        if self.replaying {
            // What the run before left out (its poison policy), a replay leaves out.
            if self.skip_set.contains(&skip_id(&subject)) {
                self.count("replaySkipped", 1);
                return Ok(Flow::Ok);
            }
            for attempt in 0..=self.o.retries {
                let cp = self.checkpoint();
                match self.run_work(&work).await {
                    Ok(()) => return Ok(Flow::Ok),
                    Err(e) => {
                        self.restore(&cp);
                        if attempt == self.o.retries {
                            self.count("replaySkipped", 1);
                            tracing::warn!(query_id = %self.sh.id, error = %e.message, "FeedRunner.replaySkip");
                            return Ok(Flow::Ok);
                        }
                        self.sleep(self.o.retry_delay_millis * 2f64.powi(attempt as i32))
                            .await;
                    }
                }
            }
            return Ok(Flow::Ok);
        }
        let mut last: Option<FeedErr> = None;
        if matches!(self.terminal, Some(Terminal::ForeachBatch(_))) {
            self.subject = Some(Arc::new(match &subject {
                Subject::Entry(e) => ItemSubject::Entry(Arc::clone(&e.shape)),
                Subject::Emit(em) => ItemSubject::Emit {
                    step: em.step,
                    kind: em.ctx.get("kind").cloned().unwrap_or(Value::Null),
                    key: em.key.clone(),
                    entry: em.entry.clone(),
                },
            }));
        }
        for attempt in 0..=self.o.retries {
            let cp = self.checkpoint();
            match self.run_work(&work).await {
                Ok(()) => return Ok(Flow::Ok),
                Err(e) => {
                    self.restore(&cp);
                    last = Some(e);
                    if self.stopping() {
                        return Err(FeedErr::stopping());
                    }
                    if attempt < self.o.retries {
                        self.count("retries", 1);
                        self.sleep(self.o.retry_delay_millis * 2f64.powi(attempt as i32))
                            .await;
                    }
                }
            }
        }
        let err = last.unwrap_or_else(|| FeedErr::msg("failed"));
        Ok(self.poisoned(&subject, &err, self.o.retries + 1))
    }

    fn poisoned(&mut self, subject: &Subject<'_>, err: &FeedErr, attempts: u32) -> Flow {
        self.count("poisoned", 1);
        let has_state = self.p().has_state();
        if !matches!(self.o.on_poison, Poison::Stop) && has_state {
            self.pending_skips.push(skip_id(subject));
        }
        // A feed message: the unit's own, or one an event-time step held and took in its turn.
        let entry: Option<Value> = match subject {
            Subject::Entry(e) => Some(e.to_value()),
            Subject::Emit(em) => em.entry.as_deref().cloned(),
        };
        let (what, where_) = match subject {
            Subject::Entry(_) => describe(entry.as_ref(), None),
            Subject::Emit(em) => {
                let kind = em.ctx.get("kind").cloned().unwrap_or(Value::Null);
                describe(entry.as_ref(), Some((em.step, &kind, &em.key)))
            }
        };
        let reason = err.message.clone();
        let mut ctx = Map::new();
        ctx.insert("phase".into(), json!("poison"));
        match &self.o.on_poison {
            Poison::Skip => {
                self.count("skipped", 1);
                ctx.insert("action".into(), json!("skipped"));
                ctx.insert("attempts".into(), json!(attempts));
                for (k, v) in &what {
                    ctx.insert(k.clone(), v.clone());
                }
                let e = FeedErr::msg(format!(
                    "{where_} failed {attempts} times and was skipped: {reason}"
                ));
                self.report(&e, Value::Object(ctx));
                Flow::Ok
            }
            Poison::DeadLetter(queue) => {
                let queue = queue.clone();
                let mut record = Map::new();
                record.insert("queryId".into(), json!(self.sh.query_id));
                record.insert("attempts".into(), json!(attempts));
                record.insert(
                    "error".into(),
                    json!({ "name": "Error", "message": reason }),
                );
                match (&entry, subject) {
                    (Some(e), _) => {
                        record.insert("entry".into(), snapshot_of(e, &None));
                    }
                    (None, Subject::Emit(em)) => {
                        let mut r = em.ctx.as_object().cloned().unwrap_or_default();
                        if let Some(v) = &em.value {
                            r.insert("value".into(), v.clone());
                        }
                        if let Some(m) = em.state.get("msg") {
                            r.insert("waiting".into(), m.clone());
                        }
                        record.insert("result".into(), Value::Object(r));
                    }
                    _ => {}
                }
                if let Some((index, count)) = self.sh.shard {
                    record.insert("shard".into(), json!({ "index": index, "count": count }));
                }
                // A message an event-time step held is one of maybe several items of it.
                let held = match subject {
                    Subject::Emit(em) if em.release => {
                        format!("\n{}\n{}", em.step, em.id.as_deref().unwrap_or(""))
                    }
                    _ => String::new(),
                };
                let (id, partition) = match &entry {
                    Some(e) => {
                        let s = |k: &str| js::string_of(e.get(k));
                        let h = sha(&format!(
                            "{}\n{}\ndlq\n{}\n{}\n{}\n{}{held}",
                            self.sh.id,
                            self.epoch,
                            s("position"),
                            s("queue"),
                            s("partition"),
                            s("offset")
                        ));
                        (
                            format!("{}{}", output_prefix(&self.sh.query_id), &h[..40]),
                            s("partition"),
                        )
                    }
                    None => {
                        let Subject::Emit(em) = subject else {
                            unreachable!()
                        };
                        (self.emit_id(), em.key.clone())
                    }
                };
                self.pending.outputs.push(Output {
                    queue: queue.clone(),
                    partition,
                    data: Value::Object(record),
                    transaction_id: id,
                });
                self.count("deadLettered", 1);
                ctx.insert("action".into(), json!("deadLettered"));
                ctx.insert("attempts".into(), json!(attempts));
                for (k, v) in &what {
                    ctx.insert(k.clone(), v.clone());
                }
                let e = FeedErr::msg(format!(
                    "{where_} failed {attempts} times and went to {queue}: {reason}"
                ));
                self.report(&e, Value::Object(ctx));
                Flow::Ok
            }
            Poison::Stop => {
                self.poison_stop = Some(PoisonStop {
                    where_,
                    what,
                    attempts,
                    reason,
                    batch: false,
                });
                Flow::Stop
            }
        }
    }

    /// onPoison 'stop': commit what came before the message, then stop there.
    async fn stop_on_poison(&mut self) -> std::result::Result<(), FeedErr> {
        // A batch that waits came before the message: it goes first (the commit
        // moves the position past it). An item of it that fails stops the stream
        // instead, at the last commit (Halt: `poison_stop` names that item now).
        if !self.pending.batch.is_empty() && self.poison_stop.as_ref().is_some_and(|p| !p.batch) {
            self.flush_batch().await?;
        }
        let Some(p) = &self.poison_stop else {
            return Ok(());
        };
        let (where_, attempts, reason, what, batch) = (
            p.where_.clone(),
            p.attempts,
            p.reason.clone(),
            p.what.clone(),
            p.batch,
        );
        // This commit's record says why the stream stopped, and at which
        // message: it is written even with nothing else to commit.
        let mut mark = Map::new();
        mark.insert("reason".into(), json!("poison"));
        mark.insert("at".into(), json!(js::iso(self.now())));
        mark.insert(
            "error".into(),
            json!(clip_text(&reason, STOP_ERROR_MAX_CHARS)),
        );
        mark.insert("attempts".into(), json!(attempts));
        for (k, v) in &what {
            mark.insert(k.clone(), super::about::clip(v, 1));
        }
        self.stop_mark = Some(Value::Object(mark));
        let r = self.commit().await;
        self.stop_mark = None;
        if r? != Flow::Ok {
            // Another commit won: the stream reads on from the stored position.
            self.poison_stop = None;
            return Ok(());
        }
        let message = if batch {
            format!(
                "{where_} failed {attempts} times in foreachBatch: {reason}. The stream stopped before its batch: fix the function \
                 and start it again (it gets the batch again), or run it with onPoison: 'skip' or a dead-letter queue"
            )
        } else {
            format!(
                "{where_} failed {attempts} times: {reason}. The stream stopped there: fix the function and start it again, \
                 or run it with onPoison: 'skip' or a dead-letter queue"
            )
        };
        let mut stopped = Map::new();
        stopped.insert("reason".into(), json!("poison"));
        stopped.insert("error".into(), json!(reason));
        stopped.insert("attempts".into(), json!(attempts));
        let mut ctx = Map::new();
        ctx.insert("phase".into(), json!("poison"));
        ctx.insert("action".into(), json!("stopped"));
        ctx.insert("attempts".into(), json!(attempts));
        for (k, v) in &what {
            stopped.insert(k.clone(), v.clone());
            ctx.insert(k.clone(), v.clone());
        }
        self.sh.c().set("stopped", Value::Object(stopped));
        let e = FeedErr::coded(message, "feed_poison");
        self.report(&e, Value::Object(ctx));
        self.set_stopping();
        Ok(())
    }

    fn checkpoint(&self) -> Cp {
        let p = self.p();
        Cp {
            state: p.checkpoint(),
            outputs: self.pending.outputs.len(),
            work: self.pending.work,
            effects: self.pending.effects,
            emit_seq: self.pending.emit_seq,
            clock: self.clock,
            units: self.units,
            late: p.late,
            skips: self.pending_skips.len(),
            batch: self.pending.batch.len(),
            batch_since: self.pending.batch_since,
            emitted: self.sh.c().get_u64("emitted"),
        }
    }

    fn restore(&mut self, cp: &Cp) {
        {
            let mut p = self.p();
            p.restore(&cp.state);
            p.late = cp.late;
        }
        self.pending.outputs.truncate(cp.outputs);
        self.pending.work = cp.work;
        self.pending.effects = cp.effects;
        self.pending.emit_seq = cp.emit_seq;
        self.clock = cp.clock;
        self.units = cp.units;
        self.pending_skips.truncate(cp.skips);
        // Never grows it: a batch that went since the checkpoint stays gone.
        self.pending.batch.truncate(cp.batch);
        self.pending.batch_since = if self.pending.batch.is_empty() {
            None
        } else {
            cp.batch_since
        };
        self.batch_changed();
        self.sh.c().set("emitted", json!(cp.emitted));
    }

    /// Back to the last commit: what was produced since is dropped, and read again.
    fn rollback(&mut self) {
        self.pending = fresh_pending();
        self.batch_changed();
        self.pending_skips.clear();
        self.clock = self.committed_clock;
        self.units = self.committed_units;
        self.p().discard();
        let pos = self.position;
        if let Some(r) = self.reader.as_mut() {
            r.seek(pos);
        }
    }

    fn has_pending(&self) -> bool {
        let p = &self.pending;
        let (has_state, dirty) = {
            let pl = self.p();
            (pl.has_state(), pl.dirty())
        };
        !p.outputs.is_empty()
            || p.work > 0
            || p.effects > 0
            || !p.batch.is_empty()
            || (has_state && dirty)
            || p.next
                .is_some_and(|n| self.position != FeedFrom::Position(n))
    }

    /// One result at the end of the chain: the queues it goes to (pushed
    /// with the next commit) or the `.foreach` function.
    async fn out(
        &mut self,
        env: Env,
        entry: Option<&FeedEntry>,
        ctr: &mut u64,
    ) -> std::result::Result<(), FeedErr> {
        if self.replaying {
            return Ok(());
        }
        self.count("emitted", 1);
        let ctx: Value = if env.derived {
            env.ctx.clone().unwrap_or(Value::Null)
        } else {
            entry.map(|e| e.to_value()).unwrap_or(Value::Null)
        };
        let value = env.value().cloned();
        match self.terminal.clone() {
            Some(t @ (Terminal::Foreach(_) | Terminal::ForeachBatch(_))) => {
                let fctx = if let (false, Some(entry)) = (env.derived, entry) {
                    let e = entry.shape();
                    let mut m = Map::new();
                    for k in [
                        "position",
                        "queue",
                        "partition",
                        "partitionId",
                        "offset",
                        "transactionId",
                    ] {
                        if let Some(v) = e.get(k) {
                            m.insert(k.into(), v.clone());
                        }
                    }
                    Value::Object(m)
                } else {
                    env.ctx.clone().unwrap_or(Value::Null)
                };
                let f = match t {
                    Terminal::Foreach(f) => f,
                    _ => {
                        // Kept for the batch, with the id a queue output of it would have: the same on every delivery.
                        let id = match entry {
                            Some(e) => {
                                let id = feed_output_id(
                                    &self.sh.query_id,
                                    e.shape(),
                                    *ctr,
                                    self.epoch,
                                    &self.sh.id,
                                );
                                *ctr += 1;
                                id
                            }
                            None => self.emit_id(),
                        };
                        if self.pending.batch.is_empty() {
                            self.pending.batch_since = Some(self.now());
                        }
                        let partition = match entry {
                            Some(e) => e.partition.clone(),
                            None => env.key.clone(),
                        };
                        self.pending.batch.push(PendingItem {
                            value,
                            ctx: fctx,
                            id,
                            subject: self.subject.clone(),
                            partition,
                        });
                        self.batch_changed();
                        return Ok(());
                    }
                };
                f(value, fctx).await.map_err(FeedErr::msg)?;
                self.count("effects", 1);
                self.pending.effects += 1;
                Ok(())
            }
            Some(Terminal::To(sink)) => {
                let fallback: Option<String> = if env.derived {
                    Some(env.key.clone())
                } else {
                    Some(
                        entry
                            .map(|e| e.partition.clone())
                            .unwrap_or_else(|| env.key.clone()),
                    )
                };
                let targets = targets_for(&sink, value.as_ref(), ctx, fallback).await?;
                for (queue, partition) in targets {
                    let transaction_id = match entry {
                        Some(e) => {
                            let id = feed_output_id(
                                &self.sh.query_id,
                                e.shape(),
                                *ctr,
                                self.epoch,
                                &self.sh.id,
                            );
                            *ctr += 1;
                            id
                        }
                        None => self.emit_id(),
                    };
                    self.pending.outputs.push(Output {
                        queue,
                        partition,
                        data: payload_of(value.as_ref()),
                        transaction_id,
                    });
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    fn emit_id(&mut self) -> String {
        let seq = self.pending.emit_seq;
        self.pending.emit_seq += 1;
        let h = sha(&format!(
            "{}\n{}\nemit\n{}\n{seq}",
            self.sh.id, self.epoch, self.version
        ));
        format!("{}{}", output_prefix(&self.sh.query_id), &h[..40])
    }

    /// After a page: commit when it is time, else move the broker's clock if a deadline is due.
    async fn settle(&mut self, page: &FeedPage) -> std::result::Result<(), FeedErr> {
        let now = self.now();
        let due = if !self.pending.batch.is_empty() {
            // A batch goes when it is due, and the commit right after it moves the
            // position. Until then nothing commits past it (no position, no tick):
            // the next read waits until the batch is due, at most.
            if !self.batch_due(now) {
                let since = self.pending.batch_since.unwrap_or(now);
                let w = (since + self.batch_wait_millis() as f64 - now)
                    .ceil()
                    .min(self.o.wait_millis as f64)
                    .max(0.0);
                self.next_wait = Some(w as u64);
                return Ok(());
            }
            true
        } else {
            // Outputs and done effects commit soon (at once when caught up); state
            // alone every commitEveryMillis; a position alone now and then.
            let outputs = !self.pending.outputs.is_empty() || self.pending.effects > 0;
            let moved = self
                .pending
                .next
                .is_some_and(|n| self.position != FeedFrom::Position(n));
            let dirty = self.p().dirty();
            if outputs {
                page.caught_up
                    || self.pending.outputs.len() >= self.o.max_pending_outputs
                    || now - self.last_commit_at >= self.o.commit_every_millis
            } else if dirty || self.pending.work > 0 {
                now - self.last_commit_at >= self.o.commit_every_millis
            } else if moved {
                now - self.last_position_commit >= self.o.position_commit_millis
            } else {
                false
            }
        };
        if due && self.commit_all().await? == Flow::Halt {
            self.stop_on_poison().await?;
            return Ok(());
        }
        let (has_state, needs, next) = {
            let mut p = self.p();
            let hs = p.has_state();
            let nf = p.needs_feed_clock();
            let nd = if hs && nf { p.next_due() } else { None };
            (hs, nf, nd)
        };
        if !has_state || !needs || self.resolved_at == 0.0 {
            return Ok(());
        }
        let Some(next) = next else { return Ok(()) };
        // A window or a deadline is pending. Right after a write of this
        // process, a short read shows the broker's time (the best sample).
        if due {
            self.next_wait = Some(TICK_READ_WAIT_MILLIS);
            return Ok(());
        }
        if !page.caught_up {
            return Ok(());
        }
        // Where the broker's clock is now, as far as this process can tell.
        let estimate = now + self.offset;
        if estimate >= next {
            if now - self.last_tick_at >= TICK_MIN_MILLIS {
                self.tick(now).await?;
            }
            self.next_wait = Some(TICK_READ_WAIT_MILLIS);
        } else if now - self.resolved_at >= CLOCK_SYNC_MILLIS
            && now - self.last_tick_at >= CLOCK_SYNC_MILLIS
        {
            // The last write anyone made is old: the estimate may be far behind.
            self.tick(now).await?;
            self.next_wait = Some(TICK_READ_WAIT_MILLIS);
        } else {
            let w = (next - estimate)
                .ceil()
                .max(0.0)
                .min(self.o.wait_millis as f64);
            self.next_wait = Some(w as u64);
        }
        Ok(())
    }

    // ---- foreach_batch -------------------------------------------------------------

    fn batch_sink(&self) -> Option<&super::batch::ForeachBatch> {
        match &self.terminal {
            Some(Terminal::ForeachBatch(b)) => Some(b),
            _ => None,
        }
    }

    fn batch_wait_millis(&self) -> u64 {
        self.batch_sink().map_or(0, |b| b.max_wait_millis)
    }

    /// The batch holds max_items: it goes now.
    fn batch_full(&self) -> bool {
        self.batch_sink()
            .is_some_and(|b| self.pending.batch.len() >= b.max_items)
    }

    /// The batch goes now: full, at the end of a read (max_wait_millis 0), or its first item waited max_wait_millis.
    fn batch_due(&self, now: f64) -> bool {
        let Some(b) = self.batch_sink() else {
            return true;
        };
        let since = self.pending.batch_since.unwrap_or(now);
        self.pending.batch.len() >= b.max_items
            || b.max_wait_millis == 0
            || now - since >= b.max_wait_millis as f64
    }

    /// The batch that waits, then the commit (which moves the position past it). Halt: an item stopped the stream.
    async fn commit_all(&mut self) -> std::result::Result<Flow, FeedErr> {
        if !self.pending.batch.is_empty() && self.flush_batch().await? == Flow::Halt {
            return Ok(Flow::Halt);
        }
        self.commit().await
    }

    /// Every item that waits, to the function, max_items at a time. Ok, or
    /// Halt (rolled back to the last commit).
    async fn flush_batch(&mut self) -> std::result::Result<Flow, FeedErr> {
        let max = self.batch_sink().map_or(usize::MAX, |b| b.max_items.max(1));
        let items = std::mem::take(&mut self.pending.batch);
        for slice in items.chunks(max) {
            match self.deliver(slice).await {
                Ok(Flow::Halt) => return Ok(Flow::Halt),
                Ok(_) => {}
                Err(e) => {
                    // Left as it was: the caller rolls back.
                    if self.pending.batch.is_empty() {
                        self.pending.batch = items;
                    }
                    return Err(e);
                }
            }
        }
        self.pending.batch = Vec::new();
        self.pending.batch_since = None;
        self.batch_changed();
        Ok(Flow::Ok)
    }

    /// `metrics().pendingBatch` follows the batch as it changes (as JS reads it).
    fn batch_changed(&self) {
        self.sh.c().pending_batch = self.pending.batch.len();
    }

    /// One call: tried `retries` more times, with the pause of a message's
    /// retries; then item by item, each tried as often, so the poison policy
    /// meets the item that fails, and the others are delivered.
    async fn deliver(&mut self, items: &[PendingItem]) -> std::result::Result<Flow, FeedErr> {
        let Some(err) = self.try_items(items).await? else {
            return Ok(Flow::Ok);
        };
        if items.len() == 1 {
            return Ok(self.item_poisoned(&items[0], &err));
        }
        for item in items {
            if let Some(e) = self.try_items(std::slice::from_ref(item)).await? {
                if self.item_poisoned(item, &e) == Flow::Halt {
                    return Ok(Flow::Halt);
                }
            }
        }
        Ok(Flow::Ok)
    }

    /// `None` when the function returned, else the error of the last try. A stop cuts the tries short.
    async fn try_items(
        &mut self,
        items: &[PendingItem],
    ) -> std::result::Result<Option<FeedErr>, FeedErr> {
        let Some(f) = self.batch_sink().map(|b| b.f.clone()) else {
            return Ok(None);
        };
        for attempt in 0..=self.o.retries {
            match f(items.iter().map(PendingItem::item).collect()).await {
                Ok(()) => {
                    let n = items.len() as u64;
                    {
                        let mut c = self.sh.c();
                        c.add("batches", 1);
                        c.add("effects", n);
                    }
                    self.pending.effects += n;
                    return Ok(None);
                }
                Err(e) => {
                    if self.stopping() {
                        return Err(FeedErr::stopping());
                    }
                    if attempt == self.o.retries {
                        return Ok(Some(FeedErr::msg(e)));
                    }
                    self.count("retries", 1);
                    self.sleep(self.o.retry_delay_millis * 2f64.powi(attempt as i32))
                        .await;
                }
            }
        }
        Ok(None)
    }

    /// The poison policy for one item of a batch: Skip leaves it out, a queue
    /// receives it (`{ queryId, attempts, error, item }`, with the next commit);
    /// Stop rolls back to the last commit -- nothing of the batch commits, the
    /// next start delivers it again -- and answers Halt. (Not in the record's
    /// skips: the unit passed, and a replay applies its state again.)
    fn item_poisoned(&mut self, item: &PendingItem, err: &FeedErr) -> Flow {
        let attempts = self.o.retries + 1;
        self.count("poisoned", 1);
        let (what, where_) = describe_item(item.subject.as_deref());
        let where_ = format!("an item of {where_}");
        let reason = err.message.clone();
        let mut ctx = Map::new();
        ctx.insert("phase".into(), json!("poison"));
        match self.o.on_poison.clone() {
            Poison::Skip => {
                self.count("skipped", 1);
                ctx.insert("action".into(), json!("skipped"));
                ctx.insert("attempts".into(), json!(attempts));
                for (k, v) in &what {
                    ctx.insert(k.clone(), v.clone());
                }
                let e = FeedErr::msg(format!(
                    "{where_} failed {attempts} times in foreachBatch and was skipped: {reason}"
                ));
                self.report(&e, Value::Object(ctx));
                Flow::Ok
            }
            Poison::DeadLetter(queue) => {
                let mut it = Map::new();
                if let Some(v) = &item.value {
                    it.insert("value".into(), v.clone());
                }
                it.insert("ctx".into(), item.ctx.clone());
                it.insert("id".into(), json!(item.id));
                let mut record = Map::new();
                record.insert("queryId".into(), json!(self.sh.query_id));
                record.insert("attempts".into(), json!(attempts));
                record.insert(
                    "error".into(),
                    json!({ "name": "Error", "message": reason }),
                );
                record.insert("item".into(), Value::Object(it));
                if let Some((index, count)) = self.sh.shard {
                    record.insert("shard".into(), json!({ "index": index, "count": count }));
                }
                let h = sha(&format!(
                    "{}\n{}\ndlq-item\n{}",
                    self.sh.id, self.epoch, item.id
                ));
                self.pending.outputs.push(Output {
                    queue: queue.clone(),
                    partition: item.partition.clone(),
                    data: Value::Object(record),
                    transaction_id: format!("{}{}", output_prefix(&self.sh.query_id), &h[..40]),
                });
                self.count("deadLettered", 1);
                ctx.insert("action".into(), json!("deadLettered"));
                ctx.insert("attempts".into(), json!(attempts));
                for (k, v) in &what {
                    ctx.insert(k.clone(), v.clone());
                }
                let e = FeedErr::msg(format!(
                    "{where_} failed {attempts} times in foreachBatch and went to {queue}: {reason}"
                ));
                self.report(&e, Value::Object(ctx));
                Flow::Ok
            }
            Poison::Stop => {
                self.rollback();
                self.poison_stop = Some(PoisonStop {
                    where_,
                    what,
                    attempts,
                    reason,
                    batch: true,
                });
                Flow::Halt
            }
        }
    }

    /// Write the position: the write moves the broker's clock, and the next read shows it.
    async fn tick(&mut self, now: f64) -> std::result::Result<(), FeedErr> {
        self.last_tick_at = now;
        self.count("ticks", 1);
        self.commit().await?;
        Ok(())
    }

    // ---- commit ------------------------------------------------------------------

    /// What this stream is (about.rs), for `queen-feed` / `<queryId>#about`:
    /// made once per process.
    fn about_value(&mut self) -> Option<Value> {
        if self.about.is_none() {
            let v = {
                let p = self.p();
                super::about::describe_feed_stream(&super::about::AboutInput {
                    steps: &self.steps,
                    pipeline: &p,
                    source: &self.source,
                    pushdown: &self.pushdown,
                    hash: &self.hash,
                    terminal: self.terminal.as_ref(),
                    query_id: &self.sh.query_id,
                    shard: self.sh.shard,
                    shard_by_fn: matches!(self.o.shard_by, ShardBy::Key(_)),
                    poison: &self.o.on_poison,
                    exclusive: self.o.exclusive,
                    lock_ttl: &self.o.lock_ttl,
                    position_commit_millis: self.o.position_commit_millis,
                })
            };
            self.about = Some(Some(v));
        }
        self.about.clone().flatten()
    }

    /// The clock a commit records: in the middle of a close, the time the close runs up to.
    fn record_clock(&self) -> f64 {
        match self.burst_target {
            None => self.clock,
            Some(b) => self.clock.max(b),
        }
    }

    /// The snapshot the commit at `next` names.
    async fn snapshot_for(&mut self, next: Value) -> std::result::Result<SnapFor, FeedErr> {
        let (changed, empty, metas, touched, keys) = {
            let p = self.p();
            (
                p.changed_since_snapshot(),
                p.empty(),
                p.metas(),
                p.touched_since_snapshot(),
                p.state_keys(),
            )
        };
        let prev = self.snap.clone();
        let clock = self.record_clock();
        if let (Some(prev), false) = (&prev, changed) {
            let mut moved = prev.as_object().cloned().unwrap_or_default();
            moved.insert("position".into(), next);
            moved.insert("clock".into(), js::num(clock));
            moved.insert("metas".into(), metas);
            moved.insert("units".into(), json!(self.units));
            return Ok(SnapFor {
                pointer: Value::Object(moved),
                wrote: false,
                moved: true,
                full: false,
                replaced: Vec::new(),
                keys: Vec::new(),
                millis: 0,
            });
        }
        let layers = layers_of(prev.as_ref());
        if empty {
            return Ok(SnapFor {
                pointer: pointer_of(&[], next, clock, metas, Some(self.units)),
                wrote: true,
                moved: false,
                full: false,
                replaced: layers,
                keys: Vec::new(),
                millis: 0,
            });
        }
        // Due on the wall clock, after so many messages, at a stop, or with many skips.
        let due = prev.is_none()
            || self.stopping()
            || self.now() - self.snap_wall_at >= self.o.snapshot_every_millis
            || self.entries_since_snap >= self.o.snapshot_max_entries
            || self.skips.len() + self.pending_skips.len() > MAX_SKIPS;
        if !due {
            return Ok(SnapFor {
                pointer: prev.unwrap_or(Value::Null),
                wrote: false,
                moved: false,
                full: false,
                replaced: Vec::new(),
                keys: Vec::new(),
                millis: 0,
            });
        }
        let started = Instant::now();
        let base = layers.first().cloned();
        let delta_bytes: u64 = layers.iter().skip(1).map(|l| l.bytes).sum();
        let full = match &base {
            None => true,
            Some(b) => {
                b.v1 || layers.len() >= MAX_LAYERS || delta_bytes >= b.bytes || touched >= keys
            }
        };
        let sid = new_snapshot_id();
        let (chunks, bytes) = self.write_layer(&sid, full).await?;
        let layer = Layer {
            sid: sid.clone(),
            chunks,
            bytes: bytes as u64,
            full,
            v1: false,
        };
        let named: Vec<Layer> = if full {
            vec![layer.clone()]
        } else if chunks > 0 {
            let mut l = layers.clone();
            l.push(layer.clone());
            l
        } else {
            layers.clone()
        };
        Ok(SnapFor {
            pointer: pointer_of(&named, next, clock, metas, Some(self.units)),
            wrote: true,
            moved: false,
            full,
            replaced: if full { layers } else { Vec::new() },
            keys: layer_keys(&self.sh.id, &[layer]),
            millis: started.elapsed().as_millis() as u64,
        })
    }

    /// Write layer `sid`: every entry of every step (`full`), or the keys
    /// touched since the last snapshot. `(chunks, bytes)`.
    async fn write_layer(
        &self,
        sid: &str,
        full: bool,
    ) -> std::result::Result<(usize, usize), FeedErr> {
        let id = self.sh.id.clone();
        let mut w = LayerWriter::new(
            &self.queen,
            POSITION_NAMESPACE,
            &id,
            sid,
            self.lock.as_ref(),
            self.o.snapshot_chunk_chars,
            self.o.snapshot_piece_bytes,
        );
        let room = w.room;
        let n_steps = self.p().stateful_states().len();
        for s in 0..n_steps {
            let mut cursor = 0usize;
            loop {
                // One piece's entries, read under the pipeline's lock; nothing
                // changes the state while the loop is busy writing it.
                let (kind, buf, next, done, count) = {
                    let p = self.p();
                    let states = p.stateful_states();
                    let (kind, st) = states[s];
                    let mut buf = PieceBuf::default();
                    let mut count = 0usize;
                    let mut i = cursor;
                    let mut full_piece = false;
                    if full {
                        let map = st.map();
                        while i < map.slot_count() {
                            let e = map.entry_at(i);
                            i += 1;
                            if let Some((k, v)) = e {
                                count += 1;
                                if buf.add(k, Some(v), room) {
                                    full_piece = true;
                                    break;
                                }
                            }
                        }
                        (kind, buf, i, !full_piece || i >= map.slot_count(), count)
                    } else {
                        let keys = st.touched_keys();
                        while i < keys.len() {
                            let k = &keys[i];
                            i += 1;
                            count += 1;
                            if buf.add(k, st.get(k), room) {
                                full_piece = true;
                                break;
                            }
                        }
                        (kind, buf, i, !full_piece || i >= keys.len(), count)
                    }
                };
                w.entries += count;
                if !buf.is_empty() {
                    w.piece(s, kind, &buf.set, &buf.del).await?;
                }
                cursor = next;
                if done {
                    break;
                }
            }
        }
        let (chunks, bytes, _) = w.finish().await?;
        Ok((chunks, bytes))
    }

    /// One transaction: the outputs, the position (and the snapshot it names).
    async fn commit(&mut self) -> std::result::Result<Flow, FeedErr> {
        let next = match self.pending.next {
            Some(n) => FeedFrom::Position(n),
            None => self.position,
        };
        let stateful = self.p().has_state();
        let snap = if stateful {
            match self.snapshot_for(next.to_value()).await {
                Ok(s) => Some(s),
                Err(e) => {
                    // Chunks of this attempt may be in KV, named by nothing.
                    self.had_stray = true;
                    return Err(e);
                }
            }
        } else {
            None
        };
        let r = self.commit_with(next, stateful, snap.as_ref()).await;
        let ok = matches!(r, Ok(Flow::Ok));
        if !ok {
            if let Some(s) = &snap {
                if s.wrote && !s.keys.is_empty() {
                    self.had_stray = true;
                }
            }
        }
        r
    }

    async fn commit_with(
        &mut self,
        next: FeedFrom,
        stateful: bool,
        snap: Option<&SnapFor>,
    ) -> std::result::Result<Flow, FeedErr> {
        let mut tx = self.queen.transaction();
        if let Some(l) = &self.lock {
            tx = tx.guard(l);
        }
        // Grouped by (queue, partition), in the order each group first appears.
        let mut order: Vec<(String, String)> = Vec::new();
        let mut groups: HashMap<(String, String), Vec<&Output>> = HashMap::new();
        for it in &self.pending.outputs {
            let k = (it.queue.clone(), it.partition.clone());
            if !groups.contains_key(&k) {
                order.push(k.clone());
            }
            groups.entry(k).or_default().push(it);
        }
        for k in &order {
            for it in &groups[k] {
                tx = tx
                    .push_item(
                        queen_protocol::TxnPushItem::new(it.queue.clone(), it.data.clone())
                            .partition(it.partition.clone())
                            .transaction_id(it.transaction_id.clone()),
                    )
                    .map_err(FeedErr::from)?;
            }
        }
        let mut value = Map::new();
        value.insert("position".into(), next.to_value());
        value.insert("queryId".into(), json!(self.sh.query_id));
        value.insert("epoch".into(), json!(self.epoch));
        value.insert("updatedAt".into(), json!(js::iso(self.now())));
        if let Some((index, count)) = self.sh.shard {
            value.insert("shard".into(), json!({ "index": index, "count": count }));
        }
        if let Some(t) = &self.reset_token {
            value.insert("resetToken".into(), json!(t));
        }
        let recorded = self.record_clock();
        if let (true, Some(s)) = (stateful, snap) {
            value.insert("hash".into(), json!(self.hash));
            value.insert("clock".into(), js::num(recorded));
            value.insert("units".into(), json!(self.units));
            value.insert("snapshot".into(), s.pointer.clone());
        }
        // The skips since the snapshot this record names (none past a new one).
        let skips: Vec<String> = match (stateful, snap) {
            (true, Some(s)) if !(s.wrote || s.moved) => {
                let mut v = self.skips.clone();
                v.extend(self.pending_skips.iter().cloned());
                v
            }
            _ => Vec::new(),
        };
        if !skips.is_empty() {
            value.insert("skips".into(), json!(skips));
        }
        // For a reader of the record (the console): the feed time the position
        // is at, whether the read was caught up there, what each step holds,
        // and why it stopped.
        let read_to = self.pending.read_to.unwrap_or(self.committed_read_to);
        let caught_up = self
            .pending
            .caught_up
            .unwrap_or(next == self.position && self.committed_caught_up);
        if read_to > 0.0 {
            value.insert("readTo".into(), js::num(read_to));
        }
        value.insert("caughtUp".into(), json!(caught_up));
        if stateful {
            let (open, held) = self.p().open_and_held();
            value.insert("open".into(), open);
            if let Some(h) = held {
                value.insert("held".into(), h);
            }
        }
        if let Some(m) = &self.stop_mark {
            value.insert("stopped".into(), m.clone());
        }
        // Every shard writes the same description, under the lock like the rest of the commit.
        if self.about_owed {
            if let Some(about) = self.about_value() {
                tx = tx.kv(KvOperation::put(
                    POSITION_NAMESPACE,
                    super::about::about_key(&self.sh.query_id),
                    about,
                    Expiry::Forever,
                )
                .map_err(FeedErr::msg)?);
            }
        }
        // A layout another deploy wrote since this process started stops it here.
        tx = tx.kv(KvOperation::check(
            POSITION_NAMESPACE,
            format!("{}#layout", self.sh.query_id),
            self.layout_version,
        )
        .required());
        let value = Value::Object(value);
        let put = if self.version > 0 {
            KvOperation::put(
                POSITION_NAMESPACE,
                self.sh.id.clone(),
                value,
                Expiry::Forever,
            )
            .map_err(FeedErr::msg)?
            .expect(self.version)
            .required()
        } else {
            KvOperation::put_if_absent(
                POSITION_NAMESPACE,
                self.sh.id.clone(),
                value,
                Expiry::Forever,
            )
            .map_err(FeedErr::msg)?
            .required()
        };
        tx = tx.kv(put);
        let res = match tx.commit_outcome().await.map_err(FeedErr::from)? {
            TxOutcome::LockNotHeld(n) => return Err(FeedErr::lock_not_held(&n)),
            TxOutcome::Answered(r) => r,
        };
        if !res.success {
            match res.reason.as_deref() {
                // A pushed output is already stored: its id is made from what it
                // comes from, so another process committed these messages, or
                // this one did and its answer was lost. Carry on from the stored position.
                Some("duplicate") => {
                    self.reload("duplicate").await?;
                    return Ok(Flow::Abandon);
                }
                Some("kv_precondition") => {
                    // The guard lost (the lock is somebody else's now), or the
                    // position moved. Nothing of this commit was written.
                    if self.lock.as_ref().is_some_and(|l| !l.held()) {
                        self.stand_down("guard");
                        return Ok(Flow::Abandon);
                    }
                    self.layout_moved().await?;
                    self.reload("position").await?;
                    return Ok(Flow::Abandon);
                }
                other => {
                    let mut e = FeedErr::msg(
                        res.error
                            .clone()
                            .unwrap_or_else(|| "Transaction failed".into()),
                    );
                    e.reason = other.map(str::to_string);
                    return Err(e);
                }
            }
        }
        let version = version_of(&res, &self.sh.id);
        let committed_outputs = self.pending.outputs.len() as u64;
        self.position = next;
        self.committed_clock = recorded;
        self.committed_units = self.units;
        self.committed_read_to = read_to;
        self.committed_caught_up = caught_up;
        self.about_owed = false;
        if let (true, Some(s)) = (stateful, snap) {
            self.snap = Some(s.pointer.clone());
            if s.wrote || s.moved {
                self.p().snapshot_taken();
                self.entries_since_snap = 0;
                self.snap_wall_at = self.now();
            }
            self.skip_set = skips.iter().cloned().collect();
            self.skips = skips;
            self.pending_skips.clear();
            if s.wrote {
                let layers = layers_of(Some(&s.pointer));
                let mut c = self.sh.c();
                c.add("snapshots", 1);
                if s.full {
                    c.add("snapshotsFull", 1);
                }
                c.set(
                    "snapshotBytes",
                    s.pointer.get("bytes").cloned().unwrap_or(json!(0)),
                );
                c.set(
                    "snapshotChunks",
                    s.pointer.get("chunks").cloned().unwrap_or(json!(0)),
                );
                c.set("snapshotLayers", json!(layers.len()));
                c.set("snapshotMillis", json!(s.millis));
                drop(c);
                // The layers the new pointer does not name: nobody reads them again.
                if !s.replaced.is_empty() {
                    self.old_keys.extend(layer_keys(&self.sh.id, &s.replaced));
                }
            }
            self.p().commit_point();
        }
        self.pending = fresh_pending();
        let now = self.now();
        self.last_commit_at = now;
        self.last_position_commit = now;
        {
            let mut c = self.sh.c();
            c.add("commits", 1);
            c.add("outputs", committed_outputs);
            c.set("position", next.to_value());
        }
        match version {
            Some(v) => self.version = v,
            None => self.refresh_version().await?,
        }
        if let (true, Some(s)) = (stateful, snap) {
            if s.wrote && self.had_stray && self.lock.is_some() {
                // Holding the lock, this process is the only writer: every chunk
                // not of the snapshot just named is left over.
                self.had_stray = false;
                let keep: HashSet<String> = layers_of(Some(&s.pointer))
                    .into_iter()
                    .map(|l| l.sid)
                    .collect();
                match stray_chunk_keys(&self.queen, POSITION_NAMESPACE, &self.sh.id, &keep).await {
                    Ok(k) => self.old_keys.extend(k),
                    Err(e) => {
                        tracing::warn!(query_id = %self.sh.id, error = %e.message, "FeedRunner.snapshotStray")
                    }
                }
            }
        }
        if !self.old_keys.is_empty() {
            self.drop_old_keys().await;
        }
        self.publish();
        Ok(Flow::Ok)
    }

    /// Delete what no record names any more (best effort).
    async fn drop_old_keys(&mut self) {
        let keys = std::mem::take(&mut self.old_keys);
        if let Err(e) =
            delete_keys(&self.queen, POSITION_NAMESPACE, &keys, self.lock.as_ref()).await
        {
            tracing::warn!(query_id = %self.sh.id, keys = keys.len(), error = %e.message, "FeedRunner.snapshotCleanup");
        }
    }

    async fn refresh_version(&mut self) -> std::result::Result<(), FeedErr> {
        let got = self.kv_get(&self.sh.id.clone()).await?;
        if got.found() {
            if let Some(v) = got.version.filter(|v| *v != 0) {
                self.version = v;
            }
        }
        Ok(())
    }

    // ---- load --------------------------------------------------------------------

    /// Lost a commit to another one: take the stored position and state, read on from there.
    async fn reload(&mut self, why: &str) -> std::result::Result<(), FeedErr> {
        self.count("conflicts", 1);
        self.rollback();
        if let Err(e) = self.load(false).await {
            // Half loaded is not loaded: the next cycle loads all of it again.
            self.set_active(false);
            return Err(e);
        }
        tracing::debug!(query_id = %self.sh.id, why, version = self.version, "FeedRunner.conflict");
        Ok(())
    }

    /// The position, and the state as of it: the snapshot the record names,
    /// then the feed read again from the snapshot's position to the committed one.
    // Boxed, with its type written out: a load replays, a replay's close may
    // commit, and a commit may reload.
    fn load(&mut self, first: bool) -> LoadFuture<'_> {
        Box::pin(self.load_inner(first))
    }

    async fn load_inner(&mut self, first: bool) -> std::result::Result<(), FeedErr> {
        let qid = self.sh.id.clone();
        self.initial_snap = false;
        self.old_keys.clear();
        let row = self.kv_get(&qid).await?;
        let meta = row.value.clone().filter(|v| row.found() && v.is_object());
        let stored = meta.clone().filter(|m| js::is_integer(m.get("position")));
        self.version = if row.found() {
            row.version.unwrap_or(0)
        } else {
            0
        };
        let stored_token = stored
            .as_ref()
            .and_then(|s| s.get("resetToken"))
            .and_then(|t| t.as_str())
            .map(str::to_string);
        let start_over = (first && self.o.reset == Reset::Always)
            || matches!(&self.o.reset, Reset::Token(t) if stored.is_some() && stored_token.as_deref() != Some(t.as_str()));
        self.skips = match (&stored, start_over) {
            (Some(s), false) => s
                .get("skips")
                .and_then(|x| x.as_array())
                .map(|a| a.iter().map(|v| js::string_of(Some(v))).collect())
                .unwrap_or_default(),
            _ => Vec::new(),
        };
        self.skip_set = self.skips.iter().cloned().collect();
        self.pending_skips.clear();
        self.reset_token = match &self.o.reset {
            Reset::Token(t) => Some(t.clone()),
            _ => stored_token.clone(),
        };
        let has_state = self.p().has_state();
        let mut is_new = false;
        match (&stored, start_over) {
            (Some(s), false) => {
                self.epoch = js::int_of(s.get("epoch")).unwrap_or(0);
                self.position =
                    FeedFrom::Position(js::as_num(s.get("position")).unwrap_or(0.0) as u64);
                self.clock = js::finite(s.get("clock")).unwrap_or(0.0);
                if has_state {
                    let s = s.clone();
                    self.load_state(&s).await?;
                }
            }
            _ => {
                is_new = true;
                self.epoch = match &stored {
                    Some(s) => js::int_of(s.get("epoch")).unwrap_or(0) + 1,
                    None => match meta.as_ref().and_then(|m| js::int_of(m.get("epoch"))) {
                        Some(e) => e + 1,
                        None => 0,
                    },
                };
                self.position =
                    self.o
                        .from
                        .or(self.source.from)
                        .unwrap_or(if self.source.since.is_some() {
                            FeedFrom::Earliest
                        } else {
                            FeedFrom::Now
                        });
                self.clock = 0.0;
                if has_state {
                    self.p().wipe();
                }
                // What the stored record kept is nobody's now.
                if let Some(s) = &stored {
                    if let Some(snap) = s.get("snapshot") {
                        self.old_keys
                            .extend(layer_keys(&qid, &layers_of(Some(snap))));
                    }
                    if let Some(b) = js::int_of(s.get("buckets")) {
                        for i in 0..b.max(0) {
                            self.old_keys.push(format!("{qid}#{i}"));
                        }
                    }
                }
                self.snap = None;
                if start_over {
                    tracing::debug!(query_id = %qid, epoch = self.epoch, "FeedRunner.reset");
                }
            }
        }
        self.committed_clock = self.clock;
        self.committed_read_to = match (&stored, is_new) {
            (Some(s), false) => js::finite(s.get("readTo")).unwrap_or(self.clock),
            _ => self.clock,
        };
        self.committed_caught_up = false;
        if is_new {
            self.units = 0;
        }
        self.committed_units = self.units;
        if is_new {
            self.entries_since_snap = 0;
        }
        self.snap_wall_at = self.now();
        self.pending = fresh_pending();
        // Snapshot attempts that died between their chunks and their commit
        // leave chunks nobody names: looked for at the next snapshot.
        if has_state {
            self.had_stray = true;
        }
        if is_new {
            // 'now' is the position when the stream starts, not when its first read comes.
            if self.position == FeedFrom::Now && self.source.since.is_none() {
                let answer = self.reader().probe().await.map_err(FeedErr::from)?;
                let next = answer
                    .get("next")
                    .and_then(|n| n.as_u64())
                    .ok_or_else(|| FeedErr::msg("feed: a probe without next"))?;
                self.position = FeedFrom::Position(next);
            }
            let initial_all = self.p().tables_initial_all();
            if initial_all && self.position != FeedFrom::Earliest {
                self.prelude().await?;
            }
            // Empty where it starts (a table's first rows aside, which need a real snapshot).
            self.initial_snap = {
                let p = self.p();
                p.has_state() && p.empty()
            };
            match self.position {
                FeedFrom::Position(_) => {
                    let pos = self.position;
                    self.reader().seek(pos);
                }
                _ if self.source.since.is_some() => {
                    self.reader().close();
                    self.reader = Some(
                        self.make_reader(self.position, self.source.since.clone(), false)
                            .map_err(FeedErr::from)?,
                    );
                }
                pos => self.reader().seek(pos),
            }
        } else {
            let pos = self.position;
            self.reader().seek(pos);
        }
        self.publish();
        Ok(())
    }

    async fn load_state(&mut self, stored: &Value) -> std::result::Result<(), FeedErr> {
        let qid = self.sh.id.clone();
        let snap = stored.get("snapshot").filter(|s| s.is_object()).cloned();
        let stored_position = js::as_num(stored.get("position")).unwrap_or(0.0);
        let snap_position = snap.as_ref().and_then(|s| js::int_of(s.get("position")));
        let has_old_state = snap.as_ref().is_some_and(|s| {
            !layers_of(Some(s)).is_empty()
                || snap_position.is_some_and(|p| (p as f64) < stored_position)
        }) || js::is_integer(stored.get("buckets"));
        if has_old_state {
            if let Some(h) = stored.get("hash").and_then(|h| h.as_str()) {
                if h != self.hash {
                    return Err(FeedErr::fatal(
                        format!(
                            "the steps of {} changed since it last ran, and its state was kept by the old chain: \
                             run it with {{ reset: true }} (or a reset string) to start it over, or give it a new queryId",
                            self.sh.query_id
                        ),
                        "feed_chain_changed",
                    ));
                }
            }
        }
        if let Some(snap) = snap {
            let layers = layers_of(Some(&snap));
            let loaded: std::result::Result<(), FeedErr> = async {
                if layers.first().is_some_and(|l| l.v1) {
                    // Format 1: one JSON of the whole state across the chunks of one id.
                    let chunks = read_chunks(
                        &self.queen,
                        POSITION_NAMESPACE,
                        &qid,
                        &layers[0].sid,
                        layers[0].chunks,
                    )
                    .await?;
                    let dump = decode_snapshot(&chunks)?;
                    self.p().load(Some(&dump), snap.get("metas"));
                } else {
                    // Format 2: every layer in order, piece by piece.
                    self.p().load_begin();
                    for l in &layers {
                        let sh = Arc::clone(&self.sh);
                        let mut on_piece = move |pc: super::snapshot::Piece| {
                            sh.p().load_piece(pc.s, &pc.k, &pc.set, &pc.del);
                        };
                        read_layer(
                            &self.queen,
                            POSITION_NAMESPACE,
                            &qid,
                            &l.sid,
                            l.chunks,
                            &mut on_piece,
                        )
                        .await?;
                    }
                    super::pipeline::load_end_in_batches(&self.sh.pipeline, snap.get("metas"))
                        .await;
                }
                Ok(())
            }
            .await;
            if let Err(err) = loaded {
                // Another process replaced the snapshot while this one read it
                // (exclusive: false): load again, from the record as it is now.
                let now = self.kv_get(&qid).await?;
                let sids = |x: Option<&Value>| {
                    layers_of(x)
                        .iter()
                        .map(|l| l.sid.clone())
                        .collect::<Vec<_>>()
                        .join(",")
                };
                let now_snap = now.value.as_ref().and_then(|v| v.get("snapshot"));
                if err.code.as_deref() == Some("feed_snapshot_missing")
                    && now.found()
                    && sids(now_snap) != sids(Some(&snap))
                {
                    return Err(FeedErr::coded(
                        format!(
                            "the snapshot of {qid} was replaced while it loaded; loading again"
                        ),
                        "feed_snapshot_moved",
                    ));
                }
                return Err(err);
            }
            // The closes the state holds (the snapshot's count), and how many the
            // stored record says came by its position: the replay stops there.
            let snap_units = js::int_of(snap.get("units"))
                .filter(|u| *u >= 0)
                .map(|u| u as u64);
            self.units = snap_units.unwrap_or(0);
            let cap = match (snap_units, js::int_of(stored.get("units"))) {
                (Some(_), Some(u)) => Some(u.max(0) as u64),
                _ => None,
            };
            let snap_clock = js::finite(snap.get("clock")).unwrap_or(0.0);
            self.snap = Some(snap.clone());
            let stored_pos = stored_position as u64;
            let behind = snap_position.is_some_and(|p| (p as f64) < stored_position);
            if behind || cap.is_some_and(|c| c > self.units) || snap_clock < self.clock {
                let from = snap_position.map(|p| p as u64).unwrap_or(stored_pos);
                let clock = self.clock;
                self.entries_since_snap = self
                    .replay(from, stored_pos, snap_clock, clock, cap)
                    .await?;
            }
        } else if let Some(b) = js::int_of(stored.get("buckets")) {
            // A record of the bucket format (before snapshots): loaded once.
            let keys: Vec<String> = (0..b.max(0)).map(|i| format!("{qid}#{i}")).collect();
            let rows = if keys.is_empty() {
                Vec::new()
            } else {
                self.queen
                    .kv()
                    .get_many(POSITION_NAMESPACE, keys.clone())
                    .await
                    .map_err(FeedErr::from)?
                    .rows()
                    .to_vec()
            };
            let mut entries: Vec<Value> = Vec::new();
            for row in rows {
                if let Some(Value::Object(e)) = row.value.as_ref().and_then(|v| v.get("e")) {
                    for (k, v) in e {
                        entries.push(json!([k, v]));
                    }
                }
            }
            let metas =
                json!([{ "watermark": stored.get("watermark").cloned().unwrap_or(Value::Null) }]);
            {
                let mut p = self.p();
                let (name, kind) = {
                    let names = p.step_names();
                    let kinds = p.stateful_states();
                    (
                        names.first().cloned().unwrap_or_default(),
                        kinds.first().map(|k| k.0).unwrap_or(""),
                    )
                };
                let dump = json!({ "steps": [{ "name": name, "kind": kind, "entries": entries }] });
                p.load(Some(&dump), Some(&metas));
            }
            self.snap = None;
            self.old_keys.extend(keys);
        } else {
            self.p().load(None, None);
            self.snap = None;
        }
        Ok(())
    }

    /// The feed from `from` (a snapshot's position) to `to` (the committed
    /// one) once more, as it ran, every result dropped.
    async fn replay(
        &mut self,
        from: u64,
        to: u64,
        snap_clock: f64,
        committed_clock: f64,
        cap: Option<u64>,
    ) -> std::result::Result<usize, FeedErr> {
        let started = Instant::now();
        let mut reader = self
            .make_reader(FeedFrom::Position(from), None, false)
            .map_err(FeedErr::from)?;
        self.replaying = true;
        self.replay_cap = cap;
        self.clock = snap_clock;
        let mut n = 0usize;
        let r: std::result::Result<(), FeedErr> = async {
            let deadline = Instant::now() + Duration::from_millis(REPLAY_CATCH_UP_MILLIS);
            let at = |r: &FeedReader| match r.position() {
                FeedFrom::Position(p) => p,
                _ => 0,
            };
            while at(&reader) < to {
                let page = reader.read(Some(100)).await.map_err(FeedErr::from)?;
                for g in groups(&page.entries, Some(to)) {
                    self.process_group(&g, None).await?;
                    n += g.entries.len();
                }
                if page.caught_up && at(&reader) < to && Instant::now() > deadline {
                    return Err(FeedErr::msg(format!(
                        "FeedRunner: the follower read has not reached the committed position {to} (at {}) after {REPLAY_CATCH_UP_MILLIS} ms",
                        at(&reader)
                    )));
                }
            }
            self.clock = self.clock.max(committed_clock);
            let clock = self.clock;
            self.advance_to(clock, None).await?;
            Ok(())
        }
        .await;
        self.replaying = false;
        self.replay_cap = None;
        reader.close();
        r?;
        if let Some(c) = cap {
            if self.units != c {
                tracing::warn!(query_id = %self.sh.id, replayed = self.units, stored = c, "FeedRunner.replayUnits");
            }
        }
        self.p().commit_point();
        {
            let mut c = self.sh.c();
            c.add("replays", 1);
            c.add("replayEntries", n as u64);
            c.add("replayMillis", started.elapsed().as_millis() as u64);
        }
        Ok(n)
    }

    /// A new stream with a table: the table queue from its beginning up to
    /// where the stream starts, rows only.
    async fn prelude(&mut self) -> std::result::Result<(), FeedErr> {
        let start = match self.position {
            FeedFrom::Position(p) => p,
            _ => {
                if let Some(since) = self.source.since.clone() {
                    let mut probe = self
                        .make_reader(FeedFrom::Earliest, Some(since), false)
                        .map_err(FeedErr::from)?;
                    let page = probe.read(Some(0)).await.map_err(FeedErr::from);
                    probe.close();
                    let page = page?;
                    page.entries.first().map_or(page.next, |e| e.position)
                } else {
                    let answer = self.reader().probe().await.map_err(FeedErr::from)?;
                    answer.get("next").and_then(|n| n.as_u64()).unwrap_or(0)
                }
            }
        };
        self.position = FeedFrom::Position(start);
        let mut reader = self
            .make_reader(FeedFrom::Earliest, None, true)
            .map_err(FeedErr::from)?;
        let mut rows = 0u64;
        let r: std::result::Result<(), FeedErr> = async {
            loop {
                match reader.position() {
                    FeedFrom::Position(p) if p >= start => break,
                    _ => {}
                }
                let page = reader.read(Some(0)).await.map_err(FeedErr::from)?;
                for e in &page.entries {
                    if e.position >= start {
                        break;
                    }
                    self.p().apply_row(e.shape()).map_err(FeedErr::msg)?;
                    rows += 1;
                }
                if page.caught_up {
                    break;
                }
            }
            Ok(())
        }
        .await;
        reader.close();
        r?;
        self.count("tableRows", rows);
        // The rows are the starting point: a rollback keeps them, and the first commit snapshots them.
        self.p().commit_point();
        if rows > 0 {
            self.pending.work += 1;
        }
        tracing::debug!(query_id = %self.sh.id, rows, start, "FeedRunner.tablePrelude");
        Ok(())
    }
}

/// The identity of a unit, the same on a replay.
fn skip_id(subject: &Subject<'_>) -> String {
    match subject {
        Subject::Entry(e) => {
            let v = e.shape();
            let s = |k: &str| js::string_of(v.get(k));
            format!(
                "e\u{1f}{}\u{1f}{}\u{1f}{}\u{1f}{}",
                s("position"),
                s("queue"),
                s("partition"),
                s("offset")
            )
        }
        Subject::Emit(em) => {
            let time = em.time.map(js::number_text).unwrap_or_default();
            let kind = match em.ctx.get("kind") {
                None | Some(Value::Null) => String::new(),
                k => js::string_of(k),
            };
            let id = format!("r\u{1f}{}\u{1f}{}\u{1f}{time}\u{1f}{kind}", em.step, em.key);
            // A message an event-time step held: its place in the step's order.
            match &em.id {
                Some(h) if !h.is_empty() => format!("{id}\u{1f}{h}"),
                _ => id,
            }
        }
    }
}

fn version_of(res: &queen_protocol::TransactionResponse, key: &str) -> Option<i64> {
    res.kv_results()
        .into_iter()
        .find(|r| r.key.as_deref() == Some(key) && r.version.is_some())
        .and_then(|r| r.version)
}

/// A pushed payload is an object: anything else travels as `{ value }`.
pub(crate) fn payload_of(value: Option<&Value>) -> Value {
    match value {
        Some(v @ Value::Object(_)) | Some(v @ Value::Array(_)) => v.clone(),
        Some(v) => json!({ "value": v }),
        None => json!({}),
    }
}

fn require_queue(name: Option<&Value>) -> std::result::Result<String, FeedErr> {
    match name {
        Some(Value::String(s)) if !s.is_empty() => Ok(s.clone()),
        other => Err(FeedErr::msg(format!(
            ".to(): a queue name is a non-empty string, got {}",
            other
                .map(js::stringify)
                .unwrap_or_else(|| "undefined".into())
        ))),
    }
}

fn extract_queue_name(t: &Value) -> std::result::Result<String, FeedErr> {
    if let Value::String(_) = t {
        return require_queue(Some(t));
    }
    for k in ["name", "_queueName", "queueName"] {
        if let Some(Value::String(s)) = t.get(k) {
            if k != "name" || !s.is_empty() {
                return Ok(s.clone());
            }
        }
    }
    Err(FeedErr::msg(
        "Could not extract queue name from sink target. Pass either a queen.queue(\"name\") builder or { queueName: \"name\" }.",
    ))
}

fn require_partition(p: &Value, queue: &str) -> std::result::Result<String, FeedErr> {
    match p {
        Value::Number(_) => Ok(js::string_of(Some(p))),
        Value::String(s) if !s.is_empty() => Ok(s.clone()),
        other => Err(FeedErr::msg(format!(
            ".to(): the partition for {queue} is a non-empty string, got {}",
            js::stringify(other)
        ))),
    }
}

/// Where one result goes: `[(queue, partition)]`, empty to send it nowhere.
async fn targets_for(
    sink: &FeedSink,
    value: Option<&Value>,
    msg: Value,
    fallback: Option<String>,
) -> std::result::Result<Vec<(String, String)>, FeedErr> {
    let raw: Vec<Value> = match &sink.route {
        Some(route) => match route(value.cloned(), msg).await.map_err(FeedErr::msg)? {
            None | Some(Value::Null) => return Ok(Vec::new()),
            Some(Value::Array(list)) => list,
            Some(one) => vec![one],
        },
        None => sink
            .queues
            .iter()
            .map(|q| Value::String(q.clone()))
            .collect(),
    };
    let mut out = Vec::new();
    for t in raw {
        if t.is_null() {
            continue;
        }
        let own = t.is_object()
            && !t.get("name").is_some_and(|n| n.is_string())
            && t.get("queue").is_some_and(|q| q.is_string());
        let queue = if own {
            require_queue(t.get("queue"))?
        } else {
            extract_queue_name(&t)?
        };
        let own_partition = if own {
            t.get("partition").filter(|p| !p.is_null()).cloned()
        } else {
            None
        };
        let partition = match own_partition {
            Some(p) => p,
            None => match &sink.partition {
                SinkPartition::By(f) => {
                    Value::String(js::string_of(f(value).map_err(FeedErr::msg)?.as_ref()))
                }
                SinkPartition::Fixed(p) => Value::String(p.clone()),
                SinkPartition::Default => match &fallback {
                    None => json!("Default"),
                    Some(f) if f.is_empty() => json!("Default"),
                    Some(f) => json!(f),
                },
            },
        };
        out.push((queue.clone(), require_partition(&partition, &queue)?));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_follow_the_js_formulas() {
        assert!(output_prefix("orders-it").starts_with("fd-"));
        assert_eq!(output_prefix("orders-it").len(), 12);
        let entry = json!({ "position": 5, "queue": "orders", "partition": "c1", "offset": 1 });
        let id = feed_output_id("orders-it", &entry, 0, 0, "orders-it");
        assert_eq!(id.len(), 12 + 40);
        assert_eq!(shard_id("q", Some((1, 3))), "q#shard1of3");
        assert_eq!(lock_name("q", Some((1, 3))), "queen-feed/q/shard-1-of-3");
        assert_eq!(lock_name("q", None), "queen-feed/q");
    }

    #[test]
    fn globs_match_like_the_js_regex() {
        assert!(glob_match("orders*", "orders-it"));
        assert!(glob_match("orders*", "orders"));
        assert!(!glob_match("orders*", "xorders"));
        assert!(glob_match("acme.*", "acme.bookings"));
        assert!(!glob_match("acme.*", "acmeXbookings"));
        assert!(glob_match("a?c", "abc"));
        assert!(!glob_match("a?c", "ac"));
        assert!(glob_match("*", ""));
    }

    #[test]
    fn payloads_are_objects() {
        assert_eq!(payload_of(Some(&json!(3))), json!({ "value": 3 }));
        assert_eq!(payload_of(Some(&json!(null))), json!({ "value": null }));
        assert_eq!(payload_of(None), json!({}));
        assert_eq!(payload_of(Some(&json!({ "a": 1 }))), json!({ "a": 1 }));
    }

    #[test]
    fn query_ids_are_checked() {
        assert!(require_query_id("a#b")
            .unwrap_err()
            .message
            .contains("without '#'"));
        assert!(require_query_id("").is_err());
        assert!(require_query_id("ok").is_ok());
    }
}
