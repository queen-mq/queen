//! The tenant feed: every push to a tenant's queues, in the order the broker's
//! log committed them, read by position (`POST /api/v1/feed`).
//!
//! ```no_run
//! use queen_mq::feed::{FeedFrom, FeedOptions};
//! # async fn example(queen: queen_mq::Queen) -> queen_mq::Result<()> {
//! // What happened to one booking today, then stop.
//! let mut feed = queen.feed(
//!     FeedOptions::new().partitions(["booking-9137"]).since("1d").follow(false),
//! )?;
//! while let Some(e) = feed.next_entry().await? {
//!     println!("{} {} {} {}", e.position, e.queue, e.partition, e.data);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! A position is the broker log entry that wrote a message. One number orders
//! every queue of the tenant, and the pushes of one transaction share theirs.
//! Nothing is leased, acked or moved: the feed is a read, and every consumer
//! group still gets its messages.
//!
//! Followers serve it. A leader answers 421 with the followers to ask, and the
//! reader then reads from those (and goes back to the configured URL if none
//! answers). A single broker serves it itself.
//!
//! Same rules and wire as the JS SDK's `queen.feed()` (feed/Feed.js).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use serde_json::{json, Map, Value};

use crate::error::{Error, Result};
use crate::http::{HttpClient, Opts};
use crate::js;

const FEED_ON_LEADER: &str = "feed_on_leader";
/// The code of the error a broker without the feed route gives.
pub const NO_FEED_ROUTE: &str = "no_feed_route";
const OPS: [&str; 11] = [
    "=", "==", "!=", "<", "<=", ">", ">=", "in", "exists", "prefix", "!prefix",
];
const DEFAULT_WAIT_MILLIS: u64 = 5000;
/// A long poll may hold the request this long; the HTTP timeout must be longer.
const MAX_WAIT_MILLIS: u64 = 30_000;

// ------------------------------------------------------------------ where

/// One `where` condition, checked: `{ field, op, value }`.
///
/// Fields: `queue`, `partition`, `transactionId`, `offset`, `position`,
/// `data`, `data.<path>`. Ops: `= != < <= > >= in exists prefix !prefix`. A
/// field the message does not have holds for no op, `!=` and `!prefix`
/// included.
#[derive(Debug, Clone, PartialEq)]
pub struct WhereCondition {
    pub field: String,
    pub op: String,
    /// Absent for `exists`; `null` when not given for the others.
    pub value: Option<Value>,
}

impl WhereCondition {
    /// `whereCondition(field, op, value)`.
    pub fn new(field: impl Into<String>, op: &str, value: Option<Value>) -> Result<Self> {
        let field = field.into();
        if field.is_empty() {
            return Err(Error::Invalid(format!(
                "where: field must be a non-empty string, got {}",
                js::string_json(&field)
            )));
        }
        if !OPS.contains(&op) {
            return Err(Error::Invalid(format!(
                "where: unknown op {} (= != < <= > >= in exists prefix !prefix)",
                js::string_json(op)
            )));
        }
        if op == "in" && !matches!(value, Some(Value::Array(_))) {
            return Err(Error::Invalid(format!(
                "where: in on {field} needs an array"
            )));
        }
        if (op == "prefix" || op == "!prefix") && !matches!(value, Some(Value::String(_))) {
            return Err(Error::Invalid(format!(
                "where: {op} on {field} needs a string"
            )));
        }
        let value = if op == "exists" {
            None
        } else {
            Some(value.unwrap_or(Value::Null))
        };
        Ok(Self {
            field,
            op: op.to_string(),
            value,
        })
    }

    /// The condition as the broker reads it (and as JS writes it):
    /// `{"field","op","value"}` in that order.
    pub fn to_json_text(&self) -> String {
        let mut out = String::from("{\"field\":");
        js::write_string(&mut out, &self.field);
        out.push_str(",\"op\":");
        js::write_string(&mut out, &self.op);
        if let Some(v) = &self.value {
            out.push_str(",\"value\":");
            js::write_value(&mut out, v);
        }
        out.push('}');
        out
    }

    pub(crate) fn to_value(&self) -> Value {
        let mut m = Map::new();
        m.insert("field".into(), Value::String(self.field.clone()));
        m.insert("op".into(), Value::String(self.op.clone()));
        if let Some(v) = &self.value {
            m.insert("value".into(), v.clone());
        }
        Value::Object(m)
    }

    /// Whether the condition holds for a message (in the feed's shape),
    /// evaluated here: the rules the broker applies to a feed read.
    pub fn holds(&self, msg: &Value) -> bool {
        condition_holds(self, msg)
    }
}

fn field_value<'a>(field: &str, msg: &'a Value) -> Option<&'a Value> {
    // `a ?? b`: b when a is null or missing.
    let either = |a: &str, b: &str| match msg.get(a) {
        None | Some(Value::Null) => msg.get(b),
        v => v,
    };
    match field {
        "queue" => return either("queue", "queueName"),
        "partition" => return either("partition", "partitionName"),
        "transactionId" => return msg.get("transactionId"),
        "offset" => return msg.get("offset"),
        "position" => return msg.get("position"),
        "data" => return msg.get("data"),
        _ => {}
    }
    let path = field.strip_prefix("data.")?;
    let mut cur = msg.get("data")?;
    for key in path.split('.') {
        cur = match cur {
            Value::Array(items) => {
                // `Number(key)`: a whole number indexes; anything else is no item.
                let i: f64 = if key.trim().is_empty() {
                    0.0
                } else {
                    key.trim().parse().ok()?
                };
                if i < 0.0 || i.fract() != 0.0 {
                    return None;
                }
                items.get(i as usize)?
            }
            Value::Object(map) => map.get(key)?,
            _ => return None,
        };
    }
    Some(cur)
}

fn json_eq_where(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Number(_), Value::Number(_)) => js::json_eq(a, b),
        (Value::Array(_) | Value::Object(_), Value::Array(_) | Value::Object(_)) => {
            js::stringify(a) == js::stringify(b)
        }
        _ => a == b,
    }
}

fn compare(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.as_f64()?.partial_cmp(&y.as_f64()?),
        (Value::String(x), Value::String(y)) => Some(js::str_cmp(x, y)),
        _ => None,
    }
}

/// `conditionHolds(cond, msg)` (feed/Feed.js).
pub(crate) fn condition_holds(cond: &WhereCondition, msg: &Value) -> bool {
    let Some(v) = field_value(&cond.field, msg) else {
        return false;
    };
    let null = Value::Null;
    let want = cond.value.as_ref().unwrap_or(&null);
    use std::cmp::Ordering::*;
    match cond.op.as_str() {
        "exists" => true,
        "=" | "==" => json_eq_where(v, want),
        "!=" => !json_eq_where(v, want),
        "in" => matches!(want, Value::Array(xs) if xs.iter().any(|x| json_eq_where(v, x))),
        "prefix" => {
            matches!((v, want), (Value::String(s), Value::String(p)) if s.starts_with(p.as_str()))
        }
        "!prefix" => {
            matches!((v, want), (Value::String(s), Value::String(p)) if !s.starts_with(p.as_str()))
        }
        "<" => compare(v, want) == Some(Less),
        "<=" => matches!(compare(v, want), Some(Less | Equal)),
        ">" => compare(v, want) == Some(Greater),
        ">=" => matches!(compare(v, want), Some(Greater | Equal)),
        _ => false,
    }
}

// ------------------------------------------------------------------- since

/// A start time as the broker reads it: a time, epoch milliseconds, an ISO
/// string, or a duration back from now (`"30s"`, `"15m"`, `"2h"`, `"1d"`,
/// `"1w"`).
#[derive(Debug, Clone, PartialEq)]
pub enum Since {
    Text(String),
    Millis(f64),
    Time(SystemTime),
}

impl From<&str> for Since {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}
impl From<String> for Since {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}
impl From<i64> for Since {
    fn from(ms: i64) -> Self {
        Self::Millis(ms as f64)
    }
}
impl From<u64> for Since {
    fn from(ms: u64) -> Self {
        Self::Millis(ms as f64)
    }
}
impl From<f64> for Since {
    fn from(ms: f64) -> Self {
        Self::Millis(ms)
    }
}
impl From<SystemTime> for Since {
    fn from(t: SystemTime) -> Self {
        Self::Time(t)
    }
}

fn since_unit(u: &str) -> Option<f64> {
    Some(match u {
        "ms" => 1.0,
        "s" => 1000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        "w" => 604_800_000.0,
        _ => return None,
    })
}

/// `sinceToIso(since, now)`.
pub fn since_to_iso(since: &Since, now_ms: f64) -> Result<String> {
    let bad = |shown: String| {
        Error::Invalid(format!(
            "feed: since must be a Date, epoch ms, an ISO time or a duration like '15m', got {shown}"
        ))
    };
    match since {
        Since::Time(t) => {
            let ms = match t.duration_since(SystemTime::UNIX_EPOCH) {
                Ok(d) => d.as_millis() as f64,
                Err(e) => -(e.duration().as_millis() as f64),
            };
            js::try_iso(ms).ok_or_else(|| bad("an invalid time".into()))
        }
        Since::Millis(ms) if ms.is_finite() => {
            js::try_iso(*ms).ok_or_else(|| bad(js::number_text(*ms)))
        }
        Since::Millis(ms) => Err(bad(js::number_text(*ms))),
        Since::Text(s) => {
            // /^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h|d|w)\s*$/
            let t = s.trim();
            let digits_end = t
                .char_indices()
                .find(|(_, c)| !(c.is_ascii_digit() || *c == '.'))
                .map(|(i, _)| i)
                .unwrap_or(t.len());
            let (n, rest) = t.split_at(digits_end);
            let number_ok = !n.is_empty()
                && !n.starts_with('.')
                && !n.ends_with('.')
                && n.matches('.').count() <= 1;
            if number_ok {
                if let Some(unit) = since_unit(rest.trim_start()) {
                    let n: f64 = n.parse().map_err(|_| bad(js::string_json(s)))?;
                    return js::try_iso(now_ms - n * unit).ok_or_else(|| bad(js::string_json(s)));
                }
            }
            match js::parse_date(s) {
                Some(ms) => js::try_iso(ms).ok_or_else(|| bad(js::string_json(s))),
                None => Err(bad(js::string_json(s))),
            }
        }
    }
}

// ----------------------------------------------------------------- options

/// Where a read starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FeedFrom {
    /// Only what comes next.
    Now,
    /// The oldest record the queue logs still hold.
    Earliest,
    /// This position, included.
    Position(u64),
}

impl FeedFrom {
    pub(crate) fn to_value(self) -> Value {
        match self {
            Self::Now => Value::String("now".into()),
            Self::Earliest => Value::String("earliest".into()),
            Self::Position(p) => Value::from(p),
        }
    }

    /// `'now'`, `'earliest'` or a whole number from 0.
    pub fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::String(s) if s == "now" => Some(Self::Now),
            Value::String(s) if s == "earliest" => Some(Self::Earliest),
            v if js::is_integer(Some(v)) && js::as_num(Some(v)).unwrap_or(-1.0) >= 0.0 => {
                Some(Self::Position(js::as_num(Some(v)).unwrap_or(0.0) as u64))
            }
            _ => None,
        }
    }
}

/// A shard of the feed: the partitions whose name hashes (FNV-1a, 32 bit,
/// UTF-8) to `index` of `count`; with `queues`, only those queues are split
/// and the others are read whole.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FeedShard {
    pub count: u32,
    pub index: u32,
    pub queues: Option<Vec<String>>,
}

/// The options of [`crate::Queen::feed`] (the JS `queen.feed(options)`).
#[derive(Debug, Clone)]
pub struct FeedOptions {
    pub queues: Option<Vec<String>>,
    pub partitions: Option<Vec<String>>,
    pub conditions: Vec<WhereCondition>,
    pub kinds: Option<Vec<String>>,
    /// First position; default `Now` (`Earliest` with `since`).
    pub from: Option<FeedFrom>,
    pub since: Option<Since>,
    /// Keep waiting for new entries (default); `false` stops once caught up.
    pub follow: bool,
    /// Long poll of each read while caught up (max 30000).
    pub wait_millis: u64,
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
    /// Nodes to read from first, e.g. one follower's URL.
    pub follower_urls: Vec<String>,
    pub shard: Option<FeedShard>,
    /// A `where` that failed its check, surfaced by `Queen::feed`.
    pub(crate) error: Option<String>,
}

impl Default for FeedOptions {
    fn default() -> Self {
        Self {
            queues: None,
            partitions: None,
            conditions: Vec::new(),
            kinds: None,
            from: None,
            since: None,
            follow: true,
            wait_millis: DEFAULT_WAIT_MILLIS,
            max_records: None,
            max_bytes: None,
            follower_urls: Vec::new(),
            shard: None,
            error: None,
        }
    }
}

impl FeedOptions {
    pub fn new() -> Self {
        Self::default()
    }

    /// Names or patterns (`orders*`); default every queue.
    pub fn queues<I, S>(mut self, queues: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.queues = Some(queues.into_iter().map(Into::into).collect());
        self
    }

    pub fn partitions<I, S>(mut self, partitions: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.partitions = Some(partitions.into_iter().map(Into::into).collect());
        self
    }

    /// A condition sent to the broker. `value` is `None` for `exists`.
    pub fn where_(mut self, field: &str, op: &str, value: impl Into<Option<Value>>) -> Self {
        match WhereCondition::new(field, op, value.into()) {
            Ok(c) => self.conditions.push(c),
            Err(e) => {
                if self.error.is_none() {
                    self.error = Some(e.to_string());
                }
            }
        }
        self
    }

    pub fn condition(mut self, cond: WhereCondition) -> Self {
        self.conditions.push(cond);
        self
    }

    pub fn kinds<I, S>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.kinds = Some(kinds.into_iter().map(Into::into).collect());
        self
    }

    pub fn from(mut self, from: FeedFrom) -> Self {
        self.from = Some(from);
        self
    }

    pub fn since(mut self, since: impl Into<Since>) -> Self {
        self.since = Some(since.into());
        self
    }

    pub fn follow(mut self, follow: bool) -> Self {
        self.follow = follow;
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

    pub fn max_bytes(mut self, n: u64) -> Self {
        self.max_bytes = Some(n);
        self
    }

    pub fn follower_urls<I, S>(mut self, urls: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.follower_urls = urls.into_iter().map(Into::into).collect();
        self
    }

    pub fn shard(mut self, shard: FeedShard) -> Self {
        self.shard = Some(shard);
        self
    }
}

// ------------------------------------------------------------------- reads

/// A feed entry: the message in Queen's shape, plus its position.
#[derive(Debug, Clone, PartialEq)]
pub struct FeedEntry {
    pub position: u64,
    pub kind: String,
    pub queue: String,
    pub partition: String,
    pub partition_id: String,
    pub offset: Value,
    pub id: String,
    pub transaction_id: String,
    pub data: Value,
    pub ts: String,
    /// The entry as the broker sent it, in the JS shape (`toEntry`):
    /// `{position, kind, queue, partition, partitionId, offset, id,
    /// transactionId, data, ts, createdAt}`.
    pub(crate) shape: Arc<Value>,
}

impl FeedEntry {
    /// The message as a stream's functions see it (the JS entry object).
    pub fn to_value(&self) -> Value {
        (*self.shape).clone()
    }

    pub(crate) fn shape(&self) -> &Arc<Value> {
        &self.shape
    }

    /// `toEntry(e)`: the broker's entry as the reader hands it out. The
    /// payload is moved into `data`; the message shape holds one copy of it.
    /// `shape_only`: a stream's reader, whose runner reads the message from
    /// the shape only: the payload is moved there and `data` is left null.
    fn of_wire(mut w: WireEntry, shape_only: bool) -> Self {
        let payload = std::mem::take(&mut w.payload);
        let (data, in_shape) = if shape_only {
            (Value::Null, payload)
        } else {
            (payload.clone(), payload)
        };
        let mut m = Map::new();
        m.insert("position".into(), Value::from(w.position));
        m.insert("kind".into(), Value::String(w.kind.clone()));
        m.insert("queue".into(), Value::String(w.queue.clone()));
        m.insert("partition".into(), Value::String(w.partition.clone()));
        m.insert("partitionId".into(), Value::String(w.partition_id.clone()));
        m.insert("offset".into(), w.offset.clone());
        m.insert("id".into(), Value::String(w.id.clone()));
        m.insert(
            "transactionId".into(),
            Value::String(w.transaction_id.clone()),
        );
        m.insert("data".into(), in_shape);
        m.insert("ts".into(), Value::String(w.ts.clone()));
        m.insert("createdAt".into(), Value::String(w.ts.clone()));
        Self {
            position: w.position,
            kind: w.kind,
            queue: w.queue,
            partition: w.partition,
            partition_id: w.partition_id,
            offset: w.offset,
            id: w.id,
            transaction_id: w.transaction_id,
            data,
            ts: w.ts,
            shape: Arc::new(Value::Object(m)),
        }
    }
}

/// One entry as the broker writes it, its payload decoded with it.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireEntry {
    position: u64,
    #[serde(default)]
    kind: String,
    #[serde(default)]
    queue: String,
    #[serde(default)]
    partition: String,
    #[serde(default)]
    partition_id: String,
    #[serde(default)]
    offset: Value,
    #[serde(default)]
    id: String,
    #[serde(default)]
    transaction_id: String,
    #[serde(default)]
    payload: Value,
    #[serde(default)]
    ts: String,
}

/// The answer to a read, as the broker writes it.
#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WireAnswer {
    next: Option<u64>,
    #[serde(default)]
    resolved_ts: Option<String>,
    #[serde(default)]
    caught_up: bool,
    #[serde(default)]
    applied: Option<u64>,
    #[serde(default)]
    entries: Vec<WireEntry>,
}

/// One read: the entries, and where the next one starts.
#[derive(Debug, Clone)]
pub struct FeedPage {
    pub entries: Vec<FeedEntry>,
    pub next: u64,
    pub resolved_ts: Option<String>,
    pub caught_up: bool,
    pub applied: Option<u64>,
}

struct Follower {
    url: String,
    // Shared: a read ahead asks the node that answered last on its own task.
    client: Arc<HttpClient>,
}

/// Reads the tenant feed (`queen.feed(...)`).
pub struct FeedReader {
    http: Arc<HttpClient>,
    queues: Option<Vec<String>>,
    partitions: Option<Vec<String>>,
    conditions: Vec<WhereCondition>,
    kinds: Option<Vec<String>>,
    since: Option<String>,
    follow: bool,
    wait_millis: u64,
    max_records: Option<u64>,
    max_bytes: Option<u64>,
    shard: Option<FeedShard>,
    position: FeedFrom,
    stopped: bool,
    // The nodes a leader named, as clients, tried in order before the configured URL.
    followers: Vec<Follower>,
    follower_at: usize,
    fallback_until: Option<Instant>,
    buffered: VecDeque<FeedEntry>,
    done: bool,
    // A stream's reader: entries carry the payload in their shape only (FeedEntry::of_wire).
    shape_only: bool,
    // The page read while the caller works on the one before: its position, and the read.
    ahead: Option<(u64, tokio::task::JoinHandle<Result<FeedPage>>)>,
    // The follower (its index) or, None, the configured URL that answered the last read.
    last_target: Option<usize>,
}

fn checked_list(v: Option<Vec<String>>, what: &str) -> Result<Option<Vec<String>>> {
    let Some(v) = v else { return Ok(None) };
    if let Some(bad) = v.iter().find(|x| x.is_empty()) {
        return Err(Error::Invalid(format!(
            "feed: {what} must be names (strings), got {}",
            js::string_json(bad)
        )));
    }
    Ok(if v.is_empty() { None } else { Some(v) })
}

fn checked_shard(shard: Option<FeedShard>) -> Result<Option<FeedShard>> {
    let Some(s) = shard else { return Ok(None) };
    if s.count < 1 || s.count > 4096 {
        return Err(Error::Invalid(format!(
            "feed: shard.count is a whole number from 1 to 4096, got {}",
            s.count
        )));
    }
    if s.index >= s.count {
        return Err(Error::Invalid(format!(
            "feed: shard.index is from 0 to {}, got {}",
            s.count - 1,
            s.index
        )));
    }
    Ok(Some(FeedShard {
        count: s.count,
        index: s.index,
        queues: checked_list(s.queues, "shard.queues")?,
    }))
}

impl FeedReader {
    pub(crate) fn new(http: Arc<HttpClient>, options: FeedOptions) -> Result<Self> {
        if let Some(e) = options.error {
            return Err(Error::Invalid(e));
        }
        let since = match &options.since {
            Some(s) => Some(since_to_iso(s, js::now_ms())?),
            None => None,
        };
        let mut followers = Vec::new();
        for url in &options.follower_urls {
            followers.push(Follower {
                url: url.clone(),
                client: Arc::new(http.for_node(url)?),
            });
        }
        let position = options.from.unwrap_or(if options.since.is_some() {
            FeedFrom::Earliest
        } else {
            FeedFrom::Now
        });
        Ok(Self {
            http,
            queues: checked_list(options.queues, "queues")?,
            partitions: checked_list(options.partitions, "partitions")?,
            conditions: options.conditions,
            kinds: options.kinds,
            since,
            follow: options.follow,
            wait_millis: options.wait_millis.min(MAX_WAIT_MILLIS),
            max_records: options.max_records,
            max_bytes: options.max_bytes,
            shard: checked_shard(options.shard)?,
            position,
            stopped: false,
            followers,
            follower_at: 0,
            fallback_until: None,
            buffered: VecDeque::new(),
            done: false,
            shape_only: false,
            ahead: None,
            last_target: None,
        })
    }

    /// Where the next read starts: a position once a read has answered.
    pub fn position(&self) -> FeedFrom {
        self.position
    }

    /// Move the reader (a runner whose commit lost goes back to the committed position).
    pub fn seek(&mut self, position: FeedFrom) {
        self.position = position;
        self.buffered.clear();
        self.drop_ahead();
    }

    /// One read, and the reader moves to `next`. Waits up to `wait_millis`
    /// (default: the option's) for an entry while there is nothing new. A
    /// read whose future is dropped leaves the reader where it was. While the
    /// answers are not caught up, the next page is read at once, on a task of
    /// its own, so it comes in while the caller works on this one.
    pub async fn read(&mut self, wait_millis: Option<u64>) -> Result<FeedPage> {
        let wait = wait_millis.unwrap_or(self.wait_millis).min(MAX_WAIT_MILLIS);
        if let Some((from, task)) = self.ahead.take() {
            if self.position == FeedFrom::Position(from) {
                // A read that failed, or found nothing while caught up (it
                // waited for nothing), is done again here, waiting as asked.
                if let Ok(Ok(page)) = task.await {
                    if !(page.entries.is_empty() && page.caught_up) {
                        self.position = FeedFrom::Position(page.next);
                        self.read_on(&page);
                        return Ok(page);
                    }
                }
            } else {
                task.abort();
            }
        }
        let body = self.body(self.position.to_value(), wait);
        let answer = self
            .post(&body, Duration::from_millis(wait + 10_000))
            .await?;
        let page = parse_page_with(&answer, self.shape_only)?;
        self.position = FeedFrom::Position(page.next);
        self.read_on(&page);
        Ok(page)
    }

    /// After a page that is not caught up: read the next one now, from the
    /// node that answered (no long poll), and decode it off the async threads.
    fn read_on(&mut self, page: &FeedPage) {
        if page.caught_up || self.stopped {
            return;
        }
        let client = match self.last_target {
            Some(i) => match self.followers.get(i) {
                Some(f) => Arc::clone(&f.client),
                None => return,
            },
            None => Arc::clone(&self.http),
        };
        let body = self.body(Value::from(page.next), 0);
        let shape_only = self.shape_only;
        let opts = Opts::default().timeout(Duration::from_millis(10_000));
        let task = tokio::spawn(async move {
            let bytes = client
                .post_bytes("/api/v1/feed", &body, &opts)
                .await?
                .ok_or_else(|| Error::Decode("feed: an empty answer".into()))?;
            tokio::task::spawn_blocking(move || parse_page_with(&bytes, shape_only))
                .await
                .map_err(|e| Error::Decode(format!("feed: the page read ahead: {e}")))?
        });
        self.ahead = Some((page.next, task));
    }

    fn drop_ahead(&mut self) {
        if let Some((_, task)) = self.ahead.take() {
            task.abort();
        }
    }

    /// The body of a read from `from`, waiting up to `wait` ms while nothing is new.
    fn body(&self, from: Value, wait: u64) -> Value {
        let mut body = Map::new();
        body.insert("from".into(), from);
        body.insert("maxWaitMs".into(), Value::from(wait));
        if let Some(q) = &self.queues {
            body.insert("queues".into(), json!(q));
        }
        if let Some(p) = &self.partitions {
            body.insert("partitions".into(), json!(p));
        }
        if !self.conditions.is_empty() {
            body.insert(
                "where".into(),
                Value::Array(self.conditions.iter().map(|c| c.to_value()).collect()),
            );
        }
        if let Some(k) = &self.kinds {
            body.insert("kinds".into(), json!(k));
        }
        if let Some(s) = &self.since {
            body.insert("since".into(), Value::String(s.clone()));
        }
        if let Some(n) = self.max_records {
            body.insert("maxRecords".into(), Value::from(n));
        }
        if let Some(n) = self.max_bytes {
            body.insert("maxBytes".into(), Value::from(n));
        }
        if let Some(s) = &self.shard {
            let mut m = Map::new();
            m.insert("count".into(), Value::from(s.count));
            m.insert("index".into(), Value::from(s.index));
            if let Some(q) = &s.queues {
                m.insert("queues".into(), json!(q));
            }
            body.insert("shard".into(), Value::Object(m));
        }
        Value::Object(body)
    }

    /// One call that reads nothing (from `now`) and leaves the reader where
    /// it is: it fails as a read would, a broker without the feed included.
    /// The broker's answer (its `next` is where `now` is).
    pub async fn probe(&mut self) -> Result<Value> {
        let body = self
            .post(
                &json!({ "from": "now", "maxWaitMs": 0, "maxRecords": 1 }),
                Duration::from_secs(10),
            )
            .await?;
        serde_json::from_slice(&body)
            .map_err(|e| Error::Decode(format!("feed: the answer is not JSON: {e}")))
    }

    /// The next entry, in log order; `None` once caught up when `follow` is
    /// false, or after [`FeedReader::stop`].
    pub async fn next_entry(&mut self) -> Result<Option<FeedEntry>> {
        loop {
            if self.stopped {
                return Ok(None);
            }
            if let Some(e) = self.buffered.pop_front() {
                return Ok(Some(e));
            }
            if self.done {
                return Ok(None);
            }
            let wait = if self.follow { self.wait_millis } else { 0 };
            let page = self.read(Some(wait)).await?;
            if !self.follow && page.caught_up {
                self.done = true;
            }
            self.buffered.extend(page.entries);
        }
    }

    /// A stream's reader: its entries carry the payload in their message shape
    /// only (the runner reads nothing else), so it is not copied into `data`.
    pub(crate) fn set_shape_only(&mut self) {
        self.shape_only = true;
    }

    /// Ends the iteration after the entry in hand.
    pub fn stop(&mut self) {
        self.stopped = true;
        self.drop_ahead();
    }

    /// Drops the clients this reader opened to followers.
    pub fn close(&mut self) {
        self.stopped = true;
        self.drop_ahead();
        self.followers.clear();
    }

    fn learn(&mut self, followers: Option<&Value>) {
        let scheme = self.http.scheme();
        let Some(Value::Array(list)) = followers else {
            return;
        };
        for f in list {
            let addr = f
                .get("httpAddr")
                .and_then(|a| a.as_str())
                .map(str::trim)
                .unwrap_or("");
            if addr.is_empty() {
                continue;
            }
            let url = if addr.starts_with("http://") || addr.starts_with("https://") {
                addr.to_string()
            } else {
                format!("{scheme}://{addr}")
            };
            if self.followers.iter().any(|x| x.url == url) {
                continue;
            }
            if let Ok(client) = self.http.for_node(&url) {
                self.followers.push(Follower {
                    url,
                    client: Arc::new(client),
                });
            }
        }
    }

    /// The answer's bytes: a page is decoded from them in one pass ([`parse_page`]).
    async fn post(&mut self, body: &Value, timeout: Duration) -> Result<Vec<u8>> {
        let opts = Opts::default().timeout(timeout);
        let mut last: Option<Error> = None;
        let mut attempt = 0;
        // Each follower once, then the configured URL; a leader's 421 adds followers.
        while attempt < self.followers.len() + 3 {
            attempt += 1;
            let use_follower = !self.followers.is_empty()
                && self.fallback_until.is_none_or(|t| Instant::now() >= t);
            let target = use_follower.then(|| self.follower_at % self.followers.len());
            let client: &HttpClient = match target {
                Some(i) => &self.followers[i].client,
                None => &self.http,
            };
            let answer: Result<Option<Vec<u8>>> =
                client.post_bytes("/api/v1/feed", body, &opts).await;
            match answer {
                Ok(v) => {
                    self.last_target = target;
                    return v.ok_or_else(|| Error::Decode("feed: an empty answer".into()));
                }
                Err(err) => {
                    let status = err.status();
                    let code = err.code().map(|c| c.as_str().to_string());
                    if status == Some(421) && code.as_deref() == Some(FEED_ON_LEADER) {
                        // A leader: read from the followers it names (this node may lead now).
                        let named = err.body().and_then(|b| b.get("followers")).cloned();
                        self.learn(named.as_ref());
                        if target.is_some() {
                            self.follower_at += 1;
                        }
                        self.fallback_until = None;
                        last = Some(err);
                        continue;
                    }
                    if target.is_some() && status.is_none_or(|s| s >= 500) {
                        // That follower is gone or busy: the next one, then the configured URL.
                        self.follower_at += 1;
                        if self.follower_at % self.followers.len() == 0 {
                            self.fallback_until = Some(Instant::now() + Duration::from_secs(5));
                        }
                        last = Some(err);
                        continue;
                    }
                    if status == Some(404) {
                        return Err(no_feed_route(&err));
                    }
                    return Err(err);
                }
            }
        }
        Err(Error::Network(format!(
            "feed: every node asked is the leader or did not answer; the feed is read from followers \
             (pass followerUrls, or run the broker with QUEEN_FEED_ON_LEADER=true on a test cluster){}",
            last.map(|e| format!(" (last: {e})")).unwrap_or_default()
        )))
    }
}

// A page still being read ahead is of no use to anyone once the reader is gone.
impl Drop for FeedReader {
    fn drop(&mut self) {
        self.drop_ahead();
    }
}

/// A 404 from the feed route: the broker predates the feed. Said plainly, and
/// coded [`NO_FEED_ROUTE`], so a runner stops instead of retrying forever.
fn no_feed_route(cause: &Error) -> Error {
    Error::Http {
        status: 404,
        message: format!(
            "feed: this broker has no POST /api/v1/feed (it answered 404): it is older than \
             2.8.0, the first broker with the tenant feed. ({cause})"
        ),
        code: Some(queen_protocol::ErrorCode::Other(NO_FEED_ROUTE.into())),
        retry_after_seconds: None,
        body: None,
    }
}

/// Whether an error is a broker without the feed route.
pub fn is_no_feed_route(err: &Error) -> bool {
    err.code().map(|c| c.as_str()) == Some(NO_FEED_ROUTE)
}

/// The least a decoding thread takes, in bytes of the page. A stream reads
/// the broker's default pages (1000 entries, 1 MiB at most), so the split
/// goes by bytes, not entries: a page of large messages holds few of them.
const SPLIT_PART_BYTES: usize = 64 << 10;
/// A smaller page is decoded on one thread: a split costs more than it gives.
const SPLIT_MIN_BYTES: usize = 2 * SPLIT_PART_BYTES;
/// The fewest entries a decoding thread takes.
const SPLIT_MIN_ENTRIES: usize = 8;

/// The broker's answer to a read, from its bytes: in one pass (the answer,
/// its entries and their payloads together, no tree of the whole answer to
/// copy entries out of), or, for a large page, split at its entries by a
/// light scan and decoded on several threads. Whatever the scan does not
/// expect goes to the one-pass decode, which then says what is wrong.
#[cfg(test)]
fn parse_page(body: &[u8]) -> Result<FeedPage> {
    parse_page_with(body, false)
}

fn parse_page_with(body: &[u8], shape_only: bool) -> Result<FeedPage> {
    if body.len() >= SPLIT_MIN_BYTES {
        let threads = std::thread::available_parallelism().map_or(1, |n| n.get());
        if threads > 1 {
            if let Some(p) = parse_split(body, threads, shape_only) {
                return Ok(p);
            }
        }
    }
    let a: WireAnswer = serde_json::from_slice(body).map_err(|e| {
        Error::Decode(format!(
            "feed: the answer is not a feed page: {e} (body: {})",
            String::from_utf8_lossy(&body[..body.len().min(256)])
        ))
    })?;
    let entries = a
        .entries
        .into_iter()
        .map(|w| FeedEntry::of_wire(w, shape_only))
        .collect();
    page_of(a.next, a.resolved_ts, a.caught_up, a.applied, entries)
}

fn page_of(
    next: Option<u64>,
    resolved_ts: Option<String>,
    caught_up: bool,
    applied: Option<u64>,
    entries: Vec<FeedEntry>,
) -> Result<FeedPage> {
    let next = next.ok_or_else(|| Error::Decode("feed: an answer without next".into()))?;
    Ok(FeedPage {
        entries,
        next,
        resolved_ts,
        caught_up,
        applied,
    })
}

/// The answer without its entries, then the entries in parts, a thread each.
fn parse_split(body: &[u8], threads: usize, shape_only: bool) -> Option<FeedPage> {
    let (spans, arr) = entry_spans(body)?;
    if spans.len() < 2 * SPLIT_MIN_ENTRIES {
        return None;
    }
    let threads = threads
        .min(body.len() / SPLIT_PART_BYTES)
        .min(spans.len() / SPLIT_MIN_ENTRIES)
        .max(1);
    let mut rest = Vec::with_capacity(arr.0 + 2 + body.len() - arr.1);
    rest.extend_from_slice(&body[..arr.0]);
    rest.extend_from_slice(b"[]");
    rest.extend_from_slice(&body[arr.1..]);
    let head: WireAnswer = serde_json::from_slice(&rest).ok()?;
    let per = spans.len().div_ceil(threads);
    let parts: Vec<Option<Vec<FeedEntry>>> = std::thread::scope(|s| {
        let handles: Vec<_> = spans
            .chunks(per)
            .map(|chunk| {
                s.spawn(move || {
                    // The part as one JSON array: its entries and the commas between them.
                    let (from, to) = (chunk[0].0, chunk[chunk.len() - 1].1);
                    let mut buf = Vec::with_capacity(to - from + 2);
                    buf.push(b'[');
                    buf.extend_from_slice(&body[from..to]);
                    buf.push(b']');
                    let es: Vec<WireEntry> = serde_json::from_slice(&buf).ok()?;
                    (es.len() == chunk.len()).then(|| {
                        es.into_iter()
                            .map(|w| FeedEntry::of_wire(w, shape_only))
                            .collect()
                    })
                })
            })
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().ok().flatten())
            .collect()
    });
    let mut entries = Vec::with_capacity(spans.len());
    for p in parts {
        entries.extend(p?);
    }
    page_of(
        head.next,
        head.resolved_ts,
        head.caught_up,
        head.applied,
        entries,
    )
    .ok()
}

fn skip_space(b: &[u8], mut i: usize) -> usize {
    while i < b.len() && matches!(b[i], b' ' | b'\n' | b'\r' | b'\t') {
        i += 1;
    }
    i
}

/// Past the closing quote of the string at `b[i]` (`"`).
fn skip_string(b: &[u8], mut i: usize) -> Option<usize> {
    i += 1;
    while i < b.len() {
        match b[i] {
            b'\\' => i += 2,
            b'"' => return Some(i + 1),
            _ => i += 1,
        }
    }
    None
}

/// Past the JSON value at `b[i]`. It does not validate: what it splits is
/// then decoded, and so checked, by serde_json.
fn skip_value(b: &[u8], mut i: usize) -> Option<usize> {
    match *b.get(i)? {
        b'"' => skip_string(b, i),
        b'{' | b'[' => {
            let mut depth = 0usize;
            while i < b.len() {
                match b[i] {
                    b'"' => {
                        i = skip_string(b, i)?;
                        continue;
                    }
                    b'{' | b'[' => depth += 1,
                    b'}' | b']' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(i + 1);
                        }
                    }
                    _ => {}
                }
                i += 1;
            }
            None
        }
        _ => {
            while i < b.len() && !matches!(b[i], b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t')
            {
                i += 1;
            }
            Some(i)
        }
    }
}

/// The answer's `entries` array: each entry's span, and the array's.
type Spans = (Vec<(usize, usize)>, (usize, usize));

fn entry_spans(b: &[u8]) -> Option<Spans> {
    let mut i = skip_space(b, 0);
    if *b.get(i)? != b'{' {
        return None;
    }
    i += 1;
    loop {
        i = skip_space(b, i);
        if *b.get(i)? != b'"' {
            return None;
        }
        let ke = skip_string(b, i)?;
        let key = &b[i + 1..ke - 1];
        i = skip_space(b, ke);
        if *b.get(i)? != b':' {
            return None;
        }
        i = skip_space(b, i + 1);
        if key == b"entries" {
            if *b.get(i)? != b'[' {
                return None;
            }
            let start = i;
            let mut spans = Vec::new();
            let mut j = skip_space(b, i + 1);
            if *b.get(j)? == b']' {
                return Some((spans, (start, j + 1)));
            }
            loop {
                let e = skip_value(b, j)?;
                spans.push((j, e));
                j = skip_space(b, e);
                match *b.get(j)? {
                    b']' => return Some((spans, (start, j + 1))),
                    b',' => j = skip_space(b, j + 1),
                    _ => return None,
                }
            }
        }
        let e = skip_value(b, i)?;
        i = skip_space(b, e);
        if *b.get(i)? != b',' {
            return None;
        }
        i += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cond(f: &str, op: &str, v: Option<Value>) -> WhereCondition {
        WhereCondition::new(f, op, v).unwrap()
    }

    #[test]
    fn builds_and_checks_conditions() {
        assert_eq!(
            cond("data.country", "=", Some(json!("IT"))).to_json_text(),
            r#"{"field":"data.country","op":"=","value":"IT"}"#
        );
        assert_eq!(
            cond("data.vip", "exists", None).to_json_text(),
            r#"{"field":"data.vip","op":"exists"}"#
        );
        assert!(WhereCondition::new("x", "~", Some(json!(1)))
            .unwrap_err()
            .to_string()
            .contains("unknown op"));
        assert!(WhereCondition::new("x", "in", Some(json!(1)))
            .unwrap_err()
            .to_string()
            .contains("needs an array"));
        assert!(WhereCondition::new("x", "prefix", Some(json!(1)))
            .unwrap_err()
            .to_string()
            .contains("needs a string"));
        assert_eq!(cond("x", "=", None).value, Some(Value::Null));
    }

    #[test]
    fn evaluates_like_the_broker_on_a_message() {
        let msg = json!({ "queue": "orders", "partition": "booking-9", "data": { "country": "IT", "total": 40, "tags": ["vip"] } });
        assert!(cond("data.country", "=", Some(json!("IT"))).holds(&msg));
        assert!(!cond("data.total", ">", Some(json!(100))).holds(&msg));
        assert!(cond("data.total", "<=", Some(json!(40.0))).holds(&msg));
        assert!(cond("data.tags.0", "=", Some(json!("vip"))).holds(&msg));
        assert!(cond("partition", "prefix", Some(json!("booking-"))).holds(&msg));
        assert!(cond("data.country", "in", Some(json!(["FR", "IT"]))).holds(&msg));
        // A missing field holds for no op, `!=` included.
        assert!(!cond("data.missing", "!=", Some(json!("x"))).holds(&msg));
        assert!(!cond("data.country", "!prefix", Some(json!("I"))).holds(&msg));
        assert!(cond("transactionId", "!prefix", Some(json!("fd-")))
            .holds(&json!({"transactionId": "t1"})));
    }

    #[test]
    fn reads_since_as_a_duration_a_date_or_a_time() {
        let now = js::parse_date("2026-11-06T10:00:00Z").unwrap();
        assert_eq!(
            since_to_iso(&"1d".into(), now).unwrap(),
            "2026-11-05T10:00:00.000Z"
        );
        assert_eq!(
            since_to_iso(&"15m".into(), now).unwrap(),
            "2026-11-06T09:45:00.000Z"
        );
        assert_eq!(
            since_to_iso(&" 2 h ".into(), now).unwrap(),
            "2026-11-06T08:00:00.000Z"
        );
        assert_eq!(
            since_to_iso(&Since::Millis(now), 0.0).unwrap(),
            "2026-11-06T10:00:00.000Z"
        );
        assert_eq!(
            since_to_iso(&"2026-11-01T00:00:00Z".into(), 0.0).unwrap(),
            "2026-11-01T00:00:00.000Z"
        );
        assert!(since_to_iso(&"yesterday".into(), now)
            .unwrap_err()
            .to_string()
            .contains("since must be"));
    }

    #[test]
    fn hands_entries_out_in_queen_message_shape() {
        let page = parse_page(
            json!({ "entries": [{
                "id": "m1", "kind": "push", "offset": 1, "partition": "c1", "partitionId": "pid-1",
                "payload": { "n": 1 }, "position": 5, "queue": "orders", "transactionId": "t1",
                "ts": "2026-11-06T09:00:00.000000Z"
            }], "next": 6, "caughtUp": true })
            .to_string()
            .as_bytes(),
        )
        .unwrap();
        let e = &page.entries[0];
        let v = e.to_value();
        assert_eq!(v["data"], json!({ "n": 1 }));
        assert_eq!(e.data, json!({ "n": 1 }));
        assert_eq!(v["createdAt"], v["ts"]);
        assert_eq!(v["position"], json!(5));
        assert_eq!(e.partition, "c1");
        assert!(v.get("payload").is_none());
        assert_eq!((page.next, page.caught_up, page.applied), (6, true, None));
    }

    /// A page as the broker writes it, with the strings a split must not cut
    /// at: quotes, escapes, brackets and commas inside strings.
    fn big_page(n: usize) -> Vec<u8> {
        let entries: Vec<Value> = (0..n)
            .map(|i| {
                json!({
                    "id": format!("m{i}"), "kind": "push", "offset": i, "partition": format!("p{}", i % 7),
                    "partitionId": format!("u{}", i % 7), "position": 10 + i / 3, "queue": "q",
                    "transactionId": format!("t{i}"), "ts": "2026-11-07T10:00:00.000001Z",
                    "payload": { "i": i, "s": format!("a \"quote\" \\ back, [not] {{an}} array {i} ☃"),
                                 "nested": { "list": [1, "]", "}", null, true] } }
                })
            })
            .collect();
        json!({ "applied": 100 + n, "caughtUp": false, "entries": entries, "next": 11 + n / 3,
                "resolvedTs": "2026-11-07T10:00:00.000001Z" })
        .to_string()
        .into_bytes()
    }

    #[test]
    fn a_split_page_decodes_as_the_whole_page() {
        let body = big_page(5000);
        assert!(body.len() >= SPLIT_MIN_BYTES, "{} bytes", body.len());
        let split = parse_split(&body, 4, false).expect("a page the broker writes is split");
        let a: WireAnswer = serde_json::from_slice(&body).unwrap();
        let whole: Vec<FeedEntry> = a
            .entries
            .into_iter()
            .map(|w| FeedEntry::of_wire(w, false))
            .collect();
        assert_eq!(split.entries, whole);
        assert_eq!(
            (split.next, split.caught_up, split.applied),
            (11 + 5000 / 3, false, Some(5100))
        );
        assert_eq!(parse_page(&body).unwrap().entries, whole);
        // A stream's reader: the payload is in the shape only.
        let shapes = parse_page_with(&body, true).unwrap().entries;
        assert!(shapes
            .iter()
            .zip(&whole)
            .all(|(s, w)| s.data.is_null() && s.to_value() == w.to_value()));
        // Keys in another order, an empty page and a broken page.
        let text = String::from_utf8(body.clone()).unwrap();
        let arr = &text[text.find('[').unwrap()..=text.rfind(']').unwrap()];
        let reordered = format!("{{\"next\":5,\"entries\":{arr},\"caughtUp\":true}}");
        assert_eq!(parse_page(reordered.as_bytes()).unwrap().entries, whole);
        assert!(parse_page(br#"{"entries":[],"next":3,"caughtUp":true}"#)
            .unwrap()
            .entries
            .is_empty());
        // Cut short: refused whichever way it is decoded.
        assert!(parse_page(&body[..body.len() - 40]).is_err());
    }
}
