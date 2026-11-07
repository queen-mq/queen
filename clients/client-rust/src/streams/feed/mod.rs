//! Streams over the tenant feed: [`Stream::from_feed`](crate::streams::Stream::from_feed).
//!
//! The same engine as the JS SDK's `Stream.fromFeed` — stateless operators,
//! routing sinks, windows, patterns, joins of two sides and of many, gathers,
//! table joins, event time, any number of stateful steps in one chain,
//! snapshots and replay, one active runner per queryId with standbys, a
//! poison policy, shards, and live state reads — and the same formats in KV,
//! so one stream can move between SDKs: a JS runner stops, a Rust runner with
//! the same queryId continues where it was, with its open windows.
//!
//! ```no_run
//! use queen_mq::Queen;
//! use queen_mq::streams::Stream;
//! use queen_mq::streams::feed::{FeedRunOptions, FeedSource, FeedWindow, FeedAggregate};
//! use serde_json::{json, Value};
//!
//! # async fn example(queen: Queen) -> queen_mq::Result<()> {
//! let handle = Stream::from_feed(&queen, FeedSource::new().queues(["orders"]))
//!     .where_("data.country", "=", json!("IT"))
//!     .window(FeedWindow::tumbling(60.0))
//!     .aggregate(FeedAggregate::new().count("count").field("sum", |v: &Value| v["total"].clone()))
//!     .to("orders-it-per-minute")
//!     .run(FeedRunOptions::new("orders-it"))
//!     .await?;
//! handle.stop().await;
//! # Ok(())
//! # }
//! ```

mod about;
mod batch;
mod clock;
mod collect;
mod env;
mod ops;
mod patterns;
mod pipeline;
mod query;
mod runner;
mod shards;
pub(crate) mod snapshot;
mod state;
mod tables;
mod windows;

use std::future::Future;
use std::sync::Arc;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::feed::{FeedFrom, Since, WhereCondition};
use crate::queue::QueueBuilder;
use crate::{js, Queen};

pub use batch::{BatchItem, BatchOptions};
pub use collect::Gather;
pub use ops::{
    FeedAggregate, FeedDuration, FeedWindow, FnResult, IntoEventTime, IntoFeedBool, IntoFeedDone,
    IntoFeedList, IntoFeedValue,
};
pub use patterns::{Join, Pattern};
pub use query::{query_queue_name, FeedView, ViewOptions};
pub use runner::{
    feed_output_id, output_prefix, shard_id, FeedErrorInfo, FeedHandle, FeedRunOptions, FeedRunner,
    FeedStateView, Poison, Reset, ShardBy, POSITION_NAMESPACE,
};
pub use shards::FeedShardGroup;
pub use state::{fnv1a32, shard_of};
pub use tables::TableJoin;

use ops::{
    desc_text, filter_fn, flat_map_fn, fn1, map_fn, CatchUnwind, Desc, ForeachFn, PartitionFn,
    RouteFn,
};
use pipeline::{PlanStep, StatelessOp};

/// JavaScript's `String(v)` (`None` is `undefined`): how a feed stream turns
/// a key into a string (`3`, not `3.0`; `[1,2]` is `1,2`).
pub fn js_string(v: Option<&Value>) -> String {
    js::string_of(v)
}

/// A JSON number as JavaScript keeps it: an integer when it is one (so it
/// prints `60`, never `60.0`), `null` for NaN and the infinities.
pub fn js_number(x: f64) -> Value {
    js::num(x)
}

/// `Date.parse` for ISO 8601 (and the `toUTCString` form): epoch milliseconds.
pub fn js_parse_date(text: &str) -> Option<f64> {
    js::parse_date(text)
}

/// `a < b` on two JS strings (by UTF-16 code units).
pub fn js_str_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    js::str_cmp(a, b)
}

/// Milliseconds of a duration string (`"15m"`, `"1h30m"`, `"250ms"`), as the JS options read it.
pub fn parse_duration_ms(text: &str) -> std::result::Result<f64, String> {
    js::parse_duration_ms(text)
}

// ------------------------------------------------------------------ source

/// Where a feed stream reads: `Stream::from_feed(&queen, FeedSource::new()...)`.
#[derive(Debug, Clone, Default)]
pub struct FeedSource {
    pub queues: Option<Vec<String>>,
    pub partitions: Option<Vec<String>>,
    /// Where a NEW query starts (default `now`; `earliest` with `since`).
    pub from: Option<FeedFrom>,
    /// A new query starts at the messages created since then.
    pub since: Option<Since>,
    pub follower_urls: Vec<String>,
    pub kinds: Option<Vec<String>>,
    pub max_records: Option<u64>,
    pub max_bytes: Option<u64>,
}

impl FeedSource {
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

    pub fn from(mut self, from: FeedFrom) -> Self {
        self.from = Some(from);
        self
    }

    pub fn since(mut self, since: impl Into<Since>) -> Self {
        self.since = Some(since.into());
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

    pub fn kinds<I, S>(mut self, kinds: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.kinds = Some(kinds.into_iter().map(Into::into).collect());
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
}

// -------------------------------------------------------------------- sink

/// Where results go: one queue, several (each result to every one), or an
/// async function that picks (`(value, msg) -> target(s)`: a queue name, a
/// `{ "queue", "partition" }` object, a list of those, or `null` for
/// nowhere). A partition: the target's own, else the sink's, else the
/// source partition (or the key of a window or a pattern).
#[derive(Clone)]
pub struct FeedSink {
    pub(crate) queues: Vec<String>,
    pub(crate) route: Option<RouteFn>,
    pub(crate) partition: SinkPartition,
    pub(crate) error: Option<String>,
}

#[derive(Clone)]
pub(crate) enum SinkPartition {
    Default,
    Fixed(String),
    By(PartitionFn),
}

impl FeedSink {
    pub fn queue(name: impl Into<String>) -> Self {
        Self::queues([name.into()])
    }

    /// Every result to each of these queues.
    pub fn queues<I, S>(names: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut queues: Vec<String> = Vec::new();
        let mut error = None;
        for n in names {
            let n: String = n.into();
            if n.is_empty() {
                error = Some(format!(
                    ".to(): a queue name is a non-empty string, got {}",
                    js::string_json(&n)
                ));
            }
            if !queues.contains(&n) {
                queues.push(n);
            }
        }
        if queues.is_empty() && error.is_none() {
            error = Some(".to([]): name at least one queue".into());
        }
        Self {
            queues,
            route: None,
            partition: SinkPartition::Default,
            error,
        }
    }

    /// A function picks the target(s) of each result.
    pub fn route<F, Fut, R>(f: F) -> Self
    where
        F: Fn(Option<Value>, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoFeedValue + 'static,
    {
        let f = Arc::new(f);
        let route: RouteFn = Arc::new(move |v, m| {
            let fut = f(v, m);
            Box::pin(async move {
                match CatchUnwind(Box::pin(fut)).await {
                    Ok(r) => r.into_feed_value(),
                    Err(e) => Err(e),
                }
            })
        });
        Self {
            queues: Vec::new(),
            route: Some(route),
            partition: SinkPartition::Default,
            error: None,
        }
    }

    /// Every result to this partition.
    pub fn partition(mut self, name: impl Into<String>) -> Self {
        self.partition = SinkPartition::Fixed(name.into());
        self
    }

    /// The partition from the result: `(value) -> name`.
    pub fn partition_by<F, R>(mut self, f: F) -> Self
    where
        F: Fn(Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.partition = SinkPartition::By(Arc::new(move |v| ops::call(|| f(v).into_feed_value())));
        self
    }
}

impl From<&str> for FeedSink {
    fn from(s: &str) -> Self {
        Self::queue(s)
    }
}
impl From<String> for FeedSink {
    fn from(s: String) -> Self {
        Self::queue(s)
    }
}
impl From<QueueBuilder> for FeedSink {
    fn from(q: QueueBuilder) -> Self {
        Self::queue(q.name().unwrap_or_default().to_string())
    }
}
impl From<&QueueBuilder> for FeedSink {
    fn from(q: &QueueBuilder) -> Self {
        Self::queue(q.name().unwrap_or_default().to_string())
    }
}
impl From<Vec<String>> for FeedSink {
    fn from(v: Vec<String>) -> Self {
        Self::queues(v)
    }
}
impl From<Vec<&str>> for FeedSink {
    fn from(v: Vec<&str>) -> Self {
        Self::queues(v)
    }
}
impl<const N: usize> From<[&str; N]> for FeedSink {
    fn from(v: [&str; N]) -> Self {
        Self::queues(v)
    }
}
impl From<Vec<QueueBuilder>> for FeedSink {
    fn from(v: Vec<QueueBuilder>) -> Self {
        Self::queues(v.iter().map(|q| q.name().unwrap_or_default().to_string()))
    }
}

/// The end of a chain.
#[derive(Clone)]
pub(crate) enum Terminal {
    To(FeedSink),
    Foreach(ForeachFn),
    ForeachBatch(batch::ForeachBatch),
}

// --------------------------------------------------------------- operators

#[derive(Clone)]
enum FeedOp {
    Where(WhereCondition),
    Stateless(StatelessOp),
    Window(Box<FeedWindow>),
    Reduce(ops::ReducerSpec),
    Aggregate(FeedAggregate),
    Pattern(Box<Pattern>),
    Join(Box<Join>),
    Gather(Box<Gather>),
    Table(Box<TableJoin>),
    Name(String),
    Terminal(Terminal),
}

impl FeedOp {
    fn kind(&self) -> &'static str {
        match self {
            FeedOp::Where(_) => "filter",
            FeedOp::Stateless(StatelessOp::Map(_)) => "map",
            FeedOp::Stateless(StatelessOp::Filter(_))
            | FeedOp::Stateless(StatelessOp::Where(_)) => "filter",
            FeedOp::Stateless(StatelessOp::FlatMap(_)) => "flatMap",
            FeedOp::Stateless(StatelessOp::KeyBy(_)) => "keyBy",
            FeedOp::Window(_) => "window",
            FeedOp::Reduce(_) => "reduce",
            FeedOp::Aggregate(_) => "aggregate",
            FeedOp::Pattern(_) => "pattern",
            FeedOp::Join(j) if j.is_many() => "collect",
            FeedOp::Join(_) => "pattern",
            FeedOp::Gather(_) => "collect",
            FeedOp::Table(_) => "table",
            FeedOp::Name(_) => "name",
            FeedOp::Terminal(Terminal::To(_)) => "sink",
            FeedOp::Terminal(Terminal::Foreach(_)) => "foreach",
            FeedOp::Terminal(Terminal::ForeachBatch(_)) => "foreachBatch",
        }
    }
}

/// A stream over the tenant feed, under construction. Every call returns
/// the stream; a chain that cannot run fails at [`FeedStream::run`] (or
/// [`FeedStream::chain_hash`]), with the JS SDK's message.
#[derive(Clone)]
pub struct FeedStream {
    queen: Queen,
    source: FeedSource,
    ops: Vec<FeedOp>,
    error: Option<String>,
}

/// A chain checked and split into steps.
#[derive(Clone)]
pub(crate) struct CompiledFeed {
    pub queen: Queen,
    pub source: FeedSource,
    pub steps: Vec<PlanStep>,
    /// The `where`s before the first stateful step: sent to the broker.
    pub pushdown: Vec<WhereCondition>,
    pub terminal: Option<Terminal>,
    pub chain_hash: String,
}

impl FeedStream {
    pub(crate) fn new(queen: &Queen, source: FeedSource) -> Self {
        Self {
            queen: queen.clone(),
            source,
            ops: Vec::new(),
            error: None,
        }
    }

    fn push(mut self, op: FeedOp) -> Self {
        self.ops.push(op);
        self
    }

    fn fail(mut self, e: String) -> Self {
        if self.error.is_none() {
            self.error = Some(e);
        }
        self
    }

    /// Keep the source messages where `field op value` holds: sent to the
    /// broker when no stateful step comes before it (`value`: `None` for
    /// `exists`).
    pub fn where_(self, field: &str, op: &str, value: impl Into<Option<Value>>) -> Self {
        match WhereCondition::new(field, op, value.into()) {
            Ok(c) => self.push(FeedOp::Where(c)),
            Err(e) => self.fail(e.to_string()),
        }
    }

    /// Keep what the test accepts: `(msg, ctx) -> bool`.
    pub fn filter<F, R>(self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedBool,
    {
        self.push(FeedOp::Stateless(StatelessOp::Filter(filter_fn(f))))
    }

    /// `(msg, ctx) -> value`. Every map sees the message (or the step's
    /// result), as in JS: the last map's value is what goes on.
    pub fn map<F, R>(self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.push(FeedOp::Stateless(StatelessOp::Map(map_fn(f))))
    }

    /// `(msg, ctx) -> list`: one envelope per item.
    pub fn flat_map<F, R>(self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedList,
    {
        self.push(FeedOp::Stateless(StatelessOp::FlatMap(flat_map_fn(f))))
    }

    /// The key of the stateful steps after it: `String(f(msg))`.
    pub fn key_by<F, R>(self, f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.push(FeedOp::Stateless(StatelessOp::KeyBy(fn1(f))))
    }

    /// A window; `.reduce()` or `.aggregate()` must come right after it.
    pub fn window(self, w: FeedWindow) -> Self {
        self.push(FeedOp::Window(Box::new(w)))
    }

    pub fn window_tumbling(self, seconds: f64) -> Self {
        self.window(FeedWindow::tumbling(seconds))
    }

    pub fn window_sliding(self, size: f64, slide: f64) -> Self {
        self.window(FeedWindow::sliding(size, slide))
    }

    pub fn window_session(self, gap: f64) -> Self {
        self.window(FeedWindow::session(gap))
    }

    pub fn window_cron(self, every: &str) -> Self {
        self.window(FeedWindow::cron(every))
    }

    /// Fold each window: `(acc, value) -> acc`, starting from `initial`
    /// (copied for each window; `None`: the first fold gets `None`).
    pub fn reduce<F, R>(self, initial: Option<Value>, f: F) -> Self
    where
        F: Fn(Option<Value>, &Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.push(FeedOp::Reduce(ops::reducer(initial, f)))
    }

    /// Named fields per window: count, sum, min, max, avg, or a running sum.
    pub fn aggregate(self, a: FeedAggregate) -> Self {
        self.push(FeedOp::Aggregate(a))
    }

    pub fn pattern(self, p: Pattern) -> Self {
        self.push(FeedOp::Pattern(Box::new(p)))
    }

    pub fn join(self, j: Join) -> Self {
        self.push(FeedOp::Join(Box::new(j)))
    }

    pub fn gather(self, g: Gather) -> Self {
        self.push(FeedOp::Gather(Box::new(g)))
    }

    pub fn join_table(self, t: TableJoin) -> Self {
        self.push(FeedOp::Table(Box::new(t)))
    }

    /// Name the stateful step right before this call, for live reads
    /// (`handle.state(name)`, `Stream::view(...).get(name, key)`). Unnamed
    /// steps are `step1`, `step2`, ... in chain order.
    pub fn name(self, name: &str) -> Self {
        let ok = !name.is_empty()
            && name.len() <= 64
            && name
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-'));
        if !ok {
            return self.fail(format!(
                ".name(): a step name is 1 to 64 letters, digits, '_', '.' or '-', got {}",
                js::string_json(name)
            ));
        }
        self.push(FeedOp::Name(name.to_string()))
    }

    /// Push each result to a queue, several, or where a function says
    /// ([`FeedSink`]), in the commit that moves the position.
    pub fn to(self, sink: impl Into<FeedSink>) -> Self {
        let sink = sink.into();
        if let Some(e) = &sink.error {
            let e = e.clone();
            return self.fail(e);
        }
        self.push(FeedOp::Terminal(Terminal::To(sink)))
    }

    /// Run an async side effect per result, `(value, ctx)`, before the
    /// position moves (at least once).
    pub fn foreach<F, Fut, R>(self, f: F) -> Self
    where
        F: Fn(Option<Value>, Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoFeedDone + 'static,
    {
        let f = Arc::new(f);
        let fe: ForeachFn = Arc::new(move |v, c| {
            let fut = f(v, c);
            Box::pin(async move {
                match CatchUnwind(Box::pin(fut)).await {
                    Ok(r) => r.into_feed_done(),
                    Err(e) => Err(e),
                }
            })
        });
        self.push(FeedOp::Terminal(Terminal::Foreach(fe)))
    }

    /// Run an async side effect per batch of results, before the position
    /// moves (at least once): `f(items)` gets up to `max_items` results, each
    /// a [`BatchItem`] (`value` and `ctx` as `.foreach()` gets them, and an
    /// `id` that is the same on every delivery of that result: a key for an
    /// upsert). A batch goes at the end of each read, or, with
    /// `max_wait_millis`, once it is full or its first item has waited that
    /// long. The position moves only after `f` returns: a crash before that
    /// delivers the batch again.
    ///
    /// A batch that fails is tried again whole (`retries`), then item by
    /// item, so the poison policy (`on_poison`) meets only the item that fails.
    ///
    /// Feed streams only: a queue source has no `.foreach_batch()` (use
    /// `.foreach()`, or `.to()` a queue that a worker pops in batches).
    ///
    /// ```compile_fail
    /// # async fn f(queen: queen_mq::Queen) {
    /// use queen_mq::streams::feed::BatchOptions;
    /// use queen_mq::streams::Stream;
    /// let _ = Stream::from(queen.queue("orders")).foreach_batch(|_items| async {}, BatchOptions::new());
    /// # }
    /// ```
    pub fn foreach_batch<F, Fut, R>(self, f: F, opts: BatchOptions) -> Self
    where
        F: Fn(Vec<BatchItem>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = R> + Send + 'static,
        R: IntoFeedDone + 'static,
    {
        match batch::ForeachBatch::new(f, opts) {
            Ok(fb) => self.push(FeedOp::Terminal(Terminal::ForeachBatch(fb))),
            Err(e) => self.fail(e),
        }
    }

    /// The chain's hash, as JS computes it (`chainHash`): the stored state
    /// was kept by this chain when the record names it.
    pub fn chain_hash(&self) -> Result<String> {
        Ok(self.compile()?.chain_hash)
    }

    /// The names of the stateful steps, in chain order.
    pub fn step_names(&self) -> Result<Vec<String>> {
        Ok(self
            .compile()?
            .steps
            .iter()
            .filter_map(|s| match s {
                PlanStep::Ops(_) => None,
                PlanStep::Window { name, .. }
                | PlanStep::Pattern { name, .. }
                | PlanStep::Collect { name, .. }
                | PlanStep::Table { name, .. } => Some(name.clone()),
            })
            .collect())
    }

    /// Start: one runner (or `shards` of them), active when the lock is
    /// free, else a standby. Fails when the chain cannot run, the broker has
    /// no feed, or the stored state was kept by another chain.
    pub async fn run(self, opts: FeedRunOptions) -> Result<FeedHandle> {
        let compiled = self.compile()?;
        runner::run(compiled, opts).await
    }

    /// `_compileFeed`: the steps, the pushdown, the chain hash.
    pub(crate) fn compile(&self) -> Result<CompiledFeed> {
        let bad = |m: String| Error::Invalid(m);
        if let Some(e) = &self.error {
            return Err(bad(e.clone()));
        }
        let ops = &self.ops;
        let sink_idx = ops.iter().position(|o| matches!(o, FeedOp::Terminal(_)));
        if let Some(i) = sink_idx {
            if i != ops.len() - 1 {
                return Err(bad(format!(
                    "sink operators (.to / .foreach / .foreachBatch) must be the last in the chain; found {} at position {i} of {}",
                    ops[i].kind(),
                    ops.len()
                )));
            }
        }
        let terminal = sink_idx.map(|i| match &ops[i] {
            FeedOp::Terminal(t) => t.clone(),
            _ => unreachable!(),
        });
        let upstream = match sink_idx {
            Some(i) => &ops[..i],
            None => &ops[..],
        };

        let mut steps: Vec<PlanStep> = Vec::new();
        let mut pushdown: Vec<WhereCondition> = Vec::new();
        let mut pending: Vec<StatelessOp> = Vec::new();
        let mut window: Option<(usize, ops::WindowSpec)> = None;
        let mut stateful = 0usize;
        // For the hash: each upstream op's description and whether it takes
        // the event time of the step before (`inheriting`).
        let mut descs: Vec<(usize, Desc)> = Vec::new();
        // (op index, has eventTime fn, eventTime off) of each stateful step with an engine.
        let mut clocked: Vec<(usize, bool, bool)> = Vec::new();

        fn flush(steps: &mut Vec<PlanStep>, pending: &mut Vec<StatelessOp>) {
            if !pending.is_empty() {
                steps.push(PlanStep::Ops(std::mem::take(pending)));
            }
        }

        for (i, op) in upstream.iter().enumerate() {
            if window.is_some() && !matches!(op, FeedOp::Reduce(_) | FeedOp::Aggregate(_)) {
                return Err(bad(
                    "Stream.fromFeed: a window needs .reduce() or .aggregate() right after it"
                        .into(),
                ));
            }
            match op {
                FeedOp::Where(c) => {
                    // Before any stateful step a where tests the source message: the broker does it.
                    if stateful == 0 {
                        pushdown.push(c.clone());
                    } else {
                        pending.push(StatelessOp::Where(c.clone()));
                    }
                }
                FeedOp::Stateless(s) => {
                    if let StatelessOp::KeyBy(_) = s {
                        descs.push((i, vec![("kind".into(), "\"keyBy\"".into())]));
                    }
                    pending.push(s.clone());
                }
                FeedOp::Window(w) => {
                    flush(&mut steps, &mut pending);
                    let spec = w.build().map_err(bad)?;
                    descs.push((i, spec.desc.clone()));
                    clocked.push((i, spec.time.event_time.is_some(), spec.time.off));
                    window = Some((i, spec));
                }
                FeedOp::Reduce(r) => {
                    let Some((_, w)) = window.take() else {
                        return Err(bad(
                            "reduce/aggregate requires a preceding window operator".into()
                        ));
                    };
                    stateful += 1;
                    descs.push((i, r.desc.clone()));
                    steps.push(PlanStep::Window {
                        window: w,
                        reducer: r.clone(),
                        name: format!("step{stateful}"),
                    });
                }
                FeedOp::Aggregate(a) => {
                    let Some((_, w)) = window.take() else {
                        return Err(bad(
                            "reduce/aggregate requires a preceding window operator".into()
                        ));
                    };
                    let r = a.build().map_err(bad)?;
                    stateful += 1;
                    descs.push((i, r.desc.clone()));
                    steps.push(PlanStep::Window {
                        window: w,
                        reducer: r,
                        name: format!("step{stateful}"),
                    });
                }
                FeedOp::Pattern(p) => {
                    flush(&mut steps, &mut pending);
                    let spec = p.build().map_err(bad)?;
                    stateful += 1;
                    descs.push((i, spec.desc.clone()));
                    clocked.push((i, spec.time.event_time.is_some(), spec.time.off));
                    let name = spec
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("step{stateful}"));
                    steps.push(PlanStep::Pattern {
                        op: Arc::new(spec),
                        name,
                    });
                }
                FeedOp::Join(j) => {
                    flush(&mut steps, &mut pending);
                    stateful += 1;
                    if j.is_many() {
                        let spec = j.build_many().map_err(bad)?;
                        descs.push((i, spec.desc.clone()));
                        clocked.push((i, spec.time.event_time.is_some(), spec.time.off));
                        let name = spec
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("step{stateful}"));
                        steps.push(PlanStep::Collect {
                            op: Arc::new(spec),
                            name,
                        });
                    } else {
                        let spec = j.build_two().map_err(bad)?;
                        descs.push((i, spec.desc.clone()));
                        clocked.push((i, spec.time.event_time.is_some(), spec.time.off));
                        let name = spec
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("step{stateful}"));
                        steps.push(PlanStep::Pattern {
                            op: Arc::new(spec),
                            name,
                        });
                    }
                }
                FeedOp::Gather(g) => {
                    flush(&mut steps, &mut pending);
                    let spec = g.build().map_err(bad)?;
                    stateful += 1;
                    descs.push((i, spec.desc.clone()));
                    clocked.push((i, spec.time.event_time.is_some(), spec.time.off));
                    let name = spec
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("step{stateful}"));
                    steps.push(PlanStep::Collect {
                        op: Arc::new(spec),
                        name,
                    });
                }
                FeedOp::Table(t) => {
                    flush(&mut steps, &mut pending);
                    let spec = t.build().map_err(bad)?;
                    stateful += 1;
                    descs.push((i, spec.desc.clone()));
                    let name = spec
                        .name
                        .clone()
                        .unwrap_or_else(|| format!("step{stateful}"));
                    steps.push(PlanStep::Table {
                        op: Arc::new(spec),
                        name,
                    });
                }
                FeedOp::Name(n) => {
                    let last = steps
                        .iter_mut()
                        .rev()
                        .find(|s| !matches!(s, PlanStep::Ops(_)));
                    match last {
                        Some(last) if pending.is_empty() => match last {
                            PlanStep::Window { name, .. }
                            | PlanStep::Pattern { name, .. }
                            | PlanStep::Collect { name, .. }
                            | PlanStep::Table { name, .. } => *name = n.clone(),
                            PlanStep::Ops(_) => unreachable!(),
                        },
                        _ => {
                            return Err(bad(
                                ".name() names the stateful step right before it".into()
                            ))
                        }
                    }
                    descs.push((
                        i,
                        vec![
                            ("kind".into(), "\"name\"".into()),
                            ("name".into(), js::string_json(n)),
                        ],
                    ));
                }
                FeedOp::Terminal(_) => unreachable!("the terminal is cut off"),
            }
        }
        if window.is_some() {
            return Err(bad(
                "Stream.fromFeed: a window needs .reduce() or .aggregate() after it".into(),
            ));
        }
        flush(&mut steps, &mut pending);
        let mut names: Vec<&String> = Vec::new();
        for s in &steps {
            let name = match s {
                PlanStep::Ops(_) => continue,
                PlanStep::Window { name, .. }
                | PlanStep::Pattern { name, .. }
                | PlanStep::Collect { name, .. }
                | PlanStep::Table { name, .. } => name,
            };
            if names.contains(&name) {
                return Err(bad(format!(
                    "Stream.fromFeed: two stateful steps are named {}",
                    js::string_json(name)
                )));
            }
            names.push(name);
        }
        // The clock each stateful step runs on, as the pipeline resolves it: a
        // step that takes the event time of the step before says so in the hash.
        let mut inheriting: Vec<usize> = Vec::new();
        let mut on_event_time = false;
        for (i, has_fn, off) in &clocked {
            if *has_fn {
                on_event_time = true;
            } else if on_event_time && !off {
                inheriting.push(*i);
            } else {
                on_event_time = false;
            }
        }
        let mut text = String::from("[");
        for (n, (i, d)) in descs.iter().enumerate() {
            if n > 0 {
                text.push(',');
            }
            let mut d = d.clone();
            if inheriting.contains(i) {
                d.push(("clock".into(), "\"event\"".into()));
            }
            text.push_str(&desc_text(&d));
        }
        text.push(']');
        let chain_hash = super::hash::sha256_hex(&text)[..16].to_string();
        Ok(CompiledFeed {
            queen: self.queen.clone(),
            source: self.source.clone(),
            steps,
            pushdown,
            terminal,
            chain_hash,
        })
    }
}

#[cfg(test)]
mod tests;
