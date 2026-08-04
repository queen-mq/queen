//! Windowed stream processing on top of Queen.
//!
//! A stream is a chain of operators over a source queue. The engine pops a
//! batch, folds it into per-key window state, and commits the state change, the
//! output and the source ack **in one PostgreSQL transaction** — so a window
//! never advances without its output being written, and never emits twice for
//! the same input.
//!
//! ```no_run
//! use std::time::Duration;
//! use queen_mq::{Config, Queen};
//! use queen_mq::streams::{RunOptions, Stream};
//!
//! # async fn example() -> queen_mq::Result<()> {
//! let queen = Queen::connect(Config::new("http://localhost:6632"))?;
//!
//! let handle = Stream::from(queen.queue("clicks"))
//!     .filter(|r| r.text("kind") == Some("purchase"))
//!     .window_tumbling(60)
//!     .aggregate_count("count")
//!     .aggregate_sum("revenue", |r| r.number("amount"))
//!     .to(queen.queue("revenue-per-minute"))
//!     .run(&queen, RunOptions::new("revenue-rollup"))
//!     .await?;
//!
//! tokio::time::sleep(Duration::from_secs(60)).await;
//! handle.stop().await?;
//! # Ok(())
//! # }
//! ```
//!
//! # State lives in Postgres, keyed by partition
//!
//! Window accumulators are rows in `queen_streams.state`, scoped to
//! `(query_id, partition_id, key)`. The key defaults to the source
//! `partition_id`, which is what makes the default configuration free of
//! cross-worker contention: a partition is claimed by one worker at a time, so
//! its state rows have one writer. [`Stream::key_by`] overrides that, and the
//! warning on it is not decorative.
//!
//! # Chain rules
//!
//! Checked when the stream is compiled, not at runtime:
//!
//! * a terminal (`to` / `foreach`) must be last;
//! * `reduce`/`aggregate` needs a window in front of it;
//! * at most one window, one reducer, one `key_by`, one `gate`;
//! * `gate` cannot share a stream with windowing — a gate stops a batch
//!   part-way, and the window model assumes the whole batch commits atomically.

mod engine;
mod hash;
pub mod ops;
mod runner;

use std::sync::Arc;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::queue::QueueBuilder;
use crate::Queen;

pub use ops::{
    AggregateField, AggregateKind, EmitCtx, Every, Extractor, GateCtx, LatePolicy, Record, Reducer,
    Sink, SinkPartition, Window, WindowKind,
};
pub use runner::{RunOptions, StreamHandle, StreamMetrics};

use ops::{EventTimeFn, ForeachFn, KeyByFn, Op, ReduceFn};

pub(crate) enum Terminal {
    Sink(Sink),
    Foreach(ForeachFn),
}

pub(crate) struct Stages {
    pub pre: Vec<Op>,
    pub key_by: Option<KeyByFn>,
    pub window: Option<Window>,
    pub reducer: Option<Reducer>,
    pub gate: Option<ops::GateFn>,
    pub post: Vec<Op>,
    pub terminal: Option<Terminal>,
}

/// Hand-written because the stages hold user closures, which have no `Debug`.
/// The shape is what is worth seeing anyway.
impl std::fmt::Debug for Stages {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Stages")
            .field(
                "pre",
                &self.pre.iter().map(|o| o.kind()).collect::<Vec<_>>(),
            )
            .field("key_by", &self.key_by.is_some())
            .field("window", &self.window)
            .field("reducer", &self.reducer)
            .field("gate", &self.gate.is_some())
            .field(
                "post",
                &self.post.iter().map(|o| o.kind()).collect::<Vec<_>>(),
            )
            .field(
                "terminal",
                &match &self.terminal {
                    Some(Terminal::Sink(s)) => format!("sink({})", s.queue),
                    Some(Terminal::Foreach(_)) => "foreach".to_string(),
                    None => "none".to_string(),
                },
            )
            .finish()
    }
}

/// A pipeline under construction.
///
/// Every combinator consumes and returns the stream, so a chain reads as one
/// expression. Errors in the chain's *shape* surface at [`Stream::run`].
pub struct Stream {
    source: QueueBuilder,
    ops: Vec<Op>,
}

impl Stream {
    /// Source a stream from a queue.
    pub fn from(source: QueueBuilder) -> Self {
        Self {
            source,
            ops: Vec::new(),
        }
    }

    fn push(mut self, op: Op) -> Self {
        self.ops.push(op);
        self
    }

    // ----------------------------------------------------------- stateless

    /// Transform each record.
    pub fn map<F>(self, f: F) -> Self
    where
        F: Fn(&Record) -> Value + Send + Sync + 'static,
    {
        self.push(Op::Map(Arc::new(f)))
    }

    /// Keep records the predicate accepts.
    pub fn filter<F>(self, f: F) -> Self
    where
        F: Fn(&Record) -> bool + Send + Sync + 'static,
    {
        self.push(Op::Filter(Arc::new(f)))
    }

    /// Expand each record into zero or more.
    pub fn flat_map<F>(self, f: F) -> Self
    where
        F: Fn(&Record) -> Vec<Value> + Send + Sync + 'static,
    {
        self.push(Op::FlatMap(Arc::new(f)))
    }

    // --------------------------------------------------------------- keying

    /// Override the state key, which defaults to the source partition.
    ///
    /// Only do this when the derived key stays *within* a partition. A key that
    /// spans partitions means two workers holding different leases write the
    /// same state row, and neither the broker nor this client can serialize
    /// that. The fix is to repartition: push to an intermediate queue keyed the
    /// way you want, and run the stateful stream over that.
    pub fn key_by<F>(self, f: F) -> Self
    where
        F: Fn(&Record) -> String + Send + Sync + 'static,
    {
        self.push(Op::KeyBy(Arc::new(f)))
    }

    // -------------------------------------------------------------- windows

    /// Fixed-size, non-overlapping windows.
    pub fn window_tumbling(self, seconds: i64) -> Self {
        self.push(Op::Window(Window::new(WindowKind::Tumbling { seconds })))
    }

    /// Overlapping windows of `size` that start every `slide` seconds.
    ///
    /// Each event lands in `size / slide` windows and so writes that many state
    /// rows per key — a one-hour window sliding every minute is 60 rows per
    /// event.
    pub fn window_sliding(self, size: i64, slide: i64) -> Self {
        self.push(Op::Window(Window::new(WindowKind::Sliding { size, slide })))
    }

    /// Per-key windows that close after `gap` seconds of silence.
    pub fn window_session(self, gap: i64) -> Self {
        self.push(Op::Window(Window::new(WindowKind::Session { gap })))
    }

    /// Windows aligned to wall-clock boundaries (UTC).
    pub fn window_cron(self, every: Every) -> Self {
        self.push(Op::Window(Window::new(WindowKind::Cron { every })))
    }

    /// Adjust the most recently added window.
    pub fn window_options<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Window),
    {
        if let Some(Op::Window(w)) = self.ops.last_mut() {
            f(w);
        }
        self
    }

    /// Switch the most recent window to event time.
    ///
    /// The extractor reads the timestamp *out of the payload*, so windows are
    /// decided by when things happened rather than when they arrived — which is
    /// what makes a replay produce the same windows as the live run. Events
    /// older than the watermark are dropped by default; see [`LatePolicy`].
    pub fn event_time<F>(self, f: F) -> Self
    where
        F: Fn(&queen_protocol::Message) -> Option<i64> + Send + Sync + 'static,
    {
        let f: EventTimeFn = Arc::new(f);
        self.window_options(move |w| w.event_time = Some(f))
    }

    /// How much out-of-orderness to tolerate before calling an event late.
    pub fn allowed_lateness(self, seconds: i64) -> Self {
        self.window_options(move |w| w.allowed_lateness_seconds = seconds)
    }

    /// What to do with an event older than the watermark.
    pub fn on_late(self, policy: LatePolicy) -> Self {
        self.window_options(move |w| w.on_late = policy)
    }

    /// Extra time a window stays open past its end.
    pub fn grace_seconds(self, seconds: i64) -> Self {
        self.window_options(move |w| w.grace_seconds = seconds)
    }

    /// How often quiet partitions are swept for ripe windows. `0` disables the
    /// sweep, which means a window on a partition that goes silent stays open
    /// until traffic returns.
    pub fn idle_flush_ms(self, ms: i64) -> Self {
        self.window_options(move |w| w.idle_flush_ms = ms)
    }

    // -------------------------------------------------------------- reducing

    /// Fold each window with your own function.
    pub fn reduce<F>(self, initial: Value, f: F) -> Self
    where
        F: Fn(Value, &Record) -> Value + Send + Sync + 'static,
    {
        let f: ReduceFn = Arc::new(f);
        self.push(Op::Reduce(Reducer::Fold {
            fold: f,
            initial: Some(initial),
        }))
    }

    fn aggregate_field(mut self, field: AggregateField) -> Self {
        // Aggregate fields accumulate into a single reducer rather than
        // stacking, so `.aggregate_count(..).aggregate_sum(..)` reads as one
        // aggregate with two fields — which is also what the config hash
        // describes.
        if let Some(Op::Reduce(Reducer::Aggregate { fields })) = self.ops.last_mut() {
            fields.push(field);
            return self;
        }
        self.push(Op::Reduce(Reducer::Aggregate {
            fields: vec![field],
        }))
    }

    /// Count records per window.
    pub fn aggregate_count(self, name: impl Into<String>) -> Self {
        self.aggregate_field(AggregateField {
            name: name.into(),
            kind: AggregateKind::Count,
            extract: Arc::new(|_| None),
        })
    }

    /// Sum an extracted number per window.
    pub fn aggregate_sum<F>(self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(&Record) -> Option<f64> + Send + Sync + 'static,
    {
        self.aggregate_field(AggregateField {
            name: name.into(),
            kind: AggregateKind::Sum,
            extract: Arc::new(f),
        })
    }

    /// Smallest extracted value per window. Records with no value are skipped,
    /// so a window that saw none reports `null` rather than zero.
    pub fn aggregate_min<F>(self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(&Record) -> Option<f64> + Send + Sync + 'static,
    {
        self.aggregate_field(AggregateField {
            name: name.into(),
            kind: AggregateKind::Min,
            extract: Arc::new(f),
        })
    }

    /// Largest extracted value per window.
    pub fn aggregate_max<F>(self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(&Record) -> Option<f64> + Send + Sync + 'static,
    {
        self.aggregate_field(AggregateField {
            name: name.into(),
            kind: AggregateKind::Max,
            extract: Arc::new(f),
        })
    }

    /// Mean of the extracted values per window.
    pub fn aggregate_avg<F>(self, name: impl Into<String>, f: F) -> Self
    where
        F: Fn(&Record) -> Option<f64> + Send + Sync + 'static,
    {
        self.aggregate_field(AggregateField {
            name: name.into(),
            kind: AggregateKind::Avg,
            extract: Arc::new(f),
        })
    }

    // ------------------------------------------------------------------ gate

    /// Per-message allow/deny with persistent per-key state — rate limiters,
    /// throttles, circuit breakers.
    ///
    /// Returning `false` stops the batch there. The prefix that was allowed is
    /// acked, the rest keeps its lease, and the denied messages come back **in
    /// their original order** when it expires. That is why a gate needs no
    /// deferred queue and cannot reorder a partition.
    ///
    /// State mutations from a denied message are discarded — it did not happen,
    /// so it must not consume a token.
    pub fn gate<F>(self, f: F) -> Self
    where
        F: Fn(&Record, &mut GateCtx<'_>) -> bool + Send + Sync + 'static,
    {
        self.push(Op::Gate(Arc::new(f)))
    }

    // -------------------------------------------------------------- terminal

    /// Push each emitted value to a queue, inside the cycle transaction.
    pub fn to(self, sink: QueueBuilder) -> Self {
        let queue = sink.name().unwrap_or_default().to_string();
        self.push(Op::Sink(Sink {
            queue,
            partition: SinkPartition::Source,
        }))
    }

    /// Sink to a queue, choosing the partition from the emitted value.
    pub fn to_partitioned<F>(self, sink: QueueBuilder, partition: F) -> Self
    where
        F: Fn(&Value) -> String + Send + Sync + 'static,
    {
        let queue = sink.name().unwrap_or_default().to_string();
        self.push(Op::Sink(Sink {
            queue,
            partition: SinkPartition::Derived(Arc::new(partition)),
        }))
    }

    /// Run a side effect per emitted value.
    ///
    /// At-least-once: the effect runs *before* the ack commits, so a crash in
    /// between redelivers. For exactly-once, sink to a queue and drain it with
    /// a transactional consumer.
    pub fn foreach<F, Fut>(self, f: F) -> Self
    where
        F: Fn(Value, EmitCtx) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = std::result::Result<(), String>> + Send + 'static,
    {
        self.push(Op::Foreach(Arc::new(move |v, ctx| Box::pin(f(v, ctx)))))
    }

    // ------------------------------------------------------------------- run

    /// The chain's structural fingerprint — the same value the other SDKs
    /// compute for an equivalent chain.
    pub fn config_hash(&self) -> String {
        hash::config_hash(&self.ops.iter().map(|o| o.describe()).collect::<Vec<_>>())
    }

    /// Register the query and start processing.
    pub async fn run(self, queen: &Queen, opts: RunOptions) -> Result<StreamHandle> {
        let config_hash = self.config_hash();
        let source = self.source.clone();
        let stages = self.compile()?;
        runner::start(queen.inner_handle(), source, stages, config_hash, opts).await
    }

    /// Validate the chain and split it into stages.
    pub(crate) fn compile(self) -> Result<Stages> {
        let mut stages = Stages {
            pre: Vec::new(),
            key_by: None,
            window: None,
            reducer: None,
            gate: None,
            post: Vec::new(),
            terminal: None,
        };

        let total = self.ops.len();
        let mut phase = Phase::Pre;

        for (i, op) in self.ops.into_iter().enumerate() {
            let is_last = i + 1 == total;
            match op {
                Op::Sink(_) | Op::Foreach(_) if !is_last => {
                    return Err(Error::Invalid(format!(
                        "a terminal ('{}') must be the last operator, but it is at position {} of \
                         {total}",
                        op.kind(),
                        i + 1
                    )))
                }
                Op::Sink(s) => stages.terminal = Some(Terminal::Sink(s)),
                Op::Foreach(f) => stages.terminal = Some(Terminal::Foreach(f)),

                Op::Map(_) | Op::Filter(_) | Op::FlatMap(_) => match phase {
                    Phase::Reducer | Phase::Gate => stages.post.push(op),
                    _ => stages.pre.push(op),
                },

                Op::KeyBy(f) => {
                    if stages.key_by.is_some() {
                        return Err(Error::Invalid("only one key_by per stream".into()));
                    }
                    if matches!(phase, Phase::Reducer | Phase::Gate) {
                        return Err(Error::Invalid(
                            "key_by must come before the window, reducer or gate it keys".into(),
                        ));
                    }
                    stages.key_by = Some(f);
                    if matches!(phase, Phase::Pre) {
                        phase = Phase::Keyed;
                    }
                }

                Op::Window(w) => {
                    if stages.window.is_some() {
                        return Err(Error::Invalid("only one window per stream".into()));
                    }
                    if stages.gate.is_some() {
                        return Err(Error::Invalid(
                            "a window cannot share a stream with gate: a gate stops a batch \
                             part-way, and windowing assumes the whole batch commits atomically. \
                             Run them as two streams."
                                .into(),
                        ));
                    }
                    if matches!(phase, Phase::Reducer) {
                        return Err(Error::Invalid(
                            "the window must come before reduce/aggregate".into(),
                        ));
                    }
                    w.validate()?;
                    stages.window = Some(w);
                    phase = Phase::Window;
                }

                Op::Reduce(r) => {
                    if stages.reducer.is_some() {
                        return Err(Error::Invalid(
                            "only one reduce/aggregate per stream".into(),
                        ));
                    }
                    if stages.window.is_none() {
                        return Err(Error::Invalid(
                            "reduce/aggregate needs a window in front of it — without one there \
                             is nothing to close, so nothing would ever be emitted"
                                .into(),
                        ));
                    }
                    if stages.gate.is_some() {
                        return Err(Error::Invalid(
                            "reduce/aggregate cannot share a stream with gate".into(),
                        ));
                    }
                    stages.reducer = Some(r);
                    phase = Phase::Reducer;
                }

                Op::Gate(f) => {
                    if stages.gate.is_some() {
                        return Err(Error::Invalid("only one gate per stream".into()));
                    }
                    if stages.window.is_some() || stages.reducer.is_some() {
                        return Err(Error::Invalid(
                            "gate cannot share a stream with windowing or reduce".into(),
                        ));
                    }
                    stages.gate = Some(f);
                    phase = Phase::Gate;
                }
            }
        }

        if stages.window.is_some() && stages.reducer.is_none() && stages.terminal.is_none() {
            return Err(Error::Invalid(
                "a windowed stream needs a reduce/aggregate, or a terminal to send the annotated \
                 records to"
                    .into(),
            ));
        }

        Ok(stages)
    }
}

#[derive(Clone, Copy, PartialEq)]
enum Phase {
    Pre,
    Keyed,
    Window,
    Reducer,
    Gate,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Queen};

    fn queen() -> Queen {
        Queen::connect(Config::new("http://127.0.0.1:1")).unwrap()
    }

    fn stream() -> Stream {
        Stream::from(queen().queue("src"))
    }

    #[test]
    fn a_bare_chain_compiles_to_pre_stages() {
        let stages = stream()
            .map(|r| r.data.clone())
            .filter(|_| true)
            .compile()
            .unwrap();
        assert_eq!(stages.pre.len(), 2);
        assert!(stages.post.is_empty());
        assert!(stages.window.is_none());
        assert!(stages.terminal.is_none());
    }

    #[test]
    fn stateless_ops_after_a_reducer_become_post_stages() {
        let stages = stream()
            .map(|r| r.data.clone())
            .window_tumbling(60)
            .aggregate_count("count")
            .map(|r| r.data.clone())
            .filter(|_| true)
            .compile()
            .unwrap();
        assert_eq!(
            stages.pre.len(),
            1,
            "the map before the window is a pre stage"
        );
        assert_eq!(stages.post.len(), 2, "the two after the reducer are post");
    }

    #[test]
    fn a_terminal_must_be_last() {
        let err = stream()
            .to(queen().queue("out"))
            .map(|r| r.data.clone())
            .compile()
            .unwrap_err();
        assert!(
            err.to_string().contains("must be the last operator"),
            "{err}"
        );
    }

    #[test]
    fn a_terminal_at_the_end_is_accepted() {
        let stages = stream()
            .map(|r| r.data.clone())
            .to(queen().queue("out"))
            .compile()
            .unwrap();
        assert!(matches!(stages.terminal, Some(Terminal::Sink(_))));
    }

    #[test]
    fn reduce_without_a_window_is_refused() {
        let err = stream()
            .reduce(serde_json::json!(0), |acc, _| acc)
            .compile()
            .unwrap_err();
        assert!(err.to_string().contains("needs a window"), "{err}");
    }

    #[test]
    fn a_windowed_stream_needs_somewhere_for_its_output_to_go() {
        let err = stream().window_tumbling(60).compile().unwrap_err();
        assert!(err.to_string().contains("needs a reduce"), "{err}");
        // ...but a window feeding a sink directly is fine.
        assert!(stream()
            .window_tumbling(60)
            .to(queen().queue("out"))
            .compile()
            .is_ok());
    }

    #[test]
    fn gate_and_windowing_cannot_share_a_stream() {
        let err = stream()
            .gate(|_, _| true)
            .window_tumbling(60)
            .compile()
            .unwrap_err();
        assert!(err.to_string().contains("cannot share a stream"), "{err}");

        let err = stream()
            .window_tumbling(60)
            .aggregate_count("c")
            .gate(|_, _| true)
            .compile()
            .unwrap_err();
        assert!(err.to_string().contains("cannot share a stream"), "{err}");
    }

    #[test]
    fn duplicated_singleton_stages_are_refused() {
        assert!(stream()
            .window_tumbling(60)
            .window_tumbling(30)
            .compile()
            .is_err());
        assert!(stream()
            .key_by(|_| "a".into())
            .key_by(|_| "b".into())
            .compile()
            .is_err());
        assert!(stream()
            .gate(|_, _| true)
            .gate(|_, _| true)
            .compile()
            .is_err());
        assert!(stream()
            .window_tumbling(60)
            .aggregate_count("a")
            .reduce(serde_json::json!(0), |acc, _| acc)
            .compile()
            .is_err());
    }

    #[test]
    fn key_by_must_precede_the_stage_it_keys() {
        let err = stream()
            .window_tumbling(60)
            .aggregate_count("c")
            .key_by(|_| "k".into())
            .compile()
            .unwrap_err();
        assert!(err.to_string().contains("must come before"), "{err}");
    }

    #[test]
    fn an_invalid_window_is_caught_at_compile_time() {
        assert!(stream()
            .window_sliding(60, 7)
            .aggregate_count("c")
            .compile()
            .is_err());
        assert!(stream()
            .window_tumbling(0)
            .aggregate_count("c")
            .compile()
            .is_err());
    }

    #[test]
    fn aggregate_fields_accumulate_into_one_reducer() {
        let stages = stream()
            .window_tumbling(60)
            .aggregate_count("count")
            .aggregate_sum("total", |r| r.number("n"))
            .aggregate_max("peak", |r| r.number("n"))
            .compile()
            .unwrap();
        match stages.reducer.unwrap() {
            Reducer::Aggregate { fields } => {
                assert_eq!(
                    fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(),
                    vec!["count", "total", "peak"]
                );
            }
            _ => panic!("expected an aggregate"),
        }
    }

    // --- config hash agrees with the JS SDK ---------------------------------

    #[test]
    fn the_chain_hash_matches_the_js_sdk() {
        // Same shapes as the vectors in hash.rs, but built through the public
        // API — so the builder cannot drift from the wire contract.
        let h = stream()
            .map(|r| r.data.clone())
            .filter(|_| true)
            .to(queen().queue("out"))
            .config_hash();
        assert_eq!(
            h, "26e8e576fb2b4e60473d325efea14df31e80fd246930f4754557250236e49b69",
            "chain hash drifted from the JS SDK"
        );

        let h = stream()
            .window_tumbling(60)
            .aggregate_count("count")
            .aggregate_sum("sum", |r| r.number("amount"))
            .to(queen().queue("out"))
            .config_hash();
        assert_eq!(
            h,
            "ed0617b6fa58103f0e4ac04443e6ea8e9a890a1ba5575b395c9d089300bfd743"
        );

        let h = stream()
            .gate(|_, _| true)
            .to(queen().queue("out"))
            .config_hash();
        assert_eq!(
            h,
            "5fd7e4a41b14cfa1442b1ae0dbd020cc1110911bc923b4dd42f5f925ac5b0b1d"
        );

        let h = stream()
            .window_cron(Every::Minute)
            .reduce(serde_json::json!(0), |acc, _| acc)
            .config_hash();
        assert_eq!(
            h,
            "0dbc1015f01f460eba75be6b67c07071e8cbb5466d13120eeb8afc96912d30e1"
        );
    }

    #[test]
    fn changing_a_window_changes_the_hash() {
        let a = stream()
            .window_tumbling(60)
            .aggregate_count("c")
            .config_hash();
        let b = stream()
            .window_tumbling(30)
            .aggregate_count("c")
            .config_hash();
        assert_ne!(a, b);

        // ...and so does turning on event time, which changes how state is
        // interpreted even though the shape looks the same.
        let c = stream()
            .window_tumbling(60)
            .event_time(|_| Some(0))
            .aggregate_count("c")
            .config_hash();
        assert_ne!(a, c);
    }

    #[test]
    fn window_options_apply_to_the_most_recent_window() {
        let stages = stream()
            .window_tumbling(60)
            .grace_seconds(5)
            .allowed_lateness(2)
            .on_late(LatePolicy::Include)
            .idle_flush_ms(250)
            .aggregate_count("c")
            .compile()
            .unwrap();
        let w = stages.window.unwrap();
        assert_eq!(w.grace_seconds, 5);
        assert_eq!(w.grace_ms(), 5_000);
        assert_eq!(w.allowed_lateness_seconds, 2);
        assert_eq!(w.on_late, LatePolicy::Include);
        assert_eq!(w.idle_flush_ms, 250);
    }
}
