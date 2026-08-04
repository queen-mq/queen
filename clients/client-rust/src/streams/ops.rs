//! Operators and the values that flow between them.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde_json::Value;

use queen_protocol::Message;

use super::hash::{Json, OpDescription};
use crate::error::{Error, Result};

/// What a user function sees.
///
/// Before a reducer, `data` is the source message's payload and `message` is
/// the message it came from. After one, `data` is the emitted value, `message`
/// is `None` (an aggregate has no single source message) and `ctx` describes
/// the window it closed. The same operator types run on both sides, which is
/// why this carries all three rather than being two separate types.
#[derive(Debug, Clone)]
pub struct Record {
    pub data: Value,
    pub message: Option<Message>,
    pub ctx: Option<EmitCtx>,
}

impl Record {
    pub(crate) fn from_message(message: Message) -> Self {
        Self {
            data: message.data.clone(),
            message: Some(message),
            ctx: None,
        }
    }

    /// Read a field of the payload, or `Value::Null`.
    pub fn field(&self, name: &str) -> &Value {
        self.data.get(name).unwrap_or(&Value::Null)
    }

    /// Read a numeric field, if it is one.
    pub fn number(&self, name: &str) -> Option<f64> {
        self.data.get(name).and_then(|v| v.as_f64())
    }

    /// Read a string field, if it is one.
    pub fn text(&self, name: &str) -> Option<&str> {
        self.data.get(name).and_then(|v| v.as_str())
    }
}

/// Where an emitted value came from.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct EmitCtx {
    pub partition: String,
    pub partition_id: String,
    pub key: String,
    pub window_key: Option<String>,
    /// Epoch milliseconds.
    pub window_start: Option<i64>,
    /// Epoch milliseconds.
    pub window_end: Option<i64>,
}

/// Mutable per-key state a gate reads and writes.
///
/// Mutations are persisted **only** when the gate allows the message. A denied
/// message's changes are discarded — it did not happen, so it must not consume
/// a token.
pub struct GateCtx<'a> {
    pub state: &'a mut Value,
    /// Wall clock for this cycle, for refill arithmetic.
    pub stream_time_ms: i64,
    pub partition_id: &'a str,
    pub partition: &'a str,
    pub key: &'a str,
}

impl GateCtx<'_> {
    /// Read a numeric state field, defaulting when absent.
    pub fn num(&self, key: &str, default: f64) -> f64 {
        self.state
            .get(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(default)
    }

    /// Write a numeric state field.
    pub fn set_num(&mut self, key: &str, value: f64) {
        if !self.state.is_object() {
            *self.state = Value::Object(Default::default());
        }
        if let Some(obj) = self.state.as_object_mut() {
            obj.insert(
                key.to_string(),
                serde_json::Number::from_f64(value)
                    .map(Value::Number)
                    .unwrap_or(Value::Null),
            );
        }
    }
}

// --------------------------------------------------------------- fn aliases

pub type MapFn = Arc<dyn Fn(&Record) -> Value + Send + Sync>;
pub type FilterFn = Arc<dyn Fn(&Record) -> bool + Send + Sync>;
pub type FlatMapFn = Arc<dyn Fn(&Record) -> Vec<Value> + Send + Sync>;
pub type KeyByFn = Arc<dyn Fn(&Record) -> String + Send + Sync>;
/// `(accumulator, record) -> accumulator`.
pub type ReduceFn = Arc<dyn Fn(Value, &Record) -> Value + Send + Sync>;
pub type GateFn = Arc<dyn Fn(&Record, &mut GateCtx<'_>) -> bool + Send + Sync>;
/// `message -> epoch milliseconds`. `None` means the timestamp could not be
/// determined, which is a hard error rather than a silent fallback.
pub type EventTimeFn = Arc<dyn Fn(&Message) -> Option<i64> + Send + Sync>;
pub type PartitionFn = Arc<dyn Fn(&Value) -> String + Send + Sync>;
/// One aggregate field's contribution.
pub type Extractor = Arc<dyn Fn(&Record) -> Option<f64> + Send + Sync>;

pub type ForeachFuture = Pin<Box<dyn Future<Output = std::result::Result<(), String>> + Send>>;
pub type ForeachFn = Arc<dyn Fn(Value, EmitCtx) -> ForeachFuture + Send + Sync>;

// ------------------------------------------------------------------ windows

/// Wall-clock alignment for [`WindowKind::Cron`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Every {
    Second,
    Minute,
    Hour,
    Day,
    Week,
}

impl Every {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
        }
    }

    pub fn millis(self) -> i64 {
        match self {
            Self::Second => 1_000,
            Self::Minute => 60_000,
            Self::Hour => 3_600_000,
            Self::Day => 86_400_000,
            Self::Week => 7 * 86_400_000,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowKind {
    Tumbling { seconds: i64 },
    Sliding { size: i64, slide: i64 },
    Session { gap: i64 },
    Cron { every: Every },
}

/// What to do with an event older than the watermark.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LatePolicy {
    /// Drop it. The default, and the only one that keeps emits exact.
    #[default]
    Drop,
    /// Accumulate anyway. Best-effort: if the window already closed this
    /// recreates its state row and produces a second emit for it.
    Include,
}

impl LatePolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Drop => "drop",
            Self::Include => "include",
        }
    }
}

/// A window operator: how events are bucketed, and when a bucket is done.
#[derive(Clone)]
pub struct Window {
    pub kind: WindowKind,
    /// Extra time a closed window stays open for stragglers.
    pub grace_seconds: i64,
    /// How often the runner sweeps quiet partitions for ripe windows.
    /// 0 disables it — which means a window on a partition that goes silent
    /// never closes until traffic returns.
    pub idle_flush_ms: i64,
    /// Present in event-time mode.
    pub event_time: Option<EventTimeFn>,
    pub allowed_lateness_seconds: i64,
    pub on_late: LatePolicy,
}

impl fmt::Debug for Window {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Window")
            .field("kind", &self.kind)
            .field("grace_seconds", &self.grace_seconds)
            .field("idle_flush_ms", &self.idle_flush_ms)
            .field("event_time", &self.event_time.is_some())
            .field("allowed_lateness_seconds", &self.allowed_lateness_seconds)
            .field("on_late", &self.on_late)
            .finish()
    }
}

impl Window {
    pub fn new(kind: WindowKind) -> Self {
        Self {
            kind,
            grace_seconds: 0,
            // Matches the other SDKs: 5s for tumbling/sliding, 1s for session
            // (sessions are far more lateness-sensitive), 30s for cron.
            idle_flush_ms: match kind {
                WindowKind::Session { .. } => 1_000,
                WindowKind::Cron { .. } => 30_000,
                _ => 5_000,
            },
            event_time: None,
            allowed_lateness_seconds: 0,
            on_late: LatePolicy::Drop,
        }
    }

    /// The tag that scopes this operator's state rows, so two windows in one
    /// query never collide and the idle flush can scan by prefix.
    pub fn tag(&self) -> String {
        match self.kind {
            WindowKind::Tumbling { seconds } => format!("tumb:{seconds}"),
            WindowKind::Sliding { size, slide } => format!("slide:{size}:{slide}"),
            WindowKind::Session { gap } => format!("sess:{gap}"),
            WindowKind::Cron { every } => format!("cron:{}", every.as_str()),
        }
    }

    pub fn grace_ms(&self) -> i64 {
        self.grace_seconds.max(0) * 1000
    }

    pub fn allowed_lateness_ms(&self) -> i64 {
        self.allowed_lateness_seconds.max(0) * 1000
    }

    pub fn is_session(&self) -> bool {
        matches!(self.kind, WindowKind::Session { .. })
    }

    pub(crate) fn validate(&self) -> Result<()> {
        match self.kind {
            WindowKind::Tumbling { seconds } if seconds <= 0 => Err(Error::Invalid(
                "window_tumbling needs a positive number of seconds".into(),
            )),
            WindowKind::Session { gap } if gap <= 0 => Err(Error::Invalid(
                "window_session needs a positive gap in seconds".into(),
            )),
            WindowKind::Sliding { size, slide } => {
                if size <= 0 || slide <= 0 {
                    return Err(Error::Invalid(
                        "window_sliding needs a positive size and slide".into(),
                    ));
                }
                if size % slide != 0 {
                    // Otherwise the per-event window count is not an integer
                    // and events near a boundary land in a different number of
                    // windows than events in the middle.
                    return Err(Error::Invalid(format!(
                        "window_sliding: size ({size}) must be a whole multiple of slide \
                         ({slide}) to keep the per-event window count finite"
                    )));
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    /// Timestamp for a message: the extractor in event-time mode, the broker's
    /// `createdAt` otherwise.
    pub(crate) fn timestamp_of(&self, message: &Message) -> Option<i64> {
        match &self.event_time {
            Some(f) => f(message),
            None => parse_iso_ms(&message.created_at),
        }
    }

    pub(crate) fn describe(&self) -> OpDescription {
        let mut d: OpDescription = Vec::new();
        match self.kind {
            WindowKind::Tumbling { seconds } => {
                d.push(("kind", Json::Str("window-tumbling".into())));
                d.push(("seconds", Json::Int(seconds)));
            }
            WindowKind::Sliding { size, slide } => {
                d.push(("kind", Json::Str("window-sliding".into())));
                d.push(("size", Json::Int(size)));
                d.push(("slide", Json::Int(slide)));
            }
            WindowKind::Session { gap } => {
                d.push(("kind", Json::Str("window-session".into())));
                d.push(("gap", Json::Int(gap)));
            }
            WindowKind::Cron { every } => {
                d.push(("kind", Json::Str("window-cron".into())));
                d.push(("every", Json::Str(every.as_str().into())));
            }
        }
        d.push(("gracePeriod", Json::Int(self.grace_seconds)));
        d.push(("idleFlushMs", Json::Int(self.idle_flush_ms)));
        d.push(("eventTime", Json::Bool(self.event_time.is_some())));
        d.push(("allowedLateness", Json::Int(self.allowed_lateness_seconds)));
        d.push(("onLate", Json::Str(self.on_late.as_str().into())));
        d
    }
}

// ------------------------------------------------------------------ reducer

/// One named field of an aggregate.
#[derive(Clone)]
pub struct AggregateField {
    pub name: String,
    pub kind: AggregateKind,
    pub extract: Extractor,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggregateKind {
    Count,
    Sum,
    Min,
    Max,
    Avg,
    /// Any other name: a numeric running total.
    Custom,
}

/// A fold over a window, either hand-written or built from named aggregates.
#[derive(Clone)]
pub enum Reducer {
    Fold {
        fold: ReduceFn,
        initial: Option<Value>,
    },
    Aggregate {
        fields: Vec<AggregateField>,
    },
}

impl fmt::Debug for Reducer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fold { initial, .. } => f
                .debug_struct("Fold")
                .field("has_initial", &initial.is_some())
                .finish(),
            Self::Aggregate { fields } => f
                .debug_struct("Aggregate")
                .field(
                    "fields",
                    &fields.iter().map(|x| &x.name).collect::<Vec<_>>(),
                )
                .finish(),
        }
    }
}

impl Reducer {
    pub(crate) fn describe(&self) -> OpDescription {
        match self {
            Self::Fold { initial, .. } => vec![
                ("kind", Json::Str("reduce".into())),
                ("hasInitial", Json::Bool(initial.is_some())),
            ],
            Self::Aggregate { fields } => vec![
                ("kind", Json::Str("aggregate".into())),
                (
                    "fields",
                    Json::StrArray(fields.iter().map(|f| f.name.clone()).collect()),
                ),
            ],
        }
    }

    /// A fresh accumulator for a window that has not been seen before.
    pub(crate) fn initial(&self) -> Value {
        match self {
            Self::Fold { initial, .. } => initial.clone().unwrap_or(Value::Null),
            Self::Aggregate { fields } => {
                let mut obj = serde_json::Map::new();
                for f in fields {
                    match f.kind {
                        // min/max start empty rather than at zero: a running
                        // min seeded with 0 would never rise above it.
                        AggregateKind::Min | AggregateKind::Max => {
                            obj.insert(f.name.clone(), Value::Null);
                        }
                        AggregateKind::Avg => {
                            obj.insert("__avg_sum".into(), num(0.0));
                            obj.insert("__avg_count".into(), num(0.0));
                            obj.insert(f.name.clone(), num(0.0));
                        }
                        _ => {
                            obj.insert(f.name.clone(), num(0.0));
                        }
                    }
                }
                Value::Object(obj)
            }
        }
    }

    /// Fold one record into an accumulator.
    pub(crate) fn step(&self, acc: Value, record: &Record) -> Value {
        match self {
            Self::Fold { fold, .. } => fold(acc, record),
            Self::Aggregate { fields } => {
                let mut obj = match acc {
                    Value::Object(m) => m,
                    _ => serde_json::Map::new(),
                };
                for f in fields {
                    let v = (f.extract)(record);
                    match f.kind {
                        AggregateKind::Count => {
                            let cur = obj.get(&f.name).and_then(|x| x.as_f64()).unwrap_or(0.0);
                            obj.insert(f.name.clone(), num(cur + v.unwrap_or(1.0)));
                        }
                        AggregateKind::Sum | AggregateKind::Custom => {
                            let cur = obj.get(&f.name).and_then(|x| x.as_f64()).unwrap_or(0.0);
                            obj.insert(f.name.clone(), num(cur + v.unwrap_or(0.0)));
                        }
                        AggregateKind::Min => {
                            if let Some(v) = v {
                                let cur = obj.get(&f.name).and_then(|x| x.as_f64());
                                obj.insert(f.name.clone(), num(cur.map_or(v, |c| c.min(v))));
                            }
                        }
                        AggregateKind::Max => {
                            if let Some(v) = v {
                                let cur = obj.get(&f.name).and_then(|x| x.as_f64());
                                obj.insert(f.name.clone(), num(cur.map_or(v, |c| c.max(v))));
                            }
                        }
                        AggregateKind::Avg => {
                            if let Some(v) = v {
                                let sum =
                                    obj.get("__avg_sum").and_then(|x| x.as_f64()).unwrap_or(0.0)
                                        + v;
                                let count = obj
                                    .get("__avg_count")
                                    .and_then(|x| x.as_f64())
                                    .unwrap_or(0.0)
                                    + 1.0;
                                obj.insert("__avg_sum".into(), num(sum));
                                obj.insert("__avg_count".into(), num(count));
                                obj.insert(
                                    f.name.clone(),
                                    num(if count > 0.0 { sum / count } else { 0.0 }),
                                );
                            }
                        }
                    }
                }
                Value::Object(obj)
            }
        }
    }
}

fn num(v: f64) -> Value {
    serde_json::Number::from_f64(v)
        .map(Value::Number)
        .unwrap_or(Value::Null)
}

// --------------------------------------------------------------------- sink

#[derive(Clone)]
pub enum SinkPartition {
    /// Reuse the source partition, keeping an entity's lane intact end to end.
    Source,
    Fixed(String),
    Derived(PartitionFn),
}

#[derive(Clone)]
pub struct Sink {
    pub queue: String,
    pub partition: SinkPartition,
}

impl Sink {
    pub(crate) fn resolve_partition(&self, value: &Value, source_partition: &str) -> String {
        match &self.partition {
            SinkPartition::Source => {
                if source_partition.is_empty() {
                    queen_protocol::DEFAULT_PARTITION.to_string()
                } else {
                    source_partition.to_string()
                }
            }
            SinkPartition::Fixed(p) => p.clone(),
            SinkPartition::Derived(f) => f(value),
        }
    }
}

// ---------------------------------------------------------------- operators

/// One link in the chain.
#[derive(Clone)]
pub(crate) enum Op {
    Map(MapFn),
    Filter(FilterFn),
    FlatMap(FlatMapFn),
    KeyBy(KeyByFn),
    Window(Window),
    Reduce(Reducer),
    Gate(GateFn),
    Sink(Sink),
    Foreach(ForeachFn),
}

impl Op {
    pub(crate) fn kind(&self) -> &'static str {
        match self {
            Self::Map(_) => "map",
            Self::Filter(_) => "filter",
            Self::FlatMap(_) => "flatMap",
            Self::KeyBy(_) => "keyBy",
            Self::Window(_) => "window",
            Self::Reduce(_) => "reduce",
            Self::Gate(_) => "gate",
            Self::Sink(_) => "sink",
            Self::Foreach(_) => "foreach",
        }
    }

    pub(crate) fn describe(&self) -> OpDescription {
        match self {
            Self::Map(_) => vec![("kind", Json::Str("map".into()))],
            Self::Filter(_) => vec![("kind", Json::Str("filter".into()))],
            Self::FlatMap(_) => vec![("kind", Json::Str("flatMap".into()))],
            Self::KeyBy(_) => vec![("kind", Json::Str("keyBy".into()))],
            Self::Gate(_) => vec![("kind", Json::Str("gate".into()))],
            Self::Window(w) => w.describe(),
            Self::Reduce(r) => r.describe(),
            Self::Sink(s) => vec![
                ("kind", Json::Str("sink".into())),
                ("queue", Json::Str(s.queue.clone())),
            ],
            Self::Foreach(_) => vec![("kind", Json::Str("foreach".into()))],
        }
    }
}

// -------------------------------------------------------------------- time

/// Parse an ISO-8601 timestamp to epoch milliseconds.
///
/// Hand-rolled rather than pulling in a date crate: the only timestamps this
/// sees are the broker's own `createdAt`, whose shape is fixed
/// (`YYYY-MM-DDTHH:MM:SS[.fff]Z` or with a `+00` offset).
pub fn parse_iso_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 {
        return None;
    }
    let n = |from: usize, to: usize| -> Option<i64> { s.get(from..to)?.parse::<i64>().ok() };
    let year = n(0, 4)?;
    let month = n(5, 7)?;
    let day = n(8, 10)?;
    let hour = n(11, 13)?;
    let min = n(14, 16)?;
    let sec = n(17, 19)?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }

    let mut millis = 0i64;
    if b.len() > 19 && b[19] == b'.' {
        let frac: String = s[20..]
            .chars()
            .take_while(|c| c.is_ascii_digit())
            .take(3)
            .collect();
        if !frac.is_empty() {
            let padded = format!("{frac:0<3}");
            millis = padded.parse::<i64>().ok()?;
        }
    }

    let days = days_from_civil(year, month, day);
    Some(((days * 86_400 + hour * 3_600 + min * 60 + sec) * 1_000) + millis)
}

/// Days since the Unix epoch for a civil date. Howard Hinnant's algorithm.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// Format epoch milliseconds as the ISO-8601 string used for window keys.
///
/// Must match JavaScript's `new Date(ms).toISOString()` exactly — the window
/// key is part of the state key, so a different rendering would make this SDK
/// unable to read state written by another.
pub fn format_iso_ms(ms: i64) -> String {
    let days = ms.div_euclid(86_400_000);
    let rem = ms.rem_euclid(86_400_000);
    let (y, m, d) = civil_from_days(days);
    let hour = rem / 3_600_000;
    let min = (rem % 3_600_000) / 60_000;
    let sec = (rem % 60_000) / 1_000;
    let milli = rem % 1_000;
    format!("{y:04}-{m:02}-{d:02}T{hour:02}:{min:02}:{sec:02}.{milli:03}Z")
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_parsing_handles_the_brokers_shapes() {
        assert_eq!(parse_iso_ms("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_iso_ms("1970-01-01T00:00:00.000Z"), Some(0));
        assert_eq!(parse_iso_ms("1970-01-01T00:00:01.500Z"), Some(1500));
        assert_eq!(
            parse_iso_ms("2026-08-04T10:00:00.000Z"),
            Some(1_785_837_600_000)
        );
        // Postgres renders microseconds; only the first three digits matter.
        assert_eq!(
            parse_iso_ms("2026-08-04T10:00:00.123456Z"),
            Some(1_785_837_600_123)
        );
        // Short fractions are padded, not truncated: .1 is 100ms, not 1ms.
        assert_eq!(
            parse_iso_ms("2026-08-04T10:00:00.1Z"),
            Some(1_785_837_600_100)
        );
        assert_eq!(
            parse_iso_ms("2026-08-04T10:00:00.12Z"),
            Some(1_785_837_600_120)
        );
    }

    #[test]
    fn iso_parsing_rejects_nonsense() {
        assert_eq!(parse_iso_ms(""), None);
        assert_eq!(parse_iso_ms("not a date"), None);
        assert_eq!(parse_iso_ms("2026-13-04T10:00:00Z"), None);
        assert_eq!(parse_iso_ms("2026-08-99T10:00:00Z"), None);
    }

    #[test]
    fn iso_formatting_matches_javascript_to_iso_string() {
        // Window keys are part of state keys, so this has to agree with the
        // other SDKs character for character.
        assert_eq!(format_iso_ms(0), "1970-01-01T00:00:00.000Z");
        assert_eq!(format_iso_ms(1_785_837_600_000), "2026-08-04T10:00:00.000Z");
        assert_eq!(format_iso_ms(1_785_837_600_123), "2026-08-04T10:00:00.123Z");
        assert_eq!(format_iso_ms(1_000), "1970-01-01T00:00:01.000Z");
    }

    #[test]
    fn iso_round_trips() {
        for ms in [0i64, 1_000, 1_785_837_600_123, 946_684_800_000] {
            assert_eq!(parse_iso_ms(&format_iso_ms(ms)), Some(ms), "ms={ms}");
        }
    }

    #[test]
    fn window_tags_scope_state_per_operator() {
        assert_eq!(
            Window::new(WindowKind::Tumbling { seconds: 60 }).tag(),
            "tumb:60"
        );
        assert_eq!(
            Window::new(WindowKind::Sliding {
                size: 60,
                slide: 10
            })
            .tag(),
            "slide:60:10"
        );
        assert_eq!(
            Window::new(WindowKind::Session { gap: 30 }).tag(),
            "sess:30"
        );
        assert_eq!(
            Window::new(WindowKind::Cron {
                every: Every::Minute
            })
            .tag(),
            "cron:minute"
        );
    }

    #[test]
    fn sliding_requires_size_to_be_a_multiple_of_slide() {
        assert!(Window::new(WindowKind::Sliding {
            size: 60,
            slide: 10
        })
        .validate()
        .is_ok());
        let err = Window::new(WindowKind::Sliding { size: 60, slide: 7 })
            .validate()
            .unwrap_err();
        assert!(err.to_string().contains("whole multiple"));
    }

    #[test]
    fn windows_reject_non_positive_sizes() {
        assert!(Window::new(WindowKind::Tumbling { seconds: 0 })
            .validate()
            .is_err());
        assert!(Window::new(WindowKind::Session { gap: -1 })
            .validate()
            .is_err());
    }

    #[test]
    fn idle_flush_defaults_differ_per_window_kind() {
        // Sessions close on silence, so they need a tighter sweep than a
        // tumbling window whose boundary is known in advance.
        assert_eq!(
            Window::new(WindowKind::Tumbling { seconds: 60 }).idle_flush_ms,
            5_000
        );
        assert_eq!(
            Window::new(WindowKind::Session { gap: 30 }).idle_flush_ms,
            1_000
        );
        assert_eq!(
            Window::new(WindowKind::Cron {
                every: Every::Minute
            })
            .idle_flush_ms,
            30_000
        );
    }

    fn record(data: serde_json::Value) -> Record {
        Record {
            data,
            message: None,
            ctx: None,
        }
    }

    #[test]
    fn aggregate_count_defaults_to_one_per_record() {
        let r = Reducer::Aggregate {
            fields: vec![AggregateField {
                name: "count".into(),
                kind: AggregateKind::Count,
                extract: Arc::new(|_| None),
            }],
        };
        let mut acc = r.initial();
        for _ in 0..3 {
            acc = r.step(acc, &record(serde_json::json!({})));
        }
        assert_eq!(acc["count"], 3.0);
    }

    #[test]
    fn aggregate_min_max_ignore_missing_values() {
        let r = Reducer::Aggregate {
            fields: vec![
                AggregateField {
                    name: "min".into(),
                    kind: AggregateKind::Min,
                    extract: Arc::new(|rec| rec.number("v")),
                },
                AggregateField {
                    name: "max".into(),
                    kind: AggregateKind::Max,
                    extract: Arc::new(|rec| rec.number("v")),
                },
            ],
        };
        let mut acc = r.initial();
        // A fresh min/max is null, not zero — otherwise a stream of positive
        // values would report a minimum of 0.
        assert_eq!(acc["min"], serde_json::Value::Null);

        for v in [5.0, 2.0, 9.0] {
            acc = r.step(acc, &record(serde_json::json!({ "v": v })));
        }
        acc = r.step(acc, &record(serde_json::json!({ "other": 1 })));
        assert_eq!(acc["min"], 2.0);
        assert_eq!(acc["max"], 9.0);
    }

    #[test]
    fn aggregate_avg_tracks_sum_and_count_internally() {
        let r = Reducer::Aggregate {
            fields: vec![AggregateField {
                name: "avg".into(),
                kind: AggregateKind::Avg,
                extract: Arc::new(|rec| rec.number("v")),
            }],
        };
        let mut acc = r.initial();
        for v in [1.0, 2.0, 6.0] {
            acc = r.step(acc, &record(serde_json::json!({ "v": v })));
        }
        assert_eq!(acc["avg"], 3.0);
        assert_eq!(acc["__avg_count"], 3.0);
    }

    #[test]
    fn a_custom_aggregate_field_is_a_running_total() {
        let r = Reducer::Aggregate {
            fields: vec![AggregateField {
                name: "revenue".into(),
                kind: AggregateKind::Custom,
                extract: Arc::new(|rec| rec.number("amount")),
            }],
        };
        let mut acc = r.initial();
        for v in [10.0, 2.5] {
            acc = r.step(acc, &record(serde_json::json!({ "amount": v })));
        }
        assert_eq!(acc["revenue"], 12.5);
    }

    #[test]
    fn a_fold_uses_its_initial_value() {
        let r = Reducer::Fold {
            fold: Arc::new(|acc, rec| {
                let cur = acc.as_i64().unwrap_or(0);
                serde_json::json!(cur + rec.number("n").unwrap_or(0.0) as i64)
            }),
            initial: Some(serde_json::json!(100)),
        };
        let mut acc = r.initial();
        assert_eq!(acc, serde_json::json!(100));
        acc = r.step(acc, &record(serde_json::json!({ "n": 5 })));
        assert_eq!(acc, serde_json::json!(105));
    }

    #[test]
    fn sink_partition_falls_back_to_the_source_lane() {
        let s = Sink {
            queue: "out".into(),
            partition: SinkPartition::Source,
        };
        assert_eq!(s.resolve_partition(&serde_json::json!({}), "eu"), "eu");
        // An unnamed source lane still has to produce a valid partition.
        assert_eq!(s.resolve_partition(&serde_json::json!({}), ""), "Default");

        let s = Sink {
            queue: "out".into(),
            partition: SinkPartition::Derived(Arc::new(|v| {
                v.get("tenant")
                    .and_then(|t| t.as_str())
                    .unwrap_or("none")
                    .to_string()
            })),
        };
        assert_eq!(
            s.resolve_partition(&serde_json::json!({ "tenant": "acme" }), "eu"),
            "acme"
        );
    }

    #[test]
    fn gate_ctx_reads_and_writes_numeric_state() {
        let mut state = serde_json::json!({});
        let mut ctx = GateCtx {
            state: &mut state,
            stream_time_ms: 0,
            partition_id: "p",
            partition: "P",
            key: "k",
        };
        assert_eq!(ctx.num("tokens", 10.0), 10.0);
        ctx.set_num("tokens", 9.0);
        assert_eq!(ctx.num("tokens", 10.0), 9.0);
    }
}
