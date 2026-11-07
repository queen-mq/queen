//! The functions a feed stream calls and the operators that hold them.
//!
//! Every function sees messages as JSON in the JS SDK's shapes (a feed
//! message: `position`, `kind`, `queue`, `partition`, `partitionId`,
//! `offset`, `id`, `transactionId`, `data`, `ts`, `createdAt`; a step's
//! result after a window, pattern or gather: see the JS `messageOf`). A
//! function may FAIL: it answers a `Result` whose error meets the stream's
//! poison policy, or it panics (caught, and a failure like an error).
//!
//! What a function answers is converted with the `IntoFeed*` traits: a JSON
//! value, `None` (JavaScript's `undefined`: "nothing"), or either in a
//! `Result`.

use std::any::Any;
use std::fmt::Display;
use std::future::Future;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::SystemTime;

use serde_json::Value;

use crate::js;

/// A function's verdict: its answer, or why it failed.
pub type FnResult<T> = std::result::Result<T, String>;

pub(crate) type BoxFuture<T> = Pin<Box<dyn Future<Output = T> + Send>>;

// ------------------------------------------------------------- conversions

/// What a value function may answer.
pub trait IntoFeedValue {
    fn into_feed_value(self) -> FnResult<Option<Value>>;
}

impl IntoFeedValue for Value {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(self))
    }
}
impl IntoFeedValue for Option<Value> {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(self)
    }
}
impl IntoFeedValue for () {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(None)
    }
}
impl IntoFeedValue for String {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(Value::String(self)))
    }
}
impl IntoFeedValue for &str {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(Value::String(self.to_string())))
    }
}
impl IntoFeedValue for bool {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(Value::Bool(self)))
    }
}
impl IntoFeedValue for i64 {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(Value::from(self)))
    }
}
impl IntoFeedValue for i32 {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(Value::from(self)))
    }
}
impl IntoFeedValue for u64 {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(Value::from(self)))
    }
}
impl IntoFeedValue for f64 {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        Ok(Some(js::num(self)))
    }
}
impl<T: IntoFeedValue, E: Display> IntoFeedValue for std::result::Result<T, E> {
    fn into_feed_value(self) -> FnResult<Option<Value>> {
        self.map_err(|e| e.to_string())?.into_feed_value()
    }
}

/// What a test may answer: a `bool`, or a JSON value read by JavaScript's
/// truthiness.
pub trait IntoFeedBool {
    fn into_feed_bool(self) -> FnResult<bool>;
}
impl IntoFeedBool for bool {
    fn into_feed_bool(self) -> FnResult<bool> {
        Ok(self)
    }
}
impl IntoFeedBool for Value {
    fn into_feed_bool(self) -> FnResult<bool> {
        Ok(js::truthy(Some(&self)))
    }
}
impl IntoFeedBool for Option<Value> {
    fn into_feed_bool(self) -> FnResult<bool> {
        Ok(js::truthy(self.as_ref()))
    }
}
impl<T: IntoFeedBool, E: Display> IntoFeedBool for std::result::Result<T, E> {
    fn into_feed_bool(self) -> FnResult<bool> {
        self.map_err(|e| e.to_string())?.into_feed_bool()
    }
}

/// What a `.flat_map()` function may answer: a list (a JSON value that is not
/// a list is a failure, as in JS).
pub trait IntoFeedList {
    fn into_feed_list(self) -> FnResult<Vec<Value>>;
}
impl IntoFeedList for Vec<Value> {
    fn into_feed_list(self) -> FnResult<Vec<Value>> {
        Ok(self)
    }
}
impl IntoFeedList for Value {
    fn into_feed_list(self) -> FnResult<Vec<Value>> {
        match self {
            Value::Array(items) => Ok(items),
            other => Err(format!(
                "flatMap fn must return an array (got {})",
                type_of(Some(&other))
            )),
        }
    }
}
impl IntoFeedList for Option<Value> {
    fn into_feed_list(self) -> FnResult<Vec<Value>> {
        match self {
            Some(v) => v.into_feed_list(),
            None => Err("flatMap fn must return an array (got undefined)".into()),
        }
    }
}
impl<T: IntoFeedList, E: Display> IntoFeedList for std::result::Result<T, E> {
    fn into_feed_list(self) -> FnResult<Vec<Value>> {
        self.map_err(|e| e.to_string())?.into_feed_list()
    }
}

/// JavaScript's `typeof`, for messages.
pub(crate) fn type_of(v: Option<&Value>) -> &'static str {
    match v {
        None => "undefined",
        Some(Value::Bool(_)) => "boolean",
        Some(Value::Number(_)) => "number",
        Some(Value::String(_)) => "string",
        Some(_) => "object",
    }
}

/// What an `eventTime` function may answer: epoch milliseconds, an ISO time,
/// a [`SystemTime`], or nothing (no time: the message meets the poison
/// policy). As JS `extractEventTime`: a JSON number is milliseconds, a JSON
/// string is read with `Date.parse`, anything else is no time.
pub trait IntoEventTime {
    fn into_event_time(self) -> FnResult<Option<f64>>;
}
impl IntoEventTime for Value {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        Ok(match &self {
            Value::Number(n) => n.as_f64().filter(|x| x.is_finite()),
            Value::String(s) => js::parse_date(s),
            _ => None,
        })
    }
}
impl IntoEventTime for Option<Value> {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        match self {
            Some(v) => v.into_event_time(),
            None => Ok(None),
        }
    }
}
impl IntoEventTime for i64 {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        Ok(Some(self as f64))
    }
}
impl IntoEventTime for f64 {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        Ok(Some(self).filter(|x| x.is_finite()))
    }
}
impl IntoEventTime for String {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        Ok(js::parse_date(&self))
    }
}
impl IntoEventTime for &str {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        Ok(js::parse_date(self))
    }
}
impl IntoEventTime for SystemTime {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        Ok(Some(match self.duration_since(SystemTime::UNIX_EPOCH) {
            Ok(d) => d.as_millis() as f64,
            Err(e) => -(e.duration().as_millis() as f64),
        }))
    }
}
impl<T: IntoEventTime, E: Display> IntoEventTime for std::result::Result<T, E> {
    fn into_event_time(self) -> FnResult<Option<f64>> {
        self.map_err(|e| e.to_string())?.into_event_time()
    }
}

/// What a `.foreach()` function may answer: nothing, or a `Result`.
pub trait IntoFeedDone {
    fn into_feed_done(self) -> FnResult<()>;
}
impl IntoFeedDone for () {
    fn into_feed_done(self) -> FnResult<()> {
        Ok(())
    }
}
impl<E: Display> IntoFeedDone for std::result::Result<(), E> {
    fn into_feed_done(self) -> FnResult<()> {
        self.map_err(|e| e.to_string())
    }
}

// ------------------------------------------------------------------ panics

fn panic_text(p: Box<dyn Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        (*s).to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "a function panicked".to_string()
    }
}

/// Run a user function: its error, or its panic, is a failure.
pub(crate) fn call<T>(f: impl FnOnce() -> FnResult<T>) -> FnResult<T> {
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(r) => r,
        Err(p) => Err(panic_text(p)),
    }
}

/// A future whose panic is a failure.
pub(crate) struct CatchUnwind<F>(pub Pin<Box<F>>);

impl<F: Future> Future for CatchUnwind<F> {
    type Output = FnResult<F::Output>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let fut = self.0.as_mut();
        match catch_unwind(AssertUnwindSafe(move || fut.poll(cx))) {
            Ok(Poll::Ready(v)) => Poll::Ready(Ok(v)),
            Ok(Poll::Pending) => Poll::Pending,
            Err(p) => Poll::Ready(Err(panic_text(p))),
        }
    }
}

// ---------------------------------------------------------- erased fn types

/// `(msg, ctx) -> value`: map.
pub(crate) type MapFn =
    Arc<dyn Fn(&Value, Option<&Value>) -> FnResult<Option<Value>> + Send + Sync>;
/// `(msg, ctx) -> bool`: filter.
pub(crate) type FilterFn = Arc<dyn Fn(&Value, Option<&Value>) -> FnResult<bool> + Send + Sync>;
/// `(msg, ctx) -> list`: flatMap.
pub(crate) type FlatMapFn =
    Arc<dyn Fn(&Value, Option<&Value>) -> FnResult<Vec<Value>> + Send + Sync>;
/// `(x) -> value`: keyBy (msg), a table's key and value (row), shardBy (msg).
pub(crate) type Fn1 = Arc<dyn Fn(&Value) -> FnResult<Option<Value>> + Send + Sync>;
/// `(msg) -> bool`: a pattern's start and then, a join's left and right.
pub(crate) type Pred1 = Arc<dyn Fn(&Value) -> FnResult<bool> + Send + Sync>;
/// `(msg, value) -> value`: key, keep, count, a table's on.
pub(crate) type Fn2 = Arc<dyn Fn(&Value, Option<&Value>) -> FnResult<Option<Value>> + Send + Sync>;
/// `(msg, value) -> bool`: the test of a side or a step.
pub(crate) type Pred2 = Arc<dyn Fn(&Value, Option<&Value>) -> FnResult<bool> + Send + Sync>;
/// `(acc, value) -> acc`.
pub(crate) type ReduceFn =
    Arc<dyn Fn(Option<Value>, &Value) -> FnResult<Option<Value>> + Send + Sync>;
/// `(value) -> value`: one aggregate field.
pub(crate) type ExtractFn = Arc<dyn Fn(&Value) -> FnResult<Option<Value>> + Send + Sync>;
/// `(msg) -> time`.
pub(crate) type EventTimeFn = Arc<dyn Fn(&Value) -> FnResult<Option<f64>> + Send + Sync>;
/// `(a, b) -> value`: onMatch(start, then), select(left, right).
pub(crate) type PairFn = Arc<dyn Fn(&Value, &Value) -> FnResult<Option<Value>> + Send + Sync>;
/// `(msg, side?) -> value`: a pattern's or a join's onTimeout.
pub(crate) type TimeoutFn =
    Arc<dyn Fn(&Value, Option<&str>) -> FnResult<Option<Value>> + Send + Sync>;
/// `(got, key) -> value`: onComplete, a many-sided join's select.
pub(crate) type GotFn = Arc<dyn Fn(&Value, &str) -> FnResult<Option<Value>> + Send + Sync>;
/// `(got, missing, key) -> value`: a gather's or a many-sided join's onTimeout.
pub(crate) type GotMissingFn =
    Arc<dyn Fn(&Value, &Value, &str) -> FnResult<Option<Value>> + Send + Sync>;
/// `(value, row, msg) -> value`: a table join's select.
pub(crate) type SelectRowFn =
    Arc<dyn Fn(Option<&Value>, Option<&Value>, &Value) -> FnResult<Option<Value>> + Send + Sync>;
/// `(value) -> partition`.
pub(crate) type PartitionFn = Arc<dyn Fn(Option<&Value>) -> FnResult<Option<Value>> + Send + Sync>;
/// `(value, msg) -> target(s)`, async.
pub(crate) type RouteFn =
    Arc<dyn Fn(Option<Value>, Value) -> BoxFuture<FnResult<Option<Value>>> + Send + Sync>;
/// `(value, ctx)`, async.
pub(crate) type ForeachFn =
    Arc<dyn Fn(Option<Value>, Value) -> BoxFuture<FnResult<()>> + Send + Sync>;

pub(crate) fn map_fn<F, R>(f: F) -> MapFn
where
    F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
    R: IntoFeedValue,
{
    Arc::new(move |m, c| call(|| f(m, c).into_feed_value()))
}

pub(crate) fn filter_fn<F, R>(f: F) -> FilterFn
where
    F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
    R: IntoFeedBool,
{
    Arc::new(move |m, c| call(|| f(m, c).into_feed_bool()))
}

pub(crate) fn flat_map_fn<F, R>(f: F) -> FlatMapFn
where
    F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
    R: IntoFeedList,
{
    Arc::new(move |m, c| call(|| f(m, c).into_feed_list()))
}

pub(crate) fn fn1<F, R>(f: F) -> Fn1
where
    F: Fn(&Value) -> R + Send + Sync + 'static,
    R: IntoFeedValue,
{
    Arc::new(move |m| call(|| f(m).into_feed_value()))
}

pub(crate) fn pred1<F, R>(f: F) -> Pred1
where
    F: Fn(&Value) -> R + Send + Sync + 'static,
    R: IntoFeedBool,
{
    Arc::new(move |m| call(|| f(m).into_feed_bool()))
}

pub(crate) fn fn2<F, R>(f: F) -> Fn2
where
    F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
    R: IntoFeedValue,
{
    Arc::new(move |m, v| call(|| f(m, v).into_feed_value()))
}

pub(crate) fn pred2<F, R>(f: F) -> Pred2
where
    F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
    R: IntoFeedBool,
{
    Arc::new(move |m, v| call(|| f(m, v).into_feed_bool()))
}

pub(crate) fn event_time_fn<F, R>(f: F) -> EventTimeFn
where
    F: Fn(&Value) -> R + Send + Sync + 'static,
    R: IntoEventTime,
{
    Arc::new(move |m| call(|| f(m).into_event_time()))
}

// -------------------------------------------------------------- durations

/// A duration option: a number of SECONDS, or a duration string (`"30s"`,
/// `"5m"`, `"1h30m"`), as the JS options take them.
#[derive(Debug, Clone, PartialEq)]
pub enum FeedDuration {
    Seconds(f64),
    Text(String),
}

impl From<f64> for FeedDuration {
    fn from(s: f64) -> Self {
        Self::Seconds(s)
    }
}
impl From<i64> for FeedDuration {
    fn from(s: i64) -> Self {
        Self::Seconds(s as f64)
    }
}
impl From<i32> for FeedDuration {
    fn from(s: i32) -> Self {
        Self::Seconds(f64::from(s))
    }
}
impl From<&str> for FeedDuration {
    fn from(s: &str) -> Self {
        Self::Text(s.to_string())
    }
}
impl From<String> for FeedDuration {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

impl FeedDuration {
    /// The option as JSON text, as it was given (the chain hash holds it so).
    pub(crate) fn json(&self) -> String {
        match self {
            Self::Seconds(s) => js::number_text(*s),
            Self::Text(t) => js::string_json(t),
        }
    }
}

/// `durationMs(value, name, what)` (_windowCommon.js): milliseconds.
pub(crate) fn duration_ms(value: &FeedDuration, name: &str, what: &str) -> FnResult<f64> {
    match value {
        FeedDuration::Seconds(s) => {
            if !s.is_finite() || *s < 0.0 {
                return Err(format!(
                    "{what}: {name} is a number of seconds from 0, or a duration string such as '30s'"
                ));
            }
            Ok((s * 1000.0).floor())
        }
        FeedDuration::Text(t) => js::parse_duration_ms(t),
    }
}

/// `windowLatenessMs(value, what)`: seconds (floored to ms, never negative) or a duration string.
pub(crate) fn window_lateness_ms(value: &FeedDuration, what: &str) -> FnResult<f64> {
    match value {
        FeedDuration::Text(t) => js::parse_duration_ms(t),
        FeedDuration::Seconds(s) => {
            if !s.is_finite() {
                return Err(format!(
                    "{what}: allowedLateness is a number of seconds, or a duration string such as '5m'"
                ));
            }
            Ok((s * 1000.0).floor().max(0.0))
        }
    }
}

/// `onLate`: `'drop'` (default) or `'include'`.
pub(crate) fn late_policy(on_late: Option<&str>) -> FnResult<bool> {
    match on_late {
        None | Some("drop") => Ok(false),
        Some("include") => Ok(true),
        Some(other) => Err(format!(
            "onLate must be 'drop' or 'include' (got {})",
            js::string_json(other)
        )),
    }
}

// ---------------------------------------------------------------- the desc

/// An operator's structural config as `JSON.stringify` writes it: ordered
/// keys, JSON text values. The chain hash is made of these.
pub(crate) type Desc = Vec<(String, String)>;

pub(crate) fn desc_text(d: &Desc) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in d.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        js::write_string(&mut out, k);
        out.push(':');
        out.push_str(v);
    }
    out.push('}');
    out
}

// ------------------------------------------------------------- step times

/// The event-time options of a step (`stepTimeOptions`, `feedTimeOptions`).
#[derive(Clone, Default)]
pub(crate) struct StepTime {
    pub event_time: Option<EventTimeFn>,
    /// `eventTime: false`: the feed's clock, even after a step on event time.
    pub off: bool,
    pub lateness_ms: f64,
    pub include_late: bool,
    pub idle_ms: Option<f64>,
}

// ------------------------------------------------------------------ windows

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum WindowShape {
    Tumbling {
        window_ms: f64,
    },
    Sliding {
        size_ms: f64,
        slide_ms: f64,
        per_event: f64,
    },
    Session {
        gap_ms: f64,
    },
    Cron {
        window_ms: f64,
        week: bool,
    },
}

impl WindowShape {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Tumbling { .. } => "tumbling",
            Self::Sliding { .. } => "sliding",
            Self::Session { .. } => "session",
            Self::Cron { .. } => "cron",
        }
    }
}

const WEEK_EPOCH_OFFSET_MS: f64 = 4.0 * 86_400_000.0;

/// A window of a feed stream: `.window(FeedWindow::tumbling(60))`, the JS
/// `.windowTumbling({ seconds: 60 })` and its siblings, with the same options.
#[derive(Clone)]
pub struct FeedWindow {
    pub(crate) shape: Option<WindowShape>,
    raw: Vec<(String, String)>,
    every: Option<String>,
    grace_period: f64,
    idle_flush_ms: f64,
    event_time: Option<EventTimeFn>,
    event_time_off: bool,
    allowed_lateness: FeedDuration,
    on_late: Option<String>,
    idle: Option<FeedDuration>,
    pub(crate) error: Option<String>,
}

impl FeedWindow {
    fn base(
        shape: Option<WindowShape>,
        raw: Vec<(String, String)>,
        idle_flush_ms: f64,
        error: Option<String>,
    ) -> Self {
        Self {
            shape,
            raw,
            every: None,
            grace_period: 0.0,
            idle_flush_ms,
            event_time: None,
            event_time_off: false,
            allowed_lateness: FeedDuration::Seconds(0.0),
            on_late: None,
            idle: None,
            error,
        }
    }

    /// Fixed-size, non-overlapping windows of `seconds`.
    pub fn tumbling(seconds: f64) -> Self {
        let ok = seconds > 0.0 && seconds.is_finite();
        Self::base(
            ok.then_some(WindowShape::Tumbling {
                window_ms: seconds * 1000.0,
            }),
            vec![
                ("kind".into(), "\"window-tumbling\"".into()),
                ("seconds".into(), js::number_text(seconds)),
            ],
            5000.0,
            (!ok).then(|| "windowTumbling requires { seconds: <positive number> }".to_string()),
        )
    }

    /// Windows of `size` seconds that start every `slide` seconds.
    pub fn sliding(size: f64, slide: f64) -> Self {
        let mut error = None;
        // `!(x > 0)`: NaN is not positive either.
        let positive = |x: f64| x > 0.0;
        if !positive(size) {
            error = Some("windowSliding requires { size: <positive number> }".to_string());
        } else if !positive(slide) {
            error = Some("windowSliding requires { slide: <positive number> }".to_string());
        } else if size % slide != 0.0 {
            error = Some(format!(
                "windowSliding: size ({}) must be an integer multiple of slide ({}) to keep the per-event window count finite",
                js::number_text(size),
                js::number_text(slide)
            ));
        }
        Self::base(
            error.is_none().then_some(WindowShape::Sliding {
                size_ms: size * 1000.0,
                slide_ms: slide * 1000.0,
                per_event: size / slide,
            }),
            vec![
                ("kind".into(), "\"window-sliding\"".into()),
                ("size".into(), js::number_text(size)),
                ("slide".into(), js::number_text(slide)),
            ],
            5000.0,
            error,
        )
    }

    /// Per-key sessions that close after `gap` seconds without a message.
    pub fn session(gap: f64) -> Self {
        let ok = gap > 0.0 && gap.is_finite();
        Self::base(
            ok.then_some(WindowShape::Session {
                gap_ms: gap * 1000.0,
            }),
            vec![
                ("kind".into(), "\"window-session\"".into()),
                ("gap".into(), js::number_text(gap)),
            ],
            1000.0,
            (!ok).then(|| "windowSession requires { gap: <positive number> }".to_string()),
        )
    }

    /// Windows on UTC boundaries: `second`, `minute`, `hour`, `day`, `week`
    /// (weeks start on Monday).
    pub fn cron(every: &str) -> Self {
        let ms = match every {
            "second" => Some(1000.0),
            "minute" => Some(60_000.0),
            "hour" => Some(3_600_000.0),
            "day" => Some(86_400_000.0),
            "week" => Some(7.0 * 86_400_000.0),
            _ => None,
        };
        let mut w = Self::base(
            ms.map(|window_ms| WindowShape::Cron {
                window_ms,
                week: every == "week",
            }),
            vec![
                ("kind".into(), "\"window-cron\"".into()),
                ("every".into(), js::string_json(every)),
            ],
            30_000.0,
            ms.is_none().then(|| {
                format!(
                    "windowCron: every must be one of second|minute|hour|day|week (got {})",
                    js::string_json(every)
                )
            }),
        );
        w.every = Some(every.to_string());
        w
    }

    /// Accept messages for `seconds` past a window's end before it closes.
    pub fn grace_period(mut self, seconds: f64) -> Self {
        self.grace_period = seconds;
        self
    }

    /// Kept for the chain hash (a queue source's flush cadence).
    pub fn idle_flush_ms(mut self, ms: f64) -> Self {
        self.idle_flush_ms = ms;
        self
    }

    /// Event time: the step's clock is this time of each message.
    pub fn event_time<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: IntoEventTime,
    {
        self.event_time = Some(event_time_fn(f));
        self.event_time_off = false;
        self
    }

    /// `eventTime: false`: the feed's clock, even after a step on event time.
    pub fn feed_clock(mut self) -> Self {
        self.event_time = None;
        self.event_time_off = true;
        self
    }

    /// Event time only: how far out of order messages may come (seconds or `"5m"`).
    pub fn allowed_lateness(mut self, d: impl Into<FeedDuration>) -> Self {
        self.allowed_lateness = d.into();
        self
    }

    /// `"drop"` (default) or `"include"`.
    pub fn on_late(mut self, policy: &str) -> Self {
        self.on_late = Some(policy.to_string());
        self
    }

    /// The first step on event time: after this long without a message the
    /// watermark moves on with the feed's clock (seconds or `"1m"`).
    pub fn idle(mut self, d: impl Into<FeedDuration>) -> Self {
        self.idle = Some(d.into());
        self
    }

    /// Checked: the window as the engine runs it.
    pub(crate) fn build(&self) -> FnResult<WindowSpec> {
        if let Some(e) = &self.error {
            return Err(e.clone());
        }
        let shape = self.shape.expect("a checked window has a shape");
        let what = match shape {
            WindowShape::Tumbling { .. } => "windowTumbling",
            WindowShape::Sliding { .. } => "windowSliding",
            WindowShape::Session { .. } => "windowSession",
            WindowShape::Cron { .. } => "windowCron",
        };
        let include_late = late_policy(self.on_late.as_deref())?;
        let lateness_ms = window_lateness_ms(&self.allowed_lateness, what)?;
        let idle_ms = match &self.idle {
            Some(d) => Some(duration_ms(d, "idle", what)?),
            None => None,
        };
        if let Some(ms) = idle_ms {
            if ms <= 0.0 {
                return Err(format!("{what}: idle is a duration above zero"));
            }
        }
        let grace_ms = (self.grace_period * 1000.0).floor().max(0.0);
        let idle_flush = self.idle_flush_ms.floor().max(0.0);
        let mut desc: Desc = self.raw.clone();
        desc.push(("gracePeriod".into(), js::number_text(self.grace_period)));
        desc.push(("idleFlushMs".into(), js::number_text(idle_flush)));
        desc.push(("eventTime".into(), self.event_time.is_some().to_string()));
        desc.push(("allowedLateness".into(), self.allowed_lateness.json()));
        desc.push((
            "onLate".into(),
            if include_late {
                "\"include\""
            } else {
                "\"drop\""
            }
            .into(),
        ));
        if let Some(ms) = idle_ms {
            desc.push(("idle".into(), js::number_text(ms)));
        }
        Ok(WindowSpec {
            shape,
            grace_ms,
            time: StepTime {
                event_time: self.event_time.clone(),
                off: self.event_time_off,
                lateness_ms,
                include_late,
                idle_ms,
            },
            desc,
        })
    }
}

/// A window as the engine runs it.
#[derive(Clone)]
pub(crate) struct WindowSpec {
    pub shape: WindowShape,
    pub grace_ms: f64,
    pub time: StepTime,
    pub desc: Desc,
}

impl WindowSpec {
    /// The windows a time falls in: `[(start, end)]`.
    pub fn windows_of(&self, ts: f64) -> Vec<(f64, f64)> {
        match self.shape {
            WindowShape::Tumbling { window_ms } => {
                let start = (ts / window_ms).floor() * window_ms;
                vec![(start, start + window_ms)]
            }
            WindowShape::Cron { window_ms, week } => {
                let start = if week {
                    let shifted = ts - WEEK_EPOCH_OFFSET_MS;
                    (shifted / window_ms).floor() * window_ms + WEEK_EPOCH_OFFSET_MS
                } else {
                    (ts / window_ms).floor() * window_ms
                };
                vec![(start, start + window_ms)]
            }
            WindowShape::Sliding {
                size_ms,
                slide_ms,
                per_event,
            } => {
                let latest = (ts / slide_ms).floor() * slide_ms;
                let mut out = Vec::new();
                let mut i = 0.0;
                while i < per_event {
                    let start = latest - i * slide_ms;
                    if ts >= start && ts < start + size_ms {
                        out.push((start, start + size_ms));
                    }
                    i += 1.0;
                }
                out
            }
            WindowShape::Session { .. } => Vec::new(),
        }
    }
}

// ------------------------------------------------------------------ reduce

/// A window's fold: `.reduce(initial, f)` or `.aggregate(...)`.
#[derive(Clone)]
pub(crate) struct ReducerSpec {
    pub fold: ReduceFn,
    pub initial: Option<Value>,
    pub aggregate: bool,
    pub desc: Desc,
}

impl ReducerSpec {
    /// A fresh accumulator: the initial value, copied.
    pub fn initial(&self) -> Option<Value> {
        self.initial.clone()
    }
}

/// The named fields of an `.aggregate(...)`: `count`, `sum`, `min`, `max`,
/// `avg`, and any other name as a running sum. Each extractor gets the value
/// the window folds.
#[derive(Clone, Default)]
pub struct FeedAggregate {
    fields: Vec<(String, ExtractFn)>,
}

impl FeedAggregate {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn field<F, R>(mut self, name: &str, f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.fields.push((
            name.to_string(),
            Arc::new(move |v| call(|| f(v).into_feed_value())),
        ));
        self
    }

    /// `count: () => 1`.
    pub fn count(self, name: &str) -> Self {
        self.field(name, |_| Value::from(1))
    }

    pub(crate) fn build(&self) -> FnResult<ReducerSpec> {
        if self.fields.is_empty() {
            return Err("aggregate requires at least one extractor".into());
        }
        let mut initial = serde_json::Map::new();
        for (name, _) in &self.fields {
            match name.as_str() {
                "min" | "max" => {
                    initial.insert(name.clone(), Value::Null);
                }
                "avg" => {
                    initial.insert("__avg_sum".into(), Value::from(0));
                    initial.insert("__avg_count".into(), Value::from(0));
                    initial.insert("avg".into(), Value::from(0));
                }
                _ => {
                    initial.insert(name.clone(), Value::from(0));
                }
            }
        }
        let fields = self.fields.clone();
        let fold: ReduceFn = Arc::new(move |acc, msg| {
            let mut next = match acc {
                Some(Value::Object(m)) => m,
                _ => serde_json::Map::new(),
            };
            let num_of = |next: &serde_json::Map<String, Value>, k: &str| -> f64 {
                // `next[k] || 0`
                match next.get(k) {
                    Some(Value::Number(n)) => n.as_f64().unwrap_or(0.0),
                    _ => 0.0,
                }
            };
            for (name, ex) in &fields {
                let v = ex(msg)?;
                let n = js::as_num(v.as_ref());
                match name.as_str() {
                    "count" => {
                        let c = num_of(&next, "count") + n.unwrap_or(1.0);
                        next.insert("count".into(), js::num(c));
                    }
                    "sum" => {
                        let s = num_of(&next, "sum") + n.unwrap_or(0.0);
                        next.insert("sum".into(), js::num(s));
                    }
                    "min" | "max" => {
                        if let Some(x) = n {
                            let cur = js::as_num(next.get(name.as_str()));
                            let out = match cur {
                                None => x,
                                Some(c) if name == "min" => c.min(x),
                                Some(c) => c.max(x),
                            };
                            next.insert(name.clone(), js::num(out));
                        }
                    }
                    "avg" => {
                        if let Some(x) = n {
                            let s = num_of(&next, "__avg_sum") + x;
                            let c = num_of(&next, "__avg_count") + 1.0;
                            next.insert("__avg_sum".into(), js::num(s));
                            next.insert("__avg_count".into(), js::num(c));
                            next.insert("avg".into(), js::num(if c > 0.0 { s / c } else { 0.0 }));
                        }
                    }
                    _ => {
                        let s = num_of(&next, name) + n.unwrap_or(0.0);
                        next.insert(name.clone(), js::num(s));
                    }
                }
            }
            Ok(Some(Value::Object(next)))
        });
        let names: Vec<String> = self
            .fields
            .iter()
            .map(|(n, _)| js::string_json(n))
            .collect();
        Ok(ReducerSpec {
            fold,
            initial: Some(Value::Object(initial)),
            aggregate: true,
            desc: vec![
                ("kind".into(), "\"aggregate\"".into()),
                ("fields".into(), format!("[{}]", names.join(","))),
            ],
        })
    }
}

/// `.reduce(fn, initial)`.
pub(crate) fn reducer<F, R>(initial: Option<Value>, f: F) -> ReducerSpec
where
    F: Fn(Option<Value>, &Value) -> R + Send + Sync + 'static,
    R: IntoFeedValue,
{
    let has = initial.is_some();
    ReducerSpec {
        fold: Arc::new(move |acc, v| call(|| f(acc, v).into_feed_value())),
        initial,
        aggregate: false,
        desc: vec![
            ("kind".into(), "\"reduce\"".into()),
            ("hasInitial".into(), has.to_string()),
        ],
    }
}

// ---------------------------------------------- step time options builder

/// The event-time options of a pattern, a join or a gather, as given.
#[derive(Clone, Default)]
pub(crate) struct TimeOpts {
    pub event_time: Option<EventTimeFn>,
    pub off: bool,
    pub allowed_lateness: Option<FeedDuration>,
    pub on_late: Option<String>,
    pub idle: Option<FeedDuration>,
}

impl TimeOpts {
    /// `stepTimeOptions(op, opts, what)`: the options checked, and their part of the config.
    pub(crate) fn build(&self, what: &str, desc: &mut Desc) -> FnResult<StepTime> {
        let lateness_ms = match &self.allowed_lateness {
            Some(d) => duration_ms(d, "allowedLateness", what)?,
            None => 0.0,
        };
        let include_late = late_policy(self.on_late.as_deref())?;
        let idle_ms = match &self.idle {
            Some(d) => Some(duration_ms(d, "idle", what)?),
            None => None,
        };
        if let Some(ms) = idle_ms {
            if ms <= 0.0 {
                return Err(format!("{what}: idle is a duration above zero"));
            }
        }
        if self.event_time.is_some() {
            desc.push(("eventTime".into(), "true".into()));
        }
        if self.allowed_lateness.is_some() {
            desc.push(("allowedLateness".into(), js::number_text(lateness_ms)));
        }
        if self.on_late.is_some() {
            desc.push((
                "onLate".into(),
                if include_late {
                    "\"include\""
                } else {
                    "\"drop\""
                }
                .into(),
            ));
        }
        if let Some(ms) = idle_ms {
            desc.push(("idle".into(), js::number_text(ms)));
        }
        Ok(StepTime {
            event_time: self.event_time.clone(),
            off: self.off,
            lateness_ms,
            include_late,
            idle_ms,
        })
    }
}

macro_rules! time_options {
    () => {
        /// Event time: deadlines on this time of each message.
        pub fn event_time<F, R>(mut self, f: F) -> Self
        where
            F: Fn(&Value) -> R + Send + Sync + 'static,
            R: crate::streams::feed::ops::IntoEventTime,
        {
            self.time.event_time = Some(crate::streams::feed::ops::event_time_fn(f));
            self.time.off = false;
            self
        }

        /// `eventTime: false`: the feed's clock, even after a step on event time.
        pub fn feed_clock(mut self) -> Self {
            self.time.event_time = None;
            self.time.off = true;
            self
        }

        /// How far out of order messages may come (seconds, or `"5m"`).
        pub fn allowed_lateness(
            mut self,
            d: impl Into<crate::streams::feed::ops::FeedDuration>,
        ) -> Self {
            self.time.allowed_lateness = Some(d.into());
            self
        }

        /// `"drop"` (default) or `"include"`.
        pub fn on_late(mut self, policy: &str) -> Self {
            self.time.on_late = Some(policy.to_string());
            self
        }

        /// After this long without a message, the watermark moves on with the feed's clock.
        pub fn idle(mut self, d: impl Into<crate::streams::feed::ops::FeedDuration>) -> Self {
            self.time.idle = Some(d.into());
            self
        }

        /// The step's name, for live reads (as `.name()` after it).
        pub fn name(mut self, name: &str) -> Self {
            self.name = (!name.is_empty()).then(|| name.to_string());
            self
        }
    };
}
pub(crate) use time_options;
