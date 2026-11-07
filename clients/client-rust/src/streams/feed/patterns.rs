//! `.pattern()` and `.join()` of two sides on the tenant feed
//! (feed/patterns.js): two kinds of message for one entity, in time, on the
//! clock every queue shares, or on event time.
//!
//! State keys: `p\x1f<key>` for the `start` a pattern waits on, and
//! `j\x1fL\x1f<key>` / `j\x1fR\x1f<key>` for the side a join waits on. A
//! value is `{ key, msg, side?, due }`: the waiting message (or what `keep`
//! kept of it), and `due`, the first millisecond past its deadline.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::collect::{CollectSpec, Part};
use super::env::{message_of, snapshot_of, Env, Kept};
use super::ops::{
    call, fn2, pred1, pred2, time_options, Desc, Fn2, FnResult, GotFn, GotMissingFn, IntoFeedBool,
    IntoFeedValue, PairFn, Pred1, StepTime, TimeOpts, TimeoutFn,
};
use super::pipeline::{Compute, Emit};
use super::state::{is_buffer_key, DueHeap, FeedState};
use crate::js;

const SEP: char = '\u{1f}';

/// A pattern or a two-sided join, checked.
pub(crate) struct PatternSpec {
    pub join: bool,
    pub within_ms: f64,
    pub key: Option<Fn2>,
    pub keep: Option<Fn2>,
    pub name: Option<String>,
    pub on_timeout: Option<TimeoutFn>,
    pub restart: bool,
    pub start: Option<Pred1>,
    pub then: Option<Pred1>,
    pub on_match: Option<PairFn>,
    pub left: Option<Pred1>,
    pub right: Option<Pred1>,
    pub select: Option<PairFn>,
    pub time: StepTime,
    pub desc: Desc,
}

/// What a step keeps of a message: `{ value }` from its `keep`, else all of it.
pub(crate) fn kept_of(keep: &Option<Fn2>, msg: &Value, value: Option<&Value>) -> FnResult<Kept> {
    match keep {
        None => Ok(None),
        Some(f) => Ok(Some(call(|| f(msg, value))?)),
    }
}

/// `.pattern(Pattern::new(start, then, "15m")...)`: a `start` message, then
/// a `then` message for the same entity within `within`.
#[derive(Clone)]
pub struct Pattern {
    start: Pred1,
    then: Pred1,
    within: String,
    key: Option<Fn2>,
    keep: Option<Fn2>,
    on_match: Option<PairFn>,
    on_timeout: Option<TimeoutFn>,
    on_repeat: Option<String>,
    time: TimeOpts,
    name: Option<String>,
}

impl Pattern {
    pub fn new<S, SR, T, TR>(start: S, then: T, within: &str) -> Self
    where
        S: Fn(&Value) -> SR + Send + Sync + 'static,
        SR: IntoFeedBool,
        T: Fn(&Value) -> TR + Send + Sync + 'static,
        TR: IntoFeedBool,
    {
        Self {
            start: pred1(start),
            then: pred1(then),
            within: within.to_string(),
            key: None,
            keep: None,
            on_match: None,
            on_timeout: None,
            on_repeat: None,
            time: TimeOpts::default(),
            name: None,
        }
    }

    /// The entity: `key(msg, value)`, else `.key_by()`, else the partition.
    pub fn key<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.key = Some(fn2(f));
        self
    }

    /// What a waiting message keeps as its `data`.
    pub fn keep<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.keep = Some(fn2(f));
        self
    }

    /// `(start, then) -> result`; `None` sends nothing.
    pub fn on_match<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.on_match = Some(Arc::new(move |a, b| call(|| f(a, b).into_feed_value())));
        self
    }

    /// `(start) -> result` at the deadline.
    pub fn on_timeout<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.on_timeout = Some(Arc::new(move |m, _| call(|| f(m).into_feed_value())));
        self
    }

    /// `"keep"` (default) or `"restart"`.
    pub fn on_repeat(mut self, r: &str) -> Self {
        self.on_repeat = Some(r.to_string());
        self
    }

    #[cfg(test)]
    pub(crate) fn within_ms_for_test(mut self, within: &str) -> Self {
        self.within = within.to_string();
        self
    }

    time_options!();

    pub(crate) fn build(&self) -> FnResult<PatternSpec> {
        let mut spec = base_spec(
            "pattern",
            &self.within,
            &self.on_repeat,
            &["keep", "restart"],
        )?;
        spec.key = self.key.clone();
        spec.keep = self.keep.clone();
        spec.on_timeout = self.on_timeout.clone();
        spec.start = Some(self.start.clone());
        spec.then = Some(self.then.clone());
        spec.on_match = self.on_match.clone();
        spec.name = self.name.clone();
        finish_spec(spec, &self.time, "pattern")
    }
}

fn base_spec(
    what: &str,
    within: &str,
    on_repeat: &Option<String>,
    allowed: &[&str],
) -> FnResult<PatternSpec> {
    let within_ms = js::parse_duration_ms(within)?;
    let repeat = on_repeat.clone().unwrap_or_else(|| "keep".into());
    if !allowed.contains(&repeat.as_str()) {
        return Err(format!(
            "{what}: onRepeat is 'keep' or 'restart', got {}",
            js::string_json(&repeat)
        ));
    }
    Ok(PatternSpec {
        join: what == "join",
        within_ms,
        key: None,
        keep: None,
        name: None,
        on_timeout: None,
        restart: repeat == "restart",
        start: None,
        then: None,
        on_match: None,
        left: None,
        right: None,
        select: None,
        time: StepTime::default(),
        desc: vec![
            ("kind".into(), js::string_json(what)),
            ("within".into(), js::number_text(within_ms)),
            ("onRepeat".into(), js::string_json(&repeat)),
        ],
    })
}

fn finish_spec(mut spec: PatternSpec, time: &TimeOpts, what: &str) -> FnResult<PatternSpec> {
    spec.desc
        .push(("timeout".into(), spec.on_timeout.is_some().to_string()));
    spec.time = time.build(what, &mut spec.desc)?;
    Ok(spec)
}

/// `.join(...)`: two sides (`Join::new(left, right, within)`, a pattern
/// engine), or any number of named sides (`Join::sides(within)`, collected
/// like a gather).
#[derive(Clone)]
pub struct Join {
    left: Option<Pred1>,
    right: Option<Pred1>,
    sides: Vec<(String, Part)>,
    within: String,
    key: Option<Fn2>,
    keep: Option<Fn2>,
    select: Option<PairFn>,
    select_got: Option<GotFn>,
    on_timeout: Option<TimeoutFn>,
    on_timeout_got: Option<GotMissingFn>,
    on_repeat: Option<String>,
    time: TimeOpts,
    name: Option<String>,
    many: bool,
}

impl Join {
    /// A `left` and a `right` message for one entity, in either order,
    /// within `within` of each other.
    pub fn new<L, LR, R, RR>(left: L, right: R, within: &str) -> Self
    where
        L: Fn(&Value) -> LR + Send + Sync + 'static,
        LR: IntoFeedBool,
        R: Fn(&Value) -> RR + Send + Sync + 'static,
        RR: IntoFeedBool,
    {
        Self {
            left: Some(pred1(left)),
            right: Some(pred1(right)),
            sides: Vec::new(),
            within: within.to_string(),
            key: None,
            keep: None,
            select: None,
            select_got: None,
            on_timeout: None,
            on_timeout_got: None,
            on_repeat: None,
            time: TimeOpts::default(),
            name: None,
            many: false,
        }
    }

    /// Named sides, each a test (`side`) or a queue (`side_queue`); a
    /// message is the side of the first test it passes.
    pub fn sides(within: &str) -> Self {
        Self {
            left: None,
            right: None,
            sides: Vec::new(),
            within: within.to_string(),
            key: None,
            keep: None,
            select: None,
            select_got: None,
            on_timeout: None,
            on_timeout_got: None,
            on_repeat: None,
            time: TimeOpts::default(),
            name: None,
            many: true,
        }
    }

    pub fn side<F, R>(mut self, name: &str, test: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedBool,
    {
        self.sides.push((name.to_string(), Part::Test(pred2(test))));
        self
    }

    /// A side that is every message of `queue`.
    pub fn side_queue(mut self, name: &str, queue: &str) -> Self {
        self.sides
            .push((name.to_string(), Part::Queue(queue.to_string())));
        self
    }

    pub fn key<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.key = Some(fn2(f));
        self
    }

    pub fn keep<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.keep = Some(fn2(f));
        self
    }

    /// Two sides: `(left, right) -> result`.
    pub fn select<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.select = Some(Arc::new(move |a, b| call(|| f(a, b).into_feed_value())));
        self
    }

    /// Named sides: `(got, key) -> result`.
    pub fn select_all<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &str) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.select_got = Some(Arc::new(move |g, k| call(|| f(g, k).into_feed_value())));
        self
    }

    /// Two sides: `(msg, side) -> result`, `side` `"left"` or `"right"`.
    pub fn on_timeout<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &str) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.on_timeout = Some(Arc::new(move |m, side| {
            call(|| f(m, side.unwrap_or("")).into_feed_value())
        }));
        self
    }

    /// Named sides: `(got, missing, key) -> result`.
    pub fn on_timeout_all<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &Value, &str) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.on_timeout_got = Some(Arc::new(move |g, m, k| {
            call(|| f(g, m, k).into_feed_value())
        }));
        self
    }

    /// Two sides: `"keep"` or `"restart"`; named sides: `"keep"` or `"latest"`.
    pub fn on_repeat(mut self, r: &str) -> Self {
        self.on_repeat = Some(r.to_string());
        self
    }

    time_options!();

    pub(crate) fn is_many(&self) -> bool {
        self.many
    }

    pub(crate) fn build_two(&self) -> FnResult<PatternSpec> {
        let mut spec = base_spec("join", &self.within, &self.on_repeat, &["keep", "restart"])?;
        spec.key = self.key.clone();
        spec.keep = self.keep.clone();
        spec.on_timeout = self.on_timeout.clone();
        spec.left = self.left.clone();
        spec.right = self.right.clone();
        spec.select = self.select.clone();
        spec.name = self.name.clone();
        finish_spec(spec, &self.time, "join")
    }

    pub(crate) fn build_many(&self) -> FnResult<CollectSpec> {
        CollectSpec::build(
            "join",
            &self.within,
            Some(&self.sides),
            None,
            self.key.clone(),
            self.keep.clone(),
            self.select_got.clone(),
            self.on_timeout_got.clone(),
            &self.on_repeat,
            &self.time,
            &self.name,
        )
    }
}

pub(crate) struct PatternEngine {
    pub spec: Arc<PatternSpec>,
    heap: DueHeap,
}

impl PatternEngine {
    pub fn new(spec: Arc<PatternSpec>) -> Self {
        Self {
            spec,
            heap: DueHeap::default(),
        }
    }

    /// Whether a message concerns the step (a start or then, a side).
    pub fn relevant(&self, env: &Env, t: f64) -> FnResult<bool> {
        let msg = message_of(env, t);
        let s = &self.spec;
        if !s.join {
            let then = s.then.as_ref().expect("pattern then");
            let start = s.start.as_ref().expect("pattern start");
            return Ok(then(&msg)? || start(&msg)?);
        }
        let left = s.left.as_ref().expect("join left");
        let right = s.right.as_ref().expect("join right");
        Ok(left(&msg)? || right(&msg)?)
    }

    pub fn rebuild(&mut self, state: &FeedState) {
        self.rebuild_begin();
        for (k, v) in state.entries() {
            self.rebuild_entry(k, v);
        }
    }

    /// A rebuild in batches: `rebuild_begin`, then each entry of the state.
    pub fn rebuild_begin(&mut self) {
        self.heap.clear();
    }

    pub fn rebuild_entry(&mut self, k: &str, v: &Value) {
        if !is_buffer_key(k) && v.is_object() {
            if let Some(due) = js::finite(v.get("due")) {
                self.heap.push(due, k.to_string());
            }
        }
    }

    fn prune(&mut self, state: &FeedState) {
        while let Some(top) = self.heap.peek() {
            if let Some(v) = state.get(&top.key) {
                if js::as_num(v.get("due")) == Some(top.due) {
                    return;
                }
            }
            self.heap.pop();
        }
    }

    pub fn next_due(&mut self, state: &FeedState) -> Option<f64> {
        self.prune(state);
        self.heap.peek().map(|d| d.due)
    }

    pub fn open(&self, state: &FeedState) -> usize {
        state.count("open")
    }

    /// Pass every deadline at or before `t`: one timeout emit each when `onTimeout` is set.
    pub fn advance(&mut self, t: f64, emits: &mut Vec<Emit>, limit: usize, state: &mut FeedState) {
        for _ in 0..limit {
            self.prune(state);
            match self.heap.peek() {
                Some(d) if d.due <= t => {}
                _ => return,
            }
            let sk = self.heap.pop().expect("peeked").key;
            let v = state.get(&sk).cloned().unwrap_or(Value::Null);
            state.delete(&sk);
            if self.spec.on_timeout.is_none() {
                continue;
            }
            let side = match v.get("side").and_then(|s| s.as_str()) {
                Some("R") => Some("right"),
                Some("L") => Some("left"),
                _ => None,
            };
            let due = js::as_num(v.get("due")).unwrap_or(f64::NAN);
            let key = js::string_of(v.get("key"));
            let mut ctx = Map::new();
            ctx.insert("kind".into(), json!("timeout"));
            ctx.insert("key".into(), v.get("key").cloned().unwrap_or(Value::Null));
            ctx.insert(
                "partition".into(),
                v.get("key").cloned().unwrap_or(Value::Null),
            );
            if let Some(sd) = side {
                ctx.insert("side".into(), json!(sd));
            }
            ctx.insert("deadline".into(), js::num(due - 1.0));
            if let Some(m) = v.get("msg") {
                ctx.insert("waitedFor".into(), m.clone());
            }
            let msg = v.get("msg").cloned().unwrap_or(Value::Null);
            let mut em = Emit::new(key, Value::Object(ctx), v);
            em.time = Some(due);
            em.event_time = Some(due - 1.0);
            em.compute = Some(Compute::Timeout { msg, side });
            emits.push(em);
        }
    }

    /// One envelope at time `t`; a match is appended to `emits`.
    pub fn process(
        &mut self,
        env: &Env,
        t: f64,
        emits: &mut Vec<Emit>,
        state: &mut FeedState,
    ) -> FnResult<()> {
        let msg = message_of(env, t);
        let spec = Arc::clone(&self.spec);
        // The entity: key(msg, value), so a .flat_map() before can key each
        // item of a message on its own id. Asked only of a message that
        // starts or ends a wait: the others need not have the field.
        let key_of = || -> FnResult<String> {
            match &spec.key {
                Some(f) => Ok(js::string_of(f(&msg, env.value())?.as_ref())),
                None => Ok(env.key.clone()),
            }
        };
        if !spec.join {
            let is_then = (spec.then.as_ref().expect("then"))(&msg)?;
            let is_start = (spec.start.as_ref().expect("start"))(&msg)?;
            if !is_then && !is_start {
                return Ok(());
            }
            let key = key_of()?;
            let sk = format!("p{SEP}{key}");
            let waiting = state.get(&sk).cloned();
            match waiting {
                Some(w) if is_then => {
                    state.delete(&sk);
                    let snap = snapshot_of(&msg, &kept_of(&spec.keep, &msg, env.value())?);
                    let mut em = Emit::new(key.clone(), match_ctx(&key, &w), w.clone());
                    em.event_time = Some(t);
                    em.compute = Some(Compute::Match {
                        key: key.clone(),
                        a: w.get("msg").cloned().unwrap_or(Value::Null),
                        b: snap,
                    });
                    emits.push(em);
                    if is_start {
                        self.wait(&sk, &key, &msg, env.value(), t, None, state)?;
                    }
                }
                waiting => {
                    if is_start && (waiting.is_none() || spec.restart) {
                        self.wait(&sk, &key, &msg, env.value(), t, None, state)?;
                    }
                }
            }
            return Ok(());
        }
        // join: a message that is both left and right counts as left.
        let side = if (spec.left.as_ref().expect("left"))(&msg)? {
            "L"
        } else if (spec.right.as_ref().expect("right"))(&msg)? {
            "R"
        } else {
            return Ok(());
        };
        let key = key_of()?;
        let other = if side == "L" { "R" } else { "L" };
        let other_key = format!("j{SEP}{other}{SEP}{key}");
        if let Some(w) = state.get(&other_key).cloned() {
            state.delete(&other_key);
            let snap = snapshot_of(&msg, &kept_of(&spec.keep, &msg, env.value())?);
            let waited = w.get("msg").cloned().unwrap_or(Value::Null);
            let (left, right) = if side == "L" {
                (snap, waited)
            } else {
                (waited, snap)
            };
            let mut em = Emit::new(key.clone(), match_ctx(&key, &w), w.clone());
            em.event_time = Some(t);
            em.compute = Some(Compute::JoinMatch {
                key: key.clone(),
                a: left,
                b: right,
            });
            emits.push(em);
            return Ok(());
        }
        let my_key = format!("j{SEP}{side}{SEP}{key}");
        if state.get(&my_key).is_none() || spec.restart {
            self.wait(&my_key, &key, &msg, env.value(), t, Some(side), state)?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn wait(
        &mut self,
        sk: &str,
        key: &str,
        msg: &Value,
        value: Option<&Value>,
        t: f64,
        side: Option<&str>,
        state: &mut FeedState,
    ) -> FnResult<()> {
        let due = t + self.spec.within_ms + 1.0;
        let snap = snapshot_of(msg, &kept_of(&self.spec.keep, msg, value)?);
        let mut v = Map::new();
        v.insert("key".into(), Value::String(key.to_string()));
        if let Some(s) = side {
            v.insert("side".into(), Value::String(s.to_string()));
        }
        v.insert("msg".into(), snap);
        v.insert("due".into(), js::num(due));
        state.set(sk.to_string(), Value::Object(v));
        self.heap.push(due, sk.to_string());
        Ok(())
    }

    /// The value of a match or a timeout (user code: run in its unit).
    pub fn compute(&self, c: &Compute) -> FnResult<Option<Value>> {
        let s = &self.spec;
        match c {
            Compute::Match { key, a, b } => match &s.on_match {
                Some(f) => call(|| f(a, b)),
                None => Ok(Some(json!({ "key": key, "start": a, "then": b }))),
            },
            Compute::JoinMatch { key, a, b } => match &s.select {
                Some(f) => call(|| f(a, b)),
                None => Ok(Some(json!({ "key": key, "left": a, "right": b }))),
            },
            Compute::Timeout { msg, side } => match &s.on_timeout {
                Some(f) => call(|| f(msg, *side)),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// What waits for `key`, for a live read: `{ key, waiting: [...] }`, or `None`.
    pub fn inspect(&self, key: &str, state: &FeedState) -> Option<Value> {
        let keys = if self.spec.join {
            vec![format!("j{SEP}L{SEP}{key}"), format!("j{SEP}R{SEP}{key}")]
        } else {
            vec![format!("p{SEP}{key}")]
        };
        let out: Vec<Value> = keys.iter().filter_map(|k| state.get(k)).map(view).collect();
        (!out.is_empty()).then(|| json!({ "key": key, "waiting": out }))
    }

    /// Every waiting entity, for a live read.
    pub fn list<'a>(&'a self, state: &'a FeedState) -> impl Iterator<Item = Value> + 'a {
        state
            .entries()
            .filter(|(k, _)| !is_buffer_key(k))
            .map(|(_, v)| {
                let mut m = Map::new();
                if let Some(k) = v.get("key") {
                    m.insert("key".into(), k.clone());
                }
                m.insert("waiting".into(), json!([view(v)]));
                Value::Object(m)
            })
    }
}

fn match_ctx(key: &str, waiting: &Value) -> Value {
    let due = js::as_num(waiting.get("due")).unwrap_or(f64::NAN);
    json!({ "kind": "match", "key": key, "partition": key, "deadline": js::num(due - 1.0) })
}

fn view(v: &Value) -> Value {
    let mut w = Map::new();
    if let Some(m) = v.get("msg") {
        w.insert("msg".into(), m.clone());
    }
    let due = js::as_num(v.get("due")).unwrap_or(f64::NAN);
    w.insert("deadline".into(), Value::String(js::iso(due - 1.0)));
    if js::truthy(v.get("side")) {
        let side = if v.get("side").and_then(|s| s.as_str()) == Some("L") {
            "left"
        } else {
            "right"
        };
        w.insert("side".into(), Value::String(side.into()));
    }
    Value::Object(w)
}
