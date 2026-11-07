//! The steps of a feed stream, run in chain order (feed/pipeline.js):
//! stateless operators, and any number of stateful steps (a window with
//! reduce/aggregate, a pattern, a join, a gather, a table join), each with
//! its own state and clock.
//!
//! A feed message enters as `{ msg, key: partition, value: data }`. A
//! stateful step that sends a result hands it on as an envelope of its own
//! (`{ msg: result, value: result, key, ctx, eventTime }`, derived). The
//! steps after the last one feed the sink.
//!
//! Closes are taken in the order they fell due on the feed's clock, the
//! later step first at one time, so a result that reaches a step finds every
//! wait of that step due by then already closed: one order every run, on
//! every replay. The runner drives the closes (it runs each result as a unit
//! of its own, with retries and the poison policy) with [`Pipeline::next_close`],
//! [`Pipeline::take`], [`Pipeline::run_emit`] and [`Pipeline::run_release`];
//! everything here is synchronous.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::clock::{buffer_key, Clock, ClockCp, EventClock};
use super::collect::{CollectEngine, CollectSpec};
use super::env::{message_of, pack, unpack, Env};
use super::ops::{call, FilterFn, FlatMapFn, Fn1, FnResult, MapFn, ReducerSpec, WindowSpec};
use super::patterns::{PatternEngine, PatternSpec};
#[cfg(test)]
use super::state::CLOSE_BATCH;
use super::state::{Classify, FeedState, REBUILD_BATCH};
use super::tables::{TableSpec, TableStep};
use super::windows::WindowEngine;
use crate::feed::WhereCondition;
use crate::js;

// ------------------------------------------------------------------ plan

/// A stateless operator of a feed chain.
#[derive(Clone)]
pub(crate) enum StatelessOp {
    Map(MapFn),
    Filter(FilterFn),
    /// A `where` after a stateful step: it tests the step's result.
    Where(WhereCondition),
    FlatMap(FlatMapFn),
    KeyBy(Fn1),
}

/// One step of a compiled chain.
#[derive(Clone)]
pub(crate) enum PlanStep {
    Ops(Vec<StatelessOp>),
    Window {
        window: WindowSpec,
        reducer: ReducerSpec,
        name: String,
    },
    Pattern {
        op: Arc<PatternSpec>,
        name: String,
    },
    Collect {
        op: Arc<CollectSpec>,
        name: String,
    },
    Table {
        op: Arc<TableSpec>,
        name: String,
    },
}

// ---------------------------------------------------------------- emits

/// What a result's value comes from: user code, run in the result's unit.
#[derive(Clone, Debug)]
pub(crate) enum Compute {
    Match {
        key: String,
        a: Value,
        b: Value,
    },
    JoinMatch {
        key: String,
        a: Value,
        b: Value,
    },
    Timeout {
        msg: Value,
        side: Option<&'static str>,
    },
    Complete {
        key: String,
        got: Value,
    },
    CollectTimeout {
        key: String,
        got: Value,
        missing: Value,
    },
}

/// One result of a step (a window closed, a match, a timeout, a gathered
/// run), or a held message in its turn (`release`).
#[derive(Clone, Debug)]
pub(crate) struct Emit {
    pub step: usize,
    pub key: String,
    pub ctx: Value,
    /// The state entry it came from (a DLQ record names what waited).
    pub state: Value,
    pub value: Option<Value>,
    pub compute: Option<Compute>,
    pub time: Option<f64>,
    pub event_time: Option<f64>,
    /// A held message: its key in the step's order.
    pub id: Option<String>,
    /// A held feed message: the message.
    pub entry: Option<Arc<Value>>,
    pub release: bool,
}

impl Emit {
    pub fn new(key: String, ctx: Value, state: Value) -> Self {
        Self {
            step: 0,
            key,
            ctx,
            state,
            value: None,
            compute: None,
            time: None,
            event_time: None,
            id: None,
            entry: None,
            release: false,
        }
    }
}

/// A held message taken out in its turn.
pub(crate) struct Release {
    pub key: String,
    pub ts: f64,
    pub env: Env,
}

/// What a clock gave: closes (`emits`), or one held message.
pub(crate) struct Took {
    pub emits: Vec<Emit>,
    pub release: Option<Release>,
}

// ---------------------------------------------------------------- engines

pub(crate) enum Engine {
    Window(WindowEngine),
    Pattern(PatternEngine),
    Collect(CollectEngine),
}

impl Engine {
    fn relevant(&self, env: &Env, t: f64) -> FnResult<bool> {
        match self {
            Engine::Window(_) => Ok(true),
            Engine::Pattern(e) => e.relevant(env, t),
            Engine::Collect(e) => e.relevant(env, t),
        }
    }
    fn process(
        &mut self,
        env: &Env,
        t: f64,
        emits: &mut Vec<Emit>,
        state: &mut FeedState,
    ) -> FnResult<()> {
        match self {
            Engine::Window(e) => e.process(env, t, emits, state),
            Engine::Pattern(e) => e.process(env, t, emits, state),
            Engine::Collect(e) => e.process(env, t, emits, state),
        }
    }
    fn advance(&mut self, t: f64, emits: &mut Vec<Emit>, limit: usize, state: &mut FeedState) {
        match self {
            Engine::Window(e) => e.advance(t, emits, limit, state),
            Engine::Pattern(e) => e.advance(t, emits, limit, state),
            Engine::Collect(e) => e.advance(t, emits, limit, state),
        }
    }
    fn next_due(&mut self, state: &FeedState) -> Option<f64> {
        match self {
            Engine::Window(e) => e.next_due(state),
            Engine::Pattern(e) => e.next_due(state),
            Engine::Collect(e) => e.next_due(state),
        }
    }
    fn rebuild(&mut self, state: &FeedState) {
        match self {
            Engine::Window(e) => e.rebuild(state),
            Engine::Pattern(e) => e.rebuild(state),
            Engine::Collect(e) => e.rebuild(state),
        }
    }
    fn rebuild_begin(&mut self) {
        match self {
            Engine::Window(e) => e.rebuild_begin(),
            Engine::Pattern(e) => e.rebuild_begin(),
            Engine::Collect(e) => e.rebuild_begin(),
        }
    }
    fn rebuild_entry(&mut self, k: &str, v: &Value) {
        match self {
            Engine::Window(e) => e.rebuild_entry(k, v),
            Engine::Pattern(e) => e.rebuild_entry(k, v),
            Engine::Collect(e) => e.rebuild_entry(k, v),
        }
    }
    fn open(&self, state: &FeedState) -> usize {
        match self {
            Engine::Window(e) => e.open(state),
            Engine::Pattern(e) => e.open(state),
            Engine::Collect(e) => e.open(state),
        }
    }
    fn compute(&self, c: &Compute) -> FnResult<Option<Value>> {
        match self {
            Engine::Window(_) => Ok(None),
            Engine::Pattern(e) => e.compute(c),
            Engine::Collect(e) => e.compute(c),
        }
    }
    fn inspect(&self, key: &str, state: &FeedState) -> Option<Value> {
        match self {
            Engine::Window(e) => e.inspect(key, state),
            Engine::Pattern(e) => e.inspect(key, state),
            Engine::Collect(e) => e.inspect(key, state),
        }
    }
    fn list(&self, state: &FeedState, n: usize) -> Vec<Value> {
        match self {
            Engine::Window(e) => e.list(state).take(n).collect(),
            Engine::Pattern(e) => e.list(state).take(n).collect(),
            Engine::Collect(e) => e.list(state).take(n).collect(),
        }
    }
}

// ------------------------------------------------------------------ steps

// One per stateful step, a handful per stream: the size of the larger variant costs nothing.
#[allow(clippy::large_enum_variant)]
pub(crate) enum Body {
    Engine {
        engine: Engine,
        clock: Clock,
        committed: Option<ClockCp>,
    },
    Table(TableStep),
}

pub(crate) struct Stateful {
    /// `window`, `pattern`, `collect` or `table`.
    pub kind: &'static str,
    pub name: String,
    pub state: FeedState,
    pub body: Body,
}

pub(crate) enum Step {
    Ops(Vec<StatelessOp>),
    Stateful(Box<Stateful>),
}

/// A point to come back to: every step's state mark and clock.
#[derive(Clone, Debug, Default)]
pub(crate) struct PipelineCp(Vec<Option<(usize, Option<ClockCp>)>>);

pub(crate) struct Pipeline {
    pub steps: Vec<Step>,
    pub late: u64,
}

fn step_label(s: &PlanStep) -> String {
    match s {
        PlanStep::Window { window, .. } => format!("the {} window", window.shape.name()),
        PlanStep::Pattern { op, .. } => format!("the {}", if op.join { "join" } else { "pattern" }),
        PlanStep::Collect { op, .. } => format!("the {}", op.kind),
        PlanStep::Table { .. } => "the table".into(),
        PlanStep::Ops(_) => "the operators".into(),
    }
}

/// The clock of a step (`clockOf`): event time when it says `eventTime`, or
/// when the stateful step before it runs on event time (unless it says
/// `eventTime: false`); else the feed's.
fn clock_of(s: &PlanStep, name: &str, before: Option<usize>, fed: bool) -> FnResult<Clock> {
    let (time, grace, ordered, shape_value) = match s {
        PlanStep::Window { window, .. } => (
            &window.time,
            window.grace_ms,
            matches!(window.shape, super::ops::WindowShape::Session { .. }),
            true,
        ),
        PlanStep::Pattern { op, .. } => (&op.time, 0.0, true, false),
        PlanStep::Collect { op, .. } => (&op.time, 0.0, true, false),
        _ => unreachable!("a clock for a step with an engine"),
    };
    let what = format!("{} {}", step_label(s), js::string_json(name));
    if let Some(f) = &time.event_time {
        if fed && time.idle_ms.is_some() {
            return Err(format!(
                "Stream.fromFeed: idle on {what}, whose messages are the results of the step before it: moving on with the feed's \
                 clock it could pass a result that step has not sent yet. Leave out its eventTime (it then takes the times of those \
                 results and follows that step's watermark), and put idle on the first step on event time"
            ));
        }
        return Ok(Clock::Event(Box::new(EventClock::new(
            Some(f.clone()),
            shape_value,
            time.lateness_ms,
            time.idle_ms,
            time.include_late,
            ordered,
            None,
            grace,
        )?)));
    }
    if let Some(b) = before {
        if !time.off {
            if time.idle_ms.is_some() {
                return Err(format!(
                    "Stream.fromFeed: {what} runs on the event time of the step before it and follows its watermark: put idle on the first step on event time"
                ));
            }
            return Ok(Clock::Event(Box::new(EventClock::new(
                None,
                shape_value,
                time.lateness_ms,
                None,
                time.include_late,
                ordered,
                Some(b),
                grace,
            )?)));
        }
    }
    if time.idle_ms.is_some() {
        return Err(format!(
            "Stream.fromFeed: idle moves a watermark on: {what} runs on the feed's clock, give it eventTime (or put it after a step on event time)"
        ));
    }
    // A window as a pattern: both default to no lateness and 'drop', so anything else was asked for.
    if time.lateness_ms > 0.0 || time.include_late {
        return Err(format!(
            "Stream.fromFeed: allowedLateness and onLate are for event time: {what} runs on the feed's clock, give it eventTime (or put it after a step on event time)"
        ));
    }
    Ok(Clock::Feed)
}

fn apply_op(op: &StatelessOp, cur: Vec<Env>) -> FnResult<Vec<Env>> {
    let mut out = Vec::with_capacity(cur.len());
    for env in cur {
        match op {
            StatelessOp::Map(f) => {
                let v = f(&env.msg, env.ctx.as_ref())?;
                out.push(env.with_value(v));
            }
            StatelessOp::Filter(f) => {
                if f(&env.msg, env.ctx.as_ref())? {
                    out.push(env);
                }
            }
            StatelessOp::Where(c) => {
                if c.holds(&env.msg) {
                    out.push(env);
                }
            }
            StatelessOp::FlatMap(f) => {
                for v in f(&env.msg, env.ctx.as_ref())? {
                    out.push(env.with_value(Some(v)));
                }
            }
            StatelessOp::KeyBy(f) => {
                let k = f(&env.msg)?;
                out.push(env.with_key(js::string_of(k.as_ref())));
            }
        }
    }
    Ok(out)
}

/// Where a load's rebuild in batches is ([`Pipeline::load_end_some`]).
pub(crate) struct RebuildCursor {
    metas: Option<Value>,
    step: usize,
    slot: usize,
    begun: bool,
}

impl RebuildCursor {
    pub fn new(metas: Option<&Value>) -> Self {
        Self {
            metas: metas.cloned(),
            step: 0,
            slot: 0,
            begun: false,
        }
    }
}

/// `load_end` in batches of [`REBUILD_BATCH`] entries, the runtime turning
/// between two (the pipeline's lock is let go in between).
pub(crate) async fn load_end_in_batches(
    pipeline: &std::sync::Mutex<Pipeline>,
    metas: Option<&Value>,
) {
    let mut c = RebuildCursor::new(metas);
    loop {
        let done = {
            let mut p = pipeline.lock().unwrap_or_else(|e| e.into_inner());
            p.load_end_some(&mut c, REBUILD_BATCH)
        };
        if done {
            return;
        }
        tokio::task::yield_now().await;
    }
}

impl Pipeline {
    pub fn new(plan: &[PlanStep]) -> FnResult<Self> {
        let mut steps = Vec::with_capacity(plan.len());
        // The event-time clock of the last step with an engine: a step after it runs on event time too.
        let mut before: Option<usize> = None;
        let mut fed = false;
        for (i, s) in plan.iter().enumerate() {
            let (kind, name, mut state, body) = match s {
                PlanStep::Ops(ops) => {
                    steps.push(Step::Ops(ops.clone()));
                    continue;
                }
                PlanStep::Window {
                    window,
                    reducer,
                    name,
                } => {
                    let engine = Engine::Window(WindowEngine::new(window.clone(), reducer.clone()));
                    (
                        "window",
                        name,
                        FeedState::new(),
                        Some((engine, Classify::Window)),
                    )
                }
                PlanStep::Pattern { op, name } => (
                    "pattern",
                    name,
                    FeedState::new(),
                    Some((
                        Engine::Pattern(PatternEngine::new(Arc::clone(op))),
                        Classify::Pattern,
                    )),
                ),
                PlanStep::Collect { op, name } => (
                    "collect",
                    name,
                    FeedState::new(),
                    Some((
                        Engine::Collect(CollectEngine::new(Arc::clone(op))),
                        Classify::Collect,
                    )),
                ),
                PlanStep::Table { op, name } => {
                    steps.push(Step::Stateful(Box::new(Stateful {
                        kind: "table",
                        name: name.clone(),
                        state: FeedState::new(),
                        body: Body::Table(TableStep::new(Arc::clone(op))),
                    })));
                    continue;
                }
            };
            let (engine, classify) = body.expect("an engine");
            state.classify_with(classify);
            let clock = clock_of(s, name, before, fed)?;
            before = matches!(clock, Clock::Event(_)).then_some(i);
            fed = true;
            steps.push(Step::Stateful(Box::new(Stateful {
                kind,
                name: name.clone(),
                state,
                body: Body::Engine {
                    engine,
                    clock,
                    committed: None,
                },
            })));
        }
        let mut p = Self { steps, late: 0 };
        p.commit_point();
        Ok(p)
    }

    fn stateful_mut(&mut self, i: usize) -> &mut Stateful {
        match &mut self.steps[i] {
            Step::Stateful(s) => s,
            Step::Ops(_) => panic!("step {i} keeps no state"),
        }
    }

    fn stateful_ref(&self, i: usize) -> &Stateful {
        match &self.steps[i] {
            Step::Stateful(s) => s,
            Step::Ops(_) => panic!("step {i} keeps no state"),
        }
    }

    fn event_clock(&self, i: usize) -> Option<&EventClock> {
        match &self.steps[i] {
            Step::Stateful(s) => match &s.body {
                Body::Engine {
                    clock: Clock::Event(c),
                    ..
                } => Some(c),
                _ => None,
            },
            _ => None,
        }
    }

    fn has_clock(&self, i: usize) -> bool {
        matches!(&self.steps[i], Step::Stateful(s) if matches!(s.body, Body::Engine { .. }))
    }

    /// The stateful steps' indexes, in chain order.
    pub fn stateful_indexes(&self) -> Vec<usize> {
        (0..self.steps.len())
            .filter(|&i| matches!(self.steps[i], Step::Stateful(_)))
            .collect()
    }

    pub fn has_state(&self) -> bool {
        self.steps.iter().any(|s| matches!(s, Step::Stateful(_)))
    }

    /// The tables' queues, every table of the chain.
    pub fn table_queues(&self) -> Vec<String> {
        let mut out = Vec::new();
        for s in &self.steps {
            if let Step::Stateful(st) = s {
                if let Body::Table(t) = &st.body {
                    for q in &t.spec.queues {
                        if !out.contains(q) {
                            out.push(q.clone());
                        }
                    }
                }
            }
        }
        out
    }

    pub fn tables_initial_all(&self) -> bool {
        self.steps.iter().any(|s| match s {
            Step::Stateful(st) => matches!(&st.body, Body::Table(t) if t.spec.initial_all),
            _ => false,
        })
    }

    fn step_index(&self, name: &str) -> Option<usize> {
        self.stateful_indexes()
            .into_iter()
            .find(|&i| self.stateful_ref(i).name == name)
    }

    // ---- the clocks ----------------------------------------------------------

    fn prune_and_peek(&mut self, i: usize) -> (Option<f64>, Option<String>, Option<f64>) {
        let st = self.stateful_mut(i);
        let Stateful { state, body, .. } = st;
        let Body::Engine { engine, clock, .. } = body else {
            return (None, None, None);
        };
        let due = engine.next_due(state);
        let (held_due, held_key) = match clock {
            Clock::Event(c) => {
                c.prune(state);
                match c.heap.peek() {
                    Some(d) => (Some(d.due), Some(d.key.clone())),
                    None => (None, None),
                }
            }
            Clock::Feed => (None, None),
        };
        (due, held_key, held_due)
    }

    /// The watermark of step `i` at feed time `t`.
    pub fn watermark_at(&mut self, i: usize, t: f64) -> f64 {
        let Some(c) = self.event_clock(i) else {
            return f64::NEG_INFINITY;
        };
        let (up, w_last, lateness, t_last, idle) =
            (c.upstream, c.w_last, c.lateness_ms, c.t_last, c.idle_ms);
        if let Some(u) = up {
            let f = self.floor_at(u, t);
            return if f.is_finite() {
                w_last.max(f - lateness)
            } else {
                w_last
            };
        }
        let (Some(t_last), Some(idle)) = (t_last, idle) else {
            return w_last;
        };
        if !w_last.is_finite() {
            return w_last;
        }
        let quiet = t - t_last - idle;
        if quiet > 0.0 {
            w_last + quiet
        } else {
            w_last
        }
    }

    /// The feed time at which step `i`'s watermark reaches `x`; `None` when
    /// only a message, or the step before, can take it there.
    fn ready_at(&mut self, i: usize, x: f64) -> Option<f64> {
        let c = self.event_clock(i)?;
        let (up, w_last, lateness, t_last, idle, since) = (
            c.upstream,
            c.w_last,
            c.lateness_ms,
            c.t_last,
            c.idle_ms,
            c.since,
        );
        let at = if t_last.is_some() && x <= w_last {
            t_last
        } else if let Some(u) = up {
            self.floor_ready_at(u, x + lateness)
        } else if let (Some(tl), Some(id)) = (t_last, idle) {
            if w_last.is_finite() {
                Some(tl + id + (x - w_last))
            } else {
                None
            }
        } else {
            None
        };
        let at = at?;
        Some(match since {
            Some(s) if s > at => s,
            _ => at,
        })
    }

    /// The oldest event time step `i` can still send a result with, at feed time `t`.
    fn floor_at(&mut self, i: usize, t: f64) -> f64 {
        let mut f = self.watermark_at(i, t);
        let grace = self.event_clock(i).map_or(0.0, |c| c.grace_ms);
        let (due, _, held_due) = self.prune_and_peek(i);
        if let Some(h) = held_due {
            f = f.min(h);
        }
        if let Some(d) = due {
            f = f.min(d - grace - 2.0);
        }
        f
    }

    /// The feed time at which step `i`'s floor reaches `x`; `None` while a
    /// held message or an open close could still send something older.
    fn floor_ready_at(&mut self, i: usize, x: f64) -> Option<f64> {
        let grace = self.event_clock(i).map_or(0.0, |c| c.grace_ms);
        let (due, _, held_due) = self.prune_and_peek(i);
        if held_due.is_some_and(|h| h < x) {
            return None;
        }
        if due.is_some_and(|d| d - grace - 2.0 < x) {
            return None;
        }
        self.ready_at(i, x)
    }

    fn needs_feed_clock_of(&self, i: usize) -> bool {
        match &self.steps[i] {
            Step::Stateful(s) => match &s.body {
                Body::Engine {
                    clock: Clock::Feed, ..
                } => true,
                Body::Engine {
                    clock: Clock::Event(c),
                    ..
                } => c.idle_ms.is_some() || c.upstream.is_some_and(|u| self.needs_feed_clock_of(u)),
                Body::Table(_) => false,
            },
            Step::Ops(_) => false,
        }
    }

    /// The next thing due in event time: a deadline (first, at a tie) or a held message.
    fn event_next(&mut self, i: usize) -> Option<(f64, Option<String>)> {
        let (due, held_key, held_due) = self.prune_and_peek(i);
        if let (Some(h), Some(k)) = (held_due, held_key) {
            if due.is_none_or(|d| h < d) {
                return Some((h, Some(k)));
            }
        }
        due.map(|d| (d, None))
    }

    /// The feed time of step `i`'s next close or release, or `None`.
    fn next_at(&mut self, i: usize) -> Option<f64> {
        if self.event_clock(i).is_some() {
            let (at, _) = self.event_next(i)?;
            return self.ready_at(i, at);
        }
        let st = self.stateful_mut(i);
        let Stateful { state, body, .. } = st;
        match body {
            Body::Engine { engine, .. } => engine.next_due(state),
            Body::Table(_) => None,
        }
    }

    /// Whether a step closes as the feed's clock moves (the runner then moves it on a quiet feed).
    pub fn needs_feed_clock(&self) -> bool {
        (0..self.steps.len()).any(|i| self.has_clock(i) && self.needs_feed_clock_of(i))
    }

    /// When the next window, deadline or held message is due on the feed's clock; `None` if none waits on it.
    pub fn next_due(&mut self) -> Option<f64> {
        let mut min: Option<f64> = None;
        for i in 0..self.steps.len() {
            if !self.has_clock(i) || !self.needs_feed_clock_of(i) {
                continue;
            }
            if let Some(d) = self.next_at(i) {
                if min.is_none_or(|m| d < m) {
                    min = Some(d);
                }
            }
        }
        min
    }

    /// The lowest event-time watermark at feed time `t`, for the metrics.
    pub fn watermark(&mut self, t: f64) -> Option<f64> {
        let mut w: Option<f64> = None;
        for i in 0..self.steps.len() {
            if self.event_clock(i).is_none() {
                continue;
            }
            let x = self.watermark_at(i, t);
            if x.is_finite() {
                w = Some(w.map_or(x, |m: f64| m.min(x)));
            }
        }
        w
    }

    // ---- running -----------------------------------------------------------

    /// A table queue's message: every table of that queue takes the row.
    pub fn apply_row(&mut self, entry: &Value) -> FnResult<()> {
        let queue = entry.get("queue").and_then(|q| q.as_str()).unwrap_or("");
        for s in &mut self.steps {
            if let Step::Stateful(st) = s {
                if let Body::Table(t) = &st.body {
                    if t.spec.queues.iter().any(|q| q == queue) {
                        t.apply_row(entry, &mut st.state)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// One feed message at feed time `t`: the envelopes that reach the end.
    pub fn entry(&mut self, entry: &Arc<Value>, t: f64) -> FnResult<Vec<Env>> {
        let position = entry.get("position").and_then(|p| p.as_u64());
        self.flow(0, vec![Env::of_entry(Arc::clone(entry))], t, position)
    }

    /// The step whose close is due next by feed time `t`, and when (the
    /// earliest; at one time the later step): `None` when nothing is due.
    pub fn next_close(&mut self, t: f64) -> Option<(usize, f64)> {
        let mut at: Option<usize> = None;
        let mut when = f64::INFINITY;
        for i in (0..self.steps.len()).rev() {
            if !self.has_clock(i) {
                continue;
            }
            if let Some(w) = self.next_at(i) {
                if w <= t && w < when {
                    at = Some(i);
                    when = w;
                }
            }
        }
        at.map(|i| (i, when))
    }

    /// Close what step `at` has due at `when` (at most `limit`), or take its
    /// next held message out.
    pub fn take(&mut self, at: usize, when: f64, limit: usize) -> Took {
        let next = if self.event_clock(at).is_some() {
            self.event_next(at)
        } else {
            None
        };
        let st = self.stateful_mut(at);
        let Stateful { state, body, .. } = st;
        let Body::Engine { engine, clock, .. } = body else {
            return Took {
                emits: Vec::new(),
                release: None,
            };
        };
        let mut emits = Vec::new();
        match clock {
            Clock::Feed => {
                if let Some(due) = engine.next_due(state) {
                    engine.advance(due, &mut emits, limit, state);
                }
            }
            Clock::Event(c) => {
                if when.is_finite() && c.since.is_none_or(|s| when > s) {
                    c.since = Some(when);
                }
                match next {
                    Some((n_at, None)) => engine.advance(n_at, &mut emits, limit, state),
                    Some((_, Some(key))) => {
                        c.heap.pop();
                        let v = state.get(&key).cloned().unwrap_or(Value::Null);
                        state.delete(&key);
                        let ts = js::as_num(v.get("t")).unwrap_or(f64::NAN);
                        let env = unpack(v.get("e").unwrap_or(&Value::Null));
                        return Took {
                            emits,
                            release: Some(Release { key, ts, env }),
                        };
                    }
                    None => {}
                }
            }
        }
        for em in &mut emits {
            em.step = at;
        }
        Took {
            emits,
            release: None,
        }
    }

    /// The unit of a held message: what the runner reports and dead-letters.
    pub fn release_emit(&self, at: usize, rel: &Release) -> Emit {
        let env = &rel.env;
        let kind = if env.derived {
            env.ctx
                .as_ref()
                .and_then(|c| c.get("kind"))
                .filter(|k| !k.is_null())
                .cloned()
                .unwrap_or_else(|| json!("result"))
        } else {
            json!("message")
        };
        let ctx = json!({ "kind": kind, "key": env.key, "partition": env.key, "eventTime": js::num(rel.ts) });
        let mut em = Emit::new(env.key.clone(), ctx, json!({ "msg": (*env.msg).clone() }));
        em.step = at;
        em.time = Some(rel.ts);
        em.id = Some(rel.key.clone());
        em.entry = (!env.derived).then(|| Arc::clone(&env.msg));
        em.value = if env.derived {
            env.value().cloned()
        } else {
            None
        };
        em.release = true;
        em
    }

    /// A result of a close, run on through the steps after it.
    pub fn run_emit(&mut self, at: usize, em: &Emit, when: f64) -> FnResult<Vec<Env>> {
        let envs = self.derive(at, std::slice::from_ref(em), None)?;
        self.flow(at + 1, envs, when, None)
    }

    /// A held message, now in its turn: through its step, and its results on.
    /// The position its outputs carry comes back with them.
    pub fn run_release(
        &mut self,
        at: usize,
        rel: &Release,
        when: f64,
    ) -> FnResult<(Vec<Env>, Option<u64>)> {
        let env = &rel.env;
        let mut emits = Vec::new();
        {
            let st = self.stateful_mut(at);
            let Stateful { state, body, .. } = st;
            if let Body::Engine { engine, .. } = body {
                engine.process(env, rel.ts, &mut emits, state)?;
            }
        }
        for em in &mut emits {
            em.step = at;
        }
        let position = env.position.or(if env.derived {
            None
        } else {
            env.msg.get("position").and_then(|p| p.as_u64())
        });
        let envs = self.derive(at, &emits, position)?;
        Ok((self.flow(at + 1, envs, when, position)?, position))
    }

    fn accept(&mut self, i: usize, env: &Env, t: f64, emits: &mut Vec<Emit>) -> FnResult<bool> {
        if self.event_clock(i).is_none() {
            let st = self.stateful_mut(i);
            let Stateful { state, body, .. } = st;
            if let Body::Engine { engine, .. } = body {
                engine.process(env, t, emits, state)?;
            }
            return Ok(false);
        }
        let (ordered, upstream) = {
            let c = self.event_clock(i).expect("event clock");
            (c.ordered, c.upstream)
        };
        if ordered {
            let rt = env.event_time.filter(|x| x.is_finite()).unwrap_or(t);
            let st = self.stateful_ref(i);
            let Body::Engine { engine, .. } = &st.body else {
                unreachable!()
            };
            if !engine.relevant(env, rt)? {
                return Ok(false);
            }
        }
        let ts = self.time_of(i, env, t)?;
        let w = match upstream {
            Some(_) => self.event_clock(i).expect("event clock").w_last,
            None => self.watermark_at(i, t),
        };
        let st = self.stateful_mut(i);
        let Stateful { state, body, .. } = st;
        let Body::Engine {
            engine,
            clock: Clock::Event(c),
            ..
        } = body
        else {
            unreachable!()
        };
        c.w_last = w.max(ts - c.lateness_ms);
        c.t_last = Some(t);
        if c.since.is_none_or(|s| t > s) {
            c.since = Some(t);
        }
        if ts < w {
            if c.include_late {
                engine.process(env, ts, emits, state)?;
            }
            return Ok(true);
        }
        if !c.ordered {
            engine.process(env, ts, emits, state)?;
            return Ok(false);
        }
        let key = buffer_key(c.seq);
        c.seq += 1;
        state.set(key.clone(), json!({ "t": js::num(ts), "e": pack(env) }));
        c.heap.push(ts, key);
        Ok(false)
    }

    fn time_of(&self, i: usize, env: &Env, t: f64) -> FnResult<f64> {
        let c = self.event_clock(i).expect("event clock");
        match &c.event_time {
            Some(f) => {
                let msg = if c.shape_value {
                    Arc::clone(&env.msg)
                } else {
                    message_of(env, t)
                };
                let ts = call(|| f(&msg)).map_err(|e| format!("eventTime extractor threw: {e}"))?;
                ts.ok_or_else(|| {
                    "eventTime gave no time for this message (a Date, epoch milliseconds or an ISO string)".to_string()
                })
            }
            None => env.event_time.filter(|x| x.is_finite()).ok_or_else(|| {
                "a result reached a step on event time without an event time".to_string()
            }),
        }
    }

    /// Run `envs` through the steps from `from`: the envelopes that reach the end.
    fn flow(
        &mut self,
        from: usize,
        envs: Vec<Env>,
        t: f64,
        position: Option<u64>,
    ) -> FnResult<Vec<Env>> {
        let mut cur = envs;
        let mut i = from;
        while i < self.steps.len() && !cur.is_empty() {
            if self.has_clock(i) {
                let mut emits = Vec::new();
                for env in &cur {
                    if self.accept(i, env, t, &mut emits)? {
                        self.late += 1;
                    }
                }
                for em in &mut emits {
                    em.step = i;
                }
                cur = self.derive(i, &emits, position)?;
            } else {
                match &self.steps[i] {
                    Step::Ops(ops) => {
                        for op in ops {
                            cur = apply_op(op, cur)?;
                        }
                    }
                    Step::Stateful(st) => {
                        if let Body::Table(table) = &st.body {
                            let mut next = Vec::new();
                            for env in &cur {
                                next.extend(table.apply(env, t, &st.state)?);
                            }
                            cur = next;
                        }
                    }
                }
            }
            i += 1;
        }
        Ok(cur)
    }

    fn derive(&self, step: usize, emits: &[Emit], position: Option<u64>) -> FnResult<Vec<Env>> {
        let mut out = Vec::with_capacity(emits.len());
        for em in emits {
            let value = match &em.compute {
                Some(c) => {
                    let st = self.stateful_ref(step);
                    let Body::Engine { engine, .. } = &st.body else {
                        unreachable!()
                    };
                    match engine.compute(c)? {
                        None | Some(Value::Null) => continue,
                        Some(v) => Some(v),
                    }
                }
                None => em.value.clone(),
            };
            // A result of `undefined` (a reduce that answered nothing): no value.
            let value_kind = if value.is_some() {
                super::env::EnvValue::Msg
            } else {
                super::env::EnvValue::Own(None)
            };
            let msg = Arc::new(value.unwrap_or(Value::Null));
            out.push(Env {
                msg,
                key: em.key.clone(),
                value: value_kind,
                ctx: Some(em.ctx.clone()),
                derived: true,
                position,
                event_time: em.event_time.filter(|x| x.is_finite()),
            });
        }
        Ok(out)
    }

    /// A synchronous close loop over [`Self::next_close`] and [`Self::take`],
    /// for tests: `unit` gets each close; `"stop"` ends it.
    #[cfg(test)]
    pub fn advance_sync(
        &mut self,
        t: f64,
        unit: &mut dyn FnMut(&mut Pipeline, &Emit) -> bool,
    ) -> bool {
        loop {
            let Some((at, when)) = self.next_close(t) else {
                return true;
            };
            let took = self.take(at, when, CLOSE_BATCH);
            if let Some(rel) = took.release {
                let em = self.release_emit(at, &rel);
                if !unit(self, &em) {
                    return false;
                }
                continue;
            }
            for em in &took.emits {
                if !unit(self, em) {
                    return false;
                }
            }
        }
    }

    // ---- state -------------------------------------------------------------

    pub fn checkpoint(&self) -> PipelineCp {
        PipelineCp(
            self.steps
                .iter()
                .map(|s| match s {
                    Step::Stateful(st) => Some((
                        st.state.mark(),
                        match &st.body {
                            Body::Engine { clock, .. } => clock.checkpoint(),
                            Body::Table(_) => None,
                        },
                    )),
                    Step::Ops(_) => None,
                })
                .collect(),
        )
    }

    pub fn restore(&mut self, cp: &PipelineCp) {
        for (s, c) in self.steps.iter_mut().zip(cp.0.iter()) {
            let (Step::Stateful(st), Some((mark, clock_cp))) = (s, c) else {
                continue;
            };
            st.state.rollback_to(*mark);
            if let (Body::Engine { clock, .. }, Some(cc)) = (&mut st.body, clock_cp) {
                clock.restore(cc);
            }
        }
    }

    /// What is in now is committed: the point a rollback comes back to.
    pub fn commit_point(&mut self) {
        for s in &mut self.steps {
            if let Step::Stateful(st) = s {
                st.state.committed();
                if let Body::Engine {
                    clock, committed, ..
                } = &mut st.body
                {
                    *committed = clock.checkpoint();
                }
            }
        }
    }

    /// Back to the last commit point.
    pub fn discard(&mut self) {
        for s in &mut self.steps {
            if let Step::Stateful(st) = s {
                st.state.discard();
                let Stateful { state, body, .. } = &mut **st;
                if let Body::Engine {
                    engine,
                    clock,
                    committed,
                } = body
                {
                    if let Some(cp) = committed {
                        clock.restore(cp);
                    }
                    engine.rebuild(state);
                    clock.rebuild(state);
                }
            }
        }
    }

    pub fn dirty(&self) -> bool {
        self.steps
            .iter()
            .any(|s| matches!(s, Step::Stateful(st) if st.state.dirty()))
    }

    pub fn changed_since_snapshot(&self) -> bool {
        self.steps
            .iter()
            .any(|s| matches!(s, Step::Stateful(st) if st.state.changed_since_snapshot()))
    }

    pub fn snapshot_taken(&mut self) {
        for s in &mut self.steps {
            if let Step::Stateful(st) = s {
                st.state.snapshot_taken();
            }
        }
    }

    /// No step holds anything.
    pub fn empty(&self) -> bool {
        self.steps
            .iter()
            .all(|s| !matches!(s, Step::Stateful(st) if st.state.size() > 0))
    }

    /// The clocks' own values, carried in the snapshot pointer.
    pub fn metas(&self) -> Value {
        Value::Array(
            self.stateful_indexes()
                .into_iter()
                .map(|i| match &self.stateful_ref(i).body {
                    Body::Engine { clock, .. } => clock.meta(),
                    Body::Table(_) => Value::Object(Map::new()),
                })
                .collect(),
        )
    }

    /// Every step from a dump (`None`: empty) and the clocks' values.
    pub fn load(&mut self, dump: Option<&Value>, metas: Option<&Value>) {
        let stored: Vec<Value> = dump
            .and_then(|d| d.get("steps"))
            .and_then(|s| s.as_array())
            .cloned()
            .unwrap_or_default();
        for (n, i) in self.stateful_indexes().into_iter().enumerate() {
            let st = self.stateful_mut(i);
            let entries: Vec<(String, Value)> = match stored.get(n) {
                Some(d) if d.get("kind").and_then(|k| k.as_str()) == Some(st.kind) => d
                    .get("entries")
                    .and_then(|e| e.as_array())
                    .map(|list| {
                        list.iter()
                            .filter_map(|kv| {
                                let k = kv.get(0)?.as_str()?.to_string();
                                Some((k, kv.get(1).cloned().unwrap_or(Value::Null)))
                            })
                            .collect()
                    })
                    .unwrap_or_default(),
                _ => Vec::new(),
            };
            st.state.load(entries);
            let Stateful { state, body, .. } = st;
            if let Body::Engine { engine, clock, .. } = body {
                clock.load_meta(metas.and_then(|m| m.get(n)));
                engine.rebuild(state);
                clock.rebuild(state);
            }
        }
        self.commit_point();
    }

    /// A load in pieces (format 2): `load_begin()`, then `load_piece` for
    /// each piece of each layer in order, then `load_end(metas)`.
    pub fn load_begin(&mut self) {
        for i in self.stateful_indexes() {
            self.stateful_mut(i).state.load_begin();
        }
    }

    /// A piece of another kind of step than the one at that place is left out.
    pub fn load_piece(&mut self, step: usize, kind: &str, set: &[Value], del: &[Value]) {
        let idx = self.stateful_indexes();
        let Some(&i) = idx.get(step) else { return };
        let st = self.stateful_mut(i);
        if st.kind != kind {
            return;
        }
        for kv in set {
            if let Some(k) = kv.get(0).and_then(|k| k.as_str()) {
                st.state
                    .load_set(k.to_string(), kv.get(1).cloned().unwrap_or(Value::Null));
            }
        }
        for k in del {
            if let Some(k) = k.as_str() {
                st.state.load_delete(k);
            }
        }
    }

    #[cfg(test)]
    pub fn load_end(&mut self, metas: Option<&Value>) {
        let mut c = RebuildCursor::new(metas);
        while !self.load_end_some(&mut c, usize::MAX) {}
    }

    /// `load_end` in batches (as the JS `await loadEnd(metas)`): call it
    /// with the same cursor until it answers true, at most `n` entries a
    /// call. Between two calls the state is whole and its indexes are not:
    /// nothing but a read may touch the pipeline.
    pub fn load_end_some(&mut self, c: &mut RebuildCursor, n: usize) -> bool {
        let idx = self.stateful_indexes();
        let mut budget = n.max(1);
        while c.step < idx.len() {
            let pos = c.step;
            let st = self.stateful_mut(idx[pos]);
            let Stateful { state, body, .. } = st;
            if !c.begun {
                state.loaded();
                if let Body::Engine { engine, clock, .. } = &mut *body {
                    clock.load_meta(c.metas.as_ref().and_then(|m| m.get(pos)));
                    engine.rebuild_begin();
                    clock.rebuild_begin();
                }
                c.begun = true;
                c.slot = 0;
            }
            if let Body::Engine { engine, clock, .. } = body {
                let (list, next) = state.map().slice_from(c.slot, budget);
                for (k, v) in &list {
                    engine.rebuild_entry(k, v);
                    clock.rebuild_entry(k, v);
                }
                budget -= list.len().min(budget);
                c.slot = next;
                if next < state.map().slot_count() {
                    return false;
                }
            }
            c.step += 1;
            c.begun = false;
            if budget == 0 && c.step < idx.len() {
                return false;
            }
        }
        self.commit_point();
        true
    }

    /// How many keys changed since the last snapshot, over every step.
    pub fn touched_since_snapshot(&self) -> usize {
        self.stateful_indexes()
            .into_iter()
            .map(|i| self.stateful_ref(i).state.touched_since_snapshot())
            .sum()
    }

    /// Every step empty (a reset): the next snapshot writes it.
    pub fn wipe(&mut self) {
        for i in self.stateful_indexes() {
            let st = self.stateful_mut(i);
            st.state.wipe();
            let Stateful { state, body, .. } = st;
            if let Body::Engine { engine, clock, .. } = body {
                clock.load_meta(None);
                engine.rebuild(state);
                clock.rebuild(state);
            }
        }
        self.commit_point();
    }

    /// Open windows, waits, runs and rows, over every step.
    pub fn open(&self) -> usize {
        self.stateful_indexes()
            .into_iter()
            .map(|i| {
                let st = self.stateful_ref(i);
                match &st.body {
                    Body::Engine { engine, .. } => engine.open(&st.state),
                    Body::Table(t) => t.open(&st.state),
                }
            })
            .sum()
    }

    pub fn state_keys(&self) -> usize {
        self.stateful_indexes()
            .into_iter()
            .map(|i| self.stateful_ref(i).state.size())
            .sum()
    }

    /// What each stateful step holds, for the position record: `open`
    /// `{ <name>: n }`, and `held` `{ <name>: n }` for event-time steps that hold messages.
    pub fn open_and_held(&self) -> (Value, Option<Value>) {
        let mut open = Map::new();
        let mut held = Map::new();
        for i in self.stateful_indexes() {
            let st = self.stateful_ref(i);
            match &st.body {
                Body::Engine { engine, clock, .. } => {
                    open.insert(st.name.clone(), json!(engine.open(&st.state)));
                    if let Clock::Event(_) = clock {
                        let h = clock.held(&st.state);
                        if h > 0 {
                            held.insert(st.name.clone(), json!(h));
                        }
                    }
                }
                Body::Table(t) => {
                    open.insert(st.name.clone(), json!(t.open(&st.state)));
                }
            }
        }
        (
            Value::Object(open),
            (!held.is_empty()).then_some(Value::Object(held)),
        )
    }

    /// The clock kind of step `i` (`feed`, `event`), `None` for a step without one.
    pub fn clock_kind(&self, i: usize) -> Option<&'static str> {
        match self.steps.get(i)? {
            Step::Stateful(st) => match &st.body {
                Body::Engine { clock, .. } => Some(clock.kind()),
                Body::Table(_) => None,
            },
            Step::Ops(_) => None,
        }
    }

    /// The names of the stateful steps.
    pub fn step_names(&self) -> Vec<String> {
        self.stateful_indexes()
            .into_iter()
            .map(|i| self.stateful_ref(i).name.clone())
            .collect()
    }

    /// The stateful steps, for a snapshot: `(kind, state)` in chain order.
    pub fn stateful_states(&self) -> Vec<(&'static str, &FeedState)> {
        self.stateful_indexes()
            .into_iter()
            .map(|i| {
                let st = self.stateful_ref(i);
                (st.kind, &st.state)
            })
            .collect()
    }

    /// Per step: what is open, the state's keys, and on event time the watermark and the messages held.
    pub fn step_metrics(&mut self, t: f64) -> Value {
        let mut out = Vec::new();
        for i in self.stateful_indexes() {
            let (name, kind, open, keys, clock_kind, held) = {
                let st = self.stateful_ref(i);
                let (open, clock_kind, held) = match &st.body {
                    Body::Engine { engine, clock, .. } => (
                        engine.open(&st.state),
                        Some(clock.kind()),
                        clock.held(&st.state),
                    ),
                    Body::Table(t) => (t.open(&st.state), None, 0),
                };
                (
                    st.name.clone(),
                    st.kind,
                    open,
                    st.state.size(),
                    clock_kind,
                    held,
                )
            };
            let mut m = Map::new();
            m.insert("name".into(), json!(name));
            m.insert("kind".into(), json!(kind));
            m.insert("open".into(), json!(open));
            m.insert("keys".into(), json!(keys));
            if let Some(ck) = clock_kind {
                m.insert("clock".into(), json!(ck));
                if ck == "event" {
                    let w = self.watermark_at(i, t);
                    m.insert(
                        "watermark".into(),
                        if w.is_finite() {
                            json!(js::iso(w))
                        } else {
                            Value::Null
                        },
                    );
                    m.insert("held".into(), json!(held));
                }
            }
            out.push(Value::Object(m));
        }
        Value::Array(out)
    }

    fn no_step(&self, name: &str) -> String {
        format!(
            "no step named {} ({})",
            js::string_json(name),
            self.step_names().join(", ")
        )
    }

    /// A live read of one step: `{ found, value? }` or `{ found: false, error }`.
    pub fn inspect(&self, name: &str, key: &str) -> Value {
        let Some(i) = self.step_index(name) else {
            return json!({ "found": false, "error": self.no_step(name) });
        };
        let st = self.stateful_ref(i);
        let v = match &st.body {
            Body::Engine { engine, .. } => engine.inspect(key, &st.state),
            Body::Table(t) => t.inspect(key, &st.state),
        };
        match v {
            Some(v) => json!({ "found": true, "value": v }),
            None => json!({ "found": false }),
        }
    }

    /// Up to `limit` entries of one step: `{ items, truncated, total }`.
    pub fn list(&self, name: &str, limit: usize) -> Value {
        let Some(i) = self.step_index(name) else {
            return json!({ "error": self.no_step(name), "items": [] });
        };
        let st = self.stateful_ref(i);
        let (all, held): (Vec<Value>, usize) = match &st.body {
            Body::Engine { engine, clock, .. } => (
                engine.list(&st.state, limit.saturating_add(1)),
                clock.held(&st.state),
            ),
            Body::Table(t) => (t.list(&st.state).take(limit.saturating_add(1)).collect(), 0),
        };
        let truncated = all.len() > limit;
        let items: Vec<Value> = all.into_iter().take(limit).collect();
        // The messages an event-time step holds in order are not the step's to list.
        json!({ "items": items, "truncated": truncated, "total": st.state.size().saturating_sub(held) })
    }
}
