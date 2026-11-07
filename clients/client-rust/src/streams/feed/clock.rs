//! The clock of one stateful step of a feed stream (feed/clock.js): the
//! feed's, or event time. The parts of an event clock that need other steps
//! (its watermark follows the FLOOR of the step before it) live in the
//! pipeline, which owns every step; this file holds the clock's own values.
//!
//! An event clock keeps a WATERMARK: the latest event time seen, minus
//! `allowedLateness`, as of the feed time `tLast`. A step that depends on
//! the order of its messages (patterns, joins, gathers, sessions) holds each
//! message that concerns it until the watermark reaches the message's time,
//! then takes them in event-time order. With `idle`, once the step has had no
//! message for that long on the feed's clock, its watermark moves on as the
//! feed's clock does. The held messages are entries of the step's state
//! (`b\x1f<n>`); the watermark and its anchors are the step's meta, carried
//! in the snapshot pointer.

use serde_json::{Map, Value};

use super::ops::EventTimeFn;
use super::state::{is_buffer_key, DueHeap, FeedState, BUFFER_PREFIX};
use crate::js;

pub(crate) fn buffer_key(n: u64) -> String {
    format!("{BUFFER_PREFIX}{n:012}")
}

/// `{ w, t, s, n }`: what a rollback brings back.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct ClockCp {
    pub w: f64,
    pub t: Option<f64>,
    pub s: Option<f64>,
    pub n: u64,
}

pub(crate) struct EventClock {
    pub event_time: Option<EventTimeFn>,
    /// What `event_time` gets: the envelope's msg (windows) or the feed's shape (the others).
    pub shape_value: bool,
    pub lateness_ms: f64,
    pub idle_ms: Option<f64>,
    pub include_late: bool,
    pub ordered: bool,
    /// The step this one is fed by: its watermark follows that step's floor.
    pub upstream: Option<usize>,
    pub grace_ms: f64,
    pub heap: DueHeap,
    pub w_last: f64,
    pub t_last: Option<f64>,
    pub since: Option<f64>,
    pub seq: u64,
}

impl EventClock {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_time: Option<EventTimeFn>,
        shape_value: bool,
        lateness_ms: f64,
        idle_ms: Option<f64>,
        include_late: bool,
        ordered: bool,
        upstream: Option<usize>,
        grace_ms: f64,
    ) -> Result<Self, String> {
        if !lateness_ms.is_finite() || lateness_ms < 0.0 {
            return Err(format!(
                "allowedLateness must be a duration of zero or more, got {}",
                js::number_text(lateness_ms)
            ));
        }
        if let Some(i) = idle_ms {
            if !i.is_finite() || i <= 0.0 {
                return Err(format!(
                    "idle must be a duration above zero, got {}",
                    js::number_text(i)
                ));
            }
        }
        if upstream.is_some() && idle_ms.is_some() {
            return Err("a step fed by a step on event time follows its watermark: idle goes on the first step on event time".into());
        }
        Ok(Self {
            event_time,
            shape_value,
            lateness_ms,
            idle_ms,
            include_late,
            ordered,
            upstream,
            grace_ms: if grace_ms.is_finite() && grace_ms > 0.0 {
                grace_ms
            } else {
                0.0
            },
            heap: DueHeap::default(),
            w_last: f64::NEG_INFINITY,
            t_last: None,
            since: None,
            seq: 0,
        })
    }

    /// A held message a rollback took back out of the state, or put back
    /// under the same key with another time.
    pub fn prune(&mut self, state: &FeedState) {
        while let Some(top) = self.heap.peek() {
            if let Some(v) = state.get(&top.key) {
                if js::as_num(v.get("t")) == Some(top.due) {
                    return;
                }
            }
            self.heap.pop();
        }
    }

    pub fn checkpoint(&self) -> ClockCp {
        ClockCp {
            w: self.w_last,
            t: self.t_last,
            s: self.since,
            n: self.seq,
        }
    }

    pub fn restore(&mut self, cp: &ClockCp) {
        self.w_last = cp.w;
        self.t_last = cp.t;
        self.since = cp.s;
        self.seq = cp.n;
    }

    /// The watermark and its anchors, for the snapshot pointer (JSON: no infinities).
    pub fn meta(&self) -> Value {
        let mut m = Map::new();
        m.insert("n".into(), Value::from(self.seq));
        if self.w_last.is_finite() {
            m.insert("w".into(), js::num(self.w_last));
        }
        if let Some(t) = self.t_last {
            m.insert("t".into(), js::num(t));
        }
        if let Some(s) = self.since {
            m.insert("s".into(), js::num(s));
        }
        Value::Object(m)
    }

    pub fn load_meta(&mut self, meta: Option<&Value>) {
        let empty = Value::Object(Map::new());
        let m = match meta {
            Some(v) if v.is_object() => v,
            _ => &empty,
        };
        // `watermark`: a record from before the anchors (the windows' own watermark).
        self.w_last = js::finite(m.get("w"))
            .or_else(|| js::finite(m.get("watermark")))
            .unwrap_or(f64::NEG_INFINITY);
        self.t_last = js::finite(m.get("t"));
        self.since = js::finite(m.get("s")).or(self.t_last);
        self.seq = match js::as_num(m.get("n")) {
            Some(n) if n.fract() == 0.0 && n >= 0.0 => n as u64,
            _ => 0,
        };
    }

    /// Index the held messages of the state, after a load; numbers go on
    /// after the highest one held.
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
        if !is_buffer_key(k) || !v.is_object() {
            return;
        }
        let Some(t) = js::finite(v.get("t")) else {
            return;
        };
        self.heap.push(t, k.to_string());
        // `Number(k.slice(2))`
        if let Ok(n) = k[BUFFER_PREFIX.len()..].trim().parse::<u64>() {
            if n >= self.seq {
                self.seq = n + 1;
            }
        }
    }
}

/// A step's clock.
pub(crate) enum Clock {
    /// The time the log committed each message.
    Feed,
    Event(Box<EventClock>),
}

impl Clock {
    pub fn kind(&self) -> &'static str {
        match self {
            Clock::Feed => "feed",
            Clock::Event(_) => "event",
        }
    }

    pub fn checkpoint(&self) -> Option<ClockCp> {
        match self {
            Clock::Feed => None,
            Clock::Event(c) => Some(c.checkpoint()),
        }
    }

    pub fn restore(&mut self, cp: &ClockCp) {
        if let Clock::Event(c) = self {
            c.restore(cp);
        }
    }

    pub fn meta(&self) -> Value {
        match self {
            Clock::Feed => Value::Object(Map::new()),
            Clock::Event(c) => c.meta(),
        }
    }

    pub fn load_meta(&mut self, meta: Option<&Value>) {
        if let Clock::Event(c) = self {
            c.load_meta(meta);
        }
    }

    pub fn rebuild(&mut self, state: &FeedState) {
        if let Clock::Event(c) = self {
            c.rebuild(state);
        }
    }

    pub fn rebuild_begin(&mut self) {
        if let Clock::Event(c) = self {
            c.rebuild_begin();
        }
    }

    pub fn rebuild_entry(&mut self, k: &str, v: &Value) {
        if let Clock::Event(c) = self {
            c.rebuild_entry(k, v);
        }
    }

    /// The messages held until the watermark reaches them.
    pub fn held(&self, state: &FeedState) -> usize {
        match self {
            Clock::Feed => 0,
            Clock::Event(_) => state.count("held"),
        }
    }
}
