//! Windows on the tenant feed (feed/windows.js): tumbling, sliding, cron and
//! session windows with reduce/aggregate, per key, kept in the step's
//! [`FeedState`].
//!
//! State keys: `w\x1f<start>\x1f<key>` for a window of the fixed kinds,
//! `s\x1f<key>` for a key's open session. A value is
//! `{ key, start, end, due, acc }` (a session also keeps `last`), where `due`
//! is when it closes.

use serde_json::{json, Map, Value};

use super::env::Env;
use super::ops::{call, FnResult, ReducerSpec, WindowShape, WindowSpec};
use super::pipeline::Emit;
use super::state::{DueHeap, FeedState};
use crate::js;

const SEP: char = '\u{1f}';

/// An aggregate's result, without the running sums it keeps for `avg`.
fn final_acc(acc: Option<&Value>, aggregate: bool) -> Option<Value> {
    match acc {
        Some(Value::Object(m)) if aggregate => {
            let mut out = Map::new();
            for (k, v) in m {
                if !k.starts_with("__avg_") {
                    out.insert(k.clone(), v.clone());
                }
            }
            Some(Value::Object(out))
        }
        other => other.cloned(),
    }
}

pub(crate) struct WindowEngine {
    pub window: WindowSpec,
    pub reducer: ReducerSpec,
    heap: DueHeap,
}

fn num_of(v: &Value, k: &str) -> f64 {
    js::as_num(v.get(k)).unwrap_or(f64::NAN)
}

impl WindowEngine {
    pub fn new(window: WindowSpec, reducer: ReducerSpec) -> Self {
        Self {
            window,
            reducer,
            heap: DueHeap::default(),
        }
    }

    pub fn kind(&self) -> &'static str {
        self.window.shape.name()
    }

    /// Index what the state holds, after a load.
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
        if (k.starts_with("w\u{1f}") || k.starts_with("s\u{1f}")) && v.is_object() {
            if let Some(due) = js::finite(v.get("due")) {
                self.heap.push(due, k.to_string());
            }
        }
    }

    fn prune(&mut self, state: &FeedState) {
        // A heap item whose window was closed, or moved, since it was pushed.
        while let Some(top) = self.heap.peek() {
            if let Some(v) = state.get(&top.key) {
                if js::as_num(v.get("due")) == Some(top.due) {
                    return;
                }
            }
            self.heap.pop();
        }
    }

    /// When the next window closes; `None` if none is open.
    pub fn next_due(&mut self, state: &FeedState) -> Option<f64> {
        self.prune(state);
        self.heap.peek().map(|d| d.due)
    }

    pub fn open(&self, state: &FeedState) -> usize {
        state.count("open")
    }

    /// Close every window due at or before `t` (at most `limit`): one emit each.
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
            // The result's time is when the window closed, not when this
            // process saw it close: one time on every run and every replay.
            let mut em = self.emit_of(&v);
            em.time = js::finite(v.get("due"));
            emits.push(em);
        }
    }

    /// Fold one envelope in at time `t` (the step's clock); a session that
    /// the gap closed is appended to `emits`.
    pub fn process(
        &mut self,
        env: &Env,
        t: f64,
        emits: &mut Vec<Emit>,
        state: &mut FeedState,
    ) -> FnResult<()> {
        let key = env.key.clone();
        let value = env.value_or_msg();
        if let WindowShape::Session { gap_ms } = self.window.shape {
            return self.session(&key, t, value, gap_ms, emits, state);
        }
        for (start, end) in self.window.windows_of(t) {
            self.fold(&key, start, end, value, state)?;
        }
        Ok(())
    }

    fn fold(
        &mut self,
        key: &str,
        start: f64,
        end: f64,
        value: &Value,
        state: &mut FeedState,
    ) -> FnResult<()> {
        let sk = format!("w{SEP}{}{SEP}{key}", js::number_text(start));
        let cur = state.get(&sk);
        let acc_in = match cur {
            Some(c) => c.get("acc").cloned(),
            None => self.reducer.initial(),
        };
        let had = cur.is_some();
        let fold = self.reducer.fold.clone();
        let acc = call(|| fold(acc_in, value))?;
        let due = end + self.window.grace_ms;
        let mut v = Map::new();
        v.insert("key".into(), Value::String(key.to_string()));
        v.insert("start".into(), js::num(start));
        v.insert("end".into(), js::num(end));
        v.insert("due".into(), js::num(due));
        if let Some(a) = acc {
            v.insert("acc".into(), a);
        }
        state.set(sk.clone(), Value::Object(v));
        if !had {
            self.heap.push(due, sk);
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn session(
        &mut self,
        key: &str,
        ts: f64,
        value: &Value,
        gap: f64,
        emits: &mut Vec<Emit>,
        state: &mut FeedState,
    ) -> FnResult<()> {
        let sk = format!("s{SEP}{key}");
        let mut cur = state.get(&sk).cloned();
        if let Some(c) = &cur {
            if ts > num_of(c, "last") + gap {
                // Past the gap: that session is over, and this message starts the next.
                state.delete(&sk);
                emits.push(self.emit_of(c));
                cur = None;
            }
        }
        let acc_in = match &cur {
            Some(c) => c.get("acc").cloned(),
            None => self.reducer.initial(),
        };
        let fold = self.reducer.fold.clone();
        let acc = call(|| fold(acc_in, value))?;
        let start = cur
            .as_ref()
            .map(|c| num_of(c, "start").min(ts))
            .unwrap_or(ts);
        let last = cur
            .as_ref()
            .map(|c| num_of(c, "last").max(ts))
            .unwrap_or(ts);
        // A message exactly `gap` after the last one is still in the session
        // (as a pattern's `within` is in time): it closes the millisecond after.
        let due = last + gap + 1.0 + self.window.grace_ms;
        let mut v = Map::new();
        v.insert("key".into(), Value::String(key.to_string()));
        v.insert("start".into(), js::num(start));
        v.insert("last".into(), js::num(last));
        v.insert("end".into(), js::num(last + gap));
        v.insert("due".into(), js::num(due));
        if let Some(a) = acc {
            v.insert("acc".into(), a);
        }
        let cur_due = cur.as_ref().and_then(|c| js::as_num(c.get("due")));
        state.set(sk.clone(), Value::Object(v));
        if cur.is_none() || cur_due != Some(due) {
            self.heap.push(due, sk);
        }
        Ok(())
    }

    /// The open windows of `key`, for a live read: `{ key, windows: [...] }`, or `None`.
    pub fn inspect(&self, key: &str, state: &FeedState) -> Option<Value> {
        let mut windows: Vec<(f64, Value)> = Vec::new();
        for (_, v) in state.entries() {
            if v.get("key").and_then(|k| k.as_str()) == Some(key) {
                windows.push((num_of(v, "start"), self.view(v)));
            }
        }
        if windows.is_empty() {
            return None;
        }
        windows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        Some(json!({ "key": key, "windows": windows.into_iter().map(|w| w.1).collect::<Vec<_>>() }))
    }

    /// Every open window, for a live read.
    pub fn list<'a>(&'a self, state: &'a FeedState) -> impl Iterator<Item = Value> + 'a {
        state.entries().filter_map(move |(_, v)| {
            let key = v.get("key")?;
            Some(json!({ "key": key, "windows": [self.view(v)] }))
        })
    }

    fn view(&self, v: &Value) -> Value {
        let mut m = Map::new();
        m.insert("start".into(), Value::String(js::iso(num_of(v, "start"))));
        m.insert("end".into(), Value::String(js::iso(num_of(v, "end"))));
        m.insert("closesAt".into(), Value::String(js::iso(num_of(v, "due"))));
        if let Some(a) = final_acc(v.get("acc"), self.reducer.aggregate) {
            m.insert("value".into(), a);
        }
        Value::Object(m)
    }

    fn emit_of(&self, v: &Value) -> Emit {
        let key = js::string_of(v.get("key"));
        let start = num_of(v, "start");
        let end = num_of(v, "end");
        let mut ctx = Map::new();
        ctx.insert("kind".into(), Value::String(self.kind().into()));
        ctx.insert("key".into(), v.get("key").cloned().unwrap_or(Value::Null));
        ctx.insert(
            "partition".into(),
            v.get("key").cloned().unwrap_or(Value::Null),
        );
        ctx.insert("windowStart".into(), js::num(start));
        ctx.insert("windowEnd".into(), js::num(end));
        ctx.insert("windowKey".into(), Value::String(js::iso(start)));
        let mut em = Emit::new(key, Value::Object(ctx), v.clone());
        em.value = final_acc(v.get("acc"), self.reducer.aggregate);
        // On to a later step on event time: the last millisecond the window covers.
        em.event_time = Some(end - 1.0);
        em
    }
}
