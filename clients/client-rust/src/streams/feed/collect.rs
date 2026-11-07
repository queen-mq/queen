//! `.join()` of many sides and `.gather()` on the tenant feed
//! (feed/collect.js): collect the parts of one entity until every part is
//! in, or its deadline passes.
//!
//! State key `c\x1f<key>`; a value is `{ key, first, due, parts | list+ids,
//! expected }`, or `{ key, due, done: true }` for a finished gather.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::env::{message_of, snapshot_of, Env};
use super::ops::{
    call, fn2, pred2, time_options, Desc, Fn2, FnResult, GotFn, GotMissingFn, IntoFeedBool,
    IntoFeedValue, Pred2, StepTime, TimeOpts,
};
use super::patterns::kept_of;
use super::pipeline::{Compute, Emit};
use super::state::{is_buffer_key, DueHeap, FeedState};
use crate::js;

const SEP: char = '\u{1f}';

/// A part's test: a function of `(msg, value)`, or a queue name (the message's queue is it).
#[derive(Clone)]
pub(crate) enum Part {
    Test(Pred2),
    Queue(String),
}

impl Part {
    fn holds(&self, msg: &Value, value: Option<&Value>) -> FnResult<bool> {
        match self {
            Part::Test(f) => f(msg, value),
            Part::Queue(q) => Ok(msg.get("queue").and_then(|v| v.as_str()) == Some(q.as_str())),
        }
    }
}

/// A gather or a many-sided join, checked.
pub(crate) struct CollectSpec {
    pub kind: &'static str,
    pub within_ms: f64,
    pub key: Option<Fn2>,
    pub keep: Option<Fn2>,
    pub on_timeout: Option<GotMissingFn>,
    pub latest: bool,
    pub name: Option<String>,
    pub parts: Option<Vec<(String, Part)>>,
    pub count: Option<Fn2>,
    pub on_complete: Option<GotFn>,
    pub remember: bool,
    pub time: StepTime,
    pub desc: Desc,
}

impl CollectSpec {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn build(
        what: &'static str,
        within: &str,
        parts: Option<&Vec<(String, Part)>>,
        count: Option<Fn2>,
        key: Option<Fn2>,
        keep: Option<Fn2>,
        on_complete: Option<GotFn>,
        on_timeout: Option<GotMissingFn>,
        on_repeat: &Option<String>,
        time: &TimeOpts,
        name: &Option<String>,
    ) -> FnResult<CollectSpec> {
        let within_ms = js::parse_duration_ms(within)?;
        let repeat = on_repeat.clone().unwrap_or_else(|| "keep".into());
        if repeat != "keep" && repeat != "latest" {
            return Err(format!(
                "{what}: onRepeat is 'keep' (the first message of a part stays) or 'latest', got {}",
                js::string_json(&repeat)
            ));
        }
        let mut checked: Option<Vec<(String, Part)>> = None;
        if let Some(list) = parts {
            let noun = if what == "join" { "side" } else { "step" };
            let mut out: Vec<(String, Part)> = Vec::new();
            for (n, p) in list {
                if n.is_empty() {
                    return Err(format!("{what}: every {noun} has a name"));
                }
                if out.iter().any(|(m, _)| m == n) {
                    return Err(format!(
                        "{what}: {noun} {} is named twice",
                        js::string_json(n)
                    ));
                }
                out.push((n.clone(), p.clone()));
            }
            checked = Some(out);
        }
        if what == "join" {
            if checked.as_ref().map_or(0, |p| p.len()) < 2 {
                return Err("join: sides names at least two".into());
            }
        } else {
            if checked.is_some() == count.is_some() {
                return Err(
                    "gather: give steps (the parts by name) or count (how many parts a run has), one of the two"
                        .into(),
                );
            }
            if checked.as_ref().is_some_and(|p| p.is_empty()) {
                return Err("gather: steps names at least one".into());
            }
        }
        let parts_json = match &checked {
            Some(p) => format!(
                "[{}]",
                p.iter()
                    .map(|(n, _)| js::string_json(n))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            None => "null".into(),
        };
        let mut desc: Desc = vec![
            ("kind".into(), js::string_json(what)),
            ("within".into(), js::number_text(within_ms)),
            ("parts".into(), parts_json),
            ("count".into(), count.is_some().to_string()),
            ("onRepeat".into(), js::string_json(&repeat)),
            ("timeout".into(), on_timeout.is_some().to_string()),
        ];
        let time = time.build(what, &mut desc)?;
        Ok(CollectSpec {
            kind: what,
            within_ms,
            key,
            keep,
            on_timeout,
            latest: repeat == "latest",
            name: name.clone(),
            parts: checked,
            count,
            on_complete,
            remember: what == "gather",
            time,
            desc,
        })
    }
}

/// `.gather(...)`: the parts of one run, collected until all are in or the
/// deadline passes. Parts by name (`Gather::steps(within).step_queue(..)`)
/// or by count (`Gather::count(within, |m, v| n)`).
#[derive(Clone)]
pub struct Gather {
    steps: Option<Vec<(String, Part)>>,
    count: Option<Fn2>,
    within: String,
    key: Option<Fn2>,
    keep: Option<Fn2>,
    on_complete: Option<GotFn>,
    on_timeout: Option<GotMissingFn>,
    on_repeat: Option<String>,
    time: TimeOpts,
    name: Option<String>,
}

impl Gather {
    /// Parts by name: add them with `step` / `step_queue`.
    pub fn steps(within: &str) -> Self {
        Self {
            steps: Some(Vec::new()),
            count: None,
            within: within.to_string(),
            key: None,
            keep: None,
            on_complete: None,
            on_timeout: None,
            on_repeat: None,
            time: TimeOpts::default(),
            name: None,
        }
    }

    /// Distinct messages (by queue, partition and transaction id) until as
    /// many as a part says there are.
    pub fn count<F, R>(within: &str, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        Self {
            steps: None,
            count: Some(fn2(f)),
            ..Self::steps(within)
        }
    }

    pub fn step<F, R>(mut self, name: &str, test: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedBool,
    {
        self.steps
            .get_or_insert_with(Vec::new)
            .push((name.to_string(), Part::Test(pred2(test))));
        self
    }

    /// A step that is every message of the queue of this name.
    pub fn step_queue(mut self, queue: &str) -> Self {
        self.steps
            .get_or_insert_with(Vec::new)
            .push((queue.to_string(), Part::Queue(queue.to_string())));
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

    /// `(got, key) -> result`.
    pub fn on_complete<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &str) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.on_complete = Some(Arc::new(move |g, k| call(|| f(g, k).into_feed_value())));
        self
    }

    /// `(got, missing, key) -> result`.
    pub fn on_timeout<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, &Value, &str) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.on_timeout = Some(Arc::new(move |g, m, k| {
            call(|| f(g, m, k).into_feed_value())
        }));
        self
    }

    /// `"keep"` (default) or `"latest"`.
    pub fn on_repeat(mut self, r: &str) -> Self {
        self.on_repeat = Some(r.to_string());
        self
    }

    /// Steps and count both given, as `{ steps, count }` in JS (an error at run).
    pub fn with_count<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.count = Some(fn2(f));
        self
    }

    /// Neither steps nor count (an error at run, as in JS).
    pub fn without_steps(mut self) -> Self {
        self.steps = None;
        self
    }

    time_options!();

    pub(crate) fn build(&self) -> FnResult<CollectSpec> {
        CollectSpec::build(
            "gather",
            &self.within,
            self.steps.as_ref(),
            self.count.clone(),
            self.key.clone(),
            self.keep.clone(),
            self.on_complete.clone(),
            self.on_timeout.clone(),
            &self.on_repeat,
            &self.time,
            &self.name,
        )
    }
}

/// `partId(msg)`: a part's identity, for a gather by count.
fn part_id(msg: &Value) -> String {
    let s = |k: &str| js::string_of(msg.get(k));
    if js::truthy(msg.get("transactionId")) {
        return format!(
            "{}{SEP}{}{SEP}{}",
            s("queue"),
            s("partition"),
            s("transactionId")
        );
    }
    let nullish = |v: Option<&Value>| matches!(v, None | Some(Value::Null));
    // `a ?? ''`
    let or_empty = |v: Option<&Value>| {
        if nullish(v) {
            String::new()
        } else {
            js::string_of(v)
        }
    };
    if js::truthy(msg.get("derived")) {
        let empty = Value::Object(Map::new());
        let c = match msg.get("ctx") {
            Some(c) if js::truthy(Some(c)) => c,
            _ => &empty,
        };
        let start_or_deadline = if nullish(c.get("windowStart")) {
            c.get("deadline")
        } else {
            c.get("windowStart")
        };
        return format!(
            "d{SEP}{}{SEP}{}{SEP}{}{SEP}{}",
            s("key"),
            or_empty(c.get("kind")),
            or_empty(start_or_deadline),
            or_empty(msg.get("ts"))
        );
    }
    let key_or_partition = if nullish(msg.get("key")) {
        msg.get("partition")
    } else {
        msg.get("key")
    };
    format!(
        "{}{SEP}{}{SEP}{}",
        s("position"),
        or_empty(msg.get("offset")),
        or_empty(key_or_partition)
    )
}

pub(crate) struct CollectEngine {
    pub spec: Arc<CollectSpec>,
    heap: DueHeap,
}

impl CollectEngine {
    pub fn new(spec: Arc<CollectSpec>) -> Self {
        Self {
            spec,
            heap: DueHeap::default(),
        }
    }

    /// Whether a message is a part (with steps or sides: one of their tests passes; by count: every message is).
    pub fn relevant(&self, env: &Env, t: f64) -> FnResult<bool> {
        let Some(parts) = &self.spec.parts else {
            return Ok(true);
        };
        let msg = message_of(env, t);
        for (_, p) in parts {
            if p.holds(&msg, env.value())? {
                return Ok(true);
            }
        }
        Ok(false)
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
            if js::truthy(v.get("done")) || self.spec.on_timeout.is_none() {
                continue;
            }
            let got = self.got(&v);
            let missing = self.missing(&v);
            let due = js::as_num(v.get("due")).unwrap_or(f64::NAN);
            let key = js::string_of(v.get("key"));
            let ctx = json!({
                "kind": "timeout",
                "key": v.get("key").cloned().unwrap_or(Value::Null),
                "partition": v.get("key").cloned().unwrap_or(Value::Null),
                "deadline": js::num(due - 1.0),
                "missing": missing.clone(),
            });
            let mut em = Emit::new(key.clone(), ctx, v);
            em.time = Some(due);
            em.event_time = Some(due - 1.0);
            em.compute = Some(Compute::CollectTimeout { key, got, missing });
            emits.push(em);
        }
    }

    pub fn process(
        &mut self,
        env: &Env,
        t: f64,
        emits: &mut Vec<Emit>,
        state: &mut FeedState,
    ) -> FnResult<()> {
        let spec = Arc::clone(&self.spec);
        let msg = message_of(env, t);
        let mut part: Option<String> = None;
        if let Some(parts) = &spec.parts {
            for (name, p) in parts {
                if p.holds(&msg, env.value())? {
                    part = Some(name.clone());
                    break;
                }
            }
            if part.is_none() {
                return Ok(());
            }
        }
        let key = match &spec.key {
            Some(f) => js::string_of(f(&msg, env.value())?.as_ref()),
            None => env.key.clone(),
        };
        let sk = format!("c{SEP}{key}");
        let run = state.get(&sk).cloned();
        // A gather that finished: remembered until its deadline, and quiet.
        if run.as_ref().is_some_and(|r| js::truthy(r.get("done"))) {
            return Ok(());
        }
        let snap = snapshot_of(&msg, &kept_of(&spec.keep, &msg, env.value())?);
        let next = if let Some(part) = &part {
            if let Some(r) = &run {
                if r.get("parts").and_then(|p| p.get(part.as_str())).is_some() && !spec.latest {
                    return Ok(());
                }
            }
            match &run {
                Some(r) => {
                    let mut n = r.as_object().cloned().unwrap_or_default();
                    let mut parts = r
                        .get("parts")
                        .and_then(|p| p.as_object())
                        .cloned()
                        .unwrap_or_default();
                    parts.insert(part.clone(), snap);
                    n.insert("parts".into(), Value::Object(parts));
                    Value::Object(n)
                }
                None => {
                    let mut parts = Map::new();
                    parts.insert(part.clone(), snap);
                    json!({ "key": key, "first": js::num(t), "due": js::num(t + spec.within_ms + 1.0), "parts": parts })
                }
            }
        } else {
            let id = part_id(&msg);
            if let Some(r) = &run {
                let seen = r
                    .get("ids")
                    .and_then(|i| i.as_array())
                    .is_some_and(|ids| ids.iter().any(|x| x.as_str() == Some(id.as_str())));
                if seen {
                    return Ok(());
                }
            }
            let mut expected = run
                .as_ref()
                .and_then(|r| r.get("expected"))
                .cloned()
                .unwrap_or(Value::Null);
            if let Some(f) = &spec.count {
                let n = f(&msg, env.value())?;
                if let Some(x) = js::as_num(n.as_ref()).filter(|x| x.fract() == 0.0 && *x > 0.0) {
                    expected = match js::as_num(Some(&expected)) {
                        Some(e) if !expected.is_null() => js::num(e.max(x)),
                        _ => js::num(x),
                    };
                }
            }
            match &run {
                Some(r) => {
                    let mut n = r.as_object().cloned().unwrap_or_default();
                    let mut list = r
                        .get("list")
                        .and_then(|l| l.as_array())
                        .cloned()
                        .unwrap_or_default();
                    list.push(snap);
                    let mut ids = r
                        .get("ids")
                        .and_then(|l| l.as_array())
                        .cloned()
                        .unwrap_or_default();
                    ids.push(Value::String(id));
                    n.insert("list".into(), Value::Array(list));
                    n.insert("ids".into(), Value::Array(ids));
                    n.insert("expected".into(), expected);
                    Value::Object(n)
                }
                None => json!({
                    "key": key, "first": js::num(t), "due": js::num(t + spec.within_ms + 1.0),
                    "list": [snap], "ids": [id], "expected": expected
                }),
            }
        };
        let due = js::as_num(next.get("due")).unwrap_or(f64::NAN);
        if self.complete(&next) {
            let got = self.got(&next);
            let ctx = json!({ "kind": "complete", "key": key, "partition": key, "deadline": js::num(due - 1.0) });
            let mut em = Emit::new(key.clone(), ctx, next.clone());
            em.event_time = Some(t);
            em.compute = Some(Compute::Complete {
                key: key.clone(),
                got,
            });
            emits.push(em);
            if spec.remember {
                state.set(
                    sk.clone(),
                    json!({ "key": key, "due": js::num(due), "done": true }),
                );
                if run.is_none() {
                    self.heap.push(due, sk);
                }
            } else {
                state.delete(&sk);
            }
            return Ok(());
        }
        state.set(sk.clone(), next);
        if run.is_none() {
            self.heap.push(due, sk);
        }
        Ok(())
    }

    pub fn compute(&self, c: &Compute) -> FnResult<Option<Value>> {
        match c {
            Compute::Complete { key, got } => match &self.spec.on_complete {
                Some(f) => call(|| f(got, key)),
                None => Ok(Some(json!({ "key": key, "parts": got }))),
            },
            Compute::CollectTimeout { key, got, missing } => match &self.spec.on_timeout {
                Some(f) => call(|| f(got, missing, key)),
                None => Ok(None),
            },
            _ => Ok(None),
        }
    }

    /// The run of `key`, for a live read.
    pub fn inspect(&self, key: &str, state: &FeedState) -> Option<Value> {
        state.get(&format!("c{SEP}{key}")).map(|v| self.view(v))
    }

    pub fn list<'a>(&'a self, state: &'a FeedState) -> impl Iterator<Item = Value> + 'a {
        state
            .entries()
            .filter(|(k, _)| !is_buffer_key(k))
            .map(|(_, v)| self.view(v))
    }

    fn view(&self, v: &Value) -> Value {
        let due = js::as_num(v.get("due")).unwrap_or(f64::NAN);
        let mut m = Map::new();
        if let Some(k) = v.get("key") {
            m.insert("key".into(), k.clone());
        }
        if js::truthy(v.get("done")) {
            m.insert("done".into(), Value::Bool(true));
            m.insert("until".into(), Value::String(js::iso(due - 1.0)));
            return Value::Object(m);
        }
        m.insert("got".into(), self.got(v));
        m.insert("missing".into(), self.missing(v));
        if js::truthy(v.get("list")) {
            if let Some(e) = v.get("expected") {
                m.insert("expected".into(), e.clone());
            }
        }
        m.insert("deadline".into(), Value::String(js::iso(due - 1.0)));
        Value::Object(m)
    }

    fn complete(&self, v: &Value) -> bool {
        match &self.spec.parts {
            Some(parts) => parts
                .iter()
                .all(|(n, _)| v.get("parts").and_then(|p| p.get(n.as_str())).is_some()),
            None => {
                let len = v
                    .get("list")
                    .and_then(|l| l.as_array())
                    .map_or(0, |l| l.len()) as f64;
                match v.get("expected") {
                    None | Some(Value::Null) => false,
                    e => js::as_num(e).is_some_and(|x| len >= x),
                }
            }
        }
    }

    fn got(&self, v: &Value) -> Value {
        let k = if self.spec.parts.is_some() {
            "parts"
        } else {
            "list"
        };
        v.get(k).cloned().unwrap_or(Value::Null)
    }

    fn missing(&self, v: &Value) -> Value {
        match &self.spec.parts {
            Some(parts) => Value::Array(
                parts
                    .iter()
                    .filter(|(n, _)| v.get("parts").and_then(|p| p.get(n.as_str())).is_none())
                    .map(|(n, _)| Value::String(n.clone()))
                    .collect(),
            ),
            None => match v.get("expected") {
                None | Some(Value::Null) => Value::Null,
                e => {
                    let len = v
                        .get("list")
                        .and_then(|l| l.as_array())
                        .map_or(0, |l| l.len()) as f64;
                    js::num((js::as_num(e).unwrap_or(0.0) - len).max(0.0))
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn part_ids_follow_the_js_template() {
        assert_eq!(
            part_id(&json!({ "queue": "q", "partition": "p", "transactionId": "t" })),
            "q\u{1f}p\u{1f}t"
        );
        assert_eq!(
            part_id(
                &json!({ "derived": true, "key": "k", "ctx": { "kind": "tumbling", "windowStart": 60000 }, "ts": "x" })
            ),
            "d\u{1f}k\u{1f}tumbling\u{1f}60000\u{1f}x"
        );
        assert_eq!(
            part_id(&json!({ "position": 3, "partition": "p" })),
            "3\u{1f}\u{1f}p"
        );
        assert_eq!(
            part_id(&json!({ "queue": null, "transactionId": "t" })),
            "null\u{1f}undefined\u{1f}t"
        );
    }
}
