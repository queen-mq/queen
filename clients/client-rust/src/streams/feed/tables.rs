//! `.join_table()` on the tenant feed (feed/tables.js): the latest row per key
//! of a table queue, looked up for every message that reaches the step.
//!
//! State key `t\x1f<key>`; a value is `{ v, p }`: the row and its position.

use std::sync::Arc;

use serde_json::{json, Map, Value};

use super::env::{message_of, Env};
use super::ops::{call, fn1, fn2, Desc, Fn1, Fn2, FnResult, IntoFeedValue, SelectRowFn};
use super::state::FeedState;
use crate::js;

const SEP: char = '\u{1f}';

/// A table join, checked.
pub(crate) struct TableSpec {
    pub queues: Vec<String>,
    pub on: Fn2,
    pub key: Option<Fn1>,
    pub value: Option<Fn1>,
    pub select: Option<SelectRowFn>,
    pub drop_missing: bool,
    pub initial_all: bool,
    pub name: Option<String>,
    pub desc: Desc,
}

/// `.join_table(TableJoin::new("settings", |m, v| m["data"]["property"].clone()))`:
/// join every message with the latest row of a table queue (one message per
/// change; null deletes). A message sees the table as of its own position.
#[derive(Clone)]
pub struct TableJoin {
    queues: Vec<String>,
    on: Fn2,
    key: Option<Fn1>,
    value: Option<Fn1>,
    select: Option<SelectRowFn>,
    missing: Option<String>,
    initial: Option<String>,
    name: Option<String>,
}

impl TableJoin {
    pub fn new<F, R>(queue: &str, on: F) -> Self
    where
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        Self::queues([queue], on)
    }

    /// A table kept in several queues (names, no patterns).
    pub fn queues<I, S, F, R>(queues: I, on: F) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
        F: Fn(&Value, Option<&Value>) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        Self {
            queues: queues.into_iter().map(Into::into).collect(),
            on: fn2(on),
            key: None,
            value: None,
            select: None,
            missing: None,
            initial: None,
            name: None,
        }
    }

    /// A row's key (default: its partition).
    pub fn key<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.key = Some(fn1(f));
        self
    }

    /// What the table keeps of a row (default: its payload; null deletes).
    pub fn value<F, R>(mut self, f: F) -> Self
    where
        F: Fn(&Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.value = Some(fn1(f));
        self
    }

    /// `(value, row, msg) -> result` (`row` is `None` when missing).
    pub fn select<F, R>(mut self, f: F) -> Self
    where
        F: Fn(Option<&Value>, Option<&Value>, &Value) -> R + Send + Sync + 'static,
        R: IntoFeedValue,
    {
        self.select = Some(Arc::new(move |v, r, m| {
            call(|| f(v, r, m).into_feed_value())
        }));
        self
    }

    /// `"keep"` (default: select gets no row) or `"drop"`.
    pub fn missing(mut self, m: &str) -> Self {
        self.missing = Some(m.to_string());
        self
    }

    /// `"all"` (default: a new stream first reads the table queue from its beginning) or `"now"`.
    pub fn initial(mut self, i: &str) -> Self {
        self.initial = Some(i.to_string());
        self
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = (!name.is_empty()).then(|| name.to_string());
        self
    }

    pub(crate) fn build(&self) -> FnResult<TableSpec> {
        if self.queues.is_empty()
            || self
                .queues
                .iter()
                .any(|q| q.is_empty() || q.contains(['*', '?']))
        {
            return Err(
                "joinTable: queue is the table queue's name (or a list of names, no patterns)"
                    .into(),
            );
        }
        let mut queues: Vec<String> = Vec::new();
        for q in &self.queues {
            if !queues.contains(q) {
                queues.push(q.clone());
            }
        }
        let missing = self.missing.clone().unwrap_or_else(|| "keep".into());
        if missing != "keep" && missing != "drop" {
            return Err(format!(
                "joinTable: missing is 'keep' or 'drop', got {}",
                js::string_json(&missing)
            ));
        }
        let initial = self.initial.clone().unwrap_or_else(|| "all".into());
        if initial != "all" && initial != "now" {
            return Err(format!(
                "joinTable: initial is 'all' (read the table queue from its beginning) or 'now', got {}",
                js::string_json(&initial)
            ));
        }
        let qs = queues
            .iter()
            .map(|q| js::string_json(q))
            .collect::<Vec<_>>()
            .join(",");
        Ok(TableSpec {
            queues,
            on: self.on.clone(),
            key: self.key.clone(),
            value: self.value.clone(),
            select: self.select.clone(),
            drop_missing: missing == "drop",
            initial_all: initial == "all",
            name: self.name.clone(),
            desc: vec![
                ("kind".into(), "\"table\"".into()),
                ("queues".into(), format!("[{qs}]")),
                ("key".into(), self.key.is_some().to_string()),
                ("value".into(), self.value.is_some().to_string()),
            ],
        })
    }
}

pub(crate) struct TableStep {
    pub spec: Arc<TableSpec>,
}

fn nullish(v: Option<&Value>) -> bool {
    matches!(v, None | Some(Value::Null))
}

impl TableStep {
    pub fn new(spec: Arc<TableSpec>) -> Self {
        Self { spec }
    }

    pub fn open(&self, state: &FeedState) -> usize {
        state.size()
    }

    /// A message of the table's queue: the row it sets, or deletes.
    pub fn apply_row(&self, entry: &Value, state: &mut FeedState) -> FnResult<()> {
        let key = match &self.spec.key {
            Some(f) => f(entry)?,
            None => entry.get("partition").cloned(),
        };
        if nullish(key.as_ref()) {
            return Ok(());
        }
        let value = match &self.spec.value {
            Some(f) => f(entry)?,
            None => entry.get("data").cloned(),
        };
        let sk = format!("t{SEP}{}", js::string_of(key.as_ref()));
        match value {
            None | Some(Value::Null) => state.delete(&sk),
            Some(v) => {
                let mut m = Map::new();
                m.insert("v".into(), v);
                if let Some(p) = entry.get("position") {
                    m.insert("p".into(), p.clone());
                }
                state.set(sk, Value::Object(m));
            }
        }
        Ok(())
    }

    /// A stream message: the envelope with the row joined in, or none when
    /// the row is missing and missing is `drop`.
    pub fn apply(&self, env: &Env, t: f64, state: &FeedState) -> FnResult<Vec<Env>> {
        let msg = message_of(env, t);
        let key = (self.spec.on)(&msg, env.value())?;
        let row = if nullish(key.as_ref()) {
            None
        } else {
            state
                .get(&format!("t{SEP}{}", js::string_of(key.as_ref())))
                .and_then(|f| f.get("v"))
                .cloned()
        };
        if row.is_none() && self.spec.drop_missing {
            return Ok(Vec::new());
        }
        let value = match &self.spec.select {
            Some(f) => f(env.value(), row.as_ref(), &msg)?,
            None => {
                let mut m = Map::new();
                if let Some(v) = env.value() {
                    m.insert("value".into(), v.clone());
                }
                if let Some(r) = row {
                    m.insert("row".into(), r);
                }
                Some(Value::Object(m))
            }
        };
        if nullish(value.as_ref()) {
            return Ok(Vec::new());
        }
        Ok(vec![env.with_value(value)])
    }

    pub fn inspect(&self, key: &str, state: &FeedState) -> Option<Value> {
        state.get(&format!("t{SEP}{key}")).map(|f| {
            let mut m = Map::new();
            m.insert("key".into(), json!(key));
            if let Some(r) = f.get("v") {
                m.insert("row".into(), r.clone());
            }
            if let Some(p) = f.get("p") {
                m.insert("position".into(), p.clone());
            }
            Value::Object(m)
        })
    }

    pub fn list<'a>(&'a self, state: &'a FeedState) -> impl Iterator<Item = Value> + 'a {
        state.entries().map(|(k, v)| {
            let mut m = Map::new();
            m.insert("key".into(), Value::String(k.chars().skip(2).collect()));
            if let Some(r) = v.get("v") {
                m.insert("row".into(), r.clone());
            }
            if let Some(p) = v.get("p") {
                m.insert("position".into(), p.clone());
            }
            Value::Object(m)
        })
    }
}
