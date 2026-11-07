//! What a feed stream is, for whoever looks at it from outside (the
//! console): the queues it reads, its steps in order, where its results go,
//! and what it does with a message that keeps failing (feed/about.js).
//!
//! The active runner writes it to `queen-feed` / `<queryId>#about` in its
//! first commit after it becomes active, under the lock like the rest of the
//! commit (one record per query: every shard writes the same one). Nothing
//! reads it back to run the stream: the chain in the code is the stream.
//! Functions are not described (a `.map(fn)` is `map`), and long values are
//! cut.

use serde_json::{json, Map, Value};

use crate::feed::{Since, WhereCondition};
use crate::js;

use super::ops::{Desc, FeedDuration};
use super::pipeline::{Pipeline, PlanStep, StatelessOp};
use super::runner::Poison;
use super::{FeedSource, Terminal};

/// The key of a query's description in `queen-feed`.
pub(crate) fn about_key(query_id: &str) -> String {
    format!("{query_id}#about")
}

const FORMAT: i64 = 1;
const MAX_TEXT: usize = 200;
const MAX_LIST: usize = 50;
const MAX_DEPTH: usize = 4;

/// A copy cut to a size worth storing: long strings, long lists, deep objects.
pub(crate) fn clip(v: &Value, depth: usize) -> Value {
    match v {
        Value::String(s) => {
            if js::utf16_len(s) > MAX_TEXT {
                let units: Vec<u16> = s.encode_utf16().take(MAX_TEXT).collect();
                Value::String(format!("{}…", String::from_utf16_lossy(&units)))
            } else {
                v.clone()
            }
        }
        Value::Array(items) => {
            let mut out: Vec<Value> = items
                .iter()
                .take(MAX_LIST)
                .map(|x| clip(x, depth + 1))
                .collect();
            if items.len() > MAX_LIST {
                out.push(Value::String(format!("… {} more", items.len() - MAX_LIST)));
            }
            Value::Array(out)
        }
        Value::Object(m) => {
            if depth >= MAX_DEPTH {
                return Value::String("…".into());
            }
            Value::Object(
                m.iter()
                    .map(|(k, x)| (k.clone(), clip(x, depth + 1)))
                    .collect(),
            )
        }
        other => other.clone(),
    }
}

fn desc_value(d: &Desc) -> Value {
    let mut m = Map::new();
    for (k, text) in d {
        m.insert(k.clone(), serde_json::from_str(text).unwrap_or(Value::Null));
    }
    Value::Object(m)
}

fn where_of(c: &WhereCondition) -> Value {
    clip(&c.to_value(), 0)
}

fn list(v: &Option<Vec<String>>) -> Value {
    match v {
        Some(l) => clip(&json!(l), 0),
        None => Value::Null,
    }
}

pub(crate) struct AboutInput<'a> {
    pub steps: &'a [PlanStep],
    pub pipeline: &'a Pipeline,
    pub source: &'a FeedSource,
    pub pushdown: &'a [WhereCondition],
    pub hash: &'a str,
    pub terminal: Option<&'a Terminal>,
    pub query_id: &'a str,
    pub shard: Option<(u32, u32)>,
    pub shard_by_fn: bool,
    pub poison: &'a Poison,
    pub exclusive: bool,
    pub lock_ttl: &'a FeedDuration,
    pub position_commit_millis: f64,
}

/// The description of one stream.
pub(crate) fn describe_feed_stream(a: &AboutInput<'_>) -> Value {
    let mut reads = Map::new();
    reads.insert("queues".into(), list(&a.source.queues));
    reads.insert("partitions".into(), list(&a.source.partitions));
    reads.insert(
        "where".into(),
        Value::Array(a.pushdown.iter().map(where_of).collect()),
    );
    if let Some(k) = &a.source.kinds {
        reads.insert("kinds".into(), clip(&json!(k), 0));
    }
    if let Some(s) = &a.source.since {
        let v = match s {
            Since::Text(t) => json!(t),
            Since::Millis(ms) => js::num(*ms),
            Since::Time(t) => {
                let ms = match t.duration_since(std::time::SystemTime::UNIX_EPOCH) {
                    Ok(d) => d.as_millis() as f64,
                    Err(e) => -(e.duration().as_millis() as f64),
                };
                js::try_iso(ms).map(Value::String).unwrap_or(Value::Null)
            }
        };
        reads.insert("since".into(), clip(&v, 0));
    }
    let tables = a.pipeline.table_queues();
    if !tables.is_empty() {
        reads.insert("tables".into(), clip(&json!(tables), 0));
    }
    let steps: Vec<Value> = a
        .steps
        .iter()
        .enumerate()
        .map(|(i, s)| step_of(s, a.pipeline.clock_kind(i)))
        .collect();
    let sink = match a.terminal {
        None => Value::Null,
        Some(Terminal::Foreach(_)) => json!({ "kind": "foreach" }),
        Some(Terminal::ForeachBatch(b)) => json!({
            "kind": "foreachBatch",
            "maxItems": b.max_items,
            "maxWaitMillis": b.max_wait_millis
        }),
        Some(Terminal::To(sink)) if sink.route.is_some() => json!({ "kind": "route" }),
        Some(Terminal::To(sink)) => {
            json!({ "kind": "queues", "queues": clip(&json!(sink.queues), 0) })
        }
    };
    let on_poison = match a.poison {
        Poison::DeadLetter(q) => json!({ "queue": q }),
        Poison::Skip => json!("skip"),
        Poison::Stop => json!("stop"),
    };
    let shards = match a.shard {
        Some((_, count)) => {
            json!({ "count": count, "by": if a.shard_by_fn { "function" } else { "partition" } })
        }
        None => Value::Null,
    };
    let lock_ttl = match a.lock_ttl {
        FeedDuration::Text(t) => clip(&json!(t), 0),
        FeedDuration::Seconds(s) => js::num(*s),
    };
    json!({
        "v": FORMAT,
        "queryId": a.query_id,
        "hash": if a.hash.is_empty() { Value::Null } else { json!(a.hash) },
        "client": format!("client-rust {}", env!("CARGO_PKG_VERSION")),
        "reads": Value::Object(reads),
        "steps": steps,
        "sink": sink,
        "onPoison": on_poison,
        "shards": shards,
        "exclusive": a.exclusive,
        "lockTtl": lock_ttl,
        "positionCommitMillis": if a.position_commit_millis.is_finite() { js::num(a.position_commit_millis) } else { Value::Null },
    })
}

/// One compiled step as the description says it; `clock` is the one it runs on.
fn step_of(s: &PlanStep, clock: Option<&'static str>) -> Value {
    let mut out = Map::new();
    match s {
        PlanStep::Ops(ops) => {
            out.insert("kind".into(), json!("ops"));
            let names: Vec<&str> = ops
                .iter()
                .map(|op| match op {
                    StatelessOp::Map(_) => "map",
                    StatelessOp::Filter(_) => "filter",
                    StatelessOp::Where(_) => "where",
                    StatelessOp::FlatMap(_) => "flatMap",
                    StatelessOp::KeyBy(_) => "keyBy",
                })
                .collect();
            out.insert("ops".into(), json!(names));
            let wheres: Vec<Value> = ops
                .iter()
                .filter_map(|op| match op {
                    StatelessOp::Where(c) => Some(where_of(c)),
                    _ => None,
                })
                .collect();
            if !wheres.is_empty() {
                out.insert("where".into(), Value::Array(wheres));
            }
            return Value::Object(out);
        }
        PlanStep::Window {
            window,
            reducer,
            name,
        } => {
            out.insert("kind".into(), json!("window"));
            out.insert("name".into(), json!(name));
            out.insert("window".into(), clip(&desc_value(&window.desc), 0));
            out.insert("reducer".into(), clip(&desc_value(&reducer.desc), 0));
        }
        PlanStep::Pattern { op, name } => {
            let config = clip(&desc_value(&op.desc), 0);
            out.insert(
                "kind".into(),
                config.get("kind").cloned().unwrap_or(json!("pattern")),
            );
            out.insert("name".into(), json!(name));
            out.insert("config".into(), config);
        }
        PlanStep::Collect { op, name } => {
            let config = clip(&desc_value(&op.desc), 0);
            out.insert(
                "kind".into(),
                config.get("kind").cloned().unwrap_or(json!("collect")),
            );
            out.insert("name".into(), json!(name));
            out.insert("config".into(), config);
        }
        PlanStep::Table { op, name } => {
            out.insert("kind".into(), json!("table"));
            out.insert("name".into(), json!(name));
            out.insert("config".into(), clip(&desc_value(&op.desc), 0));
        }
    }
    if let Some(c) = clock {
        out.insert("clock".into(), json!(c));
    }
    Value::Object(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clip_cuts_long_strings_lists_and_depth() {
        let long = "x".repeat(250);
        assert_eq!(clip(&json!(long), 0).as_str().unwrap().chars().count(), 201);
        let l: Vec<i64> = (0..60).collect();
        let c = clip(&json!(l), 0);
        assert_eq!(c.as_array().unwrap().len(), 51);
        assert_eq!(c[50], json!("… 10 more"));
        let deep = json!({ "a": { "b": { "c": { "d": { "e": 1 } } } } });
        assert_eq!(clip(&deep, 0)["a"]["b"]["c"]["d"], json!("…"));
    }
}
