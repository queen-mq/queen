//! The KV seam: what the broker offers the proxy, and typed helpers over it.
//!
//! One call is ONE atomic batch of the broker's KV ops (the `POST
//! /api/v1/kv` wire, 024 shapes), executed for the proxy's system tenant
//! ([`super::schema::PROXY_TENANT`]); the answer is index-aligned. An op with
//! `"required": true` that loses its precondition fails the WHOLE batch with
//! [`KvError::Precondition`] and nothing is written — that is how the unique
//! indexes of [`super::schema`] stay unique across nodes.
//!
//! Op shapes (server/src/rsm/planner/kv.rs):
//! - `{op:"get", ns, key}` → `{found, key, value, version}`
//! - `{op:"getMany", ns, keys:[..]}` → `{rows:[{key,value,version}], truncated}`
//! - `{op:"getPrefix", ns, prefix, after?, limit?, keysOnly?}` →
//!   `{rows:[{key,value,version}], truncated, nextAfter}`
//! - `{op:"put"|"putIfAbsent", ns, key, value, forever:true | ttlSeconds:N,
//!   expect?:N (0 = must not exist), required?}` → `{applied, reason?, key,
//!   value, version}`
//! - `{op:"delete", ns, key, expect?, required?}` → `{applied, reason?, ...}`
//! - `{op:"incr", ns, key, delta, forever | ttlSeconds, required?}` →
//!   `{applied, value, version}`

use std::future::Future;
use std::pin::Pin;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};

/// A boxed future, so the trait stays object-safe without `async_trait`.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

/// What the broker offers: one atomic KV batch for the proxy's tenant.
pub trait KvBackend: Send + Sync + 'static {
    fn kv(&self, ops: Vec<Value>) -> BoxFut<'_, Result<Vec<Value>, KvError>>;
}

#[derive(Debug, Clone)]
pub enum KvError {
    /// A shape or size refusal (the broker's 400/413): a proxy bug.
    Invalid { status: u16, reason: String, detail: String },
    /// A `required` op lost its precondition: nothing was written. `detail`
    /// names the op (`{index, op, ns, key, reason, version, value}`).
    Precondition { detail: Value },
    /// The broker could not answer now (no leader, retry, timeout).
    Unavailable(String),
}

impl std::fmt::Display for KvError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvError::Invalid { status, reason, detail } => {
                write!(f, "kv refused ({status} {reason}): {detail}")
            }
            KvError::Precondition { detail } => write!(f, "kv precondition lost: {detail}"),
            KvError::Unavailable(m) => write!(f, "kv unavailable: {m}"),
        }
    }
}

impl std::error::Error for KvError {}

impl KvError {
    /// Which op of the batch lost its precondition, when that is the error.
    pub fn precondition_index(&self) -> Option<usize> {
        match self {
            KvError::Precondition { detail } => detail.get("index")?.as_u64().map(|i| i as usize),
            _ => None,
        }
    }
}

/// A stored document and its version.
#[derive(Debug, Clone)]
pub struct Doc<T> {
    pub value: T,
    pub version: u64,
}

/// A write's precondition.
#[derive(Debug, Clone, Copy)]
pub enum Expect {
    /// No precondition (upsert).
    Any,
    /// The key must not exist.
    Absent,
    /// The key must be at exactly this version.
    Version(u64),
}

/// A write's expiry (every KV write declares one).
#[derive(Debug, Clone, Copy)]
pub enum Ttl {
    Forever,
    Seconds(u64),
}

fn expiry(o: &mut serde_json::Map<String, Value>, ttl: Ttl) {
    match ttl {
        Ttl::Forever => {
            o.insert("forever".into(), Value::Bool(true));
        }
        Ttl::Seconds(s) => {
            o.insert("ttlSeconds".into(), Value::from(s.max(1)));
        }
    }
}

/// `put` of `value` at `ns/key`. `required`: a lost `expect` fails the batch.
pub fn put_op(ns: &str, key: &str, value: &impl Serialize, expect: Expect, ttl: Ttl, required: bool) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("op".into(), "put".into());
    o.insert("ns".into(), ns.into());
    o.insert("key".into(), key.into());
    o.insert("value".into(), serde_json::to_value(value).unwrap_or(Value::Null));
    expiry(&mut o, ttl);
    match expect {
        Expect::Any => {}
        Expect::Absent => {
            o.insert("expect".into(), Value::from(0));
        }
        Expect::Version(v) => {
            o.insert("expect".into(), Value::from(v));
        }
    }
    if required {
        o.insert("required".into(), Value::Bool(true));
    }
    Value::Object(o)
}

/// `delete` of `ns/key`.
pub fn delete_op(ns: &str, key: &str, expect: Expect, required: bool) -> Value {
    let mut o = json!({"op":"delete","ns":ns,"key":key});
    if let Some(m) = o.as_object_mut() {
        match expect {
            Expect::Any => {}
            Expect::Absent => {
                m.insert("expect".into(), Value::from(0));
            }
            Expect::Version(v) => {
                m.insert("expect".into(), Value::from(v));
            }
        }
        if required {
            m.insert("required".into(), Value::Bool(true));
        }
    }
    o
}

/// `incr` of the integer at `ns/key` by `delta`.
pub fn incr_op(ns: &str, key: &str, delta: i64, ttl: Ttl) -> Value {
    let mut o = serde_json::Map::new();
    o.insert("op".into(), "incr".into());
    o.insert("ns".into(), ns.into());
    o.insert("key".into(), key.into());
    o.insert("delta".into(), Value::from(delta));
    expiry(&mut o, ttl);
    Value::Object(o)
}

/// One write's verdict.
#[derive(Debug, Clone)]
pub struct Written {
    pub applied: bool,
    /// Why not, when not applied (`exists`, `version_mismatch`, `absent`, …).
    pub reason: Option<String>,
    pub version: u64,
    pub value: Value,
}

/// Run a batch of writes atomically; one verdict per op.
pub async fn write(kv: &dyn KvBackend, ops: Vec<Value>) -> Result<Vec<Written>, KvError> {
    let out = kv.kv(ops).await?;
    Ok(out
        .into_iter()
        .map(|r| Written {
            applied: r.get("applied").and_then(Value::as_bool).unwrap_or(false),
            reason: r.get("reason").and_then(Value::as_str).map(str::to_string),
            version: r.get("version").and_then(Value::as_u64).unwrap_or(0),
            value: r.get("value").cloned().unwrap_or(Value::Null),
        })
        .collect())
}

fn decode<T: DeserializeOwned>(v: &Value) -> Option<T> {
    serde_json::from_value(v.clone()).ok()
}

/// The document at `ns/key`, if any (a value that does not decode as `T`
/// reads as absent and is logged).
pub async fn get<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, key: &str) -> Result<Option<Doc<T>>, KvError> {
    let out = kv.kv(vec![json!({"op":"get","ns":ns,"key":key})]).await?;
    let Some(r) = out.into_iter().next() else { return Ok(None) };
    if r.get("found").and_then(Value::as_bool) != Some(true) {
        return Ok(None);
    }
    let version = r.get("version").and_then(Value::as_u64).unwrap_or(0);
    match r.get("value").and_then(decode::<T>) {
        Some(value) => Ok(Some(Doc { value, version })),
        None => {
            tracing::warn!(target: "store", ns, key, "kv document does not decode; treated as absent");
            Ok(None)
        }
    }
}

/// Several documents of one namespace at once, in `keys` order.
pub async fn get_many<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, keys: &[String]) -> Result<Vec<Option<Doc<T>>>, KvError> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let mut found: std::collections::HashMap<String, Doc<T>> = std::collections::HashMap::new();
    for chunk in keys.chunks(500) {
        let out = kv.kv(vec![json!({"op":"getMany","ns":ns,"keys":chunk})]).await?;
        if let Some(rows) = out.first().and_then(|r| r.get("rows")).and_then(Value::as_array) {
            for row in rows {
                let (Some(k), Some(v)) = (row.get("key").and_then(Value::as_str), row.get("value").and_then(decode::<T>)) else {
                    continue;
                };
                found.insert(
                    k.to_string(),
                    Doc { value: v, version: row.get("version").and_then(Value::as_u64).unwrap_or(0) },
                );
            }
        }
    }
    Ok(keys.iter().map(|k| found.remove(k)).collect())
}

/// Every document of `ns` whose key starts with `prefix`, in key order
/// (pages through `getPrefix` until it is not truncated).
pub async fn scan<T: DeserializeOwned>(kv: &dyn KvBackend, ns: &str, prefix: &str) -> Result<Vec<(String, Doc<T>)>, KvError> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut op = json!({"op":"getPrefix","ns":ns,"prefix":prefix,"limit":1000});
        if let (Some(a), Some(m)) = (&after, op.as_object_mut()) {
            m.insert("after".into(), Value::String(a.clone()));
        }
        let res = kv.kv(vec![op]).await?;
        let Some(r) = res.into_iter().next() else { break };
        let rows = r.get("rows").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut last = None;
        for row in &rows {
            let Some(k) = row.get("key").and_then(Value::as_str) else { continue };
            last = Some(k.to_string());
            if let Some(v) = row.get("value").and_then(decode::<T>) {
                out.push((k.to_string(), Doc { value: v, version: row.get("version").and_then(Value::as_u64).unwrap_or(0) }));
            }
        }
        let truncated = r.get("truncated").and_then(Value::as_bool).unwrap_or(false);
        let next = r.get("nextAfter").and_then(Value::as_str).map(str::to_string).or(last);
        if !truncated || rows.is_empty() || next.is_none() {
            break;
        }
        after = next;
    }
    Ok(out)
}

/// Every KEY of `ns` under `prefix` (index scans: the value is not needed).
pub async fn scan_keys(kv: &dyn KvBackend, ns: &str, prefix: &str) -> Result<Vec<String>, KvError> {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let mut op = json!({"op":"getPrefix","ns":ns,"prefix":prefix,"limit":1000,"keysOnly":true});
        if let (Some(a), Some(m)) = (&after, op.as_object_mut()) {
            m.insert("after".into(), Value::String(a.clone()));
        }
        let res = kv.kv(vec![op]).await?;
        let Some(r) = res.into_iter().next() else { break };
        let rows = r.get("rows").and_then(Value::as_array).cloned().unwrap_or_default();
        for row in &rows {
            if let Some(k) = row.get("key").and_then(Value::as_str) {
                out.push(k.to_string());
            }
        }
        let truncated = r.get("truncated").and_then(Value::as_bool).unwrap_or(false);
        let next = r.get("nextAfter").and_then(Value::as_str).map(str::to_string).or_else(|| out.last().cloned());
        if !truncated || rows.is_empty() || next.is_none() {
            break;
        }
        after = next;
    }
    Ok(out)
}
