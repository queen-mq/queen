//! An in-memory [`KvBackend`] with the broker's KV semantics (atomic batch,
//! `expect` versions, `required`, TTL, `getPrefix` paging, `incr`), for
//! tests of every repository, and [`Down`], a backend that never answers.
//! Not production backends.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{json, Value};

use super::kv::{BoxFut, KvBackend, KvError};

#[derive(Clone)]
struct Row {
    value: Value,
    version: u64,
    expires_at_ms: Option<u128>,
}

#[derive(Default)]
pub struct MemKv {
    rows: Mutex<BTreeMap<(String, String), Row>>,
    /// The version counter (monotone across keys, like the broker's).
    next: Mutex<u64>,
}

impl MemKv {
    pub fn new() -> MemKv {
        MemKv::default()
    }

    /// Every live key of `ns`, for test assertions.
    pub fn keys(&self, ns: &str) -> Vec<String> {
        let now = now_ms();
        self.rows
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .iter()
            .filter(|((n, _), r)| n == ns && r.expires_at_ms.is_none_or(|e| e > now))
            .map(|((_, k), _)| k.clone())
            .collect()
    }
}

/// A backend that never answers: every batch fails [`KvError::Unavailable`],
/// like a KV with no leader. For the fail-open / fail-closed tests.
pub struct Down;

impl KvBackend for Down {
    fn kv(&self, _ops: Vec<Value>) -> BoxFut<'_, Result<Vec<Value>, KvError>> {
        Box::pin(async { Err(KvError::Unavailable("no leader".into())) })
    }
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0)
}

fn s(o: &Value, k: &str) -> String {
    o.get(k).and_then(Value::as_str).unwrap_or("").to_string()
}

fn expiry(o: &Value) -> Option<u128> {
    o.get("ttlSeconds")
        .and_then(Value::as_u64)
        .map(|t| now_ms() + t as u128 * 1000)
}

impl KvBackend for MemKv {
    fn kv(&self, ops: Vec<Value>) -> BoxFut<'_, Result<Vec<Value>, KvError>> {
        Box::pin(async move {
            let now = now_ms();
            let mut rows = self.rows.lock().unwrap_or_else(|p| p.into_inner());
            let mut next = self.next.lock().unwrap_or_else(|p| p.into_inner());
            let mut work = rows.clone();
            let mut version = *next;
            let live = |m: &BTreeMap<(String, String), Row>, ns: &str, k: &str| -> Option<Row> {
                m.get(&(ns.to_string(), k.to_string()))
                    .filter(|r| r.expires_at_ms.is_none_or(|e| e > now))
                    .cloned()
            };
            let mut out = Vec::with_capacity(ops.len());
            for (i, o) in ops.iter().enumerate() {
                let op = s(o, "op");
                let ns = s(o, "ns");
                let key = s(o, "key");
                let required = o.get("required").and_then(Value::as_bool).unwrap_or(false);
                let lost = |reason: &str, cur: &Option<Row>| KvError::Precondition {
                    detail: json!({"index":i,"op":op,"ns":ns,"key":key,"reason":reason,
                        "version":cur.as_ref().map(|r| r.version).unwrap_or(0),
                        "value":cur.as_ref().map(|r| r.value.clone()).unwrap_or(Value::Null)}),
                };
                match op.as_str() {
                    "get" => {
                        out.push(match live(&work, &ns, &key) {
                            Some(r) => json!({"index":i,"op":"get","found":true,"key":key,"value":r.value,"version":r.version}),
                            None => json!({"index":i,"op":"get","found":false,"key":key,"value":null,"version":0}),
                        });
                    }
                    "getMany" => {
                        let keys: Vec<String> = o
                            .get("keys")
                            .and_then(Value::as_array)
                            .map(|a| a.iter().filter_map(|k| k.as_str().map(str::to_string)).collect())
                            .unwrap_or_default();
                        let rows: Vec<Value> = keys
                            .iter()
                            .filter_map(|k| live(&work, &ns, k).map(|r| json!({"key":k,"value":r.value,"version":r.version})))
                            .collect();
                        out.push(json!({"index":i,"op":"getMany","rows":rows,"truncated":false}));
                    }
                    "getPrefix" => {
                        let prefix = s(o, "prefix");
                        if prefix.is_empty() {
                            return Err(KvError::Invalid {
                                status: 400,
                                reason: "kv_prefix_required".into(),
                                detail: format!("op at index {i}: getPrefix needs a non-empty prefix"),
                            });
                        }
                        let after = o.get("after").and_then(Value::as_str).map(str::to_string);
                        let limit = o.get("limit").and_then(Value::as_u64).unwrap_or(100).clamp(1, 1000) as usize;
                        let keys_only = o.get("keysOnly").and_then(Value::as_bool).unwrap_or(false);
                        let mut page = Vec::new();
                        let mut truncated = false;
                        for ((n, k), r) in work.range((ns.clone(), prefix.clone())..) {
                            if n != &ns || !k.starts_with(&prefix) {
                                break;
                            }
                            if r.expires_at_ms.is_some_and(|e| e <= now) {
                                continue;
                            }
                            if after.as_ref().is_some_and(|a| k <= a) {
                                continue;
                            }
                            if page.len() == limit {
                                truncated = true;
                                break;
                            }
                            page.push(if keys_only {
                                json!({"key":k,"version":r.version})
                            } else {
                                json!({"key":k,"value":r.value,"version":r.version})
                            });
                        }
                        let next_after = if truncated {
                            page.last().and_then(|r| r.get("key").cloned()).unwrap_or(Value::Null)
                        } else {
                            Value::Null
                        };
                        out.push(json!({"index":i,"op":"getPrefix","rows":page,"truncated":truncated,"nextAfter":next_after}));
                    }
                    "put" | "putIfAbsent" => {
                        let cur = live(&work, &ns, &key);
                        let expect = o.get("expect").and_then(Value::as_u64);
                        let refused = if op == "putIfAbsent" || expect == Some(0) {
                            cur.as_ref().map(|_| "exists")
                        } else {
                            match (expect, &cur) {
                                (Some(v), Some(r)) if r.version != v => Some("version_mismatch"),
                                (Some(_), None) => Some("absent"),
                                _ => None,
                            }
                        };
                        if let Some(reason) = refused {
                            if required {
                                return Err(lost(reason, &cur));
                            }
                            out.push(json!({"index":i,"op":op,"applied":false,"reason":reason,"key":key,
                                "value":cur.as_ref().map(|r| r.value.clone()).unwrap_or(Value::Null),
                                "version":cur.as_ref().map(|r| r.version).unwrap_or(0)}));
                            continue;
                        }
                        version += 1;
                        let value = o.get("value").cloned().unwrap_or(Value::Null);
                        work.insert((ns.clone(), key.clone()), Row { value: value.clone(), version, expires_at_ms: expiry(o) });
                        out.push(json!({"index":i,"op":op,"applied":true,"key":key,"value":value,"version":version}));
                    }
                    "delete" => {
                        let cur = live(&work, &ns, &key);
                        let expect = o.get("expect").and_then(Value::as_u64);
                        let refused = match (expect, &cur) {
                            (Some(0), Some(_)) => Some("exists"),
                            (Some(v), Some(r)) if v != 0 && r.version != v => Some("version_mismatch"),
                            (Some(v), None) if v != 0 => Some("absent"),
                            _ => None,
                        };
                        if let Some(reason) = refused {
                            if required {
                                return Err(lost(reason, &cur));
                            }
                            out.push(json!({"index":i,"op":"delete","applied":false,"reason":reason,"key":key,"value":null,"version":0}));
                            continue;
                        }
                        let existed = work.remove(&(ns.clone(), key.clone())).is_some() && cur.is_some();
                        out.push(json!({"index":i,"op":"delete","applied":existed,"key":key,"value":null,"version":0}));
                    }
                    "incr" => {
                        let cur = live(&work, &ns, &key);
                        let delta = o.get("delta").and_then(Value::as_i64).unwrap_or(1);
                        let base = cur.as_ref().and_then(|r| r.value.as_i64()).unwrap_or(0);
                        version += 1;
                        let value = Value::from(base + delta);
                        work.insert((ns.clone(), key.clone()), Row { value: value.clone(), version, expires_at_ms: expiry(o) });
                        out.push(json!({"index":i,"op":"incr","applied":true,"key":key,"value":value,"version":version}));
                    }
                    other => {
                        return Err(KvError::Invalid {
                            status: 400,
                            reason: "kv_bad_op".into(),
                            detail: format!("op at index {i}: unknown op {other:?}"),
                        })
                    }
                }
            }
            *rows = work;
            *next = version;
            Ok(out)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::super::kv::{self, Expect, Ttl};
    use super::*;

    #[tokio::test]
    async fn required_put_is_all_or_nothing() {
        let m = MemKv::new();
        kv::write(&m, vec![kv::put_op("px.t", "#a", &1, Expect::Absent, Ttl::Forever, true)]).await.unwrap();
        let err = kv::write(
            &m,
            vec![
                kv::put_op("px.t", "#b", &2, Expect::Absent, Ttl::Forever, true),
                kv::put_op("px.t", "#a", &3, Expect::Absent, Ttl::Forever, true),
            ],
        )
        .await
        .unwrap_err();
        assert_eq!(err.precondition_index(), Some(1));
        assert_eq!(m.keys("px.t"), vec!["#a".to_string()]);
    }

    #[tokio::test]
    async fn scan_pages_through_a_prefix() {
        let m = MemKv::new();
        let ops = (0..2500).map(|i| kv::put_op("px.t", &format!("#{i:05}"), &i, Expect::Any, Ttl::Forever, false)).collect();
        kv::write(&m, ops).await.unwrap();
        let all: Vec<(String, kv::Doc<i64>)> = kv::scan(&m, "px.t", "#").await.unwrap();
        assert_eq!(all.len(), 2500);
        assert_eq!(all[2499].1.value, 2499);
        assert_eq!(kv::scan_keys(&m, "px.t", "#01").await.unwrap().len(), 1000);
    }
}
