//! An envelope: one message (or one step's result) on its way through the
//! steps of a feed stream, and the shapes the JS engine makes of it
//! (`messageOf`, `snapshotOf`, `pack` / `unpack` in patterns.js and clock.js).

use std::sync::Arc;

use serde_json::{Map, Value};

use crate::js;

/// What an envelope carries as its value.
#[derive(Clone, Debug)]
pub(crate) enum EnvValue {
    /// The message's `data` (a feed message nothing has mapped).
    Data,
    /// The message itself (a step's result: msg and value are one).
    Msg,
    /// A value of its own; `None` is JavaScript's `undefined`.
    Own(Option<Arc<Value>>),
}

/// `{ msg, key, value, ctx?, derived?, position?, eventTime? }`.
#[derive(Clone, Debug)]
pub(crate) struct Env {
    pub msg: Arc<Value>,
    pub key: String,
    pub value: EnvValue,
    pub ctx: Option<Value>,
    pub derived: bool,
    pub position: Option<u64>,
    pub event_time: Option<f64>,
}

impl Env {
    /// A feed message as it enters: `{ msg: entry, key: partition, value: data }`.
    pub fn of_entry(entry: Arc<Value>) -> Self {
        let key = entry
            .get("partition")
            .map(|p| js::string_of(Some(p)))
            .unwrap_or_else(|| "undefined".into());
        Self {
            msg: entry,
            key,
            value: EnvValue::Data,
            ctx: None,
            derived: false,
            position: None,
            event_time: None,
        }
    }

    /// `env.value`.
    pub fn value(&self) -> Option<&Value> {
        match &self.value {
            EnvValue::Data => self.msg.get("data"),
            EnvValue::Msg => Some(&self.msg),
            EnvValue::Own(v) => v.as_deref(),
        }
    }

    /// `env.value !== undefined ? env.value : env.msg`: what a window folds.
    pub fn value_or_msg(&self) -> &Value {
        self.value().unwrap_or(&self.msg)
    }

    /// `{ ...env, value }`.
    pub fn with_value(&self, value: Option<Value>) -> Self {
        let mut e = self.clone();
        e.value = EnvValue::Own(value.map(Arc::new));
        e
    }

    /// `{ ...env, key }`.
    pub fn with_key(&self, key: String) -> Self {
        let mut e = self.clone();
        e.key = key;
        e
    }
}

/// The message a pattern, join, gather or table join sees for an envelope:
/// the feed message itself, or, after an earlier stateful step, that step's
/// result in the feed's shape (`data` is the result, `queue` is null,
/// `partition` the key, `ctx` what the step said about it).
pub(crate) fn message_of(env: &Env, t: f64) -> Arc<Value> {
    if !env.derived {
        return Arc::clone(&env.msg);
    }
    let mut m = Map::new();
    m.insert(
        "position".into(),
        env.position.map(Value::from).unwrap_or(Value::Null),
    );
    m.insert("queue".into(), Value::Null);
    m.insert("partition".into(), Value::String(env.key.clone()));
    m.insert("key".into(), Value::String(env.key.clone()));
    if let Some(v) = env.value() {
        m.insert("data".into(), v.clone());
    }
    m.insert("ctx".into(), env.ctx.clone().unwrap_or(Value::Null));
    m.insert(
        "ts".into(),
        if t.is_finite() {
            js::try_iso(t).map(Value::String).unwrap_or(Value::Null)
        } else {
            Value::Null
        },
    );
    m.insert("derived".into(), Value::Bool(true));
    Arc::new(Value::Object(m))
}

/// What a step keeps of a message: `{ value }` from its `keep`, else all of it.
pub(crate) type Kept = Option<Option<Value>>;

/// `snapshotOf(msg, kept)`: the parts of a message a waiting state keeps;
/// `data` is what `keep` answered when the step has one.
pub(crate) fn snapshot_of(msg: &Value, kept: &Kept) -> Value {
    let mut snap = Map::new();
    let copy = |snap: &mut Map<String, Value>, k: &str| {
        if let Some(v) = msg.get(k) {
            snap.insert(k.to_string(), v.clone());
        }
    };
    for k in [
        "position",
        "queue",
        "partition",
        "partitionId",
        "offset",
        "id",
        "transactionId",
        "ts",
    ] {
        copy(&mut snap, k);
    }
    // `createdAt: msg.createdAt ?? msg.ts`
    match msg.get("createdAt") {
        Some(v) if !v.is_null() => {
            snap.insert("createdAt".into(), v.clone());
        }
        _ => {
            if let Some(ts) = msg.get("ts") {
                snap.insert("createdAt".into(), ts.clone());
            }
        }
    }
    match kept {
        None => copy(&mut snap, "data"),
        Some(Some(v)) => {
            snap.insert("data".into(), v.clone());
        }
        Some(None) => {}
    }
    if js::truthy(msg.get("derived")) {
        snap.insert("derived".into(), Value::Bool(true));
        copy(&mut snap, "key");
        if js::truthy(msg.get("ctx")) {
            copy(&mut snap, "ctx");
        }
    }
    Value::Object(snap)
}

/// An envelope as a held entry: the message, and only what differs from it.
/// (JS tests identity — `value === msg`, `value === msg.data` — and this
/// tests equality, which reads back the same.)
pub(crate) fn pack(env: &Env) -> Value {
    let mut e = Map::new();
    e.insert("m".into(), (*env.msg).clone());
    e.insert("k".into(), Value::String(env.key.clone()));
    match &env.value {
        EnvValue::Msg => {
            e.insert("vm".into(), Value::from(1));
        }
        EnvValue::Data if env.msg.is_object() => {
            e.insert("vd".into(), Value::from(1));
        }
        EnvValue::Data => {
            if let Some(v) = env.value() {
                e.insert("v".into(), v.clone());
            }
        }
        EnvValue::Own(v) => {
            let same_msg = v.as_deref().is_some_and(|x| js::json_eq(x, &env.msg));
            let same_data = env.msg.is_object()
                && match (v.as_deref(), env.msg.get("data")) {
                    (None, None) => true,
                    (Some(x), Some(d)) => js::json_eq(x, d),
                    _ => false,
                };
            if same_msg {
                e.insert("vm".into(), Value::from(1));
            } else if same_data {
                e.insert("vd".into(), Value::from(1));
            } else if let Some(x) = v {
                e.insert("v".into(), (**x).clone());
            }
        }
    }
    if env.derived {
        e.insert("d".into(), Value::from(1));
    }
    if let Some(c) = &env.ctx {
        e.insert("c".into(), c.clone());
    }
    if let Some(p) = env.position {
        e.insert("p".into(), Value::from(p));
    }
    if let Some(t) = env.event_time.filter(|t| t.is_finite()) {
        e.insert("et".into(), js::num(t));
    }
    Value::Object(e)
}

pub(crate) fn unpack(e: &Value) -> Env {
    let msg = Arc::new(e.get("m").cloned().unwrap_or(Value::Null));
    let key = match e.get("k") {
        Some(Value::String(s)) => s.clone(),
        other => js::string_of(other),
    };
    let value = if js::truthy(e.get("vm")) {
        EnvValue::Msg
    } else if js::truthy(e.get("vd")) {
        if msg.is_object() {
            EnvValue::Data
        } else {
            EnvValue::Own(None)
        }
    } else {
        EnvValue::Own(e.get("v").cloned().map(Arc::new))
    };
    Env {
        msg,
        key,
        value,
        ctx: e.get("c").cloned(),
        derived: js::truthy(e.get("d")),
        position: e.get("p").and_then(|p| p.as_u64()),
        event_time: js::finite(e.get("et")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn entry() -> Arc<Value> {
        Arc::new(json!({
            "position": 5, "kind": "push", "queue": "orders", "partition": "c1", "partitionId": "p1",
            "offset": 1, "id": "m1", "transactionId": "t1", "data": { "n": 1 }, "ts": "2026-11-06T09:00:00.000Z",
            "createdAt": "2026-11-06T09:00:00.000Z"
        }))
    }

    #[test]
    fn a_held_envelope_packs_only_what_differs() {
        let env = Env::of_entry(entry());
        let p = pack(&env);
        assert_eq!(p["vd"], json!(1));
        assert!(p.get("v").is_none());
        let back = unpack(&p);
        assert_eq!(back.value(), Some(&json!({ "n": 1 })));
        assert_eq!(back.key, "c1");

        let mapped = env.with_value(Some(json!({ "x": 2 })));
        let p = pack(&mapped);
        assert_eq!(p["v"], json!({ "x": 2 }));
        assert_eq!(unpack(&p).value(), Some(&json!({ "x": 2 })));

        let undefined = env.with_value(None);
        let p = pack(&undefined);
        assert!(p.get("v").is_none() && p.get("vd").is_none());
        assert_eq!(unpack(&p).value(), None);
    }

    #[test]
    fn a_result_is_seen_in_the_feeds_shape() {
        let env = Env {
            msg: Arc::new(json!({ "count": 2 })),
            key: "k".into(),
            value: EnvValue::Msg,
            ctx: Some(json!({ "kind": "tumbling" })),
            derived: true,
            position: None,
            event_time: Some(59_999.0),
        };
        let m = message_of(&env, 60_000.0);
        assert_eq!(m["partition"], json!("k"));
        assert_eq!(m["queue"], Value::Null);
        assert_eq!(m["data"], json!({ "count": 2 }));
        assert_eq!(m["ts"], json!("1970-01-01T00:01:00.000Z"));
        let snap = snapshot_of(&m, &None);
        assert_eq!(snap["derived"], json!(true));
        assert_eq!(snap["key"], json!("k"));
        assert_eq!(snap["createdAt"], json!("1970-01-01T00:01:00.000Z"));
        assert!(snap.get("partitionId").is_none());
        let p = pack(&env);
        assert_eq!(p["vm"], json!(1));
        assert_eq!(p["d"], json!(1));
        assert_eq!(p["et"], json!(59_999));
    }
}
