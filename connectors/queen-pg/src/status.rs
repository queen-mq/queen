//! The status cell every connector keeps: a phase, since when, the last error
//! (code, message, when) and a free-form detail object the engine fills.
//! `/status` and `GET /api/v1/connectors` render [`StatusCell::to_json`].

use std::sync::Mutex;

use serde_json::{json, Map, Value};

use crate::error::Error;

#[derive(Debug, Default)]
struct Inner {
    phase: &'static str,
    since_us: i64,
    error: Option<(String, String, i64)>,
    detail: Map<String, Value>,
}

/// Shared, cheap to update, read by the broker's status renderer.
#[derive(Debug, Default)]
pub struct StatusCell {
    inner: Mutex<Inner>,
}

pub fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

impl StatusCell {
    pub fn new(phase: &'static str) -> StatusCell {
        StatusCell {
            inner: Mutex::new(Inner {
                phase,
                since_us: now_us(),
                ..Inner::default()
            }),
        }
    }

    /// Enter `phase` (no-op when already there: `since` keeps its value).
    pub fn set_phase(&self, phase: &'static str) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        if g.phase != phase {
            g.phase = phase;
            g.since_us = now_us();
        }
    }

    pub fn phase(&self) -> &'static str {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).phase
    }

    /// Record `e` as the last error (and keep the phase).
    pub fn set_error(&self, e: &Error) {
        let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        g.error = Some((e.code().to_string(), e.to_string(), now_us()));
    }

    pub fn clear_error(&self) {
        self.inner.lock().unwrap_or_else(|p| p.into_inner()).error = None;
    }

    /// Set one detail field (`lsn`, `slotLagBytes`, `applied`, …).
    pub fn set(&self, key: &str, value: Value) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .detail
            .insert(key.to_string(), value);
    }

    pub fn remove(&self, key: &str) {
        self.inner
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .detail
            .remove(key);
    }

    pub fn to_json(&self) -> Value {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let mut out = Map::new();
        out.insert("phase".into(), Value::String(g.phase.to_string()));
        out.insert(
            "since".into(),
            Value::String(crate::values::iso_utc_micros(g.since_us)),
        );
        out.insert(
            "error".into(),
            match &g.error {
                Some((code, message, at)) => json!({
                    "code": code,
                    "message": message,
                    "at": crate::values::iso_utc_micros(*at),
                }),
                None => Value::Null,
            },
        );
        for (k, v) in &g.detail {
            out.insert(k.clone(), v.clone());
        }
        Value::Object(out)
    }
}
