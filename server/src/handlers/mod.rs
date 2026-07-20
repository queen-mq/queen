use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use deadpool_postgres::Pool;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::db;
use crate::frames::{
    pack_frames, unpack_frames, uuid_bytes_to_string, uuid_string_to_bytes, zstd_compress,
    zstd_decompress, FrameIn,
};
use crate::fusion::{json_escape_into, AddMsg, Fusion, ItemResult, OwnedFrame, PushState};
use crate::metrics::Metrics;
use crate::util::uuidv7_bytes;
use crate::vegas::Vegas;

pub struct AppState {
    pub pool: Pool,
    pub fusion: Arc<Fusion>,
    pub push_vegas: Arc<Vegas>,
    pub pop_vegas: Arc<Vegas>,
    pub metrics: Arc<Metrics>,
    pub stmt_timeout: Duration,
    pub pop_default_timeout_ms: u64,
    pub pop_wait_poll_ms: u64,
    // zstd level for broker-packed segments on the transaction push path (the
    // fusion path carries its own copy).
    pub zstd_level: i32,
    // Per-queue configured lease time (seconds), read from queen.seg_queues on
    // first use. No invalidation for now (queue-config invalidation is a later
    // slice); a reconfigure of leaseTime is not reflected until restart.
    pub lease_cache: Mutex<HashMap<String, i32>>,
    // System maintenance flags, mirrored to queen.system_state (the SAME
    // {"enabled":..} rows the C++ SharedStateManager uses). `maintenance` is
    // reported only — the segments broker has no file buffer, so it does not
    // divert pushes and bufferedMessages is always 0. `pop_maintenance` pauses
    // pops (handle_pop / handle_pop_partition early-return {messages:[],paused:true}).
    pub maintenance: AtomicBool,
    pub pop_maintenance: AtomicBool,
    // Long-poll waker + inter-instance notifier. A local push wakes locally-parked
    // pops through this, and (when a UDP transport is attached) fans MESSAGE_AVAILABLE
    // / maintenance / queue-config changes out to peer replicas. With no peers it is
    // a pure in-process waker — no packets, behaviour otherwise unchanged.
    pub notifier: Arc<crate::notify::Notifier>,
}

const DEFAULT_LEASE_SECONDS: i32 = 300;

impl AppState {
    // Resolve the queue's lease time, caching the lookup. Falls back to
    // DEFAULT_LEASE_SECONDS when the queue has no seg_queues row yet or the DB
    // is unreachable. The std Mutex guard is always dropped before the .await.
    async fn lease_time_for(&self, queue: &str) -> i32 {
        if let Some(v) = self.lease_cache.lock().unwrap().get(queue).copied() {
            return v;
        }
        let v = match self.pool.get().await {
            Ok(c) => db::queue_lease_time(&c, queue)
                .await
                .ok()
                .flatten()
                .unwrap_or(DEFAULT_LEASE_SECONDS),
            Err(_) => DEFAULT_LEASE_SECONDS,
        };
        self.lease_cache.lock().unwrap().insert(queue.to_string(), v);
        v
    }
}

pub(crate) fn json(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

// Build a VALID JSON error body: {"error":"<prefix><escaped message>"}. The
// message is JSON-escaped (via json_escape_into) while the STRUCTURAL quotes are
// left intact, so clients receive parseable JSON carrying the real error text.
// Replaces the old format-then-blanket-quote-replace pattern, whose replacement
// of every quote mangled the structural quotes into invalid `{'error':'..'}`
// (which is what hid the real DB error on the streams path).
pub(crate) fn json_err(prefix: &str, e: impl std::fmt::Display) -> String {
    let mut out = String::from("{\"error\":\"");
    out.push_str(prefix);
    json_escape_into(&mut out, &e.to_string());
    out.push_str("\"}");
    out
}


mod data;
mod queues;
mod messages;
mod traces;
mod status;
mod consumer_groups;
mod maintenance;
mod streams;
mod static_files;

pub use data::*;
pub use queues::*;
pub use messages::*;
pub use traces::*;
pub use status::*;
pub use consumer_groups::*;
pub use maintenance::*;
pub use streams::*;
pub use static_files::*;

pub(crate) fn status_is_ok(s: Option<&str>) -> bool {
    // The JS/Go/etc clients send "completed" for success and "failed" for a nack.
    // Absent status defaults to success (a bare completion).
    match s {
        Some(v) => matches!(v, "completed" | "success" | "acked" | "ok"),
        None => true,
    }
}

pub(crate) fn text_plain(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "text/plain; version=0.0.4")], body).into_response()
}

// Collect the given query keys into a JSON filter object, keeping only
// non-empty string values.
pub(crate) fn filters_from_query(
    params: &HashMap<String, String>,
    keys: &[&str],
) -> serde_json::Map<String, serde_json::Value> {
    let mut m = serde_json::Map::new();
    for &k in keys {
        if let Some(v) = params.get(k) {
            if !v.is_empty() {
                m.insert(k.to_string(), serde_json::Value::String(v.clone()));
            }
        }
    }
    m
}

pub(crate) fn qint(params: &HashMap<String, String>, key: &str, def: i32) -> i32 {
    params.get(key).and_then(|v| v.parse::<i32>().ok()).unwrap_or(def)
}

pub(crate) fn qbool(params: &HashMap<String, String>, key: &str, def: bool) -> bool {
    match params.get(key).map(|s| s.as_str()) {
        Some("false" | "0" | "no") => false,
        Some("true" | "1" | "yes") => true,
        _ => def,
    }
}

