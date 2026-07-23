use std::collections::{HashMap, HashSet};
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
    // ACK REGISTRY fast path (server/src/ack_registry.rs): leasing pops record
    // (worker, batch_end, delivered-batch hash set) here; a later full-batch
    // completed ack resolves to one positional cursor advance instead of the
    // per-ack log_ack_by_hash_v1 hash resolution. Any miss falls through to the
    // unchanged SQL ack path — the registry is an optimization, never authority.
    pub ack_registry: Arc<crate::ack_registry::AckRegistry>,
    pub push_vegas: Arc<Vegas>,
    pub pop_vegas: Arc<Vegas>,
    pub metrics: Arc<Metrics>,
    pub stmt_timeout: Duration,
    pub pop_default_timeout_ms: u64,
    // RUSTFIX item 19: exponential-backoff knobs for the long-poll re-query interval.
    pub pop_wait_initial_interval_ms: u64,
    pub pop_wait_backoff_threshold: u32,
    pub pop_wait_backoff_multiplier: f64,
    pub pop_wait_max_interval_ms: u64,
    // zstd level for broker-packed segments on the transaction push path (the
    // fusion path carries its own copy).
    pub zstd_level: i32,
    // Per-queue configured lease time (seconds), read from queen.seg_queues on
    // first use. No invalidation for now (queue-config invalidation is a later
    // slice); a reconfigure of leaseTime is not reflected until restart.
    pub lease_cache: Mutex<HashMap<String, i32>>,
    // RUSTFIX item 8: at-rest payload encryption. `encryption` holds the key (or is
    // disabled); `enc_cache` memoizes each queue's encryption_enabled flag (same
    // lazy-fetch + UDP-invalidation lifecycle as lease_cache).
    pub encryption: Arc<crate::encryption::Encryption>,
    pub enc_cache: Mutex<HashMap<String, bool>>,
    // System maintenance flags, mirrored to queen.system_state (the SAME
    // {"enabled":..} rows the C++ SharedStateManager uses). When `maintenance` is
    // on, pushes are diverted to the file buffer (RUSTFIX item 17) and reported
    // status:"buffered", exactly like C++ — nothing reaches queen.seg_segments
    // until maintenance is disabled and the buffer drains. `pop_maintenance` pauses
    // pops (handle_pop / handle_pop_partition early-return {messages:[],paused:true}).
    pub maintenance: AtomicBool,
    pub pop_maintenance: AtomicBool,
    // Long-poll waker + inter-instance notifier. A local push wakes locally-parked
    // pops through this, and (when a UDP transport is attached) fans MESSAGE_AVAILABLE
    // / maintenance / queue-config changes out to peer replicas. With no peers it is
    // a pure in-process waker — no packets, behaviour otherwise unchanged.
    pub notifier: Arc<crate::notify::Notifier>,
    // Disk spool for DB-outage durability (RUSTFIX item 1) and maintenance-mode
    // buffering (item 17). Failed pushes and maintenance-diverted pushes are
    // appended here and replayed to the DB by the background drain loop.
    pub file_buffer: Arc<crate::file_buffer::FileBufferManager>,
    // Partition-id -> queue-name memo, used to attribute the partitionId-keyed
    // ack wire to a queue for the per-queue ack metrics. Filled by keyed pops
    // (queue known at zero cost) and lazily by a DB lookup on an ack-first miss.
    // The mapping is immutable (a partition never changes queue), so entries
    // never go stale; the map is only size-capped.
    pub partition_queue: Mutex<HashMap<String, String>>,
    // Phase 2 first-contact safety (targeted pop). Monotonic positive cache of
    // (queue -> {group}) pairs whose group-first-contact BULK SEED (043
    // log_pop_wildcard_*_v1 / log_pop_discover_wire_v1) is known committed — i.e.
    // the single-row consumer_groups_metadata marker exists. Until a (queue,
    // group) is seeded, a woken pop's hint-driven targeted single-partition pop
    // (db::pop_specific -> queen.log_pop_v1) is SUPPRESSED and the pop falls
    // through to the wildcard backstop, which CARRIES the seed. This restores the
    // invariant the anti-convoy fix (51e50c4) relies on: no per-partition lazy
    // first-contact INSERT storm before the set-based seed has run — the merge
    // regression that wedged the broker on Lock:transactionid at t=0. Nested map
    // so the steady-state hit borrows queue+group with no allocation; cleared
    // alongside the other per-queue caches by reconcile so a delete+recreate
    // self-heals within one interval.
    pub seeded_groups: Mutex<HashMap<String, HashSet<String>>>,
}

const PARTITION_QUEUE_CACHE_CAP: usize = 100_000;

// RUSTFIX item 18: fallback lease when a queue has no seg_queues row / DB is
// unreachable — the "60" floor of COALESCE(request, queue.lease_time, 60).
const DEFAULT_LEASE_SECONDS: i32 = 60;

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

    // RUSTFIX item 8: resolve the queue's encryption_enabled flag, caching the
    // lookup (same shape as lease_time_for). False when the queue has no
    // queen.queues row yet or the DB is unreachable. Guard dropped before .await.
    pub(crate) async fn encryption_enabled_for(&self, queue: &str) -> bool {
        if let Some(v) = self.enc_cache.lock().unwrap().get(queue).copied() {
            return v;
        }
        let v = match self.pool.get().await {
            Ok(c) => db::queue_encryption_enabled(&c, queue)
                .await
                .ok()
                .flatten()
                .unwrap_or(false),
            Err(_) => false,
        };
        self.enc_cache.lock().unwrap().insert(queue.to_string(), v);
        v
    }

    // Phase 2 first-contact safety: has this (queue, group)'s group-first-contact
    // bulk seed committed (043)? Fast path = the monotonic positive cache (zero
    // DB, no allocation on a hit). On a miss, ONE indexed marker lookup
    // (db::group_seed_marker_exists); a positive result is cached so the
    // steady-state targeted pop path never reads again. A pool/DB error, or an
    // absent marker, returns false — the caller then uses the wildcard backstop,
    // which is always first-contact-safe. This NEVER returns true before the seed
    // exists, which is the whole point: it gates hint-driven targeted pops until
    // the convoy-preventing seed is in place. Guard is dropped before every await.
    pub(crate) async fn group_seeded(&self, queue: &str, group: &str) -> bool {
        {
            let g = self.seeded_groups.lock().unwrap();
            if let Some(gs) = g.get(queue) {
                if gs.contains(group) {
                    return true;
                }
            }
        }
        let seeded = match self.pool.get().await {
            Ok(c) => db::group_seed_marker_exists(&c, queue, group)
                .await
                .unwrap_or(false),
            Err(_) => false,
        };
        if seeded {
            self.seeded_groups
                .lock()
                .unwrap()
                .entry(queue.to_string())
                .or_default()
                .insert(group.to_string());
        }
        seeded
    }

    // Record a partition -> queue mapping learned from a pop response.
    pub(crate) fn remember_partition_queue(&self, partition_id: &str, queue: &str) {
        if partition_id.is_empty() || queue.is_empty() {
            return;
        }
        let mut m = self.partition_queue.lock().unwrap();
        if m.len() >= PARTITION_QUEUE_CACHE_CAP {
            m.clear(); // rare, cheap reset; repopulates from live traffic
        }
        m.entry(partition_id.to_string()).or_insert_with(|| queue.to_string());
    }

    // Resolve a partition id to its queue name for ack attribution: memo first,
    // then one DB lookup on miss. None when the partition is unknown (deleted /
    // rows-engine id) — the ack still succeeds, it just goes unattributed.
    pub(crate) async fn queue_for_partition(
        &self,
        client: &deadpool_postgres::Client,
        partition_id: &str,
    ) -> Option<String> {
        if let Some(q) = self.partition_queue.lock().unwrap().get(partition_id).cloned() {
            return Some(q);
        }
        match db::partition_queue_name(client, partition_id).await {
            Ok(Some(q)) => {
                self.remember_partition_queue(partition_id, &q);
                Some(q)
            }
            _ => None,
        }
    }

    // RUSTFIX item 19: the next long-poll re-query interval for a given consecutive
    // empty-wait count (C++ _set_next_backoff_time, lib/queen.hpp:2281-2296). Below
    // the threshold it is the initial interval; above it grows
    // initial*count*multiplier, clamped to max. A push-wake resets `count` to 0.
    pub(crate) fn pop_backoff_interval(&self, backoff_count: u32) -> Duration {
        let ms = if backoff_count > self.pop_wait_backoff_threshold {
            let v = (self.pop_wait_initial_interval_ms as f64)
                * (backoff_count as f64)
                * self.pop_wait_backoff_multiplier;
            (v as u64).min(self.pop_wait_max_interval_ms)
        } else {
            self.pop_wait_initial_interval_ms
        };
        Duration::from_millis(ms.max(1))
    }
}

pub(crate) fn json(status: StatusCode, body: String) -> Response {
    // RFC 9110 §15.3.5: 204 responses MUST NOT carry content. Announcing a
    // content-length on a body hyper then elides makes strict HTTP/1.1 clients
    // (Node's undici/llhttp) treat the connection as poisoned and drop it —
    // under empty-poll load that snowballed into ECONNRESET storms. The empty
    // pop/maintenance bodies carried no information the clients read (they all
    // early-return on status 204), so drop the body, keep the status.
    if status == StatusCode::NO_CONTENT {
        return StatusCode::NO_CONTENT.into_response();
    }
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

// RUSTFIX item 25: map an SP result carrying an embedded {"error":...} to the
// right HTTP status (parity with every C++ submit_sp_call): 404 when the error
// text contains "not found" (case-insensitive), else 500; otherwise 200. The
// original body bytes are preserved so the client-visible error text is unchanged.
pub(crate) fn sp_result_to_response(txt: String) -> Response {
    match serde_json::from_str::<serde_json::Value>(&txt) {
        Ok(v) => {
            if let Some(err) = v.get("error").filter(|e| !e.is_null()) {
                let msg = err.as_str().unwrap_or("").to_ascii_lowercase();
                let code = if msg.contains("not found") {
                    StatusCode::NOT_FOUND
                } else {
                    StatusCode::INTERNAL_SERVER_ERROR
                };
                return json(code, txt);
            }
            json(StatusCode::OK, txt)
        }
        // Not JSON (SPs always return JSON; a parse failure means a raw body already).
        Err(_) => json(StatusCode::OK, txt),
    }
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
mod analytics;
mod migration;

pub use data::*;
pub use queues::*;
pub use messages::*;
pub use traces::*;
pub use status::*;
pub use consumer_groups::*;
pub use maintenance::*;
pub use streams::*;
pub use static_files::*;
pub use analytics::*;
pub use migration::*;

pub(crate) fn status_is_ok(s: Option<&str>) -> bool {
    // The JS/Go/etc clients send "completed" for success and "failed" for a nack.
    // Absent status defaults to success (a bare completion).
    match s {
        Some(v) => matches!(v, "completed" | "success" | "acked" | "ok"),
        None => true,
    }
}

// RUSTFIX item 10: normalize a client ack status to the four v0.16.0 outcomes the
// segment ack SP branches on. `retry` and `dlq` must survive to SQL (the old code
// collapsed everything to a bool). Absent / unrecognized => completed / failed
// respectively (matching status_is_ok's completed set; anything else is a nack).
pub(crate) fn normalize_ack_status(s: Option<&str>) -> &'static str {
    match s {
        None => "completed",
        Some("completed") | Some("success") | Some("acked") | Some("ok") => "completed",
        Some("retry") => "retry",
        Some("dlq") => "dlq",
        Some(_) => "failed",
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

