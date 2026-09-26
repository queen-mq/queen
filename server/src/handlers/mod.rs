use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};

use crate::metrics::Metrics;

/// Everything a request handler reaches: the state machine facade plus the
/// node-local pieces around it (metrics, the ephemeral RAM engine, the
/// KV/timer switches and quotas, the maintenance flags).
pub struct AppState {
    pub metrics: Arc<Metrics>,
    // The request deadline the facade calls run under (`QUEEN_STMT_TIMEOUT_MS`).
    pub stmt_timeout: Duration,
    pub pop_default_timeout_ms: u64,
    /// Effective subscription mode for a grouped pop that sends none (`new` | `all`,
    /// from DEFAULT_SUBSCRIPTION_MODE). Group-less "queue mode" pops ignore this.
    pub default_subscription_mode: String,
    // Long-poll waker: a local push/apply wakes locally-parked pops through this.
    pub notifier: Arc<crate::notify::Notifier>,
    // EPHEMERAL_QUEUES.md §3.2 — the in-RAM queue class. Its whole interaction
    // with the durable engine is the wake gate it shares (`notifier`).
    pub ephemeral: Arc<crate::ephemeral::Ephemeral>,
    // EPHEMERAL_QUEUES.md §3.6 — the pooled broker→broker client the ephemeral
    // verbs relay through. It never opens a connection unless a peer is named.
    pub peers: Arc<crate::peerclient::PeerClient>,
    // Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): native tenant scoping flag
    // (QUEEN_TENANCY_HEADER). Off ⇒ every request is the default tenant.
    pub tenancy_enabled: bool,
    // PLAN_KV_TIMERS §9.3 — the occupancy gate: the measurement this node last
    // read plus its own delta since.
    pub quota: Arc<crate::quota::Quotas>,
    // PLAN_KV_TIMERS §12.1 — the boot flags plus the operator's runtime kill
    // switches, in one place so no caller can check one level and forget the
    // other.
    pub switches: Arc<crate::switches::Switches>,
    // Broker-direct dashboard identity surface (handlers/standalone.rs):
    // whether JWT auth is on decides what /auth/me and /auth/login answer;
    // `server_id` (QUEEN_SERVER_ID → HOSTNAME → random) becomes the synthetic
    // cluster's cell_slug so the SPA's cell-level pages can name this broker.
    pub auth_enabled: bool,
    pub server_id: String,
    // The storage: the replicated state machine facade (rsm/facade).
    pub rsm: std::sync::Arc<dyn crate::rsm::facade::Rsm>,
    // §14.1 — the apply-lag threshold `/health` gates `200 healthy` on
    // (`QUEEN_RAFT_READY_LAG_MS`).
    pub raft_ready_lag_ms: u64,
}

// Track B (PLAN_QUEEN_PROXY_CLOUD.md §5): per-queue scalar caches (lease time,
// encryption flag, group-seed markers) are keyed by queue NAME, which collides
// when two tenants hold the same queue name. Key them by (tenant, name) instead
// so a same-named queue of another tenant can never poison the cache (encryption
// especially — a wrong flag would down/upgrade a tenant's at-rest handling). The
// default tenant when the feature is off ⇒ the key is a stable "<default>\x1f<q>"
// so behaviour is byte-identical to a bare-name key.
#[inline]
pub(crate) fn tenant_queue_key(tenant: &str, queue: &str) -> String {
    let mut k = String::with_capacity(tenant.len() + 1 + queue.len());
    k.push_str(tenant);
    k.push('\u{1f}');
    k.push_str(queue);
    k
}

/// Inverse of `tenant_queue_key`: split a composite key back into (tenant, queue).
/// Used by the metrics collector (syscollect.rs) to attribute per-queue counters
/// to the right tenant at flush time, and by the Prometheus parked gauge. A key
/// without the separator (never produced by `tenant_queue_key`) yields the
/// default tenant + the whole string as the queue, so callers are always safe.
#[inline]
pub(crate) fn split_tenant_queue(key: &str) -> (&str, &str) {
    match key.split_once('\u{1f}') {
        Some((t, q)) => (t, q),
        None => (crate::config::DEFAULT_TENANT, key),
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

// The storage seam wiring: the dispatch helpers the message-path handlers call,
// the RsmError→HTTP mapping, `/health`, `/metrics`, the `/api` + `/streams`
// fallback to the facade, and the AppState + router builders.
pub(crate) mod raft;

mod data;
// PLAN_KV_TIMERS.md §8.1 — the KV and timer HTTP surfaces.
mod kv;
// EPHEMERAL_QUEUES.md §3.3 — the three hot verbs of the RAM-class queues.
mod ephemeral;
#[allow(dead_code)]
mod messages;
#[allow(dead_code)]
mod queues;
mod standalone;
#[allow(dead_code)]
mod status;
mod timers;
// Dashboard SPA assets (rust-embed): HTTP-broker only. The embedded library
// serves no static files, so the module and its rust-embed dependency are
// gated out of default-features = false builds.
#[cfg(feature = "server")]
mod static_files;

pub use data::*;
pub use ephemeral::*;
pub use kv::*;
// The embedded API (src/embedded) calls these; the binary serves the same
// routes through the facade fallback (`handlers::raft::raft_fallback`).
#[allow(unused_imports)]
pub use messages::*;
#[allow(unused_imports)]
pub use queues::*;
pub use standalone::*;
#[cfg(feature = "server")]
pub use static_files::*;
pub use status::*;
pub use timers::*;

pub(crate) fn qint(params: &HashMap<String, String>, key: &str, def: i32) -> i32 {
    params
        .get(key)
        .and_then(|v| v.parse::<i32>().ok())
        .unwrap_or(def)
}

pub(crate) fn qbool(params: &HashMap<String, String>, key: &str, def: bool) -> bool {
    match params.get(key).map(|s| s.as_str()) {
        Some("false" | "0" | "no") => false,
        Some("true" | "1" | "yes") => true,
        _ => def,
    }
}
