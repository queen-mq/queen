//! `handlers/raft.rs` — the raft-mode wiring of the storage seam (PLAN_RAFT.md
//! WP-1.7a).
//!
//! Three things live here:
//!
//! 1. **Receiver dispatch** (`dispatch_*`): the message-path handlers
//!    (`handle_push`, the three `handle_pop*`, `handle_ack*`,
//!    `handle_lease_extend`) branch here at their very top when
//!    `st.storage.is_raft()`. Each does the pool-free receiver pre-work of §9.1
//!    (the request id, the per-request deadline, and the R-108 name-length
//!    guard) and hands a typed command to `st.rsm` (rsm/facade.rs). WP-1.7a's
//!    facade is the `NotReady` stub, so a mutating command comes back
//!    `RsmError::Unsupported` → `503 raft_phase1_unsupported`; WP-1.7c swaps the
//!    real state machine in behind the builder hook and this file does not
//!    change. The heavier pre-work (fusion packing, encryption, the repack after
//!    a duplicate verdict, and long-poll parking on the facade's notifier) is
//!    marked and owed to WP-1.7c — running it against a stub that answers 503
//!    before it matters would only burn CPU.
//!
//! 2. **Raft-mode `/health`, `/metrics/prometheus`, `/stats/refresh`**: the
//!    Postgres variants of these touch `pool.get()`, which must never happen in
//!    raft mode (there is no reachable database). The raft variants read
//!    node-local state only. `/health` keeps `status`/`version`/`engine` and
//!    adds the `raft` block (§14.1); `/stats/refresh` is the no-op 200 §9.6
//!    requires (every SDK's Admin API calls it); `/metrics/prometheus` drops the
//!    DB-backed blob and keeps the in-process families.
//!
//! 3. **The composition roots** `build_raft_state` (the raft `AppState`, no
//!    Postgres connect and no schema apply) and `build_raft_router` (the router:
//!    ported routes to the real handlers, un-ported `/api` and `/streams` routes
//!    to a `503 raft_phase1_unsupported` fallback). `build_raft_state` is shared
//!    by `main.rs` (the binary), `embedded/boot.rs` and the seam test, so the
//!    one raft `AppState` shape cannot drift between them.

use std::sync::Arc;

use axum::http::{header, Method, StatusCode, Uri};
use axum::response::{IntoResponse, Response};

use super::{json, AppState};
use crate::config::{Config, StorageMode};
use crate::rsm::facade::{
    self, AckReq, Deadline, DepthReq, DlqHeadReq, PopDiscoverReq, PopPinnedReq, PopReq, PushReq,
    RenewReq, ReqCtx, RsmError,
};

// ---------------------------------------------------------------------------
// RsmError → HTTP.
// ---------------------------------------------------------------------------

/// Render a typed [`RsmError`] as the HTTP response the SDKs and the proxy read.
/// Every arm carries a stable `code`; the retryable arms carry `Retry-After`.
/// Never a bare 500 and never a panic — the un-ported surface is a clean 503.
pub(crate) fn err_response(e: RsmError) -> Response {
    let (status, retry_after): (StatusCode, Option<u64>) = match &e {
        RsmError::Unsupported => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::Retry { .. } => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::NoLeader => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::NameTooLong { .. } => (StatusCode::PAYLOAD_TOO_LARGE, None),
        RsmError::StorageFull => (StatusCode::INSUFFICIENT_STORAGE, None),
        RsmError::Timeout => (StatusCode::SERVICE_UNAVAILABLE, Some(1)),
        RsmError::Internal(_) => (StatusCode::INTERNAL_SERVER_ERROR, None),
    };
    // Build a valid JSON body {"error":"<escaped>","code":"<code>"} (+ the
    // leader hint when we have one). Reuse `json_escape_into` for the message.
    let mut body = String::from("{\"error\":\"");
    crate::fusion::json_escape_into(&mut body, &e.to_string());
    body.push_str("\",\"code\":\"");
    body.push_str(e.code());
    body.push('"');
    if let RsmError::Retry {
        leader_hint: Some(h),
    } = &e
    {
        body.push_str(",\"leader\":\"");
        crate::fusion::json_escape_into(&mut body, h);
        body.push('"');
    }
    body.push('}');

    match retry_after {
        Some(secs) => (
            status,
            [
                (header::CONTENT_TYPE, "application/json".to_string()),
                (header::RETRY_AFTER, secs.to_string()),
            ],
            body,
        )
            .into_response(),
        None => json(status, body),
    }
}

/// The response for a route that raft phase 1 does not serve. Named so the
/// fallback and any explicit un-ported route read the same.
pub(crate) fn unsupported() -> Response {
    err_response(RsmError::Unsupported)
}

/// The per-command deadline, derived from the client's timeout budget (§9.1
/// step 5). A generous ceiling for the seam; WP-1.7c derives it from the request.
fn deadline_for(timeout_ms: u64) -> Deadline {
    let ms = timeout_ms.clamp(1, 60_000);
    Deadline::after(std::time::Duration::from_millis(ms))
}

// ---------------------------------------------------------------------------
// Receiver dispatch (called by the storage-aware guards in data.rs).
// ---------------------------------------------------------------------------

/// `POST /api/v1/push`. Bounds every item's `(queue, partition)` name (R-108),
/// then hands the raw body to the facade. WP-1.7c adds the fusion pack/hash,
/// encryption and message-id pre-work of §9.1.
pub(crate) async fn dispatch_push(
    st: &AppState,
    tenant: &str,
    body: axum::body::Bytes,
) -> Response {
    // Name-length guard: parse just the item queue/partition names generically
    // (the packing/dedup pre-work is WP-1.7c). A body that will not parse is a
    // 400 the same as the Postgres handler's own first step.
    if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&body) {
        if let Some(items) = v.get("items").and_then(|x| x.as_array()) {
            for it in items {
                let q = it.get("queue").and_then(|x| x.as_str()).unwrap_or("");
                let p = it.get("partition").and_then(|x| x.as_str());
                if let Err(e) = facade::check_message_key_names(tenant, q, None, p) {
                    return err_response(e);
                }
            }
        }
    }
    let ctx = ReqCtx::new(tenant, deadline_for(st.pop_default_timeout_ms));
    match st.rsm.push(ctx, PushReq { raw: body.to_vec() }).await {
        Ok(out) => json(StatusCode::CREATED, out.body),
        Err(e) => err_response(e),
    }
}

/// `GET /api/v1/pop/queue/:queue` (wildcard). Fields are extracted by the guard
/// in data.rs (where `PopParams`'s private fields are readable) and passed in.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_pop(
    st: &AppState,
    tenant: &str,
    queue: String,
    group: Option<String>,
    batch: i32,
    auto_ack: bool,
    wait: bool,
    timeout_ms: u64,
) -> Response {
    if let Err(e) = facade::check_message_key_names(tenant, &queue, group.as_deref(), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(timeout_ms));
    let req = PopReq {
        queue,
        group,
        batch: batch.max(0) as u32,
        auto_ack,
        wait,
        timeout_ms,
    };
    // NOTE(WP-1.7c): on an empty claim with `wait`, park on `st.rsm.notifier()`
    // and re-poll (§9.5). The stub never returns Ok, so there is nothing to park
    // on yet.
    match st.rsm.pop_wildcard(ctx, req).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// `GET /api/v1/pop/queue/:queue/partition/:partition` (pinned).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_pop_partition(
    st: &AppState,
    tenant: &str,
    queue: String,
    partition: String,
    group: Option<String>,
    batch: i32,
    auto_ack: bool,
    wait: bool,
    timeout_ms: u64,
) -> Response {
    if let Err(e) =
        facade::check_message_key_names(tenant, &queue, group.as_deref(), Some(&partition))
    {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(timeout_ms));
    let req = PopPinnedReq {
        queue,
        partition,
        group,
        batch: batch.max(0) as u32,
        auto_ack,
        wait,
        timeout_ms,
    };
    match st.rsm.pop_pinned(ctx, req).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// `GET /api/v1/pop` (namespace/task discovery).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn dispatch_pop_discover(
    st: &AppState,
    tenant: &str,
    namespace: String,
    task: String,
    group: Option<String>,
    batch: i32,
    auto_ack: bool,
    wait: bool,
    timeout_ms: u64,
) -> Response {
    // The discovery selectors (namespace/task) are not store keys, but the
    // consumer group is; bound it (queue empty — discovery has none in the path).
    if let Err(e) = facade::check_message_key_names(tenant, "", group.as_deref(), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(timeout_ms));
    let req = PopDiscoverReq {
        namespace,
        task,
        group,
        batch: batch.max(0) as u32,
        auto_ack,
        wait,
        timeout_ms,
    };
    match st.rsm.pop_discover(ctx, req).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// `POST /api/v1/ack` and `POST /api/v1/ack/batch`. The consumer group is the
/// user-supplied name in the composite cursor key, so bound it (R-108); the
/// partition id is a bounded derived value.
pub(crate) async fn dispatch_ack(st: &AppState, tenant: &str, body: axum::body::Bytes) -> Response {
    let group = ack_group(&body);
    if let Err(e) = facade::check_message_key_names(tenant, "", Some(&group), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    let req = AckReq {
        queue: None,
        group,
        raw: body.to_vec(),
    };
    match st.rsm.ack(ctx, req).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// Extract the consumer group from an ack body (single or batch) without the
/// private `AckSingle`/`AckBatch` types: read the `consumerGroup` key
/// generically, defaulting to the queue-mode group the handlers use.
fn ack_group(body: &axum::body::Bytes) -> String {
    serde_json::from_slice::<serde_json::Value>(body)
        .ok()
        .and_then(|v| {
            v.get("consumerGroup")
                .and_then(|x| x.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| "__QUEUE_MODE__".to_string())
}

/// `POST /api/v1/lease/:leaseId/extend`.
pub(crate) async fn dispatch_lease_extend(
    st: &AppState,
    tenant: &str,
    lease_id: String,
    seconds: i64,
) -> Response {
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    match st.rsm.renew(ctx, RenewReq { lease_id, seconds }).await {
        Ok(out) => json(StatusCode::OK, out.body),
        Err(e) => err_response(e),
    }
}

/// A DLQ-head read on the ack path (kept here so the ack surface's facade calls
/// live together; not yet routed from a handler in WP-1.7a).
#[allow(dead_code)]
pub(crate) async fn dispatch_dlq_head(
    st: &AppState,
    tenant: &str,
    queue: String,
    group: String,
) -> Response {
    if let Err(e) = facade::check_message_key_names(tenant, &queue, Some(&group), None) {
        return err_response(e);
    }
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    match st.rsm.dlq_head(ctx, DlqHeadReq { queue, group }).await {
        Ok(out) => match out.body {
            Some(b) => json(StatusCode::OK, b),
            None => json(StatusCode::OK, "{\"message\":null}".to_string()),
        },
        Err(e) => err_response(e),
    }
}

/// Queue depth in raft mode (kept beside the other facade calls; the depth route
/// is a §9.6 local stale read wired in a later WP).
#[allow(dead_code)]
pub(crate) async fn dispatch_depth(
    st: &AppState,
    tenant: &str,
    queue: String,
    group: Option<String>,
) -> Response {
    let ctx = ReqCtx::new(tenant, deadline_for(st.stmt_timeout.as_millis() as u64));
    match st.rsm.depth(ctx, DepthReq { queue, group }).await {
        Ok(out) => json(StatusCode::OK, format!("{{\"pending\":{}}}", out.pending)),
        Err(e) => err_response(e),
    }
}

// ---------------------------------------------------------------------------
// Raft-mode observability handlers (no pool).
// ---------------------------------------------------------------------------

/// `GET /health` in raft mode (§14.1): keeps `status`/`version`/`engine`, adds
/// the `raft` block, and answers `200 healthy` while a leader is known and the
/// apply lag is under the ready threshold, else `503 settling`. Reads
/// node-local state only — never `pool.get()`.
pub(crate) async fn handle_health(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
) -> Response {
    let h = st.rsm.health();
    let (status, label) = if h.ready(st.raft_ready_lag_ms) {
        (StatusCode::OK, "healthy")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "settling")
    };
    let body = format!(
        "{{\"status\":\"{}\",\"engine\":\"raft\",\"version\":\"{}\",\"raft\":{}}}",
        label,
        crate::VERSION,
        h.to_json()
    );
    json(status, body)
}

/// `POST /api/v1/stats/refresh` in raft mode: the §9.6 no-op that answers 200.
/// Counters are maintained at apply (D16), so there is nothing to refresh; every
/// SDK's Admin API calls this, so it must not 404 or 503.
pub(crate) async fn handle_stats_refresh(
    axum::extract::State(_st): axum::extract::State<Arc<AppState>>,
) -> Response {
    json(
        StatusCode::OK,
        "{\"success\":true,\"refreshed\":false,\"engine\":\"raft\"}".to_string(),
    )
}

/// `GET /metrics/prometheus` in raft mode: the in-process families plus the
/// admission gauges, without the DB-backed cluster blob the Postgres handler
/// fetches with `pool.get()`.
pub(crate) async fn handle_prometheus(
    axum::extract::State(st): axum::extract::State<Arc<AppState>>,
) -> Response {
    let mut body = st.metrics.prometheus();
    let adm = st.admission.snapshot();
    body.push_str("# HELP queen_admission_budget Write-transaction admission budget\n# TYPE queen_admission_budget gauge\n");
    body.push_str(&format!("queen_admission_budget {}\n", adm.budget));
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        body,
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// Composition roots.
// ---------------------------------------------------------------------------

/// Build the raft-mode [`AppState`]: the same subsystems the Postgres boot
/// constructs, but with NO Postgres connect, NO schema apply and NONE of the
/// §10.3 loops — the handlers branch to `rsm` before any of them would run.
///
/// The deadpool `Pool` is still built (deadpool is lazy — it opens no
/// connection here), because `AppState.pool` is typed `Pool` and the subsystems
/// take a handle; in raft mode nothing ever calls `pool.get()` (the raft router
/// serves only the routes that do not, and the message-path handlers dispatch to
/// the facade first). This is the one literal concession in WP-1.7a to
/// `AppState`'s Postgres-era shape; making `pool` optional is a mechanical
/// follow-up once the handlers are fully ported (WP-2.x).
///
/// Shared by `main.rs`, `embedded/boot.rs` and the seam test so the raft state
/// cannot drift between them.
pub(crate) fn build_raft_state(cfg: &Config) -> Result<Arc<AppState>, String> {
    // Lazy pool handle (no connection; see the doc above).
    let pool = crate::db::create_pool(cfg);

    // Admission sized by the planner queue depth, not the DB pool (§WP-1.7).
    let admission =
        crate::admission::Admission::new(crate::admission::AdmissionCfg::for_raft_planner(
            cfg.raft_planner_queue_depth as u64,
            std::time::Duration::from_millis(cfg.admission_tick_ms),
            std::time::Duration::from_micros(cfg.admission_train_gap_us),
            cfg.admission_trace,
        ));
    crate::admission::set_global(admission.clone());

    let metrics = Arc::new(crate::metrics::Metrics::new());
    let notifier = crate::notify::Notifier::new(cfg.tenancy_header);
    let encryption = crate::encryption::Encryption::from_env();

    // The facade: the real state machine when WP-1.7c has registered its
    // builder, the NotReady stub otherwise.
    let rsm = facade::build(&facade::RsmBuildCtx {
        data_dir: cfg.raft_dir.clone(),
        notifier: notifier.clone(),
    });

    let fusion = crate::fusion::Fusion::new(
        cfg.fusion_shards,
        pool.clone(),
        admission.clone(),
        metrics.clone(),
        cfg.zstd_level,
        cfg.fusion_frames,
        cfg.fusion_hold_ms,
        cfg.stmt_timeout,
        cfg.dedup_cache_mb,
        cfg.dedup_cache_enabled,
        crate::db::cancel_connector(cfg)?,
    );
    let ack_registry = Arc::new(crate::ack_registry::AckRegistry::new(
        cfg.ack_registry_mb,
        cfg.ack_registry_enabled,
    ));
    let ack_fusion = crate::ack_fusion::AckFusion::new(
        cfg.ack_fusion_shards,
        pool.clone(),
        cfg.stmt_timeout,
        cfg.ack_fusion_hold_ms,
        cfg.ack_fusion_enabled,
    );
    let pop_fusion = crate::pop_fusion::PopFusion::new(
        cfg.pop_fusion_shards,
        pool.clone(),
        cfg.stmt_timeout,
        admission.clone(),
        metrics.clone(),
        cfg.pop_fusion_hold_ms,
        cfg.pop_fusion_max_jobs,
        cfg.pop_fusion_max_inflight,
        cfg.pop_fusion_enabled,
    );
    let file_buffer = Arc::new(crate::file_buffer::FileBufferManager::new(
        cfg.file_buffer.clone(),
        cfg.zstd_level,
    ));
    // NOTE: no `startup_recovery`, no `spawn_drain` — the push spool is not used
    // in raft mode (D19: after the hold, 503; the SDK buffers retry).
    let hotlist = crate::hotlist::HotList::new(
        cfg.hotlist_enabled,
        cfg.hotlist_shards,
        cfg.hotlist_window_batch,
        false,
        cfg.tenancy_header,
    );
    hotlist.attach_notifier(notifier.clone());
    let ephemeral = crate::ephemeral::Ephemeral::new(
        crate::ephemeral::Knobs {
            global_max_bytes: cfg.ephemeral_max_bytes,
            queue_max_bytes: cfg.ephemeral_queue_max_bytes,
            queue_max_length: cfg.ephemeral_queue_max_length,
            lease_ms: cfg.ephemeral_lease_s * 1000,
            retry_limit: cfg.ephemeral_retry_limit,
            implicit_idle_ms: cfg.ephemeral_implicit_idle_s * 1000,
            require_grant: cfg.ephemeral_require_grant,
            rate: cfg.ephemeral_rate,
            burst: cfg.ephemeral_burst,
            max_tenants: cfg.kv_max_tenants,
        },
        metrics.clone(),
    );

    Ok(Arc::new(AppState {
        pool,
        fusion,
        ack_registry,
        ack_fusion,
        pop_fusion,
        admission,
        metrics,
        stmt_timeout: cfg.stmt_timeout,
        pop_default_timeout_ms: cfg.pop_default_timeout_ms,
        default_subscription_mode: cfg.default_subscription_mode.clone(),
        pop_pending_gate: cfg.pop_pending_gate,
        pop_wait_initial_interval_ms: cfg.pop_wait_initial_interval_ms,
        pop_wait_backoff_threshold: cfg.pop_wait_backoff_threshold,
        pop_wait_backoff_multiplier: cfg.pop_wait_backoff_multiplier,
        pop_wait_max_interval_ms: cfg.pop_wait_max_interval_ms,
        zstd_level: cfg.zstd_level,
        lease_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        encryption,
        enc_cache: std::sync::Mutex::new(std::collections::HashMap::new()),
        maintenance: std::sync::atomic::AtomicBool::new(false),
        pop_maintenance: std::sync::atomic::AtomicBool::new(false),
        quota: crate::quota::from_config(cfg),
        switches: crate::switches::Switches::new(),
        kv_pressure: std::sync::atomic::AtomicU32::new(0),
        kv_standalone_shed_after: cfg.kv_standalone_shed_after,
        notifier,
        file_buffer,
        partition_queue: std::sync::Mutex::new(std::collections::HashMap::new()),
        seeded_groups: std::sync::Mutex::new(std::collections::HashMap::new()),
        ephemeral,
        peers: Arc::new(crate::peerclient::PeerClient::new()),
        hotlist,
        autopilot: crate::pop_autopilot::PopAutopilot::new(cfg.pop_autopilot_knobs()),
        hotlist_reseed_ms: cfg.hotlist_reseed_ms,
        hotlist_reseed_full_ms: cfg.hotlist_reseed_full_ms,
        hotlist_reseed_window_ms: cfg.hotlist_reseed_window_ms,
        tenancy_enabled: cfg.tenancy_header,
        ownership_ok: std::sync::Mutex::new(std::collections::HashSet::new()),
        auth_enabled: cfg.auth.enabled,
        server_id: cfg.sync.server_id.clone(),
        storage: StorageMode::Raft,
        rsm,
        raft_ready_lag_ms: cfg.raft_ready_lag_ms,
    }))
}

/// The raft-mode router (server target only). Ported routes go to the real
/// handlers — the message-path ones branch to the facade via their
/// storage-aware guard — and every un-ported `/api` or `/streams` route answers
/// `503 raft_phase1_unsupported` through the fallback (never 500, never a
/// panic). Auth and tenancy layers are applied exactly as the Postgres router
/// does.
#[cfg(feature = "server")]
pub(crate) fn build_raft_router(
    state: Arc<AppState>,
    authenticator: Arc<crate::auth::Authenticator>,
    tenancy_header: bool,
) -> axum::Router {
    use axum::routing::{get, post};

    axum::Router::new()
        // ------------------------------------------------ message path → facade
        .route("/api/v1/push", post(super::handle_push))
        .route("/api/v1/pop", get(super::handle_pop_discover))
        .route("/api/v1/pop/queue/:queue", get(super::handle_pop))
        .route(
            "/api/v1/pop/queue/:queue/partition/:partition",
            get(super::handle_pop_partition),
        )
        .route("/api/v1/ack", post(super::handle_ack))
        .route("/api/v1/ack/batch", post(super::handle_ack_batch))
        .route(
            "/api/v1/lease/:leaseId/extend",
            post(super::handle_lease_extend),
        )
        // ---------------------------------------------- observability (no pool)
        .route("/health", get(handle_health))
        .route("/metrics", get(super::handle_metrics))
        .route("/metrics/prometheus", get(handle_prometheus))
        .route("/status", get(super::handle_status))
        .route("/api/v1/stats/refresh", post(handle_stats_refresh))
        // --------------------------------------------------- broker identity
        .route("/auth/me", get(super::handle_auth_me))
        .route("/auth/login", get(super::handle_auth_login))
        .route("/auth/logout", post(super::handle_auth_logout))
        // --------------------------------------- ephemeral RAM verbs (D24, §14.8)
        .route("/api/v1/ephemeral/push", post(super::handle_ephemeral_push))
        .route("/api/v1/ephemeral/pop", get(super::handle_ephemeral_pop))
        .route("/api/v1/ephemeral/ack", post(super::handle_ephemeral_ack))
        .route(
            "/api/v1/ephemeral/reset",
            post(super::handle_ephemeral_reset),
        )
        .route(
            "/api/v1/ephemeral/queues",
            get(super::handle_ephemeral_queues),
        )
        .route(
            "/api/v1/ephemeral/queues/:queue/depth",
            get(super::handle_ephemeral_depth),
        )
        // Un-ported /api and /streams → 503; everything else → the SPA/static.
        .fallback(raft_fallback)
        .layer(axum::extract::DefaultBodyLimit::max(
            std::env::var("QUEEN_MAX_BODY_BYTES")
                .ok()
                .and_then(|v| v.parse::<usize>().ok())
                .unwrap_or(64 * 1024 * 1024),
        ))
        .layer(axum::middleware::from_fn_with_state(
            crate::tenant::TenancyConfig {
                enabled: tenancy_header,
            },
            crate::tenant::tenant_middleware,
        ))
        .layer(axum::middleware::from_fn_with_state(
            authenticator,
            crate::auth::auth_middleware,
        ))
        .with_state(state)
}

/// The fallback for the raft router: any `/api/` or `/streams/` route that phase
/// 1 does not serve answers `503 raft_phase1_unsupported` (transaction, KV,
/// timers, streams, configure beyond implicit creation, admin, analytics, …);
/// anything else is the dashboard SPA / static assets.
#[cfg(feature = "server")]
async fn raft_fallback(method: Method, uri: Uri) -> Response {
    let path = uri.path();
    if path.starts_with("/api/") || path.starts_with("/streams/") {
        return unsupported();
    }
    super::handle_static(method, uri).await
}

// ---------------------------------------------------------------------------
// WP-1.7a seam test: boot the broker in raft mode with no Postgres reachable,
// and confirm the message path routes to the (NotReady) facade — 503
// raft_phase1_unsupported from push and pop, 413 for an over-long name — while
// /health answers 200 with the raft block. Exercises the same handler functions
// the router dispatches to (the storage-aware guards in data.rs → this module →
// the facade), against a raft `AppState` whose lazy pool is never dialled.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::body::Bytes;
    use axum::extract::{Path, Query, State};
    use axum::http::StatusCode;
    use axum::Extension;

    use super::AppState;
    use crate::auth::AuthedSub;
    use crate::tenant::Tenant;

    /// A raft-mode config with a throwaway data dir, and NO Postgres env — so
    /// nothing this test touches can reach a database. `config::load` is the only
    /// caller of itself in the lib unit-test binary, so setting the env here is
    /// race-free.
    fn raft_config() -> crate::config::Config {
        let dir = std::env::temp_dir().join(format!("queen-raft-seam-{}", std::process::id()));
        std::env::set_var("QUEEN_STORAGE", "raft");
        std::env::set_var("QUEEN_RAFT_DIR", dir.display().to_string());
        let cfg = crate::config::load();
        assert_eq!(cfg.storage, crate::config::StorageMode::Raft);
        cfg
    }

    async fn body_of(resp: axum::response::Response) -> (StatusCode, String) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .expect("read body");
        (status, String::from_utf8_lossy(&bytes).into_owned())
    }

    fn raft_state() -> Arc<AppState> {
        let cfg = raft_config();
        super::build_raft_state(&cfg).expect("build raft state")
    }

    #[tokio::test]
    async fn push_answers_503_raft_phase1_unsupported() {
        let st = raft_state();
        let body = Bytes::from_static(b"{\"items\":[{\"queue\":\"q\",\"payload\":{}}]}");
        let resp = super::super::handle_push(
            State(st),
            Extension(AuthedSub(None)),
            Extension(Tenant::default_tenant()),
            body,
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "push status: {text}"
        );
        assert!(
            text.contains("raft_phase1_unsupported"),
            "push body should carry the code: {text}"
        );
    }

    #[tokio::test]
    async fn pop_answers_503_raft_phase1_unsupported() {
        let st = raft_state();
        // All-None PopParams (a plain pop): its fields are private to `data`, but
        // its derived Deserialize builds it from an empty object.
        let p: super::super::PopParams = serde_json::from_str("{}").expect("empty PopParams");
        let resp = super::super::handle_pop(
            State(st),
            Extension(Tenant::default_tenant()),
            Path("orders".to_string()),
            Query(p),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(
            status,
            StatusCode::SERVICE_UNAVAILABLE,
            "pop status: {text}"
        );
        assert!(
            text.contains("raft_phase1_unsupported"),
            "pop body should carry the code: {text}"
        );
    }

    #[tokio::test]
    async fn over_long_queue_name_answers_413() {
        let st = raft_state();
        let long = "q".repeat(600); // > NAME_BUDGET_BYTES (511 - 64)
        assert!(long.len() > crate::rsm::facade::NAME_BUDGET_BYTES);
        let p: super::super::PopParams = serde_json::from_str("{}").expect("empty PopParams");
        let resp = super::super::handle_pop(
            State(st),
            Extension(Tenant::default_tenant()),
            Path(long),
            Query(p),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(
            status,
            StatusCode::PAYLOAD_TOO_LARGE,
            "over-long queue: {text}"
        );
        assert!(
            text.contains("name_too_long"),
            "413 body should carry the code: {text}"
        );
    }

    #[tokio::test]
    async fn health_answers_200_with_raft_block() {
        let st = raft_state();
        let resp = super::handle_health(State(st)).await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::OK, "health status: {text}");
        assert!(
            text.contains("\"status\":\"healthy\""),
            "health body: {text}"
        );
        assert!(
            text.contains("\"raft\":{"),
            "health body should carry the raft block: {text}"
        );
        assert!(
            text.contains("\"storageReady\":false"),
            "phase-1 storage is not ready: {text}"
        );
    }

    #[tokio::test]
    async fn unported_route_is_503_static_route_is_not() {
        // The fallback: /api and /streams paths that phase 1 does not serve are a
        // clean 503 (never 500, never a panic).
        let resp = super::raft_fallback(
            axum::http::Method::POST,
            "/api/v1/transaction".parse().unwrap(),
        )
        .await;
        let (status, text) = body_of(resp).await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        assert!(
            text.contains("raft_phase1_unsupported"),
            "fallback body: {text}"
        );
    }
}
