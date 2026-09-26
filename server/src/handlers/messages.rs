//! Message and DLQ operations addressed by id, served by the state machine.
#![allow(unused_imports)]
use super::*;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;

/// `DELETE /api/v1/messages/:pid/:txn` — deletes a DLQ snapshot (a live message cannot be deleted: 404).
pub async fn handle_delete_message(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    return crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "DELETE",
        &format!("/api/v1/messages/{partition_id}/{transaction_id}"),
        None,
        Bytes::new(),
    )
    .await;
}

/// `POST /api/v1/dlq/:id/replay` — moves a DLQ row back onto its queue (or the body's override).
pub async fn handle_dlq_replay(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(dlq_id): Path<String>,
    body: Bytes,
) -> Response {
    return crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "POST",
        &format!("/api/v1/dlq/{dlq_id}/replay"),
        None,
        body,
    )
    .await;
}

/// `POST /api/v1/messages/:pid/:txn/retry` — re-queues a dead-lettered message.
pub async fn handle_retry_message(
    State(st): State<Arc<AppState>>,
    // The auth layer stamps this on every request and the embedded facade passes
    // it too. It is unused here on purpose: the move stamps no producer identity
    // on the frame (see `move_dlq_row`), and the extractor stays so the handler
    // signature — which `embedded::Broker::retry_message` calls directly — does
    // not change under the facade.
    Extension(_authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    return crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "POST",
        &format!("/api/v1/messages/{partition_id}/{transaction_id}/retry"),
        None,
        Bytes::new(),
    )
    .await;
}

/// `GET /api/v1/dlq` — lists DLQ rows, filtered by the query string.
pub async fn handle_dlq(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let query = crate::handlers::raft::query_string(&params);
    return crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "GET",
        "/api/v1/dlq",
        Some(&query),
        Bytes::new(),
    )
    .await;
}
