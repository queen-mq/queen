//! Queue configuration, served by the state machine.
#![allow(unused_imports)]
use super::*;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;

/// `POST /api/v1/configure` — creates or reconfigures a queue.
pub async fn handle_configure(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    return crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "POST",
        "/api/v1/configure",
        None,
        body,
    )
    .await;
}

/// `DELETE /api/v1/resources/queues/:queue` — deletes a queue and its data.
pub async fn handle_delete_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
) -> Response {
    return crate::handlers::raft::dispatch_api(
        &st,
        tenant.as_str(),
        "DELETE",
        &format!("/api/v1/resources/queues/{queue}"),
        None,
        Bytes::new(),
    )
    .await;
}
