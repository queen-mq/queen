//! `/status`, `/health` and the metrics endpoints.
#![allow(unused_imports)]
use super::*;

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::StatusCode;
use axum::response::Response;

/// `GET /status` — liveness plus, when the in-process Kafka facade is enabled,
/// its state under `kafka`, and when the in-process S3 sink is, its state under
/// `s3` (`s3_inproc::status_value`: the bucket, the health verdict, a row per
/// queue). With neither on the body is the fixed string.
pub async fn handle_status() -> Response {
    #[cfg(feature = "kafka")]
    let kafka = crate::kafka_inproc::status_value();
    #[cfg(not(feature = "kafka"))]
    let kafka: Option<serde_json::Value> = None;
    #[cfg(feature = "s3")]
    let s3 = crate::s3_inproc::status_value();
    #[cfg(not(feature = "s3"))]
    let s3: Option<serde_json::Value> = None;
    if kafka.is_none() && s3.is_none() {
        return json(
            StatusCode::OK,
            "{\"status\":\"ok\",\"engine\":\"segments-rust\"}".to_string(),
        );
    }
    let mut body = serde_json::json!({
        "status": "ok",
        "engine": "segments-rust",
    });
    if let Some(out) = body.as_object_mut() {
        if let Some(kafka) = kafka {
            out.insert("kafka".into(), kafka);
        }
        if let Some(s3) = s3 {
            out.insert("s3".into(), s3);
        }
    }
    json(StatusCode::OK, body.to_string())
}

/// `GET /metrics` — the JSON metrics snapshot.
pub async fn handle_metrics(State(st): State<Arc<AppState>>) -> Response {
    crate::handlers::raft::handle_metrics(State(st)).await
}

/// `GET /health` — the node's readiness (`handlers::raft::handle_health`).
pub async fn handle_health(State(st): State<Arc<AppState>>) -> Response {
    crate::handlers::raft::handle_health(State(st)).await
}

/// `GET /metrics/prometheus` — the Prometheus text exposition.
pub async fn handle_prometheus(State(st): State<Arc<AppState>>) -> Response {
    crate::handlers::raft::handle_prometheus(State(st)).await
}
