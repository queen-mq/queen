#![allow(unused_imports)]
use super::*;

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

// ------------------------------------------------------------ POST /api/v1/traces
// Engine-agnostic (queen.message_traces). Mirrors the C++ traces route: require
// transactionId/partitionId/data, default consumerGroup/eventType, pass through
// traceNames. record_trace_v1 is segment-aware via 030 (resolves message_id from
// seg_dedup, records with NULL message_id past the dedup window).
pub async fn handle_record_trace(State(st): State<Arc<AppState>>, body: Bytes) -> Response {
    let body_v: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };

    let txn = body_v.get("transactionId").and_then(|x| x.as_str()).filter(|s| !s.is_empty());
    let pid = body_v.get("partitionId").and_then(|x| x.as_str()).filter(|s| !s.is_empty());
    let (txn, pid) = match (txn, pid) {
        (Some(t), Some(p)) => (t, p),
        _ => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"transactionId and partitionId are required\"}".to_string(),
            )
        }
    };
    let data = match body_v.get("data") {
        Some(d) => d.clone(),
        None => return json(StatusCode::BAD_REQUEST, "{\"error\":\"data is required\"}".to_string()),
    };

    let mut sp = serde_json::Map::new();
    sp.insert("transactionId".to_string(), serde_json::Value::String(txn.to_string()));
    sp.insert("partitionId".to_string(), serde_json::Value::String(pid.to_string()));
    sp.insert(
        "consumerGroup".to_string(),
        serde_json::Value::String(
            body_v.get("consumerGroup").and_then(|x| x.as_str()).unwrap_or("__QUEUE_MODE__").to_string(),
        ),
    );
    sp.insert(
        "eventType".to_string(),
        serde_json::Value::String(
            body_v.get("eventType").and_then(|x| x.as_str()).unwrap_or("info").to_string(),
        ),
    );
    sp.insert("data".to_string(), data);
    sp.insert("workerId".to_string(), serde_json::Value::String("seg-rust".to_string()));
    if let Some(tn) = body_v.get("traceNames") {
        if tn.is_array() {
            sp.insert("traceNames".to_string(), tn.clone());
        } else if let Some(s) = tn.as_str() {
            sp.insert("traceNames".to_string(), serde_json::json!([s]));
        }
    }
    let sp_json = serde_json::Value::Object(sp).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::record_trace(&client, &sp_json).await {
        Ok(txt) => {
            let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
            let ok = v.get("success").and_then(|x| x.as_bool()).unwrap_or(true)
                && v.get("error").map(|e| e.is_null()).unwrap_or(true);
            json(if ok { StatusCode::CREATED } else { StatusCode::INTERNAL_SERVER_ERROR }, txt)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("trace failed: ", &e),
        ),
    }
}

// -------------------------------- GET /api/v1/traces/:partitionId/:transactionId
pub async fn handle_message_traces(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((partition_id, transaction_id)): Path<(String, String)>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    // Track B (§5) OWNERSHIP GATE (pid-addressed): a foreign pid's traces must not
    // leak — return the same empty set as a message with no traces (no-op when off).
    if !st.tenant_owns_partition(&client, &partition_id, tenant.as_str()).await {
        return json(StatusCode::OK, "{\"traces\":[]}".to_string());
    }
    match db::get_message_traces(&client, &partition_id, &transaction_id).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("traces failed: ", &e),
        ),
    }
}

// ------------------------------------------ GET /api/v1/traces/by-name/:traceName
pub async fn handle_traces_by_name(
    State(st): State<Arc<AppState>>,
    Path(trace_name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let limit = qint(&params, "limit", 100);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_traces_by_name(&client, &trace_name, limit, offset).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("traces failed: ", &e),
        ),
    }
}

// ------------------------------------------------------- GET /api/v1/traces/names
pub async fn handle_trace_names(
    State(st): State<Arc<AppState>>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let limit = qint(&params, "limit", 50);
    let offset = qint(&params, "offset", 0);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_trace_names(&client, limit, offset).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("trace names failed: ", &e),
        ),
    }
}

