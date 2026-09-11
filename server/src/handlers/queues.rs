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

// ------------------------------------------------------------------ configure
// POST /api/v1/configure — create/update a queue. The JS/Go builders send
//   {queue, namespace?, task?, options:{...15 opts...}}
// but raw callers may put the options top-level. We normalize to a single options
// object and run queen.configure_queue_v1, which owns ALL config writes on
// queen.queues (queue identity is the queues id now — there is no second queue
// table to mirror, and dedupWindowSeconds/leaseTime persist from the options
// blob like every other option). The configure_queue_v1 JSON is returned
// verbatim; the JS `configureQueue` test asserts res.configured===true and
// round-trips every options[key], so we MUST NOT reshape it.
//
// `mode` is the one key of the body that is neither the queue nor an option:
// "merge" (the default when it is absent) keeps every option the body does not
// mention, "replace" re-parses the whole configuration from defaults. It is
// translated here into the `replace` flag configure_queue_v1 reads out of the
// options bag — see `configure_replace_flag` for why the flag never travels
// from the caller untouched.
pub async fn handle_configure(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };

    // The C++ configure route only rejects a missing/non-string `queue`; an EMPTY
    // string is a valid queue name (configure_queue_v1 creates a row named ''). The
    // JS client's `.queue('').create()` (load.js::testLoadConsumerGroup) relies on
    // this, so accept "" here rather than filtering it out.
    let queue = match root.get("queue").and_then(|x| x.as_str()) {
        Some(q) => q.to_string(),
        None => {
            return json(StatusCode::BAD_REQUEST, "{\"error\":\"queue is required\"}".to_string())
        }
    };

    // Merge or replace, decided before anything is read out of the body: a
    // rejected `mode` must not configure the queue in the other one's meaning.
    let replace = match configure_replace_flag(&root) {
        Ok(r) => r,
        Err(msg) => {
            return json(
                StatusCode::BAD_REQUEST,
                serde_json::json!({ "error": msg }).to_string(),
            )
        }
    };

    // dedupWindowSeconds travels IN the options blob: configure_queue_v1
    // persists it to queen.queues.dedup_window_seconds (DDL default 3600).
    let opts_json = serde_json::Value::Object(configure_options_bag(&root, replace)).to_string();

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let cfg_txt = match db::configure_queue(&client, &queue, tenant.as_str(), &opts_json).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("configure failed: ", &e),
            )
        }
    };

    // RUSTFIX item 25: if the SP echo carries an {"error":...}, surface it as
    // 500/404 and short-circuit BEFORE the cache invalidations below.
    //
    // ...except for the SP's own OPTION REFUSALS, which carry `invalid` naming
    // the option (012_configure.sql, the two sink-hold bounds). Those are a bad
    // request, not a broken broker: sp_result_to_response would call them a 500,
    // which reaches the operator as an outage toast beside the correct sentence
    // and is metered by the cloud proxy as an unbilled upstream 5xx. The body is
    // returned verbatim either way — only the status differs.
    if cfg_txt.contains("\"error\"") {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&cfg_txt) {
            if v.get("error").filter(|e| !e.is_null()).is_some() {
                if v.get("invalid").and_then(|i| i.as_str()).is_some() {
                    return json(StatusCode::BAD_REQUEST, cfg_txt);
                }
                return sp_result_to_response(cfg_txt);
            }
        }
    }

    // Invalidate the cached lease so a leaseTime change is reflected on next pop,
    // and the cached encryption flag so an encryptionEnabled change is reflected
    // on the next push. Track B (§5): both caches are keyed by (tenant, name) —
    // invalidate this tenant's. The pair is what `invalidate_queue_caches`
    // (main.rs) drops on the PEERS from the frame below, and what the reconcile
    // sweep clears wholesale every QUEEN_CACHE_REFRESH_INTERVAL_MS: dropping
    // only the lease here left the broker that served the call pushing under the
    // old encryption flag for up to a minute while its peers had already
    // switched.
    let qkey = crate::handlers::tenant_queue_key(tenant.as_str(), &queue);
    st.lease_cache.lock().unwrap().remove(&qkey);
    st.enc_cache.lock().unwrap().remove(&qkey);
    // Invalidate the same queue's config cache on peer replicas — the frame carries
    // the tenant, so a peer invalidates exactly this tenant's entry (§5).
    st.notifier.broadcast_queue_config_set(&qkey);

    json(StatusCode::OK, cfg_txt)
}

/// The options bag `configure_queue_v1` is handed, built from the request body
/// and the already-decided `replace` flag.
///
/// Three rules live here, and each one is a way the wrong bag silently
/// misconfigures a queue:
///
///   * the options are the nested `options` object when there is one, and the
///     top-level body minus the routing keys otherwise (raw callers spread them
///     out). `mode` is a routing key like `queue` and `options`: a top-level
///     caller must not have it land in the bag as a 22nd option.
///   * a top-level `namespace` / `task` is folded in, but only as a NON-EMPTY
///     string and only when the bag does not already carry the key — the bag is
///     where a caller can spell `""` or `null`, and that spelling wins.
///   * `replace` is INSERTED last, so a `replace` the caller put in the options
///     bag itself is OVERWRITTEN rather than honoured: `mode` is the single
///     spelling of this decision on the wire and a body cannot carry two that
///     disagree. The SP reads it out of the bag with `->>` and never stores or
///     echoes it — see the parse-section header of 012_configure.sql.
fn configure_options_bag(
    root: &serde_json::Value,
    replace: bool,
) -> serde_json::Map<String, serde_json::Value> {
    let mut opts: serde_json::Map<String, serde_json::Value> =
        match root.get("options").and_then(|o| o.as_object()) {
            Some(o) => o.clone(),
            None => {
                let mut m = root.as_object().cloned().unwrap_or_default();
                m.remove("queue");
                m.remove("options");
                m.remove("mode");
                m
            }
        };
    for key in ["namespace", "task"] {
        if !opts.contains_key(key) {
            if let Some(s) = root
                .get(key)
                .and_then(|x| x.as_str())
                .filter(|s| !s.is_empty())
            {
                opts.insert(key.to_string(), serde_json::Value::String(s.to_string()));
            }
        }
    }
    opts.insert("replace".to_string(), serde_json::Value::Bool(replace));
    opts
}

/// `mode` -> the `replace` flag `configure_queue_v1` reads, or the 400 message.
///
/// Absent is "merge" because that is the safe direction for the bodies already
/// in flight: every SDK sends only the options its caller set, so reading an
/// absent `mode` as "replace" would keep the very reset this feature exists to
/// end. An UNKNOWN value is a 400 rather than a fallback to merge — "mode":
/// "patch" is a caller who believes something about this call, and quietly
/// doing the other thing to their queue's whole configuration is exactly the
/// silent damage the merge rule is about. A non-string `mode` (say `true`) fails
/// the same way, with the same message: the value is what is wrong, not the
/// JSON type.
fn configure_replace_flag(root: &serde_json::Value) -> Result<bool, String> {
    match root.get("mode") {
        None | Some(serde_json::Value::Null) => Ok(false),
        Some(v) => match v.as_str() {
            Some("merge") => Ok(false),
            Some("replace") => Ok(true),
            _ => Err(format!("mode must be \"merge\" or \"replace\", got {v}")),
        },
    }
}

// -------------------------------------------------------------- delete queue
// DELETE /api/v1/resources/queues/:queue — drop the queue. ONE SP call:
// queen.delete_queue_v1 owns the whole delete (the FK-less log_txns/log_dlq
// purge + the cascading queen.queues delete — queue identity is the queues id
// now, so log_partitions and everything under it cascade from that one row).
// Response is the SP JSON at HTTP 200 (a 204 would make the JS client
// return null and fail res.deleted===true), with `deleted` reflecting whether a
// queue was actually removed — see the existed:false guard below.
pub async fn handle_delete_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let del_txt = match db::delete_queue(&client, &queue, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete failed: ", &e),
            )
        }
    };

    // Both caches, for the reason `handle_configure` above states: the frame
    // below makes every PEER run `invalidate_queue_caches` (main.rs), which
    // drops the lease AND the encryption flag, so dropping only the lease here
    // left the broker that served the DELETE as the single node still holding an
    // encryption flag for a queue that no longer exists — until the reconcile
    // sweep cleared it, up to a QUEEN_CACHE_REFRESH_INTERVAL_MS later. It bites
    // on the delete-then-create idiom the SDK cleanups are built on: recreate
    // the queue with encryption off and this broker keeps encrypting its pushes
    // while its peers do not, which is one queue whose messages some consumers
    // cannot read. Same key on both (§5): (tenant, name).
    let qkey = crate::handlers::tenant_queue_key(tenant.as_str(), &queue);
    st.lease_cache.lock().unwrap().remove(&qkey);
    st.enc_cache.lock().unwrap().remove(&qkey);
    // Invalidate the deleted queue's config cache on peer replicas.
    st.notifier.broadcast_queue_config_delete(&qkey);

    // The SP always reports deleted:true and hides the real outcome in
    // `existed`, so "delete a queue that isn't yours / doesn't exist" reads as
    // success to any client that trusts `deleted`. Make the body self-consistent
    // (deleted mirrors existed). The status stays 200: DELETE is idempotent here
    // and the SDKs use delete-before-create as a cleanup idiom, so a 404 would
    // turn a no-op into a thrown error for them.
    let mut v: serde_json::Value =
        serde_json::from_str(&del_txt).unwrap_or(serde_json::Value::Null);
    if v.get("existed").and_then(|x| x.as_bool()) == Some(false) {
        if let Some(o) = v.as_object_mut() {
            o.insert("deleted".to_string(), serde_json::json!(false));
            o.insert(
                "message".to_string(),
                serde_json::json!("Queue not found, nothing was deleted"),
            );
            return json(StatusCode::OK, v.to_string());
        }
    }
    json(StatusCode::OK, del_txt)
}

// ----------------------------------------------------------------- get queue
// GET /api/v1/resources/queues/:queue — basic queue detail via get_queue_v2,
// enriched with a segments message count (get_queue_v2's stats come from
// queen.stats, which the segments engine does not populate). 404 when the queue
// is gone.
pub async fn handle_get_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_queue(&client, &queue, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("get failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
    if v.get("error").is_some() || v.is_null() {
        return json(StatusCode::NOT_FOUND, "{\"error\":\"Queue not found\"}".to_string());
    }

    // Enrich with segment counts (best-effort; leave the base detail intact on error).
    if let Ok((segs, msgs)) = db::seg_queue_message_stats(&client, &queue, tenant.as_str()).await {
        if let Some(obj) = v.as_object_mut() {
            obj.insert(
                "segments".to_string(),
                serde_json::json!({"segments": segs, "messages": msgs}),
            );
        }
    }

    json(StatusCode::OK, v.to_string())
}

// GET /api/v1/resources/queues/:queue/depth?group=... — minimal per-partition
// backlog read (queen.log_queue_depth_v1, 011_log_stats). Built for relay/
// scheduler pollers that read exactly one number per partition: the watermark
// arithmetic only, no segments scan, no timestamps, no DLQ join — against the
// console-grade GET /resources/queues/:queue this is one index-only read.
// `group` absent = queue-level pending under the same worst-cursor precedence
// the dashboard publishes; `group=<name>` = that group's own backlog (the ETA
// ingredient). 404 shape matches handle_get_queue.
#[derive(Deserialize)]
pub struct QueueDepthParams {
    group: Option<String>,
}

pub async fn handle_queue_depth(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
    Query(p): Query<QueueDepthParams>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::queue_depth(&client, &queue, p.group.as_deref(), tenant.as_str()).await {
        Ok(Some(txt)) => json(StatusCode::OK, txt),
        Ok(None) => json(StatusCode::NOT_FOUND, "{\"error\":\"Queue not found\"}".to_string()),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("depth failed: ", &e),
        ),
    }
}

// ------------------------------------------------------- resources LIST API
// GET /api/v1/resources/queues — queue list via get_queues_v2, the cached
// queen.stats view (partitions = child_count, retainedBytes, pending/processing,
// all rewritten by log_refresh_all_stats_v1 at the stats cadence), enriched by
// default from log_queue_stats_all_v1 — partition count and retained frames
// LIVE, Θ(the tenant's partitions); the segment count as of the retained-bytes
// lane's last pass (queen.stats.segment_count, see 028_retained_bytes.sql) —
// for the dashboard and the CLI list, where a partition count one cadence old
// reads as wrong. The exact live segment count is handle_get_queue's.
//
// `?stats=cached` skips that enrichment and serves the cached view as is. It
// exists for the proxy's reconciler (proxy/src/registry.rs), which polls this
// route once per cluster per interval and reads only `name`, `partitions`,
// `retainedBytes` and the top-level kv/timer bytes — every one of them already
// in the cached view — so computing a pass over the tenant's partitions and
// segments for each poll bought it nothing. (Until 2026-08-23 that pass was
// over the CELL's segments, whatever the tenant's size: see the function's
// header in 011_log_stats.sql.) A broker older than the parameter ignores it
// and enriches as before, so the proxy sends it unconditionally.
//
// Namespace/task query filters are accepted but not applied — the full list is
// a valid superset for the CLI list view.
#[derive(Deserialize)]
pub struct ListQueuesParams {
    /// `live` (default): per-queue counts computed now; `cached`: the
    /// queen.stats view as of the last refresh, nothing computed per call.
    stats: Option<String>,
}

pub async fn handle_list_queues(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(p): Query<ListQueuesParams>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let txt = match db::get_queues(&client, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("list failed: ", &e),
            )
        }
    };
    let mut v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);

    // RUSTFIX item 25: surface an embedded {"error":..} SP body as 404/500 instead
    // of serving it at 200 (mirrors handle_configure's guard and the resources
    // siblings). Happy-path bodies ({"queues":[...]}) have no top-level error key.
    if v.get("error").filter(|e| !e.is_null()).is_some() {
        return sp_result_to_response(txt);
    }

    // Enrich each queue with segment-derived counts (best-effort; leave the base
    // list intact on error). Skipped on `?stats=cached`.
    let live = p.stats.as_deref() != Some("cached");
    if !live {
        // nothing to compute: the cached view is the answer
    } else if let Ok(stats) = db::seg_queue_stats_all(&client, tenant.as_str()).await {
        let map: HashMap<String, (i64, i64, i64)> = stats
            .into_iter()
            .map(|(name, parts, segs, msgs)| (name, (parts, segs, msgs)))
            .collect();
        if let Some(arr) = v.get_mut("queues").and_then(|q| q.as_array_mut()) {
            for item in arr.iter_mut() {
                let name = match item.get("name").and_then(|x| x.as_str()) {
                    Some(s) => s.to_string(),
                    None => continue,
                };
                if let (Some(&(parts, segs, msgs)), Some(obj)) =
                    (map.get(&name), item.as_object_mut())
                {
                    obj.insert(
                        "segments".to_string(),
                        serde_json::json!({"segments": segs, "messages": msgs}),
                    );
                    obj.insert("partitions".to_string(), serde_json::json!(parts));
                    // `msgs` is the RETAINED frame count (log_queue_stats_all_v1),
                    // which is `total`, NOT the unconsumed backlog. `pending` and
                    // `processing` must keep the SP's watermark values (queen.stats,
                    // refreshed for seg queues by log_refresh_all_stats_v1) — the
                    // same numbers the overview sums — or the queue list contradicts
                    // every other pending reading in the UI.
                    let m = obj.entry("messages".to_string()).or_insert_with(
                        || serde_json::json!({"total": 0, "pending": 0, "processing": 0}),
                    );
                    if let Some(mo) = m.as_object_mut() {
                        mo.insert("total".to_string(), serde_json::json!(msgs));
                    }
                }
            }
        }
    }

    // PLAN_KV_TIMERS §9.8 P2: four TOP-LEVEL fields on the response the proxy's
    // reconciler already polls, so that the storage quota — which is a live hard
    // gate, not a no-op — can see the bytes of the two new tables. They have no
    // queue, so there is no per-queue entry they could ride on, and without this
    // they are the only place in the product where a tenant occupies disk that
    // no quota can measure.
    //
    // Read from the sweeper's cached measurement via the db.rs wrapper (a
    // primary-key lookup), NEVER counted here: a cloud reconciler polls this
    // route every ten seconds per cell.
    //
    // On a failure the fields are OMITTED rather than sent as zero, and that
    // distinction is the contract: the proxy reads absent fields as zero AND
    // warns, so a cell that cannot answer produces a loud zero instead of a
    // silent under-count. Sending a zero we do not believe would be the silent
    // one.
    match db::kv_usage_snapshot(&client, tenant.as_str()).await {
        Ok(usage) => {
            let (kr, kb, tr, tb) = usage.unwrap_or((0, 0, 0, 0));
            if let Some(obj) = v.as_object_mut() {
                obj.insert("kvRows".to_string(), serde_json::json!(kr));
                obj.insert("kvBytes".to_string(), serde_json::json!(kb));
                obj.insert("timerRows".to_string(), serde_json::json!(tr));
                obj.insert("timerBytes".to_string(), serde_json::json!(tb));
            }
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "queue listing: kv/timer usage unavailable, omitting the quota fields"
            );
        }
    }

    json(StatusCode::OK, v.to_string())
}

// GET /api/v1/resources/overview — system overview via get_system_overview_v3.
// Track B (§5): scoped to the request tenant (default tenant ⇒ global, as before).
pub async fn handle_system_overview(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_system_overview(&client, tenant.as_str()).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("overview failed: ", &e),
        ),
    }
}

// GET /api/v1/resources/namespaces — namespace list via get_namespaces_v2.
// Track B (§5): scoped to the request tenant.
pub async fn handle_list_namespaces(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_namespaces(&client, tenant.as_str()).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("namespaces failed: ", &e),
        ),
    }
}

// GET /api/v1/resources/tasks — task list via get_tasks_v2.
// Track B (§5): scoped to the request tenant.
pub async fn handle_list_tasks(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_tasks(&client, tenant.as_str()).await {
        Ok(t) => sp_result_to_response(t),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("tasks failed: ", &e),
        ),
    }
}

#[cfg(test)]
mod configure_mode {
    use super::*;

    fn flag(body: &str) -> Result<bool, String> {
        configure_replace_flag(&serde_json::from_str(body).expect("test body is JSON"))
    }

    #[test]
    fn an_absent_mode_merges() {
        // The shape every SDK has always sent. It must NOT mean replace, or the
        // upgrade silently keeps resetting the 18 options a partial body omits.
        assert_eq!(flag(r#"{"queue":"orders"}"#), Ok(false));
        assert_eq!(flag(r#"{"queue":"orders","mode":null}"#), Ok(false));
        assert_eq!(
            flag(r#"{"queue":"orders","options":{"leaseTime":60}}"#),
            Ok(false)
        );
    }

    #[test]
    fn the_two_spellings_are_the_whole_vocabulary() {
        assert_eq!(flag(r#"{"queue":"o","mode":"merge"}"#), Ok(false));
        assert_eq!(flag(r#"{"queue":"o","mode":"replace"}"#), Ok(true));
    }

    #[test]
    fn anything_else_is_a_400_and_not_a_silent_merge() {
        // A caller who wrote "patch" believes something about this call; doing
        // the other thing to the queue's whole configuration is the damage the
        // merge rule exists to prevent, so it is refused instead.
        for body in [
            r#"{"queue":"o","mode":"patch"}"#,
            r#"{"queue":"o","mode":"Replace"}"#,
            r#"{"queue":"o","mode":""}"#,
            r#"{"queue":"o","mode":true}"#,
            r#"{"queue":"o","mode":1}"#,
        ] {
            let err = flag(body).expect_err(body);
            assert!(
                err.starts_with("mode must be \"merge\" or \"replace\""),
                "{body} -> {err}"
            );
        }
    }

    // ----------------------------------------------------------- the bag
    // `mode` decides nothing on its own: it decides only through the `replace`
    // key this function puts in the bag the SP reads. Deleting that one
    // insertion used to leave every test in the repository green while
    // `mode:"replace"` silently merged — i.e. while `queenctl apply -f` stopped
    // being declarative and the product lost every reset path it has.

    fn bag(body: &str, replace: bool) -> serde_json::Value {
        serde_json::Value::Object(configure_options_bag(
            &serde_json::from_str(body).expect("test body is JSON"),
            replace,
        ))
    }

    #[test]
    fn the_replace_flag_is_what_reaches_the_sp() {
        assert_eq!(
            bag(r#"{"queue":"o","options":{"leaseTime":60}}"#, false),
            serde_json::json!({ "leaseTime": 60, "replace": false })
        );
        assert_eq!(
            bag(r#"{"queue":"o","options":{"leaseTime":60}}"#, true),
            serde_json::json!({ "leaseTime": 60, "replace": true })
        );
    }

    #[test]
    fn a_caller_supplied_replace_is_overwritten_not_honoured() {
        // `mode` is the one spelling of this decision on the wire. A bag that
        // carries its own `replace` must not be able to reset a queue behind a
        // merging request's back — nor to refuse a replace the caller asked for.
        assert_eq!(
            bag(
                r#"{"queue":"o","options":{"replace":true,"retryLimit":7}}"#,
                false
            )["replace"],
            serde_json::json!(false)
        );
        assert_eq!(
            bag(r#"{"queue":"o","options":{"replace":false}}"#, true)["replace"],
            serde_json::json!(true)
        );
    }

    #[test]
    fn a_top_level_options_body_keeps_mode_out_of_the_bag() {
        // Raw callers spread the options across the top level; `mode` is a
        // routing key there, not a 22nd option the SP would ignore silently.
        let got = bag(r#"{"queue":"o","leaseTime":60,"mode":"replace"}"#, true);
        assert_eq!(got, serde_json::json!({ "leaseTime": 60, "replace": true }));
    }

    #[test]
    fn a_top_level_namespace_is_folded_in_but_never_over_the_bag() {
        // Non-empty only, and never over a value the bag already spells: `""`
        // and `null` in the bag are how a caller clears the label, and a
        // top-level fold would silently drop both.
        assert_eq!(
            bag(
                r#"{"queue":"o","namespace":"billing","task":"ingest"}"#,
                false
            ),
            serde_json::json!({ "namespace": "billing", "task": "ingest", "replace": false })
        );
        assert_eq!(
            bag(r#"{"queue":"o","namespace":"","options":{}}"#, false),
            serde_json::json!({ "replace": false })
        );
        assert_eq!(
            bag(
                r#"{"queue":"o","namespace":"billing","options":{"namespace":null}}"#,
                false
            ),
            serde_json::json!({ "namespace": null, "replace": false })
        );
    }
}
