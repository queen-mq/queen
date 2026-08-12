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

// GET /api/v1/consumer-groups — every group across both engines. Returns the
// get_consumer_groups_v4 JSON array verbatim (the Admin client reads it as-is).
pub async fn handle_consumer_groups(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_consumer_groups(&client, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("consumer groups failed: ", &e),
        ),
    }
}

// GET /api/v1/consumer-groups/lagging?minLagSeconds= — partitions lagging beyond
// the threshold. Registered BEFORE the /:group route so the static `lagging`
// segment wins over the param match.
pub async fn handle_lagging_consumers(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // RUSTFIX item 22: default 3600s, matching C++ consumer_groups.cpp:99.
    let min_lag = qint(&params, "minLagSeconds", 3600);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_lagging_partitions(&client, min_lag, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("lagging failed: ", &e),
        ),
    }
}

// GET /api/v1/consumer-groups/:group — per-queue/partition detail for one group.
pub async fn handle_consumer_group_details(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(group): Path<String>,
) -> Response {
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::get_consumer_group_details(&client, &group, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("details failed: ", &e),
        ),
    }
}

// DELETE /api/v1/consumer-groups/:group?deleteMetadata= — drop the group. Removes
// its log cursors (queen.log_consumers, all partitions) + consumer_watermarks
// AND its shared coordination state (consumer_watermarks,
// consumer_groups_metadata when deleteMetadata). deletedPartitions sums both
// engines. HTTP 200 with the merged SP JSON (a 204 would make the JS client
// return null).
pub async fn handle_delete_consumer_group(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(group): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let delete_metadata = qbool(&params, "deleteMetadata", true);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let seg = match db::delete_consumer_group_seg(&client, &group, delete_metadata, tenant.as_str()).await {
        Ok(t) => t,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete(seg) failed: ", &e),
            )
        }
    };
    // Best-effort rows-side cleanup (empty for a pure-segments deployment).
    let rows = db::delete_consumer_group_rows(&client, &group, delete_metadata, tenant.as_str())
        .await
        .unwrap_or_else(|_| "{}".to_string());

    let seg_v: serde_json::Value = serde_json::from_str(&seg).unwrap_or(serde_json::Value::Null);
    let rows_v: serde_json::Value = serde_json::from_str(&rows).unwrap_or(serde_json::Value::Null);
    let seg_n = seg_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);
    let rows_n = rows_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);

    // Hot-list invalidation (2026-07-24) — symmetry with the DB cursor/watermark
    // delete above. The delete removed the group's committed cursors, so it must
    // reconsume from the start; but with QUEEN_HOTLIST on, the in-memory ring is
    // the discovery source and a pre-delete ring (stale IDLE/wheel entries + a
    // recent reseed clock) would suppress the reconsume until the ≤30s periodic
    // floor. Drop the group's ring on every queue so first contact reseeds cold.
    // Track B (§5): tenant-scoped, matching the SQL delete above and the
    // `seeded_groups` purge below — on a shared cell `workers` is a universal group
    // name and one tenant's delete must not cold-start every other tenant's ring.
    let dropped = st.hotlist.forget_group_all_queues(tenant.as_str(), &group);
    // The group-first-contact seed marker (consumer_groups_metadata) was removed
    // when delete_metadata, but the monotonic positive `seeded_groups` cache still
    // says "seeded" and would route the next pop down the ring path, skipping the
    // first-contact BULK SEED that safely re-creates the cursors. Drop the group
    // from every queue's cached set so the next pop re-checks the (now-absent)
    // marker and re-seeds via the first-contact wildcard path.
    if delete_metadata {
        // Track B (§5): seeded_groups is keyed by (tenant, queue); drop the group
        // only from THIS tenant's queue sets (prefix "<tenant>\x1f…").
        let prefix = crate::handlers::tenant_queue_key(tenant.as_str(), "");
        let mut sg = st.seeded_groups.lock().unwrap();
        for (k, set) in sg.iter_mut() {
            if k.starts_with(&prefix) {
                set.remove(&group);
            }
        }
    }
    // A1: the local rings are gone and cold-start themselves; this is for the peers.
    // After the forget, so the walk repopulates a ring holding only post-delete state.
    repair_after_group_delete(&st, &client, &dropped, &group).await;

    let out = serde_json::json!({
        "success": true,
        "consumerGroup": group,
        "deletedPartitions": seg_n + rows_n,
        "metadataDeleted": delete_metadata,
    });
    json(StatusCode::OK, out.to_string())
}

// DELETE /api/v1/consumer-groups/:group/queues/:queue?deleteMetadata= — drop the
// group FOR ONE QUEUE only. Removes the group's segment cursors for every
// partition of THAT queue (queen.log_consumers) + its consumer_watermarks row for
// (queue, group), AND the rows-side per-queue coordination state via
// queen.delete_consumer_group_for_queue_v1 (partition_consumers, consumer_watermarks,
// and consumer_groups_metadata when deleteMetadata). Clearing the empty-scan
// watermark is what lets the group re-consume the queue from the start (an
// advanced watermark would otherwise fence off every partition). deletedPartitions
// sums both engines; HTTP 200 with the merged JSON (a 204 would make the JS client
// return null).
pub async fn handle_delete_consumer_group_for_queue(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((group, queue)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let delete_metadata = qbool(&params, "deleteMetadata", true);
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    let seg_n = match db::delete_consumer_group_for_queue_seg(&client, &group, &queue, tenant.as_str()).await {
        Ok(n) => n as i64,
        Err(e) => {
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                json_err("delete(seg) failed: ", &e),
            )
        }
    };
    // Best-effort rows-side cleanup (empty for a pure-segments deployment).
    let rows = db::delete_consumer_group_for_queue_rows(&client, &group, &queue, delete_metadata, tenant.as_str())
        .await
        .unwrap_or_else(|_| "{}".to_string());
    let rows_v: serde_json::Value = serde_json::from_str(&rows).unwrap_or(serde_json::Value::Null);
    let rows_n = rows_v.get("deletedPartitions").and_then(|x| x.as_i64()).unwrap_or(0);

    // Hot-list invalidation (2026-07-24), scoped to this queue — see the all-queues
    // sibling for the rationale. Drop the group's ring for `queue` so a stale
    // pre-delete ring cannot mask the from-the-start reconsume, and (when the
    // per-(queue, group) seed marker was removed) drop the stale positive
    // `seeded_groups` entry so the next pop re-seeds via first contact.
    let qkey = crate::handlers::tenant_queue_key(tenant.as_str(), &queue);
    st.hotlist.forget_group(&qkey, &group);
    if delete_metadata {
        // Track B (§5): seeded_groups is keyed by (tenant, queue).
        if let Some(set) = st.seeded_groups.lock().unwrap().get_mut(&qkey) {
            set.remove(&group);
        }
    }
    // A1, the per-queue half — see repair_after_group_delete. Unconditional on the
    // ring having existed: unlike the all-queues sibling this handler names the queue,
    // so there is no set to enumerate and a broker with no ring simply walks once and
    // hands the rows to the peers, which is the whole point of the call.
    repair_after_group_delete(&st, &client, std::slice::from_ref(&qkey), &group).await;

    let out = serde_json::json!({
        "success": true,
        "consumerGroup": group,
        "queueName": queue,
        "deletedPartitions": seg_n + rows_n,
        "metadataDeleted": delete_metadata,
    });
    json(StatusCode::OK, out.to_string())
}

// POST /api/v1/consumer-groups/:group/subscription {subscriptionTimestamp} —
// update the group's subscription cutoff (consumer_groups_metadata; engine-
// agnostic). Returns the SP JSON.
#[derive(Deserialize)]
struct SubscriptionBody {
    #[serde(rename = "subscriptionTimestamp")]
    subscription_timestamp: Option<String>,
}

pub async fn handle_update_subscription(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(group): Path<String>,
    body: Bytes,
) -> Response {
    let b: SubscriptionBody = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    let ts = match b.subscription_timestamp.filter(|s| !s.is_empty()) {
        Some(t) => t,
        None => {
            return json(
                StatusCode::BAD_REQUEST,
                "{\"error\":\"subscriptionTimestamp is required\"}".to_string(),
            )
        }
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::update_consumer_group_subscription(&client, &group, &ts, tenant.as_str()).await {
        Ok(txt) => sp_result_to_response(txt),
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("subscription failed: ", &e),
        ),
    }
}

// Seek body: {toEnd:true} or {timestamp:"<iso>"}. Resolve to (to_end, timestamp).
#[derive(Deserialize)]
struct SeekBody {
    #[serde(rename = "toEnd")]
    to_end: Option<bool>,
    timestamp: Option<String>,
}

fn parse_seek(body: &Bytes) -> Result<(bool, Option<String>), &'static str> {
    let b: SeekBody = serde_json::from_slice(body).map_err(|_| "bad body")?;
    if b.to_end == Some(true) {
        return Ok((true, None));
    }
    match b.timestamp.filter(|s| !s.is_empty()) {
        Some(ts) => Ok((false, Some(ts))),
        None => Err("Must specify toEnd=true or a timestamp"),
    }
}

// A seek SP returns {"success":false,...} for a no-op request (partition not
// found). Only re-seed the ring on a real cursor move; a parse miss defaults to
// true (re-seeding is evidence-based and harmless either way).
fn seek_succeeded(txt: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(txt)
        .ok()
        .and_then(|v| v.get("success").and_then(|s| s.as_bool()))
        .unwrap_or(true)
}

// C2 (PLAN_HOTLIST_FOLLOWUP.md): may a repair walk hand its rows to the peers?
//
// QUEEN_HOTLIST_RESEED_FULL_MS=0 is documented as the revert to pre-windowing
// behaviour — the one lever an operator pulls when the hot list looks wrong — and
// pre-windowing this fan-out did not exist. Under the kill switch EVERY periodic pass
// is full again at the 30s cadence on every broker, so each peer re-discovers a moved
// cursor within one floor on its own, which is precisely what it used to do. Gating
// here (rather than giving the broadcast a knob of its own) is what makes the claim in
// config.rs true; C1 is the standing lesson in what a second, independently-settable
// cadence knob costs.
fn repair_broadcasts(st: &AppState) -> bool {
    st.hotlist_reseed_full_ms > 0
}

// A1 (PLAN_HOTLIST_FOLLOWUP.md) — the peers' half of a consumer-group delete.
//
// Deleting a group removes its queen.log_consumers rows, so COALESCE(c.committed, -1)
// reads -1 again for every partition and every partition holding data is pending for
// that group name. Nothing was WRITTEN, so no last_write_at moved and the windowed
// walk is blind to it by construction. Locally that is survivable — the caller drops
// the ring and first contact cold-starts a full walk — but a peer still holds a ring
// for the same group name with `full_reseed_ms` stamped, so it keeps running windowed
// passes and stays blind for up to hotlist_reseed_full_ms. Before the windowing it
// healed within 30s because everyone walked everything that often.
//
// So: the same treatment as a seek, on exactly the queues whose ring was dropped (a
// queue where this broker held no ring for the group had nothing to fix here, and the
// SQL-published repair marker covers the peers' queues this broker does not poll). The
// walk re-populates the local ring with the correct post-delete pending set — which is
// what cold start would have computed anyway, only now — and its rows go to the mesh.
//
// Cost note: N full walks in an admin handler, N being the queues this broker polls
// for the deleted group. The DELETE that just ran was itself proportional to every
// partition of every one of those queues, so this is the smaller half of the request.
async fn repair_after_group_delete(
    st: &Arc<AppState>,
    client: &deadpool_postgres::Client,
    qkeys: &[String],
    group: &str,
) {
    // C2: unlike the seek's local walk, this whole path is a post-windowing addition —
    // before it, a delete forgot the ring and left everything else to the ≤30s floor.
    // Under the kill switch that floor is back (every peer full-walks every 30s) and
    // the local ring has just been dropped, so the honest revert is to do nothing.
    if !st.hotlist.enabled() || !repair_broadcasts(st) {
        return;
    }
    for qkey in qkeys {
        let now_ms = crate::util::now_epoch_ms();
        super::data::hotlist_reseed_full(&st.hotlist, client, qkey, group, now_ms, true).await;
    }
}

// Hot-list re-discovery after a successful QUEUE-WIDE seek (2026-07-24). A BACKWARD seek moves
// the group's committed cursor behind messages the ring already cleared as
// consumed; with QUEEN_HOTLIST on the ring is the discovery source, so without an
// explicit re-seed the reconsume only resumes at the ≤30s periodic reseed floor
// (the legacy path was immediate). Re-seed the (queue, group) ring from committed
// PG state NOW and wake parked pops. Evidence-based by construction:
// log_hotlist_reseed_v1 returns only partitions with last_offset > committed, so a
// seek-to-end (cursor moved FORWARD) re-adds nothing — no false positive — while a
// backward/timestamp seek re-adds exactly the re-pending partitions. Over-marking
// would be a harmless ~0.2ms empty probe; under-marking is the bug we are closing.
//
// FULL and BROADCASTING, not the ordinary reseed (2026-08-11, with the windowed
// floor): a seek is the one operation that makes OLD partitions pending without
// writing them, so the windowed scan is blind to it by construction — it must be the
// full walk. And the walk only repairs the ring of the broker that served the seek;
// the peers used to heal within one ≤30s floor because every peer walked everything
// that often, which the slower full cadence no longer guarantees. Handing the rows to
// the mesh dirty set restores that immediacy over the existing frame.
//
// The PER-PARTITION seek shared this until A2 gave it repair_after_partition_seek: one
// moved cursor never needed a walk of the queue, only a mark of the partition.
async fn reseed_after_seek(
    st: &Arc<AppState>,
    client: &deadpool_postgres::Client,
    group: &str,
    queue: &str,
    tenant: &str,
    txt: &str,
) -> Option<super::data::ReseedOutcome> {
    if !st.hotlist.enabled() || !seek_succeeded(txt) {
        return None;
    }
    let now_ms = crate::util::now_epoch_ms();
    let qkey = crate::handlers::tenant_queue_key(tenant, queue);
    Some(
        super::data::hotlist_reseed_full(
            &st.hotlist,
            client,
            &qkey,
            group,
            now_ms,
            repair_broadcasts(st),
        )
        .await,
    )
}

// A2 (PLAN_HOTLIST_FOLLOWUP.md) — the per-PARTITION seek's own repair.
//
// One cursor moved, so one partition became pending. Routing that through
// reseed_after_seek made the operator pay a full-queue walk (9,563 partitions, 49ms in
// production) and fanned the group's ENTIRE pending set out to the peers, which then
// armed a ring entry per partition each. The mark below is the same statement in the
// scope it actually holds: this partition, this group — set it pending on the local
// ring and queue ONE group-carrying mesh hint, which is exactly what the peers need to
// know. No SQL, so no walk to fail and nothing to report back to the caller.
//
// Over-marking is the safe direction and is what a seek TO END does here: the
// partition is not really pending, the next claim finds nothing, and the entry clears
// on that empty verdict — the ~0.2ms empty SKIP LOCKED probe the whole hot list is
// costed against. Under-marking would be a stalled replay.
//
// The durable floor underneath is the same one the queue-wide seek gets: the SQL
// publishes a repair marker naming this partition (A3, queen.hotlist_repairs), so a
// peer whose mesh frame was dropped still marks it within one reconcile interval —
// and marks that partition, not the queue.
//
// Not gated by the C2 kill switch, unlike the walks: what that switch reverts is a
// fan-out of a queue's whole pending set (9,563 items, arming a ring entry each on
// every peer). One hint for one partition is a push-shaped event, and suppressing it
// would only make the operator wait for a floor to rediscover what a single frame
// already said.
fn repair_after_partition_seek(
    st: &Arc<AppState>,
    group: &str,
    queue: &str,
    partition: &str,
    tenant: &str,
    txt: &str,
) {
    if !st.hotlist.enabled() || !seek_succeeded(txt) {
        return;
    }
    let qkey = crate::handlers::tenant_queue_key(tenant, queue);
    st.hotlist
        .mark_local_group(&qkey, partition, group, crate::util::now_epoch_ms());
}

// A4 (PLAN_HOTLIST_FOLLOWUP.md): the seek's own repair walk used to fail into silence
// — HTTP 200, no replay, nothing in the logs, and an operator who had just clicked
// "replay from yesterday" with no way to know. The cursor move itself DID commit, so
// this is not an error status; it is a partial success and it has to say so in the
// body. A healthy seek's response is byte-identical to before (clients parse this),
// and no `error` key is added — sp_result_to_response maps that to a non-200.
fn note_partial_repair(txt: String, out: Option<&super::data::ReseedOutcome>) -> String {
    match out {
        Some(o) if !o.ok => {}
        _ => return txt,
    }
    let mut v: serde_json::Value = match serde_json::from_str(&txt) {
        Ok(serde_json::Value::Object(m)) => serde_json::Value::Object(m),
        _ => return txt,
    };
    v["hotlistRepaired"] = serde_json::Value::Bool(false);
    v["warning"] = serde_json::Value::String(
        "the cursor moved, but re-discovering the partitions it made pending failed on \
         this broker; delivery of the replayed messages resumes at the next full reseed"
            .to_string(),
    );
    v.to_string()
}

// POST /api/v1/consumer-groups/:group/queues/:queue/seek — move the group's
// segment cursor for every partition of the queue.
pub async fn handle_seek_consumer_group(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((group, queue)): Path<(String, String)>,
    body: Bytes,
) -> Response {
    let (to_end, ts) = match parse_seek(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("", e)),
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_seek_consumer_group(&client, &group, &queue, to_end, ts.as_deref(), tenant.as_str()).await {
        Ok(txt) => {
            let repair = reseed_after_seek(&st, &client, &group, &queue, tenant.as_str(), &txt).await;
            sp_result_to_response(note_partial_repair(txt, repair.as_ref()))
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("seek failed: ", &e),
        ),
    }
}

// POST /api/v1/consumer-groups/:group/queues/:queue/partitions/:partition/seek —
// move the group's segment cursor for ONE partition.
pub async fn handle_seek_partition(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((group, queue, partition)): Path<(String, String, String)>,
    body: Bytes,
) -> Response {
    // The webapp's per-partition "Skip to end" button (and the C++ per-partition
    // seek) send NO body; default an empty body to toEnd=true instead of 400ing. A
    // present body still goes through parse_seek (explicit toEnd / timestamp).
    let (to_end, ts) = if body.is_empty() {
        (true, None)
    } else {
        match parse_seek(&body) {
            Ok(v) => v,
            Err(e) => return json(StatusCode::BAD_REQUEST, json_err("", e)),
        }
    };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };
    match db::seg_seek_partition(&client, &group, &queue, &partition, to_end, ts.as_deref(), tenant.as_str()).await {
        Ok(txt) => {
            // A2: targeted, not the queue-wide walk this used to share with the
            // per-queue seek.
            repair_after_partition_seek(&st, &group, &queue, &partition, tenant.as_str(), &txt);
            sp_result_to_response(txt)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("seek failed: ", &e),
        ),
    }
}

/// A4 (PLAN_HOTLIST_FOLLOWUP.md): what a seek answers when its own repair failed. The
/// cursor move committed, so the status stays 200 and the SP's own keys stay exactly as
/// they were — the only honest change is that the body now says the replay has not been
/// re-discovered yet.
#[cfg(test)]
mod seek_repair_report {
    use super::*;
    use crate::hotlist::ReseedMode;

    const SEEK_OK: &str = r#"{"success":true,"consumerGroup":"g","queueName":"orders","partitionsUpdated":9563}"#;

    fn outcome(ok: bool) -> super::super::data::ReseedOutcome {
        super::super::data::ReseedOutcome { mode: ReseedMode::Full, rows: 0, ok, stamped: true }
    }

    #[test]
    fn a_seek_whose_repair_worked_answers_exactly_what_it_used_to() {
        // Both shapes of "nothing to report": no repair ran (hot list off / the seek
        // was a no-op), and one that ran fine. Clients parse this body.
        assert_eq!(note_partial_repair(SEEK_OK.to_string(), None), SEEK_OK);
        assert_eq!(
            note_partial_repair(SEEK_OK.to_string(), Some(&outcome(true))),
            SEEK_OK
        );
    }

    #[test]
    fn a_failed_repair_is_reported_without_turning_the_seek_into_an_error() {
        let out = note_partial_repair(SEEK_OK.to_string(), Some(&outcome(false)));
        let v: serde_json::Value = serde_json::from_str(&out).expect("still JSON");
        assert_eq!(v["success"], serde_json::json!(true), "the cursor DID move");
        assert_eq!(v["partitionsUpdated"], serde_json::json!(9563), "SP keys survive");
        assert_eq!(v["hotlistRepaired"], serde_json::json!(false));
        assert!(v["warning"].as_str().unwrap().contains("failed"));
        // An `error` key would make sp_result_to_response answer 500 for a seek that
        // committed — the opposite of the partial-success this is reporting.
        assert!(v.get("error").is_none());
    }

    #[test]
    fn a_body_that_is_not_a_json_object_is_passed_through_untouched() {
        // The SPs always return an object; a future one that does not must not have its
        // response rewritten into something its client cannot read.
        assert_eq!(note_partial_repair("[]".to_string(), Some(&outcome(false))), "[]");
        assert_eq!(note_partial_repair("nope".to_string(), Some(&outcome(false))), "nope");
    }
}
