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
use crate::util::{uuidv7_bytes, FnvHashMap};
use crate::vegas::Vegas;

// ------------------------------------------------------------------ push
#[derive(Deserialize)]
struct PushItem<'a> {
    queue: &'a str,
    partition: Option<&'a str>,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(rename = "transactionId")]
    transaction_id: Option<&'a str>,
}

#[derive(Deserialize)]
struct PushBody<'a> {
    #[serde(borrow)]
    items: Vec<PushItem<'a>>,
}

// Frames staged per (queue, partition) group. `item` indexes into `results`.
struct PreFrame {
    mid: [u8; 16],
    txn: String,
    // Plaintext: refcounted slice of the request body (no copy). Encrypted:
    // owned envelope bytes.
    payload: Bytes,
    // RUSTFIX item 8: true when `payload` is an encryption envelope (encrypted at
    // push for a queue with encryption_enabled).
    encrypted: bool,
    item: usize,
}

// Resolve layer-1 duplicate followers: a follower adopts the leader's FINAL
// message_id (a leader that turned out to be a cross-flush duplicate now carries
// the pre-existing id) and inherits an "error" status if the leader errored.
fn resolve_push_followers(results: &mut [ItemResult]) {
    for i in 0..results.len() {
        if let Some(l) = results[i].dup_of {
            let leader_mid = results[l].message_id.clone();
            let leader_status = results[l].status;
            results[i].message_id = leader_mid;
            if leader_status == "error" {
                results[i].status = "error";
            }
        }
    }
}

fn render_push_results(results: &[ItemResult]) -> String {
    // ~150B/item with two 36-char ids + queue name; undersizing costs a full
    // realloc+copy of the response on every push.
    let mut out = String::with_capacity(results.len() * 176 + 2);
    out.push('[');
    for (i, item) in results.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        out.push_str(",\"message_id\":\"");
        out.push_str(&item.message_id);
        out.push_str("\",\"transaction_id\":\"");
        json_escape_into(&mut out, &item.txn);
        out.push_str("\",\"queueName\":\"");
        json_escape_into(&mut out, &item.queue);
        out.push_str("\",\"status\":\"");
        out.push_str(item.status);
        out.push_str("\"}");
    }
    out.push(']');
    out
}

pub async fn handle_push(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let parsed: PushBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let n = parsed.items.len();
    if n == 0 {
        return json(StatusCode::CREATED, "[]".to_string());
    }

    // producer_sub is stamped ONLY from the validated JWT `sub` (never the body).
    // Computed up front so the maintenance-buffer path can carry it too.
    let producer_sub = authed.0.filter(|s| !s.is_empty());

    // RUSTFIX item 17: maintenance mode diverts EVERY push to the file buffer
    // (parity with push.cpp:307-359) — nothing reaches queen.log_segments; the
    // background drain replays on disable. Return 201 with per-item
    // status:"buffered" (or "failed" on a spool write error).
    if st.maintenance.load(Ordering::Relaxed) {
        return buffer_all(&st, &parsed, producer_sub.as_deref(), tenant.as_str()).await;
    }

    let mut results: Vec<ItemResult> = Vec::with_capacity(n);
    let mut groups: FnvHashMap<(String, String), Vec<PreFrame>> = FnvHashMap::default();
    // Layer 1 — intra-request first-wins dedup: (queue, partition, txn) -> the
    // leader item's index. A repeat within THIS request becomes a duplicate
    // follower of the leader and produces no frame; at render it copies the
    // leader's final message_id. Keyed by one composed string (fields joined on
    // \x1f, which cannot appear in queue/partition/txn) instead of a 3-String
    // tuple: one allocation per item instead of six clones (lookup + insert).
    let mut seen: FnvHashMap<String, usize> = FnvHashMap::default();
    // RUSTFIX item 8: memoize each distinct queue's encryption_enabled flag so the
    // hot loop does at most one async lookup per queue (not per item).
    let mut enc_flags: HashMap<String, bool> = HashMap::new();
    for (i, it) in parsed.items.iter().enumerate() {
        let mid = uuidv7_bytes();
        let mid_str = uuid_bytes_to_string(&mid);
        let txn = it
            .transaction_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| mid_str.clone());
        let queue = it.queue.to_string();
        let partition = it.partition.unwrap_or("Default").to_string();

        let mut seen_key =
            String::with_capacity(queue.len() + partition.len() + txn.len() + 2);
        seen_key.push_str(&queue);
        seen_key.push('\x1f');
        seen_key.push_str(&partition);
        seen_key.push('\x1f');
        seen_key.push_str(&txn);
        match seen.entry(seen_key) {
            std::collections::hash_map::Entry::Occupied(e) => {
                let leader = *e.get();
                // Intra-request duplicate: follower of `leader`. message_id is
                // provisional (the leader's minted id) and finalized at render time.
                let provisional = results[leader].message_id.clone();
                results.push(ItemResult {
                    message_id: provisional,
                    txn,
                    queue,
                    status: "duplicate",
                    dup_of: Some(leader),
                });
                continue;
            }
            std::collections::hash_map::Entry::Vacant(e) => {
                e.insert(i);
            }
        }
        results.push(ItemResult {
            message_id: mid_str,
            txn: txn.clone(),
            queue: queue.clone(),
            status: "queued",
            dup_of: None,
        });

        // RUSTFIX item 8: encrypt the payload for a queue with encryption_enabled
        // (parity with push.cpp:370-405). On failure, warn + store plaintext — never
        // fail the push.
        let raw = it.payload.get().as_bytes();
        let enc_on = if st.encryption.is_enabled() {
            match enc_flags.get(&queue) {
                Some(&b) => b,
                None => {
                    let b = st.encryption_enabled_for(&queue, tenant.as_str()).await;
                    enc_flags.insert(queue.clone(), b);
                    b
                }
            }
        } else {
            false
        };
        let (payload, encrypted) = if enc_on {
            match st.encryption.encrypt(raw) {
                Some(env) => (Bytes::from(env), true),
                None => {
                    // LOGGING_PLAN.md Phase 2: this fired one eprintln PER MESSAGE
                    // with no gate — a broken cipher floods stderr at ingest rate.
                    static ENC_FAIL: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = ENC_FAIL.tick_now() {
                        tracing::warn!(target: "push", queue = %queue, suppressed, "encryption failed; stored plaintext");
                    }
                    (body.slice_ref(raw), false)
                }
            }
        } else {
            // Zero-copy: `raw` borrows from the request body Bytes, so this is a
            // refcount bump, not a per-message copy.
            (body.slice_ref(raw), false)
        };

        groups.entry((queue, partition)).or_default().push(PreFrame {
            mid,
            txn,
            payload,
            encrypted,
            item: i,
        });
    }

    // producer_sub (computed above) is carried THROUGH fusion on each OwnedFrame —
    // the flush's pack_frames stamps it into the frame (FLAG_PSUB) — so auth-enabled
    // pushes coalesce across requests exactly like the anonymous path.
    let pending = groups.len();
    // Capture the pushed (qkey, partition) set before the submit loop consumes
    // `groups`, so we can wake parked pops / notify peers once the write lands.
    // Track B (§5): the ring and the wake gate are keyed by (tenant, queue), so the
    // composite key is built HERE — one String per touched queue, exactly the
    // allocation the bare-name clone already cost.
    let notify_keys: Vec<(String, String)> = groups
        .keys()
        .map(|(q, p)| (tenant_queue_key(tenant.as_str(), q), p.clone()))
        .collect();
    // 19-wildcard-hotlist §2: capture (qkey, partition, count) before `groups`
    // is consumed, so the post-commit mark knows each partition's frame count
    // (windowBuffer batch fattening). Only when the flag is on (else zero cost).
    let hotlist_marks: Vec<(String, String, u32)> = if st.hotlist.enabled() {
        groups
            .iter()
            .map(|((q, p), v)| (tenant_queue_key(tenant.as_str(), q), p.clone(), v.len() as u32))
            .collect()
    } else {
        Vec::new()
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    let state = Arc::new(PushState {
        results: Mutex::new(results),
        pending: AtomicUsize::new(pending),
        done: Mutex::new(Some(tx)),
    });
    for ((queue, partition), pfs) in groups {
        let frames: Vec<OwnedFrame> = pfs
            .into_iter()
            .map(|p| OwnedFrame {
                message_id: p.mid,
                txn: p.txn,
                payload: p.payload,
                producer_sub: producer_sub.clone(),
                encrypted: p.encrypted,
                state: state.clone(),
                item: p.item,
            })
            .collect();
        st.fusion.submit(AddMsg {
            queue,
            partition,
            tenant: tenant.0.clone(),
            frames,
        });
    }
    let _ = rx.await;
    st.metrics.push.record_request(n);
    // RUSTFIX item 24: per-queue push throughput for queue_lag_metrics (one
    // push_request per queue touched + push_messages = its item count).
    {
        let mut per_q: HashMap<&str, u64> = HashMap::new();
        for it in &parsed.items {
            *per_q.entry(it.queue).or_insert(0) += 1;
        }
        for (q, msgs) in per_q {
            st.metrics.per_queue.add_push(tenant.as_str(), q, msgs);
        }
    }
    // The segment is committed — make it discoverable. Two paths:
    if st.hotlist.enabled() {
        // 19-wildcard-hotlist §2 + C1: mark each pushed (queue, partition) pending on
        // every group ring (QUIET — no per-push local wake). The local wake is
        // COALESCED into main's ~5ms wake tick (mark_local_quiet flags the queue), so
        // a hot queue at 25k push/s costs ~200 notify_waiters/s, not 25k×O(parked).
        // Peers still get an IMMEDIATE batched MESSAGE_AVAILABLE (fan_out_pushed_batch)
        // so cross-broker discovery stays prompt even in a mixed on/off cluster; the
        // coalesced HOTLIST_DIRTY hints are additive. Incondizionato; a mark on a
        // partition whose push actually failed is the usual harmless false positive.
        let now_ms = crate::util::now_epoch_ms();
        for (qkey, p, n) in &hotlist_marks {
            st.hotlist.mark_local_quiet(qkey, p, *n, now_ms);
        }
        st.notifier.fan_out_pushed_batch(&notify_keys);
    } else {
        // Flag off ⇒ byte-identical: wake any parked long-poll pops on these queues
        // (local, with partition hints) and notify peer replicas so cross-replica
        // consume is immediate. One batched MESSAGE_AVAILABLE covers the whole bundle.
        st.notifier.notify_pushed_batch(&notify_keys);
    }

    // RUSTFIX item 1: an "error" status means the whole DB transaction failed
    // (connection/timeout) — fusion committed nothing. Spool those items to the
    // file buffer and report status:"buffered" so a client that only checks the
    // HTTP code does not silently lose them; the background drain replays them
    // (dedup on the preserved transactionId makes replay idempotent). Followers of
    // a buffered leader inherit its status.
    //
    // Snapshot the error leaders (index + resolved txn) UNDER the guard, drop it,
    // then encrypt+spool asynchronously (item 8) — a std Mutex guard cannot be held
    // across .await — and re-apply the statuses.
    let error_leaders: Vec<usize> = {
        let mut guard = state.results.lock().unwrap();
        resolve_push_followers(guard.as_mut_slice());
        (0..guard.len())
            .filter(|&i| guard[i].dup_of.is_none() && guard[i].status == "error")
            .collect()
    };
    // status "error" == the DB transaction that would have stored these frames
    // failed. Count them before they are re-labelled "buffered" below, or the
    // spool hides the failure from the "DB errors" series entirely.
    st.metrics.record_db_errors(error_leaders.len() as u64);

    let mut enc_flags: HashMap<String, bool> = HashMap::new();
    let mut new_status: Vec<(usize, &'static str)> = Vec::with_capacity(error_leaders.len());
    let mut buffered_any = false;
    for i in error_leaders {
        let it = &parsed.items[i];
        let txn = state.results.lock().unwrap()[i].txn.clone();
        let (payload, encrypted) = spool_payload(&st, it.queue, tenant.as_str(), it.payload, &mut enc_flags).await;
        let ok = st.file_buffer.write_event(
            it.queue,
            it.partition.unwrap_or("Default"),
            tenant.as_str(),
            &txn,
            producer_sub.as_deref(),
            encrypted,
            &payload,
        );
        if ok {
            buffered_any = true;
            new_status.push((i, "buffered"));
        } else {
            new_status.push((i, "failed"));
        }
    }

    let mut guard = state.results.lock().unwrap();
    for (i, s) in new_status {
        guard[i].status = s;
    }
    // Followers of a buffered/failed leader inherit its status.
    for i in 0..guard.len() {
        if let Some(l) = guard[i].dup_of {
            if guard[i].status == "error" {
                let s = guard[l].status;
                guard[i].status = s;
            }
        }
    }
    if buffered_any {
        // The DB just failed — hint the drain loop / fast-failover path.
        st.file_buffer.mark_db_unhealthy();
    }

    let body = render_push_results(guard.as_slice());
    json(StatusCode::CREATED, body)
}

// RUSTFIX item 8: the payload to spool for `queue` — an encryption envelope when
// the queue is encrypted (so the disk spool never holds at-rest plaintext for an
// encrypted queue), else the raw payload. Never fails (plaintext fallback). The
// per-queue flag is memoized in `enc_flags`.
async fn spool_payload(
    st: &Arc<AppState>,
    queue: &str,
    tenant: &str,
    raw: &RawValue,
    enc_flags: &mut HashMap<String, bool>,
) -> (Box<RawValue>, bool) {
    if st.encryption.is_enabled() {
        let on = match enc_flags.get(queue) {
            Some(&b) => b,
            None => {
                let b = st.encryption_enabled_for(queue, tenant).await;
                enc_flags.insert(queue.to_string(), b);
                b
            }
        };
        if on {
            if let Some(env) = st.encryption.encrypt(raw.get().as_bytes()) {
                if let Ok(rv) =
                    RawValue::from_string(String::from_utf8_lossy(&env).into_owned())
                {
                    // RUSTFIX item 8: report that the spooled payload IS an envelope,
                    // so the drain re-stamps FLAG_ENCRYPTED on the replayed frame.
                    return (rv, true);
                }
            }
        }
    }
    (raw.to_owned(), false)
}

// RUSTFIX item 17: buffer every item of a push to the file buffer (maintenance
// mode), returning per-item status:"buffered". A fresh transactionId is minted
// when the client omitted one (push.cpp:322-326) so the buffered result and the
// replay dedup key are well-defined. 201 if all spooled, 500 if any write failed.
async fn buffer_all(
    st: &Arc<AppState>,
    parsed: &PushBody<'_>,
    producer_sub: Option<&str>,
    tenant: &str,
) -> Response {
    let mut results: Vec<ItemResult> = Vec::with_capacity(parsed.items.len());
    let mut all_ok = true;
    let mut enc_flags: HashMap<String, bool> = HashMap::new();
    for it in parsed.items.iter() {
        let mid_str = uuid_bytes_to_string(&uuidv7_bytes());
        let txn = it
            .transaction_id
            .map(|s| s.to_string())
            .unwrap_or_else(|| mid_str.clone());
        let partition = it.partition.unwrap_or("Default");
        // RUSTFIX item 8: spool the encrypted envelope for an encrypted queue.
        let (payload, encrypted) = spool_payload(st, it.queue, tenant, it.payload, &mut enc_flags).await;
        let ok = st
            .file_buffer
            .write_event(it.queue, partition, tenant, &txn, producer_sub, encrypted, &payload);
        if !ok {
            all_ok = false;
        }
        results.push(ItemResult {
            message_id: mid_str,
            txn,
            queue: it.queue.to_string(),
            status: if ok { "buffered" } else { "failed" },
            dup_of: None,
        });
    }
    let body = render_push_results(&results);
    let code = if all_ok {
        StatusCode::CREATED
    } else {
        StatusCode::INTERNAL_SERVER_ERROR
    };
    json(code, body)
}

// ------------------------------------------------------------------- pop
#[derive(Deserialize)]
pub struct PopParams {
    batch: Option<i32>,
    partitions: Option<i32>,
    #[serde(rename = "autoAck")]
    auto_ack: Option<bool>,
    wait: Option<bool>,
    timeout: Option<u64>,
    // RUSTFIX item 18: per-request lease override (?leaseSeconds=N). Wins over the
    // queue's configured leaseTime; 0/absent = use the queue value (else 60).
    #[serde(rename = "leaseSeconds")]
    lease_seconds: Option<i32>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    // Subscription seeding for a NEW (partition, group) cursor on first contact:
    // subscriptionMode 'new' | 'all' (default), subscriptionFrom 'now' | ISO
    // timestamp | '' (default). Threaded to the log pop SPs (p_sub_mode /
    // p_sub_from); existing cursors are never re-seeded.
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
}

#[derive(Deserialize)]
struct PopResult {
    #[serde(default)]
    partitions: Vec<PopPart>,
    #[serde(default)]
    error: Option<String>,
}
#[derive(Deserialize)]
struct PopPart {
    partition: String,
    #[serde(rename = "partitionId")]
    partition_id: String,
    #[serde(default)]
    segments: Vec<PopSeg>,
}
#[derive(Deserialize)]
struct PopSeg {
    // base_offset of the segment (the SP's `seq` key; §11 opaque token). The
    // renderer proper ignores it, but the ACK REGISTRY uses it to derive the
    // leased batch_end broker-side: batch_end = seq + startOff + take - 1 of the
    // last (highest-base) delivered segment — the exact value queen.log_pop_v1
    // wrote to log_consumers.batch_end. Defaults to 0 if absent (autoAck /
    // non-leasing renders never read it).
    #[serde(default)]
    seq: i64,
    #[serde(rename = "startOff")]
    start_off: i32,
    take: i32,
    #[serde(rename = "createdAt")]
    created_at: String,
    // base64 text on the wire_v1 paths; absent on the bin_v1 path (blobs travel
    // out-of-band as a native bytea[] aligned with segment traversal order).
    #[serde(default)]
    blob: String,
}

// 19-wildcard-hotlist §7: one per-candidate tri-state verdict from
// queen.log_pop_list_v1's `states` array. `until` is the lease-expiry ISO for
// the `leased` verdict.
#[derive(Deserialize)]
struct ListState {
    p: String,
    s: String,
    #[serde(default)]
    until: Option<String>,
    /// took only: the partition's allocator watermark read at the claim —
    /// compared with the served batch_end to derive `drained` (spec §2
    /// clear-su-ack; see 004_log_pop's log_pop_list_v1).
    #[serde(rename = "lastOff", default)]
    last_off: Option<i64>,
}

// Single-partition pop result (db::pop_specific assembles the queen.log_pop_v1
// rows into this shape): segments + partitionId, no partition name (the caller
// knows it from the path). `seq` in the segment JSON carries base_offset and
// `startOff` the start frame index — opaque tokens (§11); the renderer only
// reads startOff/take/createdAt/blob.
#[derive(Deserialize)]
struct PopSpecificResult {
    #[serde(default)]
    segments: Vec<PopSeg>,
    #[serde(rename = "partitionId", default)]
    partition_id: String,
    #[serde(default)]
    error: Option<String>,
}

pub async fn handle_pop(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
    Query(p): Query<PopParams>,
) -> Response {
    // Pop maintenance: consumers get an empty, paused result (204) — matches the
    // C++ pop-maintenance behavior, but shaped so the client's empty-response
    // handling (no messages) simply retries.
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return json(StatusCode::NO_CONTENT, "{\"messages\":[],\"paused\":true}".to_string());
    }
    let batch = p.batch.unwrap_or(200);
    let max_parts = p.partitions.unwrap_or(1);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let sub_mode = p.subscription_mode.unwrap_or_else(|| "all".to_string());
    let sub_from = p.subscription_from.unwrap_or_default();
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    // RUSTFIX item 18: COALESCE(request.leaseSeconds, queue.lease_time, 60).
    let lease_seconds = match p.lease_seconds {
        Some(v) if v > 0 => v,
        _ => st.lease_time_for(&queue, tenant.as_str()).await,
    };
    // Track B (§5): the ring and the wake gate are keyed by (tenant, queue). Built
    // ONCE per request, before any loop — never per poll iteration.
    let qkey = tenant_queue_key(tenant.as_str(), &queue);

    // 19-wildcard-hotlist: with QUEEN_HOTLIST on, serve the wildcard pop from the
    // broker-side candidate ring instead of the SQL candidate scan. Flag off ⇒
    // this branch is never taken and the path below is byte-identical.
    if st.hotlist.enabled() {
        return serve_pop_hotlist(
            &st, &qkey, &queue, &group, batch, max_parts, auto_ack, wait, deadline, lease_seconds,
            &sub_mode, &sub_from, &worker, tenant.as_str(),
        )
        .await;
    }

    let mut backoff_count: u32 = 0;
    loop {
        // ── discovery-latency fix (2026-07-24): the legacy long-poll re-checks the
        // queue every backoff interval, so at N parked consumers the rate of EMPTY
        // re-polls is O(#parked consumers) = O(#queues). Acquiring the SHARED
        // pop_vegas serving permit on every such empty re-poll let that O(#queues)
        // storm saturate the limiter (Vegas shrinks it under RTT pressure) — so a
        // freshly-woken REAL delivery pop queued behind thousands of empty re-polls
        // on acquire(), a priority inversion whose wait grew LINEARLY with the queue
        // count. Gate the vegas-limited wildcard scan behind the cheap indexed
        // has_pending SUPERSET (no permit taken): `false` means there is definitively
        // nothing to deliver → skip the scan and park; a "maybe pending" queue takes
        // the permit and scans. The probe borrows a pooled connection (uncontended —
        // pool.get measured ~0µs) and never touches pop_vegas, so the quiet re-poll
        // storm can no longer starve real deliveries.
        let pending = match st.pool.get().await {
            Ok(c) => db::has_pending(&c, &queue, &group, tenant.as_str()).await.unwrap_or(true),
            Err(_) => true, // probe unavailable → fall back to the full scan (safe)
        };

        let (txt, blobs, rtt): (String, Vec<Vec<u8>>, Duration) = if pending {
            let permit = st.pop_vegas.acquire().await;
            let client = match st.pool.get().await {
                Ok(c) => c,
                Err(_) => {
                    st.metrics.record_db_error();
                    drop(permit);
                    return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
                }
            };
            // Cancel token captured BEFORE issuing the query: on a broker-side
            // timeout we cancel the still-running statement server-side and
            // quarantine this connection instead of abandoning it.
            let cancel_token = client.cancel_token();
            let t0 = Instant::now();
            let res = tokio::time::timeout(
                st.stmt_timeout,
                db::pop_wildcard_bin(
                    &client, &queue, &group, batch, lease_seconds, &worker, auto_ack, max_parts,
                    &sub_mode, &sub_from, tenant.as_str(),
                ),
            )
            .await;
            let rtt = t0.elapsed();
            st.pop_vegas.record(rtt);
            drop(permit);
            // Spec §10 (parked long-poll): resolve_query_timeout releases the pooled
            // connection (drop on success/db-error, DETACH+cancel on timeout) BEFORE
            // any parking below — a parked pop must never pin a PG connection.
            let (txt, blobs) = match db::resolve_query_timeout(res, client, cancel_token, "pop_wildcard", &st.metrics) {
                Some(t) => {
                    // Phase 2 observability: an actual wildcard candidate scan.
                    st.metrics.pop_wildcard.fetch_add(1, Ordering::Relaxed);
                    t
                }
                None => {
                    return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pop failed\"}".to_string())
                }
            };
            (txt, blobs, rtt)
        } else {
            // Nothing pending — synthesize an empty wildcard result and fall through
            // to the shared park/serve logic below WITHOUT taking a serving permit.
            ("{\"partitions\":[]}".to_string(), Vec::new(), Duration::ZERO)
        };

        // On a leased (non-autoAck) pop, the worker id IS the lease id the client
        // echoes back in ack/renew. autoAck pops advance the cursor server-side and
        // carry no lease, so they report an empty leaseId.
        let lease_id: &str = if auto_ack { "" } else { &worker };
        let (body, count, meta) =
            build_pop_response(&txt, Some(&blobs), &queue, &group, lease_id, &st.encryption);
        if count == 0 && wait && Instant::now() < deadline {
            // Park on the queue's wake gate; a push wakes us at once.
            // RUSTFIX item 19: exponentially-backing-off re-query; a push-wake resets it.
            backoff_count += 1;
            let interval = st.pop_backoff_interval(backoff_count);
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(interval);
            // Parked gauge: held for exactly the awaited window (dashboard
            // Parked row / queue_parked_replica, sampled at 1 Hz).
            let parked = st.metrics.parked.enter(tenant.as_str(), &queue);
            if st.notifier.wait_queue(&qkey, waitd).await {
                backoff_count = 0;
                // No longer parked — we're actively serving now.
                drop(parked);
                // Phase 2: the push that woke us left a partition hint. Try a
                // targeted single-partition pop (the cheap specific-partition SP)
                // for the hinted partitions instead of another wildcard scan. If
                // every hinted partition comes back empty (another consumer won the
                // SKIP LOCKED race, or the hint was for a different group's data),
                // fall through to the wildcard backstop on the next iteration.
                // Phase 2 first-contact safety: only take the targeted
                // single-partition path once this (queue, group)'s
                // group-first-contact BULK SEED has committed (004_log_pop). Before that,
                // targeted pops would drive queen.log_pop_v1's per-partition lazy
                // first-contact INSERT en masse and re-form the transactionid
                // convoy the seed prevents (51e50c4) — the merge regression that
                // wedged the broker at t=0. Until seeded we skip draining (hints
                // stay parked, bounded by HINT_CAP) and fall through to the
                // wildcard pop on the next iteration, which is the backstop that
                // CARRIES the seed. group_seeded is a cache hit (zero DB) in
                // steady state, so the phase-2 fast path is preserved once seeded.
                if st.group_seeded(&queue, &group, tenant.as_str()).await {
                    let hints = st.notifier.drain_hints(&qkey, max_parts.max(1) as usize);
                    if !hints.is_empty() {
                        if let Some(resp) = try_targeted_serve(
                            &st, &queue, &hints, &group, batch, lease_seconds, &worker, auto_ack,
                            &sub_mode, &sub_from, tenant.as_str(),
                        )
                        .await
                        {
                            return resp;
                        }
                    }
                }
            }
            continue;
        }
        // ACK REGISTRY: record this pop's leases (no-op on autoAck / empty).
        register_leases(&st, &group, &worker, lease_seconds, &meta);
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        // RUSTFIX item 24: per-queue pop throughput for queue_lag_metrics.
        if count > 0 {
            st.metrics.per_queue.add_pop(tenant.as_str(), &queue, count as u64);
            st.metrics
                .per_queue
                .add_pop_lag(tenant.as_str(), &queue, meta.lag_sum_ms, meta.lag_max_ms, meta.lag_n);
            for pid in &meta.partition_ids {
                st.remember_partition_queue(pid, &queue);
            }
        } else {
            st.metrics.per_queue.add_pop_empty(tenant.as_str(), &queue);
        }
        // autoAck advances the cursor server-side (no client ack round-trip), but it
        // IS an acknowledgement — count it so the ack throughput / completed totals
        // on the dashboard reflect auto-acked consumption too.
        if auto_ack && count > 0 {
            st.metrics.per_queue.add_ack(tenant.as_str(), &queue, count as u64, 0);
            st.metrics.ack.record_request(count);
            st.metrics
                .ack_success
                .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
        }
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

// Phase 2: a woken long-poll drains its partition hints and pops each hinted
// partition directly via the specific-partition SP (`db::pop_specific`), skipping
// the ~10ms wildcard candidate scan. Returns Some(rendered response) once at least
// one targeted pop returns data (already metered, incl. `pop_targeted`); None when
// every hinted partition is empty — the caller then falls through to the wildcard
// pop, which is the correctness backstop for missed wakes / pre-existing backlog /
// consumer-group seeding. `batch` is the shared budget across the hinted
// partitions, decremented as data is taken, so the client never gets more than it
// asked for. Response shape / lease / metrics match the wildcard serve path.
#[allow(clippy::too_many_arguments)]
async fn try_targeted_serve(
    st: &Arc<AppState>,
    queue: &str,
    hints: &[String],
    group: &str,
    batch: i32,
    lease_seconds: i32,
    worker: &str,
    auto_ack: bool,
    sub_mode: &str,
    sub_from: &str,
    tenant: &str,
) -> Option<Response> {
    let lease_id: &str = if auto_ack { "" } else { worker };
    let mut parts: Vec<PopPart> = Vec::new();
    let mut remaining = batch;
    let mut total_rtt = Duration::ZERO;
    for hint in hints {
        if remaining <= 0 {
            break;
        }
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(permit);
                break;
            }
        };
        let cancel_token = client.cancel_token();
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_specific(
                &client, queue, hint, group, remaining, lease_seconds, worker, auto_ack, sub_mode,
                sub_from, tenant,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        total_rtt += rtt;
        drop(permit);
        // This is a targeted (hint-driven) pop — count it whether or not it found
        // data; an empty result still cost ~1ms vs the wildcard's ~10ms.
        st.metrics.pop_targeted.fetch_add(1, Ordering::Relaxed);
        let txt = match db::resolve_query_timeout(res, client, cancel_token, "pop_targeted", &st.metrics) {
            Some(t) => t,
            None => break, // DB error/timeout — abandon targeted, fall back to wildcard
        };
        let parsed: PopSpecificResult = match serde_json::from_str(&txt) {
            Ok(p) => p,
            Err(_) => continue,
        };
        if parsed.error.is_some() {
            continue;
        }
        let seg_count: i32 = parsed.segments.iter().map(|s| s.take.max(0)).sum();
        if seg_count > 0 {
            parts.push(PopPart {
                partition: hint.clone(),
                partition_id: parsed.partition_id,
                segments: parsed.segments,
            });
            remaining -= seg_count;
        }
    }
    if parts.is_empty() {
        return None;
    }
    let (body, count, meta) =
        render_pop_parts(&parts, None, queue, group, lease_id, &st.encryption);
    // A non-empty segment set that rendered to zero messages (e.g. an undecodable
    // blob) is left to the wildcard backstop rather than served as an empty 200.
    if count == 0 {
        return None;
    }
    // ACK REGISTRY: record the targeted (hint-driven) pop's leases too.
    register_leases(st, group, worker, lease_seconds, &meta);
    st.metrics.pop.record_request(count);
    st.metrics.pop.record_batch(count, true, total_rtt);
    st.metrics.per_queue.add_pop(tenant, queue, count as u64);
    st.metrics
        .per_queue
        .add_pop_lag(tenant, queue, meta.lag_sum_ms, meta.lag_max_ms, meta.lag_n);
    for pid in &meta.partition_ids {
        st.remember_partition_queue(pid, queue);
    }
    if auto_ack {
        st.metrics.per_queue.add_ack(tenant, queue, count as u64, 0);
        st.metrics.ack.record_request(count);
        st.metrics
            .ack_success
            .fetch_add(count as u64, Ordering::Relaxed);
    }
    Some(json(StatusCode::OK, body))
}

// 19-wildcard-hotlist §8: how often a (queue, group) re-runs the keyset reseed
// (correctness floor for missed marks / dropped mesh hints / cold start) is
// AppState.hotlist_reseed_ms (QUEEN_HOTLIST_RESEED_MS, default 30s).
// Bounded reseed page size and page cap (§8: ~10k chunks, keyset).
const HOTLIST_RESEED_PAGE: i32 = 10_000;
const HOTLIST_RESEED_MAX_PAGES: usize = 200;
const NIL_UUID: &str = "00000000-0000-0000-0000-000000000000";
// Queue deferral-config cache TTL (§6): a /configure change is picked up within
// this window.
const HOTLIST_CFG_TTL_MS: i64 = 30_000;

/// TASK M — should this pop hold its under-full batch back, and for how long?
/// Pure so the policy is unit-testable without a broker; every guard here is a
/// case where waiting would cost latency for no batching benefit.
///
/// `est` is the hot-list's in-memory estimate of what a claim right now could
/// take (see `HotList::ready_est`); `remaining` is what is left of the caller's
/// own deadline. Returns None (= serve now, pre-feature behaviour) whenever the
/// queue is not configured for it, the caller did not ask to block, the batch is
/// already full, or the queue is EMPTY — an empty queue belongs to the long-poll.
#[inline]
fn min_pop_wait_window(
    min_pop_wait_ms: i32,
    wait: bool,
    batch: i32,
    est: u64,
    remaining: Duration,
) -> Option<Duration> {
    // OFF by default: 0 ⇒ every existing deployment is byte-identical.
    if min_pop_wait_ms <= 0 {
        return None;
    }
    // A pop that explicitly refuses to block (`wait=false`) means "give me what is
    // there right now"; delaying it would break that contract.
    if !wait {
        return None;
    }
    // A batch of one is full on arrival — there is nothing to fatten.
    if batch <= 1 {
        return None;
    }
    // EMPTY: not our case. The long-poll park/wake path owns it, unchanged.
    if est == 0 {
        return None;
    }
    // Already full (or an unmeasurable wheel-due backlog ⇒ u64::MAX): serve now.
    if est >= batch as u64 {
        return None;
    }
    // Never exceed the caller's deadline.
    let w = Duration::from_millis(min_pop_wait_ms as u64).min(remaining);
    if w.is_zero() {
        None
    } else {
        Some(w)
    }
}

// 19-wildcard-hotlist: serve a wildcard pop from the broker candidate ring. One
// ring-serve attempt per loop iteration; an empty ring / all-empty candidate set
// parks on the SAME queue wake gate (woken by a local push, a peer's mesh dirty
// hint, an ack-promote, or the background wheel tick) and re-takes on wake —
// never re-runs the SQL candidate scan. Response shape / lease / metrics are
// identical to the SQL wildcard path.
#[allow(clippy::too_many_arguments)]
async fn serve_pop_hotlist(
    st: &Arc<AppState>,
    // Track B (§5): `qkey` addresses the in-memory ring + wake gate; `queue` is the
    // bare name the SQL and the rendered response still use. They are never
    // interchangeable — see the hotlist module note on the key contract.
    qkey: &str,
    queue: &str,
    group: &str,
    batch: i32,
    max_parts: i32,
    auto_ack: bool,
    wait: bool,
    deadline: Instant,
    lease_seconds: i32,
    sub_mode: &str,
    sub_from: &str,
    worker: &str,
    tenant: &str,
) -> Response {
    let lease_id: &str = if auto_ack { "" } else { worker };
    let mut backoff_count: u32 = 0;
    loop {
        // ── TASK M (minimum pop wait). The ceiling on a small cell is
        // COMMIT-bound, ~4 tiny PG commits per DELIVERED message, because a pop
        // claims whatever single message just arrived ("pop magro"). This holds an
        // UNDER-FULL claim back for up to the queue's configured window so ONE
        // commit carries more messages.
        //
        // WHERE the wait lives is the whole design. It is HERE — in Rust, before
        // hotlist_pop_attempt — and never inside the SQL, because a wait inside
        // log_pop_list_v1 (pg_sleep) would hold a pooled PG connection, a pop_vegas
        // serving permit and (once claimed) row locks for the entire window: it
        // would trade a commit for a connection, and on a 2-core cell with a Vegas
        // limit in the tens that is strictly worse than the disease. Waiting here
        // costs the broker a timer and nothing else — the same precedent as the
        // parked long-poll, which releases its connection BEFORE parking.
        //
        // Semantics (deliberately NOT long-poll's): long-poll `wait=true` waits on
        // an EMPTY queue; this waits only when the queue is NON-EMPTY and the batch
        // is UNDER-FULL, and it ends at the FIRST of: batch full, window elapsed,
        // caller's deadline. `est == 0` ⇒ return immediately and let the normal
        // park/wake path own the empty case, so an idle consumer's discovery
        // latency is untouched.
        // The gate is a relaxed atomic load, false on every broker where no queue
        // has ever configured the option — so the default lane pays ONE load and
        // never the global `queues` mutex the per-queue lookup would take.
        let min_wait_ms = if st.hotlist.min_pop_wait_in_play() {
            st.hotlist.min_pop_wait_ms(qkey)
        } else {
            0
        };
        if min_wait_ms > 0 {
            let now_ms = crate::util::now_epoch_ms();
            let est = st.hotlist.ready_est(qkey, group, now_ms);
            if let Some(w) = min_pop_wait_window(
                min_wait_ms,
                wait,
                batch,
                est,
                deadline.saturating_duration_since(Instant::now()),
            ) {
                let t_w = Instant::now();
                let end = t_w + w;
                loop {
                    let left = end.saturating_duration_since(Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    // A push wake cuts the sleep short so a batch that fills early
                    // is served early; a MISSED wake only costs the remainder of an
                    // already-bounded window, never correctness.
                    st.notifier.wait_queue(qkey, left).await;
                    if st.hotlist.ready_est(qkey, group, crate::util::now_epoch_ms())
                        >= batch as u64
                    {
                        break;
                    }
                }
                st.metrics.pop_fill_wait.fetch_add(1, Ordering::Relaxed);
                st.metrics
                    .pop_fill_wait_us
                    .fetch_add(t_w.elapsed().as_micros() as u64, Ordering::Relaxed);
            }
        }

        let (body, count, meta, rtt) = hotlist_pop_attempt(
            st, qkey, queue, group, batch, max_parts, auto_ack, lease_seconds, sub_mode, sub_from,
            worker, lease_id, tenant,
        )
        .await;

        if count == 0 && wait && Instant::now() < deadline {
            backoff_count += 1;
            let interval = st.pop_backoff_interval(backoff_count);
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(interval);
            let _parked = st.metrics.parked.enter(tenant, queue);
            if st.notifier.wait_queue(qkey, waitd).await {
                if st.hotlist.traced(qkey) {
                    eprintln!("[hlt] woken q={} g={} t={}", queue, group,
                        crate::hotlist::trace_now_ms());
                }
                backoff_count = 0;
            }
            continue;
        }

        register_leases(st, group, worker, lease_seconds, &meta);
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        if count > 0 && st.hotlist.traced(qkey) {
            eprintln!("[hlt] served q={} g={} n={} t={}", queue, group, count,
                crate::hotlist::trace_now_ms());
        }
        if count > 0 {
            st.metrics.per_queue.add_pop(tenant, queue, count as u64);
            st.metrics
                .per_queue
                .add_pop_lag(tenant, queue, meta.lag_sum_ms, meta.lag_max_ms, meta.lag_n);
            for pid in &meta.partition_ids {
                st.remember_partition_queue(pid, queue);
            }
        } else {
            st.metrics.per_queue.add_pop_empty(tenant, queue);
        }
        if auto_ack && count > 0 {
            st.metrics.per_queue.add_ack(tenant, queue, count as u64, 0);
            st.metrics.ack.record_request(count);
            st.metrics
                .ack_success
                .fetch_add(count as u64, Ordering::Relaxed);
        }
        return json(
            if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK },
            body,
        );
    }
}

// Cancellation-safety guard for the hot-list serve path (§4/§7). take_batch
// marks the checked-out candidates INFLIGHT; if the pop future is dropped between
// checkout and checkin, this re-appends them (Requeue) so they are never stranded
// out of the ring (a mark/promote/reseed on an INFLIGHT entry cannot re-add it).
// The normal paths set `armed = false` and run the real checkin.
struct InflightGuard {
    hl: Arc<crate::hotlist::HotList>,
    qkey: String,
    group: String,
    cands: Vec<crate::hotlist::Candidate>,
    now_ms: i64,
    auto_ack: bool,
    lease_ms: i64,
    armed: bool,
}
impl Drop for InflightGuard {
    fn drop(&mut self) {
        if !self.armed || self.cands.is_empty() {
            return;
        }
        let back: Vec<crate::hotlist::CheckinResult> = self
            .cands
            .iter()
            .map(|c| crate::hotlist::CheckinResult {
                name: c.name.clone(),
                epoch: c.epoch,
                verdict: crate::hotlist::Verdict::Requeue,
                drained: false,
            })
            .collect();
        self.hl.checkin(
            &self.qkey,
            &self.group,
            back,
            self.now_ms,
            self.auto_ack,
            self.lease_ms,
        );
    }
}

// One ring-serve attempt: (lazily) refresh the queue deferral config, take K
// candidates from the (queue, group) ring, call queen.log_pop_list_v1 on them,
// check the tri-state verdicts back into the ring (§4/§6/§7), and render. An
// empty ring triggers a throttled keyset reseed (§8) before giving up. Returns
// (body, count, meta, rtt); count==0 means the caller should park.
#[allow(clippy::too_many_arguments)]
async fn hotlist_pop_attempt(
    st: &Arc<AppState>,
    // Track B (§5): `qkey` addresses the ring; `queue` stays the bare name for the
    // SQL calls and the rendered response.
    qkey: &str,
    queue: &str,
    group: &str,
    batch: i32,
    max_parts: i32,
    auto_ack: bool,
    lease_seconds: i32,
    sub_mode: &str,
    sub_from: &str,
    worker: &str,
    lease_id: &str,
    tenant: &str,
) -> (String, usize, PopMeta, Duration) {
    let now_ms = crate::util::now_epoch_ms();
    let empty = || render_pop_parts(&[], None, queue, group, lease_id, &st.encryption);

    // First-contact BOOTSTRAP (spec §2 / §9, the st.group_seeded gate): the ring
    // path's log_pop_list_v1 does NOT carry the group-first-contact BULK SEED that
    // the wildcard SP does — and without seeded cursors, log_pop_v1's per-partition
    // lazy consumer-row creation hits its advisory-lock guard under many concurrent
    // consumers and mostly SKIPs (the 51e50c4 empty-storm / convoy). So until the
    // seed marker exists for (queue, group), serve THIS pop via the wildcard SP
    // (which carries the seed and serves) — flipping group_seeded true; every
    // subsequent pop takes the ring path. group_seeded is a cache hit (zero DB,
    // zero pool.get) once seeded, so steady state is pure ring.
    if !st.group_seeded(queue, group, tenant).await {
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(permit);
                let (b, c, m) = empty();
                return (b, c, m, Duration::ZERO);
            }
        };
        let cancel_token = client.cancel_token();
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_wildcard_bin(
                &client, queue, group, batch, lease_seconds, worker, auto_ack, max_parts,
                sub_mode, sub_from, tenant,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        drop(permit);
        let (txt, blobs) = match db::resolve_query_timeout(res, client, cancel_token, "pop_wildcard", &st.metrics)
        {
            Some(t) => t,
            None => {
                let (b, c, m) = empty();
                return (b, c, m, rtt);
            }
        };
        // Learn ids for the ack bridge; the ring is populated by the next pop's
        // reseed (cursors now seeded ⇒ pending partitions become visible to it).
        if let Ok(parsed) = serde_json::from_str::<PopResult>(&txt) {
            for part in &parsed.partitions {
                st.hotlist
                    .note_partition_id(qkey, &part.partition, &part.partition_id);
            }
            let (body, count, meta) = render_pop_parts(
                &parsed.partitions, Some(&blobs), queue, group, lease_id, &st.encryption,
            );
            return (body, count, meta, rtt);
        }
        let (b, c, m) = empty();
        return (b, c, m, rtt);
    }

    // Register the (tenant, queue, group) ring so pushes' marks reach it even before
    // the first serve completes (a mark for an unknown group ring is otherwise lost
    // until reseed, §8).
    //
    // Deliberately AFTER the seed gate above, never before it: `group_seeded` is the
    // proof that this (tenant, queue, group) EXISTS — it reads the committed
    // consumer_groups_metadata marker. Creating the ring first meant any
    // `GET /pop/queue/<random-name>` permanently allocated a QueueState (interning
    // tables + per-group rings) for a queue that never existed, which on a shared free
    // tier is a memory vector any untrusted tenant can drive in a loop. An unseeded
    // (or nonexistent) queue now returns above having allocated nothing.
    st.hotlist.ensure_group(qkey, group);

    // ── discovery-latency fix (2026-07-24): resolve the in-memory ring BEFORE
    // taking the pop_vegas serving permit / a pooled connection. A parked consumer
    // whose queue is quiet re-polls every backoff interval, so the rate of EMPTY
    // re-polls is O(#parked consumers) = O(#queues). The old order acquired the
    // SHARED pop_vegas limiter on every such empty re-poll, and that O(#queues)
    // storm saturated the limiter (Vegas shrinks it under RTT pressure) — so a
    // freshly-woken REAL delivery pop queued behind thousands of empty re-polls on
    // acquire(), a priority inversion whose wait grew LINEARLY with the queue count
    // (the multitenant discovery-latency regression). An empty ring is zero DB
    // work, so it must cost zero limiter/pool traffic. Only three things need the
    // DB: a candidate to serve (a push marked the ring), a due keyset reseed (§8
    // floor), or a stale deferral-config refresh (§6) — each gated by a cheap
    // in-memory predicate here. The quiet re-poll short-circuits WITHOUT ever
    // touching pop_vegas / the pool.
    let need_reseed = st.hotlist.reseed_due(qkey, group, now_ms, st.hotlist_reseed_ms);
    let need_cfg = !st.hotlist.cfg_fresh(qkey, now_ms, HOTLIST_CFG_TTL_MS);
    if !need_reseed && !need_cfg && !st.hotlist.has_ready(qkey, group, now_ms) {
        let (b, c, m) = empty();
        return (b, c, m, Duration::ZERO);
    }

    // Real DB work ahead: NOW take the serving permit + a pooled connection. The
    // limiter thus only ever gates genuine serving + the periodic per-(queue,group)
    // reseed/cfg floor (O(served + #queues / reseed_interval)), never the
    // O(#queues) quiet re-poll storm.
    let tr = st.hotlist.traced(qkey);
    let t_start = if tr { crate::hotlist::trace_now_ms() } else { 0 };
    let permit = st.pop_vegas.acquire().await;
    let t_vegas = if tr { crate::hotlist::trace_now_ms() } else { 0 };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            drop(permit);
            let (b, c, m) = empty();
            return (b, c, m, Duration::ZERO);
        }
    };
    if tr {
        eprintln!("[hlt] attempt q={} start={} vegas_wait={} pool_wait={}",
            queue, t_start, t_vegas - t_start, crate::hotlist::trace_now_ms() - t_vegas);
    }

    // Lazy deferral-config refresh (§6), TTL-throttled.
    if need_cfg {
        if let Ok((d, w, mpw)) = db::queue_defer_cfg(&client, queue, tenant).await {
            st.hotlist.set_queue_cfg(qkey, d, w, mpw, now_ms);
        }
    }

    // Take candidates; reseed (bounded, throttled) if the ring is cold.
    //
    // Budget-aware claim (2026-07-31): take_batch stops claiming once the ring's
    // own mark estimate covers `batch`, so under a deep backlog a serve holds 1-2
    // partitions instead of a fixed over-claim. `k` is only the sparse-ring cap -
    // the old `max_parts x 8, clamp(16, 256)` held 16-80 partitions INFLIGHT for
    // the serve's whole SQL leg and capped a 100-partition T1 ring at ~6
    // concurrent serves (measured: 806 serves/s x 863 msg avg = the entire ~700k
    // pop ceiling; July's pre-hotlist SQL claim held ONE partition per pop).
    let k = ((max_parts.max(1) as usize) * 2).clamp(2, 64);
    let want = batch.max(1) as u32;
    let mut cands = st.hotlist.take_batch(qkey, group, k, want, now_ms);
    if cands.is_empty() && need_reseed {
        hotlist_reseed_scan(&st.hotlist, &client, qkey, group, now_ms).await;
        cands = st.hotlist.take_batch(qkey, group, k, want, now_ms);
    }
    if cands.is_empty() {
        drop(client);
        drop(permit);
        let (b, c, m) = empty();
        return (b, c, m, Duration::ZERO);
    }

    let names: Vec<String> = cands.iter().map(|c| c.name.clone()).collect();
    let skip_window = st.hotlist.skip_window(qkey);
    let lease_ms = lease_seconds.max(1) as i64 * 1000;
    // Cancellation-safety: take_batch marked these candidates INFLIGHT. If this
    // pop future is DROPPED between here and checkin (a client disconnect mid
    // pop_list — axum drops the handler future at the next await), nothing would
    // ever re-append them: a mark / promote_ack / reseed on an INFLIGHT entry is a
    // no-op (reseed only re-adds IDLE/INFLIGHT via the floor, but the floor is
    // gated on an empty ring), so the partitions would be stranded out of the ring
    // permanently. This guard re-appends them (Requeue) on an un-disarmed drop; the
    // normal completion / DB-error paths disarm it and run the real checkin.
    let mut guard = InflightGuard {
        hl: st.hotlist.clone(),
        qkey: qkey.to_string(),
        group: group.to_string(),
        cands,
        now_ms,
        auto_ack,
        lease_ms,
        armed: true,
    };
    let cancel_token = client.cancel_token();
    let t0 = Instant::now();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::pop_list(
            &client, queue, group, &names, batch, lease_seconds, worker, auto_ack, max_parts,
            sub_mode, sub_from, skip_window, tenant,
        ),
    )
    .await;
    let rtt = t0.elapsed();
    if tr {
        eprintln!("[hlt] sqldone q={} cands={} sql_ms={}", queue, names.len(), rtt.as_millis());
    }
    st.pop_vegas.record(rtt);
    drop(permit);
    // The await returned (not dropped) — take ownership of the candidates back and
    // disarm the guard; every path below runs an explicit checkin.
    guard.armed = false;
    let cands = std::mem::take(&mut guard.cands);

    let (meta_txt, blobs, states_txt) =
        match db::resolve_query_timeout(res, client, cancel_token, "pop_list", &st.metrics) {
            Some(t) => t,
            None => {
                // DB error/timeout — the candidates were checked out (INFLIGHT).
                // Re-append them all (Requeue) so nothing is stranded, then park.
                let back: Vec<crate::hotlist::CheckinResult> = cands
                    .iter()
                    .map(|c| crate::hotlist::CheckinResult {
                        name: c.name.clone(),
                        epoch: c.epoch,
                        verdict: crate::hotlist::Verdict::Requeue,
                        drained: false,
                    })
                    .collect();
                st.hotlist
                    .checkin(qkey, group, back, now_ms, auto_ack, lease_ms);
                let (b, c, m) = empty();
                return (b, c, m, rtt);
            }
        };

    // Map the tri-state verdicts back to the candidates we sent (§7).
    // Parse the served partitions FIRST: the checkin needs per-partition
    // batch_end (last served offset) to pair with the claim-time lastOff.
    let parsed: PopResult = match serde_json::from_str(&meta_txt) {
        Ok(p) => p,
        Err(_) => {
            let (b, c, m) = empty();
            return (b, c, m, rtt);
        }
    };
    let mut batch_ends: HashMap<&str, i64> = HashMap::with_capacity(parsed.partitions.len());
    for part in &parsed.partitions {
        let mut end: i64 = -1;
        for seg in &part.segments {
            let e = seg.seq + seg.start_off.max(0) as i64 + seg.take.max(0) as i64 - 1;
            if e > end {
                end = e;
            }
        }
        if end >= 0 {
            batch_ends.insert(part.partition.as_str(), end);
        }
    }
    let states: Vec<ListState> = serde_json::from_str(&states_txt).unwrap_or_default();
    let mut vmap: HashMap<&str, &ListState> = HashMap::with_capacity(states.len());
    for s in &states {
        vmap.insert(s.p.as_str(), s);
    }
    let results: Vec<crate::hotlist::CheckinResult> = cands
        .iter()
        .map(|c| {
            let verdict = match vmap.get(c.name.as_str()) {
                Some(s) => match s.s.as_str() {
                    "took" => crate::hotlist::Verdict::Took,
                    "leased" => {
                        let until = s
                            .until
                            .as_deref()
                            .and_then(crate::util::parse_iso_ms)
                            .unwrap_or(now_ms + lease_seconds.max(1) as i64 * 1000);
                        crate::hotlist::Verdict::Leased(until)
                    }
                    _ => crate::hotlist::Verdict::Empty,
                },
                // Not evaluated by the SQL (budget / partition-cap) — re-append.
                None => crate::hotlist::Verdict::Requeue,
            };
            // drained (spec §2): the claim exhausted the visible backlog when the
            // last served offset reached the claim-time allocator watermark. A
            // push committing after the SQL read keeps batch_count>0 via its own
            // mark, so a stale-low lastOff can never cause a wrong clear.
            let drained = match (&verdict, vmap.get(c.name.as_str())) {
                (crate::hotlist::Verdict::Took, Some(s)) => match (s.last_off, batch_ends.get(c.name.as_str())) {
                    (Some(lo), Some(&be)) => be >= lo,
                    _ => false,
                },
                _ => false,
            };
            crate::hotlist::CheckinResult {
                name: c.name.clone(),
                epoch: c.epoch,
                verdict,
                drained,
            }
        })
        .collect();
    st.hotlist
        .checkin(qkey, group, results, now_ms, auto_ack, lease_ms);

    for part in &parsed.partitions {
        st.hotlist
            .note_partition_id(qkey, &part.partition, &part.partition_id);
    }
    let (body, count, meta) =
        render_pop_parts(&parsed.partitions, Some(&blobs), queue, group, lease_id, &st.encryption);
    (body, count, meta, rtt)
}

// 19-wildcard-hotlist §8: keyset-paginated reseed. Walk the (queue, group)'s
// probably-pending partitions in id order (bounded ~10k pages), interning each
// name + remembering its id and marking it into the ring, then stamp the reseed
// clock. This is the cold-start populator AND the correctness floor for any
// missed mark / dropped mesh hint. Errors abandon the walk (the next attempt
// retries) — the ring simply stays as-is, never wrong.
pub(crate) async fn hotlist_reseed_scan(
    hl: &crate::hotlist::HotList,
    client: &deadpool_postgres::Client,
    // Track B (§5): ONE composite key in, split here into the (tenant, queue) the
    // keyset scan binds to SQL and the ring key it feeds back. Taking the pair as a
    // single argument is what lets the background floor reseed a ring under its own
    // tenant without a tenant registry to enumerate.
    qkey: &str,
    group: &str,
    now_ms: i64,
) {
    let (tenant, queue) = split_tenant_queue(qkey);
    let mut after = NIL_UUID.to_string();
    for _ in 0..HOTLIST_RESEED_MAX_PAGES {
        let rows = match db::hotlist_reseed(client, queue, group, &after, HOTLIST_RESEED_PAGE, tenant).await
        {
            Ok(r) => r,
            Err(_) => break,
        };
        if rows.is_empty() {
            break;
        }
        for (id, name) in &rows {
            hl.reseed_row(qkey, group, id, name, now_ms);
        }
        after = rows.last().map(|(id, _)| id.clone()).unwrap_or(after);
        if rows.len() < HOTLIST_RESEED_PAGE as usize {
            break;
        }
    }
    hl.reseed_done(qkey, group, now_ms);
}

// GET /api/v1/pop/queue/:queue/partition/:partition — pop from ONE named
// partition. Same query params + long-poll + lease/leaseId semantics as the
// wildcard path; only the SP call and response adapter differ (single-partition
// shape). `partitions` is ignored here (a specific pop is one partition).
pub async fn handle_pop_partition(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path((queue, partition)): Path<(String, String)>,
    Query(p): Query<PopParams>,
) -> Response {
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return json(StatusCode::NO_CONTENT, "{\"messages\":[],\"paused\":true}".to_string());
    }
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let sub_mode = p.subscription_mode.unwrap_or_else(|| "all".to_string());
    let sub_from = p.subscription_from.unwrap_or_default();
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    // RUSTFIX item 18: COALESCE(request.leaseSeconds, queue.lease_time, 60).
    let lease_seconds = match p.lease_seconds {
        Some(v) if v > 0 => v,
        _ => st.lease_time_for(&queue, tenant.as_str()).await,
    };
    // Track B (§5): the wake gate is keyed by (tenant, queue). Built once, outside
    // the long-poll loop.
    let qkey = tenant_queue_key(tenant.as_str(), &queue);

    let mut backoff_count: u32 = 0;
    loop {
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(permit);
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
            }
        };
        // Cancel token captured BEFORE issuing the query (see handle_pop): a
        // broker-side timeout cancels the running statement server-side and
        // quarantines this connection rather than abandoning it.
        let cancel_token = client.cancel_token();
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_specific(
                &client, &queue, &partition, &group, batch, lease_seconds, &worker,
                auto_ack, &sub_mode, &sub_from, tenant.as_str(),
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        drop(permit);
        // Spec §10 (parked long-poll): resolve_query_timeout releases the pooled
        // connection (drop on success/db-error, DETACH+cancel on timeout) BEFORE any
        // parking below — a parked pop must never pin a PG connection.
        let txt = match db::resolve_query_timeout(res, client, cancel_token, "pop_specific", &st.metrics) {
            Some(t) => t,
            None => {
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pop failed\"}".to_string())
            }
        };

        let lease_id: &str = if auto_ack { "" } else { &worker };
        let (body, count, meta) = build_pop_specific_response(&txt, &queue, &partition, &group, lease_id, &st.encryption);
        if count == 0 && wait && Instant::now() < deadline {
            // A push to any partition of this queue wakes us.
            // RUSTFIX item 19: exponentially-backing-off re-query; a push-wake resets it.
            backoff_count += 1;
            let interval = st.pop_backoff_interval(backoff_count);
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(interval);
            // Parked gauge: held for exactly the awaited window.
            let _parked = st.metrics.parked.enter(tenant.as_str(), &queue);
            if st.notifier.wait_queue(&qkey, waitd).await {
                backoff_count = 0;
            }
            continue;
        }
        // ACK REGISTRY: record this specific-partition pop's lease (no-op on autoAck).
        register_leases(&st, &group, &worker, lease_seconds, &meta);
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        // RUSTFIX item 24: per-queue pop throughput for queue_lag_metrics.
        if count > 0 {
            st.metrics.per_queue.add_pop(tenant.as_str(), &queue, count as u64);
            st.metrics
                .per_queue
                .add_pop_lag(tenant.as_str(), &queue, meta.lag_sum_ms, meta.lag_max_ms, meta.lag_n);
            for pid in &meta.partition_ids {
                st.remember_partition_queue(pid, &queue);
            }
        } else {
            st.metrics.per_queue.add_pop_empty(tenant.as_str(), &queue);
        }
        // autoAck advances the cursor server-side (no client ack round-trip), but it
        // IS an acknowledgement — count it so the ack throughput / completed totals
        // on the dashboard reflect auto-acked consumption too.
        if auto_ack && count > 0 {
            st.metrics.per_queue.add_ack(tenant.as_str(), &queue, count as u64, 0);
            st.metrics.ack.record_request(count);
            st.metrics
                .ack_success
                .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
        }
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

// Discovery pop params — same knobs as PopParams plus the namespace/task scope.
// This is the bare `GET /api/v1/pop` the clients issue for
// `client.queue().namespace_name(ns).consume(...)` (no queue in the path).
#[derive(Deserialize)]
pub struct PopDiscoverParams {
    batch: Option<i32>,
    partitions: Option<i32>,
    #[serde(rename = "autoAck")]
    auto_ack: Option<bool>,
    wait: Option<bool>,
    timeout: Option<u64>,
    // RUSTFIX item 18: per-request lease override; 0/absent lets each discovered
    // partition use its own queue's queues.lease_time (else 60).
    #[serde(rename = "leaseSeconds")]
    lease_seconds: Option<i32>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    namespace: Option<String>,
    task: Option<String>,
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
}

// GET /api/v1/pop?namespace=&task=&consumerGroup=... — namespace/task discovery
// pop. Resolves every segment queue whose queen.queues row matches the requested
// namespace/task and wildcard-pops across their partitions in one call
// (queen.log_pop_discover_wire_v1), returning the SAME response shape as
// handle_pop. Same long-poll + lease/leaseId semantics; ack/attempt work
// identically because the SP reuses the per-partition queen.log_pop_v1 path.
// At least one of namespace/task must be provided (the clients never send a bare
// pop without one — QueueBuilder.pop throws first — so a neither-provided call is
// a 400 rather than an unbounded scan of every queue).
pub async fn handle_pop_discover(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Query(p): Query<PopDiscoverParams>,
) -> Response {
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return json(StatusCode::NO_CONTENT, "{\"messages\":[],\"paused\":true}".to_string());
    }
    let namespace = p.namespace.unwrap_or_default();
    let task = p.task.unwrap_or_default();
    if namespace.is_empty() && task.is_empty() {
        return json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"namespace or task is required\",\"messages\":[]}".to_string(),
        );
    }
    let batch = p.batch.unwrap_or(200);
    let max_parts = p.partitions.unwrap_or(1);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let sub_mode = p.subscription_mode.unwrap_or_else(|| "all".to_string());
    let sub_from = p.subscription_from.unwrap_or_default();
    let worker = uuid_bytes_to_string(&uuidv7_bytes());
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    // No single queue to read a lease from: the SP leases each partition with its
    // own queue's configured lease_time; this is only the fallback for a matching
    // queue that has no configured queues.lease_time.
    // RUSTFIX item 18: pass the RAW request override (0 = none) so the discover SP
    // resolves COALESCE(NULLIF(request,0), sq.lease_time, 60) PER partition.
    let lease_seconds = p.lease_seconds.filter(|v| *v > 0).unwrap_or(0);

    let mut backoff_count: u32 = 0;
    loop {
        let permit = st.pop_vegas.acquire().await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(permit);
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
            }
        };
        // Cancel token captured BEFORE issuing the query (see handle_pop): a
        // broker-side timeout cancels the running statement server-side and
        // quarantines this connection rather than abandoning it.
        let cancel_token = client.cancel_token();
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_discover(
                &client, &namespace, &task, &group, batch, lease_seconds, &worker,
                auto_ack, max_parts, &sub_mode, &sub_from, tenant.as_str(),
            ),
        )
        .await;
        let rtt = t0.elapsed();
        st.pop_vegas.record(rtt);
        drop(permit);
        // Spec §10 (parked long-poll): resolve_query_timeout releases the pooled
        // connection (drop on success/db-error, DETACH+cancel on timeout) BEFORE any
        // parking below — a parked pop must never pin a PG connection.
        let txt = match db::resolve_query_timeout(res, client, cancel_token, "pop_discover", &st.metrics) {
            Some(t) => t,
            None => {
                return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pop failed\"}".to_string())
            }
        };

        let lease_id: &str = if auto_ack { "" } else { &worker };
        // Discovery spans queues, so there is no single top-level queue name; the
        // per-message JSON carries partitionId/leaseId/consumerGroup (all the ack
        // needs), and the top-level "queue" field is left empty. Per-queue lag /
        // cache attribution is skipped here for the same reason (acks on these
        // partitions attribute via the DB-lookup fallback).
        let (body, count, meta) =
            build_pop_response(&txt, None, "", &group, lease_id, &st.encryption);
        if count == 0 && wait && Instant::now() < deadline {
            // Discovery pops span queues -> this tenant's discovery gate, woken by
            // any push of ITS OWN (another tenant's push cannot satisfy this query,
            // which is tenant-scoped in SQL).
            // RUSTFIX item 19: exponentially-backing-off re-query; a push-wake resets it.
            backoff_count += 1;
            let interval = st.pop_backoff_interval(backoff_count);
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(interval);
            if st.notifier.wait_any(tenant.as_str(), waitd).await {
                backoff_count = 0;
            }
            continue;
        }
        // ACK REGISTRY: record the discovery pop's leases. lease_seconds is 0 here
        // (resolved per-partition in SQL); insert_lease uses a TTL fallback — the
        // TTL only bounds memory, PG lease validation guards correctness.
        register_leases(&st, &group, &worker, lease_seconds, &meta);
        st.metrics.pop.record_request(count);
        st.metrics.pop.record_batch(count, true, rtt);
        // autoAck advances the cursor server-side (no client ack round-trip), but it
        // IS an acknowledgement — count it so the ack throughput / completed totals
        // on the dashboard reflect auto-acked consumption too.
        if auto_ack && count > 0 {
            st.metrics.ack.record_request(count);
            st.metrics
                .ack_success
                .fetch_add(count as u64, std::sync::atomic::Ordering::Relaxed);
        }
        return json(if count == 0 { StatusCode::NO_CONTENT } else { StatusCode::OK }, body);
    }
}

// Delivery-side observations gathered while rendering a pop response: the
// per-message age at delivery ("pop lag" — dashboard Time-lag / Prometheus
// queen_queue_pop_lag_milliseconds) and the partitionIds delivered (which seed
// the AppState partition->queue memo used for ack attribution).
#[derive(Default)]
pub(crate) struct PopMeta {
    pub lag_sum_ms: u64,
    pub lag_max_ms: u64,
    pub lag_n: u64,
    pub partition_ids: Vec<String>,
    // ACK REGISTRY lease records for this pop — one per partition that delivered
    // a LEASED (non-autoAck) batch. Empty on autoAck renders (leaseId == ""). The
    // handler feeds these to st.ack_registry.insert_lease after rendering.
    pub leases: Vec<LeaseInsert>,
}

// One partition's leased batch, captured during render (the txns are in hand as
// frames are unpacked): partition_id, the batch's end offset (= the value
// log_pop_v1 wrote to log_consumers.batch_end), and the DISTINCT-collapsible
// xxh3_128 txn hashes of the delivered frames (frame order; the registry dedups).
pub(crate) struct LeaseInsert {
    pub partition_id: String,
    pub batch_end: i64,
    pub hashes: Vec<u128>,
}

// Feed a leased pop's per-partition lease records into the ACK REGISTRY so a
// later full-batch completed ack resolves to one positional cursor advance
// (queen.log_ack_at_v1) instead of per-ack hash resolution. No-op when there are
// no leases (autoAck, or an empty/partial render) — the registry is disabled or
// the batch simply defers to the SQL ack path.
fn register_leases(st: &Arc<AppState>, group: &str, worker: &str, lease_seconds: i32, meta: &PopMeta) {
    if meta.leases.is_empty() {
        return;
    }
    let now_ms = crate::util::now_epoch_ms();
    for lz in &meta.leases {
        st.ack_registry.insert_lease(
            &lz.partition_id,
            group,
            worker,
            lz.batch_end,
            &lz.hashes,
            lease_seconds,
            now_ms,
        );
    }
}

fn pop_error_body(e: &str) -> (String, usize, PopMeta) {
    let mut out = String::from("{\"success\":false,\"error\":\"");
    json_escape_into(&mut out, e);
    out.push_str("\",\"messages\":[]}");
    (out, 0, PopMeta::default())
}

// Wildcard pop response: SP result is {"partitions":[{partition,partitionId,segments}]}.
// `bin_blobs`: Some on the bin_v1 path — native bytea[] blobs aligned with the
// meta's segment traversal order (base64 `blob` fields absent from the JSON).
fn build_pop_response(
    txt: &str,
    bin_blobs: Option<&[Vec<u8>]>,
    queue: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
) -> (String, usize, PopMeta) {
    let parsed: PopResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => return pop_error_body("parse"),
    };
    if let Some(e) = parsed.error {
        return pop_error_body(&e);
    }
    render_pop_parts(&parsed.partitions, bin_blobs, queue, group, lease_id, enc)
}

// Specific-partition pop response: SP result is single-partition shaped
// ({"segments":[...],"partitionId":..}) with no partition NAME — the broker
// supplies the name from the request path. Adapts it to the same per-partition
// structure the wildcard renderer consumes so every message emits the identical
// per-message JSON and top-level fields.
fn build_pop_specific_response(
    txt: &str,
    queue: &str,
    partition: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
) -> (String, usize, PopMeta) {
    let parsed: PopSpecificResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => return pop_error_body("parse"),
    };
    if let Some(e) = parsed.error {
        return pop_error_body(&e);
    }
    let part = PopPart {
        partition: partition.to_string(),
        partition_id: parsed.partition_id,
        segments: parsed.segments,
    };
    render_pop_parts(std::slice::from_ref(&part), None, queue, group, lease_id, enc)
}

// Shared renderer: decode + slice each partition's segment frames into the
// wire per-message JSON, then wrap with the common top-level fields.
fn render_pop_parts(
    parts: &[PopPart],
    bin_blobs: Option<&[Vec<u8>]>,
    queue: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
) -> (String, usize, PopMeta) {
    let mut count = 0usize;
    let mut meta = PopMeta::default();
    let now_ms = crate::util::now_epoch_ms();
    // Running index into bin_blobs — the SP flattens blobs in exactly this
    // traversal order (partitions in claim order, segments in base_offset order).
    let mut blob_idx = 0usize;
    // The top-level partition/partitionId are the FIRST claimed partition's —
    // known up front, so the whole response renders into ONE buffer (the old
    // two-buffer shape re-copied the entire messages array at the end, and an
    // un-sized accumulator realloc'd its way through ~hundreds of KB per pop).
    let (first_name, first_id) = parts
        .first()
        .map(|p| (p.partition.as_str(), p.partition_id.as_str()))
        .unwrap_or(("", ""));
    // Capacity estimate: base64 is ~4/3 of the compressed blob; JSON adds fixed
    // per-message fields. Underestimates only cost one doubling; the per-segment
    // reserve below refines it with the real decompressed size.
    let mut est = 256 + queue.len() + group.len() + 2 * lease_id.len();
    for part in parts {
        for seg in &part.segments {
            est += seg.blob.len() + 256;
        }
    }
    if let Some(bs) = bin_blobs {
        for b in bs {
            est += b.len() * 2;
        }
    }
    let mut out = String::with_capacity(est);
    out.push_str("{\"success\":true,\"queue\":\"");
    json_escape_into(&mut out, queue);
    out.push_str("\",\"partition\":\"");
    json_escape_into(&mut out, first_name);
    out.push_str("\",\"partitionId\":\"");
    json_escape_into(&mut out, first_id);
    out.push_str("\",\"leaseId\":\"");
    json_escape_into(&mut out, lease_id);
    out.push_str("\",\"consumerGroup\":\"");
    json_escape_into(&mut out, group);
    out.push_str("\",\"messages\":[");
    // ACK REGISTRY: collect per-partition lease records only for LEASED pops (a
    // non-empty leaseId == the worker). autoAck renders (leaseId "") carry no
    // lease and skip this entirely.
    let collect_leases = !lease_id.is_empty();
    for part in parts {
        if !part.partition_id.is_empty() {
            meta.partition_ids.push(part.partition_id.clone());
        }
        // Pre-compute (from segment metadata, before rendering) the leased
        // batch's end offset and its expected frame count. batch_end is the
        // last (highest-base) delivered segment's seq + startOff + take - 1 —
        // identical to the v_last that log_pop_v1 wrote to log_consumers.batch_end
        // (§6: v_last = base + start_idx + take - 1 for every emitted row, last
        // wins). The hashes of the delivered frames are gathered in the loop
        // below; the lease is registered only if EVERY expected frame actually
        // rendered (a skipped/partial/undecodable segment leaves rendered <
        // expected, and the batch defers to the unchanged SQL ack path).
        let mut part_expected: i64 = 0;
        let mut part_batch_end: i64 = i64::MIN;
        if collect_leases {
            for seg in &part.segments {
                let t = seg.take.max(0) as i64;
                part_expected += t;
                if t > 0 {
                    let be = seg.seq + seg.start_off.max(0) as i64 + t - 1;
                    if be > part_batch_end {
                        part_batch_end = be;
                    }
                }
            }
        }
        let mut part_hashes: Vec<u128> = if collect_leases {
            Vec::with_capacity(part_expected.max(0) as usize)
        } else {
            Vec::new()
        };
        for seg in &part.segments {
            // Pop lag: message age at delivery. All frames of a segment share the
            // segment's createdAt (one push call), so parse it once per segment.
            let seg_age_ms: Option<u64> = crate::util::parse_iso_ms(&seg.created_at)
                .map(|c| (now_ms - c).max(0) as u64);
            // bin path: the blob arrives as native bytes, positionally aligned.
            // wire path: Postgres encode(...,'base64') wraps lines at 76 cols —
            // strip whitespace before decoding (STANDARD rejects non-alphabet bytes).
            let decoded: Vec<u8>;
            let blob: &[u8] = if let Some(bs) = bin_blobs {
                let Some(b) = bs.get(blob_idx) else { continue };
                blob_idx += 1;
                b
            } else {
                let mut b64: Vec<u8> = Vec::with_capacity(seg.blob.len());
                b64.extend(seg.blob.bytes().filter(|b| !b.is_ascii_whitespace()));
                decoded = match base64::engine::general_purpose::STANDARD.decode(&b64) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                &decoded
            };
            let raw = zstd_decompress(blob);
            // Borrowed frames: no per-frame String/Vec allocations. The owned
            // fallback only fires on invalid UTF-8 in txn/psub, which this engine
            // never writes (kept for parity with the old lossy behavior).
            let owned_frames: Vec<crate::frames::FrameOut>;
            let frames: Vec<crate::frames::FrameRef> = match crate::frames::unpack_frames_ref(&raw)
            {
                Some(f) => f,
                None => {
                    owned_frames = match unpack_frames(&raw) {
                        Some(f) => f,
                        None => continue,
                    };
                    owned_frames
                        .iter()
                        .map(|f| crate::frames::FrameRef {
                            message_id: uuid_string_to_bytes(&f.message_id).unwrap_or([0; 16]),
                            txn: &f.txn,
                            trace_id: f
                                .trace_id
                                .as_deref()
                                .and_then(uuid_string_to_bytes),
                            producer_sub: f.producer_sub.as_deref(),
                            payload: &f.payload,
                            encrypted: f.encrypted,
                        })
                        .collect()
                }
            };
            let start = seg.start_off.max(0) as usize;
            let take = seg.take.max(0) as usize;
            let end = (start + take).min(frames.len());
            // Refine capacity with the real decompressed size: payload bytes are
            // spliced verbatim, plus ~192B of fixed JSON fields per message.
            out.reserve(raw.len() + (end.saturating_sub(start)) * 192);
            for f in frames.iter().take(end).skip(start) {
                if count > 0 {
                    out.push(',');
                }
                out.push_str("{\"id\":\"");
                crate::frames::uuid_hex_into(&mut out, &f.message_id);
                out.push_str("\",\"transactionId\":\"");
                json_escape_into(&mut out, f.txn);
                out.push_str("\",\"traceId\":");
                match &f.trace_id {
                    Some(t) => {
                        out.push('"');
                        crate::frames::uuid_hex_into(&mut out, t);
                        out.push('"');
                    }
                    None => out.push_str("null"),
                }
                out.push_str(",\"data\":");
                if f.payload.is_empty() {
                    out.push_str("null");
                } else if let Some(pt) = enc.decrypt_payload_bytes(f.payload) {
                    // RUSTFIX item 8: decrypted envelope -> plaintext JSON. The sniff
                    // is by envelope shape (not FLAG_ENCRYPTED), so migrated v0.16.0
                    // messages decrypt too. Disabled encryption early-returns None.
                    push_utf8(&mut out, &pt);
                } else {
                    // raw splice: plaintext JSON, or an envelope we could not decrypt
                    // (served as-is — C++ swallows decrypt failures).
                    push_utf8(&mut out, f.payload);
                }
                out.push_str(",\"producerSub\":");
                match &f.producer_sub {
                    Some(ps) => {
                        out.push('"');
                        json_escape_into(&mut out, ps);
                        out.push('"');
                    }
                    None => out.push_str("null"),
                }
                out.push_str(",\"createdAt\":\"");
                json_escape_into(&mut out, &seg.created_at);
                out.push_str("\",\"partitionId\":\"");
                json_escape_into(&mut out, &part.partition_id);
                out.push_str("\",\"partition\":\"");
                json_escape_into(&mut out, &part.partition);
                out.push_str("\",\"leaseId\":\"");
                json_escape_into(&mut out, lease_id);
                out.push_str("\",\"consumerGroup\":\"");
                json_escape_into(&mut out, group);
                out.push_str("\"}");
                count += 1;
                // ACK REGISTRY: fingerprint the delivered txn (~50ns) while it is
                // in hand — same xxh3_128 the ack path recomputes from the wire
                // txn, so the sets compare exactly.
                if collect_leases {
                    part_hashes.push(u128::from_be_bytes(crate::util::txn_hash128(f.txn)));
                }
                if let Some(age) = seg_age_ms {
                    meta.lag_sum_ms += age;
                    meta.lag_max_ms = meta.lag_max_ms.max(age);
                    meta.lag_n += 1;
                }
            }
        }
        // Register the lease only when every leased frame rendered (rendered ==
        // expected) and the partition id is known; otherwise the batch defers to
        // the SQL ack path (sound — a partial delivery can't be fast-path acked).
        if collect_leases
            && part_expected > 0
            && part_batch_end != i64::MIN
            && part_hashes.len() as i64 == part_expected
            && !part.partition_id.is_empty()
        {
            meta.leases.push(LeaseInsert {
                partition_id: part.partition_id.clone(),
                batch_end: part_batch_end,
                hashes: part_hashes,
            });
        }
    }
    out.push_str("],\"partitionsClaimed\":");
    out.push_str(&parts.len().to_string());
    out.push('}');
    (out, count, meta)
}

// Append raw bytes that are expected to be valid UTF-8 (payloads stored from
// client JSON). std's from_utf8 validation is markedly cheaper than the lossy
// chunk iterator; invalid bytes fall back to lossy replacement.
fn push_utf8(out: &mut String, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(s) => out.push_str(s),
        Err(_) => out.push_str(&String::from_utf8_lossy(bytes)),
    }
}

// ------------------------------------------------------------------- ack
// Wire contract (matches the C++ broker + JS client):
//   POST /api/v1/ack        {transactionId, partitionId, status, consumerGroup?, leaseId?}
//   POST /api/v1/ack/batch  {consumerGroup?, acknowledgments:[{transactionId, partitionId, status, leaseId?}]}
// Response is a TOP-LEVEL ARRAY, one element per ack, in request order:
//   [{index, transactionId, success, error, leaseReleased, dlq}]
#[derive(Deserialize)]
struct AckSingle {
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    // Failure reason for a nack (status:'failed'); recorded on the DLQ row when
    // this nack exhausts the retry budget on a DLQ-enabled queue.
    error: Option<String>,
}

#[derive(Deserialize)]
struct AckBatchItem {
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    error: Option<String>,
}

#[derive(Deserialize)]
struct AckBatch {
    #[serde(default)]
    acknowledgments: Vec<AckBatchItem>,
    #[serde(rename = "consumerGroup")]
    consumer_group: Option<String>,
}

// One normalized ack: original request index + resolution inputs.
struct Ack {
    txn: String,
    partition_id: String,
    worker: String,
    // RUSTFIX item 10: the normalized outcome (completed|failed|retry|dlq) threaded
    // through to queen.log_ack_by_hash_v1 so retry/dlq survive to SQL.
    status: &'static str,
    // Nack failure reason, threaded into the DLQ snapshot when retries exhaust.
    error: Option<String>,
}

// Lowercase hex of a 16-byte txn hash — the token format queen.log_ack_by_hash_v1
// returns in noopHashes/staleHashes (PG encode(..,'hex') is lowercase) and the
// 32-hex-per-frame stride the transaction wire's hashesHex/acks[].h carry.
fn hex16(b: &[u8; 16]) -> String {
    let mut s = String::with_capacity(32);
    for x in b {
        use std::fmt::Write as _;
        let _ = write!(s, "{:02x}", x);
    }
    s
}

pub async fn handle_ack(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let a: AckSingle = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let group = a.consumer_group.clone().unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let acks = vec![Ack {
        txn: a.transaction_id.unwrap_or_default(),
        partition_id: a.partition_id.unwrap_or_default(),
        worker: a.lease_id.unwrap_or_default(),
        status: normalize_ack_status(a.status.as_deref()),
        error: a.error.filter(|s| !s.is_empty()),
    }];
    let body = process_acks(&st, &group, acks, tenant.as_str()).await;
    json(StatusCode::OK, body)
}

pub async fn handle_ack_batch(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let b: AckBatch = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let group = b.consumer_group.clone().unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let acks: Vec<Ack> = b
        .acknowledgments
        .into_iter()
        .map(|it| Ack {
            txn: it.transaction_id.unwrap_or_default(),
            partition_id: it.partition_id.unwrap_or_default(),
            worker: it.lease_id.unwrap_or_default(),
            status: normalize_ack_status(it.status.as_deref()),
            error: it.error.filter(|s| !s.is_empty()),
        })
        .collect();
    let body = process_acks(&st, &group, acks, tenant.as_str()).await;
    json(StatusCode::OK, body)
}

// Outcome of the ack-registry fast path for one (partition, worker) group: the
// cursor advance either committed durably (render success), needs the SQL
// fall-back (a stale/rejected lease — resolve the true outcome), or the whole ack
// flush failed (error the client, lease expiry redelivers). Committed/FallBack
// map 1:1 to the pre-fusion synchronous path's ok/ok:false; FlushErr is the new
// ack-fusion whole-flush-failure branch.
enum FastAck {
    Committed,
    FallBack,
    FlushErr,
}

// Resolve acks per (partition, worker) via queen.log_ack_by_hash_v1, then emit
// the per-item result array in the original order. The broker computes the
// xxh3_128 txn hashes (spec §3 — SQL never sees txn strings) and maps the SP's
// noopHashes/staleHashes hex tokens back to the request's txns.
async fn process_acks(st: &Arc<AppState>, group: &str, acks: Vec<Ack>, tenant: &str) -> String {
    let n = acks.len();
    let mut success = vec![false; n];
    let mut errors: Vec<Option<String>> = vec![None; n];
    let mut lease_released = vec![false; n];
    let mut dlq_flags = vec![false; n];
    let mut noop_flags = vec![false; n];

    // Group item indices by (partition_id, worker): one log_ack_by_hash_v1 call each.
    let mut groups: HashMap<(String, String), Vec<usize>> = HashMap::new();
    for (i, a) in acks.iter().enumerate() {
        groups
            .entry((a.partition_id.clone(), a.worker.clone()))
            .or_default()
            .push(i);
    }

    // ACK FUSION: with the flag ON, a registry HIT enqueues into the fusion
    // pipeline (server/src/ack_fusion.rs) and the response resolves on the flush
    // commit, so N positional cursor advances collapse to one
    // queen.log_ack_multi_v1 transaction. With it OFF the fast path is the
    // unchanged synchronous log_ack_at_v1 call (byte-identical).
    let fusion_on = st.ack_fusion.enabled();

    // Shared pooled client for the SQL ack paths (queue-name DB miss, the flag-off
    // ack_at fast path, ack_by_hash, dlq). With fusion OFF it is acquired ONCE up
    // front and held across the loop exactly as before (a pool failure fails every
    // item). With fusion ON it is acquired lazily and NEVER held across a fusion
    // commit-wait: parking on the flush while pinning a pool connection would
    // deadlock the flush task (which needs its own connection from the same pool)
    // once more acks park than the pool is deep.
    let mut client: Option<deadpool_postgres::Client> = None;
    if !fusion_on {
        match st.pool.get().await {
            Ok(c) => client = Some(c),
            Err(_) => {
                st.metrics.record_db_error();
                for e in errors.iter_mut() {
                    *e = Some("pool".to_string());
                }
                return render_ack_results(&acks, &success, &errors, &lease_released, &dlq_flags, &noop_flags);
            }
        }
    }

    for ((pid, worker), idxs) in groups {
        // Track B (§5) OWNERSHIP GATE: the ack wire is partitionId-keyed and the
        // pid-addressed ack SPs (log_ack_at_v1/log_ack_by_hash_v1) carry no tenant,
        // so a forged/leaked pid could otherwise advance ANOTHER tenant's cursor.
        // Verify the pid belongs to the request tenant first; a foreign pid is
        // rejected (every item in the group fails, cursor untouched). Vacuous +
        // zero-cost when tenancy is off (no pool.get, byte-identical OSS path).
        if st.tenancy_enabled {
            // Confirmed-ownership cache hit ⇒ no pooled connection, no query (the
            // common case: a consumer acks the same partitions repeatedly). Only a
            // miss (first ack of a partition, or a forged/foreign pid) pays the DB
            // round trip — which was ~0.784 commits per delivered message before
            // this cache, ~23% of the cloud path's transactions.
            let owned = if st.tenant_owns_partition_cached(&pid, tenant) {
                true
            } else {
                match st.pool.get().await {
                    Ok(c) => st.tenant_owns_partition(&c, &pid, tenant).await,
                    Err(_) => false, // deny-by-default: never open a hole on a pool error
                }
            };
            if !owned {
                for &i in &idxs {
                    errors[i] = Some("partition not owned by tenant".to_string());
                }
                continue;
            }
        }

        // Per-queue ack attribution (queue_lag_metrics ack_* columns): the ack wire
        // is partitionId-keyed, so resolve the queue via the pop-fed memo. On the
        // fusion path use the memo-only lookup (no client) so nothing is held
        // across the flush-wait; a rare ack-first miss takes a short-lived client
        // dropped right here. With fusion OFF the shared client is already held.
        let queue_name = if fusion_on {
            match st.partition_queue_memo(&pid) {
                Some(q) => Some(q),
                None => match st.pool.get().await {
                    Ok(c) => st.queue_for_partition(&c, &pid).await,
                    Err(_) => None,
                },
            }
        } else {
            st.queue_for_partition(client.as_ref().unwrap(), &pid).await
        };

        // ---- ACK REGISTRY fast path ----
        // Fire ONLY when every item in this (partition, worker) group is
        // `completed` and the group carries a leaseId. A registry HIT (worker
        // matches AND the acked txn set EXACTLY covers the leased batch) collapses
        // the whole batch to ONE positional cursor advance (queen.log_ack_at_v1),
        // skipping the log_ack_by_hash_v1 hash resolution. log_ack_at_v1 still
        // re-validates the lease under the consumer row lock, so a stale HIT (lease
        // expired/reassigned between pop and ack) returns ok:false and we fall
        // through to the SQL path — the registry can never grant an ack PG refuses.
        // ANY other case (a non-completed status, a partial/extra set, a worker
        // mismatch, an unknown/evicted/expired entry) never reaches here and takes
        // the verbatim SQL path below, so below-cursor honesty, the retry budget,
        // and DLQ handoff are preserved by construction.
        if !worker.is_empty() && idxs.iter().all(|&i| acks[i].status == "completed") {
            let acked: Vec<u128> = idxs
                .iter()
                .map(|&i| u128::from_be_bytes(crate::util::txn_hash128(&acks[i].txn)))
                .collect();
            if let Some(batch_end) =
                st.ack_registry.take_if_full_batch(&pid, group, &worker, &acked)
            {
                // A HIT proved (worker matches, whole batch completed, upto ==
                // batch_end). ACK FUSION coalesces the positional advance into one
                // multi-cursor commit; the synchronous path (flag off) does the
                // single log_ack_at_v1 call. Either way the effects below run ONLY
                // on committed evidence.
                let outcome: FastAck = if fusion_on {
                    // Enqueue + PARK on the flush commit (no pooled client held).
                    match st
                        .ack_fusion
                        .ack(
                            pid.clone(),
                            group.to_string(),
                            worker.clone(),
                            batch_end,
                            idxs.len() as i32,
                        )
                        .await
                    {
                        crate::ack_fusion::AckVerdict::Committed => FastAck::Committed,
                        crate::ack_fusion::AckVerdict::Rejected => FastAck::FallBack,
                        crate::ack_fusion::AckVerdict::FlushErr => FastAck::FlushErr,
                    }
                } else {
                    // Unchanged synchronous fast path (byte-identical to before).
                    let hit_ok = match db::ack_at(
                        client.as_ref().unwrap(),
                        &pid,
                        group,
                        &worker,
                        batch_end,
                        true,
                        idxs.len() as i32,
                    )
                    .await
                    {
                        Ok(txt) => serde_json::from_str::<serde_json::Value>(&txt)
                            .ok()
                            .and_then(|v| v.get("ok").and_then(|x| x.as_bool()))
                            .unwrap_or(false),
                        Err(_) => false,
                    };
                    if hit_ok {
                        FastAck::Committed
                    } else {
                        FastAck::FallBack
                    }
                };

                match outcome {
                    FastAck::Committed => {
                        // Same per-item result shape as the SQL happy path.
                        for &i in &idxs {
                            success[i] = true;
                            lease_released[i] = true;
                        }
                        // Per-queue ack attribution (identical to the SQL-path tail).
                        if let Some(q) = queue_name.as_ref() {
                            let okc = idxs.len() as u64; // all completed on the fast path
                            st.metrics.per_queue.add_ack(tenant, q, okc, 0);
                            // 19-wildcard-hotlist §7: ack = lease released. If pushes
                            // arrived during the lease the entry is still pending —
                            // promote it to ready NOW + wake, so it is claimable
                            // immediately instead of at lease expiry. No-op if unknown.
                            if st.hotlist.enabled() {
                                // Track B (§5): the pid→queue memo yields a BARE name;
                                // pair it with this request's tenant to address the
                                // ring. One small alloc per (partition, worker) ack
                                // GROUP, not per message.
                                let qkey = tenant_queue_key(tenant, q);
                                if st.hotlist.traced(&qkey) {
                                    eprintln!("[hlt] promote q={} g={} t={}", q, group, crate::hotlist::trace_now_ms());
                                }
                                // covered=true: the registry cover test proved this
                                // ack completes the WHOLE leased batch — eligible for
                                // clear-su-ack when nothing arrived during the lease.
                                st.hotlist.promote_ack(&qkey, group, &pid, crate::util::now_epoch_ms(), true);
                            }
                        }
                        continue; // whole group handled — skip the SQL path
                    }
                    FastAck::FlushErr => {
                        // Whole ack flush failed (pool/infra/timeout/parse). Error
                        // the client; the lease expires and the batch redelivers
                        // (at-least-once, contract unchanged). The registry entry
                        // was already consumed, so do NOT retry via SQL here.
                        for &i in &idxs {
                            errors[i] = Some(
                                "ack flush failed; lease will expire and redeliver".to_string(),
                            );
                        }
                        continue;
                    }
                    FastAck::FallBack => {
                        // ok:false (e.g. expired/reassigned lease) → fall through to
                        // the SQL path, which resolves the true outcome. The entry
                        // was already consumed.
                    }
                }
            }
        }

        // SQL ack path (registry miss / partial / nack / DLQ / fusion fall-back).
        // It needs a pooled client; on the fusion path acquire one now (it was NOT
        // held across the enqueue) and release it at the end of this iteration so
        // the next group's fusion wait never pins it. With fusion OFF the shared
        // client is already held.
        if fusion_on && client.is_none() {
            match st.pool.get().await {
                Ok(c) => client = Some(c),
                Err(_) => {
                    st.metrics.record_db_error();
                    for &i in &idxs {
                        errors[i] = Some("pool".to_string());
                    }
                    continue;
                }
            }
        }

        // Any non-fast outcome resolves this lease's state via SQL (release,
        // partial advance, nack, DLQ), so drop our cached entry (worker-guarded)
        // to keep the registry from ever offering a stale HIT for this batch.
        st.ack_registry.evict(&pid, group, &worker);

        // RUSTFIX item 10 on the log wire: aligned hash/status arrays so
        // log_ack_by_hash_v1 distinguishes completed / failed / retry / dlq.
        // `hexes` keeps each item's 32-hex token for mapping the SP's
        // noopHashes/staleHashes back to request items.
        let mut hashes: Vec<Vec<u8>> = Vec::with_capacity(idxs.len());
        let mut statuses: Vec<String> = Vec::with_capacity(idxs.len());
        let mut hexes: Vec<String> = Vec::with_capacity(idxs.len());
        for &i in &idxs {
            let h = crate::util::txn_hash128(&acks[i].txn);
            hexes.push(hex16(&h));
            hashes.push(h.to_vec());
            statuses.push(acks[i].status.to_string());
        }

        match db::ack_by_hash(client.as_ref().unwrap(), &pid, group, &worker, &hashes, &statuses).await {
            Ok(txt) => {
                // {"ok":bool,"error":?,...}
                let v: serde_json::Value =
                    serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
                let sp_ok = v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false);
                if sp_ok {
                    // DLQ hand-off: the SP signalled that the head un-acked frame
                    // is poison (a forced 'dlq' status, or a nack whose retry
                    // budget is exhausted on a DLQ-enabled queue). It kept the
                    // lease held so we can decode the covering segment, snapshot
                    // the poison frame's payload, and file the queen.log_dlq row
                    // (log_dlq_head_v1 then advances the cursor + releases the
                    // lease). 'off' carries the poison OFFSET in this return —
                    // not the cursor (005_log_ack contract).
                    if v.get("dlq").and_then(|x| x.as_bool()).unwrap_or(false) {
                        let off = v.get("off").and_then(|x| x.as_i64()).unwrap_or(0);
                        match dlq_file_head(client.as_ref().unwrap(), &pid, group, &worker, off, &acks, &idxs)
                            .await
                        {
                            Ok(true) => {
                                for &i in &idxs {
                                    success[i] = true;
                                    lease_released[i] = true;
                                    dlq_flags[i] = true;
                                }
                            }
                            Ok(false) => {
                                for &i in &idxs {
                                    errors[i] = Some("dlq rejected".to_string());
                                }
                            }
                            Err(e) => {
                                let msg = e.to_string();
                                for &i in &idxs {
                                    errors[i] = Some(msg.clone());
                                }
                            }
                        }
                    } else {
                        for &i in &idxs {
                            success[i] = true;
                            lease_released[i] = true;
                        }
                    }
                    // Below-cursor honesty (ack-as-commit): the SP reports hashes
                    // whose position resolved below the committed cursor.
                    // Completed ones are harmless duplicate commits -> noop:true;
                    // explicit signals (failed/retry/dlq) can no longer be
                    // honored -> rejected per item instead of a silent ok. The
                    // wire tokens are 32-hex hashes — map them back to items via
                    // this group's aligned `hexes`.
                    let hash_set = |key: &str| -> std::collections::HashSet<String> {
                        v.get(key)
                            .and_then(|x| x.as_array())
                            .map(|a| {
                                a.iter()
                                    .filter_map(|s| s.as_str())
                                    .map(|s| s.to_ascii_lowercase())
                                    .collect()
                            })
                            .unwrap_or_default()
                    };
                    let noop_hashes = hash_set("noopHashes");
                    let stale_hashes = hash_set("staleHashes");
                    // Unresolvable honesty (2026-07-30): hashes the SP could
                    // not resolve at all (log_txns row purged, or a txn that
                    // never existed). The cursor did not move for them, so
                    // reporting them success would tell the client an ack
                    // landed that never did — the silent redelivery livelock.
                    let unresolved_hashes = hash_set("unresolvedHashes");
                    if !noop_hashes.is_empty() || !stale_hashes.is_empty()
                        || !unresolved_hashes.is_empty()
                    {
                        for (k, &i) in idxs.iter().enumerate() {
                            if unresolved_hashes.contains(&hexes[k]) {
                                success[i] = false;
                                dlq_flags[i] = false;
                                lease_released[i] = false;
                                errors[i] = Some(
                                    "unresolvable: transaction not in the ack window (hash purged or never pushed); if leased, the message redelivers"
                                        .to_string(),
                                );
                            } else if stale_hashes.contains(&hexes[k]) {
                                success[i] = false;
                                dlq_flags[i] = false;
                                errors[i] = Some(
                                    "already committed: the cursor moved past this message before this ack"
                                        .to_string(),
                                );
                            } else if noop_hashes.contains(&hexes[k]) {
                                noop_flags[i] = true;
                            }
                        }
                    }
                } else {
                    let err = v
                        .get("error")
                        .and_then(|x| x.as_str())
                        .unwrap_or("ack rejected")
                        .to_string();
                    for &i in &idxs {
                        errors[i] = Some(err.clone());
                    }
                }
            }
            Err(e) => {
                st.metrics.record_db_error();
                let msg = e.to_string();
                for &i in &idxs {
                    errors[i] = Some(msg.clone());
                }
            }
        }

        if let Some(q) = queue_name {
            // ack_success = accepted COMPLETIONS; everything else (nacks —
            // failed/retry/dlq — and rejected acks) counts as ack_failed,
            // matching the worker_metrics column comments ("Failed acks
            // (retries, errors)").
            let ok = idxs
                .iter()
                .filter(|&&i| success[i] && acks[i].status == "completed")
                .count() as u64;
            let failed = idxs.len() as u64 - ok;
            st.metrics.per_queue.add_ack(tenant, &q, ok, failed);
            // 19-wildcard-hotlist §7 promote-on-ack (SQL path): ANY ack that
            // RELEASED the lease makes the partition claimable again and must
            // promote the ring entry — not just a COMPLETED one. A NACK
            // (failed/retry) releases the lease and REDELIVERS the un-acked tail;
            // a budget-exhausted drop/DLQ releases it and exposes the backlog
            // behind the poison. The old `ok > 0` gate fired only on a completed
            // item, so a pure NACK (the retries test: handler throws → whole batch
            // failed, ok=0) left the ring's leased-Took entry stranded in the WHEEL
            // until lease expiry — up to leaseTime, 300s by default (the 300s
            // multitenant-retries stall; HOTLIST=0 was instant because the SQL
            // fallback re-discovers the released batch on the next scan).
            // promote_ack moves a WHEEL entry to ready + wakes, so the redelivery
            // is immediate (matching the legacy path). Over-firing is harmless: a
            // still-live lease (a partial completion that kept the lease) just
            // yields one empty SKIP-LOCKED probe → Leased verdict → re-wheeled at
            // the correct expiry ("stale in eccesso", §1/§7).
            let released = idxs.iter().any(|&i| lease_released[i]);
            if released && st.hotlist.enabled() {
                let qkey = tenant_queue_key(tenant, &q);
                if st.hotlist.traced(&qkey) {
                    eprintln!("[hlt] promote2 q={} g={} t={}", q, group, crate::hotlist::trace_now_ms());
                }
                // covered=false: this is the SQL-fallback release path (NACK,
                // partial, mixed) — coverage is unproven, so always promote; the
                // worst case is one spurious ~0.2ms probe on a rare path.
                st.hotlist.promote_ack(&qkey, group, &pid, crate::util::now_epoch_ms(), false);
            }
        }

        // Fusion path: release the pooled client so the NEXT group's fusion
        // commit-wait never pins it (the pool-deadlock guard). No-op if already
        // None; with fusion OFF the shared client is kept for the whole loop.
        if fusion_on {
            client = None;
        }
    }

    // Metrics: one ACK API call carrying N acknowledged items, split by outcome.
    // Mirrors the C++ WorkerMetrics ack counters that syscollect.rs flushes into
    // queen.worker_metrics (ack_request/message/success/failed + dlq). Same
    // completion-vs-nack split as the per-queue counters above.
    {
        use std::sync::atomic::Ordering::Relaxed;
        let ok = (0..n).filter(|&i| success[i] && acks[i].status == "completed").count() as u64;
        let dlq = dlq_flags.iter().filter(|&&d| d).count() as u64;
        st.metrics.ack.record_request(n);
        st.metrics.ack_success.fetch_add(ok, Relaxed);
        st.metrics.ack_failed.fetch_add((n as u64).saturating_sub(ok), Relaxed);
        st.metrics.dlq_moved.fetch_add(dlq, Relaxed);
    }

    // ACK REGISTRY: rate-limited hit-rate line ([ack-reg] hits/misses/rate), so a
    // run shows how much of the ack traffic took the positional fast path.
    st.ack_registry.maybe_report(crate::util::now_epoch_ms());

    render_ack_results(&acks, &success, &errors, &lease_released, &dlq_flags, &noop_flags)
}

// Decode the segment COVERING the poison offset, extract the poison frame
// (payload snapshot, txn, message_id), pick the failure reason from the matching
// nack, and file the queen.log_dlq row via queen.log_dlq_head_v1 (which advances
// the cursor past the frame + releases the lease). `off` is the absolute poison
// offset from the ack SP's dlq:true return; the frame index inside the covering
// blob is pure subtraction (off - base_offset).
// Ok(true) => filed, Ok(false) => couldn't extract / SP rejected.
async fn dlq_file_head(
    client: &deadpool_postgres::Client,
    partition_id: &str,
    group: &str,
    worker: &str,
    off: i64,
    acks: &[Ack],
    idxs: &[usize],
) -> Result<bool, tokio_postgres::Error> {
    let (payload, txn, message_id) = match db::log_segment_covering(client, partition_id, off).await?
    {
        Some((base, end, blob)) => {
            // log_segment_at_v1 returns the last segment with base <= off; a
            // retention gap can leave `off` past its end — nothing to snapshot.
            if off < base || off > end {
                return Ok(false);
            }
            let raw = zstd_decompress(&blob);
            match unpack_frames(&raw) {
                Some(frames) => match frames.get((off - base) as usize) {
                    Some(f) => {
                        let payload = if f.payload.is_empty() {
                            "null".to_string()
                        } else {
                            String::from_utf8_lossy(&f.payload).into_owned()
                        };
                        (payload, f.txn.clone(), f.message_id.clone())
                    }
                    None => return Ok(false),
                },
                None => return Ok(false),
            }
        }
        None => return Ok(false),
    };

    // Failure reason: the nacked ack whose txn matches the poison frame; else the
    // first nacked error in the group; else a default (v1's "Retries exhausted").
    let error = idxs
        .iter()
        .map(|&i| &acks[i])
        .filter(|a| a.status != "completed")
        .find(|a| a.txn == txn)
        .and_then(|a| a.error.clone())
        .or_else(|| {
            idxs.iter()
                .map(|&i| &acks[i])
                .filter(|a| a.status != "completed")
                .find_map(|a| a.error.clone())
        })
        .unwrap_or_else(|| "Retries exhausted".to_string());

    // db::seg_dlq_head keeps its pre-log signature: the position travels in the
    // `seq` argument (now the absolute offset); the frame_idx argument is
    // vestigial and ignored by the wrapper (pass 0).
    let res = db::seg_dlq_head(
        client, partition_id, group, worker, off, 0, &message_id, &txn, &payload, &error,
    )
    .await?;
    let v: serde_json::Value = serde_json::from_str(&res).unwrap_or(serde_json::Value::Null);
    Ok(v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false))
}

fn render_ack_results(
    acks: &[Ack],
    success: &[bool],
    errors: &[Option<String>],
    lease_released: &[bool],
    dlq_flags: &[bool],
    noop_flags: &[bool],
) -> String {
    let mut out = String::from("[");
    for (i, a) in acks.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        out.push_str(",\"transactionId\":\"");
        json_escape_into(&mut out, &a.txn);
        out.push_str("\",\"success\":");
        out.push_str(if success[i] { "true" } else { "false" });
        out.push_str(",\"error\":");
        match &errors[i] {
            Some(e) => {
                out.push('"');
                json_escape_into(&mut out, e);
                out.push('"');
            }
            None => out.push_str("null"),
        }
        out.push_str(",\"leaseReleased\":");
        out.push_str(if lease_released[i] { "true" } else { "false" });
        out.push_str(",\"dlq\":");
        out.push_str(if dlq_flags[i] { "true" } else { "false" });
        out.push_str(",\"noop\":");
        out.push_str(if noop_flags[i] { "true" } else { "false" });
        out.push('}');
    }
    out.push(']');
    out
}

// ---------------------------------------------------------------- lease/extend
// POST /api/v1/lease/:leaseId/extend  body {"seconds":60} (default 60).
// Renews every log_consumers lease held by :leaseId (= the worker id minted at
// pop) via queen.log_renew_lease_v1. Always HTTP 200 (best-effort renewal, like
// the rows engine). The response carries every key the clients read:
//   JS:  result.leaseId ? result.newExpiresAt : result.lease_expires_at
//   Go:  result["newExpiresAt"] (RFC3339 string)
#[derive(Deserialize)]
struct RenewBody {
    seconds: Option<i32>,
}

// Track B (§5) OWNERSHIP GATE for the renew above. lease/extend is addressed by
// the lease (= worker) id ALONE — bearer-token semantics — and
// queen.log_renew_lease_v1 carries no tenant, so a leaked/logged lease id would
// otherwise let one tenant keep another tenant's batch leased and delay its
// redelivery. Gate and renew in ONE statement so the check cannot race the renew:
// the SP is projected only when the one-time EXISTS proves a LIVE lease of that
// worker sits on a partition of the request tenant. The renew semantics
// (GREATEST, live-only, MIN expiry) stay in the SP — this only decides IF it
// runs. Worker ids are broker-minted uuidv7 (never client-supplied), so all lease
// rows of one worker belong to one tenant: proving one row proves the set.
// No row back ⇒ the unknown-lease body, so a foreign lease is indistinguishable
// from an expired or never-existing one (no existence leak).
const RENEW_OWNED_SQL: &str = "SELECT (queen.log_renew_lease_v1($1, $2::int))::text \
     WHERE EXISTS (SELECT 1 FROM queen.log_consumers c \
                   JOIN queen.log_partitions p ON p.id = c.partition_id \
                   JOIN queen.queues q ON q.id = p.queue_id \
                   WHERE c.worker_id = $1 \
                     AND q.tenant_id = $3::text::uuid \
                     AND c.lease_expires_at IS NOT NULL \
                     AND c.lease_expires_at > clock_timestamp())";

async fn renew_lease_owned(
    client: &deadpool_postgres::Client,
    lease_id: &str,
    seconds: i32,
    tenant: &str,
) -> Result<String, tokio_postgres::Error> {
    let stmt = client.prepare_cached(RENEW_OWNED_SQL).await?;
    let rows = client.query(&stmt, &[&lease_id, &seconds, &tenant]).await?;
    Ok(match rows.first() {
        Some(r) => r.get(0),
        None => "{\"renewed\":0,\"expiresAt\":null}".to_string(),
    })
}

pub async fn handle_lease_extend(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(lease_id): Path<String>,
    body: Bytes,
) -> Response {
    let seconds = if body.is_empty() {
        60
    } else {
        serde_json::from_slice::<RenewBody>(&body)
            .ok()
            .and_then(|b| b.seconds)
            .unwrap_or(60)
    };

    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string()),
    };

    // Tenancy OFF ⇒ the verbatim unscoped call, no gate query (an OSS broker has
    // only default-tenant queues, so the renew path stays byte-identical). ON ⇒
    // the gated statement; a DB error on it surfaces below as a 500 with nothing
    // renewed (deny-by-default — a transient failure never opens the gate).
    let renewed = if st.tenancy_enabled {
        renew_lease_owned(&client, &lease_id, seconds, tenant.as_str()).await
    } else {
        db::renew_lease(&client, &lease_id, seconds).await
    };

    match renewed {
        Ok(txt) => {
            // {"renewed":n,"expiresAt":iso|null}
            let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
            let renewed = v.get("renewed").and_then(|x| x.as_i64()).unwrap_or(0);
            let expires = v.get("expiresAt").and_then(|x| x.as_str());

            let mut out = String::from("{\"leaseId\":\"");
            json_escape_into(&mut out, &lease_id);
            out.push_str("\",\"success\":");
            out.push_str(if renewed > 0 { "true" } else { "false" });
            out.push_str(",\"renewed\":");
            out.push_str(&renewed.to_string());
            // Same value under the three keys different clients look for.
            for key in ["newExpiresAt", "expiresAt", "lease_expires_at"] {
                out.push_str(",\"");
                out.push_str(key);
                out.push_str("\":");
                match expires {
                    Some(e) => {
                        out.push('"');
                        json_escape_into(&mut out, e);
                        out.push('"');
                    }
                    None => out.push_str("null"),
                }
            }
            out.push('}');
            json(StatusCode::OK, out)
        }
        Err(e) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            json_err("renew failed: ", &e),
        ),
    }
}

// ------------------------------------------------------------------ transaction
// POST /api/v1/transaction — atomic multi-op push+ack through one call to
// queen.log_transaction_wire_v1. Body:
//   {"operations":[
//      {"type":"push","items":[{queue,partition?,payload,transactionId?,traceId?}]}
//      | {"type":"push",queue,payload,...}            (flat form)
//      | {"type":"ack",transactionId,partitionId,status,consumerGroup?,leaseId?}
//    ],
//    "requiredLeases":["<leaseId>",...]}
// Response mirrors the v1 transaction wire shape:
//   ok:    {transactionId, success:true, results:[per-op]}
//   fail:  {transactionId, success:false, error, results:[]}  (HTTP 200; the
//          SP RAISEs on rollback, surfacing as a DB error here).
struct TxnPushEcho {
    index: usize,
    txn: String,
    mid: String,
    queue: String,
    duplicate: bool,
}
struct TxnAckEcho {
    index: usize,
    txn: String,
}
struct TxnFrame {
    mid: [u8; 16],
    txn: String,
    trace: Option<[u8; 16]>,
    payload: Vec<u8>,
    // RUSTFIX item 8: set when payload was replaced with an encryption envelope.
    encrypted: bool,
}
struct TxnPushGroup {
    queue: String,
    partition: String,
    frames: Vec<TxnFrame>,
    // txn -> first message_id (intra-batch first-wins dedup, matching the C++
    // broker: a repeated txn in one (queue,partition) group would otherwise
    // trip log_push_one_v1's per-segment dedup and raise QDUP).
    seen: HashMap<String, String>,
}
// RUSTFIX item 10: one ack op inside a transaction. Carries the normalized status
// (completed|failed|retry|dlq) — NOT a bool — so retry/dlq survive to the SP, plus
// the nack error (for the DLQ reason) and the flat op index (to stamp the result).
struct TxnAckItem {
    txn: String,
    status: &'static str,
    error: Option<String>,
    index: usize,
}
struct TxnAckGroup {
    partition_id: String,
    group: String,
    worker: String,
    items: Vec<TxnAckItem>,
}

fn txn_add_push(
    item: &serde_json::Value,
    index: usize,
    groups: &mut Vec<TxnPushGroup>,
    group_of: &mut HashMap<(String, String), usize>,
    echoes: &mut Vec<TxnPushEcho>,
) {
    let queue = item.get("queue").and_then(|x| x.as_str()).unwrap_or("").to_string();
    let partition = item
        .get("partition")
        .and_then(|x| x.as_str())
        .unwrap_or("Default")
        .to_string();
    let payload = item
        .get("payload")
        .cloned()
        .or_else(|| item.get("data").cloned())
        .unwrap_or_else(|| serde_json::Value::Object(Default::default()));
    let txn_opt = item
        .get("transactionId")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string());
    let trace = item
        .get("traceId")
        .and_then(|x| x.as_str())
        .filter(|s| !s.is_empty())
        .and_then(uuid_string_to_bytes);

    let mid = uuidv7_bytes();
    let mid_str = uuid_bytes_to_string(&mid);
    let txn = txn_opt.unwrap_or_else(|| mid_str.clone());

    let key = (queue.clone(), partition.clone());
    let gi = *group_of.entry(key).or_insert_with(|| {
        groups.push(TxnPushGroup {
            queue: queue.clone(),
            partition: partition.clone(),
            frames: Vec::new(),
            seen: HashMap::new(),
        });
        groups.len() - 1
    });
    let grp = &mut groups[gi];

    if let Some(first_mid) = grp.seen.get(&txn) {
        echoes.push(TxnPushEcho {
            index,
            txn,
            mid: first_mid.clone(),
            queue,
            duplicate: true,
        });
        return;
    }
    grp.seen.insert(txn.clone(), mid_str.clone());
    echoes.push(TxnPushEcho {
        index,
        txn: txn.clone(),
        mid: mid_str,
        queue,
        duplicate: false,
    });
    grp.frames.push(TxnFrame {
        mid,
        txn,
        trace,
        payload: serde_json::to_vec(&payload).unwrap_or_default(),
        encrypted: false,
    });
}

fn txn_fail_body(txn_id: &str, err: &str, status: StatusCode) -> Response {
    let out = serde_json::json!({
        "transactionId": txn_id,
        "success": false,
        "error": err,
        "results": [],
    });
    json(status, out.to_string())
}

pub async fn handle_transaction(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, format!("{{\"error\":\"bad body: {e}\"}}")),
    };
    let txn_id = uuid_bytes_to_string(&uuidv7_bytes());
    // Authenticated producer identity (JWT sub), stamped onto every pushed frame
    // when auth is enabled. None when auth is disabled or the token had no sub.
    let producer_sub = authed.0.filter(|s| !s.is_empty());

    let operations = match root.get("operations").and_then(|o| o.as_array()) {
        Some(o) => o,
        None => {
            return txn_fail_body(&txn_id, "transaction requires an operations array", StatusCode::BAD_REQUEST)
        }
    };
    st.metrics
        .transactions
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Combined lease hints: top-level requiredLeases (where the JS/Go builders
    // put the leaseId) + any per-op ack leaseId (raw HTTP callers).
    let mut lease_hints: Vec<String> = Vec::new();
    if let Some(rl) = root.get("requiredLeases").and_then(|x| x.as_array()) {
        for l in rl {
            if let Some(s) = l.as_str() {
                if !s.is_empty() {
                    lease_hints.push(s.to_string());
                }
            }
        }
    }

    let mut flat = 0usize;
    let mut echoes: Vec<TxnPushEcho> = Vec::new();
    let mut ack_echoes: Vec<TxnAckEcho> = Vec::new();
    let mut groups: Vec<TxnPushGroup> = Vec::new();
    let mut group_of: HashMap<(String, String), usize> = HashMap::new();
    let mut ack_groups: Vec<TxnAckGroup> = Vec::new();
    let mut ack_group_of: HashMap<(String, String), usize> = HashMap::new();
    let mut any_unknown = false;

    for op in operations {
        let ty = op.get("type").and_then(|x| x.as_str()).unwrap_or("");
        match ty {
            "push" => {
                if let Some(items) = op.get("items").and_then(|x| x.as_array()) {
                    for item in items {
                        txn_add_push(item, flat, &mut groups, &mut group_of, &mut echoes);
                        flat += 1;
                    }
                } else {
                    txn_add_push(op, flat, &mut groups, &mut group_of, &mut echoes);
                    flat += 1;
                }
            }
            "ack" => {
                let txn = op.get("transactionId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let partition_id =
                    op.get("partitionId").and_then(|x| x.as_str()).unwrap_or("").to_string();
                let group = op
                    .get("consumerGroup")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .unwrap_or("__QUEUE_MODE__")
                    .to_string();
                // RUSTFIX item 10: keep the normalized status (completed|failed|retry|
                // dlq) instead of collapsing to a bool, so retry/dlq reach the SP.
                let status = normalize_ack_status(op.get("status").and_then(|x| x.as_str()));
                let error = op
                    .get("error")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                let lease = op
                    .get("leaseId")
                    .and_then(|x| x.as_str())
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                if let Some(l) = &lease {
                    lease_hints.push(l.clone());
                }

                ack_echoes.push(TxnAckEcho { index: flat, txn: txn.clone() });
                let key = (partition_id.clone(), group.clone());
                let gi = *ack_group_of.entry(key).or_insert_with(|| {
                    ack_groups.push(TxnAckGroup {
                        partition_id: partition_id.clone(),
                        group: group.clone(),
                        worker: String::new(),
                        items: Vec::new(),
                    });
                    ack_groups.len() - 1
                });
                let ag = &mut ack_groups[gi];
                if ag.worker.is_empty() {
                    if let Some(l) = lease {
                        ag.worker = l;
                    }
                }
                ag.items.push(TxnAckItem { txn, status, error, index: flat });
                flat += 1;
            }
            _ => {
                any_unknown = true;
                flat += 1;
            }
        }
    }

    if any_unknown {
        return txn_fail_body(
            &txn_id,
            "segments transaction supports only push and ack operations",
            StatusCode::BAD_REQUEST,
        );
    }

    // RUSTFIX item 24: per-queue transaction throughput (each distinct queue the
    // transaction pushes to; an ack-only transaction maps to no queue here).
    {
        let mut seen_q: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for g in &groups {
            if seen_q.insert(g.queue.as_str()) {
                st.metrics.per_queue.add_transaction(tenant.as_str(), &g.queue);
            }
        }
    }

    // Fallback worker resolution: ack groups with no per-op leaseId inherit the
    // single unambiguous lease hint (the common case — one pop batch, one lease
    // in requiredLeases). Ambiguous hints leave the worker empty and the SP
    // rejects (invalid lease), rolling the transaction back.
    let unique_hint: Option<String> = {
        let mut only: Option<&String> = None;
        let mut ambiguous = false;
        for h in &lease_hints {
            match only {
                None => only = Some(h),
                Some(o) => {
                    if o != h {
                        ambiguous = true;
                        break;
                    }
                }
            }
        }
        if ambiguous {
            None
        } else {
            only.cloned()
        }
    };
    // Acquire the DB client up front: ack worker resolution and the bogus-ack
    // pre-check below both need it, and it is reused for the SP call.
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
        }
    };

    // Resolve each ack group's worker/lease. The JS/Go builders carry the leaseId
    // only in the top-level requiredLeases (never per ack op), so a transaction
    // that acks messages leased from more than one partition cannot be resolved
    // from the request alone — requiredLeases has no (lease -> partition) mapping,
    // and the single-hint fallback goes ambiguous the moment two leases appear
    // (the transactionWithPartitions / transactionMultipleQueues failures). The
    // authoritative source is queen.log_consumers: exactly one live lease exists
    // per (partition, group), so read worker_id straight from it. Precedence:
    //   1. a per-op leaseId (raw HTTP callers), already set during parse;
    //   2. the current log_consumers.worker_id for (partition, group);
    //   3. the single unambiguous requiredLeases hint (last resort).
    // RUSTFIX item 11: only resolve a worker from log_consumers / requiredLeases
    // when the client actually supplied lease info somewhere in the transaction. If
    // NO leaseId was sent at all, every ack is a lease-less ack: leave the worker
    // empty so log_ack_by_hash_v1 skips the worker/expiry check and still advances
    // the cursor — matching the direct /ack path, where a post-expiry lease-less ack
    // must succeed, not fail. The JS/Go builders always put the popped leaseId in
    // requiredLeases, so normal pop-then-ack transactions still resolve as before
    // (live leases → back-fill runs → validation passes; a supplied-but-expired
    // leaseId still fails, per item 11's second clause).
    let client_supplied_lease = !lease_hints.is_empty();
    for ag in &mut ack_groups {
        if !client_supplied_lease || !ag.worker.is_empty() || ag.partition_id.is_empty() {
            continue;
        }
        if let Ok(stmt) = client
            .prepare_cached(
                "SELECT worker_id FROM queen.log_consumers \
                 WHERE partition_id = $1::text::uuid AND consumer_group = $2",
            )
            .await
        {
            if let Ok(Some(r)) = client
                .query_opt(&stmt, &[&ag.partition_id, &ag.group])
                .await
            {
                if let Some(w) = r.get::<_, Option<String>>(0) {
                    ag.worker = w;
                }
            }
        }
        if ag.worker.is_empty() {
            if let Some(u) = &unique_hint {
                ag.worker = u.clone();
            }
        }
    }

    // Bogus-ack pre-check (atomic rollback). log_ack_by_hash_v1 resolves acked
    // hashes through queen.log_txns and SILENTLY treats an unresolvable hash as
    // not-acked (redelivery over loss). So a transaction that acks a
    // non-existent transactionId — the transactionRollback test acks
    // {transactionId:'non-existent-id'} alongside a real ack on the same
    // partition — would otherwise have its pushes committed and report
    // success:true, because the merged ack call still returns ok. The SP cannot
    // surface that within one call, so reject it HERE, before running the SP: if
    // any acked txn's hash appears in NO surviving log_txns row of its
    // partition, we return a v1-shaped failure and never touch the DB, so the
    // pushes roll back too. Hashes are computed broker-side (spec §3) and bound
    // as bytea[]; the probe explodes each row's 16B-stride hash blob via
    // queen.log_unnest_hashes (exact compare — no substring false positives).
    for ag in &ack_groups {
        if ag.partition_id.is_empty() || ag.items.is_empty() {
            continue;
        }
        let hashes: Vec<Vec<u8>> = ag
            .items
            .iter()
            .map(|it| crate::util::txn_hash128(&it.txn).to_vec())
            .collect();
        let stmt = match client
            .prepare_cached(
                "SELECT 1 FROM unnest($2::bytea[]) AS a(h) \
                 WHERE NOT EXISTS ( \
                   SELECT 1 FROM queen.log_txns t \
                   WHERE t.partition_id = $1::text::uuid \
                     AND EXISTS (SELECT 1 FROM queen.log_unnest_hashes(t.hashes) th \
                                 WHERE th.h = a.h)) \
                 LIMIT 1",
            )
            .await
        {
            Ok(s) => s,
            Err(e) => {
                st.metrics.record_db_error();
                return txn_fail_body(&txn_id, &e.to_string(), StatusCode::OK);
            }
        };
        match client.query_opt(&stmt, &[&ag.partition_id, &hashes]).await {
            Ok(Some(_)) => {
                return txn_fail_body(
                    &txn_id,
                    "QTXN ack references unknown transactionId; transaction rolled back",
                    StatusCode::OK,
                )
            }
            Err(e) => {
                st.metrics.record_db_error();
                return txn_fail_body(&txn_id, &e.to_string(), StatusCode::OK);
            }
            Ok(None) => {}
        }
    }

    // RUSTFIX item 8: encrypt each group's frame payloads for a queue with
    // encryption_enabled (parity with the normal push path). Warn + keep plaintext
    // on failure — never fail the transaction.
    if st.encryption.is_enabled() {
        for g in &mut groups {
            if g.frames.is_empty() || !st.encryption_enabled_for(&g.queue, tenant.as_str()).await {
                continue;
            }
            for f in &mut g.frames {
                match st.encryption.encrypt(&f.payload) {
                    Some(env) => {
                        f.payload = env;
                        f.encrypted = true;
                    }
                    None => {
                        static ENC_FAIL_TXN: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                        if let Some(suppressed) = ENC_FAIL_TXN.tick_now() {
                            tracing::warn!(target: "txn", queue = %g.queue, suppressed, "encryption failed; stored plaintext");
                        }
                    }
                }
            }
        }
    }

    // ------------------------------------------------ build the SP payload
    // 005_log_ack transaction wire: pushes carry {queue, partition, count, hashesHex
    // (32-hex per frame, frame order — hex of the 16*count hash bytes), blobB64,
    // verified}. verified = -1 means "no broker dedup vouching — probe the whole
    // window" (always correct; the transaction path is rare and skips the
    // dedup.rs cache).
    let mut pushes_json: Vec<serde_json::Value> = Vec::new();
    for g in &groups {
        if g.frames.is_empty() {
            continue;
        }
        let fins: Vec<FrameIn> = g
            .frames
            .iter()
            .map(|f| FrameIn {
                message_id: f.mid,
                txn: &f.txn,
                trace_id: f.trace,
                producer_sub: producer_sub.as_deref(),
                payload: &f.payload,
                encrypted: f.encrypted,
            })
            .collect();
        let mut hashes_hex = String::with_capacity(g.frames.len() * 32);
        for f in &g.frames {
            hashes_hex.push_str(&hex16(&crate::util::txn_hash128(&f.txn)));
        }
        let blob = zstd_compress(&pack_frames(&fins), st.zstd_level);
        let blob_b64 = base64::engine::general_purpose::STANDARD.encode(&blob);
        pushes_json.push(serde_json::json!({
            "queue": g.queue,
            "partition": g.partition,
            "count": g.frames.len(),
            "hashesHex": hashes_hex,
            "blobB64": blob_b64,
            "verified": -1,
        }));
    }

    let acks_json: Vec<serde_json::Value> = ack_groups
        .iter()
        .map(|ag| {
            // RUSTFIX item 10 on the log wire: the hash form
            // {"acks":[{"h":32-hex,"status"}]} so log_ack_by_hash_v1 honors
            // retry/dlq (the wire SP maps a legacy ok:false to 'failed').
            let items: Vec<serde_json::Value> = ag
                .items
                .iter()
                .map(|it| {
                    serde_json::json!({
                        "h": hex16(&crate::util::txn_hash128(&it.txn)),
                        "status": it.status,
                    })
                })
                .collect();
            serde_json::json!({
                "partitionId": ag.partition_id,
                "group": ag.group,
                "worker": ag.worker,
                "acks": items,
            })
        })
        .collect();

    // Track B (§5): carry the request tenant into the wire SP as `_tenant`.
    // queen.log_transaction_wire_v1 reads COALESCE((p->>'_tenant')::uuid, default)
    // and enforces it on BOTH sides of the transaction — push queue resolution/
    // auto-create is (tenant, name)-scoped, and the positional/hash acks are guarded
    // pid→queue→tenant (a pid not owned by this tenant aborts the whole txn). So the
    // atomic push+ack path is scoped/owned by construction, no separate Rust gate.
    let payload = serde_json::json!({
        "pushes": pushes_json,
        "acks": acks_json,
        "_tenant": tenant.as_str(),
    })
    .to_string();

    match db::transaction(&client, &payload).await {
        Ok(txt) => {
            let v: serde_json::Value = serde_json::from_str(&txt).unwrap_or(serde_json::Value::Null);
            if v.get("ok").and_then(|x| x.as_bool()).unwrap_or(false) {
                // RUSTFIX item 10: DLQ hand-off. log_ack_by_hash_v1 (via the wire SP)
                // returns dlq:true for a forced-dlq or budget-exhausted nack and KEEPS
                // the lease, so the broker must snapshot the poison frame and file the
                // queen.log_dlq row — exactly what process_acks does on the direct
                // path. Without this the DLQ entry is delayed until lease expiry + a
                // later direct nack. Runs post-commit on the same pooled client.
                // 'off' in the dlq:true entry carries the poison OFFSET (005_log_ack).
                let mut dlq_indices: std::collections::HashSet<usize> =
                    std::collections::HashSet::new();
                if let Some(arr) = v.get("acks").and_then(|x| x.as_array()) {
                    for r in arr {
                        if !r.get("dlq").and_then(|x| x.as_bool()).unwrap_or(false) {
                            continue;
                        }
                        let pid = r.get("partitionId").and_then(|x| x.as_str()).unwrap_or("");
                        let grp = r.get("group").and_then(|x| x.as_str()).unwrap_or("");
                        let off = r.get("off").and_then(|x| x.as_i64()).unwrap_or(0);
                        // Recover the originating ack group for the worker + nack error
                        // reason, and to know which result rows to stamp dlq:true.
                        let ag = match ack_groups
                            .iter()
                            .find(|g| g.partition_id == pid && g.group == grp)
                        {
                            Some(a) => a,
                            None => continue,
                        };
                        let acks: Vec<Ack> = ag
                            .items
                            .iter()
                            .map(|it| Ack {
                                txn: it.txn.clone(),
                                partition_id: ag.partition_id.clone(),
                                worker: ag.worker.clone(),
                                status: it.status,
                                error: it.error.clone(),
                            })
                            .collect();
                        let idxs: Vec<usize> = (0..acks.len()).collect();
                        let _ = dlq_file_head(
                            &client, pid, grp, &ag.worker, off, &acks, &idxs,
                        )
                        .await;
                        for it in &ag.items {
                            dlq_indices.insert(it.index);
                        }
                    }
                }
                // DLQ metric parity with the direct ack path (process_acks bumps
                // dlq_moved per dead-lettered item) so worker_metrics.dlq_count and
                // the Prometheus lifetime DLQ total include transaction DLQs too.
                if !dlq_indices.is_empty() {
                    st.metrics
                        .dlq_moved
                        .fetch_add(dlq_indices.len() as u64, std::sync::atomic::Ordering::Relaxed);
                }
                // 19-wildcard-hotlist §7 (transaction): the txn committed in the
                // procedure (NOT via the fusion flush), so mark every (queue,
                // partition) it pushed to + promote every partition it acked (the
                // txn released those leases). Rollback is safe by construction —
                // this only runs on a committed txn; an abort marks nothing.
                if st.hotlist.enabled() {
                    let now_ms = crate::util::now_epoch_ms();
                    for g in &groups {
                        if !g.frames.is_empty() {
                            let qkey = tenant_queue_key(tenant.as_str(), &g.queue);
                            st.hotlist
                                .mark_local(&qkey, &g.partition, g.frames.len() as u32, now_ms);
                        }
                    }
                    for ag in &ack_groups {
                        if let Some(q) = st.queue_for_partition(&client, &ag.partition_id).await {
                            // covered=false: txn acks may cover only part of a
                            // leased batch; promote unconditionally (rare path).
                            let qkey = tenant_queue_key(tenant.as_str(), &q);
                            st.hotlist.promote_ack(&qkey, &ag.group, &ag.partition_id, now_ms, false);
                        }
                    }
                }
                let mut results: Vec<serde_json::Value> = vec![serde_json::Value::Null; flat];
                for e in &echoes {
                    let mut obj = serde_json::json!({
                        "index": e.index,
                        "type": "push",
                        "success": true,
                        "transactionId": e.txn,
                        "messageId": e.mid,
                        "queueName": e.queue,
                    });
                    if e.duplicate {
                        obj["duplicate"] = serde_json::Value::Bool(true);
                    }
                    if e.index < results.len() {
                        results[e.index] = obj;
                    }
                }
                for a in &ack_echoes {
                    if a.index < results.len() {
                        results[a.index] = serde_json::json!({
                            "index": a.index,
                            "type": "ack",
                            "success": true,
                            "transactionId": a.txn,
                            "error": serde_json::Value::Null,
                            "dlq": dlq_indices.contains(&a.index),
                        });
                    }
                }
                let out = serde_json::json!({
                    "transactionId": txn_id,
                    "success": true,
                    "results": results,
                });
                json(StatusCode::OK, out.to_string())
            } else {
                let err = v
                    .get("error")
                    .and_then(|x| x.as_str())
                    .map(str::to_string)
                    .unwrap_or_else(|| "transaction failed".to_string());
                txn_fail_body(&txn_id, &err, StatusCode::OK)
            }
        }
        // The SP RAISEs on rollback (duplicate push / rejected ack): surface the
        // DB message (e.g. "QDUP ...", "QTXN ...") as a v1-shaped failure,
        // HTTP 200 (matches the C++ broker).
        Err(e) => {
            let msg = e
                .as_db_error()
                .map(|d| d.message().to_string())
                .unwrap_or_else(|| e.to_string());
            // A QDUP/QTXN RAISE is a business rollback, not a database failure —
            // counting it would inflate "DB errors" on every duplicate push.
            // Anything else (connection dropped, timeout, syntax) is a real one.
            if !(msg.starts_with("QDUP") || msg.starts_with("QTXN")) {
                st.metrics.record_db_error();
            }
            txn_fail_body(&txn_id, &msg, StatusCode::OK)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shape guard only — the gate needs a live pool, so behavioural coverage
    /// lives in the two-tenant isolation smoke. What this pins down is the one
    /// property that turns lease/extend back into a bearer-token op: the renew
    /// SP running without the tenant-scoped EXISTS in front of it.
    #[test]
    fn renew_gate_is_tenant_scoped() {
        assert!(RENEW_OWNED_SQL.contains("q.tenant_id = $3::text::uuid"));
        // The ownership leg must reach the queue through the lease's partition,
        // never through a name the caller could influence.
        assert!(RENEW_OWNED_SQL.contains("JOIN queen.log_partitions p ON p.id = c.partition_id"));
        // The SP must stay behind the qual (projection), not beside it.
        assert!(RENEW_OWNED_SQL.starts_with("SELECT (queen.log_renew_lease_v1($1, $2::int))::text WHERE EXISTS"));
    }

    // --------------------------------------------- TASK M: minimum pop wait

    const SEC: Duration = Duration::from_secs(1);

    // OFF is the default and must be indistinguishable from the pre-feature
    // broker: whatever the ring says, no wait is ever produced.
    #[test]
    fn min_pop_wait_off_never_waits() {
        for est in [0u64, 1, 5, 1_000, u64::MAX] {
            assert_eq!(min_pop_wait_window(0, true, 50, est, SEC), None);
            assert_eq!(min_pop_wait_window(-1, true, 50, est, SEC), None);
        }
    }

    // The semantic distinction the whole feature rests on: long-poll waits when
    // the queue is EMPTY, this waits when it is NON-EMPTY but under-full.
    #[test]
    fn min_pop_wait_only_when_non_empty_and_under_full() {
        // empty ⇒ the long-poll's case, untouched
        assert_eq!(min_pop_wait_window(25, true, 50, 0, SEC), None);
        // non-empty but under-full ⇒ hold, for the configured window
        assert_eq!(
            min_pop_wait_window(25, true, 50, 1, SEC),
            Some(Duration::from_millis(25))
        );
        assert_eq!(
            min_pop_wait_window(25, true, 50, 49, SEC),
            Some(Duration::from_millis(25))
        );
        // full ⇒ serve now
        assert_eq!(min_pop_wait_window(25, true, 50, 50, SEC), None);
        assert_eq!(min_pop_wait_window(25, true, 50, 51, SEC), None);
        // an unmeasurable (wheel-due) backlog reads as unbounded ⇒ serve now
        assert_eq!(min_pop_wait_window(25, true, 50, u64::MAX, SEC), None);
    }

    // A non-blocking pop means "what is there right now"; the window must not
    // silently turn it into a blocking one. A batch of one is full on arrival.
    #[test]
    fn min_pop_wait_respects_the_callers_contract() {
        assert_eq!(min_pop_wait_window(25, false, 50, 1, SEC), None);
        assert_eq!(min_pop_wait_window(25, true, 1, 0, SEC), None);
        assert_eq!(min_pop_wait_window(25, true, 1, 1, SEC), None);
        assert_eq!(min_pop_wait_window(25, true, 0, 1, SEC), None);
    }

    // Never exceed the caller's own deadline: a 100ms window on a pop with 10ms
    // left waits 10ms, and one with nothing left does not wait at all.
    #[test]
    fn min_pop_wait_never_outlives_the_deadline() {
        assert_eq!(
            min_pop_wait_window(100, true, 50, 1, Duration::from_millis(10)),
            Some(Duration::from_millis(10))
        );
        assert_eq!(
            min_pop_wait_window(100, true, 50, 1, Duration::ZERO),
            None
        );
        assert_eq!(
            min_pop_wait_window(100, true, 50, 1, Duration::from_millis(100)),
            Some(Duration::from_millis(100))
        );
    }
}
