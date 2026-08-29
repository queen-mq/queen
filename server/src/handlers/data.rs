#![allow(unused_imports)]
use super::*;

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use deadpool_postgres::Pool;
use serde::de::{self, Deserializer, Visitor};
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
use crate::admission::Lane;

// ------------------------------------------------------------------ push

/// A JSON string field taken straight out of the request body: BORROWED when the
/// literal needs no unescaping (the overwhelmingly common case), OWNED only when
/// serde_json had to unescape it.
///
/// Why this exists. A bare `&'a str` field cannot be deserialized from a JSON
/// literal containing ANY escape sequence — unescaping needs an owned buffer, so
/// serde_json calls `visit_str` and the `&str` impl answers `invalid type: string
/// "...", expected a borrowed string`. That is a parse error for the WHOLE body
/// (there is one `from_slice` per request), i.e. a 400 that discards every item in
/// the batch. It is not a corner case: Go's `encoding/json` escapes `&`, `<` and
/// `>` as six-character \u sequences BY DEFAULT (SetEscapeHTML), Guzzle (the
/// PHP/Laravel SDK) escapes `/` and
/// all non-ASCII, and every JSON encoder escapes `"` and `\`. So a transaction id
/// like `Bed&Breakfast-771` from the Go SDK, or `2026/07/BK-11` from PHP, made the
/// broker 400 its own first-party clients.
///
/// Why a newtype and not `Cow<'a, str>` directly. `#[serde(borrow)]` only rewires a
/// field whose type is *literally* `Cow<'a, str>` (serde_derive's `is_cow` check).
/// `Option<Cow<'a, str>>` does not match and silently falls back to the blanket
/// `impl Deserialize for Cow`, which is ALWAYS-OWNED — an allocation on every push
/// for `partition` and `transactionId`, on the one path in this broker that is
/// tuned per-allocation. This newtype behaves identically bare and inside `Option`.
#[derive(Debug, Clone, PartialEq, Eq)]
struct CowStr<'a>(Cow<'a, str>);

impl<'a> CowStr<'a> {
    #[inline]
    fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'a> std::ops::Deref for CowStr<'a> {
    type Target = str;
    #[inline]
    fn deref(&self) -> &str {
        &self.0
    }
}

impl<'de: 'a, 'a> Deserialize<'de> for CowStr<'a> {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V<'a>(PhantomData<&'a ()>);
        impl<'de: 'a, 'a> Visitor<'de> for V<'a> {
            type Value = CowStr<'a>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("a string")
            }
            /// Hot path: the literal had no escape, so it is a slice of the request
            /// body. Zero allocation — byte-identical to the `&'a str` field this
            /// replaced.
            #[inline]
            fn visit_borrowed_str<E: de::Error>(self, v: &'de str) -> Result<CowStr<'a>, E> {
                // Unreachable by construction: serde_json's tokenizer rejects a raw
                // 0x00-0x1F byte inside a string literal, so a value that needed no
                // unescaping cannot carry one. Asserted, not scanned, so the clean
                // path stays free.
                debug_assert!(!v.as_bytes().iter().any(|&b| b < 0x20));
                Ok(CowStr(Cow::Borrowed(v)))
            }
            #[inline]
            fn visit_str<E: de::Error>(self, v: &str) -> Result<CowStr<'a>, E> {
                reject_control::<E>(v)?;
                Ok(CowStr(Cow::Owned(v.to_owned())))
            }
            #[inline]
            fn visit_string<E: de::Error>(self, v: String) -> Result<CowStr<'a>, E> {
                reject_control::<E>(&v)?;
                Ok(CowStr(Cow::Owned(v)))
            }
        }
        d.deserialize_str(V(PhantomData))
    }
}

/// The ONE charset restriction the push path genuinely depends on. The layer-1
/// dedup key below and the fusion group key (`fusion.rs`) are both composed by
/// joining fields on `\x1f`; a `\x1f` inside a field would alias two distinct
/// (queue, partition, txn) triples onto one key — silently fusing two partitions
/// into one segment. Until now that invariant held only by accident: the only way
/// to write a 0x1F inside a JSON string is a \u escape, and every escape failed
/// the borrow. Enforced here instead, in the ONLY arm that can ever see one (a
/// value that was unescaped), so the clean path pays nothing.
#[cold]
fn reject_control<E: de::Error>(v: &str) -> Result<(), E> {
    if v.as_bytes().iter().any(|&b| b < 0x20) {
        return Err(E::custom(
            "control characters are not allowed in queue, partition or transactionId",
        ));
    }
    Ok(())
}

/// Wire limit on `transactionId`, dictated by the u16 length prefix the segment
/// frame codec writes for it (`frames::pack_frames`).
pub(crate) const MAX_TXN_BYTES: usize = u16::MAX as usize;

#[derive(Deserialize)]
struct PushItem<'a> {
    #[serde(borrow)]
    queue: CowStr<'a>,
    #[serde(borrow)]
    partition: Option<CowStr<'a>>,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(borrow, rename = "transactionId")]
    transaction_id: Option<CowStr<'a>>,
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
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    let n = parsed.items.len();
    if n == 0 {
        return json(StatusCode::CREATED, "[]".to_string());
    }

    // The segment frame codec stores the transaction id behind a u16 length
    // (frames.rs `pack_frames`), so a longer one used to be silently TRUNCATED by
    // the `as u16` cast while `body_len` still counted the full length — a corrupt
    // frame, not a rejected push. Enforce the wire limit at the boundary that can
    // still answer, and reject the whole batch rather than write a bad segment.
    if let Some(bad) = parsed
        .items
        .iter()
        .position(|it| it.transaction_id.as_deref().is_some_and(|t| t.len() > MAX_TXN_BYTES))
    {
        return json(
            StatusCode::BAD_REQUEST,
            json_err(
                "bad body: ",
                format_args!("transactionId of item {bad} exceeds the {MAX_TXN_BYTES}-byte limit"),
            ),
        );
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
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| mid_str.clone());
        let queue = it.queue.as_str().to_string();
        let partition = it.partition.as_deref().unwrap_or("Default").to_string();

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
            *per_q.entry(it.queue.as_str()).or_insert(0) += 1;
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
        let (payload, encrypted) =
            spool_payload(&st, it.queue.as_str(), tenant.as_str(), it.payload, &mut enc_flags).await;
        let ok = st.file_buffer.write_event(
            it.queue.as_str(),
            it.partition.as_deref().unwrap_or("Default"),
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
            .as_deref()
            .map(|s| s.to_string())
            .unwrap_or_else(|| mid_str.clone());
        let partition = it.partition.as_deref().unwrap_or("Default");
        // RUSTFIX item 8: spool the encrypted envelope for an encrypted queue.
        let (payload, encrypted) =
            spool_payload(st, it.queue.as_str(), tenant, it.payload, &mut enc_flags).await;
        let ok = st.file_buffer.write_event(
            it.queue.as_str(),
            partition,
            tenant,
            &txn,
            producer_sub,
            encrypted,
            &payload,
        );
        if !ok {
            all_ok = false;
        }
        results.push(ItemResult {
            message_id: mid_str,
            txn,
            queue: it.queue.as_str().to_string(),
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
    // subscriptionMode 'new' | 'all', subscriptionFrom 'now' | ISO. When omitted,
    // the broker applies DEFAULT_SUBSCRIPTION_MODE (default 'new' — see
    // config::normalize_subscription_mode). Group-less "queue mode" pops are
    // hard-pinned to 'all' by the SQL and ignore both.
    // timestamp | '' (default). Threaded to the log pop SPs (p_sub_mode /
    // p_sub_from); existing cursors are never re-seeded.
    #[serde(rename = "subscriptionMode")]
    subscription_mode: Option<String>,
    #[serde(rename = "subscriptionFrom")]
    subscription_from: Option<String>,
    // PLAN_CONFLATION §3.1 — last-value delivery for this consumer GROUP on this
    // queue: a pop of a partition delivers exactly the newest visible message and
    // leases (committed, tail]. Declared here (query string, never a body field —
    // the subscriptionMode shape), persisted on the group's first registration,
    // and from then on the STORED value wins for every consumer of that group
    // (§1.1/§3.3). Absent ⇒ off ⇒ byte-identical behaviour.
    #[serde(rename = "conflation")]
    conflation: Option<bool>,
    // POP AUTOPILOT (server/src/pop_autopilot.rs) — "choose the knobs I did not
    // send". Emitted ONLY when true, the `conflation` shape and for the same
    // reason: a consumer that does not opt in sends the request it sent before
    // this option existed, byte for byte.
    //
    // It has to be a NEW parameter and could not be "field absent ⇒ broker
    // decides": absent `partitions` already MEANS 1 and absent `batch` already
    // MEANS 200, and today's SDKs omit both at their defaults (the Go SDK only
    // sends `partitions` when > 1), so the absent-field encoding would silently
    // change the behaviour of every consumer in the field that never touched the
    // knob. Per DIMENSION: `autopilot=true&partitions=1` is a manual width of 1
    // with an automatic batch, and the controller never touches a dimension the
    // client sent. Absent ⇒ off ⇒ byte-identical behaviour.
    #[serde(rename = "autopilot")]
    autopilot: Option<bool>,
}

/// PLAN_CONFLATION §3.1/§3.3 — what the response has to say about conflation.
/// `on` is the EFFECTIVE policy (the stored one, or the requested one when this
/// pop is the registrar); `conflict` records that the request disagreed with the
/// stored value. Both keys are emitted ONLY when they are true, so a flag-off
/// deployment keeps byte-identical response bytes (which
/// `handlers::data::protocol_conformance` pins against `queen-protocol`).
#[derive(Clone, Copy, Default)]
struct Conflation {
    on: bool,
    conflict: bool,
}

impl Conflation {
    const OFF: Conflation = Conflation { on: false, conflict: false };
}

/// Resolve the effective conflation policy for (queue, group) — §3.3: SQL is the
/// authority, the broker caches it. The request flag is used for exactly two
/// things, the first registration write and detecting a conflict.
///
/// A cache hit costs zero DB; a miss costs ONE indexed `consumer_groups_metadata`
/// lookup per (queue, group) per process. An unregistered group has no stored
/// policy yet, so the requested value becomes effective AND is what the
/// registering pop persists.
async fn resolve_conflation(
    st: &Arc<AppState>,
    queue: &str,
    group: &str,
    tenant: &str,
    requested: Option<bool>,
) -> Conflation {
    // Group-less queue mode has no group identity to hang a policy on; the
    // handlers refuse conflation=true there outright (§3.3), and a stray
    // conflation=false must not cost a lookup.
    if group == "__QUEUE_MODE__" {
        return Conflation::OFF;
    }
    let stored = st.group_policy(queue, group, tenant).await.map(|p| p.conflation);
    let on = stored.unwrap_or_else(|| requested.unwrap_or(false));
    let conflict = matches!(requested, Some(r) if r != on);
    if conflict {
        note_conflation_conflict(st, tenant, Some(queue), queue, group, on, requested);
    }
    Conflation { on, conflict }
}

/// §3.3 items 2 and 4 — group-setting-wins, LOUDLY: the counters and a
/// rate-limited line, never a per-request log line (a mismatched fleet would turn
/// that into a flood — the `POOL_SAT` idiom in obs.rs). The third channel, the
/// response echo, is `Conflation::conflict` and is rendered by the caller.
///
/// `queue` is `None` on the discovery route, which spans queues and so has no
/// single per-queue counter to attribute to; `scope` is the label for the log
/// line either way (a queue name, or the `namespace/task` pair).
fn note_conflation_conflict(
    st: &Arc<AppState>,
    tenant: &str,
    queue: Option<&str>,
    scope: &str,
    group: &str,
    stored: bool,
    requested: Option<bool>,
) {
    if let Some(q) = queue {
        st.metrics.per_queue.add_conflation_conflict(tenant, q);
    }
    st.metrics
        .conflation_conflicts
        .fetch_add(1, Ordering::Relaxed);
    static CFL_CONFLICT: crate::obs::Sampler = crate::obs::Sampler::new(60_000);
    if let Some(suppressed) = CFL_CONFLICT.tick(crate::util::now_epoch_ms()) {
        tracing::warn!(
            target: "conflation",
            queue = scope,
            group,
            stored,
            requested = requested.unwrap_or(false),
            suppressed,
            "consumer declared a conflation policy the group does not have — \
             the STORED group setting wins"
        );
    }
}

/// §3.3 — the two refused combinations, rejected at the handler with a 400 that
/// names the reason. This is the one place conflation REJECTS rather than warns,
/// because both are consumer bugs whose silent form is unfixable in production.
/// Returns the refusal response when the request is illegal.
fn conflation_refusal(
    requested: Option<bool>,
    has_group: bool,
    auto_ack: bool,
) -> Option<Response> {
    if requested != Some(true) {
        return None;
    }
    if !has_group {
        return Some(json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"conflation requires consumerGroup: queue mode is a \
             shared cursor with no group identity to hang a delivery policy on\",\"messages\":[]}"
                .to_string(),
        ));
    }
    if auto_ack {
        return Some(json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"conflation cannot be combined with autoAck: auto-ack \
             commits at delivery with no lease, so a failed handler loses the tail and the \
             at-least-once guarantee conflation exists to provide degrades to \
             at-most-once\",\"messages\":[]}"
                .to_string(),
        ));
    }
    None
}

#[derive(Deserialize)]
struct PopResult {
    #[serde(default)]
    partitions: Vec<PopPart>,
    #[serde(default)]
    error: Option<String>,
    // PLAN_CONFLATION §3.3, DISCOVERY ROUTE ONLY. queen.log_pop_discover_wire_v1
    // resolves the durable policy per matched queue and reports here what it
    // actually applied, plus whether any matched queue's stored policy disagreed
    // with the request. Absent — which is every other SP and every request that
    // did not carry the flag — leaves the caller's own resolution in place, so
    // the wildcard path is byte-identical.
    #[serde(default)]
    conflation: Option<bool>,
    #[serde(rename = "conflationConflict", default)]
    conflation_conflict: Option<bool>,
}
#[derive(Deserialize)]
struct PopPart {
    partition: String,
    #[serde(rename = "partitionId")]
    partition_id: String,
    // Group-scoped redelivery count written by the SQL claim path. Defaulting
    // to one keeps rolling upgrades safe when a broker briefly sees metadata
    // produced by the previous procedure version.
    #[serde(
        rename = "deliveryAttempt",
        alias = "attempt",
        default
    )]
    delivery_attempt: Option<i32>,
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
// rows into this shape): segments + partitionId + the group-scoped delivery
// attempt, with no partition name (the caller knows it from the path). `seq` in
// the segment JSON carries base_offset and `startOff` the start frame index —
// opaque tokens (§11); the renderer only reads startOff/take/createdAt/blob.
#[derive(Deserialize)]
struct PopSpecificResult {
    #[serde(default)]
    segments: Vec<PopSeg>,
    #[serde(rename = "partitionId", default)]
    partition_id: String,
    #[serde(
        rename = "deliveryAttempt",
        alias = "attempt",
        default
    )]
    delivery_attempt: Option<i32>,
    #[serde(default)]
    error: Option<String>,
}

pub async fn handle_pop(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    Path(queue): Path<String>,
    Query(p): Query<PopParams>,
) -> Response {
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    // PLAN_CONFLATION §3.3: refuse the two illegal combinations BEFORE anything
    // else — both are consumer bugs whose silent form is unfixable in production.
    // Ahead of the maintenance gate too: an illegal request is illegal whether or
    // not pops happen to be paused, and answering it 200/"paused" for the length
    // of a maintenance window would hide the bug exactly while an operator is
    // looking at the cell. Neither check touches the DB.
    if let Some(r) = conflation_refusal(p.conflation, p.consumer_group.is_some(), auto_ack) {
        return r;
    }
    // Pop maintenance: consumers get an empty, paused result (204) — matches the
    // C++ pop-maintenance behavior, but shaped so the client's empty-response
    // handling (no messages) simply retries. A conflating request gets the same
    // answer WITH a body (see POP_PAUSED_CONFLATING).
    //
    // Written out in each of the three pop handlers rather than behind a helper:
    // webdoc/scripts/gen-openapi.mjs derives a route's response codes from the
    // `StatusCode::` variants reachable IN THE HANDLER BODY, so moving these two
    // behind a call silently drops both 200 and 204 from the published OpenAPI
    // for every pop route.
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return if p.conflation == Some(true) {
            json(StatusCode::OK, POP_PAUSED_CONFLATING.to_string())
        } else {
            json(StatusCode::NO_CONTENT, POP_PAUSED.to_string())
        };
    }
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    let cfl = resolve_conflation(&st, &queue, &group, tenant.as_str(), p.conflation).await;
    // M5 (§3.2): `partitions` defaults to 1, and a conflating pop yields at most
    // ONE message per partition — so the batch budget stops being the thing that
    // sizes a pop and the partition cap becomes it. Raise the cap to `batch`,
    // clamped to the 64-wide checkout ceiling, which is load-bearing and measured
    // (see the `k` comment in hotlist_pop_attempt). A conflating pop therefore
    // returns at most 64 messages per round trip, whatever `batch` says.
    let max_parts = if cfl.on {
        p.partitions.unwrap_or(batch).clamp(1, 64)
    } else {
        p.partitions.unwrap_or(1)
    };
    let sub_mode = p
        .subscription_mode
        .as_deref()
        .map(crate::config::normalize_subscription_mode)
        .unwrap_or_else(|| st.default_subscription_mode.clone());
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
        // ── POP AUTOPILOT (server/src/pop_autopilot.rs). v1 is the RING path
        // only: every input the controller reads is a hot-list fact (ready
        // partitions, the per-candidate ready-age `take_batch` already samples
        // for the arbiter), and the legacy SQL candidate scan has none of them.
        // On that path the parameter is accepted and resolves to today's defaults
        // — the branch below is untouched.
        //
        // Conflation is exempt on purpose and it is not an oversight: with
        // `cfl.on`, `partitions` is not a sweep width at all but the message
        // budget (a conflating pop yields at most ONE message per partition, §3.2
        // above), so a controller width would silently shrink what the consumer
        // asked for. The feature that already owns that dimension keeps it.
        let ap_now = crate::util::now_epoch_ms();
        // ONE pass over the ready set for both of the controller's ring inputs:
        // how many partitions are servable, and how long the oldest of them has
        // waited. Read here, before the claim, and never on the quiet
        // empty-re-poll short-circuit inside the attempt.
        let ap_ready = if st.autopilot.active() && !cfl.on {
            let (parts, oldest_ms) = st.hotlist.ready_peek(&qkey, &group, ap_now);
            crate::pop_autopilot::ReadyPeek { parts, oldest_ms }
        } else {
            crate::pop_autopilot::ReadyPeek::default()
        };
        let ticket = if cfl.on {
            crate::pop_autopilot::PopTicket::inert()
        } else {
            st.autopilot.begin(
                &qkey,
                &group,
                crate::pop_autopilot::Ask {
                    autopilot: p.autopilot == Some(true),
                    partitions: p.partitions,
                    batch: p.batch,
                    default_partitions: max_parts,
                    default_batch: batch,
                },
                ap_ready,
                ap_now,
            )
        };
        // The ONE place the controller's answer becomes the claim's knobs. The
        // `None` arm is the pre-autopilot code path verbatim, which is what makes
        // "a request without the parameter is byte-identical" structural rather
        // than asserted: with no plan there is nothing here to get wrong.
        let (batch, max_parts) = match ticket.plan() {
            Some(pl) => (pl.batch, pl.partitions),
            None => (batch, max_parts),
        };
        return serve_pop_hotlist(
            &st, &qkey, &queue, &group, batch, max_parts, auto_ack, wait, deadline, lease_seconds,
            &sub_mode, &sub_from, &worker, tenant.as_str(), cfl, &ticket,
        )
        .await;
    }

    let mut backoff_count: u32 = 0;
    loop {
        // ── discovery-latency fix (2026-07-24): the legacy long-poll re-checks the
        // queue every backoff interval, so at N parked consumers the rate of EMPTY
        // re-polls is O(#parked consumers) = O(#queues). Acquiring the SHARED
        // pop admission slot on every such empty re-poll let that O(#queues)
        // storm saturate the limiter (Vegas shrinks it under RTT pressure) — so a
        // freshly-woken REAL delivery pop queued behind thousands of empty re-polls
        // on acquire(), a priority inversion whose wait grew LINEARLY with the queue
        // count. Gate the admission-limited wildcard scan behind the cheap indexed
        // has_pending SUPERSET (no permit taken): `false` means there is definitively
        // nothing to deliver → skip the scan and park; a "maybe pending" queue takes
        // the permit and scans. The probe borrows a pooled connection (uncontended —
        // pool.get measured ~0µs) and never touches the admission budget, so the quiet re-poll
        // storm can no longer starve real deliveries.
        let pending = match st.pool.get().await {
            Ok(c) => db::has_pending(&c, &queue, &group, tenant.as_str()).await.unwrap_or(true),
            Err(_) => true, // probe unavailable → fall back to the full scan (safe)
        };

        let (txt, blobs, rtt): (String, Vec<Vec<u8>>, Duration) = if pending {
            let mut slot = st.admission.acquire(Lane::Pop).await;
            let client = match st.pool.get().await {
                Ok(c) => c,
                Err(_) => {
                    st.metrics.record_db_error();
                    drop(slot);
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
                    &sub_mode, &sub_from, tenant.as_str(), cfl.on,
                ),
            )
            .await;
            let rtt = t0.elapsed();
            if matches!(res, Ok(Ok(_))) { slot.commit_done(rtt); }
            drop(slot);
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
            build_pop_response(&txt, Some(&blobs), &queue, &group, lease_id, &st.encryption, cfl);
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
                            &sub_mode, &sub_from, tenant.as_str(), cfl,
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
        return json(pop_status(count, cfl), body);
    }
}

/// PLAN_CONFLATION §3.1/§4 — an empty pop is a bodiless 204 (handlers::json drops
/// the body there, RFC 9110 §15.3.5), which cannot carry the `"conflation":true`
/// echo. That echo is the whole degrade-loudly contract: an SDK that asked for
/// conflation must be able to tell an old broker from a new one on the FIRST
/// round trip, before it processes a single message — so a CONFLATING pop answers
/// 200 with a body even when it delivered nothing. Every flag-off response keeps
/// the 204 byte for byte.
///
/// `conflict` counts for exactly the same reason and it is NOT an edge case: for
/// a request that disagrees with the stored policy (requested=true, stored=false)
/// `on` is FALSE, so keying only on `on` would send the disagreeing consumer a
/// bodiless 204 — indistinguishable from a pre-1.1.0 broker — and every SDK's
/// degrade-loudly check would kill it (§3.3 item 3, Q3, E2E-4: "both consumers
/// keep working"). A long-poll on an idle queue is precisely that response, so
/// the conflicting consumer would die on its FIRST empty poll. Emitting the 200
/// whenever the response has anything to say about conflation keeps flag-off
/// responses byte-identical (both fields false ⇒ 204, as before).
#[inline]
fn pop_status(count: usize, cfl: Conflation) -> StatusCode {
    if count == 0 && !cfl.on && !cfl.conflict {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::OK
    }
}

/// The pop-maintenance answer, which predates conflation and has to keep
/// predating it for every consumer that did not ask for conflation.
const POP_PAUSED: &str = "{\"messages\":[],\"paused\":true}";

/// PLAN_CONFLATION §4 — the pop-maintenance answer for a request that DID ask.
///
/// `handlers::json` drops the body on a 204, so a conflating consumer would
/// receive a bodiless response carrying neither `paused` nor the conflation echo,
/// and every SDK's degrade-loudly check reads "no echo" as "broker older than
/// 1.1.0" and STOPS the consume loop. Turning on pop maintenance — a routine
/// operator action — would therefore terminate every conflating consumer in the
/// fleet with a version error, recoverable only by restarting them.
///
/// So the answer keeps its 200 and carries both keys: `paused` (this is not a
/// verdict on the policy — the request never reached the claim path) and
/// `conflation` (this broker understands the parameter, which is all the
/// degrade-loudly check needs to know). Nothing was delivered, so no policy was
/// applied to anything, and no DB is touched: maintenance means the pop path
/// stays cold.
const POP_PAUSED_CONFLATING: &str = "{\"messages\":[],\"paused\":true,\"conflation\":true}";

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
    // PLAN_CONFLATION §3.2: the mesh-hint path calls db::pop_specific, so it
    // carries the effective policy like every other claim.
    cfl: Conflation,
) -> Option<Response> {
    let lease_id: &str = if auto_ack { "" } else { worker };
    let mut parts: Vec<PopPart> = Vec::new();
    let mut remaining = batch;
    let mut total_rtt = Duration::ZERO;
    for hint in hints {
        if remaining <= 0 {
            break;
        }
        let mut slot = st.admission.acquire(Lane::Pop).await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(slot);
                break;
            }
        };
        let cancel_token = client.cancel_token();
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_specific(
                &client, queue, hint, group, remaining, lease_seconds, worker, auto_ack, sub_mode,
                sub_from, tenant, cfl.on,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        if matches!(res, Ok(Ok(_))) { slot.commit_done(rtt); }
        total_rtt += rtt;
        drop(slot);
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
                delivery_attempt: parsed.delivery_attempt,
                segments: parsed.segments,
            });
            remaining -= seg_count;
        }
    }
    if parts.is_empty() {
        return None;
    }
    let (body, count, meta) =
        render_pop_parts(&parts, None, queue, group, lease_id, &st.encryption, cfl);
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
    // PLAN_CONFLATION §3.2: resolved once by handle_pop and threaded, never
    // re-resolved per poll iteration.
    cfl: Conflation,
    // POP AUTOPILOT: this request's controller state, resolved once by handle_pop
    // (its `batch`/`max_parts` are already the ticket's answer). Threaded rather
    // than re-derived so the claim observations and the outcome record cost no map
    // lookup, and so the lane's live-worker count is released by ONE Drop —
    // including on a dropped request future.
    ticket: &crate::pop_autopilot::PopTicket,
) -> Response {
    let lease_id: &str = if auto_ack { "" } else { worker };
    let mut backoff_count: u32 = 0;
    // Request-flow trace: the per-request uuidv7 `worker` is the correlation
    // key — grep w=<prefix> and sort by t= to reconstruct one pop's full life.
    let t_rq = Instant::now();
    // uuidv7 is time-ordered: the LEADING hex is the timestamp and collides
    // across concurrent requests — the trailing bits are the random part.
    let wtag: &str = if worker.len() >= 8 { &worker[worker.len() - 8..] } else { worker };
    if st.hotlist.traced(qkey) {
        eprintln!("[hlt] rqin q={} g={} w={} batch={} mp={} wait={} t={}",
            queue, group, wtag, batch, max_parts, wait, crate::hotlist::trace_now_ms());
    }
    loop {
        // ── TASK M (minimum pop wait). The ceiling on a small cell is
        // COMMIT-bound, ~4 tiny PG commits per DELIVERED message, because a pop
        // claims whatever single message just arrived ("pop magro"). This holds an
        // UNDER-FULL claim back for up to the queue's configured window so ONE
        // commit carries more messages.
        //
        // WHERE the wait lives is the whole design. It is HERE — in Rust, before
        // hotlist_pop_attempt — and never inside the SQL, because a wait inside
        // log_pop_list_v1 (pg_sleep) would hold a pooled PG connection, a pop admission slot
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

        let (body, count, meta, rtt) = match hotlist_pop_attempt(
            st, qkey, queue, group, batch, max_parts, auto_ack, lease_seconds, sub_mode, sub_from,
            worker, lease_id, tenant, cfl, ticket,
        )
        .await
        {
            Ok(v) => v,
            // Transport failure (no pooled connection, or a statement the broker
            // cancelled). hotlist_pop_attempt has already run the full cleanup for
            // that arm — admission slot dropped, db error counted, and any
            // checked-out candidates checked back into the ring — so the only thing
            // left here is to surface it. Return WITHOUT parking: a `wait=true` pop
            // must not long-poll its way through a dead pool, which is precisely
            // what the old empty-tuple return did (count==0 ⇒ park ⇒ eventually a
            // bodiless 204, indistinguishable from an idle queue). Same shape and
            // same point in the poll loop as the legacy scan path's 500s above.
            Err(resp) => return resp,
        };

        if count == 0 && wait && Instant::now() < deadline {
            backoff_count += 1;
            let interval = st.pop_backoff_interval(backoff_count);
            let waitd = deadline
                .saturating_duration_since(Instant::now())
                .min(interval);
            let _parked = st.metrics.parked.enter(tenant, queue);
            let t_park = Instant::now();
            let woke = st.notifier.wait_queue(qkey, waitd).await;
            if st.hotlist.traced(qkey) {
                eprintln!("[hlt] park q={} g={} w={} ms={} woke={} bo={} t={}", queue, group,
                    wtag, t_park.elapsed().as_millis(), woke, backoff_count,
                    crate::hotlist::trace_now_ms());
            }
            if woke {
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
        if st.hotlist.traced(qkey) {
            eprintln!("[hlt] rqout q={} g={} w={} n={} ms={} t={}",
                queue, group, wtag, count, t_rq.elapsed().as_millis(),
                crate::hotlist::trace_now_ms());
        }
        // ── POP AUTOPILOT: the loop closes HERE, once per request (never per poll
        // iteration) — measure at the end of pop N, apply at the start of pop
        // N+1. `count / batch` is the fill ratio; the ready-age half arrived
        // earlier, from the claim itself.
        ticket.record(batch, count, crate::util::now_epoch_ms());
        let status = pop_status(count, cfl);
        let mut body = body;
        // The additive echo, emitted if and only if this request opted in AND the
        // broker resolved at least one dimension. Note what it cannot do: a 204
        // carries no body at all (handlers::json, RFC 9110 §15.3.5), so an EMPTY
        // pop's advice is not deliverable without changing that status — and
        // changing it for autopilot requests would be a second wire change on top
        // of an additive one. The advice is stable across pops, so a client learns
        // it from the first response that carries messages and keeps it.
        if let Some(plan) = ticket.plan() {
            if status != StatusCode::NO_CONTENT {
                crate::pop_autopilot::append_echo(
                    &mut body,
                    plan,
                    // Advisory pacing for the client's empty-poll loop: the
                    // broker's OWN long-poll re-query interval (RUSTFIX item 19,
                    // POP_WAIT_INITIAL_INTERVAL_MS). No new machinery — a client
                    // polling faster than the broker re-checks is spending round
                    // trips on nothing.
                    st.pop_wait_initial_interval_ms,
                );
            }
        }
        return json(status, body);
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

// POP AUTOPILOT: hand one claim's READY-AGE to the width controller. The MAX
// over the claim, not the mean: the oldest servable partition this claim reached
// is the wait a consumer actually feels, and the mean hides exactly the lane the
// feature exists for (`ready_age_p95` is the number the soak cell was judged on).
//
// Free by construction — `take_batch` computed each candidate's `now -
// ready_since` on its way past for the arbiter's LapStats, so this is a fold over
// a slice already in hand, with no lock and no allocation. Inert (one Option
// check) when the switch is off.
#[inline]
fn note_claim_ages(
    ticket: &crate::pop_autopilot::PopTicket,
    cands: &[crate::hotlist::Candidate],
    now_ms: i64,
) {
    let max_age = cands.iter().map(|c| c.ready_age_ms).max().unwrap_or(0);
    ticket.note_claim(cands.len(), max_age, now_ms);
}

// One ring-serve attempt: (lazily) refresh the queue deferral config, take K
// candidates from the (queue, group) ring, call queen.log_pop_list_v1 on them,
// check the tri-state verdicts back into the ring (§4/§6/§7), and render. An
// empty ring triggers a throttled keyset reseed (§8) before giving up.
//
// Ok((body, count, meta, rtt)); count==0 means the caller should park.
// Err(response) is a TRANSPORT failure — no pooled connection, or a statement
// the broker cancelled — and the caller must return it as-is WITHOUT parking,
// exactly as the legacy scan path returns from inside its own poll loop. It is
// an error channel and not "count == 0" because an empty tuple renders as a
// bodiless 204 (handlers::json drops the body on 204, RFC 9110 §15.3.5), which
// is the client-visible shape of an idle queue: a consumer cannot tell a dead
// pool from "nothing for you" and simply long-polls the outage. Documented
// contract: reference/http/pop.mdx (500 + {"error":"pool"} /
// {"error":"pop failed"}) and internals/life-of-a-pop.mdx.
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
    cfl: Conflation,
    // POP AUTOPILOT: receives this claim's ring observation (see `note_claim`
    // below). Inert on every broker where the switch is off.
    ticket: &crate::pop_autopilot::PopTicket,
) -> Result<(String, usize, PopMeta, Duration), Response> {
    let now_ms = crate::util::now_epoch_ms();
    let empty = || render_pop_parts(&[], None, queue, group, lease_id, &st.encryption, cfl);

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
        let mut slot = st.admission.acquire(Lane::Pop).await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(slot);
                let (b, c, m) = empty();
                return Ok((b, c, m, Duration::ZERO));
            }
        };
        let cancel_token = client.cancel_token();
        let t0 = Instant::now();
        let res = tokio::time::timeout(
            st.stmt_timeout,
            db::pop_wildcard_bin(
                &client, queue, group, batch, lease_seconds, worker, auto_ack, max_parts,
                sub_mode, sub_from, tenant, cfl.on,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        if matches!(res, Ok(Ok(_))) { slot.commit_done(rtt); }
        drop(slot);
        let (txt, blobs) = match db::resolve_query_timeout(res, client, cancel_token, "pop_wildcard", &st.metrics)
        {
            Some(t) => t,
            None => {
                let (b, c, m) = empty();
                return Ok((b, c, m, rtt));
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
                &parsed.partitions, Some(&blobs), queue, group, lease_id, &st.encryption, cfl,
            );
            return Ok((body, count, meta, rtt));
        }
        let (b, c, m) = empty();
        return Ok((b, c, m, rtt));
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
    // taking the pop admission slot / a pooled connection. A parked consumer
    // whose queue is quiet re-polls every backoff interval, so the rate of EMPTY
    // re-polls is O(#parked consumers) = O(#queues). The old order acquired the
    // SHARED admission budget on every such empty re-poll, and that O(#queues)
    // storm saturated the limiter (Vegas shrinks it under RTT pressure) — so a
    // freshly-woken REAL delivery pop queued behind thousands of empty re-polls on
    // acquire(), a priority inversion whose wait grew LINEARLY with the queue count
    // (the multitenant discovery-latency regression). An empty ring is zero DB
    // work, so it must cost zero limiter/pool traffic. Only three things need the
    // DB: a candidate to serve (a push marked the ring), a due keyset reseed (§8
    // floor), or a stale deferral-config refresh (§6) — each gated by a cheap
    // in-memory predicate here. The quiet re-poll short-circuits WITHOUT ever
    // touching the admission budget / the pool.
    let need_reseed = st.hotlist.reseed_due(qkey, group, now_ms, st.hotlist_reseed_ms);
    let need_cfg = !st.hotlist.cfg_fresh(qkey, now_ms, HOTLIST_CFG_TTL_MS);
    if !need_reseed && !need_cfg && !st.hotlist.has_ready(qkey, group, now_ms) {
        if st.hotlist.traced(qkey) {
            eprintln!("[hlt] quiet q={} g={} w={} t={}", queue, group,
                &worker[worker.len().saturating_sub(8)..], crate::hotlist::trace_now_ms());
        }
        let (b, c, m) = empty();
        return Ok((b, c, m, Duration::ZERO));
    }

    // ── POP FUSION (server/src/pop_fusion.rs). Steady-path serves only: the
    // side quests that need a private connection (deferral-config refresh,
    // reseed floor) stay on the direct path below, which they reach unchanged
    // because this branch requires them idle. take_batch is pure in-memory, so
    // the fused route touches neither the Vegas permit nor the pool — the
    // flush task takes ONE permit + ONE connection for the whole fused
    // transaction.
    if st.pop_fusion.enabled() && !need_cfg && !need_reseed {
        // Checkout width = the SQL's serve cap, 1:1. The old 2x over-fetch dated
    // from the random-candidate-scan era, when half a claim's candidates were
    // routinely stolen or empty. With the hot-list ring the candidates are
    // near-perfect (measured 2026-08-03: leased 0.0%, empty 1.0%) — and the
    // over-fetch systematically returned the surplus HALF of every claim with
    // no verdict (requeue 49% of all candidates), sending perfectly good
    // partitions to the BACK of the ready ring for a full extra lap (~1s at
    // the sparse shape's service rate). One line, half the latency.
    let k = (max_parts.max(1) as usize).clamp(2, 64);
        let want = batch.max(1) as u32;
        let cands = st.hotlist.take_batch(qkey, group, k, want, now_ms);
        if st.hotlist.traced(qkey) {
            eprintln!("[hlt] take q={} g={} w={} got={} est={} fused=1 t={}", queue, group,
                &worker[worker.len().saturating_sub(8)..], cands.len(),
                st.hotlist.ready_est(qkey, group, now_ms), crate::hotlist::trace_now_ms());
        }
        if cands.is_empty() {
            // The quiet gate said ready, but a concurrent serve drained the
            // ring first — same outcome as the direct path's empty take.
            let (b, c, m) = empty();
            return Ok((b, c, m, Duration::ZERO));
        }
        note_claim_ages(ticket, &cands, now_ms);
        let names: Vec<String> = cands.iter().map(|c| c.name.clone()).collect();
        let skip_window = st.hotlist.skip_window(qkey);
        let lease_ms = lease_seconds.max(1) as i64 * 1000;
        // Same cancellation-safety contract as the direct path: if this future
        // is dropped while parked on the oneshot, the guard requeues the
        // checked-out candidates. The flush may still commit the leases — the
        // next serve then sees them live ('leased' verdict) and they expire
        // into redelivery, the same at-least-once window the direct path has
        // between SQL completion and render.
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
        let t_claim = Instant::now();
        let verdict = st
            .pop_fusion
            .claim(
                queue.to_string(),
                group.to_string(),
                names,
                batch,
                lease_seconds,
                worker.to_string(),
                auto_ack,
                max_parts,
                sub_mode.to_string(),
                sub_from.to_string(),
                skip_window,
                tenant.to_string(),
                cfl.on,
            )
            .await;
        guard.armed = false;
        let cands = std::mem::take(&mut guard.cands);
        if st.hotlist.traced(qkey) {
            eprintln!("[hlt] fusedwait q={} g={} w={} ms={} t={}", queue, group,
                &worker[worker.len().saturating_sub(8)..], t_claim.elapsed().as_millis(),
                crate::hotlist::trace_now_ms());
        }
        match verdict {
            crate::pop_fusion::PopVerdict::Served { meta, blobs, states, rtt } => {
                return Ok(finish_pop_serve(
                    st, qkey, queue, group, cands, meta, blobs, states, now_ms, auto_ack,
                    lease_ms, lease_seconds, lease_id, rtt, cfl,
                ));
            }
            crate::pop_fusion::PopVerdict::FlushErr => {
                // Byte-identical to the direct path's DB-error handling:
                // requeue everything, return empty; nothing strands.
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
                return Ok((b, c, m, Duration::ZERO));
            }
        }
    }

    // Real DB work ahead: NOW take the serving permit + a pooled connection. The
    // limiter thus only ever gates genuine serving + the periodic per-(queue,group)
    // reseed/cfg floor (O(served + #queues / reseed_interval)), never the
    // O(#queues) quiet re-poll storm.
    let tr = st.hotlist.traced(qkey);
    let t_start = if tr { crate::hotlist::trace_now_ms() } else { 0 };
    let mut slot = st.admission.acquire(Lane::Pop).await;
    let t_adm = if tr { crate::hotlist::trace_now_ms() } else { 0 };
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            drop(slot);
            // Transport failure, not an empty queue: same 500 + body the legacy
            // scan path returns from its own pool.get arm. No candidate was
            // checked out yet (take_batch runs below), so there is nothing to
            // check back in — only the admission slot, dropped above.
            return Err(json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "{\"error\":\"pool\"}".to_string(),
            ));
        }
    };
    if tr {
        eprintln!("[hlt] attempt q={} start={} adm_wait={} pool_wait={}",
            queue, t_start, t_adm - t_start, crate::hotlist::trace_now_ms() - t_adm);
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
    // Checkout width, ceiling 64 — and the ceiling is LOAD-BEARING for a
    // deeper reason than the serial SQL leg it was written for. 2026-08-02,
    // after log_pop_list_v1 went batched (~6 statements per pop regardless of
    // candidate count), the ceiling was experimentally raised to 512 to
    // amortize the per-pop WAL commit over wide claims. Measured on the sparse
    // CM shape (1000 partitions x 1 msg/s, 8-core cell): WORSE by 4-13x —
    // p50 21.8s at k=512/batch 512, 5.9s at k=128/batch 128, vs ~1.5s at the
    // default k=20. The INFLIGHT hold is the CLIENT's whole cycle (work +
    // derived push + ack), so wide checkouts strip the ready ring for hundreds
    // of ms regardless of how cheap the SQL leg is, and the ring starves.
    // Wide-claim WAL amortization would need a server-side pipelined serve
    // (checkin before the client cycle), not a wider client-held checkout.
    let k = ((max_parts.max(1) as usize) * 2).clamp(2, 64);
    let want = batch.max(1) as u32;
    let mut cands = st.hotlist.take_batch(qkey, group, k, want, now_ms);
    if tr {
        eprintln!("[hlt] take q={} g={} got={} est={} fused=0 t={}", queue, group,
            cands.len(), st.hotlist.ready_est(qkey, group, now_ms),
            crate::hotlist::trace_now_ms());
    }
    if cands.is_empty() && need_reseed {
        hotlist_reseed_scan(
            &st.hotlist,
            &client,
            qkey,
            group,
            now_ms,
            st.hotlist_reseed_full_ms,
            st.hotlist_reseed_window_ms,
        )
        .await;
        cands = st.hotlist.take_batch(qkey, group, k, want, now_ms);
    }
    if cands.is_empty() {
        drop(client);
        drop(slot);
        let (b, c, m) = empty();
        return Ok((b, c, m, Duration::ZERO));
    }
    note_claim_ages(ticket, &cands, now_ms);

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
            sub_mode, sub_from, skip_window, tenant, cfl.on,
        ),
    )
    .await;
    let rtt = t0.elapsed();
    if tr {
        eprintln!("[hlt] sqldone q={} cands={} sql_ms={}", queue, names.len(), rtt.as_millis());
    }
    if matches!(res, Ok(Ok(_))) { slot.commit_done(rtt); }
    drop(slot);
    // The await returned (not dropped) — take ownership of the candidates back and
    // disarm the guard; every path below runs an explicit checkin.
    guard.armed = false;
    let cands = std::mem::take(&mut guard.cands);

    let (meta_txt, blobs, states_txt) =
        match db::resolve_query_timeout(res, client, cancel_token, "pop_list", &st.metrics) {
            Some(t) => t,
            None => {
                // DB error/timeout — the candidates were checked out (INFLIGHT).
                // Re-append them all (Requeue) so nothing is stranded, and only
                // THEN surface the failure. The check-in is unconditional and
                // must stay ahead of every exit from this arm: the guard was
                // disarmed above (the await returned rather than being dropped),
                // so this call is the ONLY thing that puts the partitions back in
                // the ring — a mark/promote/reseed on an INFLIGHT entry is a
                // no-op, so an early return here would strand them permanently.
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
                // resolve_query_timeout has already cancelled the statement
                // server-side, quarantined the connection and counted the error;
                // report it like the legacy scan path instead of parking on what
                // looks like an empty queue.
                return Err(json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "{\"error\":\"pop failed\"}".to_string(),
                ));
            }
        };

    Ok(finish_pop_serve(
        st, qkey, queue, group, cands, meta_txt, blobs, states_txt, now_ms, auto_ack,
        lease_ms, lease_seconds, lease_id, rtt, cfl,
    ))
}

// The post-SQL half of a hot-list serve: map tri-state verdicts to the checked
// out candidates, check them back into the ring, and render the response. ONE
// body shared by the direct path and the pop-fusion path (pop_fusion.rs) — the
// fused route must never drift from the direct one, so it cannot have its own
// copy of this logic.
#[allow(clippy::too_many_arguments)]
fn finish_pop_serve(
    st: &Arc<AppState>,
    qkey: &str,
    queue: &str,
    group: &str,
    cands: Vec<crate::hotlist::Candidate>,
    meta_txt: String,
    blobs: Vec<Vec<u8>>,
    states_txt: String,
    now_ms: i64,
    auto_ack: bool,
    lease_ms: i64,
    lease_seconds: i32,
    lease_id: &str,
    rtt: Duration,
    cfl: Conflation,
) -> (String, usize, PopMeta, Duration) {
    let empty = || render_pop_parts(&[], None, queue, group, lease_id, &st.encryption, cfl);
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
    if st.hotlist.traced(qkey) {
        let (mut took, mut leased, mut emp, mut req) = (0u32, 0u32, 0u32, 0u32);
        for r in &results {
            match r.verdict {
                crate::hotlist::Verdict::Took => took += 1,
                crate::hotlist::Verdict::Leased(_) => leased += 1,
                crate::hotlist::Verdict::Empty => emp += 1,
                crate::hotlist::Verdict::Requeue => req += 1,
            }
        }
        eprintln!("[hlt] tri q={} g={} w={} took={} leased={} empty={} req={} t={}",
            queue, group, &lease_id[lease_id.len().saturating_sub(8)..], took, leased, emp, req,
            crate::hotlist::trace_now_ms());
    }
    st.hotlist
        .checkin(qkey, group, results, now_ms, auto_ack, lease_ms);

    for part in &parsed.partitions {
        st.hotlist
            .note_partition_id(qkey, &part.partition, &part.partition_id);
    }
    let (body, count, meta) = render_pop_parts(
        &parsed.partitions, Some(&blobs), queue, group, lease_id, &st.encryption, cfl,
    );
    (body, count, meta, rtt)
}

// 19-wildcard-hotlist §8: keyset-paginated reseed. Walk the (queue, group)'s
// probably-pending partitions in (last_write_at, id) order (bounded ~10k pages), interning each
// name + remembering its id and marking it into the ring, then stamp the reseed
// clock. This is the cold-start populator AND the correctness floor for any
// missed mark / dropped mesh hint.
//
// The scan comes in two shapes and this entry point picks between them per ring
// (see HotList::reseed_mode): a FULL walk over every partition of the queue, and a
// WINDOWED walk over the partitions written recently. Measured in prod on
// 2026-08-11 (9.5k partitions on one queue, 63 rings, reseed every 30s), the full
// walk cost 49ms and returned zero rows — 2,089,774 ms of database time per hour,
// more than the entire pop path at 24x the call count. The windowed walk answers
// the same question on the same data in 0.375ms. The full walk stays as the floor
// for the classes a window cannot see, just at a cadence that reflects how rare
// they are.
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
    full_interval_ms: i64,
    window_ms: i64,
) {
    let ticket = hl.reseed_begin(qkey, group, now_ms, full_interval_ms, window_ms);
    // The background floor discards the outcome on purpose: a failed pass here is a
    // ring that keeps its clocks and retries, and B2 already reports the streak from
    // `reseed_finish` — one line per ring per failed pass would be a flood. The
    // event-driven repairs below are the ones an operator is waiting on, and those
    // report per call.
    let _ = hotlist_reseed_run(hl, client, ticket, false).await;
}

/// What one reseed walk did. Minimal on purpose, and EXTENDED BY ADDING fields: the
/// seek path reflects it in its HTTP response (A4) and the repair paths log it.
pub(crate) struct ReseedOutcome {
    /// Which scan ran. Always `Full` on the repair paths that read this today; carried
    /// because the field set is the shared one and the periodic floor returns it too.
    #[allow(dead_code)]
    pub mode: crate::hotlist::ReseedMode,
    pub rows: usize,
    /// Every page came back without a SQL error. False means the ring was NOT repaired
    /// — the walk stopped wherever it broke.
    pub ok: bool,
    /// `reseed_finish`'s identity check passed (B3). False means the ring was replaced
    /// mid-walk, which leaves the NEW ring owing a cold-start full walk — a repair by
    /// another route, not a lost one.
    pub stamped: bool,
}

// 19-wildcard-hotlist §8: a reseed that is FORCED full — the repair every cursor move
// with no write behind it owes its own ring, and (when broadcasting) the peers' rings.
//
// The windowed pass is sound against everything a push creates; the one thing it is
// blind to by construction is a cursor moving BACKWARDS, which makes old partitions
// pending with no write to date them. That is a seek (log_seek_consumer_group_v1 /
// log_seek_partition_v1) or a consumer-group delete (A1), and it is why
// reseed_after_seek exists at all.
//
// Broadcasting matters because the local walk only fixes the ring of the broker that
// served the request. Before windowing, every peer healed within one reseed interval
// (≤30s) because every peer walked everything that often; with the full walk moved to
// a slower cadence that recovery would stretch to the full interval. So the repair's own
// rows are pushed into the same coalescing dirty set the push path uses, and the
// existing 20ms flusher carries them as an ordinary HOTLIST_DIRTY batch — no new frame
// type, so a peer mid rolling-upgrade handles it exactly as it always has.
//
// C2: `broadcast` is that half, and the caller turns it OFF under
// QUEEN_HOTLIST_RESEED_FULL_MS=0. That knob is documented as the revert to
// pre-windowing behaviour, and pre-windowing this fan-out did not exist — with every
// periodic pass full again at the 30s cadence, every peer re-discovers the seek within
// one floor exactly as it used to, so the frames buy nothing and the kill switch is
// honest. The local walk still runs: it predates the windowing by a year.
//
// A failed walk is REPORTED here rather than swallowed (A4). This path is the one an
// operator is synchronously waiting on — "replay from yesterday" — and its failure used
// to be an HTTP 200 with no replay and nothing in the logs.
pub(crate) async fn hotlist_reseed_full(
    hl: &crate::hotlist::HotList,
    client: &deadpool_postgres::Client,
    qkey: &str,
    group: &str,
    now_ms: i64,
    broadcast: bool,
) -> ReseedOutcome {
    let ticket = hl.reseed_begin_full(qkey, group, now_ms);
    let out = hotlist_reseed_run(hl, client, ticket, broadcast).await;
    if !out.ok {
        let (tenant, queue) = split_tenant_queue(qkey);
        tracing::warn!(
            target: "hotlist",
            tenant = %tenant,
            queue = %queue,
            group = %group,
            rows = out.rows,
            broadcast,
            "forced full reseed FAILED: the cursor move committed but the partitions it \
             made pending were not re-discovered; this ring recovers on its next full walk"
        );
    } else if !out.stamped {
        let (tenant, queue) = split_tenant_queue(qkey);
        tracing::warn!(
            target: "hotlist",
            tenant = %tenant,
            queue = %queue,
            group = %group,
            rows = out.rows,
            "forced full reseed landed on a ring that was replaced mid-walk; the new one \
             cold-starts full, so the repair is deferred to its first contact"
        );
    }
    out
}

// The shared keyset walk. The ticket carries the ring the rows land on and the mode
// that picks the bound the one reseed statement is pinned to; `broadcast` additionally
// queues each discovered partition as a mesh dirty hint.
//
// Errors abandon the walk (the next attempt retries) — the ring simply stays as-is,
// never wrong. The clock is stamped even on a failed walk, deliberately: it is the
// cadence throttle, and NOT stamping it would turn a database that is refusing this
// query into a hot retry loop on the pop path. A failed FULL walk does not advance the
// full clock, so the next attempt is full again.
async fn hotlist_reseed_run(
    hl: &crate::hotlist::HotList,
    client: &deadpool_postgres::Client,
    ticket: crate::hotlist::ReseedTicket,
    broadcast: bool,
) -> ReseedOutcome {
    let mode = ticket.mode();
    let (tenant, queue) = split_tenant_queue(ticket.qkey());
    let group = ticket.group();
    // ONE statement for both modes, log_hotlist_reseed_window_v1, keyset on
    // (last_write_at, id); the mode only decides the lower bound it is pinned to
    // (ReseedMode::scan_bounds). The full walk used to be its own statement,
    // log_hotlist_reseed_v1, keyset on id with queen.queues joined as a relation — and
    // under the GENERIC plan prepare_cached converges to after five executions, every
    // parameter is unknown, p_limit included, which the planner assumes keeps 10% of
    // the rows: an ordered walk of log_partitions_pkey with the queue as a join filter
    // then looked 10x cheaper than the bitmap on the queue's own index and won. At run
    // time the limit is 10k, above every queue, so the walk never stopped early: every
    // page read EVERY partition in the cell. Measured on the 2026-08-22 soak's shape
    // (229 rings, 827k partitions): 851k buffers and 303 ms per ring against 20k and
    // ~10 ms for the custom plan, the same for a 500-partition queue as for a 5,000
    // one — 229 full-cell scans per five-minute cycle, about one core continuously,
    // and growing with the partitions heap (every push bumps the indexed
    // last_write_at, so none of those updates is HOT: 124 -> 228 MB in thirty minutes
    // of pushes). The windowed statement orders by its index's own leading columns and
    // resolves the queue by scalar subquery, which removes that plan structurally (its
    // header in 004_log_pop.sql); pinned to '-infinity' it returns the id-keyset walk's
    // exact set in (last_write_at, id) order, which a reseed does not care about.
    // Same shape, same generic plan: 25k buffers, 19 ms, Theta(partitions in the queue).
    //
    // `after_write`/`after_id` are an opaque echo of the last row the previous page
    // returned; ('-infinity', nil) starts a walk. B1: the lower bound belongs to the
    // WALK, not to the page — each page is its own statement with its own now(), so
    // re-deriving it per page let the bound creep forward while the cursor climbed from
    // the oldest row, and a partition written into the band between the two was
    // excluded before the cursor reached it. The first windowed page derives the cutoff
    // and reports it, the rest echo it back; a full walk starts with it pinned.
    let (window_ms, pinned) = mode.scan_bounds();
    let mut after_id = NIL_UUID.to_string();
    let mut after_write = "-infinity".to_string();
    let mut cutoff: Option<String> = pinned.map(str::to_string);
    let mut ok = true;
    let mut seen = 0usize;
    for _ in 0..HOTLIST_RESEED_MAX_PAGES {
        // Bound before the match: the borrow of `cutoff` in the argument list would
        // otherwise outlive the arm that assigns it.
        let page = db::hotlist_reseed_window(
            client,
            queue,
            group,
            &after_write,
            &after_id,
            HOTLIST_RESEED_PAGE,
            window_ms,
            cutoff.as_deref(),
            tenant,
        )
        .await;
        let rows = match page {
            Ok((r, cut)) => {
                if let Some((id, _, write)) = r.last() {
                    after_id = id.clone();
                    after_write = write.clone();
                }
                if cutoff.is_none() {
                    cutoff = cut;
                }
                r.into_iter().map(|(id, name, _)| (id, name)).collect::<Vec<_>>()
            }
            Err(_) => {
                ok = false;
                break;
            }
        };
        if rows.is_empty() {
            break;
        }
        seen += rows.len();
        for (id, name) in &rows {
            hl.reseed_row(&ticket, id, name);
            if broadcast {
                // Scoped to the group this walk is repairing: the peers' rings for
                // every OTHER group of the queue learn nothing from a seek that moved
                // one cursor, and marking them would be 9,563 partitions of ghost
                // entries each. A peer too old to read the field over-marks, exactly
                // as it did before the field existed.
                hl.note_dirty(ticket.qkey(), name, Some(ticket.group()));
            }
        }
        if rows.len() < HOTLIST_RESEED_PAGE as usize {
            break;
        }
    }
    let stamped = hl.reseed_finish(ticket, ok);
    ReseedOutcome { mode, rows: seen, ok, stamped }
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
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    // PLAN_CONFLATION §3.3: the same two refusals as the wildcard route — a
    // group-less pinned pop has no policy to hang conflation on either. Ahead of
    // the maintenance gate, same reasoning as handle_pop.
    if let Some(r) = conflation_refusal(p.conflation, p.consumer_group.is_some(), auto_ack) {
        return r;
    }
    // Same shape as handle_pop, written out for the same reason (the OpenAPI
    // generator reads the StatusCode variants out of the handler body).
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return if p.conflation == Some(true) {
            json(StatusCode::OK, POP_PAUSED_CONFLATING.to_string())
        } else {
            json(StatusCode::NO_CONTENT, POP_PAUSED.to_string())
        };
    }
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    // `max_parts` is irrelevant here (a pinned pop is one partition), so M5 does
    // not apply; the flag only switches the claim to the tail (§3.2).
    let cfl = resolve_conflation(&st, &queue, &group, tenant.as_str(), p.conflation).await;
    let sub_mode = p
        .subscription_mode
        .as_deref()
        .map(crate::config::normalize_subscription_mode)
        .unwrap_or_else(|| st.default_subscription_mode.clone());
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
        // ── Pending gate (mirrors the wildcard gate in handle_pop): every
        // parked re-poll used to take an admission permit, a pooled connection
        // and run the FULL pop_specific SP — at Gate-style 5s windows that is
        // ~9 full SP calls per idle runner per window. The probe is one indexed
        // row read (log_partition_has_pending_v1), takes no permit, and a
        // `false` is definitive: park without popping. `true` on first contact
        // of a (partition, group) is deliberate — the full pop is what seeds
        // the subscription cursor, and for subscriptionMode=new the seed is the
        // tail AT SEED TIME, so the gate must never defer it (004_log_pop).
        // When the wait window is over (or wait=false) fall through to the full
        // pop once: the SP response builder stays the single authority on the
        // empty wire shape. QUEEN_POP_PENDING_GATE=false disables.
        if st.pop_pending_gate && wait && Instant::now() < deadline {
            let pending = match st.pool.get().await {
                Ok(c) => db::partition_has_pending(&c, &queue, &partition, &group, tenant.as_str())
                    .await
                    .unwrap_or(true),
                Err(_) => true, // probe unavailable → fall back to the full pop (safe)
            };
            if !pending {
                // Same park as the post-SP branch below: backoff re-poll,
                // push-wake resets it, parked gauge held for the window.
                backoff_count += 1;
                let interval = st.pop_backoff_interval(backoff_count);
                let waitd = deadline
                    .saturating_duration_since(Instant::now())
                    .min(interval);
                let _parked = st.metrics.parked.enter(tenant.as_str(), &queue);
                if st.notifier.wait_queue(&qkey, waitd).await {
                    backoff_count = 0;
                }
                continue;
            }
        }
        let mut slot = st.admission.acquire(Lane::Pop).await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(slot);
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
                auto_ack, &sub_mode, &sub_from, tenant.as_str(), cfl.on,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        if matches!(res, Ok(Ok(_))) { slot.commit_done(rtt); }
        drop(slot);
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
        let (body, count, meta) =
            build_pop_specific_response(&txt, &queue, &partition, &group, lease_id, &st.encryption, cfl);
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
        return json(pop_status(count, cfl), body);
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
    // PLAN_CONFLATION §3.1 — same query parameter as the queue-scoped routes.
    #[serde(rename = "conflation")]
    conflation: Option<bool>,
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
    let namespace = p.namespace.unwrap_or_default();
    let task = p.task.unwrap_or_default();
    if namespace.is_empty() && task.is_empty() {
        return json(
            StatusCode::BAD_REQUEST,
            "{\"success\":false,\"error\":\"namespace or task is required\",\"messages\":[]}".to_string(),
        );
    }
    let batch = p.batch.unwrap_or(200);
    let auto_ack = p.auto_ack.unwrap_or(false);
    let wait = p.wait.unwrap_or(false);
    let timeout_ms = p.timeout.unwrap_or(st.pop_default_timeout_ms);
    // PLAN_CONFLATION §3.3: same two refusals as the queue-scoped routes, ahead of
    // the maintenance gate (see handle_pop).
    if let Some(r) = conflation_refusal(p.conflation, p.consumer_group.is_some(), auto_ack) {
        return r;
    }
    // Same shape as handle_pop, written out for the same reason (the OpenAPI
    // generator reads the StatusCode variants out of the handler body).
    if st.pop_maintenance.load(Ordering::Relaxed) {
        return if p.conflation == Some(true) {
            json(StatusCode::OK, POP_PAUSED_CONFLATING.to_string())
        } else {
            json(StatusCode::NO_CONTENT, POP_PAUSED.to_string())
        };
    }
    let group = p.consumer_group.unwrap_or_else(|| "__QUEUE_MODE__".to_string());
    // A discovery pop spans QUEUES, so there is no single (queue, group) whose
    // stored policy the broker could cache here — and the request flag must NEVER
    // become the echo by default: queen.log_pop_discover_wire_v1 resolves the
    // durable policy per matched queue (COALESCE(cgm.conflation, requested)) and
    // applies THAT, so echoing the request would report a policy the SP may not
    // have used — the dangerous direction (the SDK concludes conflation is in
    // force and drains the backlog message by message, exactly what §4 forbids).
    //
    // So the SP resolves and REPORTS: its result JSON carries `conflation` (the
    // policy every matched queue agreed on) and `conflationConflict` (some matched
    // queue's stored policy disagreed with the request), and `cfl` below is built
    // from the answer, after the call. `requested` only decides the M5 partition
    // sizing — a budget knob, never a semantic one; the message budget still caps
    // the response either way. Known wart, documented rather than fixed: a
    // consumer that does NOT declare the flag against an already-conflating group
    // is served conflated (the SP's stored policy wins, as it must) with
    // max_parts=1, i.e. one message per round trip. Correct, just slow, and the
    // broker cannot do better without a per-queue lookup it has no key for.
    let requested = if group == "__QUEUE_MODE__" { None } else { p.conflation };
    // M5 (§3.2), as in handle_pop.
    let max_parts = if requested == Some(true) {
        p.partitions.unwrap_or(batch).clamp(1, 64)
    } else {
        p.partitions.unwrap_or(1)
    };
    let sub_mode = p
        .subscription_mode
        .as_deref()
        .map(crate::config::normalize_subscription_mode)
        .unwrap_or_else(|| st.default_subscription_mode.clone());
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
        // ── Pending gate, discovery flavor (see handle_pop_specific above):
        // namespace/task-scoped probe (log_discover_has_pending_v1). The probe
        // stays TRUE while the group is unregistered on any matched queue —
        // the first discovery pop is what stamps the durable subscription
        // timestamp, and deferring that past a push would skip it under
        // subscriptionMode=new (004_log_pop). Falls through to the full pop
        // when the wait window is over, same single-authority empty shape.
        if st.pop_pending_gate && wait && Instant::now() < deadline {
            let pending = match st.pool.get().await {
                Ok(c) => db::discover_has_pending(&c, &namespace, &task, &group, tenant.as_str())
                    .await
                    .unwrap_or(true),
                Err(_) => true, // probe unavailable → fall back to the full pop (safe)
            };
            if !pending {
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
        }
        let mut slot = st.admission.acquire(Lane::Pop).await;
        let client = match st.pool.get().await {
            Ok(c) => c,
            Err(_) => {
                st.metrics.record_db_error();
                drop(slot);
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
                auto_ack, max_parts, &sub_mode, &sub_from, tenant.as_str(), requested,
            ),
        )
        .await;
        let rtt = t0.elapsed();
        if matches!(res, Ok(Ok(_))) { slot.commit_done(rtt); }
        drop(slot);
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
        // PLAN_CONFLATION §3.3: `cfl` comes out of the SP's own report, so the
        // echo is what was APPLIED (see the `requested` comment above).
        let (body, count, meta, cfl) =
            build_pop_discover_response(&txt, &group, lease_id, &st.encryption);
        // A conflict is reported once per response, exactly like the queue-scoped
        // route does in resolve_conflation: counter + rate-limited line, never a
        // per-request log (a mismatched fleet would flood).
        if cfl.conflict {
            // No per-queue counter: this route spans queues and has no single
            // one to attribute to. The scope label is the namespace/task pair,
            // the same identity the SDKs key their warn-once registry on.
            let scope = format!(
                "{}/{}",
                if namespace.is_empty() { "*" } else { &namespace },
                if task.is_empty() { "*" } else { &task }
            );
            note_conflation_conflict(&st, tenant.as_str(), None, &scope, &group, cfl.on, requested);
        }
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
        return json(pop_status(count, cfl), body);
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

// Wildcard pop response: SP result is
// {"partitions":[{partition,partitionId,deliveryAttempt,segments}]}.
// `bin_blobs`: Some on the bin_v1 path — native bytea[] blobs aligned with the
// meta's segment traversal order (base64 `blob` fields absent from the JSON).
#[allow(clippy::too_many_arguments)]
fn build_pop_response(
    txt: &str,
    bin_blobs: Option<&[Vec<u8>]>,
    queue: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
    cfl: Conflation,
) -> (String, usize, PopMeta) {
    let parsed: PopResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => return pop_error_body("parse"),
    };
    if let Some(e) = parsed.error {
        return pop_error_body(&e);
    }
    render_pop_parts(&parsed.partitions, bin_blobs, queue, group, lease_id, enc, cfl)
}

// Discovery pop response (GET /api/v1/pop?namespace=&task=). Same SP result shape
// as build_pop_response plus the conflation report queen.log_pop_discover_wire_v1
// adds — which is why this route has its own builder: the effective policy is not
// known until the SP answers, so it is returned here rather than passed in.
//
// PLAN_CONFLATION §3.3. The SP applies COALESCE(cgm.conflation, requested) per
// matched queue; `conflation` is what every matched queue agreed on and
// `conflationConflict` says some matched queue's stored policy disagreed with the
// request. Both keys are absent whenever the request carried no flag (the SP does
// not even look), in which case the response carries neither — byte-identical to
// a pre-conflation broker.
fn build_pop_discover_response(
    txt: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
) -> (String, usize, PopMeta, Conflation) {
    let parsed: PopResult = match serde_json::from_str(txt) {
        Ok(p) => p,
        Err(_) => {
            let (b, n, m) = pop_error_body("parse");
            return (b, n, m, Conflation::OFF);
        }
    };
    if let Some(e) = parsed.error {
        let (b, n, m) = pop_error_body(&e);
        return (b, n, m, Conflation::OFF);
    }
    let cfl = Conflation {
        on: parsed.conflation.unwrap_or(false),
        conflict: parsed.conflation_conflict.unwrap_or(false),
    };
    // Discovery spans queues: no single top-level queue name (see the caller).
    let (body, count, meta) =
        render_pop_parts(&parsed.partitions, None, "", group, lease_id, enc, cfl);
    (body, count, meta, cfl)
}

// Specific-partition pop response: SP result is single-partition shaped
// ({"segments":[...],"partitionId":..}) with no partition NAME — the broker
// supplies the name from the request path. Adapts it to the same per-partition
// structure the wildcard renderer consumes so every message emits the identical
// per-message JSON and top-level fields.
#[allow(clippy::too_many_arguments)]
fn build_pop_specific_response(
    txt: &str,
    queue: &str,
    partition: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
    cfl: Conflation,
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
        delivery_attempt: parsed.delivery_attempt,
        segments: parsed.segments,
    };
    render_pop_parts(std::slice::from_ref(&part), None, queue, group, lease_id, enc, cfl)
}

// Shared renderer: decode + slice each partition's segment frames into the
// wire per-message JSON, then wrap with the common top-level fields.
#[allow(clippy::too_many_arguments)]
fn render_pop_parts(
    parts: &[PopPart],
    bin_blobs: Option<&[Vec<u8>]>,
    queue: &str,
    group: &str,
    lease_id: &str,
    enc: &crate::encryption::Encryption,
    // PLAN_CONFLATION §3.1: the two conditional top-level keys. Emitted ONLY when
    // true, so every existing deployment keeps byte-identical response bytes.
    cfl: Conflation,
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
        let delivery_attempt = part.delivery_attempt.unwrap_or(1).max(1).to_string();
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
                out.push_str("\",\"deliveryAttempt\":");
                out.push_str(&delivery_attempt);
                out.push('}');
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
    // PLAN_CONFLATION §3.1. `conflation` rides EVERY conflating response, empty
    // ones included — that is what lets an SDK detect an old broker on the first
    // round trip (§4 degrade-loudly) instead of silently draining a backlog
    // message by message. `conflationConflict` says the request disagreed with
    // the stored group policy and the stored one won (§3.3).
    if cfl.on {
        out.push_str(",\"conflation\":true");
    }
    if cfl.conflict {
        out.push_str(",\"conflationConflict\":true");
    }
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
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
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
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
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
    /// PLAN_CONFLATION §2.4: carries the SP's `conflated` skipped-position count
    /// when the lease was conflating; None for a plain lease.
    Committed(Option<i64>),
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
    // PLAN_CONFLATION §2.4: per-item skipped-position count, present only for a
    // clean ack of a CONFLATING lease. None everywhere else, so a flag-off ack
    // result carries no `conflated` key at all.
    let mut conflated: Vec<Option<i64>> = vec![None; n];

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
    // Ack-lane admission, taken BEFORE the pool checkout (the reverse order
    // deadlocks against the slot-then-pool paths; see admission.rs). Scoped
    // exactly like `client`: with fusion OFF it spans the loop, with fusion ON
    // it is taken per-iteration next to the lazy client and dropped with it —
    // never held across a fusion commit-wait.
    let mut _ack_slot: Option<crate::admission::Slot> = None;
    if !fusion_on {
        _ack_slot = crate::admission::lane_slot(crate::admission::Lane::Ack).await;
        match st.pool.get().await {
            Ok(c) => client = Some(c),
            Err(_) => {
                st.metrics.record_db_error();
                for e in errors.iter_mut() {
                    *e = Some("pool".to_string());
                }
                return render_ack_results(&acks, &success, &errors, &lease_released, &dlq_flags, &noop_flags, &conflated);
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
                        crate::ack_fusion::AckVerdict::Committed(c) => FastAck::Committed(c),
                        crate::ack_fusion::AckVerdict::Rejected => FastAck::FallBack,
                        crate::ack_fusion::AckVerdict::FlushErr => FastAck::FlushErr,
                    }
                } else {
                    // Unchanged synchronous fast path (byte-identical to before,
                    // plus the optional `conflated` read — §2.4, no request change).
                    let hit = match db::ack_at(
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
                        Ok(txt) => serde_json::from_str::<serde_json::Value>(&txt).ok(),
                        Err(_) => None,
                    };
                    let hit_ok = hit
                        .as_ref()
                        .and_then(|v| v.get("ok").and_then(|x| x.as_bool()))
                        .unwrap_or(false);
                    if hit_ok {
                        FastAck::Committed(
                            hit.as_ref().and_then(|v| v.get("conflated").and_then(|x| x.as_i64())),
                        )
                    } else {
                        FastAck::FallBack
                    }
                };

                match outcome {
                    FastAck::Committed(cfl_skipped) => {
                        // Same per-item result shape as the SQL happy path.
                        for &i in &idxs {
                            success[i] = true;
                            lease_released[i] = true;
                        }
                        // PLAN_CONFLATION §2.4/§6.2: the skipped-position count is
                        // a property of the (partition, worker) LEASE, so it is
                        // reported on the item that closed it — the first of the
                        // group — and counted once, never once per item.
                        if let Some(c) = cfl_skipped {
                            if let Some(&i0) = idxs.first() {
                                conflated[i0] = Some(c);
                            }
                            st.metrics.conflated.fetch_add(c.max(0) as u64, Ordering::Relaxed);
                            if let Some(q) = queue_name.as_ref() {
                                st.metrics.per_queue.add_conflated(tenant, q, c.max(0) as u64);
                            }
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
            _ack_slot = crate::admission::lane_slot(crate::admission::Lane::Ack).await;
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
                    // PLAN_CONFLATION §2.4: the hash path reports the same count
                    // on a clean conflating ack (the DLQ/nack branches never do —
                    // they release the lease without completing the span).
                    if let Some(c) = v.get("conflated").and_then(|x| x.as_i64()) {
                        if let Some(&i0) = idxs.first() {
                            conflated[i0] = Some(c);
                        }
                        st.metrics.conflated.fetch_add(c.max(0) as u64, Ordering::Relaxed);
                        if let Some(q) = queue_name.as_ref() {
                            st.metrics.per_queue.add_conflated(tenant, q, c.max(0) as u64);
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
            _ack_slot = None;
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

    render_ack_results(&acks, &success, &errors, &lease_released, &dlq_flags, &noop_flags, &conflated)
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

#[allow(clippy::too_many_arguments)]
fn render_ack_results(
    acks: &[Ack],
    success: &[bool],
    errors: &[Option<String>],
    lease_released: &[bool],
    dlq_flags: &[bool],
    noop_flags: &[bool],
    // PLAN_CONFLATION §2.4: emitted ONLY where present, so a flag-off ack result
    // is byte-identical to the pre-feature one.
    conflated: &[Option<i64>],
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
        if let Some(c) = conflated[i] {
            out.push_str(",\"conflated\":");
            out.push_str(&c.to_string());
        }
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

    // Ack-lane admission BEFORE the pool checkout (ordering contract,
    // admission.rs). Held for the single renew statement.
    let _slot = crate::admission::lane_slot(crate::admission::Lane::Ack).await;
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

// ============================================================================
// THE HTTP -> WIRE DEMUX (PLAN_KV_TIMERS.md §8.2), which no design specified.
//
// The transaction wire the STORED PROCEDURE speaks is parallel arrays (pushes,
// acks, and now kv and timers). The transaction wire the CLIENTS speak is
// `{"operations":[...]}` — ONE FLAT array, demuxed here by `type`. Those are two
// different index spaces, and mapping between them is this file's job. It is
// also, per §16, the single place in this feature where a contract two shipped
// clients already read can be silently misaligned, so every rule below is
// written to be checkable rather than believed.
//
// WHERE THE TWO NEW ARRAYS LIVE ON THE REQUEST, AND WHY IT IS NOT NEGOTIABLE.
// `kv` and `timers` are TOP-LEVEL fields of the request body, never elements of
// `operations` (§10.4). The reason is a silent failure in Go: two struct fields
// carrying the same JSON key at the same level are BOTH DROPPED by
// `encoding/json`, with no error and no warning. Growing `Operation` a `kv` leg
// would therefore let a body go out with ZERO kv ops while the broker committed
// a transaction with no gate — the `putIfAbsent` the bundle existed for would
// simply never have happened, and nothing anywhere would say so. An op that
// still arrives as `{"type":"kv"}` inside `operations` gets a NAMED 400 below,
// which is the best failure available and the same mechanism that gives a new
// client against an old broker a nominative refusal instead of a silent drop.
//
// THE FLAT INDEX SPACE, AND HOW ALIGNMENT IS MAINTAINED. `results[]` is indexed
// by the flat ordinal, and the layout is APPEND-ONLY:
//
//     [0, ops_flat)                             `operations`, exactly as today
//     [ops_flat, ops_flat + kv_n)               the top-level `kv` array
//     [ops_flat + kv_n, + timers_n)             the top-level `timers` array
//
// Stated so it can be checked: a push or an ack NEVER changes index because a
// rider is present, and a bundle carrying neither array produces a `results[]`
// of exactly today's length with exactly today's contents. Riders can only ever
// appear AFTER the last index that exists today. There is no interleaving rule
// to get wrong, which is precisely why this layout was chosen over honouring
// some request-order between three arrays that JSON does not order anyway.
//
// `failedIndex` (§8.2 point 4, §8.3) is in the FLAT space. The KV procedure
// raises with its own ARRAY-LOCAL ordinal in the DETAIL, and the translation
// `flat = kv_base + local` happens in exactly one place, `txn_fail_precondition`.
// Getting it wrong points the client at somebody else's operation.
//
// THE COUNT GUARD, PORTED ACROSS THE TWO SPACES (§8.2 point 3, §6.4). Each
// procedure guards its own array ("N ops in, N results out, no unfilled
// ordinal"). That guard is not expressible BETWEEN two index spaces, so it is
// rewritten here as "every flat ordinal has exactly one result", and it fails
// LOUDLY. It catches a second and sharper thing, which is the reason it is not
// decoration: a NEW BROKER against a database whose wire procedure predates the
// kv/timers graft would have its `kv` array SILENTLY IGNORED — the bundle
// commits, the client reads `success:true`, and the gate that was the whole
// point of the bundle never ran. A missing or short rider result array IS that
// case, and it must never read as success.
// ============================================================================

/// Edge ceilings for the rider arrays.
///
/// SEAM, identical to the one `handlers/kv.rs` declares and for the same reason:
/// `Config` already resolves every one of these (`kv_max_value_bytes`,
/// `timers_max_payload_bytes`, `timers_max_horizon_s`), but `AppState` does not
/// carry them yet and `AppState` is not this phase's file. For the HTTP broker
/// the two agree by construction — same variable, same default. For the EMBEDDED
/// broker they can diverge, because its configuration comes from a builder and
/// not from the process environment. Three fields on `AppState` close it.
fn wire_env_usize(key: &'static str, def: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(def)
}

fn wire_kv_max_value_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| wire_env_usize("QUEEN_KV_MAX_VALUE_BYTES", 65_536))
}

fn wire_timers_max_payload_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| wire_env_usize("QUEEN_TIMERS_MAX_PAYLOAD_BYTES", 1_048_576))
}

fn wire_timers_max_horizon_ms() -> i64 {
    static V: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *V.get_or_init(|| wire_env_usize("QUEEN_TIMERS_MAX_HORIZON_S", 7_776_000) as i64 * 1000)
}

/// The WIRE caps, which are deliberately tighter than the HTTP ones and are the
/// mirror of the constants the KV procedure carries for `p_in_wire = true`
/// (64 ops / 256 keys). Two numbers in two places is a cost paid on purpose: the
/// procedure is the floor nothing can get under (it also protects the embedded
/// broker, which never passes through a handler), and this is the guard that
/// refuses the body BEFORE a `Lane::Push` slot, a pooled connection and a blob
/// pack are spent on it. §6.1 point 4 is why a key budget exists at all next to
/// an op budget: 63 `getMany` of 256 keys is 16 128 rows read under the
/// OUTERMOST lock space of the whole product.
const WIRE_KV_MAX_OPS: usize = 64;
const WIRE_KV_MAX_KEYS: usize = 256;
const WIRE_TIMERS_MAX_OPS: usize = 256;

/// The two rider arrays of one transaction, already validated and prepared, plus
/// the flat base of each (see the layout above).
struct TxnRiders {
    kv: Vec<serde_json::Value>,
    timers: Vec<serde_json::Value>,
    kv_base: usize,
    timers_base: usize,
    /// Smallest `delayMs` among this bundle's schedules, for the post-commit
    /// sweeper wake (§7.4). `None` when the bundle schedules nothing.
    min_delay_ms: Option<i64>,
}

/// Read one top-level rider array.
///
/// `null` is accepted as absent and is not a nicety: it is what every serializer
/// emits for an unset optional field, and it is the same tolerance the wire
/// procedure buys on its side with `jsonb_typeof` instead of
/// `jsonb_array_length` (§6.3). Rejecting it would break, at the moment a client
/// merely ADDS the field, bundles that work today.
// The Err is a fully rendered `Response`, which is this module's error currency
// on every handler path; boxing it would add an allocation to a value that is
// returned immediately by the only caller.
#[allow(clippy::result_large_err)]
fn txn_rider_array(
    root: &serde_json::Value,
    key: &'static str,
    txn_id: &str,
) -> Result<Vec<serde_json::Value>, Response> {
    match root.get(key) {
        None | Some(serde_json::Value::Null) => Ok(Vec::new()),
        Some(serde_json::Value::Array(a)) => Ok(a.clone()),
        Some(_) => Err(txn_fail_body(
            txn_id,
            "bad_request",
            &format!(
                "`{key}` must be an array at the TOP LEVEL of the request body, beside \
                 `operations` and never inside it"
            ),
            StatusCode::BAD_REQUEST,
        )),
    }
}

/// Body guard for the KV rider. Shape belongs to the procedure (one rule for all
/// seven clients and the embedded broker); what is checked here is what the
/// procedure cannot check as cheaply, or at all before the money is spent.
fn txn_check_kv(ops: &[serde_json::Value], txn_id: &str) -> Option<Response> {
    if ops.len() > WIRE_KV_MAX_OPS {
        return Some(txn_fail_body(
            txn_id,
            "bad_request",
            &format!(
                "{} kv operations in one transaction, the ceiling on this wire is {}",
                ops.len(),
                WIRE_KV_MAX_OPS
            ),
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut keys = 0usize;
    for (i, o) in ops.iter().enumerate() {
        let obj = match o.as_object() {
            Some(m) => m,
            None => {
                return Some(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!("kv operation at index {i} is not an object"),
                    StatusCode::BAD_REQUEST,
                ))
            }
        };
        match obj.get("op").and_then(|v| v.as_str()) {
            // §5.5: forbidden on this wire, and the boundary is COST, not the
            // kind of operation. `get` and `getMany` are allowed because the
            // caller fixes their cost; a prefix read is unbounded work held
            // inside the transaction that owns the outermost lock space and,
            // downstream, the partition locks. The procedure refuses it too —
            // this copy exists so the refusal costs no connection and names
            // itself instead of arriving as a rolled-back SQLSTATE.
            Some("getPrefix") => {
                return Some(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!(
                        "kv operation at index {i}: getPrefix is not available inside a \
                         transaction; it lives only on POST /api/v1/kv"
                    ),
                    StatusCode::BAD_REQUEST,
                ))
            }
            Some("getMany") => {
                keys += obj
                    .get("keys")
                    .and_then(|k| k.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0)
            }
            _ => keys += 1,
        }
        // The raw-body half of the value ceiling (§9.2). The two halves measure
        // DIFFERENT THINGS on purpose and that surprise belongs in the
        // documentation: here it is the serialized bytes, in the procedure it is
        // the canonical JSONB text, which is normally shorter.
        if let Some(v) = obj.get("value") {
            let n = v.to_string().len();
            if n > wire_kv_max_value_bytes() {
                return Some(txn_fail_body(
                    txn_id,
                    "payload_too_large",
                    &format!(
                        "kv operation at index {i}: value is {n} bytes, the ceiling is {}",
                        wire_kv_max_value_bytes()
                    ),
                    StatusCode::PAYLOAD_TOO_LARGE,
                ));
            }
        }
    }
    if keys > WIRE_KV_MAX_KEYS {
        return Some(txn_fail_body(
            txn_id,
            "bad_request",
            &format!(
                "{keys} kv keys in one transaction, the ceiling on this wire is {}",
                WIRE_KV_MAX_KEYS
            ),
            StatusCode::BAD_REQUEST,
        ));
    }
    None
}

/// Prepare the timer rider: the half of §8.2 point 5 that belongs to the broker.
///
/// Three of these checks exist ONLY here, and each one is load-bearing:
///
///   * UNDERSCORE-PREFIXED FIELDS ARE REFUSED. The procedure takes the message
///     id as `COALESCE((op->>'_messageId')::uuid, ...)` — that is how the broker
///     promises the id at schedule time — and it CANNOT tell the broker's
///     injection from a client's, because both arrive in the same JSON. This
///     boundary can, so this is where a forged message id has to die. The
///     procedure independently refuses the client-spellable spellings
///     (`producerSub`, `messageId`, `tenant`, `deliverAt`, `delaySeconds`, ...).
///   * THE HORIZON. The procedure has no ceiling on `delayMs` at all; without
///     this the 90-day horizon of §9.2 exists nowhere, and with an unbounded
///     horizon the row quota stops being cyclic — a tenant fills it and never
///     frees it.
///   * THE PAYLOAD CEILING, which is `min(1 MiB, plan.max_payload_bytes)` and
///     DERIVED rather than independent: a timer becomes a message, so if its
///     ceiling were not the message's, a timer would be a service entrance
///     around `max_payload_bytes`.
///
/// ENCRYPTION HAPPENS AT SCHEDULE, NOT AT FIRE (§13.4), exactly as on the
/// standalone route: the fire happens inside the sweeper, so a payload not
/// encrypted here would sit in cleartext at rest for days. The consequences are
/// declared rather than discovered — a queue whose encryption is turned ON after
/// a timer was scheduled delivers that frame in CLEARTEXT, and a key rotation
/// between schedule and fire makes the frame undecryptable. Encryption is
/// OUTERMOST, so a `payloadZstd` payload is decrypted first and decompressed
/// second.
async fn txn_prepare_timers(
    st: &Arc<AppState>,
    tenant: &str,
    ops: Vec<serde_json::Value>,
    txn_id: &str,
) -> Result<(Vec<serde_json::Value>, Option<i64>), Response> {
    if ops.len() > WIRE_TIMERS_MAX_OPS {
        return Err(txn_fail_body(
            txn_id,
            "bad_request",
            &format!(
                "{} timer operations in one transaction, the ceiling is {}",
                ops.len(),
                WIRE_TIMERS_MAX_OPS
            ),
            StatusCode::BAD_REQUEST,
        ));
    }
    let mut out: Vec<serde_json::Value> = Vec::with_capacity(ops.len());
    let mut min_delay: Option<i64> = None;

    for (i, raw) in ops.into_iter().enumerate() {
        let mut obj = match raw {
            serde_json::Value::Object(m) => m,
            _ => {
                return Err(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!("timer operation at index {i} is not an object"),
                    StatusCode::BAD_REQUEST,
                ))
            }
        };
        if let Some(k) = obj.keys().find(|k| k.starts_with('_')) {
            return Err(txn_fail_body(
                txn_id,
                "bad_request",
                &format!(
                    "timer operation at index {i}: `{k}` is server-owned; \
                     underscore-prefixed fields are injected by the broker and cannot be supplied"
                ),
                StatusCode::BAD_REQUEST,
            ));
        }
        // And the CLIENT-SPELLABLE server-owned names, at this boundary rather
        // than only in the procedure.
        //
        // The procedure rejects these too and remains the authority — its list is
        // longer and it is the copy that protects the embedded broker and every
        // future caller. But on THIS route its RAISE arrives as SQLSTATE 22023
        // and the wire maps every procedure RAISE to HTTP 200 with a switchable
        // `reason` (see `txn_reason_for`), deliberately: a business verdict must
        // not read as a client error. The consequence, without these lines, is
        // that one class of bug gets two statuses in the SAME bundle —
        // `_messageId` is a 400 from four lines up and `producerSub` a 200 from
        // the procedure — and the integrator who hits the second one concludes
        // the field was accepted. Checking here makes "you supplied a field you
        // do not own" a 400 on both surfaces, and leaves 200 to mean what §8.3
        // says it means: a verdict the caller is expected to see.
        //
        // KEEP IN SYNC with the FOREACH list in 025_log_timers.sql. Drift is
        // safe in one direction only: a name here that the procedure does not
        // have is merely stricter, a name the procedure has and this list does
        // not falls back to the 200 path rather than escaping.
        const TIMER_SERVER_OWNED: [&str; 15] = [
            "producerSub",
            "producer_sub",
            "messageId",
            "message_id",
            "tenant",
            "tenantId",
            "tenant_id",
            "deliverAt",
            "deliver_at",
            "delaySeconds",
            "delay_seconds",
            "attempts",
            "claimToken",
            "claim_token",
            "claimedUntil",
        ];
        if let Some(k) = obj.keys().find(|k| TIMER_SERVER_OWNED.contains(&k.as_str())) {
            return Err(txn_fail_body(
                txn_id,
                "bad_request",
                &format!(
                    "timer operation at index {i}: `{k}` is server-owned and cannot be supplied. \
                     producerSub and the tenant come from the authenticated request; the message \
                     id is minted by the broker; deliverAt is not expressible — the wire carries \
                     only the relative delayMs"
                ),
                StatusCode::BAD_REQUEST,
            ));
        }

        let kind = obj.get("op").and_then(|v| v.as_str()).unwrap_or_default();
        // `cancel`, and anything unknown, travel untouched: the procedure owns
        // the closed taxonomy (§4.1) and a second copy here would give the
        // product two places to disagree about what an operation is.
        if kind != "schedule" && kind != "reschedule" {
            out.push(serde_json::Value::Object(obj));
            continue;
        }

        let queue = match obj.get("queue").and_then(|v| v.as_str()) {
            Some(q) if !q.is_empty() => q.to_string(),
            _ => {
                return Err(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!("timer operation at index {i}: queue is required"),
                    StatusCode::BAD_REQUEST,
                ))
            }
        };

        // §4.2 and §20.6: only RELATIVE durations on this wire, in MILLISECONDS.
        // The declared rule of the product is "durations that can be sub-second
        // are in milliseconds, the ones that cannot are in seconds" — a 250 ms
        // retry backoff is a real and central use of timers, a sub-second TTL is
        // not a real use for anybody. An absolute instant is not expressible:
        // one clock, Postgres's, and no inter-broker skew enters anywhere.
        let delay = match obj.get("delayMs") {
            Some(v) if v.is_number() => v.as_f64().unwrap_or(0.0),
            _ => {
                return Err(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!(
                        "timer operation at index {i}: delayMs (a number of milliseconds) is \
                         required; a delayMs in the past is LEGAL and fires on the first cycle"
                    ),
                    StatusCode::BAD_REQUEST,
                ))
            }
        };
        // 403 and not 400: §9.5 makes the two mean different things — "retry as
        // much as you like, it will not work until you change something" — and a
        // horizon overrun is a plan verdict, not a malformed body.
        if delay > wire_timers_max_horizon_ms() as f64 {
            return Err(txn_fail_body(
                txn_id,
                "timer_horizon_exceeded",
                &format!(
                    "timer operation at index {i}: delayMs {} is beyond the {} ms horizon of \
                     this cell",
                    delay as i64,
                    wire_timers_max_horizon_ms()
                ),
                StatusCode::FORBIDDEN,
            ));
        }
        let d = delay as i64;
        min_delay = Some(min_delay.map_or(d, |m: i64| m.min(d)));

        let payload_b64 = match obj.get("payload").and_then(|v| v.as_str()) {
            Some(p) => p.to_string(),
            None => {
                return Err(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!("timer operation at index {i}: payload (base64) is required"),
                    StatusCode::BAD_REQUEST,
                ))
            }
        };
        let rawb = match base64::engine::general_purpose::STANDARD.decode(payload_b64.as_bytes()) {
            Ok(b) => b,
            Err(e) => {
                return Err(txn_fail_body(
                    txn_id,
                    "bad_request",
                    &format!("timer operation at index {i}: payload is not valid base64: {e}"),
                    StatusCode::BAD_REQUEST,
                ))
            }
        };
        if rawb.len() > wire_timers_max_payload_bytes() {
            return Err(txn_fail_body(
                txn_id,
                "payload_too_large",
                &format!(
                    "timer operation at index {i}: payload is {} bytes, the ceiling is {}",
                    rawb.len(),
                    wire_timers_max_payload_bytes()
                ),
                StatusCode::PAYLOAD_TOO_LARGE,
            ));
        }

        // A client that ALSO claims `encrypted:true` while the broker is about to
        // encrypt is an ambiguity, not a convenience: double encryption or a lie
        // to the consumer, depending on who is right. Refuse instead of guessing.
        let broker_encrypts =
            st.encryption.is_enabled() && st.encryption_enabled_for(&queue, tenant).await;
        let client_claims = obj.get("encrypted").and_then(|v| v.as_bool()) == Some(true);
        if broker_encrypts && client_claims {
            return Err(txn_fail_body(
                txn_id,
                "bad_request",
                &format!(
                    "timer operation at index {i}: queue `{queue}` encrypts at rest, so \
                     `encrypted` is set by the broker and must not be supplied"
                ),
                StatusCode::BAD_REQUEST,
            ));
        }
        if broker_encrypts {
            match st.encryption.encrypt(&rawb) {
                Some(env) => {
                    obj.insert(
                        "payload".to_string(),
                        serde_json::Value::String(
                            base64::engine::general_purpose::STANDARD.encode(&env),
                        ),
                    );
                    obj.insert("encrypted".to_string(), serde_json::Value::Bool(true));
                }
                // Same policy as the push path: warn (sampled — a broken cipher
                // must not flood stderr at ingest rate) and keep plaintext.
                // NEVER fail the transaction for it.
                None => {
                    static ENC_FAIL_TIMER: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = ENC_FAIL_TIMER.tick_now() {
                        tracing::warn!(
                            target: "txn",
                            queue = %queue,
                            suppressed,
                            "encryption failed; timer payload stored as plaintext"
                        );
                    }
                }
            }
        }

        // Minted here and promised back in the result, so a client knows the id
        // of the frame it will see without a second API. The procedure mints its
        // own only as a fallback.
        let mid = uuid_bytes_to_string(&uuidv7_bytes());
        obj.insert("_messageId".to_string(), serde_json::Value::String(mid));
        out.push(serde_json::Value::Object(obj));
    }
    Ok((out, min_delay))
}

/// The inverse scatter: per-array results back into the FLAT space of
/// `results[]`, which is the contract clients read today.
///
/// The element's own `index` is the procedure's ARRAY-LOCAL ordinal and is
/// OVERWRITTEN with the flat one — that overwrite IS the mapping, and it is the
/// reason `failedIndex` and `results[i].index` can never disagree. The local
/// ordinal is kept as `opIndex` so the mapping stays inspectable from the
/// outside instead of having to be reconstructed.
fn txn_scatter_rider(
    results: &mut [serde_json::Value],
    base: usize,
    kind: &'static str,
    arr: &[serde_json::Value],
) {
    for (i, r) in arr.iter().enumerate() {
        let flat = base + i;
        if flat >= results.len() {
            break;
        }
        let mut obj = match r {
            serde_json::Value::Object(m) => m.clone(),
            other => {
                let mut m = serde_json::Map::new();
                m.insert("result".to_string(), other.clone());
                m
            }
        };
        obj.insert("opIndex".to_string(), serde_json::Value::from(i));
        obj.insert("index".to_string(), serde_json::Value::from(flat));
        obj.insert("type".to_string(), serde_json::Value::String(kind.to_string()));
        results[flat] = serde_json::Value::Object(obj);
    }
}

/// Read one rider's result array out of the procedure's answer, with the count
/// guard of §8.2 point 3. `Err` carries the flat-space explanation.
///
/// The `None` arm is the one that matters operationally: a new broker against a
/// database whose wire procedure predates the graft returns no `kv` key at all,
/// which without this guard reads as a completely successful transaction whose
/// gate never ran.
fn txn_rider_results<'a>(
    v: &'a serde_json::Value,
    key: &'static str,
    sent: usize,
) -> Result<Option<&'a Vec<serde_json::Value>>, String> {
    if sent == 0 {
        return Ok(None);
    }
    match v.get(key).and_then(|x| x.as_array()) {
        Some(a) if a.len() == sent => Ok(Some(a)),
        Some(a) => Err(format!(
            "QTXN the transaction returned {} `{key}` results for {sent} operations; \
             the flat result space cannot be aligned and the bundle is reported failed",
            a.len()
        )),
        None => Err(format!(
            "QTXN the transaction returned no `{key}` results for {sent} operations: this \
             database's transaction procedure does not carry the {key} array, so those \
             operations were IGNORED while the rest of the bundle committed. Apply the \
             schema (QUEEN_APPLY_SCHEMA=1) before enabling this feature"
        )),
    }
}

/// `putIfAbsent` is not a label of its own: it desugars to `put` with `expect:0`
/// inside the procedure, so it is one code path and therefore one series.
///
/// DUPLICATE, declared: this and the recorder below mirror `kv_op_label` and
/// `record_results` in `handlers/kv.rs`, which are private there. In-wire KV work
/// has to land in the SAME series as the standalone surface — §14.2 gives
/// `queen_kv_ops_total{op,result}` no `surface` label, and the in-wire gate is
/// the product's number-one use case, so leaving it uncounted would make the
/// series lie exactly where it is read. Two lines of `pub(super)` in `kv.rs`
/// delete this copy.
fn txn_kv_op_label(op: Option<&str>) -> Option<crate::metrics::KvOp> {
    use crate::metrics::KvOp;
    match op? {
        "get" => Some(KvOp::Get),
        "getMany" => Some(KvOp::GetMany),
        "getPrefix" => Some(KvOp::GetPrefix),
        "put" | "putIfAbsent" => Some(KvOp::Put),
        "delete" => Some(KvOp::Delete),
        "incr" => Some(KvOp::Incr),
        _ => None,
    }
}

/// Attribute each returned KV element to its own op and outcome. The element's
/// own `op` is authoritative rather than the input's: the procedure is where
/// `putIfAbsent` becomes `put`, and reading the answer instead of the question
/// keeps the two from drifting.
///
/// `ms` is the whole bundle's duration divided by the number of KV ops, and that
/// is the honest number rather than an approximation: a bundle is ONE round trip
/// and ONE commit, so this is what the KV operation actually cost its caller.
fn txn_record_kv(st: &AppState, results: &[serde_json::Value], ms: f64, bytes_in: u64) {
    use crate::metrics::KvResult;
    let mut out: u64 = 0;
    for r in results {
        let Some(op) = txn_kv_op_label(r.get("op").and_then(|v| v.as_str())) else {
            continue;
        };
        // Writes report `applied`; a read has no predicate to lose, so a read
        // that reached the database is `applied` whether or not it found a row —
        // `found:false` is a datum, not a rejection.
        let outcome = match r.get("applied").and_then(|v| v.as_bool()) {
            Some(true) => KvResult::Applied,
            Some(false) => KvResult::Rejected,
            None => KvResult::Applied,
        };
        st.metrics.kvt.kv_op(op, outcome, ms);
        if let Some(v) = r.get("value") {
            out += v.to_string().len() as u64;
        }
        if let Some(rows) = r.get("rows").and_then(|v| v.as_array()) {
            for row in rows {
                if let Some(v) = row.get("value") {
                    out += v.to_string().len() as u64;
                }
            }
        }
    }
    st.metrics.kvt.kv_bytes(bytes_in, out);
}

fn txn_record_kv_all(
    st: &AppState,
    ops: &[serde_json::Value],
    result: crate::metrics::KvResult,
    ms: f64,
) {
    for o in ops {
        if let Some(k) = txn_kv_op_label(o.get("op").and_then(|v| v.as_str())) {
            st.metrics.kvt.kv_op(k, result, ms);
        }
    }
}

// ---------------------------------------------------------------- failure body
//
// §8.3: the failure body has to grow, and it is shared by EVERY transaction
// failure, so this is a wire change across all seven clients and their retry
// policies — not a KV detail.
//
// `reason` is a CODE from a closed taxonomy and is the only field a client may
// branch on. String matching on the message is forbidden everywhere in this
// codebase, and until now `error` was the only thing a failed transaction
// carried, which left every client with no choice but to match on it:
//
//   bad_request           the body never reached the database
//   duplicate             a duplicate push rolled the bundle back (QDUP)
//   ack_rejected          an ack was invalid, expired or unknown (QTXN)
//   kv_precondition       a `required` KV gate lost — see below
//   timer_horizon_exceeded a schedule beyond this cell's horizon
//   payload_too_large     a value or a timer payload over its ceiling
//   misaligned            an internal index-space failure; never the caller's
//   db_error              everything else
//
// THE STATUS RULE ON THIS ROUTE, AND WHERE IT DIVERGES FROM §9.5 ON PURPOSE.
// §8.1's rule is that the status describes the outcome of the CALL. On this
// route that has always meant: a body that never reached the database answers
// 4xx (the pre-existing `any_unknown` 400), and every verdict the database
// itself returned answers **200** with the v1 envelope — the comment on the
// `Err` arm has said so since the C++ port, and every client's transaction path
// parses that envelope on 200. So a shape rejection caught HERE is a 400/403/413
// out of §9.5's table, and the SAME rejection caught inside a procedure is a 200
// carrying `reason:"bad_request"`. The boundary is "did we touch the database",
// which is the boundary this handler already had; promoting the second case to
// 4xx would change the status of a rolled-back transaction for seven clients,
// which is far beyond what §8.3 asks for. `reason` is what a client switches on,
// and it is the same string on both sides.

fn txn_fail_json(txn_id: &str, reason: &str, err: &str) -> serde_json::Value {
    serde_json::json!({
        "transactionId": txn_id,
        "success": false,
        "reason": reason,
        "error": err,
        "results": [],
    })
}

fn txn_fail_body(txn_id: &str, reason: &str, err: &str, status: StatusCode) -> Response {
    json(status, txn_fail_json(txn_id, reason, err).to_string())
}

/// The wire's refund, as a drop guard (§9.3).
///
/// `handle_transaction` has more than a dozen early returns between the quota
/// charge and the commit — ack resolution, the bogus-ack pre-check, encryption,
/// the pool, the SP itself — and a refund written at each one is a refund that
/// will be forgotten at the next one added. What must not be forgotten is the
/// consequence: a database outage charges every bundle and commits none, the
/// deltas inflate with nothing to correct them (the refresh is failing too), and
/// on recovery the tenant meets a 403 that says "you are full" for a fault that
/// was the cell's. §12 forbids exactly that conversion.
///
/// So the charge is refunded UNLESS the bundle committed, and `commit()` is
/// called on exactly one line — the success arm. A panic refunds too, which is
/// the right way round.
struct TxnRefund<'a> {
    st: &'a AppState,
    tenant: &'a str,
    rows: i64,
    bytes: i64,
    timers: i64,
}

impl TxnRefund<'_> {
    /// The bundle committed: the charge is real and stands until the next
    /// measurement supersedes it.
    fn commit(mut self) {
        self.rows = 0;
        self.bytes = 0;
        self.timers = 0;
    }
}

impl Drop for TxnRefund<'_> {
    fn drop(&mut self) {
        if self.rows != 0 || self.bytes != 0 || self.timers != 0 {
            self.st
                .quota
                .refund(self.tenant, self.rows, self.bytes, self.timers);
        }
    }
}

/// The kv/timers ladder on the transaction wire (§9.5, §12.1), rendered into the
/// wire's OWN failure envelope rather than the KV route's.
///
/// A client that sent a bundle gets `success:false` with a `reason` it already
/// knows how to read, and the `reason` is the ladder's code — so the same
/// condition is named identically whether it was met on `/api/v1/kv` or here.
/// The bundle is refused WHOLE: partial application is what the transaction
/// exists to prevent, and "commit the messages, drop the kv gate" would commit
/// exactly the transaction the gate was there to stop.
fn txn_gate(
    st: &AppState,
    tenant: &str,
    surface: crate::switches::Surface,
    add_rows: i64,
    add_bytes: i64,
    txn_id: &str,
) -> Option<Response> {
    use crate::switches::{decide, Origin};
    let a = decide(&st.switches, &st.quota, tenant, surface, add_rows, add_bytes);
    let h = a.http(Origin::Wire, surface)?;
    st.metrics.kvt.kv_read_rejected(match h.status {
        429 => crate::metrics::KvReject::RateLimited,
        403 => crate::metrics::KvReject::Quota,
        503 => crate::metrics::KvReject::Pool,
        _ => crate::metrics::KvReject::Disabled,
    });
    Some(txn_fail_body(
        txn_id,
        h.code,
        "the bundle was not committed: its kv/timers riders were refused, and a transaction \
         that dropped them would commit the very operations the riders exist to gate",
        StatusCode::from_u16(h.status).unwrap_or(StatusCode::FORBIDDEN),
    ))
}

/// A lost `required` KV gate, translated at the boundary (§8.3).
///
/// HTTP **200**. The transaction really did abort in SQL and the RAISE really
/// was necessary, but a lost precondition is the EXPECTED outcome of every
/// legitimate redelivery — it is the idempotency marker doing its job — and it
/// must pollute neither the error metrics nor the retry policies of seven
/// clients. Everything the caller needs is read from the DETAIL, which is JSON,
/// and never from the message: the message is deliberately opaque because
/// handlers echo DB text and namespace/key names would land in shared logs and
/// error aggregators (§13.5).
///
/// `failedIndex` is translated from the procedure's array-local ordinal into the
/// FLAT space here, and nowhere else.
///
/// `ok:false` is emitted ALONGSIDE `success:false` so that a client handling
/// `reason:"kv_precondition"` reads the same shape whether the verdict arrived
/// from `/api/v1/transaction` or from `/api/v1/kv`, which answers with `ok`.
fn txn_precondition_json(
    txn_id: &str,
    err: &str,
    detail: Option<&str>,
    kv_base: usize,
) -> serde_json::Value {
    // The DETAIL is capped at 4 KiB in the procedure, so a pathological value
    // can truncate it into invalid JSON. Degrade to the bare verdict rather than
    // turning a legitimate lost race into a 500.
    let parsed: Option<serde_json::Value> = detail.and_then(|d| serde_json::from_str(d).ok());
    let mut out = txn_fail_json(txn_id, "kv_precondition", err);
    out["ok"] = serde_json::Value::Bool(false);
    if let Some(v) = parsed {
        let flat = v
            .get("index")
            .and_then(|x| x.as_u64())
            .map(|n| serde_json::Value::from(kv_base + n as usize))
            .unwrap_or(serde_json::Value::Null);
        out["failedIndex"] = flat;
        out["kvReason"] = v.get("reason").cloned().unwrap_or(serde_json::Value::Null);
        out["version"] = v.get("version").cloned().unwrap_or(serde_json::Value::Null);
        out["value"] = v.get("value").cloned().unwrap_or(serde_json::Value::Null);
    }
    out
}

fn txn_fail_precondition(
    txn_id: &str,
    err: &str,
    detail: Option<&str>,
    kv_base: usize,
) -> Response {
    json(
        StatusCode::OK,
        txn_precondition_json(txn_id, err, detail, kv_base).to_string(),
    )
}

/// Map a database error onto the failure taxonomy above, and say whether it is a
/// real database failure. A verdict (a rolled-back business rule) must not
/// inflate the DB error series — the same discrimination the QDUP/QTXN arm makes
/// today, generalized so the KV and timer rejections inherit it.
fn txn_reason_for(sqlstate: Option<&str>, msg: &str) -> (&'static str, bool) {
    match sqlstate {
        Some("23514") => ("kv_precondition", false),
        Some("22001") => ("payload_too_large", false),
        Some("22023") => ("bad_request", false),
        _ => {
            if msg.starts_with("QDUP") {
                ("duplicate", false)
            } else if msg.starts_with("QTXN") || msg.starts_with("QTIMER") {
                ("ack_rejected", false)
            } else {
                ("db_error", true)
            }
        }
    }
}

fn txn_db_reason(e: &tokio_postgres::Error, msg: &str) -> (&'static str, bool) {
    let code = e.as_db_error().map(|d| d.code().code().to_string());
    txn_reason_for(code.as_deref(), msg)
}

/// §2.5's free mitigation, and the ONE case that is routed away from the wire.
///
/// The KV row lock taken at step 0 of the wire is held for the whole bundle —
/// provisioning, the ascending partition pre-lock, every push with its blob,
/// every ack, all the way to the fsync. That is accepted risk §18.2 and it buys
/// atomicity. A bundle that contains ONLY KV operations buys nothing with it:
/// there is no push and no ack to be atomic with, so it goes straight to the KV
/// procedure, which is a short transaction.
///
/// It is routed BEFORE `Lane::Push` admission is taken, and that is the point of
/// doing it at all: §8.4 point 2 keeps standalone KV work out of the arbiter's
/// lanes entirely, because thirty tenants each inside their own write rate would
/// otherwise burn thousands of `Lane::Push` slots per second on a stack whose
/// measured commit-bound ceiling is around 480 msg/s — nobody violating anything
/// and the message path starving.
///
/// `in_wire = true` is deliberate and is NOT a leftover of the routing: this is
/// still the transaction wire's surface, so it keeps the wire's tighter caps and
/// its ban on `getPrefix`. The flag is a parameter of the one procedure rather
/// than a second procedure, so the two surfaces cannot drift.
///
/// A timers-only bundle is deliberately NOT routed here. §2.5 names KV only, the
/// timer procedure's lock discipline is written for the wire's sequence, and a
/// second bypass would be a second place for that discipline to be re-derived.
async fn txn_kv_only(
    st: &Arc<AppState>,
    tenant: &str,
    txn_id: &str,
    ops: Vec<serde_json::Value>,
) -> Response {
    let n = ops.len();
    let bytes_in: u64 = ops
        .iter()
        .filter_map(|o| o.get("value"))
        .map(|v| v.to_string().len() as u64)
        .sum();
    let ops_json = serde_json::Value::Array(ops.clone()).to_string();

    // SEAM (§8.4 point 1): `st.kv_pool` once `AppState` carries the dedicated
    // pool. Until then this is correct but NOT yet a bulkhead — the difference
    // between "the KV endpoint is slow" and "the KV endpoint made the message
    // path slow". The 500 shape is the wire's existing one, deliberately, so
    // this route is indistinguishable from the wire on infrastructure failure.
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            return json(StatusCode::INTERNAL_SERVER_ERROR, "{\"error\":\"pool\"}".to_string());
        }
    };
    // Captured BEFORE the query: on a broker-side timeout the still-running
    // statement is cancelled server-side and the connection quarantined rather
    // than abandoned (§8.4 point 4). An abandoned statement keeps row locks in
    // the outermost lock space, and those are what another bundle waits behind.
    let cancel = client.cancel_token();
    let t0 = std::time::Instant::now();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::kv_apply(&client, &ops_json, tenant, true),
    )
    .await;
    let ms = t0.elapsed().as_secs_f64() * 1000.0 / (n.max(1) as f64);

    match super::kv::resolve_db(res, client, cancel, "kv_apply", &st.metrics) {
        Ok(txt) => match serde_json::from_str::<serde_json::Value>(&txt) {
            Ok(serde_json::Value::Array(a)) if a.len() == n => {
                txn_record_kv(st, &a, ms, bytes_in);
                let mut results: Vec<serde_json::Value> =
                    vec![serde_json::Value::Null; n];
                txn_scatter_rider(&mut results, 0, "kv", &a);
                let out = serde_json::json!({
                    "transactionId": txn_id,
                    "success": true,
                    "results": results,
                });
                json(StatusCode::OK, out.to_string())
            }
            _ => {
                st.metrics.record_db_error();
                txn_record_kv_all(st, &ops, crate::metrics::KvResult::Error, ms);
                txn_fail_body(
                    txn_id,
                    "misaligned",
                    "QTXN the kv procedure did not return one result per operation",
                    StatusCode::OK,
                )
            }
        },
        Err(Some(e)) => {
            let msg = e
                .as_db_error()
                .map(|d| d.message().to_string())
                .unwrap_or_else(|| e.to_string());
            let (reason, is_db_failure) = txn_db_reason(&e, &msg);
            if is_db_failure {
                st.metrics.record_db_error();
            }
            let outcome = if reason == "db_error" {
                crate::metrics::KvResult::Error
            } else {
                crate::metrics::KvResult::Rejected
            };
            txn_record_kv_all(st, &ops, outcome, ms);
            if reason == "kv_precondition" {
                let detail = e.as_db_error().and_then(|d| d.detail()).map(str::to_string);
                return txn_fail_precondition(txn_id, &msg, detail.as_deref(), 0);
            }
            // HTTP 200 like every other transaction verdict: this route answers
            // on the transaction contract, not on the KV route's, and a client of
            // /api/v1/transaction must not start seeing 4xx because the broker
            // took a shortcut behind its back.
            txn_fail_body(txn_id, reason, &msg, StatusCode::OK)
        }
        Err(None) => {
            txn_record_kv_all(st, &ops, crate::metrics::KvResult::Error, ms);
            txn_fail_body(txn_id, "db_error", "QTXN kv apply timed out", StatusCode::OK)
        }
    }
}

pub async fn handle_transaction(
    State(st): State<Arc<AppState>>,
    Extension(authed): Extension<crate::auth::AuthedSub>,
    Extension(tenant): Extension<crate::tenant::Tenant>,
    body: Bytes,
) -> Response {
    let root: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    let txn_id = uuid_bytes_to_string(&uuidv7_bytes());
    // Authenticated producer identity (JWT sub), stamped onto every pushed frame
    // when auth is enabled. None when auth is disabled or the token had no sub.
    let producer_sub = authed.0.filter(|s| !s.is_empty());

    // ---------------------------------------------------------- riders (§8.2)
    // Read BEFORE `operations`, because a KV-only bundle legitimately carries no
    // `operations` at all and must not be refused by a guard written when push
    // and ack were the only things a transaction could contain.
    let kv_in = match txn_rider_array(&root, "kv", &txn_id) {
        Ok(v) => v,
        Err(r) => return r,
    };
    let timers_in = match txn_rider_array(&root, "timers", &txn_id) {
        Ok(v) => v,
        Err(r) => return r,
    };
    // A `kv` or `timers` rider no longer has a boot flag to be refused by: both
    // surfaces exist on every cell, so a bundle carrying one cannot be rejected for
    // arriving at the wrong broker. What used to be two 400s here has become one
    // fewer thing a client has to handle — see the header of `switches.rs`.
    //
    // THE REMAINING RUNGS (§9.5, §12.1), on `Origin::Wire`.
    //
    // The wire differs from the routes in exactly one way, and it is deliberate:
    // the operator's runtime kill switch answers PERMANENTLY here (403) instead
    // of 503. A bundle carries MESSAGES, and a client that retries it in a loop
    // against a cell an operator has deliberately paused is a retry storm on the
    // hottest path of the product. `switches::Answer::http` owns that difference
    // so the two surfaces cannot drift.
    //
    // The whole bundle is refused, and it must be: partial application is exactly
    // what the transaction exists to prevent.
    let (kv_rows, kv_bytes) = super::kv::write_footprint(&kv_in);
    let kv_charge = if kv_rows > 0 || kv_bytes > 0 {
        Some((kv_rows, kv_bytes))
    } else {
        None
    };
    if !kv_in.is_empty() {
        let surface = if kv_charge.is_some() {
            crate::switches::Surface::KvWrite
        } else {
            crate::switches::Surface::KvRead
        };
        if let Some(r) = txn_gate(&st, tenant.as_str(), surface, kv_rows, kv_bytes, &txn_id) {
            return r;
        }
    }
    // Timer ops on the wire are schedules and cancels in one array, as on the
    // POST route. A bundle whose timer array is only cancels is charged nothing
    // and refused by nothing below the env rung (§9.6).
    let timer_schedules = timers_in
        .iter()
        .filter(|o| {
            matches!(
                o.get("op").and_then(|v| v.as_str()),
                Some("schedule") | Some("reschedule")
            )
        })
        .count() as i64;
    if timer_schedules > 0 {
        if let Some(r) = txn_gate(
            &st,
            tenant.as_str(),
            crate::switches::Surface::TimerSchedule,
            timer_schedules,
            0,
            &txn_id,
        ) {
            return r;
        }
    }
    // Everything charged above is given back unless the bundle commits. See
    // `TxnRefund`: the number of early returns between here and the commit is
    // exactly why this is a guard and not a line at each one.
    let refund = TxnRefund {
        st: &st,
        tenant: tenant.as_str(),
        rows: kv_charge.map(|(r, _)| r).unwrap_or(0),
        bytes: kv_charge.map(|(_, b)| b).unwrap_or(0),
        timers: timer_schedules,
    };
    if let Some(r) = txn_check_kv(&kv_in, &txn_id) {
        return r;
    }
    let (timers_ops, timers_min_delay) =
        match txn_prepare_timers(&st, tenant.as_str(), timers_in, &txn_id).await {
            Ok(v) => v,
            Err(r) => return r,
        };

    let empty_ops: Vec<serde_json::Value> = Vec::new();
    let operations = match root.get("operations") {
        Some(serde_json::Value::Array(o)) => o,
        // Absent or null is only legal when the bundle is riders-only. The
        // message stays the one clients have been reading, with the new shape
        // named beside it.
        None | Some(serde_json::Value::Null) if !kv_in.is_empty() || !timers_ops.is_empty() => {
            &empty_ops
        }
        _ => {
            return txn_fail_body(
                &txn_id,
                "bad_request",
                "transaction requires an operations array (or a top-level kv/timers array)",
                StatusCode::BAD_REQUEST,
            )
        }
    };
    st.metrics
        .transactions
        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // §2.5's free mitigation: a bundle of ONLY kv operations never reaches the
    // wire, and never takes a `Lane::Push` slot. Everything above this line has
    // already validated it.
    if operations.is_empty() && timers_ops.is_empty() && !kv_in.is_empty() {
        return txn_kv_only(&st, tenant.as_str(), &txn_id, kv_in).await;
    }

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
    let mut any_unknown: Option<String> = None;

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
                if any_unknown.is_none() {
                    any_unknown = Some(ty.to_string());
                }
                flat += 1;
            }
        }
    }

    if let Some(ty) = any_unknown {
        // §8.2's desirable side effect: a new client against an old broker gets a
        // CLEAN, NAMED 400 on an operation it does not understand, which is the
        // best failure available and belongs in reference/compatibility.mdx.
        //
        // The two kv/timer spellings get their own sentence because the mistake
        // they represent is the exact one §10.4 forbids: putting these ops inside
        // `operations` is what Go's encoding/json would silently drop, so the one
        // person most likely to try it is the one who must be told where they go.
        let err = match ty.as_str() {
            "kv" | "timer" | "timers" => "kv and timer operations are TOP-LEVEL arrays of the \
                 request (\"kv\":[...], \"timers\":[...]), never elements of `operations`"
                .to_string(),
            "" => "every transaction operation needs a `type` of push or ack".to_string(),
            other => format!(
                "segments transaction supports only push and ack operations, got `{other}`"
            ),
        };
        return txn_fail_body(&txn_id, "bad_request", &err, StatusCode::BAD_REQUEST);
    }

    // The flat layout, fixed here and nowhere else: operations keep the indices
    // they have today, the riders append. See the demux header.
    let riders = TxnRiders {
        kv_base: flat,
        timers_base: flat + kv_in.len(),
        kv: kv_in,
        timers: timers_ops,
        min_delay_ms: timers_min_delay,
    };
    flat += riders.kv.len() + riders.timers.len();

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
    // Push-lane admission BEFORE the pool checkout (ordering contract,
    // admission.rs): the wire transaction commits push WAL.
    let _slot = crate::admission::lane_slot(crate::admission::Lane::Push).await;
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

    // Bogus-ack atomicity is enforced INSIDE the wire SP now (2026-08-20;
    // 005_log_ack, the unresolvedHashes check after the by_hash leg). The
    // per-hash probe that used to sit here re-derived, over the partition's
    // WHOLE log_txns history and once per acked hash, a fact the ack SP
    // already computes in one bounded pass — measured at 68% of all DB time
    // under Gate relay load (every relay is ack+push on this route). An acked
    // txn that resolves nowhere makes the SP RAISE 'QTXN ...', which rolls the
    // whole wire back atomically (pushes included) and maps to the same
    // 'ack_rejected' failure body via txn_reason_for. Below-cursor duplicates
    // (replayed relays) keep resolving as duplicates, exactly as before.

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
    //
    // THE TWO RIDER ARRAYS ARE ADDED ONLY WHEN NON-EMPTY, and that is a
    // requirement rather than tidiness (§6.3): a bundle that carries neither must
    // produce a payload BYTE-IDENTICAL to today's, or the perf gate of §15 — same
    // payload, same plan, CPU per message within 1% — has nothing to compare
    // against, and `pg_stat_statements` would show a second prepared text for the
    // one statement that runs on every transaction in the product.
    //
    // `_producerSub` travels the same way `_tenant` does, for the same reason:
    // the procedure's signature is `(p JSONB)` and must not change, and an
    // underscore-prefixed key is one a client cannot spell — which is what makes
    // `producer_sub` the only non-repudiable field of a frame. A timer becomes a
    // message, so without this a scheduled frame would carry no provenance at all.
    let mut payload_obj = serde_json::json!({
        "pushes": pushes_json,
        "acks": acks_json,
        "_tenant": tenant.as_str(),
    });
    if !riders.kv.is_empty() {
        payload_obj["kv"] = serde_json::Value::Array(riders.kv.clone());
    }
    if !riders.timers.is_empty() {
        payload_obj["timers"] = serde_json::Value::Array(riders.timers.clone());
        if let Some(p) = producer_sub.as_deref() {
            payload_obj["_producerSub"] = serde_json::Value::String(p.to_string());
        }
    }
    let payload = payload_obj.to_string();
    let kv_bytes_in: u64 = riders
        .kv
        .iter()
        .filter_map(|o| o.get("value"))
        .map(|v| v.to_string().len() as u64)
        .sum();
    let t_txn = std::time::Instant::now();

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

                // ------------------------------------------- the inverse scatter
                // Per-array results back into the flat space (§8.2 point 2). The
                // count guard runs FIRST and is not optional: without it, a
                // database whose wire procedure has no kv leg answers with no
                // `kv` key, every rider ordinal stays unfilled, and the caller
                // reads `success:true` for a bundle whose gate never ran.
                for (key, sent, base, kind) in [
                    ("kv", riders.kv.len(), riders.kv_base, "kv"),
                    ("timers", riders.timers.len(), riders.timers_base, "timer"),
                ] {
                    match txn_rider_results(&v, key, sent) {
                        Ok(None) => {}
                        Ok(Some(arr)) => {
                            if key == "kv" {
                                let ms = t_txn.elapsed().as_secs_f64() * 1000.0
                                    / (sent.max(1) as f64);
                                txn_record_kv(&st, arr, ms, kv_bytes_in);
                            }
                            txn_scatter_rider(&mut results, base, kind, arr);
                        }
                        Err(msg) => {
                            st.metrics.record_db_error();
                            static RIDER_GAP: crate::obs::Sampler =
                                crate::obs::Sampler::new(60_000);
                            if let Some(suppressed) = RIDER_GAP.tick_now() {
                                tracing::error!(
                                    target: "txn",
                                    rider = key,
                                    suppressed,
                                    "transaction rider results missing or short; is the schema \
                                     applied on this database?"
                                );
                            }
                            return txn_fail_body(&txn_id, "misaligned", &msg, StatusCode::OK);
                        }
                    }
                }
                // §8.2 point 3, the guard rewritten between the two index spaces:
                // EVERY flat ordinal has exactly one result. The procedures make
                // the same assertion inside their own arrays and RAISE on a
                // violation; here it can only be an internal fault, so it is
                // reported as such and never blamed on the caller. A null left in
                // position is precisely the silent misalignment class that
                // 003_log_push.sql:372-375 fails loudly on.
                if let Some(gap) = results.iter().position(|r| r.is_null()) {
                    tracing::error!(
                        target: "txn",
                        gap,
                        total = results.len(),
                        "transaction result misaligned: a flat ordinal has no result"
                    );
                    return txn_fail_body(
                        &txn_id,
                        "misaligned",
                        &format!(
                            "QTXN transaction produced no result for flat operation {gap} of \
                             {}; the bundle committed but its result array cannot be trusted",
                            results.len()
                        ),
                        StatusCode::OK,
                    );
                }

                // SEAM (§7.4): the local, in-process sweeper wake goes HERE,
                // AFTER the commit and never before — a wake for a transaction
                // that then rolls back costs a wasted cycle and, worse, teaches
                // the loop that work exists which does not. It is one
                // `sweeper::hint_in_ms(ms)` call, and it cannot be written yet:
                // `sweeper` is declared in `main.rs` only and NOT in the twin
                // list in `lib.rs` (§7.1 names both), so naming it from a file
                // compiled into both targets breaks the library build. Nothing
                // breaks without it: QUEEN_SWEEPER_MAX_SLEEP_MS (1 s) is the
                // recovery window and `deliverAt` is "no earlier than".
                let _wake_in_ms = riders.min_delay_ms;

                // The bundle committed, so what the ladder charged actually
                // happened and the local delta keeps it (§9.3).
                refund.commit();
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
                txn_fail_body(&txn_id, "db_error", &err, StatusCode::OK)
            }
        }
        // The SP RAISEs on rollback (duplicate push / rejected ack / a lost KV
        // gate): surface the DB message (e.g. "QDUP ...", "QTXN ...") as a
        // v1-shaped failure, HTTP 200 (matches the C++ broker).
        Err(e) => {
            let msg = e
                .as_db_error()
                .map(|d| d.message().to_string())
                .unwrap_or_else(|| e.to_string());
            // A QDUP/QTXN RAISE is a business rollback, not a database failure —
            // counting it would inflate "DB errors" on every duplicate push.
            // Anything else (connection dropped, timeout, syntax) is a real one.
            // The KV and timer rejections inherit that same discrimination
            // through `txn_db_reason`, and the most frequent one of all — a lost
            // idempotency gate — must never read as an error anywhere.
            let (reason, is_db_failure) = txn_db_reason(&e, &msg);
            if is_db_failure {
                st.metrics.record_db_error();
            }
            if !riders.kv.is_empty() {
                let ms = t_txn.elapsed().as_secs_f64() * 1000.0 / (riders.kv.len() as f64);
                let outcome = if reason == "db_error" {
                    crate::metrics::KvResult::Error
                } else {
                    crate::metrics::KvResult::Rejected
                };
                txn_record_kv_all(&st, &riders.kv, outcome, ms);
            }
            if reason == "kv_precondition" {
                let detail = e.as_db_error().and_then(|d| d.detail()).map(str::to_string);
                return txn_fail_precondition(
                    &txn_id,
                    &msg,
                    detail.as_deref(),
                    riders.kv_base,
                );
            }
            txn_fail_body(&txn_id, reason, &msg, StatusCode::OK)
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

/// Conformance between this module's wire handling and the `queen-protocol`
/// crate, which is what the Rust client serializes against.
///
/// The broker cannot simply *use* the crate's types: `PushItem<'a>` borrows out
/// of the request body and the responses are rendered by hand into a pre-sized
/// `String`, both deliberate throughput choices. So the two representations are
/// locked together here instead — every test below sends a value through one
/// side and asserts the other side agrees. A renamed field, a changed default
/// or a dropped key fails here rather than in somebody's consumer loop.
#[cfg(test)]
mod protocol_conformance {
    use super::*;
    use queen_protocol as qp;

    // ------------------------------------------------------------- push in

    #[test]
    fn client_push_body_parses_into_the_brokers_zero_copy_struct() {
        let req = qp::PushRequest::new(vec![
            qp::PushItem::new("orders", serde_json::json!({"n": 1})),
            qp::PushItem::new("orders", serde_json::json!(null))
                .partition("eu")
                .transaction_id("txn-7"),
        ]);
        let body = serde_json::to_vec(&req).unwrap();

        let parsed: PushBody = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.items.len(), 2);

        assert_eq!(parsed.items[0].queue.as_str(), "orders");
        assert_eq!(parsed.items[0].partition.as_deref(), None);
        assert_eq!(parsed.items[0].transaction_id.as_deref(), None);
        assert_eq!(parsed.items[0].payload.get(), r#"{"n":1}"#);

        assert_eq!(parsed.items[1].partition.as_deref(), Some("eu"));
        assert_eq!(parsed.items[1].transaction_id.as_deref(), Some("txn-7"));
        assert_eq!(parsed.items[1].payload.get(), "null");
    }

    /// Locks the asymmetry documented on [`qp::PushItem`]: the push path has
    /// nowhere to store a trace id, so the client type does not offer one. If
    /// `PushItem<'a>` ever grows the field, this test fails and the client type
    /// gets it too — deliberately, rather than by accident.
    #[test]
    fn push_path_still_has_no_trace_id_field() {
        // A body carrying traceId parses fine (serde ignores unknown keys) and
        // nothing anywhere captures the value.
        let body = br#"{"items":[{"queue":"q","payload":1,"traceId":"6f1a3d0e-0000-7000-8000-000000000000"}]}"#;
        let parsed: PushBody = serde_json::from_slice(body).unwrap();
        assert_eq!(parsed.items.len(), 1);

        // The crate's own serialization never emits the key in the first place.
        let ours = serde_json::to_string(&qp::PushItem::new("q", serde_json::json!(1))).unwrap();
        assert!(!ours.contains("traceId"), "{ours}");

        // Whereas the transaction path does carry it — the contrast is the point.
        let txn = serde_json::to_string(
            &qp::TxnPushItem::new("q", serde_json::json!(1))
                .trace_id("6f1a3d0e-0000-7000-8000-000000000000"),
        )
        .unwrap();
        assert!(txn.contains("traceId"), "{txn}");
    }

    // ------------------------------------------------------------ push out

    #[test]
    fn rendered_push_results_parse_into_the_client_type() {
        let results = vec![
            ItemResult {
                message_id: "0190aaaa-0000-7000-8000-000000000001".into(),
                txn: "txn-1".into(),
                queue: "orders".into(),
                status: "queued",
                dup_of: None,
            },
            ItemResult {
                message_id: "0190aaaa-0000-7000-8000-000000000002".into(),
                // Exercise the escaper: a quote and a backslash in the txn must
                // survive the hand-rolled renderer intact.
                txn: r#"weird"txn\2"#.into(),
                queue: "orders".into(),
                status: "duplicate",
                dup_of: None,
            },
        ];

        let rendered = render_push_results(&results);
        let parsed: Vec<qp::PushResult> = serde_json::from_str(&rendered).unwrap();

        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].index, 0);
        assert_eq!(parsed[0].message_id, results[0].message_id);
        assert_eq!(parsed[0].transaction_id, "txn-1");
        assert_eq!(parsed[0].queue_name, "orders");
        assert_eq!(parsed[0].status, qp::PushStatus::Queued);

        assert_eq!(parsed[1].index, 1);
        assert_eq!(parsed[1].transaction_id, r#"weird"txn\2"#);
        assert_eq!(parsed[1].status, qp::PushStatus::Duplicate);
    }

    /// Every status the broker can stamp must be a variant the client can
    /// parse. An unmapped one would fail deserialization on a real response.
    #[test]
    fn every_status_the_broker_stamps_is_known_to_the_client() {
        for status in ["queued", "duplicate", "error", "buffered", "failed"] {
            let rendered = render_push_results(&[ItemResult {
                message_id: "m".into(),
                txn: "t".into(),
                queue: "q".into(),
                status,
                dup_of: None,
            }]);
            let parsed: Vec<qp::PushResult> = serde_json::from_str(&rendered)
                .unwrap_or_else(|e| panic!("client cannot parse status {status:?}: {e}"));
            // ...and round-trips back to the same spelling, so the client never
            // reports a status the broker did not stamp.
            assert_eq!(
                serde_json::to_value(parsed[0].status).unwrap(),
                serde_json::Value::String(status.to_string())
            );
        }
    }

    // -------------------------------------------------------------- ack in

    #[test]
    fn client_ack_batch_parses_into_the_brokers_struct() {
        let req = qp::AckBatchRequest {
            acknowledgments: vec![
                qp::AckBatchItem {
                    transaction_id: "t1".into(),
                    partition_id: "p1".into(),
                    status: qp::AckStatus::Completed,
                    lease_id: Some("L1".into()),
                    error: None,
                },
                qp::AckBatchItem {
                    transaction_id: "t2".into(),
                    partition_id: "p1".into(),
                    status: qp::AckStatus::Dlq,
                    lease_id: Some("L1".into()),
                    error: Some("poison".into()),
                },
            ],
            consumer_group: Some("workers".into()),
        };
        let body = serde_json::to_vec(&req).unwrap();

        let parsed: AckBatch = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed.consumer_group.as_deref(), Some("workers"));
        assert_eq!(parsed.acknowledgments.len(), 2);
        assert_eq!(parsed.acknowledgments[0].transaction_id.as_deref(), Some("t1"));
        assert_eq!(parsed.acknowledgments[0].partition_id.as_deref(), Some("p1"));
        assert_eq!(parsed.acknowledgments[1].error.as_deref(), Some("poison"));

        // And the statuses survive normalization to the same four outcomes.
        assert_eq!(
            normalize_ack_status(parsed.acknowledgments[0].status.as_deref()),
            "completed"
        );
        assert_eq!(
            normalize_ack_status(parsed.acknowledgments[1].status.as_deref()),
            "dlq"
        );
    }

    #[test]
    fn client_single_ack_parses_into_the_brokers_struct() {
        let req = qp::AckRequest {
            transaction_id: "t1".into(),
            partition_id: "p1".into(),
            status: qp::AckStatus::Failed,
            consumer_group: Some("g".into()),
            lease_id: Some("L1".into()),
            error: Some("boom".into()),
        };
        let parsed: AckSingle = serde_json::from_slice(&serde_json::to_vec(&req).unwrap()).unwrap();
        assert_eq!(parsed.transaction_id.as_deref(), Some("t1"));
        assert_eq!(parsed.partition_id.as_deref(), Some("p1"));
        assert_eq!(parsed.consumer_group.as_deref(), Some("g"));
        assert_eq!(parsed.lease_id.as_deref(), Some("L1"));
        assert_eq!(normalize_ack_status(parsed.status.as_deref()), "failed");
    }

    /// The client predicts what the broker will do with a status string —
    /// including the aliases and the "unknown means nack" fallback. If the
    /// broker's table changes, the client's copy has to change with it.
    #[test]
    fn ack_status_tables_agree_exactly() {
        let cases = [
            None,
            Some("completed"),
            Some("success"),
            Some("acked"),
            Some("ok"),
            Some("retry"),
            Some("dlq"),
            Some("failed"),
            // Unrecognized, empty and wrong-case all fall through to `failed`.
            Some(""),
            Some("COMPLETED"),
            Some("Completed"),
            Some("nonsense"),
        ];
        for case in cases {
            assert_eq!(
                qp::AckStatus::parse(case).as_str(),
                normalize_ack_status(case),
                "disagreement on {case:?}"
            );
        }
    }

    // ------------------------------------------------------------- ack out

    #[test]
    fn rendered_ack_results_parse_into_the_client_type() {
        let acks = vec![
            Ack {
                txn: "t1".into(),
                partition_id: "p1".into(),
                worker: "w".into(),
                status: "completed",
                error: None,
            },
            Ack {
                txn: r#"esc"aped"#.into(),
                partition_id: "p1".into(),
                worker: "w".into(),
                status: "dlq",
                error: Some("poison".into()),
            },
        ];
        let rendered = render_ack_results(
            &acks,
            &[true, false],
            &[None, Some("Invalid or expired lease".to_string())],
            &[true, false],
            &[false, true],
            &[false, false],
            // PLAN_CONFLATION §2.4: item 0 closed a conflating lease that skipped
            // 98 positions; item 1 carries nothing, and must render no key.
            &[Some(98), None],
        );

        let parsed: Vec<qp::AckResult> = serde_json::from_str(&rendered).unwrap();
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].conflated, Some(98));
        assert_eq!(parsed[1].conflated, None);
        assert!(
            !rendered[rendered.find("esc").unwrap()..].contains("conflated"),
            "a non-conflating ack item must carry no conflated key: {rendered}"
        );

        assert_eq!(parsed[0].index, 0);
        assert_eq!(parsed[0].transaction_id, "t1");
        assert!(parsed[0].success);
        assert!(parsed[0].error.is_none());
        assert!(parsed[0].lease_released);
        assert!(!parsed[0].dlq);
        assert!(!parsed[0].noop);

        assert_eq!(parsed[1].transaction_id, r#"esc"aped"#);
        assert!(!parsed[1].success);
        assert_eq!(parsed[1].error.as_deref(), Some("Invalid or expired lease"));
        assert!(parsed[1].dlq);
    }

    // ---------------------------------------------------------------- pop

    /// The client builds its query string from `PopParams::to_pairs`; axum
    /// parses it back into the broker's `PopParams`. Both halves are exercised
    /// here so a renamed key (`timeout` vs `timeoutMillis` is the classic one)
    /// cannot pass silently.
    #[test]
    fn client_pop_params_deserialize_into_the_brokers_params() {
        let ours = qp::PopParams {
            batch: Some(25),
            partitions: Some(4),
            auto_ack: Some(true),
            wait: Some(true),
            timeout_millis: Some(15_000),
            lease_seconds: Some(90),
            consumer_group: Some("workers".into()),
            subscription_mode: Some(qp::SubscriptionMode::New),
            subscription_from: Some("now".into()),
            namespace: None,
            task: None,
            conflation: Some(true),
            autopilot: Some(true),
        };

        let qs = ours
            .to_pairs()
            .into_iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join("&");
        let uri: axum::http::Uri = format!("http://x/api/v1/pop/queue/q?{qs}").parse().unwrap();
        let axum::extract::Query(theirs) =
            axum::extract::Query::<PopParams>::try_from_uri(&uri).unwrap();

        assert_eq!(theirs.batch, Some(25));
        assert_eq!(theirs.partitions, Some(4));
        assert_eq!(theirs.auto_ack, Some(true));
        assert_eq!(theirs.wait, Some(true));
        assert_eq!(theirs.timeout, Some(15_000));
        assert_eq!(theirs.lease_seconds, Some(90));
        assert_eq!(theirs.consumer_group.as_deref(), Some("workers"));
        assert_eq!(theirs.subscription_mode.as_deref(), Some("new"));
        assert_eq!(theirs.subscription_from.as_deref(), Some("now"));
        // PLAN_CONFLATION §3.1: the client's key must be the one axum reads.
        assert_eq!(theirs.conflation, Some(true));
        // POP AUTOPILOT: same check for the opt-in. A client that renders the
        // flag under a name the broker does not read gets today's server-side
        // defaults and no echo, which looks exactly like an old broker.
        assert_eq!(theirs.autopilot, Some(true));
    }

    /// PLAN_CONFLATION §3.1 — the response keys the broker emits must be the ones
    /// the client type reads, and they must be ABSENT (never `false`) on a
    /// non-conflating response, which is what makes the §4 degrade-loudly check
    /// meaningful.
    #[test]
    fn conflation_echo_round_trips_through_the_client_type() {
        let (body, count, _meta) = render_pop_parts(
            &[],
            None,
            "orders",
            "workers",
            "lease-1",
            &crate::encryption::Encryption::from_env(),
            Conflation { on: true, conflict: true },
        );
        assert_eq!(count, 0, "empty claim");
        let parsed: qp::PopResponse = serde_json::from_str(&body).unwrap();
        assert!(parsed.conflation_applied(), "{body}");
        assert!(parsed.has_conflation_conflict(), "{body}");

        let (plain, _, _) = render_pop_parts(
            &[],
            None,
            "orders",
            "workers",
            "lease-1",
            &crate::encryption::Encryption::from_env(),
            Conflation::OFF,
        );
        assert!(!plain.contains("conflation"), "{plain}");
        let parsed: qp::PopResponse = serde_json::from_str(&plain).unwrap();
        assert!(!parsed.conflation_applied());
        assert!(!parsed.has_conflation_conflict());
    }

    /// `deliveryAttempt` is group/partition claim metadata, so every frame in
    /// one claimed partition gets the same value while another partition in
    /// the same response may have a different redelivery history.
    #[test]
    fn delivery_attempt_is_rendered_per_partition_and_defaults_to_one() {
        let frames_a = [
            FrameIn {
                message_id: [1; 16],
                txn: "a-1",
                trace_id: None,
                producer_sub: None,
                payload: br#"{"n":1}"#,
                encrypted: false,
            },
            FrameIn {
                message_id: [2; 16],
                txn: "a-2",
                trace_id: None,
                producer_sub: None,
                payload: br#"{"n":2}"#,
                encrypted: false,
            },
        ];
        let frames_b = [FrameIn {
            message_id: [3; 16],
            txn: "b-1",
            trace_id: None,
            producer_sub: None,
            payload: br#"{"n":3}"#,
            encrypted: false,
        }];
        let blob_a = base64::engine::general_purpose::STANDARD
            .encode(zstd_compress(&pack_frames(&frames_a), 1));
        let blob_b = base64::engine::general_purpose::STANDARD
            .encode(zstd_compress(&pack_frames(&frames_b), 1));
        let meta = serde_json::json!({
            "partitions": [
                {
                    "partition": "a",
                    "partitionId": "pa",
                    "deliveryAttempt": 2,
                    "segments": [{
                        "seq": 0,
                        "startOff": 0,
                        "take": 2,
                        "createdAt": "2026-08-28T10:00:00.000000Z",
                        "blob": blob_a
                    }]
                },
                {
                    "partition": "b",
                    "partitionId": "pb",
                    "deliveryAttempt": 5,
                    "segments": [{
                        "seq": 0,
                        "startOff": 0,
                        "take": 1,
                        "createdAt": "2026-08-28T10:00:00.000000Z",
                        "blob": blob_b
                    }]
                }
            ]
        });

        let (body, count, _) = build_pop_response(
            &meta.to_string(),
            None,
            "orders",
            "workers",
            "lease-1",
            &crate::encryption::Encryption::from_env(),
            Conflation::OFF,
        );
        assert_eq!(count, 3, "{body}");
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        let attempts: Vec<i64> = body["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["deliveryAttempt"].as_i64().unwrap())
            .collect();
        assert_eq!(attempts, [2, 2, 5]);

        // Rolling upgrade safety: metadata from the previous procedure has no
        // attempt key. It must remain a first delivery, never zero/null or a
        // parse failure that strands the lease.
        let fallback = serde_json::json!({
            "partitions": [{
                "partition": "a",
                "partitionId": "pa",
                "segments": [{
                    "seq": 0,
                    "startOff": 0,
                    "take": 1,
                    "createdAt": "2026-08-28T10:00:00.000000Z",
                    "blob": meta["partitions"][0]["segments"][0]["blob"].clone()
                }]
            }]
        });
        let (body, count, _) = build_pop_response(
            &fallback.to_string(),
            None,
            "orders",
            "workers",
            "lease-2",
            &crate::encryption::Encryption::from_env(),
            Conflation::OFF,
        );
        assert_eq!(count, 1, "{body}");
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(body["messages"][0]["deliveryAttempt"], 1);

        // The pinned and targeted routes deserialize a different SQL envelope;
        // pin that adapter too so the count cannot disappear only there.
        let specific = serde_json::json!({
            "partitionId": "pa",
            "deliveryAttempt": 7,
            "segments": [meta["partitions"][0]["segments"][0].clone()]
        });
        let (body, count, _) = build_pop_specific_response(
            &specific.to_string(),
            "orders",
            "a",
            "workers",
            "lease-3",
            &crate::encryption::Encryption::from_env(),
            Conflation::OFF,
        );
        assert_eq!(count, 2, "{body}");
        let body: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(
            body["messages"]
                .as_array()
                .unwrap()
                .iter()
                .all(|message| message["deliveryAttempt"] == 7),
            "{body}"
        );
    }

    /// All five SQL assembly paths must carry the group-scoped count into the
    /// metadata consumed above. This source pin complements the live Postgres
    /// test: it runs in every ordinary `cargo test`, even without a database.
    #[test]
    fn every_sql_pop_assembly_path_carries_delivery_attempt() {
        let sql = include_str!("../../sql/procedures/004_log_pop.sql");
        let procedures = [
            "queen.log_pop_specific_v1",
            "queen.log_pop_wildcard_wire_v1",
            "queen.log_pop_wildcard_bin_v1",
            "queen.log_pop_discover_wire_v1",
            "queen.log_pop_list_v1",
        ];
        for (index, procedure) in procedures.iter().enumerate() {
            let start = sql
                .find(&format!("CREATE FUNCTION {procedure}"))
                .unwrap_or_else(|| panic!("missing {procedure}"));
            let end = procedures
                .get(index + 1)
                .and_then(|next| sql[start..].find(&format!("CREATE FUNCTION {next}")))
                .map(|offset| start + offset)
                .unwrap_or(sql.len());
            assert!(
                sql[start..end].contains("'deliveryAttempt'"),
                "{procedure} dropped deliveryAttempt"
            );
        }
        assert!(
            sql.contains("RETURNING u.ei, p.last_offset, c.attempt_count"),
            "the hot-list fast path must return the count written atomically by its lease update"
        );
    }

    /// POP AUTOPILOT — the opt-in is a query parameter the broker must read under
    /// exactly the name the SDKs send, and (the whole compatibility argument)
    /// ABSENCE must be distinguishable from `false`: absent means "the client
    /// knows nothing about this option", which is every consumer in the field.
    #[test]
    fn the_autopilot_opt_in_is_read_from_the_query() {
        let uri: axum::http::Uri =
            "http://x/api/v1/pop/queue/q?consumerGroup=w&autopilot=true"
                .parse()
                .unwrap();
        let axum::extract::Query(p) =
            axum::extract::Query::<PopParams>::try_from_uri(&uri).unwrap();
        assert_eq!(p.autopilot, Some(true));
        // Per-dimension: the opt-in rides alongside an explicit knob, and the
        // explicit knob still arrives intact for the handler to honour.
        let uri: axum::http::Uri =
            "http://x/api/v1/pop/queue/q?autopilot=true&partitions=1"
                .parse()
                .unwrap();
        let axum::extract::Query(p) =
            axum::extract::Query::<PopParams>::try_from_uri(&uri).unwrap();
        assert_eq!(p.autopilot, Some(true));
        assert_eq!(p.partitions, Some(1));
        // An old client sends neither.
        let uri: axum::http::Uri = "http://x/api/v1/pop/queue/q".parse().unwrap();
        let axum::extract::Query(p) =
            axum::extract::Query::<PopParams>::try_from_uri(&uri).unwrap();
        assert_eq!(p.autopilot, None, "absent is not false");
    }

    /// POP AUTOPILOT — the response side of the same contract. The rendered pop
    /// body is byte-identical to the pre-feature one, and the echo (when there is
    /// one) is purely ADDITIVE: appended after every existing field, nothing
    /// reordered or retyped, still parseable by the client's own response type.
    #[test]
    fn the_autopilot_echo_is_additive_and_absent_by_default() {
        let (plain, count, _meta) = render_pop_parts(
            &[],
            None,
            "orders",
            "workers",
            "lease-1",
            &crate::encryption::Encryption::from_env(),
            Conflation::OFF,
        );
        assert_eq!(count, 0);
        assert!(
            !plain.contains("autopilot"),
            "a pop that did not opt in says nothing about autopilot: {plain}"
        );

        let mut echoed = plain.clone();
        crate::pop_autopilot::append_echo(
            &mut echoed,
            crate::pop_autopilot::Plan {
                partitions: 12,
                batch: crate::pop_autopilot::AUTO_BATCH_DEFAULT,
                chose_partitions: true,
                chose_batch: true,
            },
            100,
        );
        assert!(
            echoed.starts_with(&plain[..plain.len() - 1]),
            "every existing field is where it was: {echoed}"
        );
        // Still the same response to a client that ignores the new key…
        let parsed: qp::PopResponse = serde_json::from_str(&echoed).unwrap();
        assert!(parsed.success);
        assert!(parsed.is_empty());
        // …and the new key carries the two dimensions plus the pacing advice.
        let v: serde_json::Value = serde_json::from_str(&echoed).unwrap();
        assert_eq!(v["autopilot"]["partitions"], 12);
        assert_eq!(
            v["autopilot"]["batch"],
            crate::pop_autopilot::AUTO_BATCH_DEFAULT
        );
        assert_eq!(v["autopilot"]["waitMs"], 100);
    }

    /// A minimal client (everything defaulted) must still produce a query the
    /// broker accepts — and must not accidentally send `autoAck=false`, which
    /// the broker would read as opt-in-shaped.
    #[test]
    fn defaulted_pop_params_produce_an_empty_query() {
        let ours = qp::PopParams {
            auto_ack: Some(false),
            partitions: Some(1),
            ..Default::default()
        };
        assert!(ours.to_pairs().is_empty());

        let uri: axum::http::Uri = "http://x/api/v1/pop/queue/q".parse().unwrap();
        let axum::extract::Query(theirs) =
            axum::extract::Query::<PopParams>::try_from_uri(&uri).unwrap();
        assert_eq!(theirs.batch, None);
        assert_eq!(theirs.auto_ack, None);
    }

    /// The literal bytes the pop-maintenance branch returns must not read as a
    /// failure on the client: there is no `success` key, so the client's
    /// default has to be `true`.
    #[test]
    fn pop_maintenance_body_is_not_a_failure_on_the_client() {
        let body = "{\"messages\":[],\"paused\":true}";
        let parsed: qp::PopResponse = serde_json::from_str(body).unwrap();
        assert!(parsed.success, "paused must not read as failure");
        assert!(parsed.is_paused());
        assert!(parsed.is_empty());
    }
}

/// The push body parser's charset contract. This is the ONLY body parse in the
/// broker that borrows a JSON string into a `&str`-shaped field, and for one
/// release it rejected every escape sequence -- which meant the Go SDK's default
/// HTML escaping made the broker 400 its own client, taking the whole batch with
/// it. Each case below is a real encoder's real default.
#[cfg(test)]
mod push_body_charset {
    use super::*;

    // `value_literal` is the text as it appears INSIDE the JSON string literal.
    fn one(field: &str, value_literal: &str) -> String {
        match field {
            "queue" => format!(r#"{{"items":[{{"queue":"{value_literal}","payload":1}}]}}"#),
            "partition" => format!(
                r#"{{"items":[{{"queue":"q","partition":"{value_literal}","payload":1}}]}}"#
            ),
            _ => format!(
                r#"{{"items":[{{"queue":"q","payload":1,"transactionId":"{value_literal}"}}]}}"#
            ),
        }
    }

    fn field_of<'a>(p: &'a PushBody<'a>, field: &str) -> &'a str {
        match field {
            "queue" => p.items[0].queue.as_str(),
            "partition" => p.items[0].partition.as_deref().unwrap(),
            _ => p.items[0].transaction_id.as_deref().unwrap(),
        }
    }

    /// Every escape a first-party SDK emits BY DEFAULT must parse, on all three
    /// borrowed fields, and must arrive unescaped.
    #[test]
    fn escapes_that_real_sdks_emit_are_accepted_and_unescaped() {
        // (literal inside the JSON, expected decoded value, which encoder emits it)
        let cases: &[(&str, &str, &str)] = &[
            (r"a\u0026b", "a&b", "Go encoding/json, SetEscapeHTML default true"),
            (r"a\u003cb\u003ec", "a<b>c", "Go encoding/json, < and >"),
            (r#"a\"b"#, "a\"b", "every JSON encoder: quote"),
            (r"a\\b", r"a\b", "every JSON encoder: backslash"),
            (r"2026\/07\/BK-11", "2026/07/BK-11", "PHP json_encode / Guzzle: solidus"),
            (r"citt\u00e0", "città", "PHP + httpx<0.28: non-ASCII"),
            (r"a\u0041b", "aAb", "escape of a plain ASCII char"),
            ("Bed&Breakfast-771", "Bed&Breakfast-771", "JS/Python/Rust: literal &"),
            ("città", "città", "JS/Python/Rust: raw UTF-8"),
        ];
        for field in ["queue", "partition", "transactionId"] {
            for (lit, want, who) in cases {
                let body = one(field, lit);
                let parsed: PushBody = serde_json::from_slice(body.as_bytes())
                    .unwrap_or_else(|e| panic!("{field} / {who}: {lit} -> {e}"));
                assert_eq!(field_of(&parsed, field), *want, "{field} / {who}");
            }
        }
    }

    /// The clean path must stay ZERO-COPY. If this ever reads Owned, the push hot
    /// path has silently grown three allocations per message -- which is exactly
    /// what the obvious `Option<Cow<'a, str>>` spelling does, because
    /// `#[serde(borrow)]` only rewires a BARE `Cow<'a, str>` field.
    #[test]
    fn an_unescaped_body_is_still_borrowed_not_copied() {
        let body = br#"{"items":[{"queue":"orders","partition":"eu","payload":{"n":1},"transactionId":"evt-0198f5f0-2c9d-7000-9c0e-6d3b2a1f4c55"}]}"#;
        let p: PushBody = serde_json::from_slice(body).unwrap();
        let it = &p.items[0];
        assert!(matches!(it.queue.0, Cow::Borrowed(_)), "queue was copied");
        assert!(
            matches!(it.partition.as_ref().unwrap().0, Cow::Borrowed(_)),
            "partition was copied"
        );
        assert!(
            matches!(it.transaction_id.as_ref().unwrap().0, Cow::Borrowed(_)),
            "transactionId was copied"
        );
    }

    /// Control characters stay rejected, for a stated reason: the layer-1 dedup key
    /// (handle_push) and the fusion group key (fusion.rs) are both composed by
    /// joining fields on 0x1F. A 0x1F inside a field would alias two distinct
    /// (queue, partition, txn) triples onto one key.
    #[test]
    fn control_characters_are_rejected_on_every_borrowed_field() {
        for field in ["queue", "partition", "transactionId"] {
            for lit in [r"a\u001fb", r"a\u0000b", r"a\nb", r"a\tb", r"a\rb"] {
                let body = one(field, lit);
                let err = serde_json::from_slice::<PushBody>(body.as_bytes())
                    .err()
                    .unwrap_or_else(|| panic!("{field}: {lit} must not parse"));
                assert!(
                    err.to_string().contains("control character"),
                    "{field} / {lit}: {err}"
                );
            }
        }
    }

    /// A raw control byte can never reach the BORROWED arm -- serde_json's own
    /// tokenizer rejects it. That is what makes the check above free on the hot
    /// path, so it is pinned rather than assumed.
    #[test]
    fn serde_json_itself_rejects_a_raw_control_byte_in_a_string() {
        let mut body = String::from(r#"{"items":[{"queue":"a"#);
        body.push('\u{1f}');
        body.push_str(r#"b","payload":1}]}"#);
        let err = serde_json::from_slice::<PushBody>(body.as_bytes())
            .err()
            .expect("a raw control byte must not parse");
        assert!(err.to_string().contains("control character"), "{err}");
    }

    /// The payload is parsed as &RawValue and was never affected -- it must stay
    /// that way, escapes and all, byte for byte verbatim.
    #[test]
    fn payload_escapes_are_preserved_verbatim() {
        let body = br#"{"items":[{"queue":"q","payload":{"k\u0026":[{"z":"<\"\\"}]}}]}"#;
        let p: PushBody = serde_json::from_slice(body).unwrap();
        assert_eq!(p.items[0].payload.get(), r#"{"k\u0026":[{"z":"<\"\\"}]}"#);
    }

    /// One poisoned item used to discard the whole batch: there is a single
    /// from_slice for the entire request. Pins that a mixed batch survives.
    #[test]
    fn one_escaped_item_no_longer_discards_the_whole_batch() {
        let body = br#"{"items":[
            {"queue":"q","payload":1,"transactionId":"clean-1"},
            {"queue":"q","payload":2,"transactionId":"pull:Booking.com\u0026Expedia"},
            {"queue":"q","payload":3,"transactionId":"clean-3"}
        ]}"#;
        let p: PushBody = serde_json::from_slice(body).unwrap();
        assert_eq!(p.items.len(), 3);
        assert_eq!(
            p.items[1].transaction_id.as_deref(),
            Some("pull:Booking.com&Expedia")
        );
    }

    /// The response renderer must escape what the parser now accepts, or a
    /// transactionId containing a quote produces an invalid JSON response.
    #[test]
    fn rendered_results_are_valid_json_for_every_accepted_value() {
        let results = vec![ItemResult {
            message_id: "0190aaaa-0000-7000-8000-000000000001".into(),
            txn: "a\"b\\c&d<e>f".into(),
            queue: "q\"1".into(),
            status: "queued",
            dup_of: None,
        }];
        let rendered = render_push_results(&results);
        let back: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(back[0]["transaction_id"], "a\"b\\c&d<e>f");
        assert_eq!(back[0]["queueName"], "q\"1");
    }

    /// Every 400 this handler can emit must itself be valid JSON. The body used to
    /// be built with a raw format!, and a serde error embeds the offending value in
    /// Debug quotes -- so the error body for a bad push was unparseable.
    #[test]
    fn the_bad_body_error_is_itself_valid_json() {
        let err = serde_json::from_slice::<PushBody>(br#"{"items":[{"queue":true}]}"#)
            .err()
            .expect("a bool queue must not parse");
        let body = json_err("bad body: ", err);
        let back: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(
            back["error"].as_str().unwrap().starts_with("bad body: "),
            "{body}"
        );
    }

    /// transactionId is length-prefixed with a u16 in the segment frame codec, so
    /// an over-long one used to be silently truncated into a corrupt frame. The
    /// limit is now enforced at the HTTP boundary.
    #[test]
    fn the_txn_wire_limit_matches_the_frame_codec() {
        assert_eq!(MAX_TXN_BYTES, u16::MAX as usize);
    }
}

/// The HTTP -> wire demux (PLAN_KV_TIMERS §8.2): the index contract, which is
/// the one thing in this feature that can break two shipped clients WITHOUT any
/// error appearing anywhere.
#[cfg(test)]
mod wire_demux {
    use super::*;

// ---------------------------------------------- the HTTP -> wire demux (§8.2)
//
// These pin the index contract, which is the one thing in this feature that
// can break two shipped clients WITHOUT any error appearing anywhere.

/// PARITY, and the reason it is the first test here: with no riders, nothing
/// about the answer path changes. The guard does not consult the response at
/// all when nothing was sent — it must not turn "this database has no kv leg"
/// into a failure for a bundle that never asked for one, which is every
/// bundle every existing client sends.
#[test]
fn no_rider_means_no_guard_and_no_lookup() {
    let answer = serde_json::json!({"ok": true, "pushes": [], "acks": []});
    assert!(matches!(txn_rider_results(&answer, "kv", 0), Ok(None)));
    assert!(matches!(txn_rider_results(&answer, "timers", 0), Ok(None)));
}

/// The old-database detector. A broker that grew the kv array against a
/// database whose wire procedure predates it gets NO `kv` key back: the
/// bundle committed, and without this the caller would read `success:true`
/// for a transaction whose gate never ran.
#[test]
fn a_missing_rider_result_is_loud_not_silent() {
    let answer = serde_json::json!({"ok": true, "pushes": [], "acks": []});
    let e = txn_rider_results(&answer, "kv", 2).expect_err("must not read as success");
    assert!(e.contains("IGNORED"), "{e}");
    // Short is the same class of fault as missing.
    let short = serde_json::json!({"ok": true, "kv": [{"index": 0}]});
    assert!(txn_rider_results(&short, "kv", 2).is_err());
    let exact = serde_json::json!({"ok": true, "kv": [{"index": 0}, {"index": 1}]});
    assert!(txn_rider_results(&exact, "kv", 2).is_ok());
}

/// The scatter, which is the mapping itself: `index` becomes the FLAT
/// ordinal (what `results[]` and `failedIndex` both speak), and the
/// array-local one survives as `opIndex`. If these two were ever swapped,
/// every client would index somebody else's operation and nothing would say
/// so.
#[test]
fn the_scatter_rewrites_index_into_the_flat_space() {
    let mut results = vec![serde_json::Value::Null; 4];
    // Two pushes already occupy 0 and 1.
    results[0] = serde_json::json!({"index": 0, "type": "push"});
    results[1] = serde_json::json!({"index": 1, "type": "push"});
    let kv = vec![
        serde_json::json!({"index": 0, "op": "put", "applied": true}),
        serde_json::json!({"index": 1, "op": "get", "found": false}),
    ];
    txn_scatter_rider(&mut results, 2, "kv", &kv);
    assert_eq!(results[2]["index"], 2);
    assert_eq!(results[2]["opIndex"], 0);
    assert_eq!(results[2]["type"], "kv");
    assert_eq!(results[3]["index"], 3);
    assert_eq!(results[3]["opIndex"], 1);
    // The existing entries are untouched: riders APPEND, they never move
    // anything that was already there.
    assert_eq!(results[0]["index"], 0);
    assert_eq!(results[1]["type"], "push");
    assert!(results.iter().all(|r| !r.is_null()));
}

/// §8.2 point 4. The procedure raises with its own array-local ordinal; a
/// client that indexed with it would blame the wrong operation whenever a
/// bundle carries any push or ack at all — i.e. in the case the feature
/// exists for.
#[test]
fn failed_index_is_translated_into_the_flat_space() {
    let detail = r#"{"index":1,"op":"put","ns":"saga","key":"k","reason":"exists","version":7,"value":{"a":1}}"#;
    let v = txn_precondition_json("txn-1", "kv_precondition_failed", Some(detail), 3);
    assert_eq!(v["failedIndex"], 4, "kv ordinal 1 with kv_base 3 is flat 4");
    assert_eq!(v["reason"], "kv_precondition");
    assert_eq!(v["kvReason"], "exists");
    assert_eq!(v["version"], 7);
    assert_eq!(v["value"]["a"], 1);
    // Both spellings: the transaction envelope's `success` and the KV
    // route's `ok`, so one client branch reads the same verdict from either
    // surface.
    assert_eq!(v["success"], false);
    assert_eq!(v["ok"], false);
}

/// A DETAIL truncated by the procedure's 4 KiB cap is invalid JSON. It must
/// degrade to the bare verdict, never turn a legitimate lost race into a 500.
#[test]
fn a_truncated_detail_still_yields_the_verdict() {
    let v = txn_precondition_json("txn-1", "kv_precondition_failed", Some("{\"index\":1,\"val"), 0);
    assert_eq!(v["reason"], "kv_precondition");
    assert!(v.get("failedIndex").is_none());
    assert_eq!(
        txn_fail_precondition("txn-1", "x", None, 0).status(),
        StatusCode::OK,
        "a lost precondition is a verdict, never a 4xx or a 5xx"
    );
}

/// §5.5: the boundary on this wire is COST, not the kind of operation.
/// `getMany` is allowed and counted by its keys; `getPrefix` is refused
/// before a connection is spent.
#[test]
fn the_wire_kv_guard_counts_keys_and_refuses_prefix() {
    let many: Vec<serde_json::Value> = (0..3)
        .map(|_| serde_json::json!({"op": "getMany", "ns": "n", "keys": ["a", "b", "c"]}))
        .collect();
    assert!(txn_check_kv(&many, "t").is_none(), "9 keys is under the ceiling");

    let over: Vec<serde_json::Value> = (0..2)
        .map(|_| {
            let keys: Vec<String> = (0..200).map(|i| format!("k{i}")).collect();
            serde_json::json!({"op": "getMany", "ns": "n", "keys": keys})
        })
        .collect();
    assert!(
        txn_check_kv(&over, "t").is_some(),
        "400 keys over 2 ops must be refused by the KEY budget, which an op \
         count alone does not bound"
    );

    let prefix = vec![serde_json::json!({"op": "getPrefix", "ns": "n", "prefix": "p"})];
    assert!(txn_check_kv(&prefix, "t").is_some());
}

/// A verdict is not a database failure. Counting a lost idempotency gate — the
/// single most frequent outcome of the product's number-one use case — as a DB
/// error would make every dashboard read as broken under normal operation.
#[test]
fn a_verdict_is_never_counted_as_a_database_failure() {
    for (state, msg, reason) in [
        (Some("23514"), "kv_precondition_failed", "kv_precondition"),
        (Some("22023"), "kv_bad_request", "bad_request"),
        (Some("22001"), "kv_value_too_large", "payload_too_large"),
        (None, "QDUP duplicate messages", "duplicate"),
        (None, "QTXN ack failed", "ack_rejected"),
        (None, "QTIMER op 0: field producerSub is server-owned", "ack_rejected"),
    ] {
        let (got, is_failure) = txn_reason_for(state, msg);
        assert_eq!(got, reason, "{msg}");
        assert!(!is_failure, "a verdict must not inflate the DB error series: {msg}");
    }
    // A real infrastructure failure is the one thing that IS counted.
    let (got, is_failure) = txn_reason_for(Some("08006"), "connection closed");
    assert_eq!(got, "db_error");
    assert!(is_failure);
}

/// The failure envelope grew (§8.3) and every failure now carries a `reason`
/// code. Without it a client has to string-match the message, which is
/// forbidden everywhere in this codebase — and it is what all seven of them
/// have had to do until now.
#[test]
fn every_failure_carries_a_switchable_reason() {
    let v = txn_fail_json("txn-1", "duplicate", "QDUP ...");
    assert_eq!(v["reason"], "duplicate");
    assert_eq!(v["success"], false);
    assert_eq!(v["transactionId"], "txn-1");
    assert!(v["results"].as_array().unwrap().is_empty());
    // The three fields every client already reads are still exactly where
    // they were: this grew, it did not change shape.
    assert!(v.get("error").is_some());
}

/// The wire caps are the mirror of the procedure's `p_in_wire = true`
/// constants. They are deliberately tighter than the HTTP surface's, and if
/// one side moves without the other the edge stops being a guard and starts
/// being a second, disagreeing opinion.
#[test]
fn the_wire_caps_mirror_the_procedures_in_wire_constants() {
    assert_eq!(WIRE_KV_MAX_OPS, 64);
    assert_eq!(WIRE_KV_MAX_KEYS, 256);
}
}
