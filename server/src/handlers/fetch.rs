//! `POST /api/v1/fetch` — PLAN_QUEEN_KAFKA.md core change C2.
//!
//! A batched, multi-partition, READ-ONLY read from an absolute offset, with a
//! long poll. It is the consume half of what a Kafka-shaped client needs and it
//! is deliberately NOT a pop:
//!
//!   * no lease is taken, so nothing has to be acked and nothing expires;
//!   * no consumer cursor is read, created or advanced, so a fetch interleaved
//!     with a real consumer group changes nothing that group sees;
//!   * no claim, no `SKIP LOCKED`, no partition row lock — two callers asking
//!     for the same offsets both get the same records.
//!
//! Which is why the caller must carry its own position. `POST` rather than
//! `GET` only because the request is a batch: a fetch of 1024 partitions does
//! not fit in a query string, and Kafka's own Fetch is a batch of
//! (topic, partition, offset) triples.
//!
//! Every entry answers with `highWatermark` and `logStartOffset` whether or not
//! it carried a record, so an empty fetch doubles as the bounds probe (the
//! ListOffsets `-1`/`-2` pair) and a Kafka facade needs no second endpoint for
//! it.
//!
//! ## Tenancy
//!
//! Exactly `handle_pop`'s story and by the same mechanism: the request tenant
//! comes from `Extension<Tenant>` (the middleware that reads the trusted
//! header, never a body field) and is bound into the SQL, which resolves a
//! partition through `queen.queues` on `(name, tenant_id)`. A queue belonging
//! to another tenant therefore resolves to nothing and takes the SAME
//! `UNKNOWN_TOPIC_OR_PARTITION` arm as a queue that exists nowhere — the two
//! answers are byte-identical, so this endpoint cannot be used to probe another
//! tenant's queue namespace. Nothing here is addressed by raw partition uuid,
//! so there is no ownership gate to run (the `handle_get_message` case): name
//! resolution IS the gate.
//!
//! ## Pop maintenance is deliberately NOT honoured here
//!
//! `pop_maintenance` pauses the CLAIM path — leases, cursor writes, redelivery
//! bookkeeping — because those are the things an operator needs still while
//! working on a cell. A fetch writes none of them; it is a read, in the class of
//! `GET /api/v1/messages` and the DLQ listing, neither of which pauses. Pausing
//! it would also mean the pause is invisible in the answer (there is no per-entry
//! "paused" a Kafka client could act on) and would look to a consumer exactly
//! like a queue that went empty.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::de::{self, Deserializer, Visitor};
use serde::Deserialize;

use super::data::push_utf8;
use super::{json, json_err, tenant_queue_key, AppState};
use crate::admission::Lane;
use crate::db;
use crate::fusion::json_escape_into;
use crate::tenant::Tenant;

// ---------------------------------------------------------------------------
// EDGE CEILINGS. Every one of them is clamped rather than rejected, except the
// entry count: a caller that asks for more than the broker will serve gets what
// the broker will serve, and learns the real bound from the answer (fewer
// records than asked, the next offset implied by what came back). The entry
// count is the one exception because silently dropping entries would leave the
// caller waiting for partitions the broker never looked at.

/// Max `entries` in one request. 1024 is the Kafka-facade default partition
/// count (`QUEEN_KAFKA_DEFAULT_PARTITIONS`), so a consumer assigned every
/// partition of one topic fits in a single call.
const MAX_ENTRIES: usize = 1024;

/// Max long-poll parking, milliseconds. Above this a client is not long-polling,
/// it is holding a connection: Kafka's own `fetch.max.wait.ms` default is 500.
const MAX_WAIT_MS: u64 = 30_000;

/// Per-entry `maxBytes` default and ceiling, over the COMPRESSED segment bytes
/// the SQL reads (not the rendered JSON, which is larger).
const DEFAULT_MAX_BYTES: i64 = 1024 * 1024;
const MAX_BYTES_PER_ENTRY: i64 = 8 * 1024 * 1024;

/// Whole-response ceiling, spent across entries in request order. Without it
/// 1024 entries at 8 MiB each would be a 8 GiB read that no clamp on a single
/// entry can prevent.
const MAX_TOTAL_BYTES: i64 = 64 * 1024 * 1024;

/// Per-entry record ceiling. `maxBytes` bounds the COMPRESSED read; this bounds
/// the RENDERED response, which is what actually has to fit in memory — a
/// partition of densely packed small messages can hold a lot of frames behind
/// very few compressed bytes.
const MAX_RECORDS_PER_ENTRY: i32 = 10_000;

/// Long-poll recheck ceiling. The wake gate (`notifier`) covers log appends on
/// both the hot-list and the legacy path, so in practice a push wakes a parked
/// fetch in ~one wake tick; this is the backstop for the wakes the gate cannot
/// deliver (a peer broker whose mesh frame was dropped, a queue whose gate was
/// evicted between the probe and the park). Plan C2 allows 100-250ms.
const RECHECK_MS: u64 = 200;

/// Per-entry error markers, spelled as Kafka spells them because a facade maps
/// them straight to error codes and a native client at least gets a name it can
/// search for.
///
/// The strings themselves are produced by `032_log_fetch.sql` and travel
/// through the meta untouched — this handler never composes one. They are named
/// here anyway so the agreement between the SQL that WRITES them and the client
/// type that READS them is pinned by a test instead of by three files happening
/// to spell the same thing (`tests_unit/fetch_render.rs`).
///
/// `allow(dead_code)` because that pin is the ONLY reader: nothing on the hot
/// path composes a marker, which is the property being pinned. Same precedent
/// as `frames::FrameRef::encrypted`, and stated because CI compiles with
/// `-D warnings`.
#[allow(dead_code)]
const ERR_UNKNOWN: &str = "UNKNOWN_TOPIC_OR_PARTITION";
#[allow(dead_code)]
const ERR_OUT_OF_RANGE: &str = "OFFSET_OUT_OF_RANGE";

// ---------------------------------------------------------------------------
// Request

/// A partition name that also accepts a JSON NUMBER.
///
/// Queen partitions are named, Kafka partitions are numbered, and the mapping
/// the plan settles on is "Kafka partition n = Queen partition n" — i.e. the
/// name `"3"`. A facade that serialises the number it already holds would
/// otherwise 400 the WHOLE batch on a type error, for a request whose meaning
/// was never in doubt. Accepting both spellings here costs one visitor arm and
/// removes a papercut that is invisible until it fires.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionName(String);

impl<'de> Deserialize<'de> for PartitionName {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = PartitionName;
            fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
                f.write_str("a partition name (string) or a partition number")
            }
            fn visit_str<E: de::Error>(self, v: &str) -> Result<PartitionName, E> {
                Ok(PartitionName(v.to_owned()))
            }
            fn visit_u64<E: de::Error>(self, v: u64) -> Result<PartitionName, E> {
                Ok(PartitionName(v.to_string()))
            }
            fn visit_i64<E: de::Error>(self, v: i64) -> Result<PartitionName, E> {
                Ok(PartitionName(v.to_string()))
            }
        }
        d.deserialize_any(V)
    }
}

#[derive(Deserialize)]
struct FetchEntry {
    queue: String,
    /// Omit for `"Default"` — the same literal the push path applies
    /// (`handlers::data`), so a producer that omits it and a fetcher that omits
    /// it address the same lane.
    #[serde(default)]
    partition: Option<PartitionName>,
    offset: i64,
    #[serde(rename = "maxBytes", default)]
    max_bytes: Option<i64>,
}

#[derive(Deserialize)]
struct FetchBody {
    #[serde(default)]
    entries: Vec<FetchEntry>,
    /// How long to park when nothing is available. Absent or 0 = answer now.
    #[serde(rename = "maxWaitMs", default)]
    max_wait_ms: Option<u64>,
    /// Bytes of record payload that must accumulate before the poll returns.
    /// Absent = 1, i.e. "return as soon as anything is available"; 0 = never
    /// park. An empty payload counts as one byte, so `minBytes: 1` means what
    /// every Kafka client means by it — wake me on any record — rather than
    /// parking through a run of `null` payloads.
    #[serde(rename = "minBytes", default)]
    min_bytes: Option<i64>,
}

// ---------------------------------------------------------------------------
// SP result (032_log_fetch's `meta`), index-aligned with the request entries.

#[derive(Deserialize)]
struct FetchMeta {
    #[serde(default)]
    entries: Vec<EntryMeta>,
}

#[derive(Deserialize)]
struct EntryMeta {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    high: i64,
    #[serde(rename = "logStart", default)]
    log_start: i64,
    #[serde(default)]
    segments: Vec<SegMeta>,
}

#[derive(Deserialize)]
struct SegMeta {
    /// The segment's `base_offset`: a record's ABSOLUTE offset is
    /// `base + its frame index`, which is the whole addressing scheme.
    base: i64,
    #[serde(rename = "startIdx")]
    start_idx: i32,
    take: i32,
    #[serde(rename = "createdAt")]
    created_at: String,
}

/// What one probe produced: the rendered body, plus the two facts the long-poll
/// decides on.
struct Probe {
    body: String,
    /// Record payload bytes delivered, each record counting at least 1.
    bytes: i64,
    /// An entry carried a per-entry error. The poll returns AT ONCE on one:
    /// a client whose offset fell off the log, or that named a queue this
    /// tenant does not have, must be told now — parking would answer the
    /// question `maxWaitMs` later, identically, having taught it nothing.
    any_error: bool,
}

pub async fn handle_fetch(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    let parsed: FetchBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    if parsed.entries.is_empty() {
        return json(StatusCode::OK, "{\"entries\":[]}".to_string());
    }
    if parsed.entries.len() > MAX_ENTRIES {
        return json(
            StatusCode::BAD_REQUEST,
            json_err(
                "bad body: ",
                format_args!(
                    "{} entries exceeds the {MAX_ENTRIES}-entry limit",
                    parsed.entries.len()
                ),
            ),
        );
    }
    if let Some(bad) = parsed.entries.iter().position(|e| e.offset < 0) {
        // Rejected rather than clamped to 0: a negative offset is a Kafka
        // SENTINEL (-1 latest, -2 earliest) that this endpoint does not
        // implement, and silently serving it as 0 would hand a consumer the
        // whole backlog when it asked for the tail.
        return json(
            StatusCode::BAD_REQUEST,
            json_err(
                "bad body: ",
                format_args!(
                    "entry {bad} has a negative offset; read the bounds from \
                     highWatermark / logStartOffset instead"
                ),
            ),
        );
    }

    // Index-aligned typed arrays, built ONCE: the retry loop below re-runs the
    // same query with the same bindings, so a parked fetch allocates nothing
    // per iteration.
    let n = parsed.entries.len();
    let mut queues: Vec<String> = Vec::with_capacity(n);
    let mut partitions: Vec<String> = Vec::with_capacity(n);
    let mut offsets: Vec<i64> = Vec::with_capacity(n);
    let mut max_bytes: Vec<i32> = Vec::with_capacity(n);
    for e in &parsed.entries {
        queues.push(e.queue.clone());
        partitions.push(
            e.partition
                .as_ref()
                .map(|p| p.0.clone())
                .unwrap_or_else(|| "Default".to_string()),
        );
        offsets.push(e.offset);
        max_bytes.push(
            e.max_bytes
                .unwrap_or(DEFAULT_MAX_BYTES)
                .clamp(1, MAX_BYTES_PER_ENTRY) as i32,
        );
    }

    let max_wait_ms = parsed.max_wait_ms.unwrap_or(0).min(MAX_WAIT_MS);
    let min_bytes = parsed.min_bytes.unwrap_or(1).max(0);
    let deadline = Instant::now() + Duration::from_millis(max_wait_ms);

    // Where to park. The wake gate is per (tenant, queue), so a single-queue
    // fetch — which is what an assignment-per-topic consumer sends — parks on
    // exactly the gate its own pushes ring. A fetch spanning queues has no such
    // gate and parks on the tenant-wide discovery gate instead, which every
    // push of this tenant wakes: a superset, so it costs re-probes and never a
    // missed wake.
    let all_same_queue = queues.iter().all(|q| q == &queues[0]);
    let qkey = if all_same_queue {
        Some(tenant_queue_key(tenant.as_str(), &queues[0]))
    } else {
        None
    };

    loop {
        // One probe. The pooled connection is acquired, used and RELEASED
        // inside this block (`resolve_query_timeout` drops it on success and on
        // a statement error, and detaches + cancels on a broker-side timeout),
        // so the park below never holds one — spec §10, the same rule the
        // parked pop obeys.
        let probe = {
            // A fetch is consume-side database work and shares the pop lane's
            // budget. It does NOT get pop's cheap `has_pending` pre-gate: that
            // probe is (queue, group)-scoped and a fetch has no group. What
            // stands in for it is the shape of the SQL itself — a caught-up
            // entry (offset == highWatermark) costs ONE indexed row read and
            // never touches queen.log_segments — plus the recheck ceiling, so a
            // parked fetch re-probes at most ~5 times a second.
            let mut slot = st.admission.acquire(Lane::Pop).await;
            let client = match st.pool.get().await {
                Ok(c) => c,
                Err(_) => {
                    st.metrics.record_db_error();
                    drop(slot);
                    return json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "{\"error\":\"pool\"}".to_string(),
                    );
                }
            };
            let cancel_token = client.cancel_token();
            let t0 = Instant::now();
            let res = tokio::time::timeout(
                st.stmt_timeout,
                db::log_fetch_bin(
                    &client,
                    &queues,
                    &partitions,
                    &offsets,
                    &max_bytes,
                    MAX_TOTAL_BYTES,
                    MAX_RECORDS_PER_ENTRY,
                    tenant.as_str(),
                ),
            )
            .await;
            let rtt = t0.elapsed();
            if matches!(res, Ok(Ok(_))) {
                slot.commit_done(rtt);
            }
            drop(slot);
            let (meta, blobs) = match db::resolve_query_timeout(
                res,
                client,
                cancel_token,
                "log_fetch",
                &st.metrics,
            ) {
                Some(v) => v,
                None => {
                    return json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "{\"error\":\"fetch failed\"}".to_string(),
                    )
                }
            };
            match render_fetch(&meta, &blobs, &queues, &partitions, &st.encryption) {
                Some(p) => p,
                None => {
                    return json(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "{\"error\":\"fetch decode failed\"}".to_string(),
                    )
                }
            }
        };

        if probe.any_error || probe.bytes >= min_bytes || Instant::now() >= deadline {
            return json(StatusCode::OK, probe.body);
        }

        let wait = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_millis(RECHECK_MS));
        match &qkey {
            Some(k) => st.notifier.wait_queue(k, wait).await,
            None => st.notifier.wait_any(tenant.as_str(), wait).await,
        };
        // Woken or timed out, the answer is the same: re-probe. A wake is a
        // hint that a push landed on this queue, never a promise that it landed
        // on one of THESE partitions at an offset we asked for.
    }
}

/// Render the response body from the SP meta + the aligned blobs.
///
/// Hand-rendered into one pre-sized `String` for the same reason `render_pop_parts`
/// is: the payload is spliced VERBATIM out of the decompressed frame, so every
/// JSON shape a producer pushed survives byte for byte (a round trip through
/// `serde_json::Value` would renormalise numbers) and the bytes are copied once.
///
/// `None` when the meta does not parse or does not align with the request — both
/// are broker/SQL bugs, and answering a caller with a MISALIGNED entry would
/// hand it another partition's bounds to commit.
fn render_fetch(
    meta: &str,
    blobs: &[Vec<u8>],
    queues: &[String],
    partitions: &[String],
    enc: &crate::encryption::Encryption,
) -> Option<Probe> {
    let parsed: FetchMeta = serde_json::from_str(meta).ok()?;
    if parsed.entries.len() != queues.len() {
        return None;
    }

    // Capacity: the decompressed payloads dominate, and the blobs are the only
    // measure of them we have before decompressing. Undersizing costs one
    // doubling; the per-segment reserve below refines it with the real size.
    let est = 32 + parsed.entries.len() * 128 + blobs.iter().map(|b| b.len() * 2).sum::<usize>();
    let mut out = String::with_capacity(est);
    let mut bytes: i64 = 0;
    let mut any_error = false;
    let mut blob_idx = 0usize;

    out.push_str("{\"entries\":[");
    for (i, em) in parsed.entries.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"queue\":\"");
        json_escape_into(&mut out, &queues[i]);
        out.push_str("\",\"partition\":\"");
        json_escape_into(&mut out, &partitions[i]);
        out.push_str("\",\"records\":[");
        let mut records = 0usize;
        for seg in &em.segments {
            // Blobs are flattened in the meta's own traversal order, so the
            // running index and the meta walk stay aligned by construction. A
            // segment the meta announces without a blob is that alignment
            // broken, which fails the whole render rather than reading the
            // NEXT entry's segment into this one.
            let blob = blobs.get(blob_idx)?;
            blob_idx += 1;
            let raw = crate::frames::zstd_decompress(blob);
            let frames = match crate::frames::unpack_frames_ref(&raw) {
                Some(f) => f,
                // A segment that will not decode is skipped, not fatal: the
                // rest of the fetch is still correct data, and the caller's
                // next request re-reads the same offsets rather than silently
                // stepping over them (nothing here moves a cursor).
                None => continue,
            };
            let start = seg.start_idx.max(0) as usize;
            let end = (start + seg.take.max(0) as usize).min(frames.len());
            out.reserve(raw.len() + end.saturating_sub(start) * 128);
            for (j, f) in frames.iter().enumerate().take(end).skip(start) {
                if records > 0 {
                    out.push(',');
                }
                out.push_str("{\"offset\":");
                out.push_str(&(seg.base + j as i64).to_string());
                // The transaction id is the message's addressable identity
                // (`GET /api/v1/messages/:pid/:txn` is keyed by it, not by the
                // message id) AND the slot the Kafka mapping stores a record
                // key in — so it is the one identity worth the bytes here.
                // There is deliberately no `headers`: a stored frame carries no
                // header map, and emitting an always-empty one would advertise
                // a feature the engine does not have.
                out.push_str(",\"transactionId\":\"");
                json_escape_into(&mut out, f.txn);
                out.push_str("\",\"payload\":");
                if f.payload.is_empty() {
                    out.push_str("null");
                } else if let Some(pt) = enc.decrypt_payload_bytes(f.payload) {
                    // Same envelope-sniff as the pop renderer: decrypt when the
                    // key is configured, splice as-is when it is not.
                    push_utf8(&mut out, &pt);
                } else {
                    push_utf8(&mut out, f.payload);
                }
                out.push_str(",\"ts\":\"");
                json_escape_into(&mut out, &seg.created_at);
                out.push_str("\"}");
                records += 1;
                // An empty payload still counts, so `minBytes: 1` means "any
                // record" — see FetchBody::min_bytes.
                bytes += f.payload.len().max(1) as i64;
            }
        }
        out.push_str("],\"highWatermark\":");
        out.push_str(&em.high.to_string());
        out.push_str(",\"logStartOffset\":");
        out.push_str(&em.log_start.to_string());
        if let Some(e) = em.error.as_deref().filter(|e| !e.is_empty()) {
            any_error = true;
            out.push_str(",\"error\":\"");
            json_escape_into(&mut out, e);
            out.push('"');
        }
        out.push('}');
    }
    out.push_str("]}");

    Some(Probe {
        body: out,
        bytes,
        any_error,
    })
}

#[cfg(test)]
#[path = "../tests_unit/fetch_render.rs"]
mod fetch_render_tests;
