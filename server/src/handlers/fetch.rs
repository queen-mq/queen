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
//! Only a missing QUEUE is that error. A queue that exists with a lane nothing
//! has been pushed to yet answers as the empty log it is (`high` 0, `logStart`
//! 0, no error), because `queen.log_partitions` rows are materialised lazily by
//! the first push — see 032_log_fetch.sql. Calling those unknown would be
//! false, and since any per-entry error releases the poll at once (below) one of
//! them would defeat parking for a whole 1024-entry batch.
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

/// Per-entry record ceiling, over the frames the SQL slices out of a segment.
/// It bounds a single entry's contribution; the whole-call bound on the
/// response is `MAX_RENDERED_BYTES` below.
const MAX_RECORDS_PER_ENTRY: i32 = 10_000;

/// Whole-call ceiling on the RENDERED response, and the only bound that is
/// about MEMORY.
///
/// Every other ceiling here is spent in COMPRESSED segment bytes, which is the
/// currency the SQL can count — and compressed bytes say nothing about what
/// they cost to hold. Queen segments are zstd'd, and the payloads a queue
/// carries are frequently near-identical (a heartbeat, a telemetry frame, one
/// document re-pushed with a field changed), which is the shape zstd compresses
/// 100:1 or better. Spending the 64 MiB `MAX_TOTAL_BYTES` on such a queue
/// decompresses several GiB, and `render_fetch` splices ALL of it, verbatim,
/// into ONE accumulating `String` before a byte of the response is written:
/// without this bound a single legal read-only fetch is an out-of-memory of the
/// broker process, once per admission slot.
///
/// Enforced the way the SQL enforces its own (§"Exactly ONE segment escapes"):
/// the FIRST record of the call is always delivered, so a consumer that meets an
/// over-large record steps past it instead of stalling for ever, and every
/// record after it stops at the ceiling. Truncation is safe for exactly the
/// reason the SQL's `v_take` truncation is — records are contiguous from
/// `startIdx`, so the caller resumes at the first offset it did not get — and
/// entries the ceiling cut short still report their bounds, so nothing a caller
/// commits is ever wrong, only shorter than it asked for.
const MAX_RENDERED_BYTES: usize = 64 * 1024 * 1024;

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

/// What one probe produced: the rendered body, the two facts the long-poll
/// decides on, and the watermarks that say whether re-reading could produce
/// anything different.
struct Probe {
    body: String,
    /// Record payload bytes delivered, each record counting at least 1.
    bytes: i64,
    /// An entry carried a per-entry error. The poll returns AT ONCE on one:
    /// a client whose offset fell off the log, or that named a queue this
    /// tenant does not have, must be told now — parking would answer the
    /// question `maxWaitMs` later, identically, having taught it nothing.
    any_error: bool,
    /// Per entry, index-aligned with the request: `highWatermark` and
    /// `logStartOffset` as this probe saw them. Segments are immutable
    /// (032_log_fetch's gate header), so this pair IS the state of the read:
    /// while it holds, `body` is still the answer.
    highs: Vec<i64>,
    starts: Vec<i64>,
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

    // The one unconditional read: the caller is owed an answer, and its shape is
    // what every decision below is made against.
    let mut probe = match read(&st, &queues, &partitions, &offsets, &max_bytes, &tenant).await {
        Ok(p) => p,
        Err(r) => return r,
    };

    loop {
        if probe.any_error || probe.bytes >= min_bytes {
            return json(StatusCode::OK, probe.body);
        }
        let now = Instant::now();
        if now >= deadline {
            return json(StatusCode::OK, probe.body);
        }

        let wait = deadline
            .saturating_duration_since(now)
            .min(Duration::from_millis(RECHECK_MS));
        match &qkey {
            Some(k) => st.notifier.wait_queue(k, wait).await,
            None => st.notifier.wait_any(tenant.as_str(), wait).await,
        };

        // Woken or timed out, the same question: did anything MOVE? A wake is a
        // hint that a push landed on this queue, never a promise that it landed
        // on one of THESE partitions — and the answer already rendered stays
        // correct until one of these lanes' watermarks changes, because segments
        // are immutable (032_log_fetch, the gate's header).
        //
        // So the re-probe is this permit-free gate and not the read. It is the
        // fetch path's `has_pending` (handlers/data.rs, the discovery-latency
        // fix of 2026-07-24): an EMPTY re-probe must cost zero admission budget,
        // or the O(#parked consumers) storm of them saturates the SHARED pop
        // limiter and real deliveries queue behind it with a wait that grows
        // linearly with the consumer count. It also removes the other half of
        // the waste — a `minBytes` a quiet partition will never reach used to
        // re-read, re-decompress and re-render the identical body up to 150
        // times before returning the one the first read had already produced.
        if !moved(&st, &queues, &partitions, &probe, &tenant).await {
            continue;
        }
        probe = match read(&st, &queues, &partitions, &offsets, &max_bytes, &tenant).await {
            Ok(p) => p,
            Err(r) => return r,
        };
    }
}

/// One read: the admission-limited SQL call plus the render.
///
/// The pooled connection is acquired, used and RELEASED inside this function
/// (`resolve_query_timeout` drops it on success and on a statement error, and
/// detaches + cancels on a broker-side timeout), so the caller's park never
/// holds one — spec §10, the same rule the parked pop obeys. `Err` is the
/// response to send as-is.
async fn read(
    st: &AppState,
    queues: &[String],
    partitions: &[String],
    offsets: &[i64],
    max_bytes: &[i32],
    tenant: &Tenant,
) -> Result<Probe, Response> {
    // A fetch is consume-side database work and shares the pop lane's budget.
    let mut slot = st.admission.acquire(Lane::Pop).await;
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            drop(slot);
            return Err(json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "{\"error\":\"pool\"}".to_string(),
            ));
        }
    };
    let cancel_token = client.cancel_token();
    let t0 = Instant::now();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::log_fetch_bin(
            &client,
            queues,
            partitions,
            offsets,
            max_bytes,
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
    let (meta, blobs) =
        match db::resolve_query_timeout(res, client, cancel_token, "log_fetch", &st.metrics) {
            Some(v) => v,
            None => {
                return Err(json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "{\"error\":\"fetch failed\"}".to_string(),
                ))
            }
        };
    render_fetch(&meta, &blobs, queues, partitions, &st.encryption).ok_or_else(|| {
        json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "{\"error\":\"fetch decode failed\"}".to_string(),
        )
    })
}

/// Has any named lane moved off the watermarks `probe` holds?
///
/// Borrows a pooled connection and takes NO admission permit — the whole point,
/// and the same bargain `has_pending` strikes on the pop path (the probe is one
/// indexed row read per entry against `queen.log_partitions`; `pool.get` is
/// uncontended, measured ~0µs). A probe that cannot run answers `true`, so a
/// blip costs a wasted read and never a missed record.
async fn moved(
    st: &AppState,
    queues: &[String],
    partitions: &[String],
    probe: &Probe,
    tenant: &Tenant,
) -> bool {
    match st.pool.get().await {
        Ok(c) => db::log_fetch_changed(
            &c,
            queues,
            partitions,
            &probe.highs,
            &probe.starts,
            tenant.as_str(),
        )
        .await
        .unwrap_or(true),
        Err(_) => true,
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
    render_capped(meta, blobs, queues, partitions, enc, MAX_RENDERED_BYTES)
}

/// [`render_fetch`] with the memory ceiling as an argument, so the truncation
/// rule is testable without building a 64 MiB response to trip it.
fn render_capped(
    meta: &str,
    blobs: &[Vec<u8>],
    queues: &[String],
    partitions: &[String],
    enc: &crate::encryption::Encryption,
    cap: usize,
) -> Option<Probe> {
    let parsed: FetchMeta = serde_json::from_str(meta).ok()?;
    if parsed.entries.len() != queues.len() {
        return None;
    }

    // Capacity: the decompressed payloads dominate, and the blobs are the only
    // measure of them we have before decompressing. Undersizing costs one
    // doubling; the per-segment reserve below refines it with the real size.
    // Clamped at the ceiling the render itself stops at, so the ESTIMATE cannot
    // be the allocation the ceiling exists to prevent.
    let est = (32
        + parsed.entries.len() * 128
        + blobs
            .iter()
            .map(|b| b.len().saturating_mul(2))
            .sum::<usize>())
    .min(cap);
    let mut out = String::with_capacity(est);
    let mut bytes: i64 = 0;
    let mut any_error = false;
    let mut blob_idx = 0usize;
    // Records emitted by the WHOLE call, and whether the ceiling has been hit.
    // Both are call-wide, not per entry: a per-entry allowance is what makes a
    // whole-response bound meaningless at 1024 entries (032_log_fetch's header
    // makes the same argument about its own budget).
    let mut delivered = 0usize;
    let mut full = false;
    // The watermarks the re-probe gate compares against, in request order.
    let mut highs = Vec::with_capacity(parsed.entries.len());
    let mut starts = Vec::with_capacity(parsed.entries.len());

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
        // Once full, the remaining entries are rendered as empty — bounds and
        // all, so the answer stays index-aligned with the request and every
        // caller still learns where its lanes stand. Their blobs are never
        // decompressed, which is where the CPU half of the ceiling is saved.
        for seg in &em.segments {
            if full {
                break;
            }
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
            out.reserve(raw.len().min(cap) + end.saturating_sub(start) * 128);
            for (j, f) in frames.iter().enumerate().take(end).skip(start) {
                // The first record of the call is always delivered; everything
                // after it stops at the ceiling. See MAX_RENDERED_BYTES.
                if delivered > 0 && out.len() >= cap {
                    full = true;
                    break;
                }
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
                delivered += 1;
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
        highs.push(em.high);
        starts.push(em.log_start);
    }
    out.push_str("]}");

    Some(Probe {
        body: out,
        bytes,
        any_error,
        highs,
        starts,
    })
}

#[cfg(test)]
#[path = "../tests_unit/fetch_render.rs"]
mod fetch_render_tests;
