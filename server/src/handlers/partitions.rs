//! `POST /api/v1/partitions/changed` — PLAN_S3_SINK.md §5.1.
//!
//! "Which partitions do these queues have, and which of them have been written
//! to since T?" It is the discovery half of what `POST /api/v1/fetch` is the
//! read half of, and it exists because **nothing else in the HTTP surface lists
//! partition NAMES** (the plan's F6): `GET /api/v1/resources/queues` answers a
//! per-queue count, and the Kafka facade only gets away with that because its
//! partitions are named `"0".."N-1"`. A queue partitioned by entity — "one
//! entity, one ordered partition", 10k-1M lazily-created lanes — is
//! undiscoverable over HTTP without this route, so a sink that mirrors a whole
//! queue into an object store cannot learn what to fetch.
//!
//! Like the fetch it feeds, it is deliberately NOT a pop: no lease, no cursor,
//! no claim, nothing written. It reads exactly two tables
//! (`queen.queues`, `queen.log_partitions`) plus `pg_stat_activity`, and writes
//! nothing. The SQL is VOLATILE: a snapshot-ordering requirement, not a write;
//! see the header of 033_log_partitions_changed.sql.
//!
//! ## Tenancy
//!
//! Exactly `handle_fetch`'s story and by the same mechanism: the tenant comes
//! from `Extension<Tenant>` — the middleware that reads the trusted header,
//! never a body field — and is bound into the SQL, which resolves each queue
//! through `queen.queues` on `(name, tenant_id)`. A queue belonging to another
//! tenant resolves to nothing and takes the SAME
//! `UNKNOWN_TOPIC_OR_PARTITION` arm as a queue that exists nowhere; the two
//! answers are byte-identical, so this route cannot be used to probe another
//! tenant's queue namespace. Nothing here is addressed by raw uuid, so name
//! resolution IS the ownership gate.
//!
//! ## No admission lane, on purpose
//!
//! `handle_fetch` spends the pop lane's budget because it decompresses segment
//! blobs. This route reads at most 1000 rows of one small index per entry and
//! renders them; the KV argument applies verbatim (`handlers/kv.rs`, "NO
//! arbiter lane, for reads OR writes"): burning `Lane::Pop` slots on a
//! discovery sweep would starve the message path with work the weigher cannot
//! tell apart from a delivery. **The pool is the backpressure**, and the
//! statement timeout is the ceiling on how long one call may hold a connection.
//!
//! ## `safeTime` is answered even for an empty batch
//!
//! The SQL is called even when `entries` is empty, so `safeTime` is always in
//! the answer. That is not a formality: a sink whose queues are all idle still
//! needs the watermark to close and commit the window it is already holding,
//! and an empty batch is exactly how it asks for one. The cost is one indexed
//! call per poll and one scan of `pg_stat_activity`; the alternative would be a
//! second route that answers only the watermark.

use std::sync::{Arc, OnceLock};

use axum::body::Bytes;
use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::Response;
use serde::Deserialize;

use super::{json, json_err, AppState};
use crate::db;
use crate::tenant::Tenant;

// ---------------------------------------------------------------------------
// EDGE CEILINGS.

/// Max `entries` in one request, and the one ceiling that REJECTS rather than
/// clamps — the same rule and the same reason as `handle_fetch`'s
/// `MAX_ENTRIES`: silently dropping entries would leave the caller waiting for
/// queues the broker never looked at.
///
/// 64 rather than fetch's 1024 because an entry here is a QUEUE, not a
/// partition. A sink instance owns a handful of queues (PLAN_S3_SINK.md §6.2);
/// 64 is already far past any real configuration, and each entry can cost 1000
/// rows.
const MAX_ENTRIES: usize = 64;

/// Per-entry `limit` default and ceiling, in partitions. Clamped, never
/// rejected: a caller that asks for more gets what the broker serves and learns
/// the real bound from `next` being non-null.
///
/// The whole-call worst case is therefore 64 × 1000 = 64 000 partition objects,
/// ~6 MB rendered at realistic name lengths. That is one tenth of the 64 MiB
/// `handle_fetch` already permits itself, and it is the reason there is no
/// separate byte ceiling here: the SQL can count rows, not rendered bytes, and
/// a bound in the currency it can count is the one that can be enforced where
/// the read happens. (PLAN_S3_SINK.md §5.1 estimates "≤ 4 MiB"; the row bound
/// above is the mechanism, the byte figure was the estimate.)
const DEFAULT_LIMIT: i32 = 1000;
const MAX_LIMIT: i32 = 1000;

/// Seconds of slack subtracted from `now()` before `safeTime` is answered,
/// covering stats-propagation skew between backends: a session that opened a
/// transaction microseconds ago may not be in this backend's snapshot of
/// `pg_stat_activity` yet (PLAN_S3_SINK.md §5.2).
///
/// A constant and not a knob: it is a property of PostgreSQL's stats machinery,
/// not of a deployment, and a cell that got it wrong would be unsound in a way
/// no client could detect.
const GUARD_SECS: i32 = 5;

/// Per-entry error markers. Produced by `033_log_partitions_changed.sql` and
/// travelling through this handler untouched — nothing here composes one. They
/// are named so the agreement between the SQL that WRITES them and the client
/// type that READS them is pinned by a test rather than by three files
/// happening to spell the same thing (`tests_unit/partitions_changed.rs`).
///
/// `allow(dead_code)` because that pin is the ONLY reader, which is the
/// property being pinned — the same precedent `handlers/fetch.rs` states for
/// its own two markers, and stated because CI compiles with `-D warnings`.
#[allow(dead_code)]
const ERR_UNKNOWN: &str = "UNKNOWN_TOPIC_OR_PARTITION";
#[allow(dead_code)]
const ERR_BAD_CURSOR: &str = "BAD_CURSOR";

/// The degraded `safeTime` floor, in seconds: when `pg_stat_activity` is masked
/// across roles the honest watermark is unknowable, so the SQL answers
/// `now() - floor` and flags `safeTimeDegraded` (PLAN_S3_SINK.md §5.2, F14).
///
/// Read from `QUEEN_FETCH_SAFE_FLOOR_MS` through a `OnceLock` exactly like
/// `kv.rs`'s `max_value_bytes()`, and for the same seam reason stated there:
/// the HTTP broker and `Config` read the same variable so they cannot disagree,
/// and the moment they could is the moment this moves onto `AppState`.
///
/// Milliseconds in the environment (it is a sibling of the other `_MS` knobs),
/// seconds on the wire to the SQL, and the conversion rounds **up**: a floor
/// shorter than configured is the unsafe direction — it must exceed any write
/// statement's timeout, or a segment could commit below a watermark already
/// declared settled.
fn safe_floor_secs() -> i32 {
    static V: OnceLock<i32> = OnceLock::new();
    *V.get_or_init(|| {
        let ms = std::env::var("QUEEN_FETCH_SAFE_FLOOR_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(30_000);
        secs_ceil(ms)
    })
}

/// Milliseconds to whole seconds, rounding up, saturating at `i32::MAX`.
fn secs_ceil(ms: u64) -> i32 {
    ms.div_ceil(1000).min(i32::MAX as u64) as i32
}

// ---------------------------------------------------------------------------
// Request

#[derive(Deserialize)]
struct ChangedEntry {
    queue: String,
    /// Absent or `null` = FULL ENUMERATION of the queue's partitions, ordered
    /// by name. Present = only partitions whose `last_write_at` is at or after
    /// it, ordered by `(last_write_at, name)`.
    ///
    /// Kept as a `String` and handed to PostgreSQL to parse (bound as `text[]`
    /// and cast to `timestamptz[]` in the statement), so the accepted spellings
    /// are exactly the ones every other timestamp on this wire accepts, defined
    /// in one place rather than in a parser here that could drift from it. A
    /// value PostgreSQL rejects comes back as SQLSTATE 22007/22008 and is
    /// answered `400` below.
    #[serde(default)]
    since: Option<String>,
    /// The opaque cursor from a previous answer's `next`, echoed back
    /// unmodified. Absent, `null` or `""` = start from the beginning.
    ///
    /// It is mode-tagged by the SQL, so a cursor from an enumeration sweep sent
    /// with a `since` (or the reverse) is a per-entry `BAD_CURSOR` rather than
    /// a silently wrong page. Do not parse it: its shape is the broker's.
    #[serde(default)]
    after: Option<String>,
    /// Partitions to return for this entry. Absent = 1000; clamped to 1..1000.
    #[serde(default)]
    limit: Option<i64>,
}

#[derive(Deserialize)]
struct ChangedBody {
    #[serde(default)]
    entries: Vec<ChangedEntry>,
}

/// The four index-aligned arrays the SQL takes, built once.
#[cfg_attr(test, derive(Debug))]
struct Bound {
    queues: Vec<String>,
    since: Vec<Option<String>>,
    after: Vec<Option<String>>,
    limits: Vec<i32>,
}

/// Validate the batch and transpose it into the SQL's arrays.
///
/// `Err` is the message of a `400`. Pure and free of the pool, so the ceilings
/// and the clamps are testable without a database
/// (`tests_unit/partitions_changed.rs`).
fn bind(entries: &[ChangedEntry]) -> Result<Bound, String> {
    if entries.len() > MAX_ENTRIES {
        return Err(format!(
            "{} entries exceeds the {MAX_ENTRIES}-entry limit",
            entries.len()
        ));
    }
    if let Some(i) = entries.iter().position(|e| e.queue.is_empty()) {
        // Rejected rather than passed through: an empty queue name can never
        // resolve, so it would come back UNKNOWN_TOPIC_OR_PARTITION and read
        // to the caller as "this queue was deleted" rather than "you sent an
        // empty string".
        return Err(format!("entry {i} has an empty queue name"));
    }
    let n = entries.len();
    let mut b = Bound {
        queues: Vec::with_capacity(n),
        since: Vec::with_capacity(n),
        after: Vec::with_capacity(n),
        limits: Vec::with_capacity(n),
    };
    for e in entries {
        b.queues.push(e.queue.clone());
        b.since.push(e.since.clone());
        b.after.push(e.after.clone());
        b.limits.push(
            e.limit
                .unwrap_or(DEFAULT_LIMIT as i64)
                .clamp(1, MAX_LIMIT as i64) as i32,
        );
    }
    Ok(b)
}

// ---------------------------------------------------------------------------

pub async fn handle_partitions_changed(
    State(st): State<Arc<AppState>>,
    Extension(tenant): Extension<Tenant>,
    body: Bytes,
) -> Response {
    let parsed: ChangedBody = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", e)),
    };
    let bound = match bind(&parsed.entries) {
        Ok(b) => b,
        Err(msg) => return json(StatusCode::BAD_REQUEST, json_err("bad body: ", msg)),
    };

    // NO admission permit — see the module header. The connection is acquired,
    // used and released inside this function.
    let client = match st.pool.get().await {
        Ok(c) => c,
        Err(_) => {
            st.metrics.record_db_error();
            return json(
                StatusCode::INTERNAL_SERVER_ERROR,
                "{\"error\":\"pool\"}".to_string(),
            );
        }
    };
    let cancel_token = client.cancel_token();
    let res = tokio::time::timeout(
        st.stmt_timeout,
        db::log_partitions_changed(
            &client,
            &bound.queues,
            &bound.since,
            &bound.after,
            &bound.limits,
            GUARD_SECS,
            safe_floor_secs(),
            tenant.as_str(),
        ),
    )
    .await;

    // A timestamp PostgreSQL could not parse is the CALLER's error, so it is
    // answered 400 and does NOT count a db_error — the funnel below exists to
    // keep the "DB errors" series honest, and a malformed `since` is not a
    // fault of this database. The connection took a plain statement error and
    // is still healthy, so it goes back to the pool (`resolve_query_timeout`'s
    // own `Ok(Err(_))` rule). The message is PostgreSQL's, which names the
    // offending literal the caller itself just sent.
    if let Ok(Err(e)) = &res {
        if let Some(dbe) = e.as_db_error() {
            let code = dbe.code().code().to_string();
            if code == "22007" || code == "22008" {
                let msg = dbe.message().to_string();
                drop(client);
                return json(
                    StatusCode::BAD_REQUEST,
                    json_err("bad body: since is not a timestamp: ", msg),
                );
            }
        }
    }

    match db::resolve_query_timeout(res, client, cancel_token, "partitions_changed", &st.metrics) {
        // The SQL renders the whole answer, including the two watermark fields,
        // so this handler splices nothing and re-renders nothing: the body it
        // returns is the jsonb the function built.
        Some(txt) => json(StatusCode::OK, txt),
        None => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            "{\"error\":\"partitions changed failed\"}".to_string(),
        ),
    }
}

#[cfg(test)]
#[path = "../tests_unit/partitions_changed.rs"]
mod partitions_changed_tests;
