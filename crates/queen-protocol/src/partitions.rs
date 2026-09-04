//! `POST /api/v1/partitions/changed` — partition discovery (PLAN_S3_SINK.md §5.1).
//!
//! The only route that lists partition **names**. `GET /api/v1/resources/queues`
//! answers a per-queue count, which is enough for a Kafka-shaped queue whose
//! partitions are named `"0".."N-1"` and useless for the model Queen is built
//! for — "one entity, one ordered partition", 10k-1M lazily-created lanes with
//! names nobody can enumerate from outside. A reader that wants to mirror a
//! whole queue starts here and then reads with [`crate::fetch`].
//!
//! Like the fetch it feeds it is read-only: no lease, no cursor, no claim.
//! Two callers asking the same question get the same answer and neither
//! disturbs a consumer group.
//!
//! ## Two modes, one route
//!
//! * [`ChangedEntry::since`] absent — **full enumeration**, ordered by name.
//!   The cold-start sweep: walk the whole partition set of a queue,
//!   [`ChangedEntry::limit`] at a time.
//! * [`ChangedEntry::since`] present — **what moved**, ordered by
//!   `(lastWriteAt, name)`. The steady-state sweep: the fifty lanes that moved
//!   cost fifty rows, whatever the partition count is.
//!
//! Both page through the opaque [`ChangedResult::next`] cursor.
//!
//! ## `lastWriteAt` is quantized, and what that costs
//!
//! The broker updates a partition's `lastWriteAt` at most once per second (it
//! keeps the allocator's row update HOT), and it only ever moves **forward**.
//! So a partition written to during a paged sweep can appear on a LATER page as
//! well — seen twice, never missed. A caller must therefore be idempotent in
//! what it does per partition, and may never treat "already seen this sweep" as
//! a reason to skip. That asymmetry is deliberate: seen twice costs a re-read
//! of bounds the caller already had, missed would be silent data loss.
//!
//! It also means `since` should be re-armed from data, not from a local clock:
//! the natural next value is the largest `lastWriteAt` of the sweep just
//! finished (or [`ChangedResponse::safe_time`]), never `SystemTime::now()`.
//! Nothing here is stamped by the caller's clock and comparing the two is wrong
//! by construction.

use serde::{Deserialize, Serialize};

/// No such queue **for this tenant**. The broker answers exactly this for a
/// queue that belongs to somebody else, byte for byte, so the marker is not
/// evidence either way about another tenant's namespace — the same rule
/// [`crate::fetch::ERR_UNKNOWN_TOPIC_OR_PARTITION`] states.
pub const ERR_UNKNOWN_TOPIC_OR_PARTITION: &str = "UNKNOWN_TOPIC_OR_PARTITION";

/// The `after` cursor does not belong to the sweep it was sent with: an
/// enumeration cursor replayed against a `since` request, or the reverse, or a
/// string that was never a cursor at all.
///
/// It is an error rather than a quiet restart because the quiet version loops a
/// paging caller for ever on its own first page. Recover by dropping the cursor
/// and starting the sweep again.
pub const ERR_BAD_CURSOR: &str = "BAD_CURSOR";

/// One queue to ask about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedEntry {
    pub queue: String,

    /// Only partitions whose `lastWriteAt` is **at or after** this instant, in
    /// the same ISO-8601 spelling every other timestamp on this wire uses
    /// (`2026-09-04T10:00:00.000000Z`). Absent = enumerate every partition of
    /// the queue instead.
    ///
    /// PostgreSQL parses it, so anything it accepts as a `timestamptz` is
    /// accepted here; anything else is a `400` for the whole request naming the
    /// offending literal. A value without a zone offset is read in the
    /// database's session timezone — always send the `Z` form.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,

    /// The [`ChangedResult::next`] of the previous page, echoed back
    /// unmodified. Absent, `null` or `""` = start from the beginning.
    ///
    /// **Opaque.** It encodes the sweep's mode as well as its position, and the
    /// broker rejects one that does not match the request it arrives with
    /// ([`ERR_BAD_CURSOR`]). Nothing about its shape is contract; do not parse,
    /// construct or compare it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub after: Option<String>,

    /// Partitions to return for this entry. Absent = 1000; the broker clamps to
    /// 1..1000 rather than rejecting, so a caller learns the real bound from
    /// `next` being non-null.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<i64>,
}

impl ChangedEntry {
    /// Enumerate `queue`'s partitions from the beginning.
    pub fn new(queue: impl Into<String>) -> Self {
        Self {
            queue: queue.into(),
            since: None,
            after: None,
            limit: None,
        }
    }

    /// Only what has been written at or after `since`.
    pub fn since(mut self, since: impl Into<String>) -> Self {
        self.since = Some(since.into());
        self
    }

    /// Continue a sweep from a previous answer's [`ChangedResult::next`].
    pub fn after(mut self, after: impl Into<String>) -> Self {
        self.after = Some(after.into());
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// A batch of up to **64** entries. Above that the whole request is a `400`:
/// dropping entries silently would leave a caller waiting for queues the broker
/// never looked at.
///
/// An **empty** batch is legal and useful — it answers
/// [`ChangedResponse::safe_time`] and nothing else, which is how a reader whose
/// queues are all idle closes the window it is already holding.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedRequest {
    #[serde(default)]
    pub entries: Vec<ChangedEntry>,
}

impl ChangedRequest {
    pub fn new(entries: Vec<ChangedEntry>) -> Self {
        Self { entries }
    }

    /// The watermark-only request: no queues, just
    /// [`ChangedResponse::safe_time`].
    pub fn safe_time_only() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

/// One partition, with the bounds needed to start reading it without a second
/// round trip.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedPartition {
    pub name: String,

    /// The offset of the last stored record. **One less** than
    /// [`crate::fetch::FetchEntryResult::high_watermark`], which is the next
    /// offset to be allocated — a partition that has never been written has
    /// `lastOffset` `-1`.
    #[serde(rename = "lastOffset")]
    pub last_offset: i64,

    /// The oldest offset still retained; everything below it has been deleted.
    /// The same number [`crate::fetch::FetchEntryResult::log_start_offset`]
    /// reports.
    #[serde(rename = "logStart")]
    pub log_start: i64,

    /// When this partition was last written to, ISO-8601 at microsecond
    /// precision, always UTC (`2026-09-04T10:00:01.000000Z`).
    ///
    /// **Quantized to one second** by the broker and monotonically
    /// non-decreasing — see the module header for what a caller owes that.
    #[serde(rename = "lastWriteAt")]
    pub last_write_at: String,
}

/// One entry's answer, positionally matching the request's `entries`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedResult {
    pub queue: String,

    /// Empty when nothing matched. Absent entirely on an error entry, which is
    /// why it defaults.
    #[serde(default)]
    pub partitions: Vec<ChangedPartition>,

    /// The cursor for the next page, or `null` when this page was the end of
    /// the sweep. Non-null means the page FILLED and there may be more; a
    /// caller pages until it is null.
    ///
    /// Opaque — see [`ChangedEntry::after`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next: Option<String>,

    /// Absent when the entry is healthy. A `String` and not an enum on purpose:
    /// a marker a newer broker adds must not fail the decode of the entries
    /// around it. Compare against [`ERR_UNKNOWN_TOPIC_OR_PARTITION`] /
    /// [`ERR_BAD_CURSOR`], or use the helpers below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl ChangedResult {
    pub fn is_ok(&self) -> bool {
        self.error.is_none()
    }

    /// No such queue for this tenant — deleted, never created, or somebody
    /// else's. The three are indistinguishable by design.
    pub fn is_unknown_queue(&self) -> bool {
        self.error.as_deref() == Some(ERR_UNKNOWN_TOPIC_OR_PARTITION)
    }

    /// The cursor sent did not belong to this sweep. Drop it and restart.
    pub fn is_bad_cursor(&self) -> bool {
        self.error.as_deref() == Some(ERR_BAD_CURSOR)
    }

    /// Whether a further page exists for this entry.
    pub fn has_more(&self) -> bool {
        self.next.is_some()
    }
}

/// The answer, with the watermark that makes a time window a deterministic set.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedResponse {
    /// **No record that will ever be committed to this cluster can carry a `ts`
    /// at or below this instant.** ISO-8601, microseconds, UTC — the same
    /// spelling and the same clock as [`crate::fetch::FetchRecord::ts`], which
    /// is the whole point: the two are comparable, and a reader that closes a
    /// window at or below `safeTime` can re-read that window after a crash and
    /// get exactly the same records back.
    ///
    /// It is derived from the database's own clock and from the oldest
    /// in-flight transaction, so it lags `now()` by at least a few seconds and
    /// by as much as the longest open transaction. **Never compare it to a
    /// local `SystemTime`** — a reader that does is wrong by construction.
    #[serde(rename = "safeTime")]
    pub safe_time: String,

    /// `true` when the broker could not read `pg_stat_activity` across roles
    /// and fell back to a fixed floor (`now() - QUEEN_FETCH_SAFE_FLOOR_MS`,
    /// default 30 s) instead of the oldest open transaction.
    ///
    /// The value is still safe to use — the fallback is the conservative arm —
    /// but it is not derived from the transactions actually running, so a write
    /// statement that outlives the floor would not be covered by it. It is
    /// normal and permanent on a cell whose broker connects as a role without
    /// `pg_read_all_stats`; log it once and carry on, do not stall on it.
    #[serde(rename = "safeTimeDegraded")]
    pub safe_time_degraded: bool,

    #[serde(default)]
    pub entries: Vec<ChangedResult>,
}

impl ChangedResponse {
    /// Total partitions across every entry.
    pub fn partition_count(&self) -> usize {
        self.entries.iter().map(|e| e.partitions.len()).sum()
    }

    /// Whether any entry has a further page, i.e. whether the sweep is
    /// unfinished.
    pub fn has_more(&self) -> bool {
        self.entries.iter().any(ChangedResult::has_more)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A response byte for byte as the broker emits one.
    ///
    /// The body is composed by `033_log_partitions_changed.sql` as a `jsonb`
    /// and returned by the handler verbatim, so the wire carries PostgreSQL's
    /// own rendering: keys ordered by (length, then bytewise) rather than by
    /// source order, and `", "` / `": "` separators. None of that is contract —
    /// no caller may depend on key order — but this literal is transcribed from
    /// a real answer so the types are exercised against what actually arrives.
    ///
    /// Three entries covering everything one response can hold: a queue with a
    /// filled page (`next` non-null), a queue whose sweep is finished
    /// (`next: null`), and a queue this tenant does not have.
    const A_REAL_RESPONSE: &str = concat!(
        r#"{"entries": [{"next": "n|cust-0002", "queue": "orders", "partitions": ["#,
        r#"{"name": "cust-0001", "logStart": 1, "lastOffset": 10, "#,
        r#""lastWriteAt": "2026-09-04T10:00:01.000000Z"}, "#,
        r#"{"name": "cust-0002", "logStart": 2, "lastOffset": 20, "#,
        r#""lastWriteAt": "2026-09-04T10:00:02.000000Z"}]}, "#,
        r#"{"next": null, "queue": "events", "partitions": ["#,
        r#"{"name": "eu", "logStart": 0, "lastOffset": -1, "#,
        r#""lastWriteAt": "2026-09-04T09:59:00.000000Z"}]}, "#,
        r#"{"error": "UNKNOWN_TOPIC_OR_PARTITION", "queue": "ghost"}], "#,
        r#""safeTime": "2026-09-04T10:04:57.412331Z", "safeTimeDegraded": false}"#,
    );

    #[test]
    fn a_real_response_parses_with_every_field_populated() {
        let got: ChangedResponse = serde_json::from_str(A_REAL_RESPONSE)
            .expect("the body the broker renders must deserialize");
        assert_eq!(got.entries.len(), 3);
        assert_eq!(got.partition_count(), 3);
        assert_eq!(got.safe_time, "2026-09-04T10:04:57.412331Z");
        assert!(!got.safe_time_degraded);

        let orders = &got.entries[0];
        assert!(orders.is_ok());
        assert_eq!(orders.queue, "orders");
        assert_eq!(orders.next.as_deref(), Some("n|cust-0002"));
        assert!(orders.has_more());
        assert_eq!(orders.partitions[0].name, "cust-0001");
        assert_eq!(orders.partitions[0].last_offset, 10);
        assert_eq!(orders.partitions[0].log_start, 1);
        assert_eq!(
            orders.partitions[0].last_write_at,
            "2026-09-04T10:00:01.000000Z"
        );
        assert!(got.has_more());
    }

    #[test]
    fn a_finished_sweep_says_so_with_a_null_next() {
        let got: ChangedResponse = serde_json::from_str(A_REAL_RESPONSE).unwrap();
        let events = &got.entries[1];
        assert!(events.is_ok());
        assert!(!events.has_more(), "null next is the end of the sweep");
        // A partition that exists but has never been written reports the bounds
        // of an empty log: lastOffset -1, i.e. one less than the fetch's
        // highWatermark of 0.
        assert_eq!(events.partitions[0].last_offset, -1);
        assert_eq!(events.partitions[0].log_start, 0);
    }

    #[test]
    fn an_unknown_queue_carries_no_partitions_and_no_cursor() {
        let got: ChangedResponse = serde_json::from_str(A_REAL_RESPONSE).unwrap();
        let ghost = &got.entries[2];
        assert!(!ghost.is_ok());
        assert!(ghost.is_unknown_queue());
        assert!(!ghost.is_bad_cursor());
        // The error entry omits `partitions` and `next` entirely, so both must
        // default rather than fail the decode.
        assert!(ghost.partitions.is_empty());
        assert!(!ghost.has_more());
    }

    #[test]
    fn a_bad_cursor_reads_as_its_own_marker() {
        let wire = r#"{"entries": [{"error": "BAD_CURSOR", "queue": "orders"}], "safeTime": "2026-09-04T10:04:57.412331Z", "safeTimeDegraded": false}"#;
        let got: ChangedResponse = serde_json::from_str(wire).unwrap();
        assert!(got.entries[0].is_bad_cursor());
        assert!(!got.entries[0].is_unknown_queue());
    }

    #[test]
    fn the_degraded_watermark_is_a_flag_and_not_an_error() {
        // The masked-`pg_stat_activity` arm: still a 200, still a usable
        // watermark, with one boolean saying how it was derived.
        let wire = r#"{"entries": [], "safeTime": "2026-09-04T10:04:27.000000Z", "safeTimeDegraded": true}"#;
        let got: ChangedResponse = serde_json::from_str(wire).unwrap();
        assert!(got.safe_time_degraded);
        assert!(got.entries.is_empty());
        assert_eq!(got.partition_count(), 0);
        assert!(!got.has_more());
    }

    #[test]
    fn a_response_from_a_newer_broker_still_parses() {
        // The rule every type in this crate follows: an unmodelled key must not
        // cost a caller the page it already fetched, and an unmodelled error
        // marker must not fail the decode of the entries around it.
        let wire = r#"{"entries":[{"queue":"q","partitions":[{"name":"p","lastOffset":1,"logStart":0,"lastWriteAt":"2026-09-04T10:00:00.000000Z","segments":3}],"next":null,"lag":7},{"queue":"z","error":"SOMETHING_NEW"}],"safeTime":"2026-09-04T10:00:00.000000Z","safeTimeDegraded":false,"nextSweepHint":42}"#;
        let got: ChangedResponse =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert_eq!(got.entries[0].partitions[0].name, "p");
        assert!(!got.entries[1].is_ok());
        assert!(
            !got.entries[1].is_unknown_queue() && !got.entries[1].is_bad_cursor(),
            "an unknown marker is an error the caller cannot classify, not a known one"
        );
    }

    #[test]
    fn a_request_omits_every_optional_it_did_not_set() {
        // The broker applies its own defaults for an absent key, so sending
        // `null` would be a different request than sending nothing.
        let req = ChangedRequest::new(vec![ChangedEntry::new("orders")]);
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"entries":[{"queue":"orders"}]}"#
        );

        let req = ChangedRequest::new(vec![ChangedEntry::new("orders")
            .since("2026-09-04T10:00:00.000000Z")
            .after("t|1788516004000000|cust-0004")
            .limit(500)]);
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"entries":[{"queue":"orders","since":"2026-09-04T10:00:00.000000Z","after":"t|1788516004000000|cust-0004","limit":500}]}"#
        );

        // The watermark-only request is an empty array, not an absent key: the
        // broker reads `entries` with a serde default, but a body that says so
        // explicitly is what every other request on this wire looks like.
        assert_eq!(
            serde_json::to_string(&ChangedRequest::safe_time_only()).unwrap(),
            r#"{"entries":[]}"#
        );
    }
}
