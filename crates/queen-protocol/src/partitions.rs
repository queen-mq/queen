//! `POST /api/v1/partitions/changed` — partition discovery.
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
//! ## One order, one cursor
//!
//! A queue's partitions are listed in **creation order**,
//! [`ChangedEntry::limit`] at a time, paged by the opaque
//! [`ChangedResult::next`] cursor. A **pass** is every page of one entry, from
//! no cursor until `next` comes back `null`.
//!
//! * [`ChangedEntry::since`] absent — every partition of the queue: the
//!   cold-start sweep.
//! * [`ChangedEntry::since`] present — only the partitions whose `lastWriteAt`
//!   is at or after it, in the same order: the steady-state sweep.
//!
//! **What it costs.** An answer is O(partitions it lists). The work behind it
//! is one walk over the queue's partitions per COMPLETE pass: each page resumes
//! where the previous one stopped and ends at its last match, so a pass over a
//! queue of P partitions reads P partition rows whatever the page size — and a
//! `since` pass reads all P to find the ones that moved. (1.x served `since`
//! from an index on `lastWriteAt`, so fifty partitions that moved cost fifty
//! rows; the 2.0 broker keeps no such index, and a steady-state pass costs a
//! scan of the queue's partitions in memory.)
//!
//! ## What a pass guarantees
//!
//! A partition is listed at most once per pass. One created during a pass
//! sorts after every partition that existed when it started, so the pass still
//! meets it unless it had already ended. One written to after its page was
//! served is not listed again by that pass, and does not need to be: its new
//! records are stamped above that pass's `safeTime`, so the next pass, with
//! `since` at that `safeTime`, lists it.
//!
//! ## `safeTime`
//!
//! [`ChangedResponse::safe_time`] is the greatest record stamp the answering
//! node has applied. Every record stamped at or below it is already counted in
//! the bounds of the answer that carried it (or of any later one from that
//! node) and readable by a fetch served by that node — so a time window that
//! ends at or below it is complete there, and reading it again yields the same
//! records. Over a pass, take the minimum `safeTime` of its pages: one node
//! answers them in non-decreasing order, several nodes need not.
//!
//! Re-arm `since` from data, never from a local clock: the natural next value
//! is the `safeTime` of the pass just finished, never `SystemTime::now()`.
//! Nothing here is stamped by the caller's clock, and comparing the two is
//! wrong by construction. `safeTime` is the broker's clock only as far as the
//! node has applied writes: while nothing is written it does not move.

use serde::{Deserialize, Serialize};

/// No such queue **for this tenant**. The broker answers exactly this for a
/// queue that belongs to somebody else, byte for byte, so the marker is not
/// evidence either way about another tenant's namespace — the same rule
/// [`crate::fetch::ERR_UNKNOWN_TOPIC_OR_PARTITION`] states.
pub const ERR_UNKNOWN_TOPIC_OR_PARTITION: &str = "UNKNOWN_TOPIC_OR_PARTITION";

/// The `after` cursor is not one the broker issued: a string that was never a
/// cursor, or a cursor of a 1.x broker (whose cursors the 2.0 one does not
/// read).
///
/// It is an error rather than a quiet restart because the quiet version loops a
/// paging caller for ever on its own first page. Recover by dropping the cursor
/// and starting the pass again.
pub const ERR_BAD_CURSOR: &str = "BAD_CURSOR";

/// One queue to ask about.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChangedEntry {
    pub queue: String,

    /// Only partitions whose `lastWriteAt` is **at or after** this instant, in
    /// the same ISO-8601 spelling every other timestamp on this wire uses
    /// (`2026-09-04T10:00:00.000000Z`), read to the microsecond. Absent or
    /// `null` = every partition of the queue.
    ///
    /// The broker reads `YYYY-MM-DD`, optionally followed by `THH:MM`, `:SS`
    /// and a fraction, then `Z` or a `±HH:MM` offset; a value with no
    /// designator reads as UTC. A value it cannot parse refuses the whole
    /// request with a `400`, as a 1.x broker did: answering it as "absent"
    /// would turn an incremental pass into a full listing without a word.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub since: Option<String>,

    /// The [`ChangedResult::next`] of the previous page, echoed back
    /// unmodified. Absent, `null` or `""` = start the pass from the beginning.
    ///
    /// **Opaque.** It names a position in the queue's creation order and means
    /// the same with or without `since`. Anything the broker did not issue is
    /// [`ERR_BAD_CURSOR`]. Nothing about its shape is contract; do not parse,
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

    /// Continue a pass from a previous answer's [`ChangedResult::next`].
    pub fn after(mut self, after: impl Into<String>) -> Self {
        self.after = Some(after.into());
        self
    }

    pub fn limit(mut self, limit: i64) -> Self {
        self.limit = Some(limit);
        self
    }
}

/// A batch of up to **1024** entries (a 1.x broker took 64). Above that the
/// whole request is a `400`: dropping entries silently would leave a caller
/// waiting for queues the broker never looked at.
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

    /// The partition's id (a uuid), the same for every answer about it for its
    /// whole life. A partition deleted and created again under the same name
    /// has a new id — which is how a reader tells the two apart. Absent from a
    /// 1.x broker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,

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
    /// precision, always UTC (`2026-09-04T10:00:01.000000Z`): the `ts` of its
    /// newest record, on the clock of [`ChangedResponse::safe_time`].
    ///
    /// It only moves forward, and every write moves it: a record written
    /// after a pass therefore puts its partition in any later pass whose
    /// `since` is at or below that record's `ts`.
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
    /// the pass. Non-null means the page FILLED and there may be more; a
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

    /// The cursor sent is not one the broker issued. Drop it and restart.
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
    /// ISO-8601, microseconds, UTC — the same spelling and the same clock as
    /// [`crate::fetch::FetchRecord::ts`]. The greatest record stamp the node
    /// that answered has applied, read before anything else the answer holds:
    /// no record stamped at or below it can still become visible on that node
    /// (see the module header). It moves only when that node applies a write.
    /// **Never compare it to a local `SystemTime`**.
    #[serde(rename = "safeTime")]
    pub safe_time: String,

    /// Always `false` from the 2.0 broker, whose `safeTime` is exact. A 1.x
    /// broker set it when it fell back to a fixed floor instead of deriving
    /// `safeTime` from its open transactions.
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

    /// A response byte for byte as a 1.x broker emitted one.
    ///
    /// Its rendering orders keys by (length, then bytewise) rather than by
    /// source order, with `", "` / `": "` separators. None of that is contract —
    /// no caller may depend on key order — but this literal is transcribed from
    /// a real answer so the types are exercised against a real rendering.
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
    fn a_1x_response_has_no_partition_id() {
        let got: ChangedResponse = serde_json::from_str(A_REAL_RESPONSE).unwrap();
        assert!(got
            .entries
            .iter()
            .flat_map(|e| &e.partitions)
            .all(|p| p.id.is_none()));
    }

    /// A response as the 2.0 broker renders one: `id` on every partition, a
    /// cursor of its own shape.
    const A_2_0_RESPONSE: &str = concat!(
        r#"{"entries":[{"next":"p|1027","partitions":[{"id":"0199a6f2-5c1e-7b3a-9d4e-2f6a8c0b1d3e","#,
        r#""lastOffset":10,"lastWriteAt":"2026-10-02T10:00:01.000000Z","logStart":1,"name":"cust-0001"}],"#,
        r#""queue":"orders"},{"error":"UNKNOWN_TOPIC_OR_PARTITION","queue":"ghost"}],"#,
        r#""safeTime":"2026-10-02T10:00:01.000003Z","safeTimeDegraded":false}"#,
    );

    #[test]
    fn a_2_0_response_carries_each_partitions_id() {
        let got: ChangedResponse = serde_json::from_str(A_2_0_RESPONSE).unwrap();
        let p = &got.entries[0].partitions[0];
        assert_eq!(
            p.id.as_deref(),
            Some("0199a6f2-5c1e-7b3a-9d4e-2f6a8c0b1d3e")
        );
        assert_eq!(p.name, "cust-0001");
        assert_eq!(got.entries[0].next.as_deref(), Some("p|1027"));
        assert!(got.entries[1].is_unknown_queue());
        assert!(!got.safe_time_degraded);
    }

    #[test]
    fn a_partition_renders_its_id_only_when_it_has_one() {
        let mut p = ChangedPartition {
            name: "eu".into(),
            id: None,
            last_offset: -1,
            log_start: 0,
            last_write_at: "2026-10-02T10:00:00.000000Z".into(),
        };
        assert_eq!(
            serde_json::to_string(&p).unwrap(),
            r#"{"name":"eu","lastOffset":-1,"logStart":0,"lastWriteAt":"2026-10-02T10:00:00.000000Z"}"#
        );
        p.id = Some("0199a6f2-5c1e-7b3a-9d4e-2f6a8c0b1d3e".into());
        let wire = serde_json::to_string(&p).unwrap();
        assert!(
            wire.contains(r#""id":"0199a6f2-5c1e-7b3a-9d4e-2f6a8c0b1d3e""#),
            "{wire}"
        );
        assert_eq!(serde_json::from_str::<ChangedPartition>(&wire).unwrap(), p);
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
        // The degraded arm (a 1.x broker's fixed-floor fallback): still a 200,
        // still a usable watermark, with one boolean saying how it was derived.
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
            .after("p|1027")
            .limit(500)]);
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"entries":[{"queue":"orders","since":"2026-09-04T10:00:00.000000Z","after":"p|1027","limit":500}]}"#
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
