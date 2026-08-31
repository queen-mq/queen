//! `POST /api/v1/fetch` — batched read-from-offset (PLAN_QUEEN_KAFKA.md C2).
//!
//! The non-destructive way to consume. A fetch takes no lease, moves no cursor
//! and claims nothing, so two callers reading the same offsets get the same
//! records and neither disturbs a consumer group working the same partitions.
//! The position is the caller's to keep — which is the whole trade against
//! [`crate::pop`], where the broker keeps it.
//!
//! One request carries up to 1024 [`FetchEntry`] triples of
//! (queue, partition, offset) and comes back with one [`FetchEntryResult`] per
//! entry **in request order**. Every result carries
//! [`FetchEntryResult::high_watermark`] and
//! [`FetchEntryResult::log_start_offset`] whether or not it carried a record,
//! so an empty fetch doubles as the bounds probe and there is no second
//! endpoint to call for "where does this partition start and end".

use serde::de::{self, Deserializer, Visitor};
use serde::{Deserialize, Serialize};

/// Accept a partition as a name **or** as a number, mirroring the broker's own
/// parser (`server/src/handlers/fetch.rs`). Kafka partitions are numbered and
/// Queen partitions are named; the mapping is "partition n is the partition
/// named n", so a facade that serializes the number it already holds must not
/// take a `400` for the whole batch on a type error. `null` reads as absent.
fn de_partition<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    struct V;
    impl<'de> Visitor<'de> for V {
        type Value = Option<String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a partition name (string), a partition number, or null")
        }
        fn visit_str<E: de::Error>(self, v: &str) -> Result<Self::Value, E> {
            Ok(Some(v.to_owned()))
        }
        fn visit_u64<E: de::Error>(self, v: u64) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }
        fn visit_i64<E: de::Error>(self, v: i64) -> Result<Self::Value, E> {
            Ok(Some(v.to_string()))
        }
        fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
            Ok(None)
        }
        fn visit_some<D2: Deserializer<'de>>(self, d: D2) -> Result<Self::Value, D2::Error> {
            d.deserialize_any(V)
        }
    }
    d.deserialize_option(V)
}

/// The offset the caller asked for is below the partition's
/// [`FetchEntryResult::log_start_offset`] — retention deleted it — or above its
/// [`FetchEntryResult::high_watermark`], i.e. the log never allocated it.
///
/// Both bounds come back **with** the marker, so a consumer resets from the
/// same response: to `log_start_offset` to re-read everything still retained,
/// to `high_watermark` to skip to the tail.
pub const ERR_OFFSET_OUT_OF_RANGE: &str = "OFFSET_OUT_OF_RANGE";

/// No such (queue, partition) **for this tenant**. The broker deliberately
/// answers the same thing for a queue that exists but belongs to somebody else,
/// so this marker is not evidence either way about another tenant's namespace.
pub const ERR_UNKNOWN_TOPIC_OR_PARTITION: &str = "UNKNOWN_TOPIC_OR_PARTITION";

/// One (queue, partition, offset) triple to read from.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchEntry {
    pub queue: String,

    /// Omit for [`crate::DEFAULT_PARTITION`] — the same default the push path
    /// applies, so a producer that omits it and a fetcher that omits it address
    /// the same lane.
    ///
    /// The broker also accepts a JSON **number** here and reads it as the
    /// decimal name (`3` → `"3"`), because a Kafka facade maps partition
    /// numbers straight onto partition names. This type always serializes the
    /// string form; the number is an inbound convenience, not two spellings the
    /// wire has to keep alive.
    #[serde(
        default,
        deserialize_with = "de_partition",
        skip_serializing_if = "Option::is_none"
    )]
    pub partition: Option<String>,

    /// The **absolute** offset to start at — the same coordinate
    /// [`crate::PushResult::offset`] reports and the broker's own
    /// `base_offset + frame index`. Records come back starting at the first
    /// available offset `>= offset`, so a retention gap is stepped over rather
    /// than stalling the read.
    ///
    /// There are no negative sentinels: `-1`/`-2` (latest/earliest) are a
    /// `400`, because the bounds are already in every response and serving a
    /// sentinel as `0` would hand a consumer the whole backlog when it asked
    /// for the tail.
    pub offset: i64,

    /// Ceiling on the **compressed** segment bytes the broker reads for this
    /// entry (not the rendered JSON, which is larger). Absent = 1 MiB; the
    /// broker clamps to its own ceiling. At least one segment always comes
    /// back, even when it exceeds this, so a partition of fat segments cannot
    /// stall a consumer that meets one.
    #[serde(rename = "maxBytes", default, skip_serializing_if = "Option::is_none")]
    pub max_bytes: Option<i64>,
}

impl FetchEntry {
    /// A read of `queue`'s default partition from `offset`.
    pub fn new(queue: impl Into<String>, offset: i64) -> Self {
        Self {
            queue: queue.into(),
            partition: None,
            offset,
            max_bytes: None,
        }
    }

    pub fn partition(mut self, partition: impl Into<String>) -> Self {
        self.partition = Some(partition.into());
        self
    }

    pub fn max_bytes(mut self, max_bytes: i64) -> Self {
        self.max_bytes = Some(max_bytes);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchRequest {
    pub entries: Vec<FetchEntry>,

    /// How long the broker may park when nothing is available, milliseconds.
    /// Absent or `0` = answer immediately; the broker clamps to 30 000.
    ///
    /// The park ends early on the first record, and **immediately** on any
    /// per-entry error: a caller whose offset fell off the log learns it now
    /// rather than `max_wait_ms` later.
    #[serde(rename = "maxWaitMs", default, skip_serializing_if = "Option::is_none")]
    pub max_wait_ms: Option<u64>,

    /// Bytes of record payload that must accumulate before a parked poll
    /// returns. Absent = `1`, i.e. "return as soon as anything is available";
    /// `0` = never park, whatever `max_wait_ms` says.
    ///
    /// A record with an empty payload counts as one byte, so `1` means *any
    /// record* — a run of `null` payloads cannot keep a caller parked.
    #[serde(rename = "minBytes", default, skip_serializing_if = "Option::is_none")]
    pub min_bytes: Option<i64>,
}

impl FetchRequest {
    /// An immediate (non-parking) read of `entries`.
    pub fn new(entries: Vec<FetchEntry>) -> Self {
        Self {
            entries,
            max_wait_ms: None,
            min_bytes: None,
        }
    }

    /// Park for up to `ms` waiting for at least one record.
    pub fn long_poll(mut self, ms: u64) -> Self {
        self.max_wait_ms = Some(ms);
        self
    }

    pub fn min_bytes(mut self, min_bytes: i64) -> Self {
        self.min_bytes = Some(min_bytes);
        self
    }
}

/// One record, at its absolute offset.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchRecord {
    /// Absolute offset within the partition. Contiguous within a response
    /// except across a retention gap, and the coordinate to resume from: the
    /// next fetch asks for `records.last().offset + 1`.
    pub offset: i64,

    /// The message's idempotency key — the `transactionId` it was pushed with,
    /// or the broker-minted message id when the push carried none. It is the
    /// identity `GET /api/v1/messages/:partitionId/:transactionId` is keyed by.
    #[serde(rename = "transactionId")]
    pub transaction_id: String,

    /// The payload as pushed, spliced verbatim. `null` when the stored payload
    /// was empty.
    pub payload: serde_json::Value,

    /// ISO-8601 timestamp of the *segment* this record was stored in. Every
    /// record written by one push call shares it, so it is a commit time and
    /// not a per-message one.
    pub ts: String,
}

/// One entry's answer, positionally matching the request's `entries`.
///
/// There is deliberately no `headers`: a stored frame carries no header map, so
/// a Kafka facade round-trips record headers through the payload envelope or
/// not at all. An always-empty key would advertise a feature the engine does
/// not have.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchEntryResult {
    pub queue: String,

    /// Echoed back resolved — the request's value, or `"Default"` when it
    /// omitted one.
    pub partition: String,

    /// Empty on a caught-up entry, on an entry whose byte budget was spent by
    /// an earlier one, and on every error.
    #[serde(default)]
    pub records: Vec<FetchRecord>,

    /// The next offset the partition will allocate — i.e. one past the last
    /// stored record. A caller that is caught up has asked for exactly this.
    #[serde(rename = "highWatermark")]
    pub high_watermark: i64,

    /// The oldest offset still retained. Everything below it has been deleted
    /// and can never be read again.
    #[serde(rename = "logStartOffset")]
    pub log_start_offset: i64,

    /// Absent when the entry is healthy. Modelled as a `String` and not an enum
    /// on purpose: a marker a newer broker adds must not fail the decode of a
    /// response whose other entries are perfectly readable. Compare against
    /// [`ERR_OFFSET_OUT_OF_RANGE`] / [`ERR_UNKNOWN_TOPIC_OR_PARTITION`], or use
    /// the helpers below.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl FetchEntryResult {
    /// Whether this entry carried no error, i.e. its records and both
    /// watermarks describe a partition the caller may read.
    pub fn is_ok(&self) -> bool {
        self.error.is_none()
    }

    /// The requested offset is outside `[log_start_offset, high_watermark]`.
    /// Both bounds are still populated, so the reset target is in hand.
    pub fn is_offset_out_of_range(&self) -> bool {
        self.error.as_deref() == Some(ERR_OFFSET_OUT_OF_RANGE)
    }

    /// No such (queue, partition) for this tenant.
    pub fn is_unknown_partition(&self) -> bool {
        self.error.as_deref() == Some(ERR_UNKNOWN_TOPIC_OR_PARTITION)
    }

    /// Where a caller that consumed every returned record should ask next:
    /// one past the last record, or the offset it already asked for when the
    /// entry came back empty (which the caller still holds).
    pub fn next_offset(&self) -> Option<i64> {
        self.records.last().map(|r| r.offset + 1)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FetchResponse {
    #[serde(default)]
    pub entries: Vec<FetchEntryResult>,
}

impl FetchResponse {
    /// Total records across every entry.
    pub fn record_count(&self) -> usize {
        self.entries.iter().map(|e| e.records.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fetch body byte for byte as `render_fetch` builds it
    /// (`server/src/handlers/fetch.rs`): the per-entry key order is
    /// queue, partition, records, highWatermark, logStartOffset, and `error`
    /// only when there is one; the per-record order is offset, transactionId,
    /// payload, ts.
    ///
    /// Three entries covering everything one response can hold at once: a
    /// partition delivering records across two segments, a caught-up partition
    /// (offset == highWatermark, which is the entry a long poll parks on), and
    /// a partition whose caller fell off the retention watermark.
    const FETCH_BODY_FROM_THE_RENDERER: &str = concat!(
        r#"{"entries":[{"queue":"orders","partition":"eu","records":["#,
        r#"{"offset":41,"transactionId":"order-1","payload":{"total":19.99},"#,
        r#""ts":"2026-08-28T09:15:00.123456Z"},"#,
        r#"{"offset":42,"transactionId":"order-2","payload":null,"#,
        r#""ts":"2026-08-28T09:15:00.123456Z"},"#,
        r#"{"offset":43,"transactionId":"order-3","payload":[1,2,3],"#,
        r#""ts":"2026-08-28T09:15:01.000000Z"}"#,
        r#"],"highWatermark":44,"logStartOffset":12},"#,
        r#"{"queue":"orders","partition":"us","records":[],"#,
        r#""highWatermark":7,"logStartOffset":0},"#,
        r#"{"queue":"orders","partition":"ap","records":[],"#,
        r#""highWatermark":900,"logStartOffset":800,"error":"OFFSET_OUT_OF_RANGE"}]}"#,
    );

    #[test]
    fn a_rendered_fetch_body_parses_with_every_field_populated() {
        let got: FetchResponse = serde_json::from_str(FETCH_BODY_FROM_THE_RENDERER)
            .expect("the body the broker renders for every fetch must deserialize");
        assert_eq!(got.entries.len(), 3);
        assert_eq!(got.record_count(), 3);

        let eu = &got.entries[0];
        assert!(eu.is_ok());
        assert_eq!(eu.partition, "eu");
        assert_eq!(eu.high_watermark, 44);
        assert_eq!(eu.log_start_offset, 12);
        // Offsets are the contract: contiguous, absolute, and one past the last
        // is where the caller resumes.
        assert_eq!(
            eu.records.iter().map(|r| r.offset).collect::<Vec<_>>(),
            vec![41, 42, 43]
        );
        assert_eq!(eu.next_offset(), Some(44));
        // The payload is spliced verbatim, so every JSON shape a producer can
        // push has to survive — object, null and array all appear in one real
        // response.
        assert_eq!(eu.records[0].payload, serde_json::json!({"total": 19.99}));
        assert_eq!(
            eu.records[1].payload,
            serde_json::Value::Null,
            "an empty stored payload renders as `null`, not as an absent key"
        );
        assert_eq!(eu.records[2].payload, serde_json::json!([1, 2, 3]));
        assert_eq!(eu.records[0].transaction_id, "order-1");
        assert_eq!(eu.records[0].ts, "2026-08-28T09:15:00.123456Z");
    }

    #[test]
    fn a_caught_up_entry_is_empty_but_still_says_where_the_log_is() {
        // This is the entry a long poll parks on AND the ListOffsets probe:
        // no records, no error, both bounds present.
        let got: FetchResponse = serde_json::from_str(FETCH_BODY_FROM_THE_RENDERER).unwrap();
        let us = &got.entries[1];
        assert!(us.is_ok());
        assert!(us.records.is_empty());
        assert_eq!(us.high_watermark, 7);
        assert_eq!(us.log_start_offset, 0);
        assert_eq!(
            us.next_offset(),
            None,
            "nothing came back, so the caller keeps the offset it already had"
        );
    }

    #[test]
    fn an_out_of_range_entry_carries_both_reset_targets() {
        let got: FetchResponse = serde_json::from_str(FETCH_BODY_FROM_THE_RENDERER).unwrap();
        let ap = &got.entries[2];
        assert!(!ap.is_ok());
        assert!(ap.is_offset_out_of_range());
        assert!(!ap.is_unknown_partition());
        // The whole point of returning the bounds with the error: reset to
        // earliest or to latest without a second round trip.
        assert_eq!(ap.log_start_offset, 800);
        assert_eq!(ap.high_watermark, 900);
    }

    #[test]
    fn an_unknown_partition_reads_as_its_own_marker() {
        // The broker answers this for a queue that does not exist AND for one
        // that belongs to another tenant — the two are indistinguishable by
        // design, and this pins that the client models it as one condition.
        let wire = r#"{"entries":[{"queue":"ghost","partition":"Default","records":[],"highWatermark":0,"logStartOffset":0,"error":"UNKNOWN_TOPIC_OR_PARTITION"}]}"#;
        let got: FetchResponse = serde_json::from_str(wire).unwrap();
        assert!(got.entries[0].is_unknown_partition());
        assert!(!got.entries[0].is_offset_out_of_range());
        assert_eq!(serde_json::to_string(&got).unwrap(), wire, "round-trips");
    }

    #[test]
    fn a_fetch_response_from_a_newer_broker_still_parses() {
        // The same rule the push response follows: an unmodelled key must never
        // cost a caller the records it already fetched, and an unmodelled ERROR
        // marker must not fail the decode of the entries around it.
        let wire = r#"{"entries":[{"queue":"q","partition":"p","records":[{"offset":1,"transactionId":"t","payload":1,"ts":"2026-08-28T09:15:00.000000Z","headers":{"k":"v"}}],"highWatermark":2,"logStartOffset":0,"leaderEpoch":7},{"queue":"q","partition":"z","records":[],"highWatermark":0,"logStartOffset":0,"error":"SOMETHING_NEW"}]}"#;
        let got: FetchResponse =
            serde_json::from_str(wire).expect("an unmodelled key must not fail the decode");
        assert_eq!(got.entries[0].records[0].offset, 1);
        assert!(!got.entries[1].is_ok());
        assert!(
            !got.entries[1].is_offset_out_of_range() && !got.entries[1].is_unknown_partition(),
            "an unknown marker is an error the client cannot classify, not a known one"
        );
    }

    #[test]
    fn a_request_omits_every_optional_it_did_not_set() {
        // The broker applies its own defaults for an absent key, so sending
        // `null` would be a different request than sending nothing.
        let req = FetchRequest::new(vec![FetchEntry::new("orders", 0)]);
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"entries":[{"queue":"orders","offset":0}]}"#
        );

        let req = FetchRequest::new(vec![FetchEntry::new("orders", 41)
            .partition("eu")
            .max_bytes(1_048_576)])
        .long_poll(5_000)
        .min_bytes(1);
        assert_eq!(
            serde_json::to_string(&req).unwrap(),
            r#"{"entries":[{"queue":"orders","partition":"eu","offset":41,"maxBytes":1048576}],"maxWaitMs":5000,"minBytes":1}"#
        );
    }

    #[test]
    fn a_partition_number_is_accepted_on_the_wire_and_normalized_to_a_name() {
        // The Kafka mapping is "partition n = the partition named n", and a
        // facade holding the number must not have to quote it. The struct
        // always writes the string form back.
        let req: FetchRequest =
            serde_json::from_str(r#"{"entries":[{"queue":"q","partition":3,"offset":0}]}"#)
                .expect("a numeric partition must parse");
        assert_eq!(req.entries[0].partition.as_deref(), Some("3"));
    }
}
