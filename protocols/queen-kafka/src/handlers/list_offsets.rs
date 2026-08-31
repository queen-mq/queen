//! ListOffsets — where a consumer starts, and where it stops.
//!
//! It answers one question per partition: which offset does this timestamp
//! name? Two of the answers are sentinels and they are the ones that matter —
//! `-1` (LATEST) is the high watermark, `-2` (EARLIEST) is the log start — and
//! between them they are `auto.offset.reset`, the seek-to-beginning/end calls,
//! the end-offsets an admin tool reads to compute lag, and the recovery a
//! consumer runs after an OFFSET_OUT_OF_RANGE. A facade that gets Fetch right
//! and this wrong has a consumer that cannot begin.
//!
//! ## Any node answers, in cluster mode as in single
//!
//! No leadership gate ([`crate::cluster`]): both numbers are bounds read out of
//! `queen.log_partitions` through the same `STABLE` fetch call
//! (`032_log_fetch.sql:11-19`), so every facade of a cluster answers the same
//! thing. Gating it would only break the consumer recovering from an
//! OFFSET_OUT_OF_RANGE, which is the one client that most needs an answer.
//!
//! ## Both numbers come out of a fetch
//!
//! There is no bounds endpoint on Queen and there does not need to be: C2 was
//! designed so that an EMPTY fetch is the bounds probe (PLAN_QUEEN_KAFKA.md,
//! `POST /api/v1/fetch`). Every entry reports `highWatermark` and
//! `logStartOffset` whether or not it carried a record — and reports them
//! alongside a per-entry error too, which is what makes the probe a read of
//! ZERO records rather than a read of a few: asking at offset [`i64::MAX`] is
//! an offset the log cannot have, so the answer is OFFSET_OUT_OF_RANGE with
//! both bounds attached and not one segment touched.
//!
//! Two answer shapes are therefore both correct and both handled: the
//! OFFSET_OUT_OF_RANGE one, which is what a real log gives, and the
//! no-error one, which is what a lane whose watermarks somehow reach that far
//! would give. Only UNKNOWN_TOPIC_OR_PARTITION is a refusal.
//!
//! ## A concrete timestamp answers "no match", on purpose
//!
//! Kafka's timestamp lookup is served by a per-segment TIME index, which Queen
//! does not have: `queen.log_segments` is keyed by offset and carries one
//! `created_at` for the whole segment, so answering a timestamp query would
//! mean a scan, and answering it approximately would mean a consumer silently
//! starting somewhere other than where it asked. The protocol already has the
//! answer for a broker that cannot find a match — offset `-1`, error 0 — and
//! every client handles it (the Java consumer reports the partition as having
//! no offset for that timestamp, `offsetsForTimes` returns null for it). The
//! plan defers time-index lookups; this is the honest shape of that deferral,
//! and it is a `-1` a client can act on rather than an error it cannot.
//!
//! It is still an answer about a partition that EXISTS, so the bounds probe runs
//! for a concrete timestamp too — see [`stage`]. "No offset at that time" and
//! "no such topic" are different answers, and a client that asked the first
//! question must not be told it when the truth is the second.

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::list_offsets_request::ListOffsetsPartition;
use kafka_protocol::messages::list_offsets_response::{
    ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
};
use kafka_protocol::messages::{ListOffsetsRequest, ListOffsetsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::handlers::metadata;
use crate::queen::{self, FetchEntry, Fetched};
use crate::Facade;

/// Kafka's timestamp sentinels. `-3` (MAX_TIMESTAMP) and below arrive only from
/// v7 and up, which this facade does not advertise, so they are not named here
/// — anything that is not one of these two is treated as a concrete timestamp
/// and answered "no match", which is also the right answer for a sentinel from
/// a version we do not speak.
const LATEST: i64 = -1;
const EARLIEST: i64 = -2;

/// Kafka's "there is no offset for this". The answer to a timestamp lookup with
/// no match, and to a partition that errored.
const NO_OFFSET: i64 = -1;

/// The offset asked for by the bounds probe: one the log can never have
/// allocated, so the read touches no segment and the answer is the watermarks
/// alone. See the module header.
const BOUNDS_PROBE_OFFSET: i64 = i64::MAX;

/// The per-entry byte budget for the probe. The smallest the broker accepts,
/// because the probe is meant to read nothing — it is belt on braces: the
/// offset alone already guarantees no segment is opened.
const BOUNDS_PROBE_MAX_BYTES: i64 = 1;

/// What one requested partition resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Slot {
    /// Entry `index` of the batched probe carries this partition's bounds, and
    /// `timestamp` is which of them the client asked for — one of the two
    /// sentinels, or [`NO_MATCH`] for a concrete timestamp, which is probed for
    /// its EXISTENCE alone (see [`stage`]).
    Probe { index: usize, timestamp: i64 },
    /// Answer this error.
    Reject(ResponseError, String),
}

/// The `timestamp` a [`Slot::Probe`] carries when the client asked a concrete
/// timestamp: the partition is probed to find out whether it is there at all,
/// and if it is the answer is "no offset for that time".
///
/// A value no client can send — every real timestamp is `>= -2` — so it can
/// never collide with one.
const NO_MATCH: i64 = i64::MIN;

/// Handle one ListOffsets request.
///
/// `token` is the credential to reach Queen with — `QUEEN_TOKEN` at M3, the
/// connection's own tenant token from M5 on.
pub async fn handle(
    facade: &Facade,
    req: &ListOffsetsRequest,
    token: Option<&str>,
) -> ListOffsetsResponse {
    // `isolation_level` READ_COMMITTED asks for the last stable offset instead
    // of the high watermark. With no transactions they are the same number
    // (`handlers::fetch` answers both from `highWatermark` for the same
    // reason), so both levels are served the same bounds rather than one of
    // them being refused.
    //
    // The same width the fetch path checks against, from the same cache and for
    // the same reason: a partition Metadata never advertised has no bounds to
    // report, and answering `0` for it tells a consumer that a lane which does
    // not exist is merely empty. See [`stage`].
    let widths =
        metadata::advertised_widths(facade, req.topics.iter().map(|t| t.name.0.as_str()), token)
            .await;

    let mut entries: Vec<FetchEntry> = Vec::new();
    let mut slots: Vec<Vec<Slot>> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let name = topic.name.0.as_str();
        let width = widths.get(name).copied();
        slots.push(
            topic
                .partitions
                .iter()
                .map(|p| stage(&mut entries, name, width, p))
                .collect(),
        );
    }

    // One probe for every partition of the request, chunked only if the request
    // is wider than the broker serves in one call. No long poll: the bounds are
    // whatever they are right now, and parking for them would answer the same
    // numbers later.
    let probed = probe(facade, &entries, token).await;

    render(req, &slots, &probed)
}

/// Resolve one requested partition into a probe or a refusal.
///
/// The name and index rules are `handlers::fetch`'s, and deliberately the same
/// ones: a topic Metadata hides must not become visible by asking for its
/// bounds ([`metadata::not_a_topic_here`], which is also the one code a
/// consumer's offset lookup accepts), and a partition index outside
/// `0..width` — negative, or at or past the lanes Metadata advertises — is not
/// a lane to look up. The second is what stops this call from confirming a
/// stale assignment: `seekToEnd` on a partition that does not exist would
/// otherwise answer offset 0 with no error, which reads as an empty lane and
/// sends the consumer off to fetch it. `width` is `None` when the catalog does
/// not name the topic or could not be read, and then the probe runs as before —
/// Queen is the authority on whether the queue is there at all
/// ([`metadata::advertised_widths`]).
///
/// ## A concrete timestamp is still probed
///
/// "No match" is the right ANSWER for a timestamp this facade cannot resolve
/// (see the module header) — but only for a partition that exists. Answering it
/// without asking would tell a client that `no-such-topic` is there and merely
/// has no record at that time, which is the one thing an admin tool reads this
/// call to find out, and which every other shape of this request answers
/// correctly. So the probe is staged either way, carrying [`NO_MATCH`] as its
/// timestamp; it costs the same zero-read call the sentinels already make, and
/// the partition's existence comes back with it.
fn stage(
    entries: &mut Vec<FetchEntry>,
    topic: &str,
    width: Option<i32>,
    p: &ListOffsetsPartition,
) -> Slot {
    if let Some(e) = metadata::not_a_topic_here(topic) {
        return Slot::Reject(e, format!("`{topic}` is not a topic this facade serves"));
    }
    if p.partition_index < 0 {
        return Slot::Reject(
            ResponseError::UnknownTopicOrPartition,
            format!("partition {} is not a partition index", p.partition_index),
        );
    }
    if let Some(width) = width.filter(|w| p.partition_index >= *w) {
        return Slot::Reject(
            ResponseError::UnknownTopicOrPartition,
            format!(
                "partition {} is outside 0..{width}, the width `{topic}` is served at",
                p.partition_index
            ),
        );
    }
    entries.push(FetchEntry {
        queue: topic.to_string(),
        partition: p.partition_index.to_string(),
        offset: BOUNDS_PROBE_OFFSET,
        max_bytes: BOUNDS_PROBE_MAX_BYTES,
    });
    Slot::Probe {
        index: entries.len() - 1,
        timestamp: if p.timestamp == LATEST || p.timestamp == EARLIEST {
            p.timestamp
        } else {
            NO_MATCH
        },
    }
}

/// Run the bounds probe for every staged entry, in one call when they fit in
/// one. Returns one result per entry, index-aligned with `entries`.
async fn probe(
    facade: &Facade,
    entries: &[FetchEntry],
    token: Option<&str>,
) -> Vec<queen::Result<Fetched>> {
    let mut out = Vec::with_capacity(entries.len());
    for chunk in entries.chunks(queen::MAX_FETCH_ENTRIES) {
        match facade.queen.fetch(chunk, 0, 0, token).await {
            Ok(answers) if answers.len() == chunk.len() => out.extend(answers.into_iter().map(Ok)),
            Ok(answers) => {
                let e = queen::Error::Body(format!(
                    "the bounds probe answered {} entries for {} asked",
                    answers.len(),
                    chunk.len()
                ));
                out.extend(chunk.iter().map(|_| Err(e.clone())));
            }
            Err(e) => out.extend(chunk.iter().map(|_| Err(e.clone()))),
        }
    }
    out
}

// -------------------------------------------------------------------- answers

/// Build the response, logging one line per TOPIC rather than one per
/// partition: a consumer resolving a 1024-lane assignment against a topic that
/// is not there would otherwise write 1024 identical lines. Same rule, same
/// reason as `handlers::fetch`.
fn render(
    req: &ListOffsetsRequest,
    slots: &[Vec<Slot>],
    probed: &[queen::Result<Fetched>],
) -> ListOffsetsResponse {
    let mut topics = Vec::with_capacity(req.topics.len());
    for (topic, row) in req.topics.iter().zip(slots) {
        let name = topic.name.0.as_str();
        let mut failures = Failures::default();
        let partitions = topic
            .partitions
            .iter()
            .zip(row)
            .map(|(p, slot)| match slot {
                Slot::Reject(e, why) => {
                    failures.refused(p.partition_index, why);
                    rejected(p.partition_index, *e)
                }
                // `get` and not an index: a handler must not panic on anything
                // a broker answered, however wrong it is.
                Slot::Probe { index, timestamp } => match probed.get(*index) {
                    Some(Ok(f)) => bounds(&mut failures, p.partition_index, *timestamp, f),
                    Some(Err(e)) => {
                        failures.unread(p.partition_index, e);
                        rejected(p.partition_index, kafka_error(e))
                    }
                    None => rejected(p.partition_index, ResponseError::UnknownServerError),
                },
            })
            .collect();
        failures.log(name);
        topics.push(
            ListOffsetsTopicResponse::default()
                .with_name(TopicName(StrBytes::from_string(name.to_string())))
                .with_partitions(partitions),
        );
    }
    ListOffsetsResponse::default().with_topics(topics)
}

/// One topic's failures, as counts plus one worked example each. See
/// [`render`].
#[derive(Default)]
struct Failures {
    /// Refused before Queen was asked (a name rule, a negative index).
    refused: usize,
    refused_example: Option<(i32, String)>,
    /// The probe came back with a marker that is not the expected
    /// OFFSET_OUT_OF_RANGE — a topic that is not there, or one this build has
    /// no mapping for.
    from_log: usize,
    from_log_example: Option<(i32, String)>,
    unmapped_marker: bool,
    /// The probe did not answer at all.
    unread: usize,
    unread_example: Option<(i32, String)>,
}

impl Failures {
    fn refused(&mut self, partition: i32, why: &str) {
        self.refused += 1;
        self.refused_example
            .get_or_insert_with(|| (partition, why.to_string()));
    }

    fn refused_by_log(&mut self, partition: i32, marker: &str, unmapped: bool) {
        self.from_log += 1;
        self.unmapped_marker |= unmapped;
        self.from_log_example
            .get_or_insert_with(|| (partition, marker.to_string()));
    }

    fn unread(&mut self, partition: i32, e: &queen::Error) {
        self.unread += 1;
        self.unread_example
            .get_or_insert_with(|| (partition, e.to_string()));
    }

    fn log(&self, topic: &str) {
        if let Some((partition, why)) = &self.refused_example {
            tracing::debug!(
                target: "kafka",
                topic,
                partitions = self.refused,
                partition,
                %why,
                "list offsets refused before probing"
            );
        }
        if let Some((partition, marker)) = &self.from_log_example {
            if self.unmapped_marker {
                tracing::error!(
                    target: "kafka",
                    topic,
                    partitions = self.from_log,
                    partition,
                    marker,
                    "unmapped bounds-probe error marker"
                );
            } else {
                tracing::debug!(
                    target: "kafka",
                    topic,
                    partitions = self.from_log,
                    partition,
                    marker,
                    "the bounds probe found no such topic"
                );
            }
        }
        if let Some((partition, error)) = &self.unread_example {
            tracing::warn!(
                target: "kafka",
                topic,
                partitions = self.unread,
                partition,
                %error,
                "the bounds probe failed"
            );
        }
    }
}

/// One partition's answer, read off the probe.
fn bounds(
    failures: &mut Failures,
    index: i32,
    timestamp: i64,
    f: &Fetched,
) -> ListOffsetsPartitionResponse {
    // The probe asks at an offset the log cannot hold, so OFFSET_OUT_OF_RANGE
    // is the EXPECTED shape and carries the bounds. UNKNOWN_TOPIC_OR_PARTITION
    // is the only marker that is a refusal; anything else is a broker answer
    // this build has no mapping for, and guessing which of the two it resembles
    // would mean either inventing bounds or hiding a topic that is there.
    if let Some(marker) = f.error.as_deref().filter(|e| !e.is_empty()) {
        if marker == queen::FETCH_ERR_UNKNOWN {
            failures.refused_by_log(index, marker, false);
            return rejected(index, ResponseError::UnknownTopicOrPartition);
        }
        if marker != queen::FETCH_ERR_OUT_OF_RANGE {
            failures.refused_by_log(index, marker, true);
            return rejected(index, ResponseError::UnknownServerError);
        }
    }
    // The partition IS there — that is what the probe just established — so a
    // concrete timestamp can now be answered "no match" honestly. See the
    // module header for why there is no match to give.
    let offset = match timestamp {
        NO_MATCH => NO_OFFSET,
        EARLIEST => f.log_start_offset,
        _ => f.high_watermark,
    };
    ListOffsetsPartitionResponse::default()
        .with_partition_index(index)
        .with_error_code(0)
        // -1: the offset was found by watermark, not by time, so there is no
        // record timestamp to report with it. This is what a broker sends for
        // the two sentinels, and every client ignores it for them.
        .with_timestamp(NO_OFFSET)
        .with_offset(offset)
}

fn rejected(index: i32, error: ResponseError) -> ListOffsetsPartitionResponse {
    ListOffsetsPartitionResponse::default()
        .with_partition_index(index)
        .with_error_code(error.code())
        .with_timestamp(NO_OFFSET)
        .with_offset(NO_OFFSET)
}

/// The closest Kafka error for a failed probe.
///
/// The same mapping `handlers::fetch` uses, and for the same reason: this
/// request is answered to a CONSUMER, on the path it takes to recover from an
/// out-of-range fetch, so the code has to be one that makes it retry rather
/// than one that ends it.
fn kafka_error(e: &queen::Error) -> ResponseError {
    match e {
        queen::Error::Transport(_) => ResponseError::NotLeaderOrFollower,
        queen::Error::Status { code, .. } => match code {
            401 | 403 => ResponseError::TopicAuthorizationFailed,
            404 => ResponseError::UnknownTopicOrPartition,
            // 408 with the rest of the "later, not never" family, for the reason
            // spelled out in `fetch::kafka_error`: it can only come from an
            // intermediary, and a timeout is never a permanent verdict.
            408 | 429 | 502..=504 => ResponseError::NotLeaderOrFollower,
            _ => ResponseError::UnknownServerError,
        },
        queen::Error::Body(_) => ResponseError::UnknownServerError,
        // Unreachable on this path, and the arm is not a shrug: only the
        // fenced offset commit sends a conditional write ([`crate::cluster::fence`]),
        // and ListOffsets is a pure read of a partition's bounds. If one ever appeared here it
        // would be this facade's bug, so it is answered as one.
        queen::Error::Precondition { .. } => ResponseError::UnknownServerError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use crate::queen::{Error, FetchedRecord};
    use bytes::BytesMut;
    use kafka_protocol::messages::list_offsets_request::ListOffsetsTopic;
    use kafka_protocol::protocol::{Decodable, Encodable, Message};
    use serde_json::json;
    use std::sync::Arc;

    /// The same sweep `fetch` runs, on the other pure read of a partition's
    /// bounds. Both are consume-path codes and both must stay retriable where
    /// the situation is.
    #[test]
    fn every_status_the_proxy_writes_has_a_named_kafka_code() {
        for (code, want) in [
            (401, ResponseError::TopicAuthorizationFailed),
            (403, ResponseError::TopicAuthorizationFailed),
            (404, ResponseError::UnknownTopicOrPartition),
            (408, ResponseError::NotLeaderOrFollower),
            (413, ResponseError::UnknownServerError),
            (421, ResponseError::UnknownServerError),
            (429, ResponseError::NotLeaderOrFollower),
            (500, ResponseError::UnknownServerError),
            (502, ResponseError::NotLeaderOrFollower),
            (503, ResponseError::NotLeaderOrFollower),
            (504, ResponseError::NotLeaderOrFollower),
        ] {
            assert_eq!(kafka_error(&Error::status(code, "")), want, "HTTP {code}");
        }
        assert_eq!(
            kafka_error(&Error::Transport("reset".into())),
            ResponseError::NotLeaderOrFollower
        );
    }

    // -------------------------------------------------------------- fixtures

    fn facade(queues: &[(&str, i64)]) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        let facade = crate::handlers::testing::over(api.clone(), Default::default());
        (facade, api)
    }

    /// `[(topic, [(partition, timestamp)])]` → a ListOffsets request.
    fn request(topics: &[(&str, &[(i32, i64)])]) -> ListOffsetsRequest {
        ListOffsetsRequest::default()
            .with_replica_id((-1).into())
            .with_isolation_level(0)
            .with_topics(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        ListOffsetsTopic::default()
                            .with_name(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partitions(
                                partitions
                                    .iter()
                                    .map(|(index, timestamp)| {
                                        ListOffsetsPartition::default()
                                            .with_partition_index(*index)
                                            .with_timestamp(*timestamp)
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn answer<'a>(
        resp: &'a ListOffsetsResponse,
        topic: &str,
        partition: i32,
    ) -> &'a ListOffsetsPartitionResponse {
        resp.topics
            .iter()
            .find(|t| t.name.0.as_str() == topic)
            .unwrap_or_else(|| panic!("{topic} is not in the response"))
            .partitions
            .iter()
            .find(|p| p.partition_index == partition)
            .unwrap_or_else(|| panic!("{topic}/{partition} is not in the response"))
    }

    /// A lane with `count` records above a retention watermark of `start`.
    fn seed(api: &FakeQueen, queue: &str, partition: &str, start: i64, count: usize) {
        let payloads: Vec<serde_json::Value> = (0..count).map(|i| json!({ "n": i })).collect();
        api.seed(queue, partition, start, &payloads);
    }

    // ------------------------------------------------------------ the answers

    #[tokio::test]
    async fn latest_is_the_high_watermark_and_earliest_is_the_log_start() {
        let (f, api) = facade(&[("orders", 4)]);
        seed(&api, "orders", "2", 500, 7);

        let resp = handle(&f, &request(&[("orders", &[(2, LATEST)])]), None).await;
        let p = answer(&resp, "orders", 2);
        assert_eq!(p.error_code, 0);
        assert_eq!(p.offset, 507);
        assert_eq!(p.timestamp, -1, "a watermark lookup reports no timestamp");

        let resp = handle(&f, &request(&[("orders", &[(2, EARLIEST)])]), None).await;
        assert_eq!(answer(&resp, "orders", 2).offset, 500);
    }

    /// The probe reads NOTHING: it asks at an offset no log can hold, so the
    /// answer is the watermarks and not a segment.
    #[tokio::test]
    async fn the_probe_is_a_zero_read() {
        let (f, api) = facade(&[("orders", 1)]);
        seed(&api, "orders", "0", 0, 10_000);

        handle(&f, &request(&[("orders", &[(0, LATEST)])]), None).await;
        let asked = api.fetched();
        assert_eq!(asked.len(), 1);
        assert_eq!(asked[0].offset, i64::MAX);
        assert_eq!(asked[0].max_bytes, 1);
        // No parking either: the bounds are what they are right now.
        let calls = api.fetches.lock().unwrap().clone();
        assert_eq!((calls[0].1, calls[0].2), (0, 0));
    }

    /// Both answer shapes the probe can produce carry the bounds: the
    /// OFFSET_OUT_OF_RANGE one a real log gives, and the no-error one.
    #[tokio::test]
    async fn both_probe_shapes_are_read_for_their_bounds() {
        for error in [Some(queen::FETCH_ERR_OUT_OF_RANGE.to_string()), None] {
            let (f, api) = facade(&[("orders", 1)]);
            // Two entries asked, two answered: the probe is batched like any
            // other fetch, and a short answer is a misalignment, not bounds.
            let entry = Fetched {
                records: Vec::new(),
                high_watermark: 91,
                log_start_offset: 12,
                error: error.clone(),
            };
            api.reply_fetch(vec![entry.clone(), entry]);
            let resp = handle(
                &f,
                &request(&[("orders", &[(0, LATEST), (0, EARLIEST)])]),
                None,
            )
            .await;
            let got: Vec<i64> = resp.topics[0].partitions.iter().map(|p| p.offset).collect();
            assert_eq!(got, [91, 12], "{error:?}");
            for p in &resp.topics[0].partitions {
                assert_eq!(p.error_code, 0, "{error:?}");
            }
        }
    }

    /// A never-written lane is an empty log, not a missing one: 0 and 0, no
    /// error. This is what a fresh consumer on a 1024-lane topic sees, and an
    /// error here would stop it before it started.
    #[tokio::test]
    async fn a_never_written_lane_answers_zero_for_both() {
        // 1024 lanes wide, nothing written to any of them: every lane below the
        // advertised width is an empty log, and the two bounds are 0 and 0.
        let (f, _) = facade(&[("orders", 1024)]);
        let resp = handle(
            &f,
            &request(&[("orders", &[(0, EARLIEST), (1023, LATEST)])]),
            None,
        )
        .await;
        for p in &resp.topics[0].partitions {
            assert_eq!(p.error_code, 0, "lane {}", p.partition_index);
            assert_eq!(p.offset, 0, "lane {}", p.partition_index);
        }
    }

    /// A record still in the log after a probe would have consumed budget: the
    /// probe must not return records even when the broker hands some over.
    #[tokio::test]
    async fn records_in_a_probe_answer_are_ignored() {
        let (f, api) = facade(&[("orders", 1)]);
        api.reply_fetch(vec![Fetched {
            records: vec![FetchedRecord {
                offset: 3,
                payload: json!({"k": null, "v": null}),
                ts: None,
            }],
            high_watermark: 4,
            log_start_offset: 0,
            error: None,
        }]);
        let resp = handle(&f, &request(&[("orders", &[(0, LATEST)])]), None).await;
        assert_eq!(answer(&resp, "orders", 0).offset, 4);
    }

    // ------------------------------------------------------- timestamp lookup

    /// A concrete timestamp answers "no offset", with no error — the shape
    /// every client already handles for a broker that finds no match. Queen has
    /// no time index and the plan defers one.
    #[tokio::test]
    async fn a_concrete_timestamp_is_no_match_and_no_error() {
        let (f, api) = facade(&[("orders", 1)]);
        seed(&api, "orders", "0", 0, 5);

        for timestamp in [0i64, 1, 1_756_000_000_000, i64::MAX] {
            let resp = handle(&f, &request(&[("orders", &[(0, timestamp)])]), None).await;
            let p = answer(&resp, "orders", 0);
            assert_eq!(p.error_code, 0, "{timestamp}");
            assert_eq!(p.offset, -1, "{timestamp}");
            assert_eq!(p.timestamp, -1, "{timestamp}");
        }
        // It costs one zero-read probe per partition — the same one the
        // sentinels make — because "no offset at that time" is an answer about a
        // partition that EXISTS. See the test below.
        assert_eq!(api.fetched().len(), 4);
        assert!(api.fetched().iter().all(|e| e.offset == i64::MAX));
    }

    /// ...and the existence question is really asked: a concrete timestamp
    /// against a topic that is not there answers UNKNOWN_TOPIC_OR_PARTITION, not
    /// "it exists and has no record at that time". Answering the second would
    /// tell an admin tool a topic is present when it is not.
    #[tokio::test]
    async fn a_concrete_timestamp_on_a_topic_that_is_not_there_says_so() {
        let (f, _) = facade(&[]);
        let resp = handle(&f, &request(&[("orders", &[(0, 1_756_000_000_000)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::UnknownTopicOrPartition.code());
        assert_eq!(p.offset, -1);
    }

    /// A sentinel from a version this facade does not advertise (-3 and below,
    /// v7+) is a timestamp we cannot answer, and lands on the same "no match"
    /// rather than being read as one of the two we do know.
    #[tokio::test]
    async fn an_unadvertised_sentinel_is_no_match_not_a_watermark() {
        let (f, api) = facade(&[("orders", 1)]);
        seed(&api, "orders", "0", 0, 5);
        for timestamp in [-3i64, -4, -5, -100] {
            let resp = handle(&f, &request(&[("orders", &[(0, timestamp)])]), None).await;
            assert_eq!(answer(&resp, "orders", 0).offset, -1, "{timestamp}");
            assert_eq!(answer(&resp, "orders", 0).error_code, 0, "{timestamp}");
        }
        // ...and it is a lookup that found nothing, not a lane that is missing:
        // the probe ran and the partition is there.
        assert_eq!(api.fetched().len(), 4);
    }

    // --------------------------------------------------------------- errors

    #[tokio::test]
    async fn an_unknown_topic_is_unknown() {
        let (f, _) = facade(&[]);
        let resp = handle(&f, &request(&[("orders", &[(0, LATEST)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::UnknownTopicOrPartition.code());
        assert_eq!(p.offset, -1);
    }

    /// Both halves of the name rule answer the SAME code here, and it is not the
    /// INVALID_TOPIC_EXCEPTION Metadata answers for the second: this request is
    /// on a consumer's recovery path, and Apache Kafka never answers a name
    /// error to it — a broker that does not have the topic says exactly that.
    /// See [`metadata::not_a_topic_here`].
    #[tokio::test]
    async fn a_reserved_or_unnameable_topic_never_reaches_queen() {
        let (f, api) = facade(&[("__consumer_offsets", 4)]);
        let resp = handle(
            &f,
            &request(&[
                ("__consumer_offsets", &[(0, LATEST)]),
                ("not a topic", &[(0, LATEST)]),
            ]),
            None,
        )
        .await;
        assert_eq!(
            answer(&resp, "__consumer_offsets", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(
            answer(&resp, "not a topic", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(api.fetches.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_negative_partition_index_is_unknown() {
        let (f, api) = facade(&[("orders", 4)]);
        let resp = handle(&f, &request(&[("orders", &[(-1, LATEST)])]), None).await;
        assert_eq!(
            answer(&resp, "orders", -1).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(api.fetches.lock().unwrap().is_empty());
    }

    /// A partition at or past the advertised width has no bounds, and is
    /// answered as the lane it is not.
    ///
    /// The same rule as the fetch path's, and it matters here for a reason of
    /// its own: this is the call `seekToEnd` and every lag tool makes, so a
    /// ghost lane answered `0` with no error is a consumer told to start
    /// reading a partition that will never exist. Every sentinel and a concrete
    /// timestamp alike, because all three are questions about a lane.
    #[tokio::test]
    async fn a_partition_past_the_advertised_width_is_unknown() {
        let (f, api) = facade(&[("orders", 8)]);
        seed(&api, "orders", "7", 0, 3);
        for timestamp in [LATEST, EARLIEST, 1_756_000_000_000] {
            let resp = handle(
                &f,
                &request(&[(
                    "orders",
                    &[(7, timestamp), (8, timestamp), (4242, timestamp)],
                )]),
                None,
            )
            .await;
            assert_eq!(
                answer(&resp, "orders", 7).error_code,
                0,
                "the last real lane, {timestamp}"
            );
            for ghost in [8, 4242] {
                let p = answer(&resp, "orders", ghost);
                assert_eq!(
                    p.error_code,
                    ResponseError::UnknownTopicOrPartition.code(),
                    "lane {ghost} at {timestamp}"
                );
                assert_eq!(p.offset, NO_OFFSET, "lane {ghost} at {timestamp}");
            }
        }
        // One probe per request, for the one lane that exists.
        assert_eq!(api.fetched().len(), 3);
    }

    /// ...and a catalog that cannot be read costs the check, not the answer:
    /// the probe runs and the bounds come back, exactly as before there was a
    /// width to check against.
    #[tokio::test]
    async fn an_unreadable_catalog_costs_the_width_check_and_not_the_probe() {
        let (f, api) = facade(&[("orders", 8)]);
        seed(&api, "orders", "2", 5, 4);
        api.fail_list(queen::Error::status(503, "draining"));

        let resp = handle(&f, &request(&[("orders", &[(2, LATEST)])]), None).await;
        let p = answer(&resp, "orders", 2);
        assert_eq!(p.error_code, 0);
        assert_eq!(p.offset, 9);
    }

    #[tokio::test]
    async fn a_failed_probe_maps_to_a_code_consumers_handle() {
        let cases: [(Error, ResponseError); 4] = [
            (
                Error::Transport("reset".into()),
                ResponseError::NotLeaderOrFollower,
            ),
            (
                Error::status(403, "forbidden"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(503, "draining"),
                ResponseError::NotLeaderOrFollower,
            ),
            (
                Error::status(500, "boom"),
                ResponseError::UnknownServerError,
            ),
        ];
        for (queen_error, kafka) in cases {
            let (f, api) = facade(&[("orders", 1)]);
            api.fail_fetch(queen_error.clone());
            let resp = handle(&f, &request(&[("orders", &[(0, LATEST)])]), None).await;
            let p = answer(&resp, "orders", 0);
            assert_eq!(p.error_code, kafka.code(), "{queen_error}");
            assert_eq!(p.offset, -1, "{queen_error}");
        }
    }

    #[tokio::test]
    async fn an_unmapped_marker_is_a_server_error_not_invented_bounds() {
        let (f, api) = facade(&[("orders", 1)]);
        api.reply_fetch(vec![Fetched {
            records: Vec::new(),
            high_watermark: 5,
            log_start_offset: 1,
            error: Some("SOMETHING_NEW".to_string()),
        }]);
        let resp = handle(&f, &request(&[("orders", &[(0, LATEST)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::UnknownServerError.code());
        assert_eq!(p.offset, -1);
    }

    // ------------------------------------------------------------- batching

    /// Every partition of the request goes into one probe.
    #[tokio::test]
    async fn one_request_is_one_probe_across_every_topic() {
        let (f, api) = facade(&[("orders", 4), ("clicks", 4)]);
        seed(&api, "orders", "0", 0, 3);
        seed(&api, "clicks", "1", 10, 2);

        let resp = handle(
            &f,
            &request(&[
                ("orders", &[(0, LATEST), (0, EARLIEST)]),
                ("clicks", &[(1, EARLIEST)]),
            ]),
            None,
        )
        .await;

        assert_eq!(api.fetches.lock().unwrap().len(), 1, "not one probe");
        assert_eq!(api.fetched().len(), 3);
        assert_eq!(resp.topics[0].partitions[0].offset, 3);
        assert_eq!(resp.topics[0].partitions[1].offset, 0);
        assert_eq!(answer(&resp, "clicks", 1).offset, 10);
    }

    /// A request wider than the broker's entry ceiling is chunked, because an
    /// over-long list is a 400 for the whole batch.
    #[tokio::test]
    async fn a_wide_request_is_chunked() {
        let (f, api) = facade(&[("orders", 1024), ("clicks", 1024)]);
        let lanes: Vec<(i32, i64)> = (0..1024).map(|i| (i, LATEST)).collect();
        let resp = handle(
            &f,
            &request(&[("orders", &lanes), ("clicks", &lanes)]),
            None,
        )
        .await;

        let calls = api.fetches.lock().unwrap().clone();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0.len(), queen::MAX_FETCH_ENTRIES);
        for t in &resp.topics {
            assert_eq!(t.partitions.len(), 1024);
            for p in &t.partitions {
                assert_eq!(p.error_code, 0);
                assert_eq!(p.offset, 0);
            }
        }
    }

    /// A partition asked about twice is answered twice, in the order it was
    /// asked — clients match the response against what they sent.
    #[tokio::test]
    async fn every_requested_partition_is_answered_in_order() {
        let (f, api) = facade(&[("orders", 4)]);
        seed(&api, "orders", "1", 4, 6);
        let resp = handle(
            &f,
            &request(&[("orders", &[(3, LATEST), (1, EARLIEST), (1, LATEST)])]),
            None,
        )
        .await;
        let got: Vec<(i32, i64)> = resp.topics[0]
            .partitions
            .iter()
            .map(|p| (p.partition_index, p.offset))
            .collect();
        assert_eq!(got, [(3, 0), (1, 4), (1, 10)]);
    }

    #[tokio::test]
    async fn the_token_reaches_the_probe() {
        let (f, api) = facade(&[("orders", 1)]);
        handle(
            &f,
            &request(&[("orders", &[(0, LATEST)])]),
            Some("tenant-a"),
        )
        .await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }

    // -------------------------------------------------------------- the wire

    /// Every advertised version encodes and decodes cleanly, both ways.
    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let (f, api) = facade(&[("orders", 4)]);
        seed(&api, "orders", "1", 7, 5);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::ListOffsets as i16)
            .expect("ListOffsets is advertised");
        assert!(
            row.min >= ListOffsetsRequest::VERSIONS.min
                && row.max <= ListOffsetsRequest::VERSIONS.max
        );

        for version in row.min..=row.max {
            let req = request(&[("orders", &[(1, LATEST)])]);
            let mut wire = BytesMut::new();
            req.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = ListOffsetsRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(
                buf.is_empty(),
                "v{version}: {} trailing request bytes",
                buf.len()
            );

            let resp = handle(&f, &decoded, None).await;
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = ListOffsetsResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(
                buf.is_empty(),
                "v{version}: {} trailing response bytes",
                buf.len()
            );

            let p = answer(&back, "orders", 1);
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.offset, 12, "v{version}");
            assert_eq!(p.timestamp, -1, "v{version}");
            // The leader epoch exists from v4 and is -1 everywhere: the facade
            // holds no elections to number (Metadata says the same).
            if version >= 4 {
                assert_eq!(p.leader_epoch, -1, "v{version}");
            }
        }
    }

    /// Both isolation levels are the same answer: with no transactions the last
    /// stable offset IS the high watermark.
    #[tokio::test]
    async fn read_committed_and_read_uncommitted_agree() {
        let (f, api) = facade(&[("orders", 1)]);
        seed(&api, "orders", "0", 0, 3);
        let mut seen = Vec::new();
        for level in [0i8, 1] {
            let req = request(&[("orders", &[(0, LATEST)])]).with_isolation_level(level);
            let resp = handle(&f, &req, None).await;
            seen.push(answer(&resp, "orders", 0).offset);
        }
        assert_eq!(seen, [3, 3]);
    }
}
