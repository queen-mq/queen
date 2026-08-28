//! Fetch — the read path, and the half of M3 that makes a Kafka consumer work
//! (PLAN_QUEEN_KAFKA.md M3: "group-less consume").
//!
//! One Fetch request names (topic, partition, offset) triples and asks for the
//! records at and after each offset. It maps onto `POST /api/v1/fetch` (core
//! change C2) with almost no impedance, because C2 was designed against this
//! request: it is batched over partitions, it reads from an ABSOLUTE offset, it
//! takes no lease and moves no cursor, it long-polls on `maxWaitMs`/`minBytes`,
//! and every entry reports `highWatermark` and `logStartOffset` whether or not
//! it carried a record. So one Fetch is ONE call to Queen covering every named
//! partition, and the handler's work is the two translations either side of it:
//! Kafka's ceilings onto C2's, and stored payloads back into a RecordBatch.
//!
//! ## v4..=v6, and why the ceiling is not a shrug
//!
//! v7 introduces FETCH SESSIONS (KIP-227): the client registers an incremental
//! session and thereafter sends only the partitions that CHANGED, with the
//! broker holding the rest of the assignment as durable per-connection state.
//! A facade that answered v7 would have to keep that state, keep it correct
//! across a restart it is explicitly allowed to have (the plan: "a facade
//! restart behaves like a Kafka broker restart"), and get the epoch/session-id
//! dance right — for no benefit, because the whole point of a session is to
//! save re-sending a partition list that this facade forwards to Queen in full
//! anyway. Capping at v6 deletes the problem: `session_id`, `session_epoch` and
//! `forgotten_topics_data` do not exist below v7, so there is no state to keep
//! and no way for a client to ask for any. Below, v4 is the schema's own floor.
//!
//! ## What this path asks the catalog, and what it leaves to Queen
//!
//! Metadata and Produce both start by listing queues, because both can CREATE
//! one and `/configure` is an upsert that must never run against a queue that
//! already exists. A fetch creates nothing, so it has no such hazard — and C2
//! is already the authority on whether a queue is THERE: it resolves
//! `(queue name, tenant)` itself and answers UNKNOWN_TOPIC_OR_PARTITION for a
//! queue that is not this tenant's, byte-identically to one that exists
//! nowhere. That question is left to it, so an admin-API blip can never fail a
//! fetch of records Queen would have served.
//!
//! One question cannot be: how WIDE the topic is. C2 has no notion of a width —
//! a Queen partition is a NAME, and a name nobody has written to is an empty
//! lane rather than an error — so a fetch of partition 8 of an eight-lane topic
//! comes back from it as a perfectly legitimate empty read, at high watermark
//! 0, for ever. The width is the facade's own invention: Metadata advertises it
//! and Produce refuses a write past it, which is precisely why the read has to
//! refuse one too, or a consumer holding a stale assignment polls a ghost
//! partition instead of refreshing its metadata. So the width — and only the
//! width — is read here, from the same briefly-cached queue list Produce
//! already uses ([`metadata::advertised_widths`]): one call per credential per
//! TTL, shared with every other connection, and a catalog that cannot be read
//! costs the CHECK rather than the fetch. The other local rules are the ones C2
//! cannot know: the Kafka NAME rules (so a `__` topic stays as invisible here
//! as Metadata makes it) and the request shapes C2 rejects wholesale — see
//! [`stage`].
//!
//! ## The response is counted, because the frame codec does not forgive
//!
//! A Fetch answer goes out as ONE length-prefixed frame, and `conn::codec()`
//! refuses to ENCODE one above `conn::MAX_FRAME_BYTES` — the connection is then
//! dropped with no response at all, the client retries the same fetch, and it is
//! dropped again. Nothing on the request side prevents that on its own: Kafka's
//! request-level `max_bytes` has no counterpart in C2 (see [`budget`]), C2's own
//! 64 MiB ceiling is per CALL and a wide assignment is several calls, and every
//! budget on the way in is spent in COMPRESSED segment bytes, which say nothing
//! about the records that come out. So the only place the answer can be counted
//! is here, on the bytes this handler itself produces, and that is what
//! [`MAX_RESPONSE_BYTES`] does: partitions are filled in request order until the
//! budget is gone and the rest are answered as empty reads with their real
//! watermarks. That is a Kafka broker's own behaviour — `max_bytes` is a soft
//! limit (KIP-74) and a partition that gets nothing this poll gets it on the
//! next.
//!
//! Two consequences worth naming. The first batch is always delivered whole
//! enough to carry ONE record however large it is, so a consumer can never stall
//! on a record bigger than its budget. And the fill order is the request's,
//! which for a consumer that keeps its leading partitions saturated means the
//! trailing ones are served only as the leaders drain; Apache Kafka rotates the
//! partition order per fetch to avoid that, this facade does not yet, and the
//! budget is set high enough that it takes a genuinely saturated 32 MiB poll to
//! notice.
//!
//! ## The poison-pill rule
//!
//! C2 answers per entry, but three request-level shapes make it reject the
//! WHOLE batch with a 400: a negative offset, an empty entry list, and more
//! than [`queen::MAX_FETCH_ENTRIES`] entries. Forwarding any of them would turn
//! one malformed partition into every other partition's fetch failing, which is
//! precisely the isolation Kafka clients are built to rely on. Each is
//! therefore handled before the call: the first per partition, the second by
//! not calling, and the third by chunking.

use bytes::Bytes;
use kafka_protocol::error::ResponseError;
use kafka_protocol::indexmap::IndexMap;
use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};
use kafka_protocol::messages::{FetchRequest, FetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{Record, TimestampType, NO_PRODUCER_EPOCH, NO_PRODUCER_ID};

use crate::handlers::metadata;
use crate::queen::{self, FetchEntry, Fetched};
use crate::records as envelope;
use crate::throttle;
use crate::wire;
use crate::Facade;

/// Kafka's "there is no offset here", used for every watermark of a partition
/// that carries an error (`FetchResponse.INVALID_HIGHWATERMARK` and its
/// log-start twin). C2 reports real bounds even alongside an error, and they
/// are deliberately NOT passed through: a client reads a partition's watermarks
/// only when its error code is 0, and a real-looking number beside an error is
/// one more thing a client can act on wrongly. ListOffsets is where the bounds
/// are the answer, and it asks for them there.
const NO_OFFSET: i64 = -1;

/// The per-entry byte budget when neither the request nor the partition names a
/// usable one. C2's own default for an absent `maxBytes`
/// (server/src/handlers/fetch.rs, `DEFAULT_MAX_BYTES`), so a client that sends
/// nothing meaningful gets what the broker would have chosen for it.
const DEFAULT_MAX_BYTES: i64 = 1024 * 1024;

/// Ceiling on the RECORD bytes one Fetch response may carry, and the only bound
/// on this side that is about the frame.
///
/// Sized against the two numbers either side of it. Above: `MAX_FRAME_BYTES` is
/// 100 MiB and a frame past it is not an error a client can see — the encoder
/// refuses it and the connection dies. Below: one C2 call renders at most 64 MiB
/// of JSON (server/src/handlers/fetch.rs, `MAX_RENDERED_BYTES`), whose base64
/// payloads are at most ~48 MiB of records, and the first batch of a response is
/// delivered whatever its size so a consumer never stalls. 32 MiB therefore
/// bounds the worst answer this handler can build at roughly 80 MiB, with room
/// to spare under the frame, while being larger than any batch a consumer polls
/// for in practice — Kafka's own `fetch.max.bytes` default is 50 MiB and is a
/// ceiling clients almost never reach.
const MAX_RESPONSE_BYTES: i64 = 32 * 1024 * 1024;

/// The offset and per-entry budget of the ZERO-READ bounds probe — the same pair
/// `handlers::list_offsets` asks with, and for the same reason: an offset no log
/// can hold answers OFFSET_OUT_OF_RANGE *with the bounds*, reading no segment.
/// It is how a partition the response budget did not reach still reports a true
/// high watermark instead of a placeholder. See [`bounds_only`].
const BOUNDS_PROBE_OFFSET: i64 = i64::MAX;
const BOUNDS_PROBE_MAX_BYTES: i64 = 1;

/// The framing one record adds to an encoded batch, over its own bytes: a
/// length, an attribute byte, a timestamp delta, an offset delta, a key length,
/// a value length and a header count, each a varint of at most five bytes (ten
/// for the timestamp's varlong). Rounded UP, because this number is used to
/// decide whether a record still fits and an under-estimate is the frame this
/// module exists to avoid.
const RECORD_FRAMING_BYTES: usize = 40;
/// The same, per header: two more varint lengths.
const HEADER_FRAMING_BYTES: usize = 10;
/// And the batch's own header — 61 bytes of base offset, CRC, attributes and
/// watermarks, once per partition.
const BATCH_HEADER_BYTES: usize = 64;

/// What one (topic, partition) entry of the request resolved to.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Slot {
    /// Entry `index` of the batched fetch is this partition's read.
    Read(usize),
    /// Answer this error, and read nothing.
    Reject(ResponseError, String),
}

/// Handle one Fetch request.
///
/// `token` is the credential to reach Queen with — `QUEEN_TOKEN` at M3, the
/// connection's own tenant token from M5 on.
pub async fn handle(facade: &Facade, req: &FetchRequest, token: Option<&str>) -> FetchResponse {
    // `isolation_level` is accepted at both values and acted on at neither, and
    // that is correct rather than lax: READ_COMMITTED asks the broker to hide
    // records above the last stable offset and to list the aborted
    // transactions. There are no transactions here (`handlers::produce` refuses
    // every shape of one), so every record is committed, the LSO IS the high
    // watermark, and the aborted list is empty — which is exactly what a
    // READ_COMMITTED consumer is told below. The two isolation levels are the
    // same answer because the log they read has only one kind of record in it.
    let request_max_bytes = req.max_bytes;

    // The one thing this path asks the catalog for: how wide each topic is
    // ([`metadata::advertised_widths`]). A partition index past that width was
    // never advertised and cannot be written to either, so answering it as an
    // empty lane would leave a consumer with a stale assignment polling a
    // partition that will never fill. An unreadable catalog costs the check and
    // nothing else — see the module header.
    let widths =
        metadata::advertised_widths(facade, req.topics.iter().map(|t| t.topic.0.as_str()), token)
            .await;

    let mut entries: Vec<FetchEntry> = Vec::new();
    let mut slots: Vec<Vec<Slot>> = Vec::with_capacity(req.topics.len());
    for topic in &req.topics {
        let name = topic.topic.0.as_str();
        let width = widths.get(name).copied();
        slots.push(
            topic
                .partitions
                .iter()
                .map(|p| {
                    stage(
                        &mut entries,
                        name,
                        p.partition,
                        width,
                        p.fetch_offset,
                        budget(p.partition_max_bytes, request_max_bytes),
                    )
                })
                .collect(),
        );
    }

    let budget = response_budget(request_max_bytes);
    let read = read_all(
        facade,
        &entries,
        i64::from(req.max_wait_ms).clamp(0, queen::MAX_FETCH_WAIT_MS),
        i64::from(req.min_bytes).max(0),
        budget,
        token,
    )
    .await;

    render(req, &slots, &read, budget)
}

/// How many record bytes this response may carry: what the client asked for,
/// under [`MAX_RESPONSE_BYTES`].
///
/// Never zero. A budget of nothing would answer every partition empty on every
/// poll, which is a consumer that never advances — and a client sending
/// `max_bytes = 0` (or the -1 some send for "no preference") means "you choose",
/// not "send me nothing".
fn response_budget(request_max_bytes: i32) -> usize {
    let asked = if request_max_bytes > 0 {
        i64::from(request_max_bytes)
    } else {
        MAX_RESPONSE_BYTES
    };
    asked.clamp(1, MAX_RESPONSE_BYTES) as usize
}

/// Resolve one requested partition into a read or a refusal.
///
/// Everything decidable without Queen is decided here, and each refusal is a
/// shape C2 would answer wrongly or reject wholesale:
///
///   * a reserved or unnameable topic — the same rule Metadata applies, in the
///     one code a fetching client accepts ([`metadata::not_a_topic_here`]), so a
///     queue the listing hides cannot be read by naming it;
///   * a partition index outside `0..width`, where `width` is the number of
///     lanes Metadata advertises for this topic. A negative index would be sent
///     to C2 as the partition NAME `"-1"`; an index at or past the width names a
///     lane that does not exist, which C2 — where a partition is a NAME and any
///     name is a lane nobody has written yet — would answer as an empty log
///     with a high watermark of 0 and no error. That is the worst answer
///     available: a consumer holding a stale assignment (its topic recreated
///     narrower, a partition list from another cluster) would poll a ghost
///     partition for ever instead of being told to refresh its metadata. The
///     width is the one the WRITE path already enforces
///     (`handlers::produce::stage`) from the same catalog, so a lane that
///     cannot be produced to cannot be fetched from either. `width` is `None`
///     when the catalog does not name this topic or could not be read at all,
///     and then the entry is passed to C2 as before — Queen is the authority on
///     whether a queue exists, and a blip in the admin API must not fail a
///     fetch of records it would have served;
///   * a negative fetch offset, which C2 refuses for the whole BATCH (it is a
///     Kafka sentinel, and serving it as 0 would hand a consumer the entire
///     backlog when it asked for the tail).
fn stage(
    entries: &mut Vec<FetchEntry>,
    topic: &str,
    partition: i32,
    width: Option<i32>,
    offset: i64,
    max_bytes: i64,
) -> Slot {
    if let Some(e) = metadata::not_a_topic_here(topic) {
        return Slot::Reject(e, format!("`{topic}` is not a topic this facade serves"));
    }
    if partition < 0 {
        return Slot::Reject(
            ResponseError::UnknownTopicOrPartition,
            format!("partition {partition} is not a partition index"),
        );
    }
    if let Some(width) = width.filter(|w| partition >= *w) {
        return Slot::Reject(
            ResponseError::UnknownTopicOrPartition,
            format!(
                "partition {partition} is outside 0..{width}, the width `{topic}` is served at"
            ),
        );
    }
    if offset < 0 {
        // OFFSET_OUT_OF_RANGE and not an invalid-request code: it is the answer
        // that makes a consumer with a corrupted position run its
        // `auto.offset.reset`, which is the only way out of this state.
        return Slot::Reject(
            ResponseError::OffsetOutOfRange,
            format!(
                "fetch offset {offset} is negative; the -1/-2 sentinels are ListOffsets', not \
                 Fetch's"
            ),
        );
    }
    entries.push(FetchEntry {
        queue: topic.to_string(),
        // Kafka partition n = Queen partition n, by name — the same spelling
        // the produce path writes with.
        partition: partition.to_string(),
        offset,
        max_bytes,
    });
    Slot::Read(entries.len() - 1)
}

/// The per-entry byte budget for one partition, from the two ceilings Kafka
/// sends.
///
/// `partition_max_bytes` is per partition and `max_bytes` is for the whole
/// response; a partition can never be allowed more than either. C2 has no
/// request-level knob to forward the second to — its whole-response budget is a
/// fixed 64 MiB server-side ceiling (`MAX_TOTAL_BYTES`) that a caller cannot
/// raise or lower — so the request-level number is folded into the per-entry
/// one, which is the only place it can have any effect at all.
///
/// Both are spent by C2 in COMPRESSED segment bytes rather than in record
/// bytes, so the budget buys an unknown and usually larger number of the bytes
/// Kafka is counting. That is a documented property of the field on Kafka's own
/// brokers, not a liberty taken here: `max_bytes` is a soft limit (KIP-74), a
/// fetch response always carries at least one whole batch however large, and
/// every client is written to handle a response bigger than it asked for.
/// C2 keeps the same promise from the other end (its first segment escapes
/// every bound), which is what stops an over-large record stalling a consumer.
fn budget(partition_max_bytes: i32, request_max_bytes: i32) -> i64 {
    let ceilings = [partition_max_bytes, request_max_bytes];
    ceilings
        .iter()
        .filter(|b| **b > 0)
        .map(|b| i64::from(*b))
        .min()
        .unwrap_or(DEFAULT_MAX_BYTES)
        .clamp(1, queen::MAX_FETCH_BYTES_PER_ENTRY)
}

/// Read every staged entry, in one call to Queen when they fit in one.
///
/// Returns one result per entry, index-aligned with `entries`. A call that
/// fails hands its own error to every entry it carried, so a fetch of two
/// chunks can have one succeed and the other not — the same per-partition
/// isolation the rest of the handler keeps.
///
/// ## Chunking, and where the long poll goes
///
/// C2 serves at most [`queen::MAX_FETCH_ENTRIES`] entries per call and REJECTS
/// a longer list rather than truncating it, so an assignment wider than that
/// (two full topics at the default 1024 lanes, or one native queue with more)
/// is walked in chunks. The long poll is given to the FIRST chunk and every
/// later chunk is asked not to park, which keeps the property a client actually
/// depends on — the request returns within one `max_wait_ms`, never a multiple
/// of it. What it costs is latency in the case where the parked chunk stays
/// quiet and a later one receives a record while it waits: that record is
/// answered on the consumer's next poll instead of this one. Parking on all of
/// them at once would need the calls to be concurrent, which is a change to how
/// this facade talks to Queen and not something to slip into M3; until then the
/// common case — one topic, one call, one park — is exact.
///
/// ## And where the response budget goes
///
/// Once the chunks already read carry `budget` bytes, nothing a later chunk
/// returned could be encoded into the answer anyway ([`render`] stops there),
/// and reading it would mean holding another C2 response — up to 64 MiB of JSON
/// per call — to throw away. So the remaining chunks are read as BOUNDS ONLY
/// ([`bounds_only`]): the partitions still report a true high watermark and a
/// true log start, they carry no records, and the memory a wide assignment costs
/// stops growing with the width.
async fn read_all(
    facade: &Facade,
    entries: &[FetchEntry],
    max_wait_ms: i64,
    min_bytes: i64,
    budget: usize,
    token: Option<&str>,
) -> Vec<queen::Result<Fetched>> {
    let mut out = Vec::with_capacity(entries.len());
    let mut carried = 0usize;
    for chunk in entries.chunks(queen::MAX_FETCH_ENTRIES) {
        let first = out.is_empty();
        // The first chunk is always read for real: a fetch that read nothing at
        // all would be a consumer that never advances.
        let answers = if !first && carried >= budget {
            bounds_only(facade, chunk, token).await
        } else {
            let (wait, min) = if first {
                (max_wait_ms, min_bytes)
            } else {
                (0, 0)
            };
            call(facade, chunk, wait, min, token).await
        };
        // Counted only while there is a later chunk the count could spare. On
        // the last one — which is EVERY fetch a consumer of one topic makes — it
        // would be a walk of every payload for an answer nobody reads.
        if out.len() + chunk.len() < entries.len() {
            carried += answers
                .iter()
                .filter_map(|a| a.as_ref().ok())
                .map(carried_bytes)
                .sum::<usize>();
        }
        out.extend(answers);
    }
    out
}

/// One C2 call for one chunk, answered one result per entry.
///
/// A call that fails hands its own error to every entry it carried.
async fn call(
    facade: &Facade,
    chunk: &[FetchEntry],
    max_wait_ms: i64,
    min_bytes: i64,
    token: Option<&str>,
) -> Vec<queen::Result<Fetched>> {
    match facade
        .queen
        .fetch(chunk, max_wait_ms, min_bytes, token)
        .await
    {
        Ok(answers) if answers.len() == chunk.len() => answers.into_iter().map(Ok).collect(),
        // `queen::align_fetch_results` already refuses a misaligned answer;
        // this is the same refusal at the chunk boundary, so a short answer
        // can never shift one partition's records onto another.
        Ok(answers) => {
            let e = queen::Error::Body(format!(
                "fetch answered {} entries for {} asked",
                answers.len(),
                chunk.len()
            ));
            chunk.iter().map(|_| Err(e.clone())).collect()
        }
        Err(e) => chunk.iter().map(|_| Err(e.clone())).collect(),
    }
}

/// A chunk the response budget will not reach, read for its bounds alone.
///
/// The probe asks at an offset no log can hold, so C2 reads no segment and
/// answers OFFSET_OUT_OF_RANGE *with* the real `highWatermark` and
/// `logStartOffset` — which is the whole answer needed here. What is NOT carried
/// over is the probe's own error: it is out of range because the PROBE's offset
/// is, and says nothing about the caller's. So the caller's offset is checked
/// against the bounds that came back, exactly as C2 would have checked it, and
/// only then is OFFSET_OUT_OF_RANGE the answer. Any other marker — an unknown
/// topic, one this build has not learned — is about the partition rather than
/// the offset and passes straight through.
async fn bounds_only(
    facade: &Facade,
    chunk: &[FetchEntry],
    token: Option<&str>,
) -> Vec<queen::Result<Fetched>> {
    let probes: Vec<FetchEntry> = chunk
        .iter()
        .map(|e| FetchEntry {
            queue: e.queue.clone(),
            partition: e.partition.clone(),
            offset: BOUNDS_PROBE_OFFSET,
            max_bytes: BOUNDS_PROBE_MAX_BYTES,
        })
        .collect();
    let mut answers = call(facade, &probes, 0, 0, token).await;
    for (answer, asked) in answers.iter_mut().zip(chunk) {
        if let Ok(f) = answer {
            let out_of_range = asked.offset < f.log_start_offset || asked.offset > f.high_watermark;
            let error = match f.error.as_deref() {
                None | Some(queen::FETCH_ERR_OUT_OF_RANGE) => {
                    out_of_range.then(|| queen::FETCH_ERR_OUT_OF_RANGE.to_string())
                }
                other => other.map(str::to_string),
            };
            *f = Fetched {
                // Belt on braces: the probe is a zero-read, so there is nothing
                // to drop here — but a record served past the budget is a record
                // the response cannot carry.
                records: Vec::new(),
                high_watermark: f.high_watermark,
                log_start_offset: f.log_start_offset,
                error,
            };
        }
    }
    answers
}

/// What one entry's answer costs to hold, near enough to spend a budget on.
///
/// The payloads are walked as the JSON they arrived as rather than re-serialised
/// or decoded twice; base64 makes that about a third larger than the record
/// bytes [`render`] will actually count, so the read side stops slightly EARLY,
/// which is the safe direction.
fn carried_bytes(f: &Fetched) -> usize {
    fn json_bytes(v: &serde_json::Value) -> usize {
        match v {
            serde_json::Value::String(s) => s.len(),
            serde_json::Value::Array(a) => a.iter().map(json_bytes).sum::<usize>() + 2,
            serde_json::Value::Object(o) => {
                o.iter()
                    .map(|(k, v)| k.len() + json_bytes(v))
                    .sum::<usize>()
                    + 2
            }
            _ => 8,
        }
    }
    f.records.iter().map(|r| json_bytes(&r.payload)).sum()
}

// -------------------------------------------------------------------- answers

/// Build the response from the staged slots and what the reads answered.
///
/// Failures are logged ONE LINE PER TOPIC, not one per partition, and that is
/// not tidiness: a consumer polls every few hundred milliseconds with its whole
/// assignment in one request, so a topic that was deleted under a 1024-lane
/// consumer is 1024 identical failures twice a second. Aggregating per topic
/// keeps the fact (which topic, how many lanes, what the log said, where its
/// bounds are) and drops only the repetition — the rule the broker's own
/// logging layer already follows.
fn render(
    req: &FetchRequest,
    slots: &[Vec<Slot>],
    read: &[queen::Result<Fetched>],
    budget: usize,
) -> FetchResponse {
    // Spent in request order across every topic of the response, because the
    // frame the codec refuses is the whole response and not one topic of it.
    let mut left = budget;
    // One number for the whole response, so a fetch chunked over several calls
    // to Queen answers the longest wait any of them was told to take.
    let mut throttle: Option<i32> = None;
    let mut responses = Vec::with_capacity(req.topics.len());
    for (topic, row) in req.topics.iter().zip(slots) {
        let name = topic.topic.0.as_str();
        let mut failures = Failures::default();
        let partitions = topic
            .partitions
            .iter()
            .zip(row)
            .map(|(p, slot)| match slot {
                Slot::Reject(e, why) => {
                    failures.refused(p.partition, why);
                    rejected(p.partition, *e)
                }
                // `get` and not an index: a handler must not panic on anything
                // a broker answered, however wrong it is.
                Slot::Read(i) => match read.get(*i) {
                    Some(Ok(f)) => match f.error.as_deref().filter(|e| !e.is_empty()) {
                        Some(marker) => {
                            failures.refused_by_log(p.partition, marker, f);
                            rejected(p.partition, entry_error(marker))
                        }
                        None => served(name, p.partition, f, &mut left),
                    },
                    Some(Err(e)) => match throttle::for_error(e) {
                        // Cloud back-pressure. NOT an error to the consumer:
                        // an empty read with `throttle_time_ms` beside it makes
                        // it sleep and poll again, where an error code would
                        // also cost a metadata refresh the capped tenant cannot
                        // afford. See [`crate::throttle`].
                        Some(ms) => {
                            throttle = throttle::longest(throttle, Some(ms));
                            failures.throttled(p.partition, e);
                            empty(p.partition)
                        }
                        None => {
                            failures.unread(p.partition, e);
                            rejected(p.partition, kafka_error(e))
                        }
                    },
                    None => rejected(p.partition, ResponseError::UnknownServerError),
                },
            })
            .collect();
        failures.log(name);
        responses.push(
            FetchableTopicResponse::default()
                .with_topic(TopicName(StrBytes::from_string(name.to_string())))
                .with_partitions(partitions),
        );
    }
    FetchResponse::default()
        .with_responses(responses)
        .with_throttle_time_ms(throttle.unwrap_or(0))
}

/// One topic's failures, kept as counts plus one worked example each, so a wide
/// fetch produces one log line instead of one per lane. See [`render`].
#[derive(Default)]
struct Failures {
    /// Refused before Queen was asked (a name rule, a negative index or offset).
    refused: usize,
    refused_example: Option<(i32, String)>,
    /// The log answered, and said no.
    from_log: usize,
    from_log_example: Option<(i32, String, i64, i64)>,
    /// One of those markers is not one this build knows, which means the broker
    /// grew an answer with no mapping here. It changes the level of the line
    /// rather than adding one, so it cannot flood either.
    unmapped_marker: bool,
    /// The call to Queen did not answer at all.
    unread: usize,
    unread_example: Option<(i32, String)>,
    /// The tenant is capped or frozen. Its own counter, because it is the one
    /// failure here that is not a fault: the consumer is being asked to wait,
    /// and an operator reading "fetch failed" would go looking for a break.
    throttled: usize,
    throttled_example: Option<(i32, String)>,
}

impl Failures {
    fn refused(&mut self, partition: i32, why: &str) {
        self.refused += 1;
        self.refused_example
            .get_or_insert_with(|| (partition, why.to_string()));
    }

    fn refused_by_log(&mut self, partition: i32, marker: &str, f: &Fetched) {
        self.from_log += 1;
        self.unmapped_marker |= !matches!(
            marker,
            queen::FETCH_ERR_OUT_OF_RANGE | queen::FETCH_ERR_UNKNOWN
        );
        self.from_log_example.get_or_insert_with(|| {
            (
                partition,
                marker.to_string(),
                f.high_watermark,
                f.log_start_offset,
            )
        });
    }

    fn unread(&mut self, partition: i32, e: &queen::Error) {
        self.unread += 1;
        self.unread_example
            .get_or_insert_with(|| (partition, e.to_string()));
    }

    fn throttled(&mut self, partition: i32, e: &queen::Error) {
        self.throttled += 1;
        self.throttled_example
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
                "fetch refused before reading"
            );
        }
        if let Some((partition, marker, high, log_start)) = &self.from_log_example {
            // OFFSET_OUT_OF_RANGE is the one a consumer meets in normal
            // operation (its position fell behind retention) and the one the
            // plan calls out: get it wrong and the consumer loops for ever
            // instead of resetting. At info, with the bounds it was refused
            // against, because that is the line an operator needs when a
            // consumer reports data loss — and at ERROR when the marker is one
            // this build has no mapping for, which is a broker that grew an
            // answer the facade has to learn.
            if self.unmapped_marker {
                tracing::error!(
                    target: "kafka",
                    topic,
                    partitions = self.from_log,
                    partition,
                    marker,
                    high_watermark = high,
                    log_start_offset = log_start,
                    "unmapped fetch error marker"
                );
            } else {
                tracing::info!(
                    target: "kafka",
                    topic,
                    partitions = self.from_log,
                    partition,
                    marker,
                    high_watermark = high,
                    log_start_offset = log_start,
                    "fetch entries refused by the log"
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
                "fetch failed"
            );
        }
        if let Some((partition, error)) = &self.throttled_example {
            // At warn and not at error: nothing is broken, and the consumer
            // has already been told to wait. It is here at all because a
            // consumer that is quietly making no progress is otherwise
            // indistinguishable from one with nothing to read.
            tracing::warn!(
                target: "kafka",
                topic,
                partitions = self.throttled,
                partition,
                %error,
                "fetch throttled; the consumer was answered empty and told to wait"
            );
        }
    }
}

/// One partition that was read: the bounds, and as much of the records as the
/// response has room left for. `topic` is carried only so [`batch`] can name it
/// in a log line.
///
/// `left` is the whole response's remaining budget and is spent here. At zero
/// the partition is answered as an empty read — error-free, with its true
/// watermarks, so the consumer knows exactly where it stands and comes back for
/// the records on its next poll.
fn served(topic: &str, index: i32, f: &Fetched, left: &mut usize) -> PartitionData {
    let records = if *left == 0 {
        Bytes::new()
    } else {
        batch(topic, index, &f.records, *left)
    };
    *left = left.saturating_sub(records.len());
    PartitionData::default()
        .with_partition_index(index)
        .with_error_code(0)
        .with_high_watermark(f.high_watermark)
        // With no transactions there is nothing undecided below the high
        // watermark, so the last stable offset IS it. A READ_COMMITTED consumer
        // reads up to this number and would see nothing at all if it were left
        // at the -1 the schema defaults to.
        .with_last_stable_offset(f.high_watermark)
        // v5+. Silently dropped by the encoder below v5.
        .with_log_start_offset(f.log_start_offset)
        // Empty rather than null: there are no transactions to abort, and an
        // empty list says so in a shape every client already walks.
        .with_aborted_transactions(Some(Vec::new()))
        .with_records(Some(records))
}

/// A partition that was not read and is not an error either: the throttle
/// answer (see [`crate::throttle`]).
///
/// Error code 0 with no records and the watermarks at `-1`. The watermarks are
/// unknown — the call that would have reported them was refused — and `-1` is
/// how a partition says so: every client guards its watermark handling on a
/// non-negative value (`CompletedFetch.initializeCompletedFetch` in the Java
/// consumer), so nothing is corrupted by not knowing them this poll.
fn empty(index: i32) -> PartitionData {
    PartitionData::default()
        .with_partition_index(index)
        .with_error_code(0)
        .with_high_watermark(NO_OFFSET)
        .with_last_stable_offset(NO_OFFSET)
        .with_log_start_offset(NO_OFFSET)
        .with_aborted_transactions(Some(Vec::new()))
        .with_records(Some(Bytes::new()))
}

/// A partition that was not read.
fn rejected(index: i32, error: ResponseError) -> PartitionData {
    PartitionData::default()
        .with_partition_index(index)
        .with_error_code(error.code())
        .with_high_watermark(NO_OFFSET)
        .with_last_stable_offset(NO_OFFSET)
        .with_log_start_offset(NO_OFFSET)
        .with_aborted_transactions(Some(Vec::new()))
        // Empty, not null: a client that reads the records before checking the
        // error code finds nothing rather than a null it may not expect.
        .with_records(Some(Bytes::new()))
}

/// Encode one entry's records as a single RecordBatch v2.
///
/// UNCOMPRESSED, whatever codec the producer used on the way in. That is legal
/// and not a shortcut: compression is a per-batch attribute chosen by whoever
/// writes the batch, the facade is writing this one, and every consumer decodes
/// all four codecs plus none. It is also the only option available — the
/// records were decompressed by the broker on the way into Queen and stored as
/// JSON payloads, so there is no original batch to hand back.
///
/// Each record's offset is the offset Queen assigned it, encoded as the batch's
/// base offset plus a per-record delta. Gaps are legal and are encoded as the
/// deltas they are: C2 skips a segment it cannot decode rather than failing the
/// whole read (server/src/handlers/fetch.rs), so a run of offsets with a hole
/// in it is a shape this really can produce, and a consumer that read it as
/// contiguous would commit positions that address other records.
///
/// The `sequence` written into each record is not the producer sequence of
/// anything — nothing here has one, and it never reaches the wire (a batch
/// carries one base sequence and derives the rest from the offset deltas). It
/// is set to `-1 + delta` for a mechanical reason: `RecordBatchEncoder` starts
/// a NEW batch whenever `offset - sequence` changes, so a constant difference
/// is what makes this one batch instead of one batch per record, and choosing
/// -1 for the first record is what makes the batch's base sequence the -1 that
/// says "no idempotent producer wrote this".
///
/// The headers are collected BESIDE the records rather than into them, and the
/// batch is written by [`wire::encode`] rather than by the crate's encoder
/// directly. A `Record`'s headers are a map keyed by the header NAME, and a
/// producer is allowed to send one name twice; a record that did would otherwise
/// come back to its consumer one pair short, having reached Queen intact. See
/// [`crate::wire`].
///
/// `budget` is what is left of the response, and the batch stops at the last
/// record that fits in it — except for the FIRST record, which always goes, so a
/// record larger than the whole budget is delivered rather than being asked for
/// for ever. Truncating is safe for the same reason the gap handling above is:
/// records are contiguous from the fetch offset, so the consumer resumes at the
/// first offset it did not get.
fn batch(topic: &str, index: i32, records: &[queen::FetchedRecord], budget: usize) -> Bytes {
    if records.is_empty() {
        return Bytes::new();
    }
    let mut room = budget.saturating_sub(BATCH_HEADER_BYTES);
    // The base is the LOWEST offset rather than the first, so every delta is
    // non-negative whatever order the log handed them over in.
    let base = records.iter().map(|r| r.offset).min().unwrap_or(0);
    let mut out: Vec<Record> = Vec::with_capacity(records.len());
    let mut headers: Vec<Vec<wire::Header>> = Vec::with_capacity(records.len());
    for r in records {
        // A delta that does not fit in an i32 cannot be encoded in a batch at
        // all, and the batch would fail whole. Stopping here instead answers
        // the prefix and leaves the rest for the consumer's next fetch, which
        // is the same truncation C2 already performs on its own budgets and is
        // safe for the same reason: the client resumes at the first offset it
        // did not get.
        if r.offset.saturating_sub(base) > i64::from(i32::MAX) {
            tracing::warn!(
                target: "kafka",
                topic,
                partition = index,
                base,
                offset = r.offset,
                "a record sits more than i32::MAX offsets past the batch base; \
                 the batch stops before it"
            );
            break;
        }
        let decoded = envelope::decode(&r.payload, r.timestamp_ms());
        let cost = record_bytes(&decoded);
        if !out.is_empty() && cost > room {
            // The response is full. Everything from here is this consumer's
            // next poll, which is what `max_bytes` asks for (KIP-74).
            break;
        }
        room = room.saturating_sub(cost);
        // The header list travels BESIDE the record, not inside it: `Record`
        // holds a map keyed by name, and a record whose producer sent one name
        // twice cannot be expressed in one. The map is left empty and
        // [`wire::encode`] writes the list — see [`crate::wire`].
        headers.push(decoded.headers);
        out.push(Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            // -1, the same "unknown epoch" Metadata advertises for every
            // partition: this facade holds no elections to number.
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            // CreateTime, never LogAppendTime: the timestamp is the producer's
            // own out of the envelope, and the facade never stamps one of its
            // own over it (`handlers::produce` reports no log-append time for
            // the same reason).
            timestamp_type: TimestampType::Creation,
            offset: r.offset,
            sequence: (-1i64 + (r.offset - base)) as i32,
            timestamp: decoded.timestamp,
            key: decoded.key,
            value: decoded.value,
            headers: IndexMap::new(),
        });
    }
    if out.is_empty() {
        return Bytes::new();
    }
    match wire::encode(&out, &headers) {
        Ok(bytes) => bytes,
        Err(e) => {
            // Nothing in the loop above can produce a record the encoder
            // refuses, so this is a bug rather than a client's doing. An empty
            // batch is the only answer that is not a lie: the consumer stays
            // where it is and retries.
            tracing::error!(
                target: "kafka",
                topic,
                partition = index,
                error = %e,
                records = out.len(),
                "cannot encode a record batch"
            );
            Bytes::new()
        }
    }
}

/// An upper bound on the bytes one record adds to an encoded batch.
///
/// Deliberately an OVER-estimate: it is what decides whether a record still fits
/// under the response budget, and the cost of guessing high is a slightly
/// shorter answer while the cost of guessing low is the frame the codec refuses.
fn record_bytes(d: &envelope::Decoded) -> usize {
    RECORD_FRAMING_BYTES
        + d.key.as_ref().map_or(0, |k| k.len())
        + d.value.as_ref().map_or(0, |v| v.len())
        + d.headers
            .iter()
            .map(|(name, value)| {
                HEADER_FRAMING_BYTES + name.len() + value.as_ref().map_or(0, |v| v.len())
            })
            .sum::<usize>()
}

/// The Kafka error for one of C2's per-entry markers.
///
/// A marker this build does not know becomes UNKNOWN_SERVER_ERROR rather than
/// the nearest-looking code: guessing would send a consumer down a recovery
/// path that may not apply. It is reported by [`Failures`], which raises the
/// whole topic's line to ERROR rather than adding one per partition.
fn entry_error(marker: &str) -> ResponseError {
    match marker {
        queen::FETCH_ERR_OUT_OF_RANGE => ResponseError::OffsetOutOfRange,
        queen::FETCH_ERR_UNKNOWN => ResponseError::UnknownTopicOrPartition,
        _ => ResponseError::UnknownServerError,
    }
}

/// The closest Kafka error for a failed call to Queen, on the fetch path.
///
/// The codes are chosen from the set a CONSUMER handles, which is narrower than
/// the producer's and is not a matter of taste: the Java consumer walks an
/// explicit list of fetch error codes and throws `IllegalStateException` on
/// anything outside it, so a plausible-looking code that is not on the list
/// kills the consumer instead of making it retry. NOT_LEADER_OR_FOLLOWER is
/// therefore the retriable answer here, where the produce path uses
/// LEADER_NOT_AVAILABLE — the two mean the same thing to this facade ("come
/// back, and refresh your metadata first"), and only one of them is a code a
/// fetching client is written to receive.
fn kafka_error(e: &queen::Error) -> ResponseError {
    match e {
        // No answer at all, including our own budget expiring. Retriable, and
        // it makes the client refresh metadata — which is the right move if
        // this facade is the one that is unwell.
        queen::Error::Transport(_) => ResponseError::NotLeaderOrFollower,
        queen::Error::Status { code, .. } => match code {
            // The token cannot read this topic. Fatal and named.
            401 | 403 => ResponseError::TopicAuthorizationFailed,
            404 => ResponseError::UnknownTopicOrPartition,
            // Cloud: a frozen or rate-capped tenant, and a draining or
            // unavailable broker. Both are "later, not never".
            429 | 502..=504 => ResponseError::NotLeaderOrFollower,
            // Anything else, including a 400 — which would mean the facade
            // built a body the broker rejected, our bug, and it should be loud.
            _ => ResponseError::UnknownServerError,
        },
        // A 2xx we could not read, or an answer that does not line up with the
        // entries we sent.
        queen::Error::Body(_) => ResponseError::UnknownServerError,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use crate::queen::{Error, FetchedRecord};
    use crate::records::Decoded;
    use bytes::BytesMut;
    use kafka_protocol::messages::fetch_request::{FetchPartition, FetchTopic};
    use kafka_protocol::protocol::{Decodable, Encodable, Message};
    use kafka_protocol::records::{
        Compression, RecordBatchDecoder, RecordBatchEncoder, RecordEncodeOptions, RecordSet,
        NO_TIMESTAMP,
    };
    use serde_json::json;
    use std::sync::Arc;

    // -------------------------------------------------------------- fixtures

    fn facade(queues: &[(&str, i64)]) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        let facade = crate::handlers::testing::over(api.clone(), Default::default());
        (facade, api)
    }

    /// `[(topic, [(partition, offset)])]` → a Fetch request with a client's
    /// usual ceilings.
    fn request(topics: &[(&str, &[(i32, i64)])]) -> FetchRequest {
        FetchRequest::default()
            .with_replica_id((-1).into())
            .with_max_wait_ms(500)
            .with_min_bytes(1)
            .with_max_bytes(50 * 1024 * 1024)
            .with_isolation_level(0)
            .with_topics(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        FetchTopic::default()
                            .with_topic(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partitions(
                                partitions
                                    .iter()
                                    .map(|(index, offset)| {
                                        FetchPartition::default()
                                            .with_partition(*index)
                                            .with_fetch_offset(*offset)
                                            .with_partition_max_bytes(1024 * 1024)
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    fn answer<'a>(resp: &'a FetchResponse, topic: &str, partition: i32) -> &'a PartitionData {
        resp.responses
            .iter()
            .find(|t| t.topic.0.as_str() == topic)
            .unwrap_or_else(|| panic!("{topic} is not in the response"))
            .partitions
            .iter()
            .find(|p| p.partition_index == partition)
            .unwrap_or_else(|| panic!("{topic}/{partition} is not in the response"))
    }

    /// Decode a partition's records the way a CLIENT does — through
    /// `kafka-protocol`'s decoder, which is the same code every Rust Kafka
    /// consumer runs.
    fn decode(p: &PartitionData) -> Vec<RecordSet> {
        let mut raw = p.records.clone().expect("records are never null");
        if raw.is_empty() {
            return Vec::new();
        }
        RecordBatchDecoder::decode_all(&mut raw).expect("the client side decodes it")
    }

    fn records(p: &PartitionData) -> Vec<Record> {
        decode(p).into_iter().flat_map(|s| s.records).collect()
    }

    /// The envelope a produced record becomes, so a fake log can be seeded with
    /// what the produce path would have written.
    fn envelope_of(key: Option<&[u8]>, value: &[u8], timestamp: i64) -> serde_json::Value {
        envelope::encode(
            &Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: -1,
                timestamp,
                key: key.map(Bytes::copy_from_slice),
                value: Some(Bytes::copy_from_slice(value)),
                headers: IndexMap::new(),
            },
            None,
        )
    }

    // ------------------------------------------------------- the happy path

    #[tokio::test]
    async fn stored_records_come_back_as_a_batch_at_their_own_offsets() {
        let (f, api) = facade(&[("orders", 4)]);
        api.seed(
            "orders",
            "2",
            0,
            &[
                envelope_of(Some(b"a"), b"one", 1_756_000_000_000),
                envelope_of(None, b"two", 1_756_000_000_001),
                envelope_of(Some(b"c"), b"three", 1_756_000_000_002),
            ],
        );

        let resp = handle(&f, &request(&[("orders", &[(2, 0)])]), None).await;
        let p = answer(&resp, "orders", 2);
        assert_eq!(p.error_code, 0);
        assert_eq!(p.high_watermark, 3);
        assert_eq!(
            p.last_stable_offset, 3,
            "no transactions: the LSO is the HW"
        );
        assert_eq!(p.log_start_offset, 0);
        assert_eq!(p.aborted_transactions, Some(Vec::new()));

        // ONE batch, and the records at the offsets Queen assigned them.
        let sets = decode(p);
        assert_eq!(sets.len(), 1, "not one batch");
        let got = records(p);
        assert_eq!(got.len(), 3);
        assert_eq!(
            got.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [0, 1, 2],
            "offsets are not the log's"
        );
        assert_eq!(got[0].key, Some(Bytes::from_static(b"a")));
        assert_eq!(got[0].value, Some(Bytes::from_static(b"one")));
        assert_eq!(got[0].timestamp, 1_756_000_000_000);
        assert_eq!(got[1].key, None, "a null key stays null");
        assert_eq!(got[2].value, Some(Bytes::from_static(b"three")));
        // Nothing in the batch claims a producer or a transaction.
        for r in &got {
            assert_eq!(r.producer_id, NO_PRODUCER_ID);
            assert!(!r.transactional);
            assert!(!r.control);
        }
    }

    /// A fetch from partway through the log starts where it was asked to, and
    /// the batch's base offset is that record's.
    #[tokio::test]
    async fn a_fetch_from_the_middle_starts_at_the_offset_it_asked_for() {
        let (f, api) = facade(&[("orders", 1)]);
        let payloads: Vec<serde_json::Value> = (0..5)
            .map(|i| envelope_of(None, format!("v{i}").as_bytes(), 1))
            .collect();
        api.seed("orders", "0", 0, &payloads);

        let resp = handle(&f, &request(&[("orders", &[(0, 3)])]), None).await;
        let got = records(answer(&resp, "orders", 0));
        assert_eq!(got.iter().map(|r| r.offset).collect::<Vec<_>>(), [3, 4]);
        assert_eq!(got[0].value, Some(Bytes::from_static(b"v3")));
    }

    /// The round trip that is the whole milestone: produce through the facade,
    /// then fetch it back and get the same records at the offsets produce
    /// answered.
    #[tokio::test]
    async fn what_produce_wrote_is_what_fetch_reads() {
        use crate::handlers::produce;
        use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
        use kafka_protocol::messages::ProduceRequest;

        let (f, _) = facade(&[("orders", 4)]);
        let written: Vec<Record> = [(Some(&b"k1"[..]), &b"one"[..]), (None, &b"two"[..])]
            .iter()
            .map(|(k, v)| Record {
                transactional: false,
                control: false,
                delete_horizon: false,
                partition_leader_epoch: -1,
                producer_id: NO_PRODUCER_ID,
                producer_epoch: NO_PRODUCER_EPOCH,
                timestamp_type: TimestampType::Creation,
                offset: 0,
                sequence: -1,
                timestamp: 1_756_000_000_000,
                key: k.map(Bytes::copy_from_slice),
                value: Some(Bytes::copy_from_slice(v)),
                headers: {
                    let mut h = IndexMap::new();
                    h.insert(
                        StrBytes::from_static_str("trace-id"),
                        Some(Bytes::from_static(b"abc")),
                    );
                    h
                },
            })
            .collect();
        let mut batch_bytes = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut batch_bytes,
            written.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression: Compression::Gzip,
            },
        )
        .unwrap();

        let produced = produce::handle(
            &f,
            &ProduceRequest::default()
                .with_acks(-1)
                .with_topic_data(vec![TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partition_data(vec![PartitionProduceData::default()
                        .with_index(1)
                        .with_records(Some(batch_bytes.freeze()))])]),
            None,
        )
        .await
        .expect("acks=-1 is answered");
        let base = produced.responses[0].partition_responses[0].base_offset;
        assert_eq!(base, 0);

        let resp = handle(&f, &request(&[("orders", &[(1, base)])]), None).await;
        let got = records(answer(&resp, "orders", 1));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].key, Some(Bytes::from_static(b"k1")));
        assert_eq!(got[0].value, Some(Bytes::from_static(b"one")));
        assert_eq!(got[0].timestamp, 1_756_000_000_000);
        assert_eq!(
            got[0].headers.get(&StrBytes::from_static_str("trace-id")),
            Some(&Some(Bytes::from_static(b"abc")))
        );
        assert_eq!(got[1].key, None);
        assert_eq!(got[1].value, Some(Bytes::from_static(b"two")));
    }

    /// The same round trip for the one thing `kafka-protocol` cannot carry: a
    /// header NAME sent twice. Produced through the facade and fetched back,
    /// both values are on the wire, in the order the producer sent them.
    ///
    /// The batch is read back with [`wire::header_lists`] because the crate's
    /// decoder — the one every Rust client runs — collapses the repeat into its
    /// own map on the way in; that collapse is the client's business, and what
    /// this pins is that the facade puts both pairs on the wire for a client
    /// (kafkajs, the Java consumer, franz-go) that keeps them.
    #[tokio::test]
    async fn a_repeated_header_name_survives_produce_and_fetch() {
        use crate::handlers::produce;
        use kafka_protocol::messages::produce_request::{PartitionProduceData, TopicProduceData};
        use kafka_protocol::messages::ProduceRequest;

        let (f, _) = facade(&[("orders", 4)]);
        let sent: Vec<wire::Header> = vec![
            ("x".to_string(), Some(Bytes::from_static(b"1"))),
            ("y".to_string(), Some(Bytes::from_static(b"solo"))),
            ("x".to_string(), Some(Bytes::from_static(b"2"))),
        ];
        let written = [Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: NO_PRODUCER_ID,
            producer_epoch: NO_PRODUCER_EPOCH,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1_756_000_000_000,
            key: None,
            value: Some(Bytes::from_static(b"v")),
            headers: IndexMap::new(),
        }];
        let batch_bytes =
            wire::encode(&written, std::slice::from_ref(&sent)).expect("the fixture encodes");

        let produced = produce::handle(
            &f,
            &ProduceRequest::default()
                .with_acks(-1)
                .with_topic_data(vec![TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partition_data(vec![PartitionProduceData::default()
                        .with_index(1)
                        .with_records(Some(batch_bytes))])]),
            None,
        )
        .await
        .expect("acks=-1 is answered");
        assert_eq!(produced.responses[0].partition_responses[0].error_code, 0);

        let resp = handle(&f, &request(&[("orders", &[(1, 0)])]), None).await;
        let p = answer(&resp, "orders", 1);
        let served = p.records.clone().expect("records are never null");
        let decoded = records(p);
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].value, Some(Bytes::from_static(b"v")));
        // 61 bytes of batch header, then the records section.
        assert_eq!(
            wire::header_lists(&served[61..], &decoded),
            Some(vec![sent]),
            "the fetch response does not carry both `x` headers"
        );
    }

    /// A gap in the offsets — what a segment C2 could not decode leaves behind
    /// — is encoded as the deltas it is, not smoothed into a contiguous run.
    #[tokio::test]
    async fn a_batch_with_offset_gaps_keeps_every_offset() {
        let (f, api) = facade(&[("orders", 1)]);
        api.reply_fetch(vec![Fetched {
            records: vec![
                FetchedRecord {
                    offset: 100,
                    payload: envelope_of(None, b"a", 5),
                    ts: None,
                },
                FetchedRecord {
                    offset: 104,
                    payload: envelope_of(None, b"b", 6),
                    ts: None,
                },
                FetchedRecord {
                    offset: 105,
                    payload: envelope_of(None, b"c", 7),
                    ts: None,
                },
            ],
            high_watermark: 106,
            log_start_offset: 100,
            error: None,
        }]);

        let resp = handle(&f, &request(&[("orders", &[(0, 100)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(decode(p).len(), 1, "a gap must not split the batch");
        let got = records(p);
        assert_eq!(
            got.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [100, 104, 105]
        );
        assert_eq!(got[1].value, Some(Bytes::from_static(b"b")));
        // The batch's own header: base offset 100 and a last delta of 5, which
        // is what a client reads to know how far the batch reaches.
        let mut raw = p.records.clone().unwrap();
        let infos = RecordBatchDecoder::decode_batch_info(&mut raw).unwrap();
        assert_eq!(infos.len(), 1);
        assert_eq!(infos[0].min_offset, 100);
        assert_eq!(infos[0].record_count, 3);
        // No idempotent producer wrote this, and the batch says so.
        assert_eq!(infos[0].base_sequence, -1);
        assert_eq!(infos[0].producer_id, NO_PRODUCER_ID);
    }

    /// A gap wider than a batch can express — an offset delta is an i32 — is
    /// truncated rather than encoded wrongly or dropped whole. The consumer
    /// gets the prefix and resumes past it, which is the same rule C2 applies
    /// to its own budgets.
    #[tokio::test]
    async fn a_gap_too_wide_for_a_batch_truncates_instead_of_lying() {
        let (f, api) = facade(&[("orders", 1)]);
        let far = i64::from(i32::MAX) + 2;
        api.reply_fetch(vec![Fetched {
            records: vec![
                FetchedRecord {
                    offset: 0,
                    payload: envelope_of(None, b"near", 1),
                    ts: None,
                },
                FetchedRecord {
                    offset: far,
                    payload: envelope_of(None, b"far", 2),
                    ts: None,
                },
            ],
            high_watermark: far + 1,
            log_start_offset: 0,
            error: None,
        }]);

        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, 0, "truncation is not a failure");
        let got = records(p);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].offset, 0);
        assert_eq!(got[0].value, Some(Bytes::from_static(b"near")));
        // The watermark is still the truth, so the client knows there is more.
        assert_eq!(p.high_watermark, far + 1);
    }

    /// A payload a NATIVE Queen producer wrote is servable: no key, the JSON
    /// itself as the value, and the log's own timestamp. This is the interop
    /// half — native producers in, Kafka consumers out.
    #[tokio::test]
    async fn a_native_payload_is_served_to_a_kafka_consumer() {
        let (f, api) = facade(&[("orders", 1)]);
        api.seed(
            "orders",
            "0",
            0,
            &[json!({"orderId": 7, "total": 10.5}), json!([1, 2, 3])],
        );

        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        let got = records(answer(&resp, "orders", 0));
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].key, None);
        let text = String::from_utf8(got[0].value.clone().unwrap().to_vec()).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap(),
            json!({"orderId": 7, "total": 10.5})
        );
        assert_eq!(
            String::from_utf8(got[1].value.clone().unwrap().to_vec()).unwrap(),
            "[1,2,3]"
        );
        // The fake stamps every segment 2026-08-27T10:00:00.000000Z, and that
        // is the timestamp a native record carries.
        assert_eq!(got[0].timestamp, 1_787_824_800_000);
    }

    /// ...and the envelope's own timestamp is never overwritten by the log's.
    #[tokio::test]
    async fn a_produced_timestamp_beats_the_segments() {
        let (f, api) = facade(&[("orders", 1)]);
        api.seed("orders", "0", 0, &[envelope_of(None, b"v", 42)]);
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        assert_eq!(records(answer(&resp, "orders", 0))[0].timestamp, 42);
    }

    /// A record produced without a timestamp, stored in a log whose segment
    /// timestamp is unreadable, comes back as "unknown" rather than as a
    /// guessed instant.
    #[tokio::test]
    async fn an_unparseable_segment_timestamp_is_no_timestamp() {
        let (f, api) = facade(&[("orders", 1)]);
        api.reply_fetch(vec![Fetched {
            records: vec![FetchedRecord {
                offset: 0,
                payload: envelope_of(None, b"v", NO_TIMESTAMP),
                ts: Some("whenever".to_string()),
            }],
            high_watermark: 1,
            log_start_offset: 0,
            error: None,
        }]);
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        assert_eq!(
            records(answer(&resp, "orders", 0))[0].timestamp,
            NO_TIMESTAMP
        );
    }

    // ------------------------------------------------------------ empty logs

    /// The shape a fresh consumer meets: a topic advertised at 1024 lanes that
    /// nothing has ever been pushed to. Every lane is an empty log at offset 0,
    /// and not one of them is an error — a single error entry would defeat the
    /// long poll for the whole batch.
    #[tokio::test]
    async fn a_fresh_topic_fetches_as_empty_logs_at_offset_zero() {
        // A topic that IS 1024 lanes wide and has had nothing written to it —
        // which is what the width check is about: every lane below the
        // advertised width is an empty log, not a missing one.
        let (f, _) = facade(&[("orders", 1024)]);
        let lanes: Vec<(i32, i64)> = (0..1024).map(|i| (i, 0)).collect();
        let resp = handle(&f, &request(&[("orders", &lanes)]), None).await;
        assert_eq!(resp.responses[0].partitions.len(), 1024);
        for p in &resp.responses[0].partitions {
            assert_eq!(p.error_code, 0, "lane {}", p.partition_index);
            assert_eq!(p.high_watermark, 0);
            assert_eq!(p.log_start_offset, 0);
            assert!(p.records.as_ref().unwrap().is_empty());
        }
    }

    /// A caught-up partition: valid, empty, and no error — the entry the long
    /// poll parks on.
    #[tokio::test]
    async fn fetching_at_the_high_watermark_is_empty_and_fine() {
        let (f, api) = facade(&[("orders", 1)]);
        api.seed("orders", "0", 0, &[envelope_of(None, b"v", 1)]);
        let resp = handle(&f, &request(&[("orders", &[(0, 1)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, 0);
        assert_eq!(p.high_watermark, 1);
        assert!(p.records.as_ref().unwrap().is_empty());
    }

    // --------------------------------------------------------------- errors

    /// The one the plan calls out: a fetch below the retention watermark has to
    /// be OFFSET_OUT_OF_RANGE, or the consumer loops for ever.
    #[tokio::test]
    async fn a_fetch_below_the_retention_watermark_is_out_of_range() {
        let (f, api) = facade(&[("orders", 1)]);
        // Retention has taken everything below offset 500.
        api.seed("orders", "0", 500, &[envelope_of(None, b"v", 1)]);

        let resp = handle(&f, &request(&[("orders", &[(0, 12)])]), None).await;
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::OffsetOutOfRange.code());
        assert!(p.records.as_ref().unwrap().is_empty());
        // An errored partition carries no watermarks a client could act on.
        assert_eq!(p.high_watermark, -1);
        assert_eq!(p.log_start_offset, -1);

        // ...and reading from the watermark itself works.
        let ok = handle(&f, &request(&[("orders", &[(0, 500)])]), None).await;
        assert_eq!(answer(&ok, "orders", 0).error_code, 0);
    }

    #[tokio::test]
    async fn a_fetch_past_the_end_is_out_of_range_too() {
        let (f, api) = facade(&[("orders", 1)]);
        api.seed("orders", "0", 0, &[envelope_of(None, b"v", 1)]);
        let resp = handle(&f, &request(&[("orders", &[(0, 99)])]), None).await;
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::OffsetOutOfRange.code()
        );
    }

    #[tokio::test]
    async fn an_unknown_topic_is_unknown() {
        let (f, _) = facade(&[]);
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
    }

    /// A `__` topic is as invisible here as it is in Metadata, and it never
    /// reaches Queen — a native queue by that name must not become readable by
    /// naming it directly.
    #[tokio::test]
    async fn a_reserved_topic_is_unknown_and_never_read() {
        let (f, api) = facade(&[("__consumer_offsets", 4)]);
        api.seed("__consumer_offsets", "0", 0, &[envelope_of(None, b"v", 1)]);
        let resp = handle(&f, &request(&[("__consumer_offsets", &[(0, 0)])]), None).await;
        assert_eq!(
            answer(&resp, "__consumer_offsets", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(
            api.fetches.lock().unwrap().is_empty(),
            "a __ topic was read"
        );
    }

    /// An unnameable topic is UNKNOWN here and not INVALID_TOPIC_EXCEPTION,
    /// which Metadata answers for the same name. The Java consumer's fetch path
    /// walks a closed set of per-partition codes and throws
    /// `IllegalStateException` on anything outside it — INVALID_TOPIC_EXCEPTION
    /// is outside it, so the honest-looking code would kill the consumer where
    /// "there is no such topic" makes it recover. See
    /// [`metadata::not_a_topic_here`] and `compat/ERRORS.md`.
    #[tokio::test]
    async fn an_unnameable_topic_is_unknown_not_invalid() {
        let (f, api) = facade(&[]);
        let resp = handle(&f, &request(&[("not a topic", &[(0, 0)])]), None).await;
        assert_eq!(
            answer(&resp, "not a topic", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_ne!(
            answer(&resp, "not a topic", 0).error_code,
            ResponseError::InvalidTopicException.code(),
            "a code outside the consumer's fetch set is an IllegalStateException out of poll()"
        );
        assert!(api.fetches.lock().unwrap().is_empty());
    }

    /// The whole closed set, in one place: every per-partition code this handler
    /// can put on the wire is one the Java consumer's fetch path accepts. The
    /// list is `FetchCollector`'s (`initializeCompletedFetch`), and a code
    /// outside it is an `IllegalStateException` rather than a retry.
    #[tokio::test]
    async fn every_code_this_handler_emits_is_one_a_consumer_accepts() {
        const ACCEPTED: &[ResponseError] = &[
            ResponseError::OffsetOutOfRange,
            ResponseError::UnknownTopicOrPartition,
            ResponseError::TopicAuthorizationFailed,
            ResponseError::NotLeaderOrFollower,
            ResponseError::UnknownServerError,
            ResponseError::CorruptMessage,
            ResponseError::ReplicaNotAvailable,
            ResponseError::KafkaStorageError,
            ResponseError::FencedLeaderEpoch,
            ResponseError::UnknownLeaderEpoch,
            ResponseError::OffsetNotAvailable,
            ResponseError::UnknownTopicId,
            ResponseError::InconsistentTopicId,
        ];
        let emitted = [
            // Every arm of `stage`.
            stage(&mut Vec::new(), "__consumer_offsets", 0, None, 0, 1),
            stage(&mut Vec::new(), "not a topic", 0, None, 0, 1),
            stage(&mut Vec::new(), "orders", -1, None, 0, 1),
            stage(&mut Vec::new(), "orders", 8, Some(8), 0, 1),
            stage(&mut Vec::new(), "orders", 0, None, -1, 1),
        ]
        .into_iter()
        .filter_map(|slot| match slot {
            Slot::Reject(e, _) => Some(e),
            Slot::Read(_) => None,
        })
        // ...every marker the log can answer an entry with, mapped.
        .chain(
            [
                queen::FETCH_ERR_OUT_OF_RANGE,
                queen::FETCH_ERR_UNKNOWN,
                "SOMETHING_THIS_BUILD_HAS_NEVER_HEARD_OF",
            ]
            .map(entry_error),
        )
        // ...and every failure of the call itself.
        .chain(
            [
                Error::Transport("down".into()),
                Error::Body("nonsense".into()),
            ]
            .map(|e| kafka_error(&e)),
        )
        .chain(
            [400, 401, 403, 404, 408, 429, 500, 502, 503, 504].map(|code| {
                kafka_error(&Error::Status {
                    code,
                    body: String::new(),
                    retry_after_ms: None,
                })
            }),
        );
        for e in emitted {
            assert!(
                ACCEPTED.contains(&e),
                "{e:?} is not a code the Java consumer's fetch path accepts"
            );
        }
    }

    /// The poison pill: a negative offset makes C2 reject the WHOLE batch, so
    /// it is refused per partition and the others are read normally.
    #[tokio::test]
    async fn a_negative_offset_is_refused_without_poisoning_the_request() {
        let (f, api) = facade(&[("orders", 4)]);
        api.seed("orders", "1", 0, &[envelope_of(None, b"v", 1)]);

        let resp = handle(&f, &request(&[("orders", &[(0, -1), (1, 0)])]), None).await;
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::OffsetOutOfRange.code()
        );
        assert_eq!(answer(&resp, "orders", 1).error_code, 0);
        assert_eq!(records(answer(&resp, "orders", 1)).len(), 1);
        // Only the readable lane was sent to Queen.
        assert_eq!(api.fetched().len(), 1);
        assert_eq!(api.fetched()[0].partition, "1");
    }

    #[tokio::test]
    async fn a_negative_partition_index_is_unknown() {
        let (f, api) = facade(&[("orders", 4)]);
        let resp = handle(&f, &request(&[("orders", &[(-1, 0)])]), None).await;
        assert_eq!(
            answer(&resp, "orders", -1).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(api.fetches.lock().unwrap().is_empty());
    }

    /// A partition index at or past the width Metadata advertises is not a lane
    /// this facade has, and is answered as one it does not have.
    ///
    /// It is the read half of a rule the WRITE path already had: a produce to
    /// partition 8 of an eight-lane topic is UNKNOWN_TOPIC_OR_PARTITION, and a
    /// fetch of it used to be an empty read with high watermark 0 and no error —
    /// a ghost partition a consumer with a stale assignment would poll for ever
    /// instead of refreshing its metadata. Nothing is asked of Queen for it
    /// either: a lane that does not exist is not a read to make.
    #[tokio::test]
    async fn a_partition_past_the_advertised_width_is_unknown() {
        let (f, api) = facade(&[("orders", 8)]);
        let resp = handle(
            &f,
            &request(&[("orders", &[(7, 0), (8, 0), (4242, 0)])]),
            None,
        )
        .await;

        assert_eq!(
            answer(&resp, "orders", 7).error_code,
            0,
            "the last real lane"
        );
        for ghost in [8, 4242] {
            let p = answer(&resp, "orders", ghost);
            assert_eq!(
                p.error_code,
                ResponseError::UnknownTopicOrPartition.code(),
                "lane {ghost} was served as an empty partition"
            );
            // Kafka answers a refused partition with no bounds at all, and a
            // client reads a watermark only beside error code 0.
            assert_eq!(p.high_watermark, NO_OFFSET, "lane {ghost}");
            assert_eq!(p.log_start_offset, NO_OFFSET, "lane {ghost}");
            assert_eq!(p.last_stable_offset, NO_OFFSET, "lane {ghost}");
            assert!(p.records.as_ref().unwrap().is_empty(), "lane {ghost}");
        }
        assert_eq!(
            api.fetched().len(),
            1,
            "a lane that does not exist was read from Queen"
        );
    }

    /// ...and the check never becomes a reason a fetch fails: with the queue
    /// list unreadable there is no width to check against, and the read is
    /// served exactly as it was before there was a check at all. A blip in the
    /// admin API is not an outage on the data path.
    #[tokio::test]
    async fn an_unreadable_catalog_costs_the_width_check_and_not_the_read() {
        let (f, api) = facade(&[("orders", 8)]);
        api.seed("orders", "2", 0, &[envelope_of(None, b"v", 1)]);
        api.fail_list(Error::status(503, "draining"));

        let resp = handle(&f, &request(&[("orders", &[(2, 0)])]), None).await;
        let p = answer(&resp, "orders", 2);
        assert_eq!(p.error_code, 0);
        assert_eq!(records(p).len(), 1);
    }

    /// Every HTTP failure maps to a code a CONSUMER handles — the Java consumer
    /// throws on a fetch error code outside its own list, so a plausible code
    /// that is not on it kills the client instead of making it retry.
    #[tokio::test]
    async fn a_failed_read_maps_to_a_code_consumers_handle() {
        let cases: [(Error, ResponseError); 5] = [
            (
                Error::Transport("connection reset".into()),
                ResponseError::NotLeaderOrFollower,
            ),
            (
                Error::status(401, "no token"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(404, "gone"),
                ResponseError::UnknownTopicOrPartition,
            ),
            // 429 is deliberately NOT here: a throttled tenant is answered
            // empty-and-wait rather than with an error code. See
            // `a_throttled_fetch_is_an_empty_read_and_a_wait`.
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
            let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
            let p = answer(&resp, "orders", 0);
            assert_eq!(p.error_code, kafka.code(), "{queen_error}");
            assert_eq!(p.high_watermark, -1, "{queen_error}");
        }
    }

    /// An error marker this build has no mapping for must not become a specific
    /// code a consumer would act on.
    #[tokio::test]
    async fn an_unknown_error_marker_is_a_server_error() {
        let (f, api) = facade(&[("orders", 1)]);
        api.reply_fetch(vec![Fetched {
            records: Vec::new(),
            high_watermark: 0,
            log_start_offset: 0,
            error: Some("SOMETHING_NEW".to_string()),
        }]);
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::UnknownServerError.code()
        );
    }

    // ------------------------------------------------------------- batching

    /// One request over several topics and partitions is ONE call to Queen,
    /// carrying every named lane.
    #[tokio::test]
    async fn one_request_is_one_call_across_every_topic() {
        let (f, api) = facade(&[("orders", 4), ("clicks", 4)]);
        api.seed("orders", "0", 0, &[envelope_of(None, b"o", 1)]);
        api.seed("clicks", "2", 0, &[envelope_of(None, b"c", 1)]);

        let resp = handle(
            &f,
            &request(&[("orders", &[(0, 0), (3, 0)]), ("clicks", &[(2, 0)])]),
            None,
        )
        .await;

        assert_eq!(api.fetches.lock().unwrap().len(), 1, "not one call");
        let asked = api.fetched();
        assert_eq!(asked.len(), 3);
        assert_eq!(
            asked
                .iter()
                .map(|e| (e.queue.as_str(), e.partition.as_str()))
                .collect::<Vec<_>>(),
            [("orders", "0"), ("orders", "3"), ("clicks", "2")]
        );
        assert_eq!(
            records(answer(&resp, "orders", 0))[0].value.as_deref(),
            Some(&b"o"[..])
        );
        assert_eq!(
            records(answer(&resp, "clicks", 2))[0].value.as_deref(),
            Some(&b"c"[..])
        );
        assert!(records(answer(&resp, "orders", 3)).is_empty());
    }

    /// The long poll is forwarded as the client set it, clamped to what the
    /// broker will honour.
    #[tokio::test]
    async fn the_long_poll_is_forwarded_and_clamped() {
        for (asked, forwarded) in [
            (0i32, 0i64),
            (500, 500),
            (30_000, 30_000),
            (120_000, 30_000),
        ] {
            let (f, api) = facade(&[("orders", 1)]);
            let req = request(&[("orders", &[(0, 0)])])
                .with_max_wait_ms(asked)
                .with_min_bytes(4096);
            handle(&f, &req, None).await;
            let calls = api.fetches.lock().unwrap().clone();
            assert_eq!(calls[0].1, forwarded, "maxWaitMs for {asked}");
            assert_eq!(calls[0].2, 4096, "minBytes for {asked}");
        }
    }

    /// A negative `min_bytes` or `max_wait_ms` from a client is floored rather
    /// than sent on: the broker takes the absolute value of neither.
    #[tokio::test]
    async fn negative_long_poll_values_are_floored() {
        let (f, api) = facade(&[("orders", 1)]);
        let req = request(&[("orders", &[(0, 0)])])
            .with_max_wait_ms(-5)
            .with_min_bytes(-9);
        handle(&f, &req, None).await;
        let calls = api.fetches.lock().unwrap().clone();
        assert_eq!((calls[0].1, calls[0].2), (0, 0));
    }

    /// An assignment wider than the broker's entry ceiling is chunked rather
    /// than sent whole — the broker rejects an over-long list with a 400 for
    /// the WHOLE batch, which would be the consumer's entire poll failing.
    #[tokio::test]
    async fn an_assignment_wider_than_the_entry_ceiling_is_chunked() {
        let (f, api) = facade(&[("orders", 1024), ("clicks", 1024)]);
        let lanes: Vec<(i32, i64)> = (0..1024).map(|i| (i, 0)).collect();
        let resp = handle(
            &f,
            &request(&[("orders", &lanes), ("clicks", &lanes)]),
            None,
        )
        .await;

        let calls = api.fetches.lock().unwrap().clone();
        assert_eq!(calls.len(), 2, "not chunked");
        assert_eq!(calls[0].0.len(), queen::MAX_FETCH_ENTRIES);
        assert_eq!(calls[1].0.len(), queen::MAX_FETCH_ENTRIES);
        // The park belongs to the first chunk, so the request still returns
        // within ONE max_wait_ms rather than a multiple of it.
        assert_eq!(calls[0].1, 500);
        assert_eq!(calls[1].1, 0);
        // Every lane is still answered, in the order it was asked.
        assert_eq!(resp.responses.len(), 2);
        for t in &resp.responses {
            assert_eq!(t.partitions.len(), 1024);
            for (i, p) in t.partitions.iter().enumerate() {
                assert_eq!(p.partition_index, i as i32);
                assert_eq!(p.error_code, 0);
            }
        }
    }

    /// A chunk that fails takes only its own lanes down.
    #[tokio::test]
    async fn a_failed_chunk_does_not_poison_the_others() {
        let (f, api) = facade(&[("orders", 1024), ("clicks", 1024)]);
        let lanes: Vec<(i32, i64)> = (0..1024).map(|i| (i, 0)).collect();
        // The FIRST call fails; the second is served from the fake log.
        api.fail_fetch(Error::Transport("reset".into()));
        let resp = handle(
            &f,
            &request(&[("orders", &lanes), ("clicks", &lanes)]),
            None,
        )
        .await;

        assert_eq!(
            answer(&resp, "orders", 7).error_code,
            ResponseError::NotLeaderOrFollower.code()
        );
        assert_eq!(answer(&resp, "clicks", 7).error_code, 0);
    }

    /// Nothing readable means nothing is asked of Queen at all.
    #[tokio::test]
    async fn a_request_with_nothing_readable_never_calls_queen() {
        let (f, api) = facade(&[("orders", 1)]);
        let resp = handle(&f, &request(&[("__internal", &[(0, 0)])]), None).await;
        assert!(api.fetches.lock().unwrap().is_empty());
        assert_eq!(
            answer(&resp, "__internal", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );

        // ...and a request naming no topics at all is an empty answer.
        let empty = handle(&f, &request(&[]), None).await;
        assert!(empty.responses.is_empty());
        assert!(api.fetches.lock().unwrap().is_empty());
    }

    // --------------------------------------------------------------- budgets

    #[test]
    fn the_byte_budget_is_the_tighter_of_the_two_ceilings() {
        // The partition's own, when it is the smaller.
        assert_eq!(budget(1024 * 1024, 50 * 1024 * 1024), 1024 * 1024);
        // The request's, when it is.
        assert_eq!(budget(8 * 1024 * 1024, 4096), 4096);
        // Never above what the broker will accept for one entry.
        assert_eq!(budget(i32::MAX, i32::MAX), queen::MAX_FETCH_BYTES_PER_ENTRY);
        // Never zero: an entry with no budget reads nothing for ever.
        assert_eq!(budget(0, 4096), 4096);
        assert_eq!(budget(-1, -1), DEFAULT_MAX_BYTES);
        assert_eq!(budget(0, 0), DEFAULT_MAX_BYTES);
    }

    #[tokio::test]
    async fn the_budget_reaches_the_call() {
        let (f, api) = facade(&[("orders", 1)]);
        let req = request(&[("orders", &[(0, 0)])]).with_max_bytes(4096);
        handle(&f, &req, None).await;
        assert_eq!(api.fetched()[0].max_bytes, 4096);
    }

    #[test]
    fn the_response_budget_is_the_clients_ask_under_the_hard_ceiling() {
        assert_eq!(response_budget(4096), 4096);
        // The hard ceiling wins over any ask, however large.
        assert_eq!(response_budget(i32::MAX), MAX_RESPONSE_BYTES as usize);
        assert_eq!(
            response_budget(64 * 1024 * 1024),
            MAX_RESPONSE_BYTES as usize
        );
        // "You choose" is not "send me nothing".
        assert_eq!(response_budget(0), MAX_RESPONSE_BYTES as usize);
        assert_eq!(response_budget(-1), MAX_RESPONSE_BYTES as usize);
    }

    /// THE frame defect: nothing used to count the bytes of the whole answer, so
    /// a wide assignment could build a response the frame codec refuses to
    /// ENCODE — which drops the connection with no response at all, on every
    /// poll. Partitions are filled until the budget is gone; the rest are empty
    /// reads that still carry their real bounds.
    #[tokio::test]
    async fn the_response_stops_at_the_budget_and_the_rest_report_their_bounds() {
        let (f, api) = facade(&[("orders", 8)]);
        let value = vec![b'x'; 8 * 1024];
        let payloads: Vec<serde_json::Value> = (0..4)
            .map(|_| envelope_of(None, &value, 1_756_000_000_000))
            .collect();
        for lane in 0..8 {
            api.seed("orders", &lane.to_string(), 0, &payloads);
        }

        // 40 KiB of room for 256 KiB of records.
        let lanes: Vec<(i32, i64)> = (0..8).map(|i| (i, 0)).collect();
        let resp = handle(
            &f,
            &request(&[("orders", &lanes)]).with_max_bytes(40 * 1024),
            None,
        )
        .await;

        let carried: usize = resp.responses[0]
            .partitions
            .iter()
            .map(|p| p.records.as_ref().map_or(0, |r| r.len()))
            .sum();
        assert!(carried > 0, "the response carried nothing at all");
        assert!(
            carried <= 40 * 1024 + 9 * 1024,
            "the response ran {carried} bytes past a 40 KiB budget"
        );
        for p in &resp.responses[0].partitions {
            assert_eq!(p.error_code, 0, "lane {}", p.partition_index);
            assert_eq!(
                p.high_watermark, 4,
                "lane {} lost its bounds to the budget",
                p.partition_index
            );
        }
    }

    /// The estimate the budget is spent in must never be UNDER the bytes the
    /// encoder produces, or the budget is not a bound on the frame at all.
    #[test]
    fn the_record_size_estimate_is_an_upper_bound_on_the_encoding() {
        let mut headers = IndexMap::new();
        headers.insert(
            StrBytes::from_static_str("trace-id"),
            Some(Bytes::from_static(b"9f2c1e")),
        );
        headers.insert(StrBytes::from_static_str("nothing"), None);

        let mut records = Vec::new();
        let mut estimate = BATCH_HEADER_BYTES;
        for i in 0..8i64 {
            let payload = envelope::encode(
                &Record {
                    transactional: false,
                    control: false,
                    delete_horizon: false,
                    partition_leader_epoch: -1,
                    producer_id: NO_PRODUCER_ID,
                    producer_epoch: NO_PRODUCER_EPOCH,
                    timestamp_type: TimestampType::Creation,
                    offset: 0,
                    sequence: -1,
                    timestamp: 1_756_000_000_000 + i,
                    key: Some(Bytes::from(format!("key-{i}"))),
                    value: Some(Bytes::from(vec![b'v'; 1000 + i as usize])),
                    headers: headers.clone(),
                },
                None,
            );
            estimate += record_bytes(&envelope::decode(&payload, None));
            records.push(FetchedRecord {
                offset: i,
                payload,
                ts: None,
            });
        }

        let encoded = batch("orders", 0, &records, usize::MAX);
        assert!(
            encoded.len() <= estimate,
            "the estimate ({estimate}) is below the encoding ({})",
            encoded.len()
        );
    }

    /// The budget never stalls a consumer: the first record always goes, however
    /// much larger than the whole budget it is.
    #[tokio::test]
    async fn a_record_larger_than_the_budget_is_still_delivered() {
        let (f, api) = facade(&[("orders", 1)]);
        let value = vec![b'x'; 64 * 1024];
        api.seed(
            "orders",
            "0",
            0,
            &[envelope_of(None, &value, 1_756_000_000_000)],
        );
        let resp = handle(
            &f,
            &request(&[("orders", &[(0, 0)])]).with_max_bytes(1024),
            None,
        )
        .await;

        let got = records(answer(&resp, "orders", 0));
        assert_eq!(got.len(), 1, "a consumer would stall here for ever");
        assert_eq!(got[0].value.as_ref().unwrap().len(), 64 * 1024);
    }

    /// A chunk the budget will never reach is read for its BOUNDS only — a
    /// zero-read probe instead of a second 64 MiB answer to throw away — and the
    /// caller's own offset is what decides whether it is out of range, not the
    /// probe's.
    #[tokio::test]
    async fn chunks_past_the_budget_are_read_for_their_bounds_alone() {
        let (f, api) = facade(&[("orders", 1024), ("clicks", 1024)]);
        let value = vec![b'x'; 8 * 1024];
        let payloads: Vec<serde_json::Value> = (0..4)
            .map(|_| envelope_of(None, &value, 1_756_000_000_000))
            .collect();
        // One lane of the first chunk is already more than the response holds.
        api.seed("orders", "0", 0, &payloads);
        // The second chunk: one lane readable from 0, one whose log start has
        // moved past the offset the client is asking at.
        api.seed("clicks", "5", 0, &payloads[..3]);
        api.seed("clicks", "6", 10, &payloads[..3]);

        let lanes: Vec<(i32, i64)> = (0..1024).map(|i| (i, 0)).collect();
        let req =
            request(&[("orders", &lanes), ("clicks", &[(5, 0), (6, 0)])]).with_max_bytes(4 * 1024);
        let resp = handle(&f, &req, None).await;

        let calls = api.fetches.lock().unwrap().clone();
        assert_eq!(calls.len(), 2, "not chunked");
        assert!(
            calls[1]
                .0
                .iter()
                .all(|e| e.offset == BOUNDS_PROBE_OFFSET && e.max_bytes == BOUNDS_PROBE_MAX_BYTES),
            "the chunk past the budget was read for its records"
        );

        // The first chunk still made progress.
        assert!(!records(answer(&resp, "orders", 0)).is_empty());
        // In range: no records this poll, but the bounds are the log's own.
        let five = answer(&resp, "clicks", 5);
        assert_eq!(five.error_code, 0);
        assert_eq!(five.high_watermark, 3);
        assert_eq!(five.log_start_offset, 0);
        assert!(records(five).is_empty());
        // Out of range for the CLIENT's offset, which the probe's own
        // out-of-range answer says nothing about.
        assert_eq!(
            answer(&resp, "clicks", 6).error_code,
            ResponseError::OffsetOutOfRange.code()
        );
    }

    // ---------------------------------------------------------------- tokens

    /// The token reaches the read path, not only the admin calls — the M5 seam.
    #[tokio::test]
    async fn the_token_reaches_the_read() {
        let (f, api) = facade(&[("orders", 1)]);
        handle(&f, &request(&[("orders", &[(0, 0)])]), Some("tenant-a")).await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }

    // -------------------------------------------------------------- the wire

    /// Every advertised version encodes and decodes cleanly, both ways, with
    /// the records surviving the trip. The request is built by the CLIENT half
    /// of `kafka-protocol` and decoded by the broker half; the response makes
    /// the same trip back.
    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let (f, api) = facade(&[("orders", 4)]);
        api.seed(
            "orders",
            "1",
            3,
            &[envelope_of(Some(b"k"), b"v", 7), envelope_of(None, b"w", 8)],
        );
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::Fetch as i16)
            .expect("Fetch is advertised");
        assert!(row.min >= FetchRequest::VERSIONS.min && row.max <= FetchRequest::VERSIONS.max);
        assert_eq!(row.max, 6, "fetch sessions start at v7 and must stay out");

        for version in row.min..=row.max {
            let req = request(&[("orders", &[(1, 3)])]);
            let mut wire = BytesMut::new();
            req.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = FetchRequest::decode(&mut buf, version)
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
            let back = FetchResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(
                buf.is_empty(),
                "v{version}: {} trailing response bytes",
                buf.len()
            );

            let p = answer(&back, "orders", 1);
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.high_watermark, 5, "v{version}");
            assert_eq!(p.last_stable_offset, 5, "v{version}");
            // log_start_offset only exists from v5; below it the field is
            // dropped by the encoder and reads back as the schema default.
            if version >= 5 {
                assert_eq!(p.log_start_offset, 3, "v{version}");
            }
            let got = records(p);
            assert_eq!(got.len(), 2, "v{version}");
            assert_eq!(got[0].offset, 3, "v{version}");
            assert_eq!(got[0].key, Some(Bytes::from_static(b"k")), "v{version}");
            assert_eq!(got[1].value, Some(Bytes::from_static(b"w")), "v{version}");
        }
    }

    /// Both isolation levels are the same answer, because the log has only one
    /// kind of record in it.
    #[tokio::test]
    async fn read_committed_and_read_uncommitted_agree() {
        let (f, api) = facade(&[("orders", 1)]);
        api.seed("orders", "0", 0, &[envelope_of(None, b"v", 1)]);
        let mut seen = Vec::new();
        for level in [0i8, 1] {
            let req = request(&[("orders", &[(0, 0)])]).with_isolation_level(level);
            let resp = handle(&f, &req, None).await;
            let p = answer(&resp, "orders", 0);
            seen.push((
                p.error_code,
                p.high_watermark,
                p.last_stable_offset,
                records(p).len(),
            ));
        }
        assert_eq!(seen[0], seen[1]);
        assert_eq!(seen[0], (0, 1, 1, 1));
    }

    /// Every partition of the request is answered, in the order it was asked —
    /// clients match the response against what they sent.
    #[tokio::test]
    async fn every_requested_partition_is_answered_in_order() {
        let (f, _) = facade(&[("orders", 4)]);
        let resp = handle(&f, &request(&[("orders", &[(3, 0), (0, 0), (1, 0)])]), None).await;
        let order: Vec<i32> = resp.responses[0]
            .partitions
            .iter()
            .map(|p| p.partition_index)
            .collect();
        assert_eq!(order, [3, 0, 1]);
    }

    /// The envelope module is the ONE place the payload shape lives: what fetch
    /// decodes is what produce encoded, headers and nulls included.
    #[test]
    fn the_envelope_is_not_forked() {
        let payload = json!({"k": null, "v": null, "h": [{"k": "x", "v": null}]});
        assert_eq!(
            envelope::decode(&payload, Some(5)),
            Decoded {
                key: None,
                value: None,
                headers: vec![("x".to_string(), None)],
                timestamp: 5,
            }
        );
    }

    /// Cloud back-pressure on the read path. The consumer is told to WAIT and
    /// is answered an empty read — no error code, because an error would also
    /// cost the capped tenant a metadata refresh. See [`crate::throttle`].
    #[tokio::test]
    async fn a_throttled_fetch_is_an_empty_read_and_a_wait() {
        let (f, api) = facade(&[("orders", 1)]);
        api.fail_fetch(Error::Status {
            code: 429,
            body: r#"{"error":"message rate limit exceeded","code":"rate_limited"}"#.into(),
            retry_after_ms: Some(3_000),
        });
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;

        assert_eq!(
            resp.throttle_time_ms, 3_000,
            "the proxy's Retry-After did not reach the consumer"
        );
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, 0, "a throttle is not a fetch error");
        assert!(p.records.as_ref().is_some_and(|r| r.is_empty()));
        assert_eq!(
            p.high_watermark, -1,
            "a watermark we could not read must not be reported as one we did"
        );
    }

    /// ...and with no hint, the default.
    #[tokio::test]
    async fn a_throttle_without_a_hint_still_asks_for_a_wait() {
        let (f, api) = facade(&[("orders", 1)]);
        api.fail_fetch(Error::status(429, "rate_limited"));
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        assert_eq!(resp.throttle_time_ms, crate::throttle::DEFAULT_MS);
        assert_eq!(answer(&resp, "orders", 0).error_code, 0);
    }

    /// A read that simply failed still gets its retriable error code and no
    /// throttle: nothing said when to come back.
    #[tokio::test]
    async fn an_ordinary_failure_carries_no_throttle() {
        let (f, api) = facade(&[("orders", 1)]);
        api.fail_fetch(Error::status(503, "draining"));
        let resp = handle(&f, &request(&[("orders", &[(0, 0)])]), None).await;
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::NotLeaderOrFollower.code()
        );
    }
}
