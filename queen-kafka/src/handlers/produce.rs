//! Produce — the write path, and the milestone that makes the facade useful on
//! its own ("Kafka producers in, native consumers out", PLAN_QUEEN_KAFKA.md M2).
//!
//! One Produce request carries, per topic and per partition, a blob of record
//! batches. The batch format is RecordBatch v2 (magic byte 2) from Produce v3
//! on, it may be compressed with any of four codecs the PRODUCER chose, and one
//! partition may carry several batches. All of it is decoded here — under one
//! expansion budget for the whole request, because a compressed batch declares
//! nothing about what it costs to decode ([`crate::decompress`]) — mapped to
//! Queen push items through the payload envelope in [`crate::records`], and
//! written with a SINGLE `POST /api/v1/push` covering every topic and every
//! partition of the request. One part of a record does not survive that decode
//! on its own — a header NAME a producer sent twice, which `kafka-protocol`
//! collapses into its map — and is recovered from the batch's own bytes before
//! the envelope is built ([`crate::wire`]): this is the last place it exists,
//! because what is not written into the payload here never reaches Queen.
//!
//! ## Any node serves any partition, whatever Metadata said the leader was
//!
//! In cluster mode ([`crate::cluster`]) this handler has NO leadership gate,
//! and the absence is a decision: `003_log_push.sql:131-213` allocates a
//! message's offset by locking the `queen.log_partitions` row INSIDE the
//! database transaction, so two facades appending to one partition through two
//! brokers cannot issue the same offset. Apache Kafka answers
//! NOT_LEADER_OR_FOLLOWER at a non-leader because a non-leader does not have
//! the data; here every node has all of it, and refusing would turn each
//! membership change into a synchronised metadata storm for nothing.
//!
//! ## One push, and the offsets that come back from it
//!
//! The batching is not an optimisation detail, it is what makes the answer
//! possible. C1 (PLAN_QUEEN_KAFKA.md) has the push response carry each message's
//! absolute offset, and Kafka's produce answer is one `base_offset` per
//! partition — the offset of that partition's FIRST record, with the client
//! deriving the rest by counting. That derivation is only true if the records of
//! one partition landed contiguously, which they do because a multi-message push
//! to one partition packs one segment. This handler does not assume it: it reads
//! every offset the broker returned for a partition's run of items and checks
//! that they are `base, base+1, …`. A run that breaks the rule fails THAT
//! partition with UNKNOWN_SERVER_ERROR and a log line naming the cause. A
//! guessed base offset is a consumer reading the wrong records forever, so it is
//! never guessed.
//!
//! ## The spool is not a failure
//!
//! There is one shape where the broker allocates no offsets and nothing went
//! wrong: with the broker in MAINTENANCE mode every push is diverted to the file
//! buffer, answered `201` with each item `status: "buffered"` and no offset, and
//! replayed — with offsets — when maintenance ends (server/src/handlers/data.rs,
//! `buffer_all`). Those records are durable and they WILL land, so the one thing
//! this must not answer is a final failure: a Kafka producer takes
//! UNKNOWN_SERVER_ERROR as fatal for the batch, raises a send error for records
//! that are safely spooled, and every retry the application then makes is a
//! duplicate in the log. It is answered REQUEST_TIMED_OUT instead — the one
//! produce code whose meaning is "we do not know whether it landed", retriable,
//! which is the honest description of a record with no offset yet. A retry may
//! still duplicate, and that is the at-least-once every non-idempotent Kafka
//! producer already has.
//!
//! ## The one request field that is not acted on
//!
//! `timeout_ms` is how long a Kafka leader waits for its followers to
//! acknowledge before answering REQUEST_TIMED_OUT. There are no followers here:
//! the push is one synchronous write to Postgres, and what bounds the wait is
//! this facade's own 10-second budget for a call to Queen (`queen.rs`), which is
//! below every client default (`request.timeout.ms` is 30 s). Honouring the
//! field would mean racing a timer against a write already in flight and
//! answering "timed out" about a record that landed — strictly worse than
//! answering late, which a client handles by ignoring the response it no longer
//! has a correlation id for. Everything else in v3..=v9 is acted on.
//!
//! ## Errors are per partition
//!
//! Kafka's produce response has an error code per partition and none for the
//! request, and clients are built on that: one unknown topic in a batch must not
//! poison the eleven that were fine. Everything decidable per partition is
//! therefore decided per partition — the topic's name and existence, the
//! partition index, the batch decode, the codec, the refusals below — and the
//! only errors that land on every partition at once are the ones that really are
//! request-wide (an invalid `acks`, a transactional id) or that took the whole
//! push down with them (Queen unreachable).
//!
//! ## What this refuses, and why loudly
//!
//! Transactions, EOS and the idempotent producer are out of scope by plan, and
//! the dangerous failure mode is not refusing them — it is ACCEPTING them.
//! A producer that believes its sequence numbers are being enforced silently
//! gets at-least-once with duplicates on every retry, and finds out in
//! production. So a transactional id, a transactional or control batch, and a
//! batch carrying a producer id are each refused with the truest error code
//! available (see [`refuse`]) and a message that names the reason on any client
//! new enough to read one (`error_message`, Produce v8+).

use std::collections::HashMap;

use kafka_protocol::error::ResponseError;
use kafka_protocol::messages::produce_request::PartitionProduceData;
use kafka_protocol::messages::produce_response::{PartitionProduceResponse, TopicProduceResponse};
use kafka_protocol::messages::{ProduceRequest, ProduceResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use kafka_protocol::records::{BatchDecodeInfo, RecordBatchDecoder};

use crate::decompress::{self, Refusal};
use crate::handlers::metadata::{self, Plan};
use crate::idempotent;
use crate::queen::{self, PushItem, Pushed};
use crate::records as envelope;
use crate::throttle;
use crate::wire;
use crate::Facade;

/// The three values Kafka defines for `acks`: none, the leader, the full ISR.
/// Anything else is INVALID_REQUIRED_ACKS, which is what a real broker answers
/// and what stops a typo'd `acks=2` from being silently read as "all".
const ACKS_NONE: i16 = 0;
const ACKS_LEADER: i16 = 1;
const ACKS_ALL: i16 = -1;

/// Kafka's "there is no offset here". The value a produce answer carries for a
/// partition it did not append to.
const NO_OFFSET: i64 = -1;

/// The per-item status a push answers when the broker is in maintenance mode and
/// spooled the record instead of writing it (server/src/handlers/data.rs,
/// `buffer_all`). Named because it is the difference between "no offset, and the
/// record is safe" and "no offset, and something is wrong".
const SPOOLED: &str = "buffered";

/// How much one Produce request may decompress to, in total.
///
/// [`crate::conn::MAX_FRAME_BYTES`] and not a smaller number of its own: it is
/// exactly the records the same producer could have sent UNCOMPRESSED in one
/// request, so it cannot refuse anything a client could otherwise have written,
/// and it is what stops compression buying more room than the frame limit gives.
/// See [`crate::decompress`].
const MAX_DECOMPRESSED_BYTES: usize = crate::conn::MAX_FRAME_BYTES;

/// How many records one Produce request may DECLARE across all of its batches.
///
/// A separate ceiling because a declared count is not a decoded record: the
/// decoder reserves for the count in the batch header before it reads a byte, so
/// the number has to be bounded where it is still just a number
/// ([`crate::decompress::Budget`]).
///
/// A million is far past anything that can succeed anyway, which is why it can
/// be this generous: every staged record becomes an item of ONE push body, and
/// the broker's own body limit (`QUEEN_MAX_BODY_BYTES`, 64 MiB by default) stops
/// at a few hundred thousand of even the smallest of them. A producer cannot
/// reach this ceiling by batching harder; only by lying can it.
const MAX_RECORDS_PER_REQUEST: usize = 1_000_000;

/// What one (topic, partition) entry of the request resolved to, before the
/// push. The staged items live in one shared vector; a slot names its run in it.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Slot {
    /// Items `start..start+len` of the push are this partition's records.
    ///
    /// `seq` is present only for an idempotent producer, and is what the
    /// sequence window is advanced with once the push has answered — never
    /// before ([`crate::idempotent`]).
    Push {
        start: usize,
        len: usize,
        seq: Option<idempotent::Pending>,
    },
    /// Nothing to write: the entry carried no records at all.
    Empty,
    /// An idempotent producer resent a batch this facade has already appended.
    /// Answered `error_code = 0` with the offsets the original got, and NOTHING
    /// is written — Kafka's own duplicate semantics, and the reason a retry
    /// after a lost response is invisible.
    Duplicate(i64),
    /// Answer this error, and write nothing.
    Reject(ResponseError, String),
}

/// What one partition entry needs to consult the idempotent-producer window:
/// the process-wide state, and the scope to file it under.
///
/// The tenant is resolved once per request rather than once per partition —
/// it is a lock and a hash of the connection's credential
/// ([`crate::identity::Identities::known`]), and a request carries as many
/// partitions as a client likes.
struct Idem<'a> {
    producers: &'a idempotent::Producers,
    tenant: &'a crate::identity::TenantKey,
}

/// Handle one Produce request. `None` means "write no response frame", which is
/// only ever the answer to `acks=0`.
///
/// `token` is the credential to reach Queen with — `QUEEN_TOKEN` at M2, the
/// connection's own tenant token from M5 on.
pub async fn handle(
    facade: &Facade,
    req: &ProduceRequest,
    token: Option<&str>,
) -> Option<ProduceResponse> {
    let response = build(facade, req, token).await;
    if req.acks == ACKS_NONE {
        // THE acks=0 wire contract: the request is processed exactly as any
        // other, and nothing is written back — not an empty response, not an
        // error, no frame at all. A client that sent acks=0 is not waiting for
        // one and would read the bytes as the answer to its NEXT request.
        // The response was still built: it is where the log lines about
        // anything that went wrong come from, and it is the only place a
        // fire-and-forget producer's failures are visible at all.
        log_silent_failures(&response);
        return None;
    }
    Some(response)
}

async fn build(facade: &Facade, req: &ProduceRequest, token: Option<&str>) -> ProduceResponse {
    if !matches!(req.acks, ACKS_NONE | ACKS_LEADER | ACKS_ALL) {
        tracing::warn!(target: "kafka", acks = req.acks, "produce with an invalid acks");
        return uniform(
            req,
            ResponseError::InvalidRequiredAcks,
            format!(
                "acks={} is not one of Kafka's three values (0, 1, -1)",
                req.acks
            ),
        );
    }
    // acks=1 and acks=-1 take the same path: every push is a durable,
    // synchronously committed write (PLAN_QUEEN_KAFKA.md M2 — "all = durable
    // push; 0/1 same path for now"). Queen has no in-memory tier to distinguish
    // them with today; the per-queue `relaxed` durability class is what will
    // make acks=1 cheaper than acks=-1, and until it exists answering both from
    // the durable path is the safe direction to be wrong in.
    // An EMPTY transactional id is not a transactional id, and is read here as
    // exactly the absent one it was meant to be. Erlang's `kafka_protocol`
    // hand-rolls the Produce encoder instead of deriving it from its own schema,
    // and types this field `string` where the schema says `nullable_string`
    // (`kpro_req_lib.erl:308`, with `kpro_lib.erl:140` encoding null as `""`),
    // so a plainly NON-transactional send from brod — and with it
    // `broadway_kafka` and `kaffe`, which is most of the Elixir in production —
    // puts a zero-length string on the wire where the protocol says null.
    // Apache Kafka takes those same bytes without complaint, because a real
    // broker does not gate on this field at all: a produce is transactional to
    // it when the RECORD BATCH carries the `isTransactional` attribute bit. That
    // bit is still refused, in `stage` below, which is where the refusal always
    // belonged. Measured, not reasoned: the identical brod build produces
    // cleanly against apache/kafka:3.9.1 and could not produce one message here
    // (compat/brod/README.md). A non-empty id keeps every byte of the refusal.
    // The empty-string filter lives in ONE place now
    // ([`crate::idempotent::transactional_id`]), because
    // `handlers::init_producer_id` has to read this field exactly the same way:
    // a producer that met one answer here and another there would meet two
    // different stories about transactions.
    if let Some(id) =
        idempotent::transactional_id(req.transactional_id.as_ref().map(|id| id.0.as_str()))
    {
        // TRANSACTIONAL_ID_AUTHORIZATION_FAILED rather than a generic refusal:
        // it is a code every client already handles as fatal-and-final for the
        // producer (it aborts rather than retrying), it names the transactional
        // id as the thing that was refused, and it cannot be mistaken for a
        // transient broker problem the way UNKNOWN_SERVER_ERROR can. Kafka
        // Streams and any `transactional.id` producer stops here with a clear
        // message instead of appearing to work.
        tracing::warn!(
            target: "kafka",
            transactional_id = %id,
            "produce with a transactional id: transactions are out of scope"
        );
        return uniform(
            req,
            ResponseError::TransactionalIdAuthorizationFailed,
            format!(
                "queen-kafka does not implement transactions, so it will not accept the \
                 transactional id `{}` — remove transactional.id (and exactly-once processing) \
                 from this producer",
                id
            ),
        );
    }

    let plans = topic_plans(facade, req, token).await;

    let tenant = facade.catalog.tenant_key(token);
    let idem = Idem {
        producers: &facade.producers,
        tenant: &tenant,
    };

    let mut items: Vec<PushItem> = Vec::new();
    let mut slots: Vec<Vec<Slot>> = Vec::with_capacity(req.topic_data.len());
    {
        // ONE budget for the request, not one per batch or one per partition: a
        // request carries as many of both as it likes, and a ceiling that reset
        // between them would multiply by their number.
        let budget = decompress::Budget::new(MAX_DECOMPRESSED_BYTES, MAX_RECORDS_PER_REQUEST);
        for topic in &req.topic_data {
            let name = topic.name.0.as_str();
            let plan = plans
                .get(name)
                .copied()
                // Unreachable: `topic_plans` covers every name in the request.
                .unwrap_or(Plan::Reject(ResponseError::UnknownServerError));
            slots.push(
                topic
                    .partition_data
                    .iter()
                    .map(|p| stage(&mut items, name, plan, p, &budget, &idem))
                    .collect(),
            );
        }
    }

    // One push for the whole request, or none at all when nothing survived the
    // staging. `pushed` is what every Push slot is answered from.
    let pushed = if items.is_empty() {
        None
    } else {
        Some(facade.queen.push(&items, token).await)
    };

    render(req, &slots, pushed.as_ref(), &idem)
}

// --------------------------------------------------------------------- topics

/// Resolve every distinct topic named in the request, auto-creating the ones
/// Queen does not have.
///
/// Auto-creation is unconditional here, unlike Metadata's, and the asymmetry is
/// deliberate: Metadata has a client flag to obey (`allow_auto_topic_creation`)
/// and a produce request has none, so the policy is the one Kafka brokers apply
/// with `auto.create.topics.enable` — a produce to an absent topic creates it
/// and then writes. It runs through the SAME pass Metadata uses
/// ([`metadata::create_absent`]), so the "re-read before creating, never
/// re-`/configure` an existing queue" rule holds for both.
async fn topic_plans<'a>(
    facade: &Facade,
    req: &'a ProduceRequest,
    token: Option<&str>,
) -> HashMap<&'a str, Plan> {
    let catalog = match facade.catalog.list(token).await {
        Ok(queues) => Some(queues),
        Err(e) => {
            tracing::error!(target: "kafka", error = %e, "produce: cannot read the queue list");
            None
        }
    };

    // First-seen order, de-duplicated through a set rather than a scan: the
    // topic list is the client's, one entry of it is about ten bytes on the
    // wire, and `conn::MAX_FRAME_BYTES` is 100 MiB — so a `contains` per entry
    // is a quadratic a single frame can buy.
    let mut names: Vec<&str> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for topic in &req.topic_data {
        let name = topic.name.0.as_str();
        if seen.insert(name) {
            names.push(name);
        }
    }

    // ...and the catalog as a lookup for the same reason: both sides are big
    // numbers a client and a tenant choose independently, and their product is
    // the other quadratic.
    let live: HashMap<&str, i64> = catalog
        .as_deref()
        .map(|queues| {
            queues
                .iter()
                .map(|q| (q.name.as_str(), q.partitions))
                .collect()
        })
        .unwrap_or_default();
    let mut planned: Vec<(Option<&str>, Plan)> = names
        .iter()
        .map(|name| match catalog.as_deref() {
            // Retriable, and the same code Metadata answers when Queen is
            // unreachable: the producer backs off, refreshes metadata and tries
            // again, which is exactly right for a blip.
            None => (Some(*name), Plan::Reject(ResponseError::LeaderNotAvailable)),
            Some(_) => (
                Some(*name),
                metadata::plan(
                    name,
                    live.get(name).copied(),
                    true,
                    facade.default_partitions,
                ),
            ),
        })
        .collect();

    metadata::create_absent(facade, &mut planned, token).await;

    planned
        .into_iter()
        .filter_map(|(name, plan)| name.map(|n| (n, plan)))
        .collect()
}

// --------------------------------------------------------------------- records

/// Decode one partition entry and stage its records as push items. `budget` is
/// the request's remaining decompression allowance, and it is spent here.
fn stage(
    items: &mut Vec<PushItem>,
    topic: &str,
    plan: Plan,
    p: &PartitionProduceData,
    budget: &decompress::Budget,
    idem: &Idem<'_>,
) -> Slot {
    let width = match plan {
        Plan::Serve(width) => width,
        // `plan` collapses three different refusals into two codes; the message
        // is where they say which one they were.
        Plan::Reject(e) => {
            let why = match e {
                ResponseError::UnknownTopicOrPartition => format!(
                    "there is no topic `{topic}` here, and there will not be: names beginning \
                     with `__` are Kafka's own and are never created"
                ),
                ResponseError::InvalidTopicException => format!(
                    "`{topic}` is not a legal Kafka topic name (letters, digits, `.`, `_`, `-`, \
                     up to 249 of them)"
                ),
                other => format!("topic `{topic}` cannot be resolved right now ({other})"),
            };
            return Slot::Reject(e, why);
        }
        // `create_absent` resolves every Create into a Serve or a Reject before
        // this runs; a Create arriving here would be a bug in that pass, not a
        // topic that half exists.
        Plan::Create => {
            return Slot::Reject(
                ResponseError::LeaderNotAvailable,
                format!("topic `{topic}` was not resolved before the push"),
            )
        }
    };
    if p.index < 0 || p.index >= width {
        // The client is addressing a lane Metadata never advertised. UNKNOWN
        // rather than an invalid-request code: a client that produces past the
        // width usually has a stale metadata view, and UNKNOWN_TOPIC_OR_PARTITION
        // is what makes it refresh.
        return Slot::Reject(
            ResponseError::UnknownTopicOrPartition,
            format!(
                "partition {} is outside 0..{width}, the width `{topic}` is advertised at",
                p.index
            ),
        );
    }

    let Some(raw) = p.records.as_ref().filter(|r| !r.is_empty()) else {
        return Slot::Empty;
    };

    // The batch HEADERS first, without decompressing anything: they carry the
    // CRC (so a corrupt batch is caught before any codec is handed it), the
    // flags this facade refuses, and the record count. It costs one walk of the
    // batch lengths and it means a 4 MB compressed transactional batch is
    // refused without being inflated.
    let infos = match RecordBatchDecoder::decode_batch_info(&mut raw.clone()) {
        Ok(infos) => infos,
        Err(e) => {
            tracing::debug!(target: "kafka", topic, partition = p.index, error = %e, "undecodable record batch");
            return Slot::Reject(
                ResponseError::CorruptMessage,
                format!("the record batch did not decode: {e}"),
            );
        }
    };
    if infos.is_empty() {
        // Bytes that are not a v2 batch at all — `decode_batch_info` stops at
        // the first foreign magic byte rather than failing. From Produce v3 on,
        // RecordBatch v2 is the only legal format, so this is a client speaking
        // a message set older than the version it negotiated.
        tracing::warn!(
            target: "kafka",
            topic,
            partition = p.index,
            "produce carrying a pre-v2 message set"
        );
        return Slot::Reject(
            ResponseError::UnsupportedForMessageFormat,
            "the records are not RecordBatch v2; message sets v0 and v1 are not accepted at \
             Produce v3 and above"
                .to_string(),
        );
    }
    if let Some((error, why)) = refuse(&infos) {
        tracing::warn!(target: "kafka", topic, partition = p.index, %why, "refusing a record batch");
        return Slot::Reject(error, why);
    }
    // The idempotent producer's sequence window, checked HERE — on the headers,
    // before the batch is decompressed and before the record count is charged
    // against the request's budget — because a duplicate is answered without
    // decoding a byte of it and a gap is refused without decoding a byte of it.
    // This is the place `refuse` used to answer UNSUPPORTED_FOR_MESSAGE_FORMAT
    // from ([`crate::idempotent`]).
    let seq = match idem.producers.check(idem.tenant, topic, p.index, &infos) {
        idempotent::Verdict::NotIdempotent => None,
        idempotent::Verdict::Accept(pending) => Some(pending),
        idempotent::Verdict::Duplicate(base) => {
            // Kafka answers a duplicate as the SUCCESS it was. Debug and not
            // warn: on a producer whose response was lost this is the system
            // working, and it is exactly the moment a fleet retries in unison.
            tracing::debug!(
                target: "kafka",
                topic,
                partition = p.index,
                base_offset = base,
                "an idempotent producer resent a batch this facade already appended"
            );
            return Slot::Duplicate(base);
        }
        idempotent::Verdict::Reject(error, why) => {
            tracing::warn!(
                target: "kafka",
                topic,
                partition = p.index,
                %why,
                "refusing a batch on its producer sequence"
            );
            return Slot::Reject(error, why);
        }
    };
    // Charged off the HEADERS, before the decoder can reserve for them.
    let declared = infos.iter().fold(0usize, |sum, i| {
        sum.saturating_add(i.record_count.max(0) as usize)
    });
    if let Err(why) = budget.declare_records(declared) {
        tracing::warn!(
            target: "kafka",
            topic,
            partition = p.index,
            %why,
            limit = MAX_RECORDS_PER_REQUEST,
            "refusing a record batch for the record count it declares"
        );
        return Slot::Reject(
            ResponseError::MessageTooLarge,
            format!("the record count this request declares is not one it may decode: {why}"),
        );
    }

    let batches = match decompress::decode_all(&mut raw.clone(), budget) {
        Ok(batches) => batches,
        Err(Refusal::Corrupt(why)) => {
            // The headers decoded and the CRCs passed, so this is the records
            // themselves: a codec failure (a truncated compressed block) or a
            // malformed varint inside one.
            tracing::debug!(target: "kafka", topic, partition = p.index, %why, "undecodable records");
            return Slot::Reject(
                ResponseError::CorruptMessage,
                format!("the records did not decode: {why}"),
            );
        }
        Err(Refusal::TooLarge(why)) => {
            // MESSAGE_TOO_LARGE is the code for "the server will not accept a
            // message this size", it is not retriable, and the producer's own
            // answer to it is to send less — all three are right here. Loud,
            // because on a well-behaved producer it cannot happen: reaching this
            // means one request asked to decompress more than a whole frame of
            // records.
            tracing::warn!(
                target: "kafka",
                topic,
                partition = p.index,
                %why,
                limit = MAX_DECOMPRESSED_BYTES,
                "refusing a record batch for what it decompresses to"
            );
            return Slot::Reject(
                ResponseError::MessageTooLarge,
                format!(
                    "the records of this request decompress past the {MAX_DECOMPRESSED_BYTES} \
                     bytes one produce may expand to: {why}"
                ),
            );
        }
    };

    let start = items.len();
    for batch in &batches {
        // The headers as the producer sent them: in order, and with any name it
        // repeated still there. `Record.headers` is a map and has already lost
        // the repeats by this point, so they are read back out of the bytes the
        // batch was decoded from ([`crate::wire`]) — and this is the LAST place
        // they exist, because what is not written into the payload here never
        // reaches Queen and cannot be recovered by any later read.
        let lists = wire::header_lists(&batch.records, &batch.set.records);
        if lists.is_none() && batch.set.records.iter().any(|r| !r.headers.is_empty()) {
            // Not a refusal: every field but the header repeats is intact, and
            // the map the crate decoded is still a truthful answer for a record
            // whose names are distinct — which is nearly all of them. Loud
            // because it means the walk and the decoder disagree about bytes
            // they both just read, which is a bug here rather than a client's.
            tracing::error!(
                target: "kafka",
                topic,
                partition = p.index,
                records = batch.set.records.len(),
                "cannot re-read this batch's header lists; a repeated header name would be \
                 stored once"
            );
        }
        for (i, record) in batch.set.records.iter().enumerate() {
            items.push(PushItem {
                queue: topic.to_string(),
                // The Queen partition NAME is the Kafka partition index written
                // out — the mapping the whole facade rests on.
                partition: p.index.to_string(),
                payload: envelope::encode(
                    record,
                    lists.as_ref().and_then(|l| l.get(i)).map(Vec::as_slice),
                ),
            });
        }
    }
    match items.len() - start {
        0 => Slot::Empty,
        len => Slot::Push { start, len, seq },
    }
}

/// The batch flags this facade will not accept, with the reason a client sees.
///
/// The codes are chosen to be the truest available, and each one is a decision:
///
///   * **transactional** → TRANSACTIONAL_ID_AUTHORIZATION_FAILED. Same code and
///     same reason as a request-level transactional id (see [`build`]): fatal,
///     final, and unmistakably about transactions.
///   * **control** → INVALID_RECORD. A control batch (a transaction commit or
///     abort marker) is written by a coordinator, never by a producer, so a
///     produce carrying one is a genuinely invalid record rather than an
///     unsupported feature — and this facade is no one's coordinator.
///
/// A **producer id** is no longer among them. Until M7 F3 it was refused
/// UNSUPPORTED_FOR_MESSAGE_FORMAT with a message ending "Set
/// enable.idempotence=false", on the reasoning that accepting sequence numbers
/// this facade did not check would be the worst outcome available. That
/// reasoning still holds and is why the replacement is a CHECK and not an
/// acceptance: the sequence window in [`crate::idempotent`] runs immediately
/// after this function, on the same headers, before anything is decompressed.
///
/// Checked on the batch headers, so a batch is refused before it is
/// decompressed. An EMPTY batch carrying a flag writes nothing either way and
/// is left to the "no records" path.
fn refuse(infos: &[BatchDecodeInfo]) -> Option<(ResponseError, String)> {
    for info in infos {
        if info.transactional {
            return Some((
                ResponseError::TransactionalIdAuthorizationFailed,
                "queen-kafka does not implement transactions, and will not accept a transactional \
                 record batch"
                    .to_string(),
            ));
        }
        if info.control {
            return Some((
                ResponseError::InvalidRecord,
                "a control batch is written by a transaction coordinator, and queen-kafka is not \
                 one"
                .to_string(),
            ));
        }
    }
    None
}

// -------------------------------------------------------------------- answers

/// Build the response from the staged slots and whatever the push answered.
fn render(
    req: &ProduceRequest,
    slots: &[Vec<Slot>],
    pushed: Option<&queen::Result<Vec<Pushed>>>,
    idem: &Idem<'_>,
) -> ProduceResponse {
    let mut responses = Vec::with_capacity(req.topic_data.len());
    for (topic, row) in req.topic_data.iter().zip(slots) {
        let name = topic.name.0.as_str();
        let partitions = topic
            .partition_data
            .iter()
            .zip(row)
            .map(|(p, slot)| match slot {
                Slot::Reject(e, why) => rejected(p.index, *e, why),
                // Nothing was appended, so there is no base offset to report and
                // nothing failed either: the entry asked for no work.
                Slot::Empty => appended(p.index, NO_OFFSET),
                // Already in the log, at these offsets. Nothing was pushed and
                // nothing is committed: the window already remembers this batch.
                Slot::Duplicate(base) => appended(p.index, *base),
                Slot::Push { start, len, seq } => match pushed {
                    // `get` and not an index: a handler must not panic on
                    // anything a broker answered, however wrong it is.
                    Some(Ok(results)) => match results
                        .get(*start..*start + *len)
                        .ok_or_else(|| {
                            NoBase::Broken(format!(
                                "the push answered {} results for {} items",
                                results.len(),
                                start + len
                            ))
                        })
                        .and_then(base_offset)
                    {
                        Ok(base) => {
                            // The window advances HERE and nowhere else: only a
                            // run that came back with contiguous offsets is a
                            // run whose sequences this facade may claim to have
                            // appended. A partially failed push leaves the entry
                            // untouched, so the client's retry is new work
                            // rather than a skipped duplicate.
                            if let Some(pending) = seq {
                                idem.producers.commit(pending, base, *len);
                            }
                            appended(p.index, base)
                        }
                        // The records are in the spool and will be numbered when
                        // maintenance ends: retriable, never a delivery failure.
                        // See the module header.
                        Err(NoBase::Spooled(why)) => {
                            tracing::warn!(
                                target: "kafka",
                                topic = name,
                                partition = p.index,
                                %why,
                                "the broker spooled this produce; answering it retriable"
                            );
                            rejected(p.index, ResponseError::RequestTimedOut, &why)
                        }
                        Err(NoBase::Broken(why)) => {
                            // The broker did not answer what the whole offset
                            // mapping rests on. Loud, and never guessed.
                            tracing::error!(
                                target: "kafka",
                                topic = name,
                                partition = p.index,
                                %why,
                                "the push response cannot be turned into a base offset"
                            );
                            rejected(p.index, ResponseError::UnknownServerError, &why)
                        }
                    },
                    Some(Err(e)) => {
                        let mapped = kafka_error(e);
                        rejected(p.index, mapped, &format!("push failed: {e}"))
                    }
                    // Unreachable: a Push slot exists only when items were
                    // staged, and staged items are always pushed.
                    None => rejected(
                        p.index,
                        ResponseError::UnknownServerError,
                        "records were staged but never pushed",
                    ),
                },
            })
            .collect();
        responses.push(
            TopicProduceResponse::default()
                .with_name(TopicName(StrBytes::from_string(name.to_string())))
                .with_partition_responses(partitions),
        );
    }
    if let Some(Err(e)) = pushed {
        tracing::error!(target: "kafka", error = %e, "produce push failed");
    }
    // The Cloud back-pressure channel: a 429 from the proxy becomes the
    // `throttle_time_ms` every producer already sleeps on, beside a retriable
    // per-partition code ([`crate::throttle`]). Read off the PUSH alone — an
    // auto-create refused by the same cap is answered LEADER_NOT_AVAILABLE with
    // no throttle, because the plan pass keeps no error to read it from, and a
    // producer that has to create a topic is retrying on its own timer anyway.
    let throttle = pushed
        .and_then(|r| r.as_ref().err())
        .and_then(throttle::for_error);
    ProduceResponse::default()
        .with_responses(responses)
        .with_throttle_time_ms(throttle.unwrap_or(0))
}

/// Answer every partition of every topic in the request with one error. For the
/// two refusals that really are request-wide.
fn uniform(req: &ProduceRequest, error: ResponseError, why: String) -> ProduceResponse {
    ProduceResponse::default().with_responses(
        req.topic_data
            .iter()
            .map(|topic| {
                TopicProduceResponse::default()
                    .with_name(topic.name.clone())
                    .with_partition_responses(
                        topic
                            .partition_data
                            .iter()
                            .map(|p| rejected(p.index, error, &why))
                            .collect(),
                    )
            })
            .collect(),
    )
}

/// A partition that was appended to.
fn appended(index: i32, base_offset: i64) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .with_index(index)
        .with_error_code(0)
        .with_base_offset(base_offset)
        // -1 is "the log did not stamp these records", and it is the truth: the
        // facade never sets LogAppendTime, so the timestamp a consumer gets is
        // the producer's own CreateTime out of the envelope.
        .with_log_append_time_ms(NO_OFFSET)
        // v5+, and a DELIBERATE difference from Apache Kafka, which reports the
        // partition's real log start offset here.
        //
        // A push answers a status and an offset per item (C1) and nothing about
        // the log's lower bound, so the only way to fill this in would be a
        // bounds probe — a second call to Queen, on the write path, per produce
        // request. What that would buy is a number no client this facade serves
        // reads: the field exists for the idempotent producer, which consults it
        // when it meets UNKNOWN_PRODUCER_ID after a truncation, and the
        // idempotent producer is refused outright here ([`refuse`]). -1 is the
        // protocol's own default for the field (Kafka's `ProduceResponse.json`
        // declares `"default": "-1"`, and the Java client's
        // `PartitionResponse.logStartOffset` starts there), so it reads as
        // "this broker has nothing to say about the log start" rather than as a
        // number — which is exactly the state of things, and better than the
        // guess a plausible-looking 0 would be on any partition whose retention
        // has moved.
        .with_log_start_offset(NO_OFFSET)
}

/// A partition that was not.
fn rejected(index: i32, error: ResponseError, why: &str) -> PartitionProduceResponse {
    PartitionProduceResponse::default()
        .with_index(index)
        .with_error_code(error.code())
        .with_base_offset(NO_OFFSET)
        .with_log_append_time_ms(NO_OFFSET)
        .with_log_start_offset(NO_OFFSET)
        // Produce v8+. Silently dropped by the encoder below v8, so it costs
        // nothing to always say why — and on a client new enough to read it,
        // the refusals in `refuse` arrive as sentences instead of numbers.
        .with_error_message(Some(StrBytes::from_string(why.to_string())))
}

/// Why a partition's run of pushed items has no base offset. The two are
/// different answers to a producer — one is "come back", the other is "this
/// broker is wrong" — so they are not one string.
#[derive(Debug, Clone, PartialEq, Eq)]
enum NoBase {
    /// The records are safe and unnumbered: maintenance mode spooled them. See
    /// the module header.
    Spooled(String),
    /// The push answered something no base offset describes.
    Broken(String),
}

/// The base offset of one partition's run of pushed items, or the reason there
/// is none to report.
///
/// This is where the contiguity assumption is CHECKED rather than trusted (see
/// the module header). Every item of the run must carry an offset, and the run
/// must be `base, base+1, …`: a client derives every record's offset by counting
/// from the base, so anything else means the numbers it computes address other
/// records.
fn base_offset(run: &[Pushed]) -> std::result::Result<i64, NoBase> {
    let first = run.first().ok_or_else(|| {
        NoBase::Broken("the push answered nothing for this partition".to_string())
    })?;
    let base = match first.offset {
        Some(base) => base,
        None => return Err(unnumbered(0, first)),
    };
    for (i, item) in run.iter().enumerate() {
        // Checked, because `base` is a number the broker chose: an offset near
        // i64::MAX must come back as a mismatch, not as an arithmetic panic.
        let expected = base.checked_add(i as i64);
        match item.offset {
            Some(offset) if Some(offset) == expected => {}
            Some(offset) => {
                return Err(NoBase::Broken(format!(
                    "record {i} of this partition landed at offset {offset}, not {expected:?}: \
                     the run is not contiguous, so no base offset describes it"
                )))
            }
            None => return Err(unnumbered(i, item)),
        }
    }
    Ok(base)
}

/// One item the broker allocated no offset for, read off its own status: the
/// spool says so and everything else is a broker that did not do what it said.
fn unnumbered(i: usize, item: &Pushed) -> NoBase {
    if item.status == SPOOLED {
        NoBase::Spooled(format!(
            "record {i} was spooled by a broker in maintenance mode, so it has no offset yet — \
             it is stored and will be replayed into the log when maintenance ends"
        ))
    } else {
        NoBase::Broken(format!(
            "the broker allocated no offset for record {i} (status `{}`)",
            item.status
        ))
    }
}

/// The closest Kafka error for a failed call to Queen.
///
/// The distinction that matters to a producer is retriable or not, because that
/// is what decides between a backoff and a delivery failure raised to the
/// application. Everything here is chosen on that axis first and on precision
/// second.
fn kafka_error(e: &queen::Error) -> ResponseError {
    match e {
        // No answer at all: connect refused, DNS, TLS, a reset, or our own
        // 10-second budget expiring. REQUEST_TIMED_OUT is the one produce code
        // whose meaning is "we do not know whether it landed", which is exactly
        // the state a timed-out push leaves us in, and it is retriable — the
        // producer retries, and the duplicate that may follow is the same
        // at-least-once Kafka gives a non-idempotent producer.
        queen::Error::Transport(_) => ResponseError::RequestTimedOut,
        queen::Error::Status { code, .. } => match code {
            // The broker's body limit (QUEEN_MAX_BODY_BYTES) or the proxy's.
            // Not retriable, and the producer's own answer to it is to split
            // the batch or raise max.request.size.
            413 => ResponseError::MessageTooLarge,
            408 => ResponseError::RequestTimedOut,
            // The token this facade holds is not (or no longer) allowed to
            // write. Fatal and named, rather than a mystery 500.
            401 | 403 => ResponseError::TopicAuthorizationFailed,
            404 => ResponseError::UnknownTopicOrPartition,
            // Cloud: a frozen or rate-capped tenant. The WAIT is carried by
            // `throttle_time_ms` (see `render` and [`crate::throttle`]), and
            // this is only the code that goes beside it — so it has to be one
            // every producer retries. REQUEST_TIMED_OUT is; the
            // THROTTLING_QUOTA_EXCEEDED this used to answer is not on
            // librdkafka's produce-retry list, which made a rate cap a
            // permanent delivery failure on every Confluent client.
            429 => ResponseError::RequestTimedOut,
            // Queen is there but not serving right now (a gateway, a draining
            // broker). Retriable AND it makes the client refresh metadata,
            // which is the same answer Metadata gives for the same situation.
            502..=504 => ResponseError::LeaderNotAvailable,
            // Anything else, including a 400, which would mean the facade built
            // a body the broker rejected — our bug, and it should be loud.
            _ => ResponseError::UnknownServerError,
        },
        // A 2xx we could not read, or a push response that does not line up
        // with the items we sent.
        queen::Error::Body(_) => ResponseError::UnknownServerError,
        // Unreachable on this path, and the arm is not a shrug: only the
        // fenced offset commit sends a conditional write ([`crate::cluster::fence`]),
        // and a produce is a push and not a key/value write. If one ever appeared here it
        // would be this facade's bug, so it is answered as one.
        queen::Error::Precondition { .. } => ResponseError::UnknownServerError,
    }
}

/// Everything an `acks=0` producer will never be told, said once in the log.
///
/// Fire-and-forget means the client asked not to hear about failures; it does
/// not mean nobody should. This is the only trace that a topic is being refused
/// or a push is failing for such a producer.
fn log_silent_failures(response: &ProduceResponse) {
    for topic in &response.responses {
        for p in &topic.partition_responses {
            if p.error_code != 0 {
                tracing::warn!(
                    target: "kafka",
                    topic = topic.name.0.as_str(),
                    partition = p.index,
                    error_code = p.error_code,
                    why = p.error_message.as_ref().map(|m| m.as_str()).unwrap_or(""),
                    "acks=0 produce failed; the producer will never hear about it"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::testing::FakeQueen;
    use crate::queen::Error;
    use crate::records::Decoded;
    use bytes::{Bytes, BytesMut};
    use kafka_protocol::indexmap::IndexMap;
    use kafka_protocol::messages::produce_request::TopicProduceData;
    use kafka_protocol::protocol::{Decodable, Encodable, Message};
    use kafka_protocol::records::{
        Compression, Record, RecordBatchEncoder, RecordEncodeOptions, TimestampType, NO_TIMESTAMP,
    };
    use std::sync::Arc;

    /// The idempotent-producer context a `stage` or `render` test needs, owned
    /// so the borrow outlives the call. A fresh tracker per test: the window is
    /// process-wide in production and must not be shared between tests here,
    /// where two of them producing sequence 0 would look like a duplicate.
    struct Window {
        producers: idempotent::Producers,
        tenant: crate::identity::TenantKey,
    }

    impl Window {
        fn new() -> Window {
            Window {
                producers: idempotent::Producers::new(),
                tenant: crate::identity::TenantKey::Tenant("test".into()),
            }
        }

        fn idem(&self) -> Idem<'_> {
            Idem {
                producers: &self.producers,
                tenant: &self.tenant,
            }
        }
    }

    // ------------------------------------------------------------- fixtures

    fn facade(queues: &[(&str, i64)], default_partitions: u32) -> (Facade, Arc<FakeQueen>) {
        let api = FakeQueen::with(queues);
        let facade = Facade {
            default_partitions,
            ..crate::handlers::testing::over(api.clone(), Default::default())
        };
        (facade, api)
    }

    /// A record the way a producer builds one.
    fn record(key: Option<&[u8]>, value: &[u8]) -> Record {
        Record {
            transactional: false,
            control: false,
            delete_horizon: false,
            partition_leader_epoch: -1,
            producer_id: idempotent::NO_PRODUCER_ID,
            producer_epoch: -1,
            timestamp_type: TimestampType::Creation,
            offset: 0,
            sequence: -1,
            timestamp: 1_756_000_000_000,
            key: key.map(Bytes::copy_from_slice),
            value: Some(Bytes::copy_from_slice(value)),
            headers: IndexMap::new(),
        }
    }

    /// Encode records into the batch bytes a partition entry carries, the way a
    /// CLIENT does it — `kafka-protocol`'s encoder is the fixture generator, so
    /// what the handler decodes is a batch built by the same code every Rust
    /// Kafka client would use.
    fn batch(records: &[Record], compression: Compression) -> Bytes {
        let mut out = BytesMut::new();
        RecordBatchEncoder::encode(
            &mut out,
            records.iter(),
            &RecordEncodeOptions {
                version: 2,
                compression,
            },
        )
        .expect("the client side encodes it");
        out.freeze()
    }

    /// A record from an IDEMPOTENT producer. The batch encoder reads the
    /// producer id, epoch and sequence off the first record of the batch, so
    /// these three fields become the batch HEADER the window is checked on.
    fn idempotent_record(producer_id: i64, epoch: i16, sequence: i32) -> Record {
        let mut r = record(None, b"v");
        r.producer_id = producer_id;
        r.producer_epoch = epoch;
        r.sequence = sequence;
        r
    }

    /// `[(topic, [(partition, batch bytes)])]` → a Produce request.
    fn request(topics: &[(&str, &[(i32, Bytes)])]) -> ProduceRequest {
        ProduceRequest::default()
            .with_acks(ACKS_ALL)
            .with_timeout_ms(30_000)
            .with_topic_data(
                topics
                    .iter()
                    .map(|(name, partitions)| {
                        TopicProduceData::default()
                            .with_name(TopicName(StrBytes::from_string(name.to_string())))
                            .with_partition_data(
                                partitions
                                    .iter()
                                    .map(|(index, records)| {
                                        PartitionProduceData::default()
                                            .with_index(*index)
                                            .with_records(Some(records.clone()))
                                    })
                                    .collect(),
                            )
                    })
                    .collect(),
            )
    }

    /// One topic, one partition, one batch of `records`.
    fn simple(topic: &str, partition: i32, records: &[Record]) -> ProduceRequest {
        request(&[(topic, &[(partition, batch(records, Compression::None))])])
    }

    fn answer<'a>(
        resp: &'a ProduceResponse,
        topic: &str,
        partition: i32,
    ) -> &'a PartitionProduceResponse {
        resp.responses
            .iter()
            .find(|t| t.name.0.as_str() == topic)
            .unwrap_or_else(|| panic!("{topic} is not in the response"))
            .partition_responses
            .iter()
            .find(|p| p.index == partition)
            .unwrap_or_else(|| panic!("{topic}/{partition} is not in the response"))
    }

    /// The payload of push item `i`, decoded back through the envelope.
    fn payload(api: &FakeQueen, i: usize) -> Decoded {
        crate::records::decode(&api.pushed()[i].payload, None)
    }

    // --------------------------------------------------------- the happy path

    #[tokio::test]
    async fn records_become_push_items_and_the_answer_is_the_base_offset() {
        let (f, api) = facade(&[("orders", 4)], 8);
        let records = [
            record(Some(b"a"), b"one"),
            record(Some(b"b"), b"two"),
            record(None, b"three"),
        ];
        let resp = handle(&f, &simple("orders", 2, &records), None)
            .await
            .expect("acks=-1 is answered");

        let pushed = api.pushed();
        assert_eq!(pushed.len(), 3);
        for item in &pushed {
            assert_eq!(item.queue, "orders");
            // Kafka partition n = Queen partition n, by name.
            assert_eq!(item.partition, "2");
        }
        assert_eq!(payload(&api, 0).value.unwrap(), Bytes::from_static(b"one"));
        assert_eq!(payload(&api, 1).key.unwrap(), Bytes::from_static(b"b"));
        assert_eq!(payload(&api, 2).key, None);

        let p = answer(&resp, "orders", 2);
        assert_eq!(p.error_code, 0);
        assert_eq!(p.base_offset, 0, "the first record of a fresh partition");
        assert_eq!(p.log_append_time_ms, -1);
        // A DELIBERATE difference from Apache Kafka, which reports the real log
        // start offset here (0 for this partition). A push answers no lower
        // bound, so the only alternatives are a bounds probe per produce — a
        // second call to Queen on the write path — or a guessed 0 that is wrong
        // on every partition whose retention has moved. -1 is the field's own
        // default in Kafka's schema and reads as "nothing to say"; nothing this
        // facade serves acts on it, because the one client that does is the
        // idempotent producer and that is refused. See [`appended`].
        assert_eq!(p.log_start_offset, -1);
        assert_eq!(
            api.fetches.lock().unwrap().len(),
            0,
            "a produce probed the bounds"
        );
    }

    /// The base offset is the FIRST record's, and the client counts from it —
    /// so a second produce to the same partition starts where the first ended.
    #[tokio::test]
    async fn the_base_offset_advances_with_the_log() {
        let (f, _) = facade(&[("orders", 1)], 4);
        let first = handle(
            &f,
            &simple("orders", 0, &[record(None, b"1"), record(None, b"2")]),
            None,
        )
        .await
        .unwrap();
        let second = handle(&f, &simple("orders", 0, &[record(None, b"3")]), None)
            .await
            .unwrap();
        assert_eq!(answer(&first, "orders", 0).base_offset, 0);
        assert_eq!(answer(&second, "orders", 0).base_offset, 2);
    }

    /// One request, several topics and several partitions: ONE push, and each
    /// partition answered with its own first record's offset.
    #[tokio::test]
    async fn one_request_is_one_push_across_every_topic_and_partition() {
        let (f, api) = facade(&[("orders", 4), ("clicks", 4)], 8);
        let req = request(&[
            (
                "orders",
                &[
                    (
                        0,
                        batch(
                            &[record(None, b"o0-a"), record(None, b"o0-b")],
                            Compression::None,
                        ),
                    ),
                    (3, batch(&[record(None, b"o3")], Compression::None)),
                ],
            ),
            (
                "clicks",
                &[(1, batch(&[record(None, b"c1")], Compression::None))],
            ),
        ]);
        let resp = handle(&f, &req, None).await.unwrap();

        assert_eq!(api.pushes.lock().unwrap().len(), 1, "not one push");
        assert_eq!(api.pushed().len(), 4);
        assert_eq!(answer(&resp, "orders", 0).base_offset, 0);
        assert_eq!(answer(&resp, "orders", 3).base_offset, 0);
        assert_eq!(answer(&resp, "clicks", 1).base_offset, 0);
        for t in ["orders", "clicks"] {
            for p in &resp
                .responses
                .iter()
                .find(|r| r.name.0.as_str() == t)
                .unwrap()
                .partition_responses
            {
                assert_eq!(p.error_code, 0, "{t}/{}", p.index);
            }
        }
    }

    /// Several batches for one partition in one request are processed in order,
    /// and the answer is the offset of the first record OVERALL.
    #[tokio::test]
    async fn several_batches_for_one_partition_are_one_run() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let mut records = BytesMut::new();
        records.extend_from_slice(&batch(
            &[record(None, b"1"), record(None, b"2")],
            Compression::None,
        ));
        records.extend_from_slice(&batch(&[record(None, b"3")], Compression::Gzip));
        let req = request(&[("orders", &[(0, records.freeze())])]);

        let resp = handle(&f, &req, None).await.unwrap();
        let values: Vec<Bytes> = (0..3).map(|i| payload(&api, i).value.unwrap()).collect();
        assert_eq!(values, [&b"1"[..], &b"2"[..], &b"3"[..]]);
        assert_eq!(answer(&resp, "orders", 0).base_offset, 0);
    }

    // ------------------------------------------------------------ compression

    /// `compression.type` is the PRODUCER's setting and arrives without
    /// negotiation, so every codec has to decode or a config line on the client
    /// becomes an undecodable batch.
    #[tokio::test]
    async fn every_codec_decodes() {
        for compression in [
            Compression::None,
            Compression::Gzip,
            Compression::Snappy,
            Compression::Lz4,
            Compression::Zstd,
        ] {
            let (f, api) = facade(&[("orders", 1)], 4);
            // Repetitive bytes, so the compressed forms really are compressed
            // and the codec is doing work rather than storing.
            let big = vec![b'q'; 4096];
            let records = [record(Some(b"k"), &big), record(None, b"tail")];
            let req = request(&[("orders", &[(0, batch(&records, compression))])]);

            let resp = handle(&f, &req, None).await.unwrap();
            assert_eq!(
                answer(&resp, "orders", 0).error_code,
                0,
                "{compression:?} was refused"
            );
            assert_eq!(api.pushed().len(), 2, "{compression:?}");
            assert_eq!(
                payload(&api, 0).value.unwrap(),
                Bytes::from(big),
                "{compression:?}"
            );
            assert_eq!(payload(&api, 1).value.unwrap(), Bytes::from_static(b"tail"));
        }
    }

    /// A producer may send the same header NAME twice — the header list is a
    /// list, and duplicate names are what it is for — and both values have to
    /// reach Queen. The loss this pins against was at DECODE, before the
    /// payload existed: the second value never reached the log, so no later
    /// read could have recovered it and neither side saw anything go wrong.
    ///
    /// The fixture is [`wire::encode`] because `kafka-protocol`'s encoder
    /// CANNOT build this batch — `Record.headers` is a map — which is the whole
    /// shape of the defect. `wire`'s own tests pin that encoder against the
    /// crate's, byte for byte, on every batch the crate can build.
    #[tokio::test]
    async fn a_repeated_header_name_reaches_queen_twice() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let sent: Vec<wire::Header> = vec![
            ("x".to_string(), Some(Bytes::from_static(b"1"))),
            ("y".to_string(), Some(Bytes::from_static(b"solo"))),
            ("x".to_string(), Some(Bytes::from_static(b"2"))),
            ("null-twice".to_string(), None),
            ("null-twice".to_string(), Some(Bytes::from_static(b""))),
        ];
        let records = [record(Some(b"k"), b"v")];
        let batch =
            wire::encode(&records, std::slice::from_ref(&sent)).expect("the fixture batch encodes");

        let resp = handle(&f, &request(&[("orders", &[(0, batch)])]), None)
            .await
            .unwrap();
        assert_eq!(answer(&resp, "orders", 0).error_code, 0);
        assert_eq!(api.pushed().len(), 1);
        assert_eq!(
            payload(&api, 0).headers,
            sent,
            "a header value was lost between the producer and the log"
        );
    }

    // ------------------------------------------------------------------ acks

    /// The acks=0 wire contract: no response frame at all, and the records are
    /// still written.
    #[tokio::test]
    async fn acks_zero_writes_and_answers_nothing() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let req = simple("orders", 0, &[record(None, b"v")]).with_acks(ACKS_NONE);
        assert!(handle(&f, &req, None).await.is_none());
        assert_eq!(api.pushed().len(), 1, "acks=0 must still write");
    }

    /// ...including when it fails: still no frame, and the failure is not
    /// invented into one.
    #[tokio::test]
    async fn acks_zero_is_silent_about_failures_too() {
        let (f, api) = facade(&[], 4);
        api.fail_with("connection refused");
        let req = simple("orders", 0, &[record(None, b"v")]).with_acks(ACKS_NONE);
        assert!(handle(&f, &req, None).await.is_none());
    }

    #[tokio::test]
    async fn acks_one_and_acks_all_take_the_same_path() {
        for acks in [ACKS_LEADER, ACKS_ALL] {
            let (f, api) = facade(&[("orders", 1)], 4);
            let req = simple("orders", 0, &[record(None, b"v")]).with_acks(acks);
            let resp = handle(&f, &req, None).await.expect("answered");
            assert_eq!(answer(&resp, "orders", 0).error_code, 0);
            assert_eq!(api.pushed().len(), 1);
        }
    }

    #[tokio::test]
    async fn an_invalid_acks_is_refused_and_writes_nothing() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let req = simple("orders", 0, &[record(None, b"v")]).with_acks(2);
        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::InvalidRequiredAcks.code()
        );
        assert!(api.pushed().is_empty());
    }

    // -------------------------------------------------------- what is refused

    #[tokio::test]
    async fn a_transactional_id_is_refused_before_anything_is_written() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let req = simple("orders", 0, &[record(None, b"v")])
            .with_transactional_id(Some(StrBytes::from_static_str("tx-1").into()));
        let resp = handle(&f, &req, None).await.unwrap();

        let p = answer(&resp, "orders", 0);
        assert_eq!(
            p.error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code()
        );
        assert!(p
            .error_message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("transactions"));
        assert!(
            api.pushed().is_empty(),
            "a transactional produce was written"
        );
        // The refusal is decided before Queen is touched at all, so a
        // transactional producer cannot even conjure the topic.
        assert!(
            api.created().is_empty(),
            "a refused produce created a queue"
        );
        assert_eq!(api.list_count(), 0, "a refused produce read the catalog");
    }

    /// The shape every Erlang producer sends. `kafka_protocol` encodes a NULL
    /// transactional id as a zero-length string (compat/brod/README.md), so the
    /// field arrives PRESENT and EMPTY on a produce that has nothing
    /// transactional about it. It is a plain produce, and the record is written.
    #[tokio::test]
    async fn an_empty_transactional_id_is_a_plain_produce() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let req = simple("orders", 0, &[record(None, b"v")])
            .with_transactional_id(Some(StrBytes::from_static_str("").into()));
        let resp = handle(&f, &req, None).await.unwrap();

        let p = answer(&resp, "orders", 0);
        assert_eq!(
            p.error_code, 0,
            "an empty transactional id was read as a transaction: {:?}",
            p.error_message
        );
        assert_eq!(
            api.pushed().len(),
            1,
            "an empty transactional id cost the record"
        );
    }

    /// The three shapes of the field side by side, so neither half of the line
    /// above can be widened or narrowed on its own: absent and empty are the
    /// same statement, and anything else is still refused with 53.
    #[tokio::test]
    async fn only_a_non_empty_transactional_id_is_refused() {
        let refused = ResponseError::TransactionalIdAuthorizationFailed.code();
        for (id, expected) in [(None, 0), (Some(""), 0), (Some("tx-1"), refused)] {
            let (f, api) = facade(&[("orders", 1)], 4);
            let mut req = simple("orders", 0, &[record(None, b"v")]);
            if let Some(id) = id {
                req = req.with_transactional_id(Some(StrBytes::from_string(id.to_string()).into()));
            }
            let resp = handle(&f, &req, None).await.unwrap();

            assert_eq!(
                answer(&resp, "orders", 0).error_code,
                expected,
                "transactional_id={id:?}"
            );
            assert_eq!(
                api.pushed().len(),
                usize::from(expected == 0),
                "transactional_id={id:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_transactional_batch_is_refused() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let mut r = record(None, b"v");
        r.transactional = true;
        // A transactional batch carries a producer id in practice; the flag
        // alone is what is being tested, so the id stays absent.
        let resp = handle(&f, &simple("orders", 0, &[r]), None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::TransactionalIdAuthorizationFailed.code()
        );
        assert!(api.pushed().is_empty());
    }

    #[tokio::test]
    async fn a_control_batch_is_refused() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let mut r = record(None, b"v");
        r.control = true;
        let resp = handle(&f, &simple("orders", 0, &[r]), None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::InvalidRecord.code()
        );
        assert!(api.pushed().is_empty());
    }

    /// One idempotent record, the way a stock Java producer sends it. Until
    /// M7 F3 this was UNSUPPORTED_FOR_MESSAGE_FORMAT and a message ending "Set
    /// enable.idempotence=false"; it is now the ordinary write path with the
    /// sequence window in front of it ([`crate::idempotent`]).
    #[tokio::test]
    async fn an_idempotent_producer_is_written_and_its_sequence_remembered() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let resp = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 0)]),
            None,
        )
        .await
        .unwrap();

        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, 0, "{:?}", p.error_message);
        assert_eq!(p.base_offset, 0);
        assert_eq!(api.pushed().len(), 1);
        assert_eq!(f.producers.tracked(), 1);
    }

    /// THE property: a resend of an appended batch is answered as the success it
    /// was, carrying the offsets the original got, and nothing is written twice.
    #[tokio::test]
    async fn an_idempotent_resend_is_answered_with_the_original_offsets() {
        let (f, api) = facade(&[("orders", 1)], 4);
        // Two records first, so the base offset the duplicate has to repeat is
        // not the zero every empty log would answer by accident.
        let first = simple("orders", 0, &[record(None, b"a"), record(None, b"b")]);
        handle(&f, &first, None).await.unwrap();

        let req = simple("orders", 0, &[idempotent_record(7, 0, 0)]);
        let one = handle(&f, &req, None).await.unwrap();
        let base = answer(&one, "orders", 0).base_offset;
        assert_eq!(base, 2);
        assert_eq!(api.pushed().len(), 3);

        let two = handle(&f, &req, None).await.unwrap();
        let p = answer(&two, "orders", 0);
        assert_eq!(p.error_code, 0, "a duplicate must be a success");
        assert_eq!(p.base_offset, base, "a duplicate must repeat the offsets");
        assert_eq!(
            api.pushed().len(),
            3,
            "a duplicate batch was written to the log a second time"
        );
    }

    /// The other half, and just as load-bearing: a batch that would leave a hole
    /// is refused and NOTHING is written, which is what makes the Java client
    /// re-drain and resend in order.
    #[tokio::test]
    async fn an_idempotent_gap_is_refused_and_nothing_is_written() {
        let (f, api) = facade(&[("orders", 1)], 4);
        handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 0)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(api.pushed().len(), 1);

        let resp = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 5)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::OutOfOrderSequenceNumber.code()
        );
        assert_eq!(api.pushed().len(), 1, "a gapped batch was written");
        // ...and the batch that WAS next still is.
        let next = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 1)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(answer(&next, "orders", 0).error_code, 0);
    }

    /// A window this facade never had — a restart, an eviction, a facade switch.
    /// OUT_OF_ORDER and not UNKNOWN_PRODUCER_ID, because OUT_OF_ORDER is the
    /// code whose recovery (KIP-360's epoch bump) every client implements.
    #[tokio::test]
    async fn a_producer_this_facade_never_saw_is_out_of_order_not_unknown() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let resp = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 42)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::OutOfOrderSequenceNumber.code()
        );
        assert!(api.pushed().is_empty());
    }

    /// A producer that bumped its epoch (KIP-360) resets its own sequences, and
    /// the facade accepts the reset rather than reading it as a duplicate.
    #[tokio::test]
    async fn a_bumped_epoch_resets_the_sequence_and_is_accepted() {
        let (f, api) = facade(&[("orders", 1)], 4);
        handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 0)]),
            None,
        )
        .await
        .unwrap();
        let resp = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 1, 0)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(answer(&resp, "orders", 0).error_code, 0);
        assert_eq!(api.pushed().len(), 2);
        // ...and the OLD epoch is fenced from then on.
        let stale = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(7, 0, 1)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            answer(&stale, "orders", 0).error_code,
            ResponseError::InvalidProducerEpoch.code()
        );
    }

    /// Producer id 0 is a REAL producer id and only -1 means "none". The
    /// off-by-one that would let the very first idempotent producer through is
    /// visible here as the difference between a checked sequence and an
    /// unchecked one.
    #[tokio::test]
    async fn producer_id_zero_is_a_producer_id() {
        let (f, _) = facade(&[("orders", 1)], 4);
        // Producer 0 at sequence 5, with no window: checked, and refused.
        let resp = handle(
            &f,
            &simple("orders", 0, &[idempotent_record(0, 0, 5)]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::OutOfOrderSequenceNumber.code()
        );

        // ...while -1 with the same sequence is not an idempotent produce at
        // all, and is written without a word.
        let mut plain = record(None, b"v");
        plain.sequence = 5;
        let ok = handle(&f, &simple("orders", 0, &[plain]), None)
            .await
            .unwrap();
        assert_eq!(answer(&ok, "orders", 0).error_code, 0);
        assert_eq!(f.producers.tracked(), 0);
    }

    #[tokio::test]
    async fn a_corrupt_batch_is_corrupt_not_a_server_error() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let mut bytes = BytesMut::from(&batch(&[record(None, b"v")], Compression::None)[..]);
        // Flip a byte inside the CRC-covered region.
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let req = request(&[("orders", &[(0, bytes.freeze())])]);

        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::CorruptMessage.code()
        );
        assert!(api.pushed().is_empty());
    }

    /// Bytes that are not a v2 batch at all: a client speaking a message set
    /// older than the Produce version it negotiated.
    #[tokio::test]
    async fn a_pre_v2_message_set_is_unsupported_not_corrupt() {
        let (f, _) = facade(&[("orders", 1)], 4);
        // A v1 message-set header: 8 bytes of offset, 4 of size, then a magic
        // byte of 1 where a RecordBatch would carry 2.
        let mut legacy = BytesMut::new();
        legacy.extend_from_slice(&0i64.to_be_bytes());
        legacy.extend_from_slice(&30i32.to_be_bytes());
        legacy.extend_from_slice(&0u32.to_be_bytes()); // crc
        legacy.extend_from_slice(&[1u8]); // magic
        legacy.extend_from_slice(&[0u8; 25]);
        let req = request(&[("orders", &[(0, legacy.freeze())])]);

        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::UnsupportedForMessageFormat.code()
        );
    }

    // ------------------------------------------------------------- topics

    #[tokio::test]
    async fn an_absent_topic_is_auto_created_and_then_written() {
        let (f, api) = facade(&[], 16);
        let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
            .await
            .unwrap();
        assert_eq!(api.created(), ["orders"]);
        assert_eq!(answer(&resp, "orders", 0).error_code, 0);
        assert_eq!(api.pushed().len(), 1);
    }

    /// A `__` name belongs to Kafka's own bookkeeping. It is never created and
    /// never written to, whatever else the request carries.
    #[tokio::test]
    async fn a_reserved_topic_is_unknown_and_never_created() {
        let (f, api) = facade(&[], 16);
        let resp = handle(
            &f,
            &simple("__consumer_offsets", 0, &[record(None, b"v")]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            answer(&resp, "__consumer_offsets", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(api.created().is_empty());
        assert!(api.pushed().is_empty());
    }

    #[tokio::test]
    async fn a_partition_outside_the_advertised_width_is_unknown() {
        let (f, api) = facade(&[("orders", 2)], 4);
        // Width is max(2 live, 4 configured) = 4, so 0..=3 exist and 4 does not.
        let resp = handle(&f, &simple("orders", 4, &[record(None, b"v")]), None)
            .await
            .unwrap();
        assert_eq!(
            answer(&resp, "orders", 4).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert!(api.pushed().is_empty());

        // ...and the last lane that IS advertised works.
        let ok = handle(&f, &simple("orders", 3, &[record(None, b"v")]), None)
            .await
            .unwrap();
        assert_eq!(answer(&ok, "orders", 3).error_code, 0);
    }

    #[tokio::test]
    async fn queen_unreachable_is_retriable_and_writes_nothing() {
        let (f, api) = facade(&[], 4);
        api.fail_with("connection refused");
        let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
            .await
            .unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::LeaderNotAvailable.code()
        );
        assert!(api.pushed().is_empty());
    }

    // ------------------------------------------------------ error isolation

    /// The property Kafka clients are built on: one bad partition does not
    /// poison the request. The good ones are written and answered normally, in
    /// the SAME push.
    #[tokio::test]
    async fn one_bad_partition_does_not_poison_the_request() {
        let (f, api) = facade(&[("orders", 2), ("clicks", 2)], 4);
        // An idempotent batch this facade holds no window for: refused
        // OUT_OF_ORDER_SEQUENCE_NUMBER, and its neighbours are still written.
        let stranded = idempotent_record(12, 0, 9);
        let mut corrupt = BytesMut::from(&batch(&[record(None, b"bad")], Compression::None)[..]);
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;

        let req = request(&[
            (
                "orders",
                &[
                    (0, batch(&[record(None, b"good-0")], Compression::None)),
                    (1, batch(&[stranded], Compression::None)),
                    (2, corrupt.freeze()),
                    (
                        9,
                        batch(&[record(None, b"out of range")], Compression::None),
                    ),
                ],
            ),
            (
                "__internal",
                &[(0, batch(&[record(None, b"reserved")], Compression::None))],
            ),
            (
                "clicks",
                &[(1, batch(&[record(None, b"good-1")], Compression::None))],
            ),
        ]);

        let resp = handle(&f, &req, None).await.unwrap();

        assert_eq!(answer(&resp, "orders", 0).error_code, 0);
        assert_eq!(answer(&resp, "orders", 0).base_offset, 0);
        assert_eq!(
            answer(&resp, "orders", 1).error_code,
            ResponseError::OutOfOrderSequenceNumber.code()
        );
        assert_eq!(
            answer(&resp, "orders", 2).error_code,
            ResponseError::CorruptMessage.code()
        );
        assert_eq!(
            answer(&resp, "orders", 9).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(
            answer(&resp, "__internal", 0).error_code,
            ResponseError::UnknownTopicOrPartition.code()
        );
        assert_eq!(answer(&resp, "clicks", 1).error_code, 0);
        assert_eq!(answer(&resp, "clicks", 1).base_offset, 0);

        // Exactly the two good records were written, in one push.
        assert_eq!(api.pushes.lock().unwrap().len(), 1);
        let values: Vec<Bytes> = (0..2).map(|i| payload(&api, i).value.unwrap()).collect();
        assert_eq!(values, [&b"good-0"[..], &b"good-1"[..]]);
        // A rejected partition still carries no base offset.
        for (t, p) in [
            ("orders", 1),
            ("orders", 2),
            ("orders", 9),
            ("__internal", 0),
        ] {
            assert_eq!(answer(&resp, t, p).base_offset, -1, "{t}/{p}");
        }
    }

    /// Every partition of the request is answered, in the order it was asked —
    /// clients match the response against what they sent.
    #[tokio::test]
    async fn every_requested_partition_is_answered_in_order() {
        let (f, _) = facade(&[("orders", 4)], 8);
        let req = request(&[(
            "orders",
            &[
                (3, batch(&[record(None, b"a")], Compression::None)),
                (0, batch(&[record(None, b"b")], Compression::None)),
                (1, batch(&[record(None, b"c")], Compression::None)),
            ],
        )]);
        let resp = handle(&f, &req, None).await.unwrap();
        let order: Vec<i32> = resp.responses[0]
            .partition_responses
            .iter()
            .map(|p| p.index)
            .collect();
        assert_eq!(order, [3, 0, 1]);
    }

    // ------------------------------------------------- the expansion ceiling

    /// A compressed batch says nothing about what it costs to decode, so the
    /// ceiling is on the OUTPUT (see [`crate::decompress`]). Driven through
    /// `stage` with a small budget rather than through `handle` with the real
    /// one, because the real one is a hundred megabytes and the wiring is the
    /// same either way: the refusal is MESSAGE_TOO_LARGE and nothing is staged.
    #[test]
    fn a_batch_that_expands_past_the_budget_is_refused_as_too_large() {
        let raw = batch(&[record(None, &vec![0u8; 256 * 1024])], Compression::Zstd);
        assert!(raw.len() < 4096, "the fixture is meant to be a small batch");

        let mut items = Vec::new();
        let budget = decompress::Budget::new(64 * 1024, MAX_RECORDS_PER_REQUEST);
        let p = PartitionProduceData::default()
            .with_index(0)
            .with_records(Some(raw));
        match stage(
            &mut items,
            "orders",
            Plan::Serve(4),
            &p,
            &budget,
            &Window::new().idem(),
        ) {
            Slot::Reject(e, why) => {
                assert_eq!(e, ResponseError::MessageTooLarge);
                assert!(why.contains("decompress"), "{why}");
            }
            other => panic!("a decompression bomb was staged as {other:?}"),
        }
        assert!(items.is_empty(), "a refused batch staged records anyway");
    }

    /// The OTHER number a batch header can lie about. `record_count` is trusted
    /// far enough to be reserved for before a record is read, so a batch header
    /// alone can ask for the whole machine; it is charged before the decode.
    #[test]
    fn a_declared_record_count_is_charged_before_anything_is_decoded() {
        let raw = batch(
            &[record(None, b"a"), record(None, b"b"), record(None, b"c")],
            Compression::None,
        );
        let mut items = Vec::new();
        // Room for two records; the batch says three.
        let budget = decompress::Budget::new(1024 * 1024, 2);
        let p = PartitionProduceData::default()
            .with_index(0)
            .with_records(Some(raw));
        match stage(
            &mut items,
            "orders",
            Plan::Serve(4),
            &p,
            &budget,
            &Window::new().idem(),
        ) {
            Slot::Reject(e, why) => {
                assert_eq!(e, ResponseError::MessageTooLarge);
                assert!(why.contains("declare"), "{why}");
            }
            other => panic!("an over-declared batch was staged as {other:?}"),
        }
        assert!(items.is_empty(), "a refused batch staged records anyway");
    }

    /// And that ceiling is the request's too — one batch header per partition
    /// would otherwise multiply it.
    #[test]
    fn the_declared_record_ceiling_is_shared_across_the_request() {
        let entry = |index: i32| {
            PartitionProduceData::default()
                .with_index(index)
                .with_records(Some(batch(
                    &[record(None, b"a"), record(None, b"b")],
                    Compression::None,
                )))
        };
        let mut items = Vec::new();
        let w = Window::new();
        let budget = decompress::Budget::new(1024 * 1024, 3);
        assert!(matches!(
            stage(
                &mut items,
                "orders",
                Plan::Serve(4),
                &entry(0),
                &budget,
                &w.idem()
            ),
            Slot::Push { .. }
        ));
        assert!(matches!(
            stage(
                &mut items,
                "orders",
                Plan::Serve(4),
                &entry(1),
                &budget,
                &w.idem()
            ),
            Slot::Reject(ResponseError::MessageTooLarge, _)
        ));
        assert_eq!(items.len(), 2, "only the first entry may have been staged");
    }

    /// And the ceiling belongs to the REQUEST: two partitions that each fit are
    /// still refused when together they do not, which is what stops a request
    /// multiplying the budget by the number of entries it carries.
    #[test]
    fn the_expansion_ceiling_is_shared_across_the_request() {
        let value = vec![0u8; 32 * 1024];
        let entry = |index: i32| {
            PartitionProduceData::default()
                .with_index(index)
                .with_records(Some(batch(&[record(None, &value)], Compression::Zstd)))
        };

        let mut items = Vec::new();
        let w = Window::new();
        let budget = decompress::Budget::new(48 * 1024, MAX_RECORDS_PER_REQUEST);
        assert!(matches!(
            stage(
                &mut items,
                "orders",
                Plan::Serve(4),
                &entry(0),
                &budget,
                &w.idem()
            ),
            Slot::Push { .. }
        ));
        assert!(matches!(
            stage(
                &mut items,
                "orders",
                Plan::Serve(4),
                &entry(1),
                &budget,
                &w.idem()
            ),
            Slot::Reject(ResponseError::MessageTooLarge, _)
        ));
        assert_eq!(items.len(), 1, "only the first entry may have been staged");
    }

    // ------------------------------------------------ the push response itself

    /// The C1 contract is checked, not trusted: an item the broker allocated no
    /// offset for, and cannot explain, fails that partition loudly rather than
    /// being answered with a guess.
    #[tokio::test]
    async fn a_missing_offset_fails_the_partition_instead_of_guessing() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.reply_push(vec![Pushed {
            status: "queued".to_string(),
            offset: None,
        }]);
        let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
            .await
            .unwrap();
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::UnknownServerError.code());
        assert_eq!(p.base_offset, -1);
        assert!(p
            .error_message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("queued"));
    }

    /// ...but the spool is not that. A broker in maintenance mode answers 201
    /// with `status: "buffered"` and no offset, and those records ARE stored:
    /// answering the producer a fatal UNKNOWN_SERVER_ERROR would report data
    /// loss that did not happen and make every retry a duplicate. REQUEST_TIMED_OUT
    /// is retriable and is the truth — the offset is not known yet.
    #[tokio::test]
    async fn a_spooled_push_is_retriable_and_not_a_delivery_failure() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.reply_push(vec![
            Pushed {
                status: SPOOLED.to_string(),
                offset: None,
            },
            Pushed {
                status: SPOOLED.to_string(),
                offset: None,
            },
        ]);
        let req = simple("orders", 0, &[record(None, b"a"), record(None, b"b")]);
        let resp = handle(&f, &req, None).await.unwrap();
        let p = answer(&resp, "orders", 0);
        assert_eq!(p.error_code, ResponseError::RequestTimedOut.code());
        assert!(
            ResponseError::RequestTimedOut.is_retriable(),
            "the whole point of the code is that a producer retries it"
        );
        assert_eq!(p.base_offset, -1);
        assert!(p
            .error_message
            .as_ref()
            .unwrap()
            .as_str()
            .contains("maintenance"));
    }

    /// The spool answer is read off the item's OWN status, so a run whose second
    /// item is spooled is answered the same way as one whose first is.
    #[tokio::test]
    async fn a_spool_that_starts_mid_run_is_still_the_spool() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.reply_push(vec![
            Pushed {
                status: "queued".to_string(),
                offset: Some(10),
            },
            Pushed {
                status: SPOOLED.to_string(),
                offset: None,
            },
        ]);
        let req = simple("orders", 0, &[record(None, b"a"), record(None, b"b")]);
        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::RequestTimedOut.code()
        );
    }

    /// ...and so is the contiguity the base offset means. A run that is not
    /// `base, base+1, …` describes records no single base offset addresses.
    #[tokio::test]
    async fn a_non_contiguous_run_fails_the_partition() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.reply_push(vec![
            Pushed {
                status: "queued".to_string(),
                offset: Some(10),
            },
            Pushed {
                status: "queued".to_string(),
                offset: Some(12),
            },
        ]);
        let req = simple("orders", 0, &[record(None, b"a"), record(None, b"b")]);
        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::UnknownServerError.code()
        );
    }

    /// A push answer that does not cover the items it was given must not panic
    /// the handler — nothing a broker sends back may.
    #[tokio::test]
    async fn a_short_push_answer_fails_the_partition_instead_of_panicking() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.reply_push(vec![Pushed {
            status: "queued".to_string(),
            offset: Some(0),
        }]);
        let req = simple("orders", 0, &[record(None, b"a"), record(None, b"b")]);
        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::UnknownServerError.code()
        );
    }

    /// A missing offset on ONE partition is that partition's problem: the
    /// others in the same push still get their base offsets.
    #[tokio::test]
    async fn a_missing_offset_does_not_poison_the_other_partitions() {
        let (f, api) = facade(&[("orders", 4)], 8);
        api.reply_push(vec![
            Pushed {
                status: "queued".to_string(),
                offset: Some(5),
            },
            Pushed {
                status: "error".to_string(),
                offset: None,
            },
            Pushed {
                status: "queued".to_string(),
                offset: Some(9),
            },
        ]);
        let req = request(&[(
            "orders",
            &[
                (0, batch(&[record(None, b"a")], Compression::None)),
                (1, batch(&[record(None, b"b")], Compression::None)),
                (2, batch(&[record(None, b"c")], Compression::None)),
            ],
        )]);
        let resp = handle(&f, &req, None).await.unwrap();
        assert_eq!(answer(&resp, "orders", 0).base_offset, 5);
        assert_eq!(
            answer(&resp, "orders", 1).error_code,
            ResponseError::UnknownServerError.code()
        );
        assert_eq!(answer(&resp, "orders", 2).base_offset, 9);
    }

    /// Every HTTP failure of the push maps to the closest Kafka code, on every
    /// partition the push carried.
    #[tokio::test]
    async fn a_failed_push_maps_to_the_closest_kafka_code() {
        let cases: [(Error, ResponseError); 8] = [
            (
                Error::Transport("connection reset".into()),
                ResponseError::RequestTimedOut,
            ),
            (
                Error::status(413, "too large"),
                ResponseError::MessageTooLarge,
            ),
            (
                Error::status(401, "no token"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(403, "forbidden"),
                ResponseError::TopicAuthorizationFailed,
            ),
            (
                Error::status(404, "gone"),
                ResponseError::UnknownTopicOrPartition,
            ),
            // Retriable in EVERY client's produce path, which
            // THROTTLING_QUOTA_EXCEEDED is not — see `a_throttled_push_...`
            // below and [`crate::throttle`].
            (Error::status(429, "frozen"), ResponseError::RequestTimedOut),
            (
                Error::status(503, "draining"),
                ResponseError::LeaderNotAvailable,
            ),
            (
                Error::status(500, "boom"),
                ResponseError::UnknownServerError,
            ),
        ];
        for (queen_error, kafka) in cases {
            let (f, api) = facade(&[("orders", 1)], 4);
            api.fail_push(queen_error.clone());
            let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
                .await
                .unwrap();
            let p = answer(&resp, "orders", 0);
            assert_eq!(p.error_code, kafka.code(), "{queen_error}");
            assert_eq!(p.base_offset, -1, "{queen_error}");
        }
    }

    /// A push whose answer does not line up with the items sent is a Body
    /// error, not a shifted set of offsets.
    #[test]
    fn base_offset_is_only_reported_when_the_whole_run_is_there() {
        assert!(base_offset(&[]).is_err());
        assert_eq!(
            base_offset(&[Pushed {
                status: "queued".into(),
                offset: Some(7)
            }])
            .unwrap(),
            7
        );
        assert_eq!(
            base_offset(&[
                Pushed {
                    status: "queued".into(),
                    offset: Some(7)
                },
                Pushed {
                    status: "queued".into(),
                    offset: Some(8)
                },
                Pushed {
                    status: "queued".into(),
                    offset: Some(9)
                },
            ])
            .unwrap(),
            7
        );
        // A duplicate reports the ORIGINAL occurrence's offset (C1), which
        // breaks the run — and is caught rather than averaged over.
        assert!(base_offset(&[
            Pushed {
                status: "queued".into(),
                offset: Some(7)
            },
            Pushed {
                status: "duplicate".into(),
                offset: Some(3)
            },
        ])
        .is_err());
    }

    // ------------------------------------------------------------ empty input

    /// A partition entry with no records writes nothing and fails nothing.
    #[tokio::test]
    async fn an_empty_partition_entry_is_answered_without_an_offset() {
        let (f, api) = facade(&[("orders", 1)], 4);
        for records in [None, Some(Bytes::new())] {
            let req = ProduceRequest::default()
                .with_acks(ACKS_ALL)
                .with_topic_data(vec![TopicProduceData::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partition_data(vec![PartitionProduceData::default()
                        .with_index(0)
                        .with_records(records.clone())])]);
            let resp = handle(&f, &req, None).await.unwrap();
            let p = answer(&resp, "orders", 0);
            assert_eq!(p.error_code, 0, "{records:?}");
            assert_eq!(p.base_offset, -1, "{records:?}");
        }
        assert!(
            api.pushes.lock().unwrap().is_empty(),
            "an empty request pushed"
        );
    }

    // ---------------------------------------------------------------- the wire

    /// Every advertised version encodes and decodes cleanly, both ways: the
    /// request is built by the CLIENT half of `kafka-protocol` and decoded by
    /// the broker half, and the response makes the same trip back.
    #[tokio::test]
    async fn the_exchange_round_trips_at_every_advertised_version() {
        let (f, _) = facade(&[("orders", 4)], 8);
        let row = crate::versions::lookup(kafka_protocol::messages::ApiKey::Produce as i16)
            .expect("Produce is advertised");
        assert!(row.min >= ProduceRequest::VERSIONS.min && row.max <= ProduceRequest::VERSIONS.max);

        for version in row.min..=row.max {
            let req = simple("orders", 1, &[record(Some(b"k"), b"v"), record(None, b"w")]);
            let mut wire = BytesMut::new();
            req.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode request v{version}: {e}"));
            let mut buf = wire.freeze();
            let decoded = ProduceRequest::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode request v{version}: {e}"));
            assert!(
                buf.is_empty(),
                "v{version}: {} trailing request bytes",
                buf.len()
            );

            let resp = handle(&f, &decoded, None).await.expect("answered");
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version)
                .unwrap_or_else(|e| panic!("encode response v{version}: {e}"));
            let mut buf = wire.freeze();
            let back = ProduceResponse::decode(&mut buf, version)
                .unwrap_or_else(|e| panic!("decode response v{version}: {e}"));
            assert!(
                buf.is_empty(),
                "v{version}: {} trailing response bytes",
                buf.len()
            );

            let p = answer(&back, "orders", 1);
            assert_eq!(p.error_code, 0, "v{version}");
            assert_eq!(p.index, 1, "v{version}");
            assert_eq!(p.log_append_time_ms, -1, "v{version}");
            if version >= 5 {
                assert_eq!(p.log_start_offset, -1, "v{version}");
            }
        }
    }

    /// The refusal message rides along from v8, where the field exists, and is
    /// dropped without complaint below it.
    #[tokio::test]
    async fn the_error_message_survives_from_v8() {
        let (f, _) = facade(&[("orders", 1)], 4);
        let req = simple("orders", 0, &[record(None, b"v")]).with_acks(2);
        let resp = handle(&f, &req, None).await.unwrap();
        for version in [3i16, 7, 8, 9] {
            let mut wire = BytesMut::new();
            resp.encode(&mut wire, version).unwrap();
            let mut buf = wire.freeze();
            let back = ProduceResponse::decode(&mut buf, version).unwrap();
            let p = answer(&back, "orders", 0);
            assert_eq!(p.error_code, ResponseError::InvalidRequiredAcks.code());
            let message = p.error_message.as_ref().map(|m| m.as_str());
            if version >= 8 {
                assert!(message.unwrap_or("").contains("acks=2"), "v{version}");
            } else {
                assert_eq!(message, None, "v{version}: a v8 field was encoded");
            }
        }
    }

    /// The token reaches the data path, not only the admin calls — the M5 seam.
    #[tokio::test]
    async fn the_token_reaches_the_push() {
        let (f, api) = facade(&[("orders", 1)], 4);
        handle(
            &f,
            &simple("orders", 0, &[record(None, b"v")]),
            Some("tenant-a"),
        )
        .await;
        let tokens = api.tokens.lock().unwrap().clone();
        assert!(!tokens.is_empty());
        assert!(tokens.iter().all(|t| t.as_deref() == Some("tenant-a")));
    }

    /// A record with no timestamp is a record with no timestamp — the envelope
    /// does not invent one on the way in.
    #[tokio::test]
    async fn a_record_without_a_timestamp_is_stored_without_one() {
        let (f, api) = facade(&[("orders", 1)], 4);
        let mut r = record(None, b"v");
        r.timestamp = NO_TIMESTAMP;
        handle(&f, &simple("orders", 0, &[r]), None).await;
        assert!(api.pushed()[0].payload.get("t").is_none());
    }

    /// Cloud back-pressure on the write path: a 429 becomes the wait every
    /// producer already sleeps on, beside a code every producer already
    /// retries. See [`crate::throttle`].
    #[tokio::test]
    async fn a_throttled_push_answers_a_wait_and_a_retriable_code() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.fail_push(Error::Status {
            code: 429,
            body: r#"{"error":"request rate limit exceeded","code":"rate_limited"}"#.into(),
            retry_after_ms: Some(5_000),
        });
        let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
            .await
            .unwrap();

        assert_eq!(
            resp.throttle_time_ms, 5_000,
            "the proxy's Retry-After did not reach the client"
        );
        let p = answer(&resp, "orders", 0);
        assert_eq!(
            p.error_code,
            ResponseError::RequestTimedOut.code(),
            "the code beside the throttle is not one every producer retries"
        );
        assert_eq!(p.base_offset, -1);
    }

    /// ...and with no hint, the default rather than nothing: a throttle of zero
    /// is a producer retrying flat out against a cap it has already hit.
    #[tokio::test]
    async fn a_throttle_without_a_hint_still_asks_for_a_wait() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.fail_push(Error::status(429, "rate_limited"));
        let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
            .await
            .unwrap();
        assert_eq!(resp.throttle_time_ms, crate::throttle::DEFAULT_MS);
    }

    /// A push that simply failed is not a throttle: nothing tells the client to
    /// sleep, because nothing said when to come back.
    #[tokio::test]
    async fn an_ordinary_failure_carries_no_throttle() {
        let (f, api) = facade(&[("orders", 1)], 4);
        api.fail_push(Error::status(503, "draining"));
        let resp = handle(&f, &simple("orders", 0, &[record(None, b"v")]), None)
            .await
            .unwrap();
        assert_eq!(resp.throttle_time_ms, 0);
        assert_eq!(
            answer(&resp, "orders", 0).error_code,
            ResponseError::LeaderNotAvailable.code()
        );
    }
}
