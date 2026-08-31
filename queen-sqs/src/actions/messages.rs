//! Send, receive, delete, and the visibility timeout.
//!
//! CONTRACT. This is where the one real model mismatch is resolved, and every
//! function below depends on the resolution, so it is stated here once:
//!
//! > Queen's durable lease is a per-(partition, consumer-group) claim over a
//! > contiguous offset span with a monotonic ack cursor; SQS visibility is per
//! > message. **The two models coincide at claim width 1.**
//!
//! So `ReceiveMessage` in the default `exact` mode is up to N parallel
//! `batch=1` pops (N = `MaxNumberOfMessages` ≤ 10), and at width 1 every SQS
//! verb becomes exact rather than approximate:
//!
//!   * `ChangeMessageVisibility` is `POST /lease/:id/extend` — that lease holds
//!     ONE message, so extending it extends exactly what the caller named;
//!   * a visibility timeout of ZERO is ack `retry`, which releases the message
//!     and charges nothing — the difference between a terminate and a failure;
//!   * `DeleteMessage` is ack `completed`, with no gap to swallow.
//!
//! The cost is one pop write-transaction per message, which is honest: SQS is a
//! chatty ≤10-batch protocol and its own clients poll. `QUEEN_SQS_RECEIVE_MODE=
//! amortized` would trade that for one pop over k lanes and admit two bounded
//! divergences — extending one message extends its pop-mates, terminating one
//! returns the others as duplicates — both inside SQS's own at-least-once
//! envelope. It needs `maxPerPartition` on the broker's pop (C-SQS-1), which is
//! NOT implemented, so the variable is REFUSED at boot
//! ([`crate::config::Config::from_source`]) rather than accepted and served as
//! `exact` under the other mode's name. `exact` is the only mode this build
//! serves and it is the only one it will start in.
//!
//! The other decisions this module implements:
//!
//!   * **Receipt handles are self-contained** ([`crate::handle`]), so a delete
//!     is served by any instance;
//!   * **`DelaySeconds` per message is the timers API**, `timerKey` = the send's
//!     own dedup key, payload = the envelope base64'd;
//!   * **Redrive is facade-driven**: on receive, a message whose
//!     `deliveryAttempt` exceeds the queue's `maxReceiveCount` is NOT returned —
//!     it is MOVED, push-to-DLQ plus ack-original in ONE
//!     `POST /api/v1/transaction`, with a fresh `transactionId` so the
//!     destination's dedup window cannot swallow the move and the original
//!     MessageId riding in the envelope;
//!   * **Out-of-order deletes** inside a FIFO batch are buffered as a
//!     per-(partition, leaseId) delete-set in KV (`qs:ds:`, TTL = lease + slack)
//!     and the contiguous prefix is acked — in KV and not in memory, because any
//!     instance must be able to serve the delete. That whole half of the model
//!     lives in [`super::fifo`], which every `.fifo` queue's receive, delete and
//!     visibility change routes through: a FIFO claim is a RUN of one group, so
//!     "one lease, one message" — the sentence the rest of this module rests on
//!     — is exactly what does not hold there.
//!
//! ## The three facts a reader of this file needs
//!
//! **A lane is chosen from the send's own dedup key, not from its MessageId.**
//! The MessageId is the BROKER's message uuid and does not exist until the push
//! has landed, so it cannot address the lane the push is going to. Every send
//! therefore mints one identifier up front — a fresh uuid on a standard queue,
//! the `MessageDeduplicationId` (or the SHA-256 of the body) on a FIFO one — and
//! that identifier is BOTH the push's `transactionId` and, hashed across the
//! queue's width, its lane. Spread is uniform because the value is a uuid, and
//! deterministic because a hash is.
//!
//! **A receipt handle outlives its own delivery's visibility.**
//! `ChangeMessageVisibility` extends a lease without re-issuing the handle the
//! client holds, so a handle minted to expire with its first visibility window
//! would refuse the delete of a message the client legitimately kept in flight
//! for hours. The handle's expiry is therefore SQS's own in-flight ceiling
//! (twelve hours) and the lease is what actually decides whether a delete lands.
//!
//! **`DeleteMessage` answers success for a stale handle**, which is not
//! laxity but AWS's documented contract: *"you must provide the most recently
//! received ReceiptHandle … (otherwise, the request succeeds, but the message
//! might not be deleted)"*. Only a handle this facade did not mint — forged,
//! truncated, or minted for another queue — is `ReceiptHandleIsInvalid`.

use std::collections::BTreeMap;
use std::task::Poll;

use base64::Engine;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::actions::{dlq, fifo, Ctx};
use crate::config::ReceiveMode;
use crate::envelope::{self, AttributeValue, Envelope, MessageAttribute};
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::handle::Receipt;
use crate::md5;
use crate::obs::Sampler;
use crate::queen::{self, AckItem, BoxFuture, PopOptions, PushItem, TimerSchedule};
use crate::registry::{Naming, QueueRecord};

/// SQS's own ceiling on one receive. The broker would allow far more; this is
/// the protocol's number and the facade stays inside it.
pub const MAX_RECEIVE_MESSAGES: i32 = 10;
/// SQS's ceiling on `WaitTimeSeconds`. The broker's pop allows 30.
pub const MAX_WAIT_SECONDS: i32 = 20;
/// SQS's ceiling on `VisibilityTimeout`, twelve hours.
pub const MAX_VISIBILITY_SECONDS: i32 = 43_200;
/// SQS's ceiling on per-message `DelaySeconds`, fifteen minutes. The timers
/// horizon is ninety days, so this is the binding one by a wide margin.
pub const MAX_DELAY_SECONDS: i32 = 900;
/// The default `MaximumMessageSize`, and the maximum one AWS raised to 1 MiB in
/// August 2025.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 262_144;
pub const MAX_MAX_MESSAGE_BYTES: usize = 1_048_576;

/// The visibility timeout a queue that never set one has. AWS's own default, and
/// it is here rather than in the registry because it is what a RECEIVE falls back
/// to — the registry stores what was set, not what applies.
pub const DEFAULT_VISIBILITY_SECONDS: i64 = 30;

/// The 10-entry cap every batch action shares. Same number as
/// [`crate::actions::MAX_BATCH_ENTRIES`], and the errors either side of it are
/// different errors because an SDK's batching helper branches on which.
pub const MAX_BATCH_ENTRIES: usize = 10;
/// The longest batch entry id AWS accepts.
pub const MAX_BATCH_ENTRY_ID_LEN: usize = 80;
/// The longest message attribute name, and the longest type label.
pub const MAX_ATTRIBUTE_NAME_LEN: usize = 256;

/// The one system attribute AWS defines. Anything else under
/// `MessageSystemAttributes` is refused rather than stored: a client that
/// invented one would be storing a field no consumer can ask for.
pub const AWS_TRACE_HEADER: &str = "AWSTraceHeader";

/// The longest `MessageGroupId` and `MessageDeduplicationId` AWS accepts, and
/// the punctuation it allows beside alphanumerics. Both travel onward as a Queen
/// PARTITION NAME and a `transactionId` respectively, so unvalidated input here
/// is client text deciding a broker identifier — and a control character in one
/// comes back as the broker's own 400 under a message about nothing the sender
/// wrote.
pub const MAX_FIFO_ID_LEN: usize = 128;
const FIFO_ID_PUNCTUATION: &str = "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~";

/// The prefix on every timer key this facade mints.
///
/// It is what makes `ApproximateNumberOfMessagesDelayed` ANSWERABLE at all: the
/// timers count route takes a prefix and refuses an empty one — `mode=count
/// requires a non-empty prefix`, in the handler and again in the stored
/// procedure — while the key itself is the send's own dedup key, a fresh uuid on
/// a standard queue, which no prefix of its own could cover. It also scopes the
/// count to messages THIS FACADE scheduled, leaving a native producer's timers on
/// the same queue out of a number that is answering an SQS question.
pub const TIMER_KEY_PREFIX: &str = "sqs.";

/// The timer key one delayed send is filed under.
pub fn timer_key(dedup_key: &str) -> String {
    format!("{TIMER_KEY_PREFIX}{dedup_key}")
}

/// The `Attributes` a receive can answer, and the only names
/// `AttributeNames`/`MessageSystemAttributeNames` select from here.
const ATTR_RECEIVE_COUNT: &str = "ApproximateReceiveCount";
const ATTR_SENT_TIMESTAMP: &str = "SentTimestamp";
const ATTR_GROUP_ID: &str = "MessageGroupId";
const ATTR_DEDUP_ID: &str = "MessageDeduplicationId";
/// The FIFO system attribute AND the top-level `SendMessage` field, which are
/// spelled the same and are the same number ([`system_view`]).
const ATTR_SEQUENCE_NUMBER: &str = "SequenceNumber";

/// The whole-batch failures are rare and the log line that reports one is not:
/// a fleet retrying a bad batch would write one line per attempt per worker.
static STALE_DELETE: Sampler = Sampler::new(10_000);
static AMORTIZED_UNAVAILABLE: Sampler = Sampler::new(60_000);
/// One pop of a receive failing while its siblings answered — a partial receive
/// rather than a refusal, and the only place that fact is visible.
static POP_FAILED: Sampler = Sampler::new(10_000);

// ------------------------------------------------------------------- sending

/// `SendMessage`. Answers `MessageId`, the three MD5 fields, and — on a FIFO
/// queue — `SequenceNumber`, which is the absolute offset the push allocated.
pub async fn send_message(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let record = record_of(ctx, params).await?;
    let prepared = Prepared::of(&record, params)?;
    guard_total_size(&record, &[&prepared], Batched::No)?;

    let mut answer = Map::new();
    let sent = match prepared.delay_seconds {
        0 => {
            let items = [prepared.push_item(&record.name)];
            let pushed = push_all(ctx, &items).await?;
            Sent::of_push(&pushed[0])?
        }
        delay => {
            let schedules = [prepared.timer(&record.name, delay)];
            let scheduled = ctx
                .facade
                .queen
                .timers_schedule(&schedules, ctx.token())
                .await
                .map_err(|e| super::queen_error(&e))?;
            Sent::of_timer(scheduled.first(), &prepared.transaction_id)
        }
    };
    answer.insert("MessageId".to_string(), Value::String(sent.message_id));
    prepared.write_md5s(&mut answer);
    // A `SequenceNumber` is the absolute offset the push allocated
    // (PLAN_QUEEN_SQS.md, Semantics), so it exists only where SQS itself has
    // one: a FIFO queue. Answering one on a standard queue would be answering a
    // field AWS does not send.
    //
    // DIVERGENCE, `accepted`: the offset is absolute WITHIN ITS PARTITION, and
    // on a FIFO queue the partition IS the `MessageGroupId` — so it starts at 0
    // in every group and the same number appears in each. AWS's is unique across
    // the queue. It orders a group's own messages exactly, which is what a FIFO
    // consumer reads it for; an application that keys across groups by it
    // collides, and no queue-wide counter exists to answer with instead.
    if record.fifo {
        if let Some(offset) = sent.offset {
            answer.insert(
                ATTR_SEQUENCE_NUMBER.to_string(),
                Value::String(offset.to_string()),
            );
        }
    }
    Ok(Value::Object(answer))
}

/// `SendMessageBatch`: up to ten entries in ONE push, answered as `Successful`
/// and `Failed` lists keyed by the client's own entry ids.
///
/// A per-entry failure is a `BatchResultErrorEntry` and NOT a failed request:
/// the whole point of the shape is that nine good messages are not lost to one
/// bad one. The three failures that ARE the whole request's — an empty batch, an
/// over-long one, repeated ids — are refusals of the ENVELOPE, which is why they
/// happen before any entry is looked at.
pub async fn send_message_batch(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let record = record_of(ctx, params).await?;
    let entries = batch_entries(params)?;

    // Validate every entry first, so that the size guard below counts what will
    // actually be sent and an entry that cannot be prepared costs no round trip.
    let mut outcomes: Vec<Outcome<Prepared>> = Vec::with_capacity(entries.len());
    for entry in &entries {
        outcomes.push(Outcome::of(
            entry.id.clone(),
            Prepared::of(&record, entry.params),
        ));
    }
    let accepted: Vec<&Prepared> = outcomes.iter().filter_map(Outcome::value).collect();
    guard_total_size(&record, &accepted, Batched::Yes)?;

    // Two calls at most, and neither is per entry: everything immediate goes in
    // one push, everything delayed in one schedule.
    let items: Vec<PushItem> = accepted
        .iter()
        .filter(|p| p.delay_seconds == 0)
        .map(|p| p.push_item(&record.name))
        .collect();
    let schedules: Vec<TimerSchedule> = accepted
        .iter()
        .filter(|p| p.delay_seconds > 0)
        .map(|p| p.timer(&record.name, p.delay_seconds))
        .collect();

    let pushed = match items.is_empty() {
        true => Vec::new(),
        false => push_all(ctx, &items).await?,
    };
    let scheduled = match schedules.is_empty() {
        true => Vec::new(),
        false => ctx
            .facade
            .queen
            .timers_schedule(&schedules, ctx.token())
            .await
            .map_err(|e| super::queen_error(&e))?,
    };

    let mut pushes = pushed.iter();
    let mut timers = scheduled.iter();
    let mut results = BatchResults::default();
    for outcome in &outcomes {
        let prepared = match outcome.result.as_ref() {
            Err(error) => {
                results.failed(&outcome.id, error);
                continue;
            }
            Ok(prepared) => prepared,
        };
        let sent = match prepared.delay_seconds {
            0 => match pushes.next() {
                // A per-item verdict is this ENTRY's failure and not the
                // request's: nine messages that landed are not lost to one the
                // broker did not write, which is the whole point of the shape.
                Some(pushed) => match Sent::of_push(pushed) {
                    Ok(sent) => sent,
                    Err(error) => {
                        results.failed(&outcome.id, &error);
                        continue;
                    }
                },
                None => return Err(short_answer("push")),
            },
            _ => Sent::of_timer(timers.next(), &prepared.transaction_id),
        };
        let mut entry = Map::new();
        entry.insert("Id".to_string(), Value::String(outcome.id.clone()));
        entry.insert("MessageId".to_string(), Value::String(sent.message_id));
        prepared.write_md5s(&mut entry);
        if record.fifo {
            if let Some(offset) = sent.offset {
                entry.insert(
                    ATTR_SEQUENCE_NUMBER.to_string(),
                    Value::String(offset.to_string()),
                );
            }
        }
        results.successful(Value::Object(entry));
    }
    Ok(results.into_value())
}

/// One send, validated, with everything the wire needs already decided.
struct Prepared {
    envelope: Envelope,
    /// The dedup key the push is filed under AND the lane selector — see the
    /// module header on why those are one value.
    transaction_id: String,
    partition: String,
    delay_seconds: i64,
    /// Whether [`Prepared::transaction_id`] is a DEDUP KEY the broker must file
    /// this message under, or only this facade's own lane selector. It is a
    /// dedup key exactly on a FIFO queue, where the client named it; on a
    /// standard queue it is a fresh uuid per request and sending it would file
    /// every message under a key nothing will ever ask for again.
    deduped: bool,
    /// Bytes counted against `MaximumMessageSize`.
    size: usize,
}

impl Prepared {
    fn of(record: &QueueRecord, params: &Value) -> SqsResult<Prepared> {
        let body = text(params, "MessageBody")
            .filter(|body| !body.is_empty())
            .ok_or_else(|| missing_parameter("MessageBody"))?
            .to_string();
        envelope::validate_body(&body)?;

        let envelope = Envelope {
            body,
            attributes: message_attributes(params.get("MessageAttributes"))?,
            system: system_attributes(params.get("MessageSystemAttributes"))?,
            moved_from: None,
        };

        let delay = bounded(params, "DelaySeconds", 0, i64::from(MAX_DELAY_SECONDS))?;
        let group = text(params, "MessageGroupId").filter(|g| !g.is_empty());
        let dedup = text(params, "MessageDeduplicationId").filter(|d| !d.is_empty());
        // Before either is used for anything: the group becomes a Queen
        // PARTITION NAME and the dedup id a `transactionId`, so AWS's own limits
        // are what stops client text from choosing a broker identifier the
        // broker will refuse in a language the sender cannot read.
        if let Some(group) = group {
            check_fifo_id("MessageGroupId", group)?;
        }
        if let Some(dedup) = dedup {
            check_fifo_id("MessageDeduplicationId", dedup)?;
        }

        let (transaction_id, partition) = match record.fifo {
            true => {
                // A FIFO lane IS a MessageGroupId, so there is nothing to hash
                // and nothing to synthesize: the client named the lane.
                let group = group.ok_or_else(|| missing_parameter("MessageGroupId"))?;
                if delay.is_some() {
                    return Err(wrong_queue_type("DelaySeconds"));
                }
                let key =
                    match dedup {
                        Some(id) => id.to_string(),
                        None if content_based(record) => content_key(&envelope.body),
                        None => return Err(SqsError::with(
                            ErrorKind::InvalidParameterValue,
                            "The queue should either have ContentBasedDeduplication enabled or \
                             MessageDeduplicationId provided explicitly",
                        )),
                    };
                (key, group.to_string())
            }
            false => {
                if dedup.is_some() {
                    return Err(wrong_queue_type("MessageDeduplicationId"));
                }
                // `MessageGroupId` on a standard queue is AWS's 2025 fair-queue
                // feature, which PLAN_QUEEN_SQS.md puts out of plan: it is
                // accepted so a client that works against AWS today works here,
                // and it does NOT choose the lane, because honouring it halfway
                // would promise an ordering this queue does not give.
                let key = uuid::Uuid::new_v4().to_string();
                let partition = lane_for(&key, record.partitions);
                (key, partition)
            }
        };

        Ok(Prepared {
            size: size_of(&envelope),
            envelope,
            transaction_id,
            partition,
            delay_seconds: delay.unwrap_or(0),
            deduped: record.fifo,
        })
    }

    /// The push, with a `transactionId` exactly where SQS gave us one to send.
    ///
    /// A STANDARD queue sends none, which is the at-least-once guarantee SQS
    /// standard queues themselves give: the key would be a per-request uuid, so
    /// it deduplicates nothing, and the broker mints its own from the message id
    /// when the item carries none.
    fn push_item(&self, queue: &str) -> PushItem {
        match self.deduped {
            true => PushItem::deduped(
                queue,
                &self.partition,
                self.envelope.encode(),
                &self.transaction_id,
            ),
            false => PushItem::new(queue, &self.partition, self.envelope.encode()),
        }
    }

    /// The schedule a delayed send becomes. The timer key is the send's own
    /// dedup key rather than its MessageId, which does not exist yet, under
    /// [`TIMER_KEY_PREFIX`] so the queue's delayed messages can be COUNTED.
    ///
    /// What that key does NOT buy is idempotent retry. A schedule under an
    /// existing key overwrites it, but only a FIFO send has a key stable across
    /// retries — and FIFO refuses `DelaySeconds` outright — so on the one queue
    /// type that can take this path the key is a fresh uuid per request and a
    /// retried delayed send is two delayed messages. That is what AWS answers
    /// too, and it is why the sentence is here rather than a promise.
    ///
    /// DIVERGENCE, `accepted`: on a queue that ALSO carries a queue-level
    /// `DelaySeconds`, the two delays ADD instead of the message's replacing the
    /// queue's. The queue's is the broker's `delayed_processing`, which hides a
    /// segment until it is that many seconds old (004_log_pop.sql: *"only
    /// segments at least v_delayed seconds old are visible"*) and cannot tell a
    /// segment a timer wrote from any other — so a 60-second message on a
    /// 30-second queue is visible after 90. Closing it would need a per-push
    /// "this one is already late" flag on the broker, which is a core change
    /// PLAN_QUEEN_SQS.md does not take; the bound is the queue's own default,
    /// which is 0 on every queue that never set one.
    fn timer(&self, queue: &str, delay_seconds: i64) -> TimerSchedule {
        let payload = serde_json::to_vec(&self.envelope.encode()).unwrap_or_default();
        TimerSchedule::new(
            queue,
            &timer_key(&self.transaction_id),
            &self.partition,
            delay_seconds * 1_000,
            &self.transaction_id,
            &base64::engine::general_purpose::STANDARD.encode(payload),
        )
    }

    /// The three digests, omitted rather than nulled when there is nothing to
    /// digest: an SDK that finds the key present and empty compares it.
    fn write_md5s(&self, into: &mut Map<String, Value>) {
        into.insert(
            "MD5OfMessageBody".to_string(),
            Value::String(md5::body_md5(&self.envelope.body)),
        );
        if let Some(digest) = md5::attributes_md5(&self.envelope.attributes) {
            into.insert("MD5OfMessageAttributes".to_string(), Value::String(digest));
        }
        if let Some(digest) = md5::system_attributes_md5(&self.envelope.system) {
            into.insert(
                "MD5OfMessageSystemAttributes".to_string(),
                Value::String(digest),
            );
        }
    }
}

/// What a send produced, whichever route it took.
struct Sent {
    message_id: String,
    /// The `SequenceNumber`. A scheduled message has none until it fires.
    offset: Option<i64>,
}

impl Sent {
    /// One push item's verdict, READ rather than assumed.
    ///
    /// The broker answers `201 Created` for the whole batch and reports what
    /// happened to each item in the item's own `status`, so the HTTP code is not
    /// the answer to "did this message land". Three of the five statuses mean it
    /// did:
    ///
    ///   * `queued` — written;
    ///   * `duplicate` — the dedup window doing its job, answered with the
    ///     ORIGINAL message's id and offset, which is exactly what SQS answers
    ///     for a repeated `MessageDeduplicationId`;
    ///   * `buffered` — the database transaction failed and the broker spooled
    ///     the item to its file buffer, which the drain replays under the
    ///     preserved `transactionId`. Accepted, deliberately: the broker chose
    ///     that label over `error` precisely so that a client which only reads
    ///     the HTTP code does not lose the message.
    ///
    /// `error` and `failed` mean the message is GONE — nothing wrote it and
    /// nothing spooled it — and answering a MessageId and three valid digests
    /// for one is the plainest way a queue front can lose a message. It is a
    /// `Receiver` fault and a 5xx, so an SDK's own retry policy sends it again.
    fn of_push(pushed: &queen::Pushed) -> SqsResult<Sent> {
        match pushed.status.as_str() {
            "queued" | "duplicate" | "buffered" | "" if !pushed.message_id.is_empty() => Ok(Sent {
                message_id: pushed.message_id.clone(),
                offset: pushed.offset,
            }),
            // A status this facade accepts but with no id to answer under: the
            // client would key its own records by an empty string.
            "queued" | "duplicate" | "buffered" | "" => Err(SqsError::with(
                ErrorKind::InternalFailure,
                "The queue service accepted the message and named no MessageId for it.",
            )),
            status => Err(SqsError::with(
                ErrorKind::ServiceUnavailable,
                format!(
                    "The queue service did not store the message (status `{status}`). It was not \
                     sent; sending it again is safe."
                ),
            )),
        }
    }

    /// The broker allocates a scheduled message's id when the timer is written,
    /// not when it fires, so the id this answers is the one the message will
    /// actually carry. The dedup key is the fallback only for a broker that
    /// answered no id at all.
    fn of_timer(result: Option<&queen::TimerResult>, transaction_id: &str) -> Sent {
        Sent {
            message_id: result
                .and_then(|r| r.message_id.clone())
                .unwrap_or_else(|| transaction_id.to_string()),
            offset: None,
        }
    }
}

// ----------------------------------------------------------------- receiving

/// `ReceiveMessage`. N parallel `batch=1` pops in `exact` mode; the redrive
/// check happens HERE, before a message is returned.
///
/// The long poll is ONE parked pop and never N. A receive that parked ten pops
/// on an idle queue would hold ten claims open for twenty seconds to deliver
/// nothing, and would answer the first message twenty seconds late because the
/// other nine are still waiting. So: the window is spent by a single pop, and
/// the rest are asked for only once that one has found something.
pub async fn receive_message(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let record = record_of(ctx, params).await?;
    // DIVERGENCE, `accepted`: a `MaxNumberOfMessages` outside 1..10 is
    // `InvalidParameterValue` here and `AWS.SimpleQueueService.ReadCountOutOfRange`
    // at AWS. Both are 400 Sender faults naming the parameter and the range, and
    // no SDK branches on the difference — the value is a constant in the client's
    // own code, so the answer is read by a developer and not by a retry policy.
    // The catalog is the contract and a new code is a reviewed event
    // ([`crate::error`]): this one is recorded rather than invented, because its
    // JSON 1.0 `__type` cannot be derived from the Query spelling and guessing it
    // would put a wrong `__type` on the wire for the sake of a matching string.
    let wanted = bounded(
        params,
        "MaxNumberOfMessages",
        1,
        i64::from(MAX_RECEIVE_MESSAGES),
    )?
    .unwrap_or(1) as usize;
    // A parameter that is PRESENT decides, at any legal value, and only its
    // absence reaches the queue's attribute: `WaitTimeSeconds=0` against a
    // long-polling queue is a client asking for a short poll, and falling back
    // to the queue's twenty seconds there would park a receive the client wrote
    // to return at once.
    let wait = bounded(params, "WaitTimeSeconds", 0, i64::from(MAX_WAIT_SECONDS))?
        .or_else(|| {
            queue_default(
                &record,
                "ReceiveMessageWaitTimeSeconds",
                0,
                i64::from(MAX_WAIT_SECONDS),
            )
        })
        .unwrap_or(0);
    let visibility = bounded(
        params,
        "VisibilityTimeout",
        0,
        i64::from(MAX_VISIBILITY_SECONDS),
    )?
    .or_else(|| {
        queue_default(
            &record,
            "VisibilityTimeout",
            0,
            i64::from(MAX_VISIBILITY_SECONDS),
        )
    })
    .unwrap_or(DEFAULT_VISIBILITY_SECONDS);

    // THE BACKSTOP, and no longer the gate. `amortized` needs C-SQS-1 on the
    // broker, and `Config::from_source` refuses the variable at boot, so a
    // PROCESS can never be in this state. A `Config` built in-process can — the
    // library seam and this crate's own test rig both build one by hand — and a
    // facade that answered such a config with exact-mode pops in silence would
    // make a performance decision invisible to whoever made it. One line, rate
    // limited, and never a refusal of the client's receive: the mode's fallback
    // is CORRECT, only slower than what was asked for.
    if ctx.facade.config.receive_mode == ReceiveMode::Amortized {
        if let Some(suppressed) = AMORTIZED_UNAVAILABLE.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                "QUEEN_SQS_RECEIVE_MODE=amortized needs a broker with maxPerPartition (C-SQS-1); \
                 serving this receive in exact mode"
            );
        }
    }

    let filters = Filters::of(params);
    // The attempt id is answered BEFORE anything is claimed: the whole point of
    // it is that a retry does not take a second claim on a group whose first
    // claim is still in flight (M2, [`fifo`]).
    let attempt = fifo::attempt_id(&record, params)?;
    if let Some(id) = attempt {
        if let Some(remembered) = fifo::replay(ctx, &record.name, id).await? {
            return Ok(answer_of(remembered));
        }
    }

    // The pop, the REDRIVE CHECK, and — only when the check took something away
    // — up to [`dlq::MAX_FILL_ROUNDS`] more pops to make the answer up.
    //
    // A message whose receive count exceeds the queue's `maxReceiveCount` is
    // never returned: it is moved to the dead-letter queue, atomically, and the
    // client is not told about it ([`dlq`]). That leaves the answer short of
    // what was asked for, and the refill is what keeps a queue with a few
    // exhausted messages in it from answering one message per poll. It is
    // BOUNDED because the alternative — filling until the client's count is met
    // — turns one `ReceiveMessage` into a drain of a queue whose whole backlog
    // is over the threshold, with one HTTP request held open for all of it.
    //
    // The long poll is spent by the FIRST round only. A refill that parked
    // would hold the request for another twenty seconds to answer messages the
    // client did not wait for.
    let mut popped: Vec<queen::Message> = Vec::new();
    for round in 0..=dlq::MAX_FILL_ROUNDS {
        let want = wanted - popped.len();
        let parking = match round {
            0 => wait,
            _ => 0,
        };
        let claimed = match record.fifo {
            // ONE pop of ONE lane: a group's order only means anything if the
            // whole run is claimed by one consumer ([`fifo`], module header).
            true => fifo::receive(ctx, &record, want, visibility, parking).await?,
            false => pop_exact(ctx, &record.name, want, visibility, parking).await?,
        };
        if claimed.is_empty() {
            break;
        }
        let sifted = dlq::sift(ctx, &record, claimed).await?;
        popped.extend(sifted.kept);
        if record.fifo && !sifted.moved.is_empty() {
            // The roster was written by the pop above and the move happened
            // after it, so it still lists messages this client will never be
            // given and can never delete. Trimming it is what keeps the prefix
            // of the rest of the claim ackable ([`fifo::forget`]).
            fifo::forget(ctx, &sifted.moved).await;
        }
        // Nothing was moved, so a short answer is a short QUEUE and another pop
        // would be a round trip for a lane that has just proved it is empty.
        if sifted.moved.is_empty() || popped.len() >= wanted {
            break;
        }
    }

    let mut messages = Vec::with_capacity(popped.len());
    for message in &popped {
        messages.push(deliver(ctx, &record, &filters, message));
    }

    if let Some(id) = attempt {
        // Losing this race means another request already answered this attempt
        // id. The client gets that answer — one attempt, one set of messages —
        // and the claims THIS receive took are handed straight back rather than
        // left to block their groups for a visibility timeout nobody is
        // consuming. Every claim, not just the first: a refill round takes one
        // of its own.
        if let Some(remembered) =
            fifo::remember(ctx, &record.name, id, &messages, visibility).await?
        {
            let mut released: Vec<(&str, &str)> = Vec::new();
            for message in &popped {
                let claim = (message.partition_id.as_str(), message.lease_id.as_str());
                if released.contains(&claim) {
                    continue;
                }
                released.push(claim);
                // The FIRST message of the claim, which is the only position a
                // release may name: a `retry` ack commits everything strictly
                // below it.
                fifo::release(ctx, message).await;
            }
            return Ok(answer_of(remembered));
        }
    }
    Ok(answer_of(messages))
}

/// A receive's answer. An empty one omits `Messages` entirely, which is what
/// AWS answers and what every client's `if 'Messages' in response` is written
/// against.
fn answer_of(messages: Vec<Value>) -> Value {
    let mut answer = Map::new();
    if !messages.is_empty() {
        answer.insert("Messages".to_string(), Value::Array(messages));
    }
    Value::Object(answer)
}

/// The `exact` mode's claim: up to `wanted` pops of one message from one lane.
///
/// Claim width 1 is the whole point (module header). What varies is only WHEN
/// the parking happens — see [`receive_message`].
async fn pop_exact(
    ctx: &Ctx,
    queue: &str,
    wanted: usize,
    visibility: i64,
    wait: i64,
) -> SqsResult<Vec<queen::Message>> {
    let immediate = PopOptions {
        // DIVERGENCE, `deliberate`: a `VisibilityTimeout` of 0 on a receive
        // applies the queue's default rather than making the message
        // immediately visible to other consumers.
        //
        // Zero is the broker's own "use the queue's lease time", so the two
        // meanings collide on one number and a receive that sent 0 is
        // indistinguishable here from one that sent nothing. Honouring AWS's
        // meaning would mean popping and then releasing — a second write
        // transaction per message — and the release would kill the lease the
        // receipt handle names, so the `DeleteMessage` a client is entitled to
        // make against that handle would silently stop deleting. A message held
        // for the queue's visibility instead of none is inside SQS's own
        // at-least-once envelope; a delete that answers success and deletes
        // nothing is not.
        lease_seconds: visibility.clamp(0, i64::from(MAX_VISIBILITY_SECONDS)) as i32,
        ..PopOptions::default()
    };

    let mut messages = Vec::with_capacity(wanted);
    let mut remaining = wanted;
    if wait > 0 {
        let parked = PopOptions {
            wait: true,
            timeout_ms: (wait as u64 * 1_000).min(queen::MAX_POP_TIMEOUT_MS),
            ..immediate.clone()
        };
        let first = ctx
            .facade
            .queen
            .pop_queue(queue, &parked, ctx.token())
            .await
            .map_err(|e| super::queen_error(&e))?;
        messages.extend(first.messages);
        remaining -= 1;
        // The window was spent and the queue was empty for all of it. Asking for
        // the other nine would be nine round trips for a queue that has just
        // proved it has nothing.
        if messages.is_empty() {
            return Ok(messages);
        }
    }

    if remaining > 0 {
        let pops: Vec<BoxFuture<'_, queen::Result<queen::Popped>>> = (0..remaining)
            .map(|_| ctx.facade.queen.pop_queue(queue, &immediate, ctx.token()))
            .collect();
        // A FAILED SIBLING MUST NOT DISCARD WHAT ITS NEIGHBOURS CLAIMED. These
        // pops run concurrently and each one that answered has already taken a
        // durable claim on the broker; returning the first error would drop
        // those messages on the floor with no receipt handle ever issued —
        // invisible for a full visibility timeout, charged a delivery nobody
        // saw, and one step closer to a dead-letter queue they reach without a
        // single consumer having read them.
        //
        // So: the error is only the ANSWER when there is nothing to answer
        // with. A short receive is legal SQS — the count is a ceiling, never a
        // promise — and the client polls again.
        let mut failure = None;
        for answer in join_all(pops).await {
            match answer {
                Ok(popped) => messages.extend(popped.messages),
                Err(e) => failure = Some(e),
            }
        }
        if let Some(e) = failure {
            if messages.is_empty() {
                return Err(super::queen_error(&e));
            }
            if let Some(suppressed) = POP_FAILED.tick_now() {
                tracing::warn!(
                    target: "sqs",
                    suppressed,
                    queue,
                    delivered = messages.len(),
                    "a pop of this receive failed; answering the messages its siblings claimed"
                );
            }
        }
    }
    Ok(messages)
}

/// Which attributes a receive asked for.
///
/// Two lists select the SYSTEM attributes because AWS renamed the parameter
/// without retiring the old one, and a client may send either; they are read as
/// one set, since a client that sent both meant their union.
struct Filters {
    system: Selection,
    message: Selection,
}

impl Filters {
    fn of(params: &Value) -> Filters {
        let mut system = Selection::of(params.get("AttributeNames"));
        system.extend(Selection::of(params.get("MessageSystemAttributeNames")));
        Filters {
            system,
            message: Selection::of(params.get("MessageAttributeNames")),
        }
    }
}

/// A list of requested names. EMPTY IS EMPTY, not everything: SQS answers no
/// attributes to a receive that asked for none, and a facade that defaulted to
/// `All` would send every consumer fields it never reads and a digest over them.
#[derive(Default)]
struct Selection {
    all: bool,
    exact: Vec<String>,
    /// The `prefix.*` form, stored without its two trailing characters.
    prefixes: Vec<String>,
}

impl Selection {
    fn of(value: Option<&Value>) -> Selection {
        let mut selection = Selection::default();
        let names: Vec<&str> = match value {
            Some(Value::Array(items)) => items.iter().filter_map(Value::as_str).collect(),
            // A client that sent one name unindexed means a list of one.
            Some(Value::String(name)) => vec![name.as_str()],
            _ => Vec::new(),
        };
        for name in names {
            match name {
                "All" | ".*" | "*" => selection.all = true,
                _ => match name.strip_suffix(".*") {
                    Some(prefix) => selection.prefixes.push(prefix.to_string()),
                    None => selection.exact.push(name.to_string()),
                },
            }
        }
        selection
    }

    fn extend(&mut self, other: Selection) {
        self.all |= other.all;
        self.exact.extend(other.exact);
        self.prefixes.extend(other.prefixes);
    }

    fn wants(&self, name: &str) -> bool {
        self.all
            || self.exact.iter().any(|want| want == name)
            // The dot belongs to the prefix: `trace.*` selects `trace.id` and
            // must not select `traceroute`.
            || self.prefixes.iter().any(|prefix| {
                name.starts_with(prefix) && name.as_bytes().get(prefix.len()) == Some(&b'.')
            })
    }

    fn is_empty(&self) -> bool {
        !self.all && self.exact.is_empty() && self.prefixes.is_empty()
    }
}

/// One popped message, as a client reads it.
///
/// The MD5 of the attributes is computed over the RETURNED subset and not over
/// what was sent: an SDK recomputes it from what it received, and a digest over
/// the filtered-out attributes fails on the SDK's own side.
///
/// For the same reason the message is made XML-safe HERE rather than in the
/// writer ([`crate::proto::xml::sanitize`]): a body carrying a character XML 1.0
/// cannot represent is substituted on its way into the Query protocol's
/// document, and a digest taken over the unsubstituted text would disagree with
/// the `<Body>` beside it — which every AWS SDK checks and refuses. Only a
/// NATIVE producer's payload can be in that state; the SQS send path refuses the
/// character set outright.
fn deliver(ctx: &Ctx, record: &QueueRecord, filters: &Filters, message: &queen::Message) -> Value {
    let envelope = xml_safe(Envelope::decode(&message.data));
    let receipt = Receipt {
        queue: record.name.clone(),
        partition_id: message.partition_id.clone(),
        transaction_id: message.transaction_id.clone(),
        lease_id: message.lease_id.clone(),
        message_id: message.id.clone(),
        // The handle must survive every legal ChangeMessageVisibility, so its
        // life is SQS's in-flight ceiling and not this delivery's window (module
        // header). What decides whether a delete lands is the lease.
        expires_at_ms: now_ms() + i64::from(MAX_VISIBILITY_SECONDS) * 1_000,
    };

    let mut out = Map::new();
    out.insert("MessageId".to_string(), Value::String(message.id.clone()));
    out.insert(
        "ReceiptHandle".to_string(),
        Value::String(ctx.facade.handles.encode(&receipt)),
    );
    // `MD5OfBody` here and `MD5OfMessageBody` on a send. The two names are AWS's
    // own and one is not a typo of the other.
    out.insert(
        "MD5OfBody".to_string(),
        Value::String(md5::body_md5(&envelope.body)),
    );
    out.insert("Body".to_string(), Value::String(envelope.body.clone()));

    if !filters.system.is_empty() {
        let attributes: BTreeMap<String, String> = system_view(record, message, &envelope)
            .into_iter()
            .filter(|(name, _)| filters.system.wants(name))
            .collect();
        if !attributes.is_empty() {
            out.insert("Attributes".to_string(), map_of(&attributes));
        }
    }
    if !filters.message.is_empty() {
        let selected: BTreeMap<String, MessageAttribute> = envelope
            .attributes
            .into_iter()
            .filter(|(name, _)| filters.message.wants(name))
            .collect();
        if let Some(digest) = md5::attributes_md5(&selected) {
            out.insert("MD5OfMessageAttributes".to_string(), Value::String(digest));
            out.insert("MessageAttributes".to_string(), attributes_view(&selected));
        }
    }
    Value::Object(out)
}

/// An envelope reduced to what an XML document can carry, so that every digest
/// taken below describes the bytes that will actually ship (see [`deliver`]).
///
/// Attribute NAMES are sanitized too, and two names that differ only in a
/// character XML cannot represent therefore collapse into one — which is a lossy
/// answer to a payload that was already unanswerable, and still better than a
/// digest no client can reproduce. Binary values need nothing: they ship as
/// base64.
fn xml_safe(envelope: Envelope) -> Envelope {
    use crate::proto::xml;
    let Envelope {
        body,
        attributes,
        system,
        moved_from,
    } = envelope;
    Envelope {
        body: xml::sanitize(&body).into_owned(),
        attributes: attributes
            .into_iter()
            .map(|(name, attribute)| {
                let value = match attribute.value {
                    AttributeValue::String(text) => {
                        AttributeValue::String(xml::sanitize(&text).into_owned())
                    }
                    binary => binary,
                };
                (
                    xml::sanitize(&name).into_owned(),
                    MessageAttribute {
                        data_type: xml::sanitize(&attribute.data_type).into_owned(),
                        value,
                    },
                )
            })
            .collect(),
        system: system
            .into_iter()
            .map(|(name, value)| {
                (
                    xml::sanitize(&name).into_owned(),
                    xml::sanitize(&value).into_owned(),
                )
            })
            .collect(),
        moved_from,
    }
}

/// The system attributes this facade can answer, before filtering.
///
/// DIVERGENCE, `accepted`: two attributes AWS returns on every message are
/// absent here, including under `AttributeNames=All`. Every SDK models
/// `Attributes` as an open string map, so an absence is read as an absence and
/// not as a failure — but a differential run against real AWS sees both, and
/// each is absent because the only alternative is to invent it:
///
///   * `SenderId` — the sender's principal is not stored. This facade knows who
///     is RECEIVING; the sender's identity would have to be written into the
///     payload, and PLAN_QUEEN_SQS.md fixes the envelope at four keys.
///   * `ApproximateFirstReceiveTimestamp` — nothing records the first delivery
///     of a message. `deliveryAttempt` counts them; no clock remembers them.
///
/// `SequenceNumber` used to be a third: the pop wire carried no offset, and only
/// the push side could answer one. C-SQS-3 put an `"offset"` on every popped
/// message (`render_pop_parts`, server/src/handlers/data.rs), so a FIFO receive
/// answers the SAME number the send answered — read back, never derived. It is
/// still absent against a pre-C-SQS-3 broker, where [`queen::Message::offset`]
/// parses as `None`; nothing is synthesized to cover that, because a number
/// derived from the transaction id or from the delivery position would order a
/// group's messages differently from the way the log does, and a wrong
/// SequenceNumber is worse than an absent one.
fn system_view(
    record: &QueueRecord,
    message: &queen::Message,
    envelope: &Envelope,
) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    // NOT `deliveryAttempt` alone: a message that was dead-lettered carries the
    // count it had on the queue it came from, and AWS does not reset it on a
    // move ([`dlq`]). The two are added in one place so that the number a
    // client is told and the number the redrive threshold is compared against
    // are the same expression.
    out.insert(
        ATTR_RECEIVE_COUNT.to_string(),
        dlq::received_count(envelope, message).to_string(),
    );
    if let Some(ms) = epoch_ms_of(&message.created_at) {
        out.insert(ATTR_SENT_TIMESTAMP.to_string(), ms.to_string());
    }
    if record.fifo {
        // A FIFO lane IS the group, and the dedup key IS the transaction id: both
        // are what this facade wrote on the way in, read back.
        out.insert(ATTR_GROUP_ID.to_string(), message.partition.clone());
        out.insert(ATTR_DEDUP_ID.to_string(), message.transaction_id.clone());
        // …and so is the sequence: the offset C-SQS-3 puts on the pop is the
        // offset C1 put on the push, so the number a client was answered by
        // `SendMessage` is the number it reads back off the delivery. Only on a
        // FIFO queue, for the same reason the send answers one only there — and
        // it carries the SEND's divergence with it, unchanged: the offset is
        // absolute within its partition, the partition IS the group, so it
        // starts at 0 in every group where AWS's is unique across the queue
        // (see [`send_message`]). Absent against a pre-C-SQS-3 broker.
        if let Some(offset) = message.offset {
            out.insert(ATTR_SEQUENCE_NUMBER.to_string(), offset.to_string());
        }
    }
    // The id this copy was made FROM, on a message a redrive moved. It is a
    // field of the envelope rather than one of its system attributes, so it is
    // named here or nowhere ([`dlq::SYS_ORIGINAL_MESSAGE_ID`]).
    if let Some(original) = &envelope.moved_from {
        out.insert(dlq::SYS_ORIGINAL_MESSAGE_ID.to_string(), original.clone());
    }
    for (name, value) in &envelope.system {
        // The carried count is CONSUMED above rather than echoed: two numbers
        // describing one message's delivery count, one of them frozen at the
        // moment of a move, would be a contradiction a client has to resolve.
        // `queen.sourceQueue` IS echoed — it is the one thing a dead-letter
        // consumer cannot work out for itself, and it is `queen.`-prefixed like
        // every other extension this facade adds to SQS's vocabulary.
        if name == dlq::SYS_RECEIVE_COUNT {
            continue;
        }
        out.insert(name.clone(), value.clone());
    }
    out
}

// ------------------------------------------------------------------ deleting

/// `DeleteMessage` — ack `completed` under the handle's own lease.
///
/// The registry is NOT read here, and that is a decision rather than an
/// omission: this is the hottest call on the listener, the handle already names
/// the queue and carries a tag proving this facade minted it for that queue, and
/// a queue that has been deleted since the receive answers through the ack
/// instead. One round trip saved per delivered message.
pub async fn delete_message(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let queue = queue_of(ctx, params)?;
    let receipt = receipt_of(ctx, &queue, text(params, "ReceiptHandle"))?;
    // A FIFO claim holds a RUN of one group, so the ack is never this message:
    // it is the last member of the deleted prefix, and this one may only be
    // recorded ([`fifo`], module header). The branch costs no round trip — a
    // FIFO queue is one whose NAME ends in `.fifo`, which the handle carries.
    if crate::registry::is_fifo(&queue) {
        return one_result(fifo::delete(ctx, &[&receipt]).await).map(|()| Value::Null);
    }
    let ack = AckItem::completed(
        &receipt.transaction_id,
        &receipt.partition_id,
        &receipt.lease_id,
    );
    let answered = ctx
        .facade
        .queen
        .ack(&ack, None, ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    report_delete(&answered);
    Ok(Value::Null)
}

/// `DeleteMessageBatch` — one `POST /api/v1/ack/batch`, whose per-entry answers
/// ARE the `BatchResultErrorEntry` list.
pub async fn delete_message_batch(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let queue = queue_of(ctx, params)?;
    let entries = batch_entries(params)?;

    let outcomes: Vec<Outcome<Receipt>> = entries
        .iter()
        .map(|entry| {
            Outcome::of(
                entry.id.clone(),
                receipt_of(ctx, &queue, text(entry.params, "ReceiptHandle")),
            )
        })
        .collect();
    let receipts: Vec<&Receipt> = outcomes.iter().filter_map(Outcome::value).collect();

    // On a FIFO queue the entries of one claim are applied to ONE record in one
    // read-modify-write, so a batch that deletes the whole of a batch it
    // received is one compare-and-set and one ack rather than ten of each.
    let answered: Vec<SqsResult<()>> = match crate::registry::is_fifo(&queue) {
        true => fifo::delete(ctx, &receipts).await,
        false => {
            let acks: Vec<AckItem> = receipts
                .iter()
                .map(|r| AckItem::completed(&r.transaction_id, &r.partition_id, &r.lease_id))
                .collect();
            let acked = match acks.is_empty() {
                true => Vec::new(),
                false => ctx
                    .facade
                    .queen
                    .ack_batch(&acks, None, ctx.token())
                    .await
                    .map_err(|e| super::queen_error(&e))?,
            };
            if acked.len() != acks.len() {
                return Err(short_answer("ack"));
            }
            // Every verdict is a success to the CLIENT (module header); the
            // refusal is only ever a log line.
            acked
                .iter()
                .map(|answer| {
                    report_delete(answer);
                    Ok(())
                })
                .collect()
        }
    };

    let mut answers = answered.into_iter();
    let mut results = BatchResults::default();
    for outcome in &outcomes {
        match outcome.result.as_ref() {
            Err(error) => results.failed(&outcome.id, error),
            Ok(_) => match answers.next() {
                None => return Err(short_answer("ack")),
                Some(Err(error)) => results.failed(&outcome.id, &error),
                Some(Ok(())) => results.successful(serde_json::json!({ "Id": outcome.id })),
            },
        }
    }
    Ok(results.into_value())
}

/// The one answer a single-receipt call to [`fifo::delete`] produces.
fn one_result(mut results: Vec<SqsResult<()>>) -> SqsResult<()> {
    match results.pop() {
        Some(result) => result,
        None => Err(short_answer("delete")),
    }
}

/// A delete the broker refused is still a delete AWS answers success to (module
/// header), so the only place the refusal can be seen is this log line — and it
/// is sampled, because a fleet that double-deletes does it on every message.
pub(super) fn report_delete(answered: &queen::Acked) {
    if answered.success {
        return;
    }
    if let Some(suppressed) = STALE_DELETE.tick_now() {
        tracing::debug!(
            target: "sqs",
            suppressed,
            reason = answered.error.as_deref().unwrap_or("unknown"),
            "delete answered success over a stale receipt handle"
        );
    }
}

// ---------------------------------------------------------------- visibility

/// `ChangeMessageVisibility`. Zero is a TERMINATE — ack `retry`, which
/// releases without charging the retry budget — and anything else is a lease
/// extension.
pub async fn change_message_visibility(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let queue = queue_of(ctx, params)?;
    let receipt = receipt_of(ctx, &queue, text(params, "ReceiptHandle"))?;
    let seconds = bounded(
        params,
        "VisibilityTimeout",
        0,
        i64::from(MAX_VISIBILITY_SECONDS),
    )?
    .ok_or_else(|| missing_parameter("VisibilityTimeout"))?;
    revisibilize(ctx, &receipt, seconds).await?;
    Ok(Value::Null)
}

/// `ChangeMessageVisibilityBatch`.
///
/// The entries are answered CONCURRENTLY because each one is its own call: a
/// terminate is an ack and an extension is a lease renewal, and ten of them
/// serialized would be ten round trips inside one request.
///
/// On a FIFO queue they are grouped by CLAIM first, exactly as
/// `DeleteMessageBatch` groups its entries, because there the entries are NOT
/// independent: a claim is one lease over a run of one group, so ten entries of
/// one batch are one release or one renewal. Ten independent calls would make
/// the first release end the claim and answer the other nine
/// `MessageNotInflight` — a batch of ten failures for the one gesture every SQS
/// consumer library makes on an error path, `ChangeMessageVisibility(0)` over
/// everything it just received.
pub async fn change_message_visibility_batch(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let queue = queue_of(ctx, params)?;
    let record = record_of(ctx, params).await?;
    let default_visibility = queue_default(
        &record,
        "VisibilityTimeout",
        0,
        i64::from(MAX_VISIBILITY_SECONDS),
    )
    .unwrap_or(DEFAULT_VISIBILITY_SECONDS);
    let entries = batch_entries(params)?;

    let outcomes: Vec<Outcome<(Receipt, i64)>> = entries
        .iter()
        .map(|entry| {
            let prepared =
                receipt_of(ctx, &queue, text(entry.params, "ReceiptHandle")).and_then(|receipt| {
                    // Optional per entry and required on the single action, which
                    // is AWS's own model rather than an inconsistency of ours.
                    let seconds = bounded(
                        entry.params,
                        "VisibilityTimeout",
                        0,
                        i64::from(MAX_VISIBILITY_SECONDS),
                    )?
                    .unwrap_or(default_visibility);
                    Ok((receipt, seconds))
                });
            Outcome::of(entry.id.clone(), prepared)
        })
        .collect();

    let changes: Vec<(&Receipt, i64)> = outcomes
        .iter()
        .filter_map(Outcome::value)
        .map(|(receipt, seconds)| (receipt, *seconds))
        .collect();
    let answered = match crate::registry::is_fifo(&queue) {
        true => revisibilize_claims(ctx, &changes).await,
        false => {
            let calls: Vec<BoxFuture<'_, SqsResult<()>>> = changes
                .iter()
                .map(|(receipt, seconds)| {
                    Box::pin(revisibilize(ctx, receipt, *seconds)) as BoxFuture<'_, SqsResult<()>>
                })
                .collect();
            join_all(calls).await
        }
    };

    let mut answers = answered.iter();
    let mut results = BatchResults::default();
    for outcome in &outcomes {
        match outcome.result.as_ref() {
            Err(error) => results.failed(&outcome.id, error),
            Ok(_) => match answers.next() {
                None => return Err(short_answer("visibility")),
                Some(Err(error)) => results.failed(&outcome.id, error),
                Some(Ok(())) => results.successful(serde_json::json!({ "Id": outcome.id })),
            },
        }
    }
    Ok(results.into_value())
}

/// One claim's answers, each under the index of the entry that asked for it.
type ClaimAnswers<'a> = BoxFuture<'a, Vec<(usize, SqsResult<()>)>>;

/// A FIFO visibility batch, one call per CLAIM, answered in the caller's order.
///
/// The claims are independent of each other and run concurrently; the entries
/// INSIDE one are not, so they are one call each way (see [`revisibilize_claim`]
/// for why the order within a claim is fixed).
async fn revisibilize_claims(ctx: &Ctx, changes: &[(&Receipt, i64)]) -> Vec<SqsResult<()>> {
    let mut claims: BTreeMap<(&str, &str), Vec<usize>> = BTreeMap::new();
    for (index, (receipt, _)) in changes.iter().enumerate() {
        claims
            .entry((&receipt.partition_id, &receipt.lease_id))
            .or_default()
            .push(index);
    }
    let calls: Vec<ClaimAnswers<'_>> = claims
        .into_values()
        .map(|members| Box::pin(revisibilize_claim(ctx, changes, members)) as ClaimAnswers<'_>)
        .collect();

    let mut out: Vec<SqsResult<()>> = changes.iter().map(|_| Ok(())).collect();
    for answers in join_all(calls).await {
        for (index, answer) in answers {
            out[index] = answer;
        }
    }
    out
}

/// The entries of ONE FIFO claim: at most one renewal and at most one release.
///
/// A claim is one lease over a run of one group, so every entry naming it is
/// asking about the same lease. The renewal is made at the LONGEST timeout any
/// of them asked for — which is what the broker would end at anyway, since it
/// takes the greatest of the two expiries ([`extend_lease`]) — and it goes
/// FIRST, because a release ends the claim and everything after it would be
/// answering about a lease that is gone.
async fn revisibilize_claim(
    ctx: &Ctx,
    changes: &[(&Receipt, i64)],
    members: Vec<usize>,
) -> Vec<(usize, SqsResult<()>)> {
    let mut out = Vec::with_capacity(members.len());
    let (releasing, extending): (Vec<usize>, Vec<usize>) =
        members.into_iter().partition(|i| changes[*i].1 == 0);

    if let Some(longest) = extending.iter().map(|i| changes[*i].1).max() {
        let answer = revisibilize(ctx, changes[extending[0]].0, longest).await;
        out.extend(extending.into_iter().map(|i| (i, answer.clone())));
    }
    if let Some(first) = releasing.first() {
        let answer = revisibilize(ctx, changes[*first].0, 0).await;
        out.extend(releasing.into_iter().map(|i| (i, answer.clone())));
    }
    out
}

/// One message's visibility, changed.
///
/// Zero and non-zero are two different broker calls and not one call with a
/// parameter: releasing a claim is an ack, and renewing one is a lease
/// extension. What they are NOT is two different contracts — AWS answers
/// `MessageNotInflight` for a message that is not in flight whichever timeout
/// was asked for, so both arms read the broker's verdict and neither invents a
/// success. A terminate whose lease has expired, been acked, or belongs to an
/// earlier delivery changed nothing, and telling the client its message is
/// visible again while another consumer holds it is the answer that cannot be
/// recovered from.
async fn revisibilize(ctx: &Ctx, receipt: &Receipt, seconds: i64) -> SqsResult<()> {
    // A FIFO claim covers a RUN of one group, and both halves of this function
    // change meaning there: a release must name the claim's HEAD or it commits
    // the messages below it, and an extension has a delete-set to keep alive
    // for as long as the lease it just lengthened ([`fifo`]).
    if crate::registry::is_fifo(&receipt.queue) {
        if seconds == 0 {
            return fifo::terminate(ctx, receipt).await;
        }
        let extended = extend_lease(ctx, receipt, seconds).await?;
        if extended.is_ok() {
            fifo::keep_alive(ctx, receipt, seconds).await;
        }
        return extended;
    }
    if seconds == 0 {
        let ack = AckItem::released(
            &receipt.transaction_id,
            &receipt.partition_id,
            &receipt.lease_id,
        );
        let released = ctx
            .facade
            .queen
            .ack(&ack, None, ctx.token())
            .await
            .map_err(|e| super::queen_error(&e))?;
        // `success: false` here is the broker's "invalid or expired lease", the
        // same fact `renewed == 0` carries on the other arm.
        return match released.success {
            true => Ok(()),
            false => Err(SqsError::new(ErrorKind::MessageNotInflight)),
        };
    }
    extend_lease(ctx, receipt, seconds).await?
}

/// The lease renewal both queue types share.
///
/// The route is always 200 and `renewed` is the only truth: a lease that
/// expired, was acked, or never existed renews nothing. The broker takes the
/// GREATEST of the two expiries, so a change to a SHORTER visibility does not
/// shorten the lease — a divergence from AWS, bounded by the original window.
///
/// DIVERGENCE on a FIFO queue, `accepted`: the lease covers the whole claimed
/// run of the group, so extending one member extends its batch-mates. They are
/// messages the SAME consumer is holding and nobody else can see, so the
/// visible effect is that a consumer which extends the message it is working on
/// keeps the rest of its own batch — which is what a FIFO consumer processing a
/// group in order wants, and never a message returned late to somebody else.
///
/// The nested result is deliberate: the OUTER one is the call failing, the
/// INNER one the lease's verdict, and a caller that has more to do after the
/// renewal (the delete-set's TTL) must not have to guess which it is holding.
async fn extend_lease(ctx: &Ctx, receipt: &Receipt, seconds: i64) -> SqsResult<SqsResult<()>> {
    let extended = ctx
        .facade
        .queen
        .lease_extend(&receipt.lease_id, seconds, ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    Ok(match extended.renewed {
        0 => Err(SqsError::new(ErrorKind::MessageNotInflight)),
        _ => Ok(()),
    })
}

// THE REDRIVE MOVE LIVES IN [`dlq`], not here. It is the one place in this
// facade where a message could be lost — a push and an ack that are two calls
// duplicate if the first lands and lose if the second does — so it is one
// transaction, and it is written beside the policy that decides when to make
// it rather than inside the receive loop that calls it.

// ------------------------------------------------------------------- batches

/// One entry of a batch action, with the id its result is reported under.
struct Entry<'a> {
    id: String,
    params: &'a Value,
}

/// What happened to one entry: its id, and either the value the action goes on
/// to use or the error its `BatchResultErrorEntry` reports.
struct Outcome<T> {
    id: String,
    result: SqsResult<T>,
}

impl<T> Outcome<T> {
    fn of(id: String, result: SqsResult<T>) -> Outcome<T> {
        Outcome { id, result }
    }

    fn value(&self) -> Option<&T> {
        self.result.as_ref().ok()
    }
}

/// The two lists every batch action answers.
#[derive(Default)]
struct BatchResults {
    successful: Vec<Value>,
    failed: Vec<Value>,
}

impl BatchResults {
    fn successful(&mut self, entry: Value) {
        self.successful.push(entry);
    }

    /// A `BatchResultErrorEntry` carries the SAME code a whole-request refusal
    /// would have, so a client can branch on one string whether it batched or
    /// not, and `SenderFault` is what tells it whether retrying can help.
    fn failed(&mut self, id: &str, error: &SqsError) {
        self.failed.push(serde_json::json!({
            "Id": id,
            "SenderFault": error.kind.fault() == crate::error::Fault::Sender,
            "Code": error.kind.json_type(),
            "Message": error.message,
        }));
    }

    /// An empty list is OMITTED, which is what AWS answers: a client that reads
    /// `Failed` and finds nothing there did not have a failure.
    fn into_value(self) -> Value {
        let mut out = Map::new();
        if !self.successful.is_empty() {
            out.insert("Successful".to_string(), Value::Array(self.successful));
        }
        if !self.failed.is_empty() {
            out.insert("Failed".to_string(), Value::Array(self.failed));
        }
        Value::Object(out)
    }
}

/// The entry list every batch action starts with, validated.
fn batch_entries(params: &Value) -> SqsResult<Vec<Entry<'_>>> {
    let entries = match params.get("Entries") {
        Some(Value::Array(entries)) => entries.as_slice(),
        // A batch action with no entry list is an EMPTY batch and not a missing
        // parameter: AWS gives the condition an error of its own because an
        // SDK's batching helper branches on it.
        None | Some(Value::Null) => &[][..],
        Some(_) => return Err(SqsError::new(ErrorKind::EmptyBatchRequest)),
    };
    let ids: Vec<String> = entries
        .iter()
        .map(|entry| text(entry, "Id").unwrap_or_default().to_string())
        .collect();
    check_entry_ids(&ids)?;
    Ok(entries
        .iter()
        .zip(ids)
        .map(|(params, id)| Entry { id, params })
        .collect())
}

/// Validate the ids a batch reports its results by: non-empty, within
/// [`MAX_BATCH_ENTRIES`], well-formed, and DISTINCT.
///
/// Distinctness is not fussiness — results are reported by the client's own
/// `Id`, so two entries sharing one id make the answer unreadable, which is why
/// AWS gives it an error of its own.
///
/// Public because [`crate::actions::check_batch`] is the same check for every
/// batch action, this one included, and two copies of a rule this exact is one
/// copy too many.
pub fn check_entry_ids(ids: &[String]) -> SqsResult<()> {
    if ids.is_empty() {
        return Err(SqsError::new(ErrorKind::EmptyBatchRequest));
    }
    if ids.len() > MAX_BATCH_ENTRIES {
        return Err(SqsError::with(
            ErrorKind::TooManyEntriesInBatchRequest,
            format!(
                "Maximum number of entries per request are {MAX_BATCH_ENTRIES}. You have sent {}.",
                ids.len()
            ),
        ));
    }
    for (index, id) in ids.iter().enumerate() {
        if !is_entry_id(id) {
            return Err(SqsError::with(
                ErrorKind::InvalidBatchEntryId,
                "A batch entry id can only contain alphanumeric characters, hyphens and \
                 underscores. It can be at most 80 letters long.",
            ));
        }
        if ids[..index].contains(id) {
            return Err(SqsError::with(
                ErrorKind::BatchEntryIdsNotDistinct,
                format!("Id {id} repeated."),
            ));
        }
    }
    Ok(())
}

/// The batch-entry-id charset, and its one owner: `PublishBatch` applies the
/// same rule under SNS's own error codes ([`crate::sns::publish`]).
pub(crate) fn is_entry_id(id: &str) -> bool {
    (1..=MAX_BATCH_ENTRY_ID_LEN).contains(&id.len())
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
}

// ----------------------------------------------------------------- the wire

/// One push, with the answer aligned to what was sent.
///
/// The client already aligns results by the index each carries; this is the
/// belt: an answer of the wrong length would attribute one message's id to
/// another message, which is the failure that tells a client its message landed
/// when a different one did.
async fn push_all(ctx: &Ctx, items: &[PushItem]) -> SqsResult<Vec<queen::Pushed>> {
    let pushed = ctx
        .facade
        .queen
        .push(items, ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    match pushed.len() == items.len() {
        true => Ok(pushed),
        false => Err(short_answer("push")),
    }
}

/// Poll every future to completion, concurrently, inside ONE task.
///
/// Not `tokio::spawn`: a spawned pop outlives the request that started it, so a
/// client that disconnects mid-receive — which is what every long-polling SDK
/// does on shutdown — would leave claims held on messages nobody will ack. Here
/// the futures are dropped with the request, and a cancelled receive claims
/// nothing.
pub(crate) async fn join_all<T>(futures: Vec<BoxFuture<'_, T>>) -> Vec<T> {
    let mut futures = futures;
    let mut done: Vec<Option<T>> = (0..futures.len()).map(|_| None).collect();
    {
        let (futures, done) = (&mut futures, &mut done);
        std::future::poll_fn(|cx| {
            let mut waiting = false;
            for (slot, future) in done.iter_mut().zip(futures.iter_mut()) {
                if slot.is_some() {
                    continue;
                }
                match future.as_mut().poll(cx) {
                    Poll::Ready(value) => *slot = Some(value),
                    Poll::Pending => waiting = true,
                }
            }
            match waiting {
                true => Poll::Pending,
                false => Poll::Ready(()),
            }
        })
        .await;
    }
    done.into_iter()
        .map(|value| value.expect("poll_fn returned only once every slot was filled"))
        .collect()
}

// ------------------------------------------------------- queues and receipts

/// The queue one request names.
///
/// `QueueUrl` is a CLIENT-SUPPLIED string on every action here, so this is a
/// parser of untrusted input: a traversal, another account's queue and a name
/// AWS would not have accepted all answer `QueueDoesNotExist`, which is also
/// what a client holding a URL from another deployment is told. The error names
/// nothing the client sent — a queue URL reaches this facade's logs through it.
fn queue_of(ctx: &Ctx, params: &Value) -> SqsResult<String> {
    let url = text(params, "QueueUrl").ok_or_else(|| missing_parameter("QueueUrl"))?;
    Naming::new(&ctx.facade.config.region, &ctx.facade.config.account)
        .name_of(url)
        .ok_or_else(|| SqsError::new(ErrorKind::QueueDoesNotExist))
}

/// The queue's registry record, which is where every attribute a message action
/// reads — the width, `MaximumMessageSize`, the visibility default,
/// `ContentBasedDeduplication` — actually lives.
async fn record_of(ctx: &Ctx, params: &Value) -> SqsResult<QueueRecord> {
    let name = queue_of(ctx, params)?;
    Ok(ctx.facade.registry.require(&name, ctx.token()).await?)
}

/// Decode a receipt handle and check it names THIS queue.
///
/// The queue check is not redundant with the tag: the tag proves this facade
/// minted the handle, and the queue field proves it minted it for the queue the
/// caller is now addressing. Without it a handle from one queue would delete
/// under another queue's URL, which is a cross-queue delete a client can perform
/// with two legitimate handles.
fn receipt_of(ctx: &Ctx, queue: &str, handle: Option<&str>) -> SqsResult<Receipt> {
    let handle = handle.ok_or_else(|| missing_parameter("ReceiptHandle"))?;
    let receipt = ctx.facade.handles.decode(handle, now_ms())?;
    match receipt.queue == queue {
        true => Ok(receipt),
        false => Err(SqsError::new(ErrorKind::ReceiptHandleIsInvalid)),
    }
}

/// Which lane a message with no group id lands on: the hash of its dedup key
/// across the queue's width, so sends spread without a coordinator and a lane is
/// decided before the push rather than after it (module header).
///
/// SHA-256 rather than a cheap mixer: the key is a uuid, so any hash spreads it,
/// and this one is already in the tree for SigV4 — a second hash function would
/// be a second thing to explain.
///
/// Public because it is THE lane function: the placement must be identical in
/// every instance and after every upgrade, so a second one anywhere in this
/// crate — however plausible its own hash — would put messages on lanes the send
/// path never chose.
pub fn lane_for(key: &str, partitions: u32) -> String {
    let width = u64::from(partitions.max(1));
    let digest = Sha256::digest(key.as_bytes());
    let head = u64::from_be_bytes(digest[..8].try_into().unwrap_or_default());
    (head % width).to_string()
}

/// The SHA-256 of the body, which is what `ContentBasedDeduplication` makes the
/// dedup key. Hex, lower case, and over the body's bytes exactly as sent.
fn content_key(body: &str) -> String {
    hex::encode(Sha256::digest(body.as_bytes()))
}

fn content_based(record: &QueueRecord) -> bool {
    record
        .attributes
        .get("ContentBasedDeduplication")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

// ----------------------------------------------------------- message payload

/// The `MessageAttributes` map, parsed and validated.
///
/// `pub(crate)` because SNS's `Publish` carries the SAME member with the same
/// wire shape, the same name rules, the same type labels and the same value
/// charset ([`crate::sns::publish`]). A second parser there would be a second
/// place the reserved-prefix rule can be forgotten.
pub(crate) fn message_attributes(
    value: Option<&Value>,
) -> SqsResult<BTreeMap<String, MessageAttribute>> {
    let Some(fields) = object_of(value, "MessageAttributes")? else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    for (name, entry) in fields {
        check_attribute_name(name)?;
        out.insert(name.clone(), attribute_value(name, entry)?);
    }
    Ok(out)
}

/// The `MessageSystemAttributes` map. AWS defines exactly one name, and a client
/// that invented another would be storing a field no consumer can ask for — so
/// the map is a closed set here rather than a passthrough.
fn system_attributes(value: Option<&Value>) -> SqsResult<BTreeMap<String, String>> {
    let Some(fields) = object_of(value, "MessageSystemAttributes")? else {
        return Ok(BTreeMap::new());
    };
    let mut out = BTreeMap::new();
    for (name, entry) in fields {
        if name != AWS_TRACE_HEADER {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!("Message system attribute name '{name}' is invalid."),
            ));
        }
        let attribute = attribute_value(name, entry)?;
        // EXACTLY `String`, not `String.something` and not `Number`. AWS refuses
        // anything else, and this facade could not echo a custom label anyway:
        // the digest over the system map ([`crate::md5`]) writes the type as
        // `String` unconditionally, so a stored `String.custom` would be a value
        // whose own digest disagrees with it.
        if attribute.data_type != "String" {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!("The message system attribute '{name}' must be of type String."),
            ));
        }
        match attribute.value {
            AttributeValue::String(text) => out.insert(name.clone(), text),
            AttributeValue::Binary(_) => {
                return Err(SqsError::with(
                    ErrorKind::InvalidParameterValue,
                    format!("The message system attribute '{name}' must be of type String."),
                ))
            }
        };
    }
    Ok(out)
}

/// One attribute entry: `{"DataType": …, "StringValue"|"BinaryValue": …}`.
fn attribute_value(name: &str, entry: &Value) -> SqsResult<MessageAttribute> {
    let entry = entry
        .as_object()
        .ok_or_else(|| invalid_attribute(name, "must be an object"))?;
    // The list forms exist in AWS's model and AWS refuses them on the wire; a
    // facade that dropped them silently would lose data the sender believes it
    // sent. BOTH SPELLINGS, because the two protocols spell them differently:
    // the canonical (JSON) member is plural, while the Query wire writes
    // `MessageAttribute.1.Value.StringListValue.1=…`, which the form flattener
    // rebuilds under the SINGULAR name and no lift renames — so a guard that
    // knew only the plural would miss exactly the protocol the guard exists for.
    for list in [
        "StringListValues",
        "BinaryListValues",
        "StringListValue",
        "BinaryListValue",
    ] {
        if entry.get(list).is_some_and(|v| !is_empty_list(v)) {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                "Message attribute list values in SendMessage operation are not supported.",
            ));
        }
    }
    let data_type = entry
        .get("DataType")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or_else(|| {
            SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "The message attribute '{name}' must contain a non-empty message \
                         attribute type."
                ),
            )
        })?;
    check_data_type(name, data_type)?;

    let binary = envelope::is_binary_type(data_type);
    let key = match binary {
        true => "BinaryValue",
        false => "StringValue",
    };
    let raw = entry
        .get(key)
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "The message attribute '{name}' must contain a non-empty message attribute \
                     value."
                ),
            )
        })?;

    let value = match binary {
        true => AttributeValue::Binary(
            base64::engine::general_purpose::STANDARD
                .decode(raw)
                .map_err(|_| invalid_attribute(name, "must be valid base64"))?,
        ),
        false => {
            // The body's charset is the attribute's charset too: a value with a
            // NUL in it reaches the XML rendering and produces a document no SDK
            // can parse.
            if let Some(c) = raw.chars().find(|c| !envelope::is_allowed_char(*c)) {
                return Err(invalid_attribute(
                    name,
                    &format!(
                        "contains U+{:04X}, which is outside the allowed character set",
                        c as u32
                    ),
                ));
            }
            if data_type.starts_with("Number") && raw.trim().parse::<f64>().is_err() {
                return Err(invalid_attribute(name, "can only include numeric values"));
            }
            AttributeValue::String(raw.to_string())
        }
    };
    Ok(MessageAttribute {
        data_type: data_type.to_string(),
        value,
    })
}

/// AWS's own name rules, which are stricter than "a JSON key": the reserved
/// prefixes are what stop a client from writing a name a future AWS attribute
/// would collide with.
fn check_attribute_name(name: &str) -> SqsResult<()> {
    let shaped = (1..=MAX_ATTRIBUTE_NAME_LEN).contains(&name.chars().count())
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
        && !name.starts_with('.')
        && !name.ends_with('.')
        && !name.contains("..");
    if !shaped {
        return Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            format!(
                "The message attribute name '{name}' is invalid. Attribute names can only contain \
                 alphanumeric characters, underscores, hyphens and periods."
            ),
        ));
    }
    for reserved in ["AWS.", "Amazon."] {
        // `str::get` rather than a slice: the shape check above has already
        // proved the name is ASCII, and a rule that depends on the order of two
        // checks for its memory safety is one refactor from a panic.
        if name
            .get(..reserved.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(reserved))
        {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "The message attribute name '{name}' begins with a reserved prefix and cannot \
                     be used."
                ),
            ));
        }
    }
    Ok(())
}

/// AWS's rule for a `MessageGroupId` and a `MessageDeduplicationId`: 1 to 128
/// characters, alphanumerics and the printable punctuation only.
///
/// It is not fussiness about a client's taste in identifiers. The group id
/// becomes a Queen PARTITION NAME and the dedup id a `transactionId`, and the
/// broker refuses a control character in either with a 400 whose message
/// describes a body the sender never wrote — so the choice is between AWS's own
/// refusal and an unreadable one.
pub(crate) fn check_fifo_id(name: &str, value: &str) -> SqsResult<()> {
    let shaped = (1..=MAX_FIFO_ID_LEN).contains(&value.chars().count())
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || FIFO_ID_PUNCTUATION.contains(c));
    match shaped {
        true => Ok(()),
        false => Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            format!(
                "Value for parameter {name} is invalid. Reason: {name} can only include \
                 alphanumeric and punctuation characters. 1 to {MAX_FIFO_ID_LEN} in length."
            ),
        )),
    }
}

/// `String`, `Number`, `Binary`, each optionally with a custom `.suffix`. The
/// label is kept VERBATIM afterwards because it is part of the MD5 the client
/// validates ([`crate::envelope::MessageAttribute`]).
fn check_data_type(name: &str, data_type: &str) -> SqsResult<()> {
    let base = data_type.split('.').next().unwrap_or_default();
    let shaped = matches!(base, "String" | "Number" | "Binary")
        && data_type.chars().count() <= MAX_ATTRIBUTE_NAME_LEN
        && !data_type.ends_with('.');
    match shaped {
        true => Ok(()),
        false => Err(invalid_attribute(
            name,
            "must have a type of String, Number or Binary, optionally with a custom label",
        )),
    }
}

/// What one message costs against `MaximumMessageSize`: the body plus every
/// attribute's name, type and value, which is AWS's own accounting. System
/// attributes are excluded, as they are at AWS: they are the service's fields
/// and not the sender's payload.
fn size_of(envelope: &Envelope) -> usize {
    let mut size = envelope.body.len();
    for (name, attribute) in &envelope.attributes {
        size += name.len() + attribute.data_type.len();
        size += match &attribute.value {
            AttributeValue::String(text) => text.len(),
            AttributeValue::Binary(bytes) => bytes.len(),
        };
    }
    size
}

/// Whether the size guard is answering for a batch, which decides which of the
/// two codes it refuses with. It is NOT inferred from the entry count: a batch
/// of one that is too long is still a batch, and an SDK's batching helper reads
/// the code and not the length.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Batched {
    Yes,
    No,
}

/// The size ceiling, for one message or for a whole batch.
///
/// AWS applies the SAME number to both — a batch's total may not exceed one
/// message's maximum — and refuses the WHOLE batch when it is exceeded, which is
/// why this is not a per-entry outcome.
fn guard_total_size(
    record: &QueueRecord,
    prepared: &[&Prepared],
    batched: Batched,
) -> SqsResult<()> {
    let max = attribute_i64(record, "MaximumMessageSize")
        .and_then(|n| usize::try_from(n).ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES)
        .min(MAX_MAX_MESSAGE_BYTES);
    let total: usize = prepared.iter().map(|p| p.size).sum();
    if total <= max {
        return Ok(());
    }
    Err(match batched == Batched::Yes {
        true => SqsError::new(ErrorKind::BatchRequestTooLong),
        // AWS's own sentence, and its own code: an oversized body is a bad
        // PARAMETER there, not a charset failure — `InvalidMessageContents` is
        // what a body outside the allowed character set answers.
        false => SqsError::with(
            ErrorKind::InvalidParameterValue,
            format!(
                "One or more parameters are invalid. Reason: Message must be shorter than \
                     {max} bytes."
            ),
        ),
    })
}

// ---------------------------------------------------------------- parameters

/// A string parameter, present and non-null.
fn text<'a>(params: &'a Value, name: &str) -> Option<&'a str> {
    params.get(name).and_then(Value::as_str)
}

/// A NUMBER, from either protocol: a Query form carries `"5"` and a JSON client
/// carries `5`, and this is the one place the two shapes do not converge on
/// their own (`crate::proto::query`, module header).
fn number(params: &Value, name: &str) -> SqsResult<Option<i64>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n.as_i64().map(Some).ok_or_else(|| not_a_number(name)),
        Some(Value::String(text)) if text.trim().is_empty() => Ok(None),
        Some(Value::String(text)) => text
            .trim()
            .parse::<i64>()
            .map(Some)
            .map_err(|_| not_a_number(name)),
        Some(_) => Err(not_a_number(name)),
    }
}

/// A number with AWS's own range on it. The refusal names the range, because a
/// client whose value is out of bounds is a client with a constant to fix.
fn bounded(params: &Value, name: &str, low: i64, high: i64) -> SqsResult<Option<i64>> {
    match number(params, name)? {
        Some(value) if !(low..=high).contains(&value) => Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            format!(
                "Value {value} for parameter {name} is invalid. Reason: must be {low} to {high}."
            ),
        )),
        other => Ok(other),
    }
}

/// A numeric attribute off the registry record. Unreadable is ABSENT: a stored
/// value this facade cannot parse must not become a limit no client can explain.
fn attribute_i64(record: &QueueRecord, name: &str) -> Option<i64> {
    record.attributes.get(name)?.trim().parse().ok()
}

/// The same, CLAMPED into the range the same parameter would have been held to
/// on the request.
///
/// It is not the registry's range check written twice. That one refuses a value
/// on the way IN, and this one decides what to do with a value that is already
/// stored: a record written by another version of this facade, by a hand-rolled
/// KV write, or by a `queen.` extension that widened a bound later. A queue
/// whose `ReceiveMessageWaitTimeSeconds` reads 300 must not park a receive for
/// five minutes — SQS's own ceiling is twenty seconds and every client's socket
/// timeout is written against it — and clamping is the only answer that keeps
/// serving the client instead of failing every receive on the queue.
fn queue_default(record: &QueueRecord, name: &str, low: i64, high: i64) -> Option<i64> {
    Some(attribute_i64(record, name)?.clamp(low, high))
}

/// A map-shaped parameter, or `None`. A shape that is not an object is refused
/// rather than ignored — a client sending a list where a map belongs has an
/// encoding bug, and dropping it silently sends the message without it.
fn object_of<'a>(
    value: Option<&'a Value>,
    name: &str,
) -> SqsResult<Option<&'a Map<String, Value>>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(fields)) if fields.is_empty() => Ok(None),
        Some(Value::Object(fields)) => Ok(Some(fields)),
        Some(Value::Array(items)) if items.is_empty() => Ok(None),
        Some(_) => Err(SqsError::with(
            ErrorKind::InvalidParameterValue,
            format!("The parameter {name} is not a map."),
        )),
    }
}

fn is_empty_list(value: &Value) -> bool {
    match value {
        Value::Array(items) => items.is_empty(),
        Value::Null => true,
        _ => false,
    }
}

/// The `Attributes` map of a receive, as a JSON object of strings.
fn map_of(values: &BTreeMap<String, String>) -> Value {
    Value::Object(
        values
            .iter()
            .map(|(name, value)| (name.clone(), Value::String(value.clone())))
            .collect(),
    )
}

/// The `MessageAttributes` of a receive, in the wire's own shape: a binary value
/// goes back as the base64 it arrived as, so a value never changes
/// representation anywhere between the two clients.
fn attributes_view(attributes: &BTreeMap<String, MessageAttribute>) -> Value {
    Value::Object(
        attributes
            .iter()
            .map(|(name, attribute)| {
                let mut entry = Map::with_capacity(2);
                entry.insert(
                    "DataType".to_string(),
                    Value::String(attribute.data_type.clone()),
                );
                match &attribute.value {
                    AttributeValue::String(text) => {
                        entry.insert("StringValue".to_string(), Value::String(text.clone()))
                    }
                    AttributeValue::Binary(bytes) => entry.insert(
                        "BinaryValue".to_string(),
                        Value::String(base64::engine::general_purpose::STANDARD.encode(bytes)),
                    ),
                };
                (name.clone(), Value::Object(entry))
            })
            .collect(),
    )
}

// -------------------------------------------------------------------- clocks

/// The facade's own wall clock, which is what a receipt handle's expiry is
/// measured against. It is deliberately NOT the broker's: the lease is the
/// broker's and it is the one that decides whether an ack lands.
fn now_ms() -> i64 {
    crate::obs::now_epoch_ms()
}

/// `SentTimestamp` — epoch milliseconds — out of the broker's own
/// `YYYY-MM-DDTHH:MM:SS.ffffffZ`. `None` for anything else, and the attribute is
/// then omitted rather than guessed: a wrong timestamp is worse than an absent
/// one for a client computing an age from it.
///
/// `pub(crate)` for the tests that pin the OTHER direction — the notification
/// timestamp [`crate::obs::iso8601_ms`] writes — against this parser, so the two
/// calendars in this crate are checked against each other rather than each
/// against itself.
pub(crate) fn epoch_ms_of(iso: &str) -> Option<i64> {
    let (date, rest) = iso.split_once('T')?;
    let mut parts = date.split('-');
    let year: i64 = parts.next()?.parse().ok()?;
    let month: i64 = parts.next()?.parse().ok()?;
    let day: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some() || !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    let time = rest.strip_suffix('Z').unwrap_or(rest);
    let (clock, fraction) = time.split_once('.').unwrap_or((time, ""));
    let mut parts = clock.split(':');
    let hour: i64 = parts.next()?.parse().ok()?;
    let minute: i64 = parts.next()?.parse().ok()?;
    let second: i64 = parts.next()?.parse().ok()?;
    if parts.next().is_some()
        || !(0..=23).contains(&hour)
        || !(0..=59).contains(&minute)
        || !(0..=60).contains(&second)
    {
        return None;
    }
    // Milliseconds, truncated: the broker writes microseconds and SentTimestamp
    // is milliseconds, so the three digits that survive are the ones AWS sends.
    let mut milli = 0i64;
    for (i, c) in fraction.chars().take(3).enumerate() {
        milli += i64::from(c.to_digit(10)?) * 10i64.pow(2 - i as u32);
    }
    Some(
        days_from_civil(year, month, day) * 86_400_000
            + (hour * 3_600 + minute * 60 + second) * 1_000
            + milli,
    )
}

/// Hinnant's `days_from_civil`, the inverse of the `civil_from_days` the test
/// double renders with. Branch-free calendar arithmetic, exact for every year
/// this will see.
fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

// -------------------------------------------------------------------- errors

fn missing_parameter(name: &str) -> SqsError {
    SqsError::with(
        ErrorKind::MissingParameter,
        format!("The request must contain the parameter {name}."),
    )
}

fn not_a_number(name: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!("The parameter {name} must be a whole number."),
    )
}

/// AWS's own sentence for a parameter that belongs to the OTHER queue type —
/// `DelaySeconds` on a FIFO queue, `MessageDeduplicationId` on a standard one.
fn wrong_queue_type(name: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!(
            "The request includes the parameter {name}, which is not valid for this queue type."
        ),
    )
}

fn invalid_attribute(name: &str, why: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!("The message attribute '{name}' {why}."),
    )
}

/// The broker answered fewer results than it was sent items. The client aligns
/// by index and would have refused a mismatched answer already; this is the
/// second belt, and it is `InternalFailure` rather than a guess because a
/// facade that paired the wrong id with the wrong message tells a client its
/// message landed when another one did.
fn short_answer(what: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InternalFailure,
        format!("The queue service answered fewer {what} results than it was sent items."),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::testing::{attribute, field, Rig, LANES};
    use crate::queen::testing::{iso_from_epoch_ms, TestClock};
    use crate::queen::QUEUE_MODE_GROUP;
    use serde_json::json;
    use std::time::Duration;

    // -------------------------------------------------------------- sending

    /// THE round trip, and the one every other test is a corner of: what a
    /// client sends is what the next client receives, digest for digest, and
    /// deleting it makes it go away.
    #[tokio::test]
    async fn a_send_survives_a_receive_and_a_delete_removes_it() {
        let rig = Rig::standard().await;
        let sent = rig
            .send(
                "orders",
                json!({
                    "MessageBody": "  {\"order\": 42}\n— é 🐤 ",
                    "MessageAttributes": {
                        "trace": {"DataType": "String", "StringValue": "abc"},
                        "count": {"DataType": "Number", "StringValue": "42"},
                        "blob": {"DataType": "Binary", "BinaryValue": "AAEC/w=="},
                        "custom": {"DataType": "String.tag", "StringValue": "x"}
                    }
                }),
            )
            .await
            .expect("the send is accepted");

        let mut messages = rig
            .receive_list("orders", json!({"MessageAttributeNames": ["All"]}))
            .await;
        assert_eq!(messages.len(), 1);
        let message = messages.remove(0);
        assert_eq!(field(&message, "MessageId"), field(&sent, "MessageId"));
        assert_eq!(field(&message, "Body"), "  {\"order\": 42}\n— é 🐤 ");
        // The body digest is the same value under the two names AWS gives it.
        assert_eq!(
            field(&message, "MD5OfBody"),
            field(&sent, "MD5OfMessageBody")
        );
        assert_eq!(
            field(&message, "MD5OfMessageAttributes"),
            field(&sent, "MD5OfMessageAttributes")
        );
        let attributes = message.get("MessageAttributes").expect("attributes");
        assert_eq!(attributes["trace"]["StringValue"], json!("abc"));
        assert_eq!(attributes["count"]["DataType"], json!("Number"));
        assert_eq!(attributes["blob"]["BinaryValue"], json!("AAEC/w=="));
        assert_eq!(attributes["custom"]["DataType"], json!("String.tag"));

        rig.delete("orders", field(&message, "ReceiptHandle"))
            .await
            .expect("the delete is accepted");
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
    }

    /// THE LOST MESSAGE. The broker reports what it did with each item in the
    /// item's own `status` and answers `201 Created` for the batch regardless,
    /// so a facade that reads only the HTTP code hands the client a MessageId
    /// and three valid digests for a message that was never written.
    #[tokio::test]
    async fn a_send_the_broker_did_not_store_is_not_a_successful_send() {
        for status in ["failed", "error"] {
            let rig = Rig::standard().await;
            rig.fake.next_push_statuses(&[status]);
            let error = rig
                .send("orders", json!({"MessageBody": "hello"}))
                .await
                .expect_err("a message that was not stored is not a success");
            // A `Receiver` fault and a 5xx, so an SDK's own retry policy sends
            // it again rather than moving on.
            assert_eq!(error.kind, ErrorKind::ServiceUnavailable, "{status}");
            assert_eq!(error.kind.http_status(), 503);
            assert!(error.message.contains(status), "{}", error.message);
            // ...and the queue really is empty, which is the fact the client
            // would otherwise have been told the opposite of.
            assert!(rig.receive_list("orders", json!({})).await.is_empty());
        }
    }

    /// `buffered` is the one status that looks like a failure and is not: the
    /// database transaction failed and the broker SPOOLED the item to its file
    /// buffer, relabelling it precisely so a client does not lose the message.
    #[tokio::test]
    async fn a_buffered_send_is_accepted_because_the_broker_kept_it() {
        let rig = Rig::standard().await;
        rig.fake.next_push_statuses(&["buffered"]);
        let sent = rig
            .send("orders", json!({"MessageBody": "hello"}))
            .await
            .expect("the broker spooled it and will replay it");
        assert!(!field(&sent, "MessageId").is_empty());
        // A spooled item has no offset yet, and a standard queue answers no
        // SequenceNumber anyway.
        assert!(sent.get("SequenceNumber").is_none());
    }

    /// In a batch the verdict is the ENTRY's and not the request's: nine
    /// messages that landed are not lost to one the broker did not write.
    #[tokio::test]
    async fn a_batch_reports_the_entry_the_broker_did_not_store() {
        let rig = Rig::standard().await;
        rig.fake.next_push_statuses(&["queued", "failed", "queued"]);
        let answer = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "a", "MessageBody": "1"},
                    {"Id": "b", "MessageBody": "2"},
                    {"Id": "c", "MessageBody": "3"}
                ]}),
            ),
        )
        .await
        .expect("the request itself succeeded");
        let successful = answer["Successful"].as_array().expect("successes");
        let failed = answer["Failed"].as_array().expect("failures");
        assert_eq!(successful.len(), 2);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["Id"], json!("b"));
        assert_eq!(failed[0]["Code"], json!("ServiceUnavailable"));
        // Not the sender's fault — retrying IS the right thing to do.
        assert_eq!(failed[0]["SenderFault"], json!(false));
        assert_eq!(successful[0]["Id"], json!("a"));
        assert_eq!(successful[1]["Id"], json!("c"));
    }

    /// A standard queue sends NO `transactionId`: the key would be a fresh uuid
    /// per request, so it deduplicates nothing, and the contract this facade
    /// states for the push wire is that a standard send carries none. A FIFO
    /// send carries the client's own dedup id, which is the whole point there.
    #[tokio::test]
    async fn only_a_fifo_send_carries_a_dedup_key_to_the_broker() {
        let rig = Rig::new(&[("orders", &[]), ("orders.fifo", &[])]).await;
        rig.send("orders", json!({"MessageBody": "hello"}))
            .await
            .unwrap();
        rig.send(
            "orders.fifo",
            json!({"MessageBody": "hello", "MessageGroupId": "g",
                   "MessageDeduplicationId": "d-1"}),
        )
        .await
        .unwrap();
        let pushed = rig.fake.pushed();
        assert_eq!(pushed[0].transaction_id, None, "a standard send is keyless");
        assert_eq!(pushed[1].transaction_id.as_deref(), Some("d-1"));
    }

    /// The digests are the ones [`crate::md5`] computes, which are the ones the
    /// SDKs recompute — so this pins the wiring rather than the algorithm.
    #[tokio::test]
    async fn the_send_answers_all_three_documented_digests() {
        let rig = Rig::standard().await;
        let sent = rig
            .send(
                "orders",
                json!({
                    "MessageBody": "hello",
                    "MessageAttributes": {"a": {"DataType": "String", "StringValue": "1"}},
                    "MessageSystemAttributes": {
                        "AWSTraceHeader": {"DataType": "String", "StringValue": "Root=1-2"}
                    }
                }),
            )
            .await
            .unwrap();
        assert_eq!(field(&sent, "MD5OfMessageBody"), md5::body_md5("hello"));
        let mut attributes = BTreeMap::new();
        attributes.insert("a".to_string(), MessageAttribute::string("String", "1"));
        assert_eq!(
            field(&sent, "MD5OfMessageAttributes"),
            md5::attributes_md5(&attributes).unwrap()
        );
        let mut system = BTreeMap::new();
        system.insert("AWSTraceHeader".to_string(), "Root=1-2".to_string());
        assert_eq!(
            field(&sent, "MD5OfMessageSystemAttributes"),
            md5::system_attributes_md5(&system).unwrap()
        );
    }

    /// An absent digest is an ABSENT KEY and never a null: an SDK that finds the
    /// key present compares it, and comparing against nothing fails.
    #[tokio::test]
    async fn a_message_without_attributes_omits_their_digests() {
        let rig = Rig::standard().await;
        let sent = rig
            .send("orders", json!({"MessageBody": "plain"}))
            .await
            .unwrap();
        let fields = sent.as_object().unwrap();
        assert!(fields.contains_key("MD5OfMessageBody"));
        assert!(!fields.contains_key("MD5OfMessageAttributes"));
        assert!(!fields.contains_key("MD5OfMessageSystemAttributes"));
        // …and a standard queue has no SequenceNumber at all.
        assert!(!fields.contains_key("SequenceNumber"));
    }

    /// The envelope is what is STORED, so a native Queen consumer reading this
    /// queue sees the documented shape and not a private encoding.
    #[tokio::test]
    async fn what_is_stored_is_the_documented_envelope() {
        let rig = Rig::standard().await;
        rig.send(
            "orders",
            json!({
                "MessageBody": "hello",
                "MessageAttributes": {"a": {"DataType": "Binary", "BinaryValue": "AAEC"}}
            }),
        )
        .await
        .unwrap();
        let pushed = rig.fake.pushed();
        assert_eq!(pushed.len(), 1);
        assert_eq!(
            pushed[0].payload,
            json!({"b": "hello", "a": {"a": {"t": "Binary", "v": "AAEC"}}})
        );
    }

    /// Both halves of the lane decision: every send lands inside the queue's
    /// declared width, and the hash actually spreads rather than pinning one
    /// lane.
    #[tokio::test]
    async fn sends_spread_across_the_queues_synthesized_lanes() {
        let rig = Rig::standard().await;
        for i in 0..64 {
            rig.send("orders", json!({"MessageBody": format!("m{i}")}))
                .await
                .unwrap();
        }
        let lanes: std::collections::BTreeSet<String> = rig
            .fake
            .pushed()
            .into_iter()
            .map(|item| item.partition)
            .collect();
        assert!(
            lanes
                .iter()
                .all(|lane| lane.parse::<u32>().is_ok_and(|lane| lane < LANES)),
            "a send landed outside the declared width: {lanes:?}"
        );
        assert!(lanes.len() > 1, "every send took the same lane: {lanes:?}");
    }

    /// The lane is a pure function of the key, so two facade instances choose
    /// the same one and a queue's width is the only thing that moves it.
    #[test]
    fn the_lane_is_deterministic_and_inside_the_width() {
        assert_eq!(lane_for("a-key", 8), lane_for("a-key", 8));
        for width in [1u32, 2, 8, 64, 100_000] {
            for i in 0..50 {
                let lane: u32 = lane_for(&format!("key-{i}"), width).parse().unwrap();
                assert!(lane < width, "{lane} is outside a width of {width}");
            }
        }
        // A width of zero is a record that lost its own attribute; the lane must
        // still be addressable rather than a division by zero.
        assert_eq!(lane_for("a-key", 0), "0");

        // PINNED, because the placement of a given key must survive an upgrade:
        // a lane that moved would strand every message already on the old one.
        assert_eq!(lane_for("00000000-0000-0000-0000-000000000001", 64), "13");
        assert_eq!(lane_for("", 64), "20");
        // ...and it spreads, which is the property the uuid buys.
        let lanes: BTreeMap<String, ()> = (0..500)
            .map(|i| (lane_for(&format!("msg-{i}"), 64), ()))
            .collect();
        assert!(lanes.len() > 50, "{} distinct lanes", lanes.len());
    }

    #[tokio::test]
    async fn a_send_without_a_body_is_a_missing_parameter() {
        let rig = Rig::standard().await;
        for body in [json!({}), json!({"MessageBody": ""})] {
            let error = rig.send("orders", body).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::MissingParameter);
        }
    }

    #[tokio::test]
    async fn a_body_outside_the_sqs_charset_is_refused() {
        let rig = Rig::standard().await;
        let error = rig
            .send("orders", json!({"MessageBody": "a\u{0}b"}))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidMessageContents);
        assert!(rig.fake.pushed().is_empty(), "a refused send still pushed");
    }

    /// The ceiling is the QUEUE's, and it counts the attributes: a message just
    /// under the limit passes and the same message with an attribute does not.
    #[tokio::test]
    async fn a_message_over_the_queues_maximum_is_refused() {
        let rig = Rig::new(&[("orders", &[("MaximumMessageSize", "1024")])]).await;
        rig.send("orders", json!({"MessageBody": "x".repeat(1024)}))
            .await
            .expect("exactly at the ceiling is accepted");
        let error = rig
            .send(
                "orders",
                json!({
                    "MessageBody": "x".repeat(1024),
                    "MessageAttributes": {"a": {"DataType": "String", "StringValue": "1"}}
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(
            error.message.contains("shorter than 1024 bytes"),
            "{}",
            error.message
        );
    }

    /// The attribute rules AWS enforces, one refusal each. None of these may
    /// reach the broker.
    #[tokio::test]
    async fn the_message_attribute_rules_are_enforced() {
        let rig = Rig::standard().await;
        let cases: &[(&str, Value)] = &[
            ("no type", json!({"a": {"StringValue": "1"}})),
            ("no value", json!({"a": {"DataType": "String"}})),
            (
                "empty value",
                json!({"a": {"DataType": "String", "StringValue": ""}}),
            ),
            (
                "unknown type",
                json!({"a": {"DataType": "Boolean", "StringValue": "1"}}),
            ),
            (
                "trailing dot",
                json!({"a": {"DataType": "String.", "StringValue": "1"}}),
            ),
            (
                "not a number",
                json!({"a": {"DataType": "Number", "StringValue": "x"}}),
            ),
            (
                "bad base64",
                json!({"a": {"DataType": "Binary", "BinaryValue": "not base64!"}}),
            ),
            (
                "wrong value key",
                json!({"a": {"DataType": "Binary", "StringValue": "AAEC"}}),
            ),
            (
                "reserved prefix",
                json!({"AWS.x": {"DataType": "String", "StringValue": "1"}}),
            ),
            (
                "reserved prefix, other spelling",
                json!({"Amazon.x": {"DataType": "String", "StringValue": "1"}}),
            ),
            (
                "leading dot",
                json!({".a": {"DataType": "String", "StringValue": "1"}}),
            ),
            (
                "double dot",
                json!({"a..b": {"DataType": "String", "StringValue": "1"}}),
            ),
            (
                "illegal character",
                json!({"a b": {"DataType": "String", "StringValue": "1"}}),
            ),
            (
                "control character in the value",
                json!({"a": {"DataType": "String", "StringValue": "x\u{0}"}}),
            ),
            (
                "list value",
                json!({"a": {"DataType": "String", "StringListValues": ["1"]}}),
            ),
            (
                "binary list value",
                json!({"a": {"DataType": "Binary", "BinaryListValues": ["AAEC"]}}),
            ),
            // The QUERY protocol's spelling of the same thing. AWS's form
            // encoding writes `MessageAttribute.1.Value.StringListValue.1=…`,
            // which the flattener rebuilds under the SINGULAR name with no lift
            // to rename it — so a guard that knew only the canonical plural
            // would drop the values silently, which is the exact failure the
            // guard exists to prevent.
            (
                "list value, query spelling",
                json!({"a": {"DataType": "String", "StringListValue": ["1"]}}),
            ),
            (
                "binary list value, query spelling",
                json!({"a": {"DataType": "Binary", "BinaryListValue": ["AAEC"]}}),
            ),
        ];
        for (why, attributes) in cases {
            let error = rig
                .send(
                    "orders",
                    json!({"MessageBody": "hello", "MessageAttributes": attributes}),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{why}");
            if why.starts_with("list value") || why.starts_with("binary list value") {
                assert!(
                    error.message.contains("list values"),
                    "{why} was refused for the wrong reason: {}",
                    error.message
                );
            }
        }
        assert!(
            rig.fake.pushed().is_empty(),
            "a refused send reached the broker"
        );
    }

    /// A `MessageGroupId` becomes a Queen PARTITION NAME and a
    /// `MessageDeduplicationId` becomes a `transactionId`, so unvalidated input
    /// here is client text choosing a broker identifier — and the broker's own
    /// refusal names a body the sender never wrote.
    #[tokio::test]
    async fn the_fifo_identifiers_are_held_to_aws_own_limits() {
        let rig = Rig::new(&[("orders.fifo", &[]), ("orders", &[])]).await;
        let long = "g".repeat(MAX_FIFO_ID_LEN + 1);
        let cases: &[(&str, &str, Value)] = &[
            (
                "a control character in the group",
                "MessageGroupId",
                json!({"MessageGroupId": "a\u{1}b", "MessageDeduplicationId": "d"}),
            ),
            (
                "a group past the length cap",
                "MessageGroupId",
                json!({"MessageGroupId": long, "MessageDeduplicationId": "d"}),
            ),
            (
                "a control character in the dedup id",
                "MessageDeduplicationId",
                json!({"MessageGroupId": "g", "MessageDeduplicationId": "a\u{1}b"}),
            ),
            (
                "a dedup id past the length cap",
                "MessageDeduplicationId",
                json!({"MessageGroupId": "g", "MessageDeduplicationId": long}),
            ),
        ];
        for (why, named, extra) in cases {
            let mut params = extra.clone();
            params["MessageBody"] = json!("hello");
            let error = rig.send("orders.fifo", params).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{why}");
            assert!(error.message.contains(named), "{why}: {}", error.message);
        }
        assert!(
            rig.fake.pushed().is_empty(),
            "a refused send reached the broker"
        );

        // The full punctuation set AWS allows, at the length cap, is accepted —
        // and so is a group id on a STANDARD queue, which is accepted and
        // ignored but still checked.
        let punctuated = format!("g-{}", "!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~");
        rig.send(
            "orders.fifo",
            json!({"MessageBody": "hello", "MessageGroupId": punctuated,
                   "MessageDeduplicationId": "d"}),
        )
        .await
        .expect("AWS's own character set");
        let error = rig
            .send(
                "orders",
                json!({"MessageBody": "hello", "MessageGroupId": "a\u{1}b"}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
    }

    /// The system attribute map is a CLOSED set: AWS defines one name and a
    /// facade that stored others would be storing fields no consumer can ask
    /// for.
    #[tokio::test]
    async fn only_the_trace_header_is_a_system_attribute() {
        let rig = Rig::standard().await;
        let error = rig
            .send(
                "orders",
                json!({
                    "MessageBody": "hello",
                    "MessageSystemAttributes": {"Mine": {"DataType": "String", "StringValue": "1"}}
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(error.message.contains("Mine"), "{}", error.message);

        // ...and its type must be EXACTLY String. AWS refuses anything else,
        // and this facade could not echo a custom label anyway: the digest over
        // the system map writes the type as `String` unconditionally, so a
        // stored `String.custom` would be a value whose own digest disagrees
        // with it.
        for data_type in ["String.custom", "Number", "Binary"] {
            let value = match data_type {
                "Binary" => json!({"DataType": "Binary", "BinaryValue": "AAEC"}),
                "Number" => json!({"DataType": "Number", "StringValue": "1"}),
                other => json!({"DataType": other, "StringValue": "Root=1-2"}),
            };
            let error = rig
                .send(
                    "orders",
                    json!({"MessageBody": "hello",
                           "MessageSystemAttributes": {AWS_TRACE_HEADER: value}}),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{data_type}");
            assert!(
                error.message.contains("must be of type String"),
                "{data_type}: {}",
                error.message
            );
        }
        // The one that is right still passes.
        rig.send(
            "orders",
            json!({"MessageBody": "hello", "MessageSystemAttributes": {
                AWS_TRACE_HEADER: {"DataType": "String", "StringValue": "Root=1-2"}}}),
        )
        .await
        .expect("the documented shape");
    }

    // --------------------------------------------------------------- delays

    /// A delayed send is a TIMER and not a push: nothing is in the log until it
    /// fires, which is what makes `DelaySeconds` exact rather than a sleep.
    #[tokio::test]
    async fn a_delayed_send_is_scheduled_and_not_pushed() {
        let rig = Rig::standard().await;
        let sent = rig
            .send(
                "orders",
                json!({"MessageBody": "later", "DelaySeconds": 60}),
            )
            .await
            .unwrap();
        assert!(rig.fake.pushed().is_empty(), "a delayed send was pushed");

        let calls = rig.fake.timer_calls.lock().unwrap().clone();
        assert_eq!(calls.len(), 1);
        let schedule = &calls[0][0];
        assert_eq!(schedule.op, "schedule");
        assert_eq!(schedule.queue, "orders");
        // MILLISECONDS on the wire, seconds in the request.
        assert_eq!(schedule.delay_ms, 60_000);
        // The timer key is the dedup key UNDER THE FACADE'S PREFIX, which is
        // what makes `ApproximateNumberOfMessagesDelayed` countable: the count
        // route refuses an empty prefix.
        assert_eq!(schedule.timer_key, timer_key(&schedule.txn));
        assert!(schedule.timer_key.starts_with(TIMER_KEY_PREFIX));
        // What the key does NOT buy on a standard queue is idempotent retry:
        // the dedup key is a fresh uuid per request, so a client that retries a
        // timed-out delayed send gets two delayed messages, exactly as it would
        // from AWS. (FIFO, where the key is stable, refuses DelaySeconds.)
        rig.send(
            "orders",
            json!({"MessageBody": "later", "DelaySeconds": 60}),
        )
        .await
        .unwrap();
        let calls = rig.fake.timer_calls.lock().unwrap().clone();
        assert_ne!(
            calls[0][0].timer_key, calls[1][0].timer_key,
            "a standard queue mints a fresh key per send"
        );
        let payload = base64::engine::general_purpose::STANDARD
            .decode(&schedule.payload)
            .expect("the payload is base64");
        assert_eq!(
            serde_json::from_slice::<Value>(&payload).unwrap(),
            json!({"b": "later"})
        );
        assert!(!field(&sent, "MessageId").is_empty());
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
    }

    /// …and it is delivered when the timer fires, under the id the send already
    /// answered.
    #[tokio::test]
    async fn a_delayed_message_arrives_when_its_timer_fires() {
        let rig = Rig::standard().await;
        let sent = rig
            .send(
                "orders",
                json!({"MessageBody": "later", "DelaySeconds": 60}),
            )
            .await
            .unwrap();
        rig.fake.advance(Duration::from_secs(59));
        assert!(rig.receive_list("orders", json!({})).await.is_empty());

        rig.fake.advance(Duration::from_secs(2));
        let message = rig.receive_one("orders").await;
        assert_eq!(field(&message, "Body"), "later");
        assert_eq!(field(&message, "MessageId"), field(&sent, "MessageId"));
    }

    #[tokio::test]
    async fn a_zero_delay_is_a_plain_push() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "now", "DelaySeconds": 0}))
            .await
            .unwrap();
        assert_eq!(rig.fake.pushed().len(), 1);
        assert!(rig.fake.timer_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn a_delay_beyond_the_ceiling_is_refused() {
        let rig = Rig::standard().await;
        let error = rig
            .send("orders", json!({"MessageBody": "x", "DelaySeconds": 901}))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(error.message.contains("0 to 900"), "{}", error.message);
    }

    /// `DelaySeconds` on a FIFO queue is refused at EVERY value, zero included:
    /// AWS refuses the PARAMETER and not the delay, so a client that sends an
    /// explicit zero — which several SDK helpers do when they fill in every
    /// field — gets the same answer here as it would there.
    #[tokio::test]
    async fn a_fifo_send_refuses_delay_seconds_at_any_value() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        for delay in [json!(0), json!(5), json!(900), json!("0")] {
            let error = rig
                .send(
                    "orders.fifo",
                    json!({
                        "MessageBody": "x",
                        "MessageGroupId": "g",
                        "MessageDeduplicationId": "d",
                        "DelaySeconds": delay
                    }),
                )
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{delay}");
            assert!(error.message.contains("DelaySeconds"), "{}", error.message);
            assert!(
                error.message.contains("queue type"),
                "the refusal does not say why: {}",
                error.message
            );
        }
        // A delay OUTSIDE the range is refused as a range, before the queue type
        // is consulted — the same order AWS validates in.
        let error = rig
            .send(
                "orders.fifo",
                json!({"MessageBody": "x", "MessageGroupId": "g",
                       "MessageDeduplicationId": "d", "DelaySeconds": 901}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(error.message.contains("0 to 900"), "{}", error.message);

        assert!(rig.fake.pushed().is_empty());
        assert!(rig.fake.timer_calls.lock().unwrap().is_empty());
    }

    /// …and inside a batch it is that ENTRY's failure, not the request's: a FIFO
    /// producer that sets a delay on one message of ten does not lose the nine.
    #[tokio::test]
    async fn a_fifo_batch_entry_with_a_delay_fails_only_that_entry() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        let answer = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders.fifo",
                json!({"Entries": [
                    {"Id": "plain", "MessageBody": "a", "MessageGroupId": "g",
                     "MessageDeduplicationId": "d1"},
                    {"Id": "delayed", "MessageBody": "b", "MessageGroupId": "g",
                     "MessageDeduplicationId": "d2", "DelaySeconds": 30}
                ]}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(
            answer["Successful"]
                .as_array()
                .unwrap()
                .iter()
                .map(|e| field(e, "Id"))
                .collect::<Vec<_>>(),
            ["plain"]
        );
        let failed = &answer["Failed"][0];
        assert_eq!(field(failed, "Id"), "delayed");
        assert_eq!(
            field(failed, "Code"),
            ErrorKind::InvalidParameterValue.json_type()
        );
        assert_eq!(rig.fake.pushed().len(), 1);
        assert!(rig.fake.timer_calls.lock().unwrap().is_empty());
    }

    /// DIVERGENCE, `accepted`, pinned so it is a decision and not a drift: on a
    /// queue that also carries a queue-level `DelaySeconds`, the two ADD. The
    /// facade's timer is the MESSAGE's delay — which is what this asserts — and
    /// the queue's is the broker's `delayed_processing`, which hides the segment
    /// the timer wrote for its own further seconds (004_log_pop.sql). Closing it
    /// would need a per-push "already late" flag on the broker, a core change
    /// PLAN_QUEEN_SQS.md does not take.
    #[tokio::test]
    async fn a_per_message_delay_is_the_timers_and_the_queues_own_is_the_brokers() {
        let rig = Rig::new(&[("orders", &[("DelaySeconds", "30")])]).await;
        rig.send("orders", json!({"MessageBody": "x", "DelaySeconds": 60}))
            .await
            .unwrap();
        let calls = rig.fake.timer_calls.lock().unwrap().clone();
        assert_eq!(
            calls[0][0].delay_ms, 60_000,
            "the queue's delay was added in"
        );
        assert!(rig.fake.pushed().is_empty());

        // …and a send that names no delay of its own schedules nothing at all:
        // the queue's is the broker's, and a facade timer for it would delay the
        // message twice over.
        rig.send("orders", json!({"MessageBody": "y"}))
            .await
            .unwrap();
        assert_eq!(rig.fake.timer_calls.lock().unwrap().len(), 1);
        assert_eq!(rig.fake.pushed().len(), 1);
    }

    /// A form carries only strings, so this is the shape a Query client sends
    /// and it must mean the same thing as the JSON client's number.
    #[tokio::test]
    async fn a_numeric_parameter_arrives_as_a_string_from_the_query_protocol() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x", "DelaySeconds": "60"}))
            .await
            .unwrap();
        let calls = rig.fake.timer_calls.lock().unwrap().clone();
        assert_eq!(calls[0][0].delay_ms, 60_000);
    }

    // ----------------------------------------------------------------- FIFO

    /// A FIFO lane IS the group, and the dedup key IS the transaction id: the
    /// whole FIFO mapping in one assertion.
    #[tokio::test]
    async fn a_fifo_send_is_addressed_by_its_group_and_keyed_by_its_dedup_id() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        let sent = rig
            .send(
                "orders.fifo",
                json!({
                    "MessageBody": "hello",
                    "MessageGroupId": "customer-7",
                    "MessageDeduplicationId": "order-1"
                }),
            )
            .await
            .unwrap();
        let pushed = rig.fake.pushed();
        assert_eq!(pushed[0].partition, "customer-7");
        assert_eq!(pushed[0].transaction_id.as_deref(), Some("order-1"));
        // The SequenceNumber is the absolute offset the push allocated.
        assert_eq!(field(&sent, "SequenceNumber"), "0");
    }

    /// A duplicate inside the window is not an error: it answers the ORIGINAL
    /// message's id and sequence number, which is what SQS answers, and nothing
    /// is written twice.
    #[tokio::test]
    async fn a_fifo_duplicate_answers_the_original_message() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        let params = json!({
            "MessageBody": "hello",
            "MessageGroupId": "g",
            "MessageDeduplicationId": "d1"
        });
        let first = rig.send("orders.fifo", params.clone()).await.unwrap();
        let second = rig.send("orders.fifo", params).await.unwrap();
        assert_eq!(field(&first, "MessageId"), field(&second, "MessageId"));
        assert_eq!(
            field(&first, "SequenceNumber"),
            field(&second, "SequenceNumber")
        );
        assert_eq!(rig.fake.lane("orders.fifo", "g").len(), 1);
    }

    #[tokio::test]
    async fn a_content_based_fifo_queue_derives_its_dedup_key_from_the_body() {
        let rig = Rig::new(&[("orders.fifo", &[("ContentBasedDeduplication", "true")])]).await;
        rig.send(
            "orders.fifo",
            json!({"MessageBody": "hello", "MessageGroupId": "g"}),
        )
        .await
        .unwrap();
        assert_eq!(
            rig.fake.pushed()[0].transaction_id.as_deref(),
            Some(content_key("hello").as_str())
        );
        assert_eq!(content_key("hello").len(), 64);
    }

    /// The four refusals that are the FIFO/standard boundary.
    #[tokio::test]
    async fn the_two_queue_types_refuse_each_others_parameters() {
        let fifo = Rig::new(&[("orders.fifo", &[])]).await;
        let standard = Rig::standard().await;

        let error = fifo
            .send("orders.fifo", json!({"MessageBody": "x"}))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingParameter);
        assert!(
            error.message.contains("MessageGroupId"),
            "{}",
            error.message
        );

        let error = fifo
            .send(
                "orders.fifo",
                json!({"MessageBody": "x", "MessageGroupId": "g"}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(
            error.message.contains("ContentBasedDeduplication"),
            "{}",
            error.message
        );

        let error = fifo
            .send(
                "orders.fifo",
                json!({
                    "MessageBody": "x",
                    "MessageGroupId": "g",
                    "MessageDeduplicationId": "d",
                    "DelaySeconds": 5
                }),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(error.message.contains("DelaySeconds"), "{}", error.message);

        let error = standard
            .send(
                "orders",
                json!({"MessageBody": "x", "MessageDeduplicationId": "d"}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(
            error.message.contains("MessageDeduplicationId"),
            "{}",
            error.message
        );
    }

    /// A FIFO receive answers the two attributes only a FIFO queue has, and both
    /// are what the send wrote rather than anything re-derived.
    #[tokio::test]
    async fn a_fifo_receive_answers_the_group_and_the_dedup_id() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        rig.send(
            "orders.fifo",
            json!({
                "MessageBody": "hello",
                "MessageGroupId": "customer-7",
                "MessageDeduplicationId": "order-1"
            }),
        )
        .await
        .unwrap();
        let mut messages = rig
            .receive_list("orders.fifo", json!({"AttributeNames": ["All"]}))
            .await;
        let message = messages.remove(0);
        assert_eq!(
            attribute(&message, "MessageGroupId").as_deref(),
            Some("customer-7")
        );
        assert_eq!(
            attribute(&message, "MessageDeduplicationId").as_deref(),
            Some("order-1")
        );
    }

    /// A `MessageGroupId` is required on EVERY FIFO send, whatever else the
    /// queue can derive: the group is the lane, and a message with no lane has
    /// nowhere to be ordered.
    #[tokio::test]
    async fn a_group_id_is_required_even_when_the_queue_derives_the_dedup_key() {
        let rig = Rig::new(&[("orders.fifo", &[("ContentBasedDeduplication", "true")])]).await;
        let error = rig
            .send("orders.fifo", json!({"MessageBody": "hello"}))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingParameter);
        assert!(
            error.message.contains("MessageGroupId"),
            "{}",
            error.message
        );
        assert!(rig.fake.pushed().is_empty());
    }

    /// `ContentBasedDeduplication` decides only what happens when the client
    /// names NO dedup id: an explicit one always wins, because a sender that
    /// named a key means that key.
    #[tokio::test]
    async fn an_explicit_dedup_id_wins_over_the_queues_own_derivation() {
        let rig = Rig::new(&[("orders.fifo", &[("ContentBasedDeduplication", "true")])]).await;
        rig.send(
            "orders.fifo",
            json!({
                "MessageBody": "hello",
                "MessageGroupId": "g",
                "MessageDeduplicationId": "mine"
            }),
        )
        .await
        .unwrap();
        assert_eq!(
            rig.fake.pushed()[0].transaction_id.as_deref(),
            Some("mine"),
            "the client's key, not the body's digest"
        );
    }

    /// The FIFO rules are the ENTRY's, so one entry that names no group does not
    /// take the other nine down with it.
    #[tokio::test]
    async fn a_fifo_batch_validates_every_entry_on_its_own() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        let answer = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders.fifo",
                json!({"Entries": [
                    {"Id": "a", "MessageBody": "1", "MessageGroupId": "g",
                     "MessageDeduplicationId": "d1"},
                    {"Id": "b", "MessageBody": "2", "MessageDeduplicationId": "d2"},
                    {"Id": "c", "MessageBody": "3", "MessageGroupId": "g",
                     "MessageDeduplicationId": "d3"}
                ]}),
            ),
        )
        .await
        .expect("the batch is answered");
        let successful = answer["Successful"].as_array().unwrap();
        let failed = answer["Failed"].as_array().unwrap();
        assert_eq!(successful.len(), 2);
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["Id"], json!("b"));
        assert_eq!(failed[0]["Code"], json!("MissingParameter"));
        assert_eq!(failed[0]["SenderFault"], json!(true));
        // The two good ones really landed, in order, on the one lane.
        assert_eq!(rig.fake.lane("orders.fifo", "g").len(), 2);
    }

    /// The dedup window is the broker's own: a repeated key inside it answers
    /// the ORIGINAL message and delivers ONE message, which is what SQS's
    /// five-minute window does.
    #[tokio::test]
    async fn a_deduplicated_send_is_delivered_once() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        let params = json!({
            "MessageBody": "hello",
            "MessageGroupId": "g",
            "MessageDeduplicationId": "d1"
        });
        rig.send("orders.fifo", params.clone()).await.unwrap();
        rig.send("orders.fifo", params).await.unwrap();
        let messages = rig
            .receive_list("orders.fifo", json!({"MaxNumberOfMessages": 10}))
            .await;
        assert_eq!(messages.len(), 1, "the duplicate was never written");
    }

    /// DIVERGENCE, `accepted`, pinned: the `SequenceNumber` is the offset within
    /// its OWN group, so every group starts at zero and the same number appears
    /// in each. It orders a group's messages exactly — which is what a FIFO
    /// consumer reads it for — and there is no queue-wide counter to answer
    /// with instead.
    #[tokio::test]
    async fn a_sequence_number_counts_within_its_own_group() {
        let rig = Rig::new(&[("orders.fifo", &[]), ("orders", &[])]).await;
        let send = |group: &'static str, dedup: &'static str| {
            rig.send(
                "orders.fifo",
                json!({"MessageBody": "x", "MessageGroupId": group,
                       "MessageDeduplicationId": dedup}),
            )
        };
        assert_eq!(field(&send("a", "1").await.unwrap(), "SequenceNumber"), "0");
        assert_eq!(field(&send("a", "2").await.unwrap(), "SequenceNumber"), "1");
        assert_eq!(
            field(&send("b", "3").await.unwrap(), "SequenceNumber"),
            "0",
            "a second group starts at zero too"
        );
        // A standard queue has no sequence at all, and answering one would be
        // answering a field AWS does not send.
        let standard = rig
            .send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        assert!(standard.get("SequenceNumber").is_none());
    }

    /// THE SEND AND THE RECEIVE ANSWER THE SAME NUMBER. C-SQS-3 puts the
    /// absolute offset on every popped message, so the `SequenceNumber` a client
    /// was told at `SendMessage` is the one it reads back off the delivery —
    /// read twice, never derived ([`system_view`]). A client that correlates the
    /// two is the only reason the field exists.
    #[tokio::test]
    async fn a_fifo_receive_carries_the_sequence_number_the_send_answered() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        let mut sent = Vec::new();
        for (dedup, body) in [("d1", "one"), ("d2", "two"), ("d3", "three")] {
            let answer = rig
                .send(
                    "orders.fifo",
                    json!({"MessageBody": body, "MessageGroupId": "g",
                           "MessageDeduplicationId": dedup}),
                )
                .await
                .unwrap();
            sent.push(field(&answer, "SequenceNumber").to_string());
        }
        assert_eq!(sent, vec!["0", "1", "2"], "the send side, for the record");

        let messages = rig
            .receive_list(
                "orders.fifo",
                json!({"MaxNumberOfMessages": 10, "AttributeNames": ["All"]}),
            )
            .await;
        assert_eq!(messages.len(), 3);
        let received: Vec<String> = messages
            .iter()
            .map(|m| attribute(m, "SequenceNumber").expect("C-SQS-3 offset on the pop"))
            .collect();
        assert_eq!(received, sent);
        // The rest of the FIFO view is unchanged.
        assert!(attribute(&messages[0], "MessageGroupId").is_some());
        assert!(attribute(&messages[0], "MessageDeduplicationId").is_some());
        assert!(attribute(&messages[0], "ApproximateReceiveCount").is_some());
    }

    /// THE BACKSTOP THE BOOT REFUSAL MADE UNREACHABLE, pinned anyway. A process
    /// can no longer start in `amortized` — `Config::from_source` refuses the
    /// variable, naming C-SQS-1 — so the only way into this state is a `Config`
    /// built in process. What must NOT happen there is a refused receive: the
    /// fallback is exact mode, which is correct and merely slower than what was
    /// asked for, and the operator is told once per minute rather than per call.
    #[tokio::test]
    async fn an_amortized_config_still_serves_the_receive_in_exact_mode() {
        let rig = Rig::standard().await;
        let amortized = rig.sibling_with(|c| c.receive_mode = ReceiveMode::Amortized);
        assert_eq!(
            amortized.ctx.facade.config.receive_mode,
            ReceiveMode::Amortized
        );
        rig.send("orders", json!({"MessageBody": "served anyway"}))
            .await
            .unwrap();
        let messages = amortized.receive_list("orders", json!({})).await;
        assert_eq!(messages.len(), 1);
        assert_eq!(field(&messages[0], "Body"), "served anyway");
    }

    /// …and a STANDARD queue answers none, on the receive as on the send: the
    /// offset is there on the wire, and SQS has no such field off a FIFO queue.
    #[tokio::test]
    async fn a_standard_receive_answers_no_sequence_number() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "hello"}))
            .await
            .unwrap();
        let messages = rig
            .receive_list("orders", json!({"AttributeNames": ["All"]}))
            .await;
        assert_eq!(attribute(&messages[0], "SequenceNumber"), None);
    }

    /// A pre-C-SQS-3 broker sends no `offset`, and the receive is one field
    /// lighter rather than failed or invented ([`queen::Message::offset`]). This
    /// drives [`system_view`] directly because the double is faithful to the
    /// CURRENT broker and there is no way to ask it to be an older one.
    #[tokio::test]
    async fn an_absent_offset_leaves_the_sequence_number_absent() {
        let rig = Rig::new(&[("orders.fifo", &[])]).await;
        rig.send(
            "orders.fifo",
            json!({"MessageBody": "hello", "MessageGroupId": "g",
                   "MessageDeduplicationId": "d1"}),
        )
        .await
        .unwrap();
        let record = rig
            .ctx
            .facade
            .registry
            .require("orders.fifo", None)
            .await
            .unwrap();
        let popped = rig
            .ctx
            .facade
            .queen
            .pop_queue("orders.fifo", &queen::PopOptions::default(), None)
            .await
            .unwrap();
        let mut message = popped.messages[0].clone();
        let envelope = Envelope::decode(&message.data);

        assert!(message.offset.is_some(), "the double speaks C-SQS-3");
        assert!(system_view(&record, &message, &envelope).contains_key("SequenceNumber"));
        // The one difference an older broker makes.
        message.offset = None;
        let view = system_view(&record, &message, &envelope);
        assert!(!view.contains_key("SequenceNumber"));
        // Everything else a FIFO consumer reads is still there.
        assert!(view.contains_key("MessageGroupId"));
        assert!(view.contains_key("MessageDeduplicationId"));
        assert!(view.contains_key("ApproximateReceiveCount"));
    }

    // -------------------------------------------------------------- batches

    #[tokio::test]
    async fn a_batch_send_answers_every_entry_under_its_own_id() {
        let rig = Rig::standard().await;
        let answer = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "a", "MessageBody": "one"},
                    {"Id": "b", "MessageBody": "two"},
                    {"Id": "c", "MessageBody": "three"}
                ]}),
            ),
        )
        .await
        .unwrap();
        let successful = answer["Successful"].as_array().unwrap();
        assert_eq!(successful.len(), 3);
        assert!(answer.get("Failed").is_none(), "an empty list was rendered");
        let ids: Vec<&str> = successful.iter().map(|e| field(e, "Id")).collect();
        assert_eq!(ids, ["a", "b", "c"]);
        assert_eq!(
            field(&successful[0], "MD5OfMessageBody"),
            md5::body_md5("one")
        );
        // ONE push for the batch, not one per entry.
        assert_eq!(rig.fake.pushes.lock().unwrap().len(), 1);
        assert_eq!(rig.fake.pushed().len(), 3);
    }

    /// The point of the shape: one bad entry does not lose the good ones, and
    /// the failure carries the code a whole-request refusal would have.
    #[tokio::test]
    async fn a_batch_reports_per_entry_failures_and_keeps_the_rest() {
        let rig = Rig::standard().await;
        let answer = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "good", "MessageBody": "fine"},
                    {"Id": "empty"},
                    {"Id": "control", "MessageBody": "a\u{0}b"}
                ]}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(answer["Successful"].as_array().unwrap().len(), 1);
        let failed = answer["Failed"].as_array().unwrap();
        assert_eq!(failed.len(), 2);
        assert_eq!(field(&failed[0], "Id"), "empty");
        assert_eq!(field(&failed[0], "Code"), "MissingParameter");
        assert_eq!(failed[0]["SenderFault"], json!(true));
        assert_eq!(field(&failed[1], "Code"), "InvalidMessageContents");
        // Only the good entry was sent.
        assert_eq!(rig.fake.pushed().len(), 1);
    }

    /// The three envelope refusals, which AWS gives three different codes
    /// because an SDK's batching helper branches on which.
    #[tokio::test]
    async fn the_batch_envelope_has_three_distinct_refusals() {
        let rig = Rig::standard().await;
        let entry = |i: usize| json!({"Id": format!("id{i}"), "MessageBody": "x"});

        for empty in [json!({}), json!({"Entries": []})] {
            let error = send_message_batch(&rig.ctx, &rig.params("orders", empty))
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::EmptyBatchRequest);
        }

        let too_many: Vec<Value> = (0..11).map(entry).collect();
        let error = send_message_batch(
            &rig.ctx,
            &rig.params("orders", json!({"Entries": too_many})),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::TooManyEntriesInBatchRequest);

        let error = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "same", "MessageBody": "x"},
                    {"Id": "same", "MessageBody": "y"}
                ]}),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::BatchEntryIdsNotDistinct);
        assert!(rig.fake.pushed().is_empty());
    }

    #[test]
    fn a_batch_entry_id_is_checked_before_anything_is_sent() {
        assert!(check_entry_ids(&["a".to_string()]).is_ok());
        assert!(check_entry_ids(&["a-b_C9".to_string()]).is_ok());
        for bad in ["", "with space", "dotted.id", &"x".repeat(81)] {
            let error = check_entry_ids(&[bad.to_string()]).unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidBatchEntryId, "{bad:?}");
        }
    }

    /// AWS's own charset, at both ends of the length: `[a-zA-Z0-9_-]{1,80}`. It
    /// is not decoration — the id is what a result is reported under, so a
    /// client's own parser reads it back.
    #[test]
    fn the_entry_id_charset_is_alphanumerics_hyphens_and_underscores() {
        for good in ["a", "A", "9", "a_b", "a-b", &"x".repeat(80)] {
            assert!(is_entry_id(good), "{good:?} was refused");
        }
        for bad in [
            "",
            &"x".repeat(81),
            "with space",
            "dotted.id",
            "a:b",
            "a/b",
            "é",
            "a\nb",
        ] {
            assert!(!is_entry_id(bad), "{bad:?} was accepted");
        }
    }

    // ------------------------------------------------- the batch envelope (M1)

    /// The cap is ONE number. Two constants for one AWS limit is one constant
    /// too many, and the copy that drifted would be the one an action applied.
    #[test]
    fn the_batch_cap_is_one_number_in_both_modules() {
        assert_eq!(MAX_BATCH_ENTRIES, crate::actions::MAX_BATCH_ENTRIES);
        assert_eq!(MAX_BATCH_ENTRIES, 10);
    }

    /// An entry that names no `ReceiptHandle` fails as ITSELF, with the
    /// parameter it is missing — not as a malformed batch, and not silently.
    #[tokio::test]
    async fn a_delete_batch_entry_without_a_handle_is_a_per_entry_missing_parameter() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 1);
        let message = rig.receive_one("orders").await;
        let answer = delete_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "real", "ReceiptHandle": field(&message, "ReceiptHandle")},
                    {"Id": "handleless"}
                ]}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(field(&answer["Successful"][0], "Id"), "real");
        let failed = &answer["Failed"][0];
        assert_eq!(field(failed, "Id"), "handleless");
        assert_eq!(
            field(failed, "Code"),
            ErrorKind::MissingParameter.json_type()
        );
        assert!(
            field(failed, "Message").contains("ReceiptHandle"),
            "{failed}"
        );
    }

    /// The three batch actions, by name. Every rule below belongs to the
    /// ENVELOPE rather than to one verb, and AWS applies all of them to all
    /// three — so they are proved on all three rather than on whichever one a
    /// test happened to be written for.
    const BATCH_ACTIONS: [&str; 3] = [
        "SendMessageBatch",
        "DeleteMessageBatch",
        "ChangeMessageVisibilityBatch",
    ];

    async fn batch(rig: &Rig, action: &str, extra: Value) -> SqsResult<Value> {
        let params = rig.params("orders", extra);
        match action {
            "SendMessageBatch" => send_message_batch(&rig.ctx, &params).await,
            "DeleteMessageBatch" => delete_message_batch(&rig.ctx, &params).await,
            "ChangeMessageVisibilityBatch" => {
                change_message_visibility_batch(&rig.ctx, &params).await
            }
            other => panic!("{other} is not a batch action"),
        }
    }

    /// `unwrap_err`, naming the action that was supposed to refuse — a loop over
    /// three actions must say which one answered.
    fn expect_refusal(action: &str, result: SqsResult<Value>) -> SqsError {
        match result {
            Ok(answer) => panic!("{action} answered {answer} instead of refusing"),
            Err(error) => error,
        }
    }

    /// One entry that is well-formed for whichever of the three reads it: a body
    /// for the send, a handle for the delete, a timeout for the visibility
    /// change. The envelope is checked before any of them is looked at, which is
    /// what makes one shape enough.
    fn any_entry(id: &str) -> Value {
        json!({"Id": id, "MessageBody": "x", "ReceiptHandle": "h", "VisibilityTimeout": 30})
    }

    #[tokio::test]
    async fn every_batch_action_refuses_an_empty_batch() {
        let rig = Rig::standard().await;
        for action in BATCH_ACTIONS {
            for empty in [json!({}), json!({"Entries": []})] {
                let error = expect_refusal(action, batch(&rig, action, empty.clone()).await);
                assert_eq!(error.kind, ErrorKind::EmptyBatchRequest, "{action} {empty}");
            }
        }
    }

    #[tokio::test]
    async fn every_batch_action_refuses_more_than_ten_entries() {
        let rig = Rig::standard().await;
        let ten: Vec<Value> = (0..10).map(|i| any_entry(&format!("e{i}"))).collect();
        let eleven: Vec<Value> = (0..11).map(|i| any_entry(&format!("e{i}"))).collect();
        for action in BATCH_ACTIONS {
            // Ten is the cap and not one under it: an SDK batches to exactly
            // ten, and a facade that refused them would refuse every batch.
            batch(&rig, action, json!({ "Entries": ten }))
                .await
                .unwrap_or_else(|e| panic!("{action} refused a full batch: {e:?}"));

            let error = expect_refusal(
                action,
                batch(&rig, action, json!({ "Entries": eleven })).await,
            );
            assert_eq!(
                error.kind,
                ErrorKind::TooManyEntriesInBatchRequest,
                "{action}"
            );
            assert!(error.message.contains("10"), "{action}: {}", error.message);
        }
    }

    #[tokio::test]
    async fn every_batch_action_refuses_repeated_entry_ids() {
        let rig = Rig::standard().await;
        let repeated = json!({"Entries": [any_entry("same"), any_entry("same")]});
        for action in BATCH_ACTIONS {
            let error = expect_refusal(action, batch(&rig, action, repeated.clone()).await);
            assert_eq!(error.kind, ErrorKind::BatchEntryIdsNotDistinct, "{action}");
            assert!(
                error.message.contains("same"),
                "{action}: {}",
                error.message
            );
        }
    }

    #[tokio::test]
    async fn every_batch_action_refuses_a_malformed_entry_id() {
        let rig = Rig::standard().await;
        for action in BATCH_ACTIONS {
            for entry in [
                any_entry(""),
                any_entry("with space"),
                any_entry("dotted.id"),
                any_entry(&"x".repeat(81)),
                // No `Id` at all is the same refusal: the answer is reported
                // BY the id, so an entry without one has nowhere to be reported.
                json!({"MessageBody": "x", "ReceiptHandle": "h"}),
            ] {
                let error = expect_refusal(
                    action,
                    batch(&rig, action, json!({"Entries": [entry.clone()]})).await,
                );
                assert_eq!(
                    error.kind,
                    ErrorKind::InvalidBatchEntryId,
                    "{action} {entry}"
                );
            }
        }
    }

    /// The envelope is refused BEFORE any entry is looked at, so an over-long
    /// batch of entries that are each also wrong is still the envelope's error —
    /// and none of it reaches Queen. An SDK's batching helper branches on the
    /// code, and "your eleventh entry has a bad handle" is not a code it can act
    /// on.
    #[tokio::test]
    async fn the_batch_envelope_is_refused_before_any_entry_reaches_queen() {
        let rig = Rig::standard().await;
        let bad: Vec<Value> = (0..11)
            .map(|i| json!({"Id": format!("e{i}"), "ReceiptHandle": "forged"}))
            .collect();
        for action in BATCH_ACTIONS {
            let error = expect_refusal(
                action,
                batch(&rig, action, json!({"Entries": bad.clone()})).await,
            );
            assert_eq!(
                error.kind,
                ErrorKind::TooManyEntriesInBatchRequest,
                "{action}"
            );
        }
        assert!(rig.fake.pushed().is_empty());
        assert!(rig.fake.acks.lock().unwrap().is_empty());
        assert!(rig.fake.extends.lock().unwrap().is_empty());
    }

    /// Every `BatchResultErrorEntry` carries the four fields AWS documents, and
    /// `SenderFault` is the one a client acts on: it is what says whether
    /// sending the same entry again can work.
    #[tokio::test]
    async fn a_failed_batch_entry_carries_the_four_documented_fields() {
        let rig = Rig::standard().await;
        let answer = delete_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [{"Id": "forged", "ReceiptHandle": "nope"}]}),
            ),
        )
        .await
        .unwrap();
        let failed = &answer["Failed"][0];
        assert_eq!(field(failed, "Id"), "forged");
        assert_eq!(
            field(failed, "Code"),
            ErrorKind::ReceiptHandleIsInvalid.json_type()
        );
        assert!(!field(failed, "Message").is_empty());
        assert_eq!(failed["SenderFault"], json!(true));
    }

    /// …and a RECEIVER fault inside a batch says so, which is the difference
    /// between "fix your request" and "send it again".
    #[tokio::test]
    async fn a_batch_entry_that_failed_on_queen_is_not_the_senders_fault() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        rig.fake
            .fail_ack(queen::Error::Transport("connection refused".into()));

        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [{
                    "Id": "terminate",
                    "ReceiptHandle": field(&message, "ReceiptHandle"),
                    "VisibilityTimeout": 0
                }]}),
            ),
        )
        .await
        .unwrap();
        let failed = &answer["Failed"][0];
        assert_eq!(field(failed, "Id"), "terminate");
        assert_eq!(
            field(failed, "Code"),
            ErrorKind::ServiceUnavailable.json_type()
        );
        assert_eq!(failed["SenderFault"], json!(false));
    }

    /// A per-entry value outside its range is that ENTRY's failure and not the
    /// request's: nine good changes are not lost to one bad number.
    #[tokio::test]
    async fn a_visibility_batch_entry_out_of_range_is_a_per_entry_failure() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 1);
        let message = rig.receive_one("orders").await;
        let handle = field(&message, "ReceiptHandle").to_string();

        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "over", "ReceiptHandle": handle, "VisibilityTimeout": 43_201},
                    {"Id": "under", "ReceiptHandle": handle, "VisibilityTimeout": -1},
                    {"Id": "fine", "ReceiptHandle": handle, "VisibilityTimeout": 60}
                ]}),
            ),
        )
        .await
        .unwrap();
        let failed = answer["Failed"].as_array().unwrap();
        assert_eq!(failed.len(), 2);
        for entry in failed {
            assert_eq!(
                field(entry, "Code"),
                ErrorKind::InvalidParameterValue.json_type()
            );
        }
        assert_eq!(answer["Successful"].as_array().unwrap().len(), 1);
        assert_eq!(rig.fake.extends.lock().unwrap().len(), 1);
    }

    /// An entry that names no timeout takes the QUEUE's, not the protocol's
    /// default: the queue's own attribute is what a client that set it expects
    /// every visibility change to inherit.
    #[tokio::test]
    async fn a_visibility_batch_entry_without_a_timeout_takes_the_queues_default() {
        let rig = Rig::new(&[("orders", &[("VisibilityTimeout", "45")])]).await;
        rig.seed_lanes("orders", 1);
        let message = rig.receive_one("orders").await;

        change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "default", "ReceiptHandle": field(&message, "ReceiptHandle")}
                ]}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(rig.fake.extends.lock().unwrap()[0].1, 45);
    }

    #[tokio::test]
    async fn a_visibility_batch_of_only_bad_handles_calls_nothing() {
        let rig = Rig::standard().await;
        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [{"Id": "a", "ReceiptHandle": "forged"}]}),
            ),
        )
        .await
        .unwrap();
        assert!(answer.get("Successful").is_none());
        assert_eq!(answer["Failed"].as_array().unwrap().len(), 1);
        assert!(rig.fake.extends.lock().unwrap().is_empty());
        assert!(rig.fake.acks.lock().unwrap().is_empty());
    }

    /// The visibility batch reads the queue's record, so a queue that is not
    /// there is the same refusal every other action gives — before any entry is
    /// acted on.
    #[tokio::test]
    async fn a_visibility_batch_on_a_queue_that_is_not_there_is_refused_whole() {
        let rig = Rig::standard().await;
        let error = change_message_visibility_batch(
            &rig.ctx,
            &json!({"QueueUrl": rig.url("never-created"),
                    "Entries": [{"Id": "a", "ReceiptHandle": "h", "VisibilityTimeout": 30}]}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::QueueDoesNotExist);
        assert!(rig.fake.extends.lock().unwrap().is_empty());
    }

    /// The whole batch is refused, not the entry that tipped it over: AWS
    /// applies one message's maximum to the batch's total.
    #[tokio::test]
    async fn a_batch_over_the_total_size_cap_is_refused_whole() {
        let rig = Rig::new(&[("orders", &[("MaximumMessageSize", "1024")])]).await;
        let entries: Vec<Value> = (0..3)
            .map(|i| json!({"Id": format!("id{i}"), "MessageBody": "x".repeat(400)}))
            .collect();
        let error =
            send_message_batch(&rig.ctx, &rig.params("orders", json!({"Entries": entries})))
                .await
                .unwrap_err();
        assert_eq!(error.kind, ErrorKind::BatchRequestTooLong);
        assert!(rig.fake.pushed().is_empty());

        // A batch of ONE that is too long is still a batch: the code is what an
        // SDK's batching helper reads, and it must not change with the count.
        let error = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [{"Id": "one", "MessageBody": "x".repeat(1025)}]}),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::BatchRequestTooLong);
    }

    /// A batch may mix the two routes, and each entry still answers under its
    /// own id — the alignment that a wrong answer would silently break.
    #[tokio::test]
    async fn a_batch_mixes_delayed_and_immediate_entries() {
        let rig = Rig::standard().await;
        let answer = send_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "now", "MessageBody": "now"},
                    {"Id": "later", "MessageBody": "later", "DelaySeconds": 30},
                    {"Id": "also-now", "MessageBody": "also-now"}
                ]}),
            ),
        )
        .await
        .unwrap();
        let successful = answer["Successful"].as_array().unwrap();
        let ids: Vec<&str> = successful.iter().map(|e| field(e, "Id")).collect();
        assert_eq!(ids, ["now", "later", "also-now"]);
        assert_eq!(rig.fake.pushed().len(), 2);
        assert_eq!(rig.fake.timer_calls.lock().unwrap()[0].len(), 1);
        // Each entry's digest is its OWN body's.
        assert_eq!(
            field(&successful[1], "MD5OfMessageBody"),
            md5::body_md5("later")
        );
    }

    // ------------------------------------------------------------ receiving

    /// The `exact` mode, asserted on the wire: N pops, each claiming ONE lane
    /// and taking ONE message, which is the claim width the whole module rests
    /// on.
    #[tokio::test]
    async fn a_receive_is_n_single_message_pops() {
        let rig = Rig::standard().await;
        // One message per lane, so the claim width and not the lane layout is
        // what this measures.
        rig.seed_lanes("orders", 5);
        let messages = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 5}))
            .await;
        assert_eq!(messages.len(), 5);

        let pops = rig.fake.pops.lock().unwrap().clone();
        assert_eq!(pops.len(), 5);
        for (queue, partition, options) in &pops {
            assert_eq!(queue, "orders");
            assert_eq!(*partition, None, "exact mode never pins a lane");
            assert_eq!(options.batch, 1);
            assert_eq!(options.partitions, 1);
            assert_eq!(options.group(), QUEUE_MODE_GROUP);
            assert!(!options.wait);
        }
    }

    /// The bound a synthesized standard queue really has, pinned rather than
    /// hidden: a lane is claimed whole, so two messages that hashed to the same
    /// lane are delivered ONE AT A TIME and the second only after the first is
    /// out of the way. It is the cost of building an SQS queue out of a log —
    /// invisible at the default width against a real backlog, and the reason the
    /// width is an attribute rather than a constant.
    #[tokio::test]
    async fn two_messages_on_one_lane_are_delivered_one_at_a_time() {
        let rig = Rig::standard().await;
        rig.fake.seed(
            "orders",
            "0",
            0,
            &[json!({"b": "first"}), json!({"b": "second"})],
        );

        let messages = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 10}))
            .await;
        assert_eq!(messages.len(), 1);
        assert_eq!(field(&messages[0], "Body"), "first");

        rig.delete("orders", field(&messages[0], "ReceiptHandle"))
            .await
            .unwrap();
        let messages = rig.receive_list("orders", json!({})).await;
        assert_eq!(field(&messages[0], "Body"), "second");
    }

    /// Fewer messages than asked for is not an error, and an EMPTY receive omits
    /// the member entirely — which is what every client's `if 'Messages' in
    /// response` is written against.
    #[tokio::test]
    async fn a_short_receive_answers_what_there_was() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "only one"}))
            .await
            .unwrap();
        let messages = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 10}))
            .await;
        assert_eq!(messages.len(), 1);

        let empty = rig
            .receive("orders", json!({"MaxNumberOfMessages": 10}))
            .await
            .unwrap();
        assert_eq!(empty, json!({}), "an empty receive rendered a member");
    }

    /// A RECEIVE'S POPS RUN CONCURRENTLY, AND EACH ONE THAT ANSWERED HAS
    /// ALREADY CLAIMED. Returning the first error would drop those messages on
    /// the floor with no receipt handle ever issued: invisible for a full
    /// visibility timeout, charged a delivery nobody saw, and — once M3's
    /// RedrivePolicy is live — walked toward a dead-letter queue they reach
    /// without a single consumer having read them.
    #[tokio::test]
    async fn a_failed_pop_does_not_discard_what_its_siblings_claimed() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 3);
        // One of the three pops fails; the double takes the scripted error on
        // the first call that reaches it.
        rig.fake
            .fail_pop(crate::queen::Error::Transport("connection reset".into()));

        let messages = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 3}))
            .await;
        assert_eq!(messages.len(), 2, "a short receive is legal SQS");
        // The third message is still THERE and still visible: the failing pop
        // claimed nothing, and nothing else was thrown away with it.
        let rest = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 3}))
            .await;
        assert_eq!(rest.len(), 1);
        let mut seen: Vec<&str> = messages
            .iter()
            .chain(rest.iter())
            .map(|m| field(m, "Body"))
            .collect();
        seen.sort_unstable();
        assert_eq!(seen, vec!["m0", "m1", "m2"], "every message was delivered");
    }

    /// ...and when there is nothing to answer with, the failure IS the answer.
    #[tokio::test]
    async fn a_receive_whose_only_pop_failed_is_a_failure() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 1);
        rig.fake
            .fail_pop(crate::queen::Error::Transport("connection reset".into()));
        let error = rig
            .receive("orders", json!({"MaxNumberOfMessages": 1}))
            .await
            .expect_err("nothing was claimed and nothing was answered");
        assert_eq!(error.kind.fault(), crate::error::Fault::Receiver);
    }

    #[tokio::test]
    async fn the_receive_ceilings_are_the_protocols_own() {
        let rig = Rig::standard().await;
        for (name, value) in [
            ("MaxNumberOfMessages", 0),
            ("MaxNumberOfMessages", 11),
            ("WaitTimeSeconds", 21),
            ("WaitTimeSeconds", -1),
            ("VisibilityTimeout", 43_201),
        ] {
            let error = rig
                .receive("orders", json!({ name: value }))
                .await
                .unwrap_err();
            assert_eq!(
                error.kind,
                ErrorKind::InvalidParameterValue,
                "{name}={value}"
            );
        }
    }

    /// The long poll is ONE parked pop. Ten parked pops on an idle queue would
    /// hold ten claims for twenty seconds to deliver nothing.
    #[tokio::test]
    async fn a_long_poll_parks_exactly_once() {
        let rig = Rig::standard().await;
        assert!(rig
            .receive_list(
                "orders",
                json!({"MaxNumberOfMessages": 10, "WaitTimeSeconds": 20})
            )
            .await
            .is_empty());
        let pops = rig.fake.pops.lock().unwrap().clone();
        assert_eq!(pops.len(), 1, "an empty long poll asked more than once");
        assert!(pops[0].2.wait);
        assert_eq!(pops[0].2.timeout_ms, 20_000);
    }

    /// …and once the parked pop has found something, the rest are asked for
    /// without parking, so a full batch costs one window and not ten.
    #[tokio::test]
    async fn a_long_poll_that_finds_something_fills_the_batch_without_parking() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 3);
        let messages = rig
            .receive_list(
                "orders",
                json!({"MaxNumberOfMessages": 3, "WaitTimeSeconds": 20}),
            )
            .await;
        assert_eq!(messages.len(), 3);
        let pops = rig.fake.pops.lock().unwrap().clone();
        assert_eq!(pops.len(), 3);
        assert!(pops[0].2.wait, "the first pop did not park");
        assert!(!pops[1].2.wait && !pops[2].2.wait, "an extra pop parked");
    }

    /// The queue's own `ReceiveMessageWaitTimeSeconds` is the default when the
    /// request names none — which is how a queue is made long-polling for every
    /// client at once.
    #[tokio::test]
    async fn the_queues_wait_time_is_the_default() {
        let rig = Rig::new(&[("orders", &[("ReceiveMessageWaitTimeSeconds", "5")])]).await;
        rig.receive("orders", json!({})).await.unwrap();
        let pops = rig.fake.pops.lock().unwrap().clone();
        assert!(pops[0].2.wait);
        assert_eq!(pops[0].2.timeout_ms, 5_000);
    }

    /// A parameter that is PRESENT decides, at any legal value: a client that
    /// asks for `WaitTimeSeconds=0` against a long-polling queue is asking for a
    /// short poll, and inheriting the queue's twenty seconds there would park a
    /// receive the client wrote to return at once.
    #[tokio::test]
    async fn a_receive_that_names_zero_wait_does_not_take_the_queues_default() {
        let rig = Rig::new(&[("orders", &[("ReceiveMessageWaitTimeSeconds", "20")])]).await;
        rig.receive("orders", json!({"WaitTimeSeconds": 0}))
            .await
            .unwrap();
        let pops = rig.fake.pops.lock().unwrap().clone();
        assert!(!pops[0].2.wait, "an explicit zero parked anyway");
        assert_eq!(pops[0].2.timeout_ms, 0);
    }

    /// …and the same for the visibility timeout, where zero is a documented
    /// divergence of its own but must still be the value the request NAMED
    /// rather than the queue's.
    #[tokio::test]
    async fn a_receive_that_names_zero_visibility_does_not_take_the_queues_default() {
        let rig = Rig::new(&[("orders", &[("VisibilityTimeout", "45")])]).await;
        rig.receive("orders", json!({"VisibilityTimeout": 0}))
            .await
            .unwrap();
        assert_eq!(rig.fake.pops.lock().unwrap()[0].2.lease_seconds, 0);
    }

    /// The shape of a receive that names nothing at all: one message, no
    /// parking, and the protocol's own visibility default — which is what every
    /// `receive_message()` with no arguments in every SDK sends.
    #[tokio::test]
    async fn the_receive_defaults_are_one_message_no_parking_and_thirty_seconds() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 3);
        let messages = rig.receive_list("orders", json!({})).await;
        assert_eq!(messages.len(), 1, "a receive that asked for one got more");

        let pops = rig.fake.pops.lock().unwrap().clone();
        assert_eq!(pops.len(), 1);
        assert!(!pops[0].2.wait);
        assert_eq!(pops[0].2.batch, 1);
        assert_eq!(pops[0].2.partitions, 1);
        assert_eq!(
            pops[0].2.lease_seconds, DEFAULT_VISIBILITY_SECONDS as i32,
            "the queue set none, so SQS's own default applies"
        );
    }

    /// The refusal names the parameter AND the value, which is AWS's own shape
    /// and the only form a client can act on: the fix is a constant in their
    /// code, and the message has to say which one.
    #[tokio::test]
    async fn the_receive_ceilings_name_the_parameter_and_the_value() {
        let rig = Rig::standard().await;
        for (name, value, bounds) in [
            ("MaxNumberOfMessages", 11, "1 to 10"),
            ("WaitTimeSeconds", 21, "0 to 20"),
            ("VisibilityTimeout", 43_201, "0 to 43200"),
        ] {
            let error = rig
                .receive("orders", json!({ name: value }))
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
            assert!(
                error
                    .message
                    .starts_with(&format!("Value {value} for parameter {name} is invalid.")),
                "{}",
                error.message
            );
            assert!(error.message.contains(bounds), "{}", error.message);
        }
        assert!(rig.fake.pops.lock().unwrap().is_empty());
    }

    /// DIVERGENCE, `accepted`, pinned so the catalog's gap is a decision and not
    /// a drift: AWS answers `AWS.SimpleQueueService.ReadCountOutOfRange` for a
    /// `MaxNumberOfMessages` outside 1..10 and this answers
    /// `InvalidParameterValue`. Both are 400 Sender faults naming the parameter,
    /// the value and the range; the code is a reviewed event
    /// ([`crate::error`]), and this one is recorded rather than invented because
    /// its JSON 1.0 `__type` cannot be derived from the Query spelling.
    #[tokio::test]
    async fn a_read_count_out_of_range_is_an_invalid_parameter_here() {
        let rig = Rig::standard().await;
        for value in [0, 11] {
            let error = rig
                .receive("orders", json!({"MaxNumberOfMessages": value}))
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
            assert_eq!(error.kind.http_status(), 400);
            assert_eq!(error.kind.fault(), crate::error::Fault::Sender);
            assert!(
                !crate::error::ErrorKind::ALL
                    .iter()
                    .any(|kind| kind.query_code().contains("ReadCountOutOfRange")),
                "the catalog is the contract: adding the code is a reviewed change"
            );
        }
    }

    /// A stored default outside the range a REQUEST would have been held to is
    /// clamped rather than obeyed: SQS's ceilings are what every client's socket
    /// timeout is written against, and a record from another version of this
    /// facade must not make a receive park for five minutes.
    #[test]
    fn a_stored_queue_default_is_clamped_into_the_protocols_range() {
        let mut record = QueueRecord::default();
        for (name, value) in [
            ("ReceiveMessageWaitTimeSeconds", "300"),
            ("VisibilityTimeout", "-5"),
            ("DelaySeconds", "not a number"),
        ] {
            record
                .attributes
                .insert(name.to_string(), value.to_string());
        }
        assert_eq!(
            queue_default(&record, "ReceiveMessageWaitTimeSeconds", 0, 20),
            Some(20)
        );
        assert_eq!(
            queue_default(&record, "VisibilityTimeout", 0, 43_200),
            Some(0)
        );
        // Unreadable is ABSENT, so the caller's own default applies rather than
        // a limit no client can explain.
        assert_eq!(queue_default(&record, "DelaySeconds", 0, 900), None);
        assert_eq!(queue_default(&record, "NeverSet", 0, 900), None);
    }

    /// The visibility override IS the lease's length, which is the mapping that
    /// makes every other visibility verb exact.
    #[tokio::test]
    async fn the_visibility_override_is_the_leases_own_length() {
        let rig = Rig::new(&[("orders", &[("VisibilityTimeout", "45")])]).await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();

        rig.receive("orders", json!({"VisibilityTimeout": 120}))
            .await
            .unwrap();
        assert_eq!(rig.fake.pops.lock().unwrap()[0].2.lease_seconds, 120);

        rig.fake.advance(Duration::from_secs(121));
        rig.receive("orders", json!({})).await.unwrap();
        // …and with no override it is the queue's own attribute.
        assert_eq!(rig.fake.pops.lock().unwrap()[1].2.lease_seconds, 45);
    }

    /// DIVERGENCE, `deliberate`, pinned here so it is a decision and not a
    /// drift: a receive that asks for `VisibilityTimeout: 0` gets the queue's
    /// default rather than AWS's "immediately visible to other consumers".
    ///
    /// Zero is also the broker's "use the queue's lease time", so the two
    /// meanings collide on one number; honouring AWS's would mean a second write
    /// transaction per message AND a released lease, which would make the
    /// `DeleteMessage` the client is entitled to issue against that handle stop
    /// deleting while still answering success. See [`pop_exact`].
    #[tokio::test]
    async fn a_zero_visibility_receive_takes_the_queues_default() {
        let rig = Rig::new(&[("orders", &[("VisibilityTimeout", "45")])]).await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig
            .receive_list("orders", json!({"VisibilityTimeout": 0}))
            .await
            .remove(0);
        assert_eq!(rig.fake.pops.lock().unwrap()[0].2.lease_seconds, 0);
        // The message is held, not released — and the handle still deletes,
        // which is the property the divergence buys.
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
        rig.delete("orders", field(&message, "ReceiptHandle"))
            .await
            .expect("the handle from a zero-visibility receive still deletes");
    }

    /// Nothing is answered that was not asked for: SQS returns no attributes by
    /// default, and `All` returns the ones this facade can actually derive.
    #[tokio::test]
    async fn system_attributes_are_answered_only_when_asked_for() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();

        let message = rig.receive_one("orders").await;
        assert!(message.get("Attributes").is_none(), "unasked attributes");
        rig.fake.advance(Duration::from_secs(31));

        let mut messages = rig
            .receive_list("orders", json!({"AttributeNames": ["All"]}))
            .await;
        let message = messages.remove(0);
        assert_eq!(
            attribute(&message, "ApproximateReceiveCount").as_deref(),
            Some("2")
        );
        assert!(attribute(&message, "SentTimestamp").is_some());
        // A standard queue has no group and no dedup id to answer.
        assert!(attribute(&message, "MessageGroupId").is_none());
    }

    /// The newer spelling selects the same set, and one name selects one
    /// attribute: a client that asked for the count does not get the timestamp.
    #[tokio::test]
    async fn either_spelling_selects_system_attributes_by_name() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let mut messages = rig
            .receive_list(
                "orders",
                json!({"MessageSystemAttributeNames": ["ApproximateReceiveCount"]}),
            )
            .await;
        let message = messages.remove(0);
        assert_eq!(
            attribute(&message, "ApproximateReceiveCount").as_deref(),
            Some("1")
        );
        assert!(attribute(&message, "SentTimestamp").is_none());
    }

    /// `SentTimestamp` is the broker's own `createdAt`, in the milliseconds AWS
    /// answers — so this pins the conversion and not just its presence.
    #[tokio::test]
    async fn sent_timestamp_is_the_brokers_created_at_in_milliseconds() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let mut messages = rig
            .receive_list("orders", json!({"AttributeNames": ["SentTimestamp"]}))
            .await;
        let message = messages.remove(0);
        assert_eq!(
            attribute(&message, "SentTimestamp").as_deref(),
            Some(TestClock::EPOCH_MS.to_string().as_str())
        );
    }

    /// The parser and the renderer are inverses across the boundaries that
    /// break naive calendar arithmetic, and unreadable input is `None` rather
    /// than a plausible wrong instant.
    #[test]
    fn the_broker_timestamp_parser_is_the_renderers_inverse() {
        for ms in [
            0,
            TestClock::EPOCH_MS,
            951_782_400_000,   // 2000-02-29, the leap century
            4_107_542_400_000, // 2100-03-01, the one that is not
            1_709_164_800_123,
        ] {
            assert_eq!(epoch_ms_of(&iso_from_epoch_ms(ms)), Some(ms), "{ms}");
        }
        // Truncated to milliseconds, which is what SentTimestamp is.
        assert_eq!(
            epoch_ms_of("2026-08-30T00:00:00.123456Z"),
            Some(1_788_048_000_123)
        );
        assert_eq!(
            epoch_ms_of("2026-08-30T00:00:00.5Z"),
            Some(1_788_048_000_500)
        );
        assert_eq!(epoch_ms_of("2026-08-30T00:00:00Z"), Some(1_788_048_000_000));
        for bad in [
            "",
            "not a date",
            "2026-08-30",
            "2026-13-01T00:00:00Z",
            "2026-08-30T25:00:00Z",
        ] {
            assert_eq!(epoch_ms_of(bad), None, "{bad:?}");
        }
    }

    /// Message attributes are filtered by name and by prefix, and — the part
    /// that matters — the digest is over what was RETURNED, because that is what
    /// the SDK recomputes.
    #[tokio::test]
    async fn message_attributes_are_filtered_and_digested_as_returned() {
        let rig = Rig::standard().await;
        rig.send(
            "orders",
            json!({
                "MessageBody": "x",
                "MessageAttributes": {
                    "keep": {"DataType": "String", "StringValue": "1"},
                    "trace.id": {"DataType": "String", "StringValue": "2"},
                    "trace.span": {"DataType": "String", "StringValue": "3"},
                    "drop": {"DataType": "String", "StringValue": "4"},
                    // The prefix's dot is part of the prefix: `trace.*` must not
                    // reach this one.
                    "traceroute": {"DataType": "String", "StringValue": "5"}
                }
            }),
        )
        .await
        .unwrap();

        let mut messages = rig
            .receive_list(
                "orders",
                json!({"MessageAttributeNames": ["keep", "trace.*"]}),
            )
            .await;
        let message = messages.remove(0);
        let attributes = message["MessageAttributes"].as_object().unwrap();
        assert_eq!(attributes.len(), 3);
        assert!(attributes.contains_key("keep"));
        assert!(attributes.contains_key("trace.id"));
        assert!(!attributes.contains_key("drop"));
        assert!(!attributes.contains_key("traceroute"));

        let mut returned = BTreeMap::new();
        for (name, value) in [("keep", "1"), ("trace.id", "2"), ("trace.span", "3")] {
            returned.insert(name.to_string(), MessageAttribute::string("String", value));
        }
        assert_eq!(
            field(&message, "MD5OfMessageAttributes"),
            md5::attributes_md5(&returned).unwrap()
        );
    }

    #[tokio::test]
    async fn message_attributes_are_absent_unless_asked_for() {
        let rig = Rig::standard().await;
        rig.send(
            "orders",
            json!({
                "MessageBody": "x",
                "MessageAttributes": {"a": {"DataType": "String", "StringValue": "1"}}
            }),
        )
        .await
        .unwrap();
        let message = rig.receive_one("orders").await;
        assert!(message.get("MessageAttributes").is_none());
        assert!(message.get("MD5OfMessageAttributes").is_none());
    }

    /// A payload this facade did not write is served as its own text, so a
    /// native Queen producer's messages are readable through an SQS consumer.
    #[tokio::test]
    async fn a_native_producers_message_is_readable() {
        let rig = Rig::standard().await;
        rig.fake
            .seed("orders", "3", 0, &[json!({"order": 42, "total": 10})]);
        let message = rig.receive_one("orders").await;
        assert_eq!(field(&message, "Body"), r#"{"order":42,"total":10}"#);
        assert_eq!(
            field(&message, "MD5OfBody"),
            md5::body_md5(r#"{"order":42,"total":10}"#)
        );
        assert!(message.get("MessageAttributes").is_none());
    }

    /// THE DIGEST DESCRIBES THE BYTES THAT SHIP. A native producer's payload
    /// can carry a character XML 1.0 cannot represent — `SendMessage` refuses
    /// the charset, a native push never passed through it — and the Query
    /// protocol substitutes U+FFFD for it on the way into the document. Every
    /// AWS SDK recomputes `MD5OfBody` from the body it received and throws on a
    /// mismatch, so the substitution has to happen before the digest and not
    /// inside the writer.
    #[tokio::test]
    async fn a_body_xml_cannot_carry_is_digested_as_it_will_be_written() {
        let rig = Rig::standard().await;
        rig.fake
            .seed("orders", "3", 0, &[json!({"b": "nul\u{0}byte"})]);
        let message = rig
            .receive_list("orders", json!({"MessageAttributeNames": ["All"]}))
            .await
            .remove(0);
        assert_eq!(field(&message, "Body"), "nul\u{FFFD}byte");
        assert_eq!(
            field(&message, "MD5OfBody"),
            md5::body_md5("nul\u{FFFD}byte"),
            "the digest is over the substituted body, not the stored one"
        );
        // The XML rendering therefore writes exactly what the digest covers.
        let rendered = crate::proto::render_ok(
            crate::proto::Protocol::Query,
            "ReceiveMessage",
            "rid",
            json!({"Messages": [message.clone()]}),
        );
        assert!(
            rendered.body.contains("<Body>nul\u{FFFD}byte</Body>"),
            "{}",
            rendered.body
        );

        // The same holds for an attribute, whose own digest is over the subset
        // that was returned.
        rig.fake.seed(
            "orders",
            "4",
            0,
            &[json!({"b": "x", "a": {"trace": {"t": "String", "v": "a\u{1}b"}}})],
        );
        let message = rig
            .receive_list("orders", json!({"MessageAttributeNames": ["All"]}))
            .await
            .into_iter()
            .find(|m| field(m, "Body") == "x")
            .expect("the second message");
        assert_eq!(
            message["MessageAttributes"]["trace"]["StringValue"],
            json!("a\u{FFFD}b")
        );
        let mut expected = BTreeMap::new();
        expected.insert(
            "trace".to_string(),
            MessageAttribute::string("String", "a\u{FFFD}b"),
        );
        assert_eq!(
            field(&message, "MD5OfMessageAttributes"),
            md5::attributes_md5(&expected).unwrap()
        );
    }

    /// The visibility timeout is a TIMEOUT and not a lock: when it lapses the
    /// message comes back, with the count incremented, which is the field every
    /// redrive policy is written against.
    #[tokio::test]
    async fn the_receive_count_increments_when_the_visibility_lapses() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();

        for expected in ["1", "2", "3"] {
            let mut messages = rig
                .receive_list("orders", json!({"AttributeNames": ["All"]}))
                .await;
            assert_eq!(messages.len(), 1, "delivery {expected} did not arrive");
            let message = messages.remove(0);
            assert_eq!(
                attribute(&message, "ApproximateReceiveCount").as_deref(),
                Some(expected)
            );
            // Still invisible while the lease is live.
            assert!(rig.receive_list("orders", json!({})).await.is_empty());
            rig.fake.advance(Duration::from_secs(31));
        }
    }

    // ------------------------------------------------------------- receipts

    /// The handle is opaque to the client and complete for the facade: it names
    /// the delivery, not the message, and it is what makes a delete servable by
    /// an instance that never saw the receive.
    #[tokio::test]
    async fn the_receipt_handle_names_this_delivery() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        let receipt = rig
            .ctx
            .facade
            .handles
            .decode(field(&message, "ReceiptHandle"), now_ms())
            .expect("this facade minted it");
        assert_eq!(receipt.queue, "orders");
        assert_eq!(
            receipt.message_id,
            field(&message, "MessageId"),
            "the uuid a FIFO delete-set records the delete against"
        );
        assert_eq!(
            receipt.partition_id,
            rig.fake.partition_id("orders", &receipt_lane(&rig))
        );
        // Long enough to survive every legal ChangeMessageVisibility.
        assert!(
            receipt.expires_at_ms >= now_ms() + i64::from(MAX_VISIBILITY_SECONDS) * 1_000 - 1_000
        );
    }

    /// The lane the single seeded message went to, read off the push.
    fn receipt_lane(rig: &Rig) -> String {
        rig.fake.pushed()[0].partition.clone()
    }

    /// A handle this facade did not mint for THIS queue is refused, and every
    /// refusal is the same one: telling a forger which half failed is telling
    /// them which half to work on.
    #[tokio::test]
    async fn a_forged_or_foreign_handle_is_refused() {
        let rig = Rig::new(&[("orders", &[]), ("other", &[])]).await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        let handle = field(&message, "ReceiptHandle").to_string();

        for bad in ["", "not-a-handle", &format!("{handle}x")] {
            let error = rig.delete("orders", bad).await.unwrap_err();
            assert_eq!(error.kind, ErrorKind::ReceiptHandleIsInvalid, "{bad:?}");
        }
        // A real handle, presented against another queue: the tag verifies and
        // the queue field is what refuses it.
        let error = rig.delete("other", &handle).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::ReceiptHandleIsInvalid);
        // …and the message is still there.
        assert!(rig
            .fake
            .leased("orders", &receipt_lane(&rig), QUEUE_MODE_GROUP));
    }

    #[tokio::test]
    async fn a_delete_without_a_handle_is_a_missing_parameter() {
        let rig = Rig::standard().await;
        let error = delete_message(&rig.ctx, &rig.params("orders", json!({})))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingParameter);
    }

    // ------------------------------------------------------------- deleting

    /// AWS's own contract, quoted in the module header: a stale handle answers
    /// success. A client that deletes twice — every at-least-once consumer that
    /// crashed mid-ack — must not see an error for it.
    #[tokio::test]
    async fn a_double_delete_answers_success() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        let handle = field(&message, "ReceiptHandle").to_string();

        assert_eq!(rig.delete("orders", &handle).await.unwrap(), Value::Null);
        // The broker refuses the second one — the lease is gone — and the client
        // is told success anyway.
        assert_eq!(rig.delete("orders", &handle).await.unwrap(), Value::Null);
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
    }

    /// A handle from a PREVIOUS delivery names a lease that is gone. AWS answers
    /// success and does not delete, and so does this: what must not happen is
    /// the stale handle deleting the delivery that is in flight NOW.
    #[tokio::test]
    async fn a_stale_handle_does_not_delete_the_current_delivery() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let first = rig.receive_one("orders").await;
        let stale = field(&first, "ReceiptHandle").to_string();

        rig.fake.advance(Duration::from_secs(31));
        let second = rig.receive_one("orders").await;
        assert_ne!(field(&second, "ReceiptHandle"), stale);

        rig.delete("orders", &stale).await.expect("success");
        // The message is still leased under the SECOND delivery, and deleting
        // with that handle does remove it.
        rig.delete("orders", field(&second, "ReceiptHandle"))
            .await
            .unwrap();
        rig.fake.advance(Duration::from_secs(31));
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
    }

    #[tokio::test]
    async fn a_delete_batch_answers_per_entry() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 3);
        let messages = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 3}))
            .await;
        assert_eq!(messages.len(), 3);

        let entries: Vec<Value> = messages
            .iter()
            .enumerate()
            .map(
                |(i, m)| json!({"Id": format!("e{i}"), "ReceiptHandle": field(m, "ReceiptHandle")}),
            )
            .chain([json!({"Id": "bad", "ReceiptHandle": "forged"})])
            .collect();
        let answer = delete_message_batch(
            &rig.ctx,
            &rig.params("orders", json!({ "Entries": entries })),
        )
        .await
        .unwrap();

        let successful = answer["Successful"].as_array().unwrap();
        assert_eq!(successful.len(), 3);
        assert_eq!(field(&successful[0], "Id"), "e0");
        let failed = answer["Failed"].as_array().unwrap();
        assert_eq!(failed.len(), 1);
        assert_eq!(field(&failed[0], "Id"), "bad");
        assert_eq!(field(&failed[0], "Code"), "ReceiptHandleIsInvalid");
        // ONE ack call for the three good entries, and the bad one never
        // reached it.
        assert_eq!(rig.fake.acks.lock().unwrap().len(), 1);
        assert_eq!(rig.fake.acks.lock().unwrap()[0].len(), 3);
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
    }

    /// Every entry bad is still a well-formed answer, and it costs no broker
    /// call at all.
    #[tokio::test]
    async fn a_delete_batch_of_only_bad_handles_calls_nothing() {
        let rig = Rig::standard().await;
        let answer = delete_message_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [{"Id": "a", "ReceiptHandle": "forged"}]}),
            ),
        )
        .await
        .unwrap();
        assert!(answer.get("Successful").is_none());
        assert_eq!(answer["Failed"].as_array().unwrap().len(), 1);
        assert!(rig.fake.acks.lock().unwrap().is_empty());
    }

    // ----------------------------------------------------------- visibility

    /// A visibility of zero is a TERMINATE: the message comes back immediately
    /// and the retry budget is not charged, which is the difference between
    /// giving a message back and failing it.
    #[tokio::test]
    async fn a_zero_visibility_returns_the_message_without_charging_it() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        let lane = receipt_lane(&rig);

        change_message_visibility(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({
                    "ReceiptHandle": field(&message, "ReceiptHandle"),
                    "VisibilityTimeout": 0
                }),
            ),
        )
        .await
        .expect("the terminate is accepted");

        assert!(!rig.fake.leased("orders", &lane, QUEUE_MODE_GROUP));
        assert_eq!(rig.fake.retries("orders", &lane, QUEUE_MODE_GROUP), 0);
        // Immediately visible again, with no clock movement at all.
        assert_eq!(rig.receive_list("orders", json!({})).await.len(), 1);
    }

    /// A non-zero visibility extends the lease, and the extension is what
    /// decides whether the message comes back.
    #[tokio::test]
    async fn a_non_zero_visibility_extends_the_lease() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;

        change_message_visibility(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({
                    "ReceiptHandle": field(&message, "ReceiptHandle"),
                    "VisibilityTimeout": 300
                }),
            ),
        )
        .await
        .unwrap();
        assert_eq!(rig.fake.extends.lock().unwrap().len(), 1);
        assert_eq!(rig.fake.extends.lock().unwrap()[0].1, 300);

        // Past the original thirty seconds, inside the extension.
        rig.fake.advance(Duration::from_secs(60));
        assert!(rig.receive_list("orders", json!({})).await.is_empty());
        // …and the handle still deletes it, which is why a handle outlives its
        // own delivery's window.
        rig.delete("orders", field(&message, "ReceiptHandle"))
            .await
            .unwrap();
    }

    /// A lease that is gone cannot be extended, and AWS has a code for exactly
    /// that. A terminate of the same message is a SUCCESS: the caller wanted it
    /// visible and it is.
    #[tokio::test]
    async fn extending_a_dead_lease_is_message_not_inflight() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        let handle = field(&message, "ReceiptHandle").to_string();
        rig.fake.advance(Duration::from_secs(31));

        let error = change_message_visibility(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"ReceiptHandle": handle, "VisibilityTimeout": 60}),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MessageNotInflight);

        // ...and so is a TERMINATE of the same dead lease. AWS's contract is
        // `MessageNotInflight` for both, and the two arms of `revisibilize` are
        // two broker calls and not two contracts: answering success here would
        // tell a client its message is visible again while another consumer
        // holds it.
        let error = change_message_visibility(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"ReceiptHandle": handle, "VisibilityTimeout": 0}),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MessageNotInflight);
    }

    #[tokio::test]
    async fn a_visibility_change_needs_its_timeout() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        let error = change_message_visibility(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"ReceiptHandle": field(&message, "ReceiptHandle")}),
            ),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingParameter);
    }

    /// The batch mixes both verbs and both outcomes, each reported under its own
    /// id — and the entries that name no timeout take the queue's own.
    #[tokio::test]
    async fn a_visibility_batch_answers_per_entry() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 2);
        let messages = rig
            .receive_list("orders", json!({"MaxNumberOfMessages": 2}))
            .await;
        assert_eq!(messages.len(), 2);
        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [
                    {"Id": "extend", "ReceiptHandle": field(&messages[0], "ReceiptHandle"),
                     "VisibilityTimeout": 300},
                    {"Id": "terminate", "ReceiptHandle": field(&messages[1], "ReceiptHandle"),
                     "VisibilityTimeout": 0},
                    {"Id": "default", "ReceiptHandle": field(&messages[0], "ReceiptHandle")},
                    {"Id": "forged", "ReceiptHandle": "nope"}
                ]}),
            ),
        )
        .await
        .unwrap();

        let successful = answer["Successful"].as_array().unwrap();
        let ids: Vec<&str> = successful.iter().map(|e| field(e, "Id")).collect();
        assert_eq!(ids, ["extend", "terminate", "default"]);
        let failed = answer["Failed"].as_array().unwrap();
        assert_eq!(field(&failed[0], "Id"), "forged");
        assert_eq!(field(&failed[0], "Code"), "ReceiptHandleIsInvalid");
        // The entry with no timeout of its own took the queue's default.
        let extends = rig.fake.extends.lock().unwrap().clone();
        assert_eq!(extends.len(), 2);
        assert!(extends.iter().any(|(_, seconds)| *seconds == 300));
        assert!(extends
            .iter()
            .any(|(_, seconds)| *seconds == DEFAULT_VISIBILITY_SECONDS));
    }

    /// A per-entry broker refusal is a `BatchResultErrorEntry` and not a failed
    /// request: one dead lease must not lose the nine entries around it.
    #[tokio::test]
    async fn a_visibility_batch_reports_a_dead_lease_per_entry() {
        let rig = Rig::standard().await;
        rig.send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap();
        let message = rig.receive_one("orders").await;
        rig.fake.advance(Duration::from_secs(31));

        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Entries": [{
                    "Id": "gone",
                    "ReceiptHandle": field(&message, "ReceiptHandle"),
                    "VisibilityTimeout": 60
                }]}),
            ),
        )
        .await
        .unwrap();
        assert!(answer.get("Successful").is_none());
        assert_eq!(field(&answer["Failed"][0], "Code"), "MessageNotInflight");
    }

    // ------------------------------------------------------------- refusals

    /// A URL this facade did not mint is a queue that is not here, for every
    /// action: a traversal, another account's, and a bare name are all the same
    /// answer, and none of them reaches the broker.
    #[tokio::test]
    async fn a_url_this_facade_did_not_mint_is_a_missing_queue() {
        let rig = Rig::standard().await;
        for url in [
            "http://elsewhere/999999999999/orders",
            "http://sqs.queen.test:9324/000000000000/../orders",
            "orders",
            "",
        ] {
            let error = send_message(&rig.ctx, &json!({"QueueUrl": url, "MessageBody": "x"}))
                .await
                .unwrap_err();
            assert_eq!(error.kind, ErrorKind::QueueDoesNotExist, "{url:?}");
        }
        let error = send_message(&rig.ctx, &json!({"MessageBody": "x"}))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::MissingParameter);
        assert!(rig.fake.pushed().is_empty());
    }

    /// A queue whose URL is well-formed but whose record is not there is the
    /// registry's own answer, and it costs one KV read rather than a push.
    #[tokio::test]
    async fn a_queue_with_no_record_is_a_missing_queue() {
        let rig = Rig::standard().await;
        let error = send_message(
            &rig.ctx,
            &json!({"QueueUrl": rig.url("never-created"), "MessageBody": "x"}),
        )
        .await
        .unwrap_err();
        assert_eq!(error.kind, ErrorKind::QueueDoesNotExist);
    }

    /// A broker failure becomes the catalog's own answer, and never a panic or a
    /// success with missing fields.
    #[tokio::test]
    async fn a_broker_failure_becomes_a_catalog_error() {
        let rig = Rig::standard().await;
        rig.fake
            .fail_push(queen::Error::Transport("connection refused".into()));
        let error = rig
            .send("orders", json!({"MessageBody": "x"}))
            .await
            .unwrap_err();
        assert_eq!(error.kind, ErrorKind::ServiceUnavailable);

        rig.fake.fail_pop(queen::Error::status(429, "{}"));
        let error = rig.receive("orders", json!({})).await.unwrap_err();
        assert_eq!(error.kind, ErrorKind::RequestThrottled);
    }

    /// The join helper is the receive path's concurrency, so its contract — every
    /// future completes, and the answers stay in the order they were asked for —
    /// is worth pinning on its own.
    #[tokio::test]
    async fn the_join_helper_keeps_order_and_finishes_everything() {
        let futures: Vec<BoxFuture<'_, usize>> = (0usize..8)
            .map(|i| {
                Box::pin(async move {
                    // Later futures finish first, so an implementation that
                    // collected in completion order would fail here.
                    tokio::task::yield_now().await;
                    for _ in 0..(8 - i) {
                        tokio::task::yield_now().await;
                    }
                    i
                }) as BoxFuture<'_, usize>
            })
            .collect();
        assert_eq!(join_all(futures).await, (0..8).collect::<Vec<usize>>());
        let empty: Vec<BoxFuture<'_, usize>> = Vec::new();
        assert!(join_all(empty).await.is_empty());
    }
}
