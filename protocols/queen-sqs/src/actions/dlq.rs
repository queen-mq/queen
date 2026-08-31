//! Dead-letter queues: the `RedrivePolicy`, and the atomic move that enforces
//! it.
//!
//! CONTRACT. **Redrive here is facade-driven and the dead-letter queue is a real
//! SQS queue.** Queen has a native DLQ and this path does not use it: every
//! SQS-created queue is configured `deadLetterQueue: false`
//! ([`super::queues::queue_options`]), because the broker's own hand-off would
//! take the message first and put it somewhere no SQS client can address. What
//! happens instead is one write:
//!
//! > On pop, if the message's receive count exceeds the queue's
//! > `maxReceiveCount`, the facade does not return it — it pushes a copy into
//! > the dead-letter queue and acks the original **in one
//! > `POST /api/v1/transaction`**.
//!
//! The bundle is the whole design. A facade that pushed and then acked, and
//! died between the two calls, would duplicate; one that acked and then pushed
//! would LOSE. The stored procedure's own sentence is what makes it neither:
//! *"All-or-nothing by construction: one call = one transaction, every failure
//! path RAISEs, so a duplicate push or a rejected ack rolls back every other
//! operation in the batch"* (005_log_ack.sql, `log_transaction_wire_v1`).
//!
//! ## The threshold, and the count that crosses it
//!
//! AWS: *"The `maxReceiveCount` is the number of times a consumer can receive a
//! message from a source queue before it is moved to a dead-letter queue."* So
//! the consumer gets exactly `maxReceiveCount` deliveries and the NEXT one is
//! the move — the predicate is `received > maxReceiveCount`, never `>=`, and
//! the delivery that triggers the move is never handed to anybody.
//!
//! **The count CONTINUES on the copy; it does not restart.** That is AWS's
//! behaviour and it is the one thing about a moved message people are most
//! often surprised by: the DLQ copy keeps the receive count it had on its
//! source queue, because the count is what says how many times processing
//! failed. This facade cannot do what AWS does and keep the ORIGINAL MESSAGE ID
//! — the copy is a new row in a different queue, and the broker mints ids — so
//! the id rides in the envelope's `m` and the count rides in the envelope's
//! system map under [`SYS_RECEIVE_COUNT`], where
//! [`super::messages::system_view`] adds it back to the copy's own delivery
//! attempt. What is carried is `received - 1`: the number of deliveries a
//! consumer actually SAW, since the delivery that triggered the move was
//! swallowed here and no client was given it.
//!
//! One consequence worth stating rather than discovering: a dead-letter queue
//! that has a `RedrivePolicy` of its own moves the copy on its FIRST receive,
//! because the carried count already exceeds any threshold. That is what AWS's
//! own "not reset" rule produces for chained dead-letter queues, and it is why
//! a queue may not name ITSELF as its dead-letter target ([`RedrivePolicy`]):
//! that one configuration is not a chain but a live-lock, and this facade
//! refuses it rather than rewriting a message for ever.
//!
//! ## What the move may not do
//!
//! **It may never ack past a message the client is keeping.** A Queen ack is a
//! cursor: acking position P completes every unacked position below it
//! (005_log_ack.sql). A FIFO claim covers a RUN, so if a claim ever held a
//! mixture of over-threshold and under-threshold messages, acking the last
//! over-threshold one would silently destroy the under-threshold ones below it.
//! `deliveryAttempt` is per CLAIM (`log_consumers.attempt_count`), so in
//! practice a claim is entirely over the threshold or entirely under it — and
//! [`sift`] does not depend on that: it moves only the LEADING RUN of
//! over-threshold messages in each claim and keeps the rest, which is lossless
//! whether or not the invariant holds.
//!
//! **It may not lose a message to a missing target.** A dead-letter queue that
//! has been deleted since the policy was written leaves the message where it
//! is, logged and delivered, rather than acked into nothing.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::actions::messages::lane_for;
use crate::actions::{queues, Ctx};
use crate::envelope::Envelope;
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::obs::Sampler;
use crate::queen::{self, PushItem, TxnAck};
use crate::registry::{self, Naming, QueueRecord};

/// The attribute that carries the whole policy.
pub const ATTR_REDRIVE_POLICY: &str = "RedrivePolicy";
/// Its companion on the dead-letter queue's own record — accepted and stored,
/// never enforced (PLAN_QUEEN_SQS.md's first non-goal). It is named here
/// because `GetQueueAttributes` answers the two together and a reader of this
/// module will look for it.
pub const ATTR_REDRIVE_ALLOW_POLICY: &str = "RedriveAllowPolicy";

/// AWS's range for `maxReceiveCount`.
pub const MIN_RECEIVE_COUNT: i64 = 1;
pub const MAX_RECEIVE_COUNT: i64 = 1_000;

/// The system-attribute key carrying the queue a copy was moved FROM.
///
/// It is what `StartMessageMoveTask` uses to send a message home when no
/// `DestinationArn` was given, which is AWS's documented default — and it has
/// to be in the PAYLOAD rather than in a side table, because the message may be
/// moved back by an instance that never saw the move out.
///
/// `queen.`-prefixed, like the queue attributes this facade adds to SQS's own
/// vocabulary, so it can never collide with an attribute AWS defines. A client
/// cannot forge one: [`super::messages`] accepts exactly one system attribute
/// on a send (`AWSTraceHeader`) and refuses the rest.
pub const SYS_SOURCE_QUEUE: &str = "queen.sourceQueue";

/// The receive count a moved copy continues from — see the module header. It is
/// CONSUMED by [`super::messages::system_view`] rather than echoed: two numbers
/// describing one message's delivery count, one of them frozen at the move,
/// would be a contradiction a client has to resolve.
pub const SYS_RECEIVE_COUNT: &str = "queen.receiveCount";

/// The attribute a dead-lettered copy reports its ORIGINAL MessageId under.
///
/// DIVERGENCE, `accepted`, and this is the half of it that is answerable: AWS
/// keeps a message's id across a move and this facade cannot — the copy is a new
/// row in a different queue and the broker mints ids — so the original rides in
/// the envelope's `m` ([`copy_of`]). That field is not an SQS attribute of its
/// own, so [`super::messages::system_view`] surfaces it under this name; without
/// it a DLQ consumer has NO correlation back to the message it is holding the
/// remains of, which is the first thing anybody debugging a dead-letter queue
/// asks for. `queen.`-prefixed, like [`SYS_SOURCE_QUEUE`] beside it, so it can
/// never collide with an attribute AWS defines.
pub const SYS_ORIGINAL_MESSAGE_ID: &str = "queen.originalMessageId";

/// Extra pop rounds one `ReceiveMessage` will spend refilling after a move.
///
/// A receive that moved messages has fewer than the client asked for, and the
/// natural instinct is to keep popping until it is full. It is bounded at two
/// EXTRA rounds because the failure mode of an unbounded fill is the one that
/// matters here: a queue whose whole backlog is over the threshold would turn a
/// single `ReceiveMessage` into a drain of the entire queue, holding one
/// client's HTTP request open for as long as that takes and answering nothing
/// at the end of it. A short receive is legal SQS — `MaxNumberOfMessages` is a
/// ceiling and never a promise — and the client's next poll continues the
/// drain, at the client's own pace and with the client's own timeout.
pub const MAX_FILL_ROUNDS: usize = 2;

/// The longest slice of a client's own policy document that travels back in an
/// error message. AWS echoes the whole value; this echoes a bounded prefix,
/// because the document is unbounded client input that would otherwise land
/// whole in this facade's logs.
const MAX_ECHOED_POLICY: usize = 256;

/// Queue records one `ListDeadLetterSourceQueues` reads per page of the scan.
/// Larger than the answer's own page because the filter is on the VALUE and a
/// page of a hundred queues can hold no match at all.
const SCAN_CHUNK: usize = 200;

/// Pages one source-queue scan will read. The same bound, and for the same
/// reason, as the registry's own walk: a loop whose exit depends on another
/// process's cursor needs one.
const MAX_SCAN_PAGES: usize = 64;

/// AWS's cap on `ListDeadLetterSourceQueues`.
const MAX_LIST_RESULTS: i64 = 1_000;

/// A move that could not be written. The messages stay where they are and the
/// next delivery tries again, so this is a line and never an error to a client.
static MOVE_FAILED: Sampler = Sampler::new(10_000);
/// A `RedrivePolicy` that was stored and can no longer be read, or that names a
/// dead-letter queue which is gone. Both leave the message on its own queue.
static POLICY_UNUSABLE: Sampler = Sampler::new(60_000);

// -------------------------------------------------------------- the policy

/// One queue's `RedrivePolicy`, parsed.
///
/// The dead-letter target is kept as a NAME rather than as the ARN it arrived
/// as: everything downstream — the push, the registry lookup, the source scan —
/// addresses queues by name, and an ARN that was validated once and re-parsed
/// three times is three places the validation can differ.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedrivePolicy {
    pub dead_letter_queue: String,
    pub max_receive_count: i64,
}

impl RedrivePolicy {
    /// Parse and validate everything that can be checked without asking the
    /// store: the JSON shape, the two mandatory members, the ARN's realm, and
    /// the range. Existence and queue TYPE need a read and live in [`check`].
    ///
    /// `maxReceiveCount` is read as a number OR a numeric string, because
    /// Terraform and several SDK helpers write `{"maxReceiveCount":"5"}` and a
    /// facade that accepted only one spelling would refuse half the fleet.
    pub fn parse(raw: &str, naming: &Naming) -> SqsResult<RedrivePolicy> {
        let document: Value = serde_json::from_str(raw)
            .map_err(|_| invalid_policy(raw, "Redrive policy is not a valid JSON map"))?;
        let Some(fields) = document.as_object() else {
            return Err(invalid_policy(
                raw,
                "Redrive policy is not a valid JSON map",
            ));
        };
        let arn = fields
            .get("deadLetterTargetArn")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                invalid_policy(
                    raw,
                    "Redrive policy does not contain mandatory attribute: deadLetterTargetArn",
                )
            })?;
        let dead_letter_queue = naming.name_of_arn(arn).ok_or_else(|| {
            invalid_policy(
                raw,
                "Dead-letter target owner should be same as the source, and the target must be a \
                 queue of this region and account",
            )
        })?;
        let count = fields.get("maxReceiveCount").ok_or_else(|| {
            invalid_policy(
                raw,
                "Redrive policy does not contain mandatory attribute: maxReceiveCount",
            )
        })?;
        let max_receive_count = number(count).ok_or_else(|| {
            invalid_policy(
                raw,
                "Value for maxReceiveCount is not a number, and it must be an integer",
            )
        })?;
        if !(MIN_RECEIVE_COUNT..=MAX_RECEIVE_COUNT).contains(&max_receive_count) {
            return Err(invalid_policy(
                raw,
                &format!(
                    "Value for maxReceiveCount is invalid. Reason: must be {MIN_RECEIVE_COUNT} to \
                     {MAX_RECEIVE_COUNT}"
                ),
            ));
        }
        Ok(RedrivePolicy {
            dead_letter_queue,
            max_receive_count,
        })
    }

    /// The policy a stored record carries, or `None` for a queue that has none.
    ///
    /// LENIENT, unlike [`RedrivePolicy::parse`], and deliberately so: this runs
    /// on the RECEIVE path, where the document has already been validated on
    /// the way in. A record that cannot be read here is corruption or a
    /// deployment whose region label changed under it, and the answer to either
    /// is to deliver the message and say so — never to fail a receive over an
    /// attribute the client did not send.
    pub fn of(record: &QueueRecord, naming: &Naming) -> Option<RedrivePolicy> {
        let raw = record.attributes.get(ATTR_REDRIVE_POLICY)?;
        match RedrivePolicy::parse(raw, naming) {
            Ok(policy) => Some(policy),
            Err(_) => {
                if let Some(suppressed) = POLICY_UNUSABLE.tick_now() {
                    tracing::warn!(
                        target: "sqs",
                        suppressed,
                        queue = %record.name,
                        "this queue's RedrivePolicy cannot be read; nothing is being dead-lettered \
                         from it"
                    );
                }
                None
            }
        }
    }

    /// Whether this policy names `queue` as its dead-letter target.
    pub fn targets(&self, queue: &str) -> bool {
        self.dead_letter_queue == queue
    }
}

/// `RedrivePolicy.maxReceiveCount` alone, for the queue options bag.
///
/// It does NOT go through [`RedrivePolicy::parse`], and that is not a second
/// parser by accident: the options bag is built for a record whose ARN may name
/// a realm this process no longer mints (an operator changed `QUEEN_SQS_REGION`
/// under a live deployment), and the broker's `retryLimit` must not fall back
/// to a default because of a label. One document, two questions, and the strict
/// one is asked only where a client is waiting for the answer.
pub fn max_receive_count(record: &QueueRecord) -> Option<i64> {
    let policy: Value = serde_json::from_str(record.attributes.get(ATTR_REDRIVE_POLICY)?).ok()?;
    number(policy.get("maxReceiveCount")?).filter(|n| *n > 0)
}

/// A JSON number or a numeric string, as one integer.
fn number(value: &Value) -> Option<i64> {
    value
        .as_i64()
        .or_else(|| value.as_str()?.trim().parse::<i64>().ok())
}

/// Everything about a `RedrivePolicy` that needs the store: the target must
/// EXIST, it must not be the source itself, and it must be the same queue type.
///
/// Called before the registry write on both `CreateQueue` and
/// `SetQueueAttributes`, so a policy that would be unusable never reaches a
/// record — a stored policy naming nothing is a queue that silently stops
/// dead-lettering, which is the failure nobody notices until the source queue
/// is full.
pub async fn check(
    ctx: &Ctx,
    source: &str,
    attributes: &BTreeMap<String, String>,
) -> SqsResult<()> {
    let Some(raw) = attributes.get(ATTR_REDRIVE_POLICY) else {
        return Ok(());
    };
    let policy = RedrivePolicy::parse(raw, &queues::naming(&ctx.facade.config))?;
    if policy.dead_letter_queue == source {
        // DIVERGENCE, `deliberate`. AWS's own validation may accept a queue
        // that names itself; this facade cannot, because here the consequence
        // is not a chain but a live-lock. The copy is a new message with a
        // fresh delivery budget, so it is received once, found to be over the
        // threshold by the count it carries, moved to itself again, and so on
        // for as long as anybody polls — an unbounded rewrite of one message,
        // costing a transaction each time. Cycles LONGER than one hop are not
        // detected: they would need a walk of the whole target graph on every
        // SetQueueAttributes, and they are visible to an operator as a queue
        // whose depth never falls.
        return Err(invalid_policy(
            raw,
            "The dead letter target must not be the source queue itself",
        ));
    }
    let target = ctx
        .facade
        .registry
        .queue(&policy.dead_letter_queue, ctx.token())
        .await
        .map_err(|e| SqsError::from_queen(&e))?;
    let Some(target) = target else {
        return Err(invalid_policy(raw, "Dead letter target does not exist"));
    };
    // AWS's rule, in both directions: a FIFO queue's dead-letter queue must be
    // FIFO and a standard queue's must be standard. It is not decoration here —
    // a standard target has no `MessageGroupId` to preserve, and a FIFO target
    // reached from a standard source would need a group this facade would have
    // to invent.
    if target.fifo != registry::is_fifo(source) {
        let reason = match registry::is_fifo(source) {
            true => "Dead letter target must be a FIFO queue when the source queue is a FIFO queue",
            false => {
                "Dead letter target must be a standard queue when the source queue is a standard \
                 queue"
            }
        };
        return Err(invalid_policy(raw, reason));
    }
    Ok(())
}

/// AWS's shape for a refused parameter, with the offending value echoed as AWS
/// echoes it — bounded, for the reason [`MAX_ECHOED_POLICY`] gives.
fn invalid_policy(raw: &str, reason: &str) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!(
            "Value {} for parameter RedrivePolicy is invalid. Reason: {reason}.",
            echo(raw)
        ),
    )
}

fn echo(raw: &str) -> String {
    match raw.char_indices().nth(MAX_ECHOED_POLICY) {
        None => raw.to_string(),
        Some((cut, _)) => format!("{}…", &raw[..cut]),
    }
}

// --------------------------------------------------------------- the count

/// The receive count a copy carried in from another queue: 0 for a message
/// nobody moved.
pub fn carried_count(envelope: &Envelope) -> i64 {
    envelope
        .system
        .get(SYS_RECEIVE_COUNT)
        .and_then(|v| v.trim().parse::<i64>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(0)
}

/// The queue a copy was moved from, when it was moved at all.
pub fn source_of(envelope: &Envelope) -> Option<&str> {
    envelope
        .system
        .get(SYS_SOURCE_QUEUE)
        .map(String::as_str)
        .filter(|s| !s.is_empty())
}

/// `ApproximateReceiveCount` for one delivery: what the message carried in plus
/// what this queue has delivered. It is BOTH the number the client is told and
/// the number the threshold is compared against, and those two being one
/// expression is the point — a client that is told a message was received five
/// times and a facade that moves on a different count is a facade nobody can
/// debug from a client's log.
pub fn received_count(envelope: &Envelope, message: &queen::Message) -> i64 {
    carried_count(envelope) + message.delivery_attempt
}

// ---------------------------------------------------------------- the move

/// What one [`sift`] did.
pub struct Sifted {
    /// The messages the client is given, in the order they were popped.
    pub kept: Vec<queen::Message>,
    /// The messages whose move COMMITTED — copied into the dead-letter queue
    /// and acked on this queue inside one transaction. A move that failed is in
    /// neither list: the message is still on the queue, over its threshold and
    /// undelivered, and the caller must not treat it as gone
    /// ([`super::fifo::forget`], which acks past what this list names).
    pub moved: Vec<queen::Message>,
}

/// The redrive check, between the pop and the answer.
///
/// Every message whose [`received_count`] exceeds the queue's `maxReceiveCount`
/// is moved rather than returned — see the module header for the threshold, the
/// carried count, and why only the leading run of each claim is eligible.
pub async fn sift(
    ctx: &Ctx,
    record: &QueueRecord,
    messages: Vec<queen::Message>,
) -> SqsResult<Sifted> {
    let naming = queues::naming(&ctx.facade.config);
    let Some(policy) = RedrivePolicy::of(record, &naming) else {
        return Ok(Sifted {
            kept: messages,
            moved: Vec::new(),
        });
    };

    // Decided in one pass and split afterwards, so the delivery ORDER survives
    // every path out of this function — including the one where the target is
    // gone and every message is handed back.
    let mut over_threshold: Vec<bool> = Vec::with_capacity(messages.len());
    // Claims that have already handed the client a message. Nothing below one
    // of those may be acked, so nothing below one of those may be moved.
    let mut broken: BTreeSet<(&str, &str)> = BTreeSet::new();
    for message in &messages {
        let claim = (message.partition_id.as_str(), message.lease_id.as_str());
        let envelope = Envelope::decode(&message.data);
        let eligible = !broken.contains(&claim)
            && received_count(&envelope, message) > policy.max_receive_count;
        if !eligible {
            broken.insert(claim);
        }
        over_threshold.push(eligible);
    }
    if !over_threshold.contains(&true) {
        return Ok(Sifted {
            kept: messages,
            moved: Vec::new(),
        });
    }

    let target = ctx
        .facade
        .registry
        .queue(&policy.dead_letter_queue, ctx.token())
        .await
        .map_err(|e| SqsError::from_queen(&e))?;
    let Some(target) = target else {
        // The target was deleted after the policy was written. The messages are
        // DELIVERED rather than acked into nothing: a client seeing a message
        // once more than its budget is inside SQS's own at-least-once envelope,
        // and a message acked with nowhere to go is gone.
        if let Some(suppressed) = POLICY_UNUSABLE.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                queue = %record.name,
                dead_letter = %policy.dead_letter_queue,
                held = messages.len(),
                "the dead-letter queue this queue names does not exist; nothing is being moved"
            );
        }
        return Ok(Sifted {
            kept: messages,
            moved: Vec::new(),
        });
    };

    let mut over: Vec<queen::Message> = Vec::new();
    let mut kept: Vec<queen::Message> = Vec::new();
    for (message, eligible) in messages.into_iter().zip(over_threshold) {
        match eligible {
            true => over.push(message),
            false => kept.push(message),
        }
    }
    // A move that did not commit leaves its messages in NEITHER list: they are
    // still on this queue, and the caller may neither hand them to the client —
    // the threshold says this delivery must not happen — nor treat them as gone.
    let committed = move_all(ctx, &record.name, &target, &over).await;
    over.retain(|message| committed.contains(&message.id));
    Ok(Sifted { kept, moved: over })
}

/// Move every message, one transaction per CLAIM. Answers the ids whose
/// transaction COMMITTED.
///
/// One bundle per claim and not one for all of them: a rejected ack rolls the
/// whole bundle back, so a single expired lease among ten would undo nine moves
/// that were about to succeed. The claims are independent and run concurrently,
/// which is the same shape (and the same round-trip count) as the receive that
/// claimed them.
async fn move_all(
    ctx: &Ctx,
    source: &str,
    target: &QueueRecord,
    messages: &[queen::Message],
) -> BTreeSet<String> {
    let mut claims: Vec<(String, String, Vec<&queen::Message>)> = Vec::new();
    for message in messages {
        match claims
            .iter_mut()
            .find(|(p, l, _)| *p == message.partition_id && *l == message.lease_id)
        {
            Some((_, _, run)) => run.push(message),
            None => claims.push((
                message.partition_id.clone(),
                message.lease_id.clone(),
                vec![message],
            )),
        }
    }
    // The receive path's own combinator: every claim is moved inside ONE task,
    // so a client that disconnects mid-receive drops the moves with the request
    // instead of leaving them to run against claims nobody is holding
    // ([`super::messages::join_all`]).
    let calls: Vec<queen::BoxFuture<'_, BTreeSet<String>>> = claims
        .iter()
        .map(|(partition_id, lease_id, run)| {
            let call: queen::BoxFuture<'_, BTreeSet<String>> =
                Box::pin(move_claim(ctx, source, target, partition_id, lease_id, run));
            call
        })
        .collect();
    super::messages::join_all(calls)
        .await
        .into_iter()
        .flatten()
        .collect()
}

/// One claim's messages: pushes into the dead-letter queue and ONE ack, in one
/// transaction.
///
/// The ack names the LAST message of the run and completes everything below it
/// in the claim — which is exactly the run, because [`sift`] only ever hands
/// this a leading run. Answers which were moved: all of them, or none.
async fn move_claim(
    ctx: &Ctx,
    source: &str,
    target: &QueueRecord,
    partition_id: &str,
    lease_id: &str,
    run: &[&queen::Message],
) -> BTreeSet<String> {
    let Some(last) = run.last() else {
        return BTreeSet::new();
    };
    let pushes: Vec<PushItem> = run
        .iter()
        .map(|message| copy_of(source, target, message))
        .collect();
    // NO `transactionId` on the push, which is what "a fresh transaction id"
    // means here: the broker mints one per row, so the destination's dedup
    // window has nothing to match and cannot swallow the move. Sending the
    // ORIGINAL dedup key would do exactly that on a FIFO target whose window a
    // previous move of the same key is still inside.
    let acks = [TxnAck::completed(
        &last.transaction_id,
        partition_id,
        lease_id,
    )];
    match ctx
        .facade
        .queen
        .transaction(&pushes, &acks, &[], ctx.token())
        .await
    {
        Ok(_) => run.iter().map(|message| message.id.clone()).collect(),
        Err(e) => {
            // Nothing was written and nothing was acked. The messages are NOT
            // returned to the client either: their own queue's `maxReceiveCount`
            // says this delivery must not happen, so the claim is left to expire
            // and the next receive tries the move again.
            if let Some(suppressed) = MOVE_FAILED.tick_now() {
                tracing::warn!(
                    target: "sqs",
                    suppressed,
                    queue = %source,
                    dead_letter = %target.name,
                    held = run.len(),
                    error = %e,
                    "a dead-letter move did not commit; the messages return when their visibility \
                     timeout expires"
                );
            }
            BTreeSet::new()
        }
    }
}

/// The copy one message becomes on the dead-letter queue.
///
/// Body and message attributes travel verbatim. What is added is the three
/// things a DLQ consumer and a later `StartMessageMoveTask` cannot reconstruct:
/// the original MessageId (`m`), the queue it came from
/// ([`SYS_SOURCE_QUEUE`]) and the receive count to continue from
/// ([`SYS_RECEIVE_COUNT`]).
///
/// A message a NATIVE Queen producer wrote is re-encoded as an envelope on the
/// way through, and that is a real transformation rather than a copy: its
/// payload becomes the copy's `b`. The alternative is a DLQ copy that carries
/// none of the three markers above, which is a message no move task can send
/// home.
fn copy_of(source: &str, target: &QueueRecord, message: &queen::Message) -> PushItem {
    let mut envelope = Envelope::decode(&message.data);
    let received = received_count(&envelope, message);
    envelope.moved_from = Some(message.id.clone());
    envelope
        .system
        .insert(SYS_SOURCE_QUEUE.to_string(), source.to_string());
    // `received - 1`: the delivery that triggered the move was never handed to
    // a consumer, so it is not one of the times the message was received (the
    // module header).
    envelope.system.insert(
        SYS_RECEIVE_COUNT.to_string(),
        (received - 1).max(0).to_string(),
    );
    let partition = match target.fifo {
        // SAME GROUP. A FIFO source's lane name IS its `MessageGroupId`, and a
        // dead-lettered message that changed group would be delivered out of
        // order with respect to the rest of its group on the target.
        true => message.partition.clone(),
        // The ORIGINAL MessageId across the target's width. It exists by now —
        // unlike on a send, where the lane must be chosen before the broker has
        // minted anything — and using it means two moves of one message land on
        // one lane.
        false => lane_for(&message.id, target.partitions),
    };
    PushItem::new(&target.name, &partition, envelope.encode())
}

/// Strip the markers a move added, for a message being sent back to its source.
///
/// `m` is KEPT: it is the id of the message this copy was made from, which is
/// true of a restored message too and is the only correlation a client has left
/// after two moves have each minted a new id. The other two are dropped,
/// because they are what a move READS: a restored message that still named a
/// source queue would be a candidate for restoring again, and one that still
/// carried a receive count would arrive on its source queue already over the
/// threshold and be dead-lettered on its first delivery — which would make
/// every redrive a no-op.
pub fn restored(mut envelope: Envelope) -> Envelope {
    envelope.system.remove(SYS_SOURCE_QUEUE);
    envelope.system.remove(SYS_RECEIVE_COUNT);
    envelope
}

// -------------------------------------------------- the source queue scan

/// `ListDeadLetterSourceQueues`: every queue whose `RedrivePolicy` names this
/// one. A registry scan, and never a broker call.
pub async fn list_dead_letter_source_queues(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let name = queues::queue_of(ctx, params)?;
    ctx.facade.registry.require(&name, ctx.token()).await?;
    let limit = match queues::param_int(params, "MaxResults")? {
        None => MAX_LIST_RESULTS,
        Some(n) if (1..=MAX_LIST_RESULTS).contains(&n) => n,
        Some(_) => {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "Value for parameter MaxResults is invalid. Reason: must be 1 to \
                     {MAX_LIST_RESULTS}."
                ),
            ))
        }
    };
    let page = sources_of(
        ctx,
        &name,
        limit as usize,
        queues::param_text(params, "NextToken"),
    )
    .await?;

    let urls: Vec<Value> = page
        .0
        .iter()
        .map(|q| Value::String(queues::queue_url(ctx, q)))
        .collect();
    let mut answer = serde_json::Map::new();
    // AWS answers the member even when it is empty here, unlike ListQueues: the
    // shape is required in its model, and an SDK reads the empty list as "no
    // source queues" rather than as a field it forgot to send.
    answer.insert("queueUrls".to_string(), Value::Array(urls));
    if let Some(next) = page.1 {
        answer.insert("NextToken".to_string(), Value::String(next));
    }
    Ok(Value::Object(answer))
}

/// The queues whose `RedrivePolicy` targets `target`, and the cursor to
/// continue from.
///
/// The cursor names the last queue SCANNED and not the last one matched, which
/// is the whole difference between a correct paging of a filtered scan and one
/// that skips: a page of two hundred records may hold one match, and a token
/// built from that match would resume the scan two hundred records early on the
/// next call and answer the same queues again.
pub async fn sources_of(
    ctx: &Ctx,
    target: &str,
    limit: usize,
    next_token: Option<&str>,
) -> SqsResult<(Vec<String>, Option<String>)> {
    let naming = queues::naming(&ctx.facade.config);
    let mut cursor = next_token.map(str::to_string);
    let mut found: Vec<String> = Vec::new();
    for _ in 0..MAX_SCAN_PAGES {
        let page = ctx
            .facade
            .registry
            .list("", SCAN_CHUNK, cursor.as_deref(), ctx.token())
            .await?;
        for record in &page.queues {
            let hit = RedrivePolicy::of(record, &naming).is_some_and(|p| p.targets(target));
            if hit {
                found.push(record.name.clone());
            }
            if found.len() >= limit {
                // Mid-page: the scan resumes AFTER this record, matched or not.
                return Ok((found, Some(registry::encode_token(&record.name))));
            }
        }
        match page.next_token {
            None => return Ok((found, None)),
            Some(next) => cursor = Some(next),
        }
    }
    // The scan did not finish inside its page budget. The cursor is handed back
    // so the client's next call continues rather than restarting, which is the
    // difference between a long listing and an endless one.
    Ok((found, cursor))
}

/// Whether `queue` is a dead-letter target of anything — the precondition
/// `StartMessageMoveTask` enforces, since AWS allows a move task only from a
/// queue that is configured as a dead-letter queue.
pub async fn is_dead_letter_target(ctx: &Ctx, queue: &str) -> SqsResult<bool> {
    Ok(!sources_of(ctx, queue, 1, None).await?.0.is_empty())
}

// NOTE ON THE TWO POLICY ATTRIBUTES AND `GetQueueAttributes`. There is no
// function here to render them, and that is the answer rather than an omission:
// both are stored in the queue record's own attribute map and answered from it
// with everything else ([`super::queues`]), as the JSON STRINGS the client sent,
// byte for byte. Neither is re-serialized on the way out — an SDK that
// round-trips `RedrivePolicy` through `SetQueueAttributes` compares the string
// it sent, and a re-serialization that reordered two keys or dropped the quotes
// around a numeric `maxReceiveCount` would fail that comparison for a document
// this facade itself had accepted.

/// A `{"deadLetterTargetArn": …, "maxReceiveCount": n}` document.
#[cfg(test)]
pub fn policy_document(naming: &Naming, dead_letter_queue: &str, max_receive_count: i64) -> String {
    serde_json::json!({
        "deadLetterTargetArn": naming.arn(dead_letter_queue),
        "maxReceiveCount": max_receive_count,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::queues::{create_queue, set_queue_attributes};
    use crate::actions::testing::{arn, attribute, carrying, field, redrive_policy, Rig};
    use crate::config::{DEFAULT_ACCOUNT, DEFAULT_REGION};
    use serde_json::json;

    fn naming() -> Naming {
        Naming::new(DEFAULT_REGION, DEFAULT_ACCOUNT)
    }

    /// A source with a policy, and the dead-letter queue it names. The target is
    /// created FIRST, because a policy may not name a queue that is not there.
    async fn rig_with(max_receive_count: i64) -> Rig {
        let policy = redrive_policy("orders-dlq", max_receive_count);
        Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await
    }

    /// Everything on a queue right now, with every attribute a receive can
    /// answer.
    async fn drain(rig: &Rig, queue: &str) -> Vec<Value> {
        rig.receive_list(
            queue,
            json!({
                "MaxNumberOfMessages": 10,
                "AttributeNames": ["All"],
                "MessageAttributeNames": ["All"],
            }),
        )
        .await
    }

    // ------------------------------------------------------------- the policy

    /// The two spellings AWS's own clients write. Terraform sends the count as a
    /// STRING, and a facade that read only the number would give those queues
    /// the broker's default budget without saying so.
    #[test]
    fn a_policy_parses_from_either_spelling_of_its_count() {
        let numeric = format!(
            r#"{{"deadLetterTargetArn":"{}","maxReceiveCount":5}}"#,
            arn("dead")
        );
        let stringly = format!(
            r#"{{"deadLetterTargetArn":"{}","maxReceiveCount":"5"}}"#,
            arn("dead")
        );
        for raw in [numeric, stringly] {
            assert_eq!(
                RedrivePolicy::parse(&raw, &naming()).expect("parses"),
                RedrivePolicy {
                    dead_letter_queue: "dead".to_string(),
                    max_receive_count: 5,
                },
                "{raw}"
            );
        }
    }

    /// Both members are mandatory, and the refusal NAMES the one that is
    /// missing: a client reading "invalid" from its log cannot tell which half
    /// of the document it got wrong.
    #[test]
    fn a_policy_missing_either_member_is_refused_by_name() {
        let cases = [
            (
                format!(r#"{{"deadLetterTargetArn":"{}"}}"#, arn("dead")),
                "maxReceiveCount",
            ),
            (
                r#"{"maxReceiveCount":5}"#.to_string(),
                "deadLetterTargetArn",
            ),
        ];
        for (raw, missing) in cases {
            let error = RedrivePolicy::parse(&raw, &naming()).expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
            assert!(error.message.contains(missing), "{}", error.message);
        }
    }

    #[test]
    fn a_count_outside_its_range_is_refused_at_both_ends() {
        for count in ["0", "-1", "1001"] {
            let raw = format!(
                r#"{{"deadLetterTargetArn":"{}","maxReceiveCount":{count}}}"#,
                arn("dead")
            );
            let error = RedrivePolicy::parse(&raw, &naming()).expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{count}");
            assert!(error.message.contains("1 to 1000"), "{}", error.message);
        }
        // ...and the two ends themselves are inside it.
        for count in [MIN_RECEIVE_COUNT, MAX_RECEIVE_COUNT] {
            let raw = policy_document(&naming(), "dead", count);
            assert_eq!(
                RedrivePolicy::parse(&raw, &naming())
                    .expect("inside the range")
                    .max_receive_count,
                count
            );
        }
    }

    /// The ARN is run backwards through the same naming that mints one, so a
    /// target in another account or another region — which AWS refuses, since a
    /// dead-letter queue must share both with its source — cannot be stored.
    #[test]
    fn a_target_outside_this_deployment_is_refused() {
        for target in [
            "arn:aws:sqs:queen-1:999999999999:dead",
            "arn:aws:sqs:eu-west-1:000000000000:dead",
            "arn:aws:sns:queen-1:000000000000:dead",
            "not-an-arn",
            "arn:aws:sqs:queen-1:000000000000:de ad",
            "",
        ] {
            let raw = format!(r#"{{"deadLetterTargetArn":"{target}","maxReceiveCount":5}}"#);
            let error = RedrivePolicy::parse(&raw, &naming()).expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{target}");
        }
    }

    #[test]
    fn a_document_that_is_not_a_json_object_is_refused() {
        for raw in ["", "null", "[]", "\"policy\"", "{", "7"] {
            let error = RedrivePolicy::parse(raw, &naming()).expect_err("refused");
            assert!(
                error.message.contains("not a valid JSON map"),
                "{raw}: {}",
                error.message
            );
        }
    }

    /// AWS's message shape, with the offending value echoed — bounded, because
    /// a policy document is unbounded client input and this sentence lands in
    /// this facade's own logs.
    #[test]
    fn the_refusal_echoes_a_bounded_prefix_of_the_document() {
        let long = format!(r#"{{"deadLetterTargetArn":"{}"}}"#, "x".repeat(4_000));
        let error = RedrivePolicy::parse(&long, &naming()).expect_err("refused");
        assert!(
            error.message.starts_with("Value {\"deadLetterTargetArn\""),
            "{}",
            error.message
        );
        assert!(error.message.contains('…'), "{}", error.message);
        assert!(error.message.len() < 512, "{}", error.message.len());
        // The shape AWS writes, which is what an operator comparing two logs
        // matches on.
        assert!(
            error
                .message
                .contains("for parameter RedrivePolicy is invalid. Reason: "),
            "{}",
            error.message
        );
    }

    /// The lenient read the RECEIVE path uses: a record whose policy cannot be
    /// parsed is a queue that dead-letters nothing, never a receive that fails.
    #[test]
    fn an_unreadable_stored_policy_is_no_policy_at_all() {
        let mut record = QueueRecord {
            name: "orders".to_string(),
            ..QueueRecord::default()
        };
        assert_eq!(RedrivePolicy::of(&record, &naming()), None);
        record.attributes.insert(
            ATTR_REDRIVE_POLICY.to_string(),
            "{\"maxReceiveCount\":2}".to_string(),
        );
        assert_eq!(RedrivePolicy::of(&record, &naming()), None);
        record.attributes.insert(
            ATTR_REDRIVE_POLICY.to_string(),
            policy_document(&naming(), "dead", 2),
        );
        assert_eq!(
            RedrivePolicy::of(&record, &naming()).map(|p| p.dead_letter_queue),
            Some("dead".to_string())
        );
    }

    /// The broker's `retryLimit` is read from a document whose ARN this process
    /// may no longer be able to mint — an operator changed the region label —
    /// and it must not fall back to a default because of a label.
    #[test]
    fn the_count_alone_is_readable_without_the_arns_realm() {
        let mut record = QueueRecord::default();
        record.attributes.insert(
            ATTR_REDRIVE_POLICY.to_string(),
            r#"{"deadLetterTargetArn":"arn:aws:sqs:elsewhere:1:dead","maxReceiveCount":"7"}"#
                .to_string(),
        );
        assert_eq!(max_receive_count(&record), Some(7));
        assert_eq!(RedrivePolicy::of(&record, &naming()), None);
    }

    // -------------------------------------------------- validation with the store

    #[tokio::test]
    async fn a_policy_naming_a_queue_that_is_not_there_is_refused() {
        let rig = Rig::standard().await;
        let error = create_queue(
            &rig.ctx,
            &json!({
                "QueueName": "with-redrive",
                "Attributes": {"RedrivePolicy": redrive_policy("nowhere", 3)},
            }),
        )
        .await
        .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        assert!(
            error.message.contains("Dead letter target does not exist"),
            "{}",
            error.message
        );
    }

    /// AWS's rule, in both directions. It is not decoration here: a standard
    /// target has no group to preserve, and a FIFO one reached from a standard
    /// source would need a group this facade would have to invent.
    #[tokio::test]
    async fn the_target_must_be_the_same_queue_type_as_the_source() {
        let rig = Rig::new(&[("plain-dlq", &[]), ("fifo-dlq.fifo", &[])]).await;
        let cases = [
            ("standard-source", "fifo-dlq.fifo"),
            ("fifo-source.fifo", "plain-dlq"),
        ];
        for (source, target) in cases {
            let error = create_queue(
                &rig.ctx,
                &json!({
                    "QueueName": source,
                    "Attributes": {
                        "RedrivePolicy": redrive_policy(target, 3),
                        "FifoQueue": crate::registry::is_fifo(source).to_string(),
                    },
                }),
            )
            .await
            .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{source}");
            assert!(
                error.message.contains("same queue type")
                    || error.message.contains("must be a FIFO queue")
                    || error.message.contains("must be a standard queue"),
                "{source}: {}",
                error.message
            );
        }
        // ...and the matching pairs are accepted.
        for (source, target) in [
            ("standard-source", "plain-dlq"),
            ("fifo-source.fifo", "fifo-dlq.fifo"),
        ] {
            create_queue(
                &rig.ctx,
                &json!({
                    "QueueName": source,
                    "Attributes": {
                        "RedrivePolicy": redrive_policy(target, 3),
                        "FifoQueue": crate::registry::is_fifo(source).to_string(),
                    },
                }),
            )
            .await
            .unwrap_or_else(|e| panic!("{source} -> {target}: {e}"));
        }
    }

    /// DIVERGENCE, `deliberate`: a queue may not be its own dead-letter target.
    /// The copy carries a receive count that is already over any threshold, so
    /// the configuration is not a chain but an unbounded rewrite of one message.
    #[tokio::test]
    async fn a_queue_may_not_be_its_own_dead_letter_target() {
        let rig = Rig::standard().await;
        let error = create_queue(
            &rig.ctx,
            &json!({
                "QueueName": "loop",
                "Attributes": {"RedrivePolicy": redrive_policy("loop", 3)},
            }),
        )
        .await
        .expect_err("refused");
        assert!(
            error
                .message
                .contains("must not be the source queue itself"),
            "{}",
            error.message
        );
    }

    /// The policy is MUTABLE, so the check has to run on the set as well: a
    /// queue can acquire a dead-letter target long after it was created, and a
    /// target deleted in between is exactly what a create-time-only check
    /// misses.
    #[tokio::test]
    async fn set_queue_attributes_validates_the_policy_too() {
        let rig = Rig::new(&[("orders", &[]), ("orders-dlq", &[])]).await;
        let error = set_queue_attributes(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Attributes": {"RedrivePolicy": redrive_policy("gone", 3)}}),
            ),
        )
        .await
        .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
        // ...and the one that names a real queue lands, verbatim.
        let policy = redrive_policy("orders-dlq", 4);
        set_queue_attributes(
            &rig.ctx,
            &rig.params(
                "orders",
                json!({"Attributes": {"RedrivePolicy": policy.clone()}}),
            ),
        )
        .await
        .expect("stored");
        let stored = rig
            .ctx
            .facade
            .registry
            .require("orders", None)
            .await
            .expect("the record");
        assert_eq!(
            stored.attributes.get(ATTR_REDRIVE_POLICY),
            Some(&policy),
            "the document is stored byte for byte"
        );
    }

    // ----------------------------------------------------------- the threshold

    /// THE threshold, delivery by delivery. `maxReceiveCount` is the number of
    /// times a consumer may RECEIVE the message; the next delivery is the move
    /// and is never handed to anybody.
    #[tokio::test]
    async fn the_nth_delivery_is_answered_and_the_next_one_is_moved() {
        let rig = rig_with(2).await;
        rig.fake.seed("orders", "0", 0, &[json!({"b": "work"})]);

        for expected in ["1", "2"] {
            let messages = drain(&rig, "orders").await;
            assert_eq!(messages.len(), 1, "delivery {expected}");
            assert_eq!(
                attribute(&messages[0], "ApproximateReceiveCount"),
                Some(expected.to_string())
            );
            rig.fake.advance(std::time::Duration::from_secs(31));
        }
        // The third delivery is the move: nothing is answered...
        assert!(drain(&rig, "orders").await.is_empty());
        // ...and it is on the dead-letter queue instead.
        let dead = drain(&rig, "orders-dlq").await;
        assert_eq!(dead.len(), 1);
        assert_eq!(field(&dead[0], "Body"), "work");
    }

    #[tokio::test]
    async fn a_queue_with_no_policy_never_moves_anything() {
        let rig = Rig::new(&[("orders", &[])]).await;
        rig.fake.seed("orders", "0", 0, &[json!({"b": "work"})]);
        rig.burn("orders", 9).await;
        let messages = drain(&rig, "orders").await;
        assert_eq!(messages.len(), 1);
        assert_eq!(
            attribute(&messages[0], "ApproximateReceiveCount"),
            Some("10".to_string())
        );
        assert!(rig.fake.transactions.lock().unwrap().is_empty());
    }

    // ------------------------------------------------------------- the move

    /// The push and the ack are ONE transaction. It is the property the whole
    /// design rests on: two calls duplicate if the first lands alone and lose if
    /// the second does.
    #[tokio::test]
    async fn the_move_is_one_transaction_carrying_the_push_and_the_ack_together() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[carrying("work", 5)]);
        assert!(drain(&rig, "orders").await.is_empty());

        let transactions = rig.fake.transactions.lock().unwrap().clone();
        assert_eq!(transactions.len(), 1, "one bundle and not two calls");
        let (pushes, acks, kv) = &transactions[0];
        assert_eq!(pushes.len(), 1);
        assert_eq!(pushes[0].queue, "orders-dlq");
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].status, crate::queen::AckStatus::Completed);
        assert!(kv.is_empty(), "the move needs no key/value rider");
        // Nothing was pushed or acked outside the bundle.
        assert!(rig.fake.pushed().is_empty());
        assert!(rig.fake.acked().is_empty());
    }

    /// The copy carries the three things it cannot be reconstructed from: the
    /// original MessageId, the queue it came from, and the count to continue.
    #[tokio::test]
    async fn the_copy_records_the_original_id_its_source_and_its_count() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[json!({"b": "work"})]);
        let first = drain(&rig, "orders").await;
        let original_id = field(&first[0], "MessageId").to_string();
        rig.fake.advance(std::time::Duration::from_secs(31));
        assert!(drain(&rig, "orders").await.is_empty());

        let dead = drain(&rig, "orders-dlq").await;
        assert_eq!(dead.len(), 1);
        // A NEW id — the broker mints one per row and this facade cannot keep
        // AWS's — with the original riding in the envelope.
        assert_ne!(field(&dead[0], "MessageId"), original_id);
        let stored = rig.fake.transactions.lock().unwrap()[0].0[0]
            .payload
            .clone();
        assert_eq!(stored["m"], json!(original_id));
        assert_eq!(stored["s"][SYS_SOURCE_QUEUE], json!("orders"));
        assert_eq!(
            attribute(&dead[0], SYS_SOURCE_QUEUE),
            Some("orders".to_string()),
            "a dead-letter consumer can see where it came from"
        );
    }

    /// AWS does NOT reset the receive count on a move, and this is that rule:
    /// the copy's first delivery reports one more than the number of times a
    /// consumer saw the original.
    #[tokio::test]
    async fn the_copys_receive_count_continues_from_the_original() {
        let rig = rig_with(3).await;
        rig.fake.seed("orders", "0", 0, &[json!({"b": "work"})]);
        rig.burn("orders", 3).await;
        // The fourth delivery is the move.
        assert!(drain(&rig, "orders").await.is_empty());

        let dead = drain(&rig, "orders-dlq").await;
        assert_eq!(
            attribute(&dead[0], "ApproximateReceiveCount"),
            Some("4".to_string()),
            "three deliveries the consumer saw, plus this one"
        );
        // The carrier itself is NOT echoed beside it: two numbers describing one
        // delivery count would be a contradiction.
        assert_eq!(attribute(&dead[0], SYS_RECEIVE_COUNT), None);
    }

    #[tokio::test]
    async fn the_copy_keeps_the_body_and_every_message_attribute() {
        let rig = rig_with(1).await;
        rig.send(
            "orders",
            json!({
                "MessageBody": "payload",
                "MessageAttributes": {
                    "trace": {"DataType": "String", "StringValue": "abc"},
                    "size": {"DataType": "Number", "StringValue": "42"},
                },
            }),
        )
        .await
        .expect("sent");
        drain(&rig, "orders").await;
        rig.fake.advance(std::time::Duration::from_secs(31));
        assert!(drain(&rig, "orders").await.is_empty());

        let dead = drain(&rig, "orders-dlq").await;
        assert_eq!(dead.len(), 1);
        assert_eq!(field(&dead[0], "Body"), "payload");
        assert_eq!(dead[0]["MessageAttributes"]["trace"]["StringValue"], "abc");
        assert_eq!(dead[0]["MessageAttributes"]["size"]["DataType"], "Number");
    }

    /// A fresh transaction id on the copy, so the destination's dedup window
    /// cannot swallow the move — which is what sending the ORIGINAL dedup key
    /// would do on a second move of the same key.
    #[tokio::test]
    async fn the_copy_carries_no_dedup_key_of_its_own() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[carrying("work", 9)]);
        assert!(drain(&rig, "orders").await.is_empty());
        let pushed = rig.fake.transactions.lock().unwrap()[0].0[0].clone();
        assert_eq!(pushed.transaction_id, None);
    }

    /// The original is completed by the move, so it is never delivered again —
    /// which is the half of "moved" that a push alone would not give.
    #[tokio::test]
    async fn the_original_is_gone_from_the_source_queue() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[carrying("work", 9)]);
        assert!(drain(&rig, "orders").await.is_empty());
        rig.fake.advance(std::time::Duration::from_secs(600));
        assert!(
            drain(&rig, "orders").await.is_empty(),
            "a lease that lapsed must not bring back an acked message"
        );
        assert_eq!(drain(&rig, "orders-dlq").await.len(), 1);
    }

    /// Two moves of one message land on one lane, because the lane is chosen
    /// from the id the copy records rather than from anything per-request.
    #[tokio::test]
    async fn a_standard_copy_lands_on_the_lane_its_original_id_hashes_to() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[json!({"b": "work"})]);
        let first = drain(&rig, "orders").await;
        let original_id = field(&first[0], "MessageId").to_string();
        rig.fake.advance(std::time::Duration::from_secs(31));
        assert!(drain(&rig, "orders").await.is_empty());

        let pushed = rig.fake.transactions.lock().unwrap()[0].0[0].clone();
        assert_eq!(
            pushed.partition,
            crate::actions::messages::lane_for(&original_id, crate::actions::testing::LANES)
        );
    }

    /// A FIFO move keeps the group, because a dead-lettered message that
    /// changed group would be delivered out of order with respect to the rest of
    /// its own group on the target.
    #[tokio::test]
    async fn a_fifo_move_keeps_the_message_group() {
        let policy = redrive_policy("orders-dlq.fifo", 1);
        let rig = Rig::new(&[
            ("orders-dlq.fifo", &[]),
            ("orders.fifo", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        rig.fake
            .seed("orders.fifo", "group-a", 0, &[carrying("first", 4)]);
        assert!(drain(&rig, "orders.fifo").await.is_empty());

        let pushed = rig.fake.transactions.lock().unwrap()[0].0[0].clone();
        assert_eq!(pushed.queue, "orders-dlq.fifo");
        assert_eq!(pushed.partition, "group-a");
        let dead = drain(&rig, "orders-dlq.fifo").await;
        assert_eq!(
            attribute(&dead[0], "MessageGroupId"),
            Some("group-a".into())
        );
    }

    /// The guard that makes the move lossless whatever the claim holds: an ack
    /// completes everything below the position it names, so only the LEADING
    /// run of over-threshold messages in a claim may be moved.
    #[tokio::test]
    async fn only_the_leading_run_of_a_claim_is_moved() {
        let policy = redrive_policy("orders-dlq.fifo", 3);
        let rig = Rig::new(&[
            ("orders-dlq.fifo", &[]),
            ("orders.fifo", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        // One claim, three messages, and the middle one is under the threshold.
        rig.fake.seed(
            "orders.fifo",
            "group-a",
            0,
            &[
                carrying("over", 9),
                json!({"b": "under"}),
                carrying("also-over", 9),
            ],
        );

        let messages = rig
            .receive_list("orders.fifo", json!({"MaxNumberOfMessages": 10}))
            .await;
        let bodies: Vec<&str> = messages.iter().map(|m| field(m, "Body")).collect();
        assert_eq!(
            bodies,
            vec!["under", "also-over"],
            "the message below an undeleted one is kept, not acked past"
        );
        let dead = drain(&rig, "orders-dlq.fifo").await;
        assert_eq!(dead.len(), 1);
        assert_eq!(field(&dead[0], "Body"), "over");
    }

    /// THE STALL A SIFTED CLAIM WOULD OTHERWISE HOLD. The delete-set is written
    /// by the pop and the move happens after it, so the roster lists a message
    /// the client was never given and can never delete — and a prefix stops at
    /// the first member nobody deleted. Trimming it ([`super::fifo::forget`]) is
    /// what makes a consumer that deleted everything it received free its group
    /// at once instead of one visibility timeout later.
    #[tokio::test]
    async fn a_moved_member_does_not_block_the_deletes_of_the_rest_of_its_claim() {
        let policy = redrive_policy("orders-dlq.fifo", 3);
        let rig = Rig::new(&[
            ("orders-dlq.fifo", &[]),
            ("orders.fifo", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        rig.fake.seed(
            "orders.fifo",
            "group-a",
            0,
            &[
                carrying("over", 9),
                json!({"b": "first"}),
                json!({"b": "second"}),
            ],
        );

        let messages = rig
            .receive_list("orders.fifo", json!({"MaxNumberOfMessages": 10}))
            .await;
        let bodies: Vec<&str> = messages.iter().map(|m| field(m, "Body")).collect();
        assert_eq!(bodies, vec!["first", "second"]);

        for message in &messages {
            rig.delete("orders.fifo", field(message, "ReceiptHandle"))
                .await
                .expect("the delete lands");
        }
        let group = crate::queen::QUEUE_MODE_GROUP;
        assert_eq!(
            rig.fake.committed("orders.fifo", "group-a", group),
            Some(2),
            "the client's own deletes acked the whole claim"
        );
        assert!(
            !rig.fake.leased("orders.fifo", "group-a", group),
            "and the group is free at once, not a visibility timeout later"
        );
        assert!(rig
            .receive_list("orders.fifo", json!({"MaxNumberOfMessages": 10}))
            .await
            .is_empty());
    }

    /// The correlation a dead-letter consumer has back to the message it is
    /// holding the remains of. This facade cannot keep the original MessageId —
    /// the copy is a new row in another queue — so the id it was made from is
    /// answered as an attribute, or it is unreachable through the API.
    #[tokio::test]
    async fn a_dead_lettered_copy_names_the_message_it_was_made_from() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[json!({"b": "work"})]);
        let first = drain(&rig, "orders").await;
        let original = field(&first[0], "MessageId").to_string();
        rig.fake.advance(std::time::Duration::from_secs(31));
        assert!(drain(&rig, "orders").await.is_empty());

        let dead = drain(&rig, "orders-dlq").await;
        assert_eq!(
            attribute(&dead[0], SYS_ORIGINAL_MESSAGE_ID),
            Some(original.clone())
        );
        assert_ne!(
            field(&dead[0], "MessageId"),
            original,
            "the copy is its own message, which is why the id has to be answered"
        );
        // A message nobody moved says nothing about where it came from.
        assert_eq!(attribute(&first[0], SYS_ORIGINAL_MESSAGE_ID), None);
    }

    /// A move that did not commit is a move that did not happen — and the
    /// message is NOT handed to the client either, because its own queue's
    /// `maxReceiveCount` says this delivery must not happen.
    #[tokio::test]
    async fn a_move_that_does_not_commit_answers_nothing_and_keeps_the_message() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[carrying("work", 9)]);
        rig.fake
            .fail_transaction(crate::queen::Error::status(503, "upstream"));

        assert!(drain(&rig, "orders").await.is_empty());
        assert!(drain(&rig, "orders-dlq").await.is_empty());
        // The claim lapses and the next receive tries the move again.
        rig.fake.advance(std::time::Duration::from_secs(600));
        assert!(drain(&rig, "orders").await.is_empty());
        assert_eq!(drain(&rig, "orders-dlq").await.len(), 1);
    }

    /// A dead-letter queue deleted after the policy was written leaves the
    /// message where it is. A message acked with nowhere to go is gone, and a
    /// message delivered once more than its budget is inside SQS's own
    /// at-least-once envelope.
    #[tokio::test]
    async fn a_dead_letter_queue_that_is_gone_delivers_rather_than_loses() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[carrying("work", 9)]);
        rig.ctx
            .facade
            .registry
            .delete("orders-dlq", None)
            .await
            .expect("deleted");

        let messages = drain(&rig, "orders").await;
        assert_eq!(messages.len(), 1);
        assert_eq!(field(&messages[0], "Body"), "work");
        assert!(rig.fake.transactions.lock().unwrap().is_empty());
    }

    /// The order of a receive survives the path where nothing can be moved.
    #[tokio::test]
    async fn the_delivery_order_survives_a_target_that_is_gone() {
        let policy = redrive_policy("orders-dlq.fifo", 1);
        let rig = Rig::new(&[
            ("orders-dlq.fifo", &[]),
            ("orders.fifo", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        rig.fake.seed(
            "orders.fifo",
            "group-a",
            0,
            &[
                carrying("one", 9),
                json!({"b": "two"}),
                carrying("three", 9),
            ],
        );
        rig.ctx
            .facade
            .registry
            .delete("orders-dlq.fifo", None)
            .await
            .expect("deleted");

        let messages = rig
            .receive_list("orders.fifo", json!({"MaxNumberOfMessages": 10}))
            .await;
        let bodies: Vec<&str> = messages.iter().map(|m| field(m, "Body")).collect();
        assert_eq!(bodies, vec!["one", "two", "three"]);
    }

    // ------------------------------------------------------------- the refill

    /// A receive that moved a message asks again rather than answering short:
    /// otherwise a queue with a few exhausted messages in it delivers one
    /// message per poll.
    #[tokio::test]
    async fn a_receive_refills_after_a_move() {
        let rig = rig_with(1).await;
        rig.fake.seed("orders", "0", 0, &[carrying("dead", 9)]);
        rig.fake.seed("orders", "1", 0, &[json!({"b": "live"})]);

        let messages = rig.receive_list("orders", json!({})).await;
        assert_eq!(messages.len(), 1, "the client asked for one and gets one");
        assert_eq!(field(&messages[0], "Body"), "live");
        assert_eq!(drain(&rig, "orders-dlq").await.len(), 1);
    }

    /// ...and it is BOUNDED. A queue whose whole backlog is over the threshold
    /// must not turn one `ReceiveMessage` into a drain of the queue with the
    /// client's request held open for all of it.
    #[tokio::test]
    async fn the_refill_stops_after_two_extra_rounds() {
        let rig = rig_with(1).await;
        for lane in 0..8 {
            rig.fake
                .seed("orders", &lane.to_string(), 0, &[carrying("dead", 9)]);
        }
        let messages = rig.receive_list("orders", json!({})).await;
        assert!(messages.is_empty(), "an empty receive is legal SQS");
        assert_eq!(
            rig.fake.transactions.lock().unwrap().len(),
            MAX_FILL_ROUNDS + 1,
            "one pop round plus the bounded refills, and not the whole queue"
        );
        // The rest is still on the source, and the drain continues on the
        // client's next poll — at the client's pace and under its own timeout.
        rig.receive_list("orders", json!({})).await;
        assert_eq!(
            rig.fake.transactions.lock().unwrap().len(),
            2 * (MAX_FILL_ROUNDS + 1)
        );
    }

    // ------------------------------------------ ListDeadLetterSourceQueues

    #[tokio::test]
    async fn every_queue_whose_policy_names_this_one_is_listed() {
        let policy = redrive_policy("orders-dlq", 3);
        let other = redrive_policy("other-dlq", 3);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("other-dlq", &[]),
            ("alpha", &[("RedrivePolicy", policy.as_str())]),
            ("beta", &[("RedrivePolicy", policy.as_str())]),
            ("gamma", &[("RedrivePolicy", other.as_str())]),
            ("plain", &[]),
        ])
        .await;

        let answer = list_dead_letter_source_queues(&rig.ctx, &rig.params("orders-dlq", json!({})))
            .await
            .expect("listed");
        let urls: Vec<&str> = answer["queueUrls"]
            .as_array()
            .expect("a list")
            .iter()
            .map(|u| u.as_str().unwrap_or_default())
            .collect();
        assert_eq!(urls, vec![rig.url("alpha"), rig.url("beta")]);
        assert_eq!(answer.get("NextToken"), None);
    }

    /// A queue nothing targets answers an EMPTY list rather than no member at
    /// all: the shape is required in AWS's model, and an SDK reads the empty
    /// list as "no sources" instead of as a field the service forgot.
    #[tokio::test]
    async fn a_queue_nothing_targets_lists_an_empty_set() {
        let rig = Rig::new(&[("orders-dlq", &[])]).await;
        let answer = list_dead_letter_source_queues(&rig.ctx, &rig.params("orders-dlq", json!({})))
            .await
            .expect("listed");
        assert_eq!(answer["queueUrls"], json!([]));
    }

    /// The cursor names the last queue SCANNED. A token built from the last
    /// MATCH would resume the scan early and answer the same queues twice.
    #[tokio::test]
    async fn the_listing_pages_without_repeating_or_skipping() {
        let policy = redrive_policy("orders-dlq", 3);
        let mut queues: Vec<(String, Vec<(String, String)>)> = Vec::new();
        for i in 0..6 {
            queues.push((
                format!("src-{i}"),
                vec![("RedrivePolicy".to_string(), policy.clone())],
            ));
        }
        let specs: Vec<(&str, Vec<(&str, &str)>)> = queues
            .iter()
            .map(|(name, attributes)| {
                (
                    name.as_str(),
                    attributes
                        .iter()
                        .map(|(k, v)| (k.as_str(), v.as_str()))
                        .collect(),
                )
            })
            .collect();
        let mut layout: Vec<(&str, &[(&str, &str)])> = vec![("orders-dlq", &[])];
        for (name, attributes) in &specs {
            layout.push((name, attributes.as_slice()));
        }
        let rig = Rig::new(&layout).await;

        let mut seen: Vec<String> = Vec::new();
        let mut token: Option<String> = None;
        for _ in 0..8 {
            let mut params = json!({"MaxResults": 2});
            if let Some(cursor) = &token {
                params["NextToken"] = json!(cursor);
            }
            let answer =
                list_dead_letter_source_queues(&rig.ctx, &rig.params("orders-dlq", params))
                    .await
                    .expect("listed");
            for url in answer["queueUrls"].as_array().expect("a list") {
                seen.push(url.as_str().unwrap_or_default().to_string());
            }
            token = answer
                .get("NextToken")
                .and_then(Value::as_str)
                .map(str::to_string);
            if token.is_none() {
                break;
            }
        }
        assert_eq!(token, None, "the listing ended");
        let expected: Vec<String> = (0..6).map(|i| rig.url(&format!("src-{i}"))).collect();
        assert_eq!(seen, expected);
    }

    #[tokio::test]
    async fn the_listing_refuses_a_max_results_outside_its_range() {
        let rig = Rig::new(&[("orders-dlq", &[])]).await;
        for bad in [0, 1_001] {
            let error = list_dead_letter_source_queues(
                &rig.ctx,
                &rig.params("orders-dlq", json!({"MaxResults": bad})),
            )
            .await
            .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{bad}");
        }
    }

    #[tokio::test]
    async fn the_listing_of_a_queue_that_is_not_there_says_so() {
        let rig = Rig::standard().await;
        let error =
            list_dead_letter_source_queues(&rig.ctx, &json!({"QueueUrl": rig.url("nowhere")}))
                .await
                .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::QueueDoesNotExist);
    }

    /// The predicate `StartMessageMoveTask` is gated on.
    #[tokio::test]
    async fn a_queue_knows_whether_anything_dead_letters_to_it() {
        let policy = redrive_policy("orders-dlq", 3);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        assert!(is_dead_letter_target(&rig.ctx, "orders-dlq")
            .await
            .expect("scanned"));
        assert!(!is_dead_letter_target(&rig.ctx, "orders")
            .await
            .expect("scanned"));
    }

    // --------------------------------------------------------- the markers

    /// A restored message arrives with a fresh delivery budget: one that still
    /// carried its receive count would be dead-lettered on its first delivery,
    /// which would make every redrive a no-op.
    #[test]
    fn a_restored_envelope_keeps_its_origin_and_drops_the_move_machinery() {
        let mut envelope = Envelope::of("body");
        envelope.moved_from = Some("msg-1".to_string());
        envelope
            .system
            .insert(SYS_SOURCE_QUEUE.to_string(), "orders".to_string());
        envelope
            .system
            .insert(SYS_RECEIVE_COUNT.to_string(), "7".to_string());
        envelope
            .system
            .insert("AWSTraceHeader".to_string(), "Root=1".to_string());

        let restored = restored(envelope);
        assert_eq!(restored.moved_from, Some("msg-1".to_string()));
        assert_eq!(restored.system.get(SYS_SOURCE_QUEUE), None);
        assert_eq!(restored.system.get(SYS_RECEIVE_COUNT), None);
        assert_eq!(
            restored.system.get("AWSTraceHeader"),
            Some(&"Root=1".to_string()),
            "a client's own system attribute is not the move's to drop"
        );
    }

    #[test]
    fn the_carried_count_is_read_only_when_it_is_a_positive_number() {
        let mut envelope = Envelope::of("body");
        assert_eq!(carried_count(&envelope), 0);
        for (stored, expected) in [("7", 7), ("0", 0), ("-3", 0), ("many", 0), ("", 0)] {
            envelope
                .system
                .insert(SYS_RECEIVE_COUNT.to_string(), stored.to_string());
            assert_eq!(carried_count(&envelope), expected, "{stored}");
        }
    }
}
