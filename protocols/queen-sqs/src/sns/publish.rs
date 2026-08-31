//! `Publish` and `PublishBatch`: the fan-out.
//!
//! CONTRACT. **One publish is one `POST /api/v1/transaction` bundling one push
//! per matched subscription** (PLAN_QUEEN_SQS.md, SNS). That is stronger than
//! SNS itself promises — AWS fans out per subscriber, with per-subscriber retry
//! and per-subscriber failure — and it is the single design decision every other
//! one below serves: no subscriber of a topic can receive a message that another
//! subscriber did not, because the two pushes are one Postgres transaction
//! (005_log_ack.sql: *"All-or-nothing by construction"*).
//!
//! ## The eight decisions
//!
//! **The MessageId is the PUBLISH's, not the delivery's.** One `Publish` mints
//! one uuid; it is what the publisher is answered and it is the `MessageId`
//! inside EVERY subscriber's notification — which is AWS's own behaviour and the
//! only thing that lets a fan-out be correlated end to end. The SQS `MessageId`
//! each subscriber's own client sees is a different id, the broker's, one per
//! delivery. Both are true at AWS too.
//!
//! **A batch is one transaction PER ENTRY, not one for the batch.** SNS's
//! `PublishBatch` reports per-entry results and is explicitly not atomic across
//! entries, so the atomic unit is the publish — which is exactly the fan-out
//! above. Bundling ten entries into one transaction would make one entry's
//! refusal roll back nine messages a client was told nothing about. On a
//! STANDARD topic the entries' transactions run CONCURRENTLY inside the one
//! request, which is the whole point of a batch action; on a FIFO topic they run
//! ONE AT A TIME, in entry order, because every entry pushes to the same
//! (queue, `MessageGroupId`) lane and the broker's pre-lock over that lane gives
//! them mutual exclusion but not ORDER — whichever transaction reaches it first
//! takes the lower offset. See [`deliver_all`].
//!
//! **A wide fan-out is CHUNKED, and the atomicity is per chunk.** The broker
//! caps neither the pushes in a bundle nor the bundle's operations — there is no
//! such check in the transaction handler or in `log_transaction_wire_v1`; the
//! only ceiling upstream is the HTTP body limit (`QUEEN_MAX_BODY_BYTES`, 64 MiB
//! by default) — so [`MAX_FANOUT_PER_TRANSACTION`] and
//! [`MAX_FANOUT_BYTES_PER_TRANSACTION`] are THIS FACADE'S numbers and the
//! consequence of exceeding them is stated rather than hidden. DIVERGENCE,
//! `accepted`: past 256 matched subscriptions (or 8 MiB of payload) one publish
//! commits in more than one transaction, so a facade that dies mid-fan-out can
//! leave a prefix of the subscribers delivered. A topic that wide gets a log
//! line saying so.
//!
//! The chunks are BUILT AS THEY ARE COMMITTED and the two payload variants are
//! built once ([`Payloads`]), which is what makes those two numbers a memory
//! bound and not only a transaction bound: the resident cost of one publish is
//! one bundle's worth of payload, not one copy per matched subscription. A
//! thousand-subscriber topic materializing its whole fan-out first would hold a
//! thousand copies of a message that may be 256 KiB, times every publish in
//! flight.
//!
//! **The queues are resolved in ONE READ, and a fresh one.** The fan-out needs
//! the target queue's record per subscription; a `get` per subscription inside
//! the loop is N serial round trips with the publisher blocked on all of them,
//! so the resolution is one batched read
//! ([`crate::registry::Registry::queues_fresh`]). It deliberately bypasses the
//! registry's three-second cache — see the next decision for what a stale hit
//! would cost.
//!
//! **There is no `SequenceNumber`, on a FIFO topic or anywhere.** The
//! transaction's push echoes carry no offset BY CONSTRUCTION — the wire builds
//! them without the `baseOffset` the stored procedure returned
//! (server/src/handlers/data.rs), and `POST /api/v1/push`, which does answer one,
//! is not a transaction and would forfeit the atomic fan-out this whole module
//! exists for. The atomicity is the promise the plan makes and the
//! `SequenceNumber` is not, so the number is omitted rather than invented. If
//! the differential lane finds a client that needs it, the fix is a `baseOffset`
//! on the transaction's echoes — a broker change — and never a switch to
//! `/push`.
//!
//! **A repeated `MessageDeduplicationId` is a SUCCESS — with a FRESH
//! `MessageId`.** On a FIFO topic each delivery is filed under the client's
//! dedup key, and a duplicate inside a transaction is a HARD error that rolls
//! the bundle back ([`crate::queen::Error::Duplicate`]) where a plain push would
//! report it as a soft `duplicate` status. The publisher is answered a success,
//! because nothing needed to be written and SQS answers a repeated dedup id
//! with a success — but the id in that answer is THIS call's own uuid and not
//! the id the first publish was answered.
//!
//! DIVERGENCE, `deliberate`, and the differential lane is what settles it:
//! SQS's `SendMessage` documents returning the ORIGINAL message's id for a
//! repeated dedup id; SNS's `Publish` page does not say, and real SNS may well
//! mint a new one. Returning the original here is not a matter of reading it
//! out of the answer — the answer does not carry it, twice over. A cross-request
//! duplicate makes the stored procedure RAISE, so the broker's body is
//! `{success:false, reason:"duplicate", results:[]}` with NO echoes at all
//! (server/src/handlers/data.rs, `txn_fail_json` and the reason taxonomy above
//! it); `TxnPushEcho::duplicate` is set only for two pushes sharing a
//! `transactionId` INSIDE ONE bundle, and the id it carries is that bundle's
//! own. And even a broker that echoed the winning message id would answer the
//! wrong number: an SNS `MessageId` is this facade's uuid, written INTO the
//! notification payload, not the broker's per-delivery message uuid — recovering
//! it would mean reading the stored payload of the winning message back, which
//! no verb on this wire does. So the honest answer is a fresh id and this
//! paragraph, until the differential run says what AWS does. Pinned by
//! `tests::a_repeated_deduplication_id_is_a_success_that_writes_nothing` and
//! recorded as D1 in `compat/M4_SMOKE.md`.
//!
//! DIVERGENCE, `accepted`, at the same boundary: because the bundle is
//! all-or-nothing, a subscription created BETWEEN the two publishes receives
//! nothing — the broker refuses the whole bundle at the first duplicate, and
//! there is no per-item verdict inside a transaction to skip past.
//!
//! **A subscription whose queue this facade cannot resolve is SKIPPED**, and
//! what that avoids is NOT a rolled-back fan-out. A push naming a queue the
//! broker does not have is not refused: `log_transaction_wire_v1` inserts
//! `queen.queues` and `queen.log_partitions` for every push element before the
//! loop (005_log_ack.sql), so the queue is LAZILY PROVISIONED and the delivery
//! lands — into a Queen queue that no registry record owns. That is the
//! expensive shape: `CreateQueue` refuses to adopt such a queue for ever (the
//! guard in [`crate::actions::queues`], which the purge path has to repair by
//! hand), `ReceiveMessage` answers `QueueDoesNotExist` for it, and the messages
//! delivered there are unreachable over SQS. `QTXN resolved N of M pushes` is
//! raised only for a name that resolves to nothing at all — an empty one.
//!
//! `Subscribe` refuses an endpoint whose queue does not exist
//! ([`super::admin::subscribe`]); this is the window after it was deleted, and
//! the resolution is a FRESH batched read so that the window is the request's
//! own and not the registry cache's three seconds. DIVERGENCE, `accepted`: a
//! `DeleteQueue` that lands between that read and the commit is still a delivery
//! into a re-created orphan — the same race `SendMessage` has, unclosable
//! without a broker that refuses to provision on push.
//!
//! **The notification carries no `Signature`.** Every field of it is one this
//! deployment can stand behind — `Type`, `MessageId`, `TopicArn`, `Subject`,
//! `Message`, `Timestamp`, `SignatureVersion`, `MessageAttributes` — and the
//! three AWS also writes are not: a `Signature` nothing can verify, a
//! `SigningCertURL` whose host AWS's own validator libraries pin to
//! `sns.*.amazonaws.com` (PLAN_QUEEN_SQS.md's third non-goal), and an
//! `UnsubscribeURL` that would need a SigV4 signature to work. Queue
//! subscribers read none of the three. `SignatureVersion` stays because it names
//! the version a signature WOULD carry and because clients compare it as a
//! string; its absent companion is the honest half.
//!
//! ## One more divergence, and it is a superset
//!
//! The notification envelope is ~250 bytes LARGER than the message inside it, so
//! a publish at [`MAX_MESSAGE_BYTES`] becomes a delivery over the target queue's
//! default `MaximumMessageSize`. AWS drops that delivery to that subscriber; this
//! facade lands it, because the queue attribute is a SEND-path rule
//! ([`messages`]) and the fan-out is not a send — it is a push the broker sizes
//! against its own body limit alone. Classified `accepted`: the direction is
//! delivering a message AWS would have dropped, and the alternative is a
//! subscriber that silently receives nothing for messages near the ceiling.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use crate::actions::messages::{self, lane_for};
use crate::actions::queues::{naming, param_text, require_text};
use crate::actions::{queen_error, Ctx};
use crate::envelope::{self, AttributeValue, Envelope, MessageAttribute};
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::obs::Sampler;
use crate::queen::{self, BoxFuture, PushItem};
use crate::registry::Naming;

use super::registry::{SubscriptionRecord, FILTER_SCOPE_DEFAULT};
use super::{filter, invalid, PROTOCOL_SQS};

/// The `MessageStructure` SNS defines, and the only one.
pub const MESSAGE_STRUCTURE_JSON: &str = "json";
/// The key a `json` message falls back to when it names nothing for the
/// subscriber's protocol. AWS requires it to be present.
pub const DEFAULT_PROTOCOL_KEY: &str = "default";

/// The `Type` field of an SNS notification delivered to a queue.
pub const NOTIFICATION_TYPE: &str = "Notification";
/// The `SignatureVersion` field. See the module header on why it has no
/// `Signature` beside it.
pub const SIGNATURE_VERSION: &str = "1";

/// The most a published message may be.
///
/// SNS's own 256 KiB, which is deliberately the SAME number as a queue's default
/// `MaximumMessageSize`: a topic has no per-topic size attribute to raise, and a
/// publish that a subscriber's queue would then refuse is a message accepted
/// into a fan-out that cannot land. Where a queue was raised to 1 MiB the
/// publish ceiling stays here — the topic is the narrower of the two, and the
/// narrower one is the one a client can be told about before anything is
/// written.
pub const MAX_MESSAGE_BYTES: usize = messages::DEFAULT_MAX_MESSAGE_BYTES;

/// The longest `Subject`, AWS's own.
pub const MAX_SUBJECT_LEN: usize = 100;

/// The 10-entry cap `PublishBatch` shares with SQS's batch actions.
pub const MAX_BATCH_ENTRIES: usize = 10;

/// The most deliveries one transaction carries, and the most bytes. See the
/// module header: these are the FACADE's numbers, not the broker's.
///
/// 8 MiB is an eighth of the broker's own default body limit, so a bundle at the
/// ceiling is nowhere near it even after JSON framing, and it is what keeps one
/// publish from holding a whole cell's partition lock space for the length of a
/// 64 MiB write.
///
/// Both are also the MEMORY bound of a publish, because the chunks are built as
/// they are committed ([`deliver`]): a fan-out of any width is resident one
/// bundle at a time, not one payload copy per matched subscription.
pub const MAX_FANOUT_PER_TRANSACTION: usize = 256;
pub const MAX_FANOUT_BYTES_PER_TRANSACTION: usize = 8 * 1024 * 1024;

/// A fan-out wide enough to be chunked is a topology fact an operator has to be
/// able to see; a subscription pointing at a queue that is gone is a repair
/// somebody has to make. Both are rate-limited, because both repeat once per
/// publish.
static WIDE_FANOUT: Sampler = Sampler::new(60_000);
static UNRESOLVED_ENDPOINT: Sampler = Sampler::new(60_000);
static DEDUPLICATED: Sampler = Sampler::new(60_000);
/// A stored policy this engine cannot read is a subscription receiving nothing.
/// It is the one skip an operator cannot see from the outside, so it is the one
/// that must be in the log ([`Prepared::wanted_by`]).
static UNREADABLE_POLICY: Sampler = Sampler::new(60_000);

// ------------------------------------------------------------------- actions

/// `Publish`. Answers `{"MessageId": …}`.
///
/// A publish to a topic with NO matched subscription is a success carrying a
/// MessageId, and no transaction at all: SNS has no concept of an undeliverable
/// publish, and answering an error for one would make a filter policy that
/// matched nothing look like a broken endpoint.
pub async fn publish(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let topic = topic_of(ctx, params).await?;
    let prepared = Prepared::of(&topic, params)?;
    guard_size(&[&prepared], Batched::No)?;
    deliver(ctx, &topic, &prepared).await?;
    Ok(json!({ "MessageId": prepared.message_id }))
}

/// `PublishBatch`: up to ten publishes, each its own fan-out.
///
/// The three whole-request refusals — an empty batch, an over-long one, repeated
/// ids — happen before any entry is looked at, because they are refusals of the
/// ENVELOPE. Everything after that is per entry, including the fan-out's own
/// failure: nine topics' worth of subscribers are not lost to one entry the
/// broker would not take.
pub async fn publish_batch(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let topic = topic_of(ctx, params).await?;
    let entries = batch_entries(params)?;

    let mut outcomes: Vec<(String, SqsResult<Prepared>)> = Vec::with_capacity(entries.len());
    for (id, entry) in &entries {
        outcomes.push((id.clone(), Prepared::of(&topic, entry)));
    }
    let accepted: Vec<&Prepared> = outcomes
        .iter()
        .filter_map(|(_, result)| result.as_ref().ok())
        .collect();
    guard_size(&accepted, Batched::Yes)?;

    let mut sent = deliver_all(ctx, &topic, &accepted).await.into_iter();

    let mut successful = Vec::new();
    let mut failed = Vec::new();
    for (id, result) in &outcomes {
        let error = match result {
            Err(error) => Some(error.clone()),
            Ok(_) => sent.next().and_then(Result::err),
        };
        match error {
            None => successful.push(json!({
                "Id": id,
                "MessageId": result.as_ref().map(|p| p.message_id.clone()).unwrap_or_default(),
            })),
            Some(error) => failed.push(json!({
                "Id": id,
                "SenderFault": error.kind.fault() == crate::error::Fault::Sender,
                "Code": error.kind.json_type(),
                "Message": error.message,
            })),
        }
    }
    let mut answer = Map::new();
    // An empty list is OMITTED, which is what AWS answers: a client that reads
    // `Failed` and finds nothing there did not have a failure.
    if !successful.is_empty() {
        answer.insert("Successful".to_string(), Value::Array(successful));
    }
    if !failed.is_empty() {
        answer.insert("Failed".to_string(), Value::Array(failed));
    }
    Ok(Value::Object(answer))
}

// ------------------------------------------------------------- one publish

/// One publish, validated, with everything the fan-out needs already decided.
struct Prepared {
    /// The publish's own id — answered to the publisher AND written into every
    /// subscriber's notification. See the module header.
    message_id: String,
    /// What an `sqs` subscriber receives, after `MessageStructure` selection.
    message: String,
    subject: Option<String>,
    attributes: BTreeMap<String, MessageAttribute>,
    /// FIFO only: the partition every delivery lands on.
    group_id: Option<String>,
    /// FIFO only: the `transactionId` every delivery is filed under.
    dedup_key: Option<String>,
    /// Bytes charged against [`MAX_MESSAGE_BYTES`].
    size: usize,
    /// The two documents a filter policy is matched against, built ONCE per
    /// publish rather than once per subscription: a topic with a hundred
    /// subscribers would otherwise parse the same body a hundred times.
    attribute_document: Value,
    body_document: Option<Value>,
}

impl Prepared {
    fn of(topic: &super::registry::TopicRecord, params: &Value) -> SqsResult<Prepared> {
        let raw = require_text(params, "Message")
            .map_err(|_| invalid("Message", "the message must not be empty"))?;
        let message = select_message(raw, param_text(params, "MessageStructure"))?;
        // The XML charset, on the way IN. The selected message becomes an SQS
        // body, and a body with a NUL in it produces a Query-protocol document
        // no SDK can parse — a failure the publisher cannot see and the
        // subscriber cannot explain.
        if let Some(c) = message.chars().find(|c| !envelope::is_allowed_char(*c)) {
            return Err(invalid(
                "Message",
                format!(
                    "the message contains U+{:04X}, which is outside the character set SQS and \
                     SNS carry",
                    c as u32
                ),
            ));
        }
        let subject = subject_of(params)?;
        let attributes = message_attributes(params)?;
        let (group_id, dedup_key) = fifo_ids(topic, params, &message)?;

        let attribute_document = filter::document_of_attributes(&attributes);
        // Parsed here and never per subscription. A body that is not JSON is
        // `None`, which every `MessageBody`-scope policy answers "no match" for.
        let body_document = serde_json::from_str::<Value>(&message).ok();
        Ok(Prepared {
            message_id: uuid::Uuid::new_v4().to_string(),
            // The RAW message is what the size is charged on: with
            // `MessageStructure=json` the whole document crossed the wire, and
            // charging only the selected branch would let a client send a
            // megabyte of protocol variants inside a 256 KiB publish.
            size: size_of(raw, &attributes),
            message,
            subject,
            attributes,
            group_id,
            dedup_key,
            attribute_document,
            body_document,
        })
    }

    /// The document THIS subscription's policy is matched against.
    fn document(&self, scope: &str) -> Option<&Value> {
        match scope == FILTER_SCOPE_DEFAULT {
            true => Some(&self.attribute_document),
            false => self.body_document.as_ref(),
        }
    }

    /// Whether one subscription wants this publish.
    ///
    /// THE TWO ABSENCES ARE NOT THE SAME ABSENCE, and conflating them is how a
    /// filter is silently defeated:
    ///
    ///   * NO `FilterPolicy` attribute at all — the subscription asked for
    ///     everything, and it gets everything;
    ///   * a `FilterPolicy` that is THERE and does not parse — the subscription
    ///     asked to be filtered and this facade cannot honour the request, so it
    ///     receives NOTHING. Delivering everything instead would hand a
    ///     subscriber the messages it explicitly said it did not want, which is a
    ///     correctness failure inside its own consumer; delivering nothing is an
    ///     outage visible in the queue's own depth, and it gets a log line.
    ///
    /// It is unreachable from any request today — [`super::filter::validate`]
    /// runs at `Subscribe` and at `SetSubscriptionAttributes` — so the only ways
    /// in are a record written before that validation existed and a hand-edited
    /// key. Both are exactly when the safe reading matters.
    fn wanted_by(&self, subscription: &SubscriptionRecord) -> bool {
        if !subscription.has_filter_policy() {
            return true;
        }
        match subscription.filter_policy() {
            Some(policy) => filter::matches(&policy, self.document(subscription.filter_scope())),
            None => {
                if let Some(suppressed) = UNREADABLE_POLICY.tick_now() {
                    tracing::warn!(
                        target: "sqs",
                        suppressed,
                        topic = %subscription.topic,
                        subscription = %subscription.id,
                        "a stored FilterPolicy does not parse; the subscription receives nothing \
                         until it is set again"
                    );
                }
                false
            }
        }
    }
}

// -------------------------------------------------------------- the fan-out

/// Every entry of one batch, delivered.
///
/// CONCURRENTLY on a standard topic, which is the whole point of a batch action,
/// and ONE AT A TIME IN ENTRY ORDER on a FIFO one. Every entry of a FIFO topic
/// pushes to the same (queue, `MessageGroupId`) lane, and the broker serializes
/// those with a pre-lock (005_log_ack.sql) — which fixes mutual exclusion and
/// says nothing about ORDER: whichever of N in-flight transactions reaches the
/// lock first takes the lower offset, so a concurrent batch would hand a FIFO
/// consumer its own entries shuffled. Ordering is the only thing a FIFO topic
/// sells, so it is the thing the concurrency yields to.
async fn deliver_all(
    ctx: &Ctx,
    topic: &super::registry::TopicRecord,
    accepted: &[&Prepared],
) -> Vec<SqsResult<()>> {
    if ordered(topic) {
        let mut out = Vec::with_capacity(accepted.len());
        for prepared in accepted {
            out.push(deliver(ctx, topic, prepared).await);
        }
        return out;
    }
    let futures: Vec<BoxFuture<'_, SqsResult<()>>> = accepted
        .iter()
        .map(|prepared| Box::pin(deliver(ctx, topic, prepared)) as BoxFuture<'_, SqsResult<()>>)
        .collect();
    messages::join_all(futures).await
}

/// Whether a batch's entries are delivered ONE AT A TIME, in the order the
/// client wrote them. See [`deliver_all`]: it is the FIFO topics, and the
/// predicate is named so that the decision is a thing a test can hold.
fn ordered(topic: &super::registry::TopicRecord) -> bool {
    topic.fifo
}

/// Match, build and commit. The whole of one publish's effect.
///
/// The pushes are built and committed CHUNK BY CHUNK rather than all at once:
/// see the module header on why the resident cost of one publish is one
/// transaction's worth of payload and not the whole fan-out's.
async fn deliver(
    ctx: &Ctx,
    topic: &super::registry::TopicRecord,
    prepared: &Prepared,
) -> SqsResult<()> {
    let targets = targets(ctx, topic, prepared).await?;
    if targets.is_empty() {
        return Ok(());
    }
    let naming = naming(&ctx.facade.config);
    let mut payloads = Payloads::default();
    let mut chunk: Vec<PushItem> = Vec::new();
    let mut bytes = 0usize;
    let mut commits = 0usize;
    for target in &targets {
        let (payload, size) = payloads.of(topic, prepared, &naming, target.raw);
        let push = push_for(target, topic, prepared, payload);
        // Estimated from the payload's own JSON — measured ONCE per distinct
        // payload rather than per push — plus the two names on the item.
        let cost = size + push.queue.len() + push.partition.len();
        let full = chunk.len() >= MAX_FANOUT_PER_TRANSACTION
            || bytes + cost > MAX_FANOUT_BYTES_PER_TRANSACTION;
        if full && !chunk.is_empty() {
            commit(ctx, topic, &chunk).await?;
            commits += 1;
            chunk.clear();
            bytes = 0;
        }
        bytes += cost;
        chunk.push(push);
    }
    if !chunk.is_empty() {
        commit(ctx, topic, &chunk).await?;
        commits += 1;
    }
    if commits > 1 {
        if let Some(suppressed) = WIDE_FANOUT.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                topic = %topic.name,
                chunks = commits,
                "a topic's fan-out does not fit one transaction; it commits in chunks and a \
                 facade that dies mid-publish can leave a prefix of the subscribers delivered"
            );
        }
    }
    Ok(())
}

/// One bundle, committed.
async fn commit(
    ctx: &Ctx,
    topic: &super::registry::TopicRecord,
    chunk: &[PushItem],
) -> SqsResult<()> {
    match ctx
        .facade
        .queen
        .transaction(chunk, &[], &[], ctx.token())
        .await
    {
        Ok(_) => Ok(()),
        // Every message in the bundle was already written under this publish's
        // own dedup key. Nothing landed and nothing needed to; SQS answers a
        // repeated deduplication id with a success and so does this.
        //
        // THE WINNING ID IS NOT IN HAND HERE, and cannot be: the rolled-back
        // body carries `results: []` and no echo, and the id the caller is
        // answered is an SNS `MessageId` — this facade's uuid, minted in
        // [`Prepared::of`] and written into the notification payload — which the
        // broker never saw in the first place. The publisher therefore gets a
        // fresh id for a publish that wrote nothing. See the module header's
        // deduplication paragraph for the divergence that is, and
        // `compat/M4_SMOKE.md` D1 for the measurement.
        Err(queen::Error::Duplicate(_)) => {
            if let Some(suppressed) = DEDUPLICATED.tick_now() {
                tracing::info!(
                    target: "sqs",
                    suppressed,
                    topic = %topic.name,
                    "a publish repeated a MessageDeduplicationId inside the queue's window and \
                     wrote nothing; the original message stands"
                );
            }
            Ok(())
        }
        Err(e) => Err(queen_error(&e)),
    }
}

/// One matched subscription, resolved to what a push is built from and nothing
/// else.
///
/// Deliberately NOT the [`QueueRecord`]: a wide fan-out holds one of these per
/// subscriber for the length of the publish, and a record carries the queue's
/// whole attribute and tag maps — which the resolution needs and the delivery
/// does not.
struct Target {
    queue: String,
    partitions: u32,
    raw: bool,
}

/// Who wants this publish, and which queue each of them is.
///
/// TWO passes and ONE store call. The first pass is pure — filter policies and
/// ARN parsing, no await — and the second resolves every queue the first named
/// in a single batched read ([`crate::registry::Registry::queues_fresh`]).
/// A read per subscription inside the loop would be N serial round trips with
/// the publisher blocked on all of them, and a cached read would let a queue
/// another instance deleted stay in the fan-out.
async fn targets(
    ctx: &Ctx,
    topic: &super::registry::TopicRecord,
    prepared: &Prepared,
) -> SqsResult<Vec<Target>> {
    let subscriptions = ctx
        .facade
        .registry
        .subscriptions_cached(&topic.name, ctx.token())
        .await
        .map_err(|e| queen_error(&e))?;
    let naming = naming(&ctx.facade.config);
    let mut wanted: Vec<(&SubscriptionRecord, String)> = Vec::new();
    for subscription in subscriptions.iter() {
        // Only `sqs` is deliverable in v0 ([`super`]). A record naming another
        // protocol is one a later milestone writes, and it is skipped here
        // rather than refused: a topic that also has an HTTP subscriber must
        // keep delivering to its queues.
        if subscription.protocol != PROTOCOL_SQS || !prepared.wanted_by(subscription) {
            continue;
        }
        match naming.name_of_arn(&subscription.endpoint) {
            Some(queue) => wanted.push((subscription, queue)),
            None => unresolved(
                subscription,
                None,
                "the endpoint is not a queue ARN of this account",
            ),
        }
    }
    if wanted.is_empty() {
        return Ok(Vec::new());
    }
    let names: Vec<String> = wanted.iter().map(|(_, queue)| queue.clone()).collect();
    let records = ctx
        .facade
        .registry
        .queues_fresh(&names, ctx.token())
        .await
        .map_err(|e| queen_error(&e))?;

    let mut targets = Vec::with_capacity(wanted.len());
    for (subscription, queue) in wanted {
        // SKIPPED, not refused — see the module header on what a push naming a
        // queue the broker no longer has would do.
        let Some(record) = records.get(&queue) else {
            unresolved(subscription, Some(&queue), "its queue no longer exists");
            continue;
        };
        targets.push(Target {
            queue,
            partitions: record.partitions,
            raw: subscription.raw_message_delivery(),
        });
    }
    // ONE DELIVERY PER QUEUE on a FIFO topic, whatever the records say.
    // `Subscribe` keys its idempotency on the resolved queue precisely so this
    // cannot arise ([`super::admin::subscribe`]) — but a record written before
    // that rule existed still can, and two FIFO deliveries to one queue share a
    // (queue, `MessageGroupId`, `transactionId`): the broker refuses the second
    // as a duplicate and rolls the WHOLE bundle back, which this facade answers
    // as a success. The choice is between delivering once and delivering
    // nothing, to any subscriber, for the life of the topic.
    if topic.fifo {
        let mut seen = std::collections::HashSet::new();
        targets.retain(|target| seen.insert(target.queue.clone()));
    }
    Ok(targets)
}

/// The log line a skipped subscription gets, naming the SUBSCRIPTION and the
/// queue — the two things a repair needs. The topic alone tells an operator that
/// something on a topic with hundreds of subscribers needs fixing and nothing
/// about which one ([`UNRESOLVED_ENDPOINT`]).
///
/// The endpoint string itself is never logged: it is client-supplied text on its
/// way into this facade's log, and the subscription id addresses the record it
/// came from.
fn unresolved(subscription: &SubscriptionRecord, queue: Option<&str>, why: &str) {
    if let Some(suppressed) = UNRESOLVED_ENDPOINT.tick_now() {
        tracing::warn!(
            target: "sqs",
            suppressed,
            topic = %subscription.topic,
            subscription = %subscription.id,
            queue = queue.unwrap_or("-"),
            "a subscription was skipped at publish: {why}"
        );
    }
}

/// The push one matched subscription becomes.
///
/// The PARTITION is where the two topic types part. A FIFO topic's group id is
/// the lane on every target queue, which is what makes one publish order
/// identically for every subscriber; a standard topic hashes a fresh key across
/// the TARGET queue's own width, because widths differ per queue and a lane
/// chosen from the topic's side would name a partition the target does not have.
fn push_for(
    target: &Target,
    topic: &super::registry::TopicRecord,
    prepared: &Prepared,
    payload: Value,
) -> PushItem {
    match (topic.fifo, &prepared.group_id, &prepared.dedup_key) {
        (true, Some(group), Some(dedup)) => PushItem::deduped(&target.queue, group, payload, dedup),
        // A standard topic, which is at-least-once exactly as a standard queue
        // is: no `transactionId`, because the key would be a per-request uuid
        // that deduplicates nothing and files every message under a name nothing
        // will ask for again.
        _ => {
            let lane = lane_for(&uuid::Uuid::new_v4().to_string(), target.partitions);
            PushItem::new(&target.queue, &lane, payload)
        }
    }
}

/// The payloads one publish has, built ONCE and cloned per delivery.
///
/// There are at most two of them — the notification envelope, and the raw
/// message for subscriptions that asked for it — because neither depends on
/// WHICH subscription receives it. Rendering one per subscriber would rebuild
/// the same document for every member of a fan-out, and the SIZE is carried
/// beside it because [`deliver`] would otherwise serialize each push a second
/// time only to measure it.
#[derive(Default)]
struct Payloads {
    enveloped: Option<(Value, usize)>,
    raw: Option<(Value, usize)>,
}

impl Payloads {
    fn of(
        &mut self,
        topic: &super::registry::TopicRecord,
        prepared: &Prepared,
        naming: &Naming,
        raw: bool,
    ) -> (Value, usize) {
        let slot = match raw {
            true => &mut self.raw,
            false => &mut self.enveloped,
        };
        let built = slot.get_or_insert_with(|| {
            let payload = payload_for(topic, prepared, naming, raw).encode();
            let size = payload.to_string().len();
            (payload, size)
        });
        (built.0.clone(), built.1)
    }
}

/// What one subscriber's queue actually receives.
///
/// `RawMessageDelivery=true` is the message ALONE, with the publish's own
/// message attributes forwarded as SQS message attributes — which is the whole
/// meaning of "raw": a consumer written against a queue reads the body it was
/// sent and never knows a topic was involved.
///
/// The default is the SNS notification envelope, whose fields are the module
/// header's, as the SQS body — and NO SQS message attributes, because the
/// attributes are inside the envelope and writing them twice would let the two
/// copies disagree.
fn payload_for(
    topic: &super::registry::TopicRecord,
    prepared: &Prepared,
    naming: &Naming,
    raw: bool,
) -> Envelope {
    if raw {
        return Envelope {
            body: prepared.message.clone(),
            attributes: prepared.attributes.clone(),
            ..Envelope::default()
        };
    }
    Envelope::of(notification(topic, prepared, naming).to_string())
}

/// The SNS notification document. See the module header for the three fields
/// AWS writes and this does not.
///
/// The KEY ORDER of the rendered document is the serializer's (sorted) and not
/// AWS's declaration order, which is not a shortcut: JSON object order carries
/// no meaning, every SNS consumer parses this by field name, and the signature
/// validators that DO build a canonical string build it from the parsed fields
/// in their own fixed order — never from the document's.
fn notification(
    topic: &super::registry::TopicRecord,
    prepared: &Prepared,
    naming: &Naming,
) -> Value {
    let mut out = Map::new();
    out.insert(
        "Type".to_string(),
        Value::String(NOTIFICATION_TYPE.to_string()),
    );
    out.insert(
        "MessageId".to_string(),
        Value::String(prepared.message_id.clone()),
    );
    // Through [`super::admin::arn_of`], like every administrative answer, and
    // not the record's raw field: a record written before this facade stored an
    // ARN has an empty one, and this is the field every SNS-to-SQS consumer
    // routes on — an empty `TopicArn` in the notification while
    // `GetTopicAttributes` still answers the right one is a fault invisible from
    // the control plane.
    out.insert(
        "TopicArn".to_string(),
        Value::String(super::admin::arn_of(topic, naming)),
    );
    // Omitted rather than nulled when there is none: a consumer that finds the
    // key present and empty renders an empty subject line.
    if let Some(subject) = &prepared.subject {
        out.insert("Subject".to_string(), Value::String(subject.clone()));
    }
    out.insert(
        "Message".to_string(),
        Value::String(prepared.message.clone()),
    );
    out.insert(
        "Timestamp".to_string(),
        Value::String(crate::obs::iso8601_ms(crate::obs::now_epoch_ms())),
    );
    out.insert(
        "SignatureVersion".to_string(),
        Value::String(SIGNATURE_VERSION.to_string()),
    );
    if !prepared.attributes.is_empty() {
        let mut attributes = Map::new();
        for (name, attribute) in &prepared.attributes {
            let value = match &attribute.value {
                AttributeValue::String(text) => text.clone(),
                // Base64, which is the spelling the wire and the envelope both
                // use ([`crate::envelope`]) — a value never changes
                // representation between the publisher and the subscriber.
                AttributeValue::Binary(bytes) => {
                    use base64::Engine;
                    base64::engine::general_purpose::STANDARD.encode(bytes)
                }
            };
            attributes.insert(
                name.clone(),
                json!({"Type": attribute.data_type, "Value": value}),
            );
        }
        out.insert("MessageAttributes".to_string(), Value::Object(attributes));
    }
    Value::Object(out)
}

// ------------------------------------------------------------- the parameters

/// The topic a publish addresses, refused with SNS's own 404 when it is gone.
///
/// `TopicArn` or `TargetArn`, which is AWS's own pair. A `TargetArn` at AWS
/// usually names a mobile platform endpoint; this deployment has none, so the
/// only thing it can name here is a topic — and a `TargetArn` that is not one
/// says so rather than answering `NotFound` for an ARN of a shape this facade
/// never mints. `PhoneNumber` is refused for the same reason, by name.
async fn topic_of(ctx: &Ctx, params: &Value) -> SqsResult<super::registry::TopicRecord> {
    if param_text(params, "PhoneNumber").is_some_and(|n| !n.is_empty()) {
        return Err(invalid(
            "PhoneNumber",
            "this endpoint publishes to topics only; SMS is not implemented",
        ));
    }
    let (member, arn) = match param_text(params, "TopicArn").filter(|a| !a.is_empty()) {
        Some(arn) => ("TopicArn", arn),
        None => ("TargetArn", require_text(params, "TargetArn")?),
    };
    let name = naming(&ctx.facade.config)
        .topic_of_arn(arn)
        .ok_or_else(|| {
            invalid(
                member,
                "not a topic ARN this endpoint issued; platform endpoints are not implemented",
            )
        })?;
    Ok(ctx
        .facade
        .registry
        .require_topic(&name, ctx.token())
        .await?)
}

/// The message one `sqs` subscriber gets, after `MessageStructure` selection.
///
/// Without `MessageStructure=json` the message is the string as sent. With it,
/// the message is a JSON object of protocol names, the `sqs` entry wins, and
/// `default` is the fallback AWS requires to be there — so a publisher can send
/// one document and address every protocol from it.
fn select_message(raw: &str, structure: Option<&str>) -> SqsResult<String> {
    let Some(structure) = structure.filter(|s| !s.is_empty()) else {
        return Ok(raw.to_string());
    };
    if structure != MESSAGE_STRUCTURE_JSON {
        return Err(invalid(
            "MessageStructure",
            format!("{MESSAGE_STRUCTURE_JSON} is the only message structure SNS defines"),
        ));
    }
    let document = serde_json::from_str::<Value>(raw)
        .ok()
        .and_then(|v| v.as_object().cloned())
        .ok_or_else(|| {
            invalid(
                "Message Structure",
                "JSON message body failed to parse: it must be an object whose keys are protocol \
                 names",
            )
        })?;
    // The `default` entry must EXIST even when `sqs` is present: it is what
    // makes the same publish addressable by a protocol added later, and AWS
    // refuses a document without one.
    let fallback = document
        .get(DEFAULT_PROTOCOL_KEY)
        .and_then(Value::as_str)
        .ok_or_else(|| {
            invalid(
                "Message Structure",
                "No default entry in JSON message body: it must carry a string under \"default\"",
            )
        })?;
    match document.get(PROTOCOL_SQS) {
        None => Ok(fallback.to_string()),
        Some(Value::String(text)) => Ok(text.clone()),
        // A protocol entry that is not a string is one AWS refuses: the value is
        // the message, and a message is a string.
        Some(_) => Err(invalid(
            "Message Structure",
            "every entry of a JSON message body is a string",
        )),
    }
}

/// AWS's `Subject` rules: ASCII, printable, at most 100 characters, and not
/// starting with a space.
///
/// They are enforced rather than passed through because the subject travels into
/// the notification JSON and then into an SQS body, so a control character in it
/// is the same unparseable Query document a control character in the message is.
fn subject_of(params: &Value) -> SqsResult<Option<String>> {
    let Some(subject) = param_text(params, "Subject").filter(|s| !s.is_empty()) else {
        return Ok(None);
    };
    let shaped = subject.chars().count() <= MAX_SUBJECT_LEN
        && subject.chars().all(|c| matches!(c, ' '..='~'))
        && !subject.starts_with(' ');
    match shaped {
        true => Ok(Some(subject.to_string())),
        false => Err(invalid(
            "Subject",
            format!(
                "a subject is at most {MAX_SUBJECT_LEN} printable ASCII characters and does not \
                 begin with a space"
            ),
        )),
    }
}

/// The publish's message attributes, parsed by the SQS side's own reader.
///
/// ONE parser for both services, because the wire shape, the name rules, the
/// type labels and the value charset are the same in both — and re-spelled here
/// under SNS's own error code, which is the only thing that actually differs.
fn message_attributes(params: &Value) -> SqsResult<BTreeMap<String, MessageAttribute>> {
    messages::message_attributes(params.get("MessageAttributes"))
        .map_err(|e| invalid("MessageAttributes", e.message))
}

/// The `MessageGroupId` and the dedup key, and the four rules that pair them
/// with the topic's type.
///
/// AWS's, all four: a FIFO topic requires a group; a FIFO topic requires a
/// deduplication id unless `ContentBasedDeduplication` is on; a standard topic
/// accepts neither. The ids themselves are validated by the SQS side's rule
/// ([`messages::check_fifo_id`]) because they travel onward as a Queen partition
/// name and a `transactionId` exactly as a FIFO send's do.
fn fifo_ids(
    topic: &super::registry::TopicRecord,
    params: &Value,
    message: &str,
) -> SqsResult<(Option<String>, Option<String>)> {
    let group = param_text(params, "MessageGroupId").filter(|g| !g.is_empty());
    let dedup = param_text(params, "MessageDeduplicationId").filter(|d| !d.is_empty());
    if let Some(group) = group {
        messages::check_fifo_id("MessageGroupId", group)
            .map_err(|e| invalid("MessageGroupId", e.message))?;
    }
    if let Some(dedup) = dedup {
        messages::check_fifo_id("MessageDeduplicationId", dedup)
            .map_err(|e| invalid("MessageDeduplicationId", e.message))?;
    }
    if !topic.fifo {
        for (member, value) in [("MessageGroupId", group), ("MessageDeduplicationId", dedup)] {
            if value.is_some() {
                return Err(invalid(
                    member,
                    format!("{member} is only valid for FIFO topics"),
                ));
            }
        }
        return Ok((None, None));
    }
    let group =
        group.ok_or_else(|| invalid("MessageGroupId", "a FIFO topic requires a MessageGroupId"))?;
    let dedup = match dedup {
        Some(dedup) => dedup.to_string(),
        None if content_based(topic) => {
            hex::encode(<sha2::Sha256 as sha2::Digest>::digest(message.as_bytes()))
        }
        None => {
            return Err(invalid(
                "MessageDeduplicationId",
                "the topic should either have ContentBasedDeduplication enabled or \
                 MessageDeduplicationId provided explicitly",
            ))
        }
    };
    Ok((Some(group.to_string()), Some(dedup)))
}

fn content_based(topic: &super::registry::TopicRecord) -> bool {
    topic
        .attributes
        .get("ContentBasedDeduplication")
        .is_some_and(|v| v.eq_ignore_ascii_case("true"))
}

/// What one publish costs against [`MAX_MESSAGE_BYTES`]: the message plus every
/// attribute's name, type and value — AWS's own accounting, and the SQS side's.
fn size_of(message: &str, attributes: &BTreeMap<String, MessageAttribute>) -> usize {
    let mut size = message.len();
    for (name, attribute) in attributes {
        size += name.len() + attribute.data_type.len();
        size += match &attribute.value {
            AttributeValue::String(text) => text.len(),
            AttributeValue::Binary(bytes) => bytes.len(),
        };
    }
    size
}

/// Whether the size guard is answering for a batch, which decides which of the
/// two codes it refuses with — SNS gives the batch its own.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Batched {
    Yes,
    No,
}

/// The ceiling, for one publish or for a whole batch. AWS applies the same
/// number to both and refuses the WHOLE batch when it is exceeded, which is why
/// this is not a per-entry outcome.
fn guard_size(prepared: &[&Prepared], batched: Batched) -> SqsResult<()> {
    let total: usize = prepared.iter().map(|p| p.size).sum();
    if total <= MAX_MESSAGE_BYTES {
        return Ok(());
    }
    Err(match batched {
        Batched::Yes => SqsError::new(ErrorKind::SnsBatchRequestTooLong),
        Batched::No => invalid(
            "Message",
            format!("the message is longer than the {MAX_MESSAGE_BYTES} bytes SNS carries"),
        ),
    })
}

/// The entry list `PublishBatch` starts with, validated as `(id, entry)` pairs.
///
/// `PublishBatchRequestEntries` is the member's name in BOTH protocols — the
/// Query codec's lift table maps `PublishBatchRequestEntries.member.N` onto it —
/// which is where SNS differs from SQS's three batch actions, whose entries are
/// canonically `Entries`.
fn batch_entries(params: &Value) -> SqsResult<Vec<(String, &Value)>> {
    let entries = match params.get("PublishBatchRequestEntries") {
        Some(Value::Array(entries)) => entries.as_slice(),
        // A batch action with no entry list is an EMPTY batch and not a missing
        // parameter: AWS gives the condition an error of its own because an
        // SDK's batching helper branches on it.
        None | Some(Value::Null) => &[][..],
        Some(_) => return Err(SqsError::new(ErrorKind::SnsEmptyBatchRequest)),
    };
    let ids: Vec<String> = entries
        .iter()
        .map(|entry| param_text(entry, "Id").unwrap_or_default().to_string())
        .collect();
    check_entry_ids(&ids)?;
    Ok(ids.into_iter().zip(entries.iter()).collect())
}

/// The batch envelope's three refusals, under SNS's own codes.
///
/// The RULE is the SQS side's — same cap, same charset, same distinctness
/// requirement, and [`messages::is_entry_id`] is the one owner of the charset —
/// and only the CODES differ, because SNS spells them without SQS's service
/// prefix ([`crate::error::ErrorKind::SnsEmptyBatchRequest`]).
fn check_entry_ids(ids: &[String]) -> SqsResult<()> {
    if ids.is_empty() {
        return Err(SqsError::new(ErrorKind::SnsEmptyBatchRequest));
    }
    if ids.len() > MAX_BATCH_ENTRIES {
        return Err(SqsError::with(
            ErrorKind::SnsTooManyEntriesInBatchRequest,
            format!(
                "Maximum number of entries per request are {MAX_BATCH_ENTRIES}. You have sent {}.",
                ids.len()
            ),
        ));
    }
    for (index, id) in ids.iter().enumerate() {
        if !messages::is_entry_id(id) {
            return Err(SqsError::with(
                ErrorKind::SnsInvalidBatchEntryId,
                "A batch entry id can only contain alphanumeric characters, hyphens and \
                 underscores. It can be at most 80 letters long.",
            ));
        }
        if ids[..index].contains(id) {
            return Err(SqsError::with(
                ErrorKind::SnsBatchEntryIdsNotDistinct,
                format!("Id {id} repeated."),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::testing::{arn as queue_arn, Rig};
    use crate::sns::admin;

    const TOPIC: &str = "arn:aws:sns:queen-1:000000000000:events";
    const FIFO_TOPIC: &str = "arn:aws:sns:queen-1:000000000000:events.fifo";

    /// A rig with three standard queues and one FIFO queue, and the topics the
    /// cases subscribe them to.
    async fn rig() -> Rig {
        Rig::new(&[
            ("orders", &[]),
            ("audit", &[]),
            ("billing", &[]),
            ("orders.fifo", &[]),
        ])
        .await
    }

    async fn topic(rig: &Rig, name: &str, attributes: Value) -> String {
        let mut params = json!({ "Name": name });
        if !attributes.is_null() {
            params["Attributes"] = attributes;
        }
        admin::create_topic(&rig.ctx, &params)
            .await
            .expect("the topic is created")["TopicArn"]
            .as_str()
            .expect("an arn")
            .to_string()
    }

    async fn subscribe(rig: &Rig, topic: &str, queue: &str, attributes: Value) -> String {
        let mut params = json!({
            "TopicArn": topic,
            "Protocol": "sqs",
            "Endpoint": queue_arn(queue),
        });
        if !attributes.is_null() {
            params["Attributes"] = attributes;
        }
        admin::subscribe(&rig.ctx, &params)
            .await
            .expect("subscribed")["SubscriptionArn"]
            .as_str()
            .expect("an arn")
            .to_string()
    }

    /// Every push of every transaction, in the order they were bundled.
    ///
    /// The assertions read the PUSHES rather than receiving from the queue,
    /// deliberately: what is under test is the payload this module BUILT, and a
    /// receive would re-render it through the SQS message shape — which is the
    /// message layer's own tests' subject, not this one's.
    fn pushes(rig: &Rig) -> Vec<PushItem> {
        rig.fake
            .transactions
            .lock()
            .unwrap()
            .iter()
            .flat_map(|call| call.0.clone())
            .collect()
    }

    /// Every body delivered to one queue, in publish order.
    fn bodies(rig: &Rig, queue: &str) -> Vec<String> {
        pushes(rig)
            .iter()
            .filter(|push| push.queue == queue)
            .map(|push| Envelope::decode(&push.payload).body)
            .collect()
    }

    fn notifications(rig: &Rig, queue: &str) -> Vec<Value> {
        bodies(rig, queue)
            .iter()
            .map(|body| serde_json::from_str(body).expect("a notification is JSON"))
            .collect()
    }

    // -------------------------------------------------------------- fan-out

    /// THE property: two subscribers, one transaction, both messages in it.
    #[tokio::test]
    async fn one_publish_is_one_transaction_carrying_every_subscriber() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        subscribe(&rig, &topic, "audit", Value::Null).await;
        subscribe(&rig, &topic, "billing", Value::Null).await;

        let answer = publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        assert!(answer["MessageId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));

        let calls = rig.fake.transactions.lock().unwrap().len();
        assert_eq!(calls, 1, "the fan-out is ONE transaction");
        let pushed = pushes(&rig);
        assert_eq!(pushed.len(), 3);
        let mut queues: Vec<&str> = pushed.iter().map(|p| p.queue.as_str()).collect();
        queues.sort_unstable();
        assert_eq!(queues, vec!["audit", "billing", "orders"]);
        // ...and nothing went through the plain push path, which would be a
        // fan-out that is not atomic.
        assert!(rig.fake.pushed().is_empty());
    }

    /// The publish-level MessageId is the SAME in every subscriber's
    /// notification, and it is what the publisher was answered. That identity is
    /// the only thing that lets a fan-out be correlated end to end.
    #[tokio::test]
    async fn every_delivery_carries_the_publishs_own_message_id() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        subscribe(&rig, &topic, "audit", Value::Null).await;

        let answer = publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        let id = answer["MessageId"].as_str().expect("an id").to_string();
        for queue in ["orders", "audit"] {
            let notification = notifications(&rig, queue).remove(0);
            assert_eq!(notification["MessageId"], Value::String(id.clone()));
        }
    }

    /// A publish nothing matches is a SUCCESS with a MessageId and no
    /// transaction: SNS has no undeliverable publish.
    #[tokio::test]
    async fn a_publish_with_no_match_still_answers_a_message_id() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        // No subscription at all...
        let answer = publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        assert!(answer["MessageId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));
        assert!(rig.fake.transactions.lock().unwrap().is_empty());

        // ...and a subscription whose filter says no is the same answer.
        subscribe(
            &rig,
            &topic,
            "orders",
            json!({"FilterPolicy": r#"{"kind":["refund"]}"#}),
        )
        .await;
        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "Message": "hello",
                "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "order"}},
            }),
        )
        .await
        .expect("published");
        assert!(rig.fake.transactions.lock().unwrap().is_empty());
    }

    /// A topic that is not there is SNS's 404, and never SQS's 400.
    #[tokio::test]
    async fn publishing_to_a_topic_that_is_gone_is_not_found() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        admin::delete_topic(&rig.ctx, &json!({"TopicArn": topic}))
            .await
            .expect("deleted");

        let e = publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "x"}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::NotFound);
        assert_eq!(e.kind.http_status(), 404);
        // ...and a well-formed ARN nobody ever created is the same answer.
        assert_eq!(
            publish(
                &rig.ctx,
                &json!({"TopicArn": "arn:aws:sns:queen-1:000000000000:never", "Message": "x"})
            )
            .await
            .expect_err("refused")
            .kind,
            ErrorKind::NotFound
        );
    }

    /// A subscription whose queue was deleted is SKIPPED, and the rest of the
    /// topic keeps delivering — because a push naming a queue the broker does
    /// not have rolls the whole fan-out back.
    #[tokio::test]
    async fn a_subscription_whose_queue_is_gone_does_not_stop_the_others() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        subscribe(&rig, &topic, "audit", Value::Null).await;
        rig.ctx
            .facade
            .registry
            .delete("audit", None)
            .await
            .expect("the queue is deleted");

        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        let pushed = pushes(&rig);
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert_eq!(pushed[0].queue, "orders");
    }

    // ------------------------------------------------------------- payloads

    /// The enveloped delivery, field by field: this is the document every
    /// SNS-to-SQS consumer in the world parses.
    #[tokio::test]
    async fn the_default_delivery_is_the_sns_notification_envelope() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "Message": "hello",
                "Subject": "a subject",
                "MessageAttributes": {
                    "kind": {"DataType": "String", "StringValue": "order"},
                    "blob": {"DataType": "Binary", "BinaryValue": "AAEC"},
                },
            }),
        )
        .await
        .expect("published");

        let notification = notifications(&rig, "orders").remove(0);
        assert_eq!(notification["Type"], "Notification");
        assert_eq!(notification["TopicArn"], TOPIC);
        assert_eq!(notification["Message"], "hello");
        assert_eq!(notification["Subject"], "a subject");
        assert_eq!(notification["SignatureVersion"], "1");
        assert_eq!(
            notification["MessageAttributes"],
            json!({
                "kind": {"Type": "String", "Value": "order"},
                "blob": {"Type": "Binary", "Value": "AAEC"},
            }),
            "a binary attribute keeps the wire's own base64"
        );
        // The timestamp is SNS's shape — three fractional digits and a Z — and
        // it parses back to a time.
        let timestamp = notification["Timestamp"].as_str().expect("a timestamp");
        assert!(
            timestamp.ends_with('Z') && timestamp.len() == 24,
            "{timestamp}"
        );
        assert!(messages::epoch_ms_of(timestamp).is_some(), "{timestamp}");
        // The three fields this deployment cannot stand behind are ABSENT.
        for absent in ["Signature", "SigningCertURL", "UnsubscribeURL"] {
            assert_eq!(notification.get(absent), None, "{absent}");
        }
        // A delivery carries no SQS message attributes of its own: they are
        // inside the envelope.
        let push = pushes(&rig).remove(0);
        assert!(Envelope::decode(&push.payload).attributes.is_empty());
    }

    /// `Subject` is omitted rather than empty when there is none: a consumer
    /// that finds the key present renders an empty subject line.
    #[tokio::test]
    async fn a_notification_without_a_subject_omits_the_field() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        let notification = notifications(&rig, "orders").remove(0);
        assert_eq!(notification.get("Subject"), None);
        assert_eq!(notification.get("MessageAttributes"), None);
    }

    /// `RawMessageDelivery=true` is the message ALONE, with the attributes
    /// forwarded — a consumer written against a queue never knows a topic was
    /// involved.
    #[tokio::test]
    async fn raw_delivery_is_the_message_and_its_attributes() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(
            &rig,
            &topic,
            "orders",
            json!({"RawMessageDelivery": "true"}),
        )
        .await;
        subscribe(&rig, &topic, "audit", Value::Null).await;
        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "Message": "hello",
                "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "order"}},
            }),
        )
        .await
        .expect("published");

        let raw = pushes(&rig)
            .into_iter()
            .find(|p| p.queue == "orders")
            .expect("the raw subscriber was delivered to");
        let envelope = Envelope::decode(&raw.payload);
        assert_eq!(envelope.body, "hello", "the body is the message itself");
        assert_eq!(
            envelope.attributes.get("kind"),
            Some(&MessageAttribute::string("String", "order"))
        );
        // ...and the OTHER subscriber, on the same publish, got the envelope.
        assert_eq!(notifications(&rig, "audit").remove(0)["Message"], "hello");
    }

    // -------------------------------------------------------------- filters

    /// The filter policy decides per subscription, inside ONE publish: the
    /// grammar itself is tested in [`super::super::filter`], and what is under
    /// test here is that the right subscribers get the message.
    #[tokio::test]
    async fn a_filter_policy_selects_which_subscribers_are_in_the_transaction() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(
            &rig,
            &topic,
            "orders",
            json!({"FilterPolicy": r#"{"kind":["order"]}"#}),
        )
        .await;
        subscribe(
            &rig,
            &topic,
            "audit",
            json!({"FilterPolicy": r#"{"kind":["order","refund"]}"#}),
        )
        .await;
        subscribe(&rig, &topic, "billing", Value::Null).await;

        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "Message": "m",
                "MessageAttributes": {"kind": {"DataType": "String", "StringValue": "refund"}},
            }),
        )
        .await
        .expect("published");

        let mut queues: Vec<String> = pushes(&rig).into_iter().map(|p| p.queue).collect();
        queues.sort();
        assert_eq!(
            queues,
            vec!["audit", "billing"],
            "orders filters on a kind this publish is not"
        );
        assert_eq!(rig.fake.transactions.lock().unwrap().len(), 1);
    }

    /// `MessageBody` scope parses the MESSAGE, and a message that is not JSON
    /// matches no body-scope policy at all.
    #[tokio::test]
    async fn a_body_scope_policy_reads_the_message_and_not_the_attributes() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(
            &rig,
            &topic,
            "orders",
            json!({
                "FilterPolicy": r#"{"customer":{"tier":["gold"]}}"#,
                "FilterPolicyScope": "MessageBody",
            }),
        )
        .await;

        publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": r#"{"customer":{"tier":"gold"}}"#}),
        )
        .await
        .expect("published");
        assert_eq!(pushes(&rig).len(), 1);

        publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": r#"{"customer":{"tier":"silver"}}"#}),
        )
        .await
        .expect("published");
        assert_eq!(pushes(&rig).len(), 1, "the second publish matched nothing");

        // A body that is not JSON matches nothing — AWS's own rule.
        publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": "plain text"}),
        )
        .await
        .expect("published");
        assert_eq!(pushes(&rig).len(), 1);
    }

    /// THE two absences, told apart. A subscription with no policy takes
    /// everything; one whose stored policy cannot be read asked to be filtered
    /// and receives NOTHING, because delivering everything to it would hand a
    /// consumer the messages it said it did not want.
    ///
    /// The bad value is written straight through the registry, past the
    /// validating action, because no request can produce it any more.
    #[tokio::test]
    async fn a_stored_policy_that_cannot_be_read_delivers_nothing() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        let subscription = subscribe(&rig, &topic, "orders", Value::Null).await;
        let (name, id) = crate::registry::Naming::new("queen-1", "000000000000")
            .subscription_of_arn(&subscription)
            .expect("our own arn");

        // No policy at all: everything.
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        assert_eq!(pushes(&rig).len(), 1);

        for stored in ["not json", "[\"an array\"]", "7"] {
            let changes = [("FilterPolicy".to_string(), Some(stored.to_string()))]
                .into_iter()
                .collect();
            rig.ctx
                .facade
                .registry
                .set_subscription_attributes(&name, &id, &changes, None)
                .await
                .expect("written past the validating action");
            rig.ctx.facade.registry.forget_subscriptions(&name);
            publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
                .await
                .expect("the publish still succeeds");
            assert_eq!(pushes(&rig).len(), 1, "{stored} delivered something");
        }

        // ...and clearing it takes the subscription back to everything.
        let cleared = [("FilterPolicy".to_string(), None)].into_iter().collect();
        rig.ctx
            .facade
            .registry
            .set_subscription_attributes(&name, &id, &cleared, None)
            .await
            .expect("cleared");
        rig.ctx.facade.registry.forget_subscriptions(&name);
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        assert_eq!(pushes(&rig).len(), 2);
    }

    // ----------------------------------------------------------------- fifo

    /// A FIFO topic propagates the group id as the lane and the deduplication id
    /// as the `transactionId`, on every delivery of the publish.
    #[tokio::test]
    async fn a_fifo_publish_carries_the_group_and_the_dedup_id_onto_every_queue() {
        let rig = rig().await;
        let topic = topic(&rig, "events.fifo", json!({"FifoTopic": "true"})).await;
        assert_eq!(topic, FIFO_TOPIC);
        subscribe(&rig, &topic, "orders.fifo", Value::Null).await;

        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "Message": "hello",
                "MessageGroupId": "customer-1",
                "MessageDeduplicationId": "d-1",
            }),
        )
        .await
        .expect("published");
        let push = pushes(&rig).remove(0);
        assert_eq!(push.queue, "orders.fifo");
        assert_eq!(push.partition, "customer-1");
        assert_eq!(push.transaction_id.as_deref(), Some("d-1"));
    }

    /// The four pairing rules, each AWS's own.
    #[tokio::test]
    async fn the_fifo_parameters_are_paired_with_the_topics_type() {
        let rig = rig().await;
        let standard = topic(&rig, "events", Value::Null).await;
        let fifo = topic(&rig, "events.fifo", json!({"FifoTopic": "true"})).await;

        // A FIFO topic requires a group...
        let e = publish(&rig.ctx, &json!({"TopicArn": fifo, "Message": "m"}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert!(e.message.contains("MessageGroupId"), "{}", e.message);
        // ...and a deduplication id, unless the topic makes one from the body.
        let e = publish(
            &rig.ctx,
            &json!({"TopicArn": fifo, "Message": "m", "MessageGroupId": "g"}),
        )
        .await
        .expect_err("refused");
        assert!(
            e.message.contains("ContentBasedDeduplication"),
            "{}",
            e.message
        );
        // A standard topic accepts neither.
        for member in ["MessageGroupId", "MessageDeduplicationId"] {
            let mut params = json!({"TopicArn": standard, "Message": "m"});
            params[member] = json!("value");
            let e = publish(&rig.ctx, &params).await.expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameter);
            assert!(e.message.contains(member), "{}", e.message);
        }
    }

    /// `ContentBasedDeduplication` makes the dedup key the SHA-256 of the
    /// message, so two publishes of one body are one message.
    #[tokio::test]
    async fn content_based_deduplication_keys_on_the_message() {
        let rig = rig().await;
        let topic = topic(
            &rig,
            "events.fifo",
            json!({"FifoTopic": "true", "ContentBasedDeduplication": "true"}),
        )
        .await;
        subscribe(&rig, &topic, "orders.fifo", Value::Null).await;
        publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": "hello", "MessageGroupId": "g"}),
        )
        .await
        .expect("published");
        let push = pushes(&rig).remove(0);
        assert_eq!(
            push.transaction_id.as_deref(),
            Some(&hex::encode(<sha2::Sha256 as sha2::Digest>::digest(b"hello"))[..])
        );
    }

    /// A repeat inside the dedup window is a SUCCESS that wrote nothing, which
    /// is what SQS answers for a repeated deduplication id — and what a client
    /// retrying a timed-out publish depends on.
    ///
    /// DIVERGENCE, `deliberate`, PINNED HERE: the id in that answer is the
    /// SECOND call's own, not the id the first publish was answered. The module
    /// header carries the argument — the rolled-back transaction answers no
    /// echoes at all, and an SNS `MessageId` is this facade's uuid rather than
    /// anything the broker stores under the dedup key — and `compat/M4_SMOKE.md`
    /// D1 carries the live measurement and the question the differential lane
    /// has to put to real SNS. Whichever way that answer goes, this test is the
    /// one that changes.
    #[tokio::test]
    async fn a_repeated_deduplication_id_is_a_success_that_writes_nothing() {
        let rig = rig().await;
        let topic = topic(&rig, "events.fifo", json!({"FifoTopic": "true"})).await;
        subscribe(&rig, &topic, "orders.fifo", Value::Null).await;
        let params = json!({
            "TopicArn": topic,
            "Message": "hello",
            "MessageGroupId": "g",
            "MessageDeduplicationId": "d-1",
        });
        let first = publish(&rig.ctx, &params).await.expect("published");
        assert_eq!(rig.fake.lane("orders.fifo", "g").len(), 1);

        let repeat = json!({
            "TopicArn": topic,
            // A DIFFERENT message under the same key: what is delivered has to be
            // the first one, so that "wrote nothing" is proved and not assumed.
            "Message": "the duplicate, which must not be delivered",
            "MessageGroupId": "g",
            "MessageDeduplicationId": "d-1",
        });
        let second = publish(&rig.ctx, &repeat)
            .await
            .expect("the repeat is a success");
        assert_eq!(
            rig.fake.lane("orders.fifo", "g").len(),
            1,
            "nothing was written the second time"
        );

        let delivered = notifications(&rig, "orders.fifo").remove(0);
        assert_eq!(delivered["Message"], "hello", "the FIRST publish stands");
        // The id the subscriber can see is the first publish's, which is the
        // whole of what "one publish is one MessageId" promises…
        assert_eq!(delivered["MessageId"], first["MessageId"]);
        // …and the id the second PUBLISHER was answered is neither that one nor
        // anything a subscriber ever saw. That is the divergence, in one line.
        assert_ne!(
            second["MessageId"], first["MessageId"],
            "a fresh uuid, because the rolled-back transaction carries no echo to \
             learn the winning id from"
        );
        assert!(second["MessageId"]
            .as_str()
            .is_some_and(|id| id.len() == 36));
    }

    // ------------------------------------------------------ message structure

    /// `MessageStructure=json` selects the `sqs` entry, falls back to `default`,
    /// and refuses a document without one.
    #[tokio::test]
    async fn a_json_message_structure_selects_the_sqs_entry() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;

        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "MessageStructure": "json",
                "Message": r#"{"default":"for everyone","sqs":"for queues","email":"ignored"}"#,
            }),
        )
        .await
        .expect("published");
        assert_eq!(
            notifications(&rig, "orders").remove(0)["Message"],
            "for queues"
        );

        // No `sqs` entry: the `default` is what a queue gets.
        publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "MessageStructure": "json",
                "Message": r#"{"default":"for everyone","email":"ignored"}"#,
            }),
        )
        .await
        .expect("published");
        assert_eq!(
            notifications(&rig, "orders").remove(1)["Message"],
            "for everyone"
        );
    }

    #[tokio::test]
    async fn a_json_message_structure_is_refused_when_it_cannot_be_selected_from() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        for (message, structure, expect) in [
            (r#"{"sqs":"x"}"#, "json", "No default entry"),
            ("not json", "json", "failed to parse"),
            (r#"["default"]"#, "json", "failed to parse"),
            (r#"{"default":7}"#, "json", "No default entry"),
            (r#"{"default":"x","sqs":7}"#, "json", "is a string"),
            (r#"{"default":"x"}"#, "xml", "only message structure"),
        ] {
            let e = publish(
                &rig.ctx,
                &json!({
                    "TopicArn": topic,
                    "Message": message,
                    "MessageStructure": structure,
                }),
            )
            .await
            .expect_err(message);
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{message}");
            assert!(e.message.contains(expect), "{message}: {}", e.message);
        }
    }

    // ------------------------------------------------------------ parameters

    /// Every refusal a bare `Publish` carries, each `InvalidParameter` and each
    /// naming its member.
    #[tokio::test]
    async fn the_publish_parameters_are_refused_as_sns_refuses_them() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        let cases: &[(Value, &str)] = &[
            (json!({"TopicArn": topic, "Message": ""}), "Message"),
            (json!({"TopicArn": topic}), "Message"),
            (json!({"TopicArn": topic, "Message": "a\u{0}b"}), "U+0000"),
            (
                json!({"TopicArn": topic, "Message": "m", "Subject": "with\nnewline"}),
                "Subject",
            ),
            (
                json!({"TopicArn": topic, "Message": "m", "Subject": "x".repeat(101)}),
                "Subject",
            ),
            (
                json!({"TopicArn": topic, "Message": "m", "Subject": " leading space"}),
                "Subject",
            ),
            (
                json!({"TopicArn": "arn:aws:sqs:queen-1:000000000000:orders", "Message": "m"}),
                "TopicArn",
            ),
            (
                json!({"TargetArn": "not-an-arn", "Message": "m"}),
                "TargetArn",
            ),
            (
                json!({"PhoneNumber": "+15551234", "Message": "m"}),
                "PhoneNumber",
            ),
            (
                json!({
                    "TopicArn": topic,
                    "Message": "m",
                    "MessageAttributes": {"AWS.reserved": {"DataType": "String", "StringValue": "x"}},
                }),
                "MessageAttributes",
            ),
        ];
        for (params, expect) in cases {
            let e = publish(&rig.ctx, params).await.expect_err(expect);
            assert_eq!(e.kind, ErrorKind::InvalidParameter, "{params}");
            assert!(e.message.contains(expect), "{params}: {}", e.message);
        }
        // A `TargetArn` that IS a topic ARN is accepted, which is the half of
        // the pair a client actually uses against a self-hosted endpoint.
        assert!(
            publish(&rig.ctx, &json!({"TargetArn": topic, "Message": "m"}))
                .await
                .is_ok()
        );
    }

    /// The size ceiling, and the fact that `MessageStructure=json` is charged on
    /// the WHOLE document rather than the branch a queue receives.
    #[tokio::test]
    async fn a_message_over_the_ceiling_is_refused_on_what_crossed_the_wire() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        let e = publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": "x".repeat(MAX_MESSAGE_BYTES + 1)}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameter);
        assert!(e.message.contains("longer than"), "{}", e.message);

        // A short `sqs` branch inside a long document is still a long publish.
        let document = format!(
            r#"{{"default":"short","sqs":"short","email":"{}"}}"#,
            "x".repeat(MAX_MESSAGE_BYTES)
        );
        assert!(publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": document, "MessageStructure": "json"}),
        )
        .await
        .is_err());
        // ...and exactly at the ceiling is accepted.
        assert!(publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": "x".repeat(MAX_MESSAGE_BYTES)}),
        )
        .await
        .is_ok());
    }

    // ----------------------------------------------------------------- batch

    /// A batch is per-entry: one bad entry is one `Failed` row and the other
    /// nine are published, each in its own transaction.
    #[tokio::test]
    async fn a_batch_reports_per_entry_and_publishes_the_rest() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        let answer = publish_batch(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "PublishBatchRequestEntries": [
                    {"Id": "a", "Message": "first"},
                    {"Id": "b", "Message": ""},
                    {"Id": "c", "Message": "third"},
                ],
            }),
        )
        .await
        .expect("answered");

        let successful = answer["Successful"].as_array().expect("a list");
        assert_eq!(successful.len(), 2);
        assert_eq!(successful[0]["Id"], "a");
        assert_eq!(successful[1]["Id"], "c");
        assert!(successful
            .iter()
            .all(|e| e["MessageId"].as_str().is_some_and(|id| !id.is_empty())));
        let failed = answer["Failed"].as_array().expect("a list");
        assert_eq!(failed.len(), 1);
        assert_eq!(failed[0]["Id"], "b");
        assert_eq!(failed[0]["Code"], "InvalidParameter");
        assert_eq!(failed[0]["SenderFault"], true);

        // Two entries, two transactions — the atomic unit is the publish.
        assert_eq!(rig.fake.transactions.lock().unwrap().len(), 2);
        let mut delivered = bodies(&rig, "orders")
            .iter()
            .map(|body| {
                serde_json::from_str::<Value>(body).expect("json")["Message"]
                    .as_str()
                    .expect("a message")
                    .to_string()
            })
            .collect::<Vec<_>>();
        delivered.sort();
        assert_eq!(delivered, vec!["first", "third"]);
    }

    /// The batch ENVELOPE's three refusals, under SNS's own codes — the ones
    /// without SQS's `AWS.SimpleQueueService.` prefix.
    #[tokio::test]
    async fn the_batch_envelope_is_refused_under_snss_own_codes() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        let entries = |n: usize| -> Value {
            (0..n)
                .map(|i| json!({"Id": format!("id{i}"), "Message": "m"}))
                .collect()
        };
        for (payload, kind) in [
            (json!([]), ErrorKind::SnsEmptyBatchRequest),
            (entries(11), ErrorKind::SnsTooManyEntriesInBatchRequest),
            (
                json!([{"Id": "a", "Message": "m"}, {"Id": "a", "Message": "m"}]),
                ErrorKind::SnsBatchEntryIdsNotDistinct,
            ),
            (
                json!([{"Id": "not a valid id", "Message": "m"}]),
                ErrorKind::SnsInvalidBatchEntryId,
            ),
        ] {
            let e = publish_batch(
                &rig.ctx,
                &json!({"TopicArn": topic, "PublishBatchRequestEntries": payload}),
            )
            .await
            .expect_err("refused");
            assert_eq!(e.kind, kind, "{payload}");
            assert!(
                !e.kind.query_code().starts_with("AWS."),
                "{:?} carries SQS's service prefix into an SNS answer",
                e.kind
            );
        }
        // A batch with no entry member at all is an EMPTY batch.
        assert_eq!(
            publish_batch(&rig.ctx, &json!({"TopicArn": topic}))
                .await
                .expect_err("refused")
                .kind,
            ErrorKind::SnsEmptyBatchRequest
        );
        // The whole batch's size is one message's ceiling, and it refuses the
        // batch rather than an entry.
        let long: Value = (0..2)
            .map(|i| json!({"Id": format!("id{i}"), "Message": "x".repeat(MAX_MESSAGE_BYTES)}))
            .collect();
        assert_eq!(
            publish_batch(
                &rig.ctx,
                &json!({"TopicArn": topic, "PublishBatchRequestEntries": long})
            )
            .await
            .expect_err("refused")
            .kind,
            ErrorKind::SnsBatchRequestTooLong
        );
    }

    /// A batch entry's own fan-out failure is that ENTRY's failure, not the
    /// request's.
    #[tokio::test]
    async fn a_batch_entry_whose_transaction_fails_is_one_failed_row() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        rig.fake
            .fail_transaction(queen::Error::status(503, "upstream"));
        let answer = publish_batch(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "PublishBatchRequestEntries": [
                    {"Id": "a", "Message": "first"},
                    {"Id": "b", "Message": "second"},
                ],
            }),
        )
        .await
        .expect("answered");
        // One scripted failure, so exactly one entry fails and the other lands.
        assert_eq!(answer["Failed"].as_array().map(Vec::len), Some(1));
        assert_eq!(answer["Successful"].as_array().map(Vec::len), Some(1));
        assert_eq!(answer["Failed"][0]["SenderFault"], false);
    }

    /// A queue deleted through ANOTHER instance is skipped by this one, whose
    /// own cache still holds the record.
    ///
    /// The skip is not tidiness: the broker's transaction lazily PROVISIONS a
    /// queue a push names, so the delivery would re-create a Queen queue no
    /// registry record owns — which `CreateQueue` then refuses to adopt for ever
    /// and `ReceiveMessage` answers `QueueDoesNotExist` for, with the delivered
    /// messages unreachable. Two instances behind one load balancer is the
    /// normal deployment, so the resolution is a fresh read.
    #[tokio::test]
    async fn a_queue_another_instance_deleted_is_not_delivered_to() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        subscribe(&rig, &topic, "audit", Value::Null).await;
        // The first publish is what warms this instance's queue cache.
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "one"}))
            .await
            .expect("published");
        assert_eq!(pushes(&rig).len(), 2);

        // ...and the delete goes through a SIBLING, so nothing invalidates the
        // entry this instance is holding.
        rig.sibling()
            .ctx
            .facade
            .registry
            .delete("audit", None)
            .await
            .expect("the queue is deleted");

        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "two"}))
            .await
            .expect("published");
        let after: Vec<String> = pushes(&rig)[2..].iter().map(|p| p.queue.clone()).collect();
        assert_eq!(after, vec!["orders".to_string()]);
    }

    /// Every notification's `TopicArn` is the one the administrative reads
    /// answer, including for a record written before this facade stored one.
    ///
    /// It is the field an SNS-to-SQS consumer routes on, so an empty one is a
    /// fault that is invisible from the control plane — `GetTopicAttributes`
    /// would still answer the right ARN.
    #[tokio::test]
    async fn a_notification_carries_the_arn_even_when_the_record_stored_none() {
        let rig = rig().await;
        let record = super::super::registry::TopicRecord {
            name: "legacy".to_string(),
            ..Default::default()
        };
        rig.ctx
            .facade
            .registry
            .create_topic(&record, None)
            .await
            .expect("stored")
            .expect("claimed");
        let arn = "arn:aws:sns:queen-1:000000000000:legacy";
        subscribe(&rig, arn, "orders", Value::Null).await;

        publish(&rig.ctx, &json!({"TopicArn": arn, "Message": "hello"}))
            .await
            .expect("published");
        assert_eq!(notifications(&rig, "orders").remove(0)["TopicArn"], arn);
    }

    /// A queue that appears TWICE among a FIFO topic's subscriptions is
    /// delivered to once.
    ///
    /// `Subscribe` cannot create the second record any more, but one written
    /// before that rule existed still resolves here — and two FIFO deliveries to
    /// one queue share a (queue, group, dedup id), which the broker refuses as a
    /// duplicate and rolls the whole bundle back. Delivering once beats
    /// delivering nothing to anybody.
    #[tokio::test]
    async fn a_fifo_queue_subscribed_twice_is_delivered_to_once() {
        let rig = rig().await;
        let topic = topic(&rig, "events.fifo", json!({"FifoTopic": "true"})).await;
        subscribe(&rig, &topic, "orders.fifo", Value::Null).await;
        // The record `Subscribe` refuses to write, written directly: the same
        // queue under a second id.
        let legacy = super::super::registry::SubscriptionRecord {
            topic: "events.fifo".to_string(),
            id: "00000000-0000-4000-8000-0000000000ff".to_string(),
            protocol: PROTOCOL_SQS.to_string(),
            endpoint: queue_arn("orders.fifo"),
            ..Default::default()
        };
        rig.ctx
            .facade
            .registry
            .create_subscription(&legacy, None)
            .await
            .expect("stored");
        rig.ctx.facade.registry.forget_subscriptions("events.fifo");

        publish(
            &rig.ctx,
            &json!({"TopicArn": topic, "Message": "hello",
                    "MessageGroupId": "g", "MessageDeduplicationId": "d"}),
        )
        .await
        .expect("published");
        let pushed = pushes(&rig);
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert_eq!(bodies(&rig, "orders.fifo").len(), 1);
    }

    /// A FIFO topic's batch is delivered IN ENTRY ORDER, one transaction after
    /// another. Its entries share a message group, the broker's pre-lock gives
    /// them mutual exclusion and not order, and ordering is the only thing a
    /// FIFO topic sells.
    #[tokio::test]
    async fn a_fifo_batch_is_delivered_one_entry_at_a_time_in_order() {
        let rig = rig().await;
        let topic = topic(&rig, "events.fifo", json!({"FifoTopic": "true"})).await;
        subscribe(&rig, &topic, "orders.fifo", Value::Null).await;
        let entries: Vec<Value> = ["one", "two", "three"]
            .iter()
            .enumerate()
            .map(|(i, message)| {
                json!({"Id": format!("e{i}"), "Message": message,
                       "MessageGroupId": "g", "MessageDeduplicationId": format!("d{i}")})
            })
            .collect();
        let answer = publish_batch(
            &rig.ctx,
            &json!({"TopicArn": topic, "PublishBatchRequestEntries": entries}),
        )
        .await
        .expect("answered");
        assert_eq!(answer["Successful"].as_array().map(Vec::len), Some(3));
        assert!(answer.get("Failed").is_none());

        let delivered: Vec<String> = notifications(&rig, "orders.fifo")
            .iter()
            .map(|n| n["Message"].as_str().expect("a message").to_string())
            .collect();
        assert_eq!(delivered, vec!["one", "two", "three"]);
        // ...and the decision itself, so a refactor that hands a FIFO batch to
        // the concurrent path fails here rather than in production.
        let fifo = super::super::registry::TopicRecord {
            fifo: true,
            ..Default::default()
        };
        assert!(ordered(&fifo));
        assert!(!ordered(&super::super::registry::TopicRecord::default()));
    }

    // ------------------------------------------------------------- chunking

    /// A rig with `width` numbered standard queues, all subscribed to one topic.
    /// The topic's ARN comes back with it.
    async fn wide_rig(width: usize) -> (Rig, String) {
        let names: Vec<String> = (0..width).map(|i| format!("q{i}")).collect();
        let specs: Vec<(&str, &[(&str, &str)])> =
            names.iter().map(|name| (name.as_str(), &[][..])).collect();
        let rig = Rig::new(&specs).await;
        let topic = topic(&rig, "events", Value::Null).await;
        for name in &names {
            subscribe(&rig, &topic, name, Value::Null).await;
        }
        (rig, topic)
    }

    /// A fan-out wider than one transaction carries COMMITS IN MORE THAN ONE,
    /// and every subscriber is still delivered to exactly once. The atomicity is
    /// per chunk, which the module header states out loud.
    #[tokio::test]
    async fn a_wide_fanout_commits_in_bundles_and_delivers_to_every_subscriber() {
        let width = MAX_FANOUT_PER_TRANSACTION + 2;
        let (rig, topic) = wide_rig(width).await;
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");

        let calls = rig.fake.transactions.lock().unwrap();
        assert_eq!(calls.len(), 2, "one bundle at the cap, one for the rest");
        assert_eq!(calls[0].0.len(), MAX_FANOUT_PER_TRANSACTION);
        assert_eq!(calls[1].0.len(), 2);
        drop(calls);

        let mut queues: Vec<String> = pushes(&rig).iter().map(|p| p.queue.clone()).collect();
        queues.sort();
        queues.dedup();
        assert_eq!(queues.len(), width, "every subscriber, exactly once");
    }

    /// The BYTE bound closes a bundle before the count one does: thirty-four
    /// subscribers of a message at the publish ceiling are more than the eight
    /// mebibytes one transaction carries.
    #[tokio::test]
    async fn a_heavy_fanout_closes_a_bundle_on_bytes() {
        let width = 34;
        let (rig, topic) = wide_rig(width).await;
        let message = "x".repeat(MAX_MESSAGE_BYTES - 1);
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": message}))
            .await
            .expect("published");

        let calls = rig.fake.transactions.lock().unwrap();
        assert!(
            calls.len() > 1,
            "{width} deliveries of {MAX_MESSAGE_BYTES} bytes share no bundle"
        );
        assert!(calls.iter().all(|call| call.0.len() < width));
        assert_eq!(calls.iter().map(|call| call.0.len()).sum::<usize>(), width);
    }

    /// The whole fan-out resolves its queues in ONE read, whatever its width.
    ///
    /// A read per subscription would be N serial round trips inside one client
    /// request, with the publisher blocked on every one of them.
    #[tokio::test]
    async fn a_fanout_resolves_every_queue_in_one_read() {
        let (rig, topic) = wide_rig(40).await;
        rig.fake.kv_calls.lock().unwrap().clear();
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        let calls = rig.fake.kv_calls.lock().unwrap();
        assert_eq!(
            calls.len(),
            3,
            "the topic, the subscription scan, and ONE read for all forty queues"
        );
        assert!(
            matches!(calls[2].as_slice(), [crate::queen::KvOp::GetMany { keys, .. }] if keys.len() == 40),
            "{:?}",
            calls[2]
        );
    }

    // ----------------------------------------------------------------- cache

    /// The subscription list is cached, and a `Subscribe` through THIS instance
    /// clears its own entry — the provision-then-publish sequence every
    /// framework performs at start-up.
    #[tokio::test]
    async fn a_subscription_made_here_is_visible_to_the_very_next_publish() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "one"}))
            .await
            .expect("published");
        assert_eq!(pushes(&rig).len(), 1);

        subscribe(&rig, &topic, "audit", Value::Null).await;
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "two"}))
            .await
            .expect("published");
        assert_eq!(
            pushes(&rig).len(),
            3,
            "the new subscriber is in the fan-out"
        );

        // ...and an Unsubscribe is visible immediately too.
        let listed = admin::list_subscriptions_by_topic(&rig.ctx, &json!({"TopicArn": topic}))
            .await
            .expect("listed");
        let arn = listed["Subscriptions"][0]["SubscriptionArn"]
            .as_str()
            .expect("an arn")
            .to_string();
        admin::unsubscribe(&rig.ctx, &json!({"SubscriptionArn": arn}))
            .await
            .expect("unsubscribed");
        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "three"}))
            .await
            .expect("published");
        assert_eq!(pushes(&rig).len(), 4);
    }

    /// A second instance over the same store publishes to a subscription it
    /// never saw created: the list is in KV, and a facade holds no state anyone
    /// would miss.
    #[tokio::test]
    async fn another_instance_publishes_to_a_subscription_it_never_saw() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        let sibling = rig.sibling();
        publish(
            &sibling.ctx,
            &json!({"TopicArn": topic, "Message": "hello"}),
        )
        .await
        .expect("published");
        assert_eq!(pushes(&rig).len(), 1);
    }

    // ----------------------------------------------------- the two divergences

    /// A FIFO publish answers NO `SequenceNumber`, and this test is the record
    /// of why: the transaction's push echoes carry no offset by construction,
    /// and `POST /api/v1/push` — which does answer one — is not a transaction.
    /// The atomic fan-out is the promise; the sequence number is not.
    #[tokio::test]
    async fn a_fifo_publish_answers_no_sequence_number() {
        let rig = rig().await;
        let topic = topic(&rig, "events.fifo", json!({"FifoTopic": "true"})).await;
        subscribe(&rig, &topic, "orders.fifo", Value::Null).await;
        let answer = publish(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "Message": "hello",
                "MessageGroupId": "g",
                "MessageDeduplicationId": "d",
            }),
        )
        .await
        .expect("published");
        assert_eq!(answer.get("SequenceNumber"), None, "{answer}");
        assert!(answer["MessageId"]
            .as_str()
            .is_some_and(|id| !id.is_empty()));
        // ...and neither does a batch entry, for the same reason.
        let batch = publish_batch(
            &rig.ctx,
            &json!({
                "TopicArn": topic,
                "PublishBatchRequestEntries": [{
                    "Id": "a", "Message": "m",
                    "MessageGroupId": "g", "MessageDeduplicationId": "d2",
                }],
            }),
        )
        .await
        .expect("answered");
        assert_eq!(
            batch["Successful"][0].get("SequenceNumber"),
            None,
            "{batch}"
        );
    }

    /// A fan-out that does not commit delivers to NOBODY — which is the whole
    /// promise of the one transaction, tested on the path that breaks it.
    #[tokio::test]
    async fn a_fanout_that_does_not_commit_delivers_to_nobody() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        subscribe(&rig, &topic, "audit", Value::Null).await;
        rig.fake
            .fail_transaction(queen::Error::status(503, "upstream"));

        let e = publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect_err("refused");
        // A Receiver fault, so an SDK's own retry policy sends it again.
        assert_eq!(e.kind, ErrorKind::ServiceUnavailable);
        assert_eq!(e.kind.fault(), crate::error::Fault::Receiver);
        for queue in ["orders", "audit"] {
            for lane in 0..crate::actions::testing::LANES {
                assert!(
                    rig.fake.lane(queue, &lane.to_string()).is_empty(),
                    "{queue} lane {lane} took a message from a rolled-back fan-out"
                );
            }
        }
    }

    /// A subscription with a protocol this milestone does not deliver is SKIPPED
    /// and the queues keep receiving. It is a record M6 writes, not one any
    /// action here can create, so the test writes it through the registry.
    #[tokio::test]
    async fn a_subscription_of_another_protocol_is_skipped() {
        let rig = rig().await;
        let topic = topic(&rig, "events", Value::Null).await;
        subscribe(&rig, &topic, "orders", Value::Null).await;
        let http = crate::sns::registry::SubscriptionRecord {
            topic: "events".to_string(),
            id: "an-http-subscriber".to_string(),
            protocol: "https".to_string(),
            endpoint: "https://example.invalid/hook".to_string(),
            ..crate::sns::registry::SubscriptionRecord::default()
        };
        rig.ctx
            .facade
            .registry
            .create_subscription(&http, None)
            .await
            .expect("written");
        rig.ctx.facade.registry.forget_subscriptions("events");

        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        let pushed = pushes(&rig);
        assert_eq!(pushed.len(), 1, "{pushed:?}");
        assert_eq!(pushed[0].queue, "orders");
    }

    /// A fan-out wider than one bundle, end to end: every subscriber is
    /// delivered to, and it took more than one transaction to do it. The
    /// atomicity is per chunk, which is the divergence the module header states.
    #[tokio::test]
    async fn a_fanout_past_the_bundle_cap_delivers_everything_in_more_than_one_transaction() {
        let names: Vec<String> = (0..MAX_FANOUT_PER_TRANSACTION + 1)
            .map(|i| format!("q{i}"))
            .collect();
        let queues: Vec<(&str, &[(&str, &str)])> =
            names.iter().map(|name| (name.as_str(), &[][..])).collect();
        let rig = Rig::new(&queues).await;
        let topic = topic(&rig, "events", Value::Null).await;
        for name in &names {
            subscribe(&rig, &topic, name, Value::Null).await;
        }

        publish(&rig.ctx, &json!({"TopicArn": topic, "Message": "hello"}))
            .await
            .expect("published");
        assert_eq!(
            rig.fake.transactions.lock().unwrap().len(),
            2,
            "one subscriber past the cap is one more bundle"
        );
        let pushed = pushes(&rig);
        assert_eq!(
            pushed.len(),
            names.len(),
            "every subscriber was delivered to"
        );
        let mut delivered: Vec<String> = pushed.into_iter().map(|p| p.queue).collect();
        delivered.sort();
        delivered.dedup();
        assert_eq!(delivered.len(), names.len(), "and each exactly once");
    }
}
