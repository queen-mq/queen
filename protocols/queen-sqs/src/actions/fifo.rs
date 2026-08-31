//! FIFO queues: one claim is one message group, and the delete-set that makes an
//! out-of-order delete safe.
//!
//! CONTRACT. A `.fifo` queue is NATIVE here — nothing is synthesized. The
//! `MessageGroupId` IS the Queen partition name, so the three properties SQS
//! sells are the broker's own and not this facade's emulation of them:
//!
//!   * **order within a group** is the order of a partition's offsets;
//!   * **group-blocked-while-in-flight** is the partition claim: one lease per
//!     `(partition, consumer group)`, so a second consumer cannot be served the
//!     group a first consumer holds;
//!   * **deduplication** is the `transactionId` and the queue's own dedup
//!     window.
//!
//! ## One receive is ONE pop of ONE lane
//!
//! `ReceiveMessage` on a FIFO queue is a single `pop` with
//! `batch = MaxNumberOfMessages` and `partitions = 1` — never the N parallel
//! `batch=1` pops a standard queue uses. Ordering within a group only means
//! anything if the whole run of messages is claimed by ONE consumer, and a pop
//! that spread its batch over k lanes would hand k groups to one caller while
//! claiming each of them for the whole visibility window.
//!
//! DIVERGENCE, `accepted`: AWS fills a receive from AS MANY groups as it can,
//! so a caller asking for ten messages against a hundred one-message groups
//! gets ten from AWS and one from here. It is a SHORT receive and never a wrong
//! one — `MaxNumberOfMessages` is a ceiling and never a promise, which is the
//! same sentence that covers a short standard receive — and the client polls
//! again. Closing it is C-SQS-1 (`maxPerPartition` on pop: claim k lanes, cap
//! the frames taken from each), which is the broker change PLAN_QUEEN_SQS.md
//! scopes for the `amortized` dial; without it, one pop per group is the only
//! shape that keeps a group's messages together under one lease.
//!
//! ## The delete-set: why a mid-batch delete may not be acked
//!
//! Queen's ack is a CURSOR and SQS's delete is a message. The stored procedure
//! is explicit about what that costs (005_log_ack.sql): *"acking position P
//! advances the cursor to the MAX acked-ok position in the leased range — every
//! earlier UNACKED position is below the cursor and is completed, never
//! redelivered"*. So a client that receives five messages and deletes the third
//! would, on a naive facade, silently destroy the first two. THE ACK IS
//! THEREFORE NEVER THE CLIENT'S MESSAGE — it is the last message of the
//! contiguous DELETED PREFIX, and the same sentence of the same procedure makes
//! that one ack complete the whole prefix ("ack the last message of the batch
//! completes the whole batch"), so the prefix costs one call and not one per
//! member.
//!
//! What the delete-set holds is the rest: the members that were deleted OUT of
//! order and cannot be acked yet. It lives in Queen's key/value store under
//! `qs:ds:<partitionId>:<leaseId>` and never in this process, because the
//! instance that serves the delete is not the instance that served the receive
//! — that is the sentence PLAN_QUEEN_SQS.md opens with, and a delete-set in
//! memory would break it on the second replica. Its TTL is the visibility
//! timeout plus [`TTL_SLACK_SECONDS`], and a `ChangeMessageVisibility` that
//! extends the lease extends the record with it.
//!
//! ## The redelivery, and the honest boundary of duplicate suppression
//!
//! If an earlier message is never deleted, its lease expires and the whole
//! suffix redelivers — including members the client already deleted. Every
//! receive therefore reads the delete-sets recorded for the lane it just
//! claimed, and for each redelivered message that a previous lease had marked:
//!
//!   * it is NOT returned to the client (a message the client deleted is not a
//!     message it should see again), and
//!   * it is carried into the new claim's record, so it is acked the moment the
//!     gap below it closes.
//!
//! A DELETE IS RECORDED AGAINST THE BROKER'S MESSAGE UUID and never against the
//! dedup key the client deleted by, and that is the load-bearing decision of
//! this module rather than bookkeeping taste. A `MessageDeduplicationId` is
//! unique only inside the queue's dedup window (`queen.dedupWindowSeconds`,
//! which a client may set as low as one second), so it identifies a MESSAGE
//! nowhere else:
//!
//!   * ONE CLAIM CAN HOLD TWO MESSAGES UNDER ONE KEY — the same key sent again
//!     after the window is a new message at a new offset, and a claim over a run
//!     of the group covers both. A record keyed by the key would mark both from
//!     one delete, open a prefix over a message nobody deleted, and ack it away.
//!   * A RECORD OUTLIVES THE WINDOW whenever the visibility timeout does (the
//!     window is 300 seconds by default and a visibility may be twelve hours),
//!     so a mark by key would suppress — and ack, unseen — the next message
//!     accepted under that key.
//!
//! The uuid is the broker's, per message, and never reused, so neither is
//! reachable. It rides in the receipt handle ([`crate::handle`]) so the delete
//! that arrives at another instance carries it.
//!
//! The boundary, stated rather than hidden: suppression works when the marks
//! are still there to read. They are dropped by their own TTL, by a KV write
//! this facade could not land, and by a `getPrefix` page this lane overflowed
//! ([`MAX_PRIOR_SETS`]) — after any of those the message comes back to the
//! client as a duplicate. The same boundary covers the one thing two messages
//! under one dedup key still cost after the record itself is uuid-keyed: the
//! ACK names a key, so it commits as far as the earlier namesake and the tail of
//! a batch the client finished can redeliver once (see [`delete_claim`]). That is SQS's own at-least-once envelope and is
//! classified `accepted`; what it is NOT is data loss, which is the failure the
//! prefix rule above exists to make impossible.
//!
//! DIVERGENCE, `accepted`, for the same reason and at the same boundary: a
//! `ReceiveRequestAttemptId` replays the answer it recorded for as long as the
//! record lives ([`replay`]), where AWS replays it only *"if none of the
//! messages have been modified"*. A client that received under an attempt id,
//! deleted the messages and then retried that id inside the visibility window is
//! handed handles for messages that are gone — deletes under them answer success
//! and delete nothing, which is exactly what AWS answers for a stale handle
//! (module header of [`super::messages`]). Closing it would mean naming the
//! attempt id in every delete-set so a delete could invalidate it, which is a
//! second key on the hot path to make a retry-after-delete — a client bug —
//! answer differently.
//!
//! ## SequenceNumber, and where it is not
//!
//! A FIFO `SendMessage` answers the absolute offset the push allocated — the
//! push wire carries it (C1) — and a FIFO RECEIVE now answers the same number
//! for the same message: `render_pop_parts` (server/src/handlers/data.rs) emits
//! an `"offset"` per delivered message (C-SQS-3), which is
//! [`crate::queen::Message::offset`] and which
//! [`super::messages::system_view`] renders as `SequenceNumber`. The send-side
//! and receive-side numbers are the same value read twice, so a client can
//! correlate the two.
//!
//! It is absent only against a pre-C-SQS-3 broker — a facade deployed beside an
//! older one answers `MessageGroupId` and `MessageDeduplicationId` exactly and
//! `SequenceNumber` not at all. It is NOT synthesized in that case: a number
//! derived from the transaction id or from the delivery position would order
//! two messages of a group differently from the way the log does, and a wrong
//! SequenceNumber is worse than an absent one for the only thing a client reads
//! it for.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::actions::messages::{
    DEFAULT_VISIBILITY_SECONDS, MAX_RECEIVE_MESSAGES, MAX_VISIBILITY_SECONDS,
};
use crate::actions::Ctx;
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::handle::Receipt;
use crate::obs::Sampler;
use crate::queen::{self, AckItem, KvOp, PopOptions};
use crate::registry::{QueueRecord, Registry, NS};

/// How much longer than the visibility timeout a delete-set outlives its own
/// claim. The record exists to be read by the delete of a message that is still
/// in flight and by the receive that redelivers it, so it must not expire
/// FIRST: the slack covers the clock skew between this facade and the broker
/// and the round trip of the delete that arrives at the last second.
pub const TTL_SLACK_SECONDS: u64 = 60;

/// Delete-sets one receive reads for the lane it claimed. One per previous
/// lease of that lane still inside its TTL, so the honest number is one or two;
/// the limit is what stops a lane that churned leases for a whole visibility
/// window from turning a receive into a paged scan. Overflowing it costs
/// duplicate suppression, never correctness (module header).
pub const MAX_PRIOR_SETS: i64 = 64;

/// Attempts at the compare-and-set that records a delete. The registry's
/// number, for the registry's reason: a third would be an unbounded loop under
/// exactly the contention that produced it, and the honest answer to "two other
/// instances beat me to this record" is a retriable code the SDK backs off on.
const CAS_ATTEMPTS: usize = 2;

/// The largest receive answer a `ReceiveRequestAttemptId` remembers. The store
/// refuses a value over [`queen::MAX_KV_VALUE_BYTES`] outright, so the ceiling
/// is enforced here — with room for the JSON around the messages — rather than
/// discovered as a failed write on a request that has already claimed messages.
pub const MAX_REMEMBERED_BYTES: usize = 48 * 1024;

/// The marks of a claim, under `qs:ds:<partitionId>:<leaseId>`.
const KEY_MEMBERS: &str = "m";
const KEY_MESSAGE_IDS: &str = "i";
const KEY_DELETED: &str = "d";
const KEY_TTL: &str = "t";

/// A receive whose delete-set could not be rewritten with what an earlier lease
/// had recorded: the client sees duplicates it already deleted, which is legal
/// and worth a line.
static MARKS_NOT_CARRIED: Sampler = Sampler::new(10_000);
/// The auto-ack of a redelivered, already-deleted prefix failing. Nothing is
/// lost — the marks are recorded and the next redelivery tries again — so this
/// is the only place it is visible.
static AUTO_ACK_FAILED: Sampler = Sampler::new(10_000);
/// A receive answer too large to remember for its `ReceiveRequestAttemptId`.
static ATTEMPT_TOO_LARGE: Sampler = Sampler::new(60_000);
/// A roster that could not be trimmed to what the client holds after a
/// dead-letter move ([`forget`]).
static ROSTER_NOT_TRIMMED: Sampler = Sampler::new(10_000);
/// A lane whose recorded delete-sets did not fit one page.
static PRIOR_SETS_TRUNCATED: Sampler = Sampler::new(60_000);

// --------------------------------------------------------------- the record

/// One claim's delete-set: what was delivered, in order, and which of it the
/// client has deleted.
///
/// `members` is the ORDER, and it is the whole reason the record exists: the
/// delete that arrives at another instance carries a receipt handle naming one
/// message, and nothing in that handle says which messages came before it.
/// Without the roster there is no prefix to compute, and without the prefix an
/// ack is a cursor jump over messages nobody deleted.
///
/// The two arrays are the two identities one delivered message has, and the
/// module header is where the difference matters: `members` holds the DEDUP
/// KEYS, which is what an ack addresses a position by, and `ids` holds the
/// broker's MESSAGE UUIDS, which is what a delete is recorded against because a
/// dedup key names a message nowhere outside its own window.
#[derive(Debug, Clone, Default, PartialEq)]
struct DeleteSet {
    members: Vec<String>,
    /// The broker's message uuids, positionally aligned with `members`. Empty
    /// when a record could not carry them, which acks nothing (see
    /// [`DeleteSet::prefix`]).
    ids: Vec<String>,
    /// The MESSAGE UUIDS of everything the client has deleted from this claim.
    deleted: BTreeSet<String>,
    /// Seconds the record is written with, carried in the record itself so that
    /// a DELETE — which knows a transaction id and nothing about the receive's
    /// visibility timeout — rewrites it with the window it was created for
    /// instead of guessing one.
    ttl_seconds: u64,
}

impl DeleteSet {
    /// A record read back. LENIENT in the same way the registry's is: a field
    /// this facade cannot read leaves an EMPTY roster rather than a refusal,
    /// and an empty roster is the safe direction — it acks nothing.
    fn from_value(value: &Value, fallback_ttl: u64) -> DeleteSet {
        let strings = |name: &str| -> Vec<String> {
            value
                .get(name)
                .and_then(Value::as_array)
                .map(|items| {
                    items
                        .iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .unwrap_or_default()
        };
        let members = strings(KEY_MEMBERS);
        let ids = strings(KEY_MESSAGE_IDS);
        DeleteSet {
            // Misaligned is UNKNOWN, never guessed: a mark that named the wrong
            // message would suppress the wrong message.
            ids: match ids.len() == members.len() {
                true => ids,
                false => Vec::new(),
            },
            members,
            deleted: strings(KEY_DELETED).into_iter().collect(),
            ttl_seconds: value
                .get(KEY_TTL)
                .and_then(Value::as_u64)
                .filter(|t| *t > 0)
                .unwrap_or(fallback_ttl),
        }
    }

    fn to_value(&self) -> Value {
        serde_json::json!({
            KEY_MEMBERS: self.members,
            KEY_MESSAGE_IDS: self.ids,
            KEY_DELETED: self.deleted.iter().collect::<Vec<_>>(),
            KEY_TTL: self.ttl_seconds,
        })
    }

    /// Everything this record says was deleted, in the form the next delivery of
    /// this lane matches against: message uuids, which is the only form a mark
    /// takes (module header).
    fn add_marks(&self, into: &mut Marks) {
        for id in &self.deleted {
            into.ids.insert(id.clone());
        }
    }

    /// How many members from the front are deleted — the prefix that CAN be
    /// acked, and the count that has already been acked by whoever closed it.
    ///
    /// It walks `ids` and not `members`: two members of one claim can share a
    /// dedup key, and a prefix computed over the keys would step over the
    /// namesake the client did NOT delete (module header). A record whose ids
    /// could not be read walks nothing and acks nothing, which is the safe
    /// direction.
    fn prefix(&self) -> usize {
        self.ids
            .iter()
            .position(|id| !self.deleted.contains(id))
            .unwrap_or(self.ids.len())
            .min(self.members.len())
    }

    /// The first member that is NOT deleted: the head of what the broker still
    /// holds, and the ONLY transaction id a release may name (see
    /// [`terminate`]).
    fn head(&self) -> Option<&str> {
        self.members.get(self.prefix()).map(String::as_str)
    }

    /// Every member deleted, so the record has nothing left to say and the ack
    /// that closed it released the lease.
    fn complete(&self) -> bool {
        !self.members.is_empty() && self.prefix() == self.members.len()
    }

    fn mark(&mut self, message_id: &str) {
        self.deleted.insert(message_id.to_string());
    }

    /// Drop members this claim no longer holds, in the ONE case where that is
    /// not the client's business: a redrive move that committed
    /// ([`super::dlq::sift`]) acked those messages under this very lease, and a
    /// roster that still listed them would stall every prefix behind a member
    /// nobody can ever delete.
    ///
    /// Removed rather than marked deleted: a mark travels into the next
    /// delivery's suppression, and a message that was moved is a message this
    /// lane will never deliver again.
    fn forget(&mut self, moved: &BTreeSet<&str>) {
        // Nothing of this claim was moved, or the record carries no ids to
        // recognize them by — either way the roster stands as written.
        if !self.ids.iter().any(|id| moved.contains(id.as_str())) {
            return;
        }
        let members = std::mem::take(&mut self.members);
        let ids = std::mem::take(&mut self.ids);
        for (member, id) in members.into_iter().zip(ids) {
            if moved.contains(id.as_str()) {
                continue;
            }
            self.members.push(member);
            self.ids.push(id);
        }
    }
}

/// What the earlier leases of one lane recorded as deleted: message uuids, the
/// only form a mark takes (module header).
#[derive(Debug, Default)]
struct Marks {
    ids: BTreeSet<String>,
}

impl Marks {
    /// Whether this message is one the client has already deleted.
    fn covers(&self, message: &queen::Message) -> bool {
        self.ids.contains(&message.id)
    }

    fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }
}

// -------------------------------------------------------------- receiving

/// `ReceiveMessage` on a FIFO queue: one pop of one lane, minus whatever a
/// previous lease already deleted.
///
/// The order of the three steps is the contract. The pop takes the claim; the
/// SAME key/value call reads the lane's earlier delete-sets and writes this
/// claim's roster, so the common case — a lane nobody has half-deleted — costs
/// exactly one KV round trip; only when something WAS recorded does the record
/// get rewritten with the marks and the freed prefix acked.
pub async fn receive(
    ctx: &Ctx,
    record: &QueueRecord,
    wanted: usize,
    visibility: i64,
    wait: i64,
) -> SqsResult<Vec<queen::Message>> {
    let opts = PopOptions {
        // ONE lane, `wanted` deep: the module header's first decision.
        batch: wanted.clamp(1, MAX_RECEIVE_MESSAGES as usize) as i32,
        partitions: 1,
        lease_seconds: visibility.clamp(0, i64::from(MAX_VISIBILITY_SECONDS)) as i32,
        wait: wait > 0,
        timeout_ms: (wait.max(0) as u64 * 1_000).min(queen::MAX_POP_TIMEOUT_MS),
        consumer_group: None,
    };
    let popped = ctx
        .facade
        .queen
        .pop_queue(&record.name, &opts, ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    let Some(partition_id) = popped.messages.first().map(|m| m.partition_id.clone()) else {
        return Ok(Vec::new());
    };
    let lease_id = popped.lease_id.clone();
    if lease_id.is_empty() {
        // An unleased delivery (the broker's autoAck render) has nothing to ack
        // and nothing to release, so a delete-set would describe a claim nobody
        // holds — and its key, ending in an empty lease id, is the prefix every
        // OTHER record of this lane is read by. This facade never asks for one;
        // the guard is what keeps that from being an assumption.
        return Ok(popped.messages);
    }
    let ttl = ttl_of(visibility);
    let key = Registry::key_delete_set(&partition_id, &lease_id);
    let mut set = DeleteSet {
        members: popped
            .messages
            .iter()
            .map(|m| m.transaction_id.clone())
            .collect(),
        ids: popped.messages.iter().map(|m| m.id.clone()).collect(),
        deleted: BTreeSet::new(),
        ttl_seconds: ttl,
    };

    let ops = [
        KvOp::get_prefix(
            NS,
            &Registry::key_delete_sets(&partition_id),
            MAX_PRIOR_SETS,
            None,
        ),
        KvOp::put_ttl(NS, &key, set.to_value(), ttl, None),
    ];
    let answers = ctx
        .facade
        .queen
        .kv(&ops, ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    let recorded = prior_marks(answers.first(), &key, &partition_id);
    if recorded.is_empty() {
        return Ok(popped.messages);
    }
    for message in &popped.messages {
        if recorded.covers(message) {
            set.mark(&message.id);
        }
    }
    if set.deleted.is_empty() {
        return Ok(popped.messages);
    }

    // Something in this claim was deleted under an earlier lease. Carry the
    // marks into THIS claim's record — a write that fails only costs the
    // suppression, never the messages — and ack whatever prefix they close.
    let version = answers.get(1).map_or(0, |a| a.version);
    let written = ctx
        .facade
        .queen
        .kv(
            &[KvOp::put_ttl(NS, &key, set.to_value(), ttl, Some(version))],
            ctx.token(),
        )
        .await;
    if !written.is_ok_and(|a| a.first().is_some_and(queen::KvAnswer::applied)) {
        if let Some(suppressed) = MARKS_NOT_CARRIED.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                queue = %record.name,
                "a redelivered claim kept no record of what an earlier lease deleted; those \
                 messages redeliver once more"
            );
        }
    }
    let prefix = set.prefix();
    if prefix > 0 {
        let answered = ctx
            .facade
            .queen
            .ack(
                &AckItem::completed(&set.members[prefix - 1], &partition_id, &lease_id),
                None,
                ctx.token(),
            )
            .await;
        // Nothing is lost when this does not land: the marks are recorded, the
        // lease expires, and the next redelivery acks them.
        let landed = answered.as_ref().is_ok_and(|a| a.success);
        if !landed {
            if let Some(suppressed) = AUTO_ACK_FAILED.tick_now() {
                tracing::debug!(
                    target: "sqs",
                    suppressed,
                    queue = %record.name,
                    "the redelivered prefix a previous lease deleted was not acked"
                );
            }
        }
    }
    Ok(popped
        .messages
        .into_iter()
        .filter(|m| !set.deleted.contains(&m.id))
        .collect())
}

/// What every delete-set of this lane records as deleted, in the two forms a
/// mark can take ([`Marks`]).
///
/// Our OWN key is skipped: the read and the write of the roster travel in one
/// call, and this must not depend on which of the two the store applies first.
fn prior_marks(answer: Option<&queen::KvAnswer>, own_key: &str, partition_id: &str) -> Marks {
    let Some(answer) = answer else {
        return Marks::default();
    };
    if answer.truncated {
        if let Some(suppressed) = PRIOR_SETS_TRUNCATED.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                partition_id,
                "more than {MAX_PRIOR_SETS} delete-sets on one lane; older ones are not read"
            );
        }
    }
    let mut out = Marks::default();
    for row in &answer.rows {
        if row.key == own_key {
            continue;
        }
        DeleteSet::from_value(&row.value, 0).add_marks(&mut out);
    }
    out
}

// --------------------------------------------------------------- deleting

/// `DeleteMessage` / `DeleteMessageBatch` on a FIFO queue, for receipts that may
/// name any members of any claims, in any order.
///
/// Answers one result per receipt, in the caller's own order. Receipts of the
/// SAME claim are applied in ONE read-modify-write: ten entries of one batch are
/// one record, and applying them one at a time would make this facade compete
/// with itself for its own compare-and-set.
pub async fn delete(ctx: &Ctx, receipts: &[&Receipt]) -> Vec<SqsResult<()>> {
    let mut claims: BTreeMap<(&str, &str), Vec<usize>> = BTreeMap::new();
    for (index, receipt) in receipts.iter().enumerate() {
        claims
            .entry((&receipt.partition_id, &receipt.lease_id))
            .or_default()
            .push(index);
    }
    let calls: Vec<_> = claims
        .iter()
        .map(|((partition_id, lease_id), members)| {
            // The MESSAGE UUIDS, which is what a delete is recorded against —
            // the handle's transaction id names a position only in company with
            // the roster (module header).
            let ids: Vec<String> = members
                .iter()
                .map(|i| receipts[*i].message_id.clone())
                .collect();
            Box::pin(async move {
                let done = delete_claim(ctx, partition_id, lease_id, &ids).await;
                (members, done)
            }) as queen::BoxFuture<'_, (&Vec<usize>, SqsResult<()>)>
        })
        .collect();

    let mut out: Vec<SqsResult<()>> = receipts.iter().map(|_| Ok(())).collect();
    for (members, done) in super::messages::join_all(calls).await {
        for index in members {
            out[*index] = done.clone();
        }
    }
    out
}

/// Record the deletes of ONE claim and ack whatever prefix they close.
///
/// THE ORDER IS THE POINT: the record is written FIRST and the ack second, and
/// never in one transaction — a transaction whose ack is rejected rolls the
/// whole bundle back (005_log_ack.sql), which would turn a delete over a lease
/// that has just expired into a failed request instead of AWS's own documented
/// "the request succeeds, but the message might not be deleted". Written first
/// and acked second, a facade that dies in between has recorded the client's
/// intent: the message redelivers and [`receive`] acks it on the way through.
async fn delete_claim(
    ctx: &Ctx,
    partition_id: &str,
    lease_id: &str,
    message_ids: &[String],
) -> SqsResult<()> {
    let key = Registry::key_delete_set(partition_id, lease_id);
    for _ in 0..CAS_ATTEMPTS {
        let (mut set, version) = load(ctx, &key).await?;
        let before = set.prefix();
        for message_id in message_ids {
            set.mark(message_id);
        }
        let after = set.prefix();
        // A record whose every member is deleted has nothing left to answer: the
        // ack below completes the batch and releases the lease, so the row would
        // only sit there until its TTL — and a lane whose consumer keeps up
        // would then accumulate one dead row per claim, which is what would
        // eventually overflow [`MAX_PRIOR_SETS`] and cost the suppression that
        // rows exist for. The one thing dropping it early can cost is a
        // redelivery: if the ack below then fails, the batch comes back with no
        // marks to suppress it. Duplicates, not loss — the safe direction.
        let op = match set.complete() {
            true => KvOp::delete(NS, &key, Some(version)),
            false => KvOp::put_ttl(NS, &key, set.to_value(), set.ttl_seconds, Some(version)),
        };
        let answer = ctx
            .facade
            .queen
            .kv(&[op], ctx.token())
            .await
            .map_err(|e| super::queen_error(&e))?;
        if !answer.first().is_some_and(queen::KvAnswer::applied) {
            continue;
        }
        if after > before {
            // ONE ack for the whole prefix: acking its last member completes
            // every member below it (module header). A prefix that did not grow
            // is a delete out of order — recorded, not acked, and closed by
            // whichever delete fills the gap.
            //
            // The ack names a DEDUP KEY, and a claim holding two messages under
            // one key resolves it to the LOWEST occurrence still above the
            // cursor (005_log_ack.sql: `eff = MIN(voff) FILTER (voff >= lo)`) —
            // which is at or below the position this prefix reached and never
            // above it, so the commit stays inside the run the client deleted.
            // What it can be is SHORT: the cursor stops at the earlier namesake
            // and the tail of a completed batch redelivers as duplicates. Inside
            // SQS's own envelope, and the alternative — one ack per member —
            // costs a round trip per message on every FIFO delete.
            let ack = AckItem::completed(&set.members[after - 1], partition_id, lease_id);
            let answered = ctx
                .facade
                .queen
                .ack(&ack, None, ctx.token())
                .await
                .map_err(|e| super::queen_error(&e))?;
            super::messages::report_delete(&answered);
        }
        return Ok(());
    }
    Err(SqsError::with(
        ErrorKind::ServiceUnavailable,
        "Concurrent deletes on the same batch of messages; please retry.",
    ))
}

// ------------------------------------------------------------- visibility

/// `ChangeMessageVisibility(0)` for a member of a FIFO claim.
///
/// DIVERGENCE, `accepted`: AWS returns THAT message to the queue; this returns
/// the whole remainder of the claim. It is not a choice between two behaviors —
/// the lease covers a contiguous span of the group and there is no verb that
/// releases one message of one — and returning the group's tail early is inside
/// SQS's at-least-once envelope, which is what the FIFO consumer that reads the
/// remainder again is already written for.
///
/// The release names the claim's HEAD and never the caller's own message, and
/// that is the difference between a divergence and a data loss: a `retry` ack
/// commits everything strictly below the position it names (005_log_ack.sql,
/// "explicit signals are never skipped"), so releasing at the third member of a
/// batch would silently complete the first two — messages nobody deleted.
pub async fn terminate(ctx: &Ctx, receipt: &Receipt) -> SqsResult<()> {
    let key = Registry::key_delete_set(&receipt.partition_id, &receipt.lease_id);
    let (set, _) = load(ctx, &key).await?;
    let Some(head) = set.head() else {
        // No roster. Ask the LEASE which of the two reasons it is, because they
        // have different answers and the record alone cannot tell them apart: a
        // claim that ended (its batch was deleted, or it expired and was
        // redelivered) is a message that is not in flight, which is exactly what
        // AWS answers. Renewing by a second is a safe probe — the broker takes
        // the GREATEST of the two expiries, so it can only ever be a no-op.
        let extended = ctx
            .facade
            .queen
            .lease_extend(&receipt.lease_id, 1, ctx.token())
            .await
            .map_err(|e| super::queen_error(&e))?;
        if extended.renewed == 0 {
            return Err(SqsError::new(ErrorKind::MessageNotInflight));
        }
        // The claim is LIVE and its record is gone (a visibility extended past
        // the record's TTL, or a write that never landed). Guessing the head is
        // guessing which messages may be committed, so nothing is sent: the
        // batch returns when its visibility runs out, one window later than
        // asked, and the client is TOLD rather than told nothing.
        return Err(SqsError::with(
            ErrorKind::ServiceUnavailable,
            "The delivery record for this batch of FIFO messages is no longer available, so its \
             visibility cannot be changed to zero without risking messages that were never \
             deleted. The batch returns when its visibility timeout expires.",
        ));
    };
    let ack = AckItem::released(head, &receipt.partition_id, &receipt.lease_id);
    let released = ctx
        .facade
        .queen
        .ack(&ack, None, ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    match released.success {
        true => Ok(()),
        false => Err(SqsError::new(ErrorKind::MessageNotInflight)),
    }
}

/// Keep a claim's delete-set alive for as long as the lease the client just
/// extended. BEST EFFORT, deliberately: the extension's verdict is the lease's
/// and this is hygiene — a record that outlives its claim costs one stale row,
/// and one that dies first costs [`terminate`] its roster.
pub async fn keep_alive(ctx: &Ctx, receipt: &Receipt, seconds: i64) {
    let key = Registry::key_delete_set(&receipt.partition_id, &receipt.lease_id);
    let Ok((mut set, version)) = load(ctx, &key).await else {
        return;
    };
    if version == 0 {
        return;
    }
    set.ttl_seconds = ttl_of(seconds);
    let _ = ctx
        .facade
        .queen
        .kv(
            &[KvOp::put_ttl(
                NS,
                &key,
                set.to_value(),
                set.ttl_seconds,
                Some(version),
            )],
            ctx.token(),
        )
        .await;
}

/// The message uuids the delete-sets of one lane say a client has already
/// deleted, for a caller that pops that lane WITHOUT going through [`receive`].
///
/// The mover of a message move task is the one such caller
/// ([`super::movetask`]): it drains a dead-letter queue with the broker's own
/// pop, so nothing on its path reads the marks that [`receive`] reads, and on a
/// FIFO dead-letter queue whose consumer deleted out of order it would carry
/// messages home that were already processed.
///
/// BEST EFFORT, like every use of these marks: a read that fails answers no
/// marks, which is a duplicate and never a loss.
pub async fn deleted_here(ctx: &Ctx, partition_id: &str) -> BTreeSet<String> {
    let ops = [KvOp::get_prefix(
        NS,
        &Registry::key_delete_sets(partition_id),
        MAX_PRIOR_SETS,
        None,
    )];
    let Ok(answers) = ctx.facade.queen.kv(&ops, ctx.token()).await else {
        return BTreeSet::new();
    };
    // No key is skipped here: every record of this lane belongs to a lease that
    // is not this caller's.
    prior_marks(answers.first(), "", partition_id).ids
}

/// Take the messages a redrive move committed OUT of their claim's roster.
///
/// THE STALL THIS CLOSES. A receive records the roster of everything the pop
/// answered, and [`super::dlq::sift`] runs after it: a message over its queue's
/// `maxReceiveCount` is moved and acked under the SAME lease, and the client is
/// never told it existed. It is still in the roster, it is a member no delete
/// will ever mark, and [`DeleteSet::prefix`] stops at the first member nobody
/// deleted — so a client that deletes every message it was given would ack
/// NOTHING, hold its group for a whole visibility timeout and see the batch
/// again. Which is why the roster is trimmed to what the client actually holds.
///
/// Only messages whose move COMMITTED, which is what the caller passes: a move
/// that failed leaves the message on the queue, over threshold and undelivered,
/// and acking past it would destroy the one message this facade must not.
///
/// BEST EFFORT: a write that does not land costs the stall it was closing —
/// duplicates and one blocked window, never a message.
pub async fn forget(ctx: &Ctx, moved: &[queen::Message]) {
    let mut claims: BTreeMap<(&str, &str), BTreeSet<&str>> = BTreeMap::new();
    for message in moved {
        claims
            .entry((&message.partition_id, &message.lease_id))
            .or_default()
            .insert(&message.id);
    }
    for ((partition_id, lease_id), ids) in claims {
        if lease_id.is_empty() {
            continue;
        }
        let key = Registry::key_delete_set(partition_id, lease_id);
        let Ok((mut set, version)) = load(ctx, &key).await else {
            continue;
        };
        if version == 0 {
            continue;
        }
        let before = set.prefix();
        set.forget(&ids);
        let after = set.prefix();
        // A roster with nothing left describes a claim the move's own ack
        // completed, so the row is dropped rather than left for its TTL.
        let op = match set.members.is_empty() {
            true => KvOp::delete(NS, &key, Some(version)),
            false => KvOp::put_ttl(NS, &key, set.to_value(), set.ttl_seconds, Some(version)),
        };
        let written = ctx.facade.queen.kv(&[op], ctx.token()).await;
        if !written.is_ok_and(|a| a.first().is_some_and(queen::KvAnswer::applied)) {
            if let Some(suppressed) = ROSTER_NOT_TRIMMED.tick_now() {
                tracing::warn!(
                    target: "sqs",
                    suppressed,
                    partition_id,
                    "a claim's roster still lists dead-lettered messages; the deletes of the rest \
                     of that batch ack nothing until it redelivers"
                );
            }
            continue;
        }
        // Trimming can CLOSE a gap: a member the move took out from between two
        // the previous lease had deleted (module header, the carried marks).
        if after > before {
            let ack = AckItem::completed(&set.members[after - 1], partition_id, lease_id);
            let _ = ctx.facade.queen.ack(&ack, None, ctx.token()).await;
        }
    }
}

/// Give a whole claim back, naming its first message — the one call that is
/// certain to name the head, because the caller is holding what the pop just
/// answered.
pub async fn release(ctx: &Ctx, message: &queen::Message) {
    let ack = AckItem::released(
        &message.transaction_id,
        &message.partition_id,
        &message.lease_id,
    );
    let _ = ctx.facade.queen.ack(&ack, None, ctx.token()).await;
}

// ------------------------------------------------- ReceiveRequestAttemptId

/// The `ReceiveRequestAttemptId` of a receive, validated.
///
/// A standard queue answers `None`: AWS documents the parameter as FIFO-only,
/// and this facade ACCEPTS it there rather than refusing it — the M1 rule, that
/// refusing what AWS may accept is the more expensive direction to be wrong in
/// — but it deduplicates nothing, because a standard receive has no group whose
/// order a retry could disturb.
pub fn attempt_id<'a>(record: &QueueRecord, params: &'a Value) -> SqsResult<Option<&'a str>> {
    let id = params
        .get("ReceiveRequestAttemptId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty());
    match (record.fifo, id) {
        (true, Some(id)) => {
            super::messages::check_fifo_id("ReceiveRequestAttemptId", id)?;
            Ok(Some(id))
        }
        _ => Ok(None),
    }
}

/// The messages a previous receive answered under this attempt id, if it is
/// still inside its window.
///
/// This is what makes a retried receive safe on a FIFO queue: the first attempt
/// took a claim, and a client whose answer was lost in the network would
/// otherwise ask for messages that are in flight and be told there are none —
/// with its own group blocked behind them for the whole visibility timeout.
pub async fn replay(ctx: &Ctx, queue: &str, id: &str) -> SqsResult<Option<Vec<Value>>> {
    let key = Registry::key_receive_attempt(queue, id);
    let answer = ctx
        .facade
        .queen
        .kv(&[KvOp::get(NS, &key)], ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    Ok(answer
        .first()
        .filter(|a| a.found)
        .and_then(|a| a.value.get(KEY_MEMBERS))
        .and_then(Value::as_array)
        .cloned())
}

/// Remember what this receive answered, for the length of its own visibility
/// timeout — after which the messages are no longer in flight and a retry is an
/// ordinary receive.
///
/// Answers the WINNER's messages when another request stored an answer for the
/// same id first, which is the whole point of the id: the client asked for one
/// answer to one attempt and gets one. The caller then has a claim it must give
/// back — see [`release`].
///
/// AN EMPTY ANSWER IS NOT REMEMBERED, and that is a correctness rule and not a
/// saving. The record exists to hand back messages that are IN FLIGHT; a receive
/// that claimed nothing has none, so remembering it would answer every later
/// receive under that id — a client is free to reuse one — with an instant empty
/// result for the whole visibility window, past whatever arrived in the
/// meantime, without even spending its `WaitTimeSeconds`. It would also write one
/// row per idle poll, which is the cost [`receive`] refuses on the same grounds
/// (`an_empty_receive_records_nothing`).
pub async fn remember(
    ctx: &Ctx,
    queue: &str,
    id: &str,
    messages: &[Value],
    visibility: i64,
) -> SqsResult<Option<Vec<Value>>> {
    if messages.is_empty() {
        return Ok(None);
    }
    let value = serde_json::json!({ KEY_MEMBERS: messages });
    if serde_json::to_vec(&value).map_or(usize::MAX, |bytes| bytes.len()) > MAX_REMEMBERED_BYTES {
        // DIVERGENCE, `accepted`: an answer this large is not remembered, so a
        // retry of THIS attempt id behaves as a fresh receive. The store refuses
        // a value over its own ceiling and a facade that discovered that here
        // would have to choose between failing a receive whose messages are
        // already claimed and lying about what it stored.
        if let Some(suppressed) = ATTEMPT_TOO_LARGE.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                queue,
                "a receive answer is too large to remember for its ReceiveRequestAttemptId"
            );
        }
        return Ok(None);
    }
    let key = Registry::key_receive_attempt(queue, id);
    // Never zero: the store refuses a write that declares no expiry, and a
    // receive that asked for no visibility at all still answered messages a
    // retry inside the same instant must be given back.
    let ttl = visibility.clamp(1, i64::from(MAX_VISIBILITY_SECONDS)) as u64;
    let answer = ctx
        .facade
        .queen
        .kv(
            &[KvOp::put_if_absent_ttl(NS, &key, value, ttl)],
            ctx.token(),
        )
        .await
        .map_err(|e| super::queen_error(&e))?;
    let Some(answer) = answer.first() else {
        return Ok(None);
    };
    if answer.applied() {
        return Ok(None);
    }
    Ok(answer
        .value
        .get(KEY_MEMBERS)
        .and_then(Value::as_array)
        .cloned())
}

// ---------------------------------------------------------------- helpers

/// One record read, with the version the write that follows compares against.
async fn load(ctx: &Ctx, key: &str) -> SqsResult<(DeleteSet, i64)> {
    let answer = ctx
        .facade
        .queen
        .kv(&[KvOp::get(NS, key)], ctx.token())
        .await
        .map_err(|e| super::queen_error(&e))?;
    // A record that is not there is an EMPTY roster, which acks nothing, at
    // version 0 — which the write that follows turns into a create-if-absent.
    // The FALLBACK record a delete then writes carries the client's intent and
    // nothing else: it has no roster to compute a prefix from, and its marks are
    // MESSAGE UUIDS like every other mark, so however long it outlives the
    // queue's dedup window it can only ever suppress the message it names
    // (module header).
    let fallback = ttl_of(DEFAULT_VISIBILITY_SECONDS);
    let Some(answer) = answer.first().filter(|a| a.found) else {
        return Ok((
            DeleteSet {
                ttl_seconds: fallback,
                ..DeleteSet::default()
            },
            0,
        ));
    };
    Ok((
        DeleteSet::from_value(&answer.value, fallback),
        answer.version,
    ))
}

/// The TTL a delete-set written for `visibility` seconds gets.
fn ttl_of(visibility: i64) -> u64 {
    visibility.clamp(0, i64::from(MAX_VISIBILITY_SECONDS)) as u64 + TTL_SLACK_SECONDS
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::messages::{
        change_message_visibility, change_message_visibility_batch, delete_message_batch,
    };
    use crate::actions::testing::{attribute, field, Rig};
    use crate::error::SqsResult;
    use crate::queen::{AckStatus, QueenApi, QUEUE_MODE_GROUP};
    use serde_json::json;
    use std::time::Duration;

    const QUEUE: &str = "orders.fifo";
    /// The rig's queues lease for thirty seconds and a receive that names no
    /// visibility falls back to the same number, so this is what expires a
    /// claim and what a delete-set's TTL is measured from.
    const VISIBILITY: u64 = 30;

    /// One seeded message: the envelope shape a native producer's payload takes
    /// on the way back out ([`crate::envelope`]).
    fn payload(text: &str) -> Value {
        json!({ "b": text })
    }

    /// A FIFO queue with `groups` seeded, each with `n` messages named `<g><i>`.
    async fn seeded(groups: &[(&str, usize)]) -> Rig {
        let rig = Rig::new(&[(QUEUE, &[])]).await;
        for (group, n) in groups {
            let payloads: Vec<Value> = (0..*n).map(|i| payload(&format!("{group}{i}"))).collect();
            rig.fake.seed(QUEUE, group, 0, &payloads);
        }
        rig
    }

    fn bodies(messages: &[Value]) -> Vec<String> {
        messages
            .iter()
            .map(|m| field(m, "Body").to_string())
            .collect()
    }

    async fn receive_n(rig: &Rig, n: i64) -> Vec<Value> {
        rig.receive_list(QUEUE, json!({ "MaxNumberOfMessages": n }))
            .await
    }

    /// The delete-sets the STORE holds for one lane, as `(key, record)`. Read
    /// through the store rather than through a facade field, because a record
    /// this facade kept in memory would be a record the next instance cannot
    /// see — which is the one thing the design forbids.
    async fn records(rig: &Rig, group: &str) -> Vec<(String, DeleteSet)> {
        let partition_id = rig.fake.partition_id(QUEUE, group);
        let answers = rig
            .fake
            .kv(
                &[KvOp::get_prefix(
                    NS,
                    &Registry::key_delete_sets(&partition_id),
                    100,
                    None,
                )],
                None,
            )
            .await
            .expect("the store answers");
        answers[0]
            .rows
            .iter()
            .map(|row| (row.key.clone(), DeleteSet::from_value(&row.value, 0)))
            .collect()
    }

    /// The delete-set of the lane's most recent claim.
    ///
    /// A lane keeps one record per claim until each expires, so a redelivered
    /// lane has the previous lease's record beside the current one — which is
    /// the point of them. The newest is the LAST in key order: the key carries
    /// the lease id and the double mints those in creation order.
    async fn record(rig: &Rig, group: &str) -> DeleteSet {
        records(rig, group)
            .await
            .pop()
            .expect("the lane has a delete-set")
            .1
    }

    fn committed(rig: &Rig, group: &str) -> i64 {
        rig.fake
            .committed(QUEUE, group, QUEUE_MODE_GROUP)
            .expect("the group has a cursor")
    }

    async fn delete(rig: &Rig, message: &Value) -> SqsResult<Value> {
        rig.delete(QUEUE, field(message, "ReceiptHandle")).await
    }

    async fn set_visibility(rig: &Rig, message: &Value, seconds: i64) -> SqsResult<Value> {
        change_message_visibility(
            &rig.ctx,
            &rig.params(
                QUEUE,
                json!({
                    "ReceiptHandle": field(message, "ReceiptHandle"),
                    "VisibilityTimeout": seconds
                }),
            ),
        )
        .await
    }

    // --------------------------------------------------------- the record

    /// A roster of three, `<dedup key>` over `<message uuid>`.
    fn roster(members: &[(&str, &str)]) -> DeleteSet {
        DeleteSet {
            members: members.iter().map(|(txn, _)| (*txn).to_string()).collect(),
            ids: members.iter().map(|(_, id)| (*id).to_string()).collect(),
            deleted: BTreeSet::new(),
            ttl_seconds: 90,
        }
    }

    #[test]
    fn a_prefix_is_the_leading_run_of_deleted_members() {
        let mut set = roster(&[("a", "msg-1"), ("b", "msg-2"), ("c", "msg-3")]);
        assert_eq!(set.prefix(), 0);
        set.mark("msg-2");
        assert_eq!(set.prefix(), 0, "a gap below it is not a prefix");
        set.mark("msg-1");
        assert_eq!(set.prefix(), 2, "closing the gap closes the whole run");
        set.mark("msg-3");
        assert_eq!(set.prefix(), 3);
        assert!(set.complete());
    }

    /// THE LOSS A KEY-SHAPED RECORD WOULD CAUSE. One claim can hold two
    /// messages under one `MessageDeduplicationId` — the key is unique only
    /// inside the queue's dedup window — and deleting the LATER one must not
    /// open a prefix over the earlier one, which nobody deleted.
    #[test]
    fn two_members_under_one_dedup_key_are_two_members() {
        let mut set = roster(&[("d1", "msg-1"), ("d1", "msg-2"), ("d2", "msg-3")]);
        set.mark("msg-2");
        assert_eq!(set.prefix(), 0, "the first `d1` is not deleted");
        assert_eq!(set.head(), Some("d1"), "and it is still the head");
        set.mark("msg-1");
        assert_eq!(set.prefix(), 2);
    }

    #[test]
    fn the_head_is_the_first_member_nobody_deleted() {
        let mut set = roster(&[("a", "msg-1"), ("b", "msg-2")]);
        assert_eq!(set.head(), Some("a"));
        set.mark("msg-1");
        assert_eq!(set.head(), Some("b"));
        set.mark("msg-2");
        // Nothing is left in flight, and there is nothing to release.
        assert_eq!(set.head(), None);
    }

    #[test]
    fn a_record_round_trips_through_the_store() {
        let mut set = roster(&[("a", "msg-1"), ("b", "msg-2")]);
        set.mark("msg-2");
        assert_eq!(DeleteSet::from_value(&set.to_value(), 0), set);
    }

    /// A mark is a MESSAGE UUID, always — including on the fallback record a
    /// delete that found no roster writes, which is what keeps that record from
    /// suppressing the next message sent under the same dedup key.
    #[test]
    fn a_mark_names_a_message_and_never_a_dedup_key() {
        let mut marks = Marks::default();
        let mut set = roster(&[("d1", "msg-1")]);
        set.mark("msg-1");
        set.add_marks(&mut marks);
        // The fallback record: no roster, and the mark is still the uuid.
        DeleteSet {
            deleted: ["msg-9".to_string()].into_iter().collect(),
            ..DeleteSet::default()
        }
        .add_marks(&mut marks);

        let message = |id: &str, txn: &str| queen::Message {
            id: id.to_string(),
            transaction_id: txn.to_string(),
            data: Value::Null,
            partition: String::new(),
            partition_id: String::new(),
            lease_id: String::new(),
            consumer_group: String::new(),
            delivery_attempt: 1,
            created_at: String::new(),
            offset: None,
        };
        assert!(marks.covers(&message("msg-1", "d1")));
        assert!(marks.covers(&message("msg-9", "whatever")));
        // THE ONE THIS CLOSES: the same dedup key, sent again after the window,
        // is a DIFFERENT message and is not covered by the old mark.
        assert!(!marks.covers(&message("msg-2", "d1")));
    }

    /// A record whose two arrays disagree acks nothing: a prefix computed over
    /// a roster that cannot say which message is which would ack the wrong one.
    #[test]
    fn a_misaligned_record_acks_nothing() {
        let stored = serde_json::json!({"m": ["a", "b"], "i": ["msg-1"], "d": ["msg-1"]});
        let set = DeleteSet::from_value(&stored, 90);
        assert!(set.ids.is_empty());
        assert_eq!(set.prefix(), 0);
        assert_eq!(set.head(), Some("a"));
        assert!(!set.complete());
        // The mark still travels: suppression is by uuid and needs no roster.
        let mut marks = Marks::default();
        set.add_marks(&mut marks);
        assert_eq!(marks.ids.len(), 1);
    }

    /// The moved members leave the roster, so the prefix of what is left can
    /// still be acked ([`forget`]).
    #[test]
    fn forgetting_a_moved_member_leaves_the_rest_ackable() {
        let mut set = roster(&[("d0", "msg-1"), ("d1", "msg-2"), ("d2", "msg-3")]);
        set.forget(&["msg-1"].into_iter().collect());
        assert_eq!(set.members, ["d1", "d2"]);
        assert_eq!(set.ids, ["msg-2", "msg-3"]);
        set.mark("msg-2");
        assert_eq!(set.prefix(), 1, "the head is the client's own message now");
        set.mark("msg-3");
        assert!(set.complete());
    }

    /// LENIENT in the safe direction: a record this facade cannot read leaves an
    /// empty roster, and an empty roster acks nothing — the failure that costs a
    /// redelivery rather than the one that swallows a message.
    #[test]
    fn an_unreadable_record_acks_nothing() {
        for value in [json!(null), json!("nonsense"), json!({"m": 7}), json!({})] {
            let set = DeleteSet::from_value(&value, 90);
            assert_eq!(set.prefix(), 0);
            assert_eq!(set.head(), None);
            assert!(!set.complete(), "an empty roster is never complete");
            assert_eq!(set.ttl_seconds, 90, "the fallback TTL stands in");
        }
    }

    /// The TTL outlives the claim it belongs to, and a visibility beyond SQS's
    /// own ceiling cannot make it outlive the store.
    #[test]
    fn a_records_ttl_is_the_visibility_plus_slack() {
        assert_eq!(ttl_of(30), 30 + TTL_SLACK_SECONDS);
        assert_eq!(ttl_of(0), TTL_SLACK_SECONDS);
        assert_eq!(ttl_of(-5), TTL_SLACK_SECONDS);
        assert_eq!(
            ttl_of(i64::MAX),
            MAX_VISIBILITY_SECONDS as u64 + TTL_SLACK_SECONDS
        );
    }

    /// A lane's key range cannot reach into another lane's: the ids are escaped,
    /// so the separator that ends the prefix cannot appear inside one.
    #[test]
    fn one_lanes_delete_sets_are_not_another_lanes() {
        let prefix = Registry::key_delete_sets("pid-1");
        assert!(Registry::key_delete_set("pid-1", "lease-a").starts_with(&prefix));
        assert!(!Registry::key_delete_set("pid-10", "lease-a").starts_with(&prefix));
        // A lease id carrying the separator addresses its own key and not one
        // it composed out of two halves.
        assert_ne!(
            Registry::key_delete_set("pid-1", "a:b"),
            Registry::key_delete_set("pid-1:a", "b")
        );
    }

    // -------------------------------------------------------- the receive

    /// The whole FIFO receive in one assertion: one pop of one lane, in order,
    /// every message carrying the group it came from.
    #[tokio::test]
    async fn a_fifo_receive_answers_one_group_in_order() {
        let rig = seeded(&[("g", 4)]).await;
        let messages = rig
            .receive_list(
                QUEUE,
                json!({"MaxNumberOfMessages": 10, "AttributeNames": ["All"]}),
            )
            .await;
        assert_eq!(bodies(&messages), ["g0", "g1", "g2", "g3"]);
        assert!(messages
            .iter()
            .all(|m| attribute(m, "MessageGroupId").as_deref() == Some("g")));
    }

    /// The group is BLOCKED while it is in flight — that is the partition claim,
    /// not an emulation of one — and a second consumer is served another group
    /// rather than being made to wait or, worse, being handed the same messages.
    #[tokio::test]
    async fn a_second_consumer_never_gets_the_claimed_group() {
        let rig = seeded(&[("a", 2), ("b", 2)]).await;
        let first = receive_n(&rig, 10).await;
        assert_eq!(bodies(&first), ["a0", "a1"]);

        let second = rig.sibling();
        let theirs = receive_n(&second, 10).await;
        assert_eq!(
            bodies(&theirs),
            ["b0", "b1"],
            "a claimed group is not served"
        );

        // Both groups are claimed, so a third consumer waits for one of them.
        assert!(receive_n(&rig, 10).await.is_empty());
    }

    /// DIVERGENCE, `accepted`, pinned: AWS fills a receive from as many groups
    /// as it can and this fills it from ONE. A hundred one-message groups
    /// therefore answer one message, not ten — a short receive, which
    /// `MaxNumberOfMessages` allows, and never a group split across consumers.
    #[tokio::test]
    async fn many_groups_do_not_widen_one_receive() {
        let groups: Vec<(String, usize)> = (0..20).map(|i| (format!("g{i:02}"), 1)).collect();
        let rig = seeded(
            &groups
                .iter()
                .map(|(g, n)| (g.as_str(), *n))
                .collect::<Vec<_>>(),
        )
        .await;
        let messages = receive_n(&rig, 10).await;
        assert_eq!(bodies(&messages), ["g000"], "one pop, one lane");
        // ...and the next receive gets the NEXT group, so the queue drains.
        assert_eq!(bodies(&receive_n(&rig, 10).await), ["g010"]);
    }

    /// The roster is what a delete arriving at another instance reads to know
    /// what came before the message it names.
    #[tokio::test]
    async fn a_receive_records_the_roster_it_delivered() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        let set = record(&rig, "g").await;
        assert_eq!(set.members.len(), 3);
        assert!(set.deleted.is_empty());
        assert_eq!(set.ttl_seconds, VISIBILITY + TTL_SLACK_SECONDS);
        // The roster is the DELIVERY ORDER, which is the offsets' order.
        let lane: Vec<String> = rig
            .fake
            .lane(QUEUE, "g")
            .into_iter()
            .map(|(_, txn)| txn)
            .collect();
        assert_eq!(set.members, lane);
        assert_eq!(messages.len(), 3);
    }

    /// An empty receive claims nothing, so it records nothing: a record per
    /// empty poll would be a row per idle consumer per second.
    #[tokio::test]
    async fn an_empty_receive_records_nothing() {
        let rig = seeded(&[]).await;
        assert!(receive_n(&rig, 10).await.is_empty());
        assert!(records(&rig, "g").await.is_empty());
    }

    /// One KV round trip for the common receive: the read of what earlier leases
    /// recorded and the write of this claim's roster travel together.
    #[tokio::test]
    async fn a_clean_receive_costs_one_kv_call() {
        let rig = seeded(&[("g", 3)]).await;
        let before = rig.fake.kv_ops().len();
        receive_n(&rig, 3).await;
        let ops = &rig.fake.kv_ops()[before..];
        assert_eq!(ops.len(), 2, "one getPrefix and one put, in one call");
        assert!(rig.fake.acked().is_empty(), "a receive acks nothing");
    }

    // ------------------------------------------------------ the prefix ack

    /// THE CONSTRAINT, pinned against the broker itself rather than assumed:
    /// acking a message in the middle of a claim commits everything below it.
    /// This is why the facade never acks the client's own message.
    #[tokio::test]
    async fn the_brokers_ack_commits_everything_below_the_position_it_names() {
        let rig = seeded(&[("g", 3)]).await;
        let popped = rig
            .fake
            .pop_queue(
                QUEUE,
                &PopOptions {
                    batch: 3,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        let third = &popped.messages[2];
        rig.fake
            .ack(
                &AckItem::completed(&third.transaction_id, &third.partition_id, &third.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            committed(&rig, "g"),
            2,
            "the first two are below the cursor and will never redeliver"
        );
    }

    /// Deleting the head is the one delete that CAN be acked at once.
    #[tokio::test]
    async fn deleting_the_head_advances_the_cursor() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        delete(&rig, &messages[0]).await.expect("the delete lands");
        assert_eq!(committed(&rig, "g"), 0);
        let set = record(&rig, "g").await;
        assert_eq!(set.prefix(), 1);
        // The lease is KEPT: the rest of the batch is still the client's.
        assert!(rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
    }

    /// The out-of-order delete: recorded, ACKED BY NOTHING, and — the sentence
    /// the whole module exists for — the messages below it survive.
    #[tokio::test]
    async fn a_mid_batch_delete_never_commits_the_messages_below_it() {
        let rig = seeded(&[("g", 5)]).await;
        let messages = receive_n(&rig, 5).await;
        delete(&rig, &messages[2]).await.expect("the delete lands");

        assert_eq!(committed(&rig, "g"), -1, "nothing was committed");
        assert!(rig.fake.acked().is_empty(), "and nothing was acked");
        let set = record(&rig, "g").await;
        assert_eq!(set.deleted.len(), 1);
        assert_eq!(set.prefix(), 0);

        // The proof: everything the client did not delete comes back.
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        assert_eq!(bodies(&receive_n(&rig, 5).await), ["g0", "g1", "g3", "g4"]);
    }

    /// Closing the gap acks the WHOLE prefix in one call, which is the stored
    /// procedure's own implicit-ack rule doing the work.
    #[tokio::test]
    async fn closing_the_gap_acks_the_whole_prefix_in_one_ack() {
        let rig = seeded(&[("g", 4)]).await;
        let messages = receive_n(&rig, 4).await;
        delete(&rig, &messages[1]).await.unwrap();
        delete(&rig, &messages[2]).await.unwrap();
        assert_eq!(committed(&rig, "g"), -1);

        delete(&rig, &messages[0]).await.unwrap();
        assert_eq!(committed(&rig, "g"), 2, "the cursor jumped the whole run");
        let acked = rig.fake.acked();
        assert_eq!(acked.len(), 1, "one ack, not three");
        assert_eq!(
            acked[0].transaction_id,
            rig.fake.lane(QUEUE, "g")[2].1,
            "the ack names the prefix's LAST member"
        );
        assert!(rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
    }

    /// The batch completed: the cursor is at its end, the lease is released, and
    /// the record is gone rather than left for its TTL — every later receive of
    /// this lane reads that prefix.
    #[tokio::test]
    async fn deleting_every_member_completes_the_batch_and_drops_the_record() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        for message in &messages {
            delete(&rig, message).await.unwrap();
        }
        assert_eq!(committed(&rig, "g"), 2);
        assert!(!rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
        assert!(records(&rig, "g").await.is_empty(), "the record is dropped");
        assert!(receive_n(&rig, 10).await.is_empty());
    }

    /// A second delete of the same handle is a success that changes nothing —
    /// AWS's own idempotence, and here it must not re-ack a cursor that has
    /// moved on.
    #[tokio::test]
    async fn deleting_a_message_twice_is_success_and_changes_nothing() {
        let rig = seeded(&[("g", 2)]).await;
        let messages = receive_n(&rig, 2).await;
        delete(&rig, &messages[0]).await.unwrap();
        let acks = rig.fake.acked().len();
        delete(&rig, &messages[0]).await.expect("success again");
        assert_eq!(committed(&rig, "g"), 0);
        assert_eq!(rig.fake.acked().len(), acks, "and no second ack");
    }

    /// Ten entries of one claim are ONE read-modify-write and one ack: a batch
    /// that applied its entries one at a time would make this facade compete
    /// with itself for its own compare-and-set.
    #[tokio::test]
    async fn a_batch_delete_of_one_claim_is_one_record_write() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        let before = rig.fake.kv_ops().len();
        let answer = delete_message_batch(
            &rig.ctx,
            &rig.params(
                QUEUE,
                json!({"Entries": [
                    {"Id": "c", "ReceiptHandle": field(&messages[2], "ReceiptHandle")},
                    {"Id": "a", "ReceiptHandle": field(&messages[0], "ReceiptHandle")},
                    {"Id": "b", "ReceiptHandle": field(&messages[1], "ReceiptHandle")}
                ]}),
            ),
        )
        .await
        .expect("the batch is answered");
        assert_eq!(answer["Successful"].as_array().unwrap().len(), 3);
        assert!(answer.get("Failed").is_none());

        let ops = &rig.fake.kv_ops()[before..];
        assert_eq!(ops.len(), 2, "one get and one write for the whole batch");
        assert_eq!(rig.fake.acked().len(), 1, "and one ack for the whole batch");
        assert_eq!(committed(&rig, "g"), 2);
    }

    /// A batch that deletes members of two different claims writes each claim's
    /// own record — and reports both under the client's own entry ids.
    #[tokio::test]
    async fn a_batch_delete_across_two_claims_writes_both_records() {
        let rig = seeded(&[("a", 2), ("b", 2)]).await;
        let first = receive_n(&rig, 2).await;
        let second = receive_n(&rig, 2).await;
        let answer = delete_message_batch(
            &rig.ctx,
            &rig.params(
                QUEUE,
                json!({"Entries": [
                    {"Id": "one", "ReceiptHandle": field(&first[0], "ReceiptHandle")},
                    {"Id": "two", "ReceiptHandle": field(&second[0], "ReceiptHandle")}
                ]}),
            ),
        )
        .await
        .unwrap();
        assert_eq!(answer["Successful"].as_array().unwrap().len(), 2);
        assert_eq!(committed(&rig, "a"), 0);
        assert_eq!(committed(&rig, "b"), 0);
    }

    // ------------------------------------------------------- redelivery

    /// The suffix redelivers minus what was deleted, and the marks travel into
    /// the new claim's record so they can still be acked.
    #[tokio::test]
    async fn a_redelivery_suppresses_what_was_already_deleted() {
        let rig = seeded(&[("g", 5)]).await;
        let messages = receive_n(&rig, 5).await;
        for index in [1, 2, 4] {
            delete(&rig, &messages[index]).await.unwrap();
        }
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));

        let again = receive_n(&rig, 5).await;
        assert_eq!(bodies(&again), ["g0", "g3"], "the deleted three are gone");
        // The new claim's record carries them, so the gap can still close.
        let set = record(&rig, "g").await;
        assert_eq!(set.members.len(), 5);
        assert_eq!(set.deleted.len(), 3);
        assert_eq!(set.prefix(), 0);

        // Closing it acks everything up to the last contiguous mark.
        delete(&rig, &again[0]).await.unwrap();
        assert_eq!(committed(&rig, "g"), 2);
        delete(&rig, &again[1]).await.unwrap();
        assert_eq!(committed(&rig, "g"), 4, "the whole group is drained");
        assert!(receive_n(&rig, 10).await.is_empty());
    }

    /// The self-healing path: the record is written BEFORE the ack, so a delete
    /// whose ack never landed is still a delete — the redelivery acks it on the
    /// way through and never hands it back to the client.
    #[tokio::test]
    async fn a_delete_whose_ack_was_lost_is_acked_by_the_next_receive() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        rig.fake
            .fail_ack(crate::queen::Error::status(503, "the ack never landed"));
        delete(&rig, &messages[0])
            .await
            .expect_err("the client is told to retry");
        assert_eq!(committed(&rig, "g"), -1, "nothing was committed");
        assert_eq!(
            record(&rig, "g").await.deleted.len(),
            1,
            "but it is recorded"
        );

        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        let again = receive_n(&rig, 3).await;
        assert_eq!(
            bodies(&again),
            ["g1", "g2"],
            "the deleted head is not returned"
        );
        assert_eq!(committed(&rig, "g"), 0, "it was acked on the way through");
    }

    /// THE HOLE A MARK BY DEDUP KEY WOULD OPEN. A record outlives the queue's
    /// dedup window whenever the visibility timeout does, so the same
    /// `MessageDeduplicationId` sent again afterwards is a NEW message that an
    /// old mark would suppress — and ack, unseen. The mark names the broker's
    /// message uuid, which is never reused, so it cannot.
    #[tokio::test]
    async fn a_dedup_key_accepted_again_is_a_new_message_and_is_delivered() {
        let rig = Rig::new(&[(QUEUE, &[])]).await;
        rig.fake.append(QUEUE, "g", "d1", payload("first"));
        rig.fake.append(QUEUE, "g", "d2", payload("second"));

        let messages = receive_n(&rig, 2).await;
        assert_eq!(bodies(&messages), ["first", "second"]);
        delete(&rig, &messages[0]).await.unwrap();
        assert_eq!(committed(&rig, "g"), 0);
        assert_eq!(record(&rig, "g").await.deleted.len(), 1, "`d1` is marked");

        // The window has passed and `d1` is accepted again: another message,
        // under a key an old record still calls deleted.
        rig.fake.append(QUEUE, "g", "d1", payload("third"));
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        let again = receive_n(&rig, 5).await;
        assert_eq!(
            bodies(&again),
            ["second", "third"],
            "the new message is not the deleted one"
        );
        assert_eq!(committed(&rig, "g"), 0, "and nothing was acked unseen");
    }

    /// THE LOSS A KEY-SHAPED RECORD WOULD CAUSE, end to end. One claim holds
    /// two messages under one `MessageDeduplicationId` — the key is unique only
    /// inside the queue's dedup window, which a client may set to one second —
    /// and the client deletes the SECOND. Recorded against the dedup key, that
    /// one delete would mark both, open a prefix over the first, and ack it away
    /// unseen; recorded against the message uuid it marks one.
    #[tokio::test]
    async fn deleting_one_of_two_messages_under_one_dedup_key_spares_the_other() {
        let rig = Rig::new(&[(QUEUE, &[])]).await;
        rig.fake.append(QUEUE, "g", "d1", payload("first"));
        rig.fake.append(QUEUE, "g", "d1", payload("second"));
        rig.fake.append(QUEUE, "g", "d2", payload("third"));

        let messages = receive_n(&rig, 3).await;
        assert_eq!(bodies(&messages), ["first", "second", "third"]);
        delete(&rig, &messages[1]).await.expect("the delete lands");
        assert_eq!(
            committed(&rig, "g"),
            -1,
            "`first` shares the key and was never deleted"
        );
        assert!(rig.fake.acked().is_empty(), "and nothing was acked");
        let set = record(&rig, "g").await;
        assert_eq!(set.deleted.len(), 1, "one message, not one key");
        assert_eq!(set.prefix(), 0);

        // The proof: the undeleted namesake comes back and the deleted one does
        // not — the exact opposite of what a key-shaped record produces.
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        assert_eq!(bodies(&receive_n(&rig, 3).await), ["first", "third"]);
    }

    /// A delete that finds no roster records the client's intent on its own, and
    /// that record outlives the queue's dedup window whenever its TTL does. It
    /// names a MESSAGE, so the next message accepted under the same key is
    /// delivered — where a mark by key would suppress it and, once the prefix
    /// closed over it, ack it unseen.
    #[tokio::test]
    async fn a_record_with_no_roster_cannot_suppress_the_next_message_under_that_key() {
        let rig = Rig::new(&[(QUEUE, &[])]).await;
        rig.fake.append(QUEUE, "g", "d1", payload("first"));
        let messages = receive_n(&rig, 1).await;
        delete(&rig, &messages[0]).await.unwrap();
        assert!(
            records(&rig, "g").await.is_empty(),
            "a completed batch drops its record"
        );

        // The duplicate delete AWS answers success to, with nothing left to read.
        delete(&rig, &messages[0]).await.expect("success again");
        let stray = record(&rig, "g").await;
        assert!(stray.members.is_empty(), "no roster");
        assert_eq!(stray.deleted.len(), 1);

        // The same dedup key, accepted again, inside that record's life.
        rig.fake.append(QUEUE, "g", "d1", payload("second"));
        assert_eq!(bodies(&receive_n(&rig, 5).await), ["second"]);
        assert_eq!(committed(&rig, "g"), 0, "and nothing was acked unseen");
    }

    /// The honest boundary: the marks are a TTL'd record, and a claim whose
    /// record has expired redelivers what it recorded. A duplicate, which SQS
    /// allows — never a loss, which it does not.
    #[tokio::test]
    async fn marks_that_outlived_their_record_stop_suppressing() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        delete(&rig, &messages[1]).await.unwrap();
        // Past the record's own TTL (visibility + slack), which is well past
        // the lease that produced it.
        rig.fake
            .advance(Duration::from_secs(VISIBILITY + TTL_SLACK_SECONDS + 1));
        assert!(records(&rig, "g").await.is_empty());
        assert_eq!(bodies(&receive_n(&rig, 3).await), ["g0", "g1", "g2"]);
    }

    // ------------------------------------------------------- visibility

    /// DIVERGENCE, `accepted`, pinned: a visibility of zero on ANY member gives
    /// the whole remaining claim back, because the lease covers a run of the
    /// group and there is no verb that releases one message of one.
    #[tokio::test]
    async fn zero_visibility_returns_the_whole_claim() {
        for index in [0, 2] {
            let rig = seeded(&[("g", 3)]).await;
            let messages = receive_n(&rig, 3).await;
            set_visibility(&rig, &messages[index], 0)
                .await
                .expect("the release is accepted");
            assert!(!rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
            assert_eq!(
                committed(&rig, "g"),
                -1,
                "and it commits nothing — not even the members below it"
            );
            // No clock advance: the whole batch is visible again at once.
            assert_eq!(bodies(&receive_n(&rig, 3).await), ["g0", "g1", "g2"]);
        }
    }

    /// The release names the claim's HEAD and never the caller's own message.
    /// Naming the caller's would commit everything below it — the same swallow
    /// `the_brokers_ack_commits_everything_below_the_position_it_names` pins.
    #[tokio::test]
    async fn zero_visibility_releases_at_the_head_and_charges_nothing() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        set_visibility(&rig, &messages[2], 0).await.unwrap();
        let acked = rig.fake.acked();
        assert_eq!(acked.len(), 1);
        assert_eq!(
            acked[0].status,
            AckStatus::Retry,
            "a release, not a failure"
        );
        assert_eq!(
            acked[0].transaction_id,
            rig.fake.lane(QUEUE, "g")[0].1,
            "the head, not the message the client named"
        );
        assert_eq!(rig.fake.retries(QUEUE, "g", QUEUE_MODE_GROUP), 0);
    }

    /// A release on a claim whose delete-set is gone REFUSES rather than
    /// guessing the head. Guessing means naming the caller's own message, and
    /// that commits every undeleted message below it; the batch comes back one
    /// visibility window later instead, and the client is told so.
    #[tokio::test]
    async fn zero_visibility_without_a_record_refuses_rather_than_guessing() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        let key = records(&rig, "g").await.remove(0).0;
        rig.fake
            .kv(&[KvOp::delete(NS, &key, None)], None)
            .await
            .unwrap();

        let error = set_visibility(&rig, &messages[2], 0)
            .await
            .expect_err("nothing is guessed");
        assert_eq!(error.kind, ErrorKind::ServiceUnavailable);
        assert!(rig.fake.acked().is_empty(), "and nothing was sent");
        assert!(rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
    }

    /// The other reason a record can be missing: the claim itself is over. That
    /// is not an outage, it is AWS's own `MessageNotInflight`, and the lease is
    /// what tells the two apart.
    #[tokio::test]
    async fn zero_visibility_on_a_claim_that_ended_is_not_in_flight() {
        let rig = seeded(&[("g", 2)]).await;
        let messages = receive_n(&rig, 2).await;
        // Deleting every member completes the batch and drops the record.
        for message in &messages {
            delete(&rig, message).await.unwrap();
        }
        let error = set_visibility(&rig, &messages[0], 0)
            .await
            .expect_err("the claim is over");
        assert_eq!(error.kind, ErrorKind::MessageNotInflight);

        // ...and so is an expired one, whose messages another consumer may
        // already be holding.
        let rig = seeded(&[("g", 2)]).await;
        let messages = receive_n(&rig, 2).await;
        let key = records(&rig, "g").await.remove(0).0;
        rig.fake
            .kv(&[KvOp::delete(NS, &key, None)], None)
            .await
            .unwrap();
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        assert_eq!(
            set_visibility(&rig, &messages[0], 0)
                .await
                .expect_err("the lease is gone")
                .kind,
            ErrorKind::MessageNotInflight
        );
    }

    /// DIVERGENCE, `accepted`, pinned: extending one member extends its
    /// batch-mates. They are the same consumer's messages and nobody else can
    /// see them, so what a client observes is that its own batch stays its own.
    #[tokio::test]
    async fn an_extension_extends_the_whole_claim() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        set_visibility(&rig, &messages[0], 600).await.unwrap();
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        assert!(
            receive_n(&rig, 3).await.is_empty(),
            "the batch is still in flight, all of it"
        );
    }

    /// ...and the delete-set is extended with it, so a delete arriving late in a
    /// long visibility still finds the roster it needs.
    #[tokio::test]
    async fn an_extension_carries_the_delete_set_with_it() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        set_visibility(&rig, &messages[0], 600).await.unwrap();
        assert_eq!(record(&rig, "g").await.ttl_seconds, 600 + TTL_SLACK_SECONDS);

        // Past the ORIGINAL record's TTL, inside the extended lease.
        rig.fake
            .advance(Duration::from_secs(VISIBILITY + TTL_SLACK_SECONDS + 1));
        assert_eq!(record(&rig, "g").await.members.len(), 3);
        delete(&rig, &messages[0]).await.unwrap();
        assert_eq!(committed(&rig, "g"), 0, "the prefix still acks");
    }

    /// THE BATCH EVERY CONSUMER LIBRARY SENDS ON AN ERROR PATH:
    /// `ChangeMessageVisibility(0)` over everything it just received. The
    /// entries name ONE claim, a claim is one lease, and the release is
    /// therefore one call answered to all of them — where ten independent calls
    /// would answer one success and nine `MessageNotInflight`.
    #[tokio::test]
    async fn a_visibility_batch_of_zero_over_one_claim_releases_it_once() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        let entries: Vec<Value> = messages
            .iter()
            .enumerate()
            .map(|(i, message)| {
                json!({
                    "Id": format!("e{i}"),
                    "ReceiptHandle": field(message, "ReceiptHandle"),
                    "VisibilityTimeout": 0
                })
            })
            .collect();
        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(QUEUE, json!({ "Entries": entries })),
        )
        .await
        .expect("the batch is answered");

        assert_eq!(
            answer["Successful"].as_array().map(Vec::len),
            Some(3),
            "{answer}"
        );
        assert!(answer.get("Failed").is_none(), "{answer}");
        assert_eq!(rig.fake.acked().len(), 1, "one release, not one per entry");
        assert!(!rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
        assert_eq!(committed(&rig, "g"), -1, "and it commits nothing");
        assert_eq!(bodies(&receive_n(&rig, 3).await), ["g0", "g1", "g2"]);
    }

    /// The same grouping the other way: the entries of one claim are ONE
    /// renewal, at the longest window any of them asked for — which is where the
    /// broker would end anyway, since it takes the greatest of the two expiries.
    #[tokio::test]
    async fn a_visibility_batch_that_extends_one_claim_renews_it_once() {
        let rig = seeded(&[("g", 2)]).await;
        let messages = receive_n(&rig, 2).await;
        let answer = change_message_visibility_batch(
            &rig.ctx,
            &rig.params(
                QUEUE,
                json!({"Entries": [
                    {"Id": "a", "ReceiptHandle": field(&messages[0], "ReceiptHandle"),
                     "VisibilityTimeout": 60},
                    {"Id": "b", "ReceiptHandle": field(&messages[1], "ReceiptHandle"),
                     "VisibilityTimeout": 600}
                ]}),
            ),
        )
        .await
        .expect("the batch is answered");

        assert_eq!(answer["Successful"].as_array().map(Vec::len), Some(2));
        assert!(answer.get("Failed").is_none(), "{answer}");
        assert_eq!(
            record(&rig, "g").await.ttl_seconds,
            600 + TTL_SLACK_SECONDS,
            "the record follows the longest of them"
        );
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        assert!(receive_n(&rig, 2).await.is_empty(), "still in flight");
    }

    /// DIVERGENCE, `accepted`, pinned: `ApproximateReceiveCount` inside a FIFO
    /// batch is the CLAIM's attempt count (`log_consumers.attempt_count`) and
    /// not the message's, so every message of one claim reports the same number
    /// — including one that has only ever been delivered once. Exact on standard
    /// queues, where a claim is one message.
    #[tokio::test]
    async fn every_message_of_one_fifo_claim_reports_the_same_receive_count() {
        let rig = seeded(&[("g", 1)]).await;
        // Two deliveries of `g0`, both abandoned: the claim is on its third.
        rig.burn(QUEUE, 2).await;
        rig.fake.append(QUEUE, "g", "fresh", payload("fresh"));

        let messages = rig
            .receive_list(
                QUEUE,
                json!({"MaxNumberOfMessages": 10, "AttributeNames": ["All"]}),
            )
            .await;
        assert_eq!(bodies(&messages), ["g0", "fresh"]);
        assert_eq!(
            attribute(&messages[0], "ApproximateReceiveCount").as_deref(),
            Some("3")
        );
        assert_eq!(
            attribute(&messages[1], "ApproximateReceiveCount").as_deref(),
            Some("3"),
            "AWS would answer 1 here: this is the claim's count, not the message's"
        );
    }

    /// An extension of a lease that is gone is `MessageNotInflight`, and it does
    /// not resurrect a record for a claim nobody holds.
    #[tokio::test]
    async fn an_extension_of_an_expired_claim_is_not_in_flight() {
        let rig = seeded(&[("g", 2)]).await;
        let messages = receive_n(&rig, 2).await;
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        let error = set_visibility(&rig, &messages[0], 60)
            .await
            .expect_err("the lease is gone");
        assert_eq!(error.kind, ErrorKind::MessageNotInflight);
    }

    // -------------------------------------------- ReceiveRequestAttemptId

    /// The retry contract: the same attempt id inside its window answers the
    /// same messages under the SAME receipt handles — the lease never changed,
    /// so the handles the first answer minted are still the live ones.
    #[tokio::test]
    async fn the_same_attempt_id_answers_the_same_messages_and_handles() {
        let rig = seeded(&[("a", 2), ("b", 2)]).await;
        let params = json!({"MaxNumberOfMessages": 2, "ReceiveRequestAttemptId": "attempt-1"});
        let first = rig.receive_list(QUEUE, params.clone()).await;
        assert_eq!(bodies(&first), ["a0", "a1"]);

        // The retry is served by ANOTHER instance, which is the case the id
        // exists for: the first answer was lost on the way back to the client.
        let second = rig.sibling().receive_list(QUEUE, params).await;
        assert_eq!(second, first, "byte for byte, handles included");
        // ...and it claimed nothing: the other group is untouched.
        assert!(!rig.fake.leased(QUEUE, "b", QUEUE_MODE_GROUP));
    }

    /// A delete under a replayed handle still works, which is the point of
    /// answering the same handles rather than merely the same bodies.
    #[tokio::test]
    async fn a_replayed_handle_still_deletes() {
        let rig = seeded(&[("g", 2)]).await;
        let params = json!({"MaxNumberOfMessages": 2, "ReceiveRequestAttemptId": "attempt-1"});
        rig.receive_list(QUEUE, params.clone()).await;
        let replayed = rig.receive_list(QUEUE, params).await;
        delete(&rig, &replayed[0]).await.unwrap();
        assert_eq!(committed(&rig, "g"), 0);
    }

    /// The window is the visibility timeout: once the messages are no longer in
    /// flight, the id names nothing and the receive is an ordinary one — with
    /// NEW handles, because the delivery is a new one.
    #[tokio::test]
    async fn an_attempt_id_expires_with_the_messages_it_answered() {
        let rig = seeded(&[("g", 2)]).await;
        let params = json!({"MaxNumberOfMessages": 2, "ReceiveRequestAttemptId": "attempt-1"});
        let first = rig.receive_list(QUEUE, params.clone()).await;
        rig.fake.advance(Duration::from_secs(VISIBILITY + 1));
        let again = rig.receive_list(QUEUE, params).await;
        assert_eq!(bodies(&again), bodies(&first));
        assert_ne!(
            field(&again[0], "ReceiptHandle"),
            field(&first[0], "ReceiptHandle"),
            "a new delivery is a new lease and a new handle"
        );
    }

    /// AN EMPTY ANSWER IS NOT REMEMBERED. The record hands back messages that
    /// are in flight, and a receive that claimed none has nothing to hand back —
    /// so remembering it would answer every later receive under that id with an
    /// instant empty result for the whole visibility window, past whatever
    /// arrived in the meantime and without spending the long poll.
    #[tokio::test]
    async fn an_empty_receive_is_not_remembered_for_its_attempt_id() {
        let rig = seeded(&[]).await;
        let params = json!({"MaxNumberOfMessages": 10, "ReceiveRequestAttemptId": "attempt-1"});
        assert!(rig.receive_list(QUEUE, params.clone()).await.is_empty());
        assert!(
            rig.fake
                .kv_get(NS, &Registry::key_receive_attempt(QUEUE, "attempt-1"))
                .is_none(),
            "an empty answer is not an attempt to replay"
        );

        // ...and the same id sees what arrives afterwards, rather than replaying
        // the emptiness for a visibility timeout.
        rig.fake.seed(QUEUE, "g", 0, &[payload("g0")]);
        assert_eq!(bodies(&rig.receive_list(QUEUE, params).await), ["g0"]);
    }

    /// DIVERGENCE, `accepted`, pinned: AWS replays an attempt id only *"if none
    /// of the messages have been modified"*, and this replays it for as long as
    /// the record lives. What the client gets is handles for messages that are
    /// gone — and a delete under one answers success and deletes nothing, which
    /// is AWS's own contract for a stale handle.
    #[tokio::test]
    async fn an_attempt_id_replays_even_after_its_messages_were_deleted() {
        let rig = seeded(&[("g", 2)]).await;
        let params = json!({"MaxNumberOfMessages": 2, "ReceiveRequestAttemptId": "attempt-1"});
        let first = rig.receive_list(QUEUE, params.clone()).await;
        for message in &first {
            delete(&rig, message).await.unwrap();
        }
        assert_eq!(committed(&rig, "g"), 1, "the batch is deleted");

        let replayed = rig.receive_list(QUEUE, params).await;
        assert_eq!(replayed, first, "the answer is replayed all the same");
        delete(&rig, &replayed[0])
            .await
            .expect("a stale handle answers success");
        assert_eq!(committed(&rig, "g"), 1, "and deletes nothing");
    }

    #[tokio::test]
    async fn a_different_attempt_id_is_a_different_receive() {
        let rig = seeded(&[("a", 1), ("b", 1)]).await;
        let first = rig
            .receive_list(QUEUE, json!({"ReceiveRequestAttemptId": "one"}))
            .await;
        let second = rig
            .receive_list(QUEUE, json!({"ReceiveRequestAttemptId": "two"}))
            .await;
        assert_eq!(bodies(&first), ["a0"]);
        assert_eq!(bodies(&second), ["b0"]);
    }

    /// A replay CLAIMS NOTHING: the answer is served from the record before the
    /// pop, so a retrying client cannot take a second lease on the group its
    /// first attempt is already holding.
    #[tokio::test]
    async fn a_replayed_receive_takes_no_claim_at_all() {
        let rig = seeded(&[("g", 2)]).await;
        rig.fake.kv_seed_ttl(
            NS,
            &Registry::key_receive_attempt(QUEUE, "attempt-1"),
            json!({"m": [{"MessageId": "the-winners", "Body": "already answered"}]}),
            Some(30),
        );
        let answered = rig
            .receive_list(QUEUE, json!({"ReceiveRequestAttemptId": "attempt-1"}))
            .await;
        assert_eq!(bodies(&answered), ["already answered"]);
        assert!(
            rig.fake.committed(QUEUE, "g", QUEUE_MODE_GROUP).is_none(),
            "the group was never popped"
        );
    }

    /// Losing the race means another request stored an answer for this id first.
    /// The client gets THAT answer — one attempt, one set of messages.
    #[tokio::test]
    async fn a_lost_attempt_id_race_answers_the_winner() {
        let rig = seeded(&[("g", 2)]).await;
        rig.fake.kv_seed_ttl(
            NS,
            &Registry::key_receive_attempt(QUEUE, "attempt-1"),
            json!({"m": [{"Body": "the winner's"}]}),
            Some(30),
        );
        let mine = vec![json!({"Body": "mine"})];
        let answered = remember(&rig.ctx, QUEUE, "attempt-1", &mine, 30)
            .await
            .expect("the store answers")
            .expect("the winner's messages");
        assert_eq!(answered, vec![json!({"Body": "the winner's"})]);
    }

    /// ...and the claim the loser took is handed straight back, rather than left
    /// to block its group for a whole visibility timeout for an answer nobody
    /// will read. It is released at the HEAD — the message the pop answered
    /// first — so it commits nothing.
    #[tokio::test]
    async fn a_claim_the_loser_took_is_given_straight_back() {
        let rig = seeded(&[("g", 2)]).await;
        let popped = rig
            .fake
            .pop_queue(
                QUEUE,
                &PopOptions {
                    batch: 2,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        assert!(rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));

        release(&rig.ctx, &popped.messages[0]).await;
        assert!(!rig.fake.leased(QUEUE, "g", QUEUE_MODE_GROUP));
        assert_eq!(committed(&rig, "g"), -1);
        assert_eq!(bodies(&receive_n(&rig, 2).await), ["g0", "g1"]);
    }

    /// An answer too large to remember is not remembered, and the receive still
    /// answers it: the alternative is failing a request whose messages are
    /// already claimed, or lying about what was stored.
    #[tokio::test]
    async fn an_answer_too_large_to_remember_is_still_answered() {
        let rig = Rig::new(&[(QUEUE, &[])]).await;
        let big = "x".repeat(MAX_REMEMBERED_BYTES + 1);
        rig.fake.seed(QUEUE, "g", 0, &[payload(&big)]);
        let messages = rig
            .receive_list(QUEUE, json!({"ReceiveRequestAttemptId": "attempt-1"}))
            .await;
        assert_eq!(messages.len(), 1);
        assert!(rig
            .fake
            .kv_get(NS, &Registry::key_receive_attempt(QUEUE, "attempt-1"))
            .is_none());
    }

    /// The id becomes a key component, so it is held to AWS's own rule before it
    /// reaches the store.
    #[tokio::test]
    async fn an_attempt_id_is_held_to_the_fifo_id_rules() {
        let rig = seeded(&[("g", 1)]).await;
        for bad in ["a b", "\u{7f}", &"x".repeat(129)] {
            let error = rig
                .receive(QUEUE, json!({"ReceiveRequestAttemptId": bad}))
                .await
                .expect_err("the id is refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue);
            assert!(
                error.message.contains("ReceiveRequestAttemptId"),
                "{}",
                error.message
            );
        }
        // The longest legal one is legal.
        assert_eq!(
            rig.receive_list(QUEUE, json!({"ReceiveRequestAttemptId": "x".repeat(128)}))
                .await
                .len(),
            1
        );
    }

    /// A standard queue has no group whose order a retry could disturb, so the
    /// parameter is ACCEPTED and does nothing — the M1 rule about which
    /// direction to be wrong in.
    #[tokio::test]
    async fn an_attempt_id_is_accepted_and_ignored_on_a_standard_queue() {
        let rig = Rig::standard().await;
        rig.seed_lanes("orders", 2);
        let first = rig
            .receive_list("orders", json!({"ReceiveRequestAttemptId": "attempt-1"}))
            .await;
        let second = rig
            .receive_list("orders", json!({"ReceiveRequestAttemptId": "attempt-1"}))
            .await;
        assert_eq!(first.len(), 1);
        assert_eq!(second.len(), 1);
        assert_ne!(
            field(&first[0], "MessageId"),
            field(&second[0], "MessageId"),
            "nothing was replayed"
        );
        assert!(rig
            .fake
            .kv_get(NS, &Registry::key_receive_attempt("orders", "attempt-1"))
            .is_none());
    }

    // ---------------------------------------------------- two instances

    /// The sentence the whole design protects, for the one structure M2 adds:
    /// the delete-set is the STORE's, so the instance that serves the delete
    /// does not have to be the one that served the receive.
    #[tokio::test]
    async fn another_instance_serves_the_delete_from_the_same_record() {
        let rig = seeded(&[("g", 3)]).await;
        let messages = receive_n(&rig, 3).await;
        let other = rig.sibling();

        other
            .delete(QUEUE, field(&messages[1], "ReceiptHandle"))
            .await
            .unwrap();
        assert_eq!(committed(&rig, "g"), -1, "out of order: recorded only");
        // A third instance closes the gap and the prefix acks across all three.
        rig.sibling()
            .delete(QUEUE, field(&messages[0], "ReceiptHandle"))
            .await
            .unwrap();
        assert_eq!(committed(&rig, "g"), 1);
        assert_eq!(record(&rig, "g").await.deleted.len(), 2);
    }

    /// Two instances marking two members of one claim both land: the write is a
    /// compare-and-set on the record's version, so neither can overwrite the
    /// other's mark.
    #[tokio::test]
    async fn two_instances_marking_one_claim_do_not_lose_a_mark() {
        let rig = seeded(&[("g", 4)]).await;
        let messages = receive_n(&rig, 4).await;
        let (a, b) = (rig.sibling(), rig.sibling());
        a.delete(QUEUE, field(&messages[3], "ReceiptHandle"))
            .await
            .unwrap();
        b.delete(QUEUE, field(&messages[1], "ReceiptHandle"))
            .await
            .unwrap();
        let set = record(&rig, "g").await;
        assert_eq!(set.deleted.len(), 2, "both marks are in the record");
        assert_eq!(set.prefix(), 0);
        assert_eq!(committed(&rig, "g"), -1);
    }
}
