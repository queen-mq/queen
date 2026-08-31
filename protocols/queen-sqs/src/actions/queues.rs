//! Queue lifecycle and attributes.
//!
//! CONTRACT. Each function below takes the decoded parameters and answers the
//! result payload, in the shape both codecs render. A queue is TWO things that
//! must be created and destroyed together — a Queen queue (`/configure`) and a
//! registry record (`qs:q:<name>`) — and the order is fixed: the registry
//! record is claimed FIRST, with `putIfAbsent`, so that two instances racing for
//! one name produce one queue and one loser, and the loser has the winner's
//! attributes in hand to compare against.
//!
//! The rules that are decisions rather than translation:
//!
//!   * **A standard queue is M synthesized partitions**, decimal-named
//!     `"0".."M-1"`, M from `queen.partitions` (default
//!     [`crate::config::DEFAULT_PARTITIONS`]) and IMMUTABLE afterwards —
//!     partition counts never shrink, and a queue whose width changed would
//!     strand messages on lanes nothing pops.
//!   * **A `.fifo` suffix is a FIFO queue**, where a lane IS a `MessageGroupId`
//!     and there is no synthesized width at all.
//!   * **`CreateQueue` is idempotent** unless the request CONTRADICTS the queue
//!     that is there: `QueueAlreadyExists` only when an attribute the request
//!     supplies differs from the queue's current value, which is why the loser
//!     of the CAS compares rather than erroring. The comparison is
//!     one-directional and lives in [`crate::registry::Registry::create`], with
//!     AWS's sentence for it.
//!   * **`configure_queue_v1` is an UPSERT**: it rewrites every config column it
//!     was not given to the stored procedure's defaults. So the options bag is
//!     always built WHOLE, from the registry record plus the change, and never
//!     as a patch.
//!   * **`PurgeQueue` is delete-and-recreate**, answered asynchronously the way
//!     AWS answers it, with the 60-second cooldown emulated because SDK retry
//!     behaviour depends on it.
//!   * **Depth attributes are load-bearing**: `ApproximateNumberOfMessages` and
//!     `…NotVisible` are what KEDA and every autoscaler read.
//!
//! ## Two consequences of the UPSERT, which are the whole shape of this module
//!
//! **A queue Queen already has and this facade's registry does not know is NOT
//! adopted** ([`create_queue`]): `/configure` would rewrite a live native
//! queue's `leaseTime`, its retry budget and — worst — turn retention ON at four
//! days, which deletes data nobody asked to delete. The refusal is
//! `QueueAlreadyExists`, AWS's own code for "a queue of this name exists and is
//! not the one you described". The probe costs one admin call and only on the
//! path that actually creates.
//!
//! **`DeleteQueue` removes the Queen queue FIRST and the registry record
//! second.** A crash between the two then leaves a record whose broker half is
//! gone, which the next `CreateQueue` repairs (the record matches, so the create
//! is idempotent and re-`/configure`s); the other order leaves an orphan Queen
//! queue that the guard above would refuse to create over, for ever.
//!
//! ## Attributes: two catalogs, not one
//!
//! [`crate::registry`] owns what may be WRITTEN (its `MUTABLE`/`CREATE_ONLY`
//! tables, and the ranges) and is the single source of truth for it. This module
//! owns what may be READ — [`READABLE`] — which is a different set: `QueueArn`
//! and the three depth counts are readable and unsettable, and an attribute a
//! future version stored is readable because the record itself carries it.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use crate::actions::{queen_error, Ctx};
use crate::config::Config;
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::registry::{Naming, QueueRecord};

// ------------------------------------------------------------------ the numbers

/// SQS's own default visibility timeout, which becomes Queen's `leaseTime`.
pub const DEFAULT_VISIBILITY_TIMEOUT: i64 = 30;
/// Four days: SQS's default `MessageRetentionPeriod`, and therefore the
/// `retentionSeconds` an SQS queue is created with.
pub const DEFAULT_MESSAGE_RETENTION: i64 = 345_600;
/// 256 KiB, SQS's default `MaximumMessageSize`.
pub const DEFAULT_MAXIMUM_MESSAGE_SIZE: i64 = 262_144;
/// A queue's default per-message delay, which is Queen's `delayedProcessing`.
pub const DEFAULT_DELAY_SECONDS: i64 = 0;
/// The default long-poll wait a `ReceiveMessage` that names none inherits.
pub const DEFAULT_RECEIVE_WAIT_SECONDS: i64 = 0;
/// `retryLimit` for a queue whose `RedrivePolicy` names no `maxReceiveCount`.
///
/// It is the stored procedure's own default (012_configure.sql), and on the SQS
/// path it is nearly inert: the budget is charged by ack status `failed`, which
/// this facade never sends — a terminate is `retry` and a delete is `completed`
/// — and redrive is the facade's atomic move, not the broker's DLQ. It is a
/// backstop for a native producer sharing the queue, and nothing else.
pub const DEFAULT_RETRY_LIMIT: i64 = 3;
/// The dedup window a `.fifo` queue is created with: five minutes, which is
/// AWS's own `MessageDeduplicationId` window, so a FIFO queue behaves like SQS's
/// with no attribute set at all.
pub const FIFO_DEDUP_WINDOW_SECONDS: i64 = 300;
/// A standard queue's, which is OFF: SQS standard queues are at-least-once and
/// deduplicate nothing, and a window here would silently swallow a legitimate
/// retry of the same body.
pub const STANDARD_DEDUP_WINDOW_SECONDS: i64 = 0;
/// AWS's cap on `ListQueues`' `MaxResults`, and the page size a request that
/// names none gets.
pub const MAX_LIST_RESULTS: i64 = 1_000;

/// The window widener this facade adds on top of AWS's five minutes — the
/// superset PLAN_QUEEN_SQS.md sells. Spelled here rather than imported because
/// [`crate::registry`] carries it as a row of its `MUTABLE` table and has no
/// constant to lend.
pub const ATTR_DEDUP_WINDOW: &str = "queen.dedupWindowSeconds";

// ------------------------------------------------------- the readable catalog

/// `arn:aws:sqs:<region>:<account>:<name>`, as the record pinned it at create.
pub const ATTR_QUEUE_ARN: &str = "QueueArn";
/// Messages a `ReceiveMessage` could return right now.
pub const ATTR_MESSAGES: &str = "ApproximateNumberOfMessages";
/// Messages under a live lease — SQS's "in flight".
pub const ATTR_MESSAGES_NOT_VISIBLE: &str = "ApproximateNumberOfMessagesNotVisible";
/// Messages waiting on a timer, which is where per-message `DelaySeconds` lives.
pub const ATTR_MESSAGES_DELAYED: &str = "ApproximateNumberOfMessagesDelayed";
/// Epoch SECONDS, which is the unit SQS reports and not the record's own.
pub const ATTR_CREATED: &str = "CreatedTimestamp";
/// When the registry record was last written — epoch seconds, like
/// [`ATTR_CREATED`]. AWS answers it on every queue, and Terraform and the CLI
/// ask for it by name.
pub const ATTR_MODIFIED: &str = "LastModifiedTimestamp";

/// The attributes that are computed on every read rather than stored, and each
/// of which costs something: the ARN and the timestamp cost nothing, the two
/// depth counts cost one `/depth` call between them, and the delayed count costs
/// one timers count. They are fetched only when they are asked for.
const COMPUTED: &[&str] = &[
    ATTR_QUEUE_ARN,
    ATTR_CREATED,
    ATTR_MODIFIED,
    ATTR_MESSAGES,
    ATTR_MESSAGES_NOT_VISIBLE,
    ATTR_MESSAGES_DELAYED,
];

/// The attributes a queue reports when nothing was stored for them. They are
/// AWS's documented defaults, and they are ANSWERED rather than omitted because
/// a client that reads `VisibilityTimeout` to compute its own heartbeat gets
/// nothing useful from an absence.
const DEFAULTED: &[(&str, i64)] = &[
    ("VisibilityTimeout", DEFAULT_VISIBILITY_TIMEOUT),
    ("MessageRetentionPeriod", DEFAULT_MESSAGE_RETENTION),
    ("MaximumMessageSize", DEFAULT_MAXIMUM_MESSAGE_SIZE),
    ("DelaySeconds", DEFAULT_DELAY_SECONDS),
    (
        "ReceiveMessageWaitTimeSeconds",
        DEFAULT_RECEIVE_WAIT_SECONDS,
    ),
];

/// Every attribute name `GetQueueAttributes` will answer for, on top of whatever
/// the record itself carries.
///
/// A name outside this set AND outside the record is `InvalidAttributeName`,
/// which is the answer a client that asked for something it does not get needs —
/// an absence would be read as a value. That makes the set itself load-bearing:
/// every attribute AWS answers on every queue has to be IN it, because a client
/// naming one gets a hard 400 where the real service returns a number.
pub const READABLE: &[&str] = &[
    ATTR_QUEUE_ARN,
    ATTR_CREATED,
    ATTR_MODIFIED,
    ATTR_MESSAGES,
    ATTR_MESSAGES_NOT_VISIBLE,
    ATTR_MESSAGES_DELAYED,
    "VisibilityTimeout",
    "MessageRetentionPeriod",
    "MaximumMessageSize",
    "DelaySeconds",
    "ReceiveMessageWaitTimeSeconds",
    "RedrivePolicy",
    "RedriveAllowPolicy",
    "Policy",
    "FifoQueue",
    "ContentBasedDeduplication",
    "DeduplicationScope",
    "FifoThroughputLimit",
    "KmsMasterKeyId",
    "KmsDataKeyReusePeriodSeconds",
    "SqsManagedSseEnabled",
    crate::registry::ATTR_PARTITIONS,
    ATTR_DEDUP_WINDOW,
];

/// The name that asks for all of them.
const ALL: &str = "All";

// ------------------------------------------------------------------- actions

/// `CreateQueue`. Answers `{"QueueUrl": …}`.
pub async fn create_queue(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = require_text(params, "QueueName")?.to_string();
    let attributes = param_map(params, "Attributes")?;
    let tags = tags_of(params)?;
    let facade = &ctx.facade;

    // The guard the module header explains: a queue Queen has and the registry
    // does not is NOT this facade's to reconfigure. The registry read is the
    // cheap half and answers for every repeat of a CreateQueue an SDK makes at
    // start-up; the queue list is only reached on the path that would really
    // create something, and never for a name the registry will refuse anyway —
    // a client error must not be answered with the broker's health.
    //
    // It is [`crate::queen::Catalog::has`] and NOT `refresh`: the list is a
    // snapshot either way, so refreshing does not close the race against a queue
    // a native producer creates in this same instant, while a full
    // `/api/v1/resources/queues` per request would turn a create storm into load
    // on the broker's slowest admin route. The queue this guard protects is a
    // LIVE one, which is not three seconds old.
    if name_is_plausible(&facade.config, &name) {
        let known = facade
            .registry
            .queue(&name, ctx.token())
            .await
            .map_err(store)?;
        if known.is_none()
            && facade
                .catalog
                .has(&name, ctx.token())
                .await
                .map_err(store)?
        {
            return Err(SqsError::with(
                ErrorKind::QueueAlreadyExists,
                format!(
                    "A queue already exists with the name {name} and was not created through this \
                     endpoint; its configuration is not this endpoint's to replace"
                ),
            ));
        }
    }

    // Before the record is claimed, because a `RedrivePolicy` that names
    // nothing is a queue that silently stops dead-lettering — the failure
    // nobody notices until the source queue is full. It needs the store (the
    // target must exist and be the same queue type), so it cannot live in the
    // registry's own pure validation beside the ranges.
    super::dlq::check(ctx, &name, &attributes).await?;
    let record = facade
        .registry
        .create(
            &name,
            &attributes,
            &tags,
            &naming(&facade.config),
            facade.config.default_partitions,
            ctx.token(),
        )
        .await?;
    // Second, and only after the record is claimed: a `/configure` that landed
    // before the CAS would leave a Queen queue no registry record owns, which is
    // exactly what the guard above then refuses for ever.
    facade
        .catalog
        .configure(&name, &queue_options(&record), ctx.token())
        .await
        .map_err(store)?;
    Ok(json!({ "QueueUrl": queue_url(ctx, &name) }))
}

/// `DeleteQueue`. Answers an empty result, and leaves the cooldown marker that
/// makes a too-fast re-create `QueueDeletedRecently`.
pub async fn delete_queue(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let facade = &ctx.facade;
    // The registry is the SQS truth about existence, so it is what answers
    // QueueDoesNotExist — before anything is removed anywhere.
    facade.registry.require(&name, ctx.token()).await?;
    // Queen's half first (module header). Its own `existed` is deliberately not
    // read: a record whose broker half is already gone is a delete that was
    // interrupted, and reporting QueueDoesNotExist for it would leave the record
    // undeletable.
    facade
        .catalog
        .delete(&name, ctx.token())
        .await
        .map_err(store)?;
    facade.registry.delete(&name, ctx.token()).await?;
    Ok(Value::Null)
}

/// `GetQueueUrl`. `QueueDoesNotExist` when the registry does not have it — a URL
/// minted for a queue that is not there would fail later, somewhere else.
pub async fn get_queue_url(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = require_text(params, "QueueName")?.to_string();
    // `QueueOwnerAWSAccountId` is accepted and ignored: cross-account anything is
    // out of PLAN_QUEEN_SQS.md by name, and one account segment is configured
    // per deployment.
    ctx.facade.registry.require(&name, ctx.token()).await?;
    Ok(json!({ "QueueUrl": queue_url(ctx, &name) }))
}

/// `ListQueues`, with `QueueNamePrefix`, `MaxResults` and `NextToken`.
pub async fn list_queues(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let prefix = param_text(params, "QueueNamePrefix").unwrap_or_default();
    let limit = match param_int(params, "MaxResults")? {
        None => MAX_LIST_RESULTS,
        Some(n) if (1..=MAX_LIST_RESULTS).contains(&n) => n,
        Some(_) => {
            return Err(invalid(
                "MaxResults",
                format!("must be 1 to {MAX_LIST_RESULTS}"),
            ))
        }
    };
    let page = ctx
        .facade
        .registry
        .list(
            prefix,
            limit as usize,
            param_text(params, "NextToken"),
            ctx.token(),
        )
        .await?;
    let urls: Vec<Value> = page
        .queues
        .iter()
        .map(|q| Value::String(queue_url(ctx, &q.name)))
        .collect();
    let mut answer = serde_json::Map::new();
    // An account with no queues answers no list at all, which is what AWS does
    // and what an SDK's paginator reads as the end.
    if !urls.is_empty() {
        answer.insert("QueueUrls".to_string(), Value::Array(urls));
    }
    if let Some(next) = page.next_token {
        answer.insert("NextToken".to_string(), Value::String(next));
    }
    Ok(Value::Object(answer))
}

/// `GetQueueAttributes`. `AttributeNames=All` is the common case; an unknown
/// name is `InvalidAttributeName` rather than an omission, because a client that
/// asked for something it does not get would read the absence as a value.
pub async fn get_queue_attributes(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let record = ctx.facade.registry.require(&name, ctx.token()).await?;
    let stored = effective_attributes(&record);
    let wanted = requested(params, &stored)?;

    let mut answer = BTreeMap::new();
    for (attribute, value) in stored {
        if wanted.contains(&attribute) {
            answer.insert(attribute, value);
        }
    }
    if wanted.contains(ATTR_QUEUE_ARN) {
        answer.insert(ATTR_QUEUE_ARN.to_string(), queue_arn(ctx, &record));
    }
    if wanted.contains(ATTR_CREATED) {
        // SECONDS: SQS reports epoch seconds and the record keeps milliseconds,
        // and a client that parsed milliseconds as seconds reads the year 58000.
        answer.insert(
            ATTR_CREATED.to_string(),
            (record.created_ms / 1_000).to_string(),
        );
    }
    if wanted.contains(ATTR_MODIFIED) {
        answer.insert(
            ATTR_MODIFIED.to_string(),
            (record.modified_ms / 1_000).to_string(),
        );
    }
    if wanted.contains(ATTR_MESSAGES) || wanted.contains(ATTR_MESSAGES_NOT_VISIBLE) {
        // ONE call for the pair, AND IT NAMES THE GROUP. `None` there is not the
        // queue-mode group: it is the QUEUE-LEVEL number, which the stored
        // procedure computes under a precedence where named groups win as a
        // class and `__QUEUE_MODE__` speaks only when no named cursor exists,
        // and whose `processing` sums every live lease of every group
        // (011_log_stats.sql, log_queue_depth_v1). On a queue a native Queen
        // consumer also reads — the mixed consumption this facade is built for —
        // that answers the OTHER consumer's backlog, and KEDA and every
        // autoscaler read these two numbers.
        let depth = ctx
            .facade
            .queen
            .queue_depth(&name, Some(crate::queen::QUEUE_MODE_GROUP), ctx.token())
            .await
            .map_err(store)?;
        if wanted.contains(ATTR_MESSAGES) {
            answer.insert(ATTR_MESSAGES.to_string(), depth.ready.to_string());
        }
        if wanted.contains(ATTR_MESSAGES_NOT_VISIBLE) {
            answer.insert(
                ATTR_MESSAGES_NOT_VISIBLE.to_string(),
                depth.processing.to_string(),
            );
        }
    }
    if wanted.contains(ATTR_MESSAGES_DELAYED) {
        // Every timer THIS FACADE scheduled on the queue: a delayed send is a
        // timer, and every one it writes is keyed under
        // [`crate::actions::messages::TIMER_KEY_PREFIX`]. The prefix is not
        // decoration — the count route refuses an empty one (`mode=count
        // requires a non-empty prefix`, in the handler and again in the stored
        // procedure), so an unprefixed count is a 400 and this attribute would
        // never be answerable. It also leaves a native producer's timers on the
        // same queue out of a number that is answering an SQS question.
        let delayed = ctx
            .facade
            .queen
            .timers_count(
                &name,
                crate::actions::messages::TIMER_KEY_PREFIX,
                ctx.token(),
            )
            .await
            .map_err(store)?;
        answer.insert(ATTR_MESSAGES_DELAYED.to_string(), delayed.to_string());
    }

    // A request that named no attributes answers no attributes — AWS's own
    // behaviour, and the reason `Attributes` is omitted rather than sent empty.
    Ok(match answer.is_empty() {
        true => json!({}),
        false => json!({ "Attributes": answer }),
    })
}

/// `SetQueueAttributes`. Reads the record, applies the change, writes both
/// halves under a CAS on the version it read.
pub async fn set_queue_attributes(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let changes = param_map(params, "Attributes")?;
    if changes.is_empty() {
        return Err(missing_parameter("Attribute.Name"));
    }
    // The same check `CreateQueue` makes, and it has to be made again here:
    // `RedrivePolicy` is a MUTABLE attribute, so a queue can acquire a
    // dead-letter target long after it was created — and a target that was
    // deleted in between is exactly the case a create-time-only check would
    // miss.
    super::dlq::check(ctx, &name, &changes).await?;
    // The registry owns the writable catalog, the ranges, and the CAS. What
    // comes back is the WHOLE record, which is what the options bag must be
    // built from: `configure_queue_v1` is an upsert and a partial bag would
    // reset every column it did not name.
    let record = ctx
        .facade
        .registry
        .set_attributes(&name, &changes, ctx.token())
        .await?;
    reconcile(ctx, record).await?;
    Ok(Value::Null)
}

/// Configure the broker from `record`, then make sure the broker ended up
/// configured from the record that WON.
///
/// The registry's compare-and-set orders the two records; nothing orders the two
/// `/configure` calls that follow them. Two concurrent `SetQueueAttributes` can
/// therefore interleave so that the loser's record is what every
/// `GetQueueAttributes` reports while the WINNER's — or rather, the one whose
/// configure landed last — is what the broker runs. One of the columns involved
/// is `retentionSeconds`, so the disagreement is not cosmetic: a queue that says
/// it retains for weeks would be deleting on the other value's clock.
///
/// So the write is verified: re-read the record, and if it has moved since the
/// configure, configure again from what is there now. Each pass either agrees
/// with the store or configures from a strictly newer record, and the loop is
/// bounded — under contention this exact enough that three passes disagree, the
/// answer is the retriable code an SDK already backs off on, and the next
/// successful `SetQueueAttributes` on the queue reconciles it anyway.
async fn reconcile(ctx: &Ctx, record: QueueRecord) -> SqsResult<()> {
    const PASSES: usize = 3;
    let mut record = record;
    for _ in 0..PASSES {
        let configured = record.version;
        ctx.facade
            .catalog
            .configure(&record.name, &queue_options(&record), ctx.token())
            .await
            .map_err(store)?;
        // FRESH, never the cached snapshot: the whole question is whether
        // another instance wrote between the CAS and the configure above.
        let current = ctx
            .facade
            .registry
            .queue_fresh(&record.name, ctx.token())
            .await
            .map_err(store)?;
        match current {
            // Deleted underneath us. The queue is gone; there is nothing left to
            // agree about, and DeleteQueue removed the broker's half first.
            None => return Ok(()),
            Some(current) if current.version == configured => return Ok(()),
            Some(current) => record = current,
        }
    }
    Err(SqsError::with(
        ErrorKind::ServiceUnavailable,
        format!(
            "Concurrent updates to queue {}; the attributes were stored but may not have reached \
             the queue service. Please retry.",
            record.name
        ),
    ))
}

/// `PurgeQueue`: delete the Queen queue and create it again from the registry
/// record — PLAN_QUEEN_SQS.md's D3, shipped as delete-and-recreate.
///
/// Queen has no truncate (C-SQS-2 is a recorded option and not a milestone), so
/// emptying a queue means removing it: the delete cascades through
/// `queen.log_partitions` to the segments and the consumer cursors
/// (001_log_schema.sql, `delete_queue_v1`), and the `/configure` that follows
/// puts the queue back with the same options bag its record maps onto. What the
/// client sees is a queue that still exists, still has its attributes, its tags
/// and its `CreatedTimestamp`, and has nothing in it.
///
/// DIVERGENCE, `deliberate`: AWS answers `PurgeQueue` immediately and empties
/// the queue in the background (*"the message deletion process takes up to 60
/// seconds"*); this does the whole thing inside the request, so the client's
/// call is as slow as the work. The work is one delete, one configure and the
/// cancel of whatever delayed messages the queue holds ([`purge_delayed`], one
/// round trip each and bounded by its own ceiling), so a queue with a backlog of
/// delayed sends is where it is felt.
///
/// It is synchronous because the alternative is worse HERE, not because async
/// is hard: the state this facade would have to spawn a task against is the
/// registry record, and a purge that returned before deleting would leave the
/// queue answering receives for messages it has told the client are gone — with
/// no task record for anybody to poll, unlike [`super::movetask`], where AWS
/// gives the client a handle to ask with. What a client that times out mid-purge
/// sees is the 60-second cooldown its own call claimed
/// (`PurgeQueueInProgress`), which is the same answer AWS gives a client that
/// purges twice in a minute — and the purge it started completes or is redone by
/// the retry after the window.
///
/// **Every receipt handle minted before this call stops addressing anything.**
/// The lanes are new rows with new `gen_random_uuid()` ids, and a handle carries
/// the partition id an ack is keyed by, so a delete presented after a purge
/// resolves to no consumer — it cannot delete a message that arrived since,
/// which is the one outcome that would be data loss. AWS says the same thing in
/// its own words: messages sent before the purge may still be received for up
/// to a minute, and the handles for them are not honoured afterwards.
///
/// ## The four orderings, each load-bearing
///
///   1. **The window is claimed first.** It is `putIfAbsent` in the store, so
///      two instances cannot both be inside the delete-and-recreate — the
///      second one's delete would land between the first's delete and its
///      recreate, leaving a queue whose record says it is there. A purge that
///      then FAILS holds the window anyway, which is the safe direction: nothing
///      it touched is half-done that a retry a minute later cannot redo, and the
///      alternative is releasing a lock while another instance is inside it.
///   2. **The delayed messages go before the queue does** ([`purge_delayed`]),
///      because they are the one part of a queue its own delete does not reach.
///   3. **The broker's halves in the DeleteQueue order** (delete, then
///      configure), so a crash between them leaves a record whose broker half is
///      gone — which the next CreateQueue repairs — rather than a Queen queue no
///      record owns, which the CreateQueue guard refuses for ever.
///   4. **The record is written back under a CAS on the version it was read
///      at**, which is how this call finds out what happened underneath it: a
///      concurrent `DeleteQueue` (the record is absent, so the pure update
///      writes nothing) or a concurrent `SetQueueAttributes` (someone else's
///      record is the one clients now read, so the broker must run theirs).
pub async fn purge_queue(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let facade = &ctx.facade;
    // FRESH, for the reason every write path here reads fresh: the CAS below is
    // on this version, and a cached one is a version that may already have been
    // superseded.
    let record = facade
        .registry
        .queue_fresh(&name, ctx.token())
        .await
        .map_err(store)?
        .ok_or_else(|| {
            SqsError::with(
                ErrorKind::QueueDoesNotExist,
                format!("The specified queue does not exist: {name}"),
            )
        })?;

    if !facade
        .registry
        .begin_purge(&name, ctx.token())
        .await
        .map_err(store)?
    {
        return Err(SqsError::with(
            ErrorKind::PurgeQueueInProgress,
            format!(
                "Only one PurgeQueue operation on each queue is allowed every {} seconds.",
                crate::registry::PURGE_COOLDOWN.as_secs()
            ),
        ));
    }

    purge_delayed(ctx, &name).await?;
    facade
        .catalog
        .delete(&name, ctx.token())
        .await
        .map_err(store)?;
    facade
        .catalog
        .configure(&name, &queue_options(&record), ctx.token())
        .await
        .map_err(store)?;

    match facade
        .registry
        .put_queue(&record, ctx.token())
        .await
        .map_err(store)?
    {
        Ok(_) => Ok(Value::Null),
        // Version 0 is an ABSENCE and not a competitor: a `DeleteQueue` ran
        // inside this purge, both of its halves between both of ours. The queue
        // just recreated belongs to no record — and that is exactly what the
        // CreateQueue guard refuses to adopt, for ever — so it goes back.
        Err(winner) if winner.version == 0 => {
            facade
                .catalog
                .delete(&name, ctx.token())
                .await
                .map_err(store)?;
            Err(SqsError::with(
                ErrorKind::QueueDoesNotExist,
                format!("The specified queue does not exist: {name}"),
            ))
        }
        // A `SetQueueAttributes` ran inside this purge. Its record is the one
        // every client now reads, so the broker has to run it too — the same
        // disagreement [`reconcile`] exists to close, reached the same way.
        Err(winner) => reconcile(ctx, *winner).await.map(|()| Value::Null),
    }
}

/// Cancel the delayed messages THIS FACADE scheduled on a queue, which is the
/// half of a purge the queue's own delete does not perform.
///
/// `queen.log_timers` is keyed `PRIMARY KEY (tenant_id, queue, timer_key)` on
/// the queue's NAME and carries no foreign key at all (025_log_timers.sql), so
/// the cascade off `queen.queues(id)` cannot reach it. 031_tenant_purge.sql says
/// what that means in its own words: *"a surviving timer keeps producing … into
/// a queue this purge just deleted, which push then AUTO-CREATES"* — which on
/// this path is a message arriving minutes after a client was told the queue was
/// emptied, and being counted in `ApproximateNumberOfMessagesDelayed` until it
/// does.
///
/// Only keys under [`crate::actions::messages::TIMER_KEY_PREFIX`]: a native
/// Queen producer's timers on the same queue are not SQS delayed messages, and
/// they are already excluded from the attribute that counts them. An SQS purge
/// is not a licence to cancel another producer's work.
///
/// BOUNDED, and loud at the bound. The alternative is an unbounded loop of round
/// trips inside one request. The ceiling is far above what a queue can hold —
/// `DelaySeconds` is at most fifteen minutes, so the population is the send rate
/// times 900 seconds — and a purge that reaches it has left something behind,
/// which has to be a log line rather than a silence.
async fn purge_delayed(ctx: &Ctx, queue: &str) -> SqsResult<()> {
    /// Rows one list call asks for. Well inside the route's own clamp.
    const PAGE: i64 = 200;
    /// The most one purge will cancel.
    const MAX_CANCELLED: usize = 5_000;

    let prefix = crate::actions::messages::TIMER_KEY_PREFIX;
    let mut after: Option<String> = None;
    let mut cancelled = 0usize;
    // A bound on the WALK as well as on the cancels: the loop's exit depends on
    // a cursor that another process computes.
    for _ in 0..(MAX_CANCELLED / PAGE as usize + 2) {
        let page = ctx
            .facade
            .queen
            .timers_list(queue, after.as_deref(), PAGE, ctx.token())
            .await
            .map_err(store)?;
        let last = page.rows.last().map(|row| row.timer_key.clone());
        for row in &page.rows {
            if !row.timer_key.starts_with(prefix) {
                continue;
            }
            if cancelled >= MAX_CANCELLED {
                tracing::warn!(
                    target: "sqs",
                    queue,
                    cancelled,
                    "PurgeQueue stopped cancelling delayed messages at its ceiling; the ones \
                     past it will still be delivered"
                );
                return Ok(());
            }
            // The DELETE route, which is the one a quota may never block
            // (server/src/handlers/timers.rs §9.6): the fire does not switch
            // itself off, so a purge that could not cancel would leave a queue
            // producing messages nobody can stop.
            ctx.facade
                .queen
                .timers_cancel(queue, &row.timer_key, None, ctx.token())
                .await
                .map_err(store)?;
            cancelled += 1;
        }
        // The page ended the listing, or it cannot be continued from — either
        // way there is nothing more this walk can ask for.
        let Some(next) = page.truncated.then_some(page.next_after.or(last)).flatten() else {
            return Ok(());
        };
        after = Some(next);
    }
    tracing::warn!(
        target: "sqs",
        queue,
        cancelled,
        "PurgeQueue stopped walking the delayed messages at its page ceiling"
    );
    Ok(())
}

/// `ListQueueTags` / `TagQueue` / `UntagQueue`. Tags live in the registry
/// record and never reach Queen.
pub async fn list_queue_tags(
    ctx: &Ctx,
    params: &serde_json::Value,
) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let tags = ctx.facade.registry.tags(&name, ctx.token()).await?;
    // An untagged queue answers no `Tags` member, as AWS does: an empty map and
    // an absent one are the same fact, and only one of them is what the service
    // sends.
    Ok(match tags.is_empty() {
        true => json!({}),
        false => json!({ "Tags": tags }),
    })
}

pub async fn tag_queue(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let tags = tags_of(params)?;
    if tags.is_empty() {
        return Err(missing_parameter("Tags"));
    }
    ctx.facade.registry.tag(&name, &tags, ctx.token()).await?;
    Ok(Value::Null)
}

pub async fn untag_queue(ctx: &Ctx, params: &serde_json::Value) -> SqsResult<serde_json::Value> {
    let name = queue_of(ctx, params)?;
    let keys = param_list(params, "TagKeys")?;
    if keys.is_empty() {
        return Err(missing_parameter("TagKeys"));
    }
    ctx.facade.registry.untag(&name, &keys, ctx.token()).await?;
    Ok(Value::Null)
}

// `ListDeadLetterSourceQueues` and the message-move-task trio live in
// [`super::dlq`] and [`super::movetask`], with the redrive move they are the
// other half of. A queue action that scanned for a `RedrivePolicy` here would
// be the second reader of that document in this crate, and the first one is the
// one that has to agree with the mover.

// -------------------------------------------------------------- the mapping

/// The WHOLE options bag a queue's record maps onto, which is the only shape
/// `/configure` may be called with (the stored procedure is an upsert that
/// rewrites every column it was not given).
///
/// Five of the seven are translations. The two that are decisions:
///
///   * **`deadLetterQueue` is ALWAYS false.** Queen's native DLQ stays out of
///     the SQS path entirely: redrive here is the facade's atomic
///     push-to-DLQ-plus-ack move against a real SQS queue
///     (PLAN_QUEEN_SQS.md, DLQ), and a broker DLQ underneath it would take the
///     message first and take it somewhere no SQS client can address.
///   * **`retentionEnabled` is ALWAYS true**, because `MessageRetentionPeriod`
///     is not optional in SQS — every queue has one, four days by default — and
///     `retentionSeconds` alone is inert (server/src/retention.rs selects on the
///     flag). `completedRetentionSeconds` is deliberately NOT set: it deletes
///     CONSUMED data on a clock the SQS API does not expose, and the plan
///     decides nothing about it.
pub fn queue_options(record: &QueueRecord) -> Value {
    let number = |attribute: &str| {
        record
            .attributes
            .get(attribute)
            .and_then(|v| v.trim().parse::<i64>().ok())
    };
    let dedup_default = match record.fifo {
        true => FIFO_DEDUP_WINDOW_SECONDS,
        false => STANDARD_DEDUP_WINDOW_SECONDS,
    };
    json!({
        "leaseTime": number("VisibilityTimeout").unwrap_or(DEFAULT_VISIBILITY_TIMEOUT),
        "retryLimit": max_receive_count(record).unwrap_or(DEFAULT_RETRY_LIMIT),
        "deadLetterQueue": false,
        "delayedProcessing": number("DelaySeconds").unwrap_or(DEFAULT_DELAY_SECONDS),
        "retentionSeconds": number("MessageRetentionPeriod").unwrap_or(DEFAULT_MESSAGE_RETENTION),
        "retentionEnabled": true,
        "dedupWindowSeconds": number(ATTR_DEDUP_WINDOW).unwrap_or(dedup_default),
    })
}

/// `RedrivePolicy.maxReceiveCount`, which is how many deliveries a message gets
/// before the facade moves it to the dead-letter queue. ONE reader of that
/// document, in [`super::dlq`], because the number the broker's `retryLimit` is
/// set from and the number the move is decided on must be the same number.
pub use super::dlq::max_receive_count;

/// Every attribute a queue reports before the computed ones: what was stored,
/// over AWS's documented defaults for the five that always have a value.
///
/// TWO readers, and the second is the reason this is not private:
/// [`crate::registry::Registry::create`] compares a `CreateQueue`'s attributes
/// against THIS map rather than against the stored one. On AWS a queue always has a
/// value for each of the five, so a request that supplies AWS's own default
/// against a queue created bare supplies nothing that differs — and a client
/// that reads a queue's attributes and hands them straight back to
/// `CreateQueue` must never be refused for it.
///
/// THREE attributes AWS also always reports have NO default here, and the
/// omission is a decision. DIVERGENCE, `deliberate`: `SqsManagedSseEnabled`
/// would have to be answered `true`, which claims an encryption at rest this
/// facade does not perform (SSE beyond accept-and-report is out of
/// PLAN_QUEEN_SQS.md by name); `DeduplicationScope` and `FifoThroughputLimit`
/// describe a FIFO throughput model whose truthful value here is not AWS's
/// default — Queen deduplicates per PARTITION, which is per message group. Each
/// is stored and answered when a client sets it. What it costs is a client that
/// supplies AWS's own default for one of them against a queue created bare: it
/// is refused with `QueueAlreadyExists` rather than accepted as describing the
/// queue that is there.
pub(crate) fn effective_attributes(record: &QueueRecord) -> BTreeMap<String, String> {
    let mut out: BTreeMap<String, String> = DEFAULTED
        .iter()
        .map(|(name, value)| ((*name).to_string(), value.to_string()))
        .collect();
    if record.fifo {
        // AWS reports it on every FIFO queue whether or not it was set, and a
        // client branches on it to decide whether to send a dedup id.
        out.insert("ContentBasedDeduplication".to_string(), "false".to_string());
    }
    out.extend(record.attributes.clone());
    out
}

/// The attribute names one `GetQueueAttributes` asks for, `All` expanded.
///
/// A name that is neither in [`READABLE`] nor in the record itself is
/// `InvalidAttributeName`: the record's own keys are always readable, so an
/// attribute a later version stores does not become unreadable by an older
/// catalog.
fn requested(params: &Value, stored: &BTreeMap<String, String>) -> SqsResult<BTreeSet<String>> {
    let names = param_list(params, "AttributeNames")?;
    let mut wanted = BTreeSet::new();
    for name in names {
        if name == ALL {
            wanted.extend(COMPUTED.iter().map(|a| (*a).to_string()));
            wanted.extend(stored.keys().cloned());
            continue;
        }
        if !READABLE.contains(&name.as_str()) && !stored.contains_key(&name) {
            return Err(SqsError::with(
                ErrorKind::InvalidAttributeName,
                format!("Unknown Attribute {name}."),
            ));
        }
        wanted.insert(name);
    }
    Ok(wanted)
}

// -------------------------------------------------------------- lane shapes

// THERE IS ONE LANE FUNCTION AND IT LIVES IN `messages`
// ([`crate::actions::messages::lane_for`]). This module used to carry a second,
// with a different hash and a doc comment that named a different input — the
// MessageId, which is the BROKER's uuid and does not exist until the push has
// landed, so it cannot address the lane the push is going to. Two public
// functions of the same name and shape, only one of them reachable, is a trap
// for whichever caller M3 or PurgeQueue writes next: it would place messages on
// lanes the send path never chose.

/// Whether a queue name is a FIFO queue's. One rule, in
/// [`crate::registry::is_fifo`], because a name that was stored as FIFO and read
/// back as standard would be a queue nothing can consume in order.
pub fn is_fifo(name: &str) -> bool {
    crate::registry::is_fifo(name)
}

// ------------------------------------------------------------------- naming

/// The region and account every URL and ARN this deployment mints comes from.
pub fn naming(config: &Config) -> Naming {
    Naming::new(&config.region, &config.account)
}

/// The URL a queue of this name is addressed by, for the host the CLIENT
/// reached.
///
/// The scheme is this process's own TLS setting, which is the only signal the
/// action layer has: [`Ctx`] carries the request's `Host` and not its scheme.
/// Behind a TLS-terminating load balancer that is WRONG and the operator must
/// front the fleet with one that rewrites it — the same caveat every
/// self-hosted S3 and SQS endpoint carries. When `Ctx` grows a `scheme`, this is
/// the one line that reads it.
pub fn queue_url(ctx: &Ctx, name: &str) -> String {
    let config = &ctx.facade.config;
    let scheme = match config.tls {
        Some(_) => "https",
        None => "http",
    };
    let host = match ctx.host.trim().is_empty() {
        true => config.listen.as_str(),
        false => ctx.host.as_str(),
    };
    naming(config).url(scheme, host, name)
}

/// Whether the registry could accept this name at all, decided WITHOUT a call.
///
/// It is the registry's OWN predicate, reached through the one public function
/// that applies it: [`Naming::name_of`] parses a URL back to a name only when
/// the name is one AWS would have accepted. Restating the charset rule here
/// instead would put a second source of truth in front of the first, and the two
/// directions of a disagreement are not symmetric — a local rule that is
/// stricter than the registry's would silently skip the guard above for a name
/// the registry then creates.
///
/// It gates a probe and nothing else: the authoritative refusal, with AWS's own
/// sentence, is still [`crate::registry::Registry::create`]'s.
fn name_is_plausible(config: &Config, name: &str) -> bool {
    let naming = naming(config);
    // The scheme and host are placeholders: `name_of` reads neither, and this
    // string never leaves the function.
    naming.name_of(&naming.url("http", "gate", name)).as_deref() == Some(name)
}

/// The ARN the record pinned at create, or — for a record written before this
/// facade stored one — this deployment's own. Never empty: `QueueArn` is what
/// every `RedrivePolicy` and every IAM-shaped config names a queue by.
fn queue_arn(ctx: &Ctx, record: &QueueRecord) -> String {
    match record.arn.is_empty() {
        true => naming(&ctx.facade.config).arn(&record.name),
        false => record.arn.clone(),
    }
}

/// The queue an action addresses, from the `QueueUrl` every one of them takes.
///
/// A URL this facade did not mint is `QueueDoesNotExist` and NOT a parse error:
/// it is what AWS answers for another account's queue, and the client's own
/// string is never echoed back — it is unbounded input that would land in this
/// facade's log and in the client's.
pub fn queue_of(ctx: &Ctx, params: &Value) -> SqsResult<String> {
    let url = require_text(params, "QueueUrl")?;
    naming(&ctx.facade.config)
        .name_of(url)
        .ok_or_else(|| SqsError::new(ErrorKind::QueueDoesNotExist))
}

// -------------------------------------------------------- parameter reading

/// One string parameter, or `None`.
///
/// A form carries only strings and a JSON body carries whatever the SDK's model
/// says, so every reader in this module accepts both spellings of a scalar. That
/// is the ONE place the two protocols do not converge (`proto::query`'s module
/// header), and converging it needs to know which parameters are numbers —
/// action knowledge, which is here.
pub fn param_text<'a>(params: &'a Value, name: &str) -> Option<&'a str> {
    params.get(name).and_then(Value::as_str)
}

/// The same, refused when it is absent or empty.
///
/// NOT trimmed: ` orders` is not a queue name AWS accepts, and a reader that
/// quietly turned it into one would create a queue under a name the client did
/// not send and cannot reproduce. Whatever is there goes to the rule that owns
/// it, which answers `InvalidParameterValue` with AWS's own sentence.
pub fn require_text<'a>(params: &'a Value, name: &str) -> SqsResult<&'a str> {
    param_text(params, name)
        .filter(|text| !text.is_empty())
        .ok_or_else(|| missing_parameter(name))
}

/// One integer parameter, from either protocol's spelling of one.
pub fn param_int(params: &Value, name: &str) -> SqsResult<Option<i64>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(n)) => n
            .as_i64()
            .map(Some)
            .ok_or_else(|| invalid(name, "must be an integer")),
        Some(Value::String(s)) => s
            .trim()
            .parse::<i64>()
            .map(Some)
            .map_err(|_| invalid(name, "must be an integer")),
        Some(_) => Err(invalid(name, "must be an integer")),
    }
}

/// One `name → value` map (`Attributes`, `Tags`), which both protocols deliver
/// as a JSON object.
pub fn param_map(params: &Value, name: &str) -> SqsResult<BTreeMap<String, String>> {
    let Some(value) = params.get(name).filter(|v| !v.is_null()) else {
        return Ok(BTreeMap::new());
    };
    let object = value
        .as_object()
        .ok_or_else(|| invalid(name, "must be a map of names to values"))?;
    let mut out = BTreeMap::new();
    for (key, value) in object {
        let text =
            scalar(value).ok_or_else(|| invalid(&format!("{name}.{key}"), "must be a string"))?;
        out.insert(key.clone(), text);
    }
    Ok(out)
}

/// One list parameter. A bare scalar is a list of one: a Query client may send
/// `AttributeName=All` unindexed, and the codec lifts that to a list already —
/// this covers the JSON client that does the same.
pub fn param_list(params: &Value, name: &str) -> SqsResult<Vec<String>> {
    match params.get(name) {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Array(items)) => items
            .iter()
            .map(|item| scalar(item).ok_or_else(|| invalid(name, "must be a list of strings")))
            .collect(),
        Some(single) => {
            Ok(vec![scalar(single).ok_or_else(|| {
                invalid(name, "must be a list of strings")
            })?])
        }
    }
}

/// `Tags` under either spelling. AWS lower-cases it on `CreateQueue` and nowhere
/// else; `proto::mirror_tags` puts both on the params object, and this reads
/// whichever survived a hand-built one.
fn tags_of(params: &Value) -> SqsResult<BTreeMap<String, String>> {
    let upper = param_map(params, "Tags")?;
    match upper.is_empty() {
        true => param_map(params, "tags"),
        false => Ok(upper),
    }
}

fn scalar(value: &Value) -> Option<String> {
    match value {
        Value::String(text) => Some(text.clone()),
        // A JSON client that typed `30` for a string attribute means the string
        // `"30"`, which is what its Query counterpart sends.
        Value::Number(number) => Some(number.to_string()),
        Value::Bool(flag) => Some(flag.to_string()),
        _ => None,
    }
}

// ------------------------------------------------------------------- errors

/// AWS's own sentence for a required parameter that is not there.
fn missing_parameter(name: &str) -> SqsError {
    SqsError::with(
        ErrorKind::MissingParameter,
        format!("The request must contain the parameter {name}."),
    )
}

/// ...and for a parameter that is there and wrong. Same phrasing as
/// [`crate::registry`]'s, so one grep finds every refusal of a value.
fn invalid(name: &str, why: impl std::fmt::Display) -> SqsError {
    SqsError::with(
        ErrorKind::InvalidParameterValue,
        format!("Invalid value for the parameter {name}: {why}"),
    )
}

/// A broker failure, through the one mapping ([`crate::error::SqsError::from_queen`]).
fn store(e: crate::queen::Error) -> SqsError {
    queen_error(&e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{AuthMode, ReceiveMode};
    use crate::credentials::Directory;
    use crate::handle::Handles;
    use crate::queen::testing::FakeQueen;
    use crate::queen::{Catalog, PopOptions, QueenApi, TimerSchedule};
    use crate::registry::Registry;
    use crate::Facade;
    use std::sync::Arc;
    use std::time::Duration;

    /// A facade over a fake broker, with every cache OFF: a test that counts
    /// calls to Queen, or that expects to see another instance's write, must not
    /// depend on which test ran before it.
    fn facade(api: &Arc<FakeQueen>) -> Arc<Facade> {
        let queen: Arc<dyn QueenApi> = api.clone();
        Arc::new(Facade {
            config: config(),
            catalog: Arc::new(Catalog::with_ttl(Arc::clone(&queen), Duration::ZERO)),
            registry: Arc::new(Registry::with_ttl(Arc::clone(&queen), Duration::ZERO)),
            handles: Handles::new(b"handle-secret"),
            credentials: Directory::empty(),
            queen,
        })
    }

    fn config() -> Config {
        Config {
            listen: crate::config::DEFAULT_LISTEN.to_string(),
            auth: AuthMode::Off,
            credentials: Directory::empty(),
            region: crate::config::DEFAULT_REGION.to_string(),
            account: crate::config::DEFAULT_ACCOUNT.to_string(),
            receive_mode: ReceiveMode::Exact,
            default_partitions: crate::config::DEFAULT_PARTITIONS,
            handle_secret: b"handle-secret".to_vec(),
            handle_secret_generated: false,
            queen_url: "http://localhost:6969".to_string(),
            queen_token: None,
            embedded: false,
            shutdown_grace_ms: 5_000,
            tls: None,
        }
    }

    fn ctx(api: &Arc<FakeQueen>) -> Ctx {
        Ctx {
            facade: facade(api),
            principal: crate::actions::Principal::default(),
            host: "sqs.example.test:9324".to_string(),
            request_id: "req-1".to_string(),
        }
    }

    fn url(name: &str) -> String {
        format!("http://sqs.example.test:9324/000000000000/{name}")
    }

    /// One CreateQueue, in the canonical (JSON) shape both codecs produce.
    async fn create(ctx: &Ctx, name: &str, attributes: Value) -> SqsResult<Value> {
        create_queue(ctx, &json!({ "QueueName": name, "Attributes": attributes })).await
    }

    /// The options bag one `/configure` was called with, by queue name.
    fn configured(api: &Arc<FakeQueen>, name: &str) -> Vec<Value> {
        api.configures
            .lock()
            .unwrap()
            .iter()
            .filter(|(q, _)| q == name)
            .map(|(_, options)| options.clone())
            .collect()
    }

    fn attributes_of(answer: &Value) -> BTreeMap<String, String> {
        answer
            .get("Attributes")
            .and_then(Value::as_object)
            .map(|map| {
                map.iter()
                    .map(|(k, v)| (k.clone(), v.as_str().unwrap_or_default().to_string()))
                    .collect()
            })
            .unwrap_or_default()
    }

    // ------------------------------------------------------------ CreateQueue

    /// Both halves, in the order the module header fixes, and the URL the client
    /// then posts to.
    #[tokio::test]
    async fn create_claims_the_record_then_configures_the_queen_queue() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let answer = create(&ctx, "orders", json!({})).await.expect("created");
        assert_eq!(answer, json!({ "QueueUrl": url("orders") }));

        let record = ctx
            .facade
            .registry
            .queue("orders", None)
            .await
            .expect("read")
            .expect("record");
        assert_eq!(record.partitions, 64, "the process default is stamped");
        assert_eq!(
            record.arn, "arn:aws:sqs:queen-1:000000000000:orders",
            "the ARN is pinned at create"
        );
        assert_eq!(configured(&api, "orders").len(), 1);
    }

    /// THE mapping, attribute by attribute. Every number here is a column in
    /// 012_configure.sql and a client-visible behaviour.
    #[tokio::test]
    async fn the_attributes_map_onto_the_whole_options_bag() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        // The dead-letter target must EXIST before a policy may name it (M3),
        // so the fixture creates it rather than the attribute naming a queue
        // that is not there.
        create(&ctx, "dead", json!({})).await.expect("the target");
        create(
            &ctx,
            "orders",
            json!({
                "VisibilityTimeout": "45",
                "DelaySeconds": "10",
                "MessageRetentionPeriod": "600",
                "RedrivePolicy": r#"{"maxReceiveCount":5,"deadLetterTargetArn":"arn:aws:sqs:queen-1:000000000000:dead"}"#,
            }),
        )
        .await
        .expect("created");

        assert_eq!(
            configured(&api, "orders")[0],
            json!({
                "leaseTime": 45,
                "retryLimit": 5,
                "deadLetterQueue": false,
                "delayedProcessing": 10,
                "retentionSeconds": 600,
                "retentionEnabled": true,
                "dedupWindowSeconds": 0,
            })
        );
    }

    /// A queue created with no attributes is the one the documentation
    /// describes: SQS's defaults, not the stored procedure's.
    #[tokio::test]
    async fn the_defaults_are_sqss_and_not_the_stored_procedures() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        assert_eq!(
            configured(&api, "orders")[0],
            json!({
                "leaseTime": 30,
                "retryLimit": 3,
                "deadLetterQueue": false,
                "delayedProcessing": 0,
                "retentionSeconds": 345_600,
                "retentionEnabled": true,
                "dedupWindowSeconds": 0,
            })
        );
    }

    /// `deadLetterQueue` is false on EVERY SQS queue, whatever the client asked
    /// for: the native DLQ would take the message before the facade's own
    /// redrive move could, and put it where no SQS client can address it.
    #[tokio::test]
    async fn the_native_dead_letter_queue_is_always_off() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "dead", json!({})).await.expect("the target");
        for (name, attributes) in [
            ("plain", json!({})),
            (
                "with-redrive",
                // A STRING maxReceiveCount, which is what Terraform writes.
                json!({"RedrivePolicy": r#"{"maxReceiveCount":"2","deadLetterTargetArn":"arn:aws:sqs:queen-1:000000000000:dead"}"#}),
            ),
        ] {
            create(&ctx, name, attributes).await.expect("created");
            assert_eq!(configured(&api, name)[0]["deadLetterQueue"], json!(false));
        }
        // ...and a string maxReceiveCount is read, because Terraform writes one.
        assert_eq!(configured(&api, "with-redrive")[0]["retryLimit"], json!(2));
    }

    /// A FIFO queue is created with AWS's own five-minute dedup window, and the
    /// `queen.` extension is what widens it — the superset the plan sells.
    #[tokio::test]
    async fn a_fifo_queue_gets_the_aws_dedup_window_and_the_extension_widens_it() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders.fifo", json!({"FifoQueue": "true"}))
            .await
            .expect("created");
        assert_eq!(
            configured(&api, "orders.fifo")[0]["dedupWindowSeconds"],
            json!(300)
        );

        create(
            &ctx,
            "wide.fifo",
            json!({"FifoQueue": "true", "queen.dedupWindowSeconds": "86400"}),
        )
        .await
        .expect("created");
        assert_eq!(
            configured(&api, "wide.fifo")[0]["dedupWindowSeconds"],
            json!(86_400)
        );
    }

    /// A FIFO queue synthesizes no lanes: a lane there IS a `MessageGroupId`.
    #[tokio::test]
    async fn a_fifo_queue_has_no_synthesized_width() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders.fifo", json!({"FifoQueue": "true"}))
            .await
            .expect("created");
        let record = ctx
            .facade
            .registry
            .queue("orders.fifo", None)
            .await
            .expect("read")
            .expect("record");
        assert_eq!(record.partitions, 0);
        assert!(record.fifo);
    }

    /// The `.fifo` edge of the idempotent create: the suffix already declares the
    /// type, so a re-create that does not repeat `FifoQueue` still answers the
    /// queue — and still configures it as the FIFO queue it is, five-minute dedup
    /// window included. For a queue that is NOT there, the same request is the
    /// bad create it always was.
    #[tokio::test]
    async fn a_fifo_queue_is_re_created_without_repeating_the_attribute() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = create(&ctx, "orders.fifo", json!({}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameterValue);

        create(&ctx, "orders.fifo", json!({"FifoQueue": "true"}))
            .await
            .expect("created");
        let answer = create(&ctx, "orders.fifo", json!({}))
            .await
            .expect("re-created");
        assert_eq!(answer, json!({"QueueUrl": url("orders.fifo")}));
        assert_eq!(
            configured(&api, "orders.fifo")[1],
            configured(&api, "orders.fifo")[0]
        );
        assert_eq!(
            configured(&api, "orders.fifo")[1]["dedupWindowSeconds"],
            json!(300)
        );
    }

    /// AWS's rule: a repeat of the same CreateQueue succeeds. It re-`/configure`s
    /// deliberately — the call is an idempotent upsert, and it is what repairs a
    /// queue whose broker half was lost.
    #[tokio::test]
    async fn an_identical_create_is_idempotent() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let first = create(&ctx, "orders", json!({"VisibilityTimeout": "45"}))
            .await
            .expect("created");
        let second = create(&ctx, "orders", json!({"VisibilityTimeout": "45"}))
            .await
            .expect("created again");
        assert_eq!(first, second);
        assert_eq!(configured(&api, "orders").len(), 2);
        assert_eq!(configured(&api, "orders")[0], configured(&api, "orders")[1]);
    }

    /// D1 (`compat/M0_SMOKE.md`): the create a worker performs at start-up, with
    /// no `Attributes` member at all, against a queue an operator configured. It
    /// answers the URL — and the `/configure` it makes on the way out is built
    /// from the EXISTING record, so the queue's retention and lease survive a
    /// fleet booting against it. A create that re-derived the options from the
    /// empty request would set retention back to four days, which deletes data.
    #[tokio::test]
    async fn a_create_that_names_no_attributes_answers_the_existing_queue() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(
            &ctx,
            "orders",
            json!({"VisibilityTimeout": "45", "MessageRetentionPeriod": "3600"}),
        )
        .await
        .expect("created");

        let answer = create(&ctx, "orders", json!({})).await.expect("re-created");
        assert_eq!(answer, json!({"QueueUrl": url("orders")}));
        assert_eq!(
            configured(&api, "orders")[1],
            configured(&api, "orders")[0],
            "the bare create reconfigured the queue from its own empty request"
        );
        assert_eq!(
            configured(&api, "orders")[1]["retentionSeconds"],
            json!(3600)
        );

        // ...and so does a SUBSET, which is the same request with one attribute
        // the client happens to remember.
        create(&ctx, "orders", json!({"VisibilityTimeout": "45"}))
            .await
            .expect("re-created");
    }

    /// Tags are not attributes (their own request member, their own three
    /// actions), so a re-create with different tags succeeds and leaves the
    /// queue's tags alone. `TagQueue` is the action that changes them.
    #[tokio::test]
    async fn a_re_create_with_different_tags_succeeds_and_does_not_retag() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create_queue(
            &ctx,
            &json!({"QueueName": "orders", "tags": {"team": "billing"}}),
        )
        .await
        .expect("created");

        create_queue(
            &ctx,
            &json!({"QueueName": "orders", "tags": {"team": "platform"}}),
        )
        .await
        .expect("re-created");
        assert_eq!(
            list_queue_tags(&ctx, &json!({"QueueUrl": url("orders")}))
                .await
                .expect("read"),
            json!({"Tags": {"team": "billing"}})
        );
    }

    /// The invariant across the two catalogs: every attribute
    /// `GetQueueAttributes` ANSWERS can be handed straight back to
    /// `CreateQueue`. A client that reads a queue and re-creates it from what it
    /// read is the shape D1 broke, and the defaulted five are exactly where the
    /// read view and the stored record disagree.
    #[tokio::test]
    async fn what_get_queue_attributes_answers_is_accepted_back_by_create_queue() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        let names: Vec<&str> = DEFAULTED.iter().map(|(name, _)| *name).collect();
        let read = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": names}),
        )
        .await
        .expect("read");
        let attributes = attributes_of(&read);
        assert_eq!(attributes.len(), DEFAULTED.len(), "{attributes:?}");

        create(&ctx, "orders", json!(attributes))
            .await
            .expect("the queue's own attributes were refused");
    }

    /// ...and `QueueAlreadyExists` only on a MISMATCH, which must not reconfigure
    /// the live queue on its way to the error.
    #[tokio::test]
    async fn a_create_with_different_attributes_is_refused_and_changes_nothing() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({"VisibilityTimeout": "45"}))
            .await
            .expect("created");
        let e = create(&ctx, "orders", json!({"VisibilityTimeout": "60"}))
            .await
            .expect_err("mismatch");
        assert_eq!(e.kind, ErrorKind::QueueAlreadyExists);
        assert_eq!(configured(&api, "orders").len(), 1, "nothing was rewritten");
    }

    /// The guard: a queue Queen already has that this facade's registry does not
    /// know is never adopted, because `/configure` would rewrite its columns —
    /// retention included, which deletes data.
    #[tokio::test]
    async fn a_native_queen_queue_is_never_reconfigured_into_an_sqs_one() {
        let api = FakeQueen::with(&["orders"]);
        let ctx = ctx(&api);
        let e = create(&ctx, "orders", json!({}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueAlreadyExists);
        assert!(configured(&api, "orders").is_empty());
        assert!(
            ctx.facade
                .registry
                .queue("orders", None)
                .await
                .expect("read")
                .is_none(),
            "and no record was claimed"
        );
    }

    #[tokio::test]
    async fn create_without_a_name_is_a_missing_parameter() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        for request in [json!({}), json!({"QueueName": ""})] {
            let e = create_queue(&ctx, &request).await.expect_err("refused");
            assert_eq!(e.kind, ErrorKind::MissingParameter);
        }
        assert!(api.configures.lock().unwrap().is_empty());
    }

    /// A name the registry refuses is a CLIENT error and must not cost the
    /// broker's slowest admin route: a scripted failure on the queue list proves
    /// the list was never called, because a call would have turned this into
    /// `ServiceUnavailable`.
    #[tokio::test]
    async fn an_illegal_name_costs_no_admin_list_and_configures_nothing() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        api.fail_list(crate::queen::Error::status(
            500,
            "the list would have been called",
        ));
        let e = create(&ctx, "not a name", json!({}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameterValue);
        assert!(api.configures.lock().unwrap().is_empty());
    }

    /// ...and neither does a REPEAT of a create, which is what an SDK and a
    /// provisioning run do on every start: the registry already knows the name,
    /// so the guard's expensive half is never reached.
    #[tokio::test]
    async fn a_repeat_create_costs_no_admin_list() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        api.fail_list(crate::queen::Error::status(
            500,
            "the list would have been called",
        ));
        create(&ctx, "orders", json!({}))
            .await
            .expect("created again");
    }

    /// The gate in front of that guard is the REGISTRY's own name rule, reached
    /// through the one public function that applies it. This is the test that
    /// keeps the two from drifting: a gate stricter than the registry would skip
    /// the guard for a name the registry then creates.
    #[test]
    fn the_probe_gate_is_the_registrys_own_name_rule() {
        let config = config();
        for (name, plausible) in [
            ("orders", true),
            ("Orders_2-x", true),
            ("orders.fifo", true),
            (&"q".repeat(80), true),
            (&"q".repeat(81), false),
            ("", false),
            ("with space", false),
            ("with.dot", false),
            ("a/b", false),
            ("../q", false),
            ("nul\0byte", false),
        ] {
            assert_eq!(name_is_plausible(&config, name), plausible, "{name:?}");
        }
    }

    #[tokio::test]
    async fn an_unsettable_attribute_is_refused_by_name() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = create(&ctx, "orders", json!({"Nonsense": "1"}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidAttributeName);
    }

    #[tokio::test]
    async fn tags_are_stored_at_create_under_either_spelling() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create_queue(
            &ctx,
            &json!({"QueueName": "orders", "tags": {"team": "billing"}}),
        )
        .await
        .expect("created");
        let answer = list_queue_tags(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect("listed");
        assert_eq!(answer, json!({"Tags": {"team": "billing"}}));
    }

    // ------------------------------------------------------------ DeleteQueue

    /// Both halves go, and the cooldown then refuses the re-create — the window
    /// every SDK's retry behaviour depends on.
    #[tokio::test]
    async fn delete_removes_both_halves_and_leaves_the_cooldown() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");

        let answer = delete_queue(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect("deleted");
        assert_eq!(answer, Value::Null, "an action with no output shape");
        assert_eq!(api.deletes.lock().unwrap().as_slice(), ["orders"]);
        assert!(ctx
            .facade
            .registry
            .queue("orders", None)
            .await
            .expect("read")
            .is_none());

        let e = create(&ctx, "orders", json!({}))
            .await
            .expect_err("cooldown");
        assert_eq!(e.kind, ErrorKind::QueueDeletedRecently);
    }

    /// The registry decides existence, so an unknown queue is refused before
    /// anything is removed from Queen.
    #[tokio::test]
    async fn deleting_a_queue_that_is_not_there_touches_nothing() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = delete_queue(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
        assert!(api.deletes.lock().unwrap().is_empty());
    }

    /// A delete that was interrupted after Queen's half must still be able to
    /// remove its record: the broker's `existed` is not read.
    #[tokio::test]
    async fn a_record_whose_queen_half_is_already_gone_is_still_deletable() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        api.delete_queue("orders", None).await.expect("gone");

        delete_queue(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect("deleted");
        assert!(ctx
            .facade
            .registry
            .queue("orders", None)
            .await
            .expect("read")
            .is_none());
    }

    // ------------------------------------------------------------ GetQueueUrl

    #[tokio::test]
    async fn get_queue_url_answers_the_host_the_client_reached() {
        let api = FakeQueen::empty();
        let mut ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        ctx.host = "queues.internal:9324".to_string();
        let answer = get_queue_url(&ctx, &json!({"QueueName": "orders"}))
            .await
            .expect("found");
        assert_eq!(
            answer,
            json!({"QueueUrl": "http://queues.internal:9324/000000000000/orders"})
        );
    }

    #[tokio::test]
    async fn get_queue_url_of_an_unknown_queue_does_not_mint_one() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = get_queue_url(&ctx, &json!({"QueueName": "orders"}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
        let e = get_queue_url(&ctx, &json!({})).await.expect_err("refused");
        assert_eq!(e.kind, ErrorKind::MissingParameter);
    }

    /// A URL another deployment minted names another account, and is answered the
    /// way AWS answers one: the queue does not exist here.
    #[tokio::test]
    async fn a_url_this_endpoint_did_not_mint_is_not_a_queue() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        for address in [
            "http://sqs.example.test:9324/999999999999/orders",
            "http://sqs.example.test:9324/000000000000/../etc",
            "orders",
            "",
        ] {
            let e = get_queue_attributes(&ctx, &json!({"QueueUrl": address}))
                .await
                .expect_err("refused");
            assert!(
                matches!(
                    e.kind,
                    ErrorKind::QueueDoesNotExist | ErrorKind::MissingParameter
                ),
                "{address}: {e:?}"
            );
        }
    }

    // ------------------------------------------------------------- ListQueues

    #[tokio::test]
    async fn list_queues_filters_by_prefix_and_pages_with_an_opaque_token() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        for name in ["orders-a", "orders-b", "orders-c", "other"] {
            create(&ctx, name, json!({})).await.expect("created");
        }

        let first = list_queues(&ctx, &json!({"QueueNamePrefix": "orders", "MaxResults": 2}))
            .await
            .expect("listed");
        assert_eq!(
            first["QueueUrls"],
            json!([url("orders-a"), url("orders-b")])
        );
        let token = first["NextToken"].as_str().expect("a second page");

        let second = list_queues(
            &ctx,
            &json!({"QueueNamePrefix": "orders", "MaxResults": 2, "NextToken": token}),
        )
        .await
        .expect("listed");
        assert_eq!(second["QueueUrls"], json!([url("orders-c")]));
        assert_eq!(second.get("NextToken"), None, "the end of the listing");
    }

    #[tokio::test]
    async fn an_empty_listing_answers_no_list_at_all() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        assert_eq!(
            list_queues(&ctx, &json!({"QueueNamePrefix": "nothing"}))
                .await
                .expect("listed"),
            json!({})
        );
    }

    #[tokio::test]
    async fn max_results_and_next_token_are_validated() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        for request in [
            json!({"MaxResults": 0}),
            json!({"MaxResults": 1001}),
            json!({"MaxResults": "many"}),
            json!({"NextToken": "not-a-token-this-facade-minted!!"}),
        ] {
            let e = list_queues(&ctx, &request).await.expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidParameterValue, "{request}");
        }
    }

    // ---------------------------------------------------- GetQueueAttributes

    /// `All`: what was stored, over AWS's defaults, plus the computed five. The
    /// depth pair is what KEDA reads and the numbers are the queue's own.
    #[tokio::test]
    async fn all_answers_the_stored_the_defaulted_and_the_computed() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({"VisibilityTimeout": "45"}))
            .await
            .expect("created");
        // Three messages on one lane, one of them claimed: SQS's "available" and
        // "in flight" are the two halves of that.
        api.seed(
            "orders",
            "0",
            1,
            &[json!({"b": "1"}), json!({"b": "2"}), json!({"b": "3"})],
        );
        api.pop_queue("orders", &PopOptions::default(), None)
            .await
            .expect("popped");
        // ...and one message waiting on a timer, which is a delayed send. It
        // carries the facade's own timer-key prefix, because the count is asked
        // for BY that prefix — an unprefixed count is a 400 at the broker, and a
        // native producer's timer on the same queue (the second one here) is not
        // an SQS delayed message and must not be counted as one.
        api.timers_schedule(
            &[
                TimerSchedule::new(
                    "orders",
                    &crate::actions::messages::timer_key("txn-1"),
                    "0",
                    60_000,
                    "txn-1",
                    "",
                ),
                TimerSchedule::new("orders", "native-key", "0", 60_000, "txn-2", ""),
            ],
            None,
        )
        .await
        .expect("scheduled");

        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["All"]}),
        )
        .await
        .expect("read");
        let attributes = attributes_of(&answer);

        assert_eq!(attributes["VisibilityTimeout"], "45", "stored wins");
        assert_eq!(attributes["MessageRetentionPeriod"], "345600");
        assert_eq!(attributes["MaximumMessageSize"], "262144");
        assert_eq!(attributes["DelaySeconds"], "0");
        assert_eq!(attributes["ReceiveMessageWaitTimeSeconds"], "0");
        assert_eq!(
            attributes[ATTR_QUEUE_ARN],
            "arn:aws:sqs:queen-1:000000000000:orders"
        );
        assert_eq!(attributes[crate::registry::ATTR_PARTITIONS], "64");
        assert_eq!(attributes[ATTR_MESSAGES], "2", "available, not total");
        assert_eq!(attributes[ATTR_MESSAGES_NOT_VISIBLE], "1", "in flight");
        assert_eq!(
            attributes[ATTR_MESSAGES_DELAYED], "1",
            "the facade's delayed send, not the native producer's timer"
        );
        // Seconds, not the record's milliseconds: a client that parsed the
        // latter as the former reads a timestamp fifty thousand years out.
        let created: i64 = attributes[ATTR_CREATED].parse().expect("a number");
        assert!(
            (1_600_000_000..4_000_000_000).contains(&created),
            "{created}"
        );
        // AWS answers this on every queue, so `All` has to carry it and an
        // explicit request for it has to be answerable.
        let modified: i64 = attributes[ATTR_MODIFIED].parse().expect("a number");
        assert!(
            (1_600_000_000..4_000_000_000).contains(&modified),
            "{modified}"
        );
    }

    /// THE REGISTRY'S CAS ORDERS THE RECORDS; NOTHING ORDERS THE `/configure`
    /// CALLS THAT FOLLOW THEM. Two concurrent `SetQueueAttributes` can therefore
    /// interleave so that the broker ends up running the record that LOST while
    /// every `GetQueueAttributes` reports the one that won — and one of the
    /// columns involved is `retentionSeconds`, so the disagreement deletes data
    /// on a clock the client cannot see.
    ///
    /// The write is verified instead: re-read, and configure again from what is
    /// actually stored.
    #[tokio::test]
    async fn a_configure_is_reconciled_against_the_record_that_won() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({"VisibilityTimeout": "30"}))
            .await
            .expect("created");

        // Another instance's SetQueueAttributes, landing between this one's CAS
        // and its own `/configure`. The two `None`s let the read and the write
        // of this request's own CAS through untouched; the third entry is the
        // competitor, applied just before the re-read.
        let competitor = json!({
            "attributes": {"VisibilityTimeout": "90", "queen.partitions": "64",
                           "MessageRetentionPeriod": "60"},
            "tags": {},
            "createdTs": 1_787_011_200_000i64,
            "modifiedTs": 1_787_011_200_001i64,
            "arn": "arn:aws:sqs:queen-1:000000000000:orders",
        });
        api.kv_interpose.lock().unwrap().extend([
            None,
            None,
            Some(crate::queen::KvOp::put(
                crate::registry::NS,
                "qs:q:orders",
                competitor,
            )),
        ]);

        set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Attributes": {"VisibilityTimeout": "45"}}),
        )
        .await
        .expect("stored");

        let options = configured(&api, "orders");
        let last = options.last().expect("the queue was configured");
        assert_eq!(
            last["leaseTime"],
            json!(90),
            "the broker kept the record that lost: {options:?}"
        );
        assert_eq!(
            last["retentionSeconds"],
            json!(60),
            "and its retention with it"
        );
        // What the client reads and what the broker runs now agree.
        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["VisibilityTimeout"]}),
        )
        .await
        .expect("read");
        assert_eq!(attributes_of(&answer)["VisibilityTimeout"], "90");
    }

    /// THE DEPTH IS THE SQS QUEUE'S AND NOT THE LOUDEST CONSUMER'S. Asking with
    /// no group is the QUEUE-LEVEL number, which the stored procedure computes
    /// under a precedence where named groups win as a class and
    /// `__QUEUE_MODE__` speaks only when no named cursor exists — so on a queue
    /// a native Queen consumer also reads (the mixed consumption this facade is
    /// built for) it answers that consumer's backlog. KEDA and every autoscaler
    /// read these two numbers.
    #[tokio::test]
    async fn the_depth_is_the_queue_mode_groups_and_not_another_consumers() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        api.seed(
            "orders",
            "0",
            0,
            &[json!({"b": "1"}), json!({"b": "2"}), json!({"b": "3"})],
        );

        // A native consumer group reads the lane to the end and commits it.
        let popped = api
            .pop_queue(
                "orders",
                &PopOptions {
                    batch: 10,
                    consumer_group: Some("native-analytics".to_string()),
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .expect("popped");
        assert_eq!(popped.messages.len(), 3);
        for message in &popped.messages {
            api.ack(
                &crate::queen::AckItem::completed(
                    &message.transaction_id,
                    &message.partition_id,
                    &message.lease_id,
                ),
                Some("native-analytics"),
                None,
            )
            .await
            .expect("acked");
        }

        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"),
                    "AttributeNames": [ATTR_MESSAGES, ATTR_MESSAGES_NOT_VISIBLE]}),
        )
        .await
        .expect("read");
        let attributes = attributes_of(&answer);
        assert_eq!(
            attributes[ATTR_MESSAGES], "3",
            "the SQS queue has consumed nothing; another group's cursor is not its backlog"
        );
        assert_eq!(attributes[ATTR_MESSAGES_NOT_VISIBLE], "0");
    }

    /// AWS answers `LastModifiedTimestamp` on every queue, so a client naming it
    /// must not get the hard 400 that an unknown attribute name gets. (That a
    /// WRITE moves it is the registry's own property and is pinned there.)
    #[tokio::test]
    async fn the_modified_timestamp_is_answerable_by_name() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": [ATTR_MODIFIED]}),
        )
        .await
        .expect("named, not refused");
        let attributes = attributes_of(&answer);
        assert_eq!(attributes.len(), 1);
        let modified: i64 = attributes[ATTR_MODIFIED].parse().expect("a number");
        assert!(
            (1_600_000_000..4_000_000_000).contains(&modified),
            "{modified}"
        );
    }

    /// A named filter answers exactly what was asked — and the depth call is not
    /// made for a request that did not ask for a depth.
    #[tokio::test]
    async fn a_named_filter_costs_only_what_it_asks_for() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        // A queue Queen does not have at all: the depth route would 404, so a
        // request that answers without one proves it never called.
        api.delete_queue("orders", None).await.expect("gone");

        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["QueueArn", "VisibilityTimeout"]}),
        )
        .await
        .expect("read");
        assert_eq!(
            attributes_of(&answer).keys().collect::<Vec<_>>(),
            vec!["QueueArn", "VisibilityTimeout"]
        );

        // ...and one that DOES ask for a depth reports the broker's own answer.
        let e = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": [ATTR_MESSAGES]}),
        )
        .await
        .expect_err("404");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
    }

    /// EVERY attribute the plan names, with AWS's own type and unit for each.
    /// The types are the whole of this test: a client parses `CreatedTimestamp`
    /// as seconds, `ApproximateNumberOfMessages` as an integer and
    /// `RedrivePolicy` as an EMBEDDED JSON DOCUMENT, and each of the three is a
    /// different way to be wrong.
    #[tokio::test]
    async fn every_attribute_the_plan_names_comes_back_with_its_aws_type() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let redrive = r#"{"maxReceiveCount":5,"deadLetterTargetArn":"arn:aws:sqs:queen-1:000000000000:dead"}"#;
        create(&ctx, "dead", json!({})).await.expect("the target");
        create(
            &ctx,
            "orders",
            json!({
                "VisibilityTimeout": "45",
                "MessageRetentionPeriod": "600",
                "MaximumMessageSize": "2048",
                "DelaySeconds": "7",
                "ReceiveMessageWaitTimeSeconds": "3",
                "RedrivePolicy": redrive,
                "RedriveAllowPolicy": r#"{"redrivePermission":"allowAll"}"#,
                "Policy": r#"{"Version":"2012-10-17"}"#,
                "KmsMasterKeyId": "alias/aws/sqs",
                "KmsDataKeyReusePeriodSeconds": "300",
                "SqsManagedSseEnabled": "true",
                "queen.dedupWindowSeconds": "600",
            }),
        )
        .await
        .expect("created");
        api.seed("orders", "0", 0, &[json!({"b": "1"})]);

        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["All"]}),
        )
        .await
        .expect("read");
        let attributes = attributes_of(&answer);

        // Everything the plan names is answerable, and `All` answers it.
        for name in [
            "VisibilityTimeout",
            "MessageRetentionPeriod",
            "MaximumMessageSize",
            "DelaySeconds",
            "ReceiveMessageWaitTimeSeconds",
            "RedrivePolicy",
            "RedriveAllowPolicy",
            "Policy",
            "KmsMasterKeyId",
            "KmsDataKeyReusePeriodSeconds",
            "SqsManagedSseEnabled",
            ATTR_QUEUE_ARN,
            ATTR_CREATED,
            ATTR_MODIFIED,
            ATTR_MESSAGES,
            ATTR_MESSAGES_NOT_VISIBLE,
            ATTR_MESSAGES_DELAYED,
            ATTR_DEDUP_WINDOW,
            crate::registry::ATTR_PARTITIONS,
        ] {
            assert!(attributes.contains_key(name), "{name} is not answered");
        }

        // The three counts are integers rendered as strings, never a JSON
        // number and never a float.
        for name in [
            ATTR_MESSAGES,
            ATTR_MESSAGES_NOT_VISIBLE,
            ATTR_MESSAGES_DELAYED,
        ] {
            assert!(
                answer["Attributes"][name].is_string(),
                "{name} is not a string"
            );
            attributes[name]
                .parse::<i64>()
                .unwrap_or_else(|_| panic!("{name} = {} is not an integer", attributes[name]));
        }

        // SECONDS, and ten digits until the year 2286: a client that read the
        // record's own milliseconds here would place the queue in the year
        // 58000.
        for name in [ATTR_CREATED, ATTR_MODIFIED] {
            let seconds: i64 = attributes[name].parse().expect("a number");
            assert!(
                (1_600_000_000..4_000_000_000).contains(&seconds),
                "{name} = {seconds} is not epoch seconds"
            );
        }

        // The redrive policy is an EMBEDDED document: the string a client set,
        // byte for byte, and it must parse back to the same object.
        assert_eq!(attributes["RedrivePolicy"], redrive);
        assert_eq!(
            serde_json::from_str::<Value>(&attributes["RedrivePolicy"]).expect("json"),
            serde_json::from_str::<Value>(redrive).expect("json")
        );
    }

    /// An attribute the record CARRIES is readable whatever this version's
    /// catalog knows, because the record's own keys are always in the answerable
    /// set: a queue configured by a later version of this facade must not become
    /// half-unreadable to an older one, which would answer a hard 400 for an
    /// attribute that is demonstrably there.
    #[tokio::test]
    async fn an_attribute_a_later_version_stored_is_still_readable() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        // Written straight to the store, the way a newer instance would have.
        let mut stored = api.kv_get(crate::registry::NS, "qs:q:orders").expect("row");
        stored["attributes"]["queen.somethingNew"] = json!("7");
        api.kv_seed(crate::registry::NS, "qs:q:orders", stored);

        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["queen.somethingNew"]}),
        )
        .await
        .expect("the record's own keys are readable");
        assert_eq!(attributes_of(&answer)["queen.somethingNew"], "7");

        // …and `All` carries it too, beside the ones this version knows.
        let all = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["All"]}),
        )
        .await
        .expect("read");
        assert_eq!(attributes_of(&all)["queen.somethingNew"], "7");

        // What is neither in the catalog nor in the record is still refused.
        let e = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["queen.neverStored"]}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidAttributeName);
    }

    /// `ContentBasedDeduplication` is MUTABLE on real SQS — it is a knob a queue
    /// is meant to be switched between, unlike `FifoQueue` — and the change is
    /// real rather than merely stored: with it on, a send with no
    /// `MessageDeduplicationId` is accepted and keyed by the body's own digest.
    #[tokio::test]
    async fn content_based_deduplication_is_mutable_and_the_change_is_real() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders.fifo", json!({"FifoQueue": "true"}))
            .await
            .expect("created");
        let send = json!({
            "QueueUrl": url("orders.fifo"),
            "MessageBody": "hello",
            "MessageGroupId": "g",
        });
        let e = crate::actions::messages::send_message(&ctx, &send)
            .await
            .expect_err("no dedup id and no content-based dedup");
        assert_eq!(e.kind, ErrorKind::InvalidParameterValue);

        set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders.fifo"),
                    "Attributes": {"ContentBasedDeduplication": "true"}}),
        )
        .await
        .expect("mutable, as it is on AWS");

        crate::actions::messages::send_message(&ctx, &send)
            .await
            .expect("the same send now carries the body's own digest");
        let pushed = api.pushed();
        assert_eq!(pushed.len(), 1);
        assert_eq!(
            pushed[0].transaction_id.as_deref(),
            Some(hex::encode(<sha2::Sha256 as sha2::Digest>::digest(b"hello")).as_str())
        );
        // …and it comes back as what was set.
        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders.fifo"),
                    "AttributeNames": ["ContentBasedDeduplication"]}),
        )
        .await
        .expect("read");
        assert_eq!(attributes_of(&answer)["ContentBasedDeduplication"], "true");
    }

    /// The two timestamps are the record's own two fields, in SECONDS. The unit
    /// and the source are what this pins; that a write MOVES the modified one is
    /// a property of the registry and is pinned there, in milliseconds, where a
    /// test can see it move.
    #[tokio::test]
    async fn the_timestamps_are_the_records_own_two_fields_in_seconds() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Attributes": {"DelaySeconds": "5"}}),
        )
        .await
        .expect("set");

        let record = ctx
            .facade
            .registry
            .queue_fresh("orders", None)
            .await
            .expect("read")
            .expect("record");
        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"),
                    "AttributeNames": [ATTR_CREATED, ATTR_MODIFIED]}),
        )
        .await
        .expect("read");
        let attributes = attributes_of(&answer);
        assert_eq!(
            attributes[ATTR_CREATED],
            (record.created_ms / 1_000).to_string()
        );
        assert_eq!(
            attributes[ATTR_MODIFIED],
            (record.modified_ms / 1_000).to_string()
        );
        assert!(
            record.modified_ms >= record.created_ms,
            "the set moved the created timestamp"
        );
    }

    #[tokio::test]
    async fn an_unknown_attribute_name_is_refused_rather_than_omitted() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        for name in ["Nonsense", "SentTimestamp", "all"] {
            let e = get_queue_attributes(
                &ctx,
                &json!({"QueueUrl": url("orders"), "AttributeNames": [name]}),
            )
            .await
            .expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidAttributeName, "{name}");
        }
    }

    /// A request that named no attributes answers none, which is AWS's own
    /// behaviour and not an empty map.
    #[tokio::test]
    async fn no_attribute_names_answers_no_attributes() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        assert_eq!(
            get_queue_attributes(&ctx, &json!({"QueueUrl": url("orders")}))
                .await
                .expect("read"),
            json!({})
        );
    }

    #[tokio::test]
    async fn attributes_of_a_queue_that_is_not_there() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["All"]}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
    }

    /// The two policy documents are EMBEDDED JSON STRINGS, answered byte for
    /// byte. An SDK that round-trips `RedrivePolicy` through
    /// `SetQueueAttributes` compares the string it sent, so a re-serialization
    /// that reordered two keys or dropped the quotes around a numeric
    /// `maxReceiveCount` would fail a comparison this facade itself invited.
    #[tokio::test]
    async fn the_two_policies_come_back_as_the_strings_they_were_sent_as() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "dead", json!({})).await.expect("the target");
        // Deliberately NOT the spelling this crate would generate: the members
        // are in the other order and the count is a string, which is what
        // Terraform writes.
        let redrive = r#"{"maxReceiveCount":"5","deadLetterTargetArn":"arn:aws:sqs:queen-1:000000000000:dead"}"#;
        let allow = r#"{"redrivePermission":"byQueue","sourceQueueArns":["arn:aws:sqs:queen-1:000000000000:orders"]}"#;
        create(
            &ctx,
            "orders",
            json!({"RedrivePolicy": redrive, "RedriveAllowPolicy": allow}),
        )
        .await
        .expect("created");

        let answer = get_queue_attributes(
            &ctx,
            &json!({
                "QueueUrl": url("orders"),
                "AttributeNames": ["RedrivePolicy", "RedriveAllowPolicy"],
            }),
        )
        .await
        .expect("read");
        let attributes = attributes_of(&answer);
        assert_eq!(
            attributes.get("RedrivePolicy").map(String::as_str),
            Some(redrive)
        );
        assert_eq!(
            attributes.get("RedriveAllowPolicy").map(String::as_str),
            Some(allow)
        );
        // ...and `All` answers both, beside everything else.
        let all = attributes_of(
            &get_queue_attributes(
                &ctx,
                &json!({"QueueUrl": url("orders"), "AttributeNames": ["All"]}),
            )
            .await
            .expect("read"),
        );
        assert_eq!(all.get("RedrivePolicy").map(String::as_str), Some(redrive));
        assert_eq!(
            all.get("RedriveAllowPolicy").map(String::as_str),
            Some(allow)
        );
        // The string the client sent is the string the record holds: nothing
        // parses it on the way in and re-prints it on the way out.
        assert_eq!(
            ctx.facade
                .registry
                .require("orders", None)
                .await
                .expect("the record")
                .attributes
                .get("RedrivePolicy")
                .map(String::as_str),
            Some(redrive)
        );
        // The broker's own retry budget is read from that same document,
        // whichever spelling it is in.
        assert_eq!(configured(&api, "orders")[0]["retryLimit"], json!(5));
    }

    /// `RedriveAllowPolicy` is ACCEPTED AND NOT ENFORCED — the plan's first
    /// non-goal — so nothing validates it and nothing reads it. A facade that
    /// refused a document Terraform writes unconditionally would break the
    /// client it is built for; one that pretended to enforce it would be worse.
    #[tokio::test]
    async fn the_allow_policy_is_stored_whatever_it_says() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        for (name, allow) in [
            ("a", r#"{"redrivePermission":"denyAll"}"#),
            ("b", r#"{"redrivePermission":"nonsense"}"#),
            ("c", "not json at all"),
        ] {
            create(&ctx, name, json!({"RedriveAllowPolicy": allow}))
                .await
                .unwrap_or_else(|e| panic!("{name}: {e}"));
            let attributes = attributes_of(
                &get_queue_attributes(
                    &ctx,
                    &json!({"QueueUrl": url(name), "AttributeNames": ["RedriveAllowPolicy"]}),
                )
                .await
                .expect("read"),
            );
            assert_eq!(
                attributes.get("RedriveAllowPolicy").map(String::as_str),
                Some(allow)
            );
        }
    }

    // ---------------------------------------------------- SetQueueAttributes

    /// A change MERGES — AWS has no way to remove an attribute — and the bag the
    /// broker is then given is the WHOLE one, rebuilt from the merged record.
    #[tokio::test]
    async fn set_merges_and_rebuilds_the_whole_options_bag() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({"VisibilityTimeout": "45"}))
            .await
            .expect("created");

        let answer = set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Attributes": {"DelaySeconds": "10"}}),
        )
        .await
        .expect("set");
        assert_eq!(answer, Value::Null);

        let bag = configured(&api, "orders").pop().expect("configured");
        assert_eq!(bag["delayedProcessing"], json!(10), "the change");
        assert_eq!(bag["leaseTime"], json!(45), "and what was already there");
        assert_eq!(bag["retentionSeconds"], json!(345_600), "and the defaults");
    }

    /// The queue's own `DelaySeconds` is the BROKER's `delayedProcessing`, and a
    /// change to it applies to what is sent next: the facade schedules no timer
    /// of its own for it, because a queue-level delay is a property of the queue
    /// and a per-message one is a property of the message
    /// ([`crate::actions::messages`]'s timers). A facade that answered the queue
    /// default with a timer would delay it twice.
    #[tokio::test]
    async fn a_queue_level_delay_is_the_brokers_and_never_a_facade_timer() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        assert_eq!(configured(&api, "orders")[0]["delayedProcessing"], json!(0));

        set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Attributes": {"DelaySeconds": "60"}}),
        )
        .await
        .expect("set");
        assert_eq!(
            configured(&api, "orders").last().expect("configured")["delayedProcessing"],
            json!(60),
            "the change never reached the queue"
        );

        crate::actions::messages::send_message(
            &ctx,
            &json!({"QueueUrl": url("orders"), "MessageBody": "x"}),
        )
        .await
        .expect("sent");
        assert_eq!(api.pushed().len(), 1);
        assert!(
            api.timer_calls.lock().unwrap().is_empty(),
            "the queue's delay was scheduled as a message timer as well"
        );

        // …and the attribute reads back as what was set, which is what a client
        // computing its own visibility window reads.
        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["DelaySeconds"]}),
        )
        .await
        .expect("read");
        assert_eq!(attributes_of(&answer)["DelaySeconds"], "60");
    }

    /// The create-only attributes are `InvalidAttributeName` on a set, which is
    /// what AWS answers for an attribute that exists and cannot be changed.
    #[tokio::test]
    async fn the_create_only_attributes_cannot_be_set() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders.fifo", json!({"FifoQueue": "true"}))
            .await
            .expect("created");
        let before = configured(&api, "orders.fifo").len();
        for attributes in [
            json!({"FifoQueue": "false"}),
            json!({"queen.partitions": "128"}),
            json!({"Nonsense": "1"}),
        ] {
            let e = set_queue_attributes(
                &ctx,
                &json!({"QueueUrl": url("orders.fifo"), "Attributes": attributes}),
            )
            .await
            .expect_err("refused");
            assert_eq!(e.kind, ErrorKind::InvalidAttributeName, "{attributes}");
        }
        assert_eq!(configured(&api, "orders.fifo").len(), before);
    }

    /// A value outside its range is refused BEFORE the queue is reconfigured: a
    /// stored one becomes a bad `/configure` on every later call.
    #[tokio::test]
    async fn an_out_of_range_value_never_reaches_the_broker() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        let e = set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Attributes": {"VisibilityTimeout": "99999"}}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::InvalidParameterValue);
        assert_eq!(configured(&api, "orders").len(), 1);
    }

    #[tokio::test]
    async fn set_needs_a_queue_and_at_least_one_attribute() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = set_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Attributes": {"DelaySeconds": "1"}}),
        )
        .await
        .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);

        create(&ctx, "orders", json!({})).await.expect("created");
        let e = set_queue_attributes(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::MissingParameter);
    }

    // ------------------------------------------------------------ PurgeQueue

    async fn purge(ctx: &Ctx, name: &str) -> SqsResult<Value> {
        purge_queue(ctx, &json!({ "QueueUrl": url(name) })).await
    }

    /// The action, whole: the log goes, the queue comes back configured from its
    /// own record, and the client's view of the queue does not move.
    #[tokio::test]
    async fn purge_empties_the_queue_and_leaves_it_standing() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create_queue(
            &ctx,
            &json!({"QueueName": "orders", "Attributes": {"VisibilityTimeout": "45"},
                    "Tags": {"team": "billing"}}),
        )
        .await
        .expect("created");
        api.seed("orders", "0", 0, &[json!({"b": "1"}), json!({"b": "2"})]);
        let before = ctx
            .facade
            .registry
            .queue_fresh("orders", None)
            .await
            .expect("read")
            .expect("record");

        assert_eq!(purge(&ctx, "orders").await.expect("purged"), Value::Null);

        assert!(api.lane("orders", "0").is_empty(), "the log survived");
        assert_eq!(api.deletes.lock().unwrap().as_slice(), ["orders"]);
        let options = configured(&api, "orders");
        assert_eq!(options.len(), 2, "the queue was not put back");
        assert_eq!(options[0], options[1], "it came back a different queue");
        assert_eq!(options[1]["leaseTime"], json!(45));

        // The record is the same record: attributes, tags, creation and the ARN
        // are what a purge must not touch.
        let after = ctx
            .facade
            .registry
            .queue_fresh("orders", None)
            .await
            .expect("read")
            .expect("record");
        assert_eq!(after.attributes, before.attributes);
        assert_eq!(after.tags, before.tags);
        assert_eq!(after.created_ms, before.created_ms);
        assert_eq!(
            after.modified_ms, before.modified_ms,
            "a purge is not a set"
        );
        assert_eq!(after.arn, before.arn);
        assert_ne!(
            after.version, before.version,
            "the record was not rewritten"
        );
    }

    /// A queue with something on several lanes is empty on all of them, and it
    /// takes new messages immediately: the recreate is a real queue and not a
    /// tombstone.
    #[tokio::test]
    async fn a_purged_queue_is_empty_on_every_lane_and_takes_sends_again() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        for lane in ["0", "7", "63"] {
            api.seed("orders", lane, 0, &[json!({"b": lane})]);
        }

        purge(&ctx, "orders").await.expect("purged");
        for lane in ["0", "7", "63"] {
            assert!(api.lane("orders", lane).is_empty(), "lane {lane}");
        }

        crate::actions::messages::send_message(
            &ctx,
            &json!({"QueueUrl": url("orders"), "MessageBody": "after"}),
        )
        .await
        .expect("the purged queue takes sends");
        assert_eq!(api.pushed().len(), 1);
    }

    /// A purge takes the DELAYED messages too, which is the half the queue's own
    /// delete does not perform: `queen.log_timers` is keyed by the queue's NAME
    /// with no foreign key (025_log_timers.sql), so a surviving timer would
    /// deliver, minutes later, a message the client was told was gone.
    ///
    /// A NATIVE producer's timer on the same queue is not an SQS delayed message
    /// and is left alone — the same rule that keeps it out of
    /// `ApproximateNumberOfMessagesDelayed`.
    #[tokio::test]
    async fn a_purge_cancels_the_delayed_messages_and_leaves_a_native_producers_alone() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");

        crate::actions::messages::send_message(
            &ctx,
            &json!({"QueueUrl": url("orders"), "MessageBody": "later", "DelaySeconds": 60}),
        )
        .await
        .expect("scheduled");
        api.timers_schedule(
            &[TimerSchedule::new(
                "orders",
                "native-key",
                "0",
                60_000,
                "txn-native",
                "",
            )],
            None,
        )
        .await
        .expect("the other producer's timer");
        assert_eq!(
            api.timers_count("orders", "sqs.", None)
                .await
                .expect("count"),
            1
        );

        purge(&ctx, "orders").await.expect("purged");
        assert_eq!(
            api.timers_count("orders", "sqs.", None)
                .await
                .expect("count"),
            0,
            "a delayed message survived the purge"
        );
        assert_eq!(
            api.timers_count("orders", "native-key", None)
                .await
                .expect("count"),
            1,
            "the purge cancelled another producer's timer"
        );

        // …and the delayed message never arrives, which is the whole point. The
        // native one does, into the recreated queue, exactly as it would in
        // Queen — a timer outlives the queue it names.
        api.advance(Duration::from_secs(61));
        api.timers_count("orders", "native-key", None)
            .await
            .expect("the fire runs whenever anyone looks");
        assert_eq!(
            api.lane("orders", "0").len(),
            1,
            "the purged delayed message came back too"
        );

        let delayed = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": [ATTR_MESSAGES_DELAYED]}),
        )
        .await
        .expect("read");
        assert_eq!(attributes_of(&delayed)[ATTR_MESSAGES_DELAYED], "0");
    }

    /// The sweep happens BEFORE the queue goes: a timer that fires in between
    /// lands in a log the delete then removes, while one cancelled after the
    /// recreate would already have landed in the new queue.
    #[tokio::test]
    async fn the_delayed_messages_are_cancelled_before_the_queue_is_deleted() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        crate::actions::messages::send_message(
            &ctx,
            &json!({"QueueUrl": url("orders"), "MessageBody": "later", "DelaySeconds": 60}),
        )
        .await
        .expect("scheduled");

        purge(&ctx, "orders").await.expect("purged");
        // One list, one cancel, and the delete after both of them.
        assert_eq!(api.deletes.lock().unwrap().len(), 1);
        assert_eq!(
            api.timers_list("orders", None, 10, None)
                .await
                .expect("listed")
                .rows
                .len(),
            0
        );
    }

    /// AWS's own asynchrony, as far as a client can observe it: one purge per
    /// queue per minute, and the second is the 403 an SDK branches on.
    #[tokio::test]
    async fn a_second_purge_inside_the_window_is_refused_and_deletes_nothing() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        create(&ctx, "clicks", json!({})).await.expect("created");
        purge(&ctx, "orders").await.expect("purged");

        let e = purge(&ctx, "orders").await.expect_err("inside the window");
        assert_eq!(e.kind, ErrorKind::PurgeQueueInProgress);
        assert_eq!(e.kind.http_status(), 403);
        assert_eq!(
            api.deletes.lock().unwrap().as_slice(),
            ["orders"],
            "the refused purge deleted the queue anyway"
        );

        // The window is one queue's, not the account's.
        purge(&ctx, "clicks").await.expect("another queue");

        // …and it is the TTL: nothing here computes an age.
        api.advance(crate::registry::PURGE_COOLDOWN + Duration::from_secs(1));
        purge(&ctx, "orders").await.expect("the window reopened");
        assert_eq!(api.deletes.lock().unwrap().len(), 3);
    }

    /// A purge is not a delete: the name does not enter the `QueueDeletedRecently`
    /// cooldown, so the provisioning run that purges and then re-`CreateQueue`s
    /// — which is every test suite's fixture — still works.
    #[tokio::test]
    async fn a_purge_does_not_put_the_name_in_the_delete_cooldown() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        purge(&ctx, "orders").await.expect("purged");

        assert!(!ctx
            .facade
            .registry
            .deleted_recently("orders", None)
            .await
            .expect("read"));
        create(&ctx, "orders", json!({}))
            .await
            .expect("the same create an SDK repeats at start-up");
    }

    #[tokio::test]
    async fn purging_a_queue_that_is_not_there_touches_nothing() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = purge(&ctx, "orders").await.expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
        assert!(api.deletes.lock().unwrap().is_empty());
        assert!(
            !ctx.facade
                .registry
                .purging("orders", None)
                .await
                .expect("read"),
            "a queue that does not exist was put in a purge window"
        );

        // …and a URL this endpoint did not mint is the same answer.
        let e = purge_queue(&ctx, &json!({"QueueUrl": "http://h/999999999999/orders"}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
    }

    /// A `DeleteQueue` that lands INSIDE a purge, both its halves between both of
    /// this one's. The record is gone, so the CAS writes nothing — and the queue
    /// just recreated belongs to no record, which is exactly what the CreateQueue
    /// guard refuses to adopt for ever. It goes back.
    #[tokio::test]
    async fn a_queue_deleted_inside_a_purge_is_not_left_behind_as_an_orphan() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        // The read and the window claim pass; the third KV call — the record's
        // own CAS — finds the row gone.
        api.kv_interpose.lock().unwrap().extend([
            None,
            None,
            Some(crate::queen::KvOp::delete(
                crate::registry::NS,
                "qs:q:orders",
                None,
            )),
        ]);

        let e = purge(&ctx, "orders").await.expect_err("deleted underneath");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);
        assert_eq!(
            api.deletes.lock().unwrap().as_slice(),
            ["orders", "orders"],
            "the recreated queue was left with no record owning it"
        );
        assert!(api
            .list_queues(None)
            .await
            .expect("listed")
            .iter()
            .all(|q| q.name != "orders"));
    }

    /// A `SetQueueAttributes` that lands inside a purge wins the record, so the
    /// broker must end up running ITS options and not the ones this purge read —
    /// the same disagreement `reconcile` exists to close.
    #[tokio::test]
    async fn a_set_attributes_inside_a_purge_is_what_the_broker_ends_up_running() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({"VisibilityTimeout": "30"}))
            .await
            .expect("created");
        let competitor = json!({
            "attributes": {"VisibilityTimeout": "90", "queen.partitions": "64"},
            "tags": {},
            "createdTs": 1_787_011_200_000i64,
            "modifiedTs": 1_787_011_200_001i64,
            "arn": "arn:aws:sqs:queen-1:000000000000:orders",
        });
        api.kv_interpose.lock().unwrap().extend([
            None,
            None,
            Some(crate::queen::KvOp::put(
                crate::registry::NS,
                "qs:q:orders",
                competitor,
            )),
        ]);

        purge(&ctx, "orders").await.expect("purged");
        let options = configured(&api, "orders");
        assert_eq!(
            options.last().expect("configured")["leaseTime"],
            json!(90),
            "the broker kept the record that lost: {options:?}"
        );
        // What the client reads and what the broker runs agree again.
        let answer = get_queue_attributes(
            &ctx,
            &json!({"QueueUrl": url("orders"), "AttributeNames": ["VisibilityTimeout"]}),
        )
        .await
        .expect("read");
        assert_eq!(attributes_of(&answer)["VisibilityTimeout"], "90");
    }

    /// THE PROPERTY A PURGE RESTS ON. The lanes come back as new
    /// `log_partitions` rows with new ids (001_log_schema.sql: `gen_random_uuid()`
    /// under `ON DELETE CASCADE`), and a receipt handle carries the id an ack is
    /// keyed by — so a handle minted before the purge resolves to no consumer at
    /// all afterwards and CANNOT delete a message that arrived since, which is the
    /// one outcome here that would be data loss.
    ///
    /// The delete itself still answers success, and that is AWS's own contract
    /// for a handle that no longer names anything, not laxity: *"you must provide
    /// the most recently received ReceiptHandle … otherwise, the request
    /// succeeds, but the message might not be deleted"*. The visibility verbs,
    /// which have a code for it, say `MessageNotInflight`.
    #[tokio::test]
    async fn a_receipt_handle_from_before_a_purge_addresses_nothing_afterwards() {
        use crate::actions::messages::{
            change_message_visibility, delete_message, receive_message,
        };

        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        api.seed("orders", "0", 0, &[json!({"b": "before"})]);
        let received = receive_message(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect("received");
        let handle = received["Messages"][0]["ReceiptHandle"]
            .as_str()
            .expect("a handle")
            .to_string();
        let before_id = api.partition_id("orders", "0");

        purge(&ctx, "orders").await.expect("purged");
        api.seed("orders", "0", 0, &[json!({"b": "after"})]);
        assert_ne!(
            api.partition_id("orders", "0"),
            before_id,
            "the lane came back with the id the old handle names"
        );

        // The handle is still THIS facade's — the refusal is not about its shape.
        let receipt = ctx
            .facade
            .handles
            .decode(&handle, crate::obs::now_epoch_ms())
            .expect("minted here");
        assert_eq!(receipt.partition_id, before_id);

        delete_message(
            &ctx,
            &json!({"QueueUrl": url("orders"), "ReceiptHandle": handle}),
        )
        .await
        .expect("AWS answers success for a handle that names nothing");
        let e = change_message_visibility(
            &ctx,
            &json!({"QueueUrl": url("orders"), "ReceiptHandle": handle,
                    "VisibilityTimeout": 60}),
        )
        .await
        .expect_err("nothing is in flight");
        assert_eq!(e.kind, ErrorKind::MessageNotInflight);

        // …and the message that arrived AFTER the purge is untouched by all of
        // it, which is the property that matters.
        let after = receive_message(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect("received");
        assert_eq!(after["Messages"][0]["Body"], json!("after"));
    }

    // ------------------------------------------------------------------ tags

    #[tokio::test]
    async fn tags_round_trip_and_untag_is_idempotent() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        create(&ctx, "orders", json!({})).await.expect("created");
        let address = json!({"QueueUrl": url("orders")});

        assert_eq!(
            list_queue_tags(&ctx, &address).await.expect("listed"),
            json!({}),
            "an untagged queue answers no Tags member"
        );

        tag_queue(
            &ctx,
            &json!({"QueueUrl": url("orders"), "Tags": {"team": "billing", "env": "prod"}}),
        )
        .await
        .expect("tagged");
        assert_eq!(
            list_queue_tags(&ctx, &address).await.expect("listed"),
            json!({"Tags": {"env": "prod", "team": "billing"}})
        );

        for _ in 0..2 {
            untag_queue(
                &ctx,
                &json!({"QueueUrl": url("orders"), "TagKeys": ["env", "never-set"]}),
            )
            .await
            .expect("untagged, twice");
        }
        assert_eq!(
            list_queue_tags(&ctx, &address).await.expect("listed"),
            json!({"Tags": {"team": "billing"}})
        );
        // Tags never reach Queen: nothing was reconfigured by any of that.
        assert_eq!(configured(&api, "orders").len(), 1);
    }

    #[tokio::test]
    async fn tagging_needs_a_queue_and_something_to_do() {
        let api = FakeQueen::empty();
        let ctx = ctx(&api);
        let e = list_queue_tags(&ctx, &json!({"QueueUrl": url("orders")}))
            .await
            .expect_err("refused");
        assert_eq!(e.kind, ErrorKind::QueueDoesNotExist);

        create(&ctx, "orders", json!({})).await.expect("created");
        for missing in [
            tag_queue(&ctx, &json!({"QueueUrl": url("orders")})).await,
            untag_queue(&ctx, &json!({"QueueUrl": url("orders")})).await,
        ] {
            assert_eq!(
                missing.expect_err("refused").kind,
                ErrorKind::MissingParameter
            );
        }
    }

    // -------------------------------------------------------- shapes & readers

    /// The suffix is the whole declaration, exactly as it is in AWS: there is no
    /// separate attribute that can disagree with it.
    #[test]
    fn the_fifo_suffix_is_the_declaration() {
        assert!(is_fifo("orders.fifo"));
        assert!(!is_fifo("orders"));
        assert!(!is_fifo("orders.fifo.backup"));
    }

    /// Both protocols reach an action with the same tree, and a form carries only
    /// strings — so every reader takes either spelling of a scalar.
    #[test]
    fn a_scalar_is_read_from_either_protocols_spelling() {
        let query = json!({"MaxResults": "10", "Attributes": {"DelaySeconds": "5"}});
        let json_client = json!({"MaxResults": 10, "Attributes": {"DelaySeconds": 5}});
        assert_eq!(param_int(&query, "MaxResults").expect("read"), Some(10));
        assert_eq!(
            param_int(&json_client, "MaxResults").expect("read"),
            Some(10)
        );
        assert_eq!(
            param_map(&query, "Attributes").expect("read"),
            param_map(&json_client, "Attributes").expect("read")
        );
        assert_eq!(param_int(&query, "Absent").expect("read"), None);
        assert!(param_int(&json!({"MaxResults": "x"}), "MaxResults").is_err());
        assert!(param_map(&json!({"Attributes": ["a"]}), "Attributes").is_err());
    }

    /// A list parameter reaches here as a list even when the client sent one
    /// unindexed value.
    #[test]
    fn a_bare_scalar_under_a_list_parameter_is_a_list_of_one() {
        assert_eq!(
            param_list(&json!({"AttributeNames": "All"}), "AttributeNames").expect("read"),
            vec!["All".to_string()]
        );
        assert_eq!(
            param_list(
                &json!({"AttributeNames": ["All", "QueueArn"]}),
                "AttributeNames"
            )
            .expect("read"),
            vec!["All".to_string(), "QueueArn".to_string()]
        );
        assert!(param_list(&json!({}), "AttributeNames")
            .expect("read")
            .is_empty());
    }

    /// The read catalog and the computed set are one table read two ways; a name
    /// in one and not the other would be an attribute nothing can ask for.
    #[test]
    fn every_computed_attribute_is_readable() {
        for name in COMPUTED {
            assert!(READABLE.contains(name), "{name}");
        }
        for (name, _) in DEFAULTED {
            assert!(READABLE.contains(name), "{name}");
        }
    }
}
