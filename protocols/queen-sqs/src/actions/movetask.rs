//! Message move tasks: the redrive, in reverse.
//!
//! CONTRACT. `StartMessageMoveTask` is [`super::dlq`]'s move run backwards and
//! in bulk — pop from the dead-letter queue, push to a destination, ack the
//! original, all inside one `POST /api/v1/transaction`, exactly as the forward
//! move does and for the same reason: a facade that died between the push and
//! the ack would duplicate or lose.
//!
//! What is new here is that the loop OUTLIVES the request that started it. AWS
//! answers `StartMessageMoveTask` immediately with a `TaskHandle` and moves the
//! messages afterwards, so this facade spawns a task and answers. That makes
//! three things load-bearing:
//!
//!   * **The state is in the store, not in the task.** `qs:mv:` holds the
//!     status, the counts and a heartbeat, so `ListMessageMoveTasks` and
//!     `CancelMessageMoveTask` are answerable by an instance that never ran the
//!     mover — the sentence PLAN_QUEEN_SQS.md opens with, applied to the one
//!     piece of state in this facade that is not per-request.
//!   * **Cancellation is a flag the mover reads, not a handle it holds.**
//!     `Cancel` compare-and-sets the status to `CANCELLING`; the mover sees it
//!     between two batches and writes `CANCELLED`. A cancel therefore works
//!     across instances, and it can never interrupt a transaction mid-flight.
//!   * **One task per source is a fence in the store** (`qs:mvf:`), claimed with
//!     `putIfAbsent`. AWS allows one active task per source queue; two movers on
//!     one queue would not corrupt anything — the broker's claims keep them off
//!     each other's messages — but they would double the rate the operator
//!     asked for and each would under-report, which is worse than a refusal.
//!
//! ## The honest part: a facade restart abandons the mover
//!
//! The tokio task dies with the process and the store still says `RUNNING`.
//! Nothing here pretends otherwise. What it does instead is make the stale
//! record HARMLESS: every progress write stamps a heartbeat, and a
//! `StartMessageMoveTask` on a source whose task has not written one for
//! [`STALE_AFTER_MS`] STEALS the fence with a compare-and-set on the version it
//! read, marks the abandoned task `FAILED` with a reason that says so, and
//! starts its own. A live task's heartbeat is at most one batch old, so a live
//! task is never stolen from; and the steal is a CAS, so two instances racing to
//! supersede one abandoned task produce one winner and one `UnsupportedOperation`.
//!
//! ## Where a message goes
//!
//! `DestinationArn` is optional, and its absence is AWS's own default: each
//! message goes back to the queue it came from. That queue is read from the
//! envelope's [`super::dlq::SYS_SOURCE_QUEUE`], which the forward move stamped —
//! it has to be in the payload, because the instance moving a message back is
//! not the instance that moved it out. A message with no recorded source and no
//! `DestinationArn` cannot be placed, and the task FAILS naming it rather than
//! guessing a queue or acking the message into nothing.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{Map, Value};

use crate::actions::messages::lane_for;
use crate::actions::{dlq, fifo, queues, Ctx, Principal};
use crate::envelope::Envelope;
use crate::error::{ErrorKind, SqsError, SqsResult};
use crate::obs::{now_epoch_ms, Sampler};
use crate::queen::{self, PopOptions, PushItem, TxnAck};
use crate::registry::{QueueRecord, Registry, NS};

/// How long a task may go without writing a heartbeat before a new
/// `StartMessageMoveTask` on the same source may take its fence. A live mover
/// writes one after every batch, so the window only has to cover one batch's
/// round trips; thirty seconds is the plan's number and is two orders of
/// magnitude above that.
pub const STALE_AFTER_MS: i64 = 30_000;

/// Messages one batch of the mover claims. The SQS receive ceiling, deliberately
/// — the mover is a consumer of the dead-letter queue like any other, and a
/// wider claim would hold more messages invisible for longer with nothing gained
/// but a slightly better transaction amortization.
pub const MOVE_BATCH: i32 = 10;

/// The visibility the mover claims with. Long enough that one transaction
/// cannot lose its own lease, short enough that a mover killed mid-batch gives
/// the messages back inside a minute.
pub const MOVE_VISIBILITY_SECONDS: i64 = 60;

/// AWS's range for `MaxNumberOfMessagesPerSecond`.
pub const MIN_RATE: i64 = 1;
pub const MAX_RATE: i64 = 500;

/// How long a finished task stays readable by `ListMessageMoveTasks`. AWS keeps
/// a history; this is the window in which "what happened to my redrive" has an
/// answer, and after it the record is a row nobody reads.
pub const TASK_TTL_SECONDS: u64 = 7 * 24 * 3_600;

/// How long the one-task-per-source fence lives when nothing removes it. It is
/// deleted the moment a task ends, so this only bounds the damage of a process
/// that died holding it — and the heartbeat steal above is what actually
/// releases those, far sooner.
const FENCE_TTL_SECONDS: u64 = 24 * 3_600;

/// AWS's cap on `ListMessageMoveTasks`, and its default: one, which is the most
/// recent task.
pub const MAX_TASK_RESULTS: i64 = 10;
pub const DEFAULT_TASK_RESULTS: i64 = 1;

/// Attempts at a compare-and-set on a task record. The registry's number, for
/// the registry's reason.
const CAS_ATTEMPTS: usize = 2;

/// A write to a task record that could not land twice running — a batch's
/// progress ([`Mover::record`]) or the mover's final status ([`Mover::stop_at`]).
/// Neither loses messages: the transaction committed before either, so what is
/// lost is the COUNT or the STATUS a listing reports, and the line says which.
static PROGRESS_LOST: Sampler = Sampler::new(10_000);

// --------------------------------------------------------------- the record

/// The five states AWS reports, and the only ones this facade writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    /// A cancel has been recorded and the mover has not yet seen it. It is a
    /// state and not a flag beside `RUNNING` because AWS reports it as one, and
    /// because a client that polls after cancelling must be able to tell "asked
    /// to stop" from "stopped".
    Cancelling,
    Cancelled,
    Completed,
    Failed,
}

impl TaskStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskStatus::Running => "RUNNING",
            TaskStatus::Cancelling => "CANCELLING",
            TaskStatus::Cancelled => "CANCELLED",
            TaskStatus::Completed => "COMPLETED",
            TaskStatus::Failed => "FAILED",
        }
    }

    fn of(text: &str) -> Option<TaskStatus> {
        [
            TaskStatus::Running,
            TaskStatus::Cancelling,
            TaskStatus::Cancelled,
            TaskStatus::Completed,
            TaskStatus::Failed,
        ]
        .into_iter()
        .find(|s| s.as_str() == text)
    }

    /// Whether the task is over. A finished task holds no fence and accepts no
    /// cancel.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskStatus::Cancelled | TaskStatus::Completed | TaskStatus::Failed
        )
    }
}

/// One task, as `qs:mv:<source>:<countdown>:<id>` holds it.
#[derive(Debug, Clone, PartialEq)]
pub struct Task {
    pub source: String,
    /// `None` is AWS's default: every message goes back to the queue it came
    /// from (module header).
    pub destination: Option<String>,
    pub status: TaskStatus,
    pub moved: i64,
    /// `ApproximateNumberOfMessagesToMove`, from the queue's depth at the moment
    /// the task started. It is a snapshot and it says so in its name: the answer
    /// to "how far along is this" and never to "how many are left".
    pub total: i64,
    pub rate: Option<i64>,
    pub started_ms: i64,
    /// The last time a mover wrote to this record. See [`STALE_AFTER_MS`].
    pub heartbeat_ms: i64,
    pub failure: Option<String>,
}

impl Task {
    fn to_value(&self) -> Value {
        serde_json::json!({
            "source": self.source,
            "destination": self.destination,
            "status": self.status.as_str(),
            "moved": self.moved,
            "total": self.total,
            "rate": self.rate,
            "started": self.started_ms,
            "hb": self.heartbeat_ms,
            "failureReason": self.failure,
        })
    }

    /// Read a stored row back. A row whose status cannot be read is `FAILED`
    /// rather than dropped: the record's EXISTENCE is what holds a fence, and a
    /// task that vanished from a listing while its fence stood would be a source
    /// nobody could start a task on and nobody could see why.
    fn from_value(value: &Value) -> Task {
        let text = |name: &str| {
            value
                .get(name)
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
        };
        let number = |name: &str| value.get(name).and_then(Value::as_i64);
        Task {
            source: text("source").unwrap_or_default(),
            destination: text("destination"),
            status: text("status")
                .and_then(|s| TaskStatus::of(&s))
                .unwrap_or(TaskStatus::Failed),
            moved: number("moved").unwrap_or(0),
            total: number("total").unwrap_or(0),
            rate: number("rate"),
            started_ms: number("started").unwrap_or(0),
            heartbeat_ms: number("hb").unwrap_or_else(|| number("started").unwrap_or(0)),
            failure: text("failureReason"),
        }
    }

    /// Whether nothing has written to this task recently enough for it to be
    /// believed to be running (module header).
    fn stale_at(&self, now_ms: i64) -> bool {
        !matches!(self.status, TaskStatus::Running | TaskStatus::Cancelling)
            || now_ms.saturating_sub(self.heartbeat_ms) > STALE_AFTER_MS
    }

    /// One row of `ListMessageMoveTasks`.
    fn view(&self, ctx: &Ctx, handle: &str) -> Value {
        let mut out = Map::new();
        // AWS populates `TaskHandle` ONLY for a task in RUNNING status, because
        // it is the argument to a cancel and a finished task cannot be
        // cancelled. Answering it for a completed task would hand a client a
        // handle whose only use is an error.
        if self.status == TaskStatus::Running {
            out.insert("TaskHandle".to_string(), Value::String(handle.to_string()));
        }
        out.insert(
            "Status".to_string(),
            Value::String(self.status.as_str().to_string()),
        );
        let naming = queues::naming(&ctx.facade.config);
        out.insert(
            "SourceArn".to_string(),
            Value::String(naming.arn(&self.source)),
        );
        if let Some(destination) = &self.destination {
            out.insert(
                "DestinationArn".to_string(),
                Value::String(naming.arn(destination)),
            );
        }
        if let Some(rate) = self.rate {
            out.insert(
                "MaxNumberOfMessagesPerSecond".to_string(),
                Value::Number(rate.into()),
            );
        }
        out.insert(
            "ApproximateNumberOfMessagesMoved".to_string(),
            Value::Number(self.moved.into()),
        );
        out.insert(
            "ApproximateNumberOfMessagesToMove".to_string(),
            Value::Number(self.total.into()),
        );
        if let Some(failure) = &self.failure {
            out.insert("FailureReason".to_string(), Value::String(failure.clone()));
        }
        // Epoch SECONDS, which is the unit `CreatedTimestamp` and
        // `LastModifiedTimestamp` report on this listener. It is a DECISION and
        // not a reading: this API is from 2023 and the modern AWS convention is
        // milliseconds, so which one the real service sends is a question only
        // the differential run answers (PLAN_QUEEN_SQS.md, M5). One unit across
        // one endpoint is the better guess to be wrong in, because it is the one
        // an operator comparing two of this facade's answers can reason about.
        out.insert(
            "StartedTimestamp".to_string(),
            Value::Number((self.started_ms / 1_000).into()),
        );
        Value::Object(out)
    }
}

// -------------------------------------------------------------- the handle

/// The opaque `TaskHandle` a client holds: the store key, base64url.
///
/// It carries the key and nothing else, so `Cancel` needs no index and no
/// second read to find the task — and it is URL-safe and unpadded because it
/// travels in a Query-protocol form body where `+` is a space.
fn handle_of(key: &str) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key)
}

/// The inverse, refusing anything that is not one of our keys. A handle from
/// another deployment, a truncated one and a guessed one all answer
/// `ResourceNotFoundException`, which is what AWS answers for a task it does not
/// have — the client cannot tell "malformed" from "gone", and neither can it on
/// AWS.
fn key_of(handle: &str) -> SqsResult<String> {
    use base64::Engine;
    let key = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(handle)
        .ok()
        .and_then(|bytes| String::from_utf8(bytes).ok())
        .filter(|key| key.starts_with("qs:mv:"));
    key.ok_or_else(|| {
        SqsError::with(
            ErrorKind::ResourceNotFoundException,
            "The task handle provided does not name a message move task of this endpoint.",
        )
    })
}

// -------------------------------------------------------------- the actions

/// `StartMessageMoveTask`. Answers `{"TaskHandle": …}` and leaves a mover
/// running.
pub async fn start_message_move_task(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let (key, task) = begin(ctx, params).await?;
    Mover::new(ctx, key.clone(), task).spawn();
    Ok(serde_json::json!({ "TaskHandle": handle_of(&key) }))
}

/// Everything the action does BEFORE the mover exists: validate, take the
/// fence, and record the task.
///
/// Split from the spawn so that the state machine is drivable without a
/// scheduler — the tests step the mover themselves, which is the only way an
/// assertion about "after one batch" is a fact rather than a race.
async fn begin(ctx: &Ctx, params: &Value) -> SqsResult<(String, Task)> {
    let source = queue_of_arn(ctx, params, "SourceArn")?;
    let source_record = require_queue(ctx, &source).await?;
    let destination = match queues::param_text(params, "DestinationArn") {
        None => None,
        Some(_) => {
            let name = queue_of_arn(ctx, params, "DestinationArn")?;
            let record = require_queue(ctx, &name).await?;
            if name == source {
                return Err(SqsError::with(
                    ErrorKind::InvalidParameterValue,
                    "Value for parameter DestinationArn is invalid. Reason: the destination must \
                     not be the source queue itself.",
                ));
            }
            // The same rule the RedrivePolicy is held to, for the same reason:
            // a FIFO message has a group and a standard queue has nowhere to
            // put one, and a standard message reaching a FIFO queue would need
            // a group this facade would have to invent.
            if record.fifo != source_record.fifo {
                return Err(SqsError::with(
                    ErrorKind::InvalidParameterValue,
                    "Value for parameter DestinationArn is invalid. Reason: the destination must \
                     be the same queue type as the source.",
                ));
            }
            Some(name)
        }
    };
    let rate = match queues::param_int(params, "MaxNumberOfMessagesPerSecond")? {
        None => None,
        Some(n) if (MIN_RATE..=MAX_RATE).contains(&n) => Some(n),
        Some(n) => {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "Value {n} for parameter MaxNumberOfMessagesPerSecond is invalid. Reason: \
                     must be {MIN_RATE} to {MAX_RATE}."
                ),
            ))
        }
    };
    // AWS's precondition: a move task's source must be a queue that is
    // configured as a dead-letter queue. It is not pedantry — the task's whole
    // default destination is "the queue each message came from", which only a
    // dead-letter queue's messages carry.
    if !dlq::is_dead_letter_target(ctx, &source).await? {
        return Err(SqsError::with(
            ErrorKind::UnsupportedOperation,
            format!(
                "Source queue {source} is not configured as a dead-letter queue of any queue on \
                 this endpoint, and a message move task can only read from one."
            ),
        ));
    }

    let started_ms = now_epoch_ms();
    let id = uuid::Uuid::new_v4().to_string();
    let key = Registry::key_move_task(&source, started_ms, &id);
    claim_fence(ctx, &source, &key).await?;

    let depth = ctx
        .facade
        .queen
        .queue_depth(&source, Some(queen::QUEUE_MODE_GROUP), ctx.token())
        .await
        .map_err(|e| SqsError::from_queen(&e))?;
    let task = Task {
        source: source.clone(),
        destination: destination.clone(),
        status: TaskStatus::Running,
        moved: 0,
        total: depth.ready,
        rate,
        started_ms,
        heartbeat_ms: started_ms,
        failure: None,
    };
    let written = ctx
        .facade
        .queen
        .kv(
            &[queen::KvOp::put_ttl(
                NS,
                &key,
                task.to_value(),
                TASK_TTL_SECONDS,
                None,
            )],
            ctx.token(),
        )
        .await
        .map_err(|e| SqsError::from_queen(&e))?;
    if !written.first().is_some_and(queen::KvAnswer::applied) {
        // The fence is given back: holding it for a task that does not exist
        // would make the source unstartable until the next steal window.
        release_fence(ctx, &source, &key).await;
        return Err(SqsError::with(
            ErrorKind::ServiceUnavailable,
            "The message move task could not be recorded. Nothing was moved; retry.",
        ));
    }
    Ok((key, task))
}

/// `CancelMessageMoveTask`. Answers `{"ApproximateNumberOfMessagesMoved": n}`.
///
/// It records the intent and returns; the mover is what stops. That is not a
/// weaker contract than AWS's — a cancel there is also asynchronous — and it is
/// what makes a cancel work against a task running on another instance.
pub async fn cancel_message_move_task(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let key = key_of(queues::require_text(params, "TaskHandle")?)?;
    for _ in 0..CAS_ATTEMPTS {
        let (task, version) = load(ctx, &key).await?.ok_or_else(unknown_task)?;
        if task.status.is_terminal() {
            return Err(SqsError::with(
                ErrorKind::UnsupportedOperation,
                format!(
                    "The message move task is {} and can no longer be cancelled.",
                    task.status.as_str()
                ),
            ));
        }
        if task.status == TaskStatus::Cancelling {
            // Already asked for, by this client or another. Idempotent rather
            // than an error: a client that retried a cancel it never saw the
            // answer to must not be told its task is in a state it cannot
            // cancel, when the state is the one its own call produced.
            return Ok(moved_answer(task.moved));
        }
        let cancelling = Task {
            status: TaskStatus::Cancelling,
            ..task
        };
        if write(ctx, &key, &cancelling, version).await? {
            return Ok(moved_answer(cancelling.moved));
        }
    }
    // Two losses in a row means the mover is writing progress as fast as this
    // is trying to write the cancel. Retriable, and the client's SDK already
    // backs off on it.
    Err(SqsError::with(
        ErrorKind::ServiceUnavailable,
        "The message move task was being updated concurrently and the cancellation was not \
         recorded. Retry.",
    ))
}

/// `ListMessageMoveTasks`: the most recent tasks of one source, newest first.
///
/// The order is the KEY's ([`Registry::key_move_task`]) and never a sort of the
/// page, because a page that the store truncated would then be sorted into a
/// plausible answer built from the wrong rows.
pub async fn list_message_move_tasks(ctx: &Ctx, params: &Value) -> SqsResult<Value> {
    let source = queue_of_arn(ctx, params, "SourceArn")?;
    require_queue(ctx, &source).await?;
    let limit = match queues::param_int(params, "MaxResults")? {
        None => DEFAULT_TASK_RESULTS,
        Some(n) if (1..=MAX_TASK_RESULTS).contains(&n) => n,
        Some(n) => {
            return Err(SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "Value {n} for parameter MaxResults is invalid. Reason: must be 1 to \
                     {MAX_TASK_RESULTS}."
                ),
            ))
        }
    };
    let answers = ctx
        .facade
        .queen
        .kv(
            &[queen::KvOp::get_prefix(
                NS,
                &Registry::key_move_tasks(&source),
                limit,
                None,
            )],
            ctx.token(),
        )
        .await
        .map_err(|e| SqsError::from_queen(&e))?;
    let rows = answers.first().map(|a| a.rows.clone()).unwrap_or_default();
    let results: Vec<Value> = rows
        .iter()
        .map(|row| Task::from_value(&row.value).view(ctx, &handle_of(&row.key)))
        .collect();
    Ok(serde_json::json!({ "Results": results }))
}

fn moved_answer(moved: i64) -> Value {
    serde_json::json!({ "ApproximateNumberOfMessagesMoved": moved })
}

fn unknown_task() -> SqsError {
    SqsError::with(
        ErrorKind::ResourceNotFoundException,
        "The message move task named by this handle does not exist. It may have finished long \
         enough ago for its record to have been retired.",
    )
}

/// The queue one ARN parameter names, refusing what this deployment did not
/// mint.
fn queue_of_arn(ctx: &Ctx, params: &Value, name: &str) -> SqsResult<String> {
    let arn = queues::require_text(params, name)?;
    queues::naming(&ctx.facade.config)
        .name_of_arn(arn)
        .ok_or_else(|| {
            SqsError::with(
                ErrorKind::InvalidParameterValue,
                format!(
                    "Value for parameter {name} is invalid. Reason: it is not an Amazon SQS queue \
                     ARN of this region and account."
                ),
            )
        })
}

/// The record of a queue these three actions name.
///
/// `ResourceNotFoundException` and NOT `QueueDoesNotExist`: the move-task trio
/// is a 2023 API and its models carry the modern code. An SDK's
/// `StartMessageMoveTask` catch block names this one, and the 2012 spelling
/// would reach it as an unmodelled error for a queue URL nobody sent.
async fn require_queue(ctx: &Ctx, name: &str) -> SqsResult<QueueRecord> {
    ctx.facade
        .registry
        .queue(name, ctx.token())
        .await
        .map_err(|e| SqsError::from_queen(&e))?
        .ok_or_else(|| {
            SqsError::with(
                ErrorKind::ResourceNotFoundException,
                format!("The queue {name} does not exist."),
            )
        })
}

// --------------------------------------------------------------- the fence

/// Claim the one-active-task-per-source fence, stealing it from a task nothing
/// has heartbeaten for [`STALE_AFTER_MS`].
///
/// `putIfAbsent` first, and the steal is a compare-and-set on the version the
/// read answered — so two instances that both find one abandoned task produce
/// one winner and one refusal, rather than two movers on one queue.
async fn claim_fence(ctx: &Ctx, source: &str, task_key: &str) -> SqsResult<()> {
    let key = Registry::key_move_fence(source);
    let now = now_epoch_ms();
    let value = serde_json::json!({"task": task_key, "source": source, "at": now});
    let claimed = one(
        ctx,
        queen::KvOp::put_if_absent_ttl(NS, &key, value.clone(), FENCE_TTL_SECONDS),
    )
    .await?;
    if claimed.applied() {
        return Ok(());
    }

    // Somebody holds it. Whether that somebody is alive is a question the TASK
    // record answers — the fence carries no heartbeat of its own, deliberately,
    // so there is one clock and not two that can disagree.
    let held = claimed
        .value
        .get("task")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let previous = match held.is_empty() {
        true => None,
        false => load(ctx, &held).await?,
    };
    let stale = match &previous {
        // A fence naming a task whose record is gone. It is stale ONLY once the
        // fence itself is older than a heartbeat window, and that stamp is the
        // whole reason the fence carries one: the fence is claimed BEFORE the
        // record is written, so between those two calls a concurrent start
        // reads exactly this state — and without the age check it would call a
        // task that is milliseconds old abandoned, steal the fence its rival
        // just took, and run a second mover on one queue. Which is the outcome
        // the fence exists to prevent, produced by the fence's own recovery
        // path. A fence with no stamp at all was written by a version that did
        // not have one and is treated as stale, since the alternative is a
        // source no start can ever recover.
        None => claimed
            .value
            .get("at")
            .and_then(Value::as_i64)
            .is_none_or(|at| now.saturating_sub(at) > STALE_AFTER_MS),
        Some((task, _)) => task.stale_at(now),
    };
    if !stale {
        return Err(SqsError::with(
            ErrorKind::UnsupportedOperation,
            format!(
                "There is already a message move task running for source queue {source}. Only one \
                 task per source queue may be active at a time; cancel it before starting another."
            ),
        ));
    }
    let stolen = one(
        ctx,
        queen::KvOp::put_expecting(NS, &key, value, claimed.version),
    )
    .await?;
    if !stolen.applied() {
        return Err(SqsError::with(
            ErrorKind::UnsupportedOperation,
            format!(
                "Another message move task for source queue {source} was started at the same \
                 moment as this one. Only one task per source queue may be active at a time."
            ),
        ));
    }
    // The superseded task is marked, so that a listing explains itself: a task
    // stuck at RUNNING with a heartbeat from an hour ago tells an operator
    // nothing, and this tells them the process that owned it is gone.
    if let Some((task, version)) = previous {
        if !task.status.is_terminal() {
            let failed = Task {
                status: TaskStatus::Failed,
                heartbeat_ms: now,
                failure: Some(
                    "The facade instance running this task stopped writing progress and the task \
                     was superseded by a new one."
                        .to_string(),
                ),
                ..task
            };
            let _ = write(ctx, &held, &failed, version).await;
        }
    }
    Ok(())
}

/// Give the fence back, IF IT IS STILL THIS TASK'S.
///
/// The check is not hygiene. A mover whose fence was stolen — its heartbeat went
/// stale under a long batch, or two progress writes lost their compare-and-set
/// — is a mover that keeps running beside the task that superseded it, and an
/// unconditional delete would then remove the NEW owner's fence: a third
/// `StartMessageMoveTask` would be admitted next to a live mover, which is the
/// double-mover outcome the fence exists to prevent, reached from the recovery
/// path itself. So the delete is conditional on the value naming this task and
/// is a compare-and-set on the version that named it.
///
/// BEST EFFORT beyond that: a fence nobody released is stolen by the next start
/// once its task's heartbeat goes stale, so failing to remove one costs a delay
/// and never a deadlock.
async fn release_fence(ctx: &Ctx, source: &str, task_key: &str) {
    let key = Registry::key_move_fence(source);
    let Ok(held) = one(ctx, queen::KvOp::get(NS, &key)).await else {
        return;
    };
    if !held.found || held.value.get("task").and_then(Value::as_str) != Some(task_key) {
        return;
    }
    let _ = ctx
        .facade
        .queen
        .kv(
            &[queen::KvOp::delete(NS, &key, Some(held.version))],
            ctx.token(),
        )
        .await;
}

// ---------------------------------------------------------------- the store

/// One task record and the version to compare-and-set against.
async fn load(ctx: &Ctx, key: &str) -> SqsResult<Option<(Task, i64)>> {
    let answer = one(ctx, queen::KvOp::get(NS, key)).await?;
    Ok(match answer.found {
        true => Some((Task::from_value(&answer.value), answer.version)),
        false => None,
    })
}

/// Write a task record under a CAS. `false` is a lost race and never an error.
async fn write(ctx: &Ctx, key: &str, task: &Task, version: i64) -> SqsResult<bool> {
    let op = queen::KvOp::put_ttl(NS, key, task.to_value(), TASK_TTL_SECONDS, Some(version));
    Ok(one(ctx, op).await?.applied())
}

async fn one(ctx: &Ctx, op: queen::KvOp) -> SqsResult<queen::KvAnswer> {
    let answers = ctx
        .facade
        .queen
        .kv(&[op], ctx.token())
        .await
        .map_err(|e| SqsError::from_queen(&e))?;
    answers.into_iter().next().ok_or_else(|| {
        SqsError::with(
            ErrorKind::InternalFailure,
            "The key/value store answered nothing for a single operation.",
        )
    })
}

// ---------------------------------------------------------------- the mover

/// What one batch did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Step {
    /// Messages moved. The caller paces on this number.
    Moved(usize),
    /// The task is over — completed, cancelled, failed, or superseded. Its
    /// record already says which.
    Stop,
}

/// The loop `StartMessageMoveTask` leaves running.
///
/// It owns a [`Ctx`] of its own rather than borrowing the request's: the request
/// is answered before the first batch is claimed, and a mover that borrowed it
/// would be a task outliving its own inputs. The principal is carried over so
/// that every call the mover makes to Queen goes out under the credential that
/// started the task — a mover running as the process default would be a
/// privilege the client never had.
pub struct Mover {
    ctx: Ctx,
    key: String,
    task: Task,
}

impl Mover {
    pub fn new(ctx: &Ctx, key: String, task: Task) -> Mover {
        Mover {
            ctx: Ctx {
                facade: Arc::clone(&ctx.facade),
                principal: Principal {
                    access_key_id: ctx.principal.access_key_id.clone(),
                    queen_token: ctx.principal.queen_token.clone(),
                },
                host: ctx.host.clone(),
                // Its own id, so every line the mover logs is attributable to
                // the task rather than to the request that started it — which
                // has long been answered.
                request_id: uuid::Uuid::new_v4().to_string(),
            },
            key,
            task,
        }
    }

    /// Run to completion. Spawned by `StartMessageMoveTask`; called directly by
    /// the tests, which is what keeps the state machine testable without a
    /// scheduler in the assertions.
    pub async fn run(&mut self) {
        loop {
            match self.step().await {
                Step::Stop => return,
                Step::Moved(moved) => self.pace(moved).await,
            }
        }
    }

    pub fn spawn(mut self) {
        tokio::spawn(async move { self.run().await });
    }

    /// `MaxNumberOfMessagesPerSecond`, applied between batches.
    ///
    /// Sleeping AFTER a batch rather than metering inside one is deliberate: the
    /// transaction is atomic and cannot be throttled halfway, so the only place
    /// a rate can be applied without splitting a bundle is between two of them.
    /// The cap is therefore an average over a batch and not an instantaneous
    /// ceiling — with a batch of ten and a cap of two per second, the ten move
    /// at once and the next batch waits five seconds.
    async fn pace(&self, moved: usize) {
        let Some(rate) = self.task.rate.filter(|r| *r > 0) else {
            return;
        };
        if moved == 0 {
            return;
        }
        tokio::time::sleep(Duration::from_secs_f64(moved as f64 / rate as f64)).await;
    }

    /// One batch: read the state, claim, move, record.
    pub async fn step(&mut self) -> Step {
        let Ok(Some((task, version))) = load(&self.ctx, &self.key).await else {
            // The record is gone or unreadable. Nothing can be recorded about
            // this task any more, so the honest thing is to stop moving
            // messages under its name.
            return Step::Stop;
        };
        self.task = task.clone();
        match task.status {
            TaskStatus::Cancelling => {
                return self.stop_at(version, TaskStatus::Cancelled, None).await;
            }
            TaskStatus::Running => {}
            // Superseded, or finished by another instance. Not ours any more.
            _ => return Step::Stop,
        }

        // The pop is the BROKER's and not `ReceiveMessage`'s, so it does not go
        // through the redrive check ([`super::dlq::sift`]) — deliberately. A
        // move task is an administrative drain and not a consumer: if the
        // dead-letter queue carries a `RedrivePolicy` of its own, running the
        // check here would send these messages to a third queue instead of
        // home, which is the opposite of what the operator asked for.
        let popped = match self
            .ctx
            .facade
            .queen
            .pop_queue(
                &task.source,
                &PopOptions {
                    batch: MOVE_BATCH,
                    partitions: 1,
                    lease_seconds: MOVE_VISIBILITY_SECONDS as i32,
                    ..PopOptions::default()
                },
                self.ctx.token(),
            )
            .await
        {
            Ok(popped) => popped,
            Err(e) => {
                return self
                    .fail(version, &format!("the queue could not be read: {e}"))
                    .await
            }
        };
        if popped.messages.is_empty() {
            return self.stop_at(version, TaskStatus::Completed, None).await;
        }

        // A FIFO dead-letter queue has consumers of its own, and one that
        // deleted out of order left marks the lease it held could not ack
        // ([`super::fifo`]). This pop claims the same lane and would carry those
        // messages home as duplicates of work that was already done, so the
        // marks are read and the messages they name are SKIPPED — and still
        // acked past, which is what finally completes them. A standard queue
        // writes no such marks: its claims are one message wide and a delete
        // acks directly.
        let deleted = match crate::registry::is_fifo(&task.source) {
            false => BTreeSet::new(),
            true => {
                let lane = &popped.messages[0].partition_id;
                fifo::deleted_here(&self.ctx, lane).await
            }
        };

        // The LEADING RUN that can be placed. An ack completes everything below
        // the position it names ([`super::dlq`]), so the first message with
        // nowhere to go ends the batch: acking past it would delete it.
        let mut pushes: Vec<PushItem> = Vec::with_capacity(popped.messages.len());
        let mut blocked: Option<String> = None;
        let mut destinations: BTreeMap<String, Option<QueueRecord>> = BTreeMap::new();
        // Messages of the run this batch will ack over: the ones it pushes AND
        // the ones it skipped because they were deleted already.
        let mut placed = 0usize;
        for message in &popped.messages {
            if deleted.contains(&message.id) {
                placed += 1;
                continue;
            }
            let envelope = Envelope::decode(&message.data);
            let Some(name) = self.destination_of(&envelope) else {
                blocked = Some(format!(
                    "message {} records no source queue and the task named no DestinationArn, so \
                     there is nowhere to move it",
                    message.id
                ));
                break;
            };
            if !destinations.contains_key(&name) {
                let record = self
                    .ctx
                    .facade
                    .registry
                    .queue(&name, self.ctx.token())
                    .await
                    .ok()
                    .flatten();
                destinations.insert(name.clone(), record);
            }
            let Some(Some(record)) = destinations.get(&name) else {
                blocked = Some(format!(
                    "the destination queue {name} of message {} does not exist",
                    message.id
                ));
                break;
            };
            pushes.push(restore(record, message, envelope));
            placed += 1;
        }

        if placed == 0 {
            let reason = blocked.unwrap_or_else(|| "nothing in this batch could be moved".into());
            return self.fail(version, &reason).await;
        }
        // The ack can travel alone: a run that was entirely deleted by a
        // consumer has nothing to push and still has to be completed, and an
        // acks-only bundle is a route the broker takes deliberately
        // ([`crate::queen::QueenApi::transaction`]).
        let last = &popped.messages[placed - 1];
        let acks = [TxnAck::completed(
            &last.transaction_id,
            &last.partition_id,
            &popped.lease_id,
        )];
        if let Err(e) = self
            .ctx
            .facade
            .queen
            .transaction(&pushes, &acks, &[], self.ctx.token())
            .await
        {
            // Nothing was written and nothing was acked: the whole bundle rolled
            // back, the messages are still on the dead-letter queue, and the
            // task says why. It is not retried here — a move task is
            // restartable by construction, since a new one simply reads what is
            // left — and a task that retried silently would hide a broker that
            // is refusing writes.
            return self
                .fail(version, &format!("a batch could not be committed: {e}"))
                .await;
        }

        let moved = pushes.len();
        self.record(moved as i64, version, blocked).await
    }

    /// The destination one message goes to: the task's, or the queue the
    /// forward move recorded in the envelope (module header).
    fn destination_of(&self, envelope: &Envelope) -> Option<String> {
        if let Some(destination) = &self.task.destination {
            return Some(destination.clone());
        }
        dlq::source_of(envelope)
            .filter(|home| *home != self.task.source)
            .map(str::to_string)
    }

    /// Write the batch's progress, merging onto whatever else wrote in the
    /// meantime — which is how a cancel that landed during the batch survives
    /// this write instead of being overwritten by it.
    async fn record(&mut self, moved: i64, version: i64, blocked: Option<String>) -> Step {
        let mut version = version;
        let mut task = self.task.clone();
        for attempt in 0..CAS_ATTEMPTS {
            if attempt > 0 {
                match load(&self.ctx, &self.key).await {
                    Ok(Some((fresh, fresh_version))) => {
                        // A record somebody else has FINISHED is not ours to
                        // write: the steal that superseded this mover marks the
                        // task FAILED, and merging progress onto that would bump
                        // a finished task's counters and then release a fence
                        // that now belongs to whoever superseded us. The batch
                        // that just committed goes unreported, which is what the
                        // count's own name allows.
                        if fresh.status.is_terminal() {
                            self.task = fresh;
                            return Step::Stop;
                        }
                        task = fresh;
                        version = fresh_version;
                    }
                    // The record went away under us; the messages are moved and
                    // there is nothing left to record them in.
                    _ => return Step::Stop,
                }
            }
            let mut next = Task {
                moved: task.moved + moved,
                heartbeat_ms: now_epoch_ms(),
                ..task.clone()
            };
            if let Some(reason) = &blocked {
                next.status = TaskStatus::Failed;
                next.failure = Some(reason.clone());
            }
            if matches!(write(&self.ctx, &self.key, &next, version).await, Ok(true)) {
                self.task = next.clone();
                if next.status.is_terminal() {
                    self.finish(&next).await;
                    return Step::Stop;
                }
                // A cancel that landed during the batch is honoured on the next
                // step, which is where CANCELLING becomes CANCELLED — one
                // transition, in one place.
                return Step::Moved(moved as usize);
            }
        }
        if let Some(suppressed) = PROGRESS_LOST.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                source = %self.task.source,
                moved,
                "a message move task's progress could not be recorded; the messages were moved \
                 and the count under-reports them"
            );
        }
        Step::Moved(moved as usize)
    }

    /// Mark the task failed and stop.
    async fn fail(&mut self, version: i64, reason: &str) -> Step {
        self.stop_at(version, TaskStatus::Failed, Some(reason.to_string()))
            .await
    }

    /// The one way this mover stops: write a TERMINAL status, then release the
    /// fence.
    ///
    /// The write RETRIES on a lost compare-and-set, exactly as [`Mover::record`]
    /// does, and for a sharper reason. The version was read at the top of the
    /// step and a whole batch happened since; a `CancelMessageMoveTask` landing
    /// inside that window takes the version with it, and a single-shot write
    /// would lose — leaving a record frozen at CANCELLING for its seven-day TTL
    /// while the mover stops and gives back its fence. A client polling
    /// `ListMessageMoveTasks` for CANCELLED would never see it, and a second
    /// cancel would answer the idempotent "already cancelling" for ever.
    ///
    /// A record another instance already finished is LEFT ALONE: its status is
    /// the true one, and rewriting it would bump a finished task's counters and
    /// then release a fence that is no longer this task's.
    async fn stop_at(&mut self, version: i64, status: TaskStatus, failure: Option<String>) -> Step {
        let mut task = self.task.clone();
        let mut version = version;
        for attempt in 0..CAS_ATTEMPTS {
            if attempt > 0 {
                match load(&self.ctx, &self.key).await {
                    Ok(Some((fresh, fresh_version))) => {
                        if fresh.status.is_terminal() {
                            self.task = fresh;
                            return Step::Stop;
                        }
                        task = fresh;
                        version = fresh_version;
                    }
                    // The record is gone; there is nothing left to write to and
                    // nothing this mover may claim about it.
                    _ => return Step::Stop,
                }
            }
            // A cancel that landed during the batch is honoured over a plain
            // completion: the client asked the task to stop and it stopped. A
            // FAILURE outranks both — it is the only status carrying a reason,
            // and losing it would leave an operator without the sentence that
            // says what went wrong.
            let status = match (status, task.status) {
                (TaskStatus::Completed, TaskStatus::Cancelling) => TaskStatus::Cancelled,
                _ => status,
            };
            let next = Task {
                status,
                heartbeat_ms: now_epoch_ms(),
                failure: failure.clone().or_else(|| task.failure.clone()),
                ..task.clone()
            };
            if matches!(write(&self.ctx, &self.key, &next, version).await, Ok(true)) {
                self.task = next.clone();
                self.finish(&next).await;
                return Step::Stop;
            }
        }
        // Two lost races. The mover stops either way — it may not keep moving
        // messages under a record it cannot write — and the fence stays until
        // the heartbeat window opens it, which is the safe direction: it is the
        // one thing that keeps a second mover off this queue.
        if let Some(suppressed) = PROGRESS_LOST.tick_now() {
            tracing::warn!(
                target: "sqs",
                suppressed,
                source = %self.task.source,
                status = status.as_str(),
                "a message move task stopped and its final status could not be recorded"
            );
        }
        Step::Stop
    }

    /// Release the source's fence, so the next task may start at once rather
    /// than waiting out a heartbeat window.
    async fn finish(&self, task: &Task) {
        release_fence(&self.ctx, &task.source, &self.key).await;
    }

    #[cfg(test)]
    pub fn task(&self) -> &Task {
        &self.task
    }
}

/// The push one dead-lettered message becomes on its way back.
///
/// The move markers are stripped ([`super::dlq::restored`]) so the message
/// arrives with a fresh delivery budget — a restored message that still carried
/// its receive count would be dead-lettered again on its first delivery, which
/// would make every redrive a no-op — and the lane is chosen the way the forward
/// move chooses one: the group for a FIFO destination, the message's own id
/// hashed across the width for a standard one.
fn restore(destination: &QueueRecord, message: &queen::Message, envelope: Envelope) -> PushItem {
    let partition = match destination.fifo {
        true => message.partition.clone(),
        false => lane_for(&message.id, destination.partitions),
    };
    PushItem::new(
        &destination.name,
        &partition,
        dlq::restored(envelope).encode(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::actions::testing::{arn, field, redrive_policy, Rig};
    use crate::queen::testing::FakeQueen;
    use serde_json::json;
    use std::sync::Arc;

    /// A source queue, its dead-letter queue, and messages already sitting in
    /// the dead-letter queue because they were REALLY dead-lettered — the
    /// forward move is what writes the `queen.sourceQueue` a task reads, so a
    /// fixture that hand-wrote one would be testing a marker nothing produces.
    async fn dead_lettered(messages: usize) -> Rig {
        let policy = redrive_policy("orders-dlq", 1);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        for lane in 0..messages {
            rig.fake.seed(
                "orders",
                &lane.to_string(),
                0,
                &[crate::actions::testing::carrying(&format!("m{lane}"), 9)],
            );
        }
        // Every one of them is over the threshold, so this receive moves them
        // all and answers nothing.
        for _ in 0..=messages {
            rig.receive_list("orders", json!({"MaxNumberOfMessages": 10}))
                .await;
        }
        rig.fake.transactions.lock().unwrap().clear();
        rig
    }

    fn start_params(source: &str, extra: Value) -> Value {
        let mut params = json!({"SourceArn": arn(source)});
        if let Some(fields) = extra.as_object() {
            for (name, value) in fields {
                params[name] = value.clone();
            }
        }
        params
    }

    async fn started(rig: &Rig, extra: Value) -> Mover {
        let (key, task) = begin(&rig.ctx, &start_params("orders-dlq", extra))
            .await
            .expect("the task starts");
        Mover::new(&rig.ctx, key, task)
    }

    /// Every task of a source, newest first, as the listing answers them.
    async fn listed(rig: &Rig, source: &str, max: i64) -> Vec<Value> {
        let answer = list_message_move_tasks(
            &rig.ctx,
            &json!({"SourceArn": arn(source), "MaxResults": max}),
        )
        .await
        .expect("listed");
        answer["Results"].as_array().cloned().unwrap_or_default()
    }

    /// What a queue holds right now, as a client sees it.
    async fn drain(rig: &Rig, queue: &str) -> Vec<Value> {
        rig.receive_list(
            queue,
            json!({"MaxNumberOfMessages": 10, "AttributeNames": ["All"]}),
        )
        .await
    }

    fn fence(fake: &Arc<FakeQueen>, source: &str) -> Option<Value> {
        fake.kv_get(NS, &Registry::key_move_fence(source))
    }

    // -------------------------------------------------------- the preconditions

    /// AWS allows a move task only from a queue that IS a dead-letter queue —
    /// which is also the only kind of queue whose messages record where they
    /// came from.
    #[tokio::test]
    async fn a_task_may_only_be_started_on_a_dead_letter_queue() {
        let policy = redrive_policy("orders-dlq", 3);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        let error = begin(&rig.ctx, &start_params("orders", json!({})))
            .await
            .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::UnsupportedOperation);
        assert_eq!(error.kind.http_status(), 400);
        assert!(
            error.message.contains("dead-letter queue"),
            "{}",
            error.message
        );
        // ...and the dead-letter queue itself is allowed.
        begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the task starts");
    }

    /// The three actions answer `ResourceNotFoundException` and not
    /// `QueueDoesNotExist`: they are a 2023 API and an SDK's catch block names
    /// the modern code.
    #[tokio::test]
    async fn a_queue_these_actions_cannot_find_is_a_resource_not_found() {
        let rig = Rig::new(&[("orders-dlq", &[])]).await;
        let error = begin(&rig.ctx, &start_params("nowhere", json!({})))
            .await
            .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::ResourceNotFoundException);
        assert_eq!(error.kind.http_status(), 404);

        let error = list_message_move_tasks(&rig.ctx, &json!({"SourceArn": arn("nowhere")}))
            .await
            .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::ResourceNotFoundException);
    }

    #[tokio::test]
    async fn an_arn_this_deployment_did_not_mint_is_refused() {
        let rig = Rig::new(&[("orders-dlq", &[])]).await;
        for bad in [
            "arn:aws:sqs:elsewhere:000000000000:orders-dlq",
            "arn:aws:sqs:queen-1:999999999999:orders-dlq",
            "orders-dlq",
        ] {
            let error = begin(&rig.ctx, &json!({"SourceArn": bad}))
                .await
                .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{bad}");
        }
        let error = begin(&rig.ctx, &json!({})).await.expect_err("refused");
        assert_eq!(error.kind, ErrorKind::MissingParameter);
    }

    #[tokio::test]
    async fn a_rate_outside_its_range_is_refused_and_the_ends_are_not() {
        let rig = dead_lettered(1).await;
        for bad in [0, MAX_RATE + 1] {
            let error = begin(
                &rig.ctx,
                &start_params("orders-dlq", json!({"MaxNumberOfMessagesPerSecond": bad})),
            )
            .await
            .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{bad}");
            assert!(
                error.message.contains("must be 1 to 500"),
                "{}",
                error.message
            );
        }
        let (_, task) = begin(
            &rig.ctx,
            &start_params(
                "orders-dlq",
                json!({"MaxNumberOfMessagesPerSecond": MAX_RATE}),
            ),
        )
        .await
        .expect("accepted");
        assert_eq!(task.rate, Some(MAX_RATE));
    }

    #[tokio::test]
    async fn a_destination_of_the_wrong_shape_is_refused() {
        let policy = redrive_policy("orders-dlq", 1);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
            ("other.fifo", &[]),
        ])
        .await;
        // ...its own source.
        let error = begin(
            &rig.ctx,
            &start_params("orders-dlq", json!({"DestinationArn": arn("orders-dlq")})),
        )
        .await
        .expect_err("refused");
        assert!(
            error.message.contains("not be the source queue itself"),
            "{}",
            error.message
        );
        // ...a queue of the other type.
        let error = begin(
            &rig.ctx,
            &start_params("orders-dlq", json!({"DestinationArn": arn("other.fifo")})),
        )
        .await
        .expect_err("refused");
        assert!(
            error.message.contains("same queue type"),
            "{}",
            error.message
        );
        // ...and one that is not there at all.
        let error = begin(
            &rig.ctx,
            &start_params("orders-dlq", json!({"DestinationArn": arn("nowhere")})),
        )
        .await
        .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::ResourceNotFoundException);
    }

    /// The snapshot the progress is read against, taken from the queue's depth
    /// at the moment the task started.
    #[tokio::test]
    async fn the_task_records_the_depth_it_started_against() {
        let rig = dead_lettered(3).await;
        let (_, task) = begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("started");
        assert_eq!(task.total, 3);
        assert_eq!(task.moved, 0);
        assert_eq!(task.status, TaskStatus::Running);
        assert_eq!(task.destination, None);
    }

    // ------------------------------------------------------------- the fence

    /// AWS's contract: one active task per source. Two movers on one queue
    /// would each move at the rate the operator asked for and each would
    /// under-report, which is worse than a refusal.
    #[tokio::test]
    async fn only_one_task_per_source_may_be_active() {
        let rig = dead_lettered(2).await;
        begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the first starts");
        let error = begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect_err("the second is refused");
        assert_eq!(error.kind, ErrorKind::UnsupportedOperation);
        assert!(
            error
                .message
                .contains("already a message move task running"),
            "{}",
            error.message
        );
    }

    /// ...and the fence is given back the moment the task ends, so the next one
    /// starts at once rather than waiting out a heartbeat window.
    #[tokio::test]
    async fn a_finished_task_releases_its_fence() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        assert!(fence(&rig.fake, "orders-dlq").is_some());
        mover.run().await;
        assert_eq!(fence(&rig.fake, "orders-dlq"), None);
        begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the next one starts");
    }

    /// A facade that stopped mid-task leaves a RUNNING record nothing will ever
    /// finish. The next start supersedes it — with a compare-and-set, so two
    /// instances racing to do so produce one winner.
    #[tokio::test]
    async fn a_task_nothing_has_heartbeaten_is_superseded() {
        let rig = dead_lettered(1).await;
        let abandoned_started = now_epoch_ms() - 10 * STALE_AFTER_MS;
        let key = Registry::key_move_task("orders-dlq", abandoned_started, "abandoned");
        let stale = Task {
            source: "orders-dlq".to_string(),
            destination: None,
            status: TaskStatus::Running,
            moved: 4,
            total: 9,
            rate: None,
            started_ms: abandoned_started,
            heartbeat_ms: now_epoch_ms() - STALE_AFTER_MS - 1_000,
            failure: None,
        };
        rig.fake.kv_seed(NS, &key, stale.to_value());
        rig.fake.kv_seed(
            NS,
            &Registry::key_move_fence("orders-dlq"),
            json!({"task": key, "source": "orders-dlq"}),
        );

        begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the new task takes the fence");
        // The abandoned one is marked, so a listing explains itself rather than
        // showing a task stuck at RUNNING with an hour-old heartbeat.
        let superseded = Task::from_value(&rig.fake.kv_get(NS, &key).expect("still there"));
        assert_eq!(superseded.status, TaskStatus::Failed);
        assert_eq!(superseded.moved, 4, "what it did move is not rewritten");
        assert!(
            superseded
                .failure
                .unwrap_or_default()
                .contains("stopped writing progress"),
            "the reason says what happened"
        );
        // The fence now names the new task.
        let held = fence(&rig.fake, "orders-dlq").expect("held");
        assert_ne!(held["task"].as_str().unwrap_or_default(), key);
    }

    /// ...and a task whose heartbeat is fresh is NEVER stolen from.
    #[tokio::test]
    async fn a_live_task_is_not_superseded() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        // One batch, which stamps a heartbeat.
        assert!(matches!(mover.step().await, Step::Moved(1)));
        let error = begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::UnsupportedOperation);
    }

    /// THE WINDOW THE FENCE'S OWN STAMP CLOSES. The fence is claimed BEFORE the
    /// task record is written, so a start racing another one reads a fence
    /// naming a record that is not there YET — and "the record is gone" is the
    /// steal condition. Without the stamp the second start would call a task
    /// milliseconds old abandoned, take the fence its rival just claimed, and
    /// run a second mover on one queue: the fence's own recovery path producing
    /// the outcome the fence exists to prevent.
    #[tokio::test]
    async fn a_fence_claimed_a_moment_ago_is_not_stolen_before_its_record_exists() {
        let rig = dead_lettered(1).await;
        let held = Registry::key_move_task("orders-dlq", now_epoch_ms(), "in-flight");
        claim_fence(&rig.ctx, "orders-dlq", &held)
            .await
            .expect("the first start claims it");
        assert!(
            rig.fake.kv_get(NS, &held).is_none(),
            "the state between the two calls of `begin`"
        );

        let error = begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect_err("the second start is refused");
        assert_eq!(error.kind, ErrorKind::UnsupportedOperation);
        assert_eq!(
            fence(&rig.fake, "orders-dlq").expect("held")["task"],
            json!(held),
            "and the fence still names the first"
        );

        // ...and once the stamp is older than a heartbeat window it IS taken: a
        // start that died between its two calls must not fence a source for ever.
        rig.fake.kv_seed(
            NS,
            &Registry::key_move_fence("orders-dlq"),
            json!({
                "task": held, "source": "orders-dlq",
                "at": now_epoch_ms() - STALE_AFTER_MS - 1_000
            }),
        );
        begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the abandoned fence is taken");
    }

    /// A fence is released by the task that HOLDS it and by nobody else. A
    /// mover whose fence was stolen — a stale heartbeat, two lost progress
    /// writes — is still running, and an unconditional delete on its way out
    /// would remove its successor's fence and admit a third mover beside a live
    /// one.
    #[tokio::test]
    async fn a_release_only_removes_this_tasks_own_fence() {
        let rig = dead_lettered(1).await;
        let mine = Registry::key_move_task("orders-dlq", 1, "mine");
        claim_fence(&rig.ctx, "orders-dlq", &mine)
            .await
            .expect("claimed");
        let theirs = Registry::key_move_task("orders-dlq", 2, "theirs");
        rig.fake.kv_seed(
            NS,
            &Registry::key_move_fence("orders-dlq"),
            json!({"task": theirs, "source": "orders-dlq", "at": now_epoch_ms()}),
        );

        release_fence(&rig.ctx, "orders-dlq", &mine).await;
        assert_eq!(
            fence(&rig.fake, "orders-dlq").expect("still held")["task"],
            json!(theirs),
            "the superseded task released somebody else's fence"
        );
        // The holder's own release does remove it.
        release_fence(&rig.ctx, "orders-dlq", &theirs).await;
        assert_eq!(fence(&rig.fake, "orders-dlq"), None);
    }

    /// A fence naming a task whose record is gone — it outlived its own TTL, or
    /// was never written — is a fence nothing is holding.
    #[tokio::test]
    async fn a_fence_whose_task_vanished_is_taken() {
        let rig = dead_lettered(1).await;
        rig.fake.kv_seed(
            NS,
            &Registry::key_move_fence("orders-dlq"),
            json!({"task": "qs:mv:orders-dlq:ffff:ghost", "source": "orders-dlq"}),
        );
        begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the fence is taken");
    }

    // -------------------------------------------------------------- the mover

    /// The whole run: every message goes home, the task completes, and the
    /// counts say so.
    #[tokio::test]
    async fn a_task_sends_every_message_back_to_the_queue_it_came_from() {
        let rig = dead_lettered(3).await;
        let mut mover = started(&rig, json!({})).await;
        mover.run().await;

        assert_eq!(mover.task().status, TaskStatus::Completed);
        assert_eq!(mover.task().moved, 3);
        assert!(drain(&rig, "orders-dlq").await.is_empty());
        let home = drain(&rig, "orders").await;
        let mut bodies: Vec<&str> = home.iter().map(|m| field(m, "Body")).collect();
        bodies.sort_unstable();
        assert_eq!(bodies, vec!["m0", "m1", "m2"]);
    }

    /// A restored message arrives with a FRESH delivery budget. One that still
    /// carried its dead-letter receive count would be moved straight back on its
    /// first delivery, which would make every redrive a no-op — and this queue's
    /// `maxReceiveCount` is 1, so that is exactly what would happen.
    #[tokio::test]
    async fn a_restored_message_is_delivered_rather_than_dead_lettered_again() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        mover.run().await;
        let home = drain(&rig, "orders").await;
        assert_eq!(home.len(), 1, "it is delivered, not moved again");
        assert_eq!(
            crate::actions::testing::attribute(&home[0], "ApproximateReceiveCount"),
            Some("1".to_string())
        );
        assert_eq!(
            crate::actions::testing::attribute(&home[0], crate::actions::dlq::SYS_SOURCE_QUEUE),
            None,
            "the marker a move reads is not left on a restored message"
        );
    }

    /// A dead-letter queue has consumers of its own. On a FIFO one a consumer
    /// that deleted out of order leaves marks the lease it held could not ack,
    /// and the mover claims the same lane with the broker's own pop — so it
    /// reads those marks rather than carrying work that was already done back to
    /// the source as duplicates. The messages are still ACKED past: skipping
    /// them is what completes them.
    #[tokio::test]
    async fn a_mover_does_not_carry_home_what_a_dead_letter_consumer_deleted() {
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
                crate::actions::testing::carrying("first", 9),
                crate::actions::testing::carrying("second", 9),
            ],
        );
        assert!(
            drain(&rig, "orders.fifo").await.is_empty(),
            "both are over the threshold, so both are dead-lettered"
        );

        // The dead-letter consumer takes both and deletes the SECOND only: out
        // of order, so it is recorded and nothing is acked.
        let dead = drain(&rig, "orders-dlq.fifo").await;
        assert_eq!(dead.len(), 2);
        rig.delete("orders-dlq.fifo", field(&dead[1], "ReceiptHandle"))
            .await
            .expect("recorded");
        // Its lease lapses with the first still undeleted, so the run comes back.
        rig.fake.advance(Duration::from_secs(31));

        let (key, task) = begin(&rig.ctx, &start_params("orders-dlq.fifo", json!({})))
            .await
            .expect("started");
        let mut mover = Mover::new(&rig.ctx, key, task);
        mover.run().await;

        assert_eq!(mover.task().status, TaskStatus::Completed);
        assert_eq!(mover.task().moved, 1, "one message, not two");
        let home = drain(&rig, "orders.fifo").await;
        let bodies: Vec<&str> = home.iter().map(|m| field(m, "Body")).collect();
        assert_eq!(bodies, vec!["first"]);
        assert!(
            drain(&rig, "orders-dlq.fifo").await.is_empty(),
            "and the deleted one is completed rather than left behind"
        );
    }

    /// An explicit destination overrides the per-message default for every
    /// message in the task.
    #[tokio::test]
    async fn an_explicit_destination_takes_every_message() {
        let policy = redrive_policy("orders-dlq", 1);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
            ("elsewhere", &[]),
        ])
        .await;
        rig.fake.seed(
            "orders",
            "0",
            0,
            &[crate::actions::testing::carrying("work", 9)],
        );
        rig.receive_list("orders", json!({})).await;

        let (key, task) = begin(
            &rig.ctx,
            &start_params("orders-dlq", json!({"DestinationArn": arn("elsewhere")})),
        )
        .await
        .expect("started");
        let mut mover = Mover::new(&rig.ctx, key, task);
        mover.run().await;

        assert!(drain(&rig, "orders").await.is_empty());
        let moved = drain(&rig, "elsewhere").await;
        assert_eq!(moved.len(), 1);
        assert_eq!(field(&moved[0], "Body"), "work");
    }

    /// The push and the ack are one transaction here too, one per batch.
    #[tokio::test]
    async fn one_batch_is_one_transaction() {
        let rig = dead_lettered(2).await;
        let mut mover = started(&rig, json!({})).await;
        mover.run().await;
        let transactions = rig.fake.transactions.lock().unwrap().clone();
        // The messages are on two lanes, so two claims and two bundles.
        assert_eq!(transactions.len(), 2);
        for (pushes, acks, kv) in &transactions {
            assert_eq!(pushes.len(), 1);
            assert_eq!(pushes[0].queue, "orders");
            assert_eq!(acks.len(), 1, "ONE ack completes the whole claimed run");
            assert!(kv.is_empty());
        }
    }

    /// A message with nowhere to go ends the batch at the prefix that CAN be
    /// placed: an ack completes everything below the position it names, so
    /// acking past it would delete it.
    #[tokio::test]
    async fn a_message_with_no_recorded_source_fails_the_task_and_keeps_itself() {
        let rig = dead_lettered(1).await;
        // A native producer's payload, on the same lane as a real dead-letter
        // copy so that one claim holds both.
        let lane = rig
            .fake
            .lane("orders-dlq", "0")
            .is_empty()
            .then(|| "0".to_string());
        let lane = match lane {
            // The copy landed on a hashed lane; find it.
            Some(_) => (0..crate::actions::testing::LANES)
                .map(|l| l.to_string())
                .find(|l| !rig.fake.lane("orders-dlq", l).is_empty())
                .expect("the copy is on some lane"),
            None => "0".to_string(),
        };
        rig.fake
            .append("orders-dlq", &lane, "orphan", json!({"b": "no-home"}));

        let mut mover = started(&rig, json!({})).await;
        assert!(matches!(mover.step().await, Step::Stop));
        assert_eq!(mover.task().status, TaskStatus::Failed);
        assert_eq!(mover.task().moved, 1, "the prefix that could be placed WAS");
        assert!(
            mover
                .task()
                .failure
                .clone()
                .unwrap_or_default()
                .contains("records no source queue"),
            "{:?}",
            mover.task().failure
        );
        // The orphan is still on the dead-letter queue once its claim lapses.
        rig.fake.advance(std::time::Duration::from_secs(
            MOVE_VISIBILITY_SECONDS as u64 + 1,
        ));
        let left = drain(&rig, "orders-dlq").await;
        assert_eq!(left.len(), 1);
        assert_eq!(field(&left[0], "Body"), "no-home");
    }

    /// A batch that did not commit fails the task rather than retrying: a move
    /// task is restartable by construction — a new one simply reads what is left
    /// — and one that retried silently would hide a broker refusing writes.
    #[tokio::test]
    async fn a_batch_that_does_not_commit_fails_the_task() {
        let rig = dead_lettered(1).await;
        rig.fake
            .fail_transaction(crate::queen::Error::status(503, "upstream"));
        let mut mover = started(&rig, json!({})).await;
        assert!(matches!(mover.step().await, Step::Stop));
        assert_eq!(mover.task().status, TaskStatus::Failed);
        assert_eq!(mover.task().moved, 0);
        assert_eq!(
            fence(&rig.fake, "orders-dlq"),
            None,
            "the fence is released"
        );
    }

    // ------------------------------------------------------------ cancelling

    /// A cancel is a flag in the store, so it works against a mover on another
    /// instance — and it can never interrupt a transaction mid-flight.
    #[tokio::test]
    async fn a_cancel_stops_the_mover_between_two_batches() {
        let rig = dead_lettered(3).await;
        let mut mover = started(&rig, json!({})).await;
        let handle = handle_of(&mover.key);
        assert!(matches!(mover.step().await, Step::Moved(1)));

        let answer = cancel_message_move_task(&rig.ctx, &json!({"TaskHandle": handle}))
            .await
            .expect("cancelled");
        assert_eq!(answer["ApproximateNumberOfMessagesMoved"], json!(1));
        // The mover sees CANCELLING at the top of its next batch and stops
        // there, having claimed nothing more.
        assert!(matches!(mover.step().await, Step::Stop));
        assert_eq!(mover.task().status, TaskStatus::Cancelled);
        assert_eq!(mover.task().moved, 1);
        assert_eq!(fence(&rig.fake, "orders-dlq"), None);
        // The two it did not move are still on the dead-letter queue.
        assert_eq!(drain(&rig, "orders-dlq").await.len(), 2);
    }

    /// A cancel that lands INSIDE a batch takes the record's version with it, so
    /// the mover's terminal write loses its compare-and-set. It reloads and
    /// writes a terminal status all the same: a mover that stopped and left the
    /// record at CANCELLING would leave a client polling `ListMessageMoveTasks`
    /// waiting for a state that never arrives, for the record's seven-day TTL,
    /// with the source's fence given back under it.
    #[tokio::test]
    async fn a_cancel_that_lands_inside_a_batch_still_reaches_cancelled() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        let handle = handle_of(&mover.key);
        // The version the mover read at the top of its step...
        let (_, version) = load(&rig.ctx, &mover.key)
            .await
            .expect("readable")
            .expect("there");
        // ...and the cancel that lands while the batch is in flight.
        cancel_message_move_task(&rig.ctx, &json!({"TaskHandle": handle}))
            .await
            .expect("cancelled");

        assert_eq!(
            mover.stop_at(version, TaskStatus::Completed, None).await,
            Step::Stop
        );
        let stored = Task::from_value(&rig.fake.kv_get(NS, &mover.key).expect("still there"));
        assert_eq!(
            stored.status,
            TaskStatus::Cancelled,
            "the cancel is honoured rather than lost with the version"
        );
        assert_eq!(mover.task().status, TaskStatus::Cancelled);
        assert_eq!(
            fence(&rig.fake, "orders-dlq"),
            None,
            "and the fence is back"
        );
    }

    /// A task another instance has already finished is not this mover's to
    /// rewrite: bumping a finished task's counters and then releasing a fence
    /// that is no longer its own is the double-mover path.
    #[tokio::test]
    async fn a_mover_that_was_superseded_leaves_the_finished_record_alone() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        let (_, version) = load(&rig.ctx, &mover.key)
            .await
            .expect("readable")
            .expect("there");

        // The steal: this mover's heartbeat went stale under a long batch and a
        // new start superseded it, marking the record FAILED and taking the
        // fence.
        let mut stale = mover.task().clone();
        stale.heartbeat_ms = now_epoch_ms() - STALE_AFTER_MS - 1_000;
        rig.fake.kv_seed(NS, &mover.key, stale.to_value());
        let (successor, _) = begin(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("the new task supersedes it");

        assert_eq!(
            mover.stop_at(version, TaskStatus::Completed, None).await,
            Step::Stop
        );
        let stored = Task::from_value(&rig.fake.kv_get(NS, &mover.key).expect("still there"));
        assert_eq!(stored.status, TaskStatus::Failed, "the steal's verdict");
        assert_eq!(
            fence(&rig.fake, "orders-dlq").expect("still held")["task"],
            json!(successor),
            "the successor's fence is untouched"
        );
        assert!(
            begin(&rig.ctx, &start_params("orders-dlq", json!({})))
                .await
                .is_err(),
            "and no third mover is admitted"
        );
    }

    /// The cancel a client retried because it never saw the answer must not be
    /// told its task is in a state it cannot cancel — when that state is the one
    /// its own call produced.
    #[tokio::test]
    async fn a_second_cancel_of_the_same_task_is_idempotent() {
        let rig = dead_lettered(2).await;
        let mover = started(&rig, json!({})).await;
        let handle = handle_of(&mover.key);
        for _ in 0..2 {
            cancel_message_move_task(&rig.ctx, &json!({"TaskHandle": handle.clone()}))
                .await
                .expect("cancelled");
        }
    }

    #[tokio::test]
    async fn a_finished_task_cannot_be_cancelled() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        let handle = handle_of(&mover.key);
        mover.run().await;
        let error = cancel_message_move_task(&rig.ctx, &json!({"TaskHandle": handle}))
            .await
            .expect_err("refused");
        assert_eq!(error.kind, ErrorKind::UnsupportedOperation);
        assert!(error.message.contains("COMPLETED"), "{}", error.message);
    }

    #[tokio::test]
    async fn a_handle_naming_no_task_is_a_resource_not_found() {
        let rig = dead_lettered(1).await;
        for handle in [
            // Ours, well-formed, and naming a task that is not there.
            handle_of("qs:mv:orders-dlq:ffff:ghost"),
            "not base64 at all!!".to_string(),
            // Ours, and not a task key: a handle may not address the registry.
            handle_of("qs:q:orders"),
        ] {
            let error = cancel_message_move_task(&rig.ctx, &json!({"TaskHandle": handle}))
                .await
                .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::ResourceNotFoundException);
        }
        // An ABSENT handle is a different failure, and AWS reports it as one:
        // the client left out a required parameter rather than naming a task
        // that has gone.
        for params in [json!({}), json!({"TaskHandle": ""})] {
            let error = cancel_message_move_task(&rig.ctx, &params)
                .await
                .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::MissingParameter, "{params}");
        }
    }

    // -------------------------------------------------------------- listing

    /// Newest first, and the order is the KEY's rather than a sort of a page
    /// the store may have truncated.
    #[tokio::test]
    async fn the_tasks_of_a_source_are_listed_newest_first() {
        let rig = dead_lettered(1).await;
        let now = now_epoch_ms();
        for (age, moved) in [(3_000, 1), (2_000, 2), (1_000, 3)] {
            let key = Registry::key_move_task("orders-dlq", now - age, &format!("t{moved}"));
            let task = Task {
                source: "orders-dlq".to_string(),
                destination: None,
                status: TaskStatus::Completed,
                moved,
                total: moved,
                rate: None,
                started_ms: now - age,
                heartbeat_ms: now - age,
                failure: None,
            };
            rig.fake.kv_seed(NS, &key, task.to_value());
        }
        let results = listed(&rig, "orders-dlq", MAX_TASK_RESULTS).await;
        let moved: Vec<i64> = results
            .iter()
            .map(|r| r["ApproximateNumberOfMessagesMoved"].as_i64().unwrap_or(-1))
            .collect();
        assert_eq!(moved, vec![3, 2, 1]);
        // ...and the default is the most recent one alone.
        let answer = list_message_move_tasks(&rig.ctx, &json!({"SourceArn": arn("orders-dlq")}))
            .await
            .expect("listed");
        assert_eq!(answer["Results"].as_array().map(Vec::len), Some(1));
        assert_eq!(
            answer["Results"][0]["ApproximateNumberOfMessagesMoved"],
            json!(3)
        );
    }

    /// AWS populates `TaskHandle` only for a RUNNING task: it is the argument to
    /// a cancel, and a finished task cannot be cancelled.
    #[tokio::test]
    async fn only_a_running_task_is_listed_with_a_handle() {
        let rig = dead_lettered(1).await;
        let mut mover = started(&rig, json!({})).await;
        let running = listed(&rig, "orders-dlq", 1).await;
        assert_eq!(running[0]["Status"], json!("RUNNING"));
        assert_eq!(
            running[0]["TaskHandle"].as_str(),
            Some(handle_of(&mover.key).as_str())
        );
        assert_eq!(running[0]["SourceArn"], json!(arn("orders-dlq")));
        assert!(running[0]["StartedTimestamp"].as_i64().unwrap_or(0) > 1_600_000_000);

        mover.run().await;
        let done = listed(&rig, "orders-dlq", 1).await;
        assert_eq!(done[0]["Status"], json!("COMPLETED"));
        assert_eq!(done[0].get("TaskHandle"), None);
    }

    #[tokio::test]
    async fn a_listing_refuses_a_max_results_outside_its_range() {
        let rig = dead_lettered(1).await;
        for bad in [0, MAX_TASK_RESULTS + 1] {
            let error = list_message_move_tasks(
                &rig.ctx,
                &json!({"SourceArn": arn("orders-dlq"), "MaxResults": bad}),
            )
            .await
            .expect_err("refused");
            assert_eq!(error.kind, ErrorKind::InvalidParameterValue, "{bad}");
        }
    }

    #[tokio::test]
    async fn a_source_that_never_had_a_task_lists_nothing() {
        let rig = dead_lettered(1).await;
        assert_eq!(
            listed(&rig, "orders", MAX_TASK_RESULTS).await,
            Vec::<Value>::new()
        );
    }

    /// One source's tasks are not another's: the key's first segment is the
    /// source, escaped, so no prefix can reach into a queue whose name merely
    /// starts with the same bytes.
    #[tokio::test]
    async fn one_sources_tasks_are_not_another_sources() {
        let policy = redrive_policy("orders-dlq", 1);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders-dlq-2", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        let key = Registry::key_move_task("orders-dlq", now_epoch_ms(), "one");
        let task = Task {
            source: "orders-dlq".to_string(),
            destination: None,
            status: TaskStatus::Completed,
            moved: 1,
            total: 1,
            rate: None,
            started_ms: now_epoch_ms(),
            heartbeat_ms: now_epoch_ms(),
            failure: None,
        };
        rig.fake.kv_seed(NS, &key, task.to_value());
        assert_eq!(listed(&rig, "orders-dlq", MAX_TASK_RESULTS).await.len(), 1);
        assert!(listed(&rig, "orders-dlq-2", MAX_TASK_RESULTS)
            .await
            .is_empty());
    }

    // ----------------------------------------------------------- the rate cap

    /// `MaxNumberOfMessagesPerSecond`, on a paused clock: the mover moves a
    /// batch and then waits for as long as that batch was worth.
    #[tokio::test(start_paused = true)]
    async fn the_rate_cap_paces_the_batches() {
        let policy = redrive_policy("orders-dlq", 1);
        let rig = Rig::new(&[
            ("orders-dlq", &[]),
            ("orders", &[("RedrivePolicy", policy.as_str())]),
        ])
        .await;
        // Twenty-five on ONE lane of the dead-letter queue, each carrying the
        // source it came from, so the run is three batches of 10, 10 and 5.
        let mut payloads = Vec::new();
        for i in 0..25 {
            payloads.push(json!({
                "b": format!("m{i}"),
                "s": {crate::actions::dlq::SYS_SOURCE_QUEUE: "orders"},
            }));
        }
        rig.fake.seed("orders-dlq", "0", 0, &payloads);

        let mut mover = started(&rig, json!({"MaxNumberOfMessagesPerSecond": 5})).await;
        let started_at = tokio::time::Instant::now();
        mover.run().await;
        let spent = started_at.elapsed();
        // 25 messages at five a second, paid between batches.
        assert_eq!(spent, Duration::from_secs(5));
        assert_eq!(
            listed(&rig, "orders-dlq", 1).await[0]["ApproximateNumberOfMessagesMoved"],
            json!(25)
        );
    }

    /// ...and a task with no cap does not wait at all.
    #[tokio::test(start_paused = true)]
    async fn a_task_without_a_cap_moves_at_the_brokers_pace() {
        let rig = dead_lettered(2).await;
        let mut mover = started(&rig, json!({})).await;
        let started_at = tokio::time::Instant::now();
        mover.run().await;
        assert_eq!(started_at.elapsed(), Duration::ZERO);
    }

    // ---------------------------------------------------------- the spawn path

    /// The action itself: it answers a handle immediately and the messages move
    /// afterwards, which is what makes it AWS-shaped rather than a synchronous
    /// drain wearing a task's name.
    #[tokio::test]
    async fn the_action_answers_a_handle_and_the_mover_runs_behind_it() {
        let rig = dead_lettered(2).await;
        let answer = start_message_move_task(&rig.ctx, &start_params("orders-dlq", json!({})))
            .await
            .expect("started");
        let handle = answer["TaskHandle"].as_str().expect("a handle").to_string();
        assert!(key_of(&handle).expect("ours").starts_with("qs:mv:"));

        // The spawned mover progresses as soon as this task yields.
        let mut status = String::new();
        for _ in 0..200 {
            tokio::task::yield_now().await;
            let results = listed(&rig, "orders-dlq", 1).await;
            status = results[0]["Status"]
                .as_str()
                .unwrap_or_default()
                .to_string();
            if status == "COMPLETED" {
                break;
            }
        }
        assert_eq!(status, "COMPLETED");
        assert_eq!(drain(&rig, "orders").await.len(), 2);
    }

    // ------------------------------------------------------------- the record

    #[test]
    fn a_task_round_trips_through_the_store() {
        let task = Task {
            source: "orders-dlq".to_string(),
            destination: Some("orders".to_string()),
            status: TaskStatus::Cancelling,
            moved: 12,
            total: 40,
            rate: Some(7),
            started_ms: 1_700_000_000_000,
            heartbeat_ms: 1_700_000_001_000,
            failure: Some("because".to_string()),
        };
        assert_eq!(Task::from_value(&task.to_value()), task);
    }

    /// A row whose status cannot be read is FAILED and never dropped: the
    /// record's existence is what a fence points at, and a task that vanished
    /// from a listing while its fence stood would be a source nobody could start
    /// and nobody could see why.
    #[test]
    fn an_unreadable_row_is_a_failed_task_rather_than_no_task() {
        let task = Task::from_value(&json!({"source": "orders-dlq", "status": "WAT"}));
        assert_eq!(task.status, TaskStatus::Failed);
        assert!(task.status.is_terminal());
        // A row written before the heartbeat existed reads its own start, which
        // is the truth for a task nobody has updated.
        let task = Task::from_value(&json!({"status": "RUNNING", "started": 1_000}));
        assert_eq!(task.heartbeat_ms, 1_000);
        assert!(task.stale_at(1_000 + STALE_AFTER_MS + 1));
        assert!(!task.stale_at(1_000 + STALE_AFTER_MS - 1));
    }

    #[test]
    fn the_key_orders_tasks_newest_first_and_the_handle_round_trips() {
        let older = Registry::key_move_task("dlq", 1_000, "a");
        let newer = Registry::key_move_task("dlq", 2_000, "a");
        assert!(newer < older, "{newer} must sort before {older}");
        assert!(older.starts_with(&Registry::key_move_tasks("dlq")));
        assert_eq!(key_of(&handle_of(&newer)).expect("ours"), newer);
    }

    #[test]
    fn every_status_has_one_spelling_and_it_is_aws_own() {
        for (status, text) in [
            (TaskStatus::Running, "RUNNING"),
            (TaskStatus::Cancelling, "CANCELLING"),
            (TaskStatus::Cancelled, "CANCELLED"),
            (TaskStatus::Completed, "COMPLETED"),
            (TaskStatus::Failed, "FAILED"),
        ] {
            assert_eq!(status.as_str(), text);
            assert_eq!(TaskStatus::of(text), Some(status));
        }
        assert_eq!(TaskStatus::of("running"), None);
        assert!(!TaskStatus::Running.is_terminal());
        assert!(!TaskStatus::Cancelling.is_terminal());
    }
}
