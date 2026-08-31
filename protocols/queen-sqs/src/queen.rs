//! The Queen side of the facade: an HTTP client for the broker's data and admin
//! surfaces, plus the short-lived cache the queue lookups read through.
//!
//! This is queen-kafka's `src/queen.rs` with the verb set SQS needs. The two
//! facades consume Queen through DIFFERENT halves of the broker and that is the
//! whole difference between the files: Kafka reads the log positionally
//! (`/api/v1/fetch`, offsets, no lease), SQS consumes through the ordinary
//! consumer protocol (`/api/v1/pop`, a lease, an ack), which is also why proxy
//! metering works for this facade on day one and does not for the other
//! (PLAN_QUEEN_SQS.md, "Core changes"). When `crates/queen-facade` is extracted
//! at M5 the two files become one crate with the union of these verbs; until
//! then this one is written so that the union is a mechanical merge — same
//! error type, same alignment discipline, same `BoxFuture` trait shape.
//!
//! The token is an argument of every call, never bound into the client, for the
//! reason queen-kafka's header gives and one more of its own: SigV4 identifies a
//! PRINCIPAL per request (`QUEEN_SQS_CREDENTIALS=akid:secret:queen_token`), so
//! one process serves many principals over one connection pool and a token that
//! belonged to the client object would mean one pool per credential.
//!
//! Routes used, and their shapes, read off server/src/main.rs, the handlers and
//! the stored procedures (never guessed):
//!
//!   * `GET /api/v1/resources/queues` → the enriched queue list
//!     (server/src/handlers/queues.rs, 018_stats.sql).
//!   * `POST /api/v1/configure` `{"queue":…, …options}` → `configure_queue_v1`
//!     (012_configure.sql), an UPSERT of every config column: it may only be
//!     called for a queue this facade is creating or deliberately reconfiguring.
//!   * `DELETE /api/v1/resources/queues/:queue` → 200 either way, `deleted`
//!     rewritten to mirror `existed` (server/src/handlers/queues.rs).
//!   * `GET /api/v1/resources/queues/:queue/depth[?group=]` →
//!     `log_queue_depth_v1` (011_log_stats.sql). This is
//!     `ApproximateNumberOfMessages` / `…NotVisible`, which KEDA and every
//!     autoscaler read: load-bearing, not decoration.
//!   * `POST /api/v1/push` `{"items":[{queue,partition,payload,transactionId?}]}`
//!     → one object per item carrying `message_id`, `status` and the absolute
//!     `offset` (C1). MessageId and SequenceNumber both come from here.
//!   * `GET /api/v1/pop/queue/:queue[/partition/:p]` → a LEASE envelope plus the
//!     messages (server/src/handlers/data.rs, `render_pop_parts`). SQS
//!     ReceiveMessage is N of these at `batch=1` in exact mode.
//!   * `POST /api/v1/ack` / `POST /api/v1/ack/batch` → a top-level array, one
//!     entry per ack, in request order (`{index, transactionId, success, error,
//!     leaseReleased, dlq, noop}`). DeleteMessage is `completed`; a visibility
//!     timeout of zero is `retry`, which releases without charging.
//!   * `POST /api/v1/lease/:leaseId/extend` `{"seconds":n}` →
//!     ChangeMessageVisibility.
//!   * `POST /api/v1/kv` → the SQS/SNS registry (`qs:` keys) and the delete-sets.
//!   * `POST /api/v1/timers`, `GET /api/v1/timers/:queue`,
//!     `DELETE /api/v1/timers/:queue/*timerKey` → per-message `DelaySeconds`.
//!   * `POST /api/v1/transaction` → the redrive move (push-to-DLQ + ack-original
//!     in ONE transaction) and the SNS fan-out.

use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::future::Future;
use std::hash::BuildHasher;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// A boxed future, so [`QueenApi`] stays dyn-compatible. `async fn` in a trait
/// is not: it desugars to an opaque associated type, which no trait object can
/// name. The facade holds `Arc<dyn QueenApi>` precisely so the tests can hand it
/// a double instead of a socket.
pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

pub type Result<T> = std::result::Result<T, Error>;

/// Why a call to Queen did not produce an answer. Kept coarse on purpose: the
/// mapping from these to the SQS error catalog is [`crate::error`]'s job, and
/// the distinction here exists for that mapping and for the log line, not for
/// control flow inside this module.
///
/// `Clone` because [`Catalog`] caches the failure as well as the success and
/// hands the same one to every caller inside the window.
#[derive(Debug, Clone)]
pub enum Error {
    /// The request never completed: DNS, connect, TLS, timeout, reset.
    Transport(String),
    /// Queen answered, with a status that is not a success.
    Status {
        code: u16,
        body: String,
        /// The `Retry-After` the answer carried, in milliseconds. The proxy
        /// sets it on every 429 it writes (proxy/src/errors.rs) and on nothing
        /// else. It is carried rather than mapped on the spot because its
        /// destination is an SQS `OverLimit` with a client-visible backoff.
        retry_after_ms: Option<i64>,
    },
    /// Queen answered 2xx with a body this client cannot read.
    Body(String),
    /// A conditional write lost its precondition and carried `"required": true`,
    /// so the stored procedure raised `check_violation` and the WHOLE call
    /// rolled back: not one operation landed (024_kv.sql:546-560, 1498-1502).
    ///
    /// It reaches this client as HTTP **200** with
    /// `{"ok":false,"reason":"kv_precondition",…}`, deliberately
    /// (server/src/handlers/kv.rs): "this is the EXPECTED outcome of every
    /// legitimate redelivery … it must pollute neither the error metrics nor the
    /// retry policies". It is an `Error` here for one reason only — it is the
    /// answer to a call that wrote nothing — and the fields are the loser's
    /// whole verdict, which is what makes a second round trip unnecessary.
    ///
    /// The registry ([`crate::registry`]) is the caller that has to read it: a
    /// CreateQueue racing another CreateQueue for the same name is exactly this.
    /// A push inside a TRANSACTION carried a `transactionId` the broker had
    /// already filed inside that queue's dedup window, so the stored procedure
    /// raised and the WHOLE bundle rolled back: *"A duplicate is a SOFT verdict
    /// for plain pushes but a HARD error inside a transaction"*
    /// (005_log_ack.sql). It reaches this client as HTTP **200** with
    /// `{"ok":false,"reason":"duplicate",…}`, the same deliberate shape a lost
    /// KV precondition takes.
    ///
    /// Its own variant rather than a [`Error::Body`] with a string in it,
    /// because the one caller that can produce it has to ANSWER it: an SNS
    /// publish to a FIFO topic files each delivery under the client's
    /// `MessageDeduplicationId`, and a repeat inside the window is what SQS
    /// itself reports as a `duplicate` status and a success. String-matching a
    /// message to find that out is what a taxonomy exists to prevent.
    Duplicate(String),
    Precondition {
        /// The operation that lost, by input ordinal.
        failed_index: usize,
        /// `version`, `absent` or `exists`, as the stored procedure named it.
        reason: String,
        /// The winner's version, or 0 when the key is not there. **Advisory**
        /// (024_kv.sql:1467-1471): a value to re-`expect` with once, never a
        /// fencing token to reuse blindly.
        version: i64,
        /// The winner's value, so the loser already knows what the winner did.
        value: serde_json::Value,
    },
}

impl Error {
    /// A non-2xx answer that named no `Retry-After`.
    pub fn status(code: u16, body: impl Into<String>) -> Error {
        Error::Status {
            code,
            body: body.into(),
            retry_after_ms: None,
        }
    }

    /// The `Retry-After` this failure carried, in milliseconds.
    pub fn retry_after_ms(&self) -> Option<i64> {
        match self {
            Error::Status { retry_after_ms, .. } => *retry_after_ms,
            _ => None,
        }
    }

    /// The HTTP status Queen answered, when it answered at all.
    pub fn http_status(&self) -> Option<u16> {
        match self {
            Error::Status { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Transport(e) => write!(f, "transport: {e}"),
            Error::Status {
                code,
                body,
                retry_after_ms,
            } => match retry_after_ms {
                Some(ms) => write!(f, "HTTP {code} (retry after {ms}ms): {}", Snippet(body)),
                None => write!(f, "HTTP {code}: {}", Snippet(body)),
            },
            Error::Body(e) => write!(f, "body: {e}"),
            Error::Duplicate(e) => write!(
                f,
                "a duplicate push rolled the transaction back and nothing was written: {}",
                Snippet(e)
            ),
            Error::Precondition {
                failed_index,
                reason,
                version,
                ..
            } => write!(
                f,
                "kv precondition lost on operation {failed_index} ({reason}); \
                 the current version is {version} and nothing was written"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// A response body in a log line, clamped. Queen error bodies are small JSON,
/// but a misrouted request can land on anything (an ingress error page, a
/// dashboard bundle), and that must not become the log.
struct Snippet<'a>(&'a str);

impl std::fmt::Display for Snippet<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const MAX: usize = 200;
        let s = self.0.trim();
        match s.char_indices().nth(MAX) {
            Some((cut, _)) => write!(f, "{}…", &s[..cut]),
            None => write!(f, "{s}"),
        }
    }
}

// ---------------------------------------------------------------------- queues

/// One queue as the admin list reports it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Queue {
    pub name: String,
    /// Live count of `queen.log_partitions` rows for this queue.
    ///
    /// It is NOT the declared width an SQS queue is created with: Queen has no
    /// such declaration — "the log engine creates partitions lazily on the first
    /// push" (012_configure.sql) — so a queue created a second ago reports 0 and
    /// a queue written to on lanes 0 and 7 reports 2. The width an SQS standard
    /// queue synthesizes (`queen.partitions`, default 64) lives in the registry
    /// ([`crate::registry`]) and never here.
    #[serde(default)]
    pub partitions: i64,
    /// The queue's `id` (018_stats.sql:249). Read as an OPAQUE token and
    /// compared for equality only: it is what tells the facade that the registry
    /// record it found belongs to the queue that is there NOW and not to one
    /// that was dropped and recreated under the same name — which is exactly the
    /// PurgeQueue delete-and-recreate window (PLAN_QUEEN_SQS.md, D3).
    #[serde(default)]
    pub id: Option<String>,
}

/// What one `DELETE /api/v1/resources/queues/:queue` did.
///
/// One field, because the route reports one thing this facade can act on. The
/// SP always answers `deleted:true` and hides the real outcome in `existed`,
/// which the route then corrects — it rewrites `deleted` to mirror `existed`
/// precisely so a client that trusts `deleted` is not told a queue it never had
/// was removed (server/src/handlers/queues.rs). This reads the corrected field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Deleted {
    /// Whether there was a queue of that name to delete.
    pub existed: bool,
}

/// The delete body, as the route writes it. Only the one field is named; the SP
/// also reports counts the dashboard uses and none of that may become a parse
/// failure here.
#[derive(Debug, Deserialize)]
struct DeleteQueueBody {
    #[serde(default)]
    deleted: bool,
    /// Present when the SP answered rather than the route — read only to make a
    /// stored-procedure refusal loud instead of silently "not deleted".
    #[serde(default)]
    error: Option<serde_json::Value>,
}

/// `GET /api/v1/resources/queues/:queue/depth` — the backlog numbers, per
/// partition and summed (011_log_stats.sql, `log_queue_depth_v1`).
///
/// `pending` is `ApproximateNumberOfMessages`, `processing` is
/// `ApproximateNumberOfMessagesNotVisible`. The route 404s for a queue that is
/// not there, which is what makes this the cheap existence probe as well.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Depth {
    #[serde(default)]
    pub queue: String,
    #[serde(default)]
    pub group: Option<String>,
    /// Log depth: positions still to retire for the group.
    #[serde(default)]
    pub pending: i64,
    /// Leased right now — SQS's "not visible".
    #[serde(default)]
    pub processing: i64,
    /// Pending and not leased.
    #[serde(default)]
    pub ready: i64,
    #[serde(rename = "partitionsPending", default)]
    pub partitions_pending: i64,
    #[serde(rename = "partitionsReady", default)]
    pub partitions_ready: i64,
    #[serde(default)]
    pub partitions: Vec<PartitionDepth>,
}

/// One lane's share of a [`Depth`].
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PartitionDepth {
    #[serde(default)]
    pub partition: String,
    #[serde(default)]
    pub pending: i64,
    #[serde(default)]
    pub processing: i64,
    #[serde(default)]
    pub ready: i64,
}

// ----------------------------------------------------------------------- write

/// One message to write, as `POST /api/v1/push` takes it.
///
/// `transaction_id` is OPTIONAL and, unlike queen-kafka, this facade sends it:
/// it is the broker's dedup key and SQS hands us one by name.
/// `MessageDeduplicationId` (or the SHA-256 of the body under
/// `ContentBasedDeduplication`) is exactly a dedup key with a window, which is
/// the queue's `dedupWindowSeconds`. On a STANDARD queue nothing is sent, which
/// is at-least-once — the guarantee SQS standard queues themselves give.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct PushItem {
    pub queue: String,
    /// The Queen partition NAME. A standard queue's lanes are decimal-named
    /// `"0".."M-1"`; a FIFO queue's lane IS the `MessageGroupId`
    /// (PLAN_QUEEN_SQS.md, Semantics).
    pub partition: String,
    pub payload: serde_json::Value,
    #[serde(rename = "transactionId", skip_serializing_if = "Option::is_none")]
    pub transaction_id: Option<String>,
}

impl PushItem {
    /// A push with no dedup key — the standard-queue shape.
    pub fn new(queue: &str, partition: &str, payload: serde_json::Value) -> PushItem {
        PushItem {
            queue: queue.to_string(),
            partition: partition.to_string(),
            payload,
            transaction_id: None,
        }
    }

    /// A push whose `transactionId` is the caller's dedup key — the FIFO shape.
    pub fn deduped(
        queue: &str,
        partition: &str,
        payload: serde_json::Value,
        transaction_id: &str,
    ) -> PushItem {
        PushItem {
            transaction_id: Some(transaction_id.to_string()),
            ..PushItem::new(queue, partition, payload)
        }
    }
}

/// What the broker did with one pushed item (`render_push_results`).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Pushed {
    /// `queued`, `duplicate`, `error`, `buffered` or `failed`. Kept as it came
    /// so a refusal can name itself in the log rather than becoming a bare "no
    /// offset". `duplicate` is not an error on a FIFO queue: it is the dedup
    /// window doing its job, and SQS answers it with the ORIGINAL MessageId,
    /// which is what the broker puts in `message_id` for a duplicate.
    #[serde(default)]
    pub status: String,
    /// The broker's message uuid. This is the SQS `MessageId`.
    #[serde(rename = "message_id", default)]
    pub message_id: String,
    /// The dedup key the broker filed this under — minted by the broker when the
    /// item carried none.
    #[serde(rename = "transaction_id", default)]
    pub transaction_id: String,
    /// The assigned absolute offset within the partition (C1). This is the FIFO
    /// `SequenceNumber`. Absent whenever the broker allocated none: a spooled
    /// item in maintenance mode, an item whose bundle failed. Never guessed.
    #[serde(default)]
    pub offset: Option<i64>,
}

// ------------------------------------------------------------------------ read

/// Max messages one `GET /api/v1/pop` asks for.
///
/// It is THIS FACADE'S ceiling and not the broker's: 200 is what the broker
/// applies when `batch` is ABSENT (`p.batch.unwrap_or(200)`, server/src/handlers/
/// data.rs) and the route enforces no ceiling of its own, so nothing upstream
/// would refuse or clamp a larger number. The facade sends `batch=1` anyway —
/// claim width 1 is what makes every SQS verb exact — and this is the clamp on
/// the value it sends.
pub const MAX_POP_BATCH: i32 = 200;
/// Max long-poll parking, milliseconds. SQS caps `WaitTimeSeconds` at 20, which
/// is inside this, so the SQS ceiling is always the binding one.
pub const MAX_POP_TIMEOUT_MS: u64 = 30_000;
/// The consumer group every SQS queue is consumed under. SQS has no groups, and
/// this is the broker's queue-mode sentinel: hard-pinned to subscription mode
/// `all` by the SQL, which is the "deliver everything that was ever written"
/// semantics an SQS queue has.
pub const QUEUE_MODE_GROUP: &str = "__QUEUE_MODE__";

/// What one pop asks for.
///
/// `wait`/`timeout_ms` are the long poll (`WaitTimeSeconds`), `lease_seconds` is
/// the effective visibility timeout, and `partitions` is how many lanes one call
/// may claim — 1 in `exact` mode, k in `amortized` (PLAN_QUEEN_SQS.md, the
/// batching dial).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PopOptions {
    /// Messages per claimed partition.
    pub batch: i32,
    /// Lanes this call may claim.
    pub partitions: i32,
    /// Visibility timeout for the claim, seconds. 0 means "the queue's".
    pub lease_seconds: i32,
    /// Park when there is nothing, up to `timeout_ms`.
    pub wait: bool,
    pub timeout_ms: u64,
    /// `None` is queue mode ([`QUEUE_MODE_GROUP`]), which is what every SQS
    /// queue uses.
    pub consumer_group: Option<String>,
}

impl Default for PopOptions {
    /// The `exact` receive mode's own shape: one message, one lane, no parking.
    /// Claim width 1 is what makes every SQS verb exact — a lease holds one
    /// message, so ChangeMessageVisibility, a terminate and a DeleteMessage each
    /// address exactly what the caller thinks they address.
    fn default() -> PopOptions {
        PopOptions {
            batch: 1,
            partitions: 1,
            lease_seconds: 0,
            wait: false,
            timeout_ms: 0,
            consumer_group: None,
        }
    }
}

impl PopOptions {
    /// The group this pop runs under, resolved.
    pub fn group(&self) -> &str {
        self.consumer_group.as_deref().unwrap_or(QUEUE_MODE_GROUP)
    }
}

/// What one pop answered: the lease envelope and its messages.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct Popped {
    #[serde(default)]
    pub queue: String,
    /// The FIRST claimed partition's name; empty when nothing was claimed.
    #[serde(default)]
    pub partition: String,
    #[serde(rename = "partitionId", default)]
    pub partition_id: String,
    /// The worker id the claim was taken with. Every ack of this batch and every
    /// `lease/extend` addresses it. Non-empty even on an empty pop.
    #[serde(rename = "leaseId", default)]
    pub lease_id: String,
    #[serde(rename = "consumerGroup", default)]
    pub consumer_group: String,
    #[serde(default)]
    pub messages: Vec<Message>,
    #[serde(rename = "partitionsClaimed", default)]
    pub partitions_claimed: i64,
}

/// One delivered message.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Message {
    /// The broker's message uuid — the SQS `MessageId`.
    #[serde(default)]
    pub id: String,
    /// The dedup key. This is what an ack is addressed BY, and what the receipt
    /// handle therefore carries.
    #[serde(rename = "transactionId", default)]
    pub transaction_id: String,
    /// The stored payload, spliced back verbatim: the SQS envelope
    /// ([`crate::envelope`]) for a message this facade wrote, and a native
    /// producer's own JSON otherwise.
    #[serde(default)]
    pub data: serde_json::Value,
    #[serde(default)]
    pub partition: String,
    #[serde(rename = "partitionId", default)]
    pub partition_id: String,
    #[serde(rename = "leaseId", default)]
    pub lease_id: String,
    #[serde(rename = "consumerGroup", default)]
    pub consumer_group: String,
    /// `log_consumers.attempt_count` — per CLAIM, not per message. At claim
    /// width 1 (the `exact` mode) that is exact and it is
    /// `ApproximateReceiveCount`.
    ///
    /// DIVERGENCE, `accepted`: inside a FIFO batch it is the CLAIM's, so every
    /// message of one claim reports the same count — including one that joined
    /// the lane between two deliveries and has only ever been delivered once.
    /// The count lives on the consumer row and nothing in the log counts
    /// deliveries per message, so answering per-message would mean a second
    /// store; the field's own name buys the slack, and PLAN_QUEEN_SQS.md lists
    /// it among the non-goals. Pinned by
    /// `actions::fifo::tests::every_message_of_one_fifo_claim_reports_the_same_receive_count`.
    #[serde(rename = "deliveryAttempt", default)]
    pub delivery_attempt: i64,
    /// The SEGMENT's creation instant as the broker renders it,
    /// `YYYY-MM-DDTHH:MM:SS.ffffffZ`. Per push call, not per message — which is
    /// what `SentTimestamp` is derived from.
    #[serde(rename = "createdAt", default)]
    pub created_at: String,
    /// The ABSOLUTE offset this message occupies in its partition's log — the
    /// same number the PUSH side reports (C1, [`Pushed::offset`]) and therefore
    /// the same `SequenceNumber` a FIFO `SendMessage` answered for it
    /// (`actions::messages::system_view`).
    ///
    /// `None` is a broker that predates C-SQS-3: `render_pop_parts`
    /// (server/src/handlers/data.rs) emits `offset` on every popped message
    /// today, and emitted none before it. Absence is TOLERATED rather than
    /// treated as a protocol error, because this facade is deployed beside a
    /// broker an operator upgrades separately, and an older one is a facade
    /// answering one field less — not a facade that cannot serve a receive.
    /// Nothing is ever synthesized to fill it: a number derived from the
    /// delivery position would order a group's messages differently from the
    /// way the log does.
    #[serde(default)]
    pub offset: Option<i64>,
}

// ------------------------------------------------------------------------- ack

/// The four ack outcomes the broker normalizes to
/// (server/src/handlers/mod.rs::normalize_ack_status), and what each one is in
/// SQS terms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckStatus {
    /// `DeleteMessage`. At claim width 1 there is no gap to swallow.
    Completed,
    /// A consumer that gave up: charges the retry budget once and releases the
    /// lease, so the message comes back. Not on the SQS path today — a client
    /// that stops calling gets the same effect through lease expiry — and here
    /// because the redrive loop needs to be able to say it.
    Failed,
    /// `ChangeMessageVisibility(0)`: release the lease WITHOUT charging the
    /// retry budget. The one status SQS's terminate maps to exactly.
    Retry,
    /// Force the dead-letter hand-off, bypassing the remaining budget.
    Dlq,
}

impl AckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            AckStatus::Completed => "completed",
            AckStatus::Failed => "failed",
            AckStatus::Retry => "retry",
            AckStatus::Dlq => "dlq",
        }
    }
}

impl Serialize for AckStatus {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

/// One ack, in the exact shape `POST /api/v1/ack` and the `acknowledgments`
/// array of `POST /api/v1/ack/batch` take it.
///
/// The wire is `partitionId`-keyed and not queue-keyed, which is why the receipt
/// handle carries the partition id: a delete has to be servable by an instance
/// that never saw the receive ([`crate::handle`]).
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct AckItem {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,
    #[serde(rename = "partitionId")]
    pub partition_id: String,
    pub status: AckStatus,
    /// The worker id the claim was taken with. An ack that names one is
    /// validated against the live lease; an ack that names none still advances
    /// the cursor (005_log_ack.sql, RUSTFIX item 11) — which is precisely the
    /// hole a receipt handle must not open, so the facade always sends it.
    #[serde(rename = "leaseId", skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    /// The reason a `failed` ack failed, recorded on the DLQ row.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl AckItem {
    /// `DeleteMessage`.
    pub fn completed(transaction_id: &str, partition_id: &str, lease_id: &str) -> AckItem {
        AckItem::new(transaction_id, partition_id, lease_id, AckStatus::Completed)
    }

    /// `ChangeMessageVisibility(0)` — release, charge nothing.
    pub fn released(transaction_id: &str, partition_id: &str, lease_id: &str) -> AckItem {
        AckItem::new(transaction_id, partition_id, lease_id, AckStatus::Retry)
    }

    pub fn new(
        transaction_id: &str,
        partition_id: &str,
        lease_id: &str,
        status: AckStatus,
    ) -> AckItem {
        AckItem {
            transaction_id: transaction_id.to_string(),
            partition_id: partition_id.to_string(),
            status,
            lease_id: Some(lease_id.to_string()),
            error: None,
        }
    }
}

/// What the broker did with one ack. One per request item, in request order,
/// stamped with its own `index`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Acked {
    #[serde(default)]
    pub index: usize,
    #[serde(rename = "transactionId", default)]
    pub transaction_id: String,
    #[serde(default)]
    pub success: bool,
    /// Why it did not land: `invalid or expired lease`, `already committed`,
    /// `consumer not found`, `no leased batch`, `unresolved`. The first of those
    /// is `ReceiptHandleIsInvalid`/`MessageNotInflight` on the SQS side, and the
    /// distinction is the whole reason this is a string and not a bool.
    #[serde(default)]
    pub error: Option<String>,
    #[serde(rename = "leaseReleased", default)]
    pub lease_released: bool,
    #[serde(default)]
    pub dlq: bool,
    /// A harmless duplicate commit: the position was already below the cursor.
    /// A second `DeleteMessage` for the same message is this, and SQS answers
    /// it success — which is what makes the facade's delete idempotent.
    #[serde(default)]
    pub noop: bool,
}

/// What `POST /api/v1/lease/:leaseId/extend` answered.
///
/// The route is ALWAYS 200 (best-effort renewal), so `success` and `renewed` are
/// the only truth: `renewed == 0` means the lease is gone — expired, acked, or
/// never existed — which on the SQS side is `MessageNotInflight`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct LeaseExtended {
    #[serde(rename = "leaseId", default)]
    pub lease_id: String,
    #[serde(default)]
    pub success: bool,
    #[serde(default)]
    pub renewed: i64,
    /// The same instant the route writes under three keys; this reads the
    /// canonical one.
    #[serde(rename = "newExpiresAt", default)]
    pub expires_at: Option<String>,
}

// -------------------------------------------------------------------- key/value

/// Max operations in one `POST /api/v1/kv` (`QUEEN_KV_MAX_OPS_PER_CALL`).
pub const MAX_KV_OPS_PER_CALL: usize = 256;
/// Max KV operations in the rider array of one `POST /api/v1/transaction`
/// (server/src/handlers/data.rs, `WIRE_KV_MAX_OPS`). DELIBERATELY tighter than
/// [`MAX_KV_OPS_PER_CALL`] and not this facade's choice: the KV row lock taken at
/// step 0 of a bundle is held for the whole of it — every push, every ack, the
/// fsync.
pub const WIRE_KV_MAX_OPS: usize = 64;
/// Max keys summed over the KV rider of one transaction (`WIRE_KV_MAX_KEYS`).
pub const WIRE_KV_MAX_KEYS: usize = 256;
/// Max keys summed over one call's operations (`QUEEN_KV_MAX_KEYS_PER_CALL`).
pub const MAX_KV_KEYS_PER_CALL: usize = 1024;
/// Ceiling a `getPrefix` limit is CLAMPED to, never rejected against.
pub const MAX_KV_PREFIX_LIMIT: i64 = 1000;
/// Max serialized bytes of one value (`QUEEN_KV_MAX_VALUE_BYTES`).
pub const MAX_KV_VALUE_BYTES: usize = 65_536;
/// Total value bytes ONE call may read back before the stored procedure starts
/// truncating (`QUEEN_KV_MAX_READ_BYTES`, 4 MiB). Not an error and not a clamp a
/// caller can see coming: rows past it are simply not returned and `truncated`
/// is how the answer says so. A registry listing that ignores it under-reports
/// the tenant's queues, which is why [`KvAnswer::truncated`] is never optional.
pub const MAX_KV_READ_BYTES: usize = 4 * 1024 * 1024;

/// One key/value operation, in the exact shape `kv_apply_v1` takes.
///
/// `ns` is a separate field from `key` and is validated against
/// `^[a-z0-9][a-z0-9._-]{0,63}$` by the stored procedure, so it is never a place
/// to put anything a client chose. Everything this facade stores lives under one
/// namespace with a `qs:` key prefix ([`crate::registry`]).
///
/// EXPIRY IS MANDATORY on every write (024_kv.sql:565-575): exactly one of
/// `ttlSeconds` and `forever`, deliberately, so that nothing lands in the store
/// without someone having decided when it leaves. A queue's registry record is
/// `forever` (it IS the queue); a FIFO delete-set is the lease plus slack; a
/// `ReceiveRequestAttemptId` is the visibility timeout.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op")]
pub enum KvOp {
    /// One key. `found` is separate from the value because `'null'::jsonb` is a
    /// legal stored value (024_kv.sql §5.5).
    #[serde(rename = "get")]
    Get { ns: String, key: String },
    /// A known key list. Answers `rows` for the ones that are there and
    /// `missing` for the ones that are not — absence is a datum, not a hole the
    /// caller computes by difference.
    #[serde(rename = "getMany")]
    GetMany { ns: String, keys: Vec<String> },
    /// A key range by prefix, paged with an exclusive `after` cursor in BYTE
    /// order. The one operation `POST /api/v1/kv` has and the transaction wire
    /// does not — which is why ListQueues is never a rider.
    #[serde(rename = "getPrefix")]
    GetPrefix {
        ns: String,
        prefix: String,
        limit: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        after: Option<String>,
    },
    /// An upsert, optionally conditional. Built through the constructors below
    /// rather than by writing the fields, because two of them are mutually
    /// exclusive and one of them is a fence.
    #[serde(rename = "put")]
    Put {
        ns: String,
        key: String,
        value: serde_json::Value,
        /// Serialized only when true: `"forever": false` is zero expiry
        /// declarations to the stored procedure, not one.
        #[serde(skip_serializing_if = "not_set")]
        forever: bool,
        #[serde(rename = "ttlSeconds", skip_serializing_if = "Option::is_none")]
        ttl_seconds: Option<u64>,
        /// `expect: 0` is "must not exist" — what `putIfAbsent` desugars to
        /// (024_kv.sql:960-966). `expect: N > 0` is a PURE UPDATE that creates
        /// nothing when the key is absent.
        #[serde(skip_serializing_if = "Option::is_none")]
        expect: Option<i64>,
        /// Turn a lost precondition from a verdict into an abort of the whole
        /// call. See [`Error::Precondition`].
        #[serde(skip_serializing_if = "not_set")]
        required: bool,
    },
    /// Remove a key. `expect: 0` is "it must not exist" (idempotent success when
    /// it is not); `expect: N` is a fenced delete.
    #[serde(rename = "delete")]
    Delete {
        ns: String,
        key: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        expect: Option<i64>,
    },
    /// A numeric counter, with optional bounds. It takes NO `expect`: it is the
    /// way OUT of compare-and-set (024_kv.sql §5.4), and `applied:false` with
    /// `reason:"limit"` IS the admission decision — the value that would have
    /// broken the ceiling is never written. `MaxNumberOfMessagesPerSecond` on a
    /// message-move task is exactly this.
    #[serde(rename = "incr")]
    Incr {
        ns: String,
        key: String,
        delta: i64,
        #[serde(skip_serializing_if = "Option::is_none")]
        min: Option<i64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max: Option<i64>,
        #[serde(skip_serializing_if = "not_set")]
        forever: bool,
        #[serde(rename = "ttlSeconds", skip_serializing_if = "Option::is_none")]
        ttl_seconds: Option<u64>,
    },
}

/// `skip_serializing_if` for a `bool` that is only ever written when set. Both
/// of [`KvOp::Put`]'s flags mean something by their PRESENCE.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn not_set(b: &bool) -> bool {
    !*b
}

impl KvOp {
    pub fn get(ns: &str, key: &str) -> KvOp {
        KvOp::Get {
            ns: ns.to_string(),
            key: key.to_string(),
        }
    }

    pub fn get_many(ns: &str, keys: &[String]) -> KvOp {
        KvOp::GetMany {
            ns: ns.to_string(),
            keys: keys.to_vec(),
        }
    }

    pub fn get_prefix(ns: &str, prefix: &str, limit: i64, after: Option<&str>) -> KvOp {
        KvOp::GetPrefix {
            ns: ns.to_string(),
            prefix: prefix.to_string(),
            limit,
            after: after.map(str::to_string),
        }
    }

    /// An unconditional write that never expires — a registry record's own
    /// shape.
    pub fn put(ns: &str, key: &str, value: serde_json::Value) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: None,
            required: false,
        }
    }

    /// A write that expires in `ttl_seconds`, conditional on `expect` when one
    /// is given — the delete-set of a FIFO batch, whose TTL is the lease plus
    /// slack so it cannot outlive the redelivery it exists to suppress.
    pub fn put_ttl(
        ns: &str,
        key: &str,
        value: serde_json::Value,
        ttl_seconds: u64,
        expect: Option<i64>,
    ) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: false,
            ttl_seconds: Some(ttl_seconds),
            expect,
            required: false,
        }
    }

    /// `putIfAbsent` on a key that never expires — CreateQueue's own shape. It
    /// answers a VERDICT rather than aborting, and that verdict carries the
    /// winner's value, which is exactly what QueueAlreadyExists needs to compare
    /// attributes against without a second round trip (024_kv.sql:1467-1471).
    pub fn put_if_absent(ns: &str, key: &str, value: serde_json::Value) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: Some(0),
            required: false,
        }
    }

    /// The same, with an expiry — `ReceiveRequestAttemptId`, whose TTL is the
    /// visibility timeout, and which wins against an expired-but-unpruned row
    /// (024_kv.sql:1010-1015).
    pub fn put_if_absent_ttl(
        ns: &str,
        key: &str,
        value: serde_json::Value,
        ttl_seconds: u64,
    ) -> KvOp {
        KvOp::put_ttl(ns, key, value, ttl_seconds, Some(0))
    }

    /// A conditional write that never expires and answers a verdict:
    /// SetQueueAttributes against the version it read.
    pub fn put_expecting(ns: &str, key: &str, value: serde_json::Value, expect: i64) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: Some(expect),
            required: false,
        }
    }

    /// A FENCED write: conditional on `expect`, and `required` so that losing it
    /// aborts the whole call rather than answering `applied:false` beside writes
    /// that landed anyway. The gate on a bundle that must not half-apply.
    pub fn fence(ns: &str, key: &str, value: serde_json::Value, expect: i64) -> KvOp {
        KvOp::Put {
            ns: ns.to_string(),
            key: key.to_string(),
            value,
            forever: true,
            ttl_seconds: None,
            expect: Some(expect),
            required: true,
        }
    }

    pub fn delete(ns: &str, key: &str, expect: Option<i64>) -> KvOp {
        KvOp::Delete {
            ns: ns.to_string(),
            key: key.to_string(),
            expect,
        }
    }

    /// A counter with a ceiling, created with a TTL — a rate cap over a window.
    pub fn incr(ns: &str, key: &str, delta: i64, max: Option<i64>, ttl_seconds: u64) -> KvOp {
        KvOp::Incr {
            ns: ns.to_string(),
            key: key.to_string(),
            delta,
            min: None,
            max,
            forever: false,
            ttl_seconds: Some(ttl_seconds),
        }
    }

    /// What this operation costs against [`MAX_KV_KEYS_PER_CALL`], counted the
    /// way the broker counts it (server/src/handlers/kv.rs).
    pub fn keys(&self) -> usize {
        match self {
            KvOp::Get { .. } | KvOp::Put { .. } | KvOp::Delete { .. } | KvOp::Incr { .. } => 1,
            KvOp::GetMany { keys, .. } => keys.len(),
            KvOp::GetPrefix { limit, .. } => (*limit).clamp(1, MAX_KV_PREFIX_LIMIT) as usize,
        }
    }
}

/// One row of a read.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KvRow {
    pub key: String,
    /// `'null'::jsonb` is a legal stored value, so this is `Value::Null` both
    /// for a key holding null and for an answer that carried no value at all.
    #[serde(default)]
    pub value: serde_json::Value,
    /// Opaque, unique, from a sequence — never `version + 1`, so there is no ABA
    /// (024_kv.sql:133-140). Compared for EQUALITY only, never ordered and never
    /// arithmetic. 0 is "not there", which is also how an expired row reads.
    #[serde(default)]
    pub version: i64,
}

/// What one operation answered.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct KvAnswer {
    #[serde(default)]
    pub index: usize,
    /// `get`, `put`, `getMany`, `getPrefix`, `delete`, `incr` — as the stored
    /// procedure labelled it, which is authoritative over what was asked (it is
    /// where `putIfAbsent` becomes `put`).
    #[serde(default)]
    pub op: String,
    /// The key a single-key operation addressed.
    #[serde(default)]
    pub key: String,
    /// `get` only, and never collapsed into the value: `{found:true,value:null}`
    /// and `{found:false}` are different things.
    #[serde(default)]
    pub found: bool,
    /// Writes only. A `put` with no precondition is always `Some(true)`.
    #[serde(default)]
    pub applied: Option<bool>,
    /// The version the key holds AFTER the operation when it applied, and the
    /// WINNER's when it did not.
    #[serde(default)]
    pub version: i64,
    /// Writes only, and only when `applied` is false: `version`, `absent`,
    /// `exists`, `limit` or `type`.
    #[serde(default)]
    pub reason: Option<String>,
    /// The value under the key: the winner's when the operation did not apply,
    /// the new one when it did, the stored one for a `get`.
    #[serde(default)]
    pub value: serde_json::Value,
    #[serde(default)]
    pub rows: Vec<KvRow>,
    /// `getMany` only: the keys that are not there.
    #[serde(default)]
    pub missing: Vec<String>,
    /// The read did not return everything it matched — either the page limit or
    /// the 4 MiB call budget cut it. Keys lost to the budget are in NEITHER
    /// `rows` nor `missing`, which is why this flag can never be ignored.
    #[serde(default)]
    pub truncated: bool,
    /// `getPrefix` only: the cursor to continue from, set when `truncated`.
    #[serde(rename = "nextAfter", default)]
    pub next_after: Option<String>,
}

impl KvAnswer {
    /// Whether a conditional write landed. An unconditional one is always true.
    pub fn applied(&self) -> bool {
        self.applied.unwrap_or(true)
    }
}

// ---------------------------------------------------------------------- timers

/// One schedule, in the shape `POST /api/v1/timers` takes it
/// (crates/queen-protocol/src/timers.rs, `TimerOperation`).
///
/// This is per-message `DelaySeconds`: `timer_key` is the MessageId, the payload
/// is the base64 of the envelope, and the 90-day horizon dwarfs SQS's 15
/// minutes. A second schedule under the same key OVERWRITES the pending one,
/// which is what makes a retry after a crash safe by construction.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TimerSchedule {
    /// Always `schedule`; the wire's other kinds are not sent from here (a
    /// cancel goes to the DELETE route, which nothing may block —
    /// server/src/handlers/timers.rs §9.6).
    pub op: &'static str,
    pub queue: String,
    #[serde(rename = "timerKey")]
    pub timer_key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub partition: Option<String>,
    /// MILLISECONDS from now, and relative by construction: one clock,
    /// Postgres's, and no inter-broker skew can enter anywhere. A delay in the
    /// past is LEGAL and fires on the first cycle.
    #[serde(rename = "delayMs")]
    pub delay_ms: i64,
    /// The transaction id the delivered message will carry. Mandatory on a
    /// schedule; it is also what answers "did it already fire?" without a second
    /// API call.
    pub txn: String,
    /// The message body, base64.
    pub payload: String,
}

impl TimerSchedule {
    pub fn new(
        queue: &str,
        timer_key: &str,
        partition: &str,
        delay_ms: i64,
        txn: &str,
        payload_b64: &str,
    ) -> TimerSchedule {
        TimerSchedule {
            op: "schedule",
            queue: queue.to_string(),
            timer_key: timer_key.to_string(),
            partition: Some(partition.to_string()),
            delay_ms,
            txn: txn.to_string(),
            payload: payload_b64.to_string(),
        }
    }
}

/// The closed taxonomy of a timer verdict. `absent` and `too_late` are verdicts
/// and not successes even though both answer HTTP 200 — and `absent` means "no
/// longer pending", which MAY MEAN ALREADY DELIVERED: there is no tombstone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimerStatus {
    Scheduled,
    Rescheduled,
    Cancelled,
    Absent,
    TooLate,
    /// A status this client has not been taught, kept rather than refused so a
    /// broker addition is a log line and not a parse failure.
    #[serde(other)]
    Unknown,
}

/// One element of a timer `results` array, index-aligned to its operation.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TimerResult {
    #[serde(default)]
    pub ok: bool,
    pub status: TimerStatus,
    #[serde(default)]
    pub queue: String,
    #[serde(rename = "timerKey", default)]
    pub timer_key: String,
    /// Echoed on `absent` too, which is what makes the "already delivered?"
    /// check possible without a second API.
    #[serde(default)]
    pub txn: Option<String>,
    #[serde(rename = "messageId", default)]
    pub message_id: Option<String>,
    #[serde(rename = "deliverAt", default)]
    pub deliver_at: Option<String>,
}

/// One row of `GET /api/v1/timers/:queue` — no payload; that is what peek is
/// for. `ApproximateNumberOfMessagesDelayed` is the COUNT mode, not a walk of
/// these.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TimerRow {
    #[serde(default)]
    pub queue: String,
    #[serde(rename = "timerKey", default)]
    pub timer_key: String,
    #[serde(default)]
    pub partition: Option<String>,
    #[serde(rename = "deliverAt", default)]
    pub deliver_at: Option<String>,
    #[serde(default)]
    pub txn: Option<String>,
    #[serde(rename = "messageId", default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub attempts: i64,
}

/// A keyset page of pending timers. `after` is EXCLUSIVE and stable
/// (`timer_key` carries `COLLATE "C"`); `limit` is CLAMPED by the SP and never
/// rejected, with `truncated` telling the truth.
#[derive(Debug, Clone, Default, PartialEq, Deserialize)]
pub struct TimerPage {
    #[serde(default)]
    pub rows: Vec<TimerRow>,
    #[serde(default)]
    pub truncated: bool,
    #[serde(rename = "nextAfter", default)]
    pub next_after: Option<String>,
}

// ----------------------------------------------------------------- transaction

/// One ack inside a transaction bundle. Same fields as [`AckItem`] plus the
/// group, because the wire's ack operation carries its own `consumerGroup`
/// (server/src/handlers/data.rs, `txn_add_ack`) rather than inheriting a
/// top-level one.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TxnAck {
    #[serde(rename = "transactionId")]
    pub transaction_id: String,
    #[serde(rename = "partitionId")]
    pub partition_id: String,
    pub status: AckStatus,
    #[serde(rename = "consumerGroup", skip_serializing_if = "Option::is_none")]
    pub consumer_group: Option<String>,
    #[serde(rename = "leaseId", skip_serializing_if = "Option::is_none")]
    pub lease_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

impl TxnAck {
    /// The ack half of a redrive move: complete the original under its own
    /// lease, in the same transaction as the push that copies it to the DLQ.
    pub fn completed(transaction_id: &str, partition_id: &str, lease_id: &str) -> TxnAck {
        TxnAck {
            transaction_id: transaction_id.to_string(),
            partition_id: partition_id.to_string(),
            status: AckStatus::Completed,
            consumer_group: None,
            lease_id: Some(lease_id.to_string()),
            error: None,
        }
    }
}

/// One push echo of a committed bundle. It carries no offset, by construction:
/// the wire builds the echoes without the `baseOffset` the stored procedure
/// returned (server/src/handlers/data.rs), so a caller that needs a
/// SequenceNumber must use `POST /api/v1/push` instead.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TxnPushEcho {
    #[serde(default)]
    pub index: usize,
    #[serde(rename = "transactionId", default)]
    pub transaction_id: String,
    #[serde(rename = "messageId", default)]
    pub message_id: String,
    #[serde(rename = "queueName", default)]
    pub queue: String,
    #[serde(default)]
    pub duplicate: bool,
}

/// One ack echo of a committed bundle.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TxnAckEcho {
    #[serde(default)]
    pub index: usize,
    #[serde(rename = "transactionId", default)]
    pub transaction_id: String,
    #[serde(default)]
    pub dlq: bool,
}

/// What one committed bundle answered, demuxed back into the three arrays the
/// caller sent.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct TxnOutcome {
    pub pushes: Vec<TxnPushEcho>,
    pub acks: Vec<TxnAckEcho>,
    pub kv: Vec<KvAnswer>,
}

// ------------------------------------------------------------------- the trait

/// The calls the facade makes to Queen. A trait, and not just the concrete
/// client below, so every policy built on it — the registry, the receive loop,
/// the delete-set, the redrive move — can be tested against a double that
/// behaves like a broker rather than against one.
pub trait QueenApi: Send + Sync + 'static {
    /// `GET /api/v1/resources/queues`, in the queue list's own order.
    fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>>;

    /// `POST /api/v1/configure` — create or reconfigure, with an options bag.
    ///
    /// The SP is an UPSERT that rewrites every config column it was not given to
    /// ITS defaults, so this may only be called with the FULL bag the facade
    /// wants the queue to have: CreateQueue builds it from the SQS attributes,
    /// and SetQueueAttributes rebuilds it from the registry record plus the
    /// change. There is no create-if-absent on the broker to ask for instead.
    fn configure_queue<'a>(
        &'a self,
        name: &'a str,
        options: &'a serde_json::Value,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>>;

    /// `DELETE /api/v1/resources/queues/:queue`.
    ///
    /// The route is idempotent by design and answers 200 either way, so a
    /// missing queue is not an HTTP error to be mapped: it is
    /// [`Deleted::existed`] being false, which is what becomes
    /// `QueueDoesNotExist`.
    fn delete_queue<'a>(
        &'a self,
        name: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Deleted>>;

    /// `GET /api/v1/resources/queues/:queue/depth` — the attribute pair every
    /// autoscaler reads. A queue that is not there answers 404, which is this
    /// facade's existence probe.
    fn queue_depth<'a>(
        &'a self,
        queue: &'a str,
        group: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Depth>>;

    /// `POST /api/v1/push` — one write per item, answered one result per item,
    /// aligned to `items` by the `index` each result carries.
    fn push<'a>(
        &'a self,
        items: &'a [PushItem],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Pushed>>>;

    /// `GET /api/v1/pop/queue/:queue` — claim up to `opts.partitions` lanes and
    /// take up to `opts.batch` messages from each, under a lease.
    fn pop_queue<'a>(
        &'a self,
        queue: &'a str,
        opts: &'a PopOptions,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Popped>>;

    /// `GET /api/v1/pop/queue/:queue/partition/:partition` — the same, pinned to
    /// one lane. This is how a FIFO `MessageGroupId` is consumed, and how the
    /// redrive loop walks a DLQ deterministically.
    fn pop_partition<'a>(
        &'a self,
        queue: &'a str,
        partition: &'a str,
        opts: &'a PopOptions,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Popped>>;

    /// `POST /api/v1/ack` — one ack, one answer.
    fn ack<'a>(
        &'a self,
        ack: &'a AckItem,
        group: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Acked>>;

    /// `POST /api/v1/ack/batch` — `DeleteMessageBatch` and
    /// `ChangeMessageVisibilityBatch`, answered one entry per item in request
    /// order. The per-entry answers ARE the `BatchResultErrorEntry` list.
    fn ack_batch<'a>(
        &'a self,
        acks: &'a [AckItem],
        group: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Acked>>>;

    /// `POST /api/v1/lease/:leaseId/extend` — `ChangeMessageVisibility`. At
    /// claim width 1 that lease holds exactly one message, which is what makes
    /// the mapping exact rather than approximate.
    fn lease_extend<'a>(
        &'a self,
        lease_id: &'a str,
        seconds: i64,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<LeaseExtended>>;

    /// `POST /api/v1/kv` — one answer per operation, aligned by the `index` each
    /// answer carries. `ops` must already be within [`MAX_KV_OPS_PER_CALL`] and
    /// [`MAX_KV_KEYS_PER_CALL`], because exceeding either fails the whole batch.
    fn kv<'a>(
        &'a self,
        ops: &'a [KvOp],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<KvAnswer>>>;

    /// `POST /api/v1/timers` — schedules only, one result per operation in
    /// request order.
    fn timers_schedule<'a>(
        &'a self,
        ops: &'a [TimerSchedule],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<TimerResult>>>;

    /// `GET /api/v1/timers/:queue` — a keyset page.
    fn timers_list<'a>(
        &'a self,
        queue: &'a str,
        after: Option<&'a str>,
        limit: i64,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<TimerPage>>;

    /// `GET /api/v1/timers/:queue?mode=count&prefix=…` — an exact,
    /// index-driven count. `ApproximateNumberOfMessagesDelayed` reads this and
    /// never the list.
    fn timers_count<'a>(
        &'a self,
        queue: &'a str,
        prefix: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<i64>>;

    /// `DELETE /api/v1/timers/:queue/*timerKey` — the route that is guaranteed
    /// never to be blocked, which is why a cancel goes here and not into the
    /// batch (§9.6: the fire never switches itself off, so a caller that could
    /// not cancel would keep producing messages it cannot stop).
    fn timers_cancel<'a>(
        &'a self,
        queue: &'a str,
        timer_key: &'a str,
        txn: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<TimerResult>>;

    /// `POST /api/v1/transaction` — pushes, acks and KV writes in ONE Postgres
    /// transaction.
    ///
    /// The guarantee is the stored procedure's own (005_log_ack.sql,
    /// `log_transaction_wire_v1`): *"All-or-nothing by construction: one call =
    /// one transaction, every failure path RAISEs, so a duplicate push or a
    /// rejected ack rolls back every other operation in the batch"*, and *"a KV
    /// precondition marked `required:true` raises 23514 out of `kv_apply_v1` and
    /// rolls the bundle back the same way"*.
    ///
    /// Two things in this facade depend on that sentence and on nothing else:
    /// the REDRIVE move (push-to-DLQ + ack-original, so a message can be neither
    /// lost nor duplicated by a facade that dies between the two) and the SNS
    /// fan-out (one push per matched subscription, atomic — stronger than SNS
    /// itself promises).
    ///
    /// `kv` must already be within [`WIRE_KV_MAX_OPS`] and [`WIRE_KV_MAX_KEYS`],
    /// which are tighter than the `/api/v1/kv` ceilings.
    fn transaction<'a>(
        &'a self,
        pushes: &'a [PushItem],
        acks: &'a [TxnAck],
        kv: &'a [KvOp],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<TxnOutcome>>;

    /// `GET /auth/me` — the identity Queen attributes to this credential.
    ///
    /// Defaulted to "names none" because that is the honest answer for an
    /// implementation with no identity surface, and it keeps a double that is
    /// testing something else from having to script one.
    fn identity<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Option<String>>> {
        let _ = token;
        Box::pin(async { Ok(None) })
    }

    /// The same Queen, reached with `host` as the HTTP `Host` header of every
    /// call — the Cloud shared-host fit. The first DNS label of `Host` names the
    /// cluster unless the name is in `QUEEN_PROXY_SHARED_HOSTS`, in which case
    /// the credential does (proxy/src/acting.rs).
    ///
    /// `None` means "this implementation has no HTTP to stamp", and the caller
    /// keeps the client it already has.
    ///
    /// NOTHING IN THIS CRATE CALLS IT YET, and that is a milestone boundary
    /// rather than an oversight: every request reaches Queen with the URL's own
    /// `Host`, which is correct for the OSS deployment (one facade, one broker)
    /// and is the whole of what M0–M3 need. Cloud tenancy is M5 and is not a
    /// straight lift of queen-kafka's line, because PLAN_QUEEN_SQS.md carries
    /// the tenant in the ACCOUNT SEGMENT of the queue URL rather than in the
    /// hostname, and because the [`Catalog`] and the registry cache would each
    /// have to become per-tenant instances — a process-wide cache keyed by
    /// nothing is exactly how one tenant would read another's queue list. Until
    /// that lands, this method is the shape the answer will take and not a
    /// switch anybody has thrown. The half that IS already safe: no client
    /// header is ever forwarded to Queen, so an inbound `x-queen-tenant` cannot
    /// travel through this facade.
    fn with_host(&self, host: &str) -> Option<Arc<dyn QueenApi>> {
        let _ = host;
        None
    }
}

// --------------------------------------------------------------------- client

/// Total budget for one call that is not a long poll. The HTTP request that
/// triggered it is held open until it answers, and an unbounded wait here is an
/// SDK's own timeout firing with no error the facade ever wrote.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);
const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);

/// The real client. One `reqwest::Client` for the process: it owns the
/// connection pool, and every SQS request is one to three calls on it.
pub struct HttpQueen {
    /// `QUEEN_URL`, without a trailing slash (see [`normalize_base_url`]).
    base: String,
    /// The `Host` header every call sends, when it is not the one the URL
    /// implies. See [`HttpQueen::with_host`].
    host: Option<String>,
    http: reqwest::Client,
}

impl HttpQueen {
    pub fn new(base_url: &str) -> std::result::Result<HttpQueen, String> {
        let base = normalize_base_url(base_url)?;
        let http = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .connect_timeout(CONNECT_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTP client for QUEEN_URL={base}: {e}"))?;
        Ok(HttpQueen {
            base,
            host: None,
            http,
        })
    }

    fn request(
        &self,
        method: reqwest::Method,
        path: &str,
        token: Option<&str>,
    ) -> reqwest::RequestBuilder {
        let req = self
            .http
            .request(method, format!("{}{path}", self.base))
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        // Set explicitly, which is what stops hyper filling it in from the URL's
        // authority — it only adds a `Host` that is not already there.
        let req = match &self.host {
            Some(h) => req.header(reqwest::header::HOST, h.as_str()),
            None => req,
        };
        // Bearer, matching what the broker's auth layer extracts
        // (server/src/auth.rs::extract_bearer). Per call, never on the client:
        // see the module header.
        match token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    async fn send(req: reqwest::RequestBuilder) -> Result<String> {
        let resp = req
            .send()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        let status = resp.status();
        let retry_after_ms = retry_after_ms(resp.headers());
        let body = resp
            .text()
            .await
            .map_err(|e| Error::Transport(e.to_string()))?;
        if !status.is_success() {
            return Err(Error::Status {
                code: status.as_u16(),
                body,
                retry_after_ms,
            });
        }
        Ok(body)
    }
}

/// The `Retry-After` of one answer, in milliseconds.
///
/// Only the `delta-seconds` form is read, because that is the only form the
/// proxy writes (proxy/src/errors.rs `err_429` formats an integer). RFC 9110
/// also allows an HTTP-date, and a date is deliberately answered `None` rather
/// than parsed: this value reaches a client as a backoff it may SLEEP for, and a
/// misread date is a consumer parked for hours.
fn retry_after_ms(headers: &reqwest::header::HeaderMap) -> Option<i64> {
    let seconds: i64 = headers
        .get(reqwest::header::RETRY_AFTER)?
        .to_str()
        .ok()?
        .trim()
        .parse()
        .ok()?;
    // A negative or absurd value is a broker saying something this cannot use.
    seconds.checked_mul(1_000).filter(|ms| *ms >= 0)
}

/// One path SEGMENT, percent-encoded.
///
/// The unreserved set of RFC 3986 plus nothing: every other byte is escaped, so
/// a `/`, a `?`, a `#` or a space in a name cannot change which resource the
/// request addresses. Load-bearing here in a way it is not in queen-kafka: an
/// SQS queue name is `[A-Za-z0-9_-]{1,80}` (plus `.fifo`) but a TIMER KEY is a
/// MessageId this facade mints and a MESSAGE GROUP ID is 128 characters the
/// client chooses, and both travel as path segments.
pub fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The list body. Only the fields this facade reads are named; the broker adds
/// to this response for the dashboard and none of that may turn into a parse
/// failure here.
#[derive(Debug, Deserialize)]
struct QueueListBody {
    #[serde(default)]
    queues: Vec<Queue>,
}

/// The push request body. Borrowed, so a `SendMessageBatch` is serialized
/// straight out of the items the action already built.
#[derive(Serialize)]
struct PushBody<'a> {
    items: &'a [PushItem],
}

/// One result as `render_push_results` writes it, with the `index` that says
/// which item it belongs to.
#[derive(Deserialize)]
struct PushResultItem {
    index: usize,
    #[serde(flatten)]
    pushed: Pushed,
}

/// The KV request body. `{"operations":[…]}` and not the bare array the route
/// also accepts: it is the shape the transaction wire uses, so one shape is
/// learned once.
#[derive(Serialize)]
struct KvBody<'a> {
    operations: &'a [KvOp],
}

#[derive(Deserialize)]
struct KvResponseBody {
    #[serde(default)]
    results: Vec<KvAnswer>,
    /// Present, and false, on the one 200 that is not an answer: a `required`
    /// precondition that lost. See [`Error::Precondition`].
    #[serde(default = "yes")]
    ok: bool,
    #[serde(default)]
    reason: String,
    #[serde(rename = "failedIndex", default)]
    failed_index: usize,
    #[serde(rename = "kvReason", default)]
    kv_reason: Option<String>,
    #[serde(default)]
    version: i64,
    #[serde(default)]
    value: serde_json::Value,
}

/// A body with no `ok` at all is a successful one — `ok` is written only by
/// `precondition_200` (server/src/handlers/kv.rs) and by the error shapes.
fn yes() -> bool {
    true
}

/// The ack batch body.
#[derive(Serialize)]
struct AckBatchBody<'a> {
    acknowledgments: &'a [AckItem],
    #[serde(rename = "consumerGroup", skip_serializing_if = "Option::is_none")]
    consumer_group: Option<&'a str>,
}

/// The single-ack body: the item's own fields plus the group, flattened, which
/// is what `handle_ack` reads.
#[derive(Serialize)]
struct AckSingleBody<'a> {
    #[serde(flatten)]
    ack: &'a AckItem,
    #[serde(rename = "consumerGroup", skip_serializing_if = "Option::is_none")]
    consumer_group: Option<&'a str>,
}

#[derive(Serialize)]
struct RenewBody {
    seconds: i64,
}

#[derive(Serialize)]
struct TimerBody<'a> {
    operations: &'a [TimerSchedule],
}

#[derive(Deserialize)]
struct TimerResponseBody {
    #[serde(default)]
    results: Vec<TimerResult>,
}

#[derive(Deserialize)]
struct TimerCountBody {
    #[serde(default)]
    count: i64,
}

/// The transaction wire's body.
///
/// `kv` is a TOP-LEVEL rider beside `operations` and never inside it, and that
/// is a stated rule of the route rather than a style: two Go struct fields
/// carrying the same JSON key at one level are silently dropped by
/// `encoding/json`, so a `kv` leg inside an operation would let a bundle commit
/// with the gate it existed for simply absent (server/src/handlers/data.rs).
#[derive(Serialize)]
struct TransactionBody<'a> {
    operations: &'a [TxnOperation<'a>],
    kv: &'a [KvOp],
}

/// One entry of the flat `operations` array, demuxed by `type` on the broker.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum TxnOperation<'a> {
    Push {
        items: &'a [PushItem],
    },
    #[serde(untagged)]
    Ack(TaggedAck<'a>),
}

/// An ack operation carries its fields FLAT beside `type` — there is no nested
/// object — so it cannot ride the same `tag` shape as the push above.
#[derive(Serialize)]
struct TaggedAck<'a> {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(flatten)]
    ack: &'a TxnAck,
}

/// One result of a transaction, as the wire scatters it into the flat space.
///
/// `opIndex` is present only on a RIDER result and is what aligns a KV answer to
/// the operation that asked for it (server/src/handlers/data.rs,
/// `txn_scatter_rider`). A push or ack echo carries `index` alone, and `type`
/// says which of the two it is.
#[derive(Deserialize)]
struct TxnResultEntry {
    #[serde(default)]
    index: usize,
    #[serde(rename = "opIndex", default)]
    op_index: Option<usize>,
    #[serde(rename = "type", default)]
    kind: String,
    #[serde(default = "yes")]
    success: bool,
    #[serde(flatten)]
    kv: KvAnswer,
    #[serde(flatten)]
    push: TxnPushEcho,
    #[serde(default)]
    dlq: bool,
}

#[derive(Deserialize)]
struct TransactionResponseBody {
    #[serde(default = "yes")]
    success: bool,
    #[serde(default)]
    reason: String,
    #[serde(default)]
    error: String,
    /// The loser's whole verdict, when the failure is a lost precondition.
    /// `failedIndex` is in the FLAT operation space and is translated back here,
    /// because a caller thinks in the array it sent.
    #[serde(rename = "failedIndex", default)]
    failed_index: Option<usize>,
    #[serde(rename = "kvReason", default)]
    kv_reason: Option<String>,
    #[serde(default)]
    version: Option<i64>,
    #[serde(default)]
    value: serde_json::Value,
    #[serde(default)]
    results: Vec<TxnResultEntry>,
}

impl QueenApi for HttpQueen {
    fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
        Box::pin(async move {
            // Not `?stats=cached`: that serves `queen.stats.child_count` as of
            // the last stats refresh, which for a queue created seconds ago is a
            // partition count from before the queue existed.
            let body =
                Self::send(self.request(reqwest::Method::GET, "/api/v1/resources/queues", token))
                    .await?;
            let parsed: QueueListBody =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            Ok(parsed.queues)
        })
    }

    fn configure_queue<'a>(
        &'a self,
        name: &'a str,
        options: &'a serde_json::Value,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // The bag is MERGED into `{"queue": name}` rather than nested: the
            // route reads the options from the top level of the body
            // (server/src/handlers/queues.rs → `configure_queue_v1(name,
            // options)`), and `queue` is the one key that is not one of them.
            let mut payload = serde_json::Map::new();
            if let Some(bag) = options.as_object() {
                payload.extend(bag.iter().map(|(k, v)| (k.clone(), v.clone())));
            }
            payload.insert("queue".into(), serde_json::json!(name));
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/configure", token)
                    .body(serde_json::Value::Object(payload).to_string()),
            )
            .await?;
            // `handle_configure` surfaces a stored-procedure failure as a
            // non-2xx, but the SP can also echo `{"error":…}` inside a 200.
            // Check for it rather than reporting a queue we did not create.
            let parsed: serde_json::Value =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            if let Some(e) = parsed.get("error").filter(|e| !e.is_null()) {
                return Err(Error::Body(format!("configure answered {e}")));
            }
            if parsed.get("configured").and_then(|c| c.as_bool()) != Some(true) {
                return Err(Error::Body(format!(
                    "configure did not confirm: {}",
                    Snippet(&body)
                )));
            }
            Ok(())
        })
    }

    fn delete_queue<'a>(
        &'a self,
        name: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Deleted>> {
        Box::pin(async move {
            let path = format!("/api/v1/resources/queues/{}", encode_segment(name));
            let body = Self::send(self.request(reqwest::Method::DELETE, &path, token)).await?;
            let parsed: DeleteQueueBody =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            if let Some(e) = parsed.error.filter(|e| !e.is_null()) {
                return Err(Error::Body(format!("delete answered {e}")));
            }
            Ok(Deleted {
                existed: parsed.deleted,
            })
        })
    }

    fn queue_depth<'a>(
        &'a self,
        queue: &'a str,
        group: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Depth>> {
        Box::pin(async move {
            let mut path = format!("/api/v1/resources/queues/{}/depth", encode_segment(queue));
            if let Some(g) = group {
                path.push_str(&format!("?group={}", encode_segment(g)));
            }
            let body = Self::send(self.request(reqwest::Method::GET, &path, token)).await?;
            serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))
        })
    }

    fn push<'a>(
        &'a self,
        items: &'a [PushItem],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Pushed>>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&PushBody { items })
                .map_err(|e| Error::Body(format!("cannot serialize the push body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/push", token)
                    .body(payload),
            )
            .await?;
            align_push_results(&body, items.len())
        })
    }

    fn pop_queue<'a>(
        &'a self,
        queue: &'a str,
        opts: &'a PopOptions,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Popped>> {
        Box::pin(async move {
            let path = format!(
                "/api/v1/pop/queue/{}{}",
                encode_segment(queue),
                pop_query(opts)
            );
            let body = Self::send(
                self.request(reqwest::Method::GET, &path, token)
                    .timeout(pop_timeout(opts)),
            )
            .await?;
            parse_popped(&body, queue)
        })
    }

    fn pop_partition<'a>(
        &'a self,
        queue: &'a str,
        partition: &'a str,
        opts: &'a PopOptions,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Popped>> {
        Box::pin(async move {
            let path = format!(
                "/api/v1/pop/queue/{}/partition/{}{}",
                encode_segment(queue),
                encode_segment(partition),
                pop_query(opts)
            );
            let body = Self::send(
                self.request(reqwest::Method::GET, &path, token)
                    .timeout(pop_timeout(opts)),
            )
            .await?;
            parse_popped(&body, queue)
        })
    }

    fn ack<'a>(
        &'a self,
        ack: &'a AckItem,
        group: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Acked>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&AckSingleBody {
                ack,
                consumer_group: group,
            })
            .map_err(|e| Error::Body(format!("cannot serialize the ack body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/ack", token)
                    .body(payload),
            )
            .await?;
            let mut acked = align_ack_results(&body, 1)?;
            Ok(acked.remove(0))
        })
    }

    fn ack_batch<'a>(
        &'a self,
        acks: &'a [AckItem],
        group: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<Acked>>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&AckBatchBody {
                acknowledgments: acks,
                consumer_group: group,
            })
            .map_err(|e| Error::Body(format!("cannot serialize the ack batch body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/ack/batch", token)
                    .body(payload),
            )
            .await?;
            align_ack_results(&body, acks.len())
        })
    }

    fn lease_extend<'a>(
        &'a self,
        lease_id: &'a str,
        seconds: i64,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<LeaseExtended>> {
        Box::pin(async move {
            let path = format!("/api/v1/lease/{}/extend", encode_segment(lease_id));
            let payload = serde_json::to_string(&RenewBody { seconds })
                .map_err(|e| Error::Body(format!("cannot serialize the renew body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, &path, token)
                    .body(payload),
            )
            .await?;
            serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))
        })
    }

    fn kv<'a>(
        &'a self,
        ops: &'a [KvOp],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<KvAnswer>>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&KvBody { operations: ops })
                .map_err(|e| Error::Body(format!("cannot serialize the kv body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/kv", token)
                    .body(payload),
            )
            .await?;
            align_kv_results(&body, ops.len())
        })
    }

    fn timers_schedule<'a>(
        &'a self,
        ops: &'a [TimerSchedule],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<Vec<TimerResult>>> {
        Box::pin(async move {
            let payload = serde_json::to_string(&TimerBody { operations: ops })
                .map_err(|e| Error::Body(format!("cannot serialize the timers body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/timers", token)
                    .body(payload),
            )
            .await?;
            let parsed: TimerResponseBody =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            // One result per operation, in request order, is the route's own
            // contract; short of that the caller cannot tell which schedule
            // landed, and a delayed message nobody can account for is worse than
            // a refused send.
            if parsed.results.len() != ops.len() {
                return Err(Error::Body(format!(
                    "timers answered {} results for {} operations",
                    parsed.results.len(),
                    ops.len()
                )));
            }
            Ok(parsed.results)
        })
    }

    fn timers_list<'a>(
        &'a self,
        queue: &'a str,
        after: Option<&'a str>,
        limit: i64,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<TimerPage>> {
        Box::pin(async move {
            let mut path = format!("/api/v1/timers/{}?limit={limit}", encode_segment(queue));
            if let Some(a) = after {
                path.push_str(&format!("&after={}", encode_segment(a)));
            }
            let body = Self::send(self.request(reqwest::Method::GET, &path, token)).await?;
            serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))
        })
    }

    fn timers_count<'a>(
        &'a self,
        queue: &'a str,
        prefix: &'a str,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<i64>> {
        Box::pin(async move {
            let path = format!(
                "/api/v1/timers/{}?mode=count&prefix={}",
                encode_segment(queue),
                encode_segment(prefix)
            );
            let body = Self::send(self.request(reqwest::Method::GET, &path, token)).await?;
            let parsed: TimerCountBody =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            Ok(parsed.count)
        })
    }

    fn timers_cancel<'a>(
        &'a self,
        queue: &'a str,
        timer_key: &'a str,
        txn: Option<&'a str>,
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<TimerResult>> {
        Box::pin(async move {
            let mut path = format!(
                "/api/v1/timers/{}/{}",
                encode_segment(queue),
                encode_segment(timer_key)
            );
            if let Some(t) = txn.filter(|t| !t.is_empty()) {
                path.push_str(&format!("?txn={}", encode_segment(t)));
            }
            let body = Self::send(self.request(reqwest::Method::DELETE, &path, token)).await?;
            serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))
        })
    }

    fn transaction<'a>(
        &'a self,
        pushes: &'a [PushItem],
        acks: &'a [TxnAck],
        kv: &'a [KvOp],
        token: Option<&'a str>,
    ) -> BoxFuture<'a, Result<TxnOutcome>> {
        Box::pin(async move {
            // A bundle with no records and no acks carries NO `operations` entry
            // at all, and that is a route the broker has and takes deliberately:
            // an empty `operations` array beside a KV rider goes straight to the
            // KV procedure instead of the wire, which is a short transaction
            // rather than one holding the outermost lock space for the whole
            // bundle (server/src/handlers/data.rs, `txn_kv_only`).
            let mut operations: Vec<TxnOperation<'_>> = Vec::new();
            if !pushes.is_empty() {
                operations.push(TxnOperation::Push { items: pushes });
            }
            for ack in acks {
                operations.push(TxnOperation::Ack(TaggedAck { kind: "ack", ack }));
            }
            let payload = serde_json::to_string(&TransactionBody {
                operations: &operations,
                kv,
            })
            .map_err(|e| Error::Body(format!("cannot serialize the transaction body: {e}")))?;
            let body = Self::send(
                self.request(reqwest::Method::POST, "/api/v1/transaction", token)
                    .body(payload),
            )
            .await?;
            align_transaction_results(&body, pushes.len(), acks.len(), kv.len())
        })
    }

    /// `GET /auth/me`, with this call's own bearer. The path and the payload
    /// shape are the same on both surfaces (server/src/handlers/standalone.rs is
    /// written to be "field-for-field the proxy's shape"), which is why one call
    /// covers broker-direct and Cloud.
    fn identity<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Option<String>>> {
        Box::pin(async move {
            let body = Self::send(self.request(reqwest::Method::GET, "/auth/me", token)).await?;
            let parsed: serde_json::Value =
                serde_json::from_str(&body).map_err(|e| Error::Body(e.to_string()))?;
            Ok(tenant_of(&parsed))
        })
    }

    /// A second handle on the SAME `reqwest::Client`, differing only in the
    /// `Host` header it writes. A `reqwest::Client` clone shares the connection
    /// pool, the DNS cache and the TLS session cache with the original, so a
    /// hundred hostnames are still one pool.
    fn with_host(&self, host: &str) -> Option<Arc<dyn QueenApi>> {
        Some(Arc::new(HttpQueen {
            base: self.base.clone(),
            host: Some(host.to_string()),
            http: self.http.clone(),
        }))
    }
}

/// The cluster `/auth/me` names for a credential, when it names one.
///
/// Both surfaces answer an object; the proxy names the cluster it will act on
/// and the standalone broker names none. Read leniently — a shape that carries
/// no name is `None` and never a parse failure — because this value only ever
/// KEYS a cache, and a wrong key is one extra admin call while a hard failure is
/// a refused connection.
fn tenant_of(body: &serde_json::Value) -> Option<String> {
    for key in ["cluster", "clusterName", "tenant", "tenantId"] {
        if let Some(v) = body.get(key).and_then(|v| v.as_str()) {
            if !v.is_empty() {
                return Some(v.to_string());
            }
        }
    }
    None
}

/// One pop's answer, INCLUDING the empty one — which has no body at all.
///
/// The broker answers an empty pop `204 No Content` and then drops the body on
/// the way out (server/src/handlers/mod.rs: "drop the body, keep the status"),
/// because a bodied 204 poisons some HTTP clients' connections. Pop maintenance
/// answers the same 204. So the commonest answer this facade ever receives is
/// zero bytes, and parsing it as JSON turns every empty `ReceiveMessage` — and
/// every receive of more messages than the queue has lanes with something on
/// them — into an `InternalFailure` 500.
///
/// An empty body is therefore an empty claim: no partition, no lease, no
/// messages. Nothing else in this file may treat a bodiless success as JSON.
fn parse_popped(body: &str, queue: &str) -> Result<Popped> {
    if body.trim().is_empty() {
        return Ok(Popped {
            queue: queue.to_string(),
            ..Popped::default()
        });
    }
    serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))
}

/// The query string one pop sends. Every parameter is named explicitly, even at
/// its default, because the broker's defaults are not this facade's: absent
/// `batch` MEANS 200 and absent `partitions` MEANS 1 (server/src/handlers/
/// data.rs, `PopParams`), and a receive that silently claimed 200 messages under
/// one lease would be 200 messages invisible to every other consumer for a
/// visibility timeout.
fn pop_query(opts: &PopOptions) -> String {
    let mut q = format!(
        "?batch={}&partitions={}&wait={}",
        opts.batch.clamp(1, MAX_POP_BATCH),
        opts.partitions.max(1),
        opts.wait
    );
    if opts.lease_seconds > 0 {
        q.push_str(&format!("&leaseSeconds={}", opts.lease_seconds));
    }
    if opts.timeout_ms > 0 {
        q.push_str(&format!(
            "&timeout={}",
            opts.timeout_ms.min(MAX_POP_TIMEOUT_MS)
        ));
    }
    if let Some(g) = &opts.consumer_group {
        q.push_str(&format!("&consumerGroup={}", encode_segment(g)));
    }
    q
}

/// The HTTP budget for one pop, which is the only call that does not take the
/// client's default.
///
/// It has to be its own, and the reason is not tuning: a pop with `wait=true` is
/// a LONG POLL, so the broker is EXPECTED to hold the request open for up to
/// `timeout`. Under the client-wide [`REQUEST_TIMEOUT`] a `WaitTimeSeconds=20`
/// receive would be cancelled at ten seconds and reported a transport failure —
/// an error where the correct answer was "no messages", on every poll, for ever.
fn pop_timeout(opts: &PopOptions) -> Duration {
    REQUEST_TIMEOUT + Duration::from_millis(opts.timeout_ms.min(MAX_POP_TIMEOUT_MS))
}

/// Match a push response back to the items that produced it.
///
/// By the explicit `index` each result carries, and a response that does not
/// cover every item exactly once is an error rather than a shifted answer: a
/// `SendMessageBatch` entry answered from the wrong item reports one message's
/// MessageId and MD5 for another's, which is precisely the check boto3 then
/// fails on the client side with no way to say why.
fn align_push_results(body: &str, items: usize) -> Result<Vec<Pushed>> {
    let parsed: Vec<PushResultItem> =
        serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    let mut out: Vec<Option<Pushed>> = (0..items).map(|_| None).collect();
    for r in parsed {
        let slot = out
            .get_mut(r.index)
            .ok_or_else(|| Error::Body(format!("push result {} is out of range", r.index)))?;
        if slot.is_some() {
            return Err(Error::Body(format!(
                "push result {} appears twice",
                r.index
            )));
        }
        *slot = Some(r.pushed);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, r)| r.ok_or_else(|| Error::Body(format!("push answered nothing for item {i}"))))
        .collect()
}

/// Match an ack response back to the acks that produced it.
///
/// Same discipline, and the stake is the one that makes a queue lose data: an
/// ack answer attributed to the wrong entry tells a client its message was
/// deleted when another one was.
fn align_ack_results(body: &str, acks: usize) -> Result<Vec<Acked>> {
    let parsed: Vec<Acked> = serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    let mut out: Vec<Option<Acked>> = (0..acks).map(|_| None).collect();
    for r in parsed {
        let slot = out
            .get_mut(r.index)
            .ok_or_else(|| Error::Body(format!("ack result {} is out of range", r.index)))?;
        if slot.is_some() {
            return Err(Error::Body(format!("ack result {} appears twice", r.index)));
        }
        *slot = Some(r);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, r)| r.ok_or_else(|| Error::Body(format!("ack answered nothing for item {i}"))))
        .collect()
}

/// Match a KV response back to the operations that produced it.
///
/// By the explicit `index` each result carries, because the stored procedure
/// applies write operations in KEY order rather than input order and reports
/// them by input ordinal: position is a property of the answer rather than of
/// the call.
fn align_kv_results(body: &str, ops: usize) -> Result<Vec<KvAnswer>> {
    let parsed: KvResponseBody =
        serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    // The one 200 that carries no results at all. Read BEFORE the alignment,
    // because the alignment would otherwise report it as "kv answered nothing
    // for operation 0" — a body error, which is neither retriable nor true.
    if !parsed.ok {
        return Err(match parsed.reason.as_str() {
            "kv_precondition" => Error::Precondition {
                failed_index: parsed.failed_index,
                reason: parsed.kv_reason.unwrap_or_default(),
                version: parsed.version,
                value: parsed.value,
            },
            other => Error::Body(format!("kv answered ok=false, reason={other}")),
        });
    }
    let mut out: Vec<Option<KvAnswer>> = (0..ops).map(|_| None).collect();
    for r in parsed.results {
        let slot = out
            .get_mut(r.index)
            .ok_or_else(|| Error::Body(format!("kv result {} is out of range", r.index)))?;
        if slot.is_some() {
            return Err(Error::Body(format!("kv result {} appears twice", r.index)));
        }
        *slot = Some(r);
    }
    out.into_iter()
        .enumerate()
        .map(|(i, r)| {
            r.ok_or_else(|| Error::Body(format!("kv answered nothing for operation {i}")))
        })
        .collect()
}

/// Match a transaction response back to the three arrays that produced it.
///
/// Four things happen here and each one is a defect this would otherwise hide:
///
///   * `success: false` is HTTP **200**, deliberately, so that a lost
///     precondition pollutes no retry policy and no error metric.
///     `reason: "kv_precondition"` therefore has to become
///     [`Error::Precondition`] here or a fenced bundle would read as a committed
///     one — and for the redrive move that is a message deleted from its queue
///     with nothing written to the DLQ.
///   * the loser's verdict arrives in the FLAT operation space (`failedIndex =
///     kv_base + the kv ordinal`), so it is translated back into the `kv` array
///     the caller sent.
///   * the flat space is walked by `type`, not by position: a push echo and an
///     ack echo share it, and reading one as the other is how a facade would
///     report "moved" for a message it only acked.
///   * every KV operation must come back exactly once. A body that answers short
///     is a broker whose wire procedure has no KV leg, and reading that as a
///     successful gate is precisely the silent misalignment the route's own
///     count guard exists for.
fn align_transaction_results(
    body: &str,
    pushes: usize,
    acks: usize,
    kv: usize,
) -> Result<TxnOutcome> {
    let parsed: TransactionResponseBody =
        serde_json::from_str(body).map_err(|e| Error::Body(e.to_string()))?;
    if !parsed.success {
        return Err(match parsed.reason.as_str() {
            "kv_precondition" => Error::Precondition {
                // The flat base of the rider is every push and ack operation
                // that came before it.
                failed_index: parsed
                    .failed_index
                    .map_or(0, |flat| flat.saturating_sub(flat_ops(pushes, acks))),
                reason: parsed.kv_reason.unwrap_or_default(),
                version: parsed.version.unwrap_or_default(),
                value: parsed.value,
            },
            // The one other verdict a caller ANSWERS rather than reports. See
            // [`Error::Duplicate`].
            "duplicate" => Error::Duplicate(parsed.error),
            other => Error::Body(format!(
                "the transaction answered success=false, reason={other}: {}",
                Snippet(&parsed.error)
            )),
        });
    }
    let mut out = TxnOutcome::default();
    let mut kv_slots: Vec<Option<KvAnswer>> = (0..kv).map(|_| None).collect();
    for r in parsed.results {
        match r.op_index {
            None if r.kind == "ack" => {
                if !r.success {
                    return Err(Error::Body(format!(
                        "the transaction committed but ack {} reports success=false",
                        r.index
                    )));
                }
                out.acks.push(TxnAckEcho {
                    index: r.index,
                    transaction_id: r.push.transaction_id,
                    dlq: r.dlq,
                });
            }
            None => {
                if !r.success {
                    return Err(Error::Body(format!(
                        "the transaction committed but operation {} reports success=false",
                        r.index
                    )));
                }
                out.pushes.push(TxnPushEcho {
                    index: r.index,
                    ..r.push
                });
            }
            Some(at) if r.kind == "kv" => {
                let slot = kv_slots.get_mut(at).ok_or_else(|| {
                    Error::Body(format!("transaction kv result {at} is out of range"))
                })?;
                if slot.is_some() {
                    return Err(Error::Body(format!(
                        "transaction kv result {at} appears twice"
                    )));
                }
                *slot = Some(KvAnswer { index: at, ..r.kv });
            }
            // A rider kind this facade did not send. Named rather than ignored.
            Some(at) => {
                return Err(Error::Body(format!(
                    "the transaction answered a `{}` rider at {at}, and this facade sent none",
                    r.kind
                )))
            }
        }
    }
    if out.pushes.len() != pushes {
        return Err(Error::Body(format!(
            "the transaction answered {} push results for {pushes} records",
            out.pushes.len()
        )));
    }
    if out.acks.len() != acks {
        return Err(Error::Body(format!(
            "the transaction answered {} ack results for {acks} acks",
            out.acks.len()
        )));
    }
    out.kv = kv_slots
        .into_iter()
        .enumerate()
        .map(|(i, r)| {
            r.ok_or_else(|| {
                Error::Body(format!(
                    "the transaction answered nothing for kv operation {i}; the bundle committed \
                     and its gate cannot be trusted"
                ))
            })
        })
        .collect::<Result<Vec<KvAnswer>>>()?;
    Ok(out)
}

/// Where the KV rider starts in the FLAT operation space: one ordinal per push
/// ITEM plus one per ack.
///
/// PER ITEM, and that is the whole subtlety. This facade sends its records as
/// ONE `{"type":"push","items":[…]}` entry, but the broker walks the items and
/// advances the flat index once for each (`for item in items { txn_add_push(…,
/// flat, …); flat += 1; }`, server/src/handlers/data.rs) before it sets
/// `kv_base: flat`. Counting the batch as one ordinal attributes every
/// precondition verdict of a bundle with more than one record to the wrong KV
/// operation — and for the redrive move that is a lost gate read as a won one.
fn flat_ops(pushes: usize, acks: usize) -> usize {
    pushes + acks
}

/// Reject a `QUEEN_URL` at boot instead of on the first request, and strip the
/// trailing slash so paths concatenate to exactly one.
pub fn normalize_base_url(raw: &str) -> std::result::Result<String, String> {
    let trimmed = raw.trim().trim_end_matches('/');
    let url: reqwest::Url = trimmed
        .parse()
        .map_err(|e| format!("QUEEN_URL={raw} is not a URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(format!(
            "QUEEN_URL={raw} has scheme `{}` — the facade speaks HTTP to Queen, so it must be \
             http:// or https://",
            url.scheme()
        ));
    }
    if url.host_str().is_none() {
        return Err(format!("QUEEN_URL={raw} has no host"));
    }
    Ok(trimmed.to_string())
}

// ----------------------------------------------------------------- credentials

/// One credential, as a long-lived map may keep it: 128 bits of process-local
/// keyed hash, or `None` for a call made with no credential at all.
///
/// The map this keys decides which principal's queue list a request sees, so a
/// collision is not a wrong cache entry, it is one tenant served another's. One
/// `RandomState` gives 64 bits; two independent ones give 128. Keyed by the raw
/// token instead, the map would be a permanent record of every bearer the
/// process has been shown — including the refused ones, which on a SigV4
/// listener are derived from secrets the facade holds.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct CredentialKey(Option<(u64, u64)>);

/// Deliberately says nothing. The key is not the secret, but it is a stable
/// per-process identifier for one.
impl std::fmt::Debug for CredentialKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            None => write!(f, "CredentialKey(none)"),
            Some(_) => write!(f, "CredentialKey(hashed)"),
        }
    }
}

impl CredentialKey {
    /// The key for `token`. The string is read and dropped; nothing here keeps a
    /// copy of it.
    pub fn of(token: Option<&str>) -> CredentialKey {
        let Some(token) = token else {
            return CredentialKey(None);
        };
        let (a, b) = hashers();
        CredentialKey(Some((a.hash_one(token), b.hash_one(token))))
    }

    pub fn is_anonymous(&self) -> bool {
        self.0.is_none()
    }
}

/// The two hash keys, drawn once per process from the OS.
fn hashers() -> &'static (RandomState, RandomState) {
    static HASHERS: OnceLock<(RandomState, RandomState)> = OnceLock::new();
    HASHERS.get_or_init(|| (RandomState::new(), RandomState::new()))
}

// -------------------------------------------------------------------- catalog

/// How long a queue list is served without asking Queen again.
///
/// Short on purpose. Unlike a Kafka client, an SQS client does not refresh
/// metadata on a timer — it asks for a queue URL once and then sends — so this
/// cache exists for a narrower thing: the burst of `GetQueueUrl` and
/// `ListQueues` a starting fleet makes, and the existence check every
/// SendMessage would otherwise pay for. Past a few seconds it starts hiding
/// queues from clients that just created them.
const LIST_TTL: Duration = Duration::from_secs(3);

/// How many scopes one catalog keeps an answer for. The key space is at worst
/// "credentials this facade has been shown"; a listener with `QUEEN_SQS_AUTH=off`
/// has one, a SigV4 one has as many as the operator configured. Bounded anyway,
/// and the coldest entry goes when a new one arrives.
const MAX_CREDENTIALS: usize = 1_024;

/// The queue list, cached briefly, with single-flight refresh.
///
/// ## Nothing waits behind the admin call
///
/// The refresh is single-flight, and the naive way to get that — hold the map
/// lock across the call — is what makes the whole PROCESS wait on it.
/// `GET /api/v1/resources/queues` is the enriched form, a pass over the tenant's
/// partitions, under a ten-second budget; one expiry every [`LIST_TTL`] would
/// park every request on every connection behind it.
///
/// So the map lock is never held across a call. The refresh is serialised on a
/// SEPARATE per-credential lock, and a caller that finds it taken does not queue
/// behind it: it serves the list it already has. Only a caller with nothing in
/// hand at all waits — and then it waits for the call already in flight rather
/// than starting a second one.
pub struct Catalog {
    api: Arc<dyn QueenApi>,
    ttl: Duration,
    /// Keyed by credential. Held for a map read or a map write and never across
    /// an await on Queen.
    entries: tokio::sync::Mutex<HashMap<CredentialKey, Entry>>,
    /// One refresh lock per credential — the same key space as `entries`. THIS
    /// is what is held across the call to Queen, and holding it blocks only the
    /// callers that have no list to serve.
    refreshes: tokio::sync::Mutex<HashMap<CredentialKey, Arc<tokio::sync::Mutex<()>>>>,
}

/// What the last call to Queen for one credential produced, and when it
/// finished. Both outcomes are kept, and that is the point: the success is kept
/// even after it goes stale so a blip serves a slightly old world instead of an
/// empty one, and the FAILURE is kept because the single-flight mutex only
/// collapses a storm on the success path.
struct Entry {
    queues: Option<Arc<Vec<Queue>>>,
    listed_at: Option<Instant>,
    failure: Option<Error>,
    /// When the last call finished, whichever way it went. This is what the TTL
    /// is measured against.
    probed_at: Instant,
    /// When this entry was last READ, which is a different question and is the
    /// one eviction asks: a credential in steady use is served from the cache
    /// without a call to Queen, so its `probed_at` sits still while it is doing
    /// exactly what it is here for.
    used_at: Instant,
}

impl Catalog {
    pub fn new(api: Arc<dyn QueenApi>) -> Catalog {
        Catalog::with_ttl(api, LIST_TTL)
    }

    /// Same, with an explicit TTL. For tests, which cannot wait three seconds to
    /// prove that the cache expires.
    pub fn with_ttl(api: Arc<dyn QueenApi>, ttl: Duration) -> Catalog {
        Catalog {
            api,
            ttl,
            entries: tokio::sync::Mutex::new(HashMap::new()),
            refreshes: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    /// The queue list for `token`, from cache when it is fresh.
    ///
    /// On a refresh failure with a previous list in hand, that list is served
    /// stale and the error is logged: answering `QueueDoesNotExist` because the
    /// admin API blipped for one second would make a client delete its own
    /// configuration, which is a far worse answer than a list three seconds old.
    /// With nothing cached, the error is returned and the caller decides the SQS
    /// error code.
    pub async fn list(&self, token: Option<&str>) -> Result<Arc<Vec<Queue>>> {
        let key = CredentialKey::of(token);
        if let Some(fresh) = self.cached(&key).await {
            return fresh;
        }
        let refresh = self.refresh_lock(&key).await;
        // Ours to run, or someone else's. `try_lock` and not `lock` because that
        // difference IS the fix: a caller that finds the refresh taken must not
        // queue behind it.
        let held = match refresh.try_lock() {
            Ok(held) => held,
            Err(_) => {
                if let Some(stale) = self.in_hand(&key).await {
                    return Ok(stale);
                }
                // Nothing to serve. Wait for the call in flight rather than
                // starting a second one, and take its answer.
                refresh.lock().await
            }
        };
        // Re-checked under the lock: the answer to the call we just missed may
        // have landed between the two.
        let answer = match self.cached(&key).await {
            Some(fresh) => fresh,
            None => self.fetch(&key, token, true).await,
        };
        drop(held);
        answer
    }

    /// The queue list as of now, ignoring the TTL — and reporting a failure
    /// rather than falling back to the stale copy.
    ///
    /// NO ACTION CALLS THIS, and the sentence is here so that the next reader
    /// does not assume one does. Both paths that look like candidates decided
    /// against it, in situ: CreateQueue's existence guard reads
    /// [`Catalog::has`] because refreshing does not close the race against a
    /// queue a native producer creates in the same instant, while a full
    /// `/api/v1/resources/queues` per request would turn a create storm into
    /// load on the broker's slowest admin route (`actions::queues`), and
    /// PurgeQueue's recreate reads no list at all — it re-applies the record it
    /// read fresh and configures the queue directly. What this is for is a
    /// caller that must not act on a stale world AND can pay for the call; it
    /// stays because the cache's own tests need a way to force the read.
    pub async fn refresh(&self, token: Option<&str>) -> Result<Arc<Vec<Queue>>> {
        let key = CredentialKey::of(token);
        let refresh = self.refresh_lock(&key).await;
        // Waits out a call in flight instead of overlapping one. `list` does not
        // wait here; this path does, because a stale answer is exactly what it
        // may not have.
        let _held = refresh.lock().await;
        // The TTL is deliberately ignored for a SUCCESS — that is the whole
        // reason this method exists. A fresh FAILURE is different: re-running a
        // call that just failed is not a fresher answer.
        if let Some(e) = self.remembered_failure(&key).await {
            return Err(e);
        }
        self.fetch(&key, token, false).await
    }

    /// Whether Queen has a queue of this name right now, from the cache when it
    /// is fresh. `SendMessage` to a queue that was deleted under it is
    /// `QueueDoesNotExist`, and this is the read that decides it.
    pub async fn has(&self, name: &str, token: Option<&str>) -> Result<bool> {
        Ok(self.list(token).await?.iter().any(|q| q.name == name))
    }

    /// Configure a queue and drop the cached list, so the next [`Catalog::list`]
    /// sees it.
    pub async fn configure(
        &self,
        name: &str,
        options: &serde_json::Value,
        token: Option<&str>,
    ) -> Result<()> {
        self.api.configure_queue(name, options, token).await?;
        self.entries.lock().await.remove(&CredentialKey::of(token));
        Ok(())
    }

    /// Delete a queue and drop the cached list.
    ///
    /// The cache is dropped on a FAILURE too, and deliberately: a delete whose
    /// answer did not arrive may still have landed, and serving a list that says
    /// the queue is there would be a guess.
    pub async fn delete(&self, name: &str, token: Option<&str>) -> Result<Deleted> {
        let answer = self.api.delete_queue(name, token).await;
        self.entries.lock().await.remove(&CredentialKey::of(token));
        answer
    }

    /// The answer for `key` if the last call to Queen finished within the TTL.
    /// `None` means "ask Queen".
    async fn cached(&self, key: &CredentialKey) -> Option<Result<Arc<Vec<Queue>>>> {
        let mut entries = self.entries.lock().await;
        let entry = entries.get_mut(key)?;
        if entry.probed_at.elapsed() >= self.ttl {
            return None;
        }
        entry.used_at = Instant::now();
        // A list in hand is the answer whether the last call succeeded or failed
        // — the failed one would have served it stale anyway.
        if let Some(queues) = &entry.queues {
            return Some(Ok(Arc::clone(queues)));
        }
        entry.failure.clone().map(Err)
    }

    /// The last list that arrived for `key`, at any age.
    async fn in_hand(&self, key: &CredentialKey) -> Option<Arc<Vec<Queue>>> {
        let entries = self.entries.lock().await;
        entries.get(key)?.queues.as_ref().map(Arc::clone)
    }

    /// The failure this credential's last call produced, while it is still
    /// inside the TTL.
    async fn remembered_failure(&self, key: &CredentialKey) -> Option<Error> {
        let entries = self.entries.lock().await;
        let entry = entries.get(key)?;
        (entry.probed_at.elapsed() < self.ttl)
            .then(|| entry.failure.clone())
            .flatten()
    }

    /// The refresh lock for one credential, created on first use. The map is
    /// swept rather than capped: an entry nobody is holding is a lock nobody is
    /// waiting on, so dropping it is free — and an entry that IS held stays,
    /// because two callers holding two different mutexes for one credential
    /// would be two calls to Queen where the point is to have one.
    async fn refresh_lock(&self, key: &CredentialKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut refreshes = self.refreshes.lock().await;
        if refreshes.len() >= MAX_CREDENTIALS && !refreshes.contains_key(key) {
            refreshes.retain(|_, lock| Arc::strong_count(lock) > 1);
        }
        Arc::clone(refreshes.entry(*key).or_default())
    }

    /// Ask Queen and store the answer. `fall_back_to_stale` decides what a
    /// failure means: for [`Catalog::list`] a slightly old world, for
    /// [`Catalog::refresh`] an error.
    async fn fetch(
        &self,
        key: &CredentialKey,
        token: Option<&str>,
        fall_back_to_stale: bool,
    ) -> Result<Arc<Vec<Queue>>> {
        // Read before the call so a failure does not forget the last good list.
        let previous = self
            .entries
            .lock()
            .await
            .get(key)
            .and_then(|e| e.queues.as_ref().map(|q| (Arc::clone(q), e.listed_at)));

        match self.api.list_queues(token).await {
            Ok(queues) => {
                let queues = Arc::new(queues);
                let now = Instant::now();
                let mut entries = self.entries.lock().await;
                make_room(&mut entries, key);
                entries.insert(
                    *key,
                    Entry {
                        queues: Some(Arc::clone(&queues)),
                        listed_at: Some(now),
                        failure: None,
                        probed_at: now,
                        used_at: now,
                    },
                );
                drop(entries);
                Ok(queues)
            }
            Err(e) => {
                // The failure is remembered whether or not it is served, so the
                // callers behind this one do not each pay for it.
                let now = Instant::now();
                let mut entries = self.entries.lock().await;
                make_room(&mut entries, key);
                entries.insert(
                    *key,
                    Entry {
                        queues: previous.as_ref().map(|(q, _)| Arc::clone(q)),
                        listed_at: previous.as_ref().and_then(|(_, at)| *at),
                        failure: Some(e.clone()),
                        probed_at: now,
                        used_at: now,
                    },
                );
                drop(entries);
                match previous.filter(|_| fall_back_to_stale) {
                    Some((stale, listed_at)) => {
                        tracing::warn!(
                            target: "sqs",
                            error = %e,
                            age_ms = listed_at
                                .map(|at| at.elapsed().as_millis() as u64)
                                .unwrap_or(0),
                            "queue list refresh failed; serving the last one"
                        );
                        Ok(stale)
                    }
                    None => Err(e),
                }
            }
        }
    }
}

/// Drop the coldest entry if `key` would take the map past its bound. "Coldest"
/// is least recently READ and not least recently refreshed: a credential doing
/// its job is answered from the cache without a call to Queen, and evicting the
/// entries that are working while keeping the ones that are not is the wrong way
/// round.
fn make_room(entries: &mut HashMap<CredentialKey, Entry>, key: &CredentialKey) {
    if entries.len() < MAX_CREDENTIALS || entries.contains_key(key) {
        return;
    }
    if let Some(coldest) = entries
        .iter()
        .min_by_key(|(_, e)| e.used_at)
        .map(|(k, _)| *k)
    {
        entries.remove(&coldest);
    }
}

/// The test double, shared with every module that talks to Queen through the
/// trait above.
///
/// It is deliberately NOT a script. queen-kafka's double became a log because
/// its facade's correctness is "did the right bytes reach the right offset";
/// this facade's correctness is "does a message become invisible for exactly the
/// visibility timeout, come back with the attempt count incremented, and go away
/// when it is deleted" — so the double implements the CLAIM, the LEASE, the
/// EXPIRY and the ack taxonomy, out of the two stored procedures that own them
/// (004_log_pop.sql and 005_log_ack.sql). A double that answered "ok" to every
/// ack would let every interesting SQS defect through.
#[cfg(test)]
pub mod testing {
    use super::*;
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::sync::Mutex;

    /// What the fake reads the time from.
    ///
    /// A trait, and the only clock in this module, because every question the
    /// double exists to answer is a question about a DEADLINE: has the lease
    /// expired, has the key's TTL run out, is the timer due. A test that had to
    /// sleep for those would either be slow or be flaky, and one that could not
    /// move the clock could not reach the expiry paths at all.
    pub trait Clock: Send + Sync {
        fn now_ms(&self) -> i64;
    }

    /// The process clock. Here so the trait has a production implementation and
    /// the seam is real rather than test-only decoration.
    pub struct SystemClock;

    impl Clock for SystemClock {
        fn now_ms(&self) -> i64 {
            crate::obs::now_epoch_ms()
        }
    }

    /// A clock a test drives. Starts at a fixed instant so a rendered timestamp
    /// is reproducible, and only [`TestClock::advance`] moves it.
    pub struct TestClock {
        ms: AtomicI64,
    }

    impl TestClock {
        /// 2026-08-30T00:00:00Z, which is the day this facade was designed and
        /// is far enough from every epoch boundary to make an off-by-a-day
        /// obvious in an assertion.
        pub const EPOCH_MS: i64 = 1_787_011_200_000;

        pub fn new() -> Arc<TestClock> {
            Arc::new(TestClock {
                ms: AtomicI64::new(TestClock::EPOCH_MS),
            })
        }

        pub fn advance(&self, d: Duration) {
            self.ms.fetch_add(d.as_millis() as i64, Ordering::Relaxed);
        }

        pub fn advance_secs(&self, s: u64) {
            self.advance(Duration::from_secs(s));
        }
    }

    impl Clock for TestClock {
        fn now_ms(&self) -> i64 {
            self.ms.load(Ordering::Relaxed)
        }
    }

    /// The broker's `to_char(created_at, 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`, which
    /// is the format every `createdAt` on the pop path carries and the one
    /// `SentTimestamp` is derived from.
    pub fn iso_from_epoch_ms(ms: i64) -> String {
        let days = ms.div_euclid(86_400_000);
        let rem = ms.rem_euclid(86_400_000);
        let (y, mo, d) = crate::obs::civil_from_days(days);
        let (h, mi, s, milli) = (
            rem / 3_600_000,
            (rem / 60_000) % 60,
            (rem / 1_000) % 60,
            rem % 1_000,
        );
        format!(
            "{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{:06}Z",
            milli * 1000
        )
    }

    /// One queue, as `/configure` left it. Only the columns the pop/ack
    /// semantics actually read are modelled.
    #[derive(Clone)]
    struct QueueRec {
        id: String,
        options: serde_json::Value,
    }

    impl QueueRec {
        /// `retryLimit`, the SP's own default (012_configure.sql).
        fn retry_limit(&self) -> i64 {
            self.options
                .get("retryLimit")
                .and_then(|v| v.as_i64())
                .unwrap_or(3)
        }

        /// `deadLetterQueue`, which defaults TRUE on the broker and which every
        /// SQS-created queue sets FALSE: the native DLQ stays out of the SQS
        /// path, where redrive is the facade's atomic move
        /// (PLAN_QUEEN_SQS.md, DLQ).
        fn dlq_enabled(&self) -> bool {
            self.options
                .get("deadLetterQueue")
                .and_then(|v| v.as_bool())
                .unwrap_or(true)
        }

        /// `leaseTime`, seconds — the visibility timeout a pop that named none
        /// gets.
        fn lease_time(&self) -> i64 {
            self.options
                .get("leaseTime")
                .and_then(|v| v.as_i64())
                .unwrap_or(300)
        }
    }

    /// One message in a lane.
    #[derive(Clone)]
    struct Msg {
        id: String,
        txn: String,
        payload: serde_json::Value,
        created_at: String,
    }

    /// One partition of the fake log.
    #[derive(Default, Clone)]
    struct Lane {
        /// The offset of `msgs[0]`; everything below it was retained away.
        start: i64,
        msgs: Vec<Msg>,
    }

    impl Lane {
        /// One past the last offset — `p.last_offset + 1`.
        fn high(&self) -> i64 {
            self.start + self.msgs.len() as i64
        }

        fn at(&self, offset: i64) -> Option<&Msg> {
            usize::try_from(offset - self.start)
                .ok()
                .and_then(|i| self.msgs.get(i))
        }

        /// The FIRST offset holding this dedup key — the PUSH's question, since
        /// this double's dedup index is the whole lane where the broker's is a
        /// window (see [`FakeQueen::append`]).
        fn offset_of(&self, txn: &str) -> Option<i64> {
            self.msgs
                .iter()
                .position(|m| m.txn == txn)
                .map(|i| self.start + i as i64)
        }

        /// The ACK's question: which position a hash names inside the span an
        /// ack may act on, and whether the same hash also sits at or below the
        /// cursor.
        ///
        /// It is NOT [`Lane::offset_of`], and the difference is the stored
        /// procedure's own: `eff` is `MIN(voff) FILTER (WHERE voff >= lo AND voff
        /// <= batch_end)` with `lo = GREATEST(committed + 1, txns_start)`
        /// (005_log_ack.sql), so an occurrence BELOW the cursor is excluded from
        /// the resolution rather than being the answer to it — the in-span copy
        /// is what gets acked. `below` is the procedure's separate flag, and a
        /// hash that has one and no `eff` is the noop/stale case.
        ///
        /// One claim CAN hold two messages under one hash: the key is unique
        /// only inside the queue's dedup window. A double that answered the
        /// lowest occurrence in the LANE would turn an ack the broker applies
        /// into a below-cursor noop, and every test resting on "nothing was
        /// acked" would be resting on the double.
        fn resolve(
            &self,
            txn: &str,
            committed: i64,
            batch_end: Option<i64>,
        ) -> (Option<i64>, bool) {
            let offsets = self
                .msgs
                .iter()
                .enumerate()
                .filter(|(_, m)| m.txn == txn)
                .map(|(i, _)| self.start + i as i64);
            let mut effective = None;
            let mut below = false;
            for offset in offsets {
                if offset <= committed {
                    below = true;
                    continue;
                }
                if batch_end.is_some_and(|end| offset > end) {
                    continue;
                }
                effective = Some(effective.map_or(offset, |lowest: i64| lowest.min(offset)));
            }
            (effective, below)
        }
    }

    /// One `(partition, consumer_group)` row of `queen.log_consumers` — the
    /// claim, which IS the visibility timeout.
    #[derive(Clone)]
    struct Claim {
        /// LAST ACKED offset; the next wanted one is `committed + 1`.
        committed: i64,
        /// Inclusive end of the leased span `(committed, batch_end]`.
        batch_end: Option<i64>,
        worker: Option<String>,
        lease_expires_ms: Option<i64>,
        /// `attempt_count`: incremented when a delivery starts at the SAME
        /// offset as the previous one, reset to 1 anywhere else. This is
        /// `ApproximateReceiveCount`.
        attempt_count: i64,
        attempt_offset: Option<i64>,
        /// `batch_retry_count`: charged ONCE per explicit `failed` ack while
        /// budget remains, reset on batch completion, NEVER charged on lease
        /// expiry or a plain release.
        batch_retry_count: i64,
    }

    impl Claim {
        fn leased_at(&self, now_ms: i64) -> bool {
            self.lease_expires_ms.is_some_and(|at| at > now_ms)
        }
    }

    /// One row of the fake key/value store.
    #[derive(Clone)]
    struct Stored {
        value: serde_json::Value,
        version: i64,
        /// `None` is `forever`.
        expires_ms: Option<i64>,
    }

    impl Stored {
        /// `queen.kv_live_v1`: an expired row is NEVER returned and NEVER counts
        /// as existing, before the sweeper prunes it.
        fn live(&self, now_ms: i64) -> bool {
            self.expires_ms.is_none_or(|at| at > now_ms)
        }
    }

    /// The fake key/value store. A `BTreeMap`, and that is load-bearing rather
    /// than tidy: the stored procedure's key column is `COLLATE "C"`, so a
    /// prefix read comes back in BYTE order and pages with a cursor in that
    /// order. A `HashMap` here would let a paging bug pass.
    type Store = BTreeMap<(String, String), Stored>;

    /// `queen.kv_ver_v1`: the version of a row as a READER sees it — 0 for an
    /// absent row and for an expired one alike.
    fn effective_version(row: Option<&Stored>, now_ms: i64) -> i64 {
        row.filter(|r| r.live(now_ms)).map_or(0, |r| r.version)
    }

    /// One pending timer.
    #[derive(Clone)]
    struct Timer {
        partition: String,
        deliver_at_ms: i64,
        txn: String,
        payload_b64: String,
        message_id: String,
    }

    /// Both directions of the `(queue, partition)` ↔ partition-id mapping. Both,
    /// because a pop needs the id for a lane and an ack arrives with nothing but
    /// the id.
    type PartitionIds = (
        BTreeMap<(String, String), String>,
        BTreeMap<String, (String, String)>,
    );

    /// One recorded transaction: the records, the acks and the KV rider it was
    /// sent with.
    type TxnCall = (Vec<PushItem>, Vec<TxnAck>, Vec<KvOp>);

    /// A [`QueenApi`] that behaves like a broker: a log, claims with leases, the
    /// ack taxonomy, a key/value store with preconditions and TTLs, and timers
    /// that fire.
    pub struct FakeQueen {
        pub clock: Arc<TestClock>,
        queues: Mutex<BTreeMap<String, QueueRec>>,
        logs: Mutex<BTreeMap<(String, String), Lane>>,
        claims: Mutex<BTreeMap<(String, String, String), Claim>>,
        /// `(queue, partition)` ↔ the opaque partition id an ack is addressed
        /// by. Minted on first use, in creation order, so a test can read one.
        pids: Mutex<PartitionIds>,
        kv: Mutex<Store>,
        kv_version: AtomicI64,
        /// Ceiling on the rows ONE read answers before it truncates, standing in
        /// for the stored procedure's 4 MiB budget ([`MAX_KV_READ_BYTES`]), which
        /// no test can reach with realistic values.
        kv_read_rows: Mutex<Option<usize>>,
        timers: Mutex<BTreeMap<(String, String), Timer>>,
        seq: AtomicI64,

        // ---------------------------------------------------------- recording
        pub tokens: Mutex<Vec<Option<String>>>,
        pub pushes: Mutex<Vec<Vec<PushItem>>>,
        /// Every pop, as `(queue, partition, options)`.
        pub pops: Mutex<Vec<(String, Option<String>, PopOptions)>>,
        pub acks: Mutex<Vec<Vec<AckItem>>>,
        pub extends: Mutex<Vec<(String, i64)>>,
        pub kv_calls: Mutex<Vec<Vec<KvOp>>>,
        pub timer_calls: Mutex<Vec<Vec<TimerSchedule>>>,
        /// Every `POST /api/v1/transaction`, as it was sent: the records, the
        /// acks and the KV rider TOGETHER, because what the redrive move asserts
        /// is a property of the three arrays at once — that the push and the ack
        /// are in one bundle, and that a bundle which lost its gate carries
        /// neither.
        pub transactions: Mutex<Vec<TxnCall>>,
        pub configures: Mutex<Vec<(String, serde_json::Value)>>,
        pub deletes: Mutex<Vec<String>>,

        // -------------------------------------------------- scripted failures
        /// When set, every call fails with it.
        pub fail: Mutex<Option<String>>,
        pub list_error: Mutex<Option<Error>>,
        pub push_error: Mutex<Option<Error>>,
        /// The per-item statuses the next push answers instead of `queued`. See
        /// [`FakeQueen::next_push_statuses`].
        pub push_statuses: Mutex<VecDeque<String>>,
        pub pop_error: Mutex<Option<Error>>,
        pub ack_error: Mutex<Option<Error>>,
        pub kv_error: Mutex<Option<Error>>,
        pub transaction_error: Mutex<Option<Error>>,
        /// A SECOND WRITER, landing between two of the facade's own calls: one
        /// entry is popped per KV call and applied BEFORE it. The only way to
        /// reach the arm where a compare-and-set loses to a third party that
        /// moved the version mid-sequence — a race between two facade instances,
        /// which the stateless-by-design deployment makes the normal case.
        pub kv_interpose: Mutex<VecDeque<Option<KvOp>>>,
    }

    impl FakeQueen {
        /// A broker holding `queues`, each with the broker's own default config.
        pub fn with(queues: &[&str]) -> Arc<FakeQueen> {
            let fake = FakeQueen::empty();
            for q in queues {
                fake.add_queue(q, serde_json::json!({}));
            }
            fake
        }

        pub fn empty() -> Arc<FakeQueen> {
            Arc::new(FakeQueen {
                clock: TestClock::new(),
                queues: Mutex::new(BTreeMap::new()),
                logs: Mutex::new(BTreeMap::new()),
                claims: Mutex::new(BTreeMap::new()),
                pids: Mutex::new((BTreeMap::new(), BTreeMap::new())),
                kv: Mutex::new(BTreeMap::new()),
                // Well above zero so a test asserting on a version cannot pass
                // by accident against a counter.
                kv_version: AtomicI64::new(1_000),
                kv_read_rows: Mutex::new(None),
                timers: Mutex::new(BTreeMap::new()),
                seq: AtomicI64::new(1),
                tokens: Mutex::new(Vec::new()),
                pushes: Mutex::new(Vec::new()),
                pops: Mutex::new(Vec::new()),
                acks: Mutex::new(Vec::new()),
                extends: Mutex::new(Vec::new()),
                kv_calls: Mutex::new(Vec::new()),
                timer_calls: Mutex::new(Vec::new()),
                transactions: Mutex::new(Vec::new()),
                configures: Mutex::new(Vec::new()),
                deletes: Mutex::new(Vec::new()),
                fail: Mutex::new(None),
                list_error: Mutex::new(None),
                push_error: Mutex::new(None),
                push_statuses: Mutex::new(VecDeque::new()),
                pop_error: Mutex::new(None),
                ack_error: Mutex::new(None),
                kv_error: Mutex::new(None),
                transaction_error: Mutex::new(None),
                kv_interpose: Mutex::new(VecDeque::new()),
            })
        }

        pub fn add_queue(&self, name: &str, options: serde_json::Value) {
            let id = format!("queue-{:08}", self.next());
            self.queues
                .lock()
                .unwrap()
                .insert(name.to_string(), QueueRec { id, options });
        }

        /// Put messages into a lane at `start`, as if a native producer had.
        /// Retention below `start` is what makes a claim that must skip the
        /// bottom of the log reachable.
        pub fn seed(
            &self,
            queue: &str,
            partition: &str,
            start: i64,
            payloads: &[serde_json::Value],
        ) {
            let created_at = iso_from_epoch_ms(self.clock.now_ms());
            let msgs = payloads
                .iter()
                .map(|p| Msg {
                    id: self.mint("msg"),
                    txn: self.mint("txn"),
                    payload: p.clone(),
                    created_at: created_at.clone(),
                })
                .collect();
            self.logs.lock().unwrap().insert(
                (queue.to_string(), partition.to_string()),
                Lane { start, msgs },
            );
            self.partition_id(queue, partition);
        }

        /// Append ONE message with a caller-chosen dedup key, whether or not the
        /// lane already holds that key.
        ///
        /// It is the one lane state [`FakeQueen::push`] cannot produce and the
        /// broker reaches every day: this double's dedup index is the whole
        /// lane, where the broker's is a WINDOW (`dedupWindowSeconds`, 300 on an
        /// SQS-created FIFO queue), so the same `MessageDeduplicationId` sent
        /// again after the window is a new message at a new offset with a new
        /// uuid. 005_log_ack.sql's "MIN picks the ORIGINAL occurrence" rule is
        /// written for exactly this lane, and so is [`Lane::offset_of`].
        pub fn append(&self, queue: &str, partition: &str, txn: &str, payload: serde_json::Value) {
            let msg = Msg {
                id: self.mint("msg"),
                txn: txn.to_string(),
                payload,
                created_at: iso_from_epoch_ms(self.clock.now_ms()),
            };
            self.logs
                .lock()
                .unwrap()
                .entry((queue.to_string(), partition.to_string()))
                .or_default()
                .msgs
                .push(msg);
            self.partition_id(queue, partition);
        }

        /// The opaque id an ack addresses this lane by, minting it if this is the
        /// first time the lane has been named.
        pub fn partition_id(&self, queue: &str, partition: &str) -> String {
            let key = (queue.to_string(), partition.to_string());
            let mut pids = self.pids.lock().unwrap();
            if let Some(id) = pids.0.get(&key) {
                return id.clone();
            }
            let id = format!("pid-{:08}", self.seq.fetch_add(1, Ordering::Relaxed));
            pids.0.insert(key.clone(), id.clone());
            pids.1.insert(id.clone(), key);
            id
        }

        /// Every message in a lane, in offset order, as `(offset, txn)`.
        pub fn lane(&self, queue: &str, partition: &str) -> Vec<(i64, String)> {
            let logs = self.logs.lock().unwrap();
            match logs.get(&(queue.to_string(), partition.to_string())) {
                None => Vec::new(),
                Some(lane) => lane
                    .msgs
                    .iter()
                    .enumerate()
                    .map(|(i, m)| (lane.start + i as i64, m.txn.clone()))
                    .collect(),
            }
        }

        /// The committed cursor of one `(partition, group)`, or `None` when the
        /// group has never registered there.
        pub fn committed(&self, queue: &str, partition: &str, group: &str) -> Option<i64> {
            self.claims
                .lock()
                .unwrap()
                .get(&(queue.to_string(), partition.to_string(), group.to_string()))
                .map(|c| c.committed)
        }

        /// Whether a live lease is held on one `(partition, group)`.
        pub fn leased(&self, queue: &str, partition: &str, group: &str) -> bool {
            let now = self.clock.now_ms();
            self.claims
                .lock()
                .unwrap()
                .get(&(queue.to_string(), partition.to_string(), group.to_string()))
                .is_some_and(|c| c.leased_at(now))
        }

        /// The retry budget spent on one `(partition, group)`.
        pub fn retries(&self, queue: &str, partition: &str, group: &str) -> i64 {
            self.claims
                .lock()
                .unwrap()
                .get(&(queue.to_string(), partition.to_string(), group.to_string()))
                .map_or(0, |c| c.batch_retry_count)
        }

        pub fn advance(&self, d: Duration) {
            self.clock.advance(d);
        }

        /// Put a key into the store, as if another instance had written it.
        pub fn kv_seed(&self, ns: &str, key: &str, value: serde_json::Value) {
            self.kv_seed_ttl(ns, key, value, None);
        }

        pub fn kv_seed_ttl(
            &self,
            ns: &str,
            key: &str,
            value: serde_json::Value,
            ttl_seconds: Option<u64>,
        ) {
            let version = self.next_version();
            let now = self.clock.now_ms();
            self.kv.lock().unwrap().insert(
                (ns.to_string(), key.to_string()),
                Stored {
                    value,
                    version,
                    expires_ms: ttl_seconds.map(|s| now + (s as i64) * 1000),
                },
            );
        }

        /// What the store holds for one key, or `None` when the key is absent OR
        /// expired — the same rule a reader gets.
        pub fn kv_get(&self, ns: &str, key: &str) -> Option<serde_json::Value> {
            let now = self.clock.now_ms();
            self.kv
                .lock()
                .unwrap()
                .get(&(ns.to_string(), key.to_string()))
                .filter(|row| row.live(now))
                .map(|row| row.value.clone())
        }

        /// The version one key holds, 0 when it is absent or expired.
        pub fn kv_version_of(&self, ns: &str, key: &str) -> i64 {
            let now = self.clock.now_ms();
            effective_version(
                self.kv
                    .lock()
                    .unwrap()
                    .get(&(ns.to_string(), key.to_string())),
                now,
            )
        }

        /// Truncate every read at `rows`. See [`FakeQueen::kv_read_rows`].
        pub fn kv_truncate_reads_at(&self, rows: usize) {
            *self.kv_read_rows.lock().unwrap() = Some(rows);
        }

        /// Every push, flattened.
        pub fn pushed(&self) -> Vec<PushItem> {
            self.pushes
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }

        /// Every ack, flattened.
        ///
        /// WHICH transaction id an ack names is the whole of the FIFO
        /// delete-set's contract — the cursor swallows everything below the
        /// position it resolves to (005_log_ack.sql), so a facade that acked the
        /// client's own message instead of the deleted PREFIX's last one would
        /// pass every state assertion and still lose messages. This is how a
        /// test reads that.
        pub fn acked(&self) -> Vec<AckItem> {
            self.acks
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }

        /// Every KV operation, flattened.
        pub fn kv_ops(&self) -> Vec<KvOp> {
            self.kv_calls
                .lock()
                .unwrap()
                .iter()
                .flatten()
                .cloned()
                .collect()
        }

        pub fn fail_with(&self, why: &str) {
            *self.fail.lock().unwrap() = Some(why.to_string());
        }

        pub fn fail_push(&self, e: Error) {
            *self.push_error.lock().unwrap() = Some(e);
        }

        /// The per-item verdicts the NEXT push answers, one per item, in place
        /// of what the log would have said.
        ///
        /// This is the shape the broker's own success answer can carry: a push
        /// that reports `error`, `buffered` or `failed` for an item comes back
        /// inside an HTTP 201 (server/src/handlers/data.rs relabels a failed DB
        /// transaction to `buffered`, or `failed` when even the file buffer
        /// refused it, and answers CREATED either way). A double that could only
        /// say `queued` cannot exercise the one path where a message is gone and
        /// the client was told it landed.
        pub fn next_push_statuses(&self, statuses: &[&str]) {
            *self.push_statuses.lock().unwrap() =
                statuses.iter().map(|s| (*s).to_string()).collect();
        }

        pub fn fail_pop(&self, e: Error) {
            *self.pop_error.lock().unwrap() = Some(e);
        }

        pub fn fail_ack(&self, e: Error) {
            *self.ack_error.lock().unwrap() = Some(e);
        }

        pub fn fail_kv(&self, e: Error) {
            *self.kv_error.lock().unwrap() = Some(e);
        }

        pub fn fail_transaction(&self, e: Error) {
            *self.transaction_error.lock().unwrap() = Some(e);
        }

        pub fn fail_list(&self, e: Error) {
            *self.list_error.lock().unwrap() = Some(e);
        }

        fn next(&self) -> i64 {
            self.seq.fetch_add(1, Ordering::Relaxed)
        }

        fn mint(&self, kind: &str) -> String {
            format!("{kind}-{:08}", self.next())
        }

        fn next_version(&self) -> i64 {
            self.kv_version.fetch_add(1, Ordering::Relaxed)
        }

        fn scripted(&self) -> Option<Error> {
            self.fail.lock().unwrap().clone().map(Error::Transport)
        }

        // ------------------------------------------------------------- engine

        /// Append one item to a lane, with the broker's dedup rule: a
        /// `transactionId` already in the lane is a DUPLICATE, and the answer
        /// carries the PRE-EXISTING message's id and offset (C1). That is what
        /// makes `MessageDeduplicationId` behave the way SQS documents it —
        /// a duplicate send inside the window answers the original MessageId.
        fn push_into(&self, logs: &mut BTreeMap<(String, String), Lane>, it: &PushItem) -> Pushed {
            let key = (it.queue.clone(), it.partition.clone());
            let lane = logs.entry(key).or_default();
            if let Some(txn) = it.transaction_id.as_deref().filter(|t| !t.is_empty()) {
                if let Some(off) = lane.offset_of(txn) {
                    let existing = lane.at(off).expect("resolved offset is in the lane");
                    return Pushed {
                        status: "duplicate".to_string(),
                        message_id: existing.id.clone(),
                        transaction_id: txn.to_string(),
                        offset: Some(off),
                    };
                }
            }
            let offset = lane.high();
            let id = self.mint("msg");
            let txn = it
                .transaction_id
                .clone()
                .filter(|t| !t.is_empty())
                .unwrap_or_else(|| self.mint("txn"));
            lane.msgs.push(Msg {
                id: id.clone(),
                txn: txn.clone(),
                payload: it.payload.clone(),
                created_at: iso_from_epoch_ms(self.clock.now_ms()),
            });
            self.partition_id(&it.queue, &it.partition);
            Pushed {
                status: "queued".to_string(),
                message_id: id,
                transaction_id: txn,
                offset: Some(offset),
            }
        }

        /// Fire every timer that is due, into the queue it names. The broker does
        /// this on a sweeper; the double does it whenever anyone looks, which is
        /// the same observable behaviour for a test that controls the clock.
        fn fire_due(&self) {
            let now = self.clock.now_ms();
            let due: Vec<((String, String), Timer)> = {
                let mut timers = self.timers.lock().unwrap();
                let keys: Vec<(String, String)> = timers
                    .iter()
                    .filter(|(_, t)| t.deliver_at_ms <= now)
                    .map(|(k, _)| k.clone())
                    .collect();
                keys.into_iter()
                    .filter_map(|k| timers.remove(&k).map(|t| (k, t)))
                    .collect()
            };
            if due.is_empty() {
                return;
            }
            let mut logs = self.logs.lock().unwrap();
            for ((queue, _key), t) in due {
                let payload = decode_payload(&t.payload_b64);
                let lane = logs
                    .entry((queue.clone(), t.partition.clone()))
                    .or_default();
                lane.msgs.push(Msg {
                    id: t.message_id,
                    txn: t.txn,
                    payload,
                    created_at: iso_from_epoch_ms(now),
                });
                // The lane borrow ends here; `partition_id` takes a different
                // lock and the fired message needs an addressable lane id.
                self.partition_id(&queue, &t.partition);
            }
        }

        /// The claim, exactly as `log_pop_v1` takes it (004_log_pop.sql:2432).
        ///
        /// A lane is claimable when nothing holds a LIVE lease on it and it owes
        /// work. An expired lease is therefore not an error state and costs
        /// nothing: it is simply a lane that is claimable again, which is what
        /// makes the visibility timeout a timeout rather than a lock.
        #[allow(clippy::too_many_arguments)]
        fn claim(
            &self,
            queue: &str,
            only: Option<&str>,
            opts: &PopOptions,
            logs: &BTreeMap<(String, String), Lane>,
            claims: &mut BTreeMap<(String, String, String), Claim>,
        ) -> Popped {
            let now = self.clock.now_ms();
            let group = opts.group().to_string();
            let lease_seconds = if opts.lease_seconds > 0 {
                opts.lease_seconds as i64
            } else {
                self.queues
                    .lock()
                    .unwrap()
                    .get(queue)
                    .map_or(300, QueueRec::lease_time)
            };
            let worker = self.mint("lease");
            let batch = opts.batch.clamp(1, MAX_POP_BATCH) as i64;
            let width = opts.partitions.max(1) as usize;

            let mut out = Popped {
                queue: queue.to_string(),
                partition: String::new(),
                partition_id: String::new(),
                lease_id: worker.clone(),
                consumer_group: group.clone(),
                messages: Vec::new(),
                partitions_claimed: 0,
            };
            for ((q, p), lane) in logs.iter() {
                if q != queue || only.is_some_and(|want| want != p) {
                    continue;
                }
                if out.partitions_claimed as usize >= width {
                    break;
                }
                let key = (q.clone(), p.clone(), group.clone());
                let claim = claims.entry(key).or_insert(Claim {
                    // Queue mode is hard-pinned to subscription mode `all` by
                    // the SQL, which is what an SQS queue is: a consumer sees
                    // everything that was ever written and not just what arrives
                    // after it connects.
                    committed: lane.start - 1,
                    batch_end: None,
                    worker: None,
                    lease_expires_ms: None,
                    attempt_count: 0,
                    attempt_offset: None,
                    batch_retry_count: 0,
                });
                if claim.leased_at(now) {
                    continue;
                }
                let start = claim.committed + 1;
                let take = (lane.high() - start).min(batch);
                if take <= 0 {
                    continue;
                }
                let end = start + take - 1;
                claim.worker = Some(worker.clone());
                claim.lease_expires_ms = Some(now + lease_seconds * 1000);
                claim.batch_end = Some(end);
                // Verbatim from the SQL: the same start offset as the previous
                // delivery is a REDELIVERY and increments; anywhere else is a
                // fresh batch and resets to 1.
                claim.attempt_count = if claim.attempt_offset == Some(start) {
                    claim.attempt_count + 1
                } else {
                    1
                };
                claim.attempt_offset = Some(start);
                let delivery_attempt = claim.attempt_count;
                let pid = self.partition_id(q, p);
                if out.partitions_claimed == 0 {
                    out.partition = p.clone();
                    out.partition_id = pid.clone();
                }
                out.partitions_claimed += 1;
                for off in start..=end {
                    let Some(msg) = lane.at(off) else { continue };
                    out.messages.push(Message {
                        id: msg.id.clone(),
                        transaction_id: msg.txn.clone(),
                        data: msg.payload.clone(),
                        partition: p.clone(),
                        partition_id: pid.clone(),
                        lease_id: worker.clone(),
                        consumer_group: group.clone(),
                        delivery_attempt,
                        created_at: msg.created_at.clone(),
                        // C-SQS-3, faithfully: the broker renders `seg.seq +
                        // idx`, which for this double's flat lane IS the
                        // absolute offset the append allocated — the same number
                        // [`Lane::offset_of`] answers the push side with. A
                        // double that left it `None` would let the FIFO
                        // `SequenceNumber` be tested only against the live rig.
                        offset: Some(off),
                    });
                }
            }
            out
        }

        /// The ack taxonomy of `log_ack_by_hash_v1` (005_log_ack.sql), which is
        /// the whole reason this double exists:
        ///
        ///   * IMPLICIT-ACK — the cursor goes to the highest completed position,
        ///     so every silent gap below it is completed and never redelivered;
        ///   * EXPLICIT SIGNALS ARE NEVER SKIPPED — the cursor is CLAMPED below
        ///     the lowest explicit failed/dlq/retry position in the same call,
        ///     even when a later position was completed, and that lowest signal
        ///     decides the action;
        ///   * BELOW-CURSOR HONESTY — a position at or below `committed` can
        ///     have no effect: `completed` is a harmless duplicate commit
        ///     (`noop`), anything else is rejected;
        ///   * `retry` releases the lease and charges NOTHING; `failed` charges
        ///     the budget once; an exhausted `failed` dead-letters or, on a queue
        ///     with the native DLQ off — which is every SQS queue — drops the
        ///     poison and advances past it.
        fn apply_acks(
            &self,
            group: &str,
            acks: &[AckItem],
            logs: &BTreeMap<(String, String), Lane>,
            claims: &mut BTreeMap<(String, String, String), Claim>,
        ) -> Vec<Acked> {
            let now = self.clock.now_ms();
            let pids = self.pids.lock().unwrap().1.clone();
            let queues = self.queues.lock().unwrap().clone();
            let mut out: Vec<Acked> = acks
                .iter()
                .enumerate()
                .map(|(index, a)| Acked {
                    index,
                    transaction_id: a.transaction_id.clone(),
                    success: false,
                    error: None,
                    lease_released: false,
                    dlq: false,
                    noop: false,
                })
                .collect();

            // One call to the stored procedure per (partition, worker), which is
            // also the grain at which the lease is validated.
            let mut groups: BTreeMap<(String, String), Vec<usize>> = BTreeMap::new();
            for (i, a) in acks.iter().enumerate() {
                groups
                    .entry((
                        a.partition_id.clone(),
                        a.lease_id.clone().unwrap_or_default(),
                    ))
                    .or_default()
                    .push(i);
            }

            for ((pid, worker), items) in groups {
                let Some((queue, partition)) = pids.get(&pid).cloned() else {
                    fail_all(&mut out, &items, "consumer not found");
                    continue;
                };
                let key = (queue.clone(), partition.clone(), group.to_string());
                let Some(claim) = claims.get_mut(&key) else {
                    fail_all(&mut out, &items, "consumer not found");
                    continue;
                };
                // RUSTFIX item 11: the lease is validated ONLY when the ack names
                // one. The facade always names one, which is what makes a stale
                // receipt handle `ReceiptHandleIsInvalid` instead of a silent
                // cursor advance under somebody else's lease.
                if !worker.is_empty()
                    && (claim.worker.as_deref() != Some(worker.as_str()) || !claim.leased_at(now))
                {
                    fail_all(&mut out, &items, "invalid or expired lease");
                    continue;
                }
                let Some(batch_end) = claim.batch_end else {
                    fail_all(&mut out, &items, "no leased batch");
                    continue;
                };
                let empty = Lane::default();
                let lane = logs
                    .get(&(queue.clone(), partition.clone()))
                    .unwrap_or(&empty);

                // Resolve, then decide. Nothing is written until every item of
                // the group has been placed.
                let mut in_span: Vec<(usize, i64, AckStatus)> = Vec::new();
                for &i in &items {
                    // Resolved against the SPAN, never against the lane
                    // ([`Lane::resolve`]): a hash with an in-span occurrence is
                    // acked there even when the same hash also sits below the
                    // cursor, and only a hash with no in-span occurrence at all
                    // falls through to the below-cursor or unresolved arms.
                    match lane.resolve(&acks[i].transaction_id, claim.committed, Some(batch_end)) {
                        (Some(off), _) => in_span.push((i, off, acks[i].status)),
                        (None, true) => match acks[i].status {
                            AckStatus::Completed => {
                                out[i].success = true;
                                out[i].noop = true;
                            }
                            _ => out[i].error = Some("already committed".to_string()),
                        },
                        (None, false) => out[i].error = Some("unresolved".to_string()),
                    }
                }
                if in_span.is_empty() {
                    continue;
                }

                // The lowest explicit signal, with `dlq > failed > retry` at the
                // same offset.
                let signal = in_span
                    .iter()
                    .filter(|(_, _, s)| *s != AckStatus::Completed)
                    .min_by_key(|(_, off, s)| (*off, signal_rank(*s)))
                    .copied();
                let highest_completed = in_span
                    .iter()
                    .filter(|(_, _, s)| *s == AckStatus::Completed)
                    .map(|(_, off, _)| *off)
                    .max();

                let retry_limit = queues.get(&queue).map_or(3, QueueRec::retry_limit);
                let dlq_enabled = queues.get(&queue).is_some_and(QueueRec::dlq_enabled);
                let mut released = false;
                let mut dlq_at: Option<usize> = None;

                match signal {
                    Some((i, off, status)) => {
                        // Everything strictly below the signal is completed;
                        // the signal's own position is not.
                        claim.committed = claim.committed.max(off - 1);
                        match status {
                            AckStatus::Retry => {
                                released = true;
                            }
                            AckStatus::Dlq => {
                                // The lease is KEPT so the broker can decode the
                                // leased segment for the snapshot; the hand-off
                                // advances past it afterwards.
                                dlq_at = Some(i);
                            }
                            AckStatus::Failed => {
                                if claim.batch_retry_count < retry_limit {
                                    claim.batch_retry_count += 1;
                                    released = true;
                                } else if dlq_enabled {
                                    dlq_at = Some(i);
                                } else {
                                    // Budget exhausted on a queue with no DLQ:
                                    // drop the poison and advance past it, which
                                    // is the only way the lane makes progress.
                                    claim.committed = off;
                                    claim.batch_retry_count = 0;
                                    released = true;
                                }
                            }
                            AckStatus::Completed => unreachable!("filtered above"),
                        }
                    }
                    None => {
                        if let Some(top) = highest_completed {
                            claim.committed = claim.committed.max(top);
                        }
                        if claim.committed >= batch_end {
                            // The batch is done: release, and reset the attempt
                            // and retry state — the ONLY branch that does.
                            released = true;
                            claim.batch_retry_count = 0;
                            claim.attempt_offset = None;
                            claim.attempt_count = 0;
                        }
                    }
                }
                if released {
                    claim.worker = None;
                    claim.lease_expires_ms = None;
                    claim.batch_end = None;
                }
                for (i, _, _) in in_span {
                    out[i].success = true;
                    out[i].lease_released = released;
                    out[i].dlq = dlq_at == Some(i);
                }
            }
            out
        }

        /// Apply KV operations to a WORKING copy of the store, with
        /// `kv_apply_v1`'s own rules (024_kv.sql).
        ///
        /// VALIDATE-THEN-APPLY, and all-or-nothing on an escalation: a `required`
        /// precondition that loses raises `check_violation`, which aborts the
        /// TRANSACTION — so the writes that already applied in the same call must
        /// not survive either. The working copy is what makes that true here
        /// rather than merely intended.
        fn apply_kv(&self, ops: &[KvOp], working: &mut Store) -> Result<Vec<KvAnswer>> {
            let cut = *self.kv_read_rows.lock().unwrap();
            let now = self.clock.now_ms();
            let mut answers = Vec::with_capacity(ops.len());
            for (index, op) in ops.iter().enumerate() {
                let answer = match op {
                    KvOp::Get { ns, key } => {
                        let row = working
                            .get(&(ns.clone(), key.clone()))
                            .filter(|r| r.live(now));
                        KvAnswer {
                            key: key.clone(),
                            found: row.is_some(),
                            value: row.map_or(serde_json::Value::Null, |r| r.value.clone()),
                            version: effective_version(
                                working.get(&(ns.clone(), key.clone())),
                                now,
                            ),
                            ..empty_answer(index, "get")
                        }
                    }
                    KvOp::Put {
                        ns,
                        key,
                        value,
                        ttl_seconds,
                        expect,
                        required,
                        ..
                    } => {
                        let at = (ns.clone(), key.clone());
                        let current = working.get(&at);
                        let effective = effective_version(current, now);
                        let (applied, reason) = match expect {
                            None => (true, None),
                            // "Must not exist", and it WINS against an
                            // expired-but-unpruned row.
                            Some(0) => (effective == 0, Some("exists")),
                            // A PURE UPDATE: an `expect: N > 0` on an absent key
                            // creates NOTHING.
                            Some(n) => match effective {
                                0 => (false, Some("absent")),
                                v => (v == *n, Some("version")),
                            },
                        };
                        if applied {
                            let version = self.next_version();
                            working.insert(
                                at,
                                Stored {
                                    value: value.clone(),
                                    version,
                                    expires_ms: ttl_seconds.map(|s| now + (s as i64) * 1000),
                                },
                            );
                            KvAnswer {
                                key: key.clone(),
                                applied: Some(true),
                                version,
                                value: value.clone(),
                                ..empty_answer(index, "put")
                            }
                        } else {
                            if *required {
                                // The whole call, including `working`, is
                                // discarded: nothing was written.
                                return Err(Error::Precondition {
                                    failed_index: index,
                                    reason: reason.unwrap_or_default().to_string(),
                                    version: effective,
                                    value: current
                                        .filter(|r| r.live(now))
                                        .map(|r| r.value.clone())
                                        .unwrap_or(serde_json::Value::Null),
                                });
                            }
                            KvAnswer {
                                key: key.clone(),
                                applied: Some(false),
                                version: effective,
                                reason: reason.map(str::to_string),
                                // An expired row is not a value: the loser sees
                                // the same nothing a reader would.
                                value: current
                                    .filter(|r| r.live(now))
                                    .map(|r| r.value.clone())
                                    .unwrap_or(serde_json::Value::Null),
                                ..empty_answer(index, "put")
                            }
                        }
                    }
                    KvOp::Delete { ns, key, expect } => {
                        let at = (ns.clone(), key.clone());
                        let effective = effective_version(working.get(&at), now);
                        let (applied, reason) = match expect {
                            None => (effective != 0, Some("absent")),
                            Some(0) => (effective == 0, Some("exists")),
                            Some(n) => match effective {
                                0 => (false, Some("absent")),
                                v => (v == *n, Some("version")),
                            },
                        };
                        if applied {
                            working.remove(&at);
                        }
                        KvAnswer {
                            key: key.clone(),
                            applied: Some(applied),
                            version: effective,
                            reason: (!applied).then(|| reason.unwrap_or_default().to_string()),
                            ..empty_answer(index, "delete")
                        }
                    }
                    KvOp::Incr {
                        ns,
                        key,
                        delta,
                        min,
                        max,
                        ttl_seconds,
                        ..
                    } => {
                        let at = (ns.clone(), key.clone());
                        let live = working.get(&at).filter(|r| r.live(now));
                        let typed = live.map(|r| r.value.as_i64());
                        let current = match typed {
                            // A LIVE non-numeric row is the one `type` refusal;
                            // an expired one reads as the effective zero.
                            Some(None) => {
                                answers.push(KvAnswer {
                                    key: key.clone(),
                                    applied: Some(false),
                                    reason: Some("type".to_string()),
                                    version: effective_version(working.get(&at), now),
                                    value: live
                                        .map_or(serde_json::Value::Null, |r| r.value.clone()),
                                    ..empty_answer(index, "incr")
                                });
                                continue;
                            }
                            Some(Some(n)) => n,
                            None => 0,
                        };
                        let next = current + delta;
                        // `max` and `min` do NOT saturate and do NOT truncate:
                        // the call that would break the ceiling does not apply
                        // and returns the CURRENT value, never the would-be one.
                        // With `max`, `applied` IS the admission decision.
                        let ok = min.is_none_or(|m| next >= m) && max.is_none_or(|m| next <= m);
                        if !ok {
                            KvAnswer {
                                key: key.clone(),
                                applied: Some(false),
                                reason: Some("limit".to_string()),
                                version: effective_version(working.get(&at), now),
                                value: serde_json::json!(current),
                                ..empty_answer(index, "incr")
                            }
                        } else {
                            let version = self.next_version();
                            // The TTL is CREATE-ONLY: a live row keeps its
                            // expiry, or a fixed-window limiter on an always-
                            // active client would never close its window.
                            let expires_ms = match live {
                                Some(r) => r.expires_ms,
                                None => ttl_seconds.map(|s| now + (s as i64) * 1000),
                            };
                            working.insert(
                                at,
                                Stored {
                                    value: serde_json::json!(next),
                                    version,
                                    expires_ms,
                                },
                            );
                            KvAnswer {
                                key: key.clone(),
                                applied: Some(true),
                                version,
                                value: serde_json::json!(next),
                                ..empty_answer(index, "incr")
                            }
                        }
                    }
                    KvOp::GetMany { ns, keys } => {
                        let mut rows = Vec::new();
                        let mut missing = Vec::new();
                        let mut truncated = false;
                        // Sorted, because the stored procedure returns rows
                        // ordered by key and spends its byte budget in that
                        // order — which is what decides WHICH keys a truncated
                        // read drops.
                        let mut sorted: Vec<&String> = keys.iter().collect();
                        sorted.sort();
                        for key in sorted {
                            match working
                                .get(&(ns.clone(), key.clone()))
                                .filter(|row| row.live(now))
                            {
                                Some(row) => {
                                    if cut.is_some_and(|c| rows.len() >= c) {
                                        truncated = true;
                                        continue;
                                    }
                                    rows.push(KvRow {
                                        key: key.clone(),
                                        value: row.value.clone(),
                                        version: row.version,
                                    });
                                }
                                None => missing.push(key.clone()),
                            }
                        }
                        KvAnswer {
                            rows,
                            missing,
                            truncated,
                            ..empty_answer(index, "getMany")
                        }
                    }
                    KvOp::GetPrefix {
                        ns,
                        prefix,
                        limit,
                        after,
                    } => {
                        let limit = (*limit).clamp(1, MAX_KV_PREFIX_LIMIT) as usize;
                        let limit = cut.map_or(limit, |c| limit.min(c));
                        let mut rows = Vec::new();
                        let mut truncated = false;
                        for ((row_ns, key), row) in working.iter() {
                            if row_ns != ns || !key.starts_with(prefix) || !row.live(now) {
                                continue;
                            }
                            // Exclusive, and in byte order: the same cursor the
                            // SP implements with `key > after`.
                            if after.as_ref().is_some_and(|a| key <= a) {
                                continue;
                            }
                            if rows.len() == limit {
                                truncated = true;
                                break;
                            }
                            rows.push(KvRow {
                                key: key.clone(),
                                value: row.value.clone(),
                                version: row.version,
                            });
                        }
                        let next_after = truncated
                            .then(|| rows.last().map(|r| r.key.clone()))
                            .flatten();
                        KvAnswer {
                            rows,
                            truncated,
                            next_after,
                            ..empty_answer(index, "getPrefix")
                        }
                    }
                };
                answers.push(answer);
            }
            Ok(answers)
        }
    }

    /// `dlq > failed > retry` at the same offset (005_log_ack.sql).
    fn signal_rank(status: AckStatus) -> u8 {
        match status {
            AckStatus::Dlq => 0,
            AckStatus::Failed => 1,
            AckStatus::Retry => 2,
            AckStatus::Completed => 3,
        }
    }

    fn fail_all(out: &mut [Acked], items: &[usize], why: &str) {
        for &i in items {
            out[i].success = false;
            out[i].error = Some(why.to_string());
        }
    }

    /// A timer payload as the fire path splices it: the stored bytes, read as
    /// JSON when they are JSON and as a string when they are not — which is what
    /// the broker's own lossy-UTF8 splice amounts to for a consumer.
    fn decode_payload(b64: &str) -> serde_json::Value {
        use base64::Engine;
        match base64::engine::general_purpose::STANDARD.decode(b64) {
            Ok(bytes) => serde_json::from_slice(&bytes).unwrap_or_else(|_| {
                serde_json::Value::String(String::from_utf8_lossy(&bytes).into())
            }),
            Err(_) => serde_json::Value::Null,
        }
    }

    /// A result with nothing in it but its identity.
    fn empty_answer(index: usize, op: &str) -> KvAnswer {
        KvAnswer {
            index,
            op: op.to_string(),
            key: String::new(),
            found: false,
            applied: None,
            version: 0,
            reason: None,
            value: serde_json::Value::Null,
            rows: Vec::new(),
            missing: Vec::new(),
            truncated: false,
            next_after: None,
        }
    }

    impl QueenApi for FakeQueen {
        fn list_queues<'a>(&'a self, token: Option<&'a str>) -> BoxFuture<'a, Result<Vec<Queue>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.list_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                let logs = self.logs.lock().unwrap();
                Ok(self
                    .queues
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(name, rec)| Queue {
                        name: name.clone(),
                        // The live lane count, which is what the enriched list
                        // reports: a queue nothing was pushed to has none.
                        partitions: logs.keys().filter(|(q, _)| q == name).count() as i64,
                        id: Some(rec.id.clone()),
                    })
                    .collect())
            })
        }

        fn configure_queue<'a>(
            &'a self,
            name: &'a str,
            options: &'a serde_json::Value,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<()>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                // Recorded BEFORE the failure check, so a test can assert that a
                // create was attempted and still refused.
                self.configures
                    .lock()
                    .unwrap()
                    .push((name.to_string(), options.clone()));
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                // The UPSERT: an existing queue's config columns are REWRITTEN
                // to what this bag says, which is the whole reason the facade may
                // not call it with a partial one.
                let id = self
                    .queues
                    .lock()
                    .unwrap()
                    .get(name)
                    .map(|q| q.id.clone())
                    .unwrap_or_else(|| format!("queue-{:08}", self.next()));
                self.queues.lock().unwrap().insert(
                    name.to_string(),
                    QueueRec {
                        id,
                        options: options.clone(),
                    },
                );
                Ok(())
            })
        }

        fn delete_queue<'a>(
            &'a self,
            name: &'a str,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Deleted>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.deletes.lock().unwrap().push(name.to_string());
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                let existed = self.queues.lock().unwrap().remove(name).is_some();
                if existed {
                    // The log and the cursors go with the queue, as they do in
                    // Queen: the delete cascades through log_partitions.
                    self.logs.lock().unwrap().retain(|(q, _), _| q != name);
                    self.claims.lock().unwrap().retain(|(q, _, _), _| q != name);
                    // THE TIMERS DO NOT GO WITH IT, and that is the schema and
                    // not an omission here: `queen.log_timers` is keyed
                    // `PRIMARY KEY (tenant_id, queue, timer_key)` on the queue's
                    // NAME with no foreign key at all (025_log_timers.sql:197),
                    // so the cascade off `queen.queues(id)` cannot reach it.
                    // 031_tenant_purge.sql says what that means in its own
                    // words: "a surviving timer keeps producing after the tenant
                    // is 403'd, into a queue this purge just deleted, which push
                    // then AUTO-CREATES". A double that swept them here would
                    // hide exactly that from `PurgeQueue`.
                    // AND SO DO THE PARTITION IDS. `queen.log_partitions.id` is
                    // a `gen_random_uuid()` primary key hanging off
                    // `queen.queues(id) ON DELETE CASCADE` (001_log_schema.sql),
                    // and `delete_queue_v1` deletes the queue row — so a lane of
                    // the same name in a queue recreated afterwards is a NEW
                    // row with a NEW id, never the old one. A double that kept
                    // the mapping would let a receipt handle minted before a
                    // PurgeQueue address a partition that no longer exists.
                    let mut pids = self.pids.lock().unwrap();
                    pids.0.retain(|(q, _), _| q != name);
                    pids.1.retain(|_, (q, _)| q != name);
                }
                Ok(Deleted { existed })
            })
        }

        fn queue_depth<'a>(
            &'a self,
            queue: &'a str,
            group: Option<&'a str>,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Depth>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                // The route's own 404, which is this facade's existence probe.
                if !self.queues.lock().unwrap().contains_key(queue) {
                    return Err(Error::status(404, "{\"error\":\"Queue not found\"}"));
                }
                self.fire_due();
                let now = self.clock.now_ms();
                let logs = self.logs.lock().unwrap();
                let claims = self.claims.lock().unwrap();
                let mut depth = Depth {
                    queue: queue.to_string(),
                    group: group.map(str::to_string),
                    pending: 0,
                    processing: 0,
                    ready: 0,
                    partitions_pending: 0,
                    partitions_ready: 0,
                    partitions: Vec::new(),
                };
                for ((q, p), lane) in logs.iter() {
                    if q != queue {
                        continue;
                    }
                    // A NAMED group is that group's own backlog. `None` is the
                    // QUEUE-LEVEL number, which is not the same thing and not
                    // the queue-mode group's either: the stored procedure gives
                    // named groups precedence AS A CLASS (`MIN(committed) FILTER
                    // (consumer_group <> '__QUEUE_MODE__')`, with queue mode as
                    // the fallback only when no named cursor exists) and sums
                    // every live lease of every group into `processing`. A queue
                    // a native consumer also reads therefore answers that
                    // consumer's backlog, not the SQS queue's — which is what an
                    // autoscaler would be handed.
                    // 011_log_stats.sql, log_queue_depth_v1.
                    let lane_claims: Vec<&Claim> = claims
                        .iter()
                        .filter(|((cq, cp, _), _)| cq == q && cp == p)
                        .map(|(_, claim)| claim)
                        .collect();
                    let named: Vec<&Claim> = claims
                        .iter()
                        .filter(|((cq, cp, cg), _)| cq == q && cp == p && cg != QUEUE_MODE_GROUP)
                        .map(|(_, claim)| claim)
                        .collect();
                    let claim = match group {
                        Some(g) => claims.get(&(q.clone(), p.clone(), g.to_string())),
                        None if !named.is_empty() => {
                            named.iter().copied().min_by_key(|c| c.committed)
                        }
                        None => claims.get(&(q.clone(), p.clone(), QUEUE_MODE_GROUP.to_string())),
                    };
                    let committed = claim.map_or(lane.start - 1, |c| c.committed);
                    let pending = (lane.high() - 1 - committed).max(0);
                    let leased = match group {
                        Some(_) => claim
                            .filter(|c| c.leased_at(now))
                            .and_then(|c| c.batch_end)
                            .map_or(0, |end| (end - committed).max(0)),
                        // Queue level sums every group's live lease.
                        None => lane_claims
                            .iter()
                            .filter(|c| c.leased_at(now))
                            .map(|c| c.batch_end.map_or(0, |end| (end - c.committed).max(0)))
                            .sum(),
                    };
                    let processing = pending.min(leased);
                    let ready = pending - processing;
                    depth.pending += pending;
                    depth.processing += processing;
                    depth.ready += ready;
                    depth.partitions_pending += i64::from(pending > 0);
                    depth.partitions_ready += i64::from(ready > 0);
                    depth.partitions.push(PartitionDepth {
                        partition: p.clone(),
                        pending,
                        processing,
                        ready,
                    });
                }
                Ok(depth)
            })
        }

        fn push<'a>(
            &'a self,
            items: &'a [PushItem],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Pushed>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.pushes.lock().unwrap().push(items.to_vec());
                if let Some(e) = self.push_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                let mut logs = self.logs.lock().unwrap();
                let mut statuses = self.push_statuses.lock().unwrap();
                Ok(items
                    .iter()
                    .map(|it| match statuses.pop_front() {
                        None => self.push_into(&mut logs, it),
                        Some(status) if matches!(status.as_str(), "queued" | "duplicate") => {
                            Pushed {
                                status,
                                ..self.push_into(&mut logs, it)
                            }
                        }
                        // A status the log did not take: nothing is written and
                        // no offset is allocated, which is what `error`,
                        // `failed` and `buffered` each mean on the real wire.
                        Some(status) => Pushed {
                            status,
                            message_id: self.mint("msg"),
                            transaction_id: it
                                .transaction_id
                                .clone()
                                .unwrap_or_else(|| self.mint("txn")),
                            offset: None,
                        },
                    })
                    .collect())
            })
        }

        fn pop_queue<'a>(
            &'a self,
            queue: &'a str,
            opts: &'a PopOptions,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Popped>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.pops
                    .lock()
                    .unwrap()
                    .push((queue.to_string(), None, opts.clone()));
                if let Some(e) = self.pop_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                self.fire_due();
                let logs = self.logs.lock().unwrap();
                let mut claims = self.claims.lock().unwrap();
                Ok(self.claim(queue, None, opts, &logs, &mut claims))
            })
        }

        fn pop_partition<'a>(
            &'a self,
            queue: &'a str,
            partition: &'a str,
            opts: &'a PopOptions,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Popped>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.pops.lock().unwrap().push((
                    queue.to_string(),
                    Some(partition.to_string()),
                    opts.clone(),
                ));
                if let Some(e) = self.pop_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                self.fire_due();
                let logs = self.logs.lock().unwrap();
                let mut claims = self.claims.lock().unwrap();
                Ok(self.claim(queue, Some(partition), opts, &logs, &mut claims))
            })
        }

        fn ack<'a>(
            &'a self,
            ack: &'a AckItem,
            group: Option<&'a str>,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Acked>> {
            Box::pin(async move {
                let one = [ack.clone()];
                let mut answers = self.ack_batch(&one, group, token).await?;
                Ok(answers.remove(0))
            })
        }

        fn ack_batch<'a>(
            &'a self,
            acks: &'a [AckItem],
            group: Option<&'a str>,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<Acked>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.acks.lock().unwrap().push(acks.to_vec());
                if let Some(e) = self.ack_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                let logs = self.logs.lock().unwrap();
                let mut claims = self.claims.lock().unwrap();
                Ok(self.apply_acks(group.unwrap_or(QUEUE_MODE_GROUP), acks, &logs, &mut claims))
            })
        }

        fn lease_extend<'a>(
            &'a self,
            lease_id: &'a str,
            seconds: i64,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<LeaseExtended>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.extends
                    .lock()
                    .unwrap()
                    .push((lease_id.to_string(), seconds));
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                let now = self.clock.now_ms();
                let until = now + seconds * 1000;
                let mut renewed = 0i64;
                for claim in self.claims.lock().unwrap().values_mut() {
                    // LIVE leases only, and GREATEST: a renewal never shortens a
                    // lease and never resurrects an expired one — which is what
                    // makes ChangeMessageVisibility on an already-redelivered
                    // message answer MessageNotInflight instead of stealing it
                    // back.
                    if claim.worker.as_deref() == Some(lease_id) && claim.leased_at(now) {
                        claim.lease_expires_ms =
                            Some(claim.lease_expires_ms.unwrap_or(now).max(until));
                        renewed += 1;
                    }
                }
                Ok(LeaseExtended {
                    lease_id: lease_id.to_string(),
                    success: renewed > 0,
                    renewed,
                    expires_at: (renewed > 0).then(|| iso_from_epoch_ms(until)),
                })
            })
        }

        fn kv<'a>(
            &'a self,
            ops: &'a [KvOp],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<KvAnswer>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.kv_calls.lock().unwrap().push(ops.to_vec());
                if let Some(e) = self.kv_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                // The two ceilings the broker refuses the WHOLE batch over. A
                // double that accepted them would let a caller ship a batch the
                // broker answers 400 to.
                if ops.len() > MAX_KV_OPS_PER_CALL {
                    return Err(Error::status(400, format!("{} ops in one call", ops.len())));
                }
                let keys: usize = ops.iter().map(KvOp::keys).sum();
                if keys > MAX_KV_KEYS_PER_CALL {
                    return Err(Error::status(400, format!("{keys} keys in one call")));
                }
                let interposed = self.kv_interpose.lock().unwrap().pop_front().flatten();
                let mut kv = self.kv.lock().unwrap();
                if let Some(op) = interposed {
                    let mut other = kv.clone();
                    self.apply_kv(std::slice::from_ref(&op), &mut other)?;
                    *kv = other;
                }
                let mut working = kv.clone();
                let answers = self.apply_kv(ops, &mut working)?;
                *kv = working;
                Ok(answers)
            })
        }

        fn timers_schedule<'a>(
            &'a self,
            ops: &'a [TimerSchedule],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<Vec<TimerResult>>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.timer_calls.lock().unwrap().push(ops.to_vec());
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                let now = self.clock.now_ms();
                let mut timers = self.timers.lock().unwrap();
                Ok(ops
                    .iter()
                    .map(|op| {
                        let key = (op.queue.clone(), op.timer_key.clone());
                        // A second schedule under the same key OVERWRITES the
                        // pending one, which is what makes a retry after a crash
                        // safe by construction.
                        let existed = timers.contains_key(&key);
                        let message_id = self.mint("msg");
                        let deliver_at_ms = now + op.delay_ms;
                        timers.insert(
                            key,
                            Timer {
                                partition: op.partition.clone().unwrap_or_else(|| "Default".into()),
                                deliver_at_ms,
                                txn: op.txn.clone(),
                                payload_b64: op.payload.clone(),
                                message_id: message_id.clone(),
                            },
                        );
                        TimerResult {
                            ok: true,
                            status: if existed {
                                TimerStatus::Rescheduled
                            } else {
                                TimerStatus::Scheduled
                            },
                            queue: op.queue.clone(),
                            timer_key: op.timer_key.clone(),
                            txn: Some(op.txn.clone()),
                            message_id: Some(message_id),
                            deliver_at: Some(iso_from_epoch_ms(deliver_at_ms)),
                        }
                    })
                    .collect())
            })
        }

        fn timers_list<'a>(
            &'a self,
            queue: &'a str,
            after: Option<&'a str>,
            limit: i64,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<TimerPage>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                self.fire_due();
                let limit = limit.clamp(1, 1000) as usize;
                let timers = self.timers.lock().unwrap();
                let mut rows = Vec::new();
                let mut truncated = false;
                for ((q, key), t) in timers.iter() {
                    if q != queue || after.is_some_and(|a| key.as_str() <= a) {
                        continue;
                    }
                    if rows.len() == limit {
                        truncated = true;
                        break;
                    }
                    rows.push(TimerRow {
                        queue: q.clone(),
                        timer_key: key.clone(),
                        partition: Some(t.partition.clone()),
                        deliver_at: Some(iso_from_epoch_ms(t.deliver_at_ms)),
                        txn: Some(t.txn.clone()),
                        message_id: Some(t.message_id.clone()),
                        attempts: 0,
                    });
                }
                let next_after = truncated
                    .then(|| rows.last().map(|r| r.timer_key.clone()))
                    .flatten();
                Ok(TimerPage {
                    rows,
                    truncated,
                    next_after,
                })
            })
        }

        fn timers_count<'a>(
            &'a self,
            queue: &'a str,
            prefix: &'a str,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<i64>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                // The broker REFUSES an unprefixed count — `mode=count requires
                // a non-empty prefix`, in the handler and again in the stored
                // procedure — and a double that answered one would let a caller
                // ship a query the real broker 400s on every time.
                if prefix.is_empty() {
                    return Err(Error::status(
                        400,
                        "{\"error\":\"timers_count_prefix_required: mode=count requires a \
                         non-empty prefix\"}",
                    ));
                }
                self.fire_due();
                Ok(self
                    .timers
                    .lock()
                    .unwrap()
                    .keys()
                    .filter(|(q, key)| q == queue && key.starts_with(prefix))
                    .count() as i64)
            })
        }

        fn timers_cancel<'a>(
            &'a self,
            queue: &'a str,
            timer_key: &'a str,
            txn: Option<&'a str>,
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<TimerResult>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                self.fire_due();
                let removed = self
                    .timers
                    .lock()
                    .unwrap()
                    .remove(&(queue.to_string(), timer_key.to_string()));
                Ok(match removed {
                    Some(t) => TimerResult {
                        ok: true,
                        status: TimerStatus::Cancelled,
                        queue: queue.to_string(),
                        timer_key: timer_key.to_string(),
                        txn: Some(t.txn),
                        message_id: Some(t.message_id),
                        deliver_at: None,
                    },
                    // THERE IS NO TOMBSTONE: `absent` means "no longer pending"
                    // and MAY MEAN ALREADY DELIVERED, which is why the caller's
                    // own txn is echoed back — the destination queue is the
                    // authority, and this saves the round trip to ask it.
                    None => TimerResult {
                        ok: false,
                        status: TimerStatus::Absent,
                        queue: queue.to_string(),
                        timer_key: timer_key.to_string(),
                        txn: txn.map(str::to_string),
                        message_id: None,
                        deliver_at: None,
                    },
                })
            })
        }

        /// One atomic bundle, with the property the redrive move rests on: the
        /// KV rider is applied FIRST, the acks and the records land only if it
        /// did, and a `required` precondition that loses returns `Err` having
        /// written none of the three.
        ///
        /// The scripted failure is taken BEFORE any of it, and that is the one
        /// place this differs from a broker: a failure set mid-bundle would
        /// otherwise leave a half-applied state the stored procedure cannot
        /// produce and no test should be able to script.
        fn transaction<'a>(
            &'a self,
            pushes: &'a [PushItem],
            acks: &'a [TxnAck],
            kv: &'a [KvOp],
            token: Option<&'a str>,
        ) -> BoxFuture<'a, Result<TxnOutcome>> {
            Box::pin(async move {
                self.tokens.lock().unwrap().push(token.map(str::to_string));
                self.transactions.lock().unwrap().push((
                    pushes.to_vec(),
                    acks.to_vec(),
                    kv.to_vec(),
                ));
                if let Some(e) = self.transaction_error.lock().unwrap().take() {
                    return Err(e);
                }
                if let Some(e) = self.scripted() {
                    return Err(e);
                }
                // The WIRE's ceilings, tighter than `/api/v1/kv`'s and refused
                // with the whole bundle (server/src/handlers/data.rs).
                if kv.len() > WIRE_KV_MAX_OPS {
                    return Err(Error::status(
                        400,
                        format!("{} kv operations in one transaction", kv.len()),
                    ));
                }
                let keys: usize = kv.iter().map(KvOp::keys).sum();
                if keys > WIRE_KV_MAX_KEYS {
                    return Err(Error::status(
                        400,
                        format!("{keys} kv keys in one transaction"),
                    ));
                }
                let mut store = self.kv.lock().unwrap();
                let mut logs = self.logs.lock().unwrap();
                let mut claims = self.claims.lock().unwrap();
                let mut working_kv = store.clone();
                let kv_answers = self.apply_kv(kv, &mut working_kv)?;

                let mut working_logs = logs.clone();
                let mut working_claims = claims.clone();
                let mut out = TxnOutcome {
                    kv: kv_answers,
                    ..TxnOutcome::default()
                };
                for (i, it) in pushes.iter().enumerate() {
                    let pushed = self.push_into(&mut working_logs, it);
                    // A duplicate is a SOFT verdict on `/api/v1/push` and a HARD
                    // one here: the stored procedure RAISEs and the whole bundle
                    // rolls back, including the pushes above it
                    // (005_log_ack.sql). A double that answered `duplicate:true`
                    // and committed would agree with a facade that got the SNS
                    // FIFO republish wrong.
                    if pushed.status == "duplicate" {
                        return Err(Error::Duplicate(format!(
                            "QDUP duplicate messages in queue \"{}\" partition \"{}\"; \
                             transaction rolled back",
                            it.queue, it.partition
                        )));
                    }
                    out.pushes.push(TxnPushEcho {
                        index: i,
                        transaction_id: pushed.transaction_id,
                        message_id: pushed.message_id,
                        queue: it.queue.clone(),
                        duplicate: false,
                    });
                }
                // A rejected ack RAISEs and rolls the bundle back — it is not a
                // per-item verdict the way `/api/v1/ack` gives one. That is the
                // property the move depends on: the original is either acked
                // with the copy written, or neither happened.
                // The flat ordinal an ack echo carries: the broker advances the
                // index once per push ITEM, not once per push operation
                // ([`flat_ops`]), and a double that counted the batch as one
                // would agree with a facade that got it wrong.
                let base = pushes.len();
                for (i, a) in acks.iter().enumerate() {
                    let item = AckItem {
                        transaction_id: a.transaction_id.clone(),
                        partition_id: a.partition_id.clone(),
                        status: a.status,
                        lease_id: a.lease_id.clone(),
                        error: a.error.clone(),
                    };
                    let group = a.consumer_group.as_deref().unwrap_or(QUEUE_MODE_GROUP);
                    let answered = self.apply_acks(
                        group,
                        std::slice::from_ref(&item),
                        &working_logs,
                        &mut working_claims,
                    );
                    let answer = &answered[0];
                    if !answer.success {
                        return Err(Error::Body(format!(
                            "the transaction rolled back: ack {i} was rejected ({})",
                            answer.error.clone().unwrap_or_default()
                        )));
                    }
                    out.acks.push(TxnAckEcho {
                        index: base + i,
                        transaction_id: a.transaction_id.clone(),
                        dlq: answer.dlq,
                    });
                }
                *store = working_kv;
                *logs = working_logs;
                *claims = working_claims;
                Ok(out)
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::testing::FakeQueen;
    use super::*;
    use std::time::Duration;

    // ---------------------------------------------------------------- parsing
    //
    // Every body below is the one the broker's own renderer writes, unknown keys
    // and all: the facade must ignore what it does not read rather than refuse
    // it, or a dashboard-driven addition to a response breaks message delivery.

    const REAL_LIST_BODY: &str = r#"{
      "queues": [
        {"id":"0d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a11","name":"orders","namespace":"","task":"",
         "createdAt":"2026-08-30T10:00:00.000Z","partitions":64,"retainedBytes":4096,
         "segments":{"segments":3,"messages":900},
         "messages":{"total":900,"pending":10,"processing":2}},
        {"id":"1d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a12","name":"orders-dlq","namespace":"","task":"",
         "createdAt":"2026-08-30T10:00:01.000Z","partitions":0,"retainedBytes":0,
         "messages":{"total":0,"pending":0,"processing":0}}
      ],
      "kvBytes": 0, "timerBytes": 0
    }"#;

    #[test]
    fn the_queue_list_body_parses_to_names_ids_and_lane_counts() {
        let parsed: QueueListBody = serde_json::from_str(REAL_LIST_BODY).unwrap();
        assert_eq!(
            parsed.queues,
            vec![
                Queue {
                    name: "orders".into(),
                    partitions: 64,
                    id: Some("0d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a11".into()),
                },
                Queue {
                    name: "orders-dlq".into(),
                    partitions: 0,
                    id: Some("1d5a1e9c-1f7f-4f2f-9a02-6b6b1f0b1a12".into()),
                },
            ]
        );
    }

    /// `render_push_results`: one object per item, in item order, each stamped
    /// with its index — and a duplicate carrying the PRE-EXISTING message's id
    /// and offset, which is what SQS answers a repeated
    /// `MessageDeduplicationId` with.
    const REAL_PUSH_BODY: &str = r#"[
      {"index":0,"message_id":"11111111-1111-4111-8111-111111111111","transaction_id":"t-1",
       "queueName":"orders","status":"queued","offset":41},
      {"index":1,"message_id":"22222222-2222-4222-8222-222222222222","transaction_id":"dedup-9",
       "queueName":"orders","status":"duplicate","offset":7},
      {"index":2,"message_id":"33333333-3333-4333-8333-333333333333","transaction_id":"t-3",
       "queueName":"orders","status":"buffered"}
    ]"#;

    #[test]
    fn the_push_body_parses_to_ids_offsets_and_statuses() {
        let got = align_push_results(REAL_PUSH_BODY, 3).unwrap();
        assert_eq!(got[0].status, "queued");
        assert_eq!(got[0].message_id, "11111111-1111-4111-8111-111111111111");
        assert_eq!(got[0].offset, Some(41));
        // A duplicate is not an error: it is the dedup window doing its job, and
        // the SequenceNumber it answers is the original's.
        assert_eq!(got[1].status, "duplicate");
        assert_eq!(got[1].offset, Some(7));
        assert_eq!(got[1].transaction_id, "dedup-9");
        // A spooled item allocated nothing and must not pretend to.
        assert_eq!(got[2].status, "buffered");
        assert_eq!(got[2].offset, None);
    }

    #[test]
    fn a_push_answer_that_does_not_cover_every_item_is_refused() {
        let short = r#"[{"index":0,"message_id":"m","transaction_id":"t","status":"queued"}]"#;
        assert!(matches!(align_push_results(short, 2), Err(Error::Body(_))));
        let twice = r#"[{"index":0,"status":"queued"},{"index":0,"status":"queued"}]"#;
        assert!(matches!(align_push_results(twice, 1), Err(Error::Body(_))));
        let out_of_range = r#"[{"index":5,"status":"queued"}]"#;
        assert!(matches!(
            align_push_results(out_of_range, 1),
            Err(Error::Body(_))
        ));
    }

    /// `render_pop_parts`, verbatim: the lease envelope, then one message
    /// carrying every field an ack and a receipt handle need.
    const REAL_POP_BODY: &str = r#"{"success":true,"queue":"orders","partition":"3",
      "partitionId":"7c1e0e42-0c2f-4a1b-9a3e-8f8c1b2d3e4f","leaseId":"01921f0a-worker",
      "consumerGroup":"__QUEUE_MODE__","messages":[
      {"id":"11111111-1111-4111-8111-111111111111","transactionId":"t-1","traceId":null,
       "data":{"b":"hello"},"producerSub":null,"createdAt":"2026-08-30T10:00:00.123456Z",
       "partitionId":"7c1e0e42-0c2f-4a1b-9a3e-8f8c1b2d3e4f","partition":"3",
       "leaseId":"01921f0a-worker","consumerGroup":"__QUEUE_MODE__","deliveryAttempt":2,
       "offset":41}
      ],"partitionsClaimed":1}"#;

    #[test]
    fn the_pop_body_parses_to_a_lease_and_its_messages() {
        let got: Popped = serde_json::from_str(REAL_POP_BODY).unwrap();
        assert_eq!(got.lease_id, "01921f0a-worker");
        assert_eq!(got.partition_id, "7c1e0e42-0c2f-4a1b-9a3e-8f8c1b2d3e4f");
        assert_eq!(got.consumer_group, QUEUE_MODE_GROUP);
        assert_eq!(got.partitions_claimed, 1);
        let m = &got.messages[0];
        assert_eq!(m.transaction_id, "t-1");
        assert_eq!(m.data, serde_json::json!({"b":"hello"}));
        // ApproximateReceiveCount. Exact at claim width 1.
        assert_eq!(m.delivery_attempt, 2);
        assert_eq!(m.created_at, "2026-08-30T10:00:00.123456Z");
        // C-SQS-3: the absolute offset, which is the FIFO SequenceNumber.
        assert_eq!(m.offset, Some(41));
    }

    /// A broker that predates C-SQS-3 sends the same body with no `offset` key,
    /// and this facade must serve that receive — one field lighter, not failed.
    /// The two shapes differ by exactly that key, which is what makes this an
    /// assertion about tolerance rather than about a hand-written fixture.
    #[test]
    fn a_pop_body_without_an_offset_still_parses() {
        let old = REAL_POP_BODY.replace(",\n       \"offset\":41", "");
        assert!(!old.contains("offset"), "the key really is gone: {old}");
        let got: Popped =
            serde_json::from_str(&old).expect("a pre-C-SQS-3 pop is not a protocol error");
        assert_eq!(got.messages.len(), 1);
        assert_eq!(got.messages[0].transaction_id, "t-1");
        // Absent, never zero: 0 is a real offset — the first message of a lane.
        assert_eq!(got.messages[0].offset, None);
    }

    /// THE COMMONEST ANSWER ON THIS WIRE HAS NO BODY. An empty pop is a
    /// bodiless `204` — the broker drops the body on the way out — and so is a
    /// pop taken while pop maintenance is on. Parsing zero bytes as JSON turned
    /// every empty `ReceiveMessage`, and every receive asking for more messages
    /// than the queue has occupied lanes, into an InternalFailure 500.
    #[test]
    fn an_empty_pop_has_no_body_at_all() {
        let got = parse_popped("", "orders").expect("a bodiless 204 is an empty claim");
        assert!(got.messages.is_empty());
        assert_eq!(got.queue, "orders");
        assert_eq!(got.partition, "");
        assert_eq!(got.lease_id, "");
        assert_eq!(got.partitions_claimed, 0);
        // Whitespace is the same nothing.
        assert!(parse_popped("\n", "orders").unwrap().messages.is_empty());

        // A body that IS there still parses, and a pop that claimed nothing but
        // answered 200 (the conflation shape) names no partition either.
        let body = r#"{"success":true,"queue":"orders","partition":"","partitionId":"",
          "leaseId":"w-2","consumerGroup":"__QUEUE_MODE__","messages":[],"partitionsClaimed":0}"#;
        let got = parse_popped(body, "orders").unwrap();
        assert!(got.messages.is_empty());
        assert_eq!(got.partition, "");
        assert_eq!(got.lease_id, "w-2");
        // ...and a body that is neither is still a failure.
        assert!(matches!(
            parse_popped("not json", "orders"),
            Err(Error::Body(_))
        ));
    }

    const REAL_ACK_BODY: &str = r#"[
      {"index":0,"transactionId":"t-1","success":true,"error":null,"leaseReleased":true,
       "dlq":false,"noop":false},
      {"index":1,"transactionId":"t-2","success":false,"error":"invalid or expired lease",
       "leaseReleased":false,"dlq":false,"noop":false},
      {"index":2,"transactionId":"t-3","success":true,"error":null,"leaseReleased":false,
       "dlq":false,"noop":true}
    ]"#;

    #[test]
    fn the_ack_body_parses_to_per_item_verdicts() {
        let got = align_ack_results(REAL_ACK_BODY, 3).unwrap();
        assert!(got[0].success && got[0].lease_released);
        // This one is `ReceiptHandleIsInvalid` on the SQS side, and only the
        // string says so.
        assert!(!got[1].success);
        assert_eq!(got[1].error.as_deref(), Some("invalid or expired lease"));
        // A second DeleteMessage for the same message: harmless, and success.
        assert!(got[2].success && got[2].noop);
    }

    #[test]
    fn an_ack_answer_attributed_to_the_wrong_entry_is_refused() {
        let twice = r#"[{"index":0,"success":true},{"index":0,"success":true}]"#;
        assert!(matches!(align_ack_results(twice, 2), Err(Error::Body(_))));
    }

    #[test]
    fn the_lease_extend_body_parses_the_canonical_key() {
        let body = r#"{"leaseId":"w-1","success":true,"renewed":1,
          "newExpiresAt":"2026-08-30T10:01:00.000Z","expiresAt":"2026-08-30T10:01:00.000Z",
          "lease_expires_at":"2026-08-30T10:01:00.000Z"}"#;
        let got: LeaseExtended = serde_json::from_str(body).unwrap();
        assert!(got.success);
        assert_eq!(got.renewed, 1);
        assert_eq!(got.expires_at.as_deref(), Some("2026-08-30T10:01:00.000Z"));

        // The route is ALWAYS 200: nothing renewed is how it says the lease is
        // gone, which is MessageNotInflight and not a transport failure.
        let none = r#"{"leaseId":"w-1","success":false,"renewed":0,"newExpiresAt":null,
          "expiresAt":null,"lease_expires_at":null}"#;
        let got: LeaseExtended = serde_json::from_str(none).unwrap();
        assert_eq!(got.renewed, 0);
        assert_eq!(got.expires_at, None);
    }

    #[test]
    fn the_depth_body_parses_the_two_attributes_autoscalers_read() {
        let body = r#"{"queue":"orders","group":"__QUEUE_MODE__","pending":12,"processing":3,
          "ready":9,"partitionsPending":2,"partitionsReady":2,"conflation":false,
          "effectivePending":12,"effectiveReady":9,
          "partitions":[{"partition":"0","pending":7,"processing":1,"ready":6},
                        {"partition":"1","pending":5,"processing":2,"ready":3}]}"#;
        let got: Depth = serde_json::from_str(body).unwrap();
        assert_eq!(got.pending, 12); // ApproximateNumberOfMessages
        assert_eq!(got.processing, 3); // ApproximateNumberOfMessagesNotVisible
        assert_eq!(got.partitions.len(), 2);
        assert_eq!(got.partitions[1].partition, "1");
    }

    #[test]
    fn the_kv_body_parses_and_a_lost_precondition_is_not_a_success() {
        let ok = r#"{"results":[
          {"index":1,"op":"get","found":true,"key":"qs:q:orders","value":{"n":1},"version":1007},
          {"index":0,"op":"put","applied":false,"reason":"exists","key":"qs:q:orders",
           "value":{"n":1},"version":1007}]}"#;
        let got = align_kv_results(ok, 2).unwrap();
        // Aligned by the stamped index, NOT by position: the stored procedure
        // applies writes in key order and reports them by input ordinal.
        assert_eq!(got[0].op, "put");
        assert!(!got[0].applied());
        assert_eq!(got[0].reason.as_deref(), Some("exists"));
        assert!(got[1].found);
        assert_eq!(got[1].value, serde_json::json!({"n":1}));

        // HTTP 200, and it wrote nothing.
        let lost = r#"{"ok":false,"reason":"kv_precondition","failedIndex":1,"kvReason":"version",
          "version":1009,"value":{"n":2}}"#;
        match align_kv_results(lost, 2) {
            Err(Error::Precondition {
                failed_index,
                reason,
                version,
                value,
            }) => {
                assert_eq!(failed_index, 1);
                assert_eq!(reason, "version");
                assert_eq!(version, 1009);
                assert_eq!(value, serde_json::json!({"n":2}));
            }
            other => panic!("expected a precondition verdict, got {other:?}"),
        }
    }

    /// The redrive move's own answer: one push echo, one ack echo, one KV rider,
    /// scattered into the flat operation space and demuxed back by `type`.
    const REAL_TXN_BODY: &str = r#"{"transactionId":"tx-1","success":true,"results":[
      {"index":0,"type":"push","success":true,"transactionId":"t-move",
       "messageId":"99999999-9999-4999-8999-999999999999","queueName":"orders-dlq"},
      {"index":1,"type":"ack","success":true,"transactionId":"t-orig","error":null,"dlq":false},
      {"index":2,"opIndex":0,"type":"kv","op":"put","applied":true,"key":"qs:mv:job-1",
       "version":1042}
    ]}"#;

    #[test]
    fn the_transaction_body_scatters_back_into_three_arrays() {
        let got = align_transaction_results(REAL_TXN_BODY, 1, 1, 1).unwrap();
        assert_eq!(got.pushes.len(), 1);
        assert_eq!(
            got.pushes[0].message_id,
            "99999999-9999-4999-8999-999999999999"
        );
        assert_eq!(got.pushes[0].queue, "orders-dlq");
        assert_eq!(got.acks.len(), 1);
        assert_eq!(got.acks[0].transaction_id, "t-orig");
        assert_eq!(got.kv.len(), 1);
        assert_eq!(got.kv[0].index, 0);
        assert!(got.kv[0].applied());
    }

    /// A broker whose wire procedure has no KV leg answers short, and reading
    /// that as a successful gate is the silent misalignment the count guard
    /// exists for.
    #[test]
    fn a_transaction_that_answers_no_rider_is_an_error() {
        let body = r#"{"transactionId":"tx-1","success":true,"results":[
          {"index":0,"type":"push","success":true,"transactionId":"t","messageId":"m",
           "queueName":"q"}]}"#;
        assert!(matches!(
            align_transaction_results(body, 1, 0, 1),
            Err(Error::Body(_))
        ));
    }

    /// `success:false` is HTTP 200 on purpose, and for the move it is the whole
    /// safety property: read as committed, the original would have been deleted
    /// with nothing written to the DLQ.
    #[test]
    fn a_fenced_transaction_reports_the_verdict_in_the_callers_own_index_space() {
        let body = r#"{"transactionId":"tx-1","success":false,"reason":"kv_precondition",
          "error":"QKV precondition","failedIndex":4,"kvReason":"version","version":77,
          "value":{"held":"by-someone-else"},"results":[]}"#;
        // Flat space: ONE ORDINAL PER PUSH ITEM (0, 1) — the broker walks the
        // items inside the single push operation — plus one per ack (2, 3), so
        // the kv rider starts at 4 and the caller's kv[0] is what lost. Counting
        // the batch as one operation would read this verdict as kv[2], which is
        // an index the caller never sent.
        match align_transaction_results(body, 2, 2, 1) {
            Err(Error::Precondition {
                failed_index,
                version,
                ..
            }) => {
                assert_eq!(failed_index, 0);
                assert_eq!(version, 77);
            }
            other => panic!("expected a precondition verdict, got {other:?}"),
        }
    }

    #[test]
    fn the_timer_results_parse_including_the_verdicts() {
        let body = r#"{"results":[
          {"ok":true,"status":"scheduled","queue":"orders","timerKey":"m-1","txn":"t-1",
           "messageId":"m-1","deliverAt":"2026-08-30T10:05:00.000Z"},
          {"ok":false,"status":"absent","queue":"orders","timerKey":"m-2","txn":"t-2"}
        ]}"#;
        let parsed: TimerResponseBody = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.results[0].status, TimerStatus::Scheduled);
        assert!(parsed.results[0].ok);
        // `absent` MAY MEAN ALREADY DELIVERED, and the echoed txn is what
        // answers that against the destination queue.
        assert_eq!(parsed.results[1].status, TimerStatus::Absent);
        assert_eq!(parsed.results[1].txn.as_deref(), Some("t-2"));

        // A status this build has not been taught is a log line, not a parse
        // failure.
        let odd = r#"{"results":[{"ok":true,"status":"quantum","queue":"q","timerKey":"k"}]}"#;
        let parsed: TimerResponseBody = serde_json::from_str(odd).unwrap();
        assert_eq!(parsed.results[0].status, TimerStatus::Unknown);
    }

    #[test]
    fn a_queen_url_is_checked_at_boot() {
        assert_eq!(
            normalize_base_url("http://queen:6789/").unwrap(),
            "http://queen:6789"
        );
        assert!(normalize_base_url("ws://queen:6789").is_err());
        assert!(normalize_base_url("not a url").is_err());
    }

    /// A queue name is tame; a MessageGroupId and a timer key are not, and both
    /// travel as path segments.
    #[test]
    fn a_path_segment_cannot_change_which_resource_is_addressed() {
        assert_eq!(encode_segment("orders.fifo"), "orders.fifo");
        assert_eq!(encode_segment("../../admin"), "..%2F..%2Fadmin");
        assert_eq!(encode_segment("group id?x=1"), "group%20id%3Fx%3D1");
    }

    /// Every parameter is named even at its default, because the broker's
    /// defaults are not this facade's: absent `batch` MEANS 200.
    #[test]
    fn a_pop_names_every_parameter_it_depends_on() {
        let q = pop_query(&PopOptions::default());
        assert_eq!(q, "?batch=1&partitions=1&wait=false");

        let q = pop_query(&PopOptions {
            batch: 10,
            partitions: 4,
            lease_seconds: 30,
            wait: true,
            timeout_ms: 20_000,
            consumer_group: Some("g/1".into()),
        });
        assert_eq!(
            q,
            "?batch=10&partitions=4&wait=true&leaseSeconds=30&timeout=20000&consumerGroup=g%2F1"
        );
    }

    #[test]
    fn one_credential_is_one_key_and_printing_it_prints_no_secret() {
        let a = CredentialKey::of(Some("AKIA…/secret"));
        assert_eq!(a, CredentialKey::of(Some("AKIA…/secret")));
        assert_ne!(a, CredentialKey::of(Some("AKIA…/secreT")));
        assert!(CredentialKey::of(None).is_anonymous());
        assert!(!format!("{a:?}").contains("secret"));
    }

    // ------------------------------------------------------------- the double
    //
    // From here on the assertions are about SEMANTICS, not bytes: what a
    // visibility timeout does, what an ack does to a cursor, what a lost fence
    // does to a bundle.

    const G: &str = QUEUE_MODE_GROUP;

    fn body(n: i64) -> serde_json::Value {
        serde_json::json!({ "b": format!("message {n}") })
    }

    async fn receive(fake: &Arc<FakeQueen>, queue: &str) -> Popped {
        fake.pop_queue(queue, &PopOptions::default(), None)
            .await
            .unwrap()
    }

    /// One claim of up to `batch` messages from one lane.
    async fn receive_n(fake: &Arc<FakeQueen>, queue: &str, batch: i32) -> Popped {
        fake.pop_queue(
            queue,
            &PopOptions {
                batch,
                ..PopOptions::default()
            },
            None,
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_pushed_message_comes_back_from_a_pop() {
        let fake = FakeQueen::with(&["orders"]);
        let pushed = fake
            .push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        assert_eq!(pushed[0].status, "queued");
        assert_eq!(pushed[0].offset, Some(0));

        let got = receive(&fake, "orders").await;
        assert_eq!(got.messages.len(), 1);
        assert_eq!(got.messages[0].id, pushed[0].message_id);
        assert_eq!(got.messages[0].data, body(1));
        assert_eq!(got.messages[0].delivery_attempt, 1);
        assert_eq!(
            got.messages[0].partition_id,
            fake.partition_id("orders", "0")
        );
        assert!(
            !got.lease_id.is_empty(),
            "a leased pop always names a worker"
        );
    }

    /// The claim IS the visibility timeout: while it is live nothing else sees
    /// the message, and when it lapses the lane is claimable again — no error,
    /// no cost.
    #[tokio::test]
    async fn a_leased_message_is_invisible_until_the_lease_lapses() {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let first = fake
            .pop_queue(
                "orders",
                &PopOptions {
                    lease_seconds: 30,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(first.messages.len(), 1);
        assert!(fake.leased("orders", "0", G));

        let second = receive(&fake, "orders").await;
        assert!(second.messages.is_empty(), "still invisible");

        fake.advance(Duration::from_secs(31));
        assert!(!fake.leased("orders", "0", G));
        let third = receive(&fake, "orders").await;
        assert_eq!(third.messages.len(), 1);
        // Same start offset ⇒ a REDELIVERY, and the attempt count is what
        // ApproximateReceiveCount reports.
        assert_eq!(third.messages[0].delivery_attempt, 2);
        assert_eq!(
            fake.retries("orders", "0", G),
            0,
            "expiry never charges the retry budget"
        );
    }

    /// The three things one `completed` ack does, and the two answers a REPEAT
    /// of it gets — which are different, and the difference is what the delete
    /// action has to be written against.
    #[tokio::test]
    async fn a_completed_ack_advances_the_cursor_and_a_repeat_is_a_noop_only_under_the_live_lease()
    {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(
            &[
                PushItem::new("orders", "0", body(1)),
                PushItem::new("orders", "0", body(2)),
            ],
            None,
        )
        .await
        .unwrap();
        let got = fake
            .pop_queue(
                "orders",
                &PopOptions {
                    batch: 2,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        let (first, second) = (got.messages[0].clone(), got.messages[1].clone());

        let acked = fake
            .ack(
                &AckItem::completed(&first.transaction_id, &first.partition_id, &got.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(acked.success && !acked.noop);
        assert_eq!(fake.committed("orders", "0", G), Some(0));
        assert!(
            !acked.lease_released,
            "the batch is not done, so the claim is still held"
        );

        // BELOW-CURSOR HONESTY, under the lease that is still live: the position
        // can have no effect any more, and for a `completed` that is a harmless
        // duplicate commit rather than a failure.
        let again = fake
            .ack(
                &AckItem::completed(&first.transaction_id, &first.partition_id, &got.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(again.success && again.noop);

        // Reaching batch_end releases the claim and resets the attempt state.
        let done = fake
            .ack(
                &AckItem::completed(&second.transaction_id, &second.partition_id, &got.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(done.success && done.lease_released);
        assert_eq!(fake.committed("orders", "0", G), Some(1));
        assert!(receive(&fake, "orders").await.messages.is_empty());

        // And ONCE THE LEASE IS GONE the same ack is refused rather than
        // answered `noop`: the lease check short-circuits ahead of the
        // below-cursor branch (005_log_ack.sql). A `DeleteMessage` repeated
        // after its lease was released therefore reaches the facade as `invalid
        // or expired lease`, and it is the DELETE ACTION's job to decide what
        // SQS says about it — not this layer's to pretend it did not happen.
        let stale = fake
            .ack(
                &AckItem::completed(&first.transaction_id, &first.partition_id, &got.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(!stale.success);
        assert_eq!(stale.error.as_deref(), Some("invalid or expired lease"));
    }

    /// THE RESOLUTION RULE, pinned against the procedure it doubles: a hash is
    /// resolved to its lowest occurrence INSIDE the ackable span, not to its
    /// lowest occurrence in the lane. A key re-used after the queue's dedup
    /// window puts two messages under one hash, and once the first is committed
    /// the ack of the second must move the cursor — answering `noop` there would
    /// make this double the reason a facade bug is invisible
    /// (005_log_ack.sql: `eff = MIN(voff) FILTER (voff >= GREATEST(committed +
    /// 1, txns_start))`).
    #[tokio::test]
    async fn a_hash_resolves_inside_the_span_and_not_below_the_cursor() {
        let fake = FakeQueen::with(&["orders"]);
        fake.append("orders", "0", "d1", body(1));
        fake.append("orders", "0", "d1", body(2));
        let got = receive_n(&fake, "orders", 2).await;
        assert_eq!(got.messages.len(), 2, "two messages, one dedup key");
        let claim = (
            got.messages[0].partition_id.clone(),
            got.lease_id.clone(),
            "d1".to_string(),
        );

        let first = fake
            .ack(
                &AckItem::completed(&claim.2, &claim.0, &claim.1),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(first.success && !first.noop);
        assert_eq!(fake.committed("orders", "0", G), Some(0));

        // The SAME hash again, with an occurrence below the cursor and one still
        // in the span: the in-span copy is what is acked.
        let second = fake
            .ack(
                &AckItem::completed(&claim.2, &claim.0, &claim.1),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(second.success, "{:?}", second.error);
        assert!(!second.noop, "the second occurrence is not a duplicate ack");
        assert_eq!(fake.committed("orders", "0", G), Some(1));
        assert!(second.lease_released, "the batch is complete");
    }

    /// A receipt handle from a PREVIOUS delivery names a lease that is gone.
    /// AWS answers `ReceiptHandleIsInvalid`/`MessageNotInflight`; the broker
    /// answers `invalid or expired lease`, and the facade may not turn that into
    /// a silent cursor advance.
    #[tokio::test]
    async fn an_ack_under_a_stale_lease_is_refused() {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let first = fake
            .pop_queue(
                "orders",
                &PopOptions {
                    lease_seconds: 10,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        let stale = first.messages[0].clone();
        fake.advance(Duration::from_secs(11));
        // Somebody else now holds it.
        let second = receive(&fake, "orders").await;
        assert_eq!(second.messages.len(), 1);

        let refused = fake
            .ack(
                &AckItem::completed(&stale.transaction_id, &stale.partition_id, &first.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(!refused.success);
        assert_eq!(refused.error.as_deref(), Some("invalid or expired lease"));
        assert_eq!(
            fake.committed("orders", "0", G),
            Some(-1),
            "the cursor did not move under a foreign lease"
        );
    }

    /// `ChangeMessageVisibility(0)` — the message comes straight back and the
    /// retry budget is untouched, which is the difference between a terminate
    /// and a failure.
    #[tokio::test]
    async fn a_retry_releases_without_charging_the_budget() {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let got = receive(&fake, "orders").await;
        let m = got.messages[0].clone();
        let answered = fake
            .ack(
                &AckItem::released(&m.transaction_id, &m.partition_id, &got.lease_id),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(answered.success && answered.lease_released);
        assert_eq!(fake.retries("orders", "0", G), 0);
        assert_eq!(fake.committed("orders", "0", G), Some(-1));

        let back = receive(&fake, "orders").await;
        assert_eq!(back.messages[0].transaction_id, m.transaction_id);
        assert_eq!(back.messages[0].delivery_attempt, 2);
    }

    /// `failed` charges once per ack while budget remains; the exhausted one on
    /// a queue with the native DLQ OFF — which is every SQS queue — drops the
    /// poison and advances past it, because otherwise the lane never moves.
    #[tokio::test]
    async fn a_failed_ack_charges_the_budget_and_the_exhausted_one_advances_past_the_poison() {
        let fake = FakeQueen::empty();
        fake.add_queue(
            "orders",
            serde_json::json!({ "retryLimit": 2, "deadLetterQueue": false }),
        );
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();

        for expected in 1..=2 {
            let got = receive(&fake, "orders").await;
            let m = got.messages[0].clone();
            fake.ack(
                &AckItem::new(
                    &m.transaction_id,
                    &m.partition_id,
                    &got.lease_id,
                    AckStatus::Failed,
                ),
                None,
                None,
            )
            .await
            .unwrap();
            assert_eq!(fake.retries("orders", "0", G), expected);
            assert_eq!(fake.committed("orders", "0", G), Some(-1));
        }

        let got = receive(&fake, "orders").await;
        let m = got.messages[0].clone();
        assert_eq!(m.delivery_attempt, 3);
        let answered = fake
            .ack(
                &AckItem::new(
                    &m.transaction_id,
                    &m.partition_id,
                    &got.lease_id,
                    AckStatus::Failed,
                ),
                None,
                None,
            )
            .await
            .unwrap();
        assert!(answered.success);
        assert!(!answered.dlq, "the native DLQ is off for an SQS queue");
        assert_eq!(fake.committed("orders", "0", G), Some(0));
        assert!(receive(&fake, "orders").await.messages.is_empty());
    }

    /// The lowest explicit signal in one call decides the action even when a
    /// later position was completed — the cursor is CLAMPED below it, so an
    /// explicit nack is never swallowed by an implicit ack above it.
    #[tokio::test]
    async fn an_explicit_signal_is_never_skipped_by_a_completion_above_it() {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(
            &[
                PushItem::new("orders", "0", body(1)),
                PushItem::new("orders", "0", body(2)),
                PushItem::new("orders", "0", body(3)),
            ],
            None,
        )
        .await
        .unwrap();
        let got = fake
            .pop_queue(
                "orders",
                &PopOptions {
                    batch: 3,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(got.messages.len(), 3);
        let acks: Vec<AckItem> = vec![
            AckItem::completed(
                &got.messages[0].transaction_id,
                &got.messages[0].partition_id,
                &got.lease_id,
            ),
            AckItem::released(
                &got.messages[1].transaction_id,
                &got.messages[1].partition_id,
                &got.lease_id,
            ),
            AckItem::completed(
                &got.messages[2].transaction_id,
                &got.messages[2].partition_id,
                &got.lease_id,
            ),
        ];
        let answered = fake.ack_batch(&acks, None, None).await.unwrap();
        assert!(answered.iter().all(|a| a.success));
        // Offset 0 is retired; offset 1 (the signal) and everything above it
        // redeliver — at-least-once duplicates, never a lost nack.
        assert_eq!(fake.committed("orders", "0", G), Some(0));
        let back = receive(&fake, "orders").await;
        assert_eq!(back.messages[0].data, body(2));
    }

    #[tokio::test]
    async fn extending_a_lease_keeps_the_claim_and_extending_a_dead_one_renews_nothing() {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let got = fake
            .pop_queue(
                "orders",
                &PopOptions {
                    lease_seconds: 10,
                    ..PopOptions::default()
                },
                None,
            )
            .await
            .unwrap();
        let extended = fake.lease_extend(&got.lease_id, 60, None).await.unwrap();
        assert!(extended.success);
        assert_eq!(extended.renewed, 1);

        fake.advance(Duration::from_secs(30));
        assert!(
            fake.leased("orders", "0", G),
            "the extension outlives the original expiry"
        );
        assert!(receive(&fake, "orders").await.messages.is_empty());

        fake.advance(Duration::from_secs(31));
        let dead = fake.lease_extend(&got.lease_id, 60, None).await.unwrap();
        assert_eq!(dead.renewed, 0, "an expired lease is never resurrected");
        assert!(!dead.success);
    }

    /// A FIFO group is a lane, and a pinned pop is how one is consumed without
    /// touching the others.
    #[tokio::test]
    async fn a_pinned_pop_claims_only_its_own_lane() {
        let fake = FakeQueen::with(&["orders.fifo"]);
        fake.push(
            &[
                PushItem::new("orders.fifo", "group-a", body(1)),
                PushItem::new("orders.fifo", "group-b", body(2)),
            ],
            None,
        )
        .await
        .unwrap();
        let got = fake
            .pop_partition("orders.fifo", "group-b", &PopOptions::default(), None)
            .await
            .unwrap();
        assert_eq!(got.messages.len(), 1);
        assert_eq!(got.messages[0].partition, "group-b");
        assert!(!fake.leased("orders.fifo", "group-a", G));
    }

    /// `MessageDeduplicationId` inside the window: not an error, and the answer
    /// is the ORIGINAL MessageId at the ORIGINAL SequenceNumber.
    #[tokio::test]
    async fn a_repeated_dedup_key_answers_the_original_message() {
        let fake = FakeQueen::with(&["orders.fifo"]);
        let first = fake
            .push(
                &[PushItem::deduped("orders.fifo", "g", body(1), "dedup-1")],
                None,
            )
            .await
            .unwrap();
        let second = fake
            .push(
                &[PushItem::deduped("orders.fifo", "g", body(9), "dedup-1")],
                None,
            )
            .await
            .unwrap();
        assert_eq!(second[0].status, "duplicate");
        assert_eq!(second[0].message_id, first[0].message_id);
        assert_eq!(second[0].offset, first[0].offset);
        assert_eq!(fake.lane("orders.fifo", "g").len(), 1);
    }

    #[tokio::test]
    async fn kv_writes_read_back_and_a_compare_and_set_that_loses_says_who_won() {
        let fake = FakeQueen::with(&[]);
        let answers = fake
            .kv(
                &[
                    KvOp::put("qs", "qs:q:orders", serde_json::json!({"v":1})),
                    KvOp::get("qs", "qs:q:orders"),
                ],
                None,
            )
            .await
            .unwrap();
        assert!(answers[0].applied());
        assert!(answers[1].found);
        let version = answers[0].version;

        // Somebody else moved it between our read and our write.
        fake.kv_seed("qs", "qs:q:orders", serde_json::json!({"v":2}));
        let lost = fake
            .kv(
                &[KvOp::put_expecting(
                    "qs",
                    "qs:q:orders",
                    serde_json::json!({"v":3}),
                    version,
                )],
                None,
            )
            .await
            .unwrap();
        assert!(!lost[0].applied());
        assert_eq!(lost[0].reason.as_deref(), Some("version"));
        // The loser is told what the winner holds, so it needs no second call.
        assert_eq!(lost[0].value, serde_json::json!({"v":2}));
        assert_eq!(
            fake.kv_get("qs", "qs:q:orders"),
            Some(serde_json::json!({"v":2}))
        );
    }

    /// CreateQueue is a `putIfAbsent`, and it must WIN against a row whose TTL
    /// has run out but which the sweeper has not pruned — otherwise a
    /// short-lived key would lock its own name out.
    #[tokio::test]
    async fn put_if_absent_wins_against_an_expired_row() {
        let fake = FakeQueen::with(&[]);
        fake.kv_seed_ttl("qs", "qs:rra:x", serde_json::json!("held"), Some(30));
        let blocked = fake
            .kv(
                &[KvOp::put_if_absent_ttl(
                    "qs",
                    "qs:rra:x",
                    serde_json::json!("mine"),
                    30,
                )],
                None,
            )
            .await
            .unwrap();
        assert!(!blocked[0].applied());
        assert_eq!(blocked[0].reason.as_deref(), Some("exists"));

        fake.advance(Duration::from_secs(31));
        assert_eq!(
            fake.kv_get("qs", "qs:rra:x"),
            None,
            "expired reads as absent"
        );
        let won = fake
            .kv(
                &[KvOp::put_if_absent_ttl(
                    "qs",
                    "qs:rra:x",
                    serde_json::json!("mine"),
                    30,
                )],
                None,
            )
            .await
            .unwrap();
        assert!(won[0].applied());
    }

    /// With `max`, `applied` IS the admission decision: the call that would
    /// break the ceiling does not apply and reports the CURRENT value, never the
    /// would-be one. That is what makes it usable as a rate cap.
    #[tokio::test]
    async fn incr_refuses_the_call_that_would_break_the_ceiling() {
        let fake = FakeQueen::with(&[]);
        for expected in 1..=2 {
            let a = fake
                .kv(&[KvOp::incr("qs", "qs:mv:rate", 1, Some(2), 60)], None)
                .await
                .unwrap();
            assert!(a[0].applied());
            assert_eq!(a[0].value, serde_json::json!(expected));
        }
        let refused = fake
            .kv(&[KvOp::incr("qs", "qs:mv:rate", 1, Some(2), 60)], None)
            .await
            .unwrap();
        assert!(!refused[0].applied());
        assert_eq!(refused[0].reason.as_deref(), Some("limit"));
        assert_eq!(
            refused[0].value,
            serde_json::json!(2),
            "never the would-be value"
        );
    }

    /// ListQueues walks a prefix. The cursor is exclusive and in BYTE order, and
    /// `truncated` is what says the page was cut — a listing that ignored it
    /// would under-report the tenant's queues.
    #[tokio::test]
    async fn get_prefix_pages_in_byte_order() {
        let fake = FakeQueen::with(&[]);
        for name in ["qs:q:a", "qs:q:b", "qs:q:c", "qs:t:topic"] {
            fake.kv_seed("qs", name, serde_json::json!({ "n": name }));
        }
        let page = fake
            .kv(&[KvOp::get_prefix("qs", "qs:q:", 2, None)], None)
            .await
            .unwrap();
        assert_eq!(
            page[0]
                .rows
                .iter()
                .map(|r| r.key.as_str())
                .collect::<Vec<_>>(),
            vec!["qs:q:a", "qs:q:b"]
        );
        assert!(page[0].truncated);
        assert_eq!(page[0].next_after.as_deref(), Some("qs:q:b"));

        let rest = fake
            .kv(
                &[KvOp::get_prefix(
                    "qs",
                    "qs:q:",
                    2,
                    page[0].next_after.as_deref(),
                )],
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            rest[0]
                .rows
                .iter()
                .map(|r| r.key.as_str())
                .collect::<Vec<_>>(),
            vec!["qs:q:c"]
        );
        assert!(!rest[0].truncated);
    }

    /// A `required` precondition raises out of the stored procedure and rolls
    /// the whole call back: the write beside it must not survive either.
    #[tokio::test]
    async fn a_required_precondition_that_loses_writes_nothing() {
        let fake = FakeQueen::with(&[]);
        fake.kv_seed("qs", "qs:q:orders", serde_json::json!({"v":1}));
        let refused = fake
            .kv(
                &[
                    KvOp::put("qs", "qs:q:other", serde_json::json!({"v":9})),
                    KvOp::fence("qs", "qs:q:orders", serde_json::json!({"v":2}), 12345),
                ],
                None,
            )
            .await;
        assert!(matches!(refused, Err(Error::Precondition { .. })));
        assert_eq!(
            fake.kv_get("qs", "qs:q:other"),
            None,
            "the operation before the fence rolled back with it"
        );
    }

    /// The redrive move: the copy and the delete are ONE bundle, so a facade
    /// that dies mid-move can neither lose the message nor duplicate it.
    #[tokio::test]
    async fn the_redrive_move_writes_the_copy_and_the_ack_in_one_bundle() {
        let fake = FakeQueen::with(&["orders", "orders-dlq"]);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let got = receive(&fake, "orders").await;
        let m = got.messages[0].clone();

        let outcome = fake
            .transaction(
                // A FRESH transactionId, so the destination's dedup window
                // cannot swallow the move.
                &[PushItem::deduped(
                    "orders-dlq",
                    "0",
                    serde_json::json!({"b":"message 1","m":m.id}),
                    "move-1",
                )],
                &[TxnAck::completed(
                    &m.transaction_id,
                    &m.partition_id,
                    &got.lease_id,
                )],
                &[KvOp::put(
                    "qs",
                    "qs:mv:job-1",
                    serde_json::json!({"moved":1}),
                )],
                None,
            )
            .await
            .unwrap();
        assert_eq!(outcome.pushes.len(), 1);
        assert_eq!(outcome.acks.len(), 1);
        assert!(outcome.kv[0].applied());
        assert_eq!(fake.committed("orders", "0", G), Some(0));
        assert_eq!(fake.lane("orders-dlq", "0").len(), 1);
        assert!(receive(&fake, "orders").await.messages.is_empty());
    }

    #[tokio::test]
    async fn a_bundle_whose_gate_is_lost_writes_neither_the_copy_nor_the_ack() {
        let fake = FakeQueen::with(&["orders", "orders-dlq"]);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let got = receive(&fake, "orders").await;
        let m = got.messages[0].clone();

        let refused = fake
            .transaction(
                &[PushItem::new("orders-dlq", "0", body(1))],
                &[TxnAck::completed(
                    &m.transaction_id,
                    &m.partition_id,
                    &got.lease_id,
                )],
                &[KvOp::fence(
                    "qs",
                    "qs:mv:job-1",
                    serde_json::json!({"moved":1}),
                    999,
                )],
                None,
            )
            .await;
        assert!(matches!(refused, Err(Error::Precondition { .. })));
        assert!(fake.lane("orders-dlq", "0").is_empty(), "no copy");
        assert_eq!(
            fake.committed("orders", "0", G),
            Some(-1),
            "and the original is still there"
        );
    }

    /// Per-message `DelaySeconds`: the message does not exist in the queue until
    /// the timer fires, and a cancel afterwards answers `absent` — which MAY
    /// MEAN ALREADY DELIVERED, echoing the txn so the caller can check.
    #[tokio::test]
    async fn a_delayed_message_arrives_when_its_timer_is_due() {
        let fake = FakeQueen::with(&["orders"]);
        let payload = {
            use base64::Engine;
            base64::engine::general_purpose::STANDARD.encode(br#"{"b":"later"}"#)
        };
        let scheduled = fake
            .timers_schedule(
                &[TimerSchedule::new(
                    "orders",
                    "msg-1",
                    "0",
                    300_000,
                    "t-delayed",
                    &payload,
                )],
                None,
            )
            .await
            .unwrap();
        assert_eq!(scheduled[0].status, TimerStatus::Scheduled);
        assert_eq!(
            fake.timers_count("orders", "msg-", None).await.unwrap(),
            1,
            "ApproximateNumberOfMessagesDelayed"
        );
        assert!(receive(&fake, "orders").await.messages.is_empty());

        fake.advance(Duration::from_secs(301));
        let got = receive(&fake, "orders").await;
        assert_eq!(got.messages[0].transaction_id, "t-delayed");
        assert_eq!(got.messages[0].data, serde_json::json!({"b":"later"}));

        let late = fake
            .timers_cancel("orders", "msg-1", Some("t-delayed"), None)
            .await
            .unwrap();
        assert_eq!(late.status, TimerStatus::Absent);
        assert!(!late.ok);
        assert_eq!(late.txn.as_deref(), Some("t-delayed"));
    }

    #[tokio::test]
    async fn a_cancelled_timer_never_becomes_a_message() {
        let fake = FakeQueen::with(&["orders"]);
        fake.timers_schedule(
            &[TimerSchedule::new(
                "orders", "msg-2", "0", 60_000, "t-2", "",
            )],
            None,
        )
        .await
        .unwrap();
        let cancelled = fake
            .timers_cancel("orders", "msg-2", None, None)
            .await
            .unwrap();
        assert_eq!(cancelled.status, TimerStatus::Cancelled);
        fake.advance(Duration::from_secs(120));
        assert!(receive(&fake, "orders").await.messages.is_empty());
        assert_eq!(fake.timers_count("orders", "msg-", None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn the_depth_reports_the_backlog_and_what_is_in_flight() {
        let fake = FakeQueen::with(&["orders"]);
        fake.push(
            &[
                PushItem::new("orders", "0", body(1)),
                PushItem::new("orders", "0", body(2)),
                PushItem::new("orders", "1", body(3)),
            ],
            None,
        )
        .await
        .unwrap();
        let depth = fake.queue_depth("orders", None, None).await.unwrap();
        assert_eq!(depth.pending, 3);
        assert_eq!(depth.processing, 0);
        assert_eq!(depth.partitions_pending, 2);

        let _leased = receive(&fake, "orders").await;
        let depth = fake.queue_depth("orders", None, None).await.unwrap();
        assert_eq!(depth.pending, 3);
        assert_eq!(depth.processing, 1, "one message is not visible");
        assert_eq!(depth.ready, 2);

        // The route's own 404 is the existence probe.
        let missing = fake.queue_depth("nope", None, None).await;
        assert_eq!(missing.unwrap_err().http_status(), Some(404));
    }

    #[tokio::test]
    async fn configure_is_an_upsert_and_delete_takes_the_log_with_it() {
        let fake = FakeQueen::with(&[]);
        fake.configure_queue(
            "orders",
            &serde_json::json!({"leaseTime": 30, "deadLetterQueue": false}),
            None,
        )
        .await
        .unwrap();
        assert_eq!(fake.configures.lock().unwrap().len(), 1);
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();

        // The queue's own leaseTime is the visibility timeout a pop that names
        // none gets.
        let got = receive(&fake, "orders").await;
        assert_eq!(got.messages.len(), 1);
        fake.advance(Duration::from_secs(31));
        assert!(!fake.leased("orders", "0", G));

        let deleted = fake.delete_queue("orders", None).await.unwrap();
        assert!(deleted.existed);
        assert!(fake.lane("orders", "0").is_empty());
        // Idempotent by design: a queue that is not there is `existed: false`,
        // never an HTTP error.
        assert!(!fake.delete_queue("orders", None).await.unwrap().existed);
    }

    /// THE PROPERTY `PurgeQueue` RESTS ON, pinned on the double because it is a
    /// property of the SCHEMA and not of the facade: `queen.log_partitions.id`
    /// is a `gen_random_uuid()` primary key hanging off `queen.queues(id) ON
    /// DELETE CASCADE` (001_log_schema.sql:28-30), and `delete_queue_v1` deletes
    /// the queue row (013_analytics.sql) — so a lane of the same name in a queue
    /// recreated afterwards is a NEW row with a NEW id.
    ///
    /// A receipt handle carries that id, which is what makes a handle minted
    /// before a purge address nothing at all afterwards rather than a message
    /// that arrived since.
    #[tokio::test]
    async fn a_recreated_queues_lanes_are_new_partitions_with_new_ids() {
        let fake = FakeQueen::with(&[]);
        fake.configure_queue("orders", &serde_json::json!({}), None)
            .await
            .unwrap();
        fake.push(&[PushItem::new("orders", "0", body(1))], None)
            .await
            .unwrap();
        let before = fake.partition_id("orders", "0");

        fake.delete_queue("orders", None).await.unwrap();
        fake.configure_queue("orders", &serde_json::json!({}), None)
            .await
            .unwrap();
        fake.push(&[PushItem::new("orders", "0", body(2))], None)
            .await
            .unwrap();
        let after = fake.partition_id("orders", "0");
        assert_ne!(before, after, "the old lane id survived its queue");

        // …and an ack addressed by the old id resolves to no consumer at all,
        // which is the only thing a stale handle can do after a purge.
        let acked = fake
            .ack(&AckItem::completed("txn-1", &before, "lease-1"), None, None)
            .await
            .unwrap();
        assert!(!acked.success);
        assert_eq!(acked.error.as_deref(), Some("consumer not found"));
        assert_eq!(fake.lane("orders", "0").len(), 1, "the new message is gone");
    }

    #[tokio::test]
    async fn the_catalog_collapses_a_burst_into_one_admin_call() {
        let fake = FakeQueen::with(&["orders"]);
        let catalog = Catalog::with_ttl(fake.clone(), Duration::from_secs(60));
        assert!(catalog.has("orders", None).await.unwrap());
        assert!(!catalog.has("nope", None).await.unwrap());
        assert_eq!(
            fake.tokens.lock().unwrap().len(),
            1,
            "the second read came from the cache"
        );

        // A create drops the entry, so the next read sees the new queue rather
        // than a list up to one TTL old.
        catalog
            .configure("nope", &serde_json::json!({}), None)
            .await
            .unwrap();
        assert!(catalog.has("nope", None).await.unwrap());
    }

    /// A blip on the admin API must not become `QueueDoesNotExist`: a client
    /// that believes that deletes its own configuration.
    #[tokio::test]
    async fn a_failed_refresh_serves_the_last_list_it_had() {
        let fake = FakeQueen::with(&["orders"]);
        let catalog = Catalog::with_ttl(fake.clone(), Duration::from_millis(0));
        assert!(catalog.has("orders", None).await.unwrap());
        fake.fail_list(Error::Transport("connection reset".into()));
        assert!(
            catalog.has("orders", None).await.unwrap(),
            "stale beats empty"
        );
        // But the path that may NOT act on a stale world says so.
        fake.fail_list(Error::status(503, "unavailable"));
        assert!(catalog.refresh(None).await.is_err());
    }
}
