//! Kafka transactions by BUFFER-AND-COMMIT (M9).
//!
//! A transactional producer's records are held in this process until
//! `EndTxn(commit)`, which writes the whole set through ONE call to
//! `POST /api/v1/transaction` — a single Postgres transaction that already
//! carries KV riders inside itself
//! (server/sql/procedures/005_log_ack.sql, `log_transaction_wire_v1`). Abort
//! discards the stage. **No uncommitted record ever enters the log.**
//!
//! That one sentence is the whole design, and everything Kafka builds to cope
//! with uncommitted records in the log has nothing to point at here:
//!
//!   * `read_committed` costs nothing and was already implemented:
//!     [`crate::handlers::fetch`] answers `last_stable_offset = high_watermark`
//!     with an empty `aborted_transactions` on every partition, and that answer
//!     is true for a STRONGER reason after M9 than before it.
//!   * there is no LSO to track, no aborted-transaction index, no control
//!     records and no `WriteTxnMarkers`.
//!   * offsets stay contiguous. A committed transaction of N records advances
//!     the log end offset by exactly N, where Apache Kafka advances it by N+1 —
//!     the commit marker consumes an offset there and there is no marker here.
//!
//! ## The split: the stage is this process's, the outcome is Queen's
//!
//! The same split `lib.rs` states for the coordinator. The STAGE — records,
//! staged offsets, the partition set — is in this module's memory and is
//! deliberately lost on a restart. The IDENTITY, the EPOCH and the OUTCOME are
//! durable, in one KV key per `transactional.id`:
//!
//! ```text
//!   namespace  queen-kafka                    (crate::offsets::NAMESPACE)
//!   key        qk:txn:<esc transactional.id>  (crate::offsets::escape)
//!   value      {"pid":<i64>,"epoch":<i16>,"state":"open"|"committed"|"aborted",
//!               "seq":<u64>,"node":<i32>,"incarnation":"<tok>","ts":<ms>}
//!   expiry     forever
//! ```
//!
//! `qk:txn:` shares no prefix with `qk:group:`, `qk:groups:`, `qk:fence:` or
//! `qk:node:` — they differ at the fourth character — and the disjointness is
//! pinned by a test below, including against a `transactional.id` that spells
//! one of the other prefixes.
//!
//! **`forever` and not a TTL**, for [`crate::cluster::fence`]'s stated reason: a
//! TTL would expire between two transactions of a slow producer, the `expect`
//! would come back `absent`, and a legitimate commit would abort. The
//! consequence is a key with no delete path in v1 — the same leak
//! `qk:fence:` already has, and bounded the same way: a `transactional.id` is a
//! configuration value, so a fleet has as many as it has producer instances,
//! and a hundred thousand of them is about 20 MB.
//!
//! ## Fencing is the epoch bump, and the CAS is what enforces it
//!
//! A second producer taking the same `transactional.id` writes the key at
//! `epoch + 1` and takes its new version with it. The first producer's commit
//! carries the version it holds, at **index 0 of the bundle with
//! `required: true`** — so a fenced commit does not merely fail, it writes
//! **zero records and zero offsets**, because a lost `required` precondition
//! raises 23514 out of `kv_apply_v1` and rolls the whole Postgres transaction
//! back.
//!
//! ## What survives a crash, and the property that must not be got wrong
//!
//! The stage is lost and nothing partial is ever in the log — a consequence of
//! the write being one bundle: before `EndTxn` zero records have been sent to
//! Queen, so there is nothing to be half-written. The client's next request
//! meets no binding and is answered `INVALID_TXN_STATE` (48), which is fatal in
//! the Java transactional producer.
//!
//! **A client can see `error_code = 0` on `EndTxn(commit)` only if this facade
//! held the stage and the bundle committed. A false positive — the application
//! believes committed while nothing landed — is unreachable.** The reverse (a
//! commit that landed and was answered an error, because the facade died
//! between the Postgres COMMIT and the response) is reachable, is bounded to
//! one transaction, and is the safe direction: the application resumes from
//! offsets that were committed atomically with the records and reprocesses
//! nothing.
//!
//! This is also why the `qk:txn:` marker is never consulted to answer an
//! *unheld* `EndTxn`. It cannot be: the client sends no transaction sequence
//! number, so a marker reading `state: "committed"` cannot distinguish "your
//! commit landed" from "your previous commit landed and this one did not".
//! Reading it and guessing is the fabrication this codebase refuses.
//!
//! ## Single-node only, and it is routing rather than fencing
//!
//! A Kafka producer sends `Produce` to the PARTITION LEADER and `EndTxn` to the
//! TRANSACTION COORDINATOR. In cluster mode those are different nodes by
//! construction, so the stage lands on one facade and the commit arrives at
//! another, which holds nothing. There is no cheap fix — the cluster design has
//! no facade-to-facade RPC, and a Metadata request carries no producer identity
//! to skew the answer per producer with. So `QUEEN_KAFKA_NODE_ID` set means
//! transactions are refused, legibly and fatally, at
//! [`crate::handlers::find_coordinator`] and at
//! [`crate::handlers::init_producer_id`]. The gate is on CONFIGURATION and not
//! on the live view: a cluster-mode deployment that happens to have one live
//! node must not serve transactions, because a node joining would break them
//! mid-flight.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

use kafka_protocol::error::ResponseError;
use tokio::time::Instant;

use crate::identity::TenantKey;
use crate::obs;
use crate::offsets::{self, Committed};
use crate::queen::{KvOp, PushItem};

/// The prefix of a `transactional.id`'s durable row. See the module header for
/// why it cannot collide with the three other `qk:` spaces.
const KEY_PREFIX: &str = "qk:txn:";

/// Staged record bytes one transaction may hold — `QUEEN_KAFKA_TXN_MAX_BYTES`.
///
/// DERIVED, not chosen. 8 MiB of record bytes becomes ~11 MiB of base64 inside
/// the payload envelope plus ~120 bytes of JSON per item; at
/// [`DEFAULT_MAX_TXN_RECORDS`] that is roughly 17 MiB of request body against
/// `QUEEN_MAX_BODY_BYTES`, whose default is 64 MiB (server/src/main.rs).
/// **Raising this without redoing that arithmetic turns every large commit into
/// a 413.**
pub const DEFAULT_MAX_TXN_BYTES: usize = 8 * 1024 * 1024;

/// Staged records one transaction may hold — `QUEEN_KAFKA_TXN_MAX_RECORDS`.
///
/// One `operations[]` item per record, and the broker builds a push echo, a
/// frame and a `results[]` slot for each. `handle_transaction` has **no cap on
/// `operations` length at all** — the only bound there is the body size — so
/// this is the only thing standing between a transactional producer and that
/// allocation.
pub const DEFAULT_MAX_TXN_RECORDS: usize = 50_000;

/// Partitions one transaction may span. Each becomes a push group inside the
/// bundle and one `queen.log_partitions` row lock in the pre-lock.
pub const MAX_TXN_PARTITIONS: usize = 200;

/// Offsets one transaction may commit.
///
/// **Derived and not negotiable:** the transaction wire takes at most
/// [`crate::queen::WIRE_KV_MAX_OPS`] KV operations, one of which is this
/// transaction's own fence and one of which is the group-existence index.
pub const MAX_TXN_OFFSETS: usize = crate::queen::WIRE_KV_MAX_OPS - 2;

/// Staged bytes across the whole PROCESS —
/// `QUEEN_KAFKA_TXN_MAX_STAGED_BYTES`. Sixteen producers at their full
/// per-transaction cap. This is the number that keeps a transaction stage from
/// becoming an out-of-memory an unbounded client can drive; it can be this
/// generous because, unlike the pre-auth frame budget, every byte of it belongs
/// to a connection that has already authenticated.
pub const DEFAULT_MAX_STAGED_BYTES: usize = 128 * 1024 * 1024;

/// Open transactions this process holds — `QUEEN_KAFKA_TXN_MAX_OPEN`. The shape
/// of `coordinator::MAX_GROUPS`: a number a deployment reaches only by
/// misconfiguration.
pub const DEFAULT_MAX_OPEN: usize = 1_024;

/// `transaction.max.timeout.ms`, Kafka's own default and its own refusal code.
pub const DEFAULT_MAX_TIMEOUT_MS: u64 = 900_000;

/// How often the sweep walks the registry. A transaction timeout is measured in
/// tens of seconds at least, so a second of granularity costs nothing and one
/// walk of a map bounded by [`DEFAULT_MAX_OPEN`] is not work.
pub const SWEEP_INTERVAL: Duration = Duration::from_secs(1);

/// One line per window when transactions are being refused at a cap, not one
/// per request: a cap is reached by a fleet, and a line per refusal is a line
/// per produce. See [`obs::Sampler`].
static CAPPED: obs::Sampler = obs::Sampler::new(60_000);
/// The same, for transactions the sweep drops.
static SWEPT: obs::Sampler = obs::Sampler::new(60_000);

/// The key of a `transactional.id`'s durable row, or `None` when it will not
/// fit the store's key column.
///
/// Unlike the group keys of [`crate::offsets`], this one has no longer partner
/// key to be shorter than, so it is checked on its own: a `transactional.id`
/// that does not fit is refused at InitProducerId and no state is created for
/// it.
pub fn key(transactional_id: &str) -> Option<String> {
    let key = format!("{KEY_PREFIX}{}", offsets::escape(transactional_id));
    (key.len() <= offsets::MAX_KEY_BYTES).then_some(key)
}

/// Which connection owns a stage.
///
/// Minted per accepted connection and never reused within a process, so
/// "drop everything this connection staged" is one comparison and cannot reach
/// a stage that a later connection built under the same address.
pub type ConnId = u64;

/// The next connection id.
pub fn next_conn_id() -> ConnId {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

/// What the durable row says a transaction's last decided outcome was.
///
/// Written for the operator and for forensics; nothing reads it to make a
/// decision, and the module header says why it cannot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Open,
    Committed,
    Aborted,
}

impl Outcome {
    fn as_str(self) -> &'static str {
        match self {
            Outcome::Open => "open",
            Outcome::Committed => "committed",
            Outcome::Aborted => "aborted",
        }
    }
}

/// The durable row's value.
pub fn marker(
    pid: i64,
    epoch: i16,
    outcome: Outcome,
    seq: u64,
    node: i32,
    incarnation: &str,
    now_ms: i64,
) -> serde_json::Value {
    serde_json::json!({
        "pid": pid,
        "epoch": epoch,
        "state": outcome.as_str(),
        "seq": seq,
        "node": node,
        "incarnation": incarnation,
        "ts": now_ms,
    })
}

/// Read a stored marker back. `None` for anything that is not one of ours — the
/// caller treats that as a row it must not bump blindly.
pub fn read_marker(v: &serde_json::Value) -> Option<(i64, i16, u64)> {
    Some((
        v.get("pid")?.as_i64()?,
        i16::try_from(v.get("epoch")?.as_i64()?).ok()?,
        v.get("seq").and_then(|s| s.as_u64()).unwrap_or_default(),
    ))
}

/// Where a transaction is in its own life.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    /// Bound, with nothing staged: after InitProducerId and after every
    /// decided transaction.
    Empty,
    /// A first `AddPartitionsToTxn` has arrived. The deadline is running.
    Open,
    /// The bundle is in flight. The connection is serial so a second request of
    /// the SAME connection cannot reach this, which is exactly why the state is
    /// kept: it is the answer to a request from a DIFFERENT one.
    Committing,
    /// A cap was exceeded. The producer must abort; nothing else is accepted.
    Abortable,
}

/// One `transactional.id`'s stage and binding.
pub struct Txn {
    pub pid: i64,
    pub epoch: i16,
    /// The version `qk:txn:<id>` holds, threaded exactly like
    /// `offsets::Stored::fence_version`: it is what the commit `expect`s, and
    /// it advances every time this facade writes the row.
    pub version: i64,
    pub state: TxnState,
    /// The connection that ran InitProducerId. A disconnect drops its stages.
    pub owner: ConnId,
    /// Registered by AddPartitionsToTxn, in arrival order.
    pub partitions: Vec<(String, i32)>,
    /// Registered by AddOffsetsToTxn. One group per transaction (see
    /// [`Txns::add_offsets`]).
    pub group: Option<String>,
    /// Staged by TxnOffsetCommit, keyed by the composed KV key so a second
    /// commit of one partition replaces the first rather than adding a second
    /// operation to the bundle.
    pub offsets: Vec<(String, Committed)>,
    /// The records, in arrival order. This is the stage.
    pub staged: Vec<PushItem>,
    /// What [`Txn::staged`] has been charged for.
    pub bytes: usize,
    /// How many decided transactions this id has had. Operator-facing only.
    pub seq: u64,
    /// `now + min(transaction_timeout_ms, the cap)`, set when the transaction
    /// opens.
    pub deadline: Instant,
    /// The timeout the client asked for, kept so a re-open sets the same
    /// deadline without another request.
    timeout: Duration,
}

impl Txn {
    /// Is this request's `(producer_id, epoch)` the one this binding holds?
    fn check(&self, pid: i64, epoch: i16) -> Option<Fault> {
        if pid != self.pid {
            return Some(Fault::WrongProducer);
        }
        match epoch.cmp(&self.epoch) {
            std::cmp::Ordering::Less => Some(Fault::Fenced),
            std::cmp::Ordering::Greater => Some(Fault::AheadOfUs),
            std::cmp::Ordering::Equal => None,
        }
    }

    /// Forget the stage, keep the binding. What an abort and a fence do.
    fn clear(&mut self) {
        self.partitions.clear();
        self.group = None;
        self.offsets.clear();
        self.staged.clear();
        self.bytes = 0;
        self.state = TxnState::Empty;
    }

    /// The deadline passed: forget the stage AND refuse everything that follows
    /// until the producer aborts or re-inits.
    ///
    /// Not [`Txn::clear`], and the difference is the whole safety property of
    /// this module. `clear` leaves the binding `Empty`, which is the state a
    /// DECIDED transaction ends in, and `EndTxn(commit)` against `Empty` is a
    /// legitimate commit of nothing that is answered `error_code = 0`. An
    /// expired transaction is not nothing: its records were staged and were
    /// then dropped, so answering its commit 0 would be exactly the failure the
    /// module header calls unreachable, an application believing a commit that
    /// never happened. `Abortable` is what the producer must be told, and it
    /// becomes INVALID_TXN_STATE (48) on commit and `error_code = 0` on abort,
    /// because a lost stage IS an aborted transaction.
    fn expire(&mut self) {
        self.clear();
        self.state = TxnState::Abortable;
    }
}

/// Why a transactional request cannot be served, and the code it becomes.
///
/// Every code here is one the Java transactional producer already handles on
/// the API that emits it, which is `compat/ERRORS.md`'s rule for choosing one:
/// the closed set the client accepts beats the most precise word.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// No binding for this `transactional.id` on this facade — a restart, a
    /// stage the sweep dropped, or a commit that arrived at a process which
    /// never held it. Fatal in the client, deliberately: see the module header.
    Unknown,
    /// The request's epoch is BELOW the bound one: a second producer took this
    /// id. The transaction this request belongs to writes nothing.
    Fenced,
    /// The request's epoch is ABOVE the bound one, which this facade cannot
    /// have granted.
    AheadOfUs,
    /// The `producer_id` is not the one this `transactional.id` holds.
    WrongProducer,
    /// The transaction is past its deadline and the sweep has dropped, or is
    /// about to drop, its stage.
    Expired,
    /// The request needs an open transaction and there is none, or a bundle is
    /// already in flight for this one.
    NotOpen,
    /// A bundle is in flight for this transaction right now.
    InFlight,
    /// A cap was exceeded and the producer must abort.
    Abortable,
}

impl Fault {
    /// The wire code. `INVALID_TXN_STATE` covers three different faults on
    /// purpose: they are three ways of saying "this transaction is not in a
    /// state that can continue", and the Java producer's answer to all three is
    /// the same — abort, and if it cannot, fail the application.
    pub fn code(self) -> ResponseError {
        match self {
            Fault::Unknown | Fault::Expired | Fault::NotOpen | Fault::Abortable => {
                ResponseError::InvalidTxnState
            }
            Fault::Fenced => ResponseError::ProducerFenced,
            Fault::AheadOfUs => ResponseError::InvalidProducerEpoch,
            Fault::WrongProducer => ResponseError::InvalidProducerIdMapping,
            Fault::InFlight => ResponseError::ConcurrentTransactions,
        }
    }
}

/// Why a stage refused to take more.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Full {
    /// Past [`Limits::max_txn_bytes`] or [`Limits::max_txn_records`]. NOT
    /// retriable, which is right: waiting will not make a 12 MiB transaction
    /// fit an 8 MiB stage.
    Transaction,
    /// Past [`Limits::max_staged_bytes`]. Retriable, and deliberately a
    /// different answer: memory does free up when other transactions commit.
    Process,
    /// Past [`MAX_TXN_PARTITIONS`].
    Partitions,
    /// Past [`MAX_TXN_OFFSETS`].
    Offsets,
}

/// The caps, resolved at boot. See the constants above for each derivation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub max_txn_bytes: usize,
    pub max_txn_records: usize,
    pub max_staged_bytes: usize,
    pub max_open: usize,
    pub max_timeout: Duration,
}

impl Default for Limits {
    fn default() -> Limits {
        Limits {
            max_txn_bytes: DEFAULT_MAX_TXN_BYTES,
            max_txn_records: DEFAULT_MAX_TXN_RECORDS,
            max_staged_bytes: DEFAULT_MAX_STAGED_BYTES,
            max_open: DEFAULT_MAX_OPEN,
            max_timeout: Duration::from_millis(DEFAULT_MAX_TIMEOUT_MS),
        }
    }
}

/// Every transaction this process is holding a stage for.
///
/// Keyed by `(TenantKey, transactional.id)` and never by connection alone, for
/// three reasons that are each load-bearing: the InitProducerId fence has to
/// drop ANOTHER connection's stage; the process-wide byte cap has to be
/// enforced across connections; and the sweep needs a single place to walk.
/// `lib.rs` pins that one tenant's two credentials share one coordinator and
/// that two tenants naming one group stay apart — two tenants picking
/// `"my-txn"` stay apart for exactly the same reason, and the KV key needs no
/// tenant in it because `queen.kv` is keyed by one.
///
/// The lock is a `std::sync::Mutex` and is NEVER held across an await, the
/// idiom `Coordinator::groups` and `cluster::ClusterState::view` already use.
pub struct Txns {
    inner: Mutex<Inner>,
    limits: Limits,
}

struct Inner {
    txns: HashMap<(TenantKey, String), Txn>,
    /// The process-wide charge, kept as a running total rather than summed on
    /// every produce: the map is bounded by `max_open` and the sum would be
    /// walked once per staged batch.
    staged_bytes: usize,
}

impl Default for Txns {
    fn default() -> Txns {
        Txns::new(Limits::default())
    }
}

impl Txns {
    pub fn new(limits: Limits) -> Txns {
        Txns {
            inner: Mutex::new(Inner {
                txns: HashMap::new(),
                staged_bytes: 0,
            }),
            limits,
        }
    }

    pub fn limits(&self) -> Limits {
        self.limits
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner
            .lock()
            .expect("the transaction map lock is never held across a panic")
    }

    /// How many transactions are bound right now. Tests and log lines only.
    pub fn len(&self) -> usize {
        self.lock().txns.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Staged bytes across the process. Tests and log lines only.
    pub fn staged_bytes(&self) -> usize {
        self.lock().staged_bytes
    }

    /// Bind `(tenant, id)` to a producer session, replacing whatever was there.
    ///
    /// This is what InitProducerId calls once the KV claim has been won, and it
    /// is where an old epoch's stage is dropped: the bump has already made that
    /// stage uncommittable (its `expect` can no longer match), and freeing the
    /// memory at the moment of fencing rather than at the sweep is belt and
    /// braces on top of that.
    ///
    /// `Err(())` is the open-transaction cap; the caller answers
    /// CONCURRENT_TRANSACTIONS, which is retriable and literally true.
    // Eight arguments, and they are eight because a binding IS eight
    // independent facts a claim decided. A parameter struct would move the same
    // list one file away and stop the compiler naming the one a caller forgot.
    #[allow(clippy::result_unit_err, clippy::too_many_arguments)]
    pub fn bind(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
        version: i64,
        owner: ConnId,
        timeout: Duration,
    ) -> Result<(), ()> {
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        if !inner.txns.contains_key(&at) && inner.txns.len() >= self.limits.max_open {
            if let Some(suppressed) = CAPPED.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    open = self.limits.max_open,
                    suppressed,
                    "more transactional producers than QUEEN_KAFKA_TXN_MAX_OPEN; \
                     InitProducerId is answering CONCURRENT_TRANSACTIONS"
                );
            }
            return Err(());
        }
        let timeout = timeout.min(self.limits.max_timeout);
        if let Some(old) = inner.txns.remove(&at) {
            inner.staged_bytes = inner.staged_bytes.saturating_sub(old.bytes);
        }
        inner.txns.insert(
            at,
            Txn {
                pid,
                epoch,
                version,
                state: TxnState::Empty,
                owner,
                partitions: Vec::new(),
                group: None,
                offsets: Vec::new(),
                staged: Vec::new(),
                bytes: 0,
                seq: 0,
                deadline: Instant::now() + timeout,
                timeout,
            },
        );
        Ok(())
    }

    /// Run `f` against the binding for `(tenant, id)`, having checked the
    /// request's `(producer_id, epoch)` and the deadline first.
    ///
    /// Every transactional request goes through here, which is what keeps the
    /// four APIs and the produce path from growing four different opinions
    /// about what a fenced producer is. Nothing is awaited inside `f`: the
    /// signature makes that impossible, which is how the lock discipline is
    /// enforced rather than remembered.
    pub fn with<R>(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
        f: impl FnOnce(&mut Txn) -> R,
    ) -> Result<R, Fault> {
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        let now = Instant::now();
        let txn = inner.txns.get_mut(&at).ok_or(Fault::Unknown)?;
        if let Some(fault) = txn.check(pid, epoch) {
            return Err(fault);
        }
        // Checked here rather than left to the sweep, so a request that arrives
        // between two sweeps meets the same answer the sweep would have left
        // for it. The stage is dropped on the spot for the same reason.
        if txn.state != TxnState::Empty && now >= txn.deadline {
            let freed = txn.bytes;
            txn.expire();
            inner.staged_bytes = inner.staged_bytes.saturating_sub(freed);
            return Err(Fault::Expired);
        }
        Ok(f(txn))
    }

    /// Register partitions, opening the transaction if this is its first.
    ///
    /// Answers one verdict per requested partition so a caller can refuse only
    /// the ones past the cap — AddPartitionsToTxn has per-partition error codes
    /// and no top-level one below v4.
    pub fn add_partitions(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
        wanted: &[(String, i32)],
    ) -> Result<Vec<Result<(), Full>>, Fault> {
        self.with(tenant, id, pid, epoch, |txn| {
            if txn.state == TxnState::Committing {
                return Err(Fault::InFlight);
            }
            if txn.state == TxnState::Abortable {
                return Err(Fault::Abortable);
            }
            if txn.state == TxnState::Empty {
                txn.state = TxnState::Open;
                txn.deadline = Instant::now() + txn.timeout;
            }
            Ok(wanted
                .iter()
                .map(|p| {
                    if txn.partitions.contains(p) {
                        return Ok(());
                    }
                    if txn.partitions.len() >= MAX_TXN_PARTITIONS {
                        return Err(Full::Partitions);
                    }
                    txn.partitions.push(p.clone());
                    Ok(())
                })
                .collect())
        })?
    }

    /// Register the consumer group whose offsets ride in this transaction.
    ///
    /// ONE group per transaction in v1. A second, different group is refused,
    /// because the offset budget of [`MAX_TXN_OFFSETS`] is derived from the
    /// assumption that exactly one group-index operation rides in the bundle,
    /// and a silent second group would silently shrink the partition budget.
    /// Kafka allows several; this is a stated deviation and the shape nobody
    /// uses.
    pub fn add_offsets(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
        group: &str,
    ) -> Result<Result<(), Fault>, Fault> {
        self.with(tenant, id, pid, epoch, |txn| {
            match txn.state {
                TxnState::Committing => return Err(Fault::InFlight),
                TxnState::Abortable => return Err(Fault::Abortable),
                // Kafka's own rule: AddOffsetsToTxn is sent inside a
                // transaction, and `sendOffsetsToTransaction` is preceded by
                // the produce that opened it. A transaction with no partitions
                // yet is still legitimately open here — a consume-only EOS loop
                // commits offsets and produces nothing — so this OPENS one.
                TxnState::Empty => {
                    txn.state = TxnState::Open;
                    txn.deadline = Instant::now() + txn.timeout;
                }
                TxnState::Open => {}
            }
            match &txn.group {
                Some(already) if already != group => Err(Fault::NotOpen),
                _ => {
                    txn.group = Some(group.to_string());
                    Ok(())
                }
            }
        })
    }

    /// Stage one partition's committed offset.
    pub fn stage_offset(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
        key: String,
        committed: Committed,
    ) -> Result<Result<(), Full>, Fault> {
        self.with(tenant, id, pid, epoch, |txn| {
            if let Some(slot) = txn.offsets.iter_mut().find(|(k, _)| *k == key) {
                // A re-commit of one partition inside one transaction is the
                // last one, not two operations: the bundle is an upsert per
                // key and a duplicate would spend an operation of the budget on
                // a write the next one overwrites.
                slot.1 = committed;
                return Ok(());
            }
            if txn.offsets.len() >= MAX_TXN_OFFSETS {
                return Err(Full::Offsets);
            }
            txn.offsets.push((key, committed));
            Ok(())
        })
    }

    /// Charge and stage one produce entry's records.
    ///
    /// The charge is made BEFORE the items are moved in and the whole entry is
    /// refused or taken together, which is what makes a partition's answer one
    /// error code rather than "some of your records".
    #[allow(clippy::too_many_arguments)]
    pub fn stage_records(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
        topic: &str,
        partition: i32,
        items: Vec<PushItem>,
        bytes: usize,
    ) -> Result<Result<(), Full>, Fault> {
        let limits = self.limits;
        if let Err(full) = self.with(tenant, id, pid, epoch, move |txn| {
            match txn.state {
                TxnState::Committing => return Err(Full::Transaction),
                TxnState::Abortable => return Err(Full::Transaction),
                // A Produce for a partition that was never added is refused by
                // the caller before this runs; an Empty transaction here would
                // therefore be a producer that skipped AddPartitionsToTxn.
                TxnState::Empty | TxnState::Open => {}
            }
            if !txn
                .partitions
                .iter()
                .any(|(t, p)| t == topic && *p == partition)
            {
                return Err(Full::Partitions);
            }
            Ok(())
        })? {
            // The transaction is bound and this producer owns it, but the entry
            // cannot be staged: the partition was never added, or the
            // transaction is already poisoned. Both are verdicts about THIS
            // entry, so they come back beside a served request rather than as a
            // fault about the transaction.
            return Ok(Err(full));
        }
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        let staged_bytes = inner.staged_bytes;
        let txn = inner.txns.get_mut(&at).ok_or(Fault::Unknown)?;
        if txn.bytes.saturating_add(bytes) > limits.max_txn_bytes
            || txn.staged.len().saturating_add(items.len()) > limits.max_txn_records
        {
            txn.state = TxnState::Abortable;
            if let Some(suppressed) = CAPPED.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    transactional_id = id,
                    staged = txn.bytes,
                    records = txn.staged.len(),
                    max_bytes = limits.max_txn_bytes,
                    max_records = limits.max_txn_records,
                    suppressed,
                    "a transaction is past QUEEN_KAFKA_TXN_MAX_BYTES or \
                     QUEEN_KAFKA_TXN_MAX_RECORDS; the producer must abort it"
                );
            }
            return Ok(Err(Full::Transaction));
        }
        if staged_bytes.saturating_add(bytes) > limits.max_staged_bytes {
            // NOT `Abortable`: the transaction is fine and the PROCESS is full,
            // so the producer is answered something retriable and the same
            // request succeeds once another transaction commits.
            if let Some(suppressed) = CAPPED.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    staged = staged_bytes,
                    max = limits.max_staged_bytes,
                    suppressed,
                    "the process is at QUEEN_KAFKA_TXN_MAX_STAGED_BYTES; transactional produce is \
                     being answered retriably until a transaction commits"
                );
            }
            return Ok(Err(Full::Process));
        }
        txn.bytes += bytes;
        txn.staged.extend(items);
        inner.staged_bytes += bytes;
        Ok(Ok(()))
    }

    /// Everything one commit needs, taken out of the stage in one lock.
    ///
    /// The transaction is left in [`TxnState::Committing`] and its stage is
    /// still charged: the bundle has not landed, so the memory is not free and
    /// a retriable failure must be able to commit the same stage again.
    pub fn begin_commit(
        &self,
        tenant: &TenantKey,
        id: &str,
        pid: i64,
        epoch: i16,
    ) -> Result<Bundle, Fault> {
        self.with(tenant, id, pid, epoch, |txn| match txn.state {
            TxnState::Committing => Err(Fault::InFlight),
            TxnState::Abortable => Err(Fault::Abortable),
            TxnState::Empty | TxnState::Open => {
                txn.state = TxnState::Committing;
                Ok(Bundle {
                    version: txn.version,
                    seq: txn.seq,
                    items: txn.staged.clone(),
                    group: txn.group.clone(),
                    offsets: txn.offsets.clone(),
                })
            }
        })?
    }

    /// The bundle committed: free the stage, take the fence's new version, and
    /// count the transaction.
    pub fn commit_landed(&self, tenant: &TenantKey, id: &str, version: i64) {
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        if let Some(txn) = inner.txns.get_mut(&at) {
            let freed = txn.bytes;
            txn.clear();
            txn.version = version;
            txn.seq += 1;
            inner.staged_bytes = inner.staged_bytes.saturating_sub(freed);
        }
    }

    /// The bundle failed in a way a retry could still commit: put the
    /// transaction back where it was and KEEP the stage.
    ///
    /// This is the rule that must not be got backwards. Dropping the stage on a
    /// retriable failure would turn the client's retry into a silent EMPTY
    /// commit — a transaction the application believes wrote its records and
    /// which wrote none.
    pub fn commit_failed(&self, tenant: &TenantKey, id: &str) {
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        if let Some(txn) = inner.txns.get_mut(&at) {
            txn.state = TxnState::Open;
        }
    }

    /// Drop the stage and keep the binding: an abort, or a fence.
    pub fn discard(&self, tenant: &TenantKey, id: &str) -> Option<(i64, u64)> {
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        let txn = inner.txns.get_mut(&at)?;
        let freed = txn.bytes;
        let (version, seq) = (txn.version, txn.seq);
        txn.clear();
        txn.seq += 1;
        inner.staged_bytes = inner.staged_bytes.saturating_sub(freed);
        Some((version, seq))
    }

    /// Record the version the abort marker landed at, so the NEXT transaction's
    /// commit expects the right one.
    pub fn note_version(&self, tenant: &TenantKey, id: &str, version: i64) {
        let mut inner = self.lock();
        let at = (tenant.clone(), id.to_string());
        if let Some(txn) = inner.txns.get_mut(&at) {
            txn.version = version;
        }
    }

    /// Forget a `transactional.id` entirely.
    pub fn forget(&self, tenant: &TenantKey, id: &str) {
        let mut inner = self.lock();
        if let Some(txn) = inner.txns.remove(&(tenant.clone(), id.to_string())) {
            inner.staged_bytes = inner.staged_bytes.saturating_sub(txn.bytes);
        }
    }

    /// Drop every stage owned by one connection.
    ///
    /// The ordinary path for a producer that closes, and the crash path for one
    /// that does not: either way a lost stage IS an aborted transaction,
    /// because nothing of it was ever written.
    pub fn drop_connection(&self, owner: ConnId) {
        let mut inner = self.lock();
        let freed: usize = inner
            .txns
            .values()
            .filter(|t| t.owner == owner)
            .map(|t| t.bytes)
            .sum();
        inner.txns.retain(|_, t| t.owner != owner);
        inner.staged_bytes = inner.staged_bytes.saturating_sub(freed);
    }

    /// Drop the stage of every open transaction past its deadline.
    ///
    /// Not a nicety: the stage is a memory amplifier and this is what bounds it
    /// in TIME, exactly as the caps bound it in size. A producer that opens a
    /// transaction and disappears without closing its TCP connection — a hung
    /// JVM, a partitioned network — holds its bytes until either this or
    /// `conn::IDLE_TIMEOUT` fires, and this is usually first.
    ///
    /// The binding and the durable marker are left alone. The next request
    /// naming the transaction is answered INVALID_TXN_STATE, which is what the
    /// client needs to hear.
    pub fn sweep(&self) -> usize {
        let now = Instant::now();
        let mut inner = self.lock();
        let mut swept = 0;
        let mut freed = 0;
        for txn in inner.txns.values_mut() {
            if txn.state == TxnState::Empty || now < txn.deadline {
                continue;
            }
            freed += txn.bytes;
            txn.expire();
            swept += 1;
        }
        inner.staged_bytes = inner.staged_bytes.saturating_sub(freed);
        if swept > 0 {
            if let Some(suppressed) = SWEPT.tick_now() {
                tracing::warn!(
                    target: "kafka",
                    swept,
                    freed,
                    suppressed,
                    "transactions past their transaction.timeout.ms; their staged records were \
                     dropped and were never written"
                );
            }
        }
        swept
    }
}

/// Everything `EndTxn(commit)` needs, taken out of the stage.
#[derive(Debug, Clone)]
pub struct Bundle {
    /// The `qk:txn:` version to `expect`. THE fence.
    pub version: i64,
    /// The transaction ordinal this commit will become.
    pub seq: u64,
    pub items: Vec<PushItem>,
    pub group: Option<String>,
    pub offsets: Vec<(String, Committed)>,
}

impl Bundle {
    /// The KV rider of the commit, in the order the design fixes.
    ///
    /// **Index 0 is the fence AND the outcome marker, in one operation**, and
    /// `required: true` on it is the whole mechanism: without it a lost
    /// precondition is a verdict that rolls back nothing and the records would
    /// land anyway. With it, a fenced producer writes exactly zero records and
    /// zero offsets.
    ///
    /// The offset writes are UNCONDITIONAL, byte-identical to what
    /// `offsets::store` sends in single mode today. There is no `qk:fence:`
    /// operation because there is no cluster: transactions are refused when
    /// `QUEEN_KAFKA_NODE_ID` is set.
    #[allow(clippy::too_many_arguments)]
    pub fn kv_ops(
        &self,
        transactional_id: &str,
        pid: i64,
        epoch: i16,
        node: i32,
        incarnation: &str,
        now_ms: i64,
        protocol_type: &str,
    ) -> Option<Vec<KvOp>> {
        let mut ops = Vec::with_capacity(self.offsets.len() + 2);
        ops.push(KvOp::fence(
            offsets::NAMESPACE,
            &key(transactional_id)?,
            marker(
                pid,
                epoch,
                Outcome::Committed,
                self.seq + 1,
                node,
                incarnation,
                now_ms,
            ),
            self.version,
        ));
        // The durable group index (M7 F2), and only when this transaction
        // actually commits an offset: a group that exists is a group something
        // was committed for, and writing the row for a transaction that
        // committed none would list a group with nothing in it.
        if let Some(group) = self.group.as_deref().filter(|_| !self.offsets.is_empty()) {
            ops.push(offsets::index_op(group, protocol_type, now_ms)?);
        }
        for (key, committed) in &self.offsets {
            ops.push(offsets::commit_op(key, committed));
        }
        Some(ops)
    }
}

/// The sweep, as a task. Spawned from `main.rs` and from nothing else.
pub async fn sweep_loop(txns: std::sync::Arc<Txns>) {
    let mut tick = tokio::time::interval(SWEEP_INTERVAL);
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tick.tick().await;
        txns.sweep();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tenant() -> TenantKey {
        TenantKey::Tenant("acme".into())
    }

    fn txns() -> Txns {
        Txns::new(Limits::default())
    }

    fn bound(t: &Txns, id: &str) {
        t.bind(&tenant(), id, 7, 0, 100, 1, Duration::from_secs(60))
            .expect("the cap is not reached");
    }

    fn item(topic: &str, partition: i32) -> PushItem {
        PushItem {
            queue: topic.to_string(),
            partition: partition.to_string(),
            payload: serde_json::json!({"v": "AA=="}),
        }
    }

    fn committed(offset: i64) -> Committed {
        Committed {
            offset,
            metadata: String::new(),
            ts: 0,
        }
    }

    // ------------------------------------------------------------------ keys

    /// The disjointness the whole `qk:` layout rests on, asserted against an
    /// adversarial `transactional.id` rather than against a friendly one: a
    /// producer may name itself `group:orders` or `node:1`, and neither may
    /// land in — or be read as — another key space.
    #[test]
    fn the_transaction_key_space_cannot_see_the_others() {
        for id in ["tx-1", "group:orders", "groups:orders", "node:1", "fence:g"] {
            let k = key(id).expect("short enough to store");
            assert!(k.starts_with("qk:txn:"), "{k}");
            assert!(!k.starts_with("qk:group:"), "{k}");
            assert!(!k.starts_with("qk:groups:"), "{k}");
            assert!(!k.starts_with("qk:fence:"), "{k}");
            assert!(!k.starts_with("qk:node:"), "{k}");
            // ...and the separator itself is escaped, so no id can spell a key
            // of another shape.
            assert!(!k["qk:txn:".len()..].contains(':'), "{k}");
        }
        // A prefix read of any other space cannot see a transaction row, and a
        // prefix read of this one cannot see theirs.
        assert!(!"qk:txn:x".starts_with("qk:group:"));
        assert!(!"qk:groups:x".starts_with("qk:txn:"));
    }

    #[test]
    fn a_transactional_id_that_will_not_fit_has_no_key() {
        assert!(key(&"a".repeat(400)).is_some());
        assert!(key(&"a".repeat(600)).is_none());
        // Escaping is what makes the bound reachable with a shorter id: every
        // byte outside the safe set costs three.
        assert!(key(&"\u{1}".repeat(200)).is_none());
    }

    #[test]
    fn a_marker_round_trips() {
        let v = marker(7, 3, Outcome::Committed, 9, 0, "abc", 1);
        assert_eq!(read_marker(&v), Some((7, 3, 9)));
        assert_eq!(v.get("state").and_then(|s| s.as_str()), Some("committed"));
        assert_eq!(read_marker(&serde_json::json!({"pid": 1})), None);
    }

    // --------------------------------------------------------- the machine

    #[test]
    fn a_bound_transaction_starts_empty_and_opens_on_its_first_partitions() {
        let t = txns();
        bound(&t, "tx");
        assert_eq!(
            t.with(&tenant(), "tx", 7, 0, |x| x.state).unwrap(),
            TxnState::Empty
        );
        let verdicts = t
            .add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        assert_eq!(verdicts, vec![Ok(())]);
        assert_eq!(
            t.with(&tenant(), "tx", 7, 0, |x| x.state).unwrap(),
            TxnState::Open
        );
    }

    #[test]
    fn an_unknown_transactional_id_is_a_fatal_state_error() {
        let t = txns();
        assert_eq!(
            t.with(&tenant(), "tx", 7, 0, |_| ()).unwrap_err(),
            Fault::Unknown
        );
        assert_eq!(Fault::Unknown.code(), ResponseError::InvalidTxnState);
    }

    /// The zombie path, and the reason this module exists: a second producer
    /// took the id, so the first one's next request carries an epoch below the
    /// bound one and is FENCED rather than served.
    #[test]
    fn a_stale_epoch_is_fenced_and_a_future_one_is_not_ours() {
        let t = txns();
        bound(&t, "tx");
        t.bind(&tenant(), "tx", 7, 1, 101, 2, Duration::from_secs(60))
            .unwrap();
        assert_eq!(
            t.with(&tenant(), "tx", 7, 0, |_| ()).unwrap_err(),
            Fault::Fenced
        );
        assert_eq!(Fault::Fenced.code(), ResponseError::ProducerFenced);
        assert_eq!(
            t.with(&tenant(), "tx", 7, 2, |_| ()).unwrap_err(),
            Fault::AheadOfUs
        );
        assert_eq!(Fault::AheadOfUs.code(), ResponseError::InvalidProducerEpoch);
        // ...and the right epoch still works.
        assert!(t.with(&tenant(), "tx", 7, 1, |_| ()).is_ok());
    }

    #[test]
    fn a_producer_id_that_is_not_the_bound_one_is_a_mapping_error() {
        let t = txns();
        bound(&t, "tx");
        assert_eq!(
            t.with(&tenant(), "tx", 8, 0, |_| ()).unwrap_err(),
            Fault::WrongProducer
        );
        assert_eq!(
            Fault::WrongProducer.code(),
            ResponseError::InvalidProducerIdMapping
        );
    }

    /// A rebind at a higher epoch drops what the old epoch staged, and returns
    /// the memory at the moment of fencing rather than at the sweep.
    #[test]
    fn a_rebind_drops_the_previous_epochs_stage() {
        let t = txns();
        bound(&t, "tx");
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "tx",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            1_000,
        )
        .unwrap()
        .unwrap();
        assert_eq!(t.staged_bytes(), 1_000);
        t.bind(&tenant(), "tx", 7, 1, 101, 2, Duration::from_secs(60))
            .unwrap();
        assert_eq!(t.staged_bytes(), 0);
        assert_eq!(t.len(), 1);
    }

    #[test]
    fn two_tenants_naming_one_transactional_id_stay_apart() {
        let t = txns();
        bound(&t, "tx");
        let other = TenantKey::Tenant("globex".into());
        t.bind(&other, "tx", 99, 4, 500, 2, Duration::from_secs(60))
            .unwrap();
        assert_eq!(t.len(), 2);
        // Each sees only its own binding: the other tenant's pid is a mapping
        // error here, not an accepted request.
        assert_eq!(
            t.with(&tenant(), "tx", 99, 4, |_| ()).unwrap_err(),
            Fault::WrongProducer
        );
        assert!(t.with(&other, "tx", 99, 4, |_| ()).is_ok());
    }

    // ----------------------------------------------------------- the caps

    #[test]
    fn the_open_transaction_cap_refuses_a_new_id_and_not_an_existing_one() {
        let t = Txns::new(Limits {
            max_open: 2,
            ..Limits::default()
        });
        bound(&t, "a");
        bound(&t, "b");
        assert!(t
            .bind(&tenant(), "c", 7, 0, 1, 1, Duration::from_secs(60))
            .is_err());
        // An id already bound is a REBIND, which allocates nothing.
        assert!(t
            .bind(&tenant(), "a", 7, 1, 2, 1, Duration::from_secs(60))
            .is_ok());
    }

    #[test]
    fn a_transaction_past_its_byte_cap_becomes_abortable() {
        let t = Txns::new(Limits {
            max_txn_bytes: 1_000,
            ..Limits::default()
        });
        bound(&t, "tx");
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        assert_eq!(
            t.stage_records(
                &tenant(),
                "tx",
                7,
                0,
                "orders",
                0,
                vec![item("orders", 0)],
                1_001
            )
            .unwrap(),
            Err(Full::Transaction)
        );
        assert_eq!(
            t.with(&tenant(), "tx", 7, 0, |x| x.state).unwrap(),
            TxnState::Abortable
        );
        // ...and nothing else is accepted until it is aborted.
        assert_eq!(
            t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 1)])
                .unwrap_err(),
            Fault::Abortable
        );
        assert_eq!(t.staged_bytes(), 0);
    }

    #[test]
    fn a_transaction_past_its_record_cap_becomes_abortable() {
        let t = Txns::new(Limits {
            max_txn_records: 1,
            ..Limits::default()
        });
        bound(&t, "tx");
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        assert_eq!(
            t.stage_records(
                &tenant(),
                "tx",
                7,
                0,
                "orders",
                0,
                vec![item("orders", 0), item("orders", 0)],
                10
            )
            .unwrap(),
            Err(Full::Transaction)
        );
    }

    /// The process cap answers something DIFFERENT on purpose: memory frees up
    /// when another transaction commits, so this one is retriable and the
    /// transaction is left open rather than poisoned.
    #[test]
    fn the_process_cap_is_retriable_and_leaves_the_transaction_open() {
        let t = Txns::new(Limits {
            max_staged_bytes: 1_000,
            ..Limits::default()
        });
        bound(&t, "a");
        t.add_partitions(&tenant(), "a", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "a",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            900,
        )
        .unwrap()
        .unwrap();
        assert_eq!(
            t.stage_records(
                &tenant(),
                "a",
                7,
                0,
                "orders",
                0,
                vec![item("orders", 0)],
                200
            )
            .unwrap(),
            Err(Full::Process)
        );
        assert_eq!(
            t.with(&tenant(), "a", 7, 0, |x| x.state).unwrap(),
            TxnState::Open
        );
    }

    #[test]
    fn the_partition_cap_refuses_only_the_partitions_past_it() {
        let t = txns();
        bound(&t, "tx");
        let wanted: Vec<(String, i32)> = (0..MAX_TXN_PARTITIONS as i32 + 2)
            .map(|p| ("orders".to_string(), p))
            .collect();
        let verdicts = t.add_partitions(&tenant(), "tx", 7, 0, &wanted).unwrap();
        assert_eq!(
            verdicts.iter().filter(|v| v.is_ok()).count(),
            MAX_TXN_PARTITIONS
        );
        assert_eq!(verdicts[MAX_TXN_PARTITIONS], Err(Full::Partitions));
    }

    #[test]
    fn the_offset_cap_is_the_wires_op_budget_minus_the_fence_and_the_index() {
        assert_eq!(MAX_TXN_OFFSETS, 62);
        let t = txns();
        bound(&t, "tx");
        for i in 0..MAX_TXN_OFFSETS {
            t.stage_offset(&tenant(), "tx", 7, 0, format!("k{i}"), committed(1))
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            t.stage_offset(&tenant(), "tx", 7, 0, "one-too-many".into(), committed(1))
                .unwrap(),
            Err(Full::Offsets)
        );
        // ...and a re-commit of a key already staged is not a new operation.
        assert_eq!(
            t.stage_offset(&tenant(), "tx", 7, 0, "k0".into(), committed(9))
                .unwrap(),
            Ok(())
        );
    }

    #[test]
    fn a_second_group_in_one_transaction_is_refused() {
        let t = txns();
        bound(&t, "tx");
        assert_eq!(t.add_offsets(&tenant(), "tx", 7, 0, "g").unwrap(), Ok(()));
        assert_eq!(t.add_offsets(&tenant(), "tx", 7, 0, "g").unwrap(), Ok(()));
        assert_eq!(
            t.add_offsets(&tenant(), "tx", 7, 0, "other").unwrap(),
            Err(Fault::NotOpen)
        );
    }

    // -------------------------------------------------------- the outcomes

    #[test]
    fn a_commit_that_lands_frees_the_stage_and_takes_the_new_version() {
        let t = txns();
        bound(&t, "tx");
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "tx",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            50,
        )
        .unwrap()
        .unwrap();
        let bundle = t.begin_commit(&tenant(), "tx", 7, 0).unwrap();
        assert_eq!(bundle.version, 100);
        assert_eq!(bundle.items.len(), 1);
        // While it is in flight the stage is still charged and a second EndTxn
        // is refused rather than served twice.
        assert_eq!(t.staged_bytes(), 50);
        assert_eq!(
            t.begin_commit(&tenant(), "tx", 7, 0).unwrap_err(),
            Fault::InFlight
        );
        t.commit_landed(&tenant(), "tx", 200);
        assert_eq!(t.staged_bytes(), 0);
        let (version, seq, state) = t
            .with(&tenant(), "tx", 7, 0, |x| (x.version, x.seq, x.state))
            .unwrap();
        assert_eq!((version, seq, state), (200, 1, TxnState::Empty));
    }

    /// The rule that must not be got backwards: a retriable failure KEEPS the
    /// stage, or the client's retry would commit an empty transaction while
    /// believing it wrote its records.
    #[test]
    fn a_retriable_commit_failure_keeps_the_stage() {
        let t = txns();
        bound(&t, "tx");
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "tx",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            50,
        )
        .unwrap()
        .unwrap();
        t.begin_commit(&tenant(), "tx", 7, 0).unwrap();
        t.commit_failed(&tenant(), "tx");
        assert_eq!(t.staged_bytes(), 50);
        let again = t.begin_commit(&tenant(), "tx", 7, 0).unwrap();
        assert_eq!(again.items.len(), 1);
    }

    #[test]
    fn an_abort_drops_the_stage_and_keeps_the_binding() {
        let t = txns();
        bound(&t, "tx");
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "tx",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            50,
        )
        .unwrap()
        .unwrap();
        assert_eq!(t.discard(&tenant(), "tx"), Some((100, 0)));
        assert_eq!(t.staged_bytes(), 0);
        assert_eq!(t.len(), 1);
        assert_eq!(
            t.with(&tenant(), "tx", 7, 0, |x| (x.state, x.seq)).unwrap(),
            (TxnState::Empty, 1)
        );
    }

    #[test]
    fn a_disconnect_drops_only_that_connections_stages() {
        let t = txns();
        t.bind(&tenant(), "a", 7, 0, 1, 11, Duration::from_secs(60))
            .unwrap();
        t.bind(&tenant(), "b", 8, 0, 1, 22, Duration::from_secs(60))
            .unwrap();
        for (id, pid) in [("a", 7), ("b", 8)] {
            t.add_partitions(&tenant(), id, pid, 0, &[("orders".into(), 0)])
                .unwrap();
            t.stage_records(
                &tenant(),
                id,
                pid,
                0,
                "orders",
                0,
                vec![item("orders", 0)],
                100,
            )
            .unwrap()
            .unwrap();
        }
        assert_eq!(t.staged_bytes(), 200);
        t.drop_connection(11);
        assert_eq!(t.len(), 1);
        assert_eq!(t.staged_bytes(), 100);
        assert_eq!(
            t.with(&tenant(), "a", 7, 0, |_| ()).unwrap_err(),
            Fault::Unknown
        );
    }

    // ----------------------------------------------------------- the sweep

    #[tokio::test(start_paused = true)]
    async fn the_sweep_drops_an_open_transaction_past_its_deadline() {
        let t = txns();
        t.bind(&tenant(), "tx", 7, 0, 100, 1, Duration::from_secs(30))
            .unwrap();
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "tx",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            500,
        )
        .unwrap()
        .unwrap();
        assert_eq!(t.sweep(), 0, "the deadline has not passed");
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(t.sweep(), 1);
        assert_eq!(t.staged_bytes(), 0);
        // The BINDING survives: the next request is answered INVALID_TXN_STATE
        // because the transaction is not open, not because the id is unknown.
        assert_eq!(t.len(), 1);
        // And the commit is REFUSED rather than answered as a commit of
        // nothing. This assertion is the one the acceptance suite's
        // `transaction.timeout.ms` check reads from the client side: a swept
        // transaction whose commit answered 0 would tell an application that
        // records it staged had landed, when they were dropped.
        //
        // The CODE and not the fault: whether the request meets `Expired` (the
        // deadline check in `with`, which fires first because the deadline is
        // still in the past) or `Abortable` (the state the sweep left) depends
        // only on which of the two got there first, and both must be 48.
        assert_eq!(
            t.begin_commit(&tenant(), "tx", 7, 0).unwrap_err().code(),
            kafka_protocol::error::ResponseError::InvalidTxnState
        );
        assert_eq!(
            Fault::Abortable.code(),
            kafka_protocol::error::ResponseError::InvalidTxnState
        );
    }

    /// The other half of [`Txn::expire`]: a swept transaction refuses every
    /// commit, but an ABORT resets the binding so the producer opens its next
    /// transaction without re-initialising.
    ///
    /// `handlers::end_txn::abort` is what performs the reset, by falling
    /// through to `discard` on an expired binding instead of answering early;
    /// this pins the registry half of it, which is that a discarded binding is
    /// `Empty` again and takes a FRESH deadline on the next
    /// `AddPartitionsToTxn`.
    #[tokio::test(start_paused = true)]
    async fn an_abort_reopens_a_transaction_the_deadline_closed() {
        let t = txns();
        t.bind(&tenant(), "tx", 7, 0, 100, 1, Duration::from_secs(30))
            .unwrap();
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(t.sweep(), 1);
        assert!(t.discard(&tenant(), "tx").is_some());
        // Open again, and this time the deadline is 30 seconds from NOW.
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap()
            .into_iter()
            .for_each(|r| r.unwrap());
        assert_eq!(t.sweep(), 0);
        assert_eq!(
            t.begin_commit(&tenant(), "tx", 7, 0).unwrap().items.len(),
            0
        );
    }

    /// An EMPTY binding has no deadline to miss: a producer between two
    /// transactions is idle, not late.
    #[tokio::test(start_paused = true)]
    async fn the_sweep_leaves_an_empty_binding_alone() {
        let t = txns();
        t.bind(&tenant(), "tx", 7, 0, 100, 1, Duration::from_secs(30))
            .unwrap();
        tokio::time::advance(Duration::from_secs(120)).await;
        assert_eq!(t.sweep(), 0);
        assert!(t.with(&tenant(), "tx", 7, 0, |_| ()).is_ok());
    }

    /// A request that arrives between two sweeps meets the same answer the
    /// sweep would have left for it, and frees the same memory.
    #[tokio::test(start_paused = true)]
    async fn a_request_past_the_deadline_expires_the_transaction_itself() {
        let t = txns();
        t.bind(&tenant(), "tx", 7, 0, 100, 1, Duration::from_secs(30))
            .unwrap();
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        t.stage_records(
            &tenant(),
            "tx",
            7,
            0,
            "orders",
            0,
            vec![item("orders", 0)],
            500,
        )
        .unwrap()
        .unwrap();
        tokio::time::advance(Duration::from_secs(31)).await;
        assert_eq!(
            t.begin_commit(&tenant(), "tx", 7, 0).unwrap_err(),
            Fault::Expired
        );
        assert_eq!(t.staged_bytes(), 0);
    }

    /// The timeout the client asks for is CLAMPED, never taken: a producer
    /// asking for an hour on a facade configured for fifteen minutes gets
    /// fifteen minutes rather than an argument.
    #[tokio::test(start_paused = true)]
    async fn a_timeout_above_the_cap_is_clamped() {
        let t = Txns::new(Limits {
            max_timeout: Duration::from_secs(10),
            ..Limits::default()
        });
        t.bind(&tenant(), "tx", 7, 0, 100, 1, Duration::from_secs(3_600))
            .unwrap();
        t.add_partitions(&tenant(), "tx", 7, 0, &[("orders".into(), 0)])
            .unwrap();
        tokio::time::advance(Duration::from_secs(11)).await;
        assert_eq!(t.sweep(), 1);
    }

    // ---------------------------------------------------------- the bundle

    /// THE shape of a commit: the fence first, `required`, expecting the
    /// version this facade holds — and everything else after it.
    #[test]
    fn the_fence_is_at_index_zero_and_is_required() {
        let bundle = Bundle {
            version: 100,
            seq: 3,
            items: vec![item("orders", 0)],
            group: Some("orders-consumer".to_string()),
            offsets: vec![(
                offsets::key("orders-consumer", "orders", 0).unwrap(),
                committed(42),
            )],
        };
        let ops = bundle
            .kv_ops("tx-1", 7, 2, 0, "inc", 1_700_000_000_000, "consumer")
            .expect("every key fits");
        assert_eq!(ops.len(), 3, "fence, group index, one offset");
        match &ops[0] {
            KvOp::Put {
                ns,
                key,
                value,
                forever,
                expect,
                required,
                ..
            } => {
                assert_eq!(ns, offsets::NAMESPACE);
                assert_eq!(key, "qk:txn:tx-1");
                assert!(*forever, "a TTL would abort a slow producer's next commit");
                assert_eq!(*expect, Some(100));
                assert!(*required, "without this a fenced commit writes its records");
                assert_eq!(read_marker(value), Some((7, 2, 4)));
                assert_eq!(
                    value.get("state").and_then(|s| s.as_str()),
                    Some("committed")
                );
            }
            other => panic!("index 0 is not the fence: {other:?}"),
        }
        // The offsets are UNCONDITIONAL: the fence is what gates them.
        match &ops[2] {
            KvOp::Put { expect, key, .. } => {
                assert_eq!(*expect, None);
                assert_eq!(key, "qk:group:orders-consumer:orders:0");
            }
            other => panic!("the offset is not an unconditional put: {other:?}"),
        }
    }

    /// A transaction that commits no offset writes no group index row: a group
    /// in the listing with nothing committed for it is a group that does not
    /// exist.
    #[test]
    fn a_commit_with_no_offsets_carries_no_group_index() {
        let bundle = Bundle {
            version: 1,
            seq: 0,
            items: vec![item("orders", 0)],
            group: Some("orders-consumer".to_string()),
            offsets: Vec::new(),
        };
        let ops = bundle.kv_ops("tx", 7, 0, 0, "inc", 0, "consumer").unwrap();
        assert_eq!(ops.len(), 1);
    }

    /// The bundle can never exceed the wire's own op ceiling, which is the
    /// arithmetic MAX_TXN_OFFSETS was derived from — asserted here rather than
    /// trusted, because the derivation is two constants apart.
    #[test]
    fn a_full_bundle_fits_the_wires_op_budget() {
        let offsets: Vec<(String, Committed)> = (0..MAX_TXN_OFFSETS)
            .map(|p| {
                (
                    offsets::key("g", "orders", p as i32).unwrap(),
                    committed(p as i64),
                )
            })
            .collect();
        let bundle = Bundle {
            version: 1,
            seq: 0,
            items: Vec::new(),
            group: Some("g".to_string()),
            offsets,
        };
        let ops = bundle.kv_ops("tx", 7, 0, 0, "inc", 0, "consumer").unwrap();
        assert_eq!(ops.len(), crate::queen::WIRE_KV_MAX_OPS);
        assert!(ops.len() <= crate::queen::WIRE_KV_MAX_OPS);
        assert!(ops.iter().map(KvOp::keys).sum::<usize>() <= crate::queen::WIRE_KV_MAX_KEYS);
    }

    #[test]
    fn a_transactional_id_that_will_not_fit_builds_no_bundle() {
        let bundle = Bundle {
            version: 1,
            seq: 0,
            items: Vec::new(),
            group: None,
            offsets: Vec::new(),
        };
        assert!(bundle
            .kv_ops(&"a".repeat(600), 7, 0, 0, "inc", 0, "consumer")
            .is_none());
    }

    #[test]
    fn connection_ids_are_never_reused_within_a_process() {
        let a = next_conn_id();
        let b = next_conn_id();
        assert_ne!(a, b);
        assert!(b > a);
    }
}
