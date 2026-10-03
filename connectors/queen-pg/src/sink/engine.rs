//! The workers (PLAN §5.2, §5.4): pop → ONE PostgreSQL transaction → ack.
//!
//! Each worker owns one connection and loops: pop a batch (leases on the
//! partitions it claimed), apply it in one READ COMMITTED transaction together
//! with the progress rows of its partitions (`progress.rs`), commit, and only
//! then ack every message. Everything that can go wrong between those steps is
//! made harmless by the order:
//!
//! * a crash or a lost connection before COMMIT: nothing happened; the leases
//!   run out and the batch comes back;
//! * a lost answer to COMMIT: the outcome is unknown, so the batch is retried
//!   and the progress rows say what already landed;
//! * a lost or refused ack, an expired lease, two nodes holding one partition:
//!   the redelivery meets the progress row and is acked without being applied.
//!
//! **Failures** are sorted by what can fix them, never by message text:
//! *transient* (the classification of [`crate::pg::connect::classify`]:
//! connection loss, admin shutdown, serialization failure, deadlock) → roll
//! back, back off 1 s doubling to 30 s, retry the batch; *data* (SQLSTATE
//! classes 21, 22, 23, 27, 44, 54, P0 — values the table refuses, a trigger
//! that raises — or a payload the mode cannot read) → isolate the message
//! (below) and dead-letter it after `maxAttempts`; *environment* (anything
//! else: a missing table or column, a revoked privilege, a broken trigger) →
//! back off and retry, re-reading the table's shape, and NEVER dead-letter: a
//! dropped column must stall the sink with an error status, not file the
//! whole queue in the DLQ.
//!
//! **Leases.** A batch extends its leases every third of `leaseSeconds`
//! while an attempt is in flight (a slow statement, a long isolation) and
//! stops extending while it backs off after a failure: a node that cannot
//! reach the database hands its partitions to the others once the lease runs
//! out, instead of holding them for as long as its database is away.
//!
//! **Isolation.** A data error rolls the batch back; the batch is then applied
//! again in smaller transactions, each with its own progress rows: what
//! preceded the failing statement, the failing statement's messages, what
//! followed — halving whenever the culprit is the whole piece (an append run,
//! an error at COMMIT) — down to one message, which is tried `maxAttempts`
//! times and then acked `dlq` with the server's SQLSTATE and message. Its
//! partition stops there for this batch: the messages after it are neither
//! applied nor acked. The dead-letter ack releases the partition's lease, so
//! they are delivered again at once — and the progress row never passes a
//! dead-lettered offset before the broker has filed it. Were the ack lost,
//! the message is redelivered, fails again, and is filed again; nothing behind
//! it can have moved the progress past it.

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Client, IsolationLevel, Statement, Transaction};

use super::apply::{build_ops, Op, StmtKind, Target};
use super::params::MessageRef;
use super::{progress, Shared};
use crate::config::SinkMode;
use crate::error::{Error, Result};
use crate::pg::catalog::{table_info, TableInfo};
use crate::pg::connect::{classify, connect, EgressPolicy};
use crate::queen::{AckItem, AckRequest, AckStatus, PopRequest, Popped};
use crate::stop::Stop;

/// The long poll of a pop: the broker answers as soon as messages are ready,
/// at the latest after this.
pub const POP_WAIT_MS: u64 = 10_000;

/// Back-off of a failing worker (connect, pop, a batch that keeps failing):
/// 1 s doubling to 30 s, reset by the first success.
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Between two attempts of one message that the database refused: long
/// enough for a dependency in another partition (a parent row) to land,
/// short enough not to hold the lease for nothing.
const ATTEMPT_DELAY_MIN: Duration = Duration::from_millis(200);
const ATTEMPT_DELAY_MAX: Duration = Duration::from_secs(2);

/// How often a worker re-reads the target table's shape on its own: a column
/// added while the sink runs is written within this, without a restart.
const REFRESH_EVERY: Duration = Duration::from_secs(60);

/// Prepared statements per connection (one per column set and shape). A
/// queue whose payloads name ever-new column subsets would otherwise grow the
/// cache — and the server's memory — without bound; at the cap it starts over.
const MAX_CACHED_STATEMENTS: usize = 256;

/// A pop that answers empty at once (faster than [`IDLE_AT_ONCE`]: a broker
/// that does not long-poll) must not make the loop spin: wait this, doubling
/// up to the max, until a pop brings messages.
const IDLE_AT_ONCE: Duration = Duration::from_millis(5);
const IDLE_MIN: Duration = Duration::from_millis(25);
const IDLE_MAX: Duration = Duration::from_secs(1);

/// A lease extension that failed is tried again this soon.
const KEEPER_RETRY: Duration = Duration::from_secs(1);

/// An ack whose outcome is unknown is sent again (acks are idempotent) until
/// the lease ends, and for at least this long.
const ACK_PERSIST_MIN: Duration = Duration::from_secs(5);
const ACK_RETRY_MIN: Duration = Duration::from_millis(100);
const ACK_RETRY_MAX: Duration = Duration::from_secs(2);

/// The DLQ's error text: the server's message, its detail, capped.
const DLQ_TEXT_MAX: usize = 2_000;

/// SQLSTATE classes that describe the VALUES of a message rather than the
/// database: cardinality (21), data exception (22: bad input syntax, out of
/// range, too long), integrity constraint (23: not null, unique, foreign key,
/// check), triggered data change (27), WITH CHECK OPTION (44), program limit
/// (54: an index row too large — per message; per batch, halving cures it),
/// PL/pgSQL `RAISE` (P0).
pub fn is_data_state(code: &str) -> bool {
    matches!(
        code.get(..2),
        Some("21" | "22" | "23" | "27" | "44" | "54" | "P0")
    )
}

/// What a message ended as.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum Disp {
    Pending,
    Applied,
    Skipped,
    Dead(String),
}

/// Why a transaction did not commit.
#[derive(Debug)]
pub(super) enum Failure {
    /// Retry the batch after a back-off.
    Transient(Error),
    /// Not the data, not passing: back off, re-read the table, retry; never
    /// dead-letter.
    Env(Error),
    /// The data: `culprit` the messages of the failing statement (`None`:
    /// unknown, an error at COMMIT); `own` a payload this crate could not
    /// read (no server round trip to wait for between attempts).
    Data {
        culprit: Option<Vec<usize>>,
        text: String,
        own: bool,
    },
    /// Only an operator can fix it: the sink stops.
    Fatal(Error),
    /// Stopping, and the batch's leases ran out: leave the rest to the node
    /// the broker redelivers it to.
    Abandon,
}

/// The pieces a failed chunk is retried as, in order: what precedes the
/// culprit, the culprit, what follows. When the culprit is the whole chunk,
/// or unknown, its two halves. Every piece is smaller than the chunk, so the
/// isolation ends.
pub fn split(chunk: &[usize], culprit: Option<&[usize]>) -> Vec<Vec<usize>> {
    let halves = |c: &[usize]| {
        let mid = c.len() / 2;
        vec![c[..mid].to_vec(), c[mid..].to_vec()]
    };
    let found = culprit.and_then(|cul| {
        let start = chunk.iter().position(|i| cul.contains(i))?;
        let end = chunk.iter().rposition(|i| cul.contains(i))? + 1;
        Some((start, end))
    });
    match found {
        Some((start, end)) if start > 0 || end < chunk.len() => {
            [&chunk[..start], &chunk[start..end], &chunk[end..]]
                .into_iter()
                .filter(|p| !p.is_empty())
                .map(<[usize]>::to_vec)
                .collect()
        }
        _ => halves(chunk),
    }
}

/// Exponential back-off with a little jitter (workers that failed together
/// do not retry in lock step).
pub(super) struct Backoff {
    next: Duration,
}

impl Backoff {
    pub fn new() -> Backoff {
        Backoff { next: BACKOFF_MIN }
    }

    pub fn next_delay(&mut self) -> Duration {
        let d = self.next;
        self.next = (d * 2).min(BACKOFF_MAX);
        d.mul_f64(rand::random::<f64>() * 0.2 + 0.9)
    }

    pub fn reset(&mut self) {
        self.next = BACKOFF_MIN;
    }
}

fn attempt_delay(attempt: u32) -> Duration {
    ATTEMPT_DELAY_MIN
        .saturating_mul(1u32 << attempt.saturating_sub(1).min(16))
        .min(ATTEMPT_DELAY_MAX)
}

/// One popped batch, ordered for applying.
pub(super) struct Batch {
    msgs: Vec<Popped>,
    /// `(partition id, offset)` per message.
    keys: Vec<(i64, i64)>,
    /// Apply order (partition ids ascending, offsets ascending), repeats out.
    order: Vec<usize>,
    lease_ids: Vec<String>,
}

impl Batch {
    fn new(msgs: Vec<Popped>) -> std::result::Result<Batch, String> {
        let mut keys = Vec::with_capacity(msgs.len());
        for m in &msgs {
            let pid = m.partition_number().ok_or_else(|| {
                format!(
                    "the pop answered a non-numeric partitionId {:?}",
                    m.partition_id
                )
            })?;
            keys.push((pid, m.offset));
        }
        let (order, dups) = progress::apply_order(&keys);
        if !dups.is_empty() {
            tracing::warn!(target: "queen-pg", repeats = dups.len(), "a pop delivered the same offset twice; applying it once");
        }
        let mut lease_ids: Vec<String> = msgs
            .iter()
            .filter(|m| !m.lease_id.is_empty())
            .map(|m| m.lease_id.clone())
            .collect();
        lease_ids.sort();
        lease_ids.dedup();
        Ok(Batch {
            msgs,
            keys,
            order,
            lease_ids,
        })
    }

    fn msg_ref(&self, i: usize) -> MessageRef<'_> {
        let m = &self.msgs[i];
        MessageRef {
            payload: &m.data,
            partition: &m.partition,
            partition_id: &m.partition_id,
            offset: m.offset,
            transaction_id: &m.transaction_id,
            created_at: &m.created_at,
        }
    }
}

/// When the batch's leases end, as far as this worker knows: the pop's lease,
/// moved on by every extension that answered. And whether to extend them at
/// all: only while an attempt is in flight. A batch that failed waits out
/// its back-off WITHOUT extending, so a node that cannot reach the database
/// (or keeps failing on it) lets its partitions go to the nodes that can,
/// once the lease runs out; when it retries anyway, the progress rows
/// serialize it with whoever took over.
struct LeaseClock {
    deadline: Mutex<Instant>,
    holding: AtomicBool,
}

impl LeaseClock {
    fn new(secs: u32) -> LeaseClock {
        LeaseClock {
            deadline: Mutex::new(Instant::now() + Duration::from_secs(u64::from(secs))),
            holding: AtomicBool::new(true),
        }
    }

    fn hold(&self, on: bool) {
        self.holding.store(on, Ordering::SeqCst);
    }

    fn holding(&self) -> bool {
        self.holding.load(Ordering::SeqCst)
    }

    fn deadline(&self) -> Instant {
        *self.deadline.lock().unwrap_or_else(|p| p.into_inner())
    }

    fn extended(&self, secs: u32) {
        let mut d = self.deadline.lock().unwrap_or_else(|p| p.into_inner());
        *d = (*d).max(Instant::now() + Duration::from_secs(u64::from(secs)));
    }

    fn expired(&self) -> bool {
        Instant::now() >= self.deadline()
    }
}

/// Aborts the lease keeper when the batch is over, however it ends.
struct Keeper(tokio::task::JoinHandle<()>);

impl Drop for Keeper {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// A worker's connection and what it prepared on it.
struct Conn {
    client: Client,
    info: TableInfo,
    target: Target,
    loaded: Instant,
    ensure: Statement,
    lock: Statement,
    advance: Statement,
    sql: Option<Statement>,
    delete: Option<Statement>,
    truncate: Option<Statement>,
    cache: HashMap<(StmtKind, Vec<usize>), Statement>,
}

impl Conn {
    fn forget_statements(&mut self) {
        self.cache.clear();
        self.delete = None;
        self.truncate = None;
    }
}

/// `tokio-postgres` error → the crate's, by the shared classification.
fn pg(e: tokio_postgres::Error) -> Error {
    classify(&e)
}

/// A failure outside the data's statements (BEGIN, the progress rows,
/// preparing): transient or environment, never data — a progress table that
/// refuses a value must not dead-letter the queue.
fn env_failure(e: tokio_postgres::Error) -> Failure {
    let err = classify(&e);
    if err.is_retryable() {
        Failure::Transient(err)
    } else {
        Failure::Env(err)
    }
}

/// A failure of the data's statements or of COMMIT.
fn data_failure(e: &tokio_postgres::Error, culprit: Option<&[usize]>) -> Failure {
    let err = classify(e);
    if err.is_retryable() {
        return Failure::Transient(err);
    }
    match e.as_db_error() {
        Some(db) if is_data_state(db.code().code()) => {
            let mut text = format!("{}: {}", db.code().code(), db.message());
            if let Some(d) = db.detail() {
                text.push_str(" (");
                text.push_str(d);
                text.push(')');
            }
            Failure::Data {
                culprit: culprit.map(<[usize]>::to_vec),
                text: cap(text),
                own: false,
            }
        }
        _ => Failure::Env(err),
    }
}

fn cap(mut s: String) -> String {
    if s.len() > DLQ_TEXT_MAX {
        let mut end = DLQ_TEXT_MAX;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        s.truncate(end);
        s.push('…');
    }
    s
}

/// Read the target table and plan it for the sink's mode.
async fn load_target(client: &Client, sh: &Shared) -> Result<(TableInfo, Target)> {
    let info = table_info(client, &sh.table).await?.ok_or_else(|| {
        Error::fatal(
            "table_missing",
            format!(
                "{} does not exist (sink.table); create it, then the sink starts",
                sh.table
            ),
        )
    })?;
    let target = Target::build(
        &info,
        sh.spec.mode,
        sh.spec.key.as_deref(),
        sh.spec.metadata.as_ref(),
    )?;
    Ok((info, target))
}

/// Prepare sql mode's statement with every parameter typed `text`
/// (`params.rs`), and check it takes exactly the parameters configured.
async fn prepare_sql(client: &Client, statement: &str, n: usize) -> Result<Statement> {
    let types = vec![Type::TEXT; n];
    let st = client.prepare_typed(statement, &types).await.map_err(|e| {
        let err = classify(&e);
        if err.is_retryable() {
            err
        } else {
            Error::fatal(
                "statement",
                format!("sink.statement does not prepare: {err}"),
            )
        }
    })?;
    if st.params().len() != n || st.params().iter().any(|t| *t != Type::TEXT) {
        return Err(Error::fatal(
            "statement",
            format!(
                "sink.statement takes {} parameter(s) and sink.params lists {n}: one entry per $n",
                st.params().len()
            ),
        ));
    }
    Ok(st)
}

/// Everything a sink checks once before its workers start: the table exists
/// and fits the mode (its key has a unique index, the sql statement
/// prepares), the progress table exists (created when allowed) and is
/// writable. Each problem is a [`Error::Fatal`] naming the fix.
pub(super) async fn setup(sh: &Shared) -> Result<()> {
    let client = connect(
        &sh.connection,
        sh.password.as_deref(),
        &policy(sh),
        &super::application_name(&sh.ctx.name, "setup"),
    )
    .await?;
    let (_, target) = load_target(&client, sh).await?;
    progress::prepare_table(&client, &sh.progress, sh.spec.create_progress_table).await?;
    match sh.spec.mode {
        SinkMode::Upsert | SinkMode::Cdc => {
            // ON CONFLICT needs a unique index or constraint on exactly the
            // key: the server checks it while PLANNING (`explain_key_sql`).
            if let Err(e) = client.batch_execute(&target.explain_key_sql()).await {
                let err = classify(&e);
                if err.is_retryable() {
                    return Err(err);
                }
                let names: Vec<&str> = target
                    .key
                    .iter()
                    .map(|&i| target.columns[i].quoted.as_str())
                    .collect();
                return Err(Error::fatal(
                    "no_key",
                    format!(
                        "{} cannot upsert on ({}): {err}; the key needs a unique index or constraint",
                        sh.table,
                        names.join(", ")
                    ),
                ));
            }
        }
        SinkMode::Sql => {
            prepare_sql(
                &client,
                sh.statement.as_deref().unwrap_or_default(),
                sh.params.len(),
            )
            .await?;
        }
        SinkMode::Append => {}
    }
    Ok(())
}

fn policy(sh: &Shared) -> EgressPolicy {
    EgressPolicy {
        allow_private: sh.ctx.knobs.allow_private_networks,
    }
}

type PgFuture<'a> =
    Pin<Box<dyn Future<Output = std::result::Result<u64, tokio_postgres::Error>> + Send + 'a>>;

/// One op's statement call (not for [`Op::Move`], which is never pipelined).
fn exec<'a>(tx: &'a Transaction<'_>, op: &'a Op, st: &'a Statement) -> PgFuture<'a> {
    match op {
        Op::Append { json, .. } => Box::pin(tx.execute_raw(st, [json.as_str()])),
        Op::Upsert { row, .. } => Box::pin(tx.execute_raw(st, [row.json.as_str()])),
        Op::Delete { key, .. } => Box::pin(tx.execute_raw(st, [key.as_str()])),
        Op::Truncate { .. } => Box::pin(tx.execute_raw(st, std::iter::empty::<&str>())),
        Op::Sql { params, .. } => Box::pin(tx.execute_raw(st, params.iter().map(|p| p.as_deref()))),
        Op::Move { row, old_key, .. } => {
            Box::pin(tx.execute_raw(st, [row.json.as_str(), old_key.as_str()]))
        }
    }
}

/// Run `ops` in order. Independent statements are PIPELINED, up to
/// `window` at a time — all sent, then all answers read — so a batch of 500
/// upserts costs one round trip, not 500; the server still executes them one
/// after the other, and once one fails the rest fail with "transaction
/// aborted", so the FIRST error in order is the culprit. A move waits for its
/// row count (it decides whether the upsert follows), so it ends a pipeline.
/// The isolation of a failed batch runs with a window of 1: its pieces are
/// expected to fail, and every statement pipelined behind a failure would
/// put one more "current transaction is aborted" in the server's log.
/// `Err` carries the op index.
async fn run_ops(
    tx: &Transaction<'_>,
    ops: &[Op],
    stmts: &[(Statement, Option<Statement>)],
    window: usize,
) -> std::result::Result<(), (usize, tokio_postgres::Error)> {
    let mut i = 0;
    while i < ops.len() {
        if let Op::Move { row, old_key, .. } = &ops[i] {
            let (mv, up) = &stmts[i];
            let moved = tx
                .execute_raw(mv, [row.json.as_str(), old_key.as_str()])
                .await
                .map_err(|e| (i, e))?;
            if moved == 0 {
                // The old row is not in the table (the sink started after
                // it was written, or it was deleted): the new row is all
                // there is to write.
                let up = up.as_ref().expect("a move carries its upsert");
                tx.execute_raw(up, [row.json.as_str()])
                    .await
                    .map_err(|e| (i, e))?;
            }
            i += 1;
            continue;
        }
        let j = ops[i..]
            .iter()
            .position(|o| matches!(o, Op::Move { .. }))
            .map_or(ops.len(), |p| i + p)
            .min(i.saturating_add(window.max(1)));
        let futs: Vec<PgFuture<'_>> = (i..j).map(|k| exec(tx, &ops[k], &stmts[k].0)).collect();
        let results = futures_util::future::join_all(futs).await;
        if let Some((k, e)) = results
            .into_iter()
            .enumerate()
            .find_map(|(k, r)| r.err().map(|e| (k, e)))
        {
            return Err((i + k, e));
        }
        i = j;
    }
    Ok(())
}

/// A cached statement of `kind` for `cols`, prepared on first use.
async fn cached(
    tx: &Transaction<'_>,
    cache: &mut HashMap<(StmtKind, Vec<usize>), Statement>,
    target: &Target,
    kind: StmtKind,
    cols: &[usize],
) -> std::result::Result<Statement, tokio_postgres::Error> {
    let k = (kind, cols.to_vec());
    if let Some(s) = cache.get(&k) {
        return Ok(s.clone());
    }
    let n = if kind == StmtKind::Move { 2 } else { 1 };
    let s = tx
        .prepare_typed(&target.sql(kind, cols), &vec![Type::TEXT; n])
        .await?;
    if cache.len() >= MAX_CACHED_STATEMENTS {
        cache.clear();
    }
    cache.insert(k, s.clone());
    Ok(s)
}

/// One worker: one connection, one batch at a time.
pub(super) struct Worker {
    sh: Arc<Shared>,
    /// `0..workers`, for the connection's application_name and the logs.
    id: usize,
    stop: Stop,
    conn: Option<Conn>,
    /// Whether this worker's last step failed (counted in `Shared::failing`).
    failing: bool,
    backoff: Backoff,
}

impl Worker {
    pub fn new(sh: Arc<Shared>, id: usize, stop: Stop) -> Worker {
        Worker {
            sh,
            id,
            stop,
            conn: None,
            failing: false,
            backoff: Backoff::new(),
        }
    }

    fn failed(&mut self, e: &Error) {
        tracing::warn!(target: "queen-pg", connector = %self.sh.ctx.name, worker = self.id, error = %e, "sink worker failed; retrying after a back-off");
        self.sh.record_error(e);
        if !self.failing {
            self.failing = true;
            self.sh.failing.fetch_add(1, Ordering::SeqCst);
        }
        self.sh.status.set_phase("error");
    }

    fn recovered(&mut self) {
        self.backoff.reset();
        if self.failing {
            self.failing = false;
            if self.sh.failing.fetch_sub(1, Ordering::SeqCst) == 1 {
                self.sh.status.clear_error();
                self.sh.status.set_phase("running");
            }
        }
    }

    /// Run until stopped. `Err` only for what the worker cannot get past
    /// itself (Fatal, a refused login): the sink stops and the broker
    /// restarts it after its own back-off.
    pub async fn run(mut self) -> Result<()> {
        let r = self.run_loop().await;
        if self.failing {
            self.sh.failing.fetch_sub(1, Ordering::SeqCst);
            self.failing = false;
        }
        r
    }

    async fn run_loop(&mut self) -> Result<()> {
        let mut idle = Duration::ZERO;
        loop {
            if self.stop.is_stopped() {
                return Ok(());
            }
            if let Err(e) = self.ensure_conn().await {
                if !e.is_retryable() {
                    return Err(e);
                }
                self.failed(&e);
                let d = self.backoff.next_delay();
                if self.stop.sleep(d).await {
                    return Ok(());
                }
                continue;
            }
            let req = PopRequest {
                queue: self.sh.spec.queue.clone(),
                group: self.sh.group.clone(),
                batch: self.sh.spec.batch,
                wait_ms: POP_WAIT_MS,
                lease_seconds: self.sh.spec.lease_seconds,
                subscription_mode: self.sh.spec.subscription_mode.clone(),
                // The broker's autopilot sizes the claim: the group's ready
                // partitions shared among the pops waiting, at most 64. One
                // pop (and one PostgreSQL transaction, one ack) collects a
                // batch from several sparse partitions, while `workers`
                // pops of one node — and every node's — do not starve each
                // other by leasing everything ready.
                max_partitions: None,
            };
            let started = Instant::now();
            let answer = tokio::select! {
                r = self.sh.ctx.api.pop(req) => r,
                // A pop in flight is dropped: whatever it leased comes back
                // when the lease runs out, and is then applied once.
                _ = self.stop.wait() => return Ok(()),
            };
            match answer {
                Err(e) => {
                    let wait = e.retry_after_ms().map(Duration::from_millis);
                    self.failed(&Error::Queen(e));
                    let d = wait.unwrap_or_else(|| self.backoff.next_delay());
                    if self.stop.sleep(d).await {
                        return Ok(());
                    }
                }
                Ok(a) if a.messages.is_empty() => {
                    self.recovered();
                    if started.elapsed() < IDLE_AT_ONCE {
                        idle = (idle * 2).clamp(IDLE_MIN, IDLE_MAX);
                        if self.stop.sleep(idle).await {
                            return Ok(());
                        }
                    } else {
                        idle = Duration::ZERO;
                    }
                }
                Ok(a) => {
                    idle = Duration::ZERO;
                    self.process(a.messages).await?;
                }
            }
        }
    }

    /// A live connection, re-reading the table's shape every
    /// [`REFRESH_EVERY`].
    async fn ensure_conn(&mut self) -> Result<()> {
        match &self.conn {
            Some(c) if !c.client.is_closed() => {
                if c.loaded.elapsed() >= REFRESH_EVERY {
                    self.refresh().await?;
                }
                return Ok(());
            }
            _ => {}
        }
        self.conn = None;
        self.conn = Some(self.open().await?);
        Ok(())
    }

    async fn open(&self) -> Result<Conn> {
        let sh = &self.sh;
        let client = connect(
            &sh.connection,
            sh.password.as_deref(),
            &policy(sh),
            &super::application_name(&sh.ctx.name, &self.id.to_string()),
        )
        .await?;
        // A worker never idles inside its transaction; one that does is
        // hung (a half-dead socket), and its progress rows block the node
        // that took the partitions over. The server ends such a session
        // after a lease.
        let idle_ms = u64::from(sh.spec.lease_seconds).max(30) * 1_000;
        client
            .batch_execute(&format!(
                "SET idle_in_transaction_session_timeout = {idle_ms}"
            ))
            .await
            .map_err(pg)?;
        let (info, target) = load_target(&client, sh).await?;
        let p = &sh.progress;
        let ensure = client
            .prepare_typed(
                &progress::ensure_sql(p),
                &[Type::TEXT, Type::INT8_ARRAY, Type::TEXT_ARRAY],
            )
            .await
            .map_err(pg)?;
        let lock = client
            .prepare_typed(&progress::lock_sql(p), &[Type::TEXT, Type::INT8_ARRAY])
            .await
            .map_err(pg)?;
        let advance = client
            .prepare_typed(
                &progress::advance_sql(p),
                &[Type::TEXT, Type::INT8_ARRAY, Type::INT8_ARRAY],
            )
            .await
            .map_err(pg)?;
        let sql = match (&sh.spec.mode, &sh.statement) {
            (SinkMode::Sql, Some(s)) => Some(prepare_sql(&client, s, sh.params.len()).await?),
            _ => None,
        };
        Ok(Conn {
            client,
            info,
            target,
            loaded: Instant::now(),
            ensure,
            lock,
            advance,
            sql,
            delete: None,
            truncate: None,
            cache: HashMap::new(),
        })
    }

    /// Re-read the table; a changed shape replans and forgets the prepared
    /// statements.
    async fn refresh(&mut self) -> Result<()> {
        let sh = Arc::clone(&self.sh);
        let Some(c) = self.conn.as_mut() else {
            return Ok(());
        };
        let (info, target) = load_target(&c.client, &sh).await?;
        if info != c.info {
            tracing::info!(target: "queen-pg", connector = %sh.ctx.name, table = %sh.table, "the target table changed; replanning");
            c.info = info;
            c.target = target;
            c.forget_statements();
        }
        c.loaded = Instant::now();
        Ok(())
    }

    fn spawn_keeper(&self, b: &Batch, clock: &Arc<LeaseClock>) -> Keeper {
        let api = Arc::clone(&self.sh.ctx.api);
        let leases = b.lease_ids.clone();
        let secs = self.sh.spec.lease_seconds;
        let clock = Arc::clone(clock);
        let stop = self.stop.clone();
        let name = self.sh.ctx.name.clone();
        Keeper(tokio::spawn(async move {
            // Every third of the lease, so one failed extension (a leader
            // change, a 503) still leaves time for the next, which comes
            // within a second.
            let period = Duration::from_millis(u64::from(secs) * 1_000 / 3);
            let mut wait = period;
            loop {
                tokio::time::sleep(wait).await;
                // Stopping: the batch in flight finishes within the lease
                // it has, or is left to the broker's redelivery.
                if stop.is_stopped() {
                    return;
                }
                if !clock.holding() {
                    wait = period;
                    continue;
                }
                let mut all = true;
                for l in &leases {
                    if let Err(e) = api.extend_lease(l.clone(), secs).await {
                        all = false;
                        tracing::debug!(target: "queen-pg", connector = %name, error = %e, "lease extension failed");
                    }
                }
                if all {
                    clock.extended(secs);
                    wait = period;
                } else {
                    wait = period.min(KEEPER_RETRY);
                }
            }
        }))
    }

    /// One batch, from pop to ack. `Err` only when the sink must stop.
    async fn process(&mut self, msgs: Vec<Popped>) -> Result<()> {
        let batch = match Batch::new(msgs) {
            Ok(b) => b,
            Err(why) => {
                // Unreadable: nothing is acked, the leases run out.
                self.failed(&Error::io(why));
                return Ok(());
            }
        };
        let clock = Arc::new(LeaseClock::new(self.sh.spec.lease_seconds));
        let keeper = self.spawn_keeper(&batch, &clock);
        let mut disp = vec![Disp::Pending; batch.msgs.len()];
        let mut backoff = Backoff::new();
        loop {
            clock.hold(true);
            let r = match self.ensure_conn().await {
                Ok(()) => self.apply_batch(&batch, &mut disp, &clock).await,
                Err(e) if !e.is_retryable() => Err(Failure::Fatal(e)),
                Err(e) => Err(Failure::Transient(e)),
            };
            let (e, env) = match r {
                Ok(()) => {
                    self.recovered();
                    break;
                }
                Err(Failure::Abandon) => break,
                Err(Failure::Fatal(e)) => return Err(e),
                Err(Failure::Transient(e)) => (e, false),
                Err(Failure::Env(e)) => (e, true),
                Err(Failure::Data { .. }) => unreachable!("apply_batch isolates data errors"),
            };
            clock.hold(false);
            self.failed(&e);
            if self.conn.as_ref().is_some_and(|c| c.client.is_closed()) {
                self.conn = None;
            }
            if env {
                // A cached plan may be what is stale (a dropped column); a
                // missing table now is Fatal.
                if let Some(c) = self.conn.as_mut() {
                    c.forget_statements();
                }
                if let Err(e) = self.refresh().await {
                    if !e.is_retryable() {
                        return Err(e);
                    }
                }
            }
            // Stopping: a failed batch is not retried; its leases bring it
            // back elsewhere (or here, after a restart).
            if self.stop.is_stopped() || self.stop.sleep(backoff.next_delay()).await {
                break;
            }
        }
        drop(keeper);
        self.ack(&batch, &disp, &clock).await;
        self.sh.account(&disp);
        Ok(())
    }

    /// Apply every pending message of `b`, isolating the data errors.
    async fn apply_batch(
        &mut self,
        b: &Batch,
        disp: &mut [Disp],
        clock: &LeaseClock,
    ) -> std::result::Result<(), Failure> {
        let max_attempts = self.sh.spec.max_attempts.max(1);
        // Partitions stopped at a dead letter stay stopped across retries of
        // the batch.
        let mut stopped: HashSet<i64> = b
            .order
            .iter()
            .filter(|&&i| matches!(disp[i], Disp::Dead(_)))
            .map(|&i| b.keys[i].0)
            .collect();
        let mut attempts: HashMap<usize, u32> = HashMap::new();
        let mut work: VecDeque<Vec<usize>> = VecDeque::from([b.order.clone()]);
        // Pipelined until the first data error of the batch.
        let mut isolating = false;
        while let Some(chunk) = work.pop_front() {
            let chunk: Vec<usize> = chunk
                .into_iter()
                .filter(|&i| disp[i] == Disp::Pending && !stopped.contains(&b.keys[i].0))
                .collect();
            if chunk.is_empty() {
                continue;
            }
            if self.stop.is_stopped() && clock.expired() {
                return Err(Failure::Abandon);
            }
            match self.try_chunk(b, &chunk, !isolating).await {
                Ok((applied, skipped)) => {
                    for i in applied {
                        disp[i] = Disp::Applied;
                    }
                    for i in skipped {
                        disp[i] = Disp::Skipped;
                    }
                }
                Err(Failure::Data { culprit, text, own }) => {
                    isolating = true;
                    if let [m] = chunk[..] {
                        let n = attempts.entry(m).or_insert(0);
                        *n += 1;
                        if *n >= max_attempts {
                            let msg = &b.msgs[m];
                            tracing::warn!(
                                target: "queen-pg",
                                connector = %self.sh.ctx.name,
                                partition = %msg.partition,
                                offset = msg.offset,
                                error = %text,
                                "message refused {} time(s); dead-lettering it",
                                *n
                            );
                            self.sh.record_dead_letter(msg, &text);
                            stopped.insert(b.keys[m].0);
                            disp[m] = Disp::Dead(text);
                        } else {
                            if !own {
                                tokio::time::sleep(attempt_delay(*n)).await;
                            }
                            work.push_front(chunk);
                        }
                    } else {
                        for piece in split(&chunk, culprit.as_deref()).into_iter().rev() {
                            work.push_front(piece);
                        }
                    }
                }
                Err(other) => return Err(other),
            }
        }
        Ok(())
    }

    /// One transaction: the progress rows of `chunk`'s partitions, locked;
    /// the messages above them applied; the progress advanced; COMMIT.
    /// `(applied, skipped)` when it committed.
    async fn try_chunk(
        &mut self,
        b: &Batch,
        chunk: &[usize],
        pipelined: bool,
    ) -> std::result::Result<(Vec<usize>, Vec<usize>), Failure> {
        let sh = Arc::clone(&self.sh);
        let c = self
            .conn
            .as_mut()
            .ok_or_else(|| Failure::Transient(Error::io("no connection")))?;
        let parts = progress::partitions_of(chunk, &b.keys);
        let pids: Vec<i64> = parts.iter().map(|p| p.0).collect();
        let maxes: Vec<i64> = parts.iter().map(|p| p.1).collect();
        let names: Vec<&str> = parts
            .iter()
            .map(|p| b.msgs[p.2].partition.as_str())
            .collect();
        let sink: &str = &sh.sink_key;

        let tx = c
            .client
            .build_transaction()
            .isolation_level(IsolationLevel::ReadCommitted)
            .start()
            .await
            .map_err(env_failure)?;
        let ensure_p: [&(dyn ToSql + Sync); 3] = [&sink, &pids, &names];
        let lock_p: [&(dyn ToSql + Sync); 2] = [&sink, &pids];
        let (_, rows) =
            tokio::try_join!(tx.execute(&c.ensure, &ensure_p), tx.query(&c.lock, &lock_p))
                .map_err(env_failure)?;
        let mut last = HashMap::with_capacity(rows.len());
        for r in &rows {
            let pid: i64 = r.try_get(0).map_err(env_failure)?;
            let off: i64 = r.try_get(1).map_err(env_failure)?;
            last.insert(pid, off);
        }
        let (apply, skipped) = progress::filter(chunk, &b.keys, &last);

        if !apply.is_empty() {
            let refs: Vec<(usize, MessageRef<'_>)> =
                apply.iter().map(|&i| (i, b.msg_ref(i))).collect();
            let ops = build_ops(&c.target, sh.spec.mode, &sh.params, &refs).map_err(|e| {
                Failure::Data {
                    culprit: Some(vec![e.msg]),
                    text: cap(format!("payload: {}", e.text)),
                    own: true,
                }
            })?;
            // Prepared before anything is sent, so the pipeline below only
            // carries executions (a prepare inside it would reorder it).
            let mut stmts: Vec<(Statement, Option<Statement>)> = Vec::with_capacity(ops.len());
            for op in &ops {
                let (cache, target) = (&mut c.cache, &c.target);
                stmts.push(match op {
                    Op::Append { cols, .. } => (
                        cached(&tx, cache, target, StmtKind::Append, cols)
                            .await
                            .map_err(env_failure)?,
                        None,
                    ),
                    Op::Upsert { row, .. } => (
                        cached(&tx, cache, target, StmtKind::Upsert, &row.cols)
                            .await
                            .map_err(env_failure)?,
                        None,
                    ),
                    Op::Move { row, .. } => (
                        cached(&tx, cache, target, StmtKind::Move, &row.cols)
                            .await
                            .map_err(env_failure)?,
                        Some(
                            cached(&tx, cache, target, StmtKind::Upsert, &row.cols)
                                .await
                                .map_err(env_failure)?,
                        ),
                    ),
                    Op::Delete { .. } => {
                        if c.delete.is_none() {
                            let st = tx
                                .prepare_typed(&target.delete_sql(), &[Type::TEXT])
                                .await
                                .map_err(env_failure)?;
                            c.delete = Some(st);
                        }
                        (c.delete.clone().expect("prepared above"), None)
                    }
                    Op::Truncate { .. } => {
                        if c.truncate.is_none() {
                            let st = tx
                                .prepare_typed(&target.truncate_sql(), &[])
                                .await
                                .map_err(env_failure)?;
                            c.truncate = Some(st);
                        }
                        (c.truncate.clone().expect("prepared above"), None)
                    }
                    Op::Sql { .. } => (
                        c.sql.clone().ok_or_else(|| {
                            Failure::Fatal(Error::config("sql mode without a statement"))
                        })?,
                        None,
                    ),
                });
            }
            let window = if pipelined { usize::MAX } else { 1 };
            run_ops(&tx, &ops, &stmts, window)
                .await
                .map_err(|(k, e)| data_failure(&e, Some(ops[k].msgs())))?;
        }

        let adv_p: [&(dyn ToSql + Sync); 3] = [&sink, &pids, &maxes];
        tx.execute(&c.advance, &adv_p).await.map_err(env_failure)?;
        // A deferred constraint fails here, for no statement in particular.
        tx.commit().await.map_err(|e| data_failure(&e, None))?;
        Ok((apply, skipped))
    }

    /// Ack what the batch settled: applied and skipped `completed`, dead
    /// letters `dlq` with their error; a stopped partition's tail not at all.
    /// An ack whose outcome is unknown is sent again — acks are idempotent —
    /// until the lease ends (at least [`ACK_PERSIST_MIN`]). An ack that is
    /// refused (an expired lease) is not an error: the redelivery meets the
    /// progress row.
    async fn ack(&self, b: &Batch, disp: &[Disp], clock: &LeaseClock) {
        let items: Vec<AckItem> = b
            .order
            .iter()
            .filter_map(|&i| {
                let (status, error) = match &disp[i] {
                    Disp::Applied | Disp::Skipped => (AckStatus::Ok, None),
                    Disp::Dead(text) => (AckStatus::Dlq, Some(text.clone())),
                    Disp::Pending => return None,
                };
                let m = &b.msgs[i];
                Some(AckItem {
                    transaction_id: m.transaction_id.clone(),
                    partition_id: m.partition_id.clone(),
                    lease_id: m.lease_id.clone(),
                    status,
                    error,
                })
            })
            .collect();
        if items.is_empty() {
            return;
        }
        let req = AckRequest {
            group: self.sh.group.clone(),
            items,
        };
        let give_up = clock.deadline().max(Instant::now() + ACK_PERSIST_MIN);
        let mut delay = ACK_RETRY_MIN;
        loop {
            match self.sh.ctx.api.ack(req.clone()).await {
                Ok(a) => {
                    let refused = a.results.iter().filter(|r| !r.success).count();
                    if refused > 0 {
                        tracing::debug!(target: "queen-pg", connector = %self.sh.ctx.name, refused, "acks refused (an expired lease); the redelivery is dropped by the progress rows");
                    }
                    return;
                }
                Err(e) if e.is_retryable() && Instant::now() < give_up => {
                    tokio::time::sleep(
                        e.retry_after_ms()
                            .map(Duration::from_millis)
                            .unwrap_or(delay),
                    )
                    .await;
                    delay = (delay * 2).min(ACK_RETRY_MAX);
                }
                Err(e) => {
                    tracing::warn!(target: "queen-pg", connector = %self.sh.ctx.name, error = %e, "ack failed; the batch is redelivered after its lease and dropped by the progress rows");
                    return;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_states_are_the_values_classes() {
        for s in [
            "22P02", "22003", "22001", "23505", "23502", "23503", "23514", "21000", "27000",
            "44000", "54000", "P0001",
        ] {
            assert!(is_data_state(s), "{s}");
        }
        for s in [
            "42P01", "42703", "42501", "40001", "40P01", "57P01", "08006", "53100", "25P02",
            "0A000", "XX000", "55P03", "",
        ] {
            assert!(!is_data_state(s), "{s}");
        }
    }

    #[test]
    fn a_failed_chunk_splits_around_its_culprit() {
        let chunk = [1, 2, 3, 4, 5, 6];
        assert_eq!(
            split(&chunk, Some(&[3])),
            vec![vec![1, 2], vec![3], vec![4, 5, 6]]
        );
        assert_eq!(
            split(&chunk, Some(&[1])),
            vec![vec![1], vec![2, 3, 4, 5, 6]]
        );
        assert_eq!(
            split(&chunk, Some(&[6])),
            vec![vec![1, 2, 3, 4, 5], vec![6]]
        );
        // An append run in the middle.
        assert_eq!(
            split(&chunk, Some(&[3, 4])),
            vec![vec![1, 2], vec![3, 4], vec![5, 6]]
        );
        // The whole chunk, or nobody in particular: halves.
        assert_eq!(
            split(&chunk, Some(&chunk)),
            vec![vec![1, 2, 3], vec![4, 5, 6]]
        );
        assert_eq!(split(&chunk, None), vec![vec![1, 2, 3], vec![4, 5, 6]]);
        assert_eq!(split(&[7, 8], None), vec![vec![7], vec![8]]);
        // A culprit run spanning skipped members (not in the culprit list).
        assert_eq!(split(&[1, 2, 3], Some(&[1, 3])), vec![vec![1], vec![2, 3]]);
        // A culprit not in the chunk at all: halves.
        assert_eq!(
            split(&[1, 2, 3, 4], Some(&[9])),
            vec![vec![1, 2], vec![3, 4]]
        );
    }

    #[test]
    fn isolation_always_terminates_and_finds_every_culprit() {
        // Simulate the isolation loop over a chunk of 37 messages where 3 are
        // poison and an append-like op covers runs of 5: each transaction
        // fails on its first poison, naming the run that holds it.
        let poison = [4usize, 17, 30];
        let mut work: VecDeque<Vec<usize>> = VecDeque::from([(0..37).collect::<Vec<_>>()]);
        let mut dead = Vec::new();
        let mut applied = Vec::new();
        let mut txns = 0;
        while let Some(chunk) = work.pop_front() {
            txns += 1;
            assert!(txns < 200, "isolation must terminate");
            match chunk.iter().find(|i| poison.contains(i)) {
                None => applied.extend(chunk),
                Some(&p) if chunk.len() == 1 => dead.push(p),
                Some(&p) => {
                    let run: Vec<usize> =
                        chunk.iter().copied().filter(|i| i / 5 == p / 5).collect();
                    for piece in split(&chunk, Some(&run)).into_iter().rev() {
                        work.push_front(piece);
                    }
                }
            }
        }
        assert_eq!(dead, vec![4, 17, 30]);
        applied.sort();
        assert_eq!(applied.len(), 34);
        assert!(applied.iter().all(|i| !poison.contains(i)));
    }

    #[test]
    fn attempt_delays_grow_and_stop_growing() {
        assert_eq!(attempt_delay(1), Duration::from_millis(200));
        assert_eq!(attempt_delay(2), Duration::from_millis(400));
        assert_eq!(attempt_delay(5), Duration::from_secs(2));
        assert_eq!(attempt_delay(100), Duration::from_secs(2));
        let mut b = Backoff::new();
        let d1 = b.next_delay();
        assert!(d1 >= Duration::from_millis(900) && d1 <= Duration::from_millis(1100));
        for _ in 0..10 {
            b.next_delay();
        }
        assert!(b.next_delay() <= Duration::from_secs(33));
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(1100));
    }

    #[test]
    fn dlq_text_is_capped_on_a_char_boundary() {
        let s = cap("é".repeat(3_000));
        assert!(s.len() <= DLQ_TEXT_MAX + '…'.len_utf8());
        assert!(s.ends_with('…'));
        assert_eq!(cap("short".into()), "short");
    }
}
