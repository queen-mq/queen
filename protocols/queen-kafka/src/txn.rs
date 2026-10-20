//! Kafka transactions: buffer on the raft leader, commit as ONE Queen
//! transaction.
//!
//! A transactional producer's records and offsets are held in memory on the
//! raft leader (which is every partition's leader and every coordinator, so
//! Produce, TxnOffsetCommit and EndTxn all reach it) until `EndTxn(commit)`,
//! which writes them through one `POST /api/v1/transaction`: pushes, positions
//! and the fence below, one raft entry, all or nothing. Abort drops them. No
//! uncommitted record ever enters the log, so `read_committed` is free (the
//! last stable offset is the high watermark) and there are no markers.
//!
//! The identity is durable, in one KV row per `transactional.id`:
//!
//! ```text
//!   namespace  queen-kafka
//!   key        qk:txn:<transactional.id>
//!   value      {"pid":<i64>,"epoch":<i16>,"state":"ready"|"open"|"committed"|"aborted","token":<u64>}
//! ```
//!
//! Every step writes it with `expect` = the version this leader last saw, so a
//! newer producer (InitProducerId bumps the epoch) or another leader fences
//! this one: a commit carries that write as a `required` precondition, and a
//! fenced commit writes nothing at all. `"open"` is written when a
//! transaction starts, so a new leader that meets it knows a transaction's
//! records were held by the old one and are lost: that transaction can only
//! abort (`INVALID_TXN_STATE` to anything else). `token` names the transaction,
//! so a commit whose answer was lost is recognized when the client retries it.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde_json::{json, Value};

use crate::core::{CallError, PositionOp, Queen, RawPush};
use crate::errors::*;
use crate::topics::{now_ms, NS};

pub const KEY_PREFIX: &str = "qk:txn:";
/// Record bytes one transaction may hold.
pub const MAX_TXN_BYTES: usize = 8 << 20;
/// Records one transaction may hold.
pub const MAX_TXN_RECORDS: usize = 50_000;
/// Partitions one transaction may span.
pub const MAX_TXN_PARTITIONS: usize = 1_000;
/// Record bytes held across every open transaction of this node.
pub const MAX_HELD_BYTES: usize = 256 << 20;
/// `transaction.max.timeout.ms`: one hour, Flink's default ask.
pub const MAX_TIMEOUT_MS: i32 = 3_600_000;

pub const PRODUCER_FENCED: i16 = 90;
pub const INVALID_PRODUCER_EPOCH: i16 = 47;
pub const INVALID_TXN_STATE: i16 = 48;
pub const INVALID_PRODUCER_ID_MAPPING: i16 = 49;
pub const INVALID_TRANSACTION_TIMEOUT: i16 = 50;
pub const CONCURRENT_TRANSACTIONS: i16 = 51;

pub fn key(tid: &str) -> String {
    format!("{KEY_PREFIX}{tid}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Stage {
    /// No transaction open.
    Ready,
    /// A transaction this node holds.
    Open(Open),
    /// The row says a transaction was open, and its records were held by a
    /// leader that is gone: it can only abort.
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
struct Open {
    token: u64,
    started: Option<Instant>,
    partitions: BTreeSet<(String, i32)>,
    groups: BTreeSet<String>,
    records: Vec<RawPush>,
    bytes: usize,
    offsets: BTreeMap<(String, String, i32), PositionOp>,
    /// A commit was sent and its outcome is unknown.
    committing: bool,
}

#[derive(Debug, Clone)]
struct Txn {
    pid: i64,
    epoch: i16,
    /// The KV row's version as this node last wrote or read it.
    version: i64,
    timeout: Duration,
    stage: Stage,
    /// The last transaction ended here: its token and whether it committed,
    /// for an EndTxn retried after its answer was lost.
    last_end: Option<(u64, bool)>,
    /// The producer's token: what the timeout sweep writes the row with.
    bearer: Option<String>,
}

/// The transaction coordinator of this node: meaningful only while it leads.
pub struct Txns {
    queen: Queen,
    inner: tokio::sync::Mutex<HashMap<String, Txn>>,
    held: AtomicU64,
    tokens: AtomicU64,
    node: Mutex<i64>,
}

/// The row as stored.
#[derive(Debug, Clone, PartialEq)]
struct Row {
    pid: i64,
    epoch: i16,
    state: String,
    token: u64,
    version: i64,
}

impl Txns {
    pub fn new(queen: Queen) -> Txns {
        Txns {
            queen,
            inner: tokio::sync::Mutex::new(HashMap::new()),
            held: AtomicU64::new(0),
            tokens: AtomicU64::new(now_ms() as u64),
            node: Mutex::new(0),
        }
    }

    pub fn set_node(&self, node: i32) {
        *self.node.lock().unwrap_or_else(|p| p.into_inner()) = i64::from(node.max(0));
    }

    /// This node stopped leading: what it held is lost, and the rows say so.
    pub async fn clear(&self) {
        let mut inner = self.inner.lock().await;
        for (_, t) in inner.drain() {
            if let Stage::Open(o) = t.stage {
                self.held.fetch_sub(o.bytes as u64, Ordering::Relaxed);
            }
        }
    }

    fn new_pid(&self) -> i64 {
        let seq = self.tokens.fetch_add(1, Ordering::Relaxed) as i64;
        let node = *self.node.lock().unwrap_or_else(|p| p.into_inner());
        // Bit 62 set: never one of the idempotent ids (clock << 20, below it).
        (1i64 << 62) | ((now_ms() & ((1 << 40) - 1)) << 20) | ((node & 0xf) << 16) | (seq & 0xffff)
    }

    fn new_token(&self) -> u64 {
        self.tokens.fetch_add(1, Ordering::Relaxed)
    }

    async fn read_row(&self, bearer: Option<&str>, tid: &str) -> Result<Option<Row>, CallError> {
        let ops = [crate::core::KvOp::GetMany {
            ns: NS.to_string(),
            keys: vec![key(tid)],
        }];
        let answer = self.queen.kv(bearer, &ops).await?.into_iter().next();
        let Some(row) = answer.and_then(|a| a.rows.into_iter().next()) else {
            return Ok(None);
        };
        let v = &row.value;
        Ok(Some(Row {
            pid: v.get("pid").and_then(Value::as_i64).unwrap_or(-1),
            epoch: v.get("epoch").and_then(Value::as_i64).unwrap_or(-1) as i16,
            state: v
                .get("state")
                .and_then(Value::as_str)
                .unwrap_or("ready")
                .to_string(),
            token: v.get("token").and_then(Value::as_u64).unwrap_or(0),
            version: row.version,
        }))
    }

    /// Write the row at `expect`; the new version, or `None` when the version
    /// moved (another producer or leader wrote it).
    async fn write_row(
        &self,
        bearer: Option<&str>,
        tid: &str,
        expect: i64,
        value: Value,
    ) -> Result<Option<i64>, CallError> {
        let ops = [crate::core::KvOp::put_at(NS, &key(tid), value, expect)];
        let a = self.queen.kv(bearer, &ops).await?.into_iter().next();
        Ok(a.filter(|a| a.applied != Some(false)).map(|a| a.version))
    }

    /// InitProducerId for a transactional id: the id it keeps across restarts,
    /// at the next epoch, which fences every older producer of it.
    pub async fn init(
        &self,
        bearer: Option<&str>,
        tid: &str,
        timeout_ms: i32,
        asked: (i64, i16),
    ) -> Result<(i64, i16), i16> {
        if !(1..=MAX_TIMEOUT_MS).contains(&timeout_ms) {
            return Err(INVALID_TRANSACTION_TIMEOUT);
        }
        let mut inner = self.inner.lock().await;
        let row = self
            .read_row(bearer, tid)
            .await
            .map_err(|e| of_call_for_group(&e))?;
        let (pid, epoch, expect) = match &row {
            None => (self.new_pid(), 0i16, 0i64),
            Some(r) => {
                // KIP-360: a producer that names its id and epoch must still
                // hold them.
                if asked.0 >= 0 && (asked.0 != r.pid || asked.1 != r.epoch) {
                    return Err(PRODUCER_FENCED);
                }
                if r.epoch >= i16::MAX - 1 {
                    (self.new_pid(), 0, r.version)
                } else {
                    (r.pid, r.epoch + 1, r.version)
                }
            }
        };
        let value = json!({"pid": pid, "epoch": epoch, "state": "ready", "token": 0});
        let version = match self.write_row(bearer, tid, expect, value).await {
            Ok(Some(v)) => v,
            Ok(None) => return Err(CONCURRENT_TRANSACTIONS),
            Err(e) => return Err(of_call_for_group(&e)),
        };
        if let Some(old) = inner.insert(
            tid.to_string(),
            Txn {
                pid,
                epoch,
                version,
                timeout: Duration::from_millis(timeout_ms as u64),
                stage: Stage::Ready,
                last_end: None,
                bearer: bearer.map(str::to_string),
            },
        ) {
            if let Stage::Open(o) = old.stage {
                self.held.fetch_sub(o.bytes as u64, Ordering::Relaxed);
                tracing::info!(target: "kafka", %tid, "a newer producer fenced an open transaction; its records are dropped");
            }
        }
        Ok((pid, epoch))
    }

    /// The transaction of `tid`, checked against the producer's id and epoch,
    /// read from the row when this node does not hold it.
    async fn held<'a>(
        &self,
        inner: &'a mut HashMap<String, Txn>,
        bearer: Option<&str>,
        tid: &str,
        pid: i64,
        epoch: i16,
    ) -> Result<&'a mut Txn, i16> {
        if !inner.contains_key(tid) {
            let row = self
                .read_row(bearer, tid)
                .await
                .map_err(|e| of_call_for_group(&e))?
                .ok_or(INVALID_PRODUCER_ID_MAPPING)?;
            inner.insert(
                tid.to_string(),
                Txn {
                    pid: row.pid,
                    epoch: row.epoch,
                    version: row.version,
                    timeout: Duration::from_secs(60),
                    stage: if row.state == "open" {
                        Stage::Lost
                    } else {
                        Stage::Ready
                    },
                    last_end: matches!(row.state.as_str(), "committed" | "aborted")
                        .then_some((row.token, row.state == "committed")),
                    bearer: None,
                },
            );
        }
        let t = inner.get_mut(tid).expect("inserted above");
        if t.pid != pid {
            return Err(INVALID_PRODUCER_ID_MAPPING);
        }
        if t.epoch != epoch {
            return Err(INVALID_PRODUCER_EPOCH);
        }
        if bearer.is_some() && t.bearer.as_deref() != bearer {
            t.bearer = bearer.map(str::to_string);
        }
        Ok(t)
    }

    /// Open a transaction on a Ready one: the row says `open` from here on.
    async fn begin(&self, bearer: Option<&str>, tid: &str, t: &mut Txn) -> Result<(), i16> {
        match &t.stage {
            Stage::Open(_) => return Ok(()),
            Stage::Lost => return Err(INVALID_TXN_STATE),
            Stage::Ready => {}
        }
        let token = self.new_token();
        let value = json!({"pid": t.pid, "epoch": t.epoch, "state": "open", "token": token});
        match self.write_row(bearer, tid, t.version, value).await {
            Ok(Some(v)) => {
                t.version = v;
                t.stage = Stage::Open(Open {
                    token,
                    started: Some(Instant::now()),
                    ..Open::default()
                });
                Ok(())
            }
            Ok(None) => Err(PRODUCER_FENCED),
            Err(e) => Err(of_call_for_group(&e)),
        }
    }

    pub async fn add_partitions(
        &self,
        bearer: Option<&str>,
        tid: &str,
        pid: i64,
        epoch: i16,
        partitions: &[(String, i32)],
    ) -> i16 {
        let mut inner = self.inner.lock().await;
        let t = match self.held(&mut inner, bearer, tid, pid, epoch).await {
            Ok(t) => t,
            Err(e) => return e,
        };
        if let Err(e) = self.begin(bearer, tid, t).await {
            return e;
        }
        let Stage::Open(o) = &mut t.stage else {
            return INVALID_TXN_STATE;
        };
        if o.committing {
            return CONCURRENT_TRANSACTIONS;
        }
        for p in partitions {
            o.partitions.insert(p.clone());
        }
        if o.partitions.len() > MAX_TXN_PARTITIONS {
            return INVALID_TXN_STATE;
        }
        NONE
    }

    pub async fn add_offsets(
        &self,
        bearer: Option<&str>,
        tid: &str,
        pid: i64,
        epoch: i16,
        group: &str,
    ) -> i16 {
        let mut inner = self.inner.lock().await;
        let t = match self.held(&mut inner, bearer, tid, pid, epoch).await {
            Ok(t) => t,
            Err(e) => return e,
        };
        if let Err(e) = self.begin(bearer, tid, t).await {
            return e;
        }
        let Stage::Open(o) = &mut t.stage else {
            return INVALID_TXN_STATE;
        };
        o.groups.insert(group.to_string());
        NONE
    }

    /// The records of one transactional produce, held until the commit.
    pub async fn stage_records(
        &self,
        bearer: Option<&str>,
        tid: &str,
        pid: i64,
        epoch: i16,
        partition: (&str, i32),
        records: Vec<RawPush>,
    ) -> i16 {
        let mut inner = self.inner.lock().await;
        let t = match self.held(&mut inner, bearer, tid, pid, epoch).await {
            Ok(t) => t,
            Err(e) => return e,
        };
        let Stage::Open(o) = &mut t.stage else {
            return INVALID_TXN_STATE;
        };
        if o.committing {
            return CONCURRENT_TRANSACTIONS;
        }
        if !o
            .partitions
            .contains(&(partition.0.to_string(), partition.1))
        {
            return INVALID_TXN_STATE;
        }
        let bytes: usize = records.iter().map(|r| r.payload.len()).sum();
        if o.records.len() + records.len() > MAX_TXN_RECORDS
            || o.bytes + bytes > MAX_TXN_BYTES
            || self.held.load(Ordering::Relaxed) as usize + bytes > MAX_HELD_BYTES
        {
            return MESSAGE_TOO_LARGE;
        }
        self.held.fetch_add(bytes as u64, Ordering::Relaxed);
        o.bytes += bytes;
        o.records.extend(records);
        NONE
    }

    /// The offsets of one TxnOffsetCommit, held until the commit.
    pub async fn stage_offsets(
        &self,
        bearer: Option<&str>,
        tid: &str,
        pid: i64,
        epoch: i16,
        offsets: Vec<PositionOp>,
    ) -> i16 {
        let mut inner = self.inner.lock().await;
        let t = match self.held(&mut inner, bearer, tid, pid, epoch).await {
            Ok(t) => t,
            Err(e) => return e,
        };
        if let Err(e) = self.begin(bearer, tid, t).await {
            return e;
        }
        let Stage::Open(o) = &mut t.stage else {
            return INVALID_TXN_STATE;
        };
        if o.committing {
            return CONCURRENT_TRANSACTIONS;
        }
        for op in offsets {
            let p: i32 = op.partition.parse().unwrap_or(-1);
            o.groups.insert(op.group.clone());
            o.offsets
                .insert((op.group.clone(), op.queue.clone(), p), op);
        }
        NONE
    }

    /// EndTxn.
    pub async fn end(
        &self,
        bearer: Option<&str>,
        tid: &str,
        pid: i64,
        epoch: i16,
        commit: bool,
    ) -> i16 {
        let mut inner = self.inner.lock().await;
        let t = match self.held(&mut inner, bearer, tid, pid, epoch).await {
            Ok(t) => t,
            Err(e) => return e,
        };
        let open = match &t.stage {
            Stage::Open(o) => o.clone(),
            Stage::Ready => {
                // A retry of an EndTxn whose answer was lost, or a transaction
                // that never added anything.
                return match t.last_end {
                    Some((_, c)) if c == commit => NONE,
                    _ if !commit => NONE,
                    _ => INVALID_TXN_STATE,
                };
            }
            Stage::Lost if commit => return INVALID_TXN_STATE,
            Stage::Lost => Open::default(),
        };
        if !commit {
            let value =
                json!({"pid": t.pid, "epoch": t.epoch, "state": "aborted", "token": open.token});
            return match self.write_row(bearer, tid, t.version, value).await {
                Ok(Some(v)) => {
                    t.version = v;
                    self.held.fetch_sub(open.bytes as u64, Ordering::Relaxed);
                    t.stage = Stage::Ready;
                    t.last_end = Some((open.token, false));
                    NONE
                }
                Ok(None) => PRODUCER_FENCED,
                Err(e) => of_call_for_group(&e),
            };
        }
        // A commit retried after an unknown outcome: did the first one land?
        if open.committing {
            match self.read_row(bearer, tid).await {
                Ok(Some(r)) if r.token == open.token && r.state == "committed" => {
                    t.version = r.version;
                    self.held.fetch_sub(open.bytes as u64, Ordering::Relaxed);
                    t.stage = Stage::Ready;
                    t.last_end = Some((open.token, true));
                    return NONE;
                }
                Ok(_) => {}
                Err(e) => return of_call_for_group(&e),
            }
        }
        if let Stage::Open(o) = &mut t.stage {
            o.committing = true;
        }
        let fence = crate::core::KvOp::fence(
            NS,
            &key(tid),
            json!({"pid": t.pid, "epoch": t.epoch, "state": "committed", "token": open.token}),
            t.version,
        );
        let positions: Vec<PositionOp> = open.offsets.values().cloned().collect();
        let outcome = self
            .queen
            .transaction(bearer, &open.records, &[fence], &positions)
            .await;
        let mut forget = false;
        let code = match outcome {
            Ok(Committed::Done { fence_version }) => {
                self.held.fetch_sub(open.bytes as u64, Ordering::Relaxed);
                match fence_version {
                    Some(v) => {
                        t.version = v;
                        t.stage = Stage::Ready;
                        t.last_end = Some((open.token, true));
                    }
                    // The answer did not carry the row's new version: the next
                    // call reads the row again.
                    None => forget = true,
                }
                NONE
            }
            Ok(Committed::Fenced) => {
                // Nothing was written: a newer producer or leader holds the id.
                self.held.fetch_sub(open.bytes as u64, Ordering::Relaxed);
                t.stage = Stage::Ready;
                PRODUCER_FENCED
            }
            Ok(Committed::Refused(why)) => {
                tracing::warn!(target: "kafka", %tid, %why, "a transaction commit was refused; it is aborted");
                self.held.fetch_sub(open.bytes as u64, Ordering::Relaxed);
                t.stage = Stage::Ready;
                INVALID_TXN_STATE
            }
            Err(e) => {
                // Unknown outcome: held, and the retry finds out.
                tracing::warn!(target: "kafka", %tid, error = %e, "a transaction commit has no answer; held for the retry");
                CONCURRENT_TRANSACTIONS
            }
        };
        if forget {
            inner.remove(tid);
        }
        code
    }

    /// Abort what has outlived its timeout; the producer is fenced, as Kafka's
    /// coordinator fences it, by the epoch its next InitProducerId bumps.
    pub async fn expire(&self) {
        let now = Instant::now();
        let mut inner = self.inner.lock().await;
        let mut timed_out: Vec<String> = Vec::new();
        for (tid, t) in inner.iter() {
            if let Stage::Open(o) = &t.stage {
                if !o.committing && o.started.is_some_and(|s| now.duration_since(s) > t.timeout) {
                    timed_out.push(tid.clone());
                }
            }
        }
        for tid in timed_out {
            let Some(t) = inner.get_mut(&tid) else {
                continue;
            };
            let Stage::Open(o) = &t.stage else { continue };
            let (token, bytes) = (o.token, o.bytes);
            // The epoch moves on: the timed-out producer is fenced.
            let value = json!({"pid": t.pid, "epoch": t.epoch.saturating_add(1), "state": "aborted", "token": token});
            let bearer = t.bearer.clone();
            if let Ok(Some(v)) = self
                .write_row(bearer.as_deref(), &tid, t.version, value)
                .await
            {
                t.version = v;
                t.epoch = t.epoch.saturating_add(1);
            }
            self.held.fetch_sub(bytes as u64, Ordering::Relaxed);
            t.stage = Stage::Ready;
            t.last_end = Some((token, false));
            tracing::info!(target: "kafka", %tid, "a transaction passed its timeout and was aborted");
        }
    }
}

/// What a transaction commit did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Committed {
    Done {
        fence_version: Option<i64>,
    },
    /// The fence's precondition was lost: nothing was written.
    Fenced,
    /// Refused for another reason (a body too large, a bad record).
    Refused(String),
}
