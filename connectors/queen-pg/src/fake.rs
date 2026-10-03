//! `FakeQueen`: an in-memory broker double with the routes' exact semantics,
//! plus fault injection. OWNER: agent C. See PLAN §7.
//!
//! It is the ORACLE the source and sink tests are judged by, so it copies the
//! broker's rules rather than inventing friendlier ones:
//!
//! * **KV** (server/src/rsm/planner/kv.rs): versions come from ONE global
//!   counter (unique, never re-issued); a row is live while its expiry is
//!   strictly after the fake clock; `expect: 0` means "must not exist" and
//!   wins against an expired row; `expect: n` is a pure update; one write per
//!   key per call; the writes of a call are decided in the broker's apply
//!   order (`(phase, key)`, multi-key reads last); a lost `required`
//!   precondition writes NOTHING of the call.
//! * **Transactions** (planner/txn.rs + the facade's `txn_impl`):
//!   all-or-nothing; a push whose `transactionId` is already stored in its
//!   (queue, partition) rolls the whole bundle back (`duplicate`); the same
//!   (queue, partition, transactionId) twice INSIDE one bundle collapses into
//!   the first (`duplicate: true` on the follower's result); a lost required
//!   KV precondition rolls back with `kv_precondition` and a `failedIndex` in
//!   the flat space (push items first, then the rider). Dedup is forever here
//!   (the broker's window is time-bounded; nothing in the connectors may rely
//!   on it).
//! * **Pop / ack / extend** (rsm/consume): one lease id per pop call, shared
//!   by the partitions that call claimed (the broker's "worker"); a lease
//!   covers the offsets the call returned; an expired lease makes the
//!   partition claimable again from its cursor (redelivery, attempt + 1);
//!   acks are cumulative per partition; a repeated ack of a settled message
//!   under a lease that an ack released answers success (the broker's
//!   idempotent re-ack); `extend_lease` renews every live lease of that id.
//!
//! The clock is FAKE: leases and KV TTLs move only with [`FakeQueen::advance`]
//! (and [`FakeQueen::expire_leases`]), so a test decides exactly when a lease
//! runs out. Only an empty pop sleeps in real time (min(wait, 20 ms)), so a
//! polling loop does not spin.
//!
//! Answers are rendered as the routes' JSON and parsed with the same
//! functions the broker's answers go through ([`parse_txn_answer`],
//! [`parse_kv_answer`]), so a test exercises the wire shapes too.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use serde::Deserialize;
use serde_json::value::RawValue;
use serde_json::{json, Map, Value};

use crate::queen::{
    parse_kv_answer, parse_txn_answer, AckAnswer, AckItemResult, AckRequest, AckStatus, BoxFuture,
    KvAnswer, KvOp, PopAnswer, PopRequest, Popped, QueenApi, QueenError, TxnAnswer, KV_NAMESPACE,
    MAX_KV_PREFIX_LIMIT,
};

/// KV ops in one transaction rider (the broker's `MAX_OPS_WIRE`).
const MAX_RIDER_OPS: usize = 64;
/// The broker's `transactionId` ceiling (`MAX_TXN_BYTES`).
const MAX_TXN_ID_BYTES: usize = u16::MAX as usize;
/// The first partition id handed out: far from 0 so a test that confuses a
/// partition id with an offset fails loudly.
const FIRST_PARTITION_ID: i64 = 7_001;
/// Partitions one pop may claim when the request leaves it to the broker.
const DEFAULT_MAX_PARTITIONS: u32 = 4;
/// The longest an empty pop sleeps (real time).
const EMPTY_POP_SLEEP_MS: u64 = 20;

/// One stored message, as a test inspects it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FakeMessage {
    pub partition: String,
    pub partition_id: i64,
    pub offset: i64,
    pub transaction_id: String,
    /// The payload exactly as pushed: raw JSON text.
    pub payload: String,
    /// ISO-8601 UTC, microseconds (the broker's rendering).
    pub created_at: String,
}

/// The five calls, for [`FakeQueen::inject`] and [`FakeQueen::calls`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FakeCall {
    Transaction,
    Kv,
    Pop,
    Ack,
    ExtendLease,
}

/// A one-shot fault for the next call of a kind.
#[derive(Debug, Clone)]
pub enum Fault {
    /// Refuse before applying anything: the call answers this error.
    Fail(QueenError),
    /// Apply the call fully, then answer `503 {"error":"outcome_unknown"}`
    /// (the broker committed, the answer was lost on the way).
    LoseAnswer,
}

/// The double. `Send + Sync`; hand it out as `Arc<dyn QueenApi>`.
pub struct FakeQueen {
    st: Mutex<State>,
}

impl Default for FakeQueen {
    fn default() -> FakeQueen {
        FakeQueen {
            st: Mutex::new(State::new()),
        }
    }
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

struct State {
    now_us: i64,
    next_version: i64,
    next_pid: i64,
    next_id: u64,
    kv: BTreeMap<String, KvRowF>,
    queues: BTreeMap<String, QueueF>,
    /// partition id → (queue, partition name).
    pids: HashMap<i64, (String, String)>,
    faults: HashMap<FakeCall, VecDeque<Fault>>,
    calls: HashMap<FakeCall, u64>,
}

#[derive(Debug, Clone)]
struct KvRowF {
    value: Value,
    version: i64,
    expires_us: Option<i64>,
    updated_us: i64,
}

impl KvRowF {
    fn live(&self, now_us: i64) -> bool {
        self.expires_us.is_none_or(|e| e > now_us)
    }
}

#[derive(Default)]
struct QueueF {
    parts: BTreeMap<String, PartF>,
    dlq: Vec<FakeMessage>,
    groups: BTreeMap<String, GroupF>,
}

struct PartF {
    id: i64,
    msgs: Vec<StoredF>,
    by_txn: HashMap<String, i64>,
}

struct StoredF {
    txn: String,
    payload: String,
    created_us: i64,
}

#[derive(Default)]
struct GroupF {
    /// One row per partition the group has a position on.
    rows: BTreeMap<i64, CursorF>,
}

struct CursorF {
    /// The last settled offset; -1 before the first.
    cursor: i64,
    lease: Option<LeaseF>,
    /// Lease ids an ack (or a `failed`) released on this partition: a repeat
    /// of a settled ack under one of them answers success.
    released: Vec<String>,
    /// Deliveries per offset.
    attempts: BTreeMap<i64, u32>,
}

impl CursorF {
    fn at(cursor: i64) -> CursorF {
        CursorF {
            cursor,
            lease: None,
            released: Vec::new(),
            attempts: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
struct LeaseF {
    id: String,
    expires_us: i64,
    /// The last offset the claiming pop returned.
    end: i64,
}

impl LeaseF {
    fn live(&self, now_us: i64) -> bool {
        self.expires_us > now_us
    }
}

fn wall_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

impl State {
    fn new() -> State {
        State {
            now_us: wall_us(),
            next_version: 1,
            next_pid: FIRST_PARTITION_ID,
            next_id: 1,
            kv: BTreeMap::new(),
            queues: BTreeMap::new(),
            pids: HashMap::new(),
            faults: HashMap::new(),
            calls: HashMap::new(),
        }
    }

    /// Count the call and take its fault, if one is queued.
    fn begin(&mut self, call: FakeCall) -> Option<Fault> {
        *self.calls.entry(call).or_default() += 1;
        self.faults.get_mut(&call).and_then(VecDeque::pop_front)
    }

    fn mint(&mut self) -> u64 {
        let n = self.next_id;
        self.next_id += 1;
        n
    }

    /// A uuid-shaped id, unique in this fake.
    fn mint_uuid(&mut self) -> String {
        format!("00000000-0000-7000-8000-{:012x}", self.mint())
    }

    /// The partition `(queue, partition)`, created (with a new global id) on
    /// first use.
    fn part_mut(&mut self, queue: &str, partition: &str) -> &mut PartF {
        let q = self.queues.entry(queue.to_string()).or_default();
        if !q.parts.contains_key(partition) {
            let id = self.next_pid;
            self.next_pid += 1;
            self.pids
                .insert(id, (queue.to_string(), partition.to_string()));
            q.parts.insert(
                partition.to_string(),
                PartF {
                    id,
                    msgs: Vec::new(),
                    by_txn: HashMap::new(),
                },
            );
        }
        self.queues
            .get_mut(queue)
            .and_then(|q| q.parts.get_mut(partition))
            .expect("just created")
    }

    /// Store one message; the caller has checked the txn id is new.
    fn append(&mut self, queue: &str, partition: &str, txn: &str, payload: &str) -> i64 {
        let now = self.now_us;
        let p = self.part_mut(queue, partition);
        let offset = p.msgs.len() as i64;
        p.msgs.push(StoredF {
            txn: txn.to_string(),
            payload: payload.to_string(),
            created_us: now,
        });
        p.by_txn.insert(txn.to_string(), offset);
        offset
    }

    fn stored_offset(&self, queue: &str, partition: &str, txn: &str) -> Option<i64> {
        self.queues
            .get(queue)?
            .parts
            .get(partition)?
            .by_txn
            .get(txn)
            .copied()
    }
}

fn status(code: u16, body: Value) -> QueenError {
    QueenError::Status {
        code,
        body: body.to_string(),
        retry_after_ms: None,
    }
}

fn bad_request(reason: &str, detail: impl Into<String>) -> QueenError {
    status(
        400,
        json!({"success": false, "reason": reason, "error": detail.into()}),
    )
}

/// The answer of a call whose outcome the client cannot know.
fn lost_answer() -> QueenError {
    QueenError::Status {
        code: 503,
        body: "{\"error\":\"outcome_unknown\"}".to_string(),
        retry_after_ms: None,
    }
}

// ---------------------------------------------------------------------------
// Time rendering (the broker's two timestamp shapes)
// ---------------------------------------------------------------------------

/// Howard Hinnant's `civil_from_days`.
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn split(us: i64) -> (i64, u32, u32, i64, i64, i64, i64) {
    const DAY: i64 = 86_400_000_000;
    let (y, m, d) = civil(us.div_euclid(DAY));
    let rem = us.rem_euclid(DAY);
    let secs = rem / 1_000_000;
    (
        y,
        m,
        d,
        secs / 3600,
        (secs / 60) % 60,
        secs % 60,
        rem % 1_000_000,
    )
}

/// `2026-10-02T10:00:00.123456Z` (message `createdAt`).
fn iso_us(us: i64) -> String {
    let (y, mo, d, h, mi, s, f) = split(us);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}.{f:06}Z")
}

/// `2026-10-02T10:00:00.123+00:00` (KV `expiresAt` / `updatedAt`).
fn ts_jsonb(us: i64) -> String {
    let (y, mo, d, h, mi, s, f) = split(us);
    let mut out = format!("{y:04}-{mo:02}-{d:02}T{h:02}:{mi:02}:{s:02}");
    if f != 0 {
        let frac = format!("{f:06}");
        out.push('.');
        out.push_str(frac.trim_end_matches('0'));
    }
    out.push_str("+00:00");
    out
}

// ---------------------------------------------------------------------------
// KV
// ---------------------------------------------------------------------------

/// What a KV call (or a rider) did: the per-op answers in input order, or the
/// one lost required precondition (then nothing was written).
enum KvOutcome {
    Applied(Vec<Value>),
    Failed {
        index: usize,
        reason: &'static str,
        version: i64,
        value: Value,
    },
}

fn op_name(op: &KvOp) -> &'static str {
    match op {
        KvOp::Get { .. } => "get",
        KvOp::GetMany { .. } => "getMany",
        KvOp::GetPrefix { .. } => "getPrefix",
        KvOp::Put { .. } => "put",
        KvOp::PutIfAbsent { .. } => "putIfAbsent",
        KvOp::Delete { .. } => "delete",
    }
}

fn written_key(op: &KvOp) -> Option<&str> {
    match op {
        KvOp::Put { key, .. } | KvOp::PutIfAbsent { key, .. } | KvOp::Delete { key, .. } => {
            Some(key)
        }
        _ => None,
    }
}

/// The broker's apply order: `(phase, key)`, multi-key reads (phase 1) last
/// so they see the call's writes, the input ordinal breaking ties.
fn apply_order(ops: &[KvOp]) -> Vec<usize> {
    let sort_key = |op: &KvOp| -> (u8, String) {
        match op {
            KvOp::GetMany { .. } => (1, String::new()),
            KvOp::GetPrefix { prefix, .. } => (1, prefix.clone()),
            KvOp::Get { key } => (0, key.clone()),
            other => (0, written_key(other).unwrap_or("").to_string()),
        }
    };
    let mut order: Vec<usize> = (0..ops.len()).collect();
    order.sort_by(|&a, &b| sort_key(&ops[a]).cmp(&sort_key(&ops[b])).then(a.cmp(&b)));
    order
}

fn row_json(key: &str, r: &KvRowF) -> Value {
    json!({
        "key": key,
        "value": r.value,
        "version": r.version,
        "expiresAt": r.expires_us.map(ts_jsonb),
        "updatedAt": ts_jsonb(r.updated_us),
    })
}

fn write_json(
    index: usize,
    op: &KvOp,
    applied: bool,
    reason: Option<&str>,
    value: Value,
    version: i64,
) -> Value {
    let mut m = Map::new();
    m.insert("index".into(), Value::from(index));
    m.insert("op".into(), Value::from(op_name(op)));
    m.insert("applied".into(), Value::Bool(applied));
    if let (false, Some(r)) = (applied, reason) {
        m.insert("reason".into(), Value::from(r));
    }
    m.insert("key".into(), Value::from(written_key(op).unwrap_or("")));
    m.insert("value".into(), value);
    m.insert("version".into(), Value::from(version));
    Value::Object(m)
}

/// Apply `ops` to `kv` all-or-nothing (see [`KvOutcome`]). `Err` is a call
/// the route refuses outright (400).
fn apply_kv(
    kv: &mut BTreeMap<String, KvRowF>,
    ops: &[KvOp],
    now: i64,
    next_version: &mut i64,
) -> Result<KvOutcome, QueenError> {
    let mut seen = std::collections::HashSet::new();
    for op in ops {
        if let Some(k) = written_key(op) {
            if !seen.insert(k) {
                return Err(bad_request(
                    "kv_duplicate_key_in_call",
                    "a key may be written at most once per call",
                ));
            }
        }
        match op {
            KvOp::Put { key, .. }
            | KvOp::PutIfAbsent { key, .. }
            | KvOp::Delete { key, .. }
            | KvOp::Get { key }
                if key.is_empty() =>
            {
                return Err(bad_request("kv_bad_request", "key is required"));
            }
            KvOp::Put {
                ttl_seconds: Some(0),
                ..
            }
            | KvOp::PutIfAbsent {
                ttl_seconds: Some(0),
                ..
            } => {
                return Err(bad_request(
                    "kv_bad_ttl",
                    "ttlSeconds must be a positive integer",
                ));
            }
            _ => {}
        }
    }

    let mut work = kv.clone();
    let mut out: Vec<Value> = vec![Value::Null; ops.len()];
    for i in apply_order(ops) {
        let op = &ops[i];
        match op {
            KvOp::Get { key } => {
                out[i] = match work.get(key).filter(|r| r.live(now)) {
                    Some(r) => json!({
                        "index": i, "op": "get", "found": true, "key": key,
                        "value": r.value, "version": r.version,
                        "expiresAt": r.expires_us.map(ts_jsonb),
                        "updatedAt": ts_jsonb(r.updated_us),
                    }),
                    None => json!({"index": i, "op": "get", "found": false, "key": key}),
                };
            }
            KvOp::GetMany { keys } => {
                let mut hits: Vec<(&String, &KvRowF)> = keys
                    .iter()
                    .filter_map(|k| work.get(k).filter(|r| r.live(now)).map(|r| (k, r)))
                    .collect();
                hits.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
                let missing: Vec<&String> = keys
                    .iter()
                    .filter(|k| work.get(*k).is_none_or(|r| !r.live(now)))
                    .collect();
                out[i] = json!({
                    "index": i, "op": "getMany",
                    "rows": hits.iter().map(|(k, r)| row_json(k, r)).collect::<Vec<_>>(),
                    "missing": missing,
                    "truncated": false,
                });
            }
            KvOp::GetPrefix {
                prefix,
                limit,
                after,
            } => {
                let limit = (*limit).clamp(1, MAX_KV_PREFIX_LIMIT) as usize;
                let mut page: Vec<(&String, &KvRowF)> = Vec::new();
                for (k, r) in work.iter() {
                    if !k.starts_with(prefix.as_str()) || !r.live(now) {
                        continue;
                    }
                    if after
                        .as_deref()
                        .is_some_and(|a| k.as_bytes() <= a.as_bytes())
                    {
                        continue;
                    }
                    page.push((k, r));
                    if page.len() > limit {
                        break;
                    }
                }
                let truncated = page.len() > limit;
                page.truncate(limit);
                let next_after = if truncated {
                    page.last().map(|(k, _)| (*k).clone())
                } else {
                    None
                };
                out[i] = json!({
                    "index": i, "op": "getPrefix",
                    "rows": page.iter().map(|(k, r)| row_json(k, r)).collect::<Vec<_>>(),
                    "truncated": truncated,
                    "nextAfter": next_after,
                });
            }
            KvOp::Put {
                key,
                value,
                ttl_seconds,
                expect,
                required,
            } => {
                match put(
                    &mut work,
                    key,
                    value,
                    *ttl_seconds,
                    *expect,
                    now,
                    next_version,
                ) {
                    Ok(version) => {
                        out[i] = write_json(i, op, true, None, value.clone(), version);
                    }
                    Err((reason, version, winner)) => {
                        if *required {
                            return Ok(KvOutcome::Failed {
                                index: i,
                                reason,
                                version,
                                value: winner,
                            });
                        }
                        out[i] = write_json(i, op, false, Some(reason), winner, version);
                    }
                }
            }
            KvOp::PutIfAbsent {
                key,
                value,
                ttl_seconds,
                required,
            } => match put(
                &mut work,
                key,
                value,
                *ttl_seconds,
                Some(0),
                now,
                next_version,
            ) {
                Ok(version) => {
                    out[i] = write_json(i, op, true, None, value.clone(), version);
                }
                Err((reason, version, winner)) => {
                    if *required {
                        return Ok(KvOutcome::Failed {
                            index: i,
                            reason,
                            version,
                            value: winner,
                        });
                    }
                    out[i] = write_json(i, op, false, Some(reason), winner, version);
                }
            },
            KvOp::Delete {
                key,
                expect,
                required,
            } => {
                let live = work.get(key).filter(|r| r.live(now)).cloned();
                let verdict: Result<(Value, i64), &'static str> = match expect {
                    None => match &live {
                        Some(r) => Ok((r.value.clone(), r.version)),
                        None => {
                            // A plain delete prunes an expired row, but it was
                            // never there logically: not applied.
                            work.remove(key);
                            Err("absent")
                        }
                    },
                    // "Must not exist": idempotent success when it does not.
                    Some(0) => match &live {
                        None => Ok((Value::Null, 0)),
                        Some(_) => Err("exists"),
                    },
                    Some(n) => match &live {
                        Some(r) if r.version == *n => Ok((r.value.clone(), r.version)),
                        Some(_) => Err("version"),
                        None => Err("absent"),
                    },
                };
                match verdict {
                    Ok((value, version)) => {
                        if live.is_some() {
                            work.remove(key);
                        }
                        out[i] = write_json(i, op, true, None, value, version);
                    }
                    Err(reason) => {
                        let (winner, version) = live
                            .map(|r| (r.value, r.version))
                            .unwrap_or((Value::Null, 0));
                        if *required {
                            return Ok(KvOutcome::Failed {
                                index: i,
                                reason,
                                version,
                                value: winner,
                            });
                        }
                        out[i] = write_json(i, op, false, Some(reason), winner, version);
                    }
                }
            }
        }
    }
    *kv = work;
    Ok(KvOutcome::Applied(out))
}

/// One put under `expect`: `Ok(new version)`, or `Err((reason, the winner's
/// version, the winner's value))` — what a reader would see: nothing and 0
/// for an expired row.
fn put(
    work: &mut BTreeMap<String, KvRowF>,
    key: &str,
    value: &Value,
    ttl_seconds: Option<u64>,
    expect: Option<i64>,
    now: i64,
    next_version: &mut i64,
) -> Result<i64, (&'static str, i64, Value)> {
    let live = work.get(key).filter(|r| r.live(now)).cloned();
    let decided: Result<(), &'static str> = match expect {
        None => Ok(()),
        Some(0) => match &live {
            Some(_) => Err("exists"),
            None => Ok(()),
        },
        Some(n) => match &live {
            Some(r) if r.version == n => Ok(()),
            Some(_) => Err("version"),
            None => Err("absent"),
        },
    };
    match decided {
        Ok(()) => {
            let version = *next_version;
            *next_version += 1;
            work.insert(
                key.to_string(),
                KvRowF {
                    value: value.clone(),
                    version,
                    expires_us: ttl_seconds.map(|s| now.saturating_add(s as i64 * 1_000_000)),
                    updated_us: now,
                },
            );
            Ok(version)
        }
        Err(reason) => {
            let (winner, version) = live
                .map(|r| (r.value, r.version))
                .unwrap_or((Value::Null, 0));
            Err((reason, version, winner))
        }
    }
}

/// A rider element (the JSON [`KvOp::to_json`] writes) back into an op. The
/// fake knows one namespace, the connectors' own.
fn rider_op(i: usize, v: &Value) -> Result<KvOp, QueenError> {
    let bad = |m: String| bad_request("kv_bad_request", format!("op at index {i}: {m}"));
    let o = v.as_object().ok_or_else(|| bad("not an object".into()))?;
    if o.get("ns").and_then(Value::as_str) != Some(KV_NAMESPACE) {
        return Err(bad(format!("the fake knows only namespace {KV_NAMESPACE}")));
    }
    let key = || -> Result<String, QueenError> {
        o.get("key")
            .and_then(Value::as_str)
            .filter(|k| !k.is_empty())
            .map(str::to_string)
            .ok_or_else(|| bad("key is required".into()))
    };
    let expiry = || -> Result<Option<u64>, QueenError> {
        let forever = o.get("forever").and_then(Value::as_bool).unwrap_or(false);
        match (o.get("ttlSeconds"), forever) {
            (None, true) => Ok(None),
            (Some(t), false) => t
                .as_u64()
                .filter(|s| *s > 0)
                .map(Some)
                .ok_or_else(|| bad("ttlSeconds must be a positive integer".into())),
            _ => Err(bad("exactly one of ttlSeconds and forever:true".into())),
        }
    };
    let expect = o.get("expect").and_then(Value::as_i64);
    let required = o.get("required").and_then(Value::as_bool).unwrap_or(false);
    let value = || -> Result<Value, QueenError> {
        o.get("value")
            .cloned()
            .ok_or_else(|| bad("value is required".into()))
    };
    match o.get("op").and_then(Value::as_str) {
        Some("get") => Ok(KvOp::Get { key: key()? }),
        Some("getMany") => Ok(KvOp::GetMany {
            keys: o
                .get("keys")
                .and_then(Value::as_array)
                .map(|a| {
                    a.iter()
                        .filter_map(Value::as_str)
                        .map(str::to_string)
                        .collect()
                })
                .ok_or_else(|| bad("keys must be an array".into()))?,
        }),
        Some("getPrefix") => Err(bad_request(
            "kv_get_prefix_not_allowed_in_transaction",
            format!("op at index {i}: getPrefix is only available on POST /api/v1/kv"),
        )),
        Some("put") => Ok(KvOp::Put {
            key: key()?,
            value: value()?,
            ttl_seconds: expiry()?,
            expect,
            required,
        }),
        Some("putIfAbsent") => Ok(KvOp::PutIfAbsent {
            key: key()?,
            value: value()?,
            ttl_seconds: expiry()?,
            required,
        }),
        Some("delete") => Ok(KvOp::Delete {
            key: key()?,
            expect,
            required,
        }),
        other => Err(bad_request(
            "kv_unknown_op",
            format!("op at index {i}: unknown operation {other:?}"),
        )),
    }
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxnBodyIn<'a> {
    #[serde(borrow, default)]
    operations: Option<Vec<TxnOpIn<'a>>>,
    #[serde(default)]
    kv: Option<Vec<Value>>,
    #[serde(default)]
    timers: Option<Vec<Value>>,
    #[serde(default)]
    positions: Option<Vec<Value>>,
    #[serde(default)]
    required_leases: Option<Vec<String>>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct TxnOpIn<'a> {
    #[serde(default, rename = "type")]
    ty: String,
    #[serde(borrow, default)]
    items: Option<Vec<PushItemIn<'a>>>,
    #[serde(default)]
    queue: Option<String>,
    #[serde(default)]
    partition: Option<String>,
    #[serde(borrow, default)]
    payload: Option<&'a RawValue>,
    #[serde(default)]
    transaction_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct PushItemIn<'a> {
    #[serde(default)]
    queue: String,
    #[serde(default)]
    partition: Option<String>,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(default)]
    transaction_id: Option<String>,
}

struct FlatPush {
    queue: String,
    partition: String,
    txn: String,
    payload: String,
    message_id: String,
    /// The earlier item of the same (queue, partition, txn) in this bundle.
    follower_of: Option<usize>,
}

impl State {
    /// The transaction route: the answer body (HTTP 200) or the refusal.
    fn transaction(&mut self, body: &str) -> Result<String, QueenError> {
        let parsed: TxnBodyIn<'_> = serde_json::from_str(body)
            .map_err(|e| bad_request("bad_request", format!("bad body: {e}")))?;
        let txn_id = self.mint_uuid();
        let fail = |reason: &str, error: String| {
            json!({"transactionId": txn_id, "success": false, "reason": reason,
                   "error": error, "results": []})
            .to_string()
        };
        for (field, v) in [
            (
                "timers",
                parsed.timers.as_ref().is_some_and(|t| !t.is_empty()),
            ),
            (
                "positions",
                parsed.positions.as_ref().is_some_and(|t| !t.is_empty()),
            ),
            (
                "requiredLeases",
                parsed
                    .required_leases
                    .as_ref()
                    .is_some_and(|t| !t.is_empty()),
            ),
        ] {
            if v {
                return Err(bad_request(
                    "bad_request",
                    format!("the fake does not implement `{field}` (the connectors never send it)"),
                ));
            }
        }
        let rider: Vec<Value> = parsed.kv.unwrap_or_default();
        if rider.len() > MAX_RIDER_OPS {
            return Err(bad_request(
                "kv_too_many_ops",
                format!(
                    "{} ops in one call, the ceiling is {MAX_RIDER_OPS}",
                    rider.len()
                ),
            ));
        }
        let kv_ops = rider
            .iter()
            .enumerate()
            .map(|(i, v)| rider_op(i, v))
            .collect::<Result<Vec<KvOp>, QueenError>>()?;
        let ops = parsed.operations.unwrap_or_default();
        if ops.is_empty() && kv_ops.is_empty() {
            return Ok(fail(
                "bad_request",
                "transaction requires an operations array (or a top-level kv array)".into(),
            ));
        }

        // 1. Flatten the pushes (items, or one item inline).
        let mut flat: Vec<FlatPush> = Vec::new();
        let mut leaders: HashMap<(String, String, String), usize> = HashMap::new();
        for op in &ops {
            let mut items: Vec<(String, Option<String>, &RawValue, Option<String>)> = Vec::new();
            match op.ty.as_str() {
                "push" => match &op.items {
                    Some(list) => {
                        for it in list {
                            items.push((
                                it.queue.clone(),
                                it.partition.clone(),
                                it.payload,
                                it.transaction_id.clone(),
                            ));
                        }
                    }
                    None => {
                        let (Some(q), Some(p)) = (op.queue.as_ref(), op.payload) else {
                            return Ok(fail(
                                "bad_request",
                                "a push operation needs queue and payload".into(),
                            ));
                        };
                        items.push((q.clone(), op.partition.clone(), p, op.transaction_id.clone()));
                    }
                },
                "ack" => {
                    return Err(bad_request(
                        "bad_request",
                        "the fake does not implement transactional acks (the connectors never send them)",
                    ))
                }
                other => {
                    return Err(bad_request(
                        "bad_request",
                        format!("transaction supports only push and ack operations, got `{other}`"),
                    ))
                }
            }
            for (queue, partition, payload, txn) in items {
                if queue.is_empty() {
                    return Ok(fail("bad_request", "queue is required".into()));
                }
                if txn.as_ref().is_some_and(|t| t.len() > MAX_TXN_ID_BYTES) {
                    return Err(bad_request(
                        "bad_request",
                        format!("transactionId exceeds the {MAX_TXN_ID_BYTES}-byte limit"),
                    ));
                }
                let message_id = self.mint_uuid();
                let txn = txn.unwrap_or_else(|| message_id.clone());
                let partition = partition
                    .filter(|p| !p.is_empty())
                    .unwrap_or_else(|| "Default".to_string());
                let key = (queue.clone(), partition.clone(), txn.clone());
                let follower_of = leaders.get(&key).copied();
                if follower_of.is_none() {
                    leaders.insert(key, flat.len());
                }
                flat.push(FlatPush {
                    queue,
                    partition,
                    txn,
                    payload: payload.get().to_string(),
                    message_id,
                    follower_of,
                });
            }
        }

        // 2. A push already stored rolls the WHOLE bundle back.
        for p in flat.iter().filter(|p| p.follower_of.is_none()) {
            if let Some(offset) = self.stored_offset(&p.queue, &p.partition, &p.txn) {
                return Ok(fail(
                    "duplicate",
                    format!(
                        "QDUP a pushed message to {}/{} is a duplicate (original offset \
                         {offset}); the transaction rolled back",
                        p.queue, p.partition
                    ),
                ));
            }
        }

        // 3. The KV rider, after the messages; a lost required precondition
        //    rolls back everything (apply_kv wrote nothing then).
        let kv_base = flat.len();
        let mut kv_results = Vec::new();
        if !kv_ops.is_empty() {
            let now = self.now_us;
            match apply_kv(&mut self.kv, &kv_ops, now, &mut self.next_version)? {
                KvOutcome::Applied(r) => kv_results = r,
                KvOutcome::Failed {
                    index,
                    reason,
                    version,
                    value,
                } => {
                    return Ok(json!({
                        "transactionId": txn_id,
                        "success": false,
                        "reason": "kv_precondition",
                        "error": "QKV a required KV precondition failed; the transaction rolled back",
                        "results": [],
                        "ok": false,
                        "failedIndex": kv_base + index,
                        "kvReason": reason,
                        "version": version,
                        "value": value,
                    })
                    .to_string());
                }
            }
        }

        // 4. Commit the messages, render one result per flat ordinal.
        let mut results: Vec<Value> = Vec::with_capacity(flat.len() + kv_results.len());
        for (i, p) in flat.iter().enumerate() {
            match p.follower_of {
                None => {
                    self.append(&p.queue, &p.partition, &p.txn, &p.payload);
                    results.push(json!({
                        "index": i, "type": "push", "success": true,
                        "transactionId": p.txn, "messageId": p.message_id,
                        "queueName": p.queue,
                    }));
                }
                Some(leader) => results.push(json!({
                    "index": i, "type": "push", "success": true,
                    "transactionId": p.txn, "messageId": flat[leader].message_id,
                    "queueName": p.queue, "duplicate": true,
                })),
            }
        }
        for (i, r) in kv_results.into_iter().enumerate() {
            let mut obj = match r {
                Value::Object(m) => m,
                other => {
                    let mut m = Map::new();
                    m.insert("result".into(), other);
                    m
                }
            };
            obj.insert("opIndex".into(), Value::from(i));
            obj.insert("index".into(), Value::from(kv_base + i));
            obj.insert("type".into(), Value::from("kv"));
            results.push(Value::Object(obj));
        }
        Ok(json!({"transactionId": txn_id, "success": true, "results": results}).to_string())
    }

    /// The KV route: the answer body (HTTP 200) or the refusal.
    fn kv_call(&mut self, ops: &[KvOp]) -> Result<String, QueenError> {
        if ops.is_empty() {
            return Ok(json!({"results": []}).to_string());
        }
        let now = self.now_us;
        Ok(
            match apply_kv(&mut self.kv, ops, now, &mut self.next_version)? {
                KvOutcome::Applied(results) => json!({ "results": results }).to_string(),
                KvOutcome::Failed {
                    index,
                    reason,
                    version,
                    value,
                } => json!({
                    "ok": false,
                    "reason": "kv_precondition",
                    "failedIndex": index,
                    "kvReason": reason,
                    "version": version,
                    "value": value,
                })
                .to_string(),
            },
        )
    }
}

// ---------------------------------------------------------------------------
// Pop / ack / extend
// ---------------------------------------------------------------------------

impl State {
    fn pop(&mut self, req: &PopRequest) -> Result<Vec<Popped>, QueenError> {
        let mode = match req.subscription_mode.as_str() {
            "" | "all" => "all",
            "new" => "new",
            other => {
                return Err(bad_request(
                    "bad_request",
                    format!("subscriptionMode must be all or new, got {other:?}"),
                ))
            }
        };
        if req.queue.is_empty() || req.group.is_empty() {
            return Err(bad_request(
                "bad_request",
                "queue and consumer group are required",
            ));
        }
        let now = self.now_us;
        let lease_id = format!("fake-lease-{}", self.mint());
        let q = self.queues.entry(req.queue.clone()).or_default();
        // First registration: `new` starts every existing partition at its
        // end; partitions created later start at the beginning either way.
        if !q.groups.contains_key(&req.group) {
            let mut g = GroupF::default();
            if mode == "new" {
                for p in q.parts.values() {
                    g.rows.insert(p.id, CursorF::at(p.msgs.len() as i64 - 1));
                }
            }
            q.groups.insert(req.group.clone(), g);
        }
        let mut parts: Vec<(&String, &PartF)> = q.parts.iter().collect();
        parts.sort_by_key(|(_, p)| p.id);
        let group = q.groups.get_mut(&req.group).expect("registered above");

        let max_parts = req.max_partitions.unwrap_or(DEFAULT_MAX_PARTITIONS).max(1) as usize;
        let mut budget = req.batch.max(1) as i64;
        let mut claimed = 0usize;
        let mut out = Vec::new();
        for (name, p) in parts {
            if claimed == max_parts || budget == 0 {
                break;
            }
            let row = group.rows.entry(p.id).or_insert_with(|| CursorF::at(-1));
            match &row.lease {
                Some(l) if l.live(now) => continue,
                // Expired: the partition is claimable again from its cursor.
                Some(_) => row.lease = None,
                None => {}
            }
            let from = row.cursor + 1;
            let last = p.msgs.len() as i64 - 1;
            if from > last {
                continue;
            }
            let to = last.min(from + budget - 1);
            row.lease = Some(LeaseF {
                id: lease_id.clone(),
                expires_us: now.saturating_add(i64::from(req.lease_seconds) * 1_000_000),
                end: to,
            });
            for off in from..=to {
                let m = &p.msgs[off as usize];
                let attempt = row.attempts.entry(off).or_insert(0);
                *attempt += 1;
                out.push(Popped {
                    transaction_id: m.txn.clone(),
                    partition_id: p.id.to_string(),
                    partition: name.clone(),
                    lease_id: lease_id.clone(),
                    offset: off,
                    created_at: iso_us(m.created_us),
                    data: RawValue::from_string(m.payload.clone())
                        .expect("stored payloads are valid JSON"),
                    delivery_attempt: *attempt,
                });
            }
            budget -= to - from + 1;
            claimed += 1;
        }
        Ok(out)
    }

    fn ack(&mut self, req: &AckRequest) -> Vec<AckItemResult> {
        let now = self.now_us;
        let mut results = Vec::with_capacity(req.items.len());
        // Partitions where this call dead-lettered a message: the broker
        // releases their lease (rsm/consume/ack.rs, forced DLQ), after it has
        // settled the call's other items, so the rest of the batch comes back
        // at once rather than when the lease runs out.
        let mut released_by_dlq: Vec<(String, i64, usize)> = Vec::new();
        for (index, item) in req.items.iter().enumerate() {
            let mut res = AckItemResult {
                index,
                transaction_id: item.transaction_id.clone(),
                ..AckItemResult::default()
            };
            let refuse = |mut res: AckItemResult, why: &str| {
                res.success = false;
                res.error = Some(why.to_string());
                res
            };
            let Some((queue, pname)) = item
                .partition_id
                .parse::<i64>()
                .ok()
                .and_then(|pid| self.pids.get(&pid).cloned())
            else {
                results.push(refuse(res, "unknown partition"));
                continue;
            };
            let pid: i64 = item.partition_id.parse().unwrap_or_default();
            let q = self.queues.get_mut(&queue).expect("indexed queue");
            let part = q.parts.get(&pname).expect("indexed partition");
            let Some(offset) = part.by_txn.get(&item.transaction_id).copied() else {
                results.push(refuse(res, "message not found"));
                continue;
            };
            let Some(row) = q
                .groups
                .get_mut(&req.group)
                .and_then(|g| g.rows.get_mut(&pid))
            else {
                results.push(refuse(res, "invalid or expired lease"));
                continue;
            };
            let live = row
                .lease
                .as_ref()
                .is_some_and(|l| l.id == item.lease_id && l.live(now));
            if !live {
                // The same ack again after it released this lease (its first
                // answer was lost): answered as success, nothing changes.
                if row.released.contains(&item.lease_id) && offset <= row.cursor {
                    res.success = true;
                    res.noop = true;
                    res.dlq = item.status == AckStatus::Dlq;
                    results.push(res);
                } else {
                    results.push(refuse(res, "invalid or expired lease"));
                }
                continue;
            }
            let end = row.lease.as_ref().map(|l| l.end).unwrap_or(-1);
            if offset > end {
                results.push(refuse(res, "message is not in this lease"));
                continue;
            }
            res.success = true;
            if offset <= row.cursor {
                // Settled already under this same live lease.
                res.noop = true;
                results.push(res);
                continue;
            }
            match item.status {
                AckStatus::Ok => row.cursor = row.cursor.max(offset),
                AckStatus::Dlq => {
                    let m = &part.msgs[offset as usize];
                    q.dlq.push(FakeMessage {
                        partition: pname.clone(),
                        partition_id: pid,
                        offset,
                        transaction_id: m.txn.clone(),
                        payload: m.payload.clone(),
                        created_at: iso_us(m.created_us),
                    });
                    row.cursor = row.cursor.max(offset);
                    res.dlq = true;
                    released_by_dlq.push((queue.clone(), pid, results.len()));
                }
                AckStatus::Failed => {
                    // Released without advancing: the partition redelivers
                    // from its cursor on the next pop.
                    if let Some(l) = row.lease.take() {
                        row.released.push(l.id);
                    }
                    res.lease_released = true;
                    results.push(res);
                    continue;
                }
            }
            if row.cursor >= end {
                if let Some(l) = row.lease.take() {
                    row.released.push(l.id);
                }
                res.lease_released = true;
            }
            results.push(res);
        }
        for (queue, pid, at) in released_by_dlq {
            let row = self
                .queues
                .get_mut(&queue)
                .and_then(|q| q.groups.get_mut(&req.group))
                .and_then(|g| g.rows.get_mut(&pid));
            if let Some(row) = row {
                if let Some(l) = row.lease.take() {
                    row.released.push(l.id);
                }
            }
            if let Some(r) = results.get_mut(at) {
                r.lease_released = true;
            }
        }
        results
    }

    fn extend(&mut self, lease_id: &str, seconds: u32) -> Result<(), QueenError> {
        let now = self.now_us;
        let mut found = false;
        for q in self.queues.values_mut() {
            for g in q.groups.values_mut() {
                for row in g.rows.values_mut() {
                    if let Some(l) = row.lease.as_mut() {
                        if l.id == lease_id && l.live(now) {
                            l.expires_us = now.saturating_add(i64::from(seconds) * 1_000_000);
                            found = true;
                        }
                    }
                }
            }
        }
        if found {
            Ok(())
        } else {
            Err(status(
                404,
                json!({"success": false, "error": "lease not found or expired"}),
            ))
        }
    }
}

// ---------------------------------------------------------------------------
// The public surface
// ---------------------------------------------------------------------------

impl FakeQueen {
    pub fn new() -> Arc<FakeQueen> {
        Arc::new(FakeQueen::default())
    }

    fn lock(&self) -> MutexGuard<'_, State> {
        self.st.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Every message of `queue`, every partition, ordered by
    /// (partition id, offset).
    pub fn messages(&self, queue: &str) -> Vec<FakeMessage> {
        let st = self.lock();
        let Some(q) = st.queues.get(queue) else {
            return Vec::new();
        };
        let mut parts: Vec<(&String, &PartF)> = q.parts.iter().collect();
        parts.sort_by_key(|(_, p)| p.id);
        parts
            .into_iter()
            .flat_map(|(name, p)| {
                p.msgs.iter().enumerate().map(move |(off, m)| FakeMessage {
                    partition: name.clone(),
                    partition_id: p.id,
                    offset: off as i64,
                    transaction_id: m.txn.clone(),
                    payload: m.payload.clone(),
                    created_at: iso_us(m.created_us),
                })
            })
            .collect()
    }

    /// The dead letters of `queue`, in filing order.
    pub fn dlq(&self, queue: &str) -> Vec<FakeMessage> {
        self.lock()
            .queues
            .get(queue)
            .map(|q| q.dlq.clone())
            .unwrap_or_default()
    }

    /// The live row `key` of namespace `queen-pg`: (value, version).
    pub fn kv_value(&self, key: &str) -> Option<(Value, i64)> {
        let st = self.lock();
        st.kv
            .get(key)
            .filter(|r| r.live(st.now_us))
            .map(|r| (r.value.clone(), r.version))
    }

    /// A test producer: store one message (a minted transaction id when
    /// `None`) and return its offset. Panics on a payload that is not JSON or
    /// a transaction id already in that partition: a test bug, not a verdict.
    pub fn push_raw(
        &self,
        queue: &str,
        partition: &str,
        payload_json: &str,
        transaction_id: Option<&str>,
    ) -> i64 {
        assert!(
            serde_json::from_str::<&RawValue>(payload_json).is_ok(),
            "push_raw: payload is not JSON: {payload_json}"
        );
        let mut st = self.lock();
        let txn = match transaction_id {
            Some(t) => t.to_string(),
            None => st.mint_uuid(),
        };
        assert!(
            st.stored_offset(queue, partition, &txn).is_none(),
            "push_raw: transactionId {txn} is already in {queue}/{partition}"
        );
        st.append(queue, partition, &txn, payload_json)
    }

    /// The last offset `group` settled on `queue`/`partition`; `None` when
    /// the group has no position there or settled nothing yet. (A group that
    /// registered with `new` starts at the partition's end: that counts as
    /// settled.)
    pub fn cursor(&self, queue: &str, group: &str, partition: &str) -> Option<i64> {
        let st = self.lock();
        let q = st.queues.get(queue)?;
        let pid = q.parts.get(partition)?.id;
        let c = q.groups.get(group)?.rows.get(&pid)?.cursor;
        (c >= 0).then_some(c)
    }

    /// The partition id of `queue`/`partition`, once it exists.
    pub fn partition_id(&self, queue: &str, partition: &str) -> Option<i64> {
        Some(self.lock().queues.get(queue)?.parts.get(partition)?.id)
    }

    /// Queue `fault` for the next call of kind `call` (one-shot, FIFO).
    pub fn inject(&self, call: FakeCall, fault: Fault) {
        self.lock().faults.entry(call).or_default().push_back(fault);
    }

    /// Move the fake clock (leases, KV TTLs, timestamps) forward.
    pub fn advance(&self, d: Duration) {
        let mut st = self.lock();
        st.now_us = st.now_us.saturating_add(d.as_micros() as i64);
    }

    /// Expire every live lease now (their partitions redeliver).
    pub fn expire_leases(&self) {
        let mut st = self.lock();
        let now = st.now_us;
        for q in st.queues.values_mut() {
            for g in q.groups.values_mut() {
                for row in g.rows.values_mut() {
                    if let Some(l) = row.lease.as_mut() {
                        l.expires_us = l.expires_us.min(now);
                    }
                }
            }
        }
    }

    /// How many calls of kind `call` were made (faulted ones included).
    pub fn calls(&self, call: FakeCall) -> u64 {
        self.lock().calls.get(&call).copied().unwrap_or(0)
    }
}

impl QueenApi for FakeQueen {
    fn transaction(&self, body: String) -> BoxFuture<'_, Result<TxnAnswer, QueenError>> {
        Box::pin(async move {
            let (fault, out) = {
                let mut st = self.lock();
                match st.begin(FakeCall::Transaction) {
                    Some(Fault::Fail(e)) => return Err(e),
                    fault => (fault, st.transaction(&body)),
                }
            };
            if matches!(fault, Some(Fault::LoseAnswer)) {
                return Err(lost_answer());
            }
            parse_txn_answer(&out?)
        })
    }

    fn kv(&self, ops: Vec<KvOp>) -> BoxFuture<'_, Result<KvAnswer, QueenError>> {
        Box::pin(async move {
            let (fault, out) = {
                let mut st = self.lock();
                match st.begin(FakeCall::Kv) {
                    Some(Fault::Fail(e)) => return Err(e),
                    fault => (fault, st.kv_call(&ops)),
                }
            };
            if matches!(fault, Some(Fault::LoseAnswer)) {
                return Err(lost_answer());
            }
            parse_kv_answer(&out?)
        })
    }

    fn pop(&self, req: PopRequest) -> BoxFuture<'_, Result<PopAnswer, QueenError>> {
        Box::pin(async move {
            let (fault, out) = {
                let mut st = self.lock();
                match st.begin(FakeCall::Pop) {
                    Some(Fault::Fail(e)) => return Err(e),
                    fault => (fault, st.pop(&req)),
                }
            };
            if matches!(fault, Some(Fault::LoseAnswer)) {
                return Err(lost_answer());
            }
            let messages = out?;
            if messages.is_empty() && req.wait_ms > 0 {
                tokio::time::sleep(Duration::from_millis(req.wait_ms.min(EMPTY_POP_SLEEP_MS)))
                    .await;
            }
            Ok(PopAnswer { messages })
        })
    }

    fn ack(&self, req: AckRequest) -> BoxFuture<'_, Result<AckAnswer, QueenError>> {
        Box::pin(async move {
            let (fault, results) = {
                let mut st = self.lock();
                match st.begin(FakeCall::Ack) {
                    Some(Fault::Fail(e)) => return Err(e),
                    fault => (fault, st.ack(&req)),
                }
            };
            if matches!(fault, Some(Fault::LoseAnswer)) {
                return Err(lost_answer());
            }
            Ok(AckAnswer { results })
        })
    }

    fn extend_lease(
        &self,
        lease_id: String,
        seconds: u32,
    ) -> BoxFuture<'_, Result<(), QueenError>> {
        Box::pin(async move {
            let (fault, out) = {
                let mut st = self.lock();
                match st.begin(FakeCall::ExtendLease) {
                    Some(Fault::Fail(e)) => return Err(e),
                    fault => (fault, st.extend(&lease_id, seconds)),
                }
            };
            if matches!(fault, Some(Fault::LoseAnswer)) {
                return Err(lost_answer());
            }
            out
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::queen::{AckItem, KvResult};

    fn q() -> Arc<FakeQueen> {
        FakeQueen::new()
    }

    async fn kv(f: &FakeQueen, ops: Vec<KvOp>) -> KvAnswer {
        f.kv(ops).await.unwrap()
    }

    fn put_ttl(key: &str, v: Value, ttl: u64, expect: Option<i64>, required: bool) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value: v,
            ttl_seconds: Some(ttl),
            expect,
            required,
        }
    }

    fn put_expect(key: &str, v: Value, expect: i64, required: bool) -> KvOp {
        KvOp::Put {
            key: key.into(),
            value: v,
            ttl_seconds: None,
            expect: Some(expect),
            required,
        }
    }

    // ---- KV -------------------------------------------------------------

    #[tokio::test]
    async fn kv_versions_are_unique_and_a_get_reads_the_live_row() {
        let f = q();
        let a = kv(&f, vec![KvOp::put("a", json!(1)), KvOp::put("b", json!(2))]).await;
        assert!(a.ok);
        let (va, vb) = (a.results[0].version, a.results[1].version);
        assert!(a.results[0].did_apply() && a.results[1].did_apply());
        assert_ne!(va, vb);
        let a2 = kv(&f, vec![KvOp::put("a", json!(3))]).await;
        assert!(
            ![va, vb].contains(&a2.results[0].version),
            "never re-issued"
        );
        let g = kv(&f, vec![KvOp::get("a"), KvOp::get("zz")]).await;
        assert_eq!(g.results[0].value_if_found(), Some(&json!(3)));
        assert_eq!(g.results[0].version, a2.results[0].version);
        assert_eq!(g.results[1].found, Some(false));
        assert_eq!(f.kv_value("a"), Some((json!(3), a2.results[0].version)));
        assert_eq!(f.kv_value("zz"), None);
    }

    #[tokio::test]
    async fn kv_expect_zero_means_must_not_exist_and_reports_the_winner() {
        let f = q();
        let first = kv(&f, vec![put_expect("k", json!("mine"), 0, false)]).await;
        assert!(first.results[0].did_apply());
        let v1 = first.results[0].version;
        let lost = kv(&f, vec![put_expect("k", json!("theirs"), 0, false)]).await;
        let r: &KvResult = &lost.results[0];
        assert!(lost.ok);
        assert_eq!(r.applied, Some(false));
        assert_eq!(r.reason.as_deref(), Some("exists"));
        assert_eq!(r.version, v1);
        assert_eq!(r.value, json!("mine"));
        // expect n: a pure update.
        let wrong = kv(&f, vec![put_expect("k", json!("x"), v1 + 1000, false)]).await;
        assert_eq!(wrong.results[0].reason.as_deref(), Some("version"));
        let absent = kv(&f, vec![put_expect("nope", json!("x"), v1, false)]).await;
        assert_eq!(absent.results[0].reason.as_deref(), Some("absent"));
        assert_eq!(absent.results[0].version, 0);
        assert_eq!(f.kv_value("nope"), None, "expect n never creates");
        let ok = kv(&f, vec![put_expect("k", json!("next"), v1, false)]).await;
        assert!(ok.results[0].did_apply());
        assert_eq!(f.kv_value("k").unwrap().0, json!("next"));
    }

    #[tokio::test]
    async fn kv_ttl_rows_expire_on_the_fake_clock_and_put_if_absent_wins_over_them() {
        let f = q();
        let a = kv(
            &f,
            vec![KvOp::put_if_absent_ttl("lease", json!({"node": "a"}), 10)],
        )
        .await;
        assert!(a.results[0].did_apply());
        let b = kv(
            &f,
            vec![KvOp::put_if_absent_ttl("lease", json!({"node": "b"}), 10)],
        )
        .await;
        assert_eq!(b.results[0].reason.as_deref(), Some("exists"));
        assert_eq!(b.results[0].value, json!({"node": "a"}));
        f.advance(Duration::from_secs(9));
        assert!(f.kv_value("lease").is_some());
        f.advance(Duration::from_secs(1));
        assert!(f.kv_value("lease").is_none(), "expires at exactly ttl");
        let g = kv(&f, vec![KvOp::get("lease")]).await;
        assert_eq!(g.results[0].found, Some(false));
        let c = kv(
            &f,
            vec![KvOp::put_if_absent_ttl("lease", json!({"node": "c"}), 10)],
        )
        .await;
        assert!(
            c.results[0].did_apply(),
            "an expired row does not block putIfAbsent"
        );
        assert_ne!(c.results[0].version, a.results[0].version);
        // A fenced refresh with the stale version loses.
        let stale = kv(
            &f,
            vec![KvOp::fence_ttl(
                "lease",
                json!({"node": "a"}),
                a.results[0].version,
                10,
            )],
        )
        .await;
        assert!(!stale.ok);
        assert_eq!(stale.reason, "kv_precondition");
        assert_eq!(stale.kv_reason.as_deref(), Some("version"));
        assert_eq!(stale.version, c.results[0].version);
        assert_eq!(stale.value, json!({"node": "c"}));
        // An expired row reads as absent to `expect n` too.
        f.advance(Duration::from_secs(10));
        let gone = kv(
            &f,
            vec![put_ttl(
                "lease",
                json!(1),
                5,
                Some(c.results[0].version),
                false,
            )],
        )
        .await;
        assert_eq!(gone.results[0].reason.as_deref(), Some("absent"));
        assert_eq!(gone.results[0].version, 0);
        assert_eq!(gone.results[0].value, Value::Null);
    }

    #[tokio::test]
    async fn a_lost_required_precondition_writes_nothing_of_the_call() {
        let f = q();
        let v = kv(&f, vec![KvOp::put("p", json!("v1"))]).await.results[0].version;
        let a = kv(
            &f,
            vec![
                KvOp::put("other", json!("must not land")),
                KvOp::fence("p", json!("v2"), v + 77),
                KvOp::get("p"),
            ],
        )
        .await;
        assert!(!a.ok);
        assert_eq!(a.reason, "kv_precondition");
        assert_eq!(a.failed_index, 1);
        assert_eq!(a.kv_reason.as_deref(), Some("version"));
        assert_eq!(a.version, v);
        assert_eq!(a.value, json!("v1"));
        assert!(a.results.is_empty());
        assert_eq!(f.kv_value("other"), None);
        assert_eq!(f.kv_value("p"), Some((json!("v1"), v)));
        // Not required: the other writes land, the loser reports.
        let b = kv(
            &f,
            vec![
                KvOp::put("other", json!("lands")),
                put_expect("p", json!("v2"), v + 77, false),
            ],
        )
        .await;
        assert!(b.ok);
        assert!(b.results[0].did_apply());
        assert_eq!(b.results[1].reason.as_deref(), Some("version"));
        assert_eq!(f.kv_value("other").unwrap().0, json!("lands"));
    }

    #[tokio::test]
    async fn kv_deletes_follow_the_broker() {
        let f = q();
        let v = kv(&f, vec![KvOp::put("d", json!(1))]).await.results[0].version;
        let wrong = kv(&f, vec![KvOp::delete("d", Some(v + 1))]).await;
        assert_eq!(wrong.results[0].reason.as_deref(), Some("version"));
        let ok = kv(&f, vec![KvOp::delete("d", Some(v))]).await;
        assert!(ok.results[0].did_apply());
        assert_eq!(ok.results[0].version, v);
        assert_eq!(f.kv_value("d"), None);
        let again = kv(&f, vec![KvOp::delete("d", None)]).await;
        assert_eq!(again.results[0].reason.as_deref(), Some("absent"));
        let must_not = kv(&f, vec![KvOp::delete("d", Some(0))]).await;
        assert!(must_not.results[0].did_apply(), "expect 0 on an absent key");
        let req = kv(
            &f,
            vec![KvOp::Delete {
                key: "d".into(),
                expect: Some(5),
                required: true,
            }],
        )
        .await;
        assert!(!req.ok);
        assert_eq!(req.kv_reason.as_deref(), Some("absent"));
    }

    #[tokio::test]
    async fn get_many_and_get_prefix_page_in_byte_order() {
        let f = q();
        let mut ops = Vec::new();
        for k in ["conn:t:b", "conn:t:a", "conn:t:c", "conn:u:a", "other"] {
            ops.push(KvOp::put(k, json!(k)));
        }
        kv(&f, ops).await;
        kv(
            &f,
            vec![put_ttl("conn:t:expired", json!(0), 1, None, false)],
        )
        .await;
        f.advance(Duration::from_secs(1));
        let m = kv(
            &f,
            vec![KvOp::GetMany {
                keys: vec![
                    "conn:t:c".into(),
                    "missing".into(),
                    "conn:t:a".into(),
                    "conn:t:expired".into(),
                ],
            }],
        )
        .await;
        let keys: Vec<&str> = m.results[0].rows.iter().map(|r| r.key.as_str()).collect();
        assert_eq!(keys, ["conn:t:a", "conn:t:c"]);
        assert_eq!(m.results[0].missing, ["missing", "conn:t:expired"]);
        let p1 = kv(&f, vec![KvOp::get_prefix("conn:", 2, None)]).await;
        let r = &p1.results[0];
        assert_eq!(
            r.rows.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["conn:t:a", "conn:t:b"]
        );
        assert!(r.truncated);
        assert_eq!(r.next_after.as_deref(), Some("conn:t:b"));
        let p2 = kv(&f, vec![KvOp::get_prefix("conn:", 2, r.next_after.clone())]).await;
        let r = &p2.results[0];
        assert_eq!(
            r.rows.iter().map(|r| r.key.as_str()).collect::<Vec<_>>(),
            ["conn:t:c", "conn:u:a"]
        );
        assert!(!r.truncated, "the expired row does not count");
        assert_eq!(r.next_after, None);
        assert_eq!(r.rows[0].value, json!("conn:t:c"));
    }

    #[tokio::test]
    async fn reads_after_a_write_in_the_same_call_see_it_in_apply_order() {
        let f = q();
        let a = kv(
            &f,
            vec![
                KvOp::get("x"),
                KvOp::put("x", json!(1)),
                KvOp::get_prefix("x", 10, None),
            ],
        )
        .await;
        assert_eq!(a.results[0].found, Some(false), "the get before the write");
        assert_eq!(a.results[2].rows.len(), 1, "multi-key reads run last");
    }

    #[tokio::test]
    async fn two_writes_of_one_key_in_a_call_are_refused() {
        let f = q();
        let e = f
            .kv(vec![KvOp::put("x", json!(1)), KvOp::delete("x", None)])
            .await
            .unwrap_err();
        assert!(matches!(e, QueenError::Status { code: 400, .. }), "{e}");
        assert_eq!(f.kv_value("x"), None);
    }

    // ---- transactions ---------------------------------------------------

    fn bundle(items: &[(&str, &str, &str, &str)], kv_ops: &[KvOp]) -> String {
        let items: Vec<Value> = items
            .iter()
            .map(|(q, p, payload, txn)| {
                json!({"queue": q, "partition": p,
                       "payload": serde_json::from_str::<Value>(payload).unwrap(),
                       "transactionId": txn})
            })
            .collect();
        json!({
            "operations": [{"type": "push", "items": items}],
            "kv": kv_ops.iter().map(KvOp::to_json).collect::<Vec<_>>(),
        })
        .to_string()
    }

    #[tokio::test]
    async fn a_transaction_commits_pushes_and_the_rider_together() {
        let f = q();
        let body = bundle(
            &[
                ("orders", "42", r#"{"id":42}"#, "pg:e:1:0"),
                ("orders", "7", r#"{"id":7}"#, "pg:e:1:1"),
                ("pay", "42", r#"{"x":1}"#, "pg:e:1:2"),
            ],
            &[KvOp::Put {
                key: "src:a:pointer".into(),
                value: json!({"lsn": "0/10"}),
                ttl_seconds: None,
                expect: Some(0),
                required: true,
            }],
        );
        let a = f.transaction(body).await.unwrap();
        assert!(a.success, "{a:?}");
        assert_eq!(a.results.len(), 4);
        assert_eq!(a.results[0]["type"], "push");
        assert_eq!(a.results[0]["index"], 0);
        assert_eq!(a.results[2]["queueName"], "pay");
        let k = a.kv_result(0).unwrap();
        assert!(k.did_apply());
        assert_eq!(a.results[3]["index"], 3);
        assert_eq!(
            f.kv_value("src:a:pointer"),
            Some((json!({"lsn": "0/10"}), k.version))
        );
        let m = f.messages("orders");
        assert_eq!(m.len(), 2);
        assert_eq!(m[0].partition, "42");
        assert_eq!(m[0].offset, 0);
        assert_eq!(m[1].partition, "7");
        assert!(
            m[0].partition_id < m[1].partition_id,
            "ids in creation order"
        );
        assert_ne!(f.partition_id("pay", "42"), f.partition_id("orders", "42"));
        assert_eq!(m[0].payload, r#"{"id":42}"#);
    }

    #[tokio::test]
    async fn payloads_are_stored_byte_exact() {
        let f = q();
        let body = r#"{"operations":[{"type":"push","items":[{"queue":"q","partition":"p","payload":{"big":123456789012345678901234567890,"f":1.10,"s":"é"},"transactionId":"t"}]}]}"#;
        assert!(f.transaction(body.into()).await.unwrap().success);
        assert_eq!(
            f.messages("q")[0].payload,
            r#"{"big":123456789012345678901234567890,"f":1.10,"s":"é"}"#
        );
    }

    #[tokio::test]
    async fn a_stored_duplicate_rolls_the_whole_bundle_back() {
        let f = q();
        f.push_raw("orders", "42", r#"{"old":1}"#, Some("dup"));
        let v = kv(&f, vec![KvOp::put("ptr", json!(1))]).await.results[0].version;
        let body = bundle(
            &[
                ("orders", "1", "{}", "fresh"),
                ("orders", "42", "{}", "dup"),
            ],
            &[KvOp::fence("ptr", json!(2), v)],
        );
        let a = f.transaction(body).await.unwrap();
        assert!(a.is_duplicate(), "{a:?}");
        assert!(a.results.is_empty());
        assert_eq!(f.messages("orders").len(), 1, "the fresh push rolled back");
        assert_eq!(f.kv_value("ptr"), Some((json!(1), v)), "so did the rider");
        // The same id in ANOTHER partition is not a duplicate.
        let ok = f
            .transaction(bundle(&[("orders", "43", "{}", "dup")], &[]))
            .await
            .unwrap();
        assert!(ok.success);
    }

    #[tokio::test]
    async fn the_same_message_twice_in_one_bundle_collapses() {
        let f = q();
        let a = f
            .transaction(bundle(
                &[
                    ("q", "p", r#"{"a":1}"#, "t1"),
                    ("q", "p", r#"{"a":2}"#, "t1"),
                ],
                &[],
            ))
            .await
            .unwrap();
        assert!(a.success);
        assert_eq!(a.results[1]["duplicate"], true);
        assert_eq!(a.results[1]["messageId"], a.results[0]["messageId"]);
        assert_eq!(f.messages("q").len(), 1);
        assert_eq!(f.messages("q")[0].payload, r#"{"a":1}"#);
    }

    #[tokio::test]
    async fn a_lost_rider_precondition_reports_the_flat_index() {
        let f = q();
        let v = kv(&f, vec![KvOp::put("ptr", json!({"lsn": "0/1"}))])
            .await
            .results[0]
            .version;
        let body = bundle(
            &[("q", "a", "{}", "x1"), ("q", "b", "{}", "x2")],
            &[
                KvOp::put("side", json!(1)),
                KvOp::fence("ptr", json!({"lsn": "0/2"}), v + 5),
            ],
        );
        let a = f.transaction(body).await.unwrap();
        assert!(a.is_precondition(), "{a:?}");
        assert_eq!(a.failed_index, Some(2 + 1));
        assert_eq!(a.kv_reason.as_deref(), Some("version"));
        assert_eq!(a.version, Some(v));
        assert_eq!(a.value, Some(json!({"lsn": "0/1"})));
        assert!(f.messages("q").is_empty());
        assert_eq!(f.kv_value("side"), None);
    }

    #[tokio::test]
    async fn the_inline_push_form_and_a_kv_only_bundle() {
        let f = q();
        let a = f
            .transaction(
                r#"{"operations":[{"type":"push","queue":"q","partition":"p","payload":[1,2],"transactionId":"i1"}]}"#.into(),
            )
            .await
            .unwrap();
        assert!(a.success);
        assert_eq!(f.messages("q")[0].payload, "[1,2]");
        let b = f
            .transaction(json!({"kv": [KvOp::put("only", json!(true)).to_json()]}).to_string())
            .await
            .unwrap();
        assert!(b.success);
        assert_eq!(b.kv_result(0).unwrap().index, 0);
        assert_eq!(f.kv_value("only").unwrap().0, json!(true));
        // No partition: the broker's "Default".
        f.transaction(r#"{"operations":[{"type":"push","queue":"d","payload":1}]}"#.into())
            .await
            .unwrap();
        assert_eq!(f.messages("d")[0].partition, "Default");
    }

    #[tokio::test]
    async fn malformed_bundles_are_refused() {
        let f = q();
        let e = f.transaction("not json".into()).await.unwrap_err();
        assert!(matches!(e, QueenError::Status { code: 400, .. }));
        let a = f.transaction("{}".into()).await.unwrap();
        assert!(!a.success);
        assert_eq!(a.reason.as_deref(), Some("bad_request"));
        let rider: Vec<Value> = (0..65)
            .map(|i| KvOp::put(format!("k{i}"), json!(i)).to_json())
            .collect();
        let e = f
            .transaction(json!({"kv": rider}).to_string())
            .await
            .unwrap_err();
        assert!(matches!(e, QueenError::Status { code: 400, .. }));
        let e = f
            .transaction(json!({"kv": [KvOp::get_prefix("a", 1, None).to_json()]}).to_string())
            .await
            .unwrap_err();
        assert!(e.to_string().contains("getPrefix"), "{e}");
    }

    // ---- pop / ack / extend ----------------------------------------------

    fn pop_req(group: &str, batch: u32, mode: &str) -> PopRequest {
        PopRequest {
            queue: "q".into(),
            group: group.into(),
            batch,
            wait_ms: 0,
            lease_seconds: 30,
            subscription_mode: mode.into(),
            max_partitions: None,
        }
    }

    fn ack_of(m: &Popped, status: AckStatus) -> AckItem {
        AckItem {
            transaction_id: m.transaction_id.clone(),
            partition_id: m.partition_id.clone(),
            lease_id: m.lease_id.clone(),
            status,
            error: None,
        }
    }

    async fn ack(f: &FakeQueen, group: &str, items: Vec<AckItem>) -> Vec<AckItemResult> {
        f.ack(AckRequest {
            group: group.into(),
            items,
        })
        .await
        .unwrap()
        .results
    }

    #[tokio::test]
    async fn pop_claims_partitions_in_order_with_one_lease_and_the_batch_budget() {
        let f = q();
        for i in 0..3 {
            f.push_raw("q", "a", &format!("{{\"a\":{i}}}"), None);
            f.push_raw("q", "b", &format!("{{\"b\":{i}}}"), None);
        }
        let p = f.pop(pop_req("g", 4, "all")).await.unwrap().messages;
        let got: Vec<(String, i64)> = p.iter().map(|m| (m.partition.clone(), m.offset)).collect();
        assert_eq!(
            got,
            [
                ("a".into(), 0),
                ("a".into(), 1),
                ("a".into(), 2),
                ("b".into(), 0)
            ]
        );
        assert!(p.iter().all(|m| m.lease_id == p[0].lease_id));
        assert!(p.iter().all(|m| m.delivery_attempt == 1));
        assert_eq!(p[0].data.get(), r#"{"a":0}"#);
        assert_eq!(p[0].partition_number(), f.partition_id("q", "a"));
        // Both partitions are leased now.
        assert!(f
            .pop(pop_req("g", 10, "all"))
            .await
            .unwrap()
            .messages
            .is_empty());
        // Another group is independent.
        let other = f.pop(pop_req("g2", 10, "all")).await.unwrap().messages;
        assert_eq!(other.len(), 6);
        assert_ne!(other[0].lease_id, p[0].lease_id);
        let mut one = pop_req("g3", 10, "all");
        one.max_partitions = Some(1);
        assert_eq!(f.pop(one).await.unwrap().messages.len(), 3);
    }

    #[tokio::test]
    async fn subscription_mode_new_starts_existing_partitions_at_their_end() {
        let f = q();
        f.push_raw("q", "old", "1", None);
        f.push_raw("q", "old", "2", None);
        assert!(f
            .pop(pop_req("n", 10, "new"))
            .await
            .unwrap()
            .messages
            .is_empty());
        assert_eq!(f.cursor("q", "n", "old"), Some(1));
        f.push_raw("q", "old", "3", None);
        f.push_raw("q", "fresh", "4", None);
        let p = f.pop(pop_req("n", 10, "new")).await.unwrap().messages;
        let got: Vec<&str> = p.iter().map(|m| m.data.get()).collect();
        assert_eq!(got, ["3", "4"]);
        let e = f.pop(pop_req("x", 1, "latest")).await.unwrap_err();
        assert!(matches!(e, QueenError::Status { code: 400, .. }));
    }

    #[tokio::test]
    async fn acks_advance_the_cursor_and_release_the_lease_at_the_end() {
        let f = q();
        for i in 0..3 {
            f.push_raw("q", "p", &i.to_string(), Some(&format!("t{i}")));
        }
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        let r = ack(&f, "g", vec![ack_of(&p[0], AckStatus::Ok)]).await;
        assert!(r[0].success && !r[0].lease_released);
        assert_eq!(f.cursor("q", "g", "p"), Some(0));
        let r = ack(
            &f,
            "g",
            vec![ack_of(&p[2], AckStatus::Ok), ack_of(&p[1], AckStatus::Ok)],
        )
        .await;
        assert!(
            r[0].success && r[0].lease_released,
            "cumulative: 2 settles 1 too"
        );
        assert!(r[1].success && r[1].noop);
        assert_eq!(r[1].index, 1);
        assert_eq!(f.cursor("q", "g", "p"), Some(2));
        // A lost answer, the same ack again: success, nothing changes.
        let again = ack(&f, "g", vec![ack_of(&p[2], AckStatus::Ok)]).await;
        assert!(again[0].success && again[0].noop);
        f.push_raw("q", "p", "3", Some("t3"));
        let p2 = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        assert_eq!(p2.len(), 1);
        assert_eq!(p2[0].offset, 3);
    }

    #[tokio::test]
    async fn a_wrong_or_expired_lease_is_refused_and_redelivers() {
        let f = q();
        f.push_raw("q", "p", "1", Some("t1"));
        f.push_raw("q", "p", "2", Some("t2"));
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        let mut forged = ack_of(&p[0], AckStatus::Ok);
        forged.lease_id = "someone-else".into();
        let r = ack(&f, "g", vec![forged]).await;
        assert!(!r[0].success);
        assert_eq!(r[0].error.as_deref(), Some("invalid or expired lease"));
        ack(&f, "g", vec![ack_of(&p[0], AckStatus::Ok)]).await;
        f.advance(Duration::from_secs(31));
        let r = ack(&f, "g", vec![ack_of(&p[1], AckStatus::Ok)]).await;
        assert!(!r[0].success, "the lease ran out");
        let again = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        assert_eq!(again.len(), 1);
        assert_eq!(again[0].offset, 1);
        assert_eq!(again[0].delivery_attempt, 2);
        assert_ne!(again[0].lease_id, p[0].lease_id);
        // The old lease's settled message: not released by an ack, refused.
        let old = ack(&f, "g", vec![ack_of(&p[0], AckStatus::Ok)]).await;
        assert!(!old[0].success);
        assert!(ack(&f, "g", vec![ack_of(&again[0], AckStatus::Ok)]).await[0].success);
        assert_eq!(f.cursor("q", "g", "p"), Some(1));
    }

    #[tokio::test]
    async fn a_dead_letter_releases_the_lease_so_the_rest_comes_back_at_once() {
        // The broker's forced DLQ releases the lease (rsm/consume/ack.rs): a
        // consumer that files a poison and acks nothing after it gets the
        // rest of the batch on its next pop, not when the lease runs out.
        let f = q();
        for i in 0..4 {
            f.push_raw("q", "p", &format!("{{\"n\":{i}}}"), Some(&format!("t{i}")));
        }
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        assert_eq!(p.len(), 4);
        let r = ack(
            &f,
            "g",
            vec![ack_of(&p[0], AckStatus::Ok), ack_of(&p[1], AckStatus::Dlq)],
        )
        .await;
        assert!(r[0].success && r[1].success && r[1].dlq && r[1].lease_released);
        assert_eq!(f.cursor("q", "g", "p"), Some(1));
        assert_eq!(f.dlq("q").len(), 1);
        // No clock advance: the rest is claimable now.
        let rest = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        assert_eq!(rest.iter().map(|m| m.offset).collect::<Vec<_>>(), [2, 3]);
        // The released lease's dead letter, acked again (a lost answer): success.
        let again = ack(&f, "g", vec![ack_of(&p[1], AckStatus::Dlq)]).await;
        assert!(again[0].success && again[0].noop);
        assert_eq!(f.dlq("q").len(), 1);
    }

    #[tokio::test]
    async fn failed_redelivers_and_dlq_files_and_moves_on() {
        let f = q();
        for i in 0..3 {
            f.push_raw("q", "p", &format!("{{\"n\":{i}}}"), Some(&format!("t{i}")));
        }
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        let r = ack(
            &f,
            "g",
            vec![
                ack_of(&p[0], AckStatus::Ok),
                ack_of(&p[1], AckStatus::Failed),
            ],
        )
        .await;
        assert!(r[0].success && r[1].success && r[1].lease_released);
        assert_eq!(f.cursor("q", "g", "p"), Some(0));
        let p2 = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        assert_eq!(p2.iter().map(|m| m.offset).collect::<Vec<_>>(), [1, 2]);
        assert_eq!(p2[0].delivery_attempt, 2);
        let mut d = ack_of(&p2[0], AckStatus::Dlq);
        d.error = Some("bad row".into());
        let r = ack(&f, "g", vec![d, ack_of(&p2[1], AckStatus::Ok)]).await;
        assert!(r[0].success && r[0].dlq);
        assert!(r[1].lease_released);
        let dlq = f.dlq("q");
        assert_eq!(dlq.len(), 1);
        assert_eq!((dlq[0].offset, dlq[0].payload.as_str()), (1, r#"{"n":1}"#));
        assert_eq!(f.cursor("q", "g", "p"), Some(2));
        let unknown = ack(
            &f,
            "g",
            vec![AckItem {
                transaction_id: "t0".into(),
                partition_id: "1".into(),
                lease_id: "x".into(),
                status: AckStatus::Ok,
                error: None,
            }],
        )
        .await;
        assert!(!unknown[0].success);
    }

    #[tokio::test]
    async fn extend_lease_keeps_a_lease_alive_past_its_first_expiry() {
        let f = q();
        f.push_raw("q", "a", "1", Some("t1"));
        f.push_raw("q", "b", "2", Some("t2"));
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        f.advance(Duration::from_secs(20));
        f.extend_lease(p[0].lease_id.clone(), 30).await.unwrap();
        f.advance(Duration::from_secs(20));
        let r = ack(
            &f,
            "g",
            vec![ack_of(&p[0], AckStatus::Ok), ack_of(&p[1], AckStatus::Ok)],
        )
        .await;
        assert!(
            r[0].success && r[1].success,
            "both partitions of the lease were renewed"
        );
        let e = f.extend_lease("nope".into(), 30).await.unwrap_err();
        assert!(matches!(e, QueenError::Status { code: 404, .. }));
        f.push_raw("q", "a", "3", Some("t3"));
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        f.expire_leases();
        assert!(f.extend_lease(p[0].lease_id.clone(), 30).await.is_err());
        assert_eq!(
            f.pop(pop_req("g", 10, "all")).await.unwrap().messages.len(),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_pop_waits_a_little_and_answers_empty() {
        let f = q();
        let t0 = tokio::time::Instant::now();
        let mut r = pop_req("g", 10, "all");
        r.wait_ms = 5_000;
        assert!(f.pop(r).await.unwrap().messages.is_empty());
        assert_eq!(t0.elapsed(), Duration::from_millis(EMPTY_POP_SLEEP_MS));
    }

    // ---- faults -----------------------------------------------------------

    #[tokio::test]
    async fn fail_refuses_before_applying_and_faults_are_fifo_per_kind() {
        let f = q();
        let e503 = QueenError::Status {
            code: 503,
            body: "{}".into(),
            retry_after_ms: Some(100),
        };
        f.inject(FakeCall::Transaction, Fault::Fail(e503.clone()));
        f.inject(
            FakeCall::Transaction,
            Fault::Fail(QueenError::Transport("gone".into())),
        );
        let body = bundle(&[("q", "p", "{}", "t")], &[]);
        assert_eq!(f.transaction(body.clone()).await.unwrap_err(), e503);
        assert_eq!(
            f.transaction(body.clone()).await.unwrap_err(),
            QueenError::Transport("gone".into())
        );
        assert!(f.messages("q").is_empty());
        assert!(f.transaction(body).await.unwrap().success);
        assert_eq!(f.calls(FakeCall::Transaction), 3);
        assert_eq!(f.calls(FakeCall::Kv), 0);
        f.inject(FakeCall::Kv, Fault::Fail(QueenError::Transport("x".into())));
        assert!(f.kv(vec![KvOp::put("a", json!(1))]).await.is_err());
        assert_eq!(f.kv_value("a"), None);
    }

    #[tokio::test]
    async fn lose_answer_applies_then_answers_in_doubt() {
        let f = q();
        f.inject(FakeCall::Transaction, Fault::LoseAnswer);
        let e = f
            .transaction(bundle(
                &[("q", "p", "{}", "t")],
                &[KvOp::put("ptr", json!(1))],
            ))
            .await
            .unwrap_err();
        assert!(e.is_in_doubt(), "{e}");
        assert!(
            matches!(&e, QueenError::Status { code: 503, body, .. } if body.contains("outcome_unknown"))
        );
        assert_eq!(f.messages("q").len(), 1, "it committed");
        assert!(f.kv_value("ptr").is_some());
        // The retry of the same bundle is then a duplicate.
        let again = f
            .transaction(bundle(&[("q", "p", "{}", "t")], &[]))
            .await
            .unwrap();
        assert!(again.is_duplicate());

        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        f.inject(FakeCall::Ack, Fault::LoseAnswer);
        let e = f
            .ack(AckRequest {
                group: "g".into(),
                items: vec![ack_of(&p[0], AckStatus::Ok)],
            })
            .await
            .unwrap_err();
        assert!(e.is_in_doubt());
        assert_eq!(f.cursor("q", "g", "p"), Some(0), "the ack applied");
        // The re-send: the broker's idempotent re-ack.
        let r = ack(&f, "g", vec![ack_of(&p[0], AckStatus::Ok)]).await;
        assert!(r[0].success);
        assert_eq!(f.calls(FakeCall::Ack), 2);
    }

    #[tokio::test]
    async fn a_lost_pop_answer_leaves_the_partition_leased_until_it_expires() {
        let f = q();
        f.push_raw("q", "p", "1", None);
        f.inject(FakeCall::Pop, Fault::LoseAnswer);
        assert!(f.pop(pop_req("g", 10, "all")).await.is_err());
        assert!(f
            .pop(pop_req("g", 10, "all"))
            .await
            .unwrap()
            .messages
            .is_empty());
        f.expire_leases();
        let p = f.pop(pop_req("g", 10, "all")).await.unwrap().messages;
        assert_eq!(p.len(), 1);
        assert_eq!(p[0].delivery_attempt, 2);
    }

    #[test]
    fn timestamps_render_like_the_broker() {
        // 2026-10-02T10:00:00.123456Z
        let us = 1_790_935_200_123_456;
        assert_eq!(iso_us(us), "2026-10-02T10:00:00.123456Z");
        assert_eq!(ts_jsonb(us), "2026-10-02T10:00:00.123456+00:00");
        assert_eq!(ts_jsonb(us - 123_456), "2026-10-02T10:00:00+00:00");
        assert_eq!(ts_jsonb(us - 3_456), "2026-10-02T10:00:00.12+00:00");
        assert_eq!(iso_us(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(iso_us(951_782_400_000_000), "2000-02-29T00:00:00.000000Z");
    }
}
