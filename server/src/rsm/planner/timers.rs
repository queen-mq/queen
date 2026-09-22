//! Timers — the port of `025_log_timers.sql` onto the RSM (PLAN_RAFT.md WP-2.3).
//!
//! # What maps to what
//!
//! | 025 | here |
//! |---|---|
//! | `queen.log_timers` | [`Keyspace::Timers`](crate::rsm::store::Keyspace::Timers), one [`TimerRow`] per `(tenant, queue, timer_key)` |
//! | `idx_log_timers_visible (shard, visible_at)` | [`Keyspace::TimersDue`](crate::rsm::store::Keyspace::TimersDue), `(due_us, tenant, queue, key)` |
//! | `log_timers_apply_v1` | [`Planner::plan_timer_ops`] (the reusable helper) behind [`Planner::plan_timers`] |
//! | `log_timers_due_v1` + `claim_v1` + `fire_v1` | [`Planner::plan_timer_fire`], one leader-loop step per tick |
//! | `log_timers_fail_v1` | a `TimerBackoff` effect in the SAME fire entry |
//! | `log_timers_dlq_v1` | `DlqInsert` (group `__timer__`, offset −1) + `TimerDelete`, same entry |
//! | `peek_v1` / `list_v1` / `count_v1` | local reads in the facade, rendered by [`peek_json`] / [`list_row_json`] |
//!
//! # Why there is no claim, no lease and no `too_late`
//!
//! 025 needs `claim_token`/`claimed_until` because its fire spans a broker round
//! trip: rows are claimed in one transaction, packed outside it, and pushed in
//! another. Here the fire is ONE ENTRY planned by the single serial planner: the
//! message's `Append` (plus the implicit queue/partition creation of the push
//! path) and the timer's `TimerDelete` are applied together or not at all, and
//! the overlay makes a timer whose fire entry is still in flight invisible to
//! the next cycle. There is therefore no window in which a timer is "in
//! somebody's hands", and a cancel or a reschedule is never `too_late`: it either
//! lands before the fire (the timer is gone / replaced) or after it (`absent`, or
//! a new timer under the old name — exactly 025's answer once its fire commits).
//!
//! # The fire reuses the push planner
//!
//! A fire plans a [`PushCommand`] through [`Planner::plan_push`], so the fired
//! message gets gapless offsets, the monotone `created_at`, the implicit queue
//! and partition creation and the dedup probe EXACTLY like a push. One deliberate
//! difference from 025, stated rather than discovered: 025's fire passes
//! `p_verified = last_offset`, which skips the dedup window probe for cost
//! reasons (its §6.2 note), so it never answers `duplicate`. The RSM probe is an
//! in-memory, bloom-fronted index lookup, so the fixed `txn` IS the secondary net
//! it was designed to be: a fired message whose `txn` is already in the
//! destination partition's dedup window is a `duplicate` — 025's own duplicate
//! arm: the timer is DONE, deleted, and nothing is appended.
//!
//! # Determinism
//!
//! Everything a fire decides travels in effects (I2): `deliver_at` is the
//! planner's `now_us + delay`, a backoff's `visible_at` is `now_us + backoff`,
//! ids are minted here. Apply only overwrites rows.

use std::collections::{BTreeMap, HashMap, HashSet};

use base64::Engine;
use serde_json::{Map, Value};

use crate::rsm::effect::{Effect, QueueConfig, TimerRow, VERSION_1};
use crate::rsm::entry::{Outcome, Placeholder, PushVerdict, RequestId};
use crate::rsm::store::rows::timer_due_us;
use crate::rsm::store::{Reads, TypedReads};

use super::{
    store_err, Overlay, Plan, Planned, Planner, PushCommand, PushItem, Refusal, TimerOverlay,
};

/// The outcome tag of a timers command: a [`Placeholder`] whose body is the
/// index-aligned JSON result array (the reserved range of `entry::tag`; this
/// planner owns the encoding, and a typed outcome can replace it later).
pub const TIMERS_OUTCOME_TAG: u16 = 0xF003;

/// 025 §4.5: a timer never had a consumer group; its dead letter files under
/// this one, at offset −1.
pub const TIMER_DLQ_GROUP: &str = "__timer__";

/// 025's column default for `partition`.
pub const DEFAULT_PARTITION: &str = "Default";

// ---------------------------------------------------------------------------
// Command inputs and results
// ---------------------------------------------------------------------------

/// One validated schedule/reschedule. The receiver has done the pool-free
/// pre-work (O20: the planner never packs or decompresses): the payload is
/// decoded, decompressed when the client flagged `payloadZstd`, and packed into
/// the ONE frame the fire will append; the message id is minted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerSchedule {
    pub queue: String,
    pub key: String,
    pub partition: String,
    /// Relative, in µs. May be negative: a delay in the past is LEGAL and
    /// fires on the first cycle (025 §4.2).
    pub delay_us: i64,
    pub txn: String,
    pub message_id: [u8; 16],
    /// Exactly the bytes a push of this message would carry
    /// ([`crate::frames::pack_frames`] of one frame).
    pub frame: Vec<u8>,
    /// Whether the STORED payload is zstd-compressed. The receiver
    /// decompresses before packing, so this is `false` for every frame it
    /// builds; kept because it is 025's column and the peek reports it.
    pub payload_zstd: bool,
    pub encrypted: bool,
    /// The AUTHENTICATED sub of the caller, never a client field (025 §4.2).
    pub producer_sub: Option<String>,
}

/// One op of a timers call (025 `p_ops[i]`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerOp {
    /// `schedule` and `reschedule` are the same upsert (025).
    Schedule(TimerSchedule),
    /// `txn` is the caller's expected txn, echoed back on `absent` so the
    /// "was it already delivered?" check needs no second call (025 §4.4).
    Cancel {
        queue: String,
        key: String,
        txn: Option<String>,
    },
}

impl TimerOp {
    pub fn queue(&self) -> &str {
        match self {
            TimerOp::Schedule(s) => &s.queue,
            TimerOp::Cancel { queue, .. } => queue,
        }
    }

    pub fn key(&self) -> &str {
        match self {
            TimerOp::Schedule(s) => &s.key,
            TimerOp::Cancel { key, .. } => key,
        }
    }

    /// A cheap upper estimate of what the op adds to an entry.
    pub fn size_hint(&self) -> usize {
        match self {
            TimerOp::Schedule(s) => {
                s.frame.len() + s.txn.len() + s.key.len() + s.queue.len() + s.partition.len() + 160
            }
            TimerOp::Cancel { queue, key, .. } => queue.len() + key.len() + 48,
        }
    }
}

/// `POST /api/v1/timers` and `DELETE /api/v1/timers/:queue/*key` (025
/// `log_timers_apply_v1`): one command, one atomic unit.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimersCommand {
    pub request_id: RequestId,
    pub tenant: String,
    pub ops: Vec<TimerOp>,
}

/// The verdict of one op, index-aligned with the ops (025 §4.1's closed
/// taxonomy minus `too_late`, which cannot happen here — see the module
/// header).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TimerOpResult {
    Scheduled {
        queue: String,
        key: String,
        txn: String,
        message_id: [u8; 16],
        deliver_at_us: i64,
        /// The key already held a pending timer (025's `xmax <> 0` arm).
        rescheduled: bool,
    },
    Cancelled {
        queue: String,
        key: String,
        /// The txn of the timer that was removed.
        txn: String,
    },
    /// No pending timer under that key. It MAY have been delivered already
    /// (025 §4.4: there is no tombstone), hence `ok:false`.
    Absent {
        queue: String,
        key: String,
        txn: Option<String>,
    },
}

impl TimerOpResult {
    /// The wire object 025 builds with `jsonb_build_object` for this verdict.
    pub fn to_json(&self) -> Value {
        let mut m = Map::new();
        match self {
            TimerOpResult::Scheduled {
                queue,
                key,
                txn,
                message_id,
                deliver_at_us,
                rescheduled,
            } => {
                m.insert("ok".into(), Value::Bool(true));
                m.insert(
                    "status".into(),
                    Value::String(
                        if *rescheduled {
                            "rescheduled"
                        } else {
                            "scheduled"
                        }
                        .into(),
                    ),
                );
                m.insert("queue".into(), Value::String(queue.clone()));
                m.insert("timerKey".into(), Value::String(key.clone()));
                m.insert("txn".into(), Value::String(txn.clone()));
                m.insert(
                    "messageId".into(),
                    Value::String(crate::frames::uuid_bytes_to_string(message_id)),
                );
                m.insert("deliverAt".into(), Value::String(iso_us(*deliver_at_us)));
            }
            TimerOpResult::Cancelled { queue, key, txn } => {
                m.insert("ok".into(), Value::Bool(true));
                m.insert("status".into(), Value::String("cancelled".into()));
                m.insert("queue".into(), Value::String(queue.clone()));
                m.insert("timerKey".into(), Value::String(key.clone()));
                m.insert("txn".into(), Value::String(txn.clone()));
            }
            TimerOpResult::Absent { queue, key, txn } => {
                m.insert("ok".into(), Value::Bool(false));
                m.insert("status".into(), Value::String("absent".into()));
                m.insert("queue".into(), Value::String(queue.clone()));
                m.insert("timerKey".into(), Value::String(key.clone()));
                m.insert(
                    "txn".into(),
                    txn.clone().map(Value::String).unwrap_or(Value::Null),
                );
            }
        }
        Value::Object(m)
    }
}

/// The outcome recorded for a timers command: the results as the JSON array
/// the receiver answers with, so a retry of the request id (D6, I6) is
/// answered byte-for-byte from state.
pub fn timers_outcome(results: &[TimerOpResult]) -> Result<Outcome, Refusal> {
    let body = Value::Array(results.iter().map(TimerOpResult::to_json).collect())
        .to_string()
        .into_bytes();
    Placeholder::new(TIMERS_OUTCOME_TAG, VERSION_1, body)
        .map(Outcome::Placeholder)
        .map_err(|e| Refusal::retry("internal", format!("timers outcome: {e:?}")))
}

/// The JSON results of a timers outcome, `None` when `o` is not one.
pub fn timers_results(o: &Outcome) -> Option<Vec<Value>> {
    match o {
        Outcome::Placeholder(p) if p.tag() == TIMERS_OUTCOME_TAG => {
            match serde_json::from_slice::<Value>(p.body()) {
                Ok(Value::Array(a)) => Some(a),
                _ => None,
            }
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Receiver pre-work: validate and build the ops (pure, no state)
// ---------------------------------------------------------------------------

/// A refusal of the WHOLE call that 025 raises with SQLSTATE 22023
/// (`timers_bad_request`, HTTP 400): the message and, where 025 has one, its
/// HINT. Validate-then-apply: nothing is scheduled when one op is bad.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpError {
    pub message: String,
    pub hint: Option<&'static str>,
}

impl OpError {
    fn new(message: String) -> OpError {
        OpError {
            message,
            hint: None,
        }
    }
}

/// 025's server-owned field list: present in an op, a refusal, never a field
/// quietly ignored (§4.2).
const SERVER_OWNED: [&str; 16] = [
    "producerSub",
    "producer_sub",
    "messageId",
    "message_id",
    "tenant",
    "tenantId",
    "_tenant",
    "tenant_id",
    "deliverAt",
    "deliver_at",
    "delaySeconds",
    "delay_seconds",
    "attempts",
    "claimToken",
    "claim_token",
    "claimedUntil",
];

const SERVER_OWNED_HINT: &str = "producerSub and the tenant come from the authenticated request; \
     the message id is minted by the broker; deliverAt is not expressible — the wire carries \
     only the relative delayMs";

const DELAY_HINT: &str = "a delayMs in the past is LEGAL and fires on the first cycle; an \
     absolute instant is not expressible on this wire";

const DUP_HINT: &str = "one key at most once per call is what makes the intra-space lock order \
     total (PLAN_KV_TIMERS §2.2)";

/// `op->>'field'`: the text of a JSON scalar (a string as itself, a number or
/// a boolean as its literal), `None` for an absent key or a JSON null.
fn jtext(o: &Map<String, Value>, k: &str) -> Option<String> {
    match o.get(k)? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// `(op->>'field')::boolean`, defaulting to false (025's COALESCE).
fn jbool(o: &Map<String, Value>, k: &str, i: usize) -> Result<bool, OpError> {
    match o.get(k) {
        None | Some(Value::Null) => Ok(false),
        Some(Value::Bool(b)) => Ok(*b),
        Some(Value::String(s)) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "t" | "yes" | "y" | "on" | "1" => Ok(true),
            "false" | "f" | "no" | "n" | "off" | "0" => Ok(false),
            _ => Err(OpError::new(format!("QTIMER op {i}: {k} is not a boolean"))),
        },
        Some(Value::Number(n)) if n.as_i64() == Some(0) => Ok(false),
        Some(Value::Number(n)) if n.as_i64() == Some(1) => Ok(true),
        Some(_) => Err(OpError::new(format!("QTIMER op {i}: {k} is not a boolean"))),
    }
}

/// Validate and build the ops of one call, in 025's order: every per-op check
/// first (object, known op, server-owned fields, queue, timerKey, partition,
/// then delayMs/txn/payload for a schedule), then the one-key-per-call rule,
/// then the build. `producer_sub` is the AUTHENTICATED sub, stamped on every
/// schedule's frame.
///
/// The build is the receiver's pre-work (O20): base64 decode, zstd
/// decompression when `payloadZstd` (025's fire decompresses before packing;
/// doing it here keeps the serial planner free of it and the delivered frame
/// byte-identical), the message id (`_messageId` when the HTTP edge minted one,
/// else minted here), and the ONE frame the fire will append.
pub fn parse_timer_ops(ops: &[Value], producer_sub: Option<&str>) -> Result<Vec<TimerOp>, OpError> {
    // ------------------------------------------------------------- VALIDATE
    for (i, v) in ops.iter().enumerate() {
        let Some(o) = v.as_object() else {
            return Err(OpError::new(format!("QTIMER op {i} is not an object")));
        };
        let kind = o.get("op").and_then(Value::as_str);
        if !matches!(kind, Some("schedule") | Some("reschedule") | Some("cancel")) {
            return Err(OpError::new(format!("QTIMER op {i}: unknown operation")));
        }
        for f in SERVER_OWNED {
            if o.contains_key(f) {
                return Err(OpError {
                    message: format!(
                        "QTIMER op {i}: field {f} is server-owned and cannot be supplied"
                    ),
                    hint: Some(SERVER_OWNED_HINT),
                });
            }
        }
        if jtext(o, "queue").unwrap_or_default().is_empty() {
            return Err(OpError::new(format!("QTIMER op {i}: queue is required")));
        }
        if jtext(o, "timerKey").unwrap_or_default().is_empty() {
            return Err(OpError::new(format!("QTIMER op {i}: timerKey is required")));
        }
        if o.contains_key("partition") && jtext(o, "partition").unwrap_or_default().is_empty() {
            return Err(OpError::new(format!(
                "QTIMER op {i}: partition, when present, must be non-empty"
            )));
        }
        if kind != Some("cancel") {
            if !o.get("delayMs").is_some_and(Value::is_number) {
                return Err(OpError {
                    message: format!(
                        "QTIMER op {i}: delayMs (a number of milliseconds) is required"
                    ),
                    hint: Some(DELAY_HINT),
                });
            }
            if jtext(o, "txn").unwrap_or_default().is_empty() {
                return Err(OpError::new(format!("QTIMER op {i}: txn is required")));
            }
            if !o.get("payload").is_some_and(Value::is_string) {
                return Err(OpError::new(format!(
                    "QTIMER op {i}: payload (base64) is required"
                )));
            }
        }
    }

    // One (queue, timerKey) at most once per call (025: load-bearing there
    // for the lock order; here it keeps each op's verdict independent of the
    // others, which is what lets the verdicts be decided before any is folded).
    let mut seen: HashSet<(String, String)> = HashSet::with_capacity(ops.len());
    for v in ops {
        let o = v.as_object().expect("validated above");
        let k = (
            jtext(o, "queue").unwrap_or_default(),
            jtext(o, "timerKey").unwrap_or_default(),
        );
        if !seen.insert(k) {
            return Err(OpError {
                message: "QTIMER a (queue, timerKey) appears more than once in one call".into(),
                hint: Some(DUP_HINT),
            });
        }
    }

    // ---------------------------------------------------------------- BUILD
    let mut out: Vec<TimerOp> = Vec::with_capacity(ops.len());
    for (i, v) in ops.iter().enumerate() {
        let o = v.as_object().expect("validated above");
        let queue = jtext(o, "queue").unwrap_or_default();
        let key = jtext(o, "timerKey").unwrap_or_default();
        if o.get("op").and_then(Value::as_str) == Some("cancel") {
            out.push(TimerOp::Cancel {
                queue,
                key,
                txn: jtext(o, "txn"),
            });
            continue;
        }
        let partition = jtext(o, "partition").unwrap_or_else(|| DEFAULT_PARTITION.to_string());
        let delay_ms = o.get("delayMs").and_then(Value::as_f64).unwrap_or(0.0);
        // `make_interval(secs => delayMs / 1000.0)`: µs resolution. `as`
        // saturates, so an absurd delay cannot wrap (the edge bounds it by the
        // horizon anyway).
        let delay_us = (delay_ms * 1000.0).round() as i64;
        let txn = jtext(o, "txn").unwrap_or_default();
        // The frame codec stores the txn behind a u16 (frames.rs).
        if txn.len() > u16::MAX as usize {
            return Err(OpError::new(format!(
                "QTIMER op {i}: txn exceeds the {}-byte limit",
                u16::MAX
            )));
        }
        let raw = match base64::engine::general_purpose::STANDARD
            .decode(o.get("payload").and_then(Value::as_str).unwrap_or_default())
        {
            Ok(b) => b,
            Err(e) => {
                return Err(OpError::new(format!(
                    "QTIMER op {i}: payload is not valid base64: {e}"
                )))
            }
        };
        let zstd = jbool(o, "payloadZstd", i)?;
        let encrypted = jbool(o, "encrypted", i)?;
        // 025's fire decompresses a flagged payload before packing
        // (`sweeper.rs` `group_and_pack`), a malformed one to nothing.
        let payload = if zstd {
            crate::frames::zstd_decompress(&raw)
        } else {
            raw
        };
        let message_id = o
            .get("_messageId")
            .and_then(Value::as_str)
            .and_then(crate::frames::uuid_string_to_bytes)
            .unwrap_or_else(crate::util::uuidv7_bytes);
        let psub = producer_sub.filter(|s| !s.is_empty());
        if psub.is_some_and(|s| s.len() > u16::MAX as usize) {
            return Err(OpError::new(format!(
                "QTIMER op {i}: the producer subject exceeds the frame limit"
            )));
        }
        let frame = crate::frames::pack_frames(&[crate::frames::FrameIn {
            message_id,
            txn: &txn,
            // No trace on a fired timer: the schedule's trace context is not
            // the delivery's (sweeper.rs `pack_one`).
            trace_id: None,
            producer_sub: psub,
            payload: &payload,
            encrypted,
        }]);
        out.push(TimerOp::Schedule(TimerSchedule {
            queue,
            key,
            partition,
            delay_us,
            txn,
            message_id,
            frame,
            payload_zstd: false,
            encrypted,
            producer_sub: psub.map(str::to_string),
        }));
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// The fire step's configuration and report
// ---------------------------------------------------------------------------

/// The bounds and the backoff of the leader's fire step. Node-local knobs that
/// decide WHEN and HOW MUCH is planned, never what apply does with it (the
/// effects carry every decided value).
#[derive(Clone, Debug)]
pub struct TimerFireConfig {
    /// `QUEEN_RAFT_TIMER_FIRE_BATCH`: at most this many timers per step.
    pub batch: usize,
    /// `QUEEN_RAFT_TIMER_FIRE_MAX_BYTES`: at most this many frame bytes per
    /// step (at least one timer always goes, so an oversized one still makes
    /// progress — 025's `QUEEN_SWEEPER_MAX_FIRE_BYTES` rule).
    pub max_bytes: usize,
    /// How many committed due-index entries one step may walk.
    pub scan_limit: usize,
    /// `QUEEN_SWEEPER_BACKOFF_MIN_MS` / `_MAX_MS`: a PERMANENT failure backs
    /// off `min(min * 2^attempts, max)` (025 `fail_v1`, sweeper.rs).
    pub backoff_min_ms: i64,
    pub backoff_max_ms: i64,
    /// `QUEEN_SWEEPER_TRANSIENT_BACKOFF_MS`: a TRANSIENT failure backs off
    /// this long and spends no attempt (infrastructure never consumes the DLQ
    /// budget, 025 §4.5).
    pub transient_backoff_ms: i64,
    /// `QUEEN_SWEEPER_MAX_ATTEMPTS`: past it the timer is dead-lettered into
    /// its destination queue's DLQ (`__timer__`, offset −1).
    pub max_attempts: i32,
    /// TEST ONLY: every fire of a timer targeting this queue fails PERMANENTLY,
    /// so the backoff and DLQ paths can be driven end to end (the RSM fire has
    /// no failure a well-formed timer can provoke on purpose).
    #[cfg(test)]
    pub fail_queue: Option<String>,
    /// TEST ONLY: the injected failure is TRANSIENT (retryable) instead.
    #[cfg(test)]
    pub fail_transient: bool,
}

impl Default for TimerFireConfig {
    fn default() -> TimerFireConfig {
        TimerFireConfig {
            batch: 256,
            max_bytes: 4 << 20,
            scan_limit: 4096,
            backoff_min_ms: 1000,
            backoff_max_ms: 60_000,
            transient_backoff_ms: 1000,
            max_attempts: 5,
            #[cfg(test)]
            fail_queue: None,
            #[cfg(test)]
            fail_transient: false,
        }
    }
}

impl TimerFireConfig {
    /// Resolve from the environment, ONCE at boot. The backoff knobs keep the
    /// postgres sweeper's names so one configuration means the same thing on
    /// both storage classes.
    pub fn from_env() -> TimerFireConfig {
        fn num(name: &str, cur: i64) -> i64 {
            std::env::var(name)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(cur)
        }
        let d = TimerFireConfig::default();
        TimerFireConfig {
            batch: num("QUEEN_RAFT_TIMER_FIRE_BATCH", d.batch as i64) as usize,
            max_bytes: num("QUEEN_RAFT_TIMER_FIRE_MAX_BYTES", d.max_bytes as i64) as usize,
            scan_limit: d.scan_limit,
            backoff_min_ms: num("QUEEN_SWEEPER_BACKOFF_MIN_MS", d.backoff_min_ms),
            backoff_max_ms: num("QUEEN_SWEEPER_BACKOFF_MAX_MS", d.backoff_max_ms),
            transient_backoff_ms: num("QUEEN_SWEEPER_TRANSIENT_BACKOFF_MS", d.transient_backoff_ms),
            max_attempts: num("QUEEN_SWEEPER_MAX_ATTEMPTS", d.max_attempts as i64) as i32,
            #[cfg(test)]
            fail_queue: None,
            #[cfg(test)]
            fail_transient: false,
        }
    }
}

/// What one fire step decided (tests and logs; nothing is recorded).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FireReport {
    /// Timers whose message was appended.
    pub fired: usize,
    /// Timers whose `txn` was already in the destination's dedup window: done,
    /// deleted, nothing appended (025's duplicate arm).
    pub duplicates: usize,
    /// Timers that failed and were pushed out by a backoff.
    pub backed_off: usize,
    /// Timers that exhausted their attempts and were dead-lettered.
    pub dead_lettered: usize,
    /// The step hit one of its bounds: more timers are due NOW, so the driver
    /// should run another step at once instead of waiting for the next tick.
    pub more: bool,
}

// ---------------------------------------------------------------------------
// The planner
// ---------------------------------------------------------------------------

type TimerKey = (String, String, String);

impl<'a, R: Reads + ?Sized> Planner<'a, R> {
    /// One timer as committed state plus the overlay see it.
    pub(crate) fn timer_row(
        &self,
        ov: &Overlay,
        tenant: &str,
        queue: &str,
        key: &str,
    ) -> Result<Option<TimerRow>, Refusal> {
        let tk = (tenant.to_string(), queue.to_string(), key.to_string());
        match ov.timers.get(&tk) {
            Some(TimerOverlay::Row(r)) => Ok(Some(r.clone())),
            Some(TimerOverlay::Deleted) => Ok(None),
            Some(TimerOverlay::Backoff {
                visible_at_us,
                attempts,
                last_error,
                updated_at_us,
            }) => Ok(self
                .reads()
                .timer(tenant, queue, key)
                .map_err(store_err)?
                .map(|mut r| {
                    r.visible_at_us = Some(*visible_at_us);
                    r.attempts = *attempts;
                    r.last_error = last_error.clone();
                    r.updated_at_us = *updated_at_us;
                    r
                })),
            None => self.reads().timer(tenant, queue, key).map_err(store_err),
        }
    }

    /// Plan a list of timer schedules/cancels (025 `log_timers_apply_v1`)
    /// against `ov` — THE REUSABLE HELPER: `plan_timers` is one caller, and a
    /// TRANSACTION command that carries a `timers` array (005's wire calls the
    /// same SP) is meant to be another, planning its timer ops into the same
    /// entry as its pushes and acks.
    ///
    /// Returns the effects, in op order, and the verdicts, index-aligned with
    /// `ops`. On `Ok` the effects are ALREADY FOLDED into `ov` (a later command
    /// of the cycle — or a later leg of the same transaction — sees them); on
    /// `Err` the overlay is untouched: every read happens before the first fold.
    /// An `Ok` with no effects is legal (every op was an `absent` cancel).
    ///
    /// The ops must be the receiver-built ones ([`parse_timer_ops`]); the
    /// one-key-per-call rule is re-checked here because a caller other than the
    /// HTTP receiver can reach this function.
    pub fn plan_timer_ops(
        &self,
        ov: &mut Overlay,
        tenant: &str,
        ops: &[TimerOp],
    ) -> Result<(Vec<Effect>, Vec<TimerOpResult>), Refusal> {
        let mut keys: HashSet<(&str, &str)> = HashSet::with_capacity(ops.len());
        for op in ops {
            if !keys.insert((op.queue(), op.key())) {
                return Err(Refusal::client(
                    "timers_bad_request",
                    "QTIMER a (queue, timerKey) appears more than once in one call",
                ));
            }
        }
        // Every read first: nothing below may fail once the overlay moves.
        let mut current: Vec<Option<TimerRow>> = Vec::with_capacity(ops.len());
        for op in ops {
            current.push(self.timer_row(ov, tenant, op.queue(), op.key())?);
        }

        let now = self.now_us;
        let mut effects: Vec<Effect> = Vec::with_capacity(ops.len());
        let mut results: Vec<TimerOpResult> = Vec::with_capacity(ops.len());
        for (op, cur) in ops.iter().zip(current) {
            match op {
                TimerOp::Cancel { queue, key, txn } => match cur {
                    Some(row) => {
                        effects.push(Effect::TimerDelete {
                            tenant: tenant.to_string(),
                            queue: queue.clone(),
                            key: key.clone(),
                        });
                        results.push(TimerOpResult::Cancelled {
                            queue: queue.clone(),
                            key: key.clone(),
                            txn: row.txn,
                        });
                    }
                    None => results.push(TimerOpResult::Absent {
                        queue: queue.clone(),
                        key: key.clone(),
                        txn: txn.clone(),
                    }),
                },
                TimerOp::Schedule(s) => {
                    let deliver_at_us = now.saturating_add(s.delay_us);
                    // A reschedule is a NEW timer under an OLD name: attempts
                    // back to 0, last_error cleared, txn and message id
                    // overwritten (025 §20.2). `created_at` is the one column
                    // the upsert keeps.
                    let row = TimerRow {
                        partition: s.partition.clone(),
                        deliver_at_us,
                        visible_at_us: None,
                        frame: s.frame.clone(),
                        payload_zstd: s.payload_zstd,
                        encrypted: s.encrypted,
                        txn: s.txn.clone(),
                        message_id: s.message_id,
                        attempts: 0,
                        last_error: None,
                        producer_sub: s.producer_sub.clone(),
                        created_at_us: cur.as_ref().map_or(now, |r| r.created_at_us),
                        updated_at_us: now,
                    };
                    effects.push(Effect::TimerUpsert {
                        tenant: tenant.to_string(),
                        queue: s.queue.clone(),
                        key: s.key.clone(),
                        row,
                    });
                    results.push(TimerOpResult::Scheduled {
                        queue: s.queue.clone(),
                        key: s.key.clone(),
                        txn: s.txn.clone(),
                        message_id: s.message_id,
                        deliver_at_us,
                        rescheduled: cur.is_some(),
                    });
                }
            }
        }
        ov.apply_effects(&effects);
        Ok((effects, results))
    }

    /// Plan one timers command: the §5.1 size gate, then
    /// [`Planner::plan_timer_ops`]. A call that changes nothing (every op an
    /// `absent` cancel) is `Plan::Empty` — answered, never logged (§5.4).
    pub fn plan_timers(&self, ov: &mut Overlay, cmd: &TimersCommand) -> Planned {
        let planned: usize = cmd.ops.iter().map(TimerOp::size_hint).sum::<usize>() + 256;
        if planned > self.cfg.entry_max_bytes {
            return Err(Refusal::client(
                "too_large",
                format!(
                    "planned timers call of {planned} B exceeds QUEEN_RAFT_ENTRY_MAX_BYTES ({})",
                    self.cfg.entry_max_bytes
                ),
            ));
        }
        let (effects, results) = self.plan_timer_ops(ov, &cmd.tenant, &cmd.ops)?;
        let outcome = timers_outcome(&results)?;
        if effects.is_empty() {
            Ok(Plan::Empty(outcome))
        } else {
            Ok(Plan::logged(effects, outcome))
        }
    }

    /// The leader's fire step (025 due + claim + fire + fail + dlq, as ONE
    /// planning step): every timer due at `now_us` — committed state merged
    /// with the overlay, earliest first, bounded by `cfg` — becomes its
    /// destination push (through [`Planner::plan_push`]) plus its
    /// `TimerDelete`, or its `TimerBackoff` / dead letter when the push is
    /// refused. The caller adds the returned effects to the entry as ONE
    /// command with a minted request id, answered by nobody (§10.1, like the
    /// request-id expiry step). Every effect is already folded into `ov`.
    ///
    /// `Err` only when the candidate read itself fails, before anything is
    /// folded; a refusal inside one push is that push's timers' failure, never
    /// the step's.
    pub fn plan_timer_fire(
        &self,
        ov: &mut Overlay,
        cfg: &TimerFireConfig,
    ) -> Result<(Vec<Effect>, FireReport), Refusal> {
        let now = self.now_us;
        let batch = cfg.batch.max(1);
        let mut report = FireReport::default();

        // (1) The due candidates: the committed fire-order index from its
        // front, merged with the overlay — the overlay's view of a key wins.
        // At most `batch` committed keys are taken (the union's first `batch`
        // cannot need more of them), so a backlog of due timers costs this step
        // O(batch) row reads, not O(backlog); the keys the overlay owns (a fire
        // still in flight) are stepped over without counting, up to the scan
        // bound.
        let scan_limit = cfg.scan_limit.max(batch);
        let mut due_keys: Vec<TimerKey> = Vec::new();
        let mut walked = 0usize;
        let overlay_timers = &ov.timers;
        self.reads()
            .scan_timers_due(scan_limit, &mut |due, t, q, k| {
                if due > now {
                    return false;
                }
                walked += 1;
                let tk = (t.to_string(), q.to_string(), k.to_string());
                if !overlay_timers.contains_key(&tk) {
                    due_keys.push(tk);
                }
                due_keys.len() < batch
            })
            .map_err(store_err)?;
        // Either bound reached: more may be due right now.
        let scan_capped = walked >= scan_limit || due_keys.len() >= batch;

        let mut cands: BTreeMap<(i64, String, String, String), TimerRow> = BTreeMap::new();
        let mut seen: HashSet<TimerKey> = HashSet::with_capacity(due_keys.len());
        for tk in due_keys {
            if !seen.insert(tk.clone()) {
                continue;
            }
            let Some(row) = self.reads().timer(&tk.0, &tk.1, &tk.2).map_err(store_err)? else {
                continue;
            };
            let due = timer_due_us(&row);
            if due <= now {
                cands.insert((due, tk.0, tk.1, tk.2), row);
            }
        }
        // The overlay's own view: only a due row is cloned (an in-flight
        // schedule can carry a large frame and is usually not due yet).
        for (tk, st) in ov.timers.iter() {
            let row = match st {
                TimerOverlay::Deleted => None,
                TimerOverlay::Row(r) => (timer_due_us(r) <= now).then(|| r.clone()),
                // due = max(visible_at, deliver_at) >= visible_at.
                TimerOverlay::Backoff { visible_at_us, .. } if *visible_at_us > now => None,
                TimerOverlay::Backoff { .. } => self.timer_row(ov, &tk.0, &tk.1, &tk.2)?,
            };
            if let Some(row) = row {
                let due = timer_due_us(&row);
                if due <= now {
                    cands.insert((due, tk.0.clone(), tk.1.clone(), tk.2.clone()), row);
                }
            }
        }

        // (2) The bounded selection, in due order.
        let mut chosen: Vec<(TimerKey, TimerRow)> = Vec::new();
        let mut bytes = 0usize;
        let mut more = scan_capped;
        for ((_, t, q, k), row) in cands {
            if chosen.len() >= batch
                || (!chosen.is_empty() && bytes + row.frame.len() > cfg.max_bytes)
            {
                more = true;
                break;
            }
            bytes += row.frame.len();
            chosen.push(((t, q, k), row));
        }
        report.more = more;
        if chosen.is_empty() {
            return Ok((Vec::new(), report));
        }

        // (3) GROUP BY (tenant, queue, partition) — never by (queue,
        // partition): a tenant-blind grouping would fuse two tenants' timers
        // into one push (025 §6.2 point 8). Due order inside a group, groups
        // in the order their first timer came due.
        let mut order: Vec<TimerKey> = Vec::new();
        let mut groups: HashMap<TimerKey, Vec<(String, TimerRow)>> = HashMap::new();
        for ((t, q, k), row) in chosen {
            let g = (t, q, row.partition.clone());
            match groups.get_mut(&g) {
                Some(v) => v.push((k, row)),
                None => {
                    order.push(g.clone());
                    groups.insert(g, vec![(k, row)]);
                }
            }
        }

        // (4) One push per run of DISTINCT txns: the push planner probes only
        // what came before a command, so two frames sharing a txn in one
        // command would both survive. Splitting there makes the batch plan
        // exactly as one push per timer would.
        let mut effects: Vec<Effect> = Vec::new();
        for g in order {
            let rows = groups.remove(&g).unwrap_or_default();
            let (tenant, queue, partition) = g;
            let mut run: Vec<(String, TimerRow, [u8; 16])> = Vec::new();
            let mut run_hashes: HashSet<[u8; 16]> = HashSet::new();
            for (key, row) in rows {
                let h = crate::util::txn_hash128(&row.txn);
                if !run_hashes.insert(h) {
                    self.fire_run(
                        ov,
                        cfg,
                        &tenant,
                        &queue,
                        &partition,
                        std::mem::take(&mut run),
                        &mut effects,
                        &mut report,
                    );
                    run_hashes.clear();
                    run_hashes.insert(h);
                }
                run.push((key, row, h));
            }
            if !run.is_empty() {
                self.fire_run(
                    ov,
                    cfg,
                    &tenant,
                    &queue,
                    &partition,
                    run,
                    &mut effects,
                    &mut report,
                );
            }
        }
        Ok((effects, report))
    }

    /// Fire one run of timers (same destination, distinct txns) as one push.
    #[allow(clippy::too_many_arguments)]
    fn fire_run(
        &self,
        ov: &mut Overlay,
        cfg: &TimerFireConfig,
        tenant: &str,
        queue: &str,
        partition: &str,
        run: Vec<(String, TimerRow, [u8; 16])>,
        effects: &mut Vec<Effect>,
        report: &mut FireReport,
    ) {
        #[cfg(test)]
        if cfg.fail_queue.as_deref() == Some(queue) {
            let r = if cfg.fail_transient {
                Refusal::retry("timer_fire_injected", "injected transient fire failure")
            } else {
                Refusal::client("timer_fire_injected", "injected permanent fire failure")
            };
            for (key, row, _) in &run {
                self.fire_failed(ov, cfg, tenant, queue, key, row, &r, effects, report);
            }
            return;
        }

        let cmd = PushCommand {
            // Never looked up: the fire step's request id is minted per entry
            // by the caller, and `plan_push` does not read this one.
            request_id: [0u8; 16],
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            partition: partition.to_string(),
            items: run
                .iter()
                .map(|(_, row, h)| PushItem {
                    hash: *h,
                    frame: row.frame.clone(),
                })
                .collect(),
            create_cfg: implicit_queue_config(),
        };
        let verdicts: Result<Vec<PushVerdict>, Refusal> = match self.plan_push(ov, &cmd) {
            Ok(Plan::Logged {
                effects: pushed,
                outcome: Outcome::Push(p),
            }) => {
                effects.extend(pushed);
                Ok(p.items)
            }
            Ok(Plan::Empty(Outcome::Push(p))) => Ok(p.items),
            Ok(Plan::Refused(r)) | Err(r) => Err(r),
            Ok(other) => Err(Refusal::retry(
                "internal",
                format!("timer fire: unexpected push plan {other:?}"),
            )),
        };
        match verdicts {
            Ok(items) => {
                for (i, (key, row, _)) in run.iter().enumerate() {
                    match items.get(i) {
                        Some(PushVerdict::Created { .. }) => {
                            report.fired += 1;
                            timer_delete(ov, tenant, queue, key, effects);
                        }
                        Some(PushVerdict::Duplicate { .. }) => {
                            report.duplicates += 1;
                            timer_delete(ov, tenant, queue, key, effects);
                        }
                        Some(PushVerdict::Refused { code, message }) => {
                            let r = Refusal::client(code.clone(), message.clone());
                            self.fire_failed(ov, cfg, tenant, queue, key, row, &r, effects, report);
                        }
                        None => {
                            let r =
                                Refusal::retry("internal", "timer fire: a push verdict is missing");
                            self.fire_failed(ov, cfg, tenant, queue, key, row, &r, effects, report);
                        }
                    }
                }
            }
            Err(r) => {
                for (key, row, _) in &run {
                    self.fire_failed(ov, cfg, tenant, queue, key, row, &r, effects, report);
                }
            }
        }
    }

    /// A timer's fire failed (025 `log_timers_fail_v1` + `log_timers_dlq_v1`):
    /// back it off — the row stays pending and CANCELLABLE — or, once a
    /// permanent failure exhausts its attempts, dead-letter it.
    #[allow(clippy::too_many_arguments)]
    fn fire_failed(
        &self,
        ov: &mut Overlay,
        cfg: &TimerFireConfig,
        tenant: &str,
        queue: &str,
        key: &str,
        row: &TimerRow,
        r: &Refusal,
        effects: &mut Vec<Effect>,
        report: &mut FireReport,
    ) {
        let now = self.now_us;
        let transient = r.retryable;
        // PERMANENT failures only spend budget (025 §4.5).
        let attempts = if transient {
            row.attempts
        } else {
            row.attempts.saturating_add(1)
        };
        let error = format!("{}: {}", r.code, r.message);
        if !transient && attempts >= cfg.max_attempts.max(1) {
            if let Ok(dead) = self.timer_dead_letter(ov, tenant, queue, key, row, &error, attempts)
            {
                report.dead_lettered += 1;
                effects.extend(dead);
                return;
            }
            // The dead letter itself could not be planned (a store read
            // failed): fall through to a backoff, and the next attempt tries
            // the archive again — nothing is lost.
        }
        // `min(min_ms * 2^attempts, max_ms)` on the attempts BEFORE this
        // failure, exactly sweeper.rs's `fail_batch`.
        let backoff_ms = if transient {
            cfg.transient_backoff_ms
        } else {
            let shift = row.attempts.clamp(0, 30) as u32;
            cfg.backoff_min_ms
                .saturating_mul(1i64 << shift)
                .min(cfg.backoff_max_ms)
        };
        let e = Effect::TimerBackoff {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            key: key.to_string(),
            visible_at_us: now.saturating_add(backoff_ms.max(0).saturating_mul(1000)),
            attempts,
            last_error: Some(error),
            updated_at_us: now,
        };
        ov.apply_effects(std::slice::from_ref(&e));
        effects.push(e);
        report.backed_off += 1;
    }

    /// 025 `log_timers_dlq_v1`: provision the destination queue and partition
    /// when missing (a dead letter on a missing partition is unfindable), file
    /// the dead letter under `__timer__` at offset −1 with a JSON snapshot of
    /// the payload, and delete the timer — all in the fire's entry.
    #[allow(clippy::too_many_arguments)]
    fn timer_dead_letter(
        &self,
        ov: &mut Overlay,
        tenant: &str,
        queue: &str,
        key: &str,
        row: &TimerRow,
        error: &str,
        attempts: i32,
    ) -> Result<Vec<Effect>, Refusal> {
        let now = self.now_us;
        let mut effs: Vec<Effect> = Vec::with_capacity(4);
        let pid = match self.pid_of(ov, tenant, queue, &row.partition)? {
            Some(pid) => pid,
            None => {
                if self.queue_cfg(ov, tenant, queue)?.is_none() {
                    let mut cfg = implicit_queue_config();
                    cfg.created_at_us = now;
                    effs.push(Effect::QueueUpsert {
                        tenant: tenant.to_string(),
                        queue: queue.to_string(),
                        cfg,
                    });
                }
                let pid = ov.peek_pid();
                effs.push(Effect::PartitionCreate {
                    pid,
                    uuid: crate::util::uuidv7_bytes(),
                    tenant: tenant.to_string(),
                    queue: queue.to_string(),
                    partition: row.partition.clone(),
                    created_at_us: now,
                });
                pid
            }
        };
        effs.push(Effect::DlqInsert {
            dlq_id: crate::util::uuidv7_bytes(),
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            pid,
            group: TIMER_DLQ_GROUP.to_string(),
            offset: -1,
            message_id: Some(row.message_id),
            txn: row.txn.clone(),
            payload: dlq_snapshot(&row.frame),
            error: error.to_string(),
            retry_count: attempts.max(0) as u32,
            failed_at_us: now,
        });
        effs.push(Effect::TimerDelete {
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            key: key.to_string(),
        });
        ov.apply_effects(&effs);
        Ok(effs)
    }
}

fn timer_delete(ov: &mut Overlay, tenant: &str, queue: &str, key: &str, effects: &mut Vec<Effect>) {
    let e = Effect::TimerDelete {
        tenant: tenant.to_string(),
        queue: queue.to_string(),
        key: key.to_string(),
    };
    ov.apply_effects(std::slice::from_ref(&e));
    effects.push(e);
}

/// The payload a timer would have delivered, from its frame.
pub fn frame_payload(frame: &[u8]) -> Vec<u8> {
    crate::frames::unpack_frames_ref(frame)
        .and_then(|f| f.first().map(|fr| fr.payload.to_vec()))
        .unwrap_or_default()
}

/// The DLQ snapshot (sweeper.rs `dlq`): the payload as JSON when it is JSON,
/// else `{"_raw_b64": …}` — the DLQ is the last resort and must not have a
/// branch that loses the message.
fn dlq_snapshot(frame: &[u8]) -> Vec<u8> {
    let payload = frame_payload(frame);
    match std::str::from_utf8(&payload) {
        Ok(s) if serde_json::from_str::<Value>(s).is_ok() => payload,
        _ => {
            let b64 = base64::engine::general_purpose::STANDARD.encode(&payload);
            let mut m = Map::new();
            m.insert("_raw_b64".into(), Value::String(b64));
            Value::Object(m).to_string().into_bytes()
        }
    }
}

/// The configuration an implicitly created queue gets (003 first contact and
/// 025's fire provisioning): the `queen.queues` DDL defaults. The planner
/// stamps `created_at_us`; the id is minted here, fresh per creation.
pub fn implicit_queue_config() -> QueueConfig {
    QueueConfig {
        id: crate::util::uuidv7_bytes(),
        namespace: None,
        task: None,
        priority: 0,
        lease_time: 60,
        retry_limit: 3,
        retry_delay: 1000,
        ttl: 3600,
        dead_letter_queue: true,
        dlq_after_max_retries: true,
        delayed_processing: 0,
        window_buffer: 0,
        retention_seconds: 0,
        completed_retention_seconds: 0,
        retention_enabled: false,
        encryption_enabled: false,
        max_wait_time_seconds: 0,
        max_queue_size: 0,
        min_pop_wait_time: 0,
        dedup_window_seconds: 3600,
        retention_sink_hold: String::new(),
        retention_sink_hold_max_seconds: 0,
        created_at_us: 0,
    }
}

// ---------------------------------------------------------------------------
// Read rendering (peek / list / count), shared by the facade
// ---------------------------------------------------------------------------

/// 025's `to_char(ts AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`:
/// microsecond precision, UTC.
pub fn iso_us(us: i64) -> String {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}Z",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// Howard Hinnant's `civil_from_days` (days since 1970-01-01 → y/m/d).
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m: i64 = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m as u32, d)
}

/// The columns peek and list share (025 `peek_v1` / `list_v1`). `claimed` is
/// always false: nothing here ever holds a timer (see the module header).
fn row_fields(m: &mut Map<String, Value>, queue: &str, key: &str, row: &TimerRow) {
    m.insert("queue".into(), Value::String(queue.to_string()));
    m.insert("timerKey".into(), Value::String(key.to_string()));
    m.insert("partition".into(), Value::String(row.partition.clone()));
    m.insert("deliverAt".into(), Value::String(iso_us(row.deliver_at_us)));
    m.insert("txn".into(), Value::String(row.txn.clone()));
    m.insert(
        "messageId".into(),
        Value::String(crate::frames::uuid_bytes_to_string(&row.message_id)),
    );
    m.insert("payloadZstd".into(), Value::Bool(row.payload_zstd));
    m.insert("encrypted".into(), Value::Bool(row.encrypted));
    m.insert(
        "producerSub".into(),
        row.producer_sub
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert("attempts".into(), Value::from(row.attempts));
    m.insert(
        "lastError".into(),
        row.last_error
            .clone()
            .map(Value::String)
            .unwrap_or(Value::Null),
    );
    m.insert("claimed".into(), Value::Bool(false));
    m.insert("createdAt".into(), Value::String(iso_us(row.created_at_us)));
    m.insert("updatedAt".into(), Value::String(iso_us(row.updated_at_us)));
}

/// 025 `log_timers_peek_v1`: one key WITH the payload (base64, exactly as
/// stored), or `{"found":false,…}` — a key of another tenant reads exactly like
/// a key that does not exist.
pub fn peek_json(queue: &str, key: &str, row: Option<&TimerRow>) -> Value {
    let mut m = Map::new();
    match row {
        Some(row) => {
            m.insert("found".into(), Value::Bool(true));
            row_fields(&mut m, queue, key, row);
            m.insert(
                "payload".into(),
                Value::String(
                    base64::engine::general_purpose::STANDARD.encode(frame_payload(&row.frame)),
                ),
            );
        }
        None => {
            m.insert("found".into(), Value::Bool(false));
            m.insert("queue".into(), Value::String(queue.to_string()));
            m.insert("timerKey".into(), Value::String(key.to_string()));
        }
    }
    Value::Object(m)
}

/// One row of 025 `log_timers_list_v1` (no payload: a list is never an
/// unbounded read whose cost the caller does not fix).
pub fn list_row_json(queue: &str, key: &str, row: &TimerRow) -> Value {
    let mut m = Map::new();
    row_fields(&mut m, queue, key, row);
    Value::Object(m)
}

/// 025's list clamp: default 100, `[1, 1000]`, never an error.
pub fn list_limit(limit: i32) -> usize {
    limit.clamp(1, 1000) as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_us_matches_to_char_us() {
        assert_eq!(iso_us(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(iso_us(1_768_000_000_123_456), "2026-01-09T23:06:40.123456Z");
        assert_eq!(iso_us(-1), "1969-12-31T23:59:59.999999Z");
    }

    fn op(v: Value) -> Vec<Value> {
        vec![v]
    }

    #[test]
    fn the_025_validation_order_and_messages_hold() {
        let e = parse_timer_ops(&[Value::from(3)], None).unwrap_err();
        assert_eq!(e.message, "QTIMER op 0 is not an object");
        let e = parse_timer_ops(&op(serde_json::json!({"op":"fire"})), None).unwrap_err();
        assert_eq!(e.message, "QTIMER op 0: unknown operation");
        let e = parse_timer_ops(
            &op(serde_json::json!({"op":"cancel","queue":"q","timerKey":"k","producerSub":"x"})),
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("field producerSub is server-owned"));
        assert!(e.hint.is_some());
        let e = parse_timer_ops(&op(serde_json::json!({"op":"cancel","timerKey":"k"})), None)
            .unwrap_err();
        assert_eq!(e.message, "QTIMER op 0: queue is required");
        let e = parse_timer_ops(
            &op(serde_json::json!({"op":"schedule","queue":"q","timerKey":"k","txn":"t","payload":"e30="})),
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("delayMs"));
        let e = parse_timer_ops(
            &op(serde_json::json!({"op":"schedule","queue":"q","timerKey":"k","delayMs":1,"payload":"e30="})),
            None,
        )
        .unwrap_err();
        assert_eq!(e.message, "QTIMER op 0: txn is required");
        let e = parse_timer_ops(
            &op(serde_json::json!({"op":"schedule","queue":"q","timerKey":"k","delayMs":1,"txn":"t","partition":""})),
            None,
        )
        .unwrap_err();
        assert!(e.message.contains("partition, when present"));
        let dup = vec![
            serde_json::json!({"op":"cancel","queue":"q","timerKey":"k"}),
            serde_json::json!({"op":"schedule","queue":"q","timerKey":"k","delayMs":1,"txn":"t","payload":"e30="}),
        ];
        let e = parse_timer_ops(&dup, None).unwrap_err();
        assert!(e.message.contains("more than once"));
    }

    #[test]
    fn a_schedule_packs_the_frame_the_fire_will_append() {
        let mid = "0191e1a2-0000-7000-8000-000000000001";
        let ops = parse_timer_ops(
            &op(serde_json::json!({
                "op":"schedule","queue":"q","timerKey":"k","delayMs":250.5,
                "txn":"tx-1","payload":"eyJhIjoxfQ==","_messageId": mid
            })),
            Some("svc-a"),
        )
        .unwrap();
        let TimerOp::Schedule(s) = &ops[0] else {
            panic!("a schedule")
        };
        assert_eq!(s.delay_us, 250_500);
        assert_eq!(s.partition, DEFAULT_PARTITION);
        assert_eq!(crate::frames::uuid_bytes_to_string(&s.message_id), mid);
        let frames = crate::frames::unpack_frames_ref(&s.frame).expect("one frame");
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload, br#"{"a":1}"#);
        assert_eq!(frames[0].txn, "tx-1");
        assert_eq!(frames[0].producer_sub, Some("svc-a"));
        assert_eq!(frames[0].trace_id, None);
    }

    #[test]
    fn a_zstd_payload_is_decompressed_before_packing() {
        let plain = br#"{"big":"payload"}"#;
        let z = crate::frames::zstd_compress(plain, 3);
        let b64 = base64::engine::general_purpose::STANDARD.encode(&z);
        let ops = parse_timer_ops(
            &op(serde_json::json!({
                "op":"schedule","queue":"q","timerKey":"k","delayMs":0,
                "txn":"t","payload": b64, "payloadZstd": true
            })),
            None,
        )
        .unwrap();
        let TimerOp::Schedule(s) = &ops[0] else {
            panic!("a schedule")
        };
        assert!(!s.payload_zstd, "the stored frame is plain");
        assert_eq!(frame_payload(&s.frame), plain);
    }

    #[test]
    fn the_outcome_round_trips_the_results() {
        let r = vec![
            TimerOpResult::Scheduled {
                queue: "q".into(),
                key: "k".into(),
                txn: "t".into(),
                message_id: [1u8; 16],
                deliver_at_us: 1_000_000,
                rescheduled: true,
            },
            TimerOpResult::Absent {
                queue: "q".into(),
                key: "gone".into(),
                txn: None,
            },
        ];
        let o = timers_outcome(&r).unwrap();
        let back = Outcome::decode(&o.encode()).unwrap();
        let v = timers_results(&back).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0]["status"], "rescheduled");
        assert_eq!(v[0]["deliverAt"], "1970-01-01T00:00:01.000000Z");
        assert_eq!(v[1]["ok"], false);
        assert_eq!(v[1]["txn"], Value::Null);
    }
}
