//! status — what every queue is doing on this node, as [`crate::Sink::status`]
//! reports it.
//!
//! The engine of a queue lives inside its task and is never shared. What is
//! shared is this board: each task writes a small snapshot after every round of
//! its protocol (and at the few moments in between that an operator would ask
//! about — the claim, a commit, an error, the stop), and any thread of the
//! broker reads the whole board at any time. Every number here is a copy; none
//! of it is read back by the sink.

use std::collections::BTreeMap;
use std::sync::Mutex;

use serde_json::{json, Value};

use crate::driver::Stop;
use crate::obs::{lock, now_epoch_ms};
use crate::types::Micros;
use crate::window::{Engine, EngineState};

/// Who holds a queue's lease, as this node last learned it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum Owner {
    /// Not claimed yet, or between two claims.
    #[default]
    Unknown,
    /// This node.
    Here,
    /// Another node, by the `instance` its lease row names.
    Elsewhere(String),
}

/// One queue on this node.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueueStatus {
    pub owner: Owner,
    /// `claiming`, `held` (another node owns it), `restoring`, the engine's
    /// `filling` / `intent` / `upload` / `commit`, or how the last run of the
    /// queue ended: `drained`, `fenced`, `failed`, `missing` (the queue does
    /// not exist), `crashed` — or `released`, given back to another node.
    pub state: &'static str,
    /// The last committed window, as this node's engine knows it.
    pub committed_k: u64,
    pub t_end: Option<Micros>,
    /// The stamp the lake is complete through: `tEnd`, or later when the queue
    /// is read to the end with nothing pending
    /// ([`crate::window::Engine::complete_through`]).
    pub complete_through: Option<Micros>,
    /// This node's `safeTime`, as the engine last saw it.
    pub safe_time: Option<Micros>,
    /// `safeTime − completeThrough`; `None` while either is unknown.
    pub lag: Option<Micros>,
    /// Rebuilding a window whose intent survived a crash or a handover.
    pub redo: bool,
    pub buffered_bytes: usize,
    pub partitions: usize,
    /// Since this node started the sink, for the windows this node committed.
    pub windows_committed: u64,
    pub records: u64,
    pub bytes: u64,
    pub records_lost: u64,
    /// The last error seen for this queue, with this node's wall clock (ms).
    pub last_error: Option<(i64, String)>,
}

#[derive(Default)]
struct BucketStatus {
    /// `None` until the first probe answered.
    reachable: Option<bool>,
    error: Option<String>,
    checked_at_ms: i64,
}

/// Every queue this node has a task for, and the bucket.
#[derive(Default)]
pub struct StatusBoard {
    queues: Mutex<BTreeMap<String, QueueStatus>>,
    bucket: Mutex<BucketStatus>,
}

impl StatusBoard {
    pub fn new() -> StatusBoard {
        StatusBoard::default()
    }

    fn with(&self, queue: &str, f: impl FnOnce(&mut QueueStatus)) {
        let mut g = lock(&self.queues);
        let q = g.entry(queue.to_string()).or_insert_with(|| QueueStatus {
            state: "claiming",
            ..QueueStatus::default()
        });
        f(q);
    }

    /// A task is about to claim the queue's lease.
    pub fn claiming(&self, queue: &str) {
        self.with(queue, |q| {
            q.owner = Owner::Unknown;
            q.state = "claiming";
        });
    }

    /// This node holds the lease and is restoring the queue.
    pub fn owned(&self, queue: &str) {
        self.with(queue, |q| {
            q.owner = Owner::Here;
            q.state = "restoring";
        });
    }

    /// Another node holds the lease. What this node's engine knew of the queue
    /// is not current any more, so it is not shown.
    pub fn held_by(&self, queue: &str, instance: &str) {
        self.with(queue, |q| {
            q.owner = Owner::Elsewhere(instance.to_string());
            q.state = "held";
            q.complete_through = None;
            q.safe_time = None;
            q.lag = None;
            q.redo = false;
            q.buffered_bytes = 0;
            q.partitions = 0;
        });
    }

    /// The engine after one round of the protocol.
    pub fn engine(&self, queue: &str, e: &Engine) {
        let state = match e.state() {
            EngineState::Filling => "filling",
            EngineState::Intent(_) => "intent",
            EngineState::Upload(_) => "upload",
            EngineState::Commit(_) => "commit",
            EngineState::Failed(_) => "failed",
        };
        let safe = e.safe_time();
        let lag = safe.and_then(|s| e.lag(s));
        self.with(queue, |q| {
            q.owner = Owner::Here;
            q.state = state;
            q.committed_k = e.committed_k();
            q.t_end = e.committed_t_end();
            q.complete_through = e.complete_through();
            q.safe_time = safe;
            q.lag = lag;
            q.redo = e.redoing();
            q.buffered_bytes = e.buffered_bytes();
            q.partitions = e.tracked_partitions();
        });
    }

    /// A window committed by this node.
    pub fn committed(&self, queue: &str, records: u64, bytes: u64, lost: u64) {
        self.with(queue, |q| {
            q.windows_committed += 1;
            q.records += records;
            q.bytes += bytes;
            q.records_lost += lost;
        });
    }

    /// Something failed for this queue. Kept until the next error replaces it:
    /// a status read after a recovery still says what the last trouble was, and
    /// when.
    pub fn error(&self, queue: &str, message: impl Into<String>) {
        let message = message.into();
        self.with(queue, |q| q.last_error = Some((now_epoch_ms(), message)));
    }

    /// The queue was given back to another node ([`crate::placement`]).
    pub fn released(&self, queue: &str) {
        self.with(queue, |q| {
            q.state = "released";
            if q.owner == Owner::Here {
                q.owner = Owner::Unknown;
            }
        });
    }

    /// The queue's task stopped running the driver.
    pub fn stopped(&self, queue: &str, stop: &Stop) {
        let (state, why) = match stop {
            Stop::Drained => ("drained", None),
            Stop::Fenced(why) => ("fenced", Some(why.clone())),
            Stop::Failed(why) => ("failed", Some(why.clone())),
            Stop::Missing(why) => ("missing", Some(why.clone())),
            Stop::Crashed(at) => ("crashed", Some(format!("crash point {}", at.as_str()))),
        };
        self.with(queue, |q| {
            q.state = state;
            if q.owner == Owner::Here {
                q.owner = Owner::Unknown;
            }
            if let Some(why) = why {
                q.last_error = Some((now_epoch_ms(), why));
            }
        });
    }

    pub fn bucket_ok(&self) {
        let mut b = lock(&self.bucket);
        b.reachable = Some(true);
        b.error = None;
        b.checked_at_ms = now_epoch_ms();
    }

    pub fn bucket_error(&self, message: impl Into<String>) {
        let mut b = lock(&self.bucket);
        b.reachable = Some(false);
        b.error = Some(message.into());
        b.checked_at_ms = now_epoch_ms();
    }

    /// `None` before the first probe answered.
    pub fn bucket_reachable(&self) -> Option<bool> {
        lock(&self.bucket).reachable
    }

    /// One queue's row, for tests and for the broker's own use.
    pub fn queue(&self, queue: &str) -> Option<QueueStatus> {
        lock(&self.queues).get(queue).cloned()
    }

    /// The bucket, as JSON.
    pub fn bucket_json(&self) -> Value {
        let b = lock(&self.bucket);
        json!({
            "reachable": b.reachable,
            "error": b.error,
            "checkedAt": (b.checked_at_ms > 0).then(|| iso_ms(b.checked_at_ms)),
        })
    }

    /// Every queue, by name, as JSON.
    pub fn queues_json(&self) -> Value {
        let g = lock(&self.queues);
        Value::Array(g.iter().map(|(name, q)| queue_json(name, q)).collect())
    }
}

fn queue_json(name: &str, q: &QueueStatus) -> Value {
    json!({
        "name": name,
        "ownedHere": q.owner == Owner::Here,
        "heldBy": match &q.owner {
            Owner::Elsewhere(who) => Some(who.as_str()),
            _ => None,
        },
        "state": q.state,
        "k": q.committed_k,
        "tEnd": q.t_end.map(Micros::to_iso),
        "completeThrough": q.complete_through.map(Micros::to_iso),
        "safeTime": q.safe_time.map(Micros::to_iso),
        "lagSeconds": q.lag.map(|l| (l.0.max(0) as f64) / 1_000_000.0),
        "redo": q.redo,
        "bufferedBytes": q.buffered_bytes,
        "partitions": q.partitions,
        "windowsCommitted": q.windows_committed,
        "records": q.records,
        "bytes": q.bytes,
        "recordsLost": q.records_lost,
        "lastError": q.last_error.as_ref().map(|(at, message)| json!({
            "at": iso_ms(*at),
            "message": message,
        })),
    })
}

/// A wall-clock millisecond instant, in the broker's ISO rendering.
fn iso_ms(ms: i64) -> String {
    Micros::from_millis(ms).to_iso()
}
