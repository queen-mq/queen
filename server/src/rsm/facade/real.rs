//! `rsm/facade/real.rs` — the real state machine behind the [`Rsm`] seam
//! (PLAN_RAFT.md WP-1.7c).
//!
//! WP-1.7a routed the message path to the [`super::NotReady`] stub through a
//! builder hook ([`super::set_builder`]); this module fills that hook. One
//! [`RaftFacade`] owns the whole single-node RSM of phase 1:
//!
//! - a [`HeedStore`] (D9), opened at `<data_dir>/store`;
//! - a [`LocalReplicator`] (§12.2) over `<data_dir>/log` and `<data_dir>/seg`,
//!   which spawns WP-1.4's apply thread (the only writer, I1) and publishes the
//!   segment [`segments::Reader`] the payload reads use (§7.5, D7);
//! - a [`Batcher`] (§7.1), spawned as one tokio task, that drains commands,
//!   plans, proposes and answers each receiver once its entry is committed AND
//!   applied on this node (I4).
//!
//! # What the facade does, and where the line to the planner is
//!
//! The facade is the RECEIVER of §9.1: it does the pool-free pre-work the
//! Postgres handlers used to do against a connection — parse the wire body, mint
//! message ids, hash transaction ids (`xxh3_128`), pack one frame per message
//! (O20, the survivors' frames are concatenated by the planner, never
//! repacked), build the typed [`Command`], submit it, and render the wire answer
//! from the [`Outcome`] the batcher returns. It never plans and never writes
//! committed state; the planner (`rsm/planner`) and apply (`rsm/apply`) own
//! that. A pop's payload bytes are read from THIS node's own segment files after
//! the claim applied locally (D7).
//!
//! # Phase-1 scope and the deliberate simplifications
//!
//! This is the message path of §15's phase 1 (push, the three pops, ack, renew,
//! the DLQ handoff on the ack). It is faithful where the wire and the dedup
//! semantics are load-bearing, and it takes three documented shortcuts a later
//! WP closes, none of which the SDKs parse strictly:
//!
//! - **`partitionId` is the numeric pid** (decimal), not a uuid: the RSM has no
//!   uuid→pid index, and the ack must map the wire `partitionId` back to a pid
//!   AND to a queue, both of which the pid gives directly (`partition(pid)`).
//!   Every SDK treats `partitionId` as an opaque string (the C1/C-SQS notes in
//!   `handlers/data.rs`), so this is behaviourally transparent within a raft
//!   deployment; a uuid-shaped id waits on that index.
//! - **no producer subject / trace id / encryption on the push frame**: the
//!   dispatch does not thread the validated subject in yet, and phase-1 queues
//!   are unencrypted; the frame carries the payload and the txn only.
//! - **the forced-DLQ handoff files the row with the transaction id but WITHOUT
//!   the poison payload or message id**: the ack wire carries the `transactionId`
//!   verbatim, so the receiver stamps it onto the [`DlqSnapshot`] and the dead
//!   letter is identifiable (its `txn` is set, never blank). The full O20
//!   pre-read — resolving the txn hash to its committed offset to read the frame
//!   bytes and the message id off this node's own files — is the ack-registry's
//!   job (a later WP); until then `payload` is empty and `message_id` is `None`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::value::RawValue;

use crate::frames::{pack_frames, unpack_frames_ref, uuid_bytes_to_string, FrameIn};
use crate::notify::Notifier;
use crate::rsm::apply::SystemClock;
use crate::rsm::batcher::{Batcher, BatcherConfig, Command, CommandTx, Reply, Submission};
use crate::rsm::effect::{Pid, QueueConfig};
use crate::rsm::entry::{Outcome, PopClaim, PushVerdict, RequestId};
use crate::rsm::planner::{
    bucket_of, AckCommand, AckItem, AckStatus, AckTarget, DlqSnapshot, PopCommand, PushCommand,
    PushItem, RenewCommand, SubIntent,
};
use crate::rsm::replicator::local::{LocalReplicator, OpenConfig, Waker};
use crate::rsm::replicator::Replicator;
use crate::rsm::segments;
use crate::rsm::store::{HeedStore, Store, StoreOpts, TypedReads};
use crate::util::{txn_hash128, uuidv7_bytes};

use super::{
    AckOut, AckReq, DepthOut, DepthReq, DlqHeadOut, DlqHeadReq, PendingReq, PopDiscoverReq, PopOut,
    PopPinnedReq, PopReq, PushOut, PushReq, RaftHealth, RenewOut, RenewReq, ReqCtx, Rsm,
    RsmBuildCtx, RsmError,
};

/// The single-node node id of raft1 / embedded (D2). Membership and identity
/// are WP-4.3's; phase 1 is one voter.
const NODE_ID: u64 = 1;

/// The default consumer group of "queue mode" (`handlers/data.rs`): a pop with
/// no `consumerGroup`. The SQL hard-pins it to seed `all`.
const QUEUE_MODE_GROUP: &str = "__QUEUE_MODE__";

// ---------------------------------------------------------------------------
// The waker: apply-thread wakes → the receiver's long-poll notifier (§9.5)
// ---------------------------------------------------------------------------

/// Bridges [`Waker`] (called on the apply thread after an `Append` or a lease
/// release) to the process [`Notifier`] parked pops wait on (§9.5). It replaces
/// the mesh `MESSAGE_AVAILABLE` frame: a wake for `(tenant, queue, group)` wakes
/// every pop parked on that queue's gate on this node.
struct NotifierWaker {
    notifier: Arc<Notifier>,
}

impl Waker for NotifierWaker {
    fn wake(&self, tenant: &str, queue: &str, _group: Option<&str>) {
        // The gate is per (tenant, queue), not per group (see the SELECTIVE WAKE
        // note in `notify.rs`), so the group is not part of the key. An empty
        // partition hint means "re-scan the queue".
        let qkey = crate::handlers::tenant_queue_key(tenant, queue);
        self.notifier.wake_local_hint(&qkey, "");
    }
}

// ---------------------------------------------------------------------------
// The facade
// ---------------------------------------------------------------------------

/// The real [`Rsm`] of phase 1.
pub struct RaftFacade {
    /// The ordered store (D9), shared with the batcher's planning reads and the
    /// apply thread's writes.
    store: Arc<HeedStore>,
    /// The single-node consensus + apply thread (§12.2). Held for `role`,
    /// `metrics` and `applied_index`; also the sole owner of the apply/writer
    /// threads, joined when the facade drops.
    repl: Arc<LocalReplicator<HeedStore>>,
    /// The segment reader for pop payloads (§7.5), off the live file set.
    reader: segments::Reader,
    /// The command channel the batcher drains (§7.1). Bounded; back-pressure
    /// reaches the receiver.
    cmd_tx: CommandTx,
    /// The long-poll notifier (§9.5), shared with the receiver and driven by
    /// [`NotifierWaker`].
    notifier: Arc<Notifier>,
    /// The batcher task handle, kept so [`RaftFacade::shutdown`] can join it.
    batcher_join: tokio::task::JoinHandle<()>,
    /// `QUEEN_RAFT_POP_FASTPATH_EMPTY` (PERF-J, default on): answer a wildcard
    /// pop that is provably empty from committed state WITHOUT submitting a
    /// `PopWildcard` command onto the single serial batcher pipeline. Resolved
    /// once at open.
    pop_fastpath_empty: bool,
}

/// Wall micros for the PERF-J fastpath's `ready_at` comparison. A coarse hint —
/// the pending ring's `ready_at` is in the RSM clock base (monotone wall micros),
/// so a few microseconds of skew only ever makes a borderline deferred partition
/// look not-yet-ready, which self-heals on the next re-poll (§9.5). The
/// drained-partition case this optimises has no pending row at all, so it does
/// not depend on this clock.
fn wall_micros() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// A boolean env knob: only `0`/`false`/`off`/`no` turns it off; unset or any
/// other value keeps the default (matches [`BatcherConfig::from_env`]).
fn env_flag(name: &str, default_on: bool) -> bool {
    std::env::var(name)
        .ok()
        .map(|v| {
            !matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "0" | "false" | "off" | "no"
            )
        })
        .unwrap_or(default_on)
}

impl RaftFacade {
    /// Open the whole RSM at `ctx.data_dir` (§11.1). Blocking boot I/O; called
    /// once, from the storage seam (`build_raft_state`) inside the runtime.
    pub fn open(ctx: &RsmBuildCtx) -> Result<RaftFacade, String> {
        let dir = PathBuf::from(&ctx.data_dir);
        if ctx.data_dir.trim().is_empty() {
            return Err("QUEEN_RAFT_DIR is required in raft mode (§11.1)".into());
        }
        std::fs::create_dir_all(&dir)
            .map_err(|e| format!("create raft data dir {}: {e}", dir.display()))?;

        let store = Arc::new(
            HeedStore::open(&dir.join("store"), &store_opts_from_env())
                .map_err(|e| format!("open store at {}/store: {e}", dir.display()))?,
        );

        let waker: Arc<dyn Waker> = Arc::new(NotifierWaker {
            notifier: ctx.notifier.clone(),
        });
        let repl = Arc::new(
            LocalReplicator::open(
                store.clone(),
                OpenConfig::new(NODE_ID, dir.clone()),
                waker,
                Arc::new(SystemClock),
            )
            .map_err(|e| format!("open the local replicator at {}: {e}", dir.display()))?,
        );
        let reader = repl.reader();

        let batcher = Batcher::new(store.clone(), repl.clone(), BatcherConfig::from_env());
        let (cmd_tx, batcher_join) = batcher.spawn();

        tracing::info!(
            target: "rsm",
            dir = %dir.display(),
            applied = repl.applied_index(),
            "raft facade open (WP-1.7c)",
        );

        Ok(RaftFacade {
            store,
            repl,
            reader,
            cmd_tx,
            notifier: ctx.notifier.clone(),
            batcher_join,
            pop_fastpath_empty: env_flag("QUEEN_RAFT_POP_FASTPATH_EMPTY", true),
        })
    }

    /// Clean teardown (tests, embedded restart): drop the command channel so the
    /// batcher drains and exits, join it, then join the apply and writer threads
    /// and close the store env, so the SAME data directory can be reopened in
    /// this process (heed refuses two opens of one path). The server never calls
    /// this — its facade lives for the process and the OS reclaims on exit.
    pub async fn shutdown(self) {
        let RaftFacade {
            store,
            repl,
            reader,
            cmd_tx,
            notifier: _,
            batcher_join,
            pop_fastpath_empty: _,
        } = self;
        drop(cmd_tx); // the batcher sees a closed channel, drains, and exits
        let _ = batcher_join.await; // its Arc<repl>/Arc<store> drop here
        drop(reader); // the segment Shared reference the facade held
        match Arc::try_unwrap(repl) {
            Ok(r) => {
                // Joins the apply and writer threads; returns the sole store Arc.
                match r.shutdown() {
                    Ok((_stats, store2)) => {
                        drop(store); // the facade's own clone
                        if let Ok(s) = Arc::try_unwrap(store2) {
                            s.close();
                        }
                    }
                    Err(e) => tracing::warn!(target: "rsm", error = %e, "raft facade shutdown"),
                }
            }
            Err(still) => {
                // A stray reference (a slow blocking read) still holds the
                // replicator; drop what we can and let it wind down on its own.
                drop(still);
                drop(store);
            }
        }
    }

    /// Submit one command and await its [`Reply`] under the context deadline
    /// (I15). A closed channel or an elapsed deadline is a retryable failure.
    async fn submit(&self, ctx: &ReqCtx, command: Command) -> Result<Reply, RsmError> {
        let (sub, rx) = Submission::new(command);
        // The bounded channel absorbs back-pressure; a full channel waits, up to
        // the deadline.
        let send = tokio::time::timeout(ctx.deadline.remaining(), self.cmd_tx.send(sub)).await;
        match send {
            Ok(Ok(())) => {}
            Ok(Err(_closed)) => return Err(RsmError::Internal("planner channel closed".into())),
            Err(_elapsed) => return Err(RsmError::Timeout),
        }
        match tokio::time::timeout(ctx.deadline.remaining(), rx).await {
            Ok(Ok(reply)) => Ok(reply),
            Ok(Err(_dropped)) => Err(RsmError::Internal("planner dropped the reply".into())),
            Err(_elapsed) => Err(RsmError::Timeout),
        }
    }

    /// PERF-J: whether a wildcard pop of `(tenant, queue, group)` is provably
    /// empty from committed state, so it need not enter the serial batcher
    /// pipeline (`QUEEN_RAFT_POP_FASTPATH_EMPTY`). The committed read runs on the
    /// blocking pool (I15), like every other store read on the facade's hot
    /// paths. Any error (join or store) is treated as "not provably empty", so
    /// the caller submits and the planner decides — correctness over the
    /// optimisation.
    async fn wildcard_would_be_empty(&self, tenant: &str, queue: &str, group: &str) -> bool {
        let store = self.store.clone();
        let tenant = tenant.to_string();
        let queue = queue.to_string();
        let group = group.to_string();
        let now_us = wall_micros();
        let res = tokio::task::spawn_blocking(move || {
            store.read(|r| {
                crate::rsm::planner::pop::wildcard_pop_provably_empty(
                    r,
                    &tenant,
                    &queue,
                    &group,
                    now_us,
                    crate::rsm::planner::pop::POP_FASTPATH_SCAN_CAP,
                )
            })
        })
        .await;
        matches!(res, Ok(Ok(true)))
    }

    /// Every dead letter this node has filed, decoded off the committed store.
    /// Test-only: the facade exposes no DLQ read endpoint in phase 1 (§9.6), so
    /// the ack-path DLQ tests read the rows directly here.
    #[cfg(test)]
    pub(crate) fn dlq_rows(&self) -> Vec<crate::rsm::store::rows::DlqRow> {
        use crate::rsm::store::{Keyspace, Reads};
        self.store
            .read(|r| {
                let mut out = Vec::new();
                r.scan_raw(Keyspace::Dlq, &[], &[], usize::MAX, &mut |_k, v| {
                    if let Ok(row) = crate::rsm::store::rows::dlq_decode(v) {
                        out.push(row);
                    }
                    true
                })?;
                Ok(out)
            })
            .expect("read dlq rows")
    }
}

// ---------------------------------------------------------------------------
// Reply → RsmError, and the derived per-command request id
// ---------------------------------------------------------------------------

/// Map a non-`Done` [`Reply`] to the typed facade error.
fn reply_error(reply: Reply) -> RsmError {
    match reply {
        Reply::Retry { hint } => RsmError::Retry {
            leader_hint: hint.map(|n| n.to_string()),
        },
        Reply::Refused(r) => {
            if r.retryable {
                RsmError::Retry { leader_hint: None }
            } else {
                RsmError::Rejected {
                    code: r.code,
                    message: r.message,
                }
            }
        }
        Reply::Done { .. } => RsmError::Internal("unexpected Done in reply_error".into()),
    }
}

/// Derive a distinct request id per split command from the receiver's minted
/// one (D6): the base is unique per HTTP request (uuidv7), and XOR-ing the
/// ordinal keeps it unique per group AND reproducible on a forwarding retry of
/// the same request (so the retry hits the same dedup outcome, I6).
fn derived_request_id(base: RequestId, ordinal: u32) -> RequestId {
    let mut id = base;
    let o = ordinal.to_be_bytes();
    for i in 0..4 {
        id[8 + i] ^= o[i];
    }
    id
}

// ---------------------------------------------------------------------------
// The default queue config for implicit creation (003 first contact)
// ---------------------------------------------------------------------------

/// The config an implicitly-created queue gets, from the `queen.queues` DDL
/// defaults (`server/sql/schema.sql`). The planner stamps `created_at_us`; the
/// receiver mints the id.
fn default_queue_config() -> QueueConfig {
    QueueConfig {
        id: uuidv7_bytes(),
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

/// The store options, honouring `QUEEN_RAFT_MAP_BYTES` (the boot path is exempt
/// from the I2 clock/env ban that `rsm/store` itself carries).
fn store_opts_from_env() -> StoreOpts {
    let mut o = StoreOpts::default();
    if let Some(v) = std::env::var("QUEEN_RAFT_MAP_BYTES")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
    {
        o.map_bytes = Some(v);
    }
    o
}

// ---------------------------------------------------------------------------
// Push
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct PushBodyIn<'a> {
    #[serde(borrow)]
    items: Vec<PushItemIn<'a>>,
}

#[derive(Deserialize)]
struct PushItemIn<'a> {
    #[serde(borrow)]
    queue: std::borrow::Cow<'a, str>,
    #[serde(borrow, default)]
    partition: Option<std::borrow::Cow<'a, str>>,
    #[serde(borrow)]
    payload: &'a RawValue,
    #[serde(borrow, default, rename = "transactionId")]
    transaction_id: Option<std::borrow::Cow<'a, str>>,
}

/// One input item, receiver-resolved: its original index, minted id, txn, queue,
/// partition, and the packed frame + dedup hash. A follower (an intra-request
/// same-txn duplicate) carries no frame and points at its leader.
struct PushResolved {
    message_id: String,
    txn: String,
    queue: String,
    /// `None` for a survivor; `Some(leader index into the flat results)` for an
    /// intra-request follower (postgres `resolve_push_followers`).
    follower_of: Option<usize>,
    hash: [u8; 16],
    frame: Vec<u8>,
}

/// A per-item rendered verdict, in input order.
#[derive(Clone)]
struct PushItemOut {
    message_id: String,
    txn: String,
    queue: String,
    status: &'static str,
    offset: Option<u64>,
}

impl RaftFacade {
    async fn push_impl(&self, ctx: ReqCtx, req: PushReq) -> Result<PushOut, RsmError> {
        // PERF-J: the push-only HTTP-boundary split. `_t_prep` covers the
        // pre-submit work (parse + resolve + pack); `submit_ns` accumulates the
        // `cmd_tx.send` await (channel back-pressure, which `arrival_to_proposed`
        // cannot see because it stamps just before the send).
        let _t_prep = crate::rsm::timing::stamp();
        let mut _submit_ns: u64 = 0;
        let body: PushBodyIn =
            serde_json::from_slice(&req.raw).map_err(|e| RsmError::Rejected {
                code: "bad_body".into(),
                message: format!("bad push body: {e}"),
            })?;
        if body.items.is_empty() {
            return Ok(PushOut { body: "[]".into() });
        }

        // 1. Resolve every input item and collapse intra-request duplicates by
        //    (queue, partition, txn), so the planner is never handed two frames
        //    with the same hash in one command (its probe folds only what came
        //    before, so it would let both survive). The leader is the first
        //    occurrence; a follower inherits the leader's verdict at render.
        let mut resolved: Vec<PushResolved> = Vec::with_capacity(body.items.len());
        // (queue, partition, txn) → the flat index of its leader.
        let mut seen: std::collections::HashMap<(String, String, String), usize> =
            std::collections::HashMap::new();
        // (queue, partition) → the survivors' flat indices, in order.
        let mut groups: indexed_groups::Groups = indexed_groups::Groups::new();

        for it in &body.items {
            let mid = uuidv7_bytes();
            let mid_str = uuid_bytes_to_string(&mid);
            let txn = it
                .transaction_id
                .as_deref()
                .map(str::to_string)
                .unwrap_or_else(|| mid_str.clone());
            let queue = it.queue.as_ref().to_string();
            let partition = it
                .partition
                .as_deref()
                .filter(|p| !p.is_empty())
                .unwrap_or("Default")
                .to_string();

            super::check_message_key_names(&ctx.tenant, &queue, None, Some(&partition))?;

            let flat = resolved.len();
            let key = (queue.clone(), partition.clone(), txn.clone());
            let follower_of = match seen.get(&key) {
                Some(&leader) => Some(leader),
                None => {
                    seen.insert(key, flat);
                    None
                }
            };
            let hash = txn_hash128(&txn);
            let frame = if follower_of.is_none() {
                pack_frames(&[FrameIn {
                    message_id: mid,
                    txn: &txn,
                    trace_id: None,
                    producer_sub: None,
                    payload: it.payload.get().as_bytes(),
                    encrypted: false,
                }])
            } else {
                Vec::new()
            };
            if follower_of.is_none() {
                groups.push(&queue, &partition, flat);
            }
            resolved.push(PushResolved {
                message_id: mid_str,
                txn,
                queue,
                follower_of,
                hash,
                frame,
            });
        }

        // 2. One PushCommand per (queue, partition) group, its items the group's
        //    survivors in order. Submit them all, then await every reply.
        // PERF-J: record the pre-submit leg now (parse + resolve + pack done).
        if let Some(t) = _t_prep {
            crate::rsm::timing::metrics()
                .push_h_prep
                .record_dur(t.elapsed());
        }
        let mut rxs = Vec::with_capacity(groups.len());
        for (ordinal, g) in groups.iter().enumerate() {
            let items: Vec<PushItem> = g
                .members
                .iter()
                .map(|&flat| PushItem {
                    hash: resolved[flat].hash,
                    frame: resolved[flat].frame.clone(),
                })
                .collect();
            let cmd = Command::Push(PushCommand {
                request_id: derived_request_id(ctx.request_id, ordinal as u32),
                tenant: ctx.tenant.clone(),
                queue: g.queue.clone(),
                partition: g.partition.clone(),
                items,
                create_cfg: default_queue_config(),
            });
            let (sub, rx) = Submission::new(cmd);
            let _t_send = crate::rsm::timing::stamp();
            let sent = tokio::time::timeout(ctx.deadline.remaining(), self.cmd_tx.send(sub)).await;
            if let Some(t) = _t_send {
                _submit_ns = _submit_ns.saturating_add(t.elapsed().as_nanos() as u64);
            }
            match sent {
                Ok(Ok(())) => rxs.push((g.members.clone(), rx)),
                Ok(Err(_)) => return Err(RsmError::Internal("planner channel closed".into())),
                Err(_) => return Err(RsmError::Timeout),
            }
        }
        // PERF-J: the accumulated channel-enqueue wait (all groups), and open
        // the reply-wait leg (propose+commit+apply+answer), push-only.
        let _t_await = crate::rsm::timing::stamp();
        if _t_await.is_some() {
            crate::rsm::timing::metrics()
                .push_h_submit
                .record(_submit_ns);
        }

        // 3. Collect. A whole-group Retry fails the whole push (the SDK retries
        //    with the same derived ids, deduped by request id); a non-retryable
        //    Rejected marks that group's items "error" (a push answers 201 with
        //    per-item statuses).
        let mut out: Vec<Option<PushItemOut>> = vec![None; resolved.len()];
        for (members, rx) in rxs {
            let reply = match tokio::time::timeout(ctx.deadline.remaining(), rx).await {
                Ok(Ok(r)) => r,
                Ok(Err(_)) => return Err(RsmError::Internal("planner dropped the reply".into())),
                Err(_) => return Err(RsmError::Timeout),
            };
            match reply {
                Reply::Done { outcome, .. } => {
                    let verdicts = match outcome {
                        Outcome::Push(p) => p.items,
                        other => {
                            return Err(RsmError::Internal(format!(
                                "push got a non-push outcome: {other:?}"
                            )))
                        }
                    };
                    for (k, &flat) in members.iter().enumerate() {
                        let r = &resolved[flat];
                        let (status, offset) = match verdicts.get(k) {
                            Some(PushVerdict::Created { offset, .. }) => ("queued", Some(*offset)),
                            Some(PushVerdict::Duplicate { offset, .. }) => {
                                ("duplicate", Some(*offset))
                            }
                            Some(PushVerdict::Refused { .. }) | None => ("error", None),
                        };
                        out[flat] = Some(PushItemOut {
                            message_id: r.message_id.clone(),
                            txn: r.txn.clone(),
                            queue: r.queue.clone(),
                            status,
                            offset,
                        });
                    }
                }
                Reply::Retry { hint } => {
                    return Err(RsmError::Retry {
                        leader_hint: hint.map(|n| n.to_string()),
                    })
                }
                Reply::Refused(refusal) if refusal.retryable => {
                    return Err(RsmError::Retry { leader_hint: None })
                }
                Reply::Refused(_) => {
                    for &flat in &members {
                        let r = &resolved[flat];
                        out[flat] = Some(PushItemOut {
                            message_id: r.message_id.clone(),
                            txn: r.txn.clone(),
                            queue: r.queue.clone(),
                            status: "error",
                            offset: None,
                        });
                    }
                }
            }
        }

        // PERF-J: every group's reply is in — close the reply-wait leg.
        if let Some(t) = _t_await {
            crate::rsm::timing::metrics()
                .push_h_await
                .record_dur(t.elapsed());
        }

        // 4. Followers inherit the leader's id, status ("duplicate") and offset
        //    (C1). A follower whose leader errored is an error too.
        for i in 0..resolved.len() {
            if let Some(leader) = resolved[i].follower_of {
                let lead = out[leader].clone();
                let r = &resolved[i];
                out[i] = Some(match lead {
                    Some(l) if l.status == "error" => PushItemOut {
                        message_id: l.message_id,
                        txn: r.txn.clone(),
                        queue: r.queue.clone(),
                        status: "error",
                        offset: None,
                    },
                    Some(l) => PushItemOut {
                        message_id: l.message_id,
                        txn: r.txn.clone(),
                        queue: r.queue.clone(),
                        status: "duplicate",
                        offset: l.offset,
                    },
                    None => PushItemOut {
                        message_id: r.message_id.clone(),
                        txn: r.txn.clone(),
                        queue: r.queue.clone(),
                        status: "error",
                        offset: None,
                    },
                });
            }
        }

        Ok(PushOut {
            body: render_push(&out),
        })
    }
}

/// `[{index, message_id, transaction_id, queueName, status, offset?}]`, input
/// order (`handlers/data.rs::render_push_results`).
fn render_push(items: &[Option<PushItemOut>]) -> String {
    let mut out = String::with_capacity(items.len() * 176 + 2);
    out.push('[');
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        let it = item.as_ref();
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        out.push_str(",\"message_id\":\"");
        if let Some(it) = it {
            out.push_str(&it.message_id);
        }
        out.push_str("\",\"transaction_id\":\"");
        if let Some(it) = it {
            crate::fusion::json_escape_into(&mut out, &it.txn);
        }
        out.push_str("\",\"queueName\":\"");
        if let Some(it) = it {
            crate::fusion::json_escape_into(&mut out, &it.queue);
        }
        out.push_str("\",\"status\":\"");
        out.push_str(it.map(|i| i.status).unwrap_or("error"));
        out.push('"');
        if let Some(off) = it.and_then(|i| i.offset) {
            out.push_str(",\"offset\":");
            out.push_str(&off.to_string());
        }
        out.push('}');
    }
    out.push(']');
    out
}

/// Ordered grouping of push survivors by (queue, partition), first-seen order.
#[allow(clippy::new_without_default, clippy::len_without_is_empty)]
mod indexed_groups {
    pub struct Group {
        pub queue: String,
        pub partition: String,
        pub members: Vec<usize>,
    }
    pub struct Groups {
        groups: Vec<Group>,
        index: std::collections::HashMap<(String, String), usize>,
    }
    impl Groups {
        pub fn new() -> Groups {
            Groups {
                groups: Vec::new(),
                index: std::collections::HashMap::new(),
            }
        }
        pub fn push(&mut self, queue: &str, partition: &str, flat: usize) {
            let key = (queue.to_string(), partition.to_string());
            let idx = *self.index.entry(key).or_insert_with(|| {
                self.groups.push(Group {
                    queue: queue.to_string(),
                    partition: partition.to_string(),
                    members: Vec::new(),
                });
                self.groups.len() - 1
            });
            self.groups[idx].members.push(flat);
        }
        pub fn len(&self) -> usize {
            self.groups.len()
        }
        pub fn iter(&self) -> std::slice::Iter<'_, Group> {
            self.groups.iter()
        }
    }
}

// ---------------------------------------------------------------------------
// Pop
// ---------------------------------------------------------------------------

impl RaftFacade {
    /// The shared pop driver: build the [`PopCommand`], run it (with a bounded
    /// long-poll re-poll on an empty claim when `wait`, §9.5), and render.
    #[allow(clippy::too_many_arguments)]
    async fn pop_run(
        &self,
        ctx: &ReqCtx,
        queue: String,
        partition: Option<String>,
        namespace: String,
        task: String,
        group_opt: Option<String>,
        batch: u32,
        auto_ack: bool,
        wait: bool,
        wildcard_create: bool,
    ) -> Result<PopOut, RsmError> {
        let group = group_opt.unwrap_or_else(|| QUEUE_MODE_GROUP.to_string());
        // Queue mode seeds `all`; a named group defaults to `new` (§8, 004). A
        // richer subscriptionMode is threaded by a later WP.
        let sub = if group == QUEUE_MODE_GROUP {
            SubIntent::default() // mode "" → seed at the floor (all)
        } else {
            SubIntent {
                mode: "new".into(),
                from_us: None,
                now: false,
            }
        };
        let worker = uuid_bytes_to_string(&uuidv7_bytes());
        let budget = batch.min(i32::MAX as u32) as i32;
        let qkey = crate::handlers::tenant_queue_key(&ctx.tenant, &queue);

        loop {
            let cmd = PopCommand {
                request_id: uuidv7_bytes(), // a fresh command per attempt (§5.4)
                tenant: ctx.tenant.clone(),
                queue: queue.clone(),
                partition: partition.clone(),
                group: group.clone(),
                worker: worker.clone(),
                budget,
                // A pinned pop is one partition; a wildcard/discovery pop may
                // sweep up to the batch (clamped to the 64-wide checkout ceiling).
                max_parts: if partition.is_some() {
                    1
                } else {
                    batch.clamp(1, 64) as i32
                },
                lease_seconds: 60,
                auto_ack,
                conflate: false,
                sub: sub.clone(),
                skip_window_debounce: false,
                namespace: namespace.clone(),
                task: task.clone(),
                create_cfg: if wildcard_create {
                    Some(default_queue_config())
                } else {
                    None
                },
            };
            let command = match &partition {
                Some(_) => Command::PopPinned(cmd),
                None if !namespace.is_empty() || !task.is_empty() => Command::PopDiscover(cmd),
                None => Command::PopWildcard(cmd),
            };

            // PERF-J: a wildcard pop that is provably empty from committed state
            // (the group is registered and no partition is ready) never enters
            // the single serial batcher pipeline, where its ~0.5 ms plan would
            // queue behind — and delay — the pushes. It flows into exactly the
            // same empty handling below (long-poll park or empty render); a push
            // that lands meanwhile re-arms the ring and wakes the park, so no
            // claim is stranded (§9.5).
            let claims = if self.pop_fastpath_empty
                && matches!(command, Command::PopWildcard(_))
                && self
                    .wildcard_would_be_empty(&ctx.tenant, &queue, &group)
                    .await
            {
                Vec::new()
            } else {
                let reply = self.submit(ctx, command).await?;
                match reply {
                    Reply::Done { outcome, .. } => match outcome {
                        Outcome::Pop(o) => o.claims,
                        other => {
                            return Err(RsmError::Internal(format!(
                                "pop got a non-pop outcome: {other:?}"
                            )))
                        }
                    },
                    other => return Err(reply_error(other)),
                }
            };

            if !claims.is_empty() {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, claims)
                    .await;
            }

            // Empty. Long-poll only for a queue-scoped pop (§9.5); discovery has
            // no single gate here.
            if !wait || partition.is_none() && (!namespace.is_empty() || !task.is_empty()) {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            let remaining = ctx.deadline.remaining();
            if remaining.is_zero() {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            let park = remaining.min(Duration::from_millis(500));
            let _woke = self.notifier.wait_queue(&qkey, park).await;
            if ctx.deadline.expired() {
                return self
                    .render_claims(ctx, &queue, &group, &worker, auto_ack, Vec::new())
                    .await;
            }
            // Loop and re-poll.
        }
    }

    /// Read every claim's payload off this node's files (§7.5) and render the
    /// pop wire body. The reads run on the blocking pool (I15).
    async fn render_claims(
        &self,
        ctx: &ReqCtx,
        queue: &str,
        group: &str,
        worker: &str,
        auto_ack: bool,
        claims: Vec<PopClaim>,
    ) -> Result<PopOut, RsmError> {
        let store = self.store.clone();
        let reader = self.reader.clone();
        let tenant = ctx.tenant.clone();
        let queue = queue.to_string();
        let group = group.to_string();
        let worker = worker.to_string();
        let deadline = ctx.deadline;

        let rendered = tokio::task::spawn_blocking(move || {
            // PERF-1: pop payload read latency — the blocking segment render.
            // The clock read is gated on the knob (`stamp` is `None` when
            // metrics are off) so the ablation prices it, not just the record.
            let r0 = crate::rsm::timing::stamp();
            let out = render_pop_blocking(
                &store, &reader, &tenant, &queue, &group, &worker, auto_ack, &claims, deadline,
            );
            if let Some(r0) = r0 {
                crate::rsm::timing::metrics()
                    .pop_read
                    .record_dur(r0.elapsed());
            }
            out
        })
        .await
        .map_err(|e| RsmError::Internal(format!("pop render task: {e}")))?;

        rendered.map_err(RsmError::Internal)
    }
}

/// Per-partition read context for the render.
struct PartInfo {
    name: String,
    queue: String,
    bucket: u16,
    sealed: Vec<u32>,
}

/// The blocking render: one store read for every claimed partition's name and
/// sealed-file list, then a segment read per claimed segment.
#[allow(clippy::too_many_arguments)]
fn render_pop_blocking(
    store: &HeedStore,
    reader: &segments::Reader,
    tenant: &str,
    top_queue: &str,
    group: &str,
    worker: &str,
    auto_ack: bool,
    claims: &[PopClaim],
    deadline: super::Deadline,
) -> Result<PopOut, String> {
    // Resolve every claimed pid's partition row + sealed files in one read txn.
    let mut infos: std::collections::HashMap<Pid, PartInfo> = std::collections::HashMap::new();
    store
        .read(|r| {
            for c in claims {
                if infos.contains_key(&c.pid) {
                    continue;
                }
                let Some(part) = r.partition(c.pid)? else {
                    continue;
                };
                let bucket = bucket_of(tenant, &part.queue, &part.partition);
                let mut sealed = Vec::new();
                r.scan_partition_files(c.pid, usize::MAX, &mut |f| {
                    sealed.push(f);
                    true
                })?;
                infos.insert(
                    c.pid,
                    PartInfo {
                        name: part.partition,
                        queue: part.queue,
                        bucket,
                        sealed,
                    },
                );
            }
            Ok(())
        })
        .map_err(|e| format!("pop render read: {e}"))?;

    let lease_id = if auto_ack || claims.is_empty() {
        ""
    } else {
        worker
    };
    let (first_name, first_pid) = claims
        .first()
        .map(|c| {
            (
                infos.get(&c.pid).map(|i| i.name.as_str()).unwrap_or(""),
                c.pid.to_string(),
            )
        })
        .unwrap_or(("", String::new()));

    let mut out = String::with_capacity(256 + top_queue.len());
    out.push_str("{\"success\":true,\"queue\":\"");
    crate::fusion::json_escape_into(&mut out, top_queue);
    out.push_str("\",\"partition\":\"");
    crate::fusion::json_escape_into(&mut out, first_name);
    out.push_str("\",\"partitionId\":\"");
    crate::fusion::json_escape_into(&mut out, &first_pid);
    out.push_str("\",\"leaseId\":\"");
    crate::fusion::json_escape_into(&mut out, lease_id);
    out.push_str("\",\"consumerGroup\":\"");
    crate::fusion::json_escape_into(&mut out, group);
    out.push_str("\",\"messages\":[");

    let dl = Some(deadline.instant());
    let mut count = 0usize;
    for claim in claims {
        let Some(info) = infos.get(&claim.pid) else {
            continue;
        };
        let attempt = claim.delivery_attempt.max(1);
        let partition_id = claim.pid.to_string();
        let mut off = claim.start_offset;
        while off <= claim.end_offset {
            let frame = match reader.read_at_within(info.bucket, claim.pid, off, &info.sealed, dl) {
                Ok(Some(f)) => f,
                Ok(None) => {
                    // A gap (retention passed it, or not yet visible): skip one.
                    off += 1;
                    continue;
                }
                Err(e) => {
                    return Err(format!(
                        "read pop payload at pid {} off {off}: {e}",
                        claim.pid
                    ))
                }
            };
            let base = frame.base_offset;
            let seg_created = iso_from_us(frame.created_at_us);
            let frames = unpack_frames_ref(&frame.blob);
            if let Some(frames) = frames {
                for (i, fr) in frames.iter().enumerate() {
                    let msg_off = base + i as u64;
                    if msg_off < off || msg_off < claim.start_offset || msg_off > claim.end_offset {
                        continue;
                    }
                    if count > 0 {
                        out.push(',');
                    }
                    out.push_str("{\"id\":\"");
                    crate::frames::uuid_hex_into(&mut out, &fr.message_id);
                    out.push_str("\",\"transactionId\":\"");
                    crate::fusion::json_escape_into(&mut out, fr.txn);
                    out.push_str("\",\"traceId\":");
                    match &fr.trace_id {
                        Some(t) => {
                            out.push('"');
                            crate::frames::uuid_hex_into(&mut out, t);
                            out.push('"');
                        }
                        None => out.push_str("null"),
                    }
                    out.push_str(",\"data\":");
                    if fr.payload.is_empty() {
                        out.push_str("null");
                    } else {
                        push_utf8(&mut out, fr.payload);
                    }
                    out.push_str(",\"producerSub\":");
                    match &fr.producer_sub {
                        Some(ps) => {
                            out.push('"');
                            crate::fusion::json_escape_into(&mut out, ps);
                            out.push('"');
                        }
                        None => out.push_str("null"),
                    }
                    out.push_str(",\"createdAt\":\"");
                    out.push_str(&seg_created);
                    out.push_str("\",\"partitionId\":\"");
                    crate::fusion::json_escape_into(&mut out, &partition_id);
                    out.push_str("\",\"partition\":\"");
                    crate::fusion::json_escape_into(&mut out, &info.name);
                    out.push_str("\",\"leaseId\":\"");
                    crate::fusion::json_escape_into(&mut out, lease_id);
                    out.push_str("\",\"consumerGroup\":\"");
                    crate::fusion::json_escape_into(&mut out, group);
                    out.push_str("\",\"deliveryAttempt\":");
                    out.push_str(&attempt.to_string());
                    out.push_str(",\"offset\":");
                    out.push_str(&msg_off.to_string());
                    out.push('}');
                    count += 1;
                }
            }
            // Advance past this whole segment; a claim range is a run of segments.
            off = base + frame.count as u64;
        }
        // silence the unused warning on info.queue (kept for a future
        // per-partition top-level queue on discovery).
        let _ = &info.queue;
    }

    out.push_str("],\"partitionsClaimed\":");
    out.push_str(&claims.len().to_string());
    out.push('}');
    Ok(PopOut {
        body: out,
        empty: count == 0,
    })
}

// ---------------------------------------------------------------------------
// Ack
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct AckBodyIn {
    // single
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    error: Option<String>,
    // batch
    #[serde(default)]
    acknowledgments: Vec<AckBodyItem>,
}

#[derive(Deserialize)]
struct AckBodyItem {
    #[serde(rename = "transactionId")]
    transaction_id: Option<String>,
    #[serde(rename = "partitionId")]
    partition_id: Option<String>,
    status: Option<String>,
    #[serde(rename = "leaseId")]
    lease_id: Option<String>,
    error: Option<String>,
}

/// One flat ack the receiver resolved: original index, txn, pid, worker, status,
/// error.
struct AckFlat {
    index: usize,
    txn: String,
    pid: Pid,
    worker: String,
    status: AckStatus,
    error: Option<String>,
}

fn ack_status_of(s: Option<&str>) -> AckStatus {
    match s.map(|x| x.to_ascii_lowercase()).as_deref() {
        Some("failed") => AckStatus::Failed,
        Some("dlq") => AckStatus::Dlq,
        Some("retry") => AckStatus::Retry,
        _ => AckStatus::Ok,
    }
}

impl RaftFacade {
    async fn ack_impl(&self, ctx: ReqCtx, req: AckReq) -> Result<AckOut, RsmError> {
        let body: AckBodyIn = serde_json::from_slice(&req.raw).map_err(|e| RsmError::Rejected {
            code: "bad_body".into(),
            message: format!("bad ack body: {e}"),
        })?;

        // Flatten single vs batch into (index, txn, partitionId, status, lease).
        let mut raw_items: Vec<(String, String, AckStatus, String, Option<String>)> = Vec::new();
        if !body.acknowledgments.is_empty() {
            for it in &body.acknowledgments {
                raw_items.push((
                    it.transaction_id.clone().unwrap_or_default(),
                    it.partition_id.clone().unwrap_or_default(),
                    ack_status_of(it.status.as_deref()),
                    it.lease_id.clone().unwrap_or_default(),
                    it.error.clone(),
                ));
            }
        } else {
            raw_items.push((
                body.transaction_id.clone().unwrap_or_default(),
                body.partition_id.clone().unwrap_or_default(),
                ack_status_of(body.status.as_deref()),
                body.lease_id.clone().unwrap_or_default(),
                body.error.clone(),
            ));
        }
        if raw_items.is_empty() {
            return Ok(AckOut { body: "[]".into() });
        }

        // Resolve each `partitionId` (the numeric pid, see the module header)
        // to a pid; an unparsable one is a per-item error the render carries.
        let mut flats: Vec<AckFlat> = Vec::with_capacity(raw_items.len());
        let mut bad: Vec<(usize, String)> = Vec::new();
        for (i, (txn, pid_str, status, worker, error)) in raw_items.iter().enumerate() {
            match pid_str.parse::<u64>() {
                Ok(pid) => flats.push(AckFlat {
                    index: i,
                    txn: txn.clone(),
                    pid,
                    worker: worker.clone(),
                    status: *status,
                    error: error.clone(),
                }),
                Err(_) => bad.push((i, "partitionId is not a partition id".to_string())),
            }
        }

        // Group by (pid, worker) into AckTargets. Read each pid's (tenant,
        // queue) once. A pid with no row is a per-item error.
        let store = self.store.clone();
        let tenant = ctx.tenant.clone();
        let group = req.group.clone();
        let (targets, per_item, more_bad) = tokio::task::spawn_blocking(move || {
            resolve_ack_targets(&store, &tenant, &group, flats)
        })
        .await
        .map_err(|e| RsmError::Internal(format!("ack resolve task: {e}")))?;
        bad.extend(more_bad);

        let txns: Vec<String> = raw_items.iter().map(|r| r.0.clone()).collect();

        if targets.is_empty() {
            // Nothing resolvable: render all as errors/failures.
            return Ok(AckOut {
                body: render_ack(&txns, &[], &per_item, &bad),
            });
        }

        let cmd = Command::Ack(AckCommand {
            request_id: ctx.request_id,
            targets,
        });
        let reply = self.submit(&ctx, cmd).await?;
        let results = match reply {
            Reply::Done { outcome, .. } => match outcome {
                Outcome::Ack(o) => o.results,
                other => {
                    return Err(RsmError::Internal(format!(
                        "ack got a non-ack outcome: {other:?}"
                    )))
                }
            },
            other => return Err(reply_error(other)),
        };

        Ok(AckOut {
            body: render_ack(&txns, &results, &per_item, &bad),
        })
    }
}

/// The per-input-item resolution result: which target it went to (by index into
/// the returned targets), so the render can attribute the target's lease/dlq.
/// The item's own `status` is kept because the target's `AckResult` reports the
/// DLQ as a COUNT, not a per-item set (WP-1.1's shape, R-101): the render needs
/// this item's status to tell a filed dead letter from a sibling ack on the same
/// target (see [`render_ack`]).
struct AckPerItem {
    index: usize,
    target: usize,
    hash: [u8; 16],
    status: AckStatus,
}

/// Group resolved acks by (pid, worker), reading each pid's queue once.
fn resolve_ack_targets(
    store: &HeedStore,
    tenant: &str,
    group: &str,
    flats: Vec<AckFlat>,
) -> (Vec<AckTarget>, Vec<AckPerItem>, Vec<(usize, String)>) {
    let mut targets: Vec<AckTarget> = Vec::new();
    let mut per_item: Vec<AckPerItem> = Vec::new();
    let mut bad: Vec<(usize, String)> = Vec::new();
    // (pid, worker) → target index.
    let mut index: std::collections::HashMap<(Pid, String), usize> =
        std::collections::HashMap::new();

    let read = store.read(|r| {
        for f in &flats {
            let Some(part) = r.partition(f.pid)? else {
                bad.push((f.index, format!("no partition {}", f.pid)));
                continue;
            };
            let key = (f.pid, f.worker.clone());
            let ti = match index.get(&key) {
                Some(&ti) => ti,
                None => {
                    let ti = targets.len();
                    targets.push(AckTarget {
                        pid: f.pid,
                        tenant: tenant.to_string(),
                        queue: part.queue.clone(),
                        group: group.to_string(),
                        worker: f.worker.clone(),
                        items: Vec::new(),
                    });
                    index.insert(key, ti);
                    ti
                }
            };
            let hash = txn_hash128(&f.txn);
            // A signal that may file a dead letter carries the receiver's
            // snapshot (O7/O20). The full pre-read of the poison frame is a
            // later WP; what the ack wire gives us for free is the txn, so the
            // filed row is identifiable instead of blank (its `payload` and
            // `message_id` stay empty until the offset→frame resolve lands).
            let snapshot =
                matches!(f.status, AckStatus::Dlq | AckStatus::Failed).then(|| DlqSnapshot {
                    message_id: None,
                    txn: f.txn.clone(),
                    payload: Vec::new(),
                });
            targets[ti].items.push(AckItem {
                hash,
                status: f.status,
                error: f.error.clone(),
                snapshot,
            });
            per_item.push(AckPerItem {
                index: f.index,
                target: ti,
                hash,
                status: f.status,
            });
        }
        Ok(())
    });
    if let Err(e) = read {
        bad.push((usize::MAX, format!("ack resolve read: {e}")));
    }
    (targets, per_item, bad)
}

/// `[{index, transactionId, success, error, leaseReleased, dlq}]`, input order
/// (`handlers/data.rs` ack wire). The group is applied by the caller.
fn render_ack(
    txns: &[String],
    results: &[crate::rsm::entry::AckResult],
    per_item: &[AckPerItem],
    bad: &[(usize, String)],
) -> String {
    let mut out = String::with_capacity(txns.len() * 96 + 2);
    out.push('[');
    for (i, txn) in txns.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str("{\"index\":");
        out.push_str(&i.to_string());
        // Attribute this input item to its target result.
        let item = per_item.iter().find(|p| p.index == i);
        let bad_msg = bad.iter().find(|(bi, _)| *bi == i).map(|(_, m)| m.as_str());
        out.push_str(",\"transactionId\":\"");
        crate::fusion::json_escape_into(&mut out, txn);
        out.push('"');
        if let Some(msg) = bad_msg {
            out.push_str(",\"success\":false,\"error\":\"");
            crate::fusion::json_escape_into(&mut out, msg);
            out.push_str("\",\"leaseReleased\":false,\"dlq\":false}");
            continue;
        }
        let (success, lease_released, dlq) =
            match item.and_then(|p| Some((p, results.get(p.target)?))) {
                Some((p, res)) => {
                    let stale = res.stale_hashes.contains(&p.hash);
                    // Per-item DLQ, NOT `res.dlq > 0` broadcast to the whole target
                    // (the batch-ack mis-attribution R-101 leaves us to guard here).
                    // `res.dlq` is a COUNT — the outcome shape carries no per-item
                    // DLQ set — so a completed ack that shares a (partition, lease)
                    // target with a sibling that DID dead-letter must not inherit
                    // its flag. An item reads `dlq:true` only when its target filed
                    // a dead letter AND this item itself carried a DLQ-eligible
                    // signal; `res.dlq == 0` (e.g. a `failed` whose retry budget
                    // remained, so it was released to redeliver) reads false for
                    // every item. RESIDUAL, owed to R-101's shape refinement: two+
                    // signal items on ONE target with `res.dlq == 1` see the head
                    // (lowest-offset) one filed, but the receiver holds no offsets
                    // in this AckResult shape and marks each signal item — the
                    // per-item DLQ set the outcome must carry to disambiguate.
                    let dlq = res.dlq > 0 && matches!(p.status, AckStatus::Dlq | AckStatus::Failed);
                    (!stale, res.lease_released, dlq)
                }
                None => (true, false, false),
            };
        out.push_str(",\"success\":");
        out.push_str(if success { "true" } else { "false" });
        out.push_str(",\"error\":null,\"leaseReleased\":");
        out.push_str(if lease_released { "true" } else { "false" });
        out.push_str(",\"dlq\":");
        out.push_str(if dlq { "true" } else { "false" });
        out.push('}');
    }
    out.push(']');
    out
}

// ---------------------------------------------------------------------------
// Renew, DLQ head, pending, depth
// ---------------------------------------------------------------------------

impl RaftFacade {
    async fn renew_impl(&self, ctx: ReqCtx, req: RenewReq) -> Result<RenewOut, RsmError> {
        let cmd = Command::Renew(RenewCommand {
            request_id: ctx.request_id,
            worker: req.lease_id.clone(),
            seconds: req.seconds.clamp(1, i32::MAX as i64) as i32,
        });
        let reply = self.submit(&ctx, cmd).await?;
        let outcome = match reply {
            Reply::Done { outcome, .. } => outcome,
            other => return Err(reply_error(other)),
        };
        let (renewed, expires) = match outcome {
            Outcome::Renew(r) => (r.renewed, r.min_expires_at_us),
            other => {
                return Err(RsmError::Internal(format!(
                    "renew got a non-renew outcome: {other:?}"
                )))
            }
        };
        let expires_iso = expires.map(iso_from_us);
        let mut out = String::from("{\"leaseId\":\"");
        crate::fusion::json_escape_into(&mut out, &req.lease_id);
        out.push_str("\",\"success\":");
        out.push_str(if renewed > 0 { "true" } else { "false" });
        out.push_str(",\"renewed\":");
        out.push_str(&renewed.to_string());
        for key in ["newExpiresAt", "expiresAt", "lease_expires_at"] {
            out.push_str(",\"");
            out.push_str(key);
            out.push_str("\":");
            match &expires_iso {
                Some(e) => {
                    out.push('"');
                    out.push_str(e);
                    out.push('"');
                }
                None => out.push_str("null"),
            }
        }
        out.push('}');
        Ok(RenewOut { body: out })
    }
}

// ---------------------------------------------------------------------------
// The Rsm impl
// ---------------------------------------------------------------------------

#[async_trait]
impl Rsm for RaftFacade {
    async fn push(&self, ctx: ReqCtx, req: PushReq) -> Result<PushOut, RsmError> {
        self.push_impl(ctx, req).await
    }

    async fn pop_wildcard(&self, ctx: ReqCtx, req: PopReq) -> Result<PopOut, RsmError> {
        self.pop_run(
            &ctx,
            req.queue,
            None,
            String::new(),
            String::new(),
            req.group,
            req.batch,
            req.auto_ack,
            req.wait,
            true,
        )
        .await
    }

    async fn pop_pinned(&self, ctx: ReqCtx, req: PopPinnedReq) -> Result<PopOut, RsmError> {
        self.pop_run(
            &ctx,
            req.queue,
            Some(req.partition),
            String::new(),
            String::new(),
            req.group,
            req.batch,
            req.auto_ack,
            req.wait,
            false,
        )
        .await
    }

    async fn pop_discover(&self, ctx: ReqCtx, req: PopDiscoverReq) -> Result<PopOut, RsmError> {
        self.pop_run(
            &ctx,
            String::new(),
            None,
            req.namespace,
            req.task,
            req.group,
            req.batch,
            req.auto_ack,
            req.wait,
            false,
        )
        .await
    }

    async fn ack(&self, ctx: ReqCtx, req: AckReq) -> Result<AckOut, RsmError> {
        self.ack_impl(ctx, req).await
    }

    async fn renew(&self, ctx: ReqCtx, req: RenewReq) -> Result<RenewOut, RsmError> {
        self.renew_impl(ctx, req).await
    }

    async fn dlq_head(&self, _ctx: ReqCtx, _req: DlqHeadReq) -> Result<DlqHeadOut, RsmError> {
        // The standalone DLQ-head command is not routed in phase 1 (§9.6); a
        // forced DLQ rides the ack path. Answered empty rather than erroring.
        Ok(DlqHeadOut { body: None })
    }

    async fn has_pending(&self, _ctx: ReqCtx, _req: PendingReq) -> Result<bool, RsmError> {
        // The indexed pending probe is a later WP's local stale read (§9.5). The
        // long-poll re-poll drives the pop directly meanwhile.
        Ok(true)
    }

    async fn depth(&self, _ctx: ReqCtx, _req: DepthReq) -> Result<DepthOut, RsmError> {
        // Depth is a §9.6 counter read wired by WP-2.6; not yet.
        Err(RsmError::Unsupported)
    }

    fn health(&self) -> RaftHealth {
        let m = self.repl.metrics();
        let role = self.repl.role();
        RaftHealth {
            role: match role {
                crate::rsm::replicator::Role::Leader { .. } => "leader".into(),
                crate::rsm::replicator::Role::Follower { .. } => "follower".into(),
                crate::rsm::replicator::Role::Learner => "learner".into(),
                crate::rsm::replicator::Role::Candidate => "candidate".into(),
                crate::rsm::replicator::Role::Stopped => "stopped".into(),
            },
            leader_known: m.leader.is_some()
                && !matches!(role, crate::rsm::replicator::Role::Stopped),
            term: m.term,
            applied: m.applied_index,
            commit: m.committed_index,
            lag_ms: 0,
            storage_ready: !matches!(role, crate::rsm::replicator::Role::Stopped),
        }
    }

    fn notifier(&self) -> Option<&Arc<Notifier>> {
        Some(&self.notifier)
    }
}

// ---------------------------------------------------------------------------
// The builder hook (WP-1.7a left it; the boot paths register this)
// ---------------------------------------------------------------------------

/// The [`super::RsmBuilder`] the binary and embedded boot register through
/// [`super::set_builder`]. A store or replicator that cannot open at boot is
/// fatal (the node cannot serve without its state); it is NOT registered in the
/// unit-test binary, so the WP-1.7a seam tests keep the `NotReady` stub and the
/// integration tests build [`RaftFacade`] directly.
pub fn real_builder(ctx: &RsmBuildCtx) -> Arc<dyn Rsm> {
    // PERF-E: pin the apply-side `record` dedup authority ONCE, at the
    // production boot seam only (unit tests build `RaftFacade` directly and
    // never reach here, so they keep the `rows` default that the existing
    // apply/store tests assert against). The planner side is resolved
    // independently in `BatcherConfig::from_env`; both read the same
    // `QUEEN_RAFT_DEDUP_INDEX`, so a real node's write and read paths agree.
    crate::rsm::dedup::set_record_index_mode(crate::rsm::dedup::IndexMode::from_env());
    match RaftFacade::open(ctx) {
        Ok(f) => Arc::new(f),
        Err(e) => crate::obs::fatal(format!("raft storage failed to open: {e}")),
    }
}

// ---------------------------------------------------------------------------
// Small helpers
// ---------------------------------------------------------------------------

/// Splice bytes expected to be valid UTF-8 (payload JSON), lossy on the rare
/// invalid tail — the same policy as `handlers/data.rs::push_utf8`.
fn push_utf8(out: &mut String, bytes: &[u8]) {
    match std::str::from_utf8(bytes) {
        Ok(s) => out.push_str(s),
        Err(_) => out.push_str(&String::from_utf8_lossy(bytes)),
    }
}

/// Format epoch microseconds as the SP's UTC ISO-8601 shape
/// (`YYYY-MM-DDTHH:MM:SS.mmmZ`), the inverse of `util::parse_iso_ms`'s
/// `days_from_civil`. No date-time dependency (matching `util`).
fn iso_from_us(us: i64) -> String {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY); // µs into the day
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let ms = (rem / 1000) % 1000;
    let hh = secs / 3600;
    let mm = (secs / 60) % 60;
    let ss = secs % 60;
    format!("{y:04}-{m:02}-{d:02}T{hh:02}:{mm:02}:{ss:02}.{ms:03}Z")
}

/// Howard Hinnant's `civil_from_days`: days since 1970-01-01 → (year, month,
/// day) in the proleptic Gregorian calendar.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719468;
    let era = if z >= 0 { z } else { z - 146096 } / 146097;
    let doe = z - era * 146097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m: i64 = if mp < 10 { mp + 3 } else { mp - 9 }; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m as u32, d)
}
