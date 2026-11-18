//! KEEP_OVERLAY (`QUEEN_RAFT_KEEP_OVERLAY`) — THE GATE.
//!
//! The planner thread keeps the overlay BETWEEN cycles and takes out, per
//! landed entry, exactly what that entry put in
//! ([`crate::rsm::planner::kept`]). What must hold:
//!
//! 1. After EVERY cycle of a mixed workload the kept overlay equals — field by
//!    field, and tag by tag through the entry each tag names — the overlay the
//!    old path rebuilds from scratch. The check runs INSIDE the real
//!    `plan_cycle_blocking` (`KeepCfg::verify`) at the point the planner is
//!    about to use the state. The workload: pushes that create queues and
//!    partitions and hit a dedup window, the consumption engine's checkpoints
//!    (cursor rows, leases, dead letters, group registrations) on live and on
//!    deleted partitions, transactions (committed, rolled back on a duplicate,
//!    DLQ-replay ones that answer empty, consumer-group commits of cursor rows
//!    behind a KV fence), KV puts, incrs, deletes and the expiry sweep, timer
//!    schedules and cancels with a fire step whose fires fail, back off and
//!    dead-letter, group deletes, watermarks, queue deletes and tenant purges
//!    built as the facade builds them (garbage and a first chunk in one entry)
//!    and resumed chunk by chunk, retention's partition deletes, same-cycle
//!    retries and request-id replays of commands in flight, committed, or
//!    expired. Every entry is applied by the real applier, so a command
//!    planned against a partition a delete in flight takes away (an append or
//!    a cursor on a missing row, fatal in apply) fails the gate too. The
//!    gate asserts it proposed every effect kind it names. The entries land
//!    OUT OF STEP with planning: between cycles a random number of the oldest
//!    unapplied entries is applied (none, some, all), and an entry that landed
//!    leaves the in-flight list only some cycles later — the driver drops an
//!    entry once it is RESOLVED, which lags.
//! 2. The same command stream planned with the knob ON and OFF proposes
//!    BYTE-IDENTICAL entries (uuids made deterministic for both runs), and the
//!    two stores end on the same digest.
//! 3. The safety valves: a new epoch drops the kept state, an entry planned
//!    and never proposed is caught, and a cycle whose folds do not match its
//!    entry (the DLQ-replay transaction that answers empty WITHOUT restoring
//!    the overlay) is not kept — each followed by a cycle that plans from a
//!    rebuild that matches. And the knob off keeps nothing; a periodic reset
//!    rebuilds and still matches.
//!
//! The gate was checked against deliberate bugs in the kept path — a
//! `created` flag not dropped when its create lands, dedup occurrences not
//! removed, a timer chain not re-folded, a scalar kept monotone instead of
//! recomputed, the stray-fold check off: every one fails it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use serde_json::json;

use crate::rsm::apply::{Applier, Committed as ApplyCommitted, NoNotify};
use crate::rsm::batcher::{
    plan_cycle_blocking, Command, Forced, KeepCfg, KeepStats, PlanOutput, PlannedCounts,
    PlannerState,
};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{CursorRow, Effect, GarbageScope, GroupMeta, Pid, QueueConfig};
use crate::rsm::entry::{encode_entry, AckOutcome, AckResult, Entry, Outcome, RequestId};
use crate::rsm::planner::kv::parse_ops;
use crate::rsm::planner::timers::{parse_timer_ops, TimerFireConfig, TimersCommand};
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{EffectsCommand, KvCommand, PlanConfig, PushCommand, PushItem};
use crate::rsm::store::{keys, rows, Keyspace, Reads, Store, TypedReads};

use super::apply::{cfg, seg_opts, Node};
use super::planner_harness::{qcfg, BASE_US, TENANT};

// ---------------------------------------------------------------------------
// A deterministic generator
// ---------------------------------------------------------------------------

/// xorshift64*: the same seed, the same stream, on every platform.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Rng {
        Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1)
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }

    fn chance(&mut self, pct: u64) -> bool {
        self.below(100) < pct
    }

    fn pick<'a>(&mut self, xs: &'a [&'a str]) -> &'a str {
        xs[self.below(xs.len() as u64) as usize]
    }
}

const QUEUES: [&str; 4] = ["q0", "q1", "qd", "qw"];
const GROUPS: [&str; 2] = ["g1", "g2"];
const WORKERS: [&str; 3] = ["w0", "w1", "w2"];
/// Timers fire into `qt`; every fire into `qtf` fails (the fire config's test
/// hook), so those back off and finally dead-letter.
const TIMER_QUEUES: [&str; 2] = ["qt", "qtf"];

/// The config a queue is created with: `q1` is discoverable, `qd` dedups, `qw`
/// delays visibility.
fn queue_cfg(queue: &str) -> QueueConfig {
    let mut c = qcfg();
    c.id[1] = queue
        .bytes()
        .fold(0u8, |a, b| a.wrapping_mul(31).wrapping_add(b));
    match queue {
        "q1" => {
            c.namespace = Some("ns".into());
            c.task = Some("t".into());
        }
        "qd" => c.dedup_window_seconds = 60,
        "qw" => c.delayed_processing = 1,
        _ => {}
    }
    c
}

fn rid_of(n: u64) -> RequestId {
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&n.to_be_bytes());
    id[8] = 0xA5;
    id
}

/// One message's frame as the receiver packs it. A real frame, not any bytes:
/// the scheduler reads its flags, and a frame that looked like a link would
/// plan its push alone.
fn frame_of(txn: &str) -> bytes::Bytes {
    let mut id = [0u8; 16];
    id.copy_from_slice(&crate::util::txn_hash128(txn));
    crate::frames::pack_frames(&[crate::frames::FrameIn {
        message_id: id,
        txn,
        trace_id: None,
        producer_sub: None,
        payload: format!("{{\"t\":\"{txn}\"}}").as_bytes(),
        encrypted: false,
        headers: None,
        link: None,
    }])
    .into()
}

/// The adaptive command stream: it learns partitions and their tails from the
/// entries the planner proposes, so its checkpoints target real state. Everything it decides comes from its seeded [`Rng`] and
/// from those entries, so two runs that propose the same entries generate the
/// same stream.
struct Workload {
    rng: Rng,
    next_id: u64,
    next_txn: u64,
    recent_txns: Vec<String>,
    submitted: Vec<Command>,
    /// pid → (queue, partition), from `PartitionCreate`.
    pid_queue: HashMap<Pid, (String, String)>,
    /// pid → its last offset, from every `Append`.
    tails: HashMap<Pid, u64>,
    /// Partitions a delete took away: checkpoints still name some of them
    /// (the engine's view lags), and the planner must refuse those.
    gone: Vec<Pid>,
    /// The next dead-letter id.
    next_dlq: u64,
    /// The highest watermark emitted per pid (they never move back).
    watermark: HashMap<Pid, u64>,
    /// Batches of many commands over many partitions and keys, the admin
    /// kinds rare: what the footprint scheduler splits into many groups.
    wide: bool,
}

impl Workload {
    fn new(seed: u64) -> Workload {
        Workload {
            rng: Rng::new(seed),
            next_id: 1,
            next_txn: 1,
            recent_txns: Vec::new(),
            submitted: Vec::new(),
            pid_queue: HashMap::new(),
            tails: HashMap::new(),
            gone: Vec::new(),
            next_dlq: 1,
            watermark: HashMap::new(),
            wide: false,
        }
    }

    fn wide(seed: u64) -> Workload {
        Workload {
            wide: true,
            ..Workload::new(seed)
        }
    }

    /// How many partitions (and KV keys) a draw picks from.
    fn spread(&self, narrow: u64) -> u64 {
        if self.wide {
            narrow * 12
        } else {
            narrow
        }
    }

    fn id(&mut self) -> RequestId {
        let id = rid_of(self.next_id);
        self.next_id += 1;
        id
    }

    fn txn(&mut self) -> String {
        if !self.recent_txns.is_empty() && self.rng.chance(20) {
            let i = self.rng.below(self.recent_txns.len() as u64) as usize;
            return self.recent_txns[i].clone();
        }
        let t = format!("tx{}", self.next_txn);
        self.next_txn += 1;
        self.recent_txns.push(t.clone());
        if self.recent_txns.len() > 32 {
            self.recent_txns.remove(0);
        }
        t
    }

    fn push_cmd(&mut self, queue: &str, partition: &str, n: usize) -> PushCommand {
        let items = (0..n)
            .map(|_| {
                let t = self.txn();
                PushItem {
                    hash: crate::util::txn_hash128(&t),
                    frame: frame_of(&t),
                }
            })
            .collect();
        PushCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            queue: queue.to_string(),
            partition: partition.to_string(),
            items,
            create_cfg: queue_cfg(queue),
            links: Default::default(),
        }
    }

    fn push(&mut self) -> Command {
        let queue = self.rng.pick(&QUEUES).to_string();
        let partition = format!("p{}", self.rng.below(self.spread(5)));
        let n = 1 + self.rng.below(4) as usize;
        Command::Push(self.push_cmd(&queue, &partition, n))
    }

    /// A partition the workload knows, now and then one a delete took away.
    fn some_pid(&mut self) -> Option<Pid> {
        if !self.gone.is_empty() && self.rng.chance(10) {
            let i = self.rng.below(self.gone.len() as u64) as usize;
            return Some(self.gone[i]);
        }
        let mut pids: Vec<Pid> = self.pid_queue.keys().copied().collect();
        if pids.is_empty() {
            return None;
        }
        pids.sort_unstable();
        Some(pids[self.rng.below(pids.len() as u64) as usize])
    }

    /// A cursor row as the engine checkpoints it: somewhere in the partition's
    /// retained range, now and then with a live lease on a batch past it.
    fn cursor_row(&mut self, pid: Pid) -> CursorRow {
        let tail = self.tails.get(&pid).copied().unwrap_or(0) as i64;
        let committed = self.rng.below(tail as u64 + 2) as i64 - 1;
        let leased = committed < tail && self.rng.chance(40);
        CursorRow {
            committed,
            batch_end: leased.then_some(tail as u64),
            worker: leased.then(|| self.rng.pick(&WORKERS).to_string()),
            lease_expires_at_us: leased.then_some(BASE_US + 3_600_000_000),
            lease_acquired_at_us: leased.then_some(BASE_US),
            batch_retry_count: self.rng.below(2) as u32,
            attempt_offset: None,
            attempt_count: 0,
            total_consumed: committed.max(0) as u64,
            lease_conflated: false,
            delivered: Vec::new(),
            created_at_us: BASE_US,
            metadata: String::new(),
            released: None,
        }
    }

    /// The cursor rows (and now and then a dead letter) of one engine
    /// checkpoint over 1-3 partitions.
    fn checkpoint_effects(&mut self) -> Vec<Effect> {
        let mut effects = Vec::new();
        for _ in 0..1 + self.rng.below(3) {
            let Some(pid) = self.some_pid() else {
                break;
            };
            let group = self.rng.pick(&GROUPS).to_string();
            let row = self.cursor_row(pid);
            if self.rng.chance(15) {
                let queue = self
                    .pid_queue
                    .get(&pid)
                    .map(|(q, _)| q.clone())
                    .unwrap_or_else(|| "q0".to_string());
                let mut dlq_id = [0u8; 16];
                dlq_id[..8].copy_from_slice(&self.next_dlq.to_be_bytes());
                dlq_id[8] = 0xD1;
                self.next_dlq += 1;
                effects.push(Effect::DlqInsert {
                    dlq_id,
                    tenant: TENANT.to_string(),
                    queue,
                    pid,
                    group: group.clone(),
                    offset: row.committed.max(0),
                    message_id: None,
                    txn: format!("poison{}", self.next_dlq),
                    payload: b"{}".to_vec(),
                    error: "boom".into(),
                    retry_count: 3,
                    failed_at_us: BASE_US,
                    headers: Vec::new(),
                });
            }
            effects.push(Effect::CursorSet { pid, group, row });
        }
        effects
    }

    /// An engine checkpoint: one effect command of cursor rows.
    fn checkpoint(&mut self) -> Option<Command> {
        let effects = self.checkpoint_effects();
        if effects.is_empty() {
            return None;
        }
        Some(Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects,
        }))
    }

    /// A group's first contact, registered by the engine.
    fn register(&mut self) -> Command {
        let queue = self.rng.pick(&QUEUES).to_string();
        let group = self.rng.pick(&GROUPS).to_string();
        Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects: vec![Effect::GroupUpsert {
                tenant: TENANT.to_string(),
                queue,
                group,
                meta: GroupMeta {
                    id: [7; 16],
                    partition_name: String::new(),
                    namespace: String::new(),
                    task: String::new(),
                    mode: crate::rsm::effect::SubscriptionMode::All,
                    subscription_timestamp_us: 0,
                    conflation: false,
                    seeded: false,
                    registered_at_us: BASE_US,
                },
            }],
        })
    }

    /// A consumer-group commit: the engine's cursor rows and its per-target
    /// results riding a transaction, behind a KV fence now and then.
    fn commit(&mut self) -> Option<Command> {
        let extra_effects = self.checkpoint_effects();
        if extra_effects.is_empty() {
            return None;
        }
        let results = extra_effects
            .iter()
            .filter_map(|e| match e {
                Effect::CursorSet { pid, row, .. } => Some(AckResult {
                    pid: *pid,
                    committed: row.committed,
                    acked: 1,
                    conflated: 0,
                    dlq: 0,
                    lease_released: row.worker.is_none(),
                    batch_retry_count: row.batch_retry_count,
                    noop_hashes: Vec::new(),
                    stale_hashes: Vec::new(),
                }),
                _ => None,
            })
            .collect();
        let kv = if self.rng.chance(50) {
            let k = format!("fence{}", self.rng.below(2));
            parse_ops(
                &[json!({"op":"put","ns":"fence","key":k,"value":{"n":1},"forever":true})],
                TENANT,
                true,
                511,
                crate::rsm::planner::kv::MAX_VALUE_BYTES_DEFAULT,
            )
            .expect("kv op")
        } else {
            Vec::new()
        };
        Some(Command::Transaction(TxnCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            pushes: Vec::new(),
            acks: Vec::new(),
            positional_acks: Vec::new(),
            kv,
            timers: Vec::new(),
            extra_effects,
            allow_duplicate: false,
            positions: Vec::new(),
            engine_acks: AckOutcome { results },
        }))
    }

    fn transaction(&mut self) -> Command {
        // Now and then a transaction that only reads, of keys whose homes
        // differ: it logs one no-op as its position, by the single planner
        // and by the shares alike.
        if self.rng.chance(8) {
            let ks: Vec<String> = (0..2)
                .map(|_| format!("p{}", self.rng.below(self.spread(3))))
                .collect();
            let ops = vec![
                json!({"op":"get","ns":"txn","key":format!("tk{}", self.rng.below(3))}),
                json!({"op":"getMany","ns":"txn","keys":ks}),
            ];
            let kv = parse_ops(
                &ops,
                TENANT,
                true,
                511,
                crate::rsm::planner::kv::MAX_VALUE_BYTES_DEFAULT,
            )
            .expect("kv op");
            return Command::Transaction(TxnCommand {
                request_id: self.id(),
                tenant: TENANT.to_string(),
                pushes: Vec::new(),
                acks: Vec::new(),
                positional_acks: Vec::new(),
                kv,
                timers: Vec::new(),
                extra_effects: Vec::new(),
                allow_duplicate: false,
                positions: Vec::new(),
                engine_acks: AckOutcome::default(),
            });
        }
        let mut pushes = vec![{
            let q = if self.rng.chance(60) { "qd" } else { "q0" };
            let p = format!("p{}", self.rng.below(self.spread(3)));
            let n = 1 + self.rng.below(2) as usize;
            self.push_cmd(q, &p, n)
        }];
        if self.rng.chance(50) {
            let p = format!("p{}", self.rng.below(self.spread(3)));
            pushes.push(self.push_cmd("qd", &p, 1));
        }
        // The consumption half, as the engine hands it over: cursor rows of
        // the bundle's acks as riders.
        let extra_effects = if self.rng.chance(40) {
            self.checkpoint_effects()
        } else {
            Vec::new()
        };
        let kv = if self.rng.chance(30) {
            // One key; now and then a few, of several entities, whose homes
            // differ: a counter under a partition's name, and a gate that
            // holds once (`required`: lost, it rolls the whole transaction
            // back).
            let k = format!("tk{}", self.rng.below(3));
            let mut ops =
                vec![json!({"op":"put","ns":"txn","key":k,"value":{"n":1},"forever":true})];
            if self.rng.chance(50) {
                let k = format!("p{}", self.rng.below(self.spread(3)));
                ops.push(json!({"op":"incr","ns":"txn","key":k,"delta":1,"forever":true}));
            }
            if self.rng.chance(30) {
                let k = format!("g{}", self.rng.below(self.spread(6)));
                ops.push(json!({
                    "op":"putIfAbsent","ns":"gate","key":k,"value":{"n":1},
                    "forever":true,"required":true,
                }));
            }
            // A read now and then, first: of a key the transaction may write
            // after it (answered at plan time) or of one it does not.
            if self.rng.chance(40) {
                let k = format!("tk{}", self.rng.below(3));
                ops.insert(0, json!({"op":"get","ns":"txn","key":k}));
            }
            parse_ops(
                &ops,
                TENANT,
                true,
                511,
                crate::rsm::planner::kv::MAX_VALUE_BYTES_DEFAULT,
            )
            .expect("kv op")
        } else {
            Vec::new()
        };
        // The step's timer now and then: scheduled or cancelled with it.
        let timers = if self.rng.chance(20) {
            let queue = self.rng.pick(&TIMER_QUEUES).to_string();
            let key = format!("t{}", self.rng.below(4));
            let op = if self.rng.chance(25) {
                json!({"op":"cancel","queue":queue,"timerKey":key})
            } else {
                let t = self.txn();
                // The message id as the HTTP edge mints it (see `timers`).
                let mid = format!("01910000-0000-7000-8000-{:012x}", self.next_id);
                json!({
                    "op":"schedule","queue":queue,"timerKey":key,
                    "delayMs": self.rng.below(400) as i64,
                    "txn": t, "payload": "e30=", "_messageId": mid,
                })
            };
            parse_timer_ops(&[op], Some("svc")).expect("timer op")
        } else {
            Vec::new()
        };
        Command::Transaction(TxnCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            pushes,
            acks: Vec::new(),
            positional_acks: Vec::new(),
            kv,
            timers,
            extra_effects,
            // The DLQ-replay shape: a duplicate answers empty, and the pushes
            // before it are taken back out of the overlay.
            allow_duplicate: self.rng.chance(25),
            positions: Vec::new(),
            engine_acks: AckOutcome::default(),
        })
    }

    fn kv(&mut self) -> Command {
        let k = format!("k{}", self.rng.below(self.spread(5)));
        let op = match self.rng.below(4) {
            0 => json!({"op":"put","ns":"n","key":k,"value":{"v":1},"ttlSeconds":1}),
            1 => json!({"op":"put","ns":"n","key":k,"value":{"v":2},"forever":true}),
            2 => {
                json!({"op":"incr","ns":"n","key":format!("c{}", self.rng.below(3)),"delta":1,"forever":true})
            }
            _ => json!({"op":"delete","ns":"n","key":k}),
        };
        Command::Kv(KvCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            ops: parse_ops(
                &[op],
                TENANT,
                false,
                511,
                crate::rsm::planner::kv::MAX_VALUE_BYTES_DEFAULT,
            )
            .expect("kv op"),
        })
    }

    fn timers(&mut self) -> Command {
        let queue = self.rng.pick(&TIMER_QUEUES).to_string();
        let key = format!("t{}", self.rng.below(4));
        let op = if self.rng.chance(25) {
            json!({"op":"cancel","queue":queue,"timerKey":key})
        } else {
            let t = self.txn();
            // The message id as the HTTP edge mints it, from this command's
            // id: a planner that mints one draws it in planning order, which
            // the scheduler's groups do not share.
            let mid = format!("01910000-0000-7000-8000-{:012x}", self.next_id);
            json!({
                "op":"schedule","queue":queue,"timerKey":key,
                "delayMs": self.rng.below(400) as i64,
                "txn": t, "payload": "e30=", "_messageId": mid,
            })
        };
        Command::Timers(TimersCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            ops: parse_timer_ops(&[op], Some("svc")).expect("timer op"),
        })
    }

    fn group_delete(&mut self) -> Command {
        let queue = self.rng.pick(&QUEUES).to_string();
        Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects: vec![Effect::GroupDelete {
                tenant: TENANT.to_string(),
                queue,
                group: self.rng.pick(&GROUPS).to_string(),
            }],
        })
    }

    /// A watermark on a partition the store has committed frames for, above
    /// every watermark emitted for it so far (they never move back).
    fn watermark_cmd(&mut self, store: &crate::rsm::store::HeedStore) -> Option<Command> {
        let pids: Vec<Pid> = {
            let mut v: Vec<Pid> = self.pid_queue.keys().copied().collect();
            v.sort_unstable();
            v
        };
        if pids.is_empty() {
            return None;
        }
        let pid = pids[self.rng.below(pids.len() as u64) as usize];
        let row = store.read(|r| r.partition(pid)).expect("read partition")?;
        let floor = row
            .log_start
            .max(self.watermark.get(&pid).copied().unwrap_or(0));
        let top = (row.last_offset + 1).max(0) as u64;
        if top <= floor {
            return None;
        }
        let w = floor + 1 + self.rng.below(top - floor);
        self.watermark.insert(pid, w);
        Some(Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects: vec![Effect::Watermark {
                pid,
                log_start: w,
                txns_start: w,
                rows: None,
            }],
        }))
    }

    /// A queue delete as the facade builds it (`api_delete_queue`): the pids
    /// read from committed state, their `GarbageAdd` and a first chunk in the
    /// same entry. The chunk is small, so a partition often outlives it and a
    /// resume finishes it. One in four is a purge of the whole tenant
    /// (`api_delete_tenant`).
    fn queue_delete(&mut self, store: &crate::rsm::store::HeedStore) -> Command {
        let purge = self.rng.chance(25);
        let queue = self.rng.pick(&QUEUES).to_string();
        let pids = store
            .read(|r| {
                let mut queues = Vec::new();
                if purge {
                    r.scan_queues(TENANT, usize::MAX, &mut |q, _| {
                        queues.push(q.to_string());
                        true
                    })?;
                } else {
                    queues.push(queue.clone());
                }
                let mut pids = Vec::new();
                for q in &queues {
                    r.scan_queue_partitions(TENANT, q, None, usize::MAX, &mut |pid| {
                        pids.push(pid);
                        true
                    })?;
                }
                Ok(pids)
            })
            .expect("read the partitions to delete");
        let (head, scope) = if purge {
            (
                Effect::TenantPurge {
                    tenant: TENANT.to_string(),
                },
                GarbageScope::Tenant,
            )
        } else {
            (
                Effect::QueueDelete {
                    tenant: TENANT.to_string(),
                    queue,
                },
                GarbageScope::Queue,
            )
        };
        let mut effects = vec![head];
        if !pids.is_empty() {
            effects.push(Effect::GarbageAdd {
                pids: pids.clone(),
                scope: scope.clone(),
                deleted_at_us: BASE_US,
            });
            effects.push(Effect::DeleteChunk {
                pids,
                scope,
                resume: Vec::new(),
                limit: 1 + self.rng.below(24) as u32,
            });
        }
        Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects,
        })
    }

    /// Retention's partition delete (006) of a partition the workload made.
    fn partition_delete(&mut self) -> Option<Command> {
        let mut pids: Vec<Pid> = self.pid_queue.keys().copied().collect();
        if pids.is_empty() {
            return None;
        }
        pids.sort_unstable();
        let pid = pids[self.rng.below(pids.len() as u64) as usize];
        Some(Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects: vec![Effect::PartitionDelete { pid }],
        }))
    }

    /// The resume of the deletes still open in committed state (the facade's
    /// chunk loop, the leader's maintenance): one small chunk per marker, in
    /// the marker's own scope.
    fn delete_resume(&mut self, store: &crate::rsm::store::HeedStore) -> Option<Command> {
        let markers = store
            .read(|r| {
                let mut out: Vec<(Pid, GarbageScope)> = Vec::new();
                r.scan_raw(Keyspace::Garbage, &[], &[], 8, &mut |k, v| {
                    if let (Some(pid), Ok(row)) = (keys::pid_of(k), rows::garbage_decode(v)) {
                        out.push((pid, row.scope));
                    }
                    true
                })?;
                Ok(out)
            })
            .expect("read the garbage markers");
        if markers.is_empty() {
            return None;
        }
        let effects = markers
            .into_iter()
            .map(|(pid, scope)| Effect::DeleteChunk {
                pids: vec![pid],
                scope,
                resume: Vec::new(),
                limit: 1 + self.rng.below(24) as u32,
            })
            .collect();
        Some(Command::Effects(EffectsCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            effects,
        }))
    }

    /// One cycle's batch.
    fn batch(&mut self, store: &crate::rsm::store::HeedStore) -> Vec<Command> {
        let n = if self.wide {
            8 + self.rng.below(40)
        } else {
            1 + self.rng.below(7)
        };
        let mut out: Vec<Command> = Vec::new();
        for _ in 0..n {
            // Wide: the admin kinds (83..=91) a tenth as often.
            let mut roll = self.rng.below(100);
            if self.wide && (83..=91).contains(&roll) && !self.rng.chance(10) {
                roll = self.rng.below(83);
            }
            let c = match roll {
                0..=34 => Some(self.push()),
                35..=52 => self.checkpoint(),
                53..=55 => Some(self.register()),
                56..=60 => self.commit(),
                61..=68 => Some(self.transaction()),
                69..=75 => Some(self.kv()),
                76..=82 => Some(self.timers()),
                83..=84 => Some(self.group_delete()),
                85..=86 => self.watermark_cmd(store),
                87..=88 => Some(self.queue_delete(store)),
                89 => self.partition_delete(),
                90..=91 => self.delete_resume(store),
                // A replay of anything submitted before: in flight, committed,
                // or never logged — the request-id lookup decides. (Not the raw
                // effects: once the short request-id window below has expired
                // its id, a replayed watermark would move one back.)
                _ => {
                    if self.submitted.is_empty() {
                        None
                    } else {
                        let i = self.rng.below(self.submitted.len() as u64) as usize;
                        match &self.submitted[i] {
                            Command::Effects(_) => None,
                            c => Some(c.clone()),
                        }
                    }
                }
            };
            if let Some(c) = c {
                // A same-cycle retry now and then.
                if self.rng.chance(4) {
                    out.push(c.clone());
                }
                out.push(c);
            }
        }
        for c in &out {
            self.submitted.push(c.clone());
        }
        out
    }

    /// Learn from a proposed entry: partitions and their tails.
    fn observe(&mut self, e: &Entry) {
        for eff in &e.effects {
            match eff {
                Effect::PartitionCreate {
                    pid,
                    queue,
                    partition,
                    ..
                } => {
                    self.pid_queue
                        .insert(*pid, (queue.clone(), partition.clone()));
                }
                Effect::Append {
                    pid,
                    base_offset,
                    count,
                    ..
                } => {
                    self.tails.insert(*pid, base_offset + *count as u64 - 1);
                }
                // Gone for the watermarks; checkpoints still name them now
                // and then, and must be refused.
                Effect::GarbageAdd { pids, scope, .. }
                    if !matches!(scope, GarbageScope::Group { .. }) =>
                {
                    for pid in pids {
                        if self.pid_queue.remove(pid).is_some() {
                            self.gone.push(*pid);
                        }
                    }
                }
                Effect::PartitionDelete { pid } => {
                    if self.pid_queue.remove(pid).is_some() {
                        self.gone.push(*pid);
                    }
                }
                _ => {}
            }
        }
        if self.gone.len() > 64 {
            self.gone.drain(0..16);
        }
    }
}

/// The leader steps a cycle runs: the timer fire, the KV sweep, the request-id
/// expiry — each on its own beat.
fn steps(cycle: usize) -> Steps {
    Steps {
        fire: cycle.is_multiple_of(3).then(|| TimerFireConfig {
            max_attempts: 3,
            backoff_min_ms: 1,
            backoff_max_ms: 20,
            transient_backoff_ms: 5,
            fail_queue: Some("qtf".into()),
            ..TimerFireConfig::default()
        }),
        kv_sweep: cycle.is_multiple_of(5).then_some(4),
        expire: cycle.is_multiple_of(11).then_some(3_000_000),
    }
}

struct Steps {
    fire: Option<TimerFireConfig>,
    kv_sweep: Option<usize>,
    expire: Option<i64>,
}

// ---------------------------------------------------------------------------
// The rig: the batcher's planning step, a real store, the real applier
// ---------------------------------------------------------------------------

struct Rig {
    node: Node,
    state: PlannerState,
    keep: KeepCfg,
    front: Arc<DedupFront>,
    /// Planning threads: more than one plans through the footprint scheduler.
    threads: usize,
    /// The driver's in-flight list: `(index, entry)`, dropped lazily.
    inflight: VecDeque<(u64, Arc<Entry>)>,
    /// Proposed and not yet applied.
    unapplied: VecDeque<(u64, Arc<Entry>)>,
    next_index: u64,
    applied: u64,
    wall: i64,
    rng: Rng,
    /// Every proposed entry, encoded, in log order.
    log: Vec<Vec<u8>>,
    cycles: usize,
    /// How many effects of each kind were proposed (the gate's coverage).
    kinds: BTreeMap<String, usize>,
    /// Cycles whose planning read had entries in flight above the applied
    /// index, and cycles in which some landed entry was still in the list.
    overlapped: usize,
    lingering: usize,
    /// Cycles checked against the single planner ([`Rig::check_reference`]):
    /// every cycle of a rig that plans on more than one thread.
    checked: usize,
}

impl Rig {
    fn new(tag: &str, keep: KeepCfg, front: DedupFront, seed: u64, threads: usize) -> Rig {
        Rig {
            node: Node::new(tag),
            state: PlannerState::default(),
            keep,
            front: Arc::new(front),
            threads,
            inflight: VecDeque::new(),
            unapplied: VecDeque::new(),
            next_index: 1,
            applied: 0,
            wall: BASE_US,
            rng: Rng::new(seed ^ 0x5EED),
            log: Vec::new(),
            cycles: 0,
            kinds: BTreeMap::new(),
            overlapped: 0,
            lingering: 0,
            checked: 0,
        }
    }

    /// THE ORACLE of the homes: the single planner, from the same state, plans
    /// the same commands in the order the homes planned them (each run: the
    /// homes' commands in batch order, then the commands that waited for
    /// them), and must plan the SAME ENTRY, byte for byte but for the uuids
    /// the planners mint. So planning on several threads is one valid serial
    /// order of the batch, and nothing else.
    ///
    /// The reference keeps nothing between cycles (it rebuilds its overlay
    /// from the entries in flight) and has no dedup front: neither may change
    /// what is planned.
    fn check_reference(
        &mut self,
        folded: Vec<(u64, Arc<Entry>)>,
        batch: Vec<Command>,
        order: Vec<usize>,
        steps: &Steps,
        got: Option<&Entry>,
    ) {
        let ids: Vec<RequestId> = batch.iter().map(Command::request_id).collect();
        let mut left: Vec<Option<Command>> = batch.into_iter().map(Some).collect();
        let mut reordered: Vec<Command> = Vec::with_capacity(left.len());
        for i in order {
            let c = left[i]
                .take()
                .unwrap_or_else(|| panic!("cycle {}: position {i} planned twice", self.cycles));
            reordered.push(c);
            // A second copy of the command in this batch (a client's retry)
            // stays right behind it, as it was in the batch: the scheduler
            // answers it with the first copy's entry or sends it to the next
            // cycle (S5), and the single planner plans it there to the same
            // answer.
            for j in i + 1..left.len() {
                if ids[j] == ids[i] {
                    if let Some(copy) = left[j].take() {
                        reordered.push(copy);
                    }
                }
            }
        }
        // What is left was answered from an earlier entry (its request id was
        // seen before): where it stands changes nothing.
        reordered.extend(left.into_iter().flatten());
        let front = Arc::new(DedupFront::disabled());
        let mut state = PlannerState::default();
        let off = KeepCfg {
            enabled: false,
            epoch: 0,
            verify: false,
            reset_every: 0,
        };
        let want = plan_cycle_blocking(
            self.node.store_arc(),
            &front,
            &mut state,
            off,
            1,
            None,
            None,
            folded,
            reordered,
            Rig::plan_cfg(),
            self.wall,
            steps.expire,
            steps.kv_sweep,
            steps.fire.clone(),
            None,
            None,
        )
        .expect("the reference cycle");
        let show = |e: Option<&Entry>| e.map(|e| mask_uuids(&format!("{e:?}")));
        let (a, b) = (show(want.entry.as_deref()), show(got));
        if a != b {
            let (a, b) = (a.unwrap_or_default(), b.unwrap_or_default());
            let at = a
                .bytes()
                .zip(b.bytes())
                .position(|(x, y)| x != y)
                .unwrap_or(a.len().min(b.len()));
            let from = at.saturating_sub(400);
            panic!(
                "cycle {}: the homes did not plan the single planner's entry:\n single: ...{}\n  homes: ...{}",
                self.cycles,
                &a[from..(at + 400).min(a.len())],
                &b[from..(at + 400).min(b.len())],
            );
        }
        self.checked += 1;
    }

    fn plan_cfg() -> PlanConfig {
        // No budget cut: a time-based cut would make the two runs of the A/B
        // defer different commands.
        PlanConfig {
            plan_budget_ms: 3_600_000,
            index_mode: RIG_INDEX.with(|c| c.get()),
            ..PlanConfig::default()
        }
    }

    /// One planning cycle exactly as the driver runs it: the in-flight list as
    /// folded, the batch, the leader steps; then the landed prefix dropped
    /// (lazily: resolution lags) and the new entry proposed.
    fn cycle(&mut self, batch: Vec<Command>, steps: &Steps) -> PlanOutput {
        self.cycles += 1;
        let folded: Vec<(u64, Arc<Entry>)> = self.inflight.iter().cloned().collect();
        if folded.iter().any(|(i, _)| *i > self.applied) {
            self.overlapped += 1;
        }
        if folded.iter().any(|(i, _)| *i <= self.applied) {
            self.lingering += 1;
        }
        // For the oracle: the cycle's inputs, and the order it plans in.
        let reference = (self.threads > 1).then(|| (folded.clone(), batch.clone()));
        let _ = crate::rsm::batcher::take_planned_order();
        let out = plan_cycle_blocking(
            self.node.store_arc(),
            &self.front,
            &mut self.state,
            self.keep,
            self.threads,
            None,
            None,
            folded,
            batch,
            Rig::plan_cfg(),
            self.wall,
            steps.expire,
            steps.kv_sweep,
            steps.fire.clone(),
            None,
            None,
        )
        .expect("plan cycle");
        // A planning thread lets go of the store before it answers: once the
        // cycle is back, the store is its caller's alone, and a caller that
        // closes it (this rig at its end) finds it whole. A clone dropped after
        // the answer made the close race the threads ("the store is still
        // shared at close", CI 2026-11-18).
        assert_eq!(
            Arc::strong_count(self.node.store_arc()),
            1,
            "cycle {}: a planning thread still holds the store after the cycle answered",
            self.cycles
        );
        assert!(
            self.state.stats.mismatches.is_empty(),
            "cycle {}: the kept state differs from a rebuild:\n{:#?}",
            self.cycles,
            self.state.stats.mismatches
        );
        assert_eq!(
            out.store_applied, self.applied,
            "the planner read the applied index"
        );
        if let Some((folded, batch)) = reference {
            let order = crate::rsm::batcher::take_planned_order();
            self.check_reference(folded, batch, order, steps, out.entry.as_deref());
        }
        while let Some((index, _)) = self.inflight.front() {
            if *index <= out.store_applied && self.rng.chance(60) {
                self.inflight.pop_front();
            } else {
                break;
            }
        }
        if let Some(e) = &out.entry {
            let index = self.next_index;
            self.next_index += 1;
            for eff in &e.effects {
                *self.kinds.entry(format!("{:?}", eff.kind())).or_default() += 1;
            }
            self.log.push(encode_entry(e).expect("encode"));
            self.inflight.push_back((index, e.clone()));
            self.unapplied.push_back((index, e.clone()));
        }
        self.wall += 20_000 + self.rng.below(40_000) as i64;
        out
    }

    /// Apply the `n` oldest unapplied entries through the real applier, then
    /// commit, as the apply thread does between cycles.
    fn apply(&mut self, n: usize) {
        if n == 0 || self.unapplied.is_empty() {
            return;
        }
        let (mut a, _) = Applier::open(
            self.node.store(),
            &self.node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(NoNotify),
        )
        .expect("open applier");
        for _ in 0..n {
            let Some((index, e)) = self.unapplied.pop_front() else {
                break;
            };
            a.apply(&ApplyCommitted {
                index,
                term: 1,
                entry: e,
            })
            .unwrap_or_else(|err| panic!("apply entry {index}: {err:?}"));
            self.applied = index;
        }
        a.durable_point().expect("durable point");
    }

    /// Land a random number of the oldest unapplied entries — none, some, or
    /// all — never letting more than a pipeline's worth stay unapplied.
    fn land_some(&mut self) {
        let pending = self.unapplied.len() as u64;
        let n = match self.rng.below(10) {
            0..=2 => 0,
            3..=6 => self.rng.below(pending + 1),
            _ => pending,
        };
        let n = if pending.saturating_sub(n) > 5 {
            pending - 5
        } else {
            n
        };
        self.apply(n as usize);
    }
}

/// The verifying knob-on config: compare with a rebuild every cycle.
fn keep_verified() -> KeepCfg {
    KeepCfg {
        enabled: true,
        epoch: 0,
        verify: true,
        reset_every: 0,
    }
}

thread_local! {
    /// The dedup index the rigs of this thread plan with: `rows`, the test
    /// default, unless a test asks for the product's ([`TxnsIndex`]). Apply
    /// writes the txns rows in both, so either planner finds what committed.
    static RIG_INDEX: std::cell::Cell<crate::rsm::dedup::IndexMode> =
        const { std::cell::Cell::new(crate::rsm::dedup::IndexMode::Rows) };
}

/// While it lives, the rigs of this thread plan with the `txns` dedup index,
/// the product default: the dedup front then plans every probe (skip, bands,
/// whole window), which `rows` does not do.
struct TxnsIndex;

impl TxnsIndex {
    fn on() -> TxnsIndex {
        RIG_INDEX.with(|c| c.set(crate::rsm::dedup::IndexMode::Txns));
        TxnsIndex
    }
}

impl Drop for TxnsIndex {
    fn drop(&mut self) {
        RIG_INDEX.with(|c| c.set(crate::rsm::dedup::IndexMode::Rows));
    }
}

/// What one run of the workload produced.
struct Report {
    /// Every proposed entry, encoded, in log order.
    log: Vec<Vec<u8>>,
    stats: KeepStats,
    digest: crate::rsm::apply::StateDigest,
    kinds: BTreeMap<String, usize>,
    overlapped: usize,
    lingering: usize,
    planned: PlannedCounts,
    /// The homes the scheduler planned (each a planning thread in its shards).
    bins: u64,
    /// Cycles checked against the single planner.
    checked: usize,
    cycles: usize,
    /// The transactions several homes planned by shares and kept, and the
    /// second rounds taken back because a share did not hold.
    spread: (u64, u64),
}

/// Drive `cycles` cycles of the workload through a rig planning on `threads`
/// threads; then land everything and run a few more cycles.
fn run(
    tag: &str,
    seed: u64,
    keep: KeepCfg,
    front: DedupFront,
    cycles: usize,
    threads: usize,
) -> Report {
    run_shaped(tag, seed, keep, front, cycles, threads, false)
}

fn run_shaped(
    tag: &str,
    seed: u64,
    keep: KeepCfg,
    front: DedupFront,
    cycles: usize,
    threads: usize,
    wide: bool,
) -> Report {
    let _uuids = crate::util::deterministic_uuids(seed << 20);
    let bins0 = crate::rsm::batcher::bins_planned();
    let spread0 = crate::rsm::batcher::spread_planned();
    let mut rig = Rig::new(tag, keep, front, seed, threads);
    let mut w = if wide {
        Workload::wide(seed)
    } else {
        Workload::new(seed)
    };
    for c in 0..cycles {
        let batch = w.batch(rig.node.store());
        let out = rig.cycle(batch, &steps(c));
        if let Some(e) = &out.entry {
            w.observe(e);
        }
        rig.land_some();
    }
    // Drain: everything lands, and a few quiet cycles plan over it.
    let all = rig.unapplied.len();
    rig.apply(all);
    for c in cycles..cycles + 4 {
        let out = rig.cycle(Vec::new(), &steps(c));
        if out.entry.is_some() {
            let all = rig.unapplied.len();
            rig.apply(all);
        }
    }
    Report {
        digest: rig.node.digest(),
        stats: rig.state.stats.clone(),
        log: std::mem::take(&mut rig.log),
        kinds: std::mem::take(&mut rig.kinds),
        overlapped: rig.overlapped,
        lingering: rig.lingering,
        planned: rig.state.planned,
        bins: crate::rsm::batcher::bins_planned() - bins0,
        checked: rig.checked,
        cycles: rig.cycles,
        spread: {
            let now = crate::rsm::batcher::spread_planned();
            (now.0 - spread0.0, now.1 - spread0.1)
        },
    }
}

// ---------------------------------------------------------------------------
// 1 + 2: the gate
// ---------------------------------------------------------------------------

/// The effect kinds a gate run must have proposed at least once, or it did not
/// exercise what it claims to.
const COVERED: [&str; 19] = [
    "QueueUpsert",
    "QueueDelete",
    "TenantPurge",
    "GarbageAdd",
    "DeleteChunk",
    "GroupUpsert",
    "GroupDelete",
    "PartitionCreate",
    "PartitionDelete",
    "Append",
    "CursorSet",
    "DlqInsert",
    "Watermark",
    "KvPut",
    "KvDelete",
    "TimerUpsert",
    "TimerDelete",
    "TimerBackoff",
    "RequestIdsExpire",
];

fn gate(seed: u64, front_on: bool, cycles: usize) {
    let front = || {
        if front_on {
            DedupFront::new(true, 16 << 20)
        } else {
            DedupFront::disabled()
        }
    };
    let kept = run(
        &format!("keep-on-{seed}"),
        seed,
        keep_verified(),
        front(),
        cycles,
        1,
    );
    let stats = &kept.stats;
    // The kept path was really taken: every cycle but the ones after a
    // deliberately unkept one (a fold its entry does not carry) advanced the
    // kept overlay, and nothing ever failed to reconcile.
    assert_eq!(
        stats.fallbacks, 0,
        "a kept overlay failed to advance: {stats:?}"
    );
    assert!(
        stats.kept as usize >= cycles / 2,
        "the kept path was barely exercised: {stats:?}"
    );
    assert_eq!(
        stats.rebuilt,
        1 + stats.poisoned,
        "only the first cycle and the cycle after an unkept one rebuild: {stats:?}"
    );
    // ... over the shapes it claims: entries in flight at planning, landed
    // entries still in the in-flight list, and every effect kind above.
    assert!(
        kept.overlapped >= cycles / 3,
        "entries were rarely in flight: {}",
        kept.overlapped
    );
    assert!(
        kept.lingering > 0,
        "no landed entry ever lingered in the list"
    );
    for k in COVERED {
        assert!(
            kept.kinds.get(k).copied().unwrap_or(0) > 0,
            "the workload never proposed a {k}: {:?}",
            kept.kinds
        );
    }

    let plain = run(
        &format!("keep-off-{seed}"),
        seed,
        KeepCfg::off(),
        front(),
        cycles,
        1,
    );
    assert_eq!(plain.stats.kept, 0);
    assert_eq!(
        kept.log.len(),
        plain.log.len(),
        "the knob changed how many entries were proposed"
    );
    for (i, (a, b)) in kept.log.iter().zip(plain.log.iter()).enumerate() {
        assert!(
            a == b,
            "entry {} differs with the knob on and off ({} vs {} bytes)",
            i + 1,
            a.len(),
            b.len()
        );
    }
    assert_eq!(kept.digest, plain.digest, "the two stores diverged");
    eprintln!(
        "keep_overlay gate seed {seed} front {front_on}: {} entries over {} cycles; kept {} \
         poisoned {} rebuilt {}; in flight {} lingering {}; {:?}",
        kept.log.len(),
        cycles + 4,
        stats.kept,
        stats.poisoned,
        stats.rebuilt,
        kept.overlapped,
        kept.lingering,
        kept.kinds
    );
}

#[test]
fn the_kept_overlay_equals_a_rebuild_every_cycle_and_plans_byte_identically() {
    gate(1, false, 400);
}

#[test]
fn the_gate_holds_on_other_streams() {
    for seed in [7u64, 42, 1234] {
        gate(seed, false, 250);
    }
}

#[test]
fn the_gate_holds_with_the_dedup_front_on() {
    gate(99, true, 300);
}

// ---------------------------------------------------------------------------
// 2b: the footprint scheduler's oracle
// ---------------------------------------------------------------------------

/// `text` with every run of sixteen comma-separated integers in brackets — a
/// `[u8; 16]` in `Debug` form — replaced by `U`: the planners mint uuids
/// (partitions, queues, groups, dead letters, leader-step ids) from a counter
/// on their own thread, so which uuid a create gets depends on which thread
/// planned it, and nothing else does.
fn mask_uuids(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'[' {
            let mut j = i + 1;
            let mut ints = 0;
            let mut ok = true;
            loop {
                let start = j;
                while j < b.len() && b[j].is_ascii_digit() {
                    j += 1;
                }
                if j == start {
                    ok = false;
                    break;
                }
                ints += 1;
                if j < b.len() && b[j] == b']' {
                    break;
                }
                if j + 1 < b.len() && b[j] == b',' && b[j + 1] == b' ' {
                    j += 2;
                } else {
                    ok = false;
                    break;
                }
            }
            if ok && ints == 16 {
                out.push('U');
                i = j + 1;
                continue;
            }
        }
        out.push(b[i] as char);
        i += 1;
    }
    out
}

/// The workload planned by the homes on four threads: every cycle's entry is
/// the single planner's for the same commands in the order the homes planned
/// them ([`Rig::check_reference`]), and the homes really planned in parallel.
fn oracle(seed: u64, front_on: bool, cycles: usize, wide: bool) {
    oracle_kept(seed, front_on, cycles, wide, keep_verified());
}

/// [`oracle`] with the run keeping its overlay as `keep4` says: unverified, a
/// landing runs on the planning threads while the cycle routes (verified
/// cycles land at once, to compare).
fn oracle_kept(seed: u64, front_on: bool, cycles: usize, wide: bool, keep4: KeepCfg) {
    oracle_with(seed, front_on, cycles, wide, keep4, Forced::No);
}

/// Sends every parallel run of this thread back to one thread while it lives.
struct FallBack;

impl FallBack {
    fn at(at: Forced) -> FallBack {
        crate::rsm::batcher::fall_back_at(at);
        FallBack
    }
}

impl Drop for FallBack {
    fn drop(&mut self) {
        crate::rsm::batcher::fall_back_at(Forced::No);
    }
}

/// The oracle; with `forced`, every parallel run falls back there, so what
/// its homes folded in their shards is undone and the run planned again on
/// one thread.
fn oracle_with(
    seed: u64,
    front_on: bool,
    cycles: usize,
    wide: bool,
    keep4: KeepCfg,
    forced: Forced,
) {
    let front = if front_on {
        DedupFront::new(true, 16 << 20)
    } else {
        DedupFront::disabled()
    };
    let four = {
        let _fall_back = (forced != Forced::No).then(|| FallBack::at(forced));
        run_shaped(
            &format!("homes-{seed}-{wide}-{}-{forced:?}", keep4.verify),
            seed,
            keep4,
            front,
            cycles,
            4,
            wide,
        )
    };
    assert_eq!(
        four.checked, four.cycles,
        "every cycle is checked against the single planner"
    );
    assert_eq!(four.stats.fallbacks, 0, "{:?}", four.stats);
    // The narrow workload is mostly cycles with one home at work (its batches
    // are a few commands on a few keys); the wide one is mostly parallel.
    assert!(
        four.bins > 0,
        "no run planned in a planning thread's own shards"
    );
    if forced == Forced::Second {
        // Every second round was taken back out of its homes and planned by
        // the single planner's loop; the homes' own commands stayed, so no
        // run fell back.
        assert_eq!(four.spread.0, 0, "a second round was kept");
        assert!(four.spread.1 > 0, "no second round was taken back");
        assert_eq!(four.planned.fallback, 0, "{:?}", four.planned);
    } else if forced != Forced::No {
        // Every parallel run fell back after its homes planned in place: the
        // entries are still the single planner's, and the kept overlay a
        // rebuild's (verified every cycle), so the undo took everything out.
        assert!(
            four.planned.fallback > 0,
            "no run fell back: {:?}",
            four.planned
        );
    } else {
        // The second round went both ways: transactions planned by shares and
        // kept, and rounds a share's vote took back.
        if wide {
            assert!(four.spread.0 > 0, "no transaction was planned by shares");
            assert!(four.spread.1 > 0, "no second round was taken back");
        }
        let want = if wide { cycles / 2 } else { cycles / 20 };
        assert!(
            four.planned.parallel as usize >= want,
            "the homes barely planned in parallel: {:?}",
            four.planned
        );
        assert_eq!(
            four.planned.fallback, 0,
            "a unit went to the wrong home, or two homes touched one key: {:?}",
            four.planned
        );
    }
    eprintln!(
        "homes oracle seed {seed} front {front_on} wide {wide} forced {forced:?}: {} entries; {:?}; {} homes planned; {} transactions by shares, {} second rounds taken back",
        four.log.len(),
        four.planned,
        four.bins,
        four.spread.0,
        four.spread.1
    );
}

#[test]
fn the_scheduler_oracle_holds_when_every_parallel_run_falls_back() {
    for seed in [3, 11] {
        oracle_with(seed, true, 200, true, keep_verified(), Forced::Planned);
    }
    oracle_with(42, false, 250, false, keep_verified(), Forced::Planned);
}

/// The entry refuses what the homes planned (it cannot log a result as
/// planned): their folds are already in their shards, and must leave them
/// before the run is planned again.
#[test]
fn the_scheduler_oracle_holds_when_the_entry_refuses_what_the_homes_planned() {
    for seed in [3, 11] {
        oracle_with(seed, true, 200, true, keep_verified(), Forced::Assembled);
    }
    oracle_with(42, false, 250, false, keep_verified(), Forced::Assembled);
}

/// Every second round is taken back, as when a share did not hold: what the
/// homes folded for it leaves their overlays and their shards, their own
/// commands stay, and the single planner's loop plans the shared ones.
#[test]
fn the_scheduler_oracle_holds_when_every_second_round_is_taken_back() {
    for seed in [3, 11] {
        oracle_with(seed, true, 200, true, keep_verified(), Forced::Second);
    }
    oracle_with(42, false, 250, false, keep_verified(), Forced::Second);
}

/// The oracle with the product's dedup index: the homes plan with the dedup
/// front on and trust its "new" for the messages in flight; the reference
/// plans with the front off and reads the appends in flight for every hash.
/// The entries must be the same.
#[test]
fn the_scheduler_oracle_holds_with_the_txns_dedup_index() {
    let _txns = TxnsIndex::on();
    oracle(5, true, 200, true);
    oracle(99, true, 300, false);
    oracle(11, false, 200, true);
}

#[test]
fn the_scheduler_plans_the_single_planners_entries() {
    oracle(1, false, 400, false);
}

#[test]
fn the_scheduler_oracle_holds_on_other_streams() {
    for seed in [7u64, 42, 1234] {
        oracle(seed, false, 250, false);
    }
}

#[test]
fn the_scheduler_oracle_holds_with_the_dedup_front_on() {
    oracle(99, true, 300, false);
}

#[test]
fn the_scheduler_oracle_holds_on_wide_batches() {
    for seed in [3u64, 11, 77] {
        oracle(seed, false, 200, true);
    }
}

#[test]
fn the_scheduler_oracle_holds_on_wide_batches_with_the_dedup_front_on() {
    oracle(5, true, 200, true);
}

#[test]
fn the_scheduler_oracle_holds_with_the_landing_on_the_planning_threads() {
    let unverified = KeepCfg {
        verify: false,
        ..keep_verified()
    };
    for seed in [3u64, 77] {
        oracle_kept(seed, false, 200, true, unverified);
    }
    oracle_kept(5, true, 200, true, unverified);
    oracle_kept(42, false, 250, false, unverified);
}

// ---------------------------------------------------------------------------
// 3: the safety valves
// ---------------------------------------------------------------------------

fn push_cmd(id: u64, queue: &str, partition: &str, txns: &[&str]) -> Command {
    Command::Push(PushCommand {
        request_id: rid_of(id),
        tenant: TENANT.to_string(),
        queue: queue.to_string(),
        partition: partition.to_string(),
        items: txns
            .iter()
            .map(|t| PushItem {
                hash: crate::util::txn_hash128(t),
                frame: frame_of(t),
            })
            .collect(),
        create_cfg: queue_cfg(queue),
        links: Default::default(),
    })
}

fn no_steps() -> Steps {
    Steps {
        fire: None,
        kv_sweep: None,
        expire: None,
    }
}

/// The offset a push was answered with.
fn pushed_at(e: &Entry, id: u64) -> u64 {
    let c = e
        .commands
        .iter()
        .find(|c| c.request_id == rid_of(id))
        .expect("the push was logged");
    match &c.outcome {
        Outcome::Push(p) => match &p.items[0] {
            crate::rsm::entry::PushVerdict::Created { offset, .. } => *offset,
            v => panic!("not created: {v:?}"),
        },
        o => panic!("not a push: {o:?}"),
    }
}

/// A partition a group creates is numbered with the entry, and its number may
/// be one the dedup front still holds, seeded and empty: a create that was
/// planned and then dropped (a transaction rolled back, a cycle not proposed)
/// leaves it so. A retry of the new partition's message, after its entry has
/// landed, is still a duplicate.
#[test]
fn a_partition_a_group_creates_under_a_number_the_front_held_still_dedups() {
    let mut rig = Rig::new(
        "front-reuse",
        keep_verified(),
        DedupFront::new(true, 64 << 20),
        5,
        4,
    );
    let s = no_steps();
    let e1 = rig
        .cycle(
            vec![
                push_cmd(1, "qd", "p0", &["a"]),
                push_cmd(2, "qd", "p1", &["b"]),
            ],
            &s,
        )
        .entry
        .unwrap();
    rig.apply(1);
    let top = e1
        .effects
        .iter()
        .filter_map(|e| match e {
            Effect::PartitionCreate { pid, .. } => Some(*pid),
            _ => None,
        })
        .max()
        .expect("two partitions created");
    // What a dropped create leaves behind, for the next numbers.
    for pid in top + 1..top + 16 {
        rig.front.note_created(pid);
    }
    // A new partition whose name lives in another home than p0's: the two
    // pushes are then planned by two threads, and the new partition is created
    // under a placeholder.
    let home = |name: &str| {
        crate::rsm::planner::homes::home_of_shard(
            crate::rsm::planner::homes::Homes::shard_of_name(TENANT, name),
            4,
        )
    };
    let new = (0..)
        .map(|i| format!("new{i}"))
        .find(|n| home(n) != home("p0"))
        .expect("a name in another home");
    let parallel0 = rig.state.planned.parallel;
    rig.cycle(
        vec![
            push_cmd(3, "qd", "p0", &["c"]),
            push_cmd(4, "qd", &new, &["n1"]),
        ],
        &s,
    );
    assert!(
        rig.state.planned.parallel > parallel0,
        "planned in two groups"
    );
    rig.apply(1);
    // The retry, with one new message so that the push is logged.
    let e3 = rig
        .cycle(vec![push_cmd(5, "qd", &new, &["n1", "n2"])], &s)
        .entry
        .expect("the new message is logged");
    let c = e3
        .commands
        .iter()
        .find(|c| c.request_id == rid_of(5))
        .expect("the retry was logged");
    match &c.outcome {
        Outcome::Push(p) => assert!(
            matches!(p.items[0], crate::rsm::entry::PushVerdict::Duplicate { .. }),
            "the retried message is a duplicate: {:?}",
            p.items[0]
        ),
        o => panic!("not a push: {o:?}"),
    }
}

/// The verdicts a push was answered with.
fn verdicts_of(e: &Entry, id: u64) -> Vec<crate::rsm::entry::PushVerdict> {
    let c = e
        .commands
        .iter()
        .find(|c| c.request_id == rid_of(id))
        .expect("the push was logged");
    match &c.outcome {
        Outcome::Push(p) => p.items.clone(),
        o => panic!("not a push: {o:?}"),
    }
}

/// A message whose entry is still in flight is pushed again: a duplicate, at
/// the offset the first push gave it. The overlay keeps no row per message, so
/// this is the dedup front saying "maybe" (the first push put the hash there
/// when it was planned) and the probe then reading the appends in flight to
/// the partition. With the front off, every probe reads them.
fn a_retry_of_a_message_in_flight(front: DedupFront, threads: usize, tag: &str) {
    let mut rig = Rig::new(tag, keep_verified(), front, 5, threads);
    let s = no_steps();
    // The partition exists, committed, and the front knows it.
    rig.cycle(vec![push_cmd(1, "qd", "p0", &["a"])], &s);
    rig.apply(1);
    // `b` is planned and proposed, not applied: in flight.
    let e2 = rig
        .cycle(vec![push_cmd(2, "qd", "p0", &["b"])], &s)
        .entry
        .expect("b is logged");
    let b_at = pushed_at(&e2, 2);
    let e3 = rig
        .cycle(vec![push_cmd(3, "qd", "p0", &["b", "c"])], &s)
        .entry
        .expect("c is logged");
    let v = verdicts_of(&e3, 3);
    assert!(
        matches!(v[0], crate::rsm::entry::PushVerdict::Duplicate { offset, .. } if offset == b_at),
        "the message in flight is a duplicate at {b_at}: {:?}",
        v[0]
    );
    assert!(
        matches!(v[1], crate::rsm::entry::PushVerdict::Created { .. }),
        "the new message is appended: {:?}",
        v[1]
    );
}

#[test]
fn a_message_still_in_flight_is_a_duplicate_when_it_is_pushed_again() {
    a_retry_of_a_message_in_flight(DedupFront::new(true, 64 << 20), 1, "inflight-dup-front-1");
    a_retry_of_a_message_in_flight(DedupFront::new(true, 64 << 20), 4, "inflight-dup-front-4");
    a_retry_of_a_message_in_flight(DedupFront::disabled(), 1, "inflight-dup-nofront-1");
    a_retry_of_a_message_in_flight(DedupFront::disabled(), 4, "inflight-dup-nofront-4");
}

/// The same with the product's dedup index: the front answers "scan these
/// bands" for the hash the first push put there, the committed rows of the
/// band do not hold it, and the appends in flight do.
#[test]
fn a_message_still_in_flight_is_a_duplicate_with_the_txns_index() {
    let _txns = TxnsIndex::on();
    a_retry_of_a_message_in_flight(DedupFront::new(true, 64 << 20), 1, "inflight-dup-txns-1");
    a_retry_of_a_message_in_flight(DedupFront::new(true, 64 << 20), 4, "inflight-dup-txns-4");
    a_retry_of_a_message_in_flight(DedupFront::disabled(), 1, "inflight-dup-txns-nofront-1");
    a_retry_of_a_message_in_flight(DedupFront::disabled(), 4, "inflight-dup-txns-nofront-4");
}

/// The front is emptied while a message is in flight (what a node does when
/// it becomes leader): the partition is seeded again at its next push, from
/// the committed rows AND the appends in flight, so a retry of the message in
/// flight is still a duplicate. A seed of committed rows alone would call it
/// new, and nothing else would look.
#[test]
fn a_partition_seeded_again_knows_its_messages_in_flight() {
    reseeded_with_a_message_in_flight("rows");
    let _txns = TxnsIndex::on();
    reseeded_with_a_message_in_flight("txns");
}

fn reseeded_with_a_message_in_flight(index: &str) {
    for threads in [1, 4] {
        let mut rig = Rig::new(
            &format!("reseed-inflight-{index}-{threads}"),
            keep_verified(),
            DedupFront::new(true, 64 << 20),
            5,
            threads,
        );
        let s = no_steps();
        rig.cycle(vec![push_cmd(1, "qd", "p0", &["a"])], &s);
        rig.apply(1);
        let e2 = rig
            .cycle(vec![push_cmd(2, "qd", "p0", &["b"])], &s)
            .entry
            .expect("b is logged");
        let b_at = pushed_at(&e2, 2);
        rig.front.reset();
        let e3 = rig
            .cycle(vec![push_cmd(3, "qd", "p0", &["a", "b", "c"])], &s)
            .entry
            .expect("c is logged");
        let v = verdicts_of(&e3, 3);
        assert!(
            matches!(v[0], crate::rsm::entry::PushVerdict::Duplicate { .. }),
            "the committed message is a duplicate: {:?}",
            v[0]
        );
        assert!(
            matches!(v[1], crate::rsm::entry::PushVerdict::Duplicate { offset, .. } if offset == b_at),
            "the message in flight is a duplicate at {b_at}: {:?}",
            v[1]
        );
        assert!(
            matches!(v[2], crate::rsm::entry::PushVerdict::Created { .. }),
            "the new message is appended: {:?}",
            v[2]
        );
    }
}

/// A partition a home creates is not in the front while its id is a
/// placeholder, and is seeded at its next push. That push comes while the
/// creating entry is still in flight: the seed reads its appends, and a retry
/// of the first message is a duplicate.
#[test]
fn a_partition_a_home_created_dedups_while_its_create_is_in_flight() {
    home_created_with_a_message_in_flight("rows");
    let _txns = TxnsIndex::on();
    home_created_with_a_message_in_flight("txns");
}

fn home_created_with_a_message_in_flight(index: &str) {
    let mut rig = Rig::new(
        &format!("home-created-inflight-{index}"),
        keep_verified(),
        DedupFront::new(true, 64 << 20),
        5,
        4,
    );
    let s = no_steps();
    rig.cycle(vec![push_cmd(1, "qd", "p0", &["a"])], &s);
    rig.apply(1);
    let home = |name: &str| {
        crate::rsm::planner::homes::home_of_shard(
            crate::rsm::planner::homes::Homes::shard_of_name(TENANT, name),
            4,
        )
    };
    let new = (0..)
        .map(|i| format!("new{i}"))
        .find(|n| home(n) != home("p0"))
        .expect("a name in another home");
    let parallel0 = rig.state.planned.parallel;
    let e2 = rig
        .cycle(
            vec![
                push_cmd(2, "qd", "p0", &["b"]),
                push_cmd(3, "qd", &new, &["n1"]),
            ],
            &s,
        )
        .entry
        .expect("both are logged");
    assert!(
        rig.state.planned.parallel > parallel0,
        "two homes planned: the new partition was created under a placeholder"
    );
    let n1_at = pushed_at(&e2, 3);
    // Not applied: the create and `n1` are in flight.
    let e3 = rig
        .cycle(vec![push_cmd(4, "qd", &new, &["n1", "n2"])], &s)
        .entry
        .expect("n2 is logged");
    let v = verdicts_of(&e3, 4);
    assert!(
        matches!(v[0], crate::rsm::entry::PushVerdict::Duplicate { offset, .. } if offset == n1_at),
        "the message in flight is a duplicate at {n1_at}: {:?}",
        v[0]
    );
    assert!(
        matches!(v[1], crate::rsm::entry::PushVerdict::Created { .. }),
        "the new message is appended: {:?}",
        v[1]
    );
}

#[test]
fn a_new_epoch_drops_what_was_kept_and_the_next_cycle_plans_from_a_rebuild() {
    let mut rig = Rig::new("keep-epoch", keep_verified(), DedupFront::disabled(), 3, 1);
    let s = no_steps();
    let e1 = rig
        .cycle(vec![push_cmd(1, "q0", "p0", &["a", "b"])], &s)
        .entry
        .unwrap();
    assert_eq!(pushed_at(&e1, 1), 0);
    rig.apply(1);
    // Two entries in flight, never applied.
    let e2 = rig
        .cycle(vec![push_cmd(2, "q0", "p0", &["c"])], &s)
        .entry
        .unwrap();
    assert_eq!(pushed_at(&e2, 2), 2);
    let e3 = rig
        .cycle(vec![push_cmd(3, "q0", "p0", &["d"])], &s)
        .entry
        .unwrap();
    assert_eq!(
        pushed_at(&e3, 3),
        3,
        "the kept overlay carries the entry in flight"
    );
    assert!(rig.state.stats.kept >= 2);

    // A lost leadership: the pipeline is drained (the two entries never
    // commit) and the driver moves the epoch.
    rig.inflight.clear();
    rig.unapplied.clear();
    rig.next_index = 2;
    rig.keep.epoch += 1;
    let rebuilt0 = rig.state.stats.rebuilt;
    let e4 = rig
        .cycle(vec![push_cmd(4, "q0", "p0", &["e"])], &s)
        .entry
        .unwrap();
    assert_eq!(
        rig.state.stats.rebuilt,
        rebuilt0 + 1,
        "a state kept under another epoch is dropped"
    );
    assert_eq!(
        pushed_at(&e4, 4),
        2,
        "the drained entries' offsets are free again: nothing of them was kept"
    );
    rig.apply(1);
    let e5 = rig
        .cycle(vec![push_cmd(5, "q0", "p0", &["f"])], &s)
        .entry
        .unwrap();
    assert_eq!(pushed_at(&e5, 5), 3);
}

#[test]
fn an_entry_planned_and_never_proposed_is_never_carried() {
    // The driver moves the epoch when an entry fails to encode. Even without
    // that, the reconciliation refuses a kept list that is not the in-flight
    // list: the unproposed entry is never folded into the next cycle.
    let mut keep = keep_verified();
    keep.verify = false; // the rebuilt reference is the thing under test here
    let mut rig = Rig::new("keep-unproposed", keep, DedupFront::disabled(), 4, 1);
    let s = no_steps();
    rig.cycle(vec![push_cmd(1, "q0", "p0", &["a"])], &s);
    rig.cycle(vec![push_cmd(2, "q0", "p0", &["b"])], &s);
    // Plan an entry and drop it on the floor, as a failed encode does.
    let lost = rig
        .cycle(vec![push_cmd(3, "q0", "p0", &["c"])], &s)
        .entry
        .unwrap();
    assert_eq!(pushed_at(&lost, 3), 2);
    rig.inflight.pop_back();
    rig.unapplied.pop_back();
    rig.next_index -= 1;
    rig.log.pop();
    let fallbacks0 = rig.state.stats.fallbacks;
    let e = rig
        .cycle(vec![push_cmd(4, "q0", "p0", &["d"])], &s)
        .entry
        .unwrap();
    assert_eq!(
        rig.state.stats.fallbacks,
        fallbacks0 + 1,
        "the kept list was refused"
    );
    assert_eq!(
        pushed_at(&e, 4),
        2,
        "the unproposed append is not in the overlay"
    );
}

#[test]
fn a_replay_whose_second_push_is_a_duplicate_folds_nothing() {
    // A DLQ-replay transaction whose second push is a duplicate answers EMPTY:
    // it logs nothing, so its first push (a new partition and its append)
    // must not stay folded either. A cycle that held it would hand a later
    // push to that partition an append no entry created.
    let mut rig = Rig::new("keep-replay", keep_verified(), DedupFront::disabled(), 5, 1);
    let s = no_steps();
    rig.cycle(vec![push_cmd(1, "qd", "p0", &["dup"])], &s);
    rig.apply(1);
    let replay = |id: u64| {
        Command::Transaction(TxnCommand {
            request_id: rid_of(id),
            tenant: TENANT.to_string(),
            pushes: vec![
                match push_cmd(id * 10, "qd", "p1", &["fresh"]) {
                    Command::Push(p) => p,
                    _ => unreachable!(),
                },
                match push_cmd(id * 10 + 1, "qd", "p0", &["dup"]) {
                    Command::Push(p) => p,
                    _ => unreachable!(),
                },
            ],
            acks: Vec::new(),
            positional_acks: Vec::new(),
            kv: Vec::new(),
            timers: Vec::new(),
            extra_effects: Vec::new(),
            allow_duplicate: true,
            positions: Vec::new(),
            engine_acks: AckOutcome::default(),
        })
    };
    let rebuilt0 = rig.state.stats.rebuilt;
    let out = rig.cycle(vec![replay(2)], &s);
    assert!(out.entry.is_none(), "the replay logged nothing");
    assert_eq!(rig.state.stats.poisoned, 0, "and folded nothing");
    // The same replay, then a push to `qd/p1` in the same cycle: the push
    // creates the partition, at offset 0.
    let e = rig
        .cycle(vec![replay(3), push_cmd(4, "qd", "p1", &["x"])], &s)
        .entry
        .unwrap();
    assert_eq!(rig.state.stats.poisoned, 0, "the replay folded nothing");
    assert_eq!(
        rig.state.stats.rebuilt, rebuilt0,
        "nothing was rebuilt: the kept overlay stayed exact"
    );
    assert!(
        e.commands.iter().all(|c| c.request_id != rid_of(3)),
        "the replay logged nothing"
    );
    assert_eq!(pushed_at(&e, 4), 0);
    assert!(
        e.effects
            .iter()
            .any(|x| matches!(x, Effect::PartitionCreate { partition, .. } if partition == "p1")),
        "the partition is created by the entry that is proposed"
    );
}

#[test]
fn the_knob_off_keeps_nothing() {
    let mut rig = Rig::new("keep-off", KeepCfg::off(), DedupFront::disabled(), 6, 1);
    let s = no_steps();
    for i in 0..5 {
        rig.cycle(vec![push_cmd(i + 1, "q0", "p0", &[&format!("t{i}")])], &s);
    }
    assert_eq!(rig.state.stats.kept, 0);
    assert_eq!(
        rig.state.stats.rebuilt, 5,
        "every cycle rebuilt from scratch"
    );
}

#[test]
fn a_periodic_reset_rebuilds_and_still_matches() {
    let mut keep = keep_verified();
    keep.reset_every = 3;
    let mut rig = Rig::new("keep-reset", keep, DedupFront::disabled(), 8, 1);
    let s = no_steps();
    for i in 0..9u64 {
        rig.cycle(vec![push_cmd(i + 1, "q0", "p0", &[&format!("t{i}")])], &s);
        if i % 2 == 0 {
            rig.apply(1);
        }
    }
    assert_eq!(
        rig.state.stats.rebuilt, 4,
        "cycle 1 and every third: {:?}",
        rig.state.stats
    );
    assert_eq!(rig.state.stats.kept, 5);
}
