//! KEEP_OVERLAY (`QUEEN_RAFT_KEEP_OVERLAY`) — THE GATE.
//!
//! The planner thread keeps the overlay and the wildcard rings BETWEEN cycles
//! and takes out, per landed entry, exactly what that entry put in
//! ([`crate::rsm::planner::kept`], [`crate::rsm::state::PlanRings`]). What must
//! hold:
//!
//! 1. After EVERY cycle of a mixed workload the kept overlay equals — field by
//!    field, and tag by tag through the entry each tag names — the overlay the
//!    old path rebuilds from scratch, and every kept ring equals a rebuild from
//!    `pending` (the walk, the deferred rows, the next deadline, the rows).
//!    Both checks run INSIDE the real `plan_cycle_blocking` (`KeepCfg::verify`,
//!    `verify_rings`) at the point the planner is about to use the state. The
//!    workload: pushes that create queues and partitions and hit a dedup
//!    window, wildcard / pinned / discovery pops (leases that expire, auto-ack,
//!    a delayed queue whose rows wait in the deferred set), hash and
//!    positional acks with ok/failed/dlq/retry, nacks, renews, DLQ heads,
//!    transactions (committed, rolled back on a duplicate, DLQ-replay ones
//!    that answer empty), KV puts, incrs, deletes and the expiry sweep, timer
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
//! recomputed, a cursor write not re-read into the rings, a registration not
//! dropping its ring, the stray-fold check off: every one fails it.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Arc;

use serde_json::json;

use crate::rsm::apply::{Applier, Committed as ApplyCommitted, NoNotify};
use crate::rsm::batcher::{
    plan_cycle_blocking, Command, KeepCfg, KeepStats, PlanOutput, PlannerState,
};
use crate::rsm::dedup::DedupFront;
use crate::rsm::effect::{Effect, GarbageScope, Pid, QueueConfig};
use crate::rsm::entry::{encode_entry, Entry, Outcome, RequestId};
use crate::rsm::planner::kv::parse_ops;
use crate::rsm::planner::timers::{parse_timer_ops, TimerFireConfig, TimersCommand};
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{
    AckCommand, AckItem, AckPositionalCommand, AckStatus, AckTarget, DlqHeadCommand, DlqSnapshot,
    EffectsCommand, KvCommand, NackCommand, PlanConfig, PopCommand, PushCommand, PushItem,
    RenewCommand, SubIntent,
};
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
/// delays visibility (its `pending` rows wait in the deferred set).
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

/// A delivered, leased batch a later command can ack, nack or renew.
#[derive(Clone, Debug)]
struct Delivered {
    pid: Pid,
    queue: String,
    group: String,
    worker: String,
    start: u64,
    end: u64,
}

/// The adaptive command stream: it learns partitions, offsets, hashes and
/// leased batches from the entries the planner proposes, so its acks and pops
/// target real state. Everything it decides comes from its seeded [`Rng`] and
/// from those entries, so two runs that propose the same entries generate the
/// same stream.
struct Workload {
    rng: Rng,
    next_id: u64,
    next_txn: u64,
    recent_txns: Vec<String>,
    submitted: Vec<Command>,
    commands: HashMap<RequestId, Command>,
    /// pid → (queue, partition), from `PartitionCreate`.
    pid_queue: HashMap<Pid, (String, String)>,
    /// queue → its partitions, in creation order.
    partitions: HashMap<String, Vec<(String, Pid)>>,
    /// (pid, offset) → the frame's hash, from every `Append`.
    hashes: HashMap<(Pid, u64), [u8; 16]>,
    delivered: Vec<Delivered>,
    /// The highest watermark emitted per pid (they never move back).
    watermark: HashMap<Pid, u64>,
}

impl Workload {
    fn new(seed: u64) -> Workload {
        Workload {
            rng: Rng::new(seed),
            next_id: 1,
            next_txn: 1,
            recent_txns: Vec::new(),
            submitted: Vec::new(),
            commands: HashMap::new(),
            pid_queue: HashMap::new(),
            partitions: HashMap::new(),
            hashes: HashMap::new(),
            delivered: Vec::new(),
            watermark: HashMap::new(),
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
                    frame: format!("{{\"t\":\"{t}\"}}").into_bytes(),
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
        }
    }

    fn push(&mut self) -> Command {
        let queue = self.rng.pick(&QUEUES).to_string();
        let partition = format!("p{}", self.rng.below(5));
        let n = 1 + self.rng.below(4) as usize;
        Command::Push(self.push_cmd(&queue, &partition, n))
    }

    fn pop_base(&mut self, queue: &str) -> PopCommand {
        let auto_ack = self.rng.chance(15);
        PopCommand {
            wait: false,
            request_id: self.id(),
            tenant: TENANT.to_string(),
            queue: queue.to_string(),
            partition: None,
            group: self.rng.pick(&GROUPS).to_string(),
            worker: self.rng.pick(&WORKERS).to_string(),
            budget: 1 + self.rng.below(12) as i32,
            max_parts: 1 + self.rng.below(4) as i32,
            // Short leases, so some expire under the workload and come back.
            lease_seconds: 1 + self.rng.below(3) as i32,
            auto_ack,
            conflate: false,
            sub: SubIntent {
                mode: if self.rng.chance(20) { "new" } else { "all" }.to_string(),
                from_us: None,
                now: false,
            },
            skip_window_debounce: false,
            namespace: String::new(),
            task: String::new(),
            create_cfg: Some(queue_cfg(queue)),
            deadline_us: 0,
        }
    }

    fn pop_wildcard(&mut self) -> Command {
        let queue = self.rng.pick(&QUEUES).to_string();
        Command::PopWildcard(self.pop_base(&queue))
    }

    fn pop_pinned(&mut self) -> Option<Command> {
        let queue = self.rng.pick(&QUEUES).to_string();
        let parts = self.partitions.get(&queue)?.clone();
        let (name, _) = parts[self.rng.below(parts.len() as u64) as usize].clone();
        let mut c = self.pop_base(&queue);
        c.partition = Some(name);
        Some(Command::PopPinned(c))
    }

    fn pop_discover(&mut self) -> Command {
        let mut c = self.pop_base("");
        c.namespace = "ns".into();
        c.group = "gd".into();
        c.create_cfg = None;
        Command::PopDiscover(c)
    }

    fn take_delivered(&mut self) -> Option<Delivered> {
        if self.delivered.is_empty() {
            return None;
        }
        let i = self.rng.below(self.delivered.len() as u64) as usize;
        Some(self.delivered.swap_remove(i))
    }

    fn ack_target(&mut self, d: &Delivered) -> AckTarget {
        let mut items = Vec::new();
        for off in d.start..=d.end {
            let Some(h) = self.hashes.get(&(d.pid, off)).copied() else {
                continue;
            };
            let status = match self.rng.below(20) {
                0 => AckStatus::Failed,
                1 => AckStatus::Dlq,
                2 => AckStatus::Retry,
                _ => AckStatus::Ok,
            };
            let snapshot =
                matches!(status, AckStatus::Failed | AckStatus::Dlq).then(|| DlqSnapshot {
                    message_id: None,
                    txn: format!("off{off}"),
                    payload: b"{}".to_vec(),
                });
            items.push(AckItem {
                hash: h,
                status,
                error: snapshot.as_ref().map(|_| "boom".to_string()),
                snapshot,
            });
        }
        AckTarget {
            pid: d.pid,
            tenant: TENANT.to_string(),
            queue: d.queue.clone(),
            group: d.group.clone(),
            // A lease-less ack now and then (it still advances, 005).
            worker: if self.rng.chance(10) {
                String::new()
            } else {
                d.worker.clone()
            },
            items,
        }
    }

    fn ack(&mut self) -> Option<Command> {
        let d = self.take_delivered()?;
        let t = self.ack_target(&d);
        Some(Command::Ack(AckCommand {
            request_id: self.id(),
            targets: vec![t],
        }))
    }

    fn ack_positional(&mut self) -> Option<Command> {
        let d = self.take_delivered()?;
        Some(Command::AckPositional(AckPositionalCommand {
            request_id: self.id(),
            pid: d.pid,
            tenant: TENANT.to_string(),
            queue: d.queue.clone(),
            group: d.group.clone(),
            worker: d.worker.clone(),
            upto: Some(d.end as i64),
            ok: self.rng.chance(80),
            release_lease: true,
            acked_count: (d.end - d.start + 1) as i32,
        }))
    }

    fn nack(&mut self) -> Option<Command> {
        let d = self.take_delivered()?;
        Some(Command::Nack(NackCommand {
            request_id: self.id(),
            pid: d.pid,
            tenant: TENANT.to_string(),
            queue: d.queue,
            group: d.group,
            worker: d.worker,
        }))
    }

    fn dlq_head(&mut self) -> Option<Command> {
        let d = self.take_delivered()?;
        Some(Command::DlqHead(DlqHeadCommand {
            request_id: self.id(),
            pid: d.pid,
            tenant: TENANT.to_string(),
            queue: d.queue,
            group: d.group,
            worker: d.worker,
            offset: d.start,
            error: "poison".into(),
            snapshot: DlqSnapshot {
                message_id: None,
                txn: "poison".into(),
                payload: b"{}".to_vec(),
            },
        }))
    }

    fn renew(&mut self) -> Command {
        Command::Renew(RenewCommand {
            request_id: self.id(),
            worker: self.rng.pick(&WORKERS).to_string(),
            seconds: 2,
        })
    }

    fn transaction(&mut self) -> Command {
        let mut pushes = vec![{
            let q = if self.rng.chance(60) { "qd" } else { "q0" };
            let p = format!("p{}", self.rng.below(3));
            let n = 1 + self.rng.below(2) as usize;
            self.push_cmd(q, &p, n)
        }];
        if self.rng.chance(50) {
            let p = format!("p{}", self.rng.below(3));
            pushes.push(self.push_cmd("qd", &p, 1));
        }
        let acks = match self.take_delivered() {
            Some(d) if self.rng.chance(40) => {
                let mut t = self.ack_target(&d);
                for it in &mut t.items {
                    it.status = AckStatus::Ok;
                    it.snapshot = None;
                    it.error = None;
                }
                t.worker = d.worker.clone();
                vec![t]
            }
            Some(d) => {
                self.delivered.push(d);
                Vec::new()
            }
            None => Vec::new(),
        };
        let kv = if self.rng.chance(30) {
            let k = format!("tk{}", self.rng.below(3));
            parse_ops(
                &[json!({"op":"put","ns":"txn","key":k,"value":{"n":1},"forever":true})],
                TENANT,
                true,
                511,
            )
            .expect("kv op")
        } else {
            Vec::new()
        };
        Command::Transaction(TxnCommand {
            request_id: self.id(),
            tenant: TENANT.to_string(),
            pushes,
            acks,
            positional_acks: Vec::new(),
            kv,
            timers: Vec::new(),
            extra_effects: Vec::new(),
            // The DLQ-replay shape: a duplicate answers empty WITHOUT restoring
            // the overlay (the earlier push groups stay folded).
            allow_duplicate: self.rng.chance(25),
        })
    }

    fn kv(&mut self) -> Command {
        let k = format!("k{}", self.rng.below(5));
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
            ops: parse_ops(&[op], TENANT, false, 511).expect("kv op"),
        })
    }

    fn timers(&mut self) -> Command {
        let queue = self.rng.pick(&TIMER_QUEUES).to_string();
        let key = format!("t{}", self.rng.below(4));
        let op = if self.rng.chance(25) {
            json!({"op":"cancel","queue":queue,"timerKey":key})
        } else {
            let t = self.txn();
            json!({
                "op":"schedule","queue":queue,"timerKey":key,
                "delayMs": self.rng.below(400) as i64,
                "txn": t, "payload": "e30=",
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
        let n = 1 + self.rng.below(7);
        let mut out: Vec<Command> = Vec::new();
        for _ in 0..n {
            let c = match self.rng.below(100) {
                0..=29 => Some(self.push()),
                30..=44 => Some(self.pop_wildcard()),
                45..=49 => self.pop_pinned(),
                50..=51 => Some(self.pop_discover()),
                52..=61 => self.ack(),
                62..=65 => self.ack_positional(),
                66..=67 => self.nack(),
                68..=69 => Some(self.renew()),
                70 => self.dlq_head(),
                71..=76 => Some(self.transaction()),
                77..=81 => Some(self.kv()),
                82..=86 => Some(self.timers()),
                87 => Some(self.group_delete()),
                88..=89 => self.watermark_cmd(store),
                90..=91 => Some(self.queue_delete(store)),
                92 => self.partition_delete(),
                93 => self.delete_resume(store),
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
            self.commands.insert(c.request_id(), c.clone());
            self.submitted.push(c.clone());
        }
        out
    }

    /// Learn from a proposed entry: partitions, frame hashes, leased batches.
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
                    self.partitions
                        .entry(queue.clone())
                        .or_default()
                        .push((partition.clone(), *pid));
                }
                Effect::Append {
                    pid,
                    base_offset,
                    count,
                    hashes,
                    ..
                } => {
                    for i in 0..*count as usize {
                        let mut h = [0u8; 16];
                        h.copy_from_slice(&hashes[i * 16..i * 16 + 16]);
                        self.hashes.insert((*pid, base_offset + i as u64), h);
                    }
                }
                // Gone for the watermarks; acks, nacks and renews of their
                // leases still come, and must find nothing to move.
                Effect::GarbageAdd { pids, scope, .. }
                    if !matches!(scope, GarbageScope::Group { .. }) =>
                {
                    for pid in pids {
                        self.pid_queue.remove(pid);
                    }
                }
                Effect::PartitionDelete { pid } => {
                    self.pid_queue.remove(pid);
                }
                _ => {}
            }
        }
        for c in &e.commands {
            let Some(cmd) = self.commands.get(&c.request_id) else {
                continue;
            };
            let group = match cmd {
                Command::PopPinned(p) | Command::PopWildcard(p) | Command::PopDiscover(p) => {
                    p.group.clone()
                }
                _ => continue,
            };
            if let Outcome::Pop(pop) = &c.outcome {
                for claim in &pop.claims {
                    if claim.lease_expires_at_us.is_none() {
                        continue;
                    }
                    let queue = self
                        .pid_queue
                        .get(&claim.pid)
                        .map(|(q, _)| q.clone())
                        .unwrap_or_default();
                    self.delivered.push(Delivered {
                        pid: claim.pid,
                        queue,
                        group: group.clone(),
                        worker: claim.worker.clone(),
                        start: claim.start_offset,
                        end: claim.end_offset,
                    });
                }
            }
        }
        if self.delivered.len() > 64 {
            self.delivered.drain(0..16);
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
    front: DedupFront,
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
}

impl Rig {
    fn new(tag: &str, keep: KeepCfg, front: DedupFront, seed: u64) -> Rig {
        Rig {
            node: Node::new(tag),
            state: PlannerState::default(),
            keep,
            front,
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
        }
    }

    fn plan_cfg() -> PlanConfig {
        // No budget cut: a time-based cut would make the two runs of the A/B
        // defer different commands.
        PlanConfig {
            plan_budget_ms: 3_600_000,
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
        let out = plan_cycle_blocking(
            self.node.store(),
            &self.front,
            &mut self.state,
            self.keep,
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
        )
        .expect("plan cycle");
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
                entry: (*e).clone(),
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
        verify_rings: true,
        reset_every: 0,
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
    /// `(ring loads, ring drops)`.
    rings: (u64, u64),
}

/// Drive `cycles` cycles of the workload through a rig; then land everything
/// and run a few more cycles.
fn run(tag: &str, seed: u64, keep: KeepCfg, front: DedupFront, cycles: usize) -> Report {
    let _uuids = crate::util::deterministic_uuids(seed << 20);
    let mut rig = Rig::new(tag, keep, front, seed);
    let mut w = Workload::new(seed);
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
        rings: rig.state.ring_stats(),
        log: std::mem::take(&mut rig.log),
        kinds: std::mem::take(&mut rig.kinds),
        overlapped: rig.overlapped,
        lingering: rig.lingering,
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
         poisoned {} rebuilt {}; in flight {} lingering {}; ring loads {} drops {}; {:?}",
        kept.log.len(),
        cycles + 4,
        stats.kept,
        stats.poisoned,
        stats.rebuilt,
        kept.overlapped,
        kept.lingering,
        kept.rings.0,
        kept.rings.1,
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
                frame: format!("{{\"t\":\"{t}\"}}").into_bytes(),
            })
            .collect(),
        create_cfg: queue_cfg(queue),
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

#[test]
fn a_new_epoch_drops_what_was_kept_and_the_next_cycle_plans_from_a_rebuild() {
    let mut rig = Rig::new("keep-epoch", keep_verified(), DedupFront::disabled(), 3);
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
    let mut rig = Rig::new("keep-unproposed", keep, DedupFront::disabled(), 4);
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
fn a_cycle_that_folds_what_its_entry_does_not_carry_is_not_kept() {
    // A DLQ-replay transaction whose second push is a duplicate answers EMPTY
    // without restoring the overlay: its first push group stays folded, in no
    // entry. The old path dropped that with the cycle; the kept overlay must
    // not carry it on.
    let mut rig = Rig::new("keep-poison", keep_verified(), DedupFront::disabled(), 5);
    let s = no_steps();
    rig.cycle(vec![push_cmd(1, "qd", "p0", &["dup"])], &s);
    rig.apply(1);
    let replay = Command::Transaction(TxnCommand {
        request_id: rid_of(2),
        tenant: TENANT.to_string(),
        pushes: vec![
            match push_cmd(20, "qd", "p1", &["fresh"]) {
                Command::Push(p) => p,
                _ => unreachable!(),
            },
            match push_cmd(21, "qd", "p0", &["dup"]) {
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
    });
    let rebuilt0 = rig.state.stats.rebuilt;
    let out = rig.cycle(vec![replay], &s);
    assert!(out.entry.is_none(), "the replay logged nothing");
    // The next push to `qd/p1` must create the partition afresh at offset 0:
    // the stray fold (a partition and an append nobody logged) is gone. (Had
    // it been kept, this cycle's verify would already have failed on it.)
    let e = rig
        .cycle(vec![push_cmd(3, "qd", "p1", &["x"])], &s)
        .entry
        .unwrap();
    assert_eq!(
        rig.state.stats.poisoned, 1,
        "the cycle's stray fold was caught"
    );
    assert_eq!(
        rig.state.stats.rebuilt,
        rebuilt0 + 1,
        "the next cycle rebuilt"
    );
    assert_eq!(pushed_at(&e, 3), 0);
    assert!(
        e.effects
            .iter()
            .any(|x| matches!(x, Effect::PartitionCreate { partition, .. } if partition == "p1")),
        "the partition is created by the entry that is proposed"
    );
}

#[test]
fn the_knob_off_keeps_nothing() {
    let mut rig = Rig::new("keep-off", KeepCfg::off(), DedupFront::disabled(), 6);
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
    let mut rig = Rig::new("keep-reset", keep, DedupFront::disabled(), 8);
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
