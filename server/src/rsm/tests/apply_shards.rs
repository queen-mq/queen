//! Sharded apply (`QUEEN_RAFT_APPLY_SHARDS`) against the one-thread path.
//!
//! The claim `apply_shard.rs` makes is that the shard count is invisible:
//! the same entries give byte-identical replicated state, byte-identical
//! node-local state (segment files, their table, `seg_loc`), the same qlog
//! bytes, the same notifications in the same order, the same counts and the
//! same refusal report, whatever the count. Every test here runs one workload
//! at one shard and at several and compares all of that, not "no error".
//!
//! The workload ([`Wide`]) is what makes runs interesting: entries of up to
//! ~60 effects over dozens of partitions of three queues, with partition
//! creates (and, rarely, two creates of one name in an entry), claims, acks,
//! releases and retries, cursor deletes, watermarks, dead letters with fresh
//! and REUSED ids, stream state, and the global effects that end a run in the
//! middle of an entry — group and queue upserts that must invalidate the
//! shards' caches, group and partition deletes, a queue delete with its
//! chunks and its recreation, request-id expiry and KV writes.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::rsm::apply::{
    Applied, Applier, ApplyConfig, ApplyFailure, ApplyStats, Committed, Notify, StateDigest,
};
use crate::rsm::effect::{CursorRow, Effect, GarbageScope, GroupMeta, Pid, SubscriptionMode};
use crate::rsm::entry::{CommandRecord, Entry, Outcome, PushOutcome, PushVerdict};
use crate::rsm::store::{Keyspace, Reads, Store};

use super::apply::{
    cfg, fresh_cursor, group_meta, hashes, queue_config, seg_opts, settle, uuid, Node, BASE_US,
};

// ---------------------------------------------------------------------------
// A reproducible wide workload
// ---------------------------------------------------------------------------

/// SplitMix64: the same seed, the same entries, on every machine.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        if n == 0 {
            0
        } else {
            self.next() % n
        }
    }
}

struct QueueModel {
    tenant: String,
    queue: String,
    groups: Vec<String>,
    alive: bool,
    /// Entries left before a deleted queue is recreated.
    reborn_in: u64,
}

struct PartModel {
    pid: Pid,
    q: usize,
    bucket: u16,
    last_offset: i64,
    log_start: u64,
    txns_start: u64,
    committed: BTreeMap<String, i64>,
    leased: BTreeMap<String, bool>,
    retries: BTreeMap<String, u32>,
    consumed: BTreeMap<String, u64>,
    dlq: Vec<[u8; 16]>,
    streams: Vec<String>,
}

/// Entries of many commands over many partitions (module header).
pub struct Wide {
    rng: Rng,
    n: u64,
    ids: u64,
    next_pid: u64,
    kv_next: u64,
    dlq_seq: u64,
    group_seq: u64,
    queues: Vec<QueueModel>,
    parts: Vec<PartModel>,
    /// Pids of a deleted queue whose chunks are still being emitted.
    garbage: Vec<Pid>,
}

impl Wide {
    pub fn new(seed: u64) -> Wide {
        Wide {
            rng: Rng(seed),
            n: 0,
            ids: 0,
            next_pid: 1,
            kv_next: 1,
            dlq_seq: 0,
            group_seq: 10,
            queues: vec![
                QueueModel {
                    tenant: "t1".into(),
                    queue: "orders".into(),
                    groups: vec!["g1".into(), "g2".into()],
                    alive: false,
                    reborn_in: 0,
                },
                QueueModel {
                    tenant: "t2".into(),
                    queue: "events".into(),
                    groups: vec!["g1".into()],
                    alive: false,
                    reborn_in: 0,
                },
                QueueModel {
                    tenant: "t1".into(),
                    queue: "scratch".into(),
                    groups: vec!["g1".into()],
                    alive: false,
                    reborn_in: 0,
                },
            ],
            parts: Vec::new(),
            garbage: Vec::new(),
        }
    }

    fn id(&mut self) -> [u8; 16] {
        self.ids += 1;
        super::apply::request_id(self.ids)
    }

    fn live_parts(&self) -> Vec<usize> {
        (0..self.parts.len())
            .filter(|&i| self.queues[self.parts[i].q].alive)
            .collect()
    }

    fn pick_part(&mut self) -> Option<usize> {
        let live = self.live_parts();
        if live.is_empty() {
            return None;
        }
        Some(live[self.rng.below(live.len() as u64) as usize])
    }

    fn create_queue(&mut self, q: usize, now: i64, effects: &mut Vec<Effect>) {
        let (tenant, queue) = (self.queues[q].tenant.clone(), self.queues[q].queue.clone());
        let mut qc = queue_config(now);
        qc.id = uuid(1000 + self.n * 10 + q as u64);
        effects.push(Effect::QueueUpsert {
            tenant: tenant.clone(),
            queue: queue.clone(),
            cfg: qc,
        });
        for (i, g) in self.queues[q].groups.clone().iter().enumerate() {
            effects.push(Effect::GroupUpsert {
                tenant: tenant.clone(),
                queue: queue.clone(),
                group: g.clone(),
                meta: group_meta(i as u64, now),
            });
        }
        self.queues[q].alive = true;
    }

    /// A new partition of queue `q` (the caller appends to it).
    fn create_part(&mut self, q: usize, name: Option<String>, now: i64) -> (Effect, usize) {
        let pid = self.next_pid;
        self.next_pid += 1;
        let name = name.unwrap_or_else(|| format!("p{pid}"));
        let e = Effect::PartitionCreate {
            pid,
            uuid: uuid(pid),
            tenant: self.queues[q].tenant.clone(),
            queue: self.queues[q].queue.clone(),
            partition: name,
            created_at_us: now,
        };
        self.parts.push(PartModel {
            pid,
            q,
            bucket: (pid % 16) as u16,
            last_offset: -1,
            log_start: 0,
            txns_start: 0,
            committed: BTreeMap::new(),
            leased: BTreeMap::new(),
            retries: BTreeMap::new(),
            consumed: BTreeMap::new(),
            dlq: Vec::new(),
            streams: Vec::new(),
        });
        (e, self.parts.len() - 1)
    }

    fn append(&mut self, pi: usize, now: i64) -> Effect {
        let count = 1 + self.rng.below(4) as u32;
        let seed = self.rng.next();
        let p = &mut self.parts[pi];
        let base = (p.last_offset + 1) as u64;
        p.last_offset += count as i64;
        Effect::Append {
            pid: p.pid,
            bucket: p.bucket,
            base_offset: base,
            count,
            created_at_us: now,
            hashes: hashes(seed, count),
            blob: vec![0xC3; 20 * count as usize + (seed % 7) as usize],
        }
    }

    fn group_of(&mut self, pi: usize) -> Option<(usize, String)> {
        let q = self.parts[pi].q;
        let gs = &self.queues[q].groups;
        if gs.is_empty() {
            return None;
        }
        let gi = self.rng.below(gs.len() as u64) as usize;
        Some((gi, gs[gi].clone()))
    }

    fn cursor(&mut self, pi: usize, now: i64) -> Option<Effect> {
        let (gi, g) = self.group_of(pi)?;
        let what = self.rng.below(10);
        let step = 1 + self.rng.below(3) as i64;
        let p = &mut self.parts[pi];
        if p.last_offset < 0 {
            return None;
        }
        let committed = *p.committed.get(&g).unwrap_or(&-1);
        let leased = *p.leased.get(&g).unwrap_or(&false);
        let retries = *p.retries.get(&g).unwrap_or(&0);
        let consumed = *p.consumed.get(&g).unwrap_or(&0);
        let mut row: CursorRow = fresh_cursor(committed, now);
        row.batch_retry_count = retries;
        row.total_consumed = consumed;
        match what {
            // A claim: a lease over everything not yet acked.
            0..=3 => {
                if leased || committed >= p.last_offset {
                    return None;
                }
                row.worker = Some(format!("w{gi}-{}", p.pid % 3));
                row.lease_expires_at_us = Some(now + 30_000_000);
                row.lease_acquired_at_us = Some(now);
                row.batch_end = Some(p.last_offset as u64);
                row.total_consumed = consumed + (p.last_offset - committed) as u64;
                p.leased.insert(g.clone(), true);
                p.consumed.insert(g.clone(), row.total_consumed);
            }
            // A retry: the lease stays, the retry count moves.
            4 => {
                if !leased {
                    return None;
                }
                row.worker = Some(format!("w{gi}-{}", p.pid % 3));
                row.lease_expires_at_us = Some(now + 30_000_000);
                row.batch_end = Some(p.last_offset as u64);
                row.batch_retry_count = retries + 1;
                p.retries.insert(g.clone(), retries + 1);
            }
            // An ack: committed moves up, the lease (if any) is released.
            _ => {
                let next = (committed + step).min(p.last_offset);
                row.committed = next;
                p.committed.insert(g.clone(), next);
                p.leased.insert(g.clone(), false);
            }
        }
        Some(Effect::CursorSet {
            pid: p.pid,
            group: g,
            row,
        })
    }

    fn watermark(&mut self, pi: usize) -> Option<Effect> {
        let step = 1 + self.rng.below(4);
        let p = &mut self.parts[pi];
        if p.last_offset < 0 {
            return None;
        }
        let ceiling = (p.last_offset + 1) as u64;
        let log_start = (p.log_start + step).min(ceiling);
        let txns_start = (p.txns_start + step / 2 + 1).min(log_start);
        if log_start == p.log_start && txns_start == p.txns_start {
            return None;
        }
        p.log_start = log_start;
        p.txns_start = txns_start;
        Some(Effect::Watermark {
            pid: p.pid,
            log_start,
            txns_start,
            rows: None,
        })
    }

    fn dead_letter(&mut self, pi: usize, now: i64, reuse: bool) -> Option<Effect> {
        let r = self.rng.next();
        let (_, g) = self.group_of(pi)?;
        if self.parts[pi].last_offset < 0 {
            return None;
        }
        let q = self.parts[pi].q;
        // A reused id: one another partition of the same queue holds. A
        // planner bug the one-thread path tolerates (the old row goes, its
        // index with it); sharded apply must keep it out of a run.
        let id = if reuse {
            let holder = (0..self.parts.len())
                .find(|&i| i != pi && self.parts[i].q == q && !self.parts[i].dlq.is_empty())?;
            self.parts[holder].dlq.remove(0)
        } else {
            self.dlq_seq += 1;
            uuid(9_000_000 + self.dlq_seq)
        };
        let p = &mut self.parts[pi];
        p.dlq.push(id);
        Some(Effect::DlqInsert {
            dlq_id: id,
            tenant: self.queues[q].tenant.clone(),
            queue: self.queues[q].queue.clone(),
            pid: p.pid,
            group: g,
            offset: (r % (p.last_offset as u64 + 1)) as i64,
            message_id: Some(uuid(r)),
            txn: format!("txn-{r}"),
            payload: b"{\"x\":1}".to_vec(),
            error: "boom".into(),
            retry_count: 3,
            failed_at_us: now,
        })
    }

    /// The next entry, at index `n + 1`.
    pub fn next(&mut self) -> Committed {
        self.n += 1;
        let now = BASE_US + self.n as i64 * 1_000;
        let mut entry = Entry::new(now, self.next_pid, self.kv_next);
        let mut cmds: Vec<(Vec<Effect>, Outcome)> = Vec::new();

        if self.n == 1 {
            let mut effects = Vec::new();
            for q in 0..self.queues.len() {
                self.create_queue(q, now, &mut effects);
                for _ in 0..6 {
                    let (e, _) = self.create_part(q, None, now);
                    effects.push(e);
                }
            }
            cmds.push((effects, Outcome::Empty));
        } else {
            // A deleted queue comes back after a while (the name is reusable
            // at once, §5.2; its old pids are still being chunked).
            for q in 0..self.queues.len() {
                if !self.queues[q].alive && self.queues[q].reborn_in > 0 {
                    self.queues[q].reborn_in -= 1;
                    if self.queues[q].reborn_in == 0 {
                        let mut effects = Vec::new();
                        self.create_queue(q, now, &mut effects);
                        for _ in 0..3 {
                            let (e, pi) = self.create_part(q, None, now);
                            effects.push(e);
                            effects.push(self.append(pi, now));
                        }
                        cmds.push((effects, Outcome::Empty));
                    }
                }
            }
            if !self.garbage.is_empty() {
                cmds.push((
                    vec![Effect::DeleteChunk {
                        pids: self.garbage.clone(),
                        scope: GarbageScope::Queue,
                        resume: Vec::new(),
                        limit: 9,
                    }],
                    Outcome::Empty,
                ));
                if self.rng.below(12) == 0 {
                    self.garbage.clear();
                }
            }
            // Mostly wide entries, sometimes a small one.
            let commands = if self.rng.below(5) == 0 {
                1 + self.rng.below(3)
            } else {
                8 + self.rng.below(40)
            };
            let mut dup_name: Option<(usize, String)> = None;
            for _ in 0..commands {
                let which = self.rng.below(100);
                let mut effects: Vec<Effect> = Vec::new();
                let mut outcome = Outcome::Empty;
                match which {
                    0..=29 => {
                        if let Some(pi) = self.pick_part() {
                            let e = self.append(pi, now);
                            if let Effect::Append {
                                pid,
                                base_offset,
                                count,
                                ..
                            } = &e
                            {
                                outcome = Outcome::Push(PushOutcome {
                                    items: (0..*count as u64)
                                        .map(|k| PushVerdict::Created {
                                            pid: *pid,
                                            offset: base_offset + k,
                                            created_at_us: now,
                                        })
                                        .collect(),
                                });
                            }
                            effects.push(e);
                        }
                    }
                    30..=39 => {
                        let q = self.rng.below(self.queues.len() as u64) as usize;
                        if self.queues[q].alive {
                            // Rarely, a second create of a name this entry
                            // already created: the name index must end at the
                            // later pid, as on one thread.
                            let name = match &dup_name {
                                Some((dq, n)) if *dq == q && self.rng.below(8) == 0 => {
                                    Some(n.clone())
                                }
                                _ => None,
                            };
                            let (e, pi) = self.create_part(q, name, now);
                            if let Effect::PartitionCreate { partition, .. } = &e {
                                dup_name = Some((q, partition.clone()));
                            }
                            effects.push(e);
                            effects.push(self.append(pi, now));
                        }
                    }
                    40..=59 => {
                        if let Some(pi) = self.pick_part() {
                            effects.extend(self.cursor(pi, now));
                        }
                    }
                    60..=61 => {
                        if let Some(pi) = self.pick_part() {
                            if let Some((_, g)) = self.group_of(pi) {
                                let p = &mut self.parts[pi];
                                p.committed.remove(&g);
                                p.leased.remove(&g);
                                p.retries.remove(&g);
                                p.consumed.remove(&g);
                                effects.push(Effect::CursorDelete {
                                    pid: p.pid,
                                    group: g,
                                });
                            }
                        }
                    }
                    62..=69 => {
                        if let Some(pi) = self.pick_part() {
                            effects.extend(self.watermark(pi));
                        }
                    }
                    70..=75 => {
                        if let Some(pi) = self.pick_part() {
                            effects.extend(self.dead_letter(pi, now, false));
                        }
                    }
                    76 => {
                        if let Some(pi) = self.pick_part() {
                            effects.extend(self.dead_letter(pi, now, true));
                        }
                    }
                    77..=78 => {
                        if let Some(pi) = self.pick_part() {
                            let q = self.parts[pi].q;
                            if !self.parts[pi].dlq.is_empty() {
                                let id = self.parts[pi].dlq.remove(0);
                                effects.push(Effect::DlqDelete {
                                    dlq_id: id,
                                    tenant: self.queues[q].tenant.clone(),
                                    queue: self.queues[q].queue.clone(),
                                });
                            }
                        }
                    }
                    79..=82 => {
                        if let Some(pi) = self.pick_part() {
                            let k = format!("k{}", self.rng.below(5));
                            let p = &mut self.parts[pi];
                            if self.rng.below(3) == 0 && !p.streams.is_empty() {
                                let key = p.streams.remove(0);
                                effects.push(Effect::StreamsStateDelete {
                                    query_id: uuid(77),
                                    pid: p.pid,
                                    key,
                                });
                            } else {
                                p.streams.push(k.clone());
                                effects.push(Effect::StreamsStatePut {
                                    query_id: uuid(77),
                                    pid: p.pid,
                                    key: k,
                                    value: vec![1, 2, 3, self.n as u8],
                                    updated_at_us: now,
                                });
                            }
                        }
                    }
                    83..=84 => {
                        // A new group, `all` or `new`: the shards' cached group
                        // lists must see it at once, and `all` arms every
                        // partition that predates it.
                        let q = self.rng.below(self.queues.len() as u64) as usize;
                        if self.queues[q].alive && self.queues[q].groups.len() < 4 {
                            self.group_seq += 1;
                            let g = format!("g{}", self.group_seq);
                            let mut meta: GroupMeta = group_meta(self.group_seq, now);
                            if self.rng.below(2) == 0 {
                                meta.mode = SubscriptionMode::All;
                            }
                            self.queues[q].groups.push(g.clone());
                            effects.push(Effect::GroupUpsert {
                                tenant: self.queues[q].tenant.clone(),
                                queue: self.queues[q].queue.clone(),
                                group: g,
                                meta,
                            });
                        }
                    }
                    85 => {
                        let q = self.rng.below(self.queues.len() as u64) as usize;
                        if self.queues[q].alive && self.queues[q].groups.len() > 1 {
                            let g = self.queues[q].groups.remove(0);
                            for p in self.parts.iter_mut().filter(|p| p.q == q) {
                                p.committed.remove(&g);
                                p.leased.remove(&g);
                                p.retries.remove(&g);
                                p.consumed.remove(&g);
                            }
                            effects.push(Effect::GroupDelete {
                                tenant: self.queues[q].tenant.clone(),
                                queue: self.queues[q].queue.clone(),
                                group: g,
                            });
                        }
                    }
                    86..=87 => {
                        // A configuration change: the delay the append path
                        // cached per queue must be read again.
                        let q = self.rng.below(self.queues.len() as u64) as usize;
                        if self.queues[q].alive {
                            let mut qc = queue_config(now);
                            qc.id = uuid(1000 + q as u64);
                            qc.delayed_processing = self.rng.below(3) as i32;
                            qc.window_buffer = self.rng.below(2) as i32;
                            effects.push(Effect::QueueUpsert {
                                tenant: self.queues[q].tenant.clone(),
                                queue: self.queues[q].queue.clone(),
                                cfg: qc,
                            });
                        }
                    }
                    88 => {
                        if let Some(pi) = self.pick_part() {
                            let p = self.parts.remove(pi);
                            effects.push(Effect::PartitionDelete { pid: p.pid });
                        }
                    }
                    89 => {
                        effects.push(Effect::RequestIdsExpire {
                            cutoff_us: now - 20_000,
                        });
                    }
                    90 => {
                        // The scratch queue goes: its names at once, its pids
                        // in chunks over the next entries.
                        let q = 2;
                        if self.queues[q].alive && self.garbage.is_empty() {
                            let pids: Vec<Pid> = self
                                .parts
                                .iter()
                                .filter(|p| p.q == q)
                                .map(|p| p.pid)
                                .collect();
                            self.parts.retain(|p| p.q != q);
                            self.queues[q].alive = false;
                            self.queues[q].groups = vec!["g1".into()];
                            self.queues[q].reborn_in = 5 + self.rng.below(10);
                            effects.push(Effect::QueueDelete {
                                tenant: self.queues[q].tenant.clone(),
                                queue: self.queues[q].queue.clone(),
                            });
                            if !pids.is_empty() {
                                effects.push(Effect::GarbageAdd {
                                    pids: pids.clone(),
                                    scope: GarbageScope::Queue,
                                    deleted_at_us: now,
                                });
                                self.garbage = pids;
                            }
                        }
                    }
                    91 => {
                        let version = self.kv_next;
                        self.kv_next += 1;
                        effects.push(Effect::KvPut {
                            tenant: "t1".into(),
                            ns: "ns".into(),
                            key: format!("k{}", self.rng.below(6)),
                            value: format!("{}", self.n).into_bytes(),
                            version,
                            expires_at_us: None,
                            created_at_us: now,
                            updated_at_us: now,
                        });
                    }
                    _ => {
                        // Two appends in one command (a multi-key push).
                        for _ in 0..2 {
                            if let Some(pi) = self.pick_part() {
                                effects.push(self.append(pi, now));
                            }
                        }
                    }
                }
                if !effects.is_empty() {
                    cmds.push((effects, outcome));
                }
            }
            if cmds.is_empty() {
                cmds.push((vec![Effect::Noop], Outcome::Empty));
            }
        }
        for (effects, outcome) in cmds {
            let id = self.id();
            entry
                .add_command(id, outcome, effects)
                .expect("add command");
        }
        Committed {
            index: self.n,
            term: 1,
            entry: Arc::new(entry),
        }
    }
}

// ---------------------------------------------------------------------------
// A notifier that records every call, in order
// ---------------------------------------------------------------------------

#[derive(Default)]
struct Calls {
    log: Mutex<Vec<String>>,
    failures: Mutex<Vec<ApplyFailure>>,
    append_wakes: bool,
}

impl Notify for Calls {
    fn applied(&self, index: u64, term: u64, commands: &[CommandRecord]) {
        self.log
            .lock()
            .unwrap()
            .push(format!("applied {index} {term} {}", commands.len()));
    }
    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>) {
        self.log
            .lock()
            .unwrap()
            .push(format!("wake {tenant} {queue} {group:?}"));
    }
    fn wake_append(&self, tenant: &str, queue: &str, group: &str) {
        self.log
            .lock()
            .unwrap()
            .push(format!("wake_append {tenant} {queue} {group}"));
    }
    fn wants_append_wakes(&self) -> bool {
        self.append_wakes
    }
    fn wants_appended(&self) -> bool {
        true
    }
    fn appended(&self, tenant: &str, queue: &str, partition: &str) {
        self.log
            .lock()
            .unwrap()
            .push(format!("appended {tenant} {queue} {partition}"));
    }
    fn durable(&self, index: u64) {
        self.log.lock().unwrap().push(format!("durable {index}"));
    }
    fn failed(&self, failure: &ApplyFailure) {
        self.failures.lock().unwrap().push(failure.clone());
    }
}

// ---------------------------------------------------------------------------
// One run, and the comparison
// ---------------------------------------------------------------------------

/// Everything a run leaves that must not depend on the shard count.
struct Seen {
    digest: StateDigest,
    local: StateDigest,
    rows: Vec<(&'static str, Vec<u8>, Vec<u8>)>,
    calls: Vec<String>,
    stats: ApplyStats,
    qlog: Vec<(String, Vec<u8>)>,
}

fn sharded(base: ApplyConfig, shards: usize, min: usize) -> ApplyConfig {
    ApplyConfig {
        apply_shards: shards,
        apply_shard_min: min,
        ..base
    }
}

/// Every row of every keyspace, replicated and node-local, in key order.
fn dump(node: &Node) -> Vec<(&'static str, Vec<u8>, Vec<u8>)> {
    node.store()
        .read(|r| {
            let mut out = Vec::new();
            for ks in Keyspace::ALL {
                r.scan_raw(ks, &[], &[], usize::MAX, &mut |k, v| {
                    out.push((ks.name(), k.to_vec(), v.to_vec()));
                    true
                })?;
            }
            Ok(out)
        })
        .expect("dump")
}

/// Every file under the node's `qlog/`, by relative path, with its bytes.
fn qlog_files(node: &Node) -> Vec<(String, Vec<u8>)> {
    fn walk(root: &std::path::Path, dir: &std::path::Path, out: &mut Vec<(String, Vec<u8>)>) {
        let Ok(rd) = std::fs::read_dir(dir) else {
            return;
        };
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(root, &p, out);
            } else if let Ok(b) = std::fs::read(&p) {
                let rel = p.strip_prefix(root).unwrap().to_string_lossy().to_string();
                out.push((rel, b));
            }
        }
    }
    let root = node.path().join("qlog");
    let mut out = Vec::new();
    walk(&root, &root, &mut out);
    out.sort();
    out
}

/// Apply `entries` of the wide workload at `cfg`, with a commit every few
/// entries, a durable point every so often and a GC pass every turn, as the
/// apply loop does.
fn run(tag: &str, seed: u64, entries: u64, cfg: ApplyConfig, append_wakes: bool) -> Seen {
    let node = Node::new(tag);
    let calls = Arc::new(Calls {
        append_wakes,
        ..Default::default()
    });
    let stats;
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg,
            calls.clone(),
        )
        .expect("open");
        assert_eq!(a.shards(), cfg.apply_shards.max(1));
        let mut w = Wide::new(seed);
        for n in 1..=entries {
            let c = w.next();
            assert!(matches!(a.apply(&c), Ok(Applied::Executed { .. })));
            if n.is_multiple_of(23) {
                a.durable_point().expect("durable point");
            } else if n.is_multiple_of(5) {
                a.commit().expect("commit");
            }
            a.gc_pass().expect("gc");
        }
        settle(&mut a);
        stats = a.stats();
    }
    let calls = calls.log.lock().unwrap().clone();
    Seen {
        digest: node.digest(),
        local: node.local_digest(),
        rows: dump(&node),
        calls,
        stats,
        qlog: qlog_files(&node),
    }
}

fn assert_same(what: &str, want: &Seen, got: &Seen) {
    if want.rows != got.rows {
        let first = want
            .rows
            .iter()
            .zip(got.rows.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(want.rows.len().min(got.rows.len()));
        panic!(
            "{what}: rows differ at #{first} of {}/{}: one thread {:?}, sharded {:?}",
            want.rows.len(),
            got.rows.len(),
            want.rows.get(first),
            got.rows.get(first),
        );
    }
    assert_eq!(
        want.digest,
        got.digest,
        "{what}: replicated digest, first at {:?}",
        want.digest.first_difference(&got.digest)
    );
    assert_eq!(
        want.local,
        got.local,
        "{what}: node-local digest, first at {:?}",
        want.local.first_difference(&got.local)
    );
    if want.calls != got.calls {
        let first = want
            .calls
            .iter()
            .zip(got.calls.iter())
            .position(|(a, b)| a != b)
            .unwrap_or(want.calls.len().min(got.calls.len()));
        panic!(
            "{what}: notifications differ at #{first} of {}/{}: one thread {:?}, sharded {:?}",
            want.calls.len(),
            got.calls.len(),
            want.calls.get(first),
            got.calls.get(first),
        );
    }
    assert_eq!(want.stats, got.stats, "{what}: stats");
    assert_eq!(
        want.qlog
            .iter()
            .map(|(p, b)| (p, b.len()))
            .collect::<Vec<_>>(),
        got.qlog
            .iter()
            .map(|(p, b)| (p, b.len()))
            .collect::<Vec<_>>(),
        "{what}: qlog files"
    );
    assert!(want.qlog == got.qlog, "{what}: qlog bytes");
}

/// The shared test config with the shard count forced (the suite's own
/// `QUEEN_TEST_APPLY_SHARDS` must not leak into the one-thread reference).
fn base() -> ApplyConfig {
    sharded(cfg(), 1, 1)
}

const ENTRIES: u64 = 260;

// ---------------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------------

#[test]
fn every_shard_count_leaves_the_one_thread_state_rows_files_and_wakes() {
    for seed in [0x5A4D_0001u64, 0x5A4D_0002, 0x5A4D_0003] {
        let want = run("wide-1", seed, ENTRIES, base(), false);
        assert!(
            want.stats.appends > 500 && want.stats.wakes > 100,
            "the workload is not wide enough: {:?}",
            want.stats
        );
        for shards in [2usize, 3, 4, 8] {
            let got = run(
                &format!("wide-{shards}"),
                seed,
                ENTRIES,
                sharded(base(), shards, 1),
                false,
            );
            assert_same(&format!("seed {seed:#x}, {shards} shards"), &want, &got);
        }
    }
}

#[test]
fn the_live_path_external_queue_logs_is_shard_invariant() {
    // `qlog_writer_external`: no segment frames, sentinel `seg_loc` rows, no
    // release. What the live broker runs.
    let live = ApplyConfig {
        qlog: true,
        qlog_writer_external: true,
        ..base()
    };
    let want = run("live-1", 0x11FE_0001, ENTRIES, live, true);
    for shards in [2usize, 4, 5] {
        let got = run(
            &format!("live-{shards}"),
            0x11FE_0001,
            ENTRIES,
            sharded(live, shards, 1),
            true,
        );
        assert_same(&format!("live path, {shards} shards"), &want, &got);
    }
}

#[test]
fn an_apply_owned_queue_log_gets_the_same_records_in_the_same_order() {
    // The unit-test qlog path: apply buffers every append's record, which a
    // run's pre-pass does in entry order. Compared file by file, byte by byte.
    let owned = ApplyConfig {
        qlog: true,
        qlog_writer_external: false,
        ..base()
    };
    let want = run("qlog-1", 0x0106_0001, 160, owned, false);
    assert!(!want.qlog.is_empty(), "the qlog was written");
    let got = run("qlog-4", 0x0106_0001, 160, sharded(owned, 4, 1), false);
    assert_same("apply-owned qlog, 4 shards", &want, &got);
}

#[test]
fn the_ablation_path_is_shard_invariant_too() {
    // Counters batched off: shards write counters straight into the store, so
    // the runs execute one shard after another (no pool).
    let c = ApplyConfig {
        batch_counters: false,
        ..base()
    };
    let want = run("unbatched-1", 0xAB1A_7E00, 180, c, true);
    let got = run("unbatched-4", 0xAB1A_7E00, 180, sharded(c, 4, 1), true);
    assert_same("unbatched, 4 shards", &want, &got);
}

#[test]
fn a_threshold_mixes_inline_entries_and_runs_to_the_same_state() {
    // The shipped threshold: small entries take the one-thread path, wide ones
    // run on the shards, and runs between global effects may be either.
    let want = run("thr-1", 0x7E5E_0001, ENTRIES, base(), false);
    for min in [4usize, 8, 32] {
        let got = run(
            &format!("thr-{min}"),
            0x7E5E_0001,
            ENTRIES,
            sharded(base(), 4, min),
            false,
        );
        assert_same(&format!("4 shards, min {min}"), &want, &got);
    }
}

#[test]
fn the_shard_count_may_change_across_a_restart() {
    // Node-local: a node applies half at four shards, restarts at three, then
    // at one, and holds what one thread all the way holds.
    let want = run("restart-ref", 0x4E57_A470, 240, base(), false);
    let node = Node::new("restart");
    let calls = Arc::new(Calls::default());
    let mut w = Wide::new(0x4E57_A470);
    let mut n = 0u64;
    for (shards, upto) in [(4usize, 80u64), (3, 160), (1, 240)] {
        let (mut a, rec) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            sharded(base(), shards, 1),
            calls.clone(),
        )
        .expect("open");
        // Everything applied before the restart is durable (settle below), so
        // the reopened node resumes right after it.
        assert_eq!(rec.applied_index, n);
        while n < upto {
            n += 1;
            let c = w.next();
            a.apply(&c).expect("apply");
            if n.is_multiple_of(23) {
                a.durable_point().expect("durable point");
            } else if n.is_multiple_of(5) {
                a.commit().expect("commit");
            }
            a.gc_pass().expect("gc");
        }
        settle(&mut a);
    }
    assert_eq!(node.digest(), want.digest, "replicated state");
    assert_eq!(node.local_digest(), want.local, "node-local state");
    assert!(dump(&node) == want.rows, "rows");
}

#[test]
fn the_shared_workload_at_four_shards_is_the_one_thread_workload() {
    // The generator the crash tests replay, applied as `run_workload` does.
    let ref_node = Node::new("shared-1");
    let want = super::apply::run_workload(&ref_node, 0x5EED_0042, 1500, 97);
    let node = Node::new("shared-4");
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            sharded(base(), 4, 1),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        let mut w = super::apply::Workload::new(0x5EED_0042);
        for n in 1..=1500u64 {
            a.apply(&w.next()).expect("apply");
            if n.is_multiple_of(97) {
                a.durable_point().expect("durable point");
            }
            a.gc_pass().expect("gc");
        }
        settle(&mut a);
    }
    assert_eq!(node.digest(), want, "replicated state");
    assert_eq!(
        node.local_digest(),
        ref_node.local_digest(),
        "node-local state"
    );
}

// ---------------------------------------------------------------------------
// Refusals: the report names the effect one thread stops at
// ---------------------------------------------------------------------------

/// Setup (queue, group, 8 partitions with a frame each), then one entry whose
/// effects are appends to every partition, with `bad` ordinals broken: a base
/// offset that does not continue the partition (a deterministic refusal) or a
/// bucket out of range (a segment refusal, node-local, made by the run's
/// pre-pass). Returns the failure report.
fn refusal_report(shards: usize, bad: &[(usize, &str)]) -> ApplyFailure {
    let node = Node::new(&format!("refusal-{shards}"));
    let calls = Arc::new(Calls::default());
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        sharded(base(), shards, 1),
        calls.clone(),
    )
    .expect("open");
    let mut setup = Entry::new(BASE_US, 1, 1);
    let mut effects = vec![
        Effect::QueueUpsert {
            tenant: "t1".into(),
            queue: "orders".into(),
            cfg: queue_config(BASE_US),
        },
        Effect::GroupUpsert {
            tenant: "t1".into(),
            queue: "orders".into(),
            group: "g1".into(),
            meta: group_meta(0, BASE_US),
        },
    ];
    for pid in 1..=8u64 {
        effects.push(Effect::PartitionCreate {
            pid,
            uuid: uuid(pid),
            tenant: "t1".into(),
            queue: "orders".into(),
            partition: format!("p{pid}"),
            created_at_us: BASE_US,
        });
    }
    setup
        .add_command(super::apply::request_id(1), Outcome::Empty, effects)
        .expect("cmd");
    a.apply(&Committed {
        index: 1,
        term: 1,
        entry: Arc::new(setup),
    })
    .expect("setup");

    let now = BASE_US + 1_000;
    let mut e = Entry::new(now, 9, 1);
    for pid in 1..=8u64 {
        let ord = (pid - 1) as usize;
        let how = bad.iter().find(|(o, _)| *o == ord).map(|(_, h)| *h);
        e.add_command(
            super::apply::request_id(10 + pid),
            Outcome::Empty,
            vec![Effect::Append {
                pid,
                bucket: if how == Some("bucket") { 300 } else { 1 },
                base_offset: if how == Some("offset") { 5 } else { 0 },
                count: 2,
                created_at_us: now,
                hashes: hashes(pid, 2),
                blob: vec![7; 40],
            }],
        )
        .expect("cmd");
    }
    let r = a.apply(&Committed {
        index: 2,
        term: 1,
        entry: Arc::new(e),
    });
    assert!(r.is_err(), "the entry is refused");
    // And the applier refuses everything after it.
    assert!(a.commit().is_err());
    let f = calls.failures.lock().unwrap();
    assert_eq!(f.len(), 1, "one report");
    f[0].clone()
}

#[test]
fn a_refused_run_reports_the_effect_one_thread_stops_at() {
    for bad in [
        vec![(5usize, "offset")],
        vec![(6, "offset"), (2, "offset")],
        vec![(3, "offset"), (4, "offset"), (7, "offset")],
        // A segment refusal from the pre-pass AFTER a deterministic one: one
        // thread never reaches the segment write, so the report is the offset.
        vec![(6, "bucket"), (2, "offset")],
        // And before it: the pre-pass refusal is the first one.
        vec![(1, "bucket"), (5, "offset")],
    ] {
        let want = refusal_report(1, &bad);
        for shards in [2usize, 4, 8] {
            let got = refusal_report(shards, &bad);
            assert_eq!(
                (&got.effect, &got.class, &got.error, &got.command),
                (&want.effect, &want.class, &want.error, &want.command),
                "{bad:?} at {shards} shards"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// B08: the outcome rows are the rows `put_request_outcome` wrote
// ---------------------------------------------------------------------------

#[test]
fn recorded_outcomes_are_byte_for_byte_the_row_codec_s() {
    use crate::rsm::store::rows::{self, RequestIdRow};
    use crate::rsm::store::{keys, TypedReads};

    let node = Node::new("outcomes");
    let mut w = Wide::new(0x0C0E_0001);
    let mut applied: Vec<Committed> = Vec::new();
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            base(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for _ in 0..12 {
            let c = w.next();
            a.apply(&c).expect("apply");
            applied.push(c);
        }
        a.commit().expect("commit");
    }
    let mut pushes = 0;
    node.store()
        .read(|r| {
            for c in &applied {
                for cmd in &c.entry.commands {
                    let want = rows::request_id_encode(&RequestIdRow {
                        now_us: c.entry.now_us,
                        outcome: cmd.outcome.encode(),
                    });
                    let got = r
                        .get_raw(Keyspace::RequestIds, &keys::request_ids(&cmd.request_id))?
                        .expect("the outcome row");
                    assert_eq!(got, &want[..], "request_ids row");
                    let back = r.request_outcome(&cmd.request_id)?.expect("decodes");
                    assert_eq!(Outcome::decode(&back.outcome).unwrap(), cmd.outcome);
                    assert!(r
                        .get_raw(
                            Keyspace::RequestExpiry,
                            &keys::request_expiry(c.entry.now_us, &cmd.request_id)
                        )?
                        .is_some());
                    if matches!(cmd.outcome, Outcome::Push(_)) {
                        pushes += 1;
                    }
                }
            }
            Ok(())
        })
        .expect("read");
    assert!(pushes > 10, "push outcomes carried items");
}

// ---------------------------------------------------------------------------
// The lead's split of the append wake (`Notify::wake_append`)
// ---------------------------------------------------------------------------

#[test]
fn append_wakes_come_through_wake_append_and_releases_through_wake() {
    for shards in [1usize, 4] {
        let node = Node::new(&format!("wake-kinds-{shards}"));
        let calls = Arc::new(Calls {
            append_wakes: true,
            ..Default::default()
        });
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            sharded(base(), shards, 1),
            calls.clone(),
        )
        .expect("open");
        let mut w = Wide::new(0x3A4E_0001);
        for _ in 0..60 {
            a.apply(&w.next()).expect("apply");
        }
        let log = calls.log.lock().unwrap().clone();
        let appends = log.iter().filter(|l| l.starts_with("wake_append ")).count();
        let group_wakes = log
            .iter()
            .filter(|l| l.starts_with("wake ") && l.ends_with(")") && l.contains("Some("))
            .count();
        let queue_wakes = log.iter().filter(|l| l.ends_with(" None")).count();
        assert!(appends > 50, "{shards} shards: append wakes {appends}");
        assert!(group_wakes > 0, "{shards} shards: lease-release wakes");
        assert!(queue_wakes > 0, "{shards} shards: queue-wide wakes");
    }
}

#[test]
fn a_wide_entry_really_runs_on_several_shards() {
    // Not a property of state: that the pool is used at all (the tests above
    // would pass with every run executed here), and that no shard writer
    // outlives its run — the store commits only with none alive.
    let node = Node::new("spread");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        sharded(base(), 4, 1),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    let mut w = Wide::new(0x5B4E_AD00);
    for _ in 0..80 {
        a.apply(&w.next()).expect("apply");
    }
    let (parallel, sequential) = a.run_counts();
    assert!(
        parallel > 40,
        "runs on the pool: {parallel} (and {sequential} here)"
    );
    assert_eq!(
        node.store().shard_writers(),
        0,
        "no shard writer outlives a run"
    );
}

// ---------------------------------------------------------------------------
// B41: the caches kept across commits see every catalogue change at once
// ---------------------------------------------------------------------------

fn one(entry_index: u64, now: i64, pid_base: u64, id: u64, effects: Vec<Effect>) -> Committed {
    let mut e = Entry::new(now, pid_base, 1);
    e.add_command(super::apply::request_id(id), Outcome::Empty, effects)
        .expect("cmd");
    Committed {
        index: entry_index,
        term: 1,
        entry: Arc::new(e),
    }
}

fn app(pid: Pid, base: u64, count: u32, now: i64) -> Effect {
    Effect::Append {
        pid,
        bucket: pid as u16,
        base_offset: base,
        count,
        created_at_us: now,
        hashes: hashes(pid * 1000 + base, count),
        blob: vec![1; 24],
    }
}

#[test]
fn the_append_path_sees_catalogue_changes_at_once() {
    use crate::rsm::store::keys::{self, Counter};
    use crate::rsm::store::TypedReads;
    for shards in [1usize, 4] {
        let node = Node::new(&format!("catalogue-{shards}"));
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            sharded(base(), shards, 1),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        let t = BASE_US;
        let (tn, qn) = ("t1", "orders");
        let group = |g: &str| Effect::GroupUpsert {
            tenant: tn.into(),
            queue: qn.into(),
            group: g.into(),
            meta: group_meta(3, t),
        };
        let create = |pid: Pid| Effect::PartitionCreate {
            pid,
            uuid: uuid(pid),
            tenant: tn.into(),
            queue: qn.into(),
            partition: format!("p{pid}"),
            created_at_us: t,
        };
        let mut qc = queue_config(t);
        a.apply(&one(
            1,
            t,
            1,
            1,
            vec![
                Effect::QueueUpsert {
                    tenant: tn.into(),
                    queue: qn.into(),
                    cfg: qc.clone(),
                },
                group("g1"),
                create(1),
            ],
        ))
        .expect("setup");
        // The group list is read (and cached) here...
        a.apply(&one(2, t + 10, 2, 2, vec![app(1, 0, 2, t + 10)]))
            .expect("append");
        a.commit().expect("commit");
        // ...a new group, and an append in the same entry and after a commit:
        // both must count it.
        a.apply(&one(
            3,
            t + 20,
            2,
            3,
            vec![group("g2"), app(1, 2, 3, t + 20)],
        ))
        .expect("group + append");
        a.commit().expect("commit");
        a.apply(&one(4, t + 30, 2, 4, vec![app(1, 5, 1, t + 30)]))
            .expect("append");
        // A queue upsert (a new delay) and a new partition's first frame.
        qc.delayed_processing = 5;
        a.apply(&one(
            5,
            t + 40,
            2,
            5,
            vec![
                Effect::QueueUpsert {
                    tenant: tn.into(),
                    queue: qn.into(),
                    cfg: qc.clone(),
                },
                create(2),
                app(2, 0, 1, t + 40),
            ],
        ))
        .expect("delay");
        a.commit().expect("commit");
        // The first group goes: an append after it must not bring its
        // counters back.
        a.apply(&one(
            6,
            t + 50,
            3,
            6,
            vec![
                Effect::GroupDelete {
                    tenant: tn.into(),
                    queue: qn.into(),
                    group: "g1".into(),
                },
                app(1, 6, 2, t + 50),
            ],
        ))
        .expect("group delete + append");
        settle(&mut a);
        node.store()
            .read(|r| {
                // g2 saw the append in its own entry (3), the next one (1) and
                // the last one (2); the delayed partition's frame (1) too.
                assert_eq!(
                    r.counter_at(&keys::counter_group(tn, qn, "g2", Counter::Pending))?,
                    3 + 1 + 1 + 2,
                    "{shards} shards: g2 pending"
                );
                assert_eq!(
                    r.counter_at(&keys::counter_group(tn, qn, "g1", Counter::Pending))?,
                    0
                );
                assert!(
                    r.get_raw(
                        Keyspace::Counters,
                        &keys::counter_group(tn, qn, "g1", Counter::Pending)
                    )?
                    .is_none(),
                    "{shards} shards: no g1 counter row recreated"
                );
                Ok(())
            })
            .expect("read");
    }
}

// ---------------------------------------------------------------------------
// Wakes: key order, one per event, releases before appends, queue-wide once
// ---------------------------------------------------------------------------

#[test]
fn wakes_come_in_key_order_one_per_event() {
    for shards in [1usize, 2, 4] {
        let node = Node::new(&format!("wake-order-{shards}"));
        let calls = Arc::new(Calls {
            append_wakes: true,
            ..Default::default()
        });
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            sharded(base(), shards, 1),
            calls.clone(),
        )
        .expect("open");
        let t = BASE_US;
        let mut setup = Vec::new();
        for q in ["qa", "qb"] {
            setup.push(Effect::QueueUpsert {
                tenant: "t1".into(),
                queue: q.into(),
                cfg: queue_config(t),
            });
        }
        for (q, g) in [("qa", "g2"), ("qa", "g1"), ("qb", "g1")] {
            setup.push(Effect::GroupUpsert {
                tenant: "t1".into(),
                queue: q.into(),
                group: g.into(),
                meta: group_meta(1, t),
            });
        }
        for (pid, q) in [(1u64, "qa"), (2, "qa"), (3, "qb")] {
            setup.push(Effect::PartitionCreate {
                pid,
                uuid: uuid(pid),
                tenant: "t1".into(),
                queue: q.into(),
                partition: format!("p{pid}"),
                created_at_us: t,
            });
        }
        a.apply(&one(1, t, 1, 1, setup)).expect("setup");
        // pid 2 gets frames and a lease for g1.
        let mut lease: CursorRow = fresh_cursor(-1, t);
        lease.worker = Some("w".into());
        lease.lease_expires_at_us = Some(t + 60_000_000);
        lease.lease_acquired_at_us = Some(t + 5);
        lease.batch_end = Some(2);
        a.apply(&one(
            2,
            t + 5,
            4,
            2,
            vec![
                app(2, 0, 3, t + 5),
                Effect::CursorSet {
                    pid: 2,
                    group: "g1".into(),
                    row: lease,
                },
            ],
        ))
        .expect("lease");
        calls.log.lock().unwrap().clear();

        // The entry: a release that leaves work (a wake through `wake`), then
        // appends to qb, qa, qa, qa.
        let mut e = Entry::new(t + 10, 4, 1);
        e.add_command(
            super::apply::request_id(10),
            Outcome::Empty,
            vec![Effect::CursorSet {
                pid: 2,
                group: "g1".into(),
                row: fresh_cursor(0, t),
            }],
        )
        .expect("cmd");
        for (id, eff) in [
            (11, app(3, 0, 1, t + 10)),
            (12, app(1, 0, 2, t + 10)),
            (13, app(2, 3, 1, t + 10)),
            (14, app(1, 2, 1, t + 10)),
        ] {
            e.add_command(super::apply::request_id(id), Outcome::Empty, vec![eff])
                .expect("cmd");
        }
        a.apply(&Committed {
            index: 3,
            term: 1,
            entry: Arc::new(e),
        })
        .expect("entry");
        let got: Vec<String> = calls
            .log
            .lock()
            .unwrap()
            .iter()
            .filter(|l| l.starts_with("wake"))
            .cloned()
            .collect();
        let mut want = vec![
            "wake t1 qa None".to_string(),
            "wake t1 qa Some(\"g1\")".into(),
        ];
        want.extend(std::iter::repeat_n("wake_append t1 qa g1".to_string(), 3));
        want.extend(std::iter::repeat_n("wake_append t1 qa g2".to_string(), 3));
        want.push("wake t1 qb None".into());
        want.push("wake_append t1 qb g1".into());
        assert_eq!(got, want, "{shards} shards");
    }
}

// ---------------------------------------------------------------------------
// Measurement (not a gate)
// ---------------------------------------------------------------------------

/// Apply throughput at one shard and at several, on the target shape: pushes
/// of one message to each of many partitions (each message its own key),
/// claims and acks, on the live path (queue logs external). Prints events per
/// second; the entries are built before the clock starts. Run with:
///   cargo test --profile fastrel --lib rsm::tests::apply_shards::measure \
///     -- --ignored --nocapture
/// `QUEEN_MEASURE_PARTS` (default 200000) and `QUEEN_MEASURE_ENTRIES` (300)
/// size it.
#[test]
#[ignore = "measurement, not a gate; run with --ignored --nocapture"]
fn measure_sharded_apply() {
    let parts: u64 = std::env::var("QUEEN_MEASURE_PARTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(200_000);
    let entries: u64 = std::env::var("QUEEN_MEASURE_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(300);
    const APPENDS: u64 = 600;
    const CURSORS: u64 = 200;
    let live = ApplyConfig {
        qlog: true,
        qlog_writer_external: true,
        ..base()
    };
    let t = BASE_US;
    // Setup: one queue, one group, `parts` partitions, in entries of 5000.
    let mut setup: Vec<Committed> = Vec::new();
    let mut idx = 0u64;
    let mut pid = 1u64;
    while pid <= parts {
        idx += 1;
        let mut e = Entry::new(t + idx as i64, pid, 1);
        let mut effects = Vec::new();
        if idx == 1 {
            effects.push(Effect::QueueUpsert {
                tenant: "t1".into(),
                queue: "q".into(),
                cfg: queue_config(t),
            });
            effects.push(Effect::GroupUpsert {
                tenant: "t1".into(),
                queue: "q".into(),
                group: "g1".into(),
                meta: group_meta(1, t),
            });
        }
        let upto = (pid + 5000).min(parts + 1);
        while pid < upto {
            effects.push(Effect::PartitionCreate {
                pid,
                uuid: uuid(pid),
                tenant: "t1".into(),
                queue: "q".into(),
                partition: format!("k{pid}"),
                created_at_us: t,
            });
            pid += 1;
        }
        e.add_command(super::apply::request_id(idx), Outcome::Empty, effects)
            .expect("cmd");
        setup.push(Committed {
            index: idx,
            term: 1,
            entry: Arc::new(e),
        });
    }
    // The measured entries: pushes of `PER_PUSH` messages, each message on
    // its own random partition (one `Append` of one message per key, one
    // outcome per push), then acks of partitions that have frames, a few per
    // command.
    const PER_PUSH: u64 = 10;
    const PER_ACK: u64 = 4;
    let mut rng = Rng(0xBE4C_0001);
    let mut next = vec![0u64; parts as usize + 1];
    let mut committed = vec![-1i64; parts as usize + 1];
    let mut work: Vec<Committed> = Vec::new();
    for n in 0..entries {
        idx += 1;
        let now = t + 1_000_000 + n as i64 * 1_000;
        let mut e = Entry::new(now, parts + 1, 1);
        let mut id = 1_000_000 + n * 10_000;
        for _ in 0..APPENDS / PER_PUSH {
            let mut effects = Vec::new();
            for _ in 0..PER_PUSH {
                let p = 1 + rng.below(parts);
                let base = next[p as usize];
                next[p as usize] += 1;
                effects.push(Effect::Append {
                    pid: p,
                    bucket: (p % 256) as u16,
                    base_offset: base,
                    count: 1,
                    created_at_us: now,
                    hashes: hashes(p ^ base, 1),
                    blob: (crate::rsm::segments::frame::encoded_len(1, 200) as u32)
                        .to_le_bytes()
                        .to_vec(),
                });
            }
            id += 1;
            e.add_command(super::apply::request_id(id), Outcome::Empty, effects)
                .expect("cmd");
        }
        for _ in 0..CURSORS / PER_ACK {
            let mut effects = Vec::new();
            for _ in 0..PER_ACK {
                let p = 1 + rng.below(parts);
                let last = next[p as usize] as i64 - 1;
                if last < 0 || committed[p as usize] >= last {
                    continue;
                }
                committed[p as usize] = last;
                effects.push(Effect::CursorSet {
                    pid: p,
                    group: "g1".into(),
                    row: fresh_cursor(last, now),
                });
            }
            if !effects.is_empty() {
                id += 1;
                e.add_command(super::apply::request_id(id), Outcome::Empty, effects)
                    .expect("cmd");
            }
        }
        work.push(Committed {
            index: idx,
            term: 1,
            entry: Arc::new(e),
        });
    }
    let events: usize = work.iter().map(|c| c.entry.effects.len()).sum();
    for shards in [1usize, 2, 4, 8] {
        let node = Node::new(&format!("measure-{shards}"));
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            sharded(live, shards, 8),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for c in &setup {
            a.apply(c).expect("setup");
        }
        a.durable_point().expect("durable point");
        let t0 = std::time::Instant::now();
        let mut commit_ns = 0u128;
        let mut apply_ns = 0u128;
        for (i, c) in work.iter().enumerate() {
            let a0 = std::time::Instant::now();
            a.apply(c).expect("apply");
            apply_ns += a0.elapsed().as_nanos();
            if i % 4 == 3 {
                let c0 = std::time::Instant::now();
                a.commit().expect("commit");
                commit_ns += c0.elapsed().as_nanos();
            }
        }
        let c0 = std::time::Instant::now();
        a.commit().expect("commit");
        commit_ns += c0.elapsed().as_nanos();
        let secs = t0.elapsed().as_secs_f64();
        let (par, seq) = a.run_counts();
        println!(
            "shards {shards}: {events} events in {secs:.3} s = {:.0} events/s \
             ({:.2} us/event; apply {:.1} ms, commits {:.1} ms; runs {par} parallel, {seq} here)",
            events as f64 / secs,
            secs * 1e6 / events as f64,
            apply_ns as f64 / 1e6,
            commit_ns as f64 / 1e6,
        );
    }
}
