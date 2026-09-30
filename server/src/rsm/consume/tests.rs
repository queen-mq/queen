//! The engine against a real store: a tiny harness that writes what apply
//! would (queues, partitions, `txns` rows, the tails) and applies the engine's
//! checkpoints back to the store, so a second engine over the same store is
//! a new leader loading committed state. The parity cases of the planner's
//! pop/ack/positions tests live in `tests_pop.rs` and `tests_ack.rs`; this
//! file holds the harness and the engine's own properties (checkpoints,
//! durability of answers, loading, leadership, transactions, long polls).

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::rsm::batcher::{Command, Reply};
use crate::rsm::dedup::IndexMode;
use crate::rsm::effect::{CursorRow, Effect, Pid, QueueConfig};
use crate::rsm::entry::{AckOutcome, AckResult, Outcome, PopClaim, PopOutcome, RenewOutcome};
use crate::rsm::planner::txn::TxnCommand;
use crate::rsm::planner::{
    AckCommand, AckItem, AckPositionalCommand, AckStatus, AckTarget, DlqSnapshot, NackCommand,
    PopCommand, RenewCommand, SubIntent,
};
use crate::rsm::store::rows::{DlqRow, GroupRow, PartitionRow};
use crate::rsm::store::{HeedStore, Store, StoreOpts, TypedReads, TypedWrites, Writes};

use super::{wall_us, Engine, Knobs, Served};

pub(super) const T: &str = "t1";

static SEQ: AtomicU64 = AtomicU64::new(0);

pub(super) fn knobs() -> Knobs {
    Knobs {
        lanes: 4,
        ckpt_ms: 5,
        fast: false,
        lease_ms: 400,
        skew_us: 500_000,
        margin_us: 50_000,
        rows_per_cmd: 2,
        txn_ttl_us: 30_000_000,
    }
}

pub(super) fn qcfg() -> QueueConfig {
    QueueConfig {
        id: {
            let mut u = [0u8; 16];
            u[0] = 0xC0;
            u
        },
        namespace: None,
        task: None,
        priority: 0,
        lease_time: 30,
        retry_limit: 3,
        retry_delay: 0,
        ttl: 0,
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
        dedup_window_seconds: 0,
        retention_sink_hold: String::new(),
        retention_sink_hold_max_seconds: 0,
        created_at_us: 0,
    }
}

pub(super) fn mode(m: &str) -> SubIntent {
    SubIntent {
        mode: m.to_string(),
        from_us: None,
        now: false,
    }
}

pub(super) fn hash(txn: &str) -> [u8; 16] {
    crate::util::txn_hash128(txn)
}

/// One node's store and engine, plus a clock the commands are stamped with.
pub(super) struct H {
    pub dir: PathBuf,
    pub store: Arc<HeedStore>,
    pub e: Arc<Engine>,
    pub now: i64,
    next_pid: u64,
    rid: u64,
}

impl Drop for H {
    fn drop(&mut self) {
        self.e.on_step_down();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

impl H {
    pub fn new(tag: &str) -> H {
        H::with(tag, knobs())
    }

    pub fn with(tag: &str, k: Knobs) -> H {
        let dir = std::env::temp_dir().join(format!(
            "queen-consume-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let store = Arc::new(
            HeedStore::open(
                &dir.join("store"),
                &StoreOpts {
                    map_bytes: Some(64 << 20),
                    ..Default::default()
                },
            )
            .expect("open store"),
        );
        let e = Engine::with_knobs(store.clone(), k);
        e.set_mode(IndexMode::Txns);
        e.on_leader(1, u64::MAX);
        e.serve_after_us.store(0, Ordering::Release);
        H {
            dir,
            store,
            e,
            now: wall_us(),
            next_pid: 1,
            rid: 0,
        }
    }

    /// A new leader over the same committed state (the old one steps down).
    pub fn failover(&mut self) {
        self.failover_with(knobs());
    }

    pub fn failover_with(&mut self, k: Knobs) {
        self.e.on_step_down();
        let e = Engine::with_knobs(self.store.clone(), k);
        e.set_mode(IndexMode::Txns);
        e.on_leader(2, u64::MAX);
        e.serve_after_us.store(0, Ordering::Release);
        self.e = e;
    }

    pub fn advance(&mut self, us: i64) {
        self.now += us;
    }

    pub fn rid(&mut self) -> [u8; 16] {
        self.rid += 1;
        let mut id = [0u8; 16];
        id[0..8].copy_from_slice(&self.rid.to_be_bytes());
        id[8] = 0xE7;
        id
    }

    pub fn queue(&mut self, q: &str, cfg: QueueConfig) {
        let mut w = self.store.write().expect("write");
        w.put_queue(T, q, &cfg).expect("queue");
        w.commit().expect("commit");
        drop(w);
        self.e.on_effect(
            &Effect::QueueUpsert {
                tenant: T.to_string(),
                queue: q.to_string(),
                cfg,
            },
            0,
        );
    }

    /// Append `txns` to `(q, part)` stamped `created`: what apply writes for
    /// an `Append` (the queue and the partition created on first use), then
    /// the hook apply calls.
    pub fn push_at(&mut self, q: &str, part: &str, txns: &[&str], created: i64) -> Pid {
        let mut w = self.store.write().expect("write");
        if w.queue(T, q).expect("read").is_none() {
            w.put_queue(T, q, &qcfg()).expect("queue");
        }
        let (pid, new) = match w.pid_of(T, q, part).expect("read") {
            Some(p) => (p, false),
            None => {
                let pid = self.next_pid;
                self.next_pid += 1;
                let row = PartitionRow::new([pid as u8; 16], T, q, part, created);
                w.create_partition(pid, &row).expect("create");
                (pid, true)
            }
        };
        let mut row = w.partition(pid).expect("read").expect("row");
        let base = (row.last_offset + 1) as u64;
        let end = base + txns.len() as u64 - 1;
        let accepted: Vec<([u8; 16], u64)> = txns
            .iter()
            .enumerate()
            .map(|(i, t)| (hash(t), base + i as u64))
            .collect();
        crate::rsm::dedup::record_txns(&mut w, pid, base, end, &accepted, created).expect("txns");
        row.last_offset = end as i64;
        row.last_created_at_us = created;
        row.last_write_at_us = created;
        w.put_partition(pid, &row).expect("tail");
        w.commit().expect("commit");
        drop(w);
        if new {
            self.e.on_effect(
                &Effect::PartitionCreate {
                    pid,
                    uuid: [pid as u8; 16],
                    tenant: T.to_string(),
                    queue: q.to_string(),
                    partition: part.to_string(),
                    created_at_us: created,
                },
                0,
            );
        }
        self.e.on_append(pid, end as i64);
        pid
    }

    pub fn push(&mut self, q: &str, part: &str, txns: &[&str]) -> Pid {
        let at = self.now;
        self.now += 1;
        self.push_at(q, part, txns, at)
    }

    /// Move a partition's log start (retention).
    pub fn watermark(&mut self, pid: Pid, log_start: u64) {
        let mut w = self.store.write().expect("write");
        let mut row = w.partition(pid).expect("read").expect("row");
        row.log_start = log_start;
        w.put_partition(pid, &row).expect("put");
        w.commit().expect("commit");
        drop(w);
        self.e.on_effect(
            &Effect::Watermark {
                pid,
                log_start,
                txns_start: 0,
            },
            0,
        );
    }

    /// Apply one effect to the store as apply would (the rows the tests read).
    fn apply_effect(w: &mut <HeedStore as Store>::Write<'_>, e: &Effect) {
        match e {
            Effect::CursorSet { pid, group, row } => {
                w.put_cursor(*pid, group, row).expect("cursor")
            }
            Effect::CursorDelete { pid, group } => {
                w.del_cursor(*pid, group).expect("del cursor");
            }
            Effect::DlqInsert {
                dlq_id,
                tenant,
                queue,
                pid,
                group,
                offset,
                message_id,
                txn,
                payload,
                error,
                retry_count,
                failed_at_us,
            } => w
                .put_dlq(
                    tenant,
                    queue,
                    dlq_id,
                    &DlqRow {
                        pid: *pid,
                        group: group.clone(),
                        offset: *offset,
                        message_id: *message_id,
                        txn: txn.clone(),
                        payload: payload.clone(),
                        error: error.clone(),
                        retry_count: *retry_count,
                        failed_at_us: *failed_at_us,
                    },
                )
                .expect("dlq"),
            Effect::GroupUpsert {
                tenant,
                queue,
                group,
                meta,
            } => w
                .put_group(
                    tenant,
                    queue,
                    group,
                    &GroupRow {
                        meta: meta.clone(),
                        reg_index: 0,
                        reg_effect: 0,
                    },
                )
                .expect("group"),
            Effect::QueueUpsert { tenant, queue, cfg } => {
                w.put_queue(tenant, queue, cfg).expect("queue")
            }
            other => panic!("the engine logged an unexpected effect: {other:?}"),
        }
    }

    /// Apply a list of effects (a checkpoint's, a transaction's) and report
    /// them to the engine as apply does.
    pub fn apply(&mut self, effects: &[Effect]) {
        let mut w = self.store.write().expect("write");
        for e in effects {
            H::apply_effect(&mut w, e);
        }
        w.commit().expect("commit");
        drop(w);
        for e in effects {
            self.e.on_effect(e, 0);
        }
    }

    /// Take a checkpoint and commit it: the number of effects it carried.
    pub fn checkpoint(&mut self) -> usize {
        let Some(c) = self.e.take_checkpoint(self.now) else {
            return 0;
        };
        let effects: Vec<Effect> = c.commands.iter().flat_map(|c| c.effects.clone()).collect();
        self.apply(&effects);
        self.e.checkpoint_resolved(c.ticket, true);
        effects.len()
    }

    /// Take a checkpoint and REFUSE it (nothing applied).
    pub fn checkpoint_refused(&mut self) -> usize {
        let Some(c) = self.e.take_checkpoint(self.now) else {
            return 0;
        };
        let n = c.commands.iter().map(|c| c.effects.len()).sum();
        self.e.checkpoint_resolved(c.ticket, false);
        n
    }

    /// Serve a command to its answer, committing checkpoints as they come.
    pub fn serve(&mut self, cmd: Command) -> Reply {
        // The clock work at the commands' clock (lease expiries, holds).
        self.e.tick(self.now);
        match self.e.serve(&cmd, self.now) {
            Served::Now(r) => r,
            Served::NotMine => panic!("not the engine's: {cmd:?}"),
            Served::Later(mut rx) => {
                self.e.wait_loads();
                for _ in 0..2000 {
                    self.checkpoint();
                    match rx.try_recv() {
                        Ok(r) => return r,
                        Err(tokio::sync::oneshot::error::TryRecvError::Empty) => {
                            std::thread::sleep(std::time::Duration::from_millis(1))
                        }
                        Err(e) => panic!("the answer was dropped: {e}"),
                    }
                }
                panic!("no answer for {cmd:?}")
            }
        }
    }

    /// Serve expecting a held answer (the group loaded first if it had to).
    pub fn later(&mut self, cmd: Command) -> tokio::sync::oneshot::Receiver<Reply> {
        self.e.tick(self.now);
        match self.e.serve(&cmd, self.now) {
            Served::Later(rx) => {
                self.e.wait_loads();
                rx
            }
            Served::Now(r) => panic!("answered at once: {r:?}"),
            Served::NotMine => panic!("not mine"),
        }
    }

    /// Serve and expect an outcome.
    pub fn outcome(&mut self, cmd: Command) -> Outcome {
        match self.serve(cmd) {
            Reply::Done { outcome, .. } => outcome,
            other => panic!("expected an outcome, got {other:?}"),
        }
    }

    pub fn pop_cmd(&mut self, queue: &str, group: &str, worker: &str) -> PopCommand {
        PopCommand {
            wait: false,
            request_id: self.rid(),
            tenant: T.to_string(),
            queue: queue.to_string(),
            partition: None,
            group: group.to_string(),
            worker: worker.to_string(),
            budget: 100,
            max_parts: 10,
            lease_seconds: 60,
            auto_ack: false,
            conflate: false,
            sub: mode("all"),
            skip_window_debounce: false,
            namespace: String::new(),
            task: String::new(),
            create_cfg: Some(qcfg()),
            deadline_us: 0,
        }
    }

    pub fn wildcard(&mut self, q: &str, g: &str, w: &str) -> Vec<PopClaim> {
        self.wildcard_with(q, g, w, |_| {})
    }

    pub fn wildcard_with(
        &mut self,
        q: &str,
        g: &str,
        w: &str,
        f: impl FnOnce(&mut PopCommand),
    ) -> Vec<PopClaim> {
        let mut c = self.pop_cmd(q, g, w);
        f(&mut c);
        claims(&self.outcome(Command::PopWildcard(c)))
    }

    pub fn pinned(&mut self, q: &str, part: &str, g: &str, w: &str) -> Vec<PopClaim> {
        self.pinned_with(q, part, g, w, |_| {})
    }

    pub fn pinned_with(
        &mut self,
        q: &str,
        part: &str,
        g: &str,
        w: &str,
        f: impl FnOnce(&mut PopCommand),
    ) -> Vec<PopClaim> {
        let mut c = self.pop_cmd(q, g, w);
        c.partition = Some(part.to_string());
        f(&mut c);
        claims(&self.outcome(Command::PopPinned(c)))
    }

    pub fn ack_target(
        &self,
        pid: Pid,
        q: &str,
        g: &str,
        w: &str,
        items: &[(&str, AckStatus)],
    ) -> AckTarget {
        AckTarget {
            pid,
            tenant: T.to_string(),
            queue: q.to_string(),
            group: g.to_string(),
            worker: w.to_string(),
            items: items
                .iter()
                .map(|(t, s)| AckItem {
                    hash: hash(t),
                    status: *s,
                    error: None,
                    snapshot: matches!(s, AckStatus::Failed | AckStatus::Dlq).then(|| {
                        DlqSnapshot {
                            message_id: None,
                            txn: t.to_string(),
                            payload: format!("{{\"t\":\"{t}\"}}").into_bytes(),
                        }
                    }),
                })
                .collect(),
        }
    }

    pub fn ack(
        &mut self,
        pid: Pid,
        q: &str,
        g: &str,
        w: &str,
        items: &[(&str, AckStatus)],
    ) -> AckResult {
        let t = self.ack_target(pid, q, g, w, items);
        let c = AckCommand {
            request_id: self.rid(),
            targets: vec![t],
        };
        one_ack(&self.outcome(Command::Ack(c)))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn ack_pos(
        &mut self,
        pid: Pid,
        q: &str,
        g: &str,
        w: &str,
        upto: Option<i64>,
        ok: bool,
        release: bool,
        acked_count: i32,
    ) -> Reply {
        let c = AckPositionalCommand {
            request_id: self.rid(),
            pid,
            tenant: T.to_string(),
            queue: q.to_string(),
            group: g.to_string(),
            worker: w.to_string(),
            upto,
            ok,
            release_lease: release,
            acked_count,
        };
        self.serve(Command::AckPositional(c))
    }

    pub fn nack(&mut self, pid: Pid, q: &str, g: &str, w: &str) -> Reply {
        let c = NackCommand {
            request_id: self.rid(),
            pid,
            tenant: T.to_string(),
            queue: q.to_string(),
            group: g.to_string(),
            worker: w.to_string(),
        };
        self.serve(Command::Nack(c))
    }

    pub fn renew(&mut self, w: &str, seconds: i32) -> RenewOutcome {
        let c = RenewCommand {
            request_id: self.rid(),
            tenant: None,
            worker: w.to_string(),
            seconds,
        };
        match self.outcome(Command::Renew(c)) {
            Outcome::Renew(r) => r,
            o => panic!("not a renew: {o:?}"),
        }
    }

    pub fn pid_of(&self, q: &str, part: &str) -> Pid {
        self.store
            .read(|r| r.pid_of(T, q, part))
            .expect("read")
            .expect("partition")
    }

    pub fn cursor(&self, pid: Pid, g: &str) -> Option<CursorRow> {
        self.store.read(|r| r.cursor(pid, g)).expect("read")
    }

    pub fn group_exists(&self, q: &str, g: &str) -> bool {
        self.store
            .read(|r| Ok(r.group(T, q, g)?.is_some()))
            .expect("read")
    }

    pub fn dlq_rows(&self) -> Vec<DlqRow> {
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
            .expect("read")
    }

    pub fn txn(&self, id: [u8; 16]) -> TxnCommand {
        TxnCommand {
            request_id: id,
            tenant: T.to_string(),
            pushes: Vec::new(),
            acks: Vec::new(),
            positional_acks: Vec::new(),
            kv: Vec::new(),
            timers: Vec::new(),
            extra_effects: Vec::new(),
            allow_duplicate: false,
            positions: Vec::new(),
            engine_acks: AckOutcome::default(),
        }
    }
}

pub(super) fn claims(o: &Outcome) -> Vec<PopClaim> {
    match o {
        Outcome::Pop(PopOutcome { claims }) => claims.clone(),
        other => panic!("not a pop outcome: {other:?}"),
    }
}

pub(super) fn only(v: Vec<PopClaim>) -> PopClaim {
    assert_eq!(v.len(), 1, "expected exactly one claim: {v:?}");
    v.into_iter().next().unwrap()
}

pub(super) fn one_ack(o: &Outcome) -> AckResult {
    match o {
        Outcome::Ack(a) => {
            assert_eq!(a.results.len(), 1, "one target: {a:?}");
            a.results[0].clone()
        }
        other => panic!("not an ack outcome: {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Checkpoints and durable answers
// ---------------------------------------------------------------------------

#[test]
fn a_claim_is_answered_only_once_its_checkpoint_committed() {
    let mut h = H::new("durable-claim");
    h.push("q", "p0", &["a", "b"]);
    let c = h.pop_cmd("q", "g", "w1");
    let mut rx = h.later(Command::PopWildcard(c));
    assert!(rx.try_recv().is_err(), "nothing before the checkpoint");
    // A refused checkpoint does not release it; the next one does.
    assert!(h.checkpoint_refused() > 0);
    assert!(
        rx.try_recv().is_err(),
        "a refused checkpoint releases nothing"
    );
    assert!(h.checkpoint() > 0);
    match rx.try_recv() {
        Ok(Reply::Done {
            outcome: Outcome::Pop(p),
            ..
        }) => assert_eq!(p.claims.len(), 1),
        other => panic!("{other:?}"),
    }
    let pid = h.pid_of("q", "p0");
    let row = h.cursor(pid, "g").expect("the lease row was written");
    assert_eq!(row.batch_end, Some(1));
    assert_eq!(row.worker.as_deref(), Some("w1"));
    assert!(
        row.delivered.is_empty(),
        "checkpoints carry no delivered set"
    );
    assert!(
        h.group_exists("q", "g"),
        "the registration was checkpointed"
    );
}

/// The batcher answers a checkpoint when its entry COMMITS, so the leader
/// applies the checkpoint's own cursor rows after the engine answered and
/// moved on. A row of this term (an entry past the log's end at election) is
/// the engine's own and never rolls it back — adopting it here would hand the
/// acked batch's lease back to its first worker.
#[test]
fn a_cursor_row_of_this_term_applied_after_its_answer_does_not_roll_back() {
    let mut h = H::new("own-term");
    h.e.on_leader(1, 100);
    h.e.serve_after_us.store(0, Ordering::Release);
    h.push("q", "p0", &["a", "b"]);
    let pid = h.pid_of("q", "p0");
    // The claim, answered once its checkpoint committed — not yet applied.
    let c = h.pop_cmd("q", "g", "w1");
    let mut rx = h.later(Command::PopWildcard(c));
    let lease = h.e.take_checkpoint(h.now).expect("the lease checkpoint");
    h.e.checkpoint_resolved(lease.ticket, true);
    match rx.try_recv() {
        Ok(Reply::Done {
            outcome: Outcome::Pop(p),
            ..
        }) => assert_eq!(p.claims.len(), 1),
        other => panic!("{other:?}"),
    }
    // The batch is acked (its own checkpoint commits and applies).
    let r = h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)],
    );
    assert_eq!((r.committed, r.acked), (1, 2), "{r:?}");
    // Now the lease checkpoint's entry (this term's, index 101) applies.
    let rows: Vec<Effect> = lease
        .commands
        .iter()
        .flat_map(|c| c.effects.clone())
        .collect();
    assert!(rows.iter().any(|e| matches!(e, Effect::CursorSet { .. })));
    for e in &rows {
        h.e.on_effect(e, 101);
    }
    // Nothing rolled back: the next message goes to another worker at once,
    // and the acked ones are not delivered again.
    h.push("q", "p0", &["c"]);
    let again = h.wildcard("q", "g", "w2");
    assert_eq!(again.len(), 1, "the partition is not leased to w1 again");
    assert_eq!(again[0].start_offset, 2, "a and b stay acked");
}

/// Apply never waits for the engine: an append to a partition whose shard
/// is busy is queued, and the serve thread arms it. (Taking the lock here
/// while the test holds it would deadlock the test.)
#[test]
fn an_append_to_a_busy_shard_is_queued_not_waited_for() {
    let mut h = H::new("busy-append");
    h.push("q", "p0", &["a"]);
    let pid = h.pid_of("q", "p0");
    assert_eq!(h.wildcard("q", "g", "w1").len(), 1);
    let r = h.ack(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]);
    assert_eq!(r.committed, 0, "{r:?}");
    {
        let e = h.e.clone();
        let _busy = e.shards[super::state::shard_of(pid)].lock().expect("shard");
        h.push("q", "p0", &["b"]);
    }
    h.e.drain_appends();
    let c = h.wildcard("q", "g", "w2");
    assert_eq!(c.len(), 1, "the queued append was armed");
    assert_eq!((c[0].start_offset, c[0].end_offset), (1, 1));
}

#[test]
fn fast_mode_answers_at_once_and_still_checkpoints() {
    let mut k = knobs();
    k.fast = true;
    let mut h = H::with("fast", k);
    h.queue("q", qcfg());
    // First contact (the group's load) answers through the load.
    assert!(h.wildcard("q", "g", "w1").is_empty());
    let pid = h.push("q", "p0", &["a"]);
    let c = h.pop_cmd("q", "g", "w1");
    match h.e.serve(&Command::PopWildcard(c), h.now) {
        Served::Now(Reply::Done {
            outcome: Outcome::Pop(p),
            ..
        }) => assert_eq!(p.claims.len(), 1),
        _ => panic!("fast mode answers now"),
    }
    assert!(h.cursor(pid, "g").is_none());
    h.checkpoint();
    assert_eq!(h.cursor(pid, "g").unwrap().batch_end, Some(0));
}

#[test]
fn a_checkpoint_is_one_command_per_tenant_and_lane_with_whole_rows() {
    let mut h = H::new("ckpt-shape");
    let mut pids = Vec::new();
    for i in 0..6 {
        pids.push(h.push("q", &format!("p{i}"), &["x"]));
    }
    // Claim all six (one pop) without committing the checkpoint yet.
    let c = h.pop_cmd("q", "g", "w1");
    let _rx = h.later(Command::PopWildcard(c));
    let ck = h.e.take_checkpoint(h.now).expect("a checkpoint");
    let lanes = knobs().lanes;
    for cmd in &ck.commands {
        assert_eq!(cmd.tenant, T);
        let rows: Vec<Pid> = cmd
            .effects
            .iter()
            .filter_map(|e| match e {
                Effect::CursorSet { pid, row, .. } => {
                    assert!(row.worker.is_some() && row.lease_expires_at_us.is_some());
                    assert!(row.delivered.is_empty());
                    Some(*pid)
                }
                _ => None,
            })
            .collect();
        assert!(rows.len() <= knobs().rows_per_cmd, "chunked: {rows:?}");
        if let Some(first) = rows.first() {
            assert!(
                rows.iter().all(|p| p % lanes == first % lanes),
                "one lane per command: {rows:?}"
            );
        }
    }
    let rows: usize = ck
        .commands
        .iter()
        .flat_map(|c| &c.effects)
        .filter(|e| matches!(e, Effect::CursorSet { .. }))
        .count();
    assert_eq!(rows, 6);
    // A second take while it is in flight re-sends nothing.
    assert!(h.e.take_checkpoint(h.now).is_none());
    h.e.checkpoint_resolved(ck.ticket, true);
}

#[test]
fn a_refused_checkpoint_rewrites_the_current_state_not_the_old_rows() {
    let mut h = H::new("ckpt-refused");
    let pid = h.push("q", "p0", &["a", "b"]);
    let c = h.pop_cmd("q", "g", "w1");
    let _rx = h.later(Command::PopWildcard(c));
    let first = h.e.take_checkpoint(h.now).expect("ckpt");
    // While it is in flight the lease is acked (the answer waits).
    let t = h.ack_target(
        pid,
        "q",
        "g",
        "w1",
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)],
    );
    let ack = AckCommand {
        request_id: h.rid(),
        targets: vec![t],
    };
    let mut rx = h.later(Command::Ack(ack));
    // The first is refused: the next checkpoint carries the ACKED row.
    h.e.checkpoint_resolved(first.ticket, false);
    let second = h.e.take_checkpoint(h.now).expect("ckpt");
    let row = second
        .commands
        .iter()
        .flat_map(|c| &c.effects)
        .find_map(|e| match e {
            Effect::CursorSet { row, .. } => Some(row.clone()),
            _ => None,
        })
        .expect("the row");
    assert_eq!(row.committed, 1, "the current state");
    assert!(row.worker.is_none());
    let effects: Vec<Effect> = second
        .commands
        .iter()
        .flat_map(|c| c.effects.clone())
        .collect();
    h.apply(&effects);
    h.e.checkpoint_resolved(second.ticket, true);
    assert!(
        matches!(rx.try_recv(), Ok(Reply::Done { .. })),
        "the ack is answered"
    );
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 1);
}

// ---------------------------------------------------------------------------
// Loading committed state (a new leader)
// ---------------------------------------------------------------------------

#[test]
fn a_new_leader_loads_cursors_and_keeps_live_leases() {
    let mut h = H::new("load-leases");
    let p0 = h.push("q", "p0", &["a", "b"]);
    let p1 = h.push("q", "p1", &["x"]);
    // w1 leases p0 only (budget 2), acked nothing yet.
    let got = h.wildcard_with("q", "g", "w1", |c| {
        c.budget = 2;
        c.max_parts = 1;
    });
    let leased = only(got).pid;
    h.failover();
    // The lease is LIVE on the new leader: another worker gets only the rest.
    let got = h.wildcard("q", "g", "w2");
    assert_eq!(got.len(), 1, "{got:?}");
    assert_ne!(got[0].pid, leased, "the leased partition stays leased");
    assert!(got[0].pid == p0 || got[0].pid == p1);
    // The original worker's full ack lands on the rebuilt delivered set.
    let hs: &[(&str, AckStatus)] = if leased == p0 {
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)]
    } else {
        &[("x", AckStatus::Ok)]
    };
    let r = h.ack(leased, "q", "g", "w1", hs);
    assert!(r.lease_released, "{r:?}");
    assert!(r.stale_hashes.is_empty(), "{r:?}");
}

#[test]
fn a_foreign_lease_is_held_for_the_skew_grace_then_redelivered() {
    let mut h = H::new("load-grace");
    h.push("q", "p0", &["m1"]);
    let first = only(h.wildcard_with("q", "g", "wa", |c| c.lease_seconds = 1));
    assert_eq!(first.worker, "wa");
    h.failover();
    // 1.2 s later the lease ran out by the clock, but another node timed it.
    h.advance(1_200_000);
    assert!(
        h.wildcard("q", "g", "wb").is_empty(),
        "held inside the grace"
    );
    h.advance(400_000);
    let late = only(h.wildcard("q", "g", "wb"));
    assert_eq!(late.worker, "wb");
    assert_eq!(late.delivery_attempt, 2, "the redelivery counts up");
}

#[test]
fn a_lease_this_term_granted_ends_on_time() {
    let mut h = H::new("own-lease");
    h.push("q", "p0", &["m1"]);
    only(h.wildcard_with("q", "g", "wa", |c| c.lease_seconds = 1));
    h.advance(1_200_000);
    assert_eq!(only(h.wildcard("q", "g", "wb")).worker, "wb");
}

#[test]
fn a_new_leader_resumes_from_the_committed_cursor() {
    let mut h = H::new("load-cursor");
    let pid = h.push("q", "p0", &["a", "b", "c"]);
    let c = only(h.wildcard_with("q", "g", "w1", |c| c.budget = 2));
    assert_eq!((c.start_offset, c.end_offset), (0, 1));
    h.ack(
        pid,
        "q",
        "g",
        "w1",
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)],
    );
    h.failover();
    let c = only(h.wildcard("q", "g", "w2"));
    assert_eq!((c.start_offset, c.end_offset), (2, 2));
}

// ---------------------------------------------------------------------------
// Leadership
// ---------------------------------------------------------------------------

#[test]
fn a_step_down_answers_every_held_reply_retry() {
    let mut h = H::new("step-down");
    h.push("q", "p0", &["a"]);
    let c = h.pop_cmd("q", "g", "w1");
    let mut claim_rx = h.later(Command::PopWildcard(c));
    // A long poll of another (empty) queue is parked.
    h.queue("q3", qcfg());
    let mut c = h.pop_cmd("q3", "g3", "w3");
    c.wait = true;
    c.deadline_us = wall_us() + 30_000_000;
    let mut park_rx = h.later(Command::PopWildcard(c));
    h.e.on_step_down();
    assert!(matches!(claim_rx.try_recv(), Ok(Reply::Retry { .. })));
    assert!(matches!(park_rx.try_recv(), Ok(Reply::Retry { .. })));
    // Not the leader any more: every consumption command is Retry.
    let c = h.pop_cmd("q", "g", "w1");
    assert!(matches!(
        h.e.serve(&Command::PopWildcard(c), h.now),
        Served::Now(Reply::Retry { .. })
    ));
}

#[test]
fn a_stale_leader_lease_answers_retry() {
    let mut h = H::new("lease-probe");
    h.push("q", "p0", &["a"]);
    h.e.set_leader_lease(Arc::new(|| Some(std::time::Duration::from_secs(5))));
    let c = h.pop_cmd("q", "g", "w1");
    assert!(matches!(
        h.e.serve(&Command::PopWildcard(c), h.now),
        Served::Now(Reply::Retry { .. })
    ));
    h.e.set_leader_lease(Arc::new(|| Some(std::time::Duration::ZERO)));
    assert_eq!(h.wildcard("q", "g", "w1").len(), 1);
}

#[test]
fn a_new_leader_pauses_then_serves_what_it_held() {
    let mut h = H::new("pause");
    h.push("q", "p0", &["a"]);
    h.e.on_step_down();
    h.e.on_leader(3, u64::MAX);
    let c = h.pop_cmd("q", "g", "w1");
    let mut rx = match h.e.serve(&Command::PopWildcard(c), wall_us()) {
        Served::Later(rx) => rx,
        Served::Now(r) => panic!("served inside the pause: {r:?}"),
        _ => panic!(),
    };
    for _ in 0..3000 {
        h.checkpoint();
        if let Ok(r) = rx.try_recv() {
            match r {
                Reply::Done {
                    outcome: Outcome::Pop(p),
                    ..
                } => {
                    assert_eq!(p.claims.len(), 1);
                    return;
                }
                other => panic!("{other:?}"),
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("the held pop was never served");
}

#[test]
fn followers_hold_nothing() {
    let h = H::new("follower");
    h.e.on_step_down();
    // Apply hooks on a follower are no-ops.
    h.e.on_append(1, 10);
    assert!(h.e.take_checkpoint(h.now).is_none());
    assert!(h.e.ready_count(T, "q", "g").is_none());
}

// ---------------------------------------------------------------------------
// Long polls
// ---------------------------------------------------------------------------

#[test]
fn a_parked_pop_is_answered_when_data_arrives() {
    let mut h = H::new("park-data");
    h.queue("q", qcfg());
    // Register the group first (answered empty once durable).
    assert!(h.wildcard("q", "g", "w1").is_empty());
    let mut c = h.pop_cmd("q", "g", "w1");
    c.wait = true;
    c.deadline_us = wall_us() + 10_000_000;
    h.now = wall_us();
    let mut rx = h.later(Command::PopWildcard(c));
    assert!(rx.try_recv().is_err());
    h.now = wall_us();
    h.push("q", "p0", &["a"]);
    for _ in 0..3000 {
        h.checkpoint();
        if let Ok(r) = rx.try_recv() {
            match r {
                Reply::Done {
                    outcome: Outcome::Pop(p),
                    ..
                } => {
                    assert_eq!(p.claims.len(), 1, "the parked pop got the new message");
                    return;
                }
                other => panic!("{other:?}"),
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    panic!("the parked pop never woke");
}

#[test]
fn a_parked_pop_is_answered_empty_at_its_deadline() {
    let mut h = H::new("park-deadline");
    h.queue("q", qcfg());
    assert!(h.wildcard("q", "g", "w1").is_empty());
    let mut c = h.pop_cmd("q", "g", "w1");
    c.wait = true;
    c.deadline_us = wall_us() + 120_000; // 120 ms, margin 50 ms
    let start = std::time::Instant::now();
    h.now = wall_us();
    let mut rx = h.later(Command::PopWildcard(c));
    loop {
        if let Ok(r) = rx.try_recv() {
            match r {
                Reply::Done {
                    outcome: Outcome::Pop(p),
                    ..
                } => assert!(p.claims.is_empty()),
                other => panic!("{other:?}"),
            }
            let took = start.elapsed();
            assert!(took >= std::time::Duration::from_millis(60), "{took:?}");
            assert!(took < std::time::Duration::from_millis(1000), "{took:?}");
            return;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(3),
            "never answered"
        );
        std::thread::sleep(std::time::Duration::from_millis(2));
    }
}

#[test]
fn a_claim_nobody_receives_is_released() {
    let mut h = H::new("dropped-answer");
    let pid = h.push("q", "p0", &["a"]);
    let c = h.pop_cmd("q", "g", "w1");
    drop(h.later(Command::PopWildcard(c))); // the caller left
    h.checkpoint(); // the claim's answer fails to send: the lease goes
    h.checkpoint();
    assert!(h.cursor(pid, "g").unwrap().worker.is_none(), "released");
    let again = only(h.wildcard("q", "g", "w2"));
    assert_eq!(again.worker, "w2");
}

// ---------------------------------------------------------------------------
// Transactions
// ---------------------------------------------------------------------------

#[test]
fn a_transaction_ack_is_reserved_until_it_resolves() {
    let mut h = H::new("txn-ack");
    let pid = h.push("q", "p0", &["a", "b"]);
    only(h.wildcard("q", "g", "w1"));
    let id = h.rid();
    let mut txn = h.txn(id);
    txn.acks.push(h.ack_target(
        pid,
        "q",
        "g",
        "w1",
        &[("a", AckStatus::Ok), ("b", AckStatus::Ok)],
    ));
    let part =
        h.e.txn_prepare(&txn, h.now)
            .expect("prepare")
            .expect("a part");
    assert_eq!(part.acks.results.len(), 1);
    assert_eq!(part.acks.results[0].committed, 1);
    // Idempotent per request id.
    let again =
        h.e.txn_prepare(&txn, h.now)
            .expect("prepare")
            .expect("a part");
    assert_eq!(again, part);
    // Reserved: a plain ack of it is refused (retryable) meanwhile.
    let t = h.ack_target(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]);
    let c = AckCommand {
        request_id: h.rid(),
        targets: vec![t],
    };
    assert!(matches!(
        h.serve(Command::Ack(c)),
        Reply::Refused(r) if r.retryable
    ));
    // The entry carries the rows; it commits.
    let rows: Vec<&Effect> = part
        .effects
        .iter()
        .filter(|e| matches!(e, Effect::CursorSet { .. }))
        .collect();
    assert_eq!(rows.len(), 1);
    h.apply(&part.effects);
    h.e.txn_resolve(id, true);
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 1);
    // Nothing left to checkpoint for it, and the partition is free again.
    h.checkpoint();
    h.push("q", "p0", &["c"]);
    let c = only(h.wildcard("q", "g", "w2"));
    assert_eq!((c.start_offset, c.end_offset), (2, 2));
}

#[test]
fn a_transaction_with_a_stale_ack_rolls_back_whole() {
    let mut h = H::new("txn-stale");
    let pid = h.push("q", "p0", &["a"]);
    only(h.wildcard("q", "g", "w1"));
    let id = h.rid();
    let mut txn = h.txn(id);
    txn.acks
        .push(h.ack_target(pid, "q", "g", "wrong", &[("a", AckStatus::Ok)]));
    match h.e.txn_prepare(&txn, h.now) {
        Err(r) => {
            assert_eq!(r.code, "rejected_ack");
            assert!(r.message.starts_with("QTXN"));
        }
        other => panic!("{other:?}"),
    }
    // Nothing reserved: the right worker acks normally.
    let r = h.ack(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]);
    assert!(r.lease_released);
}

#[test]
fn a_refused_transaction_changes_nothing() {
    let mut h = H::new("txn-refused");
    let pid = h.push("q", "p0", &["a"]);
    only(h.wildcard("q", "g", "w1"));
    let id = h.rid();
    let mut txn = h.txn(id);
    txn.acks
        .push(h.ack_target(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]));
    h.e.txn_prepare(&txn, h.now).unwrap().unwrap();
    h.e.txn_resolve(id, false);
    // A second resolve is a no-op.
    h.e.txn_resolve(id, false);
    let r = h.ack(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]);
    assert!(r.lease_released, "the lease was still there: {r:?}");
}

#[test]
fn positions_set_move_and_forget_a_cursor() {
    use crate::rsm::planner::positions::PositionOp;
    let mut h = H::new("positions");
    let pid = h.push("q", "0", &["a", "b", "c", "d", "e"]);
    let op = |offset: Option<u64>, meta: &str| PositionOp {
        queue: "q".into(),
        partition: "0".into(),
        group: "billing".into(),
        offset,
        metadata: meta.into(),
        sub: mode("all"),
    };
    let id = h.rid();
    let mut txn = h.txn(id);
    txn.positions.push(op(Some(3), "batch-3"));
    txn.positions.push(PositionOp {
        partition: "missing".into(),
        ..op(Some(0), "")
    });
    let part = h.e.txn_prepare(&txn, h.now).unwrap().unwrap();
    assert!(part
        .effects
        .iter()
        .any(|e| matches!(e, Effect::GroupUpsert { group, .. } if group == "billing")));
    // The partition that does not exist is the planner's to create.
    assert_eq!(part.planner_positions.len(), 1);
    assert_eq!(part.planner_positions[0].partition, "missing");
    h.apply(&part.effects);
    h.e.txn_resolve(id, true);
    let row = h.cursor(pid, "billing").unwrap();
    assert_eq!((row.committed, row.metadata.as_str()), (2, "batch-3"));
    // A native consumer of the group resumes there.
    let c = only(h.wildcard("q", "billing", "w1"));
    assert_eq!(c.start_offset, 3);
    // A position set to where it is writes nothing.
    let id2 = h.rid();
    let mut same = h.txn(id2);
    same.positions.push(op(Some(3), "batch-3"));
    let p2 = h.e.txn_prepare(&same, h.now).unwrap().unwrap();
    // (The lease the pop took is released by a set: so it does write.)
    h.apply(&p2.effects);
    h.e.txn_resolve(id2, true);
    let id3 = h.rid();
    let mut again = h.txn(id3);
    again.positions.push(op(Some(3), "batch-3"));
    let p3 = h.e.txn_prepare(&again, h.now).unwrap().unwrap();
    assert!(p3.effects.is_empty(), "{:?}", p3.effects);
    h.e.txn_resolve(id3, true);
    // Forget.
    let id4 = h.rid();
    let mut forget = h.txn(id4);
    forget.positions.push(op(None, ""));
    let p4 = h.e.txn_prepare(&forget, h.now).unwrap().unwrap();
    assert!(p4
        .effects
        .iter()
        .any(|e| matches!(e, Effect::CursorDelete { .. })));
    h.apply(&p4.effects);
    h.e.txn_resolve(id4, true);
    assert!(h.cursor(pid, "billing").is_none());
    // A queue that does not exist refuses the bundle.
    let bid = h.rid();
    let mut bad = h.txn(bid);
    bad.positions.push(PositionOp {
        queue: "nope".into(),
        ..op(Some(1), "")
    });
    assert_eq!(
        h.e.txn_prepare(&bad, h.now).unwrap_err().code,
        "queue_not_found"
    );
}

// ---------------------------------------------------------------------------
// Apply hooks
// ---------------------------------------------------------------------------

#[test]
fn a_deleted_partition_leaves_the_engine() {
    let mut h = H::new("delete");
    let pid = h.push("q", "p0", &["a"]);
    only(h.wildcard("q", "g", "w1"));
    h.e.on_effect(&Effect::PartitionDelete { pid }, 0);
    assert_eq!(h.e.ready_count(T, "q", "g"), Some(0));
    // A group delete drops the group.
    h.e.on_effect(
        &Effect::GroupDelete {
            tenant: T.into(),
            queue: "q".into(),
            group: "g".into(),
        },
        0,
    );
    assert!(h.e.ready_count(T, "q", "g").is_none());
}

#[test]
fn a_seek_is_taken_over_and_checkpointed() {
    let mut h = H::new("seek");
    let pid = h.push("q", "p0", &["a", "b", "c"]);
    only(h.wildcard("q", "g", "w1"));
    let mut row = h.cursor(pid, "g").unwrap();
    row.committed = 1;
    row.worker = None;
    row.batch_end = None;
    row.lease_expires_at_us = None;
    row.lease_acquired_at_us = None;
    let cmd = Command::Effects(crate::rsm::planner::EffectsCommand {
        request_id: h.rid(),
        tenant: T.into(),
        effects: vec![Effect::CursorSet {
            pid,
            group: "g".into(),
            row,
        }],
    });
    assert!(matches!(h.serve(cmd), Reply::Done { .. }));
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 1);
    let c = only(h.wildcard("q", "g", "w2"));
    assert_eq!((c.start_offset, c.end_offset), (2, 2), "after the seek");
}

#[test]
fn a_foreign_cursor_row_is_adopted() {
    let mut h = H::new("adopt");
    let pid = h.push("q", "p0", &["a", "b", "c"]);
    let c = only(h.wildcard_with("q", "g", "w1", |c| c.budget = 1));
    assert_eq!(c.end_offset, 0);
    let mut row = h.cursor(pid, "g").unwrap();
    row.committed = 1;
    row.worker = None;
    row.batch_end = None;
    row.lease_expires_at_us = None;
    h.apply(&[Effect::CursorSet {
        pid,
        group: "g".into(),
        row,
    }]);
    let c = only(h.wildcard("q", "g", "w2"));
    assert_eq!((c.start_offset, c.end_offset), (2, 2));
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

/// Four consumers pop and ack while one "apply" thread appends and commits
/// checkpoints: every message is delivered exactly once, nothing deadlocks.
#[test]
fn concurrent_consumers_get_every_message_exactly_once() {
    use std::collections::HashSet;
    use std::sync::Mutex;
    let mut h = H::new("stress");
    const PARTS: usize = 16;
    const PER: usize = 40;
    for p in 0..PARTS {
        h.push("q", &format!("p{p}"), &[&format!("seed-{p}")]);
    }
    let total = PARTS * (PER + 1);
    let e = h.e.clone();
    let seen: Arc<Mutex<HashSet<(Pid, u64)>>> = Arc::new(Mutex::new(HashSet::new()));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut consumers = Vec::new();
    for w in 0..4 {
        let e = e.clone();
        let seen = seen.clone();
        let done = done.clone();
        consumers.push(std::thread::spawn(move || {
            let mut n = 0u64;
            while !done.load(Ordering::Acquire) {
                n += 1;
                let worker = format!("w{w}-{n}");
                let c = PopCommand {
                    wait: false,
                    request_id: crate::util::uuidv7_bytes(),
                    tenant: T.to_string(),
                    queue: "q".into(),
                    partition: None,
                    group: "g".into(),
                    worker: worker.clone(),
                    budget: 7,
                    max_parts: 3,
                    lease_seconds: 60,
                    auto_ack: false,
                    conflate: false,
                    sub: mode("all"),
                    skip_window_debounce: false,
                    namespace: String::new(),
                    task: String::new(),
                    create_cfg: None,
                    deadline_us: 0,
                };
                let reply = match e.serve(&Command::PopWildcard(c), wall_us()) {
                    Served::Now(r) => r,
                    Served::Later(rx) => match rx.blocking_recv() {
                        Ok(r) => r,
                        Err(_) => continue,
                    },
                    Served::NotMine => panic!(),
                };
                let claims = match reply {
                    Reply::Done {
                        outcome: Outcome::Pop(p),
                        ..
                    } => p.claims,
                    Reply::Retry { .. } => continue,
                    other => panic!("{other:?}"),
                };
                for cl in claims {
                    {
                        let mut s = seen.lock().unwrap();
                        for off in cl.start_offset..=cl.end_offset {
                            assert!(s.insert((cl.pid, off)), "delivered twice: {cl:?}");
                        }
                    }
                    let a = AckPositionalCommand {
                        request_id: crate::util::uuidv7_bytes(),
                        pid: cl.pid,
                        tenant: T.to_string(),
                        queue: "q".into(),
                        group: "g".into(),
                        worker: worker.clone(),
                        upto: Some(cl.end_offset as i64),
                        ok: true,
                        release_lease: true,
                        acked_count: (cl.end_offset - cl.start_offset + 1) as i32,
                    };
                    let r = match e.serve(&Command::AckPositional(a), wall_us()) {
                        Served::Now(r) => r,
                        Served::Later(rx) => rx.blocking_recv().expect("ack answer"),
                        Served::NotMine => panic!(),
                    };
                    assert!(matches!(r, Reply::Done { .. }), "{r:?}");
                }
            }
        }));
    }
    // The apply thread: appends interleaved with checkpoints.
    let start = std::time::Instant::now();
    for i in 0..PER {
        for p in 0..PARTS {
            h.now = wall_us();
            h.push("q", &format!("p{p}"), &[&format!("m-{p}-{i}")]);
        }
        h.checkpoint();
    }
    loop {
        h.checkpoint();
        if seen.lock().unwrap().len() >= total {
            break;
        }
        assert!(
            start.elapsed() < std::time::Duration::from_secs(60),
            "stuck at {} of {total}",
            seen.lock().unwrap().len()
        );
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    done.store(true, Ordering::Release);
    // Drain: consumers may be waiting on answers the checkpoints release.
    let drain_until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while consumers.iter().any(|c| !c.is_finished()) && std::time::Instant::now() < drain_until {
        h.checkpoint();
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    for c in consumers {
        c.join().expect("consumer");
    }
    assert_eq!(seen.lock().unwrap().len(), total);
    // Every cursor is at its tail once the last checkpoint landed.
    h.checkpoint();
    for p in 0..PARTS {
        let pid = h.pid_of("q", &format!("p{p}"));
        assert_eq!(h.cursor(pid, "g").unwrap().committed, PER as i64, "p{p}");
    }
}

#[test]
fn a_committed_bundle_retried_under_its_id_gets_the_same_part() {
    let mut h = H::new("txn-retry");
    let pid = h.push("q", "p0", &["a"]);
    only(h.wildcard("q", "g", "w1"));
    let id = h.rid();
    let mut txn = h.txn(id);
    txn.acks
        .push(h.ack_target(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]));
    let part = h.e.txn_prepare(&txn, h.now).unwrap().unwrap();
    h.apply(&part.effects);
    h.e.txn_resolve(id, true);
    // The retry (the batcher answers it from its record): same part, and the
    // resolve(false) that follows changes nothing.
    let again = h.e.txn_prepare(&txn, h.now).unwrap().unwrap();
    assert_eq!(again, part);
    h.e.txn_resolve(id, false);
    assert_eq!(h.cursor(pid, "g").unwrap().committed, 0);
    // A different id acking the same message again is stale.
    let id2 = h.rid();
    let mut t2 = h.txn(id2);
    t2.acks
        .push(h.ack_target(pid, "q", "g", "w1", &[("a", AckStatus::Ok)]));
    assert_eq!(
        h.e.txn_prepare(&t2, h.now).unwrap_err().code,
        "rejected_ack"
    );
}

#[test]
fn a_single_voter_serves_without_the_failover_pause() {
    let mut h = H::new("single-voter");
    h.push("q", "p0", &["a"]);
    h.e.on_step_down();
    h.e.set_leader_lease(Arc::new(|| Some(std::time::Duration::ZERO)));
    h.e.on_leader(5, u64::MAX);
    let c = h.pop_cmd("q", "g", "w1");
    // Answered through its checkpoint, not held for ~half a second.
    let start = std::time::Instant::now();
    let got = claims(&h.outcome(Command::PopWildcard(c)));
    assert_eq!(got.len(), 1);
    assert!(start.elapsed() < std::time::Duration::from_millis(400));
}

/// The engine times leases on its own clock: it only moves forward, and a new
/// term anchors it at or above the log's clock — a wall clock that jumps (or
/// strobes, as Jepsen's clock nemesis does) cannot expire a lease early.
#[test]
fn the_engine_clock_only_moves_forward_and_anchors_at_or_above_the_log() {
    let h = H::new("clock");
    let a = h.e.now_us();
    assert!(h.e.now_us() >= a);
    // The log's clock is a minute ahead of this wall clock: the clock follows.
    h.e.anchor_clock(a + 60_000_000);
    let b = h.e.now_us();
    assert!(b >= a + 60_000_000, "{b} vs {a}");
    // A later anchor with the wall clock behind never pulls it back.
    h.e.anchor_clock(0);
    assert!(h.e.now_us() >= b);
}
