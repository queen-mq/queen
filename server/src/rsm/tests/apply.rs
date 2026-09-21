//! The apply thread against a real store and real segment files (WP-1.4).
//!
//! Everything here opens a throwaway data directory under the system temp
//! directory and removes it on the way out ([`Node`]). Nothing writes into the
//! repository, every file the tests make is a few kilobytes, and no test
//! leaves a process behind.
//!
//! The four properties the work package owes are:
//!
//! - **I2** — [`two_nodes_with_different_hash_seeds_reach_the_same_digest`]:
//!   the same entries against two fresh states give byte-equal digests of
//!   every replicated keyspace, in key order.
//! - **idempotence** — [`re_applying_from_the_durable_index_changes_nothing`]:
//!   the in-process half of §11.5's repair path (the `kill -9` half is
//!   `apply_crash.rs`).
//! - **counters** — [`counters_equal_a_recount`]: every counter §6.4 maintains
//!   is recomputed from the rows and compared.
//! - **pins** — [`a_pin_blocks_the_unlink_until_it_is_dropped`]: I4's claim pin
//!   against I10's two-phase GC.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::rsm::apply::{
    state_digest, Applied, Applier, ApplyConfig, ApplyError, Committed, Notify, StateDigest,
};
use crate::rsm::dedup;
use crate::rsm::effect::{
    CursorRow, Effect, GarbageScope, GroupMeta, Pid, QueueConfig, SubscriptionMode,
};
use crate::rsm::entry::{Entry, Outcome, RequestId};
use crate::rsm::segments::{self, FsyncMode};
use crate::rsm::store::keys::{self, Counter};
use crate::rsm::store::{HeedStore, Keyspace, Reads, Store, StoreOpts, TypedReads};

// ---------------------------------------------------------------------------
// A throwaway node
// ---------------------------------------------------------------------------

static SEQ: AtomicU64 = AtomicU64::new(0);

/// A data directory that removes itself, in the §11.1 shape (`store/`, `seg/`).
pub struct Node {
    dir: PathBuf,
    store: Option<HeedStore>,
    /// Set by [`Node::keep`]: the directory outlives this handle, because
    /// another one is going to reopen it.
    keep: bool,
}

impl Node {
    pub fn new(tag: &str) -> Node {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-apply-{tag}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        Node::at(dir)
    }

    /// Open (or reopen) a node at a given path: what recovery does (§11.5).
    pub fn at(dir: PathBuf) -> Node {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("open store");
        Node {
            dir,
            store: Some(store),
            keep: false,
        }
    }

    /// Do not remove the directory when this handle goes: the test reopens it.
    pub fn keep(&mut self) {
        self.keep = true;
    }

    pub fn store(&self) -> &HeedStore {
        self.store.as_ref().expect("store is open")
    }

    pub fn seg_dir(&self) -> PathBuf {
        self.dir.join("seg")
    }

    pub fn path(&self) -> &Path {
        &self.dir
    }

    /// Close the environment without deleting the directory, so the same path
    /// can be reopened in this process (heed refuses a second open).
    pub fn close(&mut self) {
        if let Some(s) = self.store.take() {
            s.close();
        }
    }

    pub fn digest(&self) -> StateDigest {
        self.store()
            .read(|r| Ok(state_digest(r).expect("digest")))
            .expect("read")
    }

    /// The NODE-LOCAL half: `seg_loc`, `files`, `partition_files`. Comparable
    /// only on one node, across a restart (§6.2, D8).
    pub fn local_digest(&self) -> StateDigest {
        self.store()
            .read(|r| Ok(crate::rsm::apply::local_digest(r).expect("digest")))
            .expect("read")
    }
}

/// A throwaway directory path that survives `Node::close`, for the tests that
/// reopen one.
pub fn tmp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-apply-{tag}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The applier of a node, with this file's options.
pub fn open_at(node: &Node) -> (Applier<'_, HeedStore>, crate::rsm::apply::Recovered) {
    Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open")
}

impl Drop for Node {
    fn drop(&mut self) {
        self.close();
        if !self.keep {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
}

pub fn store_opts() -> StoreOpts {
    StoreOpts {
        // A small map keeps the sparse file small on a laptop.
        map_bytes: Some(256 << 20),
        ..Default::default()
    }
}

/// Small files, so a handful of appends rolls one and the seal, the `.qidx`
/// and the GC paths are all exercised; `Data` keeps the tests off macOS's
/// serialized `F_FULLFSYNC`.
pub fn seg_opts() -> segments::Options {
    seg_opts_buckets(segments::NBUCKETS)
}

/// PERF-F: the same small-file options with a chosen bucket count, for the
/// crash-harness cells that run apply at `QUEEN_RAFT_BUCKETS` = 1 and 16.
pub fn seg_opts_buckets(nbuckets: usize) -> segments::Options {
    segments::Options {
        segment_bytes: 8 << 10,
        fsync: FsyncMode::Data,
        fsync_threads: 1,
        nbuckets,
    }
}

pub fn cfg() -> ApplyConfig {
    ApplyConfig {
        store_commit_ms: 4,
        store_commit_entries: 256,
        durable_every_ms: 1000,
        durable_every_bytes: 256 << 20,
        gc_per_pass: 32,
        idle_tick_ms: 2,
        durable_async: true,
        // PERF-C: exercise write coalescing across the whole apply suite; the
        // pool is off here (no writer-thread churn per test), and covered by
        // its own tests that set `apply_writers`.
        seg_buffered: true,
        apply_writers: 0,
        // PERF-D: batch counters everywhere (transparent), keep pending on the
        // shipped byte-for-byte path; the transition tests opt in per-test.
        batch_counters: true,
        pending_transitions: false,
        // Phase A1: the shadow qlog is off across the shared apply suite, so
        // every existing test runs today's exact path; the qlog tests opt in.
        qlog: false,
    }
}

// ---------------------------------------------------------------------------
// A notifier that records
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct Recorder {
    pub applied: Mutex<Vec<(u64, u64, usize)>>,
    pub wakes: Mutex<Vec<(String, String, Option<String>)>>,
    pub durable: Mutex<Vec<u64>>,
}

impl Notify for Recorder {
    fn applied(&self, index: u64, term: u64, commands: &[crate::rsm::entry::CommandRecord]) {
        self.applied
            .lock()
            .expect("recorder")
            .push((index, term, commands.len()));
    }

    fn wake(&self, tenant: &str, queue: &str, group: Option<&str>) {
        self.wakes.lock().expect("recorder").push((
            tenant.to_string(),
            queue.to_string(),
            group.map(str::to_string),
        ));
    }

    fn durable(&self, index: u64) {
        self.durable.lock().expect("recorder").push(index);
    }
}

// ---------------------------------------------------------------------------
// Building entries
// ---------------------------------------------------------------------------

pub const TENANT: &str = "t1";
pub const QUEUE: &str = "orders";

pub fn request_id(n: u64) -> RequestId {
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&n.to_be_bytes());
    id[8] = 0xA5;
    id
}

pub fn uuid(n: u64) -> [u8; 16] {
    let mut id = [0u8; 16];
    id[0..8].copy_from_slice(&n.to_le_bytes());
    id[15] = 7;
    id
}

pub fn queue_config(created_at_us: i64) -> QueueConfig {
    QueueConfig {
        id: uuid(1),
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
        dedup_window_seconds: 3600,
        retention_sink_hold: String::new(),
        retention_sink_hold_max_seconds: 0,
        created_at_us,
    }
}

pub fn group_meta(n: u64, created_at_us: i64) -> GroupMeta {
    GroupMeta {
        id: uuid(100 + n),
        partition_name: String::new(),
        namespace: String::new(),
        task: String::new(),
        mode: SubscriptionMode::New,
        subscription_timestamp_us: 0,
        conflation: false,
        seeded: false,
        registered_at_us: created_at_us,
    }
}

pub fn hashes(seed: u64, count: u32) -> Vec<u8> {
    let mut v = Vec::with_capacity(count as usize * 16);
    for i in 0..count as u64 {
        let h = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i);
        v.extend_from_slice(&h.to_le_bytes());
        v.extend_from_slice(&(!h).to_le_bytes());
    }
    v
}

pub fn hash_at(seed: u64, i: u64) -> [u8; 16] {
    let h = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15).wrapping_add(i);
    let mut out = [0u8; 16];
    out[0..8].copy_from_slice(&h.to_le_bytes());
    out[8..16].copy_from_slice(&(!h).to_le_bytes());
    out
}

pub fn fresh_cursor(committed: i64, created_at_us: i64) -> CursorRow {
    CursorRow {
        committed,
        batch_end: None,
        worker: None,
        lease_expires_at_us: None,
        lease_acquired_at_us: None,
        batch_retry_count: 0,
        attempt_offset: None,
        attempt_count: 0,
        total_consumed: 0,
        lease_conflated: false,
        delivered: Vec::new(),
        created_at_us,
    }
}

/// One entry per call, with its request ids drawn from a counter.
pub struct Build {
    pub now_us: i64,
    pub pid_base: u64,
    pub kv_base: u64,
    ids: u64,
    entry: Entry,
}

impl Build {
    pub fn new(now_us: i64, pid_base: u64, ids_from: u64) -> Build {
        Build {
            now_us,
            pid_base,
            kv_base: 1,
            ids: ids_from,
            entry: Entry::new(now_us, pid_base, 1),
        }
    }

    pub fn cmd(mut self, effects: Vec<Effect>) -> Build {
        self.ids += 1;
        let id = request_id(self.ids);
        self.entry
            .add_command(id, Outcome::Empty, effects)
            .expect("add command");
        self
    }

    pub fn at(self, index: u64, term: u64) -> Committed {
        Committed {
            index,
            term,
            entry: self.entry,
        }
    }
}

// ---------------------------------------------------------------------------
// A reproducible workload (shared with apply_crash.rs)
// ---------------------------------------------------------------------------

/// The entry generator both the in-process tests and the crash test use.
///
/// It is a plain LCG over a state it keeps itself, so the SAME seed gives the
/// same entries in a parent and in a child process. Nothing here reads a
/// clock: entry `n` is stamped `BASE_US + n × 1000`, which is what makes two
/// runs comparable at all.
pub struct Workload {
    rng: u64,
    ids: u64,
    n: u64,
    next_pid: u64,
    dlq_seq: u64,
    parts: Vec<Part>,
    groups: Vec<String>,
    seeded: bool,
}

#[derive(Clone, Debug)]
struct Part {
    pid: Pid,
    bucket: u16,
    last_offset: i64,
    log_start: u64,
    txns_start: u64,
    committed: BTreeMap<String, i64>,
    dlq: Vec<[u8; 16]>,
}

pub const BASE_US: i64 = 1_800_000_000_000_000;

impl Workload {
    pub fn new(seed: u64) -> Workload {
        Workload {
            rng: seed | 1,
            ids: 0,
            n: 0,
            next_pid: 1,
            dlq_seq: 0,
            parts: Vec::new(),
            groups: vec!["g1".to_string(), "g2".to_string()],
            seeded: false,
        }
    }

    fn roll(&mut self) -> u64 {
        self.rng = self
            .rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.rng >> 17
    }

    /// The next entry, at index `n + 1`.
    pub fn next(&mut self) -> Committed {
        self.n += 1;
        let now_us = BASE_US + self.n as i64 * 1_000;
        let index = self.n;
        let mut b = Build::new(now_us, self.next_pid, self.ids);
        if !self.seeded {
            self.seeded = true;
            let mut effects = vec![Effect::QueueUpsert {
                tenant: TENANT.to_string(),
                queue: QUEUE.to_string(),
                cfg: queue_config(now_us),
            }];
            for (i, g) in self.groups.clone().iter().enumerate() {
                effects.push(Effect::GroupUpsert {
                    tenant: TENANT.to_string(),
                    queue: QUEUE.to_string(),
                    group: g.clone(),
                    meta: group_meta(i as u64, now_us),
                });
            }
            for i in 0..4u64 {
                let pid = self.next_pid + i;
                effects.push(Effect::PartitionCreate {
                    pid,
                    uuid: uuid(pid),
                    tenant: TENANT.to_string(),
                    queue: QUEUE.to_string(),
                    partition: format!("p{i}"),
                    created_at_us: now_us,
                });
                self.parts.push(Part {
                    pid,
                    bucket: (pid % 8) as u16,
                    last_offset: -1,
                    log_start: 0,
                    txns_start: 0,
                    committed: BTreeMap::new(),
                    dlq: Vec::new(),
                });
            }
            self.next_pid += 4;
            b = b.cmd(effects);
            self.ids = b.ids;
            return b.at(index, 1);
        }

        let commands = 1 + (self.roll() % 3) as usize;
        for _ in 0..commands {
            let which = self.roll() % 10;
            let pi = (self.roll() % self.parts.len() as u64) as usize;
            let effects = match which {
                0..=5 => self.an_append(pi, now_us),
                6 | 7 => self.a_cursor(pi, now_us),
                8 => self.a_dead_letter(pi, now_us),
                _ => self.a_watermark(pi),
            };
            if !effects.is_empty() {
                b = b.cmd(effects);
            }
        }
        if b.entry.commands.is_empty() {
            b = b.cmd(vec![Effect::Noop]);
        }
        self.ids = b.ids;
        b.at(index, 1)
    }

    fn an_append(&mut self, pi: usize, now_us: i64) -> Vec<Effect> {
        let count = 1 + (self.roll() % 4) as u32;
        let seed = self.roll();
        let p = &mut self.parts[pi];
        let base = (p.last_offset + 1) as u64;
        p.last_offset += count as i64;
        Vec::from([Effect::Append {
            pid: p.pid,
            bucket: p.bucket,
            base_offset: base,
            count,
            created_at_us: now_us,
            hashes: hashes(seed, count),
            blob: vec![0xAB; 24 * count as usize],
        }])
    }

    fn a_cursor(&mut self, pi: usize, now_us: i64) -> Vec<Effect> {
        let gi = (self.roll() % self.groups.len() as u64) as usize;
        let group = self.groups[gi].clone();
        let leased = self.roll().is_multiple_of(2);
        let step = 1 + (self.roll() % 3) as i64;
        let p = &mut self.parts[pi];
        if p.last_offset < 0 {
            return Vec::new();
        }
        let cur = *p.committed.get(&group).unwrap_or(&-1);
        let next = (cur + step).min(p.last_offset);
        p.committed.insert(group.clone(), next);
        let mut row = fresh_cursor(next, now_us);
        if leased {
            row.worker = Some(format!("w{gi}"));
            row.lease_expires_at_us = Some(now_us + 30_000_000);
            row.lease_acquired_at_us = Some(now_us);
            row.batch_end = Some(p.last_offset as u64);
        }
        row.total_consumed = (next + 1).max(0) as u64;
        Vec::from([Effect::CursorSet {
            pid: p.pid,
            group,
            row,
        }])
    }

    fn a_dead_letter(&mut self, pi: usize, now_us: i64) -> Vec<Effect> {
        let r = self.roll();
        self.dlq_seq += 1;
        // A dead letter's id is minted once per dead letter, and the row is
        // keyed by it: two of them sharing an id is a planner bug, not a case
        // to generate.
        let seq = self.dlq_seq;
        let p = &mut self.parts[pi];
        if p.last_offset < 0 {
            return Vec::new();
        }
        if !p.dlq.is_empty() && r.is_multiple_of(3) {
            let id = p.dlq.remove(0);
            return Vec::from([Effect::DlqDelete {
                dlq_id: id,
                tenant: TENANT.to_string(),
                queue: QUEUE.to_string(),
            }]);
        }
        let id = uuid(9_000_000 + seq);
        p.dlq.push(id);
        let offset = r as i64 % (p.last_offset + 1);
        Vec::from([Effect::DlqInsert {
            dlq_id: id,
            tenant: TENANT.to_string(),
            queue: QUEUE.to_string(),
            pid: p.pid,
            group: "g1".to_string(),
            offset,
            message_id: Some(uuid(r)),
            txn: format!("txn-{r}"),
            payload: b"{\"x\":1}".to_vec(),
            error: "boom".to_string(),
            retry_count: 3,
            failed_at_us: now_us,
        }])
    }

    fn a_watermark(&mut self, pi: usize) -> Vec<Effect> {
        let step = 1 + (self.roll() % 3);
        let p = &mut self.parts[pi];
        if p.last_offset < 0 {
            return Vec::new();
        }
        let ceiling = (p.last_offset + 1) as u64;
        let log_start = (p.log_start + step).min(ceiling);
        let txns_start = (p.txns_start + step / 2).min(log_start);
        if log_start == p.log_start && txns_start == p.txns_start {
            return Vec::new();
        }
        p.log_start = log_start;
        p.txns_start = txns_start;
        Vec::from([Effect::Watermark {
            pid: p.pid,
            log_start,
            txns_start,
        }])
    }
}

/// Apply `count` entries of a workload to a node, with a durable point every
/// `durable_every` entries, and return the digest of its replicated state.
///
/// It runs file GC on every turn, exactly as [`crate::rsm::apply::run`] does:
/// a workload with retention in it kills files, and a reference run that never
/// collected them would not be comparable with one that did — and would leave
/// the whole of §11.7 outside every test that uses this.
pub fn run_workload(node: &Node, seed: u64, count: u64, durable_every: u64) -> StateDigest {
    let mut w = Workload::new(seed);
    {
        let (mut a, _rec) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for n in 1..=count {
            let c = w.next();
            a.apply(&c).expect("apply");
            if durable_every > 0 && n % durable_every == 0 {
                a.durable_point().expect("durable point");
            }
            a.gc_pass().expect("gc");
        }
        settle(&mut a);
        // The dead-file set is maintained incrementally at every mutation of
        // the file table (§11.7, I8): if a mutation forgot to tell it, GC would
        // either leak a file for ever or offer a live one. A full workload,
        // with its appends, seals, watermarks, partition deletes and unlinks,
        // is where that drift would appear, so it is checked HERE against a
        // scan of the whole table — the scan the product code no longer does.
        let by_scan: Vec<(u16, u32)> = a
            .segments_mut()
            .files()
            .into_iter()
            .filter(|(_, _, m)| m.is_dead())
            .map(|(b, id, _)| (b, id))
            .collect();
        let incremental = a.segments_mut().gc_candidates(usize::MAX);
        assert_eq!(
            incremental, by_scan,
            "the incremental dead set drifted from the file table",
        );
    }
    node.digest()
}

/// Bring a node to rest: everything applied is durable, every dead file has
/// been through both phases of GC, and nothing is left staged.
///
/// Two rounds because GC is two-phase by construction (I10): the first
/// durable point stops naming the files, the second unlinks them and stages
/// whatever the first round's unlinks made collectable.
pub fn settle<S: Store>(a: &mut Applier<'_, S>) {
    for _ in 0..3 {
        a.gc_pass().expect("gc");
        a.durable_point().expect("durable point");
    }
}

/// PERF-D laptop smoke: store puts per append and apply time under the A20k and
/// C1000 shapes, with the batching levers off (before), counters on, and both
/// on (after). Prints the numbers PERF-D reports; run with:
///   cargo test -p queen-engine --lib rsm::tests::apply::perf_d_store_ops_per_append \
///     -- --ignored --nocapture
#[test]
#[ignore = "measurement, not a gate; run with --ignored --nocapture"]
fn perf_d_store_ops_per_append() {
    // One shape: `parts` partitions, `batch` messages per append, `groups`
    // subscribed groups. Returns (puts/append, ns/append) over `windows` × 256
    // append entries applied after setup, committing every 256 (the shipped
    // store-commit cadence), so the counter flush at each commit is amortised in.
    fn run(
        parts: u64,
        batch: u32,
        groups: u64,
        batch_counters: bool,
        pending_transitions: bool,
        windows: u64,
    ) -> (f64, f64) {
        let node = Node::new("perfd");
        let cfg = ApplyConfig {
            batch_counters,
            pending_transitions,
            ..cfg()
        };
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg,
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        let mut now = BASE_US;
        let mut ids = 0u64;

        // Setup: one entry creates the queue, the groups and every partition.
        let mut effects = vec![Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            cfg: queue_config(now),
        }];
        for g in 0..groups {
            effects.push(Effect::GroupUpsert {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                group: format!("g{g}"),
                meta: group_meta(g, now),
            });
        }
        for p in 0..parts {
            effects.push(Effect::PartitionCreate {
                pid: 1 + p,
                uuid: uuid(1 + p),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: format!("p{p}"),
                created_at_us: now,
            });
        }
        let setup = {
            let b = Build::new(now, 1, ids).cmd(effects);
            ids += 1;
            b.at(1, 1)
        };
        a.apply(&setup).expect("setup");
        a.commit().expect("commit setup");

        let pid_base = 1 + parts;
        let mut last_off = vec![-1i64; parts as usize];
        let mut k = 0u64;
        let mut idx = 1u64;

        let puts0 = crate::rsm::store::StoreMetrics::get(&node.store().metrics().rows_put);
        let t0 = std::time::Instant::now();
        for _ in 0..windows {
            for _ in 0..256 {
                now += 1000;
                idx += 1;
                ids += 1;
                let pi = (k % parts) as usize;
                let base = (last_off[pi] + 1) as u64;
                last_off[pi] += batch as i64;
                let app = Effect::Append {
                    pid: 1 + pi as u64,
                    bucket: (pi % 8) as u16,
                    base_offset: base,
                    count: batch,
                    created_at_us: now,
                    hashes: hashes(k, batch),
                    blob: vec![0xAB; 24 * batch as usize],
                };
                let c = Build::new(now, pid_base, ids).cmd(vec![app]).at(idx, 1);
                a.apply(&c).expect("append");
                k += 1;
            }
            a.commit().expect("commit window");
        }
        let dt = t0.elapsed();
        let puts1 = crate::rsm::store::StoreMetrics::get(&node.store().metrics().rows_put);
        let puts_per = (puts1 - puts0) as f64 / k as f64;
        let ns_per = dt.as_nanos() as f64 / k as f64;
        (puts_per, ns_per)
    }

    for (name, parts, batch, groups, windows) in [
        ("A20k", 100u64, 10u32, 1u64, 40u64),
        ("C1000", 1000, 1, 1, 40),
    ] {
        let off = run(parts, batch, groups, false, false, windows);
        let ctr = run(parts, batch, groups, true, false, windows);
        let both = run(parts, batch, groups, true, true, windows);
        println!(
            "{name} ({parts} parts, batch {batch}, {groups} grp): \
             puts/append off={:.2} counters={:.2} counters+pending={:.2} | \
             ns/append off={:.0} counters={:.0} both={:.0}",
            off.0, ctr.0, both.0, off.1, ctr.1, both.1
        );
    }
}

/// I2 for PERF-D: the committed replicated state after the same entries, and at
/// every entry boundary, must be identical whatever the store-commit cadence —
/// the batched counters (and, with `pending_transitions` on, the transition
/// `pending` writes) must fold to a cadence-free result. Run at commit cadences
/// of 1, 3 and 1000 entries and compare the digest snapshot taken at every
/// boundary (a boundary forces a commit and reads committed state).
fn cadence_independence(pending_transitions: bool) {
    let m = 90u64;
    let mut w = Workload::new(0x1D2);
    let entries: Vec<Committed> = (0..m).map(|_| w.next()).collect();
    let cfg = ApplyConfig {
        pending_transitions,
        ..cfg()
    };

    let snapshots = |cadence: usize| -> Vec<u128> {
        let node = Node::new("cad");
        let mut digests = Vec::new();
        {
            let (mut a, _) = Applier::open(
                node.store(),
                &node.seg_dir(),
                seg_opts(),
                cfg.clone(),
                Arc::new(crate::rsm::apply::NoNotify),
            )
            .expect("open");
            for (i, c) in entries.iter().enumerate() {
                a.apply(c).expect("apply");
                if (i + 1) % cadence == 0 {
                    a.commit().expect("commit");
                }
                if (i + 1) % 10 == 0 {
                    // The observation boundary: commit whatever is open and read
                    // the committed state. The result must not depend on the
                    // intermediate cadence.
                    a.commit().expect("commit");
                    digests.push(
                        node.store()
                            .read(|r| Ok(state_digest(r).expect("digest").whole))
                            .expect("read"),
                    );
                }
            }
        }
        digests
    };

    let d1 = snapshots(1);
    let d3 = snapshots(3);
    let d1000 = snapshots(1000);
    assert_eq!(
        d1, d3,
        "cadence 1 vs 3 diverged (pending_transitions={pending_transitions})"
    );
    assert_eq!(
        d1, d1000,
        "cadence 1 vs 1000 diverged (pending_transitions={pending_transitions})"
    );
}

#[test]
fn batched_counters_are_independent_of_the_commit_cadence() {
    cadence_independence(false);
}

#[test]
fn pending_transitions_are_independent_of_the_commit_cadence() {
    cadence_independence(true);
}

#[test]
fn pending_transitions_rebuild_from_pending_equals_the_live_rings() {
    // PERF-D: with transitions on, the ring is moved only on the transitions
    // that write the `pending` row, so a rebuild of the ready rings from the
    // committed `pending` keyspace (what a reopen / leadership start does) is
    // byte-for-byte the live ring — the same ready candidates in order and the
    // same parked deadlines. The workload's delay is 0 and its lease deadlines
    // are 30 s out, so nothing is due for promotion between the two.
    use crate::rsm::state::{Derived, ReadyIndex};

    // The ready MEMBERSHIP (sorted), the deferred count and the next deadline —
    // not the FIFO walk order, which a rebuild resets to pid order on BOTH the
    // transitions-on and the shipped path (the ring is a hint, coarse by
    // contract). Equal membership is what "the same rings" means here.
    fn summary(d: &Derived) -> Vec<(String, Vec<Pid>, usize, Option<i64>)> {
        let mut out = Vec::new();
        for g in ["g1", "g2"] {
            let (mut ready, mut deferred, mut next) = (Vec::new(), 0usize, None);
            if let Some(r) = d.ring(TENANT, QUEUE, g) {
                let r: &ReadyIndex = r;
                r.walk(usize::MAX, &mut |pid| {
                    ready.push(pid);
                    true
                });
                ready.sort_unstable();
                deferred = r.deferred_len();
                next = r.next_deadline();
            }
            out.push((g.to_string(), ready, deferred, next));
        }
        out
    }

    let cfg = ApplyConfig {
        pending_transitions: true,
        ..cfg()
    };
    let node = Node::new("pend-rebuild");
    let mut w = Workload::new(0xBEEF);
    let live = {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg.clone(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for _ in 0..80 {
            let c = w.next();
            a.apply(&c).expect("apply");
        }
        a.commit().expect("commit"); // so the rebuild sees the committed pending
                                     // Also check pending is in step with outstanding work per group.
        node.store()
            .read(|r| {
                for pid in 1..=4u64 {
                    let Some(p) = r.partition(pid)? else { continue };
                    for g in ["g1", "g2"] {
                        let committed = r.cursor(pid, g)?.map(|c| c.committed).unwrap_or(-1);
                        let has_work = committed < p.last_offset;
                        let has_row = r.pending_at(TENANT, QUEUE, g, pid)?.is_some();
                        assert_eq!(
                            has_row, has_work,
                            "pid {pid} g {g}: pending row {has_row} but outstanding work {has_work}"
                        );
                    }
                }
                Ok(())
            })
            .expect("read");
        summary(a.derived())
    };
    let rebuilt = node
        .store()
        .read(|r| {
            let now = r.last_now_us()?;
            Ok(summary(&Derived::rebuild(r, now)?))
        })
        .expect("read");
    assert_eq!(live, rebuilt, "the rebuilt rings differ from the live ones");
}

// ---------------------------------------------------------------------------
// PERF-H: the ring re-arm audit made executable
//
// Every one of these drives real entries through `apply` (which now promotes
// the live rings to each entry's stamp) with `pending_transitions` on — the
// default — and checks the one law: a partition is in its group's ring exactly
// when the SQL would consider it claimable, and re-enters it on every event
// that makes it claimable again.
// ---------------------------------------------------------------------------

/// Per-group `(ready sorted, parked count, next deadline)` — the shape the
/// rebuild-equality test uses, normalised so a group with an empty live ring
/// and a group a rebuild never created read the same.
fn ring_summary(
    d: &crate::rsm::state::Derived,
    groups: &[&str],
) -> Vec<(String, Vec<Pid>, usize, Option<i64>)> {
    let mut out = Vec::new();
    for g in groups {
        let (mut ready, mut deferred, mut next) = (Vec::new(), 0usize, None);
        if let Some(r) = d.ring(TENANT, QUEUE, g) {
            r.walk(usize::MAX, &mut |pid| {
                ready.push(pid);
                true
            });
            ready.sort_unstable();
            deferred = r.deferred_len();
            next = r.next_deadline();
        }
        out.push((g.to_string(), ready, deferred, next));
    }
    out
}

/// A pop that leases `pid` to `w1` until `expires`, having consumed up to
/// `committed`, with the batch reaching `batch_end`.
fn lease_pop(committed: i64, batch_end: u64, expires: i64, now: i64) -> CursorRow {
    let mut row = fresh_cursor(committed, now);
    row.worker = Some("w1".into());
    row.lease_expires_at_us = Some(expires);
    row.lease_acquired_at_us = Some(now);
    row.batch_end = Some(batch_end);
    row
}

/// Queue + one group + `parts` partitions, in one entry at [`BASE_US`].
fn setup_entry(parts: u64, groups: &[&str], qc: QueueConfig) -> Committed {
    let mut effects = vec![Effect::QueueUpsert {
        tenant: TENANT.into(),
        queue: QUEUE.into(),
        cfg: qc,
    }];
    for (i, g) in groups.iter().enumerate() {
        effects.push(Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            group: (*g).into(),
            meta: group_meta(i as u64, BASE_US),
        });
    }
    for p in 0..parts {
        effects.push(Effect::PartitionCreate {
            pid: 1 + p,
            uuid: uuid(1 + p),
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            partition: format!("p{p}"),
            created_at_us: BASE_US,
        });
    }
    Build::new(BASE_US, 1, 0).cmd(effects).at(1, 1)
}

fn transitions_cfg() -> ApplyConfig {
    ApplyConfig {
        pending_transitions: true,
        ..cfg()
    }
}

#[test]
fn an_append_to_a_leased_partition_is_armed_for_after_the_lease() {
    // The pop defers the partition to the lease expiry; a frame appended while
    // the lease is live must NOT pull `ready_at` back to now (the wildcard pop
    // would only offer it and skip — the lease is live). Covers both the
    // backlog-left pop (a `pending` row already holds the expiry) and the
    // draining pop (no row, the floor comes from the live lease).
    let node = Node::new("leased-append");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        transitions_cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    let exp = BASE_US + 5_000_000;
    a.apply(&setup_entry(2, &["g1"], queue_config(BASE_US)))
        .expect("setup");
    // pid 1: 10 frames, pop leaves backlog (committed 4 < last 9).
    // pid 2: 3 frames, pop drains it (committed 2 == last 2) but keeps a lease.
    a.apply(
        &Build::new(BASE_US + 10, 3, 10)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 0,
                count: 10,
                created_at_us: BASE_US + 10,
                hashes: hashes(7, 10),
                blob: vec![0xAB; 240],
            }])
            .cmd(vec![Effect::Append {
                pid: 2,
                bucket: 2,
                base_offset: 0,
                count: 3,
                created_at_us: BASE_US + 10,
                hashes: hashes(9, 3),
                blob: vec![0xAB; 72],
            }])
            .at(2, 1),
    )
    .expect("append");
    a.apply(
        &Build::new(BASE_US + 20, 3, 20)
            .cmd(vec![Effect::CursorSet {
                pid: 1,
                group: "g1".into(),
                row: lease_pop(4, 9, exp, BASE_US + 20),
            }])
            .cmd(vec![Effect::CursorSet {
                pid: 2,
                group: "g1".into(),
                row: lease_pop(2, 2, exp, BASE_US + 20),
            }])
            .at(3, 1),
    )
    .expect("pop");
    a.commit().expect("commit");
    node.store()
        .read(|r| {
            assert_eq!(
                r.pending_at(TENANT, QUEUE, "g1", 1).unwrap(),
                Some(exp),
                "backlog-left pop must defer to the lease expiry"
            );
            assert_eq!(
                r.pending_at(TENANT, QUEUE, "g1", 2).unwrap(),
                None,
                "the draining pop left no pending row"
            );
            Ok(())
        })
        .expect("read");
    assert_eq!(a.derived().lease_count(), 2);

    // Append to BOTH while leased, at now well below the expiry.
    a.apply(
        &Build::new(BASE_US + 100, 3, 100)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 10,
                count: 1,
                created_at_us: BASE_US + 100,
                hashes: hashes(8, 1),
                blob: vec![0xAB; 24],
            }])
            .cmd(vec![Effect::Append {
                pid: 2,
                bucket: 2,
                base_offset: 3,
                count: 1,
                created_at_us: BASE_US + 100,
                hashes: hashes(11, 1),
                blob: vec![0xAB; 24],
            }])
            .at(4, 1),
    )
    .expect("append while leased");
    a.commit().expect("commit");
    node.store()
        .read(|r| {
            assert_eq!(
                r.pending_at(TENANT, QUEUE, "g1", 1).unwrap(),
                Some(exp),
                "the append undercut a live lease (pid 1)"
            );
            assert_eq!(
                r.pending_at(TENANT, QUEUE, "g1", 2).unwrap(),
                Some(exp),
                "the draining-lease append armed pid 2 at now, not after the lease"
            );
            Ok(())
        })
        .expect("read");
    assert!(
        !a.derived().ring_has_ready(TENANT, QUEUE, "g1"),
        "a leased partition is offered to the pop"
    );
}

#[test]
fn the_shipped_default_overwrites_a_leased_partitions_ready_at() {
    // Why the transitions path is the default: with it OFF, the append path
    // OVERWRITES `ready_at` unconditionally, so a frame appended under a live
    // lease pulls the partition's revisit time back to now — an under-arm the
    // claim then has to paper over. This pins that difference.
    let node = Node::new("leased-append-off");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(), // pending_transitions: false — the pre-PERF-H shipped path
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    let exp = BASE_US + 5_000_000;
    a.apply(&setup_entry(1, &["g1"], queue_config(BASE_US)))
        .expect("setup");
    a.apply(
        &Build::new(BASE_US + 10, 2, 10)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 0,
                count: 10,
                created_at_us: BASE_US + 10,
                hashes: hashes(7, 10),
                blob: vec![0xAB; 240],
            }])
            .at(2, 1),
    )
    .expect("append");
    a.apply(
        &Build::new(BASE_US + 20, 2, 20)
            .cmd(vec![Effect::CursorSet {
                pid: 1,
                group: "g1".into(),
                row: lease_pop(4, 9, exp, BASE_US + 20),
            }])
            .at(3, 1),
    )
    .expect("pop");
    a.apply(
        &Build::new(BASE_US + 100, 2, 100)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 10,
                count: 1,
                created_at_us: BASE_US + 100,
                hashes: hashes(8, 1),
                blob: vec![0xAB; 24],
            }])
            .at(4, 1),
    )
    .expect("append while leased");
    a.commit().expect("commit");
    let ra = node
        .store()
        .read(|r| r.pending_at(TENANT, QUEUE, "g1", 1))
        .expect("read");
    assert_eq!(
        ra,
        Some(BASE_US + 100),
        "the shipped path was expected to overwrite the lease deferral"
    );
}

#[test]
fn a_delayed_queue_keeps_the_earliest_visibility() {
    // `delayed_processing`: a late frame's visibility is LATER than an earlier
    // one's, so on the transitions path the append must not push the partition's
    // `ready_at` forward past an already-visible frame (an under-arm that would
    // strand claimable work). The shipped OFF path would overwrite it.
    let mut qc = queue_config(BASE_US);
    qc.delayed_processing = 2; // 2 s
    let node = Node::new("delayed");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        transitions_cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    a.apply(&setup_entry(1, &["g1"], qc)).expect("setup");
    // Frame 0 created at BASE_US → visible at BASE_US + 2_000_000.
    a.apply(
        &Build::new(BASE_US, 2, 10)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 0,
                count: 1,
                created_at_us: BASE_US,
                hashes: hashes(1, 1),
                blob: vec![0xAB; 24],
            }])
            .at(2, 1),
    )
    .expect("append 0");
    // A much later frame, created at BASE_US + 1_000_000 → visible later still.
    a.apply(
        &Build::new(BASE_US + 1_000_000, 2, 20)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 1,
                count: 1,
                created_at_us: BASE_US + 1_000_000,
                hashes: hashes(2, 1),
                blob: vec![0xAB; 24],
            }])
            .at(3, 1),
    )
    .expect("append 1");
    a.commit().expect("commit");
    let ra = node
        .store()
        .read(|r| r.pending_at(TENANT, QUEUE, "g1", 1))
        .expect("read");
    assert_eq!(
        ra,
        Some(BASE_US + 2_000_000),
        "the second append pushed ready_at past the first frame's visibility"
    );
}

#[test]
fn a_lease_expiry_re_arms_the_live_ring_at_the_next_entry() {
    // No ack, no release: the lease simply EXPIRES. The partition must re-enter
    // its ring the moment an entry's stamp reaches the expiry — the case the
    // unwired `promote_due` used to miss for the live ring, and the reason the
    // apply loop now promotes at each boundary. The planner side is covered by
    // the rebuild equality below.
    let node = Node::new("expiry-rearm");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        transitions_cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    let exp = BASE_US + 1_000_000;
    a.apply(&setup_entry(1, &["g1"], queue_config(BASE_US)))
        .expect("setup");
    a.apply(
        &Build::new(BASE_US + 10, 2, 10)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 0,
                count: 5,
                created_at_us: BASE_US + 10,
                hashes: hashes(7, 5),
                blob: vec![0xAB; 120],
            }])
            .at(2, 1),
    )
    .expect("append");
    a.apply(
        &Build::new(BASE_US + 20, 2, 20)
            .cmd(vec![Effect::CursorSet {
                pid: 1,
                group: "g1".into(),
                row: lease_pop(1, 4, exp, BASE_US + 20),
            }])
            .at(3, 1),
    )
    .expect("pop");
    assert!(
        !a.derived().ring_has_ready(TENANT, QUEUE, "g1"),
        "leased: deferred to the expiry"
    );
    assert_eq!(a.derived().next_ring_deadline(), Some(exp));
    // A later, unrelated entry (a Noop) whose stamp is past the expiry: apply
    // promotes the ring even though nothing touched pid 1.
    a.apply(&Build::new(exp + 1, 2, 30).cmd(vec![Effect::Noop]).at(4, 1))
        .expect("tick past expiry");
    assert!(
        a.derived().ring_has_ready(TENANT, QUEUE, "g1"),
        "the expired lease did not re-arm the live ring"
    );
    a.commit().expect("commit");
    let rebuilt = node
        .store()
        .read(|r| Ok(crate::rsm::state::Derived::rebuild(r, exp + 1)?))
        .expect("read");
    assert!(
        rebuilt.ring_has_ready(TENANT, QUEUE, "g1"),
        "a rebuild past the expiry disagrees with the live ring"
    );
}

#[test]
fn transitions_ring_equals_a_rebuild_at_every_boundary() {
    // The strong invariant: with transitions on, after EVERY entry the live
    // rings equal a rebuild from committed `pending` at that entry's stamp —
    // ready membership, parked count and next deadline. Unlike the earlier
    // equality test this drives SHORT leases and a clock that jumps past them,
    // so promotions actually fire and the promote-at-boundary path is exercised.
    let groups = ["g1", "g2"];
    let node = Node::new("boundary-eq");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        transitions_cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    a.apply(&setup_entry(4, &groups, queue_config(BASE_US)))
        .expect("setup");

    let lease = 300_000i64;
    let mut last = [-1i64; 4];
    let mut committed = [[-1i64; 2]; 4];
    let mut rng = 0x51ED_2A17u64;
    let mut roll = |rng: &mut u64| {
        *rng = rng
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        *rng >> 20
    };
    let mut now = BASE_US + 1_000;
    let mut idx = 2u64;
    let mut ids = 1_000u64;

    for step in 0..250u64 {
        now += 80_000 + (roll(&mut rng) % 400_000) as i64; // steps that cross a lease
        let pi = (roll(&mut rng) % 4) as usize;
        let pid = 1 + pi as u64;
        let want_append = roll(&mut rng) % 5 < 3 || last[pi] < 0;
        let effects = if want_append {
            let count = 1 + (roll(&mut rng) % 4) as u32;
            let base = (last[pi] + 1) as u64;
            last[pi] += count as i64;
            vec![Effect::Append {
                pid,
                bucket: (pi % 8) as u16,
                base_offset: base,
                count,
                created_at_us: now,
                hashes: hashes(roll(&mut rng), count),
                blob: vec![0xAB; 24 * count as usize],
            }]
        } else {
            let gi = (roll(&mut rng) % 2) as usize;
            let step_by = 1 + (roll(&mut rng) % 3) as i64;
            let cur = committed[pi][gi];
            let next = (cur + step_by).min(last[pi]);
            committed[pi][gi] = next;
            let leased = roll(&mut rng) % 2 == 0 && next < last[pi];
            let row = if leased {
                lease_pop(next, last[pi] as u64, now + lease, now)
            } else {
                fresh_cursor(next, now)
            };
            vec![Effect::CursorSet {
                pid,
                group: groups[gi].into(),
                row,
            }]
        };
        a.apply(&Build::new(now, 5, ids).cmd(effects).at(idx, 1))
            .expect("apply");
        ids += 2;
        idx += 1;
        a.commit().expect("commit");

        let live = ring_summary(a.derived(), &groups);
        let rebuilt = node
            .store()
            .read(|r| {
                Ok(ring_summary(
                    &crate::rsm::state::Derived::rebuild(r, now)?,
                    &groups,
                ))
            })
            .expect("read");
        assert_eq!(
            live, rebuilt,
            "step {step} at now {now}: live rings != a rebuild",
        );
    }
}

#[test]
fn slow_consumers_never_leave_the_ring_empty_while_lag_remains() {
    // 8 partitions, one group, short leases, consumers that lease and stall
    // (never ack), with fresh frames arriving under the leases. Every round the
    // clock jumps past the lease, so the held partitions must re-arm: the ring a
    // pop would walk (both the live one and a rebuild) must hold EXACTLY the
    // partitions that are claimable now — never empty while any partition has
    // un-acked backlog and no live lease. This is the lease pathology AB-0 could
    // not provoke, made deterministic.
    const N: u64 = 8;
    let lease = 200_000i64;
    let node = Node::new("slow-consumers");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        transitions_cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    a.apply(&setup_entry(N, &["g1"], queue_config(BASE_US)))
        .expect("setup");
    // Seed every partition with 3 frames.
    let mut last = [-1i64; N as usize];
    let mut committed = [-1i64; N as usize];
    let mut lease_exp = [None::<i64>; N as usize];
    let mut idx = 2u64;
    let mut ids = 1_000u64;
    for p in 0..N as usize {
        a.apply(
            &Build::new(BASE_US + 10 + p as i64, 1 + N, ids)
                .cmd(vec![Effect::Append {
                    pid: 1 + p as u64,
                    bucket: (p % 8) as u16,
                    base_offset: 0,
                    count: 3,
                    created_at_us: BASE_US + 10 + p as i64,
                    hashes: hashes(p as u64 + 1, 3),
                    blob: vec![0xAB; 72],
                }])
                .at(idx, 1),
        )
        .expect("seed");
        last[p] = 2;
        idx += 1;
        ids += 1;
    }

    let mut now = BASE_US + 1_000;
    for round in 0..40u64 {
        now += lease + 50_000; // every held lease from last round has expired

        // Claimable now = backlog remains AND no live lease.
        let claimable = |committed: &[i64], last: &[i64], lease_exp: &[Option<i64>]| -> Vec<Pid> {
            (0..N as usize)
                .filter(|&p| {
                    committed[p] < last[p] && lease_exp[p].map(|e| e <= now).unwrap_or(true)
                })
                .map(|p| 1 + p as u64)
                .collect()
        };

        // Build one entry: pop (lease, no ack) up to 4 claimable partitions, and
        // slip a fresh frame under one still-leased partition.
        let mut effects = Vec::new();
        let mut expect = claimable(&committed, &last, &lease_exp);
        expect.sort_unstable();
        // Appends during leases: add a frame to a partition leased last round.
        if let Some(p) = (0..N as usize).find(|&p| lease_exp[p].is_some_and(|e| e > now - lease)) {
            let base = (last[p] + 1) as u64;
            effects.push(Effect::Append {
                pid: 1 + p as u64,
                bucket: (p % 8) as u16,
                base_offset: base,
                count: 1,
                created_at_us: now,
                hashes: hashes(9_000 + round, 1),
                blob: vec![0xAB; 24],
            });
            last[p] += 1;
        }
        for &pid in expect.iter().take(4) {
            let p = (pid - 1) as usize;
            // Lease without acking: committed unchanged, backlog stays.
            effects.push(Effect::CursorSet {
                pid,
                group: "g1".into(),
                row: lease_pop(committed[p], last[p] as u64, now + lease, now),
            });
            lease_exp[p] = Some(now + lease);
        }
        if effects.is_empty() {
            effects.push(Effect::Noop);
        }
        a.apply(&Build::new(now, 1 + N, ids).cmd(effects).at(idx, 1))
            .expect("round");
        idx += 1;
        ids += 1;
        a.commit().expect("commit");

        // After the entry, the just-leased partitions are deferred; recompute
        // what is claimable at this same `now` and assert BOTH rings hold it.
        let mut want = claimable(&committed, &last, &lease_exp);
        want.sort_unstable();
        let live = ring_summary(a.derived(), &["g1"]);
        let rebuilt = node
            .store()
            .read(|r| {
                Ok(ring_summary(
                    &crate::rsm::state::Derived::rebuild(r, now)?,
                    &["g1"],
                ))
            })
            .expect("read");
        assert_eq!(live[0].1, want, "round {round}: live ring != claimable");
        assert_eq!(rebuilt[0].1, want, "round {round}: rebuild != claimable");
        // Lag is always > 0 here (nothing is ever acked), and every lease from a
        // prior round has expired, so there is always claimable work and the
        // ring is never empty.
        assert!(
            !want.is_empty(),
            "round {round}: lag remains but nothing is claimable"
        );
    }
}

#[test]
fn batched_counters_survive_a_crash_at_a_non_durable_commit() {
    crash_at_non_durable_commit(false);
    crash_at_non_durable_commit(true);
}

/// The `replicator_crash` shape in process: apply a prefix with periodic
/// NON-DURABLE commits (no durable point, so `durable_index` stays 0), then a
/// tail with no commit, then drop the applier — the open transaction AND the
/// per-transaction overlay (counters, and with transitions on the `pending`
/// decisions) go with it. Reopen at the committed prefix and replay the whole
/// stream (the prefix is skipped), then settle. The state must equal a clean
/// run of the same stream (PERF-D: the overlay is transparent across a crash at
/// a non-durable commit).
fn crash_at_non_durable_commit(pending_transitions: bool) {
    let cfg = ApplyConfig {
        pending_transitions,
        ..cfg()
    };
    let m = 60u64;
    let mut w = Workload::new(0xC0FFEE);
    let entries: Vec<Committed> = (0..m).map(|_| w.next()).collect();

    let clean = Node::new("ctr-crash-ref");
    {
        let (mut a, _) = Applier::open(
            clean.store(),
            &clean.seg_dir(),
            seg_opts(),
            cfg.clone(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for (i, c) in entries.iter().enumerate() {
            a.apply(c).expect("apply");
            if i % 5 == 4 {
                a.commit().expect("commit");
            }
        }
        settle(&mut a);
    }
    let want = clean.digest();

    let node = Node::new("ctr-crash");
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg.clone(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for (i, c) in entries.iter().take(20).enumerate() {
            a.apply(c).expect("apply");
            if i % 5 == 4 {
                a.commit().expect("commit"); // non-durable
            }
        }
        // A tail with no commit, then the applier (and the overlay) is dropped.
        for c in entries.iter().take(40).skip(20) {
            a.apply(c).expect("apply");
        }
        assert_eq!(a.durable_index(), 0, "no durable point in the prefix");
    }
    {
        let (mut a, rec) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg.clone(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        assert!(
            rec.applied_index > 0 && rec.applied_index <= 20 && rec.durable_index == 0,
            "reopened at a non-durable commit inside the prefix, got applied {} durable {}",
            rec.applied_index,
            rec.durable_index
        );
        for c in &entries {
            a.apply(c).expect("apply"); // the prefix is skipped
        }
        settle(&mut a);
    }
    let got = node.digest();
    assert_eq!(
        got.whole,
        want.whole,
        "state diverged across the crash (pending_transitions={pending_transitions}), \
         first at {:?}",
        got.first_difference(&want),
    );
}

// ---------------------------------------------------------------------------
// The gates on an entry
// ---------------------------------------------------------------------------

#[test]
fn an_entry_at_or_below_the_applied_index_is_skipped() {
    let node = Node::new("skip");
    let (mut a, rec) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    assert_eq!(rec.applied_index, 0);
    assert_eq!(rec.replay_after, 0);

    let c = Build::new(BASE_US, 1, 0)
        .cmd(vec![Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            cfg: queue_config(BASE_US),
        }])
        .at(1, 1);
    assert!(matches!(
        a.apply(&c).expect("apply"),
        Applied::Executed { .. }
    ));
    // The same entry again: already in state, so it is a no-op. This one guard
    // is the whole of apply's idempotence (§11.5's repair replays from the
    // durable index, which is at or below the applied one).
    assert_eq!(a.apply(&c).expect("re-apply"), Applied::Skipped);
    assert_eq!(a.applied_index(), 1);
    assert_eq!(a.stats().skipped, 1);
}

#[test]
fn a_hole_in_the_log_is_refused() {
    let node = Node::new("gap");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    let c = Build::new(BASE_US, 1, 0).cmd(vec![Effect::Noop]).at(3, 1);
    match a.apply(&c) {
        Err(ApplyError::Gap { expected, got }) => {
            assert_eq!((expected, got), (1, 3));
        }
        other => panic!("expected a refused hole, got {other:?}"),
    }
}

#[test]
fn the_planner_assigned_bases_are_asserted_against_meta() {
    // I18. The header exists to make this checkable and nothing else checks
    // it: two `PartitionCreate`s carrying one pid make both partitions one
    // partition on every node at once.
    let node = Node::new("bases");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");

    let mut e = Entry::new(BASE_US, 7, 1);
    e.add_command(
        request_id(1),
        Outcome::Empty,
        vec![Effect::PartitionCreate {
            pid: 7,
            uuid: uuid(7),
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            partition: "p0".into(),
            created_at_us: BASE_US,
        }],
    )
    .expect("add");
    let c = Committed {
        index: 1,
        term: 1,
        entry: e,
    };
    match a.apply(&c) {
        Err(ApplyError::Bases {
            what,
            expected,
            got,
        }) => {
            assert_eq!(what, "partition id");
            assert_eq!((expected, got), (1, 7));
        }
        other => panic!("expected an I18 refusal, got {other:?}"),
    }

    // And the base that DOES match advances by exactly the number of ids the
    // entry assigned.
    let c = Build::new(BASE_US, 1, 0)
        .cmd(vec![
            Effect::PartitionCreate {
                pid: 1,
                uuid: uuid(1),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: BASE_US,
            },
            Effect::PartitionCreate {
                pid: 2,
                uuid: uuid(2),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p1".into(),
                created_at_us: BASE_US,
            },
        ])
        .at(1, 1);
    a.apply(&c).expect("apply");
    a.commit().expect("commit");
    let next = node.store().read(|r| r.next_pid()).expect("read next_pid");
    assert_eq!(next, 3);
}

#[test]
fn time_never_goes_backwards() {
    // I5. The planner's `now` is monotone across terms by construction; apply
    // is the belt, because every lease expiry and every TTL is judged against
    // it.
    let node = Node::new("time");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    a.apply(&Build::new(BASE_US, 1, 0).cmd(vec![Effect::Noop]).at(1, 1))
        .expect("apply");
    let back = Build::new(BASE_US - 1, 1, 10)
        .cmd(vec![Effect::Noop])
        .at(2, 1);
    assert!(matches!(
        a.apply(&back),
        Err(ApplyError::TimeWentBackwards { .. })
    ));
}

#[test]
fn a_kind_this_build_cannot_apply_stops_the_node() {
    // I16: never skipped. Phase 2 brings the `kv` keyspace and the arm
    // together; until then the node refuses rather than dropping a committed
    // effect on the floor.
    let node = Node::new("unsupported");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    let c = Build::new(BASE_US, 1, 0)
        .cmd(vec![Effect::KvPut {
            tenant: TENANT.into(),
            ns: "n".into(),
            key: "k".into(),
            value: b"1".to_vec(),
            version: 1,
            expires_at_us: None,
            created_at_us: BASE_US,
            updated_at_us: BASE_US,
        }])
        .at(1, 1);
    match a.apply(&c) {
        Err(e @ ApplyError::Unsupported { .. }) => assert!(e.fatal()),
        other => panic!("expected an unsupported kind, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// The message path
// ---------------------------------------------------------------------------

#[test]
fn an_append_writes_the_payload_the_index_and_the_pending_rows() {
    let node = Node::new("append");
    let rec = Arc::new(Recorder::default());
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            rec.clone(),
        )
        .expect("open");

        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![
                    Effect::QueueUpsert {
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        cfg: queue_config(BASE_US),
                    },
                    Effect::GroupUpsert {
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        group: "g1".into(),
                        meta: group_meta(0, BASE_US),
                    },
                    Effect::PartitionCreate {
                        pid: 1,
                        uuid: uuid(1),
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        partition: "p0".into(),
                        created_at_us: BASE_US,
                    },
                ])
                .at(1, 1),
        )
        .expect("apply");

        let blob = b"the packed frames".to_vec();
        a.apply(
            &Build::new(BASE_US + 10, 2, 10)
                .cmd(vec![Effect::Append {
                    pid: 1,
                    bucket: 1,
                    base_offset: 0,
                    count: 3,
                    created_at_us: BASE_US + 10,
                    hashes: hashes(42, 3),
                    blob: blob.clone(),
                }])
                .at(2, 1),
        )
        .expect("apply");
        a.commit().expect("commit");

        // The bytes are readable through the position the store recorded, and
        // the frame verifies on the way out.
        let pos = node
            .store()
            .read(|r| r.seg_loc(1, 0))
            .expect("read")
            .expect("a seg_loc row for the append");
        let frame = a
            .reader()
            .read(segments::Position {
                bucket: pos.bucket,
                file_id: pos.file_id,
                offset: pos.offset,
                len: pos.len,
            })
            .expect("read the frame");
        assert_eq!(frame.blob, blob);
        assert_eq!(frame.pid, 1);
        assert_eq!(frame.count, 3);
    }

    node.store()
        .read(|r| {
            // The partition row carries the visible tail and the stamps.
            let p = r.partition(1).unwrap().expect("partition row");
            assert_eq!(p.last_offset, 2);
            assert_eq!(p.last_created_at_us, BASE_US + 10);
            assert_eq!(r.max_created_at_us().unwrap(), BASE_US + 10);

            // Dedup: every hash of the append probes as a duplicate at its own
            // offset, and the txns row holds the whole list (D10 lean).
            for i in 0..3u64 {
                let h = hash_at(42, i);
                assert_eq!(
                    dedup::probe_one(r, 1, &h, 0).unwrap(),
                    Some(i),
                    "hash {i} must probe at its own offset"
                );
            }
            let row = r.get_raw(Keyspace::Txns, &keys::txns(1, 0)).unwrap();
            let row = dedup::TxnsRow::decode(row.expect("a txns row")).unwrap();
            assert_eq!(row.end, 2);
            assert_eq!(row.count(), 3);

            // One `pending` row per subscribed group (§6.1).
            assert_eq!(
                r.pending_at(TENANT, QUEUE, "g1", 1).unwrap(),
                Some(BASE_US + 10)
            );
            // Counters (§6.4).
            assert_eq!(r.partition_counter(1, Counter::Pushed).unwrap(), 3);
            assert_eq!(r.queue_counter(TENANT, QUEUE, Counter::Pushed).unwrap(), 3);
            assert_eq!(r.tenant_counter(TENANT, Counter::Pushed).unwrap(), 3);
            assert!(r.partition_counter(1, Counter::RetainedBytes).unwrap() > 0);
            Ok(())
        })
        .expect("read");

    // The wake that a parked long-poll needs (§9.5), and the waiter
    // resolution of I4, both happened and named the group.
    let wakes = rec.wakes.lock().unwrap().clone();
    assert_eq!(
        wakes,
        vec![(
            TENANT.to_string(),
            QUEUE.to_string(),
            Some("g1".to_string())
        )]
    );
    assert_eq!(rec.applied.lock().unwrap().len(), 2);
}

#[test]
fn an_append_that_does_not_continue_the_partition_is_refused() {
    let node = Node::new("gapless");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![Effect::PartitionCreate {
                pid: 1,
                uuid: uuid(1),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: BASE_US,
            }])
            .at(1, 1),
    )
    .expect("apply");
    let c = Build::new(BASE_US + 1, 2, 10)
        .cmd(vec![Effect::Append {
            pid: 1,
            bucket: 1,
            base_offset: 5,
            count: 1,
            created_at_us: BASE_US + 1,
            hashes: hashes(1, 1),
            blob: b"x".to_vec(),
        }])
        .at(2, 1);
    match a.apply(&c) {
        Err(ApplyError::Inconsistent { what, .. }) => assert_eq!(what, "Append"),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[test]
fn a_cursor_carries_its_lease_index_and_wakes_on_release() {
    let node = Node::new("cursor");
    let rec = Arc::new(Recorder::default());
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            rec.clone(),
        )
        .expect("open");
        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![
                    Effect::QueueUpsert {
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        cfg: queue_config(BASE_US),
                    },
                    Effect::GroupUpsert {
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        group: "g1".into(),
                        meta: group_meta(0, BASE_US),
                    },
                    Effect::PartitionCreate {
                        pid: 1,
                        uuid: uuid(1),
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        partition: "p0".into(),
                        created_at_us: BASE_US,
                    },
                ])
                .at(1, 1),
        )
        .expect("apply");
        a.apply(
            &Build::new(BASE_US + 10, 2, 10)
                .cmd(vec![Effect::Append {
                    pid: 1,
                    bucket: 1,
                    base_offset: 0,
                    count: 4,
                    created_at_us: BASE_US + 10,
                    hashes: hashes(7, 4),
                    blob: vec![1, 2, 3],
                }])
                .at(2, 1),
        )
        .expect("apply");

        // A claim: the lease index gets its row and the ring defers the
        // partition to the lease expiry.
        let mut row = fresh_cursor(-1, BASE_US);
        row.worker = Some("w1".into());
        row.lease_expires_at_us = Some(BASE_US + 30_000_000);
        row.lease_acquired_at_us = Some(BASE_US + 20);
        row.batch_end = Some(3);
        a.apply(
            &Build::new(BASE_US + 20, 2, 20)
                .cmd(vec![Effect::CursorSet {
                    pid: 1,
                    group: "g1".into(),
                    row,
                }])
                .at(3, 1),
        )
        .expect("apply");
        assert_eq!(a.derived().lease_count(), 1);
        a.commit().expect("commit");
        node.store()
            .read(|r| {
                let mut seen = Vec::new();
                r.scan_worker_leases("w1", 16, &mut |pid, g, at| {
                    seen.push((pid, g.to_string(), at));
                    true
                })?;
                assert_eq!(seen, vec![(1, "g1".to_string(), BASE_US + 30_000_000)]);
                Ok(())
            })
            .expect("read");

        // The release: ack everything, no lease. The `pending` row goes with
        // it and the wake fires for the group.
        let row = fresh_cursor(3, BASE_US);
        a.apply(
            &Build::new(BASE_US + 30, 2, 30)
                .cmd(vec![Effect::CursorSet {
                    pid: 1,
                    group: "g1".into(),
                    row,
                }])
                .at(4, 1),
        )
        .expect("apply");
        a.commit().expect("commit");
        assert_eq!(a.derived().lease_count(), 0);
    }

    node.store()
        .read(|r| {
            assert_eq!(r.pending_at(TENANT, QUEUE, "g1", 1).unwrap(), None);
            assert_eq!(
                r.counter_at(&keys::counter_group(
                    TENANT,
                    QUEUE,
                    "g1",
                    Counter::Completed
                ))
                .unwrap(),
                4
            );
            assert_eq!(
                r.counter_at(&keys::counter_group(TENANT, QUEUE, "g1", Counter::Pending))
                    .unwrap(),
                0
            );
            Ok(())
        })
        .expect("read");

    let wakes = rec.wakes.lock().unwrap().clone();
    // One for the append, one for the released lease.
    assert_eq!(wakes.len(), 2, "{wakes:?}");
    assert_eq!(wakes[1].2, Some("g1".to_string()));
}

#[test]
fn a_watermark_releases_the_payload_before_the_hash_lists() {
    // D10: the hash lists outlive the segments retention deletes, so a
    // re-push inside the txns window is still a duplicate after the payload
    // has gone.
    let node = Node::new("watermark");
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(1),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                }])
                .at(1, 1),
        )
        .expect("apply");
        for n in 0..4u64 {
            a.apply(
                &Build::new(BASE_US + 10 + n as i64, 2, 10 + n * 10)
                    .cmd(vec![Effect::Append {
                        pid: 1,
                        bucket: 1,
                        base_offset: n,
                        count: 1,
                        created_at_us: BASE_US + 10 + n as i64,
                        hashes: hashes(100 + n, 1),
                        blob: vec![7; 16],
                    }])
                    .at(2 + n, 1),
            )
            .expect("apply");
        }
        a.commit().expect("commit");
        let retained_before = node
            .store()
            .read(|r| r.partition_counter(1, Counter::RetainedBytes))
            .expect("read");
        assert!(retained_before > 0);

        // Retention takes the payload of offsets 0 and 1; the hash lists stay.
        a.apply(
            &Build::new(BASE_US + 100, 2, 100)
                .cmd(vec![Effect::Watermark {
                    pid: 1,
                    log_start: 2,
                    txns_start: 0,
                }])
                .at(6, 1),
        )
        .expect("apply");
        a.commit().expect("commit");
        node.store()
            .read(|r| {
                assert_eq!(
                    dedup::probe_one(r, 1, &hash_at(100, 0), 0).unwrap(),
                    Some(0),
                    "the hash list outlives the segment (D10)"
                );
                assert!(
                    r.seg_loc(1, 0).unwrap().is_some(),
                    "the position stays while the hash list needs to be findable"
                );
                assert!(r.partition_counter(1, Counter::RetainedBytes).unwrap() < retained_before);
                Ok(())
            })
            .expect("read");

        // The txns purge then takes them.
        a.apply(
            &Build::new(BASE_US + 200, 2, 200)
                .cmd(vec![Effect::Watermark {
                    pid: 1,
                    log_start: 2,
                    txns_start: 2,
                }])
                .at(7, 1),
        )
        .expect("apply");
        a.commit().expect("commit");
        node.store()
            .read(|r| {
                assert_eq!(dedup::probe_one(r, 1, &hash_at(100, 0), 0).unwrap(), None);
                assert_eq!(dedup::probe_one(r, 1, &hash_at(101, 0), 0).unwrap(), None);
                assert!(r.seg_loc(1, 0).unwrap().is_none());
                assert!(r.seg_loc(1, 2).unwrap().is_some(), "offset 2 is still live");
                assert_eq!(
                    dedup::probe_one(r, 1, &hash_at(102, 0), 0).unwrap(),
                    Some(2)
                );
                let p = r.partition(1).unwrap().unwrap();
                assert_eq!((p.log_start, p.txns_start), (2, 2));
                Ok(())
            })
            .expect("read");

        // And it never moves back.
        let back = Build::new(BASE_US + 300, 2, 300)
            .cmd(vec![Effect::Watermark {
                pid: 1,
                log_start: 1,
                txns_start: 0,
            }])
            .at(8, 1);
        assert!(matches!(
            a.apply(&back),
            Err(ApplyError::Inconsistent {
                what: "Watermark",
                ..
            })
        ));
    }
}

#[test]
fn a_queue_delete_frees_the_names_and_the_chunks_take_the_rest() {
    // §5.2's rules: the name-keyed rows go at once (the name is reusable
    // immediately), the pid-keyed data goes in bounded chunks behind a
    // `GarbageAdd`, and readers ignore a garbage pid from the moment it is
    // marked.
    let node = Node::new("delete");
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        let mut w = Workload::new(99);
        for _ in 0..40 {
            let c = w.next();
            a.apply(&c).expect("apply");
        }
        a.commit().expect("commit");
        let pids: Vec<Pid> = node
            .store()
            .read(|r| {
                let mut v = Vec::new();
                r.scan_queue_partitions(TENANT, QUEUE, None, 64, &mut |p| {
                    v.push(p);
                    true
                })?;
                Ok(v)
            })
            .expect("read");
        assert_eq!(pids.len(), 4);

        a.apply(
            &Build::new(BASE_US + 1_000_000, 5, 10_000)
                .cmd(vec![
                    Effect::QueueDelete {
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                    },
                    Effect::GarbageAdd {
                        pids: pids.clone(),
                        scope: GarbageScope::Queue,
                        deleted_at_us: BASE_US + 1_000_000,
                    },
                ])
                .at(41, 1),
        )
        .expect("apply");
        a.commit().expect("commit");

        // The name is free at once: nothing under it is left.
        node.store()
            .read(|r| {
                assert!(r.queue(TENANT, QUEUE).unwrap().is_none());
                assert!(r.group(TENANT, QUEUE, "g1").unwrap().is_none());
                assert_eq!(r.pid_of(TENANT, QUEUE, "p0").unwrap(), None);
                let mut n = 0;
                r.scan_queue_partitions(TENANT, QUEUE, None, 64, &mut |_p| {
                    n += 1;
                    true
                })?;
                assert_eq!(n, 0);
                for pid in &pids {
                    assert!(r.garbage(*pid).unwrap().is_some(), "pid {pid} is garbage");
                }
                Ok(())
            })
            .expect("read");

        // The chunks. A small limit, so the resume key is exercised across
        // stages and pids, and the loop must terminate.
        let mut index = 42u64;
        let mut ids = 20_000u64;
        for _ in 0..200 {
            let left = node
                .store()
                .read(|r| r.count(Keyspace::Garbage))
                .expect("read");
            if left == 0 {
                break;
            }
            a.apply(
                &Build::new(BASE_US + 1_000_000 + index as i64, 5, ids)
                    .cmd(vec![Effect::DeleteChunk {
                        pids: pids.clone(),
                        scope: GarbageScope::Queue,
                        resume: Vec::new(),
                        limit: 7,
                    }])
                    .at(index, 1),
            )
            .expect("apply");
            a.commit().expect("commit");
            index += 1;
            ids += 10;
        }

        node.store()
            .read(|r| {
                assert_eq!(
                    r.count(Keyspace::Garbage).unwrap(),
                    0,
                    "the chunks finished"
                );
                for ks in [
                    Keyspace::Partitions,
                    Keyspace::Cursors,
                    Keyspace::Dedup,
                    Keyspace::Txns,
                    Keyspace::SegLoc,
                    Keyspace::Dlq,
                    Keyspace::DlqByPos,
                    Keyspace::PartitionFiles,
                    Keyspace::Pending,
                    Keyspace::LeasesByWorker,
                ] {
                    assert_eq!(r.count(ks).unwrap(), 0, "{} still holds rows", ks.name());
                }
                // The TENANT survives its queue, so its counters do; what they
                // must not survive is the storage the queue held, which the
                // proxy's quota reads.
                let mut left = Vec::new();
                r.scan_raw(Keyspace::Counters, &[], &[], usize::MAX, &mut |k, _v| {
                    left.push(k[0]);
                    true
                })?;
                assert!(
                    left.iter()
                        .all(|scope| *scope == keys::CounterScope::Tenant as u8),
                    "only tenant-scope counters survive a queue delete, found {left:?}"
                );
                assert_eq!(r.tenant_counter(TENANT, Counter::RetainedBytes).unwrap(), 0);
                Ok(())
            })
            .expect("read");
    }
}

// ---------------------------------------------------------------------------
// Counters
// ---------------------------------------------------------------------------

#[test]
fn counters_equal_a_recount() {
    // §6.4/D16: counters are maintained at apply, O(1) per effect, and there
    // is no periodic aggregation to fall back on — so the only check is to
    // recompute every one of them from the rows.
    let node = Node::new("counters");
    let _ = run_workload(&node, 4242, 120, 40);

    node.store()
        .read(|r| {
            let mut pids = Vec::new();
            r.scan_queue_partitions(TENANT, QUEUE, None, 64, &mut |p| {
                pids.push(p);
                true
            })?;
            assert_eq!(pids.len(), 4);

            let groups = ["g1", "g2"];
            let mut queue_pushed = 0i64;
            let mut queue_retained = 0i64;
            let mut queue_dlq = 0i64;
            let mut group_completed: BTreeMap<&str, i64> = BTreeMap::new();
            let mut group_pending: BTreeMap<&str, i64> = BTreeMap::new();

            for pid in &pids {
                let p = r.partition(*pid).unwrap().expect("partition row");
                // Offsets are gapless from 0, so the tail IS the count pushed.
                let pushed = p.last_offset + 1;
                assert_eq!(
                    r.partition_counter(*pid, Counter::Pushed).unwrap(),
                    pushed,
                    "pid {pid} pushed"
                );
                queue_pushed += pushed;

                // Retained bytes: the sum of the live positions.
                let mut retained = 0i64;
                r.scan_seg_loc(*pid, 0, usize::MAX, &mut |_b, row| {
                    retained += row.len as i64;
                    true
                })?;
                // The positions of frames whose payload retention dropped are
                // still there while their hash lists are (D10), so the counter
                // is the sum over offsets at or above `log_start`.
                let mut live = 0i64;
                r.scan_seg_loc(*pid, p.log_start, usize::MAX, &mut |_b, row| {
                    live += row.len as i64;
                    true
                })?;
                assert_eq!(
                    r.partition_counter(*pid, Counter::RetainedBytes).unwrap(),
                    live,
                    "pid {pid} retained bytes (of {retained} recorded)"
                );
                queue_retained += live;

                for g in groups {
                    let committed = r
                        .cursor(*pid, g)
                        .unwrap()
                        .map(|c| c.committed)
                        .unwrap_or(-1);
                    let completed = committed + 1;
                    *group_completed.entry(g).or_default() += completed;
                    *group_pending.entry(g).or_default() += pushed - completed;
                }

                // Counted from the DLQ ROWS, not from the position index:
                // two dead letters of one partition can share a
                // `(pid, group, offset)` — a replayed message that dies again
                // — and the index holds the newest, while both rows exist and
                // both were counted.
                let mut dlq = 0i64;
                r.scan_raw(Keyspace::Dlq, &[], &[], usize::MAX, &mut |_k, v| {
                    if let Ok(row) = crate::rsm::store::rows::dlq_decode(v) {
                        if row.pid == *pid {
                            dlq += 1;
                        }
                    }
                    true
                })?;
                assert_eq!(
                    r.partition_counter(*pid, Counter::DlqCount).unwrap(),
                    dlq,
                    "pid {pid} dead letters"
                );
                queue_dlq += dlq;
            }

            assert_eq!(
                r.queue_counter(TENANT, QUEUE, Counter::Pushed).unwrap(),
                queue_pushed
            );
            assert_eq!(
                r.tenant_counter(TENANT, Counter::Pushed).unwrap(),
                queue_pushed
            );
            assert_eq!(
                r.queue_counter(TENANT, QUEUE, Counter::RetainedBytes)
                    .unwrap(),
                queue_retained
            );
            assert_eq!(
                r.tenant_counter(TENANT, Counter::RetainedBytes).unwrap(),
                queue_retained
            );
            assert_eq!(
                r.queue_counter(TENANT, QUEUE, Counter::DlqCount).unwrap(),
                queue_dlq
            );
            for (g, completed) in &group_completed {
                assert_eq!(
                    r.counter_at(&keys::counter_group(TENANT, QUEUE, g, Counter::Completed))
                        .unwrap(),
                    *completed,
                    "group {g} completed"
                );
                assert_eq!(
                    r.counter_at(&keys::counter_group(TENANT, QUEUE, g, Counter::Pending))
                        .unwrap(),
                    group_pending[g],
                    "group {g} pending: pushed minus completed, over every partition"
                );
            }
            Ok(())
        })
        .expect("read");
}

// ---------------------------------------------------------------------------
// I2: determinism
// ---------------------------------------------------------------------------

#[test]
fn two_nodes_with_different_hash_seeds_reach_the_same_digest() {
    // I2's own test, as §4 words it: two fresh states with different
    // `RandomState` seeds apply the same entries and end with equal digests of
    // every replicated keyspace, in key order.
    //
    // The seeds ARE different: `RandomState::new` bumps a per-thread counter
    // for every instance, so the two `Derived`s, the two stores and everything
    // they build hash differently. The assertion below fails the test rather
    // than passing it vacuously if that ever stops being true.
    use std::collections::hash_map::RandomState;
    use std::hash::BuildHasher;
    let (a_seed, b_seed) = (RandomState::new(), RandomState::new());
    assert_ne!(
        a_seed.hash_one("queen"),
        b_seed.hash_one("queen"),
        "two RandomStates in one process must differ, or this test proves nothing"
    );

    let one = Node::new("det-a");
    let two = Node::new("det-b");
    // Different durable cadences as well: a durable point is node-local and
    // must not change a single byte of replicated state.
    let da = run_workload(&one, 20260918, 150, 25);
    let db = run_workload(&two, 20260918, 150, 7);

    assert_eq!(
        da.per_keyspace.len(),
        db.per_keyspace.len(),
        "the keyspace list is the same on both"
    );
    if da.whole != db.whole {
        panic!(
            "the two states differ, first at {:?}\n a = {:?}\n b = {:?}",
            da.first_difference(&db),
            da.per_keyspace,
            db.per_keyspace
        );
    }
    // And it is not the digest of an empty state.
    let rows: u64 = da.per_keyspace.iter().map(|(_, _, n)| n).sum();
    assert!(rows > 100, "the workload wrote {rows} rows");
}

#[test]
fn re_applying_from_the_durable_index_changes_nothing() {
    // The in-process half of §11.5's single-voter repair: replay the local log
    // from the durable point. Entries the store already holds are skipped, so
    // the state is the one an uninterrupted run produced, byte for byte.
    let reference = Node::new("idem-ref");
    let want = run_workload(&reference, 7, 80, 20);

    let node = Node::new("idem");
    let got = {
        let mut w = Workload::new(7);
        let mut entries = Vec::new();
        for _ in 0..80 {
            entries.push(w.next());
        }
        {
            let (mut a, _) = Applier::open(
                node.store(),
                &node.seg_dir(),
                seg_opts(),
                cfg(),
                Arc::new(crate::rsm::apply::NoNotify),
            )
            .expect("open");
            for (i, c) in entries.iter().enumerate() {
                a.apply(c).expect("apply");
                // No point at the very end: the replay must have entries
                // above the durable index to re-deliver.
                if (i + 1) % 20 == 0 && i + 1 < 80 {
                    a.durable_point().expect("durable point");
                }
            }
            let durable = a.durable_index();
            assert!(durable > 0 && durable < 80);
            // The repair: replay everything after the durable point. The store
            // is AHEAD of it (pin 1 plus an intact page cache), so most of
            // these are skips.
            let mut skipped = 0;
            for c in entries.iter().filter(|c| c.index > durable) {
                if a.apply(c).expect("re-apply") == Applied::Skipped {
                    skipped += 1;
                }
            }
            assert!(skipped > 0, "the replay must cross the applied index");
            a.flush().expect("flush");
        }
        node.digest()
    };
    assert_eq!(
        got.whole,
        want.whole,
        "first difference: {:?}",
        got.first_difference(&want)
    );
}

// ---------------------------------------------------------------------------
// Recovery, durable points and GC
// ---------------------------------------------------------------------------

#[test]
fn a_reopened_node_recovers_its_indexes_and_replays_from_the_durable_point() {
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-apply-reopen-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut node = Node::at(dir.clone());
    let (applied, durable, before) = {
        let mut w = Workload::new(11);
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        for n in 1..=60u64 {
            let c = w.next();
            a.apply(&c).expect("apply");
            if n == 40 {
                a.durable_point().expect("durable point");
            }
        }
        a.commit().expect("commit");
        (a.applied_index(), a.durable_index(), node.digest())
    };
    assert_eq!(applied, 60);
    assert_eq!(durable, 40);

    node.close();
    let node = Node::at(dir);
    {
        let (a, rec) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("reopen");
        // §11.5 step 2: the applied index the REOPENED state reports, which is
        // legitimately past the durable point.
        assert_eq!(rec.applied_index, 60);
        assert_eq!(rec.durable_index, 40);
        assert_eq!(rec.replay_after, 40);
        // Step 4: the rings and the leases are back, rebuilt from `pending`
        // and `leases_by_worker` and from nothing else.
        assert!(rec.rings > 0, "the ready rings came back");
        assert!(
            a.derived().pending_rows() > 0,
            "the rebuild walked the pending keyspace"
        );
        // Step 3 found no disagreement: nothing was truncated below a
        // recorded length and no file was missing.
        assert!(rec.segments.truncated.is_empty(), "{:?}", rec.segments);
    }
    assert_eq!(
        node.digest().whole,
        before.whole,
        "a reopen changes nothing"
    );
}

#[test]
fn a_failed_durable_point_reports_no_durable_index() {
    // §11.4 step 3 and the S1/S3 finding behind `StoreError::CommitFailed`: on
    // Linux the kernel consumes an fsync error and drops the pages, so the
    // failing call is the ONLY moment the failure is visible. A durable index
    // reported here would let a log be truncated behind effects nothing can
    // replay (I4, I11).
    let node = Node::new("fsync-fail");
    let rec = Arc::new(Recorder::default());
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        rec.clone(),
    )
    .expect("open");
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![Effect::PartitionCreate {
                pid: 1,
                uuid: uuid(1),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: BASE_US,
            }])
            .at(1, 1),
    )
    .expect("apply");

    node.store().fail_next_sync();
    let err = a.durable_point().expect_err("the durable point must fail");
    assert!(err.lost_durable_point(), "{err}");
    assert!(err.fatal());
    assert_eq!(a.durable_index(), 0, "nothing is reported as durable");
    assert!(
        rec.durable.lock().unwrap().is_empty(),
        "the replicator was never told"
    );
    assert_eq!(a.stats().durable_points_failed, 1);
}

#[test]
fn a_pin_blocks_the_unlink_until_it_is_dropped() {
    // I4 and I10 together: a committed pop claim pins the files holding its
    // segments until the payloads have been read, so retention cannot unlink
    // the bytes between the claim's apply and the payload read.
    let dir = tmp_dir("pins");
    let mut node = Node::at(dir.clone());
    let (mut a, _) = open_at(&node);
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![Effect::PartitionCreate {
                pid: 1,
                uuid: uuid(1),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: BASE_US,
            }])
            .at(1, 1),
    )
    .expect("apply");

    // Enough appends to roll the 8 KiB file, so there is a SEALED file to
    // unlink (the active one never is).
    let mut index = 2u64;
    for n in 0..24u64 {
        a.apply(
            &Build::new(BASE_US + 10 + n as i64, 2, 100 + n * 10)
                .cmd(vec![Effect::Append {
                    pid: 1,
                    bucket: 1,
                    base_offset: n,
                    count: 1,
                    created_at_us: BASE_US + 10 + n as i64,
                    hashes: hashes(500 + n, 1),
                    blob: vec![9; 700],
                }])
                .at(index, 1),
        )
        .expect("apply");
        index += 1;
    }
    a.durable_point().expect("durable point");
    let sealed: Vec<(u16, u32)> = a
        .segments_mut()
        .files()
        .into_iter()
        .filter(|(_, _, m)| m.sealed)
        .map(|(b, id, _)| (b, id))
        .collect();
    assert!(!sealed.is_empty(), "the appends must have rolled a file");
    let (bucket, file_id) = sealed[0];

    // A reader holds the file, exactly as a committed claim does.
    let reader = a.reader();
    let pin = reader.pin(bucket, file_id).expect("a pin for a live file");

    // Retention drops everything: the file is dead but pinned.
    let last = 24u64;
    a.apply(
        &Build::new(BASE_US + 1000, 2, 9000)
            .cmd(vec![Effect::Watermark {
                pid: 1,
                log_start: last,
                txns_start: last,
            }])
            .at(index, 1),
    )
    .expect("apply");
    a.gc_pass().expect("gc");
    a.durable_point().expect("durable point");
    a.gc_pass().expect("gc");
    a.durable_point().expect("durable point");
    assert!(
        a.segments_mut().file_meta(bucket, file_id).is_some(),
        "a pinned file is never unlinked"
    );
    // Its unpinned neighbours are free to go; this one is not.
    let unlinked_while_pinned = a.stats().files_unlinked;

    drop(pin);
    drop(reader);
    // Phase one removes the rows, the durable point that follows unlinks the
    // bytes (I10: never before a durable commit that no longer names it).
    a.gc_pass().expect("gc");
    a.durable_point().expect("durable point");
    assert!(
        a.segments_mut().file_meta(bucket, file_id).is_none(),
        "the file goes once nothing holds it"
    );
    assert!(
        a.stats().files_unlinked > unlinked_while_pinned,
        "the pinned file was unlinked only after the pin went"
    );
    assert_eq!(a.segments_mut().saturated_releases(), 0);
    drop(a);
    node.close();

    // And the node this GC left behind starts. The two phases ran here with a
    // pin in the middle, a deferral, and unlinks in two different durable
    // points — every shape that can leave the store naming a file that is not
    // there (I10, I11).
    let node = Node::at(dir);
    let (_a, rec) = open_at(&node);
    assert!(
        rec.segments.deleted.is_empty() && rec.segments.truncated.is_empty(),
        "recovery had to reconcile nothing: {rec:?}"
    );
}

// ---------------------------------------------------------------------------
// The thread
// ---------------------------------------------------------------------------

#[test]
fn the_thread_applies_in_order_and_flushes_when_the_channel_closes() {
    let node = Node::new("thread");
    let dir = node.path().to_path_buf();
    let seg = node.seg_dir();
    // The thread owns the store, so this node hands it over: heed refuses two
    // opens of one path in a process.
    let mut node = node;
    node.close();
    drop(node);

    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("open"));
    let rec = Arc::new(Recorder::default());
    let (tx, rx) = crate::rsm::apply::channel(8);
    let clock = Arc::new(crate::rsm::apply::SystemClock);
    let handle = crate::rsm::apply::spawn(
        store.clone(),
        seg,
        seg_opts(),
        ApplyConfig {
            durable_every_ms: 20,
            ..cfg()
        },
        rec.clone(),
        clock,
        rx,
    );

    let mut w = Workload::new(5);
    for _ in 0..50 {
        tx.send(w.next()).expect("send");
    }
    drop(tx);
    let stats = handle.join().expect("join").expect("the loop");
    assert_eq!(stats.entries, 50);
    assert!(stats.appends > 0);
    assert!(stats.durable_points >= 1, "{stats:?}");

    let applied = rec.applied.lock().unwrap().clone();
    assert_eq!(applied.len(), 50);
    assert!(
        applied.windows(2).all(|w| w[0].0 + 1 == w[1].0),
        "entries are resolved in index order"
    );
    let durable = rec.durable.lock().unwrap().clone();
    assert_eq!(durable.last().copied(), Some(50));

    let store = match Arc::try_unwrap(store) {
        Ok(s) => s,
        Err(_) => panic!("the apply thread still holds the store"),
    };
    assert_eq!(store.read(|r| r.applied_index()).expect("read"), 50);
    assert_eq!(store.read(|r| r.durable_index()).expect("read"), 50);
    store.close();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn the_async_durable_knob_leaves_the_same_state_and_reopens_clean() {
    // `QUEEN_RAFT_DURABLE_ASYNC` is a page-cache warm-up, not a change to what a
    // durable point records: the same log, driven through the real apply thread
    // with the helper on and with it off, must leave the SAME committed state
    // (I11), and each must reopen with recovery reconciling nothing. A tight
    // 15 ms durable cadence over 200 entries makes the loop take many points
    // under continuous appends — the shape the lever exists for.
    fn run(tag: &str, durable_async: bool) -> (StateDigest, StateDigest, u64) {
        let mut node = Node::new(tag);
        let dir = node.path().to_path_buf();
        let seg = node.seg_dir();
        node.keep();
        node.close();
        drop(node);

        let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("open"));
        let rec = Arc::new(Recorder::default());
        let (tx, rx) = crate::rsm::apply::channel(8);
        let clock = Arc::new(crate::rsm::apply::SystemClock);
        let handle = crate::rsm::apply::spawn(
            store.clone(),
            seg,
            seg_opts(),
            ApplyConfig {
                durable_every_ms: 15,
                durable_async,
                ..cfg()
            },
            rec.clone(),
            clock,
            rx,
        );
        let mut w = Workload::new(9);
        for _ in 0..200 {
            tx.send(w.next()).expect("send");
        }
        drop(tx);
        let stats = handle.join().expect("join").expect("the loop");
        assert_eq!(stats.entries, 200);
        assert!(stats.durable_points >= 1, "{tag}: {stats:?}");

        let store = Arc::try_unwrap(store)
            .ok()
            .expect("the apply thread released the store");
        let digest = store
            .read(|r| Ok(state_digest(r).expect("digest")))
            .expect("read");
        store.close();

        // Reopen from the platter: nothing to truncate or delete (I11).
        let node = Node::at(dir.clone());
        {
            let (a, rerec) = open_at(&node);
            assert!(
                rerec.segments.truncated.is_empty() && rerec.segments.deleted.is_empty(),
                "{tag}: a clean reopen, got {:?}",
                rerec.segments
            );
            drop(a);
        }
        let redigest = node.digest();
        drop(node);
        let _ = std::fs::remove_dir_all(&dir);
        (digest, redigest, stats.durable_points)
    }

    let (on, on_re, on_points) = run("async-on", true);
    let (off, off_re, _off_points) = run("async-off", false);
    assert_eq!(
        on.whole, off.whole,
        "the async knob does not change committed state"
    );
    assert_eq!(on.whole, on_re.whole, "state survives a reopen (async on)");
    assert_eq!(
        off.whole, off_re.whole,
        "state survives a reopen (async off)"
    );
    assert!(
        on_points >= 1,
        "the async run took durable points: {on_points}"
    );
}

/// The laptop before/after of PERF-A. Not a gate (macOS `fsync` is not the VM's
/// `fdatasync`, §0.3), printed on demand:
///
/// ```text
/// QUEEN_RAFT_METRICS=1 QUEEN_RAFT_DURABLE_ASYNC=0 \
///   cargo test -p queen-engine --lib -- --ignored --nocapture measure_async_durable_point
/// QUEEN_RAFT_METRICS=1 QUEEN_RAFT_DURABLE_ASYNC=1 \
///   cargo test -p queen-engine --lib -- --ignored --nocapture measure_async_durable_point
/// ```
///
/// Each run is a fresh process, so the global histograms hold one config only.
/// It drives a sustained-append workload through the real apply thread and
/// prints the durable-point, its segment-fsync leg, and the per-entry apply
/// histograms — the "durable point under continuous appends" numbers.
#[test]
#[ignore = "measurement, not a gate; run with --ignored --nocapture"]
fn measure_async_durable_point() {
    let durable_async = std::env::var("QUEEN_RAFT_DURABLE_ASYNC")
        .map(|v| v != "0")
        .unwrap_or(true);
    let entries: u64 = std::env::var("MEASURE_ENTRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(20_000);

    let node = Node::new("measure-async");
    let dir = node.path().to_path_buf();
    let seg = node.seg_dir();
    let mut node = node;
    node.close();
    drop(node);

    let store = Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("open"));
    let rec = Arc::new(Recorder::default());
    let (tx, rx) = crate::rsm::apply::channel(64);
    let clock = Arc::new(crate::rsm::apply::SystemClock);
    // A tight 50 ms durable cadence over many small-file appends: lots of
    // points under continuous writes, which is what the lever is for.
    let handle = crate::rsm::apply::spawn(
        store.clone(),
        seg,
        seg_opts(),
        ApplyConfig {
            durable_every_ms: 50,
            durable_async,
            ..cfg()
        },
        rec.clone(),
        clock,
        rx,
    );

    let t0 = std::time::Instant::now();
    let mut w = Workload::new(1);
    for _ in 0..entries {
        tx.send(w.next()).expect("send");
    }
    drop(tx);
    let stats = handle.join().expect("join").expect("the loop");
    let wall = t0.elapsed();

    let m = crate::rsm::timing::metrics();
    let dp = m.durable_point.snapshot();
    let seg_fsync = m.durable_seg_fsync.snapshot();
    let ae = m.apply_entry.snapshot();
    let ms = |ns: u64| ns as f64 / 1e6;
    let mean = |s: crate::rsm::timing::HistSnapshot| {
        if s.count == 0 {
            0.0
        } else {
            s.sum as f64 / s.count as f64 / 1e6
        }
    };
    let store = Arc::try_unwrap(store).ok().expect("released");
    store.close();
    let _ = std::fs::remove_dir_all(&dir);

    println!(
        "\n== measure_async_durable_point  QUEEN_RAFT_DURABLE_ASYNC={} ==",
        durable_async as u8
    );
    println!(
        "entries {entries}  wall {:.2}s  durable_points {}  appends {}  bytes_appended {}",
        wall.as_secs_f64(),
        stats.durable_points,
        stats.appends,
        stats.bytes_appended,
    );
    println!(
        "durable_point     mean {:.3}  p50 {:.3}  p99 {:.3}  max {:.3} ms  (n={})",
        mean(dp),
        ms(dp.p50),
        ms(dp.p99),
        ms(dp.max),
        dp.count
    );
    println!(
        "durable_seg_fsync mean {:.3}  p50 {:.3}  p99 {:.3}  max {:.3} ms  (n={})",
        mean(seg_fsync),
        ms(seg_fsync.p50),
        ms(seg_fsync.p99),
        ms(seg_fsync.max),
        seg_fsync.count
    );
    println!(
        "apply_entry       mean {:.4}  p50 {:.3}  p99 {:.3}  max {:.3} ms  (n={})",
        mean(ae),
        ms(ae.p50),
        ms(ae.p99),
        ms(ae.max),
        ae.count
    );
    assert!(stats.durable_points >= 1, "the run took durable points");
}

#[test]
fn the_cadences_are_node_local_and_bounded() {
    // §11.3/§11.4: the store commit is every 4 ms or 256 entries, the durable
    // point every 1000 ms or 256 MiB. A manual clock drives both, so the test
    // decides the time rather than the machine.
    let node = Node::new("cadence");
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        ApplyConfig {
            store_commit_entries: 4,
            ..cfg()
        },
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");
    assert!(!a.commit_due(Duration::ZERO), "nothing is dirty yet");
    for n in 1..=3u64 {
        a.apply(
            &Build::new(BASE_US + n as i64, 1, n * 10)
                .cmd(vec![Effect::Noop])
                .at(n, 1),
        )
        .expect("apply");
    }
    assert!(!a.commit_due(Duration::from_millis(1)), "3 of 4 entries");
    assert!(
        a.commit_due(Duration::from_millis(4)),
        "the 4 ms leg fires on its own"
    );
    a.apply(
        &Build::new(BASE_US + 4, 1, 40)
            .cmd(vec![Effect::Noop])
            .at(4, 1),
    )
    .expect("apply");
    assert!(a.commit_due(Duration::ZERO), "the 4-entry leg fires");
    assert!(!a.durable_due(Duration::from_millis(999)));
    assert!(a.durable_due(Duration::from_millis(1000)));
    a.commit().expect("commit");
    assert!(
        !a.commit_due(Duration::from_secs(1)),
        "nothing left to commit"
    );
}

#[test]
fn a_damaged_frame_below_a_recorded_length_refuses_to_start() {
    // §11.5 step 3 and I11: the store and the files disagree, and there is
    // nothing safe to assume. Phase 1 has no snapshot to repair from (WP-4.6),
    // so the answer is a TYPED refusal — never a truncation, never a partition
    // that quietly starts one message short.
    let dir = std::env::temp_dir().join(format!(
        "queen-rsm-apply-damaged-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let mut node = Node::at(dir.clone());
    let (bucket, file_id) = {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(1),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                }])
                .at(1, 1),
        )
        .expect("apply");
        for n in 0..3u64 {
            a.apply(
                &Build::new(BASE_US + 10 + n as i64, 2, 100 + n * 10)
                    .cmd(vec![Effect::Append {
                        pid: 1,
                        bucket: 3,
                        base_offset: n,
                        count: 1,
                        created_at_us: BASE_US + 10 + n as i64,
                        hashes: hashes(77 + n, 1),
                        blob: vec![5; 64],
                    }])
                    .at(2 + n, 1),
            )
            .expect("apply");
        }
        a.flush().expect("flush");
        (
            3u16,
            a.segments_mut().active_file(3).expect("an active file"),
        )
    };
    node.close();

    // One byte of the first frame's payload, well below the length the durable
    // point recorded.
    let path = node
        .seg_dir()
        .join(format!("b{bucket:03}"))
        .join(format!("f{file_id:010}.seg"));
    let mut bytes =
        std::fs::read(&path).unwrap_or_else(|e| panic!("the segment file {}: {e}", path.display()));
    assert!(bytes.len() > 64, "a frame to damage");
    bytes[60] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("damage the frame");

    let node = Node::at(dir);
    let opened = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    );
    match opened {
        Err(e @ ApplyError::Disagreement { .. }) => {
            assert!(e.fatal(), "a disagreement stops this node");
        }
        Err(other) => panic!("expected the I11 disagreement, got {other:?}"),
        Ok(_) => panic!("a damaged frame below the recorded length must not be ignored"),
    }
}

#[test]
fn a_sealed_file_records_which_partitions_it_holds() {
    // §6.1's G0 amendment: `partition_files` is written once per SEAL, not
    // once per append, and it is what a read path needs to know which sealed
    // files can answer for a partition.
    let node = Node::new("seals");
    {
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            cfg(),
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(1),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                }])
                .at(1, 1),
        )
        .expect("apply");
        for n in 0..24u64 {
            a.apply(
                &Build::new(BASE_US + 10 + n as i64, 2, 100 + n * 10)
                    .cmd(vec![Effect::Append {
                        pid: 1,
                        bucket: 2,
                        base_offset: n,
                        count: 1,
                        created_at_us: BASE_US + 10 + n as i64,
                        hashes: hashes(900 + n, 1),
                        blob: vec![3; 700],
                    }])
                    .at(2 + n, 1),
            )
            .expect("apply");
        }
        a.flush().expect("flush");
        let sealed: Vec<u32> = a
            .segments_mut()
            .files()
            .into_iter()
            .filter(|(b, _, m)| *b == 2 && m.sealed)
            .map(|(_, id, _)| id)
            .collect();
        assert!(!sealed.is_empty(), "the appends must have rolled a file");

        let recorded: Vec<u32> = node
            .store()
            .read(|r| {
                let mut v = Vec::new();
                r.scan_partition_files(1, 64, &mut |f| {
                    v.push(f);
                    true
                })?;
                Ok(v)
            })
            .expect("read");
        for id in &sealed {
            assert!(
                recorded.contains(id),
                "sealed file {id} is not in partition_files ({recorded:?})"
            );
        }
        // The ACTIVE file is not recorded: its index is in RAM and its rows
        // are written when it seals.
        let active = a.segments_mut().active_file(2).expect("an active file");
        assert!(!recorded.contains(&active));
    }
}

// ---------------------------------------------------------------------------
// File GC (§11.7) and what it leaves on disk (I10, I11)
// ---------------------------------------------------------------------------

/// A partition, `appends` frames of `bytes`, and the retention that kills them.
///
/// Returns the applier's stats after the GC that follows, so a caller can
/// assert files actually went.
fn retention_cycle<S: Store>(a: &mut Applier<'_, S>, bucket: u16, appends: u64, bytes: usize) {
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![Effect::PartitionCreate {
                pid: 1,
                uuid: uuid(1),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: BASE_US,
            }])
            .at(1, 1),
    )
    .expect("apply");
    let mut index = 2u64;
    for n in 0..appends {
        a.apply(
            &Build::new(BASE_US + 10 + n as i64, 2, 100 + n * 10)
                .cmd(vec![Effect::Append {
                    pid: 1,
                    bucket,
                    base_offset: n,
                    count: 1,
                    created_at_us: BASE_US + 10 + n as i64,
                    hashes: hashes(500 + n, 1),
                    blob: vec![9; bytes],
                }])
                .at(index, 1),
        )
        .expect("apply");
        index += 1;
    }
    a.durable_point().expect("durable point");
    // Retention, then the txns purge: both watermarks past the last frame, so
    // every sealed file is dead (§11.7).
    a.apply(
        &Build::new(BASE_US + 1_000_000, 2, 900_000)
            .cmd(vec![Effect::Watermark {
                pid: 1,
                log_start: appends,
                txns_start: appends,
            }])
            .at(index, 1),
    )
    .expect("apply");
}

#[test]
fn a_collected_file_leaves_a_node_that_reopens() {
    // I10 and I11 together, in the order §11.7 prescribes: `gc_pass` takes the
    // dead file's rows out, and the durable point that follows unlinks its
    // bytes because no committed transaction names it any more.
    //
    // The first cut of this work package failed exactly here. `release` marks
    // a file TOUCHED, the durable point drains the touched set into the very
    // commit that is supposed to stop naming the file, and the row came back
    // — after which the same call unlinked the file. The next open answered
    // `Disagreement { segment b001/f0 is missing }`: a node that cannot boot
    // after an ordinary retention cycle, and in raft3 a state directory
    // discarded and a snapshot installed over routine retention (§11.5).
    let dir = tmp_dir("collected");
    let mut node = Node::at(dir.clone());
    let unlinked = {
        let (mut a, _) = open_at(&node);
        retention_cycle(&mut a, 1, 24, 700);
        a.gc_pass().expect("gc");
        a.durable_point().expect("durable point");
        let n = a.stats().files_unlinked;
        assert!(n > 0, "the retention cycle must have collected a file");
        // Nothing this node holds is claimed twice (§11.7's figures feed
        // compaction).
        assert_eq!(a.segments_mut().saturated_releases(), 0);
        n
    };
    node.close();

    // The whole of the check: the node comes back.
    let node = Node::at(dir);
    let (mut a, rec) = open_at(&node);
    assert!(
        rec.segments.deleted.is_empty(),
        "the store and the disk agreed, so recovery had nothing to delete: {:?}",
        rec.segments.deleted
    );
    assert!(rec.segments.truncated.is_empty());

    // And the two agree in the other direction too: every row names a file
    // that exists, and the collected ones left no rows behind.
    let rows = node
        .store()
        .read(|r| {
            let mut files = Vec::new();
            r.scan_files(usize::MAX, &mut |b, id, _row| {
                files.push((b, id));
                true
            })?;
            let mut pf = Vec::new();
            r.scan_partition_files(1, usize::MAX, &mut |id| {
                pf.push(id);
                true
            })?;
            Ok((files, pf))
        })
        .expect("read");
    for (b, id) in &rows.0 {
        assert!(
            a.reader().has_file(*b, *id),
            "the store names b{b:03}/f{id}, which is not on disk",
        );
    }
    // The EXACT surviving set, both ways. A count is not enough: "fewer rows
    // than files plus unlinks" is satisfied while every collected file has
    // left every one of its rows behind, which is the leak this asserts
    // against.
    let orphans: Vec<u32> = rows
        .1
        .iter()
        .copied()
        .filter(|id| !rows.0.iter().any(|(_, f)| f == id))
        .collect();
    assert!(
        orphans.is_empty(),
        "partition_files rows of pid 1 name files that are gone: {orphans:?}",
    );
    let mut owed = 0usize;
    for (b, id) in &rows.0 {
        let sealed = a.segments_mut().file_meta(*b, *id).expect("meta").sealed;
        if sealed
            && a.segments_mut()
                .pids_in(*b, *id)
                .expect("index")
                .contains(&1)
        {
            owed += 1;
            assert!(
                rows.1.contains(id),
                "a sealed file that holds pid 1 has no partition_files row: b{b:03}/f{id}",
            );
        }
    }
    assert_eq!(
        rows.1.len(),
        owed,
        "the rows that survived are exactly the sealed files that survived: {:?}",
        rows.1
    );
    assert!(unlinked > 0);
}

#[test]
fn a_file_collected_after_a_restart_takes_its_partition_files_rows_with_it() {
    // I8: "durable points cost proportional to change". The rows that say
    // which sealed files hold a partition's data were deleted through a RAM
    // map that `Applier::open` rebuilds EMPTY, so every file that sealed
    // before a restart and was collected after it kept its rows — one per
    // partition, per file, for ever, naming a file that is not on disk. They
    // come from the file's own index now, which a restart does not forget.
    let dir = tmp_dir("pf-restart");
    let mut node = Node::at(dir.clone());
    {
        let (mut a, _) = open_at(&node);
        retention_cycle(&mut a, 4, 24, 700);
        a.flush().expect("flush");
        let recorded = node
            .store()
            .read(|r| {
                let mut v = Vec::new();
                r.scan_partition_files(1, usize::MAX, &mut |id| {
                    v.push(id);
                    true
                })?;
                Ok(v)
            })
            .expect("read");
        assert!(
            !recorded.is_empty(),
            "the sealed files must have been recorded first"
        );
    }
    node.close();

    // A NEW process: nothing in RAM remembers which partitions those files
    // hold.
    let node = Node::at(dir);
    {
        let (mut a, _) = open_at(&node);
        settle(&mut a);
        assert!(
            a.stats().files_unlinked > 0,
            "the dead files must be collected after the restart"
        );
    }
    let left = node
        .store()
        .read(|r| {
            let mut v = Vec::new();
            r.scan_partition_files(1, usize::MAX, &mut |id| {
                v.push(id);
                true
            })?;
            Ok(v)
        })
        .expect("read");
    assert!(
        left.is_empty(),
        "partition_files rows survived the files they name: {left:?}"
    );
}

// ---------------------------------------------------------------------------
// Counters on the DELETE paths (§6.4, D16)
// ---------------------------------------------------------------------------

/// Every gauge of §6.4, recomputed from the rows that are left.
struct Recount {
    queue_dlq: i64,
    tenant_dlq: i64,
    queue_retained: i64,
    pending: BTreeMap<String, i64>,
}

fn recount(node: &Node) -> Recount {
    node.store()
        .read(|r| {
            let mut pids = Vec::new();
            r.scan_queue_partitions(TENANT, QUEUE, None, usize::MAX, &mut |p| {
                pids.push(p);
                true
            })?;
            let mut groups: Vec<String> = Vec::new();
            r.scan_groups(TENANT, QUEUE, usize::MAX, &mut |g, _row| {
                groups.push(g.to_string());
                true
            })?;
            let mut out = Recount {
                queue_dlq: 0,
                tenant_dlq: 0,
                queue_retained: 0,
                pending: BTreeMap::new(),
            };
            // Dead letters: the ROWS, counted per partition that still exists.
            r.scan_raw(Keyspace::Dlq, &[], &[], usize::MAX, &mut |_k, v| {
                if let Ok(row) = crate::rsm::store::rows::dlq_decode(v) {
                    if pids.contains(&row.pid) {
                        out.queue_dlq += 1;
                    }
                    out.tenant_dlq += 1;
                }
                true
            })?;
            for pid in &pids {
                let p = r.partition(*pid).unwrap().expect("partition row");
                r.scan_seg_loc(*pid, p.log_start, usize::MAX, &mut |_b, row| {
                    out.queue_retained += row.len as i64;
                    true
                })?;
                for g in &groups {
                    let committed = r
                        .cursor(*pid, g)
                        .unwrap()
                        .map(|c| c.committed)
                        .unwrap_or(-1);
                    *out.pending.entry(g.clone()).or_default() += p.last_offset - committed;
                }
            }
            Ok(out)
        })
        .expect("read")
}

fn group_counter(node: &Node, group: &str, c: Counter) -> i64 {
    node.store()
        .read(|r| r.counter_at(&keys::counter_group(TENANT, QUEUE, group, c)))
        .expect("read")
}

fn group_counter_rows(node: &Node, group: &str) -> usize {
    node.store()
        .read(|r| {
            let mut prefix = Vec::new();
            prefix.push(keys::CounterScope::Group as u8);
            keys::push_name(&mut prefix, TENANT);
            keys::push_name(&mut prefix, QUEUE);
            keys::push_name(&mut prefix, group);
            let mut n = 0;
            r.scan_raw(
                Keyspace::Counters,
                &prefix,
                &prefix,
                usize::MAX,
                &mut |_k, _v| {
                    n += 1;
                    true
                },
            )?;
            Ok(n)
        })
        .expect("read")
}

#[test]
fn the_gauges_settle_when_groups_partitions_and_queues_are_deleted() {
    // D16 says the counters ARE the answer: there is no aggregation to fall
    // back on, so a gauge that is not settled on a delete path is wrong for
    // ever. The recount below is over the rows that survive each delete.
    //
    // What this catches, and the WP's own `counters_equal_a_recount` could
    // not (its workload emits no delete at all): a consumer-group delete took
    // the group's dead letters without touching `dlq_count` and left its
    // `pending`/`completed` rows for the next group of the same name; a
    // partition delete took its dead letters the same way and left its share
    // of every group's `pending` behind; a queue delete settled the tenant's
    // retained bytes and not its dead letters.
    let node = Node::new("gauges");
    let (mut a, _) = open_at(&node);

    // Two partitions, two groups, four dead letters (three for g1, one for g2).
    let mut effects = vec![
        Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            cfg: queue_config(BASE_US),
        },
        Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            group: "g1".into(),
            meta: group_meta(1, BASE_US),
        },
        Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            group: "g2".into(),
            meta: group_meta(2, BASE_US),
        },
    ];
    for pid in 1..=2u64 {
        effects.push(Effect::PartitionCreate {
            pid,
            uuid: uuid(pid),
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            partition: format!("p{pid}"),
            created_at_us: BASE_US,
        });
    }
    a.apply(&Build::new(BASE_US, 1, 0).cmd(effects).at(1, 1))
        .expect("apply");

    let mut index = 2u64;
    let mut ids = 1_000u64;
    let mut at = |a: &mut Applier<'_, HeedStore>, effects: Vec<Effect>| {
        a.apply(
            &Build::new(BASE_US + index as i64 * 1_000, 3, ids)
                .cmd(effects)
                .at(index, 1),
        )
        .expect("apply");
        index += 1;
        ids += 10;
    };
    for pid in 1..=2u64 {
        at(
            &mut a,
            vec![Effect::Append {
                pid,
                bucket: (pid % 8) as u16,
                base_offset: 0,
                count: 4,
                created_at_us: BASE_US + 100 + pid as i64,
                hashes: hashes(pid * 31, 4),
                blob: vec![7; 128],
            }],
        );
    }
    // g1 acked two of pid 1; g2 acked one of pid 2.
    at(
        &mut a,
        vec![Effect::CursorSet {
            pid: 1,
            group: "g1".into(),
            row: fresh_cursor(1, BASE_US + 200),
        }],
    );
    at(
        &mut a,
        vec![Effect::CursorSet {
            pid: 2,
            group: "g2".into(),
            row: fresh_cursor(0, BASE_US + 200),
        }],
    );
    let dead = [(1u64, "g1", 0i64), (1, "g1", 1), (2, "g1", 2), (2, "g2", 3)];
    for (n, (pid, group, offset)) in dead.iter().enumerate() {
        at(
            &mut a,
            vec![Effect::DlqInsert {
                dlq_id: uuid(7_000 + n as u64),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                pid: *pid,
                group: (*group).into(),
                offset: *offset,
                message_id: Some(uuid(8_000 + n as u64)),
                txn: format!("txn-{n}"),
                payload: b"{}".to_vec(),
                error: "boom".into(),
                retry_count: 1,
                failed_at_us: BASE_US + 300,
            }],
        );
    }
    a.commit().expect("commit");

    let before = recount(&node);
    assert_eq!(before.queue_dlq, 4);
    assert_eq!(before.pending["g1"], (4 - 2) + 4, "g1 is behind on both");
    assert_eq!(before.pending["g2"], 4 + (4 - 1));
    let assert_settled = |node: &Node, what: &str| {
        let r = recount(node);
        let got = node
            .store()
            .read(|s| {
                Ok((
                    s.queue_counter(TENANT, QUEUE, Counter::DlqCount)?,
                    s.tenant_counter(TENANT, Counter::DlqCount)?,
                    s.queue_counter(TENANT, QUEUE, Counter::RetainedBytes)?,
                    s.tenant_counter(TENANT, Counter::RetainedBytes)?,
                ))
            })
            .expect("read");
        assert_eq!(got.0, r.queue_dlq, "{what}: queue dlq_count");
        assert_eq!(got.1, r.tenant_dlq, "{what}: tenant dlq_count");
        assert_eq!(got.2, r.queue_retained, "{what}: queue retained bytes");
        assert_eq!(got.3, r.queue_retained, "{what}: tenant retained bytes");
        for (g, want) in &r.pending {
            assert_eq!(
                group_counter(node, g, Counter::Pending),
                *want,
                "{what}: {g} pending"
            );
        }
    };
    assert_settled(&node, "before any delete");

    // -- a consumer group (014) ------------------------------------------
    at(
        &mut a,
        vec![
            Effect::GroupDelete {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                group: "g2".into(),
            },
            Effect::GarbageAdd {
                pids: vec![1, 2],
                scope: GarbageScope::Group { group: "g2".into() },
                deleted_at_us: BASE_US + 400,
            },
        ],
    );
    for _ in 0..8 {
        at(
            &mut a,
            vec![Effect::DeleteChunk {
                pids: vec![1, 2],
                scope: GarbageScope::Group { group: "g2".into() },
                resume: Vec::new(),
                limit: 4,
            }],
        );
    }
    a.commit().expect("commit");
    assert_eq!(
        group_counter_rows(&node, "g2"),
        0,
        "a deleted group's counters go with its name: a group recreated under \
         it must not inherit its lag"
    );
    assert_settled(&node, "after the group delete");

    // -- a partition, while the queue lives (006 cleanup) -----------------
    at(&mut a, vec![Effect::PartitionDelete { pid: 2 }]);
    a.commit().expect("commit");
    assert_settled(&node, "after the partition delete");
    assert_eq!(group_counter(&node, "g1", Counter::Pending), 4 - 2);

    // -- the queue -------------------------------------------------------
    at(
        &mut a,
        vec![
            Effect::QueueDelete {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
            },
            Effect::GarbageAdd {
                pids: vec![1],
                scope: GarbageScope::Queue,
                deleted_at_us: BASE_US + 900,
            },
        ],
    );
    for _ in 0..12 {
        at(
            &mut a,
            vec![Effect::DeleteChunk {
                pids: vec![1],
                scope: GarbageScope::Queue,
                resume: Vec::new(),
                limit: 8,
            }],
        );
    }
    a.commit().expect("commit");
    let left = node
        .store()
        .read(|r| {
            Ok((
                r.tenant_counter(TENANT, Counter::DlqCount)?,
                r.tenant_counter(TENANT, Counter::RetainedBytes)?,
                r.count(Keyspace::Garbage)?,
            ))
        })
        .expect("read");
    assert_eq!(left.2, 0, "the chunks finished");
    assert_eq!(left.0, 0, "the tenant's dead letters went with its queue");
    assert_eq!(left.1, 0, "and so did its retained bytes");
    assert_eq!(a.segments_mut().saturated_releases(), 0);
}

#[test]
fn a_recreated_queue_name_does_not_inherit_the_dead_queue_s_gauges() {
    // §5.2 ratifies that a deleted queue's NAME is reusable at once — "a push,
    // configure or pop right after the delete recreates it", which is what
    // Kafka's DeleteTopics→CreateTopics and the client suites do — while the
    // pid-keyed rows of the queue that had it are deleted in chunks for as
    // long as that takes. The chunks used to ask whether the queue was still
    // there BY NAME, so the dead queue's dead letters were subtracted from the
    // live queue that had taken it: with D16 there is no aggregation to
    // correct the gauge, so both the queue's and the tenant's `dlq_count` went
    // negative and stayed there.
    let node = Node::new("recreate");
    let (mut a, _) = open_at(&node);

    // Incarnation one: queue id A, one partition, two dead letters.
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![
                Effect::QueueUpsert {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    cfg: QueueConfig {
                        id: uuid(1),
                        ..queue_config(BASE_US)
                    },
                },
                Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(11),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                },
            ])
            .at(1, 1),
    )
    .expect("apply");
    a.apply(
        &Build::new(BASE_US + 10, 2, 100)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 3,
                base_offset: 0,
                count: 2,
                created_at_us: BASE_US + 10,
                hashes: hashes(5, 2),
                blob: vec![1; 128],
            }])
            .at(2, 1),
    )
    .expect("apply");
    for n in 0..2u64 {
        a.apply(
            &Build::new(BASE_US + 20 + n as i64, 2, 200 + n * 10)
                .cmd(vec![Effect::DlqInsert {
                    dlq_id: uuid(500 + n),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    pid: 1,
                    group: "g1".into(),
                    offset: n as i64,
                    message_id: Some(uuid(600 + n)),
                    txn: format!("old-{n}"),
                    payload: b"{}".to_vec(),
                    error: "boom".into(),
                    retry_count: 1,
                    failed_at_us: BASE_US + 20,
                }])
                .at(3 + n, 1),
        )
        .expect("apply");
    }

    // The delete: the name-keyed rows go at once, the pid-keyed ones behind a
    // `GarbageAdd` (§5.2). The tenant's gauges are settled HERE, from the
    // queue's own counters.
    a.apply(
        &Build::new(BASE_US + 100, 2, 300)
            .cmd(vec![
                Effect::QueueDelete {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                },
                Effect::GarbageAdd {
                    pids: vec![1],
                    scope: GarbageScope::Queue,
                    deleted_at_us: BASE_US + 100,
                },
            ])
            .at(5, 1),
    )
    .expect("apply");
    a.commit().expect("commit");
    let after_delete = node
        .store()
        .read(|r| {
            Ok((
                r.tenant_counter(TENANT, Counter::DlqCount)?,
                r.tenant_counter(TENANT, Counter::RetainedBytes)?,
            ))
        })
        .expect("read");
    assert_eq!(
        after_delete,
        (0, 0),
        "the delete settles the tenant from the queue it took away"
    );

    // A push recreates the NAME while the chunks are still outstanding: a new
    // queue row with a new id, a new partition, its own dead letter.
    a.apply(
        &Build::new(BASE_US + 200, 2, 400)
            .cmd(vec![
                Effect::QueueUpsert {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    cfg: QueueConfig {
                        id: uuid(2),
                        ..queue_config(BASE_US + 200)
                    },
                },
                Effect::PartitionCreate {
                    pid: 2,
                    uuid: uuid(12),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US + 200,
                },
            ])
            .at(6, 1),
    )
    .expect("apply");
    a.apply(
        &Build::new(BASE_US + 210, 3, 500)
            .cmd(vec![
                Effect::Append {
                    pid: 2,
                    bucket: 4,
                    base_offset: 0,
                    count: 1,
                    created_at_us: BASE_US + 210,
                    hashes: hashes(9, 1),
                    blob: vec![2; 64],
                },
                Effect::DlqInsert {
                    dlq_id: uuid(700),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    pid: 2,
                    group: "g1".into(),
                    offset: 0,
                    message_id: Some(uuid(701)),
                    txn: "new-0".into(),
                    payload: b"{}".to_vec(),
                    error: "boom".into(),
                    retry_count: 1,
                    failed_at_us: BASE_US + 210,
                },
            ])
            .at(7, 1),
    )
    .expect("apply");
    a.commit().expect("commit");
    let fresh_retained = node_counters(&node).2;
    assert_eq!(node_counters(&node), (1, 1, fresh_retained));
    assert!(fresh_retained > 0, "the new queue holds bytes of its own");

    // And only now do the dead queue's chunks land.
    let mut index = 8u64;
    for _ in 0..12 {
        a.apply(
            &Build::new(BASE_US + 300 + index as i64, 3, 600 + index * 10)
                .cmd(vec![Effect::DeleteChunk {
                    pids: vec![1],
                    scope: GarbageScope::Queue,
                    resume: Vec::new(),
                    limit: 4,
                }])
                .at(index, 1),
        )
        .expect("apply");
        index += 1;
        a.commit().expect("commit");
        // Never, at any step, may a gauge of the LIVE queue move because of
        // the dead one.
        assert_eq!(
            node_counters(&node),
            (1, 1, fresh_retained),
            "a chunk of the deleted queue moved the recreated queue's gauges"
        );
    }
    a.commit().expect("commit");
    let left = node
        .store()
        .read(|r| r.count(Keyspace::Garbage))
        .expect("read");
    assert_eq!(left, 0, "the chunks finished");
    assert_eq!(
        node_counters(&node),
        (1, 1, fresh_retained),
        "the recreated queue keeps exactly its own dead letter and bytes"
    );
    // The dead queue's own rows are gone, all of them.
    let rows = node
        .store()
        .read(|r| Ok((r.count(Keyspace::Dlq)?, r.count(Keyspace::DlqByPos)?)))
        .expect("read");
    assert_eq!(rows, (1, 1), "only the live queue's dead letter is left");
}

#[test]
fn a_group_chunk_in_flight_does_not_drop_a_queue_delete_s_marker() {
    // `GarbageAdd` overwrites: a queue delete that lands while a consumer
    // group's chunks are still outstanding replaces their marker with its own,
    // wider one. The group chunk that arrives next used to delete that marker
    // as if it were its own — after which every `DeleteChunk` of the queue
    // delete finds no garbage row and does nothing, and the partition, its
    // `seg_loc` rows, its dedup and its counters stay on the node for ever
    // (§5.2: the pid-keyed data goes in chunks, behind the marker).
    let node = Node::new("marker");
    let (mut a, _) = open_at(&node);
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![
                Effect::QueueUpsert {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    cfg: queue_config(BASE_US),
                },
                Effect::GroupUpsert {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    group: "g2".into(),
                    meta: group_meta(2, BASE_US),
                },
                Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(1),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                },
            ])
            .at(1, 1),
    )
    .expect("apply");
    a.apply(
        &Build::new(BASE_US + 10, 2, 100)
            .cmd(vec![
                Effect::Append {
                    pid: 1,
                    bucket: 6,
                    base_offset: 0,
                    count: 2,
                    created_at_us: BASE_US + 10,
                    hashes: hashes(31, 2),
                    blob: vec![3; 96],
                },
                Effect::CursorSet {
                    pid: 1,
                    group: "g2".into(),
                    row: fresh_cursor(0, BASE_US + 10),
                },
            ])
            .at(2, 1),
    )
    .expect("apply");

    // The group delete opens its garbage, and the queue delete lands before
    // the group's chunks have run.
    a.apply(
        &Build::new(BASE_US + 20, 2, 200)
            .cmd(vec![
                Effect::GroupDelete {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    group: "g2".into(),
                },
                Effect::GarbageAdd {
                    pids: vec![1],
                    scope: GarbageScope::Group { group: "g2".into() },
                    deleted_at_us: BASE_US + 20,
                },
            ])
            .at(3, 1),
    )
    .expect("apply");
    a.apply(
        &Build::new(BASE_US + 30, 2, 300)
            .cmd(vec![
                Effect::QueueDelete {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                },
                Effect::GarbageAdd {
                    pids: vec![1],
                    scope: GarbageScope::Queue,
                    deleted_at_us: BASE_US + 30,
                },
            ])
            .at(4, 1),
    )
    .expect("apply");

    // The group loop's chunk, proposed before the queue delete.
    a.apply(
        &Build::new(BASE_US + 40, 2, 400)
            .cmd(vec![Effect::DeleteChunk {
                pids: vec![1],
                scope: GarbageScope::Group { group: "g2".into() },
                resume: Vec::new(),
                limit: 8,
            }])
            .at(5, 1),
    )
    .expect("apply");
    a.commit().expect("commit");
    assert!(
        node.store()
            .read(|r| Ok(r.garbage(1)?.is_some()))
            .expect("read"),
        "the queue delete's marker is not the group chunk's to remove",
    );

    // And the queue delete finishes, taking everything pid-keyed with it.
    let mut index = 6u64;
    for _ in 0..12 {
        a.apply(
            &Build::new(BASE_US + 50 + index as i64, 2, 500 + index * 10)
                .cmd(vec![Effect::DeleteChunk {
                    pids: vec![1],
                    scope: GarbageScope::Queue,
                    resume: Vec::new(),
                    limit: 8,
                }])
                .at(index, 1),
        )
        .expect("apply");
        index += 1;
    }
    a.commit().expect("commit");
    let left = node
        .store()
        .read(|r| {
            let mut seg = 0usize;
            r.scan_seg_loc(1, 0, usize::MAX, &mut |_b, _row| {
                seg += 1;
                true
            })?;
            Ok((r.partition(1)?.is_some(), seg, r.count(Keyspace::Garbage)?))
        })
        .expect("read");
    assert_eq!(
        left,
        (false, 0, 0),
        "(partition row, seg_loc rows, garbage rows) after the queue delete finished",
    );
}

/// `(queue dlq_count, tenant dlq_count, queue retained bytes)` — the gauges a
/// name reuse can corrupt.
fn node_counters(node: &Node) -> (i64, i64, i64) {
    node.store()
        .read(|r| {
            Ok((
                r.queue_counter(TENANT, QUEUE, Counter::DlqCount)?,
                r.tenant_counter(TENANT, Counter::DlqCount)?,
                r.queue_counter(TENANT, QUEUE, Counter::RetainedBytes)?,
            ))
        })
        .expect("read")
}

// ---------------------------------------------------------------------------
// What a GC pass costs, and what it may not block (I8, I10, §11.7)
// ---------------------------------------------------------------------------

/// A partition of its own in a bucket of its own, filled until it has rolled
/// at least one sealed file. Returns the next entry index.
fn fill_a_bucket<S: Store>(
    a: &mut Applier<'_, S>,
    pid: Pid,
    bucket: u16,
    appends: u64,
    mut index: u64,
) -> u64 {
    a.apply(
        &Build::new(BASE_US + index as i64 * 1_000, pid, index * 100)
            .cmd(vec![Effect::PartitionCreate {
                pid,
                uuid: uuid(pid),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: format!("p{pid}"),
                created_at_us: BASE_US + index as i64 * 1_000,
            }])
            .at(index, 1),
    )
    .expect("apply");
    index += 1;
    let mut effects = Vec::new();
    for n in 0..appends {
        effects.push(Effect::Append {
            pid,
            bucket,
            base_offset: n,
            count: 1,
            created_at_us: BASE_US + index as i64 * 1_000 + n as i64,
            hashes: hashes(pid * 977 + n, 1),
            blob: vec![9; 700],
        });
    }
    a.apply(
        &Build::new(BASE_US + index as i64 * 1_000, pid + 1, index * 100)
            .cmd(effects)
            .at(index, 1),
    )
    .expect("apply");
    index + 1
}

/// Retention and the txns purge past everything one partition holds: its
/// sealed files die, its bucket's active file does not.
fn kill_a_partition<S: Store>(
    a: &mut Applier<'_, S>,
    pid: Pid,
    appends: u64,
    next_pid: u64,
    index: u64,
) -> u64 {
    a.apply(
        &Build::new(BASE_US + index as i64 * 1_000, next_pid, index * 100)
            .cmd(vec![Effect::Watermark {
                pid,
                log_start: appends,
                txns_start: appends,
            }])
            .at(index, 1),
    )
    .expect("apply");
    index + 1
}

#[test]
fn finding_files_to_collect_costs_the_dead_files_not_the_files_held() {
    // I8: "no periodic work proportional to stored data". `gc_pass` runs after
    // every applied entry and on every 2 ms idle tick, and the first cut
    // answered it by locking the pins, walking the WHOLE file table and
    // allocating a vector of every dead file, before throwing all but
    // `gc_per_pass` of them away. That cost grows with retained bytes /
    // `QUEEN_RAFT_SEGMENT_BYTES` — it is the shape §13.6's flatness test
    // measures, paid once per entry.
    let node = Node::new("gc-cost");
    let (mut a, _) = open_at(&node);
    let mut index = 1u64;
    let mut pid = 1u64;
    // Eight buckets, each with a live partition: sealed files nothing wants to
    // collect, which the walk used to pay for on every turn.
    for bucket in 1..=8u16 {
        index = fill_a_bucket(&mut a, pid, bucket, 24, index);
        pid += 1;
    }
    let held = a.segments_mut().files().len();
    assert!(
        held >= 24,
        "the fixture must hold enough files for the difference to show: {held}"
    );

    // An idle node with all those files pays NOTHING per turn.
    let before = a.segments_mut().gc_examined();
    for _ in 0..20 {
        a.gc_pass().expect("gc");
    }
    let idle = a.segments_mut().gc_examined() - before;
    assert_eq!(
        idle, 0,
        "20 turns over {held} files, none of them dead, examined {idle} entries"
    );

    // Now one partition's files die. The pass pays for THEM, not for the table.
    index = kill_a_partition(&mut a, 1, 24, pid, index);
    let _ = index;
    let dead = a
        .segments_mut()
        .files()
        .iter()
        .filter(|(_, _, m)| m.is_dead())
        .count();
    assert!(dead > 0, "the watermark must have killed a file");
    let before = a.segments_mut().gc_examined();
    a.gc_pass().expect("gc");
    let spent = a.segments_mut().gc_examined() - before;
    assert!(
        spent <= dead as u64,
        "one pass examined {spent} file table entries for {dead} dead files out of {held}"
    );
}

#[test]
fn a_durable_point_costs_the_files_that_grew_not_the_files_held() {
    // §11.4: "Cost is proportional to what changed". A durable point advances
    // every file's durable length — the floor recovery verifies from (§11.5
    // step 3) — and the first cut found the files to advance by walking the
    // whole file table, once a second, for the life of the process. That is
    // the same shape as the GC walk above, at a different cadence.
    let node = Node::new("point-cost");
    let (mut a, _) = open_at(&node);
    let mut index = 1u64;
    let mut pid = 1u64;
    for bucket in 1..=6u16 {
        index = fill_a_bucket(&mut a, pid, bucket, 16, index);
        pid += 1;
    }
    a.durable_point().expect("durable point");
    let held = a.segments_mut().files().len();
    assert!(held >= 18, "{held}");

    // One more frame, in one bucket. The point that follows advances ONE file.
    let before = a.segments_mut().durable_examined();
    a.apply(
        &Build::new(BASE_US + index as i64 * 1_000, pid, index * 100)
            .cmd(vec![Effect::Append {
                pid: 1,
                bucket: 1,
                base_offset: 16,
                count: 1,
                created_at_us: BASE_US + index as i64 * 1_000,
                hashes: hashes(1, 1),
                blob: vec![4; 700],
            }])
            .at(index, 1),
    )
    .expect("apply");
    a.durable_point().expect("durable point");
    assert_eq!(
        a.segments_mut().durable_examined() - before,
        1,
        "one frame was written and the point examined more than one file of {held}",
    );

    // And a point with nothing written at all costs nothing.
    let before = a.segments_mut().durable_examined();
    a.apply(
        &Build::new(BASE_US + (index + 1) as i64 * 1_000, pid, (index + 1) * 100)
            .cmd(vec![Effect::Noop])
            .at(index + 1, 1),
    )
    .expect("apply");
    a.durable_point().expect("durable point");
    assert_eq!(a.segments_mut().durable_examined() - before, 0);
}

#[test]
fn a_verified_tail_gets_the_barrier_it_never_had() {
    // §11.5 step 3 verifies the frames between a file's durable length and its
    // recorded length — bytes a plain store commit recorded and NO fsync has
    // ever covered (§11.3). Recovery accepted them and the next durable point
    // called them durable, without a barrier of its own: a process crash
    // followed by a power loss inside the writeback window would lose exactly
    // those bytes, and the boot after it would not re-verify them, because the
    // row would say they had been durable all along.
    let dir = tmp_dir("tail-barrier");
    let mut node = Node::at(dir.clone());
    {
        let (mut a, rec) = open_at(&node);
        assert_eq!(rec.segments.synced, 0, "a fresh node owes no barrier");
        let mut index = fill_a_bucket(&mut a, 1, 6, 24, 1);
        a.durable_point().expect("durable point");
        // More frames, then an ordinary NON-durable store commit: the rows now
        // record bytes above the durable length.
        index = fill_a_bucket(&mut a, 2, 7, 24, index);
        let _ = index;
        a.commit().expect("commit");
        let gap = a
            .segments_mut()
            .files()
            .into_iter()
            .filter(|(_, _, m)| m.durable_bytes < m.bytes)
            .count();
        assert!(gap > 0, "the fixture must leave unbarriered bytes");
    }
    node.close();

    // The reopen verifies that tail (step 3) and gives it the barrier it never
    // had (step 7).
    let mut node = Node::at(dir.clone());
    {
        let (mut a, rec) = open_at(&node);
        assert!(
            rec.segments.synced > 0,
            "the frames recovery verified were never fsynced: {rec:?}",
        );
        assert!(!rec.segments.verified.is_empty() || !rec.segments.rescanned.is_empty());
        // Recording them is what stops the next boot from doing it again.
        a.commit().expect("commit");
    }
    node.close();

    // And the boot after that has nothing to verify and nothing to sync: the
    // cost followed the change and did not repeat it (I8).
    let node = Node::at(dir);
    let (_a, rec) = open_at(&node);
    assert_eq!(
        (rec.segments.synced, rec.segments.verified.len()),
        (0, 0),
        "the same frames were verified and synced twice: {rec:?}",
    );
}

#[test]
fn a_pinned_file_does_not_fill_the_pass_and_stop_every_other_file() {
    // §11.7 with I4 on top: a staged file whose unlink a claim pin refuses is
    // re-staged at every durable point. Counting those against `gc_per_pass`
    // meant that `gc_per_pass` long-held claims stopped file GC for the WHOLE
    // node — the disk fills while retention says it has run.
    let dir = tmp_dir("gc-block");
    let mut node = Node::at(dir.clone());
    let (mut a, _) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        ApplyConfig {
            // Two, so the test is small; the shape is the same at 32.
            gc_per_pass: 2,
            ..cfg()
        },
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("open");

    let mut index = 1u64;
    for (pid, bucket) in [(1u64, 1u16), (2, 2), (3, 3)] {
        index = fill_a_bucket(&mut a, pid, bucket, 12, index);
    }
    // Two partitions die: exactly `gc_per_pass` files to stage.
    index = kill_a_partition(&mut a, 1, 12, 4, index);
    index = kill_a_partition(&mut a, 2, 12, 4, index);
    a.gc_pass().expect("gc");

    // Their claims are read only now — a committed pop whose payloads the
    // receiver has not fetched yet (I4). The unlink must wait.
    let doomed: Vec<(u16, u32)> = a
        .segments_mut()
        .files()
        .into_iter()
        .filter(|(_, _, m)| m.is_dead())
        .map(|(b, id, _)| (b, id))
        .collect();
    assert_eq!(doomed.len(), 2, "{doomed:?}");
    let reader = a.reader();
    let pins: Vec<_> = doomed
        .iter()
        .map(|(b, id)| reader.pin(*b, *id).expect("a pin for a staged file"))
        .collect();
    a.durable_point().expect("durable point");
    assert_eq!(a.stats().files_unlinked, 0, "both unlinks were deferred");
    assert_eq!(a.stats().gc_deferred, 2);

    // A third partition dies while those two pins are still held.
    let _ = kill_a_partition(&mut a, 3, 12, 4, index);
    let third: Vec<(u16, u32)> = a
        .segments_mut()
        .files()
        .into_iter()
        .filter(|(b, id, m)| m.is_dead() && !doomed.contains(&(*b, *id)))
        .map(|(b, id, _)| (b, id))
        .collect();
    assert_eq!(third.len(), 1, "{third:?}");
    a.gc_pass().expect("gc");
    a.durable_point().expect("durable point");
    assert!(
        a.segments_mut().file_meta(third[0].0, third[0].1).is_none(),
        "a file nothing holds waited behind two pinned ones",
    );
    assert_eq!(a.stats().files_unlinked, 1);
    for (b, id) in &doomed {
        assert!(
            a.segments_mut().file_meta(*b, *id).is_some(),
            "and the pinned ones are still here, as I4 says",
        );
    }

    // When the claims are read, they go too — and the node reopens.
    drop(pins);
    drop(reader);
    settle(&mut a);
    for (b, id) in &doomed {
        assert!(a.segments_mut().file_meta(*b, *id).is_none());
    }
    assert_eq!(a.segments_mut().saturated_releases(), 0);
    let local = node.local_digest();
    drop(a);
    node.close();
    let node = Node::at(dir);
    let (_a, rec) = open_at(&node);
    assert!(
        rec.segments.deleted.is_empty() && rec.segments.truncated.is_empty(),
        "recovery had to reconcile nothing: {rec:?}"
    );
    assert_eq!(
        node.local_digest().whole,
        local.whole,
        "the file table came back exactly as the GC left it"
    );
}

// ---------------------------------------------------------------------------
// A refusal in the middle of an entry
// ---------------------------------------------------------------------------

#[test]
fn an_entry_that_fails_half_way_stops_this_applier_and_lands_nothing() {
    // Apply is not a transaction: the effects before the one that refused are
    // already in the open store transaction, in the segment file and in the
    // derived RAM, and there is no undo. So the applier refuses everything
    // afterwards and the open transaction is never committed — the entry's
    // prefix reaches the disk only if the node applies the entry AGAIN, which
    // would give it two frames at one `(pid, base_offset)`, two sets of dedup
    // occurrences and double counters. Recovery replays from the last durable
    // point instead (§11.5).
    let dir = tmp_dir("poison");
    let mut node = Node::at(dir.clone());
    {
        // PERF-C: pin the INLINE write path, the mechanism this test documents
        // and asserts — the refused entry's prefix reaches the segment FILE and
        // recovery truncates it. With buffering the prefix is buffered and
        // never written, so there is nothing to truncate; that path is covered
        // by `a_buffered_half_entry_leaves_nothing_on_the_disk` and by the
        // mode-independent end-state asserts below.
        let inline = ApplyConfig {
            seg_buffered: false,
            ..cfg()
        };
        let (mut a, _) = Applier::open(
            node.store(),
            &node.seg_dir(),
            seg_opts(),
            inline,
            Arc::new(crate::rsm::apply::NoNotify),
        )
        .expect("open");
        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![
                    Effect::PartitionCreate {
                        pid: 1,
                        uuid: uuid(1),
                        tenant: TENANT.into(),
                        queue: QUEUE.into(),
                        partition: "p0".into(),
                        created_at_us: BASE_US,
                    },
                    Effect::Append {
                        pid: 1,
                        bucket: 5,
                        base_offset: 0,
                        count: 1,
                        created_at_us: BASE_US,
                        hashes: hashes(1, 1),
                        blob: vec![1; 64],
                    },
                ])
                .at(1, 1),
        )
        .expect("apply");
        a.durable_point().expect("durable point");

        // One command, two effects: the first lands, the second names a
        // partition that does not exist.
        let err = a
            .apply(
                &Build::new(BASE_US + 10, 2, 100)
                    .cmd(vec![
                        Effect::Append {
                            pid: 1,
                            bucket: 5,
                            base_offset: 1,
                            count: 1,
                            created_at_us: BASE_US + 10,
                            hashes: hashes(2, 1),
                            blob: vec![2; 64],
                        },
                        Effect::Append {
                            pid: 99,
                            bucket: 5,
                            base_offset: 0,
                            count: 1,
                            created_at_us: BASE_US + 10,
                            hashes: hashes(3, 1),
                            blob: vec![3; 64],
                        },
                    ])
                    .at(2, 1),
            )
            .expect_err("the second effect must refuse");
        assert!(matches!(err, ApplyError::Inconsistent { .. }), "{err}");
        assert!(err.fatal(), "every apply refusal is fatal: {err}");

        // Nothing else happens on this applier — including a re-delivery of
        // the entry, which is what a caller reading `fatal()` as "retryable"
        // would do.
        for e in [
            a.apply(
                &Build::new(BASE_US + 20, 2, 200)
                    .cmd(vec![Effect::Noop])
                    .at(3, 1),
            )
            .map(|_| ())
            .expect_err("poisoned"),
            a.commit().expect_err("poisoned"),
            a.durable_point().map(|_| ()).expect_err("poisoned"),
            a.flush().expect_err("poisoned"),
            a.gc_pass().expect_err("poisoned"),
        ] {
            assert!(matches!(e, ApplyError::Poisoned { .. }), "{e}");
        }
    }
    node.close();

    // The state on disk is the one before the failed entry: its prefix was
    // never committed, and the bytes it appended are truncated away by the
    // recorded length (I11).
    let node = Node::at(dir);
    let (_a, rec) = open_at(&node);
    assert_eq!(
        rec.applied_index, 1,
        "the failed entry left no applied index"
    );
    let p = node
        .store()
        .read(|r| Ok(r.partition(1)?.expect("partition row")))
        .expect("read");
    assert_eq!(
        p.last_offset, 0,
        "the half entry's append is not in the state"
    );
    let seg_rows = node
        .store()
        .read(|r| {
            let mut n = 0;
            r.scan_seg_loc(1, 0, usize::MAX, &mut |_b, _row| {
                n += 1;
                true
            })?;
            Ok(n)
        })
        .expect("read");
    assert_eq!(seg_rows, 1, "one frame, not two");
    assert!(
        rec.segments.truncated.len() == 1,
        "the bytes the failed entry appended are truncated away: {:?}",
        rec.segments.truncated
    );
}

// ---------------------------------------------------------------------------
// PERF-C: write coalescing and the write pool
// ---------------------------------------------------------------------------

#[test]
fn a_buffered_entry_coalesces_writes_per_file_and_reads_back() {
    // One entry that appends four frames across three buckets costs ONE `write`
    // per touched file (PERF-C), not one per message, and every frame is
    // readable through the position the store recorded — bytes and offsets are
    // identical to the inline path. Run with the write pool on, so the pooled
    // write plus the apply-thread record publish is exercised, and reopened, so
    // the coalesced bytes are shown durable.
    let dir = tmp_dir("perfc-coalesce");
    {
        let mut node = Node::at(dir.clone());
        let with_pool = ApplyConfig {
            seg_buffered: true,
            apply_writers: 2,
            ..cfg()
        };
        let blobs: Vec<Vec<u8>> = (0..4u8).map(|i| vec![i + 1; 40]).collect();
        {
            let (mut a, _) = Applier::open(
                node.store(),
                &node.seg_dir(),
                seg_opts(),
                with_pool,
                Arc::new(crate::rsm::apply::NoNotify),
            )
            .expect("open");

            a.apply(
                &Build::new(BASE_US, 1, 0)
                    .cmd(vec![
                        Effect::QueueUpsert {
                            tenant: TENANT.into(),
                            queue: QUEUE.into(),
                            cfg: queue_config(BASE_US),
                        },
                        Effect::PartitionCreate {
                            pid: 1,
                            uuid: uuid(1),
                            tenant: TENANT.into(),
                            queue: QUEUE.into(),
                            partition: "p1".into(),
                            created_at_us: BASE_US,
                        },
                        Effect::PartitionCreate {
                            pid: 2,
                            uuid: uuid(2),
                            tenant: TENANT.into(),
                            queue: QUEUE.into(),
                            partition: "p2".into(),
                            created_at_us: BASE_US,
                        },
                        Effect::PartitionCreate {
                            pid: 3,
                            uuid: uuid(3),
                            tenant: TENANT.into(),
                            queue: QUEUE.into(),
                            partition: "p3".into(),
                            created_at_us: BASE_US,
                        },
                    ])
                    .at(1, 1),
            )
            .expect("setup");
            let writes_before = a.segments().segment_writes();

            // Two appends to bucket 1, one each to buckets 2 and 3 — one entry.
            // pid_base is 4: entry 1 assigned pids 1..=3, so `next_pid` is 4.
            a.apply(
                &Build::new(BASE_US + 10, 4, 10)
                    .cmd(vec![
                        Effect::Append {
                            pid: 1,
                            bucket: 1,
                            base_offset: 0,
                            count: 1,
                            created_at_us: BASE_US + 10,
                            hashes: hashes(10, 1),
                            blob: blobs[0].clone(),
                        },
                        Effect::Append {
                            pid: 1,
                            bucket: 1,
                            base_offset: 1,
                            count: 1,
                            created_at_us: BASE_US + 11,
                            hashes: hashes(11, 1),
                            blob: blobs[1].clone(),
                        },
                        Effect::Append {
                            pid: 2,
                            bucket: 2,
                            base_offset: 0,
                            count: 1,
                            created_at_us: BASE_US + 12,
                            hashes: hashes(12, 1),
                            blob: blobs[2].clone(),
                        },
                        Effect::Append {
                            pid: 3,
                            bucket: 3,
                            base_offset: 0,
                            count: 1,
                            created_at_us: BASE_US + 13,
                            hashes: hashes(13, 1),
                            blob: blobs[3].clone(),
                        },
                    ])
                    .at(2, 1),
            )
            .expect("append");

            let writes = a.segments().segment_writes() - writes_before;
            assert_eq!(
                writes, 3,
                "one write per touched file (3), not one per message (4)"
            );

            a.commit().expect("commit");

            // Every frame reads back through the position the store recorded.
            for (pid, off, blob) in [
                (1u64, 0u64, &blobs[0]),
                (1, 1, &blobs[1]),
                (2, 0, &blobs[2]),
                (3, 0, &blobs[3]),
            ] {
                let pos = node
                    .store()
                    .read(|r| r.seg_loc(pid, off))
                    .expect("read")
                    .expect("a seg_loc row");
                let frame = a
                    .reader()
                    .read(segments::Position {
                        bucket: pos.bucket,
                        file_id: pos.file_id,
                        offset: pos.offset,
                        len: pos.len,
                    })
                    .expect("read frame");
                assert_eq!(&frame.blob, blob, "pid {pid} off {off} reads back");
            }
            a.durable_point().expect("durable point");
        }
        node.keep();
        node.close();
    }

    // Reopen: the coalesced bytes are on the platter, nothing is truncated, and
    // the tails are whole.
    let node = Node::at(dir);
    let (a, rec) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("reopen");
    assert_eq!(rec.applied_index, 2, "both entries are durable");
    assert!(
        rec.segments.truncated.is_empty(),
        "the coalesced writes were on the fd before the point recorded them: {:?}",
        rec.segments.truncated
    );
    for (pid, last) in [(1u64, 1i64), (2, 0), (3, 0)] {
        let p = node
            .store()
            .read(|r| Ok(r.partition(pid)?.expect("partition")))
            .expect("read");
        assert_eq!(p.last_offset, last, "pid {pid} tail survives the reopen");
    }
    // A frame still reads back after recovery.
    let pos = node
        .store()
        .read(|r| r.seg_loc(1, 1))
        .expect("read")
        .expect("seg_loc");
    let frame = a
        .reader()
        .read(segments::Position {
            bucket: pos.bucket,
            file_id: pos.file_id,
            offset: pos.offset,
            len: pos.len,
        })
        .expect("read frame after reopen");
    assert_eq!(frame.blob, vec![2u8; 40]);
}

#[test]
fn a_buffered_half_entry_leaves_nothing_on_the_disk() {
    // The buffered counterpart of the poison test: a refused entry's valid
    // prefix is BUFFERED, so the failure (before the end-of-entry flush) means
    // its bytes never reach the fd at all — nothing is written, nothing is
    // truncated, and the committed state is the one before the entry.
    let dir = tmp_dir("perfc-halfentry");
    {
        let mut node = Node::at(dir.clone());
        {
            // Buffering on, pool off: the prefix is held in RAM and discarded.
            let (mut a, _) = Applier::open(
                node.store(),
                &node.seg_dir(),
                seg_opts(),
                cfg(),
                Arc::new(crate::rsm::apply::NoNotify),
            )
            .expect("open");
            a.apply(
                &Build::new(BASE_US, 1, 0)
                    .cmd(vec![
                        Effect::PartitionCreate {
                            pid: 1,
                            uuid: uuid(1),
                            tenant: TENANT.into(),
                            queue: QUEUE.into(),
                            partition: "p0".into(),
                            created_at_us: BASE_US,
                        },
                        Effect::Append {
                            pid: 1,
                            bucket: 5,
                            base_offset: 0,
                            count: 1,
                            created_at_us: BASE_US,
                            hashes: hashes(1, 1),
                            blob: vec![1; 64],
                        },
                    ])
                    .at(1, 1),
            )
            .expect("apply");
            a.durable_point().expect("durable point");

            let err = a
                .apply(
                    &Build::new(BASE_US + 10, 2, 100)
                        .cmd(vec![
                            Effect::Append {
                                pid: 1,
                                bucket: 5,
                                base_offset: 1,
                                count: 1,
                                created_at_us: BASE_US + 10,
                                hashes: hashes(2, 1),
                                blob: vec![2; 64],
                            },
                            Effect::Append {
                                pid: 99,
                                bucket: 5,
                                base_offset: 0,
                                count: 1,
                                created_at_us: BASE_US + 10,
                                hashes: hashes(3, 1),
                                blob: vec![3; 64],
                            },
                        ])
                        .at(2, 1),
                )
                .expect_err("the second effect must refuse");
            assert!(matches!(err, ApplyError::Inconsistent { .. }), "{err}");
        }
        node.keep();
        node.close();
    }

    let node = Node::at(dir);
    let (_a, rec) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    )
    .expect("reopen");
    assert_eq!(
        rec.applied_index, 1,
        "the failed entry left no applied index"
    );
    assert!(
        rec.segments.truncated.is_empty(),
        "the buffered prefix never reached the fd, so there is nothing to truncate: {:?}",
        rec.segments.truncated
    );
    let p = node
        .store()
        .read(|r| Ok(r.partition(1)?.expect("partition")))
        .expect("read");
    assert_eq!(
        p.last_offset, 0,
        "the half entry's append is not in the state"
    );
}

#[test]
fn a_frame_damaged_above_the_last_durable_point_refuses_to_start() {
    // §11.5 step 3: "verify the checksums of every frame the state references
    // past the last durable point". A SEALED file was exempt from that: its
    // `.qidx` is written at the seal and normally opens fine, and recovery
    // scanned only when the index did NOT open. A file sealed after the last
    // durable point therefore had its frames accepted unverified — the exact
    // frames no barrier has covered.
    let dir = tmp_dir("damaged-tail");
    let mut node = Node::at(dir.clone());
    let (bucket, file_id, from, to) = {
        let (mut a, _) = open_at(&node);
        a.apply(
            &Build::new(BASE_US, 1, 0)
                .cmd(vec![Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(1),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                }])
                .at(1, 1),
        )
        .expect("apply");
        let mut index = 2u64;
        let mut append = |a: &mut Applier<'_, HeedStore>, n: u64| {
            a.apply(
                &Build::new(BASE_US + 10 + n as i64, 2, 100 + n * 10)
                    .cmd(vec![Effect::Append {
                        pid: 1,
                        bucket: 6,
                        base_offset: n,
                        count: 1,
                        created_at_us: BASE_US + 10 + n as i64,
                        hashes: hashes(500 + n, 1),
                        blob: vec![9; 700],
                    }])
                    .at(index, 1),
            )
            .expect("apply");
            index += 1;
        };
        // A durable point with the file part written, then enough appends to
        // SEAL it, then an ordinary (non-durable) store commit: the file now
        // has frames above its durable length and a valid `.qidx`.
        for n in 0..3 {
            append(&mut a, n);
        }
        a.durable_point().expect("durable point");
        for n in 3..24 {
            append(&mut a, n);
        }
        a.commit().expect("commit");
        let sealed: Vec<(u16, u32, u64, u64)> = a
            .segments_mut()
            .files()
            .into_iter()
            .filter(|(_, _, m)| m.sealed && m.durable_bytes < m.bytes)
            .map(|(b, id, m)| (b, id, m.durable_bytes, m.bytes))
            .collect();
        assert!(
            !sealed.is_empty(),
            "a file must have sealed above the durable point"
        );
        sealed[0]
    };
    node.close();

    // Healthy first: the reopen VERIFIES that tail — the frames between the
    // file's durable length and its recorded length — and accepts it.
    {
        let mut node = Node::at(dir.clone());
        node.keep();
        let (_a, rec) = open_at(&node);
        let verified = rec
            .segments
            .verified
            .iter()
            .find(|(b, id, _, _)| (*b, *id) == (bucket, file_id))
            .copied();
        assert_eq!(
            verified,
            Some((bucket, file_id, from, to)),
            "the tail above the durable point is the range §11.5 step 3 checks: {:?}",
            rec.segments.verified
        );
    }

    let path = node
        .seg_dir()
        .join(format!("b{bucket:03}"))
        .join(format!("f{file_id:010}.seg"));
    let mut bytes = std::fs::read(&path).expect("the segment file");
    let at = (from + 40) as usize;
    assert!(
        at < to as usize,
        "a frame above the durable point to damage"
    );
    bytes[at] ^= 0xFF;
    std::fs::write(&path, &bytes).expect("damage the frame");

    let node = Node::at(dir);
    let opened = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg(),
        Arc::new(crate::rsm::apply::NoNotify),
    );
    match opened {
        Err(e @ ApplyError::Disagreement { .. }) => assert!(e.fatal(), "{e}"),
        Err(other) => panic!("expected the I11 disagreement, got {other:?}"),
        Ok(_) => panic!(
            "a frame damaged between b{bucket:03}/f{file_id}'s durable length \
             {from} and its recorded length {to} was accepted"
        ),
    }
}

#[test]
fn a_delete_releases_each_frame_claim_exactly_once() {
    // §11.7 counts two claims per frame: retention retires the payload, the
    // txns purge retires the hash list, and only the second lets the file die
    // (D10). A partition delete retired BOTH for every row it found, including
    // rows whose payload retention had already released — `Segments::release`
    // saturates, so the loss was silent and the file table ended up recording
    // FEWER retained frames and bytes than the file really holds. Liveness
    // survived it (the window count is exact); §11.7's compaction, which
    // chooses files by exactly those figures, would not.
    let node = Node::new("claims");
    let (mut a, _) = open_at(&node);
    let mut effects = Vec::new();
    for pid in 1..=2u64 {
        effects.push(Effect::PartitionCreate {
            pid,
            uuid: uuid(pid),
            tenant: TENANT.into(),
            queue: QUEUE.into(),
            partition: format!("p{pid}"),
            created_at_us: BASE_US,
        });
    }
    a.apply(&Build::new(BASE_US, 1, 0).cmd(effects).at(1, 1))
        .expect("apply");

    // Both partitions write into ONE file, so what one of them loses is
    // visible in what the file still claims for the other.
    let mut index = 2u64;
    for pid in 1..=2u64 {
        for n in 0..4u64 {
            a.apply(
                &Build::new(BASE_US + index as i64, 3, index * 10)
                    .cmd(vec![Effect::Append {
                        pid,
                        bucket: 7,
                        base_offset: n,
                        count: 1,
                        created_at_us: BASE_US + index as i64,
                        hashes: hashes(pid * 77 + n, 1),
                        blob: vec![4; 64],
                    }])
                    .at(index, 1),
            )
            .expect("apply");
            index += 1;
        }
    }
    let file = a.segments_mut().active_file(7).expect("one active file");
    let before = a.segments_mut().file_meta(7, file).expect("file meta");
    assert_eq!(before.retained_frames, 8);
    assert_eq!(before.window_frames, 8);

    // Retention takes two of partition 1's frames; their `seg_loc` rows stay,
    // because the hash lists are still inside the txns window (D10).
    a.apply(
        &Build::new(BASE_US + 500, 3, 5_000)
            .cmd(vec![Effect::Watermark {
                pid: 1,
                log_start: 2,
                txns_start: 0,
            }])
            .at(index, 1),
    )
    .expect("apply");
    index += 1;
    let mid = a.segments_mut().file_meta(7, file).expect("file meta");
    assert_eq!(mid.retained_frames, 6, "two payloads went, six are live");
    assert_eq!(mid.window_frames, 8, "no hash list has expired yet");

    // Now the partition goes. It must retire what it still holds: two
    // payloads and four hash lists.
    a.apply(
        &Build::new(BASE_US + 600, 3, 6_000)
            .cmd(vec![Effect::PartitionDelete { pid: 1 }])
            .at(index, 1),
    )
    .expect("apply");
    let after = a.segments_mut().file_meta(7, file).expect("file meta");
    assert_eq!(
        a.segments_mut().saturated_releases(),
        0,
        "a claim was released twice"
    );
    assert_eq!(
        after.retained_frames, 4,
        "partition 2's four frames are still retained"
    );
    assert_eq!(
        after.retained_bytes,
        mid.retained_bytes - 2 * (before.retained_bytes / 8)
    );
    assert_eq!(after.window_frames, 4);
}

#[test]
fn two_dead_letters_at_one_position_both_go_with_their_partition() {
    // `log_dlq`'s index on `(partition_id, consumer_group, "offset")` is NOT
    // unique in postgres (005): a message replayed out of the DLQ that dies
    // again is filed at the same position twice, and both rows exist. The
    // `dlq_by_pos` index held ONE id per position, so the older row became
    // reachable by nothing — every delete path walks that index — and it
    // outlived its partition, its queue and its tenant, with `dlq_count`
    // counting it for ever.
    let node = Node::new("dlq-collision");
    let (mut a, _) = open_at(&node);
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![
                Effect::QueueUpsert {
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    cfg: queue_config(BASE_US),
                },
                Effect::PartitionCreate {
                    pid: 1,
                    uuid: uuid(1),
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    partition: "p0".into(),
                    created_at_us: BASE_US,
                },
                Effect::Append {
                    pid: 1,
                    bucket: 2,
                    base_offset: 0,
                    count: 4,
                    created_at_us: BASE_US,
                    hashes: hashes(11, 4),
                    blob: vec![6; 64],
                },
            ])
            .at(1, 1),
    )
    .expect("apply");
    for (n, id) in [uuid(4_001), uuid(4_002)].iter().enumerate() {
        a.apply(
            &Build::new(BASE_US + 10 + n as i64, 2, 500 + n as u64 * 10)
                .cmd(vec![Effect::DlqInsert {
                    dlq_id: *id,
                    tenant: TENANT.into(),
                    queue: QUEUE.into(),
                    pid: 1,
                    group: "g1".into(),
                    offset: 2,
                    message_id: Some(uuid(6_000 + n as u64)),
                    txn: format!("txn-{n}"),
                    payload: b"{}".to_vec(),
                    error: "again".into(),
                    retry_count: 1,
                    failed_at_us: BASE_US + 10,
                }])
                .at(2 + n as u64, 1),
        )
        .expect("apply");
    }
    a.commit().expect("commit");
    let counted = node
        .store()
        .read(|r| {
            Ok((
                r.count(Keyspace::Dlq)?,
                r.queue_counter(TENANT, QUEUE, Counter::DlqCount)?,
            ))
        })
        .expect("read");
    assert_eq!(counted, (2, 2), "two rows at one position, both counted");

    a.apply(
        &Build::new(BASE_US + 100, 2, 700)
            .cmd(vec![Effect::PartitionDelete { pid: 1 }])
            .at(4, 1),
    )
    .expect("apply");
    a.commit().expect("commit");
    let left = node
        .store()
        .read(|r| {
            Ok((
                r.count(Keyspace::Dlq)?,
                r.count(Keyspace::DlqByPos)?,
                r.queue_counter(TENANT, QUEUE, Counter::DlqCount)?,
                r.tenant_counter(TENANT, Counter::DlqCount)?,
            ))
        })
        .expect("read");
    assert_eq!(
        left,
        (0, 0, 0, 0),
        "both rows, both index entries, both counts"
    );
}

#[test]
fn a_file_that_dies_before_its_seal_is_recorded_leaves_no_rows() {
    // The window between a seal and the commit that records it (§6.1's G0
    // amendment writes `partition_files` once per seal, not per append).
    // Retention can empty a file INSIDE that window — one entry rolls the file
    // and the next moves both watermarks past everything in it — and then the
    // same commit that GC phase one wrote its deletions into would ALSO write
    // the seal's rows, for a file the durable point is about to unlink. The
    // store would come back naming files that are not there, which is the I11
    // disagreement (and the shape of the blocker this WP was refuted on).
    let dir = tmp_dir("dead-before-seal");
    let mut node = Node::at(dir.clone());
    {
        let (mut a, _) = open_at(&node);
        // No durable point and no commit anywhere in here: the seals stay
        // unrecorded until after the retention below.
        retention_cycle_uncommitted(&mut a, 3, 24, 700);
        a.gc_pass().expect("gc");
        a.commit().expect("commit");
        a.durable_point().expect("durable point");
        assert!(
            a.stats().files_unlinked > 0,
            "the dead files must have been collected"
        );
    }
    let rows = node
        .store()
        .read(|r| {
            let mut v = Vec::new();
            r.scan_partition_files(1, usize::MAX, &mut |id| {
                v.push(id);
                true
            })?;
            let mut files = Vec::new();
            r.scan_files(usize::MAX, &mut |b, id, _row| {
                files.push((b, id));
                true
            })?;
            Ok((v, files))
        })
        .expect("read");
    assert!(
        rows.0.is_empty(),
        "partition_files rows were written for files that were collected: {:?}",
        rows.0
    );
    node.close();
    let node = Node::at(dir);
    let (_a, rec) = open_at(&node);
    assert!(
        rec.segments.deleted.is_empty(),
        "the store and the disk agreed: {:?}",
        rec.segments.deleted
    );
}

/// [`retention_cycle`] without the durable point between the appends and the
/// watermark, so the seals are still unrecorded when the files die.
fn retention_cycle_uncommitted<S: Store>(
    a: &mut Applier<'_, S>,
    bucket: u16,
    appends: u64,
    bytes: usize,
) {
    a.apply(
        &Build::new(BASE_US, 1, 0)
            .cmd(vec![Effect::PartitionCreate {
                pid: 1,
                uuid: uuid(1),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: BASE_US,
            }])
            .at(1, 1),
    )
    .expect("apply");
    let mut index = 2u64;
    for n in 0..appends {
        a.apply(
            &Build::new(BASE_US + 10 + n as i64, 2, 100 + n * 10)
                .cmd(vec![Effect::Append {
                    pid: 1,
                    bucket,
                    base_offset: n,
                    count: 1,
                    created_at_us: BASE_US + 10 + n as i64,
                    hashes: hashes(500 + n, 1),
                    blob: vec![9; bytes],
                }])
                .at(index, 1),
        )
        .expect("apply");
        index += 1;
    }
    a.apply(
        &Build::new(BASE_US + 1_000_000, 2, 900_000)
            .cmd(vec![Effect::Watermark {
                pid: 1,
                log_start: appends,
                txns_start: appends,
            }])
            .at(index, 1),
    )
    .expect("apply");
}
