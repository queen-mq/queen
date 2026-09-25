//! Phase C — the per-queue logs as the ONLY write-ahead log.
//!
//! - **the layout** ([`every_entry_lands_in_exactly_the_logs_it_touches`]): a
//!   real `LocalReplicator` writes every entry's payload-free entry record into
//!   exactly the queue logs its effects touch (appends to two queues → both; a
//!   pop / an ack on one queue → that queue only; a `Noop` → the system log),
//!   with `copies` = the number of those logs, the payload records beside them,
//!   and NOTHING in the raft log;
//! - **the merge** ([`scan_entries_merges_dedups_orders_and_skips_a_torn_tail`]):
//!   `QLogSet::scan_entries` merges the logs by seq, delivers each entry once,
//!   in order, across rolled (sealed) files, and a torn tail is dropped by the
//!   reopen exactly as for payload records;
//! - **the cut** ([`scan_stops_at_an_incomplete_entry_and_truncation_drops_the_tail`],
//!   [`scan_stops_at_a_gap`], [`two_disagreeing_copies_are_refused`]): recovery
//!   delivers only the gapless prefix of COMPLETE entries; `truncate_from`
//!   removes the unacknowledged tail (entries AND payload records) durably, so
//!   its seqs can be reused;
//! - **the floor** ([`unlink_never_drops_a_file_above_the_recovery_floor`]);
//! - **the proof** ([`the_queue_logs_alone_recover_acked_work`],
//!   [`the_queue_logs_alone_recover_acked_work_pipelined`]): pushes, pops and
//!   acks across two queues, the store rolled back to an OLDER durable
//!   checkpoint, and a reopen that replays from the queue logs ALONE (the raft
//!   log holds nothing) reaches the live state byte-for-byte; the writer then
//!   continues at the next index and routes to partitions created inside the
//!   replay window;
//! - **the cut, end to end** ([`an_incomplete_tail_is_cut_and_its_seqs_are_reused`]):
//!   an injected incomplete group (a partial entry + a stale payload record +
//!   an orphan later entry) is discarded at reopen, its seq is reused by a new
//!   entry, and a replay from an older checkpoint reads the NEW entry.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;

use super::set::{EntryLayout, EntryScan, QLogSet, SYSTEM_QUEUE_ID};
use super::{
    EntryInput, EntryPart, EntryRecord, EntryStubInput, QLogOptions, RecordInput, WriteRecord,
};
use crate::rsm::apply::{self, state_digest, StateDigest, SystemClock};
use crate::rsm::effect::{Effect, GroupMeta, Kind};
use crate::rsm::entry::{encode_entry, encode_entry_payload_free, Entry, Outcome};
use crate::rsm::replicator::local::{LocalReplicator, NoWaker, OpenConfig};
use crate::rsm::replicator::log::{Fsync as LogFsync, LogOptions, LogStore};
use crate::rsm::replicator::{AppliedAt, Replicator};
use crate::rsm::segments;
use crate::rsm::store::{HeedStore, Store, StoreOpts, TypedReads};
use crate::rsm::tests::samples;

/// A directory that removes itself, named after its test.
struct TmpDir(PathBuf);

impl TmpDir {
    fn new(tag: &str) -> TmpDir {
        use std::sync::atomic::{AtomicU64, Ordering};
        static N: AtomicU64 = AtomicU64::new(0);
        let p = std::env::temp_dir().join(format!(
            "queen-rsm-qlog-wal-{tag}-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&p);
        std::fs::create_dir_all(&p).expect("temp dir");
        TmpDir(p)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TmpDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

// ---------------------------------------------------------------------------
// QLogSet-level: merge, dedup, order, torn tail, cut, floor
// ---------------------------------------------------------------------------

const Q1: u64 = 11;
const Q2: u64 = 22;

/// A recognisable entry body for `seq`.
fn eb(seq: u64, tag: u8) -> Vec<u8> {
    let mut v = vec![tag; 40 + (seq as usize % 7)];
    v[0] = seq as u8;
    v
}

fn ent(seq: u64, copies: u32, bytes: &[u8]) -> WriteRecord<'_> {
    WriteRecord::Entry(EntryInput {
        seq,
        now_us: 1_000 + seq as i64,
        copies,
        term: 0,
        entry: bytes,
    })
}

fn msg<'a>(seq: u64, pid: u64, base: u64, hashes: &'a [u8], payload: &'a [u8]) -> WriteRecord<'a> {
    WriteRecord::Msg(RecordInput {
        seq,
        pid,
        base_offset: base,
        count: 1,
        created_at_us: 1_000 + seq as i64,
        txn: None,
        hashes,
        payload,
    })
}

fn scan_all(set: &QLogSet, from: u64) -> (Vec<EntryRecord>, EntryScan) {
    let mut got = Vec::new();
    let out = set
        .scan_entries(from, &mut |r| {
            got.push(r);
            Ok(())
        })
        .expect("scan_entries");
    (got, out)
}

fn reopen(root: &Path, opts: QLogOptions) -> QLogSet {
    let mut set = QLogSet::new(root.to_path_buf(), opts);
    set.reopen_all().expect("reopen_all");
    set
}

/// The highest-numbered `.qlog` file of queue `qid` (its active file).
fn active_file(root: &Path, qid: u64) -> PathBuf {
    let dir = root.join(format!("q{qid}"));
    let mut best: Option<(u64, PathBuf)> = None;
    for e in std::fs::read_dir(&dir).expect("queue dir") {
        let p = e.expect("entry").path();
        let name = p
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("")
            .to_string();
        if let Some(id) = name
            .strip_prefix('r')
            .and_then(|r| r.strip_suffix(".qlog"))
            .and_then(|n| n.parse::<u64>().ok())
        {
            if best.as_ref().is_none_or(|(b, _)| id > *b) {
                best = Some((id, p));
            }
        }
    }
    best.expect("an active file").1
}

#[test]
fn scan_entries_merges_dedups_orders_and_skips_a_torn_tail() {
    let td = TmpDir::new("merge");
    let root = td.path().join("qlog");
    // Tiny files: every couple of records rolls, so the entries span sealed
    // files too.
    let opts = QLogOptions::testing(150);
    let h = [0x11u8; 16];
    let bodies: Vec<Vec<u8>> = (0..=6).map(|s| eb(s, 0xE0)).collect();
    {
        let mut set = QLogSet::new(root.clone(), opts);
        // Group 1: seq 1 → q1 + q2 (2 copies), seq 2 → system (1 copy).
        set.write_mixed_for_qid(Q1, &[ent(1, 2, &bodies[1])])
            .unwrap();
        set.write_mixed_for_qid(Q2, &[ent(1, 2, &bodies[1])])
            .unwrap();
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(2, 1, &bodies[2])])
            .unwrap();
        set.sync().unwrap();
        // Group 2: seq 3 → q1 with a payload record first; seq 4 → q2 + system.
        set.write_mixed_for_qid(
            Q1,
            &[msg(3, 7, 0, &h, b"payload-three"), ent(3, 1, &bodies[3])],
        )
        .unwrap();
        set.write_mixed_for_qid(Q2, &[ent(4, 2, &bodies[4])])
            .unwrap();
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(4, 2, &bodies[4])])
            .unwrap();
        set.sync().unwrap();
        // Group 3: seq 5 → all three logs, seq 6 → q2.
        set.write_mixed_for_qid(Q1, &[ent(5, 3, &bodies[5])])
            .unwrap();
        set.write_mixed_for_qid(Q2, &[ent(5, 3, &bodies[5]), ent(6, 1, &bodies[6])])
            .unwrap();
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(5, 3, &bodies[5])])
            .unwrap();
        set.sync().unwrap();
        let q2 = set.log(Q2).unwrap();
        assert!(
            q2.read().unwrap().file_count() > 1,
            "the tiny segment size rolled q2"
        );
    }
    // A torn tail on q2's active file: the first half of a seq-7 entry record
    // (the group that was writing when the process died).
    {
        let mut torn = Vec::new();
        super::record::encode_entry_copies_into(&mut torn, 7, 1_007, 1, &eb(7, 0xE0));
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(active_file(&root, Q2))
            .unwrap();
        f.write_all(&torn[..torn.len() / 2]).unwrap();
    }

    let set = reopen(&root, opts);
    let (got, out) = scan_all(&set, 1);
    let seqs: Vec<u64> = got.iter().map(|r| r.seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5, 6], "merged, deduped, ordered");
    let copies: Vec<u32> = got.iter().map(|r| r.copies).collect();
    assert_eq!(copies, vec![2, 1, 1, 2, 3, 1]);
    for r in &got {
        assert_eq!(r.entry, bodies[r.seq as usize], "entry {} bytes", r.seq);
        assert_eq!(r.now_us, 1_000 + r.seq as i64);
    }
    assert_eq!(
        out,
        EntryScan {
            delivered: 6,
            next_seq: 7,
            max_seq_found: 6,
            discarded: 0,
            stopped: None,
        },
        "the torn seq-7 half-record was truncated by the reopen, not delivered"
    );

    // A later start delivers only the suffix.
    let (got, out) = scan_all(&set, 4);
    assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![4, 5, 6]);
    assert_eq!(out.next_seq, 7);

    // The payload record beside the entries is indexed and readable; the entry
    // records are not indexed (no pid lookup ever lands on one).
    let reader = set.reader();
    let rec = reader
        .read_owned(Q1, 7, 0)
        .unwrap()
        .expect("payload record");
    assert_eq!(
        (rec.seq, rec.payload.as_slice()),
        (3, &b"payload-three"[..])
    );
    for pid in [0u64, 1, 2, 3] {
        assert!(reader.read_owned(Q2, pid, 0).unwrap().is_none());
        assert!(reader
            .read_owned(SYSTEM_QUEUE_ID, pid, 0)
            .unwrap()
            .is_none());
    }
    // The totals count every open log's files and bytes.
    let (files, bytes) = set.totals();
    assert!(
        files >= 4 && bytes > 0,
        "totals {files} files / {bytes} bytes"
    );
}

#[test]
fn scan_stops_at_an_incomplete_entry_and_truncation_drops_the_tail() {
    let td = TmpDir::new("cut");
    let root = td.path().join("qlog");
    let opts = QLogOptions::testing(4096);
    let h = [0x22u8; 16];
    let b1 = eb(1, 0xA1);
    let b2 = eb(2, 0xA2);
    let b3 = eb(3, 0xA3);
    let b4 = eb(4, 0xA4);
    {
        let mut set = QLogSet::new(root.clone(), opts);
        set.write_mixed_for_qid(Q1, &[ent(1, 2, &b1), ent(2, 1, &b2)])
            .unwrap();
        set.write_mixed_for_qid(Q2, &[ent(1, 2, &b1)]).unwrap();
        // The incomplete group: entry 3 was meant for q1 AND q2 (copies 2) but
        // only q1's write landed, with its payload record; entry 4 (system)
        // landed although it follows the incomplete one.
        set.write_mixed_for_qid(Q1, &[msg(3, 9, 0, &h, b"stale-payload"), ent(3, 2, &b3)])
            .unwrap();
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(4, 1, &b4)])
            .unwrap();
        set.sync().unwrap();
    }
    let mut set = reopen(&root, opts);
    let (got, out) = scan_all(&set, 1);
    assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(out.next_seq, 3);
    assert_eq!(out.discarded, 2, "entries 3 and 4 are the discarded tail");
    assert_eq!(out.max_seq_found, 4);
    assert!(
        out.stopped.as_deref().unwrap_or("").contains("incomplete"),
        "{:?}",
        out.stopped
    );

    // Truncate at the cut: entry 3, its payload record and entry 4 all go.
    let dropped = set.truncate_from(out.next_seq).unwrap();
    assert!(dropped > 0);
    assert!(set.reader().read_owned(Q1, 9, 0).unwrap().is_none());
    // Idempotent.
    assert_eq!(set.truncate_from(out.next_seq).unwrap(), 0);
    drop(set);

    // Durable: a reopen sees the prefix only, with nothing to stop on.
    let mut set = reopen(&root, opts);
    let (got, out) = scan_all(&set, 1);
    assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2]);
    assert_eq!(
        (out.next_seq, out.max_seq_found, out.stopped.clone()),
        (3, 2, None)
    );

    // Seq 3 is REUSED by a new entry (different bytes, different payload):
    // no conflict with the dropped copy, and the new payload is what reads.
    let b3_new = eb(3, 0xB3);
    set.write_mixed_for_qid(
        Q1,
        &[msg(3, 9, 0, &h, b"fresh-payload"), ent(3, 1, &b3_new)],
    )
    .unwrap();
    set.sync().unwrap();
    drop(set);
    let set = reopen(&root, opts);
    let (got, out) = scan_all(&set, 1);
    assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
    assert_eq!(got[2].entry, b3_new);
    assert_eq!(out.stopped, None);
    let rec = set.reader().read_owned(Q1, 9, 0).unwrap().expect("record");
    assert_eq!(rec.payload, b"fresh-payload");
}

#[test]
fn scan_stops_at_a_gap() {
    let td = TmpDir::new("gap");
    let root = td.path().join("qlog");
    let opts = QLogOptions::testing(4096);
    let (b1, b3, b4) = (eb(1, 1), eb(3, 3), eb(4, 4));
    {
        let mut set = QLogSet::new(root.clone(), opts);
        set.write_mixed_for_qid(Q1, &[ent(1, 1, &b1)]).unwrap();
        // Entry 2 (q2 only) never landed; 3 and 4 did.
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(3, 1, &b3), ent(4, 1, &b4)])
            .unwrap();
        set.sync().unwrap();
    }
    let mut set = reopen(&root, opts);
    let (got, out) = scan_all(&set, 1);
    assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1]);
    assert_eq!((out.next_seq, out.discarded), (2, 2));
    assert!(out.stopped.as_deref().unwrap_or("").contains("missing"));
    set.truncate_from(2).unwrap();
    let (_, out) = scan_all(&set, 1);
    assert_eq!((out.next_seq, out.max_seq_found), (2, 1));
    // A scan that starts past the gap sees nothing to stop on either.
    let (got, out) = scan_all(&set, 2);
    assert!(got.is_empty());
    assert_eq!(out.next_seq, 2);
}

#[test]
fn two_disagreeing_copies_are_refused() {
    let td = TmpDir::new("conflict");
    let root = td.path().join("qlog");
    let opts = QLogOptions::testing(4096);
    let (a, b) = (eb(1, 0xAA), eb(1, 0xBB));
    {
        let mut set = QLogSet::new(root.clone(), opts);
        set.write_mixed_for_qid(Q1, &[ent(1, 2, &a)]).unwrap();
        set.write_mixed_for_qid(Q2, &[ent(1, 2, &b)]).unwrap();
        set.sync().unwrap();
    }
    let set = reopen(&root, opts);
    let err = set
        .scan_entries(1, &mut |_| Ok(()))
        .expect_err("two copies of one seq that disagree must be refused");
    assert!(err.to_string().contains("disagree"), "{err}");
}

#[test]
fn unlink_never_drops_a_file_above_the_recovery_floor() {
    let td = TmpDir::new("floor");
    let root = td.path().join("qlog");
    let opts = QLogOptions::testing(200);
    let bodies: Vec<Vec<u8>> = (0..=16).map(|s| eb(s, 0xF0)).collect();
    let mut set = QLogSet::new(root.clone(), opts);
    for s in 1..=16u64 {
        set.write_mixed_for_qid(Q1, &[ent(s, 1, &bodies[s as usize])])
            .unwrap();
        set.sync().unwrap();
    }
    let log = set.log(Q1).unwrap();
    fn sealed_bounds(log: &std::sync::RwLock<super::QLog>) -> Vec<u64> {
        log.read()
            .unwrap()
            .files()
            .iter()
            .filter(|m| m.sealed)
            .map(|m| m.max_seq)
            .collect()
    }
    let before = sealed_bounds(&log);
    assert!(before.len() >= 3, "several sealed files: {before:?}");

    // A fresh set's floor is 0: retention may drop nothing, whatever it says.
    assert_eq!(set.recovery_floor(), 0);
    assert_eq!(log.write().unwrap().unlink_dead_files(|_| true).unwrap(), 0);

    // Raise it through a READER (what apply can reach): only the sealed files
    // wholly at or below it go.
    let reader = set.reader();
    let floor = before[1];
    reader.set_recovery_floor(floor);
    reader.set_recovery_floor(1); // monotone: a lower value is ignored
    assert_eq!(set.recovery_floor(), floor);
    assert_eq!(reader.recovery_floor(), floor);
    let dropped = log.write().unwrap().unlink_dead_files(|_| true).unwrap();
    assert_eq!(dropped, before.iter().filter(|b| **b <= floor).count());
    assert!(sealed_bounds(&log).iter().all(|b| *b > floor));
    drop(log);
    drop(set);

    // After a reopen the sealed files' bound comes from the next file's
    // first seq; the floor still holds, and every entry above it is still
    // recoverable.
    let set = reopen(&root, opts);
    let log = set.log(Q1).unwrap();
    assert_eq!(log.write().unwrap().unlink_dead_files(|_| true).unwrap(), 0);
    set.set_recovery_floor(10);
    log.write().unwrap().unlink_dead_files(|_| true).unwrap();
    assert!(sealed_bounds(&log).iter().all(|b| *b > 10));
    let (got, _) = scan_all(&set, 11);
    assert_eq!(
        got.iter().map(|r| r.seq).collect::<Vec<_>>(),
        (11..=16).collect::<Vec<_>>()
    );
}

// ---------------------------------------------------------------------------
// End to end: a real LocalReplicator over the queue logs
// ---------------------------------------------------------------------------

const TENANT: &str = samples::T0;
const QA: &str = "phase-c-a";
const QB: &str = "phase-c-b";
const GROUP: &str = "g1";
const BASE_US: i64 = 1_800_000_000_000_000;

/// A deterministic entry builder: the planner's bases (`pid_base`,
/// `kv_version_base`) and a monotone clock, like the batcher would stamp.
struct Gen {
    n: u64,
    next_pid: u64,
    rid: u64,
    last: HashMap<u64, i64>,
    acked: HashMap<u64, i64>,
}

impl Gen {
    fn new() -> Gen {
        Gen {
            n: 0,
            next_pid: 1,
            rid: 0,
            last: HashMap::new(),
            acked: HashMap::new(),
        }
    }

    fn begin(&mut self) -> Entry {
        self.n += 1;
        Entry::new(BASE_US + self.n as i64 * 1_000, self.next_pid, 1)
    }

    fn id(&mut self) -> [u8; 16] {
        self.rid += 1;
        let mut r = [0u8; 16];
        r[..8].copy_from_slice(&self.rid.to_le_bytes());
        r[8] = 0xC7;
        r
    }

    fn cmd(&mut self, e: &mut Entry, effects: Vec<Effect>) {
        let rid = self.id();
        e.add_command(rid, Outcome::Empty, effects)
            .expect("add command");
    }

    /// A queue (config + group) and its first partition.
    fn create_queue(&mut self, e: &mut Entry, queue: &str) -> u64 {
        let now = e.now_us;
        let mut cfg = samples::queue_config();
        cfg.id = self.id();
        let meta = match samples::effect_sample(Kind::GroupUpsert) {
            Effect::GroupUpsert { mut meta, .. } => {
                meta.id = self.id();
                meta.registered_at_us = now;
                meta.subscription_timestamp_us = now;
                meta
            }
            _ => unreachable!(),
        };
        let pid = self.new_partition_pid();
        let uuid = self.id();
        self.cmd(
            e,
            vec![
                Effect::QueueUpsert {
                    tenant: TENANT.into(),
                    queue: queue.into(),
                    cfg,
                },
                group_upsert(queue, meta),
                Effect::PartitionCreate {
                    pid,
                    uuid,
                    tenant: TENANT.into(),
                    queue: queue.into(),
                    partition: "p0".into(),
                    created_at_us: now,
                },
            ],
        );
        pid
    }

    /// One more partition of an existing queue.
    fn create_partition(&mut self, e: &mut Entry, queue: &str, name: &str) -> u64 {
        let now = e.now_us;
        let pid = self.new_partition_pid();
        let uuid = self.id();
        self.cmd(
            e,
            vec![Effect::PartitionCreate {
                pid,
                uuid,
                tenant: TENANT.into(),
                queue: queue.into(),
                partition: name.into(),
                created_at_us: now,
            }],
        );
        pid
    }

    /// `pid_base + ordinal` (I18): the entry's base is `next_pid` at `begin`.
    fn new_partition_pid(&mut self) -> u64 {
        let pid = self.next_pid;
        self.next_pid += 1;
        self.last.insert(pid, -1);
        pid
    }

    fn push(&mut self, e: &mut Entry, pid: u64, count: u32) {
        let now = e.now_us;
        let last = self.last.get_mut(&pid).expect("known pid");
        let base = (*last + 1) as u64;
        *last += count as i64;
        let mut hashes = Vec::with_capacity(16 * count as usize);
        for i in 0..count as u64 {
            hashes.extend_from_slice(&pid.to_le_bytes());
            hashes.extend_from_slice(&(base + i).to_le_bytes());
        }
        let blob: Vec<u8> = (0..24 * count as usize)
            .map(|i| (pid as u8) ^ (base as u8) ^ i as u8)
            .collect();
        self.cmd(
            e,
            vec![Effect::Append {
                pid,
                bucket: (pid % 8) as u16,
                base_offset: base,
                count,
                created_at_us: now,
                hashes,
                blob,
            }],
        );
    }

    /// A pop: lease everything up to the partition's tail.
    fn pop(&mut self, e: &mut Entry, pid: u64) {
        let now = e.now_us;
        let mut row = samples::cursor_row();
        row.committed = *self.acked.get(&pid).unwrap_or(&-1);
        row.batch_end = Some(self.last[&pid] as u64);
        row.worker = Some("w1".into());
        row.lease_expires_at_us = Some(now + 30_000_000);
        row.lease_acquired_at_us = Some(now);
        row.batch_retry_count = 0;
        row.lease_conflated = false;
        row.delivered = Vec::new();
        row.created_at_us = now;
        self.cmd(
            e,
            vec![Effect::CursorSet {
                pid,
                group: GROUP.into(),
                row,
            }],
        );
    }

    /// An ack: commit up to `upto`, release the lease.
    fn ack(&mut self, e: &mut Entry, pid: u64, upto: i64) {
        let now = e.now_us;
        self.acked.insert(pid, upto);
        let mut row = samples::cursor_row();
        row.committed = upto;
        row.batch_end = None;
        row.worker = None;
        row.lease_expires_at_us = None;
        row.lease_acquired_at_us = None;
        row.batch_retry_count = 0;
        row.lease_conflated = false;
        row.delivered = Vec::new();
        row.total_consumed = (upto + 1) as u64;
        row.created_at_us = now;
        self.cmd(
            e,
            vec![Effect::CursorSet {
                pid,
                group: GROUP.into(),
                row,
            }],
        );
    }

    fn noop(&mut self, e: &mut Entry) {
        self.cmd(e, vec![Effect::Noop]);
    }
}

fn group_upsert(queue: &str, meta: GroupMeta) -> Effect {
    Effect::GroupUpsert {
        tenant: TENANT.into(),
        queue: queue.into(),
        group: GROUP.into(),
        meta,
    }
}

fn wire(e: &Entry) -> Bytes {
    Bytes::from(encode_entry(e).expect("encode"))
}

fn store_opts() -> StoreOpts {
    StoreOpts {
        map_bytes: Some(256 << 20),
        ..Default::default()
    }
}

fn open_store(dir: &Path) -> Arc<HeedStore> {
    Arc::new(HeedStore::open(&dir.join("store"), &store_opts()).expect("open store"))
}

/// The qlog knob ON, no timed durable point (only shutdown takes one), small
/// files so the queue logs roll.
fn open_cfg(dir: &Path, writer_pipeline: bool) -> OpenConfig {
    OpenConfig {
        node_id: 1,
        log_dir: dir.join("log"),
        log_opts: LogOptions {
            segment_bytes: 16 << 10,
            fsync: LogFsync::Off,
        },
        seg_root: dir.join("seg"),
        seg_opts: segments::Options {
            segment_bytes: 4 << 10,
            fsync: segments::FsyncMode::Data,
            fsync_threads: 1,
            nbuckets: segments::NBUCKETS,
        },
        apply_cfg: apply::ApplyConfig {
            qlog: true,
            durable_every_ms: 3_600_000,
            durable_every_bytes: 1 << 40,
            apply_writers: 0,
            ..apply::ApplyConfig::default()
        },
        apply_channel_capacity: 64,
        replay_deadline: Duration::from_secs(30),
        writer_pipeline,
    }
}

/// Fix the directory at one lane before anything opens it, whatever
/// `QUEEN_QLOG_LANES` says: a test asserting which log holds which record pins the
/// one-lane layout (lane 0 of a queue IS the queue's log).
fn one_lane(dir: &Path) {
    std::fs::create_dir_all(dir.join("qlog")).expect("qlog dir");
    std::fs::write(
        dir.join("qlog").join(crate::rsm::qlog::set::LANES_FILE),
        b"1\n",
    )
    .expect("LANES");
}

fn open_repl(
    dir: &Path,
    store: Arc<HeedStore>,
    writer_pipeline: bool,
) -> LocalReplicator<HeedStore> {
    open_repl_with(dir, store, writer_pipeline, EntryLayout::Copies)
}

/// [`open_repl`] with the entry layout pinned (never read from the env).
fn open_repl_with(
    dir: &Path,
    store: Arc<HeedStore>,
    writer_pipeline: bool,
    layout: EntryLayout,
) -> LocalReplicator<HeedStore> {
    LocalReplicator::open_with_entry_layout(
        store,
        open_cfg(dir, writer_pipeline),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
        layout,
    )
    .expect("open replicator (qlog WAL)")
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(30)
}

/// Shut down, then read the replicated digest and close the store.
fn shutdown_and_digest(repl: LocalReplicator<HeedStore>) -> StateDigest {
    let (_stats, store) = repl.shutdown().expect("shutdown");
    let store = Arc::try_unwrap(store).unwrap_or_else(|_| panic!("store still shared"));
    let d = store
        .read(|r| Ok(state_digest(r).expect("digest")))
        .expect("read");
    store.close();
    d
}

/// Propose every entry AT ONCE (all enqueued before any is answered, in
/// order), so the writer batches them into multi-entry groups. Returns each
/// entry's index.
async fn propose_all(repl: &LocalReplicator<HeedStore>, entries: Vec<Bytes>) -> Vec<u64> {
    use std::future::Future;
    use std::pin::Pin;
    type Fut<'a> = Pin<Box<dyn Future<Output = AppliedAt> + Send + 'a>>;
    let mut futs: Vec<Fut<'_>> = entries
        .into_iter()
        .map(|e| {
            Box::pin(async move { repl.propose(e, deadline()).await.expect("propose") }) as Fut<'_>
        })
        .collect();
    let mut index_of = vec![0u64; futs.len()];
    let mut done = vec![false; futs.len()];
    std::future::poll_fn(|cx| {
        let mut all = true;
        for i in 0..futs.len() {
            if !done[i] {
                match futs[i].as_mut().poll(cx) {
                    std::task::Poll::Ready(at) => {
                        index_of[i] = at.index;
                        done[i] = true;
                    }
                    std::task::Poll::Pending => all = false,
                }
            }
        }
        if all {
            std::task::Poll::Ready(())
        } else {
            std::task::Poll::Pending
        }
    })
    .await;
    index_of
}

/// `(seq, copies)` of every entry record in queue log `qid`.
fn entries_in(set: &QLogSet, qid: u64) -> Vec<(u64, u32)> {
    match set.log(qid) {
        Some(l) => l
            .read()
            .unwrap()
            .entry_records_from(0)
            .unwrap()
            .iter()
            .map(|r| (r.seq, r.copies))
            .collect(),
        None => Vec::new(),
    }
}

fn raft_log_last_index(dir: &Path) -> u64 {
    LogStore::open(&dir.join("log"), LogOptions::testing(16 << 10))
        .expect("open raft log")
        .0
        .last_index()
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).expect("mkdir");
    for e in std::fs::read_dir(from).expect("read_dir") {
        let e = e.expect("entry");
        if e.path().is_file() {
            std::fs::copy(e.path(), to.join(e.file_name())).expect("copy");
        }
    }
}

/// Replace `store/` with a snapshot taken earlier: the store reopens at an
/// OLDER durable checkpoint, as after a power loss.
fn roll_store_back(dir: &Path, snapshot: &Path) {
    std::fs::remove_dir_all(dir.join("store")).expect("rm store");
    copy_dir(snapshot, &dir.join("store"));
}

fn store_point(store: &HeedStore) -> (u64, u64) {
    store
        .read(|r| Ok((r.applied_index()?, r.durable_index()?)))
        .expect("read store point")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_entry_lands_in_exactly_the_logs_it_touches() {
    let td = TmpDir::new("route");
    let dir = td.path();
    // This test pins the ONE-lane layout (which log holds which copy).
    one_lane(dir);
    let mut g = Gen::new();
    let mut entries: Vec<Entry> = Vec::new();

    // E1: two queues (config, group, partition each) → both queue logs.
    let mut e = g.begin();
    let pa = g.create_queue(&mut e, QA);
    let pb = g.create_queue(&mut e, QB);
    entries.push(e);
    // E2: pushes to BOTH queues → both logs, a payload record in each.
    let mut e = g.begin();
    g.push(&mut e, pa, 2);
    g.push(&mut e, pb, 1);
    entries.push(e);
    // E3: a pop on A only → A's log only.
    let mut e = g.begin();
    g.pop(&mut e, pa);
    entries.push(e);
    // E4: a Noop → the system log only.
    let mut e = g.begin();
    g.noop(&mut e);
    entries.push(e);
    // E5: an ack on B only → B's log only.
    let mut e = g.begin();
    g.ack(&mut e, pb, 0);
    entries.push(e);
    // E6: a push and an ack on A → A's log only (one copy).
    let mut e = g.begin();
    g.push(&mut e, pa, 1);
    g.ack(&mut e, pa, 1);
    entries.push(e);

    {
        let store = open_store(dir);
        let repl = open_repl(dir, store, false);
        let at = repl
            .propose(wire(&entries[0]), deadline())
            .await
            .expect("propose E1");
        assert_eq!(at.index, 1);
        // E2..E6 in flight together: they share group commits.
        let idx = propose_all(&repl, entries[1..].iter().map(wire).collect()).await;
        assert_eq!(idx, vec![2, 3, 4, 5, 6], "gapless, in submission order");
        assert_eq!(repl.metrics().last_log_index, 6);
        assert_eq!(repl.applied_index(), 6);
        let _ = shutdown_and_digest(repl);
    }

    let set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
    let (qa, qb) = (
        QLogSet::queue_id_of(TENANT, QA),
        QLogSet::queue_id_of(TENANT, QB),
    );
    assert_ne!(qa, SYSTEM_QUEUE_ID);
    assert_eq!(entries_in(&set, qa), vec![(1, 2), (2, 2), (3, 1), (6, 1)]);
    assert_eq!(entries_in(&set, qb), vec![(1, 2), (2, 2), (5, 1)]);
    assert_eq!(entries_in(&set, SYSTEM_QUEUE_ID), vec![(4, 1)]);

    // Every copy is EXACTLY the payload-free encoding of its entry.
    let (got, out) = scan_all(&set, 1);
    assert_eq!(out.next_seq, 7);
    for r in &got {
        let want = encode_entry_payload_free(&entries[r.seq as usize - 1]).unwrap();
        assert_eq!(r.entry, want, "entry {} bytes", r.seq);
        assert_eq!(r.now_us, entries[r.seq as usize - 1].now_us);
    }

    // The payload records sit beside the entries, in their own queue's log,
    // stamped with the entry index.
    let reader = set.reader();
    let r = reader.read_owned(qa, pa, 0).unwrap().expect("A offset 0");
    assert_eq!((r.seq, r.count), (2, 2));
    let r = reader.read_owned(qa, pa, 2).unwrap().expect("A offset 2");
    assert_eq!((r.seq, r.count), (6, 1));
    let r = reader.read_owned(qb, pb, 0).unwrap().expect("B offset 0");
    assert_eq!((r.seq, r.count), (2, 1));
    assert!(reader.read_owned(qb, pa, 0).unwrap().is_none());

    // And the raft log was never written.
    assert_eq!(raft_log_last_index(dir), 0, "the raft log holds no entry");
}

/// Pushes, pops and acks across two queues; the store is rolled back to an
/// OLDER durable checkpoint (K) while the queue logs hold everything through
/// N; the raft log holds nothing. The reopen must replay K+1..N from the queue
/// logs ALONE and land on the live state byte-for-byte.
async fn recover_from_the_queue_logs_alone(tag: &str, writer_pipeline: bool, layout: EntryLayout) {
    let td = TmpDir::new(tag);
    let dir = td.path();
    // This test pins the ONE-lane layout (which log holds which copy).
    one_lane(dir);
    let snapshot = dir.join("store-at-k");
    let mut g = Gen::new();

    // Phase 1: K entries, clean shutdown (a durable point at K), snapshot.
    let (pa, pb, k) = {
        let store = open_store(dir);
        let repl = open_repl_with(dir, store, writer_pipeline, layout);
        let mut e = g.begin();
        let pa = g.create_queue(&mut e, QA);
        let pb = g.create_queue(&mut e, QB);
        let mut batch = vec![wire(&e)];
        let mut e = g.begin();
        g.push(&mut e, pa, 3);
        batch.push(wire(&e));
        let mut e = g.begin();
        g.push(&mut e, pb, 2);
        batch.push(wire(&e));
        let idx = propose_all(&repl, batch).await;
        assert_eq!(idx, vec![1, 2, 3]);
        let _ = shutdown_and_digest(repl);
        (pa, pb, 3u64)
    };
    copy_dir(&dir.join("store"), &snapshot);

    // Phase 2: N-K more entries — pushes to both queues, pops, acks, a Noop,
    // a partition created INSIDE the window — then the live state at N.
    let (live, n, pc) = {
        let store = open_store(dir);
        assert_eq!(store_point(&store), (k, k));
        let repl = open_repl_with(dir, store, writer_pipeline, layout);
        assert_eq!(repl.applied_index(), k);
        let mut batch: Vec<Bytes> = Vec::new();
        let mut e = g.begin();
        g.pop(&mut e, pa);
        batch.push(wire(&e));
        let mut e = g.begin();
        g.push(&mut e, pb, 1);
        g.push(&mut e, pa, 1);
        batch.push(wire(&e));
        let mut e = g.begin();
        g.ack(&mut e, pa, 1);
        batch.push(wire(&e));
        let mut e = g.begin();
        g.noop(&mut e);
        batch.push(wire(&e));
        let idx = propose_all(&repl, batch).await;
        assert_eq!(idx, vec![k + 1, k + 2, k + 3, k + 4]);
        // One at a time too (one entry per group).
        let mut e = g.begin();
        g.pop(&mut e, pb);
        assert_eq!(
            repl.propose(wire(&e), deadline()).await.unwrap().index,
            k + 5
        );
        let mut e = g.begin();
        g.ack(&mut e, pb, 2);
        assert_eq!(
            repl.propose(wire(&e), deadline()).await.unwrap().index,
            k + 6
        );
        let mut e = g.begin();
        let pc = g.create_partition(&mut e, QA, "p1");
        g.push(&mut e, pc, 2);
        assert_eq!(
            repl.propose(wire(&e), deadline()).await.unwrap().index,
            k + 7
        );
        let mut e = g.begin();
        g.push(&mut e, pa, 2);
        g.pop(&mut e, pc);
        assert_eq!(
            repl.propose(wire(&e), deadline()).await.unwrap().index,
            k + 8
        );
        let n = k + 8;
        assert_eq!(repl.metrics().last_log_index, n);
        (shutdown_and_digest(repl), n, pc)
    };

    // The raft log holds NOTHING: whatever the reopen recovers, it recovers
    // from the queue logs.
    assert_eq!(raft_log_last_index(dir), 0, "the raft log was written");

    // Phase 3: power loss — the store reopens at the OLDER checkpoint K.
    roll_store_back(dir, &snapshot);
    let recovered = {
        let store = open_store(dir);
        assert_eq!(store_point(&store), (k, k), "the rolled-back checkpoint");
        let repl = open_repl_with(dir, store, writer_pipeline, layout);
        assert_eq!(
            repl.applied_index(),
            n,
            "the queue logs alone brought back every acknowledged entry"
        );
        assert_eq!(repl.metrics().last_log_index, n);
        shutdown_and_digest(repl)
    };
    assert_eq!(
        recovered.whole,
        live.whole,
        "the state replayed from the queue logs differs from the live state, first at {:?}",
        recovered.first_difference(&live),
    );

    // Phase 4: from the checkpoint K again — the replay re-creates `pc` inside
    // the window (the writer's route for it is seeded from the replay) — and
    // the writer continues at N+1, routing to `pc` and to `pa` (created before
    // the checkpoint: the committed catalog).
    roll_store_back(dir, &snapshot);
    let live2 = {
        let store = open_store(dir);
        let repl = open_repl_with(dir, store, writer_pipeline, layout);
        assert_eq!(repl.applied_index(), n);
        let mut e = g.begin();
        g.push(&mut e, pc, 1);
        g.push(&mut e, pa, 1);
        assert_eq!(
            repl.propose(wire(&e), deadline()).await.unwrap().index,
            n + 1
        );
        shutdown_and_digest(repl)
    };
    // Phase 5: and once more from K: N+1 replays from the queue logs too.
    roll_store_back(dir, &snapshot);
    let recovered2 = {
        let store = open_store(dir);
        let repl = open_repl_with(dir, store, writer_pipeline, layout);
        assert_eq!(repl.applied_index(), n + 1);
        shutdown_and_digest(repl)
    };
    assert_eq!(
        recovered2.whole,
        live2.whole,
        "the replay through N+1 differs, first at {:?}",
        recovered2.first_difference(&live2),
    );
    let set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
    let qa = QLogSet::queue_id_of(TENANT, QA);
    assert!(entries_in(&set, qa).contains(&(n + 1, 1)));
    let r = set
        .reader()
        .read_owned(qa, pc, 2)
        .unwrap()
        .expect("the new push to the window-created partition");
    assert_eq!(r.seq, n + 1);
    assert_eq!(raft_log_last_index(dir), 0, "the raft log was written");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_logs_alone_recover_acked_work() {
    recover_from_the_queue_logs_alone("recover", false, EntryLayout::Copies).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_logs_alone_recover_acked_work_pipelined() {
    recover_from_the_queue_logs_alone("recover-wp", true, EntryLayout::Copies).await;
}

/// An incomplete group left on disk (a partial entry, its stale payload
/// record, an orphan later entry) is discarded at reopen; its seq goes to a
/// NEW entry; a replay from an older checkpoint then reads the new entry, not
/// the stale one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_incomplete_tail_is_cut_and_its_seqs_are_reused() {
    let td = TmpDir::new("reuse");
    let dir = td.path();
    let snapshot = dir.join("store-at-n");
    let mut g = Gen::new();
    let qa = QLogSet::queue_id_of(TENANT, QA);

    // A clean run to N, store durable at N; snapshot it.
    let (pa, n) = {
        let store = open_store(dir);
        let repl = open_repl(dir, store, false);
        let mut e = g.begin();
        let pa = g.create_queue(&mut e, QA);
        let _pb = g.create_queue(&mut e, QB);
        let mut batch = vec![wire(&e)];
        let mut e = g.begin();
        g.push(&mut e, pa, 2);
        batch.push(wire(&e));
        let mut e = g.begin();
        g.pop(&mut e, pa);
        batch.push(wire(&e));
        assert_eq!(propose_all(&repl, batch).await, vec![1, 2, 3]);
        let _ = shutdown_and_digest(repl);
        (pa, 3u64)
    };
    copy_dir(&dir.join("store"), &snapshot);

    // Inject the incomplete group N+1..N+2: entry N+1 claims two copies (A and
    // B) but only A's landed, with a stale payload record at A's next offset;
    // entry N+2 (system) landed although it follows it.
    let stale_offset = 2u64;
    {
        let mut set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
        let h = [0x5Au8; 16];
        let garbage = vec![0xEEu8; 64];
        set.write_mixed_for_qid(
            qa,
            &[
                msg(n + 1, pa, stale_offset, &h, b"STALE"),
                ent(n + 1, 2, &garbage),
            ],
        )
        .unwrap();
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(n + 2, 1, &garbage)])
            .unwrap();
        set.sync().unwrap();
    }

    // Reopen: the store is at N, the incomplete tail is cut (a replay of it
    // would fail to decode the garbage), and the writer reuses N+1.
    let new_entry = {
        let store = open_store(dir);
        let repl = open_repl(dir, store, false);
        assert_eq!(repl.applied_index(), n);
        assert_eq!(repl.metrics().last_log_index, n);
        let mut e = g.begin();
        g.push(&mut e, pa, 1);
        assert_eq!(
            repl.propose(wire(&e), deadline()).await.unwrap().index,
            n + 1
        );
        let _ = shutdown_and_digest(repl);
        e
    };
    {
        let set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
        let (got, out) = scan_all(&set, n + 1);
        assert_eq!(got.len(), 1, "only the new entry N+1 remains: {out:?}");
        assert_eq!(got[0].seq, n + 1);
        assert_eq!(got[0].entry, encode_entry_payload_free(&new_entry).unwrap());
        assert_eq!((out.next_seq, out.stopped.clone()), (n + 2, None));
        let r = set
            .reader()
            .read_owned(qa, pa, stale_offset)
            .unwrap()
            .expect("the new payload record");
        assert_ne!(r.payload, b"STALE");
        assert_eq!(r.seq, n + 1);
    }

    // A replay from the checkpoint at N reads the NEW N+1.
    roll_store_back(dir, &snapshot);
    let store = open_store(dir);
    assert_eq!(store_point(&store), (n, n));
    let repl = open_repl(dir, store, false);
    assert_eq!(repl.applied_index(), n + 1);
    let _ = shutdown_and_digest(repl);
}

// ---------------------------------------------------------------------------
// The stub layout (`QUEEN_QLOG_ENTRY_LAYOUT=stub`): the whole entry record in
// ONE touched log, a 61-byte stub in every other. Same logs, same fsyncs, same
// durability rule (every part must be found), bytes linear in the entry.
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_logs_alone_recover_acked_work_stub_layout() {
    recover_from_the_queue_logs_alone("recover-stub", false, EntryLayout::Stub).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_logs_alone_recover_acked_work_stub_layout_pipelined() {
    recover_from_the_queue_logs_alone("recover-stub-wp", true, EntryLayout::Stub).await;
}

/// A stub for the whole record `whole` of `seq`.
fn stub(seq: u64, copies: u32, whole: &[u8]) -> WriteRecord<'static> {
    WriteRecord::EntryStub(EntryStubInput {
        seq,
        now_us: 1_000 + seq as i64,
        copies,
        term: 0,
        digest: super::record::entry_digest(whole),
        len: whole.len() as u32,
    })
}

/// `(seq, 'W' whole | 's' stub, copies)` of every entry part in log `qid`.
fn parts_in(set: &QLogSet, qid: u64) -> Vec<(u64, char, u32)> {
    match set.log(qid) {
        Some(l) => l
            .read()
            .unwrap()
            .entry_parts_between(0, u64::MAX)
            .unwrap()
            .into_iter()
            .map(|p| match p {
                EntryPart::Full(r) => (r.seq, 'W', r.copies),
                EntryPart::Stub(s) => (s.seq, 's', s.copies),
            })
            .collect(),
        None => Vec::new(),
    }
}

/// On-disk bytes of every entry part (whole records and stubs) in `qids`.
fn entry_part_bytes(set: &QLogSet, qids: &[u64]) -> usize {
    let mut n = 0;
    for qid in qids {
        if let Some(l) = set.log(*qid) {
            for p in l.read().unwrap().entry_parts_between(0, u64::MAX).unwrap() {
                n += super::record::FIXED_PREFIX
                    + match p {
                        EntryPart::Full(r) => r.entry.len(),
                        EntryPart::Stub(_) => super::record::STUB_PAYLOAD,
                    };
            }
        }
    }
    n
}

#[test]
fn stub_parts_merge_into_whole_entries() {
    let td = TmpDir::new("stub-merge");
    let root = td.path().join("qlog");
    // Tiny files: the parts of one entry span sealed and active files.
    let opts = QLogOptions::testing(160);
    let h = [0x33u8; 16];
    let b: Vec<Vec<u8>> = (0..=5).map(|s| eb(s, 0xD0)).collect();
    {
        let mut set = QLogSet::new(root.clone(), opts);
        // seq 1: whole in the system log (the lowest id), stubs in q1 and q2,
        // a payload record in q1 before its stub.
        set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(1, 3, &b[1])])
            .unwrap();
        set.write_mixed_for_qid(Q1, &[msg(1, 7, 0, &h, b"one"), stub(1, 3, &b[1])])
            .unwrap();
        set.write_mixed_for_qid(Q2, &[stub(1, 3, &b[1])]).unwrap();
        set.sync().unwrap();
        // seq 2: one log: whole, no stub.
        set.write_mixed_for_qid(Q2, &[ent(2, 1, &b[2])]).unwrap();
        // seq 3: the COPIES layout (a directory written before the knob flipped).
        set.write_mixed_for_qid(Q1, &[ent(3, 2, &b[3])]).unwrap();
        set.write_mixed_for_qid(Q2, &[ent(3, 2, &b[3])]).unwrap();
        // seq 4: whole in q1, stub in q2 (the readers do not care which log).
        set.write_mixed_for_qid(Q1, &[msg(4, 7, 1, &h, b"four"), ent(4, 2, &b[4])])
            .unwrap();
        set.write_mixed_for_qid(Q2, &[stub(4, 2, &b[4])]).unwrap();
        set.sync().unwrap();
        let q1 = set.log(Q1).unwrap();
        assert!(q1.read().unwrap().file_count() > 1, "q1 rolled");
    }
    let set = reopen(&root, opts);
    let (got, out) = scan_all(&set, 1);
    assert_eq!(
        got.iter().map(|r| (r.seq, r.copies)).collect::<Vec<_>>(),
        vec![(1, 3), (2, 1), (3, 2), (4, 2)]
    );
    for r in &got {
        assert_eq!(r.entry, b[r.seq as usize], "entry {} is the whole record", r.seq);
        assert_eq!(r.now_us, 1_000 + r.seq as i64);
    }
    assert_eq!((out.next_seq, out.stopped.clone()), (5, None));
    // The range read (a follower catching up) names every log holding a part:
    // where the entry's payload records are.
    let mut logs: Vec<(u64, Vec<u64>)> = set
        .reader()
        .entry_records_range(1, 5)
        .unwrap()
        .into_iter()
        .map(|(r, mut l)| {
            l.sort_unstable();
            (r.seq, l)
        })
        .collect();
    logs.sort_unstable();
    assert_eq!(
        logs,
        vec![
            (1, vec![SYSTEM_QUEUE_ID, Q1, Q2]),
            (2, vec![Q2]),
            (3, vec![Q1, Q2]),
            (4, vec![Q1, Q2]),
        ]
    );
    // Stubs are never indexed: only the payload records read back.
    let reader = set.reader();
    let one = reader.read_owned(Q1, 7, 0).unwrap().expect("payload one");
    assert_eq!((one.seq, one.payload.as_slice()), (1, &b"one"[..]));
    let four = reader.read_owned(Q1, 7, 1).unwrap().expect("payload four");
    assert_eq!((four.seq, four.payload.as_slice()), (4, &b"four"[..]));
    assert!(reader.read_owned(Q2, 7, 0).unwrap().is_none());
}

#[test]
fn a_missing_stub_or_whole_record_leaves_the_entry_not_durable() {
    for missing in ["stub", "whole"] {
        let td = TmpDir::new(&format!("stub-cut-{missing}"));
        let root = td.path().join("qlog");
        let opts = QLogOptions::testing(4096);
        let h = [0x44u8; 16];
        let (b1, b2, b3) = (eb(1, 0xC1), eb(2, 0xC2), eb(3, 0xC3));
        {
            let mut set = QLogSet::new(root.clone(), opts);
            // seq 1 is complete: whole in q1, stub in q2.
            set.write_mixed_for_qid(Q1, &[ent(1, 2, &b1)]).unwrap();
            set.write_mixed_for_qid(Q2, &[stub(1, 2, &b1)]).unwrap();
            // seq 2 was meant for q1 (whole + a payload record) AND q2 (stub +
            // a payload record); one of the two logs' writes never landed.
            if missing != "whole" {
                set.write_mixed_for_qid(Q1, &[msg(2, 5, 0, &h, b"q1-two"), ent(2, 2, &b2)])
                    .unwrap();
            }
            if missing != "stub" {
                set.write_mixed_for_qid(Q2, &[msg(2, 6, 0, &h, b"q2-two"), stub(2, 2, &b2)])
                    .unwrap();
            }
            // seq 3 (system log) landed although it follows.
            set.write_mixed_for_qid(SYSTEM_QUEUE_ID, &[ent(3, 1, &b3)])
                .unwrap();
            set.sync().unwrap();
        }
        let mut set = reopen(&root, opts);
        let (got, out) = scan_all(&set, 1);
        assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1], "{missing}");
        assert_eq!(
            (out.next_seq, out.discarded, out.max_seq_found),
            (2, 2, 3),
            "{missing}"
        );
        assert!(
            out.stopped.as_deref().unwrap_or("").contains("incomplete"),
            "{missing}: {:?}",
            out.stopped
        );
        // The follower read stops there too.
        assert_eq!(set.reader().entry_records_range(1, 4).unwrap().len(), 1);
        // The cut drops the part that did land, its payload record and seq 3.
        assert!(set.truncate_from(out.next_seq).unwrap() > 0, "{missing}");
        drop(set);
        let set = reopen(&root, opts);
        let (got, out) = scan_all(&set, 1);
        assert_eq!(got.iter().map(|r| r.seq).collect::<Vec<_>>(), vec![1]);
        assert_eq!(
            (out.next_seq, out.max_seq_found, out.stopped.clone()),
            (2, 1, None),
            "{missing}: durable"
        );
        let reader = set.reader();
        assert!(reader.read_owned(Q1, 5, 0).unwrap().is_none());
        assert!(reader.read_owned(Q2, 6, 0).unwrap().is_none());
    }
}

#[test]
fn stubs_that_do_not_name_the_whole_record_or_stand_alone_are_refused() {
    let opts = QLogOptions::testing(4096);
    let (a, b) = (eb(1, 0xAA), eb(1, 0xBB));
    let refused = |name: &str, q1: Vec<WriteRecord<'_>>, q2: Vec<WriteRecord<'_>>, want: &str| {
        let td = TmpDir::new(name);
        let root = td.path().join("qlog");
        {
            let mut set = QLogSet::new(root.clone(), opts);
            set.write_mixed_for_qid(Q1, &q1).unwrap();
            set.write_mixed_for_qid(Q2, &q2).unwrap();
            set.sync().unwrap();
        }
        let set = reopen(&root, opts);
        let err = set
            .scan_entries(1, &mut |_| Ok(()))
            .expect_err("recovery must refuse");
        assert!(err.to_string().contains(want), "{name}: {err}");
        let err = set
            .reader()
            .entry_records_range(1, 2)
            .expect_err("the range read must refuse");
        assert!(err.to_string().contains(want), "{name}: {err}");
    };
    // A stub naming other bytes (a stale part of an older seq 1).
    refused("stub-digest", vec![ent(1, 2, &a)], vec![stub(1, 2, &b)], "disagree");
    // A stub whose copies differ from the whole record's.
    refused("stub-copies", vec![ent(1, 2, &a)], vec![stub(1, 3, &a)], "disagree");
    // Every part a stub: no log holds the whole record.
    refused("stub-only", vec![stub(1, 2, &a)], vec![stub(1, 2, &a)], "stubs");
}

/// A real `LocalReplicator` on the stub layout: each entry's whole record is in
/// the LOWEST log it touches and a stub in every other; the bytes are smaller
/// than the copies layout's for the same entries; recovery hands apply exactly
/// the payload-free encoding.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_stub_layout_writes_each_entry_once() {
    let mut g = Gen::new();
    let mut entries: Vec<Entry> = Vec::new();
    // E1: two queues → both logs.
    let mut e = g.begin();
    let pa = g.create_queue(&mut e, QA);
    let pb = g.create_queue(&mut e, QB);
    entries.push(e);
    // E2: pushes to both → both logs.
    let mut e = g.begin();
    g.push(&mut e, pa, 2);
    g.push(&mut e, pb, 1);
    entries.push(e);
    // E3: a pop on A → A only.
    let mut e = g.begin();
    g.pop(&mut e, pa);
    entries.push(e);
    // E4: a Noop → the system log only.
    let mut e = g.begin();
    g.noop(&mut e);
    entries.push(e);
    // E5: pushes, a pop and an ack across both queues.
    let mut e = g.begin();
    g.push(&mut e, pa, 1);
    g.push(&mut e, pb, 2);
    g.pop(&mut e, pb);
    g.ack(&mut e, pa, 1);
    entries.push(e);

    let (qa, qb) = (
        QLogSet::queue_id_of(TENANT, QA),
        QLogSet::queue_id_of(TENANT, QB),
    );
    let (lo, hi) = (qa.min(qb), qa.max(qb));
    let mut part_bytes = Vec::new();
    for layout in [EntryLayout::Copies, EntryLayout::Stub] {
        let td = TmpDir::new(&format!("stub-route-{layout:?}"));
        let dir = td.path();
        one_lane(dir);
        {
            let store = open_store(dir);
            let repl = open_repl_with(dir, store, false, layout);
            let at = repl.propose(wire(&entries[0]), deadline()).await.unwrap();
            assert_eq!(at.index, 1);
            let idx = propose_all(&repl, entries[1..].iter().map(wire).collect()).await;
            assert_eq!(idx, vec![2, 3, 4, 5]);
            let _ = shutdown_and_digest(repl);
        }
        let set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
        if layout == EntryLayout::Stub {
            // The entries both queues share (copies 2): whole in the lower
            // log, a stub in the higher.
            let shared = |v: Vec<(u64, char, u32)>| {
                v.into_iter().filter(|p| p.2 == 2).collect::<Vec<_>>()
            };
            assert_eq!(
                shared(parts_in(&set, lo)),
                vec![(1, 'W', 2), (2, 'W', 2), (5, 'W', 2)],
                "the lower queue log holds every shared entry whole"
            );
            assert_eq!(
                shared(parts_in(&set, hi)),
                vec![(1, 's', 2), (2, 's', 2), (5, 's', 2)],
                "the higher one a stub of each"
            );
            // E3 touches A only: whole, wherever A sorts.
            let a_parts = parts_in(&set, qa);
            assert!(a_parts.contains(&(3, 'W', 1)), "{a_parts:?}");
            assert_eq!(parts_in(&set, SYSTEM_QUEUE_ID), vec![(4, 'W', 1)]);
        } else {
            assert_eq!(parts_in(&set, SYSTEM_QUEUE_ID), vec![(4, 'W', 1)]);
            assert!(parts_in(&set, hi).iter().all(|p| p.1 == 'W'));
        }
        // Either way recovery hands apply exactly the payload-free encoding.
        let (got, out) = scan_all(&set, 1);
        assert_eq!(out.next_seq, 6);
        for r in &got {
            let want = encode_entry_payload_free(&entries[r.seq as usize - 1]).unwrap();
            assert_eq!(r.entry, want, "{layout:?} entry {}", r.seq);
        }
        part_bytes.push(entry_part_bytes(&set, &[SYSTEM_QUEUE_ID, qa, qb]));
    }
    // Three entries touch two logs: copies write their bytes twice, stubs once
    // plus 61 bytes each.
    let whole_twice: usize = [0usize, 1, 4]
        .iter()
        .map(|i| super::record::FIXED_PREFIX + encode_entry_payload_free(&entries[*i]).unwrap().len())
        .sum();
    assert_eq!(
        part_bytes[0] - part_bytes[1],
        whole_twice - 3 * (super::record::FIXED_PREFIX + super::record::STUB_PAYLOAD),
        "copies {} vs stub {} bytes",
        part_bytes[0],
        part_bytes[1]
    );
}

/// The durability rule on the REAL writer's bytes: the last entry's part in
/// one log (its stub, or its whole record) never reached the disk — cut
/// together with that log's payload record of the same group, as when that
/// log's write or fsync did not land. The reopen must stop before that entry,
/// drop what did land of it, reach the state of the entry before, and reuse
/// its index.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stub_layout_a_lost_part_is_cut_and_its_index_reused() {
    for missing in ["stub", "whole"] {
        let td = TmpDir::new(&format!("stub-lost-{missing}"));
        let dir = td.path();
        one_lane(dir);
        let snapshot = dir.join("store-at-k");
        let mut g = Gen::new();
        let (qa, qb) = (
            QLogSet::queue_id_of(TENANT, QA),
            QLogSet::queue_id_of(TENANT, QB),
        );
        // Phase 1: to K, checkpoint.
        let (pa, pb, k) = {
            let store = open_store(dir);
            let repl = open_repl_with(dir, store, false, EntryLayout::Stub);
            let mut e = g.begin();
            let pa = g.create_queue(&mut e, QA);
            let pb = g.create_queue(&mut e, QB);
            let mut batch = vec![wire(&e)];
            let mut e = g.begin();
            g.push(&mut e, pa, 2);
            g.push(&mut e, pb, 2);
            batch.push(wire(&e));
            assert_eq!(propose_all(&repl, batch).await, vec![1, 2]);
            let _ = shutdown_and_digest(repl);
            (pa, pb, 2u64)
        };
        copy_dir(&dir.join("store"), &snapshot);
        // Phase 2: to N-1 (pops, acks, pushes over both queues); the state there.
        let (before, n) = {
            let store = open_store(dir);
            let repl = open_repl_with(dir, store, true, EntryLayout::Stub);
            let mut batch = Vec::new();
            let mut e = g.begin();
            g.pop(&mut e, pa);
            g.pop(&mut e, pb);
            batch.push(wire(&e));
            let mut e = g.begin();
            g.ack(&mut e, pa, 1);
            g.push(&mut e, pb, 1);
            batch.push(wire(&e));
            assert_eq!(propose_all(&repl, batch).await, vec![k + 1, k + 2]);
            (shutdown_and_digest(repl), k + 3)
        };
        // Phase 3: entry N pushes to both queues: in each log, its payload
        // record and then its part (whole or stub) are the last records.
        let (base_a, base_b) = (g.last[&pa] + 1, g.last[&pb] + 1);
        {
            let store = open_store(dir);
            let repl = open_repl_with(dir, store, false, EntryLayout::Stub);
            let mut e = g.begin();
            g.push(&mut e, pa, 1);
            g.push(&mut e, pb, 1);
            assert_eq!(repl.propose(wire(&e), deadline()).await.unwrap().index, n);
            let _ = shutdown_and_digest(repl);
        }
        // The crash: cut N's records from the log holding its stub (or its
        // whole record), at N's payload record in that log.
        let (home, other) = (qa.min(qb), qa.max(qb));
        let victim = if missing == "whole" { home } else { other };
        let (pid, base) = if victim == qa {
            (pa, base_a as u64)
        } else {
            (pb, base_b as u64)
        };
        {
            let set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
            let parts = parts_in(&set, victim);
            let want = if missing == "whole" { 'W' } else { 's' };
            assert_eq!(parts.last(), Some(&(n, want, 2)), "{missing}: {parts:?}");
            let log = set.log(victim).unwrap();
            let log = log.read().unwrap();
            let at = log.locate(pid, base).expect("N's payload record");
            assert_eq!(Some(at.file_id), log.active_file_id());
            let f = std::fs::OpenOptions::new()
                .write(true)
                .open(active_file(&dir.join("qlog"), victim))
                .unwrap();
            f.set_len(at.offset).unwrap();
        }
        // Power loss: the store reopens at K and replays from the queue logs:
        // through N-1, landing exactly on the state before N.
        roll_store_back(dir, &snapshot);
        {
            let store = open_store(dir);
            let repl = open_repl_with(dir, store, false, EntryLayout::Stub);
            assert_eq!(repl.applied_index(), n - 1, "{missing}: N is not durable");
            assert_eq!(repl.metrics().last_log_index, n - 1);
            let after = shutdown_and_digest(repl);
            assert_eq!(
                after.whole,
                before.whole,
                "{missing}: the replay differs from the state before N, first at {:?}",
                after.first_difference(&before)
            );
        }
        // N never happened: the partitions' tails are back where they were.
        g.last.insert(pa, base_a - 1);
        g.last.insert(pb, base_b - 1);
        // The index is reused by a NEW entry.
        {
            let store = open_store(dir);
            let repl = open_repl_with(dir, store, false, EntryLayout::Stub);
            let mut e = g.begin();
            g.push(&mut e, pa, 1);
            assert_eq!(repl.propose(wire(&e), deadline()).await.unwrap().index, n);
            let _ = shutdown_and_digest(repl);
        }
        // On disk: the gapless prefix through the new N, and no part of the
        // old N left in the log that kept its records.
        {
            let set = reopen(&dir.join("qlog"), QLogOptions::testing(4 << 10));
            let (got, out) = scan_all(&set, k + 1);
            assert_eq!(out.next_seq, n + 1, "{missing}: {out:?}");
            assert_eq!(got.len() as u64, n - k);
            // What survives of the cut entry is gone: the other log holds no
            // part of the old N besides what the new N wrote (A only).
            let other_parts = parts_in(&set, if victim == home { other } else { home });
            assert!(
                other_parts.iter().all(|p| p.0 < n || (p.0 == n && p.2 == 1)),
                "{missing}: {other_parts:?}"
            );
        }
    }
}

/// openraft's log reader over the stub layout: entries read back from the
/// queue logs (below the in-memory window, or recovered payload-free above
/// the applied index) come back WHOLE — their payload-free record from the
/// one log holding it and every payload from its queue log.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn read_range_rehydrates_stub_layout_entries() {
    use crate::rsm::replicator::local::{GroupBody, GroupItem, QlogWrite};
    use crate::rsm::replicator::raft::log_store::{LogStore, OpenCfg};
    use crate::rsm::replicator::raft::types::{log_id, REntry};
    use openraft::{EntryPayload, RaftLogReader};

    const QC: &str = "phase-c-c";
    const TERM: u64 = 3;
    let td = TmpDir::new("stub-read-range");
    let dir = td.path();
    one_lane(dir);
    let root = dir.join("qlog");
    let qopts = QLogOptions::testing(1 << 10);
    let mut g = Gen::new();
    let mut entries: Vec<Entry> = Vec::new();
    let mut e = g.begin();
    let pa = g.create_queue(&mut e, QA);
    let pb = g.create_queue(&mut e, QB);
    let pc = g.create_queue(&mut e, QC);
    entries.push(e);
    let mut e = g.begin();
    g.push(&mut e, pa, 3);
    g.push(&mut e, pb, 2);
    g.push(&mut e, pc, 1);
    entries.push(e);
    let mut e = g.begin();
    g.pop(&mut e, pa);
    g.push(&mut e, pc, 2);
    entries.push(e);
    let mut e = g.begin();
    g.noop(&mut e);
    entries.push(e);
    let mut e = g.begin();
    g.ack(&mut e, pa, 2);
    g.push(&mut e, pb, 1);
    g.push(&mut e, pa, 1);
    entries.push(e);
    let n = entries.len() as u64;

    let write_all = |root: &Path| {
        let mut set = QLogSet::new(root.to_path_buf(), qopts);
        set.reopen_all().unwrap();
        let mut w = QlogWrite {
            set,
            pid_qid: HashMap::new(),
            lookup: Arc::new(|_| Ok(None)),
            layout: EntryLayout::Stub,
        };
        let mut items: Vec<GroupItem> = entries
            .iter()
            .enumerate()
            .map(|(i, e)| GroupItem {
                seq: i as u64 + 1,
                term: TERM,
                body: GroupBody::Entry {
                    entry: Arc::new(e.clone()),
                    pre: Vec::new(),
                    z: None,
                },
            })
            .collect();
        let (g1, g2) = items.split_at_mut(2);
        w.write_group(g1).unwrap();
        w.write_group(g2).unwrap();
    };
    write_all(&root);
    {
        // One whole record per entry; the multi-queue entries also have stubs.
        let set = reopen(&root, qopts);
        let qids: Vec<u64> = [QA, QB, QC]
            .iter()
            .map(|q| QLogSet::queue_id_of(TENANT, q))
            .chain([SYSTEM_QUEUE_ID])
            .collect();
        let mut whole = vec![0u32; n as usize + 1];
        let mut stubs = 0;
        for q in &qids {
            for (seq, kind, _) in parts_in(&set, *q) {
                if kind == 'W' {
                    whole[seq as usize] += 1;
                } else {
                    stubs += 1;
                }
            }
        }
        assert_eq!(&whole[1..], &[1, 1, 1, 1, 1], "exactly one whole record per entry");
        assert_eq!(stubs, 2 + 2 + 1 + 1, "E1, E2: 3 logs; E3, E5: 2 logs");
    }

    let open = |state: &str| {
        LogStore::open(OpenCfg {
            qlog_root: root.clone(),
            qopts,
            state_dir: dir.join(state),
            lookup: Arc::new(|_| Ok(None)),
            durable_index: 0,
            applied: None,
            qlog_durable_index: 0,
            poison: Arc::new(std::sync::Mutex::new(None)),
            cache_cap: 64 << 20,
        })
    };
    let check = |got: &[REntry], upto: u64| {
        assert_eq!(got.len() as u64, upto);
        for (i, re) in got.iter().enumerate() {
            assert_eq!(re.log_id, log_id(TERM, i as u64));
            let EntryPayload::Normal(app) = &re.payload else {
                panic!("entry {i} is not an application entry");
            };
            let (_pf, pf_bytes, payloads) = app.stored_parts().expect("the whole stored form");
            assert_eq!(
                pf_bytes.as_ref(),
                encode_entry_payload_free(&entries[i]).unwrap().as_slice(),
                "entry {i}: the payload-free record"
            );
            let blobs: Vec<&[u8]> = entries[i]
                .effects
                .iter()
                .filter_map(|eff| match eff {
                    Effect::Append { blob, .. } => Some(blob.as_slice()),
                    _ => None,
                })
                .collect();
            assert_eq!(payloads.len(), blobs.len(), "entry {i}");
            for (p, raw) in payloads.iter().zip(blobs) {
                let bytes = if p.zstd {
                    super::codec::decompress(&p.bytes).unwrap()
                } else {
                    p.bytes.to_vec()
                };
                assert_eq!(bytes, raw, "entry {i}: a payload");
            }
        }
    };
    // (a) Every entry applied and evicted: read from the queue logs.
    // (b) Nothing applied: recovered payload-free into the cache, then
    //     rehydrated for a follower.
    for (state, applied) in [("raft-a", Some(log_id(TERM, n - 1))), ("raft-b", None)] {
        let opened = LogStore::open(OpenCfg {
            qlog_root: root.clone(),
            qopts,
            state_dir: dir.join(state),
            lookup: Arc::new(|_| Ok(None)),
            durable_index: 0,
            applied,
            qlog_durable_index: 0,
            poison: Arc::new(std::sync::Mutex::new(None)),
            cache_cap: 64 << 20,
        })
        .expect("open the log store");
        assert_eq!(opened.recovered, n);
        let mut st = opened.store.clone();
        let got = st.try_get_log_entries(0..n).await.expect("read_range");
        check(&got, n);
        // A window in the middle.
        let mid = st.try_get_log_entries(1..4).await.expect("read_range");
        assert_eq!(mid.len(), 3);
        assert_eq!(mid[0].log_id, log_id(TERM, 1));
        opened.store.close();
        drop(st);
        opened.writer.join().expect("writer");
    }

    // (c) The last entry's stub never landed: the log store recovers the
    // prefix only, and the reader serves exactly that prefix.
    let (qa, qb) = (
        QLogSet::queue_id_of(TENANT, QA),
        QLogSet::queue_id_of(TENANT, QB),
    );
    let stub_log = qa.max(qb); // E5 touches A and B: the higher one holds its stub
    {
        let set = reopen(&root, qopts);
        assert_eq!(parts_in(&set, stub_log).last(), Some(&(n, 's', 2)));
        let log = set.log(stub_log).unwrap();
        let end = log.read().unwrap().files().last().unwrap().bytes;
        let f = std::fs::OpenOptions::new()
            .write(true)
            .open(active_file(&root, stub_log))
            .unwrap();
        f.set_len(end - (super::record::FIXED_PREFIX + super::record::STUB_PAYLOAD) as u64)
            .unwrap();
    }
    let opened = open("raft-c").expect("open after the lost stub");
    assert_eq!(opened.recovered, n - 1, "the entry missing a stub is not durable");
    let mut st = opened.store.clone();
    let got = st.try_get_log_entries(0..n).await.expect("read_range");
    check(&got, n - 1);
    opened.store.close();
    drop(st);
    opened.writer.join().expect("writer");
    let _ = (pb, pc);
}
