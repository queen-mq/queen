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

use super::set::{EntryScan, QLogSet, SYSTEM_QUEUE_ID};
use super::{EntryInput, EntryRecord, QLogOptions, RecordInput, WriteRecord};
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

fn open_repl(
    dir: &Path,
    store: Arc<HeedStore>,
    writer_pipeline: bool,
) -> LocalReplicator<HeedStore> {
    LocalReplicator::open(
        store,
        open_cfg(dir, writer_pipeline),
        Arc::new(NoWaker),
        Arc::new(SystemClock),
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
async fn recover_from_the_queue_logs_alone(tag: &str, writer_pipeline: bool) {
    let td = TmpDir::new(tag);
    let dir = td.path();
    let snapshot = dir.join("store-at-k");
    let mut g = Gen::new();

    // Phase 1: K entries, clean shutdown (a durable point at K), snapshot.
    let (pa, pb, k) = {
        let store = open_store(dir);
        let repl = open_repl(dir, store, writer_pipeline);
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
        let repl = open_repl(dir, store, writer_pipeline);
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
        let repl = open_repl(dir, store, writer_pipeline);
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
        let repl = open_repl(dir, store, writer_pipeline);
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
        let repl = open_repl(dir, store, writer_pipeline);
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
    recover_from_the_queue_logs_alone("recover", false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_logs_alone_recover_acked_work_pipelined() {
    recover_from_the_queue_logs_alone("recover-wp", true).await;
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
