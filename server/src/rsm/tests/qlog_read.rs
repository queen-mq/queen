//! Phase A2: the qlog READ path (`QUEEN_RAFT_QLOG`), against the real applier, a
//! real store and real segment files.
//!
//! A1 proved the WRITE byte-faithful (`qlog_shadow.rs`). A2 switches the READS
//! over — pop reads the payload from the queue log, and the
//! `DEDUP_INDEX=segment` dedup authority reads the per-append hashes from it —
//! and the gate is BEHAVIOURAL IDENTITY: with the knob on, the qlog read returns
//! exactly what the still-written segment/txns read would. Three properties:
//!
//! - **read-match** — [`qlog_pop_and_dedup_reads_match_the_segment_path`]: over a
//!   workload with duplicate hashes, across a rolled file, the qlog pop bytes and
//!   the qlog committed-dedup frames + O(claimed) claim walk equal the segment
//!   reader's, frame for frame and byte for byte.
//! - **reopen** — [`a_reopened_qlog_serves_the_same_reads`]: write, drop the
//!   applier, close the store, reopen with the knob on — the qlog map is rebuilt
//!   at `Applier::open` and the pop + dedup reads still match the (also reopened)
//!   segment reader.
//! - **O(claimed)** — [`the_qlog_claim_walk_is_bounded_and_committed`]: a lagging
//!   cursor's claim walk reads only the ≤ budget frames it consumes (not the
//!   whole window), and never a record past the committed tail (the exactly-once
//!   invariant).

use std::collections::HashMap;
use std::sync::Arc;

use super::apply::{
    cfg, group_meta, hashes as gen_hashes, queue_config, seg_opts, uuid, Build, Node, BASE_US,
    TENANT,
};
use crate::rsm::apply::{Applier, ApplyConfig, Committed, NoNotify};
use crate::rsm::effect::Effect;
use crate::rsm::planner::bucket_of;
use crate::rsm::qlog::set::{QLogReader, QLogSet};
use crate::rsm::segments::Reader as SegReader;
use crate::rsm::store::{HeedStore, Store, TypedReads};

/// Two queues under one tenant, so more than one log is open.
const Q1: &str = "orders";
const Q2: &str = "events";

/// A single-hash seed reused across several appends so the SAME 16-byte hash
/// value recurs at different offsets (the "incl. duplicates" the dedup read must
/// carry faithfully).
const DUP_SEED: u64 = 0xD00D;

/// What one appended message must read back as: exactly what the effect carried.
struct Expected {
    queue: &'static str,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: Vec<u8>,
    blob: Vec<u8>,
}

/// A deterministic append workload across two queues and four partitions, big
/// enough to roll an 8 KiB file (so the reads span sealed + active), with some
/// duplicate hash values. Index 1 creates the queues, a group each and the four
/// partitions; indices 2.. append.
fn build_workload() -> (Vec<Committed>, Vec<Expected>) {
    // (queue, pid, partition name). Q1 → pids 1,2 ; Q2 → pids 3,4.
    let parts: [(&'static str, u64, &str); 4] =
        [(Q1, 1, "p0"), (Q1, 2, "p1"), (Q2, 3, "p0"), (Q2, 4, "p1")];
    let name_of = |pid: u64| -> &'static str {
        parts
            .iter()
            .find(|(_, p, _)| *p == pid)
            .map(|(_, _, n)| *n)
            .expect("known pid")
    };

    let mut entries: Vec<Committed> = Vec::new();
    let mut expected: Vec<Expected> = Vec::new();
    let mut ids = 0u64;

    // --- setup entry (index 1, pid_base 1) --------------------------------
    let now = BASE_US + 1_000;
    let mut setup = vec![
        Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: Q1.into(),
            cfg: queue_config(now),
        },
        Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: Q2.into(),
            cfg: queue_config(now),
        },
        Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: Q1.into(),
            group: "g1".into(),
            meta: group_meta(1, now),
        },
        Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: Q2.into(),
            group: "g1".into(),
            meta: group_meta(2, now),
        },
    ];
    for (q, pid, part) in parts {
        setup.push(Effect::PartitionCreate {
            pid,
            uuid: uuid(pid),
            tenant: TENANT.into(),
            queue: q.into(),
            partition: part.into(),
            created_at_us: now,
        });
    }
    let b = Build::new(now, 1, ids).cmd(setup);
    ids += 1;
    entries.push(b.at(1, 1));

    // --- append entries (indices 2..) -------------------------------------
    let mut last_off: HashMap<u64, i64> = parts.iter().map(|(_, pid, _)| (*pid, -1i64)).collect();
    let n_appends = 80u64;
    for i in 0..n_appends {
        let idx = 2 + i;
        let now = BASE_US + idx as i64 * 1_000;
        let mut b = Build::new(now, 5, ids);

        // Usually one append; every 7th carries two (two partitions, one entry),
        // every 11th carries two GAPLESS appends to one partition.
        let targets: Vec<(&'static str, u64)> = if i % 7 == 6 {
            vec![(Q1, 1), (Q2, 3)]
        } else if i % 11 == 10 {
            vec![(Q1, 2), (Q1, 2)]
        } else {
            let (q, pid, _) = parts[(i % 4) as usize];
            vec![(q, pid)]
        };

        let mut effects: Vec<Effect> = Vec::new();
        for (k, &(q, pid)) in targets.iter().enumerate() {
            let count = 1 + ((i + k as u64) % 4) as u32;
            let base = (last_off[&pid] + 1) as u64;
            *last_off.get_mut(&pid).expect("pid tracked") += count as i64;
            // Every 5th append reuses DUP_SEED with count 1, so `hash_at(DUP_SEED,
            // 0)` recurs across frames and partitions — the duplicate case.
            let h = if i % 5 == 4 && count >= 1 {
                gen_hashes(DUP_SEED, count)
            } else {
                gen_hashes(i * 131 + k as u64 * 17 + 1, count)
            };
            let blob = vec![((i + k as u64) & 0xFF) as u8; 200 + 16 * count as usize];
            // The REAL bucket the leader would carry (`bucket_of`), so the
            // segment read (which folds this same logical bucket) finds the frame
            // — the read-match compares the two byte stores, so both must file
            // under the same bucket.
            effects.push(Effect::Append {
                pid,
                bucket: bucket_of(TENANT, q, name_of(pid)),
                base_offset: base,
                count,
                created_at_us: now,
                hashes: h.clone(),
                blob: blob.clone(),
            });
            expected.push(Expected {
                queue: q,
                pid,
                base_offset: base,
                count,
                created_at_us: now,
                hashes: h,
                blob,
            });
        }
        b = b.cmd(effects);
        ids += 1;
        entries.push(b.at(idx, 1));
    }

    (entries, expected)
}

/// Open an applier on `node` with the shadow knob set as asked; everything else
/// is the shared apply-suite config.
fn open_applier(node: &Node, qlog_on: bool) -> Applier<'_, HeedStore> {
    let cfg = ApplyConfig {
        qlog: qlog_on,
        ..cfg()
    };
    let (a, _rec) = Applier::open(
        node.store(),
        &node.seg_dir(),
        seg_opts(),
        cfg,
        Arc::new(NoNotify),
    )
    .expect("open applier");
    a
}

/// The committed read context of a partition, read from the store: the segment
/// bucket + sealed-file list (for the segment reader) and the committed offset
/// bound + queue id (shared by both readers).
struct PartCtx {
    bucket: u16,
    committed_end: u64,
    sealed: Vec<u32>,
    queue_id: u64,
}

fn part_ctx(store: &HeedStore, pid: u64) -> PartCtx {
    store
        .read(|r| {
            let p = r.partition(pid)?.expect("partition row");
            let bucket = bucket_of(&p.tenant, &p.queue, &p.partition);
            let queue_id = QLogSet::queue_id_of(&p.tenant, &p.queue);
            let committed_end = (p.last_offset + 1).max(0) as u64;
            let mut sealed = Vec::new();
            r.scan_partition_files(pid, usize::MAX, &mut |f| {
                sealed.push(f);
                true
            })?;
            Ok(PartCtx {
                bucket,
                committed_end,
                sealed,
                queue_id,
            })
        })
        .expect("read part ctx")
}

/// The committed dedup frames as `(base, end_exclusive, created, hashes)`, from
/// the segment reader.
fn seg_committed(seg: &SegReader, ctx: &PartCtx, pid: u64) -> Vec<(u64, u64, i64, Vec<u8>)> {
    seg.committed_dedup_rows(ctx.bucket, pid, 0, ctx.committed_end, &ctx.sealed)
        .expect("segment committed rows")
        .into_iter()
        .map(|f| (f.base_offset, f.end, f.created_at_us, f.hashes))
        .collect()
}

/// The same, from the qlog reader.
fn qlog_committed(ql: &QLogReader, ctx: &PartCtx, pid: u64) -> Vec<(u64, u64, i64, Vec<u8>)> {
    ql.committed_frames(ctx.queue_id, pid, 0, ctx.committed_end, true)
        .expect("qlog committed frames")
        .into_iter()
        .map(|f| (f.base_offset, f.end, f.created_at_us, f.hashes))
        .collect()
}

/// The claim walk of `pid` from `from`, as `(base, end_incl, created, hashes)`,
/// from the segment reader (cb always continues).
fn seg_claim(seg: &SegReader, ctx: &PartCtx, pid: u64, from: u64) -> Vec<(u64, u64, i64, Vec<u8>)> {
    let mut out = Vec::new();
    seg.claim_frames(
        ctx.bucket,
        pid,
        from,
        ctx.committed_end,
        &ctx.sealed,
        true,
        &mut |base, end_incl, created, hashes| {
            out.push((base, end_incl, created, hashes.unwrap_or_default()));
            true
        },
    )
    .expect("segment claim walk");
    out
}

/// The same, from the qlog reader.
fn qlog_claim(
    ql: &QLogReader,
    ctx: &PartCtx,
    pid: u64,
    from: u64,
) -> Vec<(u64, u64, i64, Vec<u8>)> {
    let mut out = Vec::new();
    ql.claim_frames(
        ctx.queue_id,
        pid,
        from,
        ctx.committed_end,
        true,
        &mut |base, end_incl, created, hashes| {
            out.push((base, end_incl, created, hashes.unwrap_or_default()));
            true
        },
    )
    .expect("qlog claim walk");
    out
}

#[test]
fn qlog_pop_and_dedup_reads_match_the_segment_path() {
    let (entries, expected) = build_workload();
    let node = Node::new("qlog-read-match");

    // Apply everything, then flush + fsync the whole thing (durable point), so
    // every append is committed in BOTH the segments and the qlog. The applier
    // stays alive (idle) while we read through its cloned reader handles, off the
    // same live file set + logs — the facade's arrangement, minus the threads.
    let mut a = open_applier(&node, true);
    for (i, c) in entries.iter().enumerate() {
        a.apply(c).expect("apply");
        if i % 10 == 4 {
            a.commit().expect("commit");
        }
    }
    a.durable_point().expect("durable point");
    let seg = a.reader();
    let ql = a.qlog_reader().expect("qlog reader is on");

    // Per-pid committed context, read once from the committed store.
    let pids: Vec<u64> = {
        let mut v: Vec<u64> = expected.iter().map(|e| e.pid).collect();
        v.sort_unstable();
        v.dedup();
        v
    };
    let ctxs: HashMap<u64, PartCtx> = pids
        .iter()
        .map(|&pid| (pid, part_ctx(node.store(), pid)))
        .collect();

    // A roll actually happened (the reads cross sealed + active), or the test is
    // vacuous. Both a qlog file and a segment run rolled under the 8 KiB options.
    let rolled = pids.iter().any(|pid| !ctxs[pid].sealed.is_empty());
    assert!(
        rolled,
        "no partition rolled a sealed file; the reads never left the active index"
    );

    // 1. POP BYTES: the qlog record equals the segment frame, byte for byte.
    for e in &expected {
        let ctx = &ctxs[&e.pid];
        let seg_frame = seg
            .read_at_within(ctx.bucket, e.pid, e.base_offset, &ctx.sealed, None)
            .expect("segment read")
            .expect("segment frame present");
        let qrec = ql
            .read_owned(ctx.queue_id, e.pid, e.base_offset)
            .expect("qlog read")
            .expect("qlog record present");
        assert_eq!(
            qrec.base_offset, seg_frame.base_offset,
            "base (pid {})",
            e.pid
        );
        assert_eq!(
            qrec.count, seg_frame.count,
            "count (pid {} base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            qrec.created_at_us, seg_frame.created_at_us,
            "created (pid {} base {})",
            e.pid, e.base_offset
        );
        // The payload IS the segment blob, and both equal the effect's blob.
        assert_eq!(
            qrec.payload, seg_frame.blob,
            "payload==blob (pid {} base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            qrec.payload, e.blob,
            "payload==effect (pid {} base {})",
            e.pid, e.base_offset
        );
        // The dedup hashes of the record match the segment frame's, and the
        // effect's — including the duplicate seed's recurring value.
        assert_eq!(
            qrec.hashes, seg_frame.hashes,
            "hashes==segment (pid {} base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            qrec.hashes, e.hashes,
            "hashes==effect (pid {} base {})",
            e.pid, e.base_offset
        );
    }

    // 2. DEDUP WHOLE-WINDOW: the committed dedup frames match, frame for frame.
    for &pid in &pids {
        let ctx = &ctxs[&pid];
        assert_eq!(
            qlog_committed(&ql, ctx, pid),
            seg_committed(&seg, ctx, pid),
            "committed dedup frames differ for pid {pid}"
        );
    }

    // 3. DEDUP CLAIM WALK: the O(claimed) forward walk matches over a sub-range
    //    (a lagging cursor), across the roll and including duplicate hashes.
    for &pid in &pids {
        let ctx = &ctxs[&pid];
        for from in [0u64, 1, ctx.committed_end / 2] {
            if from > ctx.committed_end {
                continue;
            }
            assert_eq!(
                qlog_claim(&ql, ctx, pid, from),
                seg_claim(&seg, ctx, pid, from),
                "claim walk differs for pid {pid} from {from}"
            );
        }
    }
}

#[test]
fn a_reopened_qlog_serves_the_same_reads() {
    let (entries, expected) = build_workload();
    let dir = super::apply::tmp_dir("qlog-read-reopen");

    // First life: apply everything, durable point, then close.
    {
        let mut node = Node::at(dir.clone());
        node.keep();
        {
            let mut a = open_applier(&node, true);
            for c in &entries {
                a.apply(c).expect("apply");
            }
            a.durable_point().expect("durable point");
        }
        node.close();
    }

    // Second life: reopen the SAME directory with the knob on. `Applier::open`
    // reopens every existing `q<id>/` (torn-tail truncate + `.qidx` rebuild)
    // before it serves a read.
    let node = Node::at(dir);
    let a = open_applier(&node, true);
    let seg = a.reader();
    let ql = a.qlog_reader().expect("qlog reader is on");

    let pids: Vec<u64> = {
        let mut v: Vec<u64> = expected.iter().map(|e| e.pid).collect();
        v.sort_unstable();
        v.dedup();
        v
    };

    // The map was rebuilt at open, before any new write.
    for &pid in &pids {
        let qid = ql.log_id_for(part_ctx(node.store(), pid).queue_id, pid);
        assert!(ql.has_queue(qid), "queue log {qid} not reopened for pid {pid}");
    }

    // The reads still match the segment reader after the restart: the active
    // index was rebuilt by scanning, and the sealed `.qidx` were remapped.
    for e in &expected {
        let ctx = part_ctx(node.store(), e.pid);
        let seg_frame = seg
            .read_at_within(ctx.bucket, e.pid, e.base_offset, &ctx.sealed, None)
            .expect("segment read")
            .expect("segment frame present");
        let qrec = ql
            .read_owned(ctx.queue_id, e.pid, e.base_offset)
            .expect("qlog read")
            .expect("qlog record present after reopen");
        assert_eq!(
            qrec.payload, seg_frame.blob,
            "reopen payload (pid {} base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            qrec.hashes, seg_frame.hashes,
            "reopen hashes (pid {} base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            qrec.payload, e.blob,
            "reopen payload==effect (pid {} base {})",
            e.pid, e.base_offset
        );
    }
    for &pid in &pids {
        let ctx = part_ctx(node.store(), pid);
        assert_eq!(
            qlog_committed(&ql, &ctx, pid),
            seg_committed(&seg, &ctx, pid),
            "reopened committed dedup frames differ for pid {pid}"
        );
    }
}

#[test]
fn an_applied_but_uncommitted_append_reads_from_the_qlog_like_the_segment() {
    // THE difffuzz bug (qlog off vs on, `$.messages.length`): a pop is rendered
    // right after apply, BEFORE the store commit, and it can claim (through the
    // planner's overlay) an offset an in-flight push appended in the SAME
    // not-yet-committed commit window. The segment payload is written PER ENTRY
    // (`segments.flush_writes`, before the leader answers), so the segment pop
    // render serves it — but the qlog was only written at commit, so the qlog
    // pop render read `None` for that offset and dropped one message (5 vs 4).
    //
    // This drives that exact boundary at the read level the render depends on:
    // apply appends and take NO commit/durable at all, so every append is
    // applied-but-uncommitted (the in-flight case), then assert every one reads
    // IDENTICALLY from the qlog (`read_owned`, the pop payload read) and the
    // segments (`read_at_within`). It FAILS before the fix (the qlog was never
    // written without a commit) and PASSES after (written per entry).
    let now0 = BASE_US + 1_000;
    let setup = vec![
        Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: Q1.into(),
            cfg: queue_config(now0),
        },
        Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: Q1.into(),
            group: "g1".into(),
            meta: group_meta(1, now0),
        },
        Effect::PartitionCreate {
            pid: 1,
            uuid: uuid(1),
            tenant: TENANT.into(),
            queue: Q1.into(),
            partition: "p0".into(),
            created_at_us: now0,
        },
    ];
    let bucket = bucket_of(TENANT, Q1, "p0");
    let qid = QLogSet::queue_id_of(TENANT, Q1);

    let node = Node::new("qlog-read-inflight");
    let mut a = open_applier(&node, true);
    a.apply(&Build::new(now0, 1, 0).cmd(setup).at(1, 1))
        .expect("apply setup");

    // A handful of small appends (no roll, so the committed `partition_files`
    // list is irrelevant and both readers serve from their ACTIVE index), and
    // NO commit or durable point after any of them — the whole run stays in the
    // open store transaction, exactly the window a pop renders in.
    let mut expected: Vec<(u64, u32, Vec<u8>, Vec<u8>)> = Vec::new(); // (base, count, hashes, blob)
    let mut base = 0u64;
    for i in 0..6u64 {
        let idx = 2 + i;
        let now = BASE_US + idx as i64 * 1_000;
        let count = 1 + (i % 3) as u32;
        let h = gen_hashes(i + 1, count);
        let blob = vec![(i & 0xFF) as u8; 64 + 16 * count as usize];
        let eff = Effect::Append {
            pid: 1,
            bucket,
            base_offset: base,
            count,
            created_at_us: now,
            hashes: h.clone(),
            blob: blob.clone(),
        };
        a.apply(&Build::new(now, 2, i + 1).cmd(vec![eff]).at(idx, 1))
            .expect("apply append");
        expected.push((base, count, h, blob));
        base += count as u64;
    }
    // Deliberately NO a.commit() / a.durable_point(): everything is applied but
    // uncommitted, the in-flight window a pop is answered in.

    let seg = a.reader();
    let ql = a.qlog_reader().expect("qlog reader is on");

    for (base, count, hashes, blob) in &expected {
        // The segment pop read (no committed bound; the claim guarantees the
        // offset). `sealed` is empty — nothing rolled — so the active index
        // serves it, exactly as it does for the render right after apply.
        let seg_frame = seg
            .read_at_within(bucket, 1, *base, &[], None)
            .expect("segment read")
            .expect("segment frame present (flush_writes wrote it per entry)");
        // The qlog pop read MUST also serve it, though nothing was committed.
        let qrec = ql
            .read_owned(qid, 1, *base)
            .expect("qlog read")
            .expect("qlog record present for an applied-but-uncommitted append");
        assert_eq!(qrec.count, *count, "count (base {base})");
        assert_eq!(
            qrec.count, seg_frame.count,
            "count vs segment (base {base})"
        );
        assert_eq!(qrec.payload, *blob, "payload (base {base})");
        assert_eq!(
            qrec.payload, seg_frame.blob,
            "payload vs segment (base {base})"
        );
        assert_eq!(qrec.hashes, *hashes, "hashes (base {base})");
        assert_eq!(
            qrec.hashes, seg_frame.hashes,
            "hashes vs segment (base {base})"
        );
    }
}

#[test]
fn the_qlog_claim_walk_is_bounded_and_committed() {
    // One partition, many single-message appends, so the window is wide and each
    // frame is one offset — easy to count.
    let mut entries: Vec<Committed> = Vec::new();
    let now0 = BASE_US + 1_000;
    let setup = vec![
        Effect::QueueUpsert {
            tenant: TENANT.into(),
            queue: Q1.into(),
            cfg: queue_config(now0),
        },
        Effect::GroupUpsert {
            tenant: TENANT.into(),
            queue: Q1.into(),
            group: "g1".into(),
            meta: group_meta(1, now0),
        },
        Effect::PartitionCreate {
            pid: 1,
            uuid: uuid(1),
            tenant: TENANT.into(),
            queue: Q1.into(),
            partition: "p0".into(),
            created_at_us: now0,
        },
    ];
    entries.push(Build::new(now0, 1, 0).cmd(setup).at(1, 1));

    let bucket = bucket_of(TENANT, Q1, "p0");
    let n = 50u64; // frames (offsets) in the partition
    for i in 0..n {
        let idx = 2 + i;
        let now = BASE_US + idx as i64 * 1_000;
        let h = gen_hashes(i + 1, 1);
        let blob = vec![(i & 0xFF) as u8; 220];
        let eff = Effect::Append {
            pid: 1,
            bucket,
            base_offset: i,
            count: 1,
            created_at_us: now,
            hashes: h,
            blob,
        };
        // Only pid 1 was created (pid_base 1 → next_pid 2), so the append entries
        // assign no pid and carry pid_base 2.
        entries.push(Build::new(now, 2, i + 1).cmd(vec![eff]).at(idx, 1));
    }

    let node = Node::new("qlog-claim-bounded");
    let mut a = open_applier(&node, true);
    for c in &entries {
        a.apply(c).expect("apply");
    }
    a.durable_point().expect("durable point");
    let ql = a.qlog_reader().expect("qlog reader is on");
    let ctx = part_ctx(node.store(), 1);
    assert_eq!(ctx.committed_end, n, "all {n} offsets committed");
    assert!(
        !ctx.sealed.is_empty(),
        "the window rolled at least one sealed file"
    );

    // (a) O(claimed): from a LAGGING cursor near the head, cap the walk at a
    //     small budget and assert it reads only that many frames — not the whole
    //     window. Each cb call is one frame read (and, want_hashes, one pread).
    let budget = 5usize;
    let lag_from = 3u64;
    let mut read = 0usize;
    ql.claim_frames(
        ctx.queue_id,
        1,
        lag_from,
        ctx.committed_end,
        true,
        &mut |_b, _e, _c, h| {
            assert!(h.is_some(), "want_hashes was set");
            read += 1;
            read < budget // stop once the budget is met (false ends the walk)
        },
    )
    .expect("qlog claim walk");
    assert_eq!(
        read, budget,
        "the claim walk read {read} frames for a budget of {budget}; it is not O(claimed)"
    );
    // The whole committed window is far larger, so the bound above is real.
    let whole = ql
        .committed_frames(ctx.queue_id, 1, 0, ctx.committed_end, false)
        .expect("committed frames");
    assert_eq!(
        whole.len() as u64,
        n,
        "the committed window holds all {n} frames"
    );
    assert!(
        whole.len() > budget * 4,
        "the window is not meaningfully larger than the budget"
    );

    // (b) The committed bound (exactly-once): pretend the tail is UNCOMMITTED by
    //     passing a committed_end below the true tail. The walk must stop there
    //     and NEVER return a record whose end reaches past it — the overlay
    //     covers those. A full walk (cb always true) visits exactly the frames
    //     below the bound.
    let bound = 20u64;
    let mut seen: Vec<u64> = Vec::new();
    ql.claim_frames(
        ctx.queue_id,
        1,
        0,
        bound,
        false,
        &mut |_base, end_incl, _c, _h| {
            seen.push(end_incl + 1); // exclusive end
            true
        },
    )
    .expect("qlog claim walk (bounded)");
    assert_eq!(
        seen.len() as u64,
        bound,
        "the walk read {} frames up to the bound {bound}",
        seen.len()
    );
    assert!(
        seen.iter().all(|&end_excl| end_excl <= bound),
        "the walk returned a record past the committed bound {bound}: {seen:?}"
    );

    // The same bound holds for the whole-window read.
    let bounded = ql
        .committed_frames(ctx.queue_id, 1, 0, bound, true)
        .expect("committed frames bounded");
    assert_eq!(
        bounded.len() as u64,
        bound,
        "committed_frames respected the bound"
    );
    assert!(
        bounded.iter().all(|f| f.end <= bound),
        "committed_frames returned a record past the bound"
    );
}
