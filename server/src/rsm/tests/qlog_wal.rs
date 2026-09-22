//! Phase A3a: the per-queue qlog as a WAL — durable AT each store commit, with a
//! recorded durable index that recovery reconciles against
//! (`ALICE_PGLESS_NEWARCH.md` §5).
//!
//! A2 fsync'd the qlog only at the durable point (~1 s); while the raft log is
//! the WAL that is fine. A3b will remove the raft-log payload, so the qlog must
//! first become independently crash-survivable AT the store commit. This file
//! proves the two halves of that, against a real applier + store + qlog:
//!
//! - **[`a_store_commit_fsyncs_the_qlog_and_records_its_durable_index`]** — a
//!   store commit (NO durable point) fsyncs the qlog and records
//!   `meta::QLOG_DURABLE_INDEX` at the last Append's entry index — in the LIVE
//!   store: Phase C keeps `meta` in RAM and persists it only at a durable
//!   point, so a reopen finds the index the last durable point recorded while
//!   the qlog, fsync'd at the later commit, is AHEAD of it; reopening reconciles
//!   (the guard does not fire) and the acked records are readable from the qlog
//!   after the reopen.
//! - **[`recovery_refuses_when_the_qlog_is_behind_the_recorded_durable_index`]**
//!   — the reconciliation is NOT vacuous: a store that records a qlog-durable
//!   index ABOVE the qlog's real tail (the shape of a lost committed record once
//!   the qlog is the sole payload store) makes `Applier::open` REFUSE with a
//!   `Disagreement` naming NA-QLOG-I1, rather than silently serving a hole.
//!
//! The end-to-end kill test (SIGKILL at `qlog.record_written` /
//! `qlog.record_fsynced`) is the crash matrix (`test/raft/crash`), which drives
//! this same path over HTTP with `QUEEN_RAFT_QLOG=1`.

use std::sync::Arc;

use super::apply::{
    cfg, group_meta, hashes, queue_config, seg_opts, store_opts, tmp_dir, uuid, Build, BASE_US,
    QUEUE, TENANT,
};
use crate::rsm::apply::{Applier, ApplyConfig, ApplyError, Committed, NoNotify};
use crate::rsm::effect::{Effect, Pid};
use crate::rsm::qlog::set::QLogReader;
use crate::rsm::store::{meta, HeedStore, Store, TypedReads, TypedWrites, Writes};

const G: &str = "g1";

/// The shared apply-suite config with the qlog knob ON.
fn cfg_qlog() -> ApplyConfig {
    ApplyConfig {
        qlog: true,
        ..cfg()
    }
}

/// A fixed three-entry script: setup (entry 1, no Append), then two Appends
/// (entries 2 and 3). Deterministic and clock-free. The highest Append entry
/// index is 3, so after a commit the qlog-durable index MUST be exactly 3 — the
/// value the reconciliation checks against.
fn append_script() -> Vec<Committed> {
    let pid: Pid = 1;
    let bucket = (pid % 8) as u16;
    let (t1, t2, t3) = (BASE_US + 1_000, BASE_US + 2_000, BASE_US + 3_000);

    let e1 = Build::new(t1, 1, 0)
        .cmd(vec![
            Effect::QueueUpsert {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                cfg: queue_config(t1),
            },
            Effect::GroupUpsert {
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                group: G.into(),
                meta: group_meta(0, t1),
            },
            Effect::PartitionCreate {
                pid,
                uuid: uuid(pid),
                tenant: TENANT.into(),
                queue: QUEUE.into(),
                partition: "p0".into(),
                created_at_us: t1,
            },
        ])
        .at(1, 1);

    let e2 = Build::new(t2, 2, 100)
        .cmd(vec![Effect::Append {
            pid,
            bucket,
            base_offset: 0,
            count: 2,
            created_at_us: t2,
            hashes: hashes(0xAB, 2),
            blob: vec![0xAB; 24 * 2],
        }])
        .at(2, 1);

    let e3 = Build::new(t3, 2, 200)
        .cmd(vec![Effect::Append {
            pid,
            bucket,
            base_offset: 2,
            count: 2,
            created_at_us: t3,
            hashes: hashes(0xCD, 2),
            blob: vec![0xCD; 24 * 2],
        }])
        .at(3, 1);

    vec![e1, e2, e3]
}

/// `meta::QLOG_DURABLE_INDEX` as a reader sees it LIVE (Phase C: `meta` is a
/// RAM table, read-uncommitted), from a thread of its own — the caller's thread
/// holds the applier's write transaction, and LMDB allows one per thread.
fn live_qlog_durable_index(store: &HeedStore) -> u64 {
    std::thread::scope(|sc| {
        sc.spawn(|| {
            store
                .read(|r| Ok(r.meta_u64(meta::QLOG_DURABLE_INDEX)?.unwrap_or(0)))
                .expect("read the live qlog durable index")
        })
        .join()
        .expect("reader thread")
    })
}

#[test]
fn a_store_commit_fsyncs_the_qlog_and_records_its_durable_index() {
    let dir = tmp_dir("qlog-wal-commit");
    std::fs::create_dir_all(&dir).expect("dir");
    let script = append_script();

    // Entries 1–2 through a DURABLE point — Phase C's only commit that persists
    // `meta`, `QLOG_DURABLE_INDEX` included — then entry 3 through a
    // NON-DURABLE store commit only. A3a fsyncs the qlog inside `commit_inner`
    // (before the store commit) and records the durable index, so the qlog is
    // crash-survivable at commit, not one durable point (~1 s) later.
    {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("open store");
        let (mut a, _rec) = Applier::open(
            &store,
            &dir.join("seg"),
            seg_opts(),
            cfg_qlog(),
            Arc::new(NoNotify),
        )
        .expect("open applier (qlog on)");
        a.apply(&script[0]).expect("apply entry 1");
        a.apply(&script[1]).expect("apply entry 2");
        assert_eq!(a.durable_point().expect("durable point"), 2);
        a.apply(&script[2]).expect("apply entry 3");
        a.commit().expect("store commit");
        assert_eq!(
            live_qlog_durable_index(&store),
            3,
            "a store commit must record the qlog durable index at the last Append (entry 3), \
             with no durable point"
        );
        drop(a);
        store.close();
    }

    // Reopen: the store is back EXACTLY at the durable point (entry 2) — the
    // plain commit persisted nothing — and so is its record of the qlog. The
    // qlog is AHEAD of that record (entry 3's record, fsync'd at the commit),
    // which is the benign direction: it is the WAL recovery replays from. So
    // `Applier::open` reconciles the reopened qlog's tail against it and
    // succeeds.
    {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("reopen store");
        let (applied, qdi) = store
            .read(|r| {
                Ok((
                    r.applied_index()?,
                    r.meta_u64(meta::QLOG_DURABLE_INDEX)?.unwrap_or(0),
                ))
            })
            .expect("read the reopened recovery point");
        assert_eq!(
            (applied, qdi),
            (2, 2),
            "the store reopens at the durable point with the qlog durable index it recorded; \
             the plain commit of entry 3 persisted nothing"
        );

        let (a, rec) = Applier::open(
            &store,
            &dir.join("seg"),
            seg_opts(),
            cfg_qlog(),
            Arc::new(NoNotify),
        )
        .expect("reopen reconciles: the qlog tail (3) is not behind the recorded index (2)");
        assert_eq!(
            rec.applied_index, 2,
            "reopened at the durable point (applied {})",
            rec.applied_index
        );

        // Both acked records are readable from the qlog AFTER the reopen —
        // entry 3's with no durable point covering it: the store commit
        // fsync'd it. The A3a property that lets A3b drop the raft-log payload.
        let reader = a.qlog_reader().expect("qlog reader is on");
        let qid = QLogReader::queue_id_of(TENANT, QUEUE);
        let r0 = reader
            .read_owned(qid, 1, 0)
            .expect("read")
            .expect("the entry-2 record is readable from the reopened qlog");
        assert_eq!((r0.base_offset, r0.count, r0.seq), (0, 2, 2));
        let r2 = reader
            .read_owned(qid, 1, 2)
            .expect("read")
            .expect("the entry-3 record is readable from the reopened qlog");
        assert_eq!((r2.base_offset, r2.count, r2.seq), (2, 2, 3));

        drop(a);
        store.close();
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recovery_refuses_when_the_qlog_is_behind_the_recorded_durable_index() {
    let dir = tmp_dir("qlog-wal-behind");
    std::fs::create_dir_all(&dir).expect("dir");
    let script = append_script();

    // A normal qlog-on run through a durable point: the store is durably at
    // entry 3 and the qlog's real tail is entry 3.
    {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("open store");
        let (mut a, _rec) = Applier::open(
            &store,
            &dir.join("seg"),
            seg_opts(),
            cfg_qlog(),
            Arc::new(NoNotify),
        )
        .expect("open applier (qlog on)");
        for c in &script {
            a.apply(c).expect("apply");
        }
        assert_eq!(a.durable_point().expect("durable point"), 3);
        drop(a);
        store.close();
    }

    // Forge the store's record of qlog durability to CLAIM a record the qlog does
    // not have (well past entry 3). This is exactly the on-disk shape of a lost
    // committed record once A3b makes the qlog the sole payload store: the store
    // says "durable through 103", the qlog only has up to 3. Phase C: `meta` is
    // persisted only by a DURABLE commit, so that is what the forgery takes.
    {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("reopen store");
        {
            let mut w = store.write().expect("write handle");
            w.set_meta_u64(meta::QLOG_DURABLE_INDEX, 103)
                .expect("forge qlog durable index");
            w.durable_commit()
                .expect("durable commit of the forged meta");
        }
        store.close();
    }

    // Reopen: the reconciliation guard MUST fire — the qlog's durable tail (3) is
    // BEHIND the store's recorded index (103), so a committed record is missing.
    // `Applier::open` refuses rather than serve a hole.
    {
        let store = HeedStore::open(&dir.join("store"), &store_opts()).expect("reopen store");
        // Map the outcome to an owned value INSIDE the match, so the applier (and
        // its borrow of `store`) is dropped before `store.close()` moves it.
        let outcome: Result<(), (bool, String)> = match Applier::open(
            &store,
            &dir.join("seg"),
            seg_opts(),
            cfg_qlog(),
            Arc::new(NoNotify),
        ) {
            Ok((a, _rec)) => {
                drop(a);
                Ok(())
            }
            Err(ApplyError::Disagreement { detail }) => Err((true, detail)),
            Err(other) => Err((false, other.to_string())),
        };
        store.close();
        match outcome {
            Err((true, detail)) => assert!(
                detail.contains("NA-QLOG-I1") && detail.contains("BEHIND"),
                "the refusal names the invariant and the direction: {detail}"
            ),
            Err((false, other)) => {
                panic!("expected a qlog reconciliation Disagreement, got {other}")
            }
            Ok(()) => panic!(
                "reopen accepted a qlog behind the store's recorded durable index — \
                 the reconciliation is vacuous"
            ),
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}
