//! Phase A1: the SHADOW per-queue log write (`QUEEN_RAFT_QLOG`), against the
//! real applier, a real store and real segment files.
//!
//! `ALICE_PGLESS_NEWARCH.md` §3/§5, Phase A1. Two properties, and they are the
//! whole point of the phase:
//!
//! - **match** — [`qlog_shadow_write_is_byte_identical_to_the_effect`]: with the
//!   knob ON, every `Append`'s qlog record round-trips byte-identical to what
//!   the effect carried (`pid`, `base_offset`, `count`, `created_at`, `hashes`,
//!   `payload`) — and `seq` is the entry index, `txn` is `None`. The effect is
//!   the single source both `segments.append` and the shadow consume (apply
//!   hands the SAME `hashes`/`blob` slices to each), so effect-equality is
//!   segment-equality; the shadow is faithful before A2 switches reads to it.
//! - **no-op** — [`qlog_knob_off_is_a_noop_and_the_shadow_perturbs_nothing`]:
//!   the same workload gives a byte-identical replicated digest with the knob
//!   OFF and ON (the shadow touches no replicated state), the OFF run creates no
//!   `qlog/` directory (today's exact behaviour), and the ON run does (so the
//!   digest equality is not vacuous).
//!
//! Everything reuses the throwaway-node harness of [`super::apply`].

use std::collections::HashMap;
use std::sync::Arc;

use super::apply::{
    cfg, group_meta, hashes as gen_hashes, queue_config, seg_opts, uuid, Build, Node, BASE_US,
    TENANT,
};
use crate::rsm::apply::{Applier, ApplyConfig, Committed, NoNotify, StateDigest};
use crate::rsm::effect::Effect;
use crate::rsm::qlog::set::QLogSet;
use crate::rsm::store::HeedStore;

/// Two queues under one tenant, so the registry holds more than one log.
const Q1: &str = "orders";
const Q2: &str = "events";

/// What one appended message must read back as: exactly what the effect carried.
struct Expected {
    queue: &'static str,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: Vec<u8>,
    blob: Vec<u8>,
    /// The entry index the append was in — the qlog `seq`.
    seq: u64,
}

/// A deterministic append workload across two queues and four partitions, and
/// the record each append should produce. Index 1 creates the queues, a group
/// each and the four partitions; indices 2.. append. Some entries carry two
/// appends — to two partitions, or two gapless appends to one — so the "several
/// appends share one entry's `seq`" case is covered.
fn build_workload() -> (Vec<Committed>, Vec<Expected>) {
    // (queue, pid, partition name). Q1 → pids 1,2 ; Q2 → pids 3,4.
    let parts: [(&'static str, u64, &str); 4] =
        [(Q1, 1, "p0"), (Q1, 2, "p1"), (Q2, 3, "p0"), (Q2, 4, "p1")];

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
    // Each entry carries exactly one command, so one request id per entry; a
    // running counter keeps them distinct (`Build::cmd` mints `request_id(ids+1)`).
    let b = Build::new(now, 1, ids).cmd(setup);
    ids += 1;
    entries.push(b.at(1, 1));

    // --- append entries (indices 2..) -------------------------------------
    // Append entries assign no pids, so pid_base stays at 5 (four created above).
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
            let seed = i * 131 + k as u64 * 17 + 1;
            let base = (last_off[&pid] + 1) as u64;
            *last_off.get_mut(&pid).expect("pid tracked") += count as i64;
            let h = gen_hashes(seed, count);
            // Payloads large enough that the cumulative per-queue writes cross an
            // 8 KiB qlog file (seg_opts()), so the read path spans sealed+active.
            let blob = vec![((i + k as u64) & 0xFF) as u8; 200 + 16 * count as usize];
            effects.push(Effect::Append {
                pid,
                bucket: (pid % 8) as u16,
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
                seq: idx,
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

/// Apply every entry and take a final durable point (which flushes + fsyncs the
/// shadow), then read the node's replicated digest.
fn run_to_digest(node: &Node, entries: &[Committed], qlog_on: bool) -> StateDigest {
    {
        let mut a = open_applier(node, qlog_on);
        for c in entries {
            a.apply(c).expect("apply");
        }
        a.durable_point().expect("durable point");
    }
    node.digest()
}

#[test]
fn qlog_shadow_write_is_byte_identical_to_the_effect() {
    let (entries, expected) = build_workload();
    let node = Node::new("qlog-match");
    let mut a = open_applier(&node, true);

    for (i, c) in entries.iter().enumerate() {
        a.apply(c).expect("apply");
        // Exercise the commit-flush path partway (leaving the tail entries for
        // the durable-flush path below).
        if i % 10 == 4 {
            a.commit().expect("commit");
        }
    }
    // The tail entries buffered since the last commit are flushed + fsynced here.
    a.durable_point().expect("durable point");

    let set = a.qlog().expect("shadow qlog is on");
    for e in &expected {
        let qid = QLogSet::queue_id_of(TENANT, e.queue);
        let log = set
            .log(qid)
            .unwrap_or_else(|| panic!("no qlog opened for queue {}", e.queue));
        let loc = log.locate(e.pid, e.base_offset).unwrap_or_else(|| {
            panic!("record (pid {}, base {}) not located", e.pid, e.base_offset)
        });
        let rec = log
            .read_record(loc.file_id, loc.offset)
            .expect("read record");
        assert_eq!(rec.pid, e.pid, "pid");
        assert_eq!(
            rec.base_offset, e.base_offset,
            "base_offset (pid {})",
            e.pid
        );
        assert_eq!(
            rec.count, e.count,
            "count (pid {}, base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            rec.created_at_us, e.created_at_us,
            "created_at_us (pid {}, base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            rec.hashes, e.hashes,
            "hashes (pid {}, base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            rec.payload, e.blob,
            "payload (pid {}, base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            rec.txn, None,
            "txn is None in A1 (pid {}, base {})",
            e.pid, e.base_offset
        );
        assert_eq!(
            rec.seq, e.seq,
            "seq == entry index (pid {}, base {})",
            e.pid, e.base_offset
        );
    }

    // At least one queue rolled a file, so the match above crossed sealed +
    // active files, not just the active RAM index.
    let rolled = [Q1, Q2].iter().any(|q| {
        set.log(QLogSet::queue_id_of(TENANT, q))
            .is_some_and(|l| l.file_count() > 1)
    });
    assert!(
        rolled,
        "the workload never rolled a qlog file; the read stayed in the active index only"
    );
}

#[test]
fn qlog_knob_off_is_a_noop_and_the_shadow_perturbs_nothing() {
    let (entries, _expected) = build_workload();

    let node_off = Node::new("qlog-off");
    let digest_off = run_to_digest(&node_off, &entries, false);

    let node_on = Node::new("qlog-on");
    let digest_on = run_to_digest(&node_on, &entries, true);

    // Turning the shadow ON changes nothing the replicated digest can see.
    assert_eq!(
        digest_off,
        digest_on,
        "the shadow qlog perturbed replicated state (first differing keyspace: {:?})",
        digest_off.first_difference(&digest_on),
    );
    // Knob off: today's exact behaviour — no `qlog/` directory is created at all.
    assert!(
        !node_off.path().join("qlog").exists(),
        "the knob was off but a qlog/ directory was created",
    );
    // Knob on: the shadow really did write, so the equality above is meaningful.
    assert!(
        node_on.path().join("qlog").exists(),
        "the knob was on but no qlog/ directory was created",
    );
}
