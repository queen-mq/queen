//! Push against the tiny apply loop — the port of `native/tests/push.rs`
//! plus the pitfalls of §8 row 003.

use super::planner_harness::{push, push_cfg, qcfg, Cell, TENANT};
use crate::rsm::entry::{Outcome, PushOutcome, PushVerdict};
use crate::rsm::planner::Plan;
use crate::rsm::store::keys::Counter;

fn verdicts(o: &Outcome) -> &Vec<PushVerdict> {
    match o {
        Outcome::Push(PushOutcome { items }) => items,
        other => panic!("not a push outcome: {other:?}"),
    }
}

#[test]
fn a_first_push_provisions_the_queue_and_partition_and_assigns_gapless_offsets() {
    let mut c = Cell::new("push-first");
    let cy = c.run(&[push(1, "orders", "p0", &["a", "b", "c"])]);
    let v = verdicts(&cy.outcome(0)).clone();
    assert_eq!(v.len(), 3);
    let pid = c.pid_of("orders", "p0").expect("partition created");
    assert!(c.queue("orders").is_some(), "queue provisioned implicitly");
    match (&v[0], &v[1], &v[2]) {
        (
            PushVerdict::Created {
                offset: 0, pid: p0, ..
            },
            PushVerdict::Created { offset: 1, .. },
            PushVerdict::Created { offset: 2, .. },
        ) => assert_eq!(*p0, pid),
        other => panic!("expected three gapless Created verdicts: {other:?}"),
    }
    let p = c.partition(pid).unwrap();
    assert_eq!(p.last_offset, 2);
    assert_eq!(c.partition_counter(pid, Counter::Pushed), 3);
}

#[test]
fn a_duplicate_returns_the_original_offset_and_writes_nothing() {
    let mut cfg = qcfg();
    cfg.dedup_window_seconds = 600;
    let mut c = Cell::new("push-dup");
    c.run(&[push_cfg(1, "q", "p0", &["a", "b"], cfg.clone())]);
    let pid = c.pid_of("q", "p0").unwrap();
    assert_eq!(c.partition(pid).unwrap().last_offset, 1);

    // "b" repeats and "d" is new: b duplicates at its ORIGINAL offset (1), d
    // gets the next gapless offset (2).
    c.advance(1000);
    let cy = c.run(&[push_cfg(2, "q", "p0", &["b", "d"], cfg)]);
    let v = verdicts(&cy.outcome(0)).clone();
    match (&v[0], &v[1]) {
        (PushVerdict::Duplicate { offset: 1, .. }, PushVerdict::Created { offset: 2, .. }) => {}
        other => panic!("expected (Duplicate@1, Created@2): {other:?}"),
    }
    assert_eq!(c.partition(pid).unwrap().last_offset, 2);
}

#[test]
fn an_all_duplicate_push_is_not_logged() {
    let mut cfg = qcfg();
    cfg.dedup_window_seconds = 600;
    let mut c = Cell::new("push-alldup");
    c.run(&[push_cfg(1, "q", "p0", &["a", "b"], cfg.clone())]);
    c.advance(1000);
    let cy = c.run(&[push_cfg(2, "q", "p0", &["a", "b"], cfg)]);
    assert!(
        !cy.logged,
        "a push with no survivor produces no entry (§5.4)"
    );
    match cy.plan(0) {
        Ok(Plan::Empty(Outcome::Push(o))) => {
            assert!(o
                .items
                .iter()
                .all(|v| matches!(v, PushVerdict::Duplicate { .. })));
        }
        other => panic!("expected an Empty all-duplicate push: {other:?}"),
    }
}

#[test]
fn created_at_is_strictly_monotone_per_partition() {
    let mut c = Cell::new("push-mono");
    let cy1 = c.run(&[push(1, "q", "p0", &["a"])]);
    let now1 = match &verdicts(&cy1.outcome(0))[0] {
        PushVerdict::Created { created_at_us, .. } => *created_at_us,
        v => panic!("{v:?}"),
    };
    // Same wall time as the previous cycle: the stamp still rises by at least
    // 1µs (PUSHSER, via max(now, last_created + 1)).
    let cy2 = c.run(&[push(2, "q", "p0", &["b"])]);
    let now2 = match &verdicts(&cy2.outcome(0))[0] {
        PushVerdict::Created { created_at_us, .. } => *created_at_us,
        v => panic!("{v:?}"),
    };
    assert!(now2 > now1, "created_at did not advance: {now1} -> {now2}");
}

#[test]
fn dedup_off_never_probes() {
    // With no dedup window every "a" is a fresh frame at a new offset.
    let mut c = Cell::new("push-nodedup");
    c.run(&[push(1, "q", "p0", &["a"])]);
    c.advance(1000);
    let cy = c.run(&[push(2, "q", "p0", &["a"])]);
    match &verdicts(&cy.outcome(0))[0] {
        PushVerdict::Created { offset: 1, .. } => {}
        v => panic!("dedup-off should have queued a second 'a' at offset 1: {v:?}"),
    }
}

#[test]
fn two_partitions_of_one_queue_get_distinct_pids_and_independent_offsets() {
    let mut c = Cell::new("push-two");
    c.run(&[push(1, "q", "p0", &["a", "b"])]);
    c.advance(1000);
    c.run(&[push(2, "q", "p1", &["x"])]);
    let p0 = c.pid_of("q", "p0").unwrap();
    let p1 = c.pid_of("q", "p1").unwrap();
    assert_ne!(p0, p1);
    assert_eq!(c.partition(p0).unwrap().last_offset, 1);
    assert_eq!(c.partition(p1).unwrap().last_offset, 0);
    let _ = TENANT;
}

// ---- dedup front (PERF-B) ---------------------------------------------------

/// The load-bearing soundness property: the dedup front changes NOTHING a
/// client sees. The same workload — new hashes, in-window duplicates,
/// out-of-window re-pushes, and a front RESET (a leadership regain / restart
/// that forces a re-seed from committed state) — must produce byte-identical
/// push verdicts with the front on and off.
#[test]
fn front_verdicts_match_baseline_across_window_and_restart() {
    let mut cfg = qcfg();
    cfg.dedup_window_seconds = 10; // 10 s window; advance in µs to cross it

    let workload = |enable: bool| -> Vec<Vec<PushVerdict>> {
        let mut c = Cell::new(if enable { "front-on" } else { "front-off" });
        if enable {
            c.enable_front(64);
        }
        let mut out = Vec::new();
        // create + three new
        out.push(
            verdicts(
                &c.run(&[push_cfg(1, "q", "p0", &["a", "b", "c"], cfg.clone())])
                    .outcome(0),
            )
            .clone(),
        );
        c.advance(1_000_000); // +1 s (still in window)
                              // b, c duplicate in window; d new
        out.push(
            verdicts(
                &c.run(&[push_cfg(2, "q", "p0", &["b", "c", "d"], cfg.clone())])
                    .outcome(0),
            )
            .clone(),
        );
        c.advance(20_000_000); // +20 s: a..d now outside the 10 s window
                               // a is NEW again (out of window); e new
        out.push(
            verdicts(
                &c.run(&[push_cfg(3, "q", "p0", &["a", "e"], cfg.clone())])
                    .outcome(0),
            )
            .clone(),
        );
        // Restart / leadership regain: drop the front. It must re-seed from the
        // committed txns window on the next touch and still answer exactly.
        if enable {
            c.front().reset();
        }
        c.advance(1_000_000);
        // e duplicates in window; f new — the answer must come from the re-seed.
        out.push(
            verdicts(
                &c.run(&[push_cfg(4, "q", "p0", &["e", "f"], cfg.clone())])
                    .outcome(0),
            )
            .clone(),
        );
        out
    };

    let base = workload(false);
    let front = workload(true);
    assert_eq!(
        base, front,
        "front verdicts diverged from the baseline probe"
    );
}

/// The front must actually engage: a run of unique pushes to a partition minted
/// this front-lifetime is BORN seeded (no scan), and the per-message committed
/// probe is skipped for the new hashes.
#[test]
fn front_skips_the_committed_probe_for_unique_traffic() {
    let mut cfg = qcfg();
    cfg.dedup_window_seconds = 3600;
    let mut c = Cell::new("front-unique");
    c.enable_front(64);
    // A fresh partition, then many unique messages across cycles.
    let words: Vec<String> = (0..500).map(|i| format!("u{i}")).collect();
    // First cycle creates the partition (no probe) with a first batch.
    let first: Vec<&str> = words[0..50].iter().map(|s| s.as_str()).collect();
    c.run(&[push_cfg(1, "q", "p0", &first, cfg.clone())]);
    // Subsequent cycles push more unique messages; each should skip the probe.
    for (k, chunk) in words[50..].chunks(50).enumerate() {
        c.advance(1000);
        let batch: Vec<&str> = chunk.iter().map(|s| s.as_str()).collect();
        c.run(&[push_cfg(2 + k as u64, "q", "p0", &batch, cfg.clone())]);
    }
    let s = c.front().stats();
    // Every message after creation was probed through the front; almost all were
    // unique and absent, so the front skipped the LMDB read for them.
    assert!(s.probes_skipped > 0, "front skipped nothing: {s:?}");
    assert!(
        s.probes_skipped * 5 > s.messages,
        "front skipped too little to matter: {s:?}"
    );
    // Born seeded ⇒ no partition ever fell to the always-probe fallback.
    assert_eq!(s.fallback_partitions, 0, "unexpected fallback: {s:?}");
    assert!(s.bytes > 0, "front holds no filter bytes: {s:?}");
}

/// Laptop micro-measurement (PERF-B): the planning cost and the committed
/// probes-per-message the front removes, front OFF vs ON, isolated from apply
/// (`plan_only` never writes). Not a gate; run with
/// `cargo test -p queen-engine --lib measure_dedup_front_planning -- --ignored --nocapture`.
#[ignore = "measurement, not a gate"]
#[test]
fn measure_dedup_front_planning() {
    use super::planner_harness::Cell;
    use std::time::Instant;

    // A-shape: one hot partition, a warm 20 k-hash committed dedup index, then
    // plan batches of 100 brand-new unique hashes (the ingest shape).
    fn a_shape(enable: bool) {
        let mut cfg = qcfg();
        cfg.dedup_window_seconds = 3600;
        let mut c = Cell::new(if enable { "measA-on" } else { "measA-off" });
        if enable {
            c.enable_front(256);
        }
        for b in 0..20u64 {
            let words: Vec<String> = (0..1000).map(|i| format!("pa{b}_{i}")).collect();
            let batch: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
            c.run(&[push_cfg(b + 1, "q", "p0", &batch, cfg.clone())]);
            c.advance(1000);
        }
        let s0 = c.front().stats();
        let iters = 200u64;
        let t = Instant::now();
        for k in 0..iters {
            let words: Vec<String> = (0..100).map(|i| format!("ma{k}_{i}")).collect();
            let batch: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
            let _ = c.plan_only(&[push_cfg(1_000_000 + k, "q", "p0", &batch, cfg.clone())]);
        }
        let dt = t.elapsed();
        let s = c.front().stats();
        let dmsg = (s.messages - s0.messages).max(1);
        let diss = s.probes_issued - s0.probes_issued;
        let dskip = s.probes_skipped - s0.probes_skipped;
        eprintln!(
            "[A-shape 1 part, off/on={:5}] plan/cmd={:7.1}us  plan/msg={:6.2}us  probes/msg={:.3}  skipped={:>6}/{:<6}  front_bytes={:>9}  B/msg={:.2}",
            enable,
            dt.as_nanos() as f64 / iters as f64 / 1000.0,
            dt.as_nanos() as f64 / (iters * 100) as f64 / 1000.0,
            diss as f64 / dmsg as f64,
            dskip,
            dmsg,
            s.bytes,
            if s.messages > 0 { s.bytes as f64 / s.messages as f64 } else { 0.0 },
        );
    }

    // C-shape: fan-out. 256 partitions, each with a small warm index; each
    // measured cycle plans one new message to every partition (256 commands),
    // the per-message store-key + txn-begin cost PERF-2 diagnosed for C1000.
    fn c_shape(enable: bool) {
        let mut cfg = qcfg();
        cfg.dedup_window_seconds = 3600;
        let mut c = Cell::new(if enable { "measC-on" } else { "measC-off" });
        if enable {
            c.enable_front(256);
        }
        let nparts = 256u64;
        for p in 0..nparts {
            let part = format!("p{p}");
            let words: Vec<String> = (0..20).map(|i| format!("c{p}_{i}")).collect();
            let batch: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
            c.run(&[push_cfg(p + 1, "q", &part, &batch, cfg.clone())]);
        }
        let s0 = c.front().stats();
        let iters = 100u64;
        let t = Instant::now();
        for k in 0..iters {
            let cmds: Vec<_> = (0..nparts)
                .map(|p| {
                    let part = format!("p{p}");
                    let w = format!("mc{k}_{p}");
                    // one fresh message per partition
                    push_cfg(
                        2_000_000 + k * nparts + p,
                        "q",
                        &part,
                        std::slice::from_ref(&w.as_str()),
                        cfg.clone(),
                    )
                })
                .collect();
            let _ = c.plan_only(&cmds);
        }
        let dt = t.elapsed();
        let s = c.front().stats();
        let dmsg = (s.messages - s0.messages).max(1);
        let diss = s.probes_issued - s0.probes_issued;
        let dskip = s.probes_skipped - s0.probes_skipped;
        eprintln!(
            "[C-shape 256 parts, off/on={:5}] plan/cmd={:7.2}us  probes/msg={:.3}  skipped={:>6}/{:<6}  front_bytes={:>9}  fallback={}",
            enable,
            dt.as_nanos() as f64 / (iters * nparts) as f64 / 1000.0,
            diss as f64 / dmsg as f64,
            dskip,
            dmsg,
            s.bytes,
            s.fallback_partitions,
        );
    }

    // Isolate the raw per-op cost the front removes: one committed
    // `dedup::probe_one` (the LMDB get) over a warm 100 k-hash index vs one
    // `front.should_probe` (skip) over a 100 k-hash front. No apply, no insert
    // growth in the timed window.
    fn probe_cost() {
        use crate::rsm::dedup::{self, DedupFront};
        use crate::rsm::store::Store;
        let mut cfg = qcfg();
        cfg.dedup_window_seconds = 3600;
        let mut c = Cell::new("measP");
        for b in 0..100u64 {
            let words: Vec<String> = (0..1000).map(|i| format!("w{b}_{i}")).collect();
            let batch: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
            c.run(&[push_cfg(b + 1, "q", "p0", &batch, cfg.clone())]);
            c.advance(1000);
        }
        let pid = c.pid_of("q", "p0").unwrap();
        let probes: Vec<[u8; 16]> = (0..50_000u64)
            .map(|i| crate::util::txn_hash128(&format!("zz{i}")))
            .collect();
        c.node
            .store()
            .read(|r| {
                let t = Instant::now();
                let mut sink = 0u64;
                for h in &probes {
                    if dedup::probe_one(r, pid, h, i64::MIN)?.is_some() {
                        sink += 1;
                    }
                }
                let ns = t.elapsed().as_nanos() as f64 / probes.len() as f64;
                eprintln!(
                    "[probe-cost] committed dedup::probe_one over a warm 100k index: {ns:6.0} ns/probe (hits={sink})"
                );
                Ok(())
            })
            .unwrap();
        let f = DedupFront::new(true, 256 << 20);
        f.note_created(pid);
        for i in 0..100_000u64 {
            f.insert(
                pid,
                &crate::util::txn_hash128(&format!("warm{i}")),
                1000,
                i64::MIN,
            );
        }
        let t = Instant::now();
        let mut sink = 0u64;
        for h in &probes {
            if f.should_probe(pid, h, i64::MIN) {
                sink += 1;
            }
        }
        let ns = t.elapsed().as_nanos() as f64 / probes.len() as f64;
        eprintln!("[probe-cost] front.should_probe (skip) over a 100k-hash front:  {ns:6.0} ns/op   (fell-through={sink})");
    }

    eprintln!("--- PERF-B dedup front laptop measurement ---");
    a_shape(false);
    a_shape(true);
    c_shape(false);
    c_shape(true);
    probe_cost();
}
