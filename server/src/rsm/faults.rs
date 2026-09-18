//! Crash points for the recovery tests (PLAN_RAFT.md §13.5).
//!
//! `QUEEN_TEST_FAULTS="apply.segment_written:2,gc.before_unlink"` aborts the
//! process at a named point on its nth hit (the first by default). A test arms
//! a point, drives a scenario over the HTTP wire, waits for the process to die,
//! restarts it and checks what survived (test/raft/crash). Off unless the
//! variable is set: one relaxed atomic load per hit on the hot paths, which is
//! why [`hit`] is `#[inline]` and reads [`ENABLED`] `Relaxed` before it touches
//! anything else.
//!
//! The wire format is the one pgless used (`native/faults.rs`, PLAN_RAFT.md
//! §3.5), because the harness (`test/raft/crash/points.py`, the SPEC side) and
//! the broker have to agree on it:
//!
//! ```text
//! QUEEN_TEST_FAULTS="point[:nth],…"
//!   point         fire the FIRST time control reaches it
//!   point:nth     fire the nth time (1-based)
//! ```
//!
//! §0.3: `QUEEN_NATIVE_*` knobs belong to pgless and mean nothing here; the
//! fault variable keeps its name because it is a TEST knob, not a storage one.
//!
//! # The catalogue
//!
//! [`POINTS`] is the set of names this build registers, and it MUST equal the
//! phase-1 rows of `test/raft/crash/points.py`. A renamed point that only
//! changes on one side silently stops being tested; `crashdrv.py
//! --verify-points <binary>` will one day diff the two, and the shape here — a
//! `const` list, printed by nothing yet but readable — is what it reads.
//!
//! The §13.5 points this phase wires (the code paths that exist in phase 1):
//!
//! | point | where it fires | invariant |
//! |---|---|---|
//! | `batcher.drained` | [`batcher`] after the drain, before planning | I3, I14 |
//! | `planner.planned` | [`batcher`] after planning, before propose | I1, I3 |
//! | `propose.sent` | [`batcher`] the entry is handed to the replicator | I4, I6 |
//! | `log.appended` | [`replicator`] the group is written, not fsynced | I4 |
//! | `log.flushed` | [`replicator`] the group is fsynced in the log | I4 |
//! | `commit.before_apply` | [`replicator`] committed, not yet applied here | I4, I13 |
//! | `apply.mid_entry` | [`apply`] some effects applied, the rest not | I1, I11 |
//! | `apply.segment_written` | [`apply`] payload bytes in a file, store not committed | I11 |
//! | `apply.store_committed` | [`apply`] the store commit landed, files unsynced | I11 |
//! | `durable.files_synced` | [`apply`] files fsynced, durable commit not landed | I11 |
//! | `durable.store_committed` | [`apply`] the durable point is complete | I11 |
//! | `gc.before_unlink` | [`apply`] a file is unreferenced, still on disk | I10 |
//! | `gc.after_unlink` | [`apply`] the file is unlinked | I10 |
//!
//! Two extra points serve the segment layer's own roll tests (R-107), so a
//! `kill` around a roll is deterministic instead of timing-driven; they are not
//! part of the §13.5 crash matrix (there is no HTTP scenario that isolates a
//! roll in phase 1):
//!
//! | `seg.rolled` | a bucket rolled to a fresh active file | I11 |
//! | `seg.qidx_written` | the sealed file's `.qidx` is written | I11 |
//!
//! [`batcher`]: crate::rsm::batcher
//! [`replicator`]: crate::rsm::replicator
//! [`apply`]: crate::rsm::apply

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

/// Every crash point this build registers. The order is §13.5's; the harness's
/// `points.py` is the spec side and MUST list the same names. Kept as a `const`
/// so a future `--verify-points` can diff the two without running a scenario.
pub const POINTS: &[&str] = &[
    // batching and planning
    "batcher.drained",
    "planner.planned",
    "propose.sent",
    // log and apply
    "log.appended",
    "log.flushed",
    "commit.before_apply",
    "apply.mid_entry",
    "apply.segment_written",
    "apply.store_committed",
    // durable points
    "durable.files_synced",
    "durable.store_committed",
    // GC
    "gc.before_unlink",
    "gc.after_unlink",
    // segment-layer roll tests (R-107; not in the §13.5 HTTP matrix)
    "seg.rolled",
    "seg.qidx_written",
];

static ENABLED: AtomicBool = AtomicBool::new(false);
static POINTS_ARMED: OnceLock<Mutex<HashMap<String, u64>>> = OnceLock::new();

/// Read `QUEEN_TEST_FAULTS` once at boot. A no-op — and cheap forever after —
/// unless the variable names at least one point. Called from the raft boot
/// paths (`run_raft`, `embedded::boot`); the postgres class never reaches a
/// fault point, so it does not call this.
pub fn init_from_env() {
    let Ok(spec) = std::env::var("QUEEN_TEST_FAULTS") else {
        return;
    };
    let mut map = HashMap::new();
    let mut unknown: Vec<String> = Vec::new();
    for item in spec.split(',').map(str::trim).filter(|s| !s.is_empty()) {
        let (name, nth) = match item.split_once(':') {
            Some((n, k)) => (n.trim(), k.trim().parse::<u64>().unwrap_or(1)),
            None => (item, 1),
        };
        if !POINTS.contains(&name) {
            unknown.push(name.to_string());
            continue;
        }
        map.insert(name.to_string(), nth.max(1));
    }
    // A misspelled point would arm nothing and the scenario would wait forever
    // for a fault that cannot fire, then call it a pass: refuse loudly instead.
    if !unknown.is_empty() {
        eprintln!("fault: unknown crash point(s) {unknown:?}; known points are {POINTS:?}");
        std::process::exit(2);
    }
    if map.is_empty() {
        return;
    }
    let names: Vec<&String> = map.keys().collect();
    tracing::warn!(
        target: "rsm",
        points = ?names,
        "faults armed: the process will abort at these points (test only)"
    );
    let _ = POINTS_ARMED.set(Mutex::new(map));
    ENABLED.store(true, Ordering::Release);
}

/// Abort here when the point is armed and this is its nth hit. Off unless
/// [`init_from_env`] armed something: the fast path is one `Relaxed` load.
///
/// Not a store, clock, env or rng call, and it never mutates committed state,
/// so it is safe on the I2 apply path (with faults unset it is invisible, and
/// the determinism tests run it unset).
#[inline]
pub fn hit(point: &str) {
    if !ENABLED.load(Ordering::Relaxed) {
        return;
    }
    let Some(m) = POINTS_ARMED.get() else { return };
    let mut g = m.lock().expect("faults map");
    let Some(left) = g.get_mut(point) else {
        return;
    };
    if *left > 0 {
        *left -= 1;
    }
    if *left == 0 {
        drop(g);
        // SIGKILL to self, not `abort()`: it is the faithful `kill -9` model the
        // crash matrix wants (no destructors, no atexit, no stdio flush, no core
        // dump) and it avoids the macOS crash reporter, which suspends an
        // `abort()`ing process for seconds while it writes a report. The
        // `eprintln!` is flushed first so the run's stderr records which point
        // fired; the marker "fault: crash point" is what the harness scan
        // excludes as the fault's own line.
        use std::io::Write;
        eprintln!("fault: crash point {point} fired — killing this process now (test only)");
        let _ = std::io::stderr().flush();
        // SAFETY: raising a signal takes no arguments that can dangle; SIGKILL
        // cannot be caught, so this never returns.
        unsafe {
            libc::raise(libc::SIGKILL);
        }
        // Unreachable — SIGKILL does not return. A belt in case a platform ever
        // refuses it.
        std::process::abort();
    }
}

/// True when any point is armed. For a test that wants to assert a fault is
/// off, or a boot line that reports it.
pub fn armed() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_catalogue_has_no_duplicates_and_no_empty_names() {
        let mut seen = std::collections::HashSet::new();
        for p in POINTS {
            assert!(!p.is_empty(), "an empty point name");
            assert!(seen.insert(*p), "duplicate point {p}");
            assert!(
                p.contains('.'),
                "a point name is group.name by convention: {p}"
            );
        }
    }

    #[test]
    fn the_thirteen_phase_one_matrix_points_are_registered() {
        // The §13.5 phase-1 set the crash matrix drives (test/raft/crash). The
        // two seg.* points are extra (R-107) and deliberately NOT in this list.
        let matrix = [
            "batcher.drained",
            "planner.planned",
            "propose.sent",
            "log.appended",
            "log.flushed",
            "commit.before_apply",
            "apply.mid_entry",
            "apply.segment_written",
            "apply.store_committed",
            "durable.files_synced",
            "durable.store_committed",
            "gc.before_unlink",
            "gc.after_unlink",
        ];
        for m in matrix {
            assert!(POINTS.contains(&m), "matrix point {m} is not registered");
        }
        assert_eq!(matrix.len(), 13);
    }

    #[test]
    fn hit_is_a_noop_when_nothing_is_armed() {
        // ENABLED starts false in a fresh test process; this must not abort.
        hit("apply.segment_written");
        hit("no.such.point");
        assert!(!armed());
    }
}
