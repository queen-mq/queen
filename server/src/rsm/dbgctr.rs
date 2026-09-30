//! TEMPORARY diagnostics for the FAT100 claim-scarcity investigation
//! (2026-09-21). Counters are rendered into `/metrics/prometheus` as
//! `queen_raft_dbg{c="..."}`. Not for merge.

use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

macro_rules! ctrs {
    ($($n:ident),* $(,)?) => {
        pub struct DbgCtr { $(pub $n: AtomicU64,)* }
        pub static C: DbgCtr = DbgCtr { $($n: AtomicU64::new(0),)* };
        pub fn render(out: &mut String) {
            out.push_str("# TYPE queen_raft_dbg counter\n");
            $( out.push_str(&format!("queen_raft_dbg{{c=\"{}\"}} {}\n", stringify!($n), C.$n.load(Relaxed))); )*
        }
    };
}

ctrs!(
    push_dedup_build,
    push_dedup_records,
    orphan_released,
    render_part_retry,
    render_part_missing,
    render_gap_offsets,
    fwd_transport_retry,
    fwd_unanswered_pops,
    fwd_unanswered_claims,
    fwd_pops_gated,
    pop_h_attempts,
    pop_h_submits,
    pop_h_submit_us,
    pop_h_submit_empty,
    pop_h_parks,
    pop_h_park_us,
    pop_h_park_woke,
    pop_h_render_us,
    render_gap_recovered,
    render_gap_claims,
    apply_waited_write,
    apply_wait_max_us,
    apply_wait_over_50ms,
    writer_write_max_us,
    writer_handoff_max_us,
    writer_persist_max_us,
);

#[inline]
pub fn inc(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Relaxed);
}

#[inline]
pub fn max(c: &AtomicU64, n: u64) {
    c.fetch_max(n, Relaxed);
}
