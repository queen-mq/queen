//! TEMPORARY diagnostics for the FAT100 claim-scarcity investigation
//! (2026-09-21). Counters are rendered into `/metrics/prometheus` as
//! `queen_raft_dbg{c="..."}`; `maybe_dump` prints one per-(queue, group) line of
//! partition state to stderr every `QUEEN_RAFT_DEBUG_DUMP_MS` (unset = off).
//! Not for merge.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering::Relaxed};
use std::sync::LazyLock;

use crate::rsm::state::Derived;
use crate::rsm::store::TypedReads;

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
    pop_over_queue,
    pop_cands,
    pop_empty_nocand,
    pop_empty_allfail,
    pop_claims,
    claim_ok,
    claim_ok_msgs,
    claim_none_nopart,
    claim_none_window,
    claim_none_leased,
    claim_none_sealed,
    claim_none_taken0_nosegs,
    claim_none_taken0_segs,
    ack_fast,
    ack_reject,
    ack_slow_released,
    ack_slow_kept,
    ack_slow_nolease,
    push_dedup_build,
    push_dedup_records,
    orphan_released,
    render_part_retry,
    render_part_missing,
);

#[inline]
pub fn inc(c: &AtomicU64, n: u64) {
    c.fetch_add(n, Relaxed);
}

static EVERY_MS: LazyLock<Option<i64>> = LazyLock::new(|| {
    std::env::var("QUEEN_RAFT_DEBUG_DUMP_MS")
        .ok()
        .and_then(|v| v.parse::<i64>().ok())
        .filter(|v| *v > 0)
});
static LAST_MS: AtomicI64 = AtomicI64::new(0);

/// One stderr line per (tenant, queue, group) with pending work: how the
/// backlog splits between leased and free partitions, the ring's view, and the
/// top partitions by backlog.
pub fn maybe_dump<R: TypedReads + ?Sized>(r: &R, now_us: i64) {
    let Some(every) = *EVERY_MS else { return };
    let now_ms = now_us / 1000;
    let last = LAST_MS.load(Relaxed);
    if now_ms - last < every || LAST_MS.compare_exchange(last, now_ms, Relaxed, Relaxed).is_err() {
        return;
    }
    // The planner now builds only the rings it walks (P4): the dump builds its
    // own full view.
    let Ok(full) = Derived::rebuild_rings(r, now_us, None) else { return };
    let derived = &full;
    // (tenant, queue, group) -> pid -> pending ready_at
    let mut pend: BTreeMap<(String, String, String), BTreeMap<u64, i64>> = BTreeMap::new();
    let _ = r.scan_pending(&[], usize::MAX, &mut |t, q, g, pid, at| {
        pend.entry((t.to_string(), q.to_string(), g.to_string()))
            .or_default()
            .insert(pid, at);
        true
    });
    for ((t, q, g), pmap) in &pend {
        let mut pids: Vec<u64> = Vec::new();
        let _ = r.scan_queue_partitions(t, q, None, usize::MAX, &mut |pid| {
            pids.push(pid);
            true
        });
        let ring = derived.ring(t, q, g);
        let (mut leased, mut leased_bl, mut free_bl, mut caught, mut free_bl_parts) = (0u64, 0i64, 0i64, 0u64, 0u64);
        let (mut in_ring, mut leased_in_ring, mut max_lease_age_ms) = (0u64, 0u64, 0i64);
        let mut rows: Vec<(i64, u64, bool, i64, i64)> = Vec::new(); // backlog, pid, leased, lease_age_ms, ready_in_ms
        for pid in &pids {
            let Ok(Some(p)) = r.partition(*pid) else { continue };
            let cur = r.cursor(*pid, g).ok().flatten();
            let committed = cur.as_ref().map(|c| c.committed).unwrap_or(-1);
            let backlog = (p.last_offset - committed).max(0);
            let live = cur.as_ref().is_some_and(|c| {
                c.worker.is_some() && c.lease_expires_at_us.is_some_and(|e| e > now_us)
            });
            let age_ms = cur
                .as_ref()
                .and_then(|c| c.lease_acquired_at_us)
                .filter(|_| live)
                .map(|a| (now_us - a) / 1000)
                .unwrap_or(0);
            let ready_in_ms = pmap.get(pid).map(|at| (at - now_us) / 1000).unwrap_or(i64::MIN);
            let ringed = ring.is_some_and(|rg| rg.contains(*pid));
            if ringed {
                in_ring += 1;
                if live {
                    leased_in_ring += 1;
                }
            }
            if live {
                leased += 1;
                leased_bl += backlog;
                max_lease_age_ms = max_lease_age_ms.max(age_ms);
            } else if backlog > 0 {
                free_bl += backlog;
                free_bl_parts += 1;
            } else {
                caught += 1;
            }
            rows.push((backlog, *pid, live, age_ms, ready_in_ms));
        }
        rows.sort_by(|a, b| b.0.cmp(&a.0));
        let top: Vec<String> = rows
            .iter()
            .take(10)
            .map(|(bl, pid, l, age, rin)| {
                let rin_s = if *rin == i64::MIN { "none".to_string() } else { rin.to_string() };
                format!("{pid}:bl={bl}:leased={}:age={age}ms:readyIn={rin_s}ms", *l as u8)
            })
            .collect();
        eprintln!(
            "DBGDUMP t={now_ms} q={q} g={g} parts={} leased={leased} leasedBacklog={leased_bl} \
             freeBacklogParts={free_bl_parts} freeBacklog={free_bl} caughtUp={caught} ringLive={in_ring} \
             leasedInRing={leased_in_ring} ringDeferred={} maxLeaseAge={max_lease_age_ms}ms top=[{}]",
            pids.len(),
            ring.map(|rg| rg.deferred_len()).unwrap_or(0),
            top.join(" ")
        );
    }
}
