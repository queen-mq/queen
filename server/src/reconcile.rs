//! Periodic state reconcile (RUSTFIX item 16).
//!
//! The mesh is best-effort (frames are dropped when a peer is slow or down), so a
//! lost MAINTENANCE_MODE_SET / QUEUE_CONFIG_SET frame would leave a replica
//! divergent forever without this. This loop mirrors the C++
//! SharedStateManager::refresh_loop (shared_state_manager.cpp:457-487): every
//! `QUEEN_CACHE_REFRESH_INTERVAL_MS` (default 60s) it re-reads the maintenance
//! flags from queen.system_state (the DB is the source of truth) into the
//! in-process atomics, and drops the per-queue lease cache so a lost config
//! invalidation heals within one interval.
//!
//! Since 1.0.1 it carries one more DB-truth floor of the same shape: the hot-list
//! repair markers (A3, `apply_hotlist_repairs`), which are what a lost
//! T_HOTLIST_DIRTY_BATCH costs now that the reseed no longer walks every partition
//! every 30s.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;

use crate::db;
use crate::handlers::AppState;

/// What this broker has already acted on, so a repair that is still sitting in
/// `queen.hotlist_repairs` is applied ONCE and not on every pass for the length of the
/// prune window.
///
/// Keyed "(tenant,queue)\x1f group", valued by the publisher's own (timestamp,
/// partition) — compared only for CHANGE, so nothing here needs the two clocks to
/// agree, or even the rows to arrive in order. Rebuilt from each read, so it is a
/// mirror of the table and inherits its bound (one entry per (tenant, queue, group)
/// repaired within the publisher's prune window).
#[derive(Default)]
pub(crate) struct AppliedRepairs(std::collections::HashMap<String, RepairValue>);

/// The publisher's (timestamp, partition scope) for one repair — the change token.
type RepairValue = (String, Option<String>);

impl AppliedRepairs {
    /// Is this repair one this broker has not acted on? A row it has already applied
    /// stays in the table until the publisher prunes it (an hour), so answering "yes"
    /// twice would cost a full walk of that ring on EVERY reconcile pass in between.
    fn is_new(&self, key: &str, value: &RepairValue) -> bool {
        self.0.get(key) != Some(value)
    }

    /// Adopt what this pass read, wholesale. Replacing rather than merging is what
    /// bounds the map: a repair the publisher has pruned takes this broker's memory of
    /// it with it, and if it is ever published again it is a new repair — which it is.
    fn replace(&mut self, next: std::collections::HashMap<String, RepairValue>) {
        self.0 = next;
    }
}

/// A3 (PLAN_HOTLIST_FOLLOWUP.md): apply every hot-list repair this broker has not
/// acted on yet. One small read per reconcile pass.
///
/// The mesh carries a moved cursor to the peers in ~20ms and drops the frame when a
/// peer's queue is full or the peer is down. That was survivable while every broker
/// walked every partition every 30s; with the windowed floor a dropped frame costs the
/// replay a full-walk interval (300s by default). So the seek and the consumer-group
/// delete also publish a durable marker in their own transaction, and this reads it —
/// mesh fast, database poll as the floor, which is the layering the mesh module already
/// promises for everything else.
///
/// What it does NOT cover, because the marker is a publication channel for DELIBERATE
/// cursor moves and not a general lost-notification repair: a ring entry wrongly
/// cleared, an INFLIGHT stranded by a dropped pop future, a stale WHEEL park, or a
/// dropped hint for a PUSH. Those still wait for the full walk.
///
/// Shared with the embedded engine's inlined loop, which needs it more: embedded has no
/// mesh at all, so there this is the ONLY cross-instance repair for a moved cursor
/// rather than the floor under one.
///
/// Skipped entirely under QUEEN_HOTLIST_RESEED_FULL_MS=0 (C2): every pass is already a
/// full walk at the 30s cadence, so the marker has nothing to add.
pub(crate) async fn apply_hotlist_repairs(
    state: &AppState,
    client: &deadpool_postgres::Client,
    seen: &mut AppliedRepairs,
) {
    if !state.hotlist.enabled() || state.hotlist_reseed_full_ms <= 0 {
        return;
    }
    let rows = match db::hotlist_repairs_all(client).await {
        Ok(r) => r,
        // Best-effort, like every other read in this loop: the next pass re-reads the
        // same table, and the full walk is underneath it either way.
        Err(_) => return,
    };
    let now = crate::util::now_epoch_ms();
    let mut next = std::collections::HashMap::with_capacity(rows.len());
    let mut applied = 0usize;
    for r in rows {
        let qkey = crate::handlers::tenant_queue_key(&r.tenant, &r.queue);
        let key = format!("{qkey}\u{1f}{}", r.group);
        let value = (r.repair_at, r.partition);
        if seen.is_new(&key, &value) {
            // A repair naming a (queue, group) this broker holds no ring for is a no-op
            // in both calls — that group is not polled here and cold-starts a full walk
            // on first contact, which is the same repair by another route.
            let hit = match value.1.as_deref() {
                // Scoped to one partition (a per-partition seek): mark exactly it, which
                // is what the seeking broker did locally and what its mesh hint carried.
                Some(p) => state.hotlist.mark_remote_group(&qkey, p, &r.group, now),
                // Queue-wide: the pending set is whatever PG says, so owe a full walk.
                None => state.hotlist.request_full_walk(&qkey, &r.group),
            };
            if hit {
                applied += 1;
            }
        }
        next.insert(key, value);
    }
    seen.replace(next);
    if applied > 0 {
        tracing::info!(
            target: "reconcile",
            applied,
            "hot-list repairs applied (a cursor moved backwards on some broker)"
        );
    }
}

/// Launch the reconcile loop. Non-blocking; spawns a detached tokio task.
pub fn spawn(state: Arc<AppState>, pool: Pool, interval_ms: u64) {
    let interval = Duration::from_millis(interval_ms.max(1));
    tracing::info!(target: "reconcile", interval_ms, "state reconcile loop started");
    tokio::spawn(async move {
        // Small initial delay so boot-time seeding settles first (C++ sleeps 5s).
        tokio::time::sleep(Duration::from_secs(5)).await;
        // A3: what this loop has already repaired. Empty at boot, which is also when
        // there is nothing to repair — no ring exists yet, and the ones that appear
        // cold-start a full walk on first contact.
        let mut repaired = AppliedRepairs::default();
        loop {
            if let Ok(c) = pool.get().await {
                // Maintenance flags: overwrite whatever the mesh left with the DB truth.
                if let Ok(m) = db::get_system_flag(&c, "maintenance_mode").await {
                    let prev = state.maintenance.swap(m, Ordering::Relaxed);
                    if prev != m {
                        tracing::info!(target: "reconcile", flag = "maintenance_mode", from = %prev, to = %m, "flag changed");
                        // Keep the file-buffer drain lifecycle in sync when the flag
                        // is flipped remotely (item 17), same as the HTTP handler.
                        if m {
                            state.file_buffer.pause_background_drain();
                        } else {
                            state.file_buffer.force_finalize_all();
                            state.file_buffer.resume_background_drain();
                        }
                    }
                }
                if let Ok(pm) = db::get_system_flag(&c, "pop_maintenance_mode").await {
                    let prev = state.pop_maintenance.swap(pm, Ordering::Relaxed);
                    if prev != pm {
                        tracing::info!(target: "reconcile", flag = "pop_maintenance_mode", from = %prev, to = %pm, "flag changed");
                    }
                }
                // PLAN_KV_TIMERS §12.1 — the kv/timers kill switches, from the
                // SAME table and for the same reason as the two above: the mesh
                // is best-effort, so without this a replica that missed the flip
                // stays divergent for ever. `get_system_flag_opt` and not
                // `get_system_flag`: an ABSENT row means ON for these, and
                // reusing the collapsing reader would switch the features off on
                // every cell that has never had an operator touch them.
                for key in [
                    crate::switches::Switches::KEY_KV,
                    crate::switches::Switches::KEY_TIMERS_SCHEDULE,
                    crate::switches::Switches::KEY_TIMERS_FIRE,
                    // EPHEMERAL_QUEUES.md §3.8: the RAM class's switch rides the
                    // same mirror, so a flip made on one broker of a cell reaches
                    // the others within one reconcile interval.
                    crate::switches::Switches::KEY_EPHEMERAL,
                ] {
                    if let Ok(v) = db::get_system_flag_opt(&c, key).await {
                        state.switches.adopt(key, v);
                    }
                }
                apply_hotlist_repairs(&state, &c, &mut repaired).await;
            }
            // Drop the per-queue caches so a lost QUEUE_CONFIG_SET heals within one
            // interval (a leaseTime / encryption_enabled reconfigure is re-fetched
            // lazily on the next pop/push).
            state.lease_cache.lock().unwrap().clear();
            state.enc_cache.lock().unwrap().clear();
            // Phase 2 first-contact safety: drop the positive seed cache too, so a
            // queue delete+recreate (marker gone) re-gates targeted pops until the
            // new seed commits. A still-seeded (queue, group) simply re-caches on
            // its next targeted serve via one indexed marker read.
            state.seeded_groups.lock().unwrap().clear();
            tokio::time::sleep(interval).await;
        }
    });
}

/// Idle sweep for the two per-(tenant, queue) maps that are NOT covered by the
/// reconcile loop above (which only clears TTL-style scalar caches): the hot-list rings
/// and the long-poll wake gates. Both are created on demand from a client-supplied queue
/// NAME, so on a shared cell an untrusted tenant can otherwise pin one ring + one gate
/// per distinct name for the process lifetime. Each map uses a second-chance flag, so an
/// entry is dropped only after a full sweep with no access AND with no live state (see
/// `HotList::evict_idle` / `Notifier::evict_idle`); the §8 reseed floor rebuilds a ring
/// that was dropped too eagerly, and a gate is a wake fast path whose correctness floor
/// is the pop's own backoff. `sweep_ms == 0` disables it.
pub fn spawn_idle_sweep(state: Arc<AppState>, sweep_ms: u64) {
    if sweep_ms == 0 {
        tracing::info!(target: "reconcile", "hot-list/gate idle sweep disabled");
        return;
    }
    let interval = Duration::from_millis(sweep_ms);
    tracing::info!(target: "reconcile", sweep_ms, "hot-list/gate idle sweep started");
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(interval).await;
            let rings = state.hotlist.evict_idle();
            let gates = state.notifier.evict_idle();
            if rings > 0 || gates > 0 {
                tracing::debug!(
                    target: "reconcile",
                    rings_evicted = rings,
                    gates_evicted = gates,
                    rings_live = state.hotlist.queue_count(),
                    gates_live = state.notifier.gate_count(),
                    "idle sweep"
                );
            }
        }
    });
}

#[cfg(test)]
mod hotlist_repair_memory {
    use super::*;
    use std::collections::HashMap;

    const KEY: &str = "tenant\u{1f}orders\u{1f}workers";

    fn v(at: &str, partition: Option<&str>) -> RepairValue {
        (at.to_string(), partition.map(|p| p.to_string()))
    }

    fn pass(seen: &mut AppliedRepairs, value: &RepairValue) -> bool {
        let acted = seen.is_new(KEY, value);
        let mut next = HashMap::new();
        next.insert(KEY.to_string(), value.clone());
        seen.replace(next);
        acted
    }

    // The regression this exists for: a repair row survives in the table until the
    // publisher prunes it (an hour). Acting on it once per pass would mean a full walk
    // of that ring every reconcile interval for an hour — 60 walks of the query this
    // whole patch exists to stop running, from ONE seek.
    #[test]
    fn a_repair_is_acted_on_once_and_not_on_every_pass() {
        let mut seen = AppliedRepairs::default();
        let row = v("2026-08-12 10:00:00+00", None);
        assert!(pass(&mut seen, &row), "first sight: repair it");
        for _ in 0..10 {
            assert!(!pass(&mut seen, &row), "same row, already repaired");
        }
    }

    // Both halves of the token matter. A second seek is a new repair even for the same
    // (queue, group), and a scope that widened from one partition to the whole queue is
    // a different repair — missing that would leave the peers marking one partition when
    // two moved.
    #[test]
    fn a_new_timestamp_or_a_widened_scope_is_a_new_repair() {
        let mut seen = AppliedRepairs::default();
        assert!(pass(&mut seen, &v("2026-08-12 10:00:00+00", Some("p1"))));
        assert!(pass(&mut seen, &v("2026-08-12 10:00:05+00", Some("p1"))), "seeked again");
        assert!(
            pass(&mut seen, &v("2026-08-12 10:00:05+00", None)),
            "same instant, wider scope — still something new to do"
        );
    }

    // No ordering is involved anywhere: the reader compares tokens for CHANGE, so a row
    // that commits with an EARLIER timestamp than one already seen (a long publishing
    // transaction stamps `now()` when it starts, not when it commits) is still acted on.
    // A watermark is what would have skipped it silently.
    #[test]
    fn a_repair_that_commits_out_of_timestamp_order_is_still_acted_on() {
        let mut seen = AppliedRepairs::default();
        assert!(pass(&mut seen, &v("2026-08-12 10:00:05+00", None)));
        assert!(
            pass(&mut seen, &v("2026-08-12 10:00:01+00", None)),
            "older than what this broker already applied, and still a repair it owes"
        );
    }

    // The memory mirrors the table, so it cannot outgrow it — and a (queue, group)
    // repaired again after its old row was pruned is genuinely new work.
    #[test]
    fn a_pruned_repair_takes_its_memory_with_it() {
        let mut seen = AppliedRepairs::default();
        let row = v("2026-08-12 10:00:00+00", None);
        assert!(pass(&mut seen, &row));
        seen.replace(HashMap::new()); // the publisher pruned it
        assert!(seen.is_new(KEY, &row), "nothing remembered, so nothing suppressed");
    }
}
