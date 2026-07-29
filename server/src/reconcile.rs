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

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;

use crate::db;
use crate::handlers::AppState;

/// Launch the reconcile loop. Non-blocking; spawns a detached tokio task.
pub fn spawn(state: Arc<AppState>, pool: Pool, interval_ms: u64) {
    let interval = Duration::from_millis(interval_ms.max(1));
    tracing::info!(target: "reconcile", interval_ms, "state reconcile loop started");
    tokio::spawn(async move {
        // Small initial delay so boot-time seeding settles first (C++ sleeps 5s).
        tokio::time::sleep(Duration::from_secs(5)).await;
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
