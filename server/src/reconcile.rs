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
    println!("reconcile: state reconcile loop started (interval={interval_ms}ms)");
    tokio::spawn(async move {
        // Small initial delay so boot-time seeding settles first (C++ sleeps 5s).
        tokio::time::sleep(Duration::from_secs(5)).await;
        loop {
            if let Ok(c) = pool.get().await {
                // Maintenance flags: overwrite whatever the mesh left with the DB truth.
                if let Ok(m) = db::get_system_flag(&c, "maintenance_mode").await {
                    let prev = state.maintenance.swap(m, Ordering::Relaxed);
                    if prev != m {
                        println!("reconcile: maintenance_mode {prev} -> {m} (from DB)");
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
                        println!("reconcile: pop_maintenance_mode {prev} -> {pm} (from DB)");
                    }
                }
            }
            // Drop the per-queue caches so a lost QUEUE_CONFIG_SET heals within one
            // interval (a leaseTime / encryption_enabled reconfigure is re-fetched
            // lazily on the next pop/push).
            state.lease_cache.lock().unwrap().clear();
            state.enc_cache.lock().unwrap().clear();
            tokio::time::sleep(interval).await;
        }
    });
}
