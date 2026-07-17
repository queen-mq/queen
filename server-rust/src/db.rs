use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

use crate::engine::OnPu;

#[derive(Deserialize, Serialize, Clone)]
struct PuEntry {
    queue_name: String,
    partition_id: String,
    last_message_id: String,
    last_message_created_at: String,
}

/// PUSHPOPLOOKUPSOL: coalescing buffer (latest-wins per partition) flushed on a
/// short tick as ONE update_partition_lookup_v1 call, plus a periodic reconcile.
pub struct PartitionLookup {
    tx: mpsc::UnboundedSender<Vec<u8>>,
}

impl PartitionLookup {
    pub fn new(pool: Pool, stmt_to: Duration, flush: Duration) -> Arc<PartitionLookup> {
        let (tx, rx) = mpsc::unbounded_channel::<Vec<u8>>();
        tokio::spawn(flush_loop(pool, stmt_to, flush.max(Duration::from_millis(10)), rx));
        Arc::new(PartitionLookup { tx })
    }

    /// Returns an on_pu callback the engine calls with push_messages_v3's
    /// partition_updates array bytes.
    pub fn on_pu(self: &Arc<Self>) -> OnPu {
        let tx = self.tx.clone();
        Arc::new(move |bytes: Vec<u8>| {
            let _ = tx.send(bytes);
        })
    }
}

async fn flush_loop(pool: Pool, stmt_to: Duration, flush: Duration, mut rx: mpsc::UnboundedReceiver<Vec<u8>>) {
    let mut pending: HashMap<String, PuEntry> = HashMap::new();
    let mut ticker = tokio::time::interval(flush);
    loop {
        tokio::select! {
            maybe = rx.recv() => {
                match maybe {
                    Some(bytes) => {
                        if let Ok(entries) = serde_json::from_slice::<Vec<PuEntry>>(&bytes) {
                            for e in entries {
                                match pending.get(&e.partition_id) {
                                    Some(cur) if e.last_message_created_at < cur.last_message_created_at => {}
                                    _ => { pending.insert(e.partition_id.clone(), e); }
                                }
                            }
                        }
                    }
                    None => return,
                }
            }
            _ = ticker.tick() => {
                if pending.is_empty() { continue; }
                let batch: Vec<PuEntry> = pending.drain().map(|(_, v)| v).collect();
                if let Ok(payload) = serde_json::to_string(&batch) {
                    if let Ok(client) = pool.get().await {
                        if let Ok(stmt) = client.prepare_cached("SELECT queen.update_partition_lookup_v1($1::text::jsonb)").await {
                            let _ = tokio::time::timeout(stmt_to, client.execute(&stmt, &[&payload])).await;
                        }
                    }
                }
            }
        }
    }
}

pub fn start_reconcile(pool: Pool, interval: Duration, lookback: i32, stmt_to: Duration) {
    if interval.is_zero() {
        return;
    }
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        loop {
            ticker.tick().await;
            if let Ok(client) = pool.get().await {
                if let Ok(stmt) = client.prepare_cached("SELECT queen.reconcile_partition_lookup_v1($1)").await {
                    let _ = tokio::time::timeout(stmt_to, client.execute(&stmt, &[&lookback])).await;
                }
            }
        }
    });
}
