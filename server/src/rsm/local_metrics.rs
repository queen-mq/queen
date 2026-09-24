//! Node-local metrics and retention history (D17).
//!
//! The replicated store is deliberately not used for per-replica samples.
//! Each RSM directory instead owns `local.db`, a small checksum-framed journal.
//! A torn final record is discarded on reopen, the in-memory window is bounded,
//! and the journal rewrites itself at twice that bound. This is an observability
//! store: losing its unflushed tail may lose a graph point, never queue state.

use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, LazyLock, Mutex, Weak};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use xxhash_rust::xxh3::xxh3_64;

const MAGIC: &[u8; 8] = b"QNLOC1\0\0";
const RETENTION_CAP: usize = 4_096;
const MAX_RECORD: usize = 1 << 20;

#[derive(Clone, Serialize, Deserialize)]
struct RetentionEvent {
    at_us: i64,
    tenant: String,
    queue: String,
    pid: u64,
    log_from: u64,
    log_to: u64,
    txns_from: u64,
    txns_to: u64,
}

/// A journal record of finished partition-churn minutes.
#[derive(Serialize, Deserialize)]
struct ChurnRecord {
    churn: Vec<crate::rsm::dashboard::model::ChurnRow>,
}

/// Partition churn is kept this long (it is one row per queue per minute
/// with partitions created or deleted).
const CHURN_KEEP_US: i64 = 24 * 3600 * 1_000_000;

struct State {
    retention: VecDeque<RetentionEvent>,
    appended: usize,
    /// `(minute, tenant, queue)` → (created, deleted). Minutes before
    /// `churn_journaled_to` are in the journal; the open minute is not yet.
    churn: std::collections::BTreeMap<(i64, String, String), (i64, i64)>,
    churn_journaled_to: i64,
}

/// One node's local observability store. The registry below makes the applier
/// and facade share the same instance without putting a node-local handle in a
/// replicated entry or store row.
pub struct LocalMetrics {
    path: PathBuf,
    state: Mutex<State>,
}

static REGISTRY: LazyLock<Mutex<HashMap<PathBuf, Weak<LocalMetrics>>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

/// [`LocalMetrics::flush_churn`] on every open store (every raft group of
/// this process).
pub fn flush_all_churn(now_us: i64) {
    let stores: Vec<Arc<LocalMetrics>> = REGISTRY
        .lock()
        .expect("local metrics registry poisoned")
        .values()
        .filter_map(Weak::upgrade)
        .collect();
    for s in stores {
        s.flush_churn(now_us);
    }
}

pub fn open(path: impl Into<PathBuf>) -> io::Result<Arc<LocalMetrics>> {
    let path = path.into();
    let mut registry = REGISTRY.lock().expect("local metrics registry poisoned");
    if let Some(existing) = registry.get(&path).and_then(Weak::upgrade) {
        return Ok(existing);
    }
    let store = Arc::new(LocalMetrics::load(path.clone())?);
    registry.insert(path, Arc::downgrade(&store));
    Ok(store)
}

impl LocalMetrics {
    fn load(path: PathBuf) -> io::Result<LocalMetrics> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut events = VecDeque::with_capacity(RETENTION_CAP);
        let mut appended = 0usize;
        let mut churn = std::collections::BTreeMap::new();
        let mut churn_to = 0i64;
        match File::open(&path) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                if bytes.len() >= MAGIC.len() && &bytes[..MAGIC.len()] == MAGIC {
                    let mut at = MAGIC.len();
                    let mut valid = at;
                    while at + 12 <= bytes.len() {
                        let len =
                            u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap()) as usize;
                        let checksum =
                            u64::from_le_bytes(bytes[at + 4..at + 12].try_into().unwrap());
                        at += 12;
                        if len > MAX_RECORD || at + len > bytes.len() {
                            break;
                        }
                        let payload = &bytes[at..at + len];
                        if xxh3_64(payload) != checksum {
                            break;
                        }
                        if let Ok(event) = serde_json::from_slice::<RetentionEvent>(payload) {
                            if events.len() == RETENTION_CAP {
                                events.pop_front();
                            }
                            events.push_back(event);
                            appended += 1;
                        } else if let Ok(rec) = serde_json::from_slice::<ChurnRecord>(payload) {
                            for c in rec.churn {
                                let e = churn
                                    .entry((c.bucket_us, c.tenant, c.queue))
                                    .or_insert((0, 0));
                                e.0 += c.created;
                                e.1 += c.deleted;
                                churn_to = churn_to.max(c.bucket_us + 60_000_000);
                            }
                            appended += 1;
                        }
                        at += len;
                        valid = at;
                    }
                    if valid < bytes.len() {
                        OpenOptions::new()
                            .write(true)
                            .open(&path)?
                            .set_len(valid as u64)?;
                    }
                } else if !bytes.is_empty() {
                    // Node-local history is rebuildable; preserve an unknown old
                    // file for diagnosis and begin a clean journal.
                    let old = path.with_extension("db.old");
                    let _ = std::fs::rename(&path, old);
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if !path.exists() {
            let mut file = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
        }
        Ok(LocalMetrics {
            path,
            state: Mutex::new(State {
                retention: events,
                appended,
                churn,
                churn_journaled_to: churn_to,
            }),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn record_retention(
        &self,
        at_us: i64,
        tenant: &str,
        queue: &str,
        pid: u64,
        log_from: u64,
        log_to: u64,
        txns_from: u64,
        txns_to: u64,
    ) {
        if log_to <= log_from && txns_to <= txns_from {
            return;
        }
        let event = RetentionEvent {
            at_us,
            tenant: tenant.to_string(),
            queue: queue.to_string(),
            pid,
            log_from,
            log_to,
            txns_from,
            txns_to,
        };
        let Ok(payload) = serde_json::to_vec(&event) else {
            return;
        };
        let mut state = self.state.lock().expect("local metrics poisoned");
        if append_record(&self.path, &payload).is_err() {
            return;
        }
        if state.retention.len() == RETENTION_CAP {
            state.retention.pop_front();
        }
        state.retention.push_back(event);
        state.appended += 1;
        if state.appended >= RETENTION_CAP * 2 {
            if rewrite(&self.path, &state.retention, &state.churn).is_ok() {
                state.appended = state.retention.len() + 1;
            }
        }
    }

    /// Count partitions created / deleted in `queue` at `at_us` (the applied
    /// entry's time, so every node counts the same minute). RAM only on the
    /// apply thread: a finished minute is journaled in one record when the
    /// next minute's first event arrives.
    pub fn record_churn(&self, at_us: i64, tenant: &str, queue: &str, created: i64, deleted: i64) {
        use crate::rsm::dashboard::model::{trunc_us, ChurnRow, US_PER_MIN};
        let bucket = trunc_us(at_us, US_PER_MIN);
        let mut state = self.state.lock().expect("local metrics poisoned");
        if bucket > state.churn_journaled_to {
            let done: Vec<ChurnRow> = state
                .churn
                .range((state.churn_journaled_to, String::new(), String::new())..)
                .take_while(|((b, _, _), _)| *b < bucket)
                .map(|((b, t, q), (c, d))| ChurnRow {
                    bucket_us: *b,
                    tenant: t.clone(),
                    queue: q.clone(),
                    created: *c,
                    deleted: *d,
                })
                .collect();
            if !done.is_empty() {
                if let Ok(payload) = serde_json::to_vec(&ChurnRecord { churn: done }) {
                    if append_record(&self.path, &payload).is_ok() {
                        state.appended += 1;
                    }
                }
            }
            state.churn_journaled_to = bucket;
            let floor = bucket - CHURN_KEEP_US;
            state.churn.retain(|(b, _, _), _| *b >= floor);
        }
        let e = state
            .churn
            .entry((bucket, tenant.to_string(), queue.to_string()))
            .or_insert((0, 0));
        e.0 += created;
        e.1 += deleted;
    }

    /// Journal every churn minute that ended before `now_us` and is not in
    /// the journal yet. [`LocalMetrics::record_churn`] journals a minute only
    /// when a later minute's first event arrives; this closes the last one
    /// when no further event comes (called by the metrics collector).
    pub fn flush_churn(&self, now_us: i64) {
        use crate::rsm::dashboard::model::{trunc_us, ChurnRow, US_PER_MIN};
        let open = trunc_us(now_us, US_PER_MIN);
        let mut state = self.state.lock().expect("local metrics poisoned");
        if open <= state.churn_journaled_to {
            return;
        }
        let done: Vec<ChurnRow> = state
            .churn
            .range((state.churn_journaled_to, String::new(), String::new())..)
            .take_while(|((b, _, _), _)| *b < open)
            .map(|((b, t, q), (c, d))| ChurnRow {
                bucket_us: *b,
                tenant: t.clone(),
                queue: q.clone(),
                created: *c,
                deleted: *d,
            })
            .collect();
        if !done.is_empty() {
            if let Ok(payload) = serde_json::to_vec(&ChurnRecord { churn: done }) {
                if append_record(&self.path, &payload).is_ok() {
                    state.appended += 1;
                }
            }
        }
        state.churn_journaled_to = open;
    }

    /// The tenant's partition churn in `[from_us, to_us)`, the open minute
    /// included.
    pub fn churn_rows(
        &self,
        tenant: &str,
        from_us: i64,
        to_us: i64,
    ) -> Vec<crate::rsm::dashboard::model::ChurnRow> {
        let state = self.state.lock().expect("local metrics poisoned");
        state
            .churn
            .range((from_us, String::new(), String::new())..)
            .take_while(|((b, _, _), _)| *b < to_us)
            .filter(|((_, t, _), _)| t == tenant)
            .map(|((b, t, q), (c, d))| crate::rsm::dashboard::model::ChurnRow {
                bucket_us: *b,
                tenant: t.clone(),
                queue: q.clone(),
                created: *c,
                deleted: *d,
            })
            .collect()
    }

    /// The tenant's retention steps in `[from_us, to_us)` as dashboard rows.
    pub fn retention_rows(
        &self,
        tenant: &str,
        from_us: i64,
        to_us: i64,
    ) -> Vec<crate::rsm::dashboard::model::RetentionRow> {
        let state = self.state.lock().expect("local metrics poisoned");
        state
            .retention
            .iter()
            .filter(|e| e.tenant == tenant && e.at_us >= from_us && e.at_us < to_us)
            .map(|e| crate::rsm::dashboard::model::RetentionRow {
                at_us: e.at_us,
                tenant: e.tenant.clone(),
                queue: e.queue.clone(),
                partition_id: e.pid,
                retention_msgs: e.log_to.saturating_sub(e.log_from) as i64,
                completed_retention_msgs: 0,
                eviction_msgs: 0,
                log_from: e.log_from,
                log_to: e.log_to,
                txns_from: e.txns_from,
                txns_to: e.txns_to,
            })
            .collect()
    }

    pub fn retention_json(&self, tenant: &str) -> Value {
        let state = self.state.lock().expect("local metrics poisoned");
        let mut total = 0u64;
        let series: Vec<Value> = state
            .retention
            .iter()
            .filter(|event| event.tenant == tenant)
            .map(|event| {
                let deleted = event.log_to.saturating_sub(event.log_from);
                total = total.saturating_add(deleted);
                json!({
                    "bucket": crate::rsm::planner::timers::iso_us(event.at_us),
                    "queueName": event.queue,
                    "partitionId": event.pid,
                    "retentionMsgs": deleted,
                    "completedRetentionMsgs": 0,
                    "evictionMsgs": 0,
                    "totalMsgs": deleted,
                    "eventCount": 1,
                    "logStartFrom": event.log_from,
                    "logStartTo": event.log_to,
                    "txnsStartFrom": event.txns_from,
                    "txnsStartTo": event.txns_to,
                })
            })
            .collect();
        json!({
            "bucketMinutes": 1,
            "series": series,
            "totals": {
                "retentionMsgs": total,
                "completedRetentionMsgs": 0,
                "evictionMsgs": 0,
                "totalMsgs": total,
                "eventCount": series.len(),
            }
        })
    }
}

fn append_record(path: &Path, payload: &[u8]) -> io::Result<()> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(&(payload.len() as u32).to_le_bytes())?;
    file.write_all(&xxh3_64(payload).to_le_bytes())?;
    file.write_all(payload)
}

fn rewrite(
    path: &Path,
    events: &VecDeque<RetentionEvent>,
    churn: &std::collections::BTreeMap<(i64, String, String), (i64, i64)>,
) -> io::Result<()> {
    let tmp = path.with_extension("db.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(MAGIC)?;
    for event in events {
        let payload = serde_json::to_vec(event).map_err(io::Error::other)?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&xxh3_64(&payload).to_le_bytes())?;
        file.write_all(&payload)?;
    }
    let rows: Vec<crate::rsm::dashboard::model::ChurnRow> = churn
        .iter()
        .map(|((b, t, q), (c, d))| crate::rsm::dashboard::model::ChurnRow {
            bucket_us: *b,
            tenant: t.clone(),
            queue: q.clone(),
            created: *c,
            deleted: *d,
        })
        .collect();
    for chunk in rows.chunks(5_000) {
        let payload =
            serde_json::to_vec(&ChurnRecord { churn: chunk.to_vec() }).map_err(io::Error::other)?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&xxh3_64(&payload).to_le_bytes())?;
        file.write_all(&payload)?;
    }
    file.sync_all()?;
    std::fs::rename(tmp, path)?;
    if let Some(parent) = path.parent() {
        File::open(parent)?.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_history_survives_reopen_and_truncates_a_torn_tail() {
        let root = std::env::temp_dir().join(format!(
            "queen-local-metrics-{}-{}",
            std::process::id(),
            crate::util::now_epoch_ms()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("local.db");
        {
            let store = LocalMetrics::load(path.clone()).unwrap();
            store.record_retention(1, "t", "q", 7, 0, 4, 0, 2);
        }
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&100u32.to_le_bytes())
            .unwrap();
        let before = std::fs::metadata(&path).unwrap().len();
        let store = LocalMetrics::load(path.clone()).unwrap();
        let after = std::fs::metadata(&path).unwrap().len();
        assert!(after < before);
        let value = store.retention_json("t");
        assert_eq!(value["totals"]["totalMsgs"], 4);
        let _ = std::fs::remove_dir_all(root);
    }
}
