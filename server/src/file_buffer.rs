//! On-disk push spool for DB-outage durability + maintenance-mode buffering
//! (RUSTFIX items 1 & 17).
//!
//! Port of the C++ `FileBufferManager` (server/src/services/file_buffer.cpp,
//! server/include/queen/file_buffer.hpp). When Postgres is unreachable (a push
//! bundle resolves to `error`) or maintenance mode is on, pushes are appended to
//! rotating on-disk event files instead of being lost; a background task replays
//! them oldest-first once the DB is healthy, preserving each original
//! `transactionId` so dedup makes the replay idempotent.
//!
//! Simplifications vs C++, all deliberate: the Rust push path has no QoS-0 concept,
//! so there is a single event class (the C++ "failover" file). File writes are
//! synchronous under a mutex — acceptable because the buffer is only active in the
//! degraded DB-down / maintenance paths, never on the healthy hot path.
//!
//! File format (little-endian), append-only, matching the codec spirit of
//! file_buffer.cpp:132-204:  repeated [ u32 body_len | body(JSON event) ].
//! Active files are `*.tmp`; finalized (drainable) files are `*.buf`. Names embed
//! a 13-digit epoch-ms + an 8-digit sequence so a lexical sort is FIFO.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use deadpool_postgres::Pool;
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

use crate::config::FileBufferConfig;
use crate::frames::{pack_frames, uuid_bytes_to_string, zstd_compress, FrameIn};
use crate::fusion::json_escape_into;
use crate::util::uuidv7_bytes;

/// Circuit breaker (file_buffer.hpp:150-151): after this many consecutive drain
/// failures the drain loop backs off for `COOLDOWN`.
const MAX_CONSECUTIVE_FAILURES: usize = 10;
const COOLDOWN: Duration = Duration::from_millis(5000);
/// Startup recovery is capped (file_buffer.cpp MAX_STARTUP_RECOVERY_SECONDS).
const MAX_STARTUP_RECOVERY: Duration = Duration::from_secs(3600);
/// Files drained per cycle when the DB is healthy vs unhealthy (background_processor
/// drains up to 10 when healthy, 1 otherwise).
const DRAIN_FILES_HEALTHY: usize = 10;
const DRAIN_FILES_UNHEALTHY: usize = 1;

/// One spooled push event. Serialized as the on-disk JSON body; `payload` is the
/// raw JSON of the original message, embedded verbatim.
#[derive(Serialize)]
struct WriteEvent<'a> {
    queue: &'a str,
    partition: &'a str,
    #[serde(rename = "transactionId")]
    transaction_id: &'a str,
    #[serde(rename = "producerSub", skip_serializing_if = "Option::is_none")]
    producer_sub: Option<&'a str>,
    payload: &'a RawValue,
}

#[derive(Deserialize)]
struct StoredEvent {
    queue: String,
    partition: String,
    #[serde(rename = "transactionId")]
    transaction_id: String,
    #[serde(rename = "producerSub")]
    producer_sub: Option<String>,
    payload: Box<RawValue>,
}

struct Active {
    file: std::fs::File,
    path: PathBuf,
    count: usize,
    last_write: Instant,
}

pub struct FileBufferManager {
    cfg: FileBufferConfig,
    dir: PathBuf,
    zstd_level: i32,
    active: Mutex<Option<Active>>,
    file_seq: AtomicU64,
    /// Events written but not yet drained (approximate live pending count).
    pending: AtomicUsize,
    /// Events that failed to be written to disk (spool I/O errors).
    failed: AtomicUsize,
    /// DB reachability hint: false after a push had to be buffered / a drain
    /// failed, flipped back true on a successful drain (parity with mark_db_*).
    db_healthy: AtomicBool,
    consecutive_failures: AtomicUsize,
    /// Drain suppressed while maintenance mode is on (item 17). Writes still spool.
    drain_paused: AtomicBool,
    cooldown_until: Mutex<Option<Instant>>,
}

impl FileBufferManager {
    pub fn new(cfg: FileBufferConfig, zstd_level: i32) -> FileBufferManager {
        let dir = PathBuf::from(&cfg.dir);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            eprintln!(
                "file_buffer: could not create spool dir {}: {} (buffering will fail)",
                dir.display(),
                e
            );
        }
        FileBufferManager {
            cfg,
            dir,
            zstd_level,
            active: Mutex::new(None),
            file_seq: AtomicU64::new(0),
            pending: AtomicUsize::new(0),
            failed: AtomicUsize::new(0),
            db_healthy: AtomicBool::new(true),
            consecutive_failures: AtomicUsize::new(0),
            drain_paused: AtomicBool::new(false),
            cooldown_until: Mutex::new(None),
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending.load(Ordering::Relaxed)
    }
    pub fn failed_count(&self) -> usize {
        self.failed.load(Ordering::Relaxed)
    }
    pub fn db_healthy(&self) -> bool {
        self.db_healthy.load(Ordering::Relaxed)
    }
    pub fn mark_db_unhealthy(&self) {
        self.db_healthy.store(false, Ordering::Relaxed);
    }
    pub fn pause_background_drain(&self) {
        self.drain_paused.store(true, Ordering::Relaxed);
    }
    pub fn resume_background_drain(&self) {
        self.drain_paused.store(false, Ordering::Relaxed);
    }

    fn new_tmp_path(&self) -> PathBuf {
        let seq = self.file_seq.fetch_add(1, Ordering::Relaxed);
        let ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis())
            .unwrap_or(0);
        self.dir.join(format!("queen-{:013}-{:08}.tmp", ms, seq))
    }

    /// Append one push event to the active spool file, rotating at
    /// `max_events_per_file`. Returns true on success; on any I/O error the event
    /// is counted as failed and false is returned (the caller reports "failed").
    pub fn write_event(
        &self,
        queue: &str,
        partition: &str,
        transaction_id: &str,
        producer_sub: Option<&str>,
        payload: &RawValue,
    ) -> bool {
        let ev = WriteEvent {
            queue,
            partition,
            transaction_id,
            producer_sub,
            payload,
        };
        let body = match serde_json::to_vec(&ev) {
            Ok(b) => b,
            Err(_) => {
                self.failed.fetch_add(1, Ordering::Relaxed);
                return false;
            }
        };
        let mut guard = self.active.lock().unwrap();
        // Rotate if the current file is full or absent.
        let need_new = match guard.as_ref() {
            None => true,
            Some(a) => a.count >= self.cfg.max_events_per_file,
        };
        if need_new {
            if let Some(old) = guard.take() {
                finalize_path(&old.path);
            }
            let path = self.new_tmp_path();
            match std::fs::OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => {
                    *guard = Some(Active {
                        file,
                        path,
                        count: 0,
                        last_write: Instant::now(),
                    })
                }
                Err(e) => {
                    eprintln!("file_buffer: open {} failed: {}", path.display(), e);
                    self.failed.fetch_add(1, Ordering::Relaxed);
                    return false;
                }
            }
        }
        let a = guard.as_mut().unwrap();
        let len = (body.len() as u32).to_le_bytes();
        if a.file.write_all(&len).and_then(|_| a.file.write_all(&body)).is_err() {
            self.failed.fetch_add(1, Ordering::Relaxed);
            return false;
        }
        a.count += 1;
        a.last_write = Instant::now();
        self.pending.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Finalize the active file (rename `.tmp` → `.buf`) so its events become
    /// drainable. Called before each drain cycle and by `force_finalize_all`
    /// (maintenance disable). No-op when there is no active file with events.
    pub fn force_finalize_all(&self) {
        let mut guard = self.active.lock().unwrap();
        if let Some(a) = guard.take() {
            if a.count > 0 {
                finalize_path(&a.path);
            } else {
                // Empty active file — discard it rather than leaving an empty .buf.
                let _ = std::fs::remove_file(&a.path);
            }
        }
    }

    /// buffer stats block, shaped like the C++ get_buffer_stats()
    /// (async_queue_manager.cpp:1157-1188).
    pub fn buffer_stats(&self) -> serde_json::Value {
        let (count, total_bytes) = self.finalized_file_stats();
        serde_json::json!({
            "pendingCount": self.pending_count(),
            "failedCount": self.failed_count(),
            "dbHealthy": self.db_healthy(),
            "failedFiles": {
                "count": count,
                "totalBytes": total_bytes,
                "totalMB": (total_bytes as f64) / (1024.0 * 1024.0),
                "failoverCount": count,
                "qos0Count": 0,
            }
        })
    }

    fn finalized_file_stats(&self) -> (usize, u64) {
        let mut count = 0usize;
        let mut total = 0u64;
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) == Some("buf") {
                    count += 1;
                    if let Ok(m) = e.metadata() {
                        total += m.len();
                    }
                }
            }
        }
        (count, total)
    }

    /// Finalized `.buf` files, oldest-first (lexical sort == FIFO).
    fn list_buf_files(&self) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = Vec::new();
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) == Some("buf") {
                    v.push(p);
                }
            }
        }
        v.sort();
        v
    }

    /// Drain leftover spool before serving (item 1 startup recovery,
    /// file_buffer.cpp:206-245). Any crash-left `.tmp` files are finalized first,
    /// then all `.buf` are replayed FIFO. Capped at `MAX_STARTUP_RECOVERY`.
    pub async fn startup_recovery(&self, pool: &Pool) {
        // Recover leftover .tmp (a crash mid-outage) by finalizing them.
        if let Ok(rd) = std::fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) == Some("tmp") {
                    finalize_path(&p);
                }
            }
        }
        let files = self.list_buf_files();
        if files.is_empty() {
            return;
        }
        println!("file_buffer: startup recovery draining {} spool file(s)", files.len());
        let start = Instant::now();
        for f in files {
            if start.elapsed() >= MAX_STARTUP_RECOVERY {
                eprintln!("file_buffer: startup recovery hit {}s cap; leaving remaining spool for the drain loop", MAX_STARTUP_RECOVERY.as_secs());
                break;
            }
            match self.drain_file(pool, &f).await {
                Ok(n) => {
                    let _ = std::fs::remove_file(&f);
                    self.pending.fetch_sub(n.min(self.pending.load(Ordering::Relaxed)), Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("file_buffer: startup recovery of {} failed: {} (will retry in drain loop)", f.display(), e);
                    self.db_healthy.store(false, Ordering::Relaxed);
                    break;
                }
            }
        }
    }

    /// One drain cycle: finalize the active file, then replay `.buf` files
    /// oldest-first. Honors pause (maintenance) and the circuit breaker.
    async fn drain_cycle(&self, pool: &Pool) {
        if self.drain_paused.load(Ordering::Relaxed) {
            return;
        }
        // Circuit breaker cooldown.
        {
            let mut cd = self.cooldown_until.lock().unwrap();
            if let Some(until) = *cd {
                if Instant::now() < until {
                    return;
                }
                *cd = None;
            }
        }
        // Make in-flight events drainable.
        self.force_finalize_all();
        let files = self.list_buf_files();
        if files.is_empty() {
            return;
        }
        let cap = if self.db_healthy.load(Ordering::Relaxed) {
            DRAIN_FILES_HEALTHY
        } else {
            DRAIN_FILES_UNHEALTHY
        };
        for f in files.into_iter().take(cap) {
            match self.drain_file(pool, &f).await {
                Ok(n) => {
                    let _ = std::fs::remove_file(&f);
                    self.pending.fetch_sub(n.min(self.pending.load(Ordering::Relaxed)), Ordering::Relaxed);
                    self.db_healthy.store(true, Ordering::Relaxed);
                    self.consecutive_failures.store(0, Ordering::Relaxed);
                }
                Err(e) => {
                    eprintln!("file_buffer: drain {} failed: {}", f.display(), e);
                    self.db_healthy.store(false, Ordering::Relaxed);
                    let fails = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if fails >= MAX_CONSECUTIVE_FAILURES {
                        *self.cooldown_until.lock().unwrap() = Some(Instant::now() + COOLDOWN);
                        self.consecutive_failures.store(0, Ordering::Relaxed);
                    }
                    break; // stop this cycle; retry next tick
                }
            }
        }
    }

    /// Replay one spool file: read its events, group by (queue, partition), rebuild
    /// one segment per group (fresh message ids, ORIGINAL transactionIds), and push
    /// them in one multi-segment transaction. Returns the number of events on
    /// success. An Err means the DB is (probably) still down — the file is left in
    /// place for a later retry.
    async fn drain_file(&self, pool: &Pool, path: &Path) -> Result<usize, String> {
        let events = read_events(path).map_err(|e| format!("read: {e}"))?;
        if events.is_empty() {
            return Ok(0);
        }
        let n = events.len();
        // Replay in batches of `max_batch_size` (parity with FILE_BUFFER_MAX_BATCH)
        // so a large spool file is not one giant transaction. If a later batch
        // fails, earlier committed batches replay idempotently on the next
        // full-file retry (dedup keys on the preserved transactionId).
        let batch = self.cfg.max_batch_size.max(1);
        let mut chunk: Vec<StoredEvent> = Vec::with_capacity(batch);
        let mut it = events.into_iter();
        loop {
            chunk.clear();
            for ev in it.by_ref().take(batch) {
                chunk.push(ev);
            }
            if chunk.is_empty() {
                break;
            }
            self.replay_batch(pool, &chunk).await?;
        }
        Ok(n)
    }

    /// Replay one batch of events as a single multi-segment transaction: group by
    /// (queue, partition), rebuild one segment per group, push. An Err means the DB
    /// is (probably) down — leave the file for retry.
    async fn replay_batch(&self, pool: &Pool, events: &[StoredEvent]) -> Result<(), String> {
        let mut order: Vec<(String, String)> = Vec::new();
        let mut groups: std::collections::HashMap<(String, String), Vec<&StoredEvent>> =
            std::collections::HashMap::new();
        for ev in events {
            let key = (ev.queue.clone(), ev.partition.clone());
            if !groups.contains_key(&key) {
                order.push(key.clone());
            }
            groups.entry(key).or_default().push(ev);
        }

        let mut segs = String::with_capacity(order.len() * 160);
        segs.push('[');
        let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(order.len());
        for (pos, key) in order.iter().enumerate() {
            if pos > 0 {
                segs.push(',');
            }
            let evs = &groups[key];
            let (metas, blob) = build_segment(evs, self.zstd_level);
            segs.push_str("{\"queue\":\"");
            json_escape_into(&mut segs, &key.0);
            segs.push_str("\",\"partition\":\"");
            json_escape_into(&mut segs, &key.1);
            segs.push_str("\",\"metas\":");
            segs.push_str(&metas);
            segs.push_str(",\"msg_count\":");
            segs.push_str(&evs.len().to_string());
            segs.push('}');
            blobs.push(blob);
        }
        segs.push(']');

        let client = pool.get().await.map_err(|e| format!("pool: {e}"))?;
        db::push_segments_multi(&client, &segs, blobs)
            .await
            .map_err(|e| format!("push: {e}"))?;
        Ok(())
    }
}

use crate::db;

/// Build (metas JSON, zstd blob) for a group of spooled events. Fresh message ids
/// are minted (dedup keys on transactionId, not message_id), original
/// transactionIds and producer_sub are preserved.
fn build_segment(evs: &[&StoredEvent], zstd_level: i32) -> (String, Vec<u8>) {
    let mids: Vec<[u8; 16]> = evs.iter().map(|_| uuidv7_bytes()).collect();
    let mut metas = String::with_capacity(evs.len() * 80 + 2);
    metas.push('[');
    for (i, ev) in evs.iter().enumerate() {
        if i > 0 {
            metas.push(',');
        }
        metas.push_str("{\"i\":");
        metas.push_str(&i.to_string());
        metas.push_str(",\"mid\":\"");
        metas.push_str(&uuid_bytes_to_string(&mids[i]));
        metas.push_str("\",\"txn\":\"");
        json_escape_into(&mut metas, &ev.transaction_id);
        metas.push_str("\"}");
    }
    metas.push(']');

    let fins: Vec<FrameIn> = evs
        .iter()
        .enumerate()
        .map(|(i, ev)| FrameIn {
            message_id: mids[i],
            txn: &ev.transaction_id,
            trace_id: None,
            producer_sub: ev.producer_sub.as_deref(),
            payload: ev.payload.get().as_bytes(),
            encrypted: false,
        })
        .collect();
    let blob = zstd_compress(&pack_frames(&fins), zstd_level);
    (metas, blob)
}

/// Rename an active `.tmp` to its `.buf` sibling (drainable). Best-effort.
fn finalize_path(tmp: &Path) {
    if tmp.extension().and_then(|s| s.to_str()) != Some("tmp") {
        return;
    }
    let buf = tmp.with_extension("buf");
    if let Err(e) = std::fs::rename(tmp, &buf) {
        eprintln!("file_buffer: finalize {} failed: {}", tmp.display(), e);
    }
}

/// Read all events from a spool file. Tolerates a truncated trailing record (a
/// crash mid-write) by stopping at the first incomplete frame.
fn read_events(path: &Path) -> std::io::Result<Vec<StoredEvent>> {
    let mut f = std::fs::File::open(path)?;
    let mut raw = Vec::new();
    f.read_to_end(&mut raw)?;
    let mut out = Vec::new();
    let mut o = 0usize;
    while o + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[o], raw[o + 1], raw[o + 2], raw[o + 3]]) as usize;
        let end = o + 4 + len;
        if end > raw.len() {
            break; // truncated final record — ignore it
        }
        if let Ok(ev) = serde_json::from_slice::<StoredEvent>(&raw[o + 4..end]) {
            out.push(ev);
        }
        o = end;
    }
    Ok(out)
}

/// Launch the background drain loop (item 1). Non-blocking. Every
/// `flush_interval_ms` it runs one drain cycle (skipped while paused / in cooldown).
pub fn spawn_drain(manager: std::sync::Arc<FileBufferManager>, pool: Pool) {
    let interval = Duration::from_millis(manager.cfg.flush_interval_ms);
    println!(
        "file_buffer: drain loop started (dir={}, flush={}ms)",
        manager.dir.display(),
        manager.cfg.flush_interval_ms
    );
    tokio::spawn(async move {
        loop {
            let start = Instant::now();
            manager.drain_cycle(&pool).await;
            let sleep = interval.checked_sub(start.elapsed()).unwrap_or(Duration::ZERO);
            tokio::time::sleep(sleep).await;
        }
    });
}
