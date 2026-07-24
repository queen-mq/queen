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
use crate::frames::{pack_frames, zstd_compress, FrameIn};
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
/// RUSTFIX item 1: a spool file that fails with a PERMANENT (bad-data / server-side
/// SQL) error this many times in a row is moved to the `failed/` quarantine dir so
/// one poison file stops blocking replay of every newer buffered message. A DB
/// outage produces TRANSIENT errors, which never count toward this — good spool is
/// never quarantined during an outage.
const MAX_FILE_ATTEMPTS: u32 = 5;

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
    // RUSTFIX item 8: true when `payload` is an {encrypted,iv,authTag} envelope, so
    // the drain re-stamps FLAG_ENCRYPTED on the replayed frame (else isEncrypted is
    // wrongly reported false for buffered->drained messages on encrypted queues).
    encrypted: bool,
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
    // #[serde(default)] so spool files written before this field existed (which
    // stored the envelope with no flag) deserialize to false — still decrypt via
    // the read-path envelope sniff, identical to their pre-fix behavior.
    #[serde(default)]
    encrypted: bool,
    payload: Box<RawValue>,
}

/// A drain failure, classified so the drain loop can tell a recoverable outage
/// (leave the file, back off, retry) from a poison file that will never succeed (a
/// server-side data/SQL rejection or an unreadable spool file) and must be
/// quarantined so the FIFO advances.
enum DrainErr {
    Transient(String),
    Permanent(String),
}

impl std::fmt::Display for DrainErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DrainErr::Transient(e) => write!(f, "{e} (transient)"),
            DrainErr::Permanent(e) => write!(f, "{e} (permanent)"),
        }
    }
}

/// Classify a DB push error. A connection-level failure (no SQLSTATE) and the
/// retryable serialization/deadlock/connection/resource SQLSTATE classes are
/// transient; any other server-side SQL error (bad data, constraint, custom RAISE)
/// is permanent — the same data will fail every retry, so quarantine it.
fn classify_push_error(e: &tokio_postgres::Error) -> DrainErr {
    match e.as_db_error() {
        None => DrainErr::Transient(e.to_string()),
        Some(db) => {
            let code = db.code().code();
            let class = &code[..2.min(code.len())];
            let transient = code == "40001" // serialization_failure
                || code == "40P01" // deadlock_detected
                || matches!(class, "08" | "53" | "57" | "58");
            if transient {
                DrainErr::Transient(db.message().to_string())
            } else {
                DrainErr::Permanent(db.message().to_string())
            }
        }
    }
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
    /// RUSTFIX item 1: quarantine dir (`<dir>/failed`) for poison spool files.
    failed_dir: PathBuf,
    /// Head-of-FIFO poison tracker: (file, consecutive permanent-failure count).
    poison: Mutex<Option<(PathBuf, u32)>>,
}

impl FileBufferManager {
    pub fn new(cfg: FileBufferConfig, zstd_level: i32) -> FileBufferManager {
        let mut dir = PathBuf::from(&cfg.dir);
        if let Err(e) = std::fs::create_dir_all(&dir) {
            // The C++-parity default (/var/lib/queen/buffers) is only writable in
            // Linux/Docker deployments. On a dev machine (macOS, non-root) the
            // create fails — and without a fallback EVERY buffered push would be
            // dropped and counted as failed (the "N failed buffered messages"
            // dashboard symptom). Fall back to a per-user temp spool instead of
            // silently disabling durability; production should still set
            // FILE_BUFFER_DIR explicitly.
            let fallback = std::env::temp_dir().join("queen-buffers");
            tracing::warn!(
                target: "spool",
                dir = %dir.display(),
                error = %e,
                fallback = %fallback.display(),
                "could not create spool dir; falling back"
            );
            if let Err(e2) = std::fs::create_dir_all(&fallback) {
                tracing::error!(
                    target: "spool",
                    fallback = %fallback.display(),
                    error = %e2,
                    "fallback spool dir also failed; buffering WILL fail"
                );
            } else {
                dir = fallback;
            }
        }
        // RUSTFIX item 1: quarantine dir for poison files. It sits inside the spool
        // dir but is invisible to the drain (list_buf_files / finalized_file_stats /
        // the startup .tmp scan all filter by .buf/.tmp extension, non-recursively).
        let failed_dir = dir.join("failed");
        if let Err(e) = std::fs::create_dir_all(&failed_dir) {
            tracing::warn!(
                target: "spool",
                quarantine_dir = %failed_dir.display(),
                error = %e,
                "could not create quarantine dir; poison files can't be quarantined"
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
            failed_dir,
            poison: Mutex::new(None),
        }
    }

    /// Record a permanent failure for the current FIFO-head file; returns its
    /// running consecutive count. Switching to a different head resets the count.
    fn note_poison(&self, path: &Path) -> u32 {
        let mut g = self.poison.lock().unwrap();
        match g.as_mut() {
            Some((p, n)) if p == path => {
                *n += 1;
                *n
            }
            _ => {
                *g = Some((path.to_path_buf(), 1));
                1
            }
        }
    }

    fn clear_poison(&self) {
        *self.poison.lock().unwrap() = None;
    }

    /// Move a poison spool file into `failed/` so the FIFO advances. Last resort on
    /// a rename failure: delete it — its data is a permanent SQL failure that can
    /// never be persisted, and leaving it would re-freeze the FIFO.
    fn quarantine(&self, path: &Path) {
        let dest = match path.file_name() {
            Some(n) => self.failed_dir.join(n),
            None => return,
        };
        match std::fs::rename(path, &dest) {
            Ok(_) => tracing::warn!(
                target: "spool",
                spool_file = %path.display(),
                dest = %dest.display(),
                "quarantined poison spool file"
            ),
            Err(e) => {
                tracing::warn!(
                    target: "spool",
                    spool_file = %path.display(),
                    error = %e,
                    "could not quarantine poison file; deleting it to unblock the FIFO"
                );
                let _ = std::fs::remove_file(path);
            }
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
        encrypted: bool,
        payload: &RawValue,
    ) -> bool {
        let ev = WriteEvent {
            queue,
            partition,
            transaction_id,
            producer_sub,
            encrypted,
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
                    static OPEN_FAIL: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = OPEN_FAIL.tick_now() {
                        tracing::error!(
                            target: "spool",
                            spool_file = %path.display(),
                            error = %e,
                            suppressed,
                            "open spool file failed"
                        );
                    }
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
        tracing::info!(target: "spool", count = files.len(), "startup recovery draining spool files");
        let start = Instant::now();
        for f in files {
            if start.elapsed() >= MAX_STARTUP_RECOVERY {
                tracing::warn!(
                    target: "spool",
                    cap_secs = MAX_STARTUP_RECOVERY.as_secs(),
                    "startup recovery hit cap; leaving remaining spool for the drain loop"
                );
                break;
            }
            match self.drain_file(pool, &f).await {
                Ok(n) => {
                    let _ = std::fs::remove_file(&f);
                    self.pending.fetch_sub(n.min(self.pending.load(Ordering::Relaxed)), Ordering::Relaxed);
                }
                Err(e) => {
                    tracing::warn!(
                        target: "spool",
                        spool_file = %f.display(),
                        error = %e,
                        "startup recovery of file failed; will retry in drain loop"
                    );
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
                    self.clear_poison(); // made progress on the FIFO head
                }
                Err(DrainErr::Transient(e)) => {
                    // DB (probably) down: leave the file, trip the circuit breaker,
                    // retry next cycle. Never quarantines — good spool survives an outage.
                    static DRAIN_TRANSIENT: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = DRAIN_TRANSIENT.tick_now() {
                        tracing::warn!(
                            target: "spool",
                            spool_file = %f.display(),
                            error = %e,
                            suppressed,
                            "drain failed (transient)"
                        );
                    }
                    self.db_healthy.store(false, Ordering::Relaxed);
                    let fails = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if fails >= MAX_CONSECUTIVE_FAILURES {
                        *self.cooldown_until.lock().unwrap() = Some(Instant::now() + COOLDOWN);
                        self.consecutive_failures.store(0, Ordering::Relaxed);
                    }
                    break; // stop this cycle; retry next tick
                }
                Err(DrainErr::Permanent(e)) => {
                    // The DB is reachable but this file's data can't be stored. Count
                    // consecutive failures of THIS head file (DB stays "healthy", the
                    // transient breaker is untouched); quarantine after
                    // MAX_FILE_ATTEMPTS so a poison file stops blocking newer ones.
                    let attempts = self.note_poison(&f);
                    static DRAIN_PERMANENT: crate::obs::Sampler = crate::obs::Sampler::new(10_000);
                    if let Some(suppressed) = DRAIN_PERMANENT.tick_now() {
                        tracing::error!(
                            target: "spool",
                            spool_file = %f.display(),
                            attempt = attempts,
                            max_attempts = MAX_FILE_ATTEMPTS,
                            error = %e,
                            suppressed,
                            "drain failed (permanent)"
                        );
                    }
                    if attempts >= MAX_FILE_ATTEMPTS {
                        self.quarantine(&f);
                        self.clear_poison();
                        continue; // advance the FIFO; drain the rest of this cycle
                    }
                    break; // retry the head next cycle (a mis-classified retryable
                           // error gets MAX_FILE_ATTEMPTS chances before quarantine)
                }
            }
        }
    }

    /// Replay one spool file: read its events, group by (queue, partition), rebuild
    /// one segment per group (fresh message ids, ORIGINAL transactionIds), and push
    /// them in one multi-segment transaction. Returns the number of events on
    /// success. An Err means the DB is (probably) still down — the file is left in
    /// place for a later retry.
    async fn drain_file(&self, pool: &Pool, path: &Path) -> Result<usize, DrainErr> {
        // A spool file we cannot read is corrupt — permanent (retrying can't fix it).
        let events = read_events(path).map_err(|e| DrainErr::Permanent(format!("read: {e}")))?;
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
    /// (queue, partition), rebuild one segment per group, push via
    /// queen.log_push_multi_v1 (db::log_push_multi typed arrays). Hashes are
    /// recomputed from the spooled transactionIds (crate::util::txn_hash128 — the
    /// on-disk format already carries the txn strings, unchanged); `verified` is
    /// -1 for every replayed segment, so SQL probes the whole dedup window — the
    /// correct posture for replay (an earlier partially-committed drain attempt
    /// must be detected server-side, the broker cache can't vouch for it). A
    /// fully-committed-before segment therefore comes back {"status":"duplicate"}
    /// and writes nothing, which is exactly the idempotent-replay contract. An
    /// Err means the DB is (probably) down — leave the file for retry.
    async fn replay_batch(&self, pool: &Pool, events: &[StoredEvent]) -> Result<(), DrainErr> {
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

        let mut queues: Vec<String> = Vec::with_capacity(order.len());
        let mut partitions: Vec<String> = Vec::with_capacity(order.len());
        let mut counts: Vec<i32> = Vec::with_capacity(order.len());
        let mut hashes: Vec<Vec<u8>> = Vec::with_capacity(order.len());
        let mut verified: Vec<i64> = Vec::with_capacity(order.len());
        let mut blobs: Vec<Vec<u8>> = Vec::with_capacity(order.len());
        for key in &order {
            let evs = &groups[key];
            let (seg_hashes, blob) = build_segment(evs, self.zstd_level);
            queues.push(key.0.clone());
            partitions.push(key.1.clone());
            counts.push(evs.len() as i32);
            hashes.push(seg_hashes);
            verified.push(-1); // no broker vouching on replay — full-window probe
            blobs.push(blob);
        }

        // Pool acquisition failure = DB unreachable = transient.
        let client = pool
            .get()
            .await
            .map_err(|e| DrainErr::Transient(format!("pool: {e}")))?;
        db::log_push_multi(&client, &queues, &partitions, &counts, &hashes, &verified, &blobs)
            .await
            .map_err(|e| classify_push_error(&e))?;
        Ok(())
    }
}

use crate::db;

/// Build (hash blob, zstd blob) for a group of spooled events: 16 bytes of
/// xxh3_128(transactionId) per frame in frame order — the log-engine dedup /
/// ack-resolution token (18-log-engine.md §3) — plus the packed frame blob.
/// Fresh message ids are minted (dedup keys on the txn hash, not message_id),
/// original transactionIds and producer_sub are preserved.
fn build_segment(evs: &[&StoredEvent], zstd_level: i32) -> (Vec<u8>, Vec<u8>) {
    let mids: Vec<[u8; 16]> = evs.iter().map(|_| uuidv7_bytes()).collect();
    let mut hashes: Vec<u8> = Vec::with_capacity(evs.len() * 16);
    for ev in evs.iter() {
        hashes.extend_from_slice(&crate::util::txn_hash128(&ev.transaction_id));
    }

    let fins: Vec<FrameIn> = evs
        .iter()
        .enumerate()
        .map(|(i, ev)| FrameIn {
            message_id: mids[i],
            txn: &ev.transaction_id,
            trace_id: None,
            producer_sub: ev.producer_sub.as_deref(),
            payload: ev.payload.get().as_bytes(),
            // RUSTFIX item 8: restore the persisted flag so a buffered->drained
            // encrypted payload keeps FLAG_ENCRYPTED (was hardcoded false).
            encrypted: ev.encrypted,
        })
        .collect();
    let blob = zstd_compress(&pack_frames(&fins), zstd_level);
    (hashes, blob)
}

/// Rename an active `.tmp` to its `.buf` sibling (drainable). Best-effort.
fn finalize_path(tmp: &Path) {
    if tmp.extension().and_then(|s| s.to_str()) != Some("tmp") {
        return;
    }
    let buf = tmp.with_extension("buf");
    if let Err(e) = std::fs::rename(tmp, &buf) {
        tracing::error!(
            target: "spool",
            spool_file = %tmp.display(),
            error = %e,
            "finalize rename failed"
        );
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
    tracing::info!(
        target: "spool",
        dir = %manager.dir.display(),
        flush_ms = manager.cfg.flush_interval_ms,
        "drain loop started"
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
