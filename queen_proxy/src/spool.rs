//! Disk spool for meter samples when pxdb is down. OWNER: Agent D.
//! Simplified single-writer JSONL sibling of the broker's file_buffer.rs
//! (server/src/file_buffer.rs) pattern — no binary frames, no per-message
//! dedup concerns (usage rollups are additive/idempotent by construction via
//! the UPSERT), so the elaborate .tmp->.buf finalize dance isn't needed:
//! files are written directly as `meter-<epoch_ms>.buf` and are only ever
//! read back by `recover()`, which runs exactly once, at process startup,
//! strictly before this process's own spool writer can produce anything —
//! so a `.buf` file `recover()` sees was always left complete by a *prior*
//! process instance.
//!
//! Rotation: size-based only, 5MB. Durability: flush (no per-line fsync) —
//! "best-effort", matching the task spec; a lost tail on an unclean crash is
//! acceptable for usage metering (unlike message payloads). Circuit breaker:
//! minimal — after 3 consecutive recovery failures, sleep 30s before trying
//! the next file, so a startup recovery pass against a still-down DB never
//! hammers it.

use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::meter::UsageRow;

const ROTATE_BYTES: u64 = 5 * 1024 * 1024;
const BREAKER_THRESHOLD: u32 = 3;
const BREAKER_COOLDOWN: Duration = Duration::from_secs(30);

struct ActiveFile {
    file: File,
    path: PathBuf,
    bytes: u64,
}

pub struct Spool {
    dir: PathBuf,
    active: Mutex<Option<ActiveFile>>,
    rotate_bytes: u64,
    cooldown: Duration,
    /// Disambiguates filenames for rotations that land in the same
    /// millisecond (file_buffer.rs's `file_seq` pattern) — the epoch-ms
    /// prefix alone isn't enough on a fast machine, and it must vary per
    /// call, not just per instance, so it cannot be derived from `self`.
    file_seq: AtomicU64,
}

impl Spool {
    pub fn new(dir: &str) -> Spool {
        Spool::with_params(dir, ROTATE_BYTES, BREAKER_COOLDOWN)
    }

    fn with_params(dir: &str, rotate_bytes: u64, cooldown: Duration) -> Spool {
        let dir = PathBuf::from(dir);
        if let Err(e) = fs::create_dir_all(&dir) {
            tracing::warn!(target: "spool", dir = %dir.display(), error = %e, "could not create meter spool dir");
        }
        Spool { dir, active: Mutex::new(None), rotate_bytes, cooldown, file_seq: AtomicU64::new(0) }
    }

    /// Append rows as JSONL to the active spool file, rotating past
    /// `rotate_bytes`. Best-effort: an I/O error drops that row with a warn
    /// (the spool is already the last resort after a DB write failed — there
    /// is no further fallback).
    pub fn write(&self, rows: &[UsageRow]) {
        if rows.is_empty() {
            return;
        }
        let mut guard = self.active.lock().unwrap();
        for row in rows {
            let mut line = match serde_json::to_vec(row) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!(target: "spool", error = %e, "meter row serialize failed; dropping row");
                    continue;
                }
            };
            line.push(b'\n');

            let need_new = match guard.as_ref() {
                None => true,
                Some(a) => a.bytes >= self.rotate_bytes,
            };
            if need_new {
                let path = self.new_path();
                match OpenOptions::new().create(true).append(true).open(&path) {
                    Ok(file) => *guard = Some(ActiveFile { file, path, bytes: 0 }),
                    Err(e) => {
                        tracing::warn!(target: "spool", path = %path.display(), error = %e, "open spool file failed; dropping meter row");
                        continue;
                    }
                }
            }

            if let Some(a) = guard.as_mut() {
                if let Err(e) = a.file.write_all(&line) {
                    tracing::warn!(target: "spool", path = %a.path.display(), error = %e, "spool write failed; dropping row");
                    continue;
                }
                let _ = a.file.flush(); // best-effort: flush, no fsync per row
                a.bytes += line.len() as u64;
            }
        }
    }

    fn new_path(&self) -> PathBuf {
        let ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
        let seq = self.file_seq.fetch_add(1, Ordering::Relaxed);
        // epoch-ms prefix + monotonic sequence: same lexical-sort-is-FIFO
        // property as file_buffer.rs, and the sequence (not just the
        // millisecond) guarantees uniqueness for rotations that land in the
        // same millisecond.
        self.dir.join(format!("meter-{ms:013}-{seq:08}.buf"))
    }

    fn list_files(&self) -> Vec<PathBuf> {
        let mut v = Vec::new();
        if let Ok(rd) = fs::read_dir(&self.dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) == Some("buf") {
                    v.push(p);
                }
            }
        }
        v.sort(); // epoch-ms prefix -> lexical sort is FIFO
        v
    }

    /// One-shot recovery: replay every existing `*.buf` file (oldest first)
    /// through `persist` — normally the DB UPSERT — deleting each file that
    /// fully succeeds. Minimal circuit breaker: after `BREAKER_THRESHOLD`
    /// consecutive failures (read errors or persist errors both count),
    /// sleep `cooldown` before moving on to the next file, so a startup pass
    /// against a still-down DB backs off instead of hammering it. A file
    /// that fails is left in place for the next process restart's recovery
    /// pass (this call never retries the same file within itself).
    pub async fn recover<F, Fut>(&self, persist: F)
    where
        F: Fn(Vec<UsageRow>) -> Fut,
        Fut: std::future::Future<Output = Result<(), String>>,
    {
        let files = self.list_files();
        if files.is_empty() {
            return;
        }
        tracing::info!(target: "spool", count = files.len(), "meter spool recovery starting");
        let mut consecutive = 0u32;
        let mut recovered = 0usize;
        for f in &files {
            let rows = match read_rows(f) {
                Ok(r) => r,
                Err(e) => {
                    tracing::warn!(target: "spool", file = %f.display(), error = %e, "unreadable spool file; leaving in place");
                    consecutive += 1;
                    if consecutive >= BREAKER_THRESHOLD {
                        tracing::warn!(target: "spool", cooldown_s = self.cooldown.as_secs(), "meter spool circuit breaker tripped");
                        tokio::time::sleep(self.cooldown).await;
                        consecutive = 0;
                    }
                    continue;
                }
            };
            if rows.is_empty() {
                let _ = fs::remove_file(f); // empty/corrupt-trailer-only file, nothing to lose
                continue;
            }
            match persist(rows).await {
                Ok(()) => {
                    let _ = fs::remove_file(f);
                    consecutive = 0;
                    recovered += 1;
                }
                Err(e) => {
                    tracing::warn!(target: "spool", file = %f.display(), error = %e, "spool recovery write failed; will retry next restart");
                    consecutive += 1;
                    if consecutive >= BREAKER_THRESHOLD {
                        tracing::warn!(target: "spool", cooldown_s = self.cooldown.as_secs(), "meter spool circuit breaker tripped");
                        tokio::time::sleep(self.cooldown).await;
                        consecutive = 0;
                    }
                }
            }
        }
        tracing::info!(target: "spool", recovered, total = files.len(), "meter spool recovery done");
    }

    #[cfg(test)]
    fn new_for_test(dir: &std::path::Path, rotate_bytes: u64, cooldown: Duration) -> Spool {
        Spool::with_params(dir.to_str().unwrap(), rotate_bytes, cooldown)
    }
}

fn read_rows(path: &Path) -> std::io::Result<Vec<UsageRow>> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<UsageRow>(&line) {
            Ok(r) => out.push(r),
            Err(e) => {
                tracing::warn!(target: "spool", file = %path.display(), error = %e, "skipping malformed spool line")
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;
    use uuid::Uuid;

    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn tempdir() -> TempDir {
        let mut p = std::env::temp_dir();
        p.push(format!("queen-proxy-spool-test-{}", Uuid::new_v4()));
        fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }

    fn row(n: u64) -> UsageRow {
        UsageRow { cluster_id: Uuid::new_v4(), minute: 1000 + n, op: "push".to_string(), reqs: n, msgs: n * 2, bytes_in: n * 10, bytes_out: n }
    }

    #[tokio::test]
    async fn write_then_recover_roundtrip() {
        let dir = tempdir();
        // Separate writer/reader Spool instances on the same directory,
        // mirroring reality: recover() only ever runs at the *next* process's
        // startup, never against a still-open file this same instance wrote.
        let writer = Spool::new_for_test(dir.path(), ROTATE_BYTES, Duration::from_millis(10));
        let rows: Vec<UsageRow> = (0..5).map(row).collect();
        writer.write(&rows[0..2]);
        writer.write(&rows[2..5]);
        drop(writer);

        assert_eq!(list_buf_files(dir.path()).len(), 1, "under the 5MB rotation threshold, one file");

        let received: Arc<Mutex<Vec<UsageRow>>> = Arc::new(Mutex::new(Vec::new()));
        let received2 = received.clone();
        let reader = Spool::new_for_test(dir.path(), ROTATE_BYTES, Duration::from_millis(10));
        reader
            .recover(move |batch| {
                let received2 = received2.clone();
                async move {
                    received2.lock().unwrap().extend(batch);
                    Ok(())
                }
            })
            .await;

        let got = received.lock().unwrap();
        assert_eq!(got.len(), 5);
        for r in &rows {
            assert!(got.contains(r), "row {r:?} should have been recovered");
        }
        assert!(list_buf_files(dir.path()).is_empty(), "fully-recovered files must be deleted");
    }

    #[tokio::test]
    async fn rotation_creates_multiple_files_past_the_byte_threshold() {
        let dir = tempdir();
        // Tiny threshold: every write() call lands in its own file.
        let writer = Spool::new_for_test(dir.path(), 1, Duration::from_millis(10));
        writer.write(&[row(1)]);
        writer.write(&[row(2)]);
        writer.write(&[row(3)]);
        drop(writer);
        assert_eq!(list_buf_files(dir.path()).len(), 3);
    }

    #[tokio::test]
    async fn circuit_breaker_sleeps_after_three_consecutive_failures() {
        let dir = tempdir();
        let cooldown = Duration::from_millis(30);
        let writer = Spool::new_for_test(dir.path(), 1, cooldown); // 1 row per file
        for n in 0..4 {
            writer.write(&[row(n)]);
        }
        drop(writer);
        assert_eq!(list_buf_files(dir.path()).len(), 4);

        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        let reader = Spool::new_for_test(dir.path(), 1, cooldown);
        let start = std::time::Instant::now();
        reader
            .recover(move |_rows| {
                let n = calls2.fetch_add(1, Ordering::SeqCst);
                async move {
                    if n < 3 {
                        Err("simulated db down".to_string())
                    } else {
                        Ok(())
                    }
                }
            })
            .await;
        let elapsed = start.elapsed();

        assert_eq!(calls.load(Ordering::SeqCst), 4, "all four files attempted");
        assert!(elapsed >= cooldown, "breaker must have slept at least one cooldown, elapsed={elapsed:?}");
        // 3 failed and were left in place; the 4th succeeded and was removed.
        assert_eq!(list_buf_files(dir.path()).len(), 3);
    }

    #[tokio::test]
    async fn recover_on_empty_dir_is_a_noop() {
        let dir = tempdir();
        let spool = Spool::new_for_test(dir.path(), ROTATE_BYTES, Duration::from_millis(10));
        let calls = Arc::new(AtomicU32::new(0));
        let calls2 = calls.clone();
        spool
            .recover(move |_rows| {
                calls2.fetch_add(1, Ordering::SeqCst);
                async move { Ok(()) }
            })
            .await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    fn list_buf_files(dir: &Path) -> Vec<PathBuf> {
        let mut v = Vec::new();
        if let Ok(rd) = fs::read_dir(dir) {
            for e in rd.flatten() {
                let p = e.path();
                if p.extension().and_then(|s| s.to_str()) == Some("buf") {
                    v.push(p);
                }
            }
        }
        v
    }
}
