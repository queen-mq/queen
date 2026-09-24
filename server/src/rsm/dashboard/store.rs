//! This node's dashboard rows ([`super::model`]): an in-memory window per row
//! kind, journaled to `<data dir>/dash.db` so a restart keeps its history.
//!
//! One store per PROCESS (every raft group of a node shares it): the rows
//! describe the node — its clients, its CPU — not a group. The journal uses
//! `local.db`'s framing (length, xxh3, payload) with JSON payloads; a torn tail
//! is cut on reopen and the file rewrites itself from memory once it holds
//! twice the live rows. Losing the unflushed tail loses a graph point, never
//! queue state.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use super::model::{ParkedRow, QueueRow, SystemRow, WorkerRow, US_PER_SEC};

const MAGIC: &[u8; 8] = b"QNDASH1\0";
const MAX_RECORD: usize = 16 << 20;

/// How long each kind is kept. Node rows are one per flush (a week of minutes
/// is ~10k rows); queue and parked rows are one per active queue per minute,
/// so they keep a day and are also capped by count.
#[derive(Clone, Copy, Debug)]
pub struct Retention {
    pub node_us: i64,
    pub queue_us: i64,
    pub max_queue_rows: usize,
}

impl Retention {
    /// `QUEEN_DASH_NODE_RETENTION_H` (default 168), `QUEEN_DASH_QUEUE_RETENTION_H`
    /// (default 24), `QUEEN_DASH_MAX_QUEUE_ROWS` (default 500 000).
    pub fn from_env() -> Retention {
        let h = |k: &str, d: i64| {
            std::env::var(k)
                .ok()
                .and_then(|v| v.trim().parse::<i64>().ok())
                .filter(|v| *v > 0)
                .unwrap_or(d)
        };
        Retention {
            node_us: h("QUEEN_DASH_NODE_RETENTION_H", 168) * 3600 * US_PER_SEC,
            queue_us: h("QUEEN_DASH_QUEUE_RETENTION_H", 24) * 3600 * US_PER_SEC,
            max_queue_rows: h("QUEEN_DASH_MAX_QUEUE_ROWS", 500_000) as usize,
        }
    }
}

/// One journal record: everything one flush wrote.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Flush {
    #[serde(default)]
    pub worker: Vec<WorkerRow>,
    #[serde(default)]
    pub system: Vec<SystemRow>,
    #[serde(default)]
    pub queue: Vec<QueueRow>,
    #[serde(default)]
    pub parked: Vec<ParkedRow>,
}

/// Rows in a time range, as a gather returns them (JSON on the wire).
pub type Rows = Flush;

#[derive(Default)]
struct Window {
    worker: VecDeque<WorkerRow>,
    system: VecDeque<SystemRow>,
    queue: VecDeque<QueueRow>,
    parked: VecDeque<ParkedRow>,
    /// Rows appended to the journal since it was last rewritten.
    journaled: usize,
}

impl Window {
    fn live(&self) -> usize {
        self.worker.len() + self.system.len() + self.queue.len() + self.parked.len()
    }

    fn add(&mut self, f: Flush) {
        self.worker.extend(f.worker);
        self.system.extend(f.system);
        self.queue.extend(f.queue);
        self.parked.extend(f.parked);
    }

    fn trim(&mut self, now_us: i64, r: &Retention) {
        let node_floor = now_us - r.node_us;
        let queue_floor = now_us - r.queue_us;
        while self.worker.front().is_some_and(|x| x.at_us < node_floor) {
            self.worker.pop_front();
        }
        while self.system.front().is_some_and(|x| x.at_us < node_floor) {
            self.system.pop_front();
        }
        while self.queue.front().is_some_and(|x| x.bucket_us < queue_floor)
            || self.queue.len() > r.max_queue_rows
        {
            self.queue.pop_front();
        }
        while self.parked.front().is_some_and(|x| x.bucket_us < queue_floor)
            || self.parked.len() > r.max_queue_rows
        {
            self.parked.pop_front();
        }
    }
}

/// The node's dashboard rows.
pub struct DashStore {
    path: PathBuf,
    retention: Retention,
    win: Mutex<Window>,
}

static GLOBAL: OnceLock<Arc<DashStore>> = OnceLock::new();

/// Open (once per process) the store at `path`; later calls return the first.
pub fn open_global(path: &Path) -> io::Result<Arc<DashStore>> {
    if let Some(s) = GLOBAL.get() {
        return Ok(s.clone());
    }
    let s = Arc::new(DashStore::open(path.to_path_buf(), Retention::from_env())?);
    Ok(GLOBAL.get_or_init(|| s).clone())
}

/// The process's store, once a facade opened it.
pub fn global() -> Option<Arc<DashStore>> {
    GLOBAL.get().cloned()
}

impl DashStore {
    pub fn open(path: PathBuf, retention: Retention) -> io::Result<DashStore> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut win = Window::default();
        match File::open(&path) {
            Ok(mut file) => {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                if bytes.len() >= MAGIC.len() && &bytes[..MAGIC.len()] == MAGIC {
                    let mut at = MAGIC.len();
                    let mut valid = at;
                    while at + 12 <= bytes.len() {
                        let len = u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap_or([0; 4]))
                            as usize;
                        let sum = u64::from_le_bytes(bytes[at + 4..at + 12].try_into().unwrap_or([0; 8]));
                        at += 12;
                        if len > MAX_RECORD || at + len > bytes.len() {
                            break;
                        }
                        let payload = &bytes[at..at + len];
                        if xxh3_64(payload) != sum {
                            break;
                        }
                        if let Ok(f) = serde_json::from_slice::<Flush>(payload) {
                            win.journaled += f.worker.len() + f.system.len() + f.queue.len() + f.parked.len();
                            win.add(f);
                        }
                        at += len;
                        valid = at;
                    }
                    if valid < bytes.len() {
                        OpenOptions::new().write(true).open(&path)?.set_len(valid as u64)?;
                    }
                } else if !bytes.is_empty() {
                    let _ = std::fs::rename(&path, path.with_extension("db.old"));
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        if !path.exists() {
            let mut file = OpenOptions::new().create_new(true).write(true).open(&path)?;
            file.write_all(MAGIC)?;
            file.sync_all()?;
        }
        win.trim(now_us(), &retention);
        Ok(DashStore {
            path,
            retention,
            win: Mutex::new(win),
        })
    }

    /// Record one flush: journal it, keep it in memory, drop what aged out.
    pub fn append(&self, f: Flush) {
        let n = f.worker.len() + f.system.len() + f.queue.len() + f.parked.len();
        if n == 0 {
            return;
        }
        let Ok(payload) = serde_json::to_vec(&f) else {
            return;
        };
        let mut win = self.win.lock().unwrap_or_else(|p| p.into_inner());
        let journaled = append_record(&self.path, &payload).is_ok();
        win.add(f);
        win.trim(now_us(), &self.retention);
        if journaled {
            win.journaled += n;
        }
        if win.journaled > 2 * win.live() + 1024 && rewrite(&self.path, &win).is_ok() {
            win.journaled = win.live();
        }
    }

    /// [`DashStore::range`] plus, when `totals`, ONE extra worker row stamped
    /// just before `from_us` that carries the counter sums of every older
    /// worker row this node keeps: what the status view's lifetime totals
    /// need, without shipping a week of rows on every refresh.
    pub fn range_with_totals(&self, from_us: i64, to_us: i64, totals: bool) -> Rows {
        let mut rows = self.range(from_us, to_us);
        if totals {
            let win = self.win.lock().unwrap_or_else(|p| p.into_inner());
            let mut t: Option<WorkerRow> = None;
            for r in win.worker.iter().filter(|r| r.at_us < from_us) {
                let acc = t.get_or_insert_with(|| WorkerRow {
                    at_us: from_us - 1,
                    hostname: r.hostname.clone(),
                    worker_id: r.worker_id,
                    pid: 0,
                    ..Default::default()
                });
                acc.push_requests += r.push_requests;
                acc.push_messages += r.push_messages;
                acc.pop_requests += r.pop_requests;
                acc.pop_messages += r.pop_messages;
                acc.ack_requests += r.ack_requests;
                acc.ack_messages += r.ack_messages;
                acc.ack_success += r.ack_success;
                acc.ack_failed += r.ack_failed;
                acc.transactions += r.transactions;
                acc.dlq += r.dlq;
                acc.db_errors += r.db_errors;
            }
            if let Some(t) = t {
                rows.worker.insert(0, t);
            }
        }
        rows
    }

    /// Every row stamped in `[from_us, to_us)`.
    pub fn range(&self, from_us: i64, to_us: i64) -> Rows {
        let win = self.win.lock().unwrap_or_else(|p| p.into_inner());
        let inr = |t: i64| t >= from_us && t < to_us;
        Rows {
            worker: win.worker.iter().filter(|r| inr(r.at_us)).cloned().collect(),
            system: win.system.iter().filter(|r| inr(r.at_us)).cloned().collect(),
            queue: win.queue.iter().filter(|r| inr(r.bucket_us)).cloned().collect(),
            parked: win.parked.iter().filter(|r| inr(r.bucket_us)).cloned().collect(),
        }
    }
}

fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

fn append_record(path: &Path, payload: &[u8]) -> io::Result<()> {
    let mut rec = Vec::with_capacity(payload.len() + 12);
    rec.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    rec.extend_from_slice(&xxh3_64(payload).to_le_bytes());
    rec.extend_from_slice(payload);
    OpenOptions::new().append(true).open(path)?.write_all(&rec)
}

fn rewrite(path: &Path, win: &Window) -> io::Result<()> {
    let tmp = path.with_extension("db.tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp)?;
    file.write_all(MAGIC)?;
    // Several records, so none comes near MAX_RECORD however many rows live.
    const CHUNK: usize = 5_000;
    let mut put = |f: &Flush| -> io::Result<()> {
        let payload = serde_json::to_vec(f).map_err(io::Error::other)?;
        file.write_all(&(payload.len() as u32).to_le_bytes())?;
        file.write_all(&xxh3_64(&payload).to_le_bytes())?;
        file.write_all(&payload)
    };
    for c in win.worker.iter().cloned().collect::<Vec<_>>().chunks(CHUNK) {
        put(&Flush { worker: c.to_vec(), ..Default::default() })?;
    }
    for c in win.system.iter().cloned().collect::<Vec<_>>().chunks(CHUNK) {
        put(&Flush { system: c.to_vec(), ..Default::default() })?;
    }
    for c in win.queue.iter().cloned().collect::<Vec<_>>().chunks(CHUNK) {
        put(&Flush { queue: c.to_vec(), ..Default::default() })?;
    }
    for c in win.parked.iter().cloned().collect::<Vec<_>>().chunks(CHUNK) {
        put(&Flush { parked: c.to_vec(), ..Default::default() })?;
    }
    file.sync_all()?;
    std::fs::rename(tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir() -> PathBuf {
        let d = std::env::temp_dir().join(format!(
            "queen-dash-{}-{}",
            std::process::id(),
            now_us()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn rows_survive_reopen_and_a_torn_tail_is_cut() {
        let d = dir();
        let path = d.join("dash.db");
        let now = now_us();
        let keep = Retention {
            node_us: 3600 * US_PER_SEC,
            queue_us: 3600 * US_PER_SEC,
            max_queue_rows: 10,
        };
        {
            let s = DashStore::open(path.clone(), keep).unwrap();
            s.append(Flush {
                worker: vec![WorkerRow {
                    at_us: now,
                    hostname: "n1".into(),
                    push_messages: 7,
                    ..Default::default()
                }],
                queue: vec![QueueRow {
                    bucket_us: now,
                    tenant: "t".into(),
                    queue: "q".into(),
                    push_messages: 7,
                    ..Default::default()
                }],
                ..Default::default()
            });
        }
        OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(&[9, 0, 0])
            .unwrap();
        let s = DashStore::open(path.clone(), keep).unwrap();
        let r = s.range(now - 1, now + 1);
        assert_eq!(r.worker.len(), 1);
        assert_eq!(r.queue[0].push_messages, 7);
        assert!(s.range(now + 1, now + 2).worker.is_empty());
        let _ = std::fs::remove_dir_all(d);
    }

    #[test]
    fn queue_rows_are_capped_by_count_and_age() {
        let d = dir();
        let now = now_us();
        let s = DashStore::open(
            d.join("dash.db"),
            Retention {
                node_us: 3600 * US_PER_SEC,
                queue_us: 60 * US_PER_SEC,
                max_queue_rows: 3,
            },
        )
        .unwrap();
        let q = |t: i64| QueueRow {
            bucket_us: t,
            tenant: "t".into(),
            queue: "q".into(),
            ..Default::default()
        };
        s.append(Flush {
            queue: vec![q(now - 120 * US_PER_SEC), q(now - 3), q(now - 2), q(now - 1), q(now)],
            ..Default::default()
        });
        let r = s.range(0, i64::MAX);
        assert_eq!(r.queue.len(), 3);
        assert_eq!(r.queue[0].bucket_us, now - 2);
        let _ = std::fs::remove_dir_all(d);
    }
}
