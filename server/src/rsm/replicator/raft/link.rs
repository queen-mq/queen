//! The source side of a cluster link ([`crate::rsm::link`]): what a node
//! serves a standby from its own log.
//!
//! Any node of the source can serve. The read is bounded by this node's
//! APPLIED index: an applied entry is committed, and nothing above the commit
//! point may leave the cluster (a later leader can still replace it). A
//! follower therefore serves the same entries as the leader, a moment later.
//!
//! # The standby's place in this log
//!
//! A standby names the last entry it applied by index and term. This node
//! answers with what follows only when its own log holds that entry: the term
//! it applied there ([`Shared::term_at`]), else the entry read back, else — at
//! the purge point exactly — the purged log id. A different term means the
//! standby replayed another log (this cluster was force-recovered from a node
//! that lacked those entries, or the standby points at the wrong cluster):
//! [`Answer::Mismatch`]. A position below the purge point cannot be served:
//! [`Answer::Purged`].
//!
//! # Holding the log
//!
//! The purge driver drops entries once every live follower has them. A
//! standby is not a follower, so each call it makes leaves a HOLD ([`Holds`]):
//! the purge stays at or below the standby's position. A standby reads one
//! node and tells the others where it is every few seconds
//! ([`Request::hold_only`]), so every node of the source keeps what the
//! standby still needs and can take the reads over; and a node writes its
//! holds to `raft/link_holds.json`, so it still knows them after a restart.
//!
//! A standby that starts from a SEED has read nothing when its snapshot is
//! made, and reads for the first time only once its node has staged the
//! snapshot, started on it and logged its standby entry. Its first hold is
//! therefore left by the seed itself ([`LinkSource::seed`]): the node that
//! sends the snapshot holds its log from the snapshot's position, under the
//! name the standby will read with, and the first read moves that hold like
//! any other.
//!
//! A hold costs the source disk, and the source's own writes come first. A
//! hold ends when:
//!
//! - the standby was silent for `QUEEN_LINK_HOLD_S`: it is given up on, as a
//!   follower is;
//! - the standby is below this node's purge point: nothing kept from here on
//!   would serve it;
//! - the node's data volume is `QUEEN_LINK_HOLD_DISK_PCT` full: the node
//!   purges as if nobody read it, before it would have to refuse writes.
//!
//! A standby whose entries are purged on every node of the source needs a new
//! seed.

use std::collections::{BTreeMap, HashMap};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use openraft::EntryPayload;

use super::log_store::{write_atomic, LogStore};
use super::snapshot::{stream_checkpoint, Held, SendCtx};
use super::types::{rsm_index, term_of};
use super::Shared;
use crate::rsm::link::wire::{Answer, Request, SourceEntry, MAX_BYTES_DEFAULT, WAIT_MS_MAX};

/// Entries read from the log per step of one answer.
const WINDOW: u64 = 64;
/// The most one answer carries, whatever the standby asks for (one entry is
/// always sent whole).
const MAX_BYTES_CAP: usize = 64 << 20;

/// The file a node keeps its holds in, under `raft/`.
const HOLDS_FILE: &str = "link_holds.json";
/// How often the holds are written while their readers read. A reader that is
/// new, or came back lower, is written at once; a position a few seconds old
/// only keeps a little more log after a restart.
const SAVE_EVERY: Duration = Duration::from_secs(10);
/// What a restarted node gives every reader it knew, however long the node
/// was away: the time to say where it is before anything it may need goes.
const RESTART_GRACE: Duration = Duration::from_secs(60);

/// `QUEEN_LINK_HOLD_S` (default 3600): how long a standby that stopped reading
/// still holds this node's log back from being purged. Longer than a
/// follower's hold (`QUEEN_RAFT_PURGE_HOLD_S`): what a standby that is given
/// up on costs is a whole new seed.
pub(crate) fn hold_from_env() -> Duration {
    Duration::from_secs(super::env_u64("QUEEN_LINK_HOLD_S", 3600))
}

/// `QUEEN_LINK_HOLD_DISK_PCT` (default: `QUEEN_RAFT_DISK_LOW_PCT`, itself 80):
/// how full the data volume may be while this node still keeps its log for a
/// standby. Below the fullness at which a node refuses writes
/// (`QUEEN_RAFT_DISK_HIGH_PCT`), so a standby that reads too slowly, or not at
/// all, loses its place and never costs the source its writes. 100: never.
pub(crate) fn hold_disk_pct_from_env() -> f64 {
    let pct = |name: &str| {
        std::env::var(name)
            .ok()
            .and_then(|v| v.trim().parse::<f64>().ok())
            .filter(|v| (1.0..=100.0).contains(v))
    };
    pct("QUEEN_LINK_HOLD_DISK_PCT")
        .or_else(|| pct("QUEEN_RAFT_DISK_LOW_PCT"))
        // The unit tests open their nodes on the developer's disk, however
        // full it is.
        .unwrap_or(if cfg!(test) { 100.0 } else { 80.0 })
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// How full the filesystem holding `dir` is, in percent.
fn disk_used_pct(dir: &Path) -> Option<f64> {
    let (total, available) = crate::syscollect::filesystem_usage(dir)?;
    (total > 0).then(|| total.saturating_sub(available) as f64 * 100.0 / total as f64)
}

/// One standby's hold.
#[derive(Clone, Copy)]
struct Hold {
    /// The source RSM index the standby has applied.
    after: u64,
    /// When it last said so: on this process's clock, which the hold's age is
    /// measured by, and on the wall clock (ms), which the file keeps.
    at: Instant,
    seen_ms: u64,
}

/// [`HOLDS_FILE`].
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct HoldsFile {
    readers: BTreeMap<String, HeldReader>,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct HeldReader {
    after: u64,
    #[serde(rename = "seenMs")]
    seen_ms: u64,
}

/// The standbys reading this cluster's log, as this node knows them, and how
/// far each has got.
pub(crate) struct Holds {
    readers: Mutex<HashMap<String, Hold>>,
    /// How long a silent standby keeps its hold.
    ttl: Duration,
    /// The node's `raft/` directory: where the holds are written, and the
    /// volume whose fullness ends them. `None`: holds in memory, never ended
    /// by a disk.
    dir: Option<PathBuf>,
    /// The fullness of that volume (percent) from which nothing is held; 100
    /// and above: never.
    disk_pct: f64,
    /// Whether the volume was that full when last looked at.
    released: AtomicBool,
    /// When the file was last written (or its write decided).
    saved: Mutex<Option<Instant>>,
    /// One writer of the file at a time.
    writing: Mutex<()>,
    /// A test's hand on the next seed ([`LinkSource::seed`]): called once,
    /// with the checkpoint its copy holds, between the copy and the hold
    /// left there.
    #[cfg(test)]
    after_copy: Mutex<Option<AfterCopy>>,
}

#[cfg(test)]
type AfterCopy = Box<dyn FnOnce(u64) + Send>;

/// One reader of this node's log, for a status page.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Reader {
    pub name: String,
    /// The RSM index the reader has applied.
    pub after: u64,
    /// How long ago it last read.
    pub idle: Duration,
}

impl Holds {
    /// Holds kept in memory only, for `ttl`.
    pub(crate) fn in_memory(ttl: Duration) -> Holds {
        Holds {
            readers: Mutex::new(HashMap::new()),
            ttl,
            dir: None,
            disk_pct: 100.0,
            released: AtomicBool::new(false),
            saved: Mutex::new(None),
            writing: Mutex::new(()),
            #[cfg(test)]
            after_copy: Mutex::new(None),
        }
    }

    /// The holds of the node whose `raft/` directory is `dir`: the ones it
    /// wrote before it stopped, each with the age it has now — and
    /// [`RESTART_GRACE`] left at least, since a standby could not reach a
    /// node that was down.
    pub(crate) fn open(dir: &Path, ttl: Duration, disk_pct: f64) -> Holds {
        let mut readers = HashMap::new();
        match std::fs::read(dir.join(HOLDS_FILE)) {
            Ok(bytes) => match serde_json::from_slice::<HoldsFile>(&bytes) {
                Ok(file) => {
                    let (now, wall) = (Instant::now(), wall_ms());
                    let oldest = ttl.saturating_sub(RESTART_GRACE);
                    for (name, h) in file.readers {
                        let age = Duration::from_millis(wall.saturating_sub(h.seen_ms)).min(oldest);
                        tracing::info!(
                            target: "rsm",
                            reader = %name,
                            after = h.after,
                            "rsm link: this node keeps its log for a standby it knew before it restarted",
                        );
                        readers.insert(
                            name,
                            Hold {
                                after: h.after,
                                at: now.checked_sub(age).unwrap_or(now),
                                seen_ms: h.seen_ms,
                            },
                        );
                    }
                }
                Err(e) => tracing::warn!(
                    target: "rsm",
                    error = %e,
                    "rsm link: raft/{HOLDS_FILE} does not parse: this node starts with no hold on its log",
                ),
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => tracing::warn!(
                target: "rsm",
                error = %e,
                "rsm link: raft/{HOLDS_FILE} was not read: this node starts with no hold on its log",
            ),
        }
        Holds {
            readers: Mutex::new(readers),
            dir: Some(dir.to_path_buf()),
            disk_pct,
            ..Holds::in_memory(ttl)
        }
    }

    /// `reader` has applied everything up to RSM index `after`. A position
    /// only moves a hold forward within one life of the reader; a reader that
    /// comes back lower (it was seeded again) is taken at its word.
    ///
    /// `true`: the file is due ([`Self::save`], off the runtime).
    pub(crate) fn note(&self, reader: &str, after: u64) -> bool {
        let name = if reader.is_empty() { "standby" } else { reader };
        let now = Instant::now();
        let fresh = {
            let mut readers = self.readers.lock().expect("link holds");
            let fresh = readers.get(name).is_none_or(|h| after < h.after);
            readers.insert(
                name.to_string(),
                Hold {
                    after,
                    at: now,
                    seen_ms: wall_ms(),
                },
            );
            fresh
        };
        if self.dir.is_none() {
            return false;
        }
        let mut saved = self.saved.lock().expect("link holds saved");
        let due = fresh || saved.is_none_or(|at| now.duration_since(at) >= SAVE_EVERY);
        if due {
            *saved = Some(now);
        }
        due
    }

    /// `reader` holds nothing here any more. `true`: the file is due.
    pub(crate) fn forget(&self, reader: &str) -> bool {
        let name = if reader.is_empty() { "standby" } else { reader };
        let had = self
            .readers
            .lock()
            .expect("link holds")
            .remove(name)
            .is_some();
        had && self.dir.is_some()
    }

    /// Write the holds to their file. Blocking (an fsync). A failed write
    /// costs what a restart then forgets, nothing while the node runs.
    pub(crate) fn save(&self) {
        let Some(dir) = &self.dir else {
            return;
        };
        let _one = self.writing.lock().expect("link holds file");
        let file = HoldsFile {
            readers: self
                .readers
                .lock()
                .expect("link holds")
                .iter()
                .map(|(name, h)| {
                    (
                        name.clone(),
                        HeldReader {
                            after: h.after,
                            seen_ms: h.seen_ms,
                        },
                    )
                })
                .collect(),
        };
        let bytes = serde_json::to_vec(&file).expect("the holds serialize");
        if let Err(e) = write_atomic(dir, HOLDS_FILE, &bytes) {
            tracing::warn!(
                target: "rsm",
                error = %e,
                "rsm link: raft/{HOLDS_FILE} was not written: a restart of this node forgets its standbys' holds",
            );
        }
    }

    /// The highest openraft index the log may be purged up to while every
    /// standby that holds it can still be served; `u64::MAX` when none does.
    /// A standby that applied RSM index `a` still needs the entry AT `a` to be
    /// recognisable (it is the purge point at most) and everything after:
    /// openraft index `a - 1` is the last that may go.
    ///
    /// Called by the purge driver, twice a second. It writes the file when a
    /// standby is given up on, which is once in that standby's life.
    pub(crate) fn floor(&self) -> u64 {
        let mut gone: Vec<(String, u64)> = Vec::new();
        let floor = {
            let mut readers = self.readers.lock().expect("link holds");
            readers.retain(|name, h| {
                let heard = h.at.elapsed() < self.ttl;
                if !heard {
                    gone.push((name.clone(), h.after));
                }
                heard
            });
            readers
                .values()
                .map(|h| h.after.saturating_sub(1))
                .min()
                .unwrap_or(u64::MAX)
        };
        if !gone.is_empty() {
            for (reader, after) in &gone {
                tracing::warn!(
                    target: "rsm",
                    reader = %reader,
                    after,
                    silent_for = ?self.ttl,
                    "rsm link: a standby stopped reading: this node no longer keeps its log for it",
                );
            }
            self.save();
        }
        if floor == u64::MAX || !self.disk_full() {
            return floor;
        }
        u64::MAX
    }

    /// Whether the node's volume is too full to keep anything for a standby.
    fn disk_full(&self) -> bool {
        let used = match &self.dir {
            Some(dir) if self.disk_pct < 100.0 => disk_used_pct(dir),
            _ => None,
        };
        let full = used.is_some_and(|u| u >= self.disk_pct);
        if self.released.swap(full, Ordering::AcqRel) != full {
            if full {
                tracing::error!(
                    target: "rsm",
                    used_pct = used.unwrap_or(0.0),
                    limit_pct = self.disk_pct,
                    "rsm link: this node's data volume is too full to keep its log for a standby \
                     (QUEEN_LINK_HOLD_DISK_PCT): it purges as if nobody read it, and a standby \
                     that falls behind the purge needs a new seed",
                );
            } else {
                tracing::info!(
                    target: "rsm",
                    "rsm link: this node keeps its log for its standbys again",
                );
            }
        }
        full
    }

    /// Whether the holds count at all: `false` while the node's volume is too
    /// full ([`Self::floor`] looked last).
    pub(crate) fn holding(&self) -> bool {
        !self.released.load(Ordering::Acquire)
    }

    /// The standbys that hold this node's log.
    pub(crate) fn readers(&self) -> Vec<Reader> {
        let readers = self.readers.lock().expect("link holds");
        let mut out: Vec<Reader> = readers
            .iter()
            .filter(|(_, h)| h.at.elapsed() < self.ttl)
            .map(|(name, h)| Reader {
                name: name.clone(),
                after: h.after,
                idle: h.at.elapsed(),
            })
            .collect();
        out.sort_by(|a, b| a.name.cmp(&b.name));
        out
    }
}

/// A node's log as a standby reads it. Cheap to clone; holds the node's log
/// and shared state, never the replicator.
#[derive(Clone)]
pub struct LinkSource {
    log: LogStore,
    shared: Arc<Shared>,
}

impl LinkSource {
    pub(crate) fn new(log: LogStore, shared: Arc<Shared>) -> LinkSource {
        LinkSource { log, shared }
    }

    /// The RSM index this node has applied.
    pub fn applied(&self) -> u64 {
        self.shared.applied_index.load(Ordering::Acquire)
    }

    /// The RSM index of the last entry purged from this node's log.
    pub fn purged(&self) -> u64 {
        self.log.purged_log_id().map_or(0, |p| rsm_index(p.index))
    }

    /// The standbys that hold this node's log.
    pub fn readers(&self) -> Vec<Reader> {
        self.shared.link_holds.readers()
    }

    /// Whether this node keeps its log for them: `false` while its data
    /// volume is too full (`QUEEN_LINK_HOLD_DISK_PCT`).
    pub fn holding(&self) -> bool {
        self.shared.link_holds.holding()
    }

    /// The term of this log's entry at RSM `index` (at or above the purge
    /// point), if the log still tells.
    fn term_at(&self, index: u64) -> io::Result<Option<u64>> {
        if let Some(p) = self.log.purged_log_id() {
            if rsm_index(p.index) == index {
                return Ok(Some(term_of(&p)));
            }
        }
        if let Some(t) = self.shared.term_at(index) {
            return Ok(Some(t));
        }
        let at = index - 1;
        Ok(self
            .log
            .read_range(at, at + 1)?
            .first()
            .filter(|e| e.log_id.index == at)
            .map(|e| term_of(&e.log_id)))
    }

    /// The application entries after RSM index `after`, up to `max_bytes` of
    /// them (one at least), once this log is known to hold the entry the
    /// standby applied last (`after`, `after_term`). Blocking: it reads the
    /// queue logs.
    pub fn read(&self, after: u64, after_term: u64, max_bytes: usize) -> io::Result<Answer> {
        // Read once: everything below is judged against this point, and an
        // entry applied meanwhile is the next call's.
        let applied = self.applied();
        let purged = self.purged();
        if after < purged {
            return Ok(Answer::Purged { purged, applied });
        }
        if after > applied {
            // This node has not applied that far (a follower behind, or a
            // source that lost entries the standby holds): nothing to send
            // and nothing to compare. The standby sees `applied` and decides.
            return Ok(Answer::Entries {
                entries: Vec::new(),
                upto: applied,
                applied,
                purged,
            });
        }
        if after > 0 {
            match self.term_at(after)? {
                Some(t) if t == after_term => {}
                Some(t) => {
                    return Ok(Answer::Mismatch {
                        index: after,
                        source_term: Some(t),
                        applied,
                        purged,
                    })
                }
                // Purged between the two reads above and this one.
                None if after < self.purged() => {
                    return Ok(Answer::Purged {
                        purged: self.purged(),
                        applied,
                    })
                }
                // An applied entry above the purge point is in the log; if
                // this node cannot read it now, the standby asks again.
                None => {
                    return Err(io::Error::other(format!(
                        "raft log entry {after} is applied here and could not be read"
                    )))
                }
            }
        }

        let mut entries: Vec<SourceEntry> = Vec::new();
        let mut bytes = 0usize;
        // openraft numbering from here: RSM `after + 1 ..= applied`.
        let (mut next, end) = (after, applied);
        'read: while next < end {
            let got = self.log.read_range(next, (next + WINDOW).min(end))?;
            let Some(first) = got.first() else {
                break;
            };
            if first.log_id.index != next {
                // The purge point passed what was asked while it was read.
                return Ok(Answer::Purged {
                    purged: self.purged(),
                    applied,
                });
            }
            for e in &got {
                next = e.log_id.index + 1;
                // A leader's blank entry and a membership change stay here:
                // they describe this cluster's raft group.
                let EntryPayload::Normal(app) = &e.payload else {
                    continue;
                };
                let stored = app.wire()?;
                bytes += stored.len();
                entries.push(SourceEntry {
                    index: rsm_index(e.log_id.index),
                    term: term_of(&e.log_id),
                    stored,
                });
                if bytes >= max_bytes {
                    break 'read;
                }
            }
        }
        Ok(Answer::Entries {
            entries,
            // `next` is the openraft index after the last entry looked at,
            // which is that entry's RSM index.
            upto: next,
            applied,
            purged,
        })
    }

    /// Leave `reader`'s hold at `after` — or none: a standby below this
    /// node's purge point finds nothing here whatever is kept from now on,
    /// and a hold for it would only stop the purge for as long as it asks.
    /// `true`: the holds' file is due ([`Holds::save`]).
    fn hold(&self, reader: &str, after: u64) -> bool {
        let holds = &self.shared.link_holds;
        if after < self.purged() {
            holds.forget(reader)
        } else {
            holds.note(reader, after)
        }
    }

    /// This node's snapshot as a stream, for the seed of the standby that
    /// will read as `reader` ([`crate::rsm::link::seed`]) — and that
    /// standby's first hold.
    ///
    /// A seeded standby reads this log for the first time long after its
    /// snapshot was made: its node stages the stream, starts on it, elects
    /// itself and logs its standby entry first. A hold left by that read
    /// ([`Self::serve`]) comes too late — the entries after the snapshot may
    /// be purged by then — so the seed leaves it:
    ///
    /// - before the store is copied, at this node's purge point. The copy
    ///   takes as long as the store is large, and the checkpoint it turns
    ///   out to hold is not below that point: the purge driver stays behind
    ///   the store's durable index, and the copy holds that index at least;
    /// - once the copy is made, at its checkpoint, which the standby's first
    ///   read names;
    /// - again as the standby reads the stream, piece by piece, so a
    ///   transfer longer than the hold's time does not outlive its hold.
    ///
    /// From the last piece on it is a hold like any other: the standby has
    /// `QUEEN_LINK_HOLD_S` to read, and its first read moves the hold. When
    /// the snapshot cannot be built the hold is dropped again. An empty
    /// `reader`: nobody reads after this snapshot, and nothing is held.
    pub(crate) async fn seed(
        &self,
        ctx: &Arc<SendCtx>,
        reader: &str,
    ) -> io::Result<axum::body::Body> {
        if reader.is_empty() {
            return stream_checkpoint(ctx, None).await;
        }
        // Noted whatever the purge point is by now: a hold at or below it
        // stops the purge where it stands, which is what the copy needs.
        let (me, name) = (self.clone(), reader.to_string());
        tokio::task::spawn_blocking(move || {
            let holds = &me.shared.link_holds;
            if holds.note(&name, me.purged()) {
                holds.save();
            }
        })
        .await
        .map_err(|e| io::Error::other(format!("link hold task: {e}")))?;
        let (me, name) = (self.clone(), reader.to_string());
        let held: Held = Arc::new(move |checkpoint| {
            #[cfg(test)]
            {
                let test = me.shared.link_holds.after_copy.lock().expect("seam").take();
                if let Some(test) = test {
                    test(checkpoint);
                }
            }
            if me.hold(&name, checkpoint) {
                me.shared.link_holds.save();
            }
        });
        let built = stream_checkpoint(ctx, Some(held)).await;
        if built.is_err() {
            let (me, name) = (self.clone(), reader.to_string());
            let _ = tokio::task::spawn_blocking(move || {
                let holds = &me.shared.link_holds;
                if holds.forget(&name) {
                    holds.save();
                }
            })
            .await;
        }
        built
    }

    /// Stand between the next seed's copy and the hold left at its
    /// checkpoint: `test` is called once there, with the checkpoint, and the
    /// seed goes on when it returns.
    #[cfg(test)]
    pub(crate) fn after_the_next_copy(&self, test: impl FnOnce(u64) + Send + 'static) {
        *self.shared.link_holds.after_copy.lock().expect("seam") = Some(Box::new(test));
    }

    /// Answer one call of a standby, encoded: leave its hold, wait for
    /// something to send if it asked to, then read and encode off the
    /// caller's runtime (megabytes of entries, and the Raft RPC server's
    /// threads carry votes and appends).
    pub async fn serve(&self, req: Request) -> io::Result<bytes::Bytes> {
        // The hold first: it must stand while the read waits.
        if self.hold(&req.reader, req.after) {
            let shared = self.shared.clone();
            tokio::task::spawn_blocking(move || shared.link_holds.save())
                .await
                .map_err(|e| io::Error::other(format!("link hold task: {e}")))?;
        }
        if req.hold_only {
            return Ok(bytes::Bytes::from(
                Answer::Entries {
                    entries: Vec::new(),
                    upto: req.after,
                    applied: self.applied(),
                    purged: self.purged(),
                }
                .encode(),
            ));
        }
        let wait = Duration::from_millis(req.wait_ms.min(WAIT_MS_MAX));
        if !wait.is_zero() && self.applied() <= req.after {
            let _ = self
                .shared
                .edge
                .applied
                .wait(req.after + 1, Instant::now() + wait)
                .await;
        }
        let max_bytes = match req.max_bytes as usize {
            0 => MAX_BYTES_DEFAULT,
            n => n.min(MAX_BYTES_CAP),
        };
        let me = self.clone();
        tokio::task::spawn_blocking(move || {
            let answer = me.read(req.after, req.after_term, max_bytes)?;
            // Purged while this call was on its way: the hold left above
            // keeps nothing.
            if matches!(answer, Answer::Purged { .. }) && me.hold(&req.reader, req.after) {
                me.shared.link_holds.save();
            }
            Ok(bytes::Bytes::from(answer.encode()))
        })
        .await
        .map_err(|e| io::Error::other(format!("link read task: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "queen-link-holds-{name}-{}-{}",
            std::process::id(),
            wall_ms()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    #[test]
    fn a_hold_follows_its_standby_and_ends_with_its_silence() {
        let holds = Holds::in_memory(Duration::from_millis(600));
        assert_eq!(holds.floor(), u64::MAX, "nobody reads");

        holds.note("a", 10);
        holds.note("b", 5);
        assert_eq!(holds.floor(), 4, "the standby furthest behind decides");
        holds.note("b", 40);
        assert_eq!(holds.floor(), 9);
        // Seeded again, a standby comes back lower and is taken at its word.
        holds.note("a", 3);
        assert_eq!(holds.floor(), 2);
        // A standby that has applied nothing needs the whole log.
        holds.note("c", 0);
        assert_eq!(holds.floor(), 0);
        holds.forget("c");
        assert_eq!(holds.floor(), 2);
        assert_eq!(
            holds
                .readers()
                .iter()
                .map(|r| (r.name.as_str(), r.after))
                .collect::<Vec<_>>(),
            vec![("a", 3), ("b", 40)]
        );

        // `b` keeps saying where it is; `a` stopped.
        for _ in 0..5 {
            std::thread::sleep(Duration::from_millis(150));
            holds.note("b", 41);
        }
        assert_eq!(holds.floor(), 40, "the silent standby was given up on");
        assert_eq!(holds.readers().len(), 1);
        std::thread::sleep(Duration::from_millis(700));
        assert_eq!(holds.floor(), u64::MAX);
    }

    #[test]
    fn a_node_knows_its_holds_after_a_restart() {
        let dir = scratch("restart");
        let ttl = Duration::from_secs(3600);
        let holds = Holds::open(&dir, ttl, 100.0);
        assert_eq!(holds.floor(), u64::MAX);
        assert!(holds.note("a", 10), "a new standby is written at once");
        holds.save();
        assert!(!holds.note("a", 11), "its next position can wait");
        assert!(holds.note("a", 7), "one that came back lower cannot");
        holds.save();
        assert!(holds.note("b", 99));
        holds.save();
        drop(holds);

        // What the file says is what the restarted node holds, a moment old.
        let holds = Holds::open(&dir, ttl, 100.0);
        assert_eq!(holds.floor(), 6);
        let readers = holds.readers();
        assert_eq!(
            readers
                .iter()
                .map(|r| (r.name.as_str(), r.after))
                .collect::<Vec<_>>(),
            vec![("a", 7), ("b", 99)]
        );
        assert!(readers.iter().all(|r| r.idle < Duration::from_secs(30)));
        // Given up on, a standby leaves the file too.
        assert!(holds.forget("a"));
        holds.save();
        drop(holds);
        assert_eq!(Holds::open(&dir, ttl, 100.0).floor(), 98);

        // A standby last heard long ago (this node was down for hours): it is
        // not held for another hour, and not dropped before it can speak.
        let old = HoldsFile {
            readers: BTreeMap::from([(
                "c".to_string(),
                HeldReader {
                    after: 50,
                    seen_ms: wall_ms() - 10 * 3_600_000,
                },
            )]),
        };
        write_atomic(&dir, HOLDS_FILE, &serde_json::to_vec(&old).unwrap()).unwrap();
        let holds = Holds::open(&dir, ttl, 100.0);
        assert_eq!(holds.floor(), 49, "held through the restart");
        let idle = holds.readers()[0].idle;
        assert!(
            idle >= ttl - RESTART_GRACE && idle < ttl,
            "a minute left, not an hour: idle for {idle:?}"
        );

        // A file that does not parse costs the holds, never the start.
        std::fs::write(dir.join(HOLDS_FILE), b"{").unwrap();
        assert_eq!(Holds::open(&dir, ttl, 100.0).floor(), u64::MAX);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[cfg(unix)]
    #[test]
    fn a_full_volume_ends_the_holds() {
        let dir = scratch("disk");
        let ttl = Duration::from_secs(3600);
        // No volume is less than 0% full: this node is always "too full".
        let full = Holds::open(&dir, ttl, 0.0);
        assert!(full.holding(), "nothing was asked yet");
        assert_eq!(full.floor(), u64::MAX, "nobody reads");
        full.note("a", 10);
        assert_eq!(full.floor(), u64::MAX, "the purge goes on as if nobody read");
        assert!(!full.holding());
        assert_eq!(full.readers().len(), 1, "the standby is still known");

        // 100: the volume's fullness never ends a hold.
        let never = Holds::open(&dir, ttl, 100.0);
        never.note("a", 10);
        assert_eq!(never.floor(), 9);
        assert!(never.holding());
        let _ = std::fs::remove_dir_all(dir);
    }
}
