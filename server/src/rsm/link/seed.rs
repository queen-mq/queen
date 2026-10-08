//! Seeding a standby: its first state, when its source already has a history.
//!
//! A standby replays its source's log from where its own state stands. An
//! EMPTY standby stands at the start, which works only while the source still
//! holds its whole log — a source that has run for a while purged the
//! beginning long ago. So a standby of such a source starts from the source's
//! SNAPSHOT instead: the stream a follower that is too far behind receives
//! (the store's checkpoint, every queue-log and segment file), asked of any
//! source node with the link's token and staged on ONE node of the standby as
//! the first state of a new cluster of which that node is the only voter
//! ([`crate::rsm::replicator::raft::snapshot::stage_seed`]). The other nodes
//! of the standby are added to it afterwards, as to any cluster, and receive
//! their state from it.
//!
//! # What makes the seeded cluster a standby
//!
//! The snapshot is the source's state, role row included: nothing in it says
//! standby. The seed leaves a record beside the data, [`SEED_FILE`], and the
//! first time the node opens the seeded store it notes what that state held
//! ([`Captured`]). From the two comes the driver's directive
//! ([`crate::rsm::batcher::LinkBoot`]): a node that leads the cluster in
//! exactly that state — nothing applied since, its role row untouched —
//! writes the standby entry first, at the snapshot's position in the source's
//! log. Once it has, the cluster's own role row decides, and the record is a
//! note of where the cluster came from.
//!
//! # A seed never replaces data
//!
//! It is taken only by a node whose directory holds none. A node told to
//! seed that holds data keeps it and says so: emptying a data directory is an
//! operator's act.

use std::io;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};

use super::driver::LinkConfig;
use super::wire::{SNAPSHOT_PATH, TOKEN_HEADER};
use super::{Position, FLAG_ROLE};
use crate::rsm::batcher::LinkBoot;
use crate::rsm::replicator::raft::log_store::write_atomic;
use crate::rsm::replicator::raft::snapshot::{stage_seed, Seeded};
use crate::rsm::replicator::raft::types::QueenNode;
use crate::rsm::replicator::NodeId;
use crate::rsm::store::{Store, TypedReads};

/// The seed's record, in the node's data directory.
pub const SEED_FILE: &str = "link.seed";

/// The longest the source may send nothing while its snapshot is read.
const STALL: Duration = Duration::from_secs(60);

const STAGING: &str = "staging";
const STAGED: &str = "staged";

/// [`SEED_FILE`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SeedRecord {
    /// The source node the snapshot was read from.
    pub source: String,
    /// When, in ms since the epoch.
    pub at_ms: u64,
    /// `staging` while the snapshot is read, `staged` once it is whole.
    pub state: String,
    /// Where the source's log stood in the snapshot: the standby's position.
    #[serde(default)]
    pub applied: u64,
    #[serde(default)]
    pub applied_term: u64,
    /// What the seeded store held when this node first opened it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub captured: Option<Captured>,
}

/// The two values the driver's directive compares the cluster's state with
/// ([`LinkBoot`]).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Captured {
    /// `meta.last_now_us` of the seeded state.
    pub last_now_us: i64,
    /// The seeded state's role row (JSON text), if the source had one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_row: Option<String>,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// The record in `dir`, if a seed was ever taken there.
pub fn read(dir: &Path) -> io::Result<Option<SeedRecord>> {
    match std::fs::read(dir.join(SEED_FILE)) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| io::Error::other(format!("{SEED_FILE} does not parse: {e}"))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

fn write(dir: &Path, rec: &SeedRecord) -> io::Result<()> {
    write_atomic(
        dir,
        SEED_FILE,
        &serde_json::to_vec_pretty(rec).map_err(io::Error::other)?,
    )
}

/// Whether `dir` holds a node's data: a store, queue logs, segments or a raft
/// state. A snapshot area left by a transfer that never finished is not data.
fn holds_data(dir: &Path) -> bool {
    ["store", "qlog", "seg", "raft"].iter().any(|name| {
        std::fs::read_dir(dir.join(name)).is_ok_and(|mut entries| entries.next().is_some())
    })
}

/// How a source node's snapshot is read: its address, to the stream. The
/// binary reads over HTTP ([`http_snapshot`]); a test may read in-process.
pub type SnapshotStream = std::pin::Pin<Box<dyn futures_util::Stream<Item = Result<Bytes, String>> + Send>>;

/// `POST http://<addr>/link/v1/snapshot` with the link's token.
pub async fn http_snapshot(addr: &str, token: Option<&str>) -> Result<SnapshotStream, String> {
    use axum::body::Body;
    use axum::http::{header, Method, StatusCode};
    use http_body_util::BodyExt;

    let mut connector = hyper_util::client::legacy::connect::HttpConnector::new();
    connector.set_nodelay(true);
    connector.set_connect_timeout(Some(Duration::from_secs(5)));
    let client = hyper_util::client::legacy::Client::builder(hyper_util::rt::TokioExecutor::new())
        .build::<_, Body>(connector);
    let url = format!("http://{addr}{SNAPSHOT_PATH}");
    let mut req = axum::http::Request::builder()
        .method(Method::POST)
        .uri(&url)
        .header(header::CONTENT_LENGTH, "0");
    if let Some(t) = token {
        req = req.header(TOKEN_HEADER, t);
    }
    let req = req
        .body(Body::empty())
        .map_err(|e| format!("{url}: {e}"))?;
    // The source builds the snapshot (a copy of its store) before the first
    // byte of the answer.
    let resp = match tokio::time::timeout(Duration::from_secs(600), client.request(req)).await {
        Err(_) => return Err(format!("{url}: no answer within 10 minutes")),
        Ok(Err(e)) => return Err(format!("{url}: {e}")),
        Ok(Ok(r)) => r,
    };
    match resp.status() {
        StatusCode::OK => {}
        StatusCode::UNAUTHORIZED => {
            return Err(format!(
                "{url}: the source refused this standby's QUEEN_LINK_SOURCE_TOKEN"
            ))
        }
        StatusCode::NOT_FOUND => {
            return Err(format!(
                "{url}: the source serves no link (QUEEN_LINK_TOKEN is not set on it, or it \
                 runs a release without one)"
            ))
        }
        other => {
            let text = resp
                .into_body()
                .collect()
                .await
                .map(|b| String::from_utf8_lossy(&b.to_bytes()).into_owned())
                .unwrap_or_default();
            return Err(format!("{url}: {other}: {text}"));
        }
    }
    let stream = resp.into_body().into_data_stream();
    // A source that stops sending must not hold this node's boot for ever.
    let paced = futures_util::stream::unfold(stream, move |mut s| async move {
        match tokio::time::timeout(STALL, s.next()).await {
            Ok(Some(Ok(b))) => Some((Ok(b), s)),
            Ok(Some(Err(e))) => Some((Err(e.to_string()), s)),
            Ok(None) => None,
            Err(_) => Some((
                Err(format!("the source sent nothing for {STALL:?}")),
                s,
            )),
        }
    });
    Ok(Box::pin(paced))
}

/// Why a node told to seed did not.
#[derive(Debug, PartialEq, Eq)]
pub enum Skipped {
    /// It was seeded before: the record is there.
    Seeded,
    /// Its directory holds data, and a seed never replaces data.
    HoldsData,
}

/// Take a seed for the node at `dir`, if it has none and holds no data: read
/// a source node's snapshot (each configured node in turn until one answers)
/// and stage it as the first state of a cluster whose only voter is
/// `node_id` at `node`'s addresses. The caller then boots as it always does:
/// the staged snapshot is swapped in before the store opens.
///
/// `Ok(Ok(seeded))`: taken now. `Ok(Err(why))`: not taken, and the node boots
/// with what it has. `Err`: the seed failed, and the node must not boot as a
/// cluster of its own with nothing in it.
pub async fn take(
    dir: &Path,
    node_id: NodeId,
    node: QueenNode,
    cfg: &LinkConfig,
) -> io::Result<Result<Seeded, Skipped>> {
    match read(dir)? {
        Some(rec) if rec.state == STAGED => return Ok(Err(Skipped::Seeded)),
        // Interrupted while the snapshot was read: nothing of it is in place
        // (the marker that swaps it in is written last), so read it again.
        Some(_) => {}
        None if holds_data(dir) => {
            tracing::error!(
                target: "rsm",
                dir = %dir.display(),
                "rsm link: QUEEN_LINK_SEED names this node, but its data directory holds data and \
                 a seed never replaces data: the node starts with what it holds. Empty the \
                 directory to seed it",
            );
            return Ok(Err(Skipped::HoldsData));
        }
        None => {}
    }
    std::fs::create_dir_all(dir)?;
    let mut last = String::from("no source node is configured");
    for addr in &cfg.sources {
        write(
            dir,
            &SeedRecord {
                source: addr.clone(),
                at_ms: now_ms(),
                state: STAGING.to_string(),
                applied: 0,
                applied_term: 0,
                captured: None,
            },
        )?;
        tracing::warn!(
            target: "rsm",
            source = %addr,
            "rsm link: seeding this node from its source's snapshot",
        );
        let stream = match http_snapshot(addr, cfg.token.as_deref()).await {
            Ok(s) => s,
            Err(e) => {
                tracing::warn!(target: "rsm", error = %e, "rsm link: the seed was not read");
                last = e;
                continue;
            }
        };
        match stage_seed(dir, stream, node_id, node.clone()).await {
            Ok(seeded) => {
                write(
                    dir,
                    &SeedRecord {
                        source: addr.clone(),
                        at_ms: now_ms(),
                        state: STAGED.to_string(),
                        applied: seeded.applied,
                        applied_term: seeded.applied_term,
                        captured: None,
                    },
                )?;
                return Ok(Ok(seeded));
            }
            Err(e) => {
                tracing::warn!(target: "rsm", source = %addr, error = %e, "rsm link: the seed was not staged");
                last = format!("{addr}: {e}");
            }
        }
    }
    Err(io::Error::other(format!(
        "this node could not be seeded from its source: {last}"
    )))
}

/// [`take`] from a boot path: on a thread and a runtime of its own (the
/// caller may be on a runtime worker, where blocking on a future panics).
pub fn take_blocking(
    dir: &Path,
    node_id: NodeId,
    node: QueenNode,
    cfg: &LinkConfig,
) -> io::Result<Result<Seeded, Skipped>> {
    std::thread::scope(|s| {
        s.spawn(|| {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()?
                .block_on(take(dir, node_id, node, cfg))
        })
        .join()
        .unwrap_or_else(|_| Err(io::Error::other("the seed thread panicked")))
    })
}

/// The driver's directive for a seeded node, once its store is open (the
/// staged snapshot has been swapped in) and before anything writes to it:
/// `None` for a node that was never seeded.
///
/// The first call notes what the seeded state holds and keeps it in the
/// record; every later one reads the record, so the directive names the
/// SEEDED state for good and stops matching the moment the cluster writes a
/// role row of its own.
pub fn boot<S: Store>(dir: &Path, store: &S) -> io::Result<Option<LinkBoot>> {
    let Some(mut rec) = read(dir)? else {
        return Ok(None);
    };
    if rec.state != STAGED {
        return Ok(None);
    }
    let captured = match rec.captured.clone() {
        Some(c) => c,
        None => {
            let (applied, last_now_us, role_row) = store
                .read(|r| Ok((r.applied_index()?, r.last_now_us()?, r.flag(FLAG_ROLE)?)))
                .map_err(|e| io::Error::other(format!("read the seeded store: {e}")))?;
            if applied != rec.applied {
                return Err(io::Error::other(format!(
                    "{SEED_FILE} names a seed at source entry {}, and this node's store is at \
                     {applied}: the seed was not installed. Empty the data directory and seed again",
                    rec.applied
                )));
            }
            let role_row = role_row
                .map(|b| {
                    String::from_utf8(b)
                        .map_err(|_| io::Error::other("the seeded role row is not text"))
                })
                .transpose()?;
            let c = Captured {
                last_now_us,
                role_row,
            };
            rec.captured = Some(c.clone());
            write(dir, &rec)?;
            c
        }
    };
    Ok(Some(LinkBoot {
        source: rec.source,
        position: Position {
            index: rec.applied,
            term: rec.applied_term,
            now_us: captured.last_now_us,
        },
        last_now_us: captured.last_now_us,
        role_row: captured.role_row.map(String::into_bytes),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "queen-rsm-linkseed-{tag}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("scratch dir");
        dir
    }

    #[test]
    fn a_directory_holds_data_once_a_node_wrote_there() {
        let dir = scratch("holds");
        assert!(!holds_data(&dir), "an empty directory");
        // What a transfer that never finished leaves is not data.
        std::fs::create_dir_all(dir.join("snapshots").join("recv-1")).unwrap();
        std::fs::create_dir_all(dir.join("store")).unwrap();
        assert!(!holds_data(&dir), "an empty store directory");
        std::fs::write(dir.join("store").join("data.mdb"), b"x").unwrap();
        assert!(holds_data(&dir));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn the_record_round_trips() {
        let dir = scratch("record");
        assert_eq!(read(&dir).unwrap(), None);
        let rec = SeedRecord {
            source: "a:7400".into(),
            at_ms: 5,
            state: STAGED.into(),
            applied: 41_233,
            applied_term: 7,
            captured: Some(Captured {
                last_now_us: 9,
                role_row: None,
            }),
        };
        write(&dir, &rec).unwrap();
        assert_eq!(read(&dir).unwrap(), Some(rec));
        let _ = std::fs::remove_dir_all(dir);
    }
}
