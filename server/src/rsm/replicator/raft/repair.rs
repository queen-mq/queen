//! Operator repairs that run at boot, before openraft reads the log: the two
//! ways out of a cluster that cannot heal itself.
//!
//! # `QUEEN_RAFT_FORCE_RECOVER=<this node's id>`: one survivor, the rest lost
//!
//! A majority of the voters lost their disks for good: no quorum will ever
//! form again, and Raft has no way out on its own. The survivor named here
//! appends, at the end of its log and in a NEW term, a membership entry whose
//! only voter is itself, and votes for itself in that term; openraft then
//! starts with that membership as the effective one ([`openraft`] takes the
//! last membership entry in the log), elects the node alone, and commits the
//! entry — and with it everything the survivor's log held — under its blank
//! entry. New nodes join it through the membership endpoints (learner, catch
//! up, promote).
//!
//! Why the entry is safe to write: an entry in term `T` must come from term
//! `T`'s leader, and `T` is one past the highest term this node ever saw (its
//! vote and its last entry), so no leader of `T` existed anywhere this node
//! heard of. The lost members are the only ones that could know a higher term,
//! and the operator asserts they never come back with their data. The entry is
//! appended after everything the log holds (nothing is truncated), through the
//! same writer and fsync as any entry, and the vote goes down first, so the
//! node never holds an entry from a term above its vote.
//!
//! What it loses: whatever the survivor did not have — entries the lost
//! members had committed without it (acknowledged to clients). What it
//! keeps and commits: every entry the survivor held, including ones that were
//! never committed (their clients saw no answer; a retry with the same request
//! id finds them). The lost members must never rejoin with their old data:
//! two of them could still form a majority of the OLD voter set among
//! themselves and elect a second leader. Wipe them, or give their
//! replacements new data directories.
//!
//! Idempotent: a node that is already its only voter does nothing, and one
//! that recovered earlier refuses to recover again once other members were
//! added (the setting was left on; recovering again would drop them).
//!
//! # `QUEEN_RAFT_APPLY_SKIP=[g<group>/]<index>[:<digest>],...`: a poisoned entry
//!
//! Apply refuses an entry deterministically (a logic bug: the same refusal on
//! every node), so every node stops at it, and a restart replays it and stops
//! again. Every node set with this rewrites, in its own log, each named entry
//! into a SKIP MARKER (`apply::skip_marker_of`) at the same log id: no command,
//! no effect, the entry's clock and the id bases after it. Apply then steps
//! over it ([`crate::rsm::apply`], `execute_skip_marker`): none of its effects,
//! no outcome for its commands (a retry plans them again), its partition ids
//! and KV versions burned so the entries planned after it keep their bases.
//!
//! Why rewrite the log instead of skipping in apply alone: the writer put the
//! entry's payload records and entry record into the queue logs before apply
//! saw it. Left there, they would come back: the partitions it appended to
//! would take their next messages at the SAME offsets, and a pop, a Kafka
//! fetch or a dedup probe reading one of those offsets could find the skipped
//! payload (or its hashes) instead. The rewrite truncates the queue logs from
//! the first named entry and writes the suffix back — the named entries as
//! markers, the rest byte for byte — so the skipped records are gone from
//! every file, index and recovery scan, and the tail checks never saw them
//! (only applied records are recorded, and these never applied).
//!
//! Crash safety: the suffix, already rewritten, goes to `raft/apply_skip.journal`
//! (temporary file, fsync, rename) before the truncation, and the next boot
//! finishes a journal it finds before anything else reads the log.
//!
//! Checks, each refusing to start: a digest that is not the digest of the entry
//! this node holds at that index; an entry this node APPLIED without skipping
//! (skipping it elsewhere diverges the nodes); an entry whose last failure here
//! was node-local (I/O, space: the other nodes applied it); an openraft entry
//! (blank, membership), which cannot fail apply. An index beyond this node's
//! log is left alone: the marker arrives from the leader.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{IOFlushed, RaftLogStorage};
use openraft::type_config::TypeConfigExt;
use openraft::{EntryPayload, RaftLogReader};
use serde::{Deserialize, Serialize};

use super::log_store::{write_atomic, LogStore};
use super::types::{
    log_id, rsm_index, term_of, AppEntry, LogId, Membership, NodeId, QueenNode, REntry,
    StoredMembership, TypeConfig, Vote,
};
use crate::rsm::apply::{is_skip_marker, skip_marker_of, ApplyFailure};
use crate::rsm::entry::entry_digest;

/// Written once a force recovery committed its membership entry to the log.
pub(crate) const FORCE_RECOVERED: &str = "force_recovered.json";
/// The rewritten suffix of an apply skip, until it is back in the log.
pub(crate) const SKIP_JOURNAL: &str = "apply_skip.journal";
/// Every entry this node rewrote into a skip marker (index, term, digest).
pub(crate) const SKIPPED_FILE: &str = "apply_skipped.json";
/// The last entry apply refused on this node ([`ApplyFailure`]).
pub(crate) const FAILURE_FILE: &str = "apply_failure.json";

const JOURNAL_MAGIC: &[u8; 8] = b"QSKIPJ1\n";

/// One entry named by `QUEEN_RAFT_APPLY_SKIP`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkipSpec {
    /// The raft group it belongs to (`g<group>/`, default 0).
    pub group: usize,
    /// Its RSM index (what apply reports).
    pub index: u64,
    /// [`entry_digest`] as reported, when given.
    pub digest: Option<u64>,
}

/// Parse `QUEEN_RAFT_APPLY_SKIP`: comma-separated `[g<group>/]<index>[:<digest>]`
/// (the digest in hex, as the apply failure prints it).
pub fn parse_apply_skip(raw: &str) -> Result<Vec<SkipSpec>, String> {
    let mut out = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
        let (group, rest) = match part.split_once('/') {
            Some((g, rest)) => {
                let g = g
                    .strip_prefix('g')
                    .and_then(|n| n.parse::<usize>().ok())
                    .ok_or_else(|| {
                        format!("QUEEN_RAFT_APPLY_SKIP: `{part}`: the group is `g<number>/`")
                    })?;
                (g, rest)
            }
            None => (0, part),
        };
        let (index, digest) = match rest.split_once(':') {
            Some((i, d)) => (i, Some(d)),
            None => (rest, None),
        };
        let index: u64 = index
            .trim()
            .parse()
            .ok()
            .filter(|i| *i > 0)
            .ok_or_else(|| {
                format!("QUEEN_RAFT_APPLY_SKIP: `{part}`: `{index}` is not an entry index")
            })?;
        let digest = match digest {
            Some(d) => Some(u64::from_str_radix(d.trim(), 16).map_err(|_| {
                format!("QUEEN_RAFT_APPLY_SKIP: `{part}`: `{d}` is not a hex digest")
            })?),
            None => None,
        };
        out.push(SkipSpec {
            group,
            index,
            digest,
        });
    }
    Ok(out)
}

/// The raft group a data directory belongs to: `<dir>/groups/g<N>` is group
/// `N` ([`crate::rsm::facade::real::RaftFacade::open_group`]), anything else 0.
pub(crate) fn group_of_dir(data_dir: &Path) -> usize {
    let is_groups = data_dir
        .parent()
        .and_then(|p| p.file_name())
        .is_some_and(|n| n == "groups");
    if !is_groups {
        return 0;
    }
    data_dir
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_prefix('g'))
        .and_then(|n| n.parse().ok())
        .unwrap_or(0)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(b) => serde_json::from_slice(&b)
            .map(Some)
            .map_err(|e| io::Error::other(format!("{} does not parse: {e}", path.display()))),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Append `entries` through the log's writer and wait until they are fsynced.
async fn append_durably(log: &mut LogStore, entries: Vec<REntry>) -> io::Result<()> {
    let (tx, rx) = TypeConfig::oneshot::<Result<(), io::Error>>();
    log.append(entries, IOFlushed::signal(tx)).await?;
    rx.await.map_err(|_| {
        io::Error::other("the raft log writer stopped before the append was flushed")
    })?
}

/// Every entry of the log in `[start, end)` (openraft numbering), payloads
/// restored.
async fn read(log: &mut LogStore, start: u64, end: u64) -> io::Result<Vec<REntry>> {
    RaftLogReader::try_get_log_entries(log, start..end).await
}

/// The membership openraft will start with: the last membership entry above
/// the store's applied entry, else the one the state machine saved.
async fn effective_membership(
    log: &mut LogStore,
    state_dir: &Path,
    applied: Option<LogId>,
) -> io::Result<StoredMembership> {
    if let Some(last) = log.last_log_id() {
        let start = applied.map_or(0, |a| a.index + 1);
        let mut end = last.index + 1;
        while end > start {
            let from = end.saturating_sub(256).max(start);
            for e in read(log, from, end).await?.iter().rev() {
                if let EntryPayload::Membership(m) = &e.payload {
                    return Ok(StoredMembership::new(Some(e.log_id), m.clone()));
                }
            }
            end = from;
        }
    }
    Ok(read_json::<StoredMembership>(&state_dir.join("membership.json"))?.unwrap_or_default())
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Recovered {
    node_id: NodeId,
    term: u64,
    index: u64,
    previous_voters: Vec<NodeId>,
    previous_nodes: Vec<NodeId>,
    at_ms: u64,
}

/// `QUEEN_RAFT_FORCE_RECOVER` on node `node_id` (see the module header).
/// `self_node` is this node's addresses from `QUEEN_RAFT_PEERS`, when set.
pub(crate) async fn force_recover(
    log: &mut LogStore,
    state_dir: &Path,
    node_id: NodeId,
    self_node: Option<QueenNode>,
    applied: Option<LogId>,
) -> io::Result<()> {
    let eff = effective_membership(log, state_dir, applied).await?;
    let mem = eff.membership();
    let voters: BTreeSet<NodeId> = mem.voter_ids().collect();
    let nodes: BTreeSet<NodeId> = mem.nodes().map(|(id, _)| *id).collect();
    let marker = state_dir.join(FORCE_RECOVERED);
    let only_me = voters == BTreeSet::from([node_id]) && nodes == BTreeSet::from([node_id]);
    if only_me {
        tracing::warn!(
            target: "rsm",
            node = node_id,
            "QUEEN_RAFT_FORCE_RECOVER: this node is already its cluster's only voter: nothing \
             to recover (unset the variable)",
        );
        return Ok(());
    }
    if let Some(done) = read_json::<Recovered>(&marker)? {
        return Err(io::Error::other(format!(
            "QUEEN_RAFT_FORCE_RECOVER={node_id} is still set, but this node already recovered \
             (entry {}, term {}) and the membership has grown since (voters {voters:?}, members \
             {nodes:?}): recovering again would drop them. Unset QUEEN_RAFT_FORCE_RECOVER",
            done.index, done.term
        )));
    }
    let Some(last) = log.last_log_id() else {
        return Err(io::Error::other(
            "QUEEN_RAFT_FORCE_RECOVER: this node holds no raft log: there is nothing to recover \
             from (start the node whose data survived)",
        ));
    };
    let vote: Option<Vote> = RaftLogReader::read_vote(log).await?;
    let term = vote
        .map(|v| v.leader_id.term)
        .unwrap_or(0)
        .max(term_of(&last))
        + 1;
    let node = self_node
        .or_else(|| mem.get_node(&node_id).cloned())
        .unwrap_or_default();
    let m = Membership::new(
        vec![BTreeSet::from([node_id])],
        BTreeMap::from([(node_id, node.clone())]),
    )
    .map_err(|e| io::Error::other(format!("the recovered membership: {e}")))?;
    let at = log_id(term, last.index + 1);
    tracing::error!(
        target: "rsm",
        node = node_id,
        term,
        index = rsm_index(at.index),
        previous_voters = ?voters,
        previous_members = ?nodes,
        raft = %node.raft,
        http = %node.http,
        "QUEEN_RAFT_FORCE_RECOVER: UNSAFE RECOVERY. This node makes itself the ONLY voter, in a \
         new term, and will serve with exactly the log it holds: whatever the lost members had \
         committed without it is gone. The lost members must NEVER come back with their old \
         data. Add new nodes with POST /api/v1/system/raft/membership/learners, then promote them, and \
         unset QUEEN_RAFT_FORCE_RECOVER",
    );
    // The vote first: the node never holds an entry of a term above its vote.
    RaftLogStorage::save_vote(log, &Vote::new(term, node_id)).await?;
    append_durably(
        log,
        vec![REntry {
            log_id: at,
            payload: EntryPayload::Membership(m),
        }],
    )
    .await?;
    let rec = Recovered {
        node_id,
        term,
        index: rsm_index(at.index),
        previous_voters: voters.into_iter().collect(),
        previous_nodes: nodes.into_iter().collect(),
        at_ms: now_ms(),
    };
    write_atomic(
        state_dir,
        FORCE_RECOVERED,
        &serde_json::to_vec(&rec).map_err(io::Error::other)?,
    )?;
    Ok(())
}

/// One entry this node rewrote into a skip marker.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct SkippedEntry {
    pub index: u64,
    pub term: u64,
    pub digest: String,
    /// What the entry held: its commands and effects.
    pub held: String,
    pub at_ms: u64,
}

/// The entries this node rewrote into skip markers (`raft/apply_skipped.json`).
pub(crate) fn skipped_here(state_dir: &Path) -> io::Result<Vec<SkippedEntry>> {
    Ok(read_json::<Vec<SkippedEntry>>(&state_dir.join(SKIPPED_FILE))?.unwrap_or_default())
}

/// Record the failure apply reported (`raft/apply_failure.json`).
pub(crate) fn record_failure(state_dir: &Path, f: &ApplyFailure) -> io::Result<()> {
    write_atomic(
        state_dir,
        FAILURE_FILE,
        &serde_json::to_vec(f).map_err(io::Error::other)?,
    )
}

#[derive(Serialize, Deserialize)]
struct JournalHead {
    /// The entry the rewritten suffix follows.
    prev: Option<LogId>,
    /// The chunks' byte lengths, in order.
    chunks: Vec<u64>,
}

fn encode_journal(prev: Option<LogId>, entries: &[REntry]) -> io::Result<Vec<u8>> {
    let mut chunks: Vec<Vec<u8>> = Vec::new();
    let mut at = 0usize;
    while at < entries.len() {
        let req = openraft::raft::AppendEntriesRequest::<TypeConfig> {
            vote: Vote::new(0, 0),
            prev_log_id: None,
            entries: entries[at..].to_vec(),
            leader_commit: None,
        };
        let (bytes, n) = super::wire::encode_append(&req)?;
        if n == 0 {
            return Err(io::Error::other(
                "the skip journal: an entry does not encode",
            ));
        }
        chunks.push(bytes);
        at += n;
    }
    let head = serde_json::to_vec(&JournalHead {
        prev,
        chunks: chunks.iter().map(|c| c.len() as u64).collect(),
    })
    .map_err(io::Error::other)?;
    let mut out = Vec::with_capacity(16 + head.len() + chunks.iter().map(Vec::len).sum::<usize>());
    out.extend_from_slice(JOURNAL_MAGIC);
    out.extend_from_slice(&(head.len() as u32).to_le_bytes());
    out.extend_from_slice(&head);
    for c in chunks {
        out.extend_from_slice(&c);
    }
    Ok(out)
}

fn decode_journal(b: &[u8]) -> io::Result<(Option<LogId>, Vec<REntry>)> {
    let bad = |m: &str| io::Error::new(io::ErrorKind::InvalidData, format!("{SKIP_JOURNAL}: {m}"));
    if b.len() < 12 || &b[..8] != JOURNAL_MAGIC {
        return Err(bad("not a skip journal"));
    }
    let hlen = u32::from_le_bytes(b[8..12].try_into().expect("4 bytes")) as usize;
    let head_end = 12usize
        .checked_add(hlen)
        .filter(|e| *e <= b.len())
        .ok_or_else(|| bad("truncated header"))?;
    let head: JournalHead =
        serde_json::from_slice(&b[12..head_end]).map_err(|e| bad(&e.to_string()))?;
    let mut at = head_end;
    let mut entries = Vec::new();
    for len in head.chunks {
        let end = at
            .checked_add(len as usize)
            .filter(|e| *e <= b.len())
            .ok_or_else(|| bad("truncated chunk"))?;
        let req = super::wire::decode_append(&bytes::Bytes::copy_from_slice(&b[at..end]))?;
        entries.extend(req.entries);
        at = end;
    }
    if at != b.len() {
        return Err(bad("trailing bytes"));
    }
    Ok((head.prev, entries))
}

/// Put the journaled suffix back: cut the log after `prev`, append the
/// suffix, drop the journal. Repeatable: the cut takes whatever a crash left.
async fn replay_journal(log: &mut LogStore, state_dir: &Path, bytes: &[u8]) -> io::Result<()> {
    let (prev, entries) = decode_journal(bytes)?;
    RaftLogStorage::truncate_after(log, prev).await?;
    if !entries.is_empty() {
        append_durably(log, entries).await?;
    }
    fs::remove_file(state_dir.join(SKIP_JOURNAL))?;
    fs::File::open(state_dir)?.sync_all()?;
    Ok(())
}

/// Finish a skip rewrite a crash interrupted. First thing at boot, before
/// anything reads the log.
pub(crate) async fn finish_journal(log: &mut LogStore, state_dir: &Path) -> io::Result<()> {
    let path = state_dir.join(SKIP_JOURNAL);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    tracing::warn!(target: "rsm", "raft: finishing an apply-skip rewrite a restart interrupted");
    replay_journal(log, state_dir, &bytes).await
}

/// What an entry held, for the operator's record.
fn summary(e: &crate::rsm::entry::Entry) -> String {
    let kinds: Vec<&str> = e.effects.iter().map(|x| x.kind().name()).collect();
    format!(
        "{} command(s), {} effect(s): {}",
        e.commands.len(),
        e.effects.len(),
        kinds.join(" ")
    )
}

/// `QUEEN_RAFT_APPLY_SKIP` on this node (see the module header). `specs` are
/// this group's; `applied` is the store's applied RSM index; `skipped_in_store`
/// the skip markers the store applied (`meta apply_skipped/*`).
pub(crate) async fn apply_skip(
    log: &mut LogStore,
    state_dir: &Path,
    specs: &[SkipSpec],
    applied: u64,
    skipped_in_store: &BTreeSet<u64>,
) -> io::Result<()> {
    if specs.is_empty() {
        return Ok(());
    }
    let failure: Option<ApplyFailure> = read_json(&state_dir.join(FAILURE_FILE))?;
    let last = log.last_log_id();
    // RSM index -> (marker, digest, what it held, term)
    let mut rewrite: BTreeMap<u64, (crate::rsm::entry::Entry, u64, String, u64)> = BTreeMap::new();
    for s in specs {
        if s.index <= applied {
            if skipped_in_store.contains(&s.index) {
                tracing::info!(target: "rsm", index = s.index, "QUEEN_RAFT_APPLY_SKIP: entry already skipped on this node");
                continue;
            }
            return Err(io::Error::other(format!(
                "QUEEN_RAFT_APPLY_SKIP names entry {}, which this node APPLIED (its applied index \
                 is {applied}) and did not skip: skipping it on the other nodes would make them \
                 diverge from this one. Remove it from QUEEN_RAFT_APPLY_SKIP on every node and \
                 find out why this node applied what the others refused",
                s.index
            )));
        }
        let raft_index = s.index - 1;
        if last.is_none_or(|l| raft_index > l.index) {
            tracing::warn!(
                target: "rsm",
                index = s.index,
                last = last.map_or(0, |l| rsm_index(l.index)),
                "QUEEN_RAFT_APPLY_SKIP names an entry this node's log does not hold yet: nothing \
                 to rewrite here (the leader's rewritten entry arrives by replication)",
            );
            continue;
        }
        let found = read(log, raft_index, raft_index + 1).await?;
        let Some(e) = found.into_iter().next() else {
            return Err(io::Error::other(format!(
                "QUEEN_RAFT_APPLY_SKIP: entry {} is not readable from this node's log (purged?)",
                s.index
            )));
        };
        let EntryPayload::Normal(app) = &e.payload else {
            return Err(io::Error::other(format!(
                "QUEEN_RAFT_APPLY_SKIP names entry {}, which is openraft's own (a blank or a \
                 membership entry): it cannot fail apply. Check the index the failure reported",
                s.index
            )));
        };
        let pf = app.payload_free()?;
        if is_skip_marker(&pf) {
            tracing::info!(target: "rsm", index = s.index, "QUEEN_RAFT_APPLY_SKIP: entry already rewritten into a skip marker");
            continue;
        }
        let digest = entry_digest(&pf);
        if let Some(want) = s.digest {
            if want != digest {
                return Err(io::Error::other(format!(
                    "QUEEN_RAFT_APPLY_SKIP names entry {} with digest {want:016x}, but the entry \
                     this node holds there (term {}) has digest {digest:016x}: it is not the \
                     entry that failed. Refusing to skip it. Check the index and digest the \
                     failure reported, on every node",
                    s.index,
                    term_of(&e.log_id)
                )));
            }
        }
        if let Some(f) = failure.as_ref().filter(|f| f.index == s.index) {
            if !f.deterministic() {
                return Err(io::Error::other(format!(
                    "QUEEN_RAFT_APPLY_SKIP names entry {}, which failed on THIS node with a \
                     node-local error ({}): the other nodes apply it, so skipping it here would \
                     diverge this node. Repair this node instead, or wipe it and let it rejoin",
                    s.index, f.error
                )));
            }
        }
        rewrite.insert(
            s.index,
            (
                skip_marker_of(&pf),
                digest,
                summary(&pf),
                term_of(&e.log_id),
            ),
        );
    }
    let Some((&first, _)) = rewrite.iter().next() else {
        return Ok(());
    };
    let last = last.expect("an entry to rewrite is in the log");
    let cut = first - 1; // openraft index of the first rewritten entry
    let mut suffix = read(log, cut, last.index + 1).await?;
    if suffix.len() as u64 != last.index + 1 - cut {
        return Err(io::Error::other(format!(
            "QUEEN_RAFT_APPLY_SKIP: the log from entry {first} on is not complete ({} of {} \
             entries readable)",
            suffix.len(),
            last.index + 1 - cut
        )));
    }
    for e in suffix.iter_mut() {
        if let Some((marker, digest, held, term)) = rewrite.get(&rsm_index(e.log_id.index)) {
            tracing::error!(
                target: "rsm",
                index = rsm_index(e.log_id.index),
                term,
                digest = %format!("{digest:016x}"),
                held = %held,
                "QUEEN_RAFT_APPLY_SKIP: entry {} is REPLACED by a skip marker in this node's log: \
                 none of its effects will apply, its payloads leave the queue logs",
                rsm_index(e.log_id.index),
            );
            e.payload =
                EntryPayload::Normal(AppEntry::proposed(Arc::new(marker.clone()), Vec::new()));
        }
    }
    let prev = if cut == 0 {
        None
    } else {
        match read(log, cut - 1, cut).await?.into_iter().next() {
            Some(e) => Some(e.log_id),
            None => {
                let st = RaftLogStorage::get_log_state(log).await?;
                match st.last_purged_log_id {
                    Some(p) if p.index == cut - 1 => Some(p),
                    _ => {
                        return Err(io::Error::other(format!(
                            "QUEEN_RAFT_APPLY_SKIP: the entry before {first} is not readable"
                        )))
                    }
                }
            }
        }
    };
    let journal = encode_journal(prev, &suffix)?;
    write_atomic(state_dir, SKIP_JOURNAL, &journal)?;
    replay_journal(log, state_dir, &journal).await?;
    let mut done = skipped_here(state_dir)?;
    for (index, (_, digest, held, term)) in rewrite {
        done.push(SkippedEntry {
            index,
            term,
            digest: format!("{digest:016x}"),
            held,
            at_ms: now_ms(),
        });
    }
    write_atomic(
        state_dir,
        SKIPPED_FILE,
        &serde_json::to_vec(&done).map_err(io::Error::other)?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_skip_parses_groups_indexes_and_digests() {
        assert_eq!(parse_apply_skip("").unwrap(), vec![]);
        assert_eq!(
            parse_apply_skip("12, g2/40:00ff00ff00ff00ff ,7:A").unwrap(),
            vec![
                SkipSpec {
                    group: 0,
                    index: 12,
                    digest: None
                },
                SkipSpec {
                    group: 2,
                    index: 40,
                    digest: Some(0x00ff00ff00ff00ff)
                },
                SkipSpec {
                    group: 0,
                    index: 7,
                    digest: Some(0xa)
                },
            ]
        );
        for bad in ["0", "x", "1:zz", "h2/5", "g/5", "-3"] {
            assert!(parse_apply_skip(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_group_comes_from_the_data_directory() {
        assert_eq!(group_of_dir(Path::new("/data/queen")), 0);
        assert_eq!(group_of_dir(Path::new("/data/queen/groups/g3")), 3);
        assert_eq!(group_of_dir(Path::new("/data/queen/other/g3")), 0);
    }
}
