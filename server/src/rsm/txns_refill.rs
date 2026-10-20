//! The boot's refill of the `txns` rows' hash lists from the queue logs.
//!
//! A checkpoint stores a `txns` row without its hash list
//! ([`crate::rsm::dedup::txns_without_hashes`]): every one of those hashes is
//! already in the message record its append wrote to the queue log, and the
//! queue log keeps that record as long as the row can exist. A record dies
//! only below its partition's `txns_start` ([`crate::rsm::qlog::QLog`]'s
//! reclaim), and rows start at `rows_start`, never below `txns_start`
//! ([`crate::rsm::store::rows::PartitionRow::rows_start`]). So the store loads
//! the rows without them, and this step reads them back after the queue logs
//! open and before the replay or any reader runs.
//!
//! The rows are read file by file, not row by row: the records every
//! partition's rows need are gathered from the indexes first
//! ([`QLogReader::refill_reads`], the same records a committed-frames read of
//! each partition would take), then each `.qlog` file that holds some is read
//! once, front to back in large reads, [`READERS`] files at a time. One small
//! `pread` per row was ~160 µs on a network disk: 4.4 million rows took
//! 12 minutes on stage (2026-10-20), where a pass over the same files is bound
//! by the disk's throughput.
//!
//! A row whose record is gone is one a committed watermark, or a partition
//! delete, removed after the checkpoint was taken: the file's reclaim judged
//! it from applied state the checkpoint did not hold yet. That entry is above
//! the durable index, so the replay removes the row again before this node
//! leads (I13) or serves a read. Such a row is left as it is and counted.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc;
use std::time::Instant;

use crate::rsm::qlog::set::QLogReader;
use crate::rsm::qlog::CommittedFrame;
use crate::rsm::store::{Store, TypedReads};

/// Files read at once. Each is read front to back in large reads, so a few in
/// flight keep a network disk at its throughput; more only add seeks.
const READERS: usize = 8;

/// What one refill did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Refilled {
    /// Rows the store loaded without their hash lists.
    pub rows: usize,
    /// Rows that took them back from the queue log.
    pub refilled: usize,
    /// Rows whose record the queue log no longer holds (removed again by the
    /// replay, see the module header).
    pub missing: usize,
    /// Rows of a partition the store no longer has (a delete still sweeping).
    pub orphaned: usize,
}

fn io_err(e: crate::rsm::store::StoreError) -> io::Error {
    io::Error::other(format!("txns refill: {e}"))
}

/// Put back the hash lists of every `txns` row `store` holds without them,
/// reading them from `reader`'s queue logs. A boot step: nothing else may
/// write the store meanwhile.
pub fn refill<S: Store + ?Sized>(store: &S, reader: &QLogReader) -> io::Result<Refilled> {
    let t0 = Instant::now();
    let groups = store.txns_without_hashes().map_err(io_err)?;
    let mut out = Refilled::default();
    if groups.is_empty() {
        return Ok(out);
    }
    // Each partition's rows, and the span of its queue log they cover.
    let mut spans = Vec::with_capacity(groups.len());
    let mut rows_of: HashMap<u64, Vec<(u64, u64)>> = HashMap::with_capacity(groups.len());
    for (pid, mut rows) in groups {
        out.rows += rows.len();
        let Some(part) = store.read(|r| r.partition(pid)).map_err(io_err)? else {
            out.orphaned += rows.len();
            continue;
        };
        rows.sort_unstable();
        let queue_id = QLogReader::queue_id_of(&part.tenant, &part.queue);
        let from = rows.first().map_or(0, |r| r.0);
        let to = rows.last().map_or(0, |r| r.1.saturating_add(1));
        spans.push((queue_id, pid, from, to));
        rows_of.insert(pid, rows);
    }
    let files = reader.refill_reads(&spans);
    let next = AtomicUsize::new(0);
    let stop = AtomicBool::new(false);
    let mut matched = 0usize;
    let mut bytes = 0u64;
    let mut failed: Option<io::Error> = None;
    std::thread::scope(|s| {
        let (tx, rx) = mpsc::sync_channel(READERS);
        for _ in 0..READERS.min(files.len()) {
            let tx = tx.clone();
            let (files, next, stop) = (&files, &next, &stop);
            s.spawn(move || {
                let mut buf = Vec::new();
                while !stop.load(Ordering::Relaxed) {
                    let Some(file) = files.get(next.fetch_add(1, Ordering::Relaxed)) else {
                        break;
                    };
                    if tx.send(file.read(&mut buf)).is_err() {
                        break;
                    }
                }
            });
        }
        drop(tx);
        // The store is written here, on this thread, as files come back; after
        // a failure the readers stop and what they already sent is dropped.
        for got in rx {
            if failed.is_some() {
                continue;
            }
            let written = got.and_then(|(frames, n)| {
                bytes += n;
                let (m, refilled) = refill_from(store, &rows_of, frames)?;
                matched += m;
                out.refilled += refilled;
                Ok(())
            });
            if let Err(e) = written {
                stop.store(true, Ordering::Relaxed);
                failed = Some(e);
            }
        }
    });
    if let Some(e) = failed {
        return Err(e);
    }
    out.missing = (out.rows - out.orphaned).saturating_sub(matched);
    tracing::info!(
        target: "rsm",
        rows = out.rows,
        refilled = out.refilled,
        missing = out.missing,
        orphaned = out.orphaned,
        files = files.len(),
        read_mb = bytes >> 20,
        ms = t0.elapsed().as_millis() as u64,
        "txns rows refilled from the queue logs",
    );
    if out.missing > 0 {
        tracing::warn!(
            target: "rsm",
            missing = out.missing,
            "txns rows whose record the queue log no longer holds: a watermark after the \
             checkpoint removed them, and the replay removes them again",
        );
    }
    Ok(out)
}

/// One file's records, `(pid, frame)`, put into the rows they belong to: a row
/// takes the hash list of the record with its base, its end and `16 * count`
/// bytes of hashes. Returns the rows matched and the rows the store refilled.
/// A record in a partition's span that no row asks for is skipped.
fn refill_from<S: Store + ?Sized>(
    store: &S,
    rows_of: &HashMap<u64, Vec<(u64, u64)>>,
    mut frames: Vec<(u64, CommittedFrame)>,
) -> io::Result<(usize, usize)> {
    frames.sort_unstable_by_key(|(pid, f)| (*pid, f.base_offset));
    let (mut matched, mut refilled) = (0usize, 0usize);
    for run in frames.chunk_by_mut(|a, b| a.0 == b.0) {
        let pid = run[0].0;
        let Some(rows) = rows_of.get(&pid) else {
            continue;
        };
        let mut fill: Vec<(u64, Vec<u8>)> = Vec::with_capacity(run.len());
        for (_, f) in run.iter_mut() {
            let Ok(i) = rows.binary_search_by_key(&f.base_offset, |r| r.0) else {
                continue;
            };
            let end = rows[i].1;
            let count = end.saturating_add(1).saturating_sub(f.base_offset);
            if f.end == end.saturating_add(1) && f.hashes.len() as u64 == 16 * count {
                fill.push((f.base_offset, std::mem::take(&mut f.hashes)));
            }
        }
        matched += fill.len();
        refilled += store.refill_txns(pid, &fill).map_err(io_err)?;
    }
    Ok((matched, refilled))
}
