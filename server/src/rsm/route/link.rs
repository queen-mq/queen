//! Reading through a link (PLAN_ROUTING.md §3.4).
//!
//! A reading queue's partition holds LINK frames: each has the id and the
//! transaction id of the message it stands for, maybe headers of its own, and,
//! in place of a payload, the message's address in its source queue's log
//! (`crate::frames::LinkRef`). A reader turns a record of such frames back
//! into messages with [`resolve`] right after it reads the record, before it
//! unpacks it: every frame that resolves is replaced by the source message
//! (its payload, trace id, producer and encryption flag), under the link's id
//! and transaction id, with the source's headers overlaid by the link's own.
//!
//! A link whose message is gone stays a link: below the source partition's
//! retention watermark (`retained` answers it), or not readable at all. The
//! reader skips it, as it skips a message whose TTL ran out, and the frame
//! count of the record never changes, so frame `i` is still offset
//! `base + i`.
//!
//! The source record of consecutive links is read and unpacked once.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::frames::{pack_frames, unpack_frames_ref, FrameIn, LinkRef};
use crate::rsm::qlog::set::QLogReader;

/// Links read through, and links found gone, since the process started (the
/// `queen_link_reads_total` / `queen_link_expired_at_source_total` counters).
pub static LINK_READS: AtomicU64 = AtomicU64::new(0);
pub static LINK_EXPIRED: AtomicU64 = AtomicU64::new(0);

/// A record of the source, read once per [`resolve`] call.
struct Source {
    base: u64,
    payload: Vec<u8>,
}

/// `blob` with every link that still resolves replaced by its message.
/// `retained(pid)` is the source partition's retention watermark (`None`: the
/// partition is gone); a link below it is left as it is. A blob with no link
/// is returned untouched, without a copy.
pub fn resolve(
    ql: &QLogReader,
    blob: Vec<u8>,
    retained: &mut dyn FnMut(u64) -> Option<u64>,
) -> Vec<u8> {
    if !crate::frames::any_link(&blob) {
        return blob;
    }
    let Some(frames) = unpack_frames_ref(&blob) else {
        return blob;
    };
    // Each source record read for this blob, by (pid, base offset), and the
    // record that holds a (pid, offset) once it has been found.
    let mut records: Vec<Source> = Vec::new();
    let mut by_offset: HashMap<(u64, u64), usize> = HashMap::new();
    let mut out = Vec::with_capacity(blob.len() * 4);
    for f in &frames {
        let Some(link) = f.link else {
            crate::frames::repack_into(&mut out, f);
            continue;
        };
        match source_message(ql, link, retained, &mut records, &mut by_offset) {
            Some((rec_idx, frame_idx)) => {
                let rec = &records[rec_idx];
                // The record was unpacked when it was found; it unpacks again
                // here so its frames borrow from `records`, which outlives them.
                let src =
                    unpack_frames_ref(&rec.payload).and_then(|v| v.into_iter().nth(frame_idx));
                match src {
                    Some(src) if !src.is_link() => {
                        let headers = crate::frames::overlay_headers(src.headers, f.headers);
                        out.extend_from_slice(&pack_frames(&[FrameIn {
                            message_id: f.message_id,
                            txn: f.txn,
                            trace_id: src.trace_id,
                            producer_sub: src.producer_sub,
                            payload: src.payload,
                            encrypted: src.encrypted,
                            headers: headers.as_deref(),
                            link: None,
                        }]));
                        LINK_READS.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        LINK_EXPIRED.fetch_add(1, Ordering::Relaxed);
                        crate::frames::repack_into(&mut out, f);
                    }
                }
            }
            None => {
                LINK_EXPIRED.fetch_add(1, Ordering::Relaxed);
                crate::frames::repack_into(&mut out, f);
            }
        }
    }
    out
}

/// Where the message a link points at is: (index into `records`, frame index
/// in that record). `None` when it is below the source's watermark or cannot
/// be read.
fn source_message(
    ql: &QLogReader,
    link: LinkRef,
    retained: &mut dyn FnMut(u64) -> Option<u64>,
    records: &mut Vec<Source>,
    by_offset: &mut HashMap<(u64, u64), usize>,
) -> Option<(usize, usize)> {
    match retained(link.pid) {
        Some(start) if link.offset >= start => {}
        _ => return None,
    }
    // A record already read that holds the offset.
    if let Some(&i) = by_offset.get(&(link.pid, link.offset)) {
        let r = &records[i];
        return Some((i, (link.offset - r.base) as usize));
    }
    let rec = ql.read_owned(link.qid, link.pid, link.offset).ok()??;
    if link.offset < rec.base_offset || link.offset >= rec.base_offset + u64::from(rec.count) {
        return None;
    }
    let idx = records.len();
    for o in rec.base_offset..rec.base_offset + u64::from(rec.count) {
        by_offset.insert((link.pid, o), idx);
    }
    let frame_idx = (link.offset - rec.base_offset) as usize;
    records.push(Source {
        base: rec.base_offset,
        payload: rec.payload,
    });
    Some((idx, frame_idx))
}

/// A `retained` for [`resolve`] over a store: each partition's committed
/// `log_start`, read once per partition and remembered.
pub fn retained_from<S>(store: &S) -> impl FnMut(u64) -> Option<u64> + '_
where
    S: crate::rsm::store::Store,
{
    use crate::rsm::store::TypedReads;
    let mut seen: HashMap<u64, Option<u64>> = HashMap::new();
    move |pid| {
        *seen.entry(pid).or_insert_with(|| {
            store
                .read(|r| r.partition(pid))
                .ok()
                .flatten()
                .map(|p| p.log_start)
        })
    }
}
