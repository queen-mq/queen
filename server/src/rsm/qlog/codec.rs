//! The queue log's payload codec: zstd on disk, node-local.
//!
//! The log writer compresses each message record's payload (one `Append`
//! blob: the frames of one push batch for one partition, after dedup) before
//! it writes the group, and marks the record with
//! [`super::record::FLAG_PAYLOAD_ZSTD`]. Every read goes through
//! [`super::QLog`]'s one decode point, which decompresses, so pop, fetch, DLQ
//! and sink readers only ever see the raw frames. The raft entry itself stays
//! raw: the codec, like a record's position, is this node's business (D8).
//!
//! A record is stored raw when it is under [`MIN_BYTES`] (a single small
//! message gains ~1.3x and pays a zstd frame header) or when compressing saves
//! less than a tenth of it (random bytes). Level: `QUEEN_RAFT_QLOG_ZSTD_LEVEL`,
//! default 1 (measured on the VM: 100 x 1 KB JSON 2.22x at 276 MB/s; level 3
//! buys 2.45x for 1.7x the CPU); 0 turns the codec off.
//!
//! A compressed payload whose frames hold a link or headers ends with a MARK
//! ([`mark_of`]): the framed pop hands stored records out as they are, must
//! leave such a record to the JSON render, and reads the mark instead of
//! decompressing every record to look at its frames' flags.

use crate::obs::panic_policy::LockExt;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{mpsc, Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

/// Payloads shorter than this are stored raw.
pub const MIN_BYTES: usize = 512;

/// A write group whose compressible bytes reach this is compressed on several
/// threads (the log writer is serial; ~0.45 ms of level-1 work at this size).
const PARALLEL_MIN_BYTES: usize = 128 * 1024;

/// Upper bound on a decompressed payload (a record body is capped far below).
const MAX_RAW_BYTES: u64 = 256 * 1024 * 1024;

/// The configured zstd level; 0 = the codec is off.
pub fn level() -> i32 {
    static LEVEL: OnceLock<i32> = OnceLock::new();
    *LEVEL.get_or_init(|| {
        std::env::var("QUEEN_RAFT_QLOG_ZSTD_LEVEL")
            .ok()
            .and_then(|v| v.trim().parse::<i32>().ok())
            .unwrap_or(1)
    })
}

fn threads() -> usize {
    static N: OnceLock<usize> = OnceLock::new();
    *N.get_or_init(|| {
        std::env::var("QUEEN_RAFT_QLOG_ZSTD_THREADS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .unwrap_or(4)
            .max(1)
    })
}

/// Compression contexts are reused across write groups (a context's first use
/// allocates its tables): the writer and the helper threads check one out.
static CCTX: Mutex<Vec<zstd::bulk::Compressor<'static>>> = Mutex::new(Vec::new());

fn compress_with(level: i32, raw: &[u8]) -> Option<Vec<u8>> {
    let mut c = match CCTX.lock().expect("zstd cctx pool").pop() {
        Some(c) => c,
        None => zstd::bulk::Compressor::new(level).ok()?,
    };
    let z = c.compress(raw).ok();
    CCTX.lock().expect("zstd cctx pool").push(c);
    z
}

/// The stored form of one payload: `Some(zstd bytes)` when worth it, `None` to
/// store it raw.
pub fn compress_one(raw: &[u8]) -> Option<Vec<u8>> {
    let level = level();
    if level == 0 || raw.len() < MIN_BYTES {
        return None;
    }
    let mut z = compress_with(level, raw)?;
    if z.len() * 10 > raw.len() * 9 {
        return None;
    }
    seal(crate::frames::flags_union(raw), &mut z);
    Some(z)
}

// ---------------------------------------------------------------------------
// The mark: what a compressed payload says about its frames
// ---------------------------------------------------------------------------

/// The frame flags a compressed payload answers for without being opened: a
/// link, and headers ([`crate::frames`]). Both exist only from catalogue
/// version 8, so no payload written before the mark holds either.
pub const MARKED_FLAGS: u8 = crate::frames::FLAG_LINK | crate::frames::FLAG_HEADERS;

/// The mark is a zstd SKIPPABLE frame after the data, which every zstd decoder
/// skips: [`decompress`], and anything else that reads the payload, sees the
/// same frames with or without it. `magic | u32 4 | 'Q' 'F' | version 1 |
/// flags`.
const MARK_MAGIC: [u8; 4] = 0x184D_2A5Fu32.to_le_bytes();
const MARK_LEN: usize = 12;

fn mark(flags: u8) -> [u8; MARK_LEN] {
    let mut m = [0u8; MARK_LEN];
    m[..4].copy_from_slice(&MARK_MAGIC);
    m[4..8].copy_from_slice(&4u32.to_le_bytes());
    m[8..].copy_from_slice(&[b'Q', b'F', 1, flags]);
    m
}

/// Which of the [`MARKED_FLAGS`] the frames of a compressed payload carry:
/// `None` when it has no mark, and then none of its frames is a link or
/// carries headers. Only its last twelve bytes are read.
///
/// That holds for every payload this codec stores: one with either flag is
/// marked where it is compressed ([`compress_one`]) or, for a form made
/// elsewhere, where the writer takes it ([`Pre::finish`]).
pub fn mark_of(z: &[u8]) -> Option<u8> {
    let m = &z[z.len().checked_sub(MARK_LEN)?..];
    (m[..4] == MARK_MAGIC && m[4..8] == 4u32.to_le_bytes() && m[8..11] == [b'Q', b'F', 1])
        .then_some(m[11])
}

/// Mark `z`, a compressed payload whose frames carry `flags` (or-ed), when
/// one of them is marked and `z` does not say so yet.
fn seal(flags: u8, z: &mut Vec<u8>) {
    let flags = flags & MARKED_FLAGS;
    if flags != 0 && mark_of(z).is_none() {
        z.extend_from_slice(&mark(flags));
    }
}

/// [`compress_one`] for every payload of a write group, in order. A group with
/// at least [`PARALLEL_MIN_BYTES`] of compressible payload is split across up
/// to `QUEEN_RAFT_QLOG_ZSTD_THREADS` (default 4) threads, the writer included.
pub fn compress_all(raws: &[&[u8]]) -> Vec<Option<Vec<u8>>> {
    let mut out: Vec<Option<Vec<u8>>> = vec![None; raws.len()];
    if level() == 0 {
        return out;
    }
    let work: usize = raws
        .iter()
        .filter(|r| r.len() >= MIN_BYTES)
        .map(|r| r.len())
        .sum();
    let n = threads().min(raws.len());
    if n <= 1 || work < PARALLEL_MIN_BYTES {
        for (o, r) in out.iter_mut().zip(raws) {
            *o = compress_one(r);
        }
        return out;
    }
    let per = raws.len().div_ceil(n);
    std::thread::scope(|s| {
        let mut chunks = out.chunks_mut(per).zip(raws.chunks(per));
        let first = chunks.next();
        for (o, r) in chunks {
            s.spawn(move || {
                // W1: compression for the core log writer.
                crate::obs::panic_policy::mark_current_thread_core();
                for (oi, ri) in o.iter_mut().zip(r) {
                    *oi = compress_one(ri);
                }
            });
        }
        if let Some((o, r)) = first {
            for (oi, ri) in o.iter_mut().zip(r) {
                *oi = compress_one(ri);
            }
        }
    });
    out
}

/// A payload's compression started AHEAD of the write — at propose, on the
/// codec pool — so it runs while the writer is still fsyncing the previous
/// group, and the writer only collects the result ([`Pre::finish`]). The raft
/// submitter awaits it instead ([`Pre::wait`]): a leader ships the stored form
/// to its followers, so they never compress.
pub enum Pre {
    /// Stored raw: under [`MIN_BYTES`], or the codec is off.
    Raw,
    /// The job, and the flags of the blob's frames when its answer may be a
    /// form made elsewhere (the client's own compression, one a follower
    /// sent: [`preseed`]), which carries no mark: [`Pre::finish`] marks it.
    Job(Arc<Slot>, u8),
}

/// Where a pool job leaves its answer: read by a blocking writer thread or by
/// an async task, whichever asks.
pub struct Slot {
    done: Mutex<Option<Option<Vec<u8>>>>,
    cv: std::sync::Condvar,
    notify: tokio::sync::Notify,
}

impl Slot {
    fn new() -> Arc<Slot> {
        Arc::new(Slot {
            done: Mutex::new(None),
            cv: std::sync::Condvar::new(),
            notify: tokio::sync::Notify::new(),
        })
    }

    fn put(&self, z: Option<Vec<u8>>) {
        *self.done.lock().expect("qlog zstd slot") = Some(z);
        self.cv.notify_all();
        // A permit is stored when nobody waits yet, so a later `wait` returns.
        self.notify.notify_one();
    }

    fn take_blocking(&self) -> Option<Vec<u8>> {
        let mut g = self.done.lock().expect("qlog zstd slot");
        loop {
            if let Some(z) = g.take() {
                return z;
            }
            g = self.cv.wait(g).expect("qlog zstd slot");
        }
    }

    fn ready(&self) -> bool {
        self.done.lock().expect("qlog zstd slot").is_some()
    }
}

impl Pre {
    /// Start compressing the blob of `entry.effects[eff]` (an `Append`) on the
    /// pool. The job shares the entry (no copy of the payload). A blob the
    /// facade already started compressing ([`precompress`]) takes that job
    /// instead, so its result is usually ready before the writer asks.
    pub fn start_append(entry: &Arc<crate::rsm::entry::Entry>, eff: usize) -> Pre {
        let blob = append_blob(entry, eff);
        if level() == 0 || blob.len() < MIN_BYTES {
            return Pre::Raw;
        }
        if let Some(slot) = take_early(blob) {
            EARLY_HITS.fetch_add(1, Ordering::Relaxed);
            // A few bytes a frame, of a blob `take_early` has just hashed.
            return Pre::Job(slot, crate::frames::flags_union(blob));
        }
        let slot = Slot::new();
        match pool().send(Job {
            input: JobInput::Append {
                entry: entry.clone(),
                eff,
            },
            slot: slot.clone(),
        }) {
            Ok(()) => Pre::Job(slot, 0),
            Err(_) => Pre::Raw,
        }
    }

    /// The stored form ([`compress_one`]'s answer), waiting for the job if it
    /// is still running. A lost job stores the payload raw. Blocking: writer
    /// threads only.
    pub fn finish(self) -> Option<Vec<u8>> {
        match self {
            Pre::Raw => None,
            Pre::Job(slot, flags) => {
                let mut z = slot.take_blocking()?;
                seal(flags, &mut z);
                Some(z)
            }
        }
    }

    /// Wait (async) until the job has answered; [`Pre::finish`] then returns at
    /// once.
    pub async fn wait(&self) {
        let Pre::Job(slot, _) = self else { return };
        loop {
            let notified = slot.notify.notified();
            if slot.ready() {
                return;
            }
            notified.await;
        }
    }
}

enum JobInput {
    /// An `Append` of a proposed entry (shared, no copy).
    Append {
        entry: Arc<crate::rsm::entry::Entry>,
        eff: usize,
    },
    /// A push group's frames, concatenated by the facade before planning.
    Raw(Vec<u8>),
}

struct Job {
    input: JobInput,
    slot: Arc<Slot>,
}

fn append_blob(entry: &crate::rsm::entry::Entry, eff: usize) -> &[u8] {
    match entry.effects.get(eff) {
        Some(crate::rsm::effect::Effect::Append { blob, .. }) => blob,
        _ => &[],
    }
}

/// The codec pool: `QUEEN_RAFT_QLOG_ZSTD_THREADS` (default 4) persistent
/// threads, started on first use.
fn pool() -> &'static mpsc::Sender<Job> {
    static POOL: OnceLock<mpsc::Sender<Job>> = OnceLock::new();
    POOL.get_or_init(|| {
        let (tx, rx) = mpsc::channel::<Job>();
        let rx = Arc::new(Mutex::new(rx));
        for i in 0..threads() {
            let rx = rx.clone();
            let spawned = std::thread::Builder::new()
                .name(format!("qlog-zstd-{i}"))
                .spawn(move || loop {
                    let job = match rx.lock().expect("qlog zstd pool").recv() {
                        Ok(j) => j,
                        Err(_) => return,
                    };
                    let z = match &job.input {
                        JobInput::Append { entry, eff } => compress_one(append_blob(entry, *eff)),
                        JobInput::Raw(raw) => compress_one(raw),
                    };
                    job.slot.put(z);
                });
            if spawned.is_err() {
                break;
            }
        }
        tx
    })
}

// ---------------------------------------------------------------------------
// Compression ahead of planning: the log writer never waits for zstd
// ---------------------------------------------------------------------------

/// Blobs whose compression the FACADE started when a push arrived, keyed by
/// the xxh3-128 of their raw bytes. A push group's `Append` blob is its
/// frames concatenated in order whenever every frame survives dedup (the
/// planner concatenates the survivors, O20), so the facade can start the job
/// while the command still waits for its planning cycle; the proposer takes it
/// for a byte-identical blob ([`Pre::start_append`]) and the writer finds the
/// stored form ready instead of blocking on the pool — measured at 83% of the
/// writer's waits at 300k msg/s. Content-addressed, so a stale or reused key
/// can only ever hand back the compression of the very same bytes. A blob with
/// no entry (a partial dedup, a follower's command, the codec off) is
/// compressed at propose as before; an entry nobody takes (a duplicate, a
/// refusal, a retry) leaves after [`EARLY_TTL`].
const EARLY_SHARDS: usize = 64;
/// How long an untaken early job is kept.
const EARLY_TTL: Duration = Duration::from_secs(10);
/// A shard is swept of expired entries when it holds this many.
const EARLY_SWEEP_AT: usize = 32;
/// Early jobs waiting at most (all shards): past it the facade starts none.
/// A steady state holds about push rate x queueing time (~100 at 3k pushes/s);
/// the cap only bites when takes stop — a burst of all-duplicate pushes, whose
/// blobs never become an `Append` — and bounds that memory at a few tens of MB.
const EARLY_MAX: usize = 4096;

struct Early {
    slot: Arc<Slot>,
    at: Instant,
}

static EARLY_LIVE: AtomicUsize = AtomicUsize::new(0);
static EARLY_HITS: AtomicUsize = AtomicUsize::new(0);

fn early() -> &'static [Mutex<HashMap<u128, Early>>] {
    static SHARDS: OnceLock<Vec<Mutex<HashMap<u128, Early>>>> = OnceLock::new();
    SHARDS.get_or_init(|| {
        (0..EARLY_SHARDS)
            .map(|_| Mutex::new(HashMap::new()))
            .collect()
    })
}

fn early_shard(key: u128) -> &'static Mutex<HashMap<u128, Early>> {
    &early()[(key as usize) % EARLY_SHARDS]
}

/// Start compressing `raw` — a push group's frames, concatenated — on the
/// codec pool now, for the `Append` it becomes (see [`EARLY_SHARDS`]). A no-op
/// when the codec is off or the blob would be stored raw anyway.
pub fn precompress(raw: Vec<u8>) {
    if level() == 0 || raw.len() < MIN_BYTES || EARLY_LIVE.load(Ordering::Relaxed) >= EARLY_MAX {
        return;
    }
    let key = xxhash_rust::xxh3::xxh3_128(&raw);
    if early_shard(key).lock_unpoisoned().contains_key(&key) {
        // Already there: started by an earlier call, or handed in by `preseed`.
        return;
    }
    let slot = Slot::new();
    if pool()
        .send(Job {
            input: JobInput::Raw(raw),
            slot: slot.clone(),
        })
        .is_err()
    {
        return;
    }
    let now = Instant::now();
    let mut g = early_shard(key).lock_unpoisoned();
    if g.len() >= EARLY_SWEEP_AT {
        let before = g.len();
        g.retain(|_, e| now.duration_since(e.at) < EARLY_TTL);
        EARLY_LIVE.fetch_sub(before - g.len(), Ordering::Relaxed);
    }
    if g.insert(key, Early { slot, at: now }).is_none() {
        EARLY_LIVE.fetch_add(1, Ordering::Relaxed);
    }
}

/// Hand in the stored form of `raw` made elsewhere — a push group's frames,
/// concatenated, as the CLIENT compressed them — under the key [`precompress`]
/// files its job: the `Append` whose blob is byte-identical takes these bytes
/// and nothing here compresses. The caller has checked that `z` is one zstd
/// frame that [`decompress`] turns into exactly `raw`.
pub fn preseed(raw: &[u8], z: Vec<u8>) {
    preseed_key(early_key(raw), raw.len(), z);
}

/// The key the early forms are filed under: `xxh3_128` of the raw blob.
pub fn early_key(raw: &[u8]) -> u128 {
    xxhash_rust::xxh3::xxh3_128(raw)
}

/// [`preseed`] for a blob this node never held raw: a follower sends the
/// stored forms of a push's groups beside the command it forwards
/// (`facade::remote`), each under its [`early_key`].
pub fn preseed_key(key: u128, raw_len: usize, z: Vec<u8>) {
    if level() == 0
        || raw_len < MIN_BYTES
        || z.len() >= raw_len
        || EARLY_LIVE.load(Ordering::Relaxed) >= EARLY_MAX
    {
        return;
    }
    let slot = Slot::new();
    slot.put(Some(z));
    let now = Instant::now();
    let mut g = early_shard(key).lock_unpoisoned();
    if g.len() >= EARLY_SWEEP_AT {
        let before = g.len();
        g.retain(|_, e| now.duration_since(e.at) < EARLY_TTL);
        EARLY_LIVE.fetch_sub(before - g.len(), Ordering::Relaxed);
    }
    if g.insert(key, Early { slot, at: now }).is_none() {
        EARLY_LIVE.fetch_add(1, Ordering::Relaxed);
    }
}

/// The early job for a blob byte-identical to one [`precompress`] was given.
fn take_early(blob: &[u8]) -> Option<Arc<Slot>> {
    if EARLY_LIVE.load(Ordering::Relaxed) == 0 {
        return None;
    }
    let key = xxhash_rust::xxh3::xxh3_128(blob);
    let e = early_shard(key).lock_unpoisoned().remove(&key)?;
    EARLY_LIVE.fetch_sub(1, Ordering::Relaxed);
    Some(e.slot)
}

/// `(early jobs waiting, early jobs taken)` since start, for the metrics.
pub fn early_stats() -> (usize, usize) {
    (
        EARLY_LIVE.load(Ordering::Relaxed),
        EARLY_HITS.load(Ordering::Relaxed),
    )
}

// ---------------------------------------------------------------------------
// The replication wire: one frame per entry, compressed once
// ---------------------------------------------------------------------------

/// The level the replication wire uses: the leader compresses each entry's
/// wire bytes once, for every follower (`replicator/raft/wire.rs`).
const WIRE_LEVEL: i32 = 1;

/// Compression contexts for the wire, at [`WIRE_LEVEL`] whatever the disk
/// level is (a context keeps the level it was made with).
static WIRE_CCTX: Mutex<Vec<zstd::bulk::Compressor<'static>>> = Mutex::new(Vec::new());

/// One entry's replication wire bytes as one zstd frame (level 1), or `None`
/// when that saves under a tenth. No size floor: an entry's wire holds the
/// payload-free entry and every payload the disk codec left raw (each under
/// [`MIN_BYTES`]), which compress well together even when each alone would
/// not.
pub fn compress_wire(raw: &[u8]) -> Option<Vec<u8>> {
    if raw.len() < 64 {
        return None;
    }
    let mut c = match WIRE_CCTX.lock().expect("wire zstd cctx pool").pop() {
        Some(c) => c,
        None => zstd::bulk::Compressor::new(WIRE_LEVEL).ok()?,
    };
    let z = c.compress(raw).ok();
    WIRE_CCTX.lock().expect("wire zstd cctx pool").push(c);
    z.filter(|z| z.len() * 10 <= raw.len() * 9)
}

/// One `Append`'s payload as a queue log stores it: zstd (the record carries
/// [`super::record::FLAG_PAYLOAD_ZSTD`]) or raw. A leader sends this form to
/// its followers, which write it as they receive it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredPayload {
    pub zstd: bool,
    pub bytes: bytes::Bytes,
}

thread_local! {
    static DCTX: RefCell<Option<zstd::bulk::Decompressor<'static>>> = const { RefCell::new(None) };
}

/// Decompress a stored payload. The frame carries its content size (the bulk
/// compressor writes it), which bounds the allocation.
pub fn decompress(z: &[u8]) -> io::Result<Vec<u8>> {
    let cap = match zstd::zstd_safe::get_frame_content_size(z) {
        Ok(Some(n)) if n <= MAX_RAW_BYTES => n as usize,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "qlog zstd payload: missing or implausible content size",
            ))
        }
    };
    DCTX.with(|d| {
        let mut d = d.borrow_mut();
        if d.is_none() {
            *d = Some(zstd::bulk::Decompressor::new()?);
        }
        d.as_mut().expect("dctx").decompress(z, cap)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn json_batch(n: usize) -> Vec<u8> {
        let mut v = Vec::new();
        for i in 0..n {
            v.extend_from_slice(
                format!(
                    "{{\"id\":\"{i:08}\",\"type\":\"message.created\",\"channel\":\"whatsapp\",\"text\":\"hello number {i}\"}}"
                )
                .as_bytes(),
            );
        }
        v
    }

    #[test]
    fn compressible_round_trips_small_and_random_stay_raw() {
        let raw = json_batch(50);
        let z = compress_one(&raw).expect("a JSON batch compresses");
        assert!(z.len() < raw.len() / 2);
        assert_eq!(decompress(&z).unwrap(), raw);

        assert!(
            compress_one(&raw[..MIN_BYTES - 1]).is_none(),
            "under MIN_BYTES stays raw"
        );

        let mut x: u64 = 0x9E37_79B9_7F4A_7C15;
        let random: Vec<u8> = (0..8192)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        assert!(compress_one(&random).is_none(), "incompressible stays raw");
    }

    fn entry_with_append(blob: Vec<u8>) -> Arc<crate::rsm::entry::Entry> {
        let mut e = crate::rsm::entry::Entry::new(1, 0, 0);
        e.effects.push(crate::rsm::effect::Effect::Append {
            pid: 1,
            bucket: 0,
            base_offset: 0,
            count: 1,
            created_at_us: 1,
            hashes: vec![0; 16],
            blob,
        });
        Arc::new(e)
    }

    #[test]
    fn an_early_job_serves_only_a_byte_identical_blob() {
        // A unique blob, so parallel tests cannot share its key.
        let mut raw = json_batch(120);
        raw.extend_from_slice(format!("{:?}", std::thread::current().id()).as_bytes());
        raw.extend_from_slice(&std::process::id().to_le_bytes());
        precompress(raw.clone());

        // A blob that differs in one byte never gets it.
        let mut other = raw.clone();
        other[10] ^= 1;
        let e_other = entry_with_append(other.clone());
        let z_other = Pre::start_append(&e_other, 0).finish().expect("compressed");
        assert_eq!(decompress(&z_other).unwrap(), other);

        // The identical blob takes the early job — once.
        let e = entry_with_append(raw.clone());
        let hits0 = early_stats().1;
        let pre = Pre::start_append(&e, 0);
        assert!(early_stats().1 > hits0, "the early job was taken");
        assert!(take_early(&raw).is_none(), "an early job is taken once");
        let z = pre.finish().expect("compressed");
        assert_eq!(decompress(&z).unwrap(), raw);
        assert_eq!(
            Some(z),
            compress_one(&raw),
            "the same stored form as at propose"
        );

        // Small blobs are never kept.
        let small = b"tiny".to_vec();
        precompress(small.clone());
        assert!(take_early(&small).is_none());
    }

    /// `n` packed frames of about 650 bytes each (`tag` makes the blob unique:
    /// parallel tests share the early table), with a header when asked.
    fn frames_blob(n: usize, tag: &str, headers: bool) -> Vec<u8> {
        let block = crate::frames::encode_headers([("kind", "order.paid")]);
        let mut raw = Vec::new();
        for i in 0..n {
            let txn = format!("{tag}-{i}");
            let payload = format!("{{\"n\":{i},\"pad\":\"{}\"}}", "x".repeat(600));
            raw.extend_from_slice(&crate::frames::pack_frames(&[crate::frames::FrameIn {
                message_id: [i as u8; 16],
                txn: &txn,
                trace_id: None,
                producer_sub: None,
                payload: payload.as_bytes(),
                encrypted: false,
                headers: headers.then_some(block.as_slice()),
                link: None,
            }]));
        }
        raw
    }

    /// `n` link frames.
    fn links_blob(n: usize) -> Vec<u8> {
        let mut raw = Vec::new();
        for i in 0..n {
            let txn = format!("link-{i}");
            raw.extend_from_slice(&crate::frames::pack_frames(&[crate::frames::FrameIn {
                message_id: [i as u8; 16],
                txn: &txn,
                trace_id: None,
                producer_sub: None,
                payload: b"",
                encrypted: false,
                headers: None,
                link: Some(crate::frames::LinkRef {
                    qid: 7,
                    pid: 3,
                    offset: i as u64,
                }),
            }]));
        }
        raw
    }

    #[test]
    fn a_compressed_payload_says_whether_its_frames_hold_headers_or_a_link() {
        use crate::frames::{FLAG_HEADERS, FLAG_LINK};
        let plain = frames_blob(4, "plain", false);
        let z = compress_one(&plain).expect("compresses");
        assert_eq!(mark_of(&z), None, "plain frames carry no mark");
        assert_eq!(decompress(&z).unwrap(), plain);

        let with_headers = frames_blob(4, "hdr", true);
        let z = compress_one(&with_headers).expect("compresses");
        assert_eq!(mark_of(&z), Some(FLAG_HEADERS));
        assert_eq!(
            decompress(&z).unwrap(),
            with_headers,
            "the decoder skips the mark"
        );

        let links = links_blob(60);
        let z = compress_one(&links).expect("compresses");
        assert_eq!(mark_of(&z), Some(FLAG_LINK));
        assert_eq!(decompress(&z).unwrap(), links);

        // One frame with headers among plain ones marks the whole payload.
        let mut mixed = frames_blob(3, "mixed-a", false);
        mixed.extend_from_slice(&frames_blob(1, "mixed-b", true));
        let z = compress_one(&mixed).expect("compresses");
        assert_eq!(mark_of(&z), Some(FLAG_HEADERS));
        assert_eq!(decompress(&z).unwrap(), mixed);
    }

    #[test]
    fn a_form_made_elsewhere_is_marked_when_the_writer_takes_it() {
        let tag = format!("{:?}-{}", std::thread::current().id(), std::process::id());
        // Frames with headers, compressed as a client does: no mark.
        let raw = frames_blob(4, &format!("elsewhere-hdr-{tag}"), true);
        let z = zstd::bulk::compress(&raw, 3).expect("zstd");
        assert_eq!(mark_of(&z), None);
        preseed(&raw, z.clone());
        let stored = Pre::start_append(&entry_with_append(raw.clone()), 0)
            .finish()
            .expect("the client's form is taken");
        assert!(stored.starts_with(&z), "the client's bytes are kept");
        assert_eq!(mark_of(&stored), Some(crate::frames::FLAG_HEADERS));
        assert_eq!(decompress(&stored).unwrap(), raw);

        // Plain frames a client compressed are stored byte for byte.
        let plain = frames_blob(4, &format!("elsewhere-plain-{tag}"), false);
        let z = zstd::bulk::compress(&plain, 3).expect("zstd");
        preseed(&plain, z.clone());
        let stored = Pre::start_append(&entry_with_append(plain), 0)
            .finish()
            .expect("the client's form is taken");
        assert_eq!(stored, z);
    }

    #[test]
    fn the_wire_frame_round_trips_and_is_kept_only_when_it_saves() {
        // Many small payloads, each under the disk floor: together they compress.
        let raw = json_batch(40);
        let z = compress_wire(&raw).expect("small JSON messages compress together");
        assert!(z.len() * 10 <= raw.len() * 9);
        assert_eq!(decompress(&z).unwrap(), raw);

        let mut x: u64 = 0x2545_F491_4F6C_DD1D;
        let random: Vec<u8> = (0..4096)
            .map(|_| {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                x as u8
            })
            .collect();
        assert!(compress_wire(&random).is_none(), "incompressible: sent raw");
        assert!(compress_wire(b"tiny").is_none());
    }

    #[test]
    fn compress_all_keeps_order_on_the_parallel_path() {
        let raws: Vec<Vec<u8>> = (0..12).map(|i| json_batch(200 + i * 7)).collect();
        let refs: Vec<&[u8]> = raws.iter().map(|r| r.as_slice()).collect();
        assert!(refs.iter().map(|r| r.len()).sum::<usize>() >= PARALLEL_MIN_BYTES);
        let out = compress_all(&refs);
        for (raw, z) in raws.iter().zip(&out) {
            assert_eq!(&decompress(z.as_ref().expect("compressed")).unwrap(), raw);
        }
    }
}
