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

use std::cell::RefCell;
use std::io;
use std::sync::{mpsc, Arc, Mutex, OnceLock};

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
    let z = compress_with(level, raw)?;
    (z.len() * 10 <= raw.len() * 9).then_some(z)
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
/// group, and the writer only collects the result ([`Pre::finish`]).
pub enum Pre {
    /// Stored raw: under [`MIN_BYTES`], or the codec is off.
    Raw,
    Job(mpsc::Receiver<Option<Vec<u8>>>),
}

impl Pre {
    /// Start compressing a copy of `blob` on the pool.
    pub fn start(blob: &[u8]) -> Pre {
        if level() == 0 || blob.len() < MIN_BYTES {
            return Pre::Raw;
        }
        let (tx, rx) = mpsc::sync_channel(1);
        match pool().send(Job {
            raw: blob.to_vec(),
            tx,
        }) {
            Ok(()) => Pre::Job(rx),
            Err(_) => Pre::Raw,
        }
    }

    /// The stored form ([`compress_one`]'s answer), waiting for the job if it
    /// is still running. A lost job stores the payload raw.
    pub fn finish(self) -> Option<Vec<u8>> {
        match self {
            Pre::Raw => None,
            Pre::Job(rx) => rx.recv().ok().flatten(),
        }
    }
}

struct Job {
    raw: Vec<u8>,
    tx: mpsc::SyncSender<Option<Vec<u8>>>,
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
                    let _ = job.tx.send(compress_one(&job.raw));
                });
            if spawned.is_err() {
                break;
            }
        }
        tx
    })
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
