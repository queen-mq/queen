//! The segment frame of PLAN_RAFT.md §11.2 — one [`super::Effect::Append`]'s
//! payload bytes on disk.
//!
//! ```text
//! len:u32 | xxh3:u64 | pid:u64 | base_offset:u64 | count:u32 | created_at:i64 | hashes | blob
//! ```
//!
//! Little-endian throughout, in the hand-rolled style of the pgless
//! `native/record.rs` (`git show 6e96e228:server/src/native/record.rs`): no
//! serde on disk, every field written in one place and read in one place.
//!
//! Two spans decide what the format can do:
//!
//! - `len` counts every byte AFTER itself, so a scanner that has read the
//!   first four bytes knows exactly where the next frame starts without
//!   trusting any other field. That is what makes a file self-describing and
//!   a `.qidx` rebuildable by scanning (§11.2).
//! - the checksum covers every byte AFTER itself, so it protects the fields
//!   the reader is about to believe — `pid`, `base_offset`, `count`,
//!   `created_at`, the hash list and the blob — not only the payload. pgless's
//!   `read_blob` verified nothing at all; here every read verifies (§11.2).
//!
//! `hashes` is `16 * count` bytes, the xxh3_128 of each frame's transaction id
//! in frame order, exactly as [`crate::rsm::effect::Effect::Append`] carries
//! them: the dedup index and ack-by-hash index into it by ordinal (D10, 005).
//! Its length is therefore derived from `count`, not stored a second time, and
//! the blob is whatever is left over. A `count` that does not fit inside `len`
//! is refused before anything is allocated.
//!
//! NOTHING HERE IS A POSITION. The frame does not know which file or offset it
//! landed at; that is node-local and never replicated (D8, I7). A frame
//! carries only what every node's copy of it must agree on, which is why two
//! nodes with different file boundaries produce byte-identical frames.

use xxhash_rust::xxh3::xxh3_64;

/// `len:u32 | xxh3:u64 | pid:u64 | base_offset:u64 | count:u32 | created_at:i64`.
/// The bytes before the hash list.
pub const HEADER_LEN: usize = 4 + 8 + 8 + 8 + 4 + 8;

/// Bytes of the header that `len` covers: everything after `len` itself.
const HEADER_AFTER_LEN: usize = HEADER_LEN - 4;

/// Bytes of the header the checksum does NOT cover: `len` and the checksum.
const UNCHECKED_PREFIX: usize = 4 + 8;

/// One message's hash, in bytes. The xxh3_128 of its transaction id.
pub const HASH_LEN: usize = 16;

/// The largest `len` a frame header may declare. A frame carries one
/// `Append`, whose planned size the planner already caps at
/// `QUEEN_RAFT_ENTRY_MAX_BYTES` (96 MiB, §5.1); this bound is not that limit,
/// it is the one that keeps a damaged or hostile header from being believed
/// far enough to drive an allocation. It matches
/// [`crate::rsm::effect::MAX_BODY_LEN`] for the same reason.
pub const MAX_FRAME_BODY_LEN: u32 = 256 * 1024 * 1024;

/// What a frame's bytes can be wrong about.
///
/// Every variant is a statement about BYTES, never about semantics: the caller
/// decides whether a given failure is a torn tail it may truncate (§11.5) or a
/// disagreement between the store and the files that it must repair (I11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// The buffer ends before the frame the header describes.
    Truncated { need: usize, have: usize },
    /// `len` is below the fixed header or above [`MAX_FRAME_BODY_LEN`].
    BadLength(u32),
    /// `count` claims more hash bytes than `len` leaves room for.
    Stride { count: u32, body: u32 },
    /// xxh3 of the frame body does not match the header.
    Checksum { want: u64, got: u64 },
    /// The caller asked to encode a hash list that is not `16 * count`. Raised
    /// on the WRITE side; [`crate::rsm::effect::Effect::check`] refuses the
    /// same thing one level up, before an entry is proposed.
    HashStride { count: u32, hashes: usize },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Truncated { need, have } => {
                write!(f, "frame needs {need} bytes, buffer holds {have}")
            }
            FrameError::BadLength(l) => write!(f, "frame length {l} is not believable"),
            FrameError::Stride { count, body } => {
                write!(f, "count {count} needs more than the {body} bytes of body")
            }
            FrameError::Checksum { want, got } => {
                write!(f, "frame checksum {want:#018x} != {got:#018x}")
            }
            FrameError::HashStride { count, hashes } => {
                write!(
                    f,
                    "count {count} wants {} hash bytes, got {hashes}",
                    *count as usize * HASH_LEN
                )
            }
        }
    }
}

impl std::error::Error for FrameError {}

impl From<FrameError> for std::io::Error {
    fn from(e: FrameError) -> std::io::Error {
        std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string())
    }
}

/// The fixed part of a frame, parsed but NOT yet verified.
///
/// Everything in here comes from bytes whose checksum has not been checked, so
/// a scanner may use `frame_len` to step to the next frame and must not
/// believe `pid`, `base_offset`, `count` or `created_at_us` until
/// [`verify`] has passed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Header {
    /// Bytes after the `len` field: the whole frame minus four.
    pub body_len: u32,
    pub checksum: u64,
    pub pid: u64,
    pub base_offset: u64,
    pub count: u32,
    pub created_at_us: i64,
}

impl Header {
    /// Total bytes of the frame, `len` field included.
    pub fn frame_len(&self) -> usize {
        4 + self.body_len as usize
    }

    /// Bytes of hash list: 16 per message, derived from `count`.
    pub fn hashes_len(&self) -> usize {
        self.count as usize * HASH_LEN
    }

    /// The exclusive end offset this frame gives its partition:
    /// `base_offset + count`.
    pub fn end_offset(&self) -> u64 {
        self.base_offset.saturating_add(self.count as u64)
    }
}

/// A frame decoded in place: the hash list and the blob borrow the buffer.
#[derive(Clone, Copy, Debug)]
pub struct FrameRef<'a> {
    pub header: Header,
    pub hashes: &'a [u8],
    pub blob: &'a [u8],
}

/// How many bytes the frame for this payload will occupy.
pub fn encoded_len(count: u32, blob_len: usize) -> usize {
    HEADER_LEN + count as usize * HASH_LEN + blob_len
}

/// Append one frame to `out` and return how many bytes it added.
///
/// The checksum is computed over the bytes this call just wrote, after they
/// are in place, so there is exactly one copy of the payload.
pub fn encode_into(
    out: &mut Vec<u8>,
    pid: u64,
    base_offset: u64,
    count: u32,
    created_at_us: i64,
    hashes: &[u8],
    blob: &[u8],
) -> Result<usize, FrameError> {
    if hashes.len() != count as usize * HASH_LEN {
        return Err(FrameError::HashStride {
            count,
            hashes: hashes.len(),
        });
    }
    let body_len = HEADER_AFTER_LEN + hashes.len() + blob.len();
    if body_len > MAX_FRAME_BODY_LEN as usize {
        return Err(FrameError::BadLength(MAX_FRAME_BODY_LEN));
    }
    let start = out.len();
    out.reserve(4 + body_len);
    out.extend_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // checksum, filled in below
    out.extend_from_slice(&pid.to_le_bytes());
    out.extend_from_slice(&base_offset.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&created_at_us.to_le_bytes());
    out.extend_from_slice(hashes);
    out.extend_from_slice(blob);
    let sum = xxh3_64(&out[start + UNCHECKED_PREFIX..]);
    out[start + 4..start + UNCHECKED_PREFIX].copy_from_slice(&sum.to_le_bytes());
    Ok(4 + body_len)
}

/// Parse the fixed header of a frame. The buffer may hold more or less than
/// the whole frame; only [`HEADER_LEN`] bytes are read.
///
/// The three checks here are the ones that must happen BEFORE any allocation
/// or seek: a length that is impossible, a length past the cap, and a `count`
/// whose hash list would not fit in the body it declares.
pub fn parse_header(buf: &[u8]) -> Result<Header, FrameError> {
    if buf.len() < HEADER_LEN {
        return Err(FrameError::Truncated {
            need: HEADER_LEN,
            have: buf.len(),
        });
    }
    let body_len = u32::from_le_bytes(buf[0..4].try_into().expect("4 bytes"));
    if (body_len as usize) < HEADER_AFTER_LEN || body_len > MAX_FRAME_BODY_LEN {
        return Err(FrameError::BadLength(body_len));
    }
    let count = u32::from_le_bytes(buf[28..32].try_into().expect("4 bytes"));
    // The hash list and the blob share what is left after the fixed fields.
    // `count as usize * HASH_LEN` cannot overflow: count is a u32 and usize is
    // at least 32 bits on every target this builds for, but the multiplication
    // is done in u64 so a 32-bit target cannot wrap either.
    let room = body_len as u64 - HEADER_AFTER_LEN as u64;
    if count as u64 * HASH_LEN as u64 > room {
        return Err(FrameError::Stride {
            count,
            body: body_len,
        });
    }
    Ok(Header {
        body_len,
        checksum: u64::from_le_bytes(buf[4..12].try_into().expect("8 bytes")),
        pid: u64::from_le_bytes(buf[12..20].try_into().expect("8 bytes")),
        base_offset: u64::from_le_bytes(buf[20..28].try_into().expect("8 bytes")),
        count,
        created_at_us: i64::from_le_bytes(buf[32..40].try_into().expect("8 bytes")),
    })
}

/// Verify a whole frame's checksum. `frame` must be exactly the frame.
pub fn verify(frame: &[u8], header: &Header) -> Result<(), FrameError> {
    if frame.len() != header.frame_len() {
        return Err(FrameError::Truncated {
            need: header.frame_len(),
            have: frame.len(),
        });
    }
    let got = xxh3_64(&frame[UNCHECKED_PREFIX..]);
    if got != header.checksum {
        return Err(FrameError::Checksum {
            want: header.checksum,
            got,
        });
    }
    Ok(())
}

/// Parse, verify and split one frame. `buf` must start at the frame; anything
/// past it is ignored.
///
/// This is the read path of §11.2: no caller of [`super::Segments::read`] ever
/// sees bytes whose checksum has not matched.
pub fn decode(buf: &[u8]) -> Result<FrameRef<'_>, FrameError> {
    let header = parse_header(buf)?;
    let total = header.frame_len();
    if buf.len() < total {
        return Err(FrameError::Truncated {
            need: total,
            have: buf.len(),
        });
    }
    let frame = &buf[..total];
    verify(frame, &header)?;
    let hstart = HEADER_LEN;
    let hend = hstart + header.hashes_len();
    Ok(FrameRef {
        header,
        hashes: &frame[hstart..hend],
        blob: &frame[hend..],
    })
}
