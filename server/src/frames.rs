// Segment frame codec — wire-compatible with the C++ storage-v2 broker
// (server/include/queen/storage_v2.hpp). One segment = K length-prefixed frames,
// zstd-compressed together. Frame layout (little-endian):
//   u32 body_len
//   body: u8 flags | u8[16] message_id | [u8[16] trace_id]
//         | u16 txn_len | txn | [u16 psub_len | psub] | payload(JSON bytes)

const FLAG_TRACE: u8 = 1;
const FLAG_PSUB: u8 = 2;
const FLAG_ENCRYPTED: u8 = 4;

const HEX: &[u8; 16] = b"0123456789abcdef";

pub fn uuid_bytes_to_string(b: &[u8; 16]) -> String {
    let mut out = [0u8; 36];
    let mut p = 0;
    for i in 0..16 {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            out[p] = b'-';
            p += 1;
        }
        out[p] = HEX[(b[i] >> 4) as usize];
        out[p + 1] = HEX[(b[i] & 0x0f) as usize];
        p += 2;
    }
    unsafe { String::from_utf8_unchecked(out.to_vec()) }
}

pub fn uuid_string_to_bytes(s: &str) -> Option<[u8; 16]> {
    let mut out = [0u8; 16];
    let mut n = 0;
    let mut hi: i32 = -1;
    for c in s.bytes() {
        if c == b'-' {
            continue;
        }
        let v = match c {
            b'0'..=b'9' => (c - b'0') as i32,
            b'a'..=b'f' => (c - b'a' + 10) as i32,
            b'A'..=b'F' => (c - b'A' + 10) as i32,
            _ => return None,
        };
        if hi < 0 {
            hi = v;
        } else {
            if n >= 16 {
                return None;
            }
            out[n] = ((hi << 4) | v) as u8;
            n += 1;
            hi = -1;
        }
    }
    if n == 16 && hi < 0 {
        Some(out)
    } else {
        None
    }
}

pub struct FrameIn<'a> {
    pub message_id: [u8; 16],
    pub txn: &'a str,
    pub trace_id: Option<[u8; 16]>,
    pub producer_sub: Option<&'a str>,
    pub payload: &'a [u8],
    pub encrypted: bool,
}

pub fn pack_frames(frames: &[FrameIn]) -> Vec<u8> {
    let mut est = 0usize;
    for f in frames {
        est += f.payload.len() + f.txn.len() + 64;
    }
    let mut out = Vec::with_capacity(est);
    for f in frames {
        let mut flags = 0u8;
        if f.trace_id.is_some() {
            flags |= FLAG_TRACE;
        }
        if f.producer_sub.is_some() {
            flags |= FLAG_PSUB;
        }
        if f.encrypted {
            flags |= FLAG_ENCRYPTED;
        }
        // Body length is fully determined up front, so the frame is written
        // straight into `out` — no per-frame temp Vec (that was one extra
        // malloc + full memcpy per message on the flush hot path).
        let txn = f.txn.as_bytes();
        // The length is written as a u16 below while `body_len` counts the full
        // usize, so an over-long txn would produce a frame whose declared body
        // length and actual content disagree. The push handler rejects those at
        // the HTTP boundary (`handlers::data::MAX_TXN_BYTES`); this pins the
        // invariant here, where it is actually load-bearing.
        debug_assert!(txn.len() <= u16::MAX as usize, "txn exceeds the u16 frame limit");
        let mut body_len = 1 + 16 + 2 + txn.len() + f.payload.len();
        if f.trace_id.is_some() {
            body_len += 16;
        }
        if let Some(ps) = f.producer_sub {
            body_len += 2 + ps.len();
        }
        out.extend_from_slice(&(body_len as u32).to_le_bytes());
        out.push(flags);
        out.extend_from_slice(&f.message_id);
        if let Some(t) = &f.trace_id {
            out.extend_from_slice(t);
        }
        out.extend_from_slice(&(txn.len() as u16).to_le_bytes());
        out.extend_from_slice(txn);
        if let Some(ps) = f.producer_sub {
            let ps = ps.as_bytes();
            out.extend_from_slice(&(ps.len() as u16).to_le_bytes());
            out.extend_from_slice(ps);
        }
        out.extend_from_slice(f.payload);
    }
    out
}

// Append the 36-char hyphenated hex form of a UUID without allocating (the
// owned uuid_bytes_to_string costs one String allocation per frame — visible
// at ~1M frames/s on the pop render path).
pub fn uuid_hex_into(out: &mut String, b: &[u8; 16]) {
    let mut buf = [0u8; 36];
    let mut p = 0;
    for i in 0..16 {
        if i == 4 || i == 6 || i == 8 || i == 10 {
            buf[p] = b'-';
            p += 1;
        }
        buf[p] = HEX[(b[i] >> 4) as usize];
        buf[p + 1] = HEX[(b[i] & 0x0f) as usize];
        p += 2;
    }
    // All bytes are ASCII hex or '-'.
    out.push_str(unsafe { std::str::from_utf8_unchecked(&buf) });
}

pub struct FrameOut {
    pub message_id: String,
    pub txn: String,
    pub trace_id: Option<String>,
    pub producer_sub: Option<String>,
    pub payload: Vec<u8>,
    pub encrypted: bool,
}

// Borrowed frame view for the hot pop-render path: no per-frame String/Vec
// allocations — txn/psub/payload point into the decompressed segment buffer.
// Returns None on a malformed segment OR invalid UTF-8 in txn/psub (both were
// written from &str by pack_frames, so that never happens for data this engine
// stored); callers treat None exactly like unpack_frames' None.
pub struct FrameRef<'a> {
    pub message_id: [u8; 16],
    pub txn: &'a str,
    pub trace_id: Option<[u8; 16]>,
    pub producer_sub: Option<&'a str>,
    pub payload: &'a [u8],
    // Parsed for wire completeness; production readers branch on the queue's
    // encryption flag instead, so only the pack/unpack round-trip test reads
    // this. Allowed because the plain (non-test) binary build would otherwise
    // flag it, and CI compiles with -D warnings.
    #[allow(dead_code)]
    pub encrypted: bool,
}

pub fn unpack_frames_ref(raw: &[u8]) -> Option<Vec<FrameRef<'_>>> {
    let mut out = Vec::new();
    let mut o = 0usize;
    let rd_u16 = |raw: &[u8], p: usize| -> u16 { (raw[p] as u16) | ((raw[p + 1] as u16) << 8) };
    while o + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[o], raw[o + 1], raw[o + 2], raw[o + 3]]) as usize;
        let end = o + 4 + len;
        if end > raw.len() || len < 19 {
            return None;
        }
        let mut p = o + 4;
        let flags = raw[p];
        p += 1;
        let mut mid = [0u8; 16];
        mid.copy_from_slice(&raw[p..p + 16]);
        p += 16;
        let mut trace_id = None;
        if flags & FLAG_TRACE != 0 {
            if p + 16 > end {
                return None;
            }
            let mut t = [0u8; 16];
            t.copy_from_slice(&raw[p..p + 16]);
            trace_id = Some(t);
            p += 16;
        }
        if p + 2 > end {
            return None;
        }
        let txn_len = rd_u16(raw, p) as usize;
        p += 2;
        if p + txn_len > end {
            return None;
        }
        let txn = std::str::from_utf8(&raw[p..p + txn_len]).ok()?;
        p += txn_len;
        let mut producer_sub = None;
        if flags & FLAG_PSUB != 0 {
            if p + 2 > end {
                return None;
            }
            let ps_len = rd_u16(raw, p) as usize;
            p += 2;
            if p + ps_len > end {
                return None;
            }
            producer_sub = Some(std::str::from_utf8(&raw[p..p + ps_len]).ok()?);
            p += ps_len;
        }
        out.push(FrameRef {
            message_id: mid,
            txn,
            trace_id,
            producer_sub,
            payload: &raw[p..end],
            encrypted: (flags & FLAG_ENCRYPTED) != 0,
        });
        o = end;
    }
    if o == raw.len() {
        Some(out)
    } else {
        None
    }
}

pub fn unpack_frames(raw: &[u8]) -> Option<Vec<FrameOut>> {
    let mut out = Vec::new();
    let mut o = 0usize;
    let rd_u16 = |raw: &[u8], p: usize| -> u16 { (raw[p] as u16) | ((raw[p + 1] as u16) << 8) };
    while o + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[o], raw[o + 1], raw[o + 2], raw[o + 3]]) as usize;
        let end = o + 4 + len;
        if end > raw.len() || len < 19 {
            return None;
        }
        let mut p = o + 4;
        let flags = raw[p];
        p += 1;
        let mut f = FrameOut {
            message_id: String::new(),
            txn: String::new(),
            trace_id: None,
            producer_sub: None,
            payload: Vec::new(),
            encrypted: (flags & FLAG_ENCRYPTED) != 0,
        };
        let mut mid = [0u8; 16];
        mid.copy_from_slice(&raw[p..p + 16]);
        f.message_id = uuid_bytes_to_string(&mid);
        p += 16;
        if flags & FLAG_TRACE != 0 {
            if p + 16 > end {
                return None;
            }
            let mut t = [0u8; 16];
            t.copy_from_slice(&raw[p..p + 16]);
            f.trace_id = Some(uuid_bytes_to_string(&t));
            p += 16;
        }
        if p + 2 > end {
            return None;
        }
        let txn_len = rd_u16(raw, p) as usize;
        p += 2;
        if p + txn_len > end {
            return None;
        }
        // Fast path: txn was written by pack_frames from a &str, so it is valid
        // UTF-8; from_utf8's optimized validation beats the lossy chunk iterator.
        f.txn = match std::str::from_utf8(&raw[p..p + txn_len]) {
            Ok(s) => s.to_owned(),
            Err(_) => String::from_utf8_lossy(&raw[p..p + txn_len]).into_owned(),
        };
        p += txn_len;
        if flags & FLAG_PSUB != 0 {
            if p + 2 > end {
                return None;
            }
            let ps_len = rd_u16(raw, p) as usize;
            p += 2;
            if p + ps_len > end {
                return None;
            }
            f.producer_sub = Some(match std::str::from_utf8(&raw[p..p + ps_len]) {
                Ok(s) => s.to_owned(),
                Err(_) => String::from_utf8_lossy(&raw[p..p + ps_len]).into_owned(),
            });
            p += ps_len;
        }
        f.payload = raw[p..end].to_vec();
        out.push(f);
        o = end;
    }
    if o == raw.len() {
        Some(out)
    } else {
        None
    }
}

pub fn zstd_compress(raw: &[u8], level: i32) -> Vec<u8> {
    zstd::stream::encode_all(raw, level).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// ONE segment recipe (PLAN_KV_TIMERS.md §1.3 phase (b), §15 "Unit Rust").
//
// The fusion flush and the timer fire both produce a segment for
// `queen.log_push_*`: the zstd blob that lands in `queen.log_segments.blob` and
// the `16 * count` hash blob that lands in `queen.log_txns.hashes`. Before this
// extraction the recipe existed only inside `fusion::build_hashes_and_blob`, and
// the sweeper would have had to grow a second copy of it.
//
// It lives HERE and not in `fusion.rs` for a reason the plan states as a
// constraint: §1.8 keeps the two paths apart (the sweeper must not depend on the
// fusion module, and a module-level test pins that the fire's SQL never names
// `log_push_multi_v1`), and `frames.rs` is already the shared codec both sides
// use. `fusion::build_hashes_and_blob` is now a thin adapter over
// `pack_segment_with_hashes` — one recipe, two callers.
//
// What is actually at stake, in the order it would hurt:
//  1. the hash blob IS the dedup identity SQL compares, with a frame-order
//     stride: position k belongs to packed frame k. A misordered or short blob
//     does not fail loudly — it makes the dedup probe answer about a different
//     message, and the fire's own guard (`octet_length(hashes[i]) = counts[i]*16`,
//     plus "exactly counts[i] keys carry seg_of = i") is the only thing between
//     that and a timer deleted after the wrong frame was pushed;
//  2. the fire packs OUTSIDE any transaction and then commits all-or-nothing per
//     segment, so a blob that does not decode is discovered with the lease held
//     and costs a whole batch;
//  3. duplicate txns inside one segment must KEEP THEIR POSITIONS. The fire maps
//     the `i` the allocator echoes for a duplicate back to a timer row to delete,
//     so a dedup-at-pack-time "optimization" here would delete the wrong timer.
//     Neither function collapses anything.
// ---------------------------------------------------------------------------

/// One packed segment: the two blobs the push SPs take, plus the frame count
/// their alignment guards check the blobs against.
pub struct PackedSegment {
    /// `16 * count` bytes, frame order: frame k's xxh3_128 txn fingerprint at
    /// byte offset `16 * k`.
    pub hashes: Vec<u8>,
    /// `zstd(pack_frames(frames))` — the `queen.log_segments.blob` payload.
    pub blob: Vec<u8>,
    pub count: usize,
}

/// Pack a segment, computing each frame's txn fingerprint from its `txn`.
///
/// This is the form the timer fire uses: it holds decompressed payloads and the
/// fixed `txn` of each timer and has no hashes cached anywhere.
pub fn pack_segment(frames: &[FrameIn<'_>], zstd_level: i32) -> PackedSegment {
    let mut hashes = Vec::with_capacity(frames.len() * 16);
    for f in frames {
        hashes.extend_from_slice(&crate::util::txn_hash128(f.txn));
    }
    pack_segment_with_hashes(frames, hashes, zstd_level)
}

/// The same recipe when the caller ALREADY holds the fingerprints.
///
/// The fusion flush computes them once per frame at flush time and reuses them
/// across repacks (a bundle can be repacked after a `duplicate` verdict removes
/// some frames), so re-hashing here would add xxh3 work per frame per repack to
/// the push hot path — the path a 1% CPU-per-message gate is measured on. The
/// blob half is identical either way, which is the whole point of the split.
pub fn pack_segment_with_hashes(
    frames: &[FrameIn<'_>],
    hashes: Vec<u8>,
    zstd_level: i32,
) -> PackedSegment {
    debug_assert_eq!(
        hashes.len(),
        frames.len() * 16,
        "hash blob stride broken: the push SPs check octet_length(hashes) = count * 16"
    );
    PackedSegment {
        count: frames.len(),
        blob: zstd_compress(&pack_frames(frames), zstd_level),
        hashes,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pack_unpack_ref_round_trip() {
        let mid1 = [1u8; 16];
        let mid2 = [2u8; 16];
        let trace = [9u8; 16];
        let fins = vec![
            FrameIn {
                message_id: mid1,
                txn: "txn-one",
                trace_id: Some(trace),
                producer_sub: Some("sub@x"),
                payload: br#"{"a":1}"#,
                encrypted: false,
            },
            FrameIn {
                message_id: mid2,
                txn: "txn-two",
                trace_id: None,
                producer_sub: None,
                payload: b"",
                encrypted: true,
            },
        ];
        let packed = pack_frames(&fins);

        // Owned and borrowed decoders must agree with the input and each other.
        let owned = unpack_frames(&packed).expect("owned unpack");
        let brw = unpack_frames_ref(&packed).expect("ref unpack");
        assert_eq!(owned.len(), 2);
        assert_eq!(brw.len(), 2);
        for (o, b) in owned.iter().zip(brw.iter()) {
            assert_eq!(o.txn, b.txn);
            assert_eq!(o.producer_sub.as_deref(), b.producer_sub);
            assert_eq!(o.payload, b.payload);
            assert_eq!(o.encrypted, b.encrypted);
            let mut hex = String::new();
            uuid_hex_into(&mut hex, &b.message_id);
            assert_eq!(o.message_id, hex);
            assert_eq!(o.message_id, uuid_bytes_to_string(&b.message_id));
        }
        assert_eq!(brw[0].trace_id, Some(trace));
        assert_eq!(brw[1].trace_id, None);
        assert_eq!(brw[0].payload, br#"{"a":1}"#);
    }
}

pub fn zstd_decompress(blob: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(blob).unwrap_or_default()
}

// PLAN_KV_TIMERS §15 "Unit Rust": the `pack_segment` unit tests, written before
// this extraction landed. `#[path]` on a non-inline `mod` resolves relative to
// the directory of the file that DECLARES it (i.e. `src/`), and the module is a
// child of this one, so `use super::*` reaches everything above without making
// anything more public than it already is. See `src/tests_unit/README.md`.
#[cfg(test)]
#[path = "tests_unit/pack_segment.rs"]
mod pack_segment_tests;
