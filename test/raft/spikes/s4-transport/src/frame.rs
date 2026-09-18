//! Framing of PLAN_RAFT.md §9.2: `len u32 | xxh3 u64 | type u8 | body`,
//! plus the optional per-frame MAC of D12 ("a MAC on every frame, so frames
//! cannot be injected after the handshake").
//!
//! `len` counts everything after the hash field (type + body + mac) and the
//! xxh3 covers the same bytes.
//!
//! **Revised 2026-09-18 after the WP-0.6 refutation.** The form measured in
//! the VM passes A/B MACed `type || body` only, with one key for both
//! directions and no counter, which left three holes:
//!   - a recorded frame replays on the same connection (after the D6 request
//!     id window of 600 s has expired, the planner would plan it again);
//!   - a frame recorded in one direction verifies in the other;
//!   - `len` was not authenticated.
//! The MAC now covers `dir || seq || len || type || body`, where `dir` is the
//! direction byte and `seq` is a per-direction 64-bit frame counter that is
//! NOT on the wire: both sides count, the receiver accepts only its expected
//! next value, so a replayed, reordered or injected frame fails. The key is
//! derived per direction from the handshake session key.
//!
//! Cost of the change: 13 bytes more HMAC input per frame over the ~2.8 KB
//! that was already hashed (+0.5 %, an order of magnitude below the 5 %
//! pass-to-pass spread of the measured rows) plus two HMACs per connection at
//! setup. It does, however, force the MAC onto the frame sequencer — the
//! writer task — because the sequence number must be assigned where frames
//! are serialised. That is the same task on which rustls encrypts, so the
//! measured MAC-against-TLS comparison (RESULTS-vm.md §5) is unaffected: both
//! run on the writer.
//!
//! Frames are only ever read by a dedicated per-connection reader loop with
//! `read_exact`, never inside a `select!` (cancellation safety, §9.2).
//!
//! `MAX_FRAME` is 8 MiB here because this spike only ever writes 2.8 KB
//! frames. It is NOT reconciled with `QUEEN_RAFT_ENTRY_MAX_BYTES` (96 MiB,
//! §5.1): before `rsm/net/` carries AppendEntries, either the cap rises (and
//! `buf.resize(len)` must then be bounded by a negotiated maximum, not by an
//! attacker's `len`) or large entries are chunked. Open item in MEMO.md.

use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt};

pub const HDR: usize = 12;
pub const MAC_LEN: usize = 16;
pub const MAX_FRAME: usize = 8 * 1024 * 1024;

pub const T_CMD: u8 = 1;
pub const T_OUTCOME: u8 = 2;
pub const T_STATS_REQ: u8 = 3;
pub const T_STATS_RESP: u8 = 4;
pub const T_HELLO: u8 = 10;
pub const T_HELLO_ACK: u8 = 11;
pub const T_AUTH: u8 = 12;
pub const T_AUTH_OK: u8 = 13;

pub type SessionKey = [u8; 32];

/// Direction bytes: which side wrote the frame.
pub const DIR_C2S: u8 = 1;
pub const DIR_S2C: u8 = 2;

/// Per-direction MAC state: the direction's own key (derived from the
/// handshake session key) and the frame counter. One per direction per
/// connection; the sender's and the receiver's must stay in step.
pub struct MacCtx {
    key: SessionKey,
    dir: u8,
    seq: u64,
}

impl MacCtx {
    pub fn new(session: &SessionKey, dir: u8) -> Self {
        let mut m = <Hmac<Sha256> as Mac>::new_from_slice(session).unwrap();
        m.update(&[b'D', dir]);
        let out = m.finalize().into_bytes();
        let mut key = [0u8; 32];
        key.copy_from_slice(&out);
        MacCtx { key, dir, seq: 0 }
    }
    pub fn seq(&self) -> u64 {
        self.seq
    }
}

fn frame_hmac(c: &MacCtx, len: u32, ty: u8, body: &[u8]) -> Hmac<Sha256> {
    let mut m = <Hmac<Sha256> as Mac>::new_from_slice(&c.key).unwrap();
    m.update(&[c.dir]);
    m.update(&c.seq.to_le_bytes());
    m.update(&len.to_le_bytes());
    m.update(&[ty]);
    m.update(body);
    m
}

fn frame_mac(c: &MacCtx, len: u32, ty: u8, body: &[u8]) -> [u8; MAC_LEN] {
    let out = frame_hmac(c, len, ty, body).finalize().into_bytes();
    let mut t = [0u8; MAC_LEN];
    t.copy_from_slice(&out[..MAC_LEN]);
    t
}

/// Appends a frame to `out` (so a writer task can coalesce several frames into
/// one `write_all`).
pub fn encode_into(out: &mut Vec<u8>, ty: u8, body: &[u8], mac_ctx: Option<&mut MacCtx>) {
    let len = 1 + body.len() + if mac_ctx.is_some() { MAC_LEN } else { 0 };
    let mac = mac_ctx.map(|c| {
        let m = frame_mac(c, len as u32, ty, body);
        c.seq += 1;
        m
    });
    let start = out.len();
    out.extend_from_slice(&(len as u32).to_le_bytes());
    out.extend_from_slice(&0u64.to_le_bytes()); // hash placeholder
    out.push(ty);
    out.extend_from_slice(body);
    if let Some(m) = mac {
        out.extend_from_slice(&m);
    }
    let h = xxhash_rust::xxh3::xxh3_64(&out[start + HDR..]);
    out[start + 4..start + HDR].copy_from_slice(&h.to_le_bytes());
}

pub fn encode(ty: u8, body: &[u8], mac_ctx: Option<&mut MacCtx>) -> Vec<u8> {
    let mut v = Vec::with_capacity(HDR + 1 + body.len() + MAC_LEN);
    encode_into(&mut v, ty, body, mac_ctx);
    v
}

/// Reads one whole frame into `buf`; returns the type and the body range.
/// `buf` is reused across frames by the caller.
pub async fn read_frame<R: AsyncRead + Unpin>(
    r: &mut R,
    buf: &mut Vec<u8>,
    mac_ctx: Option<&mut MacCtx>,
) -> io::Result<(u8, std::ops::Range<usize>)> {
    let mut hdr = [0u8; HDR];
    r.read_exact(&mut hdr).await?;
    let len = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let want = u64::from_le_bytes(hdr[4..12].try_into().unwrap());
    if len < 1 || len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame length"));
    }
    buf.clear();
    buf.resize(len, 0);
    r.read_exact(&mut buf[..]).await?;
    if xxhash_rust::xxh3::xxh3_64(&buf[..]) != want {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame checksum"));
    }
    let ty = buf[0];
    let mut end = len;
    if let Some(c) = mac_ctx {
        if len < 1 + MAC_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "frame mac missing",
            ));
        }
        end = len - MAC_LEN;
        // Constant time, like mesh.rs's handshake check (hmac's verify_*).
        // The expected sequence number is part of the input, so a replayed or
        // reordered frame fails here; so does a frame recorded in the other
        // direction (its key and its `dir` byte differ).
        if frame_hmac(c, len as u32, ty, &buf[1..end])
            .verify_truncated_left(&buf[end..])
            .is_err()
        {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "frame mac"));
        }
        c.seq += 1;
    }
    Ok((ty, 1..end))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(dir: u8) -> MacCtx {
        MacCtx::new(&[9u8; 32], dir)
    }

    async fn roundtrip(mac: bool, corrupt: Option<usize>) -> io::Result<(u8, Vec<u8>)> {
        let body = vec![7u8; 300];
        let (mut sc, mut rc) = (ctx(DIR_C2S), ctx(DIR_C2S));
        let mut bytes = encode(T_CMD, &body, mac.then(|| &mut sc));
        if let Some(i) = corrupt {
            bytes[i] ^= 0x01;
        }
        let mut cur = std::io::Cursor::new(bytes);
        let mut buf = Vec::new();
        let (ty, r) = read_frame(&mut cur, &mut buf, mac.then(|| &mut rc)).await?;
        Ok((ty, buf[r].to_vec()))
    }

    #[tokio::test]
    async fn frame_roundtrip_plain_and_mac() {
        let (ty, b) = roundtrip(false, None).await.unwrap();
        assert_eq!((ty, b.len()), (T_CMD, 300));
        let (ty, b) = roundtrip(true, None).await.unwrap();
        assert_eq!((ty, b.len()), (T_CMD, 300));
    }

    #[tokio::test]
    async fn corrupt_body_is_rejected() {
        // a flipped bit in the body fails the xxh3 check ...
        assert!(roundtrip(false, Some(HDR + 5)).await.is_err());
        // ... and with a MAC on, a body forged with a matching xxh3 still fails
        let body = vec![7u8; 300];
        let (mut sc, mut rc) = (ctx(DIR_C2S), ctx(DIR_C2S));
        let mut bytes = encode(T_CMD, &body, Some(&mut sc));
        bytes[HDR + 5] ^= 0x01; // forge the body
        let h = xxhash_rust::xxh3::xxh3_64(&bytes[HDR..]);
        bytes[4..HDR].copy_from_slice(&h.to_le_bytes()); // and repair the checksum
        let mut cur = std::io::Cursor::new(bytes);
        let mut buf = Vec::new();
        let e = read_frame(&mut cur, &mut buf, Some(&mut rc))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "frame mac");
    }

    #[tokio::test]
    async fn a_frame_from_another_session_is_rejected() {
        let body = vec![1u8; 64];
        let mut sc = MacCtx::new(&[1u8; 32], DIR_C2S);
        let mut rc = MacCtx::new(&[2u8; 32], DIR_C2S);
        let bytes = encode(T_CMD, &body, Some(&mut sc));
        let mut cur = std::io::Cursor::new(bytes);
        let mut buf = Vec::new();
        assert!(read_frame(&mut cur, &mut buf, Some(&mut rc)).await.is_err());
    }

    /// The hole the 2026-09-18 refutation found in the measured form: a
    /// recorded frame must not verify a second time on the same connection.
    #[tokio::test]
    async fn a_replayed_frame_is_rejected() {
        let body = vec![3u8; 128];
        let (mut sc, mut rc) = (ctx(DIR_C2S), ctx(DIR_C2S));
        let first = encode(T_CMD, &body, Some(&mut sc));
        let second = encode(T_CMD, &body, Some(&mut sc));
        assert_ne!(
            first, second,
            "the same body twice must not be the same bytes"
        );
        let mut buf = Vec::new();
        let mut cur = std::io::Cursor::new(first.clone());
        read_frame(&mut cur, &mut buf, Some(&mut rc)).await.unwrap();
        // replaying frame 0 where frame 1 is expected fails
        let mut cur = std::io::Cursor::new(first);
        let e = read_frame(&mut cur, &mut buf, Some(&mut rc))
            .await
            .unwrap_err();
        assert_eq!(e.to_string(), "frame mac");
        // and the legitimate next frame still verifies
        let mut cur = std::io::Cursor::new(second);
        let mut rc2 = ctx(DIR_C2S);
        rc2.seq = 1;
        read_frame(&mut cur, &mut buf, Some(&mut rc2))
            .await
            .unwrap();
    }

    /// ... and must not verify in the other direction (reflection).
    #[tokio::test]
    async fn a_frame_does_not_verify_in_the_other_direction() {
        let body = vec![4u8; 64];
        let mut sc = ctx(DIR_C2S);
        let bytes = encode(T_OUTCOME, &body, Some(&mut sc));
        let mut rc = ctx(DIR_S2C);
        let mut buf = Vec::new();
        let mut cur = std::io::Cursor::new(bytes);
        assert!(read_frame(&mut cur, &mut buf, Some(&mut rc)).await.is_err());
    }

    #[tokio::test]
    async fn the_length_field_is_authenticated() {
        let body = vec![5u8; 64];
        let (mut sc, mut rc) = (ctx(DIR_C2S), ctx(DIR_C2S));
        let mut bytes = encode(T_CMD, &body, Some(&mut sc));
        // truncate the body by one byte and repair len + checksum
        let newlen = (bytes.len() - HDR - 1) as u32;
        bytes.remove(HDR + 1);
        bytes[0..4].copy_from_slice(&newlen.to_le_bytes());
        let h = xxhash_rust::xxh3::xxh3_64(&bytes[HDR..]);
        bytes[4..HDR].copy_from_slice(&h.to_le_bytes());
        let mut cur = std::io::Cursor::new(bytes);
        let mut buf = Vec::new();
        assert!(read_frame(&mut cur, &mut buf, Some(&mut rc)).await.is_err());
    }
}
