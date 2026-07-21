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

pub struct FrameOut {
    pub message_id: String,
    pub txn: String,
    pub trace_id: Option<String>,
    pub producer_sub: Option<String>,
    pub payload: Vec<u8>,
    pub encrypted: bool,
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

pub fn zstd_decompress(blob: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(blob).unwrap_or_default()
}
