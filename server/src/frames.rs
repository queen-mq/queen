// Segment frame codec — wire-compatible with the C++ storage-v2 broker
// (server/include/queen/storage_v2.hpp). One segment = K length-prefixed frames,
// zstd-compressed together. Frame layout (little-endian):
//   u32 body_len
//   body: u8 flags | u8[16] message_id | [u8[16] trace_id]
//         | u16 txn_len | txn | [u16 psub_len | psub]
//         | [u16 headers_len | headers] | payload(JSON bytes)
//
// Two flags came with catalogue version 8 (PLAN_ROUTING.md):
//   - FLAG_HEADERS: the message's headers, a block of string pairs
//     (`u16 count | (u8 key_len | key | u16 value_len | value) × count`, keys
//     sorted), stored in clear whatever the queue's encryption.
//   - FLAG_LINK: a LINK, not a message. Its payload is the 24-byte address of
//     the message it stands for (`u64 qid | u64 pid | u64 offset`), and its
//     id and transaction id are that message's. Readers resolve it
//     (`rsm::route::link`) before they render anything.
// A build before version 8 never meets either: they are written only where
// the cluster version admits 8.

const FLAG_TRACE: u8 = 1;
const FLAG_PSUB: u8 = 2;
pub const FLAG_ENCRYPTED: u8 = 4;
pub const FLAG_HEADERS: u8 = 8;
pub const FLAG_LINK: u8 = 16;

/// The bytes of a link frame's payload: `u64 qid | u64 pid | u64 offset`.
pub const LINK_LEN: usize = 24;

/// What a link frame points at: the source message's queue log, partition and
/// offset. All three are the same on every node (a queue log's id is a hash of
/// its tenant and queue name, `QLogReader::queue_id_of`), so a link resolves
/// on any node that holds the source.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct LinkRef {
    pub qid: u64,
    pub pid: u64,
    pub offset: u64,
}

impl LinkRef {
    pub fn encode(&self) -> [u8; LINK_LEN] {
        let mut out = [0u8; LINK_LEN];
        out[0..8].copy_from_slice(&self.qid.to_le_bytes());
        out[8..16].copy_from_slice(&self.pid.to_le_bytes());
        out[16..24].copy_from_slice(&self.offset.to_le_bytes());
        out
    }

    pub fn decode(b: &[u8]) -> Option<LinkRef> {
        if b.len() != LINK_LEN {
            return None;
        }
        Some(LinkRef {
            qid: u64::from_le_bytes(b[0..8].try_into().ok()?),
            pid: u64::from_le_bytes(b[8..16].try_into().ok()?),
            offset: u64::from_le_bytes(b[16..24].try_into().ok()?),
        })
    }
}

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
    /// A headers block ([`encode_headers`]), or `None`. An empty block is
    /// written as no block.
    pub headers: Option<&'a [u8]>,
    /// `Some`: a link frame, whose payload is this address and not `payload`.
    pub link: Option<LinkRef>,
}

pub fn pack_frames(frames: &[FrameIn]) -> Vec<u8> {
    let mut est = 0usize;
    for f in frames {
        est += f.payload.len() + f.txn.len() + 64 + f.headers.map_or(0, <[u8]>::len);
    }
    let mut out = Vec::with_capacity(est);
    for f in frames {
        // A block of no headers (`u16 0`) is written as none.
        let headers = f.headers.filter(|h| h.len() > 2);
        let link = f.link.map(|l| l.encode());
        let payload: &[u8] = match &link {
            Some(l) => l,
            None => f.payload,
        };
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
        if headers.is_some() {
            flags |= FLAG_HEADERS;
        }
        if link.is_some() {
            flags |= FLAG_LINK;
        }
        // Body length is fully determined up front, so the frame is written
        // straight into `out` — no per-frame temp Vec (that was one extra
        // malloc + full memcpy per message on the flush hot path).
        let txn = f.txn.as_bytes();
        // The length is written as a u16 below while `body_len` counts the full
        // usize, so an over-long txn would produce a frame whose declared body
        // length and actual content disagree. The push handler rejects those at
        // facade (`MAX_TXN_BYTES` in `rsm::facade::real`); this pins the
        // invariant here, where it is actually load-bearing.
        debug_assert!(
            txn.len() <= u16::MAX as usize,
            "txn exceeds the u16 frame limit"
        );
        let mut body_len = 1 + 16 + 2 + txn.len() + payload.len();
        if f.trace_id.is_some() {
            body_len += 16;
        }
        if let Some(ps) = f.producer_sub {
            body_len += 2 + ps.len();
        }
        if let Some(h) = headers {
            // The receiver caps a block far below this (`QUEEN_MAX_HEADER_BYTES`).
            debug_assert!(
                h.len() <= u16::MAX as usize,
                "headers exceed the u16 frame limit"
            );
            body_len += 2 + h.len();
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
        if let Some(h) = headers {
            out.extend_from_slice(&(h.len() as u16).to_le_bytes());
            out.extend_from_slice(h);
        }
        out.extend_from_slice(payload);
    }
    out
}

/// One frame re-packed from a [`FrameRef`], as stored: the same bytes
/// [`pack_frames`] wrote for it.
pub fn repack_into(out: &mut Vec<u8>, f: &FrameRef<'_>) {
    out.extend_from_slice(&pack_frames(&[FrameIn {
        message_id: f.message_id,
        txn: f.txn,
        trace_id: f.trace_id,
        producer_sub: f.producer_sub,
        payload: f.payload,
        encrypted: f.encrypted,
        headers: f.headers,
        link: f.link,
    }]));
}

// ---------------------------------------------------------------- headers

/// The most headers one message may carry, by default (`QUEEN_MAX_HEADERS`).
pub const MAX_HEADERS_DEFAULT: usize = 64;
/// The most bytes one message's headers block may hold, by default
/// (`QUEEN_MAX_HEADER_BYTES`). Never above the u16 the frame stores.
pub const MAX_HEADER_BYTES_DEFAULT: usize = 8 * 1024;
/// The longest header name.
pub const MAX_HEADER_KEY_BYTES: usize = 64;

/// `QUEEN_MAX_HEADERS`, read once.
pub fn max_headers() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("QUEEN_MAX_HEADERS")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(MAX_HEADERS_DEFAULT)
    })
}

/// `QUEEN_MAX_HEADER_BYTES`, read once, at most 65,535 (the frame's u16).
pub fn max_header_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("QUEEN_MAX_HEADER_BYTES")
            .ok()
            .and_then(|v| v.trim().parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(MAX_HEADER_BYTES_DEFAULT)
            .min(u16::MAX as usize)
    })
}

/// Whether `key` may name a header: 1 to [`MAX_HEADER_KEY_BYTES`] bytes of
/// `a-z 0-9 . _ -`.
pub fn header_key_valid(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= MAX_HEADER_KEY_BYTES
        && key.bytes().all(|b| {
            b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
        })
}

/// The prefix of the headers the broker sets; a client may not set them.
pub const RESERVED_HEADER_PREFIX: &str = "queen-";

/// Encode headers as a block, sorted by name (a name given twice keeps its
/// last value). Checks nothing: [`check_headers`] does, on the way in.
pub fn encode_headers<'a>(pairs: impl IntoIterator<Item = (&'a str, &'a str)>) -> Vec<u8> {
    let mut sorted: std::collections::BTreeMap<&str, &str> = std::collections::BTreeMap::new();
    for (k, v) in pairs {
        sorted.insert(k, v);
    }
    let mut out = Vec::with_capacity(2 + sorted.len() * 16);
    out.extend_from_slice(&(sorted.len() as u16).to_le_bytes());
    for (k, v) in sorted {
        out.push(k.len() as u8);
        out.extend_from_slice(k.as_bytes());
        out.extend_from_slice(&(v.len() as u16).to_le_bytes());
        out.extend_from_slice(v.as_bytes());
    }
    out
}

/// The pairs of a headers block, in its (name) order. `None` for a block that
/// does not decode; [`encode_headers`] never writes one.
pub fn decode_headers(block: &[u8]) -> Option<Vec<(&str, &str)>> {
    if block.len() < 2 {
        return None;
    }
    let n = u16::from_le_bytes([block[0], block[1]]) as usize;
    let mut out = Vec::with_capacity(n.min(256));
    let mut p = 2usize;
    for _ in 0..n {
        let kl = *block.get(p)? as usize;
        p += 1;
        let k = std::str::from_utf8(block.get(p..p + kl)?).ok()?;
        p += kl;
        let vl = u16::from_le_bytes([*block.get(p)?, *block.get(p + 1)?]) as usize;
        p += 2;
        let v = std::str::from_utf8(block.get(p..p + vl)?).ok()?;
        p += vl;
        out.push((k, v));
    }
    (p == block.len()).then_some(out)
}

/// One header's value in a block.
pub fn header_get<'b>(block: &'b [u8], key: &str) -> Option<&'b str> {
    decode_headers(block)?
        .into_iter()
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

/// `base` with every header of `top` written over it: what a link's reader
/// sees (the source's headers, then the link's own). `None` when both are.
pub fn overlay_headers(base: Option<&[u8]>, top: Option<&[u8]>) -> Option<Vec<u8>> {
    match (base, top) {
        (None, None) => None,
        (Some(b), None) => Some(b.to_vec()),
        (None, Some(t)) => Some(t.to_vec()),
        (Some(b), Some(t)) => {
            let mut pairs = decode_headers(b).unwrap_or_default();
            pairs.extend(decode_headers(t).unwrap_or_default());
            Some(encode_headers(pairs))
        }
    }
}

/// A headers block as a JSON object (`{"type":"order.paid"}`); `{}` for an
/// empty or broken one.
pub fn headers_json_into(out: &mut String, block: &[u8]) {
    out.push('{');
    for (i, (k, v)) in decode_headers(block)
        .unwrap_or_default()
        .into_iter()
        .enumerate()
    {
        if i > 0 {
            out.push(',');
        }
        out.push('"');
        crate::util::json_escape_into(out, k);
        out.push_str("\":\"");
        crate::util::json_escape_into(out, v);
        out.push('"');
    }
    out.push('}');
}

/// A headers block as a JSON value.
pub fn headers_json(block: &[u8]) -> serde_json::Value {
    let mut m = serde_json::Map::new();
    for (k, v) in decode_headers(block).unwrap_or_default() {
        m.insert(k.to_string(), serde_json::Value::String(v.to_string()));
    }
    serde_json::Value::Object(m)
}

/// Why a client's headers are refused, checked against the limits: a name
/// outside `a-z 0-9 . _ -` or longer than 64 bytes, a reserved `queen-` name,
/// a value that is not a string, too many, too many bytes.
pub fn check_headers(v: &serde_json::Value) -> Result<Vec<u8>, String> {
    let serde_json::Value::Object(m) = v else {
        return Err("headers must be an object of strings".into());
    };
    if m.len() > max_headers() {
        return Err(format!(
            "at most {} headers per message (QUEEN_MAX_HEADERS)",
            max_headers()
        ));
    }
    if m.is_empty() {
        return Ok(Vec::new());
    }
    let mut pairs = Vec::with_capacity(m.len());
    for (k, val) in m {
        if !header_key_valid(k) {
            return Err(format!(
                "header name {k:?}: 1 to {MAX_HEADER_KEY_BYTES} bytes of a-z, 0-9, '.', '_' or '-'"
            ));
        }
        if k.starts_with(RESERVED_HEADER_PREFIX) {
            return Err(format!(
                "header name {k:?}: names starting with \"queen-\" are the broker's"
            ));
        }
        let Some(s) = val.as_str() else {
            return Err(format!("header {k:?}: the value must be a string"));
        };
        pairs.push((k.as_str(), s));
    }
    let block = encode_headers(pairs);
    if block.len() > max_header_bytes() {
        return Err(format!(
            "headers of {} B exceed {} B per message (QUEEN_MAX_HEADER_BYTES)",
            block.len(),
            max_header_bytes()
        ));
    }
    Ok(block)
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

/// An owned unpacked frame ([`unpack_frames`]); the broker reads frames with
/// [`unpack_frames_ref`], so only the fuzz decoders and the tests use this.
#[cfg(any(test, feature = "fuzzing"))]
pub struct FrameOut {
    pub message_id: String,
    pub txn: String,
    pub trace_id: Option<String>,
    pub producer_sub: Option<String>,
    pub payload: Vec<u8>,
    pub encrypted: bool,
    pub headers: Option<Vec<u8>>,
    pub link: Option<LinkRef>,
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
    /// The headers block, when the frame carries one ([`decode_headers`]).
    pub headers: Option<&'a [u8]>,
    /// `Some` for a link frame: the message it stands for. `payload` then
    /// holds the 24 address bytes, never a message.
    pub link: Option<LinkRef>,
}

impl FrameRef<'_> {
    /// Whether this frame is a link (resolved or not by the reader).
    pub fn is_link(&self) -> bool {
        self.link.is_some()
    }
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
        let mut headers = None;
        if flags & FLAG_HEADERS != 0 {
            if p + 2 > end {
                return None;
            }
            let h_len = rd_u16(raw, p) as usize;
            p += 2;
            if p + h_len > end {
                return None;
            }
            headers = Some(&raw[p..p + h_len]);
            p += h_len;
        }
        let payload = &raw[p..end];
        let link = if flags & FLAG_LINK != 0 {
            Some(LinkRef::decode(payload)?)
        } else {
            None
        };
        out.push(FrameRef {
            message_id: mid,
            txn,
            trace_id,
            producer_sub,
            payload,
            encrypted: (flags & FLAG_ENCRYPTED) != 0,
            headers,
            link,
        });
        o = end;
    }
    if o == raw.len() {
        Some(out)
    } else {
        None
    }
}

/// Rewrite the source pid of every link frame in `raw` with `f`, in place: the
/// footprint scheduler numbers a partition its cycle created once the cycle is
/// planned, and a link to a message of that partition carries its pid. A link
/// frame's payload is its address, so the pid sits 16 bytes before the frame's
/// end. `false` when `raw` is not a run of whole frames.
pub fn relink(raw: &mut [u8], f: &mut impl FnMut(u64) -> u64) -> bool {
    let mut o = 0usize;
    while o + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[o], raw[o + 1], raw[o + 2], raw[o + 3]]) as usize;
        let end = o + 4 + len;
        if end > raw.len() || len < 19 {
            return false;
        }
        if raw[o + 4] & FLAG_LINK != 0 {
            if len < 19 + LINK_LEN {
                return false;
            }
            let at = end - 16;
            let mut b = [0u8; 8];
            b.copy_from_slice(&raw[at..at + 8]);
            raw[at..at + 8].copy_from_slice(&f(u64::from_le_bytes(b)).to_le_bytes());
        }
        o = end;
    }
    o == raw.len()
}

/// Whether a segment is links only: it decodes, and every frame is a link.
/// Unlike [`any_link`], a malformed segment answers false.
pub fn all_links(raw: &[u8]) -> bool {
    unpack_frames_ref(raw).is_some_and(|f| !f.is_empty() && f.iter().all(FrameRef::is_link))
}

/// Whether any frame of a segment is a link, reading only lengths and flags.
/// A malformed segment answers true, as [`any_flag`].
pub fn any_link(raw: &[u8]) -> bool {
    any_flag(raw, FLAG_LINK)
}

/// Whether any frame of a segment carries a flag of `mask`, reading only
/// lengths and flags. A malformed segment answers true, so a caller that would
/// send the frames as they are leaves them to the full unpack.
pub fn any_flag(raw: &[u8], mask: u8) -> bool {
    let mut o = 0usize;
    while o + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[o], raw[o + 1], raw[o + 2], raw[o + 3]]) as usize;
        if len < 19 || len > raw.len() - (o + 4) {
            return true;
        }
        if raw[o + 4] & mask != 0 {
            return true;
        }
        o += 4 + len;
    }
    o != raw.len()
}

/// The flags of every frame of a segment, or-ed, reading only lengths and
/// flags. A malformed segment answers every flag, as [`any_flag`] answers
/// true: whoever would hand the frames out as they are leaves them to the
/// full unpack.
pub fn flags_union(raw: &[u8]) -> u8 {
    let mut flags = 0u8;
    let mut o = 0usize;
    while o + 4 <= raw.len() {
        let len = u32::from_le_bytes([raw[o], raw[o + 1], raw[o + 2], raw[o + 3]]) as usize;
        if len < 19 || len > raw.len() - (o + 4) {
            return u8::MAX;
        }
        flags |= raw[o + 4];
        o += 4 + len;
    }
    if o != raw.len() {
        return u8::MAX;
    }
    flags
}

#[cfg(any(test, feature = "fuzzing"))]
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
            headers: None,
            link: None,
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
        if flags & FLAG_HEADERS != 0 {
            if p + 2 > end {
                return None;
            }
            let h_len = rd_u16(raw, p) as usize;
            p += 2;
            if p + h_len > end {
                return None;
            }
            f.headers = Some(raw[p..p + h_len].to_vec());
            p += h_len;
        }
        f.payload = raw[p..end].to_vec();
        if flags & FLAG_LINK != 0 {
            f.link = Some(LinkRef::decode(&f.payload)?);
        }
        out.push(f);
        o = end;
    }
    if o == raw.len() {
        Some(out)
    } else {
        None
    }
}

#[cfg(test)]
pub fn zstd_compress(raw: &[u8], level: i32) -> Vec<u8> {
    zstd::stream::encode_all(raw, level).unwrap_or_default()
}

/// Why [`zstd_decompress_bounded`] gave nothing back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ZstdBound {
    /// Not a zstd stream (or a broken one).
    Invalid,
    /// It inflates past the limit.
    TooLarge,
}

/// A zstd payload a client sent, decompressed only while it stays within
/// `limit` bytes. A 1 MiB frame of zeros inflates to gigabytes, and
/// `decode_all` would hold all of it before any ceiling looked at the size;
/// here the stream stops one byte past the limit.
pub fn zstd_decompress_bounded(blob: &[u8], limit: usize) -> Result<Vec<u8>, ZstdBound> {
    use std::io::Read;
    let decoder = zstd::stream::read::Decoder::new(blob).map_err(|_| ZstdBound::Invalid)?;
    let mut out = Vec::new();
    // One byte more than the limit: reading it says the payload is over.
    decoder
        .take(limit as u64 + 1)
        .read_to_end(&mut out)
        .map_err(|_| ZstdBound::Invalid)?;
    if out.len() > limit {
        return Err(ZstdBound::TooLarge);
    }
    Ok(out)
}

fn env_limit(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(default)
}

/// The most messages one pop may ask for (`QUEEN_POP_MAX_BATCH`, default
/// 10,000). A larger `batch` is clamped, never refused: the client gets a
/// full batch and pops again.
pub fn pop_max_batch() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_limit("QUEEN_POP_MAX_BATCH", 10_000))
}

/// The payload bytes one pop answer may carry (`QUEEN_POP_MAX_BYTES`,
/// default 64 MiB). The claim stops taking messages once its stored sizes
/// reach it (at least one message always goes out), so a batch of 10,000
/// one-megabyte messages is not rendered into one 10 GB string.
pub fn pop_max_bytes() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| env_limit("QUEEN_POP_MAX_BYTES", 64 << 20))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_zstd_stops_past_the_limit() {
        let raw = vec![0u8; 1 << 20];
        let blob = zstd_compress(&raw, 3);
        assert_eq!(
            zstd_decompress_bounded(&blob, 1 << 20).map(|v| v.len()),
            Ok(1 << 20)
        );
        assert_eq!(
            zstd_decompress_bounded(&blob, (1 << 20) - 1),
            Err(ZstdBound::TooLarge)
        );
        assert_eq!(
            zstd_decompress_bounded(b"not zstd", 1 << 20),
            Err(ZstdBound::Invalid)
        );
    }

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
                headers: None,
                link: None,
            },
            FrameIn {
                message_id: mid2,
                txn: "txn-two",
                trace_id: None,
                producer_sub: None,
                payload: b"",
                encrypted: true,
                headers: None,
                link: None,
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

    #[test]
    fn any_flag_reads_each_frames_flags() {
        let frame = |txn: &'static str, encrypted: bool| FrameIn {
            message_id: [1; 16],
            txn,
            trace_id: Some([2; 16]),
            producer_sub: Some("sub"),
            payload: br#"{"a":1}"#,
            encrypted,
            headers: None,
            link: None,
        };
        let encrypted = |raw: &[u8]| any_flag(raw, FLAG_ENCRYPTED);
        assert!(!encrypted(&[]));
        let plain = pack_frames(&[frame("t1", false), frame("t2", false)]);
        assert!(!encrypted(&plain));
        assert!(!any_flag(&plain, FLAG_HEADERS | FLAG_LINK));
        let second = pack_frames(&[frame("t1", false), frame("t2", true)]);
        assert!(encrypted(&second));
        // A segment it cannot follow answers true: the caller unpacks it whole.
        assert!(encrypted(&plain[..plain.len() - 1]));
        let mut short = plain.clone();
        short[..4].copy_from_slice(&3u32.to_le_bytes());
        assert!(encrypted(&short));
    }
}
