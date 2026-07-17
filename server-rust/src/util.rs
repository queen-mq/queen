use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::engine::Job;

// ---- UUIDv7 (time-ordered) — mirrors the Go/C++ generators ----
static LAST_MS: AtomicU64 = AtomicU64::new(0);
static SEQ: AtomicU64 = AtomicU64::new(0);
const HEX: &[u8; 16] = b"0123456789abcdef";

pub fn uuidv7() -> String {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Monotonic sub-ms sequence so message ids keep time order within a ms.
    let last = LAST_MS.load(Ordering::Relaxed);
    let seq = if now_ms <= last {
        SEQ.fetch_add(1, Ordering::Relaxed) + 1
    } else {
        LAST_MS.store(now_ms, Ordering::Relaxed);
        SEQ.store(0, Ordering::Relaxed);
        0
    };
    let ms = LAST_MS.load(Ordering::Relaxed);
    let r: u64 = rand::random();

    let mut b = [0u8; 16];
    b[0] = (ms >> 40) as u8;
    b[1] = (ms >> 32) as u8;
    b[2] = (ms >> 24) as u8;
    b[3] = (ms >> 16) as u8;
    b[4] = (ms >> 8) as u8;
    b[5] = ms as u8;
    b[6] = 0x70 | (((seq >> 8) & 0x0f) as u8);
    b[7] = seq as u8;
    b[8] = 0x80 | (((r >> 58) & 0x3f) as u8);
    b[9] = (r >> 50) as u8;
    b[10] = (r >> 42) as u8;
    b[11] = (r >> 34) as u8;
    b[12] = (r >> 26) as u8;
    b[13] = (r >> 18) as u8;
    b[14] = (r >> 10) as u8;
    b[15] = (r >> 2) as u8;

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
    // Safety: out is ASCII hex + dashes.
    unsafe { String::from_utf8_unchecked(out.to_vec()) }
}

// ---- fusion merge: concatenate each job's item objects into one JSON array,
// front-injecting a renumbered idx/index per item (libqueen _fire_batched). ----
pub fn build_merged(batch: &[Job]) -> (String, Vec<(usize, usize)>) {
    let mut est = 16usize;
    for j in batch {
        for it in &j.items {
            est += it.len() + 32;
        }
    }
    let mut buf: Vec<u8> = Vec::with_capacity(est);
    buf.push(b'[');
    let mut ranges = Vec::with_capacity(batch.len());
    let mut seq: usize = 0;
    let mut first = true;
    for j in batch {
        let start = seq;
        let mut count = 0usize;
        for item in &j.items {
            if let Some(brace) = item.iter().position(|&c| c == b'{') {
                if !first {
                    buf.push(b',');
                }
                first = false;
                buf.extend_from_slice(b"{\"idx\":");
                buf.extend_from_slice(seq.to_string().as_bytes());
                buf.extend_from_slice(b",\"index\":");
                buf.extend_from_slice(seq.to_string().as_bytes());
                let body = &item[brace + 1..]; // after '{', includes trailing '}'
                let trimmed_start = body
                    .iter()
                    .position(|&c| c != b' ' && c != b'\t' && c != b'\r' && c != b'\n');
                match trimmed_start {
                    Some(ts) if body[ts] != b'}' => {
                        buf.push(b',');
                        buf.extend_from_slice(body);
                    }
                    _ => buf.push(b'}'),
                }
                seq += 1;
                count += 1;
            }
        }
        ranges.push((start, count));
    }
    buf.push(b']');
    (unsafe { String::from_utf8_unchecked(buf) }, ranges)
}

// Read the first "idx"/"index" integer from a JSON object's raw bytes without
// parsing the rest.
pub fn extract_leading_idx(raw: &[u8]) -> i64 {
    for key in [b"\"idx\":".as_slice(), b"\"index\":".as_slice()] {
        if let Some(i) = find_sub(raw, key) {
            let mut j = i + key.len();
            while j < raw.len() && (raw[j] == b' ' || raw[j] == b'\t') {
                j += 1;
            }
            let neg = j < raw.len() && raw[j] == b'-';
            if neg {
                j += 1;
            }
            let start = j;
            let mut n: i64 = 0;
            while j < raw.len() && raw[j].is_ascii_digit() {
                n = n * 10 + (raw[j] - b'0') as i64;
                j += 1;
            }
            if j > start {
                return if neg { -n } else { n };
            }
        }
    }
    -1
}

// Extract the raw bytes of the "result" object inside {"idx":N,"result":{...}}
// via brace matching (string-aware), no JSON parse. Mirrors C++ raw slicing.
pub fn extract_result_object(raw: &[u8]) -> Option<&[u8]> {
    let key = b"\"result\":".as_slice();
    let i = find_sub(raw, key)?;
    let mut j = i + key.len();
    while j < raw.len() && matches!(raw[j], b' ' | b'\t' | b'\n' | b'\r') {
        j += 1;
    }
    if j >= raw.len() || raw[j] != b'{' {
        return None;
    }
    let start = j;
    let mut depth = 0i32;
    let mut in_str = false;
    let mut esc = false;
    while j < raw.len() {
        let c = raw[j];
        if in_str {
            if esc {
                esc = false;
            } else if c == b'\\' {
                esc = true;
            } else if c == b'"' {
                in_str = false;
            }
        } else {
            match c {
                b'"' => in_str = true,
                b'{' => depth += 1,
                b'}' => {
                    depth -= 1;
                    if depth == 0 {
                        return Some(&raw[start..=j]);
                    }
                }
                _ => {}
            }
        }
        j += 1;
    }
    None
}

fn find_sub(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|w| w == needle)
}

pub fn count_occurrences(haystack: &[u8], needle: &[u8]) -> usize {
    if needle.is_empty() {
        return 0;
    }
    let mut count = 0;
    let mut i = 0;
    while i + needle.len() <= haystack.len() {
        if &haystack[i..i + needle.len()] == needle {
            count += 1;
            i += needle.len();
        } else {
            i += 1;
        }
    }
    count
}
