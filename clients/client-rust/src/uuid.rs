//! uuidv7 generation, matching the broker's `util::uuidv7_bytes`.
//!
//! Transaction ids are minted client-side on purpose: the id is what makes a
//! push idempotent inside the dedup window, so a caller that retries after a
//! timeout has to send the *same* id. Letting the broker mint it would make the
//! id unknowable until the response arrives, which is exactly when it is too
//! late to be useful.
//!
//! v7 rather than v4 because the leading timestamp keeps generated ids roughly
//! ordered, which is kinder to the index they land in.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

static LAST_MS: AtomicU64 = AtomicU64::new(0);
static SEQ: AtomicU64 = AtomicU64::new(0);

/// A uuidv7 as raw bytes.
///
/// Within one millisecond a monotonic counter occupies the sub-millisecond
/// bits, so ids minted in a tight loop stay ordered and distinct rather than
/// relying on the random tail alone.
pub fn uuidv7_bytes() -> [u8; 16] {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0);

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
    // Version 7 in the high nibble, then 12 bits of the intra-millisecond
    // counter.
    b[6] = 0x70 | (((seq >> 8) & 0x0f) as u8);
    b[7] = seq as u8;
    // RFC 4122 variant, then random.
    b[8] = 0x80 | (((r >> 58) & 0x3f) as u8);
    b[9] = (r >> 50) as u8;
    b[10] = (r >> 42) as u8;
    b[11] = (r >> 34) as u8;
    b[12] = (r >> 26) as u8;
    b[13] = (r >> 18) as u8;
    b[14] = (r >> 10) as u8;
    b[15] = (r >> 2) as u8;
    b
}

/// A uuidv7 in canonical hyphenated lowercase hex.
pub fn uuidv7() -> String {
    format_uuid(&uuidv7_bytes())
}

fn format_uuid(b: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(36);
    for (i, byte) in b.iter().enumerate() {
        if matches!(i, 4 | 6 | 8 | 10) {
            s.push('-');
        }
        s.push(HEX[(byte >> 4) as usize] as char);
        s.push(HEX[(byte & 0x0f) as usize] as char);
    }
    s
}

/// Whether `s` is a syntactically valid UUID.
///
/// Used to gate `traceId`, which the broker parses as a UUID and silently drops
/// when it is not one — checking here turns that into a visible client-side
/// rejection instead.
pub fn is_valid_uuid(s: &str) -> bool {
    let b = s.as_bytes();
    if b.len() != 36 {
        return false;
    }
    for (i, c) in b.iter().enumerate() {
        match i {
            8 | 13 | 18 | 23 => {
                if *c != b'-' {
                    return false;
                }
            }
            _ => {
                if !c.is_ascii_hexdigit() {
                    return false;
                }
            }
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn is_canonically_formatted_and_versioned() {
        let s = uuidv7();
        assert_eq!(s.len(), 36);
        assert!(is_valid_uuid(&s), "{s}");
        // version nibble
        assert_eq!(s.as_bytes()[14], b'7', "{s}");
        // variant nibble is one of 8/9/a/b
        assert!(matches!(s.as_bytes()[19], b'8' | b'9' | b'a' | b'b'), "{s}");
    }

    #[test]
    fn a_tight_loop_produces_distinct_ordered_ids() {
        let ids: Vec<String> = (0..5_000).map(|_| uuidv7()).collect();
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len(), "uuidv7 collided within one process");

        // The intra-millisecond counter is what keeps these sorted; without it
        // ids minted in the same millisecond would order randomly.
        let mut sorted = ids.clone();
        sorted.sort();
        assert_eq!(sorted, ids, "uuidv7 is not monotonic within a millisecond");
    }

    #[test]
    fn validator_rejects_near_misses() {
        assert!(is_valid_uuid("0190aaaa-0000-7000-8000-000000000001"));
        assert!(!is_valid_uuid(""));
        assert!(!is_valid_uuid("not-a-uuid"));
        // right length, wrong separators
        assert!(!is_valid_uuid("0190aaaa00000-7000-8000-00000000001"));
        // right shape, non-hex
        assert!(!is_valid_uuid("0190aaaa-0000-7000-8000-00000000000z"));
        // one char short
        assert!(!is_valid_uuid("0190aaaa-0000-7000-8000-00000000001"));
    }

    #[test]
    fn formatting_groups_bytes_8_4_4_4_12() {
        let s = format_uuid(&[0xab; 16]);
        assert_eq!(s, "abababab-abab-abab-abab-abababababab");
        assert_eq!(s.len(), 36);
        assert_eq!(s.matches('-').count(), 4);

        // Byte order is preserved left to right, not reversed per group.
        let mut b = [0u8; 16];
        for (i, slot) in b.iter_mut().enumerate() {
            *slot = i as u8;
        }
        assert_eq!(format_uuid(&b), "00010203-0405-0607-0809-0a0b0c0d0e0f");
    }
}
