use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

pub fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Parse an ISO 8601 / RFC 3339 timestamp ('YYYY-MM-DD"T"HH:MM:SS[.fraction]
// [Z | ±HH:MM | ±HHMM | ±HH]', 't' or a space accepted for the 'T') to epoch
// milliseconds, UTC, without a date-time dependency. No designator means UTC.
// The SPs' text always ends in "Z"; a client's timestamp (subscriptionFrom, a
// seek, `since`) may carry its local offset, which is applied: 10:25:00+02:00
// is 08:25:00Z (ignoring it put a Go client's `time.Now()` two hours in the
// future). The fraction is truncated to ms. Returns None on any shape mismatch.
pub fn parse_iso_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19
        || b[4] != b'-'
        || b[7] != b'-'
        || !matches!(b[10], b'T' | b't' | b' ')
        || b[13] != b':'
        || b[16] != b':'
    {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> { s.get(r)?.parse::<i64>().ok() };
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hh, mm, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
    let mut rest = &b[19..];
    // Fractional seconds: any number of digits after '.', truncated to ms.
    let mut frac_ms: i64 = 0;
    if let Some((b'.', tail)) = rest.split_first() {
        let n = tail.iter().take_while(|c| c.is_ascii_digit()).count();
        for i in 0..3 {
            frac_ms = frac_ms * 10 + tail[..n].get(i).map_or(0, |c| i64::from(c - b'0'));
        }
        rest = &tail[n..];
    }
    // The designator: nothing or "Z" is UTC; an offset is taken back off.
    let two = |p: &[u8]| -> Option<i64> {
        match p {
            [h, l] if h.is_ascii_digit() && l.is_ascii_digit() => {
                Some(i64::from(h - b'0') * 10 + i64::from(l - b'0'))
            }
            _ => None,
        }
    };
    let offset_min = match rest {
        [] | [b'Z' | b'z'] => 0,
        [sign @ (b'+' | b'-'), off @ ..] => {
            let (oh, om) = match off {
                [_, _] => (two(off)?, 0),
                [_, _, _, _] => (two(&off[..2])?, two(&off[2..])?),
                [_, _, b':', _, _] => (two(&off[..2])?, two(&off[3..])?),
                _ => return None,
            };
            if oh > 23 || om > 59 {
                return None;
            }
            if *sign == b'-' {
                -(oh * 60 + om)
            } else {
                oh * 60 + om
            }
        }
        _ => return None,
    };
    // days_from_civil (Howard Hinnant): days since 1970-01-01 for a proleptic
    // Gregorian date.
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days = era * 146097 + doe - 719468;
    let local_ms = ((days * 24 + hh) * 60 + mm) * 60_000 + ss * 1000 + frac_ms;
    Some(local_ms - offset_min * 60_000)
}

// Transaction-id fingerprint (doc 18 §3): xxh3_128 of the txn id's utf8 bytes,
// serialized big-endian, stored and compared as 16 bytes. 128 bits retires the
// 64-bit collision concern for ack-by-txn resolution and dedup probes.
// Wired by fusion/ack in the log-engine slice; tests exercise it meanwhile.
#[allow(dead_code)]
pub fn txn_hash128(txn: &str) -> [u8; 16] {
    xxhash_rust::xxh3::xxh3_128(txn.as_bytes()).to_be_bytes()
}

// UUIDv7 (time-ordered) as raw bytes — mirrors the C++/Go generators.
static LAST_MS: AtomicU64 = AtomicU64::new(0);
static SEQ: AtomicU64 = AtomicU64::new(0);

pub fn json_escape_into(out: &mut String, s: &str) {
    // Byte-scan fast path: every byte needing an escape ('"', '\\', <0x20) is
    // ASCII, and multi-byte UTF-8 units are all >= 0x80, so splitting the string
    // only at escape bytes always lands on char boundaries. Clean runs (the
    // overwhelmingly common case — UUIDs, txn ids, ISO timestamps) are appended
    // wholesale instead of char-by-char.
    let b = s.as_bytes();
    let mut start = 0;
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'"' || c == b'\\' || c < 0x20 {
            if start < i {
                out.push_str(&s[start..i]);
            }
            match c {
                b'"' => out.push_str("\\\""),
                b'\\' => out.push_str("\\\\"),
                b'\n' => out.push_str("\\n"),
                b'\r' => out.push_str("\\r"),
                b'\t' => out.push_str("\\t"),
                _ => {
                    out.push_str("\\u00");
                    out.push(char::from(b"0123456789abcdef"[(c >> 4) as usize]));
                    out.push(char::from(b"0123456789abcdef"[(c & 0x0f) as usize]));
                }
            }
            start = i + 1;
        }
        i += 1;
    }
    if start < b.len() {
        out.push_str(&s[start..]);
    }
}

#[cfg(test)]
thread_local! {
    /// TEST ONLY: while `Some(n)`, [`uuidv7_bytes`] on this thread returns a
    /// counter-derived id (see [`deterministic_uuids`]).
    static TEST_UUIDS: std::cell::Cell<Option<u64>> = const { std::cell::Cell::new(None) };
}

/// TEST ONLY: until the guard drops, [`uuidv7_bytes`] on THIS thread returns
/// ids derived from a counter starting at `seed` — increasing, version and
/// variant bits set — instead of random ones. The KEEP_OVERLAY gate plans one
/// command stream twice (the knob on, then off) and compares the proposed
/// entries byte for byte; the planner mints uuids (partitions, groups, dead
/// letters, leader-step request ids), so both runs must mint the same ones.
#[cfg(test)]
pub fn deterministic_uuids(seed: u64) -> DeterministicUuids {
    TEST_UUIDS.with(|c| c.set(Some(seed)));
    DeterministicUuids
}

/// Restores random uuids on this thread when dropped.
#[cfg(test)]
pub struct DeterministicUuids;

#[cfg(test)]
impl Drop for DeterministicUuids {
    fn drop(&mut self) {
        TEST_UUIDS.with(|c| c.set(None));
    }
}

pub fn uuidv7_bytes() -> [u8; 16] {
    #[cfg(test)]
    if let Some(n) = TEST_UUIDS.with(|c| {
        let v = c.get();
        if let Some(x) = v {
            c.set(Some(x.wrapping_add(1)));
        }
        v
    }) {
        let mut b = [0u8; 16];
        b[0..8].copy_from_slice(&0x0190_0000_0000_7000u64.to_be_bytes());
        b[8..16].copy_from_slice(&(n & 0x3fff_ffff_ffff_ffff).to_be_bytes());
        b[8] |= 0x80;
        return b;
    }
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
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
    b
}

#[cfg(test)]
mod tests {
    use super::*;

    // Pinned vectors: these assert OUR serialization (xxh3_128, big-endian) never
    // drifts across refactors/crate bumps — hashes are persisted, so
    // a silent change would orphan every stored fingerprint. Values are snapshots
    // of this implementation's output, not external reference vectors.
    #[test]
    fn txn_hash128_stable_vectors() {
        assert_eq!(
            txn_hash128(""),
            [
                0x99, 0xaa, 0x06, 0xd3, 0x01, 0x47, 0x98, 0xd8, 0x60, 0x01, 0xc3, 0x24, 0x46, 0x8d,
                0x49, 0x7f
            ]
        );
        assert_eq!(
            txn_hash128("txn-0001"),
            [
                0xe6, 0xdb, 0x1a, 0x37, 0x61, 0x71, 0xf0, 0x85, 0x29, 0x9a, 0x00, 0x09, 0x43, 0x06,
                0x5f, 0x9c
            ]
        );
    }

    // Reference values from Python's datetime.fromisoformat.
    #[test]
    fn parse_iso_ms_applies_the_offset() {
        let utc = Some(1_790_238_300_000);
        assert_eq!(parse_iso_ms("2026-09-24T08:25:00Z"), utc);
        assert_eq!(parse_iso_ms("2026-09-24T08:25:00z"), utc);
        assert_eq!(
            parse_iso_ms("2026-09-24T08:25:00"),
            utc,
            "no designator is UTC"
        );
        assert_eq!(parse_iso_ms("2026-09-24T08:25:00+00:00"), utc);
        assert_eq!(parse_iso_ms("2026-09-24T08:25:00-00:00"), utc);
        // What Go's time.Now().Format(time.RFC3339) sends from Rome in summer.
        assert_eq!(parse_iso_ms("2026-09-24T10:25:00+02:00"), utc);
        assert_eq!(parse_iso_ms("2026-09-24T10:25:00+0200"), utc);
        assert_eq!(parse_iso_ms("2026-09-24T10:25:00+02"), utc);
        assert_eq!(parse_iso_ms("2026-09-24T03:55:00-04:30"), utc);
        assert_eq!(parse_iso_ms("2026-09-24t10:25:00+02:00"), utc);
        assert_eq!(parse_iso_ms("2026-09-24 10:25:00+02:00"), utc);
        // The offset crosses the day and the year.
        assert_eq!(
            parse_iso_ms("2026-01-01T01:00:00+02:00"),
            Some(1_767_222_000_000)
        );
        assert_eq!(parse_iso_ms("1970-01-01T00:00:00Z"), Some(0));
    }

    #[test]
    fn parse_iso_ms_truncates_the_fraction_to_ms() {
        assert_eq!(
            parse_iso_ms("2026-09-24T10:25:00.123456+02:00"),
            Some(1_790_238_300_123)
        );
        assert_eq!(
            parse_iso_ms("2026-09-24T08:25:00.5Z"),
            Some(1_790_238_300_500)
        );
        assert_eq!(
            parse_iso_ms("2026-09-24T08:25:00.Z"),
            Some(1_790_238_300_000)
        );
        // More digits than an i64 holds: truncated, not an overflow.
        assert_eq!(
            parse_iso_ms("2026-09-24T08:25:00.12345678901234567890123Z"),
            Some(1_790_238_300_123)
        );
    }

    #[test]
    fn parse_iso_ms_refuses_what_it_cannot_place() {
        for s in [
            "2026-09-24",
            "2026-09-24X08:25:00Z",
            "2026-09-24T08:25:00+2:00",
            "2026-09-24T08:25:00+02:0",
            "2026-09-24T08:25:00+24:00",
            "2026-09-24T08:25:00+02:60",
            "2026-09-24T08:25:00+02:00:00",
            "2026-09-24T08:25:00Zjunk",
            "2026-09-24T08:25:00 UTC",
        ] {
            assert_eq!(parse_iso_ms(s), None, "{s}");
        }
    }

    #[test]
    fn txn_hash128_deterministic_and_distinct() {
        let a = txn_hash128("queue/partition/txn-A");
        let b = txn_hash128("queue/partition/txn-A");
        let c = txn_hash128("queue/partition/txn-B");
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
