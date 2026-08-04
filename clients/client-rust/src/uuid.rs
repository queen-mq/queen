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
    use std::sync::Mutex;

    /// The generator's timestamp and counter are process-global atomics, so a
    /// test that asserts *ordering* cannot run while another test is minting ids
    /// from other threads: the interleaving genuinely produces an id lower than
    /// the one before it (see `concurrent_generation_never_repeats_an_id`).
    /// Cargo runs tests in parallel within one process, so the two take turns.
    static GENERATOR: Mutex<()> = Mutex::new(());

    fn exclusive_generator() -> std::sync::MutexGuard<'static, ()> {
        GENERATOR.lock().unwrap_or_else(|e| e.into_inner())
    }

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
        let _generator = exclusive_generator();
        let ids: Vec<String> = (0..5_000).map(|_| uuidv7()).collect();
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(
            unique.len(),
            ids.len(),
            "uuidv7 collided within one process"
        );

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

    // The first six bytes are a big-endian millisecond timestamp: that, and
    // nothing else, is what makes these ids land in index order. A byte-order
    // slip would still produce a well-formed UUID and still pass every other
    // test in this file, while scattering every insert across the index.
    #[test]
    fn the_raw_bytes_carry_the_version_the_variant_and_a_real_timestamp() {
        let b = uuidv7_bytes();
        assert_eq!(b[6] & 0xf0, 0x70, "version nibble is not 7: {b:?}");
        assert_eq!(b[8] & 0xc0, 0x80, "not the RFC 4122 variant: {b:?}");

        let ms = u64::from_be_bytes([0, 0, b[0], b[1], b[2], b[3], b[4], b[5]]);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_millis() as u64;
        assert!(
            now.abs_diff(ms) < 5_000,
            "timestamp prefix reads as {ms}ms, but now is {now}ms — the 48-bit big-endian layout \
             is what keeps these ids sorted"
        );
    }

    // The id is the transaction id, and the transaction id is the broker's
    // dedup key: two threads minting the same one inside the dedup window means
    // one of two distinct messages is dropped as a duplicate and never
    // delivered. Global *ordering* across threads is deliberately not asserted
    // — the timestamp and the counter are separate atomics, so an interleaving
    // can hand out a lower id after a higher one, and pinning that would be a
    // flaky test of a property this client does not promise.
    #[test]
    fn concurrent_generation_never_repeats_an_id() {
        const THREADS: usize = 8;
        const PER_THREAD: usize = 2_000;

        let _generator = exclusive_generator();

        let workers: Vec<_> = (0..THREADS)
            .map(|_| std::thread::spawn(|| (0..PER_THREAD).map(|_| uuidv7()).collect::<Vec<_>>()))
            .collect();

        let mut all = Vec::with_capacity(THREADS * PER_THREAD);
        for w in workers {
            all.extend(w.join().expect("a generator thread panicked"));
        }

        let unique: HashSet<&String> = all.iter().collect();
        assert_eq!(
            unique.len(),
            all.len(),
            "{} of {} ids collided across threads",
            all.len() - unique.len(),
            all.len()
        );
        assert!(
            all.iter()
                .all(|s| is_valid_uuid(s) && s.as_bytes()[14] == b'7'),
            "the race left a malformed id behind"
        );
    }

    #[test]
    fn the_validator_accepts_every_form_the_broker_parses() {
        // Uppercase hex is a valid UUID; rejecting it would refuse a perfectly
        // good trace id.
        assert!(is_valid_uuid("0190AAAA-0000-7000-8000-00000000000F"));
        assert!(is_valid_uuid("00000000-0000-0000-0000-000000000000"));
        // ...and the decorated forms are not valid, so they are refused here
        // rather than dropped silently by the broker.
        assert!(!is_valid_uuid("{0190aaaa-0000-7000-8000-000000000001}"));
        assert!(!is_valid_uuid(
            "urn:uuid:0190aaaa-0000-7000-8000-000000000001"
        ));
        assert!(!is_valid_uuid("0190aaaa00007000800000000000000f"));
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
