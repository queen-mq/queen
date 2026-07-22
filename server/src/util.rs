use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

// FNV-1a hasher for the hot-path HashMaps/HashSets keyed by short strings
// (queue/partition/txn). std's default SipHash showed up at ~3% of broker CPU
// under load; these maps hold request-scoped, non-adversarial keys, so a fast
// non-DoS-resistant hash is appropriate.
pub struct FnvHasher(u64);

impl Default for FnvHasher {
    fn default() -> Self {
        FnvHasher(0xcbf29ce484222325)
    }
}

impl std::hash::Hasher for FnvHasher {
    fn write(&mut self, bytes: &[u8]) {
        let mut h = self.0;
        for b in bytes {
            h ^= *b as u64;
            h = h.wrapping_mul(0x100000001b3);
        }
        self.0 = h;
    }
    fn finish(&self) -> u64 {
        self.0
    }
}

pub type FnvBuild = std::hash::BuildHasherDefault<FnvHasher>;
pub type FnvHashMap<K, V> = std::collections::HashMap<K, V, FnvBuild>;
pub type FnvHashSet<K> = std::collections::HashSet<K, FnvBuild>;

pub fn now_epoch_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

// Parse the SPs' UTC timestamp text ('YYYY-MM-DD"T"HH24:MI:SS[.fraction]"Z"')
// to epoch milliseconds without a date-time dependency. Used to compute
// per-message pop lag (age at delivery) from PopSeg.created_at. Returns None
// on any shape mismatch — lag is best-effort instrumentation, never an error.
pub fn parse_iso_ms(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 19 || b[4] != b'-' || b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let num = |r: std::ops::Range<usize>| -> Option<i64> { s.get(r)?.parse::<i64>().ok() };
    let (y, m, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    let (hh, mm, ss) = (num(11..13)?, num(14..16)?, num(17..19)?);
    // Fractional seconds: any number of digits after '.', truncated to ms.
    let mut frac_ms: i64 = 0;
    if b.len() > 19 && b[19] == b'.' {
        let digits: String = s[20..].chars().take_while(|c| c.is_ascii_digit()).collect();
        if !digits.is_empty() {
            let v = digits.parse::<i64>().ok()?;
            let scale = 10_i64.pow(digits.len() as u32);
            frac_ms = v * 1000 / scale;
        }
    }
    // days_from_civil (Howard Hinnant): days since 1970-01-01 for a proleptic
    // Gregorian date.
    let y_adj = if m <= 2 { y - 1 } else { y };
    let era = if y_adj >= 0 { y_adj } else { y_adj - 399 } / 400;
    let yoe = y_adj - era * 400; // [0, 399]
    let mp = (m + 9) % 12; // Mar=0 .. Feb=11
    let doy = (153 * mp + 2) / 5 + d - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    let days = era * 146097 + doe - 719468;
    Some(((days * 24 + hh) * 60 + mm) * 60_000 + ss * 1000 + frac_ms)
}

// Log-engine txn fingerprint (doc 18 §3): xxh3_128 of the txn id's utf8 bytes,
// serialized big-endian. The broker is the ONLY place hashing happens — SQL
// stores and compares the 16-byte bytea verbatim (queen.log_txns.hashes,
// 16*msg_count frame-order stride). 128 bits retires the 64-bit collision
// concern for ack-by-txn resolution and dedup probes.
// Wired by fusion/ack in the log-engine slice; tests exercise it meanwhile.
#[allow(dead_code)]
pub fn txn_hash128(txn: &str) -> [u8; 16] {
    xxhash_rust::xxh3::xxh3_128(txn.as_bytes()).to_be_bytes()
}

// UUIDv7 (time-ordered) as raw bytes — mirrors the C++/Go generators.
static LAST_MS: AtomicU64 = AtomicU64::new(0);
static SEQ: AtomicU64 = AtomicU64::new(0);

pub fn uuidv7_bytes() -> [u8; 16] {
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
    // drifts across refactors/crate bumps — hashes are persisted in log_txns, so
    // a silent change would orphan every stored fingerprint. Values are snapshots
    // of this implementation's output, not external reference vectors.
    #[test]
    fn txn_hash128_stable_vectors() {
        assert_eq!(
            txn_hash128(""),
            [
                0x99, 0xaa, 0x06, 0xd3, 0x01, 0x47, 0x98, 0xd8, 0x60, 0x01, 0xc3, 0x24, 0x46,
                0x8d, 0x49, 0x7f
            ]
        );
        assert_eq!(
            txn_hash128("txn-0001"),
            [
                0xe6, 0xdb, 0x1a, 0x37, 0x61, 0x71, 0xf0, 0x85, 0x29, 0x9a, 0x00, 0x09, 0x43,
                0x06, 0x5f, 0x9c
            ]
        );
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
