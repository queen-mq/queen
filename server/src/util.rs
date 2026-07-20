use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
