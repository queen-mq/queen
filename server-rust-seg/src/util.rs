use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

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
