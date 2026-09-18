//! Synthetic Command / Outcome bodies (PLAN_RAFT.md §9.2, §5.4).
//!
//! A command is one forwarded push batch: `batch` messages of `payload` bytes
//! each, with a 16-byte dedup hash per message (D10), a 16-byte request id
//! minted by the receiver (D6) and the remaining request budget (§9.2
//! "every request carries its remaining budget").
//!
//! An outcome is the compact answer of §5.4: request id, commit index and one
//! (status, offset) pair per message. Payload bytes are never in an outcome.

/// `req_id[16] | deadline_us u64 | tenant | queue | partition | count u16 |
///  count * (hash[16] | plen u32 | payload)`
pub fn encode_command(
    req_id: &[u8; 16],
    deadline_us: u64,
    tenant: &str,
    queue: &str,
    partition: &str,
    batch: usize,
    payload: &[u8],
) -> Vec<u8> {
    let mut b = Vec::with_capacity(16 + 8 + 32 + 2 + batch * (16 + 4 + payload.len()));
    b.extend_from_slice(req_id);
    b.extend_from_slice(&deadline_us.to_le_bytes());
    for s in [tenant, queue, partition] {
        assert!(s.len() < 256);
        b.push(s.len() as u8);
        b.extend_from_slice(s.as_bytes());
    }
    b.extend_from_slice(&(batch as u16).to_le_bytes());
    for i in 0..batch {
        let mut hash = [0u8; 16];
        hash[..8].copy_from_slice(&(i as u64).to_le_bytes());
        b.extend_from_slice(&hash);
        b.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        b.extend_from_slice(payload);
    }
    b
}

/// Offset of the request id inside a command body (patched per command so the
/// 2.8 KB template is built once per run).
pub const REQ_ID_OFF: usize = 0;

pub struct Parsed {
    pub req_id: [u8; 16],
    pub count: usize,
    pub payload_bytes: usize,
}

/// Decodes and validates a command body the way the leader's receiver would:
/// every field is walked, so the parse cost is in the measurement.
pub fn decode_command(b: &[u8]) -> Result<Parsed, &'static str> {
    let mut p = 0usize;
    if b.len() < 26 {
        return Err("short command");
    }
    let mut req_id = [0u8; 16];
    req_id.copy_from_slice(&b[0..16]);
    p += 16;
    p += 8; // deadline
    for _ in 0..3 {
        if p >= b.len() {
            return Err("short str");
        }
        let n = b[p] as usize;
        p += 1 + n;
        if p > b.len() {
            return Err("short str body");
        }
    }
    if p + 2 > b.len() {
        return Err("short count");
    }
    let count = u16::from_le_bytes([b[p], b[p + 1]]) as usize;
    p += 2;
    let mut payload_bytes = 0usize;
    for _ in 0..count {
        if p + 20 > b.len() {
            return Err("short msg header");
        }
        // hash[16] would be looked up in the dedup index here
        p += 16;
        let plen = u32::from_le_bytes([b[p], b[p + 1], b[p + 2], b[p + 3]]) as usize;
        p += 4;
        if p + plen > b.len() {
            return Err("short payload");
        }
        p += plen;
        payload_bytes += plen;
    }
    if p != b.len() {
        return Err("trailing bytes");
    }
    Ok(Parsed {
        req_id,
        count,
        payload_bytes,
    })
}

/// `req_id[16] | commit_index u64 | count u16 | count * (status u8 | offset u64)`
pub fn encode_outcome(
    req_id: &[u8; 16],
    commit_index: u64,
    count: usize,
    base_offset: u64,
) -> Vec<u8> {
    let mut b = Vec::with_capacity(16 + 8 + 2 + count * 9);
    b.extend_from_slice(req_id);
    b.extend_from_slice(&commit_index.to_le_bytes());
    b.extend_from_slice(&(count as u16).to_le_bytes());
    for i in 0..count {
        b.push(1u8); // "inserted"
        b.extend_from_slice(&(base_offset + i as u64).to_le_bytes());
    }
    b
}

pub fn outcome_req_id(b: &[u8]) -> Option<[u8; 16]> {
    if b.len() < 16 {
        return None;
    }
    let mut id = [0u8; 16];
    id.copy_from_slice(&b[0..16]);
    Some(id)
}

/// Leader-side counters, answered to the receiver so one process reports both.
#[derive(Clone, Copy, Debug, Default)]
pub struct Stats {
    pub user_us: u64,
    pub sys_us: u64,
    pub rss_bytes: u64,
    pub rx_bytes: u64,
    pub tx_bytes: u64,
    pub conns: u64,
    pub open: u64,
    pub cmds: u64,
    pub msgs: u64,
}

impl Stats {
    pub fn encode(&self, req_id: &[u8; 16]) -> Vec<u8> {
        let mut b = Vec::with_capacity(16 + 8 * 9);
        b.extend_from_slice(req_id);
        for v in [
            self.user_us,
            self.sys_us,
            self.rss_bytes,
            self.rx_bytes,
            self.tx_bytes,
            self.conns,
            self.open,
            self.cmds,
            self.msgs,
        ] {
            b.extend_from_slice(&v.to_le_bytes());
        }
        b
    }

    pub fn decode(b: &[u8]) -> Option<Stats> {
        if b.len() < 16 + 72 {
            return None;
        }
        let g = |i: usize| -> u64 {
            let s = 16 + i * 8;
            u64::from_le_bytes(b[s..s + 8].try_into().unwrap())
        };
        Some(Stats {
            user_us: g(0),
            sys_us: g(1),
            rss_bytes: g(2),
            rx_bytes: g(3),
            tx_bytes: g(4),
            conns: g(5),
            open: g(6),
            cmds: g(7),
            msgs: g(8),
        })
    }
}
