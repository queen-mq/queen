//! The synthetic apply stream (PLAN_RAFT.md §6.1, §7.1).
//!
//! One applied entry = one planning cycle's effects. Per entry, at `--batch`
//! messages per push:
//!
//! - `segments` one row per push batch, key (pid, base_offset), value 40 B —
//!   ONLY in `--shape legacy`. The G0 amendment to §6.1 takes this keyspace out
//!   of the store entirely (one immutable `.qidx` per sealed file), so
//!   `--shape ratified` does not write it;
//! - `dedup` one row per MESSAGE, key (pid, 8 random bytes) = D10 option (a)'s
//!   `(pid, hash)`, value (offset, created_at) — ONLY in `--shape ratified`.
//!   These are uniformly random keys and, with a 1 h txns window at 20k msg/s,
//!   the largest keyspace in the store (~72 M rows). This is the keyspace the
//!   first campaign never had;
//! - `seg_loc` the node-local position of that same batch (§6.2, D8);
//! - `partitions` the partition row (last_offset, last_write_at, …);
//! - `pending` one row per subscribed group (§6.1: an Append is O(groups));
//! - `cursors` a claim (pop) and a commit (ack) on a group cursor;
//! - `request_ids` + `request_expiry` for the command's request id (D6), with
//!   the expired ids deleted as the window slides;
//! - `counters` the O(1) stats of D16, kept hot in RAM and written out;
//! - a KV mix (put / delete / compare-and-set / byte-ordered prefix list) at
//!   `--kv-rate` ops/s, with the `kv_expiry` index;
//! - timers: a schedule (timers + timers_due) and a fire (both deleted) at
//!   `--timer-rate` ops/s.
//!
//! Payload bytes go to the append-only segment files, never to the store.

use crate::engines::*;
use crate::hist::Hist;
use crate::seg::Segments;
use std::collections::{HashMap, VecDeque};
use xxhash_rust::xxh3::xxh3_64;

#[derive(Clone)]
pub struct Params {
    pub rate: u64,
    pub batch: u64,
    pub payload: usize,
    pub partitions: u64,
    pub queues: u64,
    pub tenants: u64,
    pub groups: u64,
    pub kv_rate: u64,
    pub kv_keys: u64,
    pub kv_value: usize,
    pub timer_rate: u64,
    pub request_window_s: i64,
    pub seed: u64,
    /// Write `segments` rows into the store (pre-G0 shape).
    pub segments_in_store: bool,
    /// Write one `dedup` row per message (D10 option (a)).
    pub dedup_rows: bool,
    /// Prune dedup rows older than this many seconds (0 = never, for runs too
    /// short to reach the real txns window).
    pub dedup_window_s: i64,
}

pub struct Counts {
    pub entries: u64,
    pub msgs: u64,
    pub kv_puts: u64,
    pub kv_dels: u64,
    pub kv_cas: u64,
    pub kv_lists: u64,
    pub timers_scheduled: u64,
    pub timers_fired: u64,
    pub reqs_expired: u64,
    pub dedup_rows: u64,
    pub dedup_pruned: u64,
    pub store_ops: u64,
    pub logical_bytes: u64,
    pub payload_bytes: u64,
}

pub struct Gen {
    p: Params,
    rng: u64,
    counters: HashMap<u64, i64>,
    reqs: VecDeque<(i64, [u8; 16])>,
    timers: VecDeque<(i64, [u8; 24])>,
    dedup: VecDeque<(i64, [u8; 16])>,
    kv_acc: f64,
    timer_acc: f64,
    kv_per_entry: f64,
    timer_per_entry: f64,
    next_offset: Vec<u64>,
    blob: Vec<u8>,
    hashes: Vec<u64>,
    pub c: Counts,
}

#[inline]
fn next(rng: &mut u64) -> u64 {
    // xorshift64*: deterministic, no dependency.
    let mut x = *rng;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *rng = x;
    x.wrapping_mul(0x2545_F491_4F6C_DD1D)
}

impl Gen {
    pub fn new(p: Params) -> Self {
        let entries_per_s = (p.rate as f64 / p.batch as f64).max(1.0);
        let kv_per_entry = p.kv_rate as f64 / entries_per_s;
        let timer_per_entry = p.timer_rate as f64 / entries_per_s;
        let mut rng = p.seed | 1;
        let mut blob = vec![0u8; (p.batch as usize * p.payload).max(1)];
        for chunk in blob.chunks_mut(8) {
            let v = next(&mut rng).to_le_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&v[..n]);
        }
        let np = p.partitions as usize;
        Self {
            rng,
            counters: HashMap::with_capacity(1 << 16),
            reqs: VecDeque::new(),
            timers: VecDeque::new(),
            dedup: VecDeque::new(),
            kv_acc: 0.0,
            timer_acc: 0.0,
            kv_per_entry,
            timer_per_entry,
            next_offset: vec![0; np],
            blob,
            hashes: Vec::new(),
            c: Counts {
                entries: 0,
                msgs: 0,
                kv_puts: 0,
                kv_dels: 0,
                kv_cas: 0,
                kv_lists: 0,
                timers_scheduled: 0,
                timers_fired: 0,
                reqs_expired: 0,
                dedup_rows: 0,
                dedup_pruned: 0,
                store_ops: 0,
                logical_bytes: 0,
                payload_bytes: 0,
            },
            p,
        }
    }

    fn bump(&mut self, scope: u8, id: u64, counter: u8, by: i64, ops: &mut OpBuf) {
        let ck = ((scope as u64) << 56) | ((id & 0x00ff_ffff_ffff_ffff) << 8) | counter as u64;
        let v = self.counters.entry(ck).or_insert(0);
        *v += by;
        let mut key = [0u8; 10];
        key[0] = scope;
        key[1..9].copy_from_slice(&id.to_be_bytes());
        key[9] = counter;
        ops.put(T_COUNTERS, &key, &v.to_le_bytes());
    }

    /// Builds one entry: appends the payload to the segment files and fills
    /// `ops` with everything the store write transaction must contain.
    pub fn entry(
        &mut self,
        idx: u64,
        now_us: i64,
        ops: &mut OpBuf,
        seg: &mut Segments,
        eng: &dyn Engine,
        reads: &mut Hist,
        lists: &mut Hist,
    ) -> Result<(), String> {
        let ops_before = ops.len() as u64;
        let logical_before = ops.logical_bytes();
        let r = next(&mut self.rng);
        let pid = r % self.p.partitions;
        let tenant = (pid % self.p.tenants) as u32;
        let queue = (pid % self.p.queues) as u32;
        let bucket = (xxh3_64(&pid.to_le_bytes()) % 256) as u16;

        // ---- push: payload to the segment file, index rows to the store ----
        let base = self.next_offset[pid as usize];
        let count = self.p.batch as u32;
        let msg_bytes = self.p.batch as usize * self.p.payload;
        self.hashes.clear();
        for i in 0..self.p.batch as usize {
            let s = i * self.p.payload;
            self.hashes.push(xxh3_64(&self.blob[s..s + self.p.payload]));
        }
        // make the batch unique so nothing dedups by accident
        self.blob[0..8].copy_from_slice(&idx.to_le_bytes());
        let pos = seg
            .append(
                bucket,
                pid,
                base,
                count,
                now_us,
                &self.hashes,
                &self.blob[..msg_bytes],
            )
            .map_err(|e| format!("segment append: {e}"))?;
        self.next_offset[pid as usize] = base + self.p.batch;
        self.c.payload_bytes += msg_bytes as u64;
        self.c.msgs += self.p.batch;

        let mut segkey = [0u8; 16];
        segkey[0..8].copy_from_slice(&pid.to_be_bytes());
        segkey[8..16].copy_from_slice(&base.to_be_bytes());

        let mut segval = [0u8; 40];
        segval[0..8].copy_from_slice(&(base + self.p.batch).to_le_bytes()); // end
        segval[8..12].copy_from_slice(&count.to_le_bytes());
        segval[12..20].copy_from_slice(&now_us.to_le_bytes()); // created_at
        segval[20..24].copy_from_slice(&(msg_bytes as u32).to_le_bytes());
        segval[24..26].copy_from_slice(&bucket.to_le_bytes());
        segval[26..28].copy_from_slice(&0u16.to_le_bytes()); // flags
        segval[28..36].copy_from_slice(&idx.to_le_bytes()); // entry index (torn-entry check)
        if self.p.segments_in_store {
            ops.put(T_SEGMENTS, &segkey, &segval);
        }

        let mut loc = [0u8; 34];
        loc[0..2].copy_from_slice(&pos.bucket.to_le_bytes());
        loc[2..6].copy_from_slice(&pos.file_id.to_le_bytes());
        loc[6..14].copy_from_slice(&pos.offset.to_le_bytes());
        loc[14..18].copy_from_slice(&pos.len.to_le_bytes());
        loc[18..26].copy_from_slice(&pos.hash.to_le_bytes());
        loc[26..34].copy_from_slice(&now_us.to_le_bytes()); // created_at, for retention
        ops.put(T_SEG_LOC, &segkey, &loc);

        // ---- dedup index (D10 option (a)): one row per message, key
        // (pid, hash) with a UNIFORMLY RANDOM hash. This is the keyspace the
        // ratified store is dominated by and the one COW page write
        // amplification is worst on.
        if self.p.dedup_rows {
            for i in 0..self.p.batch {
                let h = next(&mut self.rng);
                let mut dk = [0u8; 16];
                dk[0..8].copy_from_slice(&pid.to_be_bytes());
                dk[8..16].copy_from_slice(&h.to_be_bytes());
                let mut dv = [0u8; 16];
                dv[0..8].copy_from_slice(&(base + i).to_le_bytes());
                dv[8..16].copy_from_slice(&now_us.to_le_bytes());
                ops.put(T_DEDUP, &dk, &dv);
                self.c.dedup_rows += 1;
                if self.p.dedup_window_s > 0 {
                    self.dedup.push_back((now_us, dk));
                }
            }
            if self.p.dedup_window_s > 0 {
                let cutoff = now_us - self.p.dedup_window_s * 1_000_000;
                let mut n = 0;
                while n < 64 {
                    match self.dedup.front() {
                        Some((t, _)) if *t < cutoff => {
                            let (_, k) = self.dedup.pop_front().unwrap();
                            ops.del(T_DEDUP, &k);
                            self.c.dedup_pruned += 1;
                            n += 1;
                        }
                        _ => break,
                    }
                }
            }
        }

        // partition row
        let mut prow = [0u8; 72];
        prow[0..8].copy_from_slice(&(base + self.p.batch).to_le_bytes()); // last_offset
        prow[8..16].copy_from_slice(&now_us.to_le_bytes()); // last_write_at
        prow[16..24].copy_from_slice(&now_us.to_le_bytes()); // last_created_at
        prow[24..32].copy_from_slice(&0u64.to_le_bytes()); // log_start
        prow[32..40].copy_from_slice(&0u64.to_le_bytes()); // txns_start
        prow[40..48].copy_from_slice(&(queue as u64).to_le_bytes());
        prow[48..56].copy_from_slice(&(tenant as u64).to_le_bytes());
        prow[56..64].copy_from_slice(&idx.to_le_bytes());
        ops.put(T_PARTITIONS, &pid.to_be_bytes(), &prow);

        // pending: one row per subscribed group (O(groups) per Append)
        for g in 0..self.p.groups as u32 {
            let mut k = [0u8; 20];
            k[0..4].copy_from_slice(&tenant.to_be_bytes());
            k[4..8].copy_from_slice(&queue.to_be_bytes());
            k[8..12].copy_from_slice(&g.to_be_bytes());
            k[12..20].copy_from_slice(&pid.to_be_bytes());
            ops.put(T_PENDING, &k, &now_us.to_le_bytes());
        }

        self.bump(1, queue as u64, 0, self.p.batch as i64, ops); // queue pushed
        self.bump(2, pid, 0, self.p.batch as i64, ops); // partition pushed
        self.bump(3, tenant as u64, 1, msg_bytes as i64, ops); // tenant retained bytes
        self.bump(1, queue as u64, 2, msg_bytes as i64, ops); // queue retained bytes

        // ---- pop claim + ack on a group cursor ----
        let g = (next(&mut self.rng) % self.p.groups.max(1)) as u32;
        let mut ck = [0u8; 12];
        ck[0..8].copy_from_slice(&pid.to_be_bytes());
        ck[8..12].copy_from_slice(&g.to_be_bytes());
        let mut crow = [0u8; 64];
        crow[0..8].copy_from_slice(&base.to_le_bytes()); // committed
        crow[8..16].copy_from_slice(&(base + self.p.batch).to_le_bytes()); // batch_end
        crow[16..24].copy_from_slice(&(now_us + 30_000_000).to_le_bytes()); // lease_expires_at
        crow[24..32].copy_from_slice(&now_us.to_le_bytes()); // lease_acquired_at
        crow[32..40].copy_from_slice(&r.to_le_bytes()); // worker
        crow[40..48].copy_from_slice(&idx.to_le_bytes());
        ops.put(T_CURSORS, &ck, &crow); // claim
        crow[0..8].copy_from_slice(&(base + self.p.batch).to_le_bytes()); // committed = end
        crow[16..24].copy_from_slice(&0i64.to_le_bytes()); // lease cleared
        ops.put(T_CURSORS, &ck, &crow); // ack
        self.bump(1, queue as u64, 1, self.p.batch as i64, ops); // queue completed
        self.bump(2, pid, 1, self.p.batch as i64, ops);
        if idx % 2 == 0 {
            // the partition drained for that group
            let mut k = [0u8; 20];
            k[0..4].copy_from_slice(&tenant.to_be_bytes());
            k[4..8].copy_from_slice(&queue.to_be_bytes());
            k[8..12].copy_from_slice(&g.to_be_bytes());
            k[12..20].copy_from_slice(&pid.to_be_bytes());
            ops.del(T_PENDING, &k);
        }

        // ---- request id (D6) and the expiry index ----
        let mut rid = [0u8; 16];
        rid[0..8].copy_from_slice(&idx.to_le_bytes());
        rid[8..16].copy_from_slice(&r.to_le_bytes());
        let mut rval = [0u8; 32];
        rval[0..8].copy_from_slice(&now_us.to_le_bytes());
        rval[8..16].copy_from_slice(&(base + self.p.batch).to_le_bytes());
        ops.put(T_REQUEST_IDS, &rid, &rval);
        let mut rek = [0u8; 24];
        rek[0..8].copy_from_slice(&now_us.to_be_bytes());
        rek[8..24].copy_from_slice(&rid);
        ops.put(T_REQUEST_EXPIRY, &rek, &[]);
        self.reqs.push_back((now_us, rid));
        let cutoff = now_us - self.p.request_window_s * 1_000_000;
        while let Some(&(t, id)) = self.reqs.front() {
            if t > cutoff {
                break;
            }
            self.reqs.pop_front();
            ops.del(T_REQUEST_IDS, &id);
            let mut k = [0u8; 24];
            k[0..8].copy_from_slice(&t.to_be_bytes());
            k[8..24].copy_from_slice(&id);
            ops.del(T_REQUEST_EXPIRY, &k);
            self.c.reqs_expired += 1;
        }

        // ---- KV mix ----
        self.kv_acc += self.kv_per_entry;
        while self.kv_acc >= 1.0 {
            self.kv_acc -= 1.0;
            let x = next(&mut self.rng);
            let kv_tenant = (x % self.p.tenants) as u32;
            let ns = ((x >> 8) % 4) as u32;
            let kn = (x >> 16) % self.p.kv_keys;
            let mut k = [0u8; 24];
            k[0..4].copy_from_slice(&kv_tenant.to_be_bytes());
            k[4..8].copy_from_slice(&ns.to_be_bytes());
            // 16 ASCII-ish bytes so byte order is the lexical order clients see
            let s = format!("{kn:016}");
            k[8..24].copy_from_slice(s.as_bytes());
            let pick = (x >> 40) % 100;
            if pick < 5 {
                // prefix list (linearizable read, D15)
                let t0 = std::time::Instant::now();
                let _ = eng.prefix_count(T_KV, &k[0..8], 100)?;
                lists.record(t0.elapsed().as_micros() as u64);
                self.c.kv_lists += 1;
            } else if pick < 20 {
                // delete: read the row to learn its expiry, then remove both
                let t0 = std::time::Instant::now();
                let old = eng.get(T_KV, &k)?;
                reads.record(t0.elapsed().as_micros() as u64);
                if let Some(old) = old {
                    let exp = i64::from_le_bytes(old[8..16].try_into().unwrap());
                    ops.del(T_KV, &k);
                    let mut ek = [0u8; 32];
                    ek[0..8].copy_from_slice(&exp.to_be_bytes());
                    ek[8..32].copy_from_slice(&k);
                    ops.del(T_KV_EXPIRY, &ek);
                    self.c.kv_dels += 1;
                }
            } else {
                let cas = pick < 40;
                let mut version = 1u64;
                if cas {
                    let t0 = std::time::Instant::now();
                    let old = eng.get(T_KV, &k)?;
                    reads.record(t0.elapsed().as_micros() as u64);
                    if let Some(old) = old {
                        version = u64::from_le_bytes(old[0..8].try_into().unwrap()) + 1;
                    }
                    self.c.kv_cas += 1;
                } else {
                    self.c.kv_puts += 1;
                }
                let exp = now_us + 3_600_000_000;
                let mut v = vec![0u8; 16 + self.p.kv_value];
                v[0..8].copy_from_slice(&version.to_le_bytes());
                v[8..16].copy_from_slice(&exp.to_le_bytes());
                v[16..].copy_from_slice(&self.blob[..self.p.kv_value]);
                ops.put(T_KV, &k, &v);
                let mut ek = [0u8; 32];
                ek[0..8].copy_from_slice(&exp.to_be_bytes());
                ek[8..32].copy_from_slice(&k);
                ops.put(T_KV_EXPIRY, &ek, &[]);
                self.bump(3, kv_tenant as u64, 2, 1, ops); // tenant kv rows
            }
        }

        // ---- timers: schedule and fire ----
        self.timer_acc += self.timer_per_entry;
        while self.timer_acc >= 1.0 {
            self.timer_acc -= 1.0;
            let x = next(&mut self.rng);
            let t_tenant = (x % self.p.tenants) as u32;
            let t_queue = ((x >> 8) % self.p.queues) as u32;
            let mut k = [0u8; 24];
            k[0..4].copy_from_slice(&t_tenant.to_be_bytes());
            k[4..8].copy_from_slice(&t_queue.to_be_bytes());
            k[8..24].copy_from_slice(format!("{:016}", x >> 16).as_bytes());
            let visible_at = now_us + 1_000_000 + ((x >> 20) % 30_000_000) as i64;
            let mut row = vec![0u8; 96];
            row[0..8].copy_from_slice(&visible_at.to_le_bytes());
            row[8..16].copy_from_slice(&now_us.to_le_bytes());
            row[16..24].copy_from_slice(&(t_queue as u64).to_le_bytes());
            ops.put(T_TIMERS, &k, &row);
            let mut dk = [0u8; 32];
            dk[0..8].copy_from_slice(&visible_at.to_be_bytes());
            dk[8..32].copy_from_slice(&k);
            ops.put(T_TIMERS_DUE, &dk, &[]);
            self.timers.push_back((visible_at, k));
            self.c.timers_scheduled += 1;
        }
        while let Some(&(t, k)) = self.timers.front() {
            if t > now_us {
                break;
            }
            self.timers.pop_front();
            ops.del(T_TIMERS, &k);
            let mut dk = [0u8; 32];
            dk[0..8].copy_from_slice(&t.to_be_bytes());
            dk[8..32].copy_from_slice(&k);
            ops.del(T_TIMERS_DUE, &dk);
            self.c.timers_fired += 1;
        }

        // ---- meta: the applied index, in the SAME transaction (I11) ----
        let mut m = [0u8; 24];
        m[0..8].copy_from_slice(&idx.to_le_bytes());
        m[8..16].copy_from_slice(&1u64.to_le_bytes()); // term
        m[16..24].copy_from_slice(&now_us.to_le_bytes());
        ops.put(T_META, M_APPLIED, &m);

        self.c.entries += 1;
        // `ops` is an ACCUMULATOR across entries when §11.3's batched commit is
        // on, so count only what this entry added.
        self.c.store_ops += ops.len() as u64 - ops_before;
        self.c.logical_bytes += ops.logical_bytes() - logical_before;
        Ok(())
    }
}
