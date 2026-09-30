//! A measurement, not a gate: the RAM table containers — the B-tree every
//! keyspace was served from before (one lock per keyspace) and the pid-indexed
//! dense table (64 lock stripes) — on the shapes of the keyspaces at 1M and
//! 10M rows. Run with:
//!
//! `cargo test --release --lib rsm::store::ram_bench -- --ignored --nocapture`
//!
//! Per container and shape it prints: the bulk load, the heap the table holds
//! (the system allocator's count, per row: the values are 40-byte stored
//! values, 64 bytes each with their `Arc` header), a point read decoded under the lock
//! (`with`), the same read handing out a shared value (`get`, the old path's
//! `Arc` clone), 8 threads of point reads at once, an overwrite of a present
//! row (single writer), the same writer with 8 readers running, 4 writers at
//! once on their own pids (the sharded apply), a prefix scan of one pid (or 16
//! rows of a queue), and a full scan.

// A measurement: it times the tables (a clock) and takes a filter from the
// environment; it runs in a test binary only, never in apply (I2).
#![allow(clippy::disallowed_methods)]

use std::ops::Bound;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

use super::ram::{Layout, RamKey, RamTable, RamVal, ScanBuf, StripeBy};

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

#[derive(Clone, Copy)]
enum Shape {
    /// `partitions`: one row per pid, the key is the pid.
    Partitions,
    /// `counters`, partition scope: five rows per pid behind a scope byte.
    Counters,
    /// `cursors`: one row per pid, a 16-byte group name after the pid.
    Cursors,
    /// `partitions_by_key`: one queue of a tenant (a 36-byte id), a partition
    /// name per row.
    ByKey,
    /// `pending`: one (tenant, queue, group), a pid per row.
    Pending,
}

const TENANT: &str = "6f1c2a9e-4b7d-4e0a-9c55-3d2b8e7f1a04";

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::Partitions => "partitions",
            Shape::Counters => "counters",
            Shape::Cursors => "cursors",
            Shape::ByKey => "by_key",
            Shape::Pending => "pending",
        }
    }
    fn rows_per_pid(self) -> u64 {
        match self {
            Shape::Counters => 5,
            _ => 1,
        }
    }
    fn lead(self) -> &'static [u8] {
        match self {
            Shape::Counters => &[0],
            _ => &[],
        }
    }
    fn key(self, pid: u64, i: u64, out: &mut Vec<u8>) {
        out.clear();
        match self {
            Shape::ByKey => {
                super::keys::push_name(out, TENANT);
                super::keys::push_name(out, "orders");
                super::keys::push_name(out, &format!("p-{pid:09}"));
                return;
            }
            Shape::Pending => {
                super::keys::push_name(out, TENANT);
                super::keys::push_name(out, "orders");
                super::keys::push_name(out, "workers");
                out.extend_from_slice(&pid.to_be_bytes());
                return;
            }
            _ => {}
        }
        out.extend_from_slice(self.lead());
        out.extend_from_slice(&pid.to_be_bytes());
        match self {
            Shape::Counters => out.extend_from_slice(&(i as u16).to_be_bytes()),
            Shape::Cursors => out.extend_from_slice(b"__QUEUE_MODE__\x00\x00"),
            _ => {}
        }
    }
    /// A scan's `(from, prefix)`: one pid's rows for a pid-keyed shape; for a
    /// name-keyed one, the queue's rows from this pid's on (the scan takes
    /// 16 of them).
    fn scan(self, pid: u64, from: &mut Vec<u8>, prefix: &mut Vec<u8>) {
        prefix.clear();
        match self {
            Shape::ByKey | Shape::Pending => {
                super::keys::push_name(prefix, TENANT);
                super::keys::push_name(prefix, "orders");
                if matches!(self, Shape::Pending) {
                    super::keys::push_name(prefix, "workers");
                }
                self.key(pid, 0, from);
            }
            _ => {
                prefix.extend_from_slice(self.lead());
                prefix.extend_from_slice(&pid.to_be_bytes());
                from.clear();
                from.extend_from_slice(prefix);
            }
        }
    }
}

/// Heap bytes in use (the system allocator's own count: this test binary
/// runs on it), or 0 where it cannot be read.
fn heap_in_use() -> u64 {
    #[cfg(target_os = "macos")]
    {
        let mut st = libc::malloc_statistics_t {
            blocks_in_use: 0,
            size_in_use: 0,
            max_size_in_use: 0,
            size_allocated: 0,
        };
        // SAFETY: a null zone asks for every zone's statistics; `st` is a
        // valid out-parameter.
        unsafe { libc::malloc_zone_statistics(std::ptr::null_mut(), &mut st) };
        st.size_in_use as u64
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        // SAFETY: no arguments, returns a plain struct.
        let m = unsafe { libc::mallinfo2() };
        (m.uordblks + m.hblkhd) as u64
    }
    #[cfg(not(any(target_os = "macos", all(target_os = "linux", target_env = "gnu"))))]
    {
        0
    }
}

const VAL: usize = 40;

fn build(l: Layout, shape: Shape, rows: u64) -> (RamTable, f64) {
    let pids = rows / shape.rows_per_pid();
    let mut v: Vec<(RamKey, RamVal)> = Vec::with_capacity(rows as usize);
    let mut k = Vec::new();
    for p in 0..pids {
        for i in 0..shape.rows_per_pid() {
            shape.key(p, i, &mut k);
            let val: RamVal = Arc::from(&[(p as u8) ^ (i as u8); VAL][..]);
            v.push((RamKey::from(&k[..]), val));
        }
    }
    let t0 = Instant::now();
    let t = RamTable::load_with_layout(l, v);
    (t, t0.elapsed().as_secs_f64())
}

fn ns_per(t0: Instant, n: u64) -> f64 {
    t0.elapsed().as_nanos() as f64 / n as f64
}

fn measure(l: Layout, label: &str, shape: Shape, rows: u64) {
    let pids = rows / shape.rows_per_pid();
    let before = heap_in_use();
    let (t, load_s) = build(l, shape, rows);
    let after = heap_in_use();
    let mem_b = after.saturating_sub(before) as f64 / rows as f64;
    let ops: u64 = 2_000_000;
    let mut r = Rng(0x9E37_79B9_7F4A_7C15);
    let mut k = Vec::with_capacity(32);

    // Point reads, decoded under the lock.
    let t0 = Instant::now();
    let mut sum = 0usize;
    for _ in 0..ops {
        let p = r.next() % pids;
        shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
        sum += t.with(&k, |v| v[0] as usize).unwrap_or(0);
    }
    let with_ns = ns_per(t0, ops);

    // Point reads handing out the shared value (the old read path).
    let t0 = Instant::now();
    for _ in 0..ops {
        let p = r.next() % pids;
        shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
        sum += t.get(&k).map_or(0, |v| v.len());
    }
    let get_ns = ns_per(t0, ops);

    // 8 readers at once.
    let threads = 8u64;
    let per = ops / 2;
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        for i in 0..threads {
            let t = &t;
            sc.spawn(move || {
                let mut r = Rng(0x1234_5678 + i);
                let mut k = Vec::with_capacity(32);
                let mut s = 0usize;
                for _ in 0..per {
                    let p = r.next() % pids;
                    shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
                    s += t.with(&k, |v| v[0] as usize).unwrap_or(0);
                }
                s
            });
        }
    });
    let par_mops = (threads * per) as f64 / t0.elapsed().as_secs_f64() / 1e6;

    // Overwrites of present rows, one writer (the first write of a row since
    // the "checkpoint" also enters the dirty set).
    let val = [7u8; VAL - 8];
    let trailer = [1u8; 8];
    let t0 = Instant::now();
    for _ in 0..ops {
        let p = r.next() % pids;
        shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
        drop(t.put(&k, &val, &trailer));
    }
    let put_ns = ns_per(t0, ops);

    // The same writer with 8 readers running.
    let stop = AtomicBool::new(false);
    let mut put_busy_ns = 0.0;
    std::thread::scope(|sc| {
        for i in 0..threads {
            let (t, stop) = (&t, &stop);
            sc.spawn(move || {
                let mut r = Rng(0xABCD + i);
                let mut k = Vec::with_capacity(32);
                let mut s = 0usize;
                while !stop.load(Ordering::Relaxed) {
                    let p = r.next() % pids;
                    shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
                    s += t.with(&k, |v| v[0] as usize).unwrap_or(0);
                }
                s
            });
        }
        let t0 = Instant::now();
        let n = ops / 2;
        for _ in 0..n {
            let p = r.next() % pids;
            shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
            drop(t.put(&k, &val, &trailer));
        }
        put_busy_ns = ns_per(t0, n);
        stop.store(true, Ordering::Relaxed);
    });

    // 4 writers at once, each on its own pids (`pid % 4`): the sharded
    // apply's shape.
    let writers = 4u64;
    let per_w = ops / 4;
    let t0 = Instant::now();
    std::thread::scope(|sc| {
        for i in 0..writers {
            let t = &t;
            sc.spawn(move || {
                let mut r = Rng(0x7777 + i);
                let mut k = Vec::with_capacity(32);
                for _ in 0..per_w {
                    let p = (r.next() % (pids / writers)) * writers + i;
                    shape.key(p, r.next() % shape.rows_per_pid(), &mut k);
                    drop(t.put(&k, &val, &trailer));
                }
            });
        }
    });
    let wr4_mops = (writers * per_w) as f64 / t0.elapsed().as_secs_f64() / 1e6;

    // A prefix scan: one pid's rows, or 16 rows of the queue.
    let mut buf = ScanBuf::new();
    let n_scan = ops / 2;
    let mut end = Vec::new();
    let mut prefix = Vec::new();
    let take = match shape {
        Shape::ByKey | Shape::Pending => 16,
        _ => 256,
    };
    let t0 = Instant::now();
    for _ in 0..n_scan {
        let p = r.next() % pids;
        shape.scan(p, &mut k, &mut prefix);
        end.clear();
        end.extend_from_slice(&super::prefix_end(&prefix).unwrap_or_default());
        buf.clear();
        t.copy_range(
            Bound::Included(&k),
            if end.is_empty() {
                Bound::Unbounded
            } else {
                Bound::Excluded(&end)
            },
            false,
            take,
            &mut buf,
        );
        sum += buf.len();
    }
    let scan1_ns = ns_per(t0, n_scan);

    // A full scan, in the chunks a long scan reaches (1024 rows). The best of
    // `RAM_BENCH_REPEAT` passes (default 1): the host is shared.
    let repeat: usize = std::env::var("RAM_BENCH_REPEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let mut full_mrows: f64 = 0.0;
    for _ in 0..repeat.max(1) {
        let t0 = Instant::now();
        let mut seen = 0u64;
        let mut last: Vec<u8> = Vec::new();
        let mut first = true;
        loop {
            buf.clear();
            let lo = if first {
                Bound::Unbounded
            } else {
                Bound::Excluded(&last[..])
            };
            t.copy_range(lo, Bound::Unbounded, false, 1024, &mut buf);
            seen += buf.len() as u64;
            if buf.len() < 1024 {
                break;
            }
            last.clear();
            last.extend_from_slice(buf.key(buf.len() - 1));
            first = false;
        }
        assert_eq!(seen, rows);
        full_mrows = full_mrows.max(seen as f64 / t0.elapsed().as_secs_f64() / 1e6);
    }

    println!(
        "{:<10} {:>4}M {:<6} load {:>6.2}s  mem {:>5.0} B/row  with {:>6.1} ns  get(arc) {:>6.1} ns  \
         8thr {:>6.1} Mops/s  put {:>6.1} ns  put+8rd {:>6.1} ns  4wr {:>5.2} Mops/s  scan1 {:>6.1} ns  \
         full {:>5.1} Mrows/s  [{}]",
        shape.name(),
        rows / 1_000_000,
        label,
        load_s,
        mem_b,
        with_ns,
        get_ns,
        par_mops,
        put_ns,
        put_busy_ns,
        wr4_mops,
        scan1_ns,
        full_mrows,
        sum % 7
    );
    drop(t);
}

#[test]
#[ignore = "measurement, not a gate; run with --release --ignored --nocapture"]
fn ram_tables_old_vs_new() {
    let dense = |lead: &'static [u8]| Layout::Dense {
        lead,
        empty_fast_path: false,
    };
    let only: Option<String> = std::env::var("RAM_BENCH_ONLY").ok();
    let sizes: Vec<u64> = match std::env::var("RAM_BENCH_ROWS") {
        Ok(v) => v.split(',').filter_map(|x| x.trim().parse().ok()).collect(),
        Err(_) => vec![1_000_000, 10_000_000],
    };
    for rows in sizes {
        for shape in [
            Shape::Partitions,
            Shape::Cursors,
            Shape::Counters,
            Shape::ByKey,
            Shape::Pending,
        ] {
            if only
                .as_deref()
                .is_some_and(|o| !o.split(',').any(|x| x == shape.name()))
            {
                continue;
            }
            let new = match shape {
                Shape::ByKey => (
                    Layout::Prefixed {
                        names: 2,
                        stripe_by: StripeBy::SuffixHash,
                    },
                    "prefix",
                ),
                Shape::Pending => (
                    Layout::Prefixed {
                        names: 3,
                        stripe_by: StripeBy::SuffixPid,
                    },
                    "prefix",
                ),
                _ => (dense(shape.lead()), "dense"),
            };
            measure(new.0, new.1, shape, rows);
            measure(Layout::Tree, "btree", shape, rows);
        }
    }
}
