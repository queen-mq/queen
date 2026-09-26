//! The node-local observability rows of raft mode (PLAN_RAFT.md D17, §14.6).
//!
//! Each struct is one node's local metrics row. A node writes only its OWN
//! rows (its own HTTP clients, its own process) into its `local.db`; a
//! dashboard read gathers the rows of every node and the views in
//! [`super::node_views`] / [`super::queue_views`] re-aggregate them.
//!
//! | row | written by | gathered |
//! |---|---|---|
//! | [`WorkerRow`] | the collector, one per node per flush | yes |
//! | [`SystemRow`] | the collector, one per node per flush | yes |
//! | [`QueueRow`] | the collector, one per node, queue and minute with activity | yes (merged, [`merge_queue_rows`]) |
//! | [`ParkedRow`] | the collector, one per node, queue and minute with parked long-polls | yes |
//! | [`ChurnRow`] | apply, identical on every node | NO: served from the answering node |
//!
//! Times are epoch microseconds (UTC). The pure helpers at the bottom
//! ([`iso_us`], [`parse_ts_us`], [`trunc_us`]) are the only time functions the
//! views use, so they stay free of the crate and testable on their own.

use serde::{Deserialize, Serialize};

/// One row per node per flush (default every 60 s). `at_us` is the flush
/// time truncated to the second; the counters are the deltas since the
/// node's previous flush.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkerRow {
    pub at_us: i64,
    /// The node as the dashboard names it.
    pub hostname: String,
    /// Always 0: one async worker per process.
    pub worker_id: i32,
    pub pid: i32,
    pub push_requests: i64,
    pub push_messages: i64,
    pub pop_requests: i64,
    pub pop_messages: i64,
    pub ack_requests: i64,
    pub ack_messages: i64,
    pub ack_success: i64,
    pub ack_failed: i64,
    pub transactions: i64,
    /// Messages moved to the DLQ by this node's requests.
    pub dlq: i64,
    /// Raft has no database; commit failures of the local store land here.
    pub db_errors: i64,
    pub avg_event_loop_lag_ms: i32,
    pub max_event_loop_lag_ms: i32,
    /// Pop lag (delivery time − message creation time) over this flush.
    pub avg_lag_ms: i64,
    pub max_lag_ms: i64,
    pub lag_count: i64,
}

/// One row per node per flush. `metrics_json` is nested JSON (every numeric
/// leaf `{avg,min,max,last}`), with a `raft` family in place of the old
/// database-pool family. Kept as a string so the row encodes in any serde
/// format.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct SystemRow {
    pub at_us: i64,
    pub hostname: String,
    pub port: i32,
    /// `"worker-0"`.
    pub worker_id: String,
    /// The flush interval in seconds.
    pub sample_count: i32,
    pub metrics_json: String,
}

/// `queen.queue_lag_metrics` (the per-queue ops columns): one row per node,
/// queue and minute bucket in which the queue saw any activity. `bucket_us` is
/// the flush time truncated to the minute. Rows of several nodes for the same
/// `(bucket_us, tenant, queue)` are merged by [`merge_queue_rows`].
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct QueueRow {
    pub bucket_us: i64,
    pub tenant: String,
    pub queue: String,
    /// Messages delivered by pops.
    pub pop_messages: i64,
    pub push_requests: i64,
    pub push_messages: i64,
    /// Pops that came back empty.
    pub pop_empty: i64,
    pub transactions: i64,
    pub ack_requests: i64,
    pub ack_success: i64,
    pub ack_failed: i64,
    pub avg_lag_ms: i64,
    pub max_lag_ms: i64,
    pub lag_count: i64,
    /// Minute-average of the node's 1 Hz parked long-poll samples.
    pub parked_count: i32,
    pub conflated: i64,
}

/// `queen.queue_parked_replica`: one row per node, queue and minute with any
/// parked long-poll. Never merged: the node is part of the key.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ParkedRow {
    pub bucket_us: i64,
    pub tenant: String,
    pub queue: String,
    pub hostname: String,
    pub worker_id: i32,
    pub parked_count: i32,
}

/// The partition-lifecycle columns of `queue_lag_metrics`
/// (`partitions_created` / `partitions_deleted`). Recorded by apply, so every
/// node holds the same rows: a read uses the answering node's rows only and
/// never sums them across nodes.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct ChurnRow {
    pub bucket_us: i64,
    pub tenant: String,
    pub queue: String,
    pub created: i64,
    pub deleted: i64,
}

/// `queen.retention_history`: one row per retention step that moved a
/// partition's log start. Recorded by apply (the leader's retention loop plans
/// it, every node applies it), so like [`ChurnRow`] it is served from the
/// answering node only. Raft records the log-start move, not which rule made
/// it: `completed_retention_msgs` and `eviction_msgs` stay 0 until the
/// watermark effect carries the rule.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct RetentionRow {
    pub at_us: i64,
    pub tenant: String,
    pub queue: String,
    /// The partition's internal id.
    pub partition_id: u64,
    pub retention_msgs: i64,
    pub completed_retention_msgs: i64,
    pub eviction_msgs: i64,
    /// Log start before and after the step (`logStartFrom` / `logStartTo`).
    pub log_from: u64,
    pub log_to: u64,
    /// The dedup (txns) window start before and after the step.
    pub txns_from: u64,
    pub txns_to: u64,
}

/// Merge queue rows of several nodes into one row per
/// `(bucket_us, tenant, queue)`: counters add, `avg_lag_ms` is the
/// `lag_count`-weighted mean, `max_lag_ms` the max,
/// `parked_count` adds (each node's minute-average of its own long-polls).
/// The result is sorted by `(bucket_us, tenant, queue)`.
pub fn merge_queue_rows(rows: impl IntoIterator<Item = QueueRow>) -> Vec<QueueRow> {
    use std::collections::BTreeMap;
    let mut out: BTreeMap<(i64, String, String), QueueRow> = BTreeMap::new();
    for r in rows {
        let key = (r.bucket_us, r.tenant.clone(), r.queue.clone());
        match out.get_mut(&key) {
            None => {
                out.insert(key, r);
            }
            Some(m) => {
                let lag_n = m.lag_count + r.lag_count;
                m.avg_lag_ms = if lag_n > 0 {
                    (m.avg_lag_ms * m.lag_count + r.avg_lag_ms * r.lag_count) / lag_n
                } else {
                    0
                };
                m.lag_count = lag_n;
                m.max_lag_ms = m.max_lag_ms.max(r.max_lag_ms);
                m.pop_messages += r.pop_messages;
                m.push_requests += r.push_requests;
                m.push_messages += r.push_messages;
                m.pop_empty += r.pop_empty;
                m.transactions += r.transactions;
                m.ack_requests += r.ack_requests;
                m.ack_success += r.ack_success;
                m.ack_failed += r.ack_failed;
                m.parked_count += r.parked_count;
                m.conflated += r.conflated;
            }
        }
    }
    out.into_values().collect()
}

// ---------------------------------------------------------------------------
// Time helpers (UTC, epoch microseconds)
// ---------------------------------------------------------------------------

pub const US_PER_SEC: i64 = 1_000_000;
pub const US_PER_MIN: i64 = 60 * US_PER_SEC;

/// `us` truncated to a multiple of `step_us` (floor-divide then multiply back;
/// used for minute / hour / day steps and any other step alike).
pub fn trunc_us(us: i64, step_us: i64) -> i64 {
    if step_us <= 0 {
        return us;
    }
    us.div_euclid(step_us) * step_us
}

/// Epoch microseconds → `YYYY-MM-DDTHH:MM:SS.ffffffZ`.
pub fn iso_us(us: i64) -> String {
    const US_PER_DAY: i64 = 86_400_000_000;
    let days = us.div_euclid(US_PER_DAY);
    let rem = us.rem_euclid(US_PER_DAY);
    let (y, m, d) = civil_from_days(days);
    let secs = rem / 1_000_000;
    let frac = rem % 1_000_000;
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}Z",
        secs / 3600,
        (secs / 60) % 60,
        secs % 60
    )
}

/// Parse an ISO-8601 / RFC 3339 timestamp: `YYYY-MM-DD`,
/// `YYYY-MM-DDTHH:MM[:SS[.fff…]]` with `T` or a space, and an optional `Z` /
/// `+HH[:MM]` / `-HH[:MM]` offset (none = UTC).
/// `None` when it does not parse.
pub fn parse_ts_us(s: &str) -> Option<i64> {
    let s = s.trim();
    let b = s.as_bytes();
    let num = |r: std::ops::Range<usize>| -> Option<i64> {
        let t = s.get(r)?;
        if t.bytes().all(|c| c.is_ascii_digit()) {
            t.parse::<i64>().ok()
        } else {
            None
        }
    };
    if b.len() < 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let (y, mo, d) = (num(0..4)?, num(5..7)?, num(8..10)?);
    if !(1..=12).contains(&mo) || !(1..=31).contains(&d) {
        return None;
    }
    let mut hh = 0;
    let mut mm = 0;
    let mut ss = 0;
    let mut frac_us = 0i64;
    let mut i = 10;
    if b.len() > 10 {
        if b[10] != b'T' && b[10] != b't' && b[10] != b' ' {
            return None;
        }
        hh = num(11..13)?;
        if b.get(13) != Some(&b':') {
            return None;
        }
        mm = num(14..16)?;
        i = 16;
        if b.get(16) == Some(&b':') {
            ss = num(17..19)?;
            i = 19;
            if b.get(19) == Some(&b'.') {
                let mut j = 20;
                let mut digits = 0u32;
                let mut v = 0i64;
                while j < b.len() && b[j].is_ascii_digit() {
                    if digits < 6 {
                        v = v * 10 + (b[j] - b'0') as i64;
                        digits += 1;
                    }
                    j += 1;
                }
                if j == 20 {
                    return None;
                }
                frac_us = v * 10_i64.pow(6 - digits);
                i = j;
            }
        }
    }
    let mut offset_s = 0i64;
    if i < b.len() {
        match b[i] {
            b'Z' | b'z' if i + 1 == b.len() => {}
            b'+' | b'-' => {
                let sign = if b[i] == b'-' { -1 } else { 1 };
                let oh = num(i + 1..i + 3)?;
                let om = match b.len() - (i + 3) {
                    0 => 0,
                    3 if b[i + 3] == b':' => num(i + 4..i + 6)?,
                    2 => num(i + 3..i + 5)?,
                    _ => return None,
                };
                offset_s = sign * (oh * 3600 + om * 60);
            }
            _ => return None,
        }
    }
    let days = days_from_civil(y, mo, d);
    let secs = days * 86_400 + hh * 3600 + mm * 60 + ss - offset_s;
    Some(secs * US_PER_SEC + frac_us)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn iso_and_parse_round_trip() {
        let us = 1_790_238_190_493_123;
        assert_eq!(parse_ts_us(&iso_us(us)), Some(us));
        assert_eq!(
            parse_ts_us("2026-09-24T10:00:00+02:00"),
            parse_ts_us("2026-09-24T08:00:00Z")
        );
        assert_eq!(
            parse_ts_us("2026-09-24"),
            parse_ts_us("2026-09-24T00:00:00.000Z")
        );
        assert_eq!(
            parse_ts_us("2026-09-24 08:00"),
            parse_ts_us("2026-09-24T08:00:00Z")
        );
        assert_eq!(parse_ts_us("not a date"), None);
    }

    #[test]
    fn merge_weights_lag_and_adds_counters() {
        let a = QueueRow {
            bucket_us: 60_000_000,
            tenant: "t".into(),
            queue: "q".into(),
            push_messages: 10,
            avg_lag_ms: 100,
            max_lag_ms: 300,
            lag_count: 1,
            ..Default::default()
        };
        let b = QueueRow {
            push_messages: 5,
            avg_lag_ms: 400,
            max_lag_ms: 500,
            lag_count: 3,
            ..a.clone()
        };
        let m = merge_queue_rows([a, b]);
        assert_eq!(m.len(), 1);
        assert_eq!(m[0].push_messages, 15);
        assert_eq!(m[0].avg_lag_ms, (100 + 400 * 3) / 4);
        assert_eq!(m[0].max_lag_ms, 500);
        assert_eq!(m[0].lag_count, 4);
    }
}
