//! usage_minutes / usage_days — the metering tables — in the broker's KV
//! (PLAN_SINGLE_BINARY.md W3, metering). Every read and write `meter.rs` and
//! the console's usage pages issue lives here.
//!
//! [`ns::USAGE_MIN`] `#<cluster>/<minute_us, 20 digits>/<op_class>/<node>` →
//! [`UsageDoc`]:
//! - **One writer per row.** Each node writes only ITS OWN rows (`<node>` is a
//!   stable label unique per process), and writes its CUMULATIVE value for the
//!   minute with a plain `put`: a retry after a lost ack rewrites the same
//!   total, and no two nodes ever read-modify-write one row. Readers sum over
//!   nodes. `meter.rs` keeps the per-key base it read or wrote last, so the
//!   one read a key ever needs (the first flush of a key in a process — a
//!   restart within the minute) is of a row nobody else writes.
//! - **TTL instead of a prune.** A minute of UTC day `D` expires at the start
//!   of day `D + keep_days + 2` ([`minute_ttl_secs`]): every minute of a day
//!   expires at the same instant, so a day is never half gone, and the rollup
//!   only (re)computes days at least a full day away from that instant: a
//!   minute lives at least `keep_days` days.
//!
//! [`ns::USAGE_DAY`] `#<cluster>/<YYYY-MM-DD>/<op_class>` → [`UsageDoc`],
//! forever: RECOMPUTED from the minute rows (sum over nodes), never added to —
//! so whichever node runs the rollup writes the same value, and a re-run writes
//! nothing (unchanged rows are skipped). It only writes days that HAVE
//! minutes, so a day whose minutes expired keeps its figure.
//!
//! The monthly count (`cluster_month_msgs`) is, per day, the larger of the
//! rolled figure and the live minutes. Live minutes are read from the day before
//! the last rolled day onward (the rollup re-rolls that window every pass);
//! earlier days are final, and late spool replays for them trigger a targeted
//! re-roll ([`kv_add_minutes`] reports the days it touched).

use std::collections::{BTreeMap, HashMap};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use serde_json::{json, Value};
use uuid::Uuid;

use super::kv::{self, Expect, KvBackend, Ttl};
use super::schema::{self, ns, ClusterDoc, OutboxDoc, PlanDoc, UsageDoc};
use super::Store;
use crate::meter::UsageRow;

/// Default minute retention (`QUEEN_PROXY_USAGE_KEEP_DAYS`): 90 days.
pub const USAGE_KEEP_DAYS: u64 = 90;

/// Ops per KV write batch: the broker's transaction-wire ceiling (64), well
/// under its HTTP one (256), so the batch fits whichever the backend applies.
pub const KV_BATCH: usize = 64;

/// getPrefix page size (the broker's cap).
const PAGE: usize = 1000;

pub const MINUTE_US: i64 = 60_000_000;
pub const DAY_US: i64 = 86_400_000_000;

// ---------------------------------------------------------------------------
// Node label, retention, time
// ---------------------------------------------------------------------------

/// A node label usable as the last segment of a minute key: `[A-Za-z0-9._-]`,
/// at most 64 characters, `node` when nothing is left. It must be unique per
/// PROCESS writing to one KV (two processes sharing a label overwrite each
/// other's minutes) and stable across restarts (a restart within a minute
/// continues its row instead of starting a second one).
pub fn node_label(raw: &str) -> String {
    let s: String = raw
        .trim()
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-') { c } else { '_' })
        .take(64)
        .collect();
    if s.is_empty() {
        "node".to_string()
    } else {
        s
    }
}

/// The default label: `QUEEN_SERVER_ID`, else `HOSTNAME`, else `node`. The
/// broker may pass something better (its raft node id).
pub fn node_label_from_env() -> String {
    for k in ["QUEEN_SERVER_ID", "HOSTNAME"] {
        if let Ok(v) = std::env::var(k) {
            if !v.trim().is_empty() {
                return node_label(&v);
            }
        }
    }
    "node".to_string()
}

/// Minute retention for the KV TTL (`QUEEN_PROXY_USAGE_KEEP_DAYS`, >= 1).
pub fn keep_days_from_env() -> u64 {
    crate::config::env_u64("QUEEN_PROXY_USAGE_KEEP_DAYS", USAGE_KEEP_DAYS).max(1)
}

pub fn now_us() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

/// Days since 1970-01-01 of a proleptic-Gregorian date (H. Hinnant's
/// `days_from_civil`; the crate carries no date library).
pub fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let (m, d) = (m as i64, d as i64);
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12; // March = 0
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// The inverse of [`days_from_civil`].
pub fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m as u32, d as u32)
}

/// The UTC day number of an instant.
pub fn day_of(us: i64) -> i64 {
    us.div_euclid(DAY_US)
}

/// `YYYY-MM-DD` of a day number (usage_days.day, the USAGE_DAY key segment).
pub fn day_str(day: i64) -> String {
    let (y, m, d) = civil_from_days(day);
    format!("{y:04}-{m:02}-{d:02}")
}

/// A `YYYY-MM-DD` day, strictly.
pub fn parse_day(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() != 10 || b[4] != b'-' || b[7] != b'-' {
        return None;
    }
    let y: i64 = s[0..4].parse().ok()?;
    let m: u32 = s[5..7].parse().ok()?;
    let d: u32 = s[8..10].parse().ok()?;
    if !(1..=12).contains(&m) || d == 0 || d > 31 {
        return None;
    }
    let n = days_from_civil(y, m, d);
    (civil_from_days(n) == (y, m, d)).then_some(n)
}

/// `(first day, first day of the next month, "YYYY-MM")` of `day`'s month.
pub fn month_of(day: i64) -> (i64, i64, String) {
    let (y, m, _) = civil_from_days(day);
    let start = days_from_civil(y, m, 1);
    let next = if m == 12 { days_from_civil(y + 1, 1, 1) } else { days_from_civil(y, m + 1, 1) };
    (start, next, format!("{y:04}-{m:02}"))
}

/// `YYYY-MM-DDTHH:MM:SSZ` — exactly what the console's
/// `to_char(minute AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')` prints.
pub fn iso_minute(us: i64) -> String {
    let day = day_of(us);
    let secs = (us - day * DAY_US) / 1_000_000;
    let (h, mi, s) = (secs / 3600, (secs / 60) % 60, secs % 60);
    format!("{}T{h:02}:{mi:02}:{s:02}Z", day_str(day))
}

/// Seconds a minute row of `minute_us` has left at `now_us`: until the start
/// of UTC day `D + keep_days + 2`, `D` the minute's day. `None` = already
/// past it (too old to write at all).
pub fn minute_ttl_secs(minute_us: i64, keep_days: u64, now_us: i64) -> Option<u64> {
    let expires = (day_of(minute_us) + keep_days.max(1) as i64 + 2) * DAY_US;
    let left = expires - now_us;
    (left > 0).then(|| ((left + 999_999) / 1_000_000) as u64)
}

// ---------------------------------------------------------------------------
// Keys
// ---------------------------------------------------------------------------

/// `#<cluster>/<minute_us 20 digits>/<op_class>/<node>` (ns::USAGE_MIN).
pub fn minute_key(cluster: Uuid, minute_us: i64, op_class: &str, node: &str) -> String {
    format!("{}{cluster}/{}/{op_class}/{node}", schema::K, schema::ordered(minute_us))
}

/// `#<cluster>/<YYYY-MM-DD>/<op_class>` (ns::USAGE_DAY).
pub fn day_key(cluster: Uuid, day: &str, op_class: &str) -> String {
    format!("{}{cluster}/{day}/{op_class}", schema::K)
}

/// `(minute_us, op_class, node)` of a minute key.
pub fn parse_minute_key(k: &str) -> Option<(i64, &str, &str)> {
    let mut it = k.strip_prefix(schema::K)?.splitn(4, '/');
    let _cluster = it.next()?;
    let minute = it.next()?.parse::<i64>().ok()?;
    Some((minute, it.next()?, it.next()?))
}

/// `(day, op_class)` of a day key.
pub fn parse_day_key(k: &str) -> Option<(i64, &str)> {
    let mut it = k.strip_prefix(schema::K)?.splitn(3, '/');
    let _cluster = it.next()?;
    let day = parse_day(it.next()?)?;
    Some((day, it.next()?))
}

/// Just before the first key of `cluster` at or after `at`: a `getPrefix`
/// `after` (exclusive), or an `until` bound (keys `>=` it are past the range).
fn bound(cluster: Uuid, at: &str) -> String {
    format!("{}{cluster}/{at}", schema::K)
}

// ---------------------------------------------------------------------------
// Docs
// ---------------------------------------------------------------------------

fn add_doc(a: &mut UsageDoc, b: &UsageDoc) {
    a.msgs = a.msgs.saturating_add(b.msgs);
    a.reqs = a.reqs.saturating_add(b.reqs);
    a.bytes_in = a.bytes_in.saturating_add(b.bytes_in);
    a.bytes_out = a.bytes_out.saturating_add(b.bytes_out);
}

fn max_doc(a: &UsageDoc, b: &UsageDoc) -> UsageDoc {
    UsageDoc {
        msgs: a.msgs.max(b.msgs),
        reqs: a.reqs.max(b.reqs),
        bytes_in: a.bytes_in.max(b.bytes_in),
        bytes_out: a.bytes_out.max(b.bytes_out),
    }
}

fn clamp(v: u64) -> i64 {
    i64::try_from(v).unwrap_or(i64::MAX)
}

/// A spool/flush row's figures as a document.
pub fn row_doc(r: &UsageRow) -> UsageDoc {
    UsageDoc { msgs: clamp(r.msgs), reqs: clamp(r.reqs), bytes_in: clamp(r.bytes_in), bytes_out: clamp(r.bytes_out) }
}

fn doc_of(v: &Value) -> Option<UsageDoc> {
    serde_json::from_value(v.clone()).ok()
}

fn kv_err(e: kv::KvError) -> String {
    e.to_string()
}

// ---------------------------------------------------------------------------
// Read shapes (the console's /usage and /overview)
// ---------------------------------------------------------------------------

/// One (minute, op_class) of a cluster, summed over nodes.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct UsageMinute {
    /// The minute's start, epoch microseconds (UTC).
    pub minute_us: i64,
    /// The same instant as the console prints it (`YYYY-MM-DDTHH:MM:SSZ`).
    pub minute: String,
    pub op_class: String,
    pub reqs: i64,
    pub msgs: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
}

/// One (UTC day, op_class) of a cluster: the rolled figure, or the live
/// minutes for a day not rolled yet (today), the larger of the two per field.
#[derive(Clone, Debug, Default, PartialEq, Serialize)]
pub struct UsageDay {
    /// `YYYY-MM-DD`.
    pub day: String,
    pub op_class: String,
    pub reqs: i64,
    pub msgs: i64,
    pub bytes_in: i64,
    pub bytes_out: i64,
}

/// A cluster's month-to-date messages (`cluster_month_msgs`).
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct MonthMsgs {
    /// `YYYY-MM` (UTC).
    pub month: String,
    pub msgs: i64,
}

fn minute_out(minute_us: i64, op: String, d: &UsageDoc) -> UsageMinute {
    UsageMinute {
        minute_us,
        minute: iso_minute(minute_us),
        op_class: op,
        reqs: d.reqs,
        msgs: d.msgs,
        bytes_in: d.bytes_in,
        bytes_out: d.bytes_out,
    }
}

// ---------------------------------------------------------------------------
// KV: this node's minute rows (the meter's flush and spool replay)
// ---------------------------------------------------------------------------

/// This node's current rows for `keys` (cluster, minute epoch, op_class), in
/// order; zero where there is none. The meter's seed read: only this node
/// writes these rows, so nothing can change them between this read and the
/// write that follows it.
pub async fn kv_read_own(
    kv: &dyn KvBackend,
    node: &str,
    keys: &[(Uuid, u64, String)],
) -> Result<Vec<UsageDoc>, String> {
    let mut out = Vec::with_capacity(keys.len());
    for chunk in keys.chunks(KV_BATCH) {
        let ks: Vec<String> =
            chunk.iter().map(|(c, m, op)| minute_key(*c, (*m as i64) * MINUTE_US, op, node)).collect();
        let got = kv::get_many::<UsageDoc>(kv, ns::USAGE_MIN, &ks).await.map_err(kv_err)?;
        out.extend(got.into_iter().map(|d| d.map(|d| d.value).unwrap_or_default()));
    }
    Ok(out)
}

/// Overwrite this node's rows with `rows`' TOTALS, as ONE atomic batch (so at
/// most [`KV_BATCH`] distinct keys). A row already past its retention is not
/// written. Returns how many were.
pub async fn kv_write_totals(
    kv: &dyn KvBackend,
    node: &str,
    keep_days: u64,
    rows: &[UsageRow],
    now_us: i64,
) -> Result<usize, String> {
    let mut ops = Vec::with_capacity(rows.len());
    for r in rows {
        let minute_us = (r.minute as i64) * MINUTE_US;
        let Some(ttl) = minute_ttl_secs(minute_us, keep_days, now_us) else { continue };
        ops.push(kv::put_op(
            ns::USAGE_MIN,
            &minute_key(r.cluster_id, minute_us, &r.op, node),
            &row_doc(r),
            Expect::Any,
            Ttl::Seconds(ttl),
            false,
        ));
    }
    if ops.is_empty() {
        return Ok(0);
    }
    let n = ops.len();
    kv::write(kv, ops).await.map_err(kv_err)?;
    Ok(n)
}

/// ADD `rows` (usage deltas: the disk spool's rows) to this node's rows —
/// read, add, overwrite, one atomic write per [`KV_BATCH`] keys. Rows past
/// retention are dropped. Returns the (cluster, UTC day) pairs it wrote to, so
/// the rollup can re-roll a day that is already behind its window.
///
/// At-least-once: a write whose ack is lost is added again when the spool is
/// replayed again.
pub async fn kv_add_minutes(
    kv: &dyn KvBackend,
    node: &str,
    keep_days: u64,
    rows: &[UsageRow],
    now_us: i64,
) -> Result<Vec<(Uuid, i64)>, String> {
    // Duplicates within the rows (several spools of one minute) become one
    // key: the broker takes one write per key per call.
    let mut merged: BTreeMap<(Uuid, u64, String), UsageDoc> = BTreeMap::new();
    for r in rows {
        add_doc(merged.entry((r.cluster_id, r.minute, r.op.clone())).or_default(), &row_doc(r));
    }
    let merged: Vec<((Uuid, u64, String), UsageDoc)> = merged.into_iter().collect();
    let mut touched: Vec<(Uuid, i64)> = Vec::new();
    for chunk in merged.chunks(KV_BATCH) {
        let keys: Vec<(Uuid, u64, String)> = chunk.iter().map(|(k, _)| k.clone()).collect();
        let current = kv_read_own(kv, node, &keys).await?;
        let mut totals = Vec::with_capacity(chunk.len());
        for (((c, m, op), delta), mut cur) in chunk.iter().zip(current) {
            add_doc(&mut cur, delta);
            totals.push(UsageRow {
                cluster_id: *c,
                minute: *m,
                op: op.clone(),
                reqs: cur.reqs.max(0) as u64,
                msgs: cur.msgs.max(0) as u64,
                bytes_in: cur.bytes_in.max(0) as u64,
                bytes_out: cur.bytes_out.max(0) as u64,
            });
        }
        if kv_write_totals(kv, node, keep_days, &totals, now_us).await? > 0 {
            for r in &totals {
                let minute_us = (r.minute as i64) * MINUTE_US;
                if minute_ttl_secs(minute_us, keep_days, now_us).is_some() {
                    let day = (r.cluster_id, day_of(minute_us));
                    if !touched.contains(&day) {
                        touched.push(day);
                    }
                }
            }
        }
    }
    Ok(touched)
}

// ---------------------------------------------------------------------------
// KV: range reads
// ---------------------------------------------------------------------------

/// `(key, value)` of `ns` under `prefix`, strictly after `after`, stopping at
/// the first key `>= until`.
async fn scan_range(
    kv: &dyn KvBackend,
    ns: &str,
    prefix: &str,
    after: Option<String>,
    until: Option<&str>,
) -> Result<Vec<(String, Value)>, String> {
    let mut out = Vec::new();
    let mut after = after;
    loop {
        let mut op = json!({"op":"getPrefix","ns":ns,"prefix":prefix,"limit":PAGE});
        if let Some(a) = &after {
            op["after"] = Value::String(a.clone());
        }
        let res = kv.kv(vec![op]).await.map_err(kv_err)?;
        let Some(r) = res.into_iter().next() else { break };
        let rows = r.get("rows").and_then(Value::as_array).cloned().unwrap_or_default();
        let mut last = None;
        for row in &rows {
            let Some(k) = row.get("key").and_then(Value::as_str) else { continue };
            if until.is_some_and(|u| k >= u) {
                return Ok(out);
            }
            last = Some(k.to_string());
            out.push((k.to_string(), row.get("value").cloned().unwrap_or(Value::Null)));
        }
        let truncated = r.get("truncated").and_then(Value::as_bool).unwrap_or(false);
        let next = r.get("nextAfter").and_then(Value::as_str).map(str::to_string).or(last);
        if !truncated || rows.is_empty() || next.is_none() {
            break;
        }
        after = next;
    }
    Ok(out)
}

/// Every node's minute rows of `cluster` in `[from_us, to_us)`, unsummed:
/// `(minute_us, op_class, doc)` in key order.
async fn kv_minutes(
    kv: &dyn KvBackend,
    cluster: Uuid,
    from_us: i64,
    to_us: Option<i64>,
) -> Result<Vec<(i64, String, UsageDoc)>, String> {
    let until = to_us.map(|t| bound(cluster, &schema::ordered(t)));
    let rows = scan_range(
        kv,
        ns::USAGE_MIN,
        &schema::prefix(cluster),
        Some(bound(cluster, &schema::ordered(from_us))),
        until.as_deref(),
    )
    .await?;
    Ok(rows
        .into_iter()
        .filter_map(|(k, v)| {
            let (minute, op, _node) = parse_minute_key(&k)?;
            Some((minute, op.to_string(), doc_of(&v)?))
        })
        .collect())
}

/// The rolled rows of `cluster` for days `from..=to`.
async fn kv_days(
    kv: &dyn KvBackend,
    cluster: Uuid,
    from: i64,
    to: i64,
) -> Result<BTreeMap<(i64, String), UsageDoc>, String> {
    let mut out = BTreeMap::new();
    if from > to {
        return Ok(out);
    }
    let until = bound(cluster, &day_str(to + 1));
    let rows =
        scan_range(kv, ns::USAGE_DAY, &schema::prefix(cluster), Some(bound(cluster, &day_str(from))), Some(&until))
            .await?;
    for (k, v) in rows {
        let (Some((day, op)), Some(doc)) = (parse_day_key(&k), doc_of(&v)) else { continue };
        out.insert((day, op.to_string()), doc);
    }
    Ok(out)
}

/// Minutes of days `from..to` (exclusive), summed per (day, op_class) into `agg`.
async fn fold_days(
    kv: &dyn KvBackend,
    cluster: Uuid,
    from: i64,
    to: i64,
    agg: &mut BTreeMap<(i64, String), UsageDoc>,
) -> Result<(), String> {
    if from >= to {
        return Ok(());
    }
    for (minute, op, doc) in kv_minutes(kv, cluster, from * DAY_US, Some(to * DAY_US)).await? {
        add_doc(agg.entry((day_of(minute), op)).or_default(), &doc);
    }
    Ok(())
}

/// A cluster's minutes in `[from_us, to_us)`, summed over nodes.
pub async fn kv_usage_by_minute(
    kv: &dyn KvBackend,
    cluster: Uuid,
    from_us: i64,
    to_us: Option<i64>,
) -> Result<Vec<UsageMinute>, String> {
    let mut agg: BTreeMap<(i64, String), UsageDoc> = BTreeMap::new();
    for (minute, op, doc) in kv_minutes(kv, cluster, from_us, to_us).await? {
        add_doc(agg.entry((minute, op)).or_default(), &doc);
    }
    Ok(agg.into_iter().map(|((m, op), d)| minute_out(m, op, &d)).collect())
}

/// A cluster's days `from..=to`: rolled, or live for the days from the one
/// before the last rolled day onward, the larger per field.
pub async fn kv_usage_by_day(kv: &dyn KvBackend, cluster: Uuid, from: i64, to: i64) -> Result<Vec<UsageDay>, String> {
    let rolled = kv_days(kv, cluster, from, to).await?;
    let last = rolled.keys().map(|(d, _)| *d).max();
    let live_from = last.map_or(from, |d| (d - 1).max(from));
    let mut live = BTreeMap::new();
    fold_days(kv, cluster, live_from, to + 1, &mut live).await?;
    let mut out: BTreeMap<(i64, String), UsageDoc> = rolled;
    for (k, d) in live {
        let merged = match out.get(&k) {
            Some(r) => max_doc(r, &d),
            None => d,
        };
        out.insert(k, merged);
    }
    Ok(out
        .into_iter()
        .map(|((day, op), d)| UsageDay {
            day: day_str(day),
            op_class: op,
            reqs: d.reqs,
            msgs: d.msgs,
            bytes_in: d.bytes_in,
            bytes_out: d.bytes_out,
        })
        .collect())
}

/// `cluster_month_msgs` over the KV, for the month containing `now_us`: per
/// day, the larger of the rolled figure and the live minutes.
pub async fn kv_month_msgs(kv: &dyn KvBackend, cluster: Uuid, now_us: i64) -> Result<MonthMsgs, String> {
    let (start, next, month) = month_of(day_of(now_us));
    let mut per_day: BTreeMap<i64, (i64, i64)> = BTreeMap::new();
    for ((day, _op), d) in kv_days(kv, cluster, start, next - 1).await? {
        let e = per_day.entry(day).or_default();
        e.0 = e.0.saturating_add(d.msgs);
    }
    let last = per_day.keys().max().copied();
    let live_from = last.map_or(start, |d| (d - 1).max(start));
    for (minute, _op, d) in kv_minutes(kv, cluster, live_from * DAY_US, Some(next * DAY_US)).await? {
        let e = per_day.entry(day_of(minute)).or_default();
        e.1 = e.1.saturating_add(d.msgs);
    }
    let msgs = per_day.values().fold(0i64, |acc, (rolled, live)| acc.saturating_add((*rolled).max(*live)));
    Ok(MonthMsgs { month, msgs })
}

// ---------------------------------------------------------------------------
// KV: rollup
// ---------------------------------------------------------------------------

/// The daily rollup over the KV, as of `now_us`. For every cluster, the closed
/// days from the one before its last rolled day through yesterday (every
/// closed day in retention when none is rolled yet), plus `extra` days the
/// spool replay wrote to, are recomputed from the minutes (all nodes) and
/// written where they differ. Days whose minutes are within a day of their
/// TTL are never recomputed. Returns the rows written.
pub async fn kv_rollup(kv: &dyn KvBackend, keep_days: u64, extra: &[(Uuid, i64)], now_us: i64) -> Result<u64, String> {
    let today = day_of(now_us);
    let yesterday = today - 1;
    let floor = today - keep_days.max(1) as i64;
    if floor > yesterday {
        return Ok(0);
    }
    let mut extra_by: HashMap<Uuid, Vec<i64>> = HashMap::new();
    for (c, d) in extra {
        extra_by.entry(*c).or_default().push(*d);
    }
    let clusters: Vec<Uuid> = kv::scan_keys(kv, ns::CLUSTERS, schema::K)
        .await
        .map_err(kv_err)?
        .iter()
        .filter_map(|k| k.strip_prefix(schema::K).and_then(|s| Uuid::parse_str(s).ok()))
        .collect();
    let mut written = 0u64;
    for c in clusters {
        written +=
            kv_rollup_cluster(kv, c, floor, yesterday, extra_by.get(&c).map(Vec::as_slice).unwrap_or(&[])).await?;
    }
    Ok(written)
}

async fn kv_rollup_cluster(
    kv: &dyn KvBackend,
    cluster: Uuid,
    floor: i64,
    yesterday: i64,
    extra: &[i64],
) -> Result<u64, String> {
    let rolled = kv_days(kv, cluster, floor, yesterday).await?;
    let last = rolled.keys().map(|(d, _)| *d).max();
    let from = last.map_or(floor, |d| (d - 1).max(floor));
    let mut agg: BTreeMap<(i64, String), UsageDoc> = BTreeMap::new();
    fold_days(kv, cluster, from, yesterday + 1, &mut agg).await?;
    let mut behind: Vec<i64> = extra.iter().copied().filter(|d| *d >= floor && *d < from).collect();
    behind.sort_unstable();
    behind.dedup();
    for d in behind {
        fold_days(kv, cluster, d, d + 1, &mut agg).await?;
    }
    let ops: Vec<Value> = agg
        .iter()
        .filter(|(k, doc)| rolled.get(k) != Some(doc))
        .map(|((day, op), doc)| {
            kv::put_op(ns::USAGE_DAY, &day_key(cluster, &day_str(*day), op), doc, Expect::Any, Ttl::Forever, false)
        })
        .collect();
    for chunk in ops.chunks(KV_BATCH) {
        kv::write(kv, chunk.to_vec()).await.map_err(kv_err)?;
    }
    Ok(ops.len() as u64)
}

// ---------------------------------------------------------------------------
// KV: the monthly-quota read and the outbox
// ---------------------------------------------------------------------------

/// The quota pass's rows over the KV: clusters with a monthly allowance (plan
/// or override), not being deleted, with their month-to-date messages.
pub async fn kv_quota_rows(kv: &dyn KvBackend, now_us: i64) -> Result<Vec<QuotaRow>, String> {
    let plans: HashMap<Uuid, PlanDoc> = kv::scan::<PlanDoc>(kv, ns::PLANS, schema::K)
        .await
        .map_err(kv_err)?
        .into_iter()
        .map(|(_, d)| (d.value.id, d.value))
        .collect();
    let clusters = kv::scan::<ClusterDoc>(kv, ns::CLUSTERS, schema::K).await.map_err(kv_err)?;
    let mut out = Vec::new();
    for (_, doc) in clusters {
        let c = doc.value;
        if c.status == "deleting" {
            continue;
        }
        let Some(plan) = plans.get(&c.plan_id) else { continue };
        let overridden = c.limit_overrides.get("monthly_msgs_quota").is_some();
        if plan.monthly_msgs_quota.is_none() && !overridden {
            continue;
        }
        let m = kv_month_msgs(kv, c.id, now_us).await?;
        out.push(QuotaRow {
            cluster_id: c.id,
            tenant_id: c.tenant_id.to_string(),
            slug: c.slug,
            plan_quota: plan.monthly_msgs_quota,
            overrides: c.limit_overrides,
            msgs: m.msgs,
            month: m.month,
        });
    }
    Ok(out)
}

/// Is there an outbox event of `kind` for (`cluster_id`, `month`)? Events for
/// a month are written during it or after, so the scan starts at its first
/// instant (outbox keys sort by creation time).
pub async fn kv_outbox_seen(kv: &dyn KvBackend, kind: &str, cluster_id: &str, month: &str) -> Result<bool, String> {
    let start_day = parse_day(&format!("{month}-01")).ok_or_else(|| format!("bad month {month:?}"))?;
    let after = format!("{}{}", schema::K, schema::ordered(start_day * DAY_US));
    for (_, v) in scan_range(kv, ns::OUTBOX, schema::K, Some(after), None).await? {
        let Ok(doc) = serde_json::from_value::<OutboxDoc>(v) else { continue };
        if doc.kind == kind
            && doc.payload.get("cluster_id").and_then(Value::as_str) == Some(cluster_id)
            && doc.payload.get("month").and_then(Value::as_str) == Some(month)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// `emit_outbox(kind, payload)` over the KV: one [`OutboxDoc`] at
/// `#<created_us>/<id>` (schema.rs; the same doc the web plane's outbox
/// writer produces).
pub async fn kv_emit_outbox(kv: &dyn KvBackend, kind: &str, payload: &Value, now_us: i64) -> Result<Uuid, String> {
    let kind = kind.trim();
    if kind.is_empty() {
        return Err("emit_outbox: kind must not be empty".to_string());
    }
    let id = Uuid::new_v4();
    let doc = OutboxDoc {
        id,
        kind: kind.to_string(),
        payload: if payload.is_null() { json!({}) } else { payload.clone() },
        created_at_us: now_us,
        consumed_at_us: None,
    };
    let op =
        kv::put_op(ns::OUTBOX, &schema::key2(schema::ordered(now_us), id), &doc, Expect::Absent, Ttl::Forever, true);
    kv::write(kv, vec![op]).await.map_err(kv_err)?;
    Ok(id)
}

// ---------------------------------------------------------------------------
// The repository: one function per question
// ---------------------------------------------------------------------------

/// A cluster with a monthly allowance, as the quota pass sees it.
#[derive(Clone, Debug, PartialEq)]
pub struct QuotaRow {
    pub cluster_id: Uuid,
    pub tenant_id: String,
    pub slug: String,
    pub plan_quota: Option<i64>,
    /// `clusters.limit_overrides` (Null when it did not parse).
    pub overrides: Value,
    /// Calendar-month messages (`cluster_month_msgs`).
    pub msgs: i64,
    /// `YYYY-MM` (UTC), the month `msgs` counts.
    pub month: String,
}

/// Fold every closed day into usage_days ([`kv_rollup`], with `extra` days
/// to recompute). Returns the rows written.
pub async fn rollup_days(store: &Store, keep_days: u64, extra: &[(Uuid, i64)]) -> Result<u64, String> {
    match store {
        Store::Kv(kv) => kv_rollup(kv.as_ref(), keep_days, extra, now_us()).await,
        Store::None => Ok(0),
    }
}

/// Clusters with a monthly allowance (plan or override), not being deleted,
/// with their month-to-date messages.
pub async fn quota_rows(store: &Store) -> Result<Vec<QuotaRow>, String> {
    match store {
        Store::Kv(kv) => kv_quota_rows(kv.as_ref(), now_us()).await,
        Store::None => Ok(Vec::new()),
    }
}

/// Has this exact (kind, cluster, month) event already been written? Covers a
/// restart mid-month (and, in the single binary, a second node).
pub async fn quota_event_seen(store: &Store, kind: &str, cluster_id: &str, month: &str) -> Result<bool, String> {
    match store {
        Store::Kv(kv) => kv_outbox_seen(kv.as_ref(), kind, cluster_id, month).await,
        Store::None => Ok(false),
    }
}

/// `emit_outbox(kind, payload)`: a control-plane-bound event.
pub async fn emit_outbox(store: &Store, kind: &str, payload: &Value) -> Result<(), String> {
    match store {
        Store::Kv(kv) => kv_emit_outbox(kv.as_ref(), kind, payload, now_us()).await.map(|_| ()),
        Store::None => Ok(()),
    }
}

/// The console's `/usage?hours=N`: a cluster's minutes from `now - hours`
/// onward, per (minute, op_class) ascending, summed over nodes.
pub async fn cluster_usage_recent(store: &Store, cluster: Uuid, hours: i64) -> Result<Vec<UsageMinute>, String> {
    match store {
        Store::Kv(kv) => {
            let from = now_us().saturating_sub(hours.saturating_mul(3_600_000_000));
            kv_usage_by_minute(kv.as_ref(), cluster, from, None).await
        }
        Store::None => Ok(Vec::new()),
    }
}

/// A cluster's usage per (minute, op_class) in `[from_us, to_us)` (`to_us`
/// None = open-ended), ascending, summed over nodes.
pub async fn cluster_usage_by_minute(
    store: &Store,
    cluster: Uuid,
    from_us: i64,
    to_us: Option<i64>,
) -> Result<Vec<UsageMinute>, String> {
    match store {
        Store::Kv(kv) => kv_usage_by_minute(kv.as_ref(), cluster, from_us, to_us).await,
        Store::None => Ok(Vec::new()),
    }
}

/// A cluster's usage per (UTC day, op_class) for `from_day..=to_day`
/// (`YYYY-MM-DD`), ascending: the rolled figure, or the live minutes of a day
/// not rolled yet (today), the larger per field.
pub async fn cluster_usage_by_day(
    store: &Store,
    cluster: Uuid,
    from_day: &str,
    to_day: &str,
) -> Result<Vec<UsageDay>, String> {
    let from = parse_day(from_day).ok_or_else(|| format!("bad day {from_day:?} (YYYY-MM-DD)"))?;
    let to = parse_day(to_day).ok_or_else(|| format!("bad day {to_day:?} (YYYY-MM-DD)"))?;
    match store {
        Store::Kv(kv) => kv_usage_by_day(kv.as_ref(), cluster, from, to).await,
        Store::None => Ok(Vec::new()),
    }
}

/// The console's `/overview` counter: month-to-date messages of the current
/// UTC month (`cluster_month_msgs`), with the month.
pub async fn cluster_month_msgs(store: &Store, cluster: Uuid) -> Result<MonthMsgs, String> {
    match store {
        Store::Kv(kv) => kv_month_msgs(kv.as_ref(), cluster, now_us()).await,
        Store::None => Ok(MonthMsgs { month: month_of(day_of(now_us())).2, msgs: 0 }),
    }
}

/// Every usage row of a cluster (minutes and days), for the cascade of a
/// cluster delete. Returns the rows deleted (0 without a store).
pub async fn delete_cluster_usage(store: &Store, cluster: Uuid) -> Result<u64, String> {
    let Some(kv) = store.kv() else { return Ok(0) };
    let mut n = 0u64;
    for space in [ns::USAGE_MIN, ns::USAGE_DAY] {
        let keys = kv::scan_keys(kv.as_ref(), space, &schema::prefix(cluster)).await.map_err(kv_err)?;
        for chunk in keys.chunks(KV_BATCH) {
            let ops = chunk.iter().map(|k| kv::delete_op(space, k, Expect::Any, false)).collect();
            kv::write(kv.as_ref(), ops).await.map_err(kv_err)?;
            n += chunk.len() as u64;
        }
    }
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::super::memkv::MemKv;
    use super::*;

    const C1: &str = "11111111-1111-4111-8111-111111111111";

    fn c1() -> Uuid {
        Uuid::parse_str(C1).unwrap()
    }

    /// 2026-09-24 12:00:00 UTC.
    fn noon_0924() -> i64 {
        days_from_civil(2026, 9, 24) * DAY_US + 12 * 3_600_000_000
    }

    fn row(cluster: Uuid, minute_us: i64, op: &str, msgs: u64) -> UsageRow {
        UsageRow {
            cluster_id: cluster,
            minute: (minute_us / MINUTE_US) as u64,
            op: op.into(),
            reqs: 1,
            msgs,
            bytes_in: msgs * 10,
            bytes_out: 0,
        }
    }

    async fn put_min(kv: &MemKv, cluster: Uuid, minute_us: i64, op: &str, node: &str, msgs: i64) {
        let doc = UsageDoc { msgs, reqs: 1, bytes_in: msgs * 10, bytes_out: 0 };
        kv::write(
            kv,
            vec![kv::put_op(
                ns::USAGE_MIN,
                &minute_key(cluster, minute_us, op, node),
                &doc,
                Expect::Any,
                Ttl::Forever,
                false,
            )],
        )
        .await
        .unwrap();
    }

    async fn put_day(kv: &MemKv, cluster: Uuid, day: i64, op: &str, msgs: i64) {
        let doc = UsageDoc { msgs, reqs: 1, bytes_in: msgs * 10, bytes_out: 0 };
        kv::write(
            kv,
            vec![kv::put_op(
                ns::USAGE_DAY,
                &day_key(cluster, &day_str(day), op),
                &doc,
                Expect::Any,
                Ttl::Forever,
                false,
            )],
        )
        .await
        .unwrap();
    }

    async fn day_doc(kv: &MemKv, cluster: Uuid, day: i64, op: &str) -> Option<UsageDoc> {
        kv::get::<UsageDoc>(kv, ns::USAGE_DAY, &day_key(cluster, &day_str(day), op)).await.unwrap().map(|d| d.value)
    }

    async fn put_cluster(kv: &MemKv, id: Uuid, plan: Uuid, status: &str, overrides: Value) {
        let doc = ClusterDoc {
            id,
            tenant_id: Uuid::new_v4(),
            cell_id: Uuid::new_v4(),
            plan_id: plan,
            slug: format!("c-{}", &id.to_string()[..8]),
            broker_tenant_uuid: Uuid::new_v4(),
            status: status.into(),
            limit_overrides: overrides,
            created_at_us: 0,
        };
        kv::write(kv, vec![kv::put_op(ns::CLUSTERS, &schema::key(id), &doc, Expect::Any, Ttl::Forever, false)])
            .await
            .unwrap();
    }

    #[test]
    fn civil_dates_round_trip() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(2000, 3, 1), 11_017);
        assert_eq!(day_str(days_from_civil(2024, 2, 29)), "2024-02-29");
        for z in -800_000..800_000i64 {
            if z % 997 != 0 {
                continue;
            }
            let (y, m, d) = civil_from_days(z);
            assert_eq!(days_from_civil(y, m, d), z);
        }
        assert_eq!(parse_day("2026-09-24"), Some(days_from_civil(2026, 9, 24)));
        assert_eq!(parse_day("2026-02-30"), None, "not a date");
        assert_eq!(parse_day("2026-9-24"), None);
        let (s, n, label) = month_of(days_from_civil(2026, 12, 31));
        assert_eq!((day_str(s), day_str(n), label.as_str()), ("2026-12-01".into(), "2027-01-01".into(), "2026-12"));
        assert_eq!(iso_minute(noon_0924() + 31 * MINUTE_US), "2026-09-24T12:31:00Z");
        assert_eq!(iso_minute(0), "1970-01-01T00:00:00Z");
    }

    #[test]
    fn minute_keys_sort_by_time_and_parse_back() {
        let a = minute_key(c1(), 9 * MINUTE_US, "push", "n1");
        let b = minute_key(c1(), 10 * MINUTE_US, "delivery", "n0");
        assert!(a < b, "zero-padded minutes sort chronologically");
        assert_eq!(parse_minute_key(&b), Some((10 * MINUTE_US, "delivery", "n0")));
        let d = day_key(c1(), "2026-09-24", "push");
        assert_eq!(parse_day_key(&d), Some((days_from_civil(2026, 9, 24), "push")));
        assert_eq!(node_label(" node/1 ä "), "node_1__");
        assert_eq!(node_label(""), "node");
    }

    #[test]
    fn a_whole_day_of_minutes_expires_at_one_instant() {
        let now = noon_0924();
        let today = day_of(now);
        let first = today * DAY_US;
        let last = first + 1439 * MINUTE_US;
        let a = minute_ttl_secs(first, 90, now).unwrap() as i64;
        let b = minute_ttl_secs(last, 90, now).unwrap() as i64;
        assert_eq!(a, b, "same day, same expiry");
        assert_eq!(now + a * 1_000_000, (today + 92) * DAY_US);
        // A minute 92 days back is gone already; 91 days back still has hours.
        assert!(minute_ttl_secs((today - 92) * DAY_US, 90, now).is_none());
        assert!(minute_ttl_secs((today - 91) * DAY_US, 90, now).is_some());
    }

    #[tokio::test]
    async fn readers_sum_over_nodes_and_totals_overwrite() {
        let kv = MemKv::new();
        let now = noon_0924();
        let m = now - 5 * MINUTE_US;
        let m = m - m.rem_euclid(MINUTE_US);
        kv_write_totals(&kv, "n1", 90, &[row(c1(), m, "push", 3)], now).await.unwrap();
        kv_write_totals(&kv, "n2", 90, &[row(c1(), m, "push", 4)], now).await.unwrap();
        // n1's cumulative value grows: overwritten, not added.
        kv_write_totals(&kv, "n1", 90, &[row(c1(), m, "push", 5)], now).await.unwrap();
        assert_eq!(kv.keys(ns::USAGE_MIN).len(), 2, "one row per node");
        let got = kv_usage_by_minute(&kv, c1(), m, Some(m + MINUTE_US)).await.unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!((got[0].msgs, got[0].reqs, got[0].bytes_in), (9, 2, 90));
        assert_eq!(got[0].minute, iso_minute(m));
        // The range is half-open and minute-aligned.
        assert!(kv_usage_by_minute(&kv, c1(), m + 1, None).await.unwrap().is_empty());
        assert!(kv_usage_by_minute(&kv, c1(), 0, Some(m)).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn add_minutes_adds_to_the_row_and_reports_the_days() {
        let kv = MemKv::new();
        let now = noon_0924();
        let m = day_of(now) * DAY_US - 3 * DAY_US; // three days back
        put_min(&kv, c1(), m, "push", "n1", 10).await;
        let touched =
            kv_add_minutes(&kv, "n1", 90, &[row(c1(), m, "push", 2), row(c1(), m, "push", 3)], now).await.unwrap();
        assert_eq!(touched, vec![(c1(), day_of(m))]);
        let got = kv_usage_by_minute(&kv, c1(), m, Some(m + MINUTE_US)).await.unwrap();
        assert_eq!(got[0].msgs, 15, "10 already there + 2 + 3 replayed");
        // Past retention: dropped, nothing touched.
        let old = (day_of(now) - 200) * DAY_US;
        assert!(kv_add_minutes(&kv, "n1", 90, &[row(c1(), old, "push", 1)], now).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn rollup_sums_nodes_is_idempotent_and_takes_late_minutes() {
        let kv = MemKv::new();
        let now = noon_0924();
        let today = day_of(now);
        let y = today - 1;
        put_cluster(&kv, c1(), Uuid::new_v4(), "active", json!({})).await;
        put_min(&kv, c1(), y * DAY_US + 5 * MINUTE_US, "push", "n1", 3).await;
        put_min(&kv, c1(), y * DAY_US + 5 * MINUTE_US, "push", "n2", 4).await;
        put_min(&kv, c1(), y * DAY_US + 900 * MINUTE_US, "read", "n1", 1).await;
        put_min(&kv, c1(), today * DAY_US + MINUTE_US, "push", "n1", 100).await; // today: open day

        assert_eq!(kv_rollup(&kv, 90, &[], now).await.unwrap(), 2);
        assert_eq!(day_doc(&kv, c1(), y, "push").await.unwrap().msgs, 7);
        assert_eq!(day_doc(&kv, c1(), y, "read").await.unwrap().msgs, 1);
        assert!(day_doc(&kv, c1(), today, "push").await.is_none(), "today is not closed");

        // Same inputs, any node: nothing to write.
        assert_eq!(kv_rollup(&kv, 90, &[], now).await.unwrap(), 0);

        // A late minute for yesterday (a node's last flush): re-rolled.
        put_min(&kv, c1(), y * DAY_US + 1439 * MINUTE_US, "push", "n3", 5).await;
        assert_eq!(kv_rollup(&kv, 90, &[], now).await.unwrap(), 1);
        assert_eq!(day_doc(&kv, c1(), y, "push").await.unwrap().msgs, 12);
    }

    #[tokio::test]
    async fn rollup_window_and_spool_days_behind_it() {
        let kv = MemKv::new();
        let now = noon_0924();
        let today = day_of(now);
        put_cluster(&kv, c1(), Uuid::new_v4(), "active", json!({})).await;
        // Never rolled: every closed day in retention with minutes is.
        put_min(&kv, c1(), (today - 30) * DAY_US, "push", "n1", 2).await;
        put_min(&kv, c1(), (today - 1) * DAY_US, "push", "n1", 1).await;
        // Beyond keep_days: never touched (its minutes may be expiring).
        put_min(&kv, c1(), (today - 95) * DAY_US, "push", "n1", 9).await;
        assert_eq!(kv_rollup(&kv, 90, &[], now).await.unwrap(), 2);
        assert!(day_doc(&kv, c1(), today - 95, "push").await.is_none());

        // A spool replay adds to day -30, which is behind the re-roll window
        // now (last rolled = yesterday): only the explicit day brings it back.
        put_min(&kv, c1(), (today - 30) * DAY_US + MINUTE_US, "push", "n2", 5).await;
        assert_eq!(kv_rollup(&kv, 90, &[], now).await.unwrap(), 0);
        assert_eq!(day_doc(&kv, c1(), today - 30, "push").await.unwrap().msgs, 2);
        assert_eq!(kv_rollup(&kv, 90, &[(c1(), today - 30)], now).await.unwrap(), 1);
        assert_eq!(day_doc(&kv, c1(), today - 30, "push").await.unwrap().msgs, 7);

        // A rolled day whose minutes are gone (expired) keeps its figure:
        // only days WITH minutes are written.
        put_day(&kv, c1(), today - 2, "txn", 42).await;
        kv_rollup(&kv, 90, &[(c1(), today - 2)], now).await.unwrap();
        assert_eq!(day_doc(&kv, c1(), today - 2, "txn").await.unwrap().msgs, 42);
    }

    #[tokio::test]
    async fn rollup_skips_clusters_that_no_longer_exist() {
        let kv = MemKv::new();
        let now = noon_0924();
        put_min(&kv, c1(), (day_of(now) - 1) * DAY_US, "push", "n1", 1).await;
        assert_eq!(kv_rollup(&kv, 90, &[], now).await.unwrap(), 0, "no cluster row: FK cascade semantics");
    }

    #[tokio::test]
    async fn month_msgs_is_rolled_days_plus_the_live_remainder() {
        let kv = MemKv::new();
        let now = noon_0924();
        let today = day_of(now);
        let (start, _, _) = month_of(today);
        // Last month: never counted.
        put_day(&kv, c1(), start - 1, "push", 1_000).await;
        put_min(&kv, c1(), (start - 1) * DAY_US, "push", "n1", 1_000).await;
        // Rolled days 1..=22 of the month, 10 msgs each (two ops of 5).
        for d in start..today - 1 {
            put_day(&kv, c1(), d, "push", 5).await;
            put_day(&kv, c1(), d, "delivery", 5).await;
        }
        // Day 22 (the last rolled day) got a late minute after its rollup:
        // GREATEST(10, 12) = 12. Its own minutes are 12 in total.
        put_min(&kv, c1(), (today - 2) * DAY_US, "push", "n1", 7).await;
        put_min(&kv, c1(), (today - 2) * DAY_US, "delivery", "n2", 5).await;
        // An old rolled day's minutes (day 3) are not re-read: the rolled
        // figure stands, like a day already folded in.
        put_min(&kv, c1(), (start + 2) * DAY_US, "push", "n1", 999).await;
        // Yesterday is not rolled yet and today is live: minutes only.
        put_min(&kv, c1(), (today - 1) * DAY_US, "push", "n1", 20).await;
        put_min(&kv, c1(), today * DAY_US, "push", "n1", 30).await;
        put_min(&kv, c1(), today * DAY_US, "push", "n2", 3).await;

        let rolled_days = today - 1 - start;
        let expected = (rolled_days - 1) * 10 + 12 + 20 + 33;
        let got = kv_month_msgs(&kv, c1(), now).await.unwrap();
        assert_eq!(got.month, "2026-09");
        assert_eq!(got.msgs, expected);
    }

    #[tokio::test]
    async fn quota_rows_select_like_the_sql() {
        let kv = MemKv::new();
        let now = noon_0924();
        let quota_plan =
            PlanDoc { id: Uuid::new_v4(), code: "q".into(), monthly_msgs_quota: Some(1000), ..Default::default() };
        let free_plan =
            PlanDoc { id: Uuid::new_v4(), code: "free".into(), monthly_msgs_quota: None, ..Default::default() };
        for p in [&quota_plan, &free_plan] {
            kv::write(&kv, vec![kv::put_op(ns::PLANS, &schema::key(p.id), p, Expect::Any, Ttl::Forever, false)])
                .await
                .unwrap();
        }
        let (a, b, c, d, e) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        put_cluster(&kv, a, quota_plan.id, "active", json!({})).await; // plan quota
        put_cluster(&kv, b, free_plan.id, "push_blocked", json!({"monthly_msgs_quota": 5})).await; // override
        put_cluster(&kv, c, free_plan.id, "active", json!({"monthly_msgs_quota": null})).await; // key present
        put_cluster(&kv, d, free_plan.id, "active", json!({"max_queues": 3})).await; // no allowance
        put_cluster(&kv, e, quota_plan.id, "deleting", json!({})).await; // being deleted
        put_min(&kv, a, now - MINUTE_US, "push", "n1", 7).await;

        let rows = kv_quota_rows(&kv, now).await.unwrap();
        let mut ids: Vec<Uuid> = rows.iter().map(|r| r.cluster_id).collect();
        ids.sort();
        let mut want = vec![a, b, c];
        want.sort();
        assert_eq!(ids, want);
        let ra = rows.iter().find(|r| r.cluster_id == a).unwrap();
        assert_eq!((ra.plan_quota, ra.msgs, ra.month.as_str()), (Some(1000), 7, "2026-09"));
        let rb = rows.iter().find(|r| r.cluster_id == b).unwrap();
        assert_eq!(crate::cache::override_or(&rb.overrides, "monthly_msgs_quota", rb.plan_quota), Some(5));
    }

    #[tokio::test]
    async fn outbox_dedupe_and_emit() {
        let kv = MemKv::new();
        let now = noon_0924();
        let payload = json!({"cluster_id": C1, "month": "2026-09", "msgs": 1});
        assert!(!kv_outbox_seen(&kv, "cluster_monthly_quota_warning", C1, "2026-09").await.unwrap());
        kv_emit_outbox(&kv, " cluster_monthly_quota_warning ", &payload, now).await.unwrap();
        assert!(kv_outbox_seen(&kv, "cluster_monthly_quota_warning", C1, "2026-09").await.unwrap());
        assert!(!kv_outbox_seen(&kv, "cluster_monthly_quota_blocked", C1, "2026-09").await.unwrap());
        assert!(!kv_outbox_seen(&kv, "cluster_monthly_quota_warning", C1, "2026-10").await.unwrap());
        let docs: Vec<(String, kv::Doc<OutboxDoc>)> = kv::scan(&kv, ns::OUTBOX, "#").await.unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].1.value.kind, "cluster_monthly_quota_warning", "kind is trimmed like btrim");
        assert!(docs[0].0.starts_with(&format!("#{}/", schema::ordered(now))));
        assert!(kv_emit_outbox(&kv, "  ", &payload, now).await.is_err());
    }

    #[tokio::test]
    async fn by_day_reads_rolled_days_and_today_live() {
        let kv = MemKv::new();
        let now = noon_0924();
        let today = day_of(now);
        put_day(&kv, c1(), today - 3, "push", 10).await;
        put_day(&kv, c1(), today - 1, "push", 4).await;
        put_min(&kv, c1(), (today - 1) * DAY_US + MINUTE_US, "push", "n1", 6).await; // late
        put_min(&kv, c1(), today * DAY_US + MINUTE_US, "read", "n1", 2).await;
        let store = Store::Kv(std::sync::Arc::new(kv));
        let got = cluster_usage_by_day(&store, c1(), &day_str(today - 3), &day_str(today)).await.unwrap();
        let flat: Vec<(String, String, i64)> =
            got.iter().map(|d| (d.day.clone(), d.op_class.clone(), d.msgs)).collect();
        assert_eq!(
            flat,
            vec![
                (day_str(today - 3), "push".into(), 10),
                (day_str(today - 1), "push".into(), 6),
                (day_str(today), "read".into(), 2),
            ]
        );
        assert!(cluster_usage_by_day(&store, c1(), "2026-13-01", "2026-09-01").await.is_err());
    }

    #[tokio::test]
    async fn delete_cascades_to_one_clusters_usage() {
        let kv = std::sync::Arc::new(MemKv::new());
        let now = noon_0924();
        put_min(&kv, c1(), now, "push", "n1", 1).await;
        put_day(&kv, c1(), day_of(now) - 1, "push", 1).await;
        let other = Uuid::new_v4();
        put_min(&kv, other, now, "push", "n1", 1).await;
        let store = Store::Kv(kv.clone());
        assert_eq!(delete_cluster_usage(&store, c1()).await.unwrap(), 2);
        assert_eq!(kv.keys(ns::USAGE_MIN).len(), 1, "the other cluster keeps its row");
        assert!(kv.keys(ns::USAGE_DAY).is_empty());
    }
}
