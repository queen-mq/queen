//! Queue-scoped dashboard views of raft mode (PLAN_RAFT.md D17, §14.6).
//!
//! Pure views over the per-queue metrics, computed over the node-local rows
//! of [`super::model`]:
//!
//! | function | route |
//! |---|---|
//! | [`queue_ops_json`] | `GET /api/v1/analytics/queue-ops` |
//! | [`parked_replicas_json`] | `GET /api/v1/analytics/queue-parked-replicas` |
//! | [`workload_json`] | `GET /api/v1/analytics/workload` (`handle_workload`) |
//! | [`retention_json`] | `GET /api/v1/analytics/retention` (`handle_retention`) |
//! | [`queue_lag_json`] | `GET /api/v1/analytics/queue-lag` |
//!
//! # Inputs
//!
//! `filters` is the request's query string as a JSON object of strings (the
//! `Query<HashMap<String, String>>` a handler receives, empty values
//! included); the filters JSON built from it (`filters_from_query`,
//! handlers/mod.rs:671) works as well. Each function applies the handler's
//! rules itself: an empty value is an absent filter, except `namespace=` /
//! `task=` on the workload route; keys a handler does not forward are
//! ignored; `_tenant` is ignored because every row slice arrives already
//! filtered to the request tenant. `now_us` plays `NOW()` (the default window
//! is `[now - 1 h, now]`).
//!
//! The [`QueueRow`] slice must hold one row per `(bucket_us, queue)` (the
//! output of [`merge_queue_rows`]), like the table's unique key
//! `(bucket_time, queue_id)`; [`ChurnRow`]s of the same minute and queue are
//! folded into that row, as the partition triggers upsert into it.
//!
//! # Fidelity
//!
//! The answer carries the same keys, nesting, `null` / `0` /
//! `[]` choices, array order, bucket widths, bucket alignment and timestamp
//! texts the dashboard expects. Deliberately not byte-identical:
//!
//! * object key order (`jsonb` sorts keys by length; no reader depends on it);
//! * the text of a fractional value: these views print the `f64` a JSON
//!   parser reads from the canonical decimal text (`4.0`, `85.71428571428571`,
//!   not `4.00` or `85.7142857142857143`). The private `Num` replays
//!   numeric.c's arithmetic (division scale, half-away-from-zero rounding) so
//!   the parsed values are equal, not merely close;
//! * text ordering is bytewise (as under collation "C"), which differs from a
//!   locale-aware sort only between names that mix case or punctuation
//!   differently.
//!
//! An unparsable `from` / `to` makes these views return
//! `{"error": "invalid input syntax for type timestamp with time zone: ..."}`,
//! which a caller maps to the same 500.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::{json, Map, Value};

use super::model::*;

const US_PER_HOUR: i64 = 60 * US_PER_MIN;

/// Longest bucket axis [`workload_json`] builds. 200 000 buckets is 137 years
/// of six-hour steps, so only a nonsense range (`from=0001-01-01`) reaches it.
const MAX_AXIS_BUCKETS: i128 = 200_000;

/// The 400 message of `handle_workload` / `handle_retention`
/// (handlers/analytics.rs:158, :217); the handler's body is
/// `{"error":"bad groupBy"}`.
const BAD_GROUP_BY: &str = "bad groupBy";

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// One queue of the request tenant: the `queen.queues` columns the procedures
/// join (`q.name`, `q.namespace`, `q.task`).
///
/// A metric row survives only while its queue does. The raft rows carry the
/// queue NAME, so the views join on the name and use `created_us` to drop
/// what an earlier queue of the same name left behind. Pass every queue of
/// the tenant: a row whose queue is not in the list is dropped like a
/// deleted queue's.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueueMeta {
    /// `queen.queues.name`, the key the metric rows carry.
    pub name: String,
    /// `queen.queues.namespace`; `None` reads as `''`.
    pub namespace: Option<String>,
    /// `queen.queues.task`; `None` reads as `''`.
    pub task: Option<String>,
    /// `queen.queues.created_at`, epoch µs (raft: `QueueConfig::created_at_us`).
    /// A metric row whose minute precedes the creation minute, or a retention
    /// step before this instant, belongs to an earlier incarnation and is
    /// dropped, the same as a deleted queue's rows. 0 keeps every row.
    pub created_us: i64,
}

/// One queue of the request tenant with the live figures `get_workload_v1`
/// reads in its `cg` / `q_now` CTEs (019:1842-1862). Every queue of the tenant
/// must be present, idle ones included: they count in `queues`, in the tenant
/// row and in `queuesWithoutGroup`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct QueueNow {
    /// The queue's `queen.queues` columns (name, namespace, task, creation).
    pub meta: QueueMeta,
    /// `queen.stats.pending_messages` of the queue's `'queue'` row: the sum
    /// over the queue's partitions of
    /// `GREATEST(last_offset - GREATEST(committed, log_start - 1), 0)`, where
    /// `committed` is the partition's MIN cursor over the named groups, else its
    /// MIN `__QUEUE_MODE__` cursor, else -1.
    pub pending: i64,
    /// `queen.stats.processing_messages`: the sum, over the partition cursors
    /// whose lease is live (`lease_expires_at > now`), of
    /// `GREATEST(0, COALESCE(batch_end, committed) - committed)`, capped at
    /// `pending` (011:244).
    pub processing: i64,
    /// `queen.stats.dead_letter_messages`: the number of the queue's
    /// `queen.log_dlq` rows.
    pub dead_letter: i64,
    /// `queen.stats.retained_bytes`: the stored (compressed) payload bytes of
    /// the queue's live segments, refreshed on a slow ~10 min lane.
    pub retained_bytes: i64,
    /// `queen.stats.child_count`: the number of the queue's partitions.
    pub partitions: i64,
    /// The number of `queen.consumer_groups_metadata` rows with this
    /// `queue_id` and `partition_name = ''`: every queue-scoped group
    /// registration, `__QUEUE_MODE__` included (the wildcard pop registers
    /// one), a namespace/task discovery registration once per matched queue.
    pub groups: i64,
}

// ---------------------------------------------------------------------------
// get_queue_ops_v1
// ---------------------------------------------------------------------------

/// `GET /api/v1/analytics/queue-ops`, behind `handle_queue_ops`
/// (handlers/analytics.rs:138), which forwards `from`, `to`, `queue`.
///
/// `{timeRange, bucketMinutes, series, queues}`: one `series` element per
/// (bucket, queue) that has rows, ordered by bucket then queue, no gap
/// filling. `avgLagMs` is the pop-weighted mean of the rows' `avg_lag_ms` and,
/// with `maxLagMs`, is `null` for a bucket without pops. `partitionCount` is
/// always `null`: nothing has ever written that column. Rates divide by the
/// NOMINAL bucket width, and the six-hour width (ranges over 7 days) floors
/// to the hour (`EXTRACT(minute FROM t) % 360` is the minute itself).
pub fn queue_ops_json(
    filters: &Map<String, Value>,
    now_us: i64,
    rows: &[QueueRow],
    churn: &[ChurnRow],
    queues: &[QueueMeta],
) -> Value {
    let win = match Window::from_filters(filters, now_us) {
        Ok(w) => w,
        Err(e) => return sp_error(e),
    };
    let queue = filter_set(filters, "queue");
    let ix = QueueIndex::new(queues);

    let mut rolled: BTreeMap<(i64, &str), Rolled> = BTreeMap::new();
    for_each_lag_row(rows, churn, &win, &ix, |minute, q, r| {
        let name = ix.name(q);
        if queue.as_deref().is_some_and(|f| f != name) {
            return;
        }
        rolled.entry((win.floor(minute), name)).or_default().add(r);
    });

    let series: Vec<Value> = rolled
        .iter()
        .map(|(&(bucket, name), r)| {
            let popped = r.pop > 0;
            json!({
                "bucket": to_char_s(bucket),
                "queueName": name,
                "pushRequests": int(r.push_req),
                "pushMessages": int(r.push_msg),
                "popMessages": int(r.pop),
                "popEmpty": int(r.pop_empty),
                "ackRequests": int(r.ack_req),
                "ackSuccess": int(r.ack_ok),
                "ackFailed": int(r.ack_fail),
                "transactions": int(r.trx),
                "partitionsCreated": int(r.created),
                "partitionsDeleted": int(r.deleted),
                "partitionCount": Value::Null,
                "avgLagMs": if popped { quotient(r.lag_weight, r.pop) } else { Value::Null },
                // CASE WHEN SUM(pop_count) > 0 THEN MAX(max_lag_ms): the max
                // over EVERY row of the bucket, pop-less rows included.
                "maxLagMs": if popped { opt_int(r.max_lag_all) } else { Value::Null },
                "pushPerSecond": win.per_second(r.push_msg),
                "popPerSecond": win.per_second(r.pop),
                "ackPerSecond": win.per_second(r.ack_ok + r.ack_fail),
                "emptyPerSecond": win.per_second(r.pop_empty),
                "parkedCount": avg_2dp(r.parked_sum, r.rows),
            })
        })
        .collect();
    let names: BTreeSet<&str> = rolled.keys().map(|&(_, name)| name).collect();

    json!({
        "timeRange": win.time_range(),
        "bucketMinutes": win.bucket_min,
        "series": series,
        "queues": names.into_iter().collect::<Vec<_>>(),
    })
}

// ---------------------------------------------------------------------------
// get_queue_parked_per_replica_v1
// ---------------------------------------------------------------------------

/// `GET /api/v1/analytics/queue-parked-replicas`, behind
/// `handle_queue_parked_replicas` (handlers/analytics.rs:191), which forwards
/// `from`, `to`, `queue`.
///
/// `{timeRange, bucketMinutes, series, replicas}`: `series` is the bucket
/// average of each (queue, node) minute gauge, ordered by bucket, queue,
/// hostname, worker id; `replicas` the distinct (hostname, workerId) pairs of
/// the window. The table is keyed by queue NAME, so rows of a deleted queue
/// still show; no queue list is needed.
pub fn parked_replicas_json(
    filters: &Map<String, Value>,
    now_us: i64,
    rows: &[ParkedRow],
) -> Value {
    let win = match Window::from_filters(filters, now_us) {
        Ok(w) => w,
        Err(e) => return sp_error(e),
    };
    let queue = filter_set(filters, "queue");

    // (bucket, queue, hostname, worker_id) -> (Σ parked_count, rows)
    let mut rolled: BTreeMap<(i64, &str, &str, i32), (i128, i128)> = BTreeMap::new();
    let mut replicas: BTreeSet<(&str, i32)> = BTreeSet::new();
    for r in rows {
        if !win.contains(r.bucket_us) || queue.as_deref().is_some_and(|f| f != r.queue) {
            continue;
        }
        let e = rolled
            .entry((win.floor(r.bucket_us), &r.queue, &r.hostname, r.worker_id))
            .or_default();
        e.0 += i128::from(r.parked_count);
        e.1 += 1;
        replicas.insert((&r.hostname, r.worker_id));
    }

    let series: Vec<Value> = rolled
        .iter()
        .map(|(&(bucket, queue, host, worker), &(sum, n))| {
            json!({
                "bucket": to_char_s(bucket),
                "queueName": queue,
                "hostname": host,
                "workerId": worker,
                "parkedCount": avg_2dp(sum, n),
            })
        })
        .collect();
    let replicas: Vec<Value> = replicas
        .into_iter()
        .map(|(host, worker)| json!({ "hostname": host, "workerId": worker }))
        .collect();

    json!({
        "timeRange": win.time_range(),
        "bucketMinutes": win.bucket_min,
        "series": series,
        "replicas": replicas,
    })
}

// ---------------------------------------------------------------------------
// get_workload_v1
// ---------------------------------------------------------------------------

/// `GET /api/v1/analytics/workload`, the body of `handle_workload`
/// (handlers/analytics.rs:153-187).
///
/// `Err(msg)` is the handler's 400: `msg` is `"bad groupBy"`, sent as
/// `{"error":"bad groupBy"}`, for a non-empty `groupBy` other than
/// `namespace` / `task` / `queue`. Every other outcome is `Ok` (an unparsable
/// timestamp is the procedure's `{"error": ...}` object, see the module
/// docs).
///
/// Filters: `from`, `to`, `groupBy` (default `namespace`), `namespace`,
/// `task`, `queue`. As in the handler, `namespace=` and `task=` with an EMPTY
/// value are filters for the empty namespace / task, while an empty `queue=`
/// is no filter. The filters pick the `rows`; `tenant` aggregates every queue
/// of the tenant regardless.
///
/// `queues` is every queue of the request tenant with its live figures (see
/// [`QueueNow`]). `retention` is not read: the procedure does not touch
/// `queen.retention_history`; the parameter only keeps the call shape of the
/// other views.
pub fn workload_json(
    filters: &Map<String, Value>,
    now_us: i64,
    rows: &[QueueRow],
    churn: &[ChurnRow],
    retention: &[RetentionRow],
    queues: &[QueueNow],
) -> Result<Value, String> {
    let _ = retention;
    let group_by = group_by_filter(filters)?.unwrap_or(GroupBy::Namespace);
    let namespace = filter_text(filters, "namespace");
    let task = filter_text(filters, "task");
    let queue = filter_set(filters, "queue");
    let win = match Window::from_filters(filters, now_us) {
        Ok(w) => w,
        Err(e) => return Ok(sp_error(e)),
    };

    // generate_series(v_from_bucket, v_to_bucket, bucket): the axis.
    let step = win.bucket_min * US_PER_MIN;
    let first = win.floor(win.from_us);
    let last = win.floor(win.to_us);
    let n_axis = if last < first {
        0
    } else {
        (i128::from(last) - i128::from(first)) / i128::from(step) + 1
    };
    if n_axis > MAX_AXIS_BUCKETS {
        return Ok(sp_error(format!(
            "workload window spans {n_axis} buckets (limit {MAX_AXIS_BUCKETS})"
        )));
    }
    let axis: Vec<i64> = (0..i64::try_from(n_axis).unwrap_or(0))
        .map(|i| first + i * step)
        .collect();

    // q_all / rolled: every queue of the tenant and its (bucket) rollups.
    let ix = QueueIndex::new(queues.iter().map(|q| &q.meta));
    let mut per_queue: Vec<BTreeMap<i64, Rolled>> = vec![BTreeMap::new(); ix.len()];
    for_each_lag_row(rows, churn, &win, &ix, |minute, q, r| {
        if let Some(m) = per_queue.get_mut(q) {
            m.entry(win.floor(minute)).or_default().add(r);
        }
    });
    let nows: Vec<&QueueNow> = ix.pos.iter().filter_map(|&p| queues.get(p)).collect();

    // q_scoped 'row': the filtered queues under their group key.
    let mut groups: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (q, m) in ix.metas.iter().enumerate() {
        let keep = namespace
            .as_deref()
            .is_none_or(|f| m.namespace.as_deref().unwrap_or("") == f)
            && task
                .as_deref()
                .is_none_or(|f| m.task.as_deref().unwrap_or("") == f)
            && queue.as_deref().is_none_or(|f| m.name == f);
        if keep {
            groups.entry(group_by.key(m)).or_default().push(q);
        }
    }

    let group = |members: &[usize]| group_base(members, &per_queue, &nows, &axis);
    let out_rows: Vec<Value> = groups
        .iter()
        .map(|(&key, members)| {
            let mut o = Map::new();
            o.insert("key".into(), Value::from(key));
            o.extend(group(members));
            if group_by == GroupBy::Queue {
                // MIN(ns) / MIN(tk) of the group: its one queue's.
                let ns = members
                    .iter()
                    .filter_map(|&q| ix.meta(q))
                    .map(|m| m.namespace.as_deref().unwrap_or(""))
                    .min()
                    .unwrap_or("");
                let tk = members
                    .iter()
                    .filter_map(|&q| ix.meta(q))
                    .map(|m| m.task.as_deref().unwrap_or(""))
                    .min()
                    .unwrap_or("");
                o.insert("namespace".into(), Value::from(ns));
                o.insert("task".into(), Value::from(tk));
            }
            Value::Object(o)
        })
        .collect();

    // q_scoped 'tenant': every queue, filters ignored; '{}' without queues.
    let tenant = if ix.len() == 0 {
        Value::Object(Map::new())
    } else {
        let all: Vec<usize> = (0..ix.len()).collect();
        Value::Object(group(&all))
    };

    Ok(json!({
        "timeRange": win.time_range(),
        "bucketMinutes": win.bucket_min,
        "groupBy": group_by.as_str(),
        "buckets": axis.iter().map(|&b| to_char_s(b)).collect::<Vec<_>>(),
        "rows": out_rows,
        "tenant": tenant,
    }))
}

/// The `base` object of one workload group (`built`, 019:1876-1921):
/// `{queues, window, series, now}` over the member queues.
fn group_base(
    members: &[usize],
    per_queue: &[BTreeMap<i64, Rolled>],
    nows: &[&QueueNow],
    axis: &[i64],
) -> Map<String, Value> {
    // sb: the group's per-bucket sums (019:1753-1767).
    let mut sb: BTreeMap<i64, Rolled> = BTreeMap::new();
    // win (019:1817-1841) over qwin (019:1792-1816).
    let mut w = Rolled::default();
    let mut parked_avg = Num::ZERO;
    let mut touched = 0i128;
    let mut active = 0i128;
    for &q in members {
        let Some(buckets) = per_queue.get(q) else {
            continue;
        };
        let mut qw = Rolled::default();
        for (&bucket, r) in buckets {
            let e = sb.entry(bucket).or_default();
            e.merge(r);
            // SUM(r.parked_sum / r.parked_n): each queue's bucket average.
            if let Some(avg) = Num::int(r.parked_sum).div(Num::int(r.rows)) {
                e.parked = e.parked.add(avg);
            }
            qw.merge(r);
        }
        // SUM(parked_sum / NULLIF(parked_n, 0)): the queue's window average.
        if let Some(avg) = Num::int(qw.parked_sum).div(Num::int(qw.rows)) {
            parked_avg = parked_avg.add(avg);
        }
        if !buckets.is_empty() {
            touched += 1;
        }
        if qw.push_msg + qw.pop > 0 {
            active += 1;
        }
        w.merge(&qw);
    }

    let mut s_push = Vec::with_capacity(axis.len());
    let mut s_pop = Vec::with_capacity(axis.len());
    let mut s_empty = Vec::with_capacity(axis.len());
    let mut s_fail = Vec::with_capacity(axis.len());
    let mut s_parked = Vec::with_capacity(axis.len());
    let mut s_avg = Vec::with_capacity(axis.len());
    let mut s_max = Vec::with_capacity(axis.len());
    for b in axis {
        match sb.get(b) {
            // CROSS JOIN the axis: an untouched bucket is a null hole.
            None => {
                for s in [
                    &mut s_push,
                    &mut s_pop,
                    &mut s_empty,
                    &mut s_fail,
                    &mut s_parked,
                    &mut s_avg,
                    &mut s_max,
                ] {
                    s.push(Value::Null);
                }
            }
            Some(e) => {
                s_push.push(int(e.push_msg));
                s_pop.push(int(e.pop));
                s_empty.push(int(e.pop_empty));
                s_fail.push(int(e.ack_fail));
                s_parked.push(e.parked.round(2).to_json());
                s_avg.push(rounded_quotient(e.lag_weight, e.pop));
                s_max.push(opt_int(e.max_lag_popped));
            }
        }
    }

    let (mut pending, mut processing, mut dead, mut bytes, mut parts, mut cgs) =
        (0i128, 0i128, 0i128, 0i128, 0i128, 0i128);
    let (mut without_group, mut pending_without_group) = (0i128, 0i128);
    for &q in members {
        let Some(n) = nows.get(q) else {
            continue;
        };
        pending += i128::from(n.pending);
        processing += i128::from(n.processing);
        dead += i128::from(n.dead_letter);
        bytes += i128::from(n.retained_bytes);
        parts += i128::from(n.partitions);
        cgs += i128::from(n.groups);
        if n.groups == 0 {
            without_group += 1;
            pending_without_group += i128::from(n.pending);
        }
    }

    let mut o = Map::new();
    o.insert("queues".into(), int(members.len() as i128));
    o.insert(
        "window".into(),
        json!({
            "pushMessages": int(w.push_msg),
            "pushRequests": int(w.push_req),
            "popMessages": int(w.pop),
            "popEmpty": int(w.pop_empty),
            "ackRequests": int(w.ack_req),
            "ackSuccess": int(w.ack_ok),
            "ackFailed": int(w.ack_fail),
            "transactions": int(w.trx),
            "conflated": int(w.conflated),
            "partitionsCreated": int(w.created),
            "partitionsDeleted": int(w.deleted),
            "parkedAvg": parked_avg.round(2).to_json(),
            "avgLagMs": rounded_quotient(w.lag_weight, w.pop),
            "maxLagMs": opt_int(w.max_lag_popped),
        }),
    );
    o.insert(
        "series".into(),
        json!({
            "push": s_push,
            "pop": s_pop,
            "popEmpty": s_empty,
            "ackFailed": s_fail,
            "parked": s_parked,
            "avgLagMs": s_avg,
            "maxLagMs": s_max,
        }),
    );
    o.insert(
        "now".into(),
        json!({
            "pending": int(pending),
            "processing": int(processing),
            "deadLetter": int(dead),
            "retainedBytes": int(bytes),
            "partitions": int(parts),
            "groups": int(cgs),
            "queuesWithoutGroup": int(without_group),
            "pendingWithoutGroup": int(pending_without_group),
            "queuesTouched": int(touched),
            "queuesActive": int(active),
        }),
    );
    o
}

// ---------------------------------------------------------------------------
// get_retention_timeseries_v1
// ---------------------------------------------------------------------------

/// `GET /api/v1/analytics/retention`, the body of `handle_retention`
/// (handlers/analytics.rs:212-235), which forwards `from`, `to`, `queue`,
/// `groupBy`.
///
/// `Err(msg)` is the handler's 400 for a non-empty `groupBy` outside
/// `namespace` / `task` / `queue` (`msg` = `"bad groupBy"`). Otherwise
/// `{timeRange, bucketMinutes, series, totals}`, plus `rows` exactly when a
/// `groupBy` is given (per-group totals ordered by `totalMsgs` desc, then key).
///
/// Each [`RetentionRow`] is one `queen.retention_history` event: its three
/// counters feed `retentionMsgs` / `completedRetentionMsgs` / `evictionMsgs`,
/// their sum `totalMsgs`, and it counts once in `eventCount`. A row whose
/// queue no longer exists stays in `series` / `totals` without a `queue`
/// filter and is left out of `rows`.
pub fn retention_json(
    filters: &Map<String, Value>,
    now_us: i64,
    rows: &[RetentionRow],
    queues: &[QueueMeta],
) -> Result<Value, String> {
    let group_by = group_by_filter(filters)?;
    let win = match Window::from_filters(filters, now_us) {
        Ok(w) => w,
        Err(e) => return Ok(sp_error(e)),
    };
    let queue = filter_set(filters, "queue");
    let ix = QueueIndex::new(queues);

    let mut series: BTreeMap<i64, Retained> = BTreeMap::new();
    let mut per_group: HashMap<&str, (Retained, BTreeSet<usize>)> = HashMap::new();
    for r in rows {
        if !win.contains(r.at_us) {
            continue;
        }
        let q = ix.resolve_at(&r.queue, r.at_us);
        // (v_queue IS NULL OR q.name = v_queue): an unresolvable row only
        // passes without a queue filter.
        if let Some(f) = queue.as_deref() {
            if q.is_none_or(|i| ix.name(i) != f) {
                continue;
            }
        }
        series.entry(win.floor(r.at_us)).or_default().add(r);
        if let (Some(g), Some(i)) = (group_by, q) {
            if let Some(m) = ix.meta(i) {
                let e = per_group.entry(g.key(m)).or_default();
                e.0.add(r);
                e.1.insert(i);
            }
        }
    }

    let totals = series.values().fold(Retained::default(), |mut acc, t| {
        acc.merge(t);
        acc
    });
    let series: Vec<Value> = series
        .iter()
        .map(|(&bucket, t)| {
            let mut o = t.to_json();
            o.insert("bucket".into(), Value::from(to_char_s(bucket)));
            Value::Object(o)
        })
        .collect();

    let mut out = Map::new();
    out.insert("timeRange".into(), win.time_range());
    out.insert("bucketMinutes".into(), Value::from(win.bucket_min));
    out.insert("series".into(), Value::Array(series));
    out.insert("totals".into(), Value::Object(totals.to_json()));
    if group_by.is_some() {
        let mut groups: Vec<(&str, (Retained, BTreeSet<usize>))> = per_group.into_iter().collect();
        groups.sort_by(|a, b| b.1 .0.total.cmp(&a.1 .0.total).then_with(|| a.0.cmp(b.0)));
        let rows: Vec<Value> = groups
            .into_iter()
            .map(|(key, (t, ids))| {
                json!({
                    "key": key,
                    "queues": int(ids.len() as i128),
                    "totals": Value::Object(t.to_json()),
                })
            })
            .collect();
        out.insert("rows".into(), Value::Array(rows));
    }
    Ok(Value::Object(out))
}

/// The per-bucket / per-group sums of `queen.retention_history`.
#[derive(Clone, Copy, Debug, Default)]
struct Retained {
    retention: i128,
    completed: i128,
    eviction: i128,
    total: i128,
    events: i128,
}

impl Retained {
    fn add(&mut self, r: &RetentionRow) {
        let (a, b, c) = (
            i128::from(r.retention_msgs),
            i128::from(r.completed_retention_msgs),
            i128::from(r.eviction_msgs),
        );
        self.retention += a;
        self.completed += b;
        self.eviction += c;
        self.total += a + b + c;
        self.events += 1;
    }

    fn merge(&mut self, o: &Retained) {
        self.retention += o.retention;
        self.completed += o.completed;
        self.eviction += o.eviction;
        self.total += o.total;
        self.events += o.events;
    }

    fn to_json(self) -> Map<String, Value> {
        let mut o = Map::new();
        o.insert("retentionMsgs".into(), int(self.retention));
        o.insert("completedRetentionMsgs".into(), int(self.completed));
        o.insert("evictionMsgs".into(), int(self.eviction));
        o.insert("totalMsgs".into(), int(self.total));
        o.insert("eventCount".into(), int(self.events));
        o
    }
}

// ---------------------------------------------------------------------------
// get_queue_lag_v1
// ---------------------------------------------------------------------------

/// `GET /api/v1/analytics/queue-lag`, behind `handle_queue_lag`
/// (handlers/analytics.rs:112), which passes `from`, `to`, `queue`
/// positionally, an empty value as absent.
///
/// A BARE array of `{queueName, popCount, avgLagMs, maxLagMs, bucketMinutes,
/// bucketTime}`, ordered by bucket DESC then queue. The width rule is the
/// status rule on the UNROUNDED span (`EXTRACT(EPOCH ...) / 60 <= 60`), so a
/// 60 min 20 s range is 5-minute here and 1-minute on the other views (which
/// round it to 60 minutes first).
pub fn queue_lag_json(
    from: Option<&str>,
    to: Option<&str>,
    queue: Option<&str>,
    now_us: i64,
    rows: &[QueueRow],
    churn: &[ChurnRow],
    queues: &[QueueMeta],
) -> Value {
    let bound = |v: Option<&str>, default: i64| match v.filter(|s| !s.is_empty()) {
        Some(s) => parse_timestamptz(s),
        None => Ok(default),
    };
    let (from_us, to_us) = match (
        bound(from, now_us.saturating_sub(US_PER_HOUR)),
        bound(to, now_us),
    ) {
        (Ok(f), Ok(t)) => (f, t),
        (Err(e), _) | (_, Err(e)) => return sp_error(e),
    };
    let win = Window {
        from_us,
        to_us,
        bucket_min: lag_bucket_minutes(from_us, to_us),
    };
    let queue = queue.filter(|s| !s.is_empty());
    let ix = QueueIndex::new(queues);

    let mut rolled: BTreeMap<(i64, &str), Rolled> = BTreeMap::new();
    for_each_lag_row(rows, churn, &win, &ix, |minute, q, r| {
        let name = ix.name(q);
        if queue.is_some_and(|f| f != name) {
            return;
        }
        rolled.entry((win.floor(minute), name)).or_default().add(r);
    });

    let mut points: Vec<(&(i64, &str), &Rolled)> = rolled.iter().collect();
    points.sort_by(|a, b| b.0 .0.cmp(&a.0 .0).then_with(|| a.0 .1.cmp(b.0 .1)));
    Value::Array(
        points
            .into_iter()
            .map(|(&(bucket, name), r)| {
                let popped = r.pop > 0;
                json!({
                    "queueName": name,
                    "popCount": int(r.pop),
                    "avgLagMs": if popped { quotient(r.lag_weight, r.pop) } else { Value::Null },
                    "maxLagMs": if popped { opt_int(r.max_lag_all) } else { Value::Null },
                    "bucketMinutes": win.bucket_min,
                    "bucketTime": to_char_s(bucket),
                })
            })
            .collect(),
    )
}

// ---------------------------------------------------------------------------
// queue_lag_metrics rows
// ---------------------------------------------------------------------------

/// One `queen.queue_lag_metrics` row as the procedures read it.
#[derive(Clone, Copy, Debug, Default)]
struct LagRow {
    push_req: i64,
    push_msg: i64,
    pop: i64,
    pop_empty: i64,
    ack_req: i64,
    ack_ok: i64,
    ack_fail: i64,
    trx: i64,
    conflated: i64,
    created: i64,
    deleted: i64,
    avg_lag: i64,
    max_lag: i64,
    parked: i64,
}

impl LagRow {
    fn of(r: &QueueRow) -> LagRow {
        LagRow {
            push_req: r.push_requests,
            push_msg: r.push_messages,
            pop: r.pop_messages,
            pop_empty: r.pop_empty,
            ack_req: r.ack_requests,
            ack_ok: r.ack_success,
            ack_fail: r.ack_failed,
            trx: r.transactions,
            conflated: r.conflated,
            created: 0,
            deleted: 0,
            avg_lag: r.avg_lag_ms,
            max_lag: r.max_lag_ms,
            parked: i64::from(r.parked_count),
        }
    }
}

/// Visit the `queen.queue_lag_metrics` rows a procedure's
/// `WHERE bucket_time >= from AND bucket_time <= to` and `JOIN queen.queues`
/// keep: every [`QueueRow`] of the window whose queue resolves, with the churn
/// of its minute folded in (the partition triggers upsert into the same
/// `(bucket_time, queue_id)` row), then every churn-only minute as a row of
/// its own (counters and `parked_count` 0, as the trigger inserts it).
/// `f(minute_us, queue_index, row)`.
fn for_each_lag_row(
    rows: &[QueueRow],
    churn: &[ChurnRow],
    win: &Window,
    ix: &QueueIndex,
    mut f: impl FnMut(i64, usize, &LagRow),
) {
    let mut lifecycle: HashMap<(i64, usize), (i64, i64)> = HashMap::new();
    for c in churn {
        if !win.contains(c.bucket_us) {
            continue;
        }
        if let Some(q) = ix.resolve_bucket(&c.queue, c.bucket_us) {
            let e = lifecycle.entry((c.bucket_us, q)).or_default();
            e.0 = e.0.saturating_add(c.created);
            e.1 = e.1.saturating_add(c.deleted);
        }
    }
    for r in rows {
        if !win.contains(r.bucket_us) {
            continue;
        }
        let Some(q) = ix.resolve_bucket(&r.queue, r.bucket_us) else {
            continue;
        };
        let mut row = LagRow::of(r);
        if let Some((created, deleted)) = lifecycle.remove(&(r.bucket_us, q)) {
            row.created = created;
            row.deleted = deleted;
        }
        f(r.bucket_us, q, &row);
    }
    for ((minute, q), (created, deleted)) in lifecycle {
        let row = LagRow {
            created,
            deleted,
            ..LagRow::default()
        };
        f(minute, q, &row);
    }
}

/// Sums of `queue_lag_metrics` rows over one rollup cell (a bucket of a queue,
/// or of a group, or a whole window).
#[derive(Clone, Copy, Debug, Default)]
struct Rolled {
    push_req: i128,
    push_msg: i128,
    pop: i128,
    pop_empty: i128,
    ack_req: i128,
    ack_ok: i128,
    ack_fail: i128,
    trx: i128,
    conflated: i128,
    created: i128,
    deleted: i128,
    /// `SUM(avg_lag_ms * pop_count)`.
    lag_weight: i128,
    /// `MAX(max_lag_ms)` over every row (`get_queue_ops_v1`, `get_queue_lag_v1`).
    max_lag_all: Option<i64>,
    /// `MAX(max_lag_ms) FILTER (WHERE pop_count > 0)` (`get_workload_v1`).
    max_lag_popped: Option<i64>,
    /// `SUM(COALESCE(parked_count, 0))` and `COUNT(*)`.
    parked_sum: i128,
    rows: i128,
    /// Workload `sb.parked`: the sum of the member queues' bucket averages.
    parked: Num,
}

impl Rolled {
    fn add(&mut self, r: &LagRow) {
        self.push_req += i128::from(r.push_req);
        self.push_msg += i128::from(r.push_msg);
        self.pop += i128::from(r.pop);
        self.pop_empty += i128::from(r.pop_empty);
        self.ack_req += i128::from(r.ack_req);
        self.ack_ok += i128::from(r.ack_ok);
        self.ack_fail += i128::from(r.ack_fail);
        self.trx += i128::from(r.trx);
        self.conflated += i128::from(r.conflated);
        self.created += i128::from(r.created);
        self.deleted += i128::from(r.deleted);
        self.lag_weight += i128::from(r.avg_lag) * i128::from(r.pop);
        self.max_lag_all = max_opt(self.max_lag_all, Some(r.max_lag));
        if r.pop > 0 {
            self.max_lag_popped = max_opt(self.max_lag_popped, Some(r.max_lag));
        }
        self.parked_sum += i128::from(r.parked);
        self.rows += 1;
    }

    /// Add another cell's sums (`parked` excluded: it is built by the caller).
    fn merge(&mut self, o: &Rolled) {
        self.push_req += o.push_req;
        self.push_msg += o.push_msg;
        self.pop += o.pop;
        self.pop_empty += o.pop_empty;
        self.ack_req += o.ack_req;
        self.ack_ok += o.ack_ok;
        self.ack_fail += o.ack_fail;
        self.trx += o.trx;
        self.conflated += o.conflated;
        self.created += o.created;
        self.deleted += o.deleted;
        self.lag_weight += o.lag_weight;
        self.max_lag_all = max_opt(self.max_lag_all, o.max_lag_all);
        self.max_lag_popped = max_opt(self.max_lag_popped, o.max_lag_popped);
        self.parked_sum += o.parked_sum;
        self.rows += o.rows;
    }
}

fn max_opt(a: Option<i64>, b: Option<i64>) -> Option<i64> {
    match (a, b) {
        (Some(x), Some(y)) => Some(x.max(y)),
        (x, None) => x,
        (None, y) => y,
    }
}

/// The tenant's queues by name. A second queue of the same name is ignored
/// (`queen.queues` is unique on (tenant, name)).
struct QueueIndex<'a> {
    metas: Vec<&'a QueueMeta>,
    /// Position of each kept queue in the caller's slice.
    pos: Vec<usize>,
    by_name: HashMap<&'a str, usize>,
}

impl<'a> QueueIndex<'a> {
    fn new(metas: impl IntoIterator<Item = &'a QueueMeta>) -> QueueIndex<'a> {
        let mut ix = QueueIndex {
            metas: Vec::new(),
            pos: Vec::new(),
            by_name: HashMap::new(),
        };
        for (p, m) in metas.into_iter().enumerate() {
            if let std::collections::hash_map::Entry::Vacant(v) = ix.by_name.entry(m.name.as_str())
            {
                v.insert(ix.metas.len());
                ix.metas.push(m);
                ix.pos.push(p);
            }
        }
        ix
    }

    fn len(&self) -> usize {
        self.metas.len()
    }

    fn meta(&self, q: usize) -> Option<&'a QueueMeta> {
        self.metas.get(q).copied()
    }

    fn name(&self, q: usize) -> &'a str {
        self.meta(q).map_or("", |m| m.name.as_str())
    }

    /// The queue a minute row of `name` belongs to: the current queue of that
    /// name, unless the minute precedes its creation minute.
    fn resolve_bucket(&self, name: &str, bucket_us: i64) -> Option<usize> {
        let &q = self.by_name.get(name)?;
        let m = self.meta(q)?;
        (bucket_us >= trunc_us(m.created_us, US_PER_MIN)).then_some(q)
    }

    /// The queue an event of `name` at `at_us` belongs to.
    fn resolve_at(&self, name: &str, at_us: i64) -> Option<usize> {
        let &q = self.by_name.get(name)?;
        let m = self.meta(q)?;
        (at_us >= m.created_us).then_some(q)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GroupBy {
    Namespace,
    Task,
    Queue,
}

impl GroupBy {
    fn parse(s: &str) -> Option<GroupBy> {
        match s {
            "namespace" => Some(GroupBy::Namespace),
            "task" => Some(GroupBy::Task),
            "queue" => Some(GroupBy::Queue),
            _ => None,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            GroupBy::Namespace => "namespace",
            GroupBy::Task => "task",
            GroupBy::Queue => "queue",
        }
    }

    /// `CASE v_group_by WHEN 'queue' THEN q.name WHEN 'task' THEN
    /// COALESCE(q.task, '') ELSE COALESCE(q.namespace, '') END`.
    fn key(self, m: &QueueMeta) -> &str {
        match self {
            GroupBy::Queue => &m.name,
            GroupBy::Task => m.task.as_deref().unwrap_or(""),
            GroupBy::Namespace => m.namespace.as_deref().unwrap_or(""),
        }
    }
}

/// The handlers' gate: a non-empty `groupBy` outside namespace|task|queue is a
/// 400 before the procedure runs; an empty one is absent.
fn group_by_filter(filters: &Map<String, Value>) -> Result<Option<GroupBy>, String> {
    match filter_set(filters, "groupBy") {
        None => Ok(None),
        Some(g) => GroupBy::parse(&g)
            .map(Some)
            .ok_or_else(|| BAD_GROUP_BY.to_string()),
    }
}

// ---------------------------------------------------------------------------
// Filters, window, buckets, timestamps
// ---------------------------------------------------------------------------

/// `p_filters->>key`: absent or JSON null is `None`, a string is itself, any
/// other JSON value its text.
fn filter_text(filters: &Map<String, Value>, key: &str) -> Option<String> {
    match filters.get(key)? {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        other => Some(other.to_string()),
    }
}

/// A filter as `filters_from_query` forwards it: an empty value is absent.
fn filter_set(filters: &Map<String, Value>, key: &str) -> Option<String> {
    filter_text(filters, key).filter(|s| !s.is_empty())
}

/// The `{"error": ...}` object a raising procedure becomes in the handler.
fn sp_error(msg: String) -> Value {
    let mut o = Map::new();
    o.insert("error".into(), Value::String(msg));
    Value::Object(o)
}

/// `'…'::timestamptz` for the ISO-8601 forms the dashboard sends.
fn parse_timestamptz(s: &str) -> Result<i64, String> {
    parse_ts_us(s)
        .ok_or_else(|| format!("invalid input syntax for type timestamp with time zone: \"{s}\""))
}

/// A request window and its bucket width in minutes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Window {
    from_us: i64,
    to_us: i64,
    bucket_min: i64,
}

impl Window {
    /// `COALESCE((p_filters->>'from')::timestamptz, NOW() - INTERVAL '1 hour')`,
    /// `COALESCE((p_filters->>'to')::timestamptz, NOW())` and the status width.
    fn from_filters(filters: &Map<String, Value>, now_us: i64) -> Result<Window, String> {
        let from_us = match filter_set(filters, "from") {
            Some(s) => parse_timestamptz(&s)?,
            None => now_us.saturating_sub(US_PER_HOUR),
        };
        let to_us = match filter_set(filters, "to") {
            Some(s) => parse_timestamptz(&s)?,
            None => now_us,
        };
        Ok(Window {
            from_us,
            to_us,
            bucket_min: status_bucket_minutes(from_us, to_us),
        })
    }

    /// `bucket_time >= from AND bucket_time <= to`.
    fn contains(&self, t_us: i64) -> bool {
        t_us >= self.from_us && t_us <= self.to_us
    }

    fn floor(&self, t_us: i64) -> i64 {
        floor_bucket(t_us, self.bucket_min)
    }

    /// `{from, to}` as `to_char(... 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`.
    fn time_range(&self) -> Value {
        json!({ "from": to_char_ms(self.from_us), "to": to_char_ms(self.to_us) })
    }

    /// `ROUND(n::numeric / (v_bucket_minutes * 60), 2)`.
    fn per_second(&self, n: i128) -> Value {
        Num::int(n)
            .div(Num::int(i128::from(self.bucket_min) * 60))
            .map_or(Value::Null, |v| v.round(2).to_json())
    }
}

/// The width rule of `get_status_v3` (019:740-748), shared by every view but
/// the lag series: `v_duration_minutes INTEGER := EXTRACT(EPOCH FROM (to -
/// from)) / 60` rounds the numeric minutes half away from zero, then
/// `<= 60 → 1, <= 360 → 5, <= 1440 → 15, <= 10080 → 60, else 360`.
fn status_bucket_minutes(from_us: i64, to_us: i64) -> i64 {
    let span = i128::from(to_us) - i128::from(from_us);
    let minutes = round_half_away(span, i128::from(US_PER_MIN));
    width_for(|limit| minutes <= limit)
}

/// `get_queue_lag_v1`'s copy of the rule (019:274-280) compares the unrounded
/// `EXTRACT(EPOCH FROM (p_to - p_from)) / 60`.
fn lag_bucket_minutes(from_us: i64, to_us: i64) -> i64 {
    let span = i128::from(to_us) - i128::from(from_us);
    width_for(|limit| span <= limit * i128::from(US_PER_MIN))
}

fn width_for(within: impl Fn(i128) -> bool) -> i64 {
    if within(60) {
        1
    } else if within(360) {
        5
    } else if within(1440) {
        15
    } else if within(10080) {
        60
    } else {
        360
    }
}

/// The bucket expression of every procedure ported here, in UTC:
/// `date_trunc('minute', t) - (EXTRACT(minute FROM t)::integer % b) * INTERVAL
/// '1 minute'`. The minute field is below 60, so `b = 60` AND `b = 360` both
/// floor to the hour: a range over 7 days reports `bucketMinutes: 360` over
/// hourly buckets (and the workload axis, stepping six hours, keeps one hour
/// in six). Reproduced, not fixed.
fn floor_bucket(t_us: i64, b_min: i64) -> i64 {
    let minute = trunc_us(t_us, US_PER_MIN);
    let minute_of_hour = minute.div_euclid(US_PER_MIN).rem_euclid(60);
    minute.saturating_sub(minute_of_hour % b_min.max(1) * US_PER_MIN)
}

/// `to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS"Z"')`.
fn to_char_s(us: i64) -> String {
    let iso = iso_us(us);
    match iso.find('.').and_then(|dot| iso.get(..dot)) {
        Some(head) => format!("{head}Z"),
        None => iso,
    }
}

/// `to_char(t AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')`: the
/// milliseconds truncated, not rounded.
fn to_char_ms(us: i64) -> String {
    let iso = iso_us(us);
    match iso.find('.').and_then(|dot| iso.get(..dot + 4)) {
        Some(head) => format!("{head}Z"),
        None => iso,
    }
}

// ---------------------------------------------------------------------------
// numeric
// ---------------------------------------------------------------------------

/// A JSON integer (`SUM(bigint)`, `COUNT(*)`, …).
fn int(n: i128) -> Value {
    Num::int(n).to_json()
}

fn opt_int(v: Option<i64>) -> Value {
    v.map_or(Value::Null, Value::from)
}

/// `SUM(a) / SUM(b)` of two integer sums: the full `numeric` quotient.
fn quotient(a: i128, b: i128) -> Value {
    Num::int(a)
        .div(Num::int(b))
        .map_or(Value::Null, Num::to_json)
}

/// `ROUND(SUM(a) / SUM(b))::bigint`, `null` when `b` is 0 (the procedures'
/// `CASE WHEN SUM(pop_msg) > 0 ... END`).
fn rounded_quotient(a: i128, b: i128) -> Value {
    if b <= 0 {
        return Value::Null;
    }
    Num::int(a)
        .div(Num::int(b))
        .map_or(Value::Null, |v| v.round(0).to_json())
}

/// `ROUND(AVG(integer), 2)` from its sum and count (`int8_avg` divides as
/// `numeric`).
fn avg_2dp(sum: i128, n: i128) -> Value {
    Num::int(sum)
        .div(Num::int(n))
        .map_or(Value::Null, |v| v.round(2).to_json())
}

/// A fixed-point decimal: the value `m / 10^s`, `s` being its display scale.
///
/// Only what these views do to their integer sums is modelled, each
/// with numeric.c's rule: [`Num::div`] (`numeric_div`: the scale
/// `select_div_scale` picks, rounded half away from zero), [`Num::add`] (exact,
/// the larger scale) and [`Num::round`] (`round(numeric, int)`, half away from
/// zero). [`Num::to_json`] is the number a JSON parser reads from
/// `numeric_out`'s text.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Num {
    m: i128,
    s: u32,
}

impl Num {
    const ZERO: Num = Num { m: 0, s: 0 };

    fn int(v: i128) -> Num {
        Num { m: v, s: 0 }
    }

    /// `numeric_div`; `None` for a zero divisor (undefined; every call site
    /// guards its division first).
    fn div(self, d: Num) -> Option<Num> {
        if d.m == 0 {
            return None;
        }
        // select_div_scale: at least 16 significant digits, never fewer
        // fractional digits than either input shows.
        let (w1, f1) = self.lead();
        let (w2, f2) = d.lead();
        let mut qweight = w1 - w2;
        if f1 <= f2 {
            qweight -= 1;
        }
        let rscale = (16 - 4 * qweight)
            .max(i64::from(self.s))
            .max(i64::from(d.s))
            .clamp(0, 1000);
        // |self| / |d| = (|m1| · 10^s2) / (|m2| · 10^s1)
        let num = self.m.unsigned_abs().checked_mul(pow10(d.s)?)?;
        let den = d.m.unsigned_abs().checked_mul(pow10(self.s)?)?;
        let neg = (self.m < 0) != (d.m < 0);
        // The quotients formed here carry about 20 significant digits; the
        // loop only trades scale for range if an operand ever would not fit.
        let mut s = u32::try_from(rscale).unwrap_or(0);
        loop {
            if let Some(m) = div_round(num, den, s).and_then(|q| i128::try_from(q).ok()) {
                return Some(Num {
                    m: if neg { -m } else { m },
                    s,
                });
            }
            if s == 0 {
                return None;
            }
            s -= 1;
        }
    }

    /// `(weight, first digit)` of the value in numeric.c's base-10000 digits,
    /// as `select_div_scale` reads them; `(0, 0)` for zero.
    fn lead(self) -> (i64, u128) {
        let v = self.m.unsigned_abs();
        if v == 0 {
            return (0, 0);
        }
        let mut digits = 1i64;
        let mut x = v;
        while x >= 10 {
            x /= 10;
            digits += 1;
        }
        let exp = digits - 1 - i64::from(self.s);
        let weight = exp.div_euclid(4);
        let shift = i64::from(self.s) + 4 * weight;
        let first = if shift >= 0 {
            u32::try_from(shift)
                .ok()
                .and_then(pow10)
                .map_or(0, |p| v / p)
        } else {
            u32::try_from(-shift)
                .ok()
                .and_then(pow10)
                .and_then(|p| v.checked_mul(p))
                .unwrap_or(u128::MAX)
        };
        (weight, first)
    }

    /// Exact addition at the larger scale.
    fn add(self, o: Num) -> Num {
        let mut s = self.s.max(o.s);
        loop {
            if let (Some(a), Some(b)) = (self.at_scale(s), o.at_scale(s)) {
                match a.checked_add(b) {
                    Some(m) => return Num { m, s },
                    None if s == 0 => {
                        return Num {
                            m: a.saturating_add(b),
                            s,
                        }
                    }
                    None => {}
                }
            } else if s == 0 {
                return self;
            }
            s -= 1;
        }
    }

    /// `round(numeric, k)`: half away from zero, result scale `k`.
    fn round(self, k: u32) -> Num {
        match self.at_scale(k) {
            Some(m) => Num { m, s: k },
            None => self,
        }
    }

    /// The mantissa at scale `s`: exact when `s >= self.s`, rounded half away
    /// from zero otherwise.
    fn at_scale(self, s: u32) -> Option<i128> {
        if s >= self.s {
            let p = i128::try_from(pow10(s - self.s)?).ok()?;
            self.m.checked_mul(p)
        } else {
            let p = i128::try_from(pow10(self.s - s)?).ok()?;
            Some(round_half_away(self.m, p))
        }
    }

    /// `numeric_out`: the value with exactly `s` fractional digits.
    fn text(self) -> String {
        let digits = self.m.unsigned_abs().to_string();
        let sign = if self.m < 0 { "-" } else { "" };
        let s = self.s as usize;
        if s == 0 {
            format!("{sign}{digits}")
        } else if digits.len() > s {
            let (i, f) = digits.split_at(digits.len() - s);
            format!("{sign}{i}.{f}")
        } else {
            format!("{sign}0.{}{digits}", "0".repeat(s - digits.len()))
        }
    }

    /// The JSON number a parser reads from [`Num::text`]: an integer for a
    /// scale-0 value, else the nearest `f64`.
    fn to_json(self) -> Value {
        if self.s == 0 {
            if let Ok(i) = i64::try_from(self.m) {
                return Value::from(i);
            }
        }
        self.text()
            .parse::<f64>()
            .ok()
            .and_then(serde_json::Number::from_f64)
            .map_or(Value::Null, Value::Number)
    }
}

fn pow10(n: u32) -> Option<u128> {
    10u128.checked_pow(n)
}

/// `num / den` with `s` fractional digits, rounded half away from zero.
fn div_round(num: u128, den: u128, s: u32) -> Option<u128> {
    if den == 0 {
        return None;
    }
    let mut q = num / den;
    let mut r = num % den;
    for _ in 0..s {
        r = r.checked_mul(10)?;
        q = q.checked_mul(10)?.checked_add(r / den)?;
        r %= den;
    }
    if r >= den - r {
        q = q.checked_add(1)?;
    }
    Some(q)
}

/// `n / d` rounded half away from zero (`d > 0`).
fn round_half_away(n: i128, d: i128) -> i128 {
    if d <= 0 {
        return n;
    }
    let q = n / d;
    let r = (n % d).unsigned_abs();
    let du = d.unsigned_abs();
    if r >= du - r {
        if n < 0 {
            q - 1
        } else {
            q + 1
        }
    } else {
        q
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ts(s: &str) -> i64 {
        parse_ts_us(s).expect("test timestamp")
    }

    /// `NOW()` of every test.
    fn now() -> i64 {
        ts("2026-09-24T10:30:45.123456Z")
    }

    /// The default window's `timeRange`: `[now - 1 h, now]`, milliseconds truncated.
    fn last_hour() -> Value {
        json!({ "from": "2026-09-24T09:30:45.123Z", "to": "2026-09-24T10:30:45.123Z" })
    }

    fn filters(pairs: &[(&str, &str)]) -> Map<String, Value> {
        pairs
            .iter()
            .map(|&(k, v)| (k.to_string(), Value::from(v)))
            .collect()
    }

    fn with(base: &Map<String, Value>, pairs: &[(&str, &str)]) -> Map<String, Value> {
        let mut f = base.clone();
        f.extend(filters(pairs));
        f
    }

    /// The number a JSON parser reads from a decimal `numeric` text.
    fn pg(text: &str) -> Value {
        Value::from(text.parse::<f64>().expect("numeric text"))
    }

    fn meta(name: &str, namespace: Option<&str>, task: Option<&str>) -> QueueMeta {
        QueueMeta {
            name: name.into(),
            namespace: namespace.map(Into::into),
            task: task.map(Into::into),
            created_us: 0,
        }
    }

    fn qrow(minute: &str, queue: &str) -> QueueRow {
        QueueRow {
            bucket_us: ts(minute),
            tenant: "t".into(),
            queue: queue.into(),
            ..Default::default()
        }
    }

    fn churn(minute: &str, queue: &str, created: i64, deleted: i64) -> ChurnRow {
        ChurnRow {
            bucket_us: ts(minute),
            tenant: "t".into(),
            queue: queue.into(),
            created,
            deleted,
        }
    }

    fn strs<'a>(v: &'a Value, field: &str) -> Vec<&'a str> {
        v.as_array()
            .map(|a| a.iter().filter_map(|e| e[field].as_str()).collect())
            .unwrap_or_default()
    }

    // -- numeric ------------------------------------------------------------

    fn div_text(a: i128, b: i128) -> String {
        Num::int(a)
            .div(Num::int(b))
            .map(Num::text)
            .unwrap_or_default()
    }

    #[test]
    fn numeric_division_keeps_the_wire_text() {
        // select_div_scale: at least 16 significant digits, half away from zero.
        assert_eq!(div_text(10, 3), "3.3333333333333333");
        assert_eq!(div_text(1, 3), "0.33333333333333333333");
        assert_eq!(div_text(2, 3), "0.66666666666666666667");
        assert_eq!(div_text(100, 7), "14.2857142857142857");
        assert_eq!(div_text(123_456, 100), "1234.5600000000000000");
        assert_eq!(div_text(123_456_789, 10), "12345678.900000000000");
        assert_eq!(div_text(6000, 70), "85.7142857142857143");
        assert_eq!(div_text(-7, 2), "-3.5000000000000000");
        // AVG(integer) is numeric_div(sum, count): avg(1, 2) and avg(1).
        assert_eq!(div_text(3, 2), "1.5000000000000000");
        assert_eq!(div_text(1, 1), "1.00000000000000000000");
        assert_eq!(div_text(0, 60), "0.00000000000000000000");
        assert!(Num::int(1).div(Num::ZERO).is_none());
    }

    #[test]
    fn numeric_round_add_and_json() {
        let n = |m, s| Num { m, s };
        assert_eq!(n(2345, 3).round(2).text(), "2.35");
        assert_eq!(n(-2345, 3).round(2).text(), "-2.35");
        assert_eq!(n(25, 1).round(0).text(), "3");
        assert_eq!(n(-25, 1).round(0).text(), "-3");
        assert_eq!(n(15, 1).round(0).text(), "2");
        assert_eq!(n(4, 3).round(2).text(), "0.00");
        assert_eq!(Num::int(3).round(2).text(), "3.00");
        // Sums are exact at the larger scale.
        let third = Num::int(1).div(Num::int(3)).expect("quotient");
        let sixth = Num::int(1).div(Num::int(6)).expect("quotient");
        assert_eq!(third.add(sixth).text(), "0.50000000000000000000");
        assert_eq!(Num::int(2).add(n(5, 1)).text(), "2.5");
        // Integers stay integers; fractions are the parsed text.
        assert_eq!(Num::int(7).to_json(), json!(7));
        assert_eq!(n(400, 2).to_json(), json!(4.0));
        assert_eq!(quotient(6000, 70), pg("85.7142857142857143"));
        assert_eq!(rounded_quotient(6000, 70), json!(86));
        assert_eq!(rounded_quotient(5, 10), json!(1));
        assert_eq!(rounded_quotient(1, 0), Value::Null);
        assert_eq!(avg_2dp(10, 3), pg("3.33"));
    }

    // -- width, buckets, timestamps -----------------------------------------

    #[test]
    fn bucket_width_rules() {
        let from = ts("2026-09-24T00:00:00Z");
        let status = |secs: i64| status_bucket_minutes(from, from + secs * US_PER_SEC);
        assert_eq!(status(60 * 60), 1);
        assert_eq!(status(60 * 60 + 29), 1); // 60.48 min rounds to 60
        assert_eq!(status(60 * 60 + 30), 5); // 60.5 rounds half away from zero
        assert_eq!(status(360 * 60), 5);
        assert_eq!(status(360 * 60 + 30), 15);
        assert_eq!(status(1440 * 60), 15);
        assert_eq!(status(10080 * 60), 60);
        assert_eq!(status(10080 * 60 + 30), 360);
        assert_eq!(status(-3600), 1);
        // get_queue_lag_v1 compares the unrounded minutes.
        let lag = |us: i64| lag_bucket_minutes(from, from + us);
        assert_eq!(lag(60 * US_PER_MIN), 1);
        assert_eq!(lag(60 * US_PER_MIN + 1), 5);
        assert_eq!(lag(360 * US_PER_MIN + 1), 15);
        assert_eq!(lag(1440 * US_PER_MIN + 1), 60);
        assert_eq!(lag(10080 * US_PER_MIN + 1), 360);
    }

    #[test]
    fn buckets_floor_within_the_hour() {
        let t = ts("2026-09-24T10:37:42.5Z");
        let at = |b| to_char_s(floor_bucket(t, b));
        assert_eq!(at(1), "2026-09-24T10:37:00Z");
        assert_eq!(at(5), "2026-09-24T10:35:00Z");
        assert_eq!(at(15), "2026-09-24T10:30:00Z");
        assert_eq!(at(60), "2026-09-24T10:00:00Z");
        // EXTRACT(minute) % 360 is the minute itself: six-hour buckets floor
        // to the hour.
        assert_eq!(at(360), "2026-09-24T10:00:00Z");
    }

    #[test]
    fn timestamps_render_like_to_char() {
        let t = ts("2026-09-24T10:30:45.123999Z");
        assert_eq!(to_char_s(t), "2026-09-24T10:30:45Z");
        assert_eq!(to_char_ms(t), "2026-09-24T10:30:45.123Z");
        assert_eq!(
            to_char_ms(ts("2026-09-24T10:30:45Z")),
            "2026-09-24T10:30:45.000Z"
        );
    }

    // -- empty answers and errors -------------------------------------------

    #[test]
    fn empty_inputs_answer_like_the_procedures() {
        let none = Map::new();
        assert_eq!(
            queue_ops_json(&none, now(), &[], &[], &[]),
            json!({ "timeRange": last_hour(), "bucketMinutes": 1, "series": [], "queues": [] })
        );
        assert_eq!(
            parked_replicas_json(&none, now(), &[]),
            json!({ "timeRange": last_hour(), "bucketMinutes": 1, "series": [], "replicas": [] })
        );
        let zeros = json!({
            "retentionMsgs": 0, "completedRetentionMsgs": 0, "evictionMsgs": 0,
            "totalMsgs": 0, "eventCount": 0,
        });
        assert_eq!(
            retention_json(&none, now(), &[], &[]),
            Ok(
                json!({ "timeRange": last_hour(), "bucketMinutes": 1, "series": [], "totals": zeros })
            )
        );
        assert_eq!(
            retention_json(&filters(&[("groupBy", "queue")]), now(), &[], &[]),
            Ok(json!({
                "timeRange": last_hour(), "bucketMinutes": 1, "series": [], "totals": zeros, "rows": [],
            }))
        );
        assert_eq!(
            queue_lag_json(None, None, None, now(), &[], &[], &[]),
            json!([])
        );
        // The workload axis is generated, not read from rows; no queue means
        // no rows and an empty tenant object.
        let axis: Vec<String> = (0..61)
            .map(|i| to_char_s(ts("2026-09-24T09:30:00Z") + i * US_PER_MIN))
            .collect();
        assert_eq!(
            workload_json(&none, now(), &[], &[], &[], &[]),
            Ok(json!({
                "timeRange": last_hour(), "bucketMinutes": 1, "groupBy": "namespace",
                "buckets": axis, "rows": [], "tenant": {},
            }))
        );
    }

    #[test]
    fn an_unparsable_timestamp_is_the_procedures_error() {
        let bad = filters(&[("from", "nope")]);
        let err =
            json!({ "error": "invalid input syntax for type timestamp with time zone: \"nope\"" });
        assert_eq!(queue_ops_json(&bad, now(), &[], &[], &[]), err);
        assert_eq!(parked_replicas_json(&bad, now(), &[]), err);
        assert_eq!(
            workload_json(&bad, now(), &[], &[], &[], &[]),
            Ok(err.clone())
        );
        assert_eq!(retention_json(&bad, now(), &[], &[]), Ok(err.clone()));
        assert_eq!(
            queue_lag_json(Some("nope"), None, None, now(), &[], &[], &[]),
            err
        );
        // The handlers' 400 comes before the procedure runs.
        let both = filters(&[("from", "nope"), ("groupBy", "Queue")]);
        assert_eq!(
            workload_json(&both, now(), &[], &[], &[], &[]),
            Err("bad groupBy".into())
        );
        assert_eq!(
            retention_json(&both, now(), &[], &[]),
            Err("bad groupBy".into())
        );
    }

    // -- queue-ops ----------------------------------------------------------

    #[test]
    fn queue_ops_rolls_rows_into_buckets() {
        // 6 h: 5-minute buckets. `_tenant` rides along as the handler adds it.
        let f = filters(&[
            ("from", "2026-09-24T04:00:00Z"),
            ("to", "2026-09-24T10:00:00Z"),
            ("_tenant", "00000000-0000-0000-0000-000000000001"),
        ]);
        let queues = [
            meta("a", Some("x"), None),
            QueueMeta {
                created_us: ts("2026-09-24T09:02:30Z"),
                ..meta("b", None, None)
            },
        ];
        let rows = [
            QueueRow {
                push_requests: 3,
                push_messages: 30,
                pop_messages: 20,
                pop_empty: 5,
                ack_requests: 4,
                ack_success: 18,
                ack_failed: 2,
                transactions: 1,
                avg_lag_ms: 100,
                max_lag_ms: 500,
                lag_count: 20,
                parked_count: 4,
                ..qrow("2026-09-24T09:01:00Z", "a")
            },
            QueueRow {
                pop_messages: 10,
                avg_lag_ms: 200,
                max_lag_ms: 900,
                lag_count: 10,
                parked_count: 6,
                ..qrow("2026-09-24T09:03:00Z", "a")
            },
            QueueRow {
                push_messages: 5,
                parked_count: 2,
                ..qrow("2026-09-24T09:07:00Z", "a")
            },
            // b before its creation minute (an earlier incarnation) and a
            // deleted queue: both are excluded here.
            QueueRow {
                push_messages: 99,
                ..qrow("2026-09-24T09:01:00Z", "b")
            },
            QueueRow {
                push_messages: 7,
                pop_messages: 7,
                avg_lag_ms: 3,
                max_lag_ms: 4,
                ..qrow("2026-09-24T09:02:00Z", "b")
            },
            QueueRow {
                push_messages: 99,
                ..qrow("2026-09-24T09:02:00Z", "zombie")
            },
            // Outside [from, to]; `to` itself is inside.
            QueueRow {
                push_messages: 99,
                ..qrow("2026-09-24T10:01:00Z", "a")
            },
            QueueRow {
                push_messages: 1,
                ..qrow("2026-09-24T10:00:00Z", "b")
            },
        ];
        // 09:03 folds into a's row; 09:04 is a churn-only row: one more
        // parked sample, of 0.
        let lifecycle = [
            churn("2026-09-24T09:03:00Z", "a", 1, 0),
            churn("2026-09-24T09:04:00Z", "a", 0, 2),
        ];
        let v = queue_ops_json(&f, now(), &rows, &lifecycle, &queues);
        assert_eq!(v["bucketMinutes"], json!(5));
        assert_eq!(
            v["timeRange"],
            json!({ "from": "2026-09-24T04:00:00.000Z", "to": "2026-09-24T10:00:00.000Z" })
        );
        assert_eq!(v["queues"], json!(["a", "b"]));
        assert_eq!(
            strs(&v["series"], "bucket"),
            [
                "2026-09-24T09:00:00Z",
                "2026-09-24T09:00:00Z",
                "2026-09-24T09:05:00Z",
                "2026-09-24T10:00:00Z"
            ]
        );
        assert_eq!(strs(&v["series"], "queueName"), ["a", "b", "a", "b"]);
        assert_eq!(
            v["series"][0],
            json!({
                "bucket": "2026-09-24T09:00:00Z", "queueName": "a",
                "pushRequests": 3, "pushMessages": 30, "popMessages": 30, "popEmpty": 5,
                "ackRequests": 4, "ackSuccess": 18, "ackFailed": 2, "transactions": 1,
                "partitionsCreated": 1, "partitionsDeleted": 2, "partitionCount": null,
                // pop-weighted: (100·20 + 200·10) / 30
                "avgLagMs": pg("133.3333333333333333"),
                "maxLagMs": 900,
                "pushPerSecond": pg("0.10"), "popPerSecond": pg("0.10"),
                "ackPerSecond": pg("0.07"), "emptyPerSecond": pg("0.02"),
                // AVG over three rows: 4, 6 and the churn-only minute's 0.
                "parkedCount": pg("3.33"),
            })
        );
        assert_eq!(v["series"][1]["pushMessages"], json!(7));
        assert_eq!(v["series"][1]["avgLagMs"], pg("3.0000000000000000"));
        // No pops: the lag was not measured, which is not a zero.
        assert_eq!(v["series"][2]["avgLagMs"], Value::Null);
        assert_eq!(v["series"][2]["maxLagMs"], Value::Null);
        assert_eq!(v["series"][2]["parkedCount"], pg("2.00"));
        assert_eq!(v["series"][2]["pushPerSecond"], pg("0.02"));

        let only_b = queue_ops_json(
            &with(&f, &[("queue", "b")]),
            now(),
            &rows,
            &lifecycle,
            &queues,
        );
        assert_eq!(only_b["queues"], json!(["b"]));
        assert_eq!(strs(&only_b["series"], "queueName"), ["b", "b"]);
        // An empty value is no filter (filters_from_query drops it).
        assert_eq!(
            queue_ops_json(
                &with(&f, &[("queue", "")]),
                now(),
                &rows,
                &lifecycle,
                &queues
            ),
            v
        );
    }

    #[test]
    fn over_seven_days_the_buckets_are_hours_of_a_six_hour_width() {
        let f = filters(&[
            ("from", "2026-09-16T10:30:45Z"),
            ("to", "2026-09-24T10:30:45Z"),
        ]);
        let rows = [
            QueueRow {
                pop_messages: 5,
                ..qrow("2026-09-17T04:10:00Z", "a")
            },
            QueueRow {
                pop_messages: 7,
                ..qrow("2026-09-17T05:10:00Z", "a")
            },
        ];
        let ops = queue_ops_json(&f, now(), &rows, &[], &[meta("a", None, None)]);
        assert_eq!(ops["bucketMinutes"], json!(360));
        assert_eq!(
            strs(&ops["series"], "bucket"),
            ["2026-09-17T04:00:00Z", "2026-09-17T05:00:00Z"]
        );
        // 7 / (360 · 60): the rate divides by the nominal width.
        assert_eq!(ops["series"][1]["popPerSecond"], pg("0.00"));

        let q = [QueueNow {
            meta: meta("a", None, None),
            ..Default::default()
        }];
        let w = workload_json(&f, now(), &rows, &[], &[], &q).expect("200");
        let axis = w["buckets"].as_array().cloned().unwrap_or_default();
        assert_eq!(axis.len(), 33);
        assert_eq!(axis.first(), Some(&json!("2026-09-16T10:00:00Z")));
        assert_eq!(axis.get(3), Some(&json!("2026-09-17T04:00:00Z")));
        // The six-hour axis meets the hourly 04:00 bucket and misses 05:00 ...
        let pop = w["tenant"]["series"]["pop"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(pop.iter().filter(|p| !p.is_null()).count(), 1);
        assert_eq!(pop.get(3), Some(&json!(5)));
        // ... while the window counts both.
        assert_eq!(w["tenant"]["window"]["popMessages"], json!(12));
    }

    // -- parked per replica -------------------------------------------------

    #[test]
    fn parked_replicas_average_each_node() {
        let f = filters(&[
            ("from", "2026-09-24T04:00:00Z"),
            ("to", "2026-09-24T10:00:00Z"),
        ]);
        let p = |minute: &str, queue: &str, host: &str, parked: i32| ParkedRow {
            bucket_us: ts(minute),
            tenant: "t".into(),
            queue: queue.into(),
            hostname: host.into(),
            worker_id: 0,
            parked_count: parked,
        };
        let rows = [
            p("2026-09-24T09:02:00Z", "a", "node-2", 1),
            p("2026-09-24T09:01:00Z", "a", "node-1", 3),
            p("2026-09-24T09:02:00Z", "a", "node-1", 4),
            p("2026-09-24T09:03:00Z", "b", "node-1", 2),
            p("2026-09-24T03:59:00Z", "b", "node-3", 9),
        ];
        let v = parked_replicas_json(&f, now(), &rows);
        assert_eq!(
            v["series"],
            json!([
                { "bucket": "2026-09-24T09:00:00Z", "queueName": "a", "hostname": "node-1",
                  "workerId": 0, "parkedCount": pg("3.50") },
                { "bucket": "2026-09-24T09:00:00Z", "queueName": "a", "hostname": "node-2",
                  "workerId": 0, "parkedCount": pg("1.00") },
                { "bucket": "2026-09-24T09:00:00Z", "queueName": "b", "hostname": "node-1",
                  "workerId": 0, "parkedCount": pg("2.00") },
            ])
        );
        assert_eq!(
            v["replicas"],
            json!([{ "hostname": "node-1", "workerId": 0 }, { "hostname": "node-2", "workerId": 0 }])
        );
        let b = parked_replicas_json(&with(&f, &[("queue", "b")]), now(), &rows);
        assert_eq!(
            b["replicas"],
            json!([{ "hostname": "node-1", "workerId": 0 }])
        );
        assert_eq!(strs(&b["series"], "queueName"), ["b"]);
    }

    // -- workload -----------------------------------------------------------

    /// The tenant of app/test/fixtures/queue_ops.small.json, whose sums
    /// app/test/workload.test.js asserts for the client-side rollup of the
    /// same contract.
    fn ops_tenant() -> (Vec<QueueNow>, Vec<QueueRow>, Vec<ChurnRow>) {
        let q = |m: QueueMeta, figures: [i64; 6]| QueueNow {
            meta: m,
            pending: figures[0],
            processing: figures[1],
            dead_letter: figures[2],
            retained_bytes: figures[3],
            partitions: figures[4],
            groups: figures[5],
        };
        let queues = vec![
            q(
                meta("alpha.q1", Some("alpha"), Some("ingest")),
                [100, 5, 3, 1000, 4, 2],
            ),
            q(
                meta("beta.q2", Some("beta"), Some("")),
                [0, 0, 0, 2000, 1, 1],
            ),
            q(meta("gamma.q3", Some("gamma"), None), [7, 0, 0, 4000, 2, 0]),
        ];
        let rows = vec![
            QueueRow {
                push_requests: 2,
                push_messages: 10,
                pop_messages: 20,
                pop_empty: 5,
                ack_requests: 4,
                ack_success: 18,
                ack_failed: 2,
                transactions: 1,
                avg_lag_ms: 100,
                max_lag_ms: 500,
                lag_count: 20,
                parked_count: 4,
                ..qrow("2026-09-09T08:00:00Z", "alpha.q1")
            },
            QueueRow {
                push_requests: 3,
                push_messages: 30,
                pop_messages: 10,
                ack_requests: 2,
                ack_success: 10,
                avg_lag_ms: 200,
                max_lag_ms: 900,
                lag_count: 10,
                parked_count: 6,
                ..qrow("2026-09-09T08:15:00Z", "alpha.q1")
            },
            // Pushes but no pops: its max_lag_ms is not a lag sample.
            QueueRow {
                push_requests: 5,
                push_messages: 5,
                pop_empty: 50,
                max_lag_ms: 7,
                parked_count: 2,
                ..qrow("2026-09-09T08:00:00Z", "beta.q2")
            },
            QueueRow {
                pop_messages: 40,
                pop_empty: 1,
                ack_requests: 1,
                ack_success: 40,
                transactions: 7,
                avg_lag_ms: 50,
                max_lag_ms: 60_000,
                lag_count: 40,
                parked_count: 10,
                ..qrow("2026-09-09T08:30:00Z", "beta.q2")
            },
        ];
        let lifecycle = vec![
            churn("2026-09-09T08:00:00Z", "alpha.q1", 1, 0),
            churn("2026-09-09T08:15:00Z", "alpha.q1", 0, 2),
        ];
        (queues, rows, lifecycle)
    }

    fn ops_window() -> Map<String, Value> {
        filters(&[
            ("from", "2026-09-09T08:00:00Z"),
            ("to", "2026-09-09T08:45:00Z"),
        ])
    }

    #[test]
    fn workload_matches_the_rollup_contract() {
        let (queues, rows, lifecycle) = ops_tenant();
        let v = workload_json(&ops_window(), now(), &rows, &lifecycle, &[], &queues).expect("200");
        assert_eq!(v["groupBy"], json!("namespace"));
        assert_eq!(v["bucketMinutes"], json!(1));
        assert_eq!(v["buckets"].as_array().map(Vec::len), Some(46));

        let t = &v["tenant"];
        assert_eq!(t["queues"], json!(3));
        assert_eq!(
            t["window"],
            json!({
                "pushMessages": 45, "pushRequests": 10, "popMessages": 70, "popEmpty": 56,
                "ackRequests": 7, "ackSuccess": 68, "ackFailed": 2, "transactions": 8,
                "conflated": 0, "partitionsCreated": 1, "partitionsDeleted": 2,
                // Per queue AVG across time, then SUM: (4+6)/2 + (2+10)/2.
                "parkedAvg": pg("11.00"),
                // round((100·20 + 200·10 + 50·40) / 70)
                "avgLagMs": 86,
                "maxLagMs": 60000,
            })
        );
        assert_eq!(
            t["now"],
            json!({
                "pending": 107, "processing": 5, "deadLetter": 3, "retainedBytes": 7000,
                "partitions": 7, "groups": 3, "queuesWithoutGroup": 1,
                "pendingWithoutGroup": 7, "queuesTouched": 2, "queuesActive": 2,
            })
        );

        let out = v["rows"].as_array().cloned().unwrap_or_default();
        assert_eq!(strs(&v["rows"], "key"), ["alpha", "beta", "gamma"]);
        let (alpha, beta, gamma) = (&out[0], &out[1], &out[2]);
        assert!(
            alpha.get("namespace").is_none(),
            "namespace/task only on a queue grouping"
        );
        assert_eq!(alpha["queues"], json!(1));
        assert_eq!(alpha["window"]["pushMessages"], json!(40));
        assert_eq!(alpha["window"]["avgLagMs"], json!(133)); // round(4000 / 30)
        assert_eq!(alpha["window"]["maxLagMs"], json!(900));
        assert_eq!(alpha["window"]["parkedAvg"], pg("5.00"));
        assert_eq!(beta["window"]["parkedAvg"], pg("6.00"));
        assert_eq!(beta["window"]["avgLagMs"], json!(50));
        // A bucket without a row is null; a row that reported zero is 0.
        let at = |r: &Value, s: &str, i: usize| r["series"][s][i].clone();
        assert_eq!(
            [
                at(alpha, "pop", 0),
                at(alpha, "pop", 15),
                at(alpha, "pop", 30)
            ],
            [json!(20), json!(10), Value::Null]
        );
        assert_eq!(
            [at(beta, "pop", 0), at(beta, "pop", 15), at(beta, "pop", 30)],
            [json!(0), Value::Null, json!(40)]
        );
        // Max lag only from rows that popped (FILTER (WHERE pop_count > 0)).
        assert_eq!(
            [at(beta, "maxLagMs", 0), at(beta, "maxLagMs", 30)],
            [Value::Null, json!(60000)]
        );
        assert_eq!(
            [at(beta, "avgLagMs", 0), at(beta, "avgLagMs", 30)],
            [Value::Null, json!(50)]
        );
        assert_eq!(
            [at(alpha, "parked", 0), at(alpha, "parked", 15)],
            [pg("4.00"), pg("6.00")]
        );
        assert_eq!(at(alpha, "avgLagMs", 0), json!(100));
        // The untouched queue: zeros, unmeasured lag, an all-null series and a
        // pending backlog nobody consumes.
        assert_eq!(gamma["window"]["popMessages"], json!(0));
        assert_eq!(gamma["window"]["avgLagMs"], Value::Null);
        assert_eq!(gamma["window"]["maxLagMs"], Value::Null);
        assert_eq!(gamma["window"]["parkedAvg"], pg("0.00"));
        let gamma_pop = gamma["series"]["pop"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        assert_eq!(gamma_pop.len(), 46);
        assert!(gamma_pop.iter().all(Value::is_null));
        assert_eq!(gamma["now"]["queuesTouched"], json!(0));
        assert_eq!(gamma["now"]["pendingWithoutGroup"], json!(7));
        // The rows partition the tenant.
        let pops: i64 = out
            .iter()
            .filter_map(|r| r["window"]["popMessages"].as_i64())
            .sum();
        assert_eq!(pops, 70);
    }

    #[test]
    fn workload_filters_pick_rows_not_the_tenant() {
        let (queues, rows, lifecycle) = ops_tenant();
        let call = |extra: &[(&str, &str)]| {
            workload_json(
                &with(&ops_window(), extra),
                now(),
                &rows,
                &lifecycle,
                &[],
                &queues,
            )
        };

        let v = call(&[("groupBy", "queue"), ("namespace", "alpha")]).expect("200");
        assert_eq!(strs(&v["rows"], "key"), ["alpha.q1"]);
        assert_eq!(v["rows"][0]["namespace"], json!("alpha"));
        assert_eq!(v["rows"][0]["task"], json!("ingest"));
        assert_eq!(v["tenant"]["window"]["popMessages"], json!(70));

        // `task=` with an EMPTY value is the group of queues without a task
        // (handle_workload re-inserts it): '' and NULL alike.
        let v = call(&[("groupBy", "task"), ("task", "")]).expect("200");
        assert_eq!(strs(&v["rows"], "key"), [""]);
        assert_eq!(v["rows"][0]["queues"], json!(2));
        let v = call(&[("groupBy", "queue"), ("namespace", "")]).expect("200");
        assert_eq!(v["rows"], json!([]));

        // An empty `queue=` or `groupBy=` is no filter.
        let v = call(&[("queue", ""), ("groupBy", "")]).expect("200");
        assert_eq!(v["groupBy"], json!("namespace"));
        assert_eq!(strs(&v["rows"], "key"), ["alpha", "beta", "gamma"]);
        let v = call(&[("queue", "beta.q2"), ("groupBy", "task")]).expect("200");
        assert_eq!(strs(&v["rows"], "key"), [""]);
        assert_eq!(v["rows"][0]["queues"], json!(1));

        // Nothing matches: no rows, the tenant stays whole.
        let v = call(&[("queue", "nope")]).expect("200");
        assert_eq!(v["rows"], json!([]));
        assert_eq!(v["tenant"]["queues"], json!(3));

        assert_eq!(
            call(&[("groupBy", "partition")]),
            Err("bad groupBy".to_string())
        );
    }

    // -- retention ----------------------------------------------------------

    #[test]
    fn retention_series_totals_and_groups() {
        let queues = [
            meta("a", Some("x"), None),
            meta("b", Some("y"), None),
            QueueMeta {
                created_us: ts("2026-09-24T09:03:30Z"),
                ..meta("c", Some("y"), None)
            },
        ];
        let r =
            |at: &str, queue: &str, retention: i64, completed: i64, evicted: i64| RetentionRow {
                at_us: ts(at),
                tenant: "t".into(),
                queue: queue.into(),
                partition_id: 1,
                retention_msgs: retention,
                completed_retention_msgs: completed,
                eviction_msgs: evicted,
                ..Default::default()
            };
        let rows = [
            r("2026-09-24T09:01:10Z", "a", 100, 0, 0),
            r("2026-09-24T09:01:50.5Z", "a", 0, 0, 5),
            // A deleted queue, and c before its current incarnation: rows
            // that no longer resolve to a queue.
            r("2026-09-24T09:02:00Z", "zombie", 50, 0, 0),
            r("2026-09-24T09:03:00Z", "c", 0, 0, 1),
            r("2026-09-24T09:03:59Z", "b", 0, 7, 0),
            r("2026-09-24T09:04:00Z", "c", 0, 7, 0),
            r("2026-09-24T08:59:59Z", "a", 1000, 0, 0),
        ];
        let f = filters(&[
            ("from", "2026-09-24T09:00:00Z"),
            ("to", "2026-09-24T10:00:00Z"),
        ]);
        let entry = |bucket: &str, ret: i64, comp: i64, ev: i64, events: i64| {
            json!({
                "bucket": bucket, "retentionMsgs": ret, "completedRetentionMsgs": comp,
                "evictionMsgs": ev, "totalMsgs": ret + comp + ev, "eventCount": events,
            })
        };
        let v = retention_json(&f, now(), &rows, &queues).expect("200");
        assert_eq!(
            v["series"],
            json!([
                entry("2026-09-24T09:01:00Z", 100, 0, 5, 2),
                entry("2026-09-24T09:02:00Z", 50, 0, 0, 1),
                entry("2026-09-24T09:03:00Z", 0, 7, 1, 2),
                entry("2026-09-24T09:04:00Z", 0, 7, 0, 1),
            ])
        );
        assert_eq!(
            v["totals"],
            json!({
                "retentionMsgs": 150, "completedRetentionMsgs": 14, "evictionMsgs": 6,
                "totalMsgs": 170, "eventCount": 6,
            })
        );
        assert!(v.get("rows").is_none(), "no groupBy, no rows key");

        let v = retention_json(
            &with(&f, &[("groupBy", "namespace")]),
            now(),
            &rows,
            &queues,
        )
        .expect("200");
        assert_eq!(v["totals"]["totalMsgs"], json!(170));
        assert_eq!(
            v["rows"],
            json!([
                { "key": "x", "queues": 1, "totals": {
                    "retentionMsgs": 100, "completedRetentionMsgs": 0, "evictionMsgs": 5,
                    "totalMsgs": 105, "eventCount": 2 } },
                { "key": "y", "queues": 2, "totals": {
                    "retentionMsgs": 0, "completedRetentionMsgs": 14, "evictionMsgs": 0,
                    "totalMsgs": 14, "eventCount": 2 } },
            ])
        );
        // Ties on totalMsgs fall back to the key.
        let v =
            retention_json(&with(&f, &[("groupBy", "queue")]), now(), &rows, &queues).expect("200");
        assert_eq!(strs(&v["rows"], "key"), ["a", "b", "c"]);
        // A queue filter drops what cannot be resolved.
        let v = retention_json(&with(&f, &[("queue", "c")]), now(), &rows, &queues).expect("200");
        assert_eq!(v["totals"]["totalMsgs"], json!(7));
        assert_eq!(v["totals"]["eventCount"], json!(1));

        assert_eq!(
            retention_json(&with(&f, &[("groupBy", "tenant")]), now(), &rows, &queues),
            Err("bad groupBy".to_string())
        );
    }

    // -- queue-lag ----------------------------------------------------------

    #[test]
    fn queue_lag_is_a_bare_array_newest_first() {
        let queues = [meta("a", None, None), meta("b", None, None)];
        let rows = [
            QueueRow {
                pop_messages: 10,
                avg_lag_ms: 100,
                max_lag_ms: 300,
                ..qrow("2026-09-24T09:01:00Z", "a")
            },
            QueueRow {
                push_messages: 3,
                max_lag_ms: 50,
                ..qrow("2026-09-24T09:03:00Z", "a")
            },
            QueueRow {
                push_messages: 1,
                ..qrow("2026-09-24T09:06:00Z", "a")
            },
            QueueRow {
                pop_messages: 5,
                avg_lag_ms: 20,
                max_lag_ms: 20,
                ..qrow("2026-09-24T09:02:00Z", "b")
            },
        ];
        let (from, to) = (Some("2026-09-24T08:00:00Z"), Some("2026-09-24T10:00:00Z"));
        let v = queue_lag_json(from, to, None, now(), &rows, &[], &queues);
        assert_eq!(
            v,
            json!([
                { "queueName": "a", "popCount": 0, "avgLagMs": null, "maxLagMs": null,
                  "bucketMinutes": 5, "bucketTime": "2026-09-24T09:05:00Z" },
                // Pops in the bucket: the max covers every row of it.
                { "queueName": "a", "popCount": 10, "avgLagMs": pg("100.0000000000000000"),
                  "maxLagMs": 300, "bucketMinutes": 5, "bucketTime": "2026-09-24T09:00:00Z" },
                { "queueName": "b", "popCount": 5, "avgLagMs": pg("20.0000000000000000"),
                  "maxLagMs": 20, "bucketMinutes": 5, "bucketTime": "2026-09-24T09:00:00Z" },
            ])
        );
        let b = queue_lag_json(from, to, Some("b"), now(), &rows, &[], &queues);
        assert_eq!(strs(&b, "queueName"), ["b"]);
        // Empty values are absent: the default last hour holds none of these.
        assert_eq!(
            queue_lag_json(Some(""), Some(""), Some(""), now(), &rows, &[], &queues),
            json!([])
        );
        // 60 min 20 s: 5-minute points here, 1-minute buckets on queue-ops.
        let late = [QueueRow {
            pop_messages: 1,
            ..qrow("2026-09-24T08:30:00Z", "a")
        }];
        let (from, to) = ("2026-09-24T08:00:00Z", "2026-09-24T09:00:20Z");
        let v = queue_lag_json(Some(from), Some(to), None, now(), &late, &[], &queues);
        assert_eq!(v[0]["bucketMinutes"], json!(5));
        let ops = queue_ops_json(
            &filters(&[("from", from), ("to", to)]),
            now(),
            &late,
            &[],
            &queues,
        );
        assert_eq!(ops["bucketMinutes"], json!(1));
    }

    // -- golden shapes ------------------------------------------------------

    /// A production payload from app/test/fixtures, when the repository
    /// layout is there (the expected key sets are also spelled out, so the
    /// test never passes vacuously).
    fn fixture(name: &str) -> Option<Value> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../app/test/fixtures")
            .join(name);
        serde_json::from_str(&std::fs::read_to_string(path).ok()?).ok()
    }

    fn keys(v: &Value) -> BTreeSet<String> {
        v.as_object()
            .map(|o| o.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn set(k: &[&str]) -> BTreeSet<String> {
        k.iter().map(|s| s.to_string()).collect()
    }

    /// JSON type classes, `null` and number counted as one (a measured value
    /// may be either).
    fn kind(v: &Value) -> &'static str {
        match v {
            Value::Null | Value::Number(_) => "number|null",
            Value::String(_) => "string",
            Value::Array(_) => "array",
            Value::Object(_) => "object",
            Value::Bool(_) => "bool",
        }
    }

    fn same_shape(gold: &Value, mine: &Value, at: &str) {
        assert_eq!(keys(gold), keys(mine), "{at}: keys");
        if let (Some(g), Some(m)) = (gold.as_object(), mine.as_object()) {
            for (k, gv) in g {
                let mv = &m[k];
                assert_eq!(kind(gv), kind(mv), "{at}.{k}: type");
                if gv.is_object() {
                    same_shape(gv, mv, &format!("{at}.{k}"));
                }
            }
        }
    }

    #[test]
    fn payload_shapes_match_the_production_fixtures() {
        let (queues, rows, lifecycle) = ops_tenant();
        let call = |g: &str| {
            workload_json(
                &with(&ops_window(), &[("groupBy", g)]),
                now(),
                &rows,
                &lifecycle,
                &[],
                &queues,
            )
            .expect("200")
        };
        let by_ns = call("namespace");
        let by_q = call("queue");
        let base = ["queues", "window", "series", "now"];
        assert_eq!(
            keys(&by_ns),
            set(&[
                "timeRange",
                "bucketMinutes",
                "groupBy",
                "buckets",
                "rows",
                "tenant"
            ])
        );
        assert_eq!(keys(&by_ns["tenant"]), set(&base));
        assert_eq!(
            keys(&by_ns["rows"][0]),
            set(&["key", "queues", "window", "series", "now"])
        );
        assert_eq!(
            keys(&by_q["rows"][0]),
            set(&[
                "key",
                "namespace",
                "task",
                "queues",
                "window",
                "series",
                "now"
            ])
        );
        assert_eq!(
            keys(&by_ns["tenant"]["window"]),
            set(&[
                "pushMessages",
                "pushRequests",
                "popMessages",
                "popEmpty",
                "ackRequests",
                "ackSuccess",
                "ackFailed",
                "transactions",
                "conflated",
                "partitionsCreated",
                "partitionsDeleted",
                "parkedAvg",
                "avgLagMs",
                "maxLagMs",
            ])
        );
        assert_eq!(
            keys(&by_ns["tenant"]["series"]),
            set(&[
                "push",
                "pop",
                "popEmpty",
                "ackFailed",
                "parked",
                "avgLagMs",
                "maxLagMs"
            ])
        );
        assert_eq!(
            keys(&by_ns["tenant"]["now"]),
            set(&[
                "pending",
                "processing",
                "deadLetter",
                "retainedBytes",
                "partitions",
                "groups",
                "queuesWithoutGroup",
                "pendingWithoutGroup",
                "queuesTouched",
                "queuesActive",
            ])
        );

        for (name, mine) in [
            ("workload.namespace.1h.json", &by_ns),
            ("workload.namespace.7d.json", &by_ns),
            ("workload.queue.24h.json", &by_q),
            ("workload.queue.smartchat.1h.json", &by_q),
        ] {
            let Some(gold) = fixture(name) else {
                continue;
            };
            same_shape(&gold, mine, name);
            for (i, row) in gold["rows"].as_array().into_iter().flatten().enumerate() {
                same_shape(row, &mine["rows"][0], &format!("{name} rows[{i}]"));
            }
            // Series are index-aligned with the axis; timestamps share a format.
            let n = gold["buckets"].as_array().map(Vec::len);
            for s in gold["tenant"]["series"]
                .as_object()
                .into_iter()
                .flat_map(|o| o.values())
            {
                assert_eq!(s.as_array().map(Vec::len), n, "{name}: series length");
            }
            let len = |v: &Value| v.as_str().map(str::len);
            assert_eq!(
                len(&gold["buckets"][0]),
                len(&mine["buckets"][0]),
                "{name}: bucket text"
            );
            assert_eq!(
                len(&gold["timeRange"]["from"]),
                len(&mine["timeRange"]["from"]),
                "{name}: timeRange text"
            );
        }

        let meta_of = |q: &QueueNow| q.meta.clone();
        let metas: Vec<QueueMeta> = queues.iter().map(meta_of).collect();
        let ops = queue_ops_json(&ops_window(), now(), &rows, &lifecycle, &metas);
        assert_eq!(
            keys(&ops),
            set(&["timeRange", "bucketMinutes", "series", "queues"])
        );
        assert_eq!(
            keys(&ops["series"][0]),
            set(&[
                "bucket",
                "queueName",
                "pushRequests",
                "pushMessages",
                "popMessages",
                "popEmpty",
                "ackRequests",
                "ackSuccess",
                "ackFailed",
                "transactions",
                "partitionsCreated",
                "partitionsDeleted",
                "partitionCount",
                "avgLagMs",
                "maxLagMs",
                "pushPerSecond",
                "popPerSecond",
                "ackPerSecond",
                "emptyPerSecond",
                "parkedCount",
            ])
        );
        if let Some(gold) = fixture("queue_ops.small.json") {
            // The hand-made fixture predates the rate helpers; every key it
            // does carry must be served.
            let gold_ops = &gold["ops"];
            assert_eq!(keys(gold_ops), keys(&ops));
            assert!(keys(&gold_ops["series"][0]).is_subset(&keys(&ops["series"][0])));
            assert_eq!(keys(&gold_ops["timeRange"]), keys(&ops["timeRange"]));
        }
    }
}
